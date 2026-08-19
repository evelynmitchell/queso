//! Issue #36's regression test: a **majority** reboot of a real, separate-OS-
//! process `queso-node` cluster must not lose an acknowledged write or
//! diverge, and a **minority** reboot (the strictly easier case, kept here
//! as a contrast, matching the audit's `probe_single.sh`) must keep working
//! too.
//!
//! Unlike `tests/cluster.rs` (which runs each replica as an in-process task
//! sharing the test binary's own memory), this file spawns the actual
//! `queso-node` binary as independent OS processes and kills them with a
//! real `SIGKILL` -- the same reproduction the whole-system audit used
//! (`scratchpad/probe_amnesia.sh`/`probe_single.sh`) -- so that "restart"
//! here means exactly what it means in production: a fresh process, a blank
//! heap, nothing surviving except whatever actually made it to disk. This is
//! the only way to *actually* exercise `crate::persist`/`crate::driver`'s
//! boot-time reload path; an in-process "drop and rebuild the `SmrNode`"
//! test would still leave real disk I/O untested.
//!
//! Before this branch's persistence fix, `majority_reboot_does_not_lose_an_acknowledged_write`
//! fails exactly as `probe_amnesia.sh` does: the restarted majority answers
//! the post-reboot `Get` with `None` instead of `Some(7)` (see this crate's
//! README / `docs/STATUS.md` for how that was confirmed before landing the
//! fix).
//!
//! The real-process cluster helper this file used to define now lives in
//! `tests/support` as `ProcCluster`, so `tests/durability_faults.rs`
//! (issue #39) could use the same one rather than a second copy. It gained
//! a boot retry in the move -- see that type's docs and issue #40 for the
//! `free_addr` race it works around.

use std::net::SocketAddr;
use std::time::Duration;

use queso_net::client;
use queso_smr::{ClientId, Command as KvCommand, Outcome};

mod support;
use support::ProcCluster as RealCluster;

/// Retry `client::submit` against `addr` until it succeeds or `timeout`
/// elapses -- see `tests/cluster.rs`'s identical helper's docs.
async fn submit_with_retry(addr: SocketAddr, command: &KvCommand, timeout: Duration) -> Outcome {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match client::submit(addr, command).await {
            Ok(outcome) => return outcome,
            Err(err) => {
                if tokio::time::Instant::now() >= deadline {
                    panic!("submit to {addr} never succeeded (last error: {err:?})");
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

fn put(client: u32, seq: u64, key: u32, value: i64) -> KvCommand {
    KvCommand::Put {
        client: ClientId(client),
        seq,
        key,
        value,
    }
}

fn get(client: u32, seq: u64, key: u32) -> KvCommand {
    KvCommand::Get {
        client: ClientId(client),
        seq,
        key,
    }
}

/// The exact scenario issue #36 reports, reproduced against real,
/// independent `queso-node` OS processes (mirrors
/// `scratchpad/probe_amnesia.sh`):
///
/// 1. `Put(42, 7)` via replica 0; confirm it replicated by reading it back
///    from replica 2.
/// 2. `SIGKILL` replicas 1 *and* 2 -- a majority of the 3-replica cluster --
///    and restart both against the same `--data-dir`.
/// 3. Every replica, including the two that just rebooted, must still
///    answer `Get(42)` with `Some(7)`: the acknowledged write must survive,
///    and no replica may re-decide slot 0 as empty (which would be a P1
///    Agreement violation between replica 0's view and the restarted
///    majority's).
///
/// Without this branch's persistence (`crate::persist`) and boot-time
/// reload/`on_restart` wiring (`crate::driver::run_node`), step 3 fails
/// exactly like the audit found: the restarted replicas come back as blank
/// `SmrNode`s, form a live majority (2 of 3) with no memory of slot 0, and a
/// fresh catch-up probe re-decides it as empty.
#[tokio::test(flavor = "multi_thread")]
async fn majority_reboot_does_not_lose_an_acknowledged_write() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let mut cluster = RealCluster::start(3, 0, data_dir.path());
    let timeout = Duration::from_secs(15);

    let put_outcome = submit_with_retry(cluster.client_addr(0), &put(1, 0, 42, 7), timeout).await;
    assert_eq!(put_outcome, Outcome::Put);

    let replicated = submit_with_retry(cluster.client_addr(2), &get(2, 0, 42), timeout).await;
    assert_eq!(
        replicated,
        Outcome::Get(Some(7)),
        "write must have replicated to replica 2 before we reboot anything"
    );

    // SIGKILL + restart a majority (replicas 1 and 2), identically to
    // `probe_amnesia.sh`. Replica 0 is never touched -- it is this test's
    // "ground truth" for what was actually decided.
    cluster.kill(1);
    cluster.kill(2);
    cluster.spawn(1);
    cluster.spawn(2);

    let via_restarted_1 = submit_with_retry(cluster.client_addr(1), &get(3, 0, 42), timeout).await;
    assert_eq!(
        via_restarted_1,
        Outcome::Get(Some(7)),
        "restarted replica 1 lost the acknowledged write -- issue #36 regression"
    );

    let via_untouched_0 = submit_with_retry(cluster.client_addr(0), &get(4, 0, 42), timeout).await;
    assert_eq!(
        via_untouched_0,
        Outcome::Get(Some(7)),
        "replica 0 (never restarted) must still agree with itself"
    );

    let via_restarted_2 = submit_with_retry(cluster.client_addr(2), &get(5, 0, 42), timeout).await;
    assert_eq!(
        via_restarted_2,
        Outcome::Get(Some(7)),
        "restarted replica 2 lost the acknowledged write -- issue #36 regression"
    );
}

/// Contrast case (matches `scratchpad/probe_single.sh`): restarting only a
/// **minority** (one of three) was never broken -- the two still-live
/// replicas already form a majority throughout, so this passed even before
/// this branch's persistence fix. Kept alongside the majority test so a
/// future regression that breaks *this* easier case (rather than the
/// majority one) still gets caught.
#[tokio::test(flavor = "multi_thread")]
async fn minority_reboot_recovers_too() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let mut cluster = RealCluster::start(3, 0, data_dir.path());
    let timeout = Duration::from_secs(15);

    let put_outcome = submit_with_retry(cluster.client_addr(0), &put(1, 0, 99, 5), timeout).await;
    assert_eq!(put_outcome, Outcome::Put);

    // Only replica 1 goes down -- replicas 0 and 2 remain a live majority
    // the whole time.
    cluster.kill(1);
    cluster.spawn(1);

    let via_restarted_1 = submit_with_retry(cluster.client_addr(1), &get(2, 0, 99), timeout).await;
    assert_eq!(via_restarted_1, Outcome::Get(Some(5)));

    let via_2 = submit_with_retry(cluster.client_addr(2), &get(3, 0, 99), timeout).await;
    assert_eq!(via_2, Outcome::Get(Some(5)));
}
