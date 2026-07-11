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

use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use queso_net::client;
use queso_smr::{ClientId, Command as KvCommand, Outcome};

/// The `queso-node` binary under test, built by Cargo before this
/// integration test binary runs (`CARGO_BIN_EXE_<bin-target-name>` is set
/// automatically for any binary target in the same package).
fn node_bin() -> &'static str {
    env!("CARGO_BIN_EXE_queso-node")
}

/// An ephemeral, currently-free localhost port -- see `tests/cluster.rs`'s
/// identical helper for the caveats (small accept-immediately-drop race,
/// standard practice in tests).
fn free_addr() -> SocketAddr {
    let listener = StdTcpListener::bind(("127.0.0.1", 0)).expect("bind an ephemeral port");
    listener.local_addr().expect("read back the bound address")
}

/// One real 3-node `queso-node` cluster, each replica a genuine OS process,
/// with helpers to `SIGKILL` and respawn any subset of them against the
/// *same* on-disk `--data-dir` (so a respawned node's `queso-node` process
/// goes through exactly the same boot-time reload path a real redeploy
/// would). Kills every still-running child on drop, so a failing assertion
/// mid-test never leaks orphan processes (mirrors `probe_amnesia.sh`'s own
/// `trap cleanup EXIT`).
struct RealCluster {
    n: usize,
    peer_addrs: Vec<SocketAddr>,
    client_addrs: Vec<SocketAddr>,
    leader: u32,
    data_dir: std::path::PathBuf,
    children: Vec<Option<Child>>,
}

impl RealCluster {
    fn new(n: usize, leader: u32, data_dir: &Path) -> Self {
        let peer_addrs: Vec<SocketAddr> = (0..n).map(|_| free_addr()).collect();
        let client_addrs: Vec<SocketAddr> = (0..n).map(|_| free_addr()).collect();
        let mut cluster = Self {
            n,
            peer_addrs,
            client_addrs,
            leader,
            data_dir: data_dir.to_path_buf(),
            children: (0..n).map(|_| None).collect(),
        };
        for i in 0..n {
            cluster.spawn(i);
        }
        cluster
    }

    /// Boot (or reboot) replica `i` as a fresh `queso-node` process,
    /// pointed at this cluster's shared `--data-dir` -- the same directory
    /// every previous incarnation of replica `i` wrote its durable
    /// snapshot into, so this exercises the real reload-on-boot path.
    fn spawn(&mut self, i: usize) {
        assert!(
            self.children[i].is_none(),
            "replica {i} is already running -- kill it first"
        );
        let mut cmd = Command::new(node_bin());
        cmd.arg("--id")
            .arg(i.to_string())
            .arg("--seed")
            .arg((9_000 + i as u64).to_string())
            .arg("--listen")
            .arg(self.peer_addrs[i].to_string())
            .arg("--client-listen")
            .arg(self.client_addrs[i].to_string())
            .arg("--leader")
            .arg(self.leader.to_string())
            .arg("--tick-ms")
            .arg("5")
            .arg("--data-dir")
            .arg(&self.data_dir);
        for j in 0..self.n {
            cmd.arg("--peer").arg(format!("{j}={}", self.peer_addrs[j]));
        }
        // Quiet by default -- these tests assert on the client protocol's
        // observable behavior, not log output; pass through `RUST_LOG` if a
        // human wants to watch a failure locally.
        cmd.stdout(Stdio::null()).stderr(Stdio::null());
        let child = cmd.spawn().expect("spawn queso-node subprocess");
        self.children[i] = Some(child);
    }

    /// `SIGKILL` replica `i` and reap it -- `Child::wait` blocks until the
    /// OS has fully torn the process down (including releasing its
    /// listening sockets), so a subsequent `spawn` rebinding the same ports
    /// never races the kernel's own cleanup the way an in-process
    /// task-abort simulation would.
    fn kill(&mut self, i: usize) {
        let mut child = self.children[i]
            .take()
            .unwrap_or_else(|| panic!("replica {i} is not running"));
        child.kill().expect("SIGKILL replica");
        child.wait().expect("reap killed replica");
    }

    fn client_addr(&self, i: usize) -> SocketAddr {
        self.client_addrs[i]
    }
}

impl Drop for RealCluster {
    fn drop(&mut self) {
        for slot in &mut self.children {
            if let Some(mut child) = slot.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}

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
    let mut cluster = RealCluster::new(3, 0, data_dir.path());
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
    let mut cluster = RealCluster::new(3, 0, data_dir.path());
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
