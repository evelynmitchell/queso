//! Phase 9.2 (issue #56): the chain hook across a **real process restart**.
//!
//! `crate::chain`'s fold is volatile -- a restarted `queso-node` starts at
//! `(0, GENESIS)` and re-folds from slot 0 on its first pass, which is sound
//! only because the applied log itself is durable. That claim is the sort
//! this project does not leave argued-from-code-reading: if it were wrong, a
//! rebooted replica would publish checkpoints computed over a *truncated*
//! history, silently disagreeing with its peers at every shared `n`, and a
//! 9.2 soak would report divergence in a cluster that never diverged.
//!
//! So this test kills a real replica with `SIGKILL`, restarts it against the
//! same `--data-dir`, and requires the reborn process to republish the same
//! hashes for the slots it applied *before* the crash.
//!
//! Like `tests/restart_recovery.rs`, this spawns the actual `queso-node`
//! binary as independent OS processes -- an in-process "drop the `SmrNode`
//! and rebuild it" simulation would leave the real boot-time reload path
//! (the thing under test) untested.
//!
//! It used to keep its own copy of that spawner, on the grounds that this
//! one needs status listeners and `--chain-checkpoints`. Issue #40 is why
//! it no longer does: the shared `support::ProcCluster` retries a boot that
//! dies immediately, working around `free_addr`'s bind-then-drop race, and
//! a private copy is a copy that quietly does not get that protection.
//! Threading two optional flags through one spawner is a smaller cost than
//! maintaining two.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use queso_net::client;
use queso_smr::{ClientId, Command as KvCommand, Outcome};

mod support;
use support::ProcCluster;

const SPACING: u64 = 2;

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

fn put(seq: u64, value: i64) -> KvCommand {
    KvCommand::Put {
        client: ClientId(3),
        seq,
        key: 1,
        value,
    }
}

/// `GET <path>` over a bare TCP stream, retrying until the (possibly
/// just-restarted) status server is accepting.
async fn http_get_with_retry(addr: SocketAddr, path: &str, timeout: Duration) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let attempt = async {
            let mut stream = TcpStream::connect(addr).await.ok()?;
            let request = format!("GET {path} HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n");
            stream.write_all(request.as_bytes()).await.ok()?;
            let mut raw = String::new();
            stream.read_to_string(&mut raw).await.ok()?;
            let body = raw.split_once("\r\n\r\n").map(|(_, b)| b.to_string())?;
            Some(body)
        }
        .await;

        if let Some(body) = attempt {
            if !body.trim().is_empty() {
                return body;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("status server at {addr} never answered {path}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Parse `/chain` into `n -> h`.
async fn fetch_checkpoints(addr: SocketAddr, timeout: Duration) -> BTreeMap<u64, u64> {
    let body = http_get_with_retry(addr, "/chain", timeout).await;
    let json: serde_json::Value =
        serde_json::from_str(&body).unwrap_or_else(|err| panic!("parse /chain: {err}\n{body}"));
    json["checkpoints"]
        .as_array()
        .expect("checkpoints array")
        .iter()
        .map(|entry| {
            let n = entry["n"].as_u64().expect("checkpoint n");
            let text = entry["h"].as_str().expect("checkpoint h");
            let h = u64::from_str_radix(text.trim_start_matches("0x"), 16).expect("hex hash");
            (n, h)
        })
        .collect()
}

/// Poll `/chain` until the replica has published at least one checkpoint, or
/// the deadline passes. The status listener is up slightly before the
/// driver's boot-time fold runs, so a fetch immediately after spawn can
/// legitimately see an empty table; this waits that window out rather than
/// racing it.
async fn wait_for_checkpoints(addr: SocketAddr, timeout: Duration) -> BTreeMap<u64, u64> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let table = fetch_checkpoints(addr, timeout).await;
        if !table.is_empty() {
            return table;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("replica at {addr} never published a checkpoint");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A replica killed with `SIGKILL` and restarted against its own data
/// directory must republish, for every slot it had applied before the crash,
/// exactly the hashes it published before -- and exactly the hashes its
/// never-restarted peers publish.
///
/// This is what makes the volatile fold sound: the chain is recomputed from
/// the durable applied log, so a reboot loses the running hash but not the
/// history it summarizes.
#[tokio::test(flavor = "multi_thread")]
async fn a_restarted_replica_republishes_the_same_checkpoints() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let mut cluster = ProcCluster::start_with_status(3, 0, data_dir.path(), Some(SPACING));
    let timeout = Duration::from_secs(20);

    // Enough closed-loop writes to cross several checkpoints.
    for i in 0..8u64 {
        assert_eq!(
            submit_with_retry(cluster.client_addr(0), &put(i, i as i64 + 100), timeout).await,
            Outcome::Put,
            "write {i} should be applied"
        );
    }

    let before = fetch_checkpoints(cluster.status_addr(0), timeout).await;
    assert!(
        before.len() >= 3,
        "expected several checkpoints before the restart, got {before:?}"
    );

    // Kill and reboot the leader against the same data dir: a fresh
    // process, a blank heap, and therefore a chain folder starting from
    // genesis with nothing but the durable log to rebuild from.
    cluster.kill(0);
    cluster.spawn(0);

    // Deliberately submit nothing here: whatever the rebooted process
    // publishes now was folded from its durable applied log, not from
    // commands it saw after coming back.
    //
    // Note what this does *not* isolate. The driver folds both at boot and
    // after each batch, and a rebooted replica starts exchanging peer
    // messages (its own catch-up probe among them) immediately -- so this
    // assertion cannot tell which of the two folds produced the table, and
    // it still passes with the boot-time fold removed. What it does pin,
    // and what actually matters, is that the refold covers the *pre-crash*
    // slots: with the fold starting from the current frontier instead of
    // from genesis, this call fails outright.
    let after = wait_for_checkpoints(cluster.status_addr(0), timeout).await;

    let mut rechecked = 0usize;
    for (n, h) in &before {
        let now = after.get(n).unwrap_or_else(|| {
            panic!(
                "after restart, replica 0 no longer publishes n={n} -- it re-folded over a \
                 truncated history instead of its durable applied log. before={before:?} \
                 after={after:?}"
            )
        });
        assert_eq!(
            now, h,
            "after restart, replica 0's hash at n={n} changed: the refolded chain does not \
             match what it published before the crash"
        );
        rechecked += 1;
    }
    assert!(
        rechecked >= 3,
        "only {rechecked} pre-crash checkpoints were rechecked -- too few to establish the \
         refold property"
    );

    // ...and it still agrees with a replica that never restarted, which is
    // the property a conformance run actually depends on.
    //
    // Replica 1 needs traffic of its own first. A Queso replica learns a
    // slot's decision by *participating* -- recording for someone else's
    // proposal does not make it apply anything -- so a follower that has
    // only ever been a recorder has an empty applied log and therefore an
    // empty checkpoint table. (9.1 found this in the sim; it holds just the
    // same for real processes, and a 9.2 soak that drives load at a single
    // endpoint would compare nothing at all.)
    for i in 0..6u64 {
        let addr = cluster.client_addr((i as usize) % cluster.replicas());
        assert_eq!(
            submit_with_retry(addr, &put(200 + i, i as i64), timeout).await,
            Outcome::Put
        );
    }

    let after = fetch_checkpoints(cluster.status_addr(0), timeout).await;
    let peer = fetch_checkpoints(cluster.status_addr(1), timeout).await;
    let mut cross_checked = 0usize;
    for (n, h) in &after {
        if let Some(peer_h) = peer.get(n) {
            assert_eq!(
                h, peer_h,
                "restarted replica 0 and never-restarted replica 1 disagree at n={n}"
            );
            cross_checked += 1;
        }
    }
    assert!(
        cross_checked >= 1,
        "the restarted replica shared no checkpoint with its peer, so nothing was actually \
         compared across the restart. after={after:?} peer={peer:?}"
    );
}
