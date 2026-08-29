//! `docs/tutorial.md`, run as a test (issue #81).
//!
//! The tutorial's contract with its reader is that every step actually
//! works on the commit they cloned. A tutorial that rots fails a newcomer
//! at the exact moment they have the least context to recover, so this
//! test walks the tutorial's arc against real `queso-node` OS processes,
//! driving the real `queso-admin` binary — the same command surface the
//! page tells a human to type:
//!
//! 1. boot three replicas, `status`, `put`/`get`;
//! 2. `SIGKILL` the leader, write through the failure, read old data back;
//! 3. restart the leader, spread writes, require `/chain` agreement
//!    between the restarted replica and a never-killed one;
//! 4. `SIGKILL` a majority, require the next write to *fail*;
//! 5. restore the majority, require the write to succeed and every
//!    acknowledged key to still be there.
//!
//! What this pins is the tutorial's *arc and command surface* — flags,
//! subcommands, success/failure shapes. It does not string-match the
//! page's prose or its captured output blocks; those can drift in
//! wording without lying. If a flag is renamed, an output format changes
//! meaning, or any step of the arc stops behaving as narrated, this
//! breaks before the page does the lying.
//!
//! Port/flag details differ from the page in one way only: the tutorial
//! uses fixed localhost ports a human can type, while this test draws
//! free ports so parallel test binaries cannot collide (and adds
//! explicit `--seq` values, which the admin README requires of
//! scripted rapid-fire usage — interactive use relies on wall-clock
//! defaults instead).

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::process::Output;
use std::time::Duration;

mod support;
use support::ProcCluster;

/// The tutorial's checkpoint spacing (`--chain-checkpoints 8`).
const SPACING: u64 = 8;

/// Run the real `queso-admin` binary — the tutorial's client — and hand
/// back its exit status and outputs.
fn admin(args: &[&str]) -> Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_queso-admin"))
        .args(args)
        .output()
        .expect("spawn queso-admin")
}

/// `put <key> <value>` against `addrs`, with an explicit fresh `--seq`
/// (scripted usage, per `queso_net::admin`'s docs). Returns the raw
/// `Output`; callers assert success or failure as the step demands.
fn admin_put(addrs: &[SocketAddr], seq: u64, key: u64, value: i64) -> Output {
    let seq = seq.to_string();
    let key = key.to_string();
    let value = value.to_string();
    let mut args = vec!["put", &key, &value, "--seq", &seq];
    let addr_strings: Vec<String> = addrs.iter().map(|a| a.to_string()).collect();
    for addr in &addr_strings {
        args.push("--addr");
        args.push(addr);
    }
    admin(&args)
}

/// `get <key>` against `addrs`, asserting the command succeeds and
/// returning its stdout (e.g. `Get(Some(777))`).
fn admin_get(addrs: &[SocketAddr], seq: u64, key: u64) -> String {
    let seq = seq.to_string();
    let key = key.to_string();
    let mut args = vec!["get", &key, "--seq", &seq];
    let addr_strings: Vec<String> = addrs.iter().map(|a| a.to_string()).collect();
    for addr in &addr_strings {
        args.push("--addr");
        args.push(addr);
    }
    let out = admin(&args);
    assert!(
        out.status.success(),
        "queso-admin get {key} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("utf8 stdout")
}

/// Retry a `put` until it succeeds or `timeout` elapses — the tutorial's
/// "within a couple of seconds, retry the write" after restoring the
/// majority. Each attempt uses a fresh `seq` so a retry is never dropped
/// by the server-side A6 dedup as a duplicate of a failed attempt.
fn put_with_retry(addrs: &[SocketAddr], seq: &mut u64, key: u64, value: i64, timeout: Duration) {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        *seq += 1;
        let out = admin_put(addrs, *seq, key, value);
        if out.status.success() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "put {key}={value} never succeeded after the cluster was restored: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// `GET <path>` over a bare TCP stream, retrying until the (possibly
/// just-restarted) status server answers. Same shape as
/// `tests/chain_restart.rs`'s helper.
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

#[tokio::test(flavor = "multi_thread")]
async fn the_tutorial_arc_still_runs() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let mut cluster = ProcCluster::start_with_status(3, 0, data_dir.path(), Some(SPACING));
    let timeout = Duration::from_secs(20);
    let all: Vec<SocketAddr> = (0..3).map(|i| cluster.client_addr(i)).collect();
    let mut seq: u64 = 0;

    // Step 3: prove it is a cluster. `status` sees every replica; a write
    // round-trips.
    let status_args: Vec<String> = (0..3)
        .flat_map(|i| {
            [
                "--status-addr".to_string(),
                cluster.status_addr(i).to_string(),
            ]
        })
        .collect();
    let mut args = vec!["status"];
    args.extend(status_args.iter().map(String::as_str));
    let out = admin(&args);
    assert!(out.status.success(), "queso-admin status failed");
    let text = String::from_utf8_lossy(&out.stdout).to_string();
    assert!(
        text.contains("3/3 replicas reachable"),
        "status should see all three replicas:\n{text}"
    );

    put_with_retry(&all, &mut seq, 42, 777, timeout);
    seq += 1;
    assert_eq!(admin_get(&all, seq, 42).trim(), "Get(Some(777))");

    // Step 4: SIGKILL the leader; the cluster keeps answering.
    cluster.kill(0);
    put_with_retry(&all, &mut seq, 7, 1234, timeout);
    seq += 1;
    assert_eq!(
        admin_get(&all, seq, 42).trim(),
        "Get(Some(777))",
        "a pre-crash write must still read back with the leader dead"
    );

    // Step 5: restart the leader against the same --data-dir, spread
    // writes so every replica participates, and require the restarted
    // replica and a never-killed one to agree at every shared /chain
    // checkpoint (retrying while checkpoints accumulate: replicas lag by
    // design, so the shared entry can take a few write batches to exist).
    cluster.spawn(0);
    let deadline = tokio::time::Instant::now() + timeout;
    let mut key = 100u64;
    let (shared, restarted, survivor) = loop {
        for offset in 0..3usize {
            let rotated: Vec<SocketAddr> = (0..3)
                .map(|i| cluster.client_addr((i + offset) % 3))
                .collect();
            key += 1;
            put_with_retry(&rotated, &mut seq, key, key as i64, timeout);
        }
        let restarted = fetch_checkpoints(cluster.status_addr(0), timeout).await;
        let survivor = fetch_checkpoints(cluster.status_addr(1), timeout).await;
        let shared: Vec<u64> = restarted
            .keys()
            .filter(|n| survivor.contains_key(n))
            .copied()
            .collect();
        if !shared.is_empty() {
            break (shared, restarted, survivor);
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the restarted replica and a survivor never published a shared /chain checkpoint: \
             restarted={restarted:?} survivor={survivor:?}"
        );
    };
    for n in &shared {
        assert_eq!(
            restarted[n], survivor[n],
            "restarted replica 0 and never-killed replica 1 disagree at n={n} -- \
             the tutorial's Agreement cross-check would show a divergence"
        );
    }

    // Step 6: SIGKILL a majority; the next write must FAIL. The failure
    // case matters as much as the success case: a lone replica that
    // answered writes here would be choosing divergence over safety.
    cluster.kill(1);
    cluster.kill(2);
    seq += 1;
    let out = admin_put(&all, seq, 99, 1);
    assert!(
        !out.status.success(),
        "a write with only 1 of 3 replicas alive must not be acknowledged, but got: {}",
        String::from_utf8_lossy(&out.stdout)
    );

    // Step 7: restore the majority; the write goes through and every
    // acknowledged key -- from before any failure, from during the
    // leader outage, and the retried one -- is still there.
    cluster.spawn(1);
    cluster.spawn(2);
    put_with_retry(&all, &mut seq, 99, 1, timeout);
    for (key, expect) in [(42u64, 777i64), (7, 1234), (99, 1)] {
        seq += 1;
        assert_eq!(
            admin_get(&all, seq, key).trim(),
            format!("Get(Some({expect}))"),
            "key {key} did not survive the full tutorial arc"
        );
    }
}
