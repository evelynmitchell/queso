// Real wall-clock timing is exactly what this test measures (it exercises
// the same `queso-bench` metrics path -- see the module docs) -- same
// per-crate-root allow as `queso-net`'s `src/lib.rs` and
// `src/bin/queso-bench.rs`, needed again here since each `tests/*.rs` file
// is its own crate root.
#![allow(clippy::disallowed_methods)]

//! Phase 7.2's acceptance test: boot a real 3-node localhost cluster (the
//! same in-process/real-TCP harness `tests/cluster.rs` uses, factored into
//! `tests/support`) and drive it with `queso_net::client::Client` the same
//! way `queso-bench` does -- concurrent workers, a read/write mix, a
//! `queso_net::metrics::Recorder` -- proving the whole client -> cluster ->
//! metrics path end-to-end and that it produces non-trivial throughput and
//! a sane latency histogram, not just that one request round-trips.
//!
//! This intentionally exercises the same pieces `queso-bench`'s binary
//! wires together (`Client`, `metrics::Recorder`) rather than shelling out
//! to the binary itself, so a failure here points straight at a library
//! bug instead of a CLI/process-management one.

use std::sync::Arc;
use std::time::{Duration, Instant};

use queso_net::client::Client;
use queso_net::metrics::{OpKind, Recorder, Sample};
use queso_sim::ids::NodeId;
use queso_smr::{ClientId, Command};
use tokio::task::JoinSet;

#[path = "support/mod.rs"]
mod support;
use support::spawn_cluster;

/// Drive `total_ops` operations (split evenly read/write across
/// `concurrency` workers, each its own `ClientId`/session per A6) through
/// `client`, spread over `keys` keys, and return the resulting
/// throughput/latency [`queso_net::metrics::Summary`].
async fn run_workload(
    client: Arc<Client>,
    concurrency: u32,
    ops_per_worker: u64,
    keys: u32,
) -> queso_net::metrics::Summary {
    let (sample_tx, mut sample_rx) = tokio::sync::mpsc::unbounded_channel::<Sample>();
    let collector = tokio::spawn(async move {
        let mut recorder = Recorder::new();
        while let Some(sample) = sample_rx.recv().await {
            recorder.record(sample);
        }
        recorder
    });

    let start = Instant::now();
    let mut workers = JoinSet::new();
    for worker in 0..concurrency {
        let client = Arc::clone(&client);
        let sample_tx = sample_tx.clone();
        workers.spawn(async move {
            let client_id = ClientId(worker);
            for seq in 0..ops_per_worker {
                let key = (worker as u64 * ops_per_worker + seq) as u32 % keys.max(1);
                // Alternate reads and writes so both histograms get real
                // samples, per the acceptance criteria's "distinguish read
                // vs write latency" ask.
                let (kind, command) = if seq % 2 == 0 {
                    (
                        OpKind::Write,
                        Command::Put {
                            client: client_id,
                            seq,
                            key,
                            value: seq as i64,
                        },
                    )
                } else {
                    (
                        OpKind::Read,
                        Command::Get {
                            client: client_id,
                            seq,
                            key,
                        },
                    )
                };
                let op_start = Instant::now();
                let result = client.submit(&command).await;
                let _ = sample_tx.send(Sample {
                    kind,
                    latency: op_start.elapsed(),
                    ok: result.is_ok(),
                });
            }
        });
    }
    while workers.join_next().await.is_some() {}
    let elapsed = start.elapsed();
    drop(sample_tx);

    let recorder = collector.await.expect("collector task panicked");
    recorder.summarize(elapsed)
}

#[tokio::test(flavor = "multi_thread")]
async fn queso_bench_style_load_against_a_real_cluster_produces_sane_metrics() {
    let client_addrs = spawn_cluster(3, Some(NodeId(0)));

    // The cluster's peer connections/leader election are still settling
    // right after `spawn_cluster` returns (same as `tests/cluster.rs`'s
    // `submit_with_retry` accounts for) -- `Client`'s own
    // retry-to-another-replica plus a generous per-attempt timeout below
    // covers that without a separate readiness probe.
    let client = Arc::new(Client::with_config(
        client_addrs,
        queso_net::client::ClientConfig {
            attempt_timeout: Duration::from_secs(5),
            max_rounds: 20,
            retry_backoff: Duration::from_millis(50),
            ..queso_net::client::ClientConfig::default()
        },
    ));

    let concurrency = 12;
    let ops_per_worker = 30;
    let keys = 50;
    let summary = run_workload(client, concurrency, ops_per_worker, keys).await;

    let expected_total = concurrency as u64 * ops_per_worker;
    assert_eq!(
        summary.total_ops, expected_total,
        "every submitted op should have produced exactly one sample"
    );
    assert_eq!(
        summary.total_errors, 0,
        "a healthy 3-node cluster with a live majority should serve every op \
         (Client's retry-to-another-replica should absorb any transient \
         connection races during cluster startup): {summary:#?}"
    );

    // Non-trivial throughput: the whole point of this test is proving the
    // client -> cluster -> metrics path actually moves data, not just that
    // it doesn't crash.
    assert!(
        summary.throughput_ops_per_sec > 0.0,
        "expected positive throughput, got {summary:#?}"
    );

    // A sane latency histogram: every completed op recorded a positive
    // latency, and percentiles are monotonic (p50 <= p90 <= p99 <= max) --
    // exactly what `queso-bench --output json/csv/text` reports.
    for stats in [&summary.reads, &summary.writes, &summary.overall] {
        assert!(stats.count > 0, "expected samples in {stats:#?}");
        assert!(stats.p50_us >= 1, "{stats:#?}");
        assert!(stats.p50_us <= stats.p90_us, "{stats:#?}");
        assert!(stats.p90_us <= stats.p99_us, "{stats:#?}");
        assert!(stats.p99_us <= stats.max_us, "{stats:#?}");
    }
    assert_eq!(
        summary.reads.count + summary.writes.count,
        summary.overall.count
    );

    eprintln!(
        "queso-bench-style smoke test summary:\n{}",
        summary.to_text()
    );
}
