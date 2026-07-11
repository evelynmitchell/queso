// Real wall-clock timing/latency is exactly what this test measures --
// same per-crate-root allow as `queso-net`'s equivalent test modules.
#![allow(clippy::disallowed_methods)]

//! Phase 7.5's normal-case acceptance test: drive
//! [`queso_compare::QuesoTarget`] through [`queso_compare::run_workload`]
//! against a real, in-process, real-TCP 3-node Queso cluster, and assert
//! the run produces the same shape of output `crates/net/tests/bench.rs`
//! (Phase 7.2's own acceptance test) does -- proving this crate's
//! independently-implemented workload runner produces results consistent
//! with, and diffable against, `queso-bench`'s own. See
//! `docs/compare-etcd.md` for the numbers a real (longer, `--release`)
//! `queso-compare --target queso` run produced.

use std::sync::Arc;
use std::time::Duration;

use queso_compare::workload::{run_workload, StopCondition, WorkloadConfig};
use queso_compare::QuesoTarget;
use queso_net::client::{Client, ClientConfig};
use queso_sim::ids::NodeId;

#[path = "support/mod.rs"]
mod support;
use support::spawn_cluster;

#[tokio::test(flavor = "multi_thread")]
async fn queso_target_produces_a_sane_summary_against_a_real_cluster() {
    let client_addrs = spawn_cluster(3, Some(NodeId(0)), None);
    let client = Client::with_config(
        client_addrs,
        ClientConfig {
            attempt_timeout: Duration::from_secs(3),
            max_rounds: 10,
            retry_backoff: Duration::from_millis(20),
        },
    );
    let target = Arc::new(QuesoTarget::new(client));

    let cfg = WorkloadConfig {
        rate: None,
        concurrency: 8,
        read_frac: 0.5,
        keys: 100,
        seed: 11,
    };
    let stop = StopCondition {
        deadline: None,
        target_ops: Some(120),
    };

    let summary = tokio::time::timeout(Duration::from_secs(30), run_workload(target, cfg, stop))
        .await
        .expect("normal-case workload must not hang");

    eprintln!("queso normal-case summary:\n{}", summary.to_text());

    assert!(summary.total_ops >= 120, "{summary:#?}");
    assert_eq!(summary.total_errors, 0, "{summary:#?}");
    assert!(summary.throughput_ops_per_sec > 0.0);
    // Same monotonic-histogram property `crates/net/tests/bench.rs` asserts
    // on `queso-bench`'s own `Summary` -- this crate reuses the exact same
    // `queso_net::metrics` type, so this is really the same assertion.
    for stats in [&summary.overall, &summary.reads, &summary.writes] {
        assert!(stats.p50_us <= stats.p90_us, "{summary:#?}");
        assert!(stats.p90_us <= stats.p99_us, "{summary:#?}");
        assert!(stats.p99_us <= stats.max_us, "{summary:#?}");
    }

    // The output this crate emits is exactly `queso_net::metrics::Summary`
    // -- `--output json`/`csv` on `queso-compare` and `queso-bench` share
    // the same schema by construction, not by convention, so a real
    // Queso-vs-etcd diff needs no field-mapping/translation step.
    let json = summary.to_json();
    let parsed: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");
    assert!(parsed["throughput_ops_per_sec"].is_number());
    assert!(parsed["overall"]["p99_us"].is_number());
}
