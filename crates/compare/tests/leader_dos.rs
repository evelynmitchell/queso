// Real wall-clock timing/latency is exactly what this test measures --
// same per-crate-root allow as `queso-net`'s equivalent test modules.
#![allow(clippy::disallowed_methods)]

//! Phase 7.5's headline experiment (issue #35): kill/isolate the fast-path
//! leader mid-run and measure the **availability gap** -- does the cluster
//! stall (as a single-leader protocol like Raft must, until an election
//! timeout elapses) or keep serving through Meerkat/QuePaxa's
//! leaderless-tolerant hedging?
//!
//! This is deliberately the *same fault* `crates/net/tests/nemesis.rs`'s
//! `isolating_the_leader_lets_the_majority_keep_deciding` already exercises
//! (`Nemesis::isolate` on the fixed fast-path leader) -- that test is the
//! qualitative/safety proof; this one adds the quantitative number this
//! phase's comparison methodology asks for: the longest single gap between
//! consecutive completed writes while the leader is down, i.e. exactly the
//! "how long was the cluster unavailable" number a reader would compare
//! against etcd's own election-timeout-bounded stall.
//!
//! **Why this is the primary *comparable* fault, not the nemesis-latency
//! slow-leader scenario:** killing/isolating a leader process is something
//! both systems support identically (`kill -9`/isolate the etcd leader;
//! `Nemesis::isolate` the Queso leader) with no external proxy needed. See
//! `docs/compare-etcd.md` for the byte-for-byte-identical procedure
//! documented for etcd (which this sandbox cannot run -- see that doc's
//! "environment constraint" section) and for the real numbers this test
//! produces on the Queso side.

use std::sync::Arc;
use std::time::{Duration, Instant};

use queso_compare::workload::{run_workload, StopCondition, WorkloadConfig};
use queso_compare::{KvTarget, QuesoTarget};
use queso_net::client::{Client, ClientConfig};
use queso_net::nemesis::{FaultPlan, Nemesis};
use queso_sim::ids::NodeId;

#[path = "support/mod.rs"]
mod support;
use support::spawn_cluster;

/// Generous but bounded -- this test must never hang CI.
const DEADLINE: Duration = Duration::from_secs(20);

#[tokio::test(flavor = "multi_thread")]
async fn leader_isolation_keeps_the_majority_available_with_no_election_style_stall() {
    let nemesis = Arc::new(Nemesis::new(FaultPlan::seeded(77)));
    let client_addrs = spawn_cluster(3, Some(NodeId(0)), Some(Arc::clone(&nemesis)));

    let workload_shape = WorkloadConfig {
        rate: None,
        concurrency: 4,
        read_frac: 0.5,
        keys: 200,
        seed: 5,
    };

    // Phase 1: baseline. Leader fully reachable, ordinary load.
    let baseline_client = Client::with_config(
        client_addrs.clone(),
        ClientConfig {
            attempt_timeout: Duration::from_secs(3),
            max_rounds: 10,
            retry_backoff: Duration::from_millis(20),
            tls: None,
            tls_server_name: None,
        },
    );
    let baseline_target = Arc::new(QuesoTarget::new(baseline_client));
    let baseline_stop = StopCondition {
        deadline: None,
        target_ops: Some(40),
    };
    let baseline_summary = tokio::time::timeout(
        DEADLINE,
        run_workload(baseline_target, workload_shape.clone(), baseline_stop),
    )
    .await
    .expect("baseline phase must not hang");
    assert_eq!(baseline_summary.total_errors, 0, "{baseline_summary:#?}");

    // Phase 2: isolate the fast-path leader (node 0) completely from its
    // peers -- exactly the fault a Raft-style single-leader protocol would
    // need an election timeout to recover from. Drive load at only the two
    // non-leader replicas (proving the *cluster* stays available, not
    // merely that the client's own retry-to-another-replica routed around
    // a dead address), sequentially (one op in flight at a time) so every
    // completion's wall-clock timestamp is a direct "how long was the
    // cluster silent" measurement -- the availability-gap number this test
    // exists to report.
    nemesis.isolate(NodeId(0), [NodeId(0), NodeId(1), NodeId(2)]);
    let degraded_client = Client::with_config(
        vec![client_addrs[1], client_addrs[2]],
        ClientConfig {
            attempt_timeout: Duration::from_secs(3),
            max_rounds: 10,
            retry_backoff: Duration::from_millis(50),
            tls: None,
            tls_server_name: None,
        },
    );
    let degraded_target = Arc::new(QuesoTarget::new(degraded_client));

    let ops = 20usize;
    let deadline = tokio::time::Instant::now() + DEADLINE;
    let isolation_started = Instant::now();
    let mut last_completion = isolation_started;
    let mut max_gap = Duration::ZERO;
    for i in 0..ops {
        let key = 900 + i as u32;
        let attempt = tokio::time::timeout_at(deadline, degraded_target.put(key, i as i64))
            .await
            .unwrap_or_else(|_| {
                panic!("op {i} did not complete before the deadline with the leader isolated")
            });
        attempt.unwrap_or_else(|err| panic!("op {i} failed with the leader isolated: {err:?}"));
        let now = Instant::now();
        max_gap = max_gap.max(now.duration_since(last_completion));
        last_completion = now;
    }
    let isolation_elapsed = isolation_started.elapsed();
    let degraded_throughput = ops as f64 / isolation_elapsed.as_secs_f64();

    eprintln!(
        "leader-isolated: {ops} writes in {isolation_elapsed:?} ({degraded_throughput:.1} ops/sec), \
         max inter-op gap = {max_gap:?} (this is the availability-gap number -- \
         compare against etcd's election-timeout-bounded stall, see docs/compare-etcd.md)"
    );

    // The headline assertion: the *longest single gap* between consecutive
    // completed writes while the leader is isolated stays well under a
    // plausible Raft election-timeout window (etcd's own default election
    // timeout is 1s, with randomized backoff up to 2x that -- see
    // docs/compare-etcd.md's methodology section for the citation). This is
    // Meerkat/QuePaxa's leaderless-tolerant hedging keeping the majority
    // deciding immediately, with no election to wait out.
    assert!(
        max_gap < Duration::from_secs(2),
        "expected no single operation to stall anywhere near an election-timeout \
         window with the leader isolated, but saw a {max_gap:?} gap"
    );

    // Anti-vacuous: prove the leader was genuinely isolated, not that the
    // partition silently no-op'd.
    let stats = nemesis.stats();
    assert!(
        stats.partition_drops > 0,
        "the leader-isolation partition must have actually dropped peer frames \
         crossing the split -- otherwise this test proves nothing about \
         leader-DoS tolerance; got {stats:?}"
    );

    // Phase 3: heal, confirm the cluster (leader included) is fully healthy
    // again -- the isolation was a transient DoS, not a permanent loss.
    nemesis.heal();
    let recovered_client = Client::with_config(
        client_addrs,
        ClientConfig {
            attempt_timeout: Duration::from_secs(3),
            max_rounds: 10,
            retry_backoff: Duration::from_millis(20),
            tls: None,
            tls_server_name: None,
        },
    );
    let recovered_target = Arc::new(QuesoTarget::new(recovered_client));
    let recovered_stop = StopCondition {
        deadline: None,
        target_ops: Some(40),
    };
    let recovered_summary = tokio::time::timeout(
        DEADLINE,
        run_workload(recovered_target, workload_shape, recovered_stop),
    )
    .await
    .expect("post-heal phase must not hang");
    assert_eq!(recovered_summary.total_errors, 0, "{recovered_summary:#?}");

    eprintln!(
        "baseline:  {}\nrecovered: {}",
        baseline_summary.to_text(),
        recovered_summary.to_text()
    );
}
