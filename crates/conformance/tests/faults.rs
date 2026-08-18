//! Chain-of-Blocks under fault: crash, restart, and slow replicas, across
//! seeds, with the safety and liveness verdicts asserted throughout.
//!
//! # What this does and does not establish
//!
//! These faults are injected into the **in-process simulator**, which is
//! the same fault surface `queso-smr`'s existing crash/restart tests
//! already cover. So this file does *not* close the sim↔real gap that
//! Phase 9 (#54) exists for -- Phase 9.2 (#56), which drives real
//! `queso-node` processes under sustained turbulence, is what does that.
//!
//! What it establishes is that the CoB workload and observers behave
//! correctly *while faults are in flight*: that they distinguish a replica
//! that is legitimately lagging from one that is stuck, that they do not
//! report spurious divergence when replicas are at wildly different
//! frontiers, and that the liveness verdict is only taken at a point where
//! it means something. Those are exactly the judgement calls 9.2 will
//! depend on, and getting them wrong there would look like a bug in Queso
//! rather than a bug in the harness.

use queso_conformance::observer::Observer;
use queso_conformance::source::{Observability, SimCluster};
use queso_conformance::workload::{converge, settle, CobWorkload};
use queso_conformance::CobTarget;
use queso_sim::ids::NodeId;
use queso_sim::scheduler::{ContentObliviousAdversary, SchedulerKind};
use queso_smr::SmrCluster;

fn target(n: usize, seed: u64, drop_probability: f64) -> SimCluster {
    let adversary = ContentObliviousAdversary::new(1, 8).with_drop_probability(drop_probability);
    SimCluster::new(
        SmrCluster::new(seed, SchedulerKind::Oblivious(Box::new(adversary)), n),
        Observability::FullPrefix,
    )
}

/// Submit `count` commands one at a time, advancing and polling between
/// each, so operations genuinely overlap with whatever fault is in force.
fn drive(
    target: &mut SimCluster,
    workload: &mut CobWorkload,
    observer: &mut Observer,
    count: usize,
    advance: u64,
) {
    for _ in 0..count {
        let command = workload.next_command();
        target.submit(command);
        target.advance(advance);
        for sample in target.poll_samples() {
            observer.observe(sample);
        }
    }
}

/// The safety verdict, plus the anti-vacuity floor that keeps it honest.
fn assert_safe(observer: &Observer, min_comparisons: u64) {
    assert!(
        observer.divergences().is_empty(),
        "divergence under fault:\n{}",
        observer.render_report()
    );
    assert!(
        observer.comparisons() >= min_comparisons,
        "only {} cross-replica comparisons (wanted >= {min_comparisons}) -- the \
         'no divergence' verdict would be vacuous:\n{}",
        observer.comparisons(),
        observer.render_report()
    );
}

#[test]
fn crash_and_restart_cycles_never_diverge() {
    for seed in [1, 2, 3, 5, 8] {
        let mut target = target(5, seed, 0.1);
        let mut workload = CobWorkload::new(seed ^ 0xfa17);
        let mut observer = Observer::new();
        let replicas = target.replicas();

        drive(&mut target, &mut workload, &mut observer, 10, 400);

        // f <= (n-1)/2 = 2 for n=5: crash two, keep working, restart them.
        for victim in [replicas[1], replicas[3]] {
            target.cluster_mut().crash(victim);
            drive(&mut target, &mut workload, &mut observer, 8, 800);
        }
        for victim in [replicas[1], replicas[3]] {
            target.cluster_mut().restart(victim);
            drive(&mut target, &mut workload, &mut observer, 8, 800);
        }

        converge(&mut target, &mut workload, &mut observer, 4, 200_000);

        assert_safe(&observer, 30);

        // Liveness, judged only after every replica is back and has had
        // traffic: nobody may be frozen behind the frontier.
        let stalls = observer.stalls(target.now(), 200);
        assert!(
            stalls.is_empty(),
            "seed {seed}: replicas still stalled after restart and convergence: \
             {stalls:?}\n{}",
            observer.render_report()
        );
    }
}

#[test]
fn a_slow_replica_lags_without_diverging_or_stalling() {
    for seed in [11, 12, 13] {
        let mut target = target(3, seed, 0.05);
        let mut workload = CobWorkload::new(seed ^ 0x5104);
        let mut observer = Observer::new();
        let slowpoke = target.replicas()[2];

        target.cluster_mut().set_slow(slowpoke, 20);
        drive(&mut target, &mut workload, &mut observer, 20, 600);

        // While slow, it is expected to be *behind*. That must not be
        // mistaken for divergence.
        assert_safe(&observer, 20);

        target.cluster_mut().clear_slow(slowpoke);
        converge(&mut target, &mut workload, &mut observer, 4, 200_000);

        assert_safe(&observer, 30);
        let stalls = observer.stalls(target.now(), 200);
        assert!(
            stalls.is_empty(),
            "seed {seed}: the slow replica should have caught up once un-slowed: \
             {stalls:?}\n{}",
            observer.render_report()
        );
    }
}

#[test]
fn a_lossy_link_never_produces_divergence() {
    // A heavy drop rate: many proposals fail, which the CoB doc expects
    // ("proposals may fail under fault"). Safety must hold regardless.
    for seed in [21, 22, 23] {
        let mut target = target(5, seed, 0.4);
        let mut workload = CobWorkload::new(seed ^ 0x1055);
        let mut observer = Observer::new();

        drive(&mut target, &mut workload, &mut observer, 25, 1_000);
        converge(&mut target, &mut workload, &mut observer, 5, 300_000);

        assert_safe(&observer, 20);
    }
}

#[test]
fn a_replica_crashed_for_the_whole_run_is_reported_stalled_not_diverged() {
    // The distinction that matters most for 9.2's verdicts: a replica that
    // never participates is a *liveness* observation about that replica,
    // never a safety violation about the cluster.
    let mut target = target(3, 33, 0.1);
    let mut workload = CobWorkload::new(0xdead);
    let mut observer = Observer::new();
    let victim = target.replicas()[2];

    target.cluster_mut().crash(victim);
    drive(&mut target, &mut workload, &mut observer, 25, 800);
    settle(&mut target, &mut observer, 200_000);

    assert!(
        observer.divergences().is_empty(),
        "a permanently-crashed replica is not divergence:\n{}",
        observer.render_report()
    );
    let stalls = observer.stalls(target.now(), 200);
    assert_eq!(
        stalls.len(),
        1,
        "expected exactly the crashed replica to be stalled; got {stalls:?}\n{}",
        observer.render_report()
    );
    assert_eq!(stalls[0].replica, victim);

    // ...and the surviving majority kept the log moving, so the result is
    // not "nothing happened anywhere".
    assert!(
        observer.cluster_frontier() >= 15,
        "the surviving majority should have kept making progress; frontier {}\n{}",
        observer.cluster_frontier(),
        observer.render_report()
    );
}

#[test]
fn the_observer_agrees_with_the_clusters_own_applied_logs() {
    // Cross-check the whole apparatus against ground truth: whatever the
    // observer believes each replica's chain to be, recomputing that chain
    // directly from the replica's applied log must agree. This is what
    // stops a bug in the source (a mis-folded chain, a stale cursor) from
    // masquerading as a clean conformance run.
    let mut target = target(5, 44, 0.15);
    let mut workload = CobWorkload::new(0xc0ffee);
    let mut observer = Observer::new();
    let replicas = target.replicas();

    drive(&mut target, &mut workload, &mut observer, 15, 500);
    target.cluster_mut().crash(replicas[0]);
    drive(&mut target, &mut workload, &mut observer, 10, 800);
    target.cluster_mut().restart(replicas[0]);
    converge(&mut target, &mut workload, &mut observer, 5, 200_000);

    for (replica, observed) in observer.latest_states() {
        let truth = target.true_state(replica);
        assert_eq!(
            observed,
            truth,
            "{replica}: observer's final state disagrees with the replica's own \
             applied log -- the harness itself is wrong\n{}",
            observer.render_report()
        );
    }

    // And the ground-truth logs must be pairwise prefix-consistent, checked
    // directly rather than through the observer, so this assertion cannot
    // be satisfied by an observer that simply never looked.
    let logs: Vec<Vec<queso_smr::Command>> = replicas
        .iter()
        .map(|&r| target.cluster().applied_log(r))
        .collect();
    for i in 0..logs.len() {
        for j in (i + 1)..logs.len() {
            for (slot, (a, b)) in logs[i].iter().zip(&logs[j]).enumerate() {
                assert_eq!(
                    a, b,
                    "P5/P6: replicas {} and {} disagree at slot {slot}",
                    replicas[i], replicas[j]
                );
            }
        }
    }
}

/// A stalled replica must be reported per-replica, not as a cluster-wide
/// verdict -- 9.2 needs to name the stuck node.
#[test]
fn stalls_name_the_specific_replica() {
    let mut target = target(5, 55, 0.1);
    let mut workload = CobWorkload::new(0xabc);
    let mut observer = Observer::new();
    let victims: Vec<NodeId> = vec![target.replicas()[1], target.replicas()[4]];

    drive(&mut target, &mut workload, &mut observer, 10, 500);
    for &victim in &victims {
        target.cluster_mut().crash(victim);
    }
    drive(&mut target, &mut workload, &mut observer, 20, 800);
    // Give every *live* replica work right before judging. Without this,
    // a healthy survivor that simply had not been asked to do anything for
    // a few hundred ticks is indistinguishable from a crashed one at a
    // tight budget -- which is precisely the contract `converge` documents.
    converge(&mut target, &mut workload, &mut observer, 3, 100_000);

    let mut stalled: Vec<NodeId> = observer
        .stalls(target.now(), 200)
        .into_iter()
        .map(|s| s.replica)
        .collect();
    stalled.sort_by_key(|n| n.0);
    let mut expected = victims.clone();
    expected.sort_by_key(|n| n.0);
    assert_eq!(
        stalled,
        expected,
        "exactly the two crashed replicas should be named:\n{}",
        observer.render_report()
    );
}
