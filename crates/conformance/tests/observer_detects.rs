//! Anti-vacuity: proof that the observers *fail* when they should.
//!
//! Every other test in this crate asserts that a healthy cluster produces
//! no divergence and no stalls. On its own that is exactly the kind of
//! result this project's reviews are trained to distrust -- an observer
//! that returns "all clear" unconditionally would pass all of them. These
//! tests inject the faults the observers exist to catch and assert they are
//! caught, at the right `n`, naming the right replicas.
//!
//! # Why the divergence is synthetic
//!
//! Queso does not diverge -- that is the property, model-checked in TLA+
//! and property-tested under the adversary. So there is no way to obtain a
//! genuinely divergent Queso cluster to point the observer at, and a test
//! that waited for one would be a test that never fires.
//!
//! Instead these tests take the sample stream from a *real* run and corrupt
//! it in transit, exactly as a diverged replica's samples would have looked
//! -- same replica ids, same timing, same chain shape, one different hash
//! from some `n` onward. That validates the detector without pretending to
//! validate the cluster.

use queso_conformance::chain::ChainState;
use queso_conformance::observer::{Observer, Sample};
use queso_conformance::source::{Observability, SimCluster};
use queso_conformance::workload::CobWorkload;
use queso_conformance::CobTarget;
use queso_sim::ids::NodeId;
use queso_sim::scheduler::{ContentObliviousAdversary, SchedulerKind};
use queso_smr::SmrCluster;

fn target(n: usize, seed: u64, observability: Observability) -> SimCluster {
    let adversary = ContentObliviousAdversary::new(1, 8).with_drop_probability(0.1);
    SimCluster::new(
        SmrCluster::new(seed, SchedulerKind::Oblivious(Box::new(adversary)), n),
        observability,
    )
}

/// Drive a healthy run and return every sample it produced, in order.
fn recorded_run(seed: u64, observability: Observability, commands: usize) -> Vec<Sample> {
    let mut target = target(3, seed, observability);
    let mut workload = CobWorkload::new(seed ^ 0xc0b);
    let mut samples = Vec::new();

    for _ in 0..commands {
        let command = workload.next_command();
        target.submit(command);
        target.advance(400);
        samples.extend(target.poll_samples());
    }
    // A few rounds of traffic to every replica, so all of them are
    // represented at overlapping chain positions.
    for _ in 0..4 {
        for _ in 0..target.replicas().len() {
            let command = workload.next_command();
            target.submit(command);
        }
        target.advance(200_000);
        samples.extend(target.poll_samples());
    }
    samples
}

/// Corrupt `victim`'s samples from `from_n` onward, as a replica that
/// applied a different command at slot `from_n - 1` would appear: every
/// later hash differs, because the chain carries the difference forward.
fn corrupt_from(samples: &[Sample], victim: NodeId, from_n: u64) -> Vec<Sample> {
    samples
        .iter()
        .map(|sample| {
            if sample.replica == victim && sample.state.n >= from_n {
                Sample {
                    state: ChainState {
                        n: sample.state.n,
                        h: sample.state.h ^ 0x5eed_dead_beef_0001,
                    },
                    ..*sample
                }
            } else {
                *sample
            }
        })
        .collect()
}

fn feed(samples: &[Sample]) -> Observer {
    let mut observer = Observer::new();
    for sample in samples {
        observer.observe(*sample);
    }
    observer
}

#[test]
fn the_control_run_is_clean_and_non_trivial() {
    let samples = recorded_run(11, Observability::FullPrefix, 25);
    let observer = feed(&samples);

    assert!(
        observer.divergences().is_empty(),
        "the uncorrupted control run must be clean:\n{}",
        observer.render_report()
    );
    assert!(
        observer.comparisons() >= 40,
        "control run made only {} comparisons -- too few to make the corrupted \
         comparison meaningful",
        observer.comparisons()
    );
}

#[test]
fn injected_divergence_is_detected_at_the_point_it_was_injected() {
    let samples = recorded_run(11, Observability::FullPrefix, 25);
    let victim = NodeId(1);
    let corrupted = corrupt_from(&samples, victim, 6);
    let observer = feed(&corrupted);

    let divergences = observer.divergences();
    assert!(
        !divergences.is_empty(),
        "the observer missed an injected divergence entirely:\n{}",
        observer.render_report()
    );

    let first = divergences[0];
    assert_eq!(
        first.n,
        6,
        "detection must land on the first corrupted position, not later:\n{}",
        observer.render_report()
    );
    assert!(
        first.first.0 == victim || first.other.0 == victim,
        "the divergence must name the corrupted replica; got {first:?}"
    );
}

#[test]
fn the_report_of_a_detected_divergence_is_root_causable() {
    let samples = recorded_run(12, Observability::FullPrefix, 25);
    let corrupted = corrupt_from(&samples, NodeId(2), 9);
    let observer = feed(&corrupted);
    let report = observer.render_report();

    assert!(report.contains("DIVERGENCE at n=9"), "{report}");
    // The per-transition log either side of the divergence is what makes
    // the report actionable rather than just an alarm.
    assert!(
        report.contains("cmd=0x"),
        "report must include the per-transition command digests:\n{report}"
    );
    assert!(
        report.contains("n2"),
        "report must name the diverging replica:\n{report}"
    );
}

#[test]
fn a_crashed_replica_is_reported_as_stalled() {
    let mut target = target(3, 21, Observability::FullPrefix);
    let mut workload = CobWorkload::new(0xbeef);
    let mut observer = Observer::new();

    // Phase 1: healthy traffic to everyone.
    for _ in 0..12 {
        let command = workload.next_command();
        target.submit(command);
        target.advance(400);
        for sample in target.poll_samples() {
            observer.observe(sample);
        }
    }
    for _ in 0..3 {
        for _ in 0..3 {
            let command = workload.next_command();
            target.submit(command);
        }
        target.advance(100_000);
        for sample in target.poll_samples() {
            observer.observe(sample);
        }
    }

    // Phase 2: crash one replica (a minority, so the cluster keeps going)
    // and keep the survivors busy.
    let victim = target.replicas()[2];
    target.cluster_mut().crash(victim);
    for _ in 0..20 {
        let command = workload.next_command();
        target.submit(command);
        target.advance(2_000);
        for sample in target.poll_samples() {
            observer.observe(sample);
        }
    }

    let now = target.now();
    // Same tight budget `healthy_cluster.rs` uses for its clean verdict.
    let stalls = observer.stalls(now, 200);
    assert_eq!(
        stalls.len(),
        1,
        "exactly the crashed replica should be reported; got {stalls:?}\n{}",
        observer.render_report()
    );
    assert_eq!(stalls[0].replica, victim);
    assert!(
        stalls[0].stuck_at.n < stalls[0].cluster_frontier,
        "a stalled replica must be behind the frontier: {:?}",
        stalls[0]
    );

    // And the survivors kept the cluster live throughout -- otherwise the
    // "only the crashed one stalled" result would be trivially true because
    // nothing moved at all.
    assert!(
        observer.cluster_frontier() > stalls[0].stuck_at.n + 5,
        "the cluster did not keep making progress without the crashed replica:\n{}",
        observer.render_report()
    );
}

#[test]
fn a_restarted_replica_stops_being_reported_as_stalled() {
    let mut target = target(3, 31, Observability::FullPrefix);
    let mut workload = CobWorkload::new(0xf00d);
    let mut observer = Observer::new();

    let victim = target.replicas()[1];
    target.cluster_mut().crash(victim);
    for _ in 0..20 {
        let command = workload.next_command();
        target.submit(command);
        target.advance(2_000);
        for sample in target.poll_samples() {
            observer.observe(sample);
        }
    }
    let stalls_while_down = observer.stalls(target.now(), 200);
    assert!(
        stalls_while_down.iter().any(|s| s.replica == victim),
        "the crashed replica should be flagged while it is down: {stalls_while_down:?}\nnow={}\n{}",
        target.now(),
        observer.render_report()
    );

    // Restart it and give every replica work so it can catch up (a Queso
    // replica catches up by participating -- see `workload::converge`).
    target.cluster_mut().restart(victim);
    for _ in 0..6 {
        for _ in 0..3 {
            let command = workload.next_command();
            target.submit(command);
        }
        target.advance(200_000);
        for sample in target.poll_samples() {
            observer.observe(sample);
        }
    }

    let stalls_after = observer.stalls(target.now(), 200);
    assert!(
        stalls_after.is_empty(),
        "the restarted replica should have caught up and cleared its stall: \
         {stalls_after:?}\n{}",
        observer.render_report()
    );
    assert!(
        observer.divergences().is_empty(),
        "crash/restart must not produce divergence:\n{}",
        observer.render_report()
    );
}
