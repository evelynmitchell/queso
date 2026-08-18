//! Acceptance: a healthy cluster running the Chain-of-Blocks workload
//! never diverges, and every replica keeps making progress.
//!
//! This is the "does the harness work end to end" test. It is deliberately
//! *not* the interesting one -- a healthy in-process cluster passing CoB
//! proves little that `queso-smr`'s own `log_safety.rs` doesn't already
//! prove. Its job is to establish that the workload, the source, and the
//! observers agree with each other before Phase 9.2 (#56) points them at
//! real processes under fault.
//!
//! Every assertion here is paired with an anti-vacuity assertion: a run
//! that submitted nothing, or that never compared two replicas at the same
//! `n`, would trivially "pass" the safety check, so those quantities are
//! asserted to be meaningfully non-zero. `observer_detects.rs` closes the
//! loop by showing the same observer *fails* when divergence is present.

use queso_conformance::observer::Observer;
use queso_conformance::source::{Observability, SimCluster};
use queso_conformance::workload::{converge, run, CobWorkload, RunConfig};
use queso_conformance::CobTarget;
use queso_sim::scheduler::{ContentObliviousAdversary, SchedulerKind};
use queso_smr::SmrCluster;

/// A cluster under the content-oblivious adversary (A3) with a lossy link
/// -- the class under which QuePaxa's randomized liveness is claimed.
fn cluster(n: usize, seed: u64, drop_probability: f64) -> SmrCluster {
    let adversary = ContentObliviousAdversary::new(1, 8).with_drop_probability(drop_probability);
    SmrCluster::new(seed, SchedulerKind::Oblivious(Box::new(adversary)), n)
}

/// `min_comparisons` is the anti-vacuity floor for this observability mode
/// -- how many cross-replica checks the run must actually have made for its
/// "no divergence" verdict to carry weight. It is per-mode because the
/// modes differ enormously in sampling density: full-prefix sees every
/// state, checkpoints see one in `every`, and frontier-only (see
/// `imperfect_observability.rs`) sees essentially no aligned pairs at all.
fn scenario(n: usize, seed: u64, observability: Observability, min_comparisons: u64) {
    let mut target = SimCluster::new(cluster(n, seed, 0.1), observability);
    let mut workload = CobWorkload::new(seed ^ 0xc0b);
    let mut observer = Observer::new();

    run(
        &mut target,
        &mut workload,
        &mut observer,
        RunConfig {
            commands: 30,
            advance_between: 400,
            poll_every: 2,
            settle: 200_000,
        },
    );
    // Give every replica traffic before judging liveness -- an idle Queso
    // replica legitimately sits behind (see `workload::converge`).
    converge(&mut target, &mut workload, &mut observer, 3, 200_000);

    // --- Safety: the Chain-of-Blocks property ---
    assert!(
        observer.divergences().is_empty(),
        "healthy cluster diverged:\n{}",
        observer.render_report()
    );

    // --- Anti-vacuity: the safety check actually checked something ---
    assert!(
        observer.comparisons() >= min_comparisons,
        "too few cross-replica comparisons ({}, wanted >= {min_comparisons}) for the safety \
         verdict to mean anything:\n{}",
        observer.comparisons(),
        observer.render_report()
    );
    assert!(
        observer.cluster_frontier() >= 20,
        "cluster barely progressed (frontier {}), so nothing was really exercised",
        observer.cluster_frontier()
    );

    // --- Liveness: nobody is frozen behind the frontier ---
    //
    // The budget is deliberately tight (200 ticks of the sim's logical
    // clock, against runs whose whole span is ~3000). A generous budget
    // here would make this assertion unfalsifiable -- and measurement says
    // it does not need to be generous: after `converge`, a healthy run has
    // zero stalls even at a budget of 0, because every replica advanced at
    // the most recent observation. `observer_detects.rs` shows a crashed
    // replica *is* caught at this same budget, which is what stops this
    // from being a test that can only pass.
    let now = target.now();
    let stalls = observer.stalls(now, 200);
    assert!(
        stalls.is_empty(),
        "replicas stalled after faults-free convergence: {stalls:?}\n{}",
        observer.render_report()
    );

    // --- Ground truth: the sampled chains match the real applied logs ---
    // Guards against an observer that agrees with itself because the source
    // fed it a state the cluster never actually held.
    for (replica, state) in observer.latest_states() {
        let truth = target.true_state(replica);
        assert!(
            state.n <= truth.n,
            "{replica}: observer saw n={} ahead of the real log's n={}",
            state.n,
            truth.n
        );
    }
}

#[test]
fn n3_healthy_full_observability() {
    for seed in [1, 2, 3, 17, 99] {
        scenario(3, seed, Observability::FullPrefix, 40);
    }
}

#[test]
fn n5_healthy_full_observability() {
    for seed in [4, 5, 6] {
        scenario(5, seed, Observability::FullPrefix, 40);
    }
}

#[test]
fn n3_healthy_checkpoint_observability() {
    // The same acceptance run under the weaker, checkpointed observability
    // a real-process source can realistically offer in 9.2 -- far fewer
    // samples, same verdict, and still enough aligned comparisons for that
    // verdict to mean something.
    //
    // (`Observability::FrontierOnly`, the other weak mode, is deliberately
    // absent here: it yields zero comparisons, which
    // `imperfect_observability.rs` pins as a finding rather than papering
    // over here.)
    for seed in [7, 8, 9] {
        scenario(3, seed, Observability::Checkpoints { every: 4 }, 12);
    }
}

#[test]
fn a_run_is_reproducible_from_its_seeds() {
    let render = |seed: u64| {
        let mut target = SimCluster::new(cluster(3, seed, 0.1), Observability::FullPrefix);
        let mut workload = CobWorkload::new(seed ^ 0xc0b);
        let mut observer = Observer::new();
        run(
            &mut target,
            &mut workload,
            &mut observer,
            RunConfig {
                commands: 12,
                advance_between: 400,
                poll_every: 2,
                settle: 100_000,
            },
        );
        observer.render_report()
    };

    assert_eq!(
        render(42),
        render(42),
        "same seeds must produce the same run; a conformance failure that cannot be \
         replayed is not much use"
    );
    assert_ne!(
        render(42),
        render(43),
        "different seeds must actually explore different interleavings"
    );
}
