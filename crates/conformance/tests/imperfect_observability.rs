//! What a weakly-observable source can and cannot catch -- and the design
//! finding Phase 9.2 (#56) needs to act on.
//!
//! Phase 9.1 runs in-process, where the harness can read every replica's
//! applied log and therefore see every `(n, h)` the replica passed through.
//! Phase 9.2 will not have that: it polls real `queso-node` processes, and
//! today the only progress signal they expose is `/metrics`' `next_slot`
//! -- a *frontier*, not a history.
//!
//! Issue #55 asks the observer to "detect divergence even with imperfect
//! observability (spot it at n+1)". These tests establish exactly how far
//! that goes:
//!
//! 1. The chain does deliver on `n+1`: a divergence is caught at any later
//!    `n` two replicas happen to share, even when every sample at the
//!    divergence point itself is discarded.
//! 2. **But frontier-only sampling rarely produces a shared `n` at all.**
//!    Replicas lag each other by design, so two frontier samples usually
//!    land on different `n`, the observer has nothing to compare, and a run
//!    ends "clean" having checked almost nothing. That is a vacuous pass,
//!    and it is what a naive 9.2 implementation would produce.
//! 3. Checkpointed sampling fixes it: if every replica reports `h` at the
//!    same `n` boundaries, comparisons align by construction. A real node
//!    can do this by retaining the chain hash at each checkpoint it crosses
//!    and exposing that small table -- which is the concrete ask this phase
//!    hands to #56.
//!
//! # Measured, on the run in `frontier_only_sampling_compares_far_less_than_checkpoints`
//!
//! | sampling | samples | cross-replica comparisons |
//! |----------|--------:|--------------------------:|
//! | frontier-only (`/metrics next_slot`) | 99 | **2** |
//! | checkpoints every 4 slots | 117 | **20** |
//!
//! Same cluster, same workload, same number of polls: an order of
//! magnitude more actual checking for a comparable sample budget. The
//! assertions below are deliberately looser than these exact numbers (they
//! are seed-dependent), but the gap they pin is the point.

use queso_conformance::chain::ChainState;
use queso_conformance::observer::{Observer, Sample};
use queso_conformance::source::{Observability, SimCluster};
use queso_conformance::workload::CobWorkload;
use queso_conformance::CobTarget;
use queso_sim::ids::NodeId;
use queso_sim::scheduler::{ContentObliviousAdversary, SchedulerKind};
use queso_smr::SmrCluster;

/// Drive one run and return every sample it produced. Same seeds and same
/// schedule for every observability mode, so the only variable across the
/// comparisons below is how densely the run was sampled.
fn recorded_run(seed: u64, observability: Observability) -> Vec<Sample> {
    let adversary = ContentObliviousAdversary::new(1, 8).with_drop_probability(0.1);
    let mut target = SimCluster::new(
        SmrCluster::new(seed, SchedulerKind::Oblivious(Box::new(adversary)), 3),
        observability,
    );
    let mut workload = CobWorkload::new(seed ^ 0xc0b);
    let mut samples = Vec::new();

    for _ in 0..30 {
        let command = workload.next_command();
        target.submit(command);
        target.advance(400);
        samples.extend(target.poll_samples());
    }
    for _ in 0..3 {
        for _ in 0..target.replicas().len() {
            let command = workload.next_command();
            target.submit(command);
        }
        target.advance(200_000);
        samples.extend(target.poll_samples());
    }
    samples
}

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
fn a_divergence_is_caught_after_the_point_it_began() {
    // Corrupt from n=6, then throw away every sample below n=15 -- the
    // observer never sees the divergence point, or anything near it.
    let samples = recorded_run(101, Observability::FullPrefix);
    let corrupted = corrupt_from(&samples, NodeId(1), 6);
    let late_only: Vec<Sample> = corrupted.into_iter().filter(|s| s.state.n >= 15).collect();

    let observer = feed(&late_only);
    let divergences = observer.divergences();

    assert!(
        !divergences.is_empty(),
        "the hash chain must carry a divergence forward so it is still \
         detectable long after it began:\n{}",
        observer.render_report()
    );
    assert!(
        divergences[0].n >= 15,
        "detection should land where the observer was actually looking; got n={}",
        divergences[0].n
    );
}

#[test]
fn frontier_only_sampling_compares_far_less_than_checkpoints() {
    let frontier = feed(&recorded_run(102, Observability::FrontierOnly));
    let checkpoints = feed(&recorded_run(102, Observability::Checkpoints { every: 4 }));

    // Neither run is corrupted, so both must be clean...
    assert!(frontier.divergences().is_empty());
    assert!(checkpoints.divergences().is_empty());

    // ...but "clean" means very different things. This is the finding: a
    // frontier-only source produces a verdict backed by an order of
    // magnitude fewer actual checks.
    assert!(
        checkpoints.comparisons() > frontier.comparisons() * 4,
        "checkpointed sampling should compare far more than frontier-only \
         (checkpoints {}, frontier {})",
        checkpoints.comparisons(),
        frontier.comparisons()
    );
}

#[test]
fn checkpoints_catch_a_divergence_frontier_only_sampling_misses() {
    // The same cluster, the same run, the same injected divergence -- the
    // only difference is how the source sampled it.
    let victim = NodeId(1);
    let from_n = 7;

    let frontier = feed(&corrupt_from(
        &recorded_run(103, Observability::FrontierOnly),
        victim,
        from_n,
    ));
    let checkpoints = feed(&corrupt_from(
        &recorded_run(103, Observability::Checkpoints { every: 4 }),
        victim,
        from_n,
    ));

    assert!(
        !checkpoints.divergences().is_empty(),
        "checkpointed sampling must catch the injected divergence:\n{}",
        checkpoints.render_report()
    );
    assert!(
        frontier.divergences().len() < checkpoints.divergences().len(),
        "this test exists to show frontier-only sampling is weaker; if it has \
         become just as good, the finding recorded in this file's docs (and \
         handed to #56) needs revisiting.\nfrontier:\n{}\ncheckpoints:\n{}",
        frontier.render_report(),
        checkpoints.render_report()
    );
}

#[test]
fn checkpoint_spacing_trades_detection_latency_for_sample_volume() {
    // Tighter checkpoints mean more samples and earlier detection; wider
    // ones mean fewer samples and a later catch. Both must still catch it
    // -- the chain guarantees that -- which is what makes the spacing a
    // free tuning knob for 9.2 rather than a correctness decision.
    let victim = NodeId(1);
    let tight = feed(&corrupt_from(
        &recorded_run(104, Observability::Checkpoints { every: 2 }),
        victim,
        5,
    ));
    let wide = feed(&corrupt_from(
        &recorded_run(104, Observability::Checkpoints { every: 8 }),
        victim,
        5,
    ));

    assert!(!tight.divergences().is_empty(), "{}", tight.render_report());
    assert!(!wide.divergences().is_empty(), "{}", wide.render_report());
    assert!(
        tight.samples() > wide.samples(),
        "tighter checkpoints must cost more samples (tight {}, wide {})",
        tight.samples(),
        wide.samples()
    );
    assert!(
        tight.divergences()[0].n <= wide.divergences()[0].n,
        "tighter checkpoints must not detect later than wider ones \
         (tight n={}, wide n={})",
        tight.divergences()[0].n,
        wide.divergences()[0].n
    );
}
