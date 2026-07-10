//! Property tests for P1 (Agreement), P2 (Validity), P3 (Integrity) --
//! Phase 2's concrete protocol (ISR + four phases), across a seed corpus,
//! for n=3 and n=5, under a **genuinely asynchronous** content-oblivious
//! adversary (real per-message delay/reorder/drop, not the lock-step
//! `tcast` barrier Phase 1's equivalent test used) plus crash fault
//! injection -- as called for in `docs/03-testing-plan.md §3` and
//! `docs/00-project-outline.md` Phase 2.
//!
//! Each seed:
//! 1. Builds an n-replica [`ConcreteCluster`] with distinct initial values.
//! 2. Crashes `f = seed % (max_f + 1)` replicas (0..=f, covering both the
//!    "nobody crashes" and "the maximum tolerable f crash" ends of the
//!    envelope), always leaving a true majority live.
//! 3. Runs the slot for a generous tick budget under a lossy, reordering,
//!    content-oblivious scheduler -- with no round barrier: different
//!    replicas' proposers race independently, retry independently, and can
//!    catch up to each other mid-phase.
//! 4. Checks P1/P2/P3 against every live replica's outcome.

use std::collections::{BTreeMap, BTreeSet};

use queso_consensus::ConcreteCluster;
use queso_sim::ids::NodeId;
use queso_sim::scheduler::{ContentObliviousAdversary, SchedulerKind};

const SEED_CORPUS_SIZE: u64 = 300;
const MAX_TICKS: u64 = 200_000;

fn run_one(n: u32, seed: u64) {
    let max_f = (n - 1) / 2; // n = 2f+1
    let f = (seed % u64::from(max_f + 1)) as u32;

    let initial_values: BTreeMap<NodeId, u32> = (0..n).map(|i| (NodeId(i), i)).collect();
    let all_values: BTreeSet<u32> = initial_values.values().copied().collect();

    // A genuinely adversarial content-oblivious scheduler: delay/reorder
    // jitter plus a nontrivial drop probability. The proposer's own retry
    // logic (see `queso_consensus::proposer`) must still converge.
    let scheduler = ContentObliviousAdversary::new(1, 6).with_drop_probability(0.25);
    let mut cluster = ConcreteCluster::new(
        seed,
        SchedulerKind::Oblivious(Box::new(scheduler)),
        initial_values,
    );

    // Crash the highest-numbered `f` replicas -- deterministic given the
    // seed, no extra randomness needed, and always leaves exactly n-f live
    // (a true majority, since f <= (n-1)/2).
    let crashed: Vec<NodeId> = (n - f..n).map(NodeId).collect();
    for id in &crashed {
        cluster.crash(*id);
    }

    cluster.run_slot(MAX_TICKS);
    assert!(
        cluster.all_live_decided(),
        "seed {seed} (n={n}, f={f}): did not decide within the tick budget"
    );

    // P2 -- Validity: every live replica's decision is some replica's
    // initial input, never an invented value.
    let mut decisions: BTreeSet<u32> = BTreeSet::new();
    for &id in cluster.replicas() {
        if crashed.contains(&id) {
            continue;
        }
        let v = cluster
            .decided(id)
            .unwrap_or_else(|| panic!("seed {seed}: live replica {id} never decided"));
        assert!(
            all_values.contains(&v),
            "seed {seed}: replica {id} decided phantom value {v} (not proposed by anyone)"
        );
        decisions.insert(v);
    }

    // P1 -- Agreement: every live replica decided the *same* value.
    assert_eq!(
        decisions.len(),
        1,
        "seed {seed} (n={n}, f={f}): replicas disagreed: {decisions:?}"
    );

    // P3 -- Integrity / decide-once: `Proposer::decided` is an `Option<V>`
    // set at most once, guarded in `crate::proposer::Proposer::process_phase`
    // (only reachable while `self.decided.is_none()`, and `on_response`/
    // `on_timer` both bail out immediately once `self.decided.is_some()`).
    // Re-assert that invariant here by running well past decision and
    // confirming the value never changes (this also covers P4 Stability).
    let before: BTreeMap<NodeId, u32> = cluster
        .replicas()
        .iter()
        .filter(|id| !crashed.contains(id))
        .map(|&id| (id, cluster.decided(id).unwrap()))
        .collect();
    cluster.run_slot(1_000);
    for (&id, &v) in &before {
        assert_eq!(
            cluster.decided(id).unwrap(),
            v,
            "seed {seed}: replica {id} changed its decision after already deciding"
        );
    }
}

#[test]
fn agreement_validity_integrity_n3() {
    for seed in 0..SEED_CORPUS_SIZE {
        run_one(3, seed);
    }
}

#[test]
fn agreement_validity_integrity_n5() {
    for seed in 0..SEED_CORPUS_SIZE {
        run_one(5, seed);
    }
}

#[test]
fn no_crashes_still_agrees() {
    // The f=0 corner of the envelope, explicitly, for both sizes.
    for n in [3u32, 5u32] {
        for seed in 0..50 {
            let initial_values: BTreeMap<NodeId, u32> = (0..n).map(|i| (NodeId(i), i)).collect();
            let scheduler = ContentObliviousAdversary::new(1, 4).with_drop_probability(0.1);
            let mut cluster = ConcreteCluster::new(
                seed,
                SchedulerKind::Oblivious(Box::new(scheduler)),
                initial_values,
            );
            cluster.run_slot(MAX_TICKS);
            assert!(cluster.all_live_decided(), "n={n} seed={seed}: no decision");
            let decisions: BTreeSet<u32> = cluster
                .replicas()
                .iter()
                .map(|&id| cluster.decided(id).unwrap())
                .collect();
            assert_eq!(decisions.len(), 1, "n={n} seed={seed}: disagreement");
        }
    }
}

/// P11/O4 -- safety under *more than* `f` crashes: no live majority is ever
/// reachable, so nobody should decide (progress may stall) but nothing
/// should panic or produce disagreement among whatever partial state
/// exists.
#[test]
fn safety_holds_without_a_live_majority() {
    for n in [3u32, 5u32] {
        let max_f = (n - 1) / 2;
        let crash_count = max_f + 1; // one more than tolerable
        for seed in 0..30 {
            let initial_values: BTreeMap<NodeId, u32> = (0..n).map(|i| (NodeId(i), i)).collect();
            let scheduler = ContentObliviousAdversary::new(1, 4).with_drop_probability(0.1);
            let mut cluster = ConcreteCluster::new(
                seed,
                SchedulerKind::Oblivious(Box::new(scheduler)),
                initial_values,
            );
            for id in (n - crash_count..n).map(NodeId) {
                cluster.crash(id);
            }
            cluster.run_slot(20_000);
            for &id in cluster.live() {
                assert!(
                    cluster.decided(id).is_none(),
                    "n={n} seed={seed}: replica {id} decided without ever having a live majority"
                );
            }
        }
    }
}
