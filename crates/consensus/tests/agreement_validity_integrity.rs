//! Property tests for P1 (Agreement), P2 (Validity), P3 (Integrity) across a
//! seed corpus, for n=3 and n=5, under the content-oblivious adversary plus
//! crash fault injection -- as called for in `docs/03-testing-plan.md §3`
//! and `docs/00-project-outline.md` Phase 1.
//!
//! Each seed:
//! 1. Builds an n-replica cluster with distinct initial values.
//! 2. Crashes `f = seed % (max_f + 1)` replicas (0..=f, so both the
//!    "nobody crashes" and "the maximum tolerable f crash" ends of the
//!    envelope get covered across the corpus) -- always leaving a true
//!    majority live, matching tcast's documented precondition.
//! 3. Runs the slot to completion under a lossy, reordering,
//!    content-oblivious scheduler.
//! 4. Checks P1/P2/P3 against every live replica's outcome.

use std::collections::{BTreeMap, BTreeSet};

use queso_consensus::Cluster;
use queso_sim::ids::NodeId;
use queso_sim::scheduler::{ContentObliviousAdversary, SchedulerKind};

const SEED_CORPUS_SIZE: u64 = 300;
const MAX_ROUNDS: u32 = 100;

fn run_one(n: u32, seed: u64) {
    let max_f = (n - 1) / 2; // n = 2f+1
    let f = (seed % u64::from(max_f + 1)) as u32;

    let initial_values: BTreeMap<NodeId, u32> = (0..n).map(|i| (NodeId(i), i)).collect();
    let all_values: BTreeSet<u32> = initial_values.values().copied().collect();

    // A genuinely adversarial content-oblivious scheduler: delay/reorder
    // jitter plus a nontrivial drop probability. tcast's internal retry
    // logic (see `queso_consensus::tcast`) must still converge.
    let scheduler = ContentObliviousAdversary::new(1, 6).with_drop_probability(0.25);
    let mut cluster = Cluster::new(
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

    let rounds = cluster.run_slot(MAX_ROUNDS);
    assert!(
        cluster.all_live_decided(),
        "seed {seed} (n={n}, f={f}): did not decide within {rounds} rounds"
    );

    // P2 -- Validity: every live replica's decision is some replica's
    // initial input, never an invented value.
    let mut decisions: BTreeSet<u32> = BTreeSet::new();
    for &id in cluster.replicas() {
        if crashed.contains(&id) {
            continue;
        }
        let v = *cluster
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

    // P3 -- Integrity / decide-once: `Cluster::decided` is an `Option<V>`
    // set at most once inside `run_round` (guarded by `st.decided.is_none()`
    // there); re-assert that invariant here by re-running extra rounds past
    // decision and confirming the value never changes (this also covers
    // P4 Stability as a bonus).
    let before: BTreeMap<NodeId, u32> = cluster
        .replicas()
        .iter()
        .filter(|id| !crashed.contains(id))
        .map(|&id| (id, *cluster.decided(id).unwrap()))
        .collect();
    // Running more rounds for a fully-decided live set is a no-op (loop
    // condition in `run_slot` is `!all_live_decided()`), but guard directly
    // against regressions by calling `run_round` once more explicitly.
    cluster.run_round();
    for (&id, &v) in &before {
        assert_eq!(
            *cluster.decided(id).unwrap(),
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
            let mut cluster = Cluster::new(
                seed,
                SchedulerKind::Oblivious(Box::new(scheduler)),
                initial_values,
            );
            cluster.run_slot(MAX_ROUNDS);
            assert!(cluster.all_live_decided(), "n={n} seed={seed}: no decision");
            let decisions: BTreeSet<u32> = cluster
                .replicas()
                .iter()
                .map(|&id| *cluster.decided(id).unwrap())
                .collect();
            assert_eq!(decisions.len(), 1, "n={n} seed={seed}: disagreement");
        }
    }
}
