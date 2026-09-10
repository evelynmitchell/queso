//! P14 -- randomized termination for Phase 2's concrete protocol. Unlike
//! Phase 1's lock-step `Cluster` (where every live replica necessarily
//! finishes the same round together, so "rounds to decide" is a single
//! well-defined number per slot), the concrete protocol has **no round
//! barrier**: different replicas' proposers race independently and can
//! decide at different threshold-clock steps. So this test reports, per
//! *replica*, the round in which it decided (`step() / 4`, since a
//! proposer's `step` only stops advancing once it has decided, and decision
//! always happens in phase 2 -- see `queso_consensus::proposer`), and
//! asserts the same shape of bound the paper's Theorem (Liveness) predicts:
//! comfortably under a handful of rounds in expectation, with every replica
//! terminating with probability 1 within a generous cap.
//!
//! # Detection power (#125): none demonstrated
//!
//! Mutation **C1**, at `proposer.rs`'s `draw_priority`: return the
//! constant `1`, keeping the draw so the PRNG stream is consumed
//! identically. This is the site the *concrete* driver executes (the
//! abstract driver's priorities come from `node.rs` instead -- see
//! `termination.rs`, which records why the two must be measured apart).
//!
//! | | n=3 mean | n=3 max | n=5 mean | n=5 max |
//! |---|---|---|---|---|
//! | unmutated | 1.263 | 3 | 1.607 | 5 |
//! | C1 | 1.177 | 3 | 1.407 | 4 |
//!
//! On-path, and **0 of 441 tests in the tree fail**. Removing the
//! randomization makes convergence *faster* here, which is exactly why the
//! `mean < 4.0` assertion cannot detect it: the bound is one-sided and the
//! defect moves the statistic the safe way. See `termination.rs`'s module
//! docs for the full reasoning. What that file conjectured would falsify
//! this property -- a content-aware adversary -- was built and run in
//! #150, and it does the opposite: it costs the *randomized* build rounds
//! and the constant-priority build nothing. See
//! `termination_under_targeted_adversaries.rs`.

use std::collections::BTreeMap;

use queso_consensus::ConcreteCluster;
use queso_sim::ids::NodeId;
use queso_sim::scheduler::{ContentObliviousAdversary, SchedulerKind};

const SEEDS: u64 = 500;
const MAX_TICKS: u64 = 400_000;

fn rounds_to_decide_per_replica(n: u32, seed: u64) -> Vec<u32> {
    let initial_values: BTreeMap<NodeId, u32> = (0..n).map(|i| (NodeId(i), i)).collect();
    let scheduler = ContentObliviousAdversary::new(1, 5).with_drop_probability(0.2);
    let mut cluster = ConcreteCluster::new(
        seed,
        SchedulerKind::Oblivious(Box::new(scheduler)),
        initial_values,
    );
    cluster.run_slot(MAX_TICKS);
    assert!(
        cluster.all_live_decided(),
        "seed {seed}: failed to decide within the tick budget -- \
         randomized termination (P14) violated"
    );
    cluster
        .replicas()
        .iter()
        .map(|&id| (cluster.step(id) / 4) as u32)
        .collect()
}

fn run_distribution(n: u32) -> Vec<u32> {
    (0..SEEDS)
        .flat_map(|seed| rounds_to_decide_per_replica(n, seed))
        .collect()
}

fn report(n: u32, rounds: &[u32]) {
    let total: u64 = rounds.iter().map(|&r| u64::from(r)).sum();
    let mean = total as f64 / rounds.len() as f64;
    let max = *rounds.iter().max().unwrap();
    let min = *rounds.iter().min().unwrap();

    let mut histogram: BTreeMap<u32, u32> = BTreeMap::new();
    for &r in rounds {
        *histogram.entry(r).or_insert(0) += 1;
    }

    println!("--- P14 randomized termination (concrete), n={n}, {SEEDS} seeds ---");
    println!("  min={min} mean={mean:.3} max={max}");
    println!("  round -> count (rounds with zero occurrences omitted):");
    for (round, count) in &histogram {
        let pct = 100.0 * f64::from(*count) / rounds.len() as f64;
        println!("    round {round:>3}: {count:>4}  ({pct:.1}%)");
    }

    // Same headroom rationale as Phase 1's equivalent test: the theorem's
    // bound is an expectation over infinitely many rounds/seeds, so the
    // corpus mean should sit comfortably under a handful of rounds.
    assert!(
        mean < 4.0,
        "n={n}: mean rounds-to-decide {mean:.3} is suspiciously high for P14 (expected < 2 in the idealized model)"
    );
}

#[test]
fn randomized_termination_n3() {
    let rounds = run_distribution(3);
    report(3, &rounds);
}

#[test]
fn randomized_termination_n5() {
    let rounds = run_distribution(5);
    report(5, &rounds);
}
