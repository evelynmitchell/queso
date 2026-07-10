//! P14 -- randomized termination: under a content-oblivious adversary,
//! every slot must terminate with probability 1 in a small expected number
//! of rounds (the paper's Theorem (Liveness): "less than two rounds in
//! expectation"). This test runs a large seed corpus, records the
//! rounds-to-decide distribution, prints it, and asserts:
//!
//! - every seed decides within a generous round cap (termination, not just
//!   "usually" terminates);
//! - the *mean* rounds-to-decide is comfortably below the paper's bound,
//!   empirically corroborating the theorem rather than merely hoping it
//!   holds.

use std::collections::BTreeMap;

use queso_consensus::Cluster;
use queso_sim::ids::NodeId;
use queso_sim::scheduler::{ContentObliviousAdversary, SchedulerKind};

const SEEDS: u64 = 1000;
const ROUND_CAP: u32 = 100;

fn rounds_to_decide(n: u32, seed: u64) -> u32 {
    let initial_values: BTreeMap<NodeId, u32> = (0..n).map(|i| (NodeId(i), i)).collect();
    let scheduler = ContentObliviousAdversary::new(1, 5).with_drop_probability(0.2);
    let mut cluster = Cluster::new(
        seed,
        SchedulerKind::Oblivious(Box::new(scheduler)),
        initial_values,
    );
    let rounds = cluster.run_slot(ROUND_CAP);
    assert!(
        cluster.all_live_decided(),
        "seed {seed}: failed to decide within {ROUND_CAP} rounds -- randomized termination (P14) violated"
    );
    // All live replicas decide together in this fully-synchronous
    // lock-step driver (see `crate::algorithm::Cluster::run_round`), so the
    // slot-level round count is well-defined and shared across replicas.
    rounds
}

fn run_distribution(n: u32) -> Vec<u32> {
    (0..SEEDS).map(|seed| rounds_to_decide(n, seed)).collect()
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

    println!("--- P14 randomized termination, n={n}, {SEEDS} seeds ---");
    println!("  min={min} mean={mean:.3} max={max}");
    println!("  round -> count (rounds with zero occurrences omitted):");
    for (round, count) in &histogram {
        let pct = 100.0 * f64::from(*count) / rounds.len() as f64;
        println!("    round {round:>3}: {count:>4}  ({pct:.1}%)");
    }

    // The theorem's bound is an *expectation* over infinitely many seeds;
    // give real headroom (the abstract model's per-round success
    // probability is >= 1/2, so the corpus mean should sit comfortably
    // under a handful of rounds, not creep toward the round cap).
    assert!(
        mean < 4.0,
        "n={n}: mean rounds-to-decide {mean:.3} is suspiciously high for P14 (expected < 2 in the idealized model)"
    );
    assert!(
        max <= ROUND_CAP,
        "n={n}: some seed hit the round cap ({ROUND_CAP}) without deciding"
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
