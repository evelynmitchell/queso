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
//!
//! # Detection power (#125): none demonstrated, and here is what that means
//!
//! Mutation **C1**: replace the drawn priority with the constant `1`,
//! keeping the draw itself so the PRNG stream is consumed identically.
//! Applied at both randomization sites in this crate, separately:
//!
//! | Site | Path it serves | n=3 mean | n=5 mean | Tests killed, whole tree |
//! |---|---|---|---|---|
//! | (unmutated) | -- | 1.131 | 1.162 | -- |
//! | `node.rs`'s `on_timer` draw | abstract `Cluster` (this file) | 1.216 | 1.217 | **0 of 441** |
//! | `proposer.rs`'s `draw_priority` | `ConcreteCluster` (`concrete_termination.rs`) | see that file | see that file | **0 of 441** |
//!
//! Deleting the randomization this property is *named for* is invisible to
//! every test in the tree.
//!
//! **This is not the mutation-was-off-path mistake** that #113 records
//! (`SmrNode::submit` vs `SmrCluster::submit`). It was very nearly: the
//! first attempt mutated `proposer.rs`, ran *this* file, saw a
//! byte-identical round histogram and would have written down a zero --
//! for a mutation this file's abstract driver never executes. The two
//! sites are separated above for that reason, and each is shown on-path by
//! a moved distribution rather than by inspection: mutating `node.rs`
//! moves this file's mean from 1.131 to 1.216 (n=3) and its max from 4 to
//! 5. The code under test genuinely changed behavior; the assertions
//! simply do not look where the change is.
//!
//! **Why the bound cannot catch it.** `mean < 4.0` is a one-sided bound on
//! a statistic that removing randomization moves *downward*: with equal
//! priorities, `Proposal::Ord` (priority, then origin, then value) still
//! yields a deterministic total order, and converging on "highest origin"
//! is, under this adversary, at least as fast as converging on a random
//! draw -- in the concrete driver it is measurably faster. No headroom
//! setting on a one-sided bound detects a change in the safe direction.
//!
//! **Why the adversary cannot catch it either.** These tests run
//! `ContentObliviousAdversary`, which is the class A3 names and the class
//! the ">= 1/2 per round" theorem is stated against. An adversary that
//! cannot read proposals cannot exploit a deterministic tie-break, so in
//! this envelope randomized and origin-ordered convergence are not
//! distinguishable by outcome. Randomization earns its keep against an
//! adversary that *can* see priorities and schedule on them --
//! `crates/sim` has that class (`ContentAwareAdversary`), and no
//! termination test uses it.
//!
//! So: P14's evidence class stays *tested, power unmeasured* -- with the
//! measurement now done and the answer being "zero, for a structural
//! reason", not "nobody looked". Whether a content-aware adversary can
//! actually livelock the constant-priority variant is **unmeasured**; it
//! is a genuine open question, not a formality, and building that
//! adversary is tracked separately rather than guessed at here.

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
