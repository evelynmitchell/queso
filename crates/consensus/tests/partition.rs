//! Property tests exercising a **genuine network partition**
//! (`queso_sim::Kernel::partition`/`heal`, reached here through
//! `ConcreteCluster::partition`/`heal`/`schedule_partition`/`schedule_heal`)
//! at the consensus layer -- closing the gap the #12 review flagged: the
//! Phase 1-3 property tests exercise crash/restart between rounds and
//! scheduler-level probabilistic drop/reorder (the content-oblivious
//! adversary), but never a *manually installed, guaranteed* network cut.
//! See `docs/03-testing-plan.md §4`'s "Partition / heal" DST scenario and
//! `docs/02-properties.md` P5 (no divergence), P11 (safety under >f), P13
//! (majority progress), O4 (loss of majority may stall).
//!
//! A partition installed via `Kernel::partition` drops messages **both** at
//! send time and at arrival (a message already in flight when the
//! partition takes effect is still cut off -- see `queso_sim::fault`'s
//! `DropReason::Partitioned` vs `DropReason::PartitionedAtArrival`), so this
//! is a real cut, not merely a scheduler choosing to drop.
//!
//! # Concrete vs. abstract driver
//!
//! [`ConcreteCluster`] (Phase 2/3, tested in this file's first three
//! sections) drives every replica's proposer independently through
//! per-message `Node` callbacks with no round barrier, so "install a
//! partition, let the majority side decide, heal, let the minority catch
//! up" is a natural scenario to exercise directly.
//!
//! [`Cluster`] (Phase 1's abstract Algorithm 1 driver), by contrast, layers
//! on `crate::tcast`'s lock-step-ish barrier, whose majority precondition is
//! checked against the driver's own `live` bookkeeping -- which `partition`
//! deliberately does *not* touch (a partitioned replica is still running,
//! unlike a crashed one). A manual partition that leaves fewer than a
//! majority of `live` mutually reachable therefore does not trip tcast's
//! "no live majority" assertion; instead its mandatory `b_src` broadcast to
//! the unreachable side retries until `MAX_TCAST_RETRIES` and panics loudly
//! ("eventual delivery (A2) violated"). The final section of this file
//! exercises and documents that behavior explicitly, per the #12 review:
//! this is a loud panic, not silent divergence -- an acceptable (if noisy)
//! degradation for a driver that was never designed to be partition-aware.

use std::collections::{BTreeMap, BTreeSet};

use queso_consensus::{Cluster, ConcreteCluster};
use queso_sim::ids::NodeId;
use queso_sim::scheduler::{ContentObliviousAdversary, Fifo, SchedulerKind};

/// The smallest majority of `n` replicas: `n/2 + 1`.
fn majority_size(n: u32) -> u32 {
    n / 2 + 1
}

/// Split `0..n` into a majority group `{0..majority_size(n)}` and a
/// minority group `{majority_size(n)..n}`.
fn majority_minority_split(n: u32) -> (BTreeSet<NodeId>, BTreeSet<NodeId>) {
    let maj = majority_size(n);
    let majority: BTreeSet<NodeId> = (0..maj).map(NodeId).collect();
    let minority: BTreeSet<NodeId> = (maj..n).map(NodeId).collect();
    (majority, minority)
}

fn initial_values(n: u32) -> BTreeMap<NodeId, u32> {
    (0..n).map(|i| (NodeId(i), i)).collect()
}

// ---------------------------------------------------------------------
// Section 1: majority/minority split -- majority decides under a genuine
// partition (P13), minority provably cannot decide a *different* (or any)
// value while cut off (P5/P1), and healing lets the minority catch up to
// the exact same decision with no divergence (P5, N1).
// ---------------------------------------------------------------------

const SEED_CORPUS_SIZE: u64 = 60;
const PARTITION_TICKS: u64 = 40_000;
const HEAL_TICKS: u64 = 40_000;

/// One seed's worth of "genuinely partition into majority/minority, let the
/// majority decide, heal, confirm the minority catches up without
/// divergence."
fn run_majority_minority_then_heal(n: u32, seed: u64) {
    let (majority, minority) = majority_minority_split(n);
    debug_assert!(
        2 * majority.len() > n as usize,
        "majority group must actually be a majority"
    );
    debug_assert!(
        2 * minority.len() <= n as usize,
        "minority group must not itself be a majority"
    );

    let scheduler = ContentObliviousAdversary::new(1, 6).with_drop_probability(0.2);
    let mut cluster = ConcreteCluster::new(
        seed,
        SchedulerKind::Oblivious(Box::new(scheduler)),
        initial_values(n),
    );

    // Genuinely cut the network between the two groups from the start of
    // the run (this *is* "during a consensus run": the protocol only
    // starts once `run_slot` injects its kickoff timers, which happens
    // under the partition here).
    cluster.partition(majority.clone(), minority.clone());
    cluster.run_slot(PARTITION_TICKS);

    // P13 -- majority progress: every majority-side replica must have
    // decided, since a majority of the full membership was mutually
    // reachable the whole time.
    let mut majority_decisions: BTreeSet<u32> = BTreeSet::new();
    for &id in &majority {
        let v = cluster.decided(id).unwrap_or_else(|| {
            panic!("seed {seed} (n={n}): majority replica {id} never decided under partition")
        });
        majority_decisions.insert(v);
    }
    assert_eq!(
        majority_decisions.len(),
        1,
        "seed {seed} (n={n}): majority replicas disagreed while partitioned: {majority_decisions:?}"
    );
    let decided_value = *majority_decisions.iter().next().unwrap();

    // P5/P1 -- the minority is *provably* unable to gather a quorum of the
    // full membership (its own group is smaller than a majority, and the
    // partition genuinely blocks every message to/from the majority side),
    // so it must not have decided anything at all -- deterministically,
    // not just "probably didn't get lucky."
    for &id in &minority {
        assert!(
            cluster.decided(id).is_none(),
            "seed {seed} (n={n}): minority replica {id} decided {:?} while genuinely \
             partitioned from any reachable majority -- this would be a real safety bug",
            cluster.decided(id)
        );
    }

    // Heal, then give every replica a chance to converge. This is the
    // undecided half of `Proposer::start`'s re-kick contract (issue #13)
    // doing its job: the minority proposers stalled out here get restarted
    // at round 1 and catch up off the recorders' monotone ISR state, while
    // the majority's already-decided proposers are left completely alone
    // (`start` returns immediately once `decided` is `Some`). Both halves
    // are pinned by `proposer_start_contract.rs`.
    cluster.heal();
    cluster.run_slot(HEAL_TICKS);

    assert!(
        cluster.all_live_decided(),
        "seed {seed} (n={n}): minority did not catch up within the post-heal tick budget"
    );
    for &id in cluster.replicas() {
        assert_eq!(
            cluster.decided(id),
            Some(decided_value),
            "seed {seed} (n={n}): replica {id} diverged from the majority's decision {decided_value} \
             after heal -- this would be a real safety bug (P5)"
        );
    }
}

#[test]
fn majority_decides_under_partition_minority_catches_up_after_heal_n3() {
    for seed in 0..SEED_CORPUS_SIZE {
        run_majority_minority_then_heal(3, seed);
    }
}

#[test]
fn majority_decides_under_partition_minority_catches_up_after_heal_n5() {
    for seed in 0..SEED_CORPUS_SIZE {
        run_majority_minority_then_heal(5, seed);
    }
}

// ---------------------------------------------------------------------
// Section 2: no divergence across a seed corpus with *varied* partition
// timing, installed and healed mid-run via `schedule_partition`/
// `schedule_heal` (rather than "partitioned from the very start"), so the
// partition genuinely cuts messages that are already in flight too.
// ---------------------------------------------------------------------

const TIMING_SEED_CORPUS_SIZE: u64 = 80;
const TIMING_MAX_TICKS: u64 = 150_000;

fn run_varied_partition_timing(n: u32, seed: u64) {
    let (majority, minority) = majority_minority_split(n);

    // Deterministic, seed-derived timing: partition takes effect only
    // after some initial protocol traffic has had a chance to flow
    // (catching some messages mid-flight when the cut lands -- exercising
    // `DropReason::PartitionedAtArrival`, not just send-time drops), and
    // heals after a further, also-varied delay.
    let partition_at = 1 + (seed % 50);
    let heal_after = 500 + (seed % 2000);

    let scheduler = ContentObliviousAdversary::new(1, 6).with_drop_probability(0.2);
    let mut cluster = ConcreteCluster::new(
        seed,
        SchedulerKind::Oblivious(Box::new(scheduler)),
        initial_values(n),
    );
    cluster.schedule_partition(partition_at, majority, minority);
    cluster.schedule_heal(partition_at + heal_after);
    cluster.run_slot(TIMING_MAX_TICKS);

    // P5/P1 -- no divergence: regardless of exactly when the partition
    // landed or healed relative to in-flight traffic, no two replicas may
    // ever have decided different values.
    let decisions: BTreeSet<u32> = cluster
        .replicas()
        .iter()
        .filter_map(|&id| cluster.decided(id))
        .collect();
    assert!(
        decisions.len() <= 1,
        "seed {seed} (n={n}, partition_at={partition_at}, heal_after={heal_after}): \
         replicas disagreed: {decisions:?}"
    );

    // With a generous post-heal tick budget, everyone should also have
    // actually converged (this is a progress bonus check, not the core
    // safety property above).
    assert!(
        cluster.all_live_decided(),
        "seed {seed} (n={n}, partition_at={partition_at}, heal_after={heal_after}): \
         did not fully converge within the tick budget"
    );
}

#[test]
fn no_divergence_under_varied_partition_timing_n3() {
    for seed in 0..TIMING_SEED_CORPUS_SIZE {
        run_varied_partition_timing(3, seed);
    }
}

#[test]
fn no_divergence_under_varied_partition_timing_n5() {
    for seed in 0..TIMING_SEED_CORPUS_SIZE {
        run_varied_partition_timing(5, seed);
    }
}

// ---------------------------------------------------------------------
// Section 3: no side has a majority (P11/O4) -- with an even `n` split
// exactly in half, neither side can ever reach the `n/2 + 1` quorum
// threshold, so safety (no decision, hence trivially no divergence) must
// be preserved even though liveness necessarily stalls for as long as the
// partition holds. No panic is expected here: `ConcreteCluster`'s
// per-message driver has no lock-step "live majority" precondition --
// `Proposer` simply retries a bounded number of times per step and then
// gives up gracefully (see `crate::proposer`'s module docs), exactly the
// "safety preserved, liveness may stall" contract P11/O4 describe.
// ---------------------------------------------------------------------

const NO_MAJORITY_SEED_CORPUS_SIZE: u64 = 30;
const NO_MAJORITY_TICKS: u64 = 30_000;

fn run_no_majority_anywhere(n: u32, seed: u64) {
    assert_eq!(n % 2, 0, "an exact half/half split requires even n");
    let half = n / 2;
    let side_a: BTreeSet<NodeId> = (0..half).map(NodeId).collect();
    let side_b: BTreeSet<NodeId> = (half..n).map(NodeId).collect();
    debug_assert!(
        2 * side_a.len() <= n as usize && 2 * side_b.len() <= n as usize,
        "neither side may be a majority"
    );

    let scheduler = ContentObliviousAdversary::new(1, 6).with_drop_probability(0.2);
    let mut cluster = ConcreteCluster::new(
        seed,
        SchedulerKind::Oblivious(Box::new(scheduler)),
        initial_values(n),
    );
    cluster.partition(side_a, side_b);
    cluster.run_slot(NO_MAJORITY_TICKS);

    for &id in cluster.replicas() {
        assert!(
            cluster.decided(id).is_none(),
            "seed {seed} (n={n}): replica {id} decided {:?} despite neither partition side \
             ever having a reachable majority -- this would be a real safety bug",
            cluster.decided(id)
        );
    }
}

#[test]
fn safety_holds_with_no_majority_on_either_side_of_a_partition_n4() {
    for seed in 0..NO_MAJORITY_SEED_CORPUS_SIZE {
        run_no_majority_anywhere(4, seed);
    }
}

#[test]
fn safety_holds_with_no_majority_on_either_side_of_a_partition_n6() {
    for seed in 0..NO_MAJORITY_SEED_CORPUS_SIZE {
        run_no_majority_anywhere(6, seed);
    }
}

// ---------------------------------------------------------------------
// Section 4: the abstract (Phase 1, Algorithm 1) driver's documented
// degradation under a genuine partition. `Cluster::run_round` (via
// `crate::tcast`) has a hard "live majority" precondition checked against
// the driver's *own* `live` bookkeeping -- which a manual `partition` (as
// opposed to `crash`) deliberately leaves untouched, since a partitioned
// replica is still running. That mismatch means a partition is never
// visible to tcast's precondition check; instead its mandatory
// reliable-broadcast retry loop for the unreachable side runs out of
// retries and panics loudly. Per the #12 review, this is the expected,
// acceptable shape of this lock-step-ish driver's behavior under a genuine
// partition -- a loud, deterministic panic, never a silent divergence --
// so these tests assert the panic explicitly rather than treating it as a
// failure.
// ---------------------------------------------------------------------

#[test]
#[should_panic(expected = "tcast failed to converge")]
fn abstract_cluster_panics_loudly_rather_than_diverging_under_majority_minority_partition() {
    let n = 5u32;
    let (majority, minority) = majority_minority_split(n);
    let mut cluster: Cluster<u32> = Cluster::new(
        7,
        SchedulerKind::Oblivious(Box::new(Fifo::new(1))),
        initial_values(n),
    );
    // A majority genuinely *is* reachable here (the majority side can talk
    // amongst itself), but `Cluster::live` still contains all 5 replicas
    // (partition doesn't shrink it), so `run_round`'s tcast calls attempt
    // to reach the minority too and can never finish `b_src`'s mandatory
    // broadcast to it.
    cluster.partition(majority, minority);
    cluster.run_round();
}

#[test]
#[should_panic(expected = "tcast failed to converge")]
fn abstract_cluster_panics_loudly_rather_than_diverging_when_no_side_has_a_majority() {
    let n = 4u32;
    let side_a: BTreeSet<NodeId> = [NodeId(0), NodeId(1)].into();
    let side_b: BTreeSet<NodeId> = [NodeId(2), NodeId(3)].into();
    let mut cluster: Cluster<u32> = Cluster::new(
        9,
        SchedulerKind::Oblivious(Box::new(Fifo::new(1))),
        initial_values(n),
    );
    cluster.partition(side_a, side_b);
    cluster.run_round();
}
