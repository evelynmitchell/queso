//! Log safety across the replicated log (P5 prefix consistency, P6 total
//! order, P7 gap-free application), under a content-oblivious adversary
//! (A3) plus crash injection, across many seeds and both `n = 3` and
//! `n = 5` (crash-stop, `f <= (n-1)/2`).
//!
//! This is a black-box property test: it never inspects the consensus
//! internals, only what every replica's [`queso_smr::SmrCluster::applied_log`]
//! shows after a randomized run -- exactly the log-level guarantee
//! `docs/02-properties.md` describes ("if two replicas have a decided value
//! at a slot, it is the same value; a replica may lag but must never
//! diverge").
//!
//! # Detection power (measured, issue #113)
//!
//! Three mutations of `crates/smr/src/replica.rs`'s `finish_attempt` --
//! the function that advances the frontier -- were run against this file
//! and against `queso-conformance`'s `tests/faults.rs`, which the
//! conformance matrix cites for the same properties:
//!
//! - **P5-A** -- `st.durable.applied_log.push(decided.clone())` becomes
//!   `push(attempt.command.clone())`: a replica records *its own
//!   proposal* rather than what was decided, so replicas whose proposals
//!   lost diverge from those whose won.
//! - **P7-A** -- the `applied_log.push` is deleted, leaving
//!   `next_slot += 1`: the frontier advances past commands never appended.
//! - **P7-B** -- the `applied_log.push` is duplicated: every command is
//!   applied twice, so the log runs ahead of the frontier.
//!
//! | Mutation | this file (7 tests) | `conformance/tests/faults.rs` (6) |
//! |---|---|---|
//! | P5-A | **7/7** at the P5/P6 slot compare | **5/6** |
//! | P7-A | **7/7** at the P7 frontier-vs-length check | **2/6** |
//! | P7-B | **7/7** at the P7 frontier-vs-length check | **0/6** |
//!
//! Runs are deterministic (seeded sim, D9), so each cell is one run per
//! mutation, repeated once to confirm. The denominator is tests, not
//! seeds -- every test here already loops 6 or 8 seeds internally.
//!
//! **P7-B is invisible to the conformance suite, and that is structural,
//! not a flake.** Doubling every apply inflates every replica's log
//! *identically*, so nothing diverges. `faults.rs` compares logs pairwise
//! across replicas and asserts a cluster frontier; it has no per-replica
//! frontier-vs-log-length assertion, and `assert_log_safety` does. A
//! defect that is uniformly wrong everywhere is exactly what a
//! cross-replica comparison cannot see -- established by reading both
//! assertion sets, not by enumeration.
//!
//! P6 has no mutation of its own. At this layer a total-order violation
//! *is* a P5 violation: `finish_attempt` only ever appends at the current
//! frontier, so two replicas can apply in different orders only by
//! applying different commands. That is why the assertion below is one
//! assertion for both.
//!
//! Nothing in CI re-runs these; the counts rot silently.

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use queso_sim::ids::NodeId;
use queso_sim::scheduler::{ContentObliviousAdversary, SchedulerKind};
use queso_smr::{ClientId, SmrCluster};

/// Submit a randomized workload of put/get commands from a handful of
/// clients to random live replicas, interleaved with running the kernel
/// forward in small increments so operations genuinely overlap in logical
/// time (rather than all being queued at time 0).
fn run_random_workload(cluster: &mut SmrCluster, workload_seed: u64, ops: usize) {
    let mut rng = StdRng::seed_from_u64(workload_seed);
    let live: Vec<NodeId> = cluster.live().iter().copied().collect();
    if live.is_empty() {
        return;
    }
    for i in 0..ops {
        let replica = live[rng.gen_range(0..live.len())];
        let client = ClientId(rng.gen_range(0..4));
        let key = rng.gen_range(0..3);
        if rng.gen_bool(0.5) {
            let value = rng.gen_range(0..1000);
            cluster.submit_put(replica, client, i as u64, key, value);
        } else {
            cluster.submit_get(replica, client, i as u64, key);
        }
        cluster.run_for(rng.gen_range(1..40));
    }
    cluster.run_for(200_000);
}

/// Assert P5 (prefix consistency)/P6 (total order): for every pair of
/// replicas, wherever their applied logs both have an entry for the same
/// slot, that entry is identical. Also assert P7 (gap-free application) is
/// structurally intact: a replica's frontier always exactly matches the
/// length of what it has applied.
fn assert_log_safety(cluster: &SmrCluster) {
    let replicas = cluster.replicas().to_vec();
    let logs: Vec<Vec<queso_smr::Command>> =
        replicas.iter().map(|&r| cluster.applied_log(r)).collect();

    for (r, log) in replicas.iter().zip(&logs) {
        assert_eq!(
            cluster.next_slot(*r) as usize,
            log.len(),
            "P7: replica {r}'s frontier must exactly match its applied-log length"
        );
    }

    for i in 0..replicas.len() {
        for j in (i + 1)..replicas.len() {
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

fn scenario(n: usize, seed: u64, workload_seed: u64, crash_count: usize) {
    let adversary = ContentObliviousAdversary::new(1, 8).with_drop_probability(0.15);
    let mut cluster = SmrCluster::new(seed, SchedulerKind::Oblivious(Box::new(adversary)), n);

    // f <= (n-1)/2: crash at most the tolerated number of replicas, up
    // front, before any work is submitted (Stage 4a is crash-stop, matching
    // `queso_consensus::concrete::ConcreteCluster`'s scope).
    let f = (n - 1) / 2;
    let crash_count = crash_count.min(f);
    for i in 0..crash_count {
        cluster.crash(NodeId(i as u32));
    }

    run_random_workload(&mut cluster, workload_seed, 24);
    assert_log_safety(&cluster);
}

#[test]
fn log_safety_holds_across_seeds_n3_no_crashes() {
    for seed in 0..8u64 {
        scenario(3, seed, seed.wrapping_mul(31) + 1, 0);
    }
}

#[test]
fn log_safety_holds_across_seeds_n3_with_a_tolerated_crash() {
    for seed in 0..8u64 {
        scenario(3, seed, seed.wrapping_mul(37) + 2, 1);
    }
}

#[test]
fn log_safety_holds_across_seeds_n5_no_crashes() {
    for seed in 0..6u64 {
        scenario(5, seed, seed.wrapping_mul(41) + 3, 0);
    }
}

#[test]
fn log_safety_holds_across_seeds_n5_with_tolerated_crashes() {
    for seed in 0..6u64 {
        scenario(5, seed, seed.wrapping_mul(43) + 4, 2);
    }
}

/// P5/P6/P7 (and, structurally, P12) under crash + **restart**, not just
/// crash-stop: the crashed replicas from `scenario`'s setup come back
/// midway through the workload and must rejoin without ever diverging from
/// the rest of the log.
fn scenario_with_restart(n: usize, seed: u64, workload_seed: u64, crash_count: usize) {
    let adversary = ContentObliviousAdversary::new(1, 8).with_drop_probability(0.15);
    let mut cluster = SmrCluster::new(seed, SchedulerKind::Oblivious(Box::new(adversary)), n);

    let f = (n - 1) / 2;
    let crash_count = crash_count.min(f);
    let crashed: Vec<NodeId> = (0..crash_count as u32).map(NodeId).collect();
    for &id in &crashed {
        cluster.crash(id);
    }

    // Run half the workload with the crashed replicas down, then bring them
    // back and run the rest while they catch up as learners.
    run_random_workload(&mut cluster, workload_seed, 12);
    for &id in &crashed {
        cluster.restart(id);
    }
    run_random_workload(
        &mut cluster,
        workload_seed.wrapping_mul(7).wrapping_add(3),
        12,
    );

    assert_log_safety(&cluster);
}

#[test]
fn log_safety_holds_across_seeds_n3_with_a_restarted_replica() {
    for seed in 0..8u64 {
        scenario_with_restart(3, seed, seed.wrapping_mul(31) + 1, 1);
    }
}

#[test]
fn log_safety_holds_across_seeds_n5_with_restarted_replicas() {
    for seed in 0..6u64 {
        scenario_with_restart(5, seed, seed.wrapping_mul(41) + 3, 2);
    }
}

/// Excess crashes (more than `f`) may cost *liveness* but must never cost
/// *safety* (P11): the log-safety invariant still holds over whatever
/// prefix each replica applied before quorum was lost.
///
/// # This test used to establish nothing
///
/// Its earlier form crashed the majority *first* and only then submitted
/// work. Measured at the commit that added this note: every replica ended
/// with `next_slot=0, log_len=0`, so `assert_log_safety` compared five
/// empty logs and an all-zero frontier -- vacuously true. It survived all
/// three mutations that kill the other six tests in this file (see the
/// module docs), which is the signature of an instrument with **zero**
/// detection power, not of a robust one.
///
/// So a majority now decides a real prefix *before* the excess crashes,
/// and the test asserts that prefix is non-empty. That is also the more
/// faithful reading of P11: the interesting claim is not "nothing decided
/// and nothing diverged", it is "what was decided survives losing quorum".
///
/// Falsifier, run: the P5-A and P7-A/P7-B mutations from the module docs
/// each fail this 1/1, at the same assertions they fail the other tests
/// at. Its earlier form survived all three.
#[test]
fn log_safety_holds_even_without_a_live_majority() {
    let mut cluster = SmrCluster::new(
        123,
        SchedulerKind::Oblivious(Box::new(ContentObliviousAdversary::new(1, 4))),
        5,
    );

    // Decide a real prefix while a majority is still up.
    run_random_workload(&mut cluster, 998, 8);
    let applied_before: Vec<usize> = cluster
        .replicas()
        .iter()
        .map(|&r| cluster.applied_log(r).len())
        .collect();
    let longest_before = applied_before.iter().copied().max().unwrap_or(0);
    assert!(
        longest_before > 0,
        "anti-vacuity: no slot decided before the crashes, so the assertion below \
         would compare empty logs and establish nothing (applied: {applied_before:?})"
    );

    // Now lose quorum: 3 of 5 down is more than f = 2.
    cluster.crash(NodeId(2));
    cluster.crash(NodeId(3));
    cluster.crash(NodeId(4));
    run_random_workload(&mut cluster, 999, 6);

    // Safety: the surviving prefix is still consistent everywhere.
    assert_log_safety(&cluster);

    // Liveness cost: the two live replicas cannot decide anything new
    // without a majority. Asserted rather than assumed, because a run in
    // which they *did* make progress would mean the crashes never took
    // effect -- which would quietly turn this back into a test of the
    // healthy path.
    let longest_after = cluster
        .replicas()
        .iter()
        .map(|&r| cluster.applied_log(r).len())
        .max()
        .unwrap_or(0);
    assert_eq!(
        longest_after, longest_before,
        "no majority is live, so no further slot may be decided -- the longest \
         applied log grew from {longest_before} to {longest_after}"
    );
}
