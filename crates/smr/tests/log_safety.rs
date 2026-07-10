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

#[test]
fn log_safety_holds_even_without_a_live_majority() {
    // Excess crashes (more than f) may cost liveness but must never cost
    // safety (P11) -- the log-safety invariant must still hold over
    // whatever (possibly very short, possibly empty) prefix each replica
    // did manage to apply.
    let mut cluster = SmrCluster::new(
        123,
        SchedulerKind::Oblivious(Box::new(ContentObliviousAdversary::new(1, 4))),
        5,
    );
    cluster.crash(NodeId(2));
    cluster.crash(NodeId(3));
    cluster.crash(NodeId(4));
    run_random_workload(&mut cluster, 999, 6);
    assert_log_safety(&cluster);
}
