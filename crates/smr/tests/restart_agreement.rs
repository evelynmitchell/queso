//! Issue #83: hunting the Agreement (P1) violation the nightly soak caught —
//! a restarted replica applying its own catch-up probe at a slot the
//! majority decided differently.
//!
//! # Why `log_safety.rs` does not already cover this
//!
//! That file's restart scenarios are real coverage, but they differ from the
//! failing soak configuration in two ways that both matter here:
//!
//! 1. **They are leaderless.** `SmrCluster::new` passes `leader: None`, so
//!    the §4.2.5 fast path is never armed. The soak runs
//!    `ClusterConfig { leader: 0, .. }` — a fixed leader — and the replica
//!    that diverged *was* that leader.
//! 2. **They crash before any work is submitted.** The victim therefore goes
//!    down with empty durable state and has never answered a `record` RPC.
//!    The hazard `queso_smr::Durable`'s own docs describe is the opposite
//!    case: a recorder that *had* already seen an earlier step, losing it
//!    across a restart, and so answering a later `record` as if it never
//!    had.
//!
//! So the scenarios here crash and restart a replica that has been
//! participating, repeatedly, mid-workload, with a fixed leader — and by
//! default the victim *is* the leader, matching the soak.
//!
//! # Anti-vacuity
//!
//! A crash-restart test proves nothing if the victim had nothing to lose.
//! Every scenario therefore asserts, before each crash, that the victim has
//! actually applied commands and actually holds recorder state for a slot it
//! decided. Without that check this file could pass forever while testing an
//! empty replica — which is exactly how the existing coverage missed this.

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use queso_consensus::H;
use queso_sim::ids::NodeId;
use queso_sim::scheduler::{ContentObliviousAdversary, SchedulerKind};
use queso_smr::{ClientId, Command, SmrCluster};

/// Submit a randomized workload, interleaved with running the kernel so
/// operations genuinely overlap in logical time. Mirrors `log_safety.rs`'s
/// helper; `seq_base` keeps `(client, seq)` pairs distinct across the
/// several bursts a scenario runs, so P8a dedup never silently absorbs one.
fn run_workload(cluster: &mut SmrCluster, workload_seed: u64, ops: usize, seq_base: u64) {
    let mut rng = StdRng::seed_from_u64(workload_seed);
    let live: Vec<NodeId> = cluster.live().iter().copied().collect();
    if live.is_empty() {
        return;
    }
    for i in 0..ops {
        let replica = live[rng.gen_range(0..live.len())];
        let client = ClientId(rng.gen_range(0..4));
        let seq = seq_base + i as u64;
        let key = rng.gen_range(0..3);
        if rng.gen_bool(0.5) {
            cluster.submit_put(replica, client, seq, key, rng.gen_range(0..1000));
        } else {
            cluster.submit_get(replica, client, seq, key);
        }
        cluster.run_for(rng.gen_range(1..40));
    }
    cluster.run_for(200_000);
}

/// P5/P6/P7: wherever two replicas both have an entry for a slot, it is the
/// same entry, and a frontier always matches applied-log length.
///
/// On failure this prints the earliest differing slot and both commands —
/// the shape `queso_soak::postmortem` produces for a real occurrence, since
/// a later difference is a consequence of the first, not a second fault.
fn assert_log_safety(cluster: &SmrCluster) {
    let replicas = cluster.replicas().to_vec();
    let logs: Vec<Vec<Command>> = replicas.iter().map(|&r| cluster.applied_log(r)).collect();

    for (r, log) in replicas.iter().zip(&logs) {
        assert_eq!(
            cluster.next_slot(*r) as usize,
            log.len(),
            "P7: replica {r}'s frontier must match its applied-log length"
        );
    }

    for i in 0..replicas.len() {
        for j in i + 1..replicas.len() {
            let overlap = logs[i].len().min(logs[j].len());
            for slot in 0..overlap {
                assert_eq!(
                    logs[i][slot], logs[j][slot],
                    "P1/P6 VIOLATED: {} and {} differ at slot {slot}\n  {} applied {:?}\n  {} applied {:?}\n\
                     (frontiers: {} at {}, {} at {})",
                    replicas[i],
                    replicas[j],
                    replicas[i],
                    logs[i][slot],
                    replicas[j],
                    logs[j][slot],
                    replicas[i],
                    logs[i].len(),
                    replicas[j],
                    logs[j].len(),
                );
            }
        }
    }
}

/// How many slots the victim holds recorder (ISR) state for.
///
/// Deliberately a scan rather than a check at the frontier: **a replica can
/// apply a slot while holding no recorder for it.** Recorders are created
/// lazily, the first time some proposer touches that slot on this replica,
/// and a replica whose own attempt decided a slot need never have been sent
/// a `record` for it. Asserting "applied implies recorded" looks obvious and
/// is false — it failed here on the first run.
fn recorded_slots(cluster: &SmrCluster, victim: NodeId) -> usize {
    (0..cluster.next_slot(victim))
        .filter(|&slot| cluster.recorder_summary(victim, slot).is_some())
        .count()
}

/// The victim must have something to lose across the crash, or the scenario
/// is testing an empty replica. Returns how many slots it had applied.
fn assert_victim_has_durable_state(cluster: &SmrCluster, victim: NodeId, when: &str) -> usize {
    let applied = cluster.applied_log(victim).len();
    let recorded = recorded_slots(cluster, victim);
    assert!(
        applied > 0,
        "{when}: victim {victim} has applied nothing, so crashing it risks nothing \
         -- this scenario would be vacuous"
    );
    assert!(
        recorded > 0,
        "{when}: victim {victim} holds recorder state for no slot at all, so a restart \
         cannot lose any -- the durability hazard this test targets is not present"
    );
    applied
}

/// Run bursts until the victim has durable state worth losing, or give up.
///
/// A fixed warm-up is not enough: with message drops and a fixed leader, a
/// follower can legitimately apply nothing across eight operations, and the
/// scenario would then abort on its own anti-vacuity guard rather than test
/// anything. Bounded so a genuinely stuck cluster fails loudly instead of
/// looping.
fn warm_up(cluster: &mut SmrCluster, victim: NodeId, workload_seed: u64) {
    for round in 0..6 {
        run_workload(cluster, workload_seed.wrapping_add(round), 8, round * 10);
        if !cluster.applied_log(victim).is_empty() && recorded_slots(cluster, victim) > 0 {
            return;
        }
    }
    panic!(
        "victim {victim} still has no durable state after 6 warm-up bursts \
         (applied {}, recorded {}) -- the cluster is not making progress",
        cluster.applied_log(victim).len(),
        recorded_slots(cluster, victim)
    );
}

/// Crash and restart `victim` `cycles` times, mid-workload, with a fixed
/// leader so the §4.2.5 fast path is armed throughout.
fn scenario(
    n: usize,
    seed: u64,
    workload_seed: u64,
    leader: NodeId,
    victim: NodeId,
    cycles: usize,
) {
    let adversary = ContentObliviousAdversary::new(1, 8).with_drop_probability(0.15);
    let mut cluster = SmrCluster::new_with_leader(
        seed,
        SchedulerKind::Oblivious(Box::new(adversary)),
        n,
        Some(leader),
    );

    // Let the victim accumulate real durable state before anything is taken
    // from it.
    warm_up(&mut cluster, victim, workload_seed);
    let mut applied_before =
        assert_victim_has_durable_state(&cluster, victim, "before first crash");

    for cycle in 0..cycles {
        cluster.crash(victim);
        // Work happens while it is down, so it comes back genuinely behind
        // and must catch up rather than resume in step.
        run_workload(
            &mut cluster,
            workload_seed.wrapping_mul(7).wrapping_add(cycle as u64),
            6,
            100 + cycle as u64 * 100,
        );
        cluster.restart(victim);
        run_workload(
            &mut cluster,
            workload_seed.wrapping_mul(13).wrapping_add(cycle as u64),
            6,
            500 + cycle as u64 * 100,
        );

        let applied_now = assert_victim_has_durable_state(
            &cluster,
            victim,
            &format!("after restart cycle {cycle}"),
        );
        assert!(
            applied_now >= applied_before,
            "a restarted replica must never lose applied slots: had {applied_before}, now {applied_now}"
        );
        applied_before = applied_now;
    }

    // The cluster has to have got somewhere, or agreement over a handful of
    // slots is a weak claim.
    let frontier = cluster
        .replicas()
        .iter()
        .map(|&r| cluster.next_slot(r))
        .max()
        .unwrap_or(0);
    assert!(
        frontier >= 12,
        "the cluster only reached slot {frontier}; agreement over that little proves little"
    );

    assert_log_safety(&cluster);

    // Anti-vacuity for the *mechanism*, not just the outcome: a scenario
    // where no catch-up probe ever reached a decision would never have
    // touched the path #83 broke on, and would pass for the wrong reason.
    // A decided probe is normal — `finish_attempt`'s `CatchUp` arm treats
    // "our own probe won" as "we were already at the true frontier" — and
    // when one wins, *every* replica must apply it at that slot. Measured at
    // 2 per scenario (one per restart cycle) across every seed here.
    let decided_probes = cluster
        .replicas()
        .iter()
        .map(|&r| {
            cluster
                .applied_log(r)
                .iter()
                .filter(|c| matches!(c, Command::Get { client, .. } if client.0 == u32::MAX))
                .count()
        })
        .max()
        .unwrap_or(0);
    assert!(
        decided_probes > 0,
        "no catch-up probe reached a decision, so this scenario never exercised the \
         restart path it exists to test"
    );
}

#[test]
fn a_repeatedly_restarted_leader_never_diverges_n3() {
    for seed in 0..24u64 {
        scenario(3, seed, seed.wrapping_mul(31) + 1, NodeId(0), NodeId(0), 2);
    }
}

#[test]
fn a_repeatedly_restarted_follower_never_diverges_n3() {
    for seed in 0..24u64 {
        scenario(3, seed, seed.wrapping_mul(37) + 5, NodeId(0), NodeId(1), 2);
    }
}

#[test]
fn a_repeatedly_restarted_leader_never_diverges_n5() {
    for seed in 0..12u64 {
        scenario(5, seed, seed.wrapping_mul(41) + 3, NodeId(0), NodeId(0), 2);
    }
}

// ---------------------------------------------------------------------
// The precondition behind `fast_path_value`'s uniformity check.
// ---------------------------------------------------------------------

/// Is this the restart catch-up probe (`CATCH_UP_CLIENT`, private to
/// `queso_smr::replica`)?
fn is_catch_up_probe(command: &Command) -> bool {
    matches!(command, Command::Get { client, .. } if client.0 == u32::MAX)
}

/// Every `(replica, slot)` recorder currently holding an `H`-priority
/// proposal, as `(slot, value, origin)`.
///
/// `IsrSummary::first` is `F[S]` at the recorder's *current* step, not
/// specifically `F[4]`. So this is a post-hoc snapshot, not a trace: once a
/// slot advances past round 1 everywhere, whatever `F[4]` held is no longer
/// visible here. It can therefore establish that an `H`-tagged proposal
/// exists, but **not** that one never existed — see
/// `the_mixed_h_state_was_not_observed_by_this_snapshot` for what that
/// costs.
fn live_h_proposals(cluster: &SmrCluster, upto: u64) -> Vec<(u64, Command, NodeId)> {
    let mut found = Vec::new();
    for &replica in cluster.replicas() {
        for slot in 0..upto {
            if let Some(summary) = cluster.recorder_summary(replica, slot) {
                if let Some(proposal) = summary.first {
                    if proposal.priority == H {
                        found.push((slot, proposal.value, proposal.origin));
                    }
                }
            }
        }
    }
    found
}

/// Run the standard leader-crash scenario and return every `H` proposal
/// left visible in recorder state afterwards.
fn h_proposals_after_leader_restarts(seed: u64) -> Vec<(u64, Command, NodeId)> {
    let adversary = ContentObliviousAdversary::new(1, 8).with_drop_probability(0.15);
    let mut cluster = SmrCluster::new_with_leader(
        seed,
        SchedulerKind::Oblivious(Box::new(adversary)),
        3,
        Some(NodeId(0)),
    );
    let workload_seed = seed.wrapping_mul(31) + 1;
    warm_up(&mut cluster, NodeId(0), workload_seed);
    for cycle in 0..2u64 {
        cluster.crash(NodeId(0));
        run_workload(
            &mut cluster,
            workload_seed.wrapping_mul(7).wrapping_add(cycle),
            6,
            100 + cycle * 100,
        );
        cluster.restart(NodeId(0));
        run_workload(
            &mut cluster,
            workload_seed.wrapping_mul(13).wrapping_add(cycle),
            6,
            500 + cycle * 100,
        );
    }
    let upto = cluster
        .replicas()
        .iter()
        .map(|&r| cluster.next_slot(r))
        .max()
        .unwrap_or(0)
        + 8;
    live_h_proposals(&cluster, upto)
}

/// **A restarted leader's catch-up probe carries the reserved priority `H`.**
///
/// This is the precondition for the input
/// `queso_consensus::proposer::fast_path_value` now refuses, and without it
/// that refusal would be guarding a state nothing can produce.
///
/// The path: `SmrNode::begin_catch_up` builds the probe's `Proposer` with
/// `leader_policy.leader_for(slot)` — the same leader hint an ordinary
/// client proposal gets — and a fresh `Proposer` starts at
/// `FIRST_ROUND_STEP`. `Proposer::begin_step`'s `is_fast_path_round` is
/// therefore true for a leader proposing its own probe, and §4.2.5 attaches
/// `H`. So a leader that crashes with an in-flight proposal at slot `k` and
/// restarts issues a **second, differently-valued** `H` proposal at `k`.
///
/// `Proposer::leader`'s docs anticipate two *different* replicas each
/// attaching `H`. This is the same replica, twice, at one slot — which the
/// docs did not anticipate and the type system does not prevent.
///
/// # This test is a tripwire for the fix
///
/// The candidate fix for #83 is that a catch-up probe must never carry `H`:
/// it is a *learning* operation, not a proposal, and has no business on the
/// fast path. When that lands, this test goes red. **That is correct and
/// intended** — invert it to assert the probe is never `H`, and treat the
/// red as confirmation the fix reached the path it was aimed at.
#[test]
fn a_restarted_leaders_catch_up_probe_carries_the_reserved_priority() {
    let mut seeds_with_probe = 0usize;
    let mut probes = 0usize;
    for seed in 0..24u64 {
        let found = h_proposals_after_leader_restarts(seed);

        // Anti-vacuity: `H` must be present at all, or "we found no
        // H-tagged probe" would just mean the scenario produced no fast
        // path to look at.
        assert!(
            !found.is_empty(),
            "seed {seed}: no H-priority proposal anywhere in recorder state, so the leader \
             fast path never armed and this seed proves nothing about probes"
        );

        let seed_probes = found
            .iter()
            .filter(|(_, command, _)| is_catch_up_probe(command))
            .count();
        for (slot, command, origin) in found.iter().filter(|(_, c, _)| is_catch_up_probe(c)) {
            assert_eq!(
                *origin,
                NodeId(0),
                "seed {seed}: an H-tagged catch-up probe at slot {slot} originated at \
                 {origin}, which is not the configured leader -- H is never drawn \
                 randomly, so only a leader can attach it ({command:?})"
            );
        }
        if seed_probes > 0 {
            seeds_with_probe += 1;
            probes += seed_probes;
        }
    }

    assert!(
        seeds_with_probe >= 12,
        "only {seeds_with_probe}/24 seeds left an H-tagged catch-up probe visible \
         (measured: 24/24, {probes} probes). Far fewer means either the scenario stopped \
         restarting the leader into catch-up, or the probe stopped carrying H -- if the \
         latter, #83's fix has landed and this test should be inverted, not relaxed"
    );
}

/// The companion negative result, recorded so it is not re-derived.
///
/// Scanning the same 24 scenarios for a slot where two recorders hold `H`
/// with *different* proposals — the exact input `fast_path_value` refuses —
/// finds none. That is **not** evidence the state is unreachable, and this
/// test does not assert that it is. `IsrSummary::first` is `F[S]` at the
/// recorder's current step, so a mixed `F[4]` that has since been
/// superseded everywhere is invisible to this snapshot; and `Kernel::restart`
/// reuses the same heap-resident node, so the sim's restart is gentler than
/// a real one in ways `crates/net/tests/persist_fidelity.rs` documents.
///
/// What it does assert is that the count is *zero*, so that if a future
/// change makes the mixed state common this test goes red and says so. A
/// silent drift from "never observed" to "routine" is the kind of thing
/// that stayed invisible through five occurrences of #83.
#[test]
fn the_mixed_h_state_was_not_observed_by_this_snapshot() {
    let mut mixed = Vec::new();
    for seed in 0..24u64 {
        let found = h_proposals_after_leader_restarts(seed);
        let mut by_slot: std::collections::BTreeMap<u64, std::collections::BTreeSet<String>> =
            std::collections::BTreeMap::new();
        for (slot, command, origin) in found {
            by_slot
                .entry(slot)
                .or_default()
                .insert(format!("{origin} {command:?}"));
        }
        for (slot, values) in by_slot {
            if values.len() > 1 {
                mixed.push((seed, slot, values));
            }
        }
    }
    assert!(
        mixed.is_empty(),
        "the mixed-H state is now observable in the simulator, which it was not when this \
         test was written -- this is a *result*, not a regression: reproduce #83 from these \
         scenarios rather than from the soak. Found: {mixed:?}"
    );
}
