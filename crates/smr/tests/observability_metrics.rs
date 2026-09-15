//! D10 observability (#129): the per-replica consensus counters
//! [`queso_smr::NodeMetrics`] exposes and `queso-net`'s `GET /metrics`
//! serves.
//!
//! §D names five metrics; before #129 `/metrics` served none of them. Three
//! are counted here -- per-slot rounds, fast-path hit rate and proposer
//! activations -- as the raw counters they are derived from. The other two
//! (recovery time, per-replica self-observed latency) need a new
//! measurement point rather than a new counter and are tracked separately;
//! see `docs/02-properties.md`'s D10 and the matrix's §6.19.
//!
//! # What these tests are for, and what they are not
//!
//! A counter that nothing reads is trivially "implemented". These assert
//! the counters track the thing they are named after, which is the claim
//! that could be wrong: that `decisions` follows the frontier this replica
//! drove, that `rounds_total` reflects the round a decision landed in, that
//! `fast_path_decisions` moves only on an actual fast path, and that
//! `on_restart` clears all of it.
//!
//! They deliberately do *not* assert an absolute round count for a
//! contended run: how many rounds a contested slot takes is a scheduling
//! outcome, not a property, and pinning it would make this a change-detector
//! for the scheduler. The invariants asserted instead (`rounds_total >=
//! decisions`, `fast_path_decisions <= decisions`) hold for any schedule.
//!
//! # Detection power (measured)
//!
//! Falsifier, run: eight mutations of the counter pipeline, each applied to
//! a clean tree and scored against this file plus
//! `crates/net/tests/status.rs`'s
//! `metrics_endpoint_serves_the_consensus_counters`. Control: the unmutated
//! tree passes both files. Harness and mutation list:
//! `scratchpad/129/mutate.py` (not checked in -- the mutations are listed
//! here in full).
//!
//! | mutation | caught by |
//! |---|---|
//! | drop `st.metrics.decisions += 1` | all three here, plus the endpoint test |
//! | `decided_via_fast_path()` -> `false` | `an_uncontended_...`, `a_restart_...` |
//! | drop `st.metrics = NodeMetrics::default()` in `on_restart` | `a_restart_...` |
//! | `rounds_total += step()/4` -> `+= 0` | `an_uncontended_...`, `decisions_counts_...`, endpoint |
//! | drop `metrics.proposer_activations += 1` | `an_uncontended_...`, `a_restart_...`, endpoint |
//! | driver never calls `publish_consensus` | endpoint only |
//! | `decisions = next_slot + 1` in `finish_attempt` | `a_restart_...` |
//! | `observability()` derives `decisions` from `next_slot` | `a_restart_...` |
//!
//! 8 of 8 killed. Five of the eight are registered in
//! `falsifiers/registry.toml` (`d10-drop-decisions`,
//! `d10-fast-path-never-counted`, `d10-no-restart-reset`,
//! `d10-decisions-from-frontier`, `d10-publish-is-a-noop`) so
//! `falsifiers/replay.py` re-measures them instead of leaving this table to
//! rot -- which is the defect §6.8 of the matrix recorded about pasted kill
//! counts generally.
//!
//! Two of those results are worth reading rather than counting:
//!
//! - The last two are both "`decisions` is really the durable frontier in
//!   disguise", and **only the restart test catches either**. That is not a
//!   gap in the other two tests, it is a fact about the design: every slot
//!   a replica applies is applied inside `finish_attempt` (enumerated --
//!   the crate has exactly one `applied_log.push`), so within a single
//!   process lifetime `decisions` and `next_slot` advance together and no
//!   test that never restarts can separate them. They separate only across
//!   a restart, where the frontier survives and the counters do not.
//! - That is also why `SmrCluster::metrics` and `SmrNode::metrics` were
//!   made to share `ReplicaState::observability`. Measured: with the two
//!   as independent one-line reads, the mutation of the *node's* accessor
//!   survived all four tests -- the sim tests (which have the restart) read
//!   the state directly, and the endpoint test (which goes through the
//!   node) has no restart. Unifying them moved that mutant from *survived*
//!   to *caught*.
//!
//! What remains uncovered, stated rather than glossed: the two one-line
//! wrappers themselves, on a path with a restart. Closing that needs a
//! restart in the real-process status suite, which is tracked separately
//! rather than claimed here.

use queso_sim::ids::NodeId;
use queso_sim::scheduler::{Fifo, SchedulerKind};
use queso_smr::{ClientId, SmrCluster};

/// An uncontended cluster with a fixed leader: every slot the leader
/// proposes for should decide in round 1's first step, so `rounds_total`
/// equals `decisions` and every decision is a fast-path one.
///
/// This is the fast-path hit rate's positive case. Without it the counter
/// could sit at zero forever and every "is it <= decisions" invariant would
/// still hold.
#[test]
fn an_uncontended_cluster_decides_on_the_fast_path() {
    let leader = NodeId(0);
    let mut cluster = SmrCluster::new_with_leader(
        7,
        SchedulerKind::Oblivious(Box::new(Fifo::new(1))),
        3,
        Some(leader),
    );

    for seq in 0..5u64 {
        cluster.submit_put(leader, ClientId(1), seq, seq as u32, seq as i64 * 10);
        cluster.run_for(500);
    }

    let m = cluster.metrics(leader);
    assert_eq!(
        m.decisions, 5,
        "the leader drove five slots to a decision, so `decisions` must be 5: {m:?}"
    );
    assert_eq!(
        m.fast_path_decisions, m.decisions,
        "an uncontended fixed-leader run has no reason to leave round 1's first \
         step, so every decision must be a fast-path one: {m:?}"
    );
    assert_eq!(
        m.rounds_total, m.decisions,
        "a fast-path decision is by definition one round, so `rounds_total` must \
         equal `decisions` here: {m:?}"
    );
    assert!(
        m.proposer_activations >= m.decisions,
        "a proposer cannot decide without having sent a `record` request, so every \
         decision implies an activation: {m:?}"
    );
}

/// `decisions` counts slots *this replica finished its own attempt for* --
/// its own proposal, or a catch-up probe that learned an already-decided
/// value -- and nothing else. A replica that never proposed for a slot must
/// not count it.
///
/// That population is the denominator of the fast-path hit rate, and it is
/// the one that makes the rate mean anything: a denominator including slots
/// this replica never attempted would report a hit rate for decisions it
/// had no part in.
///
/// Submitting every operation at one replica and reading the counters at a
/// *different* one is what separates the two.
#[test]
fn decisions_counts_slots_this_replica_decided() {
    let leader = NodeId(0);
    let bystander = NodeId(1);
    let mut cluster = SmrCluster::new_with_leader(
        11,
        SchedulerKind::Oblivious(Box::new(Fifo::new(1))),
        3,
        Some(leader),
    );

    for seq in 0..4u64 {
        cluster.submit_put(leader, ClientId(1), seq, seq as u32, 100 + seq as i64);
        cluster.run_for(500);
    }

    let driver = cluster.metrics(leader);
    let watcher = cluster.metrics(bystander);
    assert_eq!(
        driver.decisions, 4,
        "the submitting replica proposed for all four slots: {driver:?}"
    );
    assert_eq!(
        watcher.decisions, 0,
        "a replica that never proposed must not count those slots as its own \
         decisions -- if it did, a fast-path hit rate would have a denominator \
         including slots this replica never proposed for: {watcher:?}"
    );

    // And the invariants that must hold for any schedule, on both.
    for (who, m) in [("driver", driver), ("watcher", watcher)] {
        assert!(
            m.rounds_total >= m.decisions,
            "{who}: every decision lands in round >= 1, so rounds_total >= \
             decisions: {m:?}"
        );
        assert!(
            m.fast_path_decisions <= m.decisions,
            "{who}: the fast path is a subset of decisions: {m:?}"
        );
    }
}

/// The counters are volatile, per-process observability: a restart zeroes
/// them, exactly as a real restart does by being a new process with a fresh
/// `queso_net::status::StatusShared` (whose `uptime_secs` restarts for the
/// same reason).
///
/// The durable frontier is asserted alongside as the control: it must
/// *survive* the same restart that clears the counters, so a failure here
/// cannot be read as "the restart lost everything".
///
/// One counter is deliberately *not* asserted to be zero immediately after
/// the restart. `on_restart` clears the counters and then calls
/// `begin_catch_up`, whose proposer activates synchronously, so the
/// catch-up attempt's own activation is the first thing the fresh process
/// counts (measured: 1, against 3 before the restart). That ordering is
/// what we want -- work the restarted process does belongs to the restarted
/// process -- so the assertion is that activations dropped, not that they
/// vanished.
#[test]
fn a_restart_clears_the_counters() {
    let leader = NodeId(0);
    let mut cluster = SmrCluster::new_with_leader(
        23,
        SchedulerKind::Oblivious(Box::new(Fifo::new(1))),
        3,
        Some(leader),
    );

    for seq in 0..3u64 {
        cluster.submit_put(leader, ClientId(1), seq, seq as u32, seq as i64);
        cluster.run_for(500);
    }
    let before = cluster.metrics(leader);
    let slots_before = cluster.next_slot(leader);
    assert!(
        before.decisions > 0 && before.fast_path_decisions > 0,
        "the run must have counted something before the restart, or this test \
         asserts nothing: {before:?}"
    );

    cluster.crash(leader);
    cluster.restart(leader);

    let after = cluster.metrics(leader);
    assert_eq!(
        after.decisions, 0,
        "`decisions` is per-process and must not survive a restart: {after:?}"
    );
    assert_eq!(
        after.fast_path_decisions, 0,
        "`fast_path_decisions` is per-process and must not survive a restart: {after:?}"
    );
    assert_eq!(
        after.rounds_total, 0,
        "`rounds_total` is per-process and must not survive a restart: {after:?}"
    );
    assert!(
        after.proposer_activations < before.proposer_activations,
        "activations are per-process too: the only ones left may be the \
         catch-up attempt's own, started after the reset: before={before:?} \
         after={after:?}"
    );
    assert_eq!(
        slots_before,
        cluster.next_slot(leader),
        "control: the *durable* frontier must survive the restart that cleared \
         the volatile counters -- otherwise this test is passing because the \
         restart lost everything, not because the counters are volatile"
    );

    // And the restarted process counts *from* zero rather than picking the
    // old total back up: catching up decides its own slot, which lands the
    // counter below where it was, not above.
    cluster.run_for(500);
    let after_run = cluster.metrics(leader);
    assert!(
        after_run.decisions > 0 && after_run.decisions < before.decisions,
        "post-restart decisions must count the restarted process's own work \
         only: before={before:?} after_run={after_run:?}"
    );
}
