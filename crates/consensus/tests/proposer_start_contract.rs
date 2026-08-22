//! Issue #13: the `Proposer::start` re-kick contract.
//!
//! `ConcreteCluster::run_slot` re-injects every live replica's
//! `KICKOFF_TIMER` on every call, so `start` is not a once-per-proposer
//! call in practice -- any test that drives a slot in more than one push
//! (partition-then-heal, run-past-decision) calls it again on proposers
//! that are mid-flight or already finished. `Proposer::start`'s docs state
//! the contract that governs those repeat calls; this file is what pins it:
//!
//! - **undecided** -> restart round 1 (`step` back to `FIRST_ROUND_STEP`,
//!   first step re-issued), so a driver can un-stall a proposer that spent
//!   its whole first push partitioned away from every quorum;
//! - **decided** -> complete no-op: no `step` rewind, no `record` traffic,
//!   no change of decision or of the fast-path provenance
//!   `decided_via_fast_path` reports.
//!
//! Every assertion here was checked against a deliberately reverted
//! `start` (the guard removed) to confirm it fails -- see the per-test
//! notes.

use std::collections::BTreeMap;

use queso_consensus::ConcreteCluster;
use queso_sim::ids::NodeId;
use queso_sim::scheduler::{ContentObliviousAdversary, SchedulerKind};

const MAX_TICKS: u64 = 200_000;
/// `4 * 1 + 0` -- round 1, phase 0. Not re-exported from `queso_consensus`,
/// and deliberately spelled out here rather than imported: this test is
/// asserting *about* the constant, so it should not read it from the code
/// under test.
const FIRST_ROUND_STEP: u64 = 4;

fn initial_values(n: u32) -> BTreeMap<NodeId, u32> {
    (0..n).map(|i| (NodeId(i), i)).collect()
}

fn lossy_cluster(seed: u64, n: u32) -> ConcreteCluster<u32> {
    let scheduler = ContentObliviousAdversary::new(1, 6).with_drop_probability(0.25);
    ConcreteCluster::new(
        seed,
        SchedulerKind::Oblivious(Box::new(scheduler)),
        initial_values(n),
    )
}

/// Run until every live replica has decided, then let the run go quiet:
/// deliver every message still in flight and let every armed timer fire,
/// so that any traffic observed afterwards can only have been caused by
/// what the test does next. Returns the quiesced message count.
///
/// Quiescence is asserted, not assumed -- if the cluster still generates
/// traffic after two full drain windows, the tests below would be
/// measuring that instead of the re-kick, and this panics rather than
/// letting them silently pass.
fn run_to_quiescence(cluster: &mut ConcreteCluster<u32>, seed: u64) -> usize {
    cluster.run_slot(MAX_TICKS);
    assert!(
        cluster.all_live_decided(),
        "seed {seed}: no decision within the tick budget"
    );
    cluster.advance(5_000);
    let after_first_drain = cluster.message_count();
    cluster.advance(5_000);
    let after_second_drain = cluster.message_count();
    assert_eq!(
        after_first_drain, after_second_drain,
        "seed {seed}: cluster never went quiet ({after_first_drain} -> {after_second_drain} \
         messages across two idle drain windows), so a later \"the re-kick emitted no traffic\" \
         assertion would be measuring leftover in-flight work"
    );
    after_second_drain
}

/// The decided half of the contract, in full: re-kicking a cluster whose
/// live replicas have all decided must change *nothing* -- not the
/// decision, not the step, not the fast-path provenance, and not one byte
/// on the wire.
///
/// Falsifier: with the `decided.is_some()` guard removed from
/// `Proposer::start`, this fails on the very first seed at the
/// message-count assertion (a full round of `record` requests goes out for
/// an already-settled slot) and at the step assertion.
#[test]
fn re_kicking_a_decided_proposer_changes_nothing() {
    for seed in 0..40 {
        let mut cluster = lossy_cluster(seed, 5);
        let quiesced_messages = run_to_quiescence(&mut cluster, seed);

        let before: BTreeMap<NodeId, (u32, u64, bool)> = cluster
            .replicas()
            .iter()
            .map(|&id| {
                (
                    id,
                    (
                        cluster.decided(id).unwrap(),
                        cluster.step(id),
                        cluster.decided_via_fast_path(id),
                    ),
                )
            })
            .collect();

        // The re-kick under test: `run_slot` re-injects every live
        // replica's `KICKOFF_TIMER`, then runs long enough for anything it
        // set in motion to be observable.
        cluster.run_slot(1_000);

        assert_eq!(
            cluster.message_count(),
            quiesced_messages,
            "seed {seed}: re-kicking decided proposers put {} new message(s) on the wire for an \
             already-settled slot",
            cluster.message_count() - quiesced_messages
        );
        for (&id, &(value, step, via_fast_path)) in &before {
            assert_eq!(
                cluster.decided(id).unwrap(),
                value,
                "seed {seed}: replica {id} changed its decision after already deciding"
            );
            assert_eq!(
                cluster.step(id),
                step,
                "seed {seed}: replica {id}'s step was rewound by a re-kick after it had decided"
            );
            assert_eq!(
                cluster.decided_via_fast_path(id),
                via_fast_path,
                "seed {seed}: replica {id}'s fast-path provenance flipped after a re-kick"
            );
        }
    }
}

/// The sharpest consequence of the missing guard, isolated: rewinding a
/// decided proposer's `step` to `FIRST_ROUND_STEP` makes
/// `decided_via_fast_path` -- which is exactly `decided.is_some() && step
/// == FIRST_ROUND_STEP` -- report a phase-0 fast-path decision for a
/// replica that actually decided several steps into the ordinary
/// spread/gather machinery.
///
/// This runs leaderless and lossy specifically so that no decision *can*
/// be a genuine fast-path one, and asserts it found such a replica rather
/// than trusting that it did.
///
/// Falsifier: with the guard removed, every one of these replicas reports
/// `decided_via_fast_path() == true` after the re-kick.
#[test]
fn a_re_kick_does_not_forge_fast_path_provenance() {
    let mut checked = 0usize;
    for seed in 0..40 {
        let mut cluster = lossy_cluster(seed, 5);
        run_to_quiescence(&mut cluster, seed);

        let slow_deciders: Vec<NodeId> = cluster
            .replicas()
            .iter()
            .copied()
            .filter(|&id| !cluster.decided_via_fast_path(id))
            .collect();
        checked += slow_deciders.len();

        cluster.run_slot(1_000);

        for id in slow_deciders {
            assert!(
                !cluster.decided_via_fast_path(id),
                "seed {seed}: replica {id} decided past round 1 but claims a phase-0 fast-path \
                 decision after being re-kicked -- `start` rewound its step"
            );
        }
    }
    assert!(
        checked >= 40,
        "only {checked} non-fast-path deciders across the corpus -- too few for this test to be \
         asserting anything; the scenario is meant to make fast-path decisions impossible"
    );
}

/// The undecided half of the contract: a re-kick *restarts* a proposer
/// that has not decided, taking it back to round 1 phase 0 rather than
/// leaving it where it was. This is the half a driver depends on when it
/// runs a slot in several pushes -- `run_slot` is how a stalled proposer
/// gets told to try again.
///
/// The scenario is a mid-flight snapshot: run a lossy leaderless cluster
/// for a short budget, keep the replicas that are past `FIRST_ROUND_STEP`
/// and still undecided, re-kick, and check they are back at
/// `FIRST_ROUND_STEP` -- then let the run finish to confirm the restart
/// really is a catch-up (the recorders' ISR state is monotone and survives
/// independently of any proposer's step) and not a loss of progress.
///
/// The `run_slot(1)` is deliberate: zero-delay kickoff timers land at
/// `now + 1`, so one tick is exactly enough to observe the re-kick itself.
///
/// Falsifier: making `start` fully idempotent (returning early whenever
/// the proposer has already been started, not only once decided) leaves
/// these replicas parked at their pre-kick step and the rewind assertion
/// fails.
#[test]
fn re_kicking_a_stalled_undecided_proposer_restarts_round_one() {
    let mut observed = 0usize;
    for seed in 0..60 {
        for budget in [40u64, 80, 160, 320] {
            let mut cluster = lossy_cluster(seed, 5);
            cluster.run_slot(budget);

            // Replicas caught mid-protocol: past round 1, not yet decided.
            let mid_flight: BTreeMap<NodeId, u64> = cluster
                .replicas()
                .iter()
                .copied()
                .filter(|&id| cluster.decided(id).is_none() && cluster.step(id) > FIRST_ROUND_STEP)
                .map(|id| (id, cluster.step(id)))
                .collect();
            if mid_flight.is_empty() {
                continue;
            }
            cluster.run_slot(1);
            for (&id, &step_before) in &mid_flight {
                if cluster.decided(id).is_some() {
                    // It decided during the very tick the re-kick landed
                    // in -- a response already in flight beat the kickoff
                    // timer. The decided clause of the contract governs
                    // from here, so what must *not* have happened is a
                    // rewind.
                    assert_eq!(
                        cluster.step(id),
                        step_before,
                        "seed {seed} (budget {budget}): replica {id} decided at step \
                         {step_before} in the re-kick tick and was then rewound anyway"
                    );
                    continue;
                }
                observed += 1;
                assert_eq!(
                    cluster.step(id),
                    FIRST_ROUND_STEP,
                    "seed {seed} (budget {budget}): re-kicking undecided replica {id} (at step \
                     {step_before}) did not restart it at round 1"
                );
            }

            cluster.advance(MAX_TICKS);
            assert!(
                cluster.all_live_decided(),
                "seed {seed} (budget {budget}): a restarted proposer lost progress instead of \
                 catching back up -- the cluster never finished the slot"
            );
        }
    }
    assert!(
        observed >= 50,
        "only {observed} mid-flight undecided replicas across the corpus -- too few for the \
         rewind this test checks to be observable"
    );
}
