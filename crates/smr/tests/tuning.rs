//! Phase 6: auto-tuning (§5.3, D4) -- the multi-armed-bandit explore/exploit
//! layer that chooses the leader and hedging schedule dynamically, exercised
//! end-to-end on [`SmrCluster::new_with_tuning`].
//!
//! Four scenarios, matching the task brief:
//!
//! 1. **Convergence (D4)**: a heterogeneous deployment (one replica made
//!    genuinely slow via the harness's slow-node fault injection) --
//!    confirm the tuner converges to a *fast* replica as leader and that
//!    steady-state per-op latency measurably improves over a fixed-bad-
//!    leader baseline running the identical fault.
//! 2. **Exploration coverage**: every replica leads during the `2n+1`
//!    explore epochs, and the resulting exploit schedule reflects the
//!    measured speed order (slow replica ranked last).
//! 3. **Re-exploration**: degrading the *current* leader mid-run (no crash)
//!    causes the tuner to switch leaders, and every operation submitted
//!    throughout still completes -- no stall.
//! 4. **Safety unchanged**: log safety (P5/P6/P7) and linearizability (P8)
//!    hold across a seed corpus while the tuner is actively exploring and
//!    switching leaders, under a content-oblivious adversary; determinism
//!    (D9) is preserved.

use std::collections::BTreeSet;

use queso_sim::ids::NodeId;
use queso_sim::scheduler::{ContentObliviousAdversary, Fifo, SchedulerKind};
use queso_smr::{history_from_records, is_linearizable, ClientId, OpId, SmrCluster};

/// Submit `count` sequential `put`s, round-robin across `submitters`,
/// running the kernel forward a fixed slice after each submission so the
/// log keeps advancing, and return every operation's [`OpId`] in submission
/// order. `seq_start` lets a caller issuing more than one batch keep
/// `(client, seq)` sequence numbers strictly increasing across batches (A6).
///
/// A single-element `submitters` slice is deliberate in most of these tests
/// (rather than round-robining across the *whole* cluster): every operation
/// submitted to a replica is, by construction, that replica's own problem to
/// propose (`queso_smr::replica::SmrNode` only ever runs a replica's own
/// colocated proposer when *that* replica has queued client work -- see
/// `crate::replica`'s module docs; there is no leader-forwards-to-itself
/// relay). So a replica that is *itself* still receiving submissions after
/// being degraded remains, unavoidably, the proposer for those specific
/// operations regardless of which replica the tuner currently designates as
/// leader -- no leader choice can route around a replica's own inbox. That
/// is realistic (a real deployment would not keep steering new client
/// sessions at a replica already known to be unhealthy) but it means a test
/// that wants to observe the tuner's leader choice actually *improve*
/// things after a degradation must stop submitting through the
/// now-degraded replica -- see
/// `re_exploration_switches_leader_when_it_degrades_without_stalling` for
/// where `submitters` is more than one replica for exactly this reason.
fn drive_puts(
    cluster: &mut SmrCluster,
    submitters: &[NodeId],
    count: u64,
    slice: u64,
    seq_start: u64,
) -> Vec<OpId> {
    let mut ops = Vec::with_capacity(count as usize);
    for i in 0..count {
        let seq = seq_start + i;
        let replica = submitters[(i % submitters.len() as u64) as usize];
        let op = cluster.submit_put(replica, ClientId(1), seq, seq as u32, seq as i64);
        cluster.run_for(slice);
        ops.push(op);
    }
    ops
}

fn op_latency(cluster: &SmrCluster, op: OpId) -> u64 {
    let record = cluster.result(op).expect("op was submitted");
    let completed = record
        .completed_at
        .unwrap_or_else(|| panic!("op {op:?} never completed"));
    completed.0 - record.invoked_at.0
}

// ---------------------------------------------------------------------
// Scenario 1: D4 -- convergence to a fast leader, with a measurable
// latency improvement over a fixed-bad-leader baseline.
// ---------------------------------------------------------------------

#[test]
fn converges_to_a_fast_replica_and_beats_a_fixed_bad_leader_baseline() {
    let n = 3usize;
    let epoch_len = 4u64;
    let base_delay = 20u64;
    let slow_multiplier = 8u64;
    let num_ops = 70u64;

    // --- Auto-tuned cluster: replica 1 is genuinely slow throughout. ---
    let mut tuned = SmrCluster::new_with_tuning(
        1,
        SchedulerKind::Oblivious(Box::new(Fifo::new(1))),
        n,
        epoch_len,
        base_delay,
    );
    tuned.set_slow(NodeId(1), slow_multiplier);
    let tuned_ops = drive_puts(&mut tuned, &[NodeId(0)], num_ops, 1_500, 0);
    tuned.run_for(300_000);

    for &op in &tuned_ops {
        assert!(
            tuned.is_complete(op),
            "tuned cluster: op {op:?} never completed"
        );
    }
    assert!(
        !tuned.tuning_is_exploring().unwrap(),
        "workload should have run long enough to leave the explore phase"
    );
    assert_ne!(
        tuned.current_leader(),
        Some(NodeId(1)),
        "the tuner must not settle on the slow replica as leader"
    );

    // Steady-state latency: the last 15 ops, well after convergence.
    let steady: Vec<u64> = tuned_ops
        .iter()
        .rev()
        .take(15)
        .map(|&op| op_latency(&tuned, op))
        .collect();
    let tuned_avg = steady.iter().sum::<u64>() / steady.len() as u64;

    // --- Baseline: a fixed leader pinned to the slow replica, same fault. ---
    let mut baseline = SmrCluster::new_with_leader(
        1,
        SchedulerKind::Oblivious(Box::new(Fifo::new(1))),
        n,
        Some(NodeId(1)),
    );
    baseline.set_slow(NodeId(1), slow_multiplier);
    let baseline_ops = drive_puts(&mut baseline, &[NodeId(0)], num_ops, 1_500, 0);
    baseline.run_for(300_000);
    for &op in &baseline_ops {
        assert!(
            baseline.is_complete(op),
            "baseline cluster: op {op:?} never completed"
        );
    }
    let baseline_steady: Vec<u64> = baseline_ops
        .iter()
        .rev()
        .take(15)
        .map(|&op| op_latency(&baseline, op))
        .collect();
    let baseline_avg = baseline_steady.iter().sum::<u64>() / baseline_steady.len() as u64;

    assert!(
        tuned_avg < baseline_avg,
        "tuned steady-state avg latency ({tuned_avg}) should beat the fixed-bad-leader \
         baseline's ({baseline_avg})"
    );
}

// ---------------------------------------------------------------------
// Scenario 2: exploration coverage + exploit schedule reflects speed order.
// ---------------------------------------------------------------------

#[test]
fn every_replica_leads_during_exploration_and_the_exploit_schedule_reflects_speed_order() {
    let n = 4usize;
    let epoch_len = 3u64;
    let base_delay = 15u64;
    let slow_multiplier = 6u64;

    let mut c = SmrCluster::new_with_tuning(
        9,
        SchedulerKind::Oblivious(Box::new(Fifo::new(1))),
        n,
        epoch_len,
        base_delay,
    );
    c.set_slow(NodeId(3), slow_multiplier);

    let explore_epochs = c.tuning_explore_epochs().unwrap();
    // Enough ops to fully close every explore epoch plus a couple of
    // exploit epochs (see `EpochTuner::close_epoch`'s docs: an epoch closes
    // the instant its own last slot decides).
    let num_ops = explore_epochs * epoch_len + epoch_len * 3;
    drive_puts(&mut c, &[NodeId(0)], num_ops, 1_200, 0);
    c.run_for(300_000);

    assert!(
        !c.tuning_is_exploring().unwrap(),
        "should have finished exploring"
    );

    let leader_log = c.tuning_leader_log().unwrap();
    assert!(
        leader_log.len() as u64 > explore_epochs,
        "expected at least one exploit epoch to have been assigned"
    );
    let explore_leaders = &leader_log[..explore_epochs as usize];
    let distinct: BTreeSet<NodeId> = explore_leaders.iter().copied().collect();
    assert_eq!(
        distinct.len(),
        n,
        "every replica should have led at least once during exploration: {explore_leaders:?}"
    );
    for r in 0..n as u32 {
        let count = explore_leaders.iter().filter(|&&x| x == NodeId(r)).count();
        assert!(
            count >= 2,
            "replica {r} led only {count} explore epochs (expected >= 2, per the paper's \
             2n+1-epoch round robin): {explore_leaders:?}"
        );
    }

    // The exploit schedule must not lead with the slow replica, and must
    // rank it last (it is the only genuinely slow one).
    let schedule = c.tuning_schedule().unwrap();
    assert_eq!(schedule.len(), n);
    assert_ne!(
        schedule[0],
        NodeId(3),
        "slow replica should not be the exploit leader"
    );
    assert_eq!(
        *schedule.last().unwrap(),
        NodeId(3),
        "slow replica should rank last in the exploit hedging schedule: {schedule:?}"
    );
}

// ---------------------------------------------------------------------
// Scenario 3: re-exploration -- switching leaders without a crash, and
// without ever stalling submitted operations.
// ---------------------------------------------------------------------

#[test]
fn re_exploration_switches_leader_when_it_degrades_without_stalling() {
    let n = 3usize;
    let epoch_len = 3u64;
    let base_delay = 15u64;

    let mut c = SmrCluster::new_with_tuning(
        5,
        SchedulerKind::Oblivious(Box::new(Fifo::new(1))),
        n,
        epoch_len,
        base_delay,
    );

    let explore_epochs = c.tuning_explore_epochs().unwrap();
    let mut all_ops = drive_puts(&mut c, &[NodeId(0)], explore_epochs * epoch_len, 1_200, 0);
    c.run_for(50_000);
    assert!(
        !c.tuning_is_exploring().unwrap(),
        "should have finished exploring"
    );

    let leader_before = c
        .current_leader()
        .expect("a tuned cluster always has a current leader");

    // Degrade the *current* leader mid-run -- no crash, just slow -- and
    // keep driving the workload.
    // A large multiplier -- comfortably larger than the hedging schedule's
    // own bounded scheduling-position spread ((n-1) * base_delay), so the
    // genuine slowdown is the dominant signal, not schedule-position noise.
    c.set_slow(leader_before, 300);
    let switches_before = c.tuning_switch_count().unwrap();

    // From here on, submit through the *other* replicas only -- see
    // `drive_puts`'s docs for why continuing to submit through the
    // now-degraded replica itself would make its badness inescapable
    // regardless of which replica the tuner designates as leader (no
    // replica's own inbox can be routed around by a leader choice).
    let others: Vec<NodeId> = c
        .replicas()
        .iter()
        .copied()
        .filter(|&r| r != leader_before)
        .collect();
    let more_ops = drive_puts(
        &mut c,
        &others,
        epoch_len * 8,
        2_500,
        explore_epochs * epoch_len,
    );
    all_ops.extend(more_ops);
    c.run_for(200_000);

    let leader_after = c.current_leader().unwrap();
    assert_ne!(
        leader_after, leader_before,
        "the tuner should have switched away from the now-degraded leader"
    );
    assert!(
        c.tuning_switch_count().unwrap() > switches_before,
        "switch_count should have increased"
    );

    // Liveness: nothing submitted -- before or after the degradation and
    // switch -- ever stalled.
    for &op in &all_ops {
        assert!(c.is_complete(op), "op {op:?} stalled instead of completing");
    }
}

// ---------------------------------------------------------------------
// Issue #29: the kernel's leader hint under tuning, and tuning x restart.
// ---------------------------------------------------------------------

/// Every leader the kernel was told about, in order, read out of the trace.
fn kernel_leader_hints(cluster: &SmrCluster) -> Vec<Option<NodeId>> {
    cluster
        .trace()
        .events()
        .iter()
        .filter_map(|event| match event {
            queso_sim::trace::TraceEvent::LeaderChanged { leader, .. } => Some(*leader),
            _ => None,
        })
        .collect()
}

/// The kernel's leader hint must follow the tuner across epoch switches.
///
/// This is issue #29's middle item. `Kernel::set_leader` is the hook
/// adversary schedulers use to decide who to target, and a tuned cluster
/// used to set it exactly once, to epoch 0's leader. Any leader-targeting
/// adversary run under auto-tuning was therefore attacking a replica that
/// had long since stopped leading — a test that looked like it stressed the
/// leader while stressing a bystander.
///
/// The assertion is written against the tuner's own leader log rather than
/// a hardcoded expectation, so it stays true if the tuner's choices change.
#[test]
fn the_kernel_leader_hint_follows_the_tuner_across_epoch_switches() {
    let n = 4usize;
    let mut c = SmrCluster::new_with_tuning(
        11,
        SchedulerKind::Oblivious(Box::new(Fifo::new(1))),
        n,
        3,
        12,
    );
    drive_puts(
        &mut c,
        &[NodeId(0), NodeId(1), NodeId(2), NodeId(3)],
        40,
        2_000,
        0,
    );
    c.run_for(50_000);

    let hints = kernel_leader_hints(&c);
    let leader_log = c
        .tuning_leader_log()
        .expect("a tuned cluster has a leader log");

    // Anti-vacuity first: before the fix this vector had exactly one entry,
    // so a test that only checked "the hints are a subsequence of the log"
    // would have passed unchanged on the broken code.
    assert!(
        hints.len() > 1,
        "the kernel was told about the leader exactly {} time(s); under tuning the \
         hint must track epoch switches, not be set once and left stale",
        hints.len()
    );
    let distinct: std::collections::BTreeSet<_> = hints.iter().collect();
    assert!(
        distinct.len() > 1,
        "the kernel only ever heard about one leader ({distinct:?}), so a \
         leader-targeting adversary would have spent the run attacking a bystander"
    );

    // The hints are exactly the leader log with consecutive repeats
    // collapsed: the tuner keeps the same leader across many epochs during
    // exploitation, and `set_leader` is only called when the value actually
    // moves (it records a trace event, and a per-tick stream of no-ops would
    // swamp the trace).
    let mut expected: Vec<Option<NodeId>> = Vec::new();
    for leader in leader_log.iter().map(|&l| Some(l)) {
        if expected.last() != Some(&leader) {
            expected.push(leader);
        }
    }
    // The run may end mid-epoch, so the hints are a prefix of the collapsed
    // log rather than the whole of it.
    assert!(
        hints.len() <= expected.len(),
        "more hints ({}) than the tuner ever had distinct leaders ({}): {hints:?} vs {expected:?}",
        hints.len(),
        expected.len()
    );
    assert_eq!(
        hints,
        expected[..hints.len()],
        "the kernel's leader hints must be the tuner's leader log with consecutive \
         repeats collapsed"
    );
}

/// A leader-targeting adversary under auto-tuning: safety holds, and the
/// DoS really does move with the leader.
///
/// The existing adversary-under-tuning scenario uses a uniform drop
/// probability, which never reads the kernel's leader hint at all — so
/// nothing in the suite exercised `with_leader_dos` together with tuning,
/// which is exactly why the stale hint could sit there unnoticed. This is
/// the combination issue #29 asks for.
#[test]
fn a_leader_targeting_adversary_under_tuning_stays_safe_and_follows_the_leader() {
    for seed in 0..6u64 {
        let n = 4usize;
        let adversary = ContentObliviousAdversary::new(1, 6).with_leader_dos(0.9);
        let mut c = SmrCluster::new_with_tuning(
            seed,
            SchedulerKind::Oblivious(Box::new(adversary)),
            n,
            3,
            10 + seed % 10,
        );

        let submitters: Vec<NodeId> = (0..n as u32).map(NodeId).collect();
        drive_puts(&mut c, &submitters, 30, 3_000, 0);
        c.run_for(300_000);

        // Safety (P5/P6/P7): every applied log is a prefix of the longest.
        let logs: Vec<Vec<queso_smr::Command>> =
            c.replicas().iter().map(|&r| c.applied_log(r)).collect();
        let longest = logs
            .iter()
            .max_by_key(|l| l.len())
            .cloned()
            .unwrap_or_default();
        for (replica, log) in c.replicas().iter().zip(&logs) {
            assert_eq!(
                &longest[..log.len()],
                log.as_slice(),
                "seed={seed}: replica {replica} diverged under a leader-targeting adversary"
            );
        }

        let history = history_from_records(&c.results());
        assert!(
            is_linearizable(&history),
            "seed={seed}: history was not linearizable"
        );

        // The point of the scenario: the DoS target moved. Without the
        // hint-syncing fix this is 1 for every seed, and the whole run
        // hammers epoch 0's leader while the cluster quietly leads from
        // somewhere else.
        let distinct: std::collections::BTreeSet<_> = kernel_leader_hints(&c).into_iter().collect();
        assert!(
            distinct.len() > 1,
            "seed={seed}: the adversary only ever targeted one replica ({distinct:?}), \
             so this scenario did not actually test a leader-targeting adversary \
             *under tuning* -- it tested one against a bystander"
        );
    }
}

/// Issue #29's first item: tuning combined with crash and restart.
///
/// The invariant at stake is per-epoch leader **pinning**. The tuner stores
/// every epoch's configuration forever (`epoch_configs`) precisely so that a
/// replica proposing "late" — a restarted one catching up on old slots —
/// reconstructs the same leader that epoch always had. Nothing exercised
/// that directly: the existing restart tests use fixed leaders, and the
/// existing tuning tests never crash anything.
#[test]
fn tuning_survives_a_crash_and_restart_with_epoch_leaders_pinned() {
    for seed in 0..6u64 {
        let n = 4usize;
        let mut c = SmrCluster::new_with_tuning(
            seed,
            SchedulerKind::Oblivious(Box::new(Fifo::new(1))),
            n,
            3,
            10,
        );
        let submitters: Vec<NodeId> = (0..n as u32).map(NodeId).collect();

        // Establish some history and let the tuner get past exploration.
        drive_puts(&mut c, &submitters, 20, 2_000, 0);
        let log_before = c
            .tuning_leader_log()
            .expect("a tuned cluster has a leader log");
        assert!(
            log_before.len() > 1,
            "seed={seed}: the tuner should have opened several epochs before the crash"
        );

        // Snapshot what each already-decided slot's leader is *now*, so the
        // same question can be asked again after the restart.
        let decided_slots = c.next_slot(NodeId(0));
        assert!(
            decided_slots > 1,
            "seed={seed}: only {decided_slots} slot(s) decided before the crash -- \
             too few for the pinning check below to mean anything"
        );
        let pinned_before: Vec<(u64, NodeId)> = (0..decided_slots)
            .map(|slot| {
                (
                    slot,
                    c.tuning_leader_for_slot(slot)
                        .expect("a tuned cluster pins a leader per slot"),
                )
            })
            .collect();
        // Anti-vacuity: if every slot so far happened to share one leader,
        // a break that answered from the current epoch could still look
        // right. Require the snapshot to span at least two leaders.
        let distinct_pinned: std::collections::BTreeSet<_> =
            pinned_before.iter().map(|(_, l)| *l).collect();
        assert!(
            distinct_pinned.len() > 1,
            "seed={seed}: all {decided_slots} decided slots share one pinned leader \
             ({distinct_pinned:?}), so the pinning check could not distinguish a \
             per-slot lookup from a current-leader lookup"
        );

        // Crash a replica that is *not* the current leader, so the cluster
        // keeps a majority and owes progress throughout -- the same
        // discipline the soak's fault schedule follows. A crashed leader is
        // a legitimate scenario too, but it conflates "did tuning survive a
        // restart" with "did the cluster recover from losing its leader".
        let leader = c.current_leader().expect("a tuned cluster has a leader");
        let victim = c
            .replicas()
            .iter()
            .copied()
            .find(|&r| r != leader)
            .expect("some replica is not the leader");
        c.crash(victim);
        drive_puts(
            &mut c,
            &submitters
                .iter()
                .copied()
                .filter(|&r| r != victim)
                .collect::<Vec<_>>(),
            20,
            2_000,
            100,
        );

        c.restart(victim);
        drive_puts(&mut c, &submitters, 20, 2_000, 200);
        c.run_for(300_000);

        // The pinning invariant, checked where it actually lives: the
        // leader a proposer derives for an *old* slot must be the same
        // after the restart as before it. If catch-up could re-derive it
        // from the tuner's current leader instead, two replicas proposing
        // for the same old slot would disagree about who leads it.
        //
        // Snapshotted per slot rather than compared against the leader log:
        // the log is append-only and would look identical even if
        // `leader_for_slot` had started answering from the current epoch,
        // which is exactly the break this is meant to catch.
        let log_after = c
            .tuning_leader_log()
            .expect("a tuned cluster has a leader log");
        assert!(
            log_after.len() >= log_before.len(),
            "seed={seed}: the leader log shrank across a restart"
        );
        assert_eq!(
            &log_after[..log_before.len()],
            log_before.as_slice(),
            "seed={seed}: an epoch's recorded leader changed across a crash/restart"
        );
        for (slot, expected) in &pinned_before {
            assert_eq!(
                c.tuning_leader_for_slot(*slot),
                Some(*expected),
                "seed={seed}: slot {slot}'s pinned leader changed across a crash/restart"
            );
        }

        // Safety, and that the restarted replica actually rejoined rather
        // than parking as a zombie (#22's failure mode).
        let logs: Vec<Vec<queso_smr::Command>> =
            c.replicas().iter().map(|&r| c.applied_log(r)).collect();
        let longest = logs
            .iter()
            .max_by_key(|l| l.len())
            .cloned()
            .unwrap_or_default();
        for (replica, log) in c.replicas().iter().zip(&logs) {
            assert_eq!(
                &longest[..log.len()],
                log.as_slice(),
                "seed={seed}: replica {replica} diverged across a tuned crash/restart"
            );
        }
        assert!(
            !longest.is_empty(),
            "seed={seed}: nothing was ever decided, so this proved nothing"
        );
        let restarted = c.applied_log(victim);
        assert!(
            !restarted.is_empty(),
            "seed={seed}: the restarted replica applied nothing -- it never rejoined"
        );

        let history = history_from_records(&c.results());
        assert!(
            is_linearizable(&history),
            "seed={seed}: history was not linearizable across a tuned crash/restart"
        );
    }
}

// ---------------------------------------------------------------------
// Scenario 4: safety unchanged while the tuner explores/switches, plus
// determinism.
// ---------------------------------------------------------------------

#[test]
fn safety_and_linearizability_hold_while_auto_tuning_under_adversary_and_a_slow_replica() {
    for seed in 0..10u64 {
        let n = 4usize;
        let epoch_len = 3u64;
        let base_delay = 10 + seed % 15;
        let adversary = ContentObliviousAdversary::new(1, 6).with_drop_probability(0.15);
        let mut c = SmrCluster::new_with_tuning(
            seed,
            SchedulerKind::Oblivious(Box::new(adversary)),
            n,
            epoch_len,
            base_delay,
        );
        if seed % 2 == 0 {
            c.set_slow(NodeId((seed % n as u64) as u32), 4);
        }

        let mut ops = Vec::new();
        for i in 0..30u64 {
            let replica = NodeId((i % n as u64) as u32);
            let op = c.submit_put(replica, ClientId(1), i, (i % 5) as u32, i as i64);
            c.run_for(3_000);
            ops.push(op);
        }
        c.run_for(300_000);

        // Log safety (P5/P6/P7): every replica's applied log is a prefix of
        // the longest one.
        let logs: Vec<Vec<queso_smr::Command>> =
            c.replicas().iter().map(|&r| c.applied_log(r)).collect();
        let longest = logs
            .iter()
            .max_by_key(|l| l.len())
            .cloned()
            .unwrap_or_default();
        for (replica, log) in c.replicas().iter().zip(&logs) {
            assert_eq!(
                &longest[..log.len()],
                log.as_slice(),
                "seed={seed}: replica {replica} diverged from the longest observed log"
            );
        }

        // Linearizability (P8) via the existing checker.
        let history = history_from_records(&c.results());
        assert!(
            is_linearizable(&history),
            "seed={seed}: history was not linearizable: {history:#?}"
        );
    }
}

#[test]
fn tuning_preserves_determinism_given_the_same_seed() {
    let run = |seed: u64| {
        let mut c = SmrCluster::new_with_tuning(
            seed,
            SchedulerKind::Oblivious(Box::new(Fifo::new(1))),
            3,
            3,
            10,
        );
        c.set_slow(NodeId(1), 5);
        drive_puts(&mut c, &[NodeId(0)], 25, 1_000, 0);
        c.run_for(100_000);
        (
            c.trace().to_canonical_bytes(),
            c.tuning_leader_log().unwrap(),
            c.tuning_schedule().unwrap(),
        )
    };
    let a = run(2024);
    let b = run(2024);
    assert_eq!(
        a, b,
        "identical seeds must produce identical traces and tuning decisions"
    );
}
