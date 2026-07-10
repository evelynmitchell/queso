//! Crash-*recovery* restart safety (Stage 4b, P9/P12): a replica that
//! crashes and later restarts must recover its durable state (recorders'
//! ISR, log frontier, applied log, `kv`), drop its volatile state, and
//! rejoin as a learner -- catching up before it resumes participating --
//! without ever losing an acknowledged write or diverging from the rest of
//! the log. See `queso_smr::replica::Durable`'s docs for the durable/
//! volatile split these tests exercise, and `queso_smr::replica::SmrNode::
//! on_restart` for the recovery sequence.

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use queso_consensus::proposer::{MAX_RETRIES_PER_STEP, RETRY_DELAY_TICKS};
use queso_sim::ids::NodeId;
use queso_sim::scheduler::{ContentObliviousAdversary, Fifo, SchedulerKind};
use queso_sim::time::LogicalTime;
use queso_smr::{
    history_from_records, is_linearizable, ClientId, ClientSession, Command, HistoryOp, OpId,
    Outcome, SmrCluster,
};

/// P1/P5/P6/P7 divergence check, mirroring `log_safety.rs`'s
/// `assert_log_safety`: wherever two replicas' applied logs both have an
/// entry for the same slot, it must be identical, and each replica's
/// frontier must exactly match its applied log's length. Run *after*
/// restarting a previously-crashed replica, to confirm recovery never
/// introduces a divergence.
fn assert_no_divergence(cluster: &SmrCluster) {
    let replicas = cluster.replicas().to_vec();
    let logs: Vec<Vec<Command>> = replicas.iter().map(|&r| cluster.applied_log(r)).collect();

    for (r, log) in replicas.iter().zip(&logs) {
        assert_eq!(
            cluster.next_slot(*r) as usize,
            log.len(),
            "P7: replica {r}'s frontier must exactly match its applied-log length after restart"
        );
    }

    for i in 0..replicas.len() {
        for j in (i + 1)..replicas.len() {
            for (slot, (a, b)) in logs[i].iter().zip(&logs[j]).enumerate() {
                assert_eq!(
                    a, b,
                    "P1/P5: replicas {} and {} disagree at slot {slot} after a restart",
                    replicas[i], replicas[j]
                );
            }
        }
    }
}

/// P12 (restart safety) + P9 (no lost committed write): crash a replica
/// while it has its own attempt genuinely in flight (not yet decided,
/// nowhere near a full round trip), let the rest of the cluster keep
/// deciding writes without it, restart it, and confirm (a) the log never
/// diverged and (b) a read routed *specifically* to the just-restarted
/// replica observes every write that was acknowledged while it was down.
/// (b) is the strongest possible check here: it goes through the exact same
/// production `submit_get` path a real client would use, so it only passes
/// if durable state (recorders/log/kv) genuinely survived the crash *and*
/// catch-up genuinely closed the gap -- a replica silently serving a stale
/// local answer (the N3 anomaly P10 forbids) would fail it.
#[test]
fn restart_recovers_without_divergence_and_preserves_acknowledged_writes() {
    let adversary = ContentObliviousAdversary::new(1, 6).with_drop_probability(0.1);
    let mut c = SmrCluster::new(21, SchedulerKind::Oblivious(Box::new(adversary)), 5);
    let victim = NodeId(2);

    // Kick off an attempt on the victim, then crash it almost immediately --
    // its own `record` requests have barely had time to be sent, let alone
    // gather a quorum, so this is genuinely mid-decision, not "before it
    // started".
    let doomed = c.submit_put(victim, ClientId(9), 0, 1, -1);
    c.run_for(2);
    c.crash(victim);
    assert!(
        !c.is_complete(doomed),
        "test setup: the victim's own attempt must still be in flight when it crashes, or \
         this test isn't exercising a mid-decision crash"
    );

    // The rest of the cluster (a live majority: 4 of 5) keeps deciding
    // writes while the victim is down.
    let live: Vec<NodeId> = c.live().iter().copied().collect();
    assert_eq!(
        live.len(),
        4,
        "test setup: exactly one replica should be down"
    );
    let mut last_value = 0i64;
    for i in 0..8u64 {
        let replica = live[i as usize % live.len()];
        last_value = 1000 + i as i64;
        let op = c.submit_put(replica, ClientId(1), i, 42, last_value);
        c.run_for(30_000);
        assert!(
            c.is_complete(op),
            "write {i} must complete with a live majority, even with the victim down"
        );
    }

    // Restart the victim and let it catch up as a learner.
    c.restart(victim);
    c.run_for(300_000);

    assert_no_divergence(&c);

    let read = c.submit_get(victim, ClientId(2), 0, 42);
    c.run_for(300_000);
    assert!(
        c.is_complete(read),
        "a read through the restarted replica must eventually complete"
    );
    assert_eq!(
        c.result(read).unwrap().outcome,
        Some(Outcome::Get(Some(last_value))),
        "a restarted replica must observe every write that was acknowledged while it was down \
         -- P9 (no lost committed writes) and P10 (no stale read) together"
    );
}

/// Regression test for the "permanent zombie replica" liveness bug found in
/// review: a replica that restarts while it cannot yet reach a live
/// majority must **not** get permanently stuck even after a majority
/// becomes reachable again -- see `queso_smr::replica`'s module docs and
/// `queso_smr::replica::SmrNode::on_catch_up_watchdog` for the fix (a
/// catch-up quiescence watchdog, entirely at the SMR/replica layer; the
/// underlying `Proposer`'s per-step retry cap is untouched).
///
/// Reproduces the review's exact scenario: crash the whole cluster, restart
/// exactly one replica *alone* (so its catch-up probe can never reach a
/// live majority), let comfortably more than the `Proposer`'s own worst-case
/// retry-exhaustion budget (`MAX_RETRIES_PER_STEP * RETRY_DELAY_TICKS`
/// ticks) pass -- long enough that, without the watchdog, its catch-up
/// `Proposer` has certainly parked for good -- then bring the rest of the
/// cluster back. The lone replica must still (eventually) rejoin: its
/// frontier must catch up, and a `Get` submitted to it -- queued the whole
/// time it was stuck, exactly as a real client's would be -- must complete
/// with the correct, consistent value.
#[test]
fn a_lone_restarted_replica_rejoins_instead_of_zombie_parking_forever() {
    let mut c = SmrCluster::new(4242, SchedulerKind::Oblivious(Box::new(Fifo::new(1))), 5);
    let all: Vec<NodeId> = c.replicas().to_vec();
    let lone = NodeId(0);

    // Get one write durably decided and applied *by `lone` itself* first, so
    // there is something real for it to have already recovered as durable
    // state (its own applied `next_slot=1`) by the time it restarts alone --
    // exactly the review's "n0 stays stuck at next_slot=1 forever" shape.
    // (Only the replica whose own `Proposer` attempt actually decides a slot
    // advances its frontier immediately; other replicas only apply it once
    // their own proposer -- ordinary work or catch-up -- visits that slot,
    // which is irrelevant to this test.)
    let put = c.submit_put(lone, ClientId(1), 0, 7, 555);
    c.run_for(50_000);
    assert!(c.is_complete(put), "test setup: the write must complete");
    assert_eq!(
        c.next_slot(lone),
        1,
        "test setup: `lone` must have applied the one write before the crash"
    );

    // Crash the entire cluster, then restart only `lone` -- alone, it can
    // never gather a majority (the other four are still crashed, and
    // `Kernel::send` drops any message to/from a crashed node outright).
    for &r in &all {
        c.crash(r);
    }
    c.restart(lone);
    assert_eq!(
        c.live().len(),
        1,
        "test setup: exactly one replica should be live"
    );

    // A real client tries `lone` while it's stuck -- exactly like the bug
    // report: this must sit queued (not erroring, not completing) as long
    // as `lone` cannot make progress.
    let read = c.submit_get(lone, ClientId(2), 0, 7);

    // Let far more than the Proposer's own worst-case per-step retry budget
    // elapse (comfortably past `MAX_RETRIES_PER_STEP * RETRY_DELAY_TICKS`,
    // and past this crate's own watchdog interval too) -- long enough that
    // the lone replica's catch-up `Proposer` has certainly either parked
    // (pre-fix) or already been re-armed at least once (post-fix).
    let stall_budget = MAX_RETRIES_PER_STEP as u64 * RETRY_DELAY_TICKS;
    c.run_for(stall_budget * 4);
    assert_eq!(
        c.next_slot(lone),
        1,
        "test setup: with no reachable majority, the lone replica cannot legitimately learn \
         anything past its own recovered frontier yet -- if this fails, the scenario isn't \
         actually exercising a stalled catch-up"
    );
    assert!(
        !c.is_complete(read),
        "test setup: the queued Get cannot have completed yet -- no majority was ever reachable"
    );

    // Now a majority becomes reachable again.
    for &r in &all {
        if r != lone {
            c.restart(r);
        }
    }
    c.run_for(600_000);

    assert_no_divergence(&c);
    assert!(
        c.next_slot(lone) > 1,
        "the lone replica must have made progress past its stuck frontier (its own catch-up \
         probe, and then the queued Get, each consume one more log slot) rather than staying \
         parked at next_slot=1 forever -- got next_slot={}",
        c.next_slot(lone)
    );
    assert!(
        c.is_complete(read),
        "the Get queued on the lone replica while it was stalled must eventually complete once \
         a majority becomes reachable -- a permanently parked catch-up attempt (the zombie \
         liveness bug) would leave it queued forever"
    );
    assert_eq!(
        c.result(read).unwrap().outcome,
        Some(Outcome::Get(Some(555))),
        "the rejoined replica must observe the write that was already decided before the crash"
    );

    // No write was lost or corrupted along the way either.
    let confirm = c.submit_get(lone, ClientId(3), 0, 7);
    c.run_for(50_000);
    assert_eq!(
        c.result(confirm).unwrap().outcome,
        Some(Outcome::Get(Some(555)))
    );
}

/// Same zombie-replica scenario as
/// `a_lone_restarted_replica_rejoins_instead_of_zombie_parking_forever`, but
/// under a lossy, reordering adversary (rather than a plain FIFO scheduler)
/// once the rest of the cluster comes back -- so the watchdog's re-issued
/// catch-up attempts also have to survive real message loss on top of the
/// unreachability that caused the stall in the first place, across several
/// seeds, for extra confidence against flakiness/nondeterminism in the
/// watchdog's own re-arm timing.
#[test]
fn a_lone_restarted_replica_rejoins_under_a_lossy_adversary_too() {
    for seed in 0..5u64 {
        let adversary = ContentObliviousAdversary::new(1, 6).with_drop_probability(0.15);
        let mut c = SmrCluster::new(
            9000 + seed,
            SchedulerKind::Oblivious(Box::new(adversary)),
            5,
        );
        let all: Vec<NodeId> = c.replicas().to_vec();
        let lone = NodeId(0);

        let put = c.submit_put(lone, ClientId(1), 0, 3, 42);
        c.run_for(200_000);
        assert!(
            c.is_complete(put),
            "seed {seed}: test setup: the write must complete"
        );
        assert_eq!(c.next_slot(lone), 1, "seed {seed}: test setup");

        for &r in &all {
            c.crash(r);
        }
        c.restart(lone);

        let read = c.submit_get(lone, ClientId(2), 0, 3);
        let stall_budget = MAX_RETRIES_PER_STEP as u64 * RETRY_DELAY_TICKS;
        c.run_for(stall_budget * 4);
        assert!(
            !c.is_complete(read),
            "seed {seed}: test setup: must still be stuck with no reachable majority"
        );

        for &r in &all {
            if r != lone {
                c.restart(r);
            }
        }
        c.run_for(1_000_000);

        assert_no_divergence(&c);
        assert!(
            c.is_complete(read),
            "seed {seed}: a lone restarted replica must rejoin (and complete a long-queued Get) \
             even once the rest of the cluster is reachable again, under a lossy adversary"
        );
        assert_eq!(
            c.result(read).unwrap().outcome,
            Some(Outcome::Get(Some(42))),
            "seed {seed}: must observe the write decided before the crash"
        );
    }
}

/// Write-before-reply (P12): once a recorder has answered a `record` RPC --
/// synchronously mutating its durable ISR state *before* the reply is even
/// constructed, see `queso_smr::replica::SmrNode::on_message`'s `Request`
/// arm -- a subsequent crash + restart of that replica must never roll that
/// state back. Checked directly against the recorder's own ISR summary
/// (`S, F_c, A_p`), the exact state `docs/02-properties.md`'s P12 note
/// calls out by name, not just the higher-level `Kv`/log view.
#[test]
fn a_recorders_isr_state_survives_a_crash_and_restart_of_that_replica() {
    let mut c = SmrCluster::new(11, SchedulerKind::Oblivious(Box::new(Fifo::new(2))), 3);
    let put = c.submit_put(NodeId(0), ClientId(1), 0, 5, 500);
    c.run_for(50_000);
    assert!(c.is_complete(put), "the write must complete");
    let slot = c.result(put).unwrap().decided_slot.unwrap();

    let victim = NodeId(1);
    let before = c
        .recorder_summary(victim, slot)
        .expect("the recorder must have handled at least one record RPC for this slot");
    assert!(
        before.first.is_some(),
        "test setup: the recorder must actually have recorded a proposal, or this test \
         proves nothing"
    );

    c.crash(victim);
    c.run_for(1_000); // real logical time passes while the replica is down
    c.restart(victim);
    c.run_for(50_000); // let restart catch-up (a *different*, later slot) finish

    let after = c
        .recorder_summary(victim, slot)
        .expect("durable recorder state must survive a crash + restart");
    assert_eq!(
        before, after,
        "write-before-reply (P12): a recorder's ISR state, once it has answered a record RPC \
         for a slot, must be exactly unchanged by a subsequent crash + restart of that replica"
    );
}

/// Run a randomized concurrent put/get workload, same shape as
/// `linearizability.rs`'s `randomized_history`, but additionally toggling
/// replicas' liveness (crash *and* restart, not just crash) at random
/// points throughout the run -- while always leaving a live majority so the
/// workload can actually make progress (liveness under an unrecoverable
/// minority is P11/O4's concern, not this one).
fn randomized_history_with_restarts(seed: u64, n: usize, ops: usize) -> Vec<queso_smr::HistoryOp> {
    let adversary = ContentObliviousAdversary::new(1, 5).with_drop_probability(0.1);
    let mut cluster = SmrCluster::new(seed, SchedulerKind::Oblivious(Box::new(adversary)), n);

    let mut rng = StdRng::seed_from_u64(seed.wrapping_mul(131).wrapping_add(7));
    let mut sessions: Vec<ClientSession> =
        (0..4).map(|i| ClientSession::new(ClientId(i))).collect();
    let replicas = cluster.replicas().to_vec();
    let majority = replicas.len() / 2 + 1;

    for _ in 0..ops {
        let live_count = cluster.live().len();
        if rng.gen_bool(0.12) && live_count > majority {
            let live: Vec<NodeId> = cluster.live().iter().copied().collect();
            let victim = live[rng.gen_range(0..live.len())];
            cluster.crash(victim);
        } else if rng.gen_bool(0.25) && live_count < replicas.len() {
            let crashed: Vec<NodeId> = replicas
                .iter()
                .filter(|r| !cluster.live().contains(r))
                .copied()
                .collect();
            let target = crashed[rng.gen_range(0..crashed.len())];
            cluster.restart(target);
        }

        let live: Vec<NodeId> = cluster.live().iter().copied().collect();
        if live.is_empty() {
            cluster.run_for(rng.gen_range(1..30));
            continue;
        }
        let session_idx = rng.gen_range(0..sessions.len());
        let session = &mut sessions[session_idx];
        let replica = live[rng.gen_range(0..live.len())];
        let key = rng.gen_range(0..3);
        let seq = session.next_seq();
        let client = session.id();
        if rng.gen_bool(0.5) {
            let value = rng.gen_range(0..1000);
            cluster.submit_put(replica, client, seq, key, value);
        } else {
            cluster.submit_get(replica, client, seq, key);
        }
        cluster.run_for(rng.gen_range(1..30));
    }

    // Bring everything back up and let the cluster settle.
    for &r in &replicas {
        if !cluster.live().contains(&r) {
            cluster.restart(r);
        }
    }
    cluster.run_for(600_000);

    // Reconcile any op that never completed. `on_restart` deliberately
    // drops a replica's *volatile* queued work (see
    // `queso_smr::replica::SmrNode::on_restart`) -- a client whose op was
    // queued on a replica that then crashed never received an ack from
    // that attempt, exactly the "uncertain outcome" case A6/P8a dedup
    // exists for. Crucially, that command's *effect* can still be decided
    // and durably applied (e.g. its `record` requests already reached a
    // majority of recorders before the crash, and some other still-live
    // proposer independently discovers and decides that same value) --
    // this is not special to restarts, it is inherent to any crash-stop
    // system, and it is exactly why a real client retries an uncertain
    // request instead of silently treating it as "definitely did not
    // happen". `history_from_records`'s documented contract is to drop any
    // op with no response -- correct in general, but *only* sound if
    // nothing else in the kept history could have observed its effect.
    // Naively resubmitting and taking the retry's own (much later) real
    // time window would violate that soundness the other way (a later
    // observer could then be *forced* after a write that, in reality,
    // could already have been visible far earlier). The standard treatment
    // for an "indeterminate" op (Jepsen calls this `:info`) is to widen its
    // window to cover every point in time it could possibly have taken
    // effect: here, `[original_invoked_at, retry_completed_at]` -- it
    // cannot have happened before the client ever asked, and by the time a
    // dedup-safe retry (P8a) has itself completed, it is definitely
    // reflected in `kv`. That interval is a strict subset of the standard
    // `[invoke, +infinity)` treatment (sound, and tighter), so it can only
    // make the checker's job *harder* to satisfy than the fully permissive
    // textbook version, never easier.
    //
    // # This widening is sound for `Put`s only -- NOT for `Get`s
    //
    // The "widen to `[original_invoked_at, retry_completed_at]`" argument
    // above leans on write-idempotency (P8a): a `Put`'s *effect* is exactly
    // the same no matter which of its (possibly many) attempts is "the one
    // that counted", so crediting it to the original invocation time is
    // always safe -- the value it wrote is fixed by its `(client, seq)`,
    // not by when any particular attempt happened to land.
    //
    // A `Get` has no such fixed effect: its outcome is *whatever value was
    // current at its true linearization point*, and a stranded `Get`'s
    // retry can genuinely observe a *different* value than the original
    // invocation would have (other writes may have landed in between).
    // Reporting the retry's observed value under the *original*, much
    // earlier, invocation time would therefore let the checker validate a
    // reordering that never actually happened -- e.g. exactly masking the
    // P10 stale-read anomaly this module's checker exists to catch
    // (confirmed: `is_linearizable([Put(K=1), Put(K=2), Get(observed=1)])`
    // correctly returns `false` against the retry's own true narrow
    // interval, but `true` -- silently masking a real anomaly -- if that
    // same observation were widened back to the original invocation time).
    //
    // Sound treatment: a stranded `Get` is dropped from the history exactly
    // like any other operation with no response (the standard treatment
    // `crate::linearizability::history_from_records`'s docs already call
    // for), and its retry -- a perfectly ordinary, freshly-submitted `Get`
    // with its own genuine `(invoked_at, completed_at)` window (`Get`s are
    // never deduplicated by `Kv::apply`, so resubmitting one is exactly as
    // sound as any other fresh submission) -- is left to stand on its own.
    // Concretely, that needs no special-casing at all: simply *not* adding
    // it to `retry_ids` (so it is never stripped back out of `results`) and
    // *not* folding it into `reconciled` under the stale original id is
    // sufficient -- `history_from_records` then picks up the retry's own
    // record with its own true, narrow, already-sound interval.
    let stuck: Vec<(OpId, LogicalTime, Command)> = cluster
        .results()
        .into_iter()
        .filter(|(_, r)| r.completed_at.is_none())
        .map(|(id, r)| (id, r.invoked_at, r.command))
        .collect();

    let mut retry_ids: std::collections::BTreeSet<OpId> = std::collections::BTreeSet::new();
    let mut reconciled: Vec<HistoryOp> = Vec::new();
    let mut pending = stuck;
    for _ in 0..4 {
        if pending.is_empty() {
            break;
        }
        let live: Vec<NodeId> = cluster.live().iter().copied().collect();
        if live.is_empty() {
            cluster.run_for(50_000);
            continue;
        }
        let mut still_pending = Vec::new();
        for (i, (orig_id, orig_invoked_at, command)) in pending.into_iter().enumerate() {
            let replica = live[i % live.len()];
            let retry = cluster.submit(replica, command.clone());
            cluster.run_for(100_000);
            match cluster.result(retry) {
                Some(r) if r.completed_at.is_some() => match &command {
                    Command::Put { .. } => {
                        // Write-idempotent (P8a): safe to widen and report
                        // under the original op id -- see the comment above.
                        retry_ids.insert(retry);
                        reconciled.push(HistoryOp {
                            op_id: orig_id,
                            command: r.command,
                            invoked_at: orig_invoked_at,
                            completed_at: r.completed_at.unwrap(),
                            outcome: r.outcome.unwrap(),
                        });
                    }
                    Command::Get { .. } => {
                        // Unsound to widen -- see the comment above. Leave
                        // the retry's own record untouched in `results`
                        // (don't add it to `retry_ids`, don't fold it into
                        // `reconciled`); `history_from_records` will pick it
                        // up on its own, already-sound, narrow interval. The
                        // original, stranded `Get` stays `completed_at:
                        // None` and is dropped by `history_from_records`,
                        // exactly the standard "no response" treatment.
                    }
                },
                _ => still_pending.push((orig_id, orig_invoked_at, command)),
            }
        }
        pending = still_pending;
    }

    let mut results = cluster.results();
    for id in &retry_ids {
        results.remove(id);
    }
    let mut history = history_from_records(&results);
    history.extend(reconciled);
    history
}

/// The Stage 4a linearizability checker must still accept histories that
/// include crash *and* restart of replicas mid-run, not merely crash-stop
/// (which `linearizability.rs` already covers).
#[test]
fn linearizability_holds_across_crash_and_restart_n5() {
    for seed in 0..6u64 {
        let history = randomized_history_with_restarts(seed + 2000, 5, 16);
        assert!(
            !history.is_empty(),
            "seed {seed}: workload produced no completed operations at all"
        );
        assert!(
            is_linearizable(&history),
            "seed {seed}: history was not linearizable across crash/restart: {history:#?}"
        );
    }
}

/// Regression test for the reconciliation-soundness gap found in review:
/// `randomized_history_with_restarts`' `[original_invoked_at,
/// retry_completed_at]` interval-widening is sound for a stranded `Put`
/// (write-idempotent, P8a) but was, before the fix, applied identically to
/// a stranded `Get` -- which is unsound, because a `Get`'s retry can observe
/// a value the original invocation never could have. This test pins down
/// the exact scenario the review confirmed: `is_linearizable` must reject a
/// genuine stale read when given the operations' true, narrow real-time
/// intervals, and *would* wrongly accept the identical anomaly if that
/// stranded `Get`'s interval were widened back to an earlier invocation
/// time the way a `Put`'s safely can be -- demonstrating both that the
/// checker itself has teeth here, and exactly why `Get`s must be exempted
/// from the widening `randomized_history_with_restarts` performs.
#[test]
fn stale_read_reconciliation_must_not_widen_a_stranded_gets_interval() {
    fn put(op: u64, key: u32, value: i64, inv: u64, comp: u64) -> HistoryOp {
        HistoryOp {
            op_id: OpId(op),
            command: Command::Put {
                client: ClientId(1),
                seq: op,
                key,
                value,
            },
            invoked_at: LogicalTime(inv),
            completed_at: LogicalTime(comp),
            outcome: Outcome::Put,
        }
    }
    fn get(op: u64, key: u32, observed: Option<i64>, inv: u64, comp: u64) -> HistoryOp {
        HistoryOp {
            op_id: OpId(op),
            command: Command::Get {
                client: ClientId(2),
                seq: op,
                key,
            },
            invoked_at: LogicalTime(inv),
            completed_at: LogicalTime(comp),
            outcome: Outcome::Get(observed),
        }
    }

    // Put(K=1) then Put(K=2), both fully complete in real time before the
    // Get is even invoked -- so any legal linearization must place both
    // writes before the read, and the read must observe 2.
    let put1 = put(0, 10, 1, 0, 5);
    let put2 = put(1, 10, 2, 6, 10);

    // The Get was actually invoked at t=20 (after both writes completed)
    // but got stranded (its replica crashed mid-attempt); its dedup-safe
    // retry was resubmitted and completed later, observing the *stale*
    // value 1 -- a genuine P10 violation no legal linearization can excuse,
    // since real time already forced both puts before it.
    let true_narrow_get = get(2, 10, Some(1), 20, 25);
    assert!(
        !is_linearizable(&[put1.clone(), put2.clone(), true_narrow_get]),
        "a Get strictly after two completed Puts must see the latest write -- this history is \
         the textbook stale-read anomaly (P10/N3) and must be rejected"
    );

    // The bug this test guards against: if the stranded Get's interval were
    // widened the same way a stranded Put's safely can be -- back to
    // *before* it was ever really invoked (here, to t=1, inside Put(K=1)'s
    // own interval and before Put(K=2) even starts) -- the checker can no
    // longer tell this apart from a legitimate concurrent read that raced
    // Put(K=1) and is entitled to see either 0 or 1. That would silently
    // *mask* the exact same P10 anomaly just confirmed above.
    let wrongly_widened_get = get(2, 10, Some(1), 1, 25);
    assert!(
        is_linearizable(&[put1, put2, wrongly_widened_get]),
        "test setup: widening the Get's interval back over Put(K=1) must make the checker \
         (wrongly) accept the same observation it just correctly rejected -- if this assertion \
         ever fails, the demonstrated masking bug no longer reproduces and this regression test \
         should be revisited"
    );
}

/// Determinism (D9) must be preserved even when crash/restart faults are
/// part of the scripted scenario: replaying the same seed and the same
/// fault schedule must produce byte-for-byte identical traces.
#[test]
fn restart_scenarios_remain_deterministic_given_the_same_seed() {
    fn run(seed: u64) -> Vec<u8> {
        let mut c = SmrCluster::new(
            seed,
            SchedulerKind::Oblivious(Box::new(
                ContentObliviousAdversary::new(1, 5).with_drop_probability(0.1),
            )),
            5,
        );
        let victim = NodeId(3);
        c.submit_put(NodeId(0), ClientId(1), 0, 1, 111);
        c.run_for(50);
        c.crash(victim);
        c.submit_put(NodeId(1), ClientId(2), 0, 1, 222);
        c.run_for(20_000);
        c.restart(victim);
        c.submit_get(victim, ClientId(3), 0, 1);
        c.run_for(200_000);
        c.trace().to_canonical_bytes()
    }
    assert_eq!(run(777), run(777));
}
