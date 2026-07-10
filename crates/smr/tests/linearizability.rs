//! End-to-end linearizability (P8): randomized concurrent `put`/`get`
//! workloads, from multiple clients through multiple replicas, checked
//! against [`queso_smr::is_linearizable`] using the harness's own logical
//! invocation/response times. Also the positive control: a deliberately
//! unsafe "stale local read" (bypassing the log -- exactly what P10
//! forbids) must be *rejected*, proving the checker has teeth rather than
//! trivially accepting everything.

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use queso_sim::ids::NodeId;
use queso_sim::scheduler::{ContentObliviousAdversary, Fifo, SchedulerKind};
use queso_smr::{
    history_from_records, is_linearizable, ClientId, ClientSession, Command, HistoryOp, OpId,
    Outcome, SmrCluster,
};

/// Run a randomized workload of several client sessions, each issuing a
/// mix of `put`/`get` against a small key domain to random live replicas,
/// interleaved with running the kernel forward so operations genuinely
/// overlap (rather than all being invoked at time 0).
fn randomized_history(seed: u64, n: usize, ops: usize) -> Vec<HistoryOp> {
    let adversary = ContentObliviousAdversary::new(1, 5).with_drop_probability(0.1);
    let mut cluster = SmrCluster::new(seed, SchedulerKind::Oblivious(Box::new(adversary)), n);

    let mut rng = StdRng::seed_from_u64(seed.wrapping_mul(97).wrapping_add(13));
    let mut sessions: Vec<ClientSession> =
        (0..4).map(|i| ClientSession::new(ClientId(i))).collect();
    let replicas = cluster.replicas().to_vec();

    for _ in 0..ops {
        let session_idx = rng.gen_range(0..sessions.len());
        let session = &mut sessions[session_idx];
        let replica = replicas[rng.gen_range(0..replicas.len())];
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
    cluster.run_for(400_000);

    history_from_records(&cluster.results())
}

#[test]
fn randomized_concurrent_workload_is_linearizable_n3() {
    for seed in 0..8u64 {
        let history = randomized_history(seed, 3, 20);
        assert!(
            !history.is_empty(),
            "seed {seed}: workload produced no completed operations at all"
        );
        assert!(
            is_linearizable(&history),
            "seed {seed}: history was not linearizable: {history:#?}"
        );
    }
}

#[test]
fn randomized_concurrent_workload_is_linearizable_n5() {
    for seed in 0..5u64 {
        let history = randomized_history(seed + 1000, 5, 16);
        assert!(
            is_linearizable(&history),
            "seed {seed}: history was not linearizable: {history:#?}"
        );
    }
}

/// The positive control: prove `is_linearizable` has teeth by feeding it a
/// history that includes a deliberately unsafe read. `queso_smr`'s real
/// `submit_get` always proposes through the log (P10); here we instead read
/// a lagging replica's local `Kv` snapshot directly -- bypassing consensus
/// entirely -- to construct the textbook stale-read anomaly (N3) and
/// confirm the checker rejects it.
#[test]
fn a_stale_local_read_is_rejected_by_the_checker() {
    let mut cluster = SmrCluster::new(555, SchedulerKind::Oblivious(Box::new(Fifo::new(1))), 3);

    let put_op = cluster.submit_put(NodeId(0), ClientId(1), 0, 7, 42);
    cluster.run_for(50_000);
    assert!(cluster.is_complete(put_op), "the write must complete");
    let put_record = cluster.result(put_op).unwrap();

    // Replica 1 never touched that write's slot (only replica 0 proposed
    // anything), so its local state has not caught up -- the read below is
    // genuinely stale, not a false alarm.
    assert_eq!(
        cluster.next_slot(NodeId(1)),
        0,
        "replica 1 must not have applied the write's slot for this control to be meaningful"
    );

    let stale_invoked_at = cluster.now();
    let stale_value = cluster.kv_snapshot(NodeId(1)).get(&7).copied();
    let stale_completed_at = cluster.now();
    assert_eq!(
        stale_value, None,
        "the stale local read must observe nothing"
    );

    // The write's real-time response strictly precedes the stale read's
    // real-time invocation, so any legal linearization must place the
    // write first -- and a `Get` for the same key placed after it can only
    // legally observe `Some(42)`. This must be a strict `<`, not `<=`: a
    // tie here would mean this "positive control" is only incidentally
    // demonstrating a rejection (the checker's real-time relation is
    // strict, so a tied `stale_invoked_at` would make the write and the
    // stale read look concurrent instead of ordered, and the test could
    // pass without ever exercising the intended stale-read anomaly).
    assert!(
        put_record.completed_at.unwrap() < stale_invoked_at,
        "test setup: the write's completion must strictly precede the stale \
         read's invocation, or this isn't testing the anomaly it claims to"
    );

    let mut history = history_from_records(&cluster.results());
    history.push(HistoryOp {
        op_id: OpId(u64::MAX),
        command: Command::Get {
            client: ClientId(2),
            seq: 0,
            key: 7,
        },
        invoked_at: stale_invoked_at,
        completed_at: stale_completed_at,
        outcome: Outcome::Get(stale_value),
    });

    assert!(
        !is_linearizable(&history),
        "a stale local read bypassing the log must be rejected -- the checker has no teeth otherwise"
    );
}

/// Regression test for a real soundness hole: `SmrCluster::submit` used to
/// stamp `invoked_at` with plain `kernel.now()`, which does not advance
/// just because `submit` is called outside any dispatched event. A
/// causally-ordered driver sequence -- `submit(a); run_until(a) completes;
/// submit(b)` -- could therefore land `b.invoked_at` exactly on
/// `a.completed_at`: a tie, which the checker's strict `<` real-time
/// relation treats as *concurrency*, not ordering, potentially accepting a
/// history where `b` reports a value from before `a`'s effect even though
/// `a` had already finished when `b` was invoked.
///
/// This drives the exact scenario through the real [`SmrCluster`] driver
/// (not a hand-built [`HistoryOp`]): step the kernel forward one tick at a
/// time and submit the read the instant the write completes, so the kernel
/// clock is parked precisely on the write's `completed_at` -- the tie
/// condition -- when the read is submitted.
#[test]
fn a_causally_ordered_submission_never_ties_a_prior_completion() {
    let mut cluster = SmrCluster::new(1, SchedulerKind::Oblivious(Box::new(Fifo::new(1))), 3);

    let put_op = cluster.submit_put(NodeId(0), ClientId(1), 0, 10, 100);
    for _ in 0..10_000 {
        if cluster.is_complete(put_op) {
            break;
        }
        cluster.run_for(1);
    }
    let put_record = cluster.result(put_op).expect("submitted");
    let put_completed_at = put_record.completed_at.expect("the write must complete");
    assert_eq!(
        cluster.now(),
        put_completed_at,
        "test setup: the kernel must be parked exactly on the put's \
         completion tick for this to exercise the tie condition"
    );

    // Submitted immediately afterward, through the real driver path, with
    // no intervening `run_for` -- exactly the untimed-but-causally-later
    // sequence the bug report describes.
    let get_op = cluster.submit_get(NodeId(1), ClientId(2), 0, 10);
    let get_invoked_at = cluster.result(get_op).expect("submitted").invoked_at;
    assert!(
        get_invoked_at > put_completed_at,
        "a submission issued after a prior completion must get a strictly \
         later invoked_at, never a tie: put completed at {put_completed_at:?}, \
         get invoked at {get_invoked_at:?}"
    );

    // Now show why that strictness matters. The real `get_op` always
    // observes the correct value (`submit_get` always proposes honestly
    // through the log -- P10), so reuse its genuine, tie-free timestamps
    // with a *forged* stale outcome to reconstruct the exact anomaly a tie
    // would have let the checker wrongly accept.
    cluster.run_for(50_000);
    let get_record = cluster.result(get_op).expect("submitted");
    assert_eq!(
        get_record.outcome,
        Some(Outcome::Get(Some(100))),
        "the real read, honestly proposed through the log, must see the write"
    );

    let mut history = history_from_records(&cluster.results());
    let forged = history
        .iter_mut()
        .find(|op| op.op_id == get_op)
        .expect("the get is in the history");
    assert_eq!(
        forged.invoked_at, get_invoked_at,
        "reusing the real timestamp"
    );
    forged.outcome = Outcome::Get(None); // forged: claims it missed the write

    assert!(
        !is_linearizable(&history),
        "a Get invoked strictly after a completed Put, forged to claim it \
         missed the write, must be rejected -- this is exactly the anomaly \
         the invoked_at tie used to let through"
    );
}

/// The flip side of the tie fix: two submissions issued *before* either has
/// completed are genuinely concurrent, and must remain so. The completion
/// floor in `SmrCluster::submit` must only ever push a *later* submission's
/// `invoked_at` past a completion that already happened -- it must never
/// force an order between ops that were, in real driver time, both in
/// flight at once.
#[test]
fn genuinely_concurrent_submissions_stay_untied_and_the_history_is_still_accepted() {
    let mut cluster = SmrCluster::new(3, SchedulerKind::Oblivious(Box::new(Fifo::new(1))), 3);

    let put_a = cluster.submit_put(NodeId(0), ClientId(1), 0, 1, 111);
    let put_b = cluster.submit_put(NodeId(1), ClientId(2), 0, 1, 222);
    let a_invoked = cluster.result(put_a).unwrap().invoked_at;
    let b_invoked = cluster.result(put_b).unwrap().invoked_at;
    assert_eq!(
        a_invoked, b_invoked,
        "both were submitted before anything had completed -- no completion \
         floor should have applied to either"
    );

    cluster.run_for(200_000);
    assert!(cluster.is_complete(put_a) && cluster.is_complete(put_b));

    let get = cluster.submit_get(NodeId(2), ClientId(3), 0, 1);
    cluster.run_for(200_000);
    let observed = cluster.result(get).unwrap().outcome;
    // Whichever write the log actually decided first, the read must see
    // it -- and the checker must accept that as one of the two legal
    // linearizations of the two concurrent writes.
    assert!(
        observed == Some(Outcome::Get(Some(111))) || observed == Some(Outcome::Get(Some(222))),
        "unexpected observed value: {observed:?}"
    );

    let history = history_from_records(&cluster.results());
    assert!(
        is_linearizable(&history),
        "two genuinely concurrent writes followed by a read of whichever \
         one the log actually decided first must remain linearizable -- \
         the invoked_at fix must not over-constrain concurrent submissions"
    );
}
