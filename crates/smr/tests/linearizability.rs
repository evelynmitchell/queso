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
    // legally observe `Some(42)`.
    assert!(put_record.completed_at.unwrap() <= stale_invoked_at);

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
