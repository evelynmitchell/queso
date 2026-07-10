//! Idempotency (P8a) and its interaction with linearizability (P8): a
//! client's `(client, seq)`-tagged command, submitted (or replayed) more
//! than once -- whether as a retry to a different replica, or reordered
//! relative to a later command from the same client -- must have
//! exactly-once effect, and the resulting operation history must still be
//! linearizable.

use queso_sim::ids::NodeId;
use queso_sim::scheduler::{Fifo, SchedulerKind};
use queso_smr::{history_from_records, is_linearizable, ClientId, Command, SmrCluster};

fn put(client: u32, seq: u64, key: u32, value: i64) -> Command {
    Command::Put {
        client: ClientId(client),
        seq,
        key,
        value,
    }
}

fn cluster(seed: u64) -> SmrCluster {
    SmrCluster::new(seed, SchedulerKind::Oblivious(Box::new(Fifo::new(1))), 3)
}

/// A duplicate submission of the exact same `(client, seq)` command --
/// simulating a client retrying to a *different* replica after an uncertain
/// ack -- must not double-apply, and must not undo a later write from the
/// same client that has since landed.
#[test]
fn a_late_duplicate_does_not_clobber_a_later_write() {
    let mut c = cluster(1);

    let first = c.submit(NodeId(0), put(1, 1, 10, 100));
    c.run_for(50_000);
    assert!(c.is_complete(first));

    let second = c.submit(NodeId(1), put(1, 2, 10, 200));
    c.run_for(50_000);
    assert!(c.is_complete(second));

    // A stale retry of the *first* command arrives late, submitted to yet
    // another replica (as if the client, unsure whether its first attempt
    // landed, retried it -- after having already moved on and issued its
    // second command).
    let duplicate = c.submit(NodeId(2), put(1, 1, 10, 100));
    c.run_for(50_000);
    assert!(c.is_complete(duplicate), "the duplicate submission still completes (P8a: applying it again is harmless, not an error)");

    // Observe the final value the only way that's actually safe (P10): a
    // fresh linearizable read through the log, not a raw peek at a
    // possibly-lagging replica's local `Kv` (a replica that only ever
    // caught up as far as the first write's slot would legitimately still
    // show the stale value locally -- that's P5's "may lag" allowance, not
    // a bug; see `linearizability.rs`'s stale-local-read control for
    // exactly this trap).
    let read = c.submit(
        NodeId(0),
        Command::Get {
            client: ClientId(9),
            seq: 0,
            key: 10,
        },
    );
    c.run_for(50_000);
    assert!(c.is_complete(read));
    assert_eq!(
        c.result(read).unwrap().outcome,
        Some(queso_smr::Outcome::Get(Some(200))),
        "the later write must win, never a regression back to the stale duplicate"
    );

    let history = history_from_records(&c.results());
    assert!(
        is_linearizable(&history),
        "a deduplicated retry must not create a linearizability violation: {history:#?}"
    );
}

/// Commands can also arrive "reordered": a client's higher-`seq` command
/// gets decided in the log before its lower-`seq` predecessor (e.g. the
/// predecessor was submitted to a slow/partitioned replica). The dedup
/// table is monotonic (`seq <= last_seq` counts as stale), so once the
/// higher seq has applied, the lower one is treated as superseded rather
/// than clobbering it -- exactly the same rule, applied to real reordering
/// instead of a literal duplicate.
#[test]
fn a_reordered_lower_seq_does_not_undo_a_higher_seq_already_applied() {
    let mut c = cluster(2);

    let newer = c.submit(NodeId(0), put(1, 5, 20, 999));
    c.run_for(50_000);
    assert!(c.is_complete(newer));

    let older = c.submit(NodeId(1), put(1, 3, 20, 111));
    c.run_for(50_000);
    assert!(
        c.is_complete(older),
        "the late-arriving older command still completes"
    );

    let read = c.submit(
        NodeId(2),
        Command::Get {
            client: ClientId(9),
            seq: 0,
            key: 20,
        },
    );
    c.run_for(50_000);
    assert!(c.is_complete(read));
    assert_eq!(
        c.result(read).unwrap().outcome,
        Some(queso_smr::Outcome::Get(Some(999))),
        "the higher seq must win; the reordered lower seq must not apply"
    );

    let history = history_from_records(&c.results());
    assert!(is_linearizable(&history), "{history:#?}");
}

/// Duplicate `Get`s (a client retrying a read whose ack it never saw) are
/// trivially safe -- reads never mutate -- but should still resolve to the
/// same observed value and not disturb linearizability.
#[test]
fn duplicate_reads_agree_and_are_linearizable() {
    let mut c = cluster(3);
    let put_op = c.submit(NodeId(0), put(1, 0, 1, 55));
    c.run_for(50_000);
    assert!(c.is_complete(put_op));

    let get_a = c.submit(
        NodeId(1),
        Command::Get {
            client: ClientId(2),
            seq: 0,
            key: 1,
        },
    );
    c.run_for(20_000);
    let get_b = c.submit(
        NodeId(2),
        Command::Get {
            client: ClientId(2),
            seq: 0,
            key: 1,
        },
    );
    c.run_for(50_000);

    assert!(c.is_complete(get_a) && c.is_complete(get_b));
    assert_eq!(
        c.result(get_a).unwrap().outcome,
        c.result(get_b).unwrap().outcome
    );

    let history = history_from_records(&c.results());
    assert!(is_linearizable(&history), "{history:#?}");
}
