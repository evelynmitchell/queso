//! Idempotency (P8a) and its interaction with linearizability (P8): a
//! client's `(client, seq)`-tagged command, submitted (or replayed) more
//! than once -- whether as a retry to a different replica, or reordered
//! relative to a later command from the same client -- must have
//! exactly-once effect, and the resulting operation history must still be
//! linearizable.
//!
//! Detection power for P10 is measured in the doc comment on
//! `crates/smr/src/cluster.rs`'s
//! `read_after_write_on_a_different_replica_still_sees_it` (issue #113):
//! the read-from-local-state mutation (P10-A) fails all 4 tests here.
//! (#113 measured 3/3; #125 added a fourth test and re-ran P10-A against
//! the file as it now stands rather than carrying the old count forward.
//! The two tests that gained a slot guard fail P10-A *at that guard*: the
//! guard's warm-up read is itself a `Get`, so a mutation that stops `Get`s
//! going through the log stops the warm-up working.)
//!
//! # Detection power for P8a (#125)
//!
//! Two mutations at `Kv::apply`'s dedup test, whole-file runs, 8 each:
//!
//! | Mutation | Effect | Kills here |
//! |---|---|---|
//! | **A1** `is_duplicate = false` | dedup off entirely | 3/4, 8/8 runs |
//! | **A2** `s >= *seq` → `s > *seq` | exact re-retry exempted | 1/4, 8/8 runs |
//!
//! Each fires on the value assertion it was written for, not on a
//! neighbouring one. `duplicate_reads_agree_and_are_linearizable` survives
//! both, correctly: `Get`s are never entered in the dedup table.
//!
//! **What measuring changed.** Before #125,
//! `a_late_duplicate_does_not_clobber_a_later_write` had *zero* power
//! against A1: with dedup disabled its observable behavior was
//! byte-identical to the unmutated build -- same completions, same decided
//! slots, same per-replica `kv_snapshot`s, same final read. It went red
//! under A1 anyway, which is what makes this worth writing down: the
//! redness came from the linearizability assertion, and that assertion
//! fires because `Kv` is *also* the checker's reference spec (see
//! `crate::linearizability`'s module docs). A1 broke the spec, the spec
//! then disagreed with a system that had behaved correctly, and the file
//! turned red for a reason unrelated to the property. A count taken from
//! "the file went red" would have recorded power this test did not have.
//! The fix is the warm-up read documented below.
//!
//! A2 was caught by **nothing** at this level and by exactly one test in
//! the whole 440-test tree (`kv::tests::duplicate_seq_is_deduplicated`).
//! `an_exact_retry_of_the_latest_command_does_not_reapply` was added to
//! close that, and does: A2 kills it 8/8.

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
///
/// Falsifier, run: with `Kv::apply`'s dedup disabled (`is_duplicate =
/// false`), this fails 8/8 on the "later write must win" assertion. That
/// count is only true of the post-#125 form: before the warm-up read
/// below, the same mutation left this test's observable behavior
/// unchanged and it failed -- when it failed -- on the linearizability
/// assertion instead, for a reason having nothing to do with P8a. The
/// narrower `s > *seq` mutation (A2) does *not* fail this test, correctly:
/// client 1's later `seq 2` write means the retry is compared as `2 > 1`,
/// which the weakened test still deduplicates. `an_exact_retry_of_the_
/// latest_command_does_not_reapply` is what covers that case.
#[test]
fn a_late_duplicate_does_not_clobber_a_later_write() {
    let mut c = cluster(1);

    let first = c.submit(NodeId(0), put(1, 1, 10, 100));
    c.run_for(50_000);
    assert!(c.is_complete(first));

    let second = c.submit(NodeId(1), put(1, 2, 10, 200));
    c.run_for(50_000);
    assert!(c.is_complete(second));

    // Bring the retry's target replica up to date *before* the retry
    // arrives. This is not scene-setting -- it is what gives the rest of
    // this test any detection power at all, and it was added after a
    // measurement showed the test had none (#125).
    //
    // `NodeId(2)` has so far participated in nobody's slot: its own
    // `next_slot` is still 0. A `Put` submitted to it therefore proposes
    // for *slot 0*, discovers that slot is already decided -- carrying a
    // command byte-identical to the one it wanted to propose -- and
    // completes on that basis. The duplicate never becomes a second log
    // entry, so `Kv::apply` is never asked to apply it a second time, and
    // the `(client, seq)` dedup table this test exists to exercise is
    // never consulted. Measured, not argued: with `Kv::apply`'s dedup
    // disabled entirely, this test's pre-#125 form produced byte-identical
    // observable behavior (same completions, same slots, same per-replica
    // `kv_snapshot`s, same final read) -- the definition of a test that
    // cannot fail for the reason it was written.
    //
    // A read through the log first advances `NodeId(2)` past both writes,
    // so the retry that follows proposes a *fresh* slot and genuinely
    // reaches `Kv::apply` a second time -- which is the only arrangement
    // under which dedup is what makes the assertions below hold.
    let warm_up = c.submit(
        NodeId(2),
        Command::Get {
            client: ClientId(8),
            seq: 0,
            key: 10,
        },
    );
    c.run_for(50_000);
    assert!(c.is_complete(warm_up));

    // A stale retry of the *first* command arrives late, submitted to yet
    // another replica (as if the client, unsure whether its first attempt
    // landed, retried it -- after having already moved on and issued its
    // second command).
    let duplicate = c.submit(NodeId(2), put(1, 1, 10, 100));
    c.run_for(50_000);
    assert!(c.is_complete(duplicate), "the duplicate submission still completes (P8a: applying it again is harmless, not an error)");

    // Establish the arrangement the warm-up buys, rather than trusting it
    // (#111's lesson: a test that assumes its own precondition can lose
    // its power silently). If the retry ever again re-decides the
    // original's slot instead of taking a fresh one, dedup stops being
    // what makes the assertions below pass, and this fires instead of the
    // test quietly going vacuous.
    assert_ne!(
        c.result(duplicate).unwrap().decided_slot,
        c.result(first).unwrap().decided_slot,
        "the retry must occupy a slot of its own -- re-deciding the original's \
         slot means `Kv::apply` never sees it twice, leaving the dedup table \
         this test exists for unexercised"
    );

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
///
/// Falsifier, run: dedup disabled (`is_duplicate = false`) fails this 8/8
/// on the "higher seq must win" assertion -- the lower `seq` genuinely
/// re-applies and clobbers 999 with 111. Unlike its sibling above, this
/// test needed no repair to have that power: the reordered command is a
/// *distinct* command, so it takes a slot of its own and reaches
/// `Kv::apply` a second time without any help.
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
///
/// Falsifier, run: survives both dedup mutations (A1, A2) 0/8, and that is
/// the correct result rather than a gap -- `Get`s are never recorded in
/// the dedup table, so no dedup defect can change what this test observes.
/// It serves as the control that keeps the other three from reading as
/// "any mutation reddens this file".
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

/// The `seq == last_seq` boundary: a client retries the command it *just*
/// sent (having never seen the ack), with no later command of its own in
/// between. This is the most ordinary retry there is, and until #125 no
/// test in `crates/` covered it at this level.
///
/// It needs a second client's write to be observable at all: `Outcome::Put`
/// does not distinguish `Applied::PutNew` from `Applied::PutDuplicate`, so
/// re-applying an identical `Put` is invisible unless somebody else's value
/// is sitting in the key when the retry lands. Client 2's write is what
/// makes the difference between "dropped as a duplicate" (key keeps 555)
/// and "applied a second time" (key regresses to 100) observable through a
/// linearizable read.
///
/// Falsifier, run: `Kv::apply`'s dedup test weakened from `s >= *seq` to
/// `s > *seq` -- the off-by-one that exempts an exact re-retry -- fails
/// this test 8/8 on the "no regression" assertion below. Before this test
/// existed, that mutation was caught by exactly one test in the whole
/// 440-test tree (`kv::tests::duplicate_seq_is_deduplicated`, a unit test
/// beside the code it checks) and by nothing at this level; the other two
/// tests in this file survive it, because each puts a *later* command from
/// the same client in between, which the surviving `>` still deduplicates.
#[test]
fn an_exact_retry_of_the_latest_command_does_not_reapply() {
    let mut c = cluster(4);

    let original = c.submit(NodeId(0), put(1, 1, 10, 100));
    c.run_for(50_000);
    assert!(c.is_complete(original));

    // A *different* client overwrites the same key. Nothing about client
    // 1's dedup state changes -- `last_seq` is per-client (A6).
    let other = c.submit(NodeId(1), put(2, 1, 10, 555));
    c.run_for(50_000);
    assert!(c.is_complete(other));

    // Warm the retry's target replica so the retry proposes a fresh slot
    // rather than re-deciding the original's -- see the long comment in
    // `a_late_duplicate_does_not_clobber_a_later_write` for why this is
    // load-bearing rather than scenery.
    let warm_up = c.submit(
        NodeId(2),
        Command::Get {
            client: ClientId(8),
            seq: 0,
            key: 10,
        },
    );
    c.run_for(50_000);
    assert!(c.is_complete(warm_up));

    // Client 1 retries its own most recent command: same `(client, seq)`,
    // no intervening command of its own. `seq == last_seq[client]`, which
    // is the case `>=` catches and `>` does not.
    let retry = c.submit(NodeId(2), put(1, 1, 10, 100));
    c.run_for(50_000);
    assert!(c.is_complete(retry));
    assert_ne!(
        c.result(retry).unwrap().decided_slot,
        c.result(original).unwrap().decided_slot,
        "the retry must occupy a slot of its own, or `Kv::apply` never sees \
         it twice and this test cannot detect a dedup defect"
    );

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
        Some(queso_smr::Outcome::Get(Some(555))),
        "an exact retry of an already-applied command must have no second \
         effect (P8a); a regression to 100 means it was applied twice"
    );

    let history = history_from_records(&c.results());
    assert!(is_linearizable(&history), "{history:#?}");
}
