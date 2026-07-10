//! A dependency-light, in-tree linearizability checker (P8).
//!
//! # The model
//!
//! A [`HistoryOp`] is one completed client operation: the command submitted,
//! the logical time it was invoked, the logical time it completed, and what
//! it observed (for a `Get`) or that it took effect (for a `Put`). Given a
//! set of these, a history is **linearizable** iff there exists *some* total
//! order of all the operations such that:
//!
//! 1. it is consistent with the real-time partial order (if `a` completed
//!    strictly before `b` was invoked, `a` precedes `b` in the order --
//!    concurrent operations, whose intervals overlap, may be ordered either
//!    way), and
//! 2. replaying the operations in that order against the sequential
//!    specification -- [`crate::kv::Kv`] itself, applied via
//!    [`crate::kv::Kv::apply`] -- reproduces every `Get`'s observed value.
//!
//! Reusing `Kv` (idempotent `(client, seq)` dedup included) as the
//! sequential spec rather than a "naive" always-overwrite KV model is
//! deliberate: P8a's contract is that a duplicate `(client, seq)` has *the
//! same effect as* applying it once, which is part of what this system
//! promises its clients, not an implementation accident to paper over. A
//! history containing a genuine duplicate submission (see the idempotency
//! tests) is only linearizable against a spec that itself dedups -- so this
//! checker validates P8 and P8a together, exactly as
//! `docs/00-project-outline.md` asks for.
//!
//! # The search
//!
//! Classic Wing & Gong / Lowe-style backtracking: repeatedly pick any
//! *minimal* not-yet-placed operation (one none of whose real-time
//! predecessors remain unplaced), tentatively apply it to a scratch copy of
//! the reference state, check it against the recorded outcome, and recurse;
//! backtrack on mismatch. A `(used-set, resulting state)` memo of
//! already-failed branches prunes repeated work. This is worst-case
//! exponential in the number of concurrent operations, which is exactly why
//! `docs/00-project-outline.md` calls for it only against *small* histories
//! -- the tests here stay in the tens-of-operations range, not thousands.

use std::collections::{BTreeMap, BTreeSet};

use queso_sim::time::LogicalTime;

use crate::command::Command;
use crate::kv::{Applied, Kv};
use crate::replica::{OpId, OpRecord, Outcome};

/// One completed operation, ready to be checked. See the module docs.
#[derive(Debug, Clone)]
pub struct HistoryOp {
    pub op_id: OpId,
    pub command: Command,
    pub invoked_at: LogicalTime,
    pub completed_at: LogicalTime,
    pub outcome: Outcome,
}

/// Build a checkable history from [`crate::cluster::SmrCluster::results`].
/// Operations that never completed (still pending, or their replica
/// crashed mid-flight) are dropped -- the standard treatment for an
/// incomplete history is to ignore operations with no response, since
/// nothing was ever observed for them to be inconsistent *with*.
pub fn history_from_records(records: &BTreeMap<OpId, OpRecord>) -> Vec<HistoryOp> {
    records
        .iter()
        .filter_map(|(&op_id, r)| {
            let completed_at = r.completed_at?;
            let outcome = r.outcome.clone()?;
            Some(HistoryOp {
                op_id,
                command: r.command.clone(),
                invoked_at: r.invoked_at,
                completed_at,
                outcome,
            })
        })
        .collect()
}

/// Is `history` linearizable against [`Kv`] as the sequential
/// specification? See the module docs for the exact model and algorithm.
///
/// # Panics
///
/// Asserts `history.len() <= 63` (the search's memo uses a `u64` bitmask of
/// "which operations have been placed so far"; 64+ operations would
/// silently shift-overflow that mask rather than error). This is a real
/// `assert!`, not a `debug_assert!`, so a release build fails loudly
/// instead of returning a wrong (and unsound) answer -- this checker is
/// documented, deliberately, as a small-history brute-force tool, not a
/// scalable one, and the check is cheap enough that release builds don't
/// need to skip it.
pub fn is_linearizable(history: &[HistoryOp]) -> bool {
    let n = history.len();
    assert!(
        n <= 63,
        "is_linearizable is a brute-force checker for small histories only"
    );
    if n == 0 {
        return true;
    }

    // must_precede[i][j]: op i's response happened-before op j's
    // invocation, so any valid linearization must place i before j.
    let mut must_precede = vec![vec![false; n]; n];
    for i in 0..n {
        for j in 0..n {
            if i != j && history[i].completed_at < history[j].invoked_at {
                must_precede[i][j] = true;
            }
        }
    }

    let mut used = vec![false; n];
    let mut failed: BTreeSet<(u64, Kv)> = BTreeSet::new();
    search(history, &must_precede, &mut used, n, Kv::new(), &mut failed)
}

fn mask_of(used: &[bool]) -> u64 {
    used.iter()
        .enumerate()
        .fold(0u64, |m, (i, &b)| if b { m | (1 << i) } else { m })
}

/// Recursive step: try every currently-minimal unused operation as "the
/// next one in the linearization", applying it to `state` and checking its
/// recorded outcome; recurse on success, backtrack on mismatch or dead end.
fn search(
    history: &[HistoryOp],
    must_precede: &[Vec<bool>],
    used: &mut [bool],
    remaining: usize,
    state: Kv,
    failed: &mut BTreeSet<(u64, Kv)>,
) -> bool {
    if remaining == 0 {
        return true;
    }
    let mask = mask_of(used);
    if failed.contains(&(mask, state.clone())) {
        return false;
    }

    let n = history.len();
    for i in 0..n {
        if used[i] {
            continue;
        }
        let has_unplaced_predecessor = (0..n).any(|j| !used[j] && j != i && must_precede[j][i]);
        if has_unplaced_predecessor {
            continue;
        }

        let mut candidate_state = state.clone();
        let applied = candidate_state.apply(&history[i].command);
        let observed_matches = match (&history[i].outcome, applied) {
            (Outcome::Put, Applied::PutNew | Applied::PutDuplicate) => true,
            (Outcome::Get(expected), Applied::Get(actual)) => *expected == actual,
            // A `Get` outcome for a `Put` command (or vice versa) would be
            // a bug in the caller building the history, not a real
            // linearizability question -- treat it as a non-match rather
            // than panicking, so a malformed test fails loudly via
            // `is_linearizable` returning `false` instead of crashing.
            _ => false,
        };
        if !observed_matches {
            continue;
        }

        used[i] = true;
        if search(
            history,
            must_precede,
            used,
            remaining - 1,
            candidate_state,
            failed,
        ) {
            used[i] = false;
            return true;
        }
        used[i] = false;
    }

    failed.insert((mask, state));
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::ClientId;

    fn put(op: u64, client: u32, seq: u64, key: u32, value: i64, inv: u64, comp: u64) -> HistoryOp {
        HistoryOp {
            op_id: OpId(op),
            command: Command::Put {
                client: ClientId(client),
                seq,
                key,
                value,
            },
            invoked_at: LogicalTime(inv),
            completed_at: LogicalTime(comp),
            outcome: Outcome::Put,
        }
    }

    fn get(
        op: u64,
        client: u32,
        seq: u64,
        key: u32,
        observed: Option<i64>,
        inv: u64,
        comp: u64,
    ) -> HistoryOp {
        HistoryOp {
            op_id: OpId(op),
            command: Command::Get {
                client: ClientId(client),
                seq,
                key,
            },
            invoked_at: LogicalTime(inv),
            completed_at: LogicalTime(comp),
            outcome: Outcome::Get(observed),
        }
    }

    #[test]
    fn empty_history_is_linearizable() {
        assert!(is_linearizable(&[]));
    }

    #[test]
    fn sequential_put_then_get_is_linearizable() {
        let history = vec![
            put(0, 1, 0, 10, 100, 0, 5),
            get(1, 2, 0, 10, Some(100), 6, 10),
        ];
        assert!(is_linearizable(&history));
    }

    #[test]
    fn a_read_before_any_write_completed_must_not_see_it() {
        // Get is invoked and completes entirely before Put's response --
        // real time forces Get before Put, so it must observe `None`.
        let history = vec![get(0, 1, 0, 10, None, 0, 2), put(1, 2, 0, 10, 100, 3, 8)];
        assert!(is_linearizable(&history));
    }

    #[test]
    fn concurrent_reads_can_observe_either_order() {
        // Two puts overlap in real time with no ordering forced between
        // them (both invoked before either completes); a get afterward
        // must be consistent with *some* order of the two, and each
        // possible resulting value is individually linearizable.
        let a = put(0, 1, 0, 10, 100, 0, 10);
        let b = put(1, 2, 0, 10, 200, 1, 11);
        let g1 = get(2, 3, 0, 10, Some(200), 12, 15);
        assert!(is_linearizable(&[a.clone(), b.clone(), g1]));

        let g2 = get(3, 3, 0, 10, Some(100), 12, 15);
        assert!(is_linearizable(&[a, b, g2]));
    }

    /// The positive control: a `get` that completes *after* a `put` to the
    /// same key has already completed, but still observes the *old* value,
    /// is the textbook stale-read anomaly (N3) -- no legal linearization
    /// can explain it, because real time forces the put before the get, and
    /// the put's effect is not optional. Demonstrates the checker has
    /// teeth: it is not vacuously true for any input.
    #[test]
    fn stale_read_after_a_completed_write_is_rejected() {
        let history = vec![put(0, 1, 0, 10, 100, 0, 5), get(1, 2, 0, 10, None, 6, 10)];
        assert!(
            !is_linearizable(&history),
            "a Get strictly after a completed Put must see it -- this history is a stale read"
        );
    }

    #[test]
    fn reordered_writes_observed_by_a_later_read_are_rejected() {
        let history = vec![
            put(0, 1, 0, 10, 100, 0, 5),
            put(1, 1, 1, 10, 200, 6, 10),
            // Real time forces 100 then 200; a read strictly afterward
            // claiming to see 100 is impossible.
            get(2, 2, 0, 10, Some(100), 11, 15),
        ];
        assert!(!is_linearizable(&history));
    }

    #[test]
    fn duplicate_client_seq_is_checked_against_the_idempotent_spec() {
        // Two *separate* submissions of the identical (client, seq) command
        // (a retry to a different replica) both "complete", followed by a
        // second write and a read that must see the second write, not a
        // regression back to the first -- exactly the P8a scenario. This is
        // only linearizable because the reference spec (`Kv`) itself
        // dedups; see the module docs.
        let dup_a = put(0, 1, 5, 10, 100, 0, 5);
        let write2 = put(1, 1, 6, 10, 200, 6, 10);
        let dup_b = put(2, 1, 5, 10, 100, 11, 15); // stale replay of the seq=5 put
        let read = get(3, 2, 0, 10, Some(200), 16, 20);
        assert!(is_linearizable(&[dup_a, write2, dup_b, read]));
    }
}
