//! Prioritized proposals and `best()` selection.
//!
//! A [`Proposal`] pairs a candidate value `v` with a random numeric
//! priority (§4.1.2 of the paper: `p = ⟨v, random()⟩`). Proposal *sets*
//! (`P`, `E`, `C`, `U`, ...) are plain [`BTreeSet`]s — never `HashSet`, per
//! the workspace determinism lints — ordered by [`Proposal`]'s `Ord` impl,
//! which is defined so that `set.iter().max()` (what [`best`] uses) always
//! and deterministically returns the highest-priority proposal, with a
//! well-defined (if astronomically unlikely to matter) tie-break.

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt;

use queso_sim::ids::NodeId;

/// A value proposed by some replica, tagged with the random priority that
/// replica drew for it this round, and the replica that drew it.
///
/// Ordering is priority-first — `Ord`/`PartialOrd` are implemented by hand
/// (rather than derived) specifically so that comparison is *never*
/// primarily by `value`: two proposals for the same value from different
/// replicas (or with different priorities) are still distinct, ordered
/// proposals, and the "biggest" one by this ordering is always the one
/// with the highest priority.
///
/// The paper assumes high-entropy priorities make ties "negligible" (see
/// footnote 4 in §4.1.3) but tells us to handle the tie case deterministically
/// anyway; the tie-break here is `origin` (the proposing replica's
/// [`NodeId`]) and then `value` itself, both total orders, so `Proposal`'s
/// `Ord` is a genuine total order with no ambiguity, ever.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proposal<V> {
    /// The proposed value.
    pub value: V,
    /// The random priority attached to this proposal this round.
    pub priority: u64,
    /// The replica that proposed it.
    pub origin: NodeId,
}

impl<V: fmt::Debug> fmt::Display for Proposal<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "<{:?}, prio={}, from={}>",
            self.value, self.priority, self.origin
        )
    }
}

impl<V: Ord> PartialOrd for Proposal<V> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<V: Ord> Ord for Proposal<V> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority
            .cmp(&other.priority)
            .then_with(|| self.origin.cmp(&other.origin))
            .then_with(|| self.value.cmp(&other.value))
    }
}

/// A set of proposals -- what tcast disseminates and returns (`P`, `E`,
/// `C`, `U`, ...). Always a `BTreeSet`, never a `HashSet`: iteration order
/// (and hence anything downstream that might depend on it) must be
/// deterministic.
pub type ProposalSet<V> = BTreeSet<Proposal<V>>;

/// `best(S)`: the highest-priority proposal in `S`, or `None` if `S` is
/// empty. `BTreeSet` iterates in ascending `Ord` order, so the maximum
/// element is exactly the highest-priority proposal per [`Proposal`]'s
/// `Ord` impl above.
pub fn best<V: Ord>(set: &ProposalSet<V>) -> Option<&Proposal<V>> {
    set.iter().max()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(value: u64, priority: u64, origin: u32) -> Proposal<u64> {
        Proposal {
            value,
            priority,
            origin: NodeId(origin),
        }
    }

    #[test]
    fn best_picks_highest_priority() {
        let set: ProposalSet<u64> = [p(1, 10, 0), p(2, 99, 1), p(3, 50, 2)].into();
        assert_eq!(best(&set), Some(&p(2, 99, 1)));
    }

    #[test]
    fn best_of_empty_set_is_none() {
        let set: ProposalSet<u64> = ProposalSet::new();
        assert_eq!(best(&set), None);
    }

    #[test]
    fn best_breaks_priority_ties_by_origin() {
        // Same priority, different origin: the higher NodeId wins,
        // deterministically, regardless of insertion order.
        let set: ProposalSet<u64> = [p(1, 42, 5), p(2, 42, 1), p(3, 42, 9)].into();
        assert_eq!(best(&set), Some(&p(3, 42, 9)));
    }

    #[test]
    fn best_breaks_priority_and_origin_ties_by_value() {
        let set: ProposalSet<u64> = [p(7, 42, 3), p(2, 42, 3)].into();
        assert_eq!(best(&set), Some(&p(7, 42, 3)));
    }

    #[test]
    fn ordering_is_total_and_consistent_with_equality() {
        let a = p(1, 5, 0);
        let b = p(1, 5, 0);
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(a, b);
    }

    #[test]
    fn higher_priority_always_wins_regardless_of_value_magnitude() {
        // A tiny value with a huge priority beats a huge value with a tiny
        // priority -- ordering must never accidentally fall back to `value`
        // as the primary key.
        let low_value_high_prio = p(0, u64::MAX, 0);
        let high_value_low_prio = p(u64::MAX, 0, 0);
        assert!(low_value_high_prio > high_value_low_prio);
    }
}
