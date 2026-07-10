//! The interval summary register (ISR), specialized to constant-space
//! integers (Algorithm 3 in the paper) via [`crate::proposal::Proposal`]'s
//! total order.
//!
//! # Mapping Algorithm 3's integers onto `Proposal<V>`
//!
//! Algorithm 3 is stated generically over "simple binary integers as ISR
//! values, zero as nil, and integer maximum for aggregate" (§4.2.3), because
//! a proposal `⟨priority, proposer, value⟩` is meant to be packed into one
//! fixed-width integer so that comparing two packed integers is the same as
//! comparing `(priority, proposer, value)` lexicographically -- which is
//! *exactly* what [`crate::proposal::Proposal`]'s hand-written `Ord` impl
//! already does (priority first, then origin, then value). So rather than
//! actually packing bits, this ISR stores `Option<Proposal<V>>` directly:
//! `None` is Algorithm 3's `nil`/zero (nothing is smaller than the "absence
//! of a proposal" -- `Option`'s derived `Ord` already places `None` below
//! every `Some`, matching "zero is smaller than every real proposal"
//! unconditionally, regardless of the priority range),
//! and `aggregate` is `Ord::max` over that `Option<Proposal<V>>`.
//!
//! # Why this is constant space
//!
//! Per Algorithm 3 (as opposed to the fully general Algorithm 2), this type
//! keeps exactly four fields -- `step`, `first`, `current_agg`,
//! `prior_agg` -- regardless of how many `record` calls it has served or
//! how many distinct steps have come and gone. Obsolete (`s < S`) values are
//! discarded immediately rather than retained.

use std::cmp::Ordering;

use crate::proposal::Proposal;

/// The `(s', f', a')` triple `record` returns: the ISR's current step, the
/// first value recorded at that step, and the aggregate of everything
/// recorded during the *immediately prior* step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IsrSummary<V> {
    /// `S`: the ISR's internal step counter after handling this call.
    pub step: u64,
    /// `F[S]`: the first value recorded at step `S`. `None` is Algorithm 3's
    /// `nil` (nothing has ever been recorded at this step -- in practice
    /// this only happens if `record` has never been called at all, since
    /// any call with `s >= S` sets it).
    pub first: Option<Proposal<V>>,
    /// `A[S-1]`: the aggregate (max) of everything recorded during the
    /// immediately prior step, or `nil` if the ISR jumped by more than one
    /// step to reach `S` (Algorithm 3's "we saw nothing in `S-1`" case) or
    /// has not yet advanced past its initial step.
    pub prior_agg: Option<Proposal<V>>,
}

/// The specialized, constant-space integer ISR (Algorithm 3), instantiated
/// with `Proposal<V>` (see the module docs for why this is a faithful
/// specialization rather than a divergence from the paper).
///
/// One `Isr<V>` per recorder per slot: this is deliberately *not* itself a
/// [`queso_sim::node::Node`] -- it is the passive state a
/// [`crate::recorder::Recorder`] wraps and drives from `on_message`.
#[derive(Debug, Clone)]
pub struct Isr<V> {
    /// `S`, initially 0.
    step: u64,
    /// `F_c`, the first value received in the current step, initially nil.
    first: Option<Proposal<V>>,
    /// `A_c`, the max value seen in the current step, initially nil.
    current_agg: Option<Proposal<V>>,
    /// `A_p`, the max value seen in the prior step, initially nil.
    prior_agg: Option<Proposal<V>>,
}

impl<V> Default for Isr<V> {
    fn default() -> Self {
        Self {
            step: 0,
            first: None,
            current_agg: None,
            prior_agg: None,
        }
    }
}

impl<V: Ord + Clone> Isr<V> {
    /// A fresh ISR at step 0, everything nil -- Algorithm 3's initial state.
    pub fn new() -> Self {
        Self::default()
    }

    /// `record(s, v) -> (s', f', a')`. See Algorithm 3: obsolete (`s < S`)
    /// values are discarded without changing any state; `s == S` aggregates
    /// `v` into the current step; `s > S` advances the step, carrying
    /// `A_c` forward into `A_p` only when the advance is by exactly one
    /// step (otherwise `A_p` becomes nil, since nothing was ever recorded
    /// in the skipped-over step(s) *at this recorder*). Either way, the
    /// call returns a fresh summary of the (possibly-just-updated) state.
    pub fn record(&mut self, s: u64, v: Proposal<V>) -> IsrSummary<V> {
        match s.cmp(&self.step) {
            Ordering::Less => {
                // Obsolete: discard `v`, state unchanged.
            }
            Ordering::Equal => {
                self.current_agg = max_option(self.current_agg.take(), Some(v));
            }
            Ordering::Greater => {
                self.prior_agg = if s == self.step + 1 {
                    self.current_agg.take()
                } else {
                    None
                };
                self.step = s;
                self.first = Some(v.clone());
                self.current_agg = Some(v);
            }
        }
        self.summary()
    }

    /// The `(S, F[S], A[S-1])` triple as it currently stands, without
    /// recording anything new. Exposed mainly for tests; `record` is the
    /// only operation the protocol itself calls.
    pub fn summary(&self) -> IsrSummary<V> {
        IsrSummary {
            step: self.step,
            first: self.first.clone(),
            prior_agg: self.prior_agg.clone(),
        }
    }
}

/// `aggregate(a, b)` from Algorithm 2/3, instantiated as `Ord::max` over
/// `Option<Proposal<V>>` (`None` = nil, `aggregate(v, nil) = v` holds
/// automatically because `None` compares below every `Some`).
fn max_option<V: Ord>(a: Option<Proposal<V>>, b: Option<Proposal<V>>) -> Option<Proposal<V>> {
    match (a, b) {
        (None, None) => None,
        (Some(x), None) => Some(x),
        (None, Some(y)) => Some(y),
        (Some(x), Some(y)) => Some(if x >= y { x } else { y }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use queso_sim::ids::NodeId;

    fn p(value: u64, priority: u64, origin: u32) -> Proposal<u64> {
        Proposal {
            value,
            priority,
            origin: NodeId(origin),
        }
    }

    #[test]
    fn initial_state_is_all_nil_at_step_zero() {
        let isr: Isr<u64> = Isr::new();
        let s = isr.summary();
        assert_eq!(s.step, 0);
        assert_eq!(s.first, None);
        assert_eq!(s.prior_agg, None);
    }

    #[test]
    fn first_record_advances_step_and_sets_first_and_current() {
        let mut isr: Isr<u64> = Isr::new();
        let out = isr.record(4, p(1, 10, 0));
        // A brand-new ISR jumping from step 0 straight to step 4 is a
        // multi-step advance (jump of 4, not 1), so A_p (prior_agg) is nil.
        assert_eq!(out.step, 4);
        assert_eq!(out.first, Some(p(1, 10, 0)));
        assert_eq!(out.prior_agg, None);
    }

    #[test]
    fn same_step_aggregates_by_max_priority_but_keeps_first_value() {
        let mut isr: Isr<u64> = Isr::new();
        isr.record(4, p(1, 10, 0));
        // A higher-priority proposal arrives at the *same* step: current_agg
        // becomes it, but F_c (the *first* value at this step) is untouched.
        let out = isr.record(4, p(2, 99, 1));
        assert_eq!(out.step, 4);
        assert_eq!(out.first, Some(p(1, 10, 0)), "first value must not change");
        // prior_agg (A_{S-1}) is still whatever it was before -- unaffected
        // by same-step activity.
        assert_eq!(out.prior_agg, None);

        // A *lower*-priority proposal at the same step must not regress the
        // aggregate (max is monotonic).
        let out2 = isr.record(4, p(3, 5, 2));
        assert_eq!(out2.step, 4);
        assert_eq!(out2.first, Some(p(1, 10, 0)));
        // We can't observe current_agg directly, but the *next* single-step
        // advance exposes it via prior_agg -- see the dedicated test below.
    }

    #[test]
    fn stale_step_is_discarded_and_does_not_affect_state() {
        let mut isr: Isr<u64> = Isr::new();
        isr.record(5, p(1, 10, 0));
        let before = isr.summary();
        // s = 3 < S = 5: must be silently discarded.
        let out = isr.record(3, p(99, 999, 9));
        assert_eq!(out, before, "obsolete record() must not change any state");
        // Confirm a subsequent same-step call still sees the *original*
        // first value, proving the stale value was never recorded as F_c.
        let out2 = isr.record(5, p(2, 1, 1));
        assert_eq!(out2.first, Some(p(1, 10, 0)));
    }

    #[test]
    fn single_step_advance_carries_current_agg_into_prior_agg() {
        let mut isr: Isr<u64> = Isr::new();
        isr.record(4, p(1, 10, 0));
        isr.record(4, p(2, 99, 1)); // current_agg at step 4 is now p(2,99,1)
        let out = isr.record(5, p(3, 1, 2)); // exactly one step forward
        assert_eq!(out.step, 5);
        assert_eq!(out.first, Some(p(3, 1, 2)));
        assert_eq!(
            out.prior_agg,
            Some(p(2, 99, 1)),
            "A_p must be the max seen during step 4"
        );
    }

    #[test]
    fn multi_step_jump_discards_intervening_aggregate_as_nil() {
        let mut isr: Isr<u64> = Isr::new();
        isr.record(4, p(1, 10, 0));
        isr.record(4, p(2, 99, 1)); // current_agg at step 4 is p(2,99,1)
                                    // Jump straight from 4 to 7 (skipping 5 and 6 entirely at this
                                    // recorder): A_p must become nil, *not* carry forward step 4's
                                    // aggregate, per Algorithm 3's "we saw nothing in s-1" branch.
        let out = isr.record(7, p(3, 1, 2));
        assert_eq!(out.step, 7);
        assert_eq!(out.first, Some(p(3, 1, 2)));
        assert_eq!(out.prior_agg, None);
    }

    #[test]
    fn advancing_by_one_after_no_activity_in_prior_step_yields_nil_prior_agg() {
        let mut isr: Isr<u64> = Isr::new();
        // Jump straight to step 10 (nothing recorded at step 9 ever).
        isr.record(10, p(1, 1, 0));
        // Now advance by exactly one, to 11: A_p should be whatever was at
        // step 10 (p(1,1,0)), since *that* transition is a proper +1.
        let out = isr.record(11, p(2, 2, 1));
        assert_eq!(out.prior_agg, Some(p(1, 1, 0)));
    }

    #[test]
    fn repeated_calls_at_the_same_step_are_idempotent_for_first() {
        let mut isr: Isr<u64> = Isr::new();
        isr.record(6, p(5, 5, 5));
        let a = isr.record(6, p(5, 5, 5));
        let b = isr.record(6, p(5, 5, 5));
        assert_eq!(a, b);
        assert_eq!(a.first, Some(p(5, 5, 5)));
    }

    #[test]
    fn max_aggregate_breaks_ties_by_proposal_ord_not_arrival_order() {
        let mut isr: Isr<u64> = Isr::new();
        // Same priority, different origin -- Proposal::Ord tie-breaks by
        // origin, so the higher NodeId should win the aggregate regardless
        // of which arrived first.
        isr.record(8, p(1, 42, 1));
        isr.record(8, p(2, 42, 9));
        let out = isr.record(9, p(3, 1, 0)); // advance by one, exposing A_p
        assert_eq!(out.prior_agg, Some(p(2, 42, 9)));
    }

    #[test]
    fn constant_space_shape_never_grows() {
        // Not a literal memory-size assertion (that would be
        // implementation-detail-brittle), but a behavioral proxy: no matter
        // how many steps/records we push through, `Isr` exposes only ever
        // the current summary triple -- there is no API to retrieve
        // historical steps, and `std::mem::size_of` is independent of call
        // count.
        let mut isr: Isr<u64> = Isr::new();
        let size_before = std::mem::size_of_val(&isr);
        for step in 4..2000u64 {
            isr.record(step, p(step, step, 0));
        }
        let size_after = std::mem::size_of_val(&isr);
        assert_eq!(size_before, size_after);
    }
}
