//! Pluggable network schedulers, in two type-level classes.
//!
//! [`ObliviousScheduler`] and [`AwareScheduler`] are the two adversary
//! classes called for in `docs/03-testing-plan.md §1` and
//! `docs/02-properties.md` A3:
//!
//! - A **content-oblivious** scheduler (`ObliviousScheduler`) decides
//!   delay/reorder/drop using only an [`EnvelopeMeta`] — source,
//!   destination, size, send time. Its trait method signature simply does
//!   not take a payload, so no implementation of it can ever read message
//!   contents, no matter how it's written. This is the class under which
//!   randomized-liveness properties (P14/P15) may be asserted.
//! - A **content-aware** scheduler (`AwareScheduler<P>`) receives the full
//!   [`Envelope<P>`], payload included, and may target specific messages
//!   (e.g. "deliver the leader's proposal to every `E` set but no `U`
//!   set" in a later phase). Tests using it may assert *safety* and
//!   *fallback*, never unconditional liveness.
//!
//! Four implementations are provided: [`Fifo`] and [`RandomScheduler`] as
//! oblivious baselines, plus [`ContentObliviousAdversary`] and
//! [`ContentAwareAdversary`] as the two adversary classes proper.

use std::collections::BTreeSet;
use std::fmt;

use rand::rngs::StdRng;
use rand::Rng;

use crate::ids::NodeId;
use crate::network::{Envelope, EnvelopeMeta};
use crate::payload::Inspectable;
use crate::time::LogicalTime;

/// What a scheduler decided to do with a message it was asked about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// Deliver the message `delay` ticks after it was sent.
    Deliver {
        /// Delay in logical ticks, added to the send time.
        delay: u64,
    },
    /// Drop the message; it will never be delivered.
    Drop,
}

/// Context handed to a scheduler on every decision: the current logical
/// time, a handle to the kernel's single seeded PRNG (so scheduler
/// randomness stays part of the one deterministic stream), and the
/// currently-designated "leader" node id, if any.
///
/// There is no consensus in Phase 0, so "leader" is nothing more than a
/// settable node id (`Kernel::set_leader`) that adversary schedulers may
/// target — a hook for later phases, and enough to demonstrate DoS-the-
/// leader / refocus-on-change behavior now.
pub struct SchedulerCtx<'a> {
    /// Logical time at which the message was sent.
    pub now: LogicalTime,
    /// The kernel's single seeded PRNG stream.
    pub rng: &'a mut StdRng,
    /// The currently-designated leader, if one has been set.
    pub leader: Option<NodeId>,
}

/// A scheduler that may only see envelope *metadata* — never payload
/// contents. See the module docs for why this is a type-level guarantee.
pub trait ObliviousScheduler: fmt::Debug {
    /// Decide what happens to a message, given only its metadata.
    fn on_send(&mut self, meta: &EnvelopeMeta, ctx: &mut SchedulerCtx<'_>) -> Decision;
}

/// A scheduler that may inspect full message envelopes, payload included.
pub trait AwareScheduler<P>: fmt::Debug {
    /// Decide what happens to a message, with full access to its payload.
    fn on_send(&mut self, envelope: &Envelope<P>, ctx: &mut SchedulerCtx<'_>) -> Decision;
}

/// The network scheduler in use for a given kernel run. Exactly one of the
/// two adversary classes is active at a time; which one is chosen up front
/// when the kernel is built, and it does not change mid-run.
pub enum SchedulerKind<P> {
    /// A content-oblivious scheduler: sees metadata only.
    Oblivious(Box<dyn ObliviousScheduler>),
    /// A content-aware scheduler: sees full envelopes.
    Aware(Box<dyn AwareScheduler<P>>),
}

impl<P> fmt::Debug for SchedulerKind<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SchedulerKind::Oblivious(s) => write!(f, "SchedulerKind::Oblivious({s:?})"),
            SchedulerKind::Aware(s) => write!(f, "SchedulerKind::Aware({s:?})"),
        }
    }
}

impl<P> SchedulerKind<P> {
    /// Ask the active scheduler what to do with `envelope`. Oblivious
    /// schedulers are shown only `envelope.meta`; aware schedulers see the
    /// whole thing.
    pub(crate) fn decide(
        &mut self,
        envelope: &Envelope<P>,
        ctx: &mut SchedulerCtx<'_>,
    ) -> Decision {
        match self {
            SchedulerKind::Oblivious(s) => s.on_send(&envelope.meta, ctx),
            SchedulerKind::Aware(s) => s.on_send(envelope, ctx),
        }
    }
}

/// Clamp a probability into `[0, 1]`.
fn clamp01(p: f64) -> f64 {
    p.clamp(0.0, 1.0)
}

/// Reliable, in-order delivery at a fixed delay. The baseline "nothing goes
/// wrong" scheduler.
#[derive(Debug, Clone, Copy)]
pub struct Fifo {
    delay: u64,
}

impl Fifo {
    /// A FIFO scheduler that delivers every message after exactly `delay`
    /// ticks (must be >= 1 so delivery is strictly in the future).
    pub fn new(delay: u64) -> Self {
        Self {
            delay: delay.max(1),
        }
    }
}

impl Default for Fifo {
    fn default() -> Self {
        Self::new(1)
    }
}

impl ObliviousScheduler for Fifo {
    fn on_send(&mut self, _meta: &EnvelopeMeta, _ctx: &mut SchedulerCtx<'_>) -> Decision {
        // A constant delay applied to a send sequence that is itself
        // strictly non-decreasing in time (a node handles one event at a
        // time) keeps per-link delivery order intact: arrival times are
        // `sent_at + delay`, non-decreasing, and same-time ties are broken
        // by the kernel's monotonic dispatch sequence number, which also
        // follows send order. So this is genuinely FIFO, not just "usually".
        Decision::Deliver { delay: self.delay }
    }
}

/// Seeded random delay + implicit reordering (delay jitter alone is enough
/// to reorder messages relative to each other). No drops: this is a sanity
/// / ordering-assumption-shakeout baseline, not an adversary.
#[derive(Debug, Clone, Copy)]
pub struct RandomScheduler {
    min_delay: u64,
    max_delay: u64,
}

impl RandomScheduler {
    /// Delays are drawn uniformly from `[min_delay, max_delay]` (inclusive).
    pub fn new(min_delay: u64, max_delay: u64) -> Self {
        let min_delay = min_delay.max(1);
        let max_delay = max_delay.max(min_delay);
        Self {
            min_delay,
            max_delay,
        }
    }
}

impl ObliviousScheduler for RandomScheduler {
    fn on_send(&mut self, _meta: &EnvelopeMeta, ctx: &mut SchedulerCtx<'_>) -> Decision {
        let delay = ctx.rng.gen_range(self.min_delay..=self.max_delay);
        Decision::Deliver { delay }
    }
}

/// The content-oblivious adversary (assumption A3). May delay, reorder,
/// drop (respecting eventual delivery — drop probability is capped below
/// 1.0), partition a configured minority away from the rest of the
/// cluster, block specific directed links (asymmetric connectivity), and
/// pile extra drop probability onto whichever node is currently the
/// designated leader — refocusing automatically when the leader changes,
/// since it reads `ctx.leader` fresh on every decision rather than caching
/// it. All of this using only [`EnvelopeMeta`].
#[derive(Debug, Clone)]
pub struct ContentObliviousAdversary {
    min_delay: u64,
    max_delay: u64,
    drop_probability: f64,
    leader_dos_extra_drop: f64,
    minority_partition: BTreeSet<NodeId>,
    asymmetric_blocked: BTreeSet<(NodeId, NodeId)>,
}

impl ContentObliviousAdversary {
    /// A new adversary with a given delay range and no other misbehavior
    /// configured yet; chain the `with_*` builders to add capabilities.
    pub fn new(min_delay: u64, max_delay: u64) -> Self {
        let min_delay = min_delay.max(1);
        let max_delay = max_delay.max(min_delay);
        Self {
            min_delay,
            max_delay,
            drop_probability: 0.0,
            leader_dos_extra_drop: 0.0,
            minority_partition: BTreeSet::new(),
            asymmetric_blocked: BTreeSet::new(),
        }
    }

    /// Baseline probability of dropping any given message. Capped at 0.95
    /// so no single message is *guaranteed* to be dropped forever —
    /// eventual delivery (A2) is a property of the retry stream, not of any
    /// one send, but this keeps a single adversary instance from trivially
    /// wedging a naive test that sends exactly once.
    #[must_use]
    pub fn with_drop_probability(mut self, p: f64) -> Self {
        self.drop_probability = clamp01(p).min(0.95);
        self
    }

    /// Extra drop probability applied on top of the baseline for any
    /// message whose source or destination is the current leader
    /// (`SchedulerCtx::leader`). Together with the baseline this is capped
    /// at 0.95.
    #[must_use]
    pub fn with_leader_dos(mut self, extra_drop: f64) -> Self {
        self.leader_dos_extra_drop = clamp01(extra_drop);
        self
    }

    /// Cut `nodes` off from the rest of the cluster: any message crossing
    /// the minority/majority boundary is dropped, in either direction.
    /// Messages within the minority, or within the majority, are
    /// unaffected by this (though still subject to delay/drop-probability).
    #[must_use]
    pub fn with_minority_partition(mut self, nodes: impl IntoIterator<Item = NodeId>) -> Self {
        self.minority_partition = nodes.into_iter().collect();
        self
    }

    /// Always drop messages sent from `from` to `to` (but not the reverse
    /// direction), simulating asymmetric connectivity.
    #[must_use]
    pub fn with_asymmetric_block(mut self, from: NodeId, to: NodeId) -> Self {
        self.asymmetric_blocked.insert((from, to));
        self
    }
}

impl ObliviousScheduler for ContentObliviousAdversary {
    fn on_send(&mut self, meta: &EnvelopeMeta, ctx: &mut SchedulerCtx<'_>) -> Decision {
        if self.asymmetric_blocked.contains(&(meta.src, meta.dst)) {
            return Decision::Drop;
        }

        let src_minority = self.minority_partition.contains(&meta.src);
        let dst_minority = self.minority_partition.contains(&meta.dst);
        if src_minority != dst_minority {
            return Decision::Drop;
        }

        let mut p = self.drop_probability;
        if let Some(leader) = ctx.leader {
            if meta.src == leader || meta.dst == leader {
                p = clamp01(p + self.leader_dos_extra_drop).min(0.95);
            }
        }
        if p > 0.0 && ctx.rng.gen::<f64>() < p {
            return Decision::Drop;
        }

        let delay = ctx.rng.gen_range(self.min_delay..=self.max_delay);
        Decision::Deliver { delay }
    }
}

/// The content-aware adversary. Same knobs as
/// [`ContentObliviousAdversary`] (delay range, drop probability) plus the
/// one capability an oblivious scheduler structurally cannot have: it may
/// target specific message *kinds* by inspecting the payload via
/// [`Inspectable::tag`]. This is the hook future phases use for fast-path
/// defeat (e.g. dropping only `Vote` messages); Phase 0 has no consensus
/// messages yet, so this exists chiefly to demonstrate the API difference
/// from `ContentObliviousAdversary`, as called for in
/// `docs/03-testing-plan.md §1`.
pub struct ContentAwareAdversary<P> {
    min_delay: u64,
    max_delay: u64,
    drop_probability: f64,
    blocked_tags: BTreeSet<&'static str>,
    _payload: std::marker::PhantomData<fn() -> P>,
}

impl<P> fmt::Debug for ContentAwareAdversary<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ContentAwareAdversary")
            .field("min_delay", &self.min_delay)
            .field("max_delay", &self.max_delay)
            .field("drop_probability", &self.drop_probability)
            .field("blocked_tags", &self.blocked_tags)
            .finish()
    }
}

impl<P> ContentAwareAdversary<P> {
    /// A new content-aware adversary with a given delay range.
    pub fn new(min_delay: u64, max_delay: u64) -> Self {
        let min_delay = min_delay.max(1);
        let max_delay = max_delay.max(min_delay);
        Self {
            min_delay,
            max_delay,
            drop_probability: 0.0,
            blocked_tags: BTreeSet::new(),
            _payload: std::marker::PhantomData,
        }
    }

    /// Baseline drop probability applied regardless of content, capped at
    /// 0.95 (see `ContentObliviousAdversary::with_drop_probability`).
    #[must_use]
    pub fn with_drop_probability(mut self, p: f64) -> Self {
        self.drop_probability = clamp01(p).min(0.95);
        self
    }

    /// Unconditionally drop any message whose payload reports this tag —
    /// something only possible because this scheduler class sees payloads
    /// at all.
    #[must_use]
    pub fn with_blocked_tag(mut self, tag: &'static str) -> Self {
        self.blocked_tags.insert(tag);
        self
    }
}

impl<P> Default for ContentAwareAdversary<P> {
    fn default() -> Self {
        Self::new(1, 1)
    }
}

impl<P: Inspectable> AwareScheduler<P> for ContentAwareAdversary<P> {
    fn on_send(&mut self, envelope: &Envelope<P>, ctx: &mut SchedulerCtx<'_>) -> Decision {
        // This line is the entire point of the type: an ObliviousScheduler
        // implementation has no `envelope` to call `.payload` on.
        if self.blocked_tags.contains(envelope.payload.tag()) {
            return Decision::Drop;
        }
        if self.drop_probability > 0.0 && ctx.rng.gen::<f64>() < self.drop_probability {
            return Decision::Drop;
        }
        let delay = ctx.rng.gen_range(self.min_delay..=self.max_delay);
        Decision::Deliver { delay }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    #[derive(Debug, Clone)]
    struct TaggedPayload(&'static str);
    impl Payload for TaggedPayload {
        fn size(&self) -> usize {
            0
        }
    }
    impl Inspectable for TaggedPayload {
        fn tag(&self) -> &'static str {
            self.0
        }
    }

    use crate::payload::Payload;

    fn meta(src: u32, dst: u32) -> EnvelopeMeta {
        EnvelopeMeta {
            id: crate::ids::MessageId(0),
            src: NodeId(src),
            dst: NodeId(dst),
            size: 10,
            sent_at: LogicalTime::ZERO,
        }
    }

    fn envelope(src: u32, dst: u32, payload: TaggedPayload) -> Envelope<TaggedPayload> {
        Envelope {
            meta: meta(src, dst),
            payload,
        }
    }

    fn ctx(rng: &mut StdRng) -> SchedulerCtx<'_> {
        SchedulerCtx {
            now: LogicalTime::ZERO,
            rng,
            leader: None,
        }
    }

    #[test]
    fn fifo_always_delivers_at_fixed_delay() {
        let mut rng = StdRng::seed_from_u64(1);
        let mut fifo = Fifo::new(3);
        for i in 0..5 {
            let d = fifo.on_send(&meta(0, 1), &mut ctx(&mut rng));
            assert_eq!(d, Decision::Deliver { delay: 3 }, "iteration {i}");
        }
    }

    #[test]
    fn fifo_delay_is_at_least_one() {
        assert_eq!(
            Fifo::new(0).on_send(&meta(0, 1), &mut ctx(&mut StdRng::seed_from_u64(0))),
            Decision::Deliver { delay: 1 }
        );
    }

    #[test]
    fn random_scheduler_delays_stay_in_range() {
        let mut rng = StdRng::seed_from_u64(42);
        let mut sched = RandomScheduler::new(5, 10);
        for _ in 0..200 {
            match sched.on_send(&meta(0, 1), &mut ctx(&mut rng)) {
                Decision::Deliver { delay } => assert!((5..=10).contains(&delay)),
                Decision::Drop => panic!("RandomScheduler must never drop"),
            }
        }
    }

    #[test]
    fn random_scheduler_is_deterministic_given_same_seed() {
        let mut rng_a = StdRng::seed_from_u64(7);
        let mut rng_b = StdRng::seed_from_u64(7);
        let mut sched_a = RandomScheduler::new(1, 100);
        let mut sched_b = RandomScheduler::new(1, 100);
        for _ in 0..50 {
            let a = sched_a.on_send(&meta(0, 1), &mut ctx(&mut rng_a));
            let b = sched_b.on_send(&meta(0, 1), &mut ctx(&mut rng_b));
            assert_eq!(a, b);
        }
    }

    #[test]
    fn content_oblivious_asymmetric_block_is_directional() {
        let mut rng = StdRng::seed_from_u64(0);
        let mut adv =
            ContentObliviousAdversary::new(1, 1).with_asymmetric_block(NodeId(0), NodeId(1));
        assert_eq!(adv.on_send(&meta(0, 1), &mut ctx(&mut rng)), Decision::Drop);
        assert_eq!(
            adv.on_send(&meta(1, 0), &mut ctx(&mut rng)),
            Decision::Deliver { delay: 1 }
        );
    }

    #[test]
    fn content_oblivious_minority_partition_drops_cross_traffic_only() {
        let mut rng = StdRng::seed_from_u64(0);
        let mut adv = ContentObliviousAdversary::new(1, 1).with_minority_partition([NodeId(2)]);
        // majority <-> majority: unaffected.
        assert_eq!(
            adv.on_send(&meta(0, 1), &mut ctx(&mut rng)),
            Decision::Deliver { delay: 1 }
        );
        // majority <-> minority: dropped, both directions.
        assert_eq!(adv.on_send(&meta(0, 2), &mut ctx(&mut rng)), Decision::Drop);
        assert_eq!(adv.on_send(&meta(2, 0), &mut ctx(&mut rng)), Decision::Drop);
    }

    #[test]
    fn content_oblivious_leader_dos_targets_only_leader_traffic() {
        let mut rng = StdRng::seed_from_u64(123);
        let mut adv = ContentObliviousAdversary::new(1, 1).with_leader_dos(0.95);
        let leader = NodeId(0);

        let mut leader_drops = 0;
        let mut other_drops = 0;
        for _ in 0..300 {
            let mut c = ctx(&mut rng);
            c.leader = Some(leader);
            if adv.on_send(&meta(0, 9), &mut c) == Decision::Drop {
                leader_drops += 1;
            }
        }
        for _ in 0..300 {
            let mut c = ctx(&mut rng);
            c.leader = Some(leader);
            if adv.on_send(&meta(8, 9), &mut c) == Decision::Drop {
                other_drops += 1;
            }
        }
        assert!(leader_drops > other_drops, "leader traffic ({leader_drops}) should be dropped far more than non-leader traffic ({other_drops})");
        assert!(
            leader_drops > 200,
            "expected heavy DoS pressure on the leader, got {leader_drops}/300"
        );
    }

    #[test]
    fn content_oblivious_refocuses_when_leader_changes() {
        // No adversary-internal leader state: it reads ctx.leader fresh on
        // every call, so "refocus on change" falls out for free.
        let mut rng = StdRng::seed_from_u64(9);
        let mut adv = ContentObliviousAdversary::new(1, 1).with_leader_dos(0.95);

        let mut drops_targeting_0 = 0;
        for _ in 0..200 {
            let mut c = ctx(&mut rng);
            c.leader = Some(NodeId(0));
            if adv.on_send(&meta(0, 5), &mut c) == Decision::Drop {
                drops_targeting_0 += 1;
            }
        }

        let mut drops_targeting_0_after_switch = 0;
        for _ in 0..200 {
            let mut c = ctx(&mut rng);
            c.leader = Some(NodeId(1)); // leadership moved to node 1
            if adv.on_send(&meta(0, 5), &mut c) == Decision::Drop {
                drops_targeting_0_after_switch += 1;
            }
        }
        assert!(
            drops_targeting_0_after_switch < drops_targeting_0,
            "traffic for the old leader should no longer be specially targeted \
             ({drops_targeting_0_after_switch} vs {drops_targeting_0})"
        );
    }

    #[test]
    fn content_aware_blocks_by_tag() {
        let mut rng = StdRng::seed_from_u64(0);
        let mut adv = ContentAwareAdversary::<TaggedPayload>::new(1, 1).with_blocked_tag("vote");
        let vote = envelope(0, 1, TaggedPayload("vote"));
        let ping = envelope(0, 1, TaggedPayload("ping"));
        assert_eq!(adv.on_send(&vote, &mut ctx(&mut rng)), Decision::Drop);
        assert_eq!(
            adv.on_send(&ping, &mut ctx(&mut rng)),
            Decision::Deliver { delay: 1 }
        );
    }

    /// This is the API-difference demonstration called for in
    /// `docs/03-testing-plan.md §1`: `ObliviousScheduler::on_send` simply
    /// has no parameter through which a payload could reach it, whereas
    /// `AwareScheduler::on_send` takes the full envelope. Both facts are
    /// visible right here at the call sites, not just in the trait
    /// declarations.
    #[test]
    fn oblivious_and_aware_schedulers_have_different_call_shapes() {
        let mut rng = StdRng::seed_from_u64(0);
        let mut oblivious = Fifo::new(1);
        let _: Decision = oblivious.on_send(&meta(0, 1), &mut ctx(&mut rng)); // meta only

        let mut aware = ContentAwareAdversary::<TaggedPayload>::new(1, 1);
        let env = envelope(0, 1, TaggedPayload("ping"));
        let _: Decision = aware.on_send(&env, &mut ctx(&mut rng)); // full envelope, payload included
    }
}
