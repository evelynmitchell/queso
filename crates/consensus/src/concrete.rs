//! The Phase-2/3 driver: runs `N` replicas, each hosting one
//! [`crate::proposer::Proposer`] (active) and one [`crate::recorder::Recorder`]
//! (passive), on the harness for a single slot, and reports decisions.
//!
//! Unlike Phase 1's [`crate::algorithm::Cluster`] (which drives replicas in
//! externally-imposed lock-step rounds, calling into `crate::tcast` as a
//! blocking, synchronous-looking function), [`ConcreteCluster`] does none of
//! that: once [`ConcreteCluster::run_slot`] injects each live replica's
//! initial kickoff timer, the entire multi-round, multi-phase protocol --
//! including every proposer's independent progress, retries, and
//! catch-ups -- unfolds purely through [`queso_sim::node::Node`] callbacks
//! (`crate::proposer::Proposer::on_response`/`on_timer`), racing against
//! whatever the configured scheduler and fault injection do to the network.
//! Different replicas' proposers can and do end up at different steps at
//! the same logical time; there is no round barrier anywhere in this
//! module.
//!
//! [`ConcreteCluster::new`] builds a purely leaderless slot (Phase 2, still
//! the correctness-baseline fallback); [`ConcreteCluster::new_with_leader`]
//! (Phase 3, §4.2.5) additionally designates one replica as the round-1
//! fast-path leader -- see `crate::proposer`'s module docs for the safety
//! argument tying the two together. [`ConcreteCluster::new_with_schedule`]
//! and [`ConcreteCluster::new_with_delays`] (Phase 5, §5.1-5.2) additionally
//! stagger *when* each replica's proposer activates -- see `crate::proposer`'s
//! module docs' "Hedging" section for the mechanism; this module's only job
//! is schedule *construction* (turning a leader + base delay δ, or an
//! explicit per-replica map, into the `activation_delay` each
//! [`crate::proposer::Proposer`] is built with) and wiring each replica's
//! recorder up to feed its own colocated proposer's evidence-of-progress
//! signal (`ReplicaNode`'s `local_step` field, below).

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::rc::Rc;

use queso_sim::fault::FaultCommand;
use queso_sim::ids::{NodeId, TimerId};
use queso_sim::node::{Node, NodeCtx};
use queso_sim::scheduler::SchedulerKind;
use queso_sim::trace::{Trace, TraceEvent};
use queso_sim::Kernel;

use crate::proposer::{Proposer, KICKOFF_TIMER};
use crate::recorder::Recorder;
use crate::rpc::ConcreteMsg;

/// One replica's [`queso_sim::node::Node`] implementation: the seam that
/// routes kernel callbacks to its colocated proposer and recorder. Thin by
/// design, mirroring `crate::node::ReplicaNode` from Phase 1 -- all the
/// actual protocol logic lives in [`crate::proposer::Proposer`] and
/// [`crate::recorder::Recorder`], reachable here only via `Rc<RefCell<_>>`
/// handles shared with [`ConcreteCluster`] (which reads decisions back out
/// after the kernel finishes running).
struct ReplicaNode<V> {
    proposer: Rc<RefCell<Proposer<V>>>,
    recorder: Rc<RefCell<Recorder<V>>>,
    /// This replica's own colocated recorder's most-recently-observed ISR
    /// step, updated below every time `recorder` answers *any* proposer's
    /// `record` request (local or remote) -- the evidence-of-progress
    /// signal `proposer` consults when hedged (`Proposer::with_hedging`;
    /// see that module's "Hedging" docs). Shared (not owned) because
    /// `Proposer` itself needs read access to the same cell.
    local_step: Rc<Cell<u64>>,
}

impl<V> ReplicaNode<V> {
    fn new(
        proposer: Rc<RefCell<Proposer<V>>>,
        recorder: Rc<RefCell<Recorder<V>>>,
        local_step: Rc<Cell<u64>>,
    ) -> Self {
        Self {
            proposer,
            recorder,
            local_step,
        }
    }
}

impl<V: Ord + Clone> Node<ConcreteMsg<V>> for ReplicaNode<V> {
    fn on_message(
        &mut self,
        from: NodeId,
        payload: ConcreteMsg<V>,
        ctx: &mut NodeCtx<'_, ConcreteMsg<V>>,
    ) {
        match payload {
            ConcreteMsg::Request(req) => {
                // Passive recorder role: answer the RPC, then publish the
                // resulting step for this replica's own (possibly hedged)
                // proposer to observe -- see `local_step`'s docs. This is
                // pure bookkeeping: the recorder's own answer is unchanged.
                let resp = self.recorder.borrow_mut().handle(req);
                self.local_step.set(resp.step);
                ctx.send(from, ConcreteMsg::Response(resp));
            }
            ConcreteMsg::Response(resp) => {
                self.proposer.borrow_mut().on_response(from, resp, ctx);
            }
        }
    }

    fn on_timer(&mut self, timer_id: TimerId, ctx: &mut NodeCtx<'_, ConcreteMsg<V>>) {
        self.proposer.borrow_mut().on_timer(timer_id, ctx);
    }

    // `on_restart` is intentionally left at the trait's default (a no-op):
    // Phase 2 has no durable-state design (that is a Phase-4 concern, see
    // docs/02-properties.md P12) and this phase's tests only exercise
    // crash injection, never restart -- so there is no sanctioned recovery
    // behavior to implement yet, and pretending otherwise here would be
    // misleading.
}

/// Drives the concrete QuePaxa protocol (Algorithm 4 + the ISR) for one
/// consensus slot across a fixed set of replicas, on top of a [`Kernel`].
pub struct ConcreteCluster<V> {
    kernel: Kernel<ConcreteMsg<V>>,
    replicas: Vec<NodeId>,
    proposers: BTreeMap<NodeId, Rc<RefCell<Proposer<V>>>>,
    /// The slot's designated fast-path leader (§4.2.5), if any -- the same
    /// value every [`Proposer`] in `proposers` was constructed with (see
    /// `Proposer::new`'s "every proposer must be built with the same
    /// `leader` value" invariant) and also handed to the kernel via
    /// `Kernel::set_leader`, so content-oblivious adversaries' `with_leader_dos`
    /// and friends target the same replica the protocol itself treats as
    /// leader.
    leader: Option<NodeId>,
    /// Driver-tracked liveness, mirroring `crate::algorithm::Cluster`'s
    /// rationale: `Kernel` doesn't expose fault state publicly, and
    /// "currently live" here is purely a bookkeeping convenience for
    /// deciding which replicas to kick off -- quorum math itself is always
    /// against the *full* replica count (see `crate::proposer`'s module
    /// docs on why).
    live: BTreeSet<NodeId>,
    /// Each replica's configured hedging activation delay (Phase 5,
    /// §5.1-5.2) -- the same value each was built with via
    /// `Proposer::with_hedging`. All zero for clusters built via `new`/
    /// `new_with_leader` (unconditional δ=0 activation, unchanged).
    activation_delays: BTreeMap<NodeId, u64>,
}

impl<V: Ord + Clone + Debug + 'static> ConcreteCluster<V> {
    /// Build a purely **leaderless** cluster (Phase 2 behavior, unchanged)
    /// with one replica per `(NodeId, initial value)` pair, all initially
    /// live. Equivalent to `Self::new_with_leader(seed, scheduler,
    /// initial_values, None)`.
    pub fn new(
        seed: u64,
        scheduler: SchedulerKind<ConcreteMsg<V>>,
        initial_values: BTreeMap<NodeId, V>,
    ) -> Self {
        Self::new_with_leader(seed, scheduler, initial_values, None)
    }

    /// Build a cluster with one replica per `(NodeId, initial value)` pair,
    /// all initially live, and `leader` designated as the slot's fast-path
    /// leader (§4.2.5, D1) for round 1 -- or `None` for the purely
    /// leaderless Phase-2 behavior. Equivalent to
    /// `Self::new_with_schedule(seed, scheduler, initial_values, leader, 0)`
    /// -- base delay δ=0, so every proposer activates unconditionally the
    /// instant it starts (Phase 3 behavior, unchanged; see
    /// `crate::proposer`'s module docs' "Hedging" section for why δ=0
    /// collapses exactly to this).
    pub fn new_with_leader(
        seed: u64,
        scheduler: SchedulerKind<ConcreteMsg<V>>,
        initial_values: BTreeMap<NodeId, V>,
        leader: Option<NodeId>,
    ) -> Self {
        Self::new_with_schedule(seed, scheduler, initial_values, leader, 0)
    }

    /// Build a cluster whose proposers follow a **hedging schedule**
    /// (Phase 5, §5.1-5.2): `leader` (if any) is always first, at delay 0;
    /// every other replica is ranked in ascending `NodeId` order and
    /// assigned delay `rank * base_delay` (the paper's single base delay
    /// δ). A leaderless cluster (`leader: None`) still gets a schedule --
    /// the lowest-`NodeId` replica is rank 0 -- since §5.2's construction
    /// only special-cases *whether* there's a leader to put first, not
    /// whether hedging itself applies.
    ///
    /// `base_delay = 0` reproduces `new_with_leader`'s unconditional
    /// activation exactly (every rank's delay collapses to `0`).
    pub fn new_with_schedule(
        seed: u64,
        scheduler: SchedulerKind<ConcreteMsg<V>>,
        initial_values: BTreeMap<NodeId, V>,
        leader: Option<NodeId>,
        base_delay: u64,
    ) -> Self {
        let mut order: Vec<NodeId> = initial_values.keys().copied().collect();
        order.sort();
        if let Some(l) = leader {
            order.retain(|&id| id != l);
            order.insert(0, l);
        }
        let delays: BTreeMap<NodeId, u64> = order
            .into_iter()
            .enumerate()
            .map(|(rank, id)| (id, rank as u64 * base_delay))
            .collect();
        Self::new_with_delays(seed, scheduler, initial_values, leader, delays)
    }

    /// Build a cluster with an **explicit, arbitrary per-replica**
    /// activation delay map (Phase 5, §5.1-5.2) -- the fully general form
    /// underlying [`Self::new_with_schedule`], useful for exercising
    /// deliberately non-monotonic or per-proposer-misconfigured schedules
    /// (P15: liveness must hold for *any* δ, including badly misconfigured
    /// ones) that a single base-delay-and-rank formula cannot express. A
    /// replica missing from `activation_delays` gets delay `0` (activates
    /// unconditionally, same as an unhedged proposer).
    ///
    /// `leader` is passed to *every* [`Proposer`] built here (the
    /// invariant `Proposer::new` documents) and to the kernel via
    /// `Kernel::set_leader`, so adversary schedulers that key off "the
    /// current leader" (e.g.
    /// `queso_sim::scheduler::ContentObliviousAdversary::with_leader_dos`)
    /// target the same replica.
    pub fn new_with_delays(
        seed: u64,
        scheduler: SchedulerKind<ConcreteMsg<V>>,
        initial_values: BTreeMap<NodeId, V>,
        leader: Option<NodeId>,
        activation_delays: BTreeMap<NodeId, u64>,
    ) -> Self {
        let n = initial_values.len();
        let mut kernel = Kernel::new(seed, scheduler);
        kernel.set_leader(leader);
        let mut proposers = BTreeMap::new();
        let mut replicas = Vec::new();
        let mut resolved_delays = BTreeMap::new();

        for (id, v) in initial_values {
            let delay = activation_delays.get(&id).copied().unwrap_or(0);
            let local_step = Rc::new(Cell::new(0));
            // Slot 0: `ConcreteCluster` is single-slot (see `crate::rpc`'s
            // `RecordRequest::slot` docs for what this tag is for and why
            // it is inert here).
            let proposer = Rc::new(RefCell::new(
                Proposer::new(id, n, v, leader, 0).with_hedging(delay, local_step.clone()),
            ));
            let recorder = Rc::new(RefCell::new(Recorder::new()));
            kernel.add_node(
                id,
                Box::new(ReplicaNode::new(proposer.clone(), recorder, local_step)),
            );
            proposers.insert(id, proposer);
            replicas.push(id);
            resolved_delays.insert(id, delay);
        }
        replicas.sort();
        let live = replicas.iter().copied().collect();

        Self {
            kernel,
            replicas,
            proposers,
            leader,
            live,
            activation_delays: resolved_delays,
        }
    }

    /// The full, static replica membership.
    pub fn replicas(&self) -> &[NodeId] {
        &self.replicas
    }

    /// The slot's designated fast-path leader, if any.
    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }

    /// This replica's configured hedging activation delay (`0` for
    /// clusters built via `new`/`new_with_leader`).
    pub fn activation_delay(&self, id: NodeId) -> u64 {
        self.activation_delays.get(&id).copied().unwrap_or(0)
    }

    /// Whether this replica's proposer has ever sent a `record` request --
    /// `false` the whole time a hedged, currently-passive proposer is
    /// waiting out its delay or deferring to observed progress elsewhere
    /// (see `crate::proposer::Proposer::activated`'s docs).
    pub fn activated(&self, id: NodeId) -> bool {
        self.proposers
            .get(&id)
            .is_some_and(|p| p.borrow().activated())
    }

    /// Total number of messages sent so far in this run (both `record`
    /// requests and their responses) -- a direct proxy for D2's `O(n)`
    /// (leader-only, under synchrony) vs. `O(n^2)` (all-proposers-active)
    /// messaging-cost distinction, read straight off the trace so it
    /// requires no separate bookkeeping.
    pub fn message_count(&self) -> usize {
        self.kernel
            .trace()
            .events()
            .iter()
            .filter(|e| matches!(e, TraceEvent::Send { .. }))
            .count()
    }

    /// Replicas the driver currently considers live (not crashed).
    pub fn live(&self) -> &BTreeSet<NodeId> {
        &self.live
    }

    /// Crash a replica: stops it from sending/receiving (via the kernel's
    /// real fault injection), and removes it from the driver's live set so
    /// `run_slot` stops trying to kick off its proposer. Must be called
    /// *before* `run_slot` -- Phase 2 only exercises crashes present from
    /// the start of the slot, matching this phase's property-test scope
    /// (crash injection, not restart -- see `ReplicaNode`'s docs).
    pub fn crash(&mut self, id: NodeId) {
        self.kernel.crash(id);
        self.live.remove(&id);
    }

    /// Install a **genuine** network partition (test-only fault-injection
    /// escape hatch wrapping `Kernel::partition`) between two disjoint
    /// groups of replicas: messages between opposite sides are dropped both
    /// at send time and, if already in flight when the partition takes
    /// effect, at arrival too (see `queso_sim::fault`'s `DropReason::
    /// Partitioned`/`PartitionedAtArrival`) -- a real network cut, not
    /// merely a scheduler-level drop. Same-side traffic is unaffected.
    /// Unlike [`Self::crash`], partitioned replicas stay in the driver's
    /// `live` set: they are still running, just unable to reach the other
    /// side.
    pub fn partition(&mut self, group_a: BTreeSet<NodeId>, group_b: BTreeSet<NodeId>) {
        self.kernel.partition(group_a, group_b);
    }

    /// Remove any active partition installed via [`Self::partition`] (or
    /// [`Self::schedule_heal`]/[`Self::schedule_partition`]). A no-op if no
    /// partition is active.
    pub fn heal(&mut self) {
        self.kernel.heal();
    }

    /// Schedule a partition to take effect `after_ticks` logical ticks from
    /// the cluster's current time (`Kernel::schedule_fault`), so a test can
    /// install a genuine mid-run partition deterministically -- e.g. after
    /// replicas have already exchanged some protocol messages -- rather
    /// than only before [`Self::run_slot`] starts.
    pub fn schedule_partition(
        &mut self,
        after_ticks: u64,
        group_a: BTreeSet<NodeId>,
        group_b: BTreeSet<NodeId>,
    ) {
        let at = self.kernel.now().advance(after_ticks);
        self.kernel
            .schedule_fault(at, FaultCommand::Partition(group_a, group_b));
    }

    /// Schedule a heal (removal of any active partition) `after_ticks`
    /// logical ticks from the cluster's current time.
    pub fn schedule_heal(&mut self, after_ticks: u64) {
        let at = self.kernel.now().advance(after_ticks);
        self.kernel.schedule_fault(at, FaultCommand::Heal);
    }

    /// This replica's decision, if it has delivered one yet.
    pub fn decided(&self, id: NodeId) -> Option<V> {
        self.proposers
            .get(&id)
            .and_then(|p| p.borrow().decided().cloned())
    }

    /// This replica's current threshold-clock step (for tests/
    /// introspection into the randomized-termination behavior).
    pub fn step(&self, id: NodeId) -> u64 {
        self.proposers[&id].borrow().step()
    }

    /// Whether this replica decided via the phase-0 fast path (§4.2.5, D1)
    /// -- i.e. in a single round-trip, without ever leaving round 1's first
    /// step. See `Proposer::decided_via_fast_path`.
    pub fn decided_via_fast_path(&self, id: NodeId) -> bool {
        self.proposers
            .get(&id)
            .is_some_and(|p| p.borrow().decided_via_fast_path())
    }

    /// True once every currently-live replica has decided.
    pub fn all_live_decided(&self) -> bool {
        !self.live.is_empty() && self.live.iter().all(|&id| self.decided(id).is_some())
    }

    /// Kick off every live replica's proposer (Algorithm 4, round 1 phase
    /// 0) and run the kernel for up to `max_ticks` logical ticks. Unlike
    /// Phase 1's `run_slot(max_rounds)`, this is a single push: there is no
    /// round-by-round external stepping to do, since the whole protocol
    /// (including retries and catch-ups) is driven entirely by `Node`
    /// callbacks once started. A bounded `run_until` (rather than an
    /// unbounded `run()`) is used deliberately: since replica progress
    /// depends on genuinely random scheduling, a given seed is not
    /// *guaranteed* to converge within any fixed tick budget (only
    /// guaranteed to converge with probability 1 in the limit, per P14) --
    /// bounding ticks keeps a pathological seed from hanging a test instead
    /// of just failing its "did everyone decide" assertion.
    pub fn run_slot(&mut self, max_ticks: u64) {
        for &id in &self.live {
            self.kernel.inject_timer(id, 0, KICKOFF_TIMER);
        }
        let until = self.kernel.now().advance(max_ticks);
        self.kernel.run_until(until);
    }

    /// The kernel's recorded trace so far (for determinism/reproducibility
    /// checks).
    pub fn trace(&self) -> &Trace {
        self.kernel.trace()
    }

    /// The kernel's current logical time.
    pub fn now(&self) -> queso_sim::time::LogicalTime {
        self.kernel.now()
    }

    /// Advance the kernel by `more_ticks` from wherever it currently is,
    /// running whatever events (retries, hedge rechecks, in-flight
    /// messages) are already scheduled -- unlike [`Self::run_slot`], this
    /// does **not** re-inject any replica's `KICKOFF_TIMER`, so it is safe
    /// to call more than once on the same cluster (e.g. to observe a
    /// hedging schedule's intermediate state before enough ticks have
    /// passed for a later-ranked proposer to activate, then let it play
    /// out further).
    pub fn advance(&mut self, more_ticks: u64) {
        let until = self.kernel.now().advance(more_ticks);
        self.kernel.run_until(until);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use queso_sim::scheduler::{ContentObliviousAdversary, Fifo};

    fn cluster(n: u32, seed: u64) -> ConcreteCluster<u32> {
        let initial: BTreeMap<NodeId, u32> = (0..n).map(|i| (NodeId(i), i)).collect();
        ConcreteCluster::new(
            seed,
            SchedulerKind::Oblivious(Box::new(Fifo::new(1))),
            initial,
        )
    }

    #[test]
    fn all_live_replicas_eventually_decide_under_fifo() {
        let mut c = cluster(5, 7);
        c.run_slot(20_000);
        assert!(
            c.all_live_decided(),
            "did not decide within the tick budget"
        );
    }

    #[test]
    fn all_replicas_decide_the_same_value() {
        let mut c = cluster(5, 7);
        c.run_slot(20_000);
        let mut decisions: BTreeSet<u32> = BTreeSet::new();
        for &id in c.replicas() {
            if let Some(v) = c.decided(id) {
                decisions.insert(v);
            }
        }
        assert_eq!(decisions.len(), 1, "replicas disagreed: {decisions:?}");
    }

    #[test]
    fn decided_value_was_some_replicas_initial_value() {
        let mut c = cluster(5, 7);
        c.run_slot(20_000);
        let v = c.decided(NodeId(0)).expect("should have decided");
        assert!((0..5).contains(&v), "decided value {v} was never proposed");
    }

    #[test]
    fn progresses_with_a_crashed_minority() {
        let mut c = cluster(5, 3);
        c.crash(NodeId(4)); // f=2 tolerated for n=5; only 1 crashed here
        c.run_slot(20_000);
        assert!(
            c.all_live_decided(),
            "did not decide within the tick budget"
        );
        assert!(
            c.decided(NodeId(4)).is_none(),
            "crashed replica should not decide"
        );
    }

    #[test]
    fn no_live_majority_does_not_panic_and_does_not_falsely_decide() {
        // n=5, crash 3 (more than f=2): no majority can ever be live. The
        // proposer's retry-then-give-up behavior (see `crate::proposer`'s
        // module docs) must mean this simply never decides -- no panic, no
        // phantom decision -- matching P11/O4.
        let mut c = cluster(5, 3);
        c.crash(NodeId(2));
        c.crash(NodeId(3));
        c.crash(NodeId(4));
        c.run_slot(20_000);
        for &id in c.live() {
            assert!(
                c.decided(id).is_none(),
                "replica {id} decided without a live majority ever being reachable"
            );
        }
    }

    #[test]
    fn survives_realistic_async_adversary() {
        let adversary = ContentObliviousAdversary::new(1, 6).with_drop_probability(0.25);
        let mut c = ConcreteCluster::new(
            99,
            SchedulerKind::Oblivious(Box::new(adversary)),
            (0..5u32).map(|i| (NodeId(i), i)).collect(),
        );
        c.run_slot(200_000);
        assert!(
            c.all_live_decided(),
            "did not decide within the tick budget under a lossy adversary"
        );
        let decisions: BTreeSet<u32> = c
            .replicas()
            .iter()
            .filter_map(|&id| c.decided(id))
            .collect();
        assert_eq!(decisions.len(), 1);
    }

    #[test]
    fn is_deterministic_given_same_seed() {
        let run = |seed: u64| {
            let mut c = cluster(5, seed);
            c.run_slot(20_000);
            let decisions: Vec<Option<u32>> =
                c.replicas().iter().map(|&id| c.decided(id)).collect();
            decisions
        };
        assert_eq!(run(42), run(42));
    }
}
