//! The Phase-2 driver: runs `N` replicas, each hosting one
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

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::rc::Rc;

use queso_sim::ids::{NodeId, TimerId};
use queso_sim::node::{Node, NodeCtx};
use queso_sim::scheduler::SchedulerKind;
use queso_sim::trace::Trace;
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
}

impl<V> ReplicaNode<V> {
    fn new(proposer: Rc<RefCell<Proposer<V>>>, recorder: Rc<RefCell<Recorder<V>>>) -> Self {
        Self { proposer, recorder }
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
                // Passive recorder role: answer the RPC, nothing else.
                let resp = self.recorder.borrow_mut().handle(req);
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
    /// Driver-tracked liveness, mirroring `crate::algorithm::Cluster`'s
    /// rationale: `Kernel` doesn't expose fault state publicly, and
    /// "currently live" here is purely a bookkeeping convenience for
    /// deciding which replicas to kick off -- quorum math itself is always
    /// against the *full* replica count (see `crate::proposer`'s module
    /// docs on why).
    live: BTreeSet<NodeId>,
}

impl<V: Ord + Clone + Debug + 'static> ConcreteCluster<V> {
    /// Build a cluster with one replica per `(NodeId, initial value)` pair,
    /// all initially live.
    pub fn new(
        seed: u64,
        scheduler: SchedulerKind<ConcreteMsg<V>>,
        initial_values: BTreeMap<NodeId, V>,
    ) -> Self {
        let n = initial_values.len();
        let mut kernel = Kernel::new(seed, scheduler);
        let mut proposers = BTreeMap::new();
        let mut replicas = Vec::new();

        for (id, v) in initial_values {
            let proposer = Rc::new(RefCell::new(Proposer::new(id, n, v)));
            let recorder = Rc::new(RefCell::new(Recorder::new()));
            kernel.add_node(id, Box::new(ReplicaNode::new(proposer.clone(), recorder)));
            proposers.insert(id, proposer);
            replicas.push(id);
        }
        replicas.sort();
        let live = replicas.iter().copied().collect();

        Self {
            kernel,
            replicas,
            proposers,
            live,
        }
    }

    /// The full, static replica membership.
    pub fn replicas(&self) -> &[NodeId] {
        &self.replicas
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
