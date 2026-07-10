//! [`SmrCluster`]: the external driver for the multi-slot replicated log --
//! builds the replica set on top of a [`Kernel`], accepts client
//! `put`/`get` submissions, and lets tests run the simulation and inspect
//! results afterward. Mirrors `queso_consensus::concrete::ConcreteCluster`'s
//! shape (a thin driver reading state back out of `Rc<RefCell<_>>` handles
//! shared with the `Node` impls it registers), extended across many slots.
//!
//! # Linearizable reads-through-log, concretely
//!
//! Per the Meerkat design this stage implements (see the crate docs): a
//! `get` is *never* answered from local state. [`SmrCluster::submit`]
//! enqueues it on the target replica exactly like a `put`; the replica
//! proposes it at its own current log frontier via an ordinary
//! [`queso_consensus::proposer::Proposer`], unmodified from Phase 2/3. Two
//! outcomes are possible once that slot decides:
//!
//! - The `get` itself is what won the slot: its result is the [`Kv`] state
//!   after applying every slot before it (a `Get` never mutates, so
//!   "before it" and "up to and including it" are the same state) --
//!   exactly the value the caller receives.
//! - Something else won (a `put`, or even another client's `get`): the
//!   replica applies *that* decided command to its own local `Kv`
//!   (catching itself up on a decision it otherwise would have missed),
//!   advances its frontier by one slot, and re-proposes the *same* pending
//!   `get` at the new frontier -- linearizing it after whatever just beat
//!   it. This repeats until the `get` wins a slot or the caller gives up
//!   waiting.
//!
//! This is the entire mechanism (see `crate::replica::SmrNode::finish_attempt`
//! for the code) -- there is no special-cased "catch-up RPC" or learner
//! protocol; it falls out of running an ordinary [`Proposer`] at a slot that
//! may already be (partially or fully) decided and trusting its existing
//! majority-intersection safety argument, which was never restricted to
//! proposers present from a slot's very first step.
//!
//! # Stage 4b seam (durability / restart, P12)
//!
//! Nothing here is durable. A crashed replica ([`SmrCluster::crash`]) simply
//! stops responding (crash-*stop*, matching this stage's scope note) --
//! there is no restart/rejoin path exercised or supported. The design seam
//! for Stage 4b is exactly the state [`crate::replica::ReplicaState`]
//! already isolates as "everything this replica knows": its per-slot
//! [`Recorder`]s' ISR state (`S, F_c, A_c, A_p` -- already `O(1)` per slot,
//! D5), `next_slot`, and `applied_log`. A durable implementation would
//! write-before-reply each `Recorder::handle` response and each `Kv::apply`
//! effect, and a restarted replica would either recover that durable state
//! or explicitly rejoin as a learner and catch up (via exactly the same
//! catch-up-via-fresh-`Proposer` mechanism described above) before
//! participating again -- `queso_sim::node::Node::on_restart` is the hook,
//! deliberately left at its no-op default here, exactly as
//! `queso_consensus::concrete::ReplicaNode` documents for the same reason.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use queso_consensus::proposer::KICKOFF_TIMER;
use queso_consensus::rpc::ConcreteMsg;
use queso_sim::ids::NodeId;
use queso_sim::scheduler::SchedulerKind;
use queso_sim::time::LogicalTime;
use queso_sim::trace::Trace;
use queso_sim::Kernel;

use crate::command::{ClientId, Command, Key, Value};
use crate::replica::{OpId, OpRecord, QueuedOp, ReplicaState, SmrNode};

/// Drives the multi-slot replicated log + KV application across a fixed set
/// of replicas, on top of one [`Kernel`]. `V` is fixed to [`Command`] (the
/// wire payload is exactly `queso_consensus`'s own
/// [`ConcreteMsg<Command>`](ConcreteMsg) -- see `crate::replica`'s module
/// docs for why no additional wrapper type is needed).
pub struct SmrCluster {
    kernel: Kernel<ConcreteMsg<Command>>,
    replicas: Vec<NodeId>,
    states: BTreeMap<NodeId, Rc<RefCell<ReplicaState>>>,
    results: Rc<RefCell<BTreeMap<OpId, OpRecord>>>,
    leader: Option<NodeId>,
    live: BTreeSet<NodeId>,
    next_op_id: u64,
}

impl SmrCluster {
    /// Build a purely leaderless cluster of `n` replicas (`NodeId(0)` ..
    /// `NodeId(n-1)`), all initially live.
    pub fn new(seed: u64, scheduler: SchedulerKind<ConcreteMsg<Command>>, n: usize) -> Self {
        Self::new_with_leader(seed, scheduler, n, None)
    }

    /// Build a cluster of `n` replicas with `leader` designated as every
    /// slot's fast-path leader (§4.2.5, D1), or `None` for the purely
    /// leaderless case. The *same* `leader` value is used for every slot,
    /// mirroring `ConcreteCluster::new_with_leader`'s single-slot
    /// convention -- a fixed steady-state leader, not per-slot rotation
    /// (auto-tuned leader rotation is Phase 6).
    pub fn new_with_leader(
        seed: u64,
        scheduler: SchedulerKind<ConcreteMsg<Command>>,
        n: usize,
        leader: Option<NodeId>,
    ) -> Self {
        let mut kernel = Kernel::new(seed, scheduler);
        kernel.set_leader(leader);
        let results = Rc::new(RefCell::new(BTreeMap::new()));
        let mut states = BTreeMap::new();
        let mut replicas = Vec::new();

        for i in 0..n as u32 {
            let id = NodeId(i);
            let state = Rc::new(RefCell::new(ReplicaState::default()));
            let node = SmrNode {
                state: state.clone(),
                results: results.clone(),
                total_replicas: n,
                leader,
            };
            kernel.add_node(id, Box::new(node));
            states.insert(id, state);
            replicas.push(id);
        }
        replicas.sort();
        let live = replicas.iter().copied().collect();

        Self {
            kernel,
            replicas,
            states,
            results,
            leader,
            live,
            next_op_id: 0,
        }
    }

    /// The full, static replica membership.
    pub fn replicas(&self) -> &[NodeId] {
        &self.replicas
    }

    /// The cluster's designated fast-path leader, if any.
    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }

    /// Replicas the driver currently considers live (not crashed).
    pub fn live(&self) -> &BTreeSet<NodeId> {
        &self.live
    }

    /// Crash a replica (crash-*stop* -- see the module docs' Stage 4b seam
    /// note). Any operation currently in flight through it will simply
    /// never complete (a liveness cost, per P11/O4 -- never a safety one).
    pub fn crash(&mut self, id: NodeId) {
        self.kernel.crash(id);
        self.live.remove(&id);
    }

    /// Submit `command` to `replica`: enqueue it, and -- if the replica was
    /// idle -- kick off its first attempt. Returns an [`OpId`] the caller
    /// uses to poll [`SmrCluster::result`] after running the simulation.
    ///
    /// Kicking off idle work is done via a zero-delay `KICKOFF_TIMER`
    /// injection rather than driving the `Proposer` directly here, because
    /// `submit` runs *outside* any `Node` callback -- there is no
    /// `NodeCtx` available to it, only [`Kernel::inject_timer`]. Once that
    /// timer fires (one logical tick later), everything from there on
    /// (including every subsequent slot this operation might need to be
    /// re-proposed at) proceeds purely through `Node` callbacks, exactly
    /// like `queso_consensus::concrete::ConcreteCluster::run_slot`.
    pub fn submit(&mut self, replica: NodeId, command: Command) -> OpId {
        let op_id = OpId(self.next_op_id);
        self.next_op_id += 1;
        let invoked_at = self.kernel.now();
        self.results.borrow_mut().insert(
            op_id,
            OpRecord {
                replica,
                command: command.clone(),
                invoked_at,
                completed_at: None,
                outcome: None,
                decided_slot: None,
            },
        );

        let state = self.states[&replica].clone();
        let should_kick = {
            let mut st = state.borrow_mut();
            st.queue.push_back(QueuedOp { op_id, command });
            st.queue.len() == 1
        };
        // `queue.len() == 1` (just after pushing) is exactly "this replica
        // had nothing queued or in flight a moment ago": if it already had
        // an attempt running, `finish_attempt` will drain the queue itself
        // once that attempt resolves; a second `submit` arriving before the
        // first kickoff timer even fires only grows the queue past length 1
        // without needing a second timer.
        if should_kick {
            self.kernel.inject_timer(replica, 0, KICKOFF_TIMER);
        }
        op_id
    }

    /// Convenience: submit a `Put`.
    pub fn submit_put(
        &mut self,
        replica: NodeId,
        client: ClientId,
        seq: u64,
        key: Key,
        value: Value,
    ) -> OpId {
        self.submit(
            replica,
            Command::Put {
                client,
                seq,
                key,
                value,
            },
        )
    }

    /// Convenience: submit a `Get`.
    pub fn submit_get(&mut self, replica: NodeId, client: ClientId, seq: u64, key: Key) -> OpId {
        self.submit(replica, Command::Get { client, seq, key })
    }

    /// Run the kernel forward `ticks` logical ticks from wherever it
    /// currently is.
    pub fn run_for(&mut self, ticks: u64) {
        let until = self.kernel.now().advance(ticks);
        self.kernel.run_until(until);
    }

    /// The current logical time.
    pub fn now(&self) -> LogicalTime {
        self.kernel.now()
    }

    /// `op`'s record, if it has ever been submitted.
    pub fn result(&self, op: OpId) -> Option<OpRecord> {
        self.results.borrow().get(&op).cloned()
    }

    /// Whether `op` has completed (decided and applied).
    pub fn is_complete(&self, op: OpId) -> bool {
        self.results
            .borrow()
            .get(&op)
            .is_some_and(|r| r.completed_at.is_some())
    }

    /// All operation records submitted so far, keyed by [`OpId`] -- the raw
    /// material [`crate::linearizability::history_from_records`] turns into
    /// a checkable history.
    pub fn results(&self) -> BTreeMap<OpId, OpRecord> {
        self.results.borrow().clone()
    }

    /// `replica`'s current KV snapshot (its local application state after
    /// everything it has applied so far -- may lag the true log length; see
    /// the module docs).
    pub fn kv_snapshot(&self, replica: NodeId) -> BTreeMap<Key, Value> {
        self.states[&replica].borrow().kv.snapshot()
    }

    /// `replica`'s exact applied-log prefix so far, `applied_log()[i]` being
    /// slot `i`'s decided command. Used by log-safety tests to assert P5
    /// (prefix consistency across replicas) and P6 (total order).
    pub fn applied_log(&self, replica: NodeId) -> Vec<Command> {
        self.states[&replica].borrow().applied_log.clone()
    }

    /// `replica`'s current frontier (first not-yet-applied slot index).
    /// Always equals `applied_log(replica).len()` -- gap-free application
    /// (P7) is structural here, not merely tested for: a replica only ever
    /// pushes onto `applied_log` one slot at a time, in order (see
    /// `crate::replica::SmrNode::finish_attempt`).
    pub fn next_slot(&self, replica: NodeId) -> u64 {
        self.states[&replica].borrow().next_slot
    }

    /// The kernel's recorded trace so far (for determinism/reproducibility
    /// checks, D9).
    pub fn trace(&self) -> &Trace {
        self.kernel.trace()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replica::Outcome;
    use queso_sim::scheduler::{ContentObliviousAdversary, Fifo};

    fn fifo_cluster(n: usize, seed: u64) -> SmrCluster {
        SmrCluster::new(seed, SchedulerKind::Oblivious(Box::new(Fifo::new(1))), n)
    }

    #[test]
    fn put_then_get_on_the_same_replica_sees_the_write() {
        let mut c = fifo_cluster(3, 1);
        let r = NodeId(0);
        let put = c.submit_put(r, ClientId(1), 0, 10, 100);
        c.run_for(50_000);
        assert!(c.is_complete(put), "put did not complete");

        let get = c.submit_get(r, ClientId(2), 0, 10);
        c.run_for(50_000);
        let result = c.result(get).expect("submitted");
        assert_eq!(result.outcome, Some(Outcome::Get(Some(100))));
    }

    #[test]
    fn read_after_write_on_a_different_replica_still_sees_it() {
        // The write is fully decided on replica 0's own view of the log
        // before the read is even submitted, but the read goes to replica
        // 1, which never participated in that slot -- it must catch up
        // (reads-through-log) rather than serve a stale answer (P10).
        let mut c = fifo_cluster(3, 2);
        let put = c.submit_put(NodeId(0), ClientId(1), 0, 42, 7);
        c.run_for(50_000);
        assert!(c.is_complete(put));
        assert_eq!(
            c.next_slot(NodeId(1)),
            0,
            "replica 1 has not touched slot 0 yet"
        );

        let get = c.submit_get(NodeId(1), ClientId(2), 0, 42);
        c.run_for(50_000);
        let result = c.result(get).expect("submitted");
        assert_eq!(result.outcome, Some(Outcome::Get(Some(7))));
    }

    #[test]
    fn a_losing_read_is_reproposed_at_the_next_slot() {
        // Force the read to lose slot 0 to a concurrent write by submitting
        // both before either has a chance to be decided, then confirm the
        // read still completes correctly, one or more slots later.
        let mut c = fifo_cluster(3, 3);
        let put = c.submit_put(NodeId(0), ClientId(1), 0, 1, 999);
        let get = c.submit_get(NodeId(1), ClientId(2), 0, 1);
        c.run_for(100_000);

        assert!(c.is_complete(put));
        let get_result = c.result(get).expect("submitted");
        assert_eq!(get_result.outcome, Some(Outcome::Get(Some(999))));
        // The read can only have observed the write if it was decided at a
        // slot at or after the write's slot.
        let put_slot = c.result(put).unwrap().decided_slot.unwrap();
        let get_slot = get_result.decided_slot.unwrap();
        assert!(get_slot >= put_slot);
    }

    #[test]
    fn progresses_with_a_crashed_minority() {
        let mut c = fifo_cluster(5, 4);
        c.crash(NodeId(4));
        let put = c.submit_put(NodeId(0), ClientId(1), 0, 1, 1);
        c.run_for(50_000);
        assert!(c.is_complete(put));
    }

    #[test]
    fn no_progress_without_a_live_majority() {
        let mut c = fifo_cluster(5, 5);
        c.crash(NodeId(2));
        c.crash(NodeId(3));
        c.crash(NodeId(4));
        let put = c.submit_put(NodeId(0), ClientId(1), 0, 1, 1);
        c.run_for(50_000);
        assert!(
            !c.is_complete(put),
            "must not decide without a live majority"
        );
    }

    #[test]
    fn survives_a_realistic_async_adversary() {
        let adversary = ContentObliviousAdversary::new(1, 6).with_drop_probability(0.2);
        let mut c = SmrCluster::new(77, SchedulerKind::Oblivious(Box::new(adversary)), 5);
        let put = c.submit_put(NodeId(0), ClientId(1), 0, 1, 5);
        c.run_for(500_000);
        assert!(c.is_complete(put));
        let get = c.submit_get(NodeId(3), ClientId(2), 0, 1);
        c.run_for(500_000);
        assert_eq!(c.result(get).unwrap().outcome, Some(Outcome::Get(Some(5))));
    }

    #[test]
    fn is_deterministic_given_the_same_seed() {
        let run = |seed: u64| {
            let mut c = fifo_cluster(3, seed);
            let put = c.submit_put(NodeId(0), ClientId(1), 0, 1, 1);
            let get = c.submit_get(NodeId(1), ClientId(2), 0, 1);
            c.run_for(50_000);
            (
                c.result(put).unwrap().outcome,
                c.result(get).unwrap().outcome,
            )
        };
        assert_eq!(run(9), run(9));
    }
}
