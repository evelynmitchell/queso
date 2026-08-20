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
//! # Durability / restart (Stage 4b, P9/P12)
//!
//! A crashed replica ([`SmrCluster::crash`]) stops responding entirely until
//! [`SmrCluster::restart`] brings it back. Restarting recovers exactly the
//! *durable* half of [`crate::replica::ReplicaState`] -- its per-slot
//! [`Recorder`]s' ISR state (`S, F_c, A_c, A_p`, already `O(1)` per slot,
//! D5), `next_slot`, `applied_log`, and `kv` (see
//! [`crate::replica::Durable`]'s docs) -- while its volatile half (pending
//! ops, any in-flight proposer) is dropped, and then rejoins as a learner:
//! [`crate::replica::SmrNode::on_restart`] drives an internal catch-up probe
//! through the exact same reads-through-log mechanism described above
//! before the replica resumes ordinary participation. See that type's docs
//! for the full recovery sequence and for how the write-before-reply
//! ordering (P12) is enforced on the recorder side.

use std::cell::{Cell, RefCell};
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
use crate::replica::{LeaderPolicy, OpId, OpRecord, QueuedOp, ReplicaState, SmrNode};
use crate::tuning::EpochTuner;

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
    /// The cluster's *fixed* leader, if built via [`Self::new`]/
    /// [`Self::new_with_leader`] -- `None` both for the purely leaderless
    /// case and for a [`Self::new_with_tuning`] cluster (whose leader is
    /// chosen dynamically; see [`Self::current_leader`] for the value that
    /// covers both cases uniformly).
    leader: Option<NodeId>,
    /// Present only for a [`Self::new_with_tuning`] cluster (Phase 6, D4) --
    /// the shared explore/exploit tuner every replica's [`SmrNode`]
    /// consults. See `crate::tuning`'s module docs.
    tuner: Option<Rc<RefCell<EpochTuner>>>,
    live: BTreeSet<NodeId>,
    next_op_id: u64,
    /// What the kernel was last told the leader is, so
    /// [`Self::sync_kernel_leader`] can tell a real switch from a no-op and
    /// avoid recording a `LeaderChanged` trace event per tick.
    kernel_leader: Option<NodeId>,
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
        Self::build(
            seed,
            scheduler,
            n,
            LeaderPolicy::Fixed(leader),
            leader,
            None,
        )
    }

    /// Build a cluster of `n` replicas whose leader and hedging schedule are
    /// **auto-tuned** (Phase 6, §5.3, D4) instead of fixed: an
    /// [`EpochTuner`] shared by every replica groups the log into
    /// `epoch_len`-slot epochs, round-robins the leader across the first
    /// `2n+1` epochs while measuring each leader's observed average epoch
    /// completion time, then exploits by leading with the fastest-observed
    /// replica and staggering the rest by `base_delay` (Phase 5's δ) in
    /// speed order -- switching leaders thereafter only if the incumbent
    /// measurably degrades relative to the next-ranked replica, never
    /// requiring a crash. See `crate::tuning`'s module docs for the full
    /// design and [`Self::current_leader`]/[`Self::tuning_epoch`]/
    /// [`Self::tuning_schedule`]/[`Self::tuning_is_exploring`]/
    /// [`Self::tuning_switch_count`]/[`Self::tuning_leader_log`]/
    /// [`Self::tuning_average`] for the introspection this driver exposes on
    /// top of it.
    pub fn new_with_tuning(
        seed: u64,
        scheduler: SchedulerKind<ConcreteMsg<Command>>,
        n: usize,
        epoch_len: u64,
        base_delay: u64,
    ) -> Self {
        let replica_ids: Vec<NodeId> = (0..n as u32).map(NodeId).collect();
        let tuner = Rc::new(RefCell::new(EpochTuner::new(
            replica_ids,
            epoch_len,
            base_delay,
        )));
        let initial_leader = Some(tuner.borrow().leader());
        Self::build(
            seed,
            scheduler,
            n,
            LeaderPolicy::Tuned(tuner.clone()),
            initial_leader,
            Some(tuner),
        )
    }

    fn build(
        seed: u64,
        scheduler: SchedulerKind<ConcreteMsg<Command>>,
        n: usize,
        policy: LeaderPolicy,
        kernel_leader_hint: Option<NodeId>,
        tuner: Option<Rc<RefCell<EpochTuner>>>,
    ) -> Self {
        let mut kernel = Kernel::new(seed, scheduler);
        // Purely a hook for adversary schedulers that target "the leader"
        // (see `Kernel::set_leader`'s docs). For a fixed-leader cluster it
        // is set once and never changes; for `LeaderPolicy::Tuned` it is
        // kept in step with the tuner by [`Self::sync_kernel_leader`], so a
        // leader-targeting adversary follows the leader across epoch
        // switches instead of aiming at whoever led epoch 0 (issue #29).
        kernel.set_leader(kernel_leader_hint);
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
                leader_policy: policy.clone(),
                local_step: Rc::new(Cell::new(0)),
            };
            kernel.add_node(id, Box::new(node));
            states.insert(id, state);
            replicas.push(id);
        }
        replicas.sort();
        let live = replicas.iter().copied().collect();
        let leader = match &policy {
            LeaderPolicy::Fixed(l) => *l,
            LeaderPolicy::Tuned(_) => None,
        };

        Self {
            kernel,
            replicas,
            states,
            results,
            leader,
            tuner,
            live,
            next_op_id: 0,
            kernel_leader: kernel_leader_hint,
        }
    }

    /// The full, static replica membership.
    pub fn replicas(&self) -> &[NodeId] {
        &self.replicas
    }

    /// The cluster's **fixed** designated leader, if any -- `None` for both
    /// the purely leaderless case and for a [`Self::new_with_tuning`]
    /// cluster (whose leader changes over time; see [`Self::current_leader`]
    /// for the accessor that covers both cases).
    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }

    /// The replica currently acting as fast-path leader, whether fixed (see
    /// [`Self::leader`]) or auto-tuned (Phase 6, D4) -- the current epoch's
    /// leader for a [`Self::new_with_tuning`] cluster, `None` only for the
    /// purely leaderless case.
    pub fn current_leader(&self) -> Option<NodeId> {
        self.leader
            .or_else(|| self.tuner.as_ref().map(|t| t.borrow().leader()))
    }

    /// The current epoch number of a [`Self::new_with_tuning`] cluster's
    /// tuner, or `None` if this cluster was not built with tuning.
    pub fn tuning_epoch(&self) -> Option<u64> {
        self.tuner.as_ref().map(|t| t.borrow().epoch())
    }

    /// How many epochs a [`Self::new_with_tuning`] cluster's tuner spends
    /// exploring (`2n+1`), or `None` if this cluster was not built with
    /// tuning.
    pub fn tuning_explore_epochs(&self) -> Option<u64> {
        self.tuner.as_ref().map(|t| t.borrow().explore_epochs())
    }

    /// Whether a [`Self::new_with_tuning`] cluster's tuner is still in its
    /// initial round-robin exploration phase, or `None` if this cluster was
    /// not built with tuning.
    pub fn tuning_is_exploring(&self) -> Option<bool> {
        self.tuner.as_ref().map(|t| t.borrow().is_exploring())
    }

    /// The current epoch's full hedging schedule (leader first, the rest in
    /// ascending speed order once exploration has finished), or `None` if
    /// this cluster was not built with tuning.
    pub fn tuning_schedule(&self) -> Option<Vec<NodeId>> {
        self.tuner.as_ref().map(|t| t.borrow().schedule().to_vec())
    }

    /// How many times the tuner has switched leaders away from a degraded
    /// incumbent during the exploit phase (§5.3's monitoring trigger), or
    /// `None` if this cluster was not built with tuning.
    pub fn tuning_switch_count(&self) -> Option<u64> {
        self.tuner.as_ref().map(|t| t.borrow().switch_count())
    }

    /// The leader pinned to `slot`'s epoch, or `None` for a cluster that is
    /// not tuned.
    ///
    /// The value a proposer for `slot` will use, whenever it runs. That
    /// "whenever" is the whole point: a replica catching up after a restart
    /// proposes for slots whose epoch closed long ago, and it must
    /// reconstruct the *same* leader those slots always had. If this ever
    /// returned the tuner's current leader instead of the pinned one, two
    /// replicas proposing for the same old slot would disagree about who
    /// leads it. Exposed so
    /// `tests/tuning.rs::tuning_survives_a_crash_and_restart_with_epoch_leaders_pinned`
    /// can check that directly rather than inferring it (issue #29).
    pub fn tuning_leader_for_slot(&self, slot: u64) -> Option<NodeId> {
        self.tuner
            .as_ref()
            .map(|tuner| tuner.borrow().leader_for_slot(slot))
    }

    /// The leader assigned to every epoch so far, in epoch order, or `None`
    /// if this cluster was not built with tuning.
    pub fn tuning_leader_log(&self) -> Option<Vec<NodeId>> {
        self.tuner
            .as_ref()
            .map(|t| t.borrow().leader_log().to_vec())
    }

    /// `replica`'s observed average epoch-completion time (as leader), or
    /// `None` if this cluster was not built with tuning or that replica has
    /// not yet led a measured epoch.
    pub fn tuning_average(&self, replica: NodeId) -> Option<u64> {
        self.tuner
            .as_ref()
            .and_then(|t| t.borrow().average_for(replica))
    }

    /// Replicas the driver currently considers live (not crashed).
    pub fn live(&self) -> &BTreeSet<NodeId> {
        &self.live
    }

    /// Multiply message delay to/from `id` by `multiplier` (>= 1) -- test/
    /// demo fault injection (`queso_sim::fault`'s slow-node facility),
    /// wrapped here the same way [`Self::crash`]/[`Self::restart`] wrap
    /// their own `Kernel` calls.
    pub fn set_slow(&mut self, id: NodeId, multiplier: u64) {
        self.kernel.set_slow(id, multiplier);
    }

    /// Remove a previously-set slow-node multiplier for `id`.
    pub fn clear_slow(&mut self, id: NodeId) {
        self.kernel.clear_slow(id);
    }

    /// Crash a replica. Any operation currently in flight through it will
    /// simply never complete from this attempt (a liveness cost, per
    /// P11/O4 -- never a safety one); a client retry (same `(client, seq)`,
    /// to this replica once restarted or to another) is safe via P8a
    /// dedup. See the module docs' "Durability / restart" section --
    /// [`SmrCluster::restart`] is the crash-*recovery* counterpart.
    pub fn crash(&mut self, id: NodeId) {
        self.kernel.crash(id);
        self.live.remove(&id);
    }

    /// Restart a crashed replica (P12): its durable state (recorders' ISR,
    /// log frontier, applied log, `kv`) is recovered untouched, its
    /// volatile state is dropped, and it rejoins as a learner, catching up
    /// on anything decided while it was down before resuming ordinary
    /// participation -- see [`crate::replica::SmrNode::on_restart`] for the
    /// exact sequence. Marks `id` live again immediately (matching
    /// `crash`'s bookkeeping), even though it may still be mid-catch-up
    /// under the hood; catch-up is safe to overlap with real traffic
    /// (P10 holds throughout, not just once catch-up finishes -- see that
    /// type's docs).
    pub fn restart(&mut self, id: NodeId) {
        self.kernel.restart(id);
        self.live.insert(id);
    }

    /// A faithful invocation timestamp for a *new* submission: at least
    /// [`Kernel::now`], but strictly after every operation that has already
    /// completed by this point in the driver's program order.
    ///
    /// This exists to close a real soundness hole in
    /// [`crate::linearizability`]: `Kernel::now()` does not advance just
    /// because `submit` is called outside of any dispatched event, so a
    /// driver sequence like `submit(a); run_for(n); submit(b)` -- where `b`
    /// is only submitted *after* `a` has been observed to complete -- can
    /// otherwise produce `b.invoked_at == a.completed_at`. The checker's
    /// real-time precedence relation is a strict `<`, so that tie would
    /// wrongly let it treat `a` and `b` as concurrent, potentially
    /// *accepting* a history it should reject (e.g. `b` observing a value
    /// from before `a`'s write, even though `a` had already completed when
    /// `b` was invoked).
    ///
    /// The fix is deliberately in the driver, not the checker or the
    /// kernel's clock: every op still submitted *before* any completion
    /// (genuinely concurrent submissions) gets `invoked_at == kernel.now()`
    /// exactly as before, so overlapping intervals -- and the checker's
    /// freedom to order them either way -- are unaffected. Only a
    /// submission that the driver issues *after* some completion is pushed
    /// strictly past that completion's timestamp.
    fn next_invoked_at(&self) -> LogicalTime {
        let now = self.kernel.now();
        let max_completed = self
            .results
            .borrow()
            .values()
            .filter_map(|r| r.completed_at)
            .max();
        match max_completed {
            Some(t) => std::cmp::max(now, t.advance(1)),
            None => now,
        }
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
        debug_assert_ne!(
            command.client_seq().0,
            crate::replica::CATCH_UP_CLIENT,
            "ClientId(u32::MAX) is reserved for this crate's internal restart catch-up probes \
             (see crate::replica::CATCH_UP_CLIENT's docs) -- a real client/test must never \
             submit using it"
        );
        let op_id = OpId(self.next_op_id);
        self.next_op_id += 1;
        let invoked_at = self.next_invoked_at();
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
    ///
    /// Under [`LeaderPolicy::Tuned`] this advances a tick at a time so the
    /// kernel's leader hint can be re-synced from the tuner as epochs turn
    /// -- see [`Self::sync_kernel_leader`]. Splitting one `run_until(t)`
    /// into successive `run_until` calls over the same range dispatches
    /// exactly the same events in exactly the same order (the queue is
    /// drained in `(time, seq)` order either way), so this changes nothing
    /// about what runs; it only creates points at which the hint can be
    /// updated. That equivalence is asserted rather than assumed --
    /// `queso-sim`'s `reproducibility::draining_the_queue_in_slices_matches_one_run`
    /// compares a sliced drain against one `run()` byte for byte under every
    /// scheduler. Fixed-leader clusters take the single-call path unchanged,
    /// since their hint never moves.
    ///
    /// Note that `start` is captured once. `run_until` takes an *absolute*
    /// instant and `now` only moves when an event is dispatched, so
    /// re-reading `now` each step would stall the moment the next event sat
    /// further out than one tick.
    pub fn run_for(&mut self, ticks: u64) {
        let target = self.kernel.now().advance(ticks);
        if self.tuner.is_none() {
            self.kernel.run_until(target);
            return;
        }
        let start = self.kernel.now();
        // From 0, not 1: `run_for(0)` must still dispatch whatever is
        // already due at the current instant, exactly as the single-call
        // path does. Starting at 1 would silently make it a no-op for tuned
        // clusters only.
        for tick in 0..=ticks {
            self.sync_kernel_leader();
            self.kernel.run_until(start.advance(tick));
        }
        self.sync_kernel_leader();
    }

    /// Point the kernel's leader hint at the tuner's current leader, if it
    /// has moved.
    ///
    /// # Why this exists
    ///
    /// `Kernel::set_leader` is the hook adversary schedulers use to decide
    /// who to target (`ContentObliviousAdversary::with_leader_dos` reads it
    /// fresh on every decision). Before this, a tuned cluster set it once,
    /// to epoch 0's leader, and never again -- so a leader-targeting
    /// adversary under auto-tuning would spend the whole run attacking a
    /// replica that had stopped being leader, and the test asserting it
    /// "stresses the leader" would be quietly asserting much less than it
    /// claimed. That is issue #29's middle item, and it is the same shape of
    /// problem as a vacuous fault test: green, and testing less than
    /// advertised.
    ///
    /// Only called when the value actually changes, because
    /// `Kernel::set_leader` records a `LeaderChanged` trace event and a
    /// per-tick stream of no-op events would swamp the trace.
    ///
    /// **Granularity, honestly:** the hint is refreshed between ticks, not
    /// between individual events. Several events can share a tick, so a
    /// switch caused by one of them is visible to the adversary from the
    /// next tick onward rather than immediately. Epochs span many ticks, so
    /// this is far finer than the thing being tracked -- but it is a
    /// residual, not exactness, and closing it properly would mean the
    /// kernel reading the leader through a shared cell rather than being
    /// told.
    fn sync_kernel_leader(&mut self) {
        let Some(tuner) = &self.tuner else { return };
        let current = Some(tuner.borrow().leader());
        if current != self.kernel_leader {
            self.kernel_leader = current;
            self.kernel.set_leader(current);
        }
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
    /// the module docs). Durable (survives a crash + restart of `replica`,
    /// see [`crate::replica::Durable`]).
    pub fn kv_snapshot(&self, replica: NodeId) -> BTreeMap<Key, Value> {
        self.states[&replica].borrow().durable.kv.snapshot()
    }

    /// `replica`'s exact applied-log prefix so far, `applied_log()[i]` being
    /// slot `i`'s decided command. Used by log-safety tests to assert P5
    /// (prefix consistency across replicas) and P6 (total order). Durable
    /// (survives a crash + restart of `replica`).
    pub fn applied_log(&self, replica: NodeId) -> Vec<Command> {
        self.states[&replica].borrow().durable.applied_log.clone()
    }

    /// `replica`'s current frontier (first not-yet-applied slot index).
    /// Always equals `applied_log(replica).len()` -- gap-free application
    /// (P7) is structural here, not merely tested for: a replica only ever
    /// pushes onto `applied_log` one slot at a time, in order (see
    /// `crate::replica::SmrNode::finish_attempt`). Durable (survives a
    /// crash + restart of `replica`).
    pub fn next_slot(&self, replica: NodeId) -> u64 {
        self.states[&replica].borrow().durable.next_slot
    }

    /// `replica`'s recorder state for `slot` (the ISR's `(S, F_c, A_p)`
    /// summary), if that recorder has ever been touched. Test/introspection
    /// only -- used to demonstrate write-before-reply (P12) durability
    /// directly: a recorder's ISR state, once it has answered a `record`
    /// RPC, must be observable here unchanged after a crash + restart of
    /// `replica` (see `crate::replica::Durable`'s docs and
    /// `tests/restart_recovery.rs`).
    pub fn recorder_summary(
        &self,
        replica: NodeId,
        slot: u64,
    ) -> Option<queso_consensus::isr::IsrSummary<Command>> {
        self.states[&replica]
            .borrow()
            .durable
            .recorders
            .get(&slot)
            .map(|r| r.peek())
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
