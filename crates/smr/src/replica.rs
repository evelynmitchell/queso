//! One replica's slice of the multi-slot log: the [`queso_sim::node::Node`]
//! implementation, its persistent per-slot [`Recorder`]s, its own
//! sequential frontier into the log, and the queue of client operations it
//! is working through.
//!
//! # How the multi-slot log drives per-slot consensus
//!
//! There is no pipelining (Stage 4a scope; see the crate docs): a replica
//! runs **at most one** [`Proposer`] at a time, always targeting its own
//! `next_slot` -- the first slot index this replica has not yet applied to
//! its local [`Kv`]. When that attempt decides (whichever command wins --
//! not necessarily this replica's own), the replica applies the decided
//! command, advances `next_slot` by exactly one, and either completes its
//! own pending operation (if it was the one decided) or immediately starts
//! a fresh attempt for the *same* pending operation at the new frontier.
//! This is exactly the "reads-through-log, catch-up-and-repropose"
//! mechanism -- see `crate::cluster`'s module docs for the full write-up
//! and why reusing [`Proposer`] completely unmodified is what makes it
//! safe.
//!
//! Different replicas' `next_slot` values can and do diverge (a replica
//! that has nothing to propose simply never advances past whatever it last
//! touched) -- this is precisely the "a replica may lag but must never
//! record a different entry" contract of P5. Multiple replicas' proposers
//! *can* legitimately be contesting the same slot concurrently (e.g. two
//! different clients' writes racing for the same slot at two different
//! replicas, or a lagging reader's `Get` racing a fresher `Put`); the
//! per-slot [`Recorder`] state persists for as long as the run does,
//! addressed by `(replica, slot)`, so a late-arriving proposer for a slot
//! whose consensus has already progressed (or even finished) simply catches
//! up via [`Proposer`]'s own existing majority-intersection machinery,
//! completely unmodified from Phase 2/3.

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use queso_consensus::proposer::{Proposer, KICKOFF_TIMER};
use queso_consensus::recorder::Recorder;
use queso_consensus::rpc::ConcreteMsg;
use queso_sim::ids::{NodeId, TimerId};
use queso_sim::node::{Node, NodeCtx};
use queso_sim::time::LogicalTime;

use crate::command::{Command, Value};
use crate::kv::Kv;

/// Identifies one client-visible operation submitted via
/// [`crate::cluster::SmrCluster::submit`], distinct from `(client, seq)`
/// (A6): two *separate* submissions of the identical `(client, seq)`
/// command -- e.g. a client retrying to a different replica -- get two
/// distinct [`OpId`]s but dedupe to a single effect at the [`Kv`] layer
/// (P8a). `OpId` is purely a driver/test bookkeeping handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OpId(pub u64);

/// What a completed operation returned to its caller.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The write was decided (and, unless it was itself a duplicate, took
    /// effect) somewhere in the log.
    Put,
    /// The read's value, as observed by applying the log up to and
    /// including (but for a `Get`, that "including" is a no-op -- see
    /// [`crate::kv::Applied`]) the slot it was decided at.
    Get(Option<Value>),
}

/// The full record of one submitted operation: when it was invoked, and --
/// once decided -- when it completed and what it returned. This is exactly
/// the shape [`crate::linearizability`]'s history needs (invocation/
/// response logical times plus the observed value).
#[derive(Debug, Clone)]
pub struct OpRecord {
    pub replica: NodeId,
    pub command: Command,
    pub invoked_at: LogicalTime,
    pub completed_at: Option<LogicalTime>,
    pub outcome: Option<Outcome>,
    /// Which slot this operation was ultimately decided at (`None` until
    /// completion). Exposed for debugging/introspection, not required for
    /// correctness.
    pub decided_slot: Option<u64>,
}

/// An operation waiting for its turn to be this replica's active attempt.
pub(crate) struct QueuedOp {
    pub(crate) op_id: OpId,
    pub(crate) command: Command,
}

/// The one [`Proposer`] this replica currently has in flight, if any.
struct CurrentAttempt {
    op_id: OpId,
    command: Command,
    slot: u64,
    proposer: Proposer<Command>,
}

/// One replica's durable-in-spirit (see the crate docs' "Stage 4b seam"
/// note -- nothing here is actually persisted yet) log/application state.
/// Fields are `pub(crate)` rather than fully private: [`crate::cluster`]
/// needs to enqueue work and read back results/state after a run, and
/// keeping both halves of the driver in one crate (rather than exposing a
/// public mutation API third parties could also poke at) is a deliberate
/// choice -- external callers only ever see [`crate::cluster::SmrCluster`].
#[derive(Default)]
pub struct ReplicaState {
    /// Persistent per-slot recorder state, created lazily the first time
    /// any proposer (this replica's own, or a remote one during a `record`
    /// RPC) touches that slot. Never removed -- Stage 4a keeps the whole
    /// log's recorder state resident for the run (log compaction is a
    /// Phase-8 stretch concern, O2/O5-adjacent, well outside this scope).
    pub(crate) recorders: BTreeMap<u64, Recorder<Command>>,
    /// The first slot index this replica has not yet applied. A replica's
    /// own attempts only ever target this slot -- see the module docs.
    pub(crate) next_slot: u64,
    /// This replica's local application state: `Kv` after applying
    /// `applied_log[0..next_slot]` in order.
    pub(crate) kv: Kv,
    /// The exact sequence of decided commands this replica has applied, in
    /// slot order (`applied_log[i]` is slot `i`'s decided command). Kept
    /// around for tests/introspection (P5/P6/P7 assertions) rather than for
    /// anything the protocol itself needs.
    pub(crate) applied_log: Vec<Command>,
    /// Operations waiting for their turn (this replica processes at most
    /// one at a time; see the module docs on why there's no pipelining).
    pub(crate) queue: VecDeque<QueuedOp>,
    current_attempt: Option<CurrentAttempt>,
}

/// The [`queso_sim::node::Node`] implementation each replica runs. Thin
/// routing shim over the shared, `Rc<RefCell<_>>`-owned [`ReplicaState`] --
/// all the interesting bookkeeping lives there, following the same pattern
/// `queso_consensus::concrete::ReplicaNode` established.
pub struct SmrNode {
    pub(crate) state: Rc<RefCell<ReplicaState>>,
    pub(crate) results: Rc<RefCell<BTreeMap<OpId, OpRecord>>>,
    pub(crate) total_replicas: usize,
    pub(crate) leader: Option<NodeId>,
}

impl SmrNode {
    /// If this replica is idle (no attempt in flight) and has something
    /// queued, start a fresh [`Proposer`] for the front of the queue,
    /// targeting the current frontier slot. No-op otherwise (called
    /// defensively from several call sites; only one of them will ever find
    /// both conditions true at once).
    fn begin_next_attempt(
        &self,
        st: &mut ReplicaState,
        ctx: &mut NodeCtx<'_, ConcreteMsg<Command>>,
    ) {
        if st.current_attempt.is_some() {
            return;
        }
        let Some(op) = st.queue.pop_front() else {
            return;
        };
        let slot = st.next_slot;
        let mut proposer = Proposer::new(
            ctx.self_id(),
            self.total_replicas,
            op.command.clone(),
            self.leader,
            slot,
        );
        proposer.start(ctx);
        st.current_attempt = Some(CurrentAttempt {
            op_id: op.op_id,
            command: op.command,
            slot,
            proposer,
        });
    }

    /// The current attempt's slot has decided (on *some* value -- not
    /// necessarily this replica's own proposal). Apply it, advance the
    /// frontier, resolve or requeue this replica's pending operation
    /// accordingly, and immediately try to start the next attempt.
    fn finish_attempt(
        &self,
        st: &mut ReplicaState,
        decided: Command,
        ctx: &mut NodeCtx<'_, ConcreteMsg<Command>>,
    ) {
        let attempt = st
            .current_attempt
            .take()
            .expect("finish_attempt is only called right after a live attempt decided");
        debug_assert_eq!(
            attempt.slot, st.next_slot,
            "an attempt always targets the current frontier -- P7 gap-free application"
        );

        let applied = st.kv.apply(&decided);
        st.applied_log.push(decided.clone());
        st.next_slot += 1;

        if decided == attempt.command {
            // Our own pending operation is the one that got decided --
            // possibly because it *is* literally the proposal we sent,
            // possibly because some other replica proposed
            // content-identical (client, seq) command that won instead (a
            // duplicate submission of the same logical operation -- see
            // `crate::kv`'s P8a docs). Either way, from this replica's
            // point of view its own client is done.
            let outcome = match &decided {
                Command::Put { .. } => Outcome::Put,
                Command::Get { .. } => Outcome::Get(applied.get_value()),
            };
            if let Some(record) = self.results.borrow_mut().get_mut(&attempt.op_id) {
                record.completed_at = Some(ctx.now());
                record.decided_slot = Some(attempt.slot);
                record.outcome = Some(outcome);
            }
        } else {
            // Someone else's command won this slot. Per Meerkat's
            // reads-through-log design (see `crate::cluster`'s module
            // docs): re-propose the *same* pending command at the new
            // frontier, linearizing it after whatever just got decided.
            st.queue.push_front(QueuedOp {
                op_id: attempt.op_id,
                command: attempt.command,
            });
        }

        self.begin_next_attempt(st, ctx);
    }
}

impl Node<ConcreteMsg<Command>> for SmrNode {
    fn on_message(
        &mut self,
        from: NodeId,
        payload: ConcreteMsg<Command>,
        ctx: &mut NodeCtx<'_, ConcreteMsg<Command>>,
    ) {
        match payload {
            ConcreteMsg::Request(req) => {
                // Passive recorder role, routed to the slot this request
                // names -- see `queso_consensus::rpc::RecordRequest::slot`'s
                // docs for why this tag exists at all.
                let mut st = self.state.borrow_mut();
                let resp = st.recorders.entry(req.slot).or_default().handle(req);
                ctx.send(from, ConcreteMsg::Response(resp));
            }
            ConcreteMsg::Response(resp) => {
                let mut st = self.state.borrow_mut();
                let decided = match st.current_attempt.as_mut() {
                    Some(attempt) if attempt.slot == resp.slot => {
                        attempt.proposer.on_response(from, resp, ctx);
                        attempt.proposer.decided().cloned()
                    }
                    // Either idle, or this reply targets a slot we've since
                    // moved past/away from -- stale, ignore (mirrors
                    // `Proposer::on_response`'s own req_step staleness
                    // check, one layer up).
                    _ => None,
                };
                if let Some(decided_command) = decided {
                    self.finish_attempt(&mut st, decided_command, ctx);
                }
            }
        }
    }

    fn on_timer(&mut self, timer_id: TimerId, ctx: &mut NodeCtx<'_, ConcreteMsg<Command>>) {
        let mut st = self.state.borrow_mut();
        if timer_id == KICKOFF_TIMER {
            // Fired by `SmrCluster::submit` to kick off the very first
            // attempt after an idle replica received work (see that
            // method's docs for why this has to be a timer round-trip
            // rather than a direct call: `submit` runs outside any `Node`
            // callback, so it has no `NodeCtx` to drive a `Proposer` with).
            self.begin_next_attempt(&mut st, ctx);
            return;
        }
        if let Some(attempt) = st.current_attempt.as_mut() {
            // A retry timer for the live attempt's current step. Retries
            // never decide by themselves (only `on_response`'s quorum check
            // does), so there is nothing to chain afterward.
            attempt.proposer.on_timer(timer_id, ctx);
        }
    }

    // `on_restart` is intentionally left at the trait's default (a no-op),
    // exactly like `queso_consensus::concrete::ReplicaNode`: Stage 4a is
    // crash-*stop* (no persistence, no rejoin-as-learner) -- see the crate
    // docs' "Stage 4b seam" note for where durable-state recovery (P12)
    // would hook in here.
}
