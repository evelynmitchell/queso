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
//!
//! # Stage 4b: durable vs. volatile state, and restart recovery (P9/P12)
//!
//! [`ReplicaState`] is split into [`Durable`] (must survive a crash +
//! restart) and everything else (must *not* -- it is rebuilt from scratch).
//! See [`Durable`]'s docs for exactly what's in each half and why, and for
//! how that split is modeled faithfully against what
//! `queso_sim::kernel::Kernel` actually does across `crash`/`restart` (in
//! short: *nothing* is dropped automatically -- `on_restart` must actively
//! clear the volatile half itself). [`SmrNode::on_restart`] is the recovery
//! entry point: it clears volatile state and kicks off a learner-style
//! catch-up (see [`SmrNode::begin_catch_up`]) before the replica resumes
//! ordinary participation.

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use queso_consensus::proposer::{Proposer, KICKOFF_TIMER};
use queso_consensus::recorder::Recorder;
use queso_consensus::rpc::ConcreteMsg;
use queso_sim::ids::{NodeId, TimerId};
use queso_sim::node::{Node, NodeCtx};
use queso_sim::time::LogicalTime;

use crate::command::{ClientId, Command, Value};
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

/// Reserved client id for this replica's own internal restart catch-up
/// probes (see [`SmrNode::begin_catch_up`]): a `Get` tagged with this
/// "client" is never a real workload operation, only ever a bounded
/// round-trip this replica issues to itself, after a restart, to discover
/// whether a slot was already decided elsewhere while it was down. Real
/// clients/tests must not use this id.
pub(crate) const CATCH_UP_CLIENT: ClientId = ClientId(u32::MAX);

/// The key a catch-up probe's `Get` names. Irrelevant to correctness (a
/// `Get` never mutates state, see [`Kv::apply`]) -- any fixed key works.
const CATCH_UP_KEY: crate::command::Key = 0;

/// Build the internal probe command a restart catch-up attempt proposes at
/// `slot`: a harmless, non-mutating `Get` that exists purely so this
/// replica can discover, via the ordinary [`Proposer`] quorum machinery,
/// whether `slot` was already decided by someone else while this replica
/// was down. `seq` is `slot` itself -- irrelevant to dedup (`Get`s are
/// never tracked in [`Kv`]'s dedup table), just a convenient, deterministic
/// tag for tracing/debugging.
fn catch_up_probe(slot: u64) -> Command {
    Command::Get {
        client: CATCH_UP_CLIENT,
        seq: slot,
        key: CATCH_UP_KEY,
    }
}

/// Which of two things a [`CurrentAttempt`] is standing in for: a real
/// client-visible operation, or this replica's own internal restart
/// catch-up probe. Both are driven by the exact same [`Proposer`]/
/// [`Recorder`] machinery -- this only changes what happens once the
/// attempt's slot decides (see `SmrNode::finish_attempt`).
enum AttemptOrigin {
    /// A real operation a client submitted, awaiting its outcome in the
    /// shared `results` table.
    Op(OpId),
    /// This replica's own internal catch-up probe (see
    /// [`SmrNode::begin_catch_up`]) -- never client-visible, never recorded
    /// in `results`.
    CatchUp,
}

/// The one [`Proposer`] this replica currently has in flight, if any.
struct CurrentAttempt {
    origin: AttemptOrigin,
    command: Command,
    slot: u64,
    proposer: Proposer<Command>,
}

/// The durable half of one replica's state (P9/P12): everything that must
/// survive a crash + restart so a restarted replica can never violate
/// P1-P10. Kept as its own type, distinct from [`ReplicaState`]'s volatile
/// fields, so the split the property model calls for (see
/// `docs/02-properties.md`'s P12 design-dependency note) is explicit in the
/// type system rather than only asserted in comments.
///
/// # What's here, and why it's exactly this and no more
///
/// - `recorders`: each slot's [`Recorder`], i.e. its ISR `(S, F_c, A_c,
///   A_p)`. This is *the* state P12's design note calls out by name.
///   Losing it on restart would let this replica's recorder answer a
///   future `record` RPC as if it had never seen an earlier step -- which
///   could let some proposer's majority-intersection safety argument
///   (`queso_consensus::proposer`'s module docs) silently lose a recorder
///   it had already (in reality) counted toward a quorum, risking
///   Agreement (P1) the next time this replica is one of the `f+1`
///   survivors a decision depends on.
/// - `next_slot` + `applied_log` + `kv`: the decided-log frontier and the
///   application state folded from it. This is what P9 ("no lost committed
///   writes") cashes out to concretely: an acknowledged write is one this
///   replica (or another) has durably applied, and `kv`'s embedded
///   `last_seq` table (see [`Kv::apply`]) *is* the client-session dedup
///   high-water marks A6/P8a call for -- there is no separate table to
///   persist, `Kv` already carries it.
///
/// Everything else in [`ReplicaState`] (the pending-op queue, the in-flight
/// [`CurrentAttempt`]) is deliberately *not* here: it is a proposer's
/// in-progress conversation with recorders across the network, which a real
/// crash genuinely loses (the proposer was mid-round-trip, holding
/// responses only in RAM) -- see [`SmrNode::on_restart`].
///
/// # Modeling durability faithfully inside a deterministic in-memory sim
///
/// There is no real disk/fsync/WAL here -- that hardening is explicitly a
/// Phase-8 item (`docs/00-project-outline.md`'s Phase 8 notes: "production
/// hardening of the durability & restart-recovery machinery"). But "no real
/// disk" does not mean "not modeled": `queso_sim::kernel::Kernel` never
/// drops or recreates a crashed node -- `Kernel::crash` only flips fault
/// state so messages/timers to it are dropped, and `Kernel::restart` calls
/// [`Node::on_restart`] on the *exact same*, still-heap-resident
/// `Box<dyn Node<_>>` it has held the whole time (see
/// `Kernel::apply_fault_command`'s `Restart` arm). Concretely: nothing here
/// is ever cleared by the harness on its own. So the faithful way to model
/// "this field is durable" is to leave it alone in `on_restart`, and
/// modeling "this field is volatile" -- something the harness itself does
/// not give for free -- means `on_restart` must *actively* clear it, which
/// is exactly what [`SmrNode::on_restart`] does. A real deployment would
/// back this struct with fsync'd storage (a write-ahead log, or
/// equivalent) written synchronously before any RPC reply that depends on
/// it -- see `SmrNode::on_message`'s `Request` arm for where that
/// write-before-reply ordering is enforced here.
#[derive(Default)]
pub struct Durable {
    /// Persistent per-slot recorder state, created lazily the first time
    /// any proposer (this replica's own, or a remote one during a `record`
    /// RPC) touches that slot. Never removed -- Stage 4a/4b keep the whole
    /// log's recorder state resident for the run (log compaction is a
    /// Phase-8 stretch concern, O2/O5-adjacent, well outside this scope).
    pub(crate) recorders: BTreeMap<u64, Recorder<Command>>,
    /// The first slot index this replica has not yet applied. A replica's
    /// own attempts only ever target this slot -- see the module docs.
    pub(crate) next_slot: u64,
    /// This replica's local application state: `Kv` after applying
    /// `applied_log[0..next_slot]` in order. Its embedded dedup table
    /// (A6/P8a) is durable along with everything else here.
    pub(crate) kv: Kv,
    /// The exact sequence of decided commands this replica has applied, in
    /// slot order (`applied_log[i]` is slot `i`'s decided command). Kept
    /// around for tests/introspection (P5/P6/P7 assertions) rather than for
    /// anything the protocol itself needs.
    pub(crate) applied_log: Vec<Command>,
}

/// One replica's full state: the durable half (see [`Durable`]) plus
/// volatile, in-memory-only bookkeeping that a real crash would genuinely
/// lose and that [`SmrNode::on_restart`] therefore clears explicitly.
/// Fields are `pub(crate)` rather than fully private: [`crate::cluster`]
/// needs to enqueue work and read back results/state after a run, and
/// keeping both halves of the driver in one crate (rather than exposing a
/// public mutation API third parties could also poke at) is a deliberate
/// choice -- external callers only ever see [`crate::cluster::SmrCluster`].
#[derive(Default)]
pub struct ReplicaState {
    /// See [`Durable`].
    pub(crate) durable: Durable,
    /// Operations waiting for their turn (this replica processes at most
    /// one at a time; see the module docs on why there's no pipelining).
    /// Volatile: a real crash loses whatever a client had in flight to this
    /// replica specifically. That is a liveness cost only (P11/O4) -- the
    /// op was never acknowledged from this attempt, and `(client, seq)`
    /// dedup (A6/P8a) makes a client's retry (to this or another replica)
    /// safe.
    pub(crate) queue: VecDeque<QueuedOp>,
    /// The one [`Proposer`] in flight, if any -- either a real client op or
    /// this replica's own catch-up probe. Volatile for the same reason as
    /// `queue`: a real crash loses a proposer's not-yet-quorate
    /// conversation with recorders.
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
        let slot = st.durable.next_slot;
        let mut proposer = Proposer::new(
            ctx.self_id(),
            self.total_replicas,
            op.command.clone(),
            self.leader,
            slot,
        );
        proposer.start(ctx);
        st.current_attempt = Some(CurrentAttempt {
            origin: AttemptOrigin::Op(op.op_id),
            command: op.command,
            slot,
            proposer,
        });
    }

    /// Rejoin as a learner after a restart (P12's rejoin policy): start (or
    /// continue -- see `finish_attempt`'s `CatchUp` arm) a fresh [`Proposer`]
    /// at the current frontier, proposing nothing but an internal,
    /// non-mutating [`catch_up_probe`]. This is *exactly* the same
    /// reads-through-log catch-up mechanism `crate::cluster`'s module docs
    /// describe for an ordinary lagging `Get` -- see that module's docs --
    /// just driven internally rather than by a client op, so a restarted
    /// replica advances its frontier on its own instead of waiting for the
    /// next real submission to discover it is behind.
    ///
    /// No-op if an attempt is already in flight (defensive; `on_restart` is
    /// the only real caller and always starts from an idle state).
    fn begin_catch_up(&self, st: &mut ReplicaState, ctx: &mut NodeCtx<'_, ConcreteMsg<Command>>) {
        if st.current_attempt.is_some() {
            return;
        }
        let slot = st.durable.next_slot;
        let command = catch_up_probe(slot);
        let mut proposer = Proposer::new(
            ctx.self_id(),
            self.total_replicas,
            command.clone(),
            self.leader,
            slot,
        );
        proposer.start(ctx);
        st.current_attempt = Some(CurrentAttempt {
            origin: AttemptOrigin::CatchUp,
            command,
            slot,
            proposer,
        });
    }

    /// The current attempt's slot has decided (on *some* value -- not
    /// necessarily this replica's own proposal). Apply it, advance the
    /// frontier, and then branch on what kind of attempt this was.
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
            attempt.slot, st.durable.next_slot,
            "an attempt always targets the current frontier -- P7 gap-free application"
        );

        let applied = st.durable.kv.apply(&decided);
        st.durable.applied_log.push(decided.clone());
        st.durable.next_slot += 1;

        match attempt.origin {
            AttemptOrigin::Op(op_id) => {
                if decided == attempt.command {
                    // Our own pending operation is the one that got decided
                    // -- possibly because it *is* literally the proposal we
                    // sent, possibly because some other replica proposed
                    // content-identical (client, seq) command that won
                    // instead (a duplicate submission of the same logical
                    // operation -- see `crate::kv`'s P8a docs). Either way,
                    // from this replica's point of view its own client is
                    // done.
                    let outcome = match &decided {
                        Command::Put { .. } => Outcome::Put,
                        Command::Get { .. } => Outcome::Get(applied.get_value()),
                    };
                    if let Some(record) = self.results.borrow_mut().get_mut(&op_id) {
                        record.completed_at = Some(ctx.now());
                        record.decided_slot = Some(attempt.slot);
                        record.outcome = Some(outcome);
                    }
                } else {
                    // Someone else's command won this slot. Per Meerkat's
                    // reads-through-log design (see `crate::cluster`'s
                    // module docs): re-propose the *same* pending command at
                    // the new frontier, linearizing it after whatever just
                    // got decided.
                    st.queue.push_front(QueuedOp {
                        op_id,
                        command: attempt.command,
                    });
                }
                self.begin_next_attempt(st, ctx);
            }
            AttemptOrigin::CatchUp => {
                if decided == attempt.command {
                    // Our own catch-up probe is what got decided: nothing
                    // was pending at this slot before we asked, so we have
                    // caught up to the true frontier. Catch-up is over --
                    // resume ordinary participation, picking up whatever
                    // client work is queued (submitted before the crash, or
                    // while catch-up was still running).
                    self.begin_next_attempt(st, ctx);
                } else {
                    // Someone else's decision -- we really were behind by
                    // at least this slot. Keep learning at the new
                    // frontier.
                    self.begin_catch_up(st, ctx);
                }
            }
        }
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
                //
                // Write-before-reply (P12): `Recorder::handle` mutates this
                // slot's durable ISR state (`st.durable.recorders`, see
                // `Durable`'s docs) synchronously, strictly before the
                // `ctx.send` below ever runs -- there is no `await`, no
                // background task, nothing that could reorder "reply sent"
                // ahead of "state persisted" on this single-threaded,
                // event-at-a-time kernel. Combined with `Durable` never
                // being cleared by `on_restart` (see that impl), a proposer
                // can *never* observe a `RecordResponse` whose corresponding
                // durable state a subsequent crash could then roll back: by
                // the time the response is even constructed, the mutation
                // has already happened. A real deployment would replace
                // this line with a synchronous, fsync'd write to the
                // recorder's on-disk state before constructing the reply
                // (Phase-8 hardening); the ordering guarantee is identical,
                // only the storage medium differs.
                let mut st = self.state.borrow_mut();
                let resp = st
                    .durable
                    .recorders
                    .entry(req.slot)
                    .or_default()
                    .handle(req);
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

    /// This replica has just restarted (P12): volatile state is gone (see
    /// [`ReplicaState`]'s docs), [`Durable`] is untouched. Concretely:
    ///
    /// 1. Drop the volatile half explicitly -- `queue` and
    ///    `current_attempt` -- exactly as a real process restart would
    ///    (the harness itself does not clear these for us; see
    ///    [`Durable`]'s "modeling durability faithfully" section). Any
    ///    client whose op was queued or mid-flight through this replica
    ///    specifically was never acknowledged from this attempt, so
    ///    dropping it costs only liveness (P11/O4); `(client, seq)` dedup
    ///    (A6/P8a) makes that client's eventual retry -- to this replica
    ///    again, or another -- safe.
    /// 2. Rejoin as a learner (the "recover durable state, or rejoin as a
    ///    learner and catch up" policy `docs/02-properties.md`'s P12 note
    ///    calls for): [`Self::begin_catch_up`] drives an internal probe
    ///    through consecutive slots starting at the *durable* `next_slot`
    ///    until it finds one nothing had decided yet, discovering (and
    ///    applying) anything decided elsewhere while this replica was
    ///    down before resuming ordinary participation. Because this
    ///    replica's own recorders/log/kv were never cleared, "recover
    ///    durable state" and "catch up on the rest" compose exactly as the
    ///    design note expects -- catch-up only ever needs to cover the
    ///    *gap*, never the whole log.
    ///
    /// This is what makes P10 hold even for a just-restarted replica: it
    /// never serves (or acts on behalf of) a client until its own
    /// `next_slot`/`kv` reflect everything decided so far that it can
    /// discover, and every real `Get` this crate ever issues goes through
    /// the log rather than reading local state directly regardless (see
    /// `crate::cluster`'s module docs) -- restart catch-up is what keeps
    /// that "everything decided so far" bound tight, not what makes P10
    /// hold in the first place.
    fn on_restart(&mut self, ctx: &mut NodeCtx<'_, ConcreteMsg<Command>>) {
        let mut st = self.state.borrow_mut();
        st.queue.clear();
        st.current_attempt = None;
        self.begin_catch_up(&mut st, ctx);
    }
}
