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
//!
//! Catch-up itself is watched by a quiescence watchdog
//! ([`SmrNode::on_catch_up_watchdog`], armed by
//! [`SmrNode::arm_catch_up_watchdog`]): a restarted replica that cannot yet
//! reach a live majority (e.g. it comes back up alone after a full-cluster
//! crash, or during a long partition) would otherwise drive its catch-up
//! probe through a [`Proposer`] whose per-step retries are capped and, once
//! exhausted, never self-resume -- permanently stalling that replica even
//! after a majority becomes reachable again. The watchdog re-issues a fresh
//! catch-up attempt at the same frontier slot whenever the current one has
//! made no progress for a full [`CATCH_UP_WATCHDOG_TICKS`] interval, so the
//! replica keeps retrying (never faster than that interval, so it never
//! races a genuinely-still-progressing attempt) until it can actually make
//! progress. See [`SmrNode::on_catch_up_watchdog`]'s docs for why re-issuing
//! is a pure liveness action that never touches ISR/decision/quorum
//! correctness.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use queso_consensus::proposer::{Proposer, KICKOFF_TIMER, RETRY_DELAY_TICKS};
use queso_consensus::recorder::Recorder;
use queso_consensus::rpc::ConcreteMsg;
use queso_sim::ids::{NodeId, TimerId};
use queso_sim::node::{Ctx, Node};
use queso_sim::time::LogicalTime;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::command::{ClientId, Command, Value};
use crate::kv::Kv;
use crate::tuning::EpochTuner;

/// How a replica's [`Proposer`]s decide who the slot's fast-path leader is
/// and (Phase 5/6) what hedging delay each replica gets.
///
/// - `Fixed` reproduces every pre-Phase-6 constructor's behavior exactly:
///   one caller-supplied `leader` (or `None`, purely leaderless) for every
///   slot in the run, unconditional δ=0 activation (no
///   [`Proposer::with_hedging`] call at all) -- unchanged from Phase 4.
/// - `Tuned` (Phase 6, D4) hands that choice to a shared [`EpochTuner`]
///   instead: leader and hedging schedule are derived per-slot from the
///   slot's epoch (see `crate::tuning`'s module docs), and every proposer is
///   built with [`Proposer::with_hedging`] even when its delay happens to be
///   `0` (harmless -- `0` collapses to the exact same immediate-activation
///   behavior, see `Proposer::start`'s docs).
///
/// `Clone` is required because every replica's [`SmrNode`] needs its own
/// copy: `Fixed`'s payload is `Copy`, and `Tuned`'s is an `Rc` clone of the
/// one shared tuner all replicas consult (see `crate::tuning`'s module docs
/// on why a single shared handle is this harness's deliberate
/// simplification of real cross-replica tuning agreement).
#[derive(Clone)]
pub(crate) enum LeaderPolicy {
    Fixed(Option<NodeId>),
    Tuned(Rc<RefCell<EpochTuner>>),
}

impl LeaderPolicy {
    fn leader_for(&self, slot: u64) -> Option<NodeId> {
        match self {
            LeaderPolicy::Fixed(leader) => *leader,
            LeaderPolicy::Tuned(tuner) => Some(tuner.borrow().leader_for_slot(slot)),
        }
    }

    /// This replica's hedging activation delay for `slot`, or `None` if
    /// hedging is not in play at all (the `Fixed` policy never hedges,
    /// matching every pre-Phase-6 constructor's unconditional-activation
    /// behavior unchanged).
    fn delay_for(&self, slot: u64, id: NodeId) -> Option<u64> {
        match self {
            LeaderPolicy::Fixed(_) => None,
            LeaderPolicy::Tuned(tuner) => Some(tuner.borrow().delay_for_slot(slot, id)),
        }
    }

    /// Record keeping for the tuner (a no-op under `Fixed`): the slot's
    /// first proposal attempt just started.
    fn note_attempt_start(&self, slot: u64, now: LogicalTime) {
        if let LeaderPolicy::Tuned(tuner) = self {
            tuner.borrow_mut().note_attempt_start(slot, now);
        }
    }

    /// Record keeping for the tuner (a no-op under `Fixed`): the slot has
    /// just decided.
    fn note_slot_decided(&self, slot: u64, now: LogicalTime) {
        if let LeaderPolicy::Tuned(tuner) = self {
            tuner.borrow_mut().note_slot_decided(slot, now);
        }
    }
}

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
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
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

/// How many logical ticks a catch-up attempt is given to make forward
/// progress before the quiescence watchdog ([`SmrNode::on_catch_up_watchdog`])
/// concludes its underlying [`Proposer`] has parked for good and re-issues
/// catch-up from scratch.
///
/// # The "permanent zombie" liveness bug this closes (P11/O4)
///
/// [`SmrNode::begin_next_attempt`]/[`SmrNode::begin_catch_up`] refuse to start
/// anything new while `current_attempt.is_some()`, so a catch-up attempt that
/// makes no progress -- e.g. a replica restarted alone after a full-cluster
/// crash, unable to reach a live majority -- would occupy that slot until it
/// eventually completes. Historically the underlying [`Proposer`] capped its
/// per-step retries and then *parked* forever, so such an attempt could never
/// recover even once a majority became reachable again. The hedging work
/// replaced that hard cap with unbounded exponential backoff, so a
/// lone-restarted replica now keeps retrying and does rejoin on its own. This
/// watchdog remains as a defensive backstop: if a catch-up attempt shows no
/// progress for a full interval it drops the stale attempt and re-issues
/// catch-up from scratch, guarding against transient stalls and against any
/// future regression to a bounded-retry proposer.
///
/// Set comfortably longer than the proposer's maximum per-step retry-backoff
/// spacing, so the watchdog only ever fires on a genuinely stalled attempt --
/// never racing (or interrupting) still-in-progress retries.
const CATCH_UP_WATCHDOG_TICKS: u64 = 128 * RETRY_DELAY_TICKS;

/// Reserved timer id for the catch-up quiescence watchdog. Distinct from
/// [`KICKOFF_TIMER`] (`TimerId(u64::MAX)`), from [`queso_consensus::proposer::HEDGE_TIMER`]
/// (`TimerId(u64::MAX - 1)` -- live in this crate as of Phase 6's
/// `LeaderPolicy::Tuned`, see `crate::tuning`'s module docs; Phase 4b picked
/// `u64::MAX - 1` for *this* constant before this crate ever called
/// [`Proposer::with_hedging`], so the two were only accidentally distinct
/// until now -- this is `u64::MAX - 2` specifically to fix that collision,
/// not incidentally), and from any live [`Proposer`]'s own per-step retry
/// timer (`TimerId(step)`, always far smaller in any run this crate's tests
/// exercise -- A7, step counts are practically bounded), so the four timer
/// namespaces a replica uses never collide.
const CATCH_UP_WATCHDOG_TIMER: TimerId = TimerId(u64::MAX - 2);

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
///
/// # Real persistence (Phase 7 hardening, issue #36)
///
/// `Clone` plus feature-gated `Serialize`/`Deserialize` are bookkeeping
/// additions only -- no change to the fields above or to any consensus/ISR/
/// quorum/decision logic. They exist so a real driver (`queso-net`) can call
/// [`SmrNode::durable_snapshot`] to obtain an owned, serializable copy of
/// this replica's durable state, write it to fsync'd on-disk storage via the
/// atomic-rename pattern, and -- on a subsequent boot -- reload it and hand
/// it to [`SmrNode::from_durable`] so a restarted *process* (not just a
/// restarted in-sim `Node`) recovers with its memory intact instead of
/// starting blank. See `crates/net/src/driver.rs`'s module docs and
/// `crates/net/src/persist.rs` for exactly how those two methods are wired
/// into the write-before-reply ordering on real disk.
#[derive(Default, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
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
    /// The `(slot, generation)` the most recently armed catch-up quiescence
    /// watchdog timer ([`CATCH_UP_WATCHDOG_TIMER`]) was armed for, if any --
    /// `generation` is `watchdog_generation`'s value at arm time. The
    /// kernel has no facility to cancel an already-scheduled timer (see
    /// `queso_sim::node::NodeCtx::schedule_timer`'s docs), so every firing
    /// must re-check both fields against current state before acting --
    /// exactly the same "recognize and ignore a stale timer" discipline
    /// [`Proposer`]'s own retry timer already uses (`req_step`/`step`). See
    /// [`SmrNode::on_catch_up_watchdog`]. Volatile: purely a liveness
    /// bookkeeping aid, not part of what a restart needs to recover.
    watchdog_armed_for: Option<(u64, u64)>,
    /// Monotonically increasing generation counter, bumped every time a
    /// catch-up attempt is (re)armed -- including by the watchdog re-issuing
    /// catch-up itself -- so a watchdog firing can recognize whether it is
    /// still watching the attempt it was armed for. See `watchdog_armed_for`.
    watchdog_generation: u64,
}

/// The [`queso_sim::node::Node`] implementation each replica runs. Thin
/// routing shim over the shared, `Rc<RefCell<_>>`-owned [`ReplicaState`] --
/// all the interesting bookkeeping lives there, following the same pattern
/// `queso_consensus::concrete::ReplicaNode` established.
pub struct SmrNode {
    pub(crate) state: Rc<RefCell<ReplicaState>>,
    pub(crate) results: Rc<RefCell<BTreeMap<OpId, OpRecord>>>,
    pub(crate) total_replicas: usize,
    pub(crate) leader_policy: LeaderPolicy,
    /// This replica's own co-located recorder's most-recently-observed ISR
    /// step *for whichever slot this replica's own current attempt targets*
    /// -- the hedging evidence-of-progress signal (see
    /// `queso_consensus::proposer`'s module docs' "Hedging" section), scoped
    /// per-slot below in [`SmrNode::on_message`] since (unlike
    /// `queso_consensus::concrete::ConcreteCluster`, which has exactly one
    /// recorder per replica) this replica's single recorder map answers
    /// `record` requests for many different slots, most of them irrelevant
    /// to this replica's *own* in-flight attempt. Only consulted when
    /// `leader_policy` is `LeaderPolicy::Tuned`; harmlessly unused
    /// otherwise.
    pub(crate) local_step: Rc<Cell<u64>>,
}

impl SmrNode {
    /// Build a single fresh replica participating in an `n`-replica cluster
    /// (`total_replicas`) with a fixed (or, if `None`, absent) fast-path
    /// leader for every slot -- the exact same per-replica construction
    /// [`crate::cluster::SmrCluster::build`] performs internally for its
    /// `Kernel`-driven nodes, just exposed publicly so a non-sim driver
    /// (`queso-net`'s real-network event loop) can build and drive this
    /// same, unmodified [`Node`] implementation over a real transport
    /// without depending on anything `Kernel`-specific.
    ///
    /// There is no `id` parameter: nothing in [`SmrNode`] itself stores
    /// this replica's own id -- every callback learns it afresh from
    /// `ctx.self_id()` (see e.g. [`Self::begin_next_attempt`]), so it is
    /// entirely the driver's responsibility (sim `Kernel::add_node`'s key,
    /// or a real driver's own config) to keep a `SmrNode` instance and the
    /// id it is driven under consistent.
    pub fn new_fixed_leader(total_replicas: usize, leader: Option<NodeId>) -> Self {
        SmrNode {
            state: Rc::new(RefCell::new(ReplicaState::default())),
            results: Rc::new(RefCell::new(BTreeMap::new())),
            total_replicas,
            leader_policy: LeaderPolicy::Fixed(leader),
            local_step: Rc::new(Cell::new(0)),
        }
    }

    /// Build a replica whose durable state is *not* empty: seeded from a
    /// previously [`Self::durable_snapshot`]ted (and, in a real deployment,
    /// persisted-then-reloaded) [`Durable`], as if this replica's process
    /// had never lost that state at all. Every volatile field
    /// ([`ReplicaState::queue`], the in-flight attempt, the watchdog
    /// bookkeeping) starts fresh, exactly like [`Self::new_fixed_leader`] --
    /// a real crash genuinely loses those (see [`Durable`]'s docs).
    ///
    /// This constructor deliberately does **not** call [`Node::on_restart`]
    /// itself: only the caller knows whether `durable` came from a real
    /// reload (a genuine restart, which must rejoin as a learner) or is a
    /// still-cold first boot's default `Durable` (which must not), so
    /// driving `on_restart` is left to the caller once it has a live `ctx`
    /// -- see `crates/net/src/driver.rs::run_node`'s boot sequence for the
    /// real driver that makes this call.
    pub fn from_durable(total_replicas: usize, leader: Option<NodeId>, durable: Durable) -> Self {
        SmrNode {
            state: Rc::new(RefCell::new(ReplicaState {
                durable,
                ..ReplicaState::default()
            })),
            results: Rc::new(RefCell::new(BTreeMap::new())),
            total_replicas,
            leader_policy: LeaderPolicy::Fixed(leader),
            local_step: Rc::new(Cell::new(0)),
        }
    }

    /// An owned snapshot of this replica's current [`Durable`] state --
    /// bookkeeping only, no logic change (a plain `Clone` of the shared
    /// `Rc<RefCell<_>>`'s durable half). See [`Self::from_durable`] and
    /// [`Durable`]'s "real persistence" docs for how a real driver uses
    /// this.
    pub fn durable_snapshot(&self) -> Durable {
        self.state.borrow().durable.clone()
    }

    /// Submit `command`, tagged `op_id`, as a fresh client-visible
    /// operation this replica should propose -- mirrors
    /// [`crate::cluster::SmrCluster::submit`]'s enqueue-then-kick logic
    /// exactly (guard against [`CATCH_UP_CLIENT`], compute a
    /// monotonic `invoked_at`, record a pending [`OpRecord`], push onto the
    /// queue, and if the replica was idle a moment ago schedule a
    /// zero-delay [`KICKOFF_TIMER`]), so any driver reaches
    /// [`Self::begin_next_attempt`] through the identical `Node::on_timer`
    /// path the sim harness already exhaustively tests -- rather than
    /// calling it directly, which would (harmlessly, but needlessly)
    /// diverge from that verified path just because a real driver happens
    /// to already hold a live `ctx` where `SmrCluster::submit` (called from
    /// outside any `Node` callback) does not.
    pub fn submit(
        &self,
        op_id: OpId,
        replica: NodeId,
        command: Command,
        ctx: &mut dyn Ctx<ConcreteMsg<Command>>,
    ) {
        debug_assert_ne!(
            command.client_seq().0,
            CATCH_UP_CLIENT,
            "ClientId(u32::MAX) is reserved for this crate's internal restart catch-up probes \
             (see crate::replica::CATCH_UP_CLIENT's docs) -- a real client/test must never \
             submit using it"
        );
        let invoked_at = self.next_invoked_at(ctx.now());
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
        let should_kick = {
            let mut st = self.state.borrow_mut();
            st.queue.push_back(QueuedOp { op_id, command });
            st.queue.len() == 1
        };
        if should_kick {
            ctx.schedule_timer(0, KICKOFF_TIMER);
        }
    }

    /// A faithful invocation timestamp for a *new* submission: at least
    /// `now`, but strictly after every operation that has already completed
    /// by this point -- the exact same tie-avoidance treatment
    /// [`crate::cluster::SmrCluster::next_invoked_at`] applies (see that
    /// method's docs for the full soundness argument against
    /// [`crate::linearizability`]). `SmrNode` has no single shared clock to
    /// consult beyond `ctx.now()` and no driver-external call site the way
    /// `SmrCluster::submit` does (this is always called from inside a live
    /// `ctx`), but the tie it closes is the same one: two submissions
    /// separated by an observed completion must never be assigned an
    /// identical `invoked_at`.
    fn next_invoked_at(&self, now: LogicalTime) -> LogicalTime {
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

    /// Read back a previously [`Self::submit`]ted operation's result, if it
    /// has completed.
    pub fn result(&self, op_id: OpId) -> Option<OpRecord> {
        self.results.borrow().get(&op_id).cloned()
    }

    /// If this replica is idle (no attempt in flight) and has something
    /// queued, start a fresh [`Proposer`] for the front of the queue,
    /// targeting the current frontier slot. No-op otherwise (called
    /// defensively from several call sites; only one of them will ever find
    /// both conditions true at once).
    fn begin_next_attempt(&self, st: &mut ReplicaState, ctx: &mut dyn Ctx<ConcreteMsg<Command>>) {
        if st.current_attempt.is_some() {
            return;
        }
        let Some(op) = st.queue.pop_front() else {
            return;
        };
        let slot = st.durable.next_slot;
        let leader = self.leader_policy.leader_for(slot);
        let mut proposer = Proposer::new(
            ctx.self_id(),
            self.total_replicas,
            op.command.clone(),
            leader,
            slot,
        );
        if let Some(delay) = self.leader_policy.delay_for(slot, ctx.self_id()) {
            proposer = proposer.with_hedging(delay, self.local_step.clone());
        }
        self.leader_policy.note_attempt_start(slot, ctx.now());
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
    fn begin_catch_up(&self, st: &mut ReplicaState, ctx: &mut dyn Ctx<ConcreteMsg<Command>>) {
        if st.current_attempt.is_some() {
            return;
        }
        let slot = st.durable.next_slot;
        let command = catch_up_probe(slot);
        let leader = self.leader_policy.leader_for(slot);
        let mut proposer = Proposer::new(
            ctx.self_id(),
            self.total_replicas,
            command.clone(),
            leader,
            slot,
        );
        if let Some(delay) = self.leader_policy.delay_for(slot, ctx.self_id()) {
            proposer = proposer.with_hedging(delay, self.local_step.clone());
        }
        self.leader_policy.note_attempt_start(slot, ctx.now());
        proposer.start(ctx);
        st.current_attempt = Some(CurrentAttempt {
            origin: AttemptOrigin::CatchUp,
            command,
            slot,
            proposer,
        });
        self.arm_catch_up_watchdog(st, ctx, slot);
    }

    /// (Re-)arm the catch-up quiescence watchdog for the catch-up attempt
    /// just started at `slot`: bump the generation counter, record which
    /// `(slot, generation)` this arm is watching, and schedule
    /// [`CATCH_UP_WATCHDOG_TIMER`] to fire [`CATCH_UP_WATCHDOG_TICKS`] ticks
    /// from now. Called only from [`Self::begin_catch_up`], so every catch-up
    /// attempt -- whether started fresh, resumed after learning of another
    /// slot, or reissued by the watchdog itself -- is always watched by
    /// exactly one live arm.
    fn arm_catch_up_watchdog(
        &self,
        st: &mut ReplicaState,
        ctx: &mut dyn Ctx<ConcreteMsg<Command>>,
        slot: u64,
    ) {
        st.watchdog_generation += 1;
        st.watchdog_armed_for = Some((slot, st.watchdog_generation));
        ctx.schedule_timer(CATCH_UP_WATCHDOG_TICKS, CATCH_UP_WATCHDOG_TIMER);
    }

    /// [`CATCH_UP_WATCHDOG_TIMER`] fired. Re-issues catch-up iff this firing
    /// is still watching a *live, unprogressed* catch-up attempt:
    ///
    /// 1. `watchdog_armed_for`'s `(slot, generation)` must still match
    ///    `watchdog_generation` exactly -- otherwise a fresher arm has
    ///    already superseded this one (catch-up advanced to a new slot, or
    ///    the watchdog itself already re-armed once), so this firing is
    ///    stale.
    /// 2. The replica's *current* attempt must still be a catch-up attempt
    ///    targeting that exact slot -- otherwise the frontier genuinely
    ///    advanced (catch-up finished and ordinary work resumed, or another
    ///    restart happened) since this timer was armed.
    ///
    /// Both together are the same "recognize and ignore a stale timer"
    /// discipline [`Proposer`]'s own retry timer already relies on
    /// (`req_step`/`step`) -- see the module docs.
    ///
    /// # Why re-issuing (drop + fresh [`Proposer`]) is safe, not just live
    ///
    /// This only ever changes *when* a catch-up probe is (re)proposed, never
    /// what gets decided or how: [`catch_up_probe`] is a pure function of
    /// `slot`, so the dropped attempt's `command` and the fresh attempt's
    /// `command` are *identical* -- `finish_attempt`'s `decided ==
    /// attempt.command` check (the one that recognizes "this replica's own
    /// probe is what won the slot") is content-based, not tied to which
    /// `Proposer` *instance* sent it. Any response the abandoned `Proposer`
    /// is still owed (its outstanding retries were never cancelled -- the
    /// kernel has no such facility, see `watchdog_armed_for`'s docs) that
    /// arrives late is simply folded into the fresh `Proposer`'s own
    /// `responses` for the same `(slot, step)` if it is still waiting on
    /// that exact step (`Proposer::on_response`'s `slot`/`req_step` check is
    /// the only gate `SmrNode::on_message` applies before forwarding) --
    /// exactly the same "any number of `record` calls, from any number of
    /// proposers, converge safely on one recorder's shared per-step ISR
    /// state" tolerance this module's own docs already describe for
    /// concurrent proposers racing the same slot from *different* replicas.
    /// Nothing here is a new safety argument, only one more source (this
    /// replica's own superseded local proposer) of the same kind of
    /// harmless, provenance-agnostic extra evidence the algorithm already
    /// tolerates by design. This never touches ISR/decision/quorum logic
    /// itself -- `Recorder`/`Proposer` are used completely unmodified.
    fn on_catch_up_watchdog(&self, st: &mut ReplicaState, ctx: &mut dyn Ctx<ConcreteMsg<Command>>) {
        let Some((watched_slot, watched_generation)) = st.watchdog_armed_for else {
            return;
        };
        if watched_generation != st.watchdog_generation {
            return; // Superseded by a fresher arm -- stale, ignore.
        }
        let is_stalled_catch_up = matches!(
            st.current_attempt.as_ref(),
            Some(attempt)
                if attempt.slot == watched_slot
                    && matches!(attempt.origin, AttemptOrigin::CatchUp)
        );
        if !is_stalled_catch_up {
            // Either idle, running an ordinary op, or already catching up on
            // a later slot -- catch-up made progress (or finished) since
            // this watchdog was armed. Nothing to re-arm.
            return;
        }
        // The catch-up attempt for `watched_slot` has made no progress in a
        // full watchdog interval -- comfortably longer than the underlying
        // `Proposer`'s own worst-case retry budget (see
        // `CATCH_UP_WATCHDOG_TICKS`), so it has almost certainly exhausted
        // its retries and parked forever: exactly the liveness bug this
        // watchdog exists to close. Drop the stale attempt and start a
        // brand-new catch-up `Proposer` at the same frontier slot -- fresh
        // retry budget, fresh watchdog arm -- so the replica keeps trying
        // instead of zombie-parking. `begin_catch_up` re-arms the watchdog
        // itself, so this replica keeps retrying, once per interval,
        // indefinitely, until a majority becomes reachable and catch-up can
        // actually make progress.
        st.current_attempt = None;
        self.begin_catch_up(st, ctx);
    }

    /// The current attempt's slot has decided (on *some* value -- not
    /// necessarily this replica's own proposal). Apply it, advance the
    /// frontier, and then branch on what kind of attempt this was.
    fn finish_attempt(
        &self,
        st: &mut ReplicaState,
        decided: Command,
        ctx: &mut dyn Ctx<ConcreteMsg<Command>>,
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
        // Auto-tuning bookkeeping (D4, no-op under `LeaderPolicy::Fixed`):
        // this is the first time *this replica* has observed `attempt.slot`
        // decide, but the tuner itself dedupes against every other
        // replica's own observation of the same slot (see
        // `EpochTuner::note_slot_decided`'s docs), so calling this
        // unconditionally here -- regardless of attempt origin -- is safe.
        self.leader_policy
            .note_slot_decided(attempt.slot, ctx.now());

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
        ctx: &mut dyn Ctx<ConcreteMsg<Command>>,
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
                let slot = req.slot;
                let resp = st.durable.recorders.entry(slot).or_default().handle(req);
                // Hedging evidence-of-progress signal (see `local_step`'s
                // field docs): only meaningful when this reply is about the
                // exact slot this replica's own attempt (if any) is
                // currently working -- a reply about some other slot this
                // replica happens to be a passive recorder for tells this
                // replica's *own* proposer nothing about its own slot's
                // progress.
                if st.current_attempt.as_ref().is_some_and(|a| a.slot == slot) {
                    self.local_step.set(resp.step);
                }
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

    fn on_timer(&mut self, timer_id: TimerId, ctx: &mut dyn Ctx<ConcreteMsg<Command>>) {
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
        if timer_id == CATCH_UP_WATCHDOG_TIMER {
            // See `Self::on_catch_up_watchdog` for the re-arm/re-issue
            // logic and why it never risks safety, only liveness.
            self.on_catch_up_watchdog(&mut st, ctx);
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
    fn on_restart(&mut self, ctx: &mut dyn Ctx<ConcreteMsg<Command>>) {
        let mut st = self.state.borrow_mut();
        st.queue.clear();
        st.current_attempt = None;
        self.begin_catch_up(&mut st, ctx);
    }
}
