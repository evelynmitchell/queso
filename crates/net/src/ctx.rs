//! [`RealCtx`]: the real-network implementation of
//! `queso_sim::node::Ctx<ConcreteMsg<Command>>` -- the same interface the
//! sim-verified `Node` implementations are written against, backed by real
//! sockets, real elapsed time, and a real seeded RNG instead of
//! `queso_sim::kernel::Kernel`'s deterministic logical clock, single PRNG
//! stream, and in-memory event queue.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use queso_consensus::rpc::ConcreteMsg;
use queso_sim::ids::{NodeId, TimerId};
use queso_sim::node::Ctx;
use queso_sim::time::LogicalTime;
use queso_smr::Command;
use rand::rngs::StdRng;
use rand::SeedableRng;
use tokio::sync::mpsc;

use crate::driver::Event;

/// The tick <-> real-time mapping plus everything [`Ctx::send`]/
/// [`Ctx::schedule_timer`] need to reach the outside world:
///
/// - `send` **buffers** `(dst, payload)` in `RealCtx::pending_outbound`
///   rather than handing it to the network immediately -- see
///   [`RealCtx::flush_outbound`]'s docs for why (write-before-reply, P12, on
///   real disk: `crate::driver::run_node` must persist this event's durable
///   mutations, if any, *before* anything it caused can actually reach the
///   wire or loop back into this replica's own inbox). Once flushed, a
///   buffered send targeting a destination this replica has no live/dialed
///   connection queue for, or whose queue is currently full (bounded at
///   `crate::transport::OUTBOUND_QUEUE_CAPACITY`, see that constant's docs),
///   is simply a silent drop, exactly like `queso_sim`'s fault injection
///   modeling a dropped message: the proposer's own unbounded
///   retry-with-backoff (see `queso_consensus::proposer`'s module docs) is
///   what tolerates this, not this layer. **Except** `dst == self_id`:
///   `queso_consensus`'s `Proposer::begin_step` sends a `RecordRequest` to
///   *every* recorder, including its own (`Proposer::all_recorders`
///   deliberately covers `0..total_replicas`, own id included -- see that
///   module's docs), and `queso_sim::kernel::Kernel::send` delivers that
///   self-send like any other. `outbound` is never populated for `self_id`
///   (see `crate::driver::run_node` -- a replica never dials itself), so a
///   naive lookup here would silently drop every replica's own vote, which
///   is fine only while a full `n`-of-`n` majority is live and quietly
///   breaks fault tolerance the moment it isn't (a proposer then needs
///   quorum entirely from its `n-1` peers, which is unreachable at the
///   actual fault-tolerance boundary). `flush_outbound` special-cases this
///   by looping the message back through `inbox` as a fresh
///   [`Event::Message`] instead, matching the sim's semantics.
/// - `schedule_timer` spawns a real `tokio::time::sleep` that re-injects a
///   `Timer` event into this replica's own inbox when it fires -- the real
///   analogue of `queso_sim::kernel::KernelCore::schedule_timer` pushing a
///   `Timer` event onto the kernel's priority queue. Unlike `send`, this is
///   *not* buffered: arming a timer has no externally-observable effect (it
///   reaches no peer, no client) until it fires and its *own* firing goes
///   through the ordinary event loop -- persisted before its own effects are
///   released, same as everything else -- so there is nothing for
///   write-before-reply to protect here.
pub struct RealCtx {
    self_id: NodeId,
    /// Fixed for the duration of dispatching one [`Event`] -- recomputed
    /// once per event by [`RealCtx::tick_now`] (called from
    /// `crate::driver::run_node`'s loop, mirroring exactly when
    /// `queso_sim::kernel::Kernel::run_until` advances its own `now` --
    /// before a callback runs, not resampled on every `Ctx::now()` call
    /// within one).
    now_ticks: LogicalTime,
    /// The lowest tick value [`RealCtx::tick_now`] will ever compute --
    /// restored, on a genuine restart, from the highest tick this replica
    /// had durably reached before it went down (see
    /// `crate::persist::PersistedState::max_tick`'s docs). `Instant::now()`
    /// itself always restarts at (real) zero on a fresh process, but
    /// `LogicalTime` must not: [`Node::on_restart`](queso_sim::node::Node::on_restart)-driven
    /// catch-up and the underlying `Proposer`'s retry/hedge backoff both
    /// compare timer deadlines computed from `now()` against `now()` as
    /// observed later, and a `LogicalTime` that jumped backward across a
    /// restart could make an old deadline look like it's already due (or
    /// vice versa) relative to state that *did* survive the restart. Zero
    /// on a genuinely fresh boot (nothing to restore).
    baseline: LogicalTime,
    start: Instant,
    tick: Duration,
    rng: StdRng,
    outbound: BTreeMap<NodeId, mpsc::Sender<ConcreteMsg<Command>>>,
    inbox: mpsc::UnboundedSender<Event>,
    /// Sends buffered by [`Ctx::send`] during the event currently being
    /// dispatched, not yet handed to the network/inbox -- see
    /// [`Self::flush_outbound`].
    pending_outbound: Vec<(NodeId, ConcreteMsg<Command>)>,
}

impl RealCtx {
    pub fn new(
        self_id: NodeId,
        seed: u64,
        tick: Duration,
        baseline: LogicalTime,
        outbound: BTreeMap<NodeId, mpsc::Sender<ConcreteMsg<Command>>>,
        inbox: mpsc::UnboundedSender<Event>,
    ) -> Self {
        Self {
            self_id,
            now_ticks: baseline,
            baseline,
            start: Instant::now(),
            tick,
            rng: StdRng::seed_from_u64(seed),
            outbound,
            inbox,
            pending_outbound: Vec::new(),
        }
    }

    /// Recompute `now_ticks` from real elapsed time since this replica
    /// started, offset by `Self::baseline`: `ticks = baseline +
    /// elapsed_nanos / tick_nanos`, i.e. the tick duration configured at
    /// startup (`NodeConfig::tick`) is exactly what every virtual-time delay
    /// in the consensus/SMR core (hedging delay, retry backoff, the
    /// catch-up watchdog interval, ...) now maps to in real wall-clock time,
    /// and `baseline` is exactly what keeps that mapping monotonic across a
    /// process restart (see `baseline`'s field docs).
    pub fn tick_now(&mut self) {
        let elapsed_nanos = self.start.elapsed().as_nanos();
        let tick_nanos = self.tick.as_nanos().max(1);
        let elapsed_ticks = (elapsed_nanos / tick_nanos) as u64;
        self.now_ticks = LogicalTime(self.baseline.0.saturating_add(elapsed_ticks));
    }

    /// Hand every send [`Ctx::send`] buffered while dispatching the event
    /// currently in progress to the real network (or this replica's own
    /// inbox, for a self-send) -- in the order they were issued. Must only
    /// be called once this event's durable-state mutations (if any) have
    /// already been fsync'd to disk: that ordering -- persist, *then*
    /// flush -- is exactly the write-before-reply property (P12) that
    /// prevents a recorder from acknowledging a `record` (or a client
    /// `Outcome` reply from being sent) whose corresponding durable
    /// mutation a subsequent crash could then roll back. See
    /// `crate::driver::run_node`'s event loop and `crate::persist`'s module
    /// docs for exactly where this is called relative to the fsync.
    pub fn flush_outbound(&mut self) {
        for (dst, payload) in self.pending_outbound.drain(..) {
            if dst == self.self_id {
                // Loopback: re-enter as a fresh event on this replica's own
                // inbox, drained by a later iteration of
                // `crate::driver::run_node`'s loop -- never delivered
                // synchronously inside the callback that originally called
                // `Ctx::send` (see `Ctx::send`'s impl below). This matches
                // `queso_sim::kernel::Kernel::send`, which always queues a
                // self-send as a later event rather than re-entering
                // `Node::on_message` on the spot, and avoids any reentrancy
                // into `SmrNode`/`Proposer` (not written to tolerate a
                // nested callback -- e.g. `RefCell::borrow_mut` in
                // `SmrNode::on_message` is already held for the duration of
                // the call that produced this buffered send, so a
                // synchronous re-entry would panic on a double-borrow).
                let _ = self.inbox.send(Event::Message {
                    from: self.self_id,
                    payload,
                });
                continue;
            }
            if let Some(tx) = self.outbound.get(&dst) {
                // `try_send` rather than the async `send`: this is called
                // from a synchronous point in the event loop, and a full
                // queue means `dst` has been unreachable for a while (see
                // `crate::transport::OUTBOUND_QUEUE_CAPACITY`'s docs) -- in
                // that case dropping this message, exactly like a genuine
                // network partition drops it in `queso_sim`, is the right
                // behavior, not blocking this replica's whole event loop
                // until space frees up.
                let _ = tx.try_send(payload);
            }
        }
    }
}

impl Ctx<ConcreteMsg<Command>> for RealCtx {
    fn self_id(&self) -> NodeId {
        self.self_id
    }

    fn now(&self) -> LogicalTime {
        self.now_ticks
    }

    fn send(&mut self, dst: NodeId, payload: ConcreteMsg<Command>) {
        // Buffered, not delivered here: see `Self::flush_outbound`'s docs
        // for why (write-before-reply, P12, on real disk) and for where the
        // actual self-loopback/outbound-queue delivery logic now lives --
        // `crate::driver::run_node`'s event loop calls `flush_outbound`
        // only after this event's durable-state mutations, if any, have
        // been fsync'd.
        self.pending_outbound.push((dst, payload));
    }

    fn schedule_timer(&mut self, after: u64, timer_id: TimerId) {
        let ticks = u128::from(after.max(1));
        let nanos = self.tick.as_nanos().saturating_mul(ticks);
        let nanos = u64::try_from(nanos).unwrap_or(u64::MAX);
        let duration = Duration::from_nanos(nanos);
        let inbox = self.inbox.clone();
        tokio::spawn(async move {
            tokio::time::sleep(duration).await;
            let _ = inbox.send(Event::Timer(timer_id));
        });
    }

    fn rng(&mut self) -> &mut StdRng {
        &mut self.rng
    }
}
