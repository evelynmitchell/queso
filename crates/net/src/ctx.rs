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
/// - `send` enqueues onto the destination peer's outbound channel (drained
///   by `crate::transport::spawn_peer_dialer`); a destination this replica
///   has no live/dialed connection queue for (never true in ordinary
///   operation -- `outbound` is populated for every configured peer at
///   startup, see `crate::driver::run_node`) is simply a silent drop,
///   exactly like `queso_sim`'s fault injection modeling a dropped
///   message: the proposer's own unbounded retry-with-backoff (see
///   `queso_consensus::proposer`'s module docs) is what tolerates this,
///   not this layer.
/// - `schedule_timer` spawns a real `tokio::time::sleep` that re-injects a
///   `Timer` event into this replica's own inbox when it fires -- the real
///   analogue of `queso_sim::kernel::KernelCore::schedule_timer` pushing a
///   `Timer` event onto the kernel's priority queue.
pub struct RealCtx {
    self_id: NodeId,
    /// Fixed for the duration of dispatching one [`Event`] -- recomputed
    /// once per event by [`RealCtx::tick_now`] (called from
    /// `crate::driver::run_node`'s loop, mirroring exactly when
    /// `queso_sim::kernel::Kernel::run_until` advances its own `now` --
    /// before a callback runs, not resampled on every `Ctx::now()` call
    /// within one).
    now_ticks: LogicalTime,
    start: Instant,
    tick: Duration,
    rng: StdRng,
    outbound: BTreeMap<NodeId, mpsc::UnboundedSender<ConcreteMsg<Command>>>,
    inbox: mpsc::UnboundedSender<Event>,
}

impl RealCtx {
    pub fn new(
        self_id: NodeId,
        seed: u64,
        tick: Duration,
        outbound: BTreeMap<NodeId, mpsc::UnboundedSender<ConcreteMsg<Command>>>,
        inbox: mpsc::UnboundedSender<Event>,
    ) -> Self {
        Self {
            self_id,
            now_ticks: LogicalTime::ZERO,
            start: Instant::now(),
            tick,
            rng: StdRng::seed_from_u64(seed),
            outbound,
            inbox,
        }
    }

    /// Recompute `now_ticks` from real elapsed time since this replica
    /// started: `ticks = elapsed_nanos / tick_nanos`, i.e. the tick
    /// duration configured at startup (`NodeConfig::tick`) is exactly what
    /// every virtual-time delay in the consensus/SMR core (hedging delay,
    /// retry backoff, the catch-up watchdog interval, ...) now maps to in
    /// real wall-clock time.
    pub fn tick_now(&mut self) {
        let elapsed_nanos = self.start.elapsed().as_nanos();
        let tick_nanos = self.tick.as_nanos().max(1);
        self.now_ticks = LogicalTime((elapsed_nanos / tick_nanos) as u64);
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
        if let Some(tx) = self.outbound.get(&dst) {
            let _ = tx.send(payload);
        }
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
