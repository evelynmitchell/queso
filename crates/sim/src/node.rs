//! The `Node` trait: how simulated participants plug into the kernel.
//!
//! Phase 0 has no consensus logic, so `Node` implementations in this phase
//! are deliberately trivial (echo, counting, forwarding — see
//! `examples/echo_demo.rs`). The trait itself is written to be the seam
//! later phases build the real protocol on top of.

use rand::rngs::StdRng;

use crate::ids::{NodeId, TimerId};
use crate::kernel::KernelCore;
use crate::payload::Payload;
use crate::time::LogicalTime;

/// A participant in the simulation. All interaction with the outside world
/// (sending messages, scheduling timers) happens through the [`Ctx`] handed
/// to each callback — a node never reaches for wall-clock time or ambient
/// randomness itself.
///
/// Methods take `&mut dyn Ctx<P>` rather than the concrete [`NodeCtx`] so
/// that the exact same `Node` implementations (the verified consensus/SMR
/// core) can be driven by something other than [`crate::kernel::Kernel`] --
/// e.g. a real tokio event loop over TCP (see `queso-net`) -- without any
/// change to their logic. `dyn Ctx<P>` keeps `Node` object-safe (the sim
/// stores nodes as `Box<dyn Node<P>>`), so this is a trait-object
/// parameter, not a generic method type parameter.
pub trait Node<P> {
    /// A message addressed to this node has arrived.
    fn on_message(&mut self, from: NodeId, payload: P, ctx: &mut dyn Ctx<P>);

    /// A timer this node scheduled has fired.
    fn on_timer(&mut self, timer_id: TimerId, ctx: &mut dyn Ctx<P>);

    /// This node has just restarted: volatile state should be considered
    /// gone. The default implementation does nothing, which is correct for
    /// stateless nodes. Phase 0 has no durable-state story yet (see
    /// `docs/02-properties.md` P12); this hook is where a future phase
    /// would recover persisted state instead of starting fresh.
    #[allow(unused_variables)]
    fn on_restart(&mut self, ctx: &mut dyn Ctx<P>) {}
}

/// The interface a [`Node`] uses to interact with its driver during a
/// callback: send messages, schedule timers, read the current time, or draw
/// from a seeded PRNG stream. [`NodeCtx`] is the in-simulation
/// implementation (backed by `KernelCore`'s deterministic logical clock
/// and single PRNG stream); a real-network driver (`queso-net`) provides a
/// second implementation backed by real time, real sockets, and a real
/// seeded RNG. Node/consensus/SMR code is written against this trait alone,
/// so it cannot tell (and must not behave differently depending on) which
/// implementation is driving it.
pub trait Ctx<P> {
    /// This node's own id.
    fn self_id(&self) -> NodeId;

    /// The current time: a logical tick count in simulation, a
    /// real-time-derived tick count over a real network. Either way, the
    /// only notion of time `Node` implementations may observe.
    fn now(&self) -> LogicalTime;

    /// Send `payload` to `dst`.
    fn send(&mut self, dst: NodeId, payload: P);

    /// Schedule a timer to fire `after` ticks from now, identified by
    /// `timer_id` (a namespace private to this node).
    fn schedule_timer(&mut self, after: u64, timer_id: TimerId);

    /// Mutable access to this driver's seeded PRNG stream.
    fn rng(&mut self) -> &mut StdRng;
}

/// The sim kernel's [`Ctx`] implementation: send/schedule_timer/rng/now are
/// forwarded to the shared `KernelCore`, scoped to `self_id`.
pub struct NodeCtx<'a, P> {
    pub(crate) core: &'a mut KernelCore<P>,
    pub(crate) self_id: NodeId,
}

impl<'a, P: Payload> Ctx<P> for NodeCtx<'a, P> {
    fn self_id(&self) -> NodeId {
        self.self_id
    }

    fn now(&self) -> LogicalTime {
        self.core.now
    }

    /// Subject to fault injection (crash/partition) and then the active
    /// scheduler's delay/reorder/drop decision.
    fn send(&mut self, dst: NodeId, payload: P) {
        self.core.send(self.self_id, dst, payload);
    }

    fn schedule_timer(&mut self, after: u64, timer_id: TimerId) {
        self.core.schedule_timer(self.self_id, after, timer_id);
    }

    /// Draws made here are part of the same deterministic sequence the
    /// scheduler draws from — order matters, not which stream, since there
    /// is only one, consumed in the kernel's single-threaded dispatch
    /// order.
    fn rng(&mut self) -> &mut StdRng {
        &mut self.core.rng
    }
}
