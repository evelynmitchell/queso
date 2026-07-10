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
/// (sending messages, scheduling timers) happens through the [`NodeCtx`]
/// handed to each callback — a node never reaches for wall-clock time or
/// ambient randomness itself.
pub trait Node<P> {
    /// A message addressed to this node has arrived.
    fn on_message(&mut self, from: NodeId, payload: P, ctx: &mut NodeCtx<'_, P>);

    /// A timer this node scheduled has fired.
    fn on_timer(&mut self, timer_id: TimerId, ctx: &mut NodeCtx<'_, P>);

    /// This node has just restarted: volatile state should be considered
    /// gone. The default implementation does nothing, which is correct for
    /// stateless nodes. Phase 0 has no durable-state story yet (see
    /// `docs/02-properties.md` P12); this hook is where a future phase
    /// would recover persisted state instead of starting fresh.
    #[allow(unused_variables)]
    fn on_restart(&mut self, ctx: &mut NodeCtx<'_, P>) {}
}

/// The handle a [`Node`] uses to interact with the kernel during a
/// callback: send messages, schedule timers, read the logical clock, or
/// draw from the shared deterministic PRNG stream.
pub struct NodeCtx<'a, P> {
    pub(crate) core: &'a mut KernelCore<P>,
    pub(crate) self_id: NodeId,
}

impl<'a, P: Payload> NodeCtx<'a, P> {
    /// This node's own id.
    pub fn self_id(&self) -> NodeId {
        self.self_id
    }

    /// The current logical time.
    pub fn now(&self) -> LogicalTime {
        self.core.now
    }

    /// Send `payload` to `dst`. Subject to fault injection (crash/
    /// partition) and then the active scheduler's delay/reorder/drop
    /// decision.
    pub fn send(&mut self, dst: NodeId, payload: P) {
        self.core.send(self.self_id, dst, payload);
    }

    /// Schedule a timer to fire `after` ticks from now, identified by
    /// `timer_id` (a namespace private to this node).
    pub fn schedule_timer(&mut self, after: u64, timer_id: TimerId) {
        self.core.schedule_timer(self.self_id, after, timer_id);
    }

    /// Mutable access to the kernel's single seeded PRNG stream. Draws made
    /// here are part of the same deterministic sequence the scheduler
    /// draws from — order matters, not which stream, since there is only
    /// one, consumed in the kernel's single-threaded dispatch order.
    pub fn rng(&mut self) -> &mut StdRng {
        &mut self.core.rng
    }
}
