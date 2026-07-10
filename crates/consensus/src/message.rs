//! The single wire message this crate ever sends: one replica's
//! disseminated proposal set for the current tcast step.

use queso_sim::payload::{Inspectable, Payload};

use crate::proposal::ProposalSet;

/// A tcast dissemination message: `src`'s proposal-set input for the
/// current tcast call. Sent point-to-point (not literally broadcast as a
/// single multicast primitive, since the harness models point-to-point
/// links) to every other live replica by the [`crate::tcast::tcast`]
/// driver.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcastMsg<V> {
    pub set: ProposalSet<V>,
}

impl<V> Payload for TcastMsg<V> {
    fn size(&self) -> usize {
        // Opaque metric only (used by the scheduler/trace for size
        // bookkeeping, never for correctness) -- proposal count is a fine
        // stand-in for an abstract-layer message that doesn't have a real
        // wire encoding yet.
        self.set.len()
    }
}

impl<V> Inspectable for TcastMsg<V> {
    fn tag(&self) -> &'static str {
        // Phase 1 has exactly one message kind; a real tag distinction
        // (e.g. by round/phase) is Phase 2's concern once the concrete
        // 4-phase protocol exists for a content-aware adversary to target.
        "tcast"
    }
}
