//! The opaque message-payload boundary.
//!
//! This is the type-level mechanism behind assumption **A3** (content-oblivious
//! adversary, see `docs/02-properties.md`). A "content-oblivious" scheduler
//! implements [`crate::scheduler::ObliviousScheduler`], whose `on_send` method
//! signature only ever receives an [`crate::network::EnvelopeMeta`] — source,
//! destination, size, send time. The payload type `P` does not appear in that
//! trait at all, so it is *structurally impossible* for an
//! `ObliviousScheduler` implementation to read message contents: there is no
//! way to name, borrow, or otherwise get at a value it was never handed.
//!
//! A "content-aware" scheduler implements
//! [`crate::scheduler::AwareScheduler<P>`] instead, whose `on_send` receives
//! the full [`crate::network::Envelope<P>`] including `payload: P`. The two
//! traits are deliberately incompatible: a type can implement one, the other,
//! or both, but nothing lets an `ObliviousScheduler` smuggle payload access
//! in through the back door.

/// Anything that can be sent as a message payload must at least be able to
/// report its own size, so the network layer can record envelope metadata
/// (`EnvelopeMeta::size`) without needing to inspect the payload itself.
pub trait Payload {
    /// Size of the payload in bytes (or whatever unit the caller wants to
    /// use consistently — the kernel treats this as an opaque metric used
    /// only for tracing/scheduling heuristics, never for correctness).
    fn size(&self) -> usize;
}

/// Payloads that voluntarily expose a coarse "kind" tag for content-aware
/// scheduling tests (e.g. "defeat the fast path by dropping `Vote` messages
/// but not `Ping`s"). Only [`crate::scheduler::AwareScheduler`] can observe
/// this; [`crate::scheduler::ObliviousScheduler`] implementors never see a
/// payload value at all, tagged or not.
pub trait Inspectable: Payload {
    /// A short, stable label identifying the kind of message this is.
    fn tag(&self) -> &'static str;
}

impl Payload for Vec<u8> {
    fn size(&self) -> usize {
        self.len()
    }
}

impl Payload for () {
    fn size(&self) -> usize {
        0
    }
}

impl Payload for u64 {
    fn size(&self) -> usize {
        std::mem::size_of::<u64>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vec_u8_size_is_its_length() {
        assert_eq!(Payload::size(&vec![1u8, 2, 3]), 3);
        assert_eq!(Payload::size(&Vec::<u8>::new()), 0);
    }

    #[test]
    fn unit_payload_has_zero_size() {
        assert_eq!(Payload::size(&()), 0);
    }

    #[test]
    fn u64_payload_has_fixed_size() {
        assert_eq!(Payload::size(&42u64), 8);
    }
}
