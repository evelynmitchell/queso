//! Envelope types: the unit of communication between simulated nodes.
//!
//! See the module docs in [`crate::payload`] for why the metadata/payload
//! split exists and how it enforces the content-oblivious vs content-aware
//! distinction at the type level.

use crate::ids::{MessageId, NodeId};
use crate::time::LogicalTime;

/// Everything about a message that a content-oblivious scheduler is allowed
/// to see: who sent it, who it's addressed to, how big it is, and when it
/// was sent. Notably absent: the payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnvelopeMeta {
    /// Unique, kernel-assigned id for this message.
    pub id: MessageId,
    /// Sending node.
    pub src: NodeId,
    /// Destination node.
    pub dst: NodeId,
    /// Payload size, as reported by `Payload::size`.
    pub size: usize,
    /// Logical time at which `send` was called.
    pub sent_at: LogicalTime,
}

/// A message in flight: metadata plus the actual payload. Only handed to
/// [`crate::scheduler::AwareScheduler`] implementations and to the
/// destination node upon delivery.
#[derive(Debug, Clone)]
pub struct Envelope<P> {
    /// The content-oblivious-visible half of the envelope.
    pub meta: EnvelopeMeta,
    /// The payload. Opaque to anything that only has an `EnvelopeMeta`.
    pub payload: P,
}
