//! Small newtype identifiers used throughout the kernel.
//!
//! These are plain `Copy` integers wrapped in newtypes so the various id
//! spaces (nodes, messages, timers) can't be accidentally confused with one
//! another or with raw logical-time values. All of them implement `Ord` so
//! they can live in `BTreeMap`/`BTreeSet` keys, which is required to keep
//! iteration order deterministic (see the crate-level docs).

use std::fmt;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Identifies a simulated node (replica) in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct NodeId(pub u32);

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "n{}", self.0)
    }
}

/// Identifies an in-flight message. Assigned by the kernel at send time in
/// strictly increasing order, so it also acts as a stable, deterministic
/// send-sequence number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MessageId(pub u64);

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "m{}", self.0)
    }
}

/// Identifies a timer within a node's own namespace. The kernel does not
/// interpret this value; it is opaque and chosen by whoever schedules the
/// timer (a node, or a test/demo driver).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimerId(pub u64);

impl fmt::Display for TimerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "t{}", self.0)
    }
}
