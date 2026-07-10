//! The trace recorder: every externally-visible kernel event, in order.
//!
//! This is the substrate for property **D9** (reproducibility): two runs of
//! the same scenario with the same seed must produce byte-for-byte identical
//! traces. `Trace::to_canonical_bytes` gives a stable byte representation
//! (derived from `Debug` output, which — unlike a hash-based representation
//! — is ordered purely by struct/enum field declaration order and Vec
//! insertion order, both of which are already deterministic here) so tests
//! can assert literal byte-for-byte equality, not just `==` on the Rust
//! value.

use std::fmt;

use crate::fault::DropReason;
use crate::ids::{MessageId, NodeId, TimerId};
use crate::time::LogicalTime;

/// A single externally-visible event, timestamped in logical time and
/// tagged with the kernel's monotonic dispatch sequence number (`seq`) so
/// that even same-`LogicalTime` events have a total, deterministic order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TraceEvent {
    /// A node called `send`.
    Send {
        time: LogicalTime,
        seq: u64,
        id: MessageId,
        src: NodeId,
        dst: NodeId,
        size: usize,
    },
    /// A message was handed to its destination node.
    Deliver {
        time: LogicalTime,
        seq: u64,
        id: MessageId,
        src: NodeId,
        dst: NodeId,
    },
    /// A message was dropped instead of delivered.
    Drop {
        time: LogicalTime,
        seq: u64,
        id: MessageId,
        src: NodeId,
        dst: NodeId,
        reason: DropReason,
    },
    /// A timer was scheduled to fire at `fire_at`.
    TimerScheduled {
        time: LogicalTime,
        seq: u64,
        node: NodeId,
        timer_id: TimerId,
        fire_at: LogicalTime,
    },
    /// A timer fired and was delivered to its node.
    TimerFired {
        time: LogicalTime,
        seq: u64,
        node: NodeId,
        timer_id: TimerId,
    },
    /// A timer's fire time arrived but the node was crashed, so it was
    /// silently discarded instead of delivered.
    TimerDropped {
        time: LogicalTime,
        seq: u64,
        node: NodeId,
        timer_id: TimerId,
    },
    /// A node was crashed.
    Crash {
        time: LogicalTime,
        seq: u64,
        node: NodeId,
    },
    /// A node was restarted.
    Restart {
        time: LogicalTime,
        seq: u64,
        node: NodeId,
    },
    /// A manual network partition was installed.
    Partition {
        time: LogicalTime,
        seq: u64,
        group_a: Vec<NodeId>,
        group_b: Vec<NodeId>,
    },
    /// The manual network partition was healed.
    Heal { time: LogicalTime, seq: u64 },
    /// A node's delay multiplier was changed.
    SlowNode {
        time: LogicalTime,
        seq: u64,
        node: NodeId,
        multiplier: u64,
    },
    /// The kernel's notion of "current leader" changed. There is no
    /// consensus yet — this is just a settable id the adversary schedulers
    /// use to decide who to target (see `docs/03-testing-plan.md §1`).
    LeaderChanged {
        time: LogicalTime,
        seq: u64,
        leader: Option<NodeId>,
    },
}

/// An ordered, append-only recording of every `TraceEvent` the kernel
/// produced during a run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Trace {
    events: Vec<TraceEvent>,
}

impl Trace {
    /// A fresh, empty trace.
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn record(&mut self, event: TraceEvent) {
        self.events.push(event);
    }

    /// All recorded events, in the order the kernel produced them.
    pub fn events(&self) -> &[TraceEvent] {
        &self.events
    }

    /// A canonical byte representation of the trace, suitable for literal
    /// byte-for-byte comparison across two runs (the Phase-0 reproducibility
    /// gate, property D9).
    pub fn to_canonical_bytes(&self) -> Vec<u8> {
        let mut out = String::new();
        for event in &self.events {
            // `{:?}` on these types is a pure function of their field values
            // (no addresses, no hash-map iteration, no time-of-day) so this
            // is deterministic across runs and processes.
            use fmt::Write;
            writeln!(out, "{event:?}").expect("writing to a String cannot fail");
        }
        out.into_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_event(seq: u64) -> TraceEvent {
        TraceEvent::Send {
            time: LogicalTime(seq),
            seq,
            id: MessageId(seq),
            src: NodeId(0),
            dst: NodeId(1),
            size: 42,
        }
    }

    #[test]
    fn events_are_recorded_in_order() {
        let mut trace = Trace::new();
        trace.record(sample_event(0));
        trace.record(sample_event(1));
        trace.record(sample_event(2));
        let seqs: Vec<u64> = trace
            .events()
            .iter()
            .map(|e| match e {
                TraceEvent::Send { seq, .. } => *seq,
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(seqs, vec![0, 1, 2]);
    }

    #[test]
    fn identical_traces_produce_identical_bytes() {
        let mut a = Trace::new();
        let mut b = Trace::new();
        for i in 0..5 {
            a.record(sample_event(i));
            b.record(sample_event(i));
        }
        assert_eq!(a, b);
        assert_eq!(a.to_canonical_bytes(), b.to_canonical_bytes());
    }

    #[test]
    fn different_traces_produce_different_bytes() {
        let mut a = Trace::new();
        let mut b = Trace::new();
        a.record(sample_event(0));
        b.record(sample_event(1));
        assert_ne!(a, b);
        assert_ne!(a.to_canonical_bytes(), b.to_canonical_bytes());
    }

    #[test]
    fn empty_trace_has_empty_bytes() {
        let trace = Trace::new();
        assert_eq!(trace.to_canonical_bytes(), Vec::<u8>::new());
    }
}
