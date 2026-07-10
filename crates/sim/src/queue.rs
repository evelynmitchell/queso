//! The deterministic event priority queue.
//!
//! Events are ordered by `(logical_time, tiebreak_seq)`: a min-heap on
//! logical time, with a strictly-monotonic sequence number assigned at
//! scheduling time breaking ties between same-`LogicalTime` events. Because
//! `tiebreak_seq` is assigned in the exact order the (single-threaded)
//! kernel schedules events, this gives every run a total, reproducible
//! order — the foundation the whole reproducibility contract (D9) sits on.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use crate::fault::FaultCommand;
use crate::ids::{MessageId, NodeId, TimerId};
use crate::time::LogicalTime;

#[derive(Debug)]
pub(crate) enum EventKind {
    Timer { node: NodeId, timer_id: TimerId },
    MessageArrival { id: MessageId },
    Fault(FaultCommand),
}

#[derive(Debug)]
pub(crate) struct ScheduledEvent {
    pub time: LogicalTime,
    pub seq: u64,
    pub kind: EventKind,
}

// Ordering intentionally ignores `kind`: `seq` is unique per event, so two
// events are never equal-and-different, and `kind` need not (and mostly
// cannot, cheaply) implement Ord itself.
impl PartialEq for ScheduledEvent {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time && self.seq == other.seq
    }
}
impl Eq for ScheduledEvent {}
impl PartialOrd for ScheduledEvent {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for ScheduledEvent {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.time, self.seq).cmp(&(other.time, other.seq))
    }
}

/// A min-heap of `ScheduledEvent`s ordered by `(time, seq)`.
#[derive(Debug, Default)]
pub(crate) struct EventQueue {
    heap: BinaryHeap<Reverse<ScheduledEvent>>,
}

impl EventQueue {
    pub fn push(&mut self, time: LogicalTime, seq: u64, kind: EventKind) {
        self.heap.push(Reverse(ScheduledEvent { time, seq, kind }));
    }

    pub fn pop(&mut self) -> Option<ScheduledEvent> {
        self.heap.pop().map(|Reverse(e)| e)
    }

    pub fn peek_time(&self) -> Option<LogicalTime> {
        self.heap.peek().map(|Reverse(e)| e.time)
    }
}
