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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{MessageId, NodeId, TimerId};

    fn marker(id: u64) -> EventKind {
        EventKind::MessageArrival { id: MessageId(id) }
    }

    #[test]
    fn pops_in_strict_time_then_seq_order_regardless_of_push_order() {
        // Deliberately out-of-order times, plus same-time ties broken only
        // by `seq`, pushed in a scrambled order -- this is the exact
        // contract (D9) the whole reproducibility story rests on.
        let mut q = EventQueue::default();
        q.push(LogicalTime(5), 30, marker(30));
        q.push(LogicalTime(2), 10, marker(10));
        q.push(LogicalTime(5), 20, marker(20));
        q.push(LogicalTime(2), 5, marker(5));
        q.push(LogicalTime(0), 1, marker(1));
        q.push(LogicalTime(5), 25, marker(25));
        q.push(LogicalTime(2), 6, marker(6));

        let expected = [
            (LogicalTime(0), 1),
            (LogicalTime(2), 5),
            (LogicalTime(2), 6),
            (LogicalTime(2), 10),
            (LogicalTime(5), 20),
            (LogicalTime(5), 25),
            (LogicalTime(5), 30),
        ];

        for (time, seq) in expected {
            assert_eq!(q.peek_time(), Some(time));
            let popped = q.pop().expect("queue should not be empty yet");
            assert_eq!(popped.time, time);
            assert_eq!(popped.seq, seq);
        }
        assert_eq!(q.pop().map(|e| e.time), None, "queue should now be empty");
    }

    #[test]
    fn empty_queue_pops_none_and_peeks_none() {
        let mut q = EventQueue::default();
        assert_eq!(q.peek_time(), None);
        assert!(q.pop().is_none());
    }

    #[test]
    fn timer_and_fault_events_also_order_purely_by_time_then_seq() {
        // Same contract, but exercised with all three `EventKind` variants
        // to make sure ordering never depends on `kind`.
        let mut q = EventQueue::default();
        q.push(LogicalTime(3), 3, EventKind::Fault(FaultCommand::Heal));
        q.push(
            LogicalTime(3),
            2,
            EventKind::Timer {
                node: NodeId(1),
                timer_id: TimerId(0),
            },
        );
        q.push(LogicalTime(3), 1, marker(99));

        let first = q.pop().unwrap();
        assert_eq!((first.time, first.seq), (LogicalTime(3), 1));
        let second = q.pop().unwrap();
        assert_eq!((second.time, second.seq), (LogicalTime(3), 2));
        let third = q.pop().unwrap();
        assert_eq!((third.time, third.seq), (LogicalTime(3), 3));
    }
}
