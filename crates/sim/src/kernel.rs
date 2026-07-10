//! The deterministic discrete-event simulation kernel.
//!
//! `Kernel<P>` is the whole harness: it owns the logical clock, the single
//! seeded PRNG, the event priority queue, the network (envelopes + active
//! scheduler), fault-injection state, and the trace recorder. Nodes never
//! see any of this directly — they only get a [`crate::node::NodeCtx`]
//! during a callback, which is the sole channel through which they may send
//! messages, schedule timers, read the clock, or draw randomness.
//!
//! # Determinism
//!
//! Given the same seed, the same sequence of node registrations, and the
//! same sequence of external calls (fault injection, `run_until`, etc.),
//! two `Kernel` runs produce byte-for-byte identical traces. This holds
//! because:
//!
//! - the kernel is single-threaded — there is no thread interleaving to
//!   vary between runs;
//! - the only randomness is `KernelCore::rng`, an `rand::rngs::StdRng`
//!   seeded once at construction;
//! - the only "time" is `LogicalTime`, advanced solely by popping events
//!   off the priority queue, never read from the OS;
//! - the priority queue orders strictly by `(LogicalTime, tiebreak_seq)`,
//!   and `tiebreak_seq` is assigned in call order, so same-time events
//!   never depend on hash-map iteration or any other incidental ordering;
//! - node/fault/scheduler collections that could otherwise introduce
//!   iteration-order nondeterminism (`nodes`, `FaultState`'s sets/maps) are
//!   all `BTreeMap`/`BTreeSet`.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use rand::rngs::StdRng;
use rand::SeedableRng;

use crate::fault::{DropReason, FaultCommand, FaultState};
use crate::ids::{MessageId, NodeId, TimerId};
use crate::network::{Envelope, EnvelopeMeta};
use crate::node::{Node, NodeCtx};
use crate::payload::Payload;
use crate::queue::{EventKind, EventQueue};
use crate::scheduler::{Decision, SchedulerCtx, SchedulerKind};
use crate::time::LogicalTime;
use crate::trace::{Trace, TraceEvent};

/// The passive state the kernel manages: clock, PRNG, queue, network,
/// faults, and the trace. Split out from `Kernel` so that message-send and
/// timer-scheduling logic (which nodes reach via `NodeCtx`, and which never
/// needs to touch the node registry) can take `&mut KernelCore<P>` without
/// the borrow checker also demanding access to `Kernel::nodes`.
pub struct KernelCore<P> {
    pub(crate) now: LogicalTime,
    next_seq: u64,
    next_message_id: u64,
    pub(crate) rng: StdRng,
    trace: Trace,
    faults: FaultState,
    scheduler: SchedulerKind<P>,
    leader: Option<NodeId>,
    queue: EventQueue,
    in_flight: BTreeMap<MessageId, Envelope<P>>,
}

impl<P: Payload> KernelCore<P> {
    fn bump_seq(&mut self) -> u64 {
        let s = self.next_seq;
        self.next_seq += 1;
        s
    }

    /// Send `payload` from `src` to `dst`, subject to fault injection and
    /// then the active scheduler's decision. This is the sole path by
    /// which a message enters the network — used by `NodeCtx::send` and by
    /// `Kernel::inject_message`.
    pub(crate) fn send(&mut self, src: NodeId, dst: NodeId, payload: P) {
        let id = MessageId(self.next_message_id);
        self.next_message_id += 1;
        let meta = EnvelopeMeta {
            id,
            src,
            dst,
            size: payload.size(),
            sent_at: self.now,
        };

        let send_seq = self.bump_seq();
        self.trace.record(TraceEvent::Send {
            time: self.now,
            seq: send_seq,
            id,
            src,
            dst,
            size: meta.size,
        });

        if self.faults.is_crashed(src) || self.faults.is_crashed(dst) {
            let seq = self.bump_seq();
            self.trace.record(TraceEvent::Drop {
                time: self.now,
                seq,
                id,
                src,
                dst,
                reason: DropReason::Crashed,
            });
            return;
        }
        if self.faults.is_partitioned(src, dst) {
            let seq = self.bump_seq();
            self.trace.record(TraceEvent::Drop {
                time: self.now,
                seq,
                id,
                src,
                dst,
                reason: DropReason::Partitioned,
            });
            return;
        }

        let envelope = Envelope { meta, payload };
        let decision = {
            let mut ctx = SchedulerCtx {
                now: self.now,
                rng: &mut self.rng,
                leader: self.leader,
            };
            self.scheduler.decide(&envelope, &mut ctx)
        };

        match decision {
            Decision::Deliver { delay } => {
                let multiplier = self.faults.slow_multiplier(src, dst);
                let total_delay = delay.saturating_mul(multiplier).max(1);
                let arrival = self.now.advance(total_delay);
                let seq = self.bump_seq();
                self.queue
                    .push(arrival, seq, EventKind::MessageArrival { id });
                self.in_flight.insert(id, envelope);
            }
            Decision::Drop => {
                let seq = self.bump_seq();
                self.trace.record(TraceEvent::Drop {
                    time: self.now,
                    seq,
                    id,
                    src,
                    dst,
                    reason: DropReason::Scheduler,
                });
            }
        }
    }

    /// Schedule a timer for `node`, firing `after` ticks from now.
    pub(crate) fn schedule_timer(&mut self, node: NodeId, after: u64, timer_id: TimerId) {
        let fire_at = self.now.advance(after.max(1));
        let seq = self.bump_seq();
        self.trace.record(TraceEvent::TimerScheduled {
            time: self.now,
            seq,
            node,
            timer_id,
            fire_at,
        });
        self.queue
            .push(fire_at, seq, EventKind::Timer { node, timer_id });
    }
}

/// The simulation kernel: owns all nodes plus the [`KernelCore`] described
/// above. `P` is the (opaque, application-defined) message payload type.
pub struct Kernel<P> {
    core: KernelCore<P>,
    nodes: BTreeMap<NodeId, Box<dyn Node<P>>>,
}

impl<P: Payload> Kernel<P> {
    /// Build a new kernel, seeded deterministically from `seed`, using
    /// `scheduler` as the active network scheduler for the whole run.
    pub fn new(seed: u64, scheduler: SchedulerKind<P>) -> Self {
        Self {
            core: KernelCore {
                now: LogicalTime::ZERO,
                next_seq: 0,
                next_message_id: 0,
                rng: StdRng::seed_from_u64(seed),
                trace: Trace::new(),
                faults: FaultState::default(),
                scheduler,
                leader: None,
                queue: EventQueue::default(),
                in_flight: BTreeMap::new(),
            },
            nodes: BTreeMap::new(),
        }
    }

    /// Register a node under `id`. Must be called before the first message
    /// or timer addressed to it is dispatched (typically: before `run`).
    pub fn add_node(&mut self, id: NodeId, node: Box<dyn Node<P>>) {
        self.nodes.insert(id, node);
    }

    /// The current logical time.
    pub fn now(&self) -> LogicalTime {
        self.core.now
    }

    /// The recorded trace so far.
    pub fn trace(&self) -> &Trace {
        &self.core.trace
    }

    /// Set (or clear) the currently-designated leader. There is no
    /// consensus in Phase 0 — this is purely a hook adversary schedulers
    /// use to decide who to target, and a foundation for later phases.
    pub fn set_leader(&mut self, leader: Option<NodeId>) {
        self.core.leader = leader;
        let seq = self.core.bump_seq();
        self.core.trace.record(TraceEvent::LeaderChanged {
            time: self.core.now,
            seq,
            leader,
        });
    }

    /// Inject a message directly, bypassing any `Node` — useful for
    /// exercising the network/scheduler/fault-injection layers in
    /// isolation, and for kicking off a scenario's first message.
    pub fn inject_message(&mut self, src: NodeId, dst: NodeId, payload: P) {
        self.core.send(src, dst, payload);
    }

    /// Schedule a timer for `node` from outside any node callback (e.g. to
    /// kick off a demo/test scenario).
    pub fn inject_timer(&mut self, node: NodeId, after: u64, timer_id: TimerId) {
        self.core.schedule_timer(node, after, timer_id);
    }

    /// Crash `node`: it stops receiving messages and timers until
    /// `restart` is called. Idempotent.
    pub fn crash(&mut self, node: NodeId) {
        self.apply_fault_command(FaultCommand::Crash(node));
    }

    /// Restart `node`: fault state clears and `Node::on_restart` fires,
    /// giving it a chance to reset volatile state (or, in a later phase,
    /// recover durable state).
    pub fn restart(&mut self, node: NodeId) {
        self.apply_fault_command(FaultCommand::Restart(node));
    }

    /// Partition the cluster into two groups that cannot reach each other.
    /// Messages within a group are unaffected.
    pub fn partition(&mut self, group_a: BTreeSet<NodeId>, group_b: BTreeSet<NodeId>) {
        self.apply_fault_command(FaultCommand::Partition(group_a, group_b));
    }

    /// Remove any active manual partition.
    pub fn heal(&mut self) {
        self.apply_fault_command(FaultCommand::Heal);
    }

    /// Multiply message delay to/from `node` by `multiplier`.
    pub fn set_slow(&mut self, node: NodeId, multiplier: u64) {
        self.apply_fault_command(FaultCommand::SlowNode(node, multiplier));
    }

    /// Remove a previously-set slow-node multiplier.
    pub fn clear_slow(&mut self, node: NodeId) {
        self.apply_fault_command(FaultCommand::ClearSlow(node));
    }

    /// Schedule a fault to apply at a future logical time, so an entire
    /// scenario (messages *and* faults) can be expressed up front as data
    /// driven by the event queue rather than by imperative interleaving
    /// with `run_until`.
    pub fn schedule_fault(&mut self, at: LogicalTime, cmd: FaultCommand) {
        assert!(at >= self.core.now, "cannot schedule a fault in the past");
        let seq = self.core.bump_seq();
        self.core.queue.push(at, seq, EventKind::Fault(cmd));
    }

    fn apply_fault_command(&mut self, cmd: FaultCommand) {
        match cmd {
            FaultCommand::Crash(node) => {
                self.core.faults.crash(node);
                let seq = self.core.bump_seq();
                self.core.trace.record(TraceEvent::Crash {
                    time: self.core.now,
                    seq,
                    node,
                });
            }
            FaultCommand::Restart(node) => {
                self.core.faults.restart(node);
                let seq = self.core.bump_seq();
                self.core.trace.record(TraceEvent::Restart {
                    time: self.core.now,
                    seq,
                    node,
                });
                if let Some(mut n) = self.nodes.remove(&node) {
                    let mut ctx = NodeCtx {
                        core: &mut self.core,
                        self_id: node,
                    };
                    n.on_restart(&mut ctx);
                    self.nodes.insert(node, n);
                }
            }
            FaultCommand::Partition(a, b) => {
                self.core.faults.set_partition(a.clone(), b.clone());
                let seq = self.core.bump_seq();
                self.core.trace.record(TraceEvent::Partition {
                    time: self.core.now,
                    seq,
                    group_a: a.into_iter().collect(),
                    group_b: b.into_iter().collect(),
                });
            }
            FaultCommand::Heal => {
                self.core.faults.heal();
                let seq = self.core.bump_seq();
                self.core.trace.record(TraceEvent::Heal {
                    time: self.core.now,
                    seq,
                });
            }
            FaultCommand::SlowNode(node, multiplier) => {
                self.core.faults.set_slow(node, multiplier);
                let seq = self.core.bump_seq();
                self.core.trace.record(TraceEvent::SlowNode {
                    time: self.core.now,
                    seq,
                    node,
                    multiplier,
                });
            }
            FaultCommand::ClearSlow(node) => {
                self.core.faults.clear_slow(node);
                let seq = self.core.bump_seq();
                self.core.trace.record(TraceEvent::SlowNode {
                    time: self.core.now,
                    seq,
                    node,
                    multiplier: 1,
                });
            }
        }
    }

    /// Run the event loop until the queue is exhausted.
    pub fn run(&mut self) {
        self.run_until(LogicalTime(u64::MAX));
    }

    /// Run the event loop until `until` (inclusive): pops and dispatches
    /// events in `(time, seq)` order, stopping before dispatching any event
    /// whose time exceeds `until`.
    pub fn run_until(&mut self, until: LogicalTime) {
        loop {
            match self.core.queue.peek_time() {
                Some(t) if t <= until => {}
                _ => break,
            }
            let event = match self.core.queue.pop() {
                Some(e) => e,
                None => break,
            };
            self.core.now = event.time;
            let seq = event.seq;
            match event.kind {
                EventKind::Timer { node, timer_id } => self.dispatch_timer(node, timer_id, seq),
                EventKind::MessageArrival { id } => self.dispatch_message_arrival(id, seq),
                EventKind::Fault(cmd) => self.apply_fault_command(cmd),
            }
        }
    }

    fn dispatch_timer(&mut self, node: NodeId, timer_id: TimerId, seq: u64) {
        if self.core.faults.is_crashed(node) {
            self.core.trace.record(TraceEvent::TimerDropped {
                time: self.core.now,
                seq,
                node,
                timer_id,
            });
            return;
        }
        if let Some(mut n) = self.nodes.remove(&node) {
            self.core.trace.record(TraceEvent::TimerFired {
                time: self.core.now,
                seq,
                node,
                timer_id,
            });
            let mut ctx = NodeCtx {
                core: &mut self.core,
                self_id: node,
            };
            n.on_timer(timer_id, &mut ctx);
            self.nodes.insert(node, n);
        }
    }

    fn dispatch_message_arrival(&mut self, id: MessageId, seq: u64) {
        let envelope = match self.core.in_flight.remove(&id) {
            Some(e) => e,
            None => return,
        };
        let src = envelope.meta.src;
        let dst = envelope.meta.dst;

        if self.core.faults.is_crashed(dst) {
            self.core.trace.record(TraceEvent::Drop {
                time: self.core.now,
                seq,
                id,
                src,
                dst,
                reason: DropReason::Crashed,
            });
            return;
        }

        if let Some(mut n) = self.nodes.remove(&dst) {
            self.core.trace.record(TraceEvent::Deliver {
                time: self.core.now,
                seq,
                id,
                src,
                dst,
            });
            let mut ctx = NodeCtx {
                core: &mut self.core,
                self_id: dst,
            };
            n.on_message(src, envelope.payload, &mut ctx);
            self.nodes.insert(dst, n);
        }
        // No node registered at `dst`: the message is silently unclaimed.
        // (Its `Send` was already traced; no misleading `Deliver` is
        // recorded since nothing actually received it.)
    }
}
