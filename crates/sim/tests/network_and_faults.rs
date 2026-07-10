//! Integration tests for the kernel: network delivery, fault injection, and
//! the trace recorder, exercised end to end through `Kernel` rather than
//! against individual modules in isolation (see the unit tests inside
//! `src/scheduler.rs`, `src/fault.rs`, `src/trace.rs`, `src/time.rs` for
//! the component-level coverage).

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;

use queso_sim::fault::DropReason;
use queso_sim::ids::{NodeId, TimerId};
use queso_sim::node::{Node, NodeCtx};
use queso_sim::scheduler::{Fifo, RandomScheduler, SchedulerKind};
use queso_sim::trace::TraceEvent;
use queso_sim::Kernel;

/// A node that just records every payload it receives, in delivery order.
struct Sink {
    log: Rc<RefCell<Vec<u64>>>,
    restarts: Rc<RefCell<u32>>,
}

impl Node<u64> for Sink {
    fn on_message(&mut self, _from: NodeId, payload: u64, _ctx: &mut NodeCtx<'_, u64>) {
        self.log.borrow_mut().push(payload);
    }

    fn on_timer(&mut self, _timer_id: TimerId, _ctx: &mut NodeCtx<'_, u64>) {}

    fn on_restart(&mut self, _ctx: &mut NodeCtx<'_, u64>) {
        *self.restarts.borrow_mut() += 1;
    }
}

fn fifo_kernel(delay: u64) -> Kernel<u64> {
    Kernel::new(1, SchedulerKind::Oblivious(Box::new(Fifo::new(delay))))
}

#[test]
fn fifo_preserves_send_order_on_a_single_link() {
    let mut kernel = fifo_kernel(3);
    let log = Rc::new(RefCell::new(Vec::new()));
    kernel.add_node(
        NodeId(1),
        Box::new(Sink {
            log: log.clone(),
            restarts: Rc::new(RefCell::new(0)),
        }),
    );

    for v in 0..20u64 {
        kernel.inject_message(NodeId(0), NodeId(1), v);
    }
    kernel.run();

    assert_eq!(*log.borrow(), (0..20u64).collect::<Vec<_>>());
}

#[test]
fn network_delivery_records_send_and_deliver_with_matching_ids() {
    let mut kernel = fifo_kernel(2);
    kernel.add_node(
        NodeId(1),
        Box::new(Sink {
            log: Rc::new(RefCell::new(Vec::new())),
            restarts: Rc::new(RefCell::new(0)),
        }),
    );
    kernel.inject_message(NodeId(0), NodeId(1), 99);
    kernel.run();

    let events = kernel.trace().events();
    let send = events
        .iter()
        .find(|e| matches!(e, TraceEvent::Send { .. }))
        .expect("a Send event");
    let deliver = events
        .iter()
        .find(|e| matches!(e, TraceEvent::Deliver { .. }))
        .expect("a Deliver event");

    let (
        TraceEvent::Send {
            id: send_id,
            time: send_time,
            ..
        },
        TraceEvent::Deliver {
            id: deliver_id,
            time: deliver_time,
            ..
        },
    ) = (send, deliver)
    else {
        unreachable!()
    };
    assert_eq!(send_id, deliver_id);
    assert_eq!(*deliver_time, send_time.advance(2));
}

#[test]
fn crashed_node_drops_messages_and_stops_receiving() {
    let mut kernel = fifo_kernel(1);
    let log = Rc::new(RefCell::new(Vec::new()));
    kernel.add_node(
        NodeId(1),
        Box::new(Sink {
            log: log.clone(),
            restarts: Rc::new(RefCell::new(0)),
        }),
    );

    kernel.crash(NodeId(1));
    kernel.inject_message(NodeId(0), NodeId(1), 1);
    kernel.run();

    assert!(
        log.borrow().is_empty(),
        "crashed node must not receive messages"
    );
    let dropped = kernel.trace().events().iter().any(|e| {
        matches!(
            e,
            TraceEvent::Drop {
                reason: DropReason::Crashed,
                ..
            }
        )
    });
    assert!(dropped, "expected a Drop{{reason: Crashed}} trace event");
}

#[test]
fn restart_clears_crash_and_fires_on_restart_hook() {
    let mut kernel = fifo_kernel(1);
    let log = Rc::new(RefCell::new(Vec::new()));
    let restarts = Rc::new(RefCell::new(0));
    kernel.add_node(
        NodeId(1),
        Box::new(Sink {
            log: log.clone(),
            restarts: restarts.clone(),
        }),
    );

    kernel.crash(NodeId(1));
    kernel.restart(NodeId(1));
    assert_eq!(*restarts.borrow(), 1, "on_restart should fire exactly once");

    kernel.inject_message(NodeId(0), NodeId(1), 42);
    kernel.run();
    assert_eq!(
        *log.borrow(),
        vec![42],
        "node should receive messages again after restart"
    );
}

#[test]
fn partition_blocks_cross_group_traffic_and_heal_restores_it() {
    let mut kernel = fifo_kernel(1);
    let log = Rc::new(RefCell::new(Vec::new()));
    kernel.add_node(
        NodeId(1),
        Box::new(Sink {
            log: log.clone(),
            restarts: Rc::new(RefCell::new(0)),
        }),
    );

    kernel.partition(BTreeSet::from([NodeId(0)]), BTreeSet::from([NodeId(1)]));
    kernel.inject_message(NodeId(0), NodeId(1), 1);
    kernel.run();
    assert!(
        log.borrow().is_empty(),
        "partitioned traffic must be dropped"
    );
    let partition_drop = kernel.trace().events().iter().any(|e| {
        matches!(
            e,
            TraceEvent::Drop {
                reason: DropReason::Partitioned,
                ..
            }
        )
    });
    assert!(partition_drop);

    kernel.heal();
    kernel.inject_message(NodeId(0), NodeId(1), 2);
    kernel.run();
    assert_eq!(
        *log.borrow(),
        vec![2],
        "traffic should flow again after heal"
    );
}

#[test]
fn partition_installed_after_send_still_drops_the_in_flight_message() {
    // The message goes out before any partition exists (so it passes the
    // send-time fault check and is queued for arrival), then a partition is
    // installed between src and dst *before* the message's arrival tick.
    // Delivery must still be cut off -- "partition = network cut" has to
    // hold for messages already in flight, not just future sends.
    let mut kernel = fifo_kernel(5);
    let log = Rc::new(RefCell::new(Vec::new()));
    kernel.add_node(
        NodeId(1),
        Box::new(Sink {
            log: log.clone(),
            restarts: Rc::new(RefCell::new(0)),
        }),
    );

    kernel.inject_message(NodeId(0), NodeId(1), 7); // arrives at t=5
    kernel.partition(BTreeSet::from([NodeId(0)]), BTreeSet::from([NodeId(1)])); // installed at t=0, before arrival
    kernel.run();

    assert!(
        log.borrow().is_empty(),
        "message already in flight when the partition was installed must not be delivered"
    );
    let arrival_drop = kernel.trace().events().iter().any(|e| {
        matches!(
            e,
            TraceEvent::Drop {
                reason: DropReason::PartitionedAtArrival,
                ..
            }
        )
    });
    assert!(
        arrival_drop,
        "expected a Drop{{reason: PartitionedAtArrival}} trace event"
    );
    // And there is no Deliver event at all for this message.
    assert!(
        !kernel
            .trace()
            .events()
            .iter()
            .any(|e| matches!(e, TraceEvent::Deliver { .. })),
        "no Deliver event should have been recorded"
    );
}

#[test]
fn sender_crash_after_send_does_not_prevent_delivery() {
    // Deliberate asymmetry: unlike a partition, a sender crashing after the
    // message was already sent does NOT retract the in-flight message in
    // this crash-stop model.
    let mut kernel = fifo_kernel(5);
    let log = Rc::new(RefCell::new(Vec::new()));
    kernel.add_node(
        NodeId(1),
        Box::new(Sink {
            log: log.clone(),
            restarts: Rc::new(RefCell::new(0)),
        }),
    );

    kernel.inject_message(NodeId(0), NodeId(1), 11); // arrives at t=5
    kernel.crash(NodeId(0)); // src crashes after the message is already in flight
    kernel.run();

    assert_eq!(
        *log.borrow(),
        vec![11],
        "an in-flight message must survive its sender crashing afterwards"
    );
}

#[test]
fn slow_node_multiplies_delivery_delay() {
    let mut fast = fifo_kernel(2);
    fast.add_node(
        NodeId(1),
        Box::new(Sink {
            log: Rc::new(RefCell::new(Vec::new())),
            restarts: Rc::new(RefCell::new(0)),
        }),
    );
    fast.inject_message(NodeId(0), NodeId(1), 1);
    fast.run();

    let mut slow = fifo_kernel(2);
    slow.add_node(
        NodeId(1),
        Box::new(Sink {
            log: Rc::new(RefCell::new(Vec::new())),
            restarts: Rc::new(RefCell::new(0)),
        }),
    );
    slow.set_slow(NodeId(1), 5);
    slow.inject_message(NodeId(0), NodeId(1), 1);
    slow.run();

    let fast_deliver_time = fast
        .trace()
        .events()
        .iter()
        .find_map(|e| match e {
            TraceEvent::Deliver { time, .. } => Some(*time),
            _ => None,
        })
        .unwrap();
    let slow_deliver_time = slow
        .trace()
        .events()
        .iter()
        .find_map(|e| match e {
            TraceEvent::Deliver { time, .. } => Some(*time),
            _ => None,
        })
        .unwrap();

    // base delay 2, multiplier 5 => 10 vs 2.
    assert_eq!(fast_deliver_time.0, 2);
    assert_eq!(slow_deliver_time.0, 10);
}

#[test]
fn trace_records_every_externally_visible_event_kind_in_a_mixed_scenario() {
    let mut kernel = fifo_kernel(1);
    kernel.add_node(
        NodeId(1),
        Box::new(Sink {
            log: Rc::new(RefCell::new(Vec::new())),
            restarts: Rc::new(RefCell::new(0)),
        }),
    );

    kernel.set_leader(Some(NodeId(0)));
    kernel.inject_message(NodeId(0), NodeId(1), 1); // -> Send, Deliver
    kernel.crash(NodeId(1)); // -> Crash
    kernel.inject_message(NodeId(0), NodeId(1), 2); // -> Send, Drop(Crashed)
    kernel.restart(NodeId(1)); // -> Restart
    kernel.partition(BTreeSet::from([NodeId(0)]), BTreeSet::from([NodeId(1)])); // -> Partition
    kernel.inject_message(NodeId(0), NodeId(1), 3); // -> Send, Drop(Partitioned)
    kernel.heal(); // -> Heal
    kernel.set_slow(NodeId(1), 2); // -> SlowNode
    kernel.inject_timer(NodeId(1), 1, TimerId(0)); // -> TimerScheduled, TimerFired
    kernel.run();

    let events = kernel.trace().events();
    let has = |pred: &dyn Fn(&TraceEvent) -> bool| events.iter().any(pred);

    assert!(has(&|e| matches!(e, TraceEvent::Send { .. })));
    assert!(has(&|e| matches!(e, TraceEvent::Deliver { .. })));
    assert!(has(&|e| matches!(
        e,
        TraceEvent::Drop {
            reason: DropReason::Crashed,
            ..
        }
    )));
    assert!(has(&|e| matches!(
        e,
        TraceEvent::Drop {
            reason: DropReason::Partitioned,
            ..
        }
    )));
    assert!(has(&|e| matches!(e, TraceEvent::Crash { .. })));
    assert!(has(&|e| matches!(e, TraceEvent::Restart { .. })));
    assert!(has(&|e| matches!(e, TraceEvent::Partition { .. })));
    assert!(has(&|e| matches!(e, TraceEvent::Heal { .. })));
    assert!(has(&|e| matches!(e, TraceEvent::SlowNode { .. })));
    assert!(has(&|e| matches!(e, TraceEvent::LeaderChanged { .. })));
    assert!(has(&|e| matches!(e, TraceEvent::TimerScheduled { .. })));
    assert!(has(&|e| matches!(e, TraceEvent::TimerFired { .. })));
}

#[test]
fn kernel_level_prng_stream_is_deterministic_given_the_same_seed() {
    fn run(seed: u64) -> Vec<u64> {
        let mut kernel = Kernel::new(
            seed,
            SchedulerKind::Oblivious(Box::new(RandomScheduler::new(1, 50))),
        );
        let log = Rc::new(RefCell::new(Vec::new()));
        kernel.add_node(
            NodeId(1),
            Box::new(Sink {
                log: log.clone(),
                restarts: Rc::new(RefCell::new(0)),
            }),
        );
        for v in 0..30u64 {
            kernel.inject_message(NodeId(0), NodeId(1), v);
        }
        kernel.run();
        kernel
            .trace()
            .events()
            .iter()
            .filter_map(|e| match e {
                TraceEvent::Deliver { time, .. } => Some(time.0),
                _ => None,
            })
            .collect()
    }

    let a = run(2024);
    let b = run(2024);
    let c = run(99);
    assert_eq!(
        a, b,
        "same seed must reproduce the exact same arrival-time sequence"
    );
    assert_ne!(
        a, c,
        "a different seed should (overwhelmingly likely) differ"
    );
}
