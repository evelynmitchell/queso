//! A tiny Phase-0 demo: no consensus, just N nodes forwarding/echoing
//! messages around a ring under an adversarial scheduler, with a couple of
//! faults injected mid-run. Prints a summary of the resulting (fully
//! deterministic, replayable) trace.
//!
//! Run it with:
//!
//! ```sh
//! cargo run -p queso-sim --example echo_demo
//! ```
//!
//! Run it twice and diff the "trace digest" lines: they are identical,
//! because everything here is seeded. That's the whole point of Phase 0.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;

use queso_sim::fault::DropReason;
use queso_sim::ids::{NodeId, TimerId};
use queso_sim::node::{Node, NodeCtx};
use queso_sim::payload::{Inspectable, Payload};
use queso_sim::scheduler::{ContentObliviousAdversary, SchedulerKind};
use queso_sim::time::LogicalTime;
use queso_sim::trace::TraceEvent;
use queso_sim::Kernel;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Msg {
    Ping(u64),
    Pong(u64),
}

impl Payload for Msg {
    fn size(&self) -> usize {
        16
    }
}

impl Inspectable for Msg {
    fn tag(&self) -> &'static str {
        match self {
            Msg::Ping(_) => "ping",
            Msg::Pong(_) => "pong",
        }
    }
}

/// Forwards `Ping(r)` around a ring of nodes while `r` is below
/// `max_round`, and echoes a `Pong` back to whoever sent it a `Ping`.
struct EchoNode {
    id: NodeId,
    ring_size: u32,
    max_round: u64,
    seen: Rc<RefCell<u32>>,
}

impl Node<Msg> for EchoNode {
    fn on_message(&mut self, from: NodeId, payload: Msg, ctx: &mut NodeCtx<'_, Msg>) {
        *self.seen.borrow_mut() += 1;
        if let Msg::Ping(round) = payload {
            ctx.send(from, Msg::Pong(round));
            if round + 1 < self.max_round {
                let next = NodeId((self.id.0 + 1) % self.ring_size);
                ctx.send(next, Msg::Ping(round + 1));
            }
        }
    }

    fn on_timer(&mut self, _timer_id: TimerId, _ctx: &mut NodeCtx<'_, Msg>) {}

    fn on_restart(&mut self, _ctx: &mut NodeCtx<'_, Msg>) {
        println!("  node {} restarted (volatile state cleared)", self.id);
    }
}

const RING_SIZE: u32 = 6;
const MAX_ROUND: u64 = 8;
const SEED: u64 = 20260710;

fn build_and_run(seed: u64) -> queso_sim::trace::Trace {
    let scheduler = ContentObliviousAdversary::new(1, 4)
        .with_drop_probability(0.05)
        .with_leader_dos(0.2);
    let mut kernel = Kernel::new(seed, SchedulerKind::Oblivious(Box::new(scheduler)));

    let mut seen_counters = Vec::new();
    for i in 0..RING_SIZE {
        let seen = Rc::new(RefCell::new(0));
        seen_counters.push(seen.clone());
        kernel.add_node(
            NodeId(i),
            Box::new(EchoNode {
                id: NodeId(i),
                ring_size: RING_SIZE,
                max_round: MAX_ROUND,
                seen,
            }),
        );
    }

    // Node 0 is the leader; the adversary will pile extra drop pressure on
    // its traffic (see `with_leader_dos` above).
    kernel.set_leader(Some(NodeId(0)));

    // Crash node 3 partway through, then bring it back; also cut nodes
    // {0, 1} off from {2, 3, 4, 5} for a stretch before healing.
    kernel.schedule_fault(
        LogicalTime(4),
        queso_sim::fault::FaultCommand::Crash(NodeId(3)),
    );
    kernel.schedule_fault(
        LogicalTime(12),
        queso_sim::fault::FaultCommand::Restart(NodeId(3)),
    );
    kernel.schedule_fault(
        LogicalTime(15),
        queso_sim::fault::FaultCommand::Partition(
            BTreeSet::from([NodeId(0), NodeId(1)]),
            BTreeSet::from([NodeId(2), NodeId(3), NodeId(4), NodeId(5)]),
        ),
    );
    kernel.schedule_fault(LogicalTime(25), queso_sim::fault::FaultCommand::Heal);

    kernel.inject_message(NodeId(0), NodeId(1), Msg::Ping(0));
    kernel.run();

    println!("Phase-0 echo demo -- seed {seed}");
    println!("  ring size: {RING_SIZE}, max round: {MAX_ROUND}");
    for (i, seen) in seen_counters.iter().enumerate() {
        println!("  node {i} handled {} messages", seen.borrow());
    }

    let events = kernel.trace().events();
    let count = |pred: fn(&TraceEvent) -> bool| events.iter().filter(|e| pred(e)).count();
    println!("  trace: {} events total", events.len());
    println!(
        "    Send      : {}",
        count(|e| matches!(e, TraceEvent::Send { .. }))
    );
    println!(
        "    Deliver   : {}",
        count(|e| matches!(e, TraceEvent::Deliver { .. }))
    );
    println!(
        "    Drop(sched): {}",
        count(|e| matches!(
            e,
            TraceEvent::Drop {
                reason: DropReason::Scheduler,
                ..
            }
        ))
    );
    println!(
        "    Drop(crash): {}",
        count(|e| matches!(
            e,
            TraceEvent::Drop {
                reason: DropReason::Crashed,
                ..
            }
        ))
    );
    println!(
        "    Drop(part) : {}",
        count(|e| matches!(
            e,
            TraceEvent::Drop {
                reason: DropReason::Partitioned,
                ..
            }
        ))
    );
    println!(
        "    Crash/Restart/Partition/Heal: {}",
        count(|e| matches!(
            e,
            TraceEvent::Crash { .. }
                | TraceEvent::Restart { .. }
                | TraceEvent::Partition { .. }
                | TraceEvent::Heal { .. }
        ))
    );

    // A short, stable digest so two runs can be eyeballed for equality
    // without printing the whole (possibly large) trace.
    let bytes = kernel.trace().to_canonical_bytes();
    let digest = bytes.iter().fold(0xcbf29ce484222325u64, |h, &b| {
        (h ^ b as u64).wrapping_mul(0x100000001b3)
    });
    println!("  trace digest (fnv-1a over canonical bytes): {digest:016x}");

    kernel.trace().clone()
}

fn main() {
    let trace_a = build_and_run(SEED);
    println!();
    let trace_b = build_and_run(SEED);

    assert_eq!(
        trace_a.to_canonical_bytes(),
        trace_b.to_canonical_bytes(),
        "same seed produced different traces -- determinism is broken!"
    );
    println!();
    println!("Replayed with the same seed: traces are byte-for-byte identical.");
}
