//! **The Phase-0 acceptance gate.**
//!
//! Property D9 (`docs/02-properties.md`): any run is exactly replayable
//! from its seed. This test runs the same scenario twice, with the same
//! seed, under each of the four schedulers (including both adversary
//! classes) plus a battery of scripted faults, and asserts the two
//! recorded traces are byte-for-byte identical. If this test is ever
//! flaky, determinism is broken and the entire DST pillar
//! (`docs/03-testing-plan.md`) is unsound — see the crate README for what
//! guarantees this test is standing on.

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;

use queso_sim::fault::FaultCommand;
use queso_sim::ids::{NodeId, TimerId};
use queso_sim::node::{Node, NodeCtx};
use queso_sim::payload::{Inspectable, Payload};
use queso_sim::scheduler::{
    ContentAwareAdversary, ContentObliviousAdversary, Fifo, RandomScheduler, SchedulerKind,
};
use queso_sim::time::LogicalTime;
use queso_sim::trace::Trace;
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

/// Forwards `Ping(r)` around a ring while `r < max_round`, and always
/// echoes a `Pong` back to whoever sent it a `Ping`. No consensus — just
/// enough behavior to generate a non-trivial, fault-sensitive trace.
struct EchoNode {
    id: NodeId,
    ring_size: u32,
    max_round: u64,
    received: Rc<RefCell<Vec<(NodeId, Msg)>>>,
}

impl Node<Msg> for EchoNode {
    fn on_message(&mut self, from: NodeId, payload: Msg, ctx: &mut NodeCtx<'_, Msg>) {
        self.received.borrow_mut().push((from, payload.clone()));
        if let Msg::Ping(round) = payload {
            ctx.send(from, Msg::Pong(round));
            if round + 1 < self.max_round {
                let next = NodeId((self.id.0 + 1) % self.ring_size);
                ctx.send(next, Msg::Ping(round + 1));
            }
        }
    }

    fn on_timer(&mut self, _timer_id: TimerId, _ctx: &mut NodeCtx<'_, Msg>) {}
}

const RING_SIZE: u32 = 5;
const MAX_ROUND: u64 = 6;

fn make_scheduler(name: &str) -> SchedulerKind<Msg> {
    match name {
        "fifo" => SchedulerKind::Oblivious(Box::new(Fifo::new(2))),
        "random" => SchedulerKind::Oblivious(Box::new(RandomScheduler::new(1, 6))),
        "content_oblivious_adversary" => SchedulerKind::Oblivious(Box::new(
            ContentObliviousAdversary::new(1, 8)
                .with_drop_probability(0.15)
                .with_leader_dos(0.3)
                .with_minority_partition([NodeId(4)]),
        )),
        "content_aware_adversary" => SchedulerKind::Aware(Box::new(
            ContentAwareAdversary::<Msg>::new(1, 8).with_drop_probability(0.1),
        )),
        other => panic!("unknown scheduler {other}"),
    }
}

/// Build and run one full scenario: nodes, a scripted fault plan, and an
/// initial message, then hand back the recorded trace.
fn run_scenario(seed: u64, scheduler_name: &str) -> Trace {
    let mut kernel = Kernel::new(seed, make_scheduler(scheduler_name));

    for i in 0..RING_SIZE {
        let node = EchoNode {
            id: NodeId(i),
            ring_size: RING_SIZE,
            max_round: MAX_ROUND,
            received: Rc::new(RefCell::new(Vec::new())),
        };
        kernel.add_node(NodeId(i), Box::new(node));
    }

    kernel.set_leader(Some(NodeId(0)));

    // The whole fault plan is scripted up front as scheduled events, so the
    // scenario is pure data driven off the seed -- no reliance on wall-clock
    // or external interleaving to reproduce it.
    kernel.schedule_fault(LogicalTime(5), FaultCommand::Crash(NodeId(2)));
    kernel.schedule_fault(LogicalTime(15), FaultCommand::Restart(NodeId(2)));
    kernel.schedule_fault(
        LogicalTime(20),
        FaultCommand::Partition(
            BTreeSet::from([NodeId(0), NodeId(1)]),
            BTreeSet::from([NodeId(2), NodeId(3), NodeId(4)]),
        ),
    );
    kernel.schedule_fault(LogicalTime(30), FaultCommand::Heal);
    kernel.schedule_fault(LogicalTime(8), FaultCommand::SlowNode(NodeId(3), 4));

    kernel.inject_message(NodeId(0), NodeId(1), Msg::Ping(0));
    kernel.run();
    kernel.trace().clone()
}

const SCHEDULERS: &[&str] = &[
    "fifo",
    "random",
    "content_oblivious_adversary",
    "content_aware_adversary",
];

#[test]
fn same_seed_produces_byte_for_byte_identical_traces() {
    for &name in SCHEDULERS {
        let seed = 0xC0FFEE ^ (name.len() as u64);
        let trace_a = run_scenario(seed, name);
        let trace_b = run_scenario(seed, name);

        assert_eq!(
            trace_a, trace_b,
            "scheduler `{name}`: traces differ as Rust values for the same seed"
        );
        assert_eq!(
            trace_a.to_canonical_bytes(),
            trace_b.to_canonical_bytes(),
            "scheduler `{name}`: traces differ byte-for-byte for the same seed"
        );
        assert!(
            !trace_a.events().is_empty(),
            "scheduler `{name}`: scenario produced an empty trace, test is vacuous"
        );
    }
}

#[test]
fn different_seeds_produce_different_traces() {
    // Sanity check that the scenario is actually seed-sensitive (otherwise
    // the equality test above would pass trivially) -- for the schedulers
    // that actually consume randomness. `fifo` is deliberately excluded:
    // it never draws from the PRNG (fixed delay, no drops), so it is
    // legitimately seed-independent, and that's fine.
    for &name in &[
        "random",
        "content_oblivious_adversary",
        "content_aware_adversary",
    ] {
        let trace_a = run_scenario(1, name);
        let trace_b = run_scenario(2, name);
        assert_ne!(
            trace_a.to_canonical_bytes(),
            trace_b.to_canonical_bytes(),
            "scheduler `{name}`: different seeds produced identical traces"
        );
    }
}

#[test]
fn reproducibility_holds_across_many_seeds_under_the_content_oblivious_adversary() {
    // The adversary class under which randomized-liveness guarantees would
    // eventually be asserted (P14/P15, once consensus exists) is the one
    // most worth hammering here.
    for seed in 0..25u64 {
        let a = run_scenario(seed, "content_oblivious_adversary");
        let b = run_scenario(seed, "content_oblivious_adversary");
        assert_eq!(
            a.to_canonical_bytes(),
            b.to_canonical_bytes(),
            "seed {seed} was not reproducible"
        );
    }
}
