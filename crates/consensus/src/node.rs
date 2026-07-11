//! The [`queso_sim::node::Node`] implementation each replica runs.
//!
//! A replica needs exactly two things from the kernel:
//!
//! 1. **Draw a priority from the kernel's single seeded PRNG stream.** The
//!    only way to reach that stream is [`queso_sim::node::Ctx::rng`],
//!    which is only available inside a `Node` callback -- so priority
//!    generation happens in `on_timer`, triggered by the driver injecting a
//!    zero-delay timer for every live replica at the start of each round.
//!    This keeps every draw of randomness anywhere in a run part of the
//!    kernel's one deterministic stream, consumed in kernel dispatch order
//!    (same contract `queso-sim` documents for its own scheduler draws).
//! 2. **Accumulate incoming tcast dissemination** for the current step into
//!    a mailbox.
//!
//! Both pieces of state are shared (`Rc<RefCell<..>>`, single-threaded --
//! see `queso_sim`'s crate docs on why this is sound here) with the
//! external driver in [`crate::algorithm`] and [`crate::tcast`], which is
//! the only thing that ever *reads* them; the pattern mirrors
//! `queso-sim`'s own `examples/echo_demo.rs`.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;

use rand::Rng;

use queso_sim::ids::{NodeId, TimerId};
use queso_sim::node::{Ctx, Node};

use crate::message::TcastMsg;
use crate::proposal::ProposalSet;

/// The timer id used to trigger a priority draw. This crate only ever
/// schedules one kind of timer, so a single constant id is enough.
pub const DRAW_PRIORITY_TIMER: TimerId = TimerId(0);

/// Accumulates, for the tcast step currently in progress, the proposal set
/// received from each sender so far this step. Cleared and reseeded with
/// the replica's own input at the start of every tcast call (see
/// [`crate::tcast::tcast`]).
#[derive(Debug)]
pub struct Mailbox<V> {
    pub received: BTreeMap<NodeId, ProposalSet<V>>,
}

impl<V> Default for Mailbox<V> {
    // Written by hand rather than derived: `#[derive(Default)]` would add
    // an unnecessary `V: Default` bound (an empty `BTreeMap` needs no such
    // bound on its value type).
    fn default() -> Self {
        Self {
            received: BTreeMap::new(),
        }
    }
}

/// One replica's `Node` implementation. Deliberately thin: all the
/// interesting per-round bookkeeping (current candidate value, decided
/// flag, round count) lives in [`crate::algorithm::ReplicaState`], owned
/// and driven externally by [`crate::algorithm::Cluster`] -- this type's
/// only job is to be the seam through which the kernel's message/timer
/// callbacks reach that externally-owned state.
pub struct ReplicaNode<V> {
    mailbox: Rc<RefCell<Mailbox<V>>>,
    drawn_priority: Rc<RefCell<Option<u64>>>,
}

impl<V> ReplicaNode<V> {
    pub fn new(mailbox: Rc<RefCell<Mailbox<V>>>, drawn_priority: Rc<RefCell<Option<u64>>>) -> Self {
        Self {
            mailbox,
            drawn_priority,
        }
    }
}

impl<V: Ord + Clone> Node<TcastMsg<V>> for ReplicaNode<V> {
    fn on_message(&mut self, from: NodeId, payload: TcastMsg<V>, _ctx: &mut dyn Ctx<TcastMsg<V>>) {
        // First writer wins for a given sender within a step: retries (see
        // `crate::tcast::tcast`) may resend the same sender's set more than
        // once, and since it's the same set every time, keeping the first
        // arrival is simplest and correct.
        self.mailbox
            .borrow_mut()
            .received
            .entry(from)
            .or_insert(payload.set);
    }

    fn on_timer(&mut self, timer_id: TimerId, ctx: &mut dyn Ctx<TcastMsg<V>>) {
        if timer_id == DRAW_PRIORITY_TIMER {
            let priority: u64 = ctx.rng().gen();
            *self.drawn_priority.borrow_mut() = Some(priority);
        }
    }

    fn on_restart(&mut self, _ctx: &mut dyn Ctx<TcastMsg<V>>) {
        self.mailbox.borrow_mut().received.clear();
        *self.drawn_priority.borrow_mut() = None;
    }
}
