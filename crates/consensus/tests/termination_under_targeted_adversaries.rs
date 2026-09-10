//! P14 under adversaries that **target** rather than perturb uniformly
//! (#150), and the record of a falsifier hunt that did not find one.
//!
//! # Why this file exists
//!
//! #125 measured P14's detection power and got a zero: replacing the drawn
//! priority with a constant kills 0 of 441 tests. It offered a conjecture
//! for what would detect it -- a *content-aware* adversary, since
//! `termination.rs` and `concrete_termination.rs` both run
//! `ContentObliviousAdversary`, the class A3 names and the class the paper's
//! ">= 1/2 per round" theorem is stated against.
//!
//! #150 tested that conjecture. **It is backwards.** A content-aware
//! adversary does separate the two builds -- in the direction that makes
//! the *randomized* build the slower one.
//!
//! # What was searched
//!
//! Every arm below was run against both the unmutated build and the
//! constant-priority mutation (C1: `draw_priority` returns `1`, draw still
//! consumed), n ∈ {3, 5, 7}, single leaderless slot, no crashes.
//!
//! | Adversary | Sees | Real build | C1 |
//! |---|---|---|---|
//! | `DelayTop` — hold the top-origin node's messages back | metadata | 1.000 rounds | 1.000 |
//! | `SkewTop` — hold them back toward half the cluster | metadata | 1.35–1.41 | **1.000** |
//! | `Rotate` — hold a different node back per message | metadata | 1.21–1.33 | **1.000** |
//! | `SkewTheMax` — hold the *highest-priority* request back toward half | payload | 1.29–1.43, max round 4 | **1.000, max 1** |
//!
//! 100 seeds per cell, two delay magnitudes (50 and 5 000 ticks) — the
//! magnitude changed nothing in any arm. In every legal configuration the
//! constant-priority build converged **at least as fast** as the randomized
//! one, and usually strictly faster. Not one arm made it slower, which is
//! what a falsifier would require.
//!
//! # The arm that "worked", and why it is not a falsifier
//!
//! One arm did leave C1 undecided in 200/200 seeds: a two-sided *drop*
//! split (top-origin node cut from the lower half, second-from-top cut from
//! the upper half). It leaves the **unmutated build undecided in 200/200
//! seeds too**. It is not an adversary that exploits determinism; it is a
//! partition that violates P13's precondition that a majority can
//! communicate. Recorded here because it looks like a result until you run
//! the control -- which is the one step #125's method notes exist to force.
//!
//! # Why the direction is backwards, mechanically
//!
//! With equal priorities, `Proposal::Ord` (priority, then origin, then
//! value) still yields a *unique* maximum in any set, so `best` stays a
//! deterministic function of the set a recorder received and convergence
//! needs no unpredictability at all. Randomization is what lets recorders
//! disagree about the maximum in the first place, which costs the extra
//! rounds the tables above show. It buys safety against an adversary that
//! can *adapt to the draw* -- and an adversary that reads priorities off
//! the wire is exactly that, which is why it hurts the randomized build and
//! not the constant one. A3 assumes private channels precisely so that
//! adversary is out of scope.
//!
//! # What this does and does not establish
//!
//! *Measured:* the four arms above, at the seed counts and bounds stated.
//! *Not established:* that **no** adversary separates them. The strategy
//! space is unbounded and this search was not exhaustive — four strategies,
//! two delay magnitudes, three cluster sizes, one slot, no crashes, no
//! hedging, leaderless. A falsifier may exist outside that box; what #150
//! establishes is that the obvious candidates are not it, and that the
//! conjectured *direction* is wrong.
//!
//! # The tests below
//!
//! They pin the **real** build's termination under targeted adversaries,
//! which nothing else in the tree does (`fast_path.rs` uses an aware
//! scheduler, but for D1's fast-path fallback, not termination). Their
//! detection power against C1 is **measured, and it is zero** — stated on
//! each test. They are a regression guard for targeted-adversary
//! termination and a record of the search, not evidence for P14.

use std::collections::{BTreeMap, BTreeSet};

use queso_consensus::{ConcreteCluster, ConcreteMsg};
use queso_sim::ids::NodeId;
use queso_sim::network::{Envelope, EnvelopeMeta};
use queso_sim::scheduler::{
    AwareScheduler, Decision, ObliviousScheduler, SchedulerCtx, SchedulerKind,
};

const SEEDS: u64 = 50;
const TICKS: u64 = 200_000;
/// Long enough that a held-back message lands well after the quorum it was
/// racing; short enough to stay inside `TICKS`.
const SLOW: u64 = 5_000;

fn initial_values(n: u32) -> BTreeMap<NodeId, u32> {
    (0..n).map(|i| (NodeId(i), i + 1)).collect()
}

/// Metadata-only: hold the top-origin node's messages back toward half the
/// cluster. Delay, never drop -- a drop-based version of this is what
/// breaks both builds (see the module docs), because a permanent cut takes
/// the cluster outside P13's envelope rather than exploiting anything.
#[derive(Debug)]
struct SkewTopOrigin {
    top: NodeId,
    half: BTreeSet<NodeId>,
}

impl ObliviousScheduler for SkewTopOrigin {
    fn on_send(&mut self, meta: &EnvelopeMeta, _ctx: &mut SchedulerCtx<'_>) -> Decision {
        let delay = if meta.src == self.top && self.half.contains(&meta.dst) {
            SLOW
        } else {
            1
        };
        Decision::Deliver { delay }
    }
}

/// Payload-reading: hold the highest-priority request seen so far back
/// toward half the cluster. Under randomization this targets whoever drew
/// highest -- a different proposer each round; under constant priorities it
/// targets the same node forever.
#[derive(Debug)]
struct SkewTheMaxPriority {
    half: BTreeSet<NodeId>,
    seen_max: u64,
}

impl AwareScheduler<ConcreteMsg<u32>> for SkewTheMaxPriority {
    fn on_send(
        &mut self,
        envelope: &Envelope<ConcreteMsg<u32>>,
        _ctx: &mut SchedulerCtx<'_>,
    ) -> Decision {
        if let ConcreteMsg::Request(req) = &envelope.payload {
            if req.proposal.priority >= self.seen_max {
                self.seen_max = req.proposal.priority;
                if self.half.contains(&envelope.meta.dst) {
                    return Decision::Deliver { delay: SLOW };
                }
            }
        }
        Decision::Deliver { delay: 1 }
    }
}

fn worst_round(c: &ConcreteCluster<u32>) -> u64 {
    c.live().iter().map(|&id| c.step(id) / 4).max().unwrap_or(0)
}

/// Termination survives an adversary that singles out one proposer by node
/// id and delivers its messages late to half the cluster (P14/P13).
///
/// Falsifier, run: **none known, and C1 measured at zero** (#150). Under
/// the constant-priority mutation every replica decides in round 1 across
/// all of n ∈ {3,5,7} — strictly *better* than the unmutated build's
/// 1.35–1.41 mean. A one-sided bound on rounds cannot detect a defect that
/// improves the statistic; see the module docs for the four arms searched.
#[test]
fn termination_survives_an_adversary_targeting_one_proposer_by_id() {
    for n in [3u32, 5, 7] {
        let half: BTreeSet<NodeId> = (0..n / 2).map(NodeId).collect();
        for seed in 0..SEEDS {
            let sched = SkewTopOrigin {
                top: NodeId(n - 1),
                half: half.clone(),
            };
            let mut c = ConcreteCluster::new(
                seed,
                SchedulerKind::Oblivious(Box::new(sched)),
                initial_values(n),
            );
            c.run_slot(TICKS);
            assert!(
                c.all_live_decided(),
                "n={n} seed={seed}: a metadata adversary that singles out one \
                 proposer must not prevent termination (P14) -- it delays \
                 messages and never drops them, so the cluster stays well \
                 inside P13's connectivity envelope"
            );
        }
    }
}

/// Termination survives an adversary that reads proposal priorities off the
/// wire and holds the current maximum back from half the cluster.
///
/// This is the adversary #125 conjectured would falsify P14. It does not:
/// it costs the *unmutated* build rounds (up to round 4 at n=7) and costs
/// the constant-priority mutation nothing at all.
///
/// Falsifier, run: **none known, and C1 measured at zero** (#150) — 0/100
/// seeds per n, every replica deciding in round 1 under the mutation.
/// A3 puts this adversary out of scope (private channels); it is tested
/// anyway because the conjecture named it, and because "the assumption
/// excludes it" is a better answer when someone has actually run it.
#[test]
fn termination_survives_an_adversary_reading_priorities_off_the_wire() {
    for n in [3u32, 5, 7] {
        let half: BTreeSet<NodeId> = (0..n / 2).map(NodeId).collect();
        for seed in 0..SEEDS {
            let sched = SkewTheMaxPriority {
                half: half.clone(),
                seen_max: 0,
            };
            let mut c = ConcreteCluster::new(
                seed,
                SchedulerKind::Aware(Box::new(sched)),
                initial_values(n),
            );
            c.run_slot(TICKS);
            assert!(
                c.all_live_decided(),
                "n={n} seed={seed}: an adversary that can see priorities may \
                 cost rounds, but must not prevent termination"
            );
            assert!(
                worst_round(&c) <= 8,
                "n={n} seed={seed}: rounds escalated to {} under a \
                 priority-aware adversary -- the searched arms never exceeded \
                 4, so this is a change worth explaining, not a threshold to \
                 raise",
                worst_round(&c)
            );
        }
    }
}
