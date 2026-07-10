//! Threshold synchronous broadcast (`tcast`), §4.1.1 of the paper.
//!
//! # What tcast must guarantee
//!
//! At each call, every live replica `i` invokes `tcast(P_i)` with a
//! proposal set it wants to disseminate. The call returns `(R_i, B_i)`
//! such that:
//!
//! 1. **`R` majority property.** `R_i` includes the inputs of some majority
//!    `S` of replicas: `|S| > n/2` and `∀ j ∈ S, P_j ⊆ R_i`.
//! 2. **`B` universal property.** `B_i` equals some replica `j`'s input
//!    `P_j`, and that same `P_j` is included in *every* live replica's `R`:
//!    `∀ k, P_j ⊆ R_k`. As the paper notes, this makes `B` itself the same
//!    value handed back to every replica (it's literally one fixed `P_j`),
//!    so `∀ i, k: B_i ⊆ R_k`.
//!
//! # How this is realized on the harness
//!
//! §4.1.1 introduces tcast atop an idealized lock-step synchronous network,
//! but Algorithm 1's correctness proof (§4.1.3) and its liveness argument in
//! particular only ever lean on the two properties above -- "a majority",
//! not "everyone". An earlier version of this module took the idealized
//! network literally and required *full* coverage (every live replica
//! receives every other live replica's input every step); that is a valid,
//! safe over-approximation of the contract, but it has a fatal flaw for
//! this crate's purposes: full coverage makes `E_i = C_i = U` collapse to
//! the exact same set on *every* call, which makes `best(E) == best(U)`
//! hold unconditionally after round 1, every single time. That is stronger
//! than the paper's liveness theorem (`>= 1/2` success probability *per
//! round*, "less than two rounds in expectation") -- it silently discards
//! the entire probabilistic character of the algorithm that P14 exists to
//! test. So this module instead realizes the *minimal* contract:
//!
//! - One designated replica per call, `b_src = min(live)`, has its input
//!   **reliably** disseminated: this function keeps retrying `b_src`'s
//!   outbound sends, unconditionally, until every live replica has it. This
//!   is what makes the `B` property exactly satisfiable (`B = inputs[b_src]`
//!   ends up a subset of every replica's `R` by construction) without
//!   requiring the concrete ISR mechanism Phase 2 will use instead.
//! - Every *other* live replica's outbound sends to a given destination
//!   `dst` are retried only until `dst` has accumulated inputs from a true
//!   majority of the full replica set (`> total_replicas / 2`, counting
//!   `dst` itself and `b_src`) -- then any of that sender's still-pending
//!   sends to `dst` are abandoned. Since majority is reached via whichever
//!   sends the scheduler's real delay/drop/reorder behavior happened to get
//!   through first, **which majority** a given replica ends up with is
//!   genuinely random and can differ between replicas -- restoring the
//!   real, content-oblivious-adversary-driven uncertainty the paper's
//!   liveness argument (§4.1.3, "each replica i observes at least 1/2
//!   probability... in its universal set") depends on.
//!
//! Both retry loops run over the *real* harness (subject to whatever
//! scheduler and fault injection a test configured), flushing the kernel to
//! quiescence after each batch; [`MAX_TCAST_RETRIES`] is a generous cap
//! that exists only to fail loudly (not hang) if a test's scheduler
//! violates eventual delivery (A2) so badly that even `b_src`'s mandatory,
//! endlessly-retried broadcast cannot get through.
//!
//! One honest caveat: always picking `b_src = min(live)` as the reliably-
//! disseminated replica is a fixed asymmetry the idealized model doesn't
//! have (there, *every* replica's step-1 dissemination is equally
//! reliable). It does not affect safety -- `B`'s universal property holds
//! regardless of which replica plays that role -- but it does mean
//! `b_src`'s own proposal is structurally somewhat more likely to survive
//! into every replica's view than another replica's is. See this module's
//! tests and `crates/consensus/tests/termination.rs` for the empirical
//! effect (or lack of one) on the round-count distribution.
//!
//! This module deliberately does **not** know about rounds, phases, E/C/U,
//! or `best()` -- that's [`crate::algorithm`]'s job, layered on top exactly
//! as Algorithm 1 layers atop tcast in the paper.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use queso_sim::ids::NodeId;
use queso_sim::Kernel;

use crate::message::TcastMsg;
use crate::node::Mailbox;
use crate::proposal::ProposalSet;

/// Generous cap on the number of internal retry batches a single `tcast`
/// call will attempt before panicking. Not a protocol concept (it is not
/// counted as an Algorithm-1 round) -- purely a "this should never happen"
/// guard against a misconfigured scheduler that violates eventual delivery.
pub const MAX_TCAST_RETRIES: usize = 500;

/// The `(R, B)` pair a `tcast` call returns: `R_i` per live replica
/// (genuinely different between replicas in general -- see module docs)
/// plus the single, shared `B`.
#[derive(Debug, Clone)]
pub struct TcastResult<V> {
    /// `R_i` for every live replica.
    pub r: BTreeMap<NodeId, ProposalSet<V>>,
    /// `B`, shared by every live replica.
    pub b: ProposalSet<V>,
}

/// Run one tcast step: every replica in `live` disseminates its entry of
/// `inputs`. `b_src = min(live)`'s dissemination is retried until every
/// live replica has it (guaranteeing `B`'s universal property); every other
/// sender's dissemination to a given destination is retried only until
/// that destination has accumulated a true majority of `total_replicas`
/// (guaranteeing `R`'s majority property, at possibly-different majorities
/// per destination). See the module docs for why.
///
/// # Panics
///
/// Panics if `live` is not a true majority of `total_replicas` (that is,
/// `live.len()` doubled does not exceed `total_replicas`). This is a hard
/// precondition, not a runtime condition tcast can gracefully degrade from:
/// tcast's majority guarantee is defined with respect to the *full*
/// configured membership, not merely whichever replicas happen to be
/// reachable, so calling it without a live majority would either hang
/// forever (no majority of `total_replicas` is reachable at all) or, had
/// this implementation not required a live majority, risk two disjoint
/// non-majority groups each satisfying tcast's contract among themselves --
/// exactly the split-brain scenario (N1) consensus must never allow.
/// Callers are responsible for only invoking tcast while a true majority is
/// live; this is also what "safety-preserved-but-liveness-may-stall"
/// (P11/O4) means operationally: with more than `f` crashed, progress (and
/// hence further tcast calls) may not be attempted.
///
/// Also panics if `inputs` is missing an entry for some replica in `live`,
/// or if retries are exhausted without every live replica reaching a
/// majority (see [`MAX_TCAST_RETRIES`]).
pub fn tcast<V: Ord + Clone + std::fmt::Debug>(
    kernel: &mut Kernel<TcastMsg<V>>,
    mailboxes: &BTreeMap<NodeId, Rc<RefCell<Mailbox<V>>>>,
    live: &BTreeSet<NodeId>,
    total_replicas: usize,
    inputs: &BTreeMap<NodeId, ProposalSet<V>>,
) -> TcastResult<V> {
    assert!(
        2 * live.len() > total_replicas,
        "tcast called without a live majority: {} live out of {} replicas",
        live.len(),
        total_replicas
    );
    assert!(!live.is_empty(), "tcast called with no live replicas");

    let majority_threshold = total_replicas / 2 + 1;
    let b_src = *live.iter().min().expect("checked non-empty above");

    for &id in live {
        let input = inputs
            .get(&id)
            .unwrap_or_else(|| panic!("tcast: missing input for live replica {id}"));
        let mailbox = &mailboxes[&id];
        let mut m = mailbox.borrow_mut();
        m.received.clear();
        // A replica trivially "receives" its own input -- no network
        // round-trip needed to know your own proposal set.
        m.received.insert(id, input.clone());
    }

    let mut pending: BTreeSet<(NodeId, NodeId)> = BTreeSet::new();
    for &src in live {
        for &dst in live {
            if src != dst {
                pending.insert((src, dst));
            }
        }
    }

    let mut attempt = 0usize;
    loop {
        // Prune before (re)sending: drop pairs already delivered, and drop
        // non-`b_src` pairs whose destination has already reached a
        // majority without them -- this is the "opportunistic, per-
        // destination-random majority" behavior the module docs describe.
        // `b_src`'s pairs are never abandoned this way: its dissemination
        // must reach literally everyone, unconditionally.
        pending.retain(|&(src, dst)| {
            let m = mailboxes[&dst].borrow();
            if m.received.contains_key(&src) {
                return false;
            }
            if src != b_src && m.received.len() >= majority_threshold {
                return false;
            }
            true
        });
        if pending.is_empty() {
            break;
        }
        attempt += 1;
        assert!(
            attempt <= MAX_TCAST_RETRIES,
            "tcast failed to converge among {} live replicas after {} retries \
             -- the configured scheduler likely violates eventual delivery (A2)",
            live.len(),
            MAX_TCAST_RETRIES
        );
        for &(src, dst) in &pending {
            kernel.inject_message(
                src,
                dst,
                TcastMsg {
                    set: inputs[&src].clone(),
                },
            );
        }
        kernel.run();
    }

    let r: BTreeMap<NodeId, ProposalSet<V>> = live
        .iter()
        .map(|&i| {
            let m = mailboxes[&i].borrow();
            debug_assert!(
                m.received.len() >= majority_threshold,
                "R_{i} has only {} senders, below the majority threshold {majority_threshold}",
                m.received.len()
            );
            let union: ProposalSet<V> = m
                .received
                .values()
                .flat_map(|s| s.iter().cloned())
                .collect();
            (i, union)
        })
        .collect();

    let b = inputs[&b_src].clone();

    TcastResult { r, b }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::ReplicaNode;
    use crate::proposal::Proposal;
    use queso_sim::scheduler::{ContentObliviousAdversary, Fifo, SchedulerKind};

    fn proposal(value: u64, priority: u64, origin: u32) -> Proposal<u64> {
        Proposal {
            value,
            priority,
            origin: NodeId(origin),
        }
    }

    /// Build a kernel with `n` replicas registered as `ReplicaNode`s, plus
    /// the mailbox/priority-cell handles the driver needs.
    #[allow(clippy::type_complexity)]
    fn build_cluster(
        seed: u64,
        n: u32,
        scheduler: SchedulerKind<TcastMsg<u64>>,
    ) -> (
        Kernel<TcastMsg<u64>>,
        BTreeMap<NodeId, Rc<RefCell<Mailbox<u64>>>>,
    ) {
        let mut kernel = Kernel::new(seed, scheduler);
        let mut mailboxes = BTreeMap::new();
        for i in 0..n {
            let id = NodeId(i);
            let mailbox = Rc::new(RefCell::new(Mailbox::default()));
            let priority = Rc::new(RefCell::new(None));
            kernel.add_node(id, Box::new(ReplicaNode::new(mailbox.clone(), priority)));
            mailboxes.insert(id, mailbox);
        }
        (kernel, mailboxes)
    }

    fn singleton_inputs(n: u32) -> BTreeMap<NodeId, ProposalSet<u64>> {
        (0..n)
            .map(|i| {
                let id = NodeId(i);
                let set: ProposalSet<u64> = [proposal(i as u64, 100 + i as u64, i)].into();
                (id, set)
            })
            .collect()
    }

    /// The literal tcast contract: `R_i` includes the inputs of *some*
    /// majority `S` of the full replica set (`|S| > n/2`). With `n=5` that
    /// means at least 3 of the 5 live replicas' inputs must show up in each
    /// `R_i` -- not necessarily the same 3 for every `i`.
    #[test]
    fn r_includes_a_majority_of_replicas_inputs() {
        let (mut kernel, mailboxes) =
            build_cluster(1, 5, SchedulerKind::Oblivious(Box::new(Fifo::new(1))));
        let live: BTreeSet<NodeId> = (0..5).map(NodeId).collect();
        let inputs = singleton_inputs(5);
        let majority = 5 / 2 + 1;

        let result = tcast(&mut kernel, &mailboxes, &live, 5, &inputs);

        for &i in &live {
            let r_i = &result.r[&i];
            let included = live.iter().filter(|&&j| inputs[&j].is_subset(r_i)).count();
            assert!(
                included >= majority,
                "R_{i} only includes {included} of a required majority ({majority}) of replicas' inputs"
            );
        }
    }

    /// Under a scheduler with real slack (majority threshold 3 < live count
    /// 5), different replicas may end up with genuinely different `R`
    /// sets -- this is what restores the paper's per-round probabilistic
    /// liveness argument (see the module docs). This test doesn't assert a
    /// specific seed produces variation (that would be flaky/over-fitted to
    /// one PRNG stream) -- it just confirms the implementation is *capable*
    /// of it by scanning a range of seeds for at least one where two live
    /// replicas' `R` sets differ.
    #[test]
    fn r_can_differ_across_replicas_when_there_is_majority_slack() {
        let live: BTreeSet<NodeId> = (0..5).map(NodeId).collect();
        let inputs = singleton_inputs(5);

        let mut saw_variation = false;
        for seed in 0..200u64 {
            let adversary = ContentObliviousAdversary::new(1, 4).with_drop_probability(0.3);
            let (mut kernel, mailboxes) =
                build_cluster(seed, 5, SchedulerKind::Oblivious(Box::new(adversary)));
            let result = tcast(&mut kernel, &mailboxes, &live, 5, &inputs);
            let sets: BTreeSet<&ProposalSet<u64>> = result.r.values().collect();
            if sets.len() > 1 {
                saw_variation = true;
                break;
            }
        }
        assert!(
            saw_variation,
            "expected at least one seed (of 200) to produce differing R sets across replicas"
        );
    }

    #[test]
    fn b_is_a_subset_of_every_live_replicas_r() {
        let (mut kernel, mailboxes) =
            build_cluster(2, 5, SchedulerKind::Oblivious(Box::new(Fifo::new(1))));
        let live: BTreeSet<NodeId> = (0..5).map(NodeId).collect();
        let inputs = singleton_inputs(5);

        let result = tcast(&mut kernel, &mailboxes, &live, 5, &inputs);

        for &i in &live {
            assert!(
                result.b.is_subset(&result.r[&i]),
                "B must be a subset of every live replica's R (R_{i})"
            );
        }
        // And B must literally equal some replica's actual input.
        assert!(inputs.values().any(|p| *p == result.b));
    }

    #[test]
    fn tcast_survives_message_loss_via_retry() {
        // A lossy but eventually-delivering scheduler: drop_probability is
        // capped below 1 by ContentObliviousAdversary itself (0.95), so
        // eventual delivery (A2) still holds and tcast must still converge
        // -- both the mandatory `b_src` dissemination and every replica's
        // opportunistic majority.
        let adversary = ContentObliviousAdversary::new(1, 3).with_drop_probability(0.6);
        let (mut kernel, mailboxes) =
            build_cluster(42, 5, SchedulerKind::Oblivious(Box::new(adversary)));
        let live: BTreeSet<NodeId> = (0..5).map(NodeId).collect();
        let inputs = singleton_inputs(5);
        let majority = 5 / 2 + 1;

        let result = tcast(&mut kernel, &mailboxes, &live, 5, &inputs);

        for &i in &live {
            let r_i = &result.r[&i];
            let included = live.iter().filter(|&&j| inputs[&j].is_subset(r_i)).count();
            assert!(
                included >= majority,
                "R_{i} only includes {included} of a required majority ({majority}) despite scheduler drops"
            );
        }
        for &i in &live {
            assert!(result.b.is_subset(&result.r[&i]));
        }
    }

    #[test]
    fn crashed_replicas_are_excluded_from_r_and_b() {
        let (mut kernel, mailboxes) =
            build_cluster(3, 5, SchedulerKind::Oblivious(Box::new(Fifo::new(1))));
        kernel.crash(NodeId(3));
        kernel.crash(NodeId(4));
        let live: BTreeSet<NodeId> = [NodeId(0), NodeId(1), NodeId(2)].into();
        let inputs: BTreeMap<NodeId, ProposalSet<u64>> = live
            .iter()
            .map(|&id| {
                let set: ProposalSet<u64> = [proposal(id.0 as u64, 100 + id.0 as u64, id.0)].into();
                (id, set)
            })
            .collect();

        let result = tcast(&mut kernel, &mailboxes, &live, 5, &inputs);

        for &i in &live {
            assert_eq!(
                result.r[&i].len(),
                3,
                "R_{i} should contain exactly the 3 live replicas' singleton proposals"
            );
            for &j in &live {
                assert!(inputs[&j].is_subset(&result.r[&i]));
            }
        }
    }

    #[test]
    #[should_panic(expected = "live majority")]
    fn tcast_panics_without_a_live_majority() {
        let (mut kernel, mailboxes) =
            build_cluster(4, 5, SchedulerKind::Oblivious(Box::new(Fifo::new(1))));
        let live: BTreeSet<NodeId> = [NodeId(0), NodeId(1)].into(); // 2 of 5 -- not a majority
        let inputs = singleton_inputs(5);
        let _ = tcast(&mut kernel, &mailboxes, &live, 5, &inputs);
    }

    #[test]
    fn tcast_is_deterministic_given_same_seed() {
        let run = |seed: u64| {
            let (mut kernel, mailboxes) = build_cluster(
                seed,
                5,
                SchedulerKind::Oblivious(Box::new(
                    ContentObliviousAdversary::new(1, 4).with_drop_probability(0.3),
                )),
            );
            let live: BTreeSet<NodeId> = (0..5).map(NodeId).collect();
            let inputs = singleton_inputs(5);
            let result = tcast(&mut kernel, &mailboxes, &live, 5, &inputs);
            (result.r, result.b)
        };

        let a = run(123);
        let b = run(123);
        assert_eq!(a, b, "same seed must produce the same tcast outcome");
    }
}
