//! Algorithm 1: abstract QuePaxa consensus, layered atop [`crate::tcast`].
//!
//! ```text
//! repeat:
//!     p  = <v, random()>        # prioritized proposal
//!     (P,  _ ) = tcast({p})     # gather a majority's proposals
//!     (E,  P') = tcast(P)       # propagate -> existent sets
//!     (C,  U ) = tcast(P')      # propagate -> common & universal sets
//!     v = best(C).value         # highest-priority proposal in C -> next round's value
//!     if best(E) == best(U):    # detect consensus
//!         deliver(v)            # decide (once per slot)
//! ```
//!
//! [`Cluster`] drives this for every live replica in lock-step rounds (the
//! tcast layer below it is itself a lock-step barrier -- see
//! `crate::tcast`'s module docs), maintaining each replica's `v`/decided
//! state in [`ReplicaState`], and exposes `run_round`/`run_slot` for tests
//! to drive one round at a time (so a test can inject a crash *between*
//! rounds) or to completion.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use queso_sim::ids::NodeId;
use queso_sim::scheduler::SchedulerKind;
use queso_sim::Kernel;

use crate::message::TcastMsg;
use crate::node::{Mailbox, ReplicaNode, DRAW_PRIORITY_TIMER};
use crate::proposal::{best, Proposal, ProposalSet};
use crate::tcast::tcast;

/// A single replica's Algorithm-1 state for this slot.
#[derive(Debug, Clone)]
pub struct ReplicaState<V> {
    /// The replica's current candidate value, updated at the end of every
    /// round to `best(C).value`.
    pub v: V,
    /// `Some(value)` once this replica has delivered a decision for the
    /// slot; `None` until then. Set at most once (Integrity, P3).
    pub decided: Option<V>,
    /// How many Algorithm-1 rounds this replica has run so far.
    pub rounds_run: u32,
}

/// Drives Algorithm 1 for one consensus slot across a fixed set of
/// replicas, on top of a [`Kernel`] running the tcast layer.
pub struct Cluster<V> {
    kernel: Kernel<TcastMsg<V>>,
    /// The full, static replica membership (§3.1, A4) -- used as the `n`
    /// tcast's majority requirement is defined against, regardless of how
    /// many are currently live.
    replicas: Vec<NodeId>,
    mailboxes: BTreeMap<NodeId, Rc<RefCell<Mailbox<V>>>>,
    priorities: BTreeMap<NodeId, Rc<RefCell<Option<u64>>>>,
    state: BTreeMap<NodeId, ReplicaState<V>>,
    /// Driver-tracked liveness, mirroring whatever this struct's own
    /// `crash`/`restart` calls have done to the underlying kernel. Needed
    /// because `Kernel` does not expose fault state publicly (see
    /// `crates/sim/src/fault.rs`) and, more fundamentally, because
    /// `tcast`'s majority precondition must be checked against a
    /// deliberately-tracked live set, not inferred from the network.
    live: BTreeSet<NodeId>,
}

impl<V: Ord + Clone + std::fmt::Debug + 'static> Cluster<V> {
    /// Build a cluster with one replica per `(NodeId, initial value)` pair
    /// in `initial_values`, all initially live.
    pub fn new(
        seed: u64,
        scheduler: SchedulerKind<TcastMsg<V>>,
        initial_values: BTreeMap<NodeId, V>,
    ) -> Self {
        let mut kernel = Kernel::new(seed, scheduler);
        let mut mailboxes = BTreeMap::new();
        let mut priorities = BTreeMap::new();
        let mut state = BTreeMap::new();
        let mut replicas = Vec::new();

        for (id, v) in initial_values {
            let mailbox = Rc::new(RefCell::new(Mailbox::default()));
            let priority = Rc::new(RefCell::new(None));
            kernel.add_node(
                id,
                Box::new(ReplicaNode::new(mailbox.clone(), priority.clone())),
            );
            mailboxes.insert(id, mailbox);
            priorities.insert(id, priority);
            state.insert(
                id,
                ReplicaState {
                    v,
                    decided: None,
                    rounds_run: 0,
                },
            );
            replicas.push(id);
        }
        replicas.sort();
        let live = replicas.iter().copied().collect();

        Self {
            kernel,
            replicas,
            mailboxes,
            priorities,
            state,
            live,
        }
    }

    /// The full, static replica membership.
    pub fn replicas(&self) -> &[NodeId] {
        &self.replicas
    }

    /// Replicas the driver currently considers live (not crashed).
    pub fn live(&self) -> &BTreeSet<NodeId> {
        &self.live
    }

    /// Crash a replica: stops it from sending/receiving (via the kernel's
    /// real fault injection) and removes it from the driver's live set so
    /// `run_round` stops trying to make it participate.
    pub fn crash(&mut self, id: NodeId) {
        self.kernel.crash(id);
        self.live.remove(&id);
    }

    /// Restart a previously-crashed replica. Phase 1 is crash-*stop* per
    /// the abstract algorithm (§3.1: replicas fail "by going silent
    /// forever"); restart-mid-slot durability semantics are a Phase-4
    /// concern (P12). This is provided mainly so a test can exercise
    /// `Node::on_restart`'s volatile-state-clear behavior, not as a
    /// sanctioned Phase-1 recovery path -- a restarted replica resumes
    /// with a *fresh* mailbox/priority cell (volatile state gone) but its
    /// `ReplicaState` (`v`, `decided`, `rounds_run`) is left untouched
    /// here, since Phase 1 has no durable-state design yet.
    pub fn restart(&mut self, id: NodeId) {
        self.kernel.restart(id);
        self.live.insert(id);
    }

    /// This replica's decision, if it has delivered one yet.
    pub fn decided(&self, id: NodeId) -> Option<&V> {
        self.state.get(&id).and_then(|s| s.decided.as_ref())
    }

    /// How many rounds this replica has run.
    pub fn rounds_run(&self, id: NodeId) -> u32 {
        self.state.get(&id).map(|s| s.rounds_run).unwrap_or(0)
    }

    /// True once every currently-live replica has decided.
    pub fn all_live_decided(&self) -> bool {
        !self.live.is_empty() && self.live.iter().all(|id| self.state[id].decided.is_some())
    }

    /// Run one Algorithm-1 round for every currently-live replica: draw
    /// priorities, run the three tcast calls, update each replica's `v`,
    /// and deliver decisions where `best(E) == best(U)`.
    ///
    /// Returns the set of replicas that newly decided *this* round.
    ///
    /// # Panics
    ///
    /// If fewer than a majority of the full replica set is currently live
    /// (see [`crate::tcast::tcast`]'s panic docs) -- Phase 1 does not
    /// attempt to make progress, gracefully or otherwise, without a live
    /// majority; that is the documented liveness envelope (P11/O4).
    pub fn run_round(&mut self) -> BTreeSet<NodeId> {
        let n = self.replicas.len();

        // 1. p_i = <v_i, random()> -- priorities are drawn via a real
        //    Node::on_timer callback so they come from the kernel's single
        //    seeded PRNG stream, consumed in deterministic dispatch order.
        for &id in &self.live {
            self.kernel.inject_timer(id, 0, DRAW_PRIORITY_TIMER);
        }
        self.kernel.run();

        let mut p: BTreeMap<NodeId, Proposal<V>> = BTreeMap::new();
        for &id in &self.live {
            let priority = self.priorities[&id]
                .borrow_mut()
                .take()
                .expect("priority timer must have fired for every live replica");
            let value = self.state[&id].v.clone();
            p.insert(
                id,
                Proposal {
                    value,
                    priority,
                    origin: id,
                },
            );
        }

        // (P, _) <- tcast({p})
        let call1_inputs: BTreeMap<NodeId, ProposalSet<V>> = p
            .iter()
            .map(|(&id, prop)| (id, ProposalSet::from([prop.clone()])))
            .collect();
        let call1 = tcast(
            &mut self.kernel,
            &self.mailboxes,
            &self.live,
            n,
            &call1_inputs,
        );
        let p_sets = call1.r;

        // (E, P') <- tcast(P)
        let call2 = tcast(&mut self.kernel, &self.mailboxes, &self.live, n, &p_sets);
        let e_sets = call2.r;
        let p_prime = call2.b;

        // (C, U) <- tcast(P')  -- P' is the same set for every replica.
        let call3_inputs: BTreeMap<NodeId, ProposalSet<V>> =
            self.live.iter().map(|&id| (id, p_prime.clone())).collect();
        let call3 = tcast(
            &mut self.kernel,
            &self.mailboxes,
            &self.live,
            n,
            &call3_inputs,
        );
        let c_sets = call3.r;
        let u = call3.b;

        // Safety crux (paper §4.1.2): the cross-node relation U ⊆ C_j ⊆ E_i is
        // exactly what forces Agreement. It holds by construction in this tcast
        // realization, but assert it every round so a future refactor of the
        // three-call composition cannot silently break it — in debug/test builds
        // this runs across the entire property-test seed corpus.
        #[cfg(debug_assertions)]
        {
            for &j in &self.live {
                assert!(
                    u.is_subset(&c_sets[&j]),
                    "crux invariant violated: U ⊄ C_{j:?}"
                );
                for &i in &self.live {
                    assert!(
                        c_sets[&j].is_subset(&e_sets[&i]),
                        "crux invariant violated: C_{j:?} ⊄ E_{i:?}"
                    );
                }
            }
        }

        let u_best = best(&u)
            .expect("U must be nonempty: every live replica proposes")
            .clone();

        let mut newly_decided = BTreeSet::new();
        for &id in &self.live {
            let c_best = best(&c_sets[&id])
                .expect("C_i must be nonempty: every live replica proposes")
                .clone();
            let e_best = best(&e_sets[&id])
                .expect("E_i must be nonempty: every live replica proposes")
                .clone();

            let st = self.state.get_mut(&id).expect("live id must have state");
            st.v = c_best.value.clone();
            st.rounds_run += 1;
            if st.decided.is_none() && e_best == u_best {
                st.decided = Some(c_best.value);
                newly_decided.insert(id);
            }
        }
        newly_decided
    }

    /// Run rounds until every currently-live replica has decided, or
    /// `max_rounds` is reached. Returns the number of rounds actually run.
    pub fn run_slot(&mut self, max_rounds: u32) -> u32 {
        let mut rounds = 0;
        while rounds < max_rounds && !self.all_live_decided() {
            self.run_round();
            rounds += 1;
        }
        rounds
    }

    /// The kernel's recorded trace so far (for determinism/reproducibility
    /// checks).
    pub fn trace(&self) -> &queso_sim::trace::Trace {
        self.kernel.trace()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use queso_sim::scheduler::Fifo;

    fn cluster(n: u32, seed: u64) -> Cluster<u32> {
        let initial: BTreeMap<NodeId, u32> = (0..n).map(|i| (NodeId(i), i)).collect();
        Cluster::new(
            seed,
            SchedulerKind::Oblivious(Box::new(Fifo::new(1))),
            initial,
        )
    }

    #[test]
    fn all_live_replicas_eventually_decide() {
        let mut c = cluster(5, 7);
        let rounds = c.run_slot(50);
        assert!(
            c.all_live_decided(),
            "did not decide within {rounds} rounds"
        );
    }

    #[test]
    fn all_replicas_decide_the_same_value() {
        let mut c = cluster(5, 7);
        c.run_slot(50);
        let mut decisions: BTreeSet<u32> = BTreeSet::new();
        for &id in c.replicas() {
            if let Some(v) = c.decided(id) {
                decisions.insert(*v);
            }
        }
        assert_eq!(decisions.len(), 1, "replicas disagreed: {decisions:?}");
    }

    #[test]
    fn decided_value_was_some_replicas_initial_value() {
        let mut c = cluster(5, 7);
        c.run_slot(50);
        let v = *c.decided(NodeId(0)).expect("should have decided");
        assert!((0..5).contains(&v), "decided value {v} was never proposed");
    }

    #[test]
    fn each_replica_decides_at_most_once() {
        // Run one round at a time and confirm `decided` never changes once set.
        let mut c = cluster(3, 11);
        let mut seen: BTreeMap<NodeId, u32> = BTreeMap::new();
        for _ in 0..20 {
            if c.all_live_decided() {
                break;
            }
            c.run_round();
            for &id in c.replicas() {
                if let Some(v) = c.decided(id) {
                    if let Some(prev) = seen.get(&id) {
                        assert_eq!(*prev, *v, "replica {id} changed its decision");
                    } else {
                        seen.insert(id, *v);
                    }
                }
            }
        }
        assert!(!seen.is_empty(), "nobody decided");
    }

    #[test]
    fn progresses_with_a_crashed_minority() {
        let mut c = cluster(5, 3);
        c.crash(NodeId(4)); // f=2 tolerated for n=5; only 1 crashed here
        let rounds = c.run_slot(50);
        assert!(
            c.all_live_decided(),
            "did not decide within {rounds} rounds"
        );
        assert!(
            c.decided(NodeId(4)).is_none(),
            "crashed replica should not decide"
        );
    }

    #[test]
    #[should_panic(expected = "live majority")]
    fn run_round_panics_without_a_live_majority() {
        let mut c = cluster(5, 3);
        c.crash(NodeId(2));
        c.crash(NodeId(3));
        c.crash(NodeId(4)); // only 2 of 5 left -- not a majority
        c.run_round();
    }
}
