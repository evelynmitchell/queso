//! Phase 3: the leader fast path (§4.2.5, D1) -- hit, fallback, and
//! agreement across a seed corpus, as called for in
//! `docs/00-project-outline.md` Phase 3 and `docs/02-properties.md` A3/P1/P16.
//!
//! Three scenarios, matching the task's three required cases:
//!
//! 1. **Fast-path hit**: a live leader plus a well-behaved (`Fifo`) network
//!    decides every live replica in round 1, phase 0 -- a genuine
//!    single-round-trip commit (`decided_via_fast_path`).
//! 2. **Fallback A -- leader crash**: the leader is crashed before the slot
//!    starts; the remaining replicas still decide, via the leaderless core,
//!    and agree.
//! 3. **Fallback B -- content-aware defeat**: a custom [`AwareScheduler`]
//!    (only possible for a content-aware adversary -- see
//!    `queso_sim::scheduler`'s module docs) prevents the leader's
//!    `H`-priority proposal from *ever* reaching a majority of recorders,
//!    exactly the §4.2.5 "delivered to every `E` set but no `U` set"
//!    scenario in miniature. The fast path can then provably never fire
//!    (see the adversary's own docs below for why), yet the slot still
//!    decides, safely, via the leaderless core.
//!
//! Plus a seed-corpus property suite (Agreement/Validity/Integrity) with a
//! leader configured -- sometimes alive, sometimes crashed, chosen
//! per-seed -- and a determinism check that leader designation does not
//! disturb the `seed -> identical trace` contract (D9).

use std::collections::{BTreeMap, BTreeSet};

use rand::Rng;

use queso_consensus::rpc::ConcreteMsg;
use queso_consensus::{ConcreteCluster, H};
use queso_sim::ids::NodeId;
use queso_sim::network::Envelope;
use queso_sim::scheduler::{
    AwareScheduler, ContentObliviousAdversary, Decision, Fifo, SchedulerCtx, SchedulerKind,
};

const MAX_TICKS: u64 = 200_000;

/// `s = 4*1 + 0`: round 1, phase 0 -- mirrors the private
/// `queso_consensus::proposer::FIRST_ROUND_STEP` (not exported; Algorithm
/// 4 fixes this value, so it is safe to hardcode here too).
const FIRST_ROUND_STEP: u64 = 4;

fn initial_values(n: u32) -> BTreeMap<NodeId, u32> {
    (0..n).map(|i| (NodeId(i), i)).collect()
}

// ---------------------------------------------------------------------
// Scenario 1: fast-path hit.
// ---------------------------------------------------------------------

/// Under a reliable, fixed-delay network the leader's step-4 requests are
/// sent (via the driver's kickoff-timer injection order, which follows
/// `NodeId` order) before any other replica's, and -- because `Fifo`'s
/// delay is identical for every link -- arrive at every recorder before any
/// other replica's competing phase-0 proposal. So every recorder's `F[4]`
/// is the leader's `H`-priority proposal, and *every* live replica (not
/// just the leader -- the fast-path check in
/// `queso_consensus::proposer::fast_path_value` is not leader-specific)
/// decides in a single round-trip.
#[test]
fn fast_path_hit_under_a_good_network() {
    for n in [3u32, 5u32, 7u32] {
        for seed in 0..30u64 {
            let mut c = ConcreteCluster::new_with_leader(
                seed,
                SchedulerKind::Oblivious(Box::new(Fifo::new(1))),
                initial_values(n),
                Some(NodeId(0)),
            );
            c.run_slot(MAX_TICKS);
            assert!(
                c.all_live_decided(),
                "n={n} seed={seed}: did not decide within the tick budget"
            );
            for &id in c.replicas() {
                assert!(
                    c.decided_via_fast_path(id),
                    "n={n} seed={seed}: replica {id} did not decide via the fast path \
                     (step={}, decided={:?})",
                    c.step(id),
                    c.decided(id)
                );
            }
            // The value decided is the leader's own initial value (0),
            // since it is the only proposal ever carrying H.
            assert_eq!(c.decided(NodeId(0)), Some(0));
        }
    }
}

// ---------------------------------------------------------------------
// Scenario 2: fallback when the leader crashes before/at phase 0.
// ---------------------------------------------------------------------

/// The leader is crashed before the slot even starts (`run_slot` only kicks
/// off currently-live replicas -- see `ConcreteCluster::crash`'s docs), so
/// no `H`-priority proposal is ever sent. The remaining replicas must still
/// decide -- and agree -- purely through the leaderless core (P16).
#[test]
fn fallback_decides_when_leader_crashes_before_the_slot_starts() {
    for n in [3u32, 5u32] {
        for seed in 0..50u64 {
            let leader = NodeId(0);
            let scheduler = ContentObliviousAdversary::new(1, 6).with_drop_probability(0.2);
            let mut c = ConcreteCluster::new_with_leader(
                seed,
                SchedulerKind::Oblivious(Box::new(scheduler)),
                initial_values(n),
                Some(leader),
            );
            c.crash(leader);
            c.run_slot(MAX_TICKS);

            assert!(
                c.all_live_decided(),
                "n={n} seed={seed}: did not decide within the tick budget with a crashed leader"
            );
            assert!(
                c.decided(leader).is_none(),
                "n={n} seed={seed}: crashed leader should not decide"
            );

            let decisions: BTreeSet<u32> =
                c.live().iter().filter_map(|&id| c.decided(id)).collect();
            assert_eq!(
                decisions.len(),
                1,
                "n={n} seed={seed}: replicas disagreed with a crashed leader: {decisions:?}"
            );

            // Nobody can have used the fast path: the leader that alone
            // could ever attach H never sent a single message.
            for &id in c.live() {
                assert!(
                    !c.decided_via_fast_path(id),
                    "n={n} seed={seed}: replica {id} reported a fast-path decision despite \
                     a crashed leader -- H must have leaked from somewhere"
                );
            }
        }
    }
}

// ---------------------------------------------------------------------
// Scenario 3: fallback when a content-aware adversary defeats the fast
// path outright (leader alive, but network-censored).
// ---------------------------------------------------------------------

/// A content-aware adversary (A3: "beyond the content-oblivious
/// assumption") that inspects each `record` request's *step number and
/// proposal priority* -- something a content-oblivious scheduler
/// structurally cannot do (see `queso_sim::scheduler`'s module docs) -- and
/// drops every round-1-phase-0 (`req_step == 4`) `H`-priority request
/// addressed to a chosen `blocked` set of recorders, unconditionally and on
/// every retry. All other traffic -- including every non-`H` phase-0
/// proposal, every later phase/round message, and even a *later* message
/// that happens to still carry priority `H` because some proposer picked up
/// the leader's proposal as its own working candidate (§4.2.5's ordinary,
/// intended behavior -- ordinary phases 1-3 just carry `p` forward
/// unmodified, priority included) -- is delivered normally, with a small
/// amount of jitter/drop to keep the leaderless path itself realistically
/// asynchronous.
///
/// The `req_step == 4` qualifier matters: an earlier version of this
/// adversary blocked *any* `H`-tagged request regardless of step, which
/// looked equivalent but was not -- once a non-leader proposer's own
/// round-1 quorum picked up the leader's `H` proposal as its candidate (the
/// intended, safe "Case 2" behavior from Lemma C.10 -- see
/// `queso_consensus::proposer`'s module docs), *its* subsequent phase-1
/// spread at step 5 still carries priority `H` (phases 1-3 never touch
/// priority) and would then *also* get blocked forever, permanently
/// stalling that proposer too -- an unbounded-censorship failure mode well
/// beyond what §4.2.5 actually claims a content-aware adversary can do
/// (defeat *round 1*, not defeat the value at every future step). Scoping
/// the block to the literal round-1 fast-path request avoids that and keeps
/// this adversary a faithful "defeat the fast path, nothing more" model.
///
/// `blocked` is sized to be *itself* a majority (`quorum_threshold`
/// recorders): the complement -- the recorders still allowed to see `H` --
/// therefore has strictly fewer than a majority of members. Since
/// `queso_consensus::proposer::fast_path_value` only ever returns `Some`
/// when *every* recorder in some proposer's own majority-sized quorum
/// reports `H`, and no majority-sized subset of `n` can fit entirely inside
/// a strictly-less-than-majority "allowed" set, the fast path is
/// **structurally impossible** to trigger under this adversary -- not just
/// unlikely. This is the concrete realization of §4.2.5's "a strong network
/// adversary can always prevent leader-based rounds from succeeding, e.g.
/// by delivering the leader's proposal to all E sets but no U set".
///
/// A side effect worth calling out rather than hiding: this also means the
/// *leader's own* round-1 quorum can never form (its every step-4 request
/// is `H`-tagged, so it too can only ever hear back from the "allowed",
/// less-than-majority set) -- the leader's own proposer stalls permanently,
/// exactly as if it had been DoS'd (§4.2.5, P16). That is the intended
/// shape of this scenario, not a bug in the adversary: the test below only
/// asserts that *other* replicas still decide and agree, deliberately not
/// requiring the network-censored-but-not-crashed leader to ever decide.
#[derive(Debug)]
struct DefeatFastPathAdversary {
    blocked: BTreeSet<NodeId>,
    min_delay: u64,
    max_delay: u64,
    drop_probability: f64,
}

impl DefeatFastPathAdversary {
    fn new(n: u32, min_delay: u64, max_delay: u64, drop_probability: f64) -> Self {
        let quorum_threshold = (n as usize) / 2 + 1;
        let blocked = (0..n).take(quorum_threshold).map(NodeId).collect();
        Self {
            blocked,
            min_delay,
            max_delay,
            drop_probability,
        }
    }
}

impl AwareScheduler<ConcreteMsg<u32>> for DefeatFastPathAdversary {
    fn on_send(
        &mut self,
        envelope: &Envelope<ConcreteMsg<u32>>,
        ctx: &mut SchedulerCtx<'_>,
    ) -> Decision {
        if let ConcreteMsg::Request(req) = &envelope.payload {
            if req.req_step == FIRST_ROUND_STEP
                && req.proposal.priority == H
                && self.blocked.contains(&envelope.meta.dst)
            {
                return Decision::Drop;
            }
        }
        if self.drop_probability > 0.0 && ctx.rng.gen::<f64>() < self.drop_probability {
            return Decision::Drop;
        }
        let delay = ctx.rng.gen_range(self.min_delay..=self.max_delay);
        Decision::Deliver { delay }
    }
}

#[test]
fn fallback_decides_when_a_content_aware_adversary_defeats_the_h_proposal() {
    for n in [3u32, 5u32, 7u32] {
        for seed in 0..30u64 {
            let leader = NodeId(0);
            let non_leader_replicas: Vec<NodeId> = (1..n).map(NodeId).collect();
            let adversary = DefeatFastPathAdversary::new(n, 1, 4, 0.05);
            let mut c = ConcreteCluster::new_with_leader(
                seed,
                SchedulerKind::Aware(Box::new(adversary)),
                initial_values(n),
                Some(leader),
            );
            c.run_slot(MAX_TICKS);

            // Deliberately *not* `c.all_live_decided()`: the leader is not
            // crashed, but this adversary structurally censors every one of
            // its round-1 requests (see the adversary's docs above), so it
            // can never itself gather a quorum -- the same shape as a DoS'd
            // leader (P16). What must still hold is that every *other*
            // replica decides and agrees.
            for &id in &non_leader_replicas {
                assert!(
                    c.decided(id).is_some(),
                    "n={n} seed={seed}: non-leader replica {id} did not decide within the \
                     tick budget under the fast-path-defeating adversary"
                );
            }

            let decisions: BTreeSet<u32> = non_leader_replicas
                .iter()
                .filter_map(|&id| c.decided(id))
                .collect();
            assert_eq!(
                decisions.len(),
                1,
                "n={n} seed={seed}: replicas disagreed under the fast-path-defeating \
                 adversary: {decisions:?}"
            );

            // Nobody -- leader included, if it somehow did decide -- may
            // ever have taken the fast path: the adversary makes that
            // structurally impossible (see the adversary's docs).
            for &id in c.replicas() {
                assert!(
                    !c.decided_via_fast_path(id),
                    "n={n} seed={seed}: replica {id} decided via the fast path despite the \
                     adversary structurally preventing any quorum from ever seeing all-H replies"
                );
            }
        }
    }
}

// ---------------------------------------------------------------------
// Property suite: Agreement / Validity / Integrity with a leader present,
// across a seed corpus, under async + crashes -- the leader is sometimes
// alive and sometimes among the crashed set, chosen deterministically per
// seed, so the corpus exercises both the fast-path-eligible case and the
// leader-crashed fallback case.
// ---------------------------------------------------------------------

const SEED_CORPUS_SIZE: u64 = 300;

fn run_one(n: u32, seed: u64) {
    let max_f = (n - 1) / 2; // n = 2f+1
    let f = (seed % u64::from(max_f + 1)) as u32;
    let leader = NodeId((seed % u64::from(n)) as u32);

    let all_values: BTreeSet<u32> = (0..n).collect();

    let scheduler = ContentObliviousAdversary::new(1, 6).with_drop_probability(0.25);
    let mut cluster = ConcreteCluster::new_with_leader(
        seed,
        SchedulerKind::Oblivious(Box::new(scheduler)),
        initial_values(n),
        Some(leader),
    );

    // Crash the highest-numbered `f` replicas -- deterministic given the
    // seed, and always leaves exactly n-f live (a true majority, since
    // f <= (n-1)/2), mirroring
    // `concrete_agreement_validity_integrity.rs::run_one`. The leader may
    // or may not be among them, depending on `leader`'s value this seed.
    let crashed: Vec<NodeId> = (n - f..n).map(NodeId).collect();
    for id in &crashed {
        cluster.crash(*id);
    }

    cluster.run_slot(MAX_TICKS);
    assert!(
        cluster.all_live_decided(),
        "seed {seed} (n={n}, f={f}, leader={leader}): did not decide within the tick budget"
    );

    // P2 -- Validity.
    let mut decisions: BTreeSet<u32> = BTreeSet::new();
    for &id in cluster.replicas() {
        if crashed.contains(&id) {
            continue;
        }
        let v = cluster
            .decided(id)
            .unwrap_or_else(|| panic!("seed {seed}: live replica {id} never decided"));
        assert!(
            all_values.contains(&v),
            "seed {seed}: replica {id} decided phantom value {v}"
        );
        decisions.insert(v);
    }

    // P1 -- Agreement.
    assert_eq!(
        decisions.len(),
        1,
        "seed {seed} (n={n}, f={f}, leader={leader}): replicas disagreed: {decisions:?}"
    );

    // P3/P4 -- Integrity / decide-once / Stability: run well past decision
    // and confirm nothing changes.
    let before: BTreeMap<NodeId, u32> = cluster
        .replicas()
        .iter()
        .filter(|id| !crashed.contains(id))
        .map(|&id| (id, cluster.decided(id).unwrap()))
        .collect();
    cluster.run_slot(1_000);
    for (&id, &v) in &before {
        assert_eq!(
            cluster.decided(id).unwrap(),
            v,
            "seed {seed}: replica {id} changed its decision after already deciding"
        );
    }
}

#[test]
fn agreement_validity_integrity_with_a_leader_n3() {
    for seed in 0..SEED_CORPUS_SIZE {
        run_one(3, seed);
    }
}

#[test]
fn agreement_validity_integrity_with_a_leader_n5() {
    for seed in 0..SEED_CORPUS_SIZE {
        run_one(5, seed);
    }
}

// ---------------------------------------------------------------------
// Determinism (D9): leader designation must not disturb `seed -> identical
// trace and decisions`.
// ---------------------------------------------------------------------

fn run_with_leader(seed: u64, n: u32) -> (Vec<u8>, BTreeMap<NodeId, u32>) {
    let scheduler = ContentObliviousAdversary::new(1, 5).with_drop_probability(0.3);
    let mut cluster = ConcreteCluster::new_with_leader(
        seed,
        SchedulerKind::Oblivious(Box::new(scheduler)),
        initial_values(n),
        Some(NodeId(0)),
    );
    cluster.run_slot(MAX_TICKS);

    let decisions: BTreeMap<NodeId, u32> = cluster
        .replicas()
        .iter()
        .filter_map(|&id| cluster.decided(id).map(|v| (id, v)))
        .collect();
    (cluster.trace().to_canonical_bytes(), decisions)
}

#[test]
fn leader_designation_preserves_determinism() {
    for seed in [1, 2, 3, 42, 999, 123_456] {
        let (trace_a, decisions_a) = run_with_leader(seed, 5);
        let (trace_b, decisions_b) = run_with_leader(seed, 5);
        assert_eq!(trace_a, trace_b, "seed {seed}: traces diverged");
        assert_eq!(decisions_a, decisions_b, "seed {seed}: decisions diverged");
    }
}

#[test]
fn leader_getter_reports_the_configured_leader() {
    let c: ConcreteCluster<u32> = ConcreteCluster::new_with_leader(
        0,
        SchedulerKind::Oblivious(Box::new(Fifo::new(1))),
        initial_values(3),
        Some(NodeId(1)),
    );
    assert_eq!(c.leader(), Some(NodeId(1)));

    let leaderless: ConcreteCluster<u32> = ConcreteCluster::new(
        0,
        SchedulerKind::Oblivious(Box::new(Fifo::new(1))),
        initial_values(3),
    );
    assert_eq!(leaderless.leader(), None);
}
