//! Issue #92: what happens to Agreement when **two distinct `H`-tagged
//! proposals exist for one slot** -- the state issue #83's restarted leader
//! created -- now that `fast_path_value` refuses a mixed-`H` quorum (#88)?
//!
//! #83 was closed on a belt-and-braces claim: #90 removes the only known
//! source of a second `H` proposal, and #88 "makes one harmless if another
//! source ever exists". The first half is true and tested
//! (`queso-smr`'s `a_catch_up_probe_never_carries_the_reserved_priority`).
//! This file settles the second half by enumeration, the way
//! `proposer::fast_path_uniformity_tests` settled the phase-0 decision --
//! and the answer is **no: the uniformity check does not make a second `H`
//! proposal harmless.** It only keeps a *mixed* quorum from deciding. A
//! quorum that is uniformly `H` on the Ord-**lesser** of the two proposals
//! still fast-decides it -- legitimately, by #88's own rule -- while the
//! ordinary spread/gather machinery, whose every `best`/max comparison
//! prefers the Ord-**greater** proposal (priorities tie at `H`, so
//! `Proposal::Ord` falls through to origin, then value), converges on the
//! other value. Lemma C.10's "nothing can ever beat `H`" simply stops being
//! true when two proposals carry `H`: one of them beats the other.
//!
//! So the load-bearing guarantee is exactly the invariant `Proposer::new`
//! documents: **at most one distinct `H`-tagged proposal may ever exist for
//! a slot** -- enforced at the call sites (#90), not recoverable inside the
//! decision rule. The tests here pin both directions:
//!
//! - `a_uniform_quorum_on_the_lesser_h_proposal_splits_the_slot`: the
//!   counterexample, as one legible hand-driven trace through the real
//!   `Proposer` and `Recorder` -- no mocked protocol logic anywhere.
//! - `enumerated_two_h_states_split_exactly_when_a_uniform_quorum_holds_the_lesser`:
//!   the whole bounded scenario space, with the divergence set characterised
//!   exactly (not just "some state diverges").
//! - `with_a_single_h_proposal_no_fast_ordinary_pair_can_split`: the same
//!   enumeration under #90's invariant -- zero divergences, which is the
//!   theorem the invariant actually buys.
//!
//! # The scenario skeleton (what is and is not enumerated)
//!
//! Every configuration runs the same asynchronous schedule, which is a
//! legal execution (drops and delays only -- nothing a content-oblivious
//! adversary could not do):
//!
//! 1. Each recorder's `F[4]` is seeded by delivering one step-4 `record`
//!    (the write-once `first` slot -- this is "which proposal reached this
//!    recorder first", the axis #83 turned on).
//! 2. Proposer P1 runs phase 0 against a chosen reply quorum. If its view
//!    is uniformly `H`, it fast-decides (the #88 rule, real code).
//! 3. Proposer P2 runs phase 0 against a chosen reply quorum, then -- if
//!    undecided -- spread (step 5) and gather (step 6) against chosen
//!    quorums, deciding by the real phase-2 rule. P1 is silent by then
//!    (decided, or its remaining traffic dropped).
//!
//! Enumerated axes: every per-recorder seeding from the proposal alphabet,
//! and every quorum choice at every step. Not enumerated: interleavings
//! that weave P1's and P2's later rounds together (two *ordinary* proposers
//! racing is the general asynchrony argument, exercised by the seed-corpus
//! suites and the TLA+ concrete model -- and requires no `H` at all). The
//! claim this file settles is specifically about the fast path against the
//! ordinary path, which is where #92's question lives.
//!
//! Detection power, measured: with `fast_path_value`'s uniformity walk
//! mutated back to the pre-#88 priority-only rule, the hand trace and the
//! exact-characterisation sweep both fail (mixed quorums fast-decide again,
//! so the divergence set no longer matches), while the single-`H` test
//! correctly stays green -- with one `H` value the permissive rule *is*
//! harmless, which is that test scoping itself honestly. The single-`H`
//! test's own falsifier is the two-`H` sweep by construction: the same
//! harness with one more letter in the alphabet, and divergences appear
//! (the sweep guards its own non-vacuity with `divergent_configs > 0`).
//! See #92 for the mutation run.

use std::collections::BTreeMap;

use rand::rngs::StdRng;
use rand::SeedableRng;

use queso_consensus::rpc::{ConcreteMsg, RecordRequest};
use queso_consensus::{Proposal, Proposer, Recorder, H};
use queso_sim::ids::{NodeId, TimerId};
use queso_sim::node::Ctx;
use queso_sim::time::LogicalTime;

/// The two `H`-tagged proposals, from the *same* origin -- #83's shape (one
/// replica attaching `H` twice at different times), not the two-leaders
/// shape. `LESSER < GREATER` under `Proposal::Ord`: priorities tie at `H`,
/// origins tie at 0, so the value breaks the tie.
const LESSER_H_VALUE: u64 = 10;
const GREATER_H_VALUE: u64 = 20;

fn lesser_h() -> Proposal<u64> {
    Proposal {
        value: LESSER_H_VALUE,
        priority: H,
        origin: NodeId(0),
    }
}

fn greater_h() -> Proposal<u64> {
    Proposal {
        value: GREATER_H_VALUE,
        priority: H,
        origin: NodeId(0),
    }
}

/// An ordinary leaderless proposal as recorder `i`'s first arrival: a drawn
/// (non-`H`) priority from some other replica. Distinct value per recorder
/// so a decision's provenance is unambiguous.
fn low(i: u32) -> Proposal<u64> {
    Proposal {
        value: 200 + u64::from(i),
        priority: 1_000 + u64::from(i),
        origin: NodeId(i),
    }
}

/// A capturing [`Ctx`]: sends are queued for the test to deliver by hand
/// (that hand-delivery *is* the adversary -- what it delivers, where, in
/// what order), timers are ignored because the test never needs a retry
/// (every awaited quorum is delivered synchronously), and the RNG is seeded
/// (only P1/P2's own phase-0 priority draws consume it, and those are
/// `< H` by construction so they never displace an `H`-seeded `first`).
struct TestCtx {
    id: NodeId,
    sent: Vec<(NodeId, ConcreteMsg<u64>)>,
    rng: StdRng,
}

impl TestCtx {
    fn new(id: NodeId) -> Self {
        Self {
            id,
            sent: Vec::new(),
            rng: StdRng::seed_from_u64(0x92),
        }
    }
}

impl Ctx<ConcreteMsg<u64>> for TestCtx {
    fn self_id(&self) -> NodeId {
        self.id
    }
    fn now(&self) -> LogicalTime {
        LogicalTime::ZERO
    }
    fn send(&mut self, dst: NodeId, payload: ConcreteMsg<u64>) {
        self.sent.push((dst, payload));
    }
    fn schedule_timer(&mut self, _after: u64, _timer_id: TimerId) {}
    fn rng(&mut self) -> &mut StdRng {
        &mut self.rng
    }
}

/// Deliver the proposer's pending (previous-step) requests to exactly the
/// recorders in `set`, feeding each reply straight back. Everything else is
/// dropped -- the adversary's prerogative. Replies that arrive after the
/// proposer has already processed a quorum and moved on are discarded by
/// the proposer's own staleness guard, exactly as on a real network.
fn deliver(
    proposer: &mut Proposer<u64>,
    ctx: &mut TestCtx,
    recorders: &mut BTreeMap<NodeId, Recorder<u64>>,
    set: &[u32],
) {
    let pending = std::mem::take(&mut ctx.sent);
    for (dst, msg) in pending {
        if !set.contains(&dst.0) {
            continue;
        }
        let ConcreteMsg::Request(req) = msg else {
            panic!("a Proposer only ever sends requests");
        };
        let resp = recorders
            .get_mut(&dst)
            .expect("request addressed to a configured recorder")
            .handle(req);
        proposer.on_response(dst, resp, ctx);
    }
}

/// Seed each recorder's write-once `F[4]` with `seeds[i]` -- "the first
/// proposal that reached recorder `i` at step 4".
fn seeded_recorders(seeds: &[Proposal<u64>]) -> BTreeMap<NodeId, Recorder<u64>> {
    seeds
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let mut r = Recorder::new();
            r.handle(RecordRequest {
                slot: 0,
                req_step: 4,
                proposal: p.clone(),
            });
            (NodeId(i as u32), r)
        })
        .collect()
}

/// Run the scenario skeleton for one configuration; returns
/// `(P1's decision, P1 via fast path, P2's decision, P2 via fast path)`.
///
/// P1 and P2 are leaderless proposers (`leader: None`) on replicas 1 and 2:
/// per `Proposer::new`'s docs a `None`-built proposer still *checks* the
/// fast path and fast-decides on a uniform `H` quorum -- it just cannot
/// create one, which is the point: the `H` proposals under test are the
/// seeded ones, and nothing in the run adds a third.
#[allow(clippy::type_complexity)]
fn run_config(
    n: usize,
    seeds: &[Proposal<u64>],
    q_fast: &[u32],
    q0: &[u32],
    q_spread: &[u32],
    q_gather: &[u32],
) -> (Option<u64>, bool, Option<u64>, bool) {
    let mut recorders = seeded_recorders(seeds);

    let mut p1 = Proposer::new(NodeId(1), n, 101, None, 0);
    let mut ctx1 = TestCtx::new(NodeId(1));
    p1.start(&mut ctx1);
    deliver(&mut p1, &mut ctx1, &mut recorders, q_fast);
    let d1 = p1.decided().copied();
    let fast1 = p1.decided_via_fast_path();
    // Whatever P1 queued beyond its phase-0 quorum (a spread, if it did not
    // fast-decide) is dropped: from here on the network silences P1.

    let mut p2 = Proposer::new(NodeId(2), n, 102, None, 0);
    let mut ctx2 = TestCtx::new(NodeId(2));
    p2.start(&mut ctx2);
    deliver(&mut p2, &mut ctx2, &mut recorders, q0);
    if p2.decided().is_none() {
        // Phase 1 (spread, step 5), then phase 2 (gather, step 6), each
        // against its chosen quorum -- the real `process_phase` decides.
        deliver(&mut p2, &mut ctx2, &mut recorders, q_spread);
        deliver(&mut p2, &mut ctx2, &mut recorders, q_gather);
    }
    let d2 = p2.decided().copied();
    let fast2 = p2.decided_via_fast_path();
    (d1, fast1, d2, fast2)
}

/// Every subset of `0..n` of **exactly** quorum size (`n/2 + 1`), as sorted
/// id lists.
///
/// Minimal quorums only, deliberately: a `Proposer` processes a step the
/// instant its `responses` map reaches the threshold, so with this
/// harness's fixed ascending delivery order a larger reply set acts as its
/// first threshold-sized prefix and the tail is dropped by the staleness
/// guard -- the sweep's first draft enumerated supersets too, and what that
/// bought was duplicate coverage of their prefixes, plus a predicate that
/// wrongly read the whole set as what the proposer saw. Every *effective*
/// quorum under any delivery order is some minimal quorum, so this is the
/// complete set of distinct behaviors, not a shortcut.
fn quorums(n: usize) -> Vec<Vec<u32>> {
    let threshold = n / 2 + 1;
    (0u32..1 << n)
        .filter(|mask| mask.count_ones() as usize == threshold)
        .map(|mask| (0..n as u32).filter(|i| mask & (1 << i) != 0).collect())
        .collect()
}

/// The counterexample, by hand -- the trace to read before the enumeration.
///
/// State: `F[4] = [lesser@H, lesser@H, greater@H]` at n=3. Reachable via
/// #83's mechanism: the leader's pre-crash proposal reached r0 and r1; its
/// post-restart probe (a second, different value also carrying `H`, before
/// #90) reached r2 first.
///
/// - P1's quorum {r0, r1} is **uniformly** `H` on the lesser proposal, so
///   #88's uniformity check passes and P1 fast-decides 10. Nothing about
///   that quorum reveals the greater proposal's existence.
/// - P2's quorum {r1, r2} is mixed, so #88 correctly refuses the fast path
///   -- and the ordinary machinery takes over: P2 adopts `best` of what it
///   saw, which is the *greater* proposal (H ties, same origin, larger
///   value), spreads it, gathers it back unopposed, and decides 20 by the
///   ordinary phase-2 rule.
///
/// Two decisions, one slot: P1 Agreement violated with #88's check doing
/// exactly what it says. The check is necessary (without it P2 would have
/// fast-decided from the mixed quorum too -- the original #83) but not
/// sufficient: only the at-most-one-`H`-proposal invariant (#90) is.
#[test]
fn a_uniform_quorum_on_the_lesser_h_proposal_splits_the_slot() {
    let seeds = [lesser_h(), lesser_h(), greater_h()];
    let (d1, fast1, d2, fast2) = run_config(3, &seeds, &[0, 1], &[1, 2], &[1, 2], &[1, 2]);

    assert_eq!(
        d1,
        Some(LESSER_H_VALUE),
        "P1's uniform lesser-H quorum must fast-decide the lesser value"
    );
    assert!(
        fast1,
        "P1's decision must be the phase-0 fast path -- if this now fails while d1 \
         is still decided, the fast-path rule changed and this file needs re-deriving"
    );
    assert_eq!(
        d2,
        Some(GREATER_H_VALUE),
        "P2 must decide the greater value through the ordinary path"
    );
    assert!(
        !fast2,
        "P2's mixed quorum must NOT fast-decide -- that is #88's check working; \
         if this fails, the uniformity check regressed (the original #83)"
    );
    assert_ne!(d1, d2, "two values decided at one slot: the counterexample");
}

/// The whole bounded space, with the divergence set characterised exactly:
/// a fast decision and an ordinary decision disagree **iff** the fast
/// quorum is uniformly the Ord-lesser `H` proposal and the ordinary
/// proposer's phase-0 quorum contains the greater one. In particular:
///
/// - A uniform quorum on the **greater** proposal never splits: the
///   ordinary path's every max prefers that same proposal, so both sides
///   agree on it. The hazard is asymmetric.
/// - Two *fast* decisions never disagree (any two quorums share a recorder,
///   whose one `F[4]` value both must match) -- re-established here through
///   real `Proposer`s, complementing `fast_path_uniformity_tests`'
///   pure-function enumeration.
/// - When they split, the values are exactly (lesser, greater).
///
/// n=3 enumerates all four axes (seedings x every quorum at every step);
/// n=5 enumerates seedings and both phase-0 quorums with full-set
/// spread/gather (the spread/gather choice cannot rescue a split -- any two
/// majorities intersect, so P2's spread always reaches its gather quorum --
/// and n=3's full sweep confirms that axis exhaustively).
#[test]
fn enumerated_two_h_states_split_exactly_when_a_uniform_quorum_holds_the_lesser() {
    let mut divergent_configs = 0u64;
    for n in [3usize, 5] {
        let qs = quorums(n);
        let full: Vec<u32> = (0..n as u32).collect();
        // Per-recorder alphabet: the lesser H proposal, the greater one, or
        // an ordinary drawn-priority proposal (`low(i)`).
        for assignment in 0..3u32.pow(n as u32) {
            let seeds: Vec<Proposal<u64>> = (0..n)
                .map(|i| match (assignment / 3u32.pow(i as u32)) % 3 {
                    0 => lesser_h(),
                    1 => greater_h(),
                    _ => low(i as u32),
                })
                .collect();
            for q_fast in &qs {
                for q0 in &qs {
                    let spread_gather: Vec<(&[u32], &[u32])> = if n == 3 {
                        qs.iter()
                            .flat_map(|s| qs.iter().map(move |g| (s.as_slice(), g.as_slice())))
                            .collect()
                    } else {
                        vec![(full.as_slice(), full.as_slice())]
                    };
                    for (q_spread, q_gather) in spread_gather {
                        let (d1, fast1, d2, fast2) =
                            run_config(n, &seeds, q_fast, q0, q_spread, q_gather);

                        let split = matches!((d1, d2), (Some(a), Some(b)) if a != b);
                        let expected = q_fast.iter().all(|&i| seeds[i as usize] == lesser_h())
                            && q0.iter().any(|&i| seeds[i as usize] == greater_h());
                        assert_eq!(
                            split, expected,
                            "n={n} seeds={seeds:?} q_fast={q_fast:?} q0={q0:?} \
                             q_spread={q_spread:?} q_gather={q_gather:?}: \
                             divergence iff (uniform-lesser fast quorum) and \
                             (greater visible to the ordinary quorum); \
                             got d1={d1:?} (fast={fast1}) d2={d2:?} (fast={fast2})"
                        );
                        if split {
                            divergent_configs += 1;
                            assert_eq!(
                                (d1, d2),
                                (Some(LESSER_H_VALUE), Some(GREATER_H_VALUE)),
                                "a split is always lesser (fast) vs greater (ordinary)"
                            );
                        }
                        if fast1 && fast2 {
                            assert_eq!(
                                d1, d2,
                                "two fast-path decisions must agree (quorum intersection)"
                            );
                        }
                    }
                }
            }
        }
    }
    // Not a magic number to preserve -- a guard that the sweep exercised
    // real divergence rather than vacuously matching an all-false predicate.
    assert!(
        divergent_configs > 0,
        "the sweep must actually reach divergent configurations"
    );
}

/// The same enumeration under the invariant #90 enforces -- at most one
/// distinct `H`-tagged proposal exists -- with everything else free: zero
/// divergences, exhaustively. A fast decision requires a uniform-`H` quorum
/// on *the* `H` proposal, and any other majority intersects it, so the
/// ordinary path's `best` always sees (and, `H` being unbeatable when
/// unique, always adopts) the same proposal. This is the theorem the
/// single-`H` invariant buys, and it is the whole safety story: not #88's
/// check, which the previous test shows cannot rescue a violated invariant.
#[test]
fn with_a_single_h_proposal_no_fast_ordinary_pair_can_split() {
    for n in [3usize, 5] {
        let qs = quorums(n);
        let full: Vec<u32> = (0..n as u32).collect();
        for assignment in 0..2u32.pow(n as u32) {
            let seeds: Vec<Proposal<u64>> = (0..n)
                .map(|i| {
                    if (assignment >> i) & 1 == 0 {
                        lesser_h()
                    } else {
                        low(i as u32)
                    }
                })
                .collect();
            for q_fast in &qs {
                for q0 in &qs {
                    let spread_gather: Vec<(&[u32], &[u32])> = if n == 3 {
                        qs.iter()
                            .flat_map(|s| qs.iter().map(move |g| (s.as_slice(), g.as_slice())))
                            .collect()
                    } else {
                        vec![(full.as_slice(), full.as_slice())]
                    };
                    for (q_spread, q_gather) in spread_gather {
                        let (d1, _, d2, _) = run_config(n, &seeds, q_fast, q0, q_spread, q_gather);
                        if let (Some(a), Some(b)) = (d1, d2) {
                            assert_eq!(
                                a, b,
                                "n={n} seeds={seeds:?} q_fast={q_fast:?} q0={q0:?} \
                                 q_spread={q_spread:?} q_gather={q_gather:?}: with a \
                                 single H proposal every decided pair must agree"
                            );
                        }
                    }
                }
            }
        }
    }
}
