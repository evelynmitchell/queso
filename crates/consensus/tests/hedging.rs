//! Phase 5: hedging (§5.1-5.2) -- the staggered delayed-activation schedule
//! that replaces Phase 3's unconditional δ=0 "every proposer active"
//! behavior, as called for in `docs/00-project-outline.md` Phase 5 and
//! `docs/02-properties.md` P15/P16/D2.
//!
//! Four scenarios, matching the task's four required cases:
//!
//! 1. **D2 -- linear messaging under synchrony**: a good leader plus a
//!    reliable, low-delay network with δ well above the round-trip time
//!    means only the leader ever sends anything; backups stay passive
//!    (`Proposer::activated() == false`) the whole time. Message count is
//!    contrasted against the unhedged (δ=0, all-active) baseline over the
//!    same tick budget to show the `O(n)` vs `O(n^2)` gap directly.
//! 2. **P16 -- fast leader-failure recovery**: crashing the leader still
//!    lets a backup take over and decide, and *how fast* recovery happens
//!    is gated by δ -- a small δ recovers well within a tick budget a large
//!    δ has not even started its rank-1 backup within.
//! 3. **P15 -- δ sweep**: δ ∈ {0, tiny, ~RTT, huge, per-proposer-
//!    misconfigured} all still decide under a majority-alive schedule (no
//!    δ can cause a livelock or permanent stall), with message count
//!    (the "redundant effort" cost) shrinking as δ grows.
//! 4. **Safety unchanged**: Agreement/Validity/Integrity still hold with
//!    hedging enabled, across a seed corpus, under a content-oblivious
//!    adversary and crashes -- hedging only ever changes *when* a proposer
//!    first activates, never the decision rule itself (see
//!    `queso_consensus::proposer`'s module docs). Determinism (seed ->
//!    identical trace) is preserved.

use std::collections::{BTreeMap, BTreeSet};

use queso_consensus::ConcreteCluster;
use queso_sim::ids::NodeId;
use queso_sim::scheduler::{ContentObliviousAdversary, Fifo, SchedulerKind};

fn initial_values(n: u32) -> BTreeMap<NodeId, u32> {
    (0..n).map(|i| (NodeId(i), i)).collect()
}

// ---------------------------------------------------------------------
// Scenario 1: D2 -- O(n) messaging under synchrony.
// ---------------------------------------------------------------------

/// Under `Fifo(1)` (a fixed 1-tick-per-hop, lossless network -- round-trip
/// is a handful of ticks) with δ far larger than that round-trip, only the
/// leader should ever activate: every backup's `Proposer::activated()`
/// stays `false`, and the total message count stays a small multiple of
/// `n` -- nowhere near the `n * n`-ish cost an all-active (δ=0) run over
/// the same tick budget produces.
#[test]
fn d2_leader_only_activation_gives_linear_not_quadratic_messaging() {
    for n in [3u32, 5u32, 7u32, 11u32] {
        let seed = 42;
        let base_delay = 5_000; // far above Fifo(1)'s few-tick round-trip
        let ticks = 200; // enough for the leader to fast-path decide, far short of base_delay

        let mut hedged = ConcreteCluster::new_with_schedule(
            seed,
            SchedulerKind::Oblivious(Box::new(Fifo::new(1))),
            initial_values(n),
            Some(NodeId(0)),
            base_delay,
        );
        hedged.run_slot(ticks);

        assert!(
            hedged.decided(NodeId(0)).is_some(),
            "n={n}: leader failed to decide within the tick budget"
        );
        assert!(
            hedged.decided_via_fast_path(NodeId(0)),
            "n={n}: leader should have decided via the one-round-trip fast path"
        );
        for &id in hedged.replicas() {
            if id != NodeId(0) {
                assert!(
                    !hedged.activated(id),
                    "n={n}: backup {id:?} activated even though δ={base_delay} \
                     is far above the network's round-trip time -- D2 violated"
                );
            }
        }

        // Leader-only cost: n requests + n responses (plus, in principle, a
        // handful of retries -- none needed here since Fifo never drops).
        let hedged_messages = hedged.message_count();
        assert!(
            hedged_messages <= 4 * n as usize,
            "n={n}: leader-only message count {hedged_messages} was not O(n)"
        );

        // Contrast against the unhedged (δ=0) baseline over the identical
        // scenario and tick budget: every proposer sends to every recorder,
        // giving an n^2-shaped cost.
        let mut baseline = ConcreteCluster::new_with_leader(
            seed,
            SchedulerKind::Oblivious(Box::new(Fifo::new(1))),
            initial_values(n),
            Some(NodeId(0)),
        );
        baseline.run_slot(ticks);
        let baseline_messages = baseline.message_count();

        assert!(
            hedged_messages < baseline_messages,
            "n={n}: hedged message count {hedged_messages} should be strictly \
             below the all-active baseline's {baseline_messages}"
        );
        if n >= 5 {
            // For n >= 5 the gap should already be substantial (roughly a
            // factor of n): guard against a regression that merely trims a
            // constant rather than eliminating the O(n) backup fan-out.
            assert!(
                baseline_messages > hedged_messages * 2,
                "n={n}: all-active baseline ({baseline_messages}) was not \
                 meaningfully larger than leader-only hedged cost ({hedged_messages})"
            );
        }
    }
}

// ---------------------------------------------------------------------
// Scenario 2: P16 -- fast leader-failure recovery, gated by δ.
// ---------------------------------------------------------------------

/// Crashing the leader before the slot starts must still let the cluster
/// decide (P16: no disruptive, progress-blocking view change is needed --
/// a backup just takes over once its position in the schedule comes up).
/// A small δ recovers comfortably inside a modest tick budget; a large δ
/// provably has *not yet even activated a single backup* within that same
/// budget (demonstrating recovery time is gated by δ, not instantaneous
/// regardless of configuration) -- yet still recovers once enough ticks
/// pass, however large δ was configured.
#[test]
fn p16_leader_failure_recovery_is_gated_by_delta_but_never_lost() {
    let n = 5;
    let seed = 7;
    let small_delta = 5;
    let large_delta = 2_000;
    let short_budget = 300;

    let mut fast = ConcreteCluster::new_with_schedule(
        seed,
        SchedulerKind::Oblivious(Box::new(Fifo::new(1))),
        initial_values(n),
        Some(NodeId(0)),
        small_delta,
    );
    fast.crash(NodeId(0));
    fast.run_slot(short_budget);
    assert!(
        fast.all_live_decided(),
        "small δ={small_delta}: did not recover within {short_budget} ticks"
    );

    let mut slow = ConcreteCluster::new_with_schedule(
        seed,
        SchedulerKind::Oblivious(Box::new(Fifo::new(1))),
        initial_values(n),
        Some(NodeId(0)),
        large_delta,
    );
    slow.crash(NodeId(0));
    slow.run_slot(short_budget);
    assert!(
        !slow.all_live_decided(),
        "large δ={large_delta}: decided suspiciously fast -- expected recovery \
         to be gated by δ within a {short_budget}-tick budget"
    );
    for &id in slow.live() {
        assert!(
            !slow.activated(id),
            "large δ={large_delta}: replica {id:?} activated before its \
             scheduled delay elapsed"
        );
    }

    // ...but it is never *lost* -- once enough ticks pass for even the
    // last-ranked backup's delay to elapse (crashed leader aside, the
    // last-ranked live backup here is rank n-1, i.e. delay
    // `(n-1) * large_delta`), the slot still decides for every live
    // replica. This is the crux of P15: no δ, however large, causes a
    // *permanent* stall -- it only ever costs latency, bounded by the
    // configured schedule.
    slow.advance(large_delta * n as u64 + 1_000);
    assert!(
        slow.all_live_decided(),
        "large δ={large_delta}: still had not decided after waiting past δ -- \
         P15/P16 violated (a large δ caused a permanent stall)"
    );
}

// ---------------------------------------------------------------------
// Scenario 3: P15 -- δ sweep, liveness for any δ, redundant-effort cost.
// ---------------------------------------------------------------------

/// δ = 0 (unconditional activation), a tiny δ, δ ≈ round-trip, and an
/// absurdly large δ must *all* still make progress -- liveness never
/// depends on δ being configured sensibly (P15/N6). This measures the
/// "redundant effort" cost within a **short, fixed window** just long
/// enough for the leader (always rank 0, delay 0, so unaffected by δ) to
/// fast-path decide: within that window, message count should shrink (or
/// at least not grow) as δ increases, since a larger δ leaves fewer
/// backups with time to activate and redundantly propose before the
/// window closes. (Given long enough to run, *every* configured backup
/// eventually activates regardless of δ -- see
/// `p15_huge_delta_eventually_converges_and_never_permanently_stalls` below
/// for why that is expected and still safe, not a bug: a backup has no
/// way to safely confirm a fast-path decision succeeded at a majority
/// without itself completing a step, so hedging's savings here are a
/// bounded head start, not a promise that slower-scheduled replicas never
/// do any work.)
#[test]
fn p15_delta_sweep_always_makes_progress_and_redundant_effort_shrinks() {
    let n = 5;
    let seed = 11;
    let window = 300; // enough for the delay-0 leader to fast-path decide

    let deltas = [0u64, 1, 3, 50, 5_000];
    let mut message_counts = Vec::new();

    for &delta in &deltas {
        let mut c = ConcreteCluster::new_with_schedule(
            seed,
            SchedulerKind::Oblivious(Box::new(Fifo::new(1))),
            initial_values(n),
            Some(NodeId(0)),
            delta,
        );
        c.run_slot(window);
        assert!(
            c.replicas().iter().any(|&id| c.decided(id).is_some()),
            "δ={delta}: nobody decided within the window -- P15 violated"
        );
        message_counts.push((delta, c.message_count()));
    }

    for pair in message_counts.windows(2) {
        let (delta_a, count_a) = pair[0];
        let (delta_b, count_b) = pair[1];
        assert!(
            count_b <= count_a,
            "δ={delta_a} cost {count_a} messages but larger δ={delta_b} cost \
             more ({count_b}) -- redundant effort should not grow with δ"
        );
    }
    // The two extremes should differ meaningfully within the window:
    // all-active (δ=0) really should cost more than leader-dominated
    // (δ=5000, whose backups have not even had a chance to check yet).
    let (_, all_active_count) = message_counts[0];
    let (_, leader_only_count) = message_counts[message_counts.len() - 1];
    assert!(
        leader_only_count < all_active_count,
        "largest δ ({leader_only_count} messages) was not cheaper than δ=0 \
         ({all_active_count} messages) within the window"
    );
}

/// A single huge δ must never cause a *permanent* stall (P15's crux):
/// within a short window the leader (delay 0, so unaffected by δ) decides
/// while backups correctly have not yet activated, but given enough ticks
/// for the schedule to fully play out, every live replica eventually
/// decides too -- the delay only ever costs latency, never liveness.
#[test]
fn p15_huge_delta_eventually_converges_and_never_permanently_stalls() {
    let n = 5;
    let seed = 23;
    let huge_delta = 50_000;

    let mut c = ConcreteCluster::new_with_schedule(
        seed,
        SchedulerKind::Oblivious(Box::new(Fifo::new(1))),
        initial_values(n),
        Some(NodeId(0)),
        huge_delta,
    );
    c.run_slot(1_000);
    assert!(
        c.decided(NodeId(0)).is_some(),
        "the leader (delay 0, unaffected by δ) should decide promptly regardless of δ"
    );
    assert!(
        !c.all_live_decided(),
        "backups should not have piled on yet within a short window under a huge δ"
    );

    c.advance(huge_delta * n as u64 + 5_000);
    assert!(
        c.all_live_decided(),
        "huge δ={huge_delta} eventually converges fully -- no permanent stall (P15)"
    );
}

/// A deliberately non-monotonic, per-proposer-misconfigured schedule (not
/// expressible as `rank * δ` for any single δ) must still decide, as long
/// as a majority of replicas are alive -- P15/N6 do not carve out an
/// exception for "sensible" schedules.
#[test]
fn p15_per_proposer_misconfigured_schedule_still_decides() {
    let n = 5;
    let seed = 11;
    let budget = 150_000;

    let mut misconfigured: BTreeMap<NodeId, u64> = BTreeMap::new();
    misconfigured.insert(NodeId(0), 9_000); // "leader" configured with a huge delay
    misconfigured.insert(NodeId(1), 0); // an ordinary replica configured with none
    misconfigured.insert(NodeId(2), 4); // small
    misconfigured.insert(NodeId(3), 9_500); // also huge
    misconfigured.insert(NodeId(4), 2); // small
    let mut weird = ConcreteCluster::new_with_delays(
        seed,
        SchedulerKind::Oblivious(Box::new(Fifo::new(1))),
        initial_values(n),
        Some(NodeId(0)),
        misconfigured,
    );
    weird.run_slot(budget);
    assert!(
        weird.all_live_decided(),
        "a per-proposer-misconfigured schedule must still decide -- P15/N6 violated"
    );
}

/// A live majority is not, by itself, enough if the *minority* is chosen
/// pathologically: this is not a hedging-specific concern (crash-tolerance
/// envelope, P11/O4), but confirms hedging does not accidentally make an
/// already-impossible scenario (no live majority ever) look like a
/// livelock instead of the expected stall.
#[test]
fn p15_no_live_majority_still_does_not_falsely_decide_under_hedging() {
    let n = 5;
    let seed = 3;
    let mut c = ConcreteCluster::new_with_schedule(
        seed,
        SchedulerKind::Oblivious(Box::new(Fifo::new(1))),
        initial_values(n),
        Some(NodeId(0)),
        10,
    );
    c.crash(NodeId(2));
    c.crash(NodeId(3));
    c.crash(NodeId(4));
    c.run_slot(20_000);
    for &id in c.live() {
        assert!(
            c.decided(id).is_none(),
            "replica {id:?} decided without a live majority ever being reachable"
        );
    }
}

// ---------------------------------------------------------------------
// Scenario 4: safety unchanged under hedging.
// ---------------------------------------------------------------------

/// Agreement/Validity/Integrity across a seed corpus, with hedging enabled
/// (a moderate δ, so both leader-fast-path and leaderless catch-up paths
/// get exercised), a content-oblivious adversary, and crashes chosen
/// per-seed. Hedging must never be able to turn into a safety mechanism by
/// accident -- it only ever gates *when* a proposer's first `begin_step`
/// runs (see `queso_consensus::proposer`'s module docs).
#[test]
fn safety_holds_under_hedging_with_adversary_and_crashes() {
    for n in [3u32, 5u32] {
        for seed in 0..60u64 {
            let mut rng_seed = seed.wrapping_mul(2654435761).wrapping_add(1);
            let mut next_bool = || {
                rng_seed = rng_seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                (rng_seed >> 63) & 1 == 1
            };

            let adversary = ContentObliviousAdversary::new(1, 6).with_drop_probability(0.2);
            let base_delay = 1 + (seed % 40); // a small, seed-varying δ
            let mut c = ConcreteCluster::new_with_schedule(
                seed,
                SchedulerKind::Oblivious(Box::new(adversary)),
                initial_values(n),
                Some(NodeId(0)),
                base_delay,
            );

            // Crash at most a tolerated minority (f = (n-1)/2), chosen
            // per-seed, leaving a live majority so termination is expected.
            let f = (n as usize - 1) / 2;
            let mut crashed = 0;
            for id in 1..n {
                if crashed < f && next_bool() {
                    c.crash(NodeId(id));
                    crashed += 1;
                }
            }

            c.run_slot(400_000);
            assert!(
                c.all_live_decided(),
                "n={n} seed={seed}: did not decide within the tick budget"
            );

            let decisions: BTreeSet<u32> = c
                .replicas()
                .iter()
                .filter_map(|&id| c.decided(id))
                .collect();
            assert_eq!(
                decisions.len(),
                1,
                "n={n} seed={seed}: replicas disagreed under hedging: {decisions:?}"
            );
            let value = *decisions.iter().next().unwrap();
            assert!(
                (0..n).contains(&value),
                "n={n} seed={seed}: decided value {value} was never proposed"
            );
        }
    }
}

/// Determinism (D9): the same seed, with hedging enabled, must reproduce a
/// byte-identical trace -- hedge timers and rechecks are scheduled purely
/// off the deterministic kernel clock, and the evidence-of-progress signal
/// is a pure function of already-deterministic message delivery, so no new
/// nondeterminism should enter with this phase.
#[test]
fn hedging_preserves_determinism_given_same_seed() {
    let run = |seed: u64| {
        let adversary = ContentObliviousAdversary::new(1, 6).with_drop_probability(0.15);
        let mut c = ConcreteCluster::new_with_schedule(
            seed,
            SchedulerKind::Oblivious(Box::new(adversary)),
            initial_values(5),
            Some(NodeId(0)),
            25,
        );
        c.run_slot(200_000);
        (c.trace().to_canonical_bytes(), c.message_count())
    };
    let (trace_a, count_a) = run(1234);
    let (trace_b, count_b) = run(1234);
    assert_eq!(
        trace_a, trace_b,
        "identical seeds produced different traces"
    );
    assert_eq!(count_a, count_b);
}
