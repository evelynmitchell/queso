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
/// stays `false`, and the message count is exactly `2n` against the `2n^2`
/// an all-active (δ=0) run over the same tick budget produces.
///
/// Both counts are **pinned exactly, not bounded** (issue #117). Measured
/// on this sandbox, and the closed forms hold at every n in the sweep:
///
/// | n | hedged | baseline |
/// |---|---|---|
/// | 3 | 6 | 18 |
/// | 5 | 10 | 50 |
/// | 7 | 14 | 98 |
/// | 11 | 22 | 242 |
/// | 21 | 42 | 882 |
///
/// The n=5 and n=21 rows are the figures `docs/STATUS.md` carried from its
/// first commit and that traced to no test -- #117 exists because they were
/// unsourced, not because they were wrong. Re-measured here, they are right.
///
/// Determinism was checked rather than assumed, because a pinned number
/// that is really a sample is worse than a bound: identical over 5 repeats
/// at n ∈ {5, 21}, and identical across seeds 0..11 at both. That is 12
/// seeds, not all seeds -- but the seed has no route to these numbers that
/// is visible here: `Fifo(1)` is a fixed schedule, and while the seed does
/// perturb the priorities `draw_priority` hands the baseline's backups, a
/// priority changes which proposal wins, never who sends to whom, which is
/// all a message count sees.
///
/// The two counts rest on different premises, which is why only one of them
/// needs the budget guarded:
///
/// - `baseline == 2n^2` held at *every* budget measured (100, 200, 400,
///   1_000, 4_000, 6_000, 20_000 ticks): all-active fan-out happens once and
///   `Fifo` never drops, so there is nothing to retry.
/// - `hedged == 2n` is the *leader-only* cost, and holds only while the
///   budget stays inside δ. Measured, it doubles to `4n` by 6_000 ticks and
///   `8n` by 20_000 as backups activate on schedule -- which is the hedging
///   design working, not a regression. The test asserts `ticks < base_delay`
///   so that premise cannot drift silently; at 200 vs 5_000 the margin is
///   25x.
///
/// Falsifier, run: both pins were mutated, one run each -- repeats add
/// nothing here, given the determinism measured just above -- and each pin
/// is killed by a mutation aimed at it:
///
/// - `begin_step` sending every `record` request twice kills the leader-only
///   pin at n=3, `left: 12, right: 6`.
/// - the same doubling applied to non-leaders only kills the baseline pin at
///   n=3, `left: 30, right: 18` -- and the leader-only pin passes first,
///   which is what shows the two are measured independently rather than one
///   masking the other.
///
/// The first mutation is also what measures the pins as *sharper than the
/// bounds they replace*, rather than that being an argument: with the
/// pre-#117 assertions restored (`hedged <= 4n`, `hedged < baseline`, and
/// `baseline > 2 * hedged` for n >= 5) that same doubling **passes** -- 2n
/// doubled is 4n, which sits exactly on the old bound.
///
/// And nothing else in the tree covers it. Under that mutation this is the
/// *only* failing test in the whole workspace -- `cargo test --workspace
/// --no-fail-fast`, 67 test binaries, one failure, this one. So before #117
/// a regression that doubled the leader's fan-out had no instrument at all:
/// the bound here admitted it and no other test saw it. That is an
/// enumeration over the workspace as it stands, not over every regression
/// shape.
///
/// `--no-fail-fast` is load-bearing in that command, not decoration: plain
/// `cargo test --workspace` stops after the first failing binary, which
/// here reports only 22 of the 67. The claim was originally cited to the
/// plain command, which could not have established it; re-run with the flag
/// (issue #116's work), the count is unchanged at one.
#[test]
fn d2_leader_only_activation_gives_linear_not_quadratic_messaging() {
    for n in [3u32, 5u32, 7u32, 11u32, 21u32] {
        let seed = 42;
        let base_delay = 5_000; // far above Fifo(1)'s few-tick round-trip
        let ticks = 200; // enough for the leader to fast-path decide, far short of base_delay
        assert!(
            ticks < base_delay,
            "test premise: the budget must stay inside δ. Past it the backups activate on \
             schedule and the pinned leader-only count stops describing what is measured -- \
             measured, the hedged count doubles to 4n by 6_000 ticks and 8n by 20_000"
        );

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

        // Leader-only cost, pinned exactly rather than bounded (issue #117):
        // n requests + n responses, no retries, since `Fifo` never drops.
        let hedged_messages = hedged.message_count();
        assert_eq!(
            hedged_messages,
            2 * n as usize,
            "n={n}: leader-only cost should be exactly 2n -- n `record` requests out, \
             n responses back, with no backup ever activating"
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

        // Pinned exactly too: every one of the n proposers fans out to every
        // one of the n recorders and is answered, so 2n^2. Together with the
        // 2n above this states the O(n)-vs-O(n^2) claim as an equality --
        // strictly stronger than the `<= 4n` bound and the `> 2x` contrast
        // this replaces, both of which a regression that merely doubled the
        // constant would have slipped through.
        assert_eq!(
            baseline_messages,
            2 * (n as usize) * (n as usize),
            "n={n}: the δ=0 all-active baseline should be exactly 2n^2"
        );
    }
}

// ---------------------------------------------------------------------
// Scenario 1b: P17 -- no destructive interference.
// ---------------------------------------------------------------------

/// P17: "multiple simultaneously-active proposers never block each other's
/// progress; concurrent proposals converge on a single decided value."
///
/// Before this test (issue #116) nothing asserted P17, and no artifact in
/// `crates/` named it. It was *exercised* incidentally -- the δ-sweep and
/// `termination.rs` both run with several proposers live -- but "the run
/// terminated" is not "the proposers did not interfere". A cluster that
/// converged despite mutual disruption, slowly, satisfied every assertion
/// those tests make.
///
/// What distinguishes interference from mere concurrency is **round
/// escalation**: duelling proposers push each other into higher and higher
/// rounds, so the round count grows with the number of concurrent
/// proposers. Non-interference is that it does not. So the assertion is on
/// rounds, and specifically on rounds being *flat in n* -- the same literal
/// bound at every n, never a bound that has to grow.
///
/// Two legs, both with δ=0 and no leader, so every proposer is active from
/// round 1 and the concurrency is maximal rather than incidental. Each
/// asserts that every replica actually activated, so neither can pass
/// vacuously by quietly testing a single-proposer run.
///
/// Measured on this sandbox:
///
/// - **Synchronous** (`Fifo(1)`, window 300): every replica activates and
///   the slot decides in **round 1** at n ∈ {3, 5, 7, 9, 11} -- flat, with
///   max step 6 (round 1, phase 2) at every one of those n.
/// - **Under 10% loss** (`ContentObliviousAdversary`, seeds 0..99, so 100
///   runs per n and 500 in total): **zero** runs ended with more than one
///   distinct decided value, and the worst-case round count was 3, 6, 6, 5,
///   6 at n = 3, 5, 7, 9, 11. Bounded, and not growing with n -- the
///   distribution shifts but its tail does not.
///
/// `MAX_ROUNDS_UNDER_LOSS` is 8 against that measured worst of 6: headroom
/// over a sampled maximum, deliberately small. The load-bearing part is not
/// the constant's value but that **one constant covers every n**; a bound
/// that had to grow with n is what would falsify P17 here.
///
/// Falsifier, run: removing the phase-0 convergence step in
/// `Proposer::process_phase` -- `self.proposal = best` dropped, so a
/// proposer keeps re-proposing its own value instead of adopting the best
/// seen, which is the textbook duelling-proposers defect -- kills this test
/// **at the round-escalation assertion**: `n=3: 3 simultaneously-active
/// proposers needed 2 rounds, not 1`, `left: 2, right: 1`.
///
/// That the kill lands on the round assertion, not on convergence, is the
/// part worth checking: it means the test reports interference *as*
/// interference. It is not the only instrument, though, and this comment
/// should not imply otherwise -- that mutation also violates safety, so 15
/// tests fail under it across the workspace (`cargo test --workspace
/// --no-fail-fast`, 67 binaries), the other 14 reporting it as an agreement
/// or validity violation.
///
/// One further mutation was tried and is recorded because it did *not* do
/// what was expected. Advancing `self.step += 4` instead of `+= 1` in
/// `process_quorum` was meant to inflate the round count while leaving the
/// decision correct -- a pure interference regression that safety tests
/// would be blind to, which would have shown this test catching something
/// nothing else can. It does not: it kills this test at the *liveness*
/// assertion (`not every live replica decided`) rather than the round one,
/// also kills `concrete_agreement_validity_integrity`, and slows the
/// workspace suite enough that a full run did not finish. So whether a
/// round-escalation defect exists that only this test can see is
/// **unmeasured**; it is not established here, and the honest summary is
/// that this test adds a *diagnosis* the others lack, not coverage they
/// lack.
#[test]
fn p17_concurrent_proposers_converge_without_destructive_interference() {
    /// `step` is `4 * round + phase` (see `queso_consensus::proposer`), so
    /// round 1 spans steps 4..=7.
    fn round_of(step: u64) -> u64 {
        step / 4
    }

    const MAX_ROUNDS_UNDER_LOSS: u64 = 8;

    // Leg 1 -- synchrony. Maximal concurrency, and still one round.
    for n in [3u32, 5, 7, 9, 11] {
        let mut c = ConcreteCluster::new_with_schedule(
            11,
            SchedulerKind::Oblivious(Box::new(Fifo::new(1))),
            initial_values(n),
            None,
            0,
        );
        c.run_slot(300);

        assert!(
            c.replicas().iter().all(|&id| c.activated(id)),
            "n={n}: not every proposer activated, so this run is not the \
             concurrent case P17 is about"
        );
        assert!(
            c.all_live_decided(),
            "n={n}: not every live replica decided"
        );

        let decided: BTreeSet<u32> = c
            .replicas()
            .iter()
            .filter_map(|&id| c.decided(id))
            .collect();
        assert_eq!(
            decided.len(),
            1,
            "n={n}: {} concurrently-active proposers converged on {} distinct \
             values, not one -- P17's convergence half",
            n,
            decided.len()
        );

        let rounds = c
            .replicas()
            .iter()
            .map(|&id| round_of(c.step(id)))
            .max()
            .unwrap();
        assert_eq!(
            rounds, 1,
            "n={n}: {n} simultaneously-active proposers needed {rounds} rounds, \
             not 1. A round count that grows with the number of concurrent \
             proposers is destructive interference -- P17 violated"
        );
    }

    // Leg 2 -- the same, with 10% of messages dropped. Retries raise the
    // round count; interference would make that rise track n.
    for n in [3u32, 5, 7, 9, 11] {
        let mut worst_rounds = 0;
        let mut split_seeds = 0;

        for seed in 0..100u64 {
            let scheduler = ContentObliviousAdversary::new(1, 6).with_drop_probability(0.10);
            let mut c = ConcreteCluster::new_with_schedule(
                seed,
                SchedulerKind::Oblivious(Box::new(scheduler)),
                initial_values(n),
                None,
                0,
            );
            c.run_slot(20_000);

            let decided: BTreeSet<u32> = c
                .replicas()
                .iter()
                .filter_map(|&id| c.decided(id))
                .collect();
            if decided.len() > 1 {
                split_seeds += 1;
            }
            worst_rounds = worst_rounds.max(
                c.replicas()
                    .iter()
                    .map(|&id| round_of(c.step(id)))
                    .max()
                    .unwrap(),
            );
        }

        assert_eq!(
            split_seeds, 0,
            "n={n}: {split_seeds} of 100 seeds decided more than one distinct \
             value under loss -- P17's convergence half"
        );
        assert!(
            worst_rounds <= MAX_ROUNDS_UNDER_LOSS,
            "n={n}: worst-case {worst_rounds} rounds over 100 seeds exceeds the \
             n-independent bound of {MAX_ROUNDS_UNDER_LOSS}. The bound is the same \
             literal at every n on purpose: one that had to grow with n would be \
             the round escalation P17 forbids"
        );
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
