# Testing Plan

*How Queso becomes robust, validated, verified, and performant — one layer at a
time. Every check here maps back to a property in
[`02-properties.md`](02-properties.md).*

The plan has five pillars, applied continuously as the implementation grows:

1. **Unit & component tests** — the small pieces are correct in isolation.
2. **Property-based / model tests** — invariants hold over randomized inputs.
3. **Deterministic simulation testing (DST)** — the whole system, under
   adversarial schedules, replayable from a seed. *(Primary correctness argument.)*
4. **Formal verification** — model-check the core safety invariants. *(A
   first-class, tracked deliverable — developed in parallel from Phase 1.)*
5. **Performance & robustness benchmarks** — it is fast enough and stays live
   under attack.

---

## 1. The test harness (built in Phase 0, used forever)

The harness is the most important early deliverable — everything else depends on
it. It is a **deterministic simulation kernel**:

- **Injectable clock.** No component reads wall-clock time directly; logical time
  is advanced by the kernel. Hedging delays are expressed in this virtual time so
  timing scenarios are exact and fast.
- **Seeded PRNG.** All randomness (proposal priorities, scheduler choices, fault
  injection) derives from one seed. Reproducibility contract:
  **`seed → identical event trace`** (property D9). This requires eliminating
  Rust's nondeterminism sources: `HashMap`/`HashSet` iteration order (use a fixed
  seed / ordered maps), async executor scheduling, and thread interleaving. Run
  the protocol on a **single-threaded deterministic executor** and/or a sim
  framework such as **`madsim`/`turmoil`**. If this contract is not airtight, DST
  is not reproducible and the entire pillar is compromised — treat it as a Phase-0
  acceptance gate.
- **Pluggable network model** between nodes. The scheduler comes in **two adversary
  classes that test different guarantees** — using the wrong one produces false
  results:
  - **Content-oblivious** (matches assumption A3): may delay, reorder, drop
    (with eventual delivery per A2), partition minorities, induce asymmetric
    connectivity, and DoS whichever replica is *currently leader* (refocusing on
    leadership change) — but chooses **without reading message contents/priorities**.
    *This is the only class under which P14/P15/randomized-termination may be
    asserted.*
  - **Content-aware / strong** (beyond A3): may target specific messages, e.g.,
    deliver a leader's proposal to all `E` sets but no `U` set. Used **only** to
    test fast-path defeat and correct fallback — asserting *safety* and *round-1 →
    leaderless fallback*, **not** unconditional eventual decision.
  - Baseline schedulers *FIFO/reliable* and *random-delay/reorder* remain for
    sanity and ordering-assumption shakeout.
- **Fault injection API.** Crash, pause, restart (with volatile-state loss down to
  durable state), partition/heal, clock skew, slow-node.
- **Trace recorder & checker hooks.** Every externally-visible event
  (submit/decide/deliver/read/ack) is logged with logical timestamps for offline
  invariant checking and linearizability analysis.
- **Shrinking.** On a failing seed, automatically minimize the schedule/fault
  sequence to the smallest reproducer.

> Design note: keep the transport behind an interface so the *same* protocol code
> runs over (a) the in-memory sim and (b) real TCP. Tests use the sim; benchmarks
> and integration use TCP.

---

## 2. Unit & component tests

Targeted at the primitives, before they are wired together:

- **ISR (Interval Summary Register).** Step advancement, first-value capture,
  integer-max aggregation, obsolete-value discard, constant-space behavior
  (property D5). Table-driven and property-based.
- **Threshold logical clock.** `step = 4·round + phase` accounting; advance only
  on threshold; monotonicity.
- **`tcast` (abstract layer).** The two guarantees: every live replica receives a
  majority's inputs (`R`), and at least one input (`B`) reaches all live replicas,
  with `B ⊆ R` across nodes.
- **Priority/`best()` selection.** Correct max, tie-handling (high-entropy → ties
  negligible; test the tie path explicitly).
- **`E`/`C`/`U` set machinery.** The cross-node subset relation `U ⊆ C ⊆ E`, and
  the decision rule `best(E) = best(U)`.
- **Leader fast path.** Leader's reserved priority `H` dominates when it reaches
  quorum first in phase 0; correct fallback to leaderless rounds otherwise.
- **Hedging schedule.** Delay assignment (0, δ, 2δ, …); a proposer proposes only
  if it has not seen earlier progress.

---

## 3. Property-based / model tests

For each property in §B/§C of the property model, a generator produces randomized
scenarios and an oracle asserts the invariant. Examples:

- **Agreement (P1) & log matching (P5).** Generate random crash/reorder schedules;
  assert no two replicas ever hold different decided values for a slot — the
  central anti-property N1 hunt.
- **Validity (P2) / no phantom decision (N5).** Every decided value traces to a
  proposal.
- **Integrity/stability (P3/P4).** Decide-once; decisions immutable.
- **Randomized termination (P14).** Over many seeds under asynchrony, every slot
  eventually decides; record the round-count distribution and assert the ≥ 1/2
  per-round success rate empirically.
- **No destructive interference (P17).** Force *all* proposers active
  simultaneously (δ = 0); assert convergence, not livelock.
- **Timeout-independence (P15).** Sweep δ across pathological values (0, tiny,
  huge, random per node); assert liveness under a majority-alive schedule for all.

A **reference model** (a simple, obviously-correct sequential specification of the
KV store) serves as the oracle for linearizability checks (below).

---

## 4. Deterministic simulation testing (DST) — the workhorse

This is the primary way we gain confidence, following the FoundationDB approach
and the direction Cloudflare states for Meerkat. A DST run:

1. Picks a seed → deterministic schedule + fault plan.
2. Drives a full cluster through a randomized workload (concurrent `get`/`put`,
   CAS, batches) while injecting faults (crash/restart/partition/DoS/slow-leader).
3. Continuously checks safety invariants (P1–P12) inline; on any violation, dumps
   the seed and the minimized trace.
4. Optionally records the operation history for offline linearizability checking.

Key DST scenarios (each a named, seeded suite):

- **Crash storm.** Random replicas crash/restart within the f-envelope; assert
  safety + eventual progress (P11/P12/P13).
- **Majority loss.** More than f down; assert safety preserved, liveness may stall
  (P11, O4) — and that progress *resumes* when a majority returns.
- **Partition / heal.** Split into minority/majority; only the majority side makes
  progress; on heal, minority catches up without divergence (P5, N1).
- **Adversarial leader targeting** *(content-oblivious scheduler)*. DoS whichever
  replica is leader, refocusing on leadership change; assert continued progress via
  leaderless rounds (P16) and no safety loss. Because the scheduler is oblivious,
  asserting eventual progress here is legitimate.
- **Fast-path defeat** *(content-aware scheduler)*. Deliver the leader's proposal
  to all `E` sets but no `U` set; assert *safety* and correct fallback to round
  ≥ 2. Do **not** assert unconditional eventual decision here — a content-aware
  adversary may defeat every leader-based round; that is expected behavior, not a
  bug.
- **Hedging misconfiguration.** δ far from network delay (both directions);
  assert liveness (P15) and no livelock with all proposers active (N6, P17), and
  measure the redundant-effort cost.
- **Restart with state loss.** Kill mid-decision, restart from durable state;
  assert P9/P12 (no lost ack'd write, no divergence) — exercises the Phase-4
  durability design directly.
- **Client-retry / idempotency.** Duplicate and reorder client commands (same
  `(client-id, seq)`) across replicas; assert exactly-once application (P8a) and no
  linearizability violation (P8).

Run these across a large seed corpus in CI (short budget per PR, long nightly
soak). Every historical failing seed becomes a permanent regression test.

---

## 5. Linearizability & consistency checking

- **Online oracle.** A linearizability monitor validates the operation history
  against the sequential KV reference model (property P8; anti-properties
  N2/N3/N4). History checkers operate on recorded histories and are effectively
  language-agnostic; options:
  - **`stateright`** — a Rust model checker with a linearizability tester, the
    most natural in-tree fit given the Rust implementation.
  - **Porcupine** (Go) or **Elle/Knossos** (Jepsen ecosystem, Clojure) for
    checking recorded histories out-of-band as a cross-check.
- **Jepsen-style black-box tests.** Once real TCP transport exists (Phase 7), run
  a Jepsen-style harness against a multi-process cluster with `nemesis`-style
  fault injection (partitions, clock skew, process kills) and check histories with
  Elle. This complements DST by exercising the *real* network stack.
- **Stale-read mode.** Verify that opt-in local reads (D7) are stale-but-never-
  inconsistent (never violate monotonicity within a client session).

---

## 6. Formal verification (first-class deliverable)

Formal verification is a primary, tracked deliverable, developed in parallel with
the implementation from Phase 1 — not a late-stage supplement.

- **Safety model in TLA+ and/or Promela/SPIN.** Model check the safety invariants
  (agreement, validity, integrity) over a small finite configuration (e.g., n = 3,
  bounded rounds/values). The paper ships SPIN-verified Promela models of the
  **concrete** core (Algorithm 4 / ISRs); **model the concrete core as the paper
  did** — it is what the real implementation actually runs. An abstract-protocol
  (Algorithm 1 / tcast) model is useful pedagogically but is a different structure
  from the running code.
- **Rust implementation-level checks.** Because the implementation is in Rust, we
  additionally pursue a path toward implementation-level assurance, as Meerkat is
  doing — e.g., `kani` (bounded model checking), `loom` (concurrency-interleaving
  tests for the async/shared-state code), and `proptest` for property tests. These
  bridge the gap between the abstract spec and the running code.
- **Trace refinement (aspirational).** Mapping recorded DST traces of the real
  async Rust implementation onto a spec and proving refinement is research-grade —
  the abstract/concrete structural gap makes a mapping to Algorithm 1 especially
  hard. Treat full trace refinement as a stretch; the primary, tracked safety
  argument is model-checking the concrete core (above) plus DST on the
  implementation.
- **Scope & honesty about limits.** Model checking must constrain the state space
  (finite replicas/rounds/values) and cannot verify probabilistic liveness. It
  raises confidence in the *logic* of safety; it does not replace DST for the real
  implementation. State this limitation wherever results are reported.
- **Tracking.** The formal model has its own home in the repo (e.g., `spec/`) and
  its checks run in CI on the small configuration so the model cannot silently
  drift from the implementation.

---

## 7. Performance & robustness benchmarks

Measured once real transport + batching exist (Phase 7), on both LAN
(single-region) and WAN (multi-region) topologies:

- **Normal-case throughput & latency.** Commands/sec at varying batch sizes;
  median/p99 latency. Baseline expectation: comparable to Multi-Paxos (the paper
  reports ~584k cmd/s LAN, ~250k WAN). Compare against a Raft/Multi-Paxos baseline
  where feasible.
- **Fast-path hit rate.** Fraction of slots decided in one round-trip under
  normal conditions (D1).
- **Adversarial throughput/latency.** Under DoS/asynchrony, assert continued
  liveness and measure degradation (D3; target: stays live with bounded median
  latency where leader-based baselines stall).
- **Hedging sensitivity.** Sweep δ relative to RTT; find the point where too-small
  δ reverts to `O(n²)` effort, and confirm liveness throughout (P15). The paper's
  reference point: QuePaxa holds full performance at δ ≈ 1/3 RTT where
  Multi-Paxos/Raft need timeouts ≥ 1.8× RTT.
- **Leader-failure recovery time.** Kill the leader mid-stream; measure time to
  resume committing (P16). Compare to a timeout-based baseline's view-change time.
- **Auto-tuning convergence.** In a heterogeneous cluster, measure time to
  converge to the fastest leader and the resulting latency improvement (D4).

Benchmarks must be **reproducible**: fixed seeds/workloads, pinned topology,
recorded environment, and results checked into `docs/benchmarks/` over time.

---

## 8. CI & process

- **Per-PR (fast):** unit + property tests + a bounded DST seed batch + the TLA+/
  SPIN safety check on the small model. Must be green to merge.
- **Nightly (soak):** large-seed DST corpus, linearizability histories, and the
  Jepsen-style suite (once Phase 7 lands).
- **On failure:** the offending seed + minimized trace is attached; a regression
  test pinning that seed is added before the fix is merged.
- **Coverage of the property model:** a checklist ties each property (P1–P17,
  P8a, N1–N6, D1–D11) to at least one passing test; unimplemented ones are marked
  `pending` with the phase that will cover them.

---

## 9. Test → property traceability (summary)

| Test pillar | Primary properties covered |
|-------------|----------------------------|
| Unit/component | ISR (D5), tcast, logical clock, fast path (D1), hedging (P15) |
| Property-based | P1–P4, P14, P15, P17, N6 |
| DST | P1–P13, P15–P17, N1–N6, restart P12, idempotency P8a |
| Linearizability/Jepsen | P8, P8a, P9, P10, N2–N4 |
| Formal (TLA+/SPIN, kani/loom) | P1–P3 (safety core), concurrency of shared state |
| Benchmarks | D1–D4 (performance/robustness) |
