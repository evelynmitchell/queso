# TLA+ model of the abstract QuePaxa consensus core (Algorithm 1)

`QuePaxaAbstract.tla` is a TLA+ model of the *abstract* QuePaxa consensus
protocol — Algorithm 1 of the QuePaxa paper (Section 4.1), for a single SMR
slot — together with a TLC configuration (`QuePaxaAbstract.cfg`) that checks
it **exhaustively** at N = 3 replicas, 2 values, all priority rankings, and a
2-round bound. The check terminates (output pasted at the bottom) and the
safety properties hold:

- **Agreement** — no two replicas decide different values;
- **Validity** — a decided value was some replica's original input;
- **Integrity / DecideOnce** — a replica decides at most once (checked as an
  action property over the transition relation);
- **DecisionUnanimity** — as soon as any replica has decided `x`, every
  replica's candidate value is `x` (the crux of the paper's Agreement proof,
  and the glue of the induction in Reduction R2 below);
- the paper's cross-node subset relationship **`U_i ⊆ C_j ⊆ E_i`**
  (**Lemma B.5**) is `Assert`-checked at the end of every round for every
  reachable tcast outcome — it is *derived* from the tcast guarantees, never
  assumed.

In addition, the two lemmas the Appendix B Agreement proof pivots on are now
`Assert`-checked directly against every reachable tcast outcome (not merely
implied by the top-level invariants passing):

- **Lemma B.4 (set cardinalities)** — every set the algorithm builds
  (`P`, `P'`, `E`, `C`, `U`) has cardinality `> n/2`, checked as each set
  becomes live in its phase;
- **Lemma B.5 corollary (decision-safety pivot)** — whenever a replica's
  decision guard `best(E_i) = best(U_i)` holds, `best` is unanimous across
  every replica's `C_j` and `E_i` (`best(U_i) = best(C_j) = best(E_i)`) — this
  is exactly what forces two same-round deciders to agree (Lemma B.7 case 1).

Both were confirmed to hold across the full state space (106,704 distinct
states, 0 counterexamples). Verified against the authoritative Appendix B text
(`Definition B.1`–`Lemma B.9`).

## What is modeled

Algorithm 1, run in lock-step by all N replicas (the paper's idealized
synchronous model, Section 4.1.1):

```
Input: v ← value preferred by this replica
repeat                                     // iterate through rounds
    p ← ⟨v, random()⟩                      // prioritized proposal
    (P, _)  ← tcast({p})                   // propagate our proposal
    (E, P′) ← tcast(P)                     // propagate existent sets
    (C, U)  ← tcast(P′)                    // propagate common sets
    v ← best(C).value                      // next candidate value
    if best(E) = best(U) then deliver(v)   // detect consensus
```

State per replica: current candidate `v`, original input `inp` (kept only to
state Validity), a decide-once latch `decided`, and a fixed priority `pri`.
A round is three atomic actions — `Phase1`, `Phase2`, `Phase3` — one per
tcast; each action resolves one tcast step for **all** replicas
simultaneously (lock-step). Replicas that have decided keep participating
(the paper's algorithm keeps looping; only the local decision flag stops
repeated delivery). The delivered value is `v` after `v ← best(C).value`,
exactly as written in Algorithm 1 — not `best(U).value`, so the model would
catch a failure of the squeeze `best(E) = best(C) = best(U)` rather than
mask it.

## How tcast is abstracted

No network is simulated. Per the paper, `tcast(In_i)` returns `(R_i, B_i)`
with two guarantees:

- **(G1)** `R_i` is "the set of all proposals **received** by replica i in
  this broadcast step" and includes the inputs of some majority `S`
  (`|S| > n/2`, `∀j ∈ S : In_j ⊆ R_i`). Inputs travel as whole messages, so
  `R_i` is a **union of whole input sets**.
- **(G2)** `B_i` is the input `In_j` of *some* replica `j` — **possibly a
  different `j` for each receiver `i`** ("not necessarily the same as i") —
  such that `In_j ⊆ R_k` for **all** `k`.

Each tcast outcome is parametrized by exactly the choices these guarantees
leave open, per receiver `i`:

- `base[i] ∈ RVals(In)` — which majority-union of whole inputs `i`
  received (G1), where `RVals(In) = { ⋃_{k∈Z} In_k : Z ∈ Majorities }`, and
- `u[i] ∈ InVals(In)` — which whole input is returned as `B_i` (G2), where
  `InVals(In) = { In_k : k ∈ Replicas }`;

with `M = ⋃_m u[m]` (every chosen origin's input must reach *everyone*, by
G2), delivered sets `R_i = base[i] ∪ M`, and `B_i = u[i]`. No filtering is
needed: every choice yields a valid outcome, and no proposal is ever
fabricated. Quantifying over the deduplicated *value* spaces
`RVals`/`InVals` rather than over sender-majority/origin-index functions is
a pure enumeration optimization — the generated outcome family is
identical.

### Faithfulness of the tcast parametrization (it is exact, not a reduction)

*Every* outcome permitted by (G1)+(G2) is generated:

- Take any valid outcome `(R, B)`. Each `R_i` is a union of received whole
  inputs `⋃_{k ∈ Recv_i} In_k` with some majority `S_i` satisfying
  `∀j ∈ S_i : In_j ⊆ R_i`. `Recv_i ∪ S_i` is a majority (`Majorities`
  contains every superset of a majority), and
  `base[i] = ⋃_{k ∈ Recv_i ∪ S_i} In_k ∈ RVals(In)` equals `R_i` exactly,
  because the inputs of `S_i \ Recv_i` are already contained in `R_i`.
- Each `B_i = In_{j_i}` with `In_{j_i} ⊆ R_k` for all `k`; choosing
  `u[i] = In_{j_i}` reproduces it, and adding `M` to every `R_k` changes
  nothing since every chosen origin's input was already contained in every
  `R_k`.
- Conversely every generated outcome satisfies (G1) (each `base[i]`
  contains a majority's inputs) and (G2) (each `u[i]` is a whole input and
  `M ⊇ u[i]` is added to every receiver's set) — soundness.

For the **first** tcast, Algorithm 1 discards `B`, but a real tcast still
produces one, which constrains `R` (at least one input reached everybody).
A single existential origin captures this exactly: an outcome with several
broadcast-to-all inputs is reproduced by putting the extra ones into the
`Z[i]`s.

Note one deliberate strengthening over the prior attempt: `B_i` is chosen
**per receiver** in the second and third tcasts. This is what the paper's
wording allows, and it lets replicas enter the third tcast with *different*
inputs `P′_i`, so their common sets `C_i` — and hence their next candidate
values — can genuinely diverge. A model with a single shared `B` (as in the
earlier WIP spec) keeps all replicas' values identical from round 1 onward
and under-exercises Agreement.

## Why the previous attempt blew up, and the structural fix

The earlier WIP spec (preserved outside the repo) modeled each round as
**one** atomic action that existentially quantified raw subset-vector
spaces for all three tcasts, nested: per state it enumerated on the order of
`6 (priorities) × 512 (P vectors) × 512 × 8 (E, B) ≈ 12.6M` candidate
combinations, filtered them afterwards, and re-drew priorities every round.
Almost all combinations collapse to identical successor states, so TLC
generated 7.7M+ states while finding only 64 distinct ones — an
astronomical branching factor over a tiny reachable space, and the run
never terminated.

Three structural fixes:

1. **Additive, not multiplicative, branching.** Each tcast is its own
   action with the intermediate vectors (`P`, then `E`/`P′`) stored in the
   state and *reset as soon as consumed*. The choices of successive tcasts
   now compose through deduplicated intermediate states instead of
   multiplying inside one action.
2. **Enumeration over outcome-value spaces, all valid by construction.**
   Each tcast action existentially quantifies over `RVals(In)` (the
   distinct majority-unions of inputs) and `InVals(In)` (the distinct whole
   inputs) — never over raw subset vectors or over sender/origin index
   functions. There is no generate-then-filter waste, and choice functions
   that would deliver identical sets are enumerated once: worst case
   `|RVals|^N × |InVals|^N ≤ 4³ × 3³ = 1,728` per state, collapsing toward
   1 when tcast inputs coincide (typical from round 2 on). This cut
   generated states ~40× (from ~160M+ to 3.68M) at identical distinct-state
   count.
3. **Priorities fixed at Init** instead of re-permuted every round
   (Reduction R1 below) — removing a ×6 multiplier from *every* round
   transition while still exploring all rankings.

## State-space reductions and why each is sound

**R1 — Priorities are fixed once, in `Init` (all N! rankings explored as
distinct initial states), instead of re-drawn every round.**
The paper's high-entropy random priorities have exactly one safety-relevant
property: within a round they are pairwise distinct (footnote 4; ties are
assumed away). Proposals from different rounds are never compared — every
set Algorithm 1 builds contains only current-round proposals — so only the
*relative ranking* of the N replicas within a round matters, and the domain
`1..N` with an injective assignment captures every possible ranking
(that part is exact, no loss). Fixing the ranking across rounds *is* an
under-approximation of behaviors, but not of the safety check, by the
following induction:

- The model's round-1 instances already range over **all** value vectors
  (`inp` is an unconstrained element of `[Replicas → Values]`) × **all**
  rankings × all tcast outcomes. A round's dynamics depend *only* on the
  current value vector, the current ranking, and the tcast outcome — not on
  the round number and not on the `decided` flags (the latch only filters
  flag updates). So every possible single round of the varying-priority
  unbounded protocol, from every value vector that could ever arise, *is*
  some checked round-1 instance. These checks establish the per-round
  lemma: cross-node containment holds; every replica's next value is
  `best(C_i).value`; and if any replica decides `x` then every replica's
  next value is `x` and every decision that round is `x`
  (`DecisionUnanimity` at the round boundary).
- Round 2 exercises the only *cross-round* state: carried `v` and carried
  `decided` latches (DecideOnce across a round boundary, decisions in a
  later round than another replica's).
- Induction for the unbounded varying-priority protocol: once some replica
  decides `x`, DecisionUnanimity gives `v ≡ x` at that round boundary; from
  a uniform value vector every proposal in every subsequent round —
  whatever its ranking and tcast outcome — carries `x`, so `v` stays
  uniformly `x` and every later decision is `x` (this closure is itself
  exercised by the checked uniform-`inp` round-1 instances). Hence
  Agreement/Validity/Integrity violations are impossible at any depth with
  any ranking sequence if the checked lemmas hold.

**R2 — MaxRound = 2.**
Sound by the same induction: round dynamics are a function of
(value vector, ranking, tcast outcome), all of which are exhaustively
covered at round 1; round 2 adds the decided-latch carry-over. Deeper
rounds repeat already-checked round instances. (Empirically, raising
MaxRound only replays the same per-round behaviors.)

**R3 — Lock-step, no explicit crashes.**
The paper's abstract model *is* lock-step synchronous (Section 4.1.1), so
that part is faithful, not a reduction. Crash faults (f < n/2) are not
modeled explicitly, but every crash scenario's *information pattern* is
subsumed by the adversarial tcast choices: a replica `c` crashed at some
point is a replica whose inputs the adversary excludes from every later
`Z[i]` and never picks as an origin — all sets are then exactly as if `c`
had stopped invoking tcast. The crashed replica's own extra actions
(updating its `v`, possibly deciding) only *add* decision events, which for
the safety properties checked here can only make violations easier to find,
never mask them. (G1 stays satisfiable since fewer than n/2 replicas crash.)

**R4 — Small finite instance: N = 3, |Values| = 2, priorities from 1..N.**
Standard finite-instance model checking (the check is exhaustive for this
instance, not a proof for all N). N = 3 is the smallest N with non-trivial
majorities; two values suffice to express any disagreement; `1..N` distinct
priorities capture every ranking exactly (see R1).

**Not a reduction (exactness arguments above):** the structured tcast
parametrization (see "Faithfulness"), whole-input-granularity of `R_i`
(the paper defines `R_i` as the set of proposals *received*, and inputs are
single messages), and computing `C`/`U` inside `Phase3` without storing
them (they are consumed in the same lock-step step that produces them; the
containment property that mentions them is Assert-checked at that moment).

## How to run TLC

From the repository root (any TLA+ distribution's `tla2tools.jar` works):

```
java -XX:+UseParallelGC -cp <path-to>/tla2tools.jar tlc2.TLC \
     -workers 4 -config spec/QuePaxaAbstract.cfg spec/QuePaxaAbstract.tla
```

`CHECK_DEADLOCK FALSE` is set in the config because the model intentionally
stutters (`Done`) once `round > MaxRound`.

## Completed TLC output

Actual output of the command above (TLC 2.19, OpenJDK 21, 4 workers; run
from a git worktree of this repo, hence the checkout path; the only line
trimmed is Java's environment-specific `JAVA_TOOL_OPTIONS` echo):

```
TLC2 Version 2.19 of 08 August 2024 (rev: ${git.shortRevision})
Running breadth-first search Model-Checking with fp 38 and seed -8807977093819176801 with 4 workers on 4 cores with 3573MB heap and 64MB offheap memory [pid: 26448] (Linux 6.18.5 amd64, Ubuntu 21.0.10 x86_64, MSBDiskFPSet, DiskStateQueue).
Parsing file /home/user/queso/.claude/worktrees/agent-ad7adec8f7811ddf4/spec/QuePaxaAbstract.tla
Parsing file /tmp/Naturals.tla
Parsing file /tmp/FiniteSets.tla
Parsing file /tmp/TLC.tla
Parsing file /tmp/Sequences.tla
Semantic processing of module Naturals
Semantic processing of module Sequences
Semantic processing of module FiniteSets
Semantic processing of module TLC
Semantic processing of module QuePaxaAbstract
Starting... (2026-07-10 14:11:46)
Computing initial states...
Computed 2 initial states...
Computed 4 initial states...
Computed 8 initial states...
Computed 16 initial states...
Computed 32 initial states...
Finished computing initial states: 48 distinct states generated at 2026-07-10 14:11:47.
Progress(6) at 2026-07-10 14:11:50: 654,351 states generated (654,351 s/min), 59,001 distinct states found (59,001 ds/min), 44,408 states left on queue.
Model checking completed. No error has been found.
  Estimates of the probability that TLC did not check all reachable states
  because two distinct states had the same fingerprint:
  calculated (optimistic):  val = 2.1E-8
  based on the actual fingerprints:  val = 1.5E-10
3681792 states generated, 106704 distinct states found, 0 states left on queue.
The depth of the complete state graph search is 7.
The average outdegree of the complete state graph is 1 (minimum is 0, the maximum 31 and the 95th percentile is 7).
Finished in 17s at (2026-07-10 14:12:04)
```

All four invariants, the `DecideOnce` action property and the in-action
`Assert` of `U_i ⊆ C_j ⊆ E_i` held over the exhaustive search
(3,681,792 states generated, 106,704 distinct, queue drained to 0, ~17 s).

### Non-vacuity sanity probes

To confirm the check is not vacuous, two deliberately false "probe"
invariants were checked against a scratch copy of the model (not committed;
both produced counterexample traces, as desired):

- `NoDecisionEver == ∀i : decided[i] = None` — **violated**: decisions are
  reachable, so Agreement/Validity/DecideOnce are exercised on real
  decisions.
- `PostRoundUniform == (phase = 1 ∧ round > 1) ⇒ ∀i,j : v[i] = v[j]` —
  **violated**: replicas' candidate values can genuinely diverge across a
  round boundary (a consequence of the per-receiver `B_i` choice; a model
  with one shared `B` cannot reach such states), so Agreement is checked on
  non-trivially divergent executions.

## Phase 2 (future work)

This model covers only the *abstract* core (Algorithm 1) atop idealized
tcast. The *concrete* QuePaxa protocol — Algorithm 4 with interval summary
registers (ISRs, Algorithm 2), asynchrony, and the proposer/recorder split
— simulates the abstract core and is planned as a separate Phase-2 model
(refinement mapping from Algorithm 4's four phases to the three tcast
invocations, per Figure 5 of the paper). It is intentionally **not**
modeled here.
