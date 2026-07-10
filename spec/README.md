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

## Phase 2

This model covers only the *abstract* core (Algorithm 1) atop idealized
tcast. The *concrete* QuePaxa protocol — Algorithm 4 with interval summary
registers (ISRs, Algorithm 3), asynchrony, and the proposer/recorder split
— simulates the abstract core and is modeled separately in
`QuePaxaConcrete.tla` / `QuePaxaConcrete.cfg`, documented in the next
(top-level) section of this file. It is intentionally **not** modeled here.

---

# TLA+ model of the concrete QuePaxa protocol (Algorithm 4 + integer ISR)

`QuePaxaConcrete.tla` is a TLA+ model of the **concrete, leaderless**
QuePaxa consensus protocol — Section 4.2 of the paper: the proposer
protocol of **Algorithm 4** (threshold logical clock `s = 4·round + phase`,
four phases per round, majority quorums of recorder replies, proposer
catch-up, the phase-2 decision rule) driving passive recorders that each
hold the **specialized constant-space integer ISR of Algorithm 3** (`S`,
`F_c`, `A_c`, `A_p`; stale-step discard, `+1` carry, skip→nil) — for a
single SMR slot, under **genuine asynchrony** (recorders sit at different
steps; requests and replies are delayed, reordered, dropped and retried;
proposers fall behind and catch up).

`QuePaxaConcrete.cfg` checks it **exhaustively** at the paper's own
Appendix D baseline — 2 proposers, 3 recorders, two full rounds (steps
4–11), 1-bit random priorities, 2 values — and the check **terminates**
(completed TLC output pasted at the bottom of this section) with all
safety properties holding:

- **Agreement** — no two proposers decide different values;
- **Validity** — a decided value was some proposer's original input
  (plus `ValuesFromInputs`, the engine behind it: no value is ever
  fabricated anywhere in the system);
- **Integrity / DecideOnce** — a decision, once made, is never changed
  (checked as an action property over the transition relation; the model
  also latches `pDecStep` and halts decided proposers, mirroring
  Algorithm 4's `return`);
- **DecisionAtPhase2** — decisions occur only on the asynchronous path,
  at steps `s ≡ 2 (mod 4)`;
- **DecisionDominance** — the concrete analogue of the abstract model's
  `DecisionUnanimity` glue: the moment any proposer has decided `v`,
  every proposer whose logical clock has entered any **later round**
  carries a working proposal with value `v`;
- **Monotone** — proposer steps and recorder ISR steps never decrease
  (the premise of Lemma C.2's proof), as an action property;
- **RepliesAhead**, **IsrConsistent**, **TypeOK** — structural sanity
  (Lemma C.2 as a state invariant; the ISR shape of Algorithm 3);
- TLC's **deadlock check is left on** and passes — the concrete analogue
  of the paper's own SPIN property that the models "never deadlock or get
  stuck".

## What is modeled, and how it mirrors Appendix C

The paper proves concrete QuePaxa correct by showing it *simulates*
abstract QuePaxa (Appendix C, Lemmas C.2–C.11). The model is built so
that each ingredient of that simulation argument is either represented
directly or checked against every reachable state:

| Appendix C | In the model |
|---|---|
| Def. C.1 (recorder reply `⟨s′, f′, a′, j⟩`) | the `(st, f, a)` reply records buffered in `pReplies`, keyed by recorder |
| Lemma C.2 (a reply can never be behind the request step, because `record` first raises `S` to `s`) | `Assert(newS >= s)` inside `Rpc`, plus the `RepliesAhead` state invariant |
| Lemma C.3 (catch-up adopts a valid `⟨s, p⟩` state) | the catch-up branch of `ProcessQuorum`: adopt `(st, f)` of a highest-step reply; `f ≠ nil` asserted |
| Lemma C.4 (phase 0 computes `best(P)` over a majority's first values) | the `ph = 0` branch: `p ← BestOf(f′)` over an all-at-step quorum |
| Lemma C.5 (spread/gather realizes tcast T2: a successfully spread proposal is carried into `A_p` by the `+1` transition and cannot be missed by the next step's gather, by quorum intersection) | `Assert(As ≠ {})` in the `ph = 2` and `ph = 3` branches: some gathered prior-step aggregate is always non-nil. This is exactly the consequence of C.5 that the Rust implementation's `expect` in `proposer.rs` relies on; the model would happily produce an all-nil gather if the `+1`-carry/quorum-intersection argument did not close in some interleaving, so TLC re-derives C.5 rather than assuming it |
| Lemmas C.7/C.8 (phases 2/3 compute `best(E)` / `best(C)`) | the `ph = 2` / `ph = 3` gather computations over the replies' prior-step aggregates `a′` |
| Lemma C.9 (asynchronous decision path ⇔ abstract `best(E) = best(U)` decision) | the `ph = 2` decision rule (`p = BestOf(a′)` over the quorum ⇒ deliver `p.value`), plus **DecisionDominance**, which checks C.9's conclusion chained with Lemma B.5: a decision forces every later round's working value |
| Lemma C.10 (leader fast path) | **not modeled** — the model is leaderless-only, matching the implemented concrete core (`crates/consensus/src/proposer.rs`), where no drawn priority can ever equal `H` and the fast-path test is statically dead |
| Lemma C.11 (liveness) | out of scope (probabilistic; same stance as the paper's own SPIN models, which "verify only the algorithm's safety"). The deadlock check plus the non-vacuity probes below stand in for "the model never gets stuck and decisions really happen" |

State, mirroring the implementation (`crates/consensus/src/isr.rs`,
`recorder.rs`, `proposer.rs`):

- per recorder: `rS, rF, rAc, rAp` — exactly Algorithm 3's `S, F_c, A_c,
  A_p` (`None` = nil). `record(s, p)` is transcribed literally: `s < S`
  discard; `s = S` aggregate by max; `s > S` advance with `A_p ← A_c` iff
  `s = S + 1` else `A_p ← nil`; reply `(S, F_c, A_p)`.
- per proposer: `pStep` (the threshold logical clock, starting at 4 =
  round 1 phase 0), `pProp` (the working proposal), `pDec`/`pDecStep`
  (decide-once latch + decision step), `pReplies` (replies buffered for
  the current step, always fewer than a quorum — see C2 below).
- a proposal is `[pri, org, val]`, compared lexicographically on
  `(pri, org)` — the record form of the paper's packed
  `⟨priority, proposer, value⟩` integer. The value needs no role in the
  order: within any step a proposer sends a single value, and every
  comparison the protocol performs is among same-step proposals, so
  `(pri, org)` already determines the proposal (this also makes `BestOf`'s
  maximum unique and `CHOOSE` deterministic). Priority ties across
  proposers are broken by proposer id — precisely the paper's
  "tiebreaking" scheme (Appendix A), the one its own Promela models use,
  and what the Rust `Proposal` `Ord` implements.

### Faithfulness of the proposal order

The paper's packed-integer comparison is lexicographic on
`(priority, proposer, value)`; the model compares `(pri, org)` only. This
is exact, not a simplification, because the `value` position can never be
reached: a proposer's logical clock visits each step at most once
(`Monotone`, checked), and while at a step it sends a single value (phase
0 varies only the priority per recorder), so *within one step* a given
`org` pairs with exactly one value. Every comparison the protocol makes —
a recorder's `AggMax` over one step's arrivals, `BestOf` over first
values `F[s]` of one step, `BestOf` over prior aggregates `A[s−1]` of one
step, the phase-2 equality test — is among same-step proposals, where
equal `(pri, org)` therefore implies equal value. This also makes the
maximum unique, so `BestOf`'s `CHOOSE` is deterministic (and recorder-
and value-symmetric, as Reductions C5/C6 require).

## How asynchrony is modeled

There is **no lock-step anywhere**: the only protocol action is
`Rpc(i, r, pr)` — proposer `i` invokes `record` at recorder `r` for its
current step, the recorder's ISR handles it atomically, and the reply is
nondeterministically **delivered** into `i`'s buffer or **lost**. TLC
explores every interleaving of these RPCs across proposers and recorders,
so recorders sit at different steps, proposers race whole rounds ahead of
each other, gathers observe half-written steps, and the catch-up path is
exercised from every phase (verified non-vacuously; see the probes).
Quorum composition is nondeterministic (whichever `Quorum` replies happen
to be delivered first form `R`), reply loss is nondeterministic per RPC,
and a proposer may re-contact a recorder it has no buffered reply from —
the implementation's retry loop.

## State-space reductions and why each is sound

The abstract model's lesson (a 7.7M-state non-terminating first attempt)
was applied from the start: every intermediate is reset when consumed,
enumeration happens over deduplicated outcome spaces, and every reduction
below is either exact (bisimulation/equivariance) or a strict
over-approximation (may add behaviors, never removes any), hence sound
for the safety invariants checked.

**C1 — Atomic RPC; no explicit message channels.** The implementation's
recorder handles each `record` request atomically and the reply's content
is fixed at that instant; the model's `Rpc` performs mutation + optional
reply delivery in one transition. Every real message-timing behavior maps
to a schedule of these transitions:

- *Delayed request* = scheduling the RPC later (the proposer, which has
  no timeout semantics, just sits at its step; nothing else is blocked by
  its residency, since there is no fairness constraint).
- *Dropped request/reply + retry* = the lost branch (recorder mutated,
  proposer learns nothing) followed by a later re-`Rpc`, whose reply
  carries the recorder's fresh state — exactly what a real retry's reply
  carries (the ISR is idempotent for repeated same-`(s,p)` records, so
  the duplicate mutation is harmless, as in the implementation).
- *Delayed reply* (content fixed at time `t1`, delivered at `t2`) —
  between `t1` and `t2` the proposer takes no action that reads the
  buffer (it only acts when a quorum completes), and buffering the reply
  touches only proposer-local state, so delivering at `t1` instead is an
  equivalent interleaving; the case where later deliveries overtake it is
  the lost-branch-plus-re-read schedule above. (First-received wins per
  recorder per step, as in the implementation's `or_insert`.)
- *Stale requests from an abandoned step* (proposer `i` moved past step
  `s`, its step-`s` request reaches a laggard recorder `r` afterwards):
  reorderable to an in-residency lost-reply RPC. The recorder mutation
  depends only on `(s, p, r`'s state`)`, not on `i`'s; so it suffices to
  show the schedule prefix can be rearranged so the mutation happens
  while `i` is still at `s`. Any event that touches `r` at a step `> s`
  before the stale delivery makes the delivery a no-op (monotone `S`
  discards it) — nothing to model. And any event that touches `r` at a
  step `≤ s` is causally independent of `i`'s progress past `s`:
  influence of `i`'s post-`s` activity propagates only through recorder
  state at steps `> s` (a `record(u, p)` with `u > s` writes only `F[u]`,
  `A[u]`, and `A[u−1]` *by moving* the already-final `A_c`; it fabricates
  no content at steps `≤ s`), and any proposer that absorbs such
  influence jumps its own clock past `s` before acting on it (catch-up),
  after which all its requests are at steps `> s`. So all `≤ s`-step
  events commute before `i`'s advancement, the stale mutation becomes an
  in-residency lost-reply `Rpc(i, r)`, and the remainder of the schedule
  replays identically.

**C2 — A quorum is processed in the transition that completes it, with
`|R| = Quorum` exactly.** This matches the implementation literally
(`process_quorum` fires the moment `responses.len()` reaches the
threshold, so `R` is exactly the first quorum of distinct-recorder
replies received) and the paper's `Await R ← quorum of replies`. Which
quorum forms, with which contents, stays fully nondeterministic through
delivery order and loss. Deferring the processing would not add
behaviors: it reads and writes only proposer-local state, so it commutes
with every other component's transitions. Consequence: the reply buffer
always holds `< Quorum` entries — for the n=3 configuration at most one —
which is a major state-count saving, and the buffer and the completed
quorum are reset/consumed in the same transition (no stale intermediates).

**C3 — Reply-field canonicalization.** A buffered reply's fields that no
code path can ever read from that point on are stored as `None`: `f` is
kept only for phase-0 at-step replies (`BestOf(f′)`) and for ahead
replies (catch-up adoption); `a` only for phase-2/3 at-step replies (the
gathers and the decision test). Catch-up reads only the *highest-step*
reply's `(st, f)`; the phase bodies read only what their branch uses;
nothing else inspects the buffer. Merging states that differ only in
unreadable fields is a bisimulation — exact, no behaviors lost.

**C4 — Fresh per-recorder phase-0 priorities drawn at RPC time (a retry
may redraw).** Algorithm 4 draws an independent random priority *per
recorder* in phase 0; the implementation pins the drawn value in its
`sent` map so a retry resends identical content. The model draws at each
RPC, so a retry after a lost reply may present the same recorder a
*different* priority for the same step. This is a strict superset of the
implementation's behaviors (the redraw can also pick the same value), so
any safety violation of the real system is preserved; the extra
behaviors — a recorder aggregating two priorities from one proposer in
one step — cannot mask violations, only (at worst) flag spurious ones,
and none were flagged. The saving is large: no per-recorder priority
scratchpad in the state vector (an earlier draft pinned priorities and
multiplied the state space by up to `3^|Recorders|` per proposer; it was
the difference between non-termination and a 38-second one-round check).

**C5 — Recorder symmetry.** Recorder identities never enter proposals,
orders, or decisions; the only id-sensitive point in the whole protocol
is the implementation's *arbitrary* deterministic tiebreak (higher node
id) when two replies tie for the highest step during catch-up. The model
makes that choice nondeterministic (a superset containing the
implementation's choice), after which the transition relation is fully
equivariant under recorder permutation, and `Recorders` is declared a TLC
`SYMMETRY` set (`RecSym == Permutations(Recorders)`). Exact for the
invariants checked (all of which are themselves symmetric in recorders);
no liveness properties are checked, so TLC's symmetry/liveness caveat
does not apply (`DecideOnce`/`Monotone` are action invariants, checked on
every generated transition, and are recorder-symmetric formulas).

**C6 — Value-permutation canonicalization of inputs.** `Init` explores
one input vector per orbit under bijections of `Values` (restricted-
growth vectors: for 2 proposers and 2 values, `⟨1,1⟩` and `⟨1,2⟩`, which
cover `⟨2,2⟩` and `⟨2,1⟩`). Exact: no operator inspects a value except
for equality (`Better`/`BestOf` order by `(pri, org)` only; the decision
test is record equality), so a value bijection maps behaviors to
behaviors and symmetric invariants to themselves.

**C7 — Two full rounds (`MaxStep = 11`, steps 4–11).** The paper's own
Appendix D baseline ("executions of two full rounds (logical time steps
4–11)"). Both decision opportunities (steps 6 and 10) and every
cross-round hand-off are inside the bound; proposers park when the bound
is exhausted (the `Done` stutter keeps TLC's deadlock check meaningful).
The argument that two rounds suffice mirrors the abstract model's
Reduction R2, with `DecisionDominance` as the machine-checked glue: a
round's dynamics depend only on the entering value/priority state, all of
which is exercised within rounds 1–2 (including maximally divergent
entries via catch-up and loss), and once any proposer decides `v`,
`DecisionDominance` (checked over the full bounded space) says every
proposer entering any later round carries `v` — from a `v`-uniform round
entry, every gathered/adopted proposal carries `v`, so every later
decision is `v`. As with the paper's own SPIN verification, this is a
finite-configuration check plus an inductive argument, not a proof for
unbounded rounds.

**C8 — No explicit crashes.** As in the abstract model's R3: a crashed
recorder is one the schedule stops delivering RPCs to (quorums of the
remaining majority still form; quorum arithmetic is over the fixed full
membership, as in the implementation); a crashed proposer is one the
schedule stops picking (no fairness assumptions anywhere). Crash
behaviors are a strict subset of the modeled schedules.

**C9 — Two active proposers, three recorders.** The paper's Appendix D
baseline configuration. A replica whose proposer role stays idle (the
normal case in QuePaxa, §4.2.1) is exactly a proposer the schedule never
picks, so the 2-proposer model is the n=3 system with one idle proposer
role; recorder-side behavior is complete. Checking more active proposers
(or more values/priorities/rounds) is a straight config change at
exponential cost — same stance as the paper ("any of these parameters may
be increased in moderation").

**C10 — Leaderless only.** The leader fast path (priority `H`, first
round only) is deliberately out of scope, matching the implemented
concrete core, where the fast-path branch is statically dead. A
leader-based round only *adds* one more way to decide the same
already-best proposal (Lemma C.10 reduces it to C.9); modeling it is
future work alongside any fast-path implementation.

**Not reductions (exact by construction):** decided proposers halt
(Algorithm 4 `return`s; the implementation ignores everything after
deciding); recorders answer every request (a recorder is purely reactive
and cannot "refuse" — loss is modeled on the reply, and request loss is
the schedule never firing that RPC); `pStep` starting at 4 for both
proposers (kickoff skew is subsumed by RPC scheduling freedom, since
nothing observes a not-yet-started proposer).

## How to run TLC

From the repository root (any TLA+ distribution's `tla2tools.jar`):

```
java -XX:+UseParallelGC -cp <path-to>/tla2tools.jar tlc2.TLC \
     -workers auto -config spec/QuePaxaConcrete.cfg spec/QuePaxaConcrete.tla
```

Deadlock checking is intentionally left enabled (see above).

## Completed TLC output

Actual output of the command above at the Appendix D baseline
(TLC 2.19, OpenJDK 21; the environment-specific `JAVA_TOOL_OPTIONS` echo
and the periodic `Progress(...)` lines are trimmed; the final summary is
verbatim):

```
TLC2 Version 2.19 of 08 August 2024 (rev: ${git.shortRevision})
Running breadth-first search Model-Checking with fp 47 ... with 4 workers on 4 cores ...
Parsing file .../spec/QuePaxaConcrete.tla
Semantic processing of module QuePaxaConcrete
Starting... (2026-07-10 19:25:47)
Computing initial states...
Finished computing initial states: 2 distinct states generated at 2026-07-10 19:25:48.
...
Model checking completed. No error has been found.
  Estimates of the probability that TLC did not check all reachable states
  because two distinct states had the same fingerprint:
  calculated (optimistic):  val = 1.1E-4
  based on the actual fingerprints:  val = 2.9E-4
165876224 states generated, 13323585 distinct states found, 0 states left on queue.
The depth of the complete state graph search is 36.
The average outdegree of the complete state graph is 1 (minimum is 0, the maximum 21 and the 95th percentile is 4).
Finished in 12min 44s at (2026-07-10 19:38:31)
```

All eight invariants (`TypeOK`, `Agreement`, `Validity`, `RepliesAhead`,
`DecisionAtPhase2`, `DecisionDominance`, `IsrConsistent`,
`ValuesFromInputs`), both action properties (`DecideOnce`, `Monotone`),
and every in-action `Assert` (the Lemma C.2 reply-step bound, the Lemma
C.3 non-nil catch-up value, and the Lemma C.5 non-nil-gather asserts in
phases 2 and 3) held over the exhaustive search: **165,876,224 states
generated, 13,323,585 distinct, queue drained to 0, depth 36, ~12m44s**.
TLC's deadlock check was enabled and passed (no reachable non-terminated
state is stuck).

**Fingerprint-collision caveat.** Because the reachable space here is
~13.3M distinct states (vs. ~107K for the abstract model), the estimated
probability that two distinct states shared a 64-bit fingerprint and one
was silently skipped is `2.9E-4` (actual) — higher than the abstract
model's `1.5E-10`, purely a function of state count, not of any modeling
weakness. This is small but non-negligible; a confirming re-run with a
different hash seed (`-fp 1`, optional) drives the residual chance that a
collision hid a safety violation down multiplicatively, and is
recommended if this model is used as a load-bearing artifact. The paper's
own more realistic Promela model faced the same tradeoff and resorted to
*bitstate* hashing (Appendix D.2), which is strictly weaker than the
exhaustive full-fingerprint search used here.

### Non-vacuity sanity probes

To confirm the check is not vacuous, five deliberately false "probe"
properties were checked against a scratch module that `EXTENDS
QuePaxaConcrete` (not committed). Each produced a counterexample, as
desired, establishing that the safety properties are exercised on real,
non-trivial executions:

- `ProbeNoDecision == ∀i : pDec[i] = None` — **violated**: decisions are
  reachable, so Agreement / Validity / DecideOnce are exercised on actual
  decisions (not vacuously true because no one ever decides).
- `ProbeNoRound2Decision == ∀i : pDecStep[i] ≠ 10` — **violated**:
  decisions at step 10 (round 2, phase 2) are reachable, so cross-round
  agreement — a proposer deciding in a *later* round than another — is
  genuinely exercised.
- `ProbeNoUndecidedValueSplit` (no two undecided proposers, both already
  in round 2, hold different working values) — **violated** by a
  reachable state where both proposers sit at step 8, undecided, holding
  working proposals with values `2` and `1` respectively. So Agreement is
  checked on genuinely divergent *mid-protocol* states (a split carried
  through a full round of spread/gather/catch-up, not merely the initial
  input split), where a wrong decision would actually be possible.
- `ProbeNoDivergence == ∀i,k : pStep[i] ≤ pStep[k] + 4` — **violated**:
  proposers' logical clocks diverge by more than a full round — real
  asynchrony, with no hidden lock-step barrier.
- `ProbeNoCatchUpJump == □[∀i : pStep'[i] ≤ pStep[i] + 1]_vars` —
  **violated**: the catch-up path fires with a multi-step jump (a
  proposer adopts a much-higher recorder step in one transition). As an
  *action* property, its violation also confirms action properties are
  genuinely enforced under the `SYMMETRY` declaration.

