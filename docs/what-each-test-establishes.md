# What each test establishes — and what it does not

A decision table for this repository's test surface, written after issue #83,
in which a rare failure was hunted for three nights with instruments whose
detection power had never been measured — while the defect sat inside a space
of eight states that could be enumerated in a millisecond.

The companion to `docs/investigating-with-logs.md`: that one is about
preserving evidence, this one is about knowing what a green test entitles you
to say.

---

## 1. "Seed" means three different things here

Nothing in this workspace draws a seed from entropy. `clippy.toml` denies
`rand::thread_rng` and `rand::random` workspace-wide, deliberately, so that a
run is reproducible from its seed alone. That is a good property. It also
means the word "seed" covers three regimes that behave in opposite ways, and
conflating them is how a test comes to be trusted for something it cannot do.

| regime | the seed fixes | re-running the same seed | coverage over time |
|---|---|---|---|
| **Sim tests** (`for seed in 0..24u64`) | the entire execution — kernel, scheduler, priorities | byte-identical, forever | **none**: a frozen corpus |
| **Nightly soak** | the turbulence *schedule* only; threads, timers and TCP are real | a different interleaving every time | new window nightly (`first_seed = run_number * seeds`) |
| **Leader experiment** | as the soak, but the window is pinned | different every time | none, by design |

Two consequences worth stating plainly.

**A sim test is a golden corpus, not a sample.** `for seed in 0..24u64`
explores the same twenty-four schedules on every CI run until someone edits the
literal. Running it a thousand times adds nothing. It is a regression pin — a
genuinely useful thing — but it is not randomized testing and must not be
reported as though the number 24 were evidence of breadth. Nowhere in this
repository does a corpus size have a documented rationale; `SEED_CORPUS_SIZE =
300` appears three times as a bare constant.

**A soak seed does not reproduce a soak failure.** The seed reproduces the
faults injected, not the interleaving that turns them into a bug. Seeds 72 and
78 were not "the failing seeds" of #83; they were seeds during which a failure
happened once. Re-running the identical window later produced nothing — see §3.

---

## 2. The decision table

For each instrument: the question it answers, what each outcome licenses, and
whether its power to detect what it was built for has actually been measured.

| instrument | question | pass licenses | fail licenses | detection power |
|---|---|---|---|---|
| `proposer::fast_path_uniformity_tests` | can the fast path decide two values at one slot? | **no, exhaustively** — every state × every quorum, n ∈ {3,5,7} — *for two fast decisions*; a fast decision against the ordinary path is the next row's question | a bare Agreement violation | **1.0** — the state space is finite and fully enumerated |
| `consensus/tests/two_h_proposals.rs` | with **two** distinct `H` proposals, can a fast decision and an ordinary decision disagree? | nothing — its *pass* is the counterexample standing: they **do** disagree, exactly when a uniform quorum holds the Ord-lesser proposal (#92). The green licenses "the single-`H` invariant (#90) is load-bearing and the uniformity check (#88) is not a recovery" | the ordinary path was strengthened, or the fast path changed — re-derive #92 | **measured**: the pre-#88 permissive rule fails 2 of 3 tests; the single-`H` test's falsifier is the two-`H` sweep itself |
| `consensus/tests/two_h_proposals.rs::with_a_single_h_proposal…` | with at most **one** distinct `H` proposal, can a fast/ordinary pair disagree? | **no, exhaustively** over the bounded scenario space (every seeding × every minimal quorum at every step, n ∈ {3,5}) — the theorem #90's invariant buys | Agreement is broken even under the invariant | as above; scope limit: one fast + one ordinary proposer per run, not two racing ordinary proposers (that is the seed corpora's and the TLA+ model's territory) |
| `restart_agreement.rs::a_catch_up_probe_never_carries_the_reserved_priority` | can a learner win a slot outright? | no probe carries `H`, on any of 24 seeds | a probe can win a slot — #83's mechanism is back | **measured**: reverting `begin_catch_up` fails it 24/24 |
| `net/tests/persist_fidelity.rs` | does a populated `Durable` survive the disk round trip? | not a serialization gap | recorder/applied state is lost on restart | **measured**: clearing recorders in `from_durable` fails it |
| `restart_agreement.rs` (3 divergence tests, 60 scenarios) | does a restarted leader diverge? | *these 60 frozen schedules* do not diverge | a reproduction, in the simulator | **measured at zero** for #83 — see below |
| `restart_agreement.rs::the_mixed_h_state_was_not_observed_by_this_snapshot` | is the mixed-`H` state visible in the sim? | not observed — **not** "unreachable" | reproduce #83 here rather than in the soak | **unknown, likely low**: post-hoc `F[S]` snapshot, not a trace |
| nightly soak | does anything diverge under real turbulence? | nothing diverged *this window* | a real, adjudicable occurrence | ~12.5%/seed for #83, CI ≈ [3.5%, 29%] |

### The row that matters most

`restart_agreement.rs`'s three divergence tests have **demonstrated zero
detection power** for the bug they were written to hunt: sixty scenarios,
crash-restarting a leader mid-workload with the fast path armed, with catch-up
probes verifiably deciding — and no reproduction. That is a measurement, not a
suspicion.

They remain worth keeping as regression pins. But their green must never be
read as "restart is safe", and a future investigation must not count them as
having ruled anything out. An instrument whose sensitivity to the target is
zero contributes zero evidence about the target, however many scenarios it runs.

---

## 3. Two errors this table caught, in work already merged

**The rate was asserted, then repeated until it looked like a fact.** #83 was
described throughout as failing "roughly two seeds in eight". That was the
worst single night. Measured at n=3:

| run | divergent seeds | of |
|---|---|---|
| 8 | 65 | 8 |
| 9 | 72, 78 | 8 |
| 10 | 81 | 8 |
| leader experiment 1, arm n0 | — | 8 |

**4 in 32 ≈ 12.5%**, 95% CI roughly [3.5%, 29%]. (Run 9's seed 77 was n=5, a
different configuration.) The inflated figure was written into
`leader-experiment.yml` and shaped the design of the experiment that used it.
This is §3 of `investigating-with-logs.md` — "the rate is asserted rather than
measured" — committed inside the tool built from that document.

**The experiment was underpowered, and nobody computed it.** At p = 0.125, an
eight-seed arm comes back empty 34% of the time. Leader experiment run 1
returning nothing was therefore not bad luck; it was the design.

| seeds per arm | P(arm shows ≥ 1 divergence) at p = 0.125 | wall clock |
|---|---|---|
| 8 | 66% | ~26 min |
| 16 | 88% | ~50 min |
| 24 | 96% | ~75 min |

At the *low* end of the confidence interval (p = 0.035) even 24 seeds is 42%
likely to come back empty — which is the honest reason to prefer the
enumeration below, rather than merely the faster one.

**Before running a stochastic experiment, write down the per-trial rate you
believe, and the number of trials that belief implies. If you cannot state the
rate, you cannot size the experiment, and a clean result will not mean
anything.**

---

## 4. Enumerate the state; do not sample the system

The decisive move on #83 was not a better soak. It was noticing that
`fast_path_value` is a **pure function of a bounded state**, and that the
property at risk — Agreement — is therefore decidable by enumeration.

The whole space:

- `n` recorders, each holding one `F[4]` value from a small alphabet.
- Every quorum: every subset of size `>= n/2 + 1`.
- 8 states × 4 quorums = **32 evaluations at n=3**; 32 × 16 = 512 at n=5.

`no_two_quorums_can_fast_decide_different_values` runs exactly that, for
n ∈ {3, 5, 7}, in under a millisecond, and asserts the real property: *no two
quorums over the same recorder state may fast-decide different values.*

### The failure combination, exactly

Under the old permissive body the decided value was `first_reply`'s — the
lowest `NodeId` in the map, since `BTreeMap` iterates in key order. A quorum of
size `>= q` drawn from `0..n` has lowest member `i` for any `i <= n - q`. So
the set of values the fast path could return was exactly

```
{ F[i] : 0 <= i <= n - q }
```

and divergence was possible **iff `F[4]` was not uniform across recorders
`0 ..= n - q`** — the first two node ids at n = 3, the first three at n = 5.
Enumerated: **4 of 8 states diverge at n = 3, 24 of 32 at n = 5.**

Majority intersection does not save this. Any two quorums do share a recorder —
but the old rule could return a value that shared recorder did not hold, which
is precisely how the intersection argument was escaped.

### The comparison

| route | trials | wall clock | result |
|---|---|---|---|
| nightly soak | 32 seed-runs over 3 nights | ~13 h of turbulence | 4 occurrences, cause unsettled |
| leader experiment | 16 seed-runs, 2 arms | ~26 min | inconclusive by construction |
| enumeration | 32 evaluations | < 1 ms | complete, with the failure combination characterised |

### The rule

**When the decision under test is a pure function of bounded state, enumerate
the state.** Sampling the whole system is what you do when you cannot identify
that function — and the cost of not looking for it is measured in nights.

Ask, before reaching for another seed corpus:

1. What is the *function* whose output would be wrong?
2. What state does it actually read? Is that state bounded?
3. If bounded — enumerate it, and assert the property, not an instance.
4. If genuinely unbounded — then sample, and *first* write down the rate you
   believe and the trial count it implies.

The soak keeps its place: it is what finds the failures nobody has thought to
enumerate, and it exercises the real network, real threads, and real disk that
no enumeration models. It is a discovery instrument, not an adjudication one.
Once it has told you *where* to look, stop running it and go read the function.

---

## 5. Housekeeping rules this implies

- **State a corpus size's rationale, or admit there isn't one.** `0..24u64`
  with no comment reads as a measurement. It is a runtime budget.
- **Never report a frozen corpus as randomized testing.** Say "24 fixed
  schedules", not "24 random seeds".
- **Record measured detection power** where it is known, and say "unmeasured"
  where it is not. A test with no falsifier and no measured power establishes
  nothing but that it compiles.
- **A test asserting an absence must say what it cannot see.** See
  `the_mixed_h_state_was_not_observed_by_this_snapshot`, whose name is the
  claim and whose docs carry the limits.

---

## 6. A tripwire test, and what happened to it

`a_restarted_leaders_catch_up_probe_carries_the_reserved_priority` was written
to assert behaviour believed to be **wrong**: that a restarted leader's probe
carries `H`. That is an odd thing to pin, and it earned its place twice over.

First, it made the mixed-`H` quorum a reachable state rather than a
hypothetical, which is what stopped
`no_two_quorums_can_fast_decide_different_values` from guarding nothing.

Second, its failure message named its own successor:

> *"if the latter, #83's fix has landed and this test should be inverted, not
> relaxed"*

When `begin_catch_up` was changed to pass `leader: None`, it failed on 24/24
seeds and said so. It is now
`a_catch_up_probe_never_carries_the_reserved_priority`, asserting the
opposite, with the same measured power in the other direction.

**When you find behaviour you believe is wrong but are not ready to change,
pin it — and write the failure message to the person who will change it.** A
characterisation test that says what its own red means costs one paragraph and
converts a future surprise into a confirmation.
