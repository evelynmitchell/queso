# Conformance Matrix: property → evidence

*Every property in [`02-properties.md`](02-properties.md), the artifact that
verifies it, and **the class of evidence that artifact provides**. Snapshot as
of 2026-09-03.*

**"Every property" is checkable, and was checked**: the set of bold identifiers
matching `P<n>`/`N<n>`/`D<n>` in `02-properties.md` and the set in this file are
compared with `grep -oE` and are equal — 35 on each side (P1–P17 including P8a,
N1–N6, D1–D11). A property added to the model without a row here breaks that
equality; nothing enforces the comparison automatically, so it is a manual check
(see §7).

This is the doc [`STATUS.md`](STATUS.md) §4b calls "still the highest-value doc
item". It is not a summary of the test suite — [`03-testing-plan.md`](03-testing-plan.md)
covers strategy and [`what-each-test-establishes.md`](what-each-test-establishes.md)
covers what different *kinds* of test can establish. This one answers a narrower
question, per property: **what would have to be wrong for this to be a lie?**

---

## 1. How to read it

The evidence classes are [`CLAUDE.md`](https://github.com/evelynmitchell/queso/blob/main/CLAUDE.md)'s, unchanged:

| Class | Means |
|---|---|
| **enumerated** | Exhaustive over a named, bounded space, stated with its bounds. |
| **model-checked** | A TLC run over a named config, with state counts. |
| **tested, power measured** | A test exists *and* its detection power was demonstrated by mutation. |
| **tested, power unmeasured** | A test exists; nobody has shown it can detect the failure it guards. |
| **argued** | A reasoning sketch. Legitimate, but never at equal confidence with the above. |
| **assumed** | Carried from the paper, or from another claim's premises. |
| **not implemented** | The feature does not exist; there is nothing to verify. |

**Provenance of the "power measured" rows.** Each cites the falsifier documented
at that location in-tree. All **29** markers say `Falsifier, run:`, but they do
not all rest on the same evidence, and the difference matters:

- **20 were executed in #114**, each mutation applied and its outcome observed,
  with the observed values recorded in the marker. Before #114 these described a
  mutation without saying whether anyone had run it, so this matrix classed rows
  as "power measured" partly on the strength of predictions. All 20 predictions
  held once the described mutation was faithfully reconstructed.
- **2 were executed in #113** (`log_safety.rs`, `cluster.rs`), which measured
  P5/P6/P7, P10 and P11 from scratch rather than checking an existing marker.
- **7 were already marked `Falsifier, run:`** by the changes that introduced them
  (`durability_faults.rs` ×4, `proposer.rs` ×2, `restart_agreement.rs`). Neither
  #113 nor #114 re-ran these; their counts are carried over as recorded —
  *assumed* from the introducing change, not re-measured here.

Two markers record something other than a plain kill, and say so where they sit:
`postmortem.rs`'s `identical_logs_agree_on_every_slot_they_share` (the loop-only
truncation **survives** it and is caught by three sibling tests instead), and
`logs_with_nothing_in_common_are_no_overlap_not_agreement` (its falsifier is a
compile error, not a failing run).

**The two model-checked configs**, referenced throughout:

| Config | Bounds | Scale | Runs |
|---|---|---|---|
| `spec/QuePaxaAbstract.cfg` | N=3 replicas, \|Values\|=2, MaxRound=2, all 3! priority rankings | 3,681,792 states generated / 106,704 distinct | **per PR** (`ci.yml`) |
| `spec/QuePaxaConcrete.cfg` | NP=2 proposers, 3 recorders, MaxStep=11 (two full rounds), PriMax=2, \|V\|=2, `RecSym` symmetry | 165,876,224 generated / 13,323,585 distinct | **nightly** (`tlc-nightly.yml`), ~15 min |

Both pin exact state counts in CI, so a model or config edit that silently shrank
the state space fails the job rather than passing every invariant over a smaller
space.

---

## 2. Safety (§B) — P1–P12

| Property | Verified by | Class |
|---|---|---|
| **P1** Agreement | `Agreement` invariant, **both** specs | model-checked |
| | `consensus/tests/two_h_proposals.rs` — every configuration with two distinct `H`-tagged proposals (#92); guards its own non-vacuity with `divergent_configs > 0` | **enumerated** |
| | `consensus/src/proposer.rs` fast-path unit tests — *falsifier: restoring the permissive `Some(p) if p.priority == H` body fails three of the five* | tested, power measured |
| | `smr/tests/restart_agreement.rs` (#83) — *falsifier: restoring `leader_policy.leader_for(slot)` in `begin_catch_up` fails 24/24 seeds* | tested, power measured |
| | `consensus/tests/{agreement_validity_integrity,concrete_agreement_validity_integrity,partition,fast_path}.rs` | tested, power unmeasured |
| | `soak/` + `conformance/` observers, real processes under fault | tested; **observer** power measured (`soak/src/postmortem.rs` falsifiers), **protocol** power unmeasured |
| **P2** Validity | `Validity` invariant, both specs; `ValuesFromInputs`, concrete | model-checked |
| | `consensus/tests/{agreement_validity_integrity,concrete_agreement_validity_integrity}.rs` | tested, power unmeasured |
| **P3** Integrity / decide-once | `DecideOnce` temporal property, both specs | model-checked |
| | same two consensus test files, plus `fast_path.rs` | tested, power unmeasured |
| **P4** Stability | `DecideOnce` (defined as "a decision, once made, is never changed"); `Monotone`, concrete | model-checked |
| | `consensus/tests/proposer_start_contract.rs` (#13) — re-kicking a *decided* proposer must change nothing: not the decision, not the step, not the fast-path provenance, not one byte on the wire — *falsifiers: removing the `decided.is_some()` guard from `Proposer::start` fails the message-count and step assertions on the first seed, and makes every replica report `decided_via_fast_path() == true`* | tested, power measured |
| | same consensus test files | tested, power unmeasured |
| **P5** Log matching / prefix consistency | `smr/tests/log_safety.rs` — *falsifier, run (#113): making `finish_attempt` record the replica's own proposal instead of the decided command fails all 7, at the P5/P6 slot compare* | tested, power measured |
| | `conformance/tests/faults.rs` — the same mutation fails 5 of its 6 | tested, power measured |
| | `soak/` real-process observers | tested, power unmeasured |
| **P6** Total order | `smr/tests/log_safety.rs` — same assertion, same falsifier as P5. **P6 has no mutation of its own**: `finish_attempt` only appends at the current frontier, so two replicas can apply in different orders only by applying different commands, which is a P5 violation. Stated because a separate P6 row would otherwise imply separate evidence | tested, power measured (via the P5 mutation) |
| | `conformance/` observer; `chain/` hash chain | tested, power unmeasured |
| **P7** Gap-free application | `smr/tests/log_safety.rs` — *falsifiers, run (#113): advancing `next_slot` without appending fails all 7; applying each decided command twice also fails all 7 — both at the frontier-vs-log-length check* | tested, power measured |
| | `conformance/tests/faults.rs` catches the first (2 of 6) and **none** of the second: a defect that inflates every replica's log identically produces no divergence, and that suite has no frontier-vs-length assertion. Cross-replica comparison cannot see uniform wrongness | argued from both assertion sets; the counts are measured |
| | `chain/src/lib.rs` | tested, power unmeasured |
| **P8** Linearizability | `smr/tests/linearizability.rs` (randomized concurrent put/get, checked offline) — *falsifier, run (#125): the P10-A stale-read mutation kills 4/5, re-verified rather than inherited. Two mutations that produce genuine P8 violations, catch-up-skips-apply and dedup-off, kill **0/5** here and are caught by restart/durability tests and by `idempotency.rs` instead; the workload never crashes a replica and never reissues a `(client, seq)`, so neither class is reachable in it. A vacuous checker is caught 8/8 by this file's own control. See §6.13* | tested, power measured — **for the stale-read class only**; P8's coverage is this file ∪ the restart and idempotency suites |
| **P8a** Idempotent commands | `smr/tests/idempotency.rs` — *falsifier, run (#125): dedup disabled kills 3/4 8/8 on their own value assertions; the `>= ` → `>` off-by-one kills 1/4. Both counts are post-repair: the file's exact-duplicate test had **zero** power before #125 and the fourth test did not exist. See §6.13* | tested, power measured |
| **P9** No lost committed writes | `smr/tests/restart_recovery.rs`; `net/tests/restart_recovery.rs` (#36, majority reboot of real OS processes) | tested, power unmeasured |
| | `net/tests/durability_faults.rs::acknowledged_writes_survive_rolling_restarts_under_load` — *falsifier, run: disabling the boot-time reload in `driver.rs` fails it 8/8, every time at the lost-write assertion* | tested, power measured |
| | Write-before-reply is asserted at runtime in `driver.rs` by a **release-mode** assert, so the check is in the shipped artifact rather than only under `cargo test`. No falsifier has been run for the assert itself | tested, power unmeasured; enforcement **in-build** |
| **P10** Read safety under lag | `smr/src/cluster.rs::read_after_write_on_a_different_replica_still_sees_it` (the most direct P10 test; **was not in this row before #113**) plus `smr/tests/{linearizability,idempotency,restart_recovery}.rs` — *falsifier, run: short-circuiting a `Get` from local state in `SmrCluster::submit` fails 13 in-process tests* | tested, power measured |
| | `net/` real-process suite — the same defect written into `SmrNode::submit` (whose only caller is `net/src/driver.rs`, the shipped path) kills **15 `queso-net` tests across 8 files and 0 of the 60 `queso-smr` tests**. The crate has two independent submit implementations, so each suite is the *only* instrument for its own site — see §6.9 | tested, power measured |
| **P11** Safety under > f crashes | `smr/tests/log_safety.rs::log_safety_holds_even_without_a_live_majority` — **had measured-zero power until #113** (§6.9): it crashed the majority before submitting anything, so every log was empty and the assertion was vacuous. It now decides a prefix first, asserts that prefix is non-empty, and asserts no further slot decides after quorum is lost; all three P5/P7 mutations now fail it | tested, power measured |
| | `consensus/tests/partition.rs` | tested, power unmeasured |
| **P12** Restart safety | `net/tests/durability_faults.rs` (#39) — four real-process fault tests. *Falsifiers, run (§6.8 and the file's "Detection power" docs): disabling the boot-time reload fails the torn-snapshot test 8/8 and the rolling-restart test 8/8, and leaves the disk-full test passing 8/8; swallowing the persist error fails the disk-full test 8/8* | tested, power measured (3 of the 4 tests) |
| | The fourth, `an_unacknowledged_write_is_lost_or_kept_but_never_split`, dies 41/100 under the reload mutation. Re-measured in #115: **5/100 of those are the never-split assertion itself** (9/156 pooled, 5.8%, Wilson 95% CI [3.1%, 10.6%]), 36/100 the separate "an acknowledged write must survive" check, and 0 the burst-acknowledged check, which is structurally dead. The earlier "never-split has no falsifier" reading came from 16 runs; at 5.8% those come back empty 39% of the time | never-split: tested, **power measured** (low). Burst-acknowledged: **measured zero**, mechanically. See §6.8, §6.12 |
| | `net/tests/persist_fidelity.rs` (#83), `group_commit.rs`; `smr/tests/{restart_recovery,restart_agreement}.rs` | tested, power unmeasured (except `restart_agreement.rs`, above) |

---

## 3. Liveness (§C) — P13–P17

| Property | Verified by | Class |
|---|---|---|
| **P13** Majority progress | `consensus/tests/partition.rs` — *falsifier, run (#152): inflating the quorum rule from a majority to the whole membership (safety-preserving, liveness-destroying) kills the two `majority_decides_…` tests 8/8, on P13's own "majority replica never decided under partition" assertion. The same mutation at the **abstract** site (`tcast`) leaves this file green 0/8 — sections 1–3 never reach `tcast`, and section 4's `should_panic(expected = "tcast failed to converge")` matches the mutant's panic too. See §6.15* | tested, power measured — for the concrete driver; **zero** for the abstract site |
| | P13's coverage is much wider than this row used to imply, and the mutation maps it: 16 tests across `queso-consensus` and `queso-smr` (incl. `concrete.rs`'s and `smr/cluster.rs`'s `progresses_with_a_crashed_minority`, `fast_path.rs`'s crashed-leader fallback, `smr/tests/restart_recovery.rs`), plus `net/tests/cluster.rs::cluster_survives_at_its_fault_tolerance_boundary` at the real-node level | tested, power measured |
| | `consensus/tests/proposer_start_contract.rs` (#13) — re-kicking an *undecided* proposer restarts round 1, so a driver can un-stall one that spent its whole first push partitioned from every quorum — *falsifier: making `start` fully idempotent leaves the replicas parked at their pre-kick step and the rewind assertion fails* | tested, power measured |
| **P14** Randomized termination | `consensus/tests/termination.rs`, `concrete_termination.rs` — content-oblivious adversary, per A3. *Falsifier, run (#125): **none exists**. Replacing the drawn priority with a constant, at each of the two randomization sites separately, is on-path (both distributions move) and kills **0 of 441** tests in the tree. `mean < 4.0` is one-sided and the defect moves the mean **down**; and under a content-oblivious adversary a deterministic tie-break by `origin` converges as well as a random draw. See §6.13. #150 then built the content-aware adversary §6.13 conjectured would detect it: it separates the builds in the **opposite** direction, costing the randomized build rounds and the constant one none — see `consensus/tests/termination_under_targeted_adversaries.rs` and §6.14* | tested, power measured — **zero**, structurally; no falsifier known after a bounded search |
| | The ≥ ½ per-round bound itself | **assumed** (paper, §4; not independently derived here) |
| **P15** Timeout-independent liveness | `consensus/tests/hedging.rs` — five tests named for it (δ-sweep, huge δ, per-proposer misconfiguration, no-live-majority, and #125's huge-δ-with-a-dead-leader). *Falsifier, run (#125): dropping the freshness test in `maybe_activate_after_hedge` — a proposer that defers forever behind a frozen recorder step — kills the misconfiguration test and the new crux test 8/8 on their own assertions, and **0/8** the δ-sweep and huge-δ tests. The huge-δ test called itself "P15's crux" and cannot detect a permanent stall: its leader stays alive, so the decision arrives regardless. See §6.13* | tested, power measured — 2 of the 5 named tests carry it |
| **P16** Leader-failure recovery | `consensus/tests/hedging.rs::p16_…`; `compare/tests/leader_dos.rs` (real cluster, leader isolated). *Falsifier, run (#125): the hedge-gate freshness mutation kills `p16_…` 8/8 on its own first assertion — "small δ did not recover within 300 ticks". With the leader dead, a backup's own activation is the only route to a decision, which is P16's claim exactly* | tested, power measured |
| **P17** No destructive interference | `consensus/tests/hedging.rs::p17_concurrent_proposers_converge_without_destructive_interference` (#116) — δ=0, leaderless, so every proposer is active; asserts that all activated (anti-vacuity), that the round count is **flat in n** (round 1 at n ∈ {3,5,7,9,11}), and that the decided set is a single value. Under 10% loss, 100 seeds × 5 values of n: 0/500 runs split, worst-case rounds 3/6/6/5/6 — bounded and not growing with n. *Falsifier, run: dropping phase-0's `self.proposal = best` (duelling proposers) kills it at the round assertion, `left: 2, right: 1` at n=3 — though 14 other tests also fail on that mutation, since it violates safety too; see §6.10* | tested, power measured |

---

## 4. Anti-properties (§E) — N1–N6

These are the negations of §B, so their evidence is the corresponding positive
property's. Listed separately because §E calls them "the failure modes we
actively hunt for", and hunting is a different activity from asserting.

| Anti-property | Hunted by | Class |
|---|---|---|
| **N1** Divergence / split brain | `consensus/tests/partition.rs`; the `soak` + `conformance` divergence observers, which compare applied logs pairwise across real replicas | tested; observer power measured, protocol power unmeasured |
| **N2** Lost acknowledged write | Not named anywhere. Covered as P9 | see §6 |
| **N3** Stale linearizable read | `smr/tests/{linearizability,restart_recovery}.rs`; `smr/src/linearizability.rs` | tested, power unmeasured |
| **N4** Reordering under linearizability | Not named anywhere. Covered as P6 | see §6 |
| **N5** Phantom decision | Not named anywhere. Covered as P2 / `ValuesFromInputs` | model-checked (via P2) |
| **N6** Timeout-induced livelock | `consensus/tests/hedging.rs` (the P15 tests are its hunt) | tested, power unmeasured |

The observers deserve their own note. `conformance/tests/observer_detects.rs` is
an *anti-vacuity* suite: it proves the divergence and liveness observers **fail
when they should**. That is what upgrades "the soak reported clean" from an
absence of evidence to evidence of absence — and it is why the N1 row can claim
measured power for the observer even where the protocol row cannot.

---

## 5. Desirable (§D)

| Property | Verified by | Class |
|---|---|---|
| **D1** One-round-trip fast path | `consensus/tests/fast_path.rs`; `proposer.rs` unit tests (falsifier above) | tested, power measured (partly) |
| **D2** Linear messaging under synchrony | `consensus/tests/hedging.rs::d2_…` — at n ∈ {3, 5, 7, 11, 21}, no backup activated and message counts pinned as equalities: `2n` leader-only against `2n²` all-active (6/18, 10/50, 14/98, 22/242, 42/882). *Falsifier, run: doubling the leader's fan-out kills the leader-only pin (`left: 12, right: 6` at n=3); doubling non-leaders' kills the baseline pin (`left: 30, right: 18`); the pre-#117 `≤ 4n` bound passes the first of those, and under it this is the only failing test in the workspace's 67 binaries — so that regression previously had no instrument anywhere.* STATUS.md's formerly unsourced figures are now this test's numbers — see §6.7 | tested, power measured |
| **D3** Adversarial robustness | `compare/tests/leader_dos.rs` measures the availability gap under leader isolation, with scheduling-stall attribution (#107), and now names D3 in its module docs (#116). The paper's ≈30× / blog's ≈10× reference points are still **not reproduced here** — this sandbox cannot run etcd — and the file says so rather than letting the mapping imply otherwise | Queso's own gap: tested, power unmeasured. The comparison against etcd: **assumed** from the paper/blog. Two evidence classes, one property |
| **D4** Auto-tuning | `smr/tests/tuning.rs` | tested, power unmeasured |
| **D5** Constant-space recorders | Integer ISR by construction (`consensus/src/concrete.rs`); `IsrConsistent` invariant | model-checked / by construction |
| **D6** Batching & pipelining | Batching: `net/tests/group_commit.rs`. **Pipelining: not implemented** | split — see §6 |
| **D7** Tunable read freshness | Not implemented (a doc mention in `smr/src/linearizability.rs` only) | not implemented |
| **D8** Transactions / CAS | Not implemented | not implemented |
| **D9** Reproducibility | `sim/tests/reproducibility.rs` (the Phase-0 acceptance gate: seed → byte-identical trace); `consensus/tests/{determinism,concrete_determinism}.rs`; enforced by `clippy.toml`'s ban on `Instant::now`, `SystemTime`, threads, `thread_rng`, `HashMap`/`HashSet` | tested + **lint-enforced** |
| **D10** Observability | `net/tests/status.rs` covers the status/metrics endpoint and now names D10 (#116). **The metric list has now been checked, and the intersection is empty**: §D names per-slot rounds, fast-path hit rate, proposer activations, recovery time and per-replica latency (five, not the four §6.3 used to say); `/metrics` serves `events_processed`, `next_slot`, `save_count`, `ready`, `uptime_secs`. None of the five is exposed. The endpoint and its failure modes are well tested; D10's metrics are a different claim | endpoint: tested. §D's metric list: **not implemented** (enumerated, both lists closed) |
| **D11** Reconfiguration | Not implemented (Phase 8 stretch; a doc mention in `smr/src/lib.rs` only) | not implemented |

---

## 6. What building this matrix found

Nine things, all of them labelling or coverage gaps rather than suspected bugs.
Listed because an unlabelled property is one nobody can audit.

1. **P17 now has a test that names it** (#116, was: nothing named it). It
   asserts non-interference rather than termination, which is the distinction
   the old incidental coverage missed — a cluster that converged *despite*
   mutual disruption, slowly, satisfied every assertion the δ-sweep and
   `termination.rs` make. The discriminator is round escalation, so the test
   asserts the round count is flat in n; see the P17 row and §6.10.
2. **N2, N4 and N5 are now named by the tests that hunt them** (#116, was:
   `grep` returned no file for any of them). Each was already covered by its
   positive twin (P9, P6, P2) — a traceability gap, not a hole — but §E frames
   them as things "we actively hunt for" and a reader could not find the hunt.
   Doc comments only; no test changed.
3. **D3 and D10 are now mapped — and checking D10 found a gap** (#116). The
   mapping half was as expected: `leader_dos.rs` is the D3 evidence,
   `status.rs` the D10 evidence, and both now say so. The check half was not:
   §D's five named D10 metrics (per-slot rounds, fast-path hit rate, proposer
   activations, recovery time, per-replica latency — this item previously
   listed only four, omitting the last) share **no** field with what
   `/metrics` actually serves. So "real coverage that is not mapped" was the
   wrong description of D10: the endpoint is well tested, and D10's metric
   list is unimplemented. See the D10 row and §6.11.
4. **Most core safety rests on model-checking plus tests of unmeasured power.**
   `grep -rn "Falsifier[,:]" crates/ --include=*.rs` finds 39 markers in 14
   files (23 in 9 before #112, then #114's re-runs, #113's two new ones, and
   #125's ten).
   They cluster where bugs were actually found: #83 (`proposer.rs` three of five, `restart_agreement.rs`
   24/24, `proposer_start_contract.rs`), #92 (`two_h_proposals.rs`,
   enumerated), the observer/postmortem machinery (`postmortem.rs`,
   `observer.rs`, `soak.rs`, `evidence.rs`, `chain.rs`, `stall.rs`), and
   `durability_faults.rs` as of finding 8. That is the expected shape — power
   gets measured where someone was already suspicious. P5, P6, P7, P10 and
   P11 came off that list with #113 (finding 9); P8, P8a and P14–P16 came
   off it with #125 (finding 13), which is where the remaining "no
   demonstrated ability to detect their own failure" text used to point.
   What survives is narrower and worse: P14 now has a *measured zero*, and
   P8's measured power covers one violation class of the several its
   property admits. P13's measured row still covers the re-kick contract
   only, not majority progress as such.

   The second-order gap in the convention itself is **closed** (#114). Every
   marker in the tree now says `Falsifier, run:`; the 20 that previously described a
   mutation without recording whether anyone ran it were each executed, and
   every one of the 20 predictions held — no marker turned out to be a
   prediction that fails.

   Three of the 20 needed their described mutation reconstructed before it
   reproduced the stated behavior, and each marker now says so. For `soak.rs`'s
   two strict-overlap markers, weakening `coalesced`'s gap test is a no-op on
   those inputs — `desired_of`'s half-open filter already leaves touching
   windows one continuous span, so the pre-fix shape has to be rebuilt as a
   merge over the window list. For its latency-window marker, dropping the
   short-piece skip is not the same edit as counting one change per window.
   In all three cases the first, obvious one-line edit survived; the marker's
   own stated behavior did not.

   That closed the provenance question, not the coverage one. CLAUDE.md's
   cautionary example is 60 sim scenarios with measured-*zero* power for #83
   being read as reassurance for weeks. The cheapest next step was not to
   mutate everything but to pick the two properties whose failure would be
   most catastrophic and least visible — P5 and P10 — and measure those. That
   is what #113 did; finding 9 records what it turned up, and P8, P8a and
   P14–P16 are what remain.

5. **The `>= ½` per-round termination bound (P14) is assumed, not derived here.**
   The tests show termination happens; they do not establish the probability
   bound. That is a legitimate `assumed` — it is the paper's theorem — but it
   should be read as such.
6. **D6 is half-true and reads as whole.** Batching is implemented and tested;
   pipelining is not implemented at all. §D states them as one property.
7. **A number in STATUS.md did not trace to a test — now labelled.** STATUS §2
   reported D2 as "measured `O(n)` vs `O(n²)` messaging under synchrony (10 vs
   50 msgs at n=5; 42 vs 882 at n=21)". `hedging.rs::d2_…` asserts a `≤ 4n`
   bound at n ∈ {3, 5, 7, 11} and runs no 21-replica cluster — nor does any
   other test in `crates/` (searched for `21` as a numeric literal; the one
   hit is a slot count in `observer_detects.rs`). `git log -S882` finds
   the figures entering the tree in `655114c`, the commit that created
   STATUS.md, with no accompanying code — they appear in no test, then or
   since. They are not shown to be wrong; they are **unsourced**, which is the
   failure mode CLAUDE.md §5 is about. The cheap half of the fix landed with
   this matrix: STATUS now states what the test actually asserts and marks the
   figures *asserted, unmeasured*. **The other half has since landed (#117)**:
   `d2_…` now runs n ∈ {3, 5, 7, 11, 21} and pins both counts as equalities,
   `2n` and `2n²`. Re-measured, the old figures were *right* — 10/50 at n=5
   and 42/882 at n=21 reproduce exactly — so what was wrong with them was
   their provenance, not their value, which is the whole point of §5: an
   unsourced number is not a false one, it is an unfalsifiable one. The pins
   were then themselves mutated, and the D2 row is *power measured*.
8. **`durability_faults.rs` claimed measured power it did not have — so it was
   measured.** Building this matrix, the P9/P12 rows were first written as
   "power measured" from the file's own note that a *one-replica* version of the
   torn-snapshot test passed with the boot-time reload disabled. That note
   measures the test's **design** (it is why the committed test crashes a
   majority), not the committed test's power, and the other three tests had no
   mutation at all. Since the failure was a pure function of a one-line edit,
   CLAUDE.md §4 says enumerate rather than argue, so both mutations were run:

   | Test | A: reload disabled | B: persist error swallowed | Assertion that fired |
   |---|---|---|---|
   | 1 torn snapshot | **8/8** | 0/8 | lost an acknowledged write after finding a torn temp file |
   | 2 disk-full fail-stop | 0/8 (control) | **8/8** | the node must exit promptly on a failed durability write |
   | 3 unacknowledged in-flight | **7/16** | 0/8 | an acknowledged write must survive the crash regardless |
   | 4 rolling restart under load | **8/8** | 0/8 | lost acknowledged write to key … across rolling restarts |

   Whole-file runs at `--test-threads=1`, one rebuild per mutation; mutation
   A was run twice, hence test 3's denominator of 16. Test 2 under mutation A
   is the control, so the other rows are not "any mutation reddens the file".
   Test 1's eight kills all landed on the assertion named, never on the
   `ENOENT` that issue #111 describes — which would otherwise have inflated
   the count. #111 is since fixed: test 1 establishes on-disk state in each
   replica before crashing it rather than assuming it, and 20 runs under the
   load that reproduced the `ENOENT` (1 in 20 before the fix) came back
   clean. Mutation A was re-run against the fixed form, 8/8 on the same
   assertion, so this row's test-1 count is measured on the test as it
   stands rather than inherited from a form that has since changed.

   Two things came out that the argued version would have hidden. First,
   **test 3's kills mostly do not come from the assertion it exists for**:
   over 16 runs all 7 were the separate "an earlier acknowledged write
   survived" assertion, which is P9-shaped and merely lives in this test.
   Second, the 7-in-16 rate was **unexplained**.

   Both were followed up in #115, and the first conclusion did not survive:
   at n=156 the never-split assertion *does* fire, 9 times (5.8%). Sixteen
   runs come back empty 39% of the time at that rate, so "no falsifier" was
   a coin flip recorded as a finding. See §6.12 for what the follow-up
   established, including why the rate is low and which assertion is
   genuinely dead. **Nothing in CI re-ran any of this and the counts rotted
   silently — until #128**, which registers the exact edit for nine
   mutations and replays them nightly, with a no-build anchor check on
   every commit. Thirty-four markers remain unregistered and still rot; see
   §6.16 and `falsifiers/README.md`.

9. **Measuring P5, P7, P10 and P11 (#113) turned up one zero-power test and
   one structural blind spot.** Both were invisible from outside; the tests
   are green either way.

   **A vacuous green.** `log_safety.rs::log_safety_holds_even_without_a_live_majority`
   crashed a majority *before* submitting any work. Measured: every replica
   finished at `next_slot=0, log_len=0`, so the assertion compared five empty
   logs. It survived all three mutations that kill the file's other six tests
   — the signature of zero detection power, not of robustness. It now decides
   a prefix first, asserts that prefix is non-empty, and asserts nothing
   further decides once quorum is lost; all three mutations now fail it.

   **Two submit paths, each invisible to the other's tests.**
   `SmrCluster::submit` and `SmrNode::submit` are independent implementations
   of the same contract. The P10 defect (answer a `Get` from local state)
   written into `SmrNode::submit` alone — the path `net/src/driver.rs` uses,
   i.e. the shipped node — leaves **all 60 `queso-smr` tests passing** and
   kills 15 `queso-net` tests. Written into `SmrCluster::submit` alone it
   kills 13 in-process tests and never reaches `queso-net`. Neither suite is
   redundant here; each is the only instrument for its own site. The first
   attempt at this measurement mutated `SmrNode::submit`, ran the sim suite,
   and would have recorded **zero power for P10** — a false finding produced
   by a mutation sitting in code the instrument never executes. Same lesson
   as §6.8 from the other direction: check which assertion fired, and check
   the mutation is on the path under test.

   What was left unmeasured after #113 — P8, P8a and P14–P16 — is measured
   in finding 13 (#125). P13 remains the outstanding one.
10. **P17's test earns a diagnosis, not exclusive coverage — and one attempt
    to show otherwise failed** (#116). The test asserts round counts because
    round escalation is what separates interference from concurrency, and the
    duelling-proposers mutation kills it *at that assertion*. But the same
    mutation fails 14 other tests, because not adopting the best proposal
    violates safety as well. A second mutation was tried specifically to find
    a round-escalation defect that safety tests are blind to (`step += 4`:
    inflate the round, keep the decision correct). It did not isolate one — it
    killed P17's test at the *liveness* assertion instead of the round one,
    killed a safety test too, and slowed the suite enough that a full run did
    not finish. So whether such a defect exists is **unmeasured**, and the
    honest summary is that P17's test reports interference *as* interference
    where the others report it as an agreement violation.
11. **Checking D10's metric list against the endpoint refuted the mapping**
    (#116). §6.3 had recorded D10 as "real coverage that is not mapped",
    implying the naming was the whole job. It was not: `/metrics` serves
    `events_processed`, `next_slot`, `save_count`, `ready`, `uptime_secs`,
    while §D names per-slot rounds, fast-path hit rate, proposer activations,
    recovery time and per-replica latency. The intersection is empty. Both
    lists are closed and short, so this is enumerated, not sampled.

    Worth separating "not exposed" from "not tracked", since they carry
    different work: the ingredients for fast-path hit rate and proposer
    activations exist in `queso_consensus` but are never aggregated or
    published; per-slot rounds is derivable from a proposer's `step` but no
    counter does it; recovery time is tracked nowhere at all; and the latency
    that *is* recorded (`queso_net::metrics::Recorder`) is the bench client's
    view of the cluster, not a node's view of itself. The endpoint is
    genuinely well tested — this finding is about what it serves, not whether
    it works.

12. **A "no falsifier" finding was itself underpowered, and did not survive
    re-measurement** (#115). §6.8 recorded that the never-split assertion in
    `durability_faults.rs` has no falsifier, on the strength of 16 runs in
    which it never fired. Re-run at n=156, mutation A kills there **9 times
    (5.8%, Wilson 95% CI [3.1%, 10.6%])**. At that rate a 16-run check comes
    back empty **39%** of the time — so the original observation was
    unremarkable, and only its promotion to "has no falsifier" was the error.
    The overall kill rate reproduced fine (41/100 vs 7/16); the reading of
    *which* assertion was firing is what needed the sample size. This is
    CLAUDE.md §5 turned on a null result: an absence needs its power stated
    just as much as a rate does.

    **The unexplained 7-in-16 has a partial explanation now**, and it is not
    the one that was guessed. The guess was that surviving replica 0 masks
    the mutation by holding the value in memory. Probing
    `SmrNode::from_durable` on the unmutated build (10 runs × 2 restarts,
    20/20 observations) shows each restarted replica reloads `next_slot=0`,
    `applied_log=0`, an empty `kv`, and 1–2 slots of recorder state. Nothing
    applied. So the state mutation A destroys is nearly nothing — enough to
    explain why destroying it usually changes no answer.

    It stops there deliberately. Mutation A does two things: it discards that
    (nearly empty) reload, and it makes `is_restart` false, skipping the
    restart catch-up pass. Given how little state there is, the skipped
    catch-up is the more plausible source of the kills — but the two were not
    separated, so which dominates is **unmeasured**, and reading these counts
    as "durable-state loss is detected at 5.8%" would overstate them. A
    0/500/2000 ms wait before the reads (n=40 per arm) moved the rate
    45%/38%/45%, no trend: consistent with catch-up being absent rather than
    slow, though only a ~20-point difference would have shown at that n.

    **One assertion in that test is dead, not unmeasured.** Over 60 probed
    runs the pre-crash bookkeeping had `settled == 0` every time, so
    `acknowledged` is always empty and the burst-acknowledged branch never
    executes. That follows mechanically from the test's own 5 ms window
    against a 10–15 ms write, and gets *more* certain on a slower machine.
    Left as-is deliberately: retuning the window would move the scenario the
    5.8% was measured on.

    **Two probes returned a false zero before either result was trusted**,
    the same trap as §6.9's and worth stating in its own right. One printed
    to a spawned `queso-node`'s stderr, which the harness does not capture;
    one failed to compile (a `pub(crate)` field read from another crate) and
    was scored as a test failure by the runner script. Both read as "this
    code is never reached". Checking that a probe can actually report is part
    of the measurement, not preparation for it.

---

## 7. Keeping it honest

This matrix rots the moment a property gains coverage and nobody updates the row.
Two rules make that visible rather than silent:

- **A new test that verifies a property adds its row here**, with its class. If
  the class is "tested, power unmeasured", say so — that is information, not an
  admission. The coverage check is mechanical: the bold `P<n>`/`N<n>`/`D<n>`
  identifiers in `02-properties.md` and in this file must be the same set —

  ```sh
  diff <(grep -oE '\*\*(P[0-9]+[a-z]?|N[0-9]+|D[0-9]+)( |\*\*)' docs/02-properties.md \
          | tr -d '* ' | sort -u) \
       <(grep -oE '\*\*(P[0-9]+[a-z]?|N[0-9]+|D[0-9]+)\*\*' docs/conformance-matrix.md \
          | tr -d '*' | sort -u)
  ```

  which is empty today (35 identifiers each side). Nothing in CI runs it.
- **A mutation run that measures a test's power belongs in the test's own doc
  comment first** (as `Falsifier, run: …` once executed, with the observed
  values and their denominator; `Falsifier (predicted, not run): …` if it is
  only a prediction — never a bare `Falsifier:`, which hides which of the two
  it is. The convention is already used in
  `soak/`, `proposer.rs`, `proposer_start_contract.rs`, `restart_agreement.rs`
  and `compare/src/stall.rs`), and in this matrix second. The doc comment is
  what survives a file move; this table is the index.

13. **Measuring the last five (#125) found one zero, one blind artifact, one
    test with no power at all, and one uncovered boundary.** P8, P8a and
    P14–P16 were what #113 left. All five are now measured, and only two of
    the five came back the way the matrix implied they would.

    **P14 has a measured zero, and a structural one.** Replacing the drawn
    priority with a constant — at *both* randomization sites, separately —
    kills **0 of 441** tests. Two reasons, and neither is an oversight:
    `mean < 4.0` is a one-sided bound and losing randomization moves the
    mean *down* (with equal priorities `Proposal::Ord` still totally orders
    by `origin`, and converging on "highest origin" is measurably faster in
    the concrete driver: n=5 mean 1.607 → 1.407); and both tests run
    `ContentObliviousAdversary`, the class A3 names, which by construction
    cannot exploit a deterministic tie-break. Randomization earns its keep
    against a content-aware adversary, and no termination test uses that
    class though `crates/sim` provides it. Whether such an adversary can
    livelock the constant-priority variant is **unmeasured** — a real open
    question, tracked separately rather than guessed at.

    This one nearly recorded a false zero the same way #113's P10 nearly
    did. The first attempt mutated `proposer.rs`, ran `termination.rs`, and
    saw a byte-identical histogram — because that file drives the
    *abstract* `Cluster`, whose priorities come from `node.rs`. Each site
    is now shown on-path by a moved distribution rather than by inspection.
    Same lesson, third occurrence: check the mutation is on the path the
    instrument runs, and check it by observing behavior change, not by
    reading the call graph.

    **P15's "crux" test could not detect the thing it named.**
    `p15_huge_delta_eventually_converges_and_never_permanently_stalls`
    described itself as P15's crux; under a hedge gate that defers forever
    behind a frozen recorder step, its activation *and* decision state is
    byte-identical to the unmutated build — all five replicas activated and
    decided, both ways — because its leader stays alive and delivers the
    decision regardless of whether hedging ever fires. The claim is
    withdrawn in the doc comment and
    `p15_huge_delta_still_decides_when_the_leader_never_delivers` (huge δ
    *and* a dead leader) now carries it, failing that mutation 8/8. Of the
    five tests named for P15, two can detect the stall defect and three
    cannot.

    **P8a's exact-duplicate test had zero power, for a reason worth
    remembering.** Its "duplicate" went to a replica whose `next_slot` was
    still 0, so the retry proposed slot 0, found it already decided
    carrying a byte-identical command, and completed — never becoming a
    second log entry, so `Kv::apply` was never asked to apply it twice.
    Under dedup-off its observable behavior was byte-identical to the
    unmutated build. It went red anyway, on the linearizability assertion,
    because `Kv` is *also* the checker's reference spec: the mutation broke
    the spec, and the spec then disagreed with a system that had behaved
    correctly. **A count taken from "the file went red" would have recorded
    power this test did not have** — §6.9's failure mode reached from a new
    direction, and an argument for reading which assertion fired *and*
    whether behavior changed, not just the exit code. Repaired with a
    warm-up read plus a slot guard so the power cannot be lost silently
    again. The `>= ` → `>` off-by-one — a client retrying its own latest
    command, the most ordinary retry there is — was caught by exactly one
    test in the tree and by nothing at integration level; a fourth test now
    covers it, 8/8.

    **P8's artifact is blind to two of the three violation classes it
    admits.** `smr/tests/linearizability.rs`'s demonstrated power is
    entirely the stale-read class (P10-A, 4/5, re-verified). Catch-up
    staleness and dedup-off both produce genuine linearizability violations
    and kill **0/5** there — caught by restart/durability tests and by
    `idempotency.rs` instead. Structural again: the randomized workload
    never crashes a replica and never reissues a `(client, seq)`, so
    neither class is reachable in it. P8's coverage is the union of those
    suites, not this file; the matrix row now says so. The file's positive
    control does have measured teeth — a vacuous checker fails it 8/8.

    **The pattern across all four.** In three of the five, the artifact the
    matrix named was not the artifact carrying the power, and in two of
    those the doc comment actively claimed otherwise. Mapping a property to
    a test whose *name* matches it is not evidence about that test; only
    mutation is. Reading the name is how P14's zero, P15's crux and P8a's
    vacuous scenario survived this long.

14. **P14's conjectured falsifier was built, and points the other way**
    (#150). §6.13 closed by conjecturing that randomization "earns its keep
    against an adversary that *can* see priorities and schedule on them",
    and left open whether such an adversary livelocks the constant-priority
    variant. It was built. It does separate the two builds — in the
    direction that makes the **randomized** build the slower one.

    Four adversaries, each run against both builds, n ∈ {3,5,7}, 100 seeds
    per cell, two delay magnitudes (which changed nothing):

    | Adversary | Sees | Real build | Constant-priority (C1) |
    |---|---|---|---|
    | hold the top-origin node back | metadata | 1.000 rounds | 1.000 |
    | …back toward half the cluster | metadata | 1.35–1.41 | **1.000** |
    | hold a different node back per message | metadata | 1.21–1.33 | **1.000** |
    | hold the **highest-priority request** back toward half | payload | 1.29–1.43, worst round 4 | **1.000, worst round 1** |

    Not one arm made the mutation slower, which is what a falsifier
    requires. The mechanism is the same one §6.13 identified, seen from the
    other side: with equal priorities `Proposal::Ord` still yields a unique
    maximum in every set, so `best` stays a deterministic function of what a
    recorder received and convergence needs no unpredictability.
    Randomization is what lets recorders *disagree* about the maximum, and
    that disagreement is what costs the extra rounds. It buys safety against
    an adversary that adapts to the draw — which is exactly what reading
    priorities off the wire is, and exactly what A3's private channels
    exclude.

    **One arm looked like a falsifier and was not.** A two-sided *drop*
    split left C1 undecided in 200/200 seeds — and the unmutated build
    undecided in 200/200 too. It is a partition that violates P13's
    "a majority can communicate" precondition, not an exploitation of
    determinism. It is recorded in the test file because it is the shape of
    thing that gets written up as a result by someone who does not run the
    control, and #125's method notes exist to force exactly that step.

    **What this does not establish.** That no adversary separates them. The
    strategy space is unbounded; this was four strategies, two delay
    magnitudes, three cluster sizes, one leaderless slot, no crashes, no
    hedging. Per §6.12's lesson about null results, the honest statement is
    "no falsifier found within these bounds", never "no falsifier exists".
    P14's row stays a measured zero.

    Two tests came out of it, pinning the real build's termination under
    targeted adversaries — coverage nothing else in the tree has, since
    `fast_path.rs`'s aware adversary targets D1's fast path rather than
    termination. Their own power against C1 is measured, and it is zero;
    each says so.

15. **P13 is measured, and the trap it was flagged for did not fire**
    (#152). P13 was the last property whose row read *tested, power
    unmeasured*. The instrument was a quorum rule inflated from a majority
    to the whole membership — safety-preserving by construction, since a
    larger quorum still intersects, and liveness-destroying the moment any
    replica is unreachable.

    | Site | Serves | Kills in `partition.rs` |
    |---|---|---|
    | `proposer.rs::quorum_threshold` | `ConcreteCluster` | **2 of 8**, 8/8 runs |
    | `tcast.rs`'s `majority_threshold` | abstract `Cluster` | **0 of 8**, 8/8 runs |

    **The risk #152 named did not materialise.** `run_majority_minority_
    then_heal` asserts progress *and* safety in one body — four assertion
    classes in one test — so a liveness mutation reddening it at a safety
    assertion would have been a miscount of exactly the kind §6.13 records
    twice. Both kills land on the "majority replica never decided under
    partition" panic: P13's own claim, while the cut is still in place.
    Across the two sim crates the mutation kills 16 tests and all but three
    die on liveness assertions.

    Three of the 16 are **not** P13 detections and are recorded so nobody
    counts them: `two_h_proposals.rs`'s two enumeration tests build explicit
    size-2 quorums at n=3 that the mutation stops being quorums at all
    (invalid fixtures, not a failing property), and `tuning.rs`'s
    leader-targeting test dies at its own anti-vacuity guard.

    **The abstract site is a clean zero, and not from sloppiness.**
    `partition.rs`'s first three sections drive `ConcreteCluster` and never
    reach `tcast`; its fourth is two `#[should_panic(expected = "tcast
    failed to converge")]` tests, and a quorum that can never be gathered
    produces *that same panic*. The `expected =` filter is present and
    correct — the two worlds are simply observationally identical there.
    The defect is caught by 7 other tests in the crate. This is the
    two-site lesson landing the other way up from §6.13's P14: there the
    artifact was blind to the site it did not execute; here it is blind to
    a site it does execute, because the observable does not distinguish.

    **One assertion in the file has zero power and now says so.**
    `run_varied_partition_timing`'s `all_live_decided` check — its own
    comment already called it "a progress bonus check, not the core safety
    property" — survives 0/8. It heals before checking, and a healed
    cluster can gather a quorum of *n*. Only an assertion made while the cut
    is in place can see this defect.

    **What the measurement changed about the row.** P13's coverage is far
    wider than the matrix implied: 16 sim tests plus the real-node
    `cluster_survives_at_its_fault_tolerance_boundary`. The row listed
    `partition.rs` and the re-kick contract. It now lists what the mutation
    actually found, which is the useful version — mapping a property to the
    test whose *name* matches it was the recurring error of §6.13 and
    §6.14, and this is the same correction applied to the last row.

    With this, **no property in §§1–3 is left at *power unmeasured*.**

16. **The recorded counts are now asserted rather than pasted, for nine of
    43 markers** (#128). §6.8 closed on "the counts rot silently". It is
    the same defect the TLA+ state counts had before #78 — a number sitting
    in prose that nothing re-derives — and it gets the same fix:
    `falsifiers/registry.toml` carries the exact edit for each registered
    mutation, and `falsifiers/replay.py` applies it and checks the recorded
    outcome still holds.

    `ci.yml` runs `--check-anchors` on every commit: no build,
    milliseconds, and it asserts each mutation's `find` string still occurs
    exactly once in its file. That catches the commonest rot — the code
    moving out from under a mutation nobody re-ran, which is precisely the
    condition §6.4 records for 3 of the 20 markers #114 re-ran.

    **The cost split this was originally built around did not survive its
    own first run.** The replay was put on a schedule rather than the
    commit gate because "one rebuild per mutation" was estimated at ~10
    minutes. Measured on `falsifiers-nightly.yml` run 1 (2026-09-12): the
    replay step took **12 seconds** for all nine mutations — cheaper than
    the abstract TLC check that already gates every commit. Not because it
    skipped anything: the run observed every recorded kill, and a replay
    that did nothing would report `LOST POWER` and exit 1, since most
    entries assert kills only a real build can produce. So the replay now
    runs per commit as well, and the nightly is the copy that keeps working
    as the registry grows (nine entries is not forty-three; when the
    per-commit cost stops being negligible, the per-commit step goes and
    the schedule stays). The estimate is corrected wherever it was written
    down rather than left to be read as measured — §5's rule applied to a
    number this matrix itself produced.

    **Both directions are asserted.** Each entry names the tests that must
    still fail *and* controls that must still pass. Without controls a
    mutation that reddens everything reads as a precise instrument, which
    is §6.8's mistake in miniature. Four of the nine entries are recorded
    **zeros** (P8's catch-up staleness, P14's two randomization sites,
    P13's abstract site): there the claim is that nothing named detects it,
    and a zero that silently becomes a kill means coverage was added and
    these docs are now wrong.

    **The checker was itself measured**, since shipping an unmeasured
    checker here would be the wrong kind of irony. Three negative controls:
    a `find` pointed at absent text is reported as drift (exit 1); a test
    added to `expect_fail` that does not fail is reported as `LOST POWER`;
    a test added to `expect_pass` that does fail is reported as `CONTROL
    BROKE`. All three fire, with the exit codes CI needs.

    **What is not covered, and why it is not an oversight.** 34 of the 43
    markers are unregistered — their edit has not been transcribed, and
    writing one down from prose *without re-running it* would recreate the
    exact problem this closes. Separately, the genuinely stochastic markers
    (`9/156`, `41/100`, `7/16`, `3/7`, `5/100`) are excluded on purpose:
    most recorded counts are deterministic and a single run gates them
    honestly, but gating a 5.8% rate on one run produces a flaky job that
    teaches everyone to ignore it. §6.12 warns against reading a null
    result without its power; the same caution applies to gating on one.
    Those need a per-entry sample size and tolerance.
