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
at that location in-tree. Those falsifiers were recorded by the change that
introduced them; this matrix reports them, it does not re-run them. Re-running
the whole set would be the natural next step if any of these rows is ever load-bearing
for a release decision.

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
| **P5** Log matching / prefix consistency | `smr/tests/log_safety.rs`; `conformance/tests/faults.rs`; `soak/` real-process observers | tested, power unmeasured |
| **P6** Total order | `smr/tests/log_safety.rs`; `conformance/` observer; `chain/` hash chain | tested, power unmeasured |
| **P7** Gap-free application | `smr/tests/log_safety.rs`; `chain/src/lib.rs` | tested, power unmeasured |
| **P8** Linearizability | `smr/tests/linearizability.rs` (randomized concurrent put/get, checked offline) | tested, power unmeasured |
| **P8a** Idempotent commands | `smr/tests/idempotency.rs` | tested, power unmeasured |
| **P9** No lost committed writes | `smr/tests/restart_recovery.rs`; `net/tests/restart_recovery.rs` (#36, majority reboot of real OS processes) | tested, power unmeasured |
| | `net/tests/durability_faults.rs::acknowledged_writes_survive_rolling_restarts_under_load` — *falsifier: disabling the boot-time reload in `driver.rs` fails it 8/8* | tested, power measured |
| | Write-before-reply is asserted at runtime in `driver.rs` by a **release-mode** assert, so the check is in the shipped artifact rather than only under `cargo test`. No falsifier has been run for the assert itself | tested, power unmeasured; enforcement **in-build** |
| **P10** Read safety under lag | `smr/tests/{linearizability,idempotency,restart_recovery}.rs` | tested, power unmeasured |
| **P11** Safety under > f crashes | `consensus/tests/partition.rs`; `smr/tests/log_safety.rs` | tested, power unmeasured |
| **P12** Restart safety | `net/tests/durability_faults.rs` (#39) — four real-process fault tests. *Falsifiers, run (see §6.8 and the file's "Detection power" docs): disabling the boot-time reload fails the torn-snapshot test 8/8, the rolling-restart test 8/8 and the in-flight-write test 3/8, and leaves the disk-full test passing 8/8; swallowing the persist error fails the disk-full test 4/4* | tested, power measured (3 of 4; the in-flight test partially — see §6.8) |
| | `net/tests/persist_fidelity.rs` (#83), `group_commit.rs`; `smr/tests/{restart_recovery,restart_agreement}.rs` | tested, power unmeasured (except `restart_agreement.rs`, above) |

---

## 3. Liveness (§C) — P13–P17

| Property | Verified by | Class |
|---|---|---|
| **P13** Majority progress | `consensus/tests/partition.rs` | tested, power unmeasured |
| | `consensus/tests/proposer_start_contract.rs` (#13) — re-kicking an *undecided* proposer restarts round 1, so a driver can un-stall one that spent its whole first push partitioned from every quorum — *falsifier: making `start` fully idempotent leaves the replicas parked at their pre-kick step and the rewind assertion fails* | tested, power measured |
| **P14** Randomized termination | `consensus/tests/termination.rs`, `concrete_termination.rs` — content-oblivious adversary, per A3 | tested, power unmeasured |
| | The ≥ ½ per-round bound itself | **assumed** (paper, §4; not independently derived here) |
| **P15** Timeout-independent liveness | `consensus/tests/hedging.rs` — four tests named for it: δ-sweep, huge δ, per-proposer misconfiguration, no-live-majority | tested, power unmeasured |
| **P16** Leader-failure recovery | `consensus/tests/hedging.rs::p16_…`; `compare/tests/leader_dos.rs` (real cluster, leader isolated) | tested, power unmeasured |
| **P17** No destructive interference | **Nothing names it** — `grep -rlE "\bP17\b" crates/` returns no file. Exercised incidentally where several proposers are active at once (the P15 δ-sweep, `termination.rs`), but no test found by that search asserts non-interference directly | see §6 |

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
| **D2** Linear messaging under synchrony | `consensus/tests/hedging.rs::d2_…` — asserts leader-only cost `≤ 4n` and that no backup activated, at n ∈ {3, 5, 7, 11}. The concrete message counts STATUS.md used to quote are **not** this test's numbers; see §6.7 | tested, power unmeasured |
| **D3** Adversarial robustness | `compare/tests/leader_dos.rs` measures the availability gap under leader isolation, now with scheduling-stall attribution (#107). **Nothing maps it to D3 by name** (`grep -rlE "\bD3\b" crates/` is empty), and the paper's ≈30× / blog's ≈10× reference points are not reproduced here | tested, power unmeasured; the comparison against etcd is **assumed** from the paper |
| **D4** Auto-tuning | `smr/tests/tuning.rs` | tested, power unmeasured |
| **D5** Constant-space recorders | Integer ISR by construction (`consensus/src/concrete.rs`); `IsrConsistent` invariant | model-checked / by construction |
| **D6** Batching & pipelining | Batching: `net/tests/group_commit.rs`. **Pipelining: not implemented** | split — see §6 |
| **D7** Tunable read freshness | Not implemented (a doc mention in `smr/src/linearizability.rs` only) | not implemented |
| **D8** Transactions / CAS | Not implemented | not implemented |
| **D9** Reproducibility | `sim/tests/reproducibility.rs` (the Phase-0 acceptance gate: seed → byte-identical trace); `consensus/tests/{determinism,concrete_determinism}.rs`; enforced by `clippy.toml`'s ban on `Instant::now`, `SystemTime`, threads, `thread_rng`, `HashMap`/`HashSet` | tested + **lint-enforced** |
| **D10** Observability | `net/tests/status.rs` covers the status/metrics endpoint. **Nothing maps it to D10 by name** (`grep -rlE "\bD10\b" crates/` is empty), and §D's specific list (per-slot rounds, fast-path hit rate, proposer activations, recovery time) is not checked against the endpoint's actual fields | tested, power unmeasured; coverage of the named metrics **unverified** |
| **D11** Reconfiguration | Not implemented (Phase 8 stretch; a doc mention in `smr/src/lib.rs` only) | not implemented |

---

## 6. What building this matrix found

Eight things, all of them labelling or coverage gaps rather than suspected bugs.
Listed because an unlabelled property is one nobody can audit.

1. **P17 has no test that names it.** Multiple simultaneously-active proposers
   converging is exercised incidentally by the P15 δ-sweep and by `termination.rs`,
   but nothing asserts non-interference as such. Cheapest fix: assert it inside
   the existing δ-sweep, where several proposers are already live.
2. **N2, N4 and N5 appear nowhere in the tree** — `grep -rlE "\bN2\b"` and the same for `N4` and `N5`,
   over `crates/`, each return no file. Each is covered by its positive
   twin (P9, P6, P2), so this is a traceability gap, not a hole — but §E frames
   them as things "we actively hunt for", and a reader cannot currently find the
   hunt.
3. **D3 and D10 have real coverage that is not mapped to them.** `leader_dos.rs`
   *is* the D3 evidence and `status.rs` *is* the D10 evidence; neither says so.
   For D10 specifically, nobody has checked the endpoint against §D's named
   metrics list.
4. **Most core safety rests on model-checking plus tests of unmeasured power.**
   Every `Falsifier:` comment in `crates/` (27 of them, `grep -rni falsifier
   crates/ --include=*.rs`) sits in one of eight files, and they cluster where
   bugs were actually found: #83 (`proposer.rs` three of five,
   `restart_agreement.rs` 24/24, `proposer_start_contract.rs`), #92
   (`two_h_proposals.rs`, enumerated), and the observer/postmortem machinery
   (`postmortem.rs`, `observer.rs`, `soak.rs`, `evidence.rs`), plus
   `durability_faults.rs` as of finding 8. That is the
   expected shape — power gets measured where someone was already suspicious —
   but it means P5–P8, P8a, P10, P11 and P14–P16 currently have **no
   demonstrated ability to detect their own failure** (P13's measured row
   covers the re-kick contract only, not majority progress as such). CLAUDE.md's own cautionary example is 60 sim
   scenarios with measured-*zero* power for #83 being read as reassurance for
   weeks. The cheapest next step is not to mutate everything; it is to pick the
   two or three properties whose failure would be most catastrophic and least
   visible (P5 and P10 are the candidates) and measure those.
5. **The `>= ½` per-round termination bound (P14) is assumed, not derived here.**
   The tests show termination happens; they do not establish the probability
   bound. That is a legitimate `assumed` — it is the paper's theorem — but it
   should be read as such.
6. **D6 is half-true and reads as whole.** Batching is implemented and tested;
   pipelining is not implemented at all. §D states them as one property.
7. **A number in STATUS.md did not trace to a test — now labelled.** STATUS §2
   reported D2 as "measured `O(n)` vs `O(n²)` messaging under synchrony (10 vs
   50 msgs at n=5; 42 vs 882 at n=21)". `hedging.rs::d2_…` asserts a `≤ 4n`
   bound at n ∈ {3, 5, 7, 11} and never runs n=21, and `git log -S882` finds
   the figures entering the tree in `655114c`, the commit that created
   STATUS.md, with no accompanying code — they appear in no test, then or
   since. They are not shown to be wrong; they are **unsourced**, which is the
   failure mode CLAUDE.md §5 is about. The cheap half of the fix landed with
   this matrix: STATUS now states what the test actually asserts and marks the
   figures *asserted, unmeasured*. The other half — re-measuring them into
   `d2_…` as pinned counts at n=5 and n=21 — is open, and would upgrade the D2
   row.
8. **`durability_faults.rs` claimed measured power it did not have — so it was
   measured.** Building this matrix, the P9/P12 rows were first written as
   "power measured" from the file's own note that a *one-replica* version of the
   torn-snapshot test passed with the boot-time reload disabled. That note
   measures the test's **design** (it is why the committed test crashes a
   majority), not the committed test's power, and the other three tests had no
   mutation at all. Since the failure was a pure function of a one-line edit,
   CLAUDE.md §4 says enumerate rather than argue, so both mutations were run:

   | Test | Reload disabled | Persist error swallowed |
   |---|---|---|
   | 1 torn snapshot | **fails 8/8** | passes |
   | 2 disk-full fail-stop | passes 8/8 (control) | **fails 4/4** |
   | 3 unacknowledged in-flight | fails **3/8** | passes |
   | 4 rolling restart under load | **fails 8/8** | passes |

   The rows are now measured rather than inherited, the numbers live in the
   file's own doc comments, and one thing came out that the argued version
   would have hidden: test 3 detects a reload bug only 3 times in 8, because
   its assertion is "lost or kept, but never split" and the lost case is
   legitimate. Its reliable job — never-split — still has **no** falsifier.
   Nothing in CI re-runs these; they rot silently.

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
  comment first** (as `Falsifier: …`, the convention already used in
  `soak/`, `proposer.rs`, `proposer_start_contract.rs`, `restart_agreement.rs`
  and `compare/src/stall.rs`), and in this matrix second. The doc comment is
  what survives a file move; this table is the index.
