# CLAUDE.md

Queso is a formally-checked implementation of the QuePaxa consensus
algorithm. Start with `README.md`; the property model is
`docs/02-properties.md`, the testing strategy `docs/03-testing-plan.md`,
and the current gap analysis `docs/STATUS.md`. Before investigating any
failure, read `docs/investigating-with-logs.md` and
`docs/what-each-test-establishes.md` — they exist because investigations
here have gone wrong in specific, recorded ways.

## Claims discipline

This project's worst bugs were not in the code. They were in **sentences**:
confident claims that outran their evidence, written by an agent, read by
the next agent as established fact, and built upon. Issue #83's closure
declared a guard "harmless in any build" from an argument sketch; the
enumeration that refuted it took one session and ~300 lines (#92,
`crates/consensus/tests/two_h_proposals.rs`). A failure rate asserted from
one bad night ("two seeds in eight") was repeated until it shaped an
underpowered experiment. These rules are the countermeasures, and they are
checkable, not aspirational. Follow them in issues, PR bodies, commit
messages, doc comments, and reviews alike.

### 1. Every correctness claim carries its evidence class

State *how you know*, inline, every time:

- **enumerated** — exhaustive over a named, bounded space ("every state ×
  every quorum, n ∈ {3,5,7}")
- **tested, power measured** — a test exists *and* its detection power was
  demonstrated by mutation ("reverting `begin_catch_up` fails it 24/24")
- **model-checked** — a TLC run over a named config, with state counts
- **tested, power unmeasured** — a test exists; nobody has shown it can
  detect the failure it guards against. Say so. (60 sim scenarios had
  measured-*zero* power for #83 and were read as reassurance for weeks.)
- **argued** — a reasoning sketch. Legitimate, but it must be labeled, and
  it must never sit next to measured claims at equal confidence.
- **assumed** — carried from the paper or from another claim's premises.

The #83 closure mixed these: "#90 removes the source" (tested, measured)
beside "#88 makes one harmless" (argued) — and the argued one was false.

### 2. No bare universal quantifiers

"Never", "always", "in any build", "whatever a future caller does" — a
universally quantified sentence must be immediately followed by the
**mechanism of its exhaustiveness** (what space was enumerated or
model-checked, and its bounds), or be rewritten as a conjecture: "we
believe X; unverified". A universal backed by a narrower check than the
claim's own scope is the exact defect that closed #83 wrongly: the
enumeration covered fast-vs-fast decisions; the sentence claimed the whole
system.

### 3. Expand slogans against the definitions before leaning on them

A slogan ("nothing can ever beat `H`", "majority intersection saves us")
inherits premises from wherever it was proved, and this codebase may not
satisfy them. Before an argument rests on one: restate it in terms of the
actual definitions in the code, and list the premises it needs. "Nothing
beats `H`" expands against `Proposal::Ord` (priority, then origin, then
value) into "…provided at most one distinct proposal carries `H`" in three
lines — which is the premise #83 had just violated. The premise inventory
is the deliverable: a claim that is safe *provided P* must say *provided
P*, because "provided P" is never defence in depth against P failing.

### 4. Enumerate before claiming, not only before debugging

`docs/investigating-with-logs.md`'s rule 0 (name the function, bound its
state, enumerate it, don't sample around it) applies symmetrically to
**verification**: before writing "this fix suffices" or "X cannot
happen", ask whether X's occurrence is a pure function of bounded state.
If it is, enumerate it *now* — minutes of work — and cite the enumeration
instead of the argument. The #92 enumeration would have cost one session
at closure time; the false claim it refuted stood on `main` instead.

### 5. Numbers are measured or labeled as asserted

A rate, a count, or a probability appears with its measurement ("4 in 32,
CI ≈ [3.5%, 29%]") or with the label *asserted, unmeasured*. Before
designing any stochastic experiment, write down the per-trial rate you
believe and the trial count it implies; if the arms are underpowered at
your own believed rate, the experiment cannot conclude anything and should
not run. (Both #83 leader-experiment runs came back empty *by design*:
8-seed arms at the measured rate detect nothing 34% of the time.)

### 6. Contrary evidence blocks closure

Before closing an issue, declaring a fix verified, or writing a summary
that says "resolved": sweep recent CI runs and open anomalies for anything
red **against the claim**, and either explain it in the closure or do not
close. #83 was closed 28 minutes after a CI run finished with a divergence
on a build the closure declared safe; nothing referenced it, and its
evidence expired unread. An unexplained contrary observation outranks any
argument, however clean the argument reads.

### 7. Review the prose as adversarially as the diff

Fresh-environment reviews here try to *break the code*, and that has
caught real bugs. Extend the same posture to the words: for every
universal sentence in a PR body, issue comment, or doc comment, attempt a
counterexample before approving. The false sentence in #83's closure was
in prose; the code beneath it was fine. A reviewer who spends ten minutes
trying to falsify "harmless in any build" finds the uniform-lesser-quorum
counterexample — it was reachable from the definitions alone.

### Before writing a conclusion

Run the falsification pass **first**, then write:

1. What would falsify this claim? If nothing observable could, it is a
   hope — label it as one.
2. Is the claim's failure a function of bounded state? Enumerate it.
3. What premises does the claim rest on? Say "provided …" out loud.
4. Is there a red run, an open anomaly, or an expired-but-recorded piece
   of evidence that contradicts it? Address it or don't conclude.
5. Does the sentence's quantifier match the evidence's scope? Shrink one
   of them until they match.
