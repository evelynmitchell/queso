---
name: claims-check
description: Audit prose for claims that outrun their evidence — universal statements backed by narrower checks, asserted numbers dressed as measurements, slogans quoted without their premises, tests with unmeasured power cited as reassurance, and conclusions written over unexamined contrary evidence. Use this whenever the user asks to check, review, audit, or tighten the claims, confidence, calibration, or overconfidence in any text; before closing an issue, writing a post-mortem, or declaring a fix verified; when reviewing a PR whose description or comments make correctness, safety, or performance claims; and on your own conclusions before stating them. Trigger even if the user only says something like "is this claim solid?", "sanity-check this writeup", or "am I overclaiming here?".
---

# Claims check

Audit a piece of prose for **claims whose stated confidence exceeds their
evidence**, and report each one with a concrete, compliant rewrite.

## Why this exists

The worst defects in engineering projects are often sentences, not code. A
real incident, condensed: a consensus project closed a safety issue on the
claim that a guard made a dangerous state "harmless in any build". The claim
was *argued* from a sketch; the test cited beside it covered a narrower case
than the sentence's own scope. A 300-line enumeration refuted it the next
day — and in the meantime a CI run had already contradicted it, unexamined,
its evidence expired. Separately, a failure rate asserted from one bad night
("roughly two in eight") was repeated until it read as a measurement and
shaped an experiment that was underpowered by its own believed rate.

Confident prose gets read by the next person — or the next agent — as
established fact and built upon. This skill exists to catch that *before*
the sentence ships, by holding every claim to one question: **how is this
known, and does the sentence's scope match that evidence's scope?**

## What to audit

Resolve the target in this order:

1. Whatever the user pointed at: a file, a PR number, an issue, a commit
   range, or pasted text.
2. If invoked while a PR is in play: the PR title/description, its linked
   issue's recent comments, and prose inside the diff (doc comments, README
   or docs changes, commit messages).
3. Otherwise: the prose portions of the staged or most recent changes.

The unit of analysis is the individual claim — usually a sentence, sometimes
a table row or a heading. Work through the target claim by claim; do not
skim for vibes. Count the claims you examined; the count anchors your
verdict.

Only **factual claims** are in scope: statements about what is true, safe,
fixed, impossible, measured, or verified. Opinions, plans, questions, and
clearly-labeled speculation are out of scope — auditing those produces noise
that teaches people to ignore the check.

## The evidence-class taxonomy

Every correctness/safety/performance claim is knowable in one of these ways.
The audit's core move is assigning each claim its *actual* class and
comparing it against the confidence the sentence projects.

| class | meaning | claim strength it supports |
|---|---|---|
| **enumerated / proved** | exhaustive over a named, bounded space, or machine-checked | universal, within the named bounds |
| **tested, power measured** | a test exists *and* it was shown (e.g. by mutation) to fail when the claim is false | strong, within what the test exercises |
| **tested, power unmeasured** | a test exists; nobody has shown it can detect the failure it guards | weak — "consistent with", never "rules out" |
| **measured** | a number from a stated procedure, with its n | that number, with its uncertainty |
| **argued** | a reasoning sketch | conditional — only as strong as its stated premises |
| **assumed / inherited** | carried from a paper, a doc, or another claim's premises | none on its own; must be labeled |

## The violation catalog

Check every claim against each of these. The IDs are for the report.

### C1 — Unlabeled evidence class

A correctness claim stated flatly, with no way for the reader to tell
whether it is enumerated, tested, argued, or assumed. Worst when an argued
claim sits beside a measured one at equal confidence — the reader inherits
the stronger neighbor's credibility for both.

- Bad: "The retry path cannot lose acknowledged writes, and the new guard
  makes double-delivery impossible."
- Good: "The retry path cannot lose acknowledged writes
  (`restart_recovery` test, kill-verified). We *believe* the new guard
  prevents double-delivery — argued from the state machine, not yet
  tested."

### C2 — Universal quantifier wider than its evidence

"Never", "always", "in any build", "under all inputs", "whatever a caller
does" — with either no exhaustiveness mechanism named, or a cited check
narrower than the sentence's own scope. This is the single most damaging
pattern: the incident above was exactly a system-wide "any build" claim
backed by an enumeration of one function.

- Bad: "The sanitizer makes injection impossible."
- Good: "The sanitizer rejects every string in the OWASP injection corpus
  (enumerated, 942 cases). Inputs outside that corpus are not covered."
- Also good: "We believe injection is now impossible; unverified beyond
  the corpus." (A universal *labeled as a conjecture* is honest.)

### C3 — Slogan without its premises

An argument leaning on a borrowed phrase ("the lock makes this atomic",
"majority intersection saves us", "TLS means the adversary can't read it")
without expanding it against the actual definitions in this codebase and
listing the premises it needs. Slogans inherit premises from wherever they
were proved; the code at hand may not satisfy them. A claim that is safe
*provided P* must say "provided P" — and "provided P" is never a defense
against P failing.

- Bad: "Nothing can beat the reserved max priority, so the fast path is
  safe."
- Good: "Nothing can beat the reserved max priority *provided at most one
  proposal carries it* (its unbeatability is a property of the ordering
  only under uniqueness — see `Ord`'s tie-break). That premise is enforced
  at the two call sites and pinned by <test>."

### C4 — Sampling where enumeration was available

A claim supported by "we ran it N times / soaked it overnight and saw
nothing", when the claim's failure is a pure function of small, bounded
state that could be enumerated outright. Sampling is for spaces you cannot
bound; citing it for a bounded space converts minutes of certainty into
nights of anecdote. (Do **not** flag sampling over genuinely unbounded or
interleaving-dependent spaces — there, sampling is correct, and demanding
enumeration is the false positive.)

- Bad: "500 random inputs produced no mismatch, so the two parsers agree."
- Good (when bounded): "Enumerated all 4,096 header combinations: the two
  parsers agree on every one."

### C5 — Asserted number dressed as a measurement

A rate, count, or probability with no measurement behind it — often a
single worst observation repeated until it reads as a fact — or an
experiment designed around an unmeasured rate. Before any stochastic
experiment: state the believed per-trial rate and the trial count it
implies; if the design is underpowered at the author's own believed rate, a
clean result was always likely and concludes nothing.

- Bad: "This flakes about 20% of the time." (source: one bad afternoon)
- Good: "Failed 4 of 32 runs (~12.5%, CI ≈ [4%, 29%]); at that rate an
  8-run re-check comes back clean 34% of the time, so we ran 24."

### C6 — Conclusion over unexamined contrary evidence

A closure, "fixed", "verified", or "resolved" written without sweeping for
evidence *against* the claim: recent CI runs, open anomalies, expired-but-
recorded failures, an unexplained red anywhere near the claim's scope. When
you have access to the project's CI or issue tracker, actually perform this
sweep for any closure-type claim in the target; when you do not, say so in
the report rather than silently skipping — an unexplained contrary
observation outranks any argument.

### C7 — Vacuous green cited as evidence of absence

A passing test or clean run cited as ruling something out, when the
instrument's power to detect that thing is unmeasured — or measured at
zero. Sixty scenarios that never once reproduced a bug are not evidence the
bug is gone; they are a measurement that the instrument cannot see it.
Related: "no divergence" from a run that compared almost nothing.

- Bad: "The restart tests all pass, so the race is fixed."
- Good: "The restart tests pass — but note they never reproduced this race
  pre-fix, so their green carries no weight here. The targeted regression
  test (fails 24/24 on the reverted fix) is the evidence."

## Calibration — what not to flag

False positives are this skill's failure mode: a report that nags about
every strong sentence teaches people to ignore it. Do not flag:

- A universal whose exhaustiveness mechanism is named and scope-matched.
- A claim already labeled with an honest class ("argued", "we believe",
  "unverified", "assumed") — that *is* the discipline.
- Hedged or clearly speculative prose, plans, opinions, or taste.
- Sampling over genuinely unbounded spaces (see C4's carve-out).
- Informal prose in casual contexts (chat, brainstorms) unless asked.

Strength of prose is not the offense; **confidence exceeding evidence** is.
When unsure whether a claim's cited evidence really is narrower than its
scope, say what you could not verify instead of guessing a violation.

## Report format

Use this structure:

```
## Claims audit: <target>

<verdict: 1-2 sentences. Include the denominator: "N claims examined,
M findings." Never "no violations" — say "none found in the N claims
examined", and name anything you could not check (C6 sweep, cited tests
you could not open).>

### Findings (most severe first)

1. **[C2] "<the exact sentence, quoted>"** (<location>)
   - Evidence class as written: <projected> — actual: <assigned class>
   - Why it matters: <one sentence, concrete>
   - Rewrite: "<a compliant version the author can paste>"

...

### Checked and sound
<optional, brief: strong claims that ARE properly backed — naming these
shows the audit distinguishes strength from overreach.>
```

Order findings by consequence (what a reader would wrongly build on), not
by order of appearance. Every finding gets a paste-ready rewrite; a finding
without a rewrite is a complaint, not a review.

## Apply the rules to your own report

The audit must survive its own standard: label how each finding was
established (textual pattern vs. verified against the repo's tests/CI),
give denominators, and never claim your own coverage universally. If you
cited a test while auditing, you read it — a rewrite that names a test you
did not open is itself a C1.
