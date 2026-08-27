# Investigating with logs: what to keep, when to add it, and what flags are for

Notes written after issue #73, in which a reported safety violation went five
occurrences without being settled — because each occurrence deleted the one
thing that could settle it. This is the lesson, generalized, so the next
investigation does not repeat the shape of that one.

The audience is whoever picks up the next unexplained failure. That will
usually be an agent, which is why this is in the repository rather than in
somebody's memory.

---

## 1. Derived observations versus primary state

Every failure report is a *derivation*: something observed the system and
produced a summary. The Chain-of-Blocks observer watches `/chain` and reports
"n0 showed `0x05b9…` at n=2150". A metric, a stack trace, a test assertion
message — all derivations.

Primary state is what the system actually did: the applied log, the durable
snapshot, the bytes on disk.

**When the reliability of the derivation is itself in question, only primary
state can answer.** This is the whole of #73 in one sentence. A divergence
report is two hashes at one height. That looks *identical* whether two replicas
really applied different commands or the observer mis-read one of them — and
those two possibilities are the entire question. No amount of staring at the
report distinguishes them. The applied logs do, immediately.

The trap is that derived observations are *articulate*. They come pre-formatted,
with names and numbers, and they invite analysis. Primary state is a
`node-0.durable.bin` that has to be loaded before it says anything. The
articulate thing is not the informative thing.

**Ask early: is the artifact I am reasoning about evidence of the system, or
evidence of the observer?**

---

## 2. Evidence has a lifetime, and yours is probably shorter than you think

Before investigating anything, ask two questions in this order:

1. **What observation would settle this?**
2. **Does that observation survive the run?**

If the answer to (2) is no, then producing it *is the task*, and everything
else is deferred. Not "step 2 on the list of suggested next actions" — the
task.

In #73, the answer to (1) was known and written down from the first occurrence:
compare the durable applied logs. The answer to (2) was no: `queso-soak` ran
every seed against a `tempfile::tempdir()`, deleted the instant the run
returned. That mismatch sat unaddressed across five occurrences while
hypotheses were raised, argued and discarded — every one of them reasoning from
a report whose provenance was the open question.

The decision that caused it was not wrong when it was made. A scratch directory
is the right choice for a test harness that passes. It became wrong the moment
the first divergence was reported, and nobody revisited it at any of the five
subsequent points where somebody was looking directly at a report it had
gutted.

**A retention decision made before the first failure is a guess. Re-make it the
first time something interesting dies.**

---

## 3. The failure mode this is guarding against

Investigation that cannot conclude, but feels productive.

Symptoms, all of which #73 exhibited:

- Hypotheses are "ruled out" — but by *reasoning* from a derived report, not by
  evidence. Both are worth doing. They are not the same thing, and they get
  written down in the same sentence.
- A fix is predicted to close the issue. (#74 fixed a genuine torn read and was
  predicted to close #73. It did not.)
- Each occurrence is treated as a fresh puzzle rather than as more evidence
  that the report is insufficient *and always will be*.
- The rate is asserted rather than measured. #73 was called "timing-dependent"
  and "did not reproduce on an idle machine". It fails roughly one seed in four
  in the right window. That single unchecked assumption made under-investment in
  reproduction look reasonable.

The test: **if I keep doing exactly this for another week, could I reach a
conclusion?** If no, stop theorizing and go build the instrument.

This is the same defect this repository keeps finding in its own tests — a
check that asserts more than it establishes — one level up, in the process
rather than the code. Vacuous kill accounting (#72), an observability path
manufacturing a safety report (#74), a divergence counted twice (#77). A
vacuous *investigation* is the same shape and is harder to notice, because
nothing goes red.

---

## 4. When to add logging

Not "log everything". Volume is not the axis that matters; the following are.

**Log the inputs to a derivation, not only its output.** If a component
summarizes, the summary is what you get for free and the inputs are what you
will wish you had. `/chain` reports `(n, h)`; the applied log is the input. The
report was always available and never sufficient.

**Log provenance, not just values.** #73's reports say "n2 showed `0x485b…` at
n=2052". They do not say *where that came from* — the checkpoint table or the
live frontier. `queso_conformance::observer::Sample` has no field for it. That
one missing field is worth more than every additional hash: with it, three
occurrences would have shown at a glance that the disputed value is always the
challenger's live frontier against an ahead replica's checkpoint. Without it,
recovering that took cross-referencing frontier tables by hand, occurrence by
occurrence. **See the open gap in §8.**

**Log the gap between intent and action.** The soak counts injections
*performed* against what the traversed schedule *asked for*, per fault kind.
That comparison is what caught #72 — a count taken from the schedule alone
would have stayed green while the injection path was broken. Anywhere the
system decides to do something and then does it, log both, and compare them.

**Log at boundaries evidence cannot cross.** Process exit, restart, deletion,
network hop. State that does not survive a boundary must be emitted before it,
or it is gone. A replica's in-flight proposer state dies with the process by
design; if you ever need it, the log line before the crash is the only copy.

**Prefer preserving state over emitting text.** One `node-0.durable.bin` beats
a million log lines about what was applied, because it can be *recomputed
from* rather than read. The soak now preserves the data dir of any failing seed
and re-folds each replica's chain from it. That is the difference between a
report you argue about and one you adjudicate.

**Log timestamps on one clock wherever you may need to correlate.** #73's
sharpest structural clue — the divergence surfaces at the exact instant the
challenger advances — was only visible because `observed_at` and
`last_progress_at` appear in the same report on the same clock. Cross-subsystem
correlation is where log value compounds, and it is impossible after the fact
if the clocks were never comparable.

---

## 5. What flags are actually for

Gating detail behind a flag is a good instinct and it has one sharp limit.

**A flag you must set in advance is useless for a failure you cannot
reproduce.** You cannot re-run a heisenbug with `-v`. #73 is nondeterministic
by construction — real threads, real timers, real TCP — and the seed reproduces
the *turbulence*, not the failure. Any evidence gated behind "run it again with
more logging" was, in practice, gated behind an event that may not recur for
days.

So:

| tier | when | gate |
|---|---|---|
| **Default** | every run, pass or fail | none — must be cheap enough to always pay |
| **On failure** | automatic, at the moment of failure | none — a flag here is a bug |
| **Deep detail** | targeted re-runs of something reproducible | a flag |

**Default tier.** What a *passing* run must emit to prove it was not vacuous.
This project already does this well: comparisons, frontier, acked submissions,
injections by kind, proxy accepts. A green run that says nothing is
indistinguishable from a green run that tested nothing.

**On-failure tier.** Everything you would wish for, dumped automatically,
because there is no second chance to ask for it. `retain_evidence` keeping a
failing seed's data dir is this tier. So is the post-mortem adjudication the
soak now prints inline. **Never put this behind a flag**, and never behind an
artifact download either — an artifact must be noticed before it expires, and
some environments cannot fetch them at all. Put the verdict where the failure
is already being read.

**Deep-detail tier.** Expensive tracing for things you can reproduce on demand
— a unit test, a deterministic simulation run, a seed that fails reliably.
Flags are exactly right here and the cost of being off by default is only a
re-run.

**When detail is expensive but the failure is rare, the gate is space, not
verbosity.** Keep a bounded ring in memory and dump it on failure — the flight
recorder pattern. `ChainCheckpoints` already does this (a 256-entry ring that
reports `truncated: true` rather than silently serving a partial history), as
does the observer's per-replica transition cap. A ring costs a fixed amount
forever and is there when you need it; a flag costs nothing and is never set
when you need it.

**Say so when you truncate.** A bounded buffer that silently drops the oldest
entries turns "we have the history" into a false claim. Both rings above report
their own truncation. Do the same.

---

## 6. Sample the system, or enumerate the state?

Every unexplained failure gets attacked one of two ways. **Sampling**: run the
whole system many times under turbulence until it misbehaves again.
**Enumeration**: identify the function that produced the wrong answer and check
every input it can receive.

Sampling is what you reach for when you do not know where the defect is. It is
also slow, expensive, and may have *no power at all* to find the thing you are
looking for — a fact you will not discover by doing more of it.

#83 was sampled for three nights: 32 soak seed-runs, roughly thirteen hours of
turbulence, four occurrences, cause unsettled. The defect was a pure function
of **eight states**. Enumerating them took 32 evaluations and under a
millisecond, and yielded not just "there is a bug" but the exact combination
that triggers it.

Two tells said "enumerate" from early on. Neither was read.

### An invariant fingerprint measures the state space

By the second occurrence there was a table, and every row of it was identical:
always node 0, always its own catch-up probe, always `seq == slot`, always
alone. That was read as *consistent, therefore the bug is real* — true, and
nearly useless.

What it actually says is this: **a rare race over a large space produces varied
symptoms. An invariant fingerprint means the outcome is fully determined by a
small amount of state.** Five identical signatures was a measurement of how big
the space was, sitting in a table that had already been drawn.

*Ask of every repeated failure: how much do the occurrences differ from each
other? If the answer is "not at all", stop sampling and go find the function.*

### The vocabulary picks the method before you do

Everything written about #83 used sampling language: rate, seed, window,
reproduce, arm, power. That vocabulary carries a model — *the space is large,
so sample it* — and every move it suggests is a sampling move: more seeds,
longer runs, another arm. The enumeration move is not rejected from inside that
frame; it is **invisible**. Nobody chose the model. It arrived with the word.

It was worse here because "seed" means three incompatible things in this
repository — a frozen simulator corpus, a nightly window that advances, and a
soak schedule that does *not* reproduce its own failures (see
`docs/what-each-test-establishes.md` §1). One word for all three concealed that
the soak seed was never going to reproduce anything, which is the assumption
the entire hunt rested on.

*When every sentence about a problem uses the vocabulary of one method, say the
other method's name out loud once and see whether it applies.*

### The question that was missing

This document already asked "what observation would settle this?" — an evidence
question, and a good one. It never asked:

> **What function computed the wrong answer, and is the state it reads
> bounded?**

For #83: `fast_path_value`, and yes. Bounded state means the property is
*decidable*, not merely testable, and a decided property needs no rate, no
window, and no luck.

### When sampling is still right

Enumeration needs a suspect. The soak keeps its place precisely because it
finds the failures nobody has thought to enumerate, and because it exercises
the real network, real threads and real disk that no enumeration models — the
simulator structurally cannot reproduce a durability gap, and #83 was found by
the soak, not by reasoning.

So: **the soak is a discovery instrument, not an adjudication one.** Once it
has told you *where* to look, stop running it and go read the function. The
mistake was not sampling first. It was continuing to sample after the sample
had already said everything it could.

---

## 7. Checklist for the next unexplained failure

0. **What function computed the wrong answer, and is the state it reads
   bounded?** If bounded, enumerate it — do not sample the system around it.
   (§6)
1. **How much do the occurrences differ from each other?** Not at all means the
   space is small. Vary a lot means you are genuinely searching. (§6)
2. What observation would settle this? Name it before doing anything else.
3. Does it survive the run? If not, make it survive. That is now the task.
4. Is what I am reading evidence of the system, or of the observer?
5. For each hypothesis I discard: evidence, or reasoning? Write down which.
6. What is the actual failure rate? **Measure it; do not assert it** — and
   before running an experiment, write down the trial count that rate implies.
   Without both numbers a clean result means nothing.
7. Has my instrument ever detected this? A run of it that finds nothing on a
   configuration known to fail is a measurement of the instrument, not a
   reassurance about the system.
8. What did this run throw away?
9. If I do exactly this for another week, could I conclude? If not, stop.

---

## 8. Open gap

`queso_conformance::observer::Sample` carries no provenance. A sample from a
replica's checkpoint table and one from its live frontier are indistinguishable
once ingested, and #73's fingerprint turns entirely on that distinction.
Adding a provenance field, and printing it in `render_report`, would make the
next divergence report self-describing on the axis that has mattered most in
every occurrence so far.

---

## 9. Retention

Failure artifacts are kept for **30 days** (`nightly-soak.yml`). That is
deliberate and sufficient for the nightly cadence: a failure gets several
nights of attention before its evidence ages out.

It is *not* sufficient for a long-lived issue. #73 has been open longer than
30 days, and by now the artifacts from its earliest occurrences are gone. So:

**When a run produces evidence that matters, quote it into the issue.** An
issue outlives every artifact. #73's original report was pasted in full for
exactly this reason, and that is the only reason it can still be reasoned about
today. The retention window buys time to look; the issue is what makes a
finding permanent.
