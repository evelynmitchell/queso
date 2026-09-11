# Falsifier replay

`crates/` carries **43** `Falsifier, run:` markers across 16 files. Each records a
mutation and the kill count it produced *at some past commit*. Until issue #128
nothing re-ran any of them.

That is a specific hazard in this repo rather than a tidiness complaint. A
marker is what the next reader treats as established fact — `CLAUDE.md`'s
opening is about exactly that failure mode — so a refactor that quietly removes
a test's detection power leaves a sentence behind still asserting the old
number. `docs/conformance-matrix.md` §6.8 put it as *"the counts rot silently"*.

This directory turns those pasted numbers into asserted ones, which is the same
move [`tlc-nightly.yml`](../.github/workflows/tlc-nightly.yml) made for the TLA+
state counts.

## Two checks, split by cost

| | What it does | Cost | Where it runs |
|---|---|---|---|
| `--check-anchors` | Every registered mutation's `find` string still occurs **exactly once** in its file | no build, milliseconds | `ci.yml`, every commit |
| `--replay` | Applies each mutation, runs its scoped tests, asserts the named kills still happen and the named controls still pass | one rebuild per mutation, ~10 min | `falsifiers-nightly.yml`, 03:00 UTC |

The anchor check is the cheap one and catches the commonest rot: code moved out
from under a mutation, so the recorded edit no longer describes anything. §6.4
records that 3 of 20 markers re-run in #114 needed their mutation *reconstructed*
before it reproduced — an anchor check makes that condition visible the day it
appears instead of years later.

## What is asserted

Each entry names `expect_fail` (tests that must still die) **and** `expect_pass`
(controls that must not). Both directions are load-bearing: without controls, a
mutation that reddens the whole suite would read as a precise instrument, which
is the §6.8 mistake in miniature.

An entry with an empty `expect_fail` is a **recorded zero** — the assertion is
that nothing named detects it. Four of the nine are zeros, and they are the
entries most worth having: a zero that silently becomes a kill means somebody
added coverage, and the docs claiming a zero are now wrong.

## The checker's own detection power, measured

Three negative controls, run against this tool (#128):

| Control | Result |
|---|---|
| point a `find` at a string absent from the tree | `--check-anchors` reports drift, exits 1 |
| add a test to `expect_fail` that does not fail | `--replay` reports `LOST POWER`, exits 1 |
| add a test to `expect_pass` that does fail | `--replay` reports `CONTROL BROKE`, exits 1 |

Shipping an unmeasured checker into a repo that exists to measure checkers
would have been the wrong kind of irony.

## What is registered, and what is not

**Nine** mutations are registered, spanning five source files and covering
P8, P8a, P13, P14, P15 and P16 — the ones measured first-hand in #125, #150
and #152, where the exact edit and the exact expected outcome are known.

The other **34** markers are *not* registered. They are not lower quality; the
edit simply has not been transcribed into a machine-applicable form, and
writing one down from prose without re-running it would recreate the very
problem this directory exists to fix. Adding them is mechanical and incremental
— see below. The count of registered entries is deliberately not claimed to be
the count of markers.

Also deliberately excluded: the genuinely **stochastic** markers. Most recorded
counts are deterministic (24 markers record `8/8`, plus `3/3`, `20/20`,
`200/200`, `1/1`), which a single run can gate. A handful are not — `9/156`
(5.8%), `41/100`, `7/16`, `3/7`, `5/100` — and gating those on one run would
produce a flaky job that teaches everyone to ignore it. §6.12 is the standing
warning about reading a null result without its power; the same caution applies
to gating on one. Registering those needs a per-entry sample size and a
tolerance, which is future work, not an oversight.

## Adding a mutation

1. Apply the edit by hand and run the scoped tests. Note **which assertion**
   fires, not just that the file went red (§6.8, §6.13).
2. Add an entry to `registry.toml`: `find` must be a string that occurs
   exactly once in the file, taken verbatim from the tree.
3. `python3 falsifiers/replay.py --replay --only <id>` — it must pass.
4. Deliberately break it both ways (a bogus `expect_fail`, a bogus
   `expect_pass`) and confirm the tool reports it. The entry is only worth
   having if it can fail.

## Safety

`--replay` edits files in the working tree. It refuses to run when tracked
files are modified, restores every file from the exact bytes it read in a
`finally`, and re-checks `git status` before exiting — a tree left dirty is
itself reported as a bug.
