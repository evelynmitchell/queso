# Trigger measurements — 2026-08-27

What the skill's *description* was measured to do, so nobody reads its
triggering behavior as better than the evidence. Method and numbers below;
this file exists because the skill's own rules demand that an instrument's
detection power be recorded, "unmeasured" included.

## Method

`skill-creator`'s description-optimization loop: 20 realistic queries
(10 should-trigger, 10 near-miss negatives), split 12 train / 8 test,
each query run 3× per candidate description against `claude -p`
(model `claude-fable-5`, CLI 2.1.247), triggering detected from the
session's first tool call. Three candidate descriptions were evaluated
(the original plus two LLM-proposed rewrites); an earlier 5-candidate run
was discarded — its 30s-per-query timeout killed every child mid-flight,
measuring the timeout rather than the description (recorded here as a
caution: that run's flat identical scores across five different
descriptions were the tell).

## Results (valid run)

| candidate | positive triggers | false triggers |
|---|---|---|
| original description | 3/30 | 0/30 |
| rewrite A | 4/30 | 0/30 |
| rewrite B | 3/30 | 0/30 |

The original won on held-out test; **no rewrite beat it**, so the
description shipped unchanged.

## What this licenses

- **Specificity, measured: perfect in this sample.** 0 false triggers in
  90 negative runs across the three candidates, including keyword
  near-misses ("insurance claims", "unit test assertions", "fact-check
  this article", "proofread"). n = 90; a rate below ~4% is consistent
  with this sample.
- **Unaided sensitivity, measured: low in this harness.** 3/30 positive
  runs triggered for the shipped description. Soft phrasings ("does this
  PR body oversell it?", "am I overclaiming?") mostly did not summon the
  skill on their own.
- **Practical consequence:** invoke it by name. `/claims-check <target>`
  as a slash command invokes the skill mechanically, independent of the
  description. Two manual by-name probes ("run claims-check on <file>",
  "use the <skill> skill on this sentence") both triggered (2/2 — small
  n, stated as such).

## Instrument caveats (why the sensitivity number is a lower bound)

The harness installs the skill under a hash-suffixed name as a bare
command file, runs a cold headless session against files that do not
exist, and counts a trigger only when the *first* tool call is the Skill
tool naming that hash — a session that begins with any other tool, or
answers in text, scores as untriggered even if it would have consulted
the skill later. Real interactive sessions differ on every one of those
axes, so treat 3/30 as "the description alone rarely fires in this
synthetic setup", not as the in-situ rate. What transfers with most
confidence is the specificity result and the by-name reliability.
