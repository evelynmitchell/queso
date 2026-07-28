# How does Queso stand up to Antithesis's Raft bug hunt?

*2026-07-28. A gap analysis prompted by two Antithesis publications:
["Finding bugs in Raft implementations"][raft-blog] and the
[Chain-of-Blocks workload][cob-docs]. Written in this project's usual
voice — the point is to find where we're weak, not to award ourselves
marks.*

[raft-blog]: https://antithesis.com/blog/2026/finding-bugs-in-raft-implementations/
[cob-docs]: https://antithesis.com/docs/resources/chain-of-blocks/

---

## The short version

Antithesis took several mature, widely-deployed Raft implementations and,
using autonomous fault-injection at the hypervisor level plus a
deliberately simple "chain of blocks" workload, found real safety bugs in
hours — including in HashiCorp's `raft`, the library behind Consul and
Nomad. Their thesis is uncomfortable and correct:

> The bugs aren't in the algorithm. They're in the gap between the
> algorithm and the implementation — the part your model checker never
> sees.

Queso is a formally-grounded implementation of a *different* consensus
algorithm (QuePaxa, SOSP '23), but it makes the same bet every consensus
implementation makes: that verifying the core buys you a correct system.
So the honest question is: **would Antithesis find bugs in Queso?**

The answer is a split decision. Queso is *structurally* immune to the
single most serious bug Antithesis found, and its safety property is
already the Chain-of-Blocks property under a different name — checked by
property tests, a linearizability checker, and two TLA+ specs. But the
place Antithesis goes looking — the real-I/O shell around the verified
core, under sustained autonomous fault — is precisely Queso's
least-tested surface, and its own review history proves bugs live there.

The rest of this post is the long version, and the plan (Phase 9,
[#54][i54]) to close the gap.

---

## What Antithesis actually did

Two ideas do the work.

**1. Autonomous deterministic simulation at the hypervisor level.**
Queso's own DST runs the consensus *logic* against a mock `Ctx` inside a
single-threaded, in-memory event kernel. Antithesis runs the *real,
unmodified binary* — real threads, real sockets, real syscalls, real
disk — inside a deterministic hypervisor that can inject faults
(partitions, clock skew, crashes, disk turbulence) and, crucially, that
*autonomously searches the state space*: it steers toward interesting
schedules rather than replaying a script the author thought of. When it
finds a violation, the whole execution is deterministically replayable.

**2. A workload simple enough to have an obvious invariant.**
Chain-of-Blocks is almost trivial: each committed block extends a hash
chain — block *n* embeds the hash of block *n-1*. A stateless client keeps
appending. Two properties fall out for free:

- **Safety:** no two replicas ever disagree about the block at a given
  height. Because each block commits its predecessor's hash, *any*
  divergence — a fork, a lost commit, a stale read served as fresh —
  breaks the chain and is caught immediately. You don't need a
  sophisticated oracle; the hash *is* the oracle.
- **Liveness:** under eventually-healed faults, the chain must keep
  growing.

Antithesis reports that partitions *alone* — no exotic fault, just network
splits and heals — surfaced safety violations within an hour.

The bugs they found in HashiCorp `raft` clustered around four *implicit
assumptions* the implementation made that the Raft paper does not license:

1. **An async heartbeat could observe torn state** — a heartbeat handler
   reading leader state concurrently with a mutation, a data race the
   model never has because the model is single-threaded.
2. **Persistence was assumed atomic** — state written in multiple steps,
   so a crash between steps left a replica recovering from a
   half-written, internally-inconsistent snapshot.
3. **A recovery/catch-up path re-derived state incorrectly** — the "happy
   path" was verified; the extension that brings a lagging or restarted
   replica back was not, and it could resurrect stale data.
4. **Cluster-membership edge cases** outside the verified core.

Notice the shape: none of these are algorithm bugs. Every one lives in the
*shell* — concurrency, persistence, recovery, reconfiguration — that a
model of the core abstracts away.

---

## Where Queso stands up well

This section is deliberately for the record, so the work Phase 9 proposes
stays scoped to the *actual* gap rather than re-litigating things Queso
already does.

**The async-heartbeat race (their most serious finding) can't exist in
Queso — structurally, not by luck.** Queso's `SmrNode` is
`Rc<RefCell<…>>` and therefore `!Send`. A single tokio task owns it end to
end (`driver::run_node`); every message, timer, and client submission is
serialized through that one task's event loop. There is no second thread
that could hold a reference to node state, so there is no torn-read race
to find. The compiler rejects the shape of the bug. This was a design
choice made for *determinism parity* with the sim kernel, and it happens
to buy immunity to Antithesis's headline bug for free.

**Non-atomic persistence is closed.** Queso persists its entire `Durable`
state as one atomic snapshot: write to a temp file, `fsync`, `rename` over
the real file, `fsync` the directory. A crash at any point leaves either
the old complete snapshot or the new complete snapshot — never a torn
half-write. This is issue #36's whole subject, and it was itself *found by
a fresh-environment review* of the real-transport durability path (see
below — this cuts both ways).

**The Chain-of-Blocks safety property is already Queso's safety
property.** CoB's "no two replicas disagree at a height, no fork, no lost
commit" is exactly Queso's P1 (agreement), P5/P6 (log-prefix consistency),
and linearizability (P-lin) — which Queso checks three ways: a
property-test corpus over the DST kernel, a Wing-Gong linearizability
checker over recorded histories, and TLC model-checking of both the
abstract (Algorithm 1) and concrete (Algorithm 4) cores. The hash-chain
oracle is a *cheaper, sharper* way to detect a violation Queso already
defines and tests for; it is not a property Queso lacks.

So on the axis of "is the safety invariant specified and checked," Queso
is in good shape. The exposure is on a different axis entirely.

---

## Where Queso is exposed

**The verified core is not the deployed system.** Everything in the
paragraph above — the property model, the linearizability checker, the
TLA+ specs — runs against the `queso-sim` kernel: a single-threaded,
in-memory, deterministic model of time, network, and disk. The thing you
would actually deploy is `crates/net`: a real tokio event loop, real TCP,
real `fsync`, real process restarts. **None of the formal verification
touches that shell.** This is the identical gap Antithesis exploits, and
saying it plainly: *app-level DST verifies the model; it does not verify
the implementation.*

**Queso's own history proves the bugs live in the shell.** This isn't
hypothetical. Every real bug found in the real-transport layer was found
by *manual* fresh-environment review, not by the verified core:

- **#36** — a lost-write durability bug in the real persistence path. The
  sim's favorable in-memory model never exercised the ordering that broke.
- **The Phase 7.1 self-send-drop** — a node dropping a message to itself,
  an artifact of the real transport that the sim's delivery model didn't
  have.
- **#22** — the catch-up "zombie replica": a *recovery-path* bug where a
  lagging replica could come back with stale state. This is
  structurally the *same class* as Antithesis's Bug 3 (recovery
  re-derives state incorrectly).
- Several **vacuous fault tests** — tests that appeared to exercise a
  partition or a delay but passed even with the fault injector neutered.
  Each was caught only because a reviewer explicitly neutered the
  mechanism and confirmed the test then failed. An autonomous searcher
  doesn't have that discipline lapse; a scripted test suite does.

Read that list against Antithesis's four assumptions. Queso is immune to
#1 (the race) and has closed #2 (atomicity). But #3 (recovery/catch-up) is
an *unverified extension* Queso has already been bitten by once, and it is
verified today only by scripted, in-process tests — exactly the regime
Antithesis argues is insufficient.

**Scripted faults are not autonomous search.** Queso's Phase 7.4 nemesis
(`crates/net/src/nemesis.rs`) injects latency, drops, resets, and
partitions — but on schedules the author wrote. That finds the faults the
author imagined. Antithesis's value is finding the schedule the author
*didn't* imagine. A hand-written partition test and a hypervisor steering
toward the worst interleaving are not the same instrument.

**Reconfiguration (their assumption #4) is a stated non-goal.** Queso
deliberately doesn't do dynamic membership yet, so that class of bug is
out of scope by construction — worth noting only so the scorecard is
complete.

---

## The honest scorecard

| Antithesis finding | Queso's status |
|---|---|
| Async heartbeat observes torn state (race) | **Immune by construction** — single-task `!Send` `SmrNode`, no second thread to race. |
| Non-atomic persistence | **Closed** — whole-`Durable` atomic snapshot (temp → fsync → rename → fsync-dir), #36. |
| Recovery/catch-up re-derives state wrong | **Exposed** — unverified extension; already bitten once (#22); only scripted in-process tests today. |
| Membership/reconfig edge cases | **Out of scope** — dynamic membership is a stated non-goal. |
| *Methodology:* real binary under autonomous fault | **Not done** — all formal work runs against the in-memory sim; the real `crates/net` shell has no DST coverage. |
| *Workload:* Chain-of-Blocks safety oracle | **Property exists (P1/P5/P6/P-lin), oracle doesn't** — no hash-chain workload wired up. |

Two rows are green because of deliberate design decisions. Two are
red-or-amber because they're the real-I/O surface, and that surface is
Queso's least-tested one. That's not a contradiction — it's the exact
pattern Antithesis predicts: the model is strong, the shell around it is
where you should be nervous.

---

## What we're doing about it: Phase 9

The gap is specific and closeable, so we've scoped it as an epic
([#54][i54]) with two concrete sub-issues plus a stretch goal. The framing
principle: **additive, no change to the verified core's logic** — this
targets the real-transport and recovery surface specifically.

**[#55][i55] — Phase 9.1: Chain-of-Blocks workload + divergence/liveness
observer.** Bring the hash-chain oracle to Queso: a minimal hash-chain
state machine (each transition embeds the prior hash), a stateless client
that keeps appending, a *divergence observer* that flags any replica
disagreement at a height, and a *liveness observer* that flags stalls
under eventually-healed faults. Runnable in-process first, reusing the
existing `queso-bench` load generator, the `/metrics` endpoint, and the
linearizability recorder. No change to the SMR core — the chain hash rides
as the command value (or behind a harness-only command), so the verified
KV logic is untouched.

**[#56][i56] — Phase 9.2: Real-binary-under-fault harness.** The piece
that actually closes the methodology gap: drive the Chain-of-Blocks
workload against **real `queso-node` OS processes** (or the Docker image),
under sustained partition and turbulence — the 7.4 nemesis wired end to
end, and/or toxiproxy/`tc`/`iptables` — asserting *no divergence* and
*eventual progress*, with seeded reproducibility, a bounded CI variant,
and a long-soak mode. This exercises the real tokio scheduling, real
sockets, and real disk that the in-process sim can't, against the recovery
path that history says is where the bugs are.

**Phase 9.3 (stretch) — Antithesis integration.** Package the Docker
image + workload + property assertions for an actual Antithesis run, if an
account becomes available. The buildable artifacts live here; the run
itself needs the owner's account. Even without it, 9.1 + 9.2 move the
needle on the two red rows: they give Queso a sharp safety oracle and run
it against the real binary under fault — the two things the current suite
lacks.

---

## Conclusion

Antithesis's argument isn't that Raft is wrong; it's that *verifying a
model is not verifying a system*, and the delta is where the dangerous
bugs hide. Queso is an honest test of that claim on a different algorithm.
It comes out better than average on the *design* axis — the single-task
`!Send` architecture and atomic snapshots close, by construction, two of
the four bug classes Antithesis found — and it already *specifies and
checks* the Chain-of-Blocks safety property three ways.

But it comes out exactly where Antithesis predicts on the *methodology*
axis: the formal verification stops at the sim boundary, and the real-I/O
shell — especially the recovery/catch-up path — is verified today only by
scripted, in-process tests that Queso's own reviews have repeatedly caught
being weaker than they looked. The recovery path has already produced one
real bug (#22) of the same class Antithesis found.

The right response to a good bug-finding methodology isn't to claim you'd
have caught the bugs. It's to point your testing at the same gap. Phase 9
does that. Until 9.2 is green under sustained autonomous-*ish* fault
against the real binary, the honest status stays what it's always been in
this project: **the core is verified; the deployed system is not — yet.**

---

## Sources

- Antithesis, *Finding bugs in Raft implementations* (2026):
  <https://antithesis.com/blog/2026/finding-bugs-in-raft-implementations/>
- Antithesis Docs, *Test state machine replication with a chain-of-blocks
  workload*: <https://antithesis.com/docs/resources/chain-of-blocks/>
- Queso Phase 9 epic: [#54][i54] · Chain-of-Blocks workload: [#55][i55] ·
  Real-binary-under-fault harness: [#56][i56]
- Prior-art bugs referenced: #36 (durability), #22 (catch-up recovery),
  Phase 7.1 self-send-drop, and the vacuous-fault-test reviews on #43/#48.

[i54]: https://github.com/evelynmitchell/queso/issues/54
[i55]: https://github.com/evelynmitchell/queso/issues/55
[i56]: https://github.com/evelynmitchell/queso/issues/56
