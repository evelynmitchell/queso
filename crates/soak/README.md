# `queso-soak` — Chain-of-Blocks against real processes (Phase 9.2)

Drives [`queso-conformance`](../conformance)'s Chain-of-Blocks workload and
observers against **real `queso-node` OS processes**, over real TCP, under
socket-level turbulence. Issue [#56], part of the Phase 9 epic [#54].

This is where Phase 9 starts actually reaching the sim↔real gap. 9.1 built
the workload and observers and ran them in-process, where they exercise the
same code the existing DST suite already covers. Here the same observers —
unchanged — watch real processes: real tokio scheduling, real sockets, real
disk, real `SIGKILL`, and partitions that genuinely close TCP connections.

Slice 3 adds the sustained half: a **seeded, randomized fault schedule**
driven continuously over a run, instead of a handful of scripted faults.

## The three pieces

**`proxy` — the out-of-process nemesis.** A TCP turbulence mesh: one
directed proxy per ordered pair of replicas, so every peer-to-peer byte
crosses something the harness controls. Cutting a link tears down live
connections and refuses new ones, so a node sees its peer connection
genuinely break and has to re-establish it. Latency injection delays
forwarded chunks.

Why not the existing `queso_net::nemesis`: that one drops and delays
*already-decoded application frames* inside a node's own transport. A
partition there never closes a socket, never trips a reconnect, never makes
a write fail — so the reconnect and recovery code, where this project's real
bugs have been, goes untested.

Why not toxiproxy / `iptables` / `tc` (which [#56] suggests): toxiproxy is an
external binary the workspace would have to assume is installed — the same
reasoning that kept `etcd-client` out of `queso-compare` — and
`iptables`/`tc` need root and mutate machine-wide state, a poor trade for a
test anyone should be able to run. A ~200-line tokio proxy cuts real
connections just as convincingly.

**Honest limit:** this cuts and delays at the byte-stream level. It does not
reproduce kernel-level packet loss, reordering, MTU effects, or half-open
connections where one side never learns the peer is gone. A `tc`-based
nemesis is still stronger for anyone willing to run as root.

**`cluster` — `RealCluster`.** Implements `queso_conformance::CobTarget`
over spawned `queso-node` processes, so 9.1's `run`/`converge`/`Observer`
work against them unchanged:

| `CobTarget` | in-process (9.1) | here (9.2) |
|---|---|---|
| `submit` | enqueue on a replica | `queso_net::client::submit` over TCP |
| `advance(units)` | run the sim kernel N ticks | sleep N **milliseconds** |
| `now` | the sim's logical clock | milliseconds since cluster start |
| `poll_samples` | read every replica's applied log | `GET /chain` on every replica |

Samples here are checkpoint-dense rather than slot-dense, because a replica
across a process boundary reports only the hashes it folded at its
configured spacing. That is precisely the observability 9.1's
`Observability::Checkpoints` mode modelled, and why the node publishes at
fixed `n` (see [`queso-net`'s `/chain`](../net#phase-92-get-chain--conformance-observability-issue-56)).

**`schedule` + `soak` — the sustained driver.** A seeded generator draws a
fault schedule; the driver walks it, reconciling the injected faults against
what the schedule says should be in force each step, offering load, polling
`/chain`, and checking safety continuously.

The premise is issue [#54]'s: the bugs that matter live in fault *sequences*
nobody would script. Slice 2's scenarios only ever visit states someone
thought to write down. A generated schedule reaches a node crashing while a
one-way cut is already in force, healing into a fresh partition, restarting
mid-catch-up.

Two design decisions carry most of the weight:

**Faults overlap on purpose.** An earlier generator always advanced past the
previous fault's end, so no two faults were ever in force at once — which
quietly made the whole concurrent-fault path dead code, budget and driver
alike. A third of the time the next fault now starts *inside* the current
one's window. `schedules_really_do_overlap_faults` asserts it stays that
way; at `n = 5`, 57 of 64 seeds now reach two concurrent node faults, where
before it was none.

**Never more than `f = (n-1)/2` nodes at once.** Not timidity — it is what
makes the liveness verdict mean anything. With a majority always available
the cluster is *obliged* to keep deciding, so a stall is a real failure
rather than the expected result of having killed a quorum. A soak that
knocked out majorities could only assert "eventually, after everything
heals", which is a far weaker property and a much worse bug detector.
Safety, by contrast, is checked continuously and unconditionally: no amount
of turbulence licenses two replicas to disagree at the same `n`.

Safety and liveness are therefore judged differently. Divergence fails the
run the moment it appears, under fault. Liveness is only asked *after* the
turbulence heals, every crashed process is back and answering `/health`, and
`workload::converge` has given every replica work — because a partitioned
replica is *supposed* to fall behind (P5 permits arbitrary lag and forbids
only divergence), and because Queso has no background replication push, so
an idle healthy replica is indistinguishable from a wedged one until it is
asked to do something.

**Load is offered without blocking on it.** A client reaches a replica on
its *client* port, which does not cross the turbulence mesh — so an isolated
replica still accepts the connection and then cannot make progress on it.
The submission does not fail, it hangs. Slice 2's blocking `submit` absorbed
the whole `submit_timeout` before recording that, which for a soak is fatal
to the point of the exercise: offered load would collapse exactly during the
partitions the run exists to test, and a cluster nobody is asking to do
anything cannot be caught failing to do it. `submit_detached` hands the wait
to the runtime, so a partitioned target costs one idle task rather than a
stalled driver, and the report carries `deferred`/`undrained` counts so the
cost is visible rather than hidden.

## Running it

```sh
cargo build --all             # queso-soak spawns the queso-node binary
cargo test -p queso-soak      # unit tests only: proxy, schedule generator, fault reconciliation

# What CI's soak job runs: the scripted scenarios plus the bounded soak, ~3 min.
cargo test -p queso-soak -- --ignored --skip a_long_soak_over_many_seeds

# The long mode: many seeds, minutes each.
cargo test -p queso-soak -- --ignored --exact a_long_soak_over_many_seeds
cargo run --release -p queso-soak --bin queso-soak -- --seeds 20 --duration-secs 300
```

The `queso-soak` binary is the long mode proper: it runs a seed range, prints
each schedule and report, and exits non-zero if any seed found a violation
**or produced a vacuous run** — so it works as a nightly job as readily as a
manual hunt.

**A failing seed keeps its cluster state.** Each seed runs against
`<--failure-dir>/seed-<n>` (default `soak-failures/`); a clean seed's
directory is deleted, a failing one is kept and its path printed. That
directory holds every replica's durable snapshot, and a snapshot carries the
replica's `applied_log` — which is the only thing that can settle whether a
reported divergence is a real Agreement violation or an artifact of the
observability path: recompute each replica's chain from its own log and
compare at the disputed slot. Issue #73 collected three divergence reports
before this existed, and every one of them deleted its own evidence. Note
that each seed's directory is recreated from scratch, so re-running a seed
discards the previous attempt's evidence first — and that with log
compaction deferred (#46), a preserved 180s seed is a few MB per replica. It *is* one: `.github/workflows/nightly-soak.yml` runs it at
07:00 UTC daily over n=3 and n=5, 8 seeds x 180s each, uploading the log as
an artifact on failure. The seed window advances with the run number rather
than repeating, so a green nightly means new fault sequences came back clean
rather than one fixed set being re-tested forever; dispatch the workflow with
`first_seed` to re-run a window that failed. `--keep-going` reports how many seeds failed rather than
stopping at the first. `--replicas 5` is worth a run of its own; `f = 2` is
the first size where two nodes may be faulted at once.

**No real-process scenario runs under `cargo test`.** They are all
`#[ignore]`d and run in [their own CI job](../../.github/workflows/ci.yml),
which is the shape [#56] asks for: a bounded variant on every commit, plus a
documented longer mode.

That is a cost decision, not a confidence one. These scenarios spend nearly
all their wall clock asleep waiting for real timers — the bounded soak alone
is ~27s of a job whose unit tests finish in seconds — so folding them into
the commit gate would roughly double it to exercise a different layer. A
failure here is a real socket or a real `SIGKILL`, and it is more legible in
a job of its own than buried among unit tests.

**A correction to what slice 2 claimed here.** That version said `cargo test
--all` runs test binaries in parallel, and gated four scenarios behind
`#[ignore]` to relieve contention with `queso-net`'s own real-process tests.
Cargo does *not* do that: test binaries run strictly one after another, both
within a package and across `--all` (measured, cargo 1.94). So those
scenarios never overlapped `queso-net`'s, and the `#[ignore]`s cannot have
been what fixed the CI failure that prompted them
(`queso-net`'s `restart_recovery::minority_reboot_recovers_too` timing out
its 10s submit retry loop). That failure remains unexplained; the most
likely remaining cause is simply a slow runner, and issue [#40]'s
bind-then-drop `free_addr` TOCTOU is still a real hazard for anything
allocating nine ephemeral ports per scenario. The `#[ignore]`s stay, on the
honest grounds above rather than the ones originally given.

The `queso-node` binary is located at run time: `QUESO_NODE_BIN` if set,
otherwise alongside the test executable (`<target>/<profile>/queso-node`).
`cargo test -p queso-soak` on its own does not build it — the crate depends
on `queso-net`'s *library* — so build first, or the tests panic with that
instruction.

Scenarios are serialized by a process-wide lock. Cargo runs the tests
*within* one binary in parallel threads, and each spawns three real node
processes, so five at once is fifteen processes competing for CPU — enough
to push cluster boot past a readiness timeout on a loaded runner, which is
exactly the flake that prompted the lock. (This is per-binary; it says
nothing about the rest of the workspace, which cargo already serializes.)

Two more robustness measures, both prompted by real flakes seen while
building this:

- **Boot is retried.** `free_addr` binds an ephemeral port, reads it, and
  drops the listener before the node binds it for real. Under
  whole-workspace parallelism something else can take that port first, and
  the node exits at once. `RealCluster::start` retries the whole boot with
  fresh ports rather than failing the run — blaming Queso for the harness's
  port allocation would be exactly the wrong signal.
- **Node stderr is kept** (`<data-dir>/node-N.err`) and a boot failure
  reports the process's exit status and the tail of that log. Before this,
  a node dying on startup looked identical to a slow one: a silent
  readiness timeout with nothing attached.

## Determinism, honestly

A 9.1 run is reproducible from `(cluster seed, workload seed)`. A run here is
not and cannot be: real thread scheduling, real timers, and real TCP make the
interleaving irreproducible even with identical seeds. The fault *schedule*
is seeded and replayable; what the cluster does under it is not. That is the
price of testing the real implementation, and it is why 9.1's deterministic
harness stays exactly as it is rather than being replaced by this.

## Anti-vacuity

A soak that silently observes nothing looks exactly like a soak that found
no bugs, so every scenario asserts what it actually checked:

- `Observer::comparisons()` — cross-replica checks at a shared `n`
  (measured ~31 on the healthy scenario; the floor is 20).
- acknowledged submissions — a cluster that accepted no writes proves
  nothing about applying them consistently.
- `Turbulence::total_accepted()` — that peer traffic really crossed the
  proxies, so faults were in the path rather than bypassed.
- The partition scenario asserts the isolated replica **fell behind** the
  majority. Verified by mutation: making `Link::cut` a no-op fails that
  assertion (`isolated n=24, frontier 24`) rather than passing quietly.

The soak adds three more, in `SoakReport::problems`, which the binary and
the tests both go through so neither can drift into a laxer standard:

- **Injections are counted per kind, from what the driver did**, and
  compared against what the traversed schedule asked for. Counting from the
  schedule instead would stay green if the injection path broke, and "the
  schedule contained faults" is not the claim that needs proving — "faults
  reached the cluster" is. Verified by mutation: making `reconcile` skip its
  cut loop fails with `scheduled cuts: 5, injected cuts: 0`. Per kind rather
  than in total, because a total stays green if only the crash path still
  works, and each kind drives different node code (reconnect,
  restart-from-disk, timeout behavior).
- **The floors scale with run length**, and the primary one is the
  **chain frontier** rather than the acknowledgement count. See below for
  why that distinction cost a CI run to learn.
- **The liveness budget has a demonstrated falsifier.**
  `a_permanently_dead_replica_is_reported_stuck` kills a replica for good and
  asserts the observer reports it *at the same 5s budget the soaks assert
  with*. Without that, `stalls.is_empty()` is a claim with no shown way to
  fail: a budget in the wrong units, or one widened to stop a flake, passes
  healthy and broken runs alike. Phase 9.1 walked into exactly that and had
  to go back and measure.

## A finding: acknowledgements are not evidence that writes happened

The first version of this floored on acknowledged submissions — a cluster
that accepted no writes proves nothing about applying them consistently.
That reasoning is fine; the metric was wrong, and CI said so:

| | fast machine | same machine, 2 cores | CI runner |
|---|---|---|---|
| chain frontier after 20s | 598 | 557 | 560 |
| comparisons | ~800 | 785 | 823 |
| **acknowledged submissions** | **511** | **423** | **136** |

The frontier and the comparison count land within 7% of each other across
all three. Acknowledgements vary four-fold — and the run with 136 of them
still applied 560 chain entries.

The explanation is that **a submission the client abandons on timeout is
still applied**. The cluster was doing ~28 entries a second everywhere; what
collapsed on the loaded runner was the client hearing back inside the 1.5s
timeout. So the acknowledgement count was measuring round-trip latency, and
a floor of 8/s had quietly encoded one machine's latency as a correctness
assertion. It failed CI while the cluster underneath was entirely healthy.

Two changes came out of it. The frontier now carries the "writes really
happened" claim, because it measures what the cluster did rather than how
fast the client learned of it. And the acknowledgement floor drops to 2/s,
meaning only what it can honestly mean — that the client path worked end to
end at all.

The submit timeout went from 1.5s to 4s at the same time. Detached
submissions make a long timeout nearly free (it bounds how long a doomed
submission holds a runtime task, not how long the driver waits), so the
short value bought nothing and threw away three quarters of the offered
load. `failed` in a report is therefore "the client gave up", almost never
"the cluster refused" — worth knowing, because the number reads like the
second one.

The timeout turned out to be most of it. On the *same* CI runner and the
same schedule, the bounded soak went from 136 acknowledged / 485 failed at
1.5s to **587 / 25** at 4s. So 1.5s was not cutting off doomed submissions;
it was cutting off ones that completed somewhere between 1.5 and 4 seconds.
The runner is slower, but the harness was throwing the work away.

This is also the argument for `--nocapture` in the CI job. The first soak
run passed with these same numbers invisible; the floors had margin nowhere
except on my machine, and nothing in a green log would have said so.

## What's still open

Slices 1-3 of [#56] are done: the node-side `/chain` hook, `RealCluster` plus
the out-of-process nemesis, and this sustained soak with its bounded CI
variant and long mode.

What this still is not: **autonomous**. Antithesis's real leverage is a
system that explores on its own, snapshots interesting states, and replays
into them deterministically. Here a human picks a seed range and reads the
output, and a failure hands back a schedule that reproduces the *turbulence*
but never the interleaving — real thread scheduling and real TCP see to
that. A soak that fails once in forty seeds is a genuine finding and also a
genuinely awkward thing to debug, and nothing here fixes that.

The honest summary of Phase 9 so far: the sim-real gap is *measured* now
rather than assumed, and real sockets, real processes and real restarts are
under continuous randomized fault. Closing the gap properly means
deterministic replay of real executions, which is [#54]'s remaining
territory.

[#40]: https://github.com/evelynmitchell/queso/issues/40
[#54]: https://github.com/evelynmitchell/queso/issues/54
[#56]: https://github.com/evelynmitchell/queso/issues/56
