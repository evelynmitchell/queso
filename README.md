# Queso

**A ground-up, formally-checked implementation of the [QuePaxa](https://bford.info/pub/os/quepaxa/quepaxa.pdf) consensus algorithm in Rust** — built as a "hello world" distributed consensus system: a fault-tolerant, linearizable key-value store over a replicated log, developed incrementally from a deterministic simulation harness all the way to a real TCP cluster you can deploy.

> **Status: educational / research prototype — not production software.** The
> consensus core is simulation-verified and formally model-checked, and the
> system runs over a real network with fsync'd durability and optional TLS.
> But the key-value application is a *demonstration* (fixed 64-bit values, no
> pipelining), durability is correctness-first rather than throughput-tuned,
> and none of it has been battle-tested at scale. It's a faithful, honestly-
> scoped implementation for learning and experimentation, not a datastore to
> put real data in. See [Honest status & limitations](#honest-status--limitations).

---

## What is this, and why?

A **consensus** system lets a set of machines agree on a single, consistent
sequence of decisions *even when some of them crash or the network drops,
delays, or reorders messages*. That agreement is the foundation almost every
other distributed guarantee is built on — a replicated database's
linearizability, a scheduler's exactly-once semantics, a lock service's
correctness. Consensus is what lets a system treat partial failure as a normal,
survivable condition rather than an outage: a correctly-built consensus cluster
of `2f+1` replicas keeps serving through the failure of any `f` of them.

**QuePaxa** (Ford, Jovanović, Sridhar, et al., SOSP 2023) is a recent consensus
protocol whose headline property is *robustness under an adversarial or degraded
leader*. Leader-based protocols like Raft (as used in etcd) are fast in the
common case but stall for an election timeout whenever the leader is slow or
attacked — a real availability weakness. QuePaxa keeps a fast one-round-trip
leader path *and* a randomized, leaderless-tolerant fallback (hedging) that lets
any live majority keep deciding immediately, with no election to wait out.
Cloudflare's [Meerkat](https://blog.cloudflare.com/meerkat-introduction/) is a
production implementation of the same algorithm.

Queso builds that algorithm from first principles, prioritizing **verified
correctness at every layer** over raw performance — the interesting engineering
here is *how you gain confidence that a consensus protocol is actually correct*,
not how many ops/sec a toy KV store can push.

## How QuePaxa compares to Paxos, Raft, and etcd

First, a category note, because the four names aren't the same kind of thing.
**Multi-Paxos**, **Raft**, and **QuePaxa** are *algorithms*. **etcd** is a
*product* — a mature, widely-deployed key-value store that happens to implement
Raft. So "Queso vs. etcd" is really two separate comparisons: an algorithm
comparison (QuePaxa vs. Raft), and a maturity comparison (a research prototype
vs. the datastore behind Kubernetes). Queso wins nothing at all on the second
one, and this section does not pretend otherwise.

|  | Multi-Paxos | Raft | etcd | **QuePaxa** (Queso) |
|---|---|---|---|---|
| **Kind** | Algorithm | Algorithm | Product (Raft) | Algorithm |
| **Leader** | Required for progress | Required for progress | Required for progress | *First among equals* — an optimization, never a requirement |
| **Common-case commit** | 1 round trip | 1 round trip | 1 round trip | 1 round trip (leader fast path, §4.2.5) |
| **Commit without the leader** | Blocked until a new leader is elected | Blocked until a new leader is elected | Blocked until a new leader is elected | ~3 round trips, immediately — no election |
| **Liveness depends on** | Partial synchrony + timeouts | Partial synchrony + timeouts | Partial synchrony + timeouts | Randomization; each round decides with probability ≥ ½ |
| **Failed leader** | View change after a timeout | Election after a randomized timeout | Same, with a default 1000 ms election timeout and randomized backoff to ~2× | Nothing to detect and nothing to elect — the next-ranked proposer's hedge delay elapses and it proceeds |
| **Slow-but-alive leader** | Drags the whole system; too fast a timeout livelocks, too slow a timeout doesn't fire | Same | Same | The hedge schedule proceeds past it; the slow leader isn't in anyone's way |
| **Timeouts to tune** | Yes, and there is no good WAN setting | Yes | Yes | None for liveness. The hedge delay δ is a *performance* knob: set it wrong and you get extra messages, not a stall |
| **Messages per decision** | `O(n)` | `O(n)` | `O(n)` | `O(n)` on the fast path, `O(n²)` when every proposer is active |
| **Safety under full asynchrony** | Yes | Yes | Yes | Yes — safety never rests on the randomization |
| **Maturity** | Decades of deployments | The most-implemented consensus algorithm | Production-hardened, enormous operational track record | SOSP 2023; one production implementation (Cloudflare Meerkat). **Queso itself is a prototype** |

### Why you would choose QuePaxa

There is essentially one reason, and it is worth stating narrowly rather than
broadly: **QuePaxa keeps making progress when the leader is degraded, and it
does so without you having chosen a timeout in advance.**

Leader-based protocols pay for their simplicity with what the QuePaxa paper
calls the *tyranny of timeouts*. Liveness is gated on a number a human picked:
too small and replicas trip over each other electing; too large and a dead
leader stalls writes for that long. Worse, the failure mode timeouts handle
*worst* is the common one — a leader that is slow but not slow enough to trip
the timeout holds the entire cluster at its own speed indefinitely. And an
adversary can weaponize this: DoS whichever single replica is currently leader
(identifiable from traffic patterns), and progress halts while only one machine
is ever under attack. Cloudflare cites multiple real incidents from unavailable
leaders in Raft systems — which is why they built Meerkat on QuePaxa.

QuePaxa's answer is that the leader is only ever an optimization. It gets the
reserved top priority `H` and a one-round-trip fast path when it's healthy; when
it isn't, the slot falls back to randomized leaderless rounds that terminate with
probability 1 without anyone detecting anything. Instead of a timeout that
*retroactively* declares the leader dead, a hedge schedule *proactively* has the
next-ranked proposer step in at delay δ, 2δ, … — and because QuePaxa proposers
cooperate rather than destructively interfere the way Paxos ballots do, an
unnecessary hedge costs messages, not a view change.

What that buys, measured in this repository rather than quoted from the paper:

> With the fast-path leader fully network-isolated mid-run, the surviving
> majority's **longest gap between two consecutive completed writes was 418 ms**
> — no election, no stall. `crates/compare/tests/leader_dos.rs` asserts that gap
> stays under 2 s on *every* run. For scale, etcd's default election timeout
> alone is 1000 ms before randomized backoff.

See [`docs/compare-etcd.md`](docs/compare-etcd.md) for the methodology, and note
its honesty caveat: the Queso-side numbers there are real, but **the etcd-side
numbers are placeholders** — this project's sandbox cannot reach or run etcd, so
that half is a runbook for you to execute, not a result we're claiming.

### Why you would not

- **The liveness assumption is different, not strictly weaker.** Raft trades
  timeouts for liveness under partial synchrony. QuePaxa trades randomization for
  liveness under asynchrony — but only against a **content-oblivious** adversary
  that can delay and reorder packets without reading them (assumption A3 in
  [`docs/02-properties.md`](docs/02-properties.md); TLS is what makes it hold in
  practice). An adversary who can read proposal contents and adaptively target
  the highest priority is outside the model. Safety is unconditional either way;
  it's the termination bound that rests on A3.
- **Off the fast path you pay for it.** A non-leader proposer takes roughly 3
  round trips instead of 1, and message cost goes to `O(n²)` when everyone is
  active. QuePaxa is buying robustness with messages.
- **Raft is dramatically easier to reason about**, and that is a real
  engineering property, not a consolation prize. It was designed for
  understandability and has the implementations, tooling, and operator intuition
  to show for it.
- **etcd is a product and Queso is not.** Watches, leases, MVCC, auth, backup and
  restore, reconfiguration, and years of production traffic — Queso has a
  demonstration KV store with 64-bit values, no pipelining, no log compaction, and
  no membership change. See [Honest status & limitations](#honest-status--limitations).
- **Leader-based systems can serve cheaper linearizable reads.** etcd's
  ReadIndex/lease reads avoid a full consensus round; Queso's reads go through
  the log so that a lagging replica is forced to catch up first.

### The short version

- **Choose Raft/etcd** if you want a consensus system today, your network is a
  reasonably well-behaved data center, and you'd rather tune an election timeout
  than reason about a probabilistic termination bound. This is the right default
  and it isn't close.
- **Choose QuePaxa** if leader unavailability is your actual operational pain —
  a WAN with no good timeout setting, adversarial conditions, or an incident
  history of leaders that were slow rather than dead — and you can accept a
  younger protocol with one production implementation.
- **Choose Queso** if you want to *read* a QuePaxa implementation whose
  correctness argument is written down and mechanically checked. It's a reference
  implementation and a learning vehicle, not a datastore to put real data in.

## How it's built: verified core, real-I/O shell

The central design idea is a hard seam between a **deterministic, verified core**
and a **real-I/O shell**, so the exact same consensus code that's checked under
simulation is what runs over a real network:

- The core crates (`sim`, `consensus`, `smr`) are **deterministic by
  construction** — no wall-clock reads, no `HashMap` iteration order, no ambient
  OS randomness. This is enforced mechanically: `clippy.toml` bans
  `Instant::now`, `SystemTime::now`, `thread::spawn`, and `thread_rng`/`random`
  workspace-wide at `deny`. A single seed reproduces an entire run bit-for-bit
  (deterministic simulation testing, "DST").
- Consensus logic is written against an abstract `Ctx` trait
  (`self_id`/`now`/`send`/`schedule_timer`/`rng`). The simulator implements it
  with a virtual clock and an in-memory network; the real node
  (`crates/net`) implements the *same trait* with tokio, TCP, and real time.
  **Not one line of consensus/SMR logic changes** between the two — the real
  transport is a different driver for the identical state machine.

That seam is what makes the correctness work transferable: bugs are hunted in
the fast, reproducible, adversarial simulator, and the deployable binary inherits
the fix for free.

## Repository layout

```
crates/
  sim/         Deterministic discrete-event simulation kernel: virtual clock,
               seeded PRNG, in-memory network, adversary scheduling, and a
               fault-injection API (crash/restart/partition/slow-node).
  consensus/   The QuePaxa protocol itself — abstract single-slot core
               (Algorithm 1), the concrete 4-phase ISR protocol with threshold
               logical clocks (Algorithm 4), the leader fast path, and hedging.
  smr/         State-machine replication: a multi-slot log, a linearizable
               key-value store (reads-through-log), idempotent client sessions,
               durable-vs-volatile state split, and the multi-armed-bandit
               auto-tuner.
  net/         The real-I/O boundary: tokio+TCP transport, the queso-node
               binary, fsync'd on-disk durability, an in-transport fault
               injector, optional app-level TLS, and status/metrics endpoints.
  compare/     A benchmark harness comparing Queso vs. an alternative
               (etcd/Raft), including the leader-DoS behavior experiment.
spec/          TLA+ models of the abstract and concrete consensus cores, with
               TLC configs that model-check the safety properties.
docs/          Design docs: backgrounder, property model, testing plan,
               deployment runbook, comparison methodology, and status.
deploy/        Dockerfile + fly.toml for deploying a multi-region cluster.
```

## Tech stack

- **Rust** (2021 edition), a Cargo workspace of five crates.
- **tokio** for the real async transport; length-delimited framing over TCP with
  `bincode`/`serde` on the wire.
- **rustls** (pure-Rust, no OpenSSL) for optional mutual-TLS between replicas.
- **TLA+ / TLC** for exhaustive formal model-checking of the consensus core.
- **hdrhistogram** for latency measurement in the load generator.
- Deterministic-simulation testing as the primary correctness tool, backed by
  ~250 tests spanning unit, property, linearizability, fault-injection, and
  real-process (spawn-and-`SIGKILL`) integration tests.

## Build & test

Requires a recent stable Rust toolchain (`rustup`, `cargo`).

```sh
# Build everything.
cargo build --workspace

# Run the full test suite (simulation, property, linearizability,
# fault-injection, and real-cluster integration tests).
cargo test --workspace

# The determinism gate the verified core is held to (also run in CI):
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
```

The core crates' determinism means their tests are **reproducible**: a failing
seed replays the exact same schedule every time, which is what makes adversarial
simulation debugging tractable.

### Formal verification

The `spec/` directory contains TLA+ models of the consensus core, checked with
TLC. Both the abstract algorithm (Algorithm 1) and the concrete protocol
(Algorithm 4 / ISR) are model-checked for the core safety properties —
Agreement, Validity, Integrity/DecideOnce — with the key lemmas from the paper's
Appendix B/C proofs `Assert`-checked directly against every reachable state, not
merely implied by the top-level invariants. See [`spec/README.md`](spec/README.md)
for the configurations, state counts, and TLC output.

## Usage

### Run a local 3-node cluster

Build the node binary, then start three replicas (each needs the full `--peer`
membership list including itself):

```sh
cargo build --release -p queso-net --bin queso-node

# terminal 1
./target/release/queso-node --id 0 --seed 1 \
  --listen 127.0.0.1:7000 --client-listen 127.0.0.1:8000 \
  --peer 0=127.0.0.1:7000 --peer 1=127.0.0.1:7001 --peer 2=127.0.0.1:7002 \
  --leader 0 --data-dir ./data/0

# terminal 2 (--id 1, ports 7001/8001, --data-dir ./data/1) ...
# terminal 3 (--id 2, ports 7002/8002, --data-dir ./data/2) ...
```

Omit `--leader` on all three to run purely leaderless. Each replica persists its
durable state under `--data-dir` and recovers it on restart, so the cluster
survives process crashes without losing acknowledged writes. See
[`crates/net/README.md`](crates/net/README.md) for the full flag reference.

### Drive a workload and measure it

`queso-bench` is an open/closed-loop load generator reporting throughput and
p50/p90/p99/max latency:

```sh
cargo build --release -p queso-net --bin queso-bench

./target/release/queso-bench \
  --addr 127.0.0.1:8000 --addr 127.0.0.1:8001 --addr 127.0.0.1:8002 \
  --concurrency 64 --read-frac 0.5 --keys 1000 --duration-secs 8
# --output json|csv for machine-readable results.
```

### Compare against etcd/Raft

`queso-compare` (crate `compare`) drives the *same* workload against Queso and an
alternative through one harness, and includes the headline experiment: with the
fast-path leader isolated mid-run, Queso's majority keeps deciding (no election
stall) where a single-leader protocol would pause. See
[`docs/compare-etcd.md`](docs/compare-etcd.md) for methodology and results, and
[How QuePaxa compares to Paxos, Raft, and etcd](#how-quepaxa-compares-to-paxos-raft-and-etcd)
for what the experiment is actually arguing.

```sh
# The headline leader-DoS experiment -- self-contained, no external etcd needed.
cargo test -p queso-compare --test leader_dos -- --nocapture
```

## Deployment

`deploy/` contains a multi-stage `Dockerfile` (pure-glibc, no OpenSSL) and
`fly.toml` files for a multi-region deployment on [fly.io](https://fly.io),
using its private `.internal` DNS for peer discovery and a persistent volume for
each replica's durable state. Optional features for a real deployment:

- **Durability**: fsync'd, crash-consistent on-disk persistence with a
  write-before-reply guarantee, group-commit batching, and an on-disk
  schema-version header.
- **TLS**: opt-in, off-by-default app-level TLS — mutual TLS between replicas and
  server-authenticated TLS for clients (`--tls-cert/--tls-key/--tls-ca`). fly's
  `.internal` network is already WireGuard-encrypted, so TLS is for non-fly /
  defense-in-depth deployments.
- **Operability**: an opt-in status/metrics HTTP server (`--status-listen`)
  exposing `/health`, `/ready`, and `/metrics` for load-balancer probes and
  monitoring.

The step-by-step runbook is [`docs/deploy-flyio.md`](docs/deploy-flyio.md).
(Actual multi-region deploys and WAN numbers require your own fly.io account.)

## Correctness model

Confidence in a consensus system comes from stating precisely what must be true
and then checking it. Queso's [property model](docs/02-properties.md) enumerates:

- **Safety/liveness invariants that must hold** (P1–P17): Agreement, Validity,
  Integrity, prefix consistency, total order, gap-free application,
  linearizability, durability, write-before-reply, and more.
- **Anti-properties that must never occur** (N1–N6): divergence, lost
  acknowledged writes, stale reads from a minority, etc.
- **Desirable properties** (D1–D11) and the **assumptions** the guarantees rest
  on (A1–A7) — notably A3, the content-oblivious-adversary / private-channel
  assumption that QuePaxa's randomized liveness depends on.

These are checked by, in increasing cost: type-level determinism lints,
unit/property tests, a Wing-Gong linearizability checker over recorded histories,
adversarial deterministic-simulation runs (content-oblivious and
content-aware adversaries, plus crash/partition/slow-node fault injection),
real-process restart-recovery tests (spawn real nodes, `SIGKILL` a majority,
assert no write is lost), and TLA+/TLC formal model-checking. The full plan is in
[`docs/03-testing-plan.md`](docs/03-testing-plan.md).

## Roadmap

Queso was built in phases, each a milestone with its own reviewed pull requests.
Phases 0–9 are complete. Phase 9's last slice packages Queso as an
Antithesis test template; running it needs an account, so that first run —
and with it deterministic **replay** of a real execution — is what remains.

| Phase | Milestone | What it delivered |
|------:|:---------:|-------------------|
| 0 | M0 | Deterministic simulation harness (virtual clock, seeded PRNG, in-memory network, adversaries, fault injection) |
| 1 | M1 | Abstract single-slot consensus (Algorithm 1) — TLC-verified |
| 2 | M1 | Concrete protocol: ISR, 4-phase, threshold logical clocks (Algorithm 4) — TLC-verified |
| 3 | M2 | Leader fast path (§4.2.5, one-round-trip common case) |
| 4 | M3 | Multi-slot log + **linearizable key-value store** (the "hello world" milestone) + durability/restart recovery |
| 5 | M4 | Hedging (delay schedule; timeout-free recovery) |
| 6 | M5 | Auto-tuning (multi-armed-bandit leader selection) |
| 7 | M6 | Real TCP transport, `queso-node`, workload generator, connection fuzzing, fly.io deployment, and comparison vs. etcd |
| 8 | M7 | Operability: durability hardening (group-commit, async fsync, versioned snapshots), TLS, status/metrics endpoints, `queso-admin` operator CLI (log compaction deliberately deferred) |
| 9 | — | Antithesis-style conformance testing: a Chain-of-Blocks workload and divergence/liveness observers, a `GET /chain` checkpoint hook on the node, a seeded randomized fault soak driving real `queso-node` processes under socket-level turbulence, and an Antithesis test template (`crates/conformance`, `crates/soak`, `crates/antithesis`, `antithesis/`) |

Explicit **non-goals**: Byzantine fault tolerance, being a general-purpose
database, side-channel resistance, and dynamic reconfiguration/membership change.

## Honest status & limitations

In keeping with the project's design principle of not overclaiming:

- The **consensus/SMR core is the trustworthy part** — simulation-verified and
  formally model-checked. The **real-transport shell** (`crates/net`) is far
  less battle-tested than the sim core, though it now has fsync'd durability
  verified by real-process reboot tests.
- The **key-value store is a demonstration app**: values are fixed 64-bit
  integers, there's no command pipelining (one decision in flight per replica),
  and durability does a whole-state snapshot fsync per group-commit batch rather
  than an incremental WAL. Benchmark numbers reflect *protocol behavior*, not a
  tuned datastore.
- The **auto-tuner (M5) is simulation-only** — not yet wired to the real
  cross-process transport.
- **No log compaction yet**, so on-disk state grows on a very long run.
- This has **never handled real production traffic**. Treat it as a reference
  implementation and a learning vehicle.

## Documentation

Everything below is also published as a website —
<https://evelynmitchell.github.io/queso/> — with these documents rendered
as a book and the full rustdoc API reference beside them. This README
stays canonical — the site links back rather than restating it — and the
site is rebuilt from `docs/` and rustdoc on every push to `main` (see
`.github/workflows/pages.yml`).

- [`docs/tutorial.md`](docs/tutorial.md) — **start here**: build, boot a three-node cluster, write to it, `SIGKILL` a replica and watch the cluster survive, then kill a majority and watch writes correctly stop. Every step exercised by CI (`crates/net/tests/tutorial.rs`).
- [`docs/00-project-outline.md`](docs/00-project-outline.md) — master outline: goals, principles, phased roadmap, milestones.
- [`docs/01-backgrounder.md`](docs/01-backgrounder.md) — white-paper backgrounder on the consensus problem space, QuePaxa, and Meerkat, with references.
- [`docs/02-properties.md`](docs/02-properties.md) — the full property model (invariants, anti-properties, assumptions).
- [`docs/03-testing-plan.md`](docs/03-testing-plan.md) — the testing strategy (harness, property tests, DST, formal verification, benchmarks).
- [`docs/conformance-matrix.md`](docs/conformance-matrix.md) — every property (P1–P17, N1–N6, D1–D11) mapped to the test or model that verifies it *and* to the class of evidence that artifact provides — model-checked, enumerated, tested with power measured, tested with power unmeasured, argued, or assumed.
- [`docs/deploy-flyio.md`](docs/deploy-flyio.md) — fly.io deployment runbook.
- [`docs/compare-etcd.md`](docs/compare-etcd.md) — Queso-vs-etcd comparison methodology and results.
- [`docs/STATUS.md`](docs/STATUS.md) — current status and gap analysis.
- [`docs/investigating-with-logs.md`](docs/investigating-with-logs.md) — how to investigate an unexplained failure: what evidence to keep, when to add logging, what flags are for, and when to stop sampling the system and enumerate the state instead. Written after a reported safety violation went five occurrences without being settled.
- [`docs/what-each-test-establishes.md`](docs/what-each-test-establishes.md) — a decision table for the test surface: what each instrument's green actually licenses you to say, which ones have measured detection power, and the three incompatible things this repo calls a "seed".
- [`spec/README.md`](spec/README.md) — the TLA+ formal models and their TLC results.
- Each crate has its own `README.md` / module docs with deeper design notes.

## How this project is developed

Queso is built by AI agents under human direction, with a deliberately rigorous
workflow: work is decomposed into GitHub issues; each change lands as its own
pull request; and **every PR is critically reviewed in a fresh, independent
environment** before it's marked ready — with reviewers instructed to try to
*break* the change (e.g. neutering a fault injector to prove a test isn't
vacuous, or attacking a hand-rolled parser). Humans do the merging. That process
has repeatedly caught real defects — a lost-acknowledged-write durability bug,
vacuous fault-injection tests, and more — that green unit tests alone missed.

## References

- QuePaxa paper (SOSP 2023): <https://bford.info/pub/os/quepaxa/quepaxa.pdf>
- Cloudflare Meerkat: <https://blog.cloudflare.com/meerkat-introduction/>

## License

Dual-licensed under either of [MIT](LICENSE-MIT) or
[Apache-2.0](LICENSE-APACHE), at your option. Unless you explicitly state
otherwise, any contribution intentionally submitted for inclusion in the work
by you shall be dual-licensed as above, without any additional terms or
conditions.
