# queso-net

Phase 7: a real-TCP transport and node binary that drives the sim-verified
`queso-consensus`/`queso-smr` core — completely unchanged — over a real
tokio event loop instead of `queso_sim::kernel::Kernel`'s deterministic
in-memory harness. Phase 7.2 adds a real client library (`client::Client`,
with a replica-address pool and retry-to-another-replica) and a
`queso-bench` load generator with throughput/latency metrics on top of that
transport. Phase 7.4 (issue #34) adds an in-transport fault injector
(`nemesis::Nemesis`: latency/jitter, frame drop, connection reset, network
partition) and adversarial perf tests that run `queso-bench`-style load
through it — see "Phase 7.4: nemesis fault injection + adversarial perf"
below. See the crate's `src/lib.rs` docs for the architecture and
`docs/STATUS.md` §4a / issues #30/#36 for how this fits into the project's
phases. Phase 7.3 (issue #33) adds deployment artifacts for running a real
cluster on fly.io -- see `docs/deploy-flyio.md` for the runbook and
`deploy/Dockerfile`/`deploy/fly.toml` for the config; that phase is also
where `--peer`'s `host:port` addresses gained hostname resolution (see
`resolve_peer_addr` in `src/transport.rs`) so peers can dial each other by
DNS name, not just literal IP.

This crate is the deliberate real-I/O boundary: real sockets, real
wall-clock time, real OS entropy. It is exempt from the workspace's
determinism lints (see `src/lib.rs`'s `#![allow(clippy::disallowed_methods)]`)
— those stay enforced at `deny` on `queso-sim`/`queso-consensus`/`queso-smr`.

## Status: prototype transport with real, fsync'd durability — read before trusting it

> **The consensus/SMR core (`queso-consensus`/`queso-smr`) is
> simulation-verified for Agreement, Validity, prefix consistency,
> linearizability, and crash-recovery (P1–P12), under adversarial
> fault-injection in `queso-sim`.** `queso-net` drives that exact,
> unmodified core over real TCP. As of this durability fix (issue #36),
> each replica also persists its durable state
> (`queso_smr::replica::Durable`: per-slot recorder ISR state, the log
> frontier, the applied log, the KV state) to fsync'd, crash-consistent
> on-disk storage, write-before-reply, and reloads it on boot before
> rejoining as a learner — so a real process `SIGKILL` + restart of a
> **majority** of replicas no longer loses an acknowledged write or
> diverges (see `tests/restart_recovery.rs`, which reproduces the exact
> scenario the audit found and asserts it no longer happens, against real
> spawned-and-killed `queso-node` processes). It is still **not** a
> production-grade durability story: see "Honest limits" below before
> deploying this for real. No TLS (A3's content-oblivious-adversary
> assumption is not realized over the wire yet), no reconfiguration, no log
> compaction. (Phase 7.2 does add a client library with
> retry-to-another-replica — see below.)

**Honest limits of the current durability implementation (Phase 7
hardening, not Phase 8's full operability story):**

- **Per-RPC fsync, not group commit.** Every inbound message that can
  mutate durable state triggers its own synchronous `fsync(2)` on the
  replica's single event loop thread — correct, but each `fsync` is a
  real disk round trip, so single-replica throughput is bounded by disk
  fsync latency (typically low hundreds to low thousands of ops/sec on
  common cloud block storage, much higher on NVMe with a battery-backed
  write cache). Group-commit/batching multiple decisions into one `fsync`
  is a natural follow-up, deliberately not built here — see
  `src/persist.rs`'s module docs.
- **Whole-state snapshot, not an append-only log.** Each persist rewrites
  the replica's *entire* durable state (every slot's recorder, the whole
  applied log), not just the delta — `O(log length)` per write. Fine for
  short-lived clusters/tests; a long-running deployment needs an
  incremental WAL plus periodic snapshot/compaction (Phase 8 territory).
- **fsync is trusted, not independently verified.** Ordinary POSIX
  `fsync`/atomic-rename semantics are assumed; this does not defend
  against a lying disk/filesystem, nor does it `fsync` the data
  directory's own parent.
- **Single-copy durability, not replicated backup.** Durable state lives
  on one disk per replica. Losing that disk (not just the process) loses
  that replica's durable state — recoverable only via the ordinary
  catch-up-from-a-live-majority path, not via any backup/replication of
  the on-disk file itself.
- **No reconfiguration, no compaction, no TLS** — unchanged from before
  this fix; see "Scope" below.

See `src/persist.rs`'s module docs for the exact on-disk format and the
write-before-reply ordering, and `src/driver.rs`'s module docs for how
boot-time reload/`on_restart` are wired into the event loop.

## Running a local 3-node cluster by hand

Build the binary once:

```sh
cargo build -p queso-net --bin queso-node
```

Then, in three separate terminals, boot each replica. Each one needs the
full `--peer id=host:port` list (including its own entry) so it knows
every replica's peer-listen address, plus its own `--id`, `--listen`
(peer port), `--client-listen` (client port), a `--seed`, and a
`--data-dir` (where its durable state is persisted — see "Status" above;
defaults to `./data`, shared safely across replicas since files are keyed
by `--id`):

```sh
# terminal 1
./target/debug/queso-node \
  --id 0 --seed 1 \
  --listen 127.0.0.1:7000 --client-listen 127.0.0.1:8000 \
  --peer 0=127.0.0.1:7000 --peer 1=127.0.0.1:7001 --peer 2=127.0.0.1:7002 \
  --leader 0

# terminal 2
./target/debug/queso-node \
  --id 1 --seed 2 \
  --listen 127.0.0.1:7001 --client-listen 127.0.0.1:8001 \
  --peer 0=127.0.0.1:7000 --peer 1=127.0.0.1:7001 --peer 2=127.0.0.1:7002 \
  --leader 0

# terminal 3
./target/debug/queso-node \
  --id 2 --seed 3 \
  --listen 127.0.0.1:7002 --client-listen 127.0.0.1:8002 \
  --peer 0=127.0.0.1:7000 --peer 1=127.0.0.1:7001 --peer 2=127.0.0.1:7002 \
  --leader 0
```

Omit `--leader` entirely on all three to run purely leaderless (still
tolerant of a minority failure, just without the §4.2.5 one-round-trip
fast path).

Once all three are up (each logs `listening for peers` / `listening for
clients`), submit load from a fourth terminal with `queso-bench` (see
below), or drive `queso_net::client::Client`/`queso_net::client::submit`
from a scratch program, or just run this crate's own integration tests
(below), which exercise the exact same path end-to-end automatically.

## Automated end-to-end tests

```sh
cargo test -p queso-net --test cluster
cargo test -p queso-net --test bench
cargo test -p queso-net --test restart_recovery
cargo test -p queso-net --test nemesis
```

`tests/cluster.rs` boots a 3-node cluster entirely in-process (each
replica on its own OS thread with its own tokio runtime, talking to the
others over real `127.0.0.1` TCP sockets — not `queso_sim::kernel::Kernel`),
waits for it to form, submits a `Put(42, 7)` to one replica and then reads
it back with a `Get(42)` from a *different* replica, and asserts the value
round-trips. A second test does the same in purely leaderless mode; a third
crashes a replica outright and checks the cluster still makes progress at
its fault-tolerance boundary (2-of-3 live).

`tests/bench.rs` (Phase 7.2's acceptance test) boots the same kind of
3-node cluster and drives it with `queso_net::client::Client` and
`queso_net::metrics::Recorder` exactly the way `queso-bench` does —
concurrent workers, a read/write mix, latency samples funneled through a
collector task — and asserts the run produces every expected sample with
zero errors, positive throughput, and a monotonic (p50 ≤ p90 ≤ p99 ≤ max)
latency histogram for reads, writes, and overall.

`tests/nemesis.rs` is issue #34's (Phase 7.4) acceptance test: fault
injection and adversarial perf against real 3-node clusters — see "Phase
7.4: nemesis fault injection + adversarial perf" above for the full
breakdown of its three scenarios.

`tests/restart_recovery.rs` is issue #36's regression test: it spawns the
actual `queso-node` binary as independent OS processes, `SIGKILL`s a
**majority** of them after an acknowledged `Put`, restarts them against the
same `--data-dir`, and asserts every replica (including the two that just
rebooted) still agrees on the write — the exact scenario the audit found
broken. A second test does the easier minority-reboot case as a contrast.

## `queso-bench`: load generator + throughput/latency metrics (Phase 7.2)

Build it once (alongside `queso-node`):

```sh
cargo build --release -p queso-net --bin queso-node --bin queso-bench
```

Point it at every replica's client-port address — listing more than one
lets the client library's retry-to-another-replica policy actually retry
somewhere if the one it happens to try first is down or mid-election:

```sh
# Closed-loop: 64 worker sessions, each looping "submit, wait, submit the
# next one" as fast as the cluster answers, for 8 seconds.
./target/release/queso-bench \
  --addr 127.0.0.1:8000 --addr 127.0.0.1:8001 --addr 127.0.0.1:8002 \
  --concurrency 64 --read-frac 0.5 --keys 1000 --duration-secs 8

# Open-loop: a fixed 500 ops/sec schedule (queueing under overload shows
# up as latency, not a throughput drop), capped at 32 operations in flight.
./target/release/queso-bench \
  --addr 127.0.0.1:8000 --addr 127.0.0.1:8001 --addr 127.0.0.1:8002 \
  --rate 500 --concurrency 32 --read-frac 0.3 --keys 500 --duration-secs 6

# Machine-readable output for comparing runs (Phase 7.5):
./target/release/queso-bench --addr 127.0.0.1:8000 --ops 2000 --output json
./target/release/queso-bench --addr 127.0.0.1:8000 --ops 2000 --output csv
```

Every flag is documented in `queso-bench --help`
(`crates/net/src/bin/queso-bench.rs`'s `Args`): target addresses, `--rate`
(open-loop) and/or `--concurrency` (closed-loop worker count / open-loop
in-flight cap), `--read-frac` (read/write mix), `--keys` (key-space size),
`--duration-secs`/`--ops` (run length, at least one required), and
`--output text|json|csv`. `--value-size` is accepted for config-surface
parity with other load generators but has no effect: `queso_smr::Value` is
a fixed 8-byte `i64` in the current schema, not a variable-length blob.

The default text summary reports throughput plus a p50/p90/p99/max latency
histogram (via `hdrhistogram`), broken out for reads, writes, and overall:

```
queso-bench: 15670 ops in 8.04s = 1950.2 ops/sec (0 errors)
  overall  count=15670    errors=0      mean= 32734.2us  p50=  30703us  p90=  62111us  p99=  80831us  max=  94591us
  reads    count=7801     errors=0      mean= 32428.6us  p50=  30543us  p90=  61279us  p99=  79743us  max=  94591us
  writes   count=7869     errors=0      mean= 33037.2us  p50=  30863us  p90=  62879us  p99=  81279us  max=  93887us
```

## Phase 7.4: nemesis fault injection + adversarial perf (issue #34)

`src/nemesis.rs`'s `Nemesis` is an **in-transport** fault injector for this
crate's replica-to-replica connections — the real-network analogue of
`queso_sim::fault`'s scripted crash/partition/slow-node model, but wired
into `transport::spawn_peer_dialer` so it runs against a real, in-process,
real-TCP cluster instead of the deterministic sim kernel. It is consulted
once per outbound peer frame, immediately before the frame would be
written to the socket, and supports:

- **Latency/jitter**: a fixed delay plus uniform random jitter added before
  every frame.
- **Frame drop**: silently discard a frame instead of sending it (the same
  "message just doesn't arrive" fault `queso_sim::fault` already models —
  safe by the same argument: `queso_consensus::proposer`'s unbounded
  retry-with-backoff re-sends whatever a live proposer still needs).
- **Connection reset**: force the peer connection to close and let the
  existing dialer reconnect loop (`transport::spawn_peer_dialer`'s
  `RECONNECT_DELAY`) take back over, modelling a mid-stream TCP RST.
- **Network partition (majority/minority splits + heal)**: split the
  cluster into two groups of `NodeId`s that cannot exchange peer frames
  with each other (`Nemesis::partition`/`Nemesis::heal`); `Nemesis::isolate`
  is sugar for cutting off one node against the rest — the leader-targeting
  scenario below.

It is **off by default**: `NodeConfig::nemesis` is `Option<Arc<Nemesis>>`,
`None` everywhere except a test/bench harness that explicitly builds one
(`queso-node`'s CLI never does), and every hook this module adds is a
strict no-op when it's `None` — an ordinary `queso-node` run is unaffected.
Its own randomness is seeded (`FaultPlan::seed`) so a fault plan's *rate/
shape* is reproducible run to run (see `src/nemesis.rs`'s module docs for
the exact determinism caveat — real tokio tasks racing a shared RNG means
this reproduces the sequence of fault decisions, not which message each
one lands on).

Why in-transport rather than an external proxy like
[toxiproxy](https://github.com/Shopify/toxiproxy): toxiproxy is a
legitimate way to fuzz a *real deployment* (Phase 7.3's fly.io territory),
but it needs an out-of-process component wired into the network topology —
awkward to stand up deterministically inside `cargo test`/CI. `Nemesis`
instead keeps the whole adversarial story self-contained in this crate,
runnable with nothing but `cargo test`. The trade-off is scope: it can only
fault traffic this crate's own transport originates, and it is a
**message-level** partition/drop model (the underlying TCP connection is
left alone; only application frames stop crossing it) rather than a
socket-level one — see `src/nemesis.rs`'s docs for the full "what it
deliberately does not cover" list.

### Tests (`tests/nemesis.rs`)

```sh
cargo test -p queso-net --test nemesis
```

Three `#[tokio::test(flavor = "multi_thread")]` scenarios against real
3-node, real-TCP clusters:

- **`partition_then_heal_preserves_acknowledged_write_and_minority_stalls`**
  — the safety test. A write is acknowledged, *then* the cluster is split
  into a 1-node minority and a 2-node majority. The live majority keeps
  deciding new operations throughout the partition; the isolated minority
  replica is asserted to never answer *anything* on its own (submitting to
  it directly must not produce a successful `Outcome` within a bounded
  deadline — a 1-of-3 replica can never reach the quorum its own new
  attempt needs, so the only safe thing it can do is stall, not fabricate
  or serve a stale value). After `Nemesis::heal`, both the pre-partition
  and during-partition writes are read back correctly *from the
  previously-isolated replica* — nothing acknowledged before/during the
  partition was lost, and the healed replica agrees with the majority
  rather than diverging.
- **`isolating_the_leader_lets_the_majority_keep_deciding`** — the
  leader-targeting / QuePaxa-vs-Raft scenario. `Nemesis::isolate` DoSes the
  fixed fast-path leader completely away from its peers — the node a
  Raft-style single-leader protocol would need to re-elect around, forcing
  a stall until an election timeout elapses. The test drives load at only
  the two non-leader replicas and asserts every operation still completes
  (and reads back correctly) well inside a generous deadline, no election
  required — demonstrating Meerkat/QuePaxa's leaderless-tolerant hedging
  (any live majority of recorders can still decide) against exactly the
  fault that would stall a single-leader protocol.
- **`adversarial_load_stays_safe_and_shows_measurable_degradation`** — the
  adversarial perf harness the issue asks for: the same
  `queso_net::client::Client` + `queso_net::metrics::Recorder` machinery
  `queso-bench`/`tests/bench.rs` use, run once against a clean baseline
  cluster and once against an independent cluster under continuous
  latency/jitter/frame-drop/connection-reset fuzzing (deliberately *no*
  partition, so the whole cluster stays majority-connected and every
  operation should eventually land). Asserts: most writes still land
  despite continuous faults (a generous bound, not an exact throughput
  number); the degraded run's mean latency clears an absolute floor
  structurally implied by the configured fault plan (deliberately *not* a
  ratio against the separately-run baseline cluster — see "Flakiness risk"
  below for why); and, the actual safety property this test exists for,
  every write the cluster *acknowledged* under fault injection is read
  back afterward with its exact value — no lost, stale, or divergent
  acknowledged write, even with the link actively dropping frames,
  delaying them, and resetting connections throughout the run.

Run individually with `--nocapture` to see both runs' full
throughput/latency summaries (`Summary::to_text`) printed for comparison.

### What Phase 7.4 does *not* cover (honest limits)

- **Client-facing connections are untouched.** `Nemesis` only wraps
  replica-to-replica peer traffic, not `crate::client`'s connections — a
  partitioned/DoS'd replica's client port itself is not blocked. This
  models "this replica cannot make progress" faithfully (the point of the
  scenarios above) without needing to simulate client-side unreachability
  too; a real caller routes around it via `Client`'s own
  retry-to-another-replica, exactly as the leader-isolation test exercises.
- **Message-level, not socket-level.** A partitioned/faulted link's
  underlying TCP connection is left alone; this cannot exercise "the OS/
  firewall itself refuses or black-holes the connection" as a distinct
  failure mode from "the connection is up but application frames don't get
  through".
- **No packet reordering/corruption/duplication** — only delay, drop,
  reset, and partition are modelled (the fault vocabulary
  `queso_sim::fault` already covers on the sim side; corruption in
  particular is out of scope for both since `wire::decode`'s bincode
  framing would just reject a corrupted frame rather than exercise any
  interesting new behavior).
- **No `queso-bench`-CLI-level `--nemesis-plan` flag yet** — the fault
  plans in `tests/nemesis.rs` are constructed and driven directly in Rust
  (`Nemesis::partition`/`heal`/`isolate`/`set_*`, or the scripted
  `nemesis::run_plan` helper for a pre-timed scenario); wiring an
  equivalent CLI surface onto `queso-bench` itself is a natural follow-up,
  not built here.
- **Flakiness risk**: the adversarial-perf test's degradation assertion is
  deliberately an absolute latency floor rather than a baseline ratio (see
  the test's own comment) specifically because a ratio against a
  separately-run baseline cluster proved flaky under heavy concurrent
  `cargo test` load in development (the baseline cluster's own latency can
  spike from unrelated CPU contention). The remaining tests use generous,
  multi-second deadlines rather than tight timing windows for the same
  reason.

## Scope (Phase 7.1 + 7.2 + 7.4 — see the crate docs)

Transport + node binary + client-facing wire protocol (Phase 7.1), a
client library with a replica-address pool and retry-to-another-replica
plus the `queso-bench` load generator with throughput/latency metrics
(Phase 7.2), and an in-transport fault injector plus adversarial perf tests
(Phase 7.4, above). Explicitly **not** in this crate yet:

- session/seq management beyond A6's one-in-flight-per-`ClientId` minimum,
  connection pooling/reuse, or pipelining in the client library;
- comparisons against alternative systems — Phase 7.5 (this crate's
  `--output json`/`csv` exist so those runs have something to diff against);
- TLS — Phase 7 (A3's content-oblivious-adversary assumption is not
  realized over the wire yet);
- group-commit/batched fsync, incremental WAL + compaction (durability is
  real but per-RPC-fsync/whole-snapshot — see "Honest limits" above) —
  Phase 8;
- cluster reconfiguration;
- the Phase 6 auto-tuned leader policy wired to a real, cross-process
  network (`queso_smr::tuning::EpochTuner` assumes a single shared
  in-process `Rc<RefCell<_>>` today — `--leader` here only supports a
  fixed leader or none).
