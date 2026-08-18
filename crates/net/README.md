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
DNS name, not just literal IP. Phase 8.2d (issue #47) adds `queso-admin`, an
out-of-cluster operator CLI (`status`/`health`/`put`/`get`) built on
`queso_net::admin` -- see "`queso-admin`: out-of-cluster operator CLI" below.

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
> spawned-and-killed `queso-node` processes). Phase 8.1a (issue #46) then
> hardened *how* that durable state is written — group-commit coalescing,
> an async fsync offload, and an on-disk schema-version header — without
> touching the write-before-reply guarantee itself; see below. Phase 8.2a
> (issue #47) adds opt-in, app-level TLS (mutual TLS on every peer
> connection, server-authenticated TLS on every client connection) so A3's
> content-oblivious-adversary assumption *can* be realized over the wire —
> see "TLS (Phase 8.2a)" below for how to enable it and exactly what it
> does and doesn't cover; it remains **off by default** (plaintext,
> unchanged, unless explicitly configured). It is still **not** a
> production-grade durability story: see "Honest limits" below before
> deploying this for real. No reconfiguration, no log compaction. (Phase
> 7.2 does add a client library with retry-to-another-replica — see below.)

**Honest limits of the current durability implementation (Phase 7
hardening plus Phase 8.1a, not Phase 8's full operability story):**

- **Group commit, not per-op group-commit-of-one.** As of Phase 8.1a
  (issue #46), the event loop no longer `fsync`s once per inbound message
  unconditionally: it applies one event, then drains any *already-queued*
  events (non-blocking, up to a capped batch size — see
  `src/driver.rs`'s "Group commit" docs) before taking a single
  `Durable` snapshot and persisting it once for the whole batch. Under
  low/serialized load (never more than one event ready at a time) a batch
  is always exactly size 1 — identical `fsync`-per-op behavior and latency
  to before this change. Under concurrent load, multiple mutating events
  can share one `fsync`, amortizing its cost — see
  `tests/group_commit.rs`'s `group_commit_coalesces_fsyncs_under_concurrent_load`,
  which measures this directly (fewer real fsync'd writes than
  durable-mutating events applied). This crate's real bottleneck is still
  disk fsync latency (typically low hundreds to low thousands of ops/sec
  on common cloud block storage, much higher on NVMe with a battery-backed
  write cache) — group commit amortizes that cost across whatever is
  genuinely ready together, it does not eliminate it, and this
  implementation's protocol has no pipelining (one decision in flight at a
  time per replica — Stage 4a scope), which caps how much cross-op
  batching is actually available to exploit.
- **The blocking write itself is offloaded, not eliminated.** The
  write+`fsync`+rename+directory-`fsync` sequence now runs on
  `tokio::task::spawn_blocking`'s dedicated thread pool rather than the
  driver's own async task (`Store::persist`, see `src/persist.rs`'s
  docs) — the driver still `.await`s its completion before releasing any
  reply that depends on it (write-before-reply, P12, unchanged), but other
  tokio tasks on the same runtime (peer/client accept loops, dialers,
  timers) keep making progress while one replica's fsync is in flight,
  and more events can accumulate for the *next* batch to coalesce meanwhile.
- **On-disk schema-version header (issue #39).** Every snapshot file now
  starts with a small magic-bytes + version header; `Store::load` rejects
  (a clear error, never a silent mis-parse) a file whose header doesn't
  match what the running build understands — see `src/persist.rs`'s docs.
  This is v1: the payload's own layout hasn't changed in this PR, only the
  header wrapping it.
- **Whole-state snapshot, not an append-only log.** Each persist still
  rewrites the replica's *entire* durable state (every slot's recorder,
  the whole applied log), not just the delta — `O(log length)` per write,
  now amortized across a batch's worth of decisions rather than
  eliminated. Fine for short-lived clusters/tests; a long-running
  deployment needs an incremental WAL plus periodic snapshot/compaction
  (Phase 8.1c territory, deliberately deferred — see issue #46's
  design-decision comment for why a byte-incremental delta-WAL is not an
  obviously-safe next step for this protocol).
- **fsync is trusted, not independently verified.** Ordinary POSIX
  `fsync`/atomic-rename semantics are assumed; this does not defend
  against a lying disk/filesystem, nor does it `fsync` the data
  directory's own parent.
- **Single-copy durability, not replicated backup.** Durable state lives
  on one disk per replica. Losing that disk (not just the process) loses
  that replica's durable state — recoverable only via the ordinary
  catch-up-from-a-live-majority path, not via any backup/replication of
  the on-disk file itself.
- **No reconfiguration, no compaction** — unchanged from before this fix;
  see "Scope" below. TLS is now available (Phase 8.2a, opt-in, off by
  default) — see "TLS (Phase 8.2a)" below.

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

## Phase 8.2: status/metrics HTTP endpoints (issue #47)

Pass `--status-listen <addr>` to bind a small, opt-in HTTP status/metrics
server on that replica (omit it -- the default -- and nothing is bound, no
task is spawned, zero overhead: see `NodeConfig::status_listen_addr`'s
docs). It's a hand-rolled HTTP/1.1 `GET` responder (`src/status.rs`), not
`hyper`/`axum`/`warp` -- dependency-light, in the same spirit as this
crate's other hand-rolled wire protocols (`src/client.rs`, `src/wire.rs`):

```sh
./target/debug/queso-node \
  --id 0 --seed 1 \
  --listen 127.0.0.1:7000 --client-listen 127.0.0.1:8000 --status-listen 127.0.0.1:9000 \
  --peer 0=127.0.0.1:7000 --peer 1=127.0.0.1:7001 --peer 2=127.0.0.1:7002 \
  --leader 0
```

Three endpoints, `GET` only:

- **`GET /health`** -- liveness: `200 OK` iff the process/runtime is up at
  all. A process-up check only, deliberately blind to consensus progress
  (so an orchestrator's liveness probe never kills a replica that is merely
  catching up).
- **`GET /ready`** -- readiness: `200 OK` iff this replica is not currently
  known to be running its own internal restart catch-up probe
  (`queso_smr::SmrNode::is_catching_up()`), `503 Service Unavailable`
  otherwise. **Honest limits**: this is *not* a proof this replica is fully
  caught up with the cluster's current frontier or that a linearizable read
  against it right now would see the latest value -- catch-up only proves
  progress up to whatever frontier a majority could show it at the moment
  it asked, and this endpoint performs no extra round trip to check whether
  the cluster has moved on since. It is also not sticky -- the catch-up
  quiescence watchdog re-issuing catch-up (e.g. after a transient
  partition) can flip this back to `false` after having been `true`. Good
  enough for "don't route to a replica that just rebooted and is still
  learning" (a load balancer's readiness probe -- see
  `docs/deploy-flyio.md`), not a linearizability guarantee. See
  `src/status.rs`'s module docs for the full reasoning.
- **`GET /metrics`** -- a small pretty-printed JSON document of counters
  this replica actually tracks: `events_processed` (total dispatched
  events since boot), `next_slot` (current log frontier), `save_count` (the
  real, always-on fsync count from `queso_net::persist::Store::save_count`
  -- not the test-only `NodeConfig::save_counter`), `ready` (same bool
  `/ready` reports), and `uptime_secs`.

Anything else (wrong method, unknown path) is a `404`/`405`; a malformed or
slow-loris-style request gets a bounded-size, bounded-time read and a `400`
or a dropped connection, never a panic. See `cargo test -p queso-net --test
status` (below) for the acceptance test against a real cluster.

## Phase 9.2: `GET /chain` — conformance observability (issue #56)

Pass `--chain-checkpoints <N>` (alongside `--status-listen`) to make this
replica fold the Chain-of-Blocks hash over the commands it applies and
publish its hash every `N` slots:

```sh
queso-node --id 0 ... --status-listen 127.0.0.1:9100 --chain-checkpoints 64
curl -s 127.0.0.1:9100/chain
```

```json
{
  "checkpoint_every": 64,
  "frontier": { "n": 137, "h": "0x9d2f..." },
  "truncated": false,
  "checkpoints": [ { "n": 64, "h": "0x41a8..." }, { "n": 128, "h": "0x7c03..." } ]
}
```

**What it's for.** A conformance harness (`queso-conformance`, Phase 9.1)
checks replication safety by comparing replicas' `(n, h)` chain states: if
two replicas ever show a different `h` at the same `n`, they applied
different command sequences. In-process it can fold that chain itself; against
real `queso-node` processes it cannot, so the node publishes it here.

**Why checkpoints rather than just the frontier.** `/metrics`' `next_slot`
looks like enough, and isn't: replicas lag each other by design, so two
frontier readings almost never share an `n` and there is nothing to compare
— 9.1 measured 2 cross-replica comparisons for frontier-only sampling versus
20 for checkpointed sampling on the same workload. Publishing at fixed `n`
boundaries makes replicas comparable by construction.

**Operational notes.**

- **Off by default**, and off entirely without `--status-listen`. `/chain`
  is a `404` when unconfigured (deliberately, so a harness pointed at the
  wrong node finds out rather than reading an empty table as "applied
  nothing"). An ordinary deployment pays nothing for it.
- **Every replica must use the same `N`.** Different spacings publish at
  disjoint slots and can never be compared.
- Hashes are hex **strings**: they use the full 64-bit range, which does not
  survive a JSON reader that parses numbers as doubles.
- The table is a bounded ring (256 entries); once it wraps, `truncated`
  becomes `true` rather than silently serving a partial history.
- The fold is volatile and rebuilt from the durable applied log at boot, so
  a restarted replica republishes the same hashes for slots it applied
  before the crash — verified against real `SIGKILL`ed processes in
  `tests/chain_restart.rs`. This holds while the whole applied log stays
  resident; log compaction (deferred, issue #46) would need a snapshot base.

Tests: `cargo test -p queso-net --test chain` (in-process cluster: published
checkpoints match a chain computed independently in the test, replicas never
conflict at a shared `n`, `/chain` 404s when unconfigured) and `--test
chain_restart` (real processes, `SIGKILL` and reboot).

## `queso-admin`: out-of-cluster operator CLI (Phase 8.2d, issue #47)

`queso-admin` is a small operator tool that talks to a running cluster from
*outside* it -- unlike `queso-node` (a replica) or `queso-bench` (a load
generator), it exists purely for "is my cluster healthy?" and "let me poke
one key" during manual operation. Its reusable logic lives in
`queso_net::admin` (unit/integration-testable in-process, see
`tests/admin.rs`); `src/bin/queso-admin.rs` is a thin `clap` wrapper over it.
Dependency-light like the rest of this crate: `status`'s `GET /metrics` fetch
is a hand-rolled request over a bare `tokio::net::TcpStream` (same style as
`tests/support/mod.rs::http_get`), not `reqwest`/`hyper`.

Build it alongside the other binaries:

```sh
cargo build -p queso-net --bin queso-admin
```

### `queso-admin status` -- the flagship subcommand

Polls every replica's status-server address (`GET /metrics`, plaintext --
the status port never speaks TLS, see "Phase 8.2" above) and renders a
cluster-health table: reachability, `ready`, log frontier (`next_slot`),
`save_count`, and `uptime_secs`, plus a rollup of how many replicas answered
and whether their frontiers agree.

```sh
./target/debug/queso-admin status \
  --status-addr 127.0.0.1:9000 --status-addr 127.0.0.1:9001 --status-addr 127.0.0.1:9002
```

```
index address                      reachable  ready   next_slot  save_cnt   uptime_s
0     127.0.0.1:9000               yes        true    2          6          11.3
1     127.0.0.1:9001               yes        true    0          2          11.3       (lagging)
2     127.0.0.1:9002               yes        true    0          2          11.3       (lagging)

cluster: 3/3 replicas reachable, all_ready=true
frontier: max next_slot=2; lagging replica indices (behind max): [1, 2]
```

A replica that is down (connection refused/timed out) or answers something
unparseable is reported **unreachable**, never a crash or a hang for the
rest of the command -- each replica's `GET /metrics` is fetched concurrently
and bounded by `--timeout-ms` (default 3000ms), so one dead address costs
this command roughly one timeout, not `timeout * replica count`.
`queso-admin status` exits non-zero only if **no** replica at all was
reachable; a degraded-but-partially-healthy cluster (some down, or some
lagging) is still reported in full and exits `0` -- still useful output,
not a failure of the command itself.

**"Lagging" is a real, expected condition, not a bug**: per
`queso_smr::replica`'s module docs, a replica only advances its own
`next_slot` when it actively attempts (or catches up on) a slot itself, so a
write that went through only one replica can legitimately leave the other
two behind until something asks them to do more work -- `status` surfaces
that honestly (`next_slot` behind the cluster's max) rather than hiding it.
There is deliberately **no "trigger catch-up" subcommand**: the node has no
such admin RPC, and doesn't need one -- a replica that fell behind
self-heals via its own restart/quiescence catch-up watchdog. `queso-admin
health --status-addr <addr>` is a cheap convenience alongside `status` for
checking one replica's `GET /health`/`GET /ready` directly.

### `queso-admin put`/`get`

Submit a real `Put`/`Get` against the cluster's *client* ports (not the
status ports) via `queso_net::client::Client` -- the same pooled-addresses,
retry-to-another-replica path `queso-bench` uses, with the same optional
TLS flags (`--tls-ca`/`--tls-server-name`, see "TLS" below):

```sh
./target/debug/queso-admin put 42 777 \
  --addr 127.0.0.1:8000 --addr 127.0.0.1:8001 --addr 127.0.0.1:8002
# Put

./target/debug/queso-admin get 42 \
  --addr 127.0.0.1:8000 --addr 127.0.0.1:8001 --addr 127.0.0.1:8002
# Get(Some(777))
```

**The admin `ClientId`.** Every `Command` is tagged `(ClientId, seq)` for
idempotent dedup (A6) -- a real application's clients own that dedup space,
so `queso-admin` must not collide with it. It defaults to
`queso_net::admin::DEFAULT_ADMIN_CLIENT_ID` (`ClientId(u32::MAX - 1)`) --
deliberately **not** `u32::MAX` itself, which `queso_smr::replica` reserves
internally for its own restart catch-up probes and asserts no real
submission ever uses (a build that picked `u32::MAX` panics every replica it
talks to). This is only a *convention*, not an enforced reservation --
override it with `--client-id` if your application also happens to use it.
**The admin `seq`** defaults to the current wall-clock time in nanoseconds
(`queso_net::admin::default_seq`) since `queso-admin` is a fresh process per
invocation with no persisted counter to draw a guaranteed-fresh `seq` from.
Two admin writes that share a `seq` collide in the server-side A6 dedup
(which is keyed by `ClientId` alone, not per-key), and the second is silently
dropped while still reporting `Put` — so pass `--seq` explicitly for
guaranteed-fresh, correctly-ordered values in scripted/rapid-fire or parallel
usage. See `queso_net::admin`'s module docs for the full reasoning on both.

### Tests (`tests/admin.rs`)

```sh
cargo test -p queso-net --test admin
```

Against real, in-process, real-TCP clusters (`tests/support/mod.rs`'s
`spawn_cluster`/`spawn_cluster_with_status`): `status` against a healthy
3-node cluster reports every replica reachable/ready and, once each replica
has been touched, agreeing frontiers; a second test swaps one replica's
status address for one nothing is listening on and asserts it's reported
unreachable (not a crash or a hang) while the other two are still reported
healthy; a third round-trips a `Put`/`Get` through `queso_net::admin`'s
`Client`-backed path; a fourth proves the admin `ClientId` doesn't interfere
with an ordinary application client's own ops on the same cluster.

## Automated end-to-end tests

```sh
cargo test -p queso-net --test cluster
cargo test -p queso-net --test bench
cargo test -p queso-net --test restart_recovery
cargo test -p queso-net --test status
cargo test -p queso-net --test admin
cargo test -p queso-net --test nemesis
cargo test -p queso-net --test group_commit
cargo test -p queso-net --test tls
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

`tests/status.rs` is issue #47's (Phase 8.2) acceptance test for the
status/metrics HTTP server: against a real 3-node cluster with
`--status-listen`-equivalent addresses bound up front (see
`spawn_cluster_with_status` in `tests/support/mod.rs`), it asserts `/health`
answers `200` before any operation is submitted, `/ready` answers `200`
once an operation has been driven through, `/metrics`' JSON counters
(`save_count`, `next_slot`, `events_processed`) actually move after a
decided `Put`, and `/unknown` is a `404`. A second test reuses the ordinary
(status-disabled) `spawn_cluster` every other test in this file uses and
drives a full `Put`/`Get` round trip, proving the feature costs nothing
observable when `status_listen_addr` is left `None`.

`tests/group_commit.rs` is issue #46's (Phase 8.1a) acceptance test:
group-commit coalescing, the async fsync offload, and — the property that
actually matters — that write-before-reply (P12) survives both changes
intact. `write_before_reply_holds_even_when_the_fsync_is_slow` proves the
ordering behaviorally (an artificially slow fsync, injected via
`NodeConfig::persist_delay`, makes a client-observable lower bound on reply
latency); `driver_source_persists_before_it_flushes_outbound_in_the_loop`
is a fast, textual companion tripwire for the same property.
`group_commit_coalesces_fsyncs_under_concurrent_load` proves batching is
real by comparing two counters from the same run (durable-mutating events
applied vs. real fsync'd writes performed); `a_single_op_at_a_time_still_works_and_is_still_persisted`
is the batch-size-1 correctness counterpart.

`tests/tls.rs` is issue #47's (Phase 8.2a) acceptance test -- see "TLS
(Phase 8.2a)" below for what it covers (handshake success against a real
mTLS+server-TLS 3-node cluster, plus the negative tests proving both mTLS
on the peer acceptor and server-cert verification on the client side are
actually enforced, not decorative).

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

## TLS (Phase 8.2a, issue #47)

Opt-in, **off by default**, app-level TLS via `rustls`/`tokio-rustls`
(pure-Rust — no OpenSSL, no system TLS library, keeps `deploy/Dockerfile`'s
glibc-only, no-dynamically-linked-libssl builder image unchanged). See
`src/tls.rs`'s module docs for the full design and exactly what is/isn't
verified; this section is the how-to-enable-it summary.

- **Peer↔peer traffic is mutual TLS (mTLS).** Every replica presents its
  own certificate both when it dials another peer and when it accepts one;
  both ends verify the other's chain against a shared, operator-supplied CA.
  The peer acceptor's client-cert requirement is enforced by
  `rustls::server::WebPkiClientVerifier`'s default (client auth
  *required*, anonymous connections denied) — an un-cert'd dialer is
  rejected at the TLS handshake, before a single `WireMsg` byte is read.
  The existing `WireMsg::Hello(NodeId)` handshake still runs *inside* the
  now-encrypted session, unchanged, to identify which replica dialed in.
- **Client→replica traffic is server-authenticated TLS only.** A client
  (`queso_net::client::Client`/`submit_with_tls`, or `queso-bench`) verifies
  the replica's server cert against the configured CA but never presents a
  client certificate of its own — end clients are not cluster members, so
  client-cert auth for them is out of scope.
- **No verification is ever disabled.** There is no "accept any cert"
  verifier anywhere in this crate. The one deliberate relaxation
  (`crate::tls::ChainOnlyServerCertVerifier`, used by default for the peer
  dialer's view of the acceptor's cert, and for the client's view of a
  replica's cert) still performs full chain-to-trust-anchor + signature +
  validity-period verification — it only skips matching the presented
  cert's Subject Alternative Names against the dialed address, since this
  crate's peers/replicas are addressed by an arbitrary `--peer`/`--addr`
  string (a literal IP, a Docker/fly-internal hostname, ...) that need not
  be baked into that node's cert as a SAN. `ClientTlsConfig::expected_server_name`
  opts a caller back into full, unrelaxed name verification when wanted.
  See `src/tls.rs`'s module docs for the complete argument.

### Enabling it

Generate a CA and one cert/key per replica (any TLS toolchain works; here's
a quick `openssl` recipe for a local test cluster — `crates/net/tests/tls.rs`
does the equivalent with `rcgen` at test-run time instead):

```sh
# One CA, trusted by every replica and every client.
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -keyout ca.key.pem -out ca.pem -days 3650 -subj "/CN=queso-test-ca"

# One cert/key per replica (repeat with -subj "/CN=node-1" etc, and a SAN
# matching the address peers/clients will actually dial, e.g. via
# -addext "subjectAltName=IP:127.0.0.1"; irrelevant for the default
# chain-only peer verifier, but required if you set
# `expected_server_name`/want strict verification).
openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
  -keyout node-0.key.pem -out node-0.csr.pem -subj "/CN=node-0"
openssl x509 -req -in node-0.csr.pem -CA ca.pem -CAkey ca.key.pem \
  -CAcreateserial -out node-0.cert.pem -days 825 \
  -extfile <(printf "extendedKeyUsage=serverAuth,clientAuth\nsubjectAltName=IP:127.0.0.1")
```

Then pass all three PEM files to `queso-node`:

```sh
./target/debug/queso-node \
  --id 0 --seed 1 --listen 127.0.0.1:7000 --client-listen 127.0.0.1:8000 \
  --peer 0=127.0.0.1:7000 --peer 1=127.0.0.1:7001 --peer 2=127.0.0.1:7002 \
  --leader 0 \
  --tls-cert node-0.cert.pem --tls-key node-0.key.pem --tls-ca ca.pem
```

`--tls-cert`/`--tls-key`/`--tls-ca` are all-or-nothing: passing only some
of the three is a startup error (`resolve_tls_config` in
`src/bin/queso-node.rs`), not a silent partial-plaintext fallback. Every
replica in the cluster needs the same `--tls-ca` (or at least a CA bundle
that verifies every other replica's cert); each gets its own
`--tls-cert`/`--tls-key`.

For a client, `queso-bench` takes a matching `--tls-ca` (and optional
`--tls-server-name` to pin strict name verification instead of the default
chain-only mode):

```sh
./target/release/queso-bench \
  --addr 127.0.0.1:8000 --addr 127.0.0.1:8001 --addr 127.0.0.1:8002 \
  --concurrency 64 --duration-secs 8 --tls-ca ca.pem
```

Programmatically, `queso_net::client::Client`/`ClientConfig::tls` (built via
`queso_net::tls::build_client_tls`) and `queso_net::client::submit_with_tls`
are the TLS-capable equivalents of `Client`/`client::submit`.

### Honest limitations

- **No cert rotation.** Every cert/key/CA PEM is loaded once at boot
  (`crate::tls::build_peer_tls`/`build_client_facing_server_tls`); rotating
  a cert requires restarting the replica. No hot-reload, no SIGHUP handler.
- **`queso-bench`/`queso-admin` are TLS-capable; other client tooling may
  not be.** The `queso_net::client` library (`Client`/`submit_with_tls`) and
  `queso-bench`'s/`queso-admin put`'s/`queso-admin get`'s `--tls-ca` flag
  are wired end to end. A hand-rolled caller using the bare `client::submit`
  helper (not `submit_with_tls`) gets plaintext regardless of a replica's
  TLS configuration — that helper intentionally stayed a minimal, non-TLS
  building block (see its docs); reach for `submit_with_tls`/`Client` for a
  real TLS-capable caller. `queso-admin status`/`health` never use TLS at
  all — the status port is always plaintext HTTP (see `src/status.rs`'s
  module docs), regardless of whether the cluster's client/peer traffic is
  TLS-enabled.
- **Name-matching is relaxed by default** (see `crate::tls::ChainOnlyServerCertVerifier`'s
  docs above) — chain-to-CA trust is the real security boundary here, not
  hostname pinning, unless `expected_server_name` is explicitly set.
- **No client-cert auth for end clients** — deliberately out of scope; a
  client's own compromise/loss is not a cluster-membership event the way a
  replica's would be.
- **Not exercised against fly.io's real `.internal` DNS/6PN mesh** in this
  environment — see `docs/deploy-flyio.md` §12 for how TLS interacts with
  fly's own already-encrypted `.internal` traffic and the honest "not
  verified here" caveat that applies to the rest of that runbook too.

## Scope (Phase 7.1 + 7.2 + 7.4 + 8.2a + 8.2d — see the crate docs)

Transport + node binary + client-facing wire protocol (Phase 7.1), a
client library with a replica-address pool and retry-to-another-replica
plus the `queso-bench` load generator with throughput/latency metrics
(Phase 7.2), an in-transport fault injector plus adversarial perf tests
(Phase 7.4), opt-in app-level TLS for both peer mTLS and
server-authenticated client TLS (Phase 8.2a), and `queso-admin`, an
out-of-cluster operator CLI for cluster-health polling and one-off
`Put`/`Get`s (Phase 8.2d, above). Explicitly **not** in this crate yet:

- session/seq management beyond A6's one-in-flight-per-`ClientId` minimum,
  connection pooling/reuse, or pipelining in the client library;
- any admin RPC to force a lagging replica to catch up -- `queso-admin
  status` only *observes* a lagging frontier (see above); the node itself
  has no such endpoint, by design (see `queso_net::admin`'s module docs);
- comparisons against alternative systems — this crate's `--output
  json`/`csv` exist so those runs have something to diff against, but the
  actual comparison harness (`queso-compare`) and methodology live in the
  new `crates/compare` crate, kept separate so this crate's own build/lints
  stay untouched by it — see `docs/compare-etcd.md` (Phase 7.5, issue #35);
- TLS cert rotation, and client-cert auth for end clients (deliberately
  out of scope) — see "TLS (Phase 8.2a)"'s "Honest limitations" above for
  the full list of what TLS itself doesn't cover;
- incremental WAL + compaction (durability is real, group-commit-batched
  as of Phase 8.1a, but still whole-snapshot, not byte-incremental — see
  "Honest limits" above) — Phase 8.1b/8.1c;
- cluster reconfiguration;
- the Phase 6 auto-tuned leader policy wired to a real, cross-process
  network (`queso_smr::tuning::EpochTuner` assumes a single shared
  in-process `Rc<RefCell<_>>` today — `--leader` here only supports a
  fixed leader or none).
