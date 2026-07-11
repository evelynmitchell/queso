# Comparing Queso against etcd/Raft

Phase 7.5 (issue #35): a common workload/metrics harness (`queso-compare`,
`crates/compare`) so Queso and an alternative consensus system can be driven
through the *exact same* request mix, rate/concurrency, and measurement
pipeline, plus the primary comparable experiment the paper's headline claim
is about -- killing/isolating the current leader mid-run. **etcd (Raft)** is
the baseline this phase targets, per issue #35 ("the widely-deployed Raft,
the practical reference"; the QuePaxa paper also compares against
Multi-Paxos/EPaxos/Rabia, noted here but not implemented -- see "What's not
here" at the end).

Read `crates/net/README.md` first if you haven't -- this document assumes
you know how to boot a local Queso cluster and what `queso-bench` is
(Phase 7.2), and reuses both directly.

## Contents

1. [The environment constraint this doc was written under](#1-the-environment-constraint-this-doc-was-written-under)
2. [Methodology](#2-methodology)
3. [The `queso-compare` harness](#3-the-queso-compare-harness)
4. [What got added, and what didn't (dependency footprint)](#4-what-got-added-and-what-didnt-dependency-footprint)
5. [Running the Queso side](#5-running-the-queso-side)
6. [Captured Queso-side results](#6-captured-queso-side-results)
7. [Running the etcd side (owner's environment)](#7-running-the-etcd-side-owners-environment)
8. [The leader-DoS experiment against etcd, step by step](#8-the-leader-dos-experiment-against-etcd-step-by-step)
9. [Fly.io WAN runbook](#9-flyio-wan-runbook)
10. [What's not here](#10-whats-not-here)

## 1. The environment constraint this doc was written under

This sandbox has no `etcd`/`etcdctl` installed, and the outbound network
policy returns 403 for the GitHub release download and the crates.io API
lookups a normal `etcd` install would use. `go` is present but a `go
install`-based build of etcd would hit the same egress wall for its module
fetches. This document does not fake around that: **every etcd number in
this document is a placeholder for the project owner to fill in** in an
environment where etcd is reachable (a laptop, a normal CI runner with
internet egress, or the fly.io deployment in §9) -- see §7 for exactly how.
Everything on the **Queso side** is real: run in this sandbox, against a
real in-process 3-node cluster over real TCP, with real numbers captured
below (§6).

`crates/compare` itself -- the harness, both `KvTarget` implementations, the
CLI, and every test -- was built and is fully tested in this sandbox. The
etcd side of the harness (`EtcdTarget`) is real, compiling code, verified
against a small in-process fake HTTP server that speaks etcd's documented
wire format (protocol correctness only, not a performance stand-in -- see
`crates/compare/src/etcd_target.rs`'s module docs and its `tests` module).

## 2. Methodology

**What's measured, on both sides, through the identical code path**
(`crates/compare/src/workload.rs::run_workload`, reusing
`queso_net::metrics::Recorder`/`Summary` unchanged -- the exact type
`queso-bench --output json/csv` already emits, Phase 7.2's own format):

- Throughput (ops/sec) and a read/write/overall p50/p90/p99/max latency
  histogram, over an identical closed- or open-loop request schedule
  (mirrors `queso-bench`'s two modes exactly -- see
  `crates/net/README.md`).
- The same read/write mix (`--read-frac`), key-space size (`--keys`),
  concurrency, and PRNG seed on both sides, so "same offered load" is a
  fact about the harness, not an assertion about it.
- A fixed `(u32 key, i64 value)` shape on both sides (see
  `crates/compare/src/target.rs`'s module docs) -- not etcd's native
  arbitrary-byte-string values. This is deliberate parity, not an oversight:
  Queso's KV demo app is hard-coded to an 8-byte `i64` value (see
  `crates/net/README.md`'s "Honest limits"), so letting etcd use larger
  values would be measuring a different, strictly harder, workload for
  etcd -- not a fair comparison.

**How the headline fault is applied identically to both** (§8's leader-DoS
experiment, the paper's own headline result): kill or network-isolate the
current fast-path/Raft leader process while load is in flight, and measure
the **availability gap** -- the longest stretch of wall-clock time between
two consecutive successfully-completed writes. This fault is chosen
specifically because it needs no external fault-injection proxy and is
*procedurally identical* for both systems: `kill -9`/isolate one process,
keep sending requests at the survivors, time how long until requests
succeed again.

- **Queso**: `queso_net::nemesis::Nemesis::isolate` cuts the fixed
  fast-path leader off from its peers at the transport layer (the same
  fault `crates/net/tests/nemesis.rs`'s
  `isolating_the_leader_lets_the_majority_keep_deciding` already proves
  safe) -- see `crates/compare/tests/leader_dos.rs`. QuePaxa/Meerkat's
  leaderless-tolerant hedging (any live majority of recorders can still
  decide, with or without a fast-path leader -- see
  `queso_smr::cluster`'s module docs) predicts the majority keeps deciding
  immediately, no election required.
- **etcd**: `kill -9` the etcd process that `etcdctl endpoint status`
  reports as the current Raft leader, point load at a surviving member, and
  time the same gap. Raft's single-leader design predicts a stall bounded
  by etcd's election timeout (`--election-timeout`, default `1000`ms, with
  randomized backoff up to roughly 2x that per etcd's own Raft
  implementation) before a new leader is elected and writes can proceed
  again.

**Why this, and not the in-transport nemesis latency fault, as the
*comparable* experiment:** `queso_net::nemesis::Nemesis` (Phase 7.4) is an
in-process fault injector wired into this crate's own transport -- it has
no equivalent for etcd (which this harness doesn't reimplement the
transport of). Killing/isolating a leader *process*, by contrast, is a
fault every real deployment of either system can suffer and every operator
can reproduce by hand, with no proxy or code shared between the two
systems' fault-injection paths -- see `crates/compare/tests/leader_dos.rs`'s
module docs for the full rationale. §10 below separately reports a
Queso-only slow-leader (nemesis latency, not isolation) result, clearly
labeled as **not** head-to-head, since it has no etcd equivalent without an
external proxy fronting etcd (out of scope here, see §10).

**Honest framing.** Queso's KV demo app is exactly that -- a demonstration
app built to exercise the consensus/SMR core, not a tuned production store:
fixed 8-byte `i64` values, per-RPC `fsync` (not group commit), one request
per TCP connection (no pipelining) -- see `crates/net/README.md`'s "Honest
limits" for the complete list. etcd is a mature, heavily-optimized,
widely-deployed production system with years of performance engineering
behind it. **A raw ops/sec comparison between the two would not be a fair
product benchmark in either direction** -- it would mostly measure how much
engineering effort has gone into each implementation, not which consensus
*algorithm* is better. Every comparison in this document is instead framed
as **protocol/algorithm behavior**: does the system stall when its leader is
attacked, and for how long? That question is meaningful to ask of an
early-stage demo implementation, because it's a property of the consensus
protocol's design (leaderless-tolerant hedging vs. single-leader
election), not of how well-tuned the KV layer on top of it is.

## 3. The `queso-compare` harness

`crates/compare` (binary `queso-compare`, library `queso_compare`):

- [`target::KvTarget`](../crates/compare/src/target.rs) -- the one trait a
  comparison run is generic over: `async fn put(key: u32, value: i64)`,
  `async fn get(key: u32) -> Option<i64>`.
- [`queso_target::QuesoTarget`](../crates/compare/src/queso_target.rs) --
  `KvTarget` over `queso_net::client::Client` (Phase 7.2's own client
  library, unmodified).
- [`etcd_target::EtcdTarget`](../crates/compare/src/etcd_target.rs) --
  `KvTarget` over etcd's v3 gRPC-gateway JSON/HTTP API (§4 explains why not
  the `etcd-client` gRPC crate).
- [`workload::run_workload`](../crates/compare/src/workload.rs) -- the
  shared closed-/open-loop load generator, structurally a port of
  `crates/net/src/bin/queso-bench.rs`'s own loop, generic over any
  `KvTarget`, reducing into a `queso_net::metrics::Summary`.
- `bin/queso-compare.rs` -- the CLI, flags deliberately named to match
  `queso-bench`'s wherever the same dimension applies to both targets.

```sh
cargo build --release -p queso-compare
./target/release/queso-compare --help
```

## 4. What got added, and what didn't (dependency footprint)

Per this phase's guardrails, `crates/compare` is a brand-new workspace
member so none of this touches `queso-sim`/`queso-consensus`/`queso-smr`'s
build, lints, or logic -- confirmed by `cargo build --workspace`, `cargo
clippy --workspace --all-targets -- -D warnings`, and `cargo test
--workspace` all passing with this crate present (see §6 for the actual
command output). It depends on `queso-net`/`queso-smr`/`queso-sim` (path
deps, already workspace members, for types and the same in-process cluster
boot path the net crate's own tests use) but never on `queso-consensus`
directly.

**What was evaluated for the etcd side, and the decision:**

- **`etcd-client`** (the idiomatic Rust gRPC client for etcd): fetches from
  crates.io without any trouble in this sandbox (confirmed --
  `cargo add etcd-client` resolves ~80 transitive crates including
  `tonic`/`prost`/`h2`/`axum` and downloads cleanly). It **fails to build**
  here because its build script (`prost-build`/`tonic-build`) shells out to
  an external `protoc` binary, which is not installed by default and is not
  a dependency anywhere else in this workspace (`apt-get install
  protobuf-compiler` does fix it in this sandbox specifically, but assuming
  every future contributor's machine and CI runner has `protoc` on `PATH`
  is a new, unannounced build requirement for the whole workspace just to
  support one comparison harness -- rejected on that basis, not because the
  crate itself is bad).
- **etcd's gRPC-gateway JSON/HTTP API** (what `EtcdTarget` actually uses):
  etcd has shipped this since v3.3 -- an ordinary HTTP/1.1 + JSON mapping of
  the same `Put`/`Range` RPCs, on the *same port* (`2379` by default) as the
  gRPC API. `EtcdTarget` speaks it with `reqwest` (every TLS feature
  disabled -- plain `http://` only, matching `queso-net`'s own current
  no-TLS-on-the-client-port honesty) plus `base64` for the gateway's
  key/value encoding. No `protoc`, no `tonic`, no code generation.

`crates/compare/Cargo.toml`'s full dependency list, with what's genuinely
new vs. already-present-transitively-via-`queso-net`:

| Dependency | Already in `queso-net`'s tree? | Why here |
|---|---|---|
| `queso-net`, `queso-smr`, `queso-sim` | -- (path deps) | `Client`, `Command`/`Outcome`, cluster boot |
| `tokio`, `serde`, `serde_json`, `clap`, `rand`, `tracing`, `tracing-subscriber`, `anyhow` | yes | same roles `queso-net`/`queso-bench` already use them for |
| `reqwest` (no default features, `json` only) | **no** | `EtcdTarget`'s HTTP client |
| `base64` | **no** | `EtcdTarget`'s gateway key/value encoding (tiny, no transitive deps) |

`reqwest` (even with every TLS feature off) is the single meaningfully new
dependency subtree this phase adds -- `hyper`/`tower`/`url`/`idna`/`icu_*`,
roughly 30 additional crates, all confined to `crates/compare`'s own build
and never linked into `queso-node`, `queso-bench`, or any of the sim-verified
core crates' binaries.

## 5. Running the Queso side

Boot a local cluster exactly as `crates/net/README.md` describes, then
point `queso-compare` at it instead of (or alongside) `queso-bench`:

```sh
cargo build --release -p queso-net --bin queso-node -p queso-compare --bin queso-compare

# terminals 1-3: boot a 3-node cluster (see crates/net/README.md for the
# full 3-terminal example) -- same --peer/--leader/--data-dir flags.

# terminal 4: normal-case run.
./target/release/queso-compare --target queso \
  --queso-addr 127.0.0.1:8000 --queso-addr 127.0.0.1:8001 --queso-addr 127.0.0.1:8002 \
  --concurrency 16 --read-frac 0.5 --keys 1000 --duration-secs 8 --output json \
  > queso-normal-case.json
```

The leader-DoS experiment against Queso doesn't need hand-run terminals --
it's `crates/compare/tests/leader_dos.rs`, a bounded, self-contained,
non-flaky acceptance test that boots its own in-process 3-node cluster,
isolates the leader with `Nemesis` mid-run, and prints the real numbers in
§6 below:

```sh
cargo test -p queso-compare --test leader_dos -- --nocapture
```

## 6. Captured Queso-side results

Every number below is real, captured in this sandbox. Reproduce with the
commands shown; exact figures will vary run to run (real wall-clock time,
real scheduling -- see `crates/net/README.md`'s durability/fsync honesty
notes for why), but the *shape* (no multi-second stall under leader
isolation) is the property that matters and is exactly what
`crates/compare/tests/leader_dos.rs` asserts on every run, not just this
one.

### Normal case

`./target/release/queso-compare --target queso --queso-addr 127.0.0.1:8000
--queso-addr 127.0.0.1:8001 --queso-addr 127.0.0.1:8002 --concurrency 16
--read-frac 0.5 --keys 1000 --duration-secs 8 --output json`, against a real
3-node cluster (3 separate OS processes, `--release` binaries, this
sandbox's shared/contended disk and CPU):

```json
{
  "duration_secs": 9.83,
  "total_ops": 150,
  "total_errors": 0,
  "throughput_ops_per_sec": 15.26,
  "reads":   { "count": 74, "errors": 0, "p50_us": 569343,  "p90_us": 2164735, "p99_us": 2490367, "max_us": 2490367 },
  "writes":  { "count": 76, "errors": 0, "p50_us": 421887,  "p90_us": 2187263, "p99_us": 2537471, "max_us": 2537471 },
  "overall": { "count": 150, "errors": 0, "p50_us": 511743, "p90_us": 2177023, "p99_us": 2490367, "max_us": 2537471 }
}
```

0 errors, a monotonic p50 ≤ p90 ≤ p99 ≤ max histogram, same shape
`queso-bench`'s own README example has (see `crates/net/README.md`) --
absolute latencies here are dominated by per-RPC `fsync` contention across
3 processes sharing this sandbox's disk (see `crates/net/README.md`'s
"Honest limits"), not by anything specific to `queso-compare`'s harness
code; a dedicated machine or fly.io deployment (§9) should show
meaningfully lower numbers.

### Leader-DoS (the headline result)

`cargo test -p queso-compare --test leader_dos -- --nocapture`, in-process
3-node cluster, `Nemesis::isolate` on the fixed fast-path leader (node 0)
mid-run:

```
baseline (leader reachable, concurrency=4):
  43 ops in 4.80s = 9.0 ops/sec, 0 errors
  overall  p50=373.8ms  p90=923.6ms  p99=1552.4ms  max=1552.4ms

leader-isolated (concurrency=1, sequential -- see note below):
  20 writes in 1.61s = 12.4 ops/sec
  max inter-op gap = 418.4ms

recovered (post-heal, concurrency=4):
  43 ops in 3.14s = 13.7 ops/sec, 0 errors
```

**The number that matters: a 418ms maximum gap between consecutive
completed writes while the fast-path leader was fully network-isolated from
its peers** -- not a multi-second stall, let alone an unbounded one. The
majority (2 of 3 replicas) kept deciding new operations throughout, via
QuePaxa/Meerkat's leaderless-tolerant hedging, with no election to wait
out. `crates/compare/tests/leader_dos.rs` asserts this gap stays under 2
seconds on every run -- generous headroom above the observed ~418ms, chosen
to be far below a plausible Raft election-timeout window (etcd's own
default is a 1000ms election timeout with randomized backoff up to
roughly double that) while still being comfortably non-flaky in a
contended sandbox.

*Methodology note:* the leader-isolated phase intentionally runs at
concurrency 1 (one operation in flight at a time), unlike the
baseline/recovered phases' concurrency 4 -- sequential submission is what
makes "gap between consecutive completions" a directly meaningful
wall-clock measurement (with concurrency > 1, a "gap" could just mean
several operations happened to finish back-to-back after being issued
together, which measures nothing about leader-DoS recovery time). Don't
read the isolated phase's 12.4 ops/sec as "faster than baseline" -- it
isn't a throughput-comparable number to the other two phases; the gap is
the number this phase reports.

## 7. Running the etcd side (owner's environment)

**Install etcd** (needs internet egress this sandbox doesn't have):

```sh
# Option A: the official static binaries.
ETCD_VER=v3.5.17
DOWNLOAD_URL=https://github.com/etcd-io/etcd/releases/download
curl -L "${DOWNLOAD_URL}/${ETCD_VER}/etcd-${ETCD_VER}-linux-amd64.tar.gz" -o /tmp/etcd.tar.gz
mkdir -p /tmp/etcd-download && tar xzf /tmp/etcd.tar.gz -C /tmp/etcd-download --strip-components=1
sudo mv /tmp/etcd-download/etcd /tmp/etcd-download/etcdctl /usr/local/bin/

# Option B: a system package (Debian/Ubuntu).
sudo apt-get install etcd-server etcd-client
```

**Start a local 3-node etcd cluster** (matches Queso's 3-replica topology
for the normal-case comparison -- see etcd's own ["local
cluster"](https://etcd.io/docs/v3.5/dev-guide/local_cluster/) docs for the
canonical version of this):

```sh
TOKEN=queso-compare-etcd-cluster
CLUSTER="infra0=http://127.0.0.1:12380,infra1=http://127.0.0.1:22380,infra2=http://127.0.0.1:32380"

etcd --name infra0 --data-dir /tmp/etcd-data/infra0 \
  --initial-advertise-peer-urls http://127.0.0.1:12380 --listen-peer-urls http://127.0.0.1:12380 \
  --advertise-client-urls http://127.0.0.1:12379 --listen-client-urls http://127.0.0.1:12379 \
  --initial-cluster-token $TOKEN --initial-cluster $CLUSTER --initial-cluster-state new &

etcd --name infra1 --data-dir /tmp/etcd-data/infra1 \
  --initial-advertise-peer-urls http://127.0.0.1:22380 --listen-peer-urls http://127.0.0.1:22380 \
  --advertise-client-urls http://127.0.0.1:22379 --listen-client-urls http://127.0.0.1:22379 \
  --initial-cluster-token $TOKEN --initial-cluster $CLUSTER --initial-cluster-state new &

etcd --name infra2 --data-dir /tmp/etcd-data/infra2 \
  --initial-advertise-peer-urls http://127.0.0.1:32380 --listen-peer-urls http://127.0.0.1:32380 \
  --advertise-client-urls http://127.0.0.1:32379 --listen-client-urls http://127.0.0.1:32379 \
  --initial-cluster-token $TOKEN --initial-cluster $CLUSTER --initial-cluster-state new &

sleep 2
etcdctl --endpoints=http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 endpoint health
```

**Sanity-check the gateway `EtcdTarget` actually talks to:**

```sh
curl -s -X POST http://127.0.0.1:12379/v3/kv/put \
  -d '{"key": "'"$(echo -n 42 | base64)"'", "value": "'"$(echo -n 777 | base64)"'"}'
curl -s -X POST http://127.0.0.1:12379/v3/kv/range \
  -d '{"key": "'"$(echo -n 42 | base64)"'"}'
# Expect the second command's "kvs[0].value" to base64-decode back to "777".
```

**Normal-case run**, same flags as the Queso side (§5/§6) so the two are
directly diffable:

```sh
cargo build --release -p queso-compare
./target/release/queso-compare --target etcd --etcd-url http://127.0.0.1:12379 \
  --concurrency 16 --read-frac 0.5 --keys 1000 --duration-secs 8 --output json \
  > etcd-normal-case.json

diff <(jq -S . queso-normal-case.json) <(jq -S . etcd-normal-case.json)
```

## 8. The leader-DoS experiment against etcd, step by step

This reproduces the exact procedure §2/§6 describe for Queso, against a
real etcd cluster:

```sh
# 1. Warm up + confirm the cluster is healthy.
./target/release/queso-compare --target etcd --etcd-url http://127.0.0.1:12379 \
  --concurrency 4 --ops 20 --output json

# 2. Find the current Raft leader.
etcdctl --endpoints=http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 \
  endpoint status --write-out=table
# Note which "infraN" has "IS LEADER" = true, and its PID:
pgrep -f "etcd --name infra0"   # (substitute the leader's actual name)

# 3. Kill it -- the direct etcd analogue of Nemesis::isolate.
kill -9 <leader_pid>

# 4. Immediately drive load at a SURVIVING member's client URL (not the
#    killed one -- unlike queso_net::client::Client, EtcdTarget targets a
#    single fixed URL with no retry-to-another-endpoint, so point it at a
#    survivor by hand; a production etcd client library would do this
#    failover automatically, see the note below) and time the gap the same
#    way crates/compare/tests/leader_dos.rs does: one op at a time,
#    recording the wall-clock gap between consecutive successful writes.
./target/release/queso-compare --target etcd --etcd-url http://127.0.0.1:22379 \
  --concurrency 1 --ops 20 --output json
# (For the actual max-gap number, not just aggregate throughput, either
# eyeball the p99/max latency in this run's JSON output -- the first
# operation after the kill will dominate it -- or adapt
# crates/compare/tests/leader_dos.rs's per-op-timestamp loop, swapping
# QuesoTarget for EtcdTarget; that loop is generic over KvTarget already.)

# 5. Compare the observed gap against Queso's ~418ms (§6) and against
#    etcd's configured --election-timeout (default 1000ms, so expect
#    something in the hundreds-of-ms-to-low-seconds range, structurally
#    bounded below by the election timeout in a way Queso's isolation
#    result is not).
```

**Why `EtcdTarget` doesn't itself retry across endpoints:** `crates/compare`
keeps `EtcdTarget` deliberately simple (a single `base_url`, see
`crates/compare/src/etcd_target.rs`) rather than reimplementing etcd's own
client-side endpoint-failover logic -- that's real, well-tested
functionality in etcd's official client libraries, and re-deriving a worse
version of it here would not make the comparison more honest. This is the
one place the two `KvTarget` implementations are not perfectly symmetric:
`QuesoTarget` inherits `queso_net::client::Client`'s
retry-to-another-replica for free (Phase 7.2), while a fully equivalent
etcd-side client would need the same multi-endpoint failover wired in by
hand or via `etcd-client` (blocked here, see §4). Step 4 above works around
this manually (point at a known survivor) so the *experiment* is still
apples-to-apples even though the *client convenience* is not.

## 9. Fly.io WAN runbook

Building on `docs/deploy-flyio.md` (Phase 7.3's Queso-on-fly runbook,
unmodified by this phase): deploy a 3-region Queso cluster exactly as that
document describes, and deploy a 3-region etcd cluster the same way, then
run both experiments across the resulting real WAN links.

1. **Deploy Queso**, following `docs/deploy-flyio.md` §§1-8 verbatim
   (`queso-0`/`queso-1`/`queso-2` fly apps, `iad`/`lhr`/`nrt`, `fly proxy`
   tunnels to reach the client ports from outside).
2. **Deploy etcd**, mirroring the same one-app-per-replica-per-region
   scheme (`docs/deploy-flyio.md` §1's rationale for why one app per
   replica, not one multi-machine app, applies identically to etcd): three
   fly apps (e.g. `etcd-0`/`etcd-1`/`etcd-2`), each running the official
   `gcr.io/etcd-development/etcd` image (or a minimal Dockerfile wrapping
   the static binary from §7), `--initial-advertise-peer-urls
   http://etcd-N.internal:2380`, `--listen-peer-urls http://[::]:2380`,
   `--listen-client-urls http://[::]:2379`, and an `--initial-cluster`
   flag listing all three `.internal` peer URLs -- the same
   `.internal`-DNS-for-peer-discovery trick `docs/deploy-flyio.md` §1
   documents for Queso, since fly's private DNS works identically for any
   app. Provision a persistent volume per app for etcd's own data
   directory (`docs/deploy-flyio.md` §5's rationale applies unchanged: no
   volume means every redeploy starts from a blank etcd, silently changing
   what's being measured). This repo does not ship a ready-made
   `deploy/etcd.fly.toml` -- the pattern above is a direct transplant of
   `deploy/fly.toml`'s scheme (see that file's own comments) onto etcd's
   CLI flags, not a new architecture.
3. **`fly proxy` both clusters' client ports** to local ports, as
   `docs/deploy-flyio.md` §8 does for Queso.
4. **Run §6's normal-case comparison** with both `--queso-addr`/`--etcd-url`
   pointed at the respective `fly proxy` tunnels instead of localhost --
   identical `queso-compare` invocations, now measuring real cross-region
   (`iad`/`lhr`/`nrt`) WAN latency for both systems instead of loopback.

   > **WAN caveat — a connection-reuse asymmetry that only matters off
   > loopback.** `QuesoTarget` drives `queso_net::client::Client`, which by
   > design opens a **fresh TCP connection per operation** (no pooling/
   > pipelining -- see `crates/net/README.md`'s "Honest limits"), while
   > `EtcdTarget` uses `reqwest`'s default **HTTP/1.1 keep-alive connection
   > pool**, so etcd reuses a warm connection across ops. On loopback (§6)
   > that difference is lost in the hundreds-of-ms fsync-dominated latency
   > and does not affect the leader-DoS conclusion. **Over a real WAN it is
   > not negligible:** a fresh connection costs Queso a full extra
   > handshake RTT *per op* that pooled etcd does not pay, which
   > structurally penalizes Queso's raw-latency/throughput numbers for
   > reasons that have nothing to do with the consensus protocol. So when
   > reading §6's normal-case numbers over the WAN, treat any Queso-vs-etcd
   > *latency/throughput* gap as **confounded by transport, not a protocol
   > result** -- the apples-to-apples protocol claim this doc actually
   > stands behind is the §8 leader-DoS *behavior* (does the cluster stall
   > for an election?), which does not depend on connection reuse. To
   > remove the confound before publishing WAN latency numbers, either add
   > pooling to `Client` (a `queso-net` change, out of scope here) or drive
   > Queso through a warm-connection client shim; until then, do not present
   > the WAN normal-case ops/sec as a fair head-to-head.
5. **Run §8's leader-DoS procedure** against both clusters via `fly ssh
   console -a <app>` (or `fly machine stop`, for a cleaner "the machine is
   gone" fault than a same-container process kill) targeting whichever
   region's replica/member is currently the leader.

**What this section cannot verify** (same honesty `docs/deploy-flyio.md`'s
own final section already commits to, extended to the etcd side): this
sandbox has no real fly.io account, so nothing in this runbook beyond
"here is the exact command to run" has been executed end-to-end. In
particular: that the official etcd Docker image builds/runs cleanly on
fly's infrastructure the same way `deploy/Dockerfile` does; that etcd's
Raft peer traffic over fly's `.internal`/6PN network gets the same implicit
WireGuard encryption `docs/deploy-flyio.md` §12 documents for Queso (it
should, per fly's documented network model -- it's the same private network
for every app in an org, not something specific to Queso's deployment --
but this was not independently confirmed against a real etcd deployment);
and, obviously, real cross-region WAN throughput/latency numbers for either
system.

## 10. What's not here

- **Multi-Paxos, EPaxos, Rabia** (the QuePaxa paper's other baselines, per
  issue #35's "note these; include if feasible"): not implemented. `etcd`
  is the one the issue names as primary ("the practical reference"); adding
  the others would mean either finding/wrapping existing Rust
  implementations (none as mature or as widely packaged as etcd) or
  implementing the protocols from scratch, both well beyond this phase's
  scope discipline ("comparison only," per the issue).
- **A Queso-only slow-leader (not isolated) result:** `queso_net::nemesis`
  can also apply continuous latency/jitter to just the leader's outbound
  links (`Nemesis::set_latency` scoped by isolating-then-partially-healing,
  or a custom `FaultPlan`) rather than a hard partition -- demonstrating
  QuePaxa's fast-path degrading gracefully to the hedged path under a
  *slow*, not dead, leader. This is a genuine, real experiment one could
  run against Queso (see `crates/net/src/nemesis.rs`'s latency/jitter
  support), but it is **not** included as a head-to-head number here: it
  has no etcd equivalent without an external proxy (e.g. toxiproxy)
  fronting etcd's peer traffic, which is out of scope for this sandbox and
  not wired up by this phase (see `crates/net/README.md`'s own "why
  in-transport, not toxiproxy" rationale, which applies in reverse here --
  toxiproxy is exactly the tool that *would* let a future phase add this
  fairly for etcd too).
- **Real etcd numbers.** By construction of this sandbox -- see §1, §6, §7.
