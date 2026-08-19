# `queso-soak` — Chain-of-Blocks against real processes (Phase 9.2)

Drives [`queso-conformance`](../conformance)'s Chain-of-Blocks workload and
observers against **real `queso-node` OS processes**, over real TCP, under
socket-level turbulence. Issue [#56], part of the Phase 9 epic [#54].

This is where Phase 9 starts actually reaching the sim↔real gap. 9.1 built
the workload and observers and ran them in-process, where they exercise the
same code the existing DST suite already covers. Here the same observers —
unchanged — watch real processes: real tokio scheduling, real sockets, real
disk, real `SIGKILL`, and partitions that genuinely close TCP connections.

It does not *close* that gap. The faults here are scripted and short — a cut,
a kill, some latency, then a check. Antithesis-style testing is about
autonomous, sustained turbulence over a long run, and a bug that needs twenty
minutes of churn to surface would still be missed. That soak is slice 3.

## The two pieces

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

## Running it

```sh
cargo build --all                 # queso-soak spawns the queso-node binary
cargo test -p queso-soak
```

The `queso-node` binary is located at run time: `QUESO_NODE_BIN` if set,
otherwise alongside the test executable (`<target>/<profile>/queso-node`).
`cargo test -p queso-soak` on its own does not build it — the crate depends
on `queso-net`'s *library* — so build first, or the tests panic with that
instruction.

Scenarios are serialized by a process-wide lock: each spawns three real node
processes, and five in parallel is fifteen processes competing for CPU,
which was enough to push cluster boot past a readiness timeout on a loaded
runner. Expect ~60s for the suite.

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

## What's still open for #56

This is slice 2. Remaining: the sustained, randomized, long-running soak
driver with a seeded fault schedule, its bounded CI variant, and the
documented long-soak invocation.

One behavior slice 3 will want to address: `submit` round-robins over every
*running* replica, and a replica that is running but unreachable (isolated
behind a cut link) absorbs the full `submit_timeout` before the submission
is recorded as failed. That is correct — the client genuinely cannot tell
"partitioned" from "slow" — but it means offered load drops sharply while a
partition is in force. A long soak should either shorten the timeout or
track which replicas are currently answering, or it will spend most of a
partition window blocked rather than driving traffic.

[#54]: https://github.com/evelynmitchell/queso/issues/54
[#56]: https://github.com/evelynmitchell/queso/issues/56
