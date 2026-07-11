# queso-net

Phase 7: a real-TCP transport and node binary that drives the sim-verified
`queso-consensus`/`queso-smr` core — completely unchanged — over a real
tokio event loop instead of `queso_sim::kernel::Kernel`'s deterministic
in-memory harness. See the crate's `src/lib.rs` docs for the architecture
and `docs/STATUS.md` §4a / issues #30/#36 for how this fits into the
project's phases.

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
> assumption is not realized over the wire yet), no client-side
> retry-to-another-replica/session library, no reconfiguration, no log
> compaction.

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
clients`), submit a `Put` and a `Get` from a fourth terminal using
`crates/net/src/bin/queso-node.rs`'s sibling client helper — the easiest
way from the CLI is a short one-off Rust program or `cargo run --example`,
but for a quick manual check you can instead drive `queso_net::client::submit`
from a scratch test, or just run this crate's own integration test (below),
which exercises the exact same path end-to-end automatically.

## Automated end-to-end tests

```sh
cargo test -p queso-net --test cluster
cargo test -p queso-net --test restart_recovery
```

`tests/cluster.rs` boots a 3-node cluster entirely in-process (each
replica on its own OS thread with its own tokio runtime, talking to the
others over real `127.0.0.1` TCP sockets — not `queso_sim::kernel::Kernel`),
waits for it to form, submits a `Put(42, 7)` to one replica and then reads
it back with a `Get(42)` from a *different* replica, and asserts the value
round-trips. A second test does the same in purely leaderless mode.

`tests/restart_recovery.rs` is issue #36's regression test: it spawns the
actual `queso-node` binary as independent OS processes, `SIGKILL`s a
**majority** of them after an acknowledged `Put`, restarts them against the
same `--data-dir`, and asserts every replica (including the two that just
rebooted) still agrees on the write — the exact scenario the audit found
broken. A second test does the easier minority-reboot case as a contrast.

## Scope (Phase 7 — see the crate docs)

Transport + node binary + fsync'd durability across process restart (see
"Status" above) + a minimal one-request-per-connection client protocol,
enough to prove the verified core runs over real TCP end-to-end and
survives a real restart without losing acknowledged writes. Explicitly
**not** in this crate yet:

- a real client library (retry-to-another-replica, session/seq management,
  connection pooling) or load generator — Phase 7.2;
- fly.io / deployment artifacts — Phase 7.3;
- fuzzing — Phase 7.4;
- TLS — Phase 7;
- group-commit/batched fsync, incremental WAL + compaction (durability is
  real but per-RPC-fsync/whole-snapshot — see "Honest limits" above) —
  Phase 8;
- cluster reconfiguration;
- the Phase 6 auto-tuned leader policy wired to a real, cross-process
  network (`queso_smr::tuning::EpochTuner` assumes a single shared
  in-process `Rc<RefCell<_>>` today — `--leader` here only supports a
  fixed leader or none).
