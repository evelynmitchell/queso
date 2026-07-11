# queso-net

Phase 7.1: a real-TCP transport and node binary that drives the
sim-verified `queso-consensus`/`queso-smr` core — completely unchanged —
over a real tokio event loop instead of `queso_sim::kernel::Kernel`'s
deterministic in-memory harness. See the crate's `src/lib.rs` docs for the
architecture and `docs/STATUS.md` §4a / issue #30 for how this fits into
the project's phases.

This crate is the deliberate real-I/O boundary: real sockets, real
wall-clock time, real OS entropy. It is exempt from the workspace's
determinism lints (see `src/lib.rs`'s `#![allow(clippy::disallowed_methods)]`)
— those stay enforced at `deny` on `queso-sim`/`queso-consensus`/`queso-smr`.

## Running a local 3-node cluster by hand

Build the binary once:

```sh
cargo build -p queso-net --bin queso-node
```

Then, in three separate terminals, boot each replica. Each one needs the
full `--peer id=host:port` list (including its own entry) so it knows
every replica's peer-listen address, plus its own `--id`, `--listen`
(peer port), `--client-listen` (client port), and a `--seed`:

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

## Automated end-to-end test

```sh
cargo test -p queso-net --test cluster
```

`tests/cluster.rs` boots a 3-node cluster entirely in-process (each
replica on its own OS thread with its own tokio runtime, talking to the
others over real `127.0.0.1` TCP sockets — not `queso_sim::kernel::Kernel`),
waits for it to form, submits a `Put(42, 7)` to one replica and then reads
it back with a `Get(42)` from a *different* replica, and asserts the value
round-trips. A second test does the same in purely leaderless mode.

## Scope (Phase 7.1 only — see the crate docs)

Transport + node binary + a minimal one-request-per-connection client
protocol, enough to prove the verified core runs over real TCP end-to-end.
Explicitly **not** in this crate yet:

- a real client library (retry-to-another-replica, session/seq management,
  connection pooling) or load generator — Phase 7.2;
- fly.io / deployment artifacts — Phase 7.3;
- fuzzing — Phase 7.4;
- real fsync'd durability (still in-memory, same as the sim's `Durable`
  model) — Phase 8;
- cluster reconfiguration;
- the Phase 6 auto-tuned leader policy wired to a real, cross-process
  network (`queso_smr::tuning::EpochTuner` assumes a single shared
  in-process `Rc<RefCell<_>>` today — `--leader` here only supports a
  fixed leader or none).
