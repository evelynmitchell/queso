# queso-compare

Phase 7.5 (issue #35): a common workload/metrics harness so Queso and an
alternative consensus system (etcd/Raft, the primary baseline) can be
measured through the exact same request mix, rate/concurrency, and
metrics pipeline (`queso_net::metrics`, unmodified).

**See [`docs/compare-etcd.md`](../../docs/compare-etcd.md)** for the full
methodology, how to run each side, the captured Queso-side results, the
etcd-side runbook (this sandbox cannot run a real etcd -- see that
document's "environment constraint" section), and the fly.io WAN runbook.

Quick start:

```sh
cargo build --release -p queso-compare
./target/release/queso-compare --help

# Queso side (against a local cluster booted per crates/net/README.md):
./target/release/queso-compare --target queso \
  --queso-addr 127.0.0.1:8000 --queso-addr 127.0.0.1:8001 --queso-addr 127.0.0.1:8002 \
  --concurrency 16 --read-frac 0.5 --keys 1000 --duration-secs 8 --output json

# The headline leader-DoS experiment (bounded, self-contained, no external
# etcd needed):
cargo test -p queso-compare --test leader_dos -- --nocapture
```

This crate depends on `queso-net`/`queso-smr`/`queso-sim` (path
dependencies, for types and the same in-process cluster boot path
`queso-net`'s own tests use) but never touches
`queso-consensus`/`queso-smr`/`queso-sim` *logic*, and does not import
`[workspace.lints]` for the same real-I/O-boundary reason `queso-net`
doesn't (see this crate's `Cargo.toml` header and `src/lib.rs`'s docs).
