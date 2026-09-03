// Like `queso-net` (this crate's only path dependency into the sim-verified
// side of the workspace), this is real-I/O boundary code: real sockets,
// real wall-clock time, driving a live etcd cluster over HTTP is exactly
// this crate's job. It does not import `[workspace.lints]` for the same
// reason `queso-net` doesn't -- see this crate's `Cargo.toml` header and
// `crates/net/src/lib.rs`'s identical rationale.
#![allow(clippy::disallowed_methods)]

//! `queso-compare`: Phase 7.5 (issue #35) -- a common workload/metrics
//! harness so Queso and an alternative consensus system (etcd/Raft, the
//! primary baseline the issue asks for) can be measured through the exact
//! same request mix, rate/concurrency, and metrics pipeline. See
//! `docs/compare-etcd.md` for the full methodology writeup, how to run
//! each side, the captured Queso-side results, and the fly WAN runbook.
//!
//! # Layout
//!
//! - [`target`] -- [`target::KvTarget`], the one trait a comparison run is
//!   generic over.
//! - [`queso_target`] -- [`queso_target::QuesoTarget`], `KvTarget` over
//!   `queso_net::client::Client`.
//! - [`etcd_target`] -- [`etcd_target::EtcdTarget`], `KvTarget` over etcd's
//!   v3 gRPC-gateway JSON/HTTP API (see that module's docs for why not
//!   `etcd-client`).
//! - [`stall`] -- [`stall::StallMonitor`], a measurement of how late this
//!   process's own threads are being scheduled, so a wall-clock gap can be
//!   attributed to the cluster or to the machine (issue #107).
//! - [`workload`] -- [`workload::run_workload`], the shared closed-/open-
//!   loop load generator (mirrors `crates/net/src/bin/queso-bench.rs`) that
//!   drives any `KvTarget` and reduces the run into a
//!   `queso_net::metrics::Summary` -- reused, not reimplemented, so a
//!   `queso-compare` run's output is diffable against a `queso-bench` run's
//!   with zero schema translation.
//!
//! # Guardrails this crate observes (see `docs/compare-etcd.md`)
//!
//! - Never touches `queso-consensus`/`queso-smr`/`queso-sim` *logic* --
//!   only depends on them for types (`Command`/`Outcome`/`ClientId`,
//!   `NodeId`) and, in its tests, the same `queso_net::run_node_with_listeners`
//!   boot path `queso-net`'s own integration tests use.
//! - No test in this crate ever blocks on a real etcd being reachable --
//!   the etcd-side tests exercise [`etcd_target::EtcdTarget`] against a
//!   tiny in-process fake HTTP server (protocol correctness only) or assert
//!   it fails fast against an address nothing is listening on. `cargo test
//!   -p queso-compare` is fully self-contained.

pub mod etcd_target;
pub mod queso_target;
pub mod stall;
pub mod target;
pub mod workload;

pub use etcd_target::EtcdTarget;
pub use queso_target::QuesoTarget;
pub use stall::{StallMonitor, StallReport};
pub use target::KvTarget;
pub use workload::{run_workload, StopCondition, WorkloadConfig};
