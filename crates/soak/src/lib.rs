// Real-I/O boundary code: real wall-clock time, real sockets and real OS
// processes are exactly this crate's job, so the workspace determinism
// lints (configured for the whole workspace in `clippy.toml`, and escalated
// by `-D warnings`) are neither achievable nor meaningful here. Allowed at
// the crate root, the same way `queso-net` and `queso-compare` do it and
// for the same reason -- see this crate's `Cargo.toml` header.
#![allow(clippy::disallowed_methods)]

//! `queso-soak`: Phase 9.2 (issue #56) -- the Chain-of-Blocks conformance
//! workload driven against **real `queso-node` OS processes** under
//! socket-level turbulence.
//!
//! Phase 9.1 (#55) built the workload and the divergence/liveness observers
//! and ran them in-process, where they exercise the same code the existing
//! DST suite already covers. This crate is what points them at the real
//! thing: real tokio scheduling, real sockets, real disk, real process
//! restarts, and partitions that genuinely close TCP connections.
//!
//! - [`proxy`] -- the out-of-process nemesis: a TCP turbulence proxy
//!   between peers, so a "partition" breaks real connections rather than
//!   dropping already-decoded application frames.
//! - [`evidence`] -- keeping a failed seed's data dir around, so a
//!   divergence report can be adjudicated against the replicas' durable
//!   applied logs instead of argued about (issue #73).
//! - [`cluster`] -- [`cluster::RealCluster`], which implements
//!   `queso_conformance::CobTarget` over spawned `queso-node` processes, so
//!   the 9.1 observers work unchanged against them.
//! - [`schedule`] -- the seeded, replayable fault schedule the sustained
//!   soak runs against.
//! - [`soak`] -- the sustained soak driver: schedule plus workload plus
//!   observer over a [`cluster::RealCluster`], checking safety continuously
//!   and liveness after the turbulence heals.
//!
//! # Determinism, honestly
//!
//! A 9.1 run is reproducible from `(cluster seed, workload seed)`. A run
//! here is not, and cannot be: real thread scheduling, real timers, and
//! real TCP make the interleaving irreproducible even with the same seeds.
//! The fault *schedule* is seeded and replayable; what the cluster does
//! under it is not. That is the price of testing the real implementation,
//! and it is why 9.1's deterministic harness stays as it is rather than
//! being replaced by this.
pub mod cluster;
pub mod evidence;
pub mod proxy;
pub mod schedule;
pub mod soak;

pub use cluster::RealCluster;
pub use proxy::Turbulence;
pub use schedule::{Fault, Schedule, ScheduleConfig, ScheduledFault};
pub use soak::{Soak, SoakConfig, SoakReport};
