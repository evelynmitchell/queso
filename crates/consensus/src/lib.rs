//! `queso-consensus`: Phase 1 -- abstract, single-slot QuePaxa consensus
//! (Algorithm 1 in the paper), layered atop `queso-sim`'s deterministic
//! simulation kernel.
//!
//! # Scope (Phase 1 only)
//!
//! This crate implements exactly the abstract protocol described in §4.1 of
//! the QuePaxa paper: prioritized proposals, threshold synchronous
//! broadcast (`tcast`), and the existent/common/universal (E/C/U) set
//! machinery for **one** consensus slot. It deliberately does **not**
//! implement:
//!
//! - the concrete interval summary register (ISR) or the four-phase
//!   protocol that realizes tcast under real network asynchrony (Phase 2);
//! - leader fast-path / hedging (Phase 3/5);
//! - a multi-slot replicated log or the KV application (Phase 4).
//!
//! See `docs/00-project-outline.md` for the full roadmap and
//! `docs/02-properties.md` for the property model this crate's tests target
//! (P1-P4 safety, P14 randomized termination).
//!
//! # Layout
//!
//! - [`proposal`] -- [`proposal::Proposal`], [`proposal::ProposalSet`],
//!   [`proposal::best`].
//! - [`message`] -- the one wire message type, [`message::TcastMsg`].
//! - [`node`] -- [`node::ReplicaNode`], the `queso_sim::node::Node` impl
//!   each replica runs.
//! - [`tcast`] -- the threshold synchronous broadcast primitive,
//!   [`tcast::tcast`], and how its two guarantees are realized on the
//!   harness (see that module's docs for the key design decision).
//! - [`algorithm`] -- [`algorithm::Cluster`], the Algorithm-1 driver: rounds,
//!   E/C/U, `best()`-based decision detection, and the per-replica decided
//!   flag.

pub mod algorithm;
pub mod message;
pub mod node;
pub mod proposal;
pub mod tcast;

pub use algorithm::{Cluster, ReplicaState};
pub use message::TcastMsg;
pub use proposal::{best, Proposal, ProposalSet};
pub use tcast::{tcast as tcast_step, TcastResult};
