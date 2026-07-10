//! `queso-consensus`: Phase 1 -- abstract, single-slot QuePaxa consensus
//! (Algorithm 1 in the paper) -- and Phase 2 -- the concrete protocol
//! (Algorithm 4 + the ISR) -- layered atop `queso-sim`'s deterministic
//! simulation kernel.
//!
//! # Scope
//!
//! **Phase 1** implements exactly the abstract protocol described in §4.1 of
//! the QuePaxa paper: prioritized proposals, threshold synchronous
//! broadcast (`tcast`), and the existent/common/universal (E/C/U) set
//! machinery for **one** consensus slot, driven in idealized lock-step
//! rounds.
//!
//! **Phase 2** implements the concrete protocol from §4.2: separate active
//! **proposer** / passive **recorder** roles communicating via RPC over a
//! genuinely asynchronous network, threshold logical clocks
//! (`step = 4*round + phase`), and the constant-space interval summary
//! register (ISR, Algorithm 3) with integer-max aggregation -- realizing
//! the same E/C/U relationship Phase 1 assumed a synchronous `tcast` for,
//! but reconstructed from majority-quorum ISR summaries instead (see
//! `crate::proposer`'s module docs for the safety argument). This phase is
//! still **leaderless** and **single-slot**: no leader fast path (Phase 3),
//! no hedging (Phase 5), no multi-slot log or KV application (Phase 4).
//!
//! See `docs/00-project-outline.md` for the full roadmap and
//! `docs/02-properties.md` for the property model this crate's tests target
//! (P1-P4 safety, P14 randomized termination).
//!
//! # Layout
//!
//! Phase 1:
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
//!
//! Phase 2:
//! - [`isr`] -- [`isr::Isr`], the constant-space integer interval summary
//!   register (Algorithm 3), unit-tested directly against its stale-step
//!   discard / step-advance behavior.
//! - [`rpc`] -- the concrete protocol's wire messages,
//!   [`rpc::RecordRequest`]/[`rpc::RecordResponse`]/[`rpc::ConcreteMsg`].
//! - [`recorder`] -- [`recorder::Recorder`], the passive role: one [`isr::Isr`]
//!   per slot, answering `record` RPCs.
//! - [`proposer`] -- [`proposer::Proposer`], the active role driving
//!   Algorithm 4: the four phases, quorum gathering, catch-up, and the
//!   decision rule -- see its module docs for the majority-intersection
//!   argument that preserves Agreement under full asynchrony.
//! - [`concrete`] -- [`concrete::ConcreteCluster`], the Phase-2 driver:
//!   runs every replica's proposer+recorder pair on the harness for one
//!   slot with no round barrier, purely via `Node` callbacks.

pub mod algorithm;
pub mod concrete;
pub mod isr;
pub mod message;
pub mod node;
pub mod proposal;
pub mod proposer;
pub mod recorder;
pub mod rpc;
pub mod tcast;

pub use algorithm::{Cluster, ReplicaState};
pub use concrete::ConcreteCluster;
pub use isr::{Isr, IsrSummary};
pub use message::TcastMsg;
pub use proposal::{best, Proposal, ProposalSet};
pub use proposer::Proposer;
pub use recorder::Recorder;
pub use rpc::{ConcreteMsg, RecordRequest, RecordResponse};
pub use tcast::{tcast as tcast_step, TcastResult};
