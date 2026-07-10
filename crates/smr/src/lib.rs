//! `queso-smr`: Phase 4a -- a fault-tolerant, linearizable key-value store
//! over a multi-slot replicated log, built directly on top of
//! `queso-consensus`'s concrete per-slot protocol (the ISR + threshold-clock
//! proposer/recorder pair, including the §4.2.5 leader fast path).
//!
//! This is Queso's headline "hello world" milestone (see
//! `docs/00-project-outline.md`'s Phase 4 and `docs/02-properties.md`'s
//! property model, particularly P5-P10 and P8a/A6). The two ideas this
//! crate adds on top of single-slot consensus:
//!
//! 1. **A log is a sequence of independently-decided slots.** Chaining
//!    slots gives prefix consistency (P5), a total order across replicas
//!    (P6), and gap-free application (P7) -- see [`replica`]'s module docs
//!    for exactly how a per-replica sequential frontier makes all three
//!    hold *by construction*, not merely by testing for them.
//! 2. **A `get` is proposed as its own log event and linearizes through the
//!    log**, per Meerkat's design: if the slot a reader targets was already
//!    decided by something else, the reader is forced to catch up (apply
//!    that decision) and re-propose its `get` at the next free slot. See
//!    [`cluster`]'s module docs for the full mechanism and why it falls out
//!    of running an ordinary, completely unmodified
//!    [`queso_consensus::proposer::Proposer`] at a possibly-already-decided
//!    slot.
//!
//! # Scope (Stage 4a)
//!
//! Single cluster, single log, **crash-stop** (matching this stage's
//! instructions -- not `docs/02-properties.md`'s Phase-4 durability design
//! item, P12, which this crate treats as a **Stage 4b** follow-on with an
//! explicit seam left for it; see [`cluster`]'s module docs). No hedging
//! tuning (Phase 5), no auto-tuning (Phase 6), no reconfiguration (Phase 8).
//! Slots are processed with no pipelining -- a replica runs at most one
//! `Proposer` at a time, always at its own frontier -- noted as future work
//! in the project outline, not a correctness shortcut (log safety and
//! linearizability do not depend on pipelining).
//!
//! # Layout
//!
//! - [`command`] -- [`command::Command`] (`Put`/`Get`, both tagged
//!   `(client, seq)` per A6), [`command::ClientId`],
//!   [`command::ClientSession`] (the minimal client-session concept A6
//!   calls for).
//! - [`kv`] -- [`kv::Kv`], the in-memory KV state machine: apply commands in
//!   log order, deduplicating by `(client, seq)` (P8a). Doubles as the
//!   sequential reference specification [`linearizability`] checks
//!   candidate orderings against.
//! - [`replica`] -- [`replica::SmrNode`] (the `Node` impl each replica
//!   runs) and [`replica::ReplicaState`] (its persistent per-slot
//!   recorders, log frontier, and pending-operation queue).
//! - [`cluster`] -- [`cluster::SmrCluster`], the external driver: builds the
//!   replica set, accepts `submit`/`submit_put`/`submit_get`, runs the
//!   simulation, and exposes results/state for tests to inspect.
//! - [`linearizability`] -- a dependency-light, in-tree Wing-Gong-style
//!   linearizability checker (P8): given a history of invocation/response
//!   logical-time intervals plus observed values, brute-force/backtracking-
//!   searches for *some* total order consistent with the real-time partial
//!   order that replays correctly against [`kv::Kv`] as the sequential
//!   spec.

pub mod cluster;
pub mod command;
pub mod kv;
pub mod linearizability;
pub mod replica;

pub use cluster::SmrCluster;
pub use command::{ClientId, ClientSession, Command, Key, Value};
pub use kv::{Applied, Kv};
pub use linearizability::{history_from_records, is_linearizable, HistoryOp};
pub use replica::{OpId, OpRecord, Outcome};
