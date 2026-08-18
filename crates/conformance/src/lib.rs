//! `queso-conformance`: Phase 9.1 (issue #55) -- a Queso port of
//! Antithesis's [Chain-of-Blocks][cob] workload, plus the divergence and
//! liveness observers that judge it.
//!
//! # What this is for
//!
//! Queso's deterministic simulation testing verifies the consensus *logic*
//! against a mock context, in a single-threaded in-process kernel. It does
//! not run the real `queso-node` binary -- real tokio scheduling, real
//! sockets, real disk -- under fault, and the project's own bug history
//! says that is exactly where the bugs have been (#36's lost write, the
//! 7.1 self-send drop, #22's catch-up zombie). Phase 9 (#54) exists to
//! close that gap.
//!
//! This crate is **9.1: the workload and the observers**, running
//! in-process, where they can be developed and their own correctness
//! demonstrated. **9.2 (#56) is what closes the gap** -- it points these
//! same observers at real `queso-node` processes under sustained fault.
//! Nothing here should be read as having closed it: an in-process CoB run
//! exercises the same code paths the existing DST suite already does. What
//! it adds *today* is a check that survives weak observability, and a
//! liveness observer; what it adds *tomorrow* is the harness 9.2 needs.
//!
//! # Layout
//!
//! - [`chain`] -- the `(n, h)` state machine: `n += 1; h = hash(h ‖ C)`.
//! - [`observer`] -- [`observer::Observer`], which ingests `(replica, n, h)`
//!   samples and reports [`observer::Divergence`] (safety) and
//!   [`observer::Stall`] (liveness), with a per-transition log for
//!   root-causing.
//! - [`source`] -- [`source::CobTarget`], the seam a cluster implements;
//!   [`source::SimCluster`] implements it over the in-process
//!   `queso_smr::SmrCluster`, with [`source::Observability`] choosing
//!   whether the full `n -> h` table or only the frontier is reported.
//! - [`workload`] -- the stateless CoB client and the run driver.
//!
//! # Guardrails this crate observes
//!
//! - **No change to the verified core.** `queso-consensus` and `queso-smr`
//!   are used exactly as they are; CoB commands are ordinary `Put`s whose
//!   value is a payload digest (#55's preferred option (a)), and the chain
//!   is folded out here, in the harness.
//! - **Deterministic.** Opts into the workspace determinism lints; a run is
//!   reproducible from `(cluster seed, workload seed)`.
//! - **Non-vacuous by construction.** The observer counts the cross-replica
//!   comparisons it actually made ([`observer::Observer::comparisons`]), so
//!   a test can assert it checked something rather than trusting an empty
//!   divergence list. `tests/observer_detects.rs` additionally proves the
//!   observer catches divergence that is deliberately injected -- including
//!   under frontier-only observability, where the divergent `n` itself is
//!   never sampled.
//!
//! # What it cannot tell you
//!
//! See [`observer`]'s "What this observer cannot see" -- in short: nothing
//! about the gaps between samples, no causal attribution, and liveness only
//! relative to a budget and a heal-time the caller supplies.
//!
//! [cob]: https://antithesis.com/docs/resources/chain-of-blocks/

pub mod chain;
pub mod observer;
pub mod source;
pub mod workload;

pub use chain::{BlockHash, ChainState, Transition};
pub use observer::{Divergence, Observer, Sample, Stall};
pub use source::{CobTarget, Observability, SimCluster};
pub use workload::{converge, run, settle, CobWorkload, RunConfig};
