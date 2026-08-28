//! `queso-sim`: the deterministic discrete-event simulation harness for
//! Queso, Phase 0.
//!
//! This crate is the **entire Phase 0 deliverable**: an injectable virtual
//! clock, a single seeded PRNG, an in-memory network with a pluggable
//! scheduler (including two adversary classes), a fault-injection API, and
//! a trace recorder, covering the deterministic-simulation core of
//! `docs/03-testing-plan.md §1`. There is **no consensus logic here**;
//! nodes in the examples/tests just echo or count messages.
//!
//! **Not yet implemented: shrinking** (minimizing a failing seed down to a
//! smaller reproducer). This is intentionally deferred until a later phase
//! has property/consensus tests that can actually produce failing seeds to
//! shrink against — there is no such workflow yet, so there is nothing to
//! shrink.
//!
//! # Determinism, end to end
//!
//! The reproducibility contract (property **D9** in
//! `docs/02-properties.md`) is: *same seed ⇒ byte-for-byte identical event
//! trace*. This crate earns that by construction:
//!
//! 1. **Single-threaded.** [`Kernel`] never spawns a thread or touches an
//!    async executor; `clippy.toml` denies `std::thread::spawn` workspace-
//!    wide as a backstop.
//! 2. **No wall clock.** The only time type is `LogicalTime`
//!    ([`time`]), advanced solely by the kernel's event loop.
//!    `clippy.toml` denies `Instant::now`/`SystemTime::now`.
//! 3. **One seeded PRNG.** `Kernel` owns a single
//!    `rand::rngs::StdRng`, seeded once via `SeedableRng::seed_from_u64`.
//!    Every random draw anywhere in a run — scheduler delay jitter, drop
//!    coin flips, node-level randomness reached through `NodeCtx::rng` —
//!    comes from that one stream, consumed in the kernel's single dispatch
//!    order. `clippy.toml` denies `rand::thread_rng`/`rand::random`.
//! 4. **Total event order.** The event queue (`queue`, private) orders by
//!    `(LogicalTime, tiebreak_seq)`; `tiebreak_seq` is a monotonic counter
//!    assigned in call order, so ties can never depend on incidental
//!    ordering.
//! 5. **Ordered collections everywhere iteration order is observable.**
//!    The node registry and all fault-injection sets/maps are
//!    `BTreeMap`/`BTreeSet`; `clippy.toml` denies `HashMap`/`HashSet`
//!    workspace-wide.
//!
//! See the crate README for a walkthrough and `tests/reproducibility.rs`
//! for the acceptance gate this all exists to satisfy.
//!
//! # Layout
//!
//! - [`time`] — the virtual logical clock.
//! - [`ids`] — `NodeId`/`MessageId`/`TimerId` newtypes.
//! - [`payload`] — the opaque-payload boundary ([`payload::Payload`],
//!   [`payload::Inspectable`]) that makes content-oblivious vs
//!   content-aware scheduling a type-level distinction.
//! - [`network`] — [`network::EnvelopeMeta`] / [`network::Envelope`].
//! - [`scheduler`] — the `Scheduler` traits and four implementations:
//!   [`scheduler::Fifo`], [`scheduler::RandomScheduler`],
//!   [`scheduler::ContentObliviousAdversary`],
//!   [`scheduler::ContentAwareAdversary`].
//! - [`fault`] — the fault-injection API (crash/restart/partition/heal/
//!   slow-node).
//! - [`trace`] — the trace recorder ([`trace::Trace`],
//!   [`trace::TraceEvent`]).
//! - [`node`] — the [`node::Node`] trait and [`node::NodeCtx`] handle.
//! - `kernel` — [`Kernel`], tying everything above together into the DES
//!   loop.

pub mod fault;
pub mod ids;
mod kernel;
pub mod network;
pub mod node;
pub mod payload;
mod queue;
pub mod scheduler;
pub mod time;
pub mod trace;

pub use kernel::Kernel;
