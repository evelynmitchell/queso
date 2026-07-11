// `clippy.toml`'s `disallowed-methods` list (no `Instant::now`,
// `SystemTime::now`, `thread::spawn`, `rand::thread_rng`/`rand::random`) is
// workspace-scoped -- it applies regardless of a crate's own `[lints]`
// table, so `cargo clippy --all-targets -- -D warnings` (the repo-wide gate)
// would otherwise flag `Instant::now` here too. This crate is the
// deliberate real-I/O boundary the rest of the workspace exists to be
// driven through (see the crate docs below) -- real wall-clock time is
// exactly its job, not a determinism bug -- so it opts out of that one
// lint explicitly rather than by omission. The core crates
// (queso-sim/queso-consensus/queso-smr) keep it at `deny` via
// `[lints] workspace = true` in their own `Cargo.toml`s, unchanged.
#![allow(clippy::disallowed_methods)]

//! `queso-net`: Phase 7.1 -- a real-TCP transport and node binary that
//! drives the sim-verified `queso-consensus`/`queso-smr` core, completely
//! unchanged, over a real tokio event loop instead of
//! `queso_sim::kernel::Kernel`'s deterministic in-memory harness.
//!
//! # The seam this crate builds on
//!
//! `queso_sim::node::Node<P>` methods take `&mut dyn queso_sim::node::Ctx<P>`
//! (an object-safe trait: `self_id`/`now`/`send`/`schedule_timer`/`rng`) --
//! not the sim-only `queso_sim::node::NodeCtx`. That abstraction (see that
//! crate's docs) is what makes this crate possible without touching a
//! single line of consensus/SMR *logic*: [`ctx::RealCtx`] is a second,
//! real-network implementation of the exact same `Ctx` interface, and
//! [`driver::run_node`] drives [`queso_smr::SmrNode`] -- the very type
//! `queso_smr::cluster::SmrCluster` drives over the sim kernel -- with it.
//!
//! # Why this crate is exempt from the workspace's determinism lints
//!
//! `queso-sim`/`queso-consensus`/`queso-smr` are deterministic-by
//! construction: no wall-clock reads, no `HashMap` iteration order, no
//! ambient OS randomness -- see those crates' docs and `clippy.toml`. This
//! crate is deliberately the *opposite*: it is the real-I/O boundary those
//! crates were built to be driven through, so it legitimately uses real
//! sockets ([`transport`]), real time (`tokio::time`, [`ctx::RealCtx`]'s
//! tick/real-time mapping), and a real seeded RNG. It does not import
//! `[workspace.lints]` for this reason -- see this crate's `Cargo.toml`.
//!
//! # Layout
//!
//! - [`config`] -- [`config::NodeConfig`], everything one replica needs to
//!   boot (identity, listen/peer addresses, tick duration, seed, leader).
//! - [`wire`] -- the peer-to-peer wire framing: length-delimited frames
//!   (`tokio_util::codec::LengthDelimitedCodec`) carrying bincode-encoded
//!   [`wire::WireMsg`] values (a `Hello` handshake, or an ordinary
//!   `queso_consensus::rpc::ConcreteMsg<queso_smr::Command>`).
//! - [`ctx`] -- [`ctx::RealCtx`], the real-network `Ctx` implementation.
//! - [`transport`] -- TCP dial/reconnect (outbound) and accept (inbound)
//!   loops that feed [`driver::Event`]s into a node's single inbox.
//! - [`client`] -- the minimal client-facing protocol this stage needs to
//!   prove the end-to-end path: one `Command` frame in, one `Outcome`
//!   frame out, per connection. A full client library (retries, session
//!   management, load generation) is Phase 7.2's scope, not this one's.
//! - [`driver`] -- [`driver::run_node`], the single-task event loop that
//!   owns one replica's [`queso_smr::SmrNode`] and drives it from
//!   messages/timers/client submissions, exactly as
//!   `queso_smr::cluster::SmrCluster` drives it over the sim kernel.
//! - [`persist`] -- [`persist::Store`], fsync'd, crash-consistent on-disk
//!   persistence for a replica's [`queso_smr::Durable`] state (issue #36):
//!   what makes a real process restart recover instead of coming back
//!   blank. See that module's docs for the write-before-reply ordering and
//!   the atomic-rename write scheme, and this crate's README for the
//!   current honest status of real-transport durability.

pub mod client;
pub mod config;
pub mod ctx;
pub mod driver;
pub mod persist;
pub mod transport;
pub mod wire;

pub use config::NodeConfig;
pub use driver::run_node;
