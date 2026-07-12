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
//! - [`client`] -- the client-facing protocol: one `Command` frame in, one
//!   `Outcome` frame out, per connection ([`client::submit`]), plus the
//!   Phase 7.2 client library ([`client::Client`]) built on top of it --
//!   a pool of replica addresses and retry-to-another-replica.
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
//! - [`metrics`] -- Phase 7.2's throughput/latency metrics
//!   ([`metrics::Recorder`], [`metrics::Summary`]) used by the
//!   `queso-bench` load generator binary (`src/bin/queso-bench.rs`).
//! - [`nemesis`] -- Phase 7.4's in-transport fault injector
//!   ([`nemesis::Nemesis`]): configurable latency/jitter, frame drop,
//!   connection reset, and network partition against real peer
//!   connections, off by default (`NodeConfig::nemesis: Option<Arc<Nemesis>>`)
//!   so it never affects an ordinary `queso-node` run. See that module's
//!   docs for the fault model and this crate's README for how to run the
//!   adversarial perf harness (`tests/nemesis.rs`).
//! - [`tls`] -- Phase 8.2a (issue #47): opt-in app-level TLS
//!   (`tokio-rustls`/`rustls`, pure-Rust, no OpenSSL) for both peer<->peer
//!   traffic (mutual TLS: `NodeConfig::tls: Option<tls::TlsConfig>`) and
//!   client->replica traffic (server-authenticated TLS only:
//!   `client::ClientConfig::tls: Option<tls::ClientTlsConfig>`), off by
//!   default (`None` is a strict plaintext no-op) -- see that module's docs
//!   for exactly what is/isn't verified and this crate's README for how to
//!   enable it.
//! - [`status`] -- Phase 8.2's opt-in, off-by-default status/metrics HTTP
//!   server (`GET /health`/`/ready`/`/metrics`,
//!   `NodeConfig::status_listen_addr: Option<SocketAddr>`): a hand-rolled,
//!   dependency-light HTTP/1.1 GET responder the driver's event loop feeds
//!   via a `Send + Sync` [`status::StatusShared`] atomics snapshot -- see
//!   that module's docs for the endpoints' precise semantics and this
//!   crate's README for the CLI flag.

pub mod client;
pub mod config;
pub mod ctx;
pub mod driver;
pub mod metrics;
pub mod nemesis;
pub mod persist;
pub mod status;
pub mod tls;
pub mod transport;
pub mod wire;

pub use config::NodeConfig;
pub use driver::{run_node, run_node_with_listeners, run_node_with_status_listener};
