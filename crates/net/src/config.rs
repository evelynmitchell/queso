//! [`NodeConfig`]: everything one replica needs to boot over a real TCP
//! network. Deliberately a plain data struct, independent of how it gets
//! built -- `queso-node`'s `main.rs` builds one from CLI flags (`clap`),
//! and `tests/cluster.rs` builds several directly for the in-process
//! 3-node integration test, without going anywhere near a CLI.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use queso_sim::ids::NodeId;

use crate::nemesis::Nemesis;

/// One replica's full real-network configuration.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// This replica's own id.
    pub id: NodeId,
    /// Address to listen on for peer (replica-to-replica) traffic.
    pub listen_addr: SocketAddr,
    /// Address to listen on for client `Put`/`Get` requests (see
    /// `crate::client`).
    pub client_listen_addr: SocketAddr,
    /// Every replica in the cluster's peer-listen address, keyed by id --
    /// including this replica's own entry (harmlessly unused: nothing ever
    /// dials or sends to `self.id`, see `crate::driver::run_node`). Its
    /// length is also this cluster's total replica count.
    ///
    /// Each value is a `host:port` string, not a pre-resolved
    /// [`SocketAddr`]: a literal IP (`"127.0.0.1:7000"`) is accepted
    /// directly, but a hostname (e.g. fly.io's private `.internal` DNS --
    /// see `docs/deploy-flyio.md`) is deliberately *not* resolved here at
    /// startup. It is resolved lazily, fresh on every dial attempt, by
    /// `crate::transport::spawn_peer_dialer` (via
    /// `crate::transport::resolve_peer_addr`) -- necessary because that DNS
    /// may not have propagated yet the instant this process starts, and
    /// because the address behind a given hostname can legitimately change
    /// across a peer's restart (a new fly machine, a rescheduled
    /// container, ...).
    pub peers: BTreeMap<NodeId, String>,
    /// Total replica count (`peers.len()`, kept as an explicit field since
    /// `queso_smr::SmrNode::new_fixed_leader` wants a plain `usize` and
    /// re-deriving it from `peers` at every call site would be redundant).
    pub total_replicas: usize,
    /// Fixed fast-path leader for every slot, or `None` for leaderless.
    /// Phase 6's auto-tuned leader policy is not wired to a real,
    /// cross-process network yet -- see the crate docs' scope note.
    pub leader: Option<NodeId>,
    /// How much real time one consensus "tick" is (hedging delay, retry
    /// backoff, the catch-up watchdog interval, ...) -- see
    /// `crate::ctx::RealCtx`'s docs for exactly how this maps real elapsed
    /// time to `LogicalTime`.
    pub tick: Duration,
    /// This replica's own PRNG seed (priority draws, `Ctx::rng`). Distinct
    /// replicas should use distinct seeds; unlike the sim harness there is
    /// no single shared stream to reproduce, so seeds only need to avoid
    /// correlated priorities across replicas, not reproduce a whole run.
    pub seed: u64,
    /// Directory this replica's durable state is persisted into (see
    /// `crate::persist::Store`) -- one `node-{id}.durable.bin` file per
    /// replica, so an entire cluster's replicas can safely share the same
    /// `data_dir` (as `queso-node`'s CLI default does) without colliding.
    /// Created if it does not already exist.
    pub data_dir: PathBuf,
    /// Phase 7.4's in-transport fault injector for this replica's outbound
    /// peer traffic (see `crate::nemesis`'s docs) -- `None` (the default
    /// everywhere except test/bench harnesses that build one explicitly,
    /// e.g. `tests/nemesis.rs`) is a strict no-op: `queso-node`'s CLI never
    /// sets this, so an ordinary run is unaffected. Shared (`Arc`) rather
    /// than owned since one `Nemesis` typically drives fault decisions for
    /// every replica in a scenario at once (a partition needs both sides to
    /// agree on which pairs are cut off).
    pub nemesis: Option<Arc<Nemesis>>,
    /// Phase 8.1a (issue #46) test-only instrumentation: an artificial
    /// extra delay this replica's [`crate::persist::Store`] sleeps before
    /// every blocking snapshot write it performs (see
    /// `crate::persist::Store::with_artificial_delay`'s docs). Always
    /// `Duration::ZERO` (a strict no-op) for every real `queso-node` run
    /// and every other test -- only `tests/group_commit.rs`'s
    /// write-before-reply ordering-regression guard sets this to something
    /// non-zero, deliberately making a real disk artificially slow so the
    /// P12 guarantee becomes observable in wall-clock time from outside the
    /// process. See that test's docs for why this is the only way to make
    /// "did a reply leave before its fsync completed" black-box-observable
    /// at all.
    pub persist_delay: Duration,
    /// Phase 8.1a (issue #46) test-only instrumentation: if set, this
    /// replica's [`crate::persist::Store`] uses `counter` as its shared
    /// save-count instead of its own private one (see
    /// `crate::persist::Store::with_save_counter`/`Store::save_count`'s
    /// docs), so a test can observe how many real fsync'd writes a live
    /// replica actually performed -- e.g. `tests/group_commit.rs`'s
    /// group-commit-coalescing test, which asserts that count stays far
    /// below the number of mutating events applied under concurrent load.
    /// `None` (each replica gets its own private, unobserved counter) for
    /// every real `queso-node` run and every other test.
    pub save_counter: Option<Arc<AtomicU64>>,
    /// Phase 8.1a (issue #46) test-only instrumentation: if set,
    /// `crate::driver::run_node`'s event loop increments this counter once
    /// for every dispatched [`crate::driver::Event::Message`] it applies --
    /// i.e. once per durable-mutating event, *regardless* of whether that
    /// event ended up sharing a batch (and therefore a single fsync/
    /// [`Self::save_counter`] increment) with others. Together the two
    /// counters directly prove group-commit coalescing: whenever
    /// `save_counter`'s final value is strictly less than this one, at
    /// least one batch must have coalesced more than one mutating event
    /// into a single persist -- see `tests/group_commit.rs`'s
    /// `group_commit_coalesces_fsyncs_under_concurrent_load`. `None` for
    /// every real `queso-node` run and every other test.
    pub durable_event_counter: Option<Arc<AtomicU64>>,
}
