//! [`NodeConfig`]: everything one replica needs to boot over a real TCP
//! network. Deliberately a plain data struct, independent of how it gets
//! built -- `queso-node`'s `main.rs` builds one from CLI flags (`clap`),
//! and `tests/cluster.rs` builds several directly for the in-process
//! 3-node integration test, without going anywhere near a CLI.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use queso_sim::ids::NodeId;

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
}
