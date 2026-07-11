//! `queso-node`: boot one Queso replica over a real TCP network.
//!
//! See `crates/net/README.md` for how to bring up a local 3-node cluster
//! and submit a `Put`/`Get` against it.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use queso_net::config::NodeConfig;
use queso_sim::ids::NodeId;

/// Boot one Queso replica over a real TCP network.
#[derive(Parser, Debug)]
#[command(name = "queso-node", about = "Boot one Queso replica over TCP")]
struct Args {
    /// This replica's own numeric id.
    #[arg(long)]
    id: u32,

    /// Address to listen on for peer (replica-to-replica) traffic.
    #[arg(long)]
    listen: SocketAddr,

    /// Address to listen on for client Put/Get requests.
    #[arg(long)]
    client_listen: SocketAddr,

    /// One `id=host:port` entry per replica in the cluster, including this
    /// one (repeatable: `--peer 0=127.0.0.1:7000 --peer 1=127.0.0.1:7001
    /// ...`). Determines both the cluster size and every peer's dial
    /// address.
    #[arg(long = "peer", required = true)]
    peers: Vec<String>,

    /// Fixed fast-path leader's id for every slot; omit for leaderless.
    #[arg(long)]
    leader: Option<u32>,

    /// How many milliseconds one consensus "tick" (hedging delay, retry
    /// backoff, the catch-up watchdog interval, ...) maps to in real time.
    #[arg(long, default_value_t = 10)]
    tick_ms: u64,

    /// This replica's own PRNG seed (priority draws). Distinct replicas
    /// should use distinct seeds.
    #[arg(long)]
    seed: u64,

    /// Directory this replica's durable state is persisted into (fsync'd,
    /// crash-consistent -- see `queso_net::persist`). Created if it does
    /// not already exist. Every replica should get its own directory (a
    /// shared directory is safe across *replicas* -- files are keyed by
    /// `--id` -- but not across two instances of the *same* `--id`).
    #[arg(long, default_value = "data")]
    data_dir: PathBuf,
}

fn parse_peer(spec: &str) -> anyhow::Result<(NodeId, SocketAddr)> {
    let (id_str, addr_str) = spec
        .split_once('=')
        .ok_or_else(|| anyhow::anyhow!("peer spec must be id=host:port, got {spec:?}"))?;
    let id: u32 = id_str.parse()?;
    let addr: SocketAddr = addr_str.parse()?;
    Ok((NodeId(id), addr))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let mut peers = BTreeMap::new();
    for spec in &args.peers {
        let (id, addr) = parse_peer(spec)?;
        peers.insert(id, addr);
    }
    let total_replicas = peers.len();
    anyhow::ensure!(
        peers.contains_key(&NodeId(args.id)),
        "--peer list must include an entry for this replica's own --id"
    );

    let config = NodeConfig {
        id: NodeId(args.id),
        listen_addr: args.listen,
        client_listen_addr: args.client_listen,
        peers,
        total_replicas,
        leader: args.leader.map(NodeId),
        tick: Duration::from_millis(args.tick_ms),
        seed: args.seed,
        data_dir: args.data_dir,
    };

    queso_net::run_node(config).await
}
