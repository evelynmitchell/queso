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

/// Parse one `--peer id=host:port` flag. `host` may be a literal IP or a
/// hostname (e.g. fly.io's private `.internal` DNS -- see
/// `docs/deploy-flyio.md`); this only validates the `host:port` *shape* (a
/// trailing `:<numeric port>`), it deliberately does not resolve `host`
/// here -- see `queso_net::config::NodeConfig::peers`'s docs for why
/// resolution happens lazily, per dial attempt, instead.
fn parse_peer(spec: &str) -> anyhow::Result<(NodeId, String)> {
    let (id_str, addr_str) = spec
        .split_once('=')
        .ok_or_else(|| anyhow::anyhow!("peer spec must be id=host:port, got {spec:?}"))?;
    let id: u32 = id_str.parse()?;
    let (_, port_str) = addr_str
        .rsplit_once(':')
        .ok_or_else(|| anyhow::anyhow!("peer address must be host:port, got {addr_str:?}"))?;
    port_str
        .parse::<u16>()
        .map_err(|_| anyhow::anyhow!("peer address port must be numeric, got {addr_str:?}"))?;
    Ok((NodeId(id), addr_str.to_string()))
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
        // Phase 7.4's in-transport nemesis (`queso_net::nemesis`) is a
        // test/bench-harness knob, not a CLI flag here -- always off for a
        // real `queso-node` run. See `NodeConfig::nemesis`'s docs.
        nemesis: None,
        // Phase 8.1a's test-only durability instrumentation (see
        // `NodeConfig::persist_delay`/`NodeConfig::save_counter`'s docs) --
        // never set for a real `queso-node` run.
        persist_delay: Duration::ZERO,
        save_counter: None,
        durable_event_counter: None,
    };

    queso_net::run_node(config).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_peer_accepts_an_ip_literal() {
        let (id, addr) = parse_peer("0=127.0.0.1:7000").unwrap();
        assert_eq!(id, NodeId(0));
        assert_eq!(addr, "127.0.0.1:7000");
    }

    #[test]
    fn parse_peer_accepts_a_hostname_like_flys_internal_dns() {
        // Not resolved here at all -- see `parse_peer`'s docs -- so this
        // must succeed even though `queso-1.internal` resolves to nothing
        // in this test process.
        let (id, addr) = parse_peer("1=queso-1.internal:7000").unwrap();
        assert_eq!(id, NodeId(1));
        assert_eq!(addr, "queso-1.internal:7000");
    }

    #[test]
    fn parse_peer_rejects_a_missing_port() {
        assert!(parse_peer("0=127.0.0.1").is_err());
    }

    #[test]
    fn parse_peer_rejects_a_non_numeric_port() {
        assert!(parse_peer("0=127.0.0.1:notaport").is_err());
    }

    #[test]
    fn parse_peer_rejects_a_missing_equals() {
        assert!(parse_peer("0-127.0.0.1:7000").is_err());
    }
}
