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
use queso_net::tls::TlsConfig;
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

    /// Phase 8.2a (issue #47): PEM file containing this replica's TLS
    /// certificate chain (leaf first, then any intermediates). Enables
    /// app-level TLS for both peer traffic (mutual TLS) and the
    /// client-facing listener (server-authenticated TLS) -- see
    /// `queso_net::tls`'s module docs and `crates/net/README.md`'s TLS
    /// section. All-or-nothing with `--tls-key`/`--tls-ca`: set none of the
    /// three for plaintext (the default, unchanged from before this flag
    /// existed), or all three to enable TLS; setting only some is a
    /// startup error.
    #[arg(long)]
    tls_cert: Option<PathBuf>,

    /// Phase 8.2a: PEM file containing this replica's TLS private key. See
    /// `--tls-cert`.
    #[arg(long)]
    tls_key: Option<PathBuf>,

    /// Phase 8.2a: PEM file containing the CA certificate(s) trusted to
    /// sign every cluster member's TLS certificate (and, implicitly, a
    /// client's view of this replica's own cert). See `--tls-cert`.
    #[arg(long)]
    tls_ca: Option<PathBuf>,

    /// Address to listen on for status/metrics HTTP requests (`GET
    /// /health`, `GET /ready`, `GET /metrics` -- see `queso_net::status`'s
    /// module docs). Omit to leave the status server off entirely (the
    /// default): no listener is bound, nothing is spawned, zero overhead.
    #[arg(long)]
    status_listen: Option<SocketAddr>,

    /// Phase 9.2 (issue #56): publish Chain-of-Blocks checkpoint hashes
    /// every N applied slots, served by `GET /chain` for a conformance
    /// harness (see `queso_net::chain`). Omit to leave the hook off, which
    /// is what an ordinary deployment wants -- it is test-support
    /// observability, not a production metric. Requires `--status-listen`.
    ///
    /// Every replica in the cluster must be given the *same* N, or their
    /// checkpoints land on disjoint slots and cannot be compared.
    #[arg(long, value_name = "N")]
    chain_checkpoints: Option<u64>,
}

/// All-or-nothing validation for `--tls-cert`/`--tls-key`/`--tls-ca`: `None`
/// if none were passed (plaintext, unchanged default behavior), `Some` if
/// all three were passed, or a clear startup error for any other
/// combination -- silently ignoring a partially-specified TLS flag set
/// would be exactly the kind of "looks configured but isn't" footgun this
/// crate's security-sensitive TLS support cannot afford.
fn resolve_tls_config(args: &Args) -> anyhow::Result<Option<TlsConfig>> {
    match (&args.tls_cert, &args.tls_key, &args.tls_ca) {
        (None, None, None) => Ok(None),
        (Some(cert_chain_path), Some(key_path), Some(ca_path)) => Ok(Some(TlsConfig {
            cert_chain_path: cert_chain_path.clone(),
            key_path: key_path.clone(),
            ca_path: ca_path.clone(),
        })),
        _ => anyhow::bail!(
            "--tls-cert/--tls-key/--tls-ca must be set all together or not at all \
             (got --tls-cert={:?} --tls-key={:?} --tls-ca={:?})",
            args.tls_cert,
            args.tls_key,
            args.tls_ca
        ),
    }
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
    let tls = resolve_tls_config(&args)?;

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
        // Phase 8.2a (issue #47): opt in only if all three --tls-* flags
        // were passed -- see `resolve_tls_config`.
        tls,
        // Phase 8.2 (issue #47): opt in only if `--status-listen` was
        // passed -- see `NodeConfig::status_listen_addr`'s docs.
        status_listen_addr: args.status_listen,
        // Phase 9.2 (issue #56): opt in only if `--chain-checkpoints` was
        // passed -- see `NodeConfig::chain_checkpoints`'s docs.
        chain_checkpoints: args.chain_checkpoints,
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

    /// Minimal, otherwise-valid `Args` for exercising `resolve_tls_config`
    /// in isolation -- every field but the three `--tls-*` ones is
    /// irrelevant to it.
    fn base_args() -> Args {
        Args::parse_from([
            "queso-node",
            "--id",
            "0",
            "--listen",
            "127.0.0.1:7000",
            "--client-listen",
            "127.0.0.1:8000",
            "--peer",
            "0=127.0.0.1:7000",
            "--seed",
            "1",
        ])
    }

    #[test]
    fn resolve_tls_config_is_none_when_no_tls_flag_is_set() {
        assert!(resolve_tls_config(&base_args()).unwrap().is_none());
    }

    #[test]
    fn resolve_tls_config_is_some_when_all_three_tls_flags_are_set() {
        let mut args = base_args();
        args.tls_cert = Some(PathBuf::from("cert.pem"));
        args.tls_key = Some(PathBuf::from("key.pem"));
        args.tls_ca = Some(PathBuf::from("ca.pem"));
        let tls = resolve_tls_config(&args).unwrap().unwrap();
        assert_eq!(tls.cert_chain_path, PathBuf::from("cert.pem"));
        assert_eq!(tls.key_path, PathBuf::from("key.pem"));
        assert_eq!(tls.ca_path, PathBuf::from("ca.pem"));
    }

    #[test]
    fn resolve_tls_config_rejects_a_partial_tls_flag_set() {
        let mut only_cert = base_args();
        only_cert.tls_cert = Some(PathBuf::from("cert.pem"));
        assert!(resolve_tls_config(&only_cert).is_err());

        let mut cert_and_key = base_args();
        cert_and_key.tls_cert = Some(PathBuf::from("cert.pem"));
        cert_and_key.tls_key = Some(PathBuf::from("key.pem"));
        assert!(resolve_tls_config(&cert_and_key).is_err());

        let mut only_ca = base_args();
        only_ca.tls_ca = Some(PathBuf::from("ca.pem"));
        assert!(resolve_tls_config(&only_ca).is_err());
    }
}
