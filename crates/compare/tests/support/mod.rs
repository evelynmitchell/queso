//! Shared test-harness helper for `queso-compare`'s integration tests:
//! boot a real, in-process, real-TCP N-node Queso cluster.
//!
//! This is a deliberate near-duplicate of
//! `crates/net/tests/support/mod.rs::spawn_cluster`/`spawn_cluster_with_nemesis`,
//! not a shared import -- `tests/` modules are private to their own crate,
//! so an external crate (this one) cannot reuse `queso-net`'s test-only
//! module directly. The pattern it reuses, per this phase's guardrails, is
//! the important part: bind every listener up front and hand the
//! already-bound `std::net::TcpListener` to `queso_net::run_node_with_listeners`,
//! which closes the "probe a free port, drop the probe, hope nobody else
//! grabs it before the real bind" TOCTOU a naive `TcpListener::bind(..).
//! local_addr()`-then-drop helper would reintroduce.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use queso_net::config::NodeConfig;
use queso_net::nemesis::Nemesis;
use queso_net::run_node_with_listeners;
use queso_sim::ids::NodeId;
use tokio::net::TcpListener as TokioTcpListener;

/// A fresh, never-before-used directory under the OS temp dir for one
/// test's replicas to persist their durable state into.
pub fn fresh_data_dir() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("queso-compare-test-{}-{n}", std::process::id()))
}

/// Boot an `n`-node cluster, each replica on its own thread + tokio
/// runtime, optionally sharing `nemesis` for its peer traffic (Phase 7.4),
/// and return every replica's client-facing address. See the module docs
/// for why every listener is bound up front rather than probed-then-dropped.
pub fn spawn_cluster(
    n: usize,
    leader: Option<NodeId>,
    nemesis: Option<Arc<Nemesis>>,
) -> Vec<SocketAddr> {
    let mut peer_listeners: Vec<StdTcpListener> = Vec::with_capacity(n);
    let mut client_listeners: Vec<StdTcpListener> = Vec::with_capacity(n);
    let mut peer_addrs: Vec<SocketAddr> = Vec::with_capacity(n);
    let mut client_addrs: Vec<SocketAddr> = Vec::with_capacity(n);
    for _ in 0..n {
        let pl = StdTcpListener::bind(("127.0.0.1", 0)).expect("bind a peer listener");
        let cl = StdTcpListener::bind(("127.0.0.1", 0)).expect("bind a client listener");
        peer_addrs.push(pl.local_addr().expect("read peer listener addr"));
        client_addrs.push(cl.local_addr().expect("read client listener addr"));
        peer_listeners.push(pl);
        client_listeners.push(cl);
    }

    let peers: BTreeMap<NodeId, String> = (0..n)
        .map(|i| (NodeId(i as u32), peer_addrs[i].to_string()))
        .collect();

    let data_dir = fresh_data_dir();
    let listeners = peer_listeners.into_iter().zip(client_listeners);
    for (i, (peer_listener, client_listener)) in listeners.enumerate() {
        let config = NodeConfig {
            id: NodeId(i as u32),
            listen_addr: peer_addrs[i],
            client_listen_addr: client_addrs[i],
            peers: peers.clone(),
            total_replicas: n,
            leader,
            tick: Duration::from_millis(5),
            seed: 1_000 + i as u64,
            data_dir: data_dir.clone(),
            nemesis: nemesis.clone(),
            // Phase 8.1a's test-only durability instrumentation (see
            // `NodeConfig::persist_delay`/`NodeConfig::save_counter`/
            // `NodeConfig::durable_event_counter`'s docs) -- this harness
            // doesn't need any of it, unlike `queso-net`'s own
            // `tests/group_commit.rs`.
            persist_delay: Duration::ZERO,
            save_counter: None,
            durable_event_counter: None,
            // Phase 8.2a (issue #47): this harness doesn't opt into TLS --
            // see `queso_net::config::NodeConfig::tls`'s docs.
            tls: None,
            // Phase 8.2's status/metrics server (`NodeConfig::status_listen_addr`)
            // is opt-in and this harness doesn't need it.
            status_listen_addr: None,
            // Phase 9.2 (issue #56): chain-checkpoint hook off.
            chain_checkpoints: None,
        };
        thread::Builder::new()
            .name(format!("queso-compare-node-{i}"))
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build a per-node tokio runtime");
                let result = rt.block_on(async move {
                    peer_listener.set_nonblocking(true)?;
                    client_listener.set_nonblocking(true)?;
                    let peer_listener = TokioTcpListener::from_std(peer_listener)?;
                    let client_listener = TokioTcpListener::from_std(client_listener)?;
                    run_node_with_listeners(config, peer_listener, client_listener).await
                });
                if let Err(err) = result {
                    panic!("node {i} exited: {err:?}");
                }
            })
            .expect("spawn node thread");
    }

    client_addrs
}
