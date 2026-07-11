//! Shared test-harness helpers for `queso-net`'s integration tests: booting
//! a real, in-process, real-TCP N-node cluster (each replica on its own OS
//! thread with its own single-node tokio runtime -- see `tests/cluster.rs`'s
//! module docs for why that is honestly "N processes' worth of isolation"
//! rather than N tasks sharing one runtime) and waiting for it to come up.
//!
//! Lives at `tests/support/mod.rs` (not `tests/support.rs`) deliberately --
//! that path is the standard way to give integration test binaries a shared
//! module without cargo also treating `support` itself as its own test
//! binary (see the `tests/` section of the Cargo book).
//!
//! Each `tests/*.rs` file that includes this module compiles it into its
//! own separate test binary, so a helper only some of them call (e.g.
//! `submit_with_retry`, used by `tests/cluster.rs` but not `tests/bench.rs`,
//! which drives submissions through `queso_net::client::Client` instead)
//! would otherwise trip `dead_code` in whichever binary doesn't call it --
//! hence the blanket allow rather than one per unused-in-some-binary item.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use queso_net::config::NodeConfig;
use queso_net::{client, run_node};
use queso_sim::ids::NodeId;
use queso_smr::{Command, Outcome};

/// An ephemeral, currently-free localhost port. Binds and immediately
/// drops a listener to let the OS pick one -- a small, standard,
/// acceptably-raced-in-tests way to get a free port without hardcoding
/// one.
pub fn free_addr() -> SocketAddr {
    let listener = StdTcpListener::bind(("127.0.0.1", 0)).expect("bind an ephemeral port");
    listener.local_addr().expect("read back the bound address")
}

/// A fresh, never-before-used directory under the OS temp dir for one
/// test's replicas to persist their durable state into (see
/// `queso_net::persist::Store`). These particular tests never restart a
/// node, so there is nothing to clean up mid-test -- unlike
/// `tests/restart_recovery.rs`, which keeps a `tempfile::TempDir` guard
/// alive for its whole test body since it deliberately reuses the same
/// directory across a simulated restart.
pub fn fresh_data_dir() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("queso-net-test-{}-{n}", std::process::id()))
}

/// Boot an `n`-node cluster, each replica on its own thread + tokio
/// runtime, and return every replica's client-facing address.
pub fn spawn_cluster(n: usize, leader: Option<NodeId>) -> Vec<SocketAddr> {
    let peer_addrs: Vec<SocketAddr> = (0..n).map(|_| free_addr()).collect();
    let client_addrs: Vec<SocketAddr> = (0..n).map(|_| free_addr()).collect();

    let peers: BTreeMap<NodeId, String> = (0..n)
        .map(|i| (NodeId(i as u32), peer_addrs[i].to_string()))
        .collect();

    let data_dir = fresh_data_dir();
    for i in 0..n {
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
        };
        thread::Builder::new()
            .name(format!("queso-node-{i}"))
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build a per-node tokio runtime");
                // `run_node` only returns on a fatal startup error (a bind
                // failure) or once its inbox channel closes (never, in a
                // test -- the sending halves outlive the test process); any
                // `Err` here means the cluster never came up, so surface it
                // loudly instead of silently stalling the test.
                if let Err(err) = rt.block_on(run_node(config)) {
                    panic!("node {i} exited: {err:?}");
                }
            })
            .expect("spawn node thread");
    }

    client_addrs
}

/// Retry `client::submit` against `addr` until it succeeds or `timeout`
/// elapses -- covers both "the cluster hasn't finished forming yet" (peer
/// connections still dialing/reconnecting) and "this replica's client
/// listener isn't bound yet" (its thread's runtime hasn't gotten there).
pub async fn submit_with_retry(addr: SocketAddr, command: &Command, timeout: Duration) -> Outcome {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match client::submit(addr, command).await {
            Ok(outcome) => return outcome,
            Err(err) => {
                if tokio::time::Instant::now() >= deadline {
                    panic!("submit to {addr} never succeeded (last error: {err:?})");
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}
