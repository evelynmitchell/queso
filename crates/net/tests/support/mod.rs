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
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use queso_net::config::NodeConfig;
use queso_net::nemesis::Nemesis;
use queso_net::{client, run_node_with_listeners};
use queso_sim::ids::NodeId;
use queso_smr::{Command, Outcome};
use tokio::net::TcpListener as TokioTcpListener;

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
    spawn_cluster_inner(n, leader, None, Duration::ZERO, false, None, None)
}

/// Like [`spawn_cluster`], but every replica shares `nemesis` (Phase 7.4,
/// `queso_net::nemesis`) for its outbound peer traffic -- see
/// `tests/nemesis.rs`. Sharing one [`Nemesis`] across every replica (rather
/// than giving each its own) is deliberate: a partition/isolate call needs
/// every replica's dialer to agree on which pairs are cut off, which only
/// works if they all consult the same fault state.
pub fn spawn_cluster_with_nemesis(
    n: usize,
    leader: Option<NodeId>,
    nemesis: Arc<Nemesis>,
) -> Vec<SocketAddr> {
    spawn_cluster_inner(n, leader, Some(nemesis), Duration::ZERO, false, None, None)
}

/// Like [`spawn_cluster`], but every replica's [`queso_net::persist::Store`]
/// is built with `persist_delay` (Phase 8.1a's
/// `NodeConfig::persist_delay`) injected as an artificial extra sleep
/// before every blocking snapshot write, and -- if `save_counter`/
/// `durable_event_counter` are `Some` -- shares those counters as every
/// replica's save count / durable-mutating-event count (Phase 8.1a's
/// `NodeConfig::save_counter`/`NodeConfig::durable_event_counter`) instead
/// of each replica getting its own private, unobserved ones. Exists purely
/// for `tests/group_commit.rs`'s write-before-reply ordering guard and
/// group-commit-coalescing tests; no other test or `queso-node` itself ever
/// sets any of these.
pub fn spawn_cluster_with_persist_hooks(
    n: usize,
    leader: Option<NodeId>,
    persist_delay: Duration,
    save_counter: Option<Arc<AtomicU64>>,
    durable_event_counter: Option<Arc<AtomicU64>>,
) -> Vec<SocketAddr> {
    spawn_cluster_inner(
        n,
        leader,
        None,
        persist_delay,
        false,
        save_counter,
        durable_event_counter,
    )
}

/// Like [`spawn_cluster_with_persist_hooks`], but the artificial
/// `persist_delay` is injected into **only the leader's** `Store`, every
/// other replica's staying at `Duration::ZERO`. This is what makes
/// `tests/group_commit.rs`'s write-before-reply timing lower-bound
/// *isolating*: with every replica delayed uniformly, the unavoidable
/// recorder-round-trip fsync on the (reorder-independent) request path
/// already floors a decision's latency at `>= delay`, so the assertion
/// passes even against a genuine reply-before-persist reordering. Delaying
/// only the leader removes that confound -- the sole remaining source of a
/// `>= delay` client-`Outcome` latency is the leader's own *decisive*
/// persist, exactly the ordering the test means to prove.
pub fn spawn_cluster_with_leader_persist_delay(
    n: usize,
    leader: Option<NodeId>,
    persist_delay: Duration,
) -> Vec<SocketAddr> {
    spawn_cluster_inner(n, leader, None, persist_delay, true, None, None)
}

#[allow(clippy::too_many_arguments)]
fn spawn_cluster_inner(
    n: usize,
    leader: Option<NodeId>,
    nemesis: Option<Arc<Nemesis>>,
    persist_delay: Duration,
    persist_delay_leader_only: bool,
    save_counter: Option<Arc<AtomicU64>>,
    durable_event_counter: Option<Arc<AtomicU64>>,
) -> Vec<SocketAddr> {
    // Bind every listener up front and keep it open until the node that owns
    // it adopts it via `run_node_with_listeners`. This closes the `free_addr`
    // TOCTOU: probing a free port, dropping the probe listener, and only then
    // asking `run_node` to re-bind that address leaves a window in which a
    // concurrently-booting test cluster's own probe can grab the same port,
    // so one of the two nodes then fails to bind and its whole cluster never
    // forms (observed as a flaky "node exited" / "submit never succeeded").
    // Handing over the already-bound listener means there is never a moment
    // the port is free for anyone else to take.
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
        // When `persist_delay_leader_only`, only the leader replica gets the
        // artificial delay (see `spawn_cluster_with_leader_persist_delay`);
        // otherwise every replica gets it.
        let this_persist_delay = if persist_delay_leader_only && leader != Some(NodeId(i as u32)) {
            Duration::ZERO
        } else {
            persist_delay
        };
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
            persist_delay: this_persist_delay,
            save_counter: save_counter.clone(),
            durable_event_counter: durable_event_counter.clone(),
        };
        thread::Builder::new()
            .name(format!("queso-node-{i}"))
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build a per-node tokio runtime");
                // Returns only on a fatal startup error or once the inbox
                // closes (never, in a test -- the sending halves outlive the
                // process); any `Err` means the cluster never came up, so
                // surface it loudly rather than silently stalling the test.
                let result = rt.block_on(async move {
                    // `from_std` requires the listener be non-blocking and be
                    // called inside a runtime -- hence the conversion here,
                    // not before the thread's runtime exists.
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
