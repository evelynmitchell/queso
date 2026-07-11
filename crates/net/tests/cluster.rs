//! Phase 7.1's acceptance test: a real 3-node localhost cluster, formed
//! over actual TCP sockets (not `queso_sim::kernel::Kernel`), serving a
//! `Put` then a `Get` -- proving the sim-verified `queso-consensus`/
//! `queso-smr` core runs, unmodified, over a real network.
//!
//! Each replica runs on its own OS thread with its own single-node tokio
//! runtime (rather than three tasks sharing one runtime), so this is
//! honestly "3 processes' worth of isolation" even though it's one test
//! binary -- nothing here relies on all three nodes being scheduled onto
//! the same runtime.

use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

use queso_net::config::NodeConfig;
use queso_net::{client, run_node};
use queso_sim::ids::NodeId;
use queso_smr::{ClientId, Command, Outcome};

/// An ephemeral, currently-free localhost port. Binds and immediately
/// drops a listener to let the OS pick one -- a small, standard,
/// acceptably-raced-in-tests way to get a free port without hardcoding
/// one.
fn free_addr() -> SocketAddr {
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
fn fresh_data_dir() -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("queso-net-test-{}-{n}", std::process::id()))
}

/// Boot a 3-node cluster, each replica on its own thread + tokio runtime,
/// and return every replica's client-facing address.
fn spawn_three_node_cluster(leader: Option<NodeId>) -> Vec<SocketAddr> {
    let n = 3;
    let peer_addrs: Vec<SocketAddr> = (0..n).map(|_| free_addr()).collect();
    let client_addrs: Vec<SocketAddr> = (0..n).map(|_| free_addr()).collect();

    let peers: BTreeMap<NodeId, SocketAddr> =
        (0..n).map(|i| (NodeId(i as u32), peer_addrs[i])).collect();

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
                // failure) or once its inbox channel closes (never, in this
                // test -- the sending halves outlive the test process);
                // any `Err` here means the cluster never came up, so
                // surface it loudly instead of silently stalling the test.
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
async fn submit_with_retry(addr: SocketAddr, command: &Command, timeout: Duration) -> Outcome {
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

#[tokio::test(flavor = "multi_thread")]
async fn three_node_cluster_forms_over_tcp_and_serves_put_then_get() {
    let client_addrs = spawn_three_node_cluster(Some(NodeId(0)));
    let timeout = Duration::from_secs(10);

    let put = Command::Put {
        client: ClientId(1),
        seq: 0,
        key: 42,
        value: 7,
    };
    let put_outcome = submit_with_retry(client_addrs[0], &put, timeout).await;
    assert_eq!(put_outcome, Outcome::Put);

    let get = Command::Get {
        client: ClientId(1),
        seq: 1,
        key: 42,
    };
    // Read from a *different* replica than the one the write went to --
    // only possible to observe correctly if the write was actually
    // replicated over the real network, not merely applied locally.
    let get_outcome = submit_with_retry(client_addrs[2], &get, timeout).await;
    assert_eq!(get_outcome, Outcome::Get(Some(7)));
}

/// Boot a 3-node cluster **configuration** (peer/client addresses for all 3
/// replicas), but only actually start the replicas whose index is in
/// `live` -- the rest never boot at all, modeling a permanently crashed
/// replica (stronger than a partition: nothing ever listens on its peer or
/// client ports). Returns every replica's client address (including the
/// down ones, which the caller must not submit to).
fn spawn_cluster_with_only(leader: Option<NodeId>, live: &[usize]) -> Vec<SocketAddr> {
    let n = 3;
    let peer_addrs: Vec<SocketAddr> = (0..n).map(|_| free_addr()).collect();
    let client_addrs: Vec<SocketAddr> = (0..n).map(|_| free_addr()).collect();

    let peers: BTreeMap<NodeId, SocketAddr> =
        (0..n).map(|i| (NodeId(i as u32), peer_addrs[i])).collect();

    let data_dir = fresh_data_dir();
    for &i in live {
        let config = NodeConfig {
            id: NodeId(i as u32),
            listen_addr: peer_addrs[i],
            client_listen_addr: client_addrs[i],
            peers: peers.clone(),
            total_replicas: n,
            leader,
            tick: Duration::from_millis(5),
            seed: 2_000 + i as u64,
            data_dir: data_dir.clone(),
        };
        thread::Builder::new()
            .name(format!("queso-node-{i}"))
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build a per-node tokio runtime");
                if let Err(err) = rt.block_on(run_node(config)) {
                    panic!("node {i} exited: {err:?}");
                }
            })
            .expect("spawn node thread");
    }

    client_addrs
}

/// Regression test for the critical review finding on this branch:
/// `RealCtx::send` used to look up `dst` in its `outbound` map
/// unconditionally, including for `dst == self_id`. Since a replica never
/// dials itself, `outbound` has no entry for its own id, so every
/// proposer's `RecordRequest` to *itself* (Meerkat's `Proposer` fans a
/// step's requests out to *all* `n` recorders, including its own --
/// `queso_consensus::proposer::Proposer::all_recorders`) was silently
/// dropped. That is invisible with the full membership live (a proposer
/// still reaches majority using only its `n-1` peers), but at the actual
/// fault-tolerance boundary -- here, a 3-node cluster with only 2
/// replicas live, so a majority of 2 is required and available only by
/// counting a live replica's *own* vote plus its one live peer's -- no
/// proposer could ever reach quorum and the cluster stalled forever.
///
/// This must fail (hang, caught by the outer timeout below) on the
/// pre-fix code and pass once `RealCtx::send` loops a self-send back
/// through the replica's own inbox instead of dropping it.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_survives_at_its_fault_tolerance_boundary() {
    // Replica 2 never boots at all -- only 0 and 1 are live, out of 3.
    let client_addrs = spawn_cluster_with_only(Some(NodeId(0)), &[0, 1]);
    let timeout = Duration::from_secs(10);

    let put = Command::Put {
        client: ClientId(1),
        seq: 0,
        key: 7,
        value: 77,
    };
    // Wrapped in its own outer timeout: if the cluster is stalled (the bug
    // this test targets), `client::submit` connects fine but then hangs
    // forever awaiting a response that never comes, so
    // `submit_with_retry`'s own internal deadline (which only re-checks
    // between *failed* connection attempts) never gets a chance to fire.
    let put_outcome =
        tokio::time::timeout(timeout, submit_with_retry(client_addrs[0], &put, timeout))
            .await
            .expect(
                "put must complete using only the live majority (2 of 3) -- \
             a proposer's own vote must count toward quorum",
            );
    assert_eq!(put_outcome, Outcome::Put);

    let get = Command::Get {
        client: ClientId(1),
        seq: 1,
        key: 7,
    };
    let get_outcome =
        tokio::time::timeout(timeout, submit_with_retry(client_addrs[1], &get, timeout))
            .await
            .expect("get must complete using only the live majority (2 of 3)");
    assert_eq!(get_outcome, Outcome::Get(Some(77)));
}

#[tokio::test(flavor = "multi_thread")]
async fn three_node_cluster_is_leaderless_capable_too() {
    let client_addrs = spawn_three_node_cluster(None);
    let timeout = Duration::from_secs(10);

    let put = Command::Put {
        client: ClientId(2),
        seq: 0,
        key: 1,
        value: 99,
    };
    let put_outcome = submit_with_retry(client_addrs[1], &put, timeout).await;
    assert_eq!(put_outcome, Outcome::Put);

    let get = Command::Get {
        client: ClientId(2),
        seq: 1,
        key: 1,
    };
    let get_outcome = submit_with_retry(client_addrs[0], &get, timeout).await;
    assert_eq!(get_outcome, Outcome::Get(Some(99)));
}
