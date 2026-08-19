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
use std::thread;
use std::time::Duration;

use queso_net::config::NodeConfig;
use queso_net::run_node_with_listeners;
use queso_sim::ids::NodeId;
use queso_smr::{ClientId, Command, Outcome};
use tokio::net::TcpListener as TokioTcpListener;

#[path = "support/mod.rs"]
mod support;
use support::{free_addr, spawn_cluster as spawn_three_node_cluster, submit_with_retry};

#[tokio::test(flavor = "multi_thread")]
async fn three_node_cluster_forms_over_tcp_and_serves_put_then_get() {
    let client_addrs = spawn_three_node_cluster(3, Some(NodeId(0)));
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
    // Bind each *live* replica's listeners up front and keep them open until
    // its node adopts them via `run_node_with_listeners` -- gap-free, so
    // there is no probe-then-drop-then-rebind window for a concurrent test
    // to steal the port (the `free_addr` TOCTOU, which flaked in CI here as
    // "Address already in use"). A never-booted replica needs only an address
    // in the peer map, never a real bind, so a plain `free_addr` probe is
    // fine for it: nothing ever listens there.
    let mut live_listeners: Vec<Option<(StdTcpListener, StdTcpListener)>> = Vec::with_capacity(n);
    let mut peer_addrs: Vec<SocketAddr> = Vec::with_capacity(n);
    let mut client_addrs: Vec<SocketAddr> = Vec::with_capacity(n);
    for i in 0..n {
        if live.contains(&i) {
            let pl = StdTcpListener::bind(("127.0.0.1", 0)).expect("bind a peer listener");
            let cl = StdTcpListener::bind(("127.0.0.1", 0)).expect("bind a client listener");
            peer_addrs.push(pl.local_addr().expect("read peer listener addr"));
            client_addrs.push(cl.local_addr().expect("read client listener addr"));
            live_listeners.push(Some((pl, cl)));
        } else {
            peer_addrs.push(free_addr());
            client_addrs.push(free_addr());
            live_listeners.push(None);
        }
    }

    let peers: BTreeMap<NodeId, String> = (0..n)
        .map(|i| (NodeId(i as u32), peer_addrs[i].to_string()))
        .collect();

    let data_dir = support::fresh_data_dir();
    for (i, slot) in live_listeners.into_iter().enumerate() {
        let Some((peer_listener, client_listener)) = slot else {
            continue; // never-booted replica
        };
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
            nemesis: None,
            persist_delay: Duration::ZERO,
            save_counter: None,
            durable_event_counter: None,
            disk_fault: None,
            tls: None,
            status_listen_addr: None,
            // Phase 9.2 (issue #56): chain-checkpoint hook off, as for any
            // ordinary run.
            chain_checkpoints: None,
        };
        thread::Builder::new()
            .name(format!("queso-node-{i}"))
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
    let client_addrs = spawn_three_node_cluster(3, None);
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
