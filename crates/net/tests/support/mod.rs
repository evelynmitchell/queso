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
use queso_net::tls::TlsConfig;
use queso_net::{client, run_node_with_listeners, run_node_with_status_listener};
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
    spawn_cluster_inner(n, leader, None, Duration::ZERO, false, None, None, None)
}

/// Like [`spawn_cluster`], but every replica boots with Phase 8.2a
/// (issue #47) app-level TLS enabled -- `tls_configs[i]` is passed as
/// replica `i`'s `NodeConfig::tls` (see `crate::tls`'s module docs).
/// `tls_configs.len()` must equal `n`. Exists purely for `tests/tls.rs`; no
/// other test sets `NodeConfig::tls` at all (every one of them is the
/// crate's existing, still-unchanged plaintext behavior).
pub fn spawn_cluster_with_tls(
    n: usize,
    leader: Option<NodeId>,
    tls_configs: Vec<TlsConfig>,
) -> Vec<SocketAddr> {
    assert_eq!(
        tls_configs.len(),
        n,
        "spawn_cluster_with_tls needs exactly one TlsConfig per replica"
    );
    spawn_cluster_inner(
        n,
        leader,
        None,
        Duration::ZERO,
        false,
        None,
        None,
        Some(tls_configs),
    )
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
    spawn_cluster_inner(
        n,
        leader,
        Some(nemesis),
        Duration::ZERO,
        false,
        None,
        None,
        None,
    )
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
        None,
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
    spawn_cluster_inner(n, leader, None, persist_delay, true, None, None, None)
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
    tls_configs: Option<Vec<TlsConfig>>,
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
            // Issue #39's disk fault injection is opt-in and never used by
            // these shared helpers -- `tests/durability_faults.rs` builds
            // its own `NodeConfig`s so it can observe a node *exiting*,
            // which is exactly what these fire-and-forget threads cannot.
            disk_fault: None,
            tls: tls_configs.as_ref().map(|configs| configs[i].clone()),
            // Phase 8.2's status/metrics server is opt-in (see
            // `NodeConfig::status_listen_addr`'s docs) -- `None` here means
            // every `spawn_cluster*` variant above behaves exactly as it
            // did before that field existed. `spawn_cluster_with_status`
            // below is the one helper that opts in.
            status_listen_addr: None,
            // Phase 9.2 (issue #56): likewise opt-in, and inert without a
            // status listener -- see
            // `spawn_cluster_with_status_and_chain` for the variant that
            // turns the chain hook on.
            chain_checkpoints: None,
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

/// Like [`submit_with_retry`], but over Phase 8.2a TLS
/// (`client::submit_with_tls`) instead of plaintext -- for
/// `tests/tls.rs`'s TLS-enabled cluster, whose client listeners no longer
/// speak plaintext at all once `NodeConfig::tls` is `Some`.
pub async fn submit_with_retry_tls(
    addr: SocketAddr,
    command: &Command,
    tls: &Arc<rustls::ClientConfig>,
    timeout: Duration,
) -> Outcome {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let server_name = queso_net::tls::server_name_for(&addr.ip().to_string(), None);
        match client::submit_with_tls(addr, command, tls, server_name).await {
            Ok(outcome) => return outcome,
            Err(err) => {
                if tokio::time::Instant::now() >= deadline {
                    panic!("TLS submit to {addr} never succeeded (last error: {err:?})");
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

/// Like [`spawn_cluster`], but each replica additionally binds and serves a
/// status/metrics HTTP listener (Phase 8.2, issue #47 --
/// `queso_net::status`, `NodeConfig::status_listen_addr`). Returns
/// `(client_addrs, status_addrs)`, both in replica index order. A
/// self-contained near-duplicate of `spawn_cluster_inner` above (see this
/// crate's `crates/compare/tests/support/mod.rs` for the same kind of
/// deliberate near-duplication across a crate boundary) rather than
/// threading a fourth listener kind through every existing `spawn_cluster*`
/// variant above, none of which need one -- `tests/status.rs` is the only
/// caller.
pub fn spawn_cluster_with_status(
    n: usize,
    leader: Option<NodeId>,
) -> (Vec<SocketAddr>, Vec<SocketAddr>) {
    spawn_cluster_with_status_and_chain(n, leader, None)
}

/// As [`spawn_cluster_with_status`], but with Phase 9.2's (issue #56)
/// chain-checkpoint hook configured: `chain_checkpoints` is passed straight
/// through to every node's `NodeConfig`, so `Some(k)` makes each replica
/// publish `GET /chain` checkpoints every `k` applied slots and `None`
/// leaves the hook off (and `/chain` a 404) exactly as an ordinary run has
/// it.
///
/// Every node gets the *same* spacing, which is what a real cluster must
/// also do -- see `queso_net::chain`'s module docs.
pub fn spawn_cluster_with_status_and_chain(
    n: usize,
    leader: Option<NodeId>,
    chain_checkpoints: Option<u64>,
) -> (Vec<SocketAddr>, Vec<SocketAddr>) {
    // Gap-free bind of all three listener kinds up front, exactly like
    // `spawn_cluster_inner` already does for peer/client -- see that
    // function's docs for why (the `free_addr` TOCTOU).
    let mut peer_listeners: Vec<StdTcpListener> = Vec::with_capacity(n);
    let mut client_listeners: Vec<StdTcpListener> = Vec::with_capacity(n);
    let mut status_listeners: Vec<StdTcpListener> = Vec::with_capacity(n);
    let mut peer_addrs: Vec<SocketAddr> = Vec::with_capacity(n);
    let mut client_addrs: Vec<SocketAddr> = Vec::with_capacity(n);
    let mut status_addrs: Vec<SocketAddr> = Vec::with_capacity(n);
    for _ in 0..n {
        let pl = StdTcpListener::bind(("127.0.0.1", 0)).expect("bind a peer listener");
        let cl = StdTcpListener::bind(("127.0.0.1", 0)).expect("bind a client listener");
        let sl = StdTcpListener::bind(("127.0.0.1", 0)).expect("bind a status listener");
        peer_addrs.push(pl.local_addr().expect("read peer listener addr"));
        client_addrs.push(cl.local_addr().expect("read client listener addr"));
        status_addrs.push(sl.local_addr().expect("read status listener addr"));
        peer_listeners.push(pl);
        client_listeners.push(cl);
        status_listeners.push(sl);
    }

    let peers: BTreeMap<NodeId, String> = (0..n)
        .map(|i| (NodeId(i as u32), peer_addrs[i].to_string()))
        .collect();

    let data_dir = fresh_data_dir();
    let listeners = peer_listeners
        .into_iter()
        .zip(client_listeners)
        .zip(status_listeners)
        .map(|((p, c), s)| (p, c, s));
    for (i, (peer_listener, client_listener, status_listener)) in listeners.enumerate() {
        let config = NodeConfig {
            id: NodeId(i as u32),
            listen_addr: peer_addrs[i],
            client_listen_addr: client_addrs[i],
            peers: peers.clone(),
            total_replicas: n,
            leader,
            tick: Duration::from_millis(5),
            seed: 4_000 + i as u64,
            data_dir: data_dir.clone(),
            nemesis: None,
            persist_delay: Duration::ZERO,
            save_counter: None,
            durable_event_counter: None,
            disk_fault: None,
            // This status-server harness doesn't opt into TLS (Phase 8.2a);
            // plaintext client/peer connections, exactly like the other
            // `spawn_cluster*` variants.
            tls: None,
            // Informational only here: `run_node_with_status_listener`
            // below ignores this field entirely (the pre-bound listener it
            // takes is authoritative -- see that function's docs) and
            // always serves status. Set to the real address anyway so the
            // config, if ever logged/inspected, isn't misleadingly `None`.
            status_listen_addr: Some(status_addrs[i]),
            chain_checkpoints,
        };
        thread::Builder::new()
            .name(format!("queso-node-status-{i}"))
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("build a per-node tokio runtime");
                let result = rt.block_on(async move {
                    peer_listener.set_nonblocking(true)?;
                    client_listener.set_nonblocking(true)?;
                    status_listener.set_nonblocking(true)?;
                    let peer_listener = TokioTcpListener::from_std(peer_listener)?;
                    let client_listener = TokioTcpListener::from_std(client_listener)?;
                    let status_listener = TokioTcpListener::from_std(status_listener)?;
                    run_node_with_status_listener(
                        config,
                        peer_listener,
                        client_listener,
                        status_listener,
                    )
                    .await
                });
                if let Err(err) = result {
                    panic!("node {i} exited: {err:?}");
                }
            })
            .expect("spawn node thread");
    }

    (client_addrs, status_addrs)
}

/// Issue one plain-HTTP `GET <path>` against `addr` over a bare
/// `tokio::net::TcpStream` -- no `reqwest`, matching this crate's own
/// dependency-light philosophy (see `queso_net::status`'s module docs).
/// Returns `(status_code, body)`; `status_code` is parsed from the response
/// line's three-digit code, `body` is everything after the blank line
/// separating headers from the body (every response here is small enough,
/// and the server sends `Connection: close`, so reading to EOF is safe and
/// simple).
pub async fn http_get(addr: SocketAddr, path: &str) -> (u16, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let mut stream = TcpStream::connect(addr)
        .await
        .unwrap_or_else(|err| panic!("connect to status server at {addr}: {err}"));
    let request = format!("GET {path} HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write GET request");

    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .expect("read status server response");
    let response = String::from_utf8(response).expect("status server response is valid UTF-8");

    let mut parts = response.splitn(2, "\r\n\r\n");
    let head = parts.next().unwrap_or_default();
    let body = parts.next().unwrap_or_default().to_string();

    let status_code = head
        .lines()
        .next()
        .and_then(|status_line| status_line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .unwrap_or_else(|| panic!("could not parse a status code out of response head: {head:?}"));

    (status_code, body)
}

/// Send arbitrary (possibly malformed) bytes to the status server and return
/// the HTTP status code it answered with, or `None` if it closed the
/// connection without a parseable HTTP response (e.g. a request the server
/// timed out waiting to terminate). Used by `tests/status.rs`'s adversarial
/// parser test to confirm no malformed input crashes or wedges the node.
/// Wrapped in a client-side timeout so a hung server surfaces as a test
/// failure, not a hung test.
pub async fn raw_status_request(addr: SocketAddr, raw: &[u8]) -> Option<u16> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let fut = async {
        let mut stream = TcpStream::connect(addr).await.ok()?;
        stream.write_all(raw).await.ok()?;
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.ok()?;
        let text = String::from_utf8_lossy(&response);
        text.lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|code| code.parse::<u16>().ok())
    };
    // 10s is well past the server's own 5s request timeout; if this fires the
    // server hung, which is itself the failure the caller wants to catch.
    tokio::time::timeout(Duration::from_secs(10), fut)
        .await
        .expect(
            "status server did not respond to (or close) a raw request within 10s -- possible hang",
        )
}

// ---------------------------------------------------------------------------
// Real-process clusters
// ---------------------------------------------------------------------------

/// One cluster of real, independent `queso-node` **OS processes** sharing a
/// `--data-dir`, with helpers to `SIGKILL` and respawn any subset.
///
/// Distinct from [`spawn_cluster`] and friends above, which run each
/// replica as a thread inside this test binary. That is enough for most
/// behavior, but not for durability: an in-process "drop and rebuild the
/// `SmrNode`" restart still leaves the real disk path untested, and a
/// thread cannot be `SIGKILL`ed mid-fsync. Anything asserting what survives
/// a crash has to use this.
///
/// Extracted from `tests/restart_recovery.rs`, which had the only copy,
/// when `tests/durability_faults.rs` (issue #39) needed the same thing.
pub struct ProcCluster {
    n: usize,
    peer_addrs: Vec<SocketAddr>,
    client_addrs: Vec<SocketAddr>,
    leader: u32,
    data_dir: PathBuf,
    children: Vec<Option<std::process::Child>>,
}

impl ProcCluster {
    /// The `queso-node` binary under test. `CARGO_BIN_EXE_<target>` is set
    /// automatically for any test target in the same package as the binary,
    /// which is why this works here and needs run-time resolution over in
    /// `queso-soak` (a different package).
    pub fn node_bin() -> &'static str {
        env!("CARGO_BIN_EXE_queso-node")
    }

    /// Boot an `n`-replica cluster against `data_dir`, retrying the whole
    /// boot if a node dies immediately.
    ///
    /// The retry exists because of the `free_addr` bind-then-drop TOCTOU
    /// issue #40 documents: the port is probed, the probe listener dropped,
    /// and only then does the spawned process bind it for real. Nothing
    /// stops another test's probe taking it in between, and when that
    /// happens the node exits at once and the whole cluster silently never
    /// forms. The in-process helpers above close this properly by handing
    /// over an already-bound listener, which is impossible across a process
    /// boundary -- so retrying is the honest remedy. Blaming Queso for the
    /// harness's port allocation would be exactly the wrong signal.
    pub fn start(n: usize, leader: u32, data_dir: &std::path::Path) -> Self {
        const ATTEMPTS: usize = 3;
        for attempt in 1..=ATTEMPTS {
            let mut cluster = Self {
                n,
                peer_addrs: (0..n).map(|_| free_addr()).collect(),
                client_addrs: (0..n).map(|_| free_addr()).collect(),
                leader,
                data_dir: data_dir.to_path_buf(),
                children: (0..n).map(|_| None).collect(),
            };
            for i in 0..n {
                cluster.spawn(i);
            }
            std::thread::sleep(Duration::from_millis(200));
            if (0..n).all(|i| !cluster.exited(i)) {
                return cluster;
            }
            if attempt == ATTEMPTS {
                panic!(
                    "a queso-node process died immediately on all {ATTEMPTS} boot attempts \
                     -- this is a harness/port problem, not a Queso failure"
                );
            }
            // `cluster` drops here, killing whatever did come up, and the
            // next attempt draws fresh ports.
        }
        unreachable!("the loop either returns or panics")
    }

    /// Boot (or reboot) replica `i` as a fresh process against this
    /// cluster's shared `--data-dir` -- the same directory every previous
    /// incarnation of replica `i` wrote its snapshot into, so this goes
    /// through the real reload-on-boot path.
    pub fn spawn(&mut self, i: usize) {
        assert!(
            self.children[i].is_none(),
            "replica {i} is already running -- kill it first"
        );
        let mut cmd = std::process::Command::new(Self::node_bin());
        cmd.arg("--id")
            .arg(i.to_string())
            .arg("--seed")
            .arg((9_000 + i as u64).to_string())
            .arg("--listen")
            .arg(self.peer_addrs[i].to_string())
            .arg("--client-listen")
            .arg(self.client_addrs[i].to_string())
            .arg("--leader")
            .arg(self.leader.to_string())
            .arg("--tick-ms")
            .arg("5")
            .arg("--data-dir")
            .arg(&self.data_dir);
        for j in 0..self.n {
            cmd.arg("--peer").arg(format!("{j}={}", self.peer_addrs[j]));
        }
        // Quiet by default -- these tests assert on the client protocol's
        // observable behavior, not log output; pass through `RUST_LOG` if a
        // human wants to watch a failure locally.
        cmd.stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        self.children[i] = Some(cmd.spawn().expect("spawn queso-node subprocess"));
    }

    /// `SIGKILL` replica `i` and reap it.
    ///
    /// `Child::wait` blocks until the OS has fully torn the process down
    /// (including releasing its listening sockets), so a later `spawn`
    /// rebinding those ports never races kernel cleanup the way an
    /// in-process task-abort simulation would.
    pub fn kill(&mut self, i: usize) {
        let mut child = self.children[i]
            .take()
            .unwrap_or_else(|| panic!("replica {i} is not running"));
        child.kill().expect("SIGKILL replica");
        child.wait().expect("reap killed replica");
    }

    /// Whether replica `i`'s process has exited on its own.
    ///
    /// Note this reaps it: a node that fail-stops is gone, and the test
    /// asking this question is exactly the one that wants to know.
    pub fn exited(&mut self, i: usize) -> bool {
        match &mut self.children[i] {
            None => true,
            Some(child) => match child.try_wait().expect("poll replica process") {
                Some(_status) => {
                    self.children[i] = None;
                    true
                }
                None => false,
            },
        }
    }

    pub fn is_running(&self, i: usize) -> bool {
        self.children[i].is_some()
    }

    pub fn client_addr(&self, i: usize) -> SocketAddr {
        self.client_addrs[i]
    }

    pub fn replicas(&self) -> usize {
        self.n
    }

    /// Replica `i`'s committed snapshot file, and the temp file its writes
    /// stage through -- the two paths `queso_net::persist`'s atomic-rename
    /// scheme moves between. A durability test needs both by name to plant
    /// or inspect the states a crash can leave behind.
    pub fn snapshot_path(&self, i: usize) -> PathBuf {
        self.data_dir.join(format!("node-{i}.durable.bin"))
    }

    pub fn snapshot_tmp_path(&self, i: usize) -> PathBuf {
        self.data_dir.join(format!("node-{i}.durable.bin.tmp"))
    }
}

impl Drop for ProcCluster {
    /// Kill every still-running child, so a failing assertion mid-test never
    /// leaks orphan `queso-node` processes.
    fn drop(&mut self) {
        for slot in &mut self.children {
            if let Some(mut child) = slot.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}
