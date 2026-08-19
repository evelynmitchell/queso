//! [`RealCluster`]: `queso_conformance::CobTarget` over real `queso-node`
//! OS processes, wired through the [`crate::proxy`] turbulence mesh.
//!
//! This is the piece that lets Phase 9.1's observers -- unchanged -- judge
//! the real implementation. The seam they were built against
//! (`CobTarget`: submit, advance, now, poll_samples) maps onto real
//! processes as:
//!
//! | `CobTarget` | in-process (9.1) | here (9.2) |
//! |---|---|---|
//! | `submit` | enqueue on a replica | `queso_net::client::submit` over TCP |
//! | `advance(units)` | run the sim kernel N ticks | sleep N **milliseconds** |
//! | `now` | the sim's logical clock | milliseconds since cluster start |
//! | `poll_samples` | read every replica's applied log | `GET /chain` on every replica |
//!
//! # What the observer can see here, and what it cannot
//!
//! In-process, the harness folds the chain itself from each replica's
//! applied log and can emit every `(n, h)` a replica passed through.
//! Across a process boundary there is no such access: a replica reports the
//! chain hashes *it* folded, at the checkpoint spacing it was configured
//! with (`queso_net::chain`, Phase 9.2 slice 1). So samples here are
//! checkpoint-dense, not slot-dense -- which is exactly the observability
//! 9.1's `Observability::Checkpoints` mode was built to model, and the
//! reason the node publishes at fixed `n` rather than only its frontier.
//!
//! # Sync trait, async world
//!
//! `CobTarget` is a synchronous trait, and everything here is async. The
//! bridge is a tokio runtime owned by the cluster, with each trait method
//! `block_on`-ing its work. That means **a `RealCluster` must not be built
//! inside an async context** -- tests here are plain `#[test]` functions,
//! not `#[tokio::test]`, or `block_on` would panic on a nested runtime.

use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command as OsCommand, Stdio};
use std::time::{Duration, Instant};

use queso_chain::ChainState;
use queso_conformance::observer::Sample;
use queso_conformance::source::CobTarget;
use queso_sim::ids::NodeId;
use queso_smr::{ClientId, Command};
use tokio::runtime::Runtime;

use crate::proxy::Turbulence;

/// How a [`RealCluster`] is laid out and configured.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// Replica count. Odd numbers only, in practice: `f <= (n-1)/2`.
    pub replicas: usize,
    /// Which replica gets the leader fast path.
    pub leader: u32,
    /// Chain checkpoint spacing, passed to every node as
    /// `--chain-checkpoints`. **Identical across the cluster** -- differing
    /// spacings publish at disjoint `n` and can never be compared.
    pub checkpoint_every: u64,
    /// `--tick-ms` for each node.
    pub tick_ms: u64,
    /// How long a client submission may be retried before it is treated as
    /// failed. Failures are expected under fault and are not errors: the
    /// Chain-of-Blocks client is explicitly allowed to have proposals fail.
    pub submit_timeout: Duration,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            replicas: 3,
            leader: 0,
            checkpoint_every: 4,
            tick_ms: 5,
            submit_timeout: Duration::from_secs(5),
        }
    }
}

/// A running cluster of real `queso-node` processes behind a turbulence
/// mesh, usable as a [`CobTarget`].
///
/// Kills every child on drop, so a panicking test never leaks node
/// processes.
pub struct RealCluster {
    config: ClusterConfig,
    runtime: Runtime,
    /// Peer listen addresses of the real nodes (what the proxies forward to).
    peer_addrs: Vec<SocketAddr>,
    client_addrs: Vec<SocketAddr>,
    status_addrs: Vec<SocketAddr>,
    /// `peer_views[i][j]` is the address node `i` is told node `j` lives at
    /// -- a proxy port, never `peer_addrs[j]` directly.
    peer_views: Vec<BTreeMap<u32, String>>,
    children: Vec<Option<Child>>,
    data_dir: PathBuf,
    turbulence: Turbulence,
    started_at: Instant,
    /// Round-robin cursor for `submit`, so every live replica gets work --
    /// a Queso replica only catches up by participating (9.1's finding), so
    /// driving load at one endpoint would leave the others unobservable.
    next_submit: usize,
    /// Monotonic client sequence, so no submission is ever deduplicated
    /// away by A6/P8a.
    seq: u64,
    /// Highest checkpoint `n` already turned into a sample, per replica --
    /// so a poll reports what is new rather than the whole table again.
    emitted: BTreeMap<NodeId, u64>,
    /// Submissions that returned an error (expected under fault).
    failed_submissions: u64,
    /// Submissions that were acknowledged.
    ok_submissions: u64,
}

/// Locate the `queso-node` binary to spawn.
///
/// `CARGO_BIN_EXE_queso-node` is only set for test targets *inside*
/// `queso-net`'s own package, which this crate is not, so the path has to
/// be resolved at run time:
///
/// 1. `QUESO_NODE_BIN`, if set -- the explicit override, and what a soak
///    run against a release build or a container image should use.
/// 2. Otherwise, alongside the running test executable: Cargo puts test
///    binaries in `<target>/<profile>/deps/`, so the sibling binary is at
///    `<target>/<profile>/queso-node`.
///
/// Panics with the fix rather than a confusing spawn error if neither
/// works: the binary simply has not been built yet.
fn node_bin() -> PathBuf {
    if let Ok(explicit) = std::env::var("QUESO_NODE_BIN") {
        return PathBuf::from(explicit);
    }
    let exe = std::env::current_exe().expect("read this test executable's own path");
    let dir = exe
        .parent()
        .expect("test executable has a parent directory");
    let profile_dir = if dir.ends_with("deps") {
        dir.parent().expect("deps has a parent")
    } else {
        dir
    };
    let candidate = profile_dir.join("queso-node");
    assert!(
        candidate.exists(),
        "queso-node binary not found at {}. Build it first \
         (`cargo build -p queso-net --bin queso-node`, which `cargo build --all` \
         also does), or point QUESO_NODE_BIN at it.",
        candidate.display()
    );
    candidate
}

fn free_addr() -> SocketAddr {
    let listener = StdTcpListener::bind(("127.0.0.1", 0)).expect("bind an ephemeral port");
    listener.local_addr().expect("read back the bound address")
}

impl RealCluster {
    /// Boot a cluster, retrying the whole boot if a node dies on the way up.
    ///
    /// `free_addr` binds an ephemeral port, reads it, and drops the
    /// listener before the node binds it for real -- a TOCTOU that is
    /// harmless in isolation and genuinely lossy when the whole workspace's
    /// tests run in parallel and something else grabs the port first (issue
    /// #40 notes the same race in `queso-net`'s helpers). A node that loses
    /// that race exits immediately, so retrying with a fresh set of ports
    /// is the right response; failing the run would be blaming Queso for
    /// the harness's port allocation.
    pub fn start(config: ClusterConfig, data_dir: &Path) -> anyhow::Result<Self> {
        const ATTEMPTS: usize = 3;
        let mut last_err = None;
        for attempt in 0..ATTEMPTS {
            let mut cluster = Self::start_once(config.clone(), data_dir)?;
            match cluster.await_ready(Duration::from_secs(30)) {
                Ok(()) => return Ok(cluster),
                Err(err) => {
                    last_err = Some(format!("attempt {}: {err}", attempt + 1));
                    // `cluster` drops here, killing every child, so the
                    // next attempt starts from a clean slate.
                }
            }
        }
        anyhow::bail!(
            "cluster did not come up in {ATTEMPTS} attempts. last: {}",
            last_err.unwrap_or_else(|| "unknown".to_string())
        )
    }

    fn start_once(config: ClusterConfig, data_dir: &Path) -> anyhow::Result<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;

        let n = config.replicas;
        let peer_addrs: Vec<SocketAddr> = (0..n).map(|_| free_addr()).collect();
        let client_addrs: Vec<SocketAddr> = (0..n).map(|_| free_addr()).collect();
        let status_addrs: Vec<SocketAddr> = (0..n).map(|_| free_addr()).collect();

        let turbulence = runtime.block_on(Turbulence::spawn(&peer_addrs))?;

        // Every node sees its peers only through proxies. Its *own* entry
        // is its real listen address: a node must bind what it was told it
        // is, and it never dials itself.
        let peer_views: Vec<BTreeMap<u32, String>> = (0..n)
            .map(|i| {
                (0..n)
                    .map(|j| {
                        let addr = if i == j {
                            peer_addrs[j].to_string()
                        } else {
                            turbulence.link(i, j).listen_addr.to_string()
                        };
                        (j as u32, addr)
                    })
                    .collect()
            })
            .collect();

        let mut cluster = Self {
            config,
            runtime,
            peer_addrs,
            client_addrs,
            status_addrs,
            peer_views,
            children: (0..n).map(|_| None).collect(),
            data_dir: data_dir.to_path_buf(),
            turbulence,
            started_at: Instant::now(),
            next_submit: 0,
            seq: 0,
            emitted: BTreeMap::new(),
            failed_submissions: 0,
            ok_submissions: 0,
        };
        for i in 0..n {
            cluster.spawn(i);
        }
        Ok(cluster)
    }

    /// Boot (or reboot) replica `i` as a fresh `queso-node` process against
    /// the cluster's shared data directory, so a reboot goes through the
    /// real on-disk reload path.
    pub fn spawn(&mut self, i: usize) {
        assert!(self.children[i].is_none(), "replica {i} is already running");
        let mut cmd = OsCommand::new(node_bin());
        cmd.arg("--id")
            .arg(i.to_string())
            .arg("--seed")
            .arg((21_000 + i as u64).to_string())
            .arg("--listen")
            .arg(self.peer_addrs[i].to_string())
            .arg("--client-listen")
            .arg(self.client_addrs[i].to_string())
            .arg("--status-listen")
            .arg(self.status_addrs[i].to_string())
            .arg("--chain-checkpoints")
            .arg(self.config.checkpoint_every.to_string())
            .arg("--leader")
            .arg(self.config.leader.to_string())
            .arg("--tick-ms")
            .arg(self.config.tick_ms.to_string())
            .arg("--data-dir")
            .arg(&self.data_dir);
        for (id, addr) in &self.peer_views[i] {
            cmd.arg("--peer").arg(format!("{id}={addr}"));
        }
        // Keep stderr. A node that dies at boot -- a port that got taken
        // between `free_addr` binding it and this process binding it, a bad
        // flag -- is otherwise a silent 45-second readiness timeout with no
        // clue attached, which is exactly what this harness saw before this
        // was captured.
        let err_path = self.data_dir.join(format!("node-{i}.err"));
        let err_file = std::fs::File::create(&err_path).expect("create node stderr log");
        cmd.stdout(Stdio::null()).stderr(Stdio::from(err_file));
        self.children[i] = Some(cmd.spawn().expect("spawn queso-node subprocess"));
    }

    /// `SIGKILL` replica `i` and reap it. `Child::wait` blocks until the OS
    /// has torn the process down, so a later `spawn` rebinding its ports
    /// never races kernel cleanup.
    pub fn kill(&mut self, i: usize) {
        let mut child = self.children[i]
            .take()
            .unwrap_or_else(|| panic!("replica {i} is not running"));
        let _ = child.kill();
        let _ = child.wait();
    }

    /// Whether replica `i` currently has a live process.
    pub fn is_running(&self, i: usize) -> bool {
        self.children[i].is_some()
    }

    /// The turbulence mesh, for injecting socket-level faults.
    pub fn turbulence(&self) -> &Turbulence {
        &self.turbulence
    }

    /// Client-facing address of replica `i`.
    pub fn client_addr(&self, i: usize) -> SocketAddr {
        self.client_addrs[i]
    }

    /// Status/`/chain` address of replica `i`.
    pub fn status_addr(&self, i: usize) -> SocketAddr {
        self.status_addrs[i]
    }

    /// How many submissions were acknowledged, and how many failed. Under
    /// fault, failures are expected -- but a run in which *nothing* was
    /// ever acknowledged proved nothing, so a soak should assert on the
    /// first number.
    pub fn submission_counts(&self) -> (u64, u64) {
        (self.ok_submissions, self.failed_submissions)
    }

    /// Wait until every live replica answers `GET /health`, so a scenario
    /// does not start injecting faults into a cluster that has not booted.
    pub fn await_ready(&mut self, timeout: Duration) -> anyhow::Result<()> {
        let indices: Vec<usize> = (0..self.config.replicas)
            .filter(|&i| self.is_running(i))
            .collect();
        let deadline = std::time::Instant::now() + timeout;

        for i in indices {
            let addr = self.status_addrs[i];
            loop {
                let healthy = self
                    .runtime
                    .block_on(async { http_get(addr, "/health").await.is_some() });
                if healthy {
                    break;
                }
                // A process that has already exited will never become
                // healthy; say so now, with whatever it printed on the way
                // out, rather than burning the whole timeout.
                if let Some(status) = self.exited(i) {
                    anyhow::bail!(
                        "replica {i} exited during boot with {status}. stderr:\n{}",
                        self.stderr_tail(i)
                    );
                }
                if std::time::Instant::now() >= deadline {
                    anyhow::bail!(
                        "replica {i} at {addr} never became healthy. stderr:\n{}",
                        self.stderr_tail(i)
                    );
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
        Ok(())
    }

    /// `Some(status)` if replica `i`'s process has already exited.
    fn exited(&mut self, i: usize) -> Option<std::process::ExitStatus> {
        self.children[i]
            .as_mut()
            .and_then(|child| child.try_wait().ok().flatten())
    }

    /// The last few lines replica `i` wrote to stderr, for error messages.
    fn stderr_tail(&self, i: usize) -> String {
        let path = self.data_dir.join(format!("node-{i}.err"));
        match std::fs::read_to_string(&path) {
            Ok(text) => {
                let tail: Vec<&str> = text.lines().rev().take(10).collect();
                if tail.is_empty() {
                    "(node wrote nothing to stderr)".to_string()
                } else {
                    tail.into_iter().rev().collect::<Vec<_>>().join("\n")
                }
            }
            Err(err) => format!("(could not read {}: {err})", path.display()),
        }
    }
}

impl Drop for RealCluster {
    fn drop(&mut self) {
        for slot in &mut self.children {
            if let Some(mut child) = slot.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}

impl CobTarget for RealCluster {
    fn replicas(&self) -> Vec<NodeId> {
        (0..self.config.replicas as u32).map(NodeId).collect()
    }

    /// Submit to the next live replica, round-robin.
    ///
    /// A failure is recorded, not propagated: the Chain-of-Blocks client is
    /// stateless and its proposals are explicitly allowed to fail under
    /// fault. Judging the run is the observers' job.
    fn submit(&mut self, command: Command) {
        let live: Vec<usize> = (0..self.config.replicas)
            .filter(|&i| self.is_running(i))
            .collect();
        if live.is_empty() {
            self.failed_submissions += 1;
            return;
        }
        let target = live[self.next_submit % live.len()];
        self.next_submit = self.next_submit.wrapping_add(1);

        self.seq += 1;
        let seq = self.seq;
        let command = match command {
            Command::Put {
                client, key, value, ..
            } => Command::Put {
                client,
                seq,
                key,
                value,
            },
            Command::Get { client, key, .. } => Command::Get { client, seq, key },
        };

        let addr = self.client_addrs[target];
        let timeout = self.config.submit_timeout;
        let outcome = self.runtime.block_on(async move {
            tokio::time::timeout(timeout, queso_net::client::submit(addr, &command)).await
        });
        match outcome {
            Ok(Ok(_)) => self.ok_submissions += 1,
            _ => self.failed_submissions += 1,
        }
    }

    /// Sleep `units` **milliseconds** of real time.
    fn advance(&mut self, units: u64) {
        self.runtime
            .block_on(async move { tokio::time::sleep(Duration::from_millis(units)).await });
    }

    /// Milliseconds since the cluster booted.
    fn now(&self) -> u64 {
        self.started_at.elapsed().as_millis() as u64
    }

    /// `GET /chain` on every replica, turning newly-published checkpoints
    /// into samples.
    ///
    /// A replica that cannot be reached (killed, or isolated behind a cut
    /// link) simply contributes no sample this poll -- which is exactly
    /// right: the observer must not be told anything about a replica nobody
    /// can see, and its liveness check already treats "not advancing" on
    /// its own terms.
    fn poll_samples(&mut self) -> Vec<Sample> {
        let now = self.now();
        let addrs: Vec<(NodeId, SocketAddr)> = (0..self.config.replicas)
            .filter(|&i| self.is_running(i))
            .map(|i| (NodeId(i as u32), self.status_addrs[i]))
            .collect();

        let bodies: Vec<(NodeId, Option<String>)> = self.runtime.block_on(async move {
            let mut out = Vec::with_capacity(addrs.len());
            for (id, addr) in addrs {
                out.push((id, http_get(addr, "/chain").await));
            }
            out
        });

        let mut samples = Vec::new();
        for (replica, body) in bodies {
            let Some(body) = body else { continue };
            let Some(report) = parse_chain(&body) else {
                continue;
            };

            let cursor = self.emitted.entry(replica).or_insert(0);
            for (n, h) in report.checkpoints {
                if n > *cursor {
                    samples.push(Sample {
                        replica,
                        observed_at: now,
                        state: ChainState { n, h },
                        // A real source sees hashes, never the individual
                        // commands behind them, so the observer's
                        // per-transition log stays empty here. Documented
                        // rather than faked.
                        command_digest: None,
                    });
                    *cursor = n;
                }
            }

            // The frontier every poll, so the liveness observer can tell
            // "not moving" from "not observed" even between checkpoints.
            samples.push(Sample {
                replica,
                observed_at: now,
                state: report.frontier,
                command_digest: None,
            });
        }
        samples
    }
}

/// A parsed `/chain` body.
struct ChainReport {
    frontier: ChainState,
    checkpoints: Vec<(u64, u64)>,
}

fn parse_hash(value: &serde_json::Value) -> Option<u64> {
    let text = value.as_str()?;
    u64::from_str_radix(text.strip_prefix("0x").unwrap_or(text), 16).ok()
}

fn parse_chain(body: &str) -> Option<ChainReport> {
    let json: serde_json::Value = serde_json::from_str(body).ok()?;
    let frontier = ChainState {
        n: json["frontier"]["n"].as_u64()?,
        h: parse_hash(&json["frontier"]["h"])?,
    };
    let checkpoints = json["checkpoints"]
        .as_array()?
        .iter()
        .filter_map(|entry| Some((entry["n"].as_u64()?, parse_hash(&entry["h"])?)))
        .collect();
    Some(ChainReport {
        frontier,
        checkpoints,
    })
}

/// One `GET`, returning the body, or `None` for any failure -- an
/// unreachable replica is an ordinary condition in a soak, not an error to
/// propagate.
async fn http_get(addr: SocketAddr, path: &str) -> Option<String> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let connect = tokio::time::timeout(
        Duration::from_millis(500),
        tokio::net::TcpStream::connect(addr),
    );
    let mut stream = connect.await.ok()?.ok()?;
    let request = format!("GET {path} HTTP/1.1\r\nHost: soak\r\nConnection: close\r\n\r\n");
    stream.write_all(request.as_bytes()).await.ok()?;

    let mut raw = String::new();
    tokio::time::timeout(Duration::from_secs(2), stream.read_to_string(&mut raw))
        .await
        .ok()?
        .ok()?;
    let (head, body) = raw.split_once("\r\n\r\n")?;
    if !head.starts_with("HTTP/1.1 200") {
        return None;
    }
    Some(body.to_string())
}

/// A Chain-of-Blocks command for a real cluster: a `Put` carrying a payload
/// digest, exactly as `queso_conformance::workload` produces for the
/// in-process harness. Sequence numbers are assigned by
/// [`RealCluster::submit`].
pub fn cob_put(client: u32, key: u32, payload_digest: i64) -> Command {
    Command::Put {
        client: ClientId(client),
        seq: 0,
        key,
        value: payload_digest,
    }
}
