//! [`RemoteCluster`]: a [`CobTarget`] over `queso-node` replicas that are
//! already running somewhere else.
//!
//! The difference from `queso-soak`'s `RealCluster` is what it *cannot* do,
//! and that is the point. It does not spawn processes, does not kill them,
//! and has no turbulence proxy — it only talks to addresses it is given.
//! Under Antithesis every one of those powers belongs to the platform: it
//! owns the network, the scheduler, and the container lifecycle, and a
//! workload that injected its own faults on top would be fighting the
//! thing it is meant to be steered by.
//!
//! So this is deliberately the *thinner* of the two: the harness supplies
//! load and observation, Antithesis supplies the adversary.

use std::collections::BTreeMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::time::{Duration, Instant};

use queso_conformance::observer::Sample;
use queso_conformance::source::{samples_from_chain, CobTarget};
use queso_net::chain::parse_chain;
use queso_sim::ids::NodeId;
use queso_smr::Command;
use tokio::runtime::Runtime;

/// One replica's two client-facing addresses.
#[derive(Debug, Clone)]
pub struct Replica {
    /// Where `queso_net::client::submit` connects.
    pub client_addr: SocketAddr,
    /// Where `GET /chain` is served.
    pub status_addr: SocketAddr,
}

/// Resolve a replica host to a `SocketAddr` on `port`.
///
/// Hostname resolution matters here in a way it does not for the
/// spawned-process harnesses: under Docker Compose the replicas are
/// `queso-0`, `queso-1`, `queso-2`, resolved by container DNS rather than
/// written as literals.
///
/// A host carrying its own port is rejected rather than accepted: each
/// replica has *two* ports (client and status), so a single `host:port`
/// could only ever satisfy one of them, and silently pointing both at the
/// same port would produce a workload that submits fine and observes
/// nothing — the exact shape of failure this whole phase exists to catch.
pub fn resolve(host: &str, port: u16) -> anyhow::Result<SocketAddr> {
    anyhow::ensure!(
        !host.contains(':'),
        "--node takes a bare host ({host:?} carries a port). Each replica needs \
         two ports, so they come from --client-port/--status-port and must be \
         the same on every replica -- which is how a container topology is laid \
         out anyway. For several replicas on one machine, give them distinct \
         loopback addresses (127.0.0.1, 127.0.0.2, ...) rather than distinct ports."
    );
    let target = format!("{host}:{port}");
    target
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| anyhow::anyhow!("no address resolved for {target}"))
}

/// A [`CobTarget`] over already-running replicas.
pub struct RemoteCluster {
    runtime: Runtime,
    replicas: Vec<Replica>,
    started_at: Instant,
    next_submit: usize,
    /// Monotonic client sequence, so no submission is deduplicated away by
    /// A6/P8a.
    seq: u64,
    /// Highest checkpoint `n` already turned into a sample, per replica.
    emitted: BTreeMap<NodeId, u64>,
    submit_timeout: Duration,
    ok_submissions: u64,
    failed_submissions: u64,
}

impl RemoteCluster {
    pub fn new(replicas: Vec<Replica>, submit_timeout: Duration) -> anyhow::Result<Self> {
        anyhow::ensure!(!replicas.is_empty(), "a cluster needs at least one replica");
        Ok(Self {
            runtime: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()?,
            replicas,
            started_at: Instant::now(),
            next_submit: 0,
            seq: 0,
            emitted: BTreeMap::new(),
            submit_timeout,
            ok_submissions: 0,
            failed_submissions: 0,
        })
    }

    /// Block until every replica answers `GET /health`, or `timeout`.
    ///
    /// Returns how long it took. A cluster that has not formed cannot be
    /// meaningfully faulted, which is exactly why Antithesis wants a
    /// `setup_complete` signal rather than starting faults at container
    /// boot.
    pub fn await_ready(&self, timeout: Duration) -> anyhow::Result<Duration> {
        let deadline = Instant::now() + timeout;
        let started = Instant::now();
        for (i, replica) in self.replicas.iter().enumerate() {
            loop {
                if self.health_ok(replica.status_addr) {
                    break;
                }
                anyhow::ensure!(
                    Instant::now() < deadline,
                    "replica {i} at {} never answered GET /health within {timeout:?}",
                    replica.status_addr
                );
                std::thread::sleep(Duration::from_millis(200));
            }
        }
        Ok(started.elapsed())
    }

    fn health_ok(&self, addr: SocketAddr) -> bool {
        self.runtime.block_on(async move {
            matches!(
                queso_net::admin::http_get(addr, "/health", Duration::from_secs(2)).await,
                Ok((200, _))
            )
        })
    }

    /// Acknowledged and failed submission counts.
    ///
    /// Failures are ordinary under fault — a partitioned replica cannot
    /// answer — so these exist to prove the *opposite*: that something was
    /// actually accepted, without which a "no divergence" verdict is a
    /// statement about an empty log.
    pub fn submission_counts(&self) -> (u64, u64) {
        (self.ok_submissions, self.failed_submissions)
    }

    pub fn replica_count(&self) -> usize {
        self.replicas.len()
    }
}

impl CobTarget for RemoteCluster {
    fn replicas(&self) -> Vec<NodeId> {
        (0..self.replicas.len() as u32).map(NodeId).collect()
    }

    fn submit(&mut self, command: Command) {
        let target = self.next_submit % self.replicas.len();
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

        let addr = self.replicas[target].client_addr;
        let timeout = self.submit_timeout;
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
        std::thread::sleep(Duration::from_millis(units));
    }

    /// Milliseconds since this workload started.
    fn now(&self) -> u64 {
        self.started_at.elapsed().as_millis() as u64
    }

    /// `GET /chain` on every replica.
    ///
    /// A replica that cannot be reached contributes no sample, which is
    /// correct: the observer must not be told anything about a replica
    /// nobody can see, and its liveness check already treats "not
    /// advancing" on its own terms.
    fn poll_samples(&mut self) -> Vec<Sample> {
        let now = self.now();
        let addrs: Vec<(NodeId, SocketAddr)> = self
            .replicas
            .iter()
            .enumerate()
            .map(|(i, r)| (NodeId(i as u32), r.status_addr))
            .collect();

        let bodies: Vec<(NodeId, Option<String>)> = self.runtime.block_on(async move {
            let mut out = Vec::with_capacity(addrs.len());
            for (id, addr) in addrs {
                let body = queso_net::admin::http_get(addr, "/chain", Duration::from_secs(2))
                    .await
                    .ok()
                    .filter(|(status, _)| *status == 200)
                    .map(|(_, body)| body);
                out.push((id, body));
            }
            out
        });

        let mut samples = Vec::new();
        for (replica, body) in bodies {
            let Some(report) = body.as_deref().and_then(parse_chain) else {
                continue;
            };
            let cursor = self.emitted.entry(replica).or_insert(0);
            samples.extend(samples_from_chain(
                replica,
                now,
                report.frontier,
                &report.checkpoints,
                cursor,
            ));
        }
        samples
    }
}
