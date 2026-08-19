//! The out-of-process nemesis: a TCP turbulence proxy sitting between real
//! `queso-node` processes.
//!
//! # Why not `queso-net`'s nemesis
//!
//! `queso_net::nemesis` (Phase 7.4) injects faults *inside* the transport:
//! it drops and delays already-decoded application frames on their way
//! through a node's own code. That is useful, and it is also the thing
//! Phase 9 exists to stop relying on -- it exercises the app's view of the
//! network, not the network. A partition injected there never closes a
//! socket, never trips a reconnect, never makes `write` fail; the reconnect
//! and recovery paths (where this project's real bugs have been) go
//! untested.
//!
//! This proxy faults the **real socket layer** instead. Each node dials its
//! peers through a proxy port; when a link is cut, the proxy closes the live
//! connections and refuses new ones, so from the node's point of view the
//! peer's TCP connection genuinely broke and has to be re-established.
//!
//! # Why not toxiproxy / iptables / tc
//!
//! Issue #56 suggests those, and they would work. Both were rejected for
//! this harness: `toxiproxy` is an external binary this workspace would
//! have to assume is installed (the same reasoning that kept `etcd-client`
//! out of `queso-compare` -- see `docs/compare-etcd.md`), and
//! `iptables`/`tc` need root and mutate machine-wide state, which is a poor
//! trade for a test that must run in anyone's CI. A ~200-line tokio proxy
//! costs less and cuts real connections just as convincingly.
//!
//! What it does *not* reproduce, honestly: kernel-level packet loss,
//! reordering, MTU effects, or half-open connections where one side never
//! learns the peer is gone. This cuts and delays at the byte-stream level.
//! A `tc`-based nemesis remains the stronger option for anyone willing to
//! run the soak as root.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

/// One directed link `from -> to`, faultable independently of its reverse.
///
/// Directed rather than symmetric on purpose: a one-way cut (A can send to
/// B but hears nothing back) is a genuinely different failure from a clean
/// partition, and is exactly the kind of asymmetry that makes real
/// consensus implementations misbehave.
#[derive(Debug)]
pub struct Link {
    /// Address the *client* side dials (the proxy's own listener).
    pub listen_addr: SocketAddr,
    /// Address the proxy forwards to (the real peer).
    pub upstream_addr: SocketAddr,
    /// While true: refuse new connections and tear down live ones.
    cut: AtomicBool,
    /// Milliseconds of artificial delay applied to each forwarded chunk.
    latency_ms: AtomicU64,
    /// Bumped on every cut, to kill in-flight connections.
    generation: watch::Sender<u64>,
    /// Count of connections this link has accepted -- lets a test assert
    /// the proxy is actually in the path rather than silently bypassed.
    accepted: Arc<AtomicU64>,
}

impl Link {
    /// Cut the link: existing connections are dropped and new ones refused
    /// until [`Self::heal`].
    pub fn cut(&self) {
        self.cut.store(true, Ordering::SeqCst);
        // Waking the watch channel is what tears down connections already
        // pumping bytes; without it a cut would only affect *new* dials,
        // and a node with an established peer connection would sail
        // straight through the "partition".
        let next = *self.generation.borrow() + 1;
        let _ = self.generation.send(next);
    }

    /// Restore the link.
    pub fn heal(&self) {
        self.cut.store(false, Ordering::SeqCst);
    }

    /// Whether the link is currently cut.
    pub fn is_cut(&self) -> bool {
        self.cut.load(Ordering::SeqCst)
    }

    /// Delay each forwarded chunk by `ms` (0 disables).
    pub fn set_latency_ms(&self, ms: u64) {
        self.latency_ms.store(ms, Ordering::SeqCst);
    }

    /// How many connections this link has accepted since boot.
    pub fn accepted(&self) -> u64 {
        self.accepted.load(Ordering::SeqCst)
    }
}

/// A mesh of directed [`Link`]s, one per ordered pair of replicas.
///
/// Node `i` is configured to reach node `j` at `link(i, j).listen_addr`, so
/// every peer-to-peer byte crosses a proxy the harness controls.
#[derive(Debug, Default)]
pub struct Turbulence {
    links: BTreeMap<(usize, usize), Arc<Link>>,
}

impl Turbulence {
    /// Bind a proxy listener for every ordered pair `(from, to)` with
    /// `from != to`, forwarding to `upstreams[to]`, and spawn its accept
    /// loop. Returns the mesh; `link(from, to).listen_addr` is what `from`
    /// should be told `to`'s address is.
    pub async fn spawn(upstreams: &[SocketAddr]) -> anyhow::Result<Self> {
        let mut links = BTreeMap::new();
        for from in 0..upstreams.len() {
            for (to, &upstream_addr) in upstreams.iter().enumerate() {
                if from == to {
                    continue;
                }
                let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
                let listen_addr = listener.local_addr()?;
                let (generation, _) = watch::channel(0u64);
                let link = Arc::new(Link {
                    listen_addr,
                    upstream_addr,
                    cut: AtomicBool::new(false),
                    latency_ms: AtomicU64::new(0),
                    generation,
                    accepted: Arc::new(AtomicU64::new(0)),
                });
                tokio::spawn(accept_loop(listener, Arc::clone(&link)));
                links.insert((from, to), link);
            }
        }
        Ok(Self { links })
    }

    /// The link carrying `from`'s traffic to `to`.
    pub fn link(&self, from: usize, to: usize) -> &Arc<Link> {
        self.links
            .get(&(from, to))
            .unwrap_or_else(|| panic!("no link {from} -> {to}"))
    }

    /// Cut every link into and out of `node` -- a clean, symmetric
    /// partition isolating it from the rest of the cluster.
    pub fn isolate(&self, node: usize) {
        for (&(from, to), link) in &self.links {
            if from == node || to == node {
                link.cut();
            }
        }
    }

    /// Heal every link into and out of `node`.
    pub fn rejoin(&self, node: usize) {
        for (&(from, to), link) in &self.links {
            if from == node || to == node {
                link.heal();
            }
        }
    }

    /// Heal every link in the mesh.
    pub fn heal_all(&self) {
        for link in self.links.values() {
            link.heal();
        }
    }

    /// Apply `ms` of latency to every link.
    pub fn set_latency_ms(&self, ms: u64) {
        for link in self.links.values() {
            link.set_latency_ms(ms);
        }
    }

    /// Total connections accepted across the mesh -- an anti-vacuity check
    /// that peers really are talking through the proxies.
    pub fn total_accepted(&self) -> u64 {
        self.links.values().map(|link| link.accepted()).sum()
    }
}

async fn accept_loop(listener: TcpListener, link: Arc<Link>) {
    loop {
        let (downstream, _peer) = match listener.accept().await {
            Ok(pair) => pair,
            // The listener is closed when the harness drops; ending the
            // task is the correct response, not a panic.
            Err(_) => return,
        };
        link.accepted.fetch_add(1, Ordering::SeqCst);

        // A cut link accepts and immediately closes, which is what a real
        // partition looks like to a dialer: the connect either fails or the
        // connection dies at once.
        if link.is_cut() {
            drop(downstream);
            continue;
        }

        let link = Arc::clone(&link);
        tokio::spawn(async move {
            let upstream = match TcpStream::connect(link.upstream_addr).await {
                Ok(stream) => stream,
                Err(_) => return,
            };
            let mut cut_rx = link.generation.subscribe();
            tokio::select! {
                _ = pump_both(downstream, upstream, Arc::clone(&link)) => {}
                // Any cut bumps the generation, tearing this connection
                // down mid-flight.
                _ = cut_rx.changed() => {}
            }
        });
    }
}

/// Copy bytes both ways until either side closes, applying the link's
/// current latency to each chunk.
async fn pump_both(downstream: TcpStream, upstream: TcpStream, link: Arc<Link>) {
    let (mut d_read, mut d_write) = downstream.into_split();
    let (mut u_read, mut u_write) = upstream.into_split();

    let forward = {
        let link = Arc::clone(&link);
        async move { pump(&mut d_read, &mut u_write, &link).await }
    };
    let backward = async move { pump(&mut u_read, &mut d_write, &link).await };

    tokio::select! {
        _ = forward => {}
        _ = backward => {}
    }
}

async fn pump<R, W>(read: &mut R, write: &mut W, link: &Link)
where
    R: AsyncReadExt + Unpin,
    W: AsyncWriteExt + Unpin,
{
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        let n = match read.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        let latency = link.latency_ms.load(Ordering::SeqCst);
        if latency > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(latency)).await;
        }
        if link.is_cut() {
            // Checked after the read as well as at accept time: a link cut
            // while this connection was idle must not deliver the next
            // chunk that arrives.
            return;
        }
        if write.write_all(&buf[..n]).await.is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// A trivial echo server to proxy to.
    async fn echo_server() -> SocketAddr {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 1024];
                    while let Ok(n) = stream.read(&mut buf).await {
                        if n == 0 || stream.write_all(&buf[..n]).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        addr
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_open_link_forwards_bytes() {
        let upstream = echo_server().await;
        let mesh = Turbulence::spawn(&[upstream, upstream])
            .await
            .expect("mesh");
        let link = mesh.link(0, 1);

        let mut client = TcpStream::connect(link.listen_addr).await.expect("connect");
        client.write_all(b"hello").await.expect("write");
        let mut buf = [0u8; 5];
        client.read_exact(&mut buf).await.expect("read echo");
        assert_eq!(&buf, b"hello");
        assert_eq!(link.accepted(), 1, "the proxy must be in the path");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_cut_link_refuses_new_connections() {
        let upstream = echo_server().await;
        let mesh = Turbulence::spawn(&[upstream, upstream])
            .await
            .expect("mesh");
        let link = mesh.link(0, 1);
        link.cut();

        let mut client = TcpStream::connect(link.listen_addr).await.expect("connect");
        client.write_all(b"hello").await.ok();
        let mut buf = [0u8; 5];
        // The proxy accepts and immediately closes, so the read sees EOF
        // (or an error) rather than an echo.
        let result = client.read_exact(&mut buf).await;
        assert!(
            result.is_err(),
            "a cut link must not deliver bytes; got {:?}",
            std::str::from_utf8(&buf)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn cutting_tears_down_a_connection_that_is_already_open() {
        // This is the property that makes the cut a *real* partition: a
        // node with an established peer connection must lose it, not keep
        // using it.
        let upstream = echo_server().await;
        let mesh = Turbulence::spawn(&[upstream, upstream])
            .await
            .expect("mesh");
        let link = mesh.link(0, 1);

        let mut client = TcpStream::connect(link.listen_addr).await.expect("connect");
        client.write_all(b"one").await.expect("write");
        let mut buf = [0u8; 3];
        client.read_exact(&mut buf).await.expect("first echo");

        link.cut();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        client.write_all(b"two").await.ok();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            client.read_exact(&mut buf),
        )
        .await
        .expect("read should not hang past the timeout");
        assert!(
            result.is_err(),
            "an established connection must die when its link is cut"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn healing_restores_forwarding() {
        let upstream = echo_server().await;
        let mesh = Turbulence::spawn(&[upstream, upstream])
            .await
            .expect("mesh");
        let link = mesh.link(0, 1);

        link.cut();
        link.heal();

        let mut client = TcpStream::connect(link.listen_addr).await.expect("connect");
        client.write_all(b"back").await.expect("write");
        let mut buf = [0u8; 4];
        client.read_exact(&mut buf).await.expect("echo after heal");
        assert_eq!(&buf, b"back");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn isolate_cuts_both_directions_and_leaves_others_alone() {
        let upstream = echo_server().await;
        let mesh = Turbulence::spawn(&[upstream, upstream, upstream])
            .await
            .expect("mesh");

        mesh.isolate(1);

        assert!(mesh.link(0, 1).is_cut(), "into the isolated node");
        assert!(mesh.link(1, 0).is_cut(), "out of the isolated node");
        assert!(mesh.link(2, 1).is_cut());
        assert!(mesh.link(1, 2).is_cut());
        assert!(
            !mesh.link(0, 2).is_cut() && !mesh.link(2, 0).is_cut(),
            "the surviving majority's own links must stay up, or the \
             'partition' is really a cluster-wide outage"
        );

        mesh.rejoin(1);
        assert!(!mesh.link(0, 1).is_cut());
        assert!(!mesh.link(1, 0).is_cut());
    }
}
