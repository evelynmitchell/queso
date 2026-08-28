//! TCP transport: persistent, reconnecting, one-directional connections
//! between every ordered pair of replicas.
//!
//! Each replica dials every *other* replica it knows about (see
//! [`spawn_peer_dialer`]) and separately accepts inbound connections from
//! them (see [`accept_peers`]). That gives two independent, one-directional
//! TCP connections between any pair of replicas `A`/`B`: `A`'s outbound
//! connection to `B` is what `A` uses to send to `B` (and `B`'s acceptor
//! learns "this connection is `A`" from the `Hello` handshake, see
//! `crate::wire::WireMsg`), and symmetrically for `B`'s outbound connection
//! to `A`. This is simpler than a single shared bidirectional connection
//! per pair (no "who dials, who accepts" tie-breaking by id) at the cost of
//! twice as many sockets -- a non-issue at the cluster sizes this stage
//! targets.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use futures_util::{SinkExt, StreamExt};
use queso_consensus::rpc::ConcreteMsg;
use queso_sim::ids::NodeId;
use queso_smr::Command;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::{debug, info, warn};

use crate::driver::Event;
use crate::nemesis::{LinkAction, Nemesis};
use crate::tls::{server_name_for, MaybeTlsStream};
use crate::wire::{decode, encode, WireMsg};

/// How long to wait between reconnect attempts (dial failure, or an
/// established connection dropping). Fixed rather than backed-off: unlike
/// the consensus layer's own retry/backoff (which must tolerate an
/// adversarial or very-long-lived partition without hammering the network
/// forever), a dead TCP connection to a known, fixed peer address is
/// expected to come back on an ordinary timescale, and a constant short
/// delay keeps reconnection latency low and this transport's code simple --
/// exactly the kind of complexity Phase 7.1's scope note excludes (no load
/// generator / perf harness, no fancy backoff policy) in favor of Phase
/// 7.2+.
const RECONNECT_DELAY: Duration = Duration::from_millis(200);

/// How many times [`ResolveBackoff`] may double [`RECONNECT_DELAY`] before
/// it stops: `200ms << 7` = 25.6s between DNS queries for a peer whose
/// hostname simply will not resolve.
///
/// Chosen so the cap is comfortably under a minute (an operator who fixes a
/// typo'd hostname sees the cluster converge without restarting anything)
/// while dropping the steady-state query rate for a permanently-broken name
/// from ~5 Hz to ~0.04 Hz -- a factor of ~128.
const RESOLVE_BACKOFF_MAX_SHIFT: u32 = 7;

/// Capped exponential backoff for the *DNS-resolution* half of
/// [`spawn_peer_dialer`]'s retry loop (issue #42).
///
/// # Why only this half
///
/// The dial half deliberately keeps its flat [`RECONNECT_DELAY`] cadence:
/// a dead TCP connection to a peer whose address already resolved is
/// expected back on an ordinary timescale, and reconnecting fast is worth
/// more than the saved SYNs (see [`RECONNECT_DELAY`]'s own docs). Failing
/// *resolution* is a different situation. It usually means the name is
/// wrong -- an operator typo in a fly app name, say -- and a wrong name
/// stays wrong. Re-resolving it every 200ms per peer forever is a ~5 Hz
/// query loop aimed at a resolver, indefinitely, for no possible benefit.
/// It also drowns the one log line that would tell the operator what is
/// actually broken.
///
/// # Why it still converges
///
/// `reset` is the load-bearing half. The instant a name resolves, the next
/// failure starts again at [`RECONNECT_DELAY`], so a peer whose DNS is
/// merely *slow to propagate* (exactly fly.io's `.internal` at process
/// start -- see [`resolve_peer_addr`]) never inherits a long delay earned
/// during some earlier outage. Backoff is only ever paid by a name that is
/// failing right now, repeatedly.
#[derive(Debug, Default)]
struct ResolveBackoff {
    /// Resolution failures since the last success. `0` means the next
    /// failure waits exactly [`RECONNECT_DELAY`], matching the pre-#42
    /// behavior for the first attempt.
    consecutive_failures: u32,
}

impl ResolveBackoff {
    /// How long the *next* failure waits: `RECONNECT_DELAY << min(failures,
    /// RESOLVE_BACKOFF_MAX_SHIFT)`. Pure and saturating -- the `min` is
    /// what keeps a long-lived broken peer from shifting into overflow.
    fn delay(&self) -> Duration {
        let shift = self.consecutive_failures.min(RESOLVE_BACKOFF_MAX_SHIFT);
        RECONNECT_DELAY.saturating_mul(1u32 << shift)
    }

    /// Wait out this failure, then escalate for the next one.
    async fn sleep_and_escalate(&mut self) {
        tokio::time::sleep(self.delay()).await;
        // Saturating so a peer left broken for a very long time keeps
        // waiting `delay()` (already clamped by the `min` above) rather
        // than wrapping back to a tight loop.
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
    }

    /// A name resolved: forget the failure history entirely.
    fn reset(&mut self) {
        self.consecutive_failures = 0;
    }
}

/// Bound on one peer's outbound queue (see [`crate::driver::run_node`],
/// which creates the bounded channel this dialer drains). While a peer is
/// reachable this never comes close to filling -- `rx.recv().await` below
/// keeps draining it as fast as frames can be written. It only matters
/// while `TcpStream::connect` keeps failing (or the connection keeps
/// dropping) for a long time: without a bound, nothing drains `rx` during
/// that stretch and a persistent partition/crash of one peer would grow
/// this queue -- and this replica's memory -- without limit, for as long
/// as the partition lasts. Capped instead: `RealCtx::send` uses
/// `Sender::try_send` and simply drops the message when this peer's queue
/// is full (see that method's docs), which is *more* faithful to
/// `queso_sim`'s fault model, not less -- the sim genuinely, unconditionally
/// drops sends to a partitioned/crashed peer, it never queues them. Safe
/// for the same reason any other dropped message is: `queso_consensus::proposer`'s
/// own unbounded retry-with-backoff re-sends whatever a live proposer still
/// needs, so a dropped queued message is at worst superseded by a later
/// retry once this peer becomes reachable again -- never a correctness
/// requirement on this transport.
///
/// 1024 is deliberately generous relative to how bursty one replica's
/// outbound traffic to a single peer actually is (at most a handful of
/// in-flight `RecordRequest`s per proposer step, Stage 4a's "no pipelining"
/// scope, times however many slots are concurrently contested) -- large
/// enough that it is never the limiting factor during ordinary operation or
/// even a brief reconnect blip, small enough to bound worst-case memory to
/// a few thousand small messages per down peer, not unbounded growth.
pub const OUTBOUND_QUEUE_CAPACITY: usize = 1024;

/// Resolve one peer's dial target, accepting either a literal `ip:port`
/// (parsed synchronously, no DNS involved) or a `host:port` hostname
/// (resolved via async DNS through [`tokio::net::lookup_host`], taking its
/// first result). Used by [`spawn_peer_dialer`], called fresh on *every*
/// dial attempt rather than once -- see `crate::config::NodeConfig::peers`'
/// docs for why eager, startup-time resolution is not good enough for a
/// deployment like fly.io's private `.internal` DNS (see
/// `docs/deploy-flyio.md`): that DNS may not have propagated yet the
/// instant this process starts, and the address behind a hostname can
/// legitimately change across a peer's restart.
///
/// # Address family (issue #42)
///
/// **This takes whatever the resolver returns first, with no IPv4/IPv6
/// preference of its own** -- stated here because it is an assumption, not
/// an accident, and nothing else in the code says so.
///
/// It is moot on the deployment target: fly.io's `.internal`/6PN network is
/// IPv6-only, so a `.internal` name has exactly one family to return. It is
/// also moot for every local and CI cluster, which pass literal `ip:port`
/// and never reach the DNS path at all.
///
/// It would stop being moot on a dual-stack hostname, where `lookup_host`'s
/// ordering is the platform resolver's business (`getaddrinfo` applies
/// RFC 6724 source-address selection; other resolvers need not). Note what
/// the *absence* of a preference buys there: because [`spawn_peer_dialer`]
/// re-resolves on every attempt, consecutive attempts may legitimately land
/// on different families, so a host reachable over only one of them still
/// finds it. Pinning a family would make the choice deterministic and could
/// pin it to the unreachable one forever. That trade-off is why this stays
/// unopinionated until something actually dials a dual-stack peer -- at
/// which point the fix is a preference *with* a fallback, not a bare
/// preference.
pub async fn resolve_peer_addr(addr: &str) -> std::io::Result<SocketAddr> {
    if let Ok(sock_addr) = addr.parse::<SocketAddr>() {
        return Ok(sock_addr);
    }
    tokio::net::lookup_host(addr).await?.next().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("DNS lookup for {addr:?} returned no addresses"),
        )
    })
}

/// Spawn the outbound-connection manager for one peer: resolve its
/// address, dial (retrying forever on failure, and re-resolving on every
/// attempt -- see [`resolve_peer_addr`]), send a [`WireMsg::Hello`]
/// handshake identifying this replica, then forward every message enqueued
/// on `rx` as a frame -- until `rx` is closed (this replica shutting down)
/// or the connection drops, in which case it reconnects and resumes
/// draining `rx` from wherever it left off. Nothing already queued in `rx`
/// is lost across a reconnect (it is an ordinary in-process channel,
/// untouched by the socket dropping); a message can still be lost either
/// by racing an in-flight write failure, or -- while this peer is
/// unreachable for a long time -- by being dropped for exceeding
/// [`OUTBOUND_QUEUE_CAPACITY`] (see that constant's docs). Both are exactly
/// the "message drop" fault the consensus layer's own unbounded
/// retry-with-backoff (`queso_consensus::proposer`'s module docs) already
/// tolerates by design -- this transport does not need its own delivery
/// guarantees on top of that.
///
/// `peer_addr` is a `host:port` string, not a pre-resolved [`SocketAddr`]
/// -- see `crate::config::NodeConfig::peers`'s docs.
///
/// `nemesis` (Phase 7.4, `crate::nemesis`) is consulted once per outbound
/// frame, immediately before it would be written: `None` (every call site
/// outside a test/bench harness that opts in via `NodeConfig::nemesis`)
/// skips the check entirely -- see that module's docs for the fault model
/// this implements ([`LinkAction::Drop`]/[`LinkAction::ResetConnection`]
/// and [`Nemesis::delay`]'s latency/jitter). It faults on the `(self_id,
/// peer_id)` pair, so it is checked against the *logical* peer identity,
/// independent of whatever address `resolve_peer_addr` produced this dial.
///
/// `tls` (Phase 8.2a, `crate::tls`) is this replica's peer-dialing mTLS
/// client config -- `None` (every call site except a real `queso-node` run
/// or test that opts in via `NodeConfig::tls`) skips the TLS handshake
/// entirely, keeping this connection exactly the plain `TcpStream` it was
/// before this parameter existed. When `Some`, the TLS handshake (this
/// replica presenting its own client cert, verifying the acceptor's server
/// cert -- see `crate::tls::build_peer_tls`) runs immediately after
/// `TcpStream::connect` succeeds and *before* the `Hello` handshake below --
/// see `crate::tls`'s module docs for why that ordering is right.
pub fn spawn_peer_dialer(
    self_id: NodeId,
    peer_id: NodeId,
    peer_addr: String,
    mut rx: mpsc::Receiver<ConcreteMsg<Command>>,
    nemesis: Option<Arc<Nemesis>>,
    tls: Option<Arc<rustls::ClientConfig>>,
) {
    tokio::spawn(async move {
        // Only the resolution failures back off; every other retry below
        // keeps the flat `RECONNECT_DELAY` cadence on purpose -- see
        // `ResolveBackoff`'s docs for the distinction.
        let mut resolve_backoff = ResolveBackoff::default();
        loop {
            let resolved = match resolve_peer_addr(&peer_addr).await {
                Ok(addr) => {
                    resolve_backoff.reset();
                    addr
                }
                Err(err) => {
                    debug!(
                        %peer_addr, %err,
                        retry_in_ms = resolve_backoff.delay().as_millis(),
                        "peer address resolution failed, retrying"
                    );
                    resolve_backoff.sleep_and_escalate().await;
                    continue;
                }
            };
            let stream = match TcpStream::connect(resolved).await {
                Ok(s) => s,
                Err(err) => {
                    debug!(%peer_addr, %resolved, %err, "dial failed, retrying");
                    tokio::time::sleep(RECONNECT_DELAY).await;
                    continue;
                }
            };
            let _ = stream.set_nodelay(true);
            let stream: MaybeTlsStream = match &tls {
                None => MaybeTlsStream::Plain(stream),
                Some(tls_config) => {
                    // Host, not `resolved`'s IP: `server_name_for` only
                    // needs a syntactically valid name (see its docs) --
                    // the default peer verifier
                    // (`crate::tls::ChainOnlyServerCertVerifier`) never
                    // actually checks it against the presented cert.
                    let host = peer_addr
                        .rsplit_once(':')
                        .map_or(peer_addr.as_str(), |(h, _)| h);
                    let server_name = server_name_for(host, None);
                    match TlsConnector::from(tls_config.clone())
                        .connect(server_name, stream)
                        .await
                    {
                        Ok(s) => MaybeTlsStream::Tls(Box::new(s.into())),
                        Err(err) => {
                            warn!(%peer_addr, %err, "peer TLS handshake failed, retrying");
                            tokio::time::sleep(RECONNECT_DELAY).await;
                            continue;
                        }
                    }
                }
            };
            let mut framed = Framed::new(stream, LengthDelimitedCodec::new());
            if framed.send(encode(&WireMsg::Hello(self_id))).await.is_err() {
                tokio::time::sleep(RECONNECT_DELAY).await;
                continue;
            }
            info!(?self_id, %peer_addr, "connected to peer");
            loop {
                match rx.recv().await {
                    Some(msg) => {
                        // Phase 7.4 nemesis hook: a no-op (no lock, no RNG
                        // draw, no delay) whenever `nemesis` is `None` -- see
                        // this function's docs and `crate::nemesis`'s.
                        if let Some(nem) = &nemesis {
                            match nem.decide(self_id, peer_id) {
                                LinkAction::Drop => continue,
                                LinkAction::ResetConnection => {
                                    warn!(?self_id, ?peer_id, "nemesis: forcing connection reset");
                                    break;
                                }
                                LinkAction::Send => {}
                            }
                            let delay = nem.delay(self_id, peer_id);
                            if !delay.is_zero() {
                                tokio::time::sleep(delay).await;
                            }
                        }
                        if framed.send(encode(&WireMsg::App(msg))).await.is_err() {
                            warn!(%peer_addr, "write failed, reconnecting");
                            break;
                        }
                    }
                    None => return, // This replica is shutting down.
                }
            }
            tokio::time::sleep(RECONNECT_DELAY).await;
        }
    });
}

/// Accept inbound peer connections forever, spawning one reader task per
/// connection. Each connection's first frame must be a [`WireMsg::Hello`]
/// identifying the dialer; every frame after that is decoded and forwarded
/// to `inbox` as [`Event::Message`].
///
/// `tls` (Phase 8.2a, `crate::tls`) is this replica's peer-accepting mTLS
/// server config -- `None` (every call site except a real `queso-node` run
/// or test that opts in via `NodeConfig::tls`) skips the TLS handshake
/// entirely. When `Some`, every accepted connection must complete a TLS
/// handshake -- including presenting a client certificate that verifies
/// against the configured CA (see `crate::tls::build_peer_tls`; client-auth
/// is *required*, not merely offered) -- *before* the `Hello` handshake
/// below; a connection that fails the TLS handshake (no cert, or a cert
/// from an untrusted CA) never reaches `Hello` at all, and is dropped by
/// `handle_peer_connection` without ever touching `inbox`.
pub async fn accept_peers(
    listener: TcpListener,
    inbox: mpsc::UnboundedSender<Event>,
    tls: Option<Arc<rustls::ServerConfig>>,
) {
    loop {
        let (stream, addr) = match listener.accept().await {
            Ok(x) => x,
            Err(err) => {
                warn!(%err, "peer accept failed");
                continue;
            }
        };
        let _ = stream.set_nodelay(true);
        let inbox = inbox.clone();
        let tls = tls.clone();
        tokio::spawn(async move {
            handle_peer_connection(stream, addr, inbox, tls).await;
        });
    }
}

async fn handle_peer_connection(
    stream: TcpStream,
    addr: SocketAddr,
    inbox: mpsc::UnboundedSender<Event>,
    tls: Option<Arc<rustls::ServerConfig>>,
) {
    let stream: MaybeTlsStream = match tls {
        None => MaybeTlsStream::Plain(stream),
        Some(tls_config) => match TlsAcceptor::from(tls_config).accept(stream).await {
            Ok(s) => MaybeTlsStream::Tls(Box::new(s.into())),
            Err(err) => {
                // Includes the case this crate's mTLS security property
                // depends on: a dialer with no client cert, or a cert from
                // a CA `tls_config`'s client verifier does not trust, fails
                // the TLS handshake itself -- rejected right here, before
                // any `WireMsg` (in particular `Hello`) is ever read.
                warn!(%addr, %err, "peer TLS handshake failed, closing");
                return;
            }
        },
    };
    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());
    let from = match framed.next().await {
        Some(Ok(bytes)) => match decode(&bytes) {
            Ok(WireMsg::Hello(id)) => id,
            Ok(WireMsg::App(_)) => {
                warn!(%addr, "expected Hello as the first frame, closing");
                return;
            }
            Err(err) => {
                warn!(%addr, %err, "failed to decode Hello, closing");
                return;
            }
        },
        _ => return,
    };
    info!(%addr, ?from, "accepted peer connection");
    while let Some(frame) = framed.next().await {
        let bytes: BytesMut = match frame {
            Ok(b) => b,
            Err(err) => {
                warn!(%addr, ?from, %err, "read error, closing connection");
                break;
            }
        };
        match decode(&bytes) {
            Ok(WireMsg::App(payload)) => {
                if inbox.send(Event::Message { from, payload }).is_err() {
                    return; // This replica is shutting down.
                }
            }
            Ok(WireMsg::Hello(_)) => {
                warn!(%addr, ?from, "unexpected repeated Hello, ignoring");
            }
            Err(err) => {
                warn!(%addr, ?from, %err, "decode error, dropping frame");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A literal `ip:port` peer address (the only kind used by every
    /// existing local/CI cluster -- see `crates/net/README.md`) must
    /// resolve without touching DNS at all, i.e. this must succeed even
    /// with no network access.
    #[tokio::test]
    async fn resolve_peer_addr_accepts_ip_literal_without_dns() {
        let resolved = resolve_peer_addr("127.0.0.1:7000").await.unwrap();
        assert_eq!(resolved, "127.0.0.1:7000".parse::<SocketAddr>().unwrap());
    }

    /// A hostname (fly.io's `.internal` DNS in production -- see
    /// `docs/deploy-flyio.md`) must go through the async DNS fallback
    /// instead of failing the synchronous `SocketAddr` parse.
    /// `localhost` is used here (rather than a real `.internal` name) so
    /// this test resolves via the machine's own hosts file/resolver and
    /// needs no external network access.
    #[tokio::test]
    async fn resolve_peer_addr_resolves_a_hostname_via_dns() {
        let resolved = resolve_peer_addr("localhost:7000").await.unwrap();
        assert_eq!(resolved.port(), 7000);
        assert!(resolved.ip().is_loopback());
    }

    /// An address DNS genuinely cannot resolve must be a clean error, not
    /// a panic or a hang -- `spawn_peer_dialer`'s loop depends on this to
    /// keep retrying rather than getting stuck. The exact `ErrorKind`
    /// varies by platform/resolver (a real NXDOMAIN vs. no resolver
    /// configured at all in a sandboxed test environment), so this only
    /// asserts that resolution fails cleanly, not which `ErrorKind` it
    /// fails with.
    #[tokio::test]
    async fn resolve_peer_addr_rejects_an_unresolvable_hostname() {
        let result = resolve_peer_addr("this-host-does-not-exist.invalid:7000").await;
        assert!(result.is_err());
    }

    /// A literal IPv6 `[addr]:port` must take the same synchronous parse
    /// path an IPv4 literal does, never DNS. This is the form fly.io's
    /// IPv6-only 6PN network produces, so it has to work with no resolver
    /// at all -- and it is the one address family `resolve_peer_addr`'s
    /// "no explicit preference" note (issue #42) says the deployment
    /// target actually uses.
    #[tokio::test]
    async fn resolve_peer_addr_accepts_an_ipv6_literal_without_dns() {
        let resolved = resolve_peer_addr("[::1]:7000").await.unwrap();
        assert_eq!(resolved, "[::1]:7000".parse::<SocketAddr>().unwrap());
        assert!(resolved.is_ipv6(), "{resolved} should have stayed IPv6");
    }

    /// The backoff schedule itself: each successive failure doubles
    /// `RECONNECT_DELAY`, starting *at* `RECONNECT_DELAY` so the first
    /// failure waits exactly what it waited before issue #42.
    #[test]
    fn resolve_backoff_doubles_from_the_flat_reconnect_delay() {
        let mut backoff = ResolveBackoff::default();
        let mut expected = RECONNECT_DELAY;
        for failure in 0..RESOLVE_BACKOFF_MAX_SHIFT {
            assert_eq!(
                backoff.delay(),
                expected,
                "failure #{failure} should wait {expected:?}"
            );
            backoff.consecutive_failures += 1;
            expected *= 2;
        }
    }

    /// The cap holds, and holds *forever*: a peer left broken for a very
    /// long time must keep waiting the capped delay rather than shifting
    /// into overflow or wrapping back to a tight loop. `u32::MAX` failures
    /// is the extreme the `min`/`saturating_add` pair exists to survive.
    #[test]
    fn resolve_backoff_caps_and_never_overflows() {
        let capped = RECONNECT_DELAY * (1 << RESOLVE_BACKOFF_MAX_SHIFT);
        assert_eq!(capped, Duration::from_millis(25_600));

        for failures in [
            RESOLVE_BACKOFF_MAX_SHIFT,
            RESOLVE_BACKOFF_MAX_SHIFT + 1,
            RESOLVE_BACKOFF_MAX_SHIFT + 64,
            u32::MAX,
        ] {
            let backoff = ResolveBackoff {
                consecutive_failures: failures,
            };
            assert_eq!(
                backoff.delay(),
                capped,
                "{failures} failures should still wait the capped {capped:?}"
            );
        }

        // Escalating from the ceiling stays at the ceiling.
        let mut backoff = ResolveBackoff {
            consecutive_failures: u32::MAX,
        };
        backoff.consecutive_failures = backoff.consecutive_failures.saturating_add(1);
        assert_eq!(backoff.delay(), capped);
    }

    /// `reset` is the half that makes this converge rather than punish: a
    /// peer whose DNS was merely slow to propagate must not carry a long
    /// delay forward once its name starts resolving. Without this, a peer
    /// that flaps once ends up permanently 25.6s slow to reconnect.
    #[test]
    fn a_successful_resolution_clears_the_backoff() {
        let mut backoff = ResolveBackoff {
            consecutive_failures: RESOLVE_BACKOFF_MAX_SHIFT + 10,
        };
        assert_ne!(backoff.delay(), RECONNECT_DELAY, "test premise");

        backoff.reset();

        assert_eq!(
            backoff.delay(),
            RECONNECT_DELAY,
            "after a success the next failure must wait the base delay again"
        );
    }

    /// The schedule is not merely *computed* -- it is actually waited out.
    /// Driven on tokio's paused clock, so this asserts real sleep calls
    /// against virtual time without spending any wall-clock time: six
    /// consecutive failures must consume 200+400+800+1600+3200+6400 ms.
    ///
    /// This is what a flat-cadence regression would fail on. Six failures
    /// under the pre-#42 behavior would consume 1.2s, not 12.6s.
    #[tokio::test(start_paused = true)]
    async fn consecutive_failures_actually_sleep_the_escalating_schedule() {
        let mut backoff = ResolveBackoff::default();
        let started = tokio::time::Instant::now();

        for _ in 0..6 {
            backoff.sleep_and_escalate().await;
        }

        assert_eq!(
            started.elapsed(),
            Duration::from_millis(200 + 400 + 800 + 1_600 + 3_200 + 6_400)
        );
    }

    /// The same, past the ceiling: once capped, each further failure costs
    /// exactly the capped delay and no more, so a permanently-broken
    /// hostname settles into a steady ~0.04 Hz query rate instead of the
    /// ~5 Hz loop issue #42 reported.
    #[tokio::test(start_paused = true)]
    async fn a_permanently_broken_name_settles_at_the_capped_rate() {
        let mut backoff = ResolveBackoff {
            consecutive_failures: RESOLVE_BACKOFF_MAX_SHIFT,
        };
        let started = tokio::time::Instant::now();

        for _ in 0..10 {
            backoff.sleep_and_escalate().await;
        }

        let capped = RECONNECT_DELAY * (1 << RESOLVE_BACKOFF_MAX_SHIFT);
        assert_eq!(started.elapsed(), capped * 10);
        assert!(
            started.elapsed() >= Duration::from_secs(256),
            "10 capped waits should span minutes, not the 2s a flat 200ms cadence would"
        );
    }
}
