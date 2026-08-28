//! The client-facing protocol and client library.
//!
//! The wire protocol itself is deliberately minimal: a client connects to a
//! replica's client port, sends one length-delimited, bincode-encoded
//! `queso_smr::Command` frame, and receives back one length-delimited,
//! bincode-encoded `queso_smr::Outcome` frame -- one request per connection,
//! no pipelining. [`submit`] is the "just enough to prove it works" helper
//! that speaks exactly that protocol with zero retry policy, used by this
//! crate's own `tests/cluster.rs`.
//!
//! [`Client`] (Phase 7.2) is what a real caller -- `queso-bench` included --
//! should actually use: it wraps [`submit`] with the two things a client
//! talking to a Meerkat-style cluster over a real, lossy network needs that
//! a single fire-and-forget call does not provide:
//!
//! 1. **A pool of replica addresses** rather than one fixed address, so a
//!    client isn't wired to a single replica that might be down.
//! 2. **Retry-to-another-replica** on connection failure or timeout, with a
//!    short backoff once every address in the pool has been tried, so a
//!    client survives a replica crashing, a partition, or (once one exists)
//!    a leadership change -- without the caller having to hand-roll retry
//!    logic itself.
//!
//! It deliberately does *not* add pipelining or pooled/reused connections:
//! each attempt is a fresh one-shot [`submit`] call, matching the wire
//! protocol above. That keeps [`Client`] simple and, per `queso_smr`'s A6
//! precondition (see `queso_smr::command::ClientSession`'s docs), correct --
//! a session may have at most one operation in flight at a time, which a
//! single outstanding `submit` per `Client::submit` call trivially satisfies.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use bytes::BytesMut;
use futures_util::{FutureExt, SinkExt, StreamExt};
use queso_smr::{Command, Outcome};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::warn;

use crate::driver::Event;
use crate::tls::{server_name_for, MaybeTlsStream};

/// Accept client connections forever, spawning one task per connection
/// (see `serve_one_client`).
///
/// `tls` (Phase 8.2a, `crate::tls`) is this replica's client-facing,
/// server-authenticated-only TLS config (see
/// `crate::tls::build_client_facing_server_tls`) -- `None` (every call site
/// except a real `queso-node` run or test that opts in via
/// `NodeConfig::tls`) skips the TLS handshake entirely. Unlike the peer
/// acceptor (`crate::transport::accept_peers`), this never requires a
/// client certificate -- end clients are not cluster members, see this
/// module's docs.
pub async fn accept_clients(
    listener: TcpListener,
    inbox: mpsc::UnboundedSender<Event>,
    tls: Option<Arc<rustls::ServerConfig>>,
) {
    loop {
        let (stream, addr) = match listener.accept().await {
            Ok(x) => x,
            Err(err) => {
                warn!(%err, "client accept failed");
                continue;
            }
        };
        let _ = stream.set_nodelay(true);
        let inbox = inbox.clone();
        let tls = tls.clone();
        tokio::spawn(async move {
            if let Err(err) = serve_one_client(stream, inbox, tls).await {
                warn!(%addr, %err, "client connection error");
            }
        });
    }
}

async fn serve_one_client(
    stream: TcpStream,
    inbox: mpsc::UnboundedSender<Event>,
    tls: Option<Arc<rustls::ServerConfig>>,
) -> anyhow::Result<()> {
    let stream: MaybeTlsStream = match tls {
        None => MaybeTlsStream::Plain(stream),
        Some(tls_config) => {
            let tls_stream = TlsAcceptor::from(tls_config)
                .accept(stream)
                .await
                .context("client-facing TLS handshake failed")?;
            MaybeTlsStream::Tls(Box::new(tls_stream.into()))
        }
    };
    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());
    let Some(frame) = framed.next().await else {
        return Ok(()); // Client disconnected without sending anything.
    };
    let bytes: BytesMut = frame?;
    let command: Command = bincode::deserialize(&bytes)?;

    let (resp_tx, resp_rx) = oneshot::channel();
    inbox
        .send(Event::ClientSubmit {
            command,
            resp: resp_tx,
        })
        .map_err(|_| anyhow::anyhow!("replica's driver loop has shut down"))?;
    let outcome = resp_rx.await?;

    let bytes = bincode::serialize(&outcome)?;
    framed.send(bytes.into()).await?;
    Ok(())
}

/// Connect to the replica listening at `addr`'s client port, submit
/// `command`, and return its `Outcome`. This is deliberately the smallest
/// possible client: one command, one connection, no retry -- if `addr`
/// isn't the fast-path leader (or crashes, or is partitioned), the command
/// can still be decided (per Meerkat's leaderless-tolerant design, see
/// `queso_smr::cluster`'s module docs) but this helper will simply hang, or
/// fail with a connection error, rather than trying anywhere else. Good
/// enough to prove the real-TCP path end-to-end (see this crate's
/// `tests/cluster.rs`); most real callers want [`Client::submit`] instead,
/// which wraps exactly this call with a pool of addresses and
/// retry-to-another-replica. Always plaintext -- see [`submit_with_tls`] for
/// the Phase 8.2a TLS-capable equivalent.
pub async fn submit(addr: SocketAddr, command: &Command) -> anyhow::Result<Outcome> {
    let stream = TcpStream::connect(addr).await?;
    submit_over(MaybeTlsStream::Plain(stream), command).await
}

/// Like [`submit`], but establishes server-authenticated TLS (Phase 8.2a,
/// `crate::tls`) over the connection before submitting `command` -- the
/// handshake runs immediately after `TcpStream::connect`, before any
/// application byte crosses the wire. `tls` should come from
/// [`crate::tls::build_client_tls`]; `server_name` is the identity to
/// present to the TLS handshake API (only actually checked against the
/// presented cert when `tls` was built with
/// [`crate::tls::ClientTlsConfig::expected_server_name`] set -- see that
/// type's docs and [`crate::tls::server_name_for`]). A handshake failure
/// (including the replica's cert not chaining to the configured CA) surfaces
/// as an `Err` here, before `command` is ever sent.
pub async fn submit_with_tls(
    addr: SocketAddr,
    command: &Command,
    tls: &Arc<rustls::ClientConfig>,
    server_name: rustls::pki_types::ServerName<'static>,
) -> anyhow::Result<Outcome> {
    let stream = TcpStream::connect(addr).await?;
    let tls_stream = TlsConnector::from(tls.clone())
        .connect(server_name, stream)
        .await
        .context("client TLS handshake failed")?;
    submit_over(MaybeTlsStream::Tls(Box::new(tls_stream.into())), command).await
}

async fn submit_over(stream: MaybeTlsStream, command: &Command) -> anyhow::Result<Outcome> {
    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());
    let bytes = bincode::serialize(command)?;
    framed.send(bytes.into()).await?;
    let Some(frame) = framed.next().await else {
        anyhow::bail!("connection closed before a response arrived");
    };
    let bytes: BytesMut = frame?;
    let outcome: Outcome = bincode::deserialize(&bytes)?;
    Ok(outcome)
}

/// [`Client`]'s retry policy knobs. The defaults are deliberately modest --
/// a load generator or interactive caller can tune these, but they need to
/// do *something* sane with zero configuration.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// How long to wait for one attempt (connect + request + response)
    /// against a single address before treating it as failed and moving to
    /// the next one.
    pub attempt_timeout: Duration,
    /// How many full passes over the address pool to attempt before giving
    /// up and returning the last error. `1` means "try every address once";
    /// `0` is treated as `1` (a client with nothing to try is not useful).
    pub max_rounds: usize,
    /// How long to sleep between rounds once every address in the pool has
    /// been tried and failed once -- a short pause so a client that has
    /// outrun a cluster mid-election (say) doesn't spin-retry it into the
    /// ground.
    pub retry_backoff: Duration,
    /// Phase 8.2a (issue #47): opt-in, server-authenticated TLS (see
    /// `crate::tls`) for every dial this [`Client`] makes. `None` -- the
    /// default -- is a true plaintext no-op, unchanged from before this
    /// field existed: every attempt goes through [`submit`] exactly as
    /// before. `Some` (built via [`crate::tls::build_client_tls`]) routes
    /// every attempt through [`submit_with_tls`] instead, verifying each
    /// replica's server cert against the CA that config was built with.
    pub tls: Option<Arc<rustls::ClientConfig>>,
    /// Only consulted when [`Self::tls`] is `Some`: the identity string to
    /// present to the TLS handshake API for every dial (see
    /// [`crate::tls::server_name_for`]/[`crate::tls::ClientTlsConfig::expected_server_name`]).
    /// `None` derives it from each attempt's own `SocketAddr` (its IP,
    /// stringified) -- fine for the default chain-only server-cert
    /// verifier, which never actually checks it; set this when `tls` was
    /// built with `expected_server_name` set, to the same name, so the
    /// (now name-checking) verifier is checking the name you actually mean.
    pub tls_server_name: Option<String>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            attempt_timeout: Duration::from_secs(2),
            max_rounds: 5,
            retry_backoff: Duration::from_millis(50),
            tls: None,
            tls_server_name: None,
        }
    }
}

/// A client that knows about a whole cluster, not just one replica.
///
/// Holds a fixed pool of candidate client-port addresses (typically every
/// replica's, though a subset works too) and, on every [`Client::submit`]
/// call, tries them in a rotating order -- starting from a different
/// address each call (via a shared round-robin cursor, so concurrent
/// callers naturally spread their *first* attempt across the pool instead
/// of piling onto address `0`) -- retrying against the next address in the
/// pool on any connection failure, I/O error, or per-attempt timeout. See
/// the module docs for why this is the right amount of robustness and no
/// more (no pooled connections, no pipelining).
#[derive(Debug)]
pub struct Client {
    addrs: Vec<SocketAddr>,
    cursor: AtomicUsize,
    config: ClientConfig,
}

impl Client {
    /// Build a client over `addrs` (must be non-empty) with the default
    /// [`ClientConfig`].
    pub fn new(addrs: Vec<SocketAddr>) -> Self {
        Self::with_config(addrs, ClientConfig::default())
    }

    /// Build a client over `addrs` (must be non-empty) with an explicit
    /// [`ClientConfig`].
    pub fn with_config(addrs: Vec<SocketAddr>, config: ClientConfig) -> Self {
        assert!(
            !addrs.is_empty(),
            "Client needs at least one replica address"
        );
        Self {
            addrs,
            cursor: AtomicUsize::new(0),
            config,
        }
    }

    /// This client's configured address pool, in the fixed order it was
    /// built with (not the per-call rotation order).
    pub fn addrs(&self) -> &[SocketAddr] {
        &self.addrs
    }

    /// Submit `command`, retrying against other addresses in the pool on
    /// failure or timeout, per [`ClientConfig`]. Returns the last error
    /// encountered if every address fails on every round.
    pub async fn submit(&self, command: &Command) -> anyhow::Result<Outcome> {
        let start = self.cursor.fetch_add(1, Ordering::Relaxed) % self.addrs.len();
        let rounds = self.config.max_rounds.max(1);
        let mut last_err: Option<anyhow::Error> = None;

        for round in 0..rounds {
            for offset in 0..self.addrs.len() {
                let addr = self.addrs[(start + offset) % self.addrs.len()];
                let attempt = match &self.config.tls {
                    None => submit(addr, command).boxed(),
                    Some(tls) => {
                        let server_name = server_name_for(
                            &addr.ip().to_string(),
                            self.config.tls_server_name.as_deref(),
                        );
                        submit_with_tls(addr, command, tls, server_name).boxed()
                    }
                };
                match tokio::time::timeout(self.config.attempt_timeout, attempt).await {
                    Ok(Ok(outcome)) => return Ok(outcome),
                    Ok(Err(err)) => {
                        warn!(%addr, %err, "queso-net client: attempt failed");
                        last_err = Some(err);
                    }
                    Err(_) => {
                        warn!(%addr, timeout = ?self.config.attempt_timeout, "queso-net client: attempt timed out");
                        last_err = Some(anyhow::anyhow!(
                            "request to {addr} timed out after {:?}",
                            self.config.attempt_timeout
                        ));
                    }
                }
            }
            let last_round = round + 1 == rounds;
            if !last_round {
                tokio::time::sleep(self.config.retry_backoff).await;
            }
        }

        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("Client::submit: address pool was empty")))
    }
}

#[cfg(test)]
mod client_pool_tests {
    use super::*;

    #[test]
    #[should_panic(expected = "at least one replica address")]
    fn rejects_an_empty_pool() {
        let _ = Client::new(vec![]);
    }

    /// A pool where every address is unreachable (nothing listening) must
    /// exhaust its retries and return an error rather than hang -- the
    /// property `queso-bench` depends on to count/report failed ops instead
    /// of stalling the whole run.
    #[tokio::test]
    async fn exhausts_retries_and_returns_an_error_when_every_address_is_dead() {
        // Bind-then-drop three listeners to get addresses that are free
        // (nothing accepting connections there) but were real, valid local
        // ports a moment ago -- deterministic "connection refused" rather
        // than relying on a hardcoded unused port range.
        let mut dead_addrs = Vec::new();
        for _ in 0..3 {
            let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
            dead_addrs.push(listener.local_addr().unwrap());
        }
        let client = Client::with_config(
            dead_addrs,
            ClientConfig {
                attempt_timeout: Duration::from_millis(200),
                max_rounds: 1,
                retry_backoff: Duration::from_millis(1),
                ..ClientConfig::default()
            },
        );
        let command = Command::Get {
            client: queso_smr::ClientId(0),
            seq: 0,
            key: 0,
        };
        let result = client.submit(&command).await;
        assert!(result.is_err(), "expected every dead address to fail");
    }
}
