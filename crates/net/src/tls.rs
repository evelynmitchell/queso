//! Phase 8.2a (issue #47): opt-in, app-level TLS for both connection kinds
//! this crate speaks -- see `docs/deploy-flyio.md` §12 for why this is
//! optional rather than mandatory (fly's `.internal`/6PN mesh is already
//! WireGuard-encrypted; this module exists for defense-in-depth there and
//! for any non-fly deployment that has no equivalent transport encryption).
//!
//! - **Peer<->peer (`crate::transport`): mutual TLS.** Every replica
//!   presents a certificate when it dials a peer *and* when it accepts one;
//!   both ends verify the other's chain against a shared, operator-supplied
//!   CA (see [`TlsConfig`]). [`build_peer_tls`] builds both halves (an
//!   `Arc<rustls::ServerConfig>` for [`crate::transport::accept_peers`], an
//!   `Arc<rustls::ClientConfig>` for
//!   [`crate::transport::spawn_peer_dialer`]) from one [`TlsConfig`].
//! - **Client->replica (`crate::client`): server-authenticated TLS only.**
//!   A client verifies the replica's cert against a CA but never presents
//!   its own -- end clients are not cluster members, so client-cert auth
//!   for them is out of scope (see the module docs on `crate::client`).
//!   [`build_client_facing_server_tls`] builds the replica-side
//!   `Arc<rustls::ServerConfig>` (from the same [`TlsConfig`] the peer side
//!   uses -- one cert/key per replica is enough for both listeners);
//!   [`build_client_tls`] builds the caller-side `Arc<rustls::ClientConfig>`
//!   from a [`ClientTlsConfig`] (just a CA, no client identity).
//!
//! Both [`NodeConfig::tls`](crate::config::NodeConfig::tls) and
//! [`ClientConfig::tls`](crate::client::ClientConfig::tls) default to
//! `None`, which is a true no-op: no rustls config is ever built, no TLS
//! handshake ever runs, every socket is exactly the plain `TcpStream` this
//! crate spoke before this module existed. Nothing here changes any
//! existing (non-opted-in) test or deployment's behavior.
//!
//! # Where the handshake runs
//!
//! Immediately after `TcpStream::connect`/`TcpListener::accept` and before
//! anything else -- in particular, before the `WireMsg::Hello` handshake
//! (`crate::wire`) that identifies *which* replica dialed in on the peer
//! side. That ordering matters: TLS's job here is to authenticate "this
//! socket belongs to someone holding a certificate signed by our
//! configured CA" (cluster membership, or -- client side -- "this really is
//! the replica we meant to talk to"); *which* specific replica dialed in is
//! still established the same way it always was, by the first plaintext
//! (now TLS-encrypted) frame on the connection. Once the handshake
//! completes, [`MaybeTlsStream`] wraps the resulting stream so every byte
//! after that point -- framing, `Hello`, every `WireMsg`/`Command`/
//! `Outcome` frame -- flows through `Framed<MaybeTlsStream, ..>` completely
//! unchanged from the plaintext path.
//!
//! # Certificate verification -- what is and is not relaxed
//!
//! **Nothing here ever disables verification.** There is no
//! `SkipServerVerification`/"accept any cert" verifier anywhere in this
//! module, and the peer acceptor's client-auth is *required*
//! (`rustls::server::WebPkiClientVerifier`'s default `AnonymousClientPolicy`
//! is `Deny` -- see [`build_peer_tls`]), not merely offered.
//!
//! The one deliberate relaxation is in `ChainOnlyServerCertVerifier`
//! (used for the peer dialer's view of the acceptor's cert, and by default
//! for the client's view of a replica's cert): it performs full X.509 chain
//! validation -- signature verification up to one of the configured CA's
//! trust anchors, and certificate validity-period checks -- via
//! `rustls::client::verify_server_cert_signed_by_trust_anchor` (the exact
//! function rustls's own default `WebPkiServerVerifier` uses for this step),
//! but deliberately does **not** call
//! `rustls::client::verify_server_name` (matching the presented cert's
//! Subject Alternative Names against the address that was dialed). Only
//! that one check is skipped, and only because this crate's peers/replicas
//! are addressed by whatever `--peer`/`--addr` string an operator gave them
//! (a literal IP, a Docker/fly-internal hostname, ...) which need not be --
//! and in general is not required to be -- baked into that node's cert as a
//! SAN; requiring an exact match there would make cert issuance depend on
//! deployment topology for no additional security benefit here, since chain
//! validation to a private, operator-controlled CA already limits "who can
//! present a cert that verifies at all" to the cluster's own membership.
//! [`ClientTlsConfig::expected_server_name`] opts a caller *back into* full,
//! unrelaxed name verification (via the stock
//! `rustls::client::WebPkiServerVerifier`) when the caller does know, and
//! wants to pin, the expected name.

use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use anyhow::Context as _;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::verify_server_cert_signed_by_trust_anchor;
use rustls::crypto::{verify_tls12_signature, verify_tls13_signature, CryptoProvider};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::{ParsedCertificate, WebPkiClientVerifier};
use rustls::{DigitallySignedStruct, RootCertStore, SignatureScheme};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

/// One replica's TLS identity: its own certificate chain + private key, plus
/// the CA/trust-anchor bundle it uses to verify everyone else (other
/// replicas' peer certs -- both as dialer and acceptor -- and, implicitly,
/// nothing about clients, since client-cert auth for end clients is out of
/// scope -- see the module docs on `crate::client`). All three are PEM
/// files, loaded once at boot (see [`build_peer_tls`]/
/// [`build_client_facing_server_tls`]), never re-read afterward -- rotating
/// a cert/key requires restarting the replica (see this crate's README's
/// TLS section for the honest limitation).
///
/// `None` (the default everywhere: `NodeConfig::tls`) means exactly what
/// [`crate::config::NodeConfig::nemesis`]'s docs describe for that field --
/// a strict no-op, plaintext TCP exactly as before this module existed.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// PEM file containing this replica's certificate chain (leaf cert
    /// first, then any intermediates -- the CA itself need not be
    /// included, only [`Self::ca_path`] needs it).
    pub cert_chain_path: PathBuf,
    /// PEM file containing this replica's private key (PKCS#1, PKCS#8, or
    /// SEC1 -- whatever `rustls_pemfile::private_key` recognizes).
    pub key_path: PathBuf,
    /// PEM file containing the CA certificate(s) trusted to sign every
    /// cluster member's (and, for [`build_client_facing_server_tls`]'s
    /// caller's) certificate. Used both to verify inbound peer client
    /// certs (mTLS acceptor) and outbound peer server certs (mTLS dialer).
    pub ca_path: PathBuf,
}

/// A client's (an end caller, e.g. `queso-bench` or `crate::client::Client`
/// -- never a replica) opt-in TLS configuration for talking to a replica's
/// client port: just a CA to verify the replica's server cert against, no
/// client identity of its own (server-authenticated TLS only -- see the
/// module docs).
#[derive(Debug, Clone)]
pub struct ClientTlsConfig {
    /// PEM file containing the CA certificate(s) trusted to sign a
    /// replica's server certificate.
    pub ca_path: PathBuf,
    /// If set, verify the replica's cert's Subject Alternative Names
    /// against this exact name (full, unrelaxed
    /// `rustls::client::verify_server_name`) instead of the default
    /// chain-only verification -- see `ChainOnlyServerCertVerifier`'s
    /// docs for why chain-only is the default and when you would want to
    /// opt back into strict name matching (e.g. every replica happens to
    /// share one cert/SAN, or the caller wants defense-in-depth against a
    /// CA-compromise-plus-address-confusion combination specifically).
    pub expected_server_name: Option<String>,
}

/// Both TLS halves a replica needs for peer traffic (Phase 8.2a): one
/// `rustls::ServerConfig` for [`crate::transport::accept_peers`] (mTLS,
/// client cert required) and one `rustls::ClientConfig` for
/// [`crate::transport::spawn_peer_dialer`] (mTLS, this replica presents its
/// own cert and verifies the acceptor's chain-only -- see
/// `ChainOnlyServerCertVerifier`).
pub struct PeerTls {
    pub server_config: Arc<rustls::ServerConfig>,
    pub client_config: Arc<rustls::ClientConfig>,
}

/// A byte stream that is either a plain `TcpStream` (the entire crate's
/// behavior before this module existed, and still the default --
/// `NodeConfig::tls`/`ClientTlsConfig` are `None`/absent unless an operator
/// opts in) or a completed `tokio_rustls` TLS session over one. Exists so
/// `Framed<_, LengthDelimitedCodec>` (`crate::transport`/`crate::client`)
/// and the `WireMsg::Hello` handshake that runs over it can stay completely
/// unaware of whether TLS is in play -- see this module's docs for exactly
/// where the handshake happens relative to `Hello`.
pub enum MaybeTlsStream {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::TlsStream<TcpStream>>),
}

impl AsyncRead for MaybeTlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_flush(cx),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

/// This crate's TLS crypto provider: `ring`, explicitly, rather than
/// relying on rustls's process-global "default provider" (which would
/// require an install-once dance and creates a footgun if some other
/// dependency ever installs a different one first) -- see
/// `Cargo.toml`'s comment on why `ring` rather than the crate-default
/// `aws-lc-rs` (no `cmake` build dependency, keeps `deploy/Dockerfile`'s
/// builder image unchanged). Every `rustls::ClientConfig`/`ServerConfig`
/// this module builds is constructed with this provider explicitly
/// (`builder_with_provider`), so building more than one (peer + client-
/// facing + a test's own) in the same process is safe and independent --
/// no shared global state.
fn crypto_provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

fn load_cert_chain(path: &Path) -> anyhow::Result<Vec<CertificateDer<'static>>> {
    let file = File::open(path)
        .with_context(|| format!("opening TLS cert chain file {}", path.display()))?;
    let mut reader = BufReader::new(file);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<_, _>>()
        .with_context(|| format!("parsing PEM certificates from {}", path.display()))?;
    anyhow::ensure!(
        !certs.is_empty(),
        "no certificates found in {}",
        path.display()
    );
    Ok(certs)
}

fn load_private_key(path: &Path) -> anyhow::Result<PrivateKeyDer<'static>> {
    let file =
        File::open(path).with_context(|| format!("opening TLS key file {}", path.display()))?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .with_context(|| format!("parsing PEM private key from {}", path.display()))?
        .ok_or_else(|| anyhow::anyhow!("no private key found in {}", path.display()))
}

fn load_root_store(path: &Path) -> anyhow::Result<Arc<RootCertStore>> {
    let certs = load_cert_chain(path)?;
    let mut roots = RootCertStore::empty();
    for cert in certs {
        roots
            .add(cert)
            .with_context(|| format!("adding a CA certificate from {}", path.display()))?;
    }
    anyhow::ensure!(
        !roots.is_empty(),
        "no usable CA certificates found in {}",
        path.display()
    );
    Ok(Arc::new(roots))
}

/// A `rustls::client::danger::ServerCertVerifier` that performs full,
/// real X.509 chain validation against a configured set of trust anchors
/// (signature verification up to the CA, certificate validity-period
/// checks -- delegated to
/// `rustls::client::verify_server_cert_signed_by_trust_anchor`, the same
/// function rustls's own stock `WebPkiServerVerifier` uses for this step)
/// but deliberately skips `rustls::client::verify_server_name` (matching
/// the leaf cert's Subject Alternative Names against the dialed address).
///
/// # Why this is safe, not "accept any cert"
///
/// This is **not** a verifier that returns `Ok` unconditionally --
/// presenting a cert that isn't signed by (a certificate chaining to) one
/// of `roots`, or that is expired/not-yet-valid, or whose signature does
/// not verify, is rejected exactly as it would be by the stock verifier.
/// The relaxation is narrowly scoped to name matching, which this crate's
/// peer-addressing model (operators dial peers by an arbitrary
/// `--peer`/`--addr` string -- a literal IP, a Docker/fly-internal
/// hostname, ...) does not need: trust here is rooted entirely in
/// possession of a certificate signed by the configured, operator-supplied
/// CA, not in a specific hostname appearing in that cert. See this
/// module's docs for [`ClientTlsConfig::expected_server_name`], which
/// opts a client back into full, unrelaxed name verification when that
/// additional binding is wanted.
#[derive(Debug)]
struct ChainOnlyServerCertVerifier {
    roots: Arc<RootCertStore>,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for ChainOnlyServerCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let cert = ParsedCertificate::try_from(end_entity)?;
        verify_server_cert_signed_by_trust_anchor(
            &cert,
            &self.roots,
            intermediates,
            now,
            self.provider.signature_verification_algorithms.all,
        )?;
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Build both halves of a replica's peer-traffic mTLS (see [`PeerTls`]).
///
/// The acceptor side (`server_config`) requires a client certificate --
/// `rustls::server::WebPkiClientVerifier::builder`'s default
/// `AnonymousClientPolicy` is `Deny`, and this deliberately never calls
/// `.allow_unauthenticated()` -- so a dialer that cannot present a cert
/// chaining to `cfg.ca_path` is rejected during the TLS handshake itself,
/// before a single `WireMsg` byte is read. The dialer side (`client_config`)
/// presents this replica's own cert as its client-auth credential and
/// verifies the acceptor's cert via `ChainOnlyServerCertVerifier`.
pub fn build_peer_tls(cfg: &TlsConfig) -> anyhow::Result<PeerTls> {
    let provider = crypto_provider();
    let cert_chain = load_cert_chain(&cfg.cert_chain_path)?;
    let key = load_private_key(&cfg.key_path)?;
    let roots = load_root_store(&cfg.ca_path)?;

    let client_verifier =
        WebPkiClientVerifier::builder_with_provider(roots.clone(), provider.clone())
            .build()
            .map_err(|e| anyhow::anyhow!("building peer client-cert verifier: {e}"))?;
    let server_config = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| anyhow::anyhow!("selecting TLS protocol versions: {e}"))?
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(cert_chain.clone(), key.clone_key())
        .context("building this replica's peer-facing TLS server config")?;

    let server_verifier = Arc::new(ChainOnlyServerCertVerifier {
        roots,
        provider: provider.clone(),
    });
    let client_config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| anyhow::anyhow!("selecting TLS protocol versions: {e}"))?
        .dangerous()
        .with_custom_certificate_verifier(server_verifier)
        .with_client_auth_cert(cert_chain, key)
        .context("building this replica's peer-dialing TLS client config")?;

    Ok(PeerTls {
        server_config: Arc::new(server_config),
        client_config: Arc::new(client_config),
    })
}

/// Build the replica-side TLS server config for the *client-facing* port
/// (`crate::client::accept_clients`): this replica's own cert/key, no
/// client-certificate requirement (end clients are not cluster members --
/// see the module docs on `crate::client`, and this crate's docs). Reuses
/// the same [`TlsConfig`] as [`build_peer_tls`] -- one cert/key per replica
/// is enough to serve both listeners; only the client-auth policy differs.
pub fn build_client_facing_server_tls(
    cfg: &TlsConfig,
) -> anyhow::Result<Arc<rustls::ServerConfig>> {
    let provider = crypto_provider();
    let cert_chain = load_cert_chain(&cfg.cert_chain_path)?;
    let key = load_private_key(&cfg.key_path)?;

    let server_config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| anyhow::anyhow!("selecting TLS protocol versions: {e}"))?
        .with_no_client_auth()
        .with_single_cert(cert_chain, key)
        .context("building this replica's client-facing TLS server config")?;
    Ok(Arc::new(server_config))
}

/// Build a caller's (an end client, e.g. `queso-bench`/`crate::client`)
/// server-authenticated TLS client config for talking to a replica's
/// client port: verifies the replica's cert against `cfg.ca_path`, presents
/// no client certificate of its own. Uses
/// `ChainOnlyServerCertVerifier` unless [`ClientTlsConfig::expected_server_name`]
/// is set, in which case it uses the stock, fully name-checking
/// `rustls::client::WebPkiServerVerifier` instead -- see that field's docs.
pub fn build_client_tls(cfg: &ClientTlsConfig) -> anyhow::Result<Arc<rustls::ClientConfig>> {
    let provider = crypto_provider();
    let roots = load_root_store(&cfg.ca_path)?;

    let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| anyhow::anyhow!("selecting TLS protocol versions: {e}"))?;

    let client_config = if cfg.expected_server_name.is_some() {
        // Opted into full, unrelaxed name verification -- see
        // `ClientTlsConfig::expected_server_name`'s docs.
        builder.with_root_certificates(roots).with_no_client_auth()
    } else {
        let verifier = Arc::new(ChainOnlyServerCertVerifier { roots, provider });
        builder
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_no_client_auth()
    };
    Ok(Arc::new(client_config))
}

/// Build the `rustls::pki_types::ServerName` to hand to the TLS handshake
/// for a given dial target. Its value only matters when full name
/// verification is actually in effect (`build_client_tls` with
/// [`ClientTlsConfig::expected_server_name`] set, which uses this
/// function's `expected_name` argument instead of `host` -- see below); for
/// `ChainOnlyServerCertVerifier` (the default, and always for peer
/// dialing) it is accepted by the TLS handshake API but never actually
/// checked against the presented cert, so any syntactically valid value
/// works. `host` should be the dial target's hostname/IP with any `:port`
/// suffix already stripped; the placeholder fallback exists only for the
/// (in practice unreachable, since callers validate `host:port` shape
/// up-front -- see `crate::transport::resolve_peer_addr`'s callers)
/// case where `host` somehow is not a syntactically valid DNS name or IP
/// literal.
pub fn server_name_for(host: &str, expected_name: Option<&str>) -> ServerName<'static> {
    let candidate = expected_name.unwrap_or(host);
    ServerName::try_from(candidate.to_string()).unwrap_or_else(|_| {
        ServerName::try_from("queso-peer.invalid".to_string())
            .expect("a fixed literal DNS name is always a valid ServerName")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_name_for_accepts_an_ip_literal() {
        let name = server_name_for("127.0.0.1", None);
        assert!(matches!(name, ServerName::IpAddress(_)));
    }

    #[test]
    fn server_name_for_accepts_a_hostname() {
        let name = server_name_for("queso-1.internal", None);
        assert!(matches!(name, ServerName::DnsName(_)));
    }

    #[test]
    fn server_name_for_prefers_an_explicit_expected_name() {
        let name = server_name_for("127.0.0.1", Some("queso-0.internal"));
        match name {
            ServerName::DnsName(dns) => assert_eq!(dns.as_ref(), "queso-0.internal"),
            other => panic!("expected a DNS name, got {other:?}"),
        }
    }

    #[test]
    fn server_name_for_falls_back_on_an_unparseable_host() {
        // Not a valid DNS name or IP literal (an empty label / trailing
        // dot immediately after another dot). This exercises the fallback
        // path -- `server_name_for` must never panic, only substitute a
        // fixed placeholder, since it's checked-but-not-security-relevant
        // for the default chain-only verifier.
        let name = server_name_for("not..valid", None);
        assert!(matches!(name, ServerName::DnsName(_)));
    }
}
