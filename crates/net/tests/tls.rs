//! Phase 8.2a's (issue #47) acceptance tests: app-level TLS
//! (`queso_net::tls`) for both peer<->peer traffic (mutual TLS) and
//! client->replica traffic (server-authenticated TLS). See that module's
//! docs for the design; this file proves it end to end against real,
//! in-process, real-TCP connections, using throwaway certs generated at
//! test-run time with `rcgen` (a dev-dependency -- no PEM fixtures are
//! checked into the repo).
//!
//! Three groups of tests:
//!
//! - [`three_node_cluster_forms_over_mtls_peers_and_server_tls_clients`]:
//!   the positive/handshake-success test -- a 3-node cluster booted with
//!   mTLS on every peer connection and server-authenticated TLS on every
//!   client connection forms and serves a real `Put`/`Get` exactly like
//!   `tests/cluster.rs`'s plaintext equivalent.
//! - `peer_mtls_rejects_*`: the negative/auth-is-real tests for the peer
//!   acceptor -- a dialer presenting no client certificate, or a client
//!   certificate from an untrusted CA, is rejected at the TLS handshake
//!   itself (`crate::transport::accept_peers`'s mTLS requirement is real,
//!   not decorative).
//! - `client_tls_refuses_a_server_cert_signed_by_the_wrong_ca`: the
//!   negative test for the client-facing side -- a caller that only trusts
//!   a different CA than the one that actually signed the replica's server
//!   cert refuses to talk to it (server-cert verification is real).
//!
//! `tests/cluster.rs`'s existing plaintext tests are entirely unchanged --
//! see this crate's guardrail that `NodeConfig::tls: None` is a true no-op.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use queso_net::tls::{
    build_client_facing_server_tls, build_client_tls, build_peer_tls, server_name_for,
    ClientTlsConfig, TlsConfig,
};
use queso_net::{client, transport};
use queso_sim::ids::NodeId;
use queso_smr::{ClientId, Command, Outcome};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
};
use tokio::net::TcpListener;
use tokio::sync::mpsc;

#[path = "support/mod.rs"]
mod support;
use support::{spawn_cluster_with_tls, submit_with_retry_tls};

/// A throwaway, self-signed CA plus a `sign_leaf` helper -- everything
/// this test file needs to hand out mutually-trusted (or, for the negative
/// tests, deliberately *not* mutually-trusted) certificates, generated
/// fresh per test with `rcgen`.
struct TestCa {
    cert: rcgen::Certificate,
    key: KeyPair,
}

impl TestCa {
    fn new() -> Self {
        let key = KeyPair::generate().expect("generate CA key");
        let mut params =
            CertificateParams::new(Vec::<String>::new()).expect("build CA cert params");
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let cert = params.self_signed(&key).expect("self-sign CA cert");
        Self { cert, key }
    }

    fn ca_pem(&self) -> String {
        self.cert.pem()
    }

    /// Sign a leaf cert (dual-use: both `ServerAuth`/`ClientAuth` EKU, so
    /// the same cert works for a peer's mTLS dialer *and* acceptor role,
    /// and for a client-facing server) with `name` (an IP literal or
    /// hostname) as its sole Subject Alternative Name. Returns
    /// `(cert_pem, key_pem)`.
    fn sign_leaf(&self, name: &str) -> (String, String) {
        let leaf_key = KeyPair::generate().expect("generate leaf key");
        let mut params =
            CertificateParams::new(vec![name.to_string()]).expect("build leaf cert params");
        params.is_ca = IsCa::NoCa;
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let cert = params
            .signed_by(&leaf_key, &self.cert, &self.key)
            .expect("sign leaf cert");
        (cert.pem(), leaf_key.serialize_pem())
    }
}

fn write_pem(dir: &Path, filename: &str, contents: &str) -> PathBuf {
    let path = dir.join(filename);
    std::fs::write(&path, contents).expect("write a throwaway test PEM file");
    path
}

/// Generous but bounded -- TLS handshakes (on top of ordinary consensus
/// round trips) add nondeterministic latency under concurrent `cargo test`
/// load, but this must never hang CI.
const DEADLINE: Duration = Duration::from_secs(20);

/// The positive/handshake-success test: a 3-node cluster where every peer
/// connection is mTLS (each replica presents a cert and verifies its
/// peer's) and every client connection is server-authenticated TLS (a
/// client verifies the replica's cert) forms and serves a real `Put`/`Get`
/// -- the exact scenario `tests/cluster.rs`'s
/// `three_node_cluster_forms_over_tcp_and_serves_put_then_get` proves for
/// plaintext, now over TLS end to end.
#[tokio::test(flavor = "multi_thread")]
async fn three_node_cluster_forms_over_mtls_peers_and_server_tls_clients() {
    let dir = tempfile::tempdir().expect("make a throwaway cert directory");
    let ca = TestCa::new();
    let ca_path = write_pem(dir.path(), "ca.pem", &ca.ca_pem());

    let mut tls_configs = Vec::new();
    for i in 0..3 {
        let (cert_pem, key_pem) = ca.sign_leaf("127.0.0.1");
        let cert_chain_path = write_pem(dir.path(), &format!("node{i}.cert.pem"), &cert_pem);
        let key_path = write_pem(dir.path(), &format!("node{i}.key.pem"), &key_pem);
        tls_configs.push(TlsConfig {
            cert_chain_path,
            key_path,
            ca_path: ca_path.clone(),
        });
    }

    let client_addrs = spawn_cluster_with_tls(3, Some(NodeId(0)), tls_configs);

    let client_tls = build_client_tls(&ClientTlsConfig {
        ca_path: ca_path.clone(),
        expected_server_name: None,
    })
    .expect("build the test client's TLS config");

    let put = Command::Put {
        client: ClientId(1),
        seq: 0,
        key: 42,
        value: 7,
    };
    let put_outcome = submit_with_retry_tls(client_addrs[0], &put, &client_tls, DEADLINE).await;
    assert_eq!(put_outcome, Outcome::Put);

    let get = Command::Get {
        client: ClientId(1),
        seq: 1,
        key: 42,
    };
    // Read from a *different* replica than the one the write went to --
    // only possible to observe correctly if the write was actually
    // replicated over the (now mTLS) peer connections, not merely applied
    // locally -- exactly `tests/cluster.rs`'s equivalent assertion.
    let get_outcome = submit_with_retry_tls(client_addrs[2], &get, &client_tls, DEADLINE).await;
    assert_eq!(get_outcome, Outcome::Get(Some(7)));
}

/// Bind a bare peer-mTLS acceptor (`transport::accept_peers`) over
/// `tls_config`, standalone -- no full cluster/consensus core, just the one
/// piece under test for the negative mTLS tests below. Returns the bound
/// address and the acceptor's raw inbox receiver: every successfully
/// `Hello`-then-`App`-framed connection forwards an [`Event::Message`] to
/// it (see `transport::handle_peer_connection`), so "did anything ever
/// arrive here" is a direct, black-box proxy for "did the acceptor treat
/// this dialer as a legitimate peer" -- see
/// [`dial_and_probe_whether_any_frame_gets_through`], which uses it.
async fn spawn_bare_peer_acceptor(
    tls_config: &TlsConfig,
) -> (
    SocketAddr,
    mpsc::UnboundedReceiver<queso_net::driver::Event>,
) {
    let peer_tls = build_peer_tls(tls_config).expect("build the acceptor's peer TLS config");
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind a peer listener");
    let addr = listener.local_addr().expect("read back the bound address");
    let (inbox_tx, inbox_rx) = mpsc::unbounded_channel();
    tokio::spawn(transport::accept_peers(
        listener,
        inbox_tx,
        Some(peer_tls.server_config),
    ));
    (addr, inbox_rx)
}

/// Dial `addr`, attempt a TLS handshake with `dialer_tls` best-effort, and
/// -- regardless of whether that handshake itself reports success or
/// failure -- attempt to push exactly the frames a real, legitimate dialer
/// would (`WireMsg::Hello` then one `WireMsg::App` frame, mirroring
/// `transport::spawn_peer_dialer`) through whatever stream results, using
/// [`std::result::Result::ok`] to swallow any error from any step (a
/// rejected dialer is *expected* to fail somewhere in here -- exactly
/// where doesn't matter for this helper's contract).
///
/// This is deliberately more thorough than asserting `TlsConnector::connect`
/// itself returns `Err`: TLS 1.3's client role can consider its own
/// handshake "complete" (and so return `Ok` from `connect`) as soon as it
/// has sent its own last flight, *before* it has read the server's
/// rejection alert -- sent only once the server finishes validating the
/// client's certificate message, which arrives after the client already
/// stopped sending. Actually trying to move real frames end to end (and,
/// in the caller, checking the acceptor's inbox never received any of
/// them) is what actually proves "no frames flow", independent of exactly
/// which layer/step the rejection surfaces at.
async fn dial_and_probe_whether_any_frame_gets_through(
    addr: SocketAddr,
    dialer_tls: Arc<rustls::ClientConfig>,
) {
    use bytes::BytesMut;
    use futures_util::SinkExt;
    use queso_consensus::proposal::Proposal;
    use queso_consensus::rpc::{ConcreteMsg, RecordRequest};
    use queso_net::wire::{encode, WireMsg};
    use tokio_util::codec::{Framed, LengthDelimitedCodec};

    let Ok(stream) = tokio::net::TcpStream::connect(addr).await else {
        return;
    };
    let server_name = server_name_for("127.0.0.1", None);
    let Ok(tls_stream) = tokio_rustls::TlsConnector::from(dialer_tls)
        .connect(server_name, stream)
        .await
    else {
        return;
    };
    let mut framed = Framed::new(tls_stream, LengthDelimitedCodec::new());
    if framed
        .send(encode(&WireMsg::Hello(NodeId(99))))
        .await
        .is_err()
    {
        return;
    }
    let probe = ConcreteMsg::Request(RecordRequest {
        slot: 0,
        req_step: 0,
        proposal: Proposal {
            value: Command::Put {
                client: ClientId(1),
                seq: 0,
                key: 0,
                value: 0,
            },
            priority: 0,
            origin: NodeId(99),
        },
    });
    let _ = framed.send(encode(&WireMsg::App(probe))).await;
    let _: Result<Option<BytesMut>, _> = tokio::time::timeout(
        Duration::from_millis(200),
        futures_util::StreamExt::next(&mut framed),
    )
    .await
    .map(|r| r.transpose())
    .unwrap_or(Ok(None));
}

/// After [`dial_and_probe_whether_any_frame_gets_through`], assert the
/// acceptor's inbox never received anything within a generous bound -- the
/// actual "no frames flow" property both negative peer-mTLS tests below
/// exist to prove.
async fn assert_no_frame_ever_arrived(
    inbox_rx: &mut mpsc::UnboundedReceiver<queso_net::driver::Event>,
) {
    let got = tokio::time::timeout(Duration::from_millis(500), inbox_rx.recv()).await;
    let arrived = matches!(got, Ok(Some(_)));
    assert!(
        !arrived,
        "a rejected mTLS dialer's frame must never reach the acceptor's inbox, but one did"
    );
}

/// The core negative test for peer mTLS: a dialer that presents **no**
/// client certificate at all -- plausible if the peer acceptor's
/// client-auth were merely *offered* rather than *required* -- must be
/// rejected: no frame it sends ever reaches the acceptor's inbox. Proves
/// `transport::accept_peers`'s `WebPkiClientVerifier` really does require
/// (not just accept) a client cert, per `crate::tls::build_peer_tls`'s
/// docs.
#[tokio::test(flavor = "multi_thread")]
async fn peer_mtls_rejects_a_dialer_with_no_client_certificate() {
    let dir = tempfile::tempdir().unwrap();
    let ca = TestCa::new();
    let ca_path = write_pem(dir.path(), "ca.pem", &ca.ca_pem());
    let (cert_pem, key_pem) = ca.sign_leaf("127.0.0.1");
    let cert_chain_path = write_pem(dir.path(), "acceptor.cert.pem", &cert_pem);
    let key_path = write_pem(dir.path(), "acceptor.key.pem", &key_pem);
    let acceptor_tls = TlsConfig {
        cert_chain_path,
        key_path,
        ca_path: ca_path.clone(),
    };
    let (addr, mut inbox_rx) = spawn_bare_peer_acceptor(&acceptor_tls).await;

    // A client config that correctly verifies the acceptor's server cert
    // against the real CA, but presents no client certificate -- the one
    // thing an honest, non-mTLS-aware dialer might do.
    let no_client_cert_tls = build_client_tls(&ClientTlsConfig {
        ca_path,
        expected_server_name: None,
    })
    .expect("build a server-cert-only (no client cert) TLS client config");

    dial_and_probe_whether_any_frame_gets_through(addr, no_client_cert_tls).await;
    assert_no_frame_ever_arrived(&mut inbox_rx).await;
}

/// The second negative test for peer mTLS: a dialer that presents a client
/// certificate signed by a **different, untrusted** CA -- not the CA the
/// acceptor was configured to trust -- must also be rejected: no frame it
/// sends ever reaches the acceptor's inbox. Proves the acceptor's
/// `WebPkiClientVerifier` actually chains the presented client cert to the
/// configured CA rather than accepting any cert shape.
#[tokio::test(flavor = "multi_thread")]
async fn peer_mtls_rejects_a_dialer_with_a_cert_from_an_untrusted_ca() {
    let dir = tempfile::tempdir().unwrap();
    let trusted_ca = TestCa::new();
    let trusted_ca_path = write_pem(dir.path(), "trusted-ca.pem", &trusted_ca.ca_pem());
    let (acceptor_cert_pem, acceptor_key_pem) = trusted_ca.sign_leaf("127.0.0.1");
    let acceptor_tls = TlsConfig {
        cert_chain_path: write_pem(dir.path(), "acceptor.cert.pem", &acceptor_cert_pem),
        key_path: write_pem(dir.path(), "acceptor.key.pem", &acceptor_key_pem),
        ca_path: trusted_ca_path.clone(),
    };
    let (addr, mut inbox_rx) = spawn_bare_peer_acceptor(&acceptor_tls).await;

    // A second, entirely independent CA the acceptor was never told to
    // trust, signing the *dialer's* client certificate.
    let untrusted_ca = TestCa::new();
    let (dialer_cert_pem, dialer_key_pem) = untrusted_ca.sign_leaf("127.0.0.1");
    let dialer_tls = TlsConfig {
        cert_chain_path: write_pem(dir.path(), "dialer.cert.pem", &dialer_cert_pem),
        key_path: write_pem(dir.path(), "dialer.key.pem", &dialer_key_pem),
        // Still verifies the *acceptor's* server cert against the real,
        // trusted CA -- this test isolates the client-cert-verification
        // failure specifically, not a server-cert one.
        ca_path: trusted_ca_path,
    };
    let dialer_peer_tls = build_peer_tls(&dialer_tls).expect("build the dialer's mTLS config");

    dial_and_probe_whether_any_frame_gets_through(addr, dialer_peer_tls.client_config).await;
    assert_no_frame_ever_arrived(&mut inbox_rx).await;
}

/// The negative test for the client-facing side: a caller that only trusts
/// a CA *other than* the one that actually signed the replica's server
/// certificate must refuse to talk to it -- proving
/// `crate::tls::build_client_tls`'s server-cert verification is real, not
/// decorative. Uses a bare `client::accept_clients` acceptor (no full
/// consensus core) as the standalone unit under test, mirroring
/// `spawn_bare_peer_acceptor` above.
#[tokio::test(flavor = "multi_thread")]
async fn client_tls_refuses_a_server_cert_signed_by_the_wrong_ca() {
    let dir = tempfile::tempdir().unwrap();
    let real_ca = TestCa::new();
    let (server_cert_pem, server_key_pem) = real_ca.sign_leaf("127.0.0.1");
    let server_tls_config = TlsConfig {
        cert_chain_path: write_pem(dir.path(), "server.cert.pem", &server_cert_pem),
        key_path: write_pem(dir.path(), "server.key.pem", &server_key_pem),
        // Unused by `build_client_facing_server_tls` (it never verifies a
        // client cert), but `TlsConfig` requires a value -- point it at
        // the real CA for realism.
        ca_path: write_pem(dir.path(), "real-ca.pem", &real_ca.ca_pem()),
    };
    let server_config = build_client_facing_server_tls(&server_tls_config)
        .expect("build the client-facing TLS server config");

    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("bind a client listener");
    let addr = listener.local_addr().unwrap();
    let (inbox_tx, _inbox_rx) = mpsc::unbounded_channel();
    tokio::spawn(client::accept_clients(
        listener,
        inbox_tx,
        Some(server_config),
    ));

    // An entirely independent CA the client trusts instead -- NOT the one
    // that signed the server's cert above.
    let wrong_ca = TestCa::new();
    let wrong_ca_path = write_pem(dir.path(), "wrong-ca.pem", &wrong_ca.ca_pem());
    let client_tls = build_client_tls(&ClientTlsConfig {
        ca_path: wrong_ca_path,
        expected_server_name: None,
    })
    .expect("build a TLS client config trusting only the wrong CA");

    let get = Command::Get {
        client: ClientId(1),
        seq: 0,
        key: 1,
    };
    let server_name = server_name_for("127.0.0.1", None);
    // Bounded so a *regression* (the client wrongly accepting the server's
    // cert, then the bare acceptor -- which never drains its inbox -- leaving
    // the submit awaiting a response that never comes) fails fast and
    // diagnostically here, rather than hanging until CI's global test timeout
    // kills the whole binary. A correct refusal returns `Ok(Err(_))` (the
    // handshake was rejected); a timeout (`Err(Elapsed)`) or a success
    // (`Ok(Ok(_))`) are both the failure this test guards against.
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        client::submit_with_tls(addr, &get, &client_tls, server_name),
    )
    .await;
    assert!(
        matches!(result, Ok(Err(_))),
        "a client trusting only an unrelated CA must refuse to talk to a replica whose \
         server cert was signed by a different CA (expected a rejected handshake, i.e. \
         Ok(Err(_))), got: {result:?}"
    );
}
