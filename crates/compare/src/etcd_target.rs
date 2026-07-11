//! [`EtcdTarget`]: [`crate::target::KvTarget`] over etcd's v3 gRPC-gateway
//! JSON/HTTP API.
//!
//! # Why the HTTP gateway, not `etcd-client`
//!
//! The "real" way to talk to etcd from Rust is the [`etcd-client`
//! crate](https://crates.io/crates/etcd-client), a `tonic`/gRPC client. It
//! was evaluated first and rejected for this crate specifically (not a
//! blanket judgment on the crate itself): it fetches from crates.io without
//! trouble, but its build script (via `prost-build`/`tonic-build`) shells
//! out to an external `protoc` binary, which is not installed in this
//! sandbox by default and is not otherwise a dependency anywhere in this
//! workspace. Requiring a system binary most Rust toolchains don't ship
//! with, just for a comparison harness, would make `cargo build --workspace`
//! newly fragile across contributors'/CI's machines depending on whether
//! `protoc` happens to be on `PATH` -- exactly the kind of footprint this
//! crate's own `Cargo.toml` header promises to avoid. See
//! `docs/compare-etcd.md`'s "What got added, and what didn't" section for
//! the full writeup (including that `protoc` genuinely *can* be
//! `apt-get install`'d, just not assumed).
//!
//! etcd has shipped an embedded [gRPC-gateway](https://etcd.io/docs/v3.5/dev-guide/api_grpc_gateway/)
//! since v3.3 -- an ordinary HTTP/1.1 + JSON translation of the same `Put`/
//! `Range` RPCs `etcd-client` would call, on the *same port* (`2379` by
//! default) as the gRPC API itself, no separate proxy or flag required.
//! [`EtcdTarget`] speaks exactly that: `POST {base}/v3/kv/put` and
//! `POST {base}/v3/kv/range`, both with base64-encoded key/value fields per
//! the gateway's documented JSON mapping of the underlying protobuf
//! (`etcdserverpb.PutRequest`/`RangeRequest`) -- the same wire format
//! `curl -X POST {base}/v3/kv/put -d '{"key": "<base64>", "value":
//! "<base64>"}'` or `etcdctl`'s own HTTP fallback would produce. This
//! crate's own dependency footprint stays small: `reqwest` with every TLS
//! feature disabled (etcd's gateway is plain HTTP unless the cluster has
//! client-cert TLS configured -- see "TLS" below) plus `base64`, versus
//! `etcd-client`'s `tonic`+`prost`+`protoc` tree.
//!
//! # This crate cannot verify this against a real etcd (env constraint)
//!
//! This sandbox has no `etcd`/`etcdctl` installed and the outbound network
//! policy blocks the GitHub release download this module's own doc comment
//! would otherwise point a reader to. [`EtcdTarget`] is real, compiles, and
//! is unit-tested against a tiny in-process fake HTTP server that speaks
//! the exact same wire shape (see the `tests` module below) -- proving the
//! request/response encoding is correct -- but its actual throughput/
//! latency numbers against a real etcd cluster are for the project owner to
//! capture in an environment where etcd is reachable. See
//! `docs/compare-etcd.md` for the exact commands to start etcd and run
//! `queso-compare --target etcd` against it.
//!
//! # TLS
//!
//! No TLS support here, matching `queso-net`'s own current honesty about
//! client-facing TLS (see `crates/net/README.md`'s "Honest limits"): this
//! module only ever dials `http://`, not `https://`. A local/dev etcd
//! (started with etcd's defaults, or `--client-cert-auth=false`) serves the
//! gateway over plain HTTP; a production etcd deployment with client-cert
//! auth enabled is out of scope for this harness, exactly as TLS is out of
//! scope for `queso-net`'s own client port today.

use std::time::Duration;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use serde::{Deserialize, Serialize};

use crate::target::KvTarget;

#[derive(Serialize)]
struct PutRequest {
    key: String,
    value: String,
}

#[derive(Deserialize)]
struct PutResponse {
    #[allow(dead_code)]
    #[serde(default)]
    header: Option<serde_json::Value>,
}

#[derive(Serialize)]
struct RangeRequest {
    key: String,
}

#[derive(Deserialize)]
struct RangeResponse {
    #[serde(default)]
    kvs: Vec<KvPair>,
}

#[derive(Deserialize)]
struct KvPair {
    #[allow(dead_code)]
    #[serde(default)]
    key: String,
    #[serde(default)]
    value: String,
}

/// Drives one etcd cluster's key-value API through its v3 gRPC-gateway JSON/
/// HTTP surface. Values are encoded as the base-10 string form of the `i64`
/// (so `etcdctl get --print-value-only <key>` on a key this wrote shows a
/// plain human-readable integer) -- see `crate::target::KvTarget`'s module
/// docs for why the key/value *shape* is fixed to match Queso's, not why
/// the *encoding* is decimal (that part is this type's own choice, made for
/// operator-readability when cross-checking with `etcdctl` by hand).
pub struct EtcdTarget {
    http: reqwest::Client,
    base_url: String,
}

impl EtcdTarget {
    /// `base_url` is the gateway's HTTP origin, e.g. `http://127.0.0.1:2379`
    /// (etcd's default client port) -- no trailing slash required (any
    /// trailing slash is stripped). `timeout` bounds every individual
    /// request (put or range), the same role
    /// `queso_net::client::ClientConfig::attempt_timeout` plays for
    /// [`crate::queso_target::QuesoTarget`].
    pub fn new(base_url: impl Into<String>, timeout: Duration) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder().timeout(timeout).build()?;
        Ok(Self {
            http,
            base_url: base_url.into().trim_end_matches('/').to_string(),
        })
    }
}

impl KvTarget for EtcdTarget {
    fn name(&self) -> &'static str {
        "etcd"
    }

    async fn put(&self, key: u32, value: i64) -> anyhow::Result<()> {
        let request = PutRequest {
            key: B64.encode(key.to_string()),
            value: B64.encode(value.to_string()),
        };
        let response = self
            .http
            .post(format!("{}/v3/kv/put", self.base_url))
            .json(&request)
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("etcd PUT key={key} failed: HTTP {status}: {body}");
        }
        let _: PutResponse = response.json().await?;
        Ok(())
    }

    async fn get(&self, key: u32) -> anyhow::Result<Option<i64>> {
        let request = RangeRequest {
            key: B64.encode(key.to_string()),
        };
        let response = self
            .http
            .post(format!("{}/v3/kv/range", self.base_url))
            .json(&request)
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("etcd GET key={key} failed: HTTP {status}: {body}");
        }
        let parsed: RangeResponse = response.json().await?;
        match parsed.kvs.into_iter().next() {
            None => Ok(None),
            Some(kv) => {
                let bytes = B64.decode(kv.value).map_err(|err| {
                    anyhow::anyhow!("etcd GET key={key}: bad base64 value: {err}")
                })?;
                let text = String::from_utf8(bytes).map_err(|err| {
                    anyhow::anyhow!("etcd GET key={key}: value was not utf8: {err}")
                })?;
                let value: i64 = text.parse().map_err(|err| {
                    anyhow::anyhow!(
                        "etcd GET key={key}: value {text:?} was not a decimal i64: {err}"
                    )
                })?;
                Ok(Some(value))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    /// A tiny, single-purpose fake HTTP server that speaks just enough of
    /// etcd's gRPC-gateway JSON wire format (`/v3/kv/put`, `/v3/kv/range`)
    /// to prove [`EtcdTarget`]'s request/response encoding round-trips
    /// correctly -- this is a protocol-correctness test for *this crate's*
    /// code, not a performance stand-in for real etcd (see this module's
    /// docs: real etcd numbers are out of scope for this sandbox).
    async fn handle_one(mut stream: TcpStream, store: Arc<Mutex<BTreeMap<String, String>>>) {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        let header_end = loop {
            let n = stream.read(&mut chunk).await.expect("read request");
            assert!(n > 0, "connection closed before a full request arrived");
            buf.extend_from_slice(&chunk[..n]);
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                break pos + 4;
            }
        };
        let header_text = String::from_utf8_lossy(&buf[..header_end]).to_string();
        let content_length: usize = header_text
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse().ok())
                    .flatten()
            })
            .unwrap_or(0);
        while buf.len() < header_end + content_length {
            let n = stream.read(&mut chunk).await.expect("read body");
            assert!(n > 0, "connection closed before the full body arrived");
            buf.extend_from_slice(&chunk[..n]);
        }
        let body = &buf[header_end..header_end + content_length];
        let path = header_text
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .unwrap_or("")
            .to_string();
        let request_json: serde_json::Value = serde_json::from_slice(body).unwrap_or_default();

        let resp_body = if path.ends_with("/v3/kv/put") {
            let key = request_json["key"].as_str().unwrap_or_default().to_string();
            let value = request_json["value"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            store.lock().unwrap().insert(key, value);
            serde_json::json!({"header": {}}).to_string()
        } else if path.ends_with("/v3/kv/range") {
            let key = request_json["key"].as_str().unwrap_or_default().to_string();
            match store.lock().unwrap().get(&key).cloned() {
                Some(value) => {
                    serde_json::json!({"header": {}, "kvs": [{"key": key, "value": value}], "count": "1"})
                        .to_string()
                }
                None => serde_json::json!({"header": {}, "count": "0"}).to_string(),
            }
        } else {
            serde_json::json!({"error": format!("unhandled path {path}")}).to_string()
        };

        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            resp_body.len(),
            resp_body
        );
        let _ = stream.write_all(response.as_bytes()).await;
        let _ = stream.shutdown().await;
    }

    async fn spawn_fake_gateway() -> std::net::SocketAddr {
        let listener = TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind fake gateway");
        let addr = listener.local_addr().expect("read fake gateway addr");
        let store = Arc::new(Mutex::new(BTreeMap::new()));
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(handle_one(stream, Arc::clone(&store)));
            }
        });
        addr
    }

    #[tokio::test]
    async fn put_then_get_round_trips_through_the_json_gateway_wire_format() {
        let addr = spawn_fake_gateway().await;
        let target = EtcdTarget::new(format!("http://{addr}"), Duration::from_secs(2))
            .expect("build target");

        target.put(42, 777).await.expect("put must succeed");
        let got = target.get(42).await.expect("get must succeed");
        assert_eq!(got, Some(777));
    }

    #[tokio::test]
    async fn get_on_a_never_written_key_returns_none() {
        let addr = spawn_fake_gateway().await;
        let target = EtcdTarget::new(format!("http://{addr}"), Duration::from_secs(2))
            .expect("build target");

        let got = target.get(999).await.expect("get must succeed");
        assert_eq!(got, None);
    }

    #[test]
    fn base_url_strips_a_trailing_slash() {
        let target = EtcdTarget::new("http://127.0.0.1:2379/", Duration::from_secs(1)).unwrap();
        assert_eq!(target.base_url, "http://127.0.0.1:2379");
    }
}
