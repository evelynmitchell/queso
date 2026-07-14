// See `crates/net/src/bin/queso-bench.rs`'s identical allow for why this is
// needed per-binary-target, not just at `src/lib.rs`: `clippy.toml`'s
// `disallowed-methods` list is enforced per crate root, and this binary is
// its own crate root. `queso_net::admin::default_seq` (real wall-clock time,
// this crate's deliberate real-I/O boundary -- see `src/lib.rs`'s docs) is
// exactly the kind of real-time read that lint exists to catch everywhere
// except here.
#![allow(clippy::disallowed_methods)]

//! `queso-admin`: an out-of-cluster operator CLI (Phase 8.2d, issue #47).
//!
//! Thin wrapper over `queso_net::admin` -- see that module's docs for the
//! full design (the admin `ClientId`/`seq` conventions, why there is no
//! "trigger catch-up" subcommand, and the dependency-light `/metrics`
//! fetch). This binary itself is just `clap` argument parsing plus printing.
//!
//! See `crates/net/README.md`'s `queso-admin` section for worked examples.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, Subcommand};
use queso_net::admin::{self, DEFAULT_ADMIN_CLIENT_ID};
use queso_net::client::{Client, ClientConfig};
use queso_net::tls::ClientTlsConfig;
use queso_smr::ClientId;

#[derive(Parser, Debug)]
#[command(
    name = "queso-admin",
    about = "Out-of-cluster operator CLI for a running Queso cluster"
)]
struct Args {
    #[command(subcommand)]
    command: AdminCommand,
}

#[derive(Subcommand, Debug)]
enum AdminCommand {
    /// Poll every replica's status/metrics endpoint (`GET /metrics`) and
    /// render a cluster-health table: reachability, readiness, log
    /// frontier (`next_slot`), save count, and uptime, plus a rollup of
    /// how many replicas are reachable and whether their frontiers agree.
    /// An unreachable or malformed-response replica is reported as such,
    /// never a fatal error for the whole command.
    Status {
        /// A replica's status-server address (repeatable: `--status-addr
        /// 127.0.0.1:9000 --status-addr 127.0.0.1:9001 ...`). At least one
        /// required. Always plaintext HTTP -- the status port never speaks
        /// TLS (see `queso_net::status`'s module docs).
        #[arg(long = "status-addr", required = true)]
        status_addrs: Vec<SocketAddr>,

        /// How long to wait for each replica's `GET /metrics` before
        /// treating it as unreachable. Replicas are polled concurrently, so
        /// this bounds the whole command's wall-clock time, not
        /// `timeout * replica count`.
        #[arg(long, default_value_t = 3000)]
        timeout_ms: u64,
    },

    /// Check one replica's liveness (`GET /health`) and readiness
    /// (`GET /ready`) directly -- a cheap convenience alongside `status`
    /// for a caller who only cares about one replica. See
    /// `queso_net::status`'s module docs for `/ready`'s precise, honest
    /// meaning (not a linearizable-read guarantee).
    Health {
        /// The replica's status-server address.
        #[arg(long = "status-addr")]
        status_addr: SocketAddr,

        /// How long to wait for each of `/health`/`/ready` before giving up.
        #[arg(long, default_value_t = 3000)]
        timeout_ms: u64,
    },

    /// Submit a `Put(key, value)` against the cluster's client ports via
    /// `queso_net::client::Client` (pooled addresses,
    /// retry-to-another-replica -- the same path `queso-bench` uses).
    Put {
        /// The key to write.
        key: u32,
        /// The value to write.
        value: i64,

        #[command(flatten)]
        target: ClientTarget,
    },

    /// Submit a `Get(key)` against the cluster's client ports and print the
    /// value observed (or `None` if the key was never written).
    Get {
        /// The key to read.
        key: u32,

        #[command(flatten)]
        target: ClientTarget,
    },
}

/// Flags shared by `put`/`get`: which replicas to talk to, this admin
/// operation's `(ClientId, seq)` tag (see `queso_net::admin`'s module docs
/// for both conventions), retry timing, and optional TLS -- mirroring
/// `queso-bench`'s `--tls-ca`/`--tls-server-name` for consistency across
/// this crate's client-facing tools.
#[derive(clap::Args, Debug)]
struct ClientTarget {
    /// A replica's client-port address (repeatable: `--addr
    /// 127.0.0.1:8000 --addr 127.0.0.1:8001 ...`). At least one required;
    /// listing every replica lets `Client`'s retry-to-another-replica
    /// policy actually have somewhere to retry.
    #[arg(long = "addr", required = true)]
    addrs: Vec<SocketAddr>,

    /// The `ClientId` this admin operation is tagged with (A6 dedup space).
    /// Defaults to `queso_net::admin::DEFAULT_ADMIN_CLIENT_ID`
    /// (`u32::MAX - 1` -- deliberately not `u32::MAX` itself, which
    /// `queso_smr::replica` reserves for its own internal catch-up probes),
    /// a documented-but-not-enforced reservation distinct from an ordinary
    /// application's client ids -- override this if your application also
    /// uses that id. See `queso_net::admin`'s module docs.
    #[arg(long, default_value_t = DEFAULT_ADMIN_CLIENT_ID.0)]
    client_id: u32,

    /// The `seq` this admin operation is tagged with. Omit to use
    /// `queso_net::admin::default_seq`'s wall-clock-derived best-effort
    /// default (see that function's docs for its limits); pass this
    /// explicitly for guaranteed-fresh `seq`s in scripted usage.
    #[arg(long)]
    seq: Option<u64>,

    /// How long `Client` waits on one attempt against one address before
    /// retrying against another.
    #[arg(long, default_value_t = 2000)]
    attempt_timeout_ms: u64,

    /// PEM file containing the CA certificate(s) trusted to sign a
    /// replica's TLS server certificate. Setting this enables
    /// server-authenticated TLS (see `queso_net::tls`'s module docs) for
    /// this operation; omit for plaintext (the default). The status port
    /// (`queso-admin status`/`health`) never uses this -- it is always
    /// plaintext HTTP, see `queso_net::status`'s module docs.
    #[arg(long)]
    tls_ca: Option<PathBuf>,

    /// Only consulted when `--tls-ca` is set: pin full server-name
    /// verification to this exact name instead of the default chain-only
    /// verification (see `queso_net::tls::ClientTlsConfig::expected_server_name`).
    #[arg(long)]
    tls_server_name: Option<String>,
}

impl ClientTarget {
    fn build_client(&self) -> anyhow::Result<Client> {
        let tls = match &self.tls_ca {
            None => None,
            Some(ca_path) => Some(queso_net::tls::build_client_tls(&ClientTlsConfig {
                ca_path: ca_path.clone(),
                expected_server_name: self.tls_server_name.clone(),
            })?),
        };
        Ok(Client::with_config(
            self.addrs.clone(),
            ClientConfig {
                attempt_timeout: Duration::from_millis(self.attempt_timeout_ms),
                tls,
                tls_server_name: self.tls_server_name.clone(),
                ..ClientConfig::default()
            },
        ))
    }

    fn client_id(&self) -> ClientId {
        ClientId(self.client_id)
    }

    fn seq(&self) -> u64 {
        self.seq.unwrap_or_else(admin::default_seq)
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    let args = Args::parse();

    match args.command {
        AdminCommand::Status {
            status_addrs,
            timeout_ms,
        } => {
            anyhow::ensure!(
                !status_addrs.is_empty(),
                "at least one --status-addr is required"
            );
            let timeout = Duration::from_millis(timeout_ms);
            let statuses = admin::fetch_cluster_status(&status_addrs, timeout).await;
            let summary = admin::summarize(&statuses);
            print!("{}", admin::render_status_table(&statuses, &summary));
            // Non-zero exit iff no replica at all was reachable -- a
            // scriptable "is this cluster up?" signal, distinct from an
            // in-band lagging/not-ready replica (still reported, still
            // exit 0: a degraded-but-partially-healthy cluster is not the
            // same failure as this command being unable to see anything).
            if summary.reachable == 0 {
                anyhow::bail!("no replica was reachable");
            }
        }
        AdminCommand::Health {
            status_addr,
            timeout_ms,
        } => {
            let timeout = Duration::from_millis(timeout_ms);
            let (health_code, _) = admin::http_get(status_addr, "/health", timeout).await?;
            let (ready_code, _) = admin::http_get(status_addr, "/ready", timeout).await?;
            println!(
                "{status_addr}: health={} ready={}",
                if health_code == 200 { "ok" } else { "FAIL" },
                if ready_code == 200 {
                    "ready"
                } else {
                    "not-ready"
                },
            );
            if health_code != 200 {
                anyhow::bail!("{status_addr} failed its /health check (HTTP {health_code})");
            }
        }
        AdminCommand::Put { key, value, target } => {
            let client = target.build_client()?;
            let outcome = admin::put(&client, target.client_id(), target.seq(), key, value).await?;
            println!("{outcome:?}");
        }
        AdminCommand::Get { key, target } => {
            let client = target.build_client()?;
            let outcome = admin::get(&client, target.client_id(), target.seq(), key).await?;
            println!("{outcome:?}");
        }
    }

    Ok(())
}
