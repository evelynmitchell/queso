// This binary is part of `queso-net`, the workspace's deliberate real-I/O
// boundary crate (see `src/lib.rs`'s crate docs and its own identical
// `#![allow(...)]`) -- real wall-clock time via `std::time::Instant` is
// exactly `queso-bench`'s job (measuring real latency/throughput), not a
// determinism bug. `clippy.toml`'s `disallowed-methods` list is enforced
// per-crate-root, so each binary target needs this allow independently of
// `src/lib.rs`'s.
#![allow(clippy::disallowed_methods)]

//! `queso-bench`: an open- or closed-loop load generator against a Queso
//! cluster's client ports (Phase 7.2), reporting throughput and read/write
//! latency histograms.
//!
//! See `crates/net/README.md` for a worked example against a local 3-node
//! cluster. Two workload modes, selected by whether `--rate` is set:
//!
//! - **Closed-loop** (default, no `--rate`): `--concurrency` worker tasks,
//!   each with its own `queso_smr::ClientId` and monotonic `seq` (per A6's
//!   one-in-flight-per-session precondition -- see
//!   `queso_smr::command::ClientSession`'s docs), loop "submit, wait for the
//!   response, submit the next one" as fast as the cluster answers. Offered
//!   load self-limits to whatever `--concurrency` outstanding requests can
//!   sustain.
//! - **Open-loop** (`--rate <ops/sec>` set): operations are scheduled on a
//!   fixed real-time tick regardless of how long prior ones took, so an
//!   overloaded cluster shows up as rising latency (queueing) rather than
//!   throughput silently capping at whatever the closed loop happened to
//!   sustain. `--concurrency` still bounds how many client sessions
//!   (hence how many operations) may be in flight at once; scheduled ticks
//!   beyond that cap queue for a free session rather than growing the
//!   in-flight count unboundedly.
//!
//! Every completed (or failed/timed-out) operation is turned into a
//! `queso_net::metrics::Sample` and funneled through a single collector
//! task into one `queso_net::metrics::Recorder` (see that module's docs for
//! why a single collector, not a shared/locked histogram, is the simpler
//! design here).
//!
//! # Where the actual scheduling lives
//!
//! In [`queso_net::bench`], not here. This file is the CLI: flags, a
//! [`ClientTarget`] adapter, and printing. The schedulers moved into the
//! library because their two most important properties -- that queue wait
//! is counted in latency, and that shed operations are attributed to the
//! right read/write side -- are only observable under sustained overload,
//! and a binary's internals cannot be reached from a test at all. Issue #40
//! and that module's docs cover it.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use queso_net::bench::{closed_loop_run, open_loop_run, OpTarget, StopCondition, WorkloadConfig};
use queso_net::client::{Client, ClientConfig};
use queso_net::metrics::{Recorder, Sample};
use queso_net::tls::ClientTlsConfig;
use queso_smr::Command;
use tokio::sync::mpsc;
use tracing::warn;

#[derive(Copy, Clone, Debug, ValueEnum)]
enum OutputFormat {
    Text,
    Json,
    Csv,
}

/// Load generator configuration. Flags map directly onto the workload
/// dimensions Phase 7.2 asks for: target addresses (a pool, not one node),
/// rate and/or concurrency (open- vs closed-loop), read/write mix,
/// key-space size, value size, and run length.
#[derive(Parser, Debug)]
#[command(
    name = "queso-bench",
    about = "Load generator + latency/throughput metrics for a Queso cluster"
)]
struct Args {
    /// A replica's client-port address to target (repeatable: `--addr
    /// 127.0.0.1:8000 --addr 127.0.0.1:8001 ...`). At least one required;
    /// listing every replica lets the client library's
    /// retry-to-another-replica policy actually have somewhere to retry.
    #[arg(long = "addr", required = true)]
    addrs: Vec<SocketAddr>,

    /// Open-loop target rate in ops/sec, issued on a fixed real-time
    /// schedule. Omit for closed-loop mode (offered load = whatever
    /// `--concurrency` outstanding requests can sustain).
    #[arg(long)]
    rate: Option<f64>,

    /// Closed-loop mode: number of worker tasks, each with its own
    /// client session, looping request-then-wait. Open-loop mode
    /// (`--rate` set): the cap on operations in flight at once (additional
    /// scheduled ticks queue for a free session rather than growing
    /// in-flight count unboundedly).
    #[arg(long, default_value_t = 16)]
    concurrency: usize,

    /// Fraction of operations that are reads (`Get`), in `[0.0, 1.0]`. The
    /// remainder are writes (`Put`).
    #[arg(long, default_value_t = 0.5)]
    read_frac: f64,

    /// Number of distinct keys to spread load over (uniformly at random).
    #[arg(long, default_value_t = 1000)]
    keys: u32,

    /// Nominal value size in bytes, accepted for config-surface parity with
    /// other load generators. `queso_smr::Command`'s value type
    /// (`queso_smr::Value`) is a fixed 8-byte `i64` in the current schema
    /// (Phase 7.2 does not change `queso-smr`'s wire types -- see the crate
    /// docs' scope note), so this flag currently has no effect on wire
    /// size; a value other than 8 only logs a one-time warning.
    #[arg(long, default_value_t = 8)]
    value_size: usize,

    /// Run length: stop after this many seconds of wall-clock time.
    #[arg(long)]
    duration_secs: Option<u64>,

    /// Run length: stop after (approximately -- concurrent workers may
    /// overshoot slightly) this many total operations. At least one of
    /// `--duration-secs`/`--ops` is required; if both are given, the run
    /// stops at whichever limit is reached first.
    #[arg(long)]
    ops: Option<u64>,

    /// Output format for the final summary.
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    output: OutputFormat,

    /// PRNG seed for this run's key/value/read-vs-write choices. Note this
    /// is real I/O boundary code (see the crate docs), so this only makes
    /// the *workload shape* reproducible run-to-run, not the cluster's own
    /// timing/scheduling behavior.
    #[arg(long, default_value_t = 1)]
    seed: u64,

    /// How long `queso_net::client::Client` waits on one attempt against
    /// one address before retrying against another.
    #[arg(long, default_value_t = 2000)]
    attempt_timeout_ms: u64,

    /// Phase 8.2a (issue #47): PEM file containing the CA certificate(s)
    /// trusted to sign a replica's TLS server certificate. Setting this
    /// enables server-authenticated TLS (see `queso_net::tls`'s module
    /// docs) for every connection this run makes; omit for plaintext (the
    /// default, unchanged from before this flag existed). Client-cert auth
    /// is out of scope for `queso-bench` -- it is not a cluster member.
    #[arg(long)]
    tls_ca: Option<PathBuf>,

    /// Only consulted when `--tls-ca` is set: pin full server-name
    /// verification to this exact name instead of the default chain-only
    /// verification (see `queso_net::tls::ClientTlsConfig::expected_server_name`'s
    /// docs for when you would want this).
    #[arg(long)]
    tls_server_name: Option<String>,
}

/// A [`Client`] as an [`OpTarget`], so `queso_net::bench`'s schedulers can
/// drive a real cluster.
///
/// That indirection is the whole reason the schedulers moved into the
/// library: a test cannot make a real cluster reliably slower than the
/// offered rate, and both properties worth regression-testing here
/// (coordinated omission, drop attribution) only appear under sustained
/// overload. See `queso_net::bench`'s module docs.
struct ClientTarget(Arc<Client>);

impl OpTarget for ClientTarget {
    fn submit<'a>(
        &'a self,
        command: Command,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + 'a>> {
        Box::pin(async move { self.0.submit(&command).await.is_ok() })
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    let args = Args::parse();

    anyhow::ensure!(!args.addrs.is_empty(), "at least one --addr is required");
    anyhow::ensure!(
        (0.0..=1.0).contains(&args.read_frac),
        "--read-frac must be within [0.0, 1.0]"
    );
    anyhow::ensure!(
        args.duration_secs.is_some() || args.ops.is_some(),
        "at least one of --duration-secs/--ops is required"
    );
    if args.value_size != 8 {
        warn!(
            requested = args.value_size,
            "--value-size has no effect: queso_smr::Value is a fixed 8-byte i64 in the current schema"
        );
    }

    // Phase 8.2a (issue #47): `--tls-ca` opts every connection this run
    // makes into server-authenticated TLS (see `queso_net::tls`'s module
    // docs); omitted, `tls` stays `None` and `Client` behaves exactly as
    // before this flag existed.
    let tls = match &args.tls_ca {
        None => None,
        Some(ca_path) => Some(queso_net::tls::build_client_tls(&ClientTlsConfig {
            ca_path: ca_path.clone(),
            expected_server_name: args.tls_server_name.clone(),
        })?),
    };

    let client = Arc::new(Client::with_config(
        args.addrs.clone(),
        ClientConfig {
            attempt_timeout: Duration::from_millis(args.attempt_timeout_ms),
            tls,
            tls_server_name: args.tls_server_name.clone(),
            ..ClientConfig::default()
        },
    ));

    let target: Arc<dyn OpTarget> = Arc::new(ClientTarget(Arc::clone(&client)));
    let workload = WorkloadConfig {
        concurrency: args.concurrency,
        keys: args.keys,
        read_frac: args.read_frac,
        seed: args.seed,
    };
    let stop = StopCondition {
        deadline: args
            .duration_secs
            .map(|s| Instant::now() + Duration::from_secs(s)),
        target_ops: args.ops,
    };
    let op_counter = Arc::new(AtomicU64::new(0));
    let (sample_tx, mut sample_rx) = mpsc::unbounded_channel::<Sample>();

    let collector = tokio::spawn(async move {
        let mut recorder = Recorder::new();
        while let Some(sample) = sample_rx.recv().await {
            recorder.record(sample);
        }
        recorder
    });

    let start = Instant::now();
    match args.rate {
        Some(rate) => {
            open_loop_run(
                Arc::clone(&target),
                rate,
                workload,
                stop,
                sample_tx.clone(),
                Arc::clone(&op_counter),
            )
            .await;
        }
        None => {
            closed_loop_run(
                Arc::clone(&target),
                workload,
                stop,
                sample_tx.clone(),
                Arc::clone(&op_counter),
            )
            .await;
        }
    }
    let elapsed = start.elapsed();
    drop(sample_tx);

    let recorder = collector.await.expect("collector task panicked");
    let summary = recorder.summarize(elapsed);

    match args.output {
        OutputFormat::Text => print!("{}", summary.to_text()),
        OutputFormat::Json => println!("{}", summary.to_json()),
        OutputFormat::Csv => print!("{}", summary.to_csv()),
    }

    Ok(())
}
