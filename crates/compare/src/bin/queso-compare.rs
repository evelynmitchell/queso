// Real wall-clock timing plus real network I/O against either target is
// exactly this binary's job -- same per-crate-root allow as
// `queso-net`'s `src/bin/queso-bench.rs` (`clippy.toml`'s
// `disallowed-methods` list is enforced per-crate-root, so each binary
// target needs this independently of `src/lib.rs`'s).
#![allow(clippy::disallowed_methods)]

//! `queso-compare`: drive either Queso or etcd through the exact same
//! workload (`queso_compare::workload::run_workload`) and print a
//! `queso_net::metrics::Summary` -- the same type, same JSON/CSV shape,
//! `queso-bench` emits, so two runs (one per target) are directly diffable.
//! See `docs/compare-etcd.md` for worked examples and the captured
//! Queso-side numbers.
//!
//! ```sh
//! # Queso side (against a local cluster booted per crates/net/README.md):
//! queso-compare --target queso \
//!   --queso-addr 127.0.0.1:8000 --queso-addr 127.0.0.1:8001 --queso-addr 127.0.0.1:8002 \
//!   --concurrency 16 --read-frac 0.5 --keys 1000 --duration-secs 8 --output json
//!
//! # etcd side (against a local etcd started per docs/compare-etcd.md):
//! queso-compare --target etcd --etcd-url http://127.0.0.1:2379 \
//!   --concurrency 16 --read-frac 0.5 --keys 1000 --duration-secs 8 --output json
//! ```

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use queso_compare::workload::{run_workload, StopCondition, WorkloadConfig};
use queso_compare::{EtcdTarget, QuesoTarget};
use queso_net::client::{Client, ClientConfig};

#[derive(Copy, Clone, Debug, ValueEnum)]
enum OutputFormat {
    Text,
    Json,
    Csv,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum TargetKind {
    Queso,
    Etcd,
}

/// Flags deliberately mirror `queso-bench`'s (see `crates/net/README.md`)
/// wherever the same dimension applies to both targets, so the same values
/// on both invocations really is the same offered load.
#[derive(Parser, Debug)]
#[command(
    name = "queso-compare",
    about = "Phase 7.5: drive Queso or etcd through the same workload/metrics harness"
)]
struct Args {
    /// Which system to drive this run.
    #[arg(long, value_enum)]
    target: TargetKind,

    /// A Queso replica's client-port address (repeatable). Required (and
    /// only used) for `--target queso`.
    #[arg(long = "queso-addr")]
    queso_addrs: Vec<SocketAddr>,

    /// etcd's gRPC-gateway HTTP origin, e.g. `http://127.0.0.1:2379`
    /// (etcd's default client port). Required (and only used) for
    /// `--target etcd`. See `docs/compare-etcd.md` for how to start etcd
    /// and confirm this is reachable before running against it.
    #[arg(long)]
    etcd_url: Option<String>,

    /// Open-loop target rate in ops/sec. Omit for closed-loop mode.
    #[arg(long)]
    rate: Option<f64>,

    /// Closed-loop: worker count. Open-loop: in-flight cap.
    #[arg(long, default_value_t = 16)]
    concurrency: usize,

    /// Fraction of operations that are reads, in `[0.0, 1.0]`.
    #[arg(long, default_value_t = 0.5)]
    read_frac: f64,

    /// Key-space size.
    #[arg(long, default_value_t = 1000)]
    keys: u32,

    /// Run length: stop after this many seconds.
    #[arg(long)]
    duration_secs: Option<u64>,

    /// Run length: stop after (approximately) this many total operations.
    /// At least one of `--duration-secs`/`--ops` is required.
    #[arg(long)]
    ops: Option<u64>,

    /// Output format for the final summary.
    #[arg(long, value_enum, default_value_t = OutputFormat::Text)]
    output: OutputFormat,

    /// PRNG seed for this run's key/value/read-vs-write choices.
    #[arg(long, default_value_t = 1)]
    seed: u64,

    /// Per-attempt timeout: how long `--target queso` waits on one replica
    /// address before retrying another, and how long `--target etcd` waits
    /// on one HTTP request before failing it.
    #[arg(long, default_value_t = 2000)]
    attempt_timeout_ms: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    let args = Args::parse();

    anyhow::ensure!(
        (0.0..=1.0).contains(&args.read_frac),
        "--read-frac must be within [0.0, 1.0]"
    );
    anyhow::ensure!(
        args.duration_secs.is_some() || args.ops.is_some(),
        "at least one of --duration-secs/--ops is required"
    );

    let cfg = WorkloadConfig {
        rate: args.rate,
        concurrency: args.concurrency,
        read_frac: args.read_frac,
        keys: args.keys,
        seed: args.seed,
    };
    let stop = StopCondition {
        deadline: args
            .duration_secs
            .map(|s| Instant::now() + Duration::from_secs(s)),
        target_ops: args.ops,
    };
    let attempt_timeout = Duration::from_millis(args.attempt_timeout_ms);

    let (target_name, summary) = match args.target {
        TargetKind::Queso => {
            anyhow::ensure!(
                !args.queso_addrs.is_empty(),
                "--target queso requires at least one --queso-addr"
            );
            let client = Client::with_config(
                args.queso_addrs.clone(),
                ClientConfig {
                    attempt_timeout,
                    ..ClientConfig::default()
                },
            );
            let target = Arc::new(QuesoTarget::new(client));
            ("queso", run_workload(target, cfg, stop).await)
        }
        TargetKind::Etcd => {
            let url = args
                .etcd_url
                .clone()
                .ok_or_else(|| anyhow::anyhow!("--target etcd requires --etcd-url"))?;
            let target = Arc::new(EtcdTarget::new(url, attempt_timeout)?);
            ("etcd", run_workload(target, cfg, stop).await)
        }
    };

    tracing::info!(target_name, "run complete");
    match args.output {
        OutputFormat::Text => print!("{}", summary.to_text()),
        OutputFormat::Json => println!("{}", summary.to_json()),
        OutputFormat::Csv => print!("{}", summary.to_csv()),
    }

    Ok(())
}
