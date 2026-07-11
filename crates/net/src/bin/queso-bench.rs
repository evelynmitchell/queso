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

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use queso_net::client::{Client, ClientConfig};
use queso_net::metrics::{OpKind, Recorder, Sample};
use queso_smr::{ClientId, Command};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio::task::JoinSet;
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
}

/// One virtual client session: a stable `ClientId` plus the monotonic
/// `seq`/RNG state that must never be touched by two operations at once
/// (A6 -- see `queso_smr::command::ClientSession`'s docs). Owned by exactly
/// one in-flight operation at a time, by construction: closed-loop workers
/// own theirs for the whole run; open-loop mode checks one out of a bounded
/// pool per operation and checks it back in when done (see
/// [`open_loop_run`]).
struct Session {
    id: ClientId,
    seq: u64,
    rng: StdRng,
}

impl Session {
    fn new(idx: usize, seed: u64) -> Self {
        Self {
            id: ClientId(idx as u32),
            seq: 0,
            rng: StdRng::seed_from_u64(seed.wrapping_add(idx as u64)),
        }
    }
}

/// Combined run-length stop condition: either a wall-clock deadline, a
/// target total op count, or both (whichever trips first).
#[derive(Clone, Copy)]
struct StopCondition {
    deadline: Option<Instant>,
    target_ops: Option<u64>,
}

impl StopCondition {
    fn should_stop(&self, op_counter: &AtomicU64) -> bool {
        if let Some(deadline) = self.deadline {
            if Instant::now() >= deadline {
                return true;
            }
        }
        if let Some(target) = self.target_ops {
            if op_counter.load(Ordering::Relaxed) >= target {
                return true;
            }
        }
        false
    }
}

/// Build, submit, and time one operation using (and mutating) `session`'s
/// seq/RNG state, returning the [`Sample`] it produced.
async fn do_one_op(client: &Client, session: &mut Session, keys: u32, read_frac: f64) -> Sample {
    let is_read = session.rng.gen_range(0.0..1.0) < read_frac;
    let key = session.rng.gen_range(0..keys.max(1));
    let seq = session.seq;
    session.seq += 1;

    let (kind, command) = if is_read {
        (
            OpKind::Read,
            Command::Get {
                client: session.id,
                seq,
                key,
            },
        )
    } else {
        let value = session.rng.gen::<i64>();
        (
            OpKind::Write,
            Command::Put {
                client: session.id,
                seq,
                key,
                value,
            },
        )
    };

    let start = Instant::now();
    let result = client.submit(&command).await;
    let latency = start.elapsed();
    Sample {
        kind,
        latency,
        ok: result.is_ok(),
    }
}

/// Closed-loop mode: `concurrency` workers, each owning one [`Session`] for
/// the whole run, looping "submit, wait, submit the next one" until `stop`.
#[allow(clippy::too_many_arguments)]
async fn closed_loop_run(
    client: Arc<Client>,
    concurrency: usize,
    keys: u32,
    read_frac: f64,
    seed: u64,
    stop: StopCondition,
    sample_tx: mpsc::UnboundedSender<Sample>,
    op_counter: Arc<AtomicU64>,
) {
    let mut workers = JoinSet::new();
    for idx in 0..concurrency.max(1) {
        let client = Arc::clone(&client);
        let sample_tx = sample_tx.clone();
        let op_counter = Arc::clone(&op_counter);
        workers.spawn(async move {
            let mut session = Session::new(idx, seed);
            while !stop.should_stop(&op_counter) {
                let sample = do_one_op(&client, &mut session, keys, read_frac).await;
                op_counter.fetch_add(1, Ordering::Relaxed);
                let _ = sample_tx.send(sample);
            }
        });
    }
    while workers.join_next().await.is_some() {}
}

/// Open-loop mode: operations are scheduled on a fixed `1/rate`-second
/// tick. Each tick checks out a [`Session`] from a bounded pool (size
/// `concurrency`) -- if the pool is empty, the operation queues for one,
/// which is where sustained overload shows up as growing outstanding work
/// rather than an unbounded task/session count. An outer admission
/// semaphore (`concurrency * 8` slots) additionally caps how many ticks may
/// be queued waiting on a session at once; a tick that can't get an
/// admission slot is counted as a dropped/failed op immediately instead of
/// piling up forever under an offered rate the cluster can never sustain.
#[allow(clippy::too_many_arguments)]
async fn open_loop_run(
    client: Arc<Client>,
    rate: f64,
    concurrency: usize,
    keys: u32,
    read_frac: f64,
    seed: u64,
    stop: StopCondition,
    sample_tx: mpsc::UnboundedSender<Sample>,
    op_counter: Arc<AtomicU64>,
) {
    let concurrency = concurrency.max(1);
    let period = Duration::from_secs_f64(1.0 / rate.max(0.001));
    let mut ticker = tokio::time::interval(period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);

    let (pool_tx, pool_rx) = mpsc::channel::<Session>(concurrency);
    for idx in 0..concurrency {
        let _ = pool_tx.send(Session::new(idx, seed)).await;
    }
    let pool_rx = Arc::new(Mutex::new(pool_rx));
    let admission = Arc::new(Semaphore::new(concurrency * 8));

    let mut tasks = JoinSet::new();
    loop {
        if stop.should_stop(&op_counter) {
            break;
        }
        ticker.tick().await;
        if stop.should_stop(&op_counter) {
            break;
        }

        let Ok(permit) = Arc::clone(&admission).try_acquire_owned() else {
            // The client-side admission queue is already full: the offered
            // rate is outrunning what `concurrency` sessions plus the
            // buffer can absorb. Count it as a dropped op rather than
            // growing memory unboundedly.
            op_counter.fetch_add(1, Ordering::Relaxed);
            let _ = sample_tx.send(Sample {
                kind: OpKind::Write,
                latency: Duration::ZERO,
                ok: false,
            });
            continue;
        };

        let client = Arc::clone(&client);
        let pool_tx = pool_tx.clone();
        let pool_rx = Arc::clone(&pool_rx);
        let sample_tx = sample_tx.clone();
        let op_counter = Arc::clone(&op_counter);
        tasks.spawn(async move {
            let _permit = permit;
            let mut session = {
                let mut rx = pool_rx.lock().await;
                match rx.recv().await {
                    Some(session) => session,
                    None => return, // pool sender dropped: run is shutting down.
                }
            };
            let sample = do_one_op(&client, &mut session, keys, read_frac).await;
            op_counter.fetch_add(1, Ordering::Relaxed);
            let _ = sample_tx.send(sample);
            let _ = pool_tx.send(session).await;
        });
    }
    while tasks.join_next().await.is_some() {}
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

    let client = Arc::new(Client::with_config(
        args.addrs.clone(),
        ClientConfig {
            attempt_timeout: Duration::from_millis(args.attempt_timeout_ms),
            ..ClientConfig::default()
        },
    ));

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
                Arc::clone(&client),
                rate,
                args.concurrency,
                args.keys,
                args.read_frac,
                args.seed,
                stop,
                sample_tx.clone(),
                Arc::clone(&op_counter),
            )
            .await;
        }
        None => {
            closed_loop_run(
                Arc::clone(&client),
                args.concurrency,
                args.keys,
                args.read_frac,
                args.seed,
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
