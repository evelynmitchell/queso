//! `queso-soak` -- the long-soak driver (Phase 9.2, issue #56, slice 3).
//!
//! The test suite's bounded soak is sized for a commit gate: one seed,
//! twenty seconds. This binary is the other mode -- run many seeds for as
//! long as you are willing to wait, and report which ones broke.
//!
//! ```sh
//! cargo build --all
//! cargo run -p queso-soak --bin queso-soak -- --seeds 20 --duration-secs 120
//! ```
//!
//! Exits non-zero if any seed found a safety or liveness violation, so it
//! can be a nightly job as easily as a manual hunt.
//!
//! # What a failure gives you
//!
//! The seed and the rendered schedule, which reproduces the *turbulence*.
//! It does not reproduce the failure: real thread scheduling, real timers
//! and real TCP make the interleaving irreproducible, and pretending
//! otherwise would be the most tempting lie in this phase. What a seed buys
//! is a narrowed search -- re-run it, and you are re-running the same fault
//! sequence rather than a fresh one.

use std::process::ExitCode;
use std::time::Duration;

use clap::Parser;

use queso_soak::cluster::{ClusterConfig, RealCluster};
use queso_soak::schedule::ScheduleConfig;
use queso_soak::soak::{Soak, SoakConfig, SoakReport};

#[derive(Parser, Debug)]
#[command(
    name = "queso-soak",
    about = "Long-running Chain-of-Blocks soak against real queso-node processes"
)]
struct Args {
    /// How many seeds to run, starting at `--first-seed`.
    #[arg(long, default_value_t = 5)]
    seeds: u64,

    /// First fault-schedule seed. Change it to explore fresh turbulence;
    /// keep it to re-run the same set.
    #[arg(long, default_value_t = 0)]
    first_seed: u64,

    /// Seconds of turbulence per seed.
    #[arg(long, default_value_t = 60)]
    duration_secs: u64,

    /// Replicas per cluster. Odd values only in practice: the schedule
    /// keeps faults within `f = (n-1)/2`, which is 0 for n=2.
    #[arg(long, default_value_t = 3)]
    replicas: usize,

    /// Chain checkpoint spacing. Denser sampling means more cross-replica
    /// comparisons and a less vacuous safety verdict; 9.1 measured
    /// frontier-only sampling collapsing 20 comparisons to 2.
    #[arg(long, default_value_t = 2)]
    checkpoint_every: u64,

    /// Keep going after a seed fails, to find out how many of them do.
    #[arg(long)]
    keep_going: bool,
}

fn main() -> ExitCode {
    let args = Args::parse();
    if args.replicas < 3 {
        eprintln!(
            "--replicas {} tolerates no faults at all (f = (n-1)/2 = {}), so \
             the schedule would be empty and every verdict vacuous",
            args.replicas,
            args.replicas.saturating_sub(1) / 2
        );
        return ExitCode::FAILURE;
    }

    let mut failures: Vec<u64> = Vec::new();
    for seed in args.first_seed..args.first_seed + args.seeds {
        println!("=== seed {seed} ===");
        match run_seed(&args, seed) {
            Ok((report, config)) => {
                print!("{}", report.render());
                // `problems`, not `is_clean`: a seed whose run observed
                // nothing is a failed seed, not a clean one. Reporting a
                // vacuous run as clean is the single worst thing a soak
                // can do, because it is indistinguishable from success.
                let problems = report.problems(&config);
                if !problems.is_empty() {
                    for problem in &problems {
                        println!("  {problem}");
                    }
                    println!("{}", report.schedule.render());
                    println!("{}", report.observer_report);
                    failures.push(seed);
                    if !args.keep_going {
                        break;
                    }
                }
            }
            Err(e) => {
                // A harness failure is not a Queso failure, and reporting
                // it as one would be the worst possible signal from a soak.
                eprintln!("seed {seed}: harness error, not a verdict: {e:#}");
                if !args.keep_going {
                    return ExitCode::FAILURE;
                }
            }
        }
    }

    if failures.is_empty() {
        println!("all {} seed(s) clean and non-vacuous", args.seeds);
        ExitCode::SUCCESS
    } else {
        println!("FAILED seeds: {failures:?}");
        ExitCode::FAILURE
    }
}

fn run_seed(args: &Args, seed: u64) -> anyhow::Result<(SoakReport, SoakConfig)> {
    let duration_ms = args.duration_secs * 1_000;
    let config = SoakConfig {
        fault_seed: seed,
        workload_seed: 0xc0b_9003 ^ seed,
        schedule: ScheduleConfig {
            replicas: args.replicas,
            duration_ms,
            min_fault_ms: 600,
            max_fault_ms: 3_000,
            min_gap_ms: 500,
            max_gap_ms: 2_500,
        },
        step_ms: 100,
        submits_per_step: 3,
        converge_rounds: 12,
        converge_advance_ms: 200,
        liveness_budget_ms: 5_000,
        // See `tests/sustained_soak.rs` for how these were sized: the
        // frontier carries the "writes really happened" claim because it is
        // stable across machines, while the acknowledgement count is not.
        min_comparisons: 10 * duration_ms / 1_000,
        min_frontier: 10 * duration_ms / 1_000,
        min_acked: 2 * duration_ms / 1_000,
    };

    let cluster_config = ClusterConfig {
        replicas: args.replicas,
        leader: 0,
        checkpoint_every: args.checkpoint_every,
        tick_ms: 5,
        submit_timeout: Duration::from_secs(4),
    };

    let data_dir = tempfile::tempdir()?;
    let mut cluster = RealCluster::start(cluster_config, data_dir.path())?;
    cluster.await_ready(Duration::from_secs(45))?;

    let soak = Soak::new(config.clone());
    println!("{}", soak.schedule().render());
    Ok((soak.run(&mut cluster), config))
}
