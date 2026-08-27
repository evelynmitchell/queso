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

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::Parser;

use queso_soak::cluster::{ClusterConfig, RealCluster};
use queso_soak::evidence::retain_evidence;
use queso_soak::postmortem::{claims_from, Claim, Postmortem};
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

    /// Which replica gets the §4.2.5 leader fast path.
    ///
    /// Exposed for #83. Every occurrence of that Agreement violation so far
    /// has been node 0 applying its own restart catch-up probe at a slot the
    /// majority decided differently -- and node 0 is also, in every run so
    /// far, the leader. Those two explanations are indistinguishable while
    /// the leader is hard-coded: re-running a failing seed window with a
    /// different leader separates them. If the divergence follows the
    /// leader, the probe carrying the leader's reserved priority `H` is
    /// implicated; if it stays on node 0, that is a red herring.
    #[arg(long, default_value_t = 0)]
    leader: u32,

    /// Chain checkpoint spacing. Denser sampling means more cross-replica
    /// comparisons and a less vacuous safety verdict; 9.1 measured
    /// frontier-only sampling collapsing 20 comparisons to 2.
    #[arg(long, default_value_t = 2)]
    checkpoint_every: u64,

    /// Keep going after a seed fails, to find out how many of them do.
    #[arg(long)]
    keep_going: bool,

    /// Where a failing seed's cluster state is preserved (issue #73).
    ///
    /// Each seed runs against `<dir>/seed-<n>`. A clean seed's directory is
    /// deleted; a failing one is kept, because the replicas' durable
    /// snapshots are the only thing that can settle whether a reported
    /// divergence is a real Agreement violation or an artifact of the
    /// observability path.
    ///
    /// **Each seed's directory is recreated from scratch**, so a re-run of
    /// the same seed removes the previous attempt's evidence first. Point
    /// this somewhere the soak owns.
    #[arg(long, default_value = "soak-failures")]
    failure_dir: PathBuf,
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

    if args.leader as usize >= args.replicas {
        eprintln!(
            "--leader {} is not a replica of a {}-node cluster (ids are 0..{})",
            args.leader,
            args.replicas,
            args.replicas - 1
        );
        return ExitCode::FAILURE;
    }

    let mut failures: Vec<u64> = Vec::new();
    let mut preserved: Vec<PathBuf> = Vec::new();
    for seed in args.first_seed..args.first_seed + args.seeds {
        println!("=== seed {seed} ===");

        // Created here rather than inside `run_seed` so it outlives a
        // harness error too: a seed whose cluster failed to boot is worth
        // a post-mortem as much as one that diverged.
        let data_dir = args.failure_dir.join(format!("seed-{seed}"));
        if let Err(e) = fresh_dir(&data_dir) {
            eprintln!(
                "seed {seed}: could not prepare {}: {e:#}",
                data_dir.display()
            );
            return ExitCode::FAILURE;
        }

        let outcome = run_seed(&args, seed, &data_dir);
        let mut reported: Vec<Claim> = Vec::new();
        let failed = match outcome {
            Ok((report, config)) => {
                print!("{}", report.render());
                // `problems`, not `is_clean`: a seed whose run observed
                // nothing is a failed seed, not a clean one. Reporting a
                // vacuous run as clean is the single worst thing a soak
                // can do, because it is indistinguishable from success.
                let problems = report.problems(&config);
                for problem in &problems {
                    println!("  {problem}");
                }
                if !problems.is_empty() {
                    println!("{}", report.schedule.render());
                    println!("{}", report.observer_report);
                    failures.push(seed);
                }
                // Both sides of every reported divergence, to be checked
                // against the replicas' own applied logs below.
                reported = claims_from(&report.divergences);
                !problems.is_empty()
            }
            Err(e) => {
                // A harness failure is not a Queso failure, and reporting
                // it as one would be the worst possible signal from a soak.
                eprintln!("seed {seed}: harness error, not a verdict: {e:#}");
                true
            }
        };

        match retain_evidence(&data_dir, failed) {
            Ok(Some(path)) => {
                println!("  evidence kept: {}", path.display());
                adjudicate(&path, &reported);
                preserved.push(path);
            }
            Ok(None) => {}
            // Worth shouting about but not worth abandoning the run for:
            // the remaining seeds can still find something.
            Err(e) => eprintln!(
                "seed {seed}: could not preserve {}: {e:#}",
                data_dir.display()
            ),
        }

        if failed && !args.keep_going {
            break;
        }
    }

    if !preserved.is_empty() {
        println!();
        println!("preserved cluster state for post-mortem:");
        for path in &preserved {
            println!("  {}", path.display());
        }
        println!(
            "each holds every replica's durable snapshot, which carries the applied log -- \
             the only thing that can settle whether a reported divergence is real (issue #73)"
        );
        println!("re-read one with: cargo run -p queso-soak --bin queso-postmortem -- <dir>");
    }

    if failures.is_empty() {
        println!("all {} seed(s) clean and non-vacuous", args.seeds);
        ExitCode::SUCCESS
    } else {
        println!("FAILED seeds: {failures:?}");
        ExitCode::FAILURE
    }
}

/// Adjudicate a preserved seed against its own durable applied logs, in
/// the run log, while the run is still going.
///
/// This is the whole point of preserving them, brought forward: the nightly
/// uploads the data dirs as an artifact, but an artifact has to be found,
/// downloaded and reasoned about by somebody who noticed the failure, and
/// it expires. The verdict costs milliseconds and belongs next to the
/// report it settles -- so a job log alone says whether a divergence was a
/// real Agreement violation or the observability path talking to itself.
///
/// Failures here are reported and stepped over. This is commentary on a
/// verdict that has already been reached; it must never become a second way
/// for the soak to fall over.
fn adjudicate(data_dir: &std::path::Path, reported: &[Claim]) {
    let postmortem = match Postmortem::open(data_dir) {
        Ok(postmortem) => postmortem,
        Err(e) => {
            eprintln!(
                "  post-mortem unavailable for {}: {e:#}",
                data_dir.display()
            );
            return;
        }
    };
    print!("{}", postmortem.render(reported));
}

/// An empty directory at `path`, replacing whatever was there.
///
/// A re-run of a seed should not inherit the previous attempt's files --
/// a stale `node-0.err` beside a fresh snapshot is worse than no evidence,
/// because it reads as evidence.
fn fresh_dir(path: &std::path::Path) -> std::io::Result<()> {
    match std::fs::remove_dir_all(path) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e),
        _ => {}
    }
    std::fs::create_dir_all(path)
}

fn run_seed(
    args: &Args,
    seed: u64,
    data_dir: &std::path::Path,
) -> anyhow::Result<(SoakReport, SoakConfig)> {
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
        leader: args.leader,
        checkpoint_every: args.checkpoint_every,
        tick_ms: 5,
        submit_timeout: Duration::from_secs(4),
    };

    let mut cluster = RealCluster::start(cluster_config, data_dir)?;
    cluster.await_ready(Duration::from_secs(45))?;

    let soak = Soak::new(config.clone());
    // Recorded per seed, not just in the invocation: a preserved failure has
    // to say which replica held the fast path, or a later reader cannot tell
    // a leader-correlated divergence from a node-0-correlated one -- the
    // exact question this flag exists to answer.
    println!(
        "cluster: {} replicas, leader n{}",
        args.replicas, args.leader
    );
    println!("{}", soak.schedule().render());
    Ok((soak.run(&mut cluster), config))
}
