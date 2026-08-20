// Real-I/O boundary code: real sockets, real wall-clock time and a real
// network are exactly this crate's job, so the workspace determinism lints
// (configured for the whole workspace in `clippy.toml`, and escalated by
// `-D warnings`) are neither achievable nor meaningful here. Allowed at the
// crate root, the same way `queso-net`, `queso-compare` and `queso-soak` do
// it and for the same reason -- see this crate's `Cargo.toml` header.
#![allow(clippy::disallowed_methods)]

//! `queso-antithesis` — Phase 9.3 (issue #54): Queso's Chain-of-Blocks
//! conformance workload, packaged as an [Antithesis] test template.
//!
//! # What this is, and what it is not
//!
//! Phases 9.1 and 9.2 built the workload, the divergence/liveness
//! observers, and a soak that drives real `queso-node` processes under
//! turbulence this repo generates itself. That soak is randomized but **not
//! autonomous**: a human picks a seed range and reads the output, and a
//! failing run reproduces its fault *schedule* but never its interleaving,
//! because real thread scheduling and real TCP see to that.
//!
//! Antithesis is the thing that closes that last gap — a deterministic
//! hypervisor that owns the scheduler, the clock and the network, explores
//! on its own, and can replay a failure exactly. This crate is the adapter:
//! it hands Antithesis a workload to run and a set of properties to judge,
//! and injects no faults of its own, because under Antithesis the platform
//! is the adversary and a workload fighting it would only obscure what it
//! found.
//!
//! Issue #54 scopes this precisely: *"buildable artifacts here; the run
//! needs the owner's account."* So what is verifiable in this repo is that
//! the workload drives a real cluster, that the properties are expressed,
//! and that the assertions actually fire — see this crate's README for what
//! was tested locally and what necessarily was not.
//!
//! # The commands
//!
//! Antithesis's Test Composer discovers executables under
//! `/opt/antithesis/test/v1/<template>/` and treats their filename prefix
//! as a schedule. This one binary backs all of them, one subcommand each:
//!
//! - [`Command::WaitReady`] → `first_…`: runs before any driver, waits for
//!   the cluster to form, and signals `setup_complete`. Until that signal
//!   Antithesis holds off on faults, so without it the platform would be
//!   partitioning a cluster that had not finished booting and every
//!   liveness result would be noise.
//! - [`Command::Traffic`] → `parallel_driver_…`: offers Chain-of-Blocks
//!   load and checks **safety** continuously, under whatever faults are in
//!   force.
//! - [`Command::Check`] → `eventually_…`: runs in the quiescent branch
//!   Antithesis creates with all faults stopped, and checks **liveness**.
//!
//! That split is not an artifact of the tool. It is the same discipline
//! `queso-soak` arrived at independently: divergence is forbidden always
//! and unconditionally, while "is anyone stuck" is only a meaningful
//! question once the faults are gone — a partitioned replica is *supposed*
//! to fall behind (P5 permits arbitrary lag and forbids only divergence).
//! Antithesis's `eventually_` semantics happen to be exactly the right
//! place to ask it.
//!
//! [Antithesis]: https://antithesis.com/docs/

mod cluster;

use std::process::ExitCode;
use std::time::Duration;

use antithesis_sdk::prelude::*;
use clap::{Parser, Subcommand};
use queso_conformance::observer::Observer;
use queso_conformance::source::CobTarget;
use queso_conformance::workload::CobWorkload;
use serde_json::json;

use cluster::{RemoteCluster, Replica};

#[derive(Parser, Debug)]
#[command(
    name = "queso-antithesis",
    about = "Chain-of-Blocks conformance workload for Antithesis"
)]
struct Args {
    /// Replica hostname (repeat once per replica, in node-id order).
    /// Under Docker Compose these are service names resolved by container
    /// DNS, not literals.
    #[arg(long = "node", required = true)]
    nodes: Vec<String>,

    /// Client port every replica listens on.
    #[arg(long, default_value_t = 7000)]
    client_port: u16,

    /// Status port every replica serves `/health` and `/chain` on.
    #[arg(long, default_value_t = 7100)]
    status_port: u16,

    /// How long a submission may take before it is recorded as failed.
    #[arg(long, default_value_t = 4)]
    submit_timeout_secs: u64,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Wait for the cluster to form, then signal `setup_complete`.
    WaitReady {
        #[arg(long, default_value_t = 120)]
        timeout_secs: u64,
    },
    /// Offer Chain-of-Blocks load and check safety continuously.
    Traffic {
        #[arg(long, default_value_t = 20)]
        duration_secs: u64,
        /// Milliseconds between rounds; also the polling interval.
        #[arg(long, default_value_t = 100)]
        step_ms: u64,
        /// Submissions offered per round.
        #[arg(long, default_value_t = 2)]
        submits_per_step: usize,
        #[arg(long, default_value_t = 1)]
        seed: u64,
    },
    /// With faults stopped, check that the cluster converges and no replica
    /// is stuck.
    Check {
        /// How long to keep driving before giving up on convergence.
        #[arg(long, default_value_t = 60)]
        timeout_secs: u64,
        #[arg(long, default_value_t = 200)]
        step_ms: u64,
    },
}

fn main() -> ExitCode {
    // Required: without it the SDK does not register the assertion catalog,
    // and an assertion that never executes is never even reported as
    // unreached.
    antithesis_init();

    let args = Args::parse();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            // A harness failure is not a Queso failure. Reporting it as one
            // would be the worst possible signal from a conformance run, so
            // it is said plainly and left to the exit code.
            eprintln!("queso-antithesis: harness error, not a property violation: {err:#}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: &Args) -> anyhow::Result<()> {
    let replicas: Vec<Replica> = args
        .nodes
        .iter()
        .map(|host| {
            Ok(Replica {
                client_addr: cluster::resolve(host, args.client_port)?,
                status_addr: cluster::resolve(host, args.status_port)?,
            })
        })
        .collect::<anyhow::Result<_>>()?;

    let mut target = RemoteCluster::new(replicas, Duration::from_secs(args.submit_timeout_secs))?;

    match &args.command {
        Command::WaitReady { timeout_secs } => wait_ready(&target, *timeout_secs),
        Command::Traffic {
            duration_secs,
            step_ms,
            submits_per_step,
            seed,
        } => traffic(
            &mut target,
            *duration_secs,
            *step_ms,
            *submits_per_step,
            *seed,
        ),
        Command::Check {
            timeout_secs,
            step_ms,
        } => check(&mut target, *timeout_secs, *step_ms),
    }
}

/// `first_` command: hold off the faults until the cluster is really up.
fn wait_ready(target: &RemoteCluster, timeout_secs: u64) -> anyhow::Result<()> {
    let took = target.await_ready(Duration::from_secs(timeout_secs))?;
    let details = json!({
        "replicas": target.replica_count(),
        "ready_after_ms": took.as_millis() as u64,
    });
    // Everything before this point is boot, not test. Antithesis begins
    // injecting faults when it sees this.
    lifecycle::setup_complete(&details);
    println!("cluster ready after {took:?}");
    Ok(())
}

/// `parallel_driver_` command: offer load, check safety under fault.
fn traffic(
    target: &mut RemoteCluster,
    duration_secs: u64,
    step_ms: u64,
    submits_per_step: usize,
    seed: u64,
) -> anyhow::Result<()> {
    let mut workload = CobWorkload::new(seed);
    let mut observer = Observer::new();
    let deadline = target.now() + duration_secs * 1_000;

    while target.now() < deadline {
        for _ in 0..submits_per_step {
            let command = workload.next_command();
            target.submit(command);
        }
        target.advance(step_ms);
        for sample in target.poll_samples() {
            observer.observe(sample);
        }

        // Safety, checked every round and under whatever faults are in
        // force: no two replicas may report a different block at the same
        // height. This is Chain-of-Blocks' central property and Queso's P1
        // Agreement seen from outside the process.
        //
        // The full observer report is rendered only when the property is
        // *violated*. This assertion runs hundreds of times in a single
        // driver invocation, and its details ride along to Antithesis on
        // every one of them -- building and shipping a whole per-replica
        // table each round would be a lot of noise to pay for a string
        // nobody reads unless something broke.
        let divergences = observer.divergences();
        let ok = divergences.is_empty();
        assert_always!(
            ok,
            "replicas never report different blocks at the same height",
            &json!({
                "divergences": divergences.len(),
                "frontier": observer.cluster_frontier(),
                "report": if ok { String::new() } else { observer.render_report() },
            })
        );
        if !ok {
            // Stop rather than pile more turbulence onto a cluster that has
            // already broken; the assertion above is what Antithesis acts
            // on, and the exit code keeps the local run honest too.
            anyhow::bail!("divergence detected:\n{}", observer.render_report());
        }
    }

    let (acked, failed) = target.submission_counts();

    // Anti-vacuity, expressed the way Antithesis wants it: `sometimes`
    // properties fail if they are *never* satisfied across the whole run.
    // Without these, a workload that silently stopped reaching the cluster
    // would report a clean bill of health indefinitely — the failure mode
    // this project has had to correct twice already.
    assert_sometimes!(
        acked > 0,
        "the cluster acknowledges Chain-of-Blocks writes",
        &json!({ "acked": acked, "failed": failed })
    );
    assert_sometimes!(
        observer.comparisons() > 0,
        "two replicas are observed at the same height, so divergence could have been seen",
        &json!({
            "comparisons": observer.comparisons(),
            "samples": observer.samples(),
            "frontier": observer.cluster_frontier(),
        })
    );
    // Faults are expected to make submissions fail; a run where none ever
    // did means the turbulence never reached the client path, and the
    // safety verdict was earned under easier conditions than advertised.
    assert_sometimes!(
        failed > 0,
        "some submissions fail, so the workload really runs under fault",
        &json!({ "acked": acked, "failed": failed })
    );

    println!(
        "traffic: {acked} acked / {failed} failed, {} comparisons, frontier n={}",
        observer.comparisons(),
        observer.cluster_frontier()
    );
    Ok(())
}

/// `eventually_` command: with the faults stopped, is the cluster alive?
///
/// Two properties, and both are needed — either alone has a hole the other
/// closes:
///
/// - **No replica is left behind.** [`Observer::stalls`] reports a replica
///   that is below the cluster frontier *and* has not advanced within the
///   budget. Note the "and": simply being behind is not a stall, because a
///   workload that keeps submitting leaves the trailing replicas
///   permanently a slot or two back. An earlier version of this check
///   demanded every replica *reach* the frontier, which on a healthy
///   cluster can never happen — the target moves every time a command
///   lands. It failed against a completely healthy local cluster
///   (n0=377, n1=378, n2=379), which is how the mistake surfaced.
/// - **The cluster as a whole moves.** Stalls alone cannot see a totally
///   wedged cluster: if every replica is stuck at the *same* height, none
///   of them is below the frontier, so nothing is reported. Requiring the
///   frontier to advance during this phase is what catches that.
fn check(target: &mut RemoteCluster, timeout_secs: u64, step_ms: u64) -> anyhow::Result<()> {
    /// A replica below the frontier and not advancing for this long, after
    /// faults have stopped and it has been given work, is stuck.
    /// `queso-soak` measured a genuinely dead replica caught at 5s; this is
    /// deliberately looser, because Antithesis's scheduler can stretch
    /// wall-clock time far more than a soak's can.
    const LIVENESS_BUDGET_MS: u64 = 15_000;
    /// Rounds of settled progress required before declaring success, so one
    /// lucky poll cannot end the check.
    const SETTLED_ROUNDS: u32 = 3;

    let mut workload = CobWorkload::new(0xc0b_9003);
    let mut observer = Observer::new();
    let deadline = target.now() + timeout_secs * 1_000;
    let replicas = target.replicas().len();

    for sample in target.poll_samples() {
        observer.observe(sample);
    }
    let start_frontier = observer.cluster_frontier();

    let mut settled = 0;
    let mut healthy = false;
    while target.now() < deadline {
        // Give *every* replica work, not just one. Queso has no background
        // replication push: a replica learns a decision by participating,
        // so an idle healthy replica is indistinguishable from a wedged one
        // until it is asked to do something. That is 9.1's finding, and
        // skipping it here would make the stall check pure noise.
        for _ in 0..replicas {
            let command = workload.next_command();
            target.submit(command);
        }
        target.advance(step_ms);
        for sample in target.poll_samples() {
            observer.observe(sample);
        }

        let progressed = observer.cluster_frontier() > start_frontier;
        let stalls = observer.stalls(target.now(), LIVENESS_BUDGET_MS);
        let all_observed = observer.latest_states().len() == replicas;
        if progressed && stalls.is_empty() && all_observed {
            settled += 1;
            if settled >= SETTLED_ROUNDS {
                healthy = true;
                break;
            }
        } else {
            settled = 0;
        }
    }

    let frontier = observer.cluster_frontier();
    let progressed = frontier > start_frontier;
    let stalls = observer.stalls(target.now(), LIVENESS_BUDGET_MS);
    let states = observer.latest_states();
    let divergences = observer.divergences();
    let details = json!({
        "healthy": healthy,
        "progressed": progressed,
        "start_frontier": start_frontier,
        "frontier": frontier,
        "stalls": stalls.len(),
        "observed_replicas": states.len(),
        "expected_replicas": replicas,
        "report": observer.render_report(),
    });

    assert_always!(
        divergences.is_empty(),
        "replicas never report different blocks at the same height",
        &details
    );
    assert_always!(
        progressed,
        "with faults stopped, the cluster keeps deciding",
        &details
    );
    assert_always!(
        stalls.is_empty(),
        "with faults stopped, no replica is left behind and frozen",
        &details
    );
    // Anti-vacuity: a phase that never saw all the replicas proves nothing
    // about any of them, and would satisfy both properties above by
    // default.
    assert_always!(
        states.len() == replicas,
        "every replica is observed during the quiescent check",
        &details
    );

    println!(
        "check: healthy={healthy} progressed={progressed} ({start_frontier} -> {frontier}), \
         {} stall(s), {}/{replicas} replicas observed",
        stalls.len(),
        states.len()
    );
    if !healthy {
        anyhow::bail!(
            "cluster did not reach a healthy quiescent state within {timeout_secs}s:\n{}",
            observer.render_report()
        );
    }
    Ok(())
}
