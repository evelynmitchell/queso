//! Phase 9.2 (issue #56), slice 3: **sustained randomized turbulence**
//! against real `queso-node` processes.
//!
//! Slice 2's scenarios are scripted: cut this link, kill that node, check.
//! They prove the machinery works and they catch gross breakage, but they
//! only ever visit the states someone thought to write down. The premise of
//! Antithesis-style testing -- and the reason issue #54 exists -- is that
//! the interesting bugs live in fault *sequences* nobody would script: a
//! node crashing while a one-way cut is already in force, healing into a
//! fresh partition, restarting mid-catch-up.
//!
//! So the schedule is drawn from a seed instead. See [`queso_soak::schedule`]
//! for the generator and, more importantly, for what a seed does and does
//! not promise: it replays the *turbulence*, never the interleaving, and
//! claiming otherwise would be the most tempting lie in this phase.
//!
//! # What runs where
//!
//! Everything here is `#[ignore]`d, and the workflow's soak job runs the
//! set minus the long one. So:
//!
//! - [`a_bounded_soak_survives_randomized_turbulence`] -- ~27s, three
//!   replicas, one seed. In CI.
//! - [`a_five_replica_soak_tolerates_two_concurrent_faults`] -- ~90s, and
//!   the only size where the schedule may fault two nodes at once. In CI.
//! - [`a_permanently_dead_replica_is_reported_stuck`] -- the positive
//!   control for the liveness budget. In CI.
//! - [`a_long_soak_over_many_seeds`] -- minutes across six seeds. **Not**
//!   in CI; run it deliberately, or use the `queso-soak` binary, which is
//!   the same thing with a seed range and a non-zero exit code.
//!
//! They all make the same claims. The bounded one just makes them about
//! less turbulence, which is a statement about cost, not about confidence.

use std::path::{Path, PathBuf};
use std::time::Duration;

use queso_soak::cluster::{ClusterConfig, RealCluster};
use queso_soak::evidence::retain_evidence;
use queso_soak::postmortem::{claims_from, Postmortem};
use queso_soak::schedule::ScheduleConfig;
use queso_soak::soak::{Soak, SoakConfig};

/// Run one real-process scenario at a time within this binary. Cargo runs
/// a binary's tests in parallel threads, and three node processes each is
/// enough contention to matter. (Test *binaries* run sequentially, so this
/// says nothing about the rest of the workspace -- see this crate's README.)
fn exclusive() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn cluster_config(replicas: usize) -> ClusterConfig {
    ClusterConfig {
        replicas,
        leader: 0,
        // Dense checkpoints: the cross-replica comparison count is the
        // anti-vacuity floor this whole test rests on, and 9.1 measured
        // that sparse sampling collapses it (frontier-only gave 2
        // comparisons where checkpoints gave 20 on the same run).
        checkpoint_every: 2,
        tick_ms: 5,
        // Generous, because the soak submits without blocking on the
        // answer: this bounds how long a doomed submission occupies a
        // runtime task, not how long the driver waits, so a short value
        // buys nothing and costs real load. 1.5s was measurably too short
        // -- on a CI runner it abandoned three quarters of submissions that
        // the cluster went on to apply anyway (136 acknowledged against a
        // frontier of 560).
        submit_timeout: Duration::from_secs(4),
    }
}

fn soak_config(fault_seed: u64, duration_ms: u64, replicas: usize) -> SoakConfig {
    SoakConfig {
        fault_seed,
        workload_seed: 0xc0b_9003 ^ fault_seed,
        schedule: ScheduleConfig {
            replicas,
            duration_ms,
            min_fault_ms: 600,
            max_fault_ms: 2_000,
            min_gap_ms: 500,
            max_gap_ms: 1_500,
        },
        step_ms: 100,
        submits_per_step: 3,
        converge_rounds: 12,
        converge_advance_ms: 200,
        // Tight enough to be falsifiable, which is the whole point:
        // `a_permanently_dead_replica_is_reported_stuck` below shows a
        // genuinely wedged replica *is* caught at this budget. A budget
        // sized for comfort rather than measured against a real stall makes
        // the liveness assertion unfalsifiable, which is exactly the trap
        // 9.1 fell into and had to correct.
        liveness_budget_ms: 5_000,
        // The floors scale with run length, hence the division. Sized from
        // measurement across three environments -- a fast machine, that
        // machine pinned to two cores, and a CI runner -- where comparisons
        // and chain height both landed within 7% of each other (~40 and ~28
        // per second respectively).
        //
        // Acknowledgements are *not* stable across those environments:
        // 511 / 423 / 136 on the same 20s schedule, because a submission the
        // client abandons on timeout is still applied by the cluster. So the
        // frontier carries the "writes really happened" claim, and the
        // acknowledgement floor stays low enough to mean only what it can
        // honestly mean -- that the client path worked at all. Tightening it
        // would encode one machine's round-trip latency as a correctness
        // assertion, and it did: at 8/s this failed CI while the cluster was
        // applying 28 entries a second.
        min_comparisons: 10 * duration_ms / 1_000,
        min_frontier: 10 * duration_ms / 1_000,
        min_acked: 2 * duration_ms / 1_000,
    }
}

/// An empty directory at `path`, replacing whatever was there -- the same
/// contract as the `queso-soak` binary's, for the same reason: a re-run
/// must not inherit a previous attempt's files, because stale evidence
/// beside fresh evidence reads as evidence.
fn fresh_dir(path: &Path) -> std::io::Result<()> {
    match std::fs::remove_dir_all(path) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e),
        _ => {}
    }
    std::fs::create_dir_all(path)
}

fn run_one(fault_seed: u64, duration_ms: u64, replicas: usize) {
    let config = soak_config(fault_seed, duration_ms, replicas);

    // Not a tempdir. A tempdir deletes itself during panic unwind -- which
    // is precisely when the cluster state matters. The one divergence this
    // suite has ever reported in CI (issue #92, on a post-#88 build) is
    // permanently unadjudicable because the unwind destroyed the replicas'
    // durable applied logs before anything could read them. So a failing
    // run keeps its state under `soak-failures/seed-<n>` -- the `queso-soak`
    // binary's convention, which CI uploads -- and a clean run removes it.
    let data_dir = PathBuf::from("soak-failures").join(format!("seed-{fault_seed}"));
    fresh_dir(&data_dir).expect("prepare evidence dir");

    let mut cluster =
        RealCluster::start(cluster_config(replicas), &data_dir).expect("boot cluster");
    cluster.await_ready(Duration::from_secs(45)).expect("ready");

    let soak = Soak::new(config.clone());
    // The schedule is the run's identity: printed unconditionally, so a
    // failure in CI carries the seed that reproduces its turbulence.
    eprintln!("{}", soak.schedule().render());

    let report = soak.run(&mut cluster);
    eprintln!("{}", report.render());

    // The nodes must be down before their durable snapshots are preserved
    // or read; dropping the cluster kills them.
    drop(cluster);

    let failed = !report.problems(&config).is_empty();
    match retain_evidence(&data_dir, failed) {
        Ok(Some(path)) => {
            // Adjudicate in the job log while the run is still legible: the
            // observer's claim says two replicas *reported* different blocks;
            // only their durable applied logs can say whether they *applied*
            // different commands (issue #73). An artifact has to be noticed
            // and downloaded before it expires; the log does not.
            eprintln!("evidence kept: {}", path.display());
            match Postmortem::open(&path) {
                Ok(postmortem) => {
                    eprint!("{}", postmortem.render(&claims_from(&report.divergences)));
                }
                Err(e) => eprintln!("post-mortem unavailable for {}: {e:#}", path.display()),
            }
        }
        Ok(None) => {}
        // Commentary on a verdict already reached -- it must never become a
        // second way for the test to fall over.
        Err(e) => eprintln!("could not preserve {}: {e:#}", data_dir.display()),
    }

    report.assert_meaningful(&config);
}

#[test]
#[ignore = "boots real node processes; run with --ignored (see this crate's README)"]
fn a_bounded_soak_survives_randomized_turbulence() {
    let _guard = exclusive();
    run_one(0xd00d, 20_000, 3);
}

/// The long mode. Many seeds, so the run visits fault sequences a single
/// schedule never would.
///
/// Not in CI at any tier -- the soak job skips it by name. Minutes per seed
/// is the wrong shape for a commit gate, and #56 asks for a documented long
/// mode rather than a slow one.
#[test]
#[ignore = "long soak: several minutes. Run deliberately -- see this crate's README"]
fn a_long_soak_over_many_seeds() {
    let _guard = exclusive();
    for seed in 0..6u64 {
        run_one(seed, 45_000, 3);
    }
}

/// Five replicas, so the schedule may fault two nodes at once and the
/// cluster still owes progress.
///
/// Worth its own test rather than a parameter: `f = 2` is the first size at
/// which two *node* faults may overlap, which is where the driver's
/// `Injected` diffing earns its keep -- a node isolated while a separate
/// one-way cut is in force, where naively retiring the isolation would heal
/// the cut too. Measured on the generator: 57 of 64 seeds reach two
/// concurrent node faults at `n = 5`, against exactly none at `n = 3`.
#[test]
#[ignore = "boots five real node processes; run with --ignored (see this crate's README)"]
fn a_five_replica_soak_tolerates_two_concurrent_faults() {
    let _guard = exclusive();
    run_one(0x5eed, 45_000, 5);
}

/// Positive control for the liveness verdict: a replica that is genuinely
/// gone **is** reported stuck, at the same budget the soaks assert with.
///
/// Without this, `stalls.is_empty()` is a claim with no demonstrated
/// falsifier. A budget in the wrong units, or one sized generously to stop
/// a flake, passes every healthy run and every broken one alike -- 9.1 hit
/// exactly that and had to go back and measure. This test is what makes
/// `liveness_budget_ms: 5_000` in [`soak_config`] mean something: it is
/// short enough that a dead replica is caught inside one converge phase.
#[test]
#[ignore = "boots real node processes; run with --ignored (see this crate's README)"]
fn a_permanently_dead_replica_is_reported_stuck() {
    use queso_conformance::observer::Observer;
    use queso_conformance::workload::{converge, CobWorkload};
    use queso_conformance::CobTarget;

    let _guard = exclusive();
    let data_dir = tempfile::tempdir().expect("tempdir");
    let mut cluster = RealCluster::start(cluster_config(3), data_dir.path()).expect("boot cluster");
    cluster.await_ready(Duration::from_secs(45)).expect("ready");

    let mut workload = CobWorkload::new(0xdead_beef);
    let mut observer = Observer::new();

    // Establish a chain everyone is on, so the dead replica's frontier is
    // known to have been current before it died.
    converge(&mut cluster, &mut workload, &mut observer, 8, 200);
    let before = observer.latest_states();
    assert!(
        before.len() == 3 && before.values().all(|s| s.n > 0),
        "every replica should have been observed advancing first: {before:?}"
    );

    cluster.kill(2);

    // Keep the surviving majority working for longer than the budget, so
    // the dead replica's last progress falls outside it.
    converge(&mut cluster, &mut workload, &mut observer, 32, 200);

    let budget = soak_config(0, 20_000, 3).liveness_budget_ms;
    let stalls = observer.stalls(cluster.now(), budget);
    assert!(
        stalls.iter().any(|s| s.replica.0 == 2),
        "a killed replica was not reported stuck at the {budget}ms budget \
         the soaks assert with -- that budget is unfalsifiable as written. \
         stalls={stalls:?}\n{}",
        observer.render_report()
    );
}
