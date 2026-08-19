//! Phase 9.2 (issue #56): the 9.1 Chain-of-Blocks observers, unchanged,
//! judging **real `queso-node` processes** over real sockets.
//!
//! This is the point of the whole phase. Everything here runs against
//! separate OS processes exchanging bytes over TCP through a proxy mesh the
//! harness can cut -- so a "partition" closes real connections and forces
//! real reconnects, and a "crash" is a real `SIGKILL` losing a real heap.
//!
//! # Anti-vacuity, which matters more here than anywhere
//!
//! A soak that silently fails to observe anything looks identical to a soak
//! that found no bugs. Every scenario below therefore asserts, alongside
//! its safety verdict:
//!
//! - `Observer::comparisons()` -- how many times two replicas were actually
//!   compared at the same `n`. Zero would mean the "no divergence" verdict
//!   rested on nothing.
//! - acknowledged submissions -- a cluster that accepted no writes proves
//!   nothing about applying them consistently.
//! - `Turbulence::total_accepted()` -- that peer traffic really crossed the
//!   proxies, so the injected faults were in the path rather than bypassed.
//!
//! Tests are plain `#[test]`, not `#[tokio::test]`: `RealCluster` owns a
//! tokio runtime and `block_on`s inside the synchronous `CobTarget`
//! methods, which would panic inside an outer runtime.
//!
//! # Why every scenario here is `#[ignore]`d
//!
//! They run in CI, just not in the `cargo test --all` job -- the workflow
//! gives the real-process suite a job of its own
//! (`cargo test -p queso-soak -- --ignored`). These scenarios spend nearly
//! all their wall clock asleep waiting for real timers, so folding them
//! into the commit gate would roughly double it to exercise a different
//! layer, and a failure here (a real socket, a real `SIGKILL`) is more
//! legible on its own than buried among unit tests.
//!
//! **Correcting what slice 2 wrote here.** That version said `cargo test
//! --all` runs test binaries in parallel, and used that to explain why
//! these scenarios had to be `#[ignore]`d: they were said to contend with
//! `queso-net`'s own real-process tests, one of which
//! (`restart_recovery::minority_reboot_recovers_too`) failed in CI on
//! slice 2's PR. Cargo does not do that -- test binaries run strictly one
//! after another, within a package and across `--all` alike (measured,
//! cargo 1.94). These scenarios therefore never overlapped that test, and
//! `#[ignore]`ing them cannot have been the fix. That failure is still
//! unexplained; a slow runner remains the likeliest cause, and issue #40's
//! bind-then-drop `free_addr` TOCTOU is a real hazard for anything
//! allocating nine ephemeral ports per scenario. The `#[ignore]`s stay on
//! the cost grounds above, which are true, rather than the contention
//! grounds, which were not.

use std::sync::{Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use queso_conformance::observer::Observer;
use queso_conformance::workload::{converge, run, settle, CobWorkload, RunConfig};
use queso_conformance::CobTarget;
use queso_soak::cluster::{ClusterConfig, RealCluster};

const CHECKPOINT_EVERY: u64 = 2;

/// Run one scenario at a time.
///
/// Each scenario spawns three real `queso-node` processes plus a proxy
/// mesh, and Cargo runs a test binary's tests in parallel threads by
/// default. Five scenarios at once is fifteen node processes competing for
/// CPU on a CI runner, which is enough to push a cluster's boot past any
/// reasonable readiness timeout -- observed as exactly that flake before
/// this guard existed. Serializing keeps `cargo test --all` working
/// unchanged without anyone needing to remember `--test-threads=1`.
///
/// Poisoning is deliberately tolerated: one scenario panicking should
/// report *its* failure, not cascade into four misleading ones.
fn exclusive() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn config() -> ClusterConfig {
    ClusterConfig {
        replicas: 3,
        leader: 0,
        checkpoint_every: CHECKPOINT_EVERY,
        tick_ms: 5,
        submit_timeout: Duration::from_secs(3),
    }
}

/// Milliseconds, since this target's clock is real time.
fn run_config(commands: usize) -> RunConfig {
    RunConfig {
        commands,
        advance_between: 20,
        poll_every: 2,
        settle: 1_500,
    }
}

fn assert_no_divergence(observer: &Observer, min_comparisons: u64) {
    assert!(
        observer.divergences().is_empty(),
        "divergence against real processes:\n{}",
        observer.render_report()
    );
    assert!(
        observer.comparisons() >= min_comparisons,
        "only {} cross-replica comparisons (wanted >= {min_comparisons}) -- the \
         'no divergence' verdict would be vacuous:\n{}",
        observer.comparisons(),
        observer.render_report()
    );
}

#[test]
#[ignore = "boots real node processes; run with --ignored (see this file's docs)"]
fn a_healthy_real_cluster_converges_and_is_actually_observed() {
    let _guard = exclusive();
    let data_dir = tempfile::tempdir().expect("tempdir");
    let mut cluster = RealCluster::start(config(), data_dir.path()).expect("boot cluster");
    cluster.await_ready(Duration::from_secs(45)).expect("ready");

    let mut workload = CobWorkload::new(0xc0b_9002);
    let mut observer = Observer::new();
    run(&mut cluster, &mut workload, &mut observer, run_config(16));
    converge(&mut cluster, &mut workload, &mut observer, 2, 800);

    // Measured on this scenario: ~31 comparisons, ~95 samples, frontier ~33.
    // The floor is set well below that but far above zero, so it survives
    // ordinary timing variance while still failing an unobserved run.
    assert_no_divergence(&observer, 20);

    let (ok, failed) = cluster.submission_counts();
    assert!(
        ok >= 14,
        "a healthy cluster should have acknowledged nearly everything; ok={ok} failed={failed}"
    );
    assert!(
        cluster.turbulence().total_accepted() > 0,
        "no peer traffic crossed the turbulence proxies -- the fault-injection \
         path is not actually in the cluster's network path"
    );
    assert!(
        observer.cluster_frontier() >= 14,
        "the cluster barely progressed (frontier {}):\n{}",
        observer.cluster_frontier(),
        observer.render_report()
    );

    // Liveness, judged after convergence traffic reached every replica.
    let stalls = observer.stalls(cluster.now(), 5_000);
    assert!(
        stalls.is_empty(),
        "replicas stalled on a healthy cluster: {stalls:?}\n{}",
        observer.render_report()
    );
}

#[test]
#[ignore = "boots real node processes; run with --ignored (see this file's docs)"]
fn a_real_socket_partition_of_a_minority_never_causes_divergence() {
    let _guard = exclusive();
    let data_dir = tempfile::tempdir().expect("tempdir");
    let mut cluster = RealCluster::start(config(), data_dir.path()).expect("boot cluster");
    cluster.await_ready(Duration::from_secs(45)).expect("ready");

    let mut workload = CobWorkload::new(0xbadc0ffee);
    let mut observer = Observer::new();

    // Warm up so every replica has applied something and is comparable.
    run(&mut cluster, &mut workload, &mut observer, run_config(12));

    // Cut replica 2 off at the socket layer: its live peer connections are
    // torn down and new dials refused, exactly as a real network partition
    // does. The surviving 2 of 3 are still a majority and must keep going.
    cluster.turbulence().isolate(2);
    run(&mut cluster, &mut workload, &mut observer, run_config(12));

    // The partition must actually have bitten. Measured: the isolated
    // replica sits at n=12 while the surviving majority reaches n=19/20.
    // Without this the scenario would still "pass" if `isolate` were a
    // no-op -- a green test for a nemesis that never fired.
    let during = observer.latest_states();
    let isolated = during
        .get(&cluster.replicas()[2])
        .copied()
        .expect("the isolated replica was observed before the cut");
    let majority_frontier = observer.cluster_frontier();
    assert!(
        isolated.n + 2 <= majority_frontier,
        "the isolated replica kept up with the majority (isolated n={}, frontier {}), \
         so the socket-level partition did not actually take effect:\n{}",
        isolated.n,
        majority_frontier,
        observer.render_report()
    );

    let frontier_during = observer.cluster_frontier();

    cluster.turbulence().heal_all();
    converge(&mut cluster, &mut workload, &mut observer, 5, 1_500);

    // Measured: ~37 comparisons by this point.
    assert_no_divergence(&observer, 20);

    assert!(
        observer.cluster_frontier() > frontier_during,
        "the cluster made no progress after healing (frontier {} -> {}):\n{}",
        frontier_during,
        observer.cluster_frontier(),
        observer.render_report()
    );

    let (ok, _failed) = cluster.submission_counts();
    assert!(
        ok >= 12,
        "the surviving majority should have kept accepting writes; ok={ok}"
    );
}

#[test]
#[ignore = "boots real node processes; run with --ignored (see this file's docs)"]
fn a_killed_and_restarted_replica_rejoins_without_diverging() {
    let _guard = exclusive();
    let data_dir = tempfile::tempdir().expect("tempdir");
    let mut cluster = RealCluster::start(config(), data_dir.path()).expect("boot cluster");
    cluster.await_ready(Duration::from_secs(45)).expect("ready");

    let mut workload = CobWorkload::new(0xdeadbeef);
    let mut observer = Observer::new();
    run(&mut cluster, &mut workload, &mut observer, run_config(12));

    // A real SIGKILL: a fresh process on restart, nothing surviving but
    // what reached disk.
    cluster.kill(1);
    run(&mut cluster, &mut workload, &mut observer, run_config(12));

    cluster.spawn(1);
    cluster.await_ready(Duration::from_secs(45)).expect("ready");
    converge(&mut cluster, &mut workload, &mut observer, 5, 1_500);

    assert_no_divergence(&observer, 15);

    // The restarted replica must be caught up, not frozen behind -- and
    // that is only meaningful because `converge` gave it traffic of its
    // own (a Queso replica catches up by participating).
    let stalls = observer.stalls(cluster.now(), 5_000);
    assert!(
        stalls.is_empty(),
        "a restarted replica is still stalled after convergence: {stalls:?}\n{}",
        observer.render_report()
    );
}

#[test]
#[ignore = "boots real node processes; run with --ignored (see this file's docs)"]
fn latency_turbulence_does_not_produce_divergence() {
    let _guard = exclusive();
    let data_dir = tempfile::tempdir().expect("tempdir");
    let mut cluster = RealCluster::start(config(), data_dir.path()).expect("boot cluster");
    cluster.await_ready(Duration::from_secs(45)).expect("ready");

    let mut workload = CobWorkload::new(0x1a7e);
    let mut observer = Observer::new();

    cluster.turbulence().set_latency_ms(25);
    run(&mut cluster, &mut workload, &mut observer, run_config(16));
    cluster.turbulence().set_latency_ms(0);
    converge(&mut cluster, &mut workload, &mut observer, 4, 1_500);

    assert_no_divergence(&observer, 15);
    let (ok, _) = cluster.submission_counts();
    assert!(ok >= 8, "slow links should still commit writes; ok={ok}");
}

#[test]
#[ignore = "boots real node processes; run with --ignored (see this file's docs)"]
fn the_observer_sees_nothing_from_a_replica_nobody_can_reach() {
    // A soak must not mistake "unreachable" for "applied nothing" -- the
    // former is silence, the latter is a claim. `poll_samples` skips a
    // replica it cannot reach, so the observer keeps that replica's last
    // known state rather than inventing a regression to genesis.
    let _guard = exclusive();
    let data_dir = tempfile::tempdir().expect("tempdir");
    let mut cluster = RealCluster::start(config(), data_dir.path()).expect("boot cluster");
    cluster.await_ready(Duration::from_secs(45)).expect("ready");

    let mut workload = CobWorkload::new(0x5ee9);
    let mut observer = Observer::new();
    run(&mut cluster, &mut workload, &mut observer, run_config(12));

    let before = observer
        .latest_states()
        .get(&cluster.replicas()[2])
        .copied()
        .expect("replica 2 was observed while healthy");
    assert!(before.n > 0, "replica 2 should have applied something");

    cluster.kill(2);
    settle(&mut cluster, &mut observer, 500);

    let after = observer
        .latest_states()
        .get(&cluster.replicas()[2])
        .copied()
        .expect("replica 2 is still known to the observer");
    assert_eq!(
        before, after,
        "a killed replica's last known chain state must be retained, not \
         overwritten or reset"
    );
    assert!(
        observer.divergences().is_empty(),
        "an unreachable replica is not divergence:\n{}",
        observer.render_report()
    );
}
