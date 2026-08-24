//! The sustained soak driver: run a seeded fault schedule against a real
//! cluster while checking safety continuously and liveness after it heals.
//!
//! # What the loop actually does
//!
//! Every step, in order:
//!
//! 1. Reconcile the injected faults with what [`Schedule::active_at`] says
//!    should be in force *now* -- applying newly-started faults and
//!    retiring expired ones.
//! 2. Offer load, without blocking on it (see
//!    [`RealCluster::submit_detached`] for why that matters here).
//! 3. Let real time pass, then poll every reachable replica's `/chain`.
//! 4. Check for divergence, and stop the run the moment there is any.
//!
//! Then the turbulence heals, every crashed replica comes back, the
//! workload runs a converge phase so that *every* replica has been given
//! work, and only then is liveness judged.
//!
//! # Why the verdict is split that way
//!
//! Safety is unconditional: no amount of turbulence licenses two replicas
//! to report different hashes at the same `n`, so that check runs at every
//! step, under fault, and fails the run immediately.
//!
//! Liveness is not. A partitioned replica is *supposed* to fall behind --
//! P5 permits arbitrary lag and forbids only divergence -- so "is anyone
//! stuck" is only a meaningful question once the faults are gone and every
//! replica has had traffic. That second condition is not a formality:
//! Queso has no background replication push, so a replica catches up only
//! by participating, and an idle healthy replica is indistinguishable from
//! a wedged one. [`queso_conformance::workload::converge`] exists to
//! remove that ambiguity, and this driver calls it before judging.
//!
//! # Anti-vacuity
//!
//! A soak that quietly observed nothing looks exactly like a soak that
//! found nothing. [`SoakReport::assert_meaningful`] refuses a run that did
//! not compare replicas at a shared `n`, did not get submissions
//! acknowledged, did not push traffic through the proxies, or did not
//! actually inject its schedule.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use queso_conformance::observer::{Divergence, Observer, Stall};
use queso_conformance::source::CobTarget;
use queso_conformance::workload::{self, CobWorkload};

use crate::cluster::RealCluster;
use crate::schedule::{Fault, Schedule, ScheduleConfig};

/// How to run a soak.
#[derive(Debug, Clone)]
pub struct SoakConfig {
    /// Seeds the fault schedule. Replayable; see [`crate::schedule`] for
    /// exactly what that does and does not promise.
    pub fault_seed: u64,
    /// Seeds the command stream.
    pub workload_seed: u64,
    /// Shape of the turbulence.
    pub schedule: ScheduleConfig,
    /// Real milliseconds each driver step lets pass. Also the polling
    /// interval, so it bounds how precisely fault windows are honoured:
    /// a fault is applied at the first step at or after its start.
    pub step_ms: u64,
    /// Submissions offered per step.
    pub submits_per_step: usize,
    /// Converge rounds after healing, before liveness is judged. Each
    /// round gives every replica one command.
    pub converge_rounds: usize,
    /// Real milliseconds between converge rounds.
    pub converge_advance_ms: u64,
    /// A replica that has not advanced for this many milliseconds *while
    /// behind the cluster frontier*, after healing and converging, is
    /// reported stuck.
    pub liveness_budget_ms: u64,
    /// Floor on cross-replica comparisons; below this the safety verdict is
    /// vacuous and the run is a failure regardless of what it found.
    pub min_comparisons: u64,
    /// Floor on the cluster's furthest chain height -- the direct evidence
    /// that writes were accepted and applied, and the primary
    /// "did anything happen" check.
    ///
    /// This, rather than the acknowledgement count, because a submission
    /// the client abandons on timeout is still *applied*: measured on one
    /// schedule, the frontier lands within 7% (598 / 557 / 560) across a
    /// fast machine, the same machine pinned to two cores, and a CI runner,
    /// while acknowledgements over the same runs vary four-fold
    /// (511 / 423 / 136). The frontier measures what the cluster did; the
    /// acknowledgement count measures how fast the client heard about it.
    pub min_frontier: u64,
    /// Floor on acknowledged submissions.
    ///
    /// Deliberately low. It proves the client path works end to end at all
    /// -- a run where every single submission timed out would be worth
    /// failing -- but it cannot be tightened without encoding one machine's
    /// round-trip latency into a correctness assertion, which is how a soak
    /// starts failing for reasons that have nothing to do with Queso.
    pub min_acked: u64,
}

impl Default for SoakConfig {
    fn default() -> Self {
        Self {
            fault_seed: 1,
            workload_seed: 1,
            schedule: ScheduleConfig::default(),
            step_ms: 100,
            submits_per_step: 3,
            converge_rounds: 12,
            converge_advance_ms: 150,
            liveness_budget_ms: 8_000,
            min_comparisons: 20,
            min_frontier: 100,
            min_acked: 20,
        }
    }
}

/// Fault injections the driver performed, by kind.
///
/// Healing is not counted: the point of the number is to show that
/// turbulence reached the cluster, so only the breaking half is tallied.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Injections {
    /// Directed links severed. One `Isolate` severs `2(n-1)` of them, so
    /// this runs well ahead of the fault count.
    pub cuts: usize,
    /// Node processes killed.
    pub kills: usize,
    /// Times a non-zero link latency was applied.
    pub latency_changes: usize,
}

impl Injections {
    /// Whether each kind happened at least as often as the schedule asked.
    ///
    /// `scheduled` counts *windows*; `self` counts what reconciling those
    /// windows did to the cluster, and one isolation severs `2(n-1)` links,
    /// so the cut comparison has slack built in rather than being
    /// one-for-one. That slack matters: a `CutLink` whose link an
    /// overlapping `Isolate` had already severed injects nothing new, and
    /// an exact comparison would call that a broken injection path.
    fn covers(&self, scheduled: &Injections) -> bool {
        self.cuts >= scheduled.cuts
            && self.kills >= scheduled.kills
            && self.latency_changes >= scheduled.latency_changes
    }

    fn add(&mut self, other: Injections) {
        self.cuts += other.cuts;
        self.kills += other.kills;
        self.latency_changes += other.latency_changes;
    }
}

/// What a finished soak found.
#[derive(Debug, Clone)]
pub struct SoakReport {
    pub schedule: Schedule,
    /// Divergences: any entry is a safety violation.
    pub divergences: Vec<Divergence>,
    /// Replicas still behind and not advancing after the cluster healed.
    pub stalls: Vec<Stall>,
    /// Cross-replica comparisons at a shared `n`.
    pub comparisons: u64,
    pub samples: u64,
    /// The cluster's furthest `n`.
    pub frontier: u64,
    /// Submissions the cluster acknowledged.
    pub acked: u64,
    /// Submissions the client **gave up on** -- overwhelmingly a timeout,
    /// not a refusal. Worth stating plainly because the number reads like
    /// cluster failure and is not: a submission abandoned at the timeout is
    /// still applied, which is why a run can show 136 acknowledgements and
    /// a frontier of 560.
    pub failed: u64,
    /// Submissions never offered because the in-flight cap was reached --
    /// the honest measure of how much load the partitions cost.
    pub deferred: u64,
    /// Detached submissions that had still not settled when the run ended.
    pub undrained: u64,
    /// Set if a replica never answered `/health` again after the cluster
    /// healed. Reported rather than panicked on, so the observer's own
    /// verdict still gets rendered alongside it.
    pub unready_after_heal: Option<String>,
    /// What the driver actually did to the cluster, by kind.
    ///
    /// Counted from what `reconcile` performed rather than from what the
    /// schedule said, on purpose. A count taken from the schedule would
    /// stay healthy if the injection path broke, and "the schedule
    /// contained faults" is not the claim the anti-vacuity check needs --
    /// "faults reached the cluster" is.
    pub injections: Injections,
    /// Fault windows the schedule opened during the run. Below the
    /// schedule's own count only if the run ended early on a divergence.
    pub windows_entered: usize,
    /// Bytes-level connections the proxies accepted, i.e. evidence that
    /// peer traffic really crossed the faultable path.
    pub proxy_accepts: u64,
    /// Per-replica frontier table and divergence context.
    pub observer_report: String,
}

impl SoakReport {
    /// Whether the run found a safety or liveness violation.
    pub fn is_clean(&self) -> bool {
        self.divergences.is_empty() && self.stalls.is_empty() && self.unready_after_heal.is_none()
    }

    /// Everything wrong with this run: violations first, then the
    /// anti-vacuity failures that make a clean verdict worthless.
    ///
    /// Empty means the run both passed *and* proved it checked something.
    /// Those are different claims and are kept separate on purpose: a soak
    /// that silently stopped observing satisfies only the first, and looks
    /// exactly like a soak that found no bugs.
    pub fn problems(&self, config: &SoakConfig) -> Vec<String> {
        let mut problems = Vec::new();
        if !self.divergences.is_empty() {
            problems.push(format!(
                "SAFETY: replicas reported different blocks at the same height: {:?}",
                self.divergences
            ));
        }
        if let Some(reason) = &self.unready_after_heal {
            problems.push(format!(
                "LIVENESS: the cluster never came back after the turbulence healed: {reason}"
            ));
        }
        if !self.stalls.is_empty() {
            problems.push(format!(
                "LIVENESS: replica(s) still behind and not advancing after the \
                 cluster healed and every replica was given work: {:?}",
                self.stalls
            ));
        }
        if self.comparisons < config.min_comparisons {
            problems.push(format!(
                "VACUOUS: only {} cross-replica comparisons (want >= {}). \
                 Nothing was actually compared, so \"no divergence\" means nothing.",
                self.comparisons, config.min_comparisons
            ));
        }
        if self.frontier < config.min_frontier {
            problems.push(format!(
                "VACUOUS: the cluster only reached n={} (want >= {}). It applied \
                 almost nothing, so \"no divergence\" is a statement about an \
                 empty chain.",
                self.frontier, config.min_frontier
            ));
        }
        if self.acked < config.min_acked {
            problems.push(format!(
                "VACUOUS: only {} submissions acknowledged (want >= {}). The client \
                 path never worked end to end, so nothing here exercised a real \
                 submission.",
                self.acked, config.min_acked
            ));
        }
        if self.proxy_accepts == 0 {
            problems.push(
                "VACUOUS: no peer connection ever crossed a proxy, so the faults \
                 were not in the path -- the turbulence was bypassed entirely"
                    .to_string(),
            );
        }
        if self.windows_entered == 0 {
            problems.push(
                "VACUOUS: the schedule never opened a fault window during the run".to_string(),
            );
        }
        // Per kind rather than in total: a total would stay green if, say,
        // only the crash path still worked, and each kind exercises
        // different code in the node (reconnect, restart-from-disk,
        // timeout tuning). Compared against the schedule the run actually
        // traversed, so an early exit on divergence cannot fire this
        // spuriously.
        let scheduled = self.injections_expected();
        if !self.injections.covers(&scheduled) {
            problems.push(format!(
                "VACUOUS: the fault-injection path did not deliver what the \
                 schedule asked for: scheduled {scheduled:?}, injected {:?}",
                self.injections
            ));
        }
        problems
    }

    /// Panic unless [`Self::problems`] is empty, with the schedule and the
    /// observer's report attached.
    pub fn assert_meaningful(&self, config: &SoakConfig) {
        let problems = self.problems(config);
        assert!(
            problems.is_empty(),
            "soak failed:\n  {}\n\n{}\n{}",
            problems.join("\n  "),
            self.schedule.render(),
            self.observer_report
        );
    }

    /// Which kinds of fault the traversed part of the schedule called for.
    ///
    /// Only counts windows that opened at or before the last one the run
    /// reached, so a run cut short by a divergence is judged against the
    /// turbulence it actually saw.
    fn injections_expected(&self) -> Injections {
        injections_expected(&self.schedule, self.windows_entered)
    }

    /// A one-screen summary, for a soak binary's stdout.
    pub fn render(&self) -> String {
        format!(
            "soak: {:?} over {} fault window(s), {} samples, \
             {} comparisons, frontier n={}\n\
             submissions: {} acked / {} failed / {} deferred / {} undrained\n\
             proxy accepts: {}\n\
             verdict: {} divergence(s), {} stall(s) -- {}\n",
            self.injections,
            self.windows_entered,
            self.samples,
            self.comparisons,
            self.frontier,
            self.acked,
            self.failed,
            self.deferred,
            self.undrained,
            self.proxy_accepts,
            self.divergences.len(),
            self.stalls.len(),
            if self.is_clean() { "clean" } else { "FAILED" },
        )
    }
}

/// The sustained soak driver.
pub struct Soak {
    config: SoakConfig,
    schedule: Schedule,
}

/// The faults currently injected into the cluster, as the driver sees them.
///
/// Tracked as desired *state* rather than as a stream of apply/retire
/// events, because faults overlap: with `n = 5` a node can be isolated
/// while a separate one-way cut is in force, and retiring the isolation by
/// calling `rejoin` would silently heal that cut too. Diffing sets makes
/// overlap a non-issue.
#[derive(Debug, Default, PartialEq, Eq)]
struct Injected {
    /// Directed `(from, to)` links that should be severed.
    cuts: BTreeSet<(usize, usize)>,
    /// Replicas whose process should be dead.
    crashed: BTreeSet<usize>,
    /// Added delay on every link, 0 for none.
    latency_ms: u64,
}

impl Injected {
    /// What the schedule says should be in force at `t_ms`.
    fn desired(schedule: &Schedule, t_ms: u64, replicas: usize) -> Self {
        let mut state = Injected::default();
        for scheduled in schedule.active_at(t_ms) {
            match scheduled.fault {
                Fault::Isolate { node } => {
                    for peer in 0..replicas {
                        if peer != node {
                            state.cuts.insert((node, peer));
                            state.cuts.insert((peer, node));
                        }
                    }
                }
                Fault::CutLink { from, to } => {
                    state.cuts.insert((from, to));
                }
                Fault::Crash { node } => {
                    state.crashed.insert(node);
                }
                // Overlapping latency faults take the worse of the two,
                // rather than the last one drawn.
                Fault::Latency { ms } => state.latency_ms = state.latency_ms.max(ms),
            }
        }
        state
    }
}

impl Soak {
    pub fn new(config: SoakConfig) -> Self {
        let schedule = Schedule::generate(config.fault_seed, config.schedule);
        Self { config, schedule }
    }

    pub fn schedule(&self) -> &Schedule {
        &self.schedule
    }

    /// Run the soak against an already-booted, already-ready cluster.
    ///
    /// The cluster is left healed and fully running, so a caller can
    /// inspect it afterwards.
    pub fn run(&self, cluster: &mut RealCluster) -> SoakReport {
        let config = &self.config;
        let mut workload = CobWorkload::new(config.workload_seed);
        let mut observer = Observer::new();
        let replicas = cluster.replicas().len();

        // The schedule's clock is milliseconds from *this* point, not from
        // cluster boot: `RealCluster::now` has been running since `start`,
        // through boot retries and readiness waiting, and charging that to
        // the schedule would silently skip its first faults.
        let started_at = cluster.now();
        let mut injected = Injected::default();
        let mut injections = Injections::default();
        let mut seen_windows: BTreeSet<(u64, u64)> = BTreeSet::new();

        let mut divergences: Vec<Divergence> = Vec::new();

        loop {
            let elapsed = cluster.now().saturating_sub(started_at);
            if elapsed >= config.schedule.duration_ms {
                break;
            }

            for scheduled in self.schedule.active_at(elapsed) {
                seen_windows.insert((scheduled.start_ms, scheduled.end_ms));
            }
            let desired = Injected::desired(&self.schedule, elapsed, replicas);
            injections.add(Self::reconcile(cluster, &injected, &desired));
            injected = desired;

            for _ in 0..config.submits_per_step {
                cluster.submit_detached(workload.next_command());
            }

            cluster.advance(config.step_ms);
            for sample in cluster.poll_samples() {
                observer.observe(sample);
            }

            // Safety is checked under fault and stops the run at once: a
            // divergence is final, and continuing would only pile more
            // turbulence on top of a cluster that has already broken.
            if !observer.divergences().is_empty() {
                divergences = observer.divergences().to_vec();
                break;
            }
        }

        // Heal everything before judging liveness.
        Self::reconcile(cluster, &injected, &Injected::default());
        let undrained = cluster.drain_inflight(Duration::from_secs(5));
        // A replica the schedule crashed near the end was only just
        // respawned. Waiting for it to answer `/health` separates "the
        // process has not finished booting" from "the replica is wedged",
        // which is exactly the distinction the liveness verdict is about.
        let unready_after_heal = cluster
            .await_ready(Duration::from_secs(30))
            .err()
            .map(|e| e.to_string());

        let stalls = if divergences.is_empty() && unready_after_heal.is_none() {
            // Give every replica work, so "behind and not advancing" is
            // evidence of a stall rather than of idleness.
            workload::converge(
                cluster,
                &mut workload,
                &mut observer,
                config.converge_rounds,
                config.converge_advance_ms,
            );
            observer.stalls(cluster.now(), config.liveness_budget_ms)
        } else {
            Vec::new()
        };

        let (acked, failed) = cluster.submission_counts();
        SoakReport {
            schedule: self.schedule.clone(),
            divergences,
            stalls,
            comparisons: observer.comparisons(),
            samples: observer.samples(),
            frontier: observer.cluster_frontier(),
            acked,
            failed,
            deferred: cluster.deferred_submissions(),
            undrained,
            unready_after_heal,
            injections,
            windows_entered: seen_windows.len(),
            proxy_accepts: cluster.turbulence().total_accepted(),
            observer_report: observer.render_report(),
        }
    }

    /// Move the cluster from one injected-fault state to another.
    ///
    /// Returns the *new* faults injected -- links severed, processes
    /// killed, non-zero latency applied. Healing does not count: the
    /// number exists to prove turbulence reached the cluster.
    fn reconcile(cluster: &mut RealCluster, from: &Injected, to: &Injected) -> Injections {
        let mut injected = Injections::default();
        let turbulence = cluster.turbulence();
        for &(a, b) in to.cuts.difference(&from.cuts) {
            turbulence.link(a, b).cut();
            injected.cuts += 1;
        }
        for &(a, b) in from.cuts.difference(&to.cuts) {
            turbulence.link(a, b).heal();
        }
        if from.latency_ms != to.latency_ms {
            turbulence.set_latency_ms(to.latency_ms);
            if to.latency_ms > 0 {
                injected.latency_changes += 1;
            }
        }

        let restart: Vec<usize> = from.crashed.difference(&to.crashed).copied().collect();
        let kill: Vec<usize> = to.crashed.difference(&from.crashed).copied().collect();
        for node in kill {
            if cluster.is_running(node) {
                cluster.kill(node);
                injected.kills += 1;
            }
        }
        for node in restart {
            if !cluster.is_running(node) {
                cluster.spawn(node);
            }
        }
        injected
    }
}

/// What the first `windows_entered` windows of `schedule` actually ask the
/// fault injector to *do*, by kind -- the expectation
/// [`Injections::covers`] is checked against.
///
/// # Why crash windows are merged (found by the first nightly soak)
///
/// Cuts and latency changes are counted per window, but crashes are not,
/// because a crash is a *state transition* and the other two are not.
/// [`Injected::reconcile`] kills a node on the `false -> true` edge of the
/// desired crashed-set (`to.crashed.difference(&from.crashed)`), so two
/// overlapping `Crash` windows for the same node produce exactly one
/// `kill()` -- correctly, since a process cannot be killed twice while it
/// is still dead. Counting one expected kill per window therefore accused
/// a perfectly healthy run of being vacuous.
///
/// That is not hypothetical. The nightly soak's first run reported seed 14
/// as `VACUOUS: scheduled kills: 14, injected kills: 13`, on a schedule
/// containing `Crash { node: 0 }` over `168880..171117ms` and again over
/// `170502..172247ms` -- overlapping, so one kill. Every seed in that run
/// had verdict `0 divergence(s), 0 stall(s)`. It took a 180s schedule with
/// 63 windows to make an overlap likely; the 20s bounded soak in CI has
/// seven or eight windows and had never produced one.
///
/// Merging does not weaken the check. A schedule whose crash windows do
/// *not* overlap still expects one kill each, and a genuinely missed kill
/// still leaves `injected` short of `expected`. What changes is only that
/// the expectation is now something the injector can physically deliver.
///
/// Assumes windows are in non-decreasing `start_ms` order, which
/// [`Schedule::generate`] guarantees.
fn injections_expected(schedule: &Schedule, windows_entered: usize) -> Injections {
    let mut expected = Injections::default();
    // Per node, the end of the crash window group currently open for it.
    let mut open_crash: BTreeMap<usize, u64> = BTreeMap::new();
    for scheduled in schedule.faults().iter().take(windows_entered) {
        match scheduled.fault {
            Fault::Isolate { .. } | Fault::CutLink { .. } => expected.cuts += 1,
            Fault::Latency { .. } => expected.latency_changes += 1,
            Fault::Crash { node } => match open_crash.get_mut(&node) {
                // Still down from an earlier window: no new kill edge.
                Some(open_until) if *open_until > scheduled.start_ms => {
                    *open_until = (*open_until).max(scheduled.end_ms);
                }
                // Back up (or never down) by the time this window opens.
                _ => {
                    expected.kills += 1;
                    open_crash.insert(node, scheduled.end_ms);
                }
            },
        }
    }
    expected
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schedule::ScheduledFault;

    fn schedule_of(faults: Vec<ScheduledFault>) -> Schedule {
        Schedule::with_faults(0, ScheduleConfig::default(), faults)
    }

    fn crash(node: usize, start_ms: u64, end_ms: u64) -> ScheduledFault {
        ScheduledFault {
            start_ms,
            end_ms,
            fault: Fault::Crash { node },
        }
    }

    /// The bug the nightly soak's first run found, reproduced from the
    /// schedule that found it: seed 14 scheduled `Crash { node: 0 }` over
    /// `168880..171117ms` and again over `170502..172247ms`. Those overlap,
    /// so `reconcile` sees one `false -> true` edge on node 0 and performs
    /// one `kill()` -- correctly. Counting one expected kill per *window*
    /// made a run whose verdict was `0 divergence(s), 0 stall(s)` fail as
    /// VACUOUS.
    ///
    /// Falsifier: count `Fault::Crash` per window (the pre-fix behavior)
    /// and this expects 2.
    #[test]
    fn overlapping_crash_windows_for_one_node_expect_a_single_kill() {
        let schedule = schedule_of(vec![crash(0, 168_880, 171_117), crash(0, 170_502, 172_247)]);
        assert_eq!(injections_expected(&schedule, 2).kills, 1);
    }

    /// The other side of the same edge: windows that do *not* overlap each
    /// imply their own kill, because the node is restarted in between. A
    /// fix that merged unconditionally -- say, one kill per node -- would
    /// pass the test above and fail this one.
    ///
    /// The gap here is the real one from seed 14: the second window ends at
    /// `172247ms` and the third opens at `173074ms`.
    #[test]
    fn separated_crash_windows_for_one_node_expect_a_kill_each() {
        let schedule = schedule_of(vec![
            crash(0, 168_880, 171_117),
            crash(0, 170_502, 172_247),
            crash(0, 173_074, 175_984),
        ]);
        assert_eq!(
            injections_expected(&schedule, 3).kills,
            2,
            "the first two windows overlap into one kill; the third is separate"
        );
    }

    /// Merging is per node. Two nodes crashed over the same interval are
    /// two kills, not one -- otherwise a five-replica schedule faulting
    /// `f = 2` nodes at once would under-expect by half.
    #[test]
    fn overlapping_crash_windows_for_different_nodes_expect_a_kill_each() {
        let schedule = schedule_of(vec![crash(0, 1_000, 3_000), crash(1, 2_000, 4_000)]);
        assert_eq!(injections_expected(&schedule, 2).kills, 2);
    }

    /// The check must still catch what it exists to catch. A schedule with
    /// separated crash windows expects a kill for each, so an injector that
    /// silently performed fewer is still reported -- merging narrowed the
    /// expectation to what is physically deliverable, it did not blunt it.
    #[test]
    fn a_genuinely_missed_kill_is_still_not_covered() {
        let schedule = schedule_of(vec![crash(0, 1_000, 2_000), crash(0, 5_000, 6_000)]);
        let expected = injections_expected(&schedule, 2);
        assert_eq!(expected.kills, 2, "test premise: two separate windows");

        let injected = Injections {
            cuts: 0,
            kills: 1,
            latency_changes: 0,
        };
        assert!(
            !injected.covers(&expected),
            "one kill delivered against two expected must still read as vacuous"
        );
    }

    /// Only the traversed prefix counts, and the merge respects it: a run
    /// cut short before the second window opened expects one kill either
    /// way, but must not carry the un-traversed window's end into the
    /// merge state.
    #[test]
    fn untraversed_windows_are_not_expected() {
        let schedule = schedule_of(vec![
            crash(0, 1_000, 2_000),
            crash(0, 5_000, 6_000),
            crash(1, 5_500, 6_500),
        ]);
        assert_eq!(injections_expected(&schedule, 1).kills, 1);
        assert_eq!(injections_expected(&schedule, 3).kills, 3);
    }

    #[test]
    fn isolating_a_node_cuts_it_in_both_directions() {
        let schedule = schedule_of(vec![ScheduledFault {
            start_ms: 0,
            end_ms: 100,
            fault: Fault::Isolate { node: 1 },
        }]);
        let state = Injected::desired(&schedule, 50, 3);
        assert_eq!(
            state.cuts,
            BTreeSet::from([(1, 0), (0, 1), (1, 2), (2, 1)]),
            "an isolated node must stop both sending and receiving"
        );
    }

    #[test]
    fn a_one_way_cut_stays_one_way() {
        let schedule = schedule_of(vec![ScheduledFault {
            start_ms: 0,
            end_ms: 100,
            fault: Fault::CutLink { from: 0, to: 2 },
        }]);
        let state = Injected::desired(&schedule, 0, 3);
        assert_eq!(state.cuts, BTreeSet::from([(0, 2)]));
    }

    #[test]
    fn retiring_an_isolation_does_not_heal_an_overlapping_cut() {
        // The reason `Injected` is a diffed state rather than a stream of
        // events: `Turbulence::rejoin` heals *every* link touching a node,
        // so retiring an isolation the naive way would silently repair a
        // one-way cut that is still supposed to be in force.
        let schedule = schedule_of(vec![
            ScheduledFault {
                start_ms: 0,
                end_ms: 100,
                fault: Fault::Isolate { node: 1 },
            },
            ScheduledFault {
                start_ms: 0,
                end_ms: 300,
                fault: Fault::CutLink { from: 1, to: 3 },
            },
        ]);
        let during = Injected::desired(&schedule, 50, 5);
        let after = Injected::desired(&schedule, 150, 5);
        assert!(during.cuts.contains(&(1, 3)));
        assert_eq!(
            after.cuts,
            BTreeSet::from([(1, 3)]),
            "the still-scheduled one-way cut must survive the isolation \
             being retired"
        );
    }

    #[test]
    fn overlapping_latency_faults_take_the_worse_one() {
        let schedule = schedule_of(vec![
            ScheduledFault {
                start_ms: 0,
                end_ms: 100,
                fault: Fault::Latency { ms: 20 },
            },
            ScheduledFault {
                start_ms: 0,
                end_ms: 100,
                fault: Fault::Latency { ms: 70 },
            },
        ]);
        assert_eq!(Injected::desired(&schedule, 10, 3).latency_ms, 70);
        assert_eq!(
            Injected::desired(&schedule, 200, 3).latency_ms,
            0,
            "latency must be removed once its window ends"
        );
    }

    #[test]
    fn nothing_is_injected_outside_a_fault_window() {
        let schedule = schedule_of(vec![ScheduledFault {
            start_ms: 100,
            end_ms: 200,
            fault: Fault::Crash { node: 2 },
        }]);
        assert_eq!(Injected::desired(&schedule, 99, 3), Injected::default());
        assert_eq!(
            Injected::desired(&schedule, 150, 3).crashed,
            BTreeSet::from([2])
        );
        assert_eq!(Injected::desired(&schedule, 200, 3), Injected::default());
    }
}
