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
use crate::schedule::{Fault, Schedule, ScheduleConfig, ScheduledFault};

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
        let scheduled = self.injections_expected(config.step_ms);
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
    fn injections_expected(&self, sample_step_ms: u64) -> Injections {
        injections_expected(&self.schedule, self.windows_entered, sample_step_ms)
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
        Self::desired_of(schedule.faults(), t_ms, replicas)
    }

    /// [`Self::desired`] over an explicit window slice. This is the one
    /// definition of "what the schedule means": the driver evaluates it
    /// over the whole schedule every step, and [`injections_expected`]
    /// evaluates it over the entered prefix's boundary instants -- so the
    /// audit and the injector cannot disagree about the semantics of a
    /// window arrangement, by construction.
    fn desired_of(faults: &[ScheduledFault], t_ms: u64, replicas: usize) -> Self {
        let mut state = Injected::default();
        for scheduled in faults
            .iter()
            .filter(|f| f.start_ms <= t_ms && t_ms < f.end_ms)
        {
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
/// # One semantics, by construction
///
/// [`Injected::reconcile`] injects *state transitions* of the desired-state
/// function, not windows. Three nightly failures in a row were
/// safety-and-liveness-clean runs failed by an expectation that counted
/// windows while the injector delivered transitions, one window
/// arrangement at a time: overlapping same-node crash windows (the first
/// nightly, seed 14), a latency window shadowed by a concurrent higher one
/// (run 13, seed 106, #98), and touching same-node crash windows (run 14,
/// seed 119). Each fix enumerated the observed arrangement; the space of
/// arrangements kept producing new members.
///
/// This function ends the family instead of patching its members. It walks
/// the entered windows' boundary instants -- desired state is
/// piecewise-constant and can only change where a window opens or closes
/// -- through [`Injected::desired_of`], the *same* function the driver
/// evaluates every step, extracts each resource's activation intervals
/// (per-link cut spans, per-node down spans, the applied-latency value
/// pieces of `L(t) = max`), and counts transitions with the same rules
/// `reconcile` applies: a cut per link entering the cut set, a kill per
/// node entering the crashed set, a latency change per move to a new
/// non-zero value. There is no second model of the schedule left to
/// disagree with the first.
///
/// This also makes the **cuts** expectation exact in `reconcile`'s own
/// units (link transitions) for the first time: the old one-per-window
/// count was a floor so loose (43 windows against 122 delivered link cuts
/// in run 14's n=3 leg) that an injector silently dropping half its cuts
/// would still have passed.
///
/// # The injector samples, so the expectation must too
///
/// The driver evaluates the desired state once per `sample_step_ms`
/// (`SoakConfig::step_ms`, 100ms in the nightly), so a same-resource
/// inactive gap no longer than one step -- a link healed for 20ms between
/// two cut windows, say -- falls between samples unless a sample happens
/// to land inside it. Whether the "extra" transition pair is delivered is
/// a coin flip the schedule does not control, so this expectation
/// coalesces same-resource gaps (and skips latency value-pieces) of
/// `<= sample_step_ms`: it demands only transitions the sampled injector
/// can be relied on to deliver, and a lucky sample that delivers more
/// simply exceeds the floor, which [`Injections::covers`] permits.
///
/// Measured across every seed of nightly runs 14 and 15 (32 seed-legs,
/// two nights, both cluster sizes): with this coalescing the expectation
/// matched the injector's delivered counts exactly on 30 of 32 and was
/// exceeded on 2 (seeds whose sub-step gaps a sample happened to catch);
/// without it, three green seed-legs (5ms, 10ms, and 20ms gaps) would
/// have been failed as vacuous. Every gap of 126ms or more in that corpus
/// -- 23 transitions across 12 gap sites, cuts and latency both -- was
/// delivered.
///
/// # The two ways delivery can still fall short, deliberately retained
///
/// - **Respawn timing** (#98): `reconcile` kills a node only if it
///   `is_running`, so a same-node crash gap longer than a step but shorter
///   than a real respawn still skips a kill this expectation counts -- the
///   suspected shape of run 9's `kills: 19/20`. Timing-dependent, not
///   schedule-dependent; not modeled here.
/// - **Sampling jitter**: a gap slightly longer than one step can still be
///   missed when the loop's processing time stretches the sampling cadence
///   past the gap. The field corpus above (23/23 delivered at >= 126ms)
///   bounds how much this matters in practice; a future occurrence will
///   fail a clean run and should move this threshold, with its evidence.
///
/// Assumes windows are in non-decreasing `start_ms` order, which
/// [`Schedule::generate`] guarantees.
fn injections_expected(
    schedule: &Schedule,
    windows_entered: usize,
    sample_step_ms: u64,
) -> Injections {
    let entered = &schedule.faults()[..windows_entered.min(schedule.faults().len())];
    let replicas = schedule.config().replicas;

    let mut boundaries: Vec<u64> = entered
        .iter()
        .flat_map(|f| [f.start_ms, f.end_ms])
        .collect();
    boundaries.sort_unstable();
    boundaries.dedup();

    // Walk the boundary instants once, extracting each resource's
    // activation intervals. Every window has closed by the last boundary
    // (it is the maximum end), so nothing is left dangling open.
    let mut cut_spans: BTreeMap<(usize, usize), Vec<(u64, u64)>> = BTreeMap::new();
    let mut open_cut: BTreeMap<(usize, usize), u64> = BTreeMap::new();
    let mut down_spans: BTreeMap<usize, Vec<(u64, u64)>> = BTreeMap::new();
    let mut open_down: BTreeMap<usize, u64> = BTreeMap::new();
    // `(value, start)` of each piece of the applied latency `L(t)`.
    let mut latency_pieces: Vec<(u64, u64)> = Vec::new();
    let mut prev = Injected::default();
    for &t in &boundaries {
        let now = Injected::desired_of(entered, t, replicas);
        for &link in now.cuts.difference(&prev.cuts) {
            open_cut.insert(link, t);
        }
        for &link in prev.cuts.difference(&now.cuts) {
            let opened = open_cut.remove(&link).expect("closing an open cut span");
            cut_spans.entry(link).or_default().push((opened, t));
        }
        for &node in now.crashed.difference(&prev.crashed) {
            open_down.insert(node, t);
        }
        for &node in prev.crashed.difference(&now.crashed) {
            let opened = open_down.remove(&node).expect("closing an open down span");
            down_spans.entry(node).or_default().push((opened, t));
        }
        if now.latency_ms != prev.latency_ms {
            latency_pieces.push((now.latency_ms, t));
        }
        prev = now;
    }
    debug_assert!(open_cut.is_empty() && open_down.is_empty());

    // One reliably-deliverable transition per coalesced activation span:
    // spans separated by a gap the sampled driver may never observe are
    // one continuous activation as far as it can be relied on to see.
    let coalesced = |spans: &[(u64, u64)]| -> usize {
        let mut groups = 0usize;
        let mut open_until: Option<u64> = None;
        for &(start, end) in spans {
            match open_until {
                Some(until) if start.saturating_sub(until) <= sample_step_ms => {}
                _ => groups += 1,
            }
            open_until = Some(open_until.unwrap_or(0).max(end));
        }
        groups
    };

    let mut expected = Injections {
        cuts: cut_spans.values().map(|spans| coalesced(spans)).sum(),
        kills: down_spans.values().map(|spans| coalesced(spans)).sum(),
        latency_changes: 0,
    };

    // Latency: `reconcile` counts a change whenever the value it *observes*
    // moves to a new non-zero value. Pieces no longer than one step may
    // never be observed, so they are skipped; what remains is the value
    // sequence the driver reliably sees.
    let mut observed = 0u64;
    for (i, &(value, start)) in latency_pieces.iter().enumerate() {
        let piece_end = latency_pieces
            .get(i + 1)
            .map(|&(_, s)| s)
            .unwrap_or_else(|| boundaries.last().copied().unwrap_or(start));
        if piece_end.saturating_sub(start) <= sample_step_ms {
            continue;
        }
        if value != observed {
            if value > 0 {
                expected.latency_changes += 1;
            }
            observed = value;
        }
    }
    expected
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn isolate(node: usize, start_ms: u64, end_ms: u64) -> ScheduledFault {
        ScheduledFault {
            start_ms,
            end_ms,
            fault: Fault::Isolate { node },
        }
    }

    fn cut_link(from: usize, to: usize, start_ms: u64, end_ms: u64) -> ScheduledFault {
        ScheduledFault {
            start_ms,
            end_ms,
            fault: Fault::CutLink { from, to },
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
        assert_eq!(injections_expected(&schedule, 2, 100).kills, 1);
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
            injections_expected(&schedule, 3, 100).kills,
            2,
            "the first two windows overlap into one kill; the third is separate"
        );
    }

    /// Nightly run 14's boundary case, minimally: two windows that *touch*
    /// (the second starts at the exact ms the first ends) are one
    /// continuous down period, because windows are half-open -- there is no
    /// instant at which the node is meant to be up, so `reconcile` never
    /// respawns it and a second kill cannot be delivered.
    ///
    /// Falsifier: merging only strictly-overlapping windows (the pre-fix
    /// behavior) expects 2 here.
    #[test]
    fn touching_crash_windows_for_one_node_expect_a_single_kill() {
        let schedule = schedule_of(vec![crash(1, 41_033, 41_636), crash(1, 41_636, 43_212)]);
        assert_eq!(injections_expected(&schedule, 2, 100).kills, 1);
    }

    /// Issue #98, second occurrence, reproduced from the schedule that
    /// found it. Nightly run 14's n=3 leg, seed 119, failed as `VACUOUS:
    /// ... kills: 18` scheduled vs `17` injected on a run whose verdict was
    /// `0 divergence(s), 0 stall(s)` and whose preserved applied logs agree
    /// pairwise on every shared slot: the schedule contains `Crash { node:
    /// 1 }` over `41033..41636` and `41636..43212` -- touching, so one
    /// deliverable kill -- and the injector delivered all 17 kills that
    /// physically exist.
    ///
    /// Regenerated from seed 119 with the `queso-soak` binary's exact
    /// `ScheduleConfig`, so this pins the real failure, not a synthetic
    /// cousin: 70 faults, 18 crash windows, 17 deliverable kills, and the
    /// cuts/latency expectations byte-identical to what run 14 logged.
    ///
    /// Falsifier: merging only strictly-overlapping crash windows (the
    /// pre-fix behavior) yields 18 here and this fails.
    #[test]
    fn nightly_run_14_seed_119s_touching_crash_windows_expect_17_kills() {
        let config = ScheduleConfig {
            replicas: 3,
            duration_ms: 180_000,
            min_fault_ms: 600,
            max_fault_ms: 3_000,
            min_gap_ms: 500,
            max_gap_ms: 2_500,
        };
        let schedule = Schedule::generate(119, config);
        assert_eq!(
            schedule.faults().len(),
            70,
            "test premise: this is the schedule run 14 actually walked"
        );
        let expected = injections_expected(&schedule, schedule.faults().len(), 100);
        assert_eq!(
            expected.kills, 17,
            "the injector reported 17 kills on this schedule and all 17 are \
             deliverable; expecting 18 is what failed the clean run"
        );
        assert_eq!(
            expected.cuts, 122,
            "in reconcile's own units (link transitions) run 14 delivered \
             exactly 122 cuts; the old per-window floor of 43 could not have \
             noticed an injector dropping half of them"
        );
        assert_eq!(
            expected.latency_changes, 9,
            "latency matched exactly in run 14 (the #99 fix held)"
        );
    }

    /// The audit, calibrated against the field: every seed of nightly runs
    /// 14 and 15 (32 seed-legs -- two nights, both cluster sizes, the two
    /// nights this rewrite was validated against), with each leg's
    /// `windows_entered` and delivered `Injections` transcribed from the
    /// job logs. The schedules regenerate deterministically; the delivered
    /// counts are history.
    ///
    /// Two properties are pinned. Every delivery covers its expectation --
    /// zero false positives over the corpus -- and 30 of the 32 match it
    /// *exactly*, which is what makes the floor meaningful rather than
    /// merely safe. The two non-exact seeds are the sub-step-gap coin
    /// flips a sample happened to catch (67ms gaps on seed 126 n=5, 32ms
    /// gaps on eight links of seed 127 n=5): delivery exceeded the floor,
    /// which `covers` permits. The corpus also contains the three flips
    /// that landed the other way (5ms, 10ms, 20ms gaps on seeds 126 n=3,
    /// 118 n=5, 120 n=5): without gap coalescing those three GREEN
    /// seed-legs would have been failed as vacuous.
    #[test]
    fn the_expectation_matches_two_nights_of_field_deliveries() {
        // (seed, replicas, windows_entered, injected cuts/kills/latency).
        let field: Vec<(u64, usize, usize, usize, usize, usize)> = vec![
            (112, 3, 65, 121, 12, 1),
            (113, 3, 68, 125, 12, 4),
            (114, 3, 66, 98, 18, 4),
            (115, 3, 66, 129, 10, 5),
            (116, 3, 67, 133, 6, 11),
            (117, 3, 57, 125, 6, 5),
            (118, 3, 67, 117, 11, 9),
            (119, 3, 70, 122, 17, 9),
            (112, 5, 67, 284, 12, 4),
            (113, 5, 66, 255, 11, 7),
            (114, 5, 73, 244, 11, 9),
            (115, 5, 73, 230, 12, 9),
            (116, 5, 69, 258, 11, 7),
            (117, 5, 67, 232, 10, 6),
            (118, 5, 66, 280, 10, 8),
            (119, 5, 73, 232, 22, 9),
            (120, 3, 63, 138, 8, 6),
            (121, 3, 57, 95, 11, 4),
            (122, 3, 62, 102, 12, 9),
            (123, 3, 66, 121, 9, 7),
            (124, 3, 68, 140, 15, 2),
            (125, 3, 60, 125, 12, 6),
            (126, 3, 63, 95, 12, 4),
            (127, 3, 62, 116, 13, 5),
            (120, 5, 77, 258, 13, 7),
            (121, 5, 66, 221, 12, 5),
            (122, 5, 71, 203, 13, 15),
            (123, 5, 72, 258, 16, 9),
            (124, 5, 69, 329, 12, 2),
            (125, 5, 62, 249, 14, 7),
            (126, 5, 70, 166, 17, 10),
            (127, 5, 65, 224, 15, 5),
        ];
        let mut exact = 0usize;
        for &(seed, replicas, entered, cuts, kills, latency_changes) in &field {
            let config = ScheduleConfig {
                replicas,
                duration_ms: 180_000,
                min_fault_ms: 600,
                max_fault_ms: 3_000,
                min_gap_ms: 500,
                max_gap_ms: 2_500,
            };
            let schedule = Schedule::generate(seed, config);
            let expected = injections_expected(&schedule, entered, 100);
            let injected = Injections {
                cuts,
                kills,
                latency_changes,
            };
            assert!(
                injected.covers(&expected),
                "seed {seed} n={replicas}: the field delivered {injected:?} but the \
                 expectation demands {expected:?} -- this expectation would have \
                 failed a green night"
            );
            if injected == expected {
                exact += 1;
            }
        }
        assert_eq!(
            exact, 30,
            "30 of 32 field deliveries matched the expectation exactly when this \
             was calibrated; fewer means the expectation drifted loose, more \
             means the two known coin-flip seeds changed"
        );
    }

    /// An `Isolate` cuts every link touching the node, both directions, so
    /// in `reconcile`'s units one isolation of one node in a 3-replica
    /// cluster is four link transitions -- which is why run 14's n=3 leg
    /// delivered 122 cuts against 43 windows, and why counting windows
    /// made the cuts floor nearly powerless.
    #[test]
    fn one_isolation_expects_a_cut_per_severed_link_direction() {
        let schedule = schedule_of(vec![isolate(0, 1_000, 3_000)]);
        assert_eq!(injections_expected(&schedule, 1, 100).cuts, 4);
    }

    /// A `CutLink` whose whole span lies inside an `Isolate` that already
    /// severs the same link asks for nothing the injector has not already
    /// done: the link never leaves the desired cut set, so there is no
    /// transition to deliver. (The driver-side twin of this fact is
    /// `reconcile`'s set-difference; this pins that the audit agrees.)
    #[test]
    fn a_cut_link_shadowed_by_an_isolation_expects_no_extra_cut() {
        let schedule = schedule_of(vec![isolate(0, 1_000, 5_000), cut_link(0, 1, 2_000, 4_000)]);
        assert_eq!(injections_expected(&schedule, 2, 100).cuts, 4);
    }

    /// The sampling rule, minimally, for cuts: the driver observes the
    /// desired state once per step (100ms here), so a link healed for less
    /// than one step between two cut windows may never be seen up --
    /// whether the second cut is delivered is a coin flip. The expectation
    /// demands only what delivery can be relied on for: one cut. The field
    /// corpus above holds both outcomes of that coin: seed 120 n=5's 20ms
    /// gap was missed, seed 127 n=5's 32ms gaps were caught.
    #[test]
    fn a_sub_step_heal_between_two_cut_windows_expects_a_single_cut() {
        let schedule = schedule_of(vec![
            cut_link(0, 1, 1_000, 2_000),
            cut_link(0, 1, 2_050, 3_000),
        ]);
        assert_eq!(injections_expected(&schedule, 2, 100).cuts, 1);
    }

    /// ...and the same gap wider than a step is two deliverable cuts: the
    /// coalescing narrows the floor to reliable deliveries, it does not
    /// blunt it.
    #[test]
    fn a_heal_wider_than_a_step_between_cut_windows_expects_a_cut_each() {
        let schedule = schedule_of(vec![
            cut_link(0, 1, 1_000, 2_000),
            cut_link(0, 1, 2_500, 3_000),
        ]);
        assert_eq!(injections_expected(&schedule, 2, 100).cuts, 2);
    }

    /// The same sampling rule for kills: a same-node crash gap no longer
    /// than one step is one continuous down period as far as the sampled
    /// driver can be relied on to see (and shorter than any real respawn
    /// anyway -- #98's timing half). This subsumes the overlap merge (the
    /// first nightly) and the touching merge (run 14) as the gap-0 cases.
    #[test]
    fn a_sub_step_gap_between_crash_windows_expects_a_single_kill() {
        let schedule = schedule_of(vec![crash(1, 1_000, 2_000), crash(1, 2_080, 3_000)]);
        assert_eq!(injections_expected(&schedule, 2, 100).kills, 1);
    }

    /// A latency value held for no longer than one step may never be
    /// observed at all, so it contributes no reliable change. Two windows
    /// arranged so `L(t)` visits 60, dips to 39 for 80ms, and returns to
    /// 60 reliably deliver only the first change.
    #[test]
    fn a_latency_piece_no_longer_than_a_step_expects_no_change() {
        let schedule = schedule_of(vec![
            latency(60, 1_000, 3_000),
            latency(39, 3_000, 3_080),
            latency(60, 3_080, 5_000),
        ]);
        assert_eq!(injections_expected(&schedule, 3, 100).latency_changes, 1);
    }

    /// Merging is per node. Two nodes crashed over the same interval are
    /// two kills, not one -- otherwise a five-replica schedule faulting
    /// `f = 2` nodes at once would under-expect by half.
    #[test]
    fn overlapping_crash_windows_for_different_nodes_expect_a_kill_each() {
        let schedule = schedule_of(vec![crash(0, 1_000, 3_000), crash(1, 2_000, 4_000)]);
        assert_eq!(injections_expected(&schedule, 2, 100).kills, 2);
    }

    /// The check must still catch what it exists to catch. A schedule with
    /// separated crash windows expects a kill for each, so an injector that
    /// silently performed fewer is still reported -- merging narrowed the
    /// expectation to what is physically deliverable, it did not blunt it.
    #[test]
    fn a_genuinely_missed_kill_is_still_not_covered() {
        let schedule = schedule_of(vec![crash(0, 1_000, 2_000), crash(0, 5_000, 6_000)]);
        let expected = injections_expected(&schedule, 2, 100);
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
        assert_eq!(injections_expected(&schedule, 1, 100).kills, 1);
        assert_eq!(injections_expected(&schedule, 3, 100).kills, 3);
    }

    fn latency(ms: u64, start_ms: u64, end_ms: u64) -> ScheduledFault {
        ScheduledFault {
            start_ms,
            end_ms,
            fault: Fault::Latency { ms },
        }
    }

    /// Issue #98, reproduced from the schedule that found it. Nightly run
    /// 13's n=5 leg, seed 106, failed as `VACUOUS: ... latency_changes: 8`
    /// scheduled vs `7` injected on a safety-clean run: the schedule
    /// contains `[83376..84188] Latency 39` fully inside `[82869..84676]
    /// Latency 60`, and `Injected::desired`'s `max` means the shadowed
    /// window never changes the applied latency. The injector delivered
    /// every change that physically exists; the expectation demanded one
    /// that does not.
    ///
    /// Regenerated from seed 106 with the `queso-soak` binary's exact
    /// `ScheduleConfig`, so this pins the real failure, not a synthetic
    /// cousin: 74 faults, 8 latency windows, 7 deliverable edges.
    ///
    /// Falsifier: counting one expected change per latency window (the
    /// pre-fix behavior) yields 8 here and this fails.
    #[test]
    fn nightly_run_13_seed_106s_shadowed_latency_window_expects_7_changes() {
        let config = ScheduleConfig {
            replicas: 5,
            duration_ms: 180_000,
            min_fault_ms: 600,
            max_fault_ms: 3_000,
            min_gap_ms: 500,
            max_gap_ms: 2_500,
        };
        let schedule = Schedule::generate(106, config);
        assert_eq!(
            schedule.faults().len(),
            74,
            "test premise: this is the schedule run 13 actually walked"
        );
        let windows_entered = schedule.faults().len();
        let expected = injections_expected(&schedule, windows_entered, 100);
        assert_eq!(
            expected.latency_changes, 7,
            "the injector reported 7 latency changes on this schedule and \
             all 7 are deliverable; expecting 8 is what failed the clean run"
        );
        assert_eq!(expected.kills, 14, "kills matched exactly in run 13");
    }

    /// The general shape of #98's failure, minimally: a lower-latency
    /// window fully inside a higher one is shadowed by `desired`'s `max`
    /// and can produce no change at all.
    #[test]
    fn a_latency_window_shadowed_by_a_higher_one_expects_no_extra_change() {
        let schedule = schedule_of(vec![latency(60, 1_000, 5_000), latency(39, 2_000, 4_000)]);
        assert_eq!(injections_expected(&schedule, 2, 100).latency_changes, 1);
    }

    /// The other side of that edge, in both directions overlap can go:
    /// windows that DO change the applied maximum each expect their edge. A
    /// fix that merged latency windows unconditionally -- say, one change
    /// per overlap group -- would pass the test above and fail both of
    /// these.
    #[test]
    fn overlapping_latency_windows_that_change_the_max_expect_a_change_each() {
        // Rising: 0 -> 30 -> 60.
        let rising = schedule_of(vec![latency(30, 1_000, 3_000), latency(60, 2_000, 4_000)]);
        assert_eq!(injections_expected(&rising, 2, 100).latency_changes, 2);

        // Falling out from under: 0 -> 60, then the 60 window ends while
        // the 39 one is still active, so 60 -> 39 is a real change too.
        let outlasting = schedule_of(vec![latency(60, 1_000, 3_000), latency(39, 2_000, 4_000)]);
        assert_eq!(injections_expected(&outlasting, 2, 100).latency_changes, 2);
    }

    /// Disjoint windows keep the old one-per-window expectation -- the fix
    /// narrows the expectation to what is deliverable, it does not blunt
    /// it -- and a genuinely missed change still fails `covers`, which is
    /// what the check exists to catch.
    #[test]
    fn a_genuinely_missed_latency_change_is_still_not_covered() {
        let schedule = schedule_of(vec![latency(30, 1_000, 2_000), latency(60, 5_000, 6_000)]);
        let expected = injections_expected(&schedule, 2, 100);
        assert_eq!(
            expected.latency_changes, 2,
            "test premise: disjoint windows"
        );

        let injected = Injections {
            cuts: 0,
            kills: 0,
            latency_changes: 1,
        };
        assert!(
            !injected.covers(&expected),
            "one change delivered against two deliverable must still read as vacuous"
        );
    }

    /// Adjacent same-value windows are one edge: `L` goes 0 -> 30 and
    /// stays there across the boundary, so `reconcile`'s `from != to`
    /// guard sees nothing at the seam. (`Schedule::generate` draws `ms`
    /// from `range(10, 80)`, so equal neighbors are unlikely but
    /// reachable.)
    #[test]
    fn adjacent_equal_latency_windows_expect_a_single_change() {
        let schedule = schedule_of(vec![latency(30, 1_000, 2_000), latency(30, 2_000, 3_000)]);
        assert_eq!(injections_expected(&schedule, 2, 100).latency_changes, 1);
    }

    /// Only the traversed prefix counts for latency too, mirroring
    /// `untraversed_windows_are_not_expected`.
    #[test]
    fn untraversed_latency_windows_are_not_expected() {
        let schedule = schedule_of(vec![latency(30, 1_000, 2_000), latency(60, 5_000, 6_000)]);
        assert_eq!(injections_expected(&schedule, 1, 100).latency_changes, 1);
        assert_eq!(injections_expected(&schedule, 2, 100).latency_changes, 2);
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
