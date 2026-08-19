//! The seeded fault schedule: what breaks, when, and for how long.
//!
//! # Reproducible schedule, irreproducible run
//!
//! This is the part of a soak that *is* replayable. Given a seed and a
//! config, [`Schedule::generate`] produces exactly the same sequence of
//! faults every time, so a failing run can be re-run against the identical
//! turbulence.
//!
//! What that does **not** buy is a reproducible failure. The cluster's
//! response depends on real thread scheduling, real timers and real TCP, so
//! replaying a schedule re-creates the conditions, not the interleaving.
//! Phase 9.1's in-process harness is where bit-for-bit replay lives; here,
//! a seed narrows the search, and that is all it does. Saying otherwise
//! would be the most tempting lie in this whole phase.
//!
//! # The majority invariant
//!
//! Faults never take more than `f = (n-1)/2` nodes out at once. That is not
//! timidity, it is what makes the verdicts mean anything: with a majority
//! always available, the cluster is *obliged* to keep deciding, so a stall
//! is a real liveness failure rather than the expected consequence of
//! having killed a quorum. A soak that knocked out majorities would have to
//! weaken its liveness check to "eventually, after everything heals", which
//! is a far weaker property and a much worse bug detector.
//!
//! Safety, by contrast, is checked continuously and unconditionally: no
//! amount of turbulence licenses two replicas to disagree at the same `n`.

use std::collections::BTreeSet;

/// A fault the soak can inject.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// Symmetric socket-level partition of one node from every peer.
    Isolate { node: usize },
    /// A one-way cut: `to` stops hearing from `from`, but not vice versa.
    /// Asymmetric failures are the ones that tend to embarrass consensus
    /// implementations, so they are worth generating deliberately.
    CutLink { from: usize, to: usize },
    /// `SIGKILL` the node, restart it when the window ends.
    Crash { node: usize },
    /// Delay every link. Degrades rather than removes, so it does not
    /// consume the fault budget.
    Latency { ms: u64 },
}

impl Fault {
    /// Which nodes this fault removes from the cluster's working set.
    ///
    /// `Latency` removes nobody. `CutLink` counts its *receiving* end:
    /// a node that has stopped hearing from a peer is degraded in the same
    /// direction as an isolated one, and counting it keeps the budget
    /// conservative rather than clever.
    fn nodes_consumed(&self) -> Option<usize> {
        match self {
            Fault::Isolate { node } | Fault::Crash { node } => Some(*node),
            Fault::CutLink { to, .. } => Some(*to),
            Fault::Latency { .. } => None,
        }
    }
}

/// One fault, and the window it is in force for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScheduledFault {
    /// Milliseconds from the start of the soak.
    pub start_ms: u64,
    /// Exclusive end, in the same units.
    pub end_ms: u64,
    pub fault: Fault,
}

impl ScheduledFault {
    fn overlaps(&self, start_ms: u64, end_ms: u64) -> bool {
        self.start_ms < end_ms && start_ms < self.end_ms
    }
}

/// Shape of the turbulence to generate.
#[derive(Debug, Clone, Copy)]
pub struct ScheduleConfig {
    pub replicas: usize,
    /// How long the soak runs.
    pub duration_ms: u64,
    /// A fault lasts uniformly between these.
    pub min_fault_ms: u64,
    pub max_fault_ms: u64,
    /// Quiet time between faults, uniformly between these. Some quiet is
    /// essential: a cluster that is never given a chance to recover cannot
    /// demonstrate that it recovers.
    pub min_gap_ms: u64,
    pub max_gap_ms: u64,
}

impl Default for ScheduleConfig {
    fn default() -> Self {
        Self {
            replicas: 3,
            duration_ms: 60_000,
            min_fault_ms: 800,
            max_fault_ms: 3_000,
            min_gap_ms: 400,
            max_gap_ms: 2_000,
        }
    }
}

/// SplitMix64 -- seeded, dependency-free, and identical to the generator
/// `queso_conformance::workload` uses, so a soak's two seeded halves behave
/// the same way.
#[derive(Debug, Clone)]
struct SplitMix64(u64);

impl SplitMix64 {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Uniform in `[low, high]`.
    fn range(&mut self, low: u64, high: u64) -> u64 {
        if high <= low {
            return low;
        }
        low + self.next_u64() % (high - low + 1)
    }

    fn below(&mut self, bound: usize) -> usize {
        if bound == 0 {
            return 0;
        }
        (self.next_u64() % bound as u64) as usize
    }
}

/// A generated, replayable fault schedule.
#[derive(Debug, Clone)]
pub struct Schedule {
    seed: u64,
    config: ScheduleConfig,
    faults: Vec<ScheduledFault>,
}

impl Schedule {
    /// Generate the schedule for `seed`.
    ///
    /// Deterministic: same seed and config, same faults, forever. See the
    /// module docs for what that does and does not promise.
    pub fn generate(seed: u64, config: ScheduleConfig) -> Self {
        let mut rng = SplitMix64(seed ^ 0x50a4_c0de_1234_5678);
        let tolerated = config.replicas.saturating_sub(1) / 2;
        let mut faults: Vec<ScheduledFault> = Vec::new();

        let mut cursor = rng.range(config.min_gap_ms, config.max_gap_ms);
        while cursor < config.duration_ms {
            let length = rng.range(config.min_fault_ms, config.max_fault_ms);
            let end = (cursor + length).min(config.duration_ms);
            if end <= cursor {
                break;
            }

            let candidate = Self::draw_fault(&mut rng, &config);
            // Keep the majority invariant: only place this fault if doing
            // so leaves a majority working throughout its window.
            let placeable = match candidate.nodes_consumed() {
                None => true,
                Some(node) => {
                    let mut busy: BTreeSet<usize> = faults
                        .iter()
                        .filter(|f| f.overlaps(cursor, end))
                        .filter_map(|f| f.fault.nodes_consumed())
                        .collect();
                    busy.insert(node);
                    busy.len() <= tolerated
                }
            };

            if placeable {
                faults.push(ScheduledFault {
                    start_ms: cursor,
                    end_ms: end,
                    fault: candidate,
                });
                // Usually wait for this fault to end before drawing the
                // next. A third of the time, start the next one *inside*
                // this window instead.
                //
                // Overlap is not a garnish, it is most of the point. A
                // crash while a partition is already in force, a link cut
                // during a latency storm -- those are the sequences nobody
                // scripts by hand, and a generator that always waited for
                // one fault to finish would never produce them, leaving the
                // majority-invariant budget and the driver's overlapping-
                // fault handling permanently untested.
                cursor = if rng.below(3) == 0 {
                    cursor
                        + rng
                            .range(config.min_gap_ms, config.max_gap_ms)
                            .clamp(1, length)
                } else {
                    end + rng.range(config.min_gap_ms, config.max_gap_ms)
                };
            } else {
                // Skip ahead rather than retry in place, so a crowded
                // window cannot spin.
                cursor += rng.range(config.min_gap_ms, config.max_gap_ms).max(1);
            }
        }

        Self {
            seed,
            config,
            faults,
        }
    }

    /// Build a schedule from an explicit fault list rather than a seed.
    ///
    /// For replaying a hand-minimized schedule: when a soak fails, the
    /// useful next step is usually to delete faults until it stops failing,
    /// and that needs a schedule that is written down rather than drawn.
    /// The majority invariant is *not* enforced here -- a caller doing this
    /// is deliberately in control, and may well want to see what a lost
    /// quorum does.
    pub fn with_faults(seed: u64, config: ScheduleConfig, faults: Vec<ScheduledFault>) -> Self {
        Self {
            seed,
            config,
            faults,
        }
    }

    fn draw_fault(rng: &mut SplitMix64, config: &ScheduleConfig) -> Fault {
        let n = config.replicas;
        match rng.below(10) {
            // Weighted toward partitions: Antithesis's headline result was
            // that network turbulence *alone* -- no crashes, no disk faults
            // -- surfaced divergence in mature Raft implementations, so
            // that is what this soak spends most of its time doing.
            0..=3 => Fault::Isolate { node: rng.below(n) },
            4..=6 => {
                let from = rng.below(n);
                let mut to = rng.below(n);
                if to == from {
                    to = (to + 1) % n;
                }
                Fault::CutLink { from, to }
            }
            7..=8 => Fault::Crash { node: rng.below(n) },
            _ => Fault::Latency {
                ms: rng.range(10, 80),
            },
        }
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    pub fn config(&self) -> &ScheduleConfig {
        &self.config
    }

    pub fn faults(&self) -> &[ScheduledFault] {
        &self.faults
    }

    /// Faults in force at `t_ms`.
    pub fn active_at(&self, t_ms: u64) -> Vec<&ScheduledFault> {
        self.faults
            .iter()
            .filter(|f| f.start_ms <= t_ms && t_ms < f.end_ms)
            .collect()
    }

    /// The largest number of distinct nodes ever faulted at the same time.
    ///
    /// The soak asserts this stays within `f`; see the module docs for why
    /// that is what makes its liveness verdict meaningful.
    pub fn max_concurrent_node_faults(&self) -> usize {
        let mut worst = 0;
        // Every change happens at some fault's start, so those are the only
        // instants worth sampling.
        for probe in self.faults.iter().map(|f| f.start_ms) {
            let nodes: BTreeSet<usize> = self
                .active_at(probe)
                .into_iter()
                .filter_map(|f| f.fault.nodes_consumed())
                .collect();
            worst = worst.max(nodes.len());
        }
        worst
    }

    /// A one-line-per-fault rendering, for a failing run's report.
    pub fn render(&self) -> String {
        use std::fmt::Write as _;
        let mut out = format!(
            "schedule seed={} duration={}ms faults={}\n",
            self.seed,
            self.config.duration_ms,
            self.faults.len()
        );
        for f in &self.faults {
            let _ = writeln!(
                out,
                "  [{:>7}ms .. {:>7}ms] {:?}",
                f.start_ms, f.end_ms, f.fault
            );
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> ScheduleConfig {
        ScheduleConfig {
            replicas: 5,
            duration_ms: 120_000,
            ..ScheduleConfig::default()
        }
    }

    #[test]
    fn a_seed_replays_exactly_and_different_seeds_differ() {
        let a = Schedule::generate(7, config());
        let b = Schedule::generate(7, config());
        let c = Schedule::generate(8, config());
        assert_eq!(a.faults(), b.faults(), "a seed must replay exactly");
        assert_ne!(
            a.faults(),
            c.faults(),
            "different seeds must explore different turbulence"
        );
    }

    #[test]
    fn the_majority_invariant_holds_across_many_seeds() {
        // The property the liveness verdict rests on. Checked across a
        // spread of seeds and cluster sizes rather than one lucky schedule.
        for replicas in [3usize, 5, 7] {
            let tolerated = (replicas - 1) / 2;
            for seed in 0..64u64 {
                let schedule = Schedule::generate(
                    seed,
                    ScheduleConfig {
                        replicas,
                        ..config()
                    },
                );
                let worst = schedule.max_concurrent_node_faults();
                assert!(
                    worst <= tolerated,
                    "n={replicas} seed={seed}: {worst} nodes faulted at once, \
                     more than the tolerated {tolerated}\n{}",
                    schedule.render()
                );
            }
        }
    }

    #[test]
    fn a_schedule_is_dense_enough_to_be_worth_running() {
        // Anti-vacuity: a soak whose schedule is empty would pass every
        // safety check it makes.
        for seed in 0..16u64 {
            let schedule = Schedule::generate(seed, config());
            assert!(
                schedule.faults().len() >= 8,
                "seed {seed} generated only {} faults over {}ms",
                schedule.faults().len(),
                config().duration_ms
            );
        }
    }

    #[test]
    fn every_fault_ends_within_the_run() {
        for seed in 0..16u64 {
            let schedule = Schedule::generate(seed, config());
            for f in schedule.faults() {
                assert!(f.start_ms < f.end_ms, "empty window: {f:?}");
                assert!(
                    f.end_ms <= config().duration_ms,
                    "fault outlives the run: {f:?}"
                );
            }
        }
    }

    #[test]
    fn every_fault_kind_shows_up_over_enough_seeds() {
        // If the generator quietly stopped producing crashes, the soak
        // would still pass while testing less. Assert the mix.
        let mut isolates = 0;
        let mut cuts = 0;
        let mut crashes = 0;
        let mut latencies = 0;
        for seed in 0..32u64 {
            for f in Schedule::generate(seed, config()).faults() {
                match f.fault {
                    Fault::Isolate { .. } => isolates += 1,
                    Fault::CutLink { .. } => cuts += 1,
                    Fault::Crash { .. } => crashes += 1,
                    Fault::Latency { .. } => latencies += 1,
                }
            }
        }
        assert!(isolates > 0 && cuts > 0 && crashes > 0 && latencies > 0,
            "every fault kind should appear: isolates={isolates} cuts={cuts} crashes={crashes} latencies={latencies}");
    }

    #[test]
    fn a_one_way_cut_never_names_the_same_node_twice() {
        for seed in 0..32u64 {
            for f in Schedule::generate(seed, config()).faults() {
                if let Fault::CutLink { from, to } = f.fault {
                    assert_ne!(from, to, "a node cannot be cut off from itself");
                }
            }
        }
    }

    #[test]
    fn schedules_really_do_overlap_faults() {
        // Anti-vacuity for the generator itself. Both the majority-invariant
        // budget and the driver's `Injected` diffing exist only to handle
        // concurrent faults, so a generator that quietly stopped producing
        // them would leave that machinery untested while every test stayed
        // green. An earlier version of this generator did exactly that --
        // it always advanced past the previous fault's end.
        let mut with_overlap = 0;
        for seed in 0..32u64 {
            let schedule = Schedule::generate(seed, config());
            let overlapping = schedule
                .faults()
                .iter()
                .filter(|f| {
                    schedule
                        .faults()
                        .iter()
                        .any(|g| !std::ptr::eq(*f, g) && g.overlaps(f.start_ms, f.end_ms))
                })
                .count();
            if overlapping > 0 {
                with_overlap += 1;
            }
        }
        assert!(
            with_overlap >= 24,
            "only {with_overlap}/32 seeds produced any concurrent faults; \
             the interesting sequences are the concurrent ones"
        );
    }

    #[test]
    fn active_at_reports_the_window_half_open() {
        let schedule = Schedule::generate(3, config());
        let first = schedule.faults()[0];
        assert!(schedule
            .active_at(first.start_ms)
            .iter()
            .any(|f| **f == first));
        assert!(
            !schedule
                .active_at(first.end_ms)
                .iter()
                .any(|f| **f == first),
            "end is exclusive, so a fault is not active at its own end"
        );
    }
}
