//! Process-wide scheduling-stall measurement, so a wall-clock latency
//! number can say *whose* stall it was.
//!
//! # The problem this exists for (issue #107)
//!
//! `tests/leader_dos.rs` reports the headline availability-gap number of
//! the Phase 7.5 comparison: the longest wall-clock gap between two
//! consecutive completed writes while the fast-path leader is isolated,
//! asserted to stay under a plausible Raft election-timeout window. That
//! bound is not a tuning knob -- the whole claim is "shorter than an
//! election timeout", and etcd's default is 1s with randomized backoff to
//! roughly 2s, so the assertion cannot simply be loosened without
//! discarding what it asserts.
//!
//! But a gap measured with two `Instant::now()` calls around an operation
//! cannot, on its own, tell "the cluster went quiet" apart from "this
//! whole process stopped being scheduled". On a shared two-vCPU CI runner
//! with another workflow's build resident, the second is a real
//! possibility, and it is what produced the 4.61s gap in CI run
//! [33221049520] on a tree that had passed the same test minutes earlier.
//!
//! [33221049520]: https://github.com/evelynmitchell/queso/actions/runs/33221049520
//!
//! # What this measures, and what it does not
//!
//! [`StallMonitor`] runs one otherwise-idle thread that sleeps [`TICK`]
//! at a time and records every interval in which it was still not running
//! more than [`FLOOR`] after its sleep was due. Those intervals are
//! *observed* scheduling delay for this process, timestamped, so
//! [`StallReport::frozen_within`] can subtract the part of them that
//! overlaps a particular measurement window.
//!
//! Two limits, stated because the correction is only as good as they are:
//!
//! - **It reads as a lower bound on contention, not a measure of it**
//!   (argued, from what the two workloads need rather than from any
//!   enumeration): a thread that only sleeps needs one scheduling slot to
//!   look healthy, while the cluster's three node threads need sustained
//!   CPU and network round-trips. A runner that starves the cluster more
//!   than it starves an idle sleeper is therefore expected to be
//!   under-corrected for, leaving the assertion to fail. That direction is
//!   the deliberate one: under-correcting fails a run that may be the
//!   machine's fault, which costs a re-run; over-correcting would pass a
//!   run that was the cluster's fault, which costs the claim.
//! - **It has not been shown to catch the occurrence that motivated it.**
//!   Run 33221049520 predates this instrumentation and its artifacts are
//!   gone, so whether the monitor would have recorded a matching stall
//!   there is *unknown*, not established. What is established (by this
//!   module's unit tests) is that the attribution arithmetic is correct;
//!   what the monitor buys is that the next occurrence arrives with the
//!   discriminating measurement attached instead of an argument about it.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// How often the monitor thread wakes to check it was scheduled on time.
///
/// Short enough to localize a stall to well inside one inter-op gap, long
/// enough that the monitor is not itself a meaningful load on a two-vCPU
/// runner.
pub const TICK: Duration = Duration::from_millis(20);

/// Lateness at or below this is ordinary scheduler jitter and is not
/// recorded. Two orders of magnitude below the 2s bound it protects, and
/// well above the sub-millisecond wake-up jitter of an unloaded machine.
pub const FLOOR: Duration = Duration::from_millis(100);

/// A running measurement of how late this process's threads are being
/// scheduled. Start it around a wall-clock measurement, [`StallMonitor::stop`]
/// it afterwards, and attribute with [`StallReport::frozen_within`].
#[derive(Debug)]
pub struct StallMonitor {
    origin: Instant,
    stop: Arc<AtomicBool>,
    handle: JoinHandle<(Vec<(Duration, Duration)>, u64)>,
}

impl StallMonitor {
    /// Begin monitoring. `origin` is the zero point every recorded span is
    /// reported relative to -- pass the same instant the measurement being
    /// corrected is timed from, so the two share a clock.
    pub fn start(origin: Instant) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let handle = thread::Builder::new()
            .name("queso-stall-monitor".to_string())
            .spawn(move || {
                let mut stalls: Vec<(Duration, Duration)> = Vec::new();
                let mut ticks = 0u64;
                while !stop_flag.load(Ordering::Relaxed) {
                    let slept_from = Instant::now();
                    thread::sleep(TICK);
                    let awake_at = Instant::now();
                    ticks += 1;
                    // The sleep itself is legitimate; only the time past
                    // when it was *due* counts as the process not running.
                    // Recording that sub-interval (rather than the whole
                    // tick) is what makes the overlap arithmetic in
                    // `frozen_within` an attribution rather than an
                    // over-estimate.
                    let due = slept_from + TICK;
                    if awake_at.saturating_duration_since(due) > FLOOR {
                        stalls.push((
                            due.saturating_duration_since(origin),
                            awake_at.saturating_duration_since(origin),
                        ));
                    }
                }
                (stalls, ticks)
            })
            .expect("spawn the stall-monitor thread");
        Self {
            origin,
            stop,
            handle,
        }
    }

    /// Stop monitoring and collect what was observed.
    pub fn stop(self) -> StallReport {
        self.stop.store(true, Ordering::Relaxed);
        let (stalls, ticks) = self
            .handle
            .join()
            .expect("the stall-monitor thread must not panic");
        StallReport {
            stalls,
            ticks,
            monitored: self.origin.elapsed(),
        }
    }
}

/// What a finished [`StallMonitor`] saw.
///
/// Spans are `(start, end)` offsets from the monitor's origin, each one an
/// interval during which the monitor thread was due to be running and was
/// not.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StallReport {
    stalls: Vec<(Duration, Duration)>,
    ticks: u64,
    monitored: Duration,
}

impl StallReport {
    /// Build a report from explicit spans. For tests that need a report
    /// without running a thread; the monitor itself does not use this.
    pub fn from_spans(stalls: Vec<(Duration, Duration)>, ticks: u64, monitored: Duration) -> Self {
        Self {
            stalls,
            ticks,
            monitored,
        }
    }

    /// How many times the monitor thread completed a tick.
    ///
    /// Zero means the monitor never ran, so its silence says nothing --
    /// worth asserting on before trusting a zero correction.
    pub fn ticks(&self) -> u64 {
        self.ticks
    }

    /// Wall-clock time from the monitor's origin to when it was stopped.
    pub fn monitored(&self) -> Duration {
        self.monitored
    }

    /// Recorded stall spans, as `(start, end)` offsets from the origin.
    pub fn spans(&self) -> &[(Duration, Duration)] {
        &self.stalls
    }

    /// The longest single stall recorded, or zero if none was.
    pub fn worst(&self) -> Duration {
        self.stalls
            .iter()
            .map(|&(start, end)| end.saturating_sub(start))
            .max()
            .unwrap_or(Duration::ZERO)
    }

    /// Total recorded stall time over the whole monitored period.
    pub fn total(&self) -> Duration {
        self.stalls
            .iter()
            .map(|&(start, end)| end.saturating_sub(start))
            .sum()
    }

    /// How much of `[start, end)` this process spent not being scheduled.
    ///
    /// Subtracting this from a wall-clock gap measured over the same
    /// window leaves the part of the gap that something other than the
    /// machine's scheduler has to account for.
    pub fn frozen_within(&self, start: Duration, end: Duration) -> Duration {
        self.stalls
            .iter()
            .map(|&(stall_start, stall_end)| overlap(stall_start, stall_end, start, end))
            .sum()
    }
}

/// Length of the intersection of `[a0, a1)` and `[b0, b1)`.
fn overlap(a0: Duration, a1: Duration, b0: Duration, b1: Duration) -> Duration {
    let lo = a0.max(b0);
    let hi = a1.min(b1);
    hi.saturating_sub(lo)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    fn report(spans: &[(u64, u64)]) -> StallReport {
        StallReport::from_spans(
            spans.iter().map(|&(s, e)| (ms(s), ms(e))).collect(),
            1_000,
            ms(10_000),
        )
    }

    #[test]
    fn disjoint_intervals_do_not_overlap() {
        assert_eq!(overlap(ms(0), ms(10), ms(20), ms(30)), Duration::ZERO);
        assert_eq!(overlap(ms(20), ms(30), ms(0), ms(10)), Duration::ZERO);
    }

    #[test]
    fn touching_intervals_do_not_overlap() {
        assert_eq!(overlap(ms(0), ms(10), ms(10), ms(20)), Duration::ZERO);
    }

    #[test]
    fn overlap_is_the_intersection_in_every_containment_shape() {
        // Partial, either direction.
        assert_eq!(overlap(ms(0), ms(30), ms(20), ms(50)), ms(10));
        assert_eq!(overlap(ms(20), ms(50), ms(0), ms(30)), ms(10));
        // Stall inside the window, and window inside the stall.
        assert_eq!(overlap(ms(10), ms(20), ms(0), ms(100)), ms(10));
        assert_eq!(overlap(ms(0), ms(100), ms(10), ms(20)), ms(10));
    }

    /// The correction must charge a stall only to the gap it happened in.
    /// Without this, a freeze anywhere in the run would excuse a stall
    /// anywhere else -- which is precisely the vacuous version of this
    /// check, and the reason the monitor timestamps its spans at all.
    ///
    /// Falsifier: sum the spans wholesale instead of intersecting them
    /// (`self.total()` in place of the map over `overlap`) and the second
    /// and third assertions here both fail.
    #[test]
    fn a_stall_is_charged_only_to_the_window_it_falls_in() {
        let seen = report(&[(1_000, 1_500)]);
        assert_eq!(seen.frozen_within(ms(900), ms(1_600)), ms(500));
        assert_eq!(seen.frozen_within(ms(0), ms(900)), Duration::ZERO);
        assert_eq!(seen.frozen_within(ms(2_000), ms(3_000)), Duration::ZERO);
        // A window catching only part of the stall is credited only that
        // part.
        assert_eq!(seen.frozen_within(ms(1_200), ms(1_600)), ms(300));
    }

    #[test]
    fn several_stalls_in_one_window_are_summed() {
        let seen = report(&[(100, 200), (300, 450), (5_000, 5_100)]);
        assert_eq!(seen.frozen_within(ms(0), ms(1_000)), ms(250));
        assert_eq!(seen.worst(), ms(150));
        assert_eq!(seen.total(), ms(350));
    }

    #[test]
    fn a_report_with_no_stalls_corrects_nothing() {
        let seen = report(&[]);
        assert_eq!(seen.frozen_within(ms(0), ms(10_000)), Duration::ZERO);
        assert_eq!(seen.worst(), Duration::ZERO);
        assert_eq!(seen.total(), Duration::ZERO);
    }

    /// The monitor must actually tick, or its silence is not evidence of a
    /// quiet machine -- which is what `ticks()` exists for the caller to
    /// assert on.
    #[test]
    fn a_monitor_that_ran_reports_its_ticks() {
        let monitor = StallMonitor::start(Instant::now());
        thread::sleep(TICK * 5);
        let seen = monitor.stop();
        assert!(
            seen.ticks() > 0,
            "the monitor slept through {:?} of monitoring without a tick: {seen:?}",
            seen.monitored()
        );
    }
}
