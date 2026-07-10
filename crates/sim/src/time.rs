//! The virtual logical clock.
//!
//! `LogicalTime` is the *only* notion of time the kernel and its components
//! are allowed to observe. It is an opaque tick counter advanced solely by
//! the event loop as it pops events off the priority queue — nothing else
//! (including node/scheduler code) may mutate it. There is deliberately no
//! `now()` free function anywhere in this crate that reads the wall clock;
//! `clippy.toml` additionally denies `std::time::Instant::now` and
//! `std::time::SystemTime::now` workspace-wide as a build-time guard.

use std::fmt;

/// A point in virtual/logical time, measured in kernel "ticks". Ticks have
/// no relation to wall-clock time; they exist purely to give a total,
/// reproducible order to events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct LogicalTime(pub u64);

impl LogicalTime {
    /// The start of simulated time.
    pub const ZERO: LogicalTime = LogicalTime(0);

    /// Returns `self + delta`, saturating at `u64::MAX` rather than
    /// overflowing (a scenario that runs long enough to hit this has bigger
    /// problems than a wrapped clock).
    #[must_use]
    pub fn advance(self, delta: u64) -> LogicalTime {
        LogicalTime(self.0.saturating_add(delta))
    }
}

impl fmt::Display for LogicalTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "t={}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_is_zero() {
        assert_eq!(LogicalTime::ZERO, LogicalTime(0));
    }

    #[test]
    fn advance_adds_ticks() {
        let t = LogicalTime(10);
        assert_eq!(t.advance(5), LogicalTime(15));
    }

    #[test]
    fn advance_saturates_instead_of_overflowing() {
        let t = LogicalTime(u64::MAX - 1);
        assert_eq!(t.advance(10), LogicalTime(u64::MAX));
    }

    #[test]
    fn ordering_is_by_tick_count() {
        assert!(LogicalTime(1) < LogicalTime(2));
        assert!(LogicalTime(5) <= LogicalTime(5));
        let mut times = [LogicalTime(3), LogicalTime(1), LogicalTime(2)];
        times.sort();
        assert_eq!(times, [LogicalTime(1), LogicalTime(2), LogicalTime(3)]);
    }
}
