// Real wall-clock timing is exactly what this test measures -- same
// per-crate-root allow as `leader_dos.rs`.
#![allow(clippy::disallowed_methods)]

//! Detection power for `queso_compare::stall::StallMonitor` (issue #107).
//!
//! `leader_dos.rs` subtracts observed scheduling stalls from its headline
//! availability gap, so that a runner which froze the whole process cannot
//! be mistaken for a cluster that went quiet. That subtraction is only
//! worth anything if the monitor *notices* a freeze -- and the unit tests
//! in `queso_compare::stall` cover the attribution arithmetic, not the
//! observation. This file covers the observation, by causing the exact
//! condition the correction exists for: the operating system stops
//! scheduling this process, then resumes it.
//!
//! # Why this test is `#[ignore]`d
//!
//! It `SIGSTOP`s its own process and relies on a helper shell to `SIGCONT`
//! it. If the helper were killed in between -- a runner teardown, an OOM
//! kill -- the test process would stay stopped until the job timed out,
//! turning a flake into a hung CI job. That risk is not worth carrying in
//! the commit gate for a test whose job is to be run deliberately, when
//! the monitor's constants or logic change. Run it with:
//!
//! ```sh
//! cargo test -p queso-compare --test stall_detection -- --ignored --nocapture
//! ```
//!
//! Measured on this sandbox at the commit that introduced it: a 1000ms
//! `SIGSTOP` was observed as a 1.0s worst-case stall, against a 0ns worst
//! case on the same machine with nothing stopping the process.
//!
//! Unix-only: there is no portable way to suspend one's own process, and
//! the CI runners this guards (`ubuntu-latest`) are Linux.

#![cfg(unix)]

use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

use queso_compare::stall::StallMonitor;

/// How long the process is suspended for. Comfortably above the monitor's
/// `FLOOR`, and in the same range as the 4.61s gap that motivated #107.
const SUSPEND: Duration = Duration::from_millis(1_000);

/// Delay before the suspension, so the monitor has ticked normally first.
const SETTLE: Duration = Duration::from_millis(300);

#[test]
#[ignore = "SIGSTOPs its own process; run deliberately, see the module docs"]
fn the_monitor_observes_the_os_suspending_this_process() {
    let pid = std::process::id();
    // `sh` is stopped and continued by a *separate* process, because a
    // stopped process cannot resume itself.
    let mut helper = Command::new("sh")
        .arg("-c")
        .arg(format!(
            "sleep {settle}; kill -STOP {pid}; sleep {suspend}; kill -CONT {pid}",
            settle = SETTLE.as_secs_f64(),
            suspend = SUSPEND.as_secs_f64(),
        ))
        .spawn()
        .expect("spawn the suspend/resume helper");

    let origin = Instant::now();
    let monitor = StallMonitor::start(origin);
    // Stay alive across the whole suspend-and-resume window. This sleep is
    // itself suspended along with everything else, so it returns late --
    // which is the point.
    thread::sleep(SETTLE + SUSPEND + Duration::from_millis(500));
    let seen = monitor.stop();

    helper.wait().expect("the helper must exit");

    assert!(
        seen.ticks() > 0,
        "the monitor never ticked, so it observed nothing: {seen:?}"
    );

    // The headline: a suspension of `SUSPEND` shows up as a stall of
    // roughly `SUSPEND`. Allowed to run short by one tick's worth of
    // rounding and long by the helper's own scheduling slop.
    let worst = seen.worst();
    assert!(
        worst >= SUSPEND.mul_f64(0.8),
        "a {SUSPEND:?} SIGSTOP must be observed as a stall of about that \
         length, but the worst recorded was {worst:?}: {seen:?}"
    );
    assert!(
        worst < SUSPEND * 3,
        "a {SUSPEND:?} SIGSTOP was recorded as {worst:?}, far more than the \
         suspension itself -- the monitor is over-reporting, which would \
         over-credit the correction in leader_dos.rs: {seen:?}"
    );

    // And it lands in the right place on the timeline: the correction in
    // `leader_dos.rs` intersects stalls with one inter-op window, so a
    // stall recorded at the wrong offset would be subtracted from the
    // wrong gap.
    let during = seen.frozen_within(SETTLE, SETTLE + SUSPEND + Duration::from_millis(200));
    assert!(
        during >= SUSPEND.mul_f64(0.8),
        "the stall must be timestamped inside the window it happened in, \
         but only {during:?} of it fell there: {seen:?}"
    );
    let before = seen.frozen_within(Duration::ZERO, SETTLE.mul_f64(0.5));
    assert_eq!(
        before,
        Duration::ZERO,
        "nothing stalled the process before the SIGSTOP, so that window must \
         be clean: {seen:?}"
    );

    eprintln!(
        "stall detection: {SUSPEND:?} SIGSTOP observed as worst={worst:?}, \
         total={:?}, {} tick(s) over {:?}",
        seen.total(),
        seen.ticks(),
        seen.monitored(),
    );
}
