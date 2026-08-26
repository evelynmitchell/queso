//! Phase 9.2 (issue #73): the decisive cross-check, against real processes.
//!
//! The Chain-of-Blocks observer judges replicas by sampled `(n, h)` pairs
//! read over `/chain`. When it reports a divergence, that report alone
//! cannot say whether two replicas really applied different commands or the
//! observability path mis-reported one of them -- both look like two hashes
//! at one height. `queso_soak::postmortem` settles it from the replicas'
//! durable applied logs, which are not on that path.
//!
//! This file runs that adjudication end to end, against real `queso-node`
//! processes, on a run whose outcome is known good. That direction matters:
//!
//! - It proves the loader reads snapshots the real driver really wrote,
//!   with real command sequences in them. The unit tests in `postmortem.rs`
//!   can only write *empty* logs (`Durable`'s fields are `pub(crate)` to
//!   `queso-smr`), so they pin down the comparison logic and nothing about
//!   whether it can be pointed at a real failure.
//! - It proves `/chain` and the durable log agree on a healthy cluster.
//!   Every hash the observer saw is re-derived here from the applied log.
//!   Had this been in place when #73 was first reported, the same three
//!   lines would have adjudicated it.
//!
//! A tool that only ever runs on the failing case is a tool nobody knows
//! the calibration of. This is the calibration.
//!
//! `#[ignore]`d for the same reason as `real_cluster.rs`'s scenarios -- see
//! that file's docs; CI runs them in the real-process job.

use std::num::NonZeroUsize;
use std::time::Duration;

use queso_conformance::observer::Observer;
use queso_conformance::workload::{converge, run, CobWorkload, RunConfig};
use queso_soak::cluster::{ClusterConfig, RealCluster};
use queso_soak::postmortem::{Claim, ClaimVerdict, PairVerdict, Postmortem};

fn config() -> ClusterConfig {
    ClusterConfig {
        replicas: 3,
        leader: 0,
        checkpoint_every: 2,
        tick_ms: 5,
        submit_timeout: Duration::from_secs(3),
    }
}

fn run_config(commands: usize) -> RunConfig {
    RunConfig {
        commands,
        advance_between: 20,
        poll_every: 2,
        settle: 1_500,
    }
}

#[test]
#[ignore = "boots real node processes; run with --ignored (see this file's docs)"]
fn the_durable_logs_corroborate_every_hash_the_observer_saw() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let claims = {
        let mut cluster = RealCluster::start(config(), data_dir.path()).expect("boot cluster");
        cluster.await_ready(Duration::from_secs(45)).expect("ready");

        let mut workload = CobWorkload::new(0x73_0073);
        let mut observer = Observer::new();
        run(&mut cluster, &mut workload, &mut observer, run_config(16));
        converge(&mut cluster, &mut workload, &mut observer, 2, 800);

        assert!(
            observer.divergences().is_empty(),
            "this scenario is the calibration case and must run clean:\n{}",
            observer.render_report()
        );

        // What the observability path claims about each replica, taken from
        // the observer itself rather than restated by hand.
        let claims: Vec<Claim> = observer
            .latest_states()
            .into_iter()
            .map(|(replica, state)| Claim {
                replica,
                n: state.n,
                h: state.h,
            })
            .collect();
        assert_eq!(claims.len(), 3, "every replica must have been observed");
        assert!(
            claims.iter().all(|c| c.n >= 8),
            "the cluster barely progressed, so corroborating it proves little: {claims:?}"
        );
        claims
        // The cluster is dropped here: real processes killed, data dir left
        // behind. Everything below reads only what reached disk, which is
        // exactly what a preserved failure offers.
    };

    let postmortem = Postmortem::open(data_dir.path()).expect("read preserved data dir");
    assert_eq!(
        postmortem.logs().count(),
        3,
        "every replica should have left a snapshot:\n{}",
        postmortem.render(&claims)
    );

    // The applied logs agree with each other, over a range worth calling a
    // comparison.
    for (left, right, verdict) in postmortem.pairs() {
        match verdict {
            PairVerdict::Agree { compared } => assert!(
                compared >= NonZeroUsize::new(8).unwrap(),
                "{left} vs {right} agreed on only {compared} slot(s), which proves little:\n{}",
                postmortem.render(&claims)
            ),
            other => panic!(
                "{left} vs {right}: {other:?}\n{}",
                postmortem.render(&claims)
            ),
        }
    }

    // And every hash `/chain` reported is re-derivable from the log of the
    // replica it was attributed to. This is the cross-check #73 asks for,
    // run in the direction where the answer is known.
    for claim in &claims {
        assert_eq!(
            postmortem.check(*claim),
            ClaimVerdict::Confirmed,
            "the observability path reported a hash replica {} cannot account for:\n{}",
            claim.replica,
            postmortem.render(&claims)
        );
    }

    // Anti-vacuity for the check itself: a hash the log does *not* fold to
    // must come back contradicted, or "confirmed" above meant nothing.
    let tampered = Claim {
        h: claims[0].h ^ 1,
        ..claims[0]
    };
    assert!(
        matches!(
            postmortem.check(tampered),
            ClaimVerdict::Contradicted { .. }
        ),
        "a hash off by one bit was still confirmed -- the check is not checking"
    );
}
