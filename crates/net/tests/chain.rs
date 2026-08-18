//! Phase 9.2's (issue #56) node-side chain hook, against a real, in-process,
//! real-TCP 3-node cluster: `GET /chain`.
//!
//! # What these tests are really checking
//!
//! The hook exists so a conformance harness running *outside* the process
//! can compare replicas' `(n, h)` chain states. That only works if two
//! independent implementations agree byte-for-byte: the node folding the
//! chain as it applies commands, and the harness folding what it believes
//! was applied. An encoding drift between them would not fail loudly -- it
//! would make every cross-replica comparison miss, and a soak would report
//! "no divergence" forever while checking nothing.
//!
//! So the central test here does not merely assert the replicas agree with
//! *each other* (they would also agree if every hash were a constant). It
//! recomputes the expected chain independently, in the test, from the
//! commands it submitted, and requires every published checkpoint to match
//! that -- and requires a non-trivial number of such matches, so a run that
//! happened to publish nothing cannot pass.
//!
//! Submissions are closed-loop (each `Put` is acknowledged before the next
//! is sent), which is what makes the applied order knowable to the test:
//! operation `i` is decided before `i + 1` is submitted, so the log order is
//! the submission order.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use queso_chain::ChainState;
use queso_sim::ids::NodeId;
use queso_smr::{ClientId, Command, Outcome};

#[path = "support/mod.rs"]
mod support;
use support::{
    http_get, spawn_cluster_with_status, spawn_cluster_with_status_and_chain, submit_with_retry,
};

const SPACING: u64 = 4;
const COMMANDS: u64 = 14;

/// One replica's `/chain` body, parsed.
#[derive(Debug)]
struct ChainReport {
    every: u64,
    frontier_n: u64,
    frontier_h: u64,
    truncated: bool,
    /// `n -> h` for every checkpoint this replica published.
    checkpoints: BTreeMap<u64, u64>,
}

fn parse_hash(raw: &serde_json::Value) -> u64 {
    let text = raw.as_str().expect("a hash is serialized as a hex string");
    let digits = text
        .strip_prefix("0x")
        .unwrap_or_else(|| panic!("hash {text:?} should be 0x-prefixed"));
    u64::from_str_radix(digits, 16).unwrap_or_else(|err| panic!("parse hash {text:?}: {err}"))
}

async fn fetch_chain(addr: SocketAddr) -> ChainReport {
    let (code, body) = http_get(addr, "/chain").await;
    assert_eq!(code, 200, "expected /chain to report 200, body: {body:?}");
    let json: serde_json::Value = serde_json::from_str(&body)
        .unwrap_or_else(|err| panic!("parse /chain body: {err}\n{body}"));

    let checkpoints = json["checkpoints"]
        .as_array()
        .expect("checkpoints is an array")
        .iter()
        .map(|entry| {
            (
                entry["n"].as_u64().expect("checkpoint n is a number"),
                parse_hash(&entry["h"]),
            )
        })
        .collect();

    ChainReport {
        every: json["checkpoint_every"]
            .as_u64()
            .expect("checkpoint_every is a number"),
        frontier_n: json["frontier"]["n"].as_u64().expect("frontier n"),
        frontier_h: parse_hash(&json["frontier"]["h"]),
        truncated: json["truncated"].as_bool().expect("truncated is a bool"),
        checkpoints,
    }
}

fn put(seq: u64, value: i64) -> Command {
    Command::Put {
        client: ClientId(7),
        seq,
        key: 0,
        value,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn published_checkpoints_match_an_independently_computed_chain() {
    let (client_addrs, status_addrs) =
        spawn_cluster_with_status_and_chain(3, Some(NodeId(0)), Some(SPACING));
    let timeout = Duration::from_secs(10);

    // Closed-loop submission, so the applied order is the submission order
    // and the test can compute the expected chain exactly.
    let mut submitted: Vec<Command> = Vec::new();
    for i in 0..COMMANDS {
        let command = put(i, (i as i64 + 1) * 31);
        let outcome = submit_with_retry(client_addrs[0], &command, timeout).await;
        assert_eq!(outcome, Outcome::Put, "submission {i} should be applied");
        submitted.push(command);
    }

    // Ground truth, folded in the test from the same commands.
    let expected: BTreeMap<u64, u64> = ChainState::prefixes(&submitted)
        .into_iter()
        .map(|state| (state.n, state.h))
        .collect();

    let mut checked = 0usize;
    for (i, &status_addr) in status_addrs.iter().enumerate() {
        let report = fetch_chain(status_addr).await;

        assert_eq!(
            report.every, SPACING,
            "replica {i} must publish at the configured spacing"
        );
        assert!(
            !report.truncated,
            "replica {i} should not have dropped checkpoints in a {COMMANDS}-command run"
        );

        for (&n, &h) in &report.checkpoints {
            assert!(
                n.is_multiple_of(SPACING),
                "replica {i} published a checkpoint at n={n}, not a multiple of {SPACING}"
            );
            let want = expected.get(&n).unwrap_or_else(|| {
                panic!("replica {i} published n={n}, beyond what was submitted")
            });
            assert_eq!(
                h, *want,
                "replica {i}'s hash at n={n} disagrees with the chain computed here -- \
                 either the node and the harness encode commands differently, or this \
                 replica applied a different sequence"
            );
            checked += 1;
        }

        // The frontier is the same chain, one step finer.
        if report.frontier_n > 0 {
            let want = expected.get(&report.frontier_n).unwrap_or_else(|| {
                panic!(
                    "replica {i} reports frontier n={}, beyond what was submitted",
                    report.frontier_n
                )
            });
            assert_eq!(
                report.frontier_h, *want,
                "replica {i}'s frontier hash disagrees with the chain computed here"
            );
        }
    }

    // Anti-vacuity: a run in which nothing was published would satisfy every
    // assertion above. With 14 commands at a spacing of 4, the leader alone
    // must have published n=4, 8 and 12.
    assert!(
        checked >= 3,
        "only {checked} checkpoints were checked against ground truth -- too few for \
         this test to mean anything"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn replicas_never_publish_conflicting_hashes_at_the_same_n() {
    let (client_addrs, status_addrs) =
        spawn_cluster_with_status_and_chain(3, Some(NodeId(0)), Some(SPACING));
    let timeout = Duration::from_secs(10);

    for i in 0..COMMANDS {
        let command = put(i, (i as i64 + 1) * 17);
        assert_eq!(
            submit_with_retry(client_addrs[0], &command, timeout).await,
            Outcome::Put
        );
    }

    // Give the followers a little traffic of their own: a Queso replica
    // catches up by participating, so without this the non-leaders may sit
    // far behind and share no checkpoint with the leader at all.
    for i in 0..6u64 {
        let command = put(COMMANDS + i, (i as i64 + 1) * 19);
        let addr = client_addrs[(i as usize) % client_addrs.len()];
        assert_eq!(
            submit_with_retry(addr, &command, timeout).await,
            Outcome::Put
        );
    }

    let mut witnessed: BTreeMap<u64, (usize, u64)> = BTreeMap::new();
    let mut comparisons = 0usize;
    for (i, &status_addr) in status_addrs.iter().enumerate() {
        let report = fetch_chain(status_addr).await;
        for (&n, &h) in &report.checkpoints {
            match witnessed.get(&n) {
                Some(&(first, first_hash)) => {
                    comparisons += 1;
                    assert_eq!(
                        h, first_hash,
                        "replicas {first} and {i} disagree at n={n}: divergence"
                    );
                }
                None => {
                    witnessed.insert(n, (i, h));
                }
            }
        }
    }

    // The safety verdict above is only worth something if replicas actually
    // met at shared checkpoints -- which is the whole reason the hook
    // publishes at fixed `n` rather than at each replica's own frontier.
    assert!(
        comparisons >= 2,
        "only {comparisons} cross-replica comparisons were possible; checkpointed \
         sampling is supposed to make replicas comparable by construction"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_chain_endpoint_is_absent_unless_the_hook_is_configured() {
    // Same status server, no `chain_checkpoints` -- i.e. exactly how every
    // ordinary deployment runs.
    let (client_addrs, status_addrs) = spawn_cluster_with_status(3, Some(NodeId(0)));
    let timeout = Duration::from_secs(10);
    assert_eq!(
        submit_with_retry(client_addrs[0], &put(0, 5), timeout).await,
        Outcome::Put
    );

    let (code, body) = http_get(status_addrs[0], "/chain").await;
    assert_eq!(
        code, 404,
        "an unconfigured node must not serve /chain at all: a harness reading an \
         empty table as 'this replica has applied nothing' would be far worse than \
         a 404. body: {body:?}"
    );

    // ...and the rest of the status server is untouched by the new route.
    let (code, body) = http_get(status_addrs[0], "/metrics").await;
    assert_eq!(code, 200, "body: {body:?}");
    assert!(body.contains("next_slot"), "body: {body:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_chain_frontier_agrees_with_the_metrics_frontier() {
    // Two endpoints, two code paths, one underlying replica: `/metrics`'
    // `next_slot` and `/chain`'s `frontier.n` are both "how far has this
    // replica applied", and must not drift apart.
    let (client_addrs, status_addrs) =
        spawn_cluster_with_status_and_chain(3, Some(NodeId(0)), Some(SPACING));
    let timeout = Duration::from_secs(10);

    for i in 0..COMMANDS {
        assert_eq!(
            submit_with_retry(client_addrs[0], &put(i, i as i64), timeout).await,
            Outcome::Put
        );
    }

    let (code, body) = http_get(status_addrs[0], "/metrics").await;
    assert_eq!(code, 200, "body: {body:?}");
    let metrics: serde_json::Value = serde_json::from_str(&body).expect("parse /metrics");
    let next_slot = metrics["next_slot"].as_u64().expect("next_slot");

    let report = fetch_chain(status_addrs[0]).await;
    assert_eq!(
        report.frontier_n, next_slot,
        "/chain's frontier and /metrics' next_slot describe the same thing"
    );
    assert!(
        next_slot >= COMMANDS,
        "the leader should have applied everything submitted to it; next_slot={next_slot}"
    );
}
