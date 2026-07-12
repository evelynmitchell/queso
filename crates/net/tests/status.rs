//! Phase 8.2's (issue #47) acceptance test: the opt-in status/metrics HTTP
//! server (`GET /health`/`/ready`/`/metrics`, `queso_net::status`) against a
//! real, in-process, real-TCP 3-node cluster -- and, separately, that an
//! ordinary cluster with no status listener configured behaves exactly like
//! every other `queso-net` test (see [`status_disabled_by_default_still_serves_put_and_get`]).

use std::time::Duration;

use queso_sim::ids::NodeId;
use queso_smr::{ClientId, Command, Outcome};

#[path = "support/mod.rs"]
mod support;
use support::{http_get, spawn_cluster, spawn_cluster_with_status, submit_with_retry};

/// `GET /health` must answer `200` right away (process-up liveness, see
/// `queso_net::status`'s module docs) -- before this replica has served a
/// single client operation. `GET /ready` becomes `200` once this replica
/// has processed at least one operation (a fresh boot is honestly ready
/// immediately -- see `StatusShared::is_ready`'s docs -- but this test
/// drives a `Put` first anyway, both to exercise the common case and to
/// prove `/metrics`' counters actually move). `GET /metrics` reports
/// counters that move as expected (`save_count`/`next_slot` advance after a
/// `Put`), and `GET /unknown` is a `404`.
#[tokio::test(flavor = "multi_thread")]
async fn status_endpoints_report_health_ready_and_metrics() {
    let (client_addrs, status_addrs) = spawn_cluster_with_status(3, Some(NodeId(0)));
    let timeout = Duration::from_secs(10);
    let leader_status = status_addrs[0];

    // `/health` must be reachable immediately, with no operation ever
    // submitted yet -- pure process-up liveness.
    let (code, body) = http_get(leader_status, "/health").await;
    assert_eq!(code, 200, "expected /health to report 200, body: {body:?}");
    assert!(body.contains("ok"));

    // Baseline `/metrics` before any op: a fresh (never-restarted) replica
    // has applied nothing yet.
    let (code, body) = http_get(leader_status, "/metrics").await;
    assert_eq!(code, 200);
    let baseline: serde_json::Value =
        serde_json::from_str(&body).expect("metrics body is valid JSON");
    assert_eq!(baseline["next_slot"], 0);
    assert_eq!(baseline["save_count"], 0);

    // Drive one real operation through the leader.
    let put = Command::Put {
        client: ClientId(1),
        seq: 0,
        key: 42,
        value: 7,
    };
    let put_outcome = submit_with_retry(client_addrs[0], &put, timeout).await;
    assert_eq!(put_outcome, Outcome::Put);

    // `/ready` must now report 200: this replica is well past its (nonexistent,
    // fresh-boot) catch-up phase and has actively processed an op.
    let (code, body) = http_get(leader_status, "/ready").await;
    assert_eq!(code, 200, "expected /ready to report 200, body: {body:?}");
    assert!(body.contains("ready"));

    // `/metrics` counters must have moved: a decided Put durably persists
    // (save_count > 0) and advances the log frontier (next_slot >= 1).
    let (code, body) = http_get(leader_status, "/metrics").await;
    assert_eq!(code, 200);
    let after_put: serde_json::Value =
        serde_json::from_str(&body).expect("metrics body is valid JSON");
    assert!(
        after_put["save_count"].as_u64().unwrap() > 0,
        "expected save_count to have advanced past 0 after a decided Put, got: {after_put}"
    );
    assert!(
        after_put["next_slot"].as_u64().unwrap() >= 1,
        "expected next_slot to have advanced to at least 1 after a decided Put, got: {after_put}"
    );
    assert!(
        after_put["events_processed"].as_u64().unwrap() > 0,
        "expected events_processed to be nonzero after a decided Put, got: {after_put}"
    );
    assert_eq!(after_put["ready"], true);
    assert!(after_put["uptime_secs"].as_f64().unwrap() >= 0.0);

    // Only GET on exactly /health, /ready, /metrics is served -- anything
    // else is a 404, never a panic or a hang.
    let (code, _) = http_get(leader_status, "/unknown").await;
    assert_eq!(code, 404);

    // A non-leader replica's status server independently reports its own
    // (also-caught-up, since it's part of the same 3-node quorum) state --
    // proving this isn't just leader-specific wiring.
    let (code, body) = http_get(status_addrs[1], "/health").await;
    assert_eq!(
        code, 200,
        "expected replica 1's /health to be 200, body: {body:?}"
    );
}

/// The status server must be truly absent -- not merely idle -- when
/// `NodeConfig::status_listen_addr` is `None` (every existing `queso-net`
/// test, and `queso-node` unless `--status-listen` is passed). This is the
/// same [`spawn_cluster`] every other `tests/cluster.rs`-style test in this
/// crate already uses (which never sets `status_listen_addr`), driving a
/// real `Put`/`Get` round trip end to end -- proving that having the status
/// feature compiled in and available costs this cluster nothing observable
/// when it isn't opted into. (There is no port to probe here precisely
/// *because* nothing is bound -- see `crate::driver::run_node_inner`'s
/// `status_listener.map(...)` -- so behavioral equivalence with every other
/// cluster test is the meaningful assertion, not a failed-connect probe
/// against an address this test was never given.)
#[tokio::test(flavor = "multi_thread")]
async fn status_disabled_by_default_still_serves_put_and_get() {
    let client_addrs = spawn_cluster(3, Some(NodeId(0)));
    let timeout = Duration::from_secs(10);

    let put = Command::Put {
        client: ClientId(2),
        seq: 0,
        key: 99,
        value: 123,
    };
    let put_outcome = submit_with_retry(client_addrs[0], &put, timeout).await;
    assert_eq!(put_outcome, Outcome::Put);

    let get = Command::Get {
        client: ClientId(2),
        seq: 1,
        key: 99,
    };
    let get_outcome = submit_with_retry(client_addrs[2], &get, timeout).await;
    assert_eq!(get_outcome, Outcome::Get(Some(123)));
}
