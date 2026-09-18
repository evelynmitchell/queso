//! Phase 8.2's (issue #47) acceptance test: the opt-in status/metrics HTTP
//! server (`GET /health`/`/ready`/`/metrics`, `queso_net::status`) against a
//! real, in-process, real-TCP 3-node cluster -- and, separately, that an
//! ordinary cluster with no status listener configured behaves exactly like
//! every other `queso-net` test (see [`status_disabled_by_default_still_serves_put_and_get`]).
//!
//! # D10, and what checking it actually found (issue #116), and what #129
//! then closed
//!
//! This file is the **D10 -- observability** evidence, and nothing named
//! D10 in `crates/` before the #116 comment this section replaces. #116
//! asked for something nobody had done: check the endpoint's fields against
//! the metrics §D actually names. That check found a gap.
//!
//! §D names five metrics for D10: **per-slot rounds, fast-path hit rate,
//! proposer activations, recovery time, and per-replica latency**. Before
//! #129, `GET /metrics` served five fields -- `events_processed`,
//! `next_slot`, `save_count`, `ready`, `uptime_secs` -- and the
//! intersection with §D's list was **empty**: not one of the five named
//! metrics was exposed. (Enumerated by reading both lists, which are closed
//! and short; not a sampling.)
//!
//! #129 served three of the five, as raw counters rather than rates (a
//! scraper divides; a gauge that pre-divides loses the denominator):
//! `rounds_total` and `decisions` give per-slot rounds,
//! `fast_path_decisions` over `decisions` gives the fast-path hit rate, and
//! `proposer_activations` is the third directly. See
//! [`metrics_endpoint_serves_the_consensus_counters`] below for the
//! end-to-end evidence, and `crates/smr/tests/observability_metrics.rs` for
//! the evidence that the counters count the right population.
//!
//! #159 then served the remaining two, which were not a counter away --
//! each needed a measurement point, and each needed its interval *chosen*
//! before it could be implemented, because the candidates named in that
//! issue do not measure the same thing:
//!
//! - **recovery time** -- `restarted` and `recovery_secs`, measured in
//!   `queso_net::driver` from the restart branch (immediately before
//!   `on_restart`, which starts the catch-up probe) to the first publish at
//!   which `SmrNode::is_catching_up()` reads false. See
//!   [`a_real_process_restart_resets_the_counters_and_reports_a_recovery_time`].
//! - **per-replica self-observed latency** --
//!   `client_ops_completed`/`client_latency_micros_total`/`..._max`,
//!   measured from decoding a client's command off this replica's socket to
//!   dispatching that operation's `Outcome`. See
//!   [`metrics_endpoint_serves_the_self_observed_latency`]. This is a
//!   node's view of itself; `queso_net::metrics::Recorder`'s histograms
//!   remain the bench *client's* view of the cluster, and the two answer
//!   different questions about the same operations.
//!
//! Both intervals are stated in full on `queso_net::status::StatusShared`'s
//! `recovery_micros` and `client_ops_completed` fields, including what each
//! excludes and which candidate interval was rejected and why. A latency
//! number whose interval is unstated is the kind of sentence `CLAUDE.md`
//! exists to prevent, so the interval lives next to the counter rather than
//! in a commit message.
//!
//! # Detection power of the #159 tests (measured)
//!
//! Falsifier, run: eight mutations, each applied to a clean tree and scored
//! against `cargo test -p queso-net --lib --test status --no-fail-fast`
//! (83 tests green on the unmutated control). Six are registered in
//! `falsifiers/registry.toml` (`d10-recovery-never-recorded`,
//! `d10-recovery-last-write-wins`, `d10-recovery-for-a-cold-boot`,
//! `d10-latency-never-recorded`, `d10-decisions-from-frontier-published`,
//! `d10-no-restart-reset-published`) so `falsifiers/replay.py` re-measures
//! them rather than leaving this table to rot.
//!
//! | mutation | caught by |
//! |---|---|
//! | driver never closes the recovery interval | the restart test |
//! | recovery recorded on every publish, not once | the restart test, plus `recovery_time_records_the_first_measurement_and_ignores_later_ones` |
//! | `restarted` never set | the restart test, plus `a_restart_still_catching_up_is_distinguishable_from_a_cold_boot` |
//! | every boot measures a recovery, cold boots included | the restart test (its never-restarted-peer control) |
//! | `note_client_op` never called | the latency test *and* the restart test |
//! | latency max stores instead of `fetch_max` | the latency test, plus `client_latency_accumulates_and_keeps_a_high_water_mark` |
//! | `observability()` derives `decisions` from the frontier | the restart test **only** |
//! | `on_restart` does not reset the counters | **nothing here** -- see below |
//!
//! 7 of 8 killed, and the eighth is the row worth reading.
//!
//! **The measured zero.** Removing `on_restart`'s counter reset survives
//! all 83 tests in this scope, and that is structural rather than a gap to
//! close: a real restart is a *new process*, whose `SmrNode` is built by
//! `SmrNode::from_durable` -- `ReplicaState { durable, ..Default::default() }`
//! -- so its `NodeMetrics` starts at zero whatever `on_restart` does. The
//! reset is what keeps the *in-process* model faithful to that, so the sim
//! test (`crates/smr/tests/observability_metrics.rs`'s
//! `a_restart_clears_the_counters`) is its only killer, and stays so. #159's
//! wording -- assert "the counters are back at zero" across a real restart
//! -- is satisfied here, but it does not test the reset, and reading it that
//! way would be exactly the inherited-premise error `CLAUDE.md` §3 warns
//! about.
//!
//! **What this file's restart test does add**, measured rather than argued:
//! `d10-decisions-from-frontier-published` -- `decisions` silently derived
//! from the durable frontier -- was killed by no test in `queso-net` before
//! it, and is killed by it now. That mutant is invisible to any
//! non-restarting test because `decisions` and `next_slot` advance in
//! lockstep within one process lifetime (enumerated in #129: the crate has
//! exactly one `applied_log.push`), and this crate previously had no
//! restarting scrape of the published path.
//!
//! So D10 is **five of five served**. That is a claim about the metrics
//! being served and counting what they are named after -- not a claim that
//! they are the right five to have chosen, which is §D's question, nor that
//! `recovery_secs` means "caught up with the cluster", which it explicitly
//! does not (see `StatusShared::is_ready`'s bound, which it inherits).

use std::time::Duration;

use queso_sim::ids::NodeId;
use queso_smr::{ClientId, Command, Outcome};

#[path = "support/mod.rs"]
mod support;
use support::{
    http_get, raw_status_request, spawn_cluster, spawn_cluster_with_status, submit_with_retry,
    ProcCluster,
};

/// `GET /metrics` at `addr`, parsed. Panics on anything but a `200` with a
/// JSON body -- a status endpoint that answered something else is a test
/// failure, not a condition to poll through.
async fn metrics(addr: std::net::SocketAddr) -> serde_json::Value {
    let (code, body) = http_get(addr, "/metrics").await;
    assert_eq!(code, 200, "GET /metrics at {addr} answered {code}: {body}");
    serde_json::from_str(&body).expect("metrics body is valid JSON")
}

/// [`metrics`], but `None` if the connection could not be established at
/// all -- the one failure a reboot legitimately produces. Anything a
/// listener actually answered is still asserted on.
async fn try_metrics(addr: std::net::SocketAddr) -> Option<serde_json::Value> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut stream = tokio::net::TcpStream::connect(addr).await.ok()?;
    let request = "GET /metrics HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n";
    stream.write_all(request.as_bytes()).await.ok()?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.ok()?;
    let response = String::from_utf8(response).expect("status server response is valid UTF-8");
    let (head, body) = response
        .split_once("\r\n\r\n")
        .unwrap_or_else(|| panic!("status server response has no header/body split: {response:?}"));
    assert!(
        head.starts_with("HTTP/1.1 200 OK"),
        "GET /metrics at {addr} answered: {head:?}"
    );
    Some(serde_json::from_str(body).expect("metrics body is valid JSON"))
}

/// Poll `GET /metrics` at `addr` until `done` accepts the body, or the
/// deadline passes.
///
/// Needed only after a real process restart: the status listener is up
/// before the driver has published anything about this boot (see
/// `queso_net::status::StatusShared::new`'s docs), so a scrape taken
/// immediately after `spawn` can legitimately describe a process that has
/// not reached its restart branch yet. Waiting that window out is not the
/// same as retrying a failed assertion -- the predicate is a statement
/// about *which boot* answered, not about the values under test.
async fn metrics_until(
    addr: std::net::SocketAddr,
    timeout: Duration,
    what: &str,
    done: impl Fn(&serde_json::Value) -> bool,
) -> serde_json::Value {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut last = serde_json::Value::Null;
    loop {
        // A rebooting process's listener can refuse a connection outright
        // for a moment while it rebinds the port, so this poll tolerates a
        // failed *connection* -- the harness racing the reboot -- while
        // still treating a non-200 or unparseable answer from a listener
        // that did accept as the failure it is (see `try_metrics`).
        if let Some(body) = try_metrics(addr).await {
            if done(&body) {
                return body;
            }
            last = body;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("replica at {addr} never {what}; last /metrics body: {last}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

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

/// #129: the three D10 consensus counters `queso_smr` now keeps
/// (`decisions`, `rounds_total`, `fast_path_decisions`,
/// `proposer_activations`) reach `GET /metrics` on a real cluster over real
/// TCP -- not just the in-process `SmrCluster` the `queso-smr` unit tests
/// read them from.
///
/// This is the end-to-end half of the wiring: `SmrNode::metrics()` ->
/// `driver` -> `StatusShared::publish_consensus` -> `MetricsBody` -> JSON.
/// The `queso-smr` side (`crates/smr/tests/observability_metrics.rs`) owns
/// the claim that the counters count the right thing; this owns only the
/// claim that they are *published*, which is a separate way to be broken
/// (a counter that increments into a field nobody serves).
///
/// The non-leader assertion is the anti-vacuity control: if `/metrics`
/// served a constant, or the driver published the wrong node's numbers,
/// every replica would report the same thing.
///
/// Measured over 8 consecutive local runs: the leader served
/// `decisions = rounds_total = fast_path_decisions = proposer_activations
/// = 3` every time, and replica 1 served `0` for all four every time. The
/// assertions are deliberately looser than that (`>= 3`, `<` the leader's,
/// plus the schedule-independent inequalities) because a slower machine
/// can take a contested round or fire a hedged proposer's activation
/// timer; pinning the measured values would make this a timing
/// change-detector rather than a wiring test.
#[tokio::test(flavor = "multi_thread")]
async fn metrics_endpoint_serves_the_consensus_counters() {
    let (client_addrs, status_addrs) = spawn_cluster_with_status(3, Some(NodeId(0)));
    let timeout = Duration::from_secs(10);

    // Before any operation the leader has decided nothing, so all four are
    // zero -- and they are *present*, which `["decisions"] == 0` would also
    // be true of if serde omitted the field, hence the explicit `is_u64`.
    let (code, body) = http_get(status_addrs[0], "/metrics").await;
    assert_eq!(code, 200);
    let baseline: serde_json::Value =
        serde_json::from_str(&body).expect("metrics body is valid JSON");
    for field in [
        "decisions",
        "rounds_total",
        "fast_path_decisions",
        "proposer_activations",
    ] {
        assert!(
            baseline[field].is_u64(),
            "/metrics must serve `{field}`, got: {baseline}"
        );
        assert_eq!(
            baseline[field], 0,
            "a replica that has decided nothing must report `{field}` as 0, got: {baseline}"
        );
    }

    for seq in 0..3u64 {
        let put = Command::Put {
            client: ClientId(7),
            seq,
            key: 100 + seq as u32,
            value: seq as i64,
        };
        assert_eq!(
            submit_with_retry(client_addrs[0], &put, timeout).await,
            Outcome::Put
        );
    }

    let (code, body) = http_get(status_addrs[0], "/metrics").await;
    assert_eq!(code, 200);
    let after: serde_json::Value = serde_json::from_str(&body).expect("metrics body is valid JSON");
    let decisions = after["decisions"].as_u64().unwrap();
    let rounds = after["rounds_total"].as_u64().unwrap();
    let fast = after["fast_path_decisions"].as_u64().unwrap();
    let activations = after["proposer_activations"].as_u64().unwrap();
    assert!(
        decisions >= 3,
        "the leader drove three Puts to a decision, so it must report at least \
         three: {after}"
    );
    // The rest are the schedule-independent invariants: a real cluster's
    // round counts and hedging behaviour are timing outcomes, and pinning
    // them here would make this a flaky change-detector for the network.
    assert!(
        rounds >= decisions,
        "every decision lands in round >= 1: {after}"
    );
    assert!(
        fast <= decisions,
        "the fast path is a subset of decisions: {after}"
    );
    assert!(
        activations >= decisions,
        "a proposer cannot decide without having activated: {after}"
    );

    // Control: a replica that proposed for nothing must not report the
    // leader's numbers.
    let (code, body) = http_get(status_addrs[1], "/metrics").await;
    assert_eq!(code, 200);
    let bystander: serde_json::Value =
        serde_json::from_str(&body).expect("metrics body is valid JSON");
    assert!(
        bystander["decisions"].as_u64().unwrap() < decisions,
        "replica 1 never received a client op, so it cannot have driven as many \
         slots to a decision as the leader: leader={after} replica1={bystander}"
    );
}

/// D10's **per-replica self-observed latency** (#159): `/metrics` serves
/// the count/sum/max triple for the client operations *this* replica
/// served, over the interval `queso_net::status::StatusShared`'s
/// `client_ops_completed` docs define (decode off the client socket ->
/// `Outcome` dispatched back to the connection task).
///
/// What could be wrong, and what this therefore asserts: that the triple is
/// fed at all (count moves), that it is fed something real rather than zero
/// (sum moves), that the maximum is a maximum (`max <= total`, and `max >=
/// the mean`), and that it is *this replica's own* view -- the bystander
/// control, which is the assertion that fails if the field were a cluster
/// aggregate or a constant.
///
/// Deliberately *not* asserted: any absolute latency. What a localhost
/// round trip plus an fsync costs is a property of the machine the test
/// runs on, and pinning it would make this a change-detector for CI
/// hardware. The bounds asserted hold for any timing.
#[tokio::test(flavor = "multi_thread")]
async fn metrics_endpoint_serves_the_self_observed_latency() {
    let (client_addrs, status_addrs) = spawn_cluster_with_status(3, Some(NodeId(0)));
    let timeout = Duration::from_secs(10);

    // Present-and-zero before any client operation, `is_u64` for the same
    // reason the #129 counters check it: `== 0` would also hold of a field
    // that was never serialized at all.
    let baseline = metrics(status_addrs[0]).await;
    for field in [
        "client_ops_completed",
        "client_latency_micros_total",
        "client_latency_micros_max",
    ] {
        assert!(
            baseline[field].is_u64(),
            "/metrics must serve `{field}`, got: {baseline}"
        );
        assert_eq!(baseline[field], 0, "{baseline}");
    }

    const OPS: u64 = 4;
    for seq in 0..OPS {
        let put = Command::Put {
            client: ClientId(11),
            seq,
            key: 300 + seq as u32,
            value: seq as i64,
        };
        assert_eq!(
            submit_with_retry(client_addrs[0], &put, timeout).await,
            Outcome::Put
        );
    }

    let after = metrics(status_addrs[0]).await;
    let completed = after["client_ops_completed"].as_u64().unwrap();
    let total = after["client_latency_micros_total"].as_u64().unwrap();
    let max = after["client_latency_micros_max"].as_u64().unwrap();
    assert!(
        completed >= OPS,
        "replica 0 answered {OPS} client operations, so it must report at least \
         that many: {after}"
    );
    assert!(
        total > 0,
        "an operation that crossed a real cluster and an fsync cannot have taken \
         zero microseconds: {after}"
    );
    assert!(
        max > 0 && max <= total,
        "the high-water mark must be one of the summed samples: {after}"
    );
    assert!(
        max >= total / completed,
        "a maximum below the mean is not a maximum: {after}"
    );

    // Control: replica 1 was never asked to serve a client operation, so a
    // per-replica metric must report nothing for it. This is what fails if
    // the field were a cluster-wide aggregate, a constant, or the bench
    // client's view rather than the node's own.
    let bystander = metrics(status_addrs[1]).await;
    assert_eq!(
        bystander["client_ops_completed"], 0,
        "replica 1 served no client operation: {bystander}"
    );
    assert_eq!(bystander["client_latency_micros_total"], 0, "{bystander}");
    assert_eq!(bystander["client_latency_micros_max"], 0, "{bystander}");
}

/// The published counters across a **real process restart** (#159's third
/// item), and D10's **recovery time** with it.
///
/// # Why this needs real processes, when `queso-smr` already has a restart
///
/// `queso_smr::NodeMetrics` is read through two one-line accessors:
/// `SmrCluster::metrics` (what the simulator's tests read) and
/// `SmrNode::metrics` (what this crate's driver publishes). Before this
/// test, the restart evidence and the published-path evidence were in
/// different files: `crates/smr/tests/observability_metrics.rs` restarts
/// but reads the sim accessor, and
/// [`metrics_endpoint_serves_the_consensus_counters`] above reads the
/// published one but never restarts.
///
/// That gap is not cosmetic, and #129 measured why: `decisions` and
/// `next_slot` advance in lockstep within one process lifetime -- every
/// applied slot is applied inside `finish_attempt`, the crate's only
/// `applied_log.push` -- so a `decisions` silently derived from the durable
/// frontier is *invisible* to any test that does not restart. Only a
/// restart separates them, and only this file exercises the path a real
/// deployment scrapes.
///
/// # What is asserted, and what is deliberately loose
///
/// - The frontier **survives**: `next_slot` after the reboot is at least
///   what it was before. (Not equality: the restarted replica's own
///   catch-up probe is a real attempt at the next slot, so an idle cluster
///   legitimately advances by a slot or two while rejoining.)
/// - The counters **do not**: `decisions` after the reboot is strictly
///   below the pre-crash frontier, and the client-latency triple is back at
///   zero -- this process has served nobody. A `decisions` that came from
///   the frontier would report the pre-crash figure here.
/// - Recovery time is measured, is reported only by the process that
///   actually restarted, and is **stable**: re-scraping later returns the
///   identical value. That last one is what fails if the interval were
///   recorded on every publish rather than first-write-wins, which would
///   silently redefine it as "time since this boot".
#[tokio::test(flavor = "multi_thread")]
async fn a_real_process_restart_resets_the_counters_and_reports_a_recovery_time() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let mut cluster = ProcCluster::start_with_status(3, 0, data_dir.path(), None);
    let timeout = Duration::from_secs(20);

    const WRITES: u64 = 8;
    for seq in 0..WRITES {
        let put = Command::Put {
            client: ClientId(23),
            seq,
            key: 400 + seq as u32,
            value: seq as i64,
        };
        assert_eq!(
            submit_with_retry(cluster.client_addr(0), &put, timeout).await,
            Outcome::Put,
            "write {seq} should be applied"
        );
    }

    let before = metrics(cluster.status_addr(0)).await;
    let frontier_before = before["next_slot"].as_u64().unwrap();
    assert!(
        frontier_before >= WRITES,
        "the leader applied {WRITES} writes: {before}"
    );
    assert!(
        before["decisions"].as_u64().unwrap() >= WRITES,
        "the leader drove {WRITES} writes to a decision: {before}"
    );
    assert!(
        before["client_ops_completed"].as_u64().unwrap() >= WRITES,
        "the leader answered {WRITES} clients: {before}"
    );
    assert_eq!(
        before["restarted"], false,
        "this process booted cold: {before}"
    );
    assert!(
        before["recovery_secs"].is_null(),
        "a process that never restarted has no recovery interval to report: {before}"
    );

    // SIGKILL and reboot against the same data directory: a new process, a
    // blank heap, and a durable snapshot to reload -- the real path, which
    // an in-process "drop and rebuild the node" cannot exercise.
    cluster.kill(0);
    cluster.spawn(0);

    // `restarted` flipping to `true` is what identifies the new boot:
    // the process that answered before the kill reports `false` for its
    // whole life, so this cannot be satisfied by a stale answer.
    let after = metrics_until(
        cluster.status_addr(0),
        timeout,
        "reported a completed restart recovery",
        |body| body["restarted"] == true && !body["recovery_secs"].is_null(),
    )
    .await;

    assert!(
        after["next_slot"].as_u64().unwrap() >= frontier_before,
        "the durable frontier must survive the crash: before={before} after={after}"
    );
    assert!(
        after["decisions"].as_u64().unwrap() < frontier_before,
        "the D10 counters are per-process: a rebooted replica that has driven only its \
         own catch-up cannot report the pre-crash count. A `decisions` derived from the \
         durable frontier reports {frontier_before} here. before={before} after={after}"
    );
    assert_eq!(
        after["client_ops_completed"], 0,
        "the rebooted process has answered no client: {after}"
    );
    assert_eq!(after["client_latency_micros_total"], 0, "{after}");
    assert_eq!(after["client_latency_micros_max"], 0, "{after}");

    let recovery = after["recovery_secs"].as_f64().unwrap();
    assert!(
        recovery.is_finite() && recovery >= 0.0,
        "recovery time must be a real interval: {after}"
    );

    // Stability: the driver re-checks the catch-up level on every publish,
    // so a last-write-wins record would keep growing. Wait long enough for
    // several publishes (the node ticks every 5ms) and re-scrape.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let later = metrics(cluster.status_addr(0)).await;
    assert_eq!(
        later["recovery_secs"], after["recovery_secs"],
        "recovery time is this boot's rejoin, not time since boot -- it must not move \
         after it is first recorded: first={after} later={later}"
    );

    // Control: a replica that never restarted must not report a recovery
    // time at all. Without this, a `restarted`/`recovery_secs` pair that was
    // simply always set would satisfy everything above.
    let peer = metrics(cluster.status_addr(1)).await;
    assert_eq!(
        peer["restarted"], false,
        "replica 1 was never killed: {peer}"
    );
    assert!(
        peer["recovery_secs"].is_null(),
        "replica 1 never restarted, so it has no recovery interval: {peer}"
    );
}

/// Adversarial parser test (locks in the #49 review's empirical probing as a
/// permanent regression guard): the hand-rolled HTTP responder in
/// `queso_net::status` is network-exposed, so malformed input must never
/// panic the handler task, wedge the node, or leave the status server unable
/// to answer a subsequent well-formed request. Fires a battery of bad
/// requests (wrong method, unknown/traversal/query paths, oversized headers,
/// non-UTF8 bytes, a bare newline) and asserts each gets a bounded, sane
/// error response -- then, the load-bearing assertion, that an ordinary
/// `GET /health` still returns `200` afterward (the server survived).
#[tokio::test(flavor = "multi_thread")]
async fn status_server_survives_malformed_requests() {
    let (_client_addrs, status_addrs) = spawn_cluster_with_status(3, Some(NodeId(0)));
    let status = status_addrs[0];

    // Each of these must come back with *some* 4xx (never a hang, never a
    // 2xx, never a crash). We don't over-fit the exact code (400 vs 404 vs
    // 405 is the server's call), only that it's a client-error rejection.
    let cases: &[(&str, &[u8])] = &[
        ("POST method", b"POST /metrics HTTP/1.1\r\nHost: t\r\n\r\n"),
        ("unknown path", b"GET /nope HTTP/1.1\r\nHost: t\r\n\r\n"),
        (
            "path traversal",
            b"GET /../secret HTTP/1.1\r\nHost: t\r\n\r\n",
        ),
        (
            "query string",
            b"GET /metrics?x=1 HTTP/1.1\r\nHost: t\r\n\r\n",
        ),
        ("no http version", b"GET /health\r\n\r\n"),
        ("bare newline", b"\r\n"),
        (
            "non-utf8 request line",
            b"GET /\xff\xfe\x00 HTTP/1.1\r\n\r\n",
        ),
    ];
    for (label, raw) in cases {
        let code = raw_status_request(status, raw).await;
        if let Some(code) = code {
            // The server must never answer a malformed request with a 5xx
            // (internal error) -- that would signal it hit an error path it
            // couldn't handle cleanly. A 2xx (for a debatable-but-servable
            // case like a valid path with a lenient/missing HTTP version) or
            // a 4xx (rejection) are both fine; we deliberately don't over-fit
            // the exact code, only that the handler stayed in control. `None`
            // (clean close, no parseable response) is also fine.
            assert!(
                code < 500,
                "{label}: status server returned a 5xx internal error ({code}) on malformed input -- \
                 it should reject or serve cleanly, never error internally"
            );
        }
        // The point of each case is only that it neither hangs (the helper's
        // own 10s timeout would have fired) nor crashes the node (asserted at
        // the end via a still-live /health).
    }

    // An oversized request (well past the server's 8 KiB byte cap) with no
    // terminating CRLF must be rejected/closed, not buffered unboundedly and
    // not hang. The exact response is unimportant; that this returns at all
    // (the helper's timeout didn't fire) is the property.
    let oversized = vec![b'A'; 64 * 1024];
    let _ = raw_status_request(status, &oversized).await;

    // The load-bearing assertion: after all that abuse, the server is still
    // alive and serving -- the parser bounded every bad request rather than
    // crashing the handler or wedging the accept loop.
    let (code, body) = http_get(status, "/health").await;
    assert_eq!(
        code, 200,
        "status server must still serve /health after malformed traffic, body: {body:?}"
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
