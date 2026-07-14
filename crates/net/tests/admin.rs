//! Phase 8.2d's (issue #47) acceptance test: `queso_net::admin`, the
//! reusable logic behind the `queso-admin` operator CLI -- against real,
//! in-process, real-TCP clusters (the same `tests/support/mod.rs` harness
//! `tests/status.rs`/`tests/cluster.rs` use), not a mocked HTTP server or a
//! spawned binary. `queso-admin`'s own `src/bin/queso-admin.rs` is a thin
//! `clap` wrapper over exactly the functions exercised here.

use std::net::SocketAddr;
use std::time::Duration;

use queso_net::admin::{self, DEFAULT_ADMIN_CLIENT_ID};
use queso_net::client::Client;
use queso_sim::ids::NodeId;
use queso_smr::{ClientId, Command, Outcome};

#[path = "support/mod.rs"]
mod support;
use support::{free_addr, spawn_cluster, spawn_cluster_with_status, submit_with_retry};

/// `status` against a healthy 3-node cluster: every replica reachable and
/// ready, and -- once a write has been driven through and had a moment to
/// propagate -- every replica's log frontier (`next_slot`) agrees, i.e. no
/// replica is reported lagging. Uses a small bounded poll loop (not a fixed
/// sleep) since Meerkat's leaderless-tolerant hedging means the exact
/// millisecond every replica converges is not deterministic, only that it
/// happens quickly.
///
/// A replica only advances its own `next_slot` when it actively attempts
/// (or catches up on) a slot itself -- see `queso_smr::replica`'s module
/// docs ("different replicas' `next_slot` values can and do diverge"), so a
/// single `Put` submitted through only one replica leaves the other two
/// legitimately behind until they are asked to do something. This test
/// submits a follow-up `Get` directly to each of the other two replicas
/// (each discovers the already-decided `Put` at slot 0 while attempting its
/// own next operation, applies it, and catches its `next_slot` up) --
/// exactly the read-your-writes-from-any-replica pattern
/// `tests/cluster.rs` already exercises -- so that convergence is a
/// realistic property of a lightly-used healthy cluster, not a wish.
#[tokio::test(flavor = "multi_thread")]
async fn status_reports_a_healthy_agreeing_cluster_after_a_write() {
    let (client_addrs, status_addrs) = spawn_cluster_with_status(3, Some(NodeId(0)));
    let fetch_timeout = Duration::from_secs(2);

    let put = Command::Put {
        client: ClientId(1),
        seq: 0,
        key: 42,
        value: 7,
    };
    let outcome = submit_with_retry(client_addrs[0], &put, Duration::from_secs(10)).await;
    assert_eq!(outcome, Outcome::Put);

    // Touch every replica (including the one the Put itself went through)
    // directly with the exact same `(client, seq, key)` Get so each one
    // catches its own frontier up to slot 0's decided Put, then to slot 1's
    // decided Get -- content-identical resubmission (see
    // `queso_smr::kv`'s P8a docs) means each replica either decides that
    // Get itself or discovers it was already decided elsewhere with
    // matching content, either way advancing its own frontier without
    // needing distinct, separately-negotiated slots per replica.
    let get = Command::Get {
        client: ClientId(1),
        seq: 1,
        key: 42,
    };
    for &addr in &client_addrs {
        let outcome = submit_with_retry(addr, &get, Duration::from_secs(10)).await;
        assert_eq!(outcome, Outcome::Get(Some(7)));
    }

    // Poll status until every replica's frontier agrees (or a generous
    // deadline elapses) -- propagation to the two non-leader replicas isn't
    // instantaneous, but should complete well within this bound in a
    // healthy, unloaded local cluster.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let (statuses, summary) = loop {
        let statuses = admin::fetch_cluster_status(&status_addrs, fetch_timeout).await;
        let summary = admin::summarize(&statuses);
        if summary.reachable == 3 && summary.all_ready && summary.lagging.is_empty() {
            break (statuses, summary);
        }
        if tokio::time::Instant::now() >= deadline {
            panic!(
                "cluster never converged to a fully healthy, agreeing status: {:#?} / {:#?}",
                statuses, summary
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    };

    assert_eq!(summary.total, 3);
    assert_eq!(summary.reachable, 3);
    assert!(summary.all_ready);
    assert!(summary.lagging.is_empty());
    let max_next_slot = summary
        .max_next_slot
        .expect("some replica must be reachable");
    assert!(
        max_next_slot >= 1,
        "expected the decided Put to have advanced next_slot to at least 1, got {max_next_slot}"
    );
    for status in &statuses {
        assert!(
            status.reachable(),
            "replica {} should be reachable",
            status.index
        );
        assert!(status.ready(), "replica {} should be ready", status.index);
        assert_eq!(
            status.next_slot(),
            Some(max_next_slot),
            "replica {} should agree with the cluster's frontier",
            status.index
        );
    }

    // The rendered table is what an operator actually reads -- assert it
    // carries the load-bearing facts, not just the structured summary.
    let table = admin::render_status_table(&statuses, &summary);
    assert!(table.contains("cluster: 3/3 replicas reachable"));
    assert!(table.contains("all_ready=true"));
    assert!(table.contains(&format!("frontier: agrees at next_slot={max_next_slot}")));
}

/// One replica down (an address nothing is listening on, never booted) must
/// not crash, hang, or otherwise abort `status` for the rest of the
/// cluster: the down replica is reported unreachable, the two live replicas
/// are still reported healthy, and the whole call returns well inside a
/// bounded time (bounded by admin's own per-replica fetch timeout, not the
/// dead replica's TCP connect ever completing).
#[tokio::test(flavor = "multi_thread")]
async fn status_reports_a_down_replica_as_unreachable_without_hanging_the_command() {
    let (client_addrs, status_addrs) = spawn_cluster_with_status(3, Some(NodeId(0)));

    // Drive a write so the two live replicas have something to agree on,
    // then swap replica 2's real status address out for a dead one --
    // nothing is listening there (bind-then-drop, same pattern this
    // crate's own tests already use for "an address that is free right
    // now"), simulating "this replica's process is down" without ever
    // booting a node against it.
    let put = Command::Put {
        client: ClientId(1),
        seq: 0,
        key: 1,
        value: 100,
    };
    let outcome = submit_with_retry(client_addrs[0], &put, Duration::from_secs(10)).await;
    assert_eq!(outcome, Outcome::Put);

    let dead_addr: SocketAddr = free_addr();
    let mixed_addrs = vec![status_addrs[0], status_addrs[1], dead_addr];

    let fetch_timeout = Duration::from_millis(500);
    let start = tokio::time::Instant::now();
    let statuses = admin::fetch_cluster_status(&mixed_addrs, fetch_timeout).await;
    let elapsed = start.elapsed();
    // Bounded: even though nothing answers on `dead_addr`, the whole call
    // must complete close to one `fetch_timeout`, not hang indefinitely --
    // generous slack for CI scheduling jitter.
    assert!(
        elapsed < fetch_timeout * 4,
        "fetch_cluster_status took {elapsed:?} against one dead address -- looks like it hung \
         rather than timing out per-replica"
    );

    assert_eq!(statuses.len(), 3);
    assert!(statuses[0].reachable(), "replica 0 should be reachable");
    assert!(statuses[1].reachable(), "replica 1 should be reachable");
    assert!(
        !statuses[2].reachable(),
        "the dead address should be reported unreachable, not crash the command"
    );
    assert!(
        statuses[2].error.is_some(),
        "an unreachable replica should carry a human-readable reason"
    );

    let summary = admin::summarize(&statuses);
    assert_eq!(summary.total, 3);
    assert_eq!(
        summary.reachable, 2,
        "the majority should still be reported healthy"
    );
    assert!(
        summary.all_ready,
        "the two live, ready replicas should make all_ready true regardless of the dead third"
    );

    // Rendering must not panic on the mixed reachable/unreachable input,
    // and must surface both facts legibly.
    let table = admin::render_status_table(&statuses, &summary);
    assert!(table.contains("cluster: 2/3 replicas reachable"));
    assert!(table.contains("unreachable:"));
}

/// `put` then `get` round-trip a value via `queso_net::admin`'s
/// `Client`-backed path -- the same path `queso-admin put`/`get`'s binary
/// wrapper drives.
#[tokio::test(flavor = "multi_thread")]
async fn admin_put_then_get_round_trips_a_value() {
    let client_addrs = spawn_cluster(3, Some(NodeId(0)));
    let client = Client::new(client_addrs);

    let put_outcome = admin::put(&client, DEFAULT_ADMIN_CLIENT_ID, 0, 55, 999)
        .await
        .expect("admin put should succeed");
    assert_eq!(put_outcome, Outcome::Put);

    let get_outcome = admin::get(&client, DEFAULT_ADMIN_CLIENT_ID, 1, 55)
        .await
        .expect("admin get should succeed");
    assert_eq!(get_outcome, Outcome::Get(Some(999)));

    // A second admin Put with a strictly greater seq to the same key must
    // still apply (not be deduplicated as a stale retry) -- proves the
    // admin ClientId's own dedup space behaves like any other client's,
    // just reserved by convention (see `queso_net::admin`'s module docs).
    let overwrite_outcome = admin::put(&client, DEFAULT_ADMIN_CLIENT_ID, 2, 55, 1000)
        .await
        .expect("second admin put should succeed");
    assert_eq!(overwrite_outcome, Outcome::Put);
    let get_after_overwrite = admin::get(&client, DEFAULT_ADMIN_CLIENT_ID, 3, 55)
        .await
        .expect("admin get after overwrite should succeed");
    assert_eq!(get_after_overwrite, Outcome::Get(Some(1000)));
}

/// The admin `ClientId` (`DEFAULT_ADMIN_CLIENT_ID`, `u32::MAX`) must not
/// interfere with an ordinary application client's own `(ClientId, seq)`
/// dedup space (A6) -- both submit against the same cluster, on different
/// keys, and both must see exactly what they wrote, unaffected by the
/// other's operations.
#[tokio::test(flavor = "multi_thread")]
async fn admin_client_id_does_not_collide_with_an_ordinary_clients_ops() {
    let client_addrs = spawn_cluster(3, Some(NodeId(0)));
    let admin_client = Client::new(client_addrs.clone());

    // The admin tool writes key 1.
    let admin_put = admin::put(&admin_client, DEFAULT_ADMIN_CLIENT_ID, 0, 1, 111)
        .await
        .expect("admin put should succeed");
    assert_eq!(admin_put, Outcome::Put);

    // An ordinary application client (a low, ordinary ClientId -- exactly
    // the kind of id `queso-bench`'s sessions use) independently writes a
    // different key on the same cluster.
    let ordinary_put = Command::Put {
        client: ClientId(0),
        seq: 0,
        key: 2,
        value: 222,
    };
    let ordinary_outcome =
        submit_with_retry(client_addrs[0], &ordinary_put, Duration::from_secs(10)).await;
    assert_eq!(ordinary_outcome, Outcome::Put);

    // Each side reads back the *other's* write correctly -- proving they
    // share one consistent log/KV state and neither's ClientId/seq
    // bookkeeping stomped on the other's.
    let admin_reads_ordinary_key = admin::get(&admin_client, DEFAULT_ADMIN_CLIENT_ID, 1, 2)
        .await
        .expect("admin get should succeed");
    assert_eq!(admin_reads_ordinary_key, Outcome::Get(Some(222)));

    let ordinary_get = Command::Get {
        client: ClientId(0),
        seq: 1,
        key: 1,
    };
    let ordinary_reads_admin_key =
        submit_with_retry(client_addrs[1], &ordinary_get, Duration::from_secs(10)).await;
    assert_eq!(ordinary_reads_admin_key, Outcome::Get(Some(111)));
}
