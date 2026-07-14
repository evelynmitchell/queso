//! Phase 8.2d (issue #47): an out-of-cluster operator toolkit --
//! `queso-admin`'s reusable logic, kept in this library module (not the
//! `src/bin/queso-admin.rs` binary) so it is unit/integration-testable
//! in-process, the same split this crate already uses for
//! `client`/`status` vs. `bin/queso-node.rs`/`bin/queso-bench.rs`.
//!
//! Two things an operator standing outside a running cluster needs:
//!
//! 1. **`status`** ([`fetch_cluster_status`]/[`summarize`]/
//!    [`render_status_table`]): poll every replica's `GET /metrics`
//!    (`crate::status`) and render an "is my cluster healthy and caught
//!    up?" table -- reachability, readiness, and whether every replica's
//!    log frontier (`next_slot`) agrees or some are lagging behind the
//!    others. Dependency-light on purpose: [`fetch_metrics`] hand-rolls the
//!    HTTP GET the same way `tests/support/mod.rs::http_get` does, no
//!    `reqwest`/`hyper` -- see that helper's docs and `crate::status`'s
//!    module docs for the wire format this parses.
//! 2. **`put`/`get`** ([`put`]/[`get`]): submit an actual `queso_smr::Command`
//!    against the cluster's *client* ports via `crate::client::Client` --
//!    the same pooled, retry-to-another-replica path `queso-bench` uses,
//!    optionally over TLS (`crate::client::ClientConfig::tls`).
//!
//! # What this module deliberately does *not* add
//!
//! No "trigger catch-up" RPC: the node has no such admin endpoint, and it
//! doesn't need one -- a replica that fell behind self-heals via its own
//! restart/quiescence catch-up watchdog (`queso_smr::SmrNode`,
//! `crate::status`'s `/ready` docs). An operator's only lever here is
//! *observing* that fact (a lagging `next_slot` in `status`'s table), not
//! forcing it.
//!
//! # The admin `ClientId` (A6)
//!
//! Every `Command` is tagged `(ClientId, seq)` for idempotent dedup (A6,
//! `queso_smr::command::ClientSession`'s docs) -- a real application's
//! clients own that dedup space, so an admin tool poking the same cluster
//! for a one-off `Put`/`Get` must not collide with it. [`DEFAULT_ADMIN_CLIENT_ID`]
//! (`ClientId(u32::MAX - 1)`) is `queso-admin`'s documented default: a very
//! high id, which no ordinary application is expected to hand out
//! (`queso-bench`'s own session ids start at `0` and count up from there --
//! see `src/bin/queso-bench.rs`'s `Session::new`). Note this is
//! deliberately **not** `u32::MAX` itself: `queso_smr::replica` reserves
//! that exact id (`CATCH_UP_CLIENT`, private to that crate) for its own
//! internal restart catch-up probes, and asserts no real submission ever
//! uses it -- a `queso-admin` build that picked `u32::MAX` would panic every
//! replica it talked to the moment it submitted anything (this was caught
//! by this crate's own `tests/admin.rs`, not by inspection). `u32::MAX - 1`
//! is the next-highest id, unreserved by anything in this workspace. Either
//! way, this is *only* a convention, not an enforced reservation -- nothing
//! stops an application from also using it -- so `queso-admin put`/`get`
//! also accept an explicit `--client-id` override for an operator who knows
//! their application's id space needs a different reservation.
//!
//! # The admin `seq` (honest limitation)
//!
//! `queso-admin` is a fresh, one-shot process per invocation -- unlike
//! `queso-bench`'s long-lived `Session` (`src/bin/queso-bench.rs`), it has
//! no persisted, monotonically-advancing counter to draw a guaranteed-fresh
//! `seq` from across separate invocations. [`default_seq`] falls back to
//! the current wall-clock time in milliseconds since the Unix epoch: real
//! time only moves forward, so consecutive manual invocations get strictly
//! increasing `seq`s in practice (this crate is already the workspace's
//! real-I/O boundary -- see `src/lib.rs`'s docs -- so this is not a
//! determinism-lint violation the way it would be in `queso-sim`/
//! `queso-consensus`/`queso-smr`). This is a *best-effort* default, not a
//! guarantee: two invocations within the same millisecond, or a system
//! clock that jumps backward, could in principle collide or go backwards.
//! A caller that needs guaranteed-fresh `seq`s (e.g. a script issuing many
//! admin `Put`s in a tight loop) should pass `--seq` explicitly rather than
//! rely on the default.

use std::fmt::Write as _;
use std::net::SocketAddr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use queso_smr::{ClientId, Command, Key, Outcome, Value};
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::client::Client;

/// `queso-admin`'s documented default `ClientId` for `put`/`get` -- see the
/// module docs' "The admin `ClientId`" section.
pub const DEFAULT_ADMIN_CLIENT_ID: ClientId = ClientId(u32::MAX - 1);

/// Default per-replica timeout [`fetch_metrics`]/[`fetch_cluster_status`]
/// use when the caller doesn't override it -- generous enough for a
/// healthy local or WAN replica to answer, short enough that one down
/// replica (nothing listening, or a black-holed connection) doesn't stall
/// `status`'s overall runtime by more than this.
pub const DEFAULT_STATUS_TIMEOUT: Duration = Duration::from_secs(3);

/// The subset of `crate::status`'s `/metrics` JSON body [`fetch_metrics`]
/// parses. Deliberately a separate type from `crate::status`'s private
/// `MetricsBody` (that module has no public serialization type to reuse) --
/// this is purely a client-side deserialization shim over the same wire
/// shape documented in `crate::status`'s module docs.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct AdminMetrics {
    pub events_processed: u64,
    pub next_slot: u64,
    pub save_count: u64,
    pub ready: bool,
    pub uptime_secs: f64,
}

/// One replica's [`fetch_metrics`] outcome, keyed by its position in the
/// caller's `--status-addr` list (`index`) and its status-server address.
/// `metrics.is_none()` means this replica was unreachable or answered
/// something [`fetch_metrics`] couldn't parse -- see `error` for why --
/// never a panic: a down/malformed replica is reported, not fatal to the
/// rest of the `status` command (see [`fetch_cluster_status`]'s docs).
#[derive(Debug, Clone)]
pub struct ReplicaStatus {
    pub index: usize,
    pub addr: SocketAddr,
    pub metrics: Option<AdminMetrics>,
    pub error: Option<String>,
}

impl ReplicaStatus {
    /// Whether this replica answered `GET /metrics` at all within the
    /// configured timeout.
    pub fn reachable(&self) -> bool {
        self.metrics.is_some()
    }

    /// This replica's `ready` bit, or `false` for an unreachable replica --
    /// an operator reading "is the cluster ready?" should never have to
    /// special-case "couldn't tell" separately from "not ready".
    pub fn ready(&self) -> bool {
        self.metrics.as_ref().is_some_and(|m| m.ready)
    }

    /// This replica's log frontier, if reachable.
    pub fn next_slot(&self) -> Option<u64> {
        self.metrics.as_ref().map(|m| m.next_slot)
    }
}

/// Issue one plain-HTTP `GET path` against `addr` over a bare
/// `tokio::net::TcpStream` and return `(status_code, body)` -- the status
/// port is always plaintext (see `crate::status`'s module docs), so this
/// never touches TLS. Hand-rolled in the same style as
/// `tests/support/mod.rs::http_get` (this crate's own test harness) rather
/// than pulling in `reqwest`/`hyper`, per this crate's dependency-light
/// philosophy (`src/lib.rs`'s docs). `timeout` bounds the whole
/// connect+write+read+parse sequence, so a replica that never answers (down,
/// or a black-holed connection) fails fast instead of hanging this call --
/// and, by extension, [`fetch_cluster_status`] -- forever. Shared by
/// [`fetch_metrics`] (`/metrics`) and `queso-admin health`'s
/// `/health`/`/ready` probes.
pub async fn http_get(
    addr: SocketAddr,
    path: &str,
    timeout: Duration,
) -> anyhow::Result<(u16, String)> {
    let attempt = async {
        let mut stream = TcpStream::connect(addr)
            .await
            .with_context(|| format!("connect to status server at {addr}"))?;
        let request =
            format!("GET {path} HTTP/1.1\r\nHost: queso-admin\r\nConnection: close\r\n\r\n");
        stream
            .write_all(request.as_bytes())
            .await
            .with_context(|| format!("write GET {path} request"))?;

        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .context("read status server response")?;
        anyhow::Ok(response)
    };

    let response = tokio::time::timeout(timeout, attempt)
        .await
        .map_err(|_| anyhow::anyhow!("timed out after {timeout:?} contacting {addr}"))??;

    let response = String::from_utf8(response).context("status server response was not UTF-8")?;
    let mut parts = response.splitn(2, "\r\n\r\n");
    let head = parts.next().unwrap_or_default();
    let body = parts.next().unwrap_or_default().to_string();

    let status_code = head
        .lines()
        .next()
        .and_then(|status_line| status_line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "could not parse a status code out of response head from {addr}: {head:?}"
            )
        })?;

    Ok((status_code, body))
}

/// Issue `GET /metrics` against `addr` and parse the JSON body. See
/// [`http_get`] for the transport-level details.
pub async fn fetch_metrics(addr: SocketAddr, timeout: Duration) -> anyhow::Result<AdminMetrics> {
    let (status_code, body) = http_get(addr, "/metrics", timeout).await?;
    anyhow::ensure!(
        status_code == 200,
        "GET /metrics against {addr} returned HTTP {status_code} (expected 200): {body:?}"
    );
    serde_json::from_str(&body)
        .with_context(|| format!("parsing /metrics JSON from {addr}: {body:?}"))
}

/// Like [`fetch_metrics`], but never returns an `Err` -- any failure
/// (connect refused, timeout, malformed response) is captured in
/// [`ReplicaStatus::error`] instead, so one down or misbehaving replica
/// never aborts the whole `status` command (see the module docs' "flagship
/// feature" framing and [`fetch_cluster_status`]).
pub async fn fetch_status(index: usize, addr: SocketAddr, timeout: Duration) -> ReplicaStatus {
    match fetch_metrics(addr, timeout).await {
        Ok(metrics) => ReplicaStatus {
            index,
            addr,
            metrics: Some(metrics),
            error: None,
        },
        Err(err) => ReplicaStatus {
            index,
            addr,
            metrics: None,
            error: Some(err.to_string()),
        },
    }
}

/// Fetch every replica in `addrs` (index order preserved in the returned
/// `Vec`) concurrently -- one down or slow replica does not delay the
/// others, and [`fetch_status`]'s per-call timeout bounds the whole call's
/// wall-clock time to roughly `timeout`, not `timeout * addrs.len()`.
pub async fn fetch_cluster_status(addrs: &[SocketAddr], timeout: Duration) -> Vec<ReplicaStatus> {
    let futures = addrs
        .iter()
        .enumerate()
        .map(|(index, &addr)| fetch_status(index, addr, timeout));
    futures_util::future::join_all(futures).await
}

/// Cluster-wide health rollup computed from a [`fetch_cluster_status`]
/// result -- the "is my cluster healthy and caught up?" answer `status`
/// exists to give an operator without them having to eyeball every row.
#[derive(Debug, Clone, PartialEq)]
pub struct ClusterSummary {
    /// How many addresses `status` was asked about.
    pub total: usize,
    /// How many of them answered `GET /metrics` within the timeout.
    pub reachable: usize,
    /// `true` iff at least one replica is reachable and every reachable
    /// replica's `ready` bit is `true`. An all-unreachable cluster is
    /// reported as *not* all-ready -- "couldn't tell" should never render
    /// as "healthy".
    pub all_ready: bool,
    /// The highest `next_slot` seen among reachable replicas -- the
    /// cluster's furthest-known-forward log frontier. `None` iff no
    /// replica was reachable at all.
    pub max_next_slot: Option<u64>,
    /// Indices (matching [`ReplicaStatus::index`]) of every reachable
    /// replica whose `next_slot` is strictly behind `max_next_slot` -- i.e.
    /// a replica that has decided fewer log entries than the furthest-ahead
    /// replica this call could see. Empty iff every reachable replica's
    /// frontier agrees (including the trivial case of zero or one
    /// reachable replica).
    pub lagging: Vec<usize>,
}

/// Roll [`fetch_cluster_status`]'s per-replica results up into a
/// [`ClusterSummary`]. Pure/sync -- no I/O -- so it's trivially unit
/// testable against hand-built [`ReplicaStatus`] fixtures, independent of a
/// real cluster.
pub fn summarize(statuses: &[ReplicaStatus]) -> ClusterSummary {
    let total = statuses.len();
    let reachable_statuses: Vec<&ReplicaStatus> =
        statuses.iter().filter(|s| s.reachable()).collect();
    let reachable = reachable_statuses.len();
    let all_ready = reachable > 0 && reachable_statuses.iter().all(|s| s.ready());
    let max_next_slot = reachable_statuses
        .iter()
        .filter_map(|s| s.next_slot())
        .max();
    let lagging = match max_next_slot {
        Some(max) => reachable_statuses
            .iter()
            .filter(|s| s.next_slot().is_some_and(|n| n < max))
            .map(|s| s.index)
            .collect(),
        None => Vec::new(),
    };
    ClusterSummary {
        total,
        reachable,
        all_ready,
        max_next_slot,
        lagging,
    }
}

/// Render `statuses`/`summary` as a human-readable table + rollup, exactly
/// what `queso-admin status` prints to stdout. Pure/sync and returns a
/// `String` (rather than writing to stdout itself) so tests can assert on
/// its content without capturing process output.
pub fn render_status_table(statuses: &[ReplicaStatus], summary: &ClusterSummary) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "{:<5} {:<28} {:<10} {:<7} {:<10} {:<10} {:<10}",
        "index", "address", "reachable", "ready", "next_slot", "save_cnt", "uptime_s"
    );
    for s in statuses {
        match &s.metrics {
            Some(m) => {
                let lag_marker = if summary.lagging.contains(&s.index) {
                    " (lagging)"
                } else {
                    ""
                };
                let _ = writeln!(
                    out,
                    "{:<5} {:<28} {:<10} {:<7} {:<10} {:<10} {:<10.1}{lag_marker}",
                    s.index, s.addr, "yes", m.ready, m.next_slot, m.save_count, m.uptime_secs
                );
            }
            None => {
                let reason = s.error.as_deref().unwrap_or("unknown error");
                let _ = writeln!(
                    out,
                    "{:<5} {:<28} {:<10} {:<7} {:<10} {:<10} {:<10}  unreachable: {reason}",
                    s.index, s.addr, "no", "-", "-", "-", "-"
                );
            }
        }
    }
    out.push('\n');
    let _ = writeln!(
        out,
        "cluster: {}/{} replicas reachable, all_ready={}",
        summary.reachable, summary.total, summary.all_ready
    );
    match summary.max_next_slot {
        Some(max) if summary.lagging.is_empty() => {
            let _ = writeln!(out, "frontier: agrees at next_slot={max}");
        }
        Some(max) => {
            let _ = writeln!(
                out,
                "frontier: max next_slot={max}; lagging replica indices (behind max): {:?}",
                summary.lagging
            );
        }
        None => {
            let _ = writeln!(out, "frontier: no reachable replica to compare");
        }
    }
    out
}

/// Best-effort default `seq` for an admin `put`/`get` -- see the module
/// docs' "The admin `seq`" section for why this is wall-clock-derived and
/// what its limits are.
pub fn default_seq() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Submit `Command::Put { client: client_id, seq, key, value }` via `client`
/// (`crate::client::Client` -- pooled addresses, retry-to-another-replica,
/// optional TLS per `client`'s own `ClientConfig`). Thin on purpose: exists
/// so `queso-admin put`'s binary wrapper stays a thin CLI shim over
/// something this crate's own tests can call in-process.
pub async fn put(
    client: &Client,
    client_id: ClientId,
    seq: u64,
    key: Key,
    value: Value,
) -> anyhow::Result<Outcome> {
    let command = Command::Put {
        client: client_id,
        seq,
        key,
        value,
    };
    client.submit(&command).await
}

/// Submit `Command::Get { client: client_id, seq, key }` via `client`. See
/// [`put`]'s docs.
pub async fn get(
    client: &Client,
    client_id: ClientId,
    seq: u64,
    key: Key,
) -> anyhow::Result<Outcome> {
    let command = Command::Get {
        client: client_id,
        seq,
        key,
    };
    client.submit(&command).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], port))
    }

    fn reachable(index: usize, next_slot: u64, ready: bool) -> ReplicaStatus {
        ReplicaStatus {
            index,
            addr: addr(9000 + index as u16),
            metrics: Some(AdminMetrics {
                events_processed: next_slot,
                next_slot,
                save_count: next_slot,
                ready,
                uptime_secs: 1.0,
            }),
            error: None,
        }
    }

    fn unreachable(index: usize) -> ReplicaStatus {
        ReplicaStatus {
            index,
            addr: addr(9000 + index as u16),
            metrics: None,
            error: Some("connection refused".to_string()),
        }
    }

    #[test]
    fn summarize_all_reachable_and_agreeing_is_fully_healthy() {
        let statuses = vec![
            reachable(0, 5, true),
            reachable(1, 5, true),
            reachable(2, 5, true),
        ];
        let summary = summarize(&statuses);
        assert_eq!(summary.total, 3);
        assert_eq!(summary.reachable, 3);
        assert!(summary.all_ready);
        assert_eq!(summary.max_next_slot, Some(5));
        assert!(summary.lagging.is_empty());
    }

    #[test]
    fn summarize_detects_a_lagging_reachable_replica() {
        let statuses = vec![
            reachable(0, 10, true),
            reachable(1, 10, true),
            reachable(2, 3, true),
        ];
        let summary = summarize(&statuses);
        assert_eq!(summary.max_next_slot, Some(10));
        assert_eq!(summary.lagging, vec![2]);
        assert!(
            summary.all_ready,
            "readiness is independent of frontier lag"
        );
    }

    #[test]
    fn summarize_one_down_replica_is_reported_but_majority_still_healthy() {
        let statuses = vec![reachable(0, 5, true), reachable(1, 5, true), unreachable(2)];
        let summary = summarize(&statuses);
        assert_eq!(summary.total, 3);
        assert_eq!(summary.reachable, 2);
        assert!(
            summary.all_ready,
            "unreachable replicas must not count against all_ready"
        );
        assert_eq!(summary.max_next_slot, Some(5));
        assert!(
            summary.lagging.is_empty(),
            "an unreachable replica is not \"lagging\" -- it's a separate, reported condition"
        );
    }

    #[test]
    fn summarize_all_unreachable_is_not_ready_and_has_no_frontier() {
        let statuses = vec![unreachable(0), unreachable(1), unreachable(2)];
        let summary = summarize(&statuses);
        assert_eq!(summary.reachable, 0);
        assert!(!summary.all_ready);
        assert_eq!(summary.max_next_slot, None);
        assert!(summary.lagging.is_empty());
    }

    #[test]
    fn summarize_not_ready_replica_makes_all_ready_false() {
        let statuses = vec![reachable(0, 5, true), reachable(1, 5, false)];
        let summary = summarize(&statuses);
        assert!(!summary.all_ready);
    }

    #[test]
    fn render_status_table_includes_every_replica_and_the_rollup_line() {
        let statuses = vec![reachable(0, 5, true), unreachable(1)];
        let summary = summarize(&statuses);
        let table = render_status_table(&statuses, &summary);
        assert!(table.contains("127.0.0.1:9000"));
        assert!(table.contains("127.0.0.1:9001"));
        assert!(table.contains("unreachable: connection refused"));
        assert!(table.contains("cluster: 1/2 replicas reachable"));
    }

    #[test]
    fn render_status_table_marks_a_lagging_replica() {
        let statuses = vec![reachable(0, 10, true), reachable(1, 2, true)];
        let summary = summarize(&statuses);
        let table = render_status_table(&statuses, &summary);
        assert!(table.contains("(lagging)"));
        assert!(table.contains("lagging replica indices"));
    }

    #[test]
    fn default_admin_client_id_is_the_documented_reserved_id() {
        assert_eq!(DEFAULT_ADMIN_CLIENT_ID, ClientId(u32::MAX - 1));
    }

    #[test]
    fn default_seq_is_nonzero_and_roughly_monotonic_across_two_calls() {
        let a = default_seq();
        let b = default_seq();
        assert!(a > 0);
        assert!(
            b >= a,
            "wall-clock time must not appear to go backwards within one test"
        );
    }
}
