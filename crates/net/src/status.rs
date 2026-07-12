//! Phase 8.2 (issue #47): a lightweight, opt-in, off-by-default HTTP
//! status/metrics server for one replica -- `GET /health` (liveness),
//! `GET /ready` (readiness), `GET /metrics` (a small JSON counters
//! document). Deliberately dependency-light: no `hyper`/`axum`/`warp`, just
//! a hand-rolled HTTP/1.1 GET responder over a plain `tokio::net::TcpListener`,
//! in the same spirit as `crate::client`'s hand-rolled client protocol and
//! `crate::transport`'s hand-rolled peer wire framing.
//!
//! # Endpoints and their precise, honest semantics
//!
//! - **`GET /health`** -- liveness. Always `200 OK` as long as this
//!   handler task is scheduled at all, i.e. the process is up and its tokio
//!   runtime is alive. This says nothing about consensus progress -- it is
//!   a process-up check only, exactly what a liveness probe should be (a
//!   liveness probe that also checked application-level readiness would
//!   make an orchestrator kill and restart a replica that is merely
//!   catching up, which is the opposite of helpful).
//! - **`GET /ready`** -- readiness: `200 OK` if [`StatusShared::is_ready`]
//!   is currently `true`, `503 Service Unavailable` otherwise. See that
//!   method's docs for exactly what "ready" means here and, just as
//!   importantly, what it does *not* claim.
//! - **`GET /metrics`** -- a small pretty-printed JSON document (see
//!   [`StatusShared::metrics_json`]) of counters this replica actually
//!   tracks: total events dispatched, current log frontier (`next_slot`),
//!   real fsync'd-save count, the same `ready` bool `/ready` reports, and
//!   uptime. Every number here is a plain, already-tracked counter --
//!   nothing is estimated or invented for this endpoint.
//!
//! Only `GET` is served, only these three paths; anything else (wrong
//! method, unknown path, or a request this parser can't make sense of) gets
//! a `4xx` and the connection is closed. See [`handle_connection`]'s docs
//! for the defensive bounds (capped read, timeout) that keep a malformed or
//! slow-loris-style request from costing this replica anything beyond one
//! bounded-lifetime task.
//!
//! # How the driver publishes status without moving `SmrNode` off-thread
//!
//! [`StatusShared`] is the `Send + Sync` snapshot
//! `crate::driver::run_node`'s single event-loop task publishes into (a
//! plain `Arc<StatusShared>` of atomics) once per loop iteration, and the
//! *only* thing the HTTP handler tasks spawned by [`serve_status`] (each an
//! independent `tokio::spawn`) are allowed to read. They never see
//! `queso_smr::SmrNode` (which is `Rc<RefCell<_>>`-based and therefore not
//! `Send` -- see `crate::driver`'s module docs' "Single-threaded ownership"
//! section) or `crate::ctx::RealCtx` at all -- exactly the same
//! channel/shared-state discipline every other task this crate spawns
//! (`crate::transport::accept_peers`, `crate::client::accept_clients`)
//! already follows, just with a shared atomics struct standing in for the
//! `mpsc` channel those use (status is a broadcast-style "latest value",
//! not a queue of discrete events, so a channel would be the wrong shape
//! here).

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::warn;

/// Hard cap on how many bytes of one status HTTP request this handler will
/// ever buffer before giving up on parsing it. These are always tiny,
/// bodyless `GET` requests (a browser tab, `curl`, or an orchestrator's
/// probe) -- a well-formed request line plus headers is always far under
/// this. Capping it means a malformed or adversarial client that never
/// sends a newline can only ever cost this replica a few KiB per
/// connection, never unbounded memory.
const MAX_REQUEST_BYTES: usize = 8 * 1024;

/// How long one status connection is given to finish sending its request
/// before this handler gives up and closes it. Guards against a client
/// that opens a connection and then sends nothing (or sends one byte at a
/// time forever, a "slow loris") from holding this task -- and the small
/// amount of memory it has buffered -- open indefinitely.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// The `Send + Sync` status snapshot [`crate::driver::run_node`] publishes
/// once per event-loop iteration and every status HTTP handler reads from.
/// Every field is a plain atomic: cheap to update on the driver's own
/// thread every iteration (no lock, no allocation) and cheap (lock-free) to
/// read from any handler task. See the module docs for why this -- not a
/// channel -- is the right shape for "latest known status", and
/// `crate::driver`'s module docs for why `SmrNode` itself can never be what
/// crosses this boundary instead.
pub struct StatusShared {
    /// Total [`crate::driver::Event`]s this replica has dispatched since
    /// boot, summed across every group-commit batch (see `crate::driver`'s
    /// "Group commit" docs) -- i.e. exactly how many `on_message`/
    /// `on_timer`/`submit` calls this replica's `SmrNode` has actually
    /// received, regardless of how those events happened to batch for a
    /// single fsync.
    events_processed: AtomicU64,
    /// This replica's most recent `queso_smr::SmrNode::next_slot()` --
    /// the first log slot index not yet applied, i.e. its current decided-
    /// log frontier.
    next_slot: AtomicU64,
    /// This replica's most recent `crate::persist::Store::save_count()` --
    /// the number of real write+fsync+rename+dir-fsync cycles this
    /// replica's durable store has completed. This is the **production**,
    /// always-on counter every `queso-node` run already tracks (see that
    /// method's docs) -- deliberately not `NodeConfig::save_counter`, which
    /// is test-only instrumentation for sharing/observing a counter *across*
    /// a test harness, not a per-replica metric a real deployment would read.
    save_count: AtomicU64,
    /// See [`Self::is_ready`] for the precise, honest meaning.
    ready: AtomicBool,
    /// Wall-clock instant this [`StatusShared`] was constructed (i.e. this
    /// replica's driver loop starting up) -- fixed for the process's whole
    /// lifetime, used only to compute `/metrics`' `uptime_secs`.
    started_at: Instant,
}

impl StatusShared {
    /// A fresh status snapshot for a replica that has not yet processed a
    /// single event: everything zero, not yet ready. `crate::driver::run_node`
    /// publishes a real first snapshot (reflecting whether this boot needs a
    /// restart catch-up pass) before entering its event loop -- see that
    /// function's body -- so an external observer can only ever see this
    /// all-zero, not-ready state for the brief window between the status
    /// listener accepting a connection and the driver's first publish, not
    /// as this replica's steady-state answer.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            events_processed: AtomicU64::new(0),
            next_slot: AtomicU64::new(0),
            save_count: AtomicU64::new(0),
            ready: AtomicBool::new(false),
            started_at: Instant::now(),
        })
    }

    /// Publish a fresh snapshot: `events_delta` (the number of events the
    /// driver's loop iteration that just finished applied -- see
    /// `crate::driver`'s "Group commit" docs, `0` for the pre-loop publish
    /// right after boot/restart-catch-up-kickoff) is *added* to the running
    /// total; `next_slot`/`save_count`/`ready` are absolute values that
    /// simply overwrite the previous snapshot. Called from the driver's own
    /// single event-loop task only -- see the module docs.
    pub fn publish(&self, events_delta: u64, next_slot: u64, save_count: u64, ready: bool) {
        if events_delta != 0 {
            self.events_processed
                .fetch_add(events_delta, Ordering::Relaxed);
        }
        self.next_slot.store(next_slot, Ordering::Relaxed);
        self.save_count.store(save_count, Ordering::Relaxed);
        self.ready.store(ready, Ordering::Relaxed);
    }

    /// Whether `GET /ready` should currently answer `200` (`true`) or `503`
    /// (`false`).
    ///
    /// **Precise, honest meaning:** `true` iff this replica's `SmrNode` is
    /// *not currently known to be running its own internal restart catch-up
    /// probe* (`queso_smr::SmrNode::is_catching_up()` was `false` as of the
    /// most recent published snapshot). Concretely: a freshly-booted
    /// replica (no on-disk snapshot, never calls `on_restart`) is ready
    /// immediately; a replica that reloaded durable state from disk and is
    /// therefore rejoining as a learner (see `crate::driver::run_node`'s
    /// "Durability across a real process restart" docs) is *not* ready
    /// until its catch-up probe decides and it falls back to idle.
    ///
    /// **What this deliberately does *not* claim:** this is not a proof
    /// that this replica has caught up to the rest of the cluster's actual
    /// current frontier, nor that a linearizable read against it right now
    /// would return the latest value -- `queso_smr::SmrNode::begin_catch_up`
    /// only proves progress up to whatever frontier a majority could show a
    /// catch-up probe *at the moment it asked*; the cluster may have moved
    /// on since, and this replica has no cheap, honest way to know that
    /// without an extra round trip this endpoint does not perform. It is
    /// also not sticky: the catch-up quiescence watchdog can re-issue
    /// catch-up (see `queso_smr::replica`'s docs on
    /// `on_catch_up_watchdog`), which would flip this back to `false` after
    /// having been `true` -- an honest reflection of a replica that fell
    /// behind again (e.g. a transient partition), not a bug. In short: this
    /// is "not in a known boot/rejoin catch-up phase right now", the
    /// cheapest signal `SmrNode` can honestly give a real driver about
    /// catch-up completion -- good enough for a load balancer's "don't
    /// route to a replica that just rebooted and is still learning" probe
    /// (the fly.io consumer this was built for), not a linearizable-read
    /// readiness guarantee.
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Relaxed)
    }

    /// Render this replica's current counters as a pretty-printed JSON
    /// document -- the `/metrics` response body.
    fn metrics_json(&self) -> String {
        let body = MetricsBody {
            events_processed: self.events_processed.load(Ordering::Relaxed),
            next_slot: self.next_slot.load(Ordering::Relaxed),
            save_count: self.save_count.load(Ordering::Relaxed),
            ready: self.is_ready(),
            uptime_secs: self.started_at.elapsed().as_secs_f64(),
        };
        // `MetricsBody` is four integers, a bool, and a finite non-negative
        // float -- there is no value this type can hold that
        // `serde_json::to_string_pretty` rejects (the only failure mode is
        // non-finite floats, and `Instant::elapsed` can't produce one).
        serde_json::to_string_pretty(&body)
            .expect("MetricsBody contains no non-finite floats to reject")
    }
}

/// The exact shape of `/metrics`' JSON body. See [`StatusShared`]'s field
/// docs for what each counter means and how it's tracked; this is purely a
/// serialization shim over a [`StatusShared`] snapshot.
#[derive(Serialize)]
struct MetricsBody {
    events_processed: u64,
    next_slot: u64,
    save_count: u64,
    ready: bool,
    uptime_secs: f64,
}

/// Accept connections on `listener` forever, spawning one bounded-lifetime
/// task per connection (see [`handle_connection`]). Each handler task only
/// ever reads from `status` -- never `queso_smr::SmrNode` or
/// `crate::ctx::RealCtx` -- so this can run on an ordinary `tokio::spawn`
/// task, off the driver's own task, exactly like
/// `crate::transport::accept_peers`/`crate::client::accept_clients`.
pub async fn serve_status(listener: TcpListener, status: Arc<StatusShared>) {
    loop {
        let (stream, _addr) = match listener.accept().await {
            Ok(x) => x,
            Err(err) => {
                warn!(%err, "status listener accept failed");
                continue;
            }
        };
        let status = Arc::clone(&status);
        tokio::spawn(async move {
            handle_connection(stream, status).await;
        });
    }
}

/// Serve exactly one connection: read a bounded amount of request bytes
/// (capped by [`MAX_REQUEST_BYTES`], time-bounded by [`REQUEST_TIMEOUT`]),
/// parse just enough of it to route on method + path, write one response,
/// and close. Never panics on malformed input -- anything this parser can't
/// make sense of (non-UTF8 bytes, a request line with no path, a read
/// timeout, a read error) is answered with `400 Bad Request` (or the
/// connection is simply dropped, for a client that never finished sending
/// anything) rather than propagated as an error that could take down this
/// task in a way that looks alarming, though even a panic here would only
/// ever unwind this one connection's task, never the driver's.
async fn handle_connection(mut stream: TcpStream, status: Arc<StatusShared>) {
    let mut buf = Vec::with_capacity(256);
    let mut chunk = [0u8; 512];

    let read_outcome = tokio::time::timeout(REQUEST_TIMEOUT, async {
        loop {
            match stream.read(&mut chunk).await {
                Ok(0) => break, // Peer closed before sending a full request line.
                Ok(n) => {
                    buf.extend_from_slice(&chunk[..n]);
                    if buf.contains(&b'\n') || buf.len() >= MAX_REQUEST_BYTES {
                        break;
                    }
                }
                Err(_) => break, // Read error -- treat exactly like an early close.
            }
        }
    })
    .await;

    if read_outcome.is_err() {
        return; // Timed out waiting for a request; nothing sane to answer with.
    }

    let (status_line, content_type, body) = match parse_request_line(&buf) {
        Some((method, path)) if method.eq_ignore_ascii_case("GET") => route(&path, &status),
        Some(_) => (
            "405 Method Not Allowed",
            "text/plain",
            "only GET is supported\n".to_string(),
        ),
        None => (
            "400 Bad Request",
            "text/plain",
            "malformed request\n".to_string(),
        ),
    };

    let response = format!(
        "HTTP/1.1 {status_line}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    // Best-effort: a write failure here just means the peer went away, no
    // different from any other connection dropping mid-response.
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.shutdown().await;
}

/// Parse just the request line (`METHOD PATH [HTTP-VERSION]`) out of
/// `buf`'s first line -- headers and any body (there never legitimately is
/// one; every route here is a bodyless `GET`) are ignored entirely. Returns
/// `None` for anything that isn't at least valid UTF-8 with a method and a
/// path token, never panics.
fn parse_request_line(buf: &[u8]) -> Option<(String, String)> {
    let line_end = buf.iter().position(|&b| b == b'\n')?;
    let mut line = &buf[..line_end];
    if line.last() == Some(&b'\r') {
        line = &line[..line.len() - 1];
    }
    let line = std::str::from_utf8(line).ok()?;
    let mut parts = line.split_whitespace();
    let method = parts.next()?.to_string();
    let path = parts.next()?.to_string();
    Some((method, path))
}

/// Route an already-parsed `GET` request's path to a response. See the
/// module docs for each endpoint's precise semantics.
fn route(path: &str, status: &StatusShared) -> (&'static str, &'static str, String) {
    match path {
        "/health" => ("200 OK", "text/plain", "ok\n".to_string()),
        "/ready" => {
            if status.is_ready() {
                ("200 OK", "text/plain", "ready\n".to_string())
            } else {
                (
                    "503 Service Unavailable",
                    "text/plain",
                    "not ready\n".to_string(),
                )
            }
        }
        "/metrics" => ("200 OK", "application/json", status.metrics_json()),
        _ => ("404 Not Found", "text/plain", "not found\n".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_status_is_not_ready_and_all_zero() {
        let status = StatusShared::new();
        assert!(!status.is_ready());
        assert_eq!(status.events_processed.load(Ordering::Relaxed), 0);
        assert_eq!(status.next_slot.load(Ordering::Relaxed), 0);
        assert_eq!(status.save_count.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn publish_accumulates_events_but_overwrites_the_rest() {
        let status = StatusShared::new();
        status.publish(1, 5, 2, true);
        status.publish(3, 9, 4, false);
        assert_eq!(status.events_processed.load(Ordering::Relaxed), 4);
        assert_eq!(status.next_slot.load(Ordering::Relaxed), 9);
        assert_eq!(status.save_count.load(Ordering::Relaxed), 4);
        assert!(!status.is_ready());
    }

    #[test]
    fn parse_request_line_accepts_a_well_formed_get() {
        let (method, path) =
            parse_request_line(b"GET /health HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
        assert_eq!(method, "GET");
        assert_eq!(path, "/health");
    }

    #[test]
    fn parse_request_line_accepts_a_request_line_with_no_trailing_cr() {
        let (method, path) = parse_request_line(b"GET /ready HTTP/1.1\n").unwrap();
        assert_eq!(method, "GET");
        assert_eq!(path, "/ready");
    }

    #[test]
    fn parse_request_line_rejects_a_missing_path() {
        assert!(parse_request_line(b"GET\r\n").is_none());
    }

    #[test]
    fn parse_request_line_rejects_a_line_with_no_newline_at_all() {
        assert!(parse_request_line(b"GET /health HTTP/1.1").is_none());
    }

    #[test]
    fn parse_request_line_rejects_non_utf8_bytes() {
        assert!(parse_request_line(b"GET /\xff\xfe HTTP/1.1\r\n").is_none());
    }

    #[test]
    fn route_metrics_body_parses_as_json_with_expected_fields() {
        let status = StatusShared::new();
        status.publish(2, 3, 1, true);
        let (status_line, content_type, body) = route("/metrics", &status);
        assert_eq!(status_line, "200 OK");
        assert_eq!(content_type, "application/json");
        let parsed: serde_json::Value = serde_json::from_str(&body).expect("valid json");
        assert_eq!(parsed["events_processed"], 2);
        assert_eq!(parsed["next_slot"], 3);
        assert_eq!(parsed["save_count"], 1);
        assert_eq!(parsed["ready"], true);
        assert!(parsed["uptime_secs"].as_f64().unwrap() >= 0.0);
    }

    #[test]
    fn route_unknown_path_is_404() {
        let status = StatusShared::new();
        let (status_line, _, _) = route("/nope", &status);
        assert_eq!(status_line, "404 Not Found");
    }
}
