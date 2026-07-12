// Real wall-clock timing is exactly what the ordering-regression guard
// below measures -- same per-crate-root allow as `queso-net`'s
// `src/lib.rs`/`tests/bench.rs`, needed again here since each `tests/*.rs`
// file is its own crate root.
#![allow(clippy::disallowed_methods)]

//! Phase 8.1a's (issue #46) acceptance tests: group-commit coalescing, the
//! async fsync offload, and -- the property that actually matters -- that
//! write-before-reply (P12) survives both changes intact. See
//! `queso_net::driver`'s module docs (`crates/net/src/driver.rs`) for the
//! mechanism these exercise: apply a batch of already-queued events, take
//! one `Durable` snapshot, persist it once (now via `Store::persist`'s
//! `spawn_blocking`-offloaded fsync), and only then flush every reply the
//! whole batch produced.
//!
//! Two of `queso_net::persist::Store`'s fields/`NodeConfig`'s fields exist
//! *purely* for these tests (see their own docs):
//!
//! - [`NodeConfig::persist_delay`]/[`Store::with_artificial_delay`]: an
//!   artificial extra sleep before every blocking snapshot write, used by
//!   [`write_before_reply_holds_even_when_the_fsync_is_slow`] to make the
//!   write-before-reply ordering observable in wall-clock time from outside
//!   the process (a black-box test otherwise cannot distinguish "the reply
//!   waited for the fsync" from "the reply raced it and won", since a fast
//!   local disk makes both indistinguishable within measurement noise).
//! - [`NodeConfig::save_counter`]/[`Store::with_save_counter`]: a shareable
//!   counter of real fsync'd writes, used by
//!   [`group_commit_coalesces_fsyncs_under_concurrent_load`] to prove
//!   batching actually reduces write amplification, not just "the code
//!   compiles and still returns the right answer".
//!
//! Neither is ever set by `queso-node`'s CLI or by any other test in this
//! crate -- both default to off (`Duration::ZERO`, `None`).
//!
//! [`NodeConfig::persist_delay`]: queso_net::config::NodeConfig::persist_delay
//! [`NodeConfig::save_counter`]: queso_net::config::NodeConfig::save_counter
//! [`Store::with_artificial_delay`]: queso_net::persist::Store::with_artificial_delay
//! [`Store::with_save_counter`]: queso_net::persist::Store::with_save_counter

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use queso_sim::ids::NodeId;
use queso_smr::{ClientId, Command, Outcome};
use tokio::task::JoinSet;

#[path = "support/mod.rs"]
mod support;
use support::{spawn_cluster_with_persist_hooks, submit_with_retry};

fn put(client: u32, seq: u64, key: u32, value: i64) -> Command {
    Command::Put {
        client: ClientId(client),
        seq,
        key,
        value,
    }
}

fn get(client: u32, seq: u64, key: u32) -> Command {
    Command::Get {
        client: ClientId(client),
        seq,
        key,
    }
}

/// **Ordering-regression guard (behavioral).** Proves, from outside the
/// process and in wall-clock time, that `queso_net::driver::run_node`'s
/// event loop genuinely persists a batch's durable mutations *before*
/// releasing anything that batch produced -- not merely "the code happens
/// to be written in that order today", but a black-box check that fails if
/// a future refactor swapped `store.persist(...).await` and
/// `ctx.flush_outbound()`.
///
/// Mechanism: every replica's blocking snapshot write is made artificially
/// slow by a fixed, generous `delay` (`NodeConfig::persist_delay`). If
/// write-before-reply holds, a client cannot receive a durable-mutating
/// op's `Outcome` any faster than that delay: the reply is buffered
/// (`RealCtx::pending_outbound`, never sent synchronously from inside
/// `Ctx::send`) and is only ever released by `flush_outbound`, which the
/// driver's loop only reaches *after* `store.persist(...).await` resolves
/// -- and `persist` cannot resolve before the sleep it injected into the
/// write finishes. If a regression reordered those two calls (or otherwise
/// let a reply race ahead of its durable write), the reply would leave at
/// ordinary network-round-trip speed, completely independent of the
/// artificial disk delay, and the lower-bound assertion below would fail.
///
/// A warm-up `Put` runs first (and is excluded from the timed measurement)
/// so cluster-formation/connection-dialing latency -- unrelated to this
/// property -- can never be mistaken for write-before-reply's effect; only
/// a *second*, freshly submitted op's round trip is actually timed, against
/// an already-live, already-connected client.
#[tokio::test(flavor = "multi_thread")]
async fn write_before_reply_holds_even_when_the_fsync_is_slow() {
    let delay = Duration::from_millis(250);
    let client_addrs = spawn_cluster_with_persist_hooks(3, Some(NodeId(0)), delay, None, None);
    let timeout = Duration::from_secs(20);

    let warmup = submit_with_retry(client_addrs[0], &put(1, 0, 1, 1), timeout).await;
    assert_eq!(
        warmup,
        Outcome::Put,
        "warm-up Put must still succeed under the artificial delay"
    );

    let start = Instant::now();
    let outcome = submit_with_retry(client_addrs[0], &put(1, 1, 42, 7), timeout).await;
    let elapsed = start.elapsed();

    assert_eq!(outcome, Outcome::Put);
    assert!(
        elapsed >= delay,
        "a Put that mutates durable state came back in {elapsed:?}, faster than the artificial \
         {delay:?} fsync delay every replica's Store was configured with -- the reply must have \
         left before the durable write it depends on actually completed, a write-before-reply \
         (P12) violation (see this test's doc comment for the full argument)"
    );
}

/// **Ordering-regression guard (structural).** A cheap, fast, no-process
/// companion to the behavioral test above: pins that
/// `queso_net::driver::run_node`'s event loop calls `store.persist(...)`
/// strictly *before* `ctx.flush_outbound()` within the loop body, so an
/// accidental reorder trips an instant unit test too, not only the slower
/// (though far more convincing) end-to-end one. Deliberately scoped to
/// *after* the loop's `while let Some(first_event) = ...` line: an earlier,
/// unrelated `ctx.flush_outbound()` call already exists in this function
/// (the `on_restart` boot-time flush, which has nothing to persist and
/// correctly runs with no preceding fsync -- see that call site's own
/// comment) and must not be confused with the loop's.
#[test]
fn driver_source_persists_before_it_flushes_outbound_in_the_loop() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let src = std::fs::read_to_string(Path::new(manifest_dir).join("src/driver.rs"))
        .expect("read crates/net/src/driver.rs");
    let loop_start = src
        .find("while let Some(first_event) = inbox_rx.recv().await {")
        .expect(
            "driver.rs's event loop must still start with this exact line -- update this \
                 test if the loop's shape changed intentionally",
        );
    let loop_body = &src[loop_start..];
    let persist_pos = loop_body
        .find("store.persist(")
        .expect("the event loop must call `store.persist(...)` somewhere in its body");
    let flush_pos = loop_body
        .find("ctx.flush_outbound();")
        .expect("the event loop must call `ctx.flush_outbound()` somewhere in its body");
    assert!(
        persist_pos < flush_pos,
        "queso_net::driver::run_node's event loop must call `store.persist(...)` (the \
         write-before-reply fsync) strictly before `ctx.flush_outbound()` -- found persist at \
         loop-relative byte {persist_pos}, flush at {flush_pos}. This is a purely textual \
         tripwire; see `write_before_reply_holds_even_when_the_fsync_is_slow` for the real \
         behavioral proof of the same property."
    );
}

/// **Group-commit actually batches.** Directly compares two counters from
/// the *same* run rather than an indirect, noisy proxy like wall-clock
/// throughput: [`NodeConfig::durable_event_counter`] counts every
/// dispatched durable-mutating [`queso_net::driver::Event::Message`],
/// regardless of batching, while [`NodeConfig::save_counter`] counts only
/// the real fsync'd writes group-commit coalescing actually performed. If
/// even one batch ever applied more than one mutating event before its
/// single persist, `save_count < durable_event_count` -- an unambiguous,
/// hard-to-fake signal that coalescing happened, with no need to guess at
/// what an "unbatched" write-amplification baseline should have been.
///
/// This needs a real *opportunity* for events to coalesce, which -- see
/// `queso_net::driver`'s "Async fsync offload" docs -- only exists while a
/// batch's own persist is still in flight (that's the window during which
/// more events can queue up for the *next* batch to drain together). On a
/// fast local test disk, a real fsync is often faster than the very network
/// round trip needed to gather the next batch's events, so coalescing can
/// be rare in practice even though the mechanism is real (this mirrors
/// production: batching's payoff scales with how slow fsync is relative to
/// message delivery, exactly the disk-bound regime issue #46 targets -- see
/// this crate's README's former "Honest limits" entry on per-RPC fsync
/// latency). [`NodeConfig::persist_delay`] recreates that disk-bound regime
/// deterministically instead of hoping a fast CI disk happens to be slow
/// enough, so this test's result does not depend on the underlying
/// filesystem's real fsync latency at all.
///
/// Concurrent load (many `Put`s submitted at once, rather than one another
/// after) matters here too: it is what lets several *independent* decisions
/// (and their own recorder round trips) be in flight near-simultaneously in
/// the first place, so there is more than one mutating event that could
/// possibly land in the same ready-batch to begin with.
#[tokio::test(flavor = "multi_thread")]
async fn group_commit_coalesces_fsyncs_under_concurrent_load() {
    let save_count = Arc::new(AtomicU64::new(0));
    let event_count = Arc::new(AtomicU64::new(0));
    let persist_delay = Duration::from_millis(15);
    let client_addrs = spawn_cluster_with_persist_hooks(
        3,
        Some(NodeId(0)),
        persist_delay,
        Some(save_count.clone()),
        Some(event_count.clone()),
    );
    let timeout = Duration::from_secs(30);

    // Warm the cluster up (connections dialed, leader settled) before
    // measuring anything, then reset both counters so only the load below
    // is measured.
    let warmup = submit_with_retry(client_addrs[0], &put(1, 0, 1, 1), timeout).await;
    assert_eq!(warmup, Outcome::Put);
    save_count.store(0, Ordering::SeqCst);
    event_count.store(0, Ordering::SeqCst);

    // Fire many Puts at the leader concurrently.
    const CONCURRENT_OPS: u64 = 150;
    let mut tasks = JoinSet::new();
    for i in 0..CONCURRENT_OPS {
        let addr = client_addrs[0];
        tasks.spawn(async move {
            submit_with_retry(addr, &put(2, i, (500 + i) as u32, i as i64), timeout).await
        });
    }
    let mut completed = 0u64;
    while let Some(res) = tasks.join_next().await {
        assert_eq!(res.expect("submit task must not panic"), Outcome::Put);
        completed += 1;
    }
    assert_eq!(
        completed, CONCURRENT_OPS,
        "every concurrently-submitted Put must still succeed"
    );

    let saves = save_count.load(Ordering::SeqCst);
    let events = event_count.load(Ordering::SeqCst);
    assert!(
        saves > 0,
        "at least one real fsync must have happened for {CONCURRENT_OPS} durable Puts"
    );
    assert!(
        saves < events,
        "expected fewer real fsync'd writes ({saves}) than durable-mutating events applied \
         ({events}) -- if they were equal, every batch coalesced at most one mutating event, \
         i.e. no group-commit coalescing happened at all under {CONCURRENT_OPS} concurrent ops \
         with a {persist_delay:?} artificial fsync delay"
    );
}

/// The counterpart to the coalescing test above: strictly **sequential**
/// (one op fully completes before the next is submitted) load never has
/// more than one event ready in the inbox when a batch starts forming, so
/// every batch this produces is exactly size 1 -- which must remain exactly
/// as correct as it always was (see `queso_net::driver`'s "Group commit"
/// docs: a batch of size 1 runs byte-for-byte the same
/// apply/snapshot/persist/flush sequence the pre-8.1a per-event code
/// always did). This also closes a gap the concurrent-load test above
/// can't: a coalescing bug that accidentally *elided* every fsync would
/// make `saves < CONCURRENT_OPS` trivially true (`0 < 200`) without proving
/// anything actually got persisted -- `save_count() > 0` here, on a
/// sequential run where every reply genuinely required its own durable
/// write, closes that hole.
#[tokio::test(flavor = "multi_thread")]
async fn a_single_op_at_a_time_still_works_and_is_still_persisted() {
    let save_count = Arc::new(AtomicU64::new(0));
    let client_addrs = spawn_cluster_with_persist_hooks(
        3,
        Some(NodeId(0)),
        Duration::ZERO,
        Some(save_count.clone()),
        None,
    );
    let timeout = Duration::from_secs(15);

    let put_outcome = submit_with_retry(client_addrs[0], &put(3, 0, 55, 99), timeout).await;
    assert_eq!(put_outcome, Outcome::Put);

    let get_outcome = submit_with_retry(client_addrs[2], &get(4, 0, 55), timeout).await;
    assert_eq!(get_outcome, Outcome::Get(Some(99)));

    assert!(
        save_count.load(Ordering::SeqCst) > 0,
        "sequential, batch-of-one ops must still actually reach durable storage"
    );
}
