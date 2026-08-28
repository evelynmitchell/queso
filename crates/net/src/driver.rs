//! [`run_node`]: boot one replica over a real TCP network and drive its
//! unmodified [`queso_smr::SmrNode`] state machine from a single task --
//! the real-network analogue of `queso_smr::cluster::SmrCluster` driving
//! the same type over `queso_sim::kernel::Kernel`.
//!
//! # Durability across a real process restart (issue #36, P9/P12)
//!
//! `queso_sim::kernel::Kernel::restart` recovers a crashed node's durable
//! state for free -- it calls [`queso_smr::SmrNode`]'s
//! [`Node::on_restart`] on the *exact same*, still-heap-resident node it had
//! held the whole time (see `queso_smr::replica::Durable`'s docs). A real
//! process has no such luxury: `SIGKILL` followed by a fresh `exec` starts
//! from a blank heap. `run_node` closes that gap:
//!
//! 1. **Boot**: [`crate::persist::Store::load`] checks whether this node id
//!    has a snapshot on disk already. If so, this is a genuine restart:
//!    build the [`SmrNode`] from the loaded `Durable` via
//!    [`SmrNode::from_durable`], restore [`RealCtx`]'s logical-time
//!    baseline from the snapshot's `max_tick`, and immediately call
//!    [`Node::on_restart`] -- the same learner/catch-up entry point the sim
//!    uses -- before this replica processes a single real event. If not,
//!    this is a genuinely fresh replica: build a blank [`SmrNode`] exactly
//!    as before, `on_restart` is never called (there is nothing to recover
//!    from, and calling it would only cost an unnecessary catch-up round
//!    trip).
//! 2. **Write-before-reply, on real disk (P12)**: every dispatched
//!    [`Event::Message`] can mutate this replica's `Durable` half (a
//!    `Request` mutates its recorder's ISR; a `Response` that completes a
//!    quorum mutates `next_slot`/`applied_log`/`kv` via
//!    `SmrNode::finish_attempt`) -- see `queso_smr::replica`'s module docs.
//!    After dispatching a *batch* of such events (see "Group commit"
//!    below), this loop snapshots the replica's current `Durable`
//!    ([`SmrNode::durable_snapshot`]) exactly once and durably persists it
//!    ([`crate::persist::Store::persist`], fsync'd, atomic-rename, offloaded
//!    -- see "Async fsync offload" below) *before* calling
//!    [`RealCtx::flush_outbound`] -- i.e. before anything any event in that
//!    batch's processing produced (a `RecordResponse` reply, a proposer's
//!    own requests, a self-loopback vote) actually reaches the network or
//!    this replica's own inbox, and before any client `Outcome` reply for
//!    an operation that just completed is sent. A crash between
//!    "decided/recorded in memory" and "fsync'd" can therefore never be
//!    observed by any peer or client -- exactly the guarantee
//!    `queso_smr::replica::SmrNode::on_message`'s doc comment already
//!    promised for a "real deployment", now made real. `Event::Timer` and
//!    `Event::ClientSubmit` never mutate `Durable` (see the call sites in
//!    `queso_smr::replica`), so a batch containing only those skips the
//!    fsync entirely -- its `flush_outbound` runs immediately, with nothing
//!    to protect.
//!
//! See `crate::persist`'s module docs for the on-disk format/write scheme,
//! and `crate::ctx::RealCtx`'s docs for the outbound-buffering and
//! logical-time-baseline mechanisms this depends on.
//!
//! # Group commit (Phase 8.1a, issue #46)
//!
//! Because every persist is a **whole-state snapshot** of `Durable` (see
//! `crate::persist`'s docs), not a per-op delta, one snapshot taken *after*
//! applying several mutating events already captures every one of their
//! effects -- there is nothing incremental to merge, no format change
//! needed, and no risk of reconstructing the wrong state from a partial
//! replay. That makes coalescing multiple events into one fsync trivially
//! correct here in a way a from-scratch WAL would not be (see issue #46's
//! design-decision comment for the full argument, in particular why a
//! command/event-replay WAL is *not* obviously correct for this protocol: a
//! `Response` that completes a quorum mutates `Durable` based on *volatile*
//! proposer attempt state, so replaying logged messages through a fresh
//! node would not reconstruct the same state).
//!
//! Concretely, each iteration of the loop below:
//!
//! 1. `.await`s the *next* event from the inbox (blocking exactly like
//!    before if none is ready).
//! 2. Applies it, then -- **without awaiting anything** -- drains any
//!    *already-queued* events with a non-blocking `try_recv`, applying each
//!    in turn, up to `GROUP_COMMIT_BATCH_LIMIT` events total. This never
//!    waits for more events to arrive: it only coalesces work that was
//!    already sitting in the channel the instant this batch started
//!    forming, so a low-traffic replica (at most one event ready at a time)
//!    always ends up with a batch of exactly one -- identical behavior,
//!    and identical latency, to the pre-8.1a per-event loop. Under
//!    concurrent load, many events routinely accumulate while this
//!    replica's *previous* batch's fsync was in flight (see "Async fsync
//!    offload" below), so this reliably coalesces in practice, not just in
//!    principle.
//! 3. If **any** event in the batch was an [`Event::Message`] (the only
//!    kind that can mutate `Durable`), takes exactly **one**
//!    [`SmrNode::durable_snapshot`] of the state after every event in the
//!    batch has been applied and persists it with **one**
//!    [`crate::persist::Store::persist`] call/fsync -- collapsing what would
//!    have been `N` snapshots/fsyncs into 1. The tick persisted alongside it
//!    is the batch's *last* applied event's [`RealCtx::now`] (`ctx.now().0`
//!    right after that event's [`RealCtx::tick_now`] call), matching
//!    exactly what a batch of size 1 would have persisted before this
//!    change.
//! 4. Calls [`RealCtx::flush_outbound`] exactly **once** for the whole
//!    batch -- every event's buffered sends (see [`RealCtx::send`]'s docs)
//!    are still queued in `RealCtx::pending_outbound` at this point
//!    (`flush_outbound` is the only thing that ever drains it), so this
//!    single call releases everything the whole batch produced, all at
//!    once, only after step 3's fsync (if any) has completed -- preserving
//!    write-before-reply for every event in the batch, not just the last
//!    one.
//!
//! `GROUP_COMMIT_BATCH_LIMIT` bounds how many events one batch (and
//! therefore one fsync-latency's worth of buffered replies) can hold, so a
//! sustained flood of ready events can't indefinitely delay the *earliest*
//! reply in a batch behind an ever-growing one -- see its own docs.
//!
//! # Async fsync offload (Phase 8.1a, issue #46 / issue #39)
//!
//! [`queso_smr::SmrNode`]/`Durable` are `Rc<RefCell<_>>`-based (see
//! "Single-threaded ownership" below) and therefore not `Send` -- they can
//! never cross a `tokio::spawn`/`spawn_blocking` boundary. What *can* cross
//! one is the serialized bytes: [`crate::persist::Store::persist`]
//! serializes the snapshot to a `Vec<u8>` on *this* (the driver) thread --
//! cheap and CPU-only -- then hands those bytes to
//! [`tokio::task::spawn_blocking`] to perform the actual blocking write/
//! `fsync`/rename/directory-`fsync` on a dedicated blocking-pool thread,
//! and `.await`s that task's completion before this loop does anything
//! else. `SmrNode` itself never leaves this task; only plain, `Send` bytes
//! do. This does not weaken write-before-reply at all -- `persist(...)
//! .await` returning is still a full durability barrier, exactly like the
//! synchronous [`crate::persist::Store::save`] it replaces here -- what it
//! changes is that *other* tokio tasks on this runtime (the peer/client
//! accept loops, outbound dialers, timer futures) keep making progress
//! while this replica's fsync is in flight on the blocking pool, instead of
//! a synchronous syscall stalling everything visible to this task. In
//! particular, more events can (and, under load, reliably do) accumulate in
//! the inbox *during* one batch's `persist().await`, ready for the next
//! batch's non-blocking drain the moment this loop iteration finishes --
//! which is exactly what makes group-commit coalesce under real load rather
//! than only in theory.
//!
//! # Single-threaded ownership (why `SmrNode` needs no `Send`/`Sync`/locks)
//!
//! [`queso_smr::SmrNode`] is built on `Rc<RefCell<_>>` (see that crate's
//! docs), which is not `Send`. `run_node`'s event loop never spawns it, or
//! anything that closes over it, onto another task -- it owns `SmrNode`
//! and `crate::ctx::RealCtx` directly and calls their methods synchronously
//! from one `.await`-driven loop on whatever task/thread is running
//! `run_node` itself. Every other task this module spawns
//! (`crate::transport::spawn_peer_dialer`/`accept_peers`,
//! `crate::client::accept_clients`) only ever touches plain, `Send` data
//! (socket bytes, ids, `mpsc`/`oneshot` channels) and communicates with the
//! driver loop exclusively through [`Event`]s over an ordinary
//! `mpsc::UnboundedChannel` -- so nothing here ever needs `SmrNode` (or
//! `RealCtx`) to cross a `tokio::spawn` boundary.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Context;
use queso_consensus::rpc::ConcreteMsg;
use queso_sim::ids::{NodeId, TimerId};
use queso_sim::node::{Ctx, Node};
use queso_sim::time::LogicalTime;
use queso_smr::{Command, OpId, OpRecord, Outcome, SmrNode};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tracing::info;

use crate::chain::ChainFolder;
use crate::client;
use crate::config::NodeConfig;
use crate::ctx::RealCtx;
use crate::persist;
use crate::status::{self, StatusShared};
use crate::transport;

/// Everything that can happen to one replica: a message arrived, a timer
/// this replica scheduled fired, or a client submitted a command. This is
/// the real-network analogue of `queso_sim::kernel::Kernel::run_until`'s
/// event queue -- except here events arrive concurrently, from independent
/// tasks, over a channel, rather than being popped off an in-process
/// priority queue in deterministic `(time, seq)` order (see the crate docs'
/// "honest notes" in `docs/STATUS.md`/the PR description for the ordering
/// consequence of that difference).
pub enum Event {
    Message {
        from: NodeId,
        payload: ConcreteMsg<Command>,
    },
    Timer(TimerId),
    ClientSubmit {
        command: Command,
        resp: oneshot::Sender<Outcome>,
    },
}

/// Max number of already-queued inbox events one iteration of the loop
/// below will coalesce into a single group-commit batch (see this module's
/// "Group commit" docs). Bounded, rather than draining without limit, so a
/// sustained flood of ready events (e.g. many concurrent clients under
/// heavy load) can't indefinitely grow one batch and delay its *earliest*
/// event's reply behind an ever-larger one -- 64 is a generous
/// amortization window (real disk fsync latency is typically orders of
/// magnitude more than the cost of applying a few dozen more in-memory
/// events) without letting one batch's worst-case latency run away.
const GROUP_COMMIT_BATCH_LIMIT: usize = 64;

/// Boot one replica: dial every peer, accept inbound peer and client
/// connections, then drive [`queso_smr::SmrNode`] forever from a single
/// loop. Only returns on a fatal setup error (failing to bind a listen
/// address); once the loop starts, it runs until the process is killed.
pub async fn run_node(config: NodeConfig) -> anyhow::Result<()> {
    // Bind both listeners up front, then hand them to the shared body. A
    // caller that needs a *gap-free* bind (e.g. an in-process test cluster
    // picking ephemeral ports without a probe-then-rebind TOCTOU) uses
    // [`run_node_with_listeners`]/[`run_node_with_status_listener`]
    // directly instead.
    let peer_listener = TcpListener::bind(config.listen_addr).await?;
    info!(id = ?config.id, addr = %config.listen_addr, "listening for peers");
    let client_listener = TcpListener::bind(config.client_listen_addr).await?;
    info!(id = ?config.id, addr = %config.client_listen_addr, "listening for clients");
    // Phase 8.2 (issue #47): `config.status_listen_addr` is `None` for
    // every real `queso-node` run unless its `--status-listen` flag was
    // passed -- see `NodeConfig::status_listen_addr`'s docs -- so this bind
    // is skipped entirely in the common case, exactly like the peer/client
    // binds above are unconditional because those two are never optional.
    let status_listener = match config.status_listen_addr {
        Some(addr) => {
            let listener = TcpListener::bind(addr).await?;
            info!(id = ?config.id, %addr, "listening for status/metrics requests");
            Some(listener)
        }
        None => None,
    };
    run_node_inner(config, peer_listener, client_listener, status_listener).await
}

/// Like [`run_node`], but drives the node over two already-bound listeners
/// instead of binding `config.listen_addr`/`config.client_listen_addr`
/// itself. This exists so an in-process cluster (tests/benches) can bind
/// ephemeral ports and keep the listeners open continuously — no
/// probe-a-free-port-then-drop-then-rebind window for another thread to
/// steal the port in (the `free_addr` TOCTOU). The two listeners should be
/// the ones bound to `config.listen_addr`/`config.client_listen_addr`
/// respectively (peers still dial `config.peers`, so those addresses must
/// match what the peer listener is actually bound to).
///
/// If `config.status_listen_addr` is `Some`, this binds it the ordinary
/// (non-gap-free) way, same as [`run_node`] -- fine for any caller that
/// doesn't specifically need a pre-bound status port; one that does (e.g. a
/// test picking an ephemeral status port without the `free_addr` TOCTOU)
/// should call [`run_node_with_status_listener`] instead, which takes an
/// already-bound status listener the same way this function already does
/// for `peer_listener`/`client_listener`.
pub async fn run_node_with_listeners(
    config: NodeConfig,
    peer_listener: TcpListener,
    client_listener: TcpListener,
) -> anyhow::Result<()> {
    let status_listener = match config.status_listen_addr {
        Some(addr) => Some(TcpListener::bind(addr).await?),
        None => None,
    };
    run_node_inner(config, peer_listener, client_listener, status_listener).await
}

/// Like [`run_node_with_listeners`], but additionally takes an
/// already-bound status/metrics listener (see [`crate::status`]) instead of
/// binding `config.status_listen_addr` itself -- the same gap-free-bind
/// rationale as `peer_listener`/`client_listener`. `config.status_listen_addr`
/// is not consulted at all here; `status_listener` is authoritative (and
/// the status server always runs, since a caller of this function
/// specifically wants one -- there is no `Option` here, unlike the config
/// field, precisely because a caller with nothing to serve status on
/// should just call [`run_node_with_listeners`] instead).
pub async fn run_node_with_status_listener(
    config: NodeConfig,
    peer_listener: TcpListener,
    client_listener: TcpListener,
    status_listener: TcpListener,
) -> anyhow::Result<()> {
    run_node_inner(
        config,
        peer_listener,
        client_listener,
        Some(status_listener),
    )
    .await
}

/// The shared body every `run_node*` entry point above bottoms out in: dial
/// every peer, accept inbound peer/client (and, if `status_listener` is
/// `Some`, status/metrics) connections, then drive
/// [`queso_smr::SmrNode`] forever from a single loop.
async fn run_node_inner(
    config: NodeConfig,
    peer_listener: TcpListener,
    client_listener: TcpListener,
    status_listener: Option<TcpListener>,
) -> anyhow::Result<()> {
    // Phase 8.2 (issue #47): `Some` only when a status listener was
    // actually bound above -- i.e. only when `config.status_listen_addr`
    // (or an explicit `run_node_with_status_listener` caller) opted in. The
    // `StatusShared` this loop publishes into below, and the
    // `status::serve_status` task reading it, simply don't exist otherwise
    // -- not merely idle, genuinely absent, so an ordinary `queso-node` run
    // (or any existing test, none of which ever set
    // `status_listen_addr`) is byte-for-byte unaffected by any of this.
    let status: Option<Arc<StatusShared>> = status_listener.map(|listener| {
        // Phase 9.2 (issue #56): `config.chain_checkpoints` is `None` for
        // every ordinary run, in which case this is exactly the Phase 8.2
        // `StatusShared::new()` -- no chain state, no `/chain`, and the
        // fold below never runs.
        let shared = StatusShared::with_chain(config.chain_checkpoints);
        tokio::spawn(status::serve_status(listener, Arc::clone(&shared)));
        shared
    });

    // Phase 9.2 (issue #56): the driver-side half of the chain hook. `Some`
    // only when the status listener exists *and* checkpoints were
    // configured -- a chain nobody can read would be pure cost. Starts at
    // genesis, so its first fold replays whatever this process already
    // applied (a restart recovering its durable log); see
    // `crate::chain`'s "Restart".
    let mut chain_folder: Option<ChainFolder> = status
        .as_ref()
        .and_then(|shared| shared.chain().map(|chain| ChainFolder::new(chain.every())));

    let (inbox_tx, mut inbox_rx) = mpsc::unbounded_channel::<Event>();

    // Phase 8.2a (issue #47): build this replica's TLS material exactly
    // once, up front, from `config.tls` -- `None` (every real `queso-node`
    // run and every existing test, see `NodeConfig::tls`'s docs) skips this
    // entirely, leaving `peer_tls`/`client_facing_tls` both `None` and
    // every socket below exactly the plain `TcpStream` this crate spoke
    // before this field existed.
    let (peer_server_tls, peer_client_tls, client_facing_tls) = match &config.tls {
        None => (None, None, None),
        Some(tls_config) => {
            let peer_tls = crate::tls::build_peer_tls(tls_config)
                .context("building this replica's peer mTLS configuration")?;
            let client_facing = crate::tls::build_client_facing_server_tls(tls_config)
                .context("building this replica's client-facing TLS configuration")?;
            (
                Some(peer_tls.server_config),
                Some(peer_tls.client_config),
                Some(client_facing),
            )
        }
    };

    // One outbound queue per *other* replica -- a replica never *dials*
    // itself. It can still be sent to by its own `Node` impl (a proposer's
    // `RecordRequest` fan-out deliberately includes its own recorder, see
    // `queso_consensus::proposer::Proposer::all_recorders`'s docs) but
    // `RealCtx::send` handles `dst == self_id` as a loopback through the
    // inbox rather than this `outbound` map -- see that method's docs.
    let mut outbound = BTreeMap::new();
    for (&peer_id, addr) in &config.peers {
        if peer_id == config.id {
            continue;
        }
        let (tx, rx) = mpsc::channel(transport::OUTBOUND_QUEUE_CAPACITY);
        outbound.insert(peer_id, tx);
        // Phase 7.4: `config.nemesis` is `None` for every real `queso-node`
        // run (see `NodeConfig::nemesis`'s docs) -- only test/bench
        // harnesses that build one explicitly reach the fault-injection
        // path inside `spawn_peer_dialer`.
        transport::spawn_peer_dialer(
            config.id,
            peer_id,
            addr.clone(),
            rx,
            config.nemesis.clone(),
            peer_client_tls.clone(),
        );
    }

    tokio::spawn(transport::accept_peers(
        peer_listener,
        inbox_tx.clone(),
        peer_server_tls,
    ));
    tokio::spawn(client::accept_clients(
        client_listener,
        inbox_tx.clone(),
        client_facing_tls,
    ));

    // Durability across a real restart (issue #36, P9/P12) -- see this
    // module's docs. `store` is this replica's on-disk snapshot handle;
    // `load()` tells us whether this is a genuine restart (a snapshot
    // already exists for this node id) or a still-cold first boot.
    // `.with_artificial_delay`/`.with_save_counter` are Phase 8.1a test-only
    // instrumentation (see `NodeConfig::persist_delay`/`save_counter`'s
    // docs) -- a strict no-op (`Duration::ZERO`, this store's own private
    // counter) for every real `queso-node` run.
    let store = persist::Store::new(&config.data_dir, config.id)?
        .with_artificial_delay(config.persist_delay);
    let store = match &config.disk_fault {
        Some(fault) => store.with_disk_fault(fault.clone()),
        None => store,
    };
    let store = match &config.save_counter {
        Some(counter) => store.with_save_counter(counter.clone()),
        None => store,
    };
    let loaded = store.load()?;
    let is_restart = loaded.is_some();
    let (mut node, baseline) = match loaded {
        Some((durable, max_tick)) => (
            SmrNode::from_durable(config.total_replicas, config.leader, durable),
            LogicalTime(max_tick),
        ),
        None => (
            SmrNode::new_fixed_leader(config.total_replicas, config.leader),
            LogicalTime::ZERO,
        ),
    };
    let mut ctx = RealCtx::new(
        config.id,
        config.seed,
        config.tick,
        baseline,
        outbound,
        inbox_tx.clone(),
    );

    if is_restart {
        info!(id = ?config.id, max_tick = baseline.0, "recovered durable state, rejoining as a learner");
        ctx.tick_now();
        // No fsync needed before this flush: `on_restart` only clears
        // volatile state and starts a catch-up `Proposer` (see
        // `queso_smr::replica::SmrNode::on_restart`'s docs) -- it does not
        // itself mutate `Durable`, so there is nothing new to persist yet.
        node.on_restart(&mut ctx);
        ctx.flush_outbound();
    }

    // Phase 8.2: publish this boot's initial status before the loop below
    // ever runs, so `GET /ready` reflects a genuine restart's catch-up
    // kickoff (see `queso_smr::SmrNode::is_catching_up`'s docs) immediately
    // -- not just "not ready" by default until the first event happens to
    // arrive, which could be an arbitrarily long time on an idle cluster. A
    // fresh boot (never restarted, `on_restart` never called above) is
    // reported ready right away: there is nothing to catch up on.
    if let Some(shared) = &status {
        shared.publish(
            0,
            node.next_slot(),
            store.save_count(),
            !node.is_catching_up(),
        );

        // Phase 9.2 (issue #56): fold the chain over whatever this boot
        // reloaded from disk, for the same reason the publish above exists
        // -- so `GET /chain` is truthful from the moment the node is up,
        // rather than reporting an empty table (which reads as "this
        // replica has applied nothing") until some event happens to arrive.
        // On a fresh boot the applied log is empty and this is a no-op; on
        // a restart it replays the durable log, which is exactly the
        // refold `crate::chain`'s "Restart" describes.
        if let (Some(folder), Some(chain)) = (chain_folder.as_mut(), shared.chain()) {
            folder.fold(&node, chain);
        }
    }

    let mut pending: BTreeMap<OpId, oneshot::Sender<Outcome>> = BTreeMap::new();
    let mut next_op_id: u64 = 0;

    while let Some(first_event) = inbox_rx.recv().await {
        let mut batch_mutated_durable = false;
        let mut batch_last_tick: Option<u64> = None;
        let mut batch_len: usize = 0;
        let mut next_event = Some(first_event);

        // Apply the event that woke this iteration, then greedily drain any
        // events *already* sitting in the inbox with a non-blocking
        // `try_recv` -- never waiting for more to arrive -- up to
        // `GROUP_COMMIT_BATCH_LIMIT`. See this module's "Group commit" docs.
        // `next_event` is `Some` at least once (`first_event`), so this loop
        // always runs at least one iteration.
        while let Some(event) = next_event.take() {
            // Fixed for this one event's dispatch, exactly like
            // `queso_sim::kernel::Kernel::run_until` fixing `KernelCore::now`
            // before invoking a `Node` callback -- see `RealCtx::tick_now`'s
            // docs. Recomputed per-event, not once per batch: each event
            // still gets its own accurate dispatch-time `now`, exactly as it
            // would have as a batch of one.
            ctx.tick_now();
            // Only an incoming `Message` can mutate this replica's `Durable`
            // half (a `Request` mutates its recorder's ISR; a `Response`
            // that completes a quorum mutates `next_slot`/`applied_log`/`kv`
            // via `SmrNode::finish_attempt`) -- see this module's docs.
            // `Timer` and `ClientSubmit` never do (a timer only re-drives an
            // already-live `Proposer`/starts a fresh one; `submit` only
            // touches the volatile op queue).
            let may_mutate_durable = matches!(event, Event::Message { .. });
            match event {
                Event::Message { from, payload } => node.on_message(from, payload, &mut ctx),
                Event::Timer(timer_id) => node.on_timer(timer_id, &mut ctx),
                Event::ClientSubmit { command, resp } => {
                    let op_id = OpId(next_op_id);
                    next_op_id += 1;
                    pending.insert(op_id, resp);
                    node.submit(op_id, config.id, command, &mut ctx);
                }
            }
            if may_mutate_durable {
                // Phase 8.1a test-only instrumentation (see
                // `NodeConfig::durable_event_counter`'s docs) -- counts every
                // dispatched mutating event, regardless of batching, so a
                // test can directly compare it against `store.save_count()`
                // to prove coalescing happened. `None` (a no-op) for every
                // real `queso-node` run and every other test.
                if let Some(counter) = &config.durable_event_counter {
                    counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                }
            }
            batch_mutated_durable |= may_mutate_durable;
            batch_last_tick = Some(ctx.now().0);
            batch_len += 1;

            if batch_len < GROUP_COMMIT_BATCH_LIMIT {
                next_event = inbox_rx.try_recv().ok();
            }
        }

        // Write-before-reply (P12, on real disk): persist this *batch's*
        // durable mutations, if any, as a single snapshot/fsync covering
        // every event just applied, before anything any of them produced --
        // a recorder's `RecordResponse`, a proposer's own requests, a
        // self-loopback vote, or (via the completed-ops check just below) a
        // client's `Outcome` -- is allowed to actually leave this replica.
        // `.await`ing `persist` here (rather than `flush_outbound` below)
        // is exactly what makes this a real durability barrier despite the
        // write itself running on a `spawn_blocking` thread -- see this
        // module's "Group commit"/"Async fsync offload" docs and
        // `crate::persist`'s.
        // Write-before-reply invariant guard. `released_ok` is false for a
        // batch that mutated durable state until that state has actually been
        // persisted+fsync'd; it is asserted immediately before *every* point
        // below that can release something this batch produced (the peer/
        // self-loopback flush here, and each client `Outcome` in the
        // completed-ops loop further down). This is a real runtime check of
        // the safety-critical P12 ordering -- not a comment, not a textual
        // source tripwire -- so a future refactor that moved a release ahead
        // of the persist (either the flush, or the separately-dispatched
        // client `Outcome`, which travels a *different* path: a direct
        // `oneshot::Sender::send`, not `flush_outbound`) fails loudly here
        // instead of silently reintroducing issue #36. Kept as `assert!`, not
        // `debug_assert!`, deliberately: a durability-ordering breach is
        // exactly the kind of thing that should fail-stop even in release.
        let mut released_ok = !batch_mutated_durable;
        if batch_mutated_durable {
            let snapshot = node.durable_snapshot();
            let tick = batch_last_tick.expect("a batch that mutated durable state applied at least one event, which always sets batch_last_tick");
            store.persist(&snapshot, tick).await?;
            released_ok = true;
        }
        assert!(
            released_ok,
            "write-before-reply violated: flush_outbound reached before this batch's durable state was persisted"
        );
        ctx.flush_outbound();

        // A submitted op usually completes several events later than the
        // one that submitted it (whichever message/timer finally drives
        // its slot to a decision) -- so every batch, not just one
        // containing a `ClientSubmit`, must check whether any pending op
        // just finished and, if so, answer its waiting client. Only reached
        // after the write-before-reply persist above, so a client can never
        // be told an operation succeeded before that success is durable.
        let completed: Vec<OpId> = pending
            .keys()
            .copied()
            .filter(|op_id| {
                matches!(
                    node.result(*op_id),
                    Some(OpRecord {
                        outcome: Some(_),
                        ..
                    })
                )
            })
            .collect();
        for op_id in completed {
            if let Some(resp) = pending.remove(&op_id) {
                if let Some(OpRecord {
                    outcome: Some(outcome),
                    ..
                }) = node.result(op_id)
                {
                    // Same P12 guard as before `flush_outbound` above, on the
                    // *client-ack* path specifically: a client must never be
                    // told its op succeeded before that success is durable.
                    // This dispatch is a direct `oneshot` send, not routed
                    // through `flush_outbound`, so it needs its own explicit
                    // check -- see `released_ok`'s definition above.
                    assert!(
                        released_ok,
                        "write-before-reply violated: a client Outcome was about to be sent before this batch's durable state was persisted"
                    );
                    let _ = resp.send(outcome);
                }
            }
        }

        // Phase 8.2: publish this iteration's fresh status *after*
        // everything above -- persist, flush, and client acks are all done,
        // so `node.next_slot()`/`store.save_count()`/`node.is_catching_up()`
        // below reflect this batch's full effect, not a partial one. Cheap
        // (a handful of atomic stores) and skipped entirely when no status
        // listener was configured.
        if let Some(shared) = &status {
            shared.publish(
                batch_len as u64,
                node.next_slot(),
                store.save_count(),
                !node.is_catching_up(),
            );

            // Phase 9.2 (issue #56): fold whatever this batch applied into
            // the Chain-of-Blocks hash and publish any checkpoint it
            // crossed. Deliberately *after* the persist above, so a
            // checkpoint is only ever visible for state this replica has
            // already made durable -- an observer must never see a hash
            // covering a write that a crash a microsecond later would
            // erase. Costs one hash per applied command; skipped entirely
            // when the hook is off.
            if let (Some(folder), Some(chain)) = (chain_folder.as_mut(), shared.chain()) {
                folder.fold(&node, chain);
            }
        }
    }

    Ok(())
}
