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
//!    build the [`SmrNode`] from the loaded [`Durable`] via
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
//!    After dispatching such an event, this loop snapshots the replica's
//!    current `Durable` ([`SmrNode::durable_snapshot`]) and durably
//!    persists it ([`crate::persist::Store::save`], fsync'd, atomic-rename)
//!    *before* calling [`RealCtx::flush_outbound`] -- i.e. before anything
//!    that event's processing produced (a `RecordResponse` reply, a
//!    proposer's own requests, a self-loopback vote) actually reaches the
//!    network or this replica's own inbox, and before any client `Outcome`
//!    reply for an operation that just completed is sent. A crash between
//!    "decided/recorded in memory" and "fsync'd" can therefore never be
//!    observed by any peer or client -- exactly the guarantee
//!    `queso_smr::replica::SmrNode::on_message`'s doc comment already
//!    promised for a "real deployment", now made real. `Event::Timer` and
//!    `Event::ClientSubmit` never mutate `Durable` (see the call sites in
//!    `queso_smr::replica`), so this loop skips the fsync for those --
//!    their `flush_outbound` runs immediately, with nothing to protect.
//!
//! See `crate::persist`'s module docs for the on-disk format/write scheme,
//! and `crate::ctx::RealCtx`'s docs for the outbound-buffering and
//! logical-time-baseline mechanisms this depends on.
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

use queso_consensus::rpc::ConcreteMsg;
use queso_sim::ids::{NodeId, TimerId};
use queso_sim::node::{Ctx, Node};
use queso_sim::time::LogicalTime;
use queso_smr::{Command, OpId, OpRecord, Outcome, SmrNode};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tracing::info;

use crate::client;
use crate::config::NodeConfig;
use crate::ctx::RealCtx;
use crate::persist;
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

/// Boot one replica: dial every peer, accept inbound peer and client
/// connections, then drive [`queso_smr::SmrNode`] forever from a single
/// loop. Only returns on a fatal setup error (failing to bind a listen
/// address); once the loop starts, it runs until the process is killed.
pub async fn run_node(config: NodeConfig) -> anyhow::Result<()> {
    // Bind both listeners up front, then hand them to the shared body. A
    // caller that needs a *gap-free* bind (e.g. an in-process test cluster
    // picking ephemeral ports without a probe-then-rebind TOCTOU) uses
    // [`run_node_with_listeners`] directly instead.
    let peer_listener = TcpListener::bind(config.listen_addr).await?;
    info!(id = ?config.id, addr = %config.listen_addr, "listening for peers");
    let client_listener = TcpListener::bind(config.client_listen_addr).await?;
    info!(id = ?config.id, addr = %config.client_listen_addr, "listening for clients");
    run_node_with_listeners(config, peer_listener, client_listener).await
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
pub async fn run_node_with_listeners(
    config: NodeConfig,
    peer_listener: TcpListener,
    client_listener: TcpListener,
) -> anyhow::Result<()> {
    let (inbox_tx, mut inbox_rx) = mpsc::unbounded_channel::<Event>();

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
        transport::spawn_peer_dialer(config.id, peer_id, addr.clone(), rx, config.nemesis.clone());
    }

    tokio::spawn(transport::accept_peers(peer_listener, inbox_tx.clone()));
    tokio::spawn(client::accept_clients(client_listener, inbox_tx.clone()));

    // Durability across a real restart (issue #36, P9/P12) -- see this
    // module's docs. `store` is this replica's on-disk snapshot handle;
    // `load()` tells us whether this is a genuine restart (a snapshot
    // already exists for this node id) or a still-cold first boot.
    let store = persist::Store::new(&config.data_dir, config.id)?;
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

    let mut pending: BTreeMap<OpId, oneshot::Sender<Outcome>> = BTreeMap::new();
    let mut next_op_id: u64 = 0;

    while let Some(event) = inbox_rx.recv().await {
        // Fixed for the whole dispatch below, exactly like
        // `queso_sim::kernel::Kernel::run_until` fixing `KernelCore::now`
        // before invoking a `Node` callback -- see `RealCtx::tick_now`'s
        // docs.
        ctx.tick_now();
        // Only an incoming `Message` can mutate this replica's `Durable`
        // half (a `Request` mutates its recorder's ISR; a `Response` that
        // completes a quorum mutates `next_slot`/`applied_log`/`kv` via
        // `SmrNode::finish_attempt`) -- see this module's docs. `Timer` and
        // `ClientSubmit` never do (a timer only re-drives an already-live
        // `Proposer`/starts a fresh one; `submit` only touches the volatile
        // op queue), so there is nothing to fsync before releasing their
        // effects.
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

        // Write-before-reply (P12, on real disk): persist this event's
        // durable mutations, if any, before anything it produced -- a
        // recorder's `RecordResponse`, a proposer's own requests, a
        // self-loopback vote, or (via the completed-ops check just below)
        // a client's `Outcome` -- is allowed to actually leave this
        // replica. See this module's docs and `crate::persist`'s.
        if may_mutate_durable {
            let snapshot = node.durable_snapshot();
            store.save(&snapshot, ctx.now().0)?;
        }
        ctx.flush_outbound();

        // A submitted op usually completes several events later than the
        // one that submitted it (whichever message/timer finally drives
        // its slot to a decision) -- so every dispatch, not just
        // `ClientSubmit`, must check whether any pending op just finished
        // and, if so, answer its waiting client. Only reached after the
        // write-before-reply persist above, so a client can never be told
        // an operation succeeded before that success is durable.
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
                    let _ = resp.send(outcome);
                }
            }
        }
    }

    Ok(())
}
