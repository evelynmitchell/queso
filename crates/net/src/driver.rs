//! [`run_node`]: boot one replica over a real TCP network and drive its
//! unmodified [`queso_smr::SmrNode`] state machine from a single task --
//! the real-network analogue of `queso_smr::cluster::SmrCluster` driving
//! the same type over `queso_sim::kernel::Kernel`.
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
use queso_sim::node::Node;
use queso_smr::{Command, OpId, OpRecord, Outcome, SmrNode};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tracing::info;

use crate::client;
use crate::config::NodeConfig;
use crate::ctx::RealCtx;
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
    let (inbox_tx, mut inbox_rx) = mpsc::unbounded_channel::<Event>();

    // One outbound queue per *other* replica -- a replica never dials or
    // sends to itself (`Ctx::send` is never called with `dst ==
    // ctx.self_id()` by any `Node` impl in this codebase, matching every
    // sim-driven test).
    let mut outbound = BTreeMap::new();
    for (&peer_id, &addr) in &config.peers {
        if peer_id == config.id {
            continue;
        }
        let (tx, rx) = mpsc::unbounded_channel();
        outbound.insert(peer_id, tx);
        transport::spawn_peer_dialer(config.id, addr, rx);
    }

    let peer_listener = TcpListener::bind(config.listen_addr).await?;
    info!(id = ?config.id, addr = %config.listen_addr, "listening for peers");
    tokio::spawn(transport::accept_peers(peer_listener, inbox_tx.clone()));

    let client_listener = TcpListener::bind(config.client_listen_addr).await?;
    info!(id = ?config.id, addr = %config.client_listen_addr, "listening for clients");
    tokio::spawn(client::accept_clients(client_listener, inbox_tx.clone()));

    let mut node = SmrNode::new_fixed_leader(config.total_replicas, config.leader);
    let mut ctx = RealCtx::new(
        config.id,
        config.seed,
        config.tick,
        outbound,
        inbox_tx.clone(),
    );

    let mut pending: BTreeMap<OpId, oneshot::Sender<Outcome>> = BTreeMap::new();
    let mut next_op_id: u64 = 0;

    while let Some(event) = inbox_rx.recv().await {
        // Fixed for the whole dispatch below, exactly like
        // `queso_sim::kernel::Kernel::run_until` fixing `KernelCore::now`
        // before invoking a `Node` callback -- see `RealCtx::tick_now`'s
        // docs.
        ctx.tick_now();
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

        // A submitted op usually completes several events later than the
        // one that submitted it (whichever message/timer finally drives
        // its slot to a decision) -- so every dispatch, not just
        // `ClientSubmit`, must check whether any pending op just finished
        // and, if so, answer its waiting client.
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
