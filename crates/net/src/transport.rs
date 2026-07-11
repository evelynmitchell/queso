//! TCP transport: persistent, reconnecting, one-directional connections
//! between every ordered pair of replicas.
//!
//! Each replica dials every *other* replica it knows about (see
//! [`spawn_peer_dialer`]) and separately accepts inbound connections from
//! them (see [`accept_peers`]). That gives two independent, one-directional
//! TCP connections between any pair of replicas `A`/`B`: `A`'s outbound
//! connection to `B` is what `A` uses to send to `B` (and `B`'s acceptor
//! learns "this connection is `A`" from the `Hello` handshake, see
//! `crate::wire::WireMsg`), and symmetrically for `B`'s outbound connection
//! to `A`. This is simpler than a single shared bidirectional connection
//! per pair (no "who dials, who accepts" tie-breaking by id) at the cost of
//! twice as many sockets -- a non-issue at the cluster sizes this stage
//! targets.

use std::net::SocketAddr;
use std::time::Duration;

use bytes::BytesMut;
use futures_util::{SinkExt, StreamExt};
use queso_consensus::rpc::ConcreteMsg;
use queso_sim::ids::NodeId;
use queso_smr::Command;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::{debug, info, warn};

use crate::driver::Event;
use crate::wire::{decode, encode, WireMsg};

/// How long to wait between reconnect attempts (dial failure, or an
/// established connection dropping). Fixed rather than backed-off: unlike
/// the consensus layer's own retry/backoff (which must tolerate an
/// adversarial or very-long-lived partition without hammering the network
/// forever), a dead TCP connection to a known, fixed peer address is
/// expected to come back on an ordinary timescale, and a constant short
/// delay keeps reconnection latency low and this transport's code simple --
/// exactly the kind of complexity Phase 7.1's scope note excludes (no load
/// generator / perf harness, no fancy backoff policy) in favor of Phase
/// 7.2+.
const RECONNECT_DELAY: Duration = Duration::from_millis(200);

/// Spawn the outbound-connection manager for one peer: dial (retrying
/// forever on failure), send a [`WireMsg::Hello`] handshake identifying
/// this replica, then forward every message enqueued on `rx` as a frame --
/// until `rx` is closed (this replica shutting down) or the connection
/// drops, in which case it reconnects and resumes draining `rx` from
/// wherever it left off. Nothing already queued in `rx` is lost across a
/// reconnect (it is an ordinary in-process channel, untouched by the
/// socket dropping); only a message that raced an in-flight write failure
/// can be silently lost, which is exactly the "message drop" fault the
/// consensus layer's own unbounded retry-with-backoff
/// (`queso_consensus::proposer`'s module docs) already tolerates by
/// design -- this transport does not need its own delivery guarantees on
/// top of that.
pub fn spawn_peer_dialer(
    self_id: NodeId,
    peer_addr: SocketAddr,
    mut rx: mpsc::UnboundedReceiver<ConcreteMsg<Command>>,
) {
    tokio::spawn(async move {
        loop {
            let stream = match TcpStream::connect(peer_addr).await {
                Ok(s) => s,
                Err(err) => {
                    debug!(%peer_addr, %err, "dial failed, retrying");
                    tokio::time::sleep(RECONNECT_DELAY).await;
                    continue;
                }
            };
            let _ = stream.set_nodelay(true);
            let mut framed = Framed::new(stream, LengthDelimitedCodec::new());
            if framed.send(encode(&WireMsg::Hello(self_id))).await.is_err() {
                tokio::time::sleep(RECONNECT_DELAY).await;
                continue;
            }
            info!(?self_id, %peer_addr, "connected to peer");
            loop {
                match rx.recv().await {
                    Some(msg) => {
                        if framed.send(encode(&WireMsg::App(msg))).await.is_err() {
                            warn!(%peer_addr, "write failed, reconnecting");
                            break;
                        }
                    }
                    None => return, // This replica is shutting down.
                }
            }
            tokio::time::sleep(RECONNECT_DELAY).await;
        }
    });
}

/// Accept inbound peer connections forever, spawning one reader task per
/// connection. Each connection's first frame must be a [`WireMsg::Hello`]
/// identifying the dialer; every frame after that is decoded and forwarded
/// to `inbox` as [`Event::Message`].
pub async fn accept_peers(listener: TcpListener, inbox: mpsc::UnboundedSender<Event>) {
    loop {
        let (stream, addr) = match listener.accept().await {
            Ok(x) => x,
            Err(err) => {
                warn!(%err, "peer accept failed");
                continue;
            }
        };
        let _ = stream.set_nodelay(true);
        let inbox = inbox.clone();
        tokio::spawn(async move {
            handle_peer_connection(stream, addr, inbox).await;
        });
    }
}

async fn handle_peer_connection(
    stream: TcpStream,
    addr: SocketAddr,
    inbox: mpsc::UnboundedSender<Event>,
) {
    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());
    let from = match framed.next().await {
        Some(Ok(bytes)) => match decode(&bytes) {
            Ok(WireMsg::Hello(id)) => id,
            Ok(WireMsg::App(_)) => {
                warn!(%addr, "expected Hello as the first frame, closing");
                return;
            }
            Err(err) => {
                warn!(%addr, %err, "failed to decode Hello, closing");
                return;
            }
        },
        _ => return,
    };
    info!(%addr, ?from, "accepted peer connection");
    while let Some(frame) = framed.next().await {
        let bytes: BytesMut = match frame {
            Ok(b) => b,
            Err(err) => {
                warn!(%addr, ?from, %err, "read error, closing connection");
                break;
            }
        };
        match decode(&bytes) {
            Ok(WireMsg::App(payload)) => {
                if inbox.send(Event::Message { from, payload }).is_err() {
                    return; // This replica is shutting down.
                }
            }
            Ok(WireMsg::Hello(_)) => {
                warn!(%addr, ?from, "unexpected repeated Hello, ignoring");
            }
            Err(err) => {
                warn!(%addr, ?from, %err, "decode error, dropping frame");
            }
        }
    }
}
