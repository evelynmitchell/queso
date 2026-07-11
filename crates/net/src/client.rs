//! The minimal client-facing protocol this stage needs to prove the
//! end-to-end path: a client connects to a replica's client port, sends
//! one length-delimited, bincode-encoded `queso_smr::Command` frame, and
//! receives back one length-delimited, bincode-encoded `queso_smr::Outcome`
//! frame -- one request per connection, no pipelining, no retry-to-another-
//! replica-on-failure. A full client library (session/seq management,
//! connection reuse, retry policy, load generation) is Phase 7.2's scope,
//! not this one's; see [`submit`] for the "just enough to prove it works"
//! helper this crate's own integration test uses.

use std::net::SocketAddr;

use bytes::BytesMut;
use futures_util::{SinkExt, StreamExt};
use queso_smr::{Command, Outcome};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::warn;

use crate::driver::Event;

/// Accept client connections forever, spawning one task per connection
/// (see [`serve_one_client`]).
pub async fn accept_clients(listener: TcpListener, inbox: mpsc::UnboundedSender<Event>) {
    loop {
        let (stream, addr) = match listener.accept().await {
            Ok(x) => x,
            Err(err) => {
                warn!(%err, "client accept failed");
                continue;
            }
        };
        let _ = stream.set_nodelay(true);
        let inbox = inbox.clone();
        tokio::spawn(async move {
            if let Err(err) = serve_one_client(stream, inbox).await {
                warn!(%addr, %err, "client connection error");
            }
        });
    }
}

async fn serve_one_client(
    stream: TcpStream,
    inbox: mpsc::UnboundedSender<Event>,
) -> anyhow::Result<()> {
    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());
    let Some(frame) = framed.next().await else {
        return Ok(()); // Client disconnected without sending anything.
    };
    let bytes: BytesMut = frame?;
    let command: Command = bincode::deserialize(&bytes)?;

    let (resp_tx, resp_rx) = oneshot::channel();
    inbox
        .send(Event::ClientSubmit {
            command,
            resp: resp_tx,
        })
        .map_err(|_| anyhow::anyhow!("replica's driver loop has shut down"))?;
    let outcome = resp_rx.await?;

    let bytes = bincode::serialize(&outcome)?;
    framed.send(bytes.into()).await?;
    Ok(())
}

/// Connect to the replica listening at `addr`'s client port, submit
/// `command`, and return its `Outcome`. This is deliberately the smallest
/// possible client: one command, one connection, no retry -- if `addr`
/// isn't the fast-path leader (or crashes, or is partitioned), the command
/// can still be decided (per Meerkat's leaderless-tolerant design, see
/// `queso_smr::cluster`'s module docs) but this helper will simply hang
/// until it is, since a real client's retry-to-a-different-replica policy
/// is Phase 7.2 scope. Good enough to prove the real-TCP path end-to-end
/// (see this crate's `tests/cluster.rs`).
pub async fn submit(addr: SocketAddr, command: &Command) -> anyhow::Result<Outcome> {
    let stream = TcpStream::connect(addr).await?;
    let mut framed = Framed::new(stream, LengthDelimitedCodec::new());
    let bytes = bincode::serialize(command)?;
    framed.send(bytes.into()).await?;
    let Some(frame) = framed.next().await else {
        anyhow::bail!("connection closed before a response arrived");
    };
    let bytes: BytesMut = frame?;
    let outcome: Outcome = bincode::deserialize(&bytes)?;
    Ok(outcome)
}
