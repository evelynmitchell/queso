//! Peer-to-peer wire framing: length-delimited frames
//! (`tokio_util::codec::LengthDelimitedCodec`) carrying bincode-encoded
//! [`WireMsg`] values.

use bytes::{Bytes, BytesMut};
use queso_consensus::rpc::ConcreteMsg;
use queso_sim::ids::NodeId;
use queso_smr::Command;
use serde::{Deserialize, Serialize};

/// One frame on a peer-to-peer connection.
///
/// A freshly `accept`ed TCP connection has no way to learn which replica
/// dialed in -- the OS-assigned source port isn't it, and this transport
/// deliberately keeps one dedicated, one-directional connection per
/// ordered `(dialer, acceptor)` pair (see `crate::transport`'s docs) rather
/// than a single shared bidirectional link -- so the dialer's very first
/// frame on every connection is always `Hello(self_id)`, and every frame
/// after that is `App`, carrying exactly the wire payload the verified
/// consensus/SMR core already speaks
/// (`queso_consensus::rpc::ConcreteMsg<queso_smr::Command>`), unmodified.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WireMsg {
    Hello(NodeId),
    App(ConcreteMsg<Command>),
}

/// Encode `msg` to bytes ready to hand to a `LengthDelimitedCodec`-framed
/// sink. Bincode encoding of these types cannot fail (no maps with
/// non-string keys, no untagged enums, nothing bincode rejects), so this
/// panics rather than threading an error through every call site -- the
/// same posture the sim-verified core takes on its own internal
/// invariants.
pub fn encode(msg: &WireMsg) -> Bytes {
    Bytes::from(bincode::serialize(msg).expect("WireMsg encoding is infallible"))
}

/// Decode one frame's bytes back into a [`WireMsg`]. Unlike `encode`, this
/// *can* fail -- the bytes came off the network, from a peer that could in
/// principle be buggy, stale (a different protocol version), or malicious
/// -- so callers must handle the error (typically: log and drop the frame,
/// or close the connection) rather than unwrap it.
pub fn decode(bytes: &BytesMut) -> Result<WireMsg, bincode::Error> {
    bincode::deserialize(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use queso_consensus::proposal::Proposal;
    use queso_consensus::rpc::RecordRequest;
    use queso_smr::ClientId;

    #[test]
    fn hello_round_trips() {
        let msg = WireMsg::Hello(NodeId(7));
        let bytes = encode(&msg);
        let decoded = decode(&BytesMut::from(&bytes[..])).unwrap();
        match decoded {
            WireMsg::Hello(id) => assert_eq!(id, NodeId(7)),
            _ => panic!("expected Hello"),
        }
    }

    #[test]
    fn app_message_round_trips() {
        let req = RecordRequest {
            slot: 3,
            req_step: 4,
            proposal: Proposal {
                value: Command::Put {
                    client: ClientId(1),
                    seq: 2,
                    key: 5,
                    value: 42,
                },
                priority: 99,
                origin: NodeId(0),
            },
        };
        let msg = WireMsg::App(ConcreteMsg::Request(req.clone()));
        let bytes = encode(&msg);
        let decoded = decode(&BytesMut::from(&bytes[..])).unwrap();
        match decoded {
            WireMsg::App(ConcreteMsg::Request(got)) => assert_eq!(got, req),
            _ => panic!("expected App(Request)"),
        }
    }
}
