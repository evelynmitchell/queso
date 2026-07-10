//! The KV "hello world" application's command language, plus the minimal
//! client-session concept (A6) idempotency depends on.
//!
//! Every client-visible operation is tagged `(client, seq)`. A6 defines a
//! "session" as the scope over which a client's `seq` is monotonic; the
//! [`ClientSession`] helper here is deliberately the smallest thing that can
//! satisfy that -- a stable id plus a monotonic counter -- since Stage 4a
//! has no notion of multiple concurrent sessions per client, reconnection,
//! or session expiry (out of scope; see the crate docs).

use queso_sim::ids::NodeId;

/// A key in the KV store. Kept small/`Copy` on purpose -- this is a "hello
/// world" application (O2), not a general-purpose database.
pub type Key = u32;

/// A value in the KV store.
pub type Value = i64;

/// A stable client identity (A6). Distinct from [`NodeId`]: a client is not
/// a replica, and many clients may submit through the same replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClientId(pub u32);

/// The smallest usable client-session concept satisfying A6: a stable
/// [`ClientId`] plus a strictly monotonic `seq` counter, one per logical
/// client. A client (or a test driving one) constructs a single
/// `ClientSession` and calls [`ClientSession::next_seq`] once per operation
/// it issues -- including retries of the *same* logical operation, which
/// must reuse the seq the first attempt used (that reuse, not a fresh
/// `next_seq` call, is what makes a retry deduplicate via P8a; see
/// `crate::kv::Kv::apply`).
///
/// # Precondition this dedup relies on (A6)
///
/// [`crate::kv::Kv::apply`]'s dedup logic -- drop any `Put` whose `seq` is
/// not strictly greater than the highest `seq` already applied for that
/// client -- is sound **only** if every real client honors both halves of
/// this contract:
///
/// 1. `seq`s are issued in strictly increasing order (never reused except
///    for an exact retry of the same logical operation, and never skipped
///    backwards), and
/// 2. the client has **at most one** operation in flight at a time (it
///    waits for a response -- or gives up -- before issuing the next
///    `seq`).
///
/// Violate either one -- e.g. a client that pipelines two writes and the
/// higher `seq` happens to get applied first -- and the dedup check cannot
/// tell "a stale retry of an old operation" apart from "a distinct write
/// that just hasn't been applied yet": it will silently and permanently
/// drop the lower-`seq` write as a false duplicate, losing a real write
/// rather than merely papering over a harmless resend. `ClientSession` is
/// the smallest thing that can honor (1); Stage 4a has no concurrent/
/// multi-session client, so (2) is left to callers to uphold by
/// construction (see the crate docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientSession {
    id: ClientId,
    next: u64,
}

impl ClientSession {
    /// A fresh session for `id`, with `seq` starting at 0.
    pub fn new(id: ClientId) -> Self {
        Self { id, next: 0 }
    }

    /// This session's client id.
    pub fn id(&self) -> ClientId {
        self.id
    }

    /// Allocate the next `seq` in this session, monotonically increasing.
    /// Call this once per *new* logical operation -- not once per retry
    /// attempt of the same operation (see the struct docs).
    pub fn next_seq(&mut self) -> u64 {
        let s = self.next;
        self.next += 1;
        s
    }
}

/// One command submitted into the replicated log. `Put` mutates the KV
/// store; `Get` never mutates, but per Meerkat's reads-through-log design
/// (see `crate::cluster`'s module docs) it is still proposed as a log event
/// like any other command, so that its position in the log fixes its
/// linearization point.
///
/// `Ord`/`Eq` are derived rather than hand-written: the derived order has no
/// semantic meaning of its own (unlike
/// `queso_consensus::proposal::Proposal`'s hand-written, priority-first
/// `Ord`) and exists only because [`queso_consensus::proposer::Proposer`]
/// requires `V: Ord` as a tie-break of last resort within `Proposal<V>`'s
/// own ordering (priority, then origin, then value) -- see that type's
/// docs. Structural equality (`==`) *is* semantically meaningful here: it is
/// how the replicated-log driver recognizes "the command that was just
/// decided for this slot is the one I submitted" (see
/// `crate::replica::ReplicaState`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Command {
    Put {
        client: ClientId,
        seq: u64,
        key: Key,
        value: Value,
    },
    Get {
        client: ClientId,
        seq: u64,
        key: Key,
    },
}

impl Command {
    /// The `(client, seq)` pair this command is tagged with (A6), used for
    /// idempotent deduplication (P8a).
    pub fn client_seq(&self) -> (ClientId, u64) {
        match self {
            Command::Put { client, seq, .. } | Command::Get { client, seq, .. } => (*client, *seq),
        }
    }

    /// The key this command reads or writes.
    pub fn key(&self) -> Key {
        match self {
            Command::Put { key, .. } | Command::Get { key, .. } => *key,
        }
    }
}

/// A minimal marker of "which replica a client talked to" -- not part of
/// [`Command`] itself (the log/consensus layer doesn't care), but useful for
/// test/demo code that wants to remember where it submitted something.
pub type Replica = NodeId;
