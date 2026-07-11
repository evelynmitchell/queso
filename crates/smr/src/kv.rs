//! The in-memory KV state machine: the deterministic function from "a
//! command" to "the next state, plus whatever a `Get` observed", applied
//! once per decided log slot, in slot order (P6/P7).
//!
//! This is *also* the sequential reference specification the
//! [`crate::linearizability`] checker replays candidate operation orderings
//! against -- see that module's docs for why reusing the exact same type
//! (idempotent dedup included) rather than a "naive" KV model is the
//! correct choice for checking P8 together with P8a.

use std::collections::BTreeMap;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::command::{ClientId, Command, Key, Value};

/// What applying one [`Command`] produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Applied {
    /// A `Put` whose `(client, seq)` had not been seen before: the mutation
    /// took effect.
    PutNew,
    /// A `Put` whose `(client, seq)` had already been applied at an earlier
    /// slot (P8a): deduplicated. This slot is "wasted" (its command has no
    /// further effect) but harmless -- exactly what idempotency promises.
    PutDuplicate,
    /// A `Get`: never mutates state; carries the value observed (`None` if
    /// the key has never been written).
    Get(Option<Value>),
}

impl Applied {
    /// The value a `Get` observed, if this was a `Get`.
    pub fn get_value(self) -> Option<Value> {
        match self {
            Applied::Get(v) => v,
            _ => None,
        }
    }
}

/// The KV store's full state: the map itself, plus the per-client
/// dedup table `last_seq` (A6/P8a) -- the highest `seq` applied for each
/// client so far. A `Put` with `seq <= last_seq[client]` is a duplicate
/// (covers both an exact retry and any older, since-superseded stale
/// resend arriving late).
///
/// `Ord`/`Eq` are derived so a whole `Kv` can be used as a search-state key
/// (see [`crate::linearizability`]) -- both fields are `BTreeMap`s, so this
/// stays a total order with no incidental (hash-based) nondeterminism.
///
/// `Serialize`/`Deserialize` (feature-gated, bookkeeping only) let a real
/// driver persist this as part of [`crate::replica::Durable`] -- see that
/// type's docs.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Kv {
    map: BTreeMap<Key, Value>,
    last_seq: BTreeMap<ClientId, u64>,
}

impl Kv {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply one command, in log order. This is the *only* way `Kv` state
    /// ever changes -- there is no other mutator, which is what makes gap-
    /// free, in-order application (P7) sufficient to guarantee every
    /// replica that has applied the same log prefix is in the same state
    /// (P5/P6 projected onto the application layer).
    pub fn apply(&mut self, cmd: &Command) -> Applied {
        match cmd {
            Command::Put {
                client,
                seq,
                key,
                value,
            } => {
                // SOUNDNESS PRECONDITION (A6): this dedup test -- "drop any
                // `seq` that is not strictly greater than the highest `seq`
                // this client has ever had applied" -- is only correct
                // under the single-in-flight-per-client contract: a client
                // issues `seq`s in strictly increasing order and never has
                // more than one operation in flight at a time (see
                // `crate::command::ClientSession`'s docs). If a client ever
                // violated that (e.g. pipelined two writes and the higher
                // `seq` happened to apply first), this check would silently
                // and permanently drop the lower-`seq` write as a spurious
                // "duplicate" even though it was a distinct, never-applied
                // command -- a real write lost, not idempotency. Stage 4a's
                // client model guarantees the precondition holds; nothing
                // here re-checks it at runtime.
                let is_duplicate = self.last_seq.get(client).is_some_and(|&s| s >= *seq);
                if is_duplicate {
                    Applied::PutDuplicate
                } else {
                    self.map.insert(*key, *value);
                    self.last_seq.insert(*client, *seq);
                    Applied::PutNew
                }
            }
            Command::Get { key, .. } => Applied::Get(self.map.get(key).copied()),
        }
    }

    /// Read `key` without recording any command -- a pure, non-mutating
    /// peek at the current reference state. Used by the checker to compute
    /// what a `Get` *should* have observed at a given point in a candidate
    /// linearization, and by tests inspecting a replica's local state
    /// directly (see `crate::cluster::SmrCluster::kv_snapshot`).
    pub fn get(&self, key: Key) -> Option<Value> {
        self.map.get(&key).copied()
    }

    /// A full snapshot of the current map contents.
    pub fn snapshot(&self) -> BTreeMap<Key, Value> {
        self.map.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(client: u32, seq: u64, key: Key, value: Value) -> Command {
        Command::Put {
            client: ClientId(client),
            seq,
            key,
            value,
        }
    }

    fn get(client: u32, seq: u64, key: Key) -> Command {
        Command::Get {
            client: ClientId(client),
            seq,
            key,
        }
    }

    #[test]
    fn put_then_get_sees_the_write() {
        let mut kv = Kv::new();
        assert_eq!(kv.apply(&put(1, 0, 10, 100)), Applied::PutNew);
        assert_eq!(kv.apply(&get(2, 0, 10)), Applied::Get(Some(100)));
    }

    #[test]
    fn get_of_unwritten_key_is_none() {
        let mut kv = Kv::new();
        assert_eq!(kv.apply(&get(1, 0, 99)), Applied::Get(None));
    }

    #[test]
    fn duplicate_seq_is_deduplicated() {
        let mut kv = Kv::new();
        assert_eq!(kv.apply(&put(1, 5, 10, 100)), Applied::PutNew);
        assert_eq!(kv.apply(&put(1, 5, 10, 999)), Applied::PutDuplicate);
        assert_eq!(kv.get(10), Some(100), "duplicate must not overwrite");
    }

    #[test]
    fn stale_duplicate_does_not_clobber_a_later_write() {
        // The scenario P8a exists for: a stale retry of an *old* seq
        // arriving after a *newer* seq from the same client has already
        // landed must not regress the value.
        let mut kv = Kv::new();
        kv.apply(&put(1, 1, 10, 100));
        kv.apply(&put(1, 2, 10, 200));
        assert_eq!(kv.apply(&put(1, 1, 10, 100)), Applied::PutDuplicate);
        assert_eq!(kv.get(10), Some(200), "must still reflect the newer write");
    }

    #[test]
    fn different_clients_do_not_dedup_each_other() {
        let mut kv = Kv::new();
        assert_eq!(kv.apply(&put(1, 0, 10, 100)), Applied::PutNew);
        assert_eq!(kv.apply(&put(2, 0, 10, 200)), Applied::PutNew);
        assert_eq!(kv.get(10), Some(200));
    }
}
