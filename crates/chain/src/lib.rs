//! `queso-chain`: the Chain-of-Blocks state machine, `(n, h)`.
//!
//! # Why this is its own crate
//!
//! Two very different things must compute *byte-identical* chain hashes:
//! the **node** (`queso-net`, folding the chain as it applies commands and
//! exposing checkpoints over `GET /chain`) and the **harness**
//! (`queso-conformance`, folding the same chain to check replicas against
//! each other). If they ever disagree on the encoding, every comparison
//! silently fails to line up and a conformance run reports nothing while
//! looking healthy.
//!
//! `queso-net` cannot depend on `queso-conformance` -- that would put test
//! harness code inside the production binary -- so the shared definition
//! lives here, in a leaf crate that depends only on `queso-smr` for
//! [`Command`]. Phase 9.1 (#55) originally defined this inside
//! `queso-conformance`; Phase 9.2 (#56) moved it out unchanged.
//!
//! Antithesis's [Chain-of-Blocks][cob] workload is the simplest
//! state-machine-replication conformance test that makes a total-order
//! violation *unmistakable*. Each replica keeps a pair:
//!
//! - `n` -- how many commands it has applied, and
//! - `h` -- a running hash: on applying command `C`, `n += 1` and
//!   `h = hash(h ‖ C)`.
//!
//! Because `h` is a *chain*, any single disagreement in the applied command
//! sequence propagates forward forever: two replicas that applied different
//! commands at slot 5 have different `h` at `n = 6`, and at `n = 7`, and at
//! every `n` after. That is what makes the property checkable under
//! **imperfect observability** -- an observer that never manages to sample
//! both replicas at `n = 6` still catches them at any later `n` they happen
//! to share. See `queso_conformance::observer` for the detection side of
//! that, and `queso_conformance::source::Observability` for how the
//! sampling density is varied deliberately in tests.
//!
//! # How a CoB command maps onto Queso
//!
//! Issue #55 offers two options and prefers (a): carry the command bytes'
//! hash as the value and keep the running chain in the observer, rather
//! than teaching the verified `queso-smr` core a new command variant.
//! That is what this module does -- **nothing in `queso-consensus` or
//! `queso-smr` changes**. A CoB "random byte-array command" becomes a
//! `Command::Put` whose value is a 64-bit digest of the payload
//! (`queso_conformance::workload`), and the chain is folded outside the
//! consensus core, from the command sequence a replica actually applied --
//! by the harness in 9.1, and additionally by the node itself in 9.2.
//!
//! A consequence worth stating plainly: the chain hashes the *decided
//! command sequence*, not the replicas' KV state. That is the stronger of
//! the two for this purpose -- P6 total order is a statement about the
//! sequence, and two replicas can hold equal KV maps having applied
//! genuinely different sequences (e.g. reordered writes to different keys),
//! which a state hash would miss and this chain catches.
//!
//! # Hash strength (honest limitation)
//!
//! [`extend`] is FNV-1a with a SplitMix64-style finalizer -- fast,
//! dependency-free, and well-mixed, but **not cryptographic**. It is sized
//! for the job it has: detecting divergence that arises by *accident*
//! (a bug in catch-up, recovery, or the transport), where two differing
//! sequences colliding at 64 bits is vanishingly unlikely. It is *not*
//! collision-resistant against an adversary who gets to choose command
//! payloads in order to force a collision, and this harness does not claim
//! to defend against one.
//!
//! [cob]: https://antithesis.com/docs/resources/chain-of-blocks/

use queso_smr::Command;

/// A Chain-of-Blocks running hash -- the `h` half of `(n, h)`.
pub type BlockHash = u64;

/// The chain's starting value, before any command has been applied: the
/// FNV-1a 64-bit offset basis.
pub const GENESIS: BlockHash = 0xcbf2_9ce4_8422_2325;

const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// FNV-1a over `bytes`, continuing from accumulator `acc`.
fn fnv1a(acc: u64, bytes: &[u8]) -> u64 {
    let mut h = acc;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(FNV_PRIME);
    }
    h
}

/// SplitMix64's finalizer -- avalanches the FNV accumulator so that two
/// command sequences differing in one low bit produce hashes differing all
/// over, which keeps the "different sequence => different `h`" property
/// robust for short chains and small commands.
fn mix(x: u64) -> u64 {
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// The canonical byte encoding of a command, used only for hashing.
///
/// Deliberately hand-rolled rather than reusing `queso-net`'s bincode wire
/// format: this must be stable for as long as recorded chain hashes are
/// compared against each other, and it must not silently change if the
/// wire format is ever renegotiated. Every field is length-fixed and
/// little-endian, and the leading tag byte keeps `Put`/`Get` disjoint.
fn encode(command: &Command) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(25);
    match command {
        Command::Put {
            client,
            seq,
            key,
            value,
        } => {
            bytes.push(0);
            bytes.extend_from_slice(&client.0.to_le_bytes());
            bytes.extend_from_slice(&seq.to_le_bytes());
            bytes.extend_from_slice(&key.to_le_bytes());
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        Command::Get { client, seq, key } => {
            bytes.push(1);
            bytes.extend_from_slice(&client.0.to_le_bytes());
            bytes.extend_from_slice(&seq.to_le_bytes());
            bytes.extend_from_slice(&key.to_le_bytes());
        }
    }
    bytes
}

/// A single command's digest, independent of chain position -- what the
/// per-transition log records so a human reading a divergence report can
/// see *which* command differed, not just that the chain broke.
pub fn command_digest(command: &Command) -> u64 {
    mix(fnv1a(GENESIS, &encode(command)))
}

/// Extend a chain by one command: `h' = hash(h ‖ C)`.
pub fn extend(h: BlockHash, command: &Command) -> BlockHash {
    let acc = fnv1a(GENESIS, &h.to_le_bytes());
    mix(fnv1a(acc, &encode(command)))
}

/// One replica's Chain-of-Blocks state: `n` commands applied, running hash
/// `h`.
///
/// `Ord` follows `n` first, so a collection of states sorts into
/// chain order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ChainState {
    /// How many commands have been applied. Equals the replica's log
    /// frontier (`next_slot`) -- P7 guarantees application is gap-free, so
    /// "applied count" and "frontier" are the same number.
    pub n: u64,
    /// The running hash over exactly those `n` commands, in order.
    pub h: BlockHash,
}

impl ChainState {
    /// The state before anything has been applied: `(0, GENESIS)`.
    pub fn genesis() -> Self {
        Self { n: 0, h: GENESIS }
    }

    /// Apply one command, returning the [`Transition`] it produced.
    pub fn apply(&mut self, command: &Command) -> Transition {
        let before = *self;
        self.n += 1;
        self.h = extend(before.h, command);
        Transition {
            before,
            after: *self,
            command_digest: command_digest(command),
        }
    }

    /// Fold a whole command sequence from genesis, returning the state
    /// after the last command.
    pub fn from_log(commands: &[Command]) -> Self {
        let mut state = Self::genesis();
        for command in commands {
            state.apply(command);
        }
        state
    }

    /// Every prefix state of `commands`, from `(0, GENESIS)` up to and
    /// including the state after the last command -- i.e. `commands.len() +
    /// 1` states.
    ///
    /// This is what a fully-observable source
    /// (`queso_conformance::source`) emits: the
    /// entire `n -> h` table for a replica, so two replicas can be compared
    /// at every `n` they share rather than only at whatever frontier they
    /// happened to be sampled at.
    pub fn prefixes(commands: &[Command]) -> Vec<Self> {
        let mut state = Self::genesis();
        let mut out = Vec::with_capacity(commands.len() + 1);
        out.push(state);
        for command in commands {
            state.apply(command);
            out.push(state);
        }
        out
    }
}

/// One applied command, recorded as `state_before => state_after` -- the
/// per-transition log the CoB doc shows, kept so a detected divergence is
/// root-causable rather than just a failed assertion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Transition {
    /// The chain state before this command was applied.
    pub before: ChainState,
    /// The chain state after it was applied. `after.n == before.n + 1`.
    pub after: ChainState,
    /// [`command_digest`] of the command applied.
    pub command_digest: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use queso_smr::ClientId;

    fn put(client: u32, seq: u64, key: u32, value: i64) -> Command {
        Command::Put {
            client: ClientId(client),
            seq,
            key,
            value,
        }
    }

    #[test]
    fn the_chain_is_order_sensitive() {
        let a = put(1, 1, 0, 10);
        let b = put(1, 2, 0, 20);
        assert_ne!(
            ChainState::from_log(&[a.clone(), b.clone()]),
            ChainState::from_log(&[b, a]),
            "two orderings of the same commands must not produce the same chain -- \
             order-insensitivity here would make the whole observer vacuous"
        );
    }

    #[test]
    fn a_single_differing_command_propagates_forward_forever() {
        let common = vec![put(1, 1, 0, 10), put(1, 2, 0, 20)];
        let mut left = common.clone();
        let mut right = common;
        left.push(put(2, 1, 0, 30));
        right.push(put(2, 1, 0, 31)); // one bit of value differs
        for i in 0..8 {
            let filler = put(3, i, 0, i as i64);
            left.push(filler.clone());
            right.push(filler);
        }

        let left_states = ChainState::prefixes(&left);
        let right_states = ChainState::prefixes(&right);
        // Equal up to the divergence point...
        for n in 0..=2 {
            assert_eq!(left_states[n], right_states[n], "prefix {n} must agree");
        }
        // ...and different at every n after it. This is the property the
        // observer relies on to catch divergence it never sampled directly.
        for n in 3..left_states.len() {
            assert_ne!(
                left_states[n].h, right_states[n].h,
                "divergence at n=3 must still be visible at n={n}"
            );
        }
    }

    #[test]
    fn get_and_put_with_identical_fields_hash_differently() {
        let g = Command::Get {
            client: ClientId(1),
            seq: 7,
            key: 3,
        };
        let p = put(1, 7, 3, 0);
        assert_ne!(command_digest(&g), command_digest(&p));
    }

    #[test]
    fn from_log_agrees_with_the_last_prefix() {
        let log = vec![put(1, 1, 0, 10), put(1, 2, 1, 20), put(2, 1, 0, -5)];
        assert_eq!(
            ChainState::from_log(&log),
            *ChainState::prefixes(&log).last().expect("non-empty")
        );
    }

    #[test]
    fn genesis_is_the_empty_log() {
        assert_eq!(ChainState::from_log(&[]), ChainState::genesis());
        assert_eq!(ChainState::prefixes(&[]), vec![ChainState::genesis()]);
    }
}
