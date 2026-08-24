//! Phase 9.2 (issue #56): the node-side Chain-of-Blocks observability hook.
//!
//! A conformance run (`queso-conformance`) checks state-machine-replication
//! safety by comparing replicas' `(n, h)` chain states: `n` commands
//! applied, `h` a running hash over exactly that command sequence. Against
//! an *in-process* cluster the harness can read every replica's applied log
//! and fold the chain itself. Against **real `queso-node` processes** it
//! cannot -- nothing crosses the process boundary but this crate's status
//! endpoint -- so the node has to fold its own chain and expose it.
//!
//! # Why checkpoints, and not just the frontier
//!
//! `/metrics` already reports `next_slot`, so the obvious design is "poll
//! each replica's frontier and compare". That does not work, and 9.1
//! measured how badly: replicas lag each other by design (P5 permits it),
//! so two frontier samples almost never land on the same `n`, the observer
//! has nothing to compare, and a long clean-looking run ends having checked
//! almost nothing -- 2 cross-replica comparisons versus 20 for checkpointed
//! sampling on the same workload (see `queso-conformance`'s
//! `tests/imperfect_observability.rs`).
//!
//! So this module reports `h` at **fixed `n` boundaries**: every replica
//! publishes its hash at `n = every, 2*every, 3*every, ...`. Comparisons
//! then align by construction rather than by luck, whatever order replicas
//! happen to reach those points in.
//!
//! **The spacing is a cluster-wide constant, not a per-node knob.** Two
//! replicas configured with different spacings publish at disjoint `n`
//! values and are never comparable -- the same vacuous verdict by another
//! route. [`ChainCheckpoints::every`] is exposed on `/chain` precisely so a
//! harness can check that every replica agrees before trusting a run.
//!
//! # Opt-in, and what it costs when on
//!
//! Off unless `NodeConfig::chain_checkpoints` is `Some` (and the status
//! listener is bound at all) -- a production `queso-node` pays nothing.
//! When on, the cost is one hash per applied command plus a bounded ring of
//! checkpoints; the fold runs on the driver's own task, right where it
//! already publishes status, and never touches the consensus path.
//!
//! # Restart
//!
//! The fold is volatile: a restarted process starts at `(0, GENESIS)` and
//! re-folds from slot 0 on its first pass, which is sound because the
//! applied log itself is durable (`queso_smr::Durable`). That costs
//! `O(log length)` once at boot and makes the replica's historical
//! checkpoints available immediately rather than only from the restart
//! point onward -- which matters, since a harness comparing a restarted
//! replica against a long-running one needs overlapping `n` values.
//!
//! This is safe only while the whole applied log stays resident. Log
//! compaction is deliberately deferred (Phase 8.1c, see issue #46); if it
//! lands, this refold needs a snapshot base to start from.

use std::collections::VecDeque;
use std::sync::Mutex;

use queso_chain::ChainState;
use queso_smr::SmrNode;

/// How many checkpoints a replica retains for reading over `/chain`.
///
/// Bounded so a long soak cannot grow this without limit. Once full, the
/// oldest checkpoint is dropped and `/chain` reports `truncated: true`
/// rather than quietly serving a partial history that a reader would
/// mistake for the whole one.
pub const CHECKPOINT_RING_CAPACITY: usize = 256;

/// The published side of the hook: what `GET /chain` serves.
///
/// # Concurrency
///
/// `crate::status::StatusShared` is otherwise deliberately all atomics --
/// see its docs for why "latest known status" wants no locks. This type is
/// the exception, and **everything it publishes lives under one `Mutex`**:
/// the frontier, the checkpoint table, and the truncation flag together.
///
/// An earlier version kept the frontier lock-free, as two `AtomicU64`s, on
/// the reasoning that "where is this replica now" should never contend with
/// the driver. That reasoning was wrong, and issue #73 is what it cost.
/// `(n, h)` is a *pair*: a hash is only meaningful attached to the height
/// it was computed at. Two independent stores mean a reader on the status
/// task can land between them and observe a hash from one chain position
/// wearing another position's height -- which is, to the Chain-of-Blocks
/// observer, indistinguishable from a genuine safety violation. The
/// nightly soak duly reported one. `a_concurrent_reader_never_observes_a_
/// torn_frontier` finds the tear within a few dozen writes when the two
/// atomics are restored, so this was never a narrow window.
///
/// Reading under the lock also makes a `/chain` response one instant's
/// view rather than two. Previously `to_json` took the frontier lock-free
/// and the table under the lock, so a response could pair a frontier from
/// one fold pass with a table from the next.
///
/// What that does *not* buy, and must not be assumed: the frontier is not
/// guaranteed to be at least the newest checkpoint. [`ChainFolder::fold`]
/// records crossings as it walks the batch and publishes the frontier only
/// at the end, so a reader mid-pass legitimately sees a table entry beyond
/// the frontier. That is harmless -- every `(n, h)` is still a pair that
/// was really computed together, which is the only property the observer
/// needs -- and closing it would mean holding the lock across a whole fold.
///
/// The cost is nil in practice: the driver takes the lock once per fold
/// pass, the handler once per request, and the critical section is a
/// `push_back` on a bounded deque.
#[derive(Debug)]
pub struct ChainCheckpoints {
    /// Checkpoint spacing in slots. See the module docs: cluster-wide.
    /// Immutable after construction, so it needs no synchronization.
    every: u64,
    /// Everything a reader can observe, behind one lock so it is always
    /// observed as a consistent whole.
    published: Mutex<Published>,
}

/// The mutable half of [`ChainCheckpoints`]. Never handed out by
/// reference -- callers get copies taken under the lock.
#[derive(Debug)]
struct Published {
    /// The replica's current chain position, updated on every fold.
    frontier: ChainState,
    /// `(n, h)` for each crossed checkpoint, oldest first.
    table: VecDeque<(u64, u64)>,
    /// Whether any checkpoint has been dropped to stay within
    /// [`CHECKPOINT_RING_CAPACITY`].
    truncated: bool,
}

impl ChainCheckpoints {
    /// A fresh, empty checkpoint table with the given spacing. A spacing of
    /// `0` is treated as `1` (every slot is a checkpoint).
    pub fn new(every: u64) -> Self {
        Self {
            every: every.max(1),
            published: Mutex::new(Published {
                frontier: ChainState::genesis(),
                table: VecDeque::new(),
                truncated: false,
            }),
        }
    }

    /// The published state, under the lock. A poisoned lock is recovered
    /// rather than propagated: this is observability, and a panic in one
    /// handler task must not take the endpoint down for every later
    /// request.
    fn published(&self) -> std::sync::MutexGuard<'_, Published> {
        self.published.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// This node's checkpoint spacing, so a harness can verify every replica
    /// in the cluster is publishing at the same `n` values.
    pub fn every(&self) -> u64 {
        self.every
    }

    /// Record a crossed checkpoint, evicting the oldest if the ring is full.
    fn record(&self, n: u64, h: u64) {
        let mut published = self.published();
        if published.table.len() == CHECKPOINT_RING_CAPACITY {
            published.table.pop_front();
            published.truncated = true;
        }
        published.table.push_back((n, h));
    }

    /// Publish the replica's current chain position.
    ///
    /// One store of the whole pair, not one per field -- see this type's
    /// "Concurrency" docs and issue #73.
    fn set_frontier(&self, state: ChainState) {
        self.published().frontier = state;
    }

    /// The replica's current chain position.
    pub fn frontier(&self) -> ChainState {
        self.published().frontier
    }

    /// Every retained checkpoint, oldest first, plus whether any older ones
    /// were dropped.
    pub fn checkpoints(&self) -> (Vec<(u64, u64)>, bool) {
        let published = self.published();
        (
            published.table.iter().copied().collect(),
            published.truncated,
        )
    }

    /// The frontier and the checkpoint table as they stood at one instant.
    ///
    /// Separate `frontier()` + `checkpoints()` calls take the lock twice
    /// and can straddle a fold, pairing one pass's frontier with the next
    /// pass's table. Callers serving a single response want this instead.
    fn snapshot(&self) -> (ChainState, Vec<(u64, u64)>, bool) {
        let published = self.published();
        (
            published.frontier,
            published.table.iter().copied().collect(),
            published.truncated,
        )
    }

    /// `GET /chain`'s JSON body.
    ///
    /// Hashes are hex strings, not JSON numbers: they use the full 64-bit
    /// range, which does not survive a reader that parses JSON numbers as
    /// IEEE doubles. The harness is Rust and would be fine either way, but
    /// an endpoint that silently rounds for `curl | jq` users is a trap.
    pub fn to_json(&self) -> String {
        let (frontier, checkpoints, truncated) = self.snapshot();

        let mut out = String::from("{\n");
        out.push_str(&format!("  \"checkpoint_every\": {},\n", self.every));
        out.push_str(&format!(
            "  \"frontier\": {{ \"n\": {}, \"h\": \"{:#018x}\" }},\n",
            frontier.n, frontier.h
        ));
        out.push_str(&format!("  \"truncated\": {truncated},\n"));
        out.push_str("  \"checkpoints\": [");
        for (i, (n, h)) in checkpoints.iter().enumerate() {
            out.push_str(if i == 0 { "\n" } else { ",\n" });
            out.push_str(&format!("    {{ \"n\": {n}, \"h\": \"{h:#018x}\" }}"));
        }
        if !checkpoints.is_empty() {
            out.push('\n');
            out.push_str("  ");
        }
        out.push_str("]\n}\n");
        out
    }
}

/// A parsed `GET /chain` body: what a conformance observer needs from one
/// replica.
///
/// Lives next to [`ChainCheckpoints::to_json`], the code that *produces*
/// this format, on purpose. The two halves are a private wire contract
/// between the node and every harness that watches it, and a parser living
/// in one of those harnesses could drift from the producer silently -- the
/// same failure mode that put the hash chain itself in its own `queso-chain`
/// crate rather than one copy per side. `chain_json_round_trips` is the
/// guard: change the encoding and it fails here, not in a soak whose
/// comparison count quietly drops to zero.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainReport {
    /// The replica's furthest `(n, h)`, reported every poll.
    pub frontier: ChainState,
    /// Retained checkpoints, oldest first.
    pub checkpoints: Vec<(u64, u64)>,
}

/// Hashes travel as hex strings so a 64-bit value survives any JSON reader
/// (a plain number would lose precision in one that parses to `f64`).
fn parse_hash(value: &serde_json::Value) -> Option<u64> {
    let text = value.as_str()?;
    u64::from_str_radix(text.strip_prefix("0x").unwrap_or(text), 16).ok()
}

/// Parse a `GET /chain` body. `None` for anything malformed -- a caller
/// polling a replica that is rebooting, or reachable but not serving,
/// treats "no report" as an ordinary condition rather than an error.
pub fn parse_chain(body: &str) -> Option<ChainReport> {
    let json: serde_json::Value = serde_json::from_str(body).ok()?;
    let frontier = ChainState {
        n: json["frontier"]["n"].as_u64()?,
        h: parse_hash(&json["frontier"]["h"])?,
    };
    let checkpoints = json["checkpoints"]
        .as_array()?
        .iter()
        .filter_map(|entry| Some((entry["n"].as_u64()?, parse_hash(&entry["h"])?)))
        .collect();
    Some(ChainReport {
        frontier,
        checkpoints,
    })
}

/// The driver-side half: folds newly-applied commands into the chain and
/// publishes crossings into a [`ChainCheckpoints`].
///
/// Holds only a cursor (`state.n` is the next slot it has *not* folded) and
/// the running hash, so each pass costs one `applied_from` range read plus
/// one hash per new command -- not a rescan of the log.
#[derive(Debug, Clone)]
pub struct ChainFolder {
    state: ChainState,
    every: u64,
}

impl ChainFolder {
    /// A folder starting from genesis. Its first [`Self::fold`] therefore
    /// replays whatever the replica has already applied -- see the module
    /// docs' "Restart".
    pub fn new(every: u64) -> Self {
        Self {
            state: ChainState::genesis(),
            every: every.max(1),
        }
    }

    /// The chain state folded so far.
    pub fn state(&self) -> ChainState {
        self.state
    }

    /// Fold everything `node` has applied since the last call, recording any
    /// checkpoints crossed and republishing the frontier.
    ///
    /// Called from the driver's own task, immediately after the batch that
    /// applied those commands has been persisted -- so a checkpoint is only
    /// ever published for state this replica has already made durable.
    pub fn fold(&mut self, node: &SmrNode, sink: &ChainCheckpoints) {
        for command in node.applied_from(self.state.n) {
            self.state.apply(&command);
            if self.state.n.is_multiple_of(self.every) {
                sink.record(self.state.n, self.state.h);
            }
        }
        sink.set_frontier(self.state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use queso_smr::{ClientId, Command};

    fn put(seq: u64, value: i64) -> Command {
        Command::Put {
            client: ClientId(0),
            seq,
            key: 0,
            value,
        }
    }

    /// Fold a command sequence through a folder without needing a live
    /// `SmrNode` -- the node-driven path is covered by `tests/chain.rs`.
    fn fold_all(folder: &mut ChainFolder, sink: &ChainCheckpoints, commands: &[Command]) {
        for command in commands {
            folder.state.apply(command);
            if folder.state.n.is_multiple_of(folder.every) {
                sink.record(folder.state.n, folder.state.h);
            }
        }
        sink.set_frontier(folder.state);
    }

    /// Every `(n, h)` a reader observes must be a pair that was actually
    /// published. `/chain` is served from the status task while the driver
    /// task folds, so this is a genuine concurrent read, not a theoretical
    /// one.
    ///
    /// Writes maintain the invariant `h == n * STRIDE`, so any observed
    /// pair violating it is torn: a hash from one chain position wearing
    /// another position's height. That is indistinguishable, to the
    /// Chain-of-Blocks observer, from a real divergence.
    ///
    /// Falsifier: with the frontier held as two independent atomics this
    /// fails within a fraction of a second.
    #[test]
    fn a_concurrent_reader_never_observes_a_torn_frontier() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::Arc;

        const STRIDE: u64 = 0x9E37_79B9_7F4A_7C15;
        const WRITES: u64 = 2_000_000;

        let chain = Arc::new(ChainCheckpoints::new(1));
        let done = Arc::new(AtomicBool::new(false));

        let writer = {
            let chain = Arc::clone(&chain);
            let done = Arc::clone(&done);
            std::thread::spawn(move || {
                for n in 1..=WRITES {
                    chain.set_frontier(ChainState {
                        n,
                        h: n.wrapping_mul(STRIDE),
                    });
                }
                done.store(true, Ordering::Relaxed);
            })
        };

        let mut torn: Option<ChainState> = None;
        while !done.load(Ordering::Relaxed) && torn.is_none() {
            let seen = chain.frontier();
            if seen.n != 0 && seen.h != seen.n.wrapping_mul(STRIDE) {
                torn = Some(seen);
            }
        }
        writer.join().expect("writer thread");

        assert!(
            torn.is_none(),
            "observed a torn frontier: n={} carried h={:#018x}, but the pair \
             published at that n was h={:#018x}. A reader must never see a \
             hash from one chain position labelled with another's height.",
            torn.unwrap().n,
            torn.unwrap().h,
            torn.unwrap().n.wrapping_mul(STRIDE),
        );
    }

    #[test]
    fn checkpoints_land_exactly_on_multiples_of_the_spacing() {
        let sink = ChainCheckpoints::new(4);
        let mut folder = ChainFolder::new(4);
        let commands: Vec<Command> = (0..10).map(|i| put(i, i as i64)).collect();
        fold_all(&mut folder, &sink, &commands);

        let (checkpoints, truncated) = sink.checkpoints();
        assert_eq!(
            checkpoints.iter().map(|(n, _)| *n).collect::<Vec<_>>(),
            vec![4, 8],
            "only multiples of the spacing may be published"
        );
        assert!(!truncated);
        assert_eq!(sink.frontier().n, 10, "the frontier tracks every command");
    }

    #[test]
    fn a_published_checkpoint_matches_the_chain_at_that_n() {
        let sink = ChainCheckpoints::new(2);
        let mut folder = ChainFolder::new(2);
        let commands: Vec<Command> = (0..6).map(|i| put(i, i as i64 * 7)).collect();
        fold_all(&mut folder, &sink, &commands);

        // Ground truth: fold the same prefix independently.
        let (checkpoints, _) = sink.checkpoints();
        for (n, h) in checkpoints {
            let expected = ChainState::from_log(&commands[..n as usize]);
            assert_eq!(
                (n, h),
                (expected.n, expected.h),
                "checkpoint at n={n} must equal the chain over the first {n} commands"
            );
        }
    }

    #[test]
    fn the_ring_is_bounded_and_says_so_when_it_drops() {
        let sink = ChainCheckpoints::new(1);
        let mut folder = ChainFolder::new(1);
        let commands: Vec<Command> = (0..(CHECKPOINT_RING_CAPACITY as u64 + 10))
            .map(|i| put(i, i as i64))
            .collect();
        fold_all(&mut folder, &sink, &commands);

        let (checkpoints, truncated) = sink.checkpoints();
        assert_eq!(checkpoints.len(), CHECKPOINT_RING_CAPACITY);
        assert!(truncated, "dropping history must be disclosed, not silent");
        assert_eq!(
            checkpoints.last().map(|(n, _)| *n),
            Some(CHECKPOINT_RING_CAPACITY as u64 + 10),
            "the newest checkpoint is always retained"
        );
    }

    #[test]
    fn a_spacing_of_zero_is_treated_as_one() {
        let sink = ChainCheckpoints::new(0);
        assert_eq!(sink.every(), 1);
        let mut folder = ChainFolder::new(0);
        fold_all(&mut folder, &sink, &[put(0, 1), put(1, 2)]);
        assert_eq!(sink.checkpoints().0.len(), 2);
    }

    #[test]
    fn json_reports_spacing_frontier_and_hex_hashes() {
        let sink = ChainCheckpoints::new(2);
        let mut folder = ChainFolder::new(2);
        fold_all(&mut folder, &sink, &[put(0, 1), put(1, 2), put(2, 3)]);

        let json = sink.to_json();
        assert!(json.contains("\"checkpoint_every\": 2"), "{json}");
        assert!(
            json.contains("\"n\": 3"),
            "frontier n must be reported: {json}"
        );
        assert!(json.contains("\"n\": 2"), "the crossed checkpoint: {json}");
        // Hashes are hex strings so 64-bit values survive any JSON reader.
        assert!(json.contains("\"h\": \"0x"), "{json}");
        assert!(
            serde_json::from_str::<serde_json::Value>(&json).is_ok(),
            "body must be valid JSON:\n{json}"
        );

        // Producer and parser are checked against each other here, so an
        // encoding change cannot pass one and silently break the other --
        // which for a harness means a comparison count quietly dropping to
        // zero rather than a failing test.
        let report = parse_chain(&json).expect("the parser must accept what the producer emits");
        assert_eq!(
            report.frontier,
            sink.frontier(),
            "the frontier must survive the round trip, hash included"
        );
        assert_eq!(
            report.checkpoints,
            sink.checkpoints().0,
            "the retained checkpoints must survive the round trip"
        );
        assert!(
            report.checkpoints.iter().any(|(_, h)| *h != 0),
            "a round trip of all-zero hashes would prove nothing about the hex encoding"
        );
    }

    #[test]
    fn an_empty_table_still_renders_valid_json() {
        let sink = ChainCheckpoints::new(8);
        let json = sink.to_json();
        assert!(
            serde_json::from_str::<serde_json::Value>(&json).is_ok(),
            "body must be valid JSON:\n{json}"
        );
        assert!(json.contains("\"checkpoints\": []"), "{json}");
    }
}
