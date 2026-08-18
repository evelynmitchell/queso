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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
/// see its docs for why "latest known status" wants no locks. A checkpoint
/// *table* cannot be an atomic, so this holds one `Mutex`. That is an
/// acceptable exception rather than a slip: the driver takes the lock only
/// when a checkpoint is actually crossed (once per `every` applied
/// commands, not once per event), the handler task takes it once per
/// request, and the critical section is a `push_back` on a bounded deque.
/// The frontier state stays lock-free so the common read path -- "where is
/// this replica now" -- never contends with the driver at all.
#[derive(Debug)]
pub struct ChainCheckpoints {
    /// Checkpoint spacing in slots. See the module docs: cluster-wide.
    every: u64,
    /// The replica's current chain position, updated on every fold.
    frontier_n: AtomicU64,
    /// The hash at `frontier_n`.
    frontier_h: AtomicU64,
    /// `(n, h)` for each crossed checkpoint, oldest first.
    table: Mutex<VecDeque<(u64, u64)>>,
    /// Whether any checkpoint has been dropped to stay within
    /// [`CHECKPOINT_RING_CAPACITY`].
    truncated: AtomicBool,
}

impl ChainCheckpoints {
    /// A fresh, empty checkpoint table with the given spacing. A spacing of
    /// `0` is treated as `1` (every slot is a checkpoint).
    pub fn new(every: u64) -> Self {
        let genesis = ChainState::genesis();
        Self {
            every: every.max(1),
            frontier_n: AtomicU64::new(genesis.n),
            frontier_h: AtomicU64::new(genesis.h),
            table: Mutex::new(VecDeque::new()),
            truncated: AtomicBool::new(false),
        }
    }

    /// This node's checkpoint spacing, so a harness can verify every replica
    /// in the cluster is publishing at the same `n` values.
    pub fn every(&self) -> u64 {
        self.every
    }

    /// Record a crossed checkpoint, evicting the oldest if the ring is full.
    fn record(&self, n: u64, h: u64) {
        let mut table = self.table.lock().unwrap_or_else(|e| e.into_inner());
        if table.len() == CHECKPOINT_RING_CAPACITY {
            table.pop_front();
            self.truncated.store(true, Ordering::Relaxed);
        }
        table.push_back((n, h));
    }

    /// Publish the replica's current chain position.
    fn set_frontier(&self, state: ChainState) {
        self.frontier_n.store(state.n, Ordering::Relaxed);
        self.frontier_h.store(state.h, Ordering::Relaxed);
    }

    /// The replica's current chain position.
    pub fn frontier(&self) -> ChainState {
        ChainState {
            n: self.frontier_n.load(Ordering::Relaxed),
            h: self.frontier_h.load(Ordering::Relaxed),
        }
    }

    /// Every retained checkpoint, oldest first, plus whether any older ones
    /// were dropped.
    pub fn checkpoints(&self) -> (Vec<(u64, u64)>, bool) {
        let table = self.table.lock().unwrap_or_else(|e| e.into_inner());
        (
            table.iter().copied().collect(),
            self.truncated.load(Ordering::Relaxed),
        )
    }

    /// `GET /chain`'s JSON body.
    ///
    /// Hashes are hex strings, not JSON numbers: they use the full 64-bit
    /// range, which does not survive a reader that parses JSON numbers as
    /// IEEE doubles. The harness is Rust and would be fine either way, but
    /// an endpoint that silently rounds for `curl | jq` users is a trap.
    pub fn to_json(&self) -> String {
        let frontier = self.frontier();
        let (checkpoints, truncated) = self.checkpoints();

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
