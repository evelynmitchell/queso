//! Crash-consistent, fsync'd on-disk persistence for one replica's
//! [`queso_smr::Durable`] state (issue #36 / P9 / P12, now backed by real
//! disk instead of only surviving inside `queso_sim::kernel::Kernel`'s
//! still-heap-resident `Box<dyn Node<_>>`).
//!
//! # What's persisted
//!
//! Exactly [`queso_smr::Durable`] (per-slot recorder ISR state, the log
//! frontier, the applied log, and the KV state with its embedded dedup
//! table) plus one extra field this crate needs that `queso_smr` has no
//! reason to know about: [`PersistedState::max_tick`], the highest
//! [`LogicalTime`] tick this replica had reached as of this snapshot -- see
//! `crate::ctx::RealCtx`'s `baseline` field docs for why a real restart
//! needs this to keep `LogicalTime` monotonic. Nothing else: the volatile
//! half of a replica's state (pending-op queue, in-flight proposer) is
//! deliberately not here, exactly as `queso_smr::replica::Durable`'s docs
//! describe -- a real crash genuinely loses it, and [`SmrNode::on_restart`]
//! is what recovers gracefully from that loss (see `crate::driver::run_node`
//! for how the two compose).
//!
//! # The atomic-write scheme
//!
//! [`Store::save`] never mutates the real snapshot file in place. It:
//!
//! 1. Serializes the new state and writes it to a *temporary* file in the
//!    same directory as the real path (same filesystem, so the rename in
//!    step 3 is guaranteed atomic -- see `rename(2)`).
//! 2. `fsync`s that temp file, so its bytes are durable on disk before
//!    anything references it by its final name.
//! 3. `rename`s the temp file over the real path -- atomic on every
//!    POSIX filesystem: a concurrent reader (or a crash) can only ever see
//!    the old complete file or the new complete file, never a half-written
//!    one.
//! 4. `fsync`s the *directory* the rename happened in, so the rename itself
//!    (a directory-entry update) is durable too -- without this, a crash
//!    right after the rename could, on some filesystems, roll the directory
//!    entry back to the old file after a power loss even though `rename`
//!    itself returned success (durable-rename is a well-known POSIX gotcha).
//!
//! This is the standard "write-temp, fsync, rename, fsync-dir" pattern for
//! crash-consistent file replacement; see e.g. PostgreSQL's/SQLite's WAL
//! checkpoint code for the same idiom.
//!
//! # Honest limits (see this crate's README for the full list)
//!
//! - **Per-RPC fsync, not group commit.** [`Store::save`] is called once per
//!   inbound message that can mutate `Durable` (see `crate::driver::run_node`),
//!   each a synchronous `fsync(2)` on this replica's single event loop --
//!   correct, but a real deployment wanting throughput would batch multiple
//!   decisions into one fsync (group commit), which this version
//!   deliberately does not build (perf follow-up, not a correctness gap).
//! - **Whole-state snapshot, not an append-only log.** Every save serializes
//!   and rewrites the *entire* `Durable` (all recorders, the whole applied
//!   log), not just the delta -- `O(log length)` per write. Fine for the
//!   tests and demos this phase targets; a real long-running deployment
//!   would want an incremental WAL plus periodic snapshots/compaction
//!   (Phase 8 territory, `docs/00-project-outline.md`'s log-compaction item).
//! - **fsync is trusted, not verified end-to-end.** This does not `fsync`
//!   the *parent* of the data directory, does not use `O_DIRECT`/`fdatasync`
//!   tuning, and does not defend against a lying disk/filesystem that
//!   acknowledges an fsync it hasn't actually made durable (a known class of
//!   real-world storage bugs) -- ordinary, well-behaved POSIX semantics are
//!   assumed throughout.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use queso_sim::ids::NodeId;
use queso_smr::Durable;
use serde::{Deserialize, Serialize};

/// Everything [`Store`] writes to disk for one replica: its durable SMR
/// state plus the logical-time baseline `crate::ctx::RealCtx` needs to
/// restore across a restart. See the module docs.
#[derive(Serialize, Deserialize)]
struct PersistedState {
    durable: Durable,
    /// The highest logical tick this replica had reached as of this
    /// snapshot -- see `crate::ctx::RealCtx`'s `baseline` field docs.
    max_tick: u64,
}

/// One replica's on-disk snapshot store: a single file per node id inside a
/// shared data directory (so a whole cluster's replicas can share one
/// `--data-dir` without colliding), written via the atomic-rename pattern
/// (see the module docs).
pub struct Store {
    /// The real, always-complete snapshot path -- never written to
    /// directly, only ever produced by renaming `tmp_path` over it.
    path: PathBuf,
    /// The temp file `save` stages a new snapshot into before renaming it
    /// over `path`. Fixed (not per-write-unique): this replica's event loop
    /// is single-threaded and every `save` call runs to completion
    /// (including the rename) before the next one starts, so there is never
    /// more than one temp file in flight, and reusing the same name means a
    /// crash mid-write simply leaves one harmless stale temp file behind
    /// (never mistaken for a real snapshot -- only `path`'s exact name is
    /// ever loaded).
    tmp_path: PathBuf,
    dir: PathBuf,
}

impl Store {
    /// Build (and ensure exists) the snapshot store for replica `id`'s state
    /// under `data_dir`.
    pub fn new(data_dir: &Path, id: NodeId) -> std::io::Result<Self> {
        fs::create_dir_all(data_dir)?;
        let dir = data_dir.to_path_buf();
        let path = dir.join(format!("node-{}.durable.bin", id.0));
        let tmp_path = dir.join(format!("node-{}.durable.bin.tmp", id.0));
        Ok(Self {
            path,
            tmp_path,
            dir,
        })
    }

    /// Load this replica's most recently persisted `(Durable, max_tick)`, or
    /// `None` if no snapshot exists yet (a genuinely fresh, never-before-run
    /// replica -- `crate::driver::run_node` must not call
    /// [`queso_smr::SmrNode::on_restart`] in that case, see that function's
    /// docs).
    pub fn load(&self) -> anyhow::Result<Option<(Durable, u64)>> {
        if !self.path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(&self.path)?;
        let state: PersistedState = bincode::deserialize(&bytes)?;
        Ok(Some((state.durable, state.max_tick)))
    }

    /// Durably persist `durable`/`max_tick`, replacing any previous
    /// snapshot, via the atomic write-temp/fsync/rename/fsync-dir sequence
    /// described in the module docs. Returns only once every step --
    /// including both `fsync`s -- has completed, so a caller that waits for
    /// this to return before releasing a dependent reply (see
    /// `crate::ctx::RealCtx::flush_outbound`'s docs) gets the
    /// write-before-reply guarantee (P12) for real.
    pub fn save(&self, durable: &Durable, max_tick: u64) -> anyhow::Result<()> {
        let state = PersistedState {
            durable: durable.clone(),
            max_tick,
        };
        let bytes = bincode::serialize(&state)?;

        let mut tmp = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.tmp_path)?;
        tmp.write_all(&bytes)?;
        tmp.sync_all()?; // fsync the temp file's contents + metadata.
        drop(tmp);

        fs::rename(&self.tmp_path, &self.path)?; // atomic on the same filesystem.

        // fsync the containing directory: without this, a crash right after
        // a successful `rename` can, on some filesystems, still lose the
        // directory-entry update on power loss (the well-known
        // durable-rename gotcha -- see the module docs).
        let dir = File::open(&self.dir)?;
        dir.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // `Durable`'s fields are `pub(crate)` to `queso_smr` (see that crate's
    // docs on why -- only `queso_smr::replica`/`queso_smr::cluster` mutate
    // it directly, everyone else goes through `SmrNode`'s public API), so
    // these tests -- like any other external crate -- can only construct it
    // via `Durable::default()` and treat it as an opaque blob; the
    // meaningful "does a *populated* snapshot survive a real restart"
    // coverage lives in `tests/restart_recovery.rs`, which drives real
    // `SmrNode`s through `submit`/`on_restart` end to end.

    #[test]
    fn round_trips_a_default_durable_state() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let store = Store::new(tmp.path(), NodeId(0)).expect("new store");
        assert!(store.load().expect("load").is_none());

        let durable = Durable::default();
        store.save(&durable, 42).expect("save");

        let (_loaded, max_tick) = store.load().expect("load").expect("snapshot present");
        assert_eq!(max_tick, 42);
    }

    #[test]
    fn a_second_save_atomically_replaces_the_first_and_leaves_no_temp_file() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let store = Store::new(tmp.path(), NodeId(1)).expect("new store");

        store.save(&Durable::default(), 10).expect("save 1");
        store.save(&Durable::default(), 20).expect("save 2");

        let (_loaded, max_tick) = store.load().expect("load").expect("snapshot present");
        assert_eq!(max_tick, 20, "load must see the second save, not the first");
        assert!(!tmp.path().join("node-1.durable.bin.tmp").exists());
    }
}
