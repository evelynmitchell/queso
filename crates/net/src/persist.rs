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
//! [`LogicalTime`]: queso_sim::time::LogicalTime
//! [`SmrNode::on_restart`]: queso_smr::SmrNode::on_restart
//!
//! # On-disk format: a versioned header, then bincode (Phase 8.1a, issue #39)
//!
//! Every snapshot file starts with a small, fixed-size, *unversioned itself*
//! header -- [`MAGIC`] (4 bytes identifying this as a queso-net durable
//! snapshot file at all, not some other file that happened to end up at this
//! path) followed by a little-endian `u16` [`FORMAT_VERSION`] -- before the
//! bincode-serialized [`PersistedState`] payload. [`Store::load`] checks
//! both fields *before* attempting to `bincode::deserialize` anything, and
//! fails loudly (a clear `anyhow::Error`, not a silent mis-parse or a
//! confusing bincode error deep in some nested field) if either doesn't
//! match what this build understands. This is what lets a future
//! `Durable`/`Isr`/`Recorder`/`Kv` layout change bump [`FORMAT_VERSION`] and
//! know for certain that an old-format data directory will be rejected
//! rather than silently misinterpreted as the new layout (bincode has no
//! self-describing schema -- a length or discriminant byte in the old
//! format could easily parse as *something* in the new one without erroring,
//! which is exactly the failure mode a version header exists to rule out).
//! This is v1: nothing about the payload's own layout changes in this PR.
//!
//! # The atomic-write scheme
//!
//! [`Store::save`]/[`Store::persist`] never mutate the real snapshot file in
//! place. They:
//!
//! 1. Serialize the new state (header + bincode payload) and write it to a
//!    *temporary* file in the same directory as the real path (same
//!    filesystem, so the rename in step 3 is guaranteed atomic -- see
//!    `rename(2)`).
//! 2. `fsync` that temp file, so its bytes are durable on disk before
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
//! checkpoint code for the same idiom. Changing *what bytes* get written
//! (adding the header) and *which thread runs the blocking syscalls* (see
//! below) does not change this sequence or its atomicity guarantee at all.
//!
//! # Group-commit + async fsync offload (Phase 8.1a, issue #46)
//!
//! Two entry points now exist:
//!
//! - [`Store::save`]: the original, fully synchronous path (serialize +
//!   blocking write/fsync/rename/dir-fsync, all on the calling thread).
//!   Still correct and still used directly by this module's own unit tests;
//!   `crate::driver::run_node` no longer calls it.
//! - [`Store::persist`]: what `crate::driver::run_node` calls instead.
//!   Serializes `durable`/`max_tick` to a `Vec<u8>` **on the calling
//!   (driver) thread** -- cheap, CPU-only, and `Durable`/`SmrNode` are
//!   `Rc<RefCell<_>>`-based (not `Send`), so they can never leave that
//!   thread -- then hands the *bytes* (which, as a plain `Vec<u8>`, are
//!   `Send`) to [`tokio::task::spawn_blocking`] to perform the actual
//!   blocking file I/O (write, `fsync`, `rename`, directory `fsync`) on a
//!   dedicated blocking-pool thread instead of the driver's own async task.
//!   The driver `.await`s that task's completion before doing anything else
//!   -- so from the driver's point of view `persist().await` returning is
//!   exactly as synchronous a durability barrier as `save()` always was; the
//!   only thing that changes is that *other* tokio tasks (peer/client
//!   accept loops, timer futures, outbound dialers) keep making progress on
//!   the driver's own thread while this replica's fsync is in flight on the
//!   blocking pool, instead of the whole runtime-visible-to-this-task being
//!   stalled on a synchronous syscall. A `spawn_blocking` `JoinError` (task
//!   panicked or the runtime is shutting down) or an I/O error from the
//!   write itself is propagated with `?`, exactly like `save`'s errors
//!   always were -- fail-stop, no attempt to paper over a failed durability
//!   write.
//!
//! See `crate::driver`'s module docs for how group-commit coalescing (many
//! mutating events, one [`Store::persist`] call) is layered on top of this.
//!
//! # Honest limits (see this crate's README for the full list)
//!
//! - **Group commit, not per-op group-commit-of-one.** [`Store::persist`] is
//!   now called at most once per *batch* of already-queued mutating events
//!   (see `crate::driver`'s docs), not once per inbound message -- but it is
//!   still a **whole-state snapshot** rewrite each time (see below), and
//!   under genuinely serialized low-throughput load a batch is usually just
//!   one event, so the per-fsync cost itself (disk round trip) is unchanged;
//!   what changes is how many *decisions* can share that cost when several
//!   are ready at once.
//! - **Whole-state snapshot, not an append-only log.** Every persist
//!   serializes and rewrites the *entire* `Durable` (all recorders, the
//!   whole applied log), not just the delta -- `O(log length)` per write.
//!   Fine for the tests and demos this phase targets; a real long-running
//!   deployment would want an incremental WAL plus periodic
//!   snapshots/compaction (Phase 8.1c, deliberately deferred -- see issue
//!   #46's design-decision comment for why byte-incremental deltas are not
//!   an obviously-safe next step here).
//! - **fsync is trusted, not verified end-to-end.** This does not `fsync`
//!   the *parent* of the data directory, does not use `O_DIRECT`/`fdatasync`
//!   tuning, and does not defend against a lying disk/filesystem that
//!   acknowledges an fsync it hasn't actually made durable (a known class of
//!   real-world storage bugs) -- ordinary, well-behaved POSIX semantics are
//!   assumed throughout.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use queso_sim::ids::NodeId;
use queso_smr::Durable;
use serde::{Deserialize, Serialize};

/// Identifies a file as a queso-net durable-snapshot file at all (as
/// opposed to some unrelated file that happened to end up at this path).
/// See the module docs' "On-disk format" section.
const MAGIC: [u8; 4] = *b"QSD1"; // "Queso Snapshot, Durable" -- the trailing
                                 // digit is *not* the schema version (that's
                                 // `FORMAT_VERSION`, checked separately) --
                                 // it is just part of the fixed magic value
                                 // itself, never bumped.
/// The on-disk schema version of [`PersistedState`]'s bincode encoding.
/// [`Store::load`] rejects any file whose header doesn't carry exactly this
/// value -- see the module docs. Bump this (and add an explicit migration
/// or an intentional "refuse to load" decision) the next time
/// `PersistedState`/`Durable`'s serialized shape changes; this PR
/// introduces the header but does not change the payload shape, so this
/// stays `1`.
const FORMAT_VERSION: u16 = 1;
/// `MAGIC` (4 bytes) + `FORMAT_VERSION` (2 bytes, little-endian).
const HEADER_LEN: usize = MAGIC.len() + 2;

/// Everything [`Store`] writes to disk for one replica: its durable SMR
/// state plus the logical-time baseline `crate::ctx::RealCtx` needs to
/// restore across a restart. See the module docs. This is the payload that
/// follows the schema-version header on disk -- it has no version field of
/// its own, because the header is exactly what's versioned.
#[derive(Serialize, Deserialize)]
struct PersistedState {
    durable: Durable,
    /// The highest logical tick this replica had reached as of this
    /// snapshot -- see `crate::ctx::RealCtx`'s `baseline` field docs.
    max_tick: u64,
}

/// Serialize `durable`/`max_tick` into the full on-disk byte layout: the
/// schema-version header (see the module docs) followed by the bincode
/// payload. Pure/CPU-only -- safe (and intended) to call from an async
/// task's own thread; see [`Store::persist`].
fn encode(durable: &Durable, max_tick: u64) -> anyhow::Result<Vec<u8>> {
    let state = PersistedState {
        durable: durable.clone(),
        max_tick,
    };
    let payload = bincode::serialize(&state)?;
    let mut bytes = Vec::with_capacity(HEADER_LEN + payload.len());
    bytes.extend_from_slice(&MAGIC);
    bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

/// The inverse of [`encode`]: validate the header, then deserialize the
/// payload -- or fail loudly (never silently mis-parse) if the header
/// doesn't match what this build understands. See the module docs.
fn decode(bytes: &[u8]) -> anyhow::Result<(Durable, u64)> {
    if bytes.len() < HEADER_LEN {
        anyhow::bail!(
            "durable snapshot file is only {} byte(s) long, too short to contain the \
             {HEADER_LEN}-byte schema header -- not a valid queso-net snapshot file",
            bytes.len()
        );
    }
    let (header, payload) = bytes.split_at(HEADER_LEN);
    let magic = &header[..MAGIC.len()];
    if magic != MAGIC {
        anyhow::bail!(
            "durable snapshot file has an unrecognized magic {magic:02x?} (expected \
             {MAGIC:02x?}) -- this is not a queso-net durable snapshot file (or it is badly \
             corrupted); refusing to load it rather than risk misinterpreting unrelated bytes \
             as `Durable` state"
        );
    }
    let version = u16::from_le_bytes([header[MAGIC.len()], header[MAGIC.len() + 1]]);
    if version != FORMAT_VERSION {
        anyhow::bail!(
            "durable snapshot file has schema version {version}, but this build only \
             understands version {FORMAT_VERSION} -- refusing to load it rather than risk \
             silently misparsing a different on-disk layout (bincode has no self-describing \
             schema, so a format mismatch can otherwise parse as *something* without erroring); \
             migrate the data directory or start with an empty one"
        );
    }
    let state: PersistedState = bincode::deserialize(payload)?;
    Ok((state.durable, state.max_tick))
}

/// One replica's on-disk snapshot store: a single file per node id inside a
/// shared data directory (so a whole cluster's replicas can share one
/// `--data-dir` without colliding), written via the atomic-rename pattern
/// (see the module docs).
///
/// `Clone`: cheap (a few `PathBuf`s and two `Arc`s) and deliberate --
/// [`Store::persist`] clones `self` into the `spawn_blocking` closure it
/// hands to the blocking thread pool, since that closure must be `'static`
/// and own everything it touches. Every clone of a given `Store` still
/// refers to the *same* underlying counter/delay state (see
/// [`Self::save_count`]/[`Self::with_artificial_delay`]) and, of course, the
/// same on-disk path.
#[derive(Clone)]
pub struct Store {
    /// The real, always-complete snapshot path -- never written to
    /// directly, only ever produced by renaming `tmp_path` over it.
    path: PathBuf,
    /// The temp file `save`/`persist` stage a new snapshot into before
    /// renaming it over `path`. Fixed (not per-write-unique): this
    /// replica's event loop only ever has one durability write in flight at
    /// a time (`crate::driver::run_node` `.await`s each batch's
    /// [`Store::persist`] before dispatching the next one), so there is
    /// never more than one temp file in flight, and reusing the same name
    /// means a crash mid-write simply leaves one harmless stale temp file
    /// behind (never mistaken for a real snapshot -- only `path`'s exact
    /// name is ever loaded).
    tmp_path: PathBuf,
    dir: PathBuf,
    /// Number of times this store has actually completed a write+fsync+
    /// rename+dir-fsync cycle to disk -- **not** the number of times
    /// `save`/`persist` was merely *called* successfully-in-the-same-count
    /// sense, they're the same thing; this counts real durable writes.
    /// Exposed via [`Self::save_count`] purely as test/observability
    /// instrumentation: it is how `tests/group_commit.rs` proves that
    /// group-commit coalescing under concurrent load produces far fewer
    /// actual fsyncs than mutating events applied. Always present (a plain
    /// `AtomicU64`, negligible cost) rather than feature-gated, since it is
    /// harmless in production and a `Store` shared out to a test via
    /// [`crate::config::NodeConfig::save_counter`] needs a real, live
    /// counter to observe -- see that field's docs. An ordinary
    /// `queso-node` run never reads this.
    save_count: Arc<AtomicU64>,
    /// Artificial extra delay [`Self::write_bytes_blocking`] sleeps before
    /// doing its real write -- always `Duration::ZERO` (a no-op) unless a
    /// test explicitly opts in via [`Self::with_artificial_delay`] /
    /// [`crate::config::NodeConfig::persist_delay`]. Exists solely to make
    /// the write-before-reply ordering guarantee (P12) observable in
    /// wall-clock time from a black-box integration test: with a real disk,
    /// a reordering bug that released a reply before its fsync completed
    /// would be invisible to any test that doesn't control fsync latency
    /// directly (see `tests/group_commit.rs`'s
    /// `write_before_reply_holds_even_when_the_fsync_is_slow`, and
    /// `crate::driver`'s module docs on why this can't otherwise be
    /// observed from outside the process). Never set by `queso-node`'s CLI
    /// or by any other test.
    artificial_delay: Duration,
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
            save_count: Arc::new(AtomicU64::new(0)),
            artificial_delay: Duration::ZERO,
        })
    }

    /// Share `counter` as this store's save counter (replacing its own
    /// freshly-created one) -- see [`Self::save_count`]'s docs. Test-only;
    /// `queso-node`'s CLI never calls this.
    #[must_use]
    pub fn with_save_counter(mut self, counter: Arc<AtomicU64>) -> Self {
        self.save_count = counter;
        self
    }

    /// Inject `delay` as an artificial sleep before every future blocking
    /// write this store performs -- see [`Self::artificial_delay`]'s docs.
    /// Test-only; `queso-node`'s CLI never calls this (always leaves the
    /// default `Duration::ZERO`, a no-op).
    #[must_use]
    pub fn with_artificial_delay(mut self, delay: Duration) -> Self {
        self.artificial_delay = delay;
        self
    }

    /// The number of real write+fsync+rename+dir-fsync cycles this store
    /// has completed so far. See [`Self::save_count`] the field's docs.
    pub fn save_count(&self) -> u64 {
        self.save_count.load(Ordering::SeqCst)
    }

    /// Load this replica's most recently persisted `(Durable, max_tick)`, or
    /// `None` if no snapshot exists yet (a genuinely fresh, never-before-run
    /// replica -- `crate::driver::run_node` must not call
    /// [`queso_smr::SmrNode::on_restart`] in that case, see that function's
    /// docs). Fails with a clear error (rather than silently mis-parsing)
    /// if the file exists but its schema header doesn't match what this
    /// build understands -- see the module docs.
    pub fn load(&self) -> anyhow::Result<Option<(Durable, u64)>> {
        if !self.path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(&self.path)?;
        let (durable, max_tick) = decode(&bytes)?;
        Ok(Some((durable, max_tick)))
    }

    /// Durably persist `durable`/`max_tick`, replacing any previous
    /// snapshot, via the atomic write-temp/fsync/rename/fsync-dir sequence
    /// described in the module docs -- fully synchronously, on the calling
    /// thread. Returns only once every step -- including both `fsync`s --
    /// has completed, so a caller that waits for this to return before
    /// releasing a dependent reply (see
    /// `crate::ctx::RealCtx::flush_outbound`'s docs) gets the
    /// write-before-reply guarantee (P12) for real.
    ///
    /// `crate::driver::run_node` calls [`Self::persist`] instead (the async,
    /// `spawn_blocking`-offloaded equivalent) so the driver's own task isn't
    /// stalled on the blocking syscalls below; this synchronous version
    /// remains for this module's own unit tests and any caller outside an
    /// async context.
    pub fn save(&self, durable: &Durable, max_tick: u64) -> anyhow::Result<()> {
        let bytes = encode(durable, max_tick)?;
        self.write_bytes_blocking(&bytes)
    }

    /// The async, offloaded equivalent of [`Self::save`]: serializes
    /// `durable`/`max_tick` to bytes on the *calling* thread (cheap,
    /// CPU-only -- see the module docs' "Group-commit + async fsync
    /// offload" section for why this step, specifically, cannot move off
    /// the driver thread), then runs the actual blocking file I/O on
    /// [`tokio::task::spawn_blocking`]'s dedicated thread pool and awaits
    /// its completion.
    ///
    /// Returns only once the write is fully durable on disk (same guarantee
    /// as [`Self::save`]) -- `.await`ing this therefore still gives the
    /// caller write-before-reply (P12) for real, it just no longer blocks
    /// *other* tasks on the calling runtime while it happens. A
    /// `spawn_blocking` `JoinError` (the blocking task panicked, or the
    /// runtime is shutting down) or an I/O error from the write itself is
    /// surfaced as an `Err` here -- fail-stop, exactly like `save`'s errors
    /// always were, never silently swallowed.
    pub async fn persist(&self, durable: &Durable, max_tick: u64) -> anyhow::Result<()> {
        let bytes = encode(durable, max_tick)?;
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.write_bytes_blocking(&bytes))
            .await
            .map_err(|join_err| {
                anyhow::anyhow!(
                    "durable-snapshot persist task panicked or was cancelled: {join_err}"
                )
            })??;
        Ok(())
    }

    /// The actual blocking write-temp/fsync/rename/fsync-dir sequence (see
    /// the module docs), shared by both [`Self::save`] (runs it inline) and
    /// [`Self::persist`] (runs it inside `spawn_blocking`). Increments
    /// [`Self::save_count`] only after every step -- including the
    /// directory fsync -- has actually completed, so the counter only ever
    /// reflects writes that are genuinely durable, never attempted-but-
    /// failed ones.
    fn write_bytes_blocking(&self, bytes: &[u8]) -> anyhow::Result<()> {
        if !self.artificial_delay.is_zero() {
            // Test-only instrumentation -- see `Self::artificial_delay`'s
            // docs. `std::thread::sleep` is fine here: this always runs on
            // a blocking-pool thread (`spawn_blocking`, or `save`'s
            // caller-provided thread for a caller outside async context),
            // never on a tokio task actually driving other work.
            std::thread::sleep(self.artificial_delay);
        }

        let mut tmp = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.tmp_path)?;
        tmp.write_all(bytes)?;
        tmp.sync_all()?; // fsync the temp file's contents + metadata.
        drop(tmp);

        fs::rename(&self.tmp_path, &self.path)?; // atomic on the same filesystem.

        // fsync the containing directory: without this, a crash right after
        // a successful `rename` can, on some filesystems, still lose the
        // directory-entry update on power loss (the well-known
        // durable-rename gotcha -- see the module docs).
        let dir = File::open(&self.dir)?;
        dir.sync_all()?;

        self.save_count.fetch_add(1, Ordering::SeqCst);
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
        assert_eq!(
            store.save_count(),
            1,
            "one real write should count as one save"
        );
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
        assert_eq!(store.save_count(), 2);
    }

    #[tokio::test]
    async fn persist_is_a_real_durability_barrier_like_save() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let store = Store::new(tmp.path(), NodeId(2)).expect("new store");

        store
            .persist(&Durable::default(), 7)
            .await
            .expect("persist");

        let (_loaded, max_tick) = store.load().expect("load").expect("snapshot present");
        assert_eq!(max_tick, 7);
        assert_eq!(store.save_count(), 1);
    }

    /// The schema-version-rejection coverage gap from issue #39: a file
    /// whose header doesn't match this build's magic/version must be
    /// rejected with a clear error, never silently mis-parsed as if it were
    /// a valid (if garbage) `PersistedState`.
    #[test]
    fn load_rejects_a_file_with_the_wrong_magic() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let store = Store::new(tmp.path(), NodeId(3)).expect("new store");
        let path = tmp.path().join("node-3.durable.bin");
        // A plausible-looking but foreign header (imagine some other tool,
        // or a much later/earlier queso version with a different magic,
        // wrote this file) followed by bytes that are not valid bincode for
        // `PersistedState` -- a version-oblivious loader would either
        // panic deep inside `bincode::deserialize` or, worse, could
        // sometimes succeed and hand back nonsense state.
        let mut bytes = b"NOPE".to_vec();
        bytes.extend_from_slice(&1u16.to_le_bytes());
        bytes.extend_from_slice(b"garbage-payload-not-real-bincode");
        fs::write(&path, &bytes).expect("write a foreign-magic file");

        // `queso_smr::Durable` deliberately has no `Debug` impl (see its own
        // docs), so `Result::expect_err` (which requires the `Ok` side to
        // be `Debug` for its panic message) doesn't apply here -- match
        // explicitly instead.
        let err = match store.load() {
            Ok(_) => panic!("a file with the wrong magic must be rejected, not mis-parsed"),
            Err(err) => err,
        };
        let msg = err.to_string();
        assert!(
            msg.contains("magic"),
            "error should explain the magic mismatch, got: {msg}"
        );
    }

    #[test]
    fn load_rejects_a_file_with_the_right_magic_but_wrong_version() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let store = Store::new(tmp.path(), NodeId(4)).expect("new store");
        let path = tmp.path().join("node-4.durable.bin");
        let mut bytes = MAGIC.to_vec();
        bytes.extend_from_slice(&9_999u16.to_le_bytes()); // a version this build cannot possibly understand
        bytes.extend_from_slice(b"irrelevant-payload-bytes");
        fs::write(&path, &bytes).expect("write a future/foreign-version file");

        let err = match store.load() {
            Ok(_) => panic!("a file with an unrecognized schema version must be rejected"),
            Err(err) => err,
        };
        let msg = err.to_string();
        assert!(
            msg.contains("version"),
            "error should explain the version mismatch, got: {msg}"
        );
        assert!(
            msg.contains("9999"),
            "error should mention the offending version, got: {msg}"
        );
    }

    #[test]
    fn load_rejects_a_truncated_file_shorter_than_the_header() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let store = Store::new(tmp.path(), NodeId(5)).expect("new store");
        let path = tmp.path().join("node-5.durable.bin");
        fs::write(&path, b"Q").expect("write a too-short file");

        let err = match store.load() {
            Ok(_) => panic!("a file shorter than the header must be rejected"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("too short"),
            "error should explain the file is too short, got: {err}"
        );
    }
}
