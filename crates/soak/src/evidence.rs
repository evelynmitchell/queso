//! Keeping a failed seed's on-disk state around long enough to look at
//! (issue #73).
//!
//! # Why this exists
//!
//! `queso-soak` ran every seed against a `tempfile::tempdir()`, which is
//! deleted the moment the run returns. That is right for the clean case and
//! exactly wrong for the interesting one: three occurrences of a
//! Chain-of-Blocks divergence have now been reported by the nightly soak,
//! and every one of them destroyed its own evidence before anybody could
//! ask the decisive question.
//!
//! The decisive question is whether the replicas' *durable applied logs*
//! actually differ at the disputed slot. If they agree, the divergence was
//! an artifact of the observability path; if they differ, it is a genuine
//! Agreement violation. `queso_smr::Durable` carries `applied_log` and is
//! serialized into the snapshot file each replica keeps in its data dir --
//! so the data dir is sufficient to answer it, and nothing else is.
//!
//! Preserving it is therefore not a convenience. It is the difference
//! between a report that can be adjudicated and one that can only be
//! argued about.

use std::io;
use std::path::{Path, PathBuf};

/// Keep `data_dir` for post-mortem, or delete it.
///
/// Returns the path when it was kept, so a caller can report where the
/// evidence went, and `None` when it was removed.
///
/// The asymmetry is deliberate and load-bearing in both directions.
/// Keeping everything would accumulate a replica's whole durable state per
/// seed per run -- with log compaction deliberately deferred (#46) that
/// grows with run length -- and a CI job that fills its disk stops finding
/// bugs. Deleting a failure is worse: it is unrecoverable, and the only
/// runs worth keeping are exactly the ones nobody planned for.
///
/// Errors are the caller's to report, not to ignore: a failure that could
/// not be preserved is a materially weaker report, and silently swallowing
/// the `io::Error` would leave an operator hunting for a directory that
/// was never written.
pub fn retain_evidence(data_dir: &Path, keep: bool) -> io::Result<Option<PathBuf>> {
    if keep {
        return Ok(Some(data_dir.to_path_buf()));
    }
    match std::fs::remove_dir_all(data_dir) {
        // Already gone is the outcome asked for, not an error.
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
        Ok(()) => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn populated_dir() -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("temp dir");
        std::fs::write(dir.path().join("node-0.state"), b"durable bytes").expect("write");
        std::fs::create_dir(dir.path().join("nested")).expect("mkdir");
        std::fs::write(dir.path().join("nested/node-1.state"), b"more").expect("write");
        dir
    }

    /// A clean seed leaves nothing behind. Without this the nightly would
    /// accumulate every replica's durable state for every seed of every
    /// run, and a CI job that fills its disk stops finding bugs.
    #[test]
    fn a_clean_seed_is_removed() {
        let dir = populated_dir();
        let path = dir.path().to_path_buf();

        assert_eq!(retain_evidence(&path, false).expect("remove"), None);
        assert!(!path.exists(), "a clean seed must not be left on disk");
    }

    /// The whole point: a failed seed's state survives, contents intact.
    ///
    /// Falsifier: invert the `keep` branch and this fails -- which is
    /// precisely the bug #73 spent three occurrences on.
    #[test]
    fn a_failed_seed_is_kept_with_its_contents() {
        let dir = populated_dir();
        let path = dir.path().to_path_buf();

        let kept = retain_evidence(&path, true).expect("keep");

        assert_eq!(kept.as_deref(), Some(path.as_path()));
        assert!(path.exists(), "a failed seed's data dir must survive");
        assert_eq!(
            std::fs::read(path.join("node-0.state")).expect("read back"),
            b"durable bytes",
            "the durable snapshot is the evidence -- it must be untouched"
        );
        assert!(
            path.join("nested/node-1.state").exists(),
            "nested per-replica state must survive too"
        );
    }

    /// Removing a directory that is already gone is the requested outcome,
    /// not a failure -- a seed whose cluster never started has nothing to
    /// clean up, and that must not be reported as an error.
    #[test]
    fn removing_an_absent_dir_is_not_an_error() {
        let dir = tempfile::tempdir().expect("temp dir");
        let missing = dir.path().join("never-created");

        assert_eq!(retain_evidence(&missing, false).expect("absent"), None);
    }

    /// Keeping does not require the directory to exist either: a harness
    /// error before the cluster booted still reports where the evidence
    /// *would* be, rather than erroring on the way out of an error path.
    #[test]
    fn keeping_an_absent_dir_reports_the_path_anyway() {
        let dir = tempfile::tempdir().expect("temp dir");
        let missing = dir.path().join("never-created");

        assert_eq!(
            retain_evidence(&missing, true).expect("keep").as_deref(),
            Some(missing.as_path())
        );
    }
}
