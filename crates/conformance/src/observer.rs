//! The divergence and liveness observers: what actually *checks* a
//! Chain-of-Blocks run.
//!
//! The observer consumes a stream of [`Sample`]s -- "replica R was seen at
//! chain state `(n, h)` at time T" -- from whatever [`crate::source`] is
//! feeding it, and maintains two verdicts:
//!
//! - **Safety (the CoB safety property).** *If any replica applies command
//!   `C` as its n-th command, every replica eventually applies `C` as its
//!   n-th command.* Restated on the chain: no two replicas may ever show a
//!   different `h` at the same `n`. This is Queso's P1 Agreement / P5
//!   prefix consistency / P6 total order in the form that survives weak
//!   observability. Violations are recorded as [`Divergence`].
//! - **Liveness.** *A submitted command is eventually applied by all
//!   replicas.* A replica that sits behind the cluster's frontier without
//!   advancing for longer than a caller-supplied budget -- after faults
//!   have healed -- is reported as a [`Stall`].
//!
//! # Why the observer takes samples rather than reading logs
//!
//! An in-process test could simply compare every replica's whole applied
//! log (that is what `queso-smr`'s `log_safety.rs` does, and it is a
//! perfectly good test). This observer deliberately works from *sampled*
//! `(n, h)` pairs instead, because that is the only shape of observation
//! available against real `queso-node` processes in Phase 9.2 (#56), where
//! nothing exposes another replica's applied log. Building the check
//! against the weaker interface now is the whole point: the same observer
//! code, unchanged, is what 9.2 points at real binaries.
//!
//! The hash chain is what makes the weaker interface sufficient -- see
//! [`crate::chain`]. A divergence at `n = 6` that no observer ever sampled
//! at `n = 6` is still caught at any later `n` that two replicas share.
//!
//! # What this observer cannot see (honest limitations)
//!
//! - **It cannot see anything between samples.** A replica that diverges
//!   and then is somehow repaired before its next sample leaves no trace.
//!   Queso has no such repair path (a decided slot is immutable), so this
//!   is a theoretical gap, but it is a gap.
//! - **It cannot attribute a divergence to a cause.** It reports which
//!   replicas disagreed at which `n`, plus the per-transition log around
//!   that point when the source could supply command digests
//!   ([`Sample::command_digest`]); root-causing from there is a human's
//!   job.
//! - **Liveness is caller-timed.** The observer does not know when faults
//!   were injected or healed; [`Observer::stalls`] answers "who is behind
//!   and has not moved within this budget" for a `now` and budget the
//!   caller chooses. Calling it while a partition is still in force will
//!   correctly report the isolated minority as stalled -- which is expected
//!   behavior, not a bug, so callers must only judge liveness after a heal.
//! - **Per-replica transition history is capped** ([`Observer::with_transition_cap`])
//!   so a long soak cannot grow it without bound; once capped, reports say
//!   so rather than silently showing a truncated history.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use queso_sim::ids::NodeId;

use crate::chain::{BlockHash, ChainState, Transition};

/// One observation: replica `replica` was seen at chain state `state` at
/// caller-defined time `observed_at`.
///
/// `observed_at` is whatever clock the caller is using -- the sim's
/// `LogicalTime` in Phase 9.1, wall-clock milliseconds or a poll counter
/// for a real-process source in 9.2. The observer only ever compares and
/// subtracts these values, never interprets their unit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Sample {
    /// Which replica was observed.
    pub replica: NodeId,
    /// When, on the caller's clock.
    pub observed_at: u64,
    /// The chain state observed.
    pub state: ChainState,
    /// Digest of the command that took this replica from `n - 1` to `n`,
    /// when the source can see individual commands (an in-process source
    /// can; a `/metrics` poll against a real process cannot). Used only to
    /// enrich divergence reports.
    pub command_digest: Option<u64>,
}

/// A safety violation: two replicas showed different chain hashes at the
/// same `n`, so they applied different command sequences.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Divergence {
    /// The chain position at which the disagreement was detected. Note
    /// this is where it was *detected*, which may be later than where it
    /// began -- the chain propagates a difference forward forever.
    pub n: u64,
    /// The first replica seen at this `n`, and the hash it showed.
    pub first: (NodeId, BlockHash),
    /// The replica that disagreed, and the hash it showed.
    pub other: (NodeId, BlockHash),
    /// When the disagreeing sample arrived, on the caller's clock.
    pub observed_at: u64,
}

/// A liveness violation: a replica is behind the cluster's frontier and has
/// not advanced within the caller's budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stall {
    /// The replica that is not making progress.
    pub replica: NodeId,
    /// Where it is stuck.
    pub stuck_at: ChainState,
    /// The last time it was observed to advance `n`.
    pub last_progress_at: u64,
    /// The `now` the caller passed to [`Observer::stalls`].
    pub now: u64,
    /// The highest `n` any replica has been observed at -- what this
    /// replica is behind.
    pub cluster_frontier: u64,
}

/// Per-replica view the observer maintains as samples arrive.
#[derive(Clone, Debug)]
struct ReplicaView {
    latest: ChainState,
    first_seen_at: u64,
    last_progress_at: u64,
    transitions: Vec<Transition>,
    transitions_truncated: bool,
}

/// The Chain-of-Blocks observer: feed it [`Sample`]s, ask it for
/// [`Divergence`]s and [`Stall`]s.
#[derive(Clone, Debug)]
pub struct Observer {
    /// `n -> (first replica seen at that n, the hash it showed)`. The
    /// witness every later sample at that `n` is checked against.
    witnesses: BTreeMap<u64, (NodeId, BlockHash)>,
    replicas: BTreeMap<NodeId, ReplicaView>,
    divergences: Vec<Divergence>,
    comparisons: u64,
    samples: u64,
    transition_cap: usize,
}

impl Default for Observer {
    fn default() -> Self {
        Self::new()
    }
}

impl Observer {
    /// A fresh observer with the default per-replica transition cap (4096).
    pub fn new() -> Self {
        Self {
            witnesses: BTreeMap::new(),
            replicas: BTreeMap::new(),
            divergences: Vec::new(),
            comparisons: 0,
            samples: 0,
            transition_cap: 4096,
        }
    }

    /// Set how many transitions are retained per replica for divergence
    /// reports. Older transitions past the cap are dropped and the report
    /// says so.
    pub fn with_transition_cap(mut self, cap: usize) -> Self {
        self.transition_cap = cap;
        self
    }

    /// Ingest one observation, checking it against everything seen so far.
    pub fn observe(&mut self, sample: Sample) {
        self.samples += 1;

        match self.witnesses.get(&sample.state.n) {
            Some(&(witness_replica, witness_hash)) => {
                // Only a *different* replica's hash is evidence of
                // divergence; re-sampling the same replica at the same n
                // is just a repeated observation.
                if witness_replica != sample.replica {
                    self.comparisons += 1;
                    if witness_hash != sample.state.h {
                        self.divergences.push(Divergence {
                            n: sample.state.n,
                            first: (witness_replica, witness_hash),
                            other: (sample.replica, sample.state.h),
                            observed_at: sample.observed_at,
                        });
                    }
                }
            }
            None => {
                self.witnesses
                    .insert(sample.state.n, (sample.replica, sample.state.h));
            }
        }

        let view = self
            .replicas
            .entry(sample.replica)
            .or_insert_with(|| ReplicaView {
                latest: sample.state,
                first_seen_at: sample.observed_at,
                last_progress_at: sample.observed_at,
                transitions: Vec::new(),
                transitions_truncated: false,
            });

        if sample.state.n > view.latest.n {
            // Record the transition only when this sample is the immediate
            // successor of the last one *and* the source told us which
            // command caused it; otherwise the gap is honestly left out of
            // the per-transition log rather than fabricated.
            if sample.state.n == view.latest.n + 1 {
                if let Some(digest) = sample.command_digest {
                    if view.transitions.len() == self.transition_cap {
                        view.transitions.remove(0);
                        view.transitions_truncated = true;
                    }
                    view.transitions.push(Transition {
                        before: view.latest,
                        after: sample.state,
                        command_digest: digest,
                    });
                }
            }
            view.latest = sample.state;
            view.last_progress_at = sample.observed_at;
        }
    }

    /// Every divergence detected so far, in detection order. Empty means
    /// the safety property has held across everything observed.
    pub fn divergences(&self) -> &[Divergence] {
        &self.divergences
    }

    /// How many times a sample was actually *checked against* another
    /// replica's hash at the same `n`.
    ///
    /// This is the observer's anti-vacuity counter: a run that produced no
    /// divergences but also zero comparisons has proven nothing, and tests
    /// are expected to assert this is meaningfully non-zero rather than
    /// trusting an empty [`Self::divergences`].
    pub fn comparisons(&self) -> u64 {
        self.comparisons
    }

    /// Total samples ingested.
    pub fn samples(&self) -> u64 {
        self.samples
    }

    /// The highest `n` any replica has been observed at.
    pub fn cluster_frontier(&self) -> u64 {
        self.replicas
            .values()
            .map(|view| view.latest.n)
            .max()
            .unwrap_or(0)
    }

    /// The latest observed state of each replica.
    pub fn latest_states(&self) -> BTreeMap<NodeId, ChainState> {
        self.replicas
            .iter()
            .map(|(&id, view)| (id, view.latest))
            .collect()
    }

    /// Replicas that are behind the cluster frontier and have not advanced
    /// within `budget` of `now`.
    ///
    /// The caller owns the timing decision: call this only once injected
    /// faults have been healed and enough time has passed for a healthy
    /// cluster to catch up, or an isolated minority will be reported --
    /// correctly, but uselessly.
    ///
    /// # Choosing a budget
    ///
    /// `budget` must exceed the interval at which a *healthy* replica
    /// advances under the offered load, or healthy-but-idle replicas are
    /// reported alongside genuinely stuck ones. There are two ways to get
    /// that right, and they pull in opposite directions:
    ///
    /// - Widen the budget to cover the natural gap. Simple, but it delays
    ///   detection of a real stall by the same amount.
    /// - Call [`crate::workload::converge`] first, so every live replica
    ///   has just been given work and a healthy one's last progress is
    ///   recent by construction. That collapses the natural gap and lets a
    ///   *tight* budget be both safe and sensitive -- which is what this
    ///   crate's tests do.
    ///
    /// The second is preferable wherever the harness controls the load,
    /// which includes everything Phase 9.2 will drive.
    ///
    /// A replica never observed at all is not reported here (there is no
    /// evidence about it either way); use [`Self::latest_states`] against
    /// the expected replica set to catch that case.
    pub fn stalls(&self, now: u64, budget: u64) -> Vec<Stall> {
        let frontier = self.cluster_frontier();
        self.replicas
            .iter()
            .filter(|(_, view)| view.latest.n < frontier)
            .filter(|(_, view)| now.saturating_sub(view.last_progress_at) > budget)
            .map(|(&replica, view)| Stall {
                replica,
                stuck_at: view.latest,
                last_progress_at: view.last_progress_at,
                now,
                cluster_frontier: frontier,
            })
            .collect()
    }

    /// The per-transition log recorded for one replica: `state_before =>
    /// state_after` with the digest of the command that caused it. Empty
    /// if the source could not supply command digests.
    pub fn transitions(&self, replica: NodeId) -> &[Transition] {
        self.replicas
            .get(&replica)
            .map(|view| view.transitions.as_slice())
            .unwrap_or(&[])
    }

    /// A human-readable report: the per-replica frontier table, then, for
    /// each divergence, the transitions either side of it on both replicas
    /// -- the shape the Chain-of-Blocks doc recommends for root-causing.
    pub fn render_report(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(
            out,
            "Chain-of-Blocks observer: {} samples, {} cross-replica comparisons, {} divergence(s)",
            self.samples,
            self.comparisons,
            self.divergences.len()
        );

        let _ = writeln!(
            out,
            "\nreplica  n      h                   last_progress_at"
        );
        for (replica, view) in &self.replicas {
            let _ = writeln!(
                out,
                "{:<8} {:<6} 0x{:016x}  {} (first seen {})",
                replica.to_string(),
                view.latest.n,
                view.latest.h,
                view.last_progress_at,
                view.first_seen_at
            );
        }

        for divergence in &self.divergences {
            let _ = writeln!(
                out,
                "\nDIVERGENCE at n={} (detected at {}):\n  {} showed 0x{:016x}\n  {} showed 0x{:016x}",
                divergence.n,
                divergence.observed_at,
                divergence.first.0,
                divergence.first.1,
                divergence.other.0,
                divergence.other.1
            );
            for (label, replica) in [("first", divergence.first.0), ("other", divergence.other.0)] {
                let _ = writeln!(
                    out,
                    "  {label} ({replica}) transitions around n={}:",
                    divergence.n
                );
                let view = match self.replicas.get(&replica) {
                    Some(view) => view,
                    None => continue,
                };
                if view.transitions.is_empty() {
                    let _ = writeln!(
                        out,
                        "    (none recorded -- this source does not expose per-command digests)"
                    );
                    continue;
                }
                if view.transitions_truncated {
                    let _ = writeln!(
                        out,
                        "    (earlier transitions dropped: per-replica cap of {} reached)",
                        self.transition_cap
                    );
                }
                for transition in view
                    .transitions
                    .iter()
                    .filter(|t| t.after.n + 3 >= divergence.n && t.after.n <= divergence.n + 3)
                {
                    let _ = writeln!(
                        out,
                        "    n={:<5} cmd=0x{:016x}  (n={}, 0x{:016x}) => (n={}, 0x{:016x})",
                        transition.after.n,
                        transition.command_digest,
                        transition.before.n,
                        transition.before.h,
                        transition.after.n,
                        transition.after.h
                    );
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chain::GENESIS;

    fn node(id: u32) -> NodeId {
        NodeId(id)
    }

    fn sample(replica: u32, at: u64, n: u64, h: u64) -> Sample {
        Sample {
            replica: node(replica),
            observed_at: at,
            state: ChainState { n, h },
            command_digest: None,
        }
    }

    #[test]
    fn agreeing_replicas_produce_no_divergence_but_do_produce_comparisons() {
        let mut observer = Observer::new();
        for n in 0..5 {
            for replica in 0..3 {
                observer.observe(sample(replica, n, n, 1000 + n));
            }
        }
        assert!(observer.divergences().is_empty());
        // 3 replicas per n, 5 values of n: 2 cross-replica comparisons each.
        assert_eq!(observer.comparisons(), 10);
    }

    #[test]
    fn a_differing_hash_at_the_same_n_is_a_divergence() {
        let mut observer = Observer::new();
        observer.observe(sample(0, 1, 7, 0xaaaa));
        observer.observe(sample(1, 2, 7, 0xbbbb));

        let divergences = observer.divergences();
        assert_eq!(divergences.len(), 1);
        assert_eq!(divergences[0].n, 7);
        assert_eq!(divergences[0].first, (node(0), 0xaaaa));
        assert_eq!(divergences[0].other, (node(1), 0xbbbb));
        assert_eq!(divergences[0].observed_at, 2);
    }

    #[test]
    fn re_sampling_one_replica_at_the_same_n_is_not_a_comparison() {
        let mut observer = Observer::new();
        observer.observe(sample(0, 1, 3, 0xaaaa));
        observer.observe(sample(0, 2, 3, 0xaaaa));
        assert_eq!(
            observer.comparisons(),
            0,
            "a replica agreeing with itself is not evidence of anything"
        );
        assert!(observer.divergences().is_empty());
    }

    #[test]
    fn stalls_report_only_replicas_that_are_behind_and_frozen() {
        let mut observer = Observer::new();
        // Replica 0 advances to n=5 at time 50; replica 1 stops at n=2 at
        // time 10; replica 2 is level with 0.
        for n in 0..=5 {
            observer.observe(sample(0, n * 10, n, 1000 + n));
        }
        for n in 0..=2 {
            observer.observe(sample(1, n * 5, n, 1000 + n));
        }
        for n in 0..=5 {
            observer.observe(sample(2, n * 10, n, 1000 + n));
        }

        // Budget generous enough to forgive replica 1: no stall.
        assert!(observer.stalls(60, 100).is_empty());

        let stalls = observer.stalls(60, 20);
        assert_eq!(stalls.len(), 1, "only replica 1 is behind and frozen");
        assert_eq!(stalls[0].replica, node(1));
        assert_eq!(stalls[0].stuck_at.n, 2);
        assert_eq!(stalls[0].cluster_frontier, 5);
    }

    #[test]
    fn a_lagging_but_still_moving_replica_is_not_a_stall() {
        let mut observer = Observer::new();
        for n in 0..=9 {
            observer.observe(sample(0, n, n, 2000 + n));
        }
        // Behind, but advanced as recently as time 8.
        for n in 0..=4 {
            observer.observe(sample(1, n + 4, n, 2000 + n));
        }
        assert!(
            observer.stalls(9, 2).is_empty(),
            "lagging is allowed (P5); not advancing at all is what liveness forbids"
        );
    }

    #[test]
    fn transitions_are_recorded_when_the_source_supplies_digests() {
        let mut observer = Observer::new();
        let mut state = ChainState::genesis();
        observer.observe(Sample {
            replica: node(0),
            observed_at: 0,
            state,
            command_digest: None,
        });
        for i in 0..3u64 {
            state = ChainState {
                n: state.n + 1,
                h: state.h ^ (i + 1),
            };
            observer.observe(Sample {
                replica: node(0),
                observed_at: i + 1,
                state,
                command_digest: Some(0xd00d + i),
            });
        }

        let transitions = observer.transitions(node(0));
        assert_eq!(transitions.len(), 3);
        assert_eq!(transitions[0].before, ChainState { n: 0, h: GENESIS });
        assert_eq!(transitions[0].command_digest, 0xd00d);
        assert_eq!(transitions[2].after.n, 3);
    }

    #[test]
    fn the_transition_cap_bounds_memory_and_is_disclosed_in_the_report() {
        let mut observer = Observer::new().with_transition_cap(2);
        let mut state = ChainState::genesis();
        for i in 0..6u64 {
            state = ChainState {
                n: state.n + 1,
                h: state.h ^ (i + 1),
            };
            observer.observe(Sample {
                replica: node(0),
                observed_at: i,
                state,
                command_digest: Some(i),
            });
        }
        assert_eq!(observer.transitions(node(0)).len(), 2);

        // Force a divergence so the report renders the transition section.
        observer.observe(sample(1, 99, state.n, state.h ^ 0xffff));
        let report = observer.render_report();
        assert!(
            report.contains("per-replica cap"),
            "a truncated history must say so; report was:\n{report}"
        );
    }

    #[test]
    fn the_report_names_both_replicas_and_the_divergent_n() {
        let mut observer = Observer::new();
        observer.observe(sample(0, 1, 4, 0x1111));
        observer.observe(sample(1, 2, 4, 0x2222));
        let report = observer.render_report();
        assert!(report.contains("DIVERGENCE at n=4"), "{report}");
        assert!(report.contains("0x0000000000001111"), "{report}");
        assert!(report.contains("0x0000000000002222"), "{report}");
    }
}
