//! The seam between the Chain-of-Blocks harness and whatever cluster it is
//! watching.
//!
//! [`CobTarget`] is the whole interface a conformance run needs: submit a
//! command, let time pass, sample every replica's chain state. Phase 9.1
//! ships one implementation, [`SimCluster`], over the in-process
//! `queso_smr::SmrCluster`. Phase 9.2 (#56) adds a second one over real
//! `queso-node` OS processes -- new implementation, *same* [`crate::observer`]
//! and [`crate::workload`] above it.
//!
//! # Observability is a parameter, not an accident
//!
//! The in-process implementation can see everything: it reads each
//! replica's whole applied log, so it can emit the complete `n -> h` table.
//! A real-process source will not be able to -- it will poll an endpoint
//! and learn a replica's *current* frontier and nothing about the states it
//! passed through in between.
//!
//! Rather than pretend that difference away, [`Observability`] makes it a
//! knob, and running the same scenario under each mode is how this harness
//! establishes what a weakly-observable source can and cannot catch:
//!
//! - [`Observability::FullPrefix`] -- every state the replica passed
//!   through. Only an in-process source can do this.
//! - [`Observability::FrontierOnly`] -- just "where is this replica now",
//!   which is all `queso-net`'s `/metrics` exposes today. **This turns out
//!   to be nearly useless for safety checking**: replicas lag each other,
//!   so two frontier samples almost never share an `n`, and the observer
//!   ends a clean-looking run having compared *nothing*.
//!   `tests/imperfect_observability.rs` pins that result.
//! - [`Observability::Checkpoints`] -- `h` reported at fixed `n`
//!   boundaries. Comparisons align by construction, detection works, and a
//!   real node can implement it by retaining a small table of checkpoint
//!   hashes. **This is the shape Phase 9.2 (#56) should expose**, and the
//!   concrete design finding this phase contributes to it.

use std::collections::BTreeMap;

use queso_sim::ids::NodeId;
use queso_smr::{Command, SmrCluster};

use crate::chain::{command_digest, ChainState};
use crate::observer::Sample;

/// How densely a source reports the chain states a replica passed through.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Observability {
    /// Emit a sample for every `n` the replica has passed through since the
    /// last poll. Only a source that can read the applied log itself (i.e.
    /// an in-process one) can do this.
    FullPrefix,
    /// Emit only the replica's current frontier state per poll,
    /// deliberately discarding the states in between -- the observability a
    /// real-process source gets from polling a "how far along are you?"
    /// endpoint such as `queso-net`'s `/metrics` `next_slot`.
    ///
    /// **This mode is close to useless for safety checking, by
    /// construction, and exists to demonstrate that.** Replicas are almost
    /// never at the same `n` at the same instant (they lag each other by
    /// design), so two frontier samples rarely share an `n`, and the
    /// observer has nothing to compare -- it reports zero divergences
    /// having made zero comparisons. See
    /// `tests/imperfect_observability.rs`, which pins that behavior, and
    /// prefer [`Observability::Checkpoints`] for anything that must
    /// actually catch divergence.
    FrontierOnly,
    /// Emit a sample each time a replica crosses a multiple of `every` --
    /// i.e. report `h` at `n = every, 2*every, 3*every, ...`.
    ///
    /// This is the mode a real-process source should implement in Phase 9.2
    /// (#56), and it is implementable there: a node need only retain the
    /// chain hash at each checkpoint it crosses and expose that small
    /// table. Because every replica reports at the *same* `n` values,
    /// comparisons align by construction instead of by luck, which is
    /// exactly what [`Observability::FrontierOnly`] fails to achieve.
    ///
    /// Each poll also reports the replica's current frontier state, the way
    /// a real source polling `/metrics` alongside a checkpoint table would.
    /// Those extra samples are what keep the liveness observer informed
    /// about replicas that are not advancing at all.
    Checkpoints {
        /// Checkpoint spacing in slots. Values below 1 are treated as 1.
        every: u64,
    },
}

/// A cluster a Chain-of-Blocks run can drive and observe.
///
/// Deliberately small: everything the workload and observers need, and
/// nothing that assumes an in-process cluster. `advance` is the only
/// method whose meaning differs between implementations -- ticks of virtual
/// time in the sim, elapsed real time against real processes -- which is
/// why the unit is left to the implementation and documented there.
pub trait CobTarget {
    /// Every replica in the cluster, live or not.
    fn replicas(&self) -> Vec<NodeId>;

    /// Submit one command to some replica of the target's choosing (the
    /// workload does not care which; spreading submissions around is the
    /// implementation's business).
    fn submit(&mut self, command: Command);

    /// Let `units` of the target's own time pass.
    fn advance(&mut self, units: u64);

    /// The target's current time, on the same clock `advance` uses. This
    /// is what lands in [`Sample::observed_at`].
    fn now(&self) -> u64;

    /// Sample every replica's chain state, according to this target's
    /// observability.
    fn poll_samples(&mut self) -> Vec<Sample>;
}

/// [`CobTarget`] over the in-process, deterministic `queso_smr::SmrCluster`.
///
/// Owns the cluster so that observation and mutation don't fight over the
/// borrow; [`Self::cluster_mut`] hands it back out for fault injection
/// (crash/restart/slow), which is scenario-specific and deliberately not
/// part of the [`CobTarget`] interface.
pub struct SimCluster {
    cluster: SmrCluster,
    observability: Observability,
    /// Per-replica incremental fold state: the chain state corresponding to
    /// the prefix of that replica's applied log already turned into
    /// samples. Keeping it means each poll folds only the *new* commands
    /// rather than rehashing the whole log.
    cursors: BTreeMap<NodeId, ChainState>,
    /// Round-robin cursor over live replicas for `submit`.
    next_submit: usize,
    /// Monotonic `(client, seq)` counter, so no submission is ever
    /// deduplicated away by A6/P8a.
    seq: u64,
}

impl SimCluster {
    /// Wrap a cluster with the given observability.
    pub fn new(cluster: SmrCluster, observability: Observability) -> Self {
        Self {
            cluster,
            observability,
            cursors: BTreeMap::new(),
            next_submit: 0,
            seq: 0,
        }
    }

    /// The underlying cluster, for fault injection and for assertions that
    /// want ground truth (e.g. comparing applied logs directly).
    pub fn cluster(&self) -> &SmrCluster {
        &self.cluster
    }

    /// Mutable access, for `crash`/`restart`/`set_slow`/`clear_slow`.
    pub fn cluster_mut(&mut self) -> &mut SmrCluster {
        &mut self.cluster
    }

    /// The chain state of a replica computed directly from its applied log
    /// -- ground truth, independent of what has been sampled so far.
    pub fn true_state(&self, replica: NodeId) -> ChainState {
        ChainState::from_log(&self.cluster.applied_log(replica))
    }
}

impl CobTarget for SimCluster {
    fn replicas(&self) -> Vec<NodeId> {
        self.cluster.replicas().to_vec()
    }

    fn submit(&mut self, command: Command) {
        let live: Vec<NodeId> = self.cluster.live().iter().copied().collect();
        if live.is_empty() {
            return;
        }
        let replica = live[self.next_submit % live.len()];
        self.next_submit = self.next_submit.wrapping_add(1);

        // Re-tag the command with this target's own monotonic sequence
        // number for the client it names, so a workload that loops forever
        // can never collide with the dedup table (A6/P8a) and have a
        // command silently absorbed.
        self.seq += 1;
        let seq = self.seq;
        let command = match command {
            Command::Put {
                client, key, value, ..
            } => Command::Put {
                client,
                seq,
                key,
                value,
            },
            Command::Get { client, key, .. } => Command::Get { client, seq, key },
        };
        self.cluster.submit(replica, command);
    }

    /// Runs the sim kernel forward by `units` ticks of virtual time.
    fn advance(&mut self, units: u64) {
        self.cluster.run_for(units);
    }

    fn now(&self) -> u64 {
        self.cluster.now().0
    }

    fn poll_samples(&mut self) -> Vec<Sample> {
        let now = self.cluster.now().0;
        let mut samples = Vec::new();

        for replica in self.cluster.replicas().to_vec() {
            let log = self.cluster.applied_log(replica);
            let cursor = *self
                .cursors
                .entry(replica)
                .or_insert_with(ChainState::genesis);

            // A replica's applied log only ever grows, and never rewrites a
            // slot it already applied (P5/P6) -- but this harness must not
            // *assume* that to compute its samples, or it would be checking
            // the property with the property. If the log is somehow shorter
            // than what we already folded, refold from genesis so the
            // sample reflects the log as it actually is now.
            let mut state = if (log.len() as u64) < cursor.n {
                ChainState::genesis()
            } else {
                cursor
            };

            let start = state.n as usize;

            let mut pending: Vec<Sample> = Vec::new();
            for command in &log[start.min(log.len())..] {
                let transition = state.apply(command);
                pending.push(Sample {
                    replica,
                    observed_at: now,
                    state,
                    command_digest: Some(transition.command_digest),
                });
            }

            self.cursors.insert(replica, state);

            // Every poll reports where the replica is *now*, in every mode.
            // Without this, a replica that has applied nothing -- a crashed
            // one, or one that never got work -- would emit no samples at
            // all, and the liveness observer would never learn it exists.
            // (A real source gets this for free: `queso-net`'s `/metrics`
            // already reports `next_slot` on every poll.)
            let frontier_sample = Sample {
                replica,
                observed_at: now,
                state,
                command_digest: pending.last().and_then(|s| s.command_digest),
            };

            match self.observability {
                Observability::FullPrefix => {
                    if pending.is_empty() {
                        samples.push(frontier_sample);
                    } else {
                        // The last pending sample *is* the frontier sample.
                        samples.extend(pending);
                    }
                }
                Observability::Checkpoints { every } => {
                    let every = every.max(1);
                    let mut emitted_frontier = false;
                    for sample in pending {
                        if sample.state.n % every == 0 {
                            emitted_frontier = sample.state.n == state.n;
                            samples.push(sample);
                        }
                    }
                    if !emitted_frontier {
                        samples.push(frontier_sample);
                    }
                }
                Observability::FrontierOnly => {
                    samples.push(pending.pop().unwrap_or(frontier_sample));
                }
            }
        }

        samples
    }
}

/// Build a [`Sample`] for a replica whose applied log is known in full --
/// the helper a test (or a future source that can dump a log) uses to feed
/// the observer without going through a [`CobTarget`].
pub fn sample_from_log(replica: NodeId, observed_at: u64, log: &[Command]) -> Sample {
    Sample {
        replica,
        observed_at,
        state: ChainState::from_log(log),
        command_digest: log.last().map(command_digest),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use queso_sim::scheduler::{ContentObliviousAdversary, SchedulerKind};
    use queso_smr::ClientId;

    fn healthy_cluster(n: usize) -> SmrCluster {
        SmrCluster::new(
            7,
            SchedulerKind::Oblivious(Box::new(ContentObliviousAdversary::new(1, 4))),
            n,
        )
    }

    fn put(client: u32, key: u32, value: i64) -> Command {
        Command::Put {
            client: ClientId(client),
            seq: 0,
            key,
            value,
        }
    }

    #[test]
    fn full_prefix_emits_every_state_the_replica_passed_through() {
        let mut target = SimCluster::new(healthy_cluster(3), Observability::FullPrefix);
        for i in 0..4 {
            target.submit(put(0, 0, i));
        }
        target.advance(200_000);

        let samples = target.poll_samples();
        let replicas = target.replicas();
        for replica in replicas {
            let mut ns: Vec<u64> = samples
                .iter()
                .filter(|s| s.replica == replica)
                .map(|s| s.state.n)
                .collect();
            ns.sort_unstable();
            assert!(!ns.is_empty(), "{replica} produced no samples");
            // Contiguous from 1 (the genesis state is only emitted as a
            // frontier sample when nothing has been applied yet).
            for (i, n) in ns.iter().enumerate() {
                assert_eq!(*n, i as u64 + 1, "prefix samples must be contiguous");
            }
        }
    }

    #[test]
    fn frontier_only_emits_exactly_one_sample_per_replica_per_poll() {
        let mut target = SimCluster::new(healthy_cluster(3), Observability::FrontierOnly);
        for i in 0..4 {
            target.submit(put(0, 0, i));
        }
        target.advance(200_000);

        let samples = target.poll_samples();
        assert_eq!(samples.len(), target.replicas().len());
        for sample in &samples {
            assert_eq!(
                sample.state,
                target.true_state(sample.replica),
                "a frontier sample must be the replica's actual current state"
            );
        }
    }

    #[test]
    fn polling_twice_with_no_progress_re_reports_the_frontier_but_not_history() {
        let mut target = SimCluster::new(healthy_cluster(3), Observability::FullPrefix);
        target.submit(put(0, 0, 1));
        target.advance(200_000);

        let first = target.poll_samples();
        assert!(!first.is_empty());

        let second = target.poll_samples();
        assert_eq!(
            second.len(),
            target.replicas().len(),
            "a no-progress poll must still report each replica's current state once, so \
             liveness can tell 'not moving' from 'not observed': {second:?}"
        );
        for sample in &second {
            assert_eq!(
                sample.state,
                target.true_state(sample.replica),
                "the re-reported state must be the replica's actual current state"
            );
        }
    }

    #[test]
    fn a_replica_that_has_applied_nothing_is_still_reported() {
        let mut target = SimCluster::new(healthy_cluster(3), Observability::FullPrefix);
        // No submissions at all: every replica is at genesis.
        let samples = target.poll_samples();
        assert_eq!(
            samples.len(),
            target.replicas().len(),
            "every replica must be observable even with an empty log -- otherwise a \
             crashed replica is invisible to the liveness observer: {samples:?}"
        );
        assert!(samples.iter().all(|s| s.state == ChainState::genesis()));
    }

    #[test]
    fn submissions_get_distinct_sequence_numbers_so_dedup_never_absorbs_them() {
        let mut target = SimCluster::new(healthy_cluster(3), Observability::FullPrefix);
        for _ in 0..5 {
            // Same client, same seq as handed in -- the target must re-tag.
            target.submit(put(0, 0, 42));
        }
        target.advance(500_000);

        // Checked against the most-advanced replica, not an arbitrary one:
        // replicas legitimately lag (P5 allows it, and a Queso replica only
        // catches up when it next participates -- see `workload::converge`),
        // so "did every submission get its own slot" is a question about the
        // log that has seen the most, not about any particular replica.
        let longest = target
            .replicas()
            .into_iter()
            .map(|r| target.cluster().applied_log(r))
            .max_by_key(Vec::len)
            .expect("a cluster has at least one replica");
        assert_eq!(
            longest.len(),
            5,
            "all five submissions must occupy their own slot; got {longest:?}"
        );

        let mut seqs: Vec<u64> = longest.iter().map(|c| c.client_seq().1).collect();
        seqs.sort_unstable();
        assert_eq!(
            seqs,
            vec![1, 2, 3, 4, 5],
            "each submission must carry its own sequence number"
        );
    }
}
