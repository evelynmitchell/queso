//! Phase 6: auto-tuning (§5.3, D4) -- the explore/exploit multi-armed-bandit
//! layer that chooses *which* replica leads and *what* hedging schedule
//! (Phase 5, §5.1-5.2) the rest of the cluster follows, instead of the fixed
//! configuration Phase 5 required a caller to pick up front.
//!
//! # Epochs
//!
//! [`EpochTuner`] groups the replicated log's slots into fixed-length
//! **epochs** (`epoch_len` slots each), each epoch having exactly one stable
//! leader and one hedging schedule for its whole duration -- `epoch_of(slot)
//! = slot / epoch_len`. This mirrors the paper's own framing directly: "QuePaxa
//! divides SMR slots into fixed-length epochs each with a stable leader."
//!
//! # Explore
//!
//! For the first `2n+1` epochs (`n` = replica count), the tuner rotates the
//! leader round-robin, one replica per epoch (`replicas[epoch % n]`) --
//! giving every replica at least two epochs as leader (one replica gets a
//! third, since `2n+1` does not divide evenly by `n`). Every slot's
//! completion latency (wall/virtual time from its first proposal attempt to
//! its first decision, see [`Self::note_attempt_start`]/
//! [`Self::note_slot_decided`]) observed during an epoch is folded into a
//! running average for that epoch's leader.
//!
//! # Exploit
//!
//! Once exploration finishes (`epoch == 2n+1`), the tuner forms a hedging
//! schedule with replicas ranked by their observed average completion time,
//! **fastest first** -- the fastest replica becomes leader at δ=0, and the
//! rest are staggered by the Phase-5 base delay in ascending speed order
//! (exactly [`crate::cluster::SmrCluster`]'s existing hedging-schedule shape,
//! just with the rank order now *learned* instead of handed in). The tuner
//! keeps recomputing the schedule every epoch from then on, but the only
//! thing that can change the *leader* is monitoring: each epoch, the current
//! leader's running average is compared against the next-ranked replica's;
//! if the leader has fallen behind, the tuner switches leaders (promotes the
//! next-ranked replica) -- no crash or timeout is required to trigger this,
//! matching §5.3's "not proactively explor[ing] other leaders unless the
//! current leader's performance falls below that of the next in the
//! schedule." Per the task brief, the paper's footnote-7 "restless bandits"
//! refinement (periodically re-exploring non-leader replicas to detect
//! *improvements* elsewhere) is deliberately out of scope here.
//!
//! # No hysteresis, and why that is safe but not free (issue #29)
//!
//! The switch rule above has no minimum dwell time and no margin: a leader
//! is replaced the moment its average falls behind the next-ranked
//! replica's, by any amount. That is faithful to §5.3's literal wording,
//! and it is deliberate — but it means an adversary with oracle-level
//! control of delivery timing can keep the comparison flipping and force a
//! leader change every epoch.
//!
//! What that can and cannot cost is worth being precise about. It is never
//! a safety or liveness failure: P15 holds regardless of how the leader is
//! chosen, every epoch's configuration stays pinned once assigned (see
//! [`EpochConfig`]), and the section above explains why nothing this module
//! produces can affect Agreement. The cost is latency and churn — each
//! switch re-ranks the hedging schedule, so the fast path keeps being handed
//! to a replica that has not settled.
//!
//! A small dwell-time or margin would damp that, at the price of reacting
//! more slowly to a leader that has genuinely degraded — which is the
//! failure the monitoring exists to catch in the first place. Left as a
//! future refinement rather than guessed at, since choosing the margin
//! wants a workload to measure against, not an invented constant.
//!
//! # Why this can never be a safety mechanism
//!
//! Everything this module produces -- a `leader: NodeId` and a
//! `delays: BTreeMap<NodeId, u64>` -- is exactly the same shape of input
//! [`crate::replica::SmrNode`] already consumed from a *fixed*
//! caller-supplied config before this phase (see `crate::replica`'s
//! `LeaderPolicy`). This module only ever *chooses* those values; it never
//! touches ISR/quorum/decision/catch-up logic. The Phase-5 safety argument
//! (`queso_consensus::proposer`'s module docs: hedging "is deliberately not
//! a safety mechanism... only ever changes *when* `begin_step` first runs")
//! therefore covers auto-tuning unchanged -- a wrong or wildly oscillating
//! leader/schedule choice can only ever cost latency, never Agreement.
//!
//! # A simplification worth being explicit about
//!
//! In a real deployment, replicas would need to *agree* on tuning decisions
//! (every proposer for a given slot must be built with the same `leader`
//! value -- see `queso_consensus::proposer::Proposer::new`'s invariant) via
//! some out-of-band mechanism the paper does not detail (e.g. piggybacked on
//! already-decided log entries, or a side channel). This harness, like the
//! rest of `crate::cluster::SmrCluster`'s existing shared-handle driver
//! architecture (`results`, `leader` were already single caller-fixed
//! values before this phase), takes the simpler route of a single shared
//! `Rc<RefCell<EpochTuner>>` all replicas consult -- deterministic and
//! trivially agreement-consistent by construction, but it does not model
//! *how* real replicas would reach that agreement. This is called out
//! explicitly rather than left implicit because it is the one place this
//! phase leans on the simulation harness being single-process.

use std::collections::{BTreeMap, BTreeSet};

use queso_sim::ids::NodeId;
use queso_sim::time::LogicalTime;

/// One epoch's pinned configuration: its leader and the full hedging
/// schedule (leader first) derived from it. Stored per-epoch, forever
/// (`epoch_configs` below), so that a replica proposing "late" against an
/// old slot (the reads-through-log catch-up mechanism, `crate::cluster`'s
/// module docs) always reconstructs the *exact* `leader` value every other
/// proposer for that slot ever used or will use -- required for
/// `Proposer::new`'s "every proposer for the same slot must be built with
/// the same leader value" invariant, which the phase-0 fast path's safety
/// argument depends on.
#[derive(Debug, Clone)]
struct EpochConfig {
    leader: NodeId,
    /// Full ranked order, leader first; rank `k`'s delay is `k * base_delay`.
    schedule: Vec<NodeId>,
}

/// A running average of observed epoch-completion times for one replica
/// (as leader).
#[derive(Debug, Clone, Copy, Default)]
struct RunningAvg {
    sum: u64,
    count: u64,
}

impl RunningAvg {
    fn add(&mut self, sample: u64) {
        self.sum += sample;
        self.count += 1;
    }

    fn avg(&self) -> Option<u64> {
        (self.count > 0).then(|| self.sum / self.count)
    }
}

/// The explore/exploit leader + hedging-schedule tuner for one
/// [`crate::cluster::SmrCluster`] run. See the module docs for the full
/// design; in short: fixed-length epochs of consensus-log slots, round-robin
/// leader rotation for the first `2n+1` epochs while recording each leader's
/// observed average completion time, then a fastest-first hedging schedule
/// re-derived every epoch, switching leaders only when the current one
/// measurably degrades relative to the next-ranked replica.
pub struct EpochTuner {
    replicas: Vec<NodeId>,
    epoch_len: u64,
    base_delay: u64,
    /// `2n + 1` -- the paper's exploration length (§5.3).
    explore_epochs: u64,
    /// The epoch currently accumulating samples (`epoch_samples`).
    epoch: u64,
    leader: NodeId,
    /// Full ranked order for the *current* epoch, leader first.
    schedule: Vec<NodeId>,
    /// Per-replica running average of observed epoch-completion times,
    /// updated only for whichever replica led the epoch a sample came from.
    /// Used **only once**, the instant exploration ends, to rank every
    /// replica for the initial exploit leader/schedule -- see
    /// [`Self::recent`]'s docs for why ongoing exploit-phase decisions use a
    /// different, more responsive signal instead of this lifetime average.
    stats: BTreeMap<NodeId, RunningAvg>,
    /// Each replica's **most recently observed** single-epoch completion
    /// time (not accumulated), updated every time it leads a closed epoch.
    /// This -- not [`Self::stats`]'s lifetime average -- is what ongoing
    /// exploit-phase decisions (the monitoring/switch check, and the
    /// hedging schedule's backup ordering) are based on.
    ///
    /// Why: a lifetime average mixes a replica's fresh performance in with
    /// however many epochs of *stale* history it has accumulated (all `2-3`
    /// explore-phase epochs, for a leader that has been exploiting for a
    /// while) -- so a genuinely degraded incumbent's lifetime average can
    /// stay artificially good for a long time after it actually degrades,
    /// directly undermining §5.3's "switch once the current leader falls
    /// below the next in the schedule" trigger (D4's whole point:
    /// responding to degradation *without* a crash). Using only the most
    /// recent epoch instead makes the monitor react within a single epoch,
    /// at the cost of being more sensitive to one-off noise -- a smoothing
    /// refinement (e.g. a short recency-weighted moving average) is exactly
    /// the kind of "restless bandit" elaboration the task brief calls out
    /// as deliberately out of scope here.
    recent: BTreeMap<NodeId, u64>,
    /// Per-slot latency samples collected for the epoch currently open
    /// (`epoch`), not yet folded into `stats`.
    epoch_samples: Vec<u64>,
    /// First-seen attempt-start time per slot (any origin), used to compute
    /// that slot's completion latency once it decides.
    slot_started: BTreeMap<u64, LogicalTime>,
    /// Slots whose completion latency has already been recorded -- a slot's
    /// decision can be independently observed by more than one replica (or
    /// more than one attempt on the same replica); only the first counts.
    slot_recorded: BTreeSet<u64>,
    /// Every epoch's pinned configuration, forever -- see [`EpochConfig`]'s
    /// docs for why this must never be overwritten or forgotten.
    epoch_configs: BTreeMap<u64, EpochConfig>,
    /// The leader assigned to each epoch, in epoch order -- test/
    /// introspection only (exploration-coverage and re-exploration checks).
    leader_log: Vec<NodeId>,
    /// How many times the exploit phase has switched leaders away from a
    /// degraded incumbent (§5.3's monitoring trigger) -- test/introspection
    /// only.
    switch_count: u64,
}

impl EpochTuner {
    /// Build a tuner over `replicas` (deduplicated/sorted internally),
    /// grouping the log into `epoch_len`-slot epochs (clamped to at least
    /// `1`) and using `base_delay` as the Phase-5 δ for the hedging
    /// schedules it constructs. Starts in explore mode, epoch 0, leader
    /// `replicas[0]`.
    pub fn new(mut replicas: Vec<NodeId>, epoch_len: u64, base_delay: u64) -> Self {
        replicas.sort();
        replicas.dedup();
        assert!(
            !replicas.is_empty(),
            "EpochTuner needs at least one replica"
        );
        let epoch_len = epoch_len.max(1);
        let n = replicas.len() as u64;
        let explore_epochs = 2 * n + 1;
        let leader = replicas[0];
        let schedule = Self::leader_first(leader, &replicas);
        let mut epoch_configs = BTreeMap::new();
        epoch_configs.insert(
            0,
            EpochConfig {
                leader,
                schedule: schedule.clone(),
            },
        );
        Self {
            replicas,
            epoch_len,
            base_delay,
            explore_epochs,
            epoch: 0,
            leader,
            schedule,
            stats: BTreeMap::new(),
            recent: BTreeMap::new(),
            epoch_samples: Vec::new(),
            slot_started: BTreeMap::new(),
            slot_recorded: BTreeSet::new(),
            epoch_configs,
            leader_log: vec![leader],
            switch_count: 0,
        }
    }

    /// `leader` first, the rest in ascending `NodeId` order -- the Phase-5
    /// default schedule shape (`ConcreteCluster::new_with_schedule`), used
    /// whenever there is not yet any speed data to rank by.
    fn leader_first(leader: NodeId, replicas: &[NodeId]) -> Vec<NodeId> {
        let mut rest: Vec<NodeId> = replicas.iter().copied().filter(|&r| r != leader).collect();
        rest.sort();
        let mut order = vec![leader];
        order.extend(rest);
        order
    }

    /// `leader` first, the rest ranked ascending by each replica's most
    /// *recent* single-epoch completion time (fastest first; missing data
    /// sorts last; ties break by `NodeId` for determinism) -- used for the
    /// ongoing exploit phase's schedule, per [`Self::recent`]'s docs.
    fn leader_first_by_speed(
        leader: NodeId,
        replicas: &[NodeId],
        recent: &BTreeMap<NodeId, u64>,
    ) -> Vec<NodeId> {
        let mut rest: Vec<NodeId> = replicas.iter().copied().filter(|&r| r != leader).collect();
        rest.sort_by_key(|r| (recent.get(r).copied().unwrap_or(u64::MAX), *r));
        let mut order = vec![leader];
        order.extend(rest);
        order
    }

    /// Every replica ranked ascending by observed average completion time
    /// (fastest first; ties by `NodeId`) -- used exactly once, the instant
    /// exploration ends, to pick the initial exploit leader.
    fn ranked_by_speed(replicas: &[NodeId], stats: &BTreeMap<NodeId, RunningAvg>) -> Vec<NodeId> {
        let mut order: Vec<NodeId> = replicas.to_vec();
        order.sort_by_key(|r| {
            (
                stats.get(r).and_then(RunningAvg::avg).unwrap_or(u64::MAX),
                *r,
            )
        });
        order
    }

    /// Which epoch `slot` falls in.
    pub fn epoch_of(&self, slot: u64) -> u64 {
        slot / self.epoch_len
    }

    fn config_for(&self, slot: u64) -> &EpochConfig {
        let epoch = self.epoch_of(slot);
        self.epoch_configs.get(&epoch).unwrap_or_else(|| {
            panic!(
                "EpochTuner asked for slot {slot} (epoch {epoch}) before that epoch was ever \
                 opened -- this violates the causality invariant every caller relies on (a \
                 replica only ever attempts slot k after it has itself observed slot k-1 \
                 decide, which is exactly what opens the next epoch's config -- see the module \
                 docs)"
            )
        })
    }

    /// The `leader` value every [`queso_consensus::proposer::Proposer`] for
    /// `slot` must be built with. Stable forever once assigned (see
    /// [`EpochConfig`]'s docs) -- safe to call for a slot whose epoch has
    /// long since closed (a lagging replica's late catch-up attempt).
    pub fn leader_for_slot(&self, slot: u64) -> NodeId {
        self.config_for(slot).leader
    }

    /// This replica's hedging activation delay for `slot`, per that slot's
    /// pinned schedule (rank in [`EpochConfig::schedule`] times the
    /// configured base delay).
    pub fn delay_for_slot(&self, slot: u64, id: NodeId) -> u64 {
        let cfg = self.config_for(slot);
        let rank = cfg
            .schedule
            .iter()
            .position(|&r| r == id)
            .unwrap_or(cfg.schedule.len());
        rank as u64 * self.base_delay
    }

    /// Record that some replica began its first attempt at `slot` at `now`
    /// (a no-op for any later attempt at the same slot -- only the first
    /// counts). Called for every attempt origin (real op or internal
    /// catch-up probe); see [`Self::note_slot_decided`] for why measurement
    /// itself does not need to distinguish them.
    pub fn note_attempt_start(&mut self, slot: u64, now: LogicalTime) {
        self.slot_started.entry(slot).or_insert(now);
    }

    /// Record that `slot` has decided (observed by some replica) at `now`.
    /// Idempotent per slot -- only the first observer's timing counts,
    /// whichever replica that happens to be; a slot's decision is a single,
    /// well-defined event in virtual time even though different replicas
    /// discover it at different (real, per-replica) moments.
    pub fn note_slot_decided(&mut self, slot: u64, now: LogicalTime) {
        if !self.slot_recorded.insert(slot) {
            return;
        }
        let latency = self
            .slot_started
            .get(&slot)
            .map_or(0, |started| now.0.saturating_sub(started.0));
        self.absorb(slot, latency);
    }

    fn absorb(&mut self, slot: u64, latency: u64) {
        debug_assert_eq!(
            self.epoch_of(slot),
            self.epoch,
            "slot {slot} decided for an epoch other than the currently open one -- this would \
             mean some slot was decided out of order, which the module docs' causality argument \
             rules out (every slot's first decision is observed in non-decreasing slot order, \
             and an epoch closes -- see below -- the instant its last slot's decision is \
             observed, strictly before any later slot's attempt could even begin)"
        );
        self.epoch_samples.push(latency);
        // Close the epoch the instant every one of its slots has decided,
        // rather than waiting to observe a slot in the *next* epoch: this
        // is both the earliest point at which it is safe to do so (the next
        // epoch's first slot cannot yet have been attempted -- a replica
        // only attempts a slot after its own predecessor decided) and
        // exactly the freshest possible moment, so a caller resolving the
        // next epoch's leader/schedule (`leader_for_slot`/`delay_for_slot`)
        // immediately after this slot's decision always finds it ready.
        if self.epoch_samples.len() as u64 >= self.epoch_len {
            self.close_epoch();
        }
    }

    /// Finalize the currently-open epoch's samples into `stats`, then
    /// compute and pin the *next* epoch's leader + schedule -- either the
    /// next round-robin explorer, the first exploit schedule, or an ongoing
    /// exploit epoch (possibly switching leaders, §5.3's monitoring trigger).
    fn close_epoch(&mut self) {
        if !self.epoch_samples.is_empty() {
            let sum: u64 = self.epoch_samples.iter().sum();
            let avg = sum / self.epoch_samples.len() as u64;
            self.stats.entry(self.leader).or_default().add(avg);
            self.recent.insert(self.leader, avg);
        }
        self.epoch_samples.clear();

        let next = self.epoch + 1;
        let (leader, schedule) = if next < self.explore_epochs {
            // Still exploring: round-robin to the next replica, no speed
            // data to rank backups by yet.
            let leader = self.replicas[(next % self.replicas.len() as u64) as usize];
            (leader, Self::leader_first(leader, &self.replicas))
        } else if next == self.explore_epochs {
            // Exploration just finished: exploit by picking the fastest
            // observed replica as leader (by its full explore-phase
            // average -- see `Self::stats`'s docs for why the lifetime
            // average is the right signal exactly once, here) and ranking
            // everyone else by speed (§5.3).
            let ranked = Self::ranked_by_speed(&self.replicas, &self.stats);
            let leader = ranked[0];
            (leader, ranked)
        } else {
            // Ongoing exploit: keep monitoring the current leader using
            // each replica's most *recent* single-epoch completion time
            // (see `Self::recent`'s docs for why not the lifetime average).
            // Switch only if the leader has measurably fallen behind the
            // next-ranked replica in the schedule -- no crash/timeout
            // required (D4).
            let mut leader = self.leader;
            if let Some(&candidate) = self.schedule.get(1) {
                if let (Some(&current_avg), Some(&candidate_avg)) =
                    (self.recent.get(&leader), self.recent.get(&candidate))
                {
                    if current_avg > candidate_avg {
                        leader = candidate;
                        self.switch_count += 1;
                    }
                }
            }
            let schedule = Self::leader_first_by_speed(leader, &self.replicas, &self.recent);
            (leader, schedule)
        };

        self.epoch = next;
        self.leader = leader;
        self.schedule = schedule.clone();
        self.epoch_configs
            .insert(next, EpochConfig { leader, schedule });
        self.leader_log.push(leader);
    }

    /// The current epoch's leader.
    pub fn leader(&self) -> NodeId {
        self.leader
    }

    /// The current epoch's full hedging schedule, leader first.
    pub fn schedule(&self) -> &[NodeId] {
        &self.schedule
    }

    /// The current epoch's per-replica hedging delays, derived from
    /// [`Self::schedule`] (rank `k` gets delay `k * base_delay`).
    pub fn delays(&self) -> BTreeMap<NodeId, u64> {
        self.schedule
            .iter()
            .enumerate()
            .map(|(rank, &id)| (id, rank as u64 * self.base_delay))
            .collect()
    }

    /// The epoch currently open for new samples.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// `2n + 1`: how many epochs the explore phase lasts.
    pub fn explore_epochs(&self) -> u64 {
        self.explore_epochs
    }

    /// Whether the tuner is still in its initial round-robin exploration
    /// phase.
    pub fn is_exploring(&self) -> bool {
        self.epoch < self.explore_epochs
    }

    /// How many times the exploit phase has switched leaders away from a
    /// degraded incumbent so far.
    pub fn switch_count(&self) -> u64 {
        self.switch_count
    }

    /// The leader assigned to every epoch so far, in epoch order
    /// (`leader_log()[e]` is epoch `e`'s leader).
    pub fn leader_log(&self) -> &[NodeId] {
        &self.leader_log
    }

    /// `replica`'s most recently observed single-epoch completion time (as
    /// leader), if it has led at least one epoch with at least one measured
    /// slot. This is [`Self::recent`], not a lifetime average -- see that
    /// field's docs for why.
    pub fn average_for(&self, replica: NodeId) -> Option<u64> {
        self.recent.get(&replica).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn replicas(n: u32) -> Vec<NodeId> {
        (0..n).map(NodeId).collect()
    }

    #[test]
    fn starts_exploring_with_replica_zero_as_leader() {
        let t = EpochTuner::new(replicas(3), 4, 10);
        assert!(t.is_exploring());
        assert_eq!(t.leader(), NodeId(0));
        assert_eq!(t.explore_epochs(), 7); // 2*3+1
        assert_eq!(t.epoch(), 0);
    }

    #[test]
    fn leader_for_slot_matches_epoch_of_slot() {
        let t = EpochTuner::new(replicas(3), 4, 10);
        assert_eq!(t.epoch_of(0), 0);
        assert_eq!(t.epoch_of(3), 0);
        assert_eq!(t.epoch_of(4), 1);
        assert_eq!(t.leader_for_slot(0), t.leader());
    }

    #[test]
    fn explore_rotates_round_robin_and_gives_every_replica_two_epochs() {
        let mut t = EpochTuner::new(replicas(3), 2, 5);
        // 2n+1 = 7 epochs, epoch_len = 2 slots each -> 14 slots for explore.
        for slot in 0..14u64 {
            t.note_attempt_start(slot, LogicalTime(slot * 10));
            t.note_slot_decided(slot, LogicalTime(slot * 10 + 5));
        }
        assert!(!t.is_exploring(), "should have finished exploring");
        let log = &t.leader_log()[..7];
        for r in 0..3u32 {
            let count = log.iter().filter(|&&x| x == NodeId(r)).count();
            assert!(count >= 2, "replica {r} led only {count} explore epochs");
        }
    }

    #[test]
    fn exploit_ranks_fastest_replica_as_leader() {
        let mut t = EpochTuner::new(replicas(3), 2, 5);
        // Drive through the 7 explore epochs (14 slots), making whichever
        // replica leads epoch e take `latency` proportional to its id, so
        // replica 0 is always fastest and replica 2 always slowest.
        for slot in 0..14u64 {
            let epoch = slot / 2;
            let leader = NodeId((epoch % 3) as u32);
            let latency = 10 + leader.0 as u64 * 100;
            t.note_attempt_start(slot, LogicalTime(0));
            t.note_slot_decided(slot, LogicalTime(latency));
        }
        assert!(!t.is_exploring());
        assert_eq!(t.leader(), NodeId(0), "fastest replica should be leader");
        assert_eq!(t.schedule(), &[NodeId(0), NodeId(1), NodeId(2)]);
    }

    #[test]
    fn exploit_switches_leader_once_it_degrades_below_the_next_in_schedule() {
        let mut t = EpochTuner::new(replicas(3), 2, 5);
        // Explore: replica 0 always fast, replica 1 medium, replica 2 slow.
        for slot in 0..14u64 {
            let epoch = slot / 2;
            let leader = NodeId((epoch % 3) as u32);
            let latency = 10 + leader.0 as u64 * 100;
            t.note_attempt_start(slot, LogicalTime(0));
            t.note_slot_decided(slot, LogicalTime(latency));
        }
        assert_eq!(t.leader(), NodeId(0));
        let switches_before = t.switch_count();

        // Now degrade replica 0 specifically (only when it is still the
        // acting leader; once the tuner switches away, whoever leads next
        // stays fast) for several more epochs -- no crash involved.
        let mut slot = 14u64;
        for _ in 0..6 {
            let latency = if t.leader() == NodeId(0) { 5_000 } else { 15 };
            for _ in 0..2 {
                t.note_attempt_start(slot, LogicalTime(0));
                t.note_slot_decided(slot, LogicalTime(latency));
                slot += 1;
            }
        }
        assert!(
            t.switch_count() > switches_before,
            "should have switched away from the degraded leader"
        );
        assert_ne!(t.leader(), NodeId(0));
    }

    #[test]
    fn delays_are_zero_for_leader_and_increase_with_rank() {
        let t = EpochTuner::new(replicas(3), 4, 7);
        let delays = t.delays();
        assert_eq!(delays[&t.leader()], 0);
        let mut values: Vec<u64> = delays.values().copied().collect();
        values.sort();
        assert_eq!(values, vec![0, 7, 14]);
    }
}
