//! Fault injection: crash, restart, partition/heal, slow-node.
//!
//! This is deliberately a separate concern from the [`crate::scheduler`]
//! adversary classes. Fault injection is *scripted*: a test or demo calls
//! `Kernel::crash`, `Kernel::partition`, etc. at times of its own choosing
//! (directly, or via `Kernel::schedule_fault` for a fully pre-planned,
//! event-driven scenario). The scheduler adversaries, by contrast, make
//! their own randomized in-the-moment decisions (delay/reorder/drop) as
//! traffic flows. Both interact with the same underlying network: fault
//! state is consulted first (hard drop if crashed or manually partitioned),
//! and only if a message survives that check is the scheduler consulted.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use crate::ids::NodeId;

/// Why a message was dropped, recorded in the trace for post-hoc analysis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropReason {
    /// Source or destination node was crashed at send or arrival time.
    Crashed,
    /// Source and destination were on opposite sides of a manually
    /// injected [`FaultState`] partition.
    Partitioned,
    /// The scheduler (adversary or otherwise) chose to drop the message.
    Scheduler,
}

/// Deterministic, explicitly-injected fault state. Mutated only through
/// `Kernel`'s fault-injection API.
#[derive(Debug, Default)]
pub struct FaultState {
    crashed: BTreeSet<NodeId>,
    /// A manual network partition: two disjoint groups of nodes that
    /// cannot reach each other. `None` means no manual partition is active.
    partition: Option<(BTreeSet<NodeId>, BTreeSet<NodeId>)>,
    /// Per-node delay multiplier applied on top of whatever delay the
    /// scheduler computed. `1` (the default for unlisted nodes) means no
    /// slowdown.
    slow: BTreeMap<NodeId, u64>,
}

impl FaultState {
    pub(crate) fn crash(&mut self, node: NodeId) {
        self.crashed.insert(node);
    }

    pub(crate) fn restart(&mut self, node: NodeId) {
        self.crashed.remove(&node);
    }

    pub(crate) fn is_crashed(&self, node: NodeId) -> bool {
        self.crashed.contains(&node)
    }

    pub(crate) fn set_partition(&mut self, a: BTreeSet<NodeId>, b: BTreeSet<NodeId>) {
        self.partition = Some((a, b));
    }

    pub(crate) fn heal(&mut self) {
        self.partition = None;
    }

    /// True if `a` and `b` are on opposite sides of a manually-injected
    /// partition. Nodes not mentioned in either side are unaffected.
    pub(crate) fn is_partitioned(&self, a: NodeId, b: NodeId) -> bool {
        match &self.partition {
            None => false,
            Some((g1, g2)) => {
                (g1.contains(&a) && g2.contains(&b)) || (g2.contains(&a) && g1.contains(&b))
            }
        }
    }

    pub(crate) fn set_slow(&mut self, node: NodeId, multiplier: u64) {
        if multiplier <= 1 {
            self.slow.remove(&node);
        } else {
            self.slow.insert(node, multiplier);
        }
    }

    pub(crate) fn clear_slow(&mut self, node: NodeId) {
        self.slow.remove(&node);
    }

    /// The combined slow-node multiplier for a message travelling from
    /// `src` to `dst`: the larger of the two endpoints' multipliers.
    pub(crate) fn slow_multiplier(&self, src: NodeId, dst: NodeId) -> u64 {
        let s = self.slow.get(&src).copied().unwrap_or(1);
        let d = self.slow.get(&dst).copied().unwrap_or(1);
        s.max(d)
    }
}

/// A scripted fault, usable either imperatively (`Kernel::crash` etc.) or
/// scheduled in advance via `Kernel::schedule_fault` so an entire scenario
/// — including its faults — can be expressed as data driven purely by the
/// deterministic event queue.
#[derive(Debug, Clone)]
pub enum FaultCommand {
    /// Node goes silent: no more timers fire for it, and messages to/from
    /// it are dropped.
    Crash(NodeId),
    /// Node comes back. Volatile state is cleared (`Node::on_restart` is
    /// called); a durable-state hook is left for a future phase.
    Restart(NodeId),
    /// Split the cluster into two groups that cannot communicate with each
    /// other (messages within a group are unaffected).
    Partition(BTreeSet<NodeId>, BTreeSet<NodeId>),
    /// Remove any active manual partition.
    Heal,
    /// Multiply message delay to/from `node` by `multiplier` (>= 1).
    SlowNode(NodeId, u64),
    /// Remove a previously-set slow-node multiplier.
    ClearSlow(NodeId),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crash_and_restart_toggle_is_crashed() {
        let mut faults = FaultState::default();
        let n = NodeId(1);
        assert!(!faults.is_crashed(n));
        faults.crash(n);
        assert!(faults.is_crashed(n));
        faults.restart(n);
        assert!(!faults.is_crashed(n));
    }

    #[test]
    fn crash_is_idempotent() {
        let mut faults = FaultState::default();
        let n = NodeId(1);
        faults.crash(n);
        faults.crash(n);
        assert!(faults.is_crashed(n));
    }

    #[test]
    fn partition_blocks_cross_group_traffic_both_directions() {
        let mut faults = FaultState::default();
        let (a, b, c) = (NodeId(1), NodeId(2), NodeId(3));
        faults.set_partition(BTreeSet::from([a]), BTreeSet::from([b, c]));

        assert!(faults.is_partitioned(a, b));
        assert!(faults.is_partitioned(b, a));
        assert!(faults.is_partitioned(a, c));
        assert!(
            !faults.is_partitioned(b, c),
            "same-side traffic must be unaffected"
        );
        assert!(!faults.is_partitioned(a, a));
    }

    #[test]
    fn heal_removes_partition() {
        let mut faults = FaultState::default();
        let (a, b) = (NodeId(1), NodeId(2));
        faults.set_partition(BTreeSet::from([a]), BTreeSet::from([b]));
        assert!(faults.is_partitioned(a, b));
        faults.heal();
        assert!(!faults.is_partitioned(a, b));
    }

    #[test]
    fn nodes_not_mentioned_in_partition_are_unaffected() {
        let mut faults = FaultState::default();
        let (a, b, c) = (NodeId(1), NodeId(2), NodeId(3));
        faults.set_partition(BTreeSet::from([a]), BTreeSet::from([b]));
        assert!(!faults.is_partitioned(a, c));
        assert!(!faults.is_partitioned(c, b));
    }

    #[test]
    fn slow_multiplier_defaults_to_one() {
        let faults = FaultState::default();
        assert_eq!(faults.slow_multiplier(NodeId(1), NodeId(2)), 1);
    }

    #[test]
    fn slow_multiplier_is_max_of_both_endpoints() {
        let mut faults = FaultState::default();
        let (a, b) = (NodeId(1), NodeId(2));
        faults.set_slow(a, 3);
        faults.set_slow(b, 7);
        assert_eq!(faults.slow_multiplier(a, b), 7);
        assert_eq!(faults.slow_multiplier(b, a), 7);
    }

    #[test]
    fn setting_multiplier_to_one_clears_it() {
        let mut faults = FaultState::default();
        let a = NodeId(1);
        faults.set_slow(a, 5);
        assert_eq!(faults.slow_multiplier(a, NodeId(2)), 5);
        faults.set_slow(a, 1);
        assert_eq!(faults.slow_multiplier(a, NodeId(2)), 1);
    }

    #[test]
    fn clear_slow_resets_to_default() {
        let mut faults = FaultState::default();
        let a = NodeId(1);
        faults.set_slow(a, 5);
        faults.clear_slow(a);
        assert_eq!(faults.slow_multiplier(a, NodeId(2)), 1);
    }
}
