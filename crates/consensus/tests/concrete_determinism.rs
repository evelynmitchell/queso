//! Reproducibility (D9) for Phase 2's concrete protocol: the same seed (and
//! the same sequence of driver calls) must produce a byte-for-byte
//! identical kernel trace and the same decisions, every time -- even though
//! this protocol now involves per-recorder random priority draws (plural,
//! one per recorder per phase-0 step -- see `queso_consensus::proposer`'s
//! module docs) and retry-timer-driven retransmission, both of which must
//! still resolve to the same deterministic sequence of kernel events given
//! the same seed.

use std::collections::BTreeMap;

use queso_consensus::ConcreteCluster;
use queso_sim::ids::NodeId;
use queso_sim::scheduler::{ContentObliviousAdversary, SchedulerKind};

const MAX_TICKS: u64 = 200_000;

fn run(seed: u64, n: u32) -> (Vec<u8>, BTreeMap<NodeId, u32>) {
    let initial_values: BTreeMap<NodeId, u32> = (0..n).map(|i| (NodeId(i), i)).collect();
    let scheduler = ContentObliviousAdversary::new(1, 5).with_drop_probability(0.3);
    let mut cluster = ConcreteCluster::new(
        seed,
        SchedulerKind::Oblivious(Box::new(scheduler)),
        initial_values,
    );
    cluster.crash(NodeId(n - 1));
    cluster.run_slot(MAX_TICKS);

    let decisions: BTreeMap<NodeId, u32> = cluster
        .replicas()
        .iter()
        .filter_map(|&id| cluster.decided(id).map(|v| (id, v)))
        .collect();
    (cluster.trace().to_canonical_bytes(), decisions)
}

#[test]
fn same_seed_produces_identical_trace_and_decisions() {
    for seed in [1, 2, 3, 42, 999, 123_456] {
        let (trace_a, decisions_a) = run(seed, 5);
        let (trace_b, decisions_b) = run(seed, 5);
        assert_eq!(trace_a, trace_b, "seed {seed}: traces diverged");
        assert_eq!(decisions_a, decisions_b, "seed {seed}: decisions diverged");
    }
}

#[test]
fn different_seeds_can_produce_different_traces() {
    let (trace_a, _) = run(1, 5);
    let (trace_b, _) = run(2, 5);
    assert_ne!(
        trace_a, trace_b,
        "different seeds produced identical traces -- suspicious"
    );
}
