//! Issue #83: does a **populated** `Durable` survive the on-disk round trip
//! intact?
//!
//! # Why this gap existed
//!
//! Two layers each test restart, and neither tests this:
//!
//! - `crates/net/src/persist.rs`'s own tests round-trip **`Durable::default()`**
//!   — an empty blob — because `Durable`'s fields are `pub(crate)` to
//!   `queso-smr` and nothing outside that crate could build a populated one.
//!   They prove the header, the atomic-rename scheme and the version check.
//!   They prove nothing about whether recorder state survives.
//! - `queso-smr`'s sim restart tests never serialize anything at all.
//!   `Kernel::restart` calls `on_restart` on the *same heap-resident node*,
//!   so durable state is preserved by construction — a faithful model of
//!   "this field is durable", and structurally incapable of catching a
//!   serialization gap.
//!
//! So the real restart path — `Store::load` → `SmrNode::from_durable` — is
//! the one place a replica's recorder state could be lost or mangled, and it
//! was the one place nothing checked. #83 is a replica that came back from a
//! restart and applied a command the majority did not, which is exactly what
//! losing recorder state across a restart would look like.
//!
//! `SmrCluster::durable_snapshot` exists so this test can build a `Durable`
//! with real content in it.
//!
//! # What "intact" means here
//!
//! `Durable` has no `PartialEq` and deliberately no `Debug`, so this compares
//! what a restarted replica actually reads back: its applied log, and its
//! recorder (ISR) summary at every slot. Observable equality is the property
//! that matters — a restarted replica must answer every question the same way
//! the crashed one would have.

use queso_net::persist::Store;
use queso_sim::ids::NodeId;
use queso_sim::scheduler::{ContentObliviousAdversary, SchedulerKind};
use queso_smr::{ClientId, SmrCluster, SmrNode};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// Drive an in-process cluster hard enough that every replica holds a
/// non-trivial applied log *and* recorder state for many slots.
fn populated_cluster(seed: u64, n: usize) -> SmrCluster {
    let adversary = ContentObliviousAdversary::new(1, 8).with_drop_probability(0.1);
    let mut cluster = SmrCluster::new_with_leader(
        seed,
        SchedulerKind::Oblivious(Box::new(adversary)),
        n,
        Some(NodeId(0)),
    );
    let mut rng = StdRng::seed_from_u64(seed ^ 0x_f1de_1117);
    for i in 0..40u64 {
        let replica = NodeId(rng.gen_range(0..n as u32));
        let client = ClientId(rng.gen_range(0..4));
        let key = rng.gen_range(0..3);
        if rng.gen_bool(0.5) {
            cluster.submit_put(replica, client, i, key, rng.gen_range(0..1000));
        } else {
            cluster.submit_get(replica, client, i, key);
        }
        cluster.run_for(rng.gen_range(1..40));
    }
    cluster.run_for(500_000);
    cluster
}

/// How many slots this cluster holds recorder state for, on `replica`.
fn recorded_slots(cluster: &SmrCluster, replica: NodeId, upto: u64) -> usize {
    (0..upto)
        .filter(|&slot| cluster.recorder_summary(replica, slot).is_some())
        .count()
}

/// The round trip, for one replica of a populated cluster.
fn assert_round_trip_is_faithful(seed: u64, n: usize, replica: NodeId) {
    let cluster = populated_cluster(seed, n);

    let applied_before = cluster.applied_log(replica);
    let frontier = cluster.next_slot(replica);
    // Scan past the frontier too: a replica holds recorder state for slots it
    // has not applied (a proposer touched them), and losing *those* is
    // precisely the hazard — they are what a future `record` reply depends on.
    let scan_upto = frontier + 16;
    let recorded_before = recorded_slots(&cluster, replica, scan_upto);

    // Anti-vacuity. An empty `Durable` round-trips trivially, which is
    // exactly why the existing persistence tests could not have caught a
    // fidelity bug.
    assert!(
        applied_before.len() >= 8,
        "seed {seed}: replica {replica} applied only {} slots -- too little to be \
         a meaningful round trip",
        applied_before.len()
    );
    assert!(
        recorded_before >= 4,
        "seed {seed}: replica {replica} holds recorder state for only {recorded_before} \
         slot(s) -- the state this test exists to check is barely present"
    );

    let dir = tempfile::tempdir().expect("tempdir");
    let store = Store::new(dir.path(), replica).expect("store");
    store
        .save(&cluster.durable_snapshot(replica), 4242)
        .expect("save");
    let (loaded, max_tick) = store
        .load()
        .expect("load")
        .expect("a snapshot was just written");
    assert_eq!(max_tick, 4242, "the extra field must survive too");

    let restored = SmrNode::from_durable(n, Some(NodeId(0)), loaded);

    assert_eq!(
        restored.applied_from(0),
        applied_before,
        "seed {seed}: replica {replica}'s applied log did not survive the disk round trip"
    );
    assert_eq!(
        restored.next_slot(),
        frontier,
        "seed {seed}: replica {replica}'s frontier did not survive the disk round trip"
    );

    let mut compared = 0usize;
    for slot in 0..scan_upto {
        let before = cluster.recorder_summary(replica, slot);
        let after = restored.recorder_summary(slot);
        assert_eq!(
            before, after,
            "seed {seed}: replica {replica}'s recorder state at slot {slot} did not \
             survive the disk round trip -- this is the P12 state whose loss \
             `Durable`'s docs warn can cost Agreement"
        );
        if before.is_some() {
            compared += 1;
        }
    }
    assert_eq!(
        compared, recorded_before,
        "internal check: every recorder present before the round trip should have been compared"
    );
}

#[test]
fn a_populated_durable_survives_the_disk_round_trip_n3() {
    for seed in 0..12u64 {
        for replica in 0..3u32 {
            assert_round_trip_is_faithful(seed, 3, NodeId(replica));
        }
    }
}

#[test]
fn a_populated_durable_survives_the_disk_round_trip_n5() {
    for seed in 0..6u64 {
        for replica in 0..5u32 {
            assert_round_trip_is_faithful(seed, 5, NodeId(replica));
        }
    }
}
