//! Issue #39's durability coverage gaps: the properties `crate::persist`
//! and `crate::driver` argue for but nothing exercised.
//!
//! The argument was sound -- the #38 review traced the write-before-reply
//! gate exhaustively and found no ack-before-commit hole. But "argued from
//! POSIX semantics and code reading" is a different claim from "a real
//! interrupted write was produced and recovered from", and issue #36 is
//! this project's standing evidence that the difference matters: a
//! durability bug that every in-process test missed because none of them
//! touched a real disk.
//!
//! Four gaps, one test each:
//!
//! 1. **Crash in the fsync/rename window.** A torn snapshot left by a
//!    crashed process must not stop the node booting or change what it
//!    reloads. (`persist.rs`'s own unit tests cover the `Store` level; this
//!    covers the real boot path.) Its majority-crash design needs each
//!    crashed replica to hold the write on disk first; that precondition is
//!    established in the test, not assumed -- see issue #111.
//! 2. **Disk-full / EIO.** That a failed durability write fail-stops the
//!    node rather than letting it serve state it never persisted was
//!    verified only by reading a `?` in `driver.rs`.
//! 3. **Unacknowledged in-flight write.** Only the inverse was tested
//!    (acknowledged writes survive). A write the client never heard back
//!    about may be lost -- but it must not leave replicas disagreeing.
//! 4. **Rapid restart under load.** Nothing drove a steady write stream
//!    across successive restarts.
//!
//! Tests 1, 3 and 4 use real `queso-node` OS processes ([`ProcCluster`]),
//! because a thread cannot be `SIGKILL`ed mid-fsync and an in-process
//! restart leaves the disk path untested -- the exact reason #36 survived
//! as long as it did. Test 2 runs in-process, because it needs to *observe
//! the node's exit value*, which a spawned binary only reports as a status
//! code.
//!
//! # What this still does not test
//!
//! Two things, both genuinely out of reach from userspace, and both left
//! exactly as `crate::persist`'s docs already state them:
//!
//! - **Power loss after a successful `rename`.** The directory fsync exists
//!   to stop a filesystem rolling the directory entry back afterwards. A
//!   test can crash *before* that fsync (and one below does), but it cannot
//!   make the kernel forget an entry it has already published to this
//!   process. That guarantee is still argued from POSIX semantics.
//! - **A lying disk.** Storage that acknowledges an fsync it has not made
//!   durable is a real class of bug and defeats everything here by
//!   construction.
//!
//! Saying so matters more than usual in this file: its whole premise is
//! that "argued" and "exercised" are different claims, which would be a
//! poor argument to make while quietly blurring the line somewhere else.
//!
//! # Detection power (measured)
//!
//! Anti-vacuity (below) shows each fault fired; it does not show the tests
//! can see the bug. Two mutations of `driver.rs` were run against all four,
//! on this sandbox, at the commit that added this section:
//!
//! - **Mutation A -- boot-time reload disabled.** `let loaded =
//!   store.load()?;` becomes `let loaded: Option<(queso_smr::Durable, u64)>
//!   = { store.load()?; None };` -- the call stays, so an I/O error still
//!   propagates, and only the reloaded state is thrown away. This is the
//!   #36 shape: a node that boots from an empty heap while its snapshot
//!   sits on disk.
//! - **Mutation B -- persist error swallowed.** `store.persist(&snapshot,
//!   tick).await?;` becomes `let _ = store.persist(&snapshot, tick).await;`
//!   -- the node serves on out of state it never persisted.
//!
//! | Test | Mutation A | Mutation B | The assertion that fired |
//! |---|---|---|---|
//! | 1 torn snapshot | **8/8** | 0/8 | `replica {i} lost an acknowledged write after finding a torn temp file` |
//! | 2 disk-full fail-stop | 0/8 (control) | **8/8** | `the node must exit promptly on a failed durability write, not hang` |
//! | 3 unacknowledged in-flight | **7/16** | 0/8 | `an acknowledged write must survive the crash regardless` |
//! | 4 rolling restart under load | **8/8** | 0/8 | `replica {i} lost acknowledged write to key {key} across rolling restarts` |
//!
//! Runs of this whole file with `--test-threads=1`, one binary rebuild per
//! mutation; mutation A was run twice (8 + 8), which is why test 3 has a
//! denominator of 16. Test 2 under mutation A is the control: it shows the
//! other rows are not "any mutation anywhere reddens the file".
//!
//! **Read test 3's row carefully -- and note it has since been corrected**
//! (issue #115). The 16-run measurement above saw every kill land on the
//! separate check that an *earlier, definitely acknowledged* write survived,
//! and concluded from that that the never-split assertion -- the one test 3
//! exists for -- has **no** falsifier. That conclusion was wrong, and wrong
//! in a way worth keeping on the page rather than quietly editing out.
//!
//! Mutation A *is* a falsifier for the never-split assertion. Re-run in
//! three batches -- 2/16, 5/100, 2/40 -- it kills there **9 times in 156,
//! 5.8%, Wilson 95% CI [3.1%, 10.6%]**,
//! reporting e.g. `replicas disagree about key 10: [Some(200), None,
//! Some(200)]`. At that rate a 16-run check comes back empty **39%** of the
//! time, so the original run was not unlucky -- it was a coin flip reported
//! as a fact. The overall kill rate reproduces (41/100 here against 7/16
//! there); only the "no falsifier" reading of it was mistaken.
//!
//! **Why the rate is low.** The old guess -- surviving replica 0 keeps the
//! value in memory -- is not what the evidence shows. Probing
//! `SmrNode::from_durable` on the *unmutated* build (10 runs, 2 restarts
//! each, 20/20 observations) shows each restarted replica reloads
//! `next_slot=0, applied_log=0`, an empty `kv`, and **1-2 slots of recorder
//! state**: nothing applied at all. So the state mutation A throws away is
//! very nearly nothing, which is enough to explain why throwing it away
//! usually changes no answer.
//!
//! That is as far as the measurement goes, and it is worth not going
//! further, because mutation A does two things, not one. `loaded` becoming
//! `None` also makes `is_restart` false (`driver.rs`), which skips the
//! restart catch-up pass entirely. Since the discarded state is nearly
//! empty, the skipped catch-up is the more plausible source of the kills --
//! but the two were not separated here, so which dominates is
//! **unmeasured**, and "mutation A measures durable-state loss" would be
//! the wrong gloss on these counts.
//!
//! One experiment bears on it and is recorded because it constrains the
//! answer rather than settling it: inserting a 0 / 500 / 2000ms wait
//! between the restarts and the reads (n=40 per arm) moved the kill rate
//! 45% / 38% / 45% -- no trend. That is consistent with catch-up being
//! *absent* rather than merely slow, but at n=40 per arm only a difference
//! of roughly 20 percentage points would have shown, so a smaller real
//! effect is not excluded.
//!
//! **The burst-acknowledged assertion is not merely unmeasured -- it is
//! dead here.** Probing the pre-crash bookkeeping over 60 runs found
//! `settled == 0` every time: no write in the burst is ever acknowledged
//! before the crash, so `acknowledged` is always empty and the
//! `if acknowledged.contains(&key)` branch never executes. That is
//! measured-zero power for a mechanical reason, not an unlucky sample. It
//! follows from the test's own design (a 5ms window against a 10-15ms
//! write); that it gets *more* certain on a slower machine is this test's
//! own reasoning, quoted in the burst comment below, not something measured
//! here. Left as-is rather than retuned: retuning the window would change
//! the scenario the 9/156 above was measured on.
//!
//! Test 1's eight kills all landed on the assertion above, never on the
//! `ENOENT` at the snapshot read that issue #111 describes -- worth saying,
//! because that flake would otherwise inflate a mutation count here. That
//! flake is since fixed: test 1 now establishes the precondition it needs
//! instead of assuming it, and 20 runs under the load that reproduced the
//! `ENOENT` (1 in 20 before the fix) came back clean. Mutation A was re-run
//! against the fixed form, 8/8 on the same assertion, so test 1's count
//! above is measured on the test as it stands rather than inherited from a
//! form that has since changed.
//!
//! Counts are from this sandbox; nothing in CI re-runs them, so they rot
//! silently. Re-running them is the way to check this section.
//!
//! # Anti-vacuity
//!
//! Every test asserts that its fault actually happened --
//! `DiskFault::has_fired`, the planted torn file's existence at reboot
//! time, at least one write genuinely still in flight at the crash. Two of
//! those checks were added after they caught this file's own tests passing
//! for the wrong reason: the torn-file check because a restarted replica's
//! first write clears the planted file (so a check afterwards fails for an
//! unrelated reason), and the in-flight check because a single write with a
//! 15ms window turned out to be acknowledged every time, silently testing
//! the acknowledged case the suite already covered.

mod support;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;

use queso_net::client;
use queso_net::config::NodeConfig;
use queso_net::persist::{DiskFault, DiskFaultPoint};
use queso_net::run_node_with_listeners;
use queso_sim::ids::NodeId;
use queso_smr::{ClientId, Command as KvCommand, Outcome};
use tokio::net::TcpListener as TokioTcpListener;

use support::ProcCluster;

fn put(client: u32, seq: u64, key: u32, value: i64) -> KvCommand {
    KvCommand::Put {
        client: ClientId(client),
        seq,
        key,
        value,
    }
}

fn get(client: u32, seq: u64, key: u32) -> KvCommand {
    KvCommand::Get {
        client: ClientId(client),
        seq,
        key,
    }
}

/// Retry until success or `timeout` -- a rebooting replica refuses
/// connections for a moment, which is not a failure.
async fn submit_with_retry(addr: SocketAddr, command: &KvCommand, timeout: Duration) -> Outcome {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match client::submit(addr, command).await {
            Ok(outcome) => return outcome,
            Err(err) => {
                if tokio::time::Instant::now() >= deadline {
                    panic!("submit to {addr} never succeeded (last error: {err:?})");
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

fn read_value(outcome: &Outcome) -> Option<i64> {
    match outcome {
        Outcome::Get(v) => *v,
        other => panic!("expected a Get outcome, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// 1. Crash in the fsync/rename window
// ---------------------------------------------------------------------------

/// A torn snapshot file left behind by a crash mid-write must not affect
/// what a restarted node loads.
///
/// The crash is simulated by planting the file rather than by timing a
/// `SIGKILL` to land inside the write, because timing one reliably is a
/// race this test would lose most runs -- and the state left behind is what
/// the recovery argument is actually about. `persist.rs`'s
/// `a_torn_write_leaves_the_previous_snapshot_intact` produces a torn file
/// through the real write path; this one proves the real *boot* path is
/// indifferent to finding one.
///
/// The planted bytes are deliberately a valid header plus a truncated
/// payload: a loader that ever looked at the temp file would find something
/// that starts out parsing correctly, which is the trap worth testing.
///
/// A **majority** is crashed and restarted, not one replica. That matters
/// for what the test can claim: with only one replica down, the restarted
/// node can learn the value by catching up from the two that stayed alive,
/// so the assertion would pass even if the reload had silently produced
/// nothing -- verified by mutation, where a one-replica version of this
/// test passed with the boot-time reload disabled entirely. Crashing the
/// majority leaves reloaded-from-disk state as the only possible source.
///
/// That design needs a precondition, and the test now *establishes* it
/// rather than assuming it (issue #111): before the kills, the value is
/// read back from replicas 1 and 2, which forces each to have the write on
/// disk. See the comment at that read for why an ack alone does not imply
/// it, and how the read does -- an argument from two premises checked in
/// the code it names, not an enumeration, which is why the `fs::read`
/// below still fails loudly and by name if it ever stops holding.
///
/// Falsifier, run: mutation A (see the module docs) fails this 8/8, every
/// time at `replica {i} lost an acknowledged write after finding a torn
/// temp file`. That 8/8 was measured on *this* form, after the #111 fix; it
/// matches the pre-fix count recorded in the module docs, which is carried
/// over from that run rather than re-measured here. The added pre-crash
/// read did not intercept the mutation on any of the 8 -- expected, since
/// it runs before any restart and mutation A only discards boot-time
/// reload, but the 8/8 is the evidence, not the expectation.
#[tokio::test(flavor = "multi_thread")]
async fn a_torn_snapshot_left_by_a_crash_does_not_affect_recovery() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let mut cluster = ProcCluster::start(3, 0, data_dir.path());

    let acked = submit_with_retry(
        cluster.client_addr(0),
        &put(1, 1, 42, 7),
        Duration::from_secs(10),
    )
    .await;
    assert!(matches!(acked, Outcome::Put), "write should be acked");

    // Establish -- not assume -- the precondition the majority-crash design
    // rests on (issue #111): each replica about to be killed must already
    // hold the acked write *on disk*, or the reboot has nothing to reload
    // and this test measures catch-up instead of recovery.
    //
    // It cannot be assumed, because an ack needs only a majority: replica 0
    // plus *one* of {1, 2}. The other is not on the critical path and may
    // not have processed a single peer message yet. A replica persists only
    // when a batch mutated durable state, and only an incoming peer
    // `Message` can do that (`driver.rs`'s `may_mutate_durable`), so
    // `node-{i}.durable.bin` need not exist at all -- which is exactly how
    // this test used to fail, with an `ENOENT` at the `fs::read` below that
    // read as an I/O accident rather than as "the precondition was never
    // established".
    //
    // Reading the value back from each replica is what forces it, provided
    // two things that hold in the code today and are checked there:
    //
    //  - `SmrNode::submit` queues *every* command, `Get` included, with no
    //    local short-circuit, so the read goes through the log (P10) and
    //    replica `i` must apply it -- a durable mutation, driven by peer
    //    messages.
    //  - `driver.rs` persists a batch's durable state before releasing
    //    anything it produced, the client `Outcome` included; that ordering
    //    is held by a release-build `assert!`, not just by comment.
    //
    // Given both, when this read returns, replica `i` has persisted a
    // snapshot covering a slot after the put's -- so the put is in it. That
    // is an argument, not an enumeration: if either premise goes, the
    // `fs::read` below is the thing that says so.
    for i in [1usize, 2] {
        let outcome = submit_with_retry(
            cluster.client_addr(i),
            &get(3, i as u64, 42),
            Duration::from_secs(20),
        )
        .await;
        assert_eq!(
            read_value(&outcome),
            Some(7),
            "replica {i} did not hold the acked write before being crashed, so the reboot \
             below would have had nothing to reload"
        );
    }

    cluster.kill(1);
    cluster.kill(2);

    // Plant exactly what a crash partway through each replica's next write
    // would have left: a truncated temp file alongside its intact snapshot.
    for i in [1usize, 2] {
        let path = cluster.snapshot_path(i);
        let snapshot = std::fs::read(&path).unwrap_or_else(|e| {
            panic!(
                "replica {i} has no snapshot at {} ({e}). The pre-crash read above is \
                 supposed to guarantee one; if this fires, that precondition no longer \
                 holds and the test cannot plant a torn file beside real state.",
                path.display()
            )
        });
        assert!(
            snapshot.len() > 16,
            "the snapshot should be substantial enough to truncate meaningfully"
        );
        let torn = &snapshot[..snapshot.len() / 2];
        std::fs::write(cluster.snapshot_tmp_path(i), torn).expect("plant a torn temp file");
    }

    // Anti-vacuity, checked *here* rather than at the end: the torn files
    // really exist for the reboot to encounter. They do not survive it --
    // the restarted replica's first successful write renames its own temp
    // file over the snapshot, clearing the planted one -- so a check after
    // the fact would fail for the wrong reason. (It did, which is how this
    // ordering got noticed.)
    for i in [1usize, 2] {
        assert!(
            cluster.snapshot_tmp_path(i).exists(),
            "replica {i}'s torn temp file was not planted"
        );
    }

    cluster.spawn(1);
    cluster.spawn(2);

    // The reboot must not be affected by the torn files: the acknowledged
    // write is still there, read back from each restarted replica.
    for i in [1usize, 2] {
        let outcome = submit_with_retry(
            cluster.client_addr(i),
            &get(2, i as u64, 42),
            Duration::from_secs(20),
        )
        .await;
        assert_eq!(
            read_value(&outcome),
            Some(7),
            "replica {i} lost an acknowledged write after finding a torn temp file"
        );
    }

    // And the nodes booted rather than dying on the file.
    for i in [1usize, 2] {
        assert!(
            !cluster.exited(i),
            "replica {i} exited instead of ignoring the torn temp file"
        );
    }
}

// ---------------------------------------------------------------------------
// 2. Disk-full / EIO
// ---------------------------------------------------------------------------

/// A failed durability write must **stop the node**, not be swallowed.
///
/// This is the gap the issue describes as "verified only by reading the `?`
/// in `driver.rs`". The failure mode it rules out is the dangerous one: a
/// node whose disk is full continuing to serve, and to acknowledge writes,
/// out of state that exists only in its heap -- which is issue #36 with a
/// different trigger.
///
/// Run in-process rather than as a spawned binary because the claim is
/// about `run_node`'s **return value**. A spawned process only reports a
/// status code, which cannot distinguish fail-stop from a panic or a
/// signal; here the actual `anyhow::Error` is inspected, and its
/// `io::ErrorKind` asserted, so the test cannot pass for the wrong reason.
///
/// A single replica is its own majority, so it decides alone and reaches
/// the durability write with no peers involved.
///
/// Falsifier, run: mutation B (see the module docs) fails this 8/8, every
/// time at `the node must exit promptly on a failed durability write, not
/// hang`. Mutation A leaves it passing 8/8, which is the right shape --
/// this test is about the write path failing loudly, not about what boot
/// reloads -- and makes it the control that shows the other three rows are
/// not just "any mutation anywhere reddens the file".
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_durability_write_stops_the_node() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    // Bound as std listeners here and converted inside the node's own
    // runtime below: a `tokio::net::TcpListener` is registered with the
    // reactor of whichever runtime bound it, so handing one to a different
    // runtime's thread does not work. Binding up front (rather than probing
    // for a free port and rebinding) also closes the `free_addr` TOCTOU
    // outright, the same way `tests/support`'s in-process helpers do.
    let peer_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind peer listener");
    let client_listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind client listener");
    let peer_addr = peer_listener.local_addr().expect("peer addr");
    let client_addr = client_listener.local_addr().expect("client addr");

    // Fires on the very first write, reported as a full disk.
    let fault =
        DiskFault::at(DiskFaultPoint::BeforeRename).with_kind(std::io::ErrorKind::StorageFull);

    let config = NodeConfig {
        id: NodeId(0),
        listen_addr: peer_addr,
        client_listen_addr: client_addr,
        peers: BTreeMap::from([(NodeId(0), peer_addr.to_string())]),
        total_replicas: 1,
        leader: Some(NodeId(0)),
        tick: Duration::from_millis(5),
        seed: 7,
        data_dir: data_dir.path().to_path_buf(),
        nemesis: None,
        persist_delay: Duration::ZERO,
        save_counter: None,
        durable_event_counter: None,
        disk_fault: Some(fault.clone()),
        tls: None,
        status_listen_addr: None,
        chain_checkpoints: None,
    };

    // `run_node_with_listeners`' future is `!Send` (the `SmrNode` is
    // `Rc<RefCell<_>>`-based by design -- see `queso_smr`), so it gets its
    // own thread and current-thread runtime rather than `tokio::spawn`,
    // exactly as `tests/cluster.rs` does.
    let (tx, rx) = std::sync::mpsc::channel();
    let node = std::thread::Builder::new()
        .name("fail-stop-node".to_string())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build a node runtime");
            let result = rt.block_on(async move {
                peer_listener.set_nonblocking(true)?;
                client_listener.set_nonblocking(true)?;
                let peer_listener = TokioTcpListener::from_std(peer_listener)?;
                let client_listener = TokioTcpListener::from_std(client_listener)?;
                run_node_with_listeners(config, peer_listener, client_listener).await
            });
            let _ = tx.send(());
            result
        })
        .expect("spawn the node thread");

    // Drive traffic until the node is gone. The submission itself is
    // expected to fail (the node dies before answering), which is the
    // point -- an unanswered client is the correct outcome of a disk that
    // cannot accept the write.
    let _ = tokio::time::timeout(
        Duration::from_secs(5),
        client::submit(client_addr, &put(1, 1, 1, 1)),
    )
    .await;

    rx.recv_timeout(Duration::from_secs(10))
        .expect("the node must exit promptly on a failed durability write, not hang");
    let outcome = node
        .join()
        .expect("the node thread should return, not panic");

    let err = match outcome {
        Ok(()) => panic!(
            "the node exited cleanly after a failed durability write -- it must fail-stop, \
             not continue serving state it never persisted"
        ),
        Err(err) => err,
    };
    assert!(
        fault.has_fired(),
        "the injected disk fault never fired, so this proved nothing"
    );

    let io_err = err
        .downcast_ref::<std::io::Error>()
        .unwrap_or_else(|| panic!("the disk error should reach the caller intact, got: {err:#}"));
    assert_eq!(
        io_err.kind(),
        std::io::ErrorKind::StorageFull,
        "the original failure kind must survive to the top level, so an operator \
         can tell a full disk from any other fault"
    );
}

// ---------------------------------------------------------------------------
// 3. Unacknowledged in-flight write
// ---------------------------------------------------------------------------

/// A write whose acknowledgement the client never received may be lost --
/// but whatever happened to it, every replica must agree, and the cluster
/// must keep working.
///
/// The existing suite only tests the inverse (`majority_reboot_does_not_
/// lose_an_acknowledged_write`). This is the "safe to lose" side, and the
/// assertion is deliberately not "the write is gone": if it reached a
/// majority before the crash it is committed and *must* survive, and if it
/// did not it is free to vanish. Asserting either specific outcome would be
/// asserting a race. What is never allowed is the third possibility --
/// replicas disagreeing about which it was, or the cluster wedging.
///
/// Falsifier, run: mutation A fails this on **41 of 100** runs, and read
/// which assertion fires, because the three do not share a fate (#115):
///
/// | assertion | kills / 100 |
/// |---|---|
/// | the separate "an acknowledged write must survive" check below | 36 |
/// | **never split** -- this test's subject | **5** |
/// | the burst-acknowledged check | 0, and structurally so |
///
/// **Never-split has a falsifier.** Pooled over 156 runs it is 9, i.e.
/// 5.8%, Wilson 95% CI [3.1%, 10.6%], reporting e.g. `replicas disagree
/// about key 10: [Some(200), None, Some(200)]` and once with the
/// disagreeing replica being the *surviving* one. An earlier 16-run
/// measurement saw none of these and recorded "no falsifier"; at 5.8% a
/// 16-run check comes back empty 39% of the time, so that was a coin flip
/// written down as a fact. The overall rate reproduces (41/100 vs 7/16);
/// only the reading of it was wrong.
///
/// **Why 5.8% and not 100%.** Probing `SmrNode::from_durable` on the
/// unmutated build -- 10 runs, 2 restarts each, 20/20 observations -- each
/// restarted replica reloads `next_slot=0, applied_log=0`, an empty `kv`,
/// and 1-2 slots of recorder state. Nothing applied. So what mutation A
/// throws away is nearly nothing, which is enough to explain why throwing
/// it away usually changes no answer. (The older guess -- that surviving
/// replica 0 masks it by holding the value in memory -- is not what the
/// probe shows.)
///
/// Note what this does *not* establish. Mutation A also makes `is_restart`
/// false, skipping the restart catch-up pass; given how little state is
/// discarded, that is the more plausible source of the kills. The two were
/// not separated, so which dominates is **unmeasured**. A 0/500/2000ms wait
/// inserted before the reads (n=40 per arm) moved the rate 45%/38%/45%, no
/// trend -- consistent with catch-up being absent rather than slow, but
/// only a ~20-point difference would have shown at that n.
///
/// **The burst-acknowledged check never runs.** Over 60 probed runs
/// `settled == 0` every time, so `acknowledged` is always empty and the
/// `if acknowledged.contains(&key)` branch is dead. Measured-zero power for
/// a mechanical reason, and it follows from this test's own 5ms window
/// against a 10-15ms write, so a slower machine makes it deader. Left
/// as-is: retuning the window would change the scenario the numbers above
/// were measured on.
///
/// One mutation that does **not** work, recorded so it is not retried:
/// clearing `kv` in `from_durable` (restore the frontier, forget the state
/// machine) survives 20/20 -- because the reloaded `kv` is already empty,
/// per the same probe.
#[tokio::test(flavor = "multi_thread")]
async fn an_unacknowledged_write_is_lost_or_kept_but_never_split() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let mut cluster = ProcCluster::start(3, 0, data_dir.path());

    // Establish the cluster with an acknowledged write first, so a later
    // disagreement cannot be blamed on a cluster that never formed.
    let acked = submit_with_retry(
        cluster.client_addr(0),
        &put(1, 1, 1, 100),
        Duration::from_secs(10),
    )
    .await;
    assert!(matches!(acked, Outcome::Put));

    // Now fire a burst of writes and crash the majority while they are
    // still in flight.
    //
    // A burst rather than one write, and a short window rather than a
    // carefully-timed one, because a single write's fate is a race this
    // test would keep losing: measured on this cluster, a write completes
    // in 10-15ms, so an earlier one-write version with a 15ms window was
    // silently testing the *acknowledged* case instead. With a burst and a
    // 5ms window the assertion below also fails in the safe direction --
    // a slower machine leaves *more* writes outstanding, not fewer.
    const BURST: u32 = 8;
    let mut in_flight = Vec::new();
    for k in 0..BURST {
        let addr = cluster.client_addr(0);
        in_flight.push(tokio::spawn(async move {
            client::submit(addr, &put(1, 10 + k as u64, 10 + k, 200 + k as i64)).await
        }));
    }
    tokio::time::sleep(Duration::from_millis(5)).await;

    // Snapshot which writes the client had heard back about *before* the
    // crash -- those, and only those, the cluster promised to keep.
    let settled: Vec<bool> = in_flight.iter().map(|h| h.is_finished()).collect();
    cluster.kill(1);
    cluster.kill(2);

    let mut acknowledged: Vec<u32> = Vec::new();
    for (k, handle) in in_flight.into_iter().enumerate() {
        if settled[k] {
            if let Ok(Ok(Outcome::Put)) = handle.await {
                acknowledged.push(10 + k as u32);
            }
        } else {
            handle.abort();
        }
    }

    // Anti-vacuity: this test is about writes whose fate the client never
    // learned, so at least one has to be in that state.
    assert!(
        settled.iter().any(|done| !done),
        "every write in the burst was acknowledged before the crash, so this run \
         tested the acknowledged case the suite already covers"
    );

    cluster.spawn(1);
    cluster.spawn(2);

    // Whatever became of each write, every replica must report the same
    // thing about it -- and anything acknowledged must be present.
    let mut seq = 1_000u64;
    for k in 0..BURST {
        let key = 10 + k;
        let mut answers = Vec::new();
        for i in 0..cluster.replicas() {
            seq += 1;
            let outcome = submit_with_retry(
                cluster.client_addr(i),
                &get(9, seq, key),
                Duration::from_secs(20),
            )
            .await;
            answers.push(read_value(&outcome));
        }
        assert!(
            answers.windows(2).all(|w| w[0] == w[1]),
            "replicas disagree about key {key}: {answers:?} -- an unacknowledged write \
             is free to be lost or kept, but not to be both"
        );
        if acknowledged.contains(&key) {
            assert_eq!(
                answers[0],
                Some(200 + k as i64),
                "key {key} was acknowledged before the crash and must have survived it"
            );
        }
    }

    // The acknowledged write from before is not free to vanish.
    let survived = submit_with_retry(
        cluster.client_addr(2),
        &get(9, 9_000, 1),
        Duration::from_secs(20),
    )
    .await;
    assert_eq!(
        read_value(&survived),
        Some(100),
        "an acknowledged write must survive the crash regardless"
    );

    // And the cluster still accepts new work -- a wedged cluster would
    // satisfy every assertion above.
    let after = submit_with_retry(
        cluster.client_addr(0),
        &put(1, 9_001, 3, 300),
        Duration::from_secs(20),
    )
    .await;
    assert!(
        matches!(after, Outcome::Put),
        "the cluster must still accept writes after the crash"
    );
}

// ---------------------------------------------------------------------------
// 4. Rapid restart under load
// ---------------------------------------------------------------------------

/// A steady write stream across successive restarts: every acknowledged
/// write must still be readable at the end.
///
/// The existing restart tests take one snapshot, reboot once, and read it
/// back. This drives writes continuously *through* several rolling
/// restarts, so a replica is rebooting while others are mid-decision --
/// which is where a reload/`on_restart` bug that a quiescent test cannot
/// reach would live (issue #22's catch-up "zombie replica" was exactly that
/// shape).
///
/// Restarts are rolling, one replica at a time: a majority stays up
/// throughout, so the cluster owes progress the whole way and a stall is a
/// real failure rather than the expected consequence of losing quorum --
/// the same discipline `queso-soak`'s fault schedule follows.
///
/// Falsifier, run: mutation A fails this 8/8, every time at `replica {i}
/// lost acknowledged write to key {key} across rolling restarts`.
#[tokio::test(flavor = "multi_thread")]
async fn acknowledged_writes_survive_rolling_restarts_under_load() {
    let data_dir = tempfile::tempdir().expect("tempdir");
    let mut cluster = ProcCluster::start(3, 0, data_dir.path());

    let mut acked: Vec<(u32, i64)> = Vec::new();
    let mut seq = 0u64;

    for round in 0..3u32 {
        // Write a few keys, recording only what was actually acknowledged
        // -- those are the ones the cluster promised to keep.
        for k in 0..4u32 {
            seq += 1;
            let key = round * 10 + k;
            let value = (round as i64 + 1) * 1_000 + k as i64;
            let outcome = submit_with_retry(
                cluster.client_addr((round as usize) % 3),
                &put(1, seq, key, value),
                Duration::from_secs(20),
            )
            .await;
            assert!(
                matches!(outcome, Outcome::Put),
                "write {key} should be acked"
            );
            acked.push((key, value));
        }

        // Roll one replica while the others keep serving.
        let victim = (round as usize + 1) % 3;
        cluster.kill(victim);
        // Keep offering load with the replica down, so the restart lands in
        // the middle of real traffic rather than into a quiet cluster.
        for k in 0..2u32 {
            seq += 1;
            let key = 500 + round * 10 + k;
            let value = 50_000 + seq as i64;
            let outcome = submit_with_retry(
                cluster.client_addr((victim + 1) % 3),
                &put(1, seq, key, value),
                Duration::from_secs(20),
            )
            .await;
            assert!(matches!(outcome, Outcome::Put));
            acked.push((key, value));
        }
        cluster.spawn(victim);
    }

    assert_eq!(
        acked.len(),
        18,
        "the test should have acknowledged every write it recorded"
    );

    // Every acknowledged write, read back from every replica -- including
    // the ones that rebooted mid-stream.
    for i in 0..cluster.replicas() {
        for (key, value) in &acked {
            seq += 1;
            let outcome = submit_with_retry(
                cluster.client_addr(i),
                &get(9, seq, *key),
                Duration::from_secs(30),
            )
            .await;
            assert_eq!(
                read_value(&outcome),
                Some(*value),
                "replica {i} lost acknowledged write to key {key} across rolling restarts"
            );
        }
    }
}
