//! Runs the Antithesis test template locally, against real `queso-node`
//! processes, and checks that the properties actually fire.
//!
//! # Why this exists
//!
//! Issue #54 scopes 9.3 as *"buildable artifacts here; the run needs the
//! owner's account."* That phrasing invites a failure mode this project has
//! spent three phases learning to distrust: shipping a pile of YAML and
//! shell scripts that nobody has ever executed, and calling it done because
//! the thing that would execute it is out of reach.
//!
//! So this runs everything that *can* be run without Antithesis. It boots a
//! real three-replica cluster, invokes each Test Composer command as its own
//! process exactly as the platform would, and then reads the assertion
//! stream the SDK emits — proving not just that the commands exit zero, but
//! that each property was **reached and evaluated**. An assertion that never
//! executes is invisible to Antithesis too, so "did it fire" is the same
//! question there as here.
//!
//! What remains genuinely unverifiable in this repo, and is stated in
//! `antithesis/README.md` rather than glossed: the container build, the
//! registry push, and the platform's own scheduling of these commands.
//!
//! # Addressing
//!
//! Replicas get distinct loopback addresses (127.0.0.1, .2, .3) on
//! *identical* ports, rather than distinct ports on one address. That is not
//! incidental — it is the same shape as the container topology
//! (`queso-0`/`queso-1`/`queso-2`, all on 7000/7100), so this exercises the
//! real addressing path instead of a local-only special case.

use std::process::{Child, Command, Stdio};

const CLIENT_PORT: u16 = 7000;
const STATUS_PORT: u16 = 7100;
const PEER_PORT: u16 = 7200;
const REPLICAS: usize = 3;

fn workload_bin() -> &'static str {
    env!("CARGO_BIN_EXE_queso-antithesis")
}

/// `CARGO_BIN_EXE_*` only covers binaries in *this* package, and
/// `queso-node` lives in `queso-net`, so it is resolved at run time --
/// the same approach, and the same reason, as `queso-soak`'s harness.
fn node_bin() -> std::path::PathBuf {
    if let Ok(explicit) = std::env::var("QUESO_NODE_BIN") {
        return std::path::PathBuf::from(explicit);
    }
    let exe = std::env::current_exe().expect("this test executable's own path");
    let dir = exe.parent().expect("test executable has a parent");
    let profile_dir = if dir.ends_with("deps") {
        dir.parent().expect("deps has a parent")
    } else {
        dir
    };
    let candidate = profile_dir.join("queso-node");
    assert!(
        candidate.exists(),
        "queso-node binary not found at {}. Build it first \
         (`cargo build --all`), or point QUESO_NODE_BIN at it.",
        candidate.display()
    );
    candidate
}

fn host(i: usize) -> String {
    format!("127.0.0.{}", i + 1)
}

/// Three real replicas, killed on drop so a failing assertion never leaks
/// node processes.
struct Cluster {
    children: Vec<Child>,
    _data: tempfile::TempDir,
}

impl Cluster {
    fn start() -> Self {
        let data = tempfile::tempdir().expect("tempdir");
        let mut children = Vec::new();
        for i in 0..REPLICAS {
            let mut cmd = Command::new(node_bin());
            cmd.arg("--id")
                .arg(i.to_string())
                .arg("--seed")
                .arg((900 + i as u64).to_string())
                .arg("--listen")
                .arg(format!("{}:{PEER_PORT}", host(i)))
                .arg("--client-listen")
                .arg(format!("{}:{CLIENT_PORT}", host(i)))
                .arg("--status-listen")
                .arg(format!("{}:{STATUS_PORT}", host(i)))
                .arg("--chain-checkpoints")
                .arg("2")
                .arg("--leader")
                .arg("0")
                .arg("--tick-ms")
                .arg("5")
                .arg("--data-dir")
                .arg(data.path());
            for j in 0..REPLICAS {
                cmd.arg("--peer")
                    .arg(format!("{j}={}:{PEER_PORT}", host(j)));
            }
            let err = std::fs::File::create(data.path().join(format!("node-{i}.err")))
                .expect("create node stderr log");
            cmd.stdout(Stdio::null()).stderr(Stdio::from(err));
            children.push(cmd.spawn().expect("spawn queso-node"));
        }
        Self {
            children,
            _data: data,
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for child in &mut self.children {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// One assertion the SDK actually evaluated.
#[derive(Debug)]
struct Fired {
    kind: String,
    condition: bool,
    message: String,
}

/// Run one Test Composer command and return `(success, assertions it
/// evaluated, whether it signalled setup_complete)`.
///
/// Each command gets its own output file because each is its own process
/// and the SDK's local handler truncates the file it is given -- a shared
/// path silently leaves only the last run's assertions, which cost a
/// debugging detour the first time this was wired up.
fn run_command(out_dir: &std::path::Path, name: &str, args: &[&str]) -> (bool, Vec<Fired>, bool) {
    let out = out_dir.join(format!("{name}.json"));
    let status = Command::new(workload_bin())
        .env("ANTITHESIS_SDK_LOCAL_OUTPUT", &out)
        .arg("--node")
        .arg(host(0))
        .arg("--node")
        .arg(host(1))
        .arg("--node")
        .arg(host(2))
        .args(args)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .expect("run the workload binary");

    let body = std::fs::read_to_string(&out).unwrap_or_default();
    let mut fired = Vec::new();
    let mut setup_complete = false;
    for line in body.lines() {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        if value.get("antithesis_setup").is_some() {
            setup_complete = true;
        }
        let Some(assertion) = value.get("antithesis_assert") else {
            continue;
        };
        // `hit` separates assertions that were *evaluated* from the catalog
        // entries the SDK registers at startup for every assertion in the
        // binary. Only the former say anything about this run.
        if assertion["hit"].as_bool() == Some(true) {
            fired.push(Fired {
                kind: assertion["assert_type"].as_str().unwrap_or("?").to_string(),
                condition: assertion["condition"].as_bool().unwrap_or(false),
                message: assertion["message"].as_str().unwrap_or("").to_string(),
            });
        }
    }
    (status.success(), fired, setup_complete)
}

fn find<'a>(fired: &'a [Fired], needle: &str) -> &'a Fired {
    fired
        .iter()
        .find(|f| f.message.contains(needle))
        .unwrap_or_else(|| {
            panic!("no assertion matching {needle:?} was evaluated; fired: {fired:#?}")
        })
}

/// The whole template, end to end, in the order Antithesis would run it.
#[test]
#[ignore = "boots real node processes and binds 127.0.0.{1,2,3}; run with --ignored"]
fn the_test_template_runs_and_its_properties_fire() {
    let _cluster = Cluster::start();
    let out_dir = tempfile::tempdir().expect("tempdir");

    // first_: wait for the cluster, signal setup_complete.
    let (ok, _fired, setup_complete) = run_command(
        out_dir.path(),
        "ready",
        &["wait-ready", "--timeout-secs", "90"],
    );
    assert!(
        ok,
        "first_wait_for_cluster must succeed against a healthy cluster"
    );
    assert!(
        setup_complete,
        "the first_ command must signal setup_complete -- without it Antithesis \
         starts injecting faults into a cluster that has not finished booting, and \
         every liveness result for the run is noise"
    );

    // parallel_driver_: safety under load.
    let (ok, fired, _) = run_command(
        out_dir.path(),
        "traffic",
        &["traffic", "--duration-secs", "8", "--seed", "3"],
    );
    assert!(
        ok,
        "the traffic driver must succeed against a healthy cluster"
    );

    let divergence = find(&fired, "never report different blocks");
    assert_eq!(divergence.kind, "always");
    assert!(divergence.condition, "a healthy cluster must not diverge");

    let acked = find(&fired, "acknowledges Chain-of-Blocks writes");
    assert_eq!(acked.kind, "sometimes");
    assert!(
        acked.condition,
        "a healthy cluster must acknowledge writes -- otherwise the safety verdict \
         is a statement about an empty log"
    );

    let comparisons = find(&fired, "observed at the same height");
    assert!(
        comparisons.condition,
        "replicas must be compared at a shared height, or 'no divergence' means nothing"
    );

    // The one property that is *expected* to be false here, and is the
    // clearest illustration of what a `sometimes` assertion is for: nothing
    // is injecting faults locally, so no submission fails. Under Antithesis
    // it becomes true the first time the platform partitions something, and
    // a run where it never did would mean the workload was never actually
    // tested under fault. Asserting the false-ness pins that reasoning down
    // rather than leaving a reader to wonder whether it is broken.
    let under_fault = find(&fired, "really runs under fault");
    assert_eq!(under_fault.kind, "sometimes");
    assert!(
        !under_fault.condition,
        "with no faults injected locally this must be unsatisfied; if it ever \
         passes here, something is failing submissions that should not be"
    );

    // eventually_: liveness in the quiescent branch.
    let (ok, fired, _) = run_command(out_dir.path(), "check", &["check", "--timeout-secs", "60"]);
    assert!(
        ok,
        "the quiescent check must succeed against a healthy cluster"
    );
    for needle in [
        "never report different blocks",
        "the cluster keeps deciding",
        "no replica is left behind",
        "every replica is observed",
    ] {
        let assertion = find(&fired, needle);
        assert_eq!(assertion.kind, "always", "{needle}");
        assert!(
            assertion.condition,
            "a healthy quiescent cluster must satisfy {needle:?}: {assertion:?}"
        );
    }
}

/// The liveness check must be able to *fail*.
///
/// `the_test_template_runs_and_its_properties_fire` only shows the
/// properties passing, and a check that cannot fail passes just as happily
/// on a broken cluster. Pointing the workload at replicas that are not there
/// is the cheapest genuine falsifier: nothing can be observed, so the
/// progress and observation properties must both be violated.
#[test]
#[ignore = "binds 127.0.0.{1,2,3}; run with --ignored"]
fn the_liveness_properties_fail_when_there_is_no_cluster() {
    let out_dir = tempfile::tempdir().expect("tempdir");
    // No cluster started -- deliberately.
    let (ok, fired, _) = run_command(
        out_dir.path(),
        "check-dead",
        &["check", "--timeout-secs", "6", "--step-ms", "200"],
    );
    assert!(!ok, "the check must fail when the cluster is not there");

    let progress = find(&fired, "the cluster keeps deciding");
    assert!(
        !progress.condition,
        "a cluster that does not exist cannot be making progress"
    );
    let observed = find(&fired, "every replica is observed");
    assert!(
        !observed.condition,
        "no replica can be observed when none is running"
    );
}
