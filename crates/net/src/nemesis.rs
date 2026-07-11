//! Phase 7.4: an in-transport, real-TCP fault injector ("nemesis") for
//! `queso-net`'s replica-to-replica connections -- the real-network
//! analogue of `queso_sim::fault`'s scripted crash/partition/slow-node
//! model (see that module's docs for the vocabulary this borrows).
//!
//! # Why in-transport, not an external proxy
//!
//! A tool like [toxiproxy](https://github.com/Shopify/toxiproxy) (an
//! external TCP proxy you point traffic through and script over its own
//! API) is a legitimate way to fuzz a real deployment, but it needs an
//! extra out-of-process component wired into the network topology --
//! awkward to stand up deterministically inside `cargo test`/CI, and one
//! more moving part to keep in sync with this crate's own transport code.
//! [`Nemesis`] instead lives *inside* the transport: it is consulted by
//! [`crate::transport::spawn_peer_dialer`] before each outbound peer frame
//! is written to the socket. That keeps the whole adversarial story
//! self-contained in this crate, runnable with nothing but `cargo test`,
//! and exactly as deterministic as its seed (see "Determinism" below) --
//! at the cost of only being able to fuzz traffic this crate's own
//! transport originates (an external proxy could also fuzz, say, a raw
//! `tcpdump`-visible packet, which this cannot). If a future phase wants
//! toxiproxy-style black-box fuzzing (e.g. against `queso-node` binaries
//! deployed on real hosts, Phase 7.3's territory), that is complementary
//! to, not a replacement for, this in-process nemesis -- it exercises a
//! different failure surface (the OS/network stack) than "what does the
//! consensus layer do when messages it sent don't arrive".
//!
//! # What it faults, and what it deliberately does not
//!
//! [`Nemesis::decide`] and [`Nemesis::delay`] are consulted per outbound
//! *frame* (one already-encoded [`crate::wire::WireMsg::App`] message), on
//! the sending side only, in [`crate::transport::spawn_peer_dialer`]:
//!
//! - **Latency/jitter**: [`Nemesis::delay`] returns a base delay plus
//!   uniform jitter to `tokio::time::sleep` before writing the frame --
//!   models WAN/queueing latency without touching the socket itself.
//! - **Frame drop**: [`LinkAction::Drop`] silently discards the frame
//!   instead of writing it -- the sender does not learn its write "failed"
//!   (there is nothing to learn; the frame is simply never sent), exactly
//!   `queso_sim::fault`'s `DropReason::Scheduler`/`Partitioned` model and
//!   exactly what this transport's own outbound-queue-capacity docs
//!   (`crate::transport::OUTBOUND_QUEUE_CAPACITY`) already say a dropped
//!   message must be safe for: `queso_consensus::proposer`'s unbounded
//!   retry-with-backoff re-sends whatever a live proposer still needs.
//! - **Connection reset**: [`LinkAction::ResetConnection`] closes the
//!   `Framed` connection (dropping the current frame too) and lets
//!   [`crate::transport::spawn_peer_dialer`]'s existing reconnect loop take
//!   over -- modelling a mid-stream TCP RST/timeout, not a permanent
//!   failure.
//! - **Partition / partial partition (majority-minority split)**:
//!   [`Nemesis::partition`] splits the cluster into two [`NodeId`] groups;
//!   every frame whose `(from, to)` pair crosses the split is dropped
//!   (checked first, ahead of the random drop/reset probabilities) until
//!   [`Nemesis::heal`]. [`Nemesis::isolate`] is sugar for the common
//!   one-against-the-rest case (see "Leader-targeting" below).
//!
//! This is a **message-level** partition/drop model, not a socket-level
//! one: a partitioned pair's underlying TCP connection is left alone (it
//! may still be up, or still reconnecting on its own schedule) -- only the
//! application frames stop crossing it. That is a deliberate scope choice:
//! it is what actually matters to the consensus layer (whether its
//! messages arrive), and it sidesteps having to simulate OS/firewall-level
//! unreachability. It does mean this nemesis cannot exercise "TCP connect
//! itself times out/refuses" as a distinct failure mode from "the
//! connection is up but nothing gets through" -- see this crate's README
//! for the fuller list of what Phase 7.4 does and does not cover.
//!
//! Client-facing connections ([`crate::client`]) are **not** touched by
//! this module at all -- partitioning/DoS-ing a replica's peer traffic
//! already gives a faithful "this replica cannot make progress"
//! ([`crate::client::Client`]'s own retry-to-another-replica is what a
//! real caller relies on to route around it, exactly as it would in
//! production).
//!
//! # Off by default
//!
//! [`crate::config::NodeConfig::nemesis`] is `Option<Arc<Nemesis>>`,
//! defaulting to `None` wherever a config is built without setting it
//! explicitly (`queso-node`'s CLI never does) -- every call site this
//! module hooks into treats `None` as a strict no-op (no lock taken, no
//! RNG draw, no delay), so an ordinary `queso-node` run is unaffected byte
//! for byte. Only test/bench harnesses that explicitly build a `Nemesis`
//! and thread it through `NodeConfig` opt in.
//!
//! # Determinism / reproducibility
//!
//! [`Nemesis::new`] takes a `u64` seed; every random decision (drop, reset,
//! jitter) is drawn from one `rand::rngs::StdRng` seeded from it, guarded
//! by a single [`std::sync::Mutex`] shared across every peer-dialer task
//! that consults this `Nemesis` -- so a given seed reproduces the same
//! *sequence* of fault decisions. It does not reproduce the same
//! *assignment* of decisions to specific messages: unlike
//! `queso_sim::kernel::Kernel`'s single-threaded deterministic event queue,
//! real peer-dialer tasks race each other over a real tokio scheduler, so
//! which dialer's frame consults the shared RNG first on a given run is
//! not fixed by the seed alone. Good enough for what this module is for
//! (a reproducible fault *rate*/*shape* for a bench run, not bit-for-bit
//! replay) -- exactly the same honest caveat `queso-bench --seed` already
//! carries for workload shape (see `src/bin/queso-bench.rs`'s docs).

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use queso_sim::ids::NodeId;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// A frame-level fault plan's static probabilities/delays, used to build a
/// [`Nemesis`]. All fields are "off" (zero) in [`FaultPlan::default`] --
/// build one field-by-field (or via the `with_*` builders) for the exact
/// mix of faults a scenario wants.
#[derive(Debug, Clone)]
pub struct FaultPlan {
    /// PRNG seed for this nemesis's drop/reset/jitter draws (see the module
    /// docs' "Determinism" section).
    pub seed: u64,
    /// Fixed delay added before every outbound peer frame, before jitter.
    pub latency: Duration,
    /// Additional uniform-random delay in `[0, jitter]` added on top of
    /// `latency`, independently per frame.
    pub jitter: Duration,
    /// Per-frame probability in `[0.0, 1.0]` of silently dropping an
    /// outbound frame (independent of partition state).
    pub drop_prob: f64,
    /// Per-frame probability in `[0.0, 1.0]` of forcing the connection to
    /// reset instead of sending (independent of `drop_prob`/partition
    /// state; checked first, since a reset already implies the frame in
    /// hand doesn't get sent).
    pub reset_prob: f64,
}

impl Default for FaultPlan {
    fn default() -> Self {
        Self {
            seed: 0,
            latency: Duration::ZERO,
            jitter: Duration::ZERO,
            drop_prob: 0.0,
            reset_prob: 0.0,
        }
    }
}

impl FaultPlan {
    /// A plan with everything off except the seed -- useful when a test
    /// only wants scripted [`Nemesis::partition`]/[`Nemesis::isolate`]
    /// calls and no ambient per-frame fuzzing.
    pub fn seeded(seed: u64) -> Self {
        Self {
            seed,
            ..Self::default()
        }
    }

    pub fn with_latency(mut self, latency: Duration, jitter: Duration) -> Self {
        self.latency = latency;
        self.jitter = jitter;
        self
    }

    pub fn with_drop_prob(mut self, drop_prob: f64) -> Self {
        self.drop_prob = drop_prob;
        self
    }

    pub fn with_reset_prob(mut self, reset_prob: f64) -> Self {
        self.reset_prob = reset_prob;
        self
    }
}

/// What [`Nemesis::decide`] says to do with one outbound frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkAction {
    /// Send normally (after [`Nemesis::delay`]'s wait, if any).
    Send,
    /// Silently drop this frame; keep the connection and outbound queue
    /// otherwise unaffected.
    Drop,
    /// Drop this frame *and* force the connection to reset (the dialer
    /// reconnects on its existing schedule).
    ResetConnection,
}

struct Inner {
    partition: Option<(BTreeSet<NodeId>, BTreeSet<NodeId>)>,
    latency: Duration,
    jitter: Duration,
    drop_prob: f64,
    reset_prob: f64,
    rng: StdRng,
}

/// Running tallies of faults this nemesis has *actually applied* to real
/// outbound frames, kept as atomics outside the [`Inner`] mutex so a test
/// can read them without contending on fault decisions. Snapshot via
/// [`Nemesis::stats`].
#[derive(Debug, Default)]
struct FaultCounters {
    partition_drops: AtomicU64,
    prob_drops: AtomicU64,
    resets: AtomicU64,
    delays_applied: AtomicU64,
}

/// A point-in-time snapshot of how many faults a [`Nemesis`] has applied to
/// outbound peer frames since it was built (see [`Nemesis::stats`]).
///
/// This exists so a fault-injection test can prove a fault *actually fired*
/// rather than passing vacuously: a partition/drop/reset/latency plan that
/// silently no-op'd (a bug in the nemesis, the transport hook, or the test's
/// own wiring) would leave the relevant counter at zero, so asserting it is
/// nonzero is what distinguishes "the cluster survived a real fault" from
/// "the fault never happened and the test proved nothing".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FaultStats {
    /// Frames dropped specifically because their `(from, to)` pair crossed
    /// an active [`Nemesis::partition`]/[`Nemesis::isolate`] split.
    pub partition_drops: u64,
    /// Frames dropped by an ambient `drop_prob` roll (not a partition).
    pub prob_drops: u64,
    /// Frames that forced a connection reset via `reset_prob`.
    pub resets: u64,
    /// Frames that had a nonzero latency/jitter delay applied before send.
    pub delays_applied: u64,
}

impl FaultStats {
    /// Total frames dropped, whether by partition or by probability.
    pub fn total_drops(&self) -> u64 {
        self.partition_drops + self.prob_drops
    }

    /// True if this nemesis has applied *any* fault at all -- the basic
    /// "the plan actually fired" check a fault-injection test needs before
    /// its safety/liveness assertions mean anything.
    pub fn any_fault_applied(&self) -> bool {
        self.partition_drops + self.prob_drops + self.resets + self.delays_applied > 0
    }
}

/// A shared, mutable fault-injection point for one cluster's peer traffic.
/// Built once per test/bench scenario (typically wrapped in an `Arc`, since
/// every replica's [`crate::transport::spawn_peer_dialer`] task holds a
/// clone of it -- see [`crate::config::NodeConfig::nemesis`]) and mutated
/// over the scenario's lifetime via [`Nemesis::partition`]/[`Nemesis::heal`]
/// /[`Nemesis::isolate`]/the `set_*` methods. See the module docs for the
/// full fault model and determinism caveats.
pub struct Nemesis {
    inner: Mutex<Inner>,
    counters: FaultCounters,
}

impl std::fmt::Debug for Nemesis {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Deliberately does not lock/print `partition`/RNG state: `Debug`
        // is only needed so `NodeConfig` (which embeds `Option<Arc<Nemesis>>`)
        // can keep deriving it, not for meaningful introspection.
        f.write_str("Nemesis { .. }")
    }
}

impl Nemesis {
    /// Build a nemesis from a static [`FaultPlan`], with no partition
    /// active.
    pub fn new(plan: FaultPlan) -> Self {
        Self {
            inner: Mutex::new(Inner {
                partition: None,
                latency: plan.latency,
                jitter: plan.jitter,
                drop_prob: plan.drop_prob.clamp(0.0, 1.0),
                reset_prob: plan.reset_prob.clamp(0.0, 1.0),
                rng: StdRng::seed_from_u64(plan.seed),
            }),
            counters: FaultCounters::default(),
        }
    }

    /// Snapshot how many faults this nemesis has actually applied to real
    /// outbound frames so far (see [`FaultStats`]). Tests use this to assert
    /// a fault genuinely fired, so a partition/drop/latency scenario can't
    /// pass vacuously.
    pub fn stats(&self) -> FaultStats {
        FaultStats {
            partition_drops: self.counters.partition_drops.load(Ordering::Relaxed),
            prob_drops: self.counters.prob_drops.load(Ordering::Relaxed),
            resets: self.counters.resets.load(Ordering::Relaxed),
            delays_applied: self.counters.delays_applied.load(Ordering::Relaxed),
        }
    }

    /// Split the cluster into two groups that cannot exchange peer frames
    /// with each other (same-group traffic is unaffected) -- replaces any
    /// previously active partition. Symmetric: either group order works,
    /// [`Nemesis::decide`] checks both directions.
    pub fn partition(&self, a: BTreeSet<NodeId>, b: BTreeSet<NodeId>) {
        self.inner.lock().unwrap().partition = Some((a, b));
    }

    /// Sugar for the common "isolate one node from the rest" case (the
    /// leader-targeting scenario): partitions `{node}` against every id in
    /// `rest`.
    pub fn isolate(&self, node: NodeId, rest: impl IntoIterator<Item = NodeId>) {
        self.partition(
            BTreeSet::from([node]),
            rest.into_iter().filter(|&id| id != node).collect(),
        );
    }

    /// Remove any active partition; peer traffic between every pair flows
    /// again (still subject to `drop_prob`/`reset_prob`/latency, if set).
    pub fn heal(&self) {
        self.inner.lock().unwrap().partition = None;
    }

    /// True if `a` and `b` are currently on opposite sides of an active
    /// partition.
    pub fn is_partitioned(&self, a: NodeId, b: NodeId) -> bool {
        let inner = self.inner.lock().unwrap();
        Self::partitioned_locked(&inner, a, b)
    }

    fn partitioned_locked(inner: &Inner, a: NodeId, b: NodeId) -> bool {
        match &inner.partition {
            None => false,
            Some((g1, g2)) => {
                (g1.contains(&a) && g2.contains(&b)) || (g2.contains(&a) && g1.contains(&b))
            }
        }
    }

    /// Replace the ambient per-frame drop probability.
    pub fn set_drop_prob(&self, drop_prob: f64) {
        self.inner.lock().unwrap().drop_prob = drop_prob.clamp(0.0, 1.0);
    }

    /// Replace the ambient per-frame connection-reset probability.
    pub fn set_reset_prob(&self, reset_prob: f64) {
        self.inner.lock().unwrap().reset_prob = reset_prob.clamp(0.0, 1.0);
    }

    /// Replace the ambient base latency/jitter added before every outbound
    /// frame.
    pub fn set_latency(&self, latency: Duration, jitter: Duration) {
        let mut inner = self.inner.lock().unwrap();
        inner.latency = latency;
        inner.jitter = jitter;
    }

    /// Decide what to do with one outbound frame from `from` to `to`.
    /// Checked by [`crate::transport::spawn_peer_dialer`] before writing
    /// each frame. Partition is checked first (a partitioned pair is
    /// always [`LinkAction::Drop`], regardless of `drop_prob`/`reset_prob`),
    /// then the reset probability (a reset already drops the frame in
    /// hand, so there's no point also rolling the drop probability), then
    /// the drop probability.
    pub fn decide(&self, from: NodeId, to: NodeId) -> LinkAction {
        let mut inner = self.inner.lock().unwrap();
        if Self::partitioned_locked(&inner, from, to) {
            self.counters
                .partition_drops
                .fetch_add(1, Ordering::Relaxed);
            return LinkAction::Drop;
        }
        if inner.reset_prob > 0.0 && inner.rng.gen::<f64>() < inner.reset_prob {
            self.counters.resets.fetch_add(1, Ordering::Relaxed);
            return LinkAction::ResetConnection;
        }
        if inner.drop_prob > 0.0 && inner.rng.gen::<f64>() < inner.drop_prob {
            self.counters.prob_drops.fetch_add(1, Ordering::Relaxed);
            return LinkAction::Drop;
        }
        LinkAction::Send
    }

    /// How long to sleep before writing a frame from `from` to `to`
    /// (`latency` plus a fresh uniform draw in `[0, jitter]`). Returns
    /// `Duration::ZERO` (no lock contention beyond the check itself) when
    /// both are zero, which is the common case for scenarios that only
    /// want drop/partition/reset faults.
    pub fn delay(&self, _from: NodeId, _to: NodeId) -> Duration {
        let mut inner = self.inner.lock().unwrap();
        if inner.latency.is_zero() && inner.jitter.is_zero() {
            return Duration::ZERO;
        }
        let jitter = if inner.jitter.is_zero() {
            Duration::ZERO
        } else {
            let jitter_ms = inner.jitter.as_millis().max(1) as u64;
            let millis = inner.rng.gen_range(0..=jitter_ms);
            Duration::from_millis(millis)
        };
        let total = inner.latency + jitter;
        if !total.is_zero() {
            self.counters.delays_applied.fetch_add(1, Ordering::Relaxed);
        }
        total
    }
}

/// One step of a pre-scripted, timed fault scenario: apply `action` after
/// `after` real time has elapsed since [`run_plan`] started (each step's
/// delay is measured from the *previous* step, i.e. these compose like a
/// storyboard: `[(1s, Partition(..)), (3s, Heal)]` partitions at T+1s and
/// heals at T+4s). This is the "`FaultPlan`-drives-a-scenario" story from
/// issue #34's ask; most of this crate's own tests instead call
/// [`Nemesis`]'s methods directly at the point in the test body where they
/// want to assert something, which gives tighter control over what's
/// asserted when -- `run_plan` is here for scenarios (or a future
/// `queso-bench --nemesis-plan`-style CLI) that just want to fire and
/// forget a whole timeline.
#[derive(Debug, Clone)]
pub enum ScheduledAction {
    Partition(BTreeSet<NodeId>, BTreeSet<NodeId>),
    Isolate(NodeId, BTreeSet<NodeId>),
    Heal,
    SetDropProb(f64),
    SetResetProb(f64),
    SetLatency(Duration, Duration),
}

/// Run a pre-scripted timeline of [`ScheduledAction`]s against `nemesis`,
/// one `tokio::time::sleep` between steps -- see [`ScheduledAction`]'s
/// docs. Intended to be `tokio::spawn`ed alongside a load-generating task.
pub async fn run_plan(nemesis: std::sync::Arc<Nemesis>, steps: Vec<(Duration, ScheduledAction)>) {
    for (after, action) in steps {
        tokio::time::sleep(after).await;
        match action {
            ScheduledAction::Partition(a, b) => nemesis.partition(a, b),
            ScheduledAction::Isolate(node, rest) => nemesis.isolate(node, rest),
            ScheduledAction::Heal => nemesis.heal(),
            ScheduledAction::SetDropProb(p) => nemesis.set_drop_prob(p),
            ScheduledAction::SetResetProb(p) => nemesis.set_reset_prob(p),
            ScheduledAction::SetLatency(base, jitter) => nemesis.set_latency(base, jitter),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_faults_always_sends_with_zero_delay() {
        let nemesis = Nemesis::new(FaultPlan::seeded(1));
        let (a, b) = (NodeId(0), NodeId(1));
        for _ in 0..50 {
            assert_eq!(nemesis.decide(a, b), LinkAction::Send);
        }
        assert_eq!(nemesis.delay(a, b), Duration::ZERO);
    }

    #[test]
    fn partition_drops_cross_group_traffic_both_directions() {
        let nemesis = Nemesis::new(FaultPlan::seeded(1));
        let (a, b, c) = (NodeId(0), NodeId(1), NodeId(2));
        nemesis.partition(BTreeSet::from([a]), BTreeSet::from([b, c]));

        assert_eq!(nemesis.decide(a, b), LinkAction::Drop);
        assert_eq!(nemesis.decide(b, a), LinkAction::Drop);
        assert_eq!(nemesis.decide(a, c), LinkAction::Drop);
        assert_eq!(
            nemesis.decide(b, c),
            LinkAction::Send,
            "same-side traffic must be unaffected"
        );
        assert!(nemesis.is_partitioned(a, b));
        assert!(!nemesis.is_partitioned(b, c));
    }

    #[test]
    fn heal_restores_traffic() {
        let nemesis = Nemesis::new(FaultPlan::seeded(1));
        let (a, b) = (NodeId(0), NodeId(1));
        nemesis.partition(BTreeSet::from([a]), BTreeSet::from([b]));
        assert_eq!(nemesis.decide(a, b), LinkAction::Drop);
        nemesis.heal();
        assert_eq!(nemesis.decide(a, b), LinkAction::Send);
    }

    #[test]
    fn isolate_partitions_one_node_against_the_rest() {
        let nemesis = Nemesis::new(FaultPlan::seeded(1));
        let (a, b, c) = (NodeId(0), NodeId(1), NodeId(2));
        nemesis.isolate(a, [a, b, c]);

        assert_eq!(nemesis.decide(a, b), LinkAction::Drop);
        assert_eq!(nemesis.decide(a, c), LinkAction::Drop);
        assert_eq!(nemesis.decide(b, c), LinkAction::Send);
    }

    #[test]
    fn drop_prob_one_always_drops_and_zero_never_does() {
        let (a, b) = (NodeId(0), NodeId(1));

        let always = Nemesis::new(FaultPlan::seeded(2).with_drop_prob(1.0));
        for _ in 0..20 {
            assert_eq!(always.decide(a, b), LinkAction::Drop);
        }

        let never = Nemesis::new(FaultPlan::seeded(2).with_drop_prob(0.0));
        for _ in 0..20 {
            assert_eq!(never.decide(a, b), LinkAction::Send);
        }
    }

    #[test]
    fn reset_prob_one_always_resets() {
        let (a, b) = (NodeId(0), NodeId(1));
        let nemesis = Nemesis::new(FaultPlan::seeded(3).with_reset_prob(1.0));
        for _ in 0..20 {
            assert_eq!(nemesis.decide(a, b), LinkAction::ResetConnection);
        }
    }

    #[test]
    fn same_seed_reproduces_the_same_decision_sequence() {
        let (a, b) = (NodeId(0), NodeId(1));
        let plan = FaultPlan::seeded(42)
            .with_drop_prob(0.5)
            .with_reset_prob(0.1);
        let n1 = Nemesis::new(plan.clone());
        let n2 = Nemesis::new(plan);
        let seq1: Vec<LinkAction> = (0..200).map(|_| n1.decide(a, b)).collect();
        let seq2: Vec<LinkAction> = (0..200).map(|_| n2.decide(a, b)).collect();
        assert_eq!(
            seq1, seq2,
            "same seed must reproduce the same decision sequence"
        );
        assert!(
            seq1.contains(&LinkAction::Drop),
            "a 0.5 drop_prob run of 200 draws should have dropped at least once"
        );
        assert!(
            seq1.contains(&LinkAction::Send),
            "a 0.5 drop_prob run of 200 draws should have sent at least once"
        );
    }

    #[test]
    fn stats_count_faults_actually_applied() {
        let (a, b, c) = (NodeId(0), NodeId(1), NodeId(2));

        // A clean nemesis applies nothing -- the vacuous-pass baseline every
        // acceptance test's `stats()` assertion is guarding against.
        let clean = Nemesis::new(FaultPlan::seeded(1));
        for _ in 0..10 {
            clean.decide(a, b);
            clean.delay(a, b);
        }
        assert!(!clean.stats().any_fault_applied());
        assert_eq!(clean.stats(), FaultStats::default());

        // Partition drops are attributed to `partition_drops`, and only for
        // pairs that actually cross the split.
        let part = Nemesis::new(FaultPlan::seeded(1));
        part.isolate(a, [a, b, c]);
        part.decide(a, b); // crosses -> partition drop
        part.decide(b, a); // crosses -> partition drop
        part.decide(b, c); // same side -> Send, not counted
        let s = part.stats();
        assert_eq!(s.partition_drops, 2);
        assert_eq!(s.prob_drops, 0);
        assert_eq!(s.total_drops(), 2);
        assert!(s.any_fault_applied());

        // Probabilistic drop, reset, and latency each land in their own bucket.
        let always_drop = Nemesis::new(FaultPlan::seeded(2).with_drop_prob(1.0));
        always_drop.decide(a, b);
        assert_eq!(always_drop.stats().prob_drops, 1);
        assert_eq!(always_drop.stats().partition_drops, 0);

        let always_reset = Nemesis::new(FaultPlan::seeded(3).with_reset_prob(1.0));
        always_reset.decide(a, b);
        assert_eq!(always_reset.stats().resets, 1);

        let latent = Nemesis::new(
            FaultPlan::seeded(4).with_latency(Duration::from_millis(5), Duration::ZERO),
        );
        latent.delay(a, b);
        assert_eq!(latent.stats().delays_applied, 1);
    }

    #[test]
    fn latency_delay_is_within_base_plus_jitter_bounds() {
        let nemesis = Nemesis::new(
            FaultPlan::seeded(4).with_latency(Duration::from_millis(10), Duration::from_millis(5)),
        );
        let (a, b) = (NodeId(0), NodeId(1));
        for _ in 0..50 {
            let d = nemesis.delay(a, b);
            assert!(d >= Duration::from_millis(10), "{d:?}");
            assert!(d <= Duration::from_millis(15), "{d:?}");
        }
    }
}
