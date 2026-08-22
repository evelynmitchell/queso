//! The load-generator core behind `queso-bench` (Phase 7.2): session
//! management, the open- and closed-loop schedulers, and the timing rules
//! that make the numbers honest.
//!
//! # Why this is a library module and not just the binary
//!
//! It used to live entirely inside `src/bin/queso-bench.rs`, which meant
//! nothing could test it — a binary's internals are unreachable from
//! integration tests, and the two properties that matter most here are
//! exactly the ones you cannot eyeball from a summary table:
//!
//! - **Coordinated omission.** An open-loop generator that starts its
//!   stopwatch when an operation *begins service* rather than when it was
//!   *scheduled* silently deletes all the queueing delay from its own
//!   measurements, and reports beautiful latencies for a cluster that is
//!   drowning. Issue #37's review found exactly that bug here.
//! - **Drop attribution.** Under sustained overload some operations are
//!   shed before they ever run. If the read/write choice is made *after*
//!   admission, every shed operation has to be charged to some default
//!   kind, and the error columns lie about the mix. That was the second
//!   bug #37 found.
//!
//! Both were fixed in #37 and neither had a regression test, which is
//! issue #40's first two items. They are testable now because the
//! schedulers take an [`OpTarget`] rather than a concrete client: a test
//! supplies a target that is deliberately slower than the offered rate and
//! asserts on what the generator reports about it.
//!
//! # The timing rule, stated once
//!
//! A [`Sample`]'s latency is measured from the instant the operation was
//! **released** — for closed-loop that is when its worker picked it up
//! (there is no queue; the worker owns its session), and for open-loop it
//! is when its scheduled tick fired, *before* it queues for a session. Time
//! spent waiting for a free session is part of the latency, because from a
//! client's point of view it is.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio::task::JoinSet;

use crate::metrics::{OpKind, Sample};
use queso_smr::{ClientId, Command};

/// How many admission slots each session gets: the open-loop scheduler
/// allows `concurrency * ADMISSION_MULTIPLE` operations to be queued
/// waiting for a session before it starts shedding.
///
/// Shedding rather than queueing without limit is the point. An offered
/// rate the cluster can never sustain would otherwise grow the task and
/// session count forever, and the run would die of memory exhaustion
/// instead of reporting the overload it was built to measure.
pub const ADMISSION_MULTIPLE: usize = 8;

/// Whatever the load generator submits operations to.
///
/// Exists so the schedulers can be driven by something other than a real
/// cluster. `queso-bench` passes a [`crate::client::Client`]; the tests in
/// this module pass targets with chosen, controllable service times, which
/// is the only way to assert what the generator does under an overload it
/// cannot otherwise be made to experience reliably.
pub trait OpTarget: Send + Sync + 'static {
    /// Submit one command; `true` if it succeeded.
    fn submit<'a>(&'a self, command: Command) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>>;
}

/// One client session: a [`ClientId`], its monotonic `seq` (A6's
/// one-in-flight-per-session precondition), and its own RNG stream for key
/// and value draws.
pub struct Session {
    id: ClientId,
    seq: u64,
    rng: StdRng,
}

impl Session {
    pub fn new(idx: usize, seed: u64) -> Self {
        Self {
            id: ClientId(idx as u32),
            seq: 0,
            rng: StdRng::seed_from_u64(seed.wrapping_add(idx as u64)),
        }
    }
}

/// Combined run-length stop condition: a wall-clock deadline, a target op
/// count, or both (whichever trips first).
#[derive(Clone, Copy)]
pub struct StopCondition {
    pub deadline: Option<Instant>,
    pub target_ops: Option<u64>,
}

impl StopCondition {
    pub fn should_stop(&self, op_counter: &AtomicU64) -> bool {
        if let Some(deadline) = self.deadline {
            if Instant::now() >= deadline {
                return true;
            }
        }
        if let Some(target) = self.target_ops {
            if op_counter.load(Ordering::Relaxed) >= target {
                return true;
            }
        }
        false
    }
}

/// The workload's shape, shared by both scheduling modes.
#[derive(Clone, Copy)]
pub struct WorkloadConfig {
    /// Client sessions. In open-loop this bounds in-flight operations; in
    /// closed-loop it is the worker count.
    pub concurrency: usize,
    /// Key-space size.
    pub keys: u32,
    /// Fraction of operations that are reads.
    pub read_frac: f64,
    pub seed: u64,
}

/// Build, submit, and time one operation of the caller-chosen kind.
///
/// The kind is decided by the caller, not here, so open-loop can attribute
/// a *shed* operation — one that never gets a session — to the right side
/// of the mix. Latency runs from the caller-supplied `released`, not from
/// entry here, so queueing counts (see the module docs).
async fn do_one_op(
    target: &dyn OpTarget,
    session: &mut Session,
    keys: u32,
    is_read: bool,
    released: Instant,
) -> Sample {
    let key = session.rng.gen_range(0..keys.max(1));
    let seq = session.seq;
    session.seq += 1;

    let (kind, command) = if is_read {
        (
            OpKind::Read,
            Command::Get {
                client: session.id,
                seq,
                key,
            },
        )
    } else {
        let value = session.rng.gen::<i64>();
        (
            OpKind::Write,
            Command::Put {
                client: session.id,
                seq,
                key,
                value,
            },
        )
    };

    let ok = target.submit(command).await;
    Sample {
        kind,
        latency: released.elapsed(),
        ok,
    }
}

/// Closed-loop mode: `concurrency` workers, each owning one [`Session`] for
/// the whole run, looping "submit, wait, submit the next" until `stop`.
///
/// Offered load self-limits to whatever `concurrency` outstanding requests
/// the target can sustain — which is precisely why it cannot show
/// overload, and why open-loop exists.
pub async fn closed_loop_run(
    target: Arc<dyn OpTarget>,
    config: WorkloadConfig,
    stop: StopCondition,
    sample_tx: mpsc::UnboundedSender<Sample>,
    op_counter: Arc<AtomicU64>,
) {
    let mut workers = JoinSet::new();
    for idx in 0..config.concurrency.max(1) {
        let target = Arc::clone(&target);
        let sample_tx = sample_tx.clone();
        let op_counter = Arc::clone(&op_counter);
        workers.spawn(async move {
            let mut session = Session::new(idx, config.seed);
            while !stop.should_stop(&op_counter) {
                let is_read = session.rng.gen_range(0.0..1.0) < config.read_frac;
                // A closed-loop worker owns its session for the whole run,
                // so there is nothing to queue behind: `released` here is
                // exactly the operation's service latency.
                let released = Instant::now();
                let sample =
                    do_one_op(&*target, &mut session, config.keys, is_read, released).await;
                op_counter.fetch_add(1, Ordering::Relaxed);
                let _ = sample_tx.send(sample);
            }
        });
    }
    while workers.join_next().await.is_some() {}
}

/// Open-loop mode: operations are scheduled on a fixed `1/rate` tick
/// regardless of how long prior ones took, so an overloaded target shows up
/// as rising latency rather than throughput silently capping.
///
/// Each tick checks a [`Session`] out of a bounded pool; if the pool is
/// empty the operation queues, and its wait counts toward its latency. An
/// admission semaphore of `concurrency * ADMISSION_MULTIPLE` slots caps how
/// many may be queued at once — beyond that they are shed and recorded as
/// failures of the kind they were already assigned.
pub async fn open_loop_run(
    target: Arc<dyn OpTarget>,
    rate: f64,
    config: WorkloadConfig,
    stop: StopCondition,
    sample_tx: mpsc::UnboundedSender<Sample>,
    op_counter: Arc<AtomicU64>,
) {
    let concurrency = config.concurrency.max(1);
    let period = Duration::from_secs_f64(1.0 / rate.max(0.001));
    let mut ticker = tokio::time::interval(period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);

    let (pool_tx, pool_rx) = mpsc::channel::<Session>(concurrency);
    for idx in 0..concurrency {
        let _ = pool_tx.send(Session::new(idx, config.seed)).await;
    }
    let pool_rx = Arc::new(Mutex::new(pool_rx));
    let admission = Arc::new(Semaphore::new(concurrency * ADMISSION_MULTIPLE));

    // A scheduler-level RNG for the read/write choice, independent of which
    // pooled session (if any) an op ends up on. Deciding the kind *here*,
    // before admission, is what lets a shed op be attributed to the right
    // side of the mix instead of all shed ops being charged to one kind.
    // Its own stream (seed perturbed) so it does not shadow any session's
    // key/value draws.
    let mut sched_rng = StdRng::seed_from_u64(config.seed ^ 0x00D5_C7ED_0BEE_F00D);

    let mut tasks = JoinSet::new();
    loop {
        if stop.should_stop(&op_counter) {
            break;
        }
        ticker.tick().await;
        if stop.should_stop(&op_counter) {
            break;
        }

        // Released now, its scheduled tick having fired. Time from here so
        // any wait for a free session counts toward the latency -- the
        // coordinated-omission correction -- and pick the kind now so a
        // shed op is attributed correctly.
        let released = Instant::now();
        let is_read = sched_rng.gen_range(0.0..1.0) < config.read_frac;

        let Ok(permit) = Arc::clone(&admission).try_acquire_owned() else {
            op_counter.fetch_add(1, Ordering::Relaxed);
            let _ = sample_tx.send(Sample {
                kind: if is_read { OpKind::Read } else { OpKind::Write },
                latency: Duration::ZERO,
                ok: false,
            });
            continue;
        };

        let target = Arc::clone(&target);
        let pool_tx = pool_tx.clone();
        let pool_rx = Arc::clone(&pool_rx);
        let sample_tx = sample_tx.clone();
        let op_counter = Arc::clone(&op_counter);
        let keys = config.keys;
        tasks.spawn(async move {
            let _permit = permit;
            let mut session = {
                let mut rx = pool_rx.lock().await;
                match rx.recv().await {
                    Some(session) => session,
                    None => return, // pool sender dropped: run is shutting down.
                }
            };
            let sample = do_one_op(&*target, &mut session, keys, is_read, released).await;
            op_counter.fetch_add(1, Ordering::Relaxed);
            let _ = sample_tx.send(sample);
            let _ = pool_tx.send(session).await;
        });
    }
    while tasks.join_next().await.is_some() {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::Recorder;

    /// A target with a fixed, chosen service time.
    ///
    /// The point of the [`OpTarget`] indirection: a real cluster cannot be
    /// made reliably slower than the offered rate, and neither property
    /// below is observable except under sustained overload.
    struct SlowTarget {
        service: Duration,
    }

    impl OpTarget for SlowTarget {
        fn submit<'a>(
            &'a self,
            _command: Command,
        ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
            let service = self.service;
            Box::pin(async move {
                tokio::time::sleep(service).await;
                true
            })
        }
    }

    /// Collect a run's samples into a [`Recorder`].
    async fn record<F, Fut>(run: F) -> Recorder
    where
        F: FnOnce(mpsc::UnboundedSender<Sample>, Arc<AtomicU64>) -> Fut,
        Fut: Future<Output = ()>,
    {
        let (tx, mut rx) = mpsc::unbounded_channel::<Sample>();
        let counter = Arc::new(AtomicU64::new(0));
        let collector = tokio::spawn(async move {
            let mut recorder = Recorder::new();
            while let Some(sample) = rx.recv().await {
                recorder.record(sample);
            }
            recorder
        });
        run(tx.clone(), Arc::clone(&counter)).await;
        drop(tx);
        collector.await.expect("collector task")
    }

    /// Every latency the run observed, in the order recorded.
    ///
    /// `Recorder` summarizes rather than retaining raw samples, so the
    /// tests that need individual latencies collect them directly.
    async fn latencies<F, Fut>(run: F) -> Vec<(OpKind, Duration, bool)>
    where
        F: FnOnce(mpsc::UnboundedSender<Sample>, Arc<AtomicU64>) -> Fut,
        Fut: Future<Output = ()>,
    {
        let (tx, mut rx) = mpsc::unbounded_channel::<Sample>();
        let counter = Arc::new(AtomicU64::new(0));
        let collector = tokio::spawn(async move {
            let mut out = Vec::new();
            while let Some(sample) = rx.recv().await {
                out.push((sample.kind, sample.latency, sample.ok));
            }
            out
        });
        run(tx.clone(), Arc::clone(&counter)).await;
        drop(tx);
        collector.await.expect("collector task")
    }

    const SERVICE: Duration = Duration::from_millis(20);

    fn workload(concurrency: usize, read_frac: f64) -> WorkloadConfig {
        WorkloadConfig {
            concurrency,
            keys: 16,
            read_frac,
            seed: 7,
        }
    }

    /// Issue #40's coordinated-omission regression test.
    ///
    /// Under an offered rate the target cannot sustain, operations queue
    /// for a session — and that wait **must** appear in the reported
    /// latency. A generator that starts its stopwatch when an operation
    /// begins service instead reports the service time forever and calls a
    /// drowning cluster healthy. That was the bug #37 found here, and this
    /// is what would have caught it: with a 20ms service time and one
    /// session, a queued operation's latency has to run well past 20ms.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn open_loop_latency_includes_time_queued_for_a_session() {
        let samples = latencies(|tx, counter| async move {
            open_loop_run(
                Arc::new(SlowTarget { service: SERVICE }),
                // 5ms period against a 20ms service time and one session:
                // four operations offered for every one that can run.
                200.0,
                workload(1, 0.5),
                StopCondition {
                    deadline: None,
                    target_ops: Some(120),
                },
                tx,
                counter,
            )
            .await;
        })
        .await;

        let served: Vec<Duration> = samples
            .iter()
            .filter(|(_, _, ok)| *ok)
            .map(|(_, latency, _)| *latency)
            .collect();
        assert!(
            served.len() >= 5,
            "only {} operation(s) completed; the run was too short to say \
             anything about queueing",
            served.len()
        );

        let worst = served.iter().max().copied().expect("some op completed");
        assert!(
            worst >= SERVICE * 3,
            "worst observed latency was {worst:?}, barely more than the {SERVICE:?} \
             service time -- queue wait is being omitted from the measurement, which \
             is exactly the coordinated-omission bug this guards"
        );
    }

    /// The other half of the same property: closed-loop latency should
    /// *not* grow, because a closed-loop worker owns its session and never
    /// queues.
    ///
    /// Without this, the test above could be satisfied by a generator that
    /// simply over-reports latency everywhere. Together they pin the actual
    /// rule: queueing counts, and there is no queueing here.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn closed_loop_latency_is_just_the_service_time() {
        let samples = latencies(|tx, counter| async move {
            closed_loop_run(
                Arc::new(SlowTarget { service: SERVICE }),
                workload(2, 0.5),
                StopCondition {
                    deadline: None,
                    target_ops: Some(12),
                },
                tx,
                counter,
            )
            .await;
        })
        .await;

        assert!(samples.len() >= 12, "run was too short: {}", samples.len());
        let worst = samples
            .iter()
            .map(|(_, latency, _)| *latency)
            .max()
            .expect("some op completed");
        assert!(
            worst < SERVICE * 3,
            "closed-loop latency reached {worst:?} against a {SERVICE:?} service time; \
             a worker owning its session has nothing to queue behind"
        );
    }

    /// Issue #40's admission-queue-exhaustion test.
    ///
    /// Under an unsatisfiable rate the scheduler sheds operations, and each
    /// shed operation must be charged to the read/write side it was
    /// actually drawn as. The original bug charged them all to writes,
    /// which made the error columns lie about the mix precisely when they
    /// mattered most. The kind is now drawn *before* admission, so it
    /// survives being shed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shed_operations_are_attributed_to_the_right_kind() {
        const READ_FRAC: f64 = 0.25;
        let recorder = record(|tx, counter| async move {
            open_loop_run(
                Arc::new(SlowTarget {
                    // Slow enough that the single session is occupied
                    // essentially the whole run, so nearly every tick is
                    // shed once the admission slots fill.
                    service: Duration::from_millis(500),
                }),
                1_000.0,
                workload(1, READ_FRAC),
                StopCondition {
                    deadline: None,
                    target_ops: Some(400),
                },
                tx,
                counter,
            )
            .await;
        })
        .await;

        let summary = recorder.summarize(Duration::from_secs(1));
        let read_errors = summary.reads.errors;
        let write_errors = summary.writes.errors;
        let total = read_errors + write_errors;

        assert!(
            total >= 100,
            "only {total} operation(s) were shed; the run did not reach the overload \
             this test is about"
        );
        // The bug in one assertion: with drops charged to a single kind,
        // one of these is zero.
        assert!(
            read_errors > 0 && write_errors > 0,
            "shed operations were all charged to one kind (reads={read_errors}, \
             writes={write_errors}) -- the read/write choice is being made after \
             admission instead of before it"
        );

        let observed = read_errors as f64 / total as f64;
        assert!(
            (observed - READ_FRAC).abs() < 0.10,
            "shed operations were {:.0}% reads against a {:.0}% read mix \
             (reads={read_errors}, writes={write_errors}); the error columns do not \
             reflect the offered mix",
            observed * 100.0,
            READ_FRAC * 100.0
        );
    }
}
