//! The shared workload runner: the same code path drives
//! [`crate::queso_target::QuesoTarget`] and [`crate::etcd_target::EtcdTarget`]
//! (or any other [`crate::target::KvTarget`]) through an identical request
//! mix, rate/concurrency, and measurement -- the actual "apples-to-apples"
//! methodology this crate exists for.
//!
//! This deliberately mirrors `crates/net/src/bin/queso-bench.rs`'s
//! closed-/open-loop shape (same [`StopCondition`], same
//! coordinated-omission-correct latency measurement, same single-collector-
//! task pattern feeding [`queso_net::metrics::Recorder`]) rather than
//! inventing a different one: Phase 7.5's whole point is comparing against
//! that phase's own methodology, not a new one. The one thing this runner
//! does *not* port over is `queso-bench`'s per-worker `Session`
//! (`ClientId`/`seq` bookkeeping) -- that is Queso-specific plumbing now
//! hidden inside [`crate::queso_target::QuesoTarget`], see its docs.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use queso_net::metrics::{OpKind, Recorder, Sample, Summary};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio::task::JoinSet;

use crate::target::KvTarget;

/// One comparison run's workload shape -- deliberately the same dimensions
/// `queso-bench` exposes as CLI flags (see `crates/net/README.md`), so a
/// `queso-compare` invocation and a `queso-bench` invocation with the same
/// flag values are driving the same offered load.
#[derive(Debug, Clone)]
pub struct WorkloadConfig {
    /// Open-loop target rate in ops/sec; `None` selects closed-loop mode.
    pub rate: Option<f64>,
    /// Closed-loop: worker count. Open-loop: in-flight cap.
    pub concurrency: usize,
    /// Fraction of operations that are reads, in `[0.0, 1.0]`.
    pub read_frac: f64,
    /// Key-space size (keys drawn uniformly at random from `0..keys`).
    pub keys: u32,
    /// PRNG seed for the workload's key/value/read-vs-write draws.
    pub seed: u64,
}

/// Combined run-length stop condition: a wall-clock deadline, a target op
/// count, or both (whichever trips first) -- identical semantics to
/// `queso-bench`'s `StopCondition`.
#[derive(Clone, Copy)]
pub struct StopCondition {
    pub deadline: Option<Instant>,
    pub target_ops: Option<u64>,
}

impl StopCondition {
    pub fn duration(dur: Duration) -> Self {
        Self {
            deadline: Some(Instant::now() + dur),
            target_ops: None,
        }
    }

    fn should_stop(&self, op_counter: &AtomicU64) -> bool {
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

/// Perform one operation of the caller-chosen kind against `target`, timed
/// from `start` (coordinated-omission correct: in open-loop mode `start` is
/// the tick's scheduled time, not the moment this function was entered --
/// see `queso-bench`'s `do_one_op` docs for why that matters).
async fn do_one_op<T: KvTarget>(
    target: &T,
    rng: &mut StdRng,
    keys: u32,
    is_read: bool,
    start: Instant,
) -> Sample {
    let key = rng.gen_range(0..keys.max(1));
    let (kind, result) = if is_read {
        (OpKind::Read, target.get(key).await.map(|_| ()))
    } else {
        let value = rng.gen::<i64>();
        (OpKind::Write, target.put(key, value).await)
    };
    Sample {
        kind,
        latency: start.elapsed(),
        ok: result.is_ok(),
    }
}

/// Closed-loop mode: `concurrency` workers, each looping "operate, wait,
/// operate again" until `stop`.
async fn closed_loop_run<T: KvTarget>(
    target: Arc<T>,
    cfg: WorkloadConfig,
    stop: StopCondition,
    sample_tx: mpsc::UnboundedSender<Sample>,
    op_counter: Arc<AtomicU64>,
) {
    let mut workers = JoinSet::new();
    for idx in 0..cfg.concurrency.max(1) {
        let target = Arc::clone(&target);
        let sample_tx = sample_tx.clone();
        let op_counter = Arc::clone(&op_counter);
        let seed = cfg.seed.wrapping_add(idx as u64);
        let keys = cfg.keys;
        let read_frac = cfg.read_frac;
        workers.spawn(async move {
            let mut rng = StdRng::seed_from_u64(seed);
            while !stop.should_stop(&op_counter) {
                let is_read = rng.gen_range(0.0..1.0) < read_frac;
                let start = Instant::now();
                let sample = do_one_op(target.as_ref(), &mut rng, keys, is_read, start).await;
                op_counter.fetch_add(1, Ordering::Relaxed);
                let _ = sample_tx.send(sample);
            }
        });
    }
    while workers.join_next().await.is_some() {}
}

/// Open-loop mode: operations scheduled on a fixed `1/rate`-second tick,
/// each checking out one of `concurrency` pooled worker "slots" -- see
/// `queso-bench`'s `open_loop_run` docs for the coordinated-omission
/// rationale and the admission-semaphore drop behavior under sustained
/// overload, both reproduced here unchanged.
async fn open_loop_run<T: KvTarget>(
    target: Arc<T>,
    rate: f64,
    cfg: WorkloadConfig,
    stop: StopCondition,
    sample_tx: mpsc::UnboundedSender<Sample>,
    op_counter: Arc<AtomicU64>,
) {
    let concurrency = cfg.concurrency.max(1);
    let period = Duration::from_secs_f64(1.0 / rate.max(0.001));
    let mut ticker = tokio::time::interval(period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);

    let (slot_tx, slot_rx) = mpsc::channel::<StdRng>(concurrency);
    for idx in 0..concurrency {
        let _ = slot_tx
            .send(StdRng::seed_from_u64(cfg.seed.wrapping_add(idx as u64)))
            .await;
    }
    let slot_rx = Arc::new(Mutex::new(slot_rx));
    let admission = Arc::new(Semaphore::new(concurrency * 8));
    let mut sched_rng = StdRng::seed_from_u64(cfg.seed ^ 0x00D5_C7ED_0BEE_F00D);

    let mut tasks = JoinSet::new();
    loop {
        if stop.should_stop(&op_counter) {
            break;
        }
        ticker.tick().await;
        if stop.should_stop(&op_counter) {
            break;
        }

        let scheduled = Instant::now();
        let is_read = sched_rng.gen_range(0.0..1.0) < cfg.read_frac;

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
        let slot_tx = slot_tx.clone();
        let slot_rx = Arc::clone(&slot_rx);
        let sample_tx = sample_tx.clone();
        let op_counter = Arc::clone(&op_counter);
        let keys = cfg.keys;
        tasks.spawn(async move {
            let _permit = permit;
            let mut rng = {
                let mut rx = slot_rx.lock().await;
                match rx.recv().await {
                    Some(rng) => rng,
                    None => return,
                }
            };
            let sample = do_one_op(target.as_ref(), &mut rng, keys, is_read, scheduled).await;
            op_counter.fetch_add(1, Ordering::Relaxed);
            let _ = sample_tx.send(sample);
            let _ = slot_tx.send(rng).await;
        });
    }
    while tasks.join_next().await.is_some() {}
}

/// Drive `target` with `cfg`'s workload shape until `stop`, and reduce
/// every completed/failed operation into a
/// [`queso_net::metrics::Summary`] -- the exact type `queso-bench --output
/// json/csv` emits, so a `queso-compare` run's output is diffable against a
/// `queso-bench` run's with zero schema translation.
pub async fn run_workload<T: KvTarget>(
    target: Arc<T>,
    cfg: WorkloadConfig,
    stop: StopCondition,
) -> Summary {
    let op_counter = Arc::new(AtomicU64::new(0));
    let (sample_tx, mut sample_rx) = mpsc::unbounded_channel::<Sample>();

    let collector = tokio::spawn(async move {
        let mut recorder = Recorder::new();
        while let Some(sample) = sample_rx.recv().await {
            recorder.record(sample);
        }
        recorder
    });

    let start = Instant::now();
    match cfg.rate {
        Some(rate) => {
            open_loop_run(
                Arc::clone(&target),
                rate,
                cfg.clone(),
                stop,
                sample_tx.clone(),
                Arc::clone(&op_counter),
            )
            .await;
        }
        None => {
            closed_loop_run(
                Arc::clone(&target),
                cfg.clone(),
                stop,
                sample_tx.clone(),
                Arc::clone(&op_counter),
            )
            .await;
        }
    }
    let elapsed = start.elapsed();
    drop(sample_tx);

    let recorder = collector.await.expect("collector task panicked");
    recorder.summarize(elapsed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::atomic::AtomicI64;
    use std::sync::Mutex;

    /// An in-memory [`KvTarget`] with no network at all -- used to test the
    /// workload runner's own mechanics (stop conditions, read/write mix,
    /// sample plumbing) in isolation from either real target.
    struct MemTarget {
        data: Mutex<BTreeMap<u32, i64>>,
        puts: AtomicI64,
        gets: AtomicI64,
    }

    impl MemTarget {
        fn new() -> Self {
            Self {
                data: Mutex::new(BTreeMap::new()),
                puts: AtomicI64::new(0),
                gets: AtomicI64::new(0),
            }
        }
    }

    impl KvTarget for MemTarget {
        fn name(&self) -> &'static str {
            "mem"
        }

        async fn put(&self, key: u32, value: i64) -> anyhow::Result<()> {
            self.puts.fetch_add(1, Ordering::Relaxed);
            self.data.lock().unwrap().insert(key, value);
            Ok(())
        }

        async fn get(&self, key: u32) -> anyhow::Result<Option<i64>> {
            self.gets.fetch_add(1, Ordering::Relaxed);
            Ok(self.data.lock().unwrap().get(&key).copied())
        }
    }

    #[tokio::test]
    async fn closed_loop_stops_at_the_target_op_count() {
        let target = Arc::new(MemTarget::new());
        let cfg = WorkloadConfig {
            rate: None,
            concurrency: 4,
            read_frac: 0.5,
            keys: 16,
            seed: 7,
        };
        let stop = StopCondition {
            deadline: None,
            target_ops: Some(200),
        };
        let summary = run_workload(Arc::clone(&target), cfg, stop).await;
        assert!(summary.total_ops >= 200, "{summary:#?}");
        assert_eq!(summary.total_errors, 0);
        assert!(summary.overall.p50_us <= summary.overall.p90_us);
        assert!(summary.overall.p90_us <= summary.overall.p99_us);
        assert!(summary.overall.p99_us <= summary.overall.max_us);
        assert!(
            target.puts.load(Ordering::Relaxed) > 0 && target.gets.load(Ordering::Relaxed) > 0,
            "a 0.5 read_frac run over 200 ops should have exercised both put and get"
        );
    }

    #[tokio::test]
    async fn open_loop_respects_a_wall_clock_deadline() {
        let target = Arc::new(MemTarget::new());
        let cfg = WorkloadConfig {
            rate: Some(200.0),
            concurrency: 8,
            read_frac: 0.5,
            keys: 16,
            seed: 3,
        };
        let stop = StopCondition::duration(Duration::from_millis(200));
        let started = Instant::now();
        let summary = run_workload(target, cfg, stop).await;
        assert!(started.elapsed() < Duration::from_secs(5), "must not hang");
        assert!(summary.total_ops > 0, "{summary:#?}");
    }

    #[tokio::test]
    async fn read_frac_roughly_matches_the_configured_mix_over_enough_ops() {
        let target = Arc::new(MemTarget::new());
        let cfg = WorkloadConfig {
            rate: None,
            concurrency: 3,
            read_frac: 0.25,
            keys: 8,
            seed: 42,
        };
        let stop = StopCondition {
            deadline: None,
            target_ops: Some(400),
        };
        let summary = run_workload(target, cfg, stop).await;
        let total = summary.reads.count + summary.writes.count;
        let observed_read_frac = summary.reads.count as f64 / total as f64;
        assert!(
            (observed_read_frac - 0.25).abs() < 0.15,
            "expected roughly 25% reads over {total} ops, got {observed_read_frac:.3} ({summary:#?})"
        );
    }
}
