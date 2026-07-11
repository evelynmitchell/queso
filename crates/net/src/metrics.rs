//! Throughput/latency metrics for `queso-bench` (Phase 7.2).
//!
//! [`Recorder`] is fed one [`Sample`] per completed (or failed) operation --
//! typically from a single collector task that owns it, receiving samples
//! over an `mpsc` channel from many concurrent worker tasks, so the
//! histograms themselves never need to be shared/locked across tasks (see
//! `crates/net/src/bin/queso-bench.rs`). [`Recorder::summarize`] reduces
//! everything collected so far into a [`Summary`] -- read/write/overall
//! latency histograms (p50/p90/p99/max, via `hdrhistogram`) plus throughput
//! -- which knows how to render itself as human-readable text, JSON, or CSV
//! (`--output` in `queso-bench`, needed so Phase 7.5 can diff runs).

use std::time::Duration;

use hdrhistogram::Histogram;
use serde::Serialize;

/// Which side of the read/write mix an operation belongs to, for
/// [`Recorder`]'s per-kind histograms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpKind {
    Read,
    Write,
}

/// One completed operation, as observed by the load generator: how long it
/// took end-to-end (dispatch/queueing included -- see the module docs on
/// open- vs closed-loop measurement in `queso-bench`), and whether it
/// succeeded.
pub struct Sample {
    pub kind: OpKind,
    pub latency: Duration,
    pub ok: bool,
}

/// The smallest and largest latency an operation could plausibly take
/// against a localhost or WAN cluster: 1 microsecond floor, 5-minute
/// ceiling (a request that takes longer than that is not meaningfully
/// distinguished from a hang, and the ceiling only bounds `hdrhistogram`'s
/// internal storage, not what latencies it accepts below it).
const HIST_LOW_US: u64 = 1;
const HIST_HIGH_US: u64 = 5 * 60 * 1_000_000;
const HIST_SIGFIGS: u8 = 3;

fn new_histogram() -> Histogram<u64> {
    Histogram::new_with_bounds(HIST_LOW_US, HIST_HIGH_US, HIST_SIGFIGS)
        .expect("HIST_LOW_US/HIST_HIGH_US/HIST_SIGFIGS are valid hdrhistogram bounds")
}

/// Accumulates [`Sample`]s into per-kind and combined latency histograms
/// plus error counts. Not `Sync` -- see the module docs for the
/// single-collector-task pattern this is meant to be used with.
pub struct Recorder {
    reads: Histogram<u64>,
    writes: Histogram<u64>,
    overall: Histogram<u64>,
    read_errors: u64,
    write_errors: u64,
}

impl Default for Recorder {
    fn default() -> Self {
        Self::new()
    }
}

impl Recorder {
    pub fn new() -> Self {
        Self {
            reads: new_histogram(),
            writes: new_histogram(),
            overall: new_histogram(),
            read_errors: 0,
            write_errors: 0,
        }
    }

    /// Record one completed or failed operation. A failed operation
    /// (`ok: false`) counts toward that kind's error total but is not
    /// recorded into any latency histogram -- a timeout/connection error's
    /// "latency" (however long the client gave up after) is not a
    /// meaningful success-path latency sample.
    pub fn record(&mut self, sample: Sample) {
        if !sample.ok {
            match sample.kind {
                OpKind::Read => self.read_errors += 1,
                OpKind::Write => self.write_errors += 1,
            }
            return;
        }
        // hdrhistogram's low bound is 1us; clamp rather than silently drop
        // an implausibly-fast (sub-microsecond, e.g. a rounding artifact)
        // sample.
        let us = (sample.latency.as_micros().min(u128::from(u64::MAX)) as u64).max(HIST_LOW_US);
        let hist = match sample.kind {
            OpKind::Read => &mut self.reads,
            OpKind::Write => &mut self.writes,
        };
        // A sample above HIST_HIGH_US (5 minutes) is dropped rather than
        // panicking or resizing -- at that point something is badly wrong
        // with the run and the count/error totals still capture it.
        let _ = hist.record(us);
        let _ = self.overall.record(us);
    }

    /// Reduce everything recorded so far into a [`Summary`], given the
    /// wall-clock duration the run took (for throughput).
    pub fn summarize(&self, elapsed: Duration) -> Summary {
        let elapsed_secs = elapsed.as_secs_f64();
        let total_ops = self.overall.len() + self.read_errors + self.write_errors;
        let total_errors = self.read_errors + self.write_errors;
        let throughput_ops_per_sec = if elapsed_secs > 0.0 {
            total_ops as f64 / elapsed_secs
        } else {
            0.0
        };
        Summary {
            duration_secs: elapsed_secs,
            total_ops,
            total_errors,
            throughput_ops_per_sec,
            reads: OpStats::from_histogram(&self.reads, self.read_errors),
            writes: OpStats::from_histogram(&self.writes, self.write_errors),
            overall: OpStats::from_histogram(&self.overall, total_errors),
        }
    }
}

/// Latency percentiles (microseconds) plus counts for one op kind (or the
/// combined `overall` bucket).
#[derive(Debug, Clone, Serialize)]
pub struct OpStats {
    pub count: u64,
    pub errors: u64,
    pub mean_us: f64,
    pub p50_us: u64,
    pub p90_us: u64,
    pub p99_us: u64,
    pub max_us: u64,
}

impl OpStats {
    fn from_histogram(h: &Histogram<u64>, errors: u64) -> Self {
        Self {
            count: h.len(),
            errors,
            mean_us: h.mean(),
            p50_us: h.value_at_quantile(0.50),
            p90_us: h.value_at_quantile(0.90),
            p99_us: h.value_at_quantile(0.99),
            max_us: h.max(),
        }
    }

    fn csv_fields(&self, prefix: &str) -> Vec<(String, String)> {
        vec![
            (format!("{prefix}_count"), self.count.to_string()),
            (format!("{prefix}_errors"), self.errors.to_string()),
            (format!("{prefix}_mean_us"), format!("{:.1}", self.mean_us)),
            (format!("{prefix}_p50_us"), self.p50_us.to_string()),
            (format!("{prefix}_p90_us"), self.p90_us.to_string()),
            (format!("{prefix}_p99_us"), self.p99_us.to_string()),
            (format!("{prefix}_max_us"), self.max_us.to_string()),
        ]
    }

    fn write_text(&self, out: &mut String, label: &str) {
        out.push_str(&format!(
            "  {label:<8} count={:<8} errors={:<6} mean={:>8.1}us  p50={:>7}us  p90={:>7}us  p99={:>7}us  max={:>7}us\n",
            self.count, self.errors, self.mean_us, self.p50_us, self.p90_us, self.p99_us, self.max_us
        ));
    }
}

/// A load generator run's full result: throughput plus read/write/overall
/// latency distributions. `#[derive(Serialize)]` backs `--output json`;
/// [`Summary::to_csv`]/[`Summary::to_text`] cover the other two
/// `queso-bench --output` modes.
#[derive(Debug, Clone, Serialize)]
pub struct Summary {
    pub duration_secs: f64,
    pub total_ops: u64,
    pub total_errors: u64,
    pub throughput_ops_per_sec: f64,
    pub reads: OpStats,
    pub writes: OpStats,
    pub overall: OpStats,
}

impl Summary {
    /// Pretty-printed JSON (`--output json`).
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("Summary contains no non-finite floats to reject")
    }

    /// A single CSV header line and a single data line (`--output csv`) --
    /// one run per invocation, so one row is all a run produces; comparing
    /// runs (Phase 7.5) means concatenating data lines under one header.
    pub fn to_csv(&self) -> String {
        let mut fields = vec![
            (
                "duration_secs".to_string(),
                format!("{:.3}", self.duration_secs),
            ),
            ("total_ops".to_string(), self.total_ops.to_string()),
            ("total_errors".to_string(), self.total_errors.to_string()),
            (
                "throughput_ops_per_sec".to_string(),
                format!("{:.2}", self.throughput_ops_per_sec),
            ),
        ];
        fields.extend(self.reads.csv_fields("reads"));
        fields.extend(self.writes.csv_fields("writes"));
        fields.extend(self.overall.csv_fields("overall"));

        let header = fields
            .iter()
            .map(|(k, _)| k.as_str())
            .collect::<Vec<_>>()
            .join(",");
        let row = fields
            .iter()
            .map(|(_, v)| v.as_str())
            .collect::<Vec<_>>()
            .join(",");
        format!("{header}\n{row}\n")
    }

    /// Human-readable text summary (`--output text`, the default).
    pub fn to_text(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!(
            "queso-bench: {} ops in {:.2}s = {:.1} ops/sec ({} errors)\n",
            self.total_ops, self.duration_secs, self.throughput_ops_per_sec, self.total_errors
        ));
        self.overall.write_text(&mut out, "overall");
        self.reads.write_text(&mut out, "reads");
        self.writes.write_text(&mut out, "writes");
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_reads_and_writes_separately() {
        let mut recorder = Recorder::new();
        for _ in 0..10 {
            recorder.record(Sample {
                kind: OpKind::Read,
                latency: Duration::from_millis(1),
                ok: true,
            });
        }
        for _ in 0..5 {
            recorder.record(Sample {
                kind: OpKind::Write,
                latency: Duration::from_millis(2),
                ok: true,
            });
        }
        recorder.record(Sample {
            kind: OpKind::Read,
            latency: Duration::ZERO,
            ok: false,
        });

        let summary = recorder.summarize(Duration::from_secs(1));
        assert_eq!(summary.reads.count, 10);
        assert_eq!(summary.reads.errors, 1);
        assert_eq!(summary.writes.count, 5);
        assert_eq!(summary.writes.errors, 0);
        assert_eq!(summary.overall.count, 15);
        assert_eq!(summary.total_ops, 16);
        assert_eq!(summary.total_errors, 1);
        assert!((summary.throughput_ops_per_sec - 16.0).abs() < 1e-9);
        // p50 latency should land near the (dominant) 1ms read samples.
        assert!(summary.reads.p50_us >= 900 && summary.reads.p50_us <= 1100);
    }

    #[test]
    fn json_and_csv_round_trip_shapes() {
        let recorder = Recorder::new();
        let summary = recorder.summarize(Duration::from_secs(1));
        let json = summary.to_json();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["total_ops"], 0);
        assert!(parsed["overall"]["p99_us"].is_number());

        let csv = summary.to_csv();
        let mut lines = csv.lines();
        let header = lines.next().unwrap();
        let row = lines.next().unwrap();
        assert_eq!(header.split(',').count(), row.split(',').count());
        assert!(header.starts_with("duration_secs,total_ops,total_errors,throughput_ops_per_sec"));
    }
}
