//! [`KvTarget`]: the one trait a comparison run is generic over.
//!
//! `crate::workload::run_workload` drives whatever implements this trait
//! through an identical request mix, rate/concurrency, and measurement path
//! ([`queso_net::metrics::Recorder`]/[`queso_net::metrics::Summary`]) --
//! that is the entire "apples-to-apples" methodology Phase 7.5 asks for.
//! Two implementations ship in this crate: [`crate::queso_target::QuesoTarget`]
//! (over `queso_net::client::Client`) and [`crate::etcd_target::EtcdTarget`]
//! (over etcd's v3 gRPC-gateway JSON HTTP API -- see that module's docs for
//! why, not `etcd-client`).
//!
//! # Why a fixed `(u32, i64)` shape
//!
//! Both `put`/`get` operate on the exact key/value shape
//! `queso_smr::command::{Key, Value}` already use (`u32`/`i64`) -- not an
//! arbitrary byte string, even though etcd's real API takes arbitrary
//! bytes. This is a deliberate parity choice, not a limitation either side
//! actually has: Queso's KV demo app is hard-coded to a fixed 8-byte `i64`
//! value (see `crates/net/README.md`'s "Honest limits"), so constraining
//! *both* targets to that same shape is what makes a throughput/latency
//! number comparable at all -- an etcd run using larger values would be
//! measuring a different, strictly harder, workload. See
//! `docs/compare-etcd.md`'s "Methodology" section.
//!
//! # Why native `async fn` in a generic trait, not `#[async_trait]`
//!
//! Every caller in this crate is generic (`fn run_workload<T: KvTarget>`,
//! monomorphized per target at the `queso-compare` binary's `--target`
//! dispatch) -- nothing needs `dyn KvTarget`. Return-position `impl Trait`
//! in traits (stable since Rust 1.75) with an explicit `+ Send` bound gives
//! exactly the `Send` futures `tokio::spawn` needs without pulling in the
//! `async-trait` crate's boxing/allocation for a form of dynamic dispatch
//! this crate never uses -- one more way this crate keeps its own
//! dependency footprint small (see the crate's `Cargo.toml` header).

use std::future::Future;

/// A minimal, protocol-agnostic key/value store interface: put a fixed-width
/// value, get it back. See the module docs for why the shape is exactly
/// `(u32, i64)` and why this is a plain trait, not `#[async_trait]`.
pub trait KvTarget: Send + Sync + 'static {
    /// Short, human-readable name for this target (used in log lines and
    /// this crate's own output file-naming convention -- see
    /// `docs/compare-etcd.md` -- not embedded in the
    /// `queso_net::metrics::Summary` JSON/CSV itself, so that schema stays
    /// byte-for-byte identical across targets for diffing).
    fn name(&self) -> &'static str;

    /// Write `value` at `key`. Must resolve only once the write is
    /// durably/linearizably acknowledged by the target (a decided Queso
    /// command; an etcd `Put` response) -- not merely "sent".
    fn put(&self, key: u32, value: i64) -> impl Future<Output = anyhow::Result<()>> + Send;

    /// Read the current value at `key`, or `None` if it has never been
    /// written.
    fn get(&self, key: u32) -> impl Future<Output = anyhow::Result<Option<i64>>> + Send;
}
