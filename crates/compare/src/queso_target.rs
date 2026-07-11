//! [`QuesoTarget`]: [`crate::target::KvTarget`] over `queso_net::client::Client`.

use std::sync::atomic::{AtomicU32, Ordering};

use queso_net::client::Client;
use queso_smr::{ClientId, Command, Outcome};

use crate::target::KvTarget;

/// Drives a Queso cluster through its ordinary client library
/// (`queso_net::client::Client`) -- exactly what `queso-bench` and a real
/// caller use, no shortcuts.
///
/// # Session/`ClientId` handling
///
/// `queso_smr`'s A6 precondition (see `queso_smr::command::ClientSession`'s
/// docs) requires at most one in-flight operation per `ClientId`/session at
/// a time. Unlike `queso-bench` (which hands each closed-loop worker a
/// long-lived `Session` with a monotonically increasing `seq`, see
/// `crates/net/src/bin/queso-bench.rs`), `QuesoTarget` mints a **fresh**
/// `ClientId` for every single `put`/`get` call (`seq` always `0`). That
/// trivially satisfies A6 (a session used exactly once has nothing to race
/// with) and keeps this type's state to one atomic counter regardless of
/// how many concurrent workers `crate::workload::run_workload` spins up --
/// no per-worker session bookkeeping needs to leak into the
/// protocol-agnostic workload runner. The trade-off, honestly noted: a
/// long-running comparison leaves one dedup entry per operation in the
/// replicas' in-memory `ClientSession` map (see that type's docs) instead
/// of one per worker -- irrelevant for the bounded, few-second/few-thousand
/// -op runs this crate's tests and CLI target, not appropriate for an
/// unbounded soak test.
pub struct QuesoTarget {
    client: Client,
    next_client_id: AtomicU32,
}

impl QuesoTarget {
    pub fn new(client: Client) -> Self {
        Self {
            client,
            next_client_id: AtomicU32::new(1),
        }
    }

    fn fresh_client_id(&self) -> ClientId {
        ClientId(self.next_client_id.fetch_add(1, Ordering::Relaxed))
    }
}

impl KvTarget for QuesoTarget {
    fn name(&self) -> &'static str {
        "queso"
    }

    async fn put(&self, key: u32, value: i64) -> anyhow::Result<()> {
        let command = Command::Put {
            client: self.fresh_client_id(),
            seq: 0,
            key,
            value,
        };
        match self.client.submit(&command).await? {
            Outcome::Put => Ok(()),
            other => anyhow::bail!("QuesoTarget::put: unexpected outcome {other:?}"),
        }
    }

    async fn get(&self, key: u32) -> anyhow::Result<Option<i64>> {
        let command = Command::Get {
            client: self.fresh_client_id(),
            seq: 0,
            key,
        };
        match self.client.submit(&command).await? {
            Outcome::Get(value) => Ok(value),
            other => anyhow::bail!("QuesoTarget::get: unexpected outcome {other:?}"),
        }
    }
}
