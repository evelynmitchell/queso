// Real wall-clock timing (bounding this test's own deadline) is exactly
// what's being tested here -- same per-crate-root allow as this crate's
// other test modules.
#![allow(clippy::disallowed_methods)]

//! Proves `cargo test -p queso-compare` never blocks on a real etcd being
//! reachable -- the guardrail this phase's guardrails explicitly call out
//! ("CI must not require etcd ... the etcd side must be skippable/
//! absent-tolerant, no test hangs waiting for an etcd that isn't there").
//!
//! [`queso_compare::etcd_target::EtcdTarget`]'s own protocol-correctness
//! tests (in `src/etcd_target.rs`) already run fully in-process against a
//! fake gateway server and need no real etcd either; this test additionally
//! covers the "etcd genuinely isn't running" case: pointed at an address
//! nothing is listening on, `EtcdTarget` must fail fast (bounded by its own
//! configured timeout) with an ordinary `Err`, not hang.

use std::time::Duration;

use queso_compare::{EtcdTarget, KvTarget};

#[tokio::test]
async fn put_and_get_fail_fast_against_an_unreachable_etcd() {
    // Bind-then-drop a listener to get an address that is free (nothing
    // accepting connections there) but was a real, valid local port a
    // moment ago -- the same deterministic "connection refused" trick
    // `crates/net/src/client.rs`'s own dead-address test uses.
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("bind a probe listener");
    let dead_addr = listener.local_addr().expect("read probe addr");
    drop(listener);

    let target = EtcdTarget::new(format!("http://{dead_addr}"), Duration::from_millis(500))
        .expect("building an EtcdTarget itself never touches the network");

    let bounded = Duration::from_secs(5);
    let put_result = tokio::time::timeout(bounded, target.put(1, 42))
        .await
        .expect("put against an unreachable etcd must fail fast, not hang");
    assert!(
        put_result.is_err(),
        "expected a connection error against an address nothing is listening on"
    );

    let get_result = tokio::time::timeout(bounded, target.get(1))
        .await
        .expect("get against an unreachable etcd must fail fast, not hang");
    assert!(
        get_result.is_err(),
        "expected a connection error against an address nothing is listening on"
    );
}
