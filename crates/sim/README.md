# queso-sim

The deterministic discrete-event simulation harness for Queso — **Phase 0**.
This crate is the whole Phase-0 deliverable described in
[`docs/00-project-outline.md`](../../docs/00-project-outline.md) and
[`docs/03-testing-plan.md`](../../docs/03-testing-plan.md) §1: an injectable
virtual clock, a single seeded PRNG, an in-memory network with a pluggable
scheduler (two adversary classes plus two sanity-baseline schedulers), a
fault-injection API, and a trace recorder. **There is no consensus logic
here.** Nodes in the tests/examples just echo or forward messages — enough
to exercise the harness, nothing more.

## Why this exists

Every later phase's correctness argument leans on deterministic simulation
testing (DST): pick a seed, get an exact, replayable run. If that contract
isn't airtight, DST is unsound and the whole testing strategy in
`docs/03-testing-plan.md` falls apart. So Phase 0's job is narrow but
load-bearing: prove `seed → identical event trace` (property **D9** in
`docs/02-properties.md`) holds, including under an adversarial scheduler and
a battery of injected faults.

## How determinism is guaranteed

1. **Single-threaded.** [`Kernel`](src/kernel.rs) never spawns a thread or
   touches an async runtime. `clippy.toml` (workspace root) denies
   `std::thread::spawn` as a build-time backstop.
2. **No wall clock.** The only notion of time is
   [`LogicalTime`](src/time.rs), an opaque tick counter advanced solely by
   the kernel's event loop as it pops events off the priority queue.
   `clippy.toml` denies `Instant::now`/`SystemTime::now`.
3. **One seeded PRNG.** `Kernel` owns exactly one `rand::rngs::StdRng`,
   seeded once via `SeedableRng::seed_from_u64`. Every random draw anywhere
   in a run — scheduler delay jitter, drop coin-flips, anything a node
   draws via `NodeCtx::rng()` — comes from that single stream, consumed in
   the kernel's single dispatch order. `clippy.toml` denies
   `rand::thread_rng`/`rand::random`.
4. **Total event order.** The event queue orders by `(LogicalTime,
   tiebreak_seq)`: a min-heap on logical time, with ties broken by a
   strictly-monotonic sequence number assigned in the exact order events are
   scheduled. Same-time events can never depend on incidental ordering.
5. **Ordered collections everywhere iteration order is observable.** The
   node registry and all fault-injection sets/maps are `BTreeMap`/
   `BTreeSet`. `clippy.toml` denies `HashMap`/`HashSet` workspace-wide.

These five points together are why `tests/reproducibility.rs` — the
Phase-0 acceptance gate — can assert **byte-for-byte** trace equality across
repeated runs of the same seed, under every scheduler including both
adversary classes, with faults injected mid-run.

## Layout

| Module | Purpose |
|---|---|
| `time` | `LogicalTime`, the virtual clock. |
| `ids` | `NodeId` / `MessageId` / `TimerId` newtypes. |
| `payload` | `Payload` / `Inspectable` — the opaque-payload boundary (see below). |
| `network` | `EnvelopeMeta` (metadata only) and `Envelope<P>` (metadata + payload). |
| `scheduler` | `ObliviousScheduler` / `AwareScheduler<P>` traits, and four implementations: `Fifo`, `RandomScheduler`, `ContentObliviousAdversary`, `ContentAwareAdversary<P>`. |
| `fault` | The fault-injection API: crash, restart, partition/heal, slow-node. |
| `trace` | `Trace` / `TraceEvent` — the append-only, replayable event log. |
| `node` | The `Node<P>` trait and the `NodeCtx<P>` handle nodes use to talk to the kernel. |
| `kernel` (private, re-exported as `Kernel`) | Ties everything together into the DES loop. |

## The opaque-payload mechanism (assumption A3)

`docs/02-properties.md` assumption A3 says the network adversary must be
**content-oblivious**: it can delay, reorder, drop, and target traffic by
metadata, but it must not be able to read message contents (in the real
system this is what TLS on inter-replica links buys you). This crate makes
that a *type-level* guarantee rather than a convention to remember:

```rust
pub trait ObliviousScheduler: fmt::Debug {
    fn on_send(&mut self, meta: &EnvelopeMeta, ctx: &mut SchedulerCtx<'_>) -> Decision;
}

pub trait AwareScheduler<P>: fmt::Debug {
    fn on_send(&mut self, envelope: &Envelope<P>, ctx: &mut SchedulerCtx<'_>) -> Decision;
}
```

`ObliviousScheduler::on_send` simply has no parameter through which a
payload could ever reach it — the method isn't even generic over `P`. There
is no way to write an `ObliviousScheduler` implementation that reads
message contents; the type checker rules it out, it isn't a matter of
discipline. `AwareScheduler<P>::on_send`, by contrast, receives the full
`Envelope<P>` (payload included) and can act on it —
`ContentAwareAdversary` uses this to target messages by an
application-supplied `Inspectable::tag()`, a hook future phases will use for
fast-path-defeat tests (e.g. "drop `Vote` but not `Ping`").

`Kernel<P>` holds exactly one scheduler at a time via the `SchedulerKind<P>`
enum (`Oblivious(Box<dyn ObliviousScheduler>)` or
`Aware(Box<dyn AwareScheduler<P>>)`) and only ever calls the matching
`on_send`. See `src/scheduler.rs` for the four scheduler implementations and
`tests` in that module for a test that exercises both call shapes side by
side.

## Fault injection vs. scheduler adversaries

These are two independent mechanisms that both end up affecting message
delivery, and it's worth keeping them apart:

- **Fault injection** (`Kernel::crash` / `restart` / `partition` / `heal` /
  `set_slow`, or `Kernel::schedule_fault` to pre-plan them on the event
  queue) is *scripted*: a test decides exactly when a node goes down or a
  partition forms.
- **Scheduler adversaries** (`ContentObliviousAdversary` /
  `ContentAwareAdversary`) make their own randomized, in-the-moment
  decisions from the kernel's PRNG stream as traffic flows — including
  their own notion of "partition a minority" and "DoS the leader", which is
  a *behavior*, not a scripted one-off event.

`Kernel::KernelCore::send` checks fault-injection state first (hard drop if
crashed or manually partitioned) and only then consults the active
scheduler — so both layers compose without either having to know about the
other.

## Running things

```sh
# The whole test suite, including the reproducibility gate.
cargo test --all

# The tiny Phase-0 demo: a ring of echoing nodes under a content-oblivious
# adversary, with a crash/restart and a partition/heal injected mid-run.
# It runs the scenario twice and asserts the traces match byte-for-byte.
cargo run -p queso-sim --example echo_demo
```

`tests/reproducibility.rs` is the load-bearing test: same seed, four
schedulers (`Fifo`, `RandomScheduler`, `ContentObliviousAdversary`,
`ContentAwareAdversary`), a scripted fault plan — asserts byte-for-byte
identical traces every time. If that test ever flakes, treat it as a
determinism regression, not a test-infra problem.

`tests/network_and_faults.rs` covers the kernel end to end: FIFO ordering,
crash/restart (including the `on_restart` hook), partition/heal, slow-node
delay multipliers, and that the trace captures every event kind. Component-
level unit tests live next to their implementations (`src/time.rs`,
`src/fault.rs`, `src/scheduler.rs`, `src/trace.rs`, `src/payload.rs`).
