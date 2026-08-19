# `queso-conformance` — Chain-of-Blocks conformance harness (Phase 9.1)

A Queso port of Antithesis's [Chain-of-Blocks][cob] workload, plus the
divergence and liveness observers that judge it. Issue [#55], part of the
Phase 9 epic [#54].

## What problem this is part of

Queso's deterministic simulation testing verifies the consensus *logic*
against a mock context in a single-threaded, in-process kernel. It does not
run the real `queso-node` binary — real tokio scheduling, real sockets, real
disk — under fault. The project's own bug history says that gap is where the
bugs have been: the [#36] lost-write durability bug, the 7.1 self-send drop,
the [#22] catch-up "zombie replica". Phase 9 exists to close it.

**This crate is 9.1: the workload and the observers.** It runs in-process,
where their own correctness can be established. **[#56] is what closes the
gap** — it points these same observers at real `queso-node` processes under
sustained turbulence. Nothing here should be read as having closed it: an
in-process Chain-of-Blocks run exercises the same code paths the existing
DST suite already does.

## The workload

A CoB state machine is the pair `(n, h)`: on applying command `C`,
`n += 1; h = hash(h ‖ C)`. Because `h` is a chain, a single disagreement in
the applied sequence propagates forward forever — two replicas that applied
different commands at slot 5 differ at every `n > 5`. That is what makes the
property checkable without reading anyone's log.

- **Safety:** no two replicas may show a different `h` at the same `n`.
  (Queso's P1 Agreement / P5 prefix consistency / P6 total order, restated.)
- **Liveness:** a replica that sits behind the cluster frontier without
  advancing, after faults have healed, is stalled.

Queso's `Value` is a fixed `i64`, so a CoB "random byte-array command" is
carried as its 64-bit digest in an ordinary `Command::Put` ([#55]'s
preferred option (a)). **No change to `queso-consensus` or `queso-smr`** —
the chain is folded in the harness, not in the cluster.

## Layout

| Module | What it holds |
|--------|---------------|
| `chain` | The `(n, h)` state machine, the command encoding, and the hash — re-exported from the [`queso-chain`](../chain) crate, which `queso-net` also depends on so node-side and harness-side hashes come from the same code |
| `observer` | `Observer`: ingests `(replica, n, h)` samples, reports `Divergence` and `Stall`, keeps a per-transition log for root-causing |
| `source` | `CobTarget`, the seam a cluster implements; `SimCluster` implements it in-process; `Observability` chooses sampling density |
| `workload` | The stateless CoB client and the run/settle/converge drivers |

## Findings this phase hands to 9.2

Two things surfaced while building this that [#56] needs to act on.

### 1. Frontier-only sampling produces a vacuous pass

`queso-net`'s `/metrics` exposes `next_slot` — a frontier, not a history. If
9.2 polls only that, replicas are almost never at the same `n` at the same
instant (they lag each other by design), so the observer has nothing to
compare and reports "no divergence" having checked essentially nothing.
Measured on one healthy run (`tests/imperfect_observability.rs`):

| sampling | samples | cross-replica comparisons |
|----------|--------:|--------------------------:|
| frontier-only | 99 | **2** |
| checkpoints every 4 slots | 117 | **20** |

**The fix is checkpointed chain hashes**: have each node retain the chain
hash at every multiple of `k` slots it crosses, and expose that small table.
Every replica then reports at the *same* `n` values, so comparisons align by
construction. `Observability::Checkpoints` models exactly this, and the
tests show it catches divergence that frontier-only sampling misses.

**Status: done.** `queso-net` folds this chain and serves it at `GET /chain`
behind `--chain-checkpoints N` (see that crate's README); `queso-soak`'s
`RealCluster` polls it as its `CobTarget` source, and the sustained soak
measures ~760 cross-replica comparisons over a 20s run — the concrete payoff
of checkpointed over frontier-only sampling.

### 2. A Queso replica does not catch up unless it is given work

Unlike a leader-driven protocol that heartbeats idle followers, a Queso
replica learns a slot's decision by *participating*. A replica given no work
sits at whatever frontier it last reached — correct behavior (P5 permits
lagging; only divergence is forbidden), but it means "behind and not
advancing" is only evidence of a stall if that replica was actually asked to
do something. `workload::converge` gives every live replica traffic for
exactly this reason, and the liveness budget should be chosen *after* it.
See `Observer::stalls`'s "Choosing a budget".

## Running it

```sh
cargo test -p queso-conformance
```

Everything is deterministic and bounded: a run is reproducible from
`(cluster seed, workload seed)`, and the crate opts into the workspace
determinism lints. There is no long-soak mode here — that arrives with
[#56], whose real-process source will need real time and real sockets and
will therefore have to drop that lint opt-in the way `queso-net` and
`queso-compare` do.

## Honest limitations

- **In-process only.** It does not test the real I/O layer. That is [#56].
- **The observer sees nothing between samples.** A replica that diverged and
  was repaired before its next sample would leave no trace. Queso has no such
  repair path (a decided slot is immutable), so this is theoretical — but it
  is a gap.
- **No causal attribution.** A report names the replicas, the `n`, and the
  transitions around it; root-causing from there is a human's job.
- **The hash is not cryptographic.** FNV-1a with a SplitMix64 finalizer,
  sized to catch divergence that arises by accident, not to resist an
  adversary choosing payloads to force a collision.
- **Injected divergence is synthetic.** Queso does not diverge, so
  `tests/observer_detects.rs` corrupts a real run's sample stream in transit
  to prove the detector fires. That validates the detector, not the cluster.

[cob]: https://antithesis.com/docs/resources/chain-of-blocks/
[#22]: https://github.com/evelynmitchell/queso/issues/22
[#36]: https://github.com/evelynmitchell/queso/issues/36
[#54]: https://github.com/evelynmitchell/queso/issues/54
[#55]: https://github.com/evelynmitchell/queso/issues/55
[#56]: https://github.com/evelynmitchell/queso/issues/56
