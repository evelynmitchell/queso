# Queso — Status Review & Gap Analysis

*Snapshot as of 2026-08-18. Covers what's built, the confidence behind it, and the
gap to a **deployable, documented, CI/CD'd** state.*

*(Supersedes the 2026-07-10 snapshot, which predated Phases 6–8. The headline
change since then: the "no real network, no real disk, no runnable node" gap that
dominated the previous §4 is closed — `crates/net`, `queso-node`, TLS, fsync'd
durability, status/metrics endpoints, a load generator, an operator CLI, a fly.io
deployment path, and an etcd comparison harness all exist and are tested.)*

---

## 1. Executive summary

Queso is a **rigorous, formally-grounded implementation of the QuePaxa consensus
algorithm** — built ground-up in Rust with a deterministic simulation harness, an
explicit property model, two model-checked TLA+ specifications, and a real-network
shell that runs the *same* verified state machine over TCP with fsync'd durability.

**What it is:** a research-/reference-quality implementation of the algorithm and a
linearizable KV store on top of it, exercised under adversarial *simulation* and, at
the transport layer, under real multi-process reboot and fault-injection tests.
Every safety-critical claim is backed by a fresh-environment critical review, a
property-test corpus, and — for the consensus core — an exhaustive TLC proof.

**What it is not:** production software. The KV application is a demonstration
(fixed 64-bit values, no pipelining), durability is a whole-state snapshot fsync
rather than an incremental WAL, there is no log compaction, and none of it has run
real traffic. See §4 for the honest remaining gaps.

**All phases through M7 are complete.**

| Milestone | Scope | Status |
|-----------|-------|--------|
| M0 | Deterministic simulation harness | ✅ merged (#2, #5, #6) |
| M1 | Abstract + concrete consensus | ✅ merged (#8, #10, #12, #14, #16, #20) |
| M2 | Leader fast path | ✅ merged (#17) |
| M3 | Multi-slot log + linearizable KV (crash **and** restart) | ✅ merged (#19, #22) |
| M4 | Hedging (timeout-free recovery) | ✅ merged (#24) |
| M5 | Auto-tuning (multi-armed bandit) | ✅ merged (#28) |
| M6 | Real transport, deployment, fuzzing, perf & comparison | ✅ merged (#30 — #32, #33, #34, #35) |
| M7 | Operability: durability hardening, TLS, status/metrics, admin CLI | ✅ merged (#45 — #46, #47) |
| — | Phase 9: Antithesis-style conformance testing | ⬜ planned (#54 — #55, #56) |

---

## 2. Accomplishments (built & merged)

**Five crates + two formal specs**, ~231 test functions, CI green on every PR.

### `crates/sim` — deterministic simulation harness (Phase 0)
- Single-threaded discrete-event kernel; virtual logical clock; one seeded PRNG.
- **Determinism enforced by lint** — `clippy.toml` bans `Instant::now`, `SystemTime`,
  threads, `thread_rng`, `HashMap`/`HashSet`. Reproducibility gate: `seed → identical
  trace`, byte-for-byte.
- **Two adversary classes at the type level**: content-oblivious (can't read payloads —
  the class under which randomized-liveness holds, per assumption A3) vs content-aware.
- Fault injection: crash, restart (volatile-state loss), partition/heal (a genuine cut,
  dropped at delivery too), slow-node. Trace recorder for offline checking.

### `crates/consensus` — the QuePaxa protocol (Phases 1–3, 5)
- **Abstract core (Algorithm 1):** `tcast`, the E/C/U sets, `best(E)=best(U)` decision.
- **Concrete core (Algorithm 4):** the integer ISR (Algorithm 3), four-phase protocol,
  threshold logical clock (`step = 4·round + phase`), proposer/recorder split — under
  **genuine asynchrony** (reordering, drops, recorders at different steps).
- **Leader fast path (§4.2.5):** reserved `H` priority → one-round-trip commit, with a
  proven-safe fallback to the leaderless path.
- **Hedging (§5):** staggered delay schedule (leader at 0, proposer *k* at `(k-1)·δ`),
  activating only if no earlier progress is seen; **unbounded retry with exponential
  backoff** (issue #13 — backoff bounds the retry *rate*, never the *count*, so a
  network that heals arbitrarily late still resumes the slot). Measured `O(n)` vs
  `O(n²)` messaging under synchrony (10 vs 50 msgs at n=5; 42 vs 882 at n=21).

### `crates/smr` — replicated log + linearizable KV (Phases 4, 6)
- Multi-slot log (prefix consistency, total order, gap-free apply) chaining per-slot
  consensus.
- Linearizable KV with **reads-through-log** (the Meerkat mechanism — a losing `Get`
  catches up and re-proposes; fell out of the existing catch-up path with *zero*
  special-case code).
- Idempotency via `(client, seq)`; **durability + crash-recovery** (durable/volatile
  split, write-before-reply, catch-up with a re-arm watchdog so a restarted-while-
  isolated replica rejoins rather than zombie-parking).
- **In-tree linearizability checker** (Wing-Gong backtracking), proven sound at
  invocation/completion ties and against a real harness-produced stale-read anomaly.
- **Auto-tuning (`tuning.rs`, M5):** multi-armed-bandit leader and δ selection, verified
  safety-inert under the adversary. *Simulation-only — not yet wired to `crates/net`.*

### `crates/net` — the real-I/O shell (Phases 7–8)
The same `SmrNode` state machine, driven by a different `Ctx` — no consensus/SMR logic
changed to accommodate it.

- **Transport & wire** (`transport.rs`, `wire.rs`, `ctx.rs`, `driver.rs`): tokio + TCP,
  length-prefixed bincode framing, peer dialer with reconnection, single-task `!Send`
  driver (structurally immune to the async-heartbeat race class).
- **TLS** (`tls.rs`) — realizes assumption A3's private-channel requirement on links
  that aren't already encrypted.
- **Durability** (`persist.rs`): fsync'd, atomically-renamed whole-state snapshots
  behind a `MAGIC` + `FORMAT_VERSION` header; the write+fsync is offloaded to
  `spawn_blocking`; write-before-reply enforced by a release-mode assert in `driver.rs`;
  group-commit batching so several ready decisions share one fsync.
- **Client & load** (`client.rs`, `bin/queso-bench.rs`): network client library and a
  load generator with honest open-loop latency accounting (coordinated-omission and
  drop-attribution bugs found and fixed in the #37 review).
- **Operability** (`status.rs`, `metrics.rs`, `admin.rs`, `bin/queso-admin.rs`): opt-in
  status/metrics HTTP endpoint with a bounded hand-rolled parser, and an out-of-cluster
  operator CLI that polls every replica and renders a health/catch-up table.
- **Fault injection** (`nemesis.rs`): in-transport latency/drop/partition against a real
  cluster.
- Tests: real multi-process cluster formation, TLS, group commit, nemesis scenarios,
  bench, admin, status parsing, and `restart_recovery.rs` (spawn real `queso-node`
  processes, `SIGKILL` a majority, assert no acknowledged write is lost).

### `crates/compare` — comparison harness (Phase 7.5)
Pluggable targets (`queso_target`, `etcd_target`) over a shared workload, with
normal-case and leader-DoS scenarios and an etcd-absent-tolerant mode; methodology and
results in `docs/compare-etcd.md`.

### `deploy/` — fly.io deployment (Phase 7.3)
`Dockerfile`, `fly.toml` + per-node configs, `.internal` DNS peer discovery, and the
runbook in `docs/deploy-flyio.md`. (Actual multi-region deploys need the owner's account.)

### `spec/` — two model-checked TLA+ specifications
- **Abstract core** — Lemmas B.4, B.5 + corollary `Assert`-checked; TLC exhaustive over
  106,704 states, 0 counterexamples.
- **Concrete core** — the paper's own Appendix D config (2 proposers, 3 recorders, two
  rounds); TLC exhaustive over **13.3M states**, 0 counterexamples; anchored to the
  Appendix C simulation lemmas (C.2/C.3/C.5). Effectively reproduces the paper's SPIN
  verification in TLA+.

### Documentation & process assets
- Planning docs (`docs/00`–`03`): outline, backgrounder, **property model** (P1–P17,
  N1–N6, D1–D11, A1–A7), testing plan.
- A real top-level `README.md` (#52), dual MIT/Apache-2.0 licensing, a deployment
  runbook, a comparison writeup, and a blog post on how Queso stands against
  Antithesis's Raft bug hunt (`docs/blog/`).
- Extensive module-level rustdoc (e.g. the full Agreement safety argument lives in
  `proposer.rs`), two formal-model READMEs, a living roadmap issue (#1).
- **Every PR fresh-environment reviewed** before merge — and the reviews caught *real*
  defects: a lost-acknowledged-write durability bug (#36), a linearizability-checker
  soundness hole, a permanent-zombie-replica liveness bug, a self-send drop in the 7.1
  transport, and several *vacuous* fault tests. This is the project's strongest quality
  signal.

---

## 3. Verification posture (what gives confidence today)

| Layer | Have | Confidence |
|-------|------|-----------|
| Formal | 2 TLA+ models, TLC-exhaustive (safety) | High for consensus *safety logic* |
| Property/DST | Agreement/Validity/Integrity, linearizability, partition, restart, hedging, tuning — adversarial seed corpora | High for the *implementation* under simulated faults |
| Linearizability | Sound in-tree checker + real anomaly control | Good |
| Real transport | Multi-process cluster tests, TLS, majority-reboot durability, in-transport nemesis | Moderate — scripted scenarios, not sustained soak |
| Review | Independent fresh-env review per PR | High — found real bugs |
| CI | fmt + clippy `-D warnings` + build + test on stable, per PR | Good baseline |

**Known verification gaps:**
- **The sim↔real gap (the big one, Phase 9 / #54).** DST verifies the consensus *logic*
  against a mock `Ctx` in a single-threaded, in-process kernel. It never runs the real
  `crates/net` binary — real tokio scheduling, real sockets, real disk — under sustained
  autonomous fault. The project's own bug history says that's exactly where bugs live
  (#36, the 7.1 self-send drop, the #22 catch-up zombie). The 7.4 nemesis is
  *in-transport* and scripted; #55/#56 turn it into a Chain-of-Blocks workload under a
  randomized, long-running fault soak against real processes.
- Liveness is not formally verified (safety-only); the fast path isn't in the concrete
  model.
- Durability fault-injection coverage is incomplete (#39): no torn-write/mid-rename
  crash test, no ENOSPC/EIO test, no unacked-write-safely-lost test, no
  restart-under-load test.
- DST runs are per-PR bounded — no nightly soak, and no trace *shrinking* (deferred
  since Phase 0).

---

## 4. Gap analysis → deployable, documented, CI/CD'd

### 4a. DEPLOYABLE

The P0 list from the previous snapshot — real transport, TLS, wire format, a runnable
node binary, real durability, a client API — is **done**. What remains:

**Correctness confidence (highest value):**
- **Phase 9 conformance testing** (#54/#55/#56) — the real-binary-under-sustained-fault
  harness described in §3. This is the top open item.
- **Durability fault-injection coverage** (#39).

**Functionality still missing:**
- **Log compaction / snapshotting** — the log grows unbounded; every persist rewrites the
  whole `Durable` (`O(log length)` per write). Deliberately deferred as Phase 8.1c; see
  #46's design-decision comment for why byte-incremental deltas aren't an obviously-safe
  next step.
- **Auto-tuning over the real transport** — M5 exists in `crates/smr` but isn't wired
  into `crates/net`, so a deployed cluster still has a fixed leader and δ.
- **Batching / pipelining** — one decision in flight per replica; group-commit batches
  fsyncs, not proposals.
- **Flow control / backpressure** — unaddressed.
- **Bounded concurrent connections on the status port** (#50) — low severity; the port is
  opt-in and documented as internal-only.
- Smaller robustness items: DNS-retry backoff and IPv6 family preference (#42),
  tuning×restart coverage and leader-switch `set_leader` (#29), bench coverage (#40),
  idempotent proposer `start()` (#13).

**Out of scope by design:** Byzantine fault tolerance, side-channel resistance,
dynamic reconfiguration/membership change, general-purpose DB features.

### 4b. DOCUMENTED

Much improved: the top-level `README.md` rewrite (#52) closed the largest gap, and the
deployment runbook, comparison writeup, and licensing all landed.

**Remaining gaps:**
- **Architecture doc** — one page tying the five crates + specs together with a diagram
  (submitter → proposer/recorder → log → KV; where the sim/real seam is). The README
  covers this in prose; a diagram would help.
- **Published API docs** (`cargo doc`) — not built or hosted anywhere.
- **Client guide** — the client library exists; there's no user-facing guide to it.
- **`CONTRIBUTING.md`, `CHANGELOG.md`, ADRs** — none.
- **Conformance matrix** — mapping each property (P1–P17, N1–N6) to its verifying
  test(s)/model in one place. Still the highest-value doc item; the testing plan
  sketches it, it isn't materialized.

### 4c. CI/CD

Unchanged since the last snapshot — this is now the least-advanced area relative to the
code it guards.

**Have (`.github/workflows/ci.yml`, per PR):** `cargo fmt --check`, `cargo clippy
--all-targets -D warnings`, `cargo build --all`, `cargo test --all` on stable. One job,
no schedule.

**CI gaps:**
- **Formal checks not in CI.** The TLA+ models are run by hand. Add a scheduled/nightly
  job (the concrete model is ~13 min — too slow per-PR; run the abstract one per-PR, the
  concrete nightly).
- **No nightly DST soak** — the testing plan calls for per-PR bounded + nightly
  large-seed corpus + minimized-seed regression capture. Only the bounded portion exists.
- **No long-running real-cluster fault soak** — this is Phase 9's CI deliverable (#56
  specifies a bounded CI variant plus a documented long-soak mode).
- **No coverage reporting**, no MSRV/toolchain matrix.
- **No supply-chain checks** — `cargo audit`/`cargo deny` for advisories & licenses.
- **No benchmark regression tracking** — `queso-bench` and `crates/compare` produce
  numbers, but nothing tracks them across commits.

**CD gaps:**
- No release pipeline / semver tagging / crate or binary publishing.
- `deploy/Dockerfile` exists but no image is built or published by CI.
- No deployment automation (IaC, rollout, config management) beyond the fly.io runbook.

---

## 5. Prioritized path forward

1. **Phase 9.1 — Chain-of-Blocks workload + divergence/liveness observer (#55).** The
   next piece of work: it targets the one documented correctness gap, unblocks #56, and
   reuses `queso-bench`, `/metrics`, the client library, and the existing nemesis.
2. **Phase 9.2 — real-binary-under-fault soak (#56).** The piece that actually closes the
   sim↔real gap, plus its bounded CI variant.
3. **Durability fault-injection coverage (#39)** — partly overlapping what a 9.2 soak
   exercises, but the unit-level torn-write and ENOSPC cases are worth having directly.
4. **CI/CD catch-up (cheap, independent):** nightly TLA+ + DST soak, `cargo audit`/`deny`,
   coverage; then an image build and a release pipeline.
5. **Docs:** the property→test **conformance matrix**, an architecture diagram, published
   `cargo doc`, `CONTRIBUTING`/`CHANGELOG`.
6. **Functional depth, if the project pushes further:** log compaction, auto-tuning over
   the real transport, pipelining, flow control.

**Reality check:** Queso is now a verified implementation of the algorithm *with* a real,
tested I/O shell — the previous snapshot's "large lift to deployable" is largely spent.
What separates it from production isn't another phase of features; it's soak time,
compaction, and the operational scaffolding in §4c. It remains, deliberately, a reference
implementation and learning vehicle rather than a datastore to put real data in.
