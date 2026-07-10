# Queso — Status Review & Gap Analysis

*Snapshot as of 2026-07-10. Covers what's built, the confidence behind it, and the
gap to a **deployable, documented, CI/CD'd** state.*

---

## 1. Executive summary

Queso is a **rigorous, formally-grounded, simulation-based implementation of the
QuePaxa consensus algorithm** — built ground-up in Rust with a test harness, an
explicit property model, and two model-checked TLA+ specifications, exactly as the
original brief asked.

**What it is:** a research-/reference-quality implementation of the algorithm and a
linearizable KV store on top of it, exercised under adversarial *simulation*.
Every safety-critical claim is backed by a fresh-environment critical review, a
property-test corpus, and — for the consensus core — an exhaustive TLC proof.

**What it is not (yet):** a deployable distributed *service*. Everything runs on an
in-memory deterministic discrete-event simulator, not a real network or real disk.
The distance from here to "deployable" is essentially the two phases that were
explicitly scoped **out** of the committed M0–M5 goal (real transport + operability),
plus productionization work described in §4.

**Committed scope (M0–M5): 5 of 6 milestones complete.**

| Milestone | Scope | Status |
|-----------|-------|--------|
| M0 | Deterministic simulation harness | ✅ merged (#5, #6) |
| M1 | Abstract + concrete consensus | ✅ merged (#8, #10, #12, #14, #20) |
| M2 | Leader fast path | ✅ merged (#17) |
| M3 | Multi-slot log + linearizable KV (crash **and** restart) | ✅ merged (#19, #22) |
| M4 | Hedging (timeout-free recovery) | ✅ merged (#24) |
| M5 | Auto-tuning (multi-armed bandit) | ⬜ not started (Phase 6) |

---

## 2. Accomplishments (built & merged)

**Three crates + two formal specs**, ~n test suites, CI green on every PR.

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
  activating only if no earlier progress is seen; **unbounded retry** (removed the
  hard cap). Measured `O(n)` vs `O(n²)` messaging under synchrony (10 vs 50 msgs at
  n=5; 42 vs 882 at n=21).

### `crates/smr` — replicated log + linearizable KV (Phase 4)
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
- Extensive module-level rustdoc (e.g. the full Agreement safety argument lives in
  `proposer.rs`), two formal-model READMEs, a living roadmap issue (#1).
- **Every PR fresh-environment reviewed** before merge — and the reviews caught *real*
  defects: a linearizability-checker soundness hole, a permanent-zombie-replica
  liveness bug, several accuracy/doc issues. This is the project's strongest quality
  signal.

---

## 3. Verification posture (what gives confidence today)

| Layer | Have | Confidence |
|-------|------|-----------|
| Formal | 2 TLA+ models, TLC-exhaustive (safety) | High for consensus *safety logic* |
| Property/DST | Agreement/Validity/Integrity, linearizability, partition, restart, hedging — adversarial seed corpora | High for the *implementation* under simulated faults |
| Linearizability | Sound in-tree checker + real anomaly control | Good |
| Review | Independent fresh-env review per PR | High — found real bugs |
| CI | fmt + clippy `-D warnings` + build + test on stable, per PR | Good baseline |

**Known verification gaps** (see also §4): liveness is not formally verified
(safety-only); the fast path isn't in the concrete model; no Jepsen-against-real-cluster;
no performance benchmarks; DST runs are per-PR bounded (no nightly soak); no trace
*shrinking* (deferred since Phase 0).

---

## 4. Gap analysis → deployable, documented, CI/CD'd

The honest headline: **crossing from "simulated algorithm" to "deployable service"
is a large body of work** — most of it the two phases deliberately left beyond the
committed scope. Grouped by the three goals you named.

### 4a. DEPLOYABLE

The single biggest gap: **there is no real network, no real disk, and no runnable
node.** The whole system is driven in-process by the sim.

**P0 — must-have for any deployment (Phase 7 + Phase 8 core):**
- **Real transport.** Swap the sim network for TCP. The transport already sits behind
  an interface, so this is additive — but nontrivial (connection mgmt, reconnection,
  framing). *~Phase 7.*
- **TLS.** Load-bearing, not optional: the content-oblivious-adversary assumption (A3)
  that the randomized-liveness guarantee depends on is only realized in practice by
  encrypting inter-replica links. **Currently implemented nowhere.** *~Phase 7.*
- **Wire format / serialization.** No Protobuf/gRPC or equivalent; the sim passes Rust
  structs. Need a versioned wire schema. *~Phase 7.*
- **A runnable node binary.** No `main`, no config file/flags, no cluster bootstrap or
  membership loading. Today there is nothing to start. *~Phase 7/8.*
- **Real durability.** Phase-4b models durability in-memory (survives *sim* restart);
  there is **no fsync/WAL/crash-consistent on-disk storage**. A real crash needs real
  persistence. *~Phase 8.*
- **A client API.** The KV is driven in-process; there is no network-facing client
  protocol or SDK, and no client-side retry/session library. *~Phase 7.*

**P1 — needed for a *usable* deployment:**
- **Reconfiguration / membership change** (add/remove replica) — today membership is
  static (A4). *~Phase 8.*
- **Log compaction / snapshotting** — the log grows unbounded; no GC. *~Phase 8.*
- **Auto-tuning (M5, Phase 6)** — leader and δ are fixed config; the paper's MAB
  adaptivity is what makes it operable without manual tuning across changing networks.
- **Batching / pipelining** — for throughput; sim path is one-op-at-a-time. *~Phase 7.*
- **Flow control / backpressure** — unaddressed.

**P2 — operability:**
- Metrics/telemetry (property D10 — not implemented), structured logging, health/
  readiness endpoints, admin tooling, per-replica placement.

**Out of scope by design (state explicitly, don't silently omit):** Byzantine fault
tolerance, side-channel resistance, general-purpose DB features.

### 4b. DOCUMENTED

Strong on *design & code* docs; missing *user & operator* docs.

**Have:** planning docs (00–03), the property model, deep rustdoc, formal-model READMEs,
a living roadmap issue, detailed PR history.

**Gaps:**
- **A real top-level `README.md`** — the current one is still the original brief. Needs:
  what Queso is, build/test instructions, architecture overview, crate map, status.
- **Architecture doc** — one page tying the three crates + specs together with a diagram
  (submitter → proposer/recorder → log → KV; where the sim boundary is).
- **Published API docs** (`cargo doc`) — not built/hosted anywhere.
- **Operator/deployment guide** — pending something to deploy (Phase 7/8), then how to
  configure a cluster, TLS, placement, recovery.
- **Client guide** — how to use the KV (once a client API exists).
- **`CONTRIBUTING.md`, `CHANGELOG.md`, ADRs** — none. A **conformance matrix** mapping
  each property (P1–P17…) to its verifying test(s)/model in one place would be high-value
  (the testing plan sketches it; it isn't materialized).

### 4c. CI/CD

Good CI **foundation**; no CD, and CI doesn't yet exercise the deepest assets.

**Have (`.github/workflows/ci.yml`, per PR):** `cargo fmt --check`, `cargo clippy
--all-targets -D warnings`, `cargo build --all`, `cargo test --all` on stable.

**CI gaps:**
- **Formal checks not in CI.** The TLA+ models are run by hand. Add a **scheduled/nightly**
  job (the concrete model is ~13 min — too slow per-PR; run the abstract one per-PR, the
  concrete nightly).
- **No nightly DST soak** — the testing plan calls for per-PR bounded + nightly large-seed
  corpus + minimized-seed regression capture. Only the bounded per-PR portion exists.
- **No linearizability / Jepsen-style job against a real multi-process cluster** (blocked
  on real transport).
- **No coverage reporting**, no MSRV/toolchain matrix.
- **No supply-chain checks** — `cargo audit`/`cargo deny` for advisories & licenses.
- **No benchmark regression tracking** (no benchmarks yet — Phase 7).

**CD gaps (essentially everything — nothing to ship yet):**
- No release pipeline / semver tagging / crate or binary publishing.
- No container image / packaging.
- No deployment automation (IaC, rollout, config management) — pending a deployable node.

---

## 5. Prioritized path forward

**To finish the committed algorithmic scope**
1. **Phase 6 — auto-tuning (M5).** Completes the QuePaxa story (self-tuning leader + δ).

**To reach "deployable" (the largest lift — Phase 7, then Phase 8)**
2. **Phase 7 — real transport & performance:** TCP + **TLS** + wire format + a node
   binary + config/bootstrap + batching; then LAN/WAN throughput/latency and adversarial
   benchmarks. This is the gate to Jepsen and to any real deployment.
3. **Phase 8 — operability:** real fsync durability, reconfiguration, log compaction/
   snapshots, metrics/observability, a client SDK.

**To reach "documented" (can proceed in parallel, cheap)**
4. Rewrite the top-level `README.md`; add an architecture doc + diagram; publish
   `cargo doc`; materialize the property→test **conformance matrix**; add
   `CONTRIBUTING`/`CHANGELOG`.

**To reach "CI/CD'd" (incremental, mostly independent)**
5. Add nightly jobs: TLA+ checks, DST soak, `cargo audit`/`deny`, coverage. Then, once a
   node binary exists (Phase 7), a release pipeline + container image + deploy automation.

**Reality check:** items 2–3 are each comparable in size to everything built so far.
Queso today is an excellent, verified *implementation of the algorithm*; turning it into
a *product* is a deliberate second project, which the phased plan already anticipates as
Phases 7–8 (explicitly beyond the committed M0–M5).
