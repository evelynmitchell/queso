# Queso — Project Outline

*A ground-up, test-driven, formally-grounded implementation of QuePaxa-style
distributed consensus.*

Status: planning · Last updated: 2026-07-10 · Owner decisions folded in;
revised after an independent adversarial review against the source papers.

---

## 1. What this project is

Queso builds a **"hello world" distributed consensus system** on the **QuePaxa**
algorithm (Tennage, Băsescu, et al., SOSP '23), the same algorithm Cloudflare is
productionizing in its **Meerkat** service. The goal is not to ship a database;
it is to *understand consensus from first principles* by building it up in
layers, each layer accompanied by:

1. an executable **test harness**,
2. an explicit **model of desired / undesired properties**, and
3. an **implementation** that is progressively made robust, validated, verified,
   and performant.

Consensus lets a set of machines agree on the same "view of reality" even when
some machines or the links between them are unreliable or unavailable. It makes
**failure a normal state of the world**: a good implementation keeps serving
reads and writes while a *minority* is down, and — critically — stays **safe
(never divergent)** even when a *majority* is down, though it stops making
progress until a majority returns.
The value it provides is a single property — **consistency** — on which other
system properties (data integrity, availability, visibility) can be built. The
use case is machine-to-machine infrastructure below the UI/UX layer.

This repository currently contains:

- `README.md` — the project brief.
- `quepaxa.pdf` — the QuePaxa paper (SOSP '23).
- `Introducing Meerkat_ an experiment in global consensus.pdf` — Cloudflare's
  Meerkat announcement.
- `docs/` — the planning artifacts described below.

## 2. Companion documents

| Doc | Purpose |
|-----|---------|
| [`01-backgrounder.md`](01-backgrounder.md) | White-paper backgrounder: the consensus problem space, why timeouts hurt, how QuePaxa/Meerkat differ, with references. |
| [`02-properties.md`](02-properties.md) | The property model: invariants that **must** hold, properties that are **desirable**, and explicitly **non-desired** / out-of-scope behaviors. |
| [`03-testing-plan.md`](03-testing-plan.md) | How we validate and verify: harness design, property tests, formal model checking, deterministic simulation, fault injection, and performance benchmarking. |

## 3. Guiding principles

- **Layered construction.** QuePaxa itself is defined in layers (abstract
  protocol over `tcast` → concrete protocol over interval-summary registers →
  SMR → hedging → auto-tuning). We mirror that layering so each layer is
  independently testable.
- **Properties before code.** Every layer starts by writing down the invariants
  it must preserve, expressed as executable checks, *before* the implementation.
- **Deterministic by construction.** The network, clock, and randomness are
  injectable. A single seed reproduces any run exactly — this is the backbone of
  both debugging and the fuzzing/DST strategy.
- **Adversarial from day one.** The network abstraction supports a
  message-scheduling adversary (reorder, delay, drop, partition, DoS-the-leader).
  Robustness is tested, not assumed.
- **Safety is non-negotiable; liveness is best-effort.** We never trade an
  agreement violation for progress. Under asynchrony we accept that a round may
  need to retry; we require that it *eventually* decides with probability 1.

## 4. Roadmap

Phases are ordered so that something demonstrable exists early and each phase has
a clear "done" bar tied to properties (see `02-properties.md`) and tests (see
`03-testing-plan.md`).

### Phase 0 — Scaffolding & harness (the ground)
- **Rust** toolchain (see Decisions). Set up repo layout (Cargo workspace), CI,
  `clippy`/`rustfmt`, and the test runner.
- Build the **simulation kernel**: injectable clock, seeded PRNG, and an
  in-memory network with a pluggable scheduler. Two adversary classes are
  distinguished from the start (they test different guarantees — see
  `03-testing-plan.md §1`): a **content-oblivious** scheduler (delay/reorder only,
  cannot read message contents) and a **content-aware** scheduler (may target
  specific messages, e.g., `E`-but-not-`U` delivery).
- Reproducibility contract: `(seed) → identical event trace`. Confront Rust's
  nondeterminism sources up front — `HashMap`/`HashSet` iteration order, async
  executor scheduling, thread interleaving — via a single-threaded deterministic
  executor and/or a sim framework (e.g., `madsim`/`turmoil`) and
  deterministic-ordered collections. Without this the whole DST pillar is unsound.
- **Deliverable:** a harness that can spin up *N* nodes, deliver messages under a
  chosen schedule, and record a replayable trace. No consensus yet.

### Phase 1 — Abstract single-slot consensus (Algorithm 1)
- Implement `tcast` (threshold synchronous broadcast) over the sim network.
- Implement the abstract QuePaxa core: prioritized proposals, the
  existent/common/universal (`E`/`C`/`U`) sets, `best()`, decision detection.
- **Properties targeted:** validity, integrity, agreement for one slot.
- **Deliverable:** *N* nodes agree on a single value under crashes and reordering;
  property checks pass; randomized termination observed empirically.

### Phase 2 — Concrete consensus (ISR + 4 phases)
- Replace the idealized `tcast` with the real construction: separate
  active **proposer** / passive **recorder** roles, threshold logical clocks
  (`step = 4·round + phase`), and the **interval summary register (ISR)** with
  integer-max aggregation and constant-space state.
- **Properties targeted:** same safety as Phase 1 under full asynchrony
  (no lock-step assumption).
- **Deliverable:** crash-tolerant single-slot consensus over an async network.

### Phase 3 — Leader fast path
- Add the designated-leader high-priority (`H`) proposal so the leader can commit
  in **one round-trip** in phase 0; fall back to leaderless rounds ≥ 2 on failure.
- **Proposer activation, Phases 3–4:** run with *unconditional* (δ = 0) activation
  — every proposer participates in every round. This is what makes leaderless
  fallback (and therefore leader-crash progress) work *before* hedging exists.
  Phase 5 does not introduce activation; it only adds *delays* to it for
  efficiency. (This resolves the otherwise-fatal gap where nothing would trigger
  backup proposers on leader failure at M2/M3.)
- **Properties targeted:** liveness/efficiency without weakening safety; correct
  fallback when a content-aware adversary defeats the fast path (safety +
  round-1 fallback asserted, not unconditional eventual decision).
- **Deliverable:** single-round-trip commit in the common case; graceful degrade.

### Phase 4 — Multi-slot SMR log + first application
- Chain slots into a replicated log; a lagging replica may trail but must never
  diverge (prefix consistency).
- Build the **"hello world" application: an in-memory key-value store** driven by
  the log, with linearizable `get`/`put`.
- **Linearizable reads, concretely:** a `get` is proposed as its own log event; if
  the slot it targets was already decided by another write, the reader is forced
  to catch up (adopt that decision) and re-propose the read at the next free slot,
  linearizing the read *after* the write. Design and document this mechanism here,
  not just as a property. (Meerkat: a lagging replica is "force[d] … to decide …
  and to propose `get k1` for slot 4".)
- **Durability & crash-recovery (design item, not deferred):** define exactly which
  recorder state (ISR `S, F_c, A_c, A_p` + slot/step, decision flags) is persisted
  and the *write-before-reply* ordering, and whether a restarted replica recovers
  durable state or rejoins as a learner and catches up before participating. The
  base QuePaxa algorithm is crash-*stop*; the must-hold restart-safety properties
  (P9/P12) follow Meerkat's stronger fault set and need this design to be real.
- **Idempotency:** client commands carry `(client-id, seq)` so retries are
  deduplicated; without this, linearizability (P8) does not survive client retries.
- **Properties targeted:** log matching, total order, **linearizability** (P8),
  no-lost-write (P9), restart safety (P12).
- **Deliverable:** a small distributed KV store that passes a linearizability
  checker under faults *including crash/restart*. *This is the headline
  "hello world" milestone.*

### Phase 5 — Hedging (replace timeouts)
- *Add delays* to the always-on activation from Phase 3: leader at delay 0,
  proposer *k* at delay `(k-1)·δ`, each proposing only if it hasn't by then seen
  earlier progress. This turns the `O(n²)` all-proposers-active worst case into
  `O(n)` under synchrony without changing what guarantees liveness.
- **Properties targeted:** liveness preserved for *any* δ (including δ = 0 and
  badly-misconfigured δ) (P15); `O(n)` messaging under synchrony (D2).
- **Deliverable:** fast leader-failure recovery with no false-timeout stalls.

### Phase 6 — Auto-tuning (multi-armed bandit)
- Epoch-based leader rotation for exploration: over the first `2n+1` epochs each
  replica leads twice. Then exploit by agreeing on a hedging schedule with
  replicas sorted in *descending* order of observed average epoch completion time
  (per the paper's §5.3); keep monitoring the leader and re-explore only if it
  falls behind the next in the schedule.
- **Properties targeted:** convergence to a good leader without harming safety or
  liveness if the estimator is wrong.
- **Deliverable:** system converges to the fastest replica as leader in a
  heterogeneous deployment.

### Phase 7 — Real network & performance
- Swap the sim transport for real TCP (optionally gRPC/Protobuf), keeping the sim
  path for tests. Add **batching** (submitter + proposer) and **pipelining**.
- **Encrypt inter-replica links (TLS).** This is the concrete realization of the
  content-oblivious-adversary assumption (A3) that the randomized-liveness
  guarantees (P14/P15) depend on — without it, a real content-aware network
  adversary voids the ≥ 1/2-per-round bound.
- Optional LAN optimization: agree on batch IDs (hashes), not batch contents.
- **Properties targeted:** throughput/latency comparable to Multi-Paxos under
  normal conditions; sustained liveness under adversarial conditions.
- **Deliverable:** benchmark suite with LAN/WAN numbers and adversarial results.

### Phase 8 — Operability (stretch)
- Reconfiguration (membership change via consensus), log compaction / snapshots,
  metrics/observability, and *production hardening* of the durability &
  restart-recovery machinery. (Note: the durability *design* and basic
  crash/restart safety land earlier, in Phase 4 — see P12; Phase 8 is about
  hardening and operating it, not introducing it.)
- **Deliverable:** a cluster that can be reconfigured, compacted, and operated.

### Cross-cutting (every phase)
- Grow the **formal model** (TLA+ and/or Promela/SPIN) alongside the code —
  this is a **first-class deliverable**, not an afterthought (see Decisions).
- Expand **deterministic simulation testing (DST)** seeds and adversarial
  schedules.
- Keep the **property-check suite** green as an executable spec.

## 5. Milestones at a glance

| Milestone | Phase | "Definition of done" |
|-----------|-------|----------------------|
| M0 Harness | 0 | Seeded, replayable N-node message sim with adversarial scheduler. |
| M1 One value | 1–2 | Single-slot agreement under crash + async; safety checks pass. |
| M2 Fast path | 3 | One-round-trip common-case commit with correct fallback. |
| M3 Hello-world KV | 4 | Linearizable replicated KV store survives fault-injection (incl. crash/restart) + Jepsen-style checks; durability & idempotency designed. |
| **M4 No timeouts** | 5 | Hedging gives fast recovery; liveness robust to any δ. |
| **M5 Self-tuning** | 6 | Converges to best leader automatically. |
| M6 Performance | 7 | Reproducible LAN/WAN throughput & latency; adversarial liveness. |

**Committed scope: through M4–M5.** M0–M3 build the fault-tolerant linearizable
KV store; M4 (hedging / timeout-free recovery) and M5 (auto-tuning) are in scope
and complete the QuePaxa story. M6 (performance) and Phase 8 (operability) are
follow-on.

## 6. Decisions

Resolved with the project owner (2026-07-10):

1. **Implementation language: Rust.** Chosen for the "validated, verified,
   performant" goals — strong types, a clean path toward implementation-level
   formal verification (as Meerkat is pursuing), and good fit for deterministic
   simulation. Repo is a Cargo workspace.
2. **Scope of "hello world": through M4–M5.** We build the fault-tolerant
   linearizable KV store (M3) and continue through **hedging / timeout-free
   recovery (M4)** and **auto-tuning (M5)**. M6 performance and Phase 8
   operability are follow-on.
3. **Formal methods: first-class deliverable.** TLA+ and/or Promela/SPIN safety
   models are a primary, tracked deliverable alongside deterministic simulation
   testing — not merely a confidence supplement. DST remains the workhorse for
   the real implementation; the formal model is developed and maintained in
   parallel from Phase 1.
4. **Non-goals: confirmed out of scope.** Byzantine fault tolerance,
   general-purpose database features, side-channel / traffic-analysis resistance,
   and dynamic membership (deferred to the Phase 8 stretch) are out of scope.

### Still open (non-blocking)

- **Benchmark deployment target.** Local multi-process is the default; real
  multi-region cloud (the paper used AWS EC2 LAN + WAN) is optional and can be
  decided when Phase 7 approaches.

## 7. References

See [`01-backgrounder.md`](01-backgrounder.md) for the annotated reference list.
Primary sources:

- QuePaxa paper: <https://bford.info/pub/os/quepaxa/quepaxa.pdf>
- Meerkat introduction: <https://blog.cloudflare.com/meerkat-introduction/>
