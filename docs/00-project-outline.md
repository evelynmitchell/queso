# Queso — Project Outline

*A ground-up, test-driven, formally-grounded implementation of QuePaxa-style
distributed consensus.*

Status: planning · Last updated: 2026-07-10

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
**failure a normal state of the world**: a good implementation keeps working
even when a minority (and, for safety, even a majority) of components are down.
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
- Pick language/toolchain (see Open Questions). Set up repo layout, CI, lint,
  test runner.
- Build the **simulation kernel**: injectable clock, seeded PRNG, and an
  in-memory network with a pluggable scheduler (FIFO, random, adversarial).
- Reproducibility contract: `(seed) → identical event trace`.
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
- **Properties targeted:** liveness/efficiency without weakening safety; correct
  fallback when an adversary defeats the fast path.
- **Deliverable:** single-round-trip commit in the common case; graceful degrade.

### Phase 4 — Multi-slot SMR log + first application
- Chain slots into a replicated log; a lagging replica may trail but must never
  diverge (prefix consistency).
- Build the **"hello world" application: an in-memory key-value store** driven by
  the log, with linearizable `get`/`put` (reads go through the log as in Meerkat).
- **Properties targeted:** log matching, total order, **linearizability** at the
  KV layer.
- **Deliverable:** a small distributed KV store that passes a linearizability
  checker under faults. *This is the headline "hello world" milestone.*

### Phase 5 — Hedging (replace timeouts)
- Introduce the hedging schedule: leader at delay 0, proposer *k* at delay
  `(k-1)·δ`, each proposing only if it hasn't seen earlier progress.
- **Properties targeted:** liveness preserved for *any* δ (including δ = 0 and
  badly-misconfigured δ); `O(n)` messaging under synchrony.
- **Deliverable:** fast leader-failure recovery with no false-timeout stalls.

### Phase 6 — Auto-tuning (multi-armed bandit)
- Epoch-based leader rotation for exploration; exploit by sorting the hedging
  schedule on observed epoch completion times; keep monitoring the leader.
- **Properties targeted:** convergence to a good leader without harming safety or
  liveness if the estimator is wrong.
- **Deliverable:** system converges to the fastest replica as leader in a
  heterogeneous deployment.

### Phase 7 — Real network & performance
- Swap the sim transport for real TCP (optionally gRPC/Protobuf), keeping the sim
  path for tests. Add **batching** (submitter + proposer) and **pipelining**.
- Optional LAN optimization: agree on batch IDs (hashes), not batch contents.
- **Properties targeted:** throughput/latency comparable to Multi-Paxos under
  normal conditions; sustained liveness under adversarial conditions.
- **Deliverable:** benchmark suite with LAN/WAN numbers and adversarial results.

### Phase 8 — Operability (stretch)
- Reconfiguration (membership change via consensus), log compaction / snapshots,
  metrics/observability, persistence & restart-recovery hardening.
- **Deliverable:** a cluster that can be reconfigured and restarted safely.

### Cross-cutting (every phase)
- Grow the **formal model** (TLA+ and/or Promela/SPIN) alongside the code.
- Expand **deterministic simulation testing (DST)** seeds and adversarial
  schedules.
- Keep the **property-check suite** green as an executable spec.

## 5. Milestones at a glance

| Milestone | Phase | "Definition of done" |
|-----------|-------|----------------------|
| M0 Harness | 0 | Seeded, replayable N-node message sim with adversarial scheduler. |
| M1 One value | 1–2 | Single-slot agreement under crash + async; safety checks pass. |
| M2 Fast path | 3 | One-round-trip common-case commit with correct fallback. |
| **M3 Hello-world KV** | 4 | Linearizable replicated KV store survives fault-injection + Jepsen-style checks. |
| M4 No timeouts | 5 | Hedging gives fast recovery; liveness robust to any δ. |
| M5 Self-tuning | 6 | Converges to best leader automatically. |
| M6 Performance | 7 | Reproducible LAN/WAN throughput & latency; adversarial liveness. |

## 6. Open questions (for the project owner)

These affect scope and effort; defaults are proposed so work can start either way.

1. **Implementation language.** The reference prototype and Meerkat are in
   **Rust/Go**. Rust buys us a path toward formal verification (as Meerkat is
   doing) and strong types; Go matches the paper and is quicker to prototype.
   *Proposed default: Rust* (best fit for "validated, verified, performant" and
   for deterministic simulation), unless you prefer Go for closeness to the paper.
2. **Scope of "hello world."** Is the target milestone **M3** (a linearizable KV
   store that survives faults), or do you want to push through hedging/auto-tuning
   (M4–M5)? *Proposed default: aim for M3 first, treat M4+ as follow-on.*
3. **Formal methods appetite.** Do you want full **TLA+/SPIN** model checking as a
   first-class deliverable, or is **deterministic simulation testing** (à la
   FoundationDB / Meerkat) sufficient as the primary correctness argument?
   *Proposed default: DST as primary, a small SPIN/TLA+ safety model as a
   confidence supplement.*
4. **Non-goals confirmation.** Byzantine fault tolerance, general-purpose
   database features, side-channel resistance, and dynamic membership (until
   Phase 8) are proposed as **out of scope**. Confirm.
5. **Deployment target for benchmarks.** Local multi-process only, or real
   multi-region cloud (the paper used AWS EC2 LAN + WAN)? *Proposed default:
   local sim + local multi-process; cloud WAN optional.*

## 7. References

See [`01-backgrounder.md`](01-backgrounder.md) for the annotated reference list.
Primary sources:

- QuePaxa paper: <https://bford.info/pub/os/quepaxa/quepaxa.pdf>
- Meerkat introduction: <https://blog.cloudflare.com/meerkat-introduction/>
