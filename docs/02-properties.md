# Property Model: Must-Hold, Desirable, and Non-Desired

*The executable specification for Queso. Every property here should map to one or
more checks in the test harness (see [`03-testing-plan.md`](03-testing-plan.md)).*

Notation: a cluster has **n = 2f + 1** replicas and tolerates **f** crash faults.
A **slot** is one position in the replicated log; a **decision** assigns a value
to a slot. Priorities are drawn with high entropy so ties are negligible.

---

## A. System model & assumptions (the ground rules)

These are assumed true; violating them voids the guarantees below.

- **A1 — Crash-stop, non-Byzantine.** Replicas fail only by going silent
  (crash/partition); they never send incorrect or malicious messages. Correctness
  is *not* promised if any actor is actively malicious.
- **A2 — Eventual delivery.** Any message between two correct replicas is
  eventually delivered (satisfied in practice over TCP). The network may delay,
  reorder, or transiently drop; it may not corrupt or fabricate.
- **A3 — Content-oblivious adversary.** The network scheduler may arbitrarily
  delay and reorder packets but cannot observe message contents or replica memory
  (satisfied by encrypting inter-replica links, e.g., TLS). Randomized-liveness
  guarantees (P14/P15) depend on this and are asserted **only** under a
  content-oblivious schedule. A *content-aware* adversary (one that can, e.g.,
  deliver a leader's proposal to all `E` sets but no `U` set) can defeat any single
  leader-based round; against it we assert *safety* and *fallback to a leaderless
  round*, not unconditional eventual decision. Tests must use the matching
  adversary class (see `03-testing-plan.md §1`).
- **A4 — Static, known membership.** The replica set is well-known and fixed
  (reconfiguration is a Phase-8 stretch goal, done via consensus).
- **A5 — No synchronized clocks.** The protocol relies on threshold *logical*
  clocks, not wall-clock time, for safety or ordering.
- **A6 — Client sessions.** A client is identified by a stable `client-id` and
  tags each command with a monotonic `seq`, enabling deduplication of retries
  (see P8a) and per-session monotonicity guarantees (see D7). A "session" is the
  scope over which a client's `seq` is monotonic.
- **A7 — Bounded step/slot numbers.** Step numbers are bounded (≈10 bits suffices,
  since the chance of a slot staying undecided beyond ~256 rounds is
  cryptographically negligible) and slot numbers are fixed-size, reset at
  reconfiguration before overflow. Implementations must force a reconfiguration
  (or fail closed) rather than wrap.

---

## B. MUST hold — safety invariants (never violate)

A violation here is a **catastrophe**. These must hold under *any* schedule,
*any* number of crashes/restarts, and *full asynchrony* — safety never depends on
timing.

### Consensus (per slot)
- **P1 — Agreement.** No two replicas decide different values for the same slot.
- **P2 — Validity.** A decided value was proposed by some replica (it is not
  invented by the protocol).
- **P3 — Integrity / decide-once.** A replica decides at most one value per slot
  and delivers each decision at most once.
- **P4 — Stability.** Once a replica decides a slot, that decision never changes.

### Replicated log (across slots)
- **P5 — Log matching / prefix consistency.** If two replicas have a decided
  value at a slot, it is the same value; a replica may **lag** (trail the log)
  but must **never diverge** (record a different entry). No "split brain."
- **P6 — Total order.** All replicas apply decided slots in the same order.
- **P7 — Gap-free application.** A replica applies slot *k* to application state
  only after applying all slots `< k` (it may fetch missing decisions first).

### Application layer (KV "hello world")
- **P8 — Linearizability.** Operations appear to take effect atomically at some
  point between their invocation and response, consistent with real-time order.
  Concretely: a `get` after a completed `put` on the same key returns that `put`'s
  value (or a later one), regardless of which replica served either request.
- **P8a — Idempotent commands.** Applying a command more than once (e.g., a client
  retry after an uncertain ack) has the same effect as applying it once. Enforced
  via `(client-id, seq)` deduplication (A6). Without this, retries can violate P8.
- **P9 — No lost committed writes.** A write acknowledged to a client is never
  lost while a majority survives, even across crashes and restarts.
- **P10 — Read safety under lag.** A replica that missed a decision cannot serve
  a stale *linearizable* read; it must catch up (or fail the read) rather than
  return an inconsistent value.

### Fault-tolerance envelope
- **P11 — Safety under ≤ any number of crashes.** P1–P10 hold even if **more than
  f** replicas crash. Excess crashes may cost *liveness* (see C) but must never
  cost *safety*.
- **P12 — Restart safety.** A replica that crashes and restarts (losing volatile
  state up to its durable state) never violates P1–P10. Durable state is
  persisted before it is acted upon where required.
  > **Design dependency.** The base QuePaxa algorithm is crash-*stop* (a failed
  > replica "goes silent forever"); P9/P12 adopt Meerkat's stronger
  > crash-*recovery* fault set and therefore require an explicit durability design
  > — which recorder state is persisted (ISR `S, F_c, A_c, A_p`, slot/step,
  > decision flags) and the *write-before-reply* ordering — plus a rejoin policy
  > (recover durable state, or rejoin as a learner and catch up). This is a
  > **Phase-4 design item**, not a Phase-8 stretch (see roadmap and §F).

---

## C. MUST hold — liveness (progress guarantees)

Liveness is conditional on the fault envelope and is *best-effort under
asynchrony* — but the following must hold.

- **P13 — Majority progress.** If a majority (≥ f + 1) of replicas are alive and
  can communicate, and a client can reach one of them, then submitted commands
  are eventually decided.
- **P14 — Randomized termination.** Under full asynchrony (no timing assumptions)
  and a content-oblivious adversary (A3), every **leaderless** round (round ≥ 2)
  decides with probability ≥ 1/2, so every slot terminates with probability 1 in a
  constant expected number of rounds (< 2 rounds in the abstract model). Round 1 is
  leader-based and a content-aware adversary can force it to fail; the ≥ 1/2 bound
  is a property of the leaderless rounds, not of round 1.
- **P15 — Timeout-independent liveness.** Liveness never depends on any timeout
  being correctly configured. There is no configuration of hedging delays
  (including δ = 0 or absurdly large δ) that can cause a livelock or permanent
  stall.
- **P16 — Leader-failure recovery.** If the current leader is slow, crashed, or
  DoS'd, the system still makes progress via leaderless rounds without a
  disruptive, progress-blocking view change.
- **P17 — No destructive interference.** Multiple simultaneously-active proposers
  never block each other's progress; concurrent proposals converge on a single
  decided value.

---

## D. DESIRABLE — quality & performance (optimize, don't compromise safety for)

These improve efficiency/operability. None may be pursued at the expense of B/C.

- **D1 — One-round-trip fast path.** Under normal conditions a designated leader
  commits a slot in a single round-trip (phase 0), matching Multi-Paxos/Raft.
- **D2 — Linear messaging under synchrony.** When network delay < base hedging
  delay δ, only the leader proposes, giving `O(n)` messages per decision.
- **D3 — Adversarial robustness.** Under DoS/asynchrony, throughput degrades
  gracefully rather than stalling as leader-based protocols do. Reference points:
  the **paper** measures QuePaxa sustaining ≥ 75k cmd/s while Multi-Paxos and Raft
  saturate at ~2.5k cmd/s under attack (≈ 30×), with **sub-380ms median WAN
  latency**; the **Meerkat blog** summarizes the advantage as "~10× higher
  throughput." (Cite them separately — the ~10× is the blog's figure, not the
  paper's.)
- **D4 — Auto-tuning.** The system converges to a good leader and hedging schedule
  automatically (multi-armed-bandit explore/exploit), and can switch leaders even
  when the current leader has not failed.
- **D5 — Constant-space recorders.** Recorder state per slot is `O(1)` (integer
  ISR), independent of the number of proposals.
- **D6 — Batching & pipelining.** Submitters and proposers batch commands and
  pipeline rounds for throughput.
- **D7 — Tunable read freshness.** Callers may opt into stale-but-never-
  inconsistent local reads (skipping a consensus round) when linearizability is
  not required. Such reads still preserve **monotonicity within a client session**
  (A6): a session never observes state older than one it has already seen.
- **D8 — Transactions / CAS.** The KV layer supports compare-and-swap and, ideally,
  general transactions bundled into a single consensus round.
- **D9 — Reproducibility.** Any run is exactly replayable from its seed
  (foundational for debugging and testing).
- **D10 — Observability.** Metrics for per-slot rounds, fast-path hit rate,
  proposer activations, recovery time, and per-replica latency.
- **D11 — Reconfiguration.** Membership can change safely via consensus (Phase 8).

---

## E. NON-DESIRED — anti-properties & out-of-scope

### Anti-properties (must be impossible; these are the negations of §B and are
called out explicitly because they are the failure modes we actively hunt for)
- **N1 — Divergence / split brain.** Two replicas with different decided values
  for the same slot. *(Negation of P1/P5.)*
- **N2 — Lost acknowledged write.** A client-acked write that later disappears.
  *(Negation of P9.)*
- **N3 — Stale linearizable read.** A linearizable `get` returning a value older
  than a completed prior `put`. *(Negation of P8/P10.)*
- **N4 — Reordering under linearizability.** Committed operations applied in
  different orders on different replicas. *(Negation of P6.)*
- **N5 — Phantom decision.** Deciding a value no replica proposed. *(Negation of
  P2.)*
- **N6 — Timeout-induced livelock.** Progress permanently blocked because of
  timeout/hedging misconfiguration or dueling proposers. *(Negation of P15/P17.)*

### Out of scope (explicit non-goals for this project)
- **O1 — Byzantine fault tolerance.** No defense against malicious replicas.
- **O2 — General-purpose database.** No SQL, no secondary indexes, no
  general query engine; the KV store is a demonstration application.
- **O3 — Side-channel / traffic-analysis resistance.** Beyond the content-oblivious
  assumption (A3); timing/size side channels are not addressed.
- **O4 — Tolerating loss of a majority for liveness.** With ≤ f alive the system
  may stall (safety is still preserved — see P11).
- **O5 — Cross-cluster / geo-partitioning logic, sharding.** Single cluster,
  single log; multi-Raft-style sharding is not in scope.
- **O6 — WAN-scale production deployment & ops tooling.** Benchmarks may run in a
  WAN, but production hardening (upgrades, quotas, multi-tenancy) is out of scope.

---

## F. Property → phase coverage matrix

| Property | Introduced/verifiable at phase |
|----------|-------------------------------|
| P1–P4 (consensus safety) | Phase 1 (abstract), re-verified Phase 2 (concrete) |
| P11 (safety under >f crashes) | Phase 1 onward (safety invariant, always checked) |
| P13–P14, P17, N6 (randomized liveness, no livelock) | Phase 1–2 (δ=0 activation) |
| D1 (fast path) | Phase 3 |
| P5–P7 (log), P8/P8a/P9/P10 (linearizability, idempotency), N1–N5 | Phase 4 |
| P12 (durability/restart) | **Phase 4** (design item; tested continuously in DST) |
| P15–P16 (timeout-free recovery), D2 (linear msgs), D3 | Phase 5 |
| D4 (auto-tuning) | Phase 6 |
| D5–D8 (space, batching, reads, txns) | Phases 2/4/7 |
| D11 (reconfig) | Phase 8 |
| D9 (reproducibility), D10 (observability) | Phase 0 onward |

*Note: A6 (client sessions), A7 (bounded step/slot), and the durability design
(P12) are prerequisites, not late add-ons — they land with the Phase-4 KV store.*

This table says *when* a property becomes verifiable, not *whether* it has been
verified or how strongly. For that, see
[`conformance-matrix.md`](conformance-matrix.md), which maps each property to
the artifact that verifies it and to the class of evidence that artifact
provides.
