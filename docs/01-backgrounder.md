# Backgrounder: The Distributed Consensus Problem Space

*A white-paper backgrounder for the Queso project.*

## 1. The problem

Modern infrastructure spreads state across many machines, often across the globe.
Cloudflare, for example, runs internal services that read and modify the same
**control-plane state** from 330+ data centers. Two things must hold at once:

- **No reader ever sees inconsistent state** — every reader observes all prior
  writes (strong consistency), and
- **The system stays available for writes** even when some machines or links fail.

The Internet makes this hard. Servers crash, queues fill, links get cut, latency
spikes and jitters, connectivity goes asymmetric. Under these conditions it is
difficult to keep geographically-distributed replicas synchronized while still
guaranteeing that "all readers read all prior writes."

**Consensus** is the tool that squares this circle. A consensus algorithm lets a
set of machines agree on the same *sequence* of values (e.g., key-value `put`/`get`
operations) as long as a majority remain alive and able to communicate. It turns
**failure into a normal, expected state** rather than an exceptional one: the
system continues to function correctly even when a minority of components are
unavailable — and remains *safe* even when a majority are.

Consensus provides exactly one property — **consistency** — but it is a
foundational one. Once you can rely on all nodes sharing the same view of
reality, you can build higher-order guarantees (data integrity, compute
availability, resource visibility) on top. Consensus lives in
machine-to-machine systems, below the UI/UX layer.

## 2. Consistency, precisely

A system's **consistency level** describes what "weird" behaviors it may exhibit
under concurrent reads and writes. Consider a store holding `x = 6` and two
writes submitted to different nodes in any order:

1. `x = x + 1`
2. `x = x / 2`

- Under **weak consistency**, writes may be reordered — different readers may
  disagree on the final value.
- Under a **stronger** model, writes are not reordered but reads may be.
- Under the **strongest** model, **linearizability**, operations appear to take
  effect in exactly the real-time order they occurred: *every read after a write
  sees that write.*

Linearizability is what most control-plane services want because it lets
programmers reason about a distributed store the way they reason about local
memory on a single thread — no menagerie of anomalies to keep in mind. (Meerkat's
KV store additionally offers **serializability**, per the blog post.)

## 3. Fault tolerance, precisely

A system's **fault-tolerance level** describes which faults it survives before a
*catastrophe* — a violation of a property it promised (e.g., two reads with no
intervening write returning different values, or the system refusing writes).

Target fault model for a Cloudflare-style consensus service:

- **Availability.** The system stays available for reads and writes from a client
  in any data center as long as (a) a **majority** of machines are alive and can
  communicate — formally, *f* faults are tolerated in a system of **2f + 1**
  machines — and (b) the client can reach some machine connected to that
  majority. A single failed machine or a single degraded link must not reduce
  availability.
- **Correctness.** The system stays correct (no two up-to-date machines ever
  disagree about the world) as long as no actor is **actively malicious** and
  there are no bugs. Faults covered: machine crashes, restarts, network
  failure/degradation, whole data-center outages.
- **Explicitly not covered:** **Byzantine** (malicious) faults.

## 4. Why the usual answer (leader + timeouts) hurts

The most-deployed consensus/SMR protocols — **Multi-Paxos** and **Raft** — elect
a single **leader** that alone drives writes, because simultaneously-active
proposers **destructively interfere** (each preempts the other's ballot, ad
infinitum). Leaders make these protocols understandable and make leader-served
reads cheaply linearizable, but they impose the **"tyranny of timeouts"**:

1. **Liveness depends on timeouts.** If the leader crashes or is partitioned, the
   system blocks all writes until some replica times out and a new leader is
   elected. These protocols are only live under **partial synchrony** (delays
   eventually small and stable enough for a leader to make progress).
2. **Timeouts must be conservatively large.** Because simultaneous leaders
   interfere and view changes are costly, timeouts are set well above the network
   delay to avoid false triggers. Too short → replicas constantly time out and
   livelock; too long → slow reaction to a genuinely failed leader. In a WAN with
   wildly varying latency, there is no good setting.
3. **Timeouts are an operational hazard.** They require manual tuning; a
   misconfiguration causes poor performance or a full outage. Static timeouts
   also can't adapt — a leader that is *slow but not slow enough to trip the
   timeout* keeps the whole system slow.

A network **adversary** sharpens the point: by focusing a denial-of-service
attack on whichever replica is currently leader (detectable via traffic
analysis), an attacker can halt progress indefinitely while only ever attacking
one machine at a time. Cloudflare reports multiple real incidents caused by
unavailable leaders in Raft-based systems.

**Asynchronous** consensus protocols avoid timeouts entirely and stay live under
arbitrary network conditions, but historically pay an `O(n²)` per-decision
communication cost versus `O(n)` for leader-based protocols, so they are rarely
deployed. The ideal is a protocol with the **normal-case efficiency of
leader-based** protocols *and* the **worst-case robustness of asynchronous** ones.

## 5. QuePaxa's answer

**QuePaxa** (Tennage, Băsescu, Syta, Jovanovic, Ford, Kokoris-Kogias,
Estrada-Galiñanes; SOSP '23) is the first protocol to offer state-of-the-art
normal-case efficiency *without depending on timeouts for liveness*. Key ideas:

- **Randomized asynchronous core.** Each proposer attaches a random priority to
  its proposal, circumventing the FLP impossibility result and guaranteeing
  termination with probability 1 in a small constant number of rounds. Each round
  independently decides with probability ≥ 1/2. Liveness holds even under a
  **content-oblivious** network adversary (one that can delay/reorder packets but
  not read their contents — satisfiable in practice with TLS).
- **Leaderless rounds cooperate instead of colliding.** Multiple proposers may be
  active in any step **without destructive interference**; they even help each
  other finish a round faster. In phase 0 proposers act as a "coin flip"
  (attaching random priority); in later phases they act as "information mules"
  spreading proposals. It doesn't matter whether one proposer or several do this.
- **A one-round-trip fast path.** A designated **leader** attaches the reserved
  highest priority `H`. If its proposal reaches a quorum first in phase 0, it
  commits in a **single round-trip** — matching Multi-Paxos/Raft. The leader is
  used only in a slot's first round; if it fails, the slot falls back to robust
  leaderless asynchronous rounds. The leader is *"first among equals"*: an
  optimization, never a requirement for progress.
- **Hedging instead of timeouts.** Rather than a timeout that retroactively
  detects failure and triggers a disruptive view change, QuePaxa uses a
  **hedging schedule**: the leader proposes at delay 0, the next proposer at
  delay δ, the next at 2δ, and so on — each proposing only if it hasn't yet seen
  earlier progress. Hedging is *proactive* and *non-interfering*: a delay of 0 is
  valid, and even a badly-misconfigured δ never breaks liveness — the only cost
  of too-small δ is redundant effort. This yields `O(n)` messaging under
  synchrony and **fast recovery** after leader failure.
- **Auto-tuning via multi-armed bandits.** Because leader choice and hedging
  delays affect only *performance*, not safety or liveness, QuePaxa treats them
  as a multi-armed-bandit problem: it explores (round-robin leader rotation over
  epochs — over the first `2n+1` epochs each replica leads twice), then exploits
  (orders the hedging schedule by replicas' observed average epoch completion
  times, sorted descending per §5.3), and keeps monitoring so it can switch to a
  better leader **even if the current one hasn't failed**.

### Structure of the protocol (how the pieces fit)

- **Replicas play two roles.** An active **proposer** drives consensus; a passive
  **recorder** stores state and answers RPCs. Proposers never talk to proposers,
  recorders never to recorders (analogous to Disk Paxos).
- **Slots and steps.** State machine replication proceeds as a totally-ordered
  series of **slots**. Each slot is decided over one or more **rounds** of four
  **phases** (0–3). A **threshold logical clock** counts steps as
  `step = 4·round + phase`, advancing only when a threshold of communication
  completes — no wall-clock or synchronized clocks required.
- **Interval Summary Register (ISR).** The recorder's entire job distills to a
  tiny primitive: record a value at a logical step and return a concise summary
  (first value this step + aggregate of the prior step). With integer-max
  aggregation, an ISR needs only **constant space**.
- **Abstract → concrete.** The abstract algorithm (Algorithm 1) runs three
  `tcast` (threshold synchronous broadcast) operations per round over the
  existent/common/universal set relationship `U ⊆ C ⊆ E`. The concrete protocol
  simulates this over an asynchronous network using ISRs and logical clocks, in
  four pipelined phases, transmitting only constant-space integer summaries.

## 6. Meerkat: QuePaxa in industry

**Meerkat** is Cloudflare Research's experimental global consensus *service*
built on QuePaxa — to their knowledge the first industrial deployment of QuePaxa
at global scale. Salient points for us:

- Developers request a cluster of fully-connected **replicas**; each participates
  in consensus and can accept reads and writes. A client sends an
  application-specific request (e.g., KV `get`/`put`) to *any* replica.
- Under the hood each request becomes a **log event** distributed to all replicas
  so every replica maintains the **same log** (a replica may lag but never
  records different entries). Applications (e.g., a KV store) build state by
  applying the log in order.
- The log is a sequence of **slots**; every slot but the last is *decided*, and
  QuePaxa's invariant is that **no two replicas ever decide different values for
  a slot**. Reads (`get`) also go through the log to guarantee linearizability: a
  replica that missed a decision is forced to catch up before its read commits,
  linearizing the read after prior writes.
- **Availability advantage over Raft:** because no leader is required, a single
  down/slow/attacked replica never blocks writes; clients write through any
  healthy replica anywhere. Proofs-of-concept ran up to **50 replicas worldwide**
  with leaders *constantly failing* and no increase in error rate.
- **Cost:** consensus means round-trips. QuePaxa takes 1 round-trip on the leader
  fast path (plus an extra broadcast to notify replicas of the decision), ~3 for a
  non-leader proposer (plus the extra broadcast), more under contention; decision
  latency is fundamentally bounded by the latency to a majority of replicas.
  Mitigations: co-locate replicas, batch writes, allow stale-but-consistent local
  reads, and bundle operations (e.g., compare-and-swap / transactions). This
  makes Meerkat well-suited to **control-plane state that is written infrequently
  but must stay consistent**, not to general-purpose databases.
- Cloudflare's stated future work includes **formal verification of their Rust
  implementation** and **deterministic simulation testing** to find bugs —
  directly informing Queso's testing plan.

## 7. Related protocols (for orientation)

- **Paxos / Multi-Paxos** — the classic leader-based consensus; destructive
  interference between proposers motivates a single leader and view changes.
- **Raft** — leader-based, viewstamped-replication lineage; the most-implemented
  algorithm; understandable but timeout-bound.
- **EPaxos** — leaderless/multi-leader, partitions commands by dependency for
  throughput; complementary (throughput-focused) rather than robustness-focused.
- **Rabia** — randomized crash-fault-tolerant SMR built on Ben-Or's binary
  asynchronous consensus; specialized to low-latency data-center networks.
- **etcd (Raft)** — the widely-used Raft implementation backing Kubernetes; a
  practical reference point for "consensus as a product."
- **Ben-Or / FLP** — foundational: FLP proves deterministic asynchronous
  consensus is impossible; randomization (Ben-Or, QuePaxa) circumvents it.

## 8. References

- P. Tennage, C. Băsescu, E. Syta, P. Jovanovic, B. Ford, L. Kokoris-Kogias,
  V. Estrada-Galiñanes. **"QuePaxa: Escaping the Tyranny of Timeouts in
  Consensus."** SOSP '23. <https://bford.info/pub/os/quepaxa/quepaxa.pdf>
- J. Larisch, B. Halley, J. P. Leite. **"Introducing Meerkat: an experiment in
  global consensus."** Cloudflare Blog, 2026.
  <https://blog.cloudflare.com/meerkat-introduction/>
- L. Lamport. **"Paxos Made Simple."** (Multi-Paxos background.)
- D. Ongaro, J. Ousterhout. **"In Search of an Understandable Consensus Algorithm
  (Raft)."** USENIX ATC '14.
- I. Moraru, D. Andersen, M. Kaminsky. **"There Is More Consensus in Egalitarian
  Parliaments (EPaxos)."** SOSP '13.
- H. Pan et al. **"Rabia: Simplifying State-Machine Replication Through
  Randomization."** SOSP '21.
- M. J. Fischer, N. A. Lynch, M. S. Paterson. **"Impossibility of Distributed
  Consensus with One Faulty Process (FLP)."** JACM 1985.
- M. Ben-Or. **"Another Advantage of Free Choice: Completely Asynchronous
  Agreement Protocols."** PODC 1983.
- etcd / Kubernetes — Raft in practice. <https://etcd.io/>
- Marc Brooker — on the dangers of weak consistency (referenced by the Meerkat
  post). <https://brooker.co.za/blog/>
