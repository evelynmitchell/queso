# Queso on Antithesis (Phase 9.3, issue [#54])

The Chain-of-Blocks conformance workload packaged as an [Antithesis] test
template: container images, a topology, and Queso's safety and liveness
properties expressed as Antithesis assertions.

## Why this is the last piece of Phase 9

9.1 built the workload and the divergence/liveness observers and ran them
in-process. 9.2 pointed them at real `queso-node` processes under
socket-level turbulence this repo generates itself, first scripted and then
as a seeded randomized soak.

That soak is randomized but **not autonomous**, and a failing run reproduces
its fault *schedule* and never its interleaving — real thread scheduling and
real TCP see to that. So a bug that shows up once in forty seeds is a genuine
finding and a genuinely awkward one to debug, which is exactly the wall
[#54] said Phase 9 would hit.

Antithesis is the thing on the other side of that wall: a deterministic
hypervisor that owns the scheduler, the clock and the network, explores on
its own, and can replay a failure exactly. This directory is the adapter.

**It injects no faults of its own.** Under Antithesis the platform is the
adversary; a workload adding its own turbulence would only obscure what the
platform found. That is why `queso-antithesis`'s cluster type is strictly
*thinner* than `queso-soak`'s — no spawning, no killing, no proxy mesh, just
load and observation.

## What is here

| | |
|---|---|
| `docker-compose.yaml` | three replicas plus a workload container, one user-defined network, named volumes for durable state |
| `Dockerfile` | the workload image: the `queso-antithesis` binary plus the test commands |
| `test/v1/main/` | the Test Composer commands, baked into the image at `/opt/antithesis/test/v1/main/` |
| [`crates/antithesis`](../crates/antithesis) | the workload itself |

Three commands, and the filename prefixes are what tell Antithesis when each
may run:

- **`first_wait_for_cluster.sh`** — runs before any driver. Waits for every
  replica to answer `/health`, then signals `setup_complete`. Antithesis
  holds off on faults until it sees that signal; without it the platform
  would be partitioning a cluster that had not finished booting, and every
  liveness result for the run would be noise.
- **`parallel_driver_cob_traffic.sh`** — offers Chain-of-Blocks load and
  checks **safety** continuously, under whatever faults are in force.
  `parallel_` rather than `singleton_` so several may run at once, which is
  what makes concurrent submissions to different replicas actually happen.
- **`eventually_quiescent_check.sh`** — runs in the branch Antithesis
  creates with drivers killed and faults stopped, and checks **liveness**.

## The properties

Safety is unconditional and checked every round, under fault:

> **always** — replicas never report different blocks at the same height.

That is Chain-of-Blocks' central property and Queso's P1 Agreement seen from
outside the process. Liveness is asked only in the quiescent branch, because
a partitioned replica is *supposed* to fall behind — P5 permits arbitrary lag
and forbids only divergence:

> **always** — with faults stopped, the cluster keeps deciding.
> **always** — with faults stopped, no replica is left behind and frozen.

Both are needed; neither alone is sufficient. A stall check cannot see a
*uniformly* wedged cluster, because if every replica is stuck at the same
height then none of them is behind the frontier and nothing is reported. A
progress check cannot see one replica left behind while the others carry on.

Three `sometimes` properties guard against the run being vacuous — the
failure mode where a workload silently stops reaching the cluster and reports
a clean bill of health forever:

> **sometimes** — the cluster acknowledges Chain-of-Blocks writes.
> **sometimes** — two replicas are observed at the same height.
> **sometimes** — some submissions fail, so the workload really runs under fault.

The third is worth dwelling on. Locally it is always **false**, because
nothing is injecting faults; under Antithesis it becomes true the first time
the platform partitions something. A run where it stayed false would mean the
safety verdict was earned under easier conditions than advertised. `sometimes`
assertions fail only if never satisfied across the whole run, which is exactly
the right shape for that claim.

## Running it locally

You do not need Docker or an Antithesis account to exercise the workload:

```sh
cargo build --all
cargo test -p queso-antithesis -- --ignored
```

That boots a real three-replica cluster, runs each Test Composer command as
its own process exactly as the platform would, and reads back the assertion
stream the SDK emits — checking not merely that the commands exit zero but
that each property was **reached and evaluated**. An assertion that never
executes is invisible to Antithesis too, so "did it fire" is the same
question there as here.

It includes a negative control: pointed at a cluster that is not running, the
progress and observation properties must both be violated. A liveness check
that cannot fail passes just as happily on a broken cluster.

Replicas get distinct loopback addresses on *identical* ports
(127.0.0.1/.2/.3, all on 7000/7100) rather than distinct ports on one
address, so the test exercises the same addressing shape the container
topology uses instead of a local-only special case.

To bring the topology up under plain Docker:

```sh
docker build -f deploy/Dockerfile     -t queso/queso-node:latest .
docker build -f antithesis/Dockerfile -t queso/queso-antithesis:latest .
docker compose -f antithesis/docker-compose.yaml up -d
docker compose -f antithesis/docker-compose.yaml exec workload \
    /opt/antithesis/test/v1/main/first_wait_for_cluster.sh
```

`QUESO_REGISTRY` and `QUESO_TAG` override the image coordinates, so the same
compose file works locally and against Antithesis's registry.

## What is verified here, and what is not

This matters more than usual: the whole premise of Phase 9 is that "argued"
and "exercised" are different claims, and it would be a poor place to blur
the line.

**Verified**, by `cargo test -p queso-antithesis -- --ignored` on every CI
run of the soak job:

- the workload drives a real cluster and observes it,
- every assertion is reached and evaluates as expected,
- `setup_complete` is signalled,
- the liveness properties can fail.

**Not verified in this repo:**

- **The container build and the registry push.** No Docker daemon was
  available in the environment this was written in, so the images have never
  been built. The compose file parses (`docker compose config`) and the
  Dockerfile follows `deploy/Dockerfile`, which does build — but that is
  reasoning, not evidence.
- **Antithesis's own conventions as of today.** `antithesis.com` was not
  reachable from that environment either, so the Test Composer layout and
  prefixes here come from Antithesis's published reference material rather
  than from a live reading of their docs. Before the first run, check the
  current [setup guide][Antithesis] for the config-image mechanism, image
  registry requirements, and any constraints on the compose file — those are
  the details most likely to have moved.
- **The run itself**, which needs the owner's account, exactly as [#54]
  scopes it.

## Honest limits of the properties themselves

- **The observer sees hashes, not commands.** A cross-process source gets
  `(n, h)` from `GET /chain` and never the commands behind them, so the
  observer's per-transition log is empty here and a divergence report names
  the replicas and the height but cannot show the diverging operations. That
  is a property of the observation channel, not of this packaging.
- **Sampling is checkpoint-dense, not slot-dense.** Replicas publish at a
  fixed spacing (`--chain-checkpoints`), which is what makes cross-replica
  comparison possible at all — 9.1 measured frontier-only sampling collapsing
  20 comparisons to 2 on the same run. Divergence at a height between
  checkpoints that is repaired before the next one would leave no trace.
  Queso has no such repair path (a decided slot is immutable), so this is
  theoretical — but it is a gap.
- **Three replicas, one fixed leader.** The topology tests `f = 1`. A larger
  cluster and a leaderless configuration are both a compose-file edit away
  and neither has been run.

[Antithesis]: https://antithesis.com/docs/
[#54]: https://github.com/evelynmitchell/queso/issues/54
