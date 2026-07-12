# Deploying a Queso cluster to fly.io

Phase 7.3 (issue #33): the deployment artifacts that let `queso-node` run as
a real, multi-region fly.io cluster instead of three localhost processes.
See `crates/net/README.md` for how to run a cluster locally by hand first
if you haven't -- everything below assumes you already understand
`--id`/`--listen`/`--client-listen`/`--peer`/`--leader`/`--data-dir` (see
`crates/net/src/bin/queso-node.rs`).

**What this buys you, concretely:** three (or more) `queso-node` replicas,
each its own fly.io app, in different regions, wired together over fly's
private network, with durable state on a real persistent volume, reachable
from the outside on a public TCP port for clients.

**What this does NOT claim:** production-hardened operability. Everything
in `crates/net/README.md`'s "Honest limits" section (whole-snapshot not
incremental WAL, no reconfiguration, no log compaction, TLS opt-in and not
enabled by default) is still true once this is running on fly -- deploying
it doesn't change what the binary itself guarantees; see §12 below for
TLS's status on this specific deployment target. This
document is honest about what is and isn't verified below (see "What this
runbook cannot verify" at the end) -- everything through `fly deploy`
itself is concrete and was checked as far as this environment allows;
actually running `fly deploy` needs the reader's own fly.io account.

## Contents

1. [Architecture: peer discovery via fly's `.internal` DNS](#1-architecture-peer-discovery-via-flys-internal-dns)
2. [Prerequisites](#2-prerequisites)
3. [Build and sanity-check the image locally](#3-build-and-sanity-check-the-image-locally)
4. [Create the fly apps](#4-create-the-fly-apps)
5. [Provision persistent volumes](#5-provision-persistent-volumes)
6. [Review the per-replica peer list](#6-review-the-per-replica-peer-list)
7. [Deploy](#7-deploy)
8. [Verify cluster health](#8-verify-cluster-health)
9. [Run a queso-bench workload against the live cluster](#9-run-a-queso-bench-workload-against-the-live-cluster)
10. [Tear down](#10-tear-down)
11. [Durability notes](#11-durability-notes)
12. [TLS / A3 note](#12-tls--a3-note)
13. [What this runbook cannot verify](#13-what-this-runbook-cannot-verify)

## 1. Architecture: peer discovery via fly's `.internal` DNS

This is the crux of Phase 7.3 -- everything else is ordinary Dockerfile/
fly.toml plumbing.

**The scheme: one fly app per replica.** Deploy each replica (`--id 0`,
`--id 1`, `--id 2`, ...) as its *own*, single-machine fly app named to
match -- `queso-0`, `queso-1`, `queso-2`. Fly gives every app a stable
private DNS name on its 6PN network, `<app-name>.internal`, that resolves
to that app's machine(s) regardless of which physical host they land on or
how many times they restart. That gives every replica a fixed, predictable
dial address for every peer: `queso-0.internal:7000`,
`queso-1.internal:7000`, `queso-2.internal:7000` -- known up front, at
config-authoring time, with no runtime service discovery, no coordination
service, no sidecar. See `deploy/fly.toml`'s header comment for the same
rationale next to the config it justifies.

(A single N-machine fly app was considered and rejected: fly does not give
each machine within one multi-machine app an individually stable,
externally-addressable `.internal` hostname the same way -- one app per
replica is the simpler scheme that actually has a fixed address per
replica.)

**The code change this required.** Before this phase, `queso-node`'s
`--peer id=host:port` flag and `queso_net::config::NodeConfig::peers`
required a literal `SocketAddr` (`ip:port`) -- a hostname like
`queso-1.internal:7000` would fail to parse at startup. This phase adds
hostname support:

- `crates/net/src/config.rs`: `NodeConfig::peers` is now
  `BTreeMap<NodeId, String>` (a `host:port` string) instead of
  `BTreeMap<NodeId, SocketAddr>`.
- `crates/net/src/transport.rs`: adds `resolve_peer_addr(addr: &str)`,
  which parses a literal `ip:port` synchronously (no DNS, the fast path
  every existing local/test cluster still takes) or falls back to async
  DNS via `tokio::net::lookup_host` for a hostname. `spawn_peer_dialer`
  calls it **fresh on every dial attempt** (not once at startup) -- this
  is deliberate, not incidental: fly's private DNS may not have propagated
  yet the instant a freshly-deployed replica's process starts, and the
  address behind a hostname can legitimately change across a peer's
  restart (a new fly machine, a rescheduled container). Resolving once at
  startup would bake in a stale/absent answer; resolving per dial attempt
  means a `queso-node` process never needs to restart just because a
  peer's underlying machine did.
- `crates/net/src/driver.rs`: threads the now-`String` peer address through
  to `spawn_peer_dialer` (`addr.clone()` instead of a `Copy`'d
  `SocketAddr`).
- `crates/net/src/bin/queso-node.rs`: `--peer id=host:port` parsing now
  only validates the `host:port` *shape* (there's a numeric port after the
  last `:`), not that `host` is a literal IP -- resolution happens later,
  per the above.
- Test-only: `crates/net/tests/cluster.rs` and
  `crates/net/tests/support/mod.rs` build `BTreeMap<NodeId, String>`
  instead of `BTreeMap<NodeId, SocketAddr>` (`.to_string()` on the same
  `127.0.0.1:PORT` addresses they always used -- no behavior change for
  local/CI clusters, which still dial literal IPs).

Unit tests for the new resolution path live in `crates/net/src/transport.rs`
(`resolve_peer_addr_accepts_ip_literal_without_dns`,
`resolve_peer_addr_resolves_a_hostname_via_dns`,
`resolve_peer_addr_rejects_an_unresolvable_hostname`) and in
`crates/net/src/bin/queso-node.rs` (`parse_peer_accepts_a_hostname_like_flys_internal_dns`
and friends).

**What did *not* change:** `--listen`/`--client-listen` (still literal
`SocketAddr` -- a replica always binds a concrete local address, never a
hostname) and, importantly, `queso_net::client::submit`/`Client` and
`queso-bench`'s `--addr` flag (still `SocketAddr` only, no hostname
resolution). That is a deliberate scope boundary, not an oversight: issue
#33 is about *peer* discovery (replica-to-replica), and Phase 7.2's client
library already predates this phase. Section 8 below shows how to work
around this when verifying a live fly cluster from outside it.

**Binding for reachability.** `--listen`/`--client-listen` in
`deploy/fly.toml` bind `[::]:PORT` -- all IPv6 interfaces -- not
`127.0.0.1`. Fly machines are reachable over their private 6PN network (and
by fly's own edge/TCP proxy, for the client port) via an IPv6 address on
the machine's own interface, not loopback; `127.0.0.1` would make the
process unreachable from any other machine, including fly's own proxy.

## 2. Prerequisites

- A fly.io account and org (`fly auth login` / `fly auth signup`).
- [`flyctl`](https://fly.io/docs/flyctl/install/) installed locally.
- Docker (for a local build sanity-check -- fly's own remote builder
  doesn't strictly require this, see step 7, but building locally first
  catches mistakes faster).
- This repo, checked out, with `deploy/Dockerfile`, `deploy/fly.toml`,
  `deploy/fly.queso-1.toml`, `deploy/fly.queso-2.toml` present.

All commands below assume your current directory is the **repository
root** (not `deploy/`) -- the Dockerfile's build context is the whole
Cargo workspace (see its header comment), and `flyctl` needs to be pointed
at both the config file (`-c deploy/fly.toml`) and, implicitly, the
current directory as the build context.

## 3. Build and sanity-check the image locally

```sh
docker build -f deploy/Dockerfile -t queso-node .

# Should print --help and exit 0 (the default CMD, see the Dockerfile) --
# proves the image at least starts and the binary runs:
docker run --rm queso-node

# Both binaries should print usage:
docker run --rm --entrypoint /usr/local/bin/queso-node   queso-node --help
docker run --rm --entrypoint /usr/local/bin/queso-bench  queso-node --help
```

If your build environment hits Docker Hub's anonymous pull-rate limit
(common in CI or shared-egress sandboxes), override the base images with
`--build-arg` -- see the Dockerfile's header comment for the exact flags
(a public Docker Hub mirror on `mirror.gcr.io` works well here). This
repo's own CI/sandbox verification of this Dockerfile used exactly that
override; see this document's final section for the full story.

A stronger local check -- actually forming a 3-node cluster from the built
image, entirely on your machine, with no fly account -- exercises the same
hostname-based peer resolution this deployment relies on:

```sh
docker network create quesotest

docker run -d --name q0 --network quesotest queso-node \
  /usr/local/bin/queso-node --id 0 --seed 1 \
  --listen 0.0.0.0:7000 --client-listen 0.0.0.0:8000 \
  --peer 0=q0:7000 --peer 1=q1:7000 --peer 2=q2:7000 --leader 0 --data-dir /data
docker run -d --name q1 --network quesotest queso-node \
  /usr/local/bin/queso-node --id 1 --seed 2 \
  --listen 0.0.0.0:7000 --client-listen 0.0.0.0:8000 \
  --peer 0=q0:7000 --peer 1=q1:7000 --peer 2=q2:7000 --leader 0 --data-dir /data
docker run -d --name q2 --network quesotest queso-node \
  /usr/local/bin/queso-node --id 2 --seed 3 \
  --listen 0.0.0.0:7000 --client-listen 0.0.0.0:8000 \
  --peer 0=q0:7000 --peer 1=q1:7000 --peer 2=q2:7000 --leader 0 --data-dir /data

# `docker logs q0` should show `connected to peer ... peer_addr=q1:7000` --
# i.e. it dialed the *hostname* `q1`, resolved via Docker's embedded DNS,
# the same mechanism fly's `.internal` DNS plays for a real deployment.
docker logs q0

# Clean up:
docker rm -f q0 q1 q2 && docker network rm quesotest
```

## 4. Create the fly apps

One app per replica, named to match `deploy/fly.toml` /
`deploy/fly.queso-1.toml` / `deploy/fly.queso-2.toml`:

```sh
fly apps create queso-0
fly apps create queso-1
fly apps create queso-2
```

(Pick your own app names if `queso-0`/`queso-1`/`queso-2` are taken in
your org -- fly app names are globally unique. If you rename them, update
the `app =` line *and* every `--peer` entry in all three `deploy/fly*.toml`
files to match, since the peer addresses are literally
`<app-name>.internal:7000`.)

## 5. Provision persistent volumes

**Do this before the first deploy.** `crates/net/src/persist.rs`'s fsync'd
durable state (issue #36/#38) lives at `--data-dir` (`/data` in the
container); without a volume mounted there, `/data` is the container's
ephemeral filesystem and every restart/redeploy starts from a blank slate
-- silently reintroducing the lose-an-acknowledged-write-on-restart
behavior `crates/net/README.md`'s "Honest limits" section warns about.

```sh
fly volumes create queso0_data --region iad --size 1 -a queso-0
fly volumes create queso1_data --region lhr --size 1 -a queso-1
fly volumes create queso2_data --region nrt --size 1 -a queso-2
```

(`--size 1` is 1 GB -- this crate's durability is a whole-state snapshot
rewritten on every persisting event, see `crates/net/README.md`'s "Honest
limits," so size this to comfortably exceed your KV store's total decided
state, not your write *volume*. Bump it for anything beyond a demo/smoke
cluster.) The region passed here **must match** each app's
`primary_region` in its `fly.toml` -- a fly volume is pinned to the region
it was created in, and a machine can only mount a volume in its own
region.

## 6. Review the per-replica peer list

`deploy/fly.toml` (replica 0, region `iad`), `deploy/fly.queso-1.toml`
(replica 1, region `lhr`), `deploy/fly.queso-2.toml` (replica 2, region
`nrt`) are a complete, working 3-region example (this is the "multi-region"
deliverable: three replicas, three regions, one WAN cluster). Each
`[processes].app` line is that replica's full `queso-node` command line;
all three list the same `--peer 0=queso-0.internal:7000 --peer
1=queso-1.internal:7000 --peer 2=queso-2.internal:7000` (every replica's
own entry included, harmlessly unused -- see
`crates/net/src/config.rs`'s `NodeConfig::peers` docs) and the same
`--leader 0`.

To change the cluster size or regions: add/remove `deploy/fly.queso-N.toml`
files, update every file's `--peer` list and `--id`/`--seed` to match, and
add/remove the matching `fly apps create`/`fly volumes create` calls
above. Nothing about the peer-discovery scheme is hardcoded to exactly
three replicas.

## 7. Deploy

```sh
fly deploy -c deploy/fly.toml        -a queso-0
fly deploy -c deploy/fly.queso-1.toml -a queso-1
fly deploy -c deploy/fly.queso-2.toml -a queso-2
```

Each `fly deploy` builds the image from `deploy/Dockerfile` (via fly's
remote builder, by default -- no local Docker required at this step,
unlike the local sanity-check in step 3) and starts one machine in that
app's `primary_region`, with the volume from step 5 mounted at `/data`.

Order doesn't matter much -- `queso-node`'s peer dialer retries forever
(`crates/net/src/transport.rs`'s `RECONNECT_DELAY`), so a replica that
comes up before its peers exist yet just keeps retrying (now also
re-resolving DNS on every attempt, see section 1) until they do.

**If `fly deploy` can't find `deploy/Dockerfile`:** flyctl resolves a
`[build].dockerfile` path in `fly.toml` relative to *either* the config
file's own directory or the invocation directory, depending on flyctl
version -- this repo could not pin down which without a real fly account
to test against (see the final section). If the above fails with a
"Dockerfile not found" error, retry from inside `deploy/` with
`dockerfile = "Dockerfile"` (edit the three `fly*.toml` files accordingly),
or pass `--dockerfile deploy/Dockerfile` explicitly on the `fly deploy`
command line.

## 8. Verify cluster health

**Logs.** Each replica logs `listening for peers`/`listening for clients`
on boot and `connected to peer ... peer_addr=queso-N.internal:7000` once
its dialers succeed (see section 1's local Docker-network check for
exactly what this looks like):

```sh
fly logs -a queso-0
```

**Reach the cluster from outside fly's network.** `queso-bench` and
`queso_net::client::submit` take a literal `SocketAddr`, not a hostname
(section 1's scope note) -- so from your own machine, use `fly proxy` to
forward a local port to each replica's client port over fly's WireGuard
tunnel rather than trying to dial `queso-0.internal:8000` directly (that
hostname only resolves *inside* fly's private network):

```sh
fly proxy 8000:8000 -a queso-0 &
fly proxy 8001:8000 -a queso-1 &
fly proxy 8002:8000 -a queso-2 &
```

**Put via one replica, Get from another, with the actual value checked.**
`queso-bench` doesn't print the values it reads/writes (it's a load
generator, not a correctness checker -- see `crates/net/README.md`), so
for an exact value-checked round trip, use the same
`queso_net::client::submit` helper `crates/net/tests/cluster.rs`'s
acceptance test uses, from a tiny throwaway program:

```sh
mkdir -p /tmp/queso-smoke && cd /tmp/queso-smoke
cat > Cargo.toml <<'EOF'
[package]
name = "queso-smoke"
version = "0.1.0"
edition = "2021"

[dependencies]
queso-net = { path = "/ABSOLUTE/PATH/TO/queso/crates/net" }
queso-smr = { path = "/ABSOLUTE/PATH/TO/queso/crates/smr" }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
anyhow = "1"
EOF
mkdir -p src && cat > src/main.rs <<'EOF'
use queso_net::client::submit;
use queso_smr::{ClientId, Command, Outcome};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let replica0 = "127.0.0.1:8000".parse()?; // -> fly proxy -> queso-0
    let replica2 = "127.0.0.1:8002".parse()?; // -> fly proxy -> queso-2

    let put = Command::Put { client: ClientId(9000), seq: 0, key: 42, value: 777 };
    let put_outcome = submit(replica0, &put).await?;
    assert_eq!(put_outcome, Outcome::Put);
    println!("Put via queso-0: {put_outcome:?}");

    let get = Command::Get { client: ClientId(9000), seq: 1, key: 42 };
    let get_outcome = submit(replica2, &get).await?;
    assert_eq!(get_outcome, Outcome::Get(Some(777)));
    println!("Get via queso-2: {get_outcome:?} -- matches the Put, cluster is healthy");
    Ok(())
}
EOF
cargo run --quiet
```

(Replace the two `path = ...` entries with your actual repo checkout path.
This is intentionally *not* checked into the repo -- it is a few lines of
throwaway verification code an operator runs once, not a maintained tool;
adding it as a permanent `crates/net` example/binary was out of scope for
this phase and would touch a crate another concurrent change is also
editing, see this document's final section.)

Expect both `assert_eq!`s to pass and both lines to print -- that is a
real Put decided by (at least) a majority and observed, with the correct
value, from a *different* replica than the one it was submitted to, over
the internet, across two different fly regions.

**Fly health checks (Phase 8.2, issue #47).** `queso-node`'s optional
`--status-listen <addr>` flag (off by default -- see `crates/net/README.md`)
serves `GET /health`/`GET /ready`/`GET /metrics`. A fly
`[[services.http_checks]]` block in `fly.toml` (alongside the
`[[services.tcp_checks]]` blocks already in `deploy/fly*.toml`) can point
its liveness probe at `/health` and its readiness probe at `/ready` once
that flag is added to each app's start command -- `/ready`'s precise,
honest meaning ("not currently known to be running its own restart
catch-up probe", not a linearizable-read guarantee) is documented in
`crates/net/README.md` and `crates/net/src/status.rs`; read that before
wiring a fly check to it. This runbook's `deploy/fly*.toml` files do not
enable `--status-listen` by default -- adding the flag and a matching
`[[services.http_checks]]` block is left to an operator who wants
fly-managed health checks, not required for the manual verification steps
above.

## 9. Run a queso-bench workload against the live cluster

With the same three `fly proxy` tunnels from step 8 still running:

```sh
docker run --rm --network host --entrypoint /usr/local/bin/queso-bench queso-node \
  --addr 127.0.0.1:8000 --addr 127.0.0.1:8001 --addr 127.0.0.1:8002 \
  --concurrency 16 --read-frac 0.5 --keys 1000 --duration-secs 15
```

(Or build `queso-bench` locally with `cargo build --release -p queso-net
--bin queso-bench` and run
`./target/release/queso-bench --addr 127.0.0.1:8000 --addr 127.0.0.1:8001
--addr 127.0.0.1:8002 ...` directly -- no Docker needed for this step.)
Expect `0 errors` and a monotonic (p50 ≤ p90 ≤ p99 ≤ max) latency
histogram, same shape as the local-cluster example in
`crates/net/README.md` -- absolute latency will be much higher here since
`fly proxy` adds a hop and consensus quorum round-trips now cross real
WAN links between `iad`/`lhr`/`nrt` instead of localhost.

## 10. Tear down

```sh
fly apps destroy queso-0 -y
fly apps destroy queso-1 -y
fly apps destroy queso-2 -y
```

`fly apps destroy` also destroys that app's volumes -- there is no
separate volume-cleanup step needed (and no way to recover the durable
state afterward; this is intentionally destructive, matching "tear down").

## 11. Durability notes

- **The volume is the only thing standing between a restart and data
  loss.** `deploy/fly.toml`'s `[mounts]` section is not optional --
  without it, `--data-dir /data` is the container's ephemeral overlay
  filesystem, wiped on every restart/redeploy. See `crates/net/README.md`'s
  "Status"/"Honest limits" sections for exactly what durability this
  crate provides once the volume *is* there (per-RPC fsync, whole-snapshot,
  single-copy).
- **A fly volume is still single-copy, host-local storage** -- it survives
  the *container* restarting/redeploying (fly re-attaches the same volume
  to the replacement machine), but not the underlying host disk failing.
  That is the same "single-copy durability, not replicated backup" limit
  `crates/net/README.md` already documents for this crate's on-disk state;
  a lost volume is recoverable only via this replica catching up from a
  live majority of its peers, identically to a bare-metal disk failure.
- Fly volumes support snapshots (`fly volumes snapshots list` /
  `fly volumes extend`); wiring those into a backup policy is Phase 8
  operability territory, not part of this phase.

## 12. TLS / A3 note

`crates/net/README.md` documents A3 (the content-oblivious-adversary
assumption the paper's randomized-liveness guarantees, P14/P15, depend on)
and Phase 8.2a's (issue #47) app-level TLS support for it -- read that
section for the full design. For **this specific deployment target**,
inter-replica traffic already has a different answer: all `.internal`/6PN
traffic between fly machines in the same org is WireGuard-encrypted by
fly's network layer itself, transparently, with no configuration on this
crate's part -- satisfying A3's content-oblivious-link assumption for
inter-replica traffic without needing this crate's own TLS. **TLS here is
therefore optional, for defense-in-depth** (a second, independent layer
against anyone who can see plaintext traffic *inside* fly's WireGuard mesh,
e.g. a compromised sibling app in the same org) rather than required to
close A3's gap on fly specifically.

The public client port (`8000`, exposed via `deploy/fly.toml`'s
`[[services]]` block) is the one traffic path fly's mesh never covers: a
client connecting from outside fly's network still talks to a replica over
plain TCP unless TLS is explicitly enabled. Two independent options here,
not mutually exclusive: fly's own TLS-terminating proxy in front of the
service (`[[services.ports]] handlers = ["tls"]` in `fly.toml` -- fly.io
platform config, not this crate's concern) covers "traffic between the
client and fly's edge, in the clear"; **this crate's own TLS**
(`queso_net::tls`, see `crates/net/README.md`) covers "traffic all the way
to the replica process," including the fly-edge-to-machine hop fly's proxy
handler does not re-encrypt.

To turn on this crate's TLS for a fly deployment: generate a cluster CA and
one cert/key per replica (any TLS toolchain; `crates/net/README.md`'s TLS
section has an `openssl` recipe), copy each replica's `cert.pem`/`key.pem`
and the shared `ca.pem` onto its persistent volume (alongside `--data-dir`,
or a second small volume) via `flyctl sftp`/a one-shot deploy step, and add
`--tls-cert`/`--tls-key`/`--tls-ca` to that machine's `queso-node` command
in `deploy/fly.toml`'s `[processes]`. Every replica's peer connections then
run mTLS and its client port requires TLS, in addition to (not instead of)
fly's own `.internal` WireGuard encryption. Not exercised end-to-end against
a real fly account in this environment -- see §13's "what this runbook
cannot verify" for the same caveat that already applies to the rest of this
doc.

## 13. What this runbook cannot verify

This was built and verified as far as an environment without a real fly.io
account allows:

- **Verified, in this environment:** `cargo build --release -p queso-net
  --bin queso-node --bin queso-bench` (the exact command
  `deploy/Dockerfile` runs); `docker build -f deploy/Dockerfile .`
  end-to-end (multi-stage build, both binaries present in the final
  image, correct permissions/volume mount point); the built image's
  `queso-node --help`/`queso-bench --help`; and, most importantly, a real
  3-container cluster on a Docker user-defined network dialing each other
  by **hostname** (`q0`/`q1`/`q2`, resolved via Docker's embedded DNS --
  architecturally the same mechanism `.internal` DNS plays on fly),
  serving `queso-bench` load with 0 errors. This is the closest
  local proxy for "peer discovery via `.internal` DNS actually works" that
  doesn't require a fly account. (This environment's own network egress
  policy blocks Docker Hub's CDN directly; the build was verified using a
  public Docker Hub mirror via `--build-arg`, see step 3 -- the committed
  Dockerfile still defaults to the plain `rust`/`debian` image names,
  which work in any normal environment including fly's remote builder.)
- **NOT verified, and cannot be without a real fly.io account:** that
  `fly deploy` itself succeeds; that `fly volumes create`/`[mounts]`
  actually persists across a real fly machine restart; that
  `<app>.internal` DNS resolves the way this document describes on fly's
  actual network (vs. this document's understanding of fly's documented
  behavior); the exact `[build].dockerfile` path-resolution behavior
  flyctl uses relative to `-c`'s location (see step 7's caveat); real
  cross-region WAN latency/throughput numbers; and that
  `auto_stop_machines = false` / `min_machines_running = 1` in
  `deploy/fly.toml` actually prevents fly from ever stopping a replica
  machine (this is this document's understanding of fly's documented
  service-config semantics, not something exercised end-to-end here).
