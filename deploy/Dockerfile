# Multi-stage build for `queso-node` (the deployable replica binary) and
# `queso-bench` (the load generator used to smoke-test a running cluster --
# see docs/deploy-flyio.md). Build from the *repository root*:
#
#   docker build -f deploy/Dockerfile -t queso-node .
#
# The build context must be the repo root, not `deploy/`, because the crate
# graph needs the whole workspace: `queso-net` depends on `queso-sim`/
# `queso-consensus`/`queso-smr` via local `path = "../..."` deps (see
# crates/net/Cargo.toml), and this is a `[workspace]`-resolved Cargo.lock
# (see the root Cargo.toml).
#
# Base images are overridable via --build-arg (defaults are the plain
# upstream images) -- useful if your build environment hits Docker Hub's
# anonymous pull-rate limit, e.g.:
#   docker build -f deploy/Dockerfile \
#     --build-arg RUST_IMAGE=mirror.gcr.io/library/rust:1-bookworm \
#     --build-arg RUNTIME_IMAGE=mirror.gcr.io/library/debian:bookworm-slim \
#     -t queso-node .
ARG RUST_IMAGE=rust:1-bookworm
ARG RUNTIME_IMAGE=debian:bookworm-slim

########################################
# Builder
########################################
FROM ${RUST_IMAGE} AS builder
WORKDIR /build

# Bring in the whole workspace. This is a small, 4-crate workspace (see
# Cargo.toml's `[workspace] members`) -- not worth the complexity of a
# manifest-only dependency-caching pre-copy for a build this size.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# Exactly the command crates/net/README.md documents for building
# queso-node alongside queso-bench (Phase 7.2's load generator -- useful
# for smoke-testing a deployed cluster from inside this same image, see
# docs/deploy-flyio.md's "verify cluster health" / "run a workload" steps).
RUN cargo build --release -p queso-net --bin queso-node --bin queso-bench

########################################
# Runtime
########################################
FROM ${RUNTIME_IMAGE} AS runtime

# ca-certificates: cheap insurance in case a future build adds any
# outbound HTTPS (there is none today -- inter-replica and client traffic
# is plain TCP; see the crate docs' "no TLS yet" note and this repo's A3
# note about fly's WireGuard-encrypted `.internal` mesh already satisfying
# the content-oblivious-link assumption for THIS deployment).
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --create-home --home-dir /home/queso --shell /usr/sbin/nologin queso

COPY --from=builder /build/target/release/queso-node /usr/local/bin/queso-node
COPY --from=builder /build/target/release/queso-bench /usr/local/bin/queso-bench

# Mount point for --data-dir (crates/net/src/persist.rs's fsync'd durable
# state -- issue #36/#38). MUST be backed by a persistent volume in any
# real deployment (see deploy/fly.toml's [mounts] and
# docs/deploy-flyio.md's "Durability" section) -- an ephemeral container
# filesystem here silently reintroduces the lose-an-acknowledged-write-on
# -restart behavior crates/net/README.md's "Honest limits" section warns
# about, since a fresh container gets a blank /data on every restart.
RUN mkdir -p /data && chown queso:queso /data
VOLUME ["/data"]

USER queso
WORKDIR /home/queso

# No fixed ENTRYPOINT: every real invocation (deploy/fly.toml's
# [processes], or a manual `docker run`) supplies the full command, since
# every replica needs distinct --id/--seed/--peer/--data-dir flags -- see
# docs/deploy-flyio.md. This default CMD only prints --help, so running the
# image with no arguments fails fast and readably instead of crash-looping
# against a peer list that doesn't exist.
CMD ["/usr/local/bin/queso-node", "--help"]
