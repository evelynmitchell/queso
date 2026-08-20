# The Antithesis workload image (Phase 9.3, issue #54).
#
# Contains the `queso-antithesis` binary and the Test Composer commands that
# invoke it. Build from the *repository root*, like deploy/Dockerfile and for
# the same reason — the crate graph needs the whole workspace:
#
#   docker build -f antithesis/Dockerfile -t queso-antithesis .
#
# Base images are overridable via --build-arg, matching deploy/Dockerfile,
# for build environments that hit Docker Hub's anonymous pull-rate limit.
ARG RUST_IMAGE=rust:1-bookworm
ARG RUNTIME_IMAGE=debian:bookworm-slim

########################################
# Builder
########################################
FROM ${RUST_IMAGE} AS builder
WORKDIR /build

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN cargo build --release -p queso-antithesis --bin queso-antithesis

########################################
# Runtime
########################################
FROM ${RUNTIME_IMAGE} AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/queso-antithesis /usr/local/bin/queso-antithesis

# Where Antithesis's Test Composer looks for commands. The directory name
# under v1 ("main") is the test template's name; the filename prefixes
# (first_, parallel_driver_, eventually_) are what tell the platform when
# each may run.
COPY antithesis/test/v1/main/ /opt/antithesis/test/v1/main/
RUN chmod +x /opt/antithesis/test/v1/main/*

# Runs as root deliberately, unlike deploy/Dockerfile's node image: the Test
# Composer executes these commands in this container, and dropping
# privileges here buys nothing while risking a permission failure that would
# look like a test failure.

# Idles. The container exists to be a place for test commands to run, not to
# do anything on its own — see the compose file's note on why the workload
# injects no faults of its own.
CMD ["sleep", "infinity"]
