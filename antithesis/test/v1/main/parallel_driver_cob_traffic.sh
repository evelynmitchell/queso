#!/bin/sh
# parallel_driver_: offers Chain-of-Blocks load and checks SAFETY
# continuously, under whatever faults are in force.
#
# `parallel_` rather than `singleton_`: several of these may run at once,
# which is what a real client population looks like and what makes
# concurrent submissions to different replicas — the interesting case for
# an agreement property — actually happen.
#
# The safety property asserted inside is unconditional: no two replicas may
# report a different block at the same height, no matter what Antithesis is
# doing to the network. Liveness is deliberately NOT checked here; a
# partitioned replica is supposed to fall behind (P5 permits arbitrary lag
# and forbids only divergence), so that question belongs in the quiescent
# `eventually_` command instead.
set -eu

exec /usr/local/bin/queso-antithesis \
    --node queso-0 --node queso-1 --node queso-2 \
    traffic --duration-secs 30 --seed "${QUESO_WORKLOAD_SEED:-1}"
