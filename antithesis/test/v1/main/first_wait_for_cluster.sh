#!/bin/sh
# first_: runs before any driver command.
#
# Waits for all three replicas to answer GET /health, then signals
# `setup_complete`. Antithesis holds off on faults until it sees that
# signal, which is the whole reason this exists as its own command: without
# it the platform would start partitioning a cluster that had not finished
# booting, and every liveness result for the rest of the run would be noise
# about a cluster that was never up.
#
# Failing here is a harness or topology problem, not a Queso property
# violation, and a non-zero exit says exactly that.
set -eu

exec /usr/local/bin/queso-antithesis \
    --node queso-0 --node queso-1 --node queso-2 \
    wait-ready --timeout-secs 180
