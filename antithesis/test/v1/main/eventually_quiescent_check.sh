#!/bin/sh
# eventually_: runs in the branch Antithesis creates with all driver
# commands killed and all faults stopped.
#
# This is where LIVENESS is asked, and the quiescent branch is precisely the
# right place: with the turbulence gone, "a replica is still behind and
# frozen" means stuck rather than merely lagging. Two properties are
# checked, because neither alone is sufficient — see the `check` command's
# docs in crates/antithesis/src/main.rs for why a stall check cannot see a
# uniformly wedged cluster, and a progress check cannot see one replica left
# behind.
set -eu

exec /usr/local/bin/queso-antithesis \
    --node queso-0 --node queso-1 --node queso-2 \
    check --timeout-secs 120
