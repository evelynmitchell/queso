#!/usr/bin/env python3
"""Replay recorded falsifier mutations and check they still kill what they claim.

Background
----------
`crates/` carries ~43 `Falsifier, run:` markers. Each records a mutation and
the kill count it produced *at some past commit*. Nothing re-ran them, so a
refactor that quietly removed a test's detection power left the marker
asserting the old number -- and the marker is exactly what a later reader
treats as established fact. `docs/conformance-matrix.md` §6.8 put it as
"the counts rot silently".

This is the same move `tlc-nightly.yml` made for the TLA+ state counts: turn
a pasted number into an asserted one.

Two modes, deliberately split by cost
-------------------------------------
``--check-anchors``   No build. Verifies every registered mutation's ``find``
                      string still occurs exactly once in its file. Runs in
                      milliseconds, so it can gate every PR. This catches the
                      most common rot -- code moved and the recorded mutation
                      no longer describes anything -- which a count check
                      would only find hours later, if at all.

``--replay``          Applies each mutation, runs its scoped tests, and checks
                      the named tests fail and the named controls pass. Costs
                      one rebuild per mutation, so it belongs on a schedule.

What is asserted
----------------
Per mutation: an exact set of tests that must FAIL, and a set that must PASS.
Both directions matter. `expect_pass` entries are controls -- without them a
mutation that reddens the entire suite would look like a precise instrument.
A registered mutation with an empty `expect_fail` is a recorded **zero**
(e.g. P14's): the assertion is that nothing named still detects it, and a new
kill there is a finding worth writing up, not a failure to paper over.

Safety
------
This edits files in the working tree. It refuses to run on a dirty tree, and
restores every file from the exact bytes it read, in a `finally`, including
on Ctrl-C. It re-checks `git status` before exiting.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
REGISTRY = Path(__file__).resolve().parent / "registry.toml"


def tracked_changes() -> str:
    """Modified/added/deleted tracked files, ignoring untracked ones.

    Untracked files are not a hazard: this script only ever rewrites files
    named in the registry, all of which are tracked. What *would* be a hazard
    is running over somebody's in-progress edits to one of those files, so
    those still block.
    """
    out = subprocess.run(
        ["git", "status", "--porcelain"], cwd=ROOT, capture_output=True, text=True
    ).stdout
    return "\n".join(l for l in out.splitlines() if not l.startswith("??")).strip()


def load_registry() -> list[dict]:
    with REGISTRY.open("rb") as fh:
        return tomllib.load(fh)["mutation"]


def check_anchors(muts: list[dict]) -> int:
    """Verify each `find` string occurs exactly once in its file."""
    bad = 0
    for m in muts:
        path = ROOT / m["file"]
        if not path.exists():
            print(f"MISSING FILE  {m['id']}: {m['file']}")
            bad += 1
            continue
        n = path.read_text().count(m["find"])
        if n != 1:
            print(
                f"ANCHOR DRIFT  {m['id']}: `find` matches {n}x in {m['file']} "
                f"(expected exactly 1)"
            )
            bad += 1
        else:
            print(f"ok  {m['id']:<28} {m['file']}")
    return bad


def run_tests(scope: list[str]) -> set[str]:
    """Run cargo test for a scope; return the set of failing test names."""
    proc = subprocess.run(
        ["cargo", "test", *scope, "--no-fail-fast"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    out = proc.stdout + proc.stderr
    # `should_panic` tests print "test name - should panic ... FAILED", so the
    # suffix has to be tolerated or those failures are silently missed.
    return set(
        re.findall(r"^test (\S+)(?: - should panic)? \.\.\. FAILED$", out, re.MULTILINE)
    )


def replay(muts: list[dict], only: str | None) -> int:
    failures = 0
    for m in muts:
        if only and m["id"] != only:
            continue
        path = ROOT / m["file"]
        original = path.read_text()
        if original.count(m["find"]) != 1:
            print(f"SKIP (anchor drift)  {m['id']}")
            failures += 1
            continue
        print(f"\n=== {m['id']}  ({m['property']}, #{m['issue']}) ===")
        try:
            path.write_text(original.replace(m["find"], m["replace"]))
            failed = run_tests(m["scope"])
        finally:
            path.write_text(original)

        want_fail = set(m.get("expect_fail", []))
        want_pass = set(m.get("expect_pass", []))

        missing = want_fail - failed
        survived_but_shouldnt = want_pass & failed

        if missing:
            print(f"  LOST POWER: these no longer fail: {sorted(missing)}")
            failures += 1
        if survived_but_shouldnt:
            print(f"  CONTROL BROKE: these should still pass: {sorted(survived_but_shouldnt)}")
            failures += 1
        if not want_fail:
            # A recorded zero. Any kill among the named controls is caught
            # above; a kill elsewhere in scope is reported but not failed,
            # since scope is broader than what the zero was measured over.
            print(f"  recorded zero holds (scope saw {len(failed)} failures, none named)")
        if not missing and not survived_but_shouldnt:
            print(f"  ok: {len(want_fail)} expected kill(s), {len(want_pass)} control(s) intact")
    return failures


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--check-anchors", action="store_true")
    ap.add_argument("--replay", action="store_true")
    ap.add_argument("--only", help="replay a single mutation id")
    args = ap.parse_args()

    muts = load_registry()

    if args.check_anchors:
        bad = check_anchors(muts)
        print(f"\n{len(muts)} registered, {bad} with drift")
        return 1 if bad else 0

    if args.replay:
        dirty = tracked_changes()
        if dirty:
            print("refusing to replay with modified tracked files -- this script edits files in place:")
            print(dirty)
            return 2
        failures = replay(muts, args.only)
        still_dirty = tracked_changes()
        if still_dirty:
            print(f"\nBUG: tree left dirty after replay:\n{still_dirty}")
            return 2
        print(f"\n{failures} mutation(s) no longer behave as recorded")
        return 1 if failures else 0

    ap.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
