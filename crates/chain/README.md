# `queso-chain` — the Chain-of-Blocks `(n, h)` hash chain

A ~200-line leaf crate holding one thing: the definition of a Queso
replica's Chain-of-Blocks state, `(n, h)`, where `n` is how many commands it
has applied and `h` is a running hash over exactly that sequence
(`n += 1; h = hash(h ‖ C)`).

## Why it's a crate rather than a module

Two independent pieces of the system must compute **byte-identical** hashes:

- the **node** — `queso-net` folds the chain as it applies commands and
  publishes checkpoint hashes over `GET /chain` (Phase 9.2, issue #56);
- the **harness** — `queso-conformance` folds the same chain to check
  replicas against each other (Phase 9.1, issue #55).

If those two ever disagree about how a command is encoded, nothing breaks
loudly. Every cross-replica comparison simply stops matching, and a
conformance soak reports "no divergence" forever while checking nothing.

`queso-net` must not depend on `queso-conformance` — that would put test
harness code inside the production binary — so the shared definition lives
here, depending only on `queso-smr` for `Command`.

## What's in it

- `ChainState { n, h }` with `apply`, `from_log`, `prefixes`.
- `extend(h, &Command)` and `command_digest(&Command)`.
- `GENESIS`, the pre-first-command hash.
- A stable, hand-rolled command encoding — deliberately *not* `queso-net`'s
  bincode wire format, which could be renegotiated without anyone realizing
  recorded chain hashes had changed meaning.

## Honest limitation

The hash is FNV-1a with a SplitMix64 finalizer: fast, dependency-free,
well-mixed, and **not cryptographic**. It is sized to detect divergence that
arises by *accident* — a bug in catch-up, recovery, or the transport — where
two differing sequences colliding at 64 bits is vanishingly unlikely. It is
not collision-resistant against an adversary choosing command payloads to
force a collision, and nothing here claims otherwise.

## History

Phase 9.1 (#55) defined this inside `queso-conformance`. Phase 9.2 (#56)
moved it out unchanged so the node could use it too; `queso-conformance`
re-exports it as `queso_conformance::chain`, so existing users are
unaffected.
