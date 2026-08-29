# Tutorial: boot a cluster, kill it, and watch it survive

This is the guided first run. In about fifteen minutes you will build
Queso, boot a three-node cluster on your machine, write to it, kill a
replica with a real `SIGKILL` and watch the cluster keep answering, bring
the dead replica back and watch it catch up, and then kill a *majority*
and watch writes correctly stop. Every command is given exactly; every
output shown was captured from a real run of these commands (your
numbers — uptimes, latencies, hashes — will differ, and the text says
what must match anyway).

Two honest framings before you start:

- **Queso is a reference implementation and a learning vehicle.** It has
  never handled production traffic, and this tutorial is not a
  getting-started guide for a product. See the README's "Honest status &
  limitations" section for what is and is not claimed.
- **The failure steps are the point.** A consensus demo that only shows
  the happy path teaches the wrong model. Step 4 (a dead replica the
  cluster shrugs off) and step 6 (a dead majority the cluster correctly
  refuses to shrug off) are each half of what "majority quorum" means.

You need a recent stable Rust toolchain (`rustup`, `cargo`) and four
terminal windows: one per replica, one for you.

## 1. Build

From the repository root:

```sh
cargo build -p queso-net --bin queso-node --bin queso-admin
```

`queso-node` is a replica; `queso-admin` is a small operator CLI for
poking a running cluster from outside. A debug build is fine for this
tutorial — nothing here measures performance.

## 2. Boot three replicas

Each replica needs its own identity (`--id`), a deterministic RNG seed
(`--seed` — QuePaxa's proposers draw random priorities, and the sim-verified
core insists all randomness be seeded), a peer-facing port (`--listen`), a
client-facing port (`--client-listen`), the **full membership list**
(`--peer`, including its own entry), and a directory for its durable state
(`--data-dir` — this is what survives the crashes to come). Two optional
flags are included so the later steps have something to look at:
`--status-listen` serves `GET /health`, `/ready`, `/metrics`, and `/chain`
over plain HTTP, and `--chain-checkpoints 8` makes each replica publish a
hash of everything it has applied at every 8th slot.

Terminal 1:

```sh
./target/debug/queso-node \
  --id 0 --seed 1 \
  --listen 127.0.0.1:7000 --client-listen 127.0.0.1:8000 \
  --status-listen 127.0.0.1:9000 --chain-checkpoints 8 \
  --peer 0=127.0.0.1:7000 --peer 1=127.0.0.1:7001 --peer 2=127.0.0.1:7002 \
  --leader 0 --data-dir ./data
```

Terminal 2 — same shape, ids and ports shifted:

```sh
./target/debug/queso-node \
  --id 1 --seed 2 \
  --listen 127.0.0.1:7001 --client-listen 127.0.0.1:8001 \
  --status-listen 127.0.0.1:9001 --chain-checkpoints 8 \
  --peer 0=127.0.0.1:7000 --peer 1=127.0.0.1:7001 --peer 2=127.0.0.1:7002 \
  --leader 0 --data-dir ./data
```

Terminal 3:

```sh
./target/debug/queso-node \
  --id 2 --seed 3 \
  --listen 127.0.0.1:7002 --client-listen 127.0.0.1:8002 \
  --status-listen 127.0.0.1:9002 --chain-checkpoints 8 \
  --peer 0=127.0.0.1:7000 --peer 1=127.0.0.1:7001 --peer 2=127.0.0.1:7002 \
  --leader 0 --data-dir ./data
```

The nodes are quiet by default — no banner, no log stream. That is
normal. `--leader 0` nominates replica 0 for the fast path (§4.2.5 of the
QuePaxa paper: one round trip when the leader's proposal reaches a full
quorum first); you could omit it on all three and run purely leaderless.
The three replicas share `./data` safely — files inside are keyed by id.

## 3. Prove it is a cluster

From your fourth terminal:

```sh
./target/debug/queso-admin status \
  --status-addr 127.0.0.1:9000 --status-addr 127.0.0.1:9001 --status-addr 127.0.0.1:9002
```

```text
index address                      reachable  ready   next_slot  save_cnt   uptime_s
0     127.0.0.1:9000               yes        true    0          0          2.0
1     127.0.0.1:9001               yes        true    0          0          2.0
2     127.0.0.1:9002               yes        true    0          0          2.0

cluster: 3/3 replicas reachable, all_ready=true
frontier: agrees at next_slot=0
```

Now write a key and read it back:

```sh
./target/debug/queso-admin put 42 777 \
  --addr 127.0.0.1:8000 --addr 127.0.0.1:8001 --addr 127.0.0.1:8002
# Put

./target/debug/queso-admin get 42 \
  --addr 127.0.0.1:8000 --addr 127.0.0.1:8001 --addr 127.0.0.1:8002
# Get(Some(777))
```

That `Put` was decided by consensus and fsync'd on a majority before the
reply came back. Run `status` again and you will see something worth
pausing on:

```text
index address                      reachable  ready   next_slot  save_cnt   uptime_s
0     127.0.0.1:9000               yes        true    2          6          2.0
1     127.0.0.1:9001               yes        true    0          2          2.0        (lagging)
2     127.0.0.1:9002               yes        true    0          2          2.0        (lagging)

cluster: 3/3 replicas reachable, all_ready=true
frontier: max next_slot=2; lagging replica indices (behind max): [1, 2]
```

"Lagging" is expected, not a bug: a Queso replica advances its own
frontier only by *participating* in slots itself — recording votes for
replica 0's proposals does not make replicas 1 and 2 apply anything. They
hold the durable consensus state that makes the write safe; they apply it
when something asks them to do work of their own (as the next steps
will). `queso-admin status` reports that honestly rather than hiding it.

## 4. Kill a node — a real one, with a real `SIGKILL`

Not Ctrl-C. `SIGKILL` cannot be caught: no flush, no goodbye message to
peers, no cleanup. The process is simply gone, mid-whatever-it-was-doing.
And kill the *leader*, because that is the interesting case — in a
single-leader protocol this is the moment everything stalls for an
election.

```sh
pkill -9 -f 'queso-node --id 0'
```

(Terminal 1's process dies. `-f` matches against the full command line,
so this pattern picks out exactly replica 0.)

Now write another key, straight away:

```sh
./target/debug/queso-admin put 7 1234 \
  --addr 127.0.0.1:8000 --addr 127.0.0.1:8001 --addr 127.0.0.1:8002
```

```text
WARN queso_net::client: queso-net client: attempt failed addr=127.0.0.1:8000 err=Connection refused (os error 111)
Put
```

The client tried the dead replica's address first, got connection
refused, moved to the next address — and the write was decided. In the
run captured here the whole thing, retry included, took 28 milliseconds.
`get 42` still answers `Get(Some(777))`. Status now reports the truth:

```text
index address                      reachable  ready   next_slot  save_cnt   uptime_s
0     127.0.0.1:9000               no         -       -          -          -           unreachable: connect to status server at 127.0.0.1:9000
1     127.0.0.1:9001               yes        true    4          26         3.1
2     127.0.0.1:9002               yes        true    0          10         3.1        (lagging)

cluster: 2/3 replicas reachable, all_ready=true
```

Two things happened at once here, and they are worth separating:

- **Any majority decides.** Three replicas tolerate one failure; the two
  survivors are a quorum, so consensus proceeds. This much any majority
  protocol gives you.
- **The dead node was the leader, and nothing stalled.** QuePaxa's
  leader is a latency optimization, not a dependency: proposers fall
  back to randomized priorities within the same round, so losing the
  leader costs the fast path, never an election timeout. This is the
  protocol's headline property, and you just watched it.

## 5. Bring it back and watch it catch up

Rerun terminal 1's exact command from step 2. The replica reloads its
durable state from `./data/node-0.durable.bin` — everything it had
applied before the kill — and then runs an internal catch-up probe to
learn what the cluster decided while it was dead.

```sh
./target/debug/queso-admin status \
  --status-addr 127.0.0.1:9000 --status-addr 127.0.0.1:9001 --status-addr 127.0.0.1:9002
```

```text
index address                      reachable  ready   next_slot  save_cnt   uptime_s
0     127.0.0.1:9000               yes        true    5          23         3.3
1     127.0.0.1:9001               yes        true    4          33         6.1        (lagging)
2     127.0.0.1:9002               yes        true    0          17         6.1        (lagging)

cluster: 3/3 replicas reachable, all_ready=true
frontier: max next_slot=5; lagging replica indices (behind max): [1, 2]
```

Three seconds after rebooting, replica 0 is back at the front (its
catch-up probe itself occupies a slot, which is why it can end up one
*past* its peers). While the probe runs, `GET /ready` on port 9000
answers `503`; on localhost the window is usually too short to catch by
hand — the probe finishes in milliseconds — but it is what a load
balancer's readiness check would use to avoid routing to a replica that
is still learning.

Now the stronger check. Spread a few writes across all three replicas so
each participates and applies:

```sh
for k in 101 102 103 104 105 106 107 108; do
  ./target/debug/queso-admin put $k $k \
    --addr 127.0.0.1:800$((k % 3)) --addr 127.0.0.1:8000 --addr 127.0.0.1:8001
done
```

(Keys 101–108, deliberately clear of the keys this tutorial already
wrote — the first draft of this page used keys 1–8 here, key 7 silently
overwrote step 4's `7=1234`, and the final read-back caught it. Keep
that in mind when scripting against a KV store: a put is an overwrite.)

and compare what the restarted replica and a never-killed one publish at
`GET /chain`:

```sh
curl -s 127.0.0.1:9000/chain
curl -s 127.0.0.1:9001/chain
```

Replica 0 (the `SIGKILL` survivor):

```json
{
  "checkpoint_every": 8,
  "frontier": { "n": 13, "h": "0x40df9678ff0f3a72" },
  "truncated": false,
  "checkpoints": [
    { "n": 8, "h": "0xaf59834e9ea79c17" }
  ]
}
```

Replica 1 (never killed):

```json
{
  "checkpoint_every": 8,
  "frontier": { "n": 11, "h": "0x51c82099317f90df" },
  "truncated": false,
  "checkpoints": [
    { "n": 8, "h": "0xaf59834e9ea79c17" }
  ]
}
```

Each checkpoint hash folds the *entire sequence* of commands that replica
applied up to slot `n`. Your hashes will differ from the ones printed
here (they depend on your exact command history), and the two replicas'
frontiers will usually differ too — but **at every `n` both replicas
publish, the hashes must be identical**, as they are in the captured run
(`n=8` above: same hash from the `SIGKILL` survivor, which refolded its
chain from the durable log at boot, and from a replica that never died).
If they ever differed, two replicas would have applied different
histories — an Agreement violation, the one thing a consensus protocol
must never allow. This exact cross-check, mechanized, is what the
project's conformance and soak harnesses run for hours at a time.

## 6. Kill a majority — and watch writes correctly stop

```sh
pkill -9 -f 'queso-node --id 1'
pkill -9 -f 'queso-node --id 2'
```

Replica 0 is now alone: alive, listening, and constitutionally unable to
decide anything. Try to write:

```sh
./target/debug/queso-admin put 99 1 \
  --addr 127.0.0.1:8000 --addr 127.0.0.1:8001 --addr 127.0.0.1:8002
```

```text
WARN queso_net::client: attempt timed out   addr=127.0.0.1:8000 timeout=2s
WARN queso_net::client: attempt failed      addr=127.0.0.1:8001 err=Connection refused (os error 111)
WARN queso_net::client: attempt failed      addr=127.0.0.1:8002 err=Connection refused (os error 111)
... (the same round, four more times) ...
Error: Connection refused (os error 111)
```

About ten seconds of retries, then a non-zero exit. Read the three lines
carefully, because they show the difference between two kinds of dead:
replicas 1 and 2 refuse the connection (nothing is listening), while
replica 0 *accepts* the connection and then times out — it took the
command, proposed it, and sat waiting for a quorum of recorders that will
never answer. (The final `Error:` line reports the *last* failure the
client saw, a connection refusal, even though the survivor's own failure
mode was the timeout.) Reads stop too: `get 42` goes through the same
consensus path, so a lone replica will not serve you a value it cannot
prove is current.

This is the failure half of the majority-quorum bargain, and it is a
feature: replica 0 cannot distinguish "my peers are dead" from "I am
partitioned from my peers, who are happily deciding without me." A
replica that answered anyway could diverge from a majority it cannot
see. Queso — like every consensus system — chooses safety over
availability here, and stops.

## 7. Put it back together

Rerun terminals 2 and 3's commands from step 2. Within a couple of
seconds, retry the write and read everything back:

```sh
./target/debug/queso-admin put 99 1 \
  --addr 127.0.0.1:8000 --addr 127.0.0.1:8001 --addr 127.0.0.1:8002
# Put

./target/debug/queso-admin get 42 --addr 127.0.0.1:8000 --addr 127.0.0.1:8001 --addr 127.0.0.1:8002
# Get(Some(777))
./target/debug/queso-admin get 7 --addr 127.0.0.1:8000 --addr 127.0.0.1:8001 --addr 127.0.0.1:8002
# Get(Some(1234))
./target/debug/queso-admin get 99 --addr 127.0.0.1:8000 --addr 127.0.0.1:8001 --addr 127.0.0.1:8002
# Get(Some(1))
```

Every write that was ever acknowledged is still there: the one from
before any failure, the one decided while the leader was dead, and the
one that was refused during the outage and retried after. Nothing
acknowledged was lost across five process deaths — that is the
write-before-reply durability guarantee doing its job.

## 8. Clean up

Ctrl-C the three node terminals (or `pkill -9 -f target/debug/queso-node`
— note `-f` matches whole command lines, so a pattern this broad can catch
an unrelated command that merely *mentions* that path), and:

```sh
rm -rf ./data
```

## Where to go next

- Why consensus is shaped like this, and where QuePaxa sits in the
  design space: [`01-backgrounder.md`](01-backgrounder.md).
- What exactly is guaranteed — the property model this tutorial's claims
  ("Agreement", "nothing acknowledged was lost") are drawn from:
  [`02-properties.md`](02-properties.md).
- The machine-checked version: the TLA+ models and what TLC exhaustively
  verified, in [`spec/`](spec.md).
- What the test suite does and does not establish, instrument by
  instrument: [`what-each-test-establishes.md`](what-each-test-establishes.md).

## How this tutorial is kept honest

A tutorial that rots fails a newcomer at the exact moment they have the
least context to recover, so this one is not left to rot:

- **The walkthrough itself runs in CI** (evidence class: tested).
  `crates/net/tests/tutorial.rs` spawns real `queso-node` processes and
  drives the real `queso-admin` binary through this exact arc — put/get,
  `SIGKILL` the leader, write through the failure, restart, `/chain`
  agreement, `SIGKILL` a majority, watch the write fail, restore, verify
  every acknowledged key. If a flag or output shape this page depends on
  changes, that test breaks before this page lies to you.
- Every output block above is from a real run of the commands shown, on
  the commit that introduced this page — not composed by hand.
- The deeper machinery has its own tests: `tests/chain_restart.rs`
  (checkpoint refold across `SIGKILL`), `tests/restart_recovery.rs`
  (durable-state reload), `tests/admin.rs` (the CLI's paths), and the
  soak/conformance harnesses (`crates/soak`, `crates/conformance`) that
  run the `/chain` cross-check under sustained fault injection.
