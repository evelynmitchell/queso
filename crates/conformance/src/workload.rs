//! The Chain-of-Blocks client: a stateless loop proposing random-payload
//! commands, plus the run driver that interleaves submission, time, and
//! observation.
//!
//! From the CoB doc: *"the client is stateless -- it loops proposing random
//! byte-array commands. Proposals may fail under fault -- that's
//! expected."* The workload asserts nothing itself; all judgement lives in
//! [`crate::observer`]. That split is deliberate: a workload that also
//! decided what counted as a failure would be tempted to excuse the
//! failures it caused.
//!
//! # The payload, and why the value is a digest
//!
//! Queso's `Value` is a fixed `i64`, so a CoB "random byte array" is
//! carried as *its digest*: the workload draws a random payload, hashes it
//! to 64 bits, and submits that as a `Command::Put` value (issue #55's
//! option (a) -- no new command variant in the verified core). Two
//! different payloads therefore produce two different commands with
//! overwhelming probability, which is all the chain needs.
//!
//! The payload bytes themselves are not retained. Nothing downstream reads
//! them: the chain hashes the *command*, and the observer compares chains.
//! Keeping them would only create the illusion that the harness could
//! reconstruct application state it never had.
//!
//! # Determinism
//!
//! The payload PRNG is a seeded SplitMix64 embedded here rather than an
//! ambient RNG, so a conformance run is reproducible from `(cluster seed,
//! workload seed)` alone -- the same contract every other test in this
//! workspace holds to, and the reason this crate opts into the workspace
//! determinism lints.

use queso_smr::{ClientId, Command, Key};

use crate::observer::Observer;
use crate::source::CobTarget;

/// SplitMix64 -- a tiny, seeded, dependency-free PRNG for payload bytes.
///
/// Not used for anything the protocol sees; only to make each submitted
/// command distinct in a reproducible way.
#[derive(Clone, Debug)]
struct SplitMix64(u64);

impl SplitMix64 {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
}

/// The stateless CoB client: draws random payloads and turns them into
/// Queso commands.
#[derive(Clone, Debug)]
pub struct CobWorkload {
    rng: SplitMix64,
    clients: u32,
    key: Key,
    issued: u64,
}

impl CobWorkload {
    /// A workload seeded by `seed`, cycling over 4 client ids, writing to a
    /// single key.
    ///
    /// One key is the right default: the Chain-of-Blocks property is about
    /// the *order commands were applied in*, which the log decides, not
    /// about key-space coverage. Use [`Self::with_key`] if a scenario wants
    /// a different one.
    pub fn new(seed: u64) -> Self {
        Self {
            // Avoid a zero state, which SplitMix64 handles fine but which
            // makes seed 0 look special in traces.
            rng: SplitMix64(seed ^ 0x51ed_2701_a5b4_c39f),
            clients: 4,
            key: 0,
            issued: 0,
        }
    }

    /// How many client ids to cycle submissions over.
    pub fn with_clients(mut self, clients: u32) -> Self {
        self.clients = clients.max(1);
        self
    }

    /// Which key the workload writes to.
    pub fn with_key(mut self, key: Key) -> Self {
        self.key = key;
        self
    }

    /// How many commands this workload has produced.
    pub fn issued(&self) -> u64 {
        self.issued
    }

    /// Draw the next command: a `Put` of a fresh random payload's digest.
    ///
    /// The `seq` field is left at 0 -- the target re-tags it with its own
    /// monotonic counter (see [`crate::source::SimCluster::submit`]), which
    /// is the only place that knows what has actually been submitted.
    pub fn next_command(&mut self) -> Command {
        let payload = self.rng.next_u64();
        let client = ClientId((self.issued % u64::from(self.clients)) as u32);
        self.issued += 1;
        Command::Put {
            client,
            seq: 0,
            key: self.key,
            // `as i64` is a reinterpretation, not a truncation: every one
            // of the 64 payload bits survives into the value.
            value: payload as i64,
        }
    }
}

/// How a conformance run interleaves submission, time, and observation.
#[derive(Clone, Copy, Debug)]
pub struct RunConfig {
    /// How many commands to submit in total.
    pub commands: usize,
    /// How much target time to let pass after each submission.
    pub advance_between: u64,
    /// Poll every replica after this many submissions. `1` polls after
    /// every command.
    pub poll_every: usize,
    /// How much target time to let pass after the last submission, before
    /// the final poll -- the window in which a healthy cluster is expected
    /// to converge.
    pub settle: u64,
}

impl Default for RunConfig {
    fn default() -> Self {
        Self {
            commands: 40,
            advance_between: 300,
            poll_every: 4,
            settle: 500_000,
        }
    }
}

/// Drive a Chain-of-Blocks run: submit, advance, poll, repeat; then settle
/// and poll once more.
///
/// Returns nothing -- every verdict is in `observer`, which the caller
/// inspects (and which is deliberately not consumed here, so a caller can
/// run several phases, injecting faults between them, against one
/// observer).
pub fn run(
    target: &mut impl CobTarget,
    workload: &mut CobWorkload,
    observer: &mut Observer,
    config: RunConfig,
) {
    let poll_every = config.poll_every.max(1);
    for i in 0..config.commands {
        let command = workload.next_command();
        target.submit(command);
        target.advance(config.advance_between);
        if (i + 1) % poll_every == 0 {
            for sample in target.poll_samples() {
                observer.observe(sample);
            }
        }
    }
    settle(target, observer, config.settle);
}

/// Let `units` of target time pass, then poll every replica once. Used
/// between fault phases -- e.g. after healing -- and at the end of a run.
pub fn settle(target: &mut impl CobTarget, observer: &mut Observer, units: u64) {
    target.advance(units);
    for sample in target.poll_samples() {
        observer.observe(sample);
    }
}

/// Give **every** replica fresh work for `rounds` rounds, letting `advance`
/// units of time pass and polling after each round.
///
/// # Why this exists, and why liveness needs it
///
/// Queso has no background replication push. A replica learns a slot's
/// decision by *participating* -- proposing, recording, or catching up
/// because it was asked to do something. Unlike a leader-driven protocol
/// that heartbeats `AppendEntries` at idle followers, a Queso replica that
/// is given no work simply sits at whatever frontier it last reached, and
/// that is correct behavior: P5 permits a replica to lag arbitrarily, and
/// only forbids it to *diverge*.
///
/// This has a direct consequence for the liveness observer: "replica is
/// behind and has not advanced" is only evidence of a stall if that replica
/// was actually given something to do. Call this before
/// [`crate::observer::Observer::stalls`] so that every replica has had
/// traffic in the window being judged; otherwise an idle-but-healthy
/// replica is indistinguishable from a stuck one, and the check reports
/// noise.
///
/// Each round submits one command per replica. [`crate::source::SimCluster`]
/// spreads submissions round-robin over the live replicas, so a round
/// reaches each of them once.
pub fn converge(
    target: &mut impl CobTarget,
    workload: &mut CobWorkload,
    observer: &mut Observer,
    rounds: usize,
    advance: u64,
) {
    let replicas = target.replicas().len().max(1);
    for _ in 0..rounds {
        for _ in 0..replicas {
            let command = workload.next_command();
            target.submit(command);
        }
        settle(target, observer, advance);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payloads_differ_run_to_run_but_repeat_for_a_given_seed() {
        let mut a = CobWorkload::new(1);
        let mut b = CobWorkload::new(1);
        let mut c = CobWorkload::new(2);

        let a_commands: Vec<Command> = (0..8).map(|_| a.next_command()).collect();
        let b_commands: Vec<Command> = (0..8).map(|_| b.next_command()).collect();
        let c_commands: Vec<Command> = (0..8).map(|_| c.next_command()).collect();

        assert_eq!(a_commands, b_commands, "same seed must replay identically");
        assert_ne!(a_commands, c_commands, "different seeds must differ");
    }

    #[test]
    fn every_command_in_a_run_is_distinct() {
        let mut workload = CobWorkload::new(99);
        let commands: Vec<Command> = (0..256).map(|_| workload.next_command()).collect();
        let mut values: Vec<i64> = commands
            .iter()
            .map(|c| match c {
                Command::Put { value, .. } => *value,
                Command::Get { .. } => unreachable!("the CoB workload only issues puts"),
            })
            .collect();
        values.sort_unstable();
        let before = values.len();
        values.dedup();
        assert_eq!(
            values.len(),
            before,
            "duplicate payload digests would make two blocks indistinguishable"
        );
    }

    #[test]
    fn submissions_cycle_over_the_configured_clients() {
        let mut workload = CobWorkload::new(5).with_clients(3);
        let clients: Vec<ClientId> = (0..6)
            .map(|_| match workload.next_command() {
                Command::Put { client, .. } => client,
                Command::Get { .. } => unreachable!(),
            })
            .collect();
        assert_eq!(
            clients,
            vec![
                ClientId(0),
                ClientId(1),
                ClientId(2),
                ClientId(0),
                ClientId(1),
                ClientId(2)
            ]
        );
    }
}
