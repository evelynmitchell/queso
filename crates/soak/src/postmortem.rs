//! Adjudicating a preserved failure (issue #73): recomputing each replica's
//! chain from its **durable applied log** and confronting that with what
//! the observability path claimed.
//!
//! # The question this exists to answer
//!
//! The Chain-of-Blocks observer compares replicas by sampled `(n, h)`
//! pairs, because that is the only observation a real `queso-node` process
//! offers ([`queso_conformance::observer`] explains why at length). When it
//! reports a divergence, exactly two things can have happened:
//!
//! 1. Two replicas really applied different commands at the same slot -- a
//!    genuine Agreement (P1) violation.
//! 2. The path that produced the samples mis-reported one of them, and the
//!    replicas never disagreed at all.
//!
//! The observer cannot tell those apart, and neither can its report: both
//! look like two hashes at one `n`. What distinguishes them is the applied
//! log, which is durable and is not on the observability path. [`evidence`]
//! is what keeps it; this module is what reads it.
//!
//! [`evidence`]: crate::evidence
//!
//! # Why the logs are compared, and not just their hashes
//!
//! Folding each log into a chain and comparing hashes would answer
//! "do they agree?" but not "on what?". Comparing the command sequences
//! directly answers both, and names the two commands at the earliest slot
//! where they part -- which is the fact a root cause is reconstructed from.
//! The hash chain is still recomputed, because the *claims* being checked
//! are hashes ([`Claim`]) and confirming one means reproducing it.
//!
//! # What a verdict here does and does not settle
//!
//! A preserved log is the state as of the replica's last durable snapshot.
//! It is authoritative about the slots it holds and says nothing about the
//! ones it does not -- a replica killed before its snapshot caught up has a
//! short log, which is neither agreement nor disagreement. Every type here
//! keeps that third answer distinct from the other two rather than folding
//! it into "agrees": [`PairVerdict::NoOverlap`] and
//! [`ClaimVerdict::ShortLog`] exist so a run that proved nothing cannot be
//! reported as a run that proved safety.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

use queso_chain::{command_digest, BlockHash, ChainState};
use queso_conformance::observer::Divergence;
use queso_consensus::isr::IsrSummary;
use queso_consensus::{Proposal, H};
use queso_net::persist::Store;
use queso_sim::ids::NodeId;
use queso_smr::{Command, SmrNode};

/// One replica's durable evidence, recovered from a preserved data dir:
/// its applied log, plus its per-slot recorder (ISR) state.
///
/// `commands[i]` is the command this replica applied at slot `i`; slots are
/// gap-free (P7), so the index *is* the slot and the chain over
/// `commands[..k]` is exactly this replica's `(k, h)` chain state.
///
/// `recorders` (issue #84) is the state the applied log cannot speak for:
/// what this replica's recorder had durably registered per slot -- the ISR
/// `(S, F, A_p)` summary P12 exists to protect. It is what distinguishes
/// "the decision was lost after being recorded" from "the recorder never
/// durably saw the step" on the next #83-shaped occurrence. Absence of a
/// slot key is itself evidence (this replica's recorder was never touched
/// there) and is kept distinct from an absent snapshot -- see
/// [`RecorderView`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedLog {
    /// Which replica this log belongs to, from its snapshot filename.
    pub replica: NodeId,
    /// The applied commands, in slot order.
    pub commands: Vec<Command>,
    /// Per-slot recorder (ISR) summaries, keyed by slot.
    pub recorders: BTreeMap<u64, IsrSummary<Command>>,
}

impl AppliedLog {
    /// Load replica `replica`'s log from a preserved data dir, or `None`
    /// when that replica left no snapshot behind.
    pub fn load(data_dir: &Path, replica: NodeId) -> anyhow::Result<Option<Self>> {
        if !data_dir.is_dir() {
            anyhow::bail!("{} is not a directory", data_dir.display());
        }
        let Some((durable, _max_tick)) = Store::new(data_dir, replica)?.load()? else {
            return Ok(None);
        };
        // `Durable`'s fields are `pub(crate)` to `queso-smr`; rehydrating
        // the node is the supported way to read them back, and costs nothing
        // beyond the clone the load already did.
        let node = SmrNode::from_durable(1, None, durable);
        Ok(Some(Self {
            replica,
            commands: node.applied_from(0),
            recorders: node.recorder_summaries(),
        }))
    }

    /// Every replica log in a preserved data dir, ordered by replica id.
    ///
    /// A data dir with no snapshots is an empty `Vec`, not an error: a
    /// cluster that died before its first persist is a real -- and
    /// interesting -- outcome, and the caller reports it as the absence of
    /// evidence it is.
    pub fn discover(data_dir: &Path) -> anyhow::Result<Vec<Self>> {
        let mut replicas: Vec<NodeId> = Vec::new();
        for entry in std::fs::read_dir(data_dir)? {
            let name = entry?.file_name();
            let Some(name) = name.to_str() else { continue };
            // Exactly the snapshot files. The `.tmp` siblings the
            // atomic-rename scheme leaves behind (see `queso_net::persist`)
            // are half-written by construction; `Store` would never read
            // one anyway, since it rebuilds the canonical filename from the
            // id, so this suffix check is what stops a `.tmp` from
            // conjuring a replica that has no snapshot at all.
            let Some(rest) = name.strip_prefix("node-") else {
                continue;
            };
            let Some(id) = rest.strip_suffix(".durable.bin") else {
                continue;
            };
            let Ok(id) = id.parse::<u32>() else { continue };
            replicas.push(NodeId(id));
        }
        replicas.sort_unstable();
        replicas
            .into_iter()
            .filter_map(|replica| Self::load(data_dir, replica).transpose())
            .collect()
    }

    /// This replica's chain state at height `n`, or `None` when its log is
    /// shorter than `n` and so has nothing to say about that height.
    pub fn chain_at(&self, n: u64) -> Option<ChainState> {
        let n = usize::try_from(n).ok()?;
        let prefix = self.commands.get(..n)?;
        Some(ChainState::from_log(prefix))
    }

    /// The chain state at the end of this log -- the furthest point this
    /// replica made durable.
    pub fn frontier(&self) -> ChainState {
        ChainState::from_log(&self.commands)
    }
}

/// What comparing two replicas' logs found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PairVerdict {
    /// Neither log holds a slot the other does, so nothing was compared.
    ///
    /// This is **not** agreement, and it is a separate variant precisely so
    /// it cannot be read as agreement: a pair of empty logs would otherwise
    /// report the same clean verdict as a pair of matching thousand-slot
    /// ones.
    NoOverlap,
    /// Every slot both logs hold matches. One being a prefix of the other
    /// is ordinary -- replicas lag, and P5 permits it.
    Agree {
        /// How many slots were actually compared. `NonZeroUsize` because a
        /// zero-comparison agreement is [`Self::NoOverlap`], and making
        /// that unrepresentable is cheaper than remembering to check.
        compared: NonZeroUsize,
    },
    /// The logs hold different commands at some slot. `slot` is the
    /// **earliest** such slot: the chain carries a difference forward
    /// forever, so a later disagreement is a consequence, not a cause.
    Differ {
        /// The earliest slot at which the two logs disagree.
        slot: u64,
        /// What the first replica applied there.
        left: Command,
        /// What the second replica applied there.
        right: Command,
    },
}

/// A claim made by the observability path, to be checked against a log.
///
/// This is a divergence report read back in: "the observer says replica R
/// showed hash `h` at height `n`". Confirming both sides of a reported
/// divergence proves the replicas really did diverge; contradicting either
/// side proves the report was manufactured somewhere between the applied
/// log and the observer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Claim {
    /// The replica the observer named.
    pub replica: NodeId,
    /// The chain height it was reported at.
    pub n: u64,
    /// The hash it was reported to have shown.
    pub h: BlockHash,
}

/// What checking a [`Claim`] against a preserved log found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimVerdict {
    /// The log recomputes to exactly the claimed hash at that height. The
    /// observability path reported this replica faithfully.
    Confirmed,
    /// The log recomputes to a different hash at that height. The claim did
    /// not come from this replica's applied log, so the observability path
    /// -- not consensus -- produced it.
    Contradicted {
        /// What the applied log actually folds to at the claimed height.
        recomputed: BlockHash,
    },
    /// The log is shorter than the claimed height, so it holds no evidence
    /// either way. Kept distinct from [`Self::Confirmed`] on purpose: a
    /// replica killed before its snapshot reached that slot must not read
    /// as having corroborated anything.
    ShortLog {
        /// How many slots this replica did make durable.
        log_len: u64,
    },
    /// No snapshot for that replica was preserved at all.
    NoLog,
}

/// What a replica's preserved evidence says about its recorder at one slot
/// (issue #84).
///
/// Three outcomes, deliberately unmergeable -- collapsing the middle two is
/// the easy accident this type exists to prevent: "this replica had no
/// record of the slot" and "this replica recorded nothing at the slot" mean
/// different things about what it witnessed, and neither may borrow the
/// other's rendering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecorderView {
    /// A recorder exists for the slot, and this is its durable ISR summary.
    Summary(IsrSummary<Command>),
    /// The snapshot exists but holds no recorder for this slot: recorders
    /// are created lazily on the first `record` RPC, so this replica's
    /// recorder was never durably touched there. Meaningful in itself --
    /// on the #83 fingerprint it is what "no trace of the earlier step"
    /// looks like.
    NoRecorder,
    /// No snapshot was preserved for this replica at all, so there is no
    /// evidence either way -- the recorder analogue of
    /// [`ClaimVerdict::NoLog`].
    NoSnapshot,
}

/// Why [`Postmortem::disputed_slot`] chose the slot it chose -- rendered
/// into the report so a reader knows what question the recorder section is
/// answering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotChoice {
    /// The earliest slot at which any pair of applied logs differs. The
    /// chain carries a difference forward, so later slots are consequences.
    EarliestDivergence,
    /// The logs agree everywhere they overlap, so the observer's disputed
    /// height is used instead -- the divergence came from the observability
    /// path, and the recorders are read precisely because nothing else is
    /// wrong.
    ObserverClaim,
}

/// Both sides of every reported divergence, as claims to check against the
/// preserved logs.
///
/// A divergence names two `(replica, n, h)` triples and asserts they are
/// incompatible. Adjudicating it means checking *both* against their own
/// replica's applied log, because the interesting outcomes are asymmetric:
/// two confirmations plus differing logs is a real Agreement violation,
/// while a single contradiction points at the observability path and closes
/// the question without implicating consensus.
///
/// Deduplicated, because one replica is usually the witness for several
/// divergences at the same height and checking its log five times says the
/// same thing five times.
pub fn claims_from(divergences: &[Divergence]) -> Vec<Claim> {
    let mut seen = BTreeSet::new();
    let mut claims = Vec::new();
    for divergence in divergences {
        for (replica, h) in [divergence.first, divergence.other] {
            let claim = Claim {
                replica,
                n: divergence.n,
                h,
            };
            if seen.insert((replica, divergence.n, h)) {
                claims.push(claim);
            }
        }
    }
    claims
}

/// The logs recovered from one preserved failure, and the verdicts drawn
/// from them.
#[derive(Debug, Clone)]
pub struct Postmortem {
    /// Keyed by replica, not a `Vec`: one replica can then appear at most
    /// once, so [`Self::pairs`] cannot compare a log against itself. A
    /// self-comparison would be reported as agreement and counted as
    /// evidence, which is the vacuity failure this repo keeps finding --
    /// `queso_conformance::observer` guards the same thing at the sample
    /// level for the same reason.
    logs: BTreeMap<NodeId, AppliedLog>,
    source: PathBuf,
}

impl Postmortem {
    /// Read every preserved replica log under `data_dir`.
    pub fn open(data_dir: &Path) -> anyhow::Result<Self> {
        Ok(Self {
            logs: keyed(AppliedLog::discover(data_dir)?),
            source: data_dir.to_path_buf(),
        })
    }

    /// Adjudicate from logs already in hand -- what the tests use, and what
    /// makes this logic testable without a filesystem.
    pub fn from_logs(logs: Vec<AppliedLog>) -> Self {
        Self {
            logs: keyed(logs),
            source: PathBuf::new(),
        }
    }

    /// The recovered logs, ordered by replica id.
    pub fn logs(&self) -> impl Iterator<Item = &AppliedLog> {
        self.logs.values()
    }

    /// Every unordered pair of replicas and what comparing their logs found.
    pub fn pairs(&self) -> Vec<(NodeId, NodeId, PairVerdict)> {
        let logs: Vec<&AppliedLog> = self.logs.values().collect();
        let mut out = Vec::new();
        for (i, left) in logs.iter().enumerate() {
            for right in &logs[i + 1..] {
                out.push((left.replica, right.replica, compare(left, right)));
            }
        }
        out
    }

    /// What `replica`'s preserved evidence says about its recorder at
    /// `slot` -- see [`RecorderView`] for the three distinct answers.
    pub fn recorder_at(&self, replica: NodeId, slot: u64) -> RecorderView {
        let Some(log) = self.logs.get(&replica) else {
            return RecorderView::NoSnapshot;
        };
        match log.recorders.get(&slot) {
            Some(summary) => RecorderView::Summary(summary.clone()),
            None => RecorderView::NoRecorder,
        }
    }

    /// The slot whose recorder state the report should focus on, and why
    /// (issue #84): the earliest slot at which any pair of applied logs
    /// differs, because the chain carries a difference forward and a later
    /// slot is a consequence -- falling back to the smallest disputed
    /// height the observer claimed when the logs agree, since that
    /// combination means the divergence came from the observability path.
    /// `None` when there is neither: nothing is disputed, so there is no
    /// slot to interrogate.
    ///
    /// The observer's `n` is a chain *height* (commands applied), so the
    /// last command it covers sits at slot `n - 1`; the divergence between
    /// heights `n-1` and `n` was introduced by that slot's command.
    pub fn disputed_slot(&self, claims: &[Claim]) -> Option<(u64, SlotChoice)> {
        let earliest = self
            .pairs()
            .into_iter()
            .filter_map(|(_, _, verdict)| match verdict {
                PairVerdict::Differ { slot, .. } => Some(slot),
                _ => None,
            })
            .min();
        if let Some(slot) = earliest {
            return Some((slot, SlotChoice::EarliestDivergence));
        }
        claims
            .iter()
            .map(|claim| claim.n.saturating_sub(1))
            .min()
            .map(|slot| (slot, SlotChoice::ObserverClaim))
    }

    /// Check one observer claim against the preserved logs.
    pub fn check(&self, claim: Claim) -> ClaimVerdict {
        let Some(log) = self.logs.get(&claim.replica) else {
            return ClaimVerdict::NoLog;
        };
        match log.chain_at(claim.n) {
            None => ClaimVerdict::ShortLog {
                log_len: log.commands.len() as u64,
            },
            Some(state) if state.h == claim.h => ClaimVerdict::Confirmed,
            Some(state) => ClaimVerdict::Contradicted {
                recomputed: state.h,
            },
        }
    }

    /// A human-readable adjudication: what was recovered, what the pairwise
    /// comparison found, how each claim fared, and -- when anything is
    /// disputed -- each replica's recorder (ISR) state around the disputed
    /// slot (issue #84).
    pub fn render(&self, claims: &[Claim]) -> String {
        self.render_with_slot(claims, None)
    }

    /// [`Self::render`], with the recorder section forced onto `slot`
    /// instead of the auto-selected disputed slot -- what
    /// `queso-postmortem --slot` uses to interrogate a slot by hand.
    pub fn render_with_slot(&self, claims: &[Claim], slot: Option<u64>) -> String {
        use std::fmt::Write as _;
        let mut out = String::new();

        if self.source.as_os_str().is_empty() {
            let _ = writeln!(out, "post-mortem over {} preserved log(s)", self.logs.len());
        } else {
            let _ = writeln!(
                out,
                "post-mortem of {}: {} preserved log(s)",
                self.source.display(),
                self.logs.len()
            );
        }
        if self.logs.is_empty() {
            let _ = writeln!(
                out,
                "  no node-*.durable.bin snapshots here -- nothing to adjudicate"
            );
            return out;
        }

        let _ = writeln!(out, "\nreplica  slots   frontier h");
        for log in self.logs.values() {
            let frontier = log.frontier();
            let _ = writeln!(
                out,
                "{:<8} {:<7} 0x{:016x}",
                log.replica.to_string(),
                frontier.n,
                frontier.h
            );
        }

        let _ = writeln!(out, "\napplied logs, pairwise:");
        for (left, right, verdict) in self.pairs() {
            match verdict {
                PairVerdict::NoOverlap => {
                    let _ = writeln!(
                        out,
                        "  {left} vs {right}: NO OVERLAP -- no slot is held by both, so this \
                         pair proves nothing"
                    );
                }
                PairVerdict::Agree { compared } => {
                    let _ = writeln!(
                        out,
                        "  {left} vs {right}: agree on all {compared} shared slot(s)"
                    );
                }
                PairVerdict::Differ {
                    slot,
                    left: l,
                    right: r,
                } => {
                    let _ = writeln!(
                        out,
                        "  {left} vs {right}: DIFFER at slot {slot}\n    {left} applied {l:?} \
                         (digest 0x{:016x})\n    {right} applied {r:?} (digest 0x{:016x})",
                        command_digest(&l),
                        command_digest(&r)
                    );
                }
            }
        }

        if !claims.is_empty() {
            let _ = writeln!(out, "\nobserver claims, against those logs:");
            for &claim in claims {
                let verdict = self.check(claim);
                let _ = write!(
                    out,
                    "  {} at n={} showed 0x{:016x}: ",
                    claim.replica, claim.n, claim.h
                );
                let _ = match verdict {
                    ClaimVerdict::Confirmed => writeln!(out, "CONFIRMED by its applied log"),
                    ClaimVerdict::Contradicted { recomputed } => writeln!(
                        out,
                        "CONTRADICTED -- its applied log folds to 0x{recomputed:016x} there, so \
                         this sample did not come from the log"
                    ),
                    ClaimVerdict::ShortLog { log_len } => writeln!(
                        out,
                        "NO EVIDENCE -- its preserved log is only {log_len} slot(s) long"
                    ),
                    ClaimVerdict::NoLog => {
                        writeln!(out, "NO EVIDENCE -- no snapshot was preserved")
                    }
                };
            }
        }

        // The recorder (ISR) section (issue #84): rendered whenever a slot
        // is in dispute (or forced by the caller), because the applied logs
        // settle *whether* replicas diverged and the recorders are what can
        // say *why* -- the decision lost after being recorded, or never
        // durably recorded at all.
        let focus = slot
            .map(|s| (s, None))
            .or_else(|| self.disputed_slot(claims).map(|(s, why)| (s, Some(why))));
        if let Some((slot, why)) = focus {
            let _ = match why {
                None => writeln!(
                    out,
                    "\nrecorder (ISR) state around slot {slot} (as requested):"
                ),
                Some(SlotChoice::EarliestDivergence) => writeln!(
                    out,
                    "\nrecorder (ISR) state around slot {slot} (the earliest slot at which the \
                     applied logs differ):"
                ),
                Some(SlotChoice::ObserverClaim) => writeln!(
                    out,
                    "\nrecorder (ISR) state around slot {slot} (from the observer's disputed \
                     height -- the applied logs agree, so the divergence came from the \
                     observability path):"
                ),
            };

            // Every replica anything names: the preserved ones, plus any a
            // claim mentions -- so a replica that left no snapshot renders
            // as exactly that, rather than silently vanishing from the
            // section.
            let mut replicas: BTreeSet<NodeId> = self.logs.keys().copied().collect();
            replicas.extend(claims.iter().map(|c| c.replica));

            for s in slot.saturating_sub(RECORDER_WINDOW)..=slot.saturating_add(RECORDER_WINDOW) {
                let _ = writeln!(out, "  slot {s}:");
                for &replica in &replicas {
                    let _ = match self.recorder_at(replica, s) {
                        RecorderView::Summary(summary) => writeln!(
                            out,
                            "    {replica}: step={} first={} prior_agg={}",
                            summary.step,
                            render_proposal(&summary.first),
                            render_proposal(&summary.prior_agg),
                        ),
                        RecorderView::NoRecorder => writeln!(
                            out,
                            "    {replica}: no recorder -- its recorder was never durably \
                             touched at this slot"
                        ),
                        RecorderView::NoSnapshot => writeln!(
                            out,
                            "    {replica}: NO EVIDENCE -- no snapshot was preserved"
                        ),
                    };
                }
            }
        }
        out
    }
}

/// How many slots either side of the disputed one the recorder section
/// covers. A window, not just the slot itself, because #83's mechanism
/// lived in the relationship between a slot and its neighbours (a probe at
/// the restart frontier, the pre-crash decision one step earlier).
const RECORDER_WINDOW: u64 = 2;

/// A proposal as the recorder section prints it: `nil` for none, and the
/// reserved maximum priority as `H` -- the form every #83-family
/// fingerprint is written in, and unreadable as `18446744073709551615`.
fn render_proposal(p: &Option<Proposal<Command>>) -> String {
    match p {
        None => "nil".to_string(),
        Some(p) if p.priority == H => format!("<prio=H from={} {:?}>", p.origin, p.value),
        Some(p) => format!("<prio={} from={} {:?}>", p.priority, p.origin, p.value),
    }
}

/// Index logs by replica, so a replica cannot appear twice.
fn keyed(logs: Vec<AppliedLog>) -> BTreeMap<NodeId, AppliedLog> {
    logs.into_iter().map(|log| (log.replica, log)).collect()
}

/// Compare two logs slot by slot over the range both hold.
fn compare(left: &AppliedLog, right: &AppliedLog) -> PairVerdict {
    let overlap = left.commands.len().min(right.commands.len());
    let Some(compared) = NonZeroUsize::new(overlap) else {
        return PairVerdict::NoOverlap;
    };
    for slot in 0..overlap {
        if left.commands[slot] != right.commands[slot] {
            return PairVerdict::Differ {
                slot: slot as u64,
                left: left.commands[slot].clone(),
                right: right.commands[slot].clone(),
            };
        }
    }
    PairVerdict::Agree { compared }
}

#[cfg(test)]
mod tests {
    use super::*;
    use queso_conformance::observer::Divergence;
    use queso_smr::{ClientId, Durable};

    fn put(client: u32, seq: u64, key: u32, value: i64) -> Command {
        Command::Put {
            client: ClientId(client),
            seq,
            key,
            value,
        }
    }

    fn log(replica: u32, commands: Vec<Command>) -> AppliedLog {
        AppliedLog {
            replica: NodeId(replica),
            commands,
            recorders: BTreeMap::new(),
        }
    }

    fn summary(step: u64, first: Option<Proposal<Command>>) -> IsrSummary<Command> {
        IsrSummary {
            step,
            first,
            prior_agg: None,
        }
    }

    fn proposal(priority: u64, origin: u32, command: Command) -> Proposal<Command> {
        Proposal {
            value: command,
            priority,
            origin: NodeId(origin),
        }
    }

    fn run(len: i64) -> Vec<Command> {
        (0..len).map(|i| put(0, i as u64, 0, i)).collect()
    }

    /// Falsifier: have `compare` stop at the first slot rather than walking
    /// the overlap and this still passes, so the count is asserted too --
    /// "agree" is only worth what it compared.
    #[test]
    fn identical_logs_agree_on_every_slot_they_share() {
        let pm = Postmortem::from_logs(vec![log(0, run(8)), log(1, run(8))]);
        assert_eq!(
            pm.pairs(),
            vec![(
                NodeId(0),
                NodeId(1),
                PairVerdict::Agree {
                    compared: NonZeroUsize::new(8).unwrap()
                }
            )]
        );
    }

    /// The chain carries a difference forward forever, so every slot after
    /// the first disagreement also disagrees. Reporting a later one would
    /// point a root-cause hunt at a consequence.
    ///
    /// Falsifier: iterate the overlap in reverse and this reports slot 6.
    #[test]
    fn the_earliest_differing_slot_is_the_one_reported() {
        let mut theirs = run(8);
        theirs[3] = put(9, 3, 0, 300);
        theirs[6] = put(9, 6, 0, 600);

        let pm = Postmortem::from_logs(vec![log(0, run(8)), log(1, theirs)]);
        let (_, _, verdict) = pm.pairs().remove(0);
        match verdict {
            PairVerdict::Differ { slot, left, right } => {
                assert_eq!(slot, 3);
                assert_eq!(left, put(0, 3, 0, 3));
                assert_eq!(right, put(9, 3, 0, 300));
            }
            other => panic!("expected a divergence at slot 3, got {other:?}"),
        }
    }

    /// Replicas lag by design (P5). A short log that matches as far as it
    /// goes is the normal case, and calling it a divergence would make the
    /// tool useless on every real failure -- the disputed run had one
    /// replica 75 slots behind another.
    #[test]
    fn a_shorter_log_is_a_prefix_not_a_divergence() {
        let pm = Postmortem::from_logs(vec![log(0, run(8)), log(1, run(3))]);
        assert_eq!(
            pm.pairs()[0].2,
            PairVerdict::Agree {
                compared: NonZeroUsize::new(3).unwrap()
            }
        );
    }

    /// The anti-vacuity case, and the reason `Agree` carries a
    /// `NonZeroUsize`: a cluster that died before persisting anything must
    /// not read as a cluster that was checked and found consistent.
    ///
    /// Falsifier: replace `NoOverlap` with `Agree { compared: 0 }` and this
    /// stops compiling -- which is the point.
    #[test]
    fn logs_with_nothing_in_common_are_no_overlap_not_agreement() {
        let pm = Postmortem::from_logs(vec![log(0, vec![]), log(1, run(4))]);
        assert_eq!(pm.pairs()[0].2, PairVerdict::NoOverlap);
        assert!(
            pm.render(&[]).contains("proves nothing"),
            "the report must say so, not just the type: {}",
            pm.render(&[])
        );
    }

    #[test]
    fn every_pair_is_compared_not_just_consecutive_ones() {
        let mut odd = run(6);
        odd[1] = put(9, 1, 0, 100);
        // 0 and 2 agree; 1 disagrees with both.
        let pm = Postmortem::from_logs(vec![log(0, run(6)), log(1, odd), log(2, run(6))]);
        let pairs = pm.pairs();
        assert_eq!(pairs.len(), 3, "3 replicas make 3 unordered pairs");
        let differing: Vec<_> = pairs
            .iter()
            .filter(|(_, _, v)| matches!(v, PairVerdict::Differ { .. }))
            .map(|(a, b, _)| (*a, *b))
            .collect();
        assert_eq!(
            differing,
            vec![(NodeId(0), NodeId(1)), (NodeId(1), NodeId(2))]
        );
    }

    /// A claim the log reproduces: the observability path reported this
    /// replica faithfully, so a divergence involving it is real on this side.
    #[test]
    fn a_claim_the_log_reproduces_is_confirmed() {
        let commands = run(10);
        let truth = ChainState::from_log(&commands[..6]);
        let pm = Postmortem::from_logs(vec![log(0, commands)]);
        assert_eq!(
            pm.check(Claim {
                replica: NodeId(0),
                n: 6,
                h: truth.h
            }),
            ClaimVerdict::Confirmed
        );
    }

    /// The other decisive direction: a hash no prefix of the log folds to
    /// was manufactured between the log and the observer. This is the
    /// verdict that would have closed #73 as an observability artifact.
    #[test]
    fn a_claim_the_log_contradicts_names_what_the_log_actually_folds_to() {
        let commands = run(10);
        let truth = ChainState::from_log(&commands[..6]);
        let pm = Postmortem::from_logs(vec![log(0, commands)]);
        assert_eq!(
            pm.check(Claim {
                replica: NodeId(0),
                n: 6,
                h: 0xdead_beef
            }),
            ClaimVerdict::Contradicted {
                recomputed: truth.h
            }
        );
    }

    /// A replica killed before its snapshot reached the disputed slot has
    /// no evidence to offer. Folding that into "confirmed" would let a
    /// short log corroborate a claim it never saw.
    ///
    /// Falsifier: make `chain_at` clamp to the log length instead of
    /// returning `None` and this reports `Confirmed` for the frontier hash.
    #[test]
    fn a_claim_past_the_end_of_a_log_is_not_confirmed() {
        let commands = run(4);
        let pm = Postmortem::from_logs(vec![log(0, commands.clone())]);
        assert_eq!(
            pm.check(Claim {
                replica: NodeId(0),
                n: 9,
                h: ChainState::from_log(&commands).h
            }),
            ClaimVerdict::ShortLog { log_len: 4 }
        );
    }

    /// Both sides, or the adjudication is one-sided: a divergence where
    /// only the witness is checked can never be attributed to the
    /// observability path, which is the outcome #73 most needs to be able
    /// to reach.
    ///
    /// Falsifier: emit only `divergence.other` and this reports one claim.
    #[test]
    fn a_divergence_becomes_a_claim_against_each_replica_it_names() {
        let claims = claims_from(&[Divergence {
            n: 7,
            first: (NodeId(0), 0xaaaa),
            other: (NodeId(2), 0xbbbb),
            observed_at: 1,
        }]);
        assert_eq!(
            claims,
            vec![
                Claim {
                    replica: NodeId(0),
                    n: 7,
                    h: 0xaaaa
                },
                Claim {
                    replica: NodeId(2),
                    n: 7,
                    h: 0xbbbb
                },
            ]
        );
    }

    /// One replica is the witness for every divergence at its height, so
    /// its side repeats. Checking the same log against the same hash twice
    /// prints the same verdict twice, which reads as corroboration -- the
    /// same misreading the observer's own duplicate reports caused.
    #[test]
    fn a_repeated_side_is_checked_once() {
        let claims = claims_from(&[
            Divergence {
                n: 7,
                first: (NodeId(0), 0xaaaa),
                other: (NodeId(1), 0xbbbb),
                observed_at: 1,
            },
            Divergence {
                n: 7,
                first: (NodeId(0), 0xaaaa),
                other: (NodeId(2), 0xcccc),
                observed_at: 2,
            },
        ]);
        assert_eq!(claims.len(), 3, "n0 once, n1 once, n2 once: {claims:?}");
        assert_eq!(claims.iter().filter(|c| c.replica == NodeId(0)).count(), 1);
    }

    #[test]
    fn a_claim_about_a_replica_that_left_no_snapshot_is_not_confirmed() {
        let pm = Postmortem::from_logs(vec![log(0, run(4))]);
        assert_eq!(
            pm.check(Claim {
                replica: NodeId(2),
                n: 1,
                h: 0
            }),
            ClaimVerdict::NoLog
        );
    }

    /// Issue #84's anti-vacuity requirement, at the type and at the text:
    /// "a recorder exists and says X", "no recorder for that slot", and
    /// "no snapshot for that replica" are three different answers, and the
    /// report must keep them apart -- a missing recorder rendering as an
    /// empty one would make "this replica had no record of the slot"
    /// indistinguishable from "this replica recorded nothing at the slot".
    ///
    /// Falsifier: have `recorder_at` return
    /// `Summary(IsrSummary::default())`-ish for an absent slot key (or
    /// `NoRecorder` for an absent snapshot) and the respective assertion
    /// fails.
    #[test]
    fn a_missing_recorder_a_missing_snapshot_and_a_recorder_are_three_answers() {
        let mut with_recorder = log(0, run(4));
        with_recorder
            .recorders
            .insert(2, summary(8, Some(proposal(7, 0, put(0, 2, 0, 2)))));
        let pm = Postmortem::from_logs(vec![with_recorder, log(1, run(4))]);

        assert!(matches!(
            pm.recorder_at(NodeId(0), 2),
            RecorderView::Summary(_)
        ));
        assert_eq!(pm.recorder_at(NodeId(0), 3), RecorderView::NoRecorder);
        assert_eq!(pm.recorder_at(NodeId(9), 2), RecorderView::NoSnapshot);

        // And the report says three different things. Forcing slot 2 so the
        // section renders even though the logs agree; naming n9 in a claim
        // so its absent snapshot is in scope.
        let claim = Claim {
            replica: NodeId(9),
            n: 3,
            h: 0,
        };
        let text = pm.render_with_slot(&[claim], Some(2));
        assert!(text.contains("n0: step=8"), "{text}");
        assert!(
            text.contains("n1: no recorder -- its recorder was never durably touched"),
            "{text}"
        );
        assert!(
            text.contains("n9: NO EVIDENCE -- no snapshot was preserved"),
            "{text}"
        );
    }

    /// The earliest divergent slot outranks the observer's claim: the chain
    /// carries a difference forward, so the claimed height is downstream of
    /// the cause.
    ///
    /// Falsifier: consult claims before pairs in `disputed_slot` and this
    /// reports slot 6 (the claim's n=7 minus one).
    #[test]
    fn the_recorder_focus_is_the_earliest_divergent_slot_not_the_claimed_height() {
        let mut theirs = run(8);
        theirs[3] = put(9, 3, 0, 300);
        let pm = Postmortem::from_logs(vec![log(0, run(8)), log(1, theirs)]);
        let claim = Claim {
            replica: NodeId(0),
            n: 7,
            h: 0,
        };
        assert_eq!(
            pm.disputed_slot(&[claim]),
            Some((3, SlotChoice::EarliestDivergence))
        );
    }

    /// When the logs agree, the observer's disputed height is the only slot
    /// worth interrogating -- and the report must say the divergence came
    /// from the observability path, because that is what agreement plus a
    /// disputed claim means. The observer's `n` is a height; the command
    /// that introduced the disputed state sits at slot `n - 1`.
    #[test]
    fn agreeing_logs_fall_back_to_the_observers_height_and_say_why() {
        let pm = Postmortem::from_logs(vec![log(0, run(8)), log(1, run(8))]);
        let claim = Claim {
            replica: NodeId(0),
            n: 5,
            h: 0xdead,
        };
        assert_eq!(
            pm.disputed_slot(&[claim]),
            Some((4, SlotChoice::ObserverClaim))
        );
        let text = pm.render(&[claim]);
        assert!(
            text.contains("the divergence came from the observability path"),
            "{text}"
        );
    }

    /// Nothing disputed, nothing to interrogate: the section must be absent
    /// rather than rendered around some arbitrary slot -- a recorder dump on
    /// every clean adjudication would bury the verdicts that matter.
    #[test]
    fn a_clean_adjudication_renders_no_recorder_section() {
        let pm = Postmortem::from_logs(vec![log(0, run(8)), log(1, run(8))]);
        assert_eq!(pm.disputed_slot(&[]), None);
        assert!(!pm.render(&[]).contains("recorder (ISR) state"));
    }

    /// The window covers the disputed slot's neighbourhood (#83's mechanism
    /// lived between a slot and its neighbours), clamped at slot 0 rather
    /// than underflowing.
    #[test]
    fn the_recorder_window_spans_the_disputed_slot_and_clamps_at_zero() {
        let pm = Postmortem::from_logs(vec![log(0, run(8))]);
        let text = pm.render_with_slot(&[], Some(1));
        for s in 0..=3 {
            assert!(
                text.contains(&format!("slot {s}:")),
                "missing slot {s}: {text}"
            );
        }
        assert!(!text.contains("slot 4:"), "{text}");
    }

    /// The reserved maximum priority renders as `H`, the form every
    /// #83-family fingerprint is written in -- not as u64::MAX's twenty
    /// digits.
    #[test]
    fn the_reserved_priority_renders_as_h() {
        let mut mine = log(0, run(4));
        mine.recorders
            .insert(2, summary(4, Some(proposal(H, 0, put(0, 2, 0, 2)))));
        let pm = Postmortem::from_logs(vec![mine]);
        let text = pm.render_with_slot(&[], Some(2));
        assert!(text.contains("prio=H"), "{text}");
        assert!(!text.contains("18446744073709551615"), "{text}");
    }

    /// The loader against real files written by the real `Store`.
    ///
    /// `Durable`'s fields are `pub(crate)` to `queso-smr`, so a unit test
    /// can only write *empty* logs -- which is exactly enough to pin down
    /// what this test is for: which files become evidence. Non-empty logs
    /// come from a real cluster, in `tests/postmortem.rs`.
    #[test]
    fn discover_reads_snapshot_files_and_nothing_else_in_the_directory() {
        let dir = tempfile::tempdir().expect("tempdir");
        for id in [0u32, 2, 7] {
            Store::new(dir.path(), NodeId(id))
                .expect("store")
                .save(&Durable::default(), 0)
                .expect("save");
        }
        // Everything else a preserved seed directory really contains: the
        // half-written sibling the atomic-rename scheme can leave behind,
        // the captured stderr, and the run log.
        std::fs::write(dir.path().join("node-9.durable.bin.tmp"), b"garbage").expect("tmp");
        std::fs::write(dir.path().join("node-0.err"), b"stderr").expect("err");
        std::fs::write(dir.path().join("soak.log"), b"log").expect("log");

        let logs = AppliedLog::discover(dir.path()).expect("discover");
        assert_eq!(
            logs.iter().map(|l| l.replica).collect::<Vec<_>>(),
            vec![NodeId(0), NodeId(2), NodeId(7)]
        );
    }

    /// A snapshot that will not parse is the loudest thing this directory
    /// can contain: the process was killed mid-something, and *that* is the
    /// failure being investigated. Reporting one fewer log and carrying on
    /// would shrink the evidence set silently -- and a two-replica
    /// comparison that should have been three-replica still looks clean.
    ///
    /// Falsifier: swallow the load error (`filter_map(|r| r.ok().flatten())`)
    /// and this passes with two logs.
    #[test]
    fn a_corrupt_snapshot_is_an_error_not_a_quietly_smaller_evidence_set() {
        let dir = tempfile::tempdir().expect("tempdir");
        for id in [0u32, 1] {
            Store::new(dir.path(), NodeId(id))
                .expect("store")
                .save(&Durable::default(), 0)
                .expect("save");
        }
        std::fs::write(dir.path().join("node-2.durable.bin"), b"truncated").expect("corrupt");

        let err =
            AppliedLog::discover(dir.path()).expect_err("a corrupt snapshot must not be silent");
        assert!(
            err.to_string().contains("magic") || err.to_string().contains("too short"),
            "the error should say what is wrong with the file: {err}"
        );
    }

    #[test]
    fn a_data_dir_with_no_snapshots_is_an_empty_recovery_not_an_error() {
        let dir = tempfile::tempdir().expect("tempdir");
        let pm = Postmortem::open(dir.path()).expect("open");
        assert_eq!(pm.logs().count(), 0);
        assert!(pm.render(&[]).contains("nothing to adjudicate"));
    }

    /// Pointing the tool at a path that does not exist must fail loudly.
    /// `Store::new` calls `create_dir_all`, so without the guard a typo'd
    /// path is silently created and reported as "no evidence found" -- the
    /// worst possible answer, since it is indistinguishable from a real
    /// empty data dir.
    #[test]
    fn a_missing_data_dir_is_an_error_not_an_empty_recovery() {
        let dir = tempfile::tempdir().expect("tempdir");
        let missing = dir.path().join("seed-404");
        assert!(AppliedLog::load(&missing, NodeId(0)).is_err());
        assert!(
            !missing.exists(),
            "the loader must not create what it reads"
        );
    }
}
