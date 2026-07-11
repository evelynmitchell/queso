//! The active **proposer** role (Algorithm 4): threshold logical clock, the
//! four phases per round, gathering a quorum of recorder replies per step,
//! and the decision rule.
//!
//! # Reading Algorithm 4 off the page
//!
//! The paper's pseudocode (`repeat { ... }`) reads as a blocking loop: send
//! requests, *await* a quorum, branch on phase, maybe advance `s`, repeat.
//! [`Proposer`] is the same state machine turned inside-out into
//! [`queso_sim::node::Node`] callbacks, since the harness is a discrete-event
//! kernel with no blocking calls: `begin_step` is the "prepare proposals /
//! send record(s, p_i) to each recorder" prologue, `on_response` is what
//! runs each time one more reply arrives (checking, after every arrival,
//! whether a quorum has now been reached -- the "await" is realized as "do
//! nothing interesting until `responses.len()` crosses the quorum
//! threshold"), and `on_timer` is the retransmission mechanism that makes
//! progress possible despite the adversary dropping some of a step's
//! requests or replies (see "Retries" below).
//!
//! # Where each phase's action lives (Algorithm 4, `s mod 4`)
//!
//! Confirmed against the paper's own box (not just the earlier prose
//! summary) that `s <- s + 1` sits at the *same* indentation as the four
//! `if s mod 4 = k` blocks, i.e. it fires once per successful step
//! (regardless of which phase), not only after phase 3:
//!
//! - **Phase 0** (`s mod 4 = 0`, "propose"): draw a fresh random priority
//!   *per recorder* (§4.2.4 "Proposal randomization" -- yes, literally a
//!   different priority per recipient; see [`draw_priority`]), *unless*
//!   this is round 1 (`s = 4`) and `self` is the slot's designated leader
//!   (§4.2.5), in which case every recorder instead gets the reserved
//!   priority [`H`] verbatim. Either way, once a quorum of replies is in:
//!   first check the fast-path decision ([`fast_path_value`]) -- if every
//!   reply's `first` is `H`-priority, decide right now and skip the rest of
//!   the round; otherwise set `p <- best_j(f'_j)` over the quorum's
//!   first-value replies exactly as Phase 2 always did.
//! - **Phase 1** (`s mod 4 = 1`, "spread E"): no computation; simply
//!   re-sends the unchanged `p` at the next step, letting the recorders'
//!   ISRs aggregate it.
//! - **Phase 2** (`s mod 4 = 2`, "gather E, spread C"): decide-check --
//!   compare `p` against `best_j(a'_j)` (this step's gathered prior-step
//!   aggregate, i.e. `best(E)`); if equal, `p` is both `best(E)` and
//!   `best(U)` (see the safety argument below) and we deliver
//!   `p.value` as the decision. Otherwise `p` is left unchanged.
//! - **Phase 3** (`s mod 4 = 3`, "gather C"): `p <- best_j(a'_j)` --
//!   unconditionally overwrite `p` with the best gathered common proposal,
//!   which becomes the next round's phase-0 starting template.
//!
//! Every successful step (all quorum replies report `s'_j = s`, our own
//! step) advances `s` by exactly one, *unless* a decision was just
//! delivered. If instead *any* quorum reply reports `s'_j > s`, we have
//! fallen behind some faster proposer; catch up by adopting that reply's
//! `(s'_j, f'_j)` pair as our new `(s, p)` (§4.2.4 "Proposer catch-up").
//!
//! # Why this preserves Agreement under full asynchrony (the crux)
//!
//! The one property this construction must reproduce from the abstract
//! `tcast`-based algorithm is: whenever proposer `i` locally decides `p`
//! (because `p = best(E_i)` in some round), *every* proposer's current or
//! future candidate is guaranteed to be `>= p` forever after -- so no other
//! proposer can ever decide a different value. Concretely, this reduces to
//! a fact about *majority intersection*, applied twice per round:
//!
//! 1. Suppose `i` completes phase *k* (any phase, not just 0) with a
//!    genuine majority quorum `Q` all reporting `s'_j = s` -- i.e. every
//!    recorder in `Q` incorporated `i`'s `p` into its `A_c` while at
//!    exactly step `s`.
//! 2. `i` (and only `i`, sequentially) then queries a majority quorum `M`
//!    at step `s+1` (the very next step) for `A_p`.
//! 3. `Q` and `M` are both true majorities of the *same* fixed `n`, so
//!    `Q ∩ M` is non-empty -- pick any `r` in that intersection.
//! 4. Because `r ∈ M`, `r`'s `record` calls have carried its internal step
//!    monotonically from whatever it was up to *exactly* `s+1` (never
//!    skipping past it -- skipping is only possible when the *first*
//!    `s > S` call a recorder sees jumps by more than one, and the paper's
//!    ISR (Algorithm 3) *always* records the exact incoming step, so
//!    landing precisely on `s+1` is only possible via a call whose own `s`
//!    was `s+1`). Combined with `r ∈ Q` (so `r`'s state at step `s`
//!    already had `p` folded into `A_c`), this forces the transition into
//!    `s+1` to be the "exactly one step forward" case in Algorithm 3, which
//!    carries `A_c` (>= `p`) into `A_p` intact. A recorder that instead
//!    skipped past `s+1` under some other, faster proposer could never
//!    later be observed reporting exactly `s+1` (steps only increase), so
//!    it simply cannot be a member of `M` -- it does not corrupt the
//!    argument, it is just excluded from it.
//! 5. Therefore `best_j(a'_j)` over `M` is always `>= p`: `best(E) >= p`
//!    whenever `i` reaches its own phase-2 gather at the step immediately
//!    following its own successful phase-1 spread of `p`. `p` can only
//!    equal `best(E)` (triggering a decision) when no *other* proposal beat
//!    it into some intersecting recorder's aggregate -- so a false-positive
//!    decision (declaring `p` decided when something else also reached
//!    majority) is structurally impossible, and the same argument, applied
//!    one step later between phase 2's spread and phase 3's gather,
//!    guarantees phase 3's `best_j(a'_j)` is likewise `>= p`, so the next
//!    round always starts from a candidate at least as good. This is the
//!    concrete-protocol analogue of the abstract algorithm's
//!    `U ⊆ C_j ⊆ E_i` cross-replica subset invariant (§4.1.3), reconstructed
//!    here from majority intersection over *consecutive* ISR steps instead
//!    of `tcast`'s explicit set inclusion.
//!
//! # The leader fast path (§4.2.5, D1) never risks Agreement
//!
//! [`Proposer`] optionally carries a `leader: Option<NodeId>` (set once, at
//! construction -- see `Proposer::new`). This changes exactly two things,
//! both confined to round 1 (`s = 4`), both in [`begin_step`] and
//! [`process_phase`]'s phase-0 arm:
//!
//! 1. If `self` *is* the leader, its phase-0 proposal carries [`H`] (the
//!    reserved maximum priority) instead of a random draw.
//! 2. Every proposer -- leader or not -- checks [`fast_path_value`] after
//!    gathering its phase-0 quorum: if every reply's `first` is
//!    `H`-priority, decide immediately, without ever reaching phase 1.
//!
//! Everything else -- the ISR, the recorder, phases 1-3, catch-up, retries
//! -- is completely unmodified, and a slot built with `leader: None`
//! reproduces Phase 2's behavior exactly (no leaderless draw is ever `H`,
//! so `fast_path_value` can never return `Some`). This matters for the
//! safety argument: the fast path is not a second, separately-justified
//! decision rule bolted onto the side of the one proven above -- it is a
//! special case of the *same* one, reached one phase early only because
//! `H` happens to be unbeatable. Concretely (this is the paper's Lemma
//! C.10, reproduced here because it is the crux of why fast and leaderless
//! decisions can never disagree):
//!
//! - `H` is `u64::MAX`, strictly greater than anything [`draw_priority`]
//!   can ever produce, and only ever attached by round 1's `leader`. So if
//!   a quorum `Q` (a genuine majority) all report `first.priority == H`,
//!   more than `n/2` recorders wrote *the leader's own proposal* into
//!   `F[4]` -- and, because `crate::isr::Isr::record` writes `F[s]` only
//!   once per step (steps never regress -- the same fact point 4 above
//!   relies on), that write can never be overwritten. Deciding here is
//!   therefore not a guess; it is already true forever.
//! - Take *any* other proposer's own round-1 phase-0 quorum `M` (also a
//!   genuine majority of the same fixed `n`). `Q` and `M` must intersect
//!   (any two majorities of a fixed universe do), so `M` contains at least
//!   one recorder whose `F[4]` is the leader's proposal.
//!   - If that other proposer's quorum is *also* all-`H`, it fast-decides
//!     too -- and, because only one leader's proposal can ever carry `H`,
//!     necessarily the *same* value (see `fast_path_value`'s
//!     `debug_assert!`).
//!   - Otherwise, it does not fast-decide, but the leader's proposal is
//!     still one of the replies its `best_j(f'_j)` selects from -- and
//!     since `H` cannot be beaten, `best_j` is forced to pick it. That
//!     proposer then spreads the leader's proposal into phase 1 exactly as
//!     if it were an ordinary round-1 candidate, and the "why this
//!     preserves Agreement" argument above (deliberately proven for "any
//!     phase, not just 0") takes over unmodified from there: once spread,
//!     a value can only be matched or beaten by any future quorum, never
//!     lost, and nothing can beat `H`.
//!
//! So a fast-path decision and a leaderless decision for the same slot are
//! never *merely consistent* -- they are provably the same value, because
//! every quorum that does not itself see all-`H` replies is still forced
//! (not just permitted) to carry the leader's proposal forward, *whenever
//! that quorum intersects one that did*. A content-aware adversary that
//! prevents any quorum from ever forming with all-`H` replies (§4.2.5:
//! always possible in principle -- e.g. delivering the leader's proposal to
//! every `E` set but no `U` set, or blocking it outright) simply means the
//! premise of the argument above never triggers *anywhere*, for *any*
//! proposer: no quorum ever sees all-`H`, so [`fast_path_value`] never
//! returns `Some`, [`is_leader`]'s branch in `begin_step` never matters to
//! the outcome, and the round proceeds exactly as an ordinary leaderless
//! one -- already proven safe on its own terms, with or without a leader's
//! proposal in the mix. Either the fast path fires and is provably
//! consistent with everything else, or it never fires at all and
//! contributes nothing but an extra (harmless) candidate; there is no
//! third, unsafe outcome. Only latency is ever at stake, never Agreement.
//!
//! # Retries
//!
//! A step's outbound `record` requests (or their replies) can be delayed,
//! reordered, or dropped by the adversary. [`Proposer`] schedules a retry
//! timer, `TimerId(s)` (the *current* step number itself, reused as the
//! timer's identity -- see [`retry_timer_id`]), whenever it begins a step;
//! if that timer fires before a quorum has formed, it re-sends only to
//! recorders that have not yet replied, using the *same* proposal content
//! recorded for them at [`begin_step`] time (never a freshly-redrawn
//! priority -- see [`Proposer::sent`]), and reschedules itself. Because the
//! timer id is the step number, a stale timer belonging to an
//! already-superseded step is trivially recognized and ignored (`s.0 !=
//! self.step`) rather than needing a separate generation counter.
//!
//! Retries are **unbounded**, with exponential backoff ([`retry_backoff_delay`])
//! capped at a maximum spacing (issue #13: the previous design, a hard
//! `MAX_RETRIES_PER_STEP` cap after which a proposer permanently "parked"
//! itself, could leave a slot stalled forever even after the network
//! healed or a majority became reachable again -- a genuine liveness bug,
//! since nothing would ever wake the parked proposer back up). A proposer
//! legitimately cannot make progress when fewer than a majority of
//! recorders are reachable at all (P11/O4: safety holds, liveness may
//! simply stall for as long as that holds) -- but the fix is to keep
//! retrying, cheaply, forever, not to give up. Backoff bounds the *rate*
//! of retries (so a permanently-unreachable majority does not spin the
//! kernel), while never bounding their *count* (so a network that heals
//! at tick 10,000,000 still resumes the slot).
//!
//! # Hedging (§5.1-5.2, P15/P16, D2)
//!
//! By default (`Proposer::new`, no [`Proposer::with_hedging`] call) a
//! proposer activates -- sends its first `record` requests -- the instant
//! [`Proposer::start`] runs, exactly reproducing Phase 3's unconditional
//! δ=0 "every proposer active from round 1" behavior. [`Proposer::with_hedging`]
//! opts a proposer into the paper's staggered *delayed-activation schedule*
//! instead: the caller assigns each replica a position in the schedule (the
//! designated leader, if any, always first with delay 0; §5.2) and an
//! `activation_delay` derived from it (`(rank) * δ` for a single configured
//! base delay δ -- see `crate::concrete::ConcreteCluster::new_with_schedule`
//! for where that arithmetic lives; this module only ever sees the final
//! per-replica delay, not the schedule-construction policy).
//!
//! `start` on a hedged proposer does not call [`begin_step`] immediately;
//! it arms a one-shot [`HEDGE_TIMER`] for `activation_delay` ticks out.
//! When that timer fires ([`Proposer::maybe_activate_after_hedge`]), the
//! proposer checks `local_recorder_step` -- a handle to *this replica's own,
//! co-located* recorder's most-recently-observed ISR step for this slot,
//! updated by the driver (`crate::concrete::ReplicaNode::on_message`) every
//! time that recorder answers *any* proposer's `record` request, local or
//! remote. This is a genuinely free (no extra messages) signal: it is
//! in-process state this replica already has for an unrelated reason
//! (being a passive recorder), simply read instead of ignored, which is
//! exactly what makes the "leader-only, no wasted proposals" `O(n)`
//! synchrony case (D2) possible -- a suppressed backup's proposer sends
//! *zero* bytes.
//!
//! - If `local_recorder_step` shows *fresh* evidence that the relevant step
//!   has already been driven past round 1's first step *since the last time
//!   this proposer checked* (§5.2: "has not by then seen evidence that some
//!   other proposer ... has already driven the relevant step to
//!   completion"), the proposer stays passive and rearms the hedge timer
//!   for another [`HEDGE_RECHECK_TICKS`] to check again later.
//! - Otherwise -- no progress was ever observed, *or* the last-observed
//!   progress has since stalled (the recorder step did not move further
//!   between two consecutive checks) -- the proposer activates via
//!   [`begin_step`], exactly as an unhedged proposer would.
//!
//! # Why no δ (P15) can cause a permanent stall
//!
//! The crux is that "stay passive" is re-earned at every recheck, not
//! granted once: a hedged proposer only *keeps* deferring while it keeps
//! observing *new* forward motion on `local_recorder_step`. A healthy,
//! progressing leader keeps producing that new motion (a live proposer
//! completing steps keeps advancing the recorder's `S`), so backups
//! correctly stay out of its way -- but the moment that motion stops (the
//! leader crashed, was DoS'd, or was partitioned away from this replica),
//! the *very next* recheck sees no advance since the previous one and
//! activates unconditionally. Since step numbers are bounded in practice
//! (A7), the number of "genuinely still progressing" rechecks before either
//! a decision or a stall is itself bounded, so total suppression time is
//! always finite regardless of δ: `activation_delay + k * HEDGE_RECHECK_TICKS`
//! for some bounded `k`. This holds even for δ=0 (no hedge timer is ever
//! armed at all -- see `start`), absurdly large δ (the very first recheck
//! after that long a wait either finds a decided/progressing slot, in which
//! case great, no wasted work was needed, or a stalled one, in which case it
//! activates immediately), and per-replica-misconfigured schedules (every
//! replica's suppression is independently re-earned, so no replica can be
//! held passive by another replica's misconfiguration -- only by *itself*
//! observing genuine, ongoing progress). A replica with no
//! `local_recorder_step` wired up at all (hedged with a bare delay and no
//! progress oracle) simply activates unconditionally once its one delay
//! elapses -- also bounded, trivially.
//!
//! One deliberate, safety-motivated limitation worth calling out: a
//! recorder's step never advances past round 1's first step ([`FIRST_ROUND_STEP`])
//! from fast-path activity alone ([`fast_path_value`] lets a proposer decide
//! *without* the recorder-visible step ever moving beyond it), so a hedged
//! backup cannot distinguish "the leader's fast-path proposal reached this
//! recorder's `F[4]` but never reached a majority" from "it reached a
//! majority and the slot is already decided" using `local_recorder_step`
//! alone -- and it would be *unsafe* to treat merely seeing `F[4]` locally
//! as proof of the latter (a content-aware adversary could deliver the
//! leader's proposal to a minority of recorders while still defeating the
//! fast path everywhere, exactly as in `crate::proposer`'s fast-path safety
//! argument above; a backup that wrongly stood down in that case, and every
//! other backup reasoning the same way from its own recorder, could
//! livelock -- N6). So a hedged backup errs on the side of eventually
//! joining in once its own schedule slot comes up, even after a
//! successful leader fast-path decision elsewhere: hedging's message
//! savings in that case are a *bounded head start* (no wasted work happens
//! before a backup's delay elapses), not a permanent guarantee that
//! later-scheduled replicas never do any work for an already-decided slot.
//! A caller that wants every replica to avoid ever redundantly deciding a
//! slot it has no independent need to confirm (e.g. because it can instead
//! learn the outcome from a replicated log) should simply not start a
//! proposer for that replica at all -- an orthogonal, driver-level policy
//! decision outside this module's scope.
//!
//! This is deliberately **not** a safety mechanism: it only ever changes
//! *when* [`begin_step`] first runs for a given proposer, never anything
//! about what a step, a quorum, a decision, or catch-up mean once activated
//! -- all of that is the same, unmodified machinery documented above. A
//! hedged proposer that activates "late" (having deferred, or having waited
//! out a long δ) simply runs Algorithm 4 from round 1, phase 0, same as
//! ever; the majority-intersection argument for Agreement never mentions
//! *when* any proposer started, only what genuine quorums it forms once it
//! does.

use queso_sim::ids::{NodeId, TimerId};
use queso_sim::node::NodeCtx;
use rand::Rng;
use std::cell::Cell;
use std::collections::BTreeMap;
use std::rc::Rc;

use crate::proposal::Proposal;
use crate::rpc::{ConcreteMsg, RecordRequest, RecordResponse};

/// `H`, the reserved maximum priority (§4.2.5, Appendix A). Two roles:
///
/// 1. It bounds the leaderless random draw range `1..H`, i.e. Algorithm 4's
///    `random(1..H-1)` -- see [`draw_priority`]. No proposal drawn in a
///    leaderless step can ever equal `H`, by construction (`gen_range`'s
///    upper bound is exclusive).
/// 2. In round 1 (`s = 4`) of a leader-based slot, the designated leader
///    attaches exactly `H` to its proposal instead of drawing randomly (see
///    [`Proposer::begin_step`]'s phase-0 branch) -- Algorithm 4's `s mod 4 =
///    0 and (s > 4 or i is not leader)` guard, negated.
///
/// Because `H` is `u64::MAX` and every leaderless draw is strictly less
/// than `H`, `H` is *the* unbeatable maximum of this priority space: once
/// any proposal carries it, `Ord` guarantees nothing else can ever compare
/// greater. This is exactly what the phase-0 fast-path decision rule
/// ([`fast_path_value`]) exploits -- see that function's docs and Lemma
/// C.10 in the paper.
pub const H: u64 = u64::MAX;

/// `s = 4*1 + 0`: the threshold-clock step for round 1, phase 0 -- the only
/// step at which a designated leader ever attaches `H` (§4.2.5), and hence
/// the only step at which the phase-0 fast path can fire.
const FIRST_ROUND_STEP: u64 = 4;

/// How many logical ticks a proposer waits, at minimum, after sending (or
/// resending) a step's requests before checking whether it needs to retry.
/// The *actual* spacing grows with [`retry_backoff_delay`]; this is the
/// base (first-retry) value.
pub const RETRY_DELAY_TICKS: u64 = 20;

/// Retries never stop (issue #13 -- see the module docs' "Retries"
/// section), but their spacing backs off exponentially up to this many
/// doublings of [`RETRY_DELAY_TICKS`], so a permanently-unreachable
/// majority costs bounded retry *rate*, not unbounded retry *count*.
const RETRY_BACKOFF_MAX_SHIFT: u32 = 6;

/// The spacing before the `retries`-th retry (0-indexed): `RETRY_DELAY_TICKS`
/// doubled up to [`RETRY_BACKOFF_MAX_SHIFT`] times, then held constant.
fn retry_backoff_delay(retries: u32) -> u64 {
    let shift = retries.min(RETRY_BACKOFF_MAX_SHIFT);
    RETRY_DELAY_TICKS.saturating_mul(1u64 << shift)
}

/// The timer id used to kick off a proposer's very first step. Distinct
/// from any real retry timer id (`TimerId(step)`) because step numbers stay
/// far below `u64::MAX` in any run this crate's tests exercise (A7: step
/// counts are practically bounded).
pub const KICKOFF_TIMER: TimerId = TimerId(u64::MAX);

/// The timer id used for a hedged proposer's delayed-activation checks (see
/// the module docs' "Hedging" section). Distinct from `KICKOFF_TIMER` and
/// from any real retry timer id (`TimerId(step)`) for the same A7-bounded
/// reason.
pub const HEDGE_TIMER: TimerId = TimerId(u64::MAX - 1);

/// How many logical ticks a hedged, currently-passive proposer waits before
/// rechecking whether it should activate (see [`Proposer::with_hedging`]).
pub const HEDGE_RECHECK_TICKS: u64 = RETRY_DELAY_TICKS;

/// The retry-timer id for a given step: reusing the step number itself as
/// the timer id, so a stale timer is recognizable without extra state (see
/// the module docs).
fn retry_timer_id(step: u64) -> TimerId {
    TimerId(step)
}

/// `best_j` from Algorithm 4: the highest-priority proposal among an
/// iterator of `Option<Proposal<V>>` (nil-safe: `None` never displaces a
/// `Some`, and an all-`None` input yields `None`).
fn best_of<V: Ord>(iter: impl Iterator<Item = Option<Proposal<V>>>) -> Option<Proposal<V>> {
    iter.max().flatten()
}

/// The phase-0 fast-path decision check (§4.2.5, D1): `Some(value)` iff
/// every reply in this (already-quorum-sized) step-4 response set reports a
/// `first` proposal at priority `H`.
///
/// # Why this is safe (Lemma C.10)
///
/// `H` is never drawn by a leaderless proposer ([`H`]'s docs), so only the
/// designated leader's round-1 proposal can ever carry it. A recorder's ISR
/// writes `F[4]` (`first`) exactly once, on the *first* `record(4, _)` call
/// it ever sees (`crate::isr::Isr::record`'s `Ordering::Greater` arm; steps
/// never regress, so nothing later can overwrite it). So if every recorder
/// in this quorum reports `first.priority == H`, more than `n/2` recorders
/// captured *the same* leader proposal in `F[4]`, and it can never be
/// overwritten.
///
/// This is enough to make the decision safe even though no other proposer
/// has necessarily seen the same thing yet: any other proposer's own
/// step-4 quorum is also a majority, and any two majorities of a fixed `n`
/// intersect. So every other proposer's quorum contains at least one
/// recorder holding the leader's `H`-proposal in `F[4]` -- either that
/// proposer's own quorum is *also* all-`H` (and it decides the same value
/// via this same fast path), or it isn't, in which case the leader's
/// proposal is nevertheless present among the replies that proposer's
/// `best_j` selects from, and -- because `H` is unbeatable -- `best_j`
/// necessarily picks it, so that proposer spreads it onward into phase 1
/// exactly as if it were an ordinary round-1 proposal. From there the
/// module's general "why this preserves Agreement" argument above (which
/// is deliberately phrased over "any phase, not just 0") takes over
/// unchanged: once spread, the value can only ever be matched or beaten,
/// never lost, by any future quorum -- and nothing can beat `H`. So every
/// live proposer that does not fast-decide still converges on exactly the
/// same value through the ordinary leaderless machinery. A fast-path
/// decision and a leaderless decision for the same slot can therefore never
/// disagree: they are the same value by construction, not by coincidence.
///
/// The `debug_assert!` below is a defense-in-depth structural check of
/// the "only the leader ever sends `H`, so all `H`-tagged replies in one
/// quorum must be identical" premise -- it should be unreachable in a
/// correct build (a single, consistently-configured leader per slot -- see
/// `Proposer::new`) but a violation here would mean an agreement-threatening
/// bug, not a benign race, so it is worth catching early in debug/test
/// builds rather than trusting the premise silently.
fn fast_path_value<V: Ord + Clone>(responses: &BTreeMap<NodeId, RecordResponse<V>>) -> Option<V> {
    let mut replies = responses.values();
    let first_reply = replies.next()?.first.as_ref()?;
    if first_reply.priority != H {
        return None;
    }
    for resp in responses.values() {
        match &resp.first {
            Some(p) if p.priority == H => {
                debug_assert!(
                    p == first_reply,
                    "two distinct H-priority proposals seen in one quorum -- H must be \
                     attached by exactly one consistently-configured leader"
                );
            }
            _ => return None,
        }
    }
    Some(first_reply.value.clone())
}

/// One proposer's Algorithm-4 state for one slot.
pub struct Proposer<V> {
    self_id: NodeId,
    /// The slot this proposer is running consensus for -- see
    /// [`crate::rpc::RecordRequest::slot`]'s docs. Attached verbatim to
    /// every outgoing [`RecordRequest`] and checked against every incoming
    /// [`RecordResponse`] ([`Proposer::on_response`]); inert for any
    /// single-slot caller (always `0`).
    slot: u64,
    /// Total configured replica count `n` (not "currently live") -- quorum
    /// is always computed against this fixed membership, matching
    /// `crate::tcast`'s reasoning for why doing otherwise risks split-brain
    /// between two disjoint groups that each satisfy a "live-relative"
    /// majority.
    total_replicas: usize,
    /// The slot's designated leader, if any (§4.2.5). Every [`Proposer`]
    /// instance for the same slot must be constructed with the *same*
    /// value here -- see `Proposer::new`'s docs -- otherwise more than one
    /// replica could believe itself the leader and attach `H` to more than
    /// one distinct proposal, which is exactly the premise
    /// [`fast_path_value`]'s `debug_assert!` guards against.
    leader: Option<NodeId>,
    /// `s`: the threshold logical clock step, `4*round + phase`.
    step: u64,
    /// `p`: the current working proposal template.
    proposal: Proposal<V>,
    /// Set at most once: this replica's decision for the slot (P3
    /// Integrity / "decide once").
    decided: Option<V>,
    /// Replies collected so far for the *current* step, keyed by
    /// responding recorder. Cleared every time a new step begins.
    responses: BTreeMap<NodeId, RecordResponse<V>>,
    /// What was actually sent to each recorder for the current step (so a
    /// retry resends identical content rather than drawing a fresh
    /// priority, and so a duplicate/racing reply can be recognized as
    /// referring to the same request).
    sent: BTreeMap<NodeId, Proposal<V>>,
    /// Retries used so far for the current step; reset every `begin_step`.
    /// Also feeds [`retry_backoff_delay`].
    retries_this_step: u32,
    /// Ticks to wait, after `start`, before this proposer's first
    /// activation attempt (see [`Proposer::with_hedging`]). `0` (the
    /// default from `Proposer::new`) reproduces unconditional δ=0
    /// activation exactly: `start` skips the hedge timer entirely.
    activation_delay: u64,
    /// A handle to this replica's own co-located recorder's
    /// most-recently-observed ISR step for this slot (see the module docs'
    /// "Hedging" section for why this, and not a network probe, is the
    /// evidence-of-progress signal). `None` if this proposer was never
    /// wired up with one -- a hedged-by-delay-only proposer with no
    /// progress oracle, which just activates once its delay elapses.
    local_recorder_step: Option<Rc<Cell<u64>>>,
    /// The `local_recorder_step` value as of the *previous* hedge recheck
    /// (or `None` before the first one), used to tell *fresh* progress
    /// (worth deferring for) from stale/stalled progress (not worth
    /// deferring for -- see the module docs' "why no δ can stall" section).
    last_seen_progress: Option<u64>,
    /// Whether `begin_step` has ever run for this proposer -- i.e. whether
    /// it has sent so much as one `record` request yet. `false` the entire
    /// time a hedged proposer is waiting out its delay or deferring.
    /// Exposed via [`Proposer::activated`] for tests asserting D2's
    /// "backups stay passive" behavior.
    activated: bool,
}

impl<V: Ord + Clone> Proposer<V> {
    /// Build a proposer that has not yet started; call [`Proposer::start`]
    /// (from a `KICKOFF_TIMER` callback) to begin round 1, phase 0.
    ///
    /// `leader` designates the slot's fast-path leader (§4.2.5), or `None`
    /// for a purely leaderless slot (Phase 2 behavior, unchanged). **Every
    /// proposer for the same slot must be built with the same `leader`
    /// value** -- this is a cluster-construction invariant (see
    /// `crate::concrete::ConcreteCluster::new_with_leader`, which enforces
    /// it by passing one shared value to every `Proposer::new` call), not
    /// something this constructor can check on its own.
    /// `slot` is attached verbatim to every outgoing `record` request and
    /// checked on every incoming reply (see [`Proposer::slot`]'s field
    /// docs); single-slot callers (this crate's own Phase 2/3 tests and
    /// [`crate::concrete::ConcreteCluster`]) always pass `0`.
    pub fn new(
        self_id: NodeId,
        total_replicas: usize,
        initial_value: V,
        leader: Option<NodeId>,
        slot: u64,
    ) -> Self {
        Self {
            self_id,
            slot,
            total_replicas,
            leader,
            step: 0,
            // Initial template per Algorithm 4: `p <- <H, i, v>`. This `H`
            // priority is never actually sent as-is: phase 0 always either
            // redraws randomly (leaderless steps) or substitutes the exact
            // constant `H` explicitly (round-1 leader step) -- see
            // `begin_step`. It only matters here as a placeholder value
            // before the first real proposal is prepared.
            proposal: Proposal {
                value: initial_value,
                priority: H,
                origin: self_id,
            },
            decided: None,
            responses: BTreeMap::new(),
            sent: BTreeMap::new(),
            retries_this_step: 0,
            activation_delay: 0,
            local_recorder_step: None,
            last_seen_progress: None,
            activated: false,
        }
    }

    /// Opt this proposer into the hedging schedule (§5.2; see the module
    /// docs' "Hedging" section for the full design). Without this call
    /// (the default), a proposer activates the instant `start` runs --
    /// Phase 3's unconditional δ=0 behavior, unchanged.
    ///
    /// `activation_delay` is this proposer's position in the schedule,
    /// already resolved to a tick count by the caller (e.g. `rank * δ` --
    /// see `crate::concrete::ConcreteCluster::new_with_schedule`); this
    /// type has no opinion on schedule-construction policy, only on what
    /// to do with the final per-replica delay.
    ///
    /// `local_recorder_step` is a handle to this replica's own co-located
    /// recorder's most-recently-observed step for this slot, kept fresh by
    /// the driver every time that recorder answers *any* `record` request
    /// (see `crate::concrete::ReplicaNode`) -- the free, zero-extra-message
    /// evidence-of-progress signal a hedged proposer consults before
    /// activating.
    pub fn with_hedging(
        mut self,
        activation_delay: u64,
        local_recorder_step: Rc<Cell<u64>>,
    ) -> Self {
        self.activation_delay = activation_delay;
        self.local_recorder_step = Some(local_recorder_step);
        self
    }

    /// This replica's decision, if any.
    pub fn decided(&self) -> Option<&V> {
        self.decided.as_ref()
    }

    /// This proposer's configured hedging delay (ticks after `start` before
    /// its first activation attempt); `0` if [`Proposer::with_hedging`] was
    /// never called.
    pub fn activation_delay(&self) -> u64 {
        self.activation_delay
    }

    /// Whether this proposer has ever sent a `record` request -- `false`
    /// the whole time a hedged, currently-passive proposer is waiting out
    /// its delay or deferring to observed progress elsewhere. See the
    /// module docs' "Hedging" section.
    pub fn activated(&self) -> bool {
        self.activated
    }

    /// The current step (for tests/introspection).
    pub fn step(&self) -> u64 {
        self.step
    }

    /// Whether this replica has decided *and* did so on the phase-0 fast
    /// path (§4.2.5, D1) -- i.e. without ever leaving round 1's first step.
    /// A decision at any later step went through the ordinary
    /// spread/gather machinery, whether or not a leader was configured.
    pub fn decided_via_fast_path(&self) -> bool {
        self.decided.is_some() && self.step == FIRST_ROUND_STEP
    }

    fn quorum_threshold(&self) -> usize {
        self.total_replicas / 2 + 1
    }

    /// Whether `self` is the slot's designated fast-path leader.
    fn is_leader(&self) -> bool {
        self.leader == Some(self.self_id)
    }

    /// True majority of the full membership, matching `crate::tcast`.
    fn all_recorders(&self) -> impl Iterator<Item = NodeId> {
        (0..self.total_replicas as u32).map(NodeId)
    }

    /// Kick off round 1, phase 0 (`s <- 4*1 + 0`). Called once, from the
    /// driver injecting a `KICKOFF_TIMER`.
    ///
    /// If this proposer was configured with [`Proposer::with_hedging`] and
    /// has a nonzero `activation_delay`, this does *not* activate
    /// immediately: it arms [`HEDGE_TIMER`] and defers the actual first
    /// `begin_step` to [`Proposer::maybe_activate_after_hedge`]. With no
    /// hedging configured (`activation_delay == 0`, the default), this is
    /// exactly Phase 3's unconditional behavior: activate right now.
    pub fn start(&mut self, ctx: &mut NodeCtx<'_, ConcreteMsg<V>>) {
        self.step = FIRST_ROUND_STEP;
        if self.activation_delay == 0 {
            self.begin_step(ctx);
        } else {
            ctx.schedule_timer(self.activation_delay, HEDGE_TIMER);
        }
    }

    /// A hedge timer has fired: either this proposer's initial
    /// `activation_delay` has elapsed, or a prior recheck's
    /// [`HEDGE_RECHECK_TICKS`] has. Decide whether to stay passive (defer
    /// again) or activate -- see the module docs' "Hedging" and "Why no δ
    /// can cause a permanent stall" sections for the full reasoning.
    fn maybe_activate_after_hedge(&mut self, ctx: &mut NodeCtx<'_, ConcreteMsg<V>>) {
        if self.decided.is_some() {
            // Nothing to activate for -- already done (only reachable if a
            // caller somehow drove this proposer to a decision through some
            // other path before its hedge timer fired; defensive, not
            // expected in the current single-slot/SMR drivers).
            return;
        }
        let current = self.local_recorder_step.as_ref().map(|cell| cell.get());
        let saw_fresh_progress = matches!(
            current,
            Some(step) if step > self.step && Some(step) != self.last_seen_progress
        );
        if saw_fresh_progress {
            self.last_seen_progress = current;
            ctx.schedule_timer(HEDGE_RECHECK_TICKS, HEDGE_TIMER);
        } else {
            // Either no progress was ever observed, or the last-observed
            // progress has stalled since the previous recheck (no further
            // advance) -- safe, and necessary for liveness (P15/P16), to
            // activate now.
            self.begin_step(ctx);
        }
    }

    /// Prepare and send this step's `record` requests to every recorder,
    /// then arm the retry timer. Used both for the very first step (via
    /// `start`, possibly hedged) and every subsequent step/catch-up.
    fn begin_step(&mut self, ctx: &mut NodeCtx<'_, ConcreteMsg<V>>) {
        self.activated = true;
        self.responses.clear();
        self.sent.clear();
        self.retries_this_step = 0;

        let phase = self.step % 4;
        // Algorithm 4's phase-0 guard, `s mod 4 = 0 and (s > 4 or i is not
        // leader)`, negated: this replica is the fast-path leader *and*
        // this is round 1's phase 0.
        let is_fast_path_round = phase == 0 && self.step == FIRST_ROUND_STEP && self.is_leader();
        for recorder in self.all_recorders() {
            let proposal = if is_fast_path_round {
                // §4.2.5: the leader attaches the reserved max priority `H`
                // to every recorder's proposal instead of drawing randomly
                // -- see `H`'s docs for why this is what makes the phase-0
                // fast-path check ([`fast_path_value`]) sound.
                Proposal {
                    value: self.proposal.value.clone(),
                    priority: H,
                    origin: self.self_id,
                }
            } else if phase == 0 {
                // Proposal randomization (§4.2.4): a *fresh, independent*
                // random priority per recorder. Covers both genuinely
                // leaderless slots and every non-leader proposer's round-1
                // phase 0 (unconditional activation, δ=0: backup proposers
                // are active from round 1 too, just without `H`) and every
                // proposer's phase 0 in round >= 2 (the leader included --
                // §4.2.5: "QuePaxa therefore uses a leader only in the
                // first round of any slot").
                Proposal {
                    value: self.proposal.value.clone(),
                    priority: draw_priority(ctx),
                    origin: self.self_id,
                }
            } else {
                self.proposal.clone()
            };
            self.sent.insert(recorder, proposal.clone());
            self.send_request(recorder, proposal, ctx);
        }
        ctx.schedule_timer(RETRY_DELAY_TICKS, retry_timer_id(self.step));
    }

    fn send_request(
        &self,
        recorder: NodeId,
        proposal: Proposal<V>,
        ctx: &mut NodeCtx<'_, ConcreteMsg<V>>,
    ) {
        ctx.send(
            recorder,
            ConcreteMsg::Request(RecordRequest {
                slot: self.slot,
                req_step: self.step,
                proposal,
            }),
        );
    }

    /// A `RecordResponse` has arrived from `from`. Correlate it to the
    /// current outstanding step (dropping it silently if it answers a
    /// request we've since moved past, or if we've already decided), fold
    /// it into this step's response set, and process a quorum as soon as
    /// one is available.
    pub fn on_response(
        &mut self,
        from: NodeId,
        resp: RecordResponse<V>,
        ctx: &mut NodeCtx<'_, ConcreteMsg<V>>,
    ) {
        if self.decided.is_some() {
            return;
        }
        if resp.slot != self.slot || resp.req_step != self.step {
            // Stale (answers an earlier, already-superseded request, or --
            // only possible when a caller multiplexes multiple slots over
            // the same replica addresses, see `crate::rpc::RecordRequest`'s
            // docs -- a different slot's reply entirely) or otherwise
            // irrelevant -- ignore.
            return;
        }
        // First writer wins for a given recorder within a step: retries
        // may cause more than one reply from the same recorder for the
        // same request, and they carry equivalent information (see
        // `crate::isr`'s idempotence within a step).
        self.responses.entry(from).or_insert(resp);

        if self.responses.len() >= self.quorum_threshold() {
            self.process_quorum(ctx);
        }
    }

    /// A retry, hedge, or kickoff timer has fired.
    pub fn on_timer(&mut self, timer_id: TimerId, ctx: &mut NodeCtx<'_, ConcreteMsg<V>>) {
        if timer_id == KICKOFF_TIMER {
            self.start(ctx);
            return;
        }
        if timer_id == HEDGE_TIMER {
            self.maybe_activate_after_hedge(ctx);
            return;
        }
        if self.decided.is_some() || !self.activated || timer_id.0 != self.step {
            // Stale retry timer for a step we've since left (via advance,
            // catch-up, or decision) -- no-op.
            //
            // `!self.activated` is also required, not merely a redundant
            // strengthening of the `timer_id.0 != self.step` check: raw step
            // numbers (`retry_timer_id`) are only unique *within* one
            // `Proposer`'s own lifetime, not across separate `Proposer`
            // instances that happen to share a `NodeId`'s timer namespace --
            // exactly what `crate::concrete`'s single-slot driver never
            // exercises (one `Proposer` per node, ever) but a multi-slot
            // driver like `queso_smr::replica::SmrNode` does: it runs one
            // slot's `Proposer` at a time, and a *superseded* attempt's
            // uncancelled retry timer (the kernel has no facility to cancel
            // an already-scheduled timer) can still be sitting in the queue
            // when the *next* slot's fresh `Proposer` starts -- also at
            // `self.step == FIRST_ROUND_STEP` almost always, so the naive
            // `timer_id.0 != self.step` check alone cannot tell the two
            // apart. Without this guard, a stale retry misrouted to a fresh,
            // still-hedged (not yet `begin_step`-activated) proposer would
            // index `self.sent` for a recorder it has never actually sent
            // anything to yet -- a genuine panic, not merely a benign no-op
            // -- since `begin_step` is what populates `self.sent` and it is
            // exactly what a hedged-but-not-yet-activated proposer has not
            // run. `begin_step` always sets `self.activated = true` as its
            // very first action, strictly before it ever schedules a retry
            // timer for the step in question, so this can never suppress a
            // *genuine* retry -- only a misrouted one. Purely a liveness/
            // robustness fix (which stale timer gets ignored); it does not
            // touch what a quorum, a step, or a decision means.
            return;
        }
        if self.responses.len() >= self.quorum_threshold() {
            // Quorum already reached (the timer lost the race); nothing to
            // retry.
            return;
        }
        // Unbounded retry with exponential backoff (issue #13) -- see the
        // module docs' "Retries" section for why this never gives up.
        let delay = retry_backoff_delay(self.retries_this_step);
        self.retries_this_step = self.retries_this_step.saturating_add(1);
        for recorder in self.all_recorders() {
            if !self.responses.contains_key(&recorder) {
                let proposal = self.sent[&recorder].clone();
                self.send_request(recorder, proposal, ctx);
            }
        }
        ctx.schedule_timer(delay, retry_timer_id(self.step));
    }

    /// A quorum of replies for the current step has been gathered; branch
    /// on whether they all agree on our step (normal phase processing) or
    /// whether we've fallen behind (catch-up), exactly mirroring Algorithm
    /// 4's `if s'_j = s in all replies ... else if any reply has s'_j > s`.
    fn process_quorum(&mut self, ctx: &mut NodeCtx<'_, ConcreteMsg<V>>) {
        let all_at_step = self.responses.values().all(|r| r.step == self.step);
        if all_at_step {
            self.process_phase();
            if self.decided.is_none() {
                self.step += 1;
                self.begin_step(ctx);
            }
        } else {
            // Catch-up: adopt the (s'_j, f'_j) pair from whichever reply
            // reports the highest s'_j (ties broken by NodeId for
            // determinism -- see module docs on why exactly one such
            // reply, at minimum, must exist here).
            let (_, best_reply) = self
                .responses
                .iter()
                .filter(|(_, r)| r.step > self.step)
                .max_by(|(id_a, r_a), (id_b, r_b)| r_a.step.cmp(&r_b.step).then(id_a.cmp(id_b)))
                .expect(
                    "process_quorum called with not-all-equal responses but no reply has s'_j > s; \
                     recorder step is monotone non-decreasing so this cannot happen",
                );
            self.step = best_reply.step;
            self.proposal = best_reply
                .first
                .clone()
                .expect("a recorder reporting s' > s must have recorded a first value there");
            self.begin_step(ctx);
        }
    }

    /// The `if s mod 4 = k` branch bodies from Algorithm 4 (everything
    /// *except* the "advance to next step" / catch-up logic, which
    /// `process_quorum` already handled).
    fn process_phase(&mut self) {
        match self.step % 4 {
            0 => {
                // Phase 0: propose. First check the leader fast path
                // (§4.2.5, D1, Lemma C.10): if every reply's `first` is
                // `H`-priority, more than n/2 recorders captured the
                // leader's proposal in F[4] and it can never be overwritten
                // or beaten -- decide immediately, after one round-trip.
                // In leaderless steps (no configured leader, or any step
                // past round 1) no drawn priority is ever `H` (see `H`'s
                // docs), so this can never fire there -- exactly recovering
                // Phase 2's leaderless behavior.
                if let Some(value) = fast_path_value(&self.responses) {
                    self.decided = Some(value);
                } else {
                    let best = best_of(self.responses.values().map(|r| r.first.clone())).expect(
                        "a quorum of recorders that just recorded a proposal must report a first value",
                    );
                    self.proposal = best;
                }
            }
            1 => {
                // Phase 1: spread E. No action required (Algorithm 4).
            }
            2 => {
                // Phase 2: gather E, spread C, detect consensus.
                let best_e = best_of(self.responses.values().map(|r| r.prior_agg.clone()));
                if best_e.as_ref() == Some(&self.proposal) {
                    self.decided = Some(self.proposal.value.clone());
                }
            }
            3 => {
                // Phase 3: gather C.
                let best_c = best_of(self.responses.values().map(|r| r.prior_agg.clone())).expect(
                    "step s+1 traffic implies some proposer completed step s with an \
                     all-at-s quorum Q_s; Q_s intersects this phase-3 quorum in a recorder \
                     whose s->s+1 transition carried a non-nil A_c into A_p (Lemma C.5), so \
                     at least one prior_agg is Some -- holds even when this proposer caught \
                     up directly into phase 3 without spreading in phase 2",
                );
                self.proposal = best_c;
            }
            _ => unreachable!("s mod 4 is always in 0..4"),
        }
    }
}

/// Draw a fresh random priority in `1..H` (`H` exclusive, matching
/// Algorithm 4's `random(1..H-1)` inclusive-inclusive range), from the
/// kernel's single seeded PRNG stream via `ctx`.
fn draw_priority<V>(ctx: &mut NodeCtx<'_, ConcreteMsg<V>>) -> u64 {
    ctx.rng().gen_range(1..H)
}
