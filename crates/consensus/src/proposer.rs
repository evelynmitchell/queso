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
//!   different priority per recipient; see [`draw_priority`]) and set
//!   `p <- best_j(f'_j)` over the quorum's first-value replies.
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
//! Retries are capped per step ([`MAX_RETRIES_PER_STEP`]); exceeding the
//! cap does not panic (unlike `crate::tcast`'s hard "you promised a live
//! majority" precondition) because a proposer legitimately *cannot* make
//! progress when fewer than a majority of recorders are reachable at all
//! (P11/O4: safety holds, liveness may simply stall) -- it just stops
//! retrying and leaves the proposer parked at its current step.

use queso_sim::ids::{NodeId, TimerId};
use queso_sim::node::NodeCtx;
use rand::Rng;
use std::collections::BTreeMap;

use crate::proposal::Proposal;
use crate::rpc::{ConcreteMsg, RecordRequest, RecordResponse};

/// `H`, the reserved maximum priority (used only to bound the random draw
/// range `1..H`, i.e. Algorithm 4's `random(1..H-1)`; the value `H` itself
/// is reserved for the leader fast path, out of scope here -- see the crate
/// docs). No proposal drawn in leaderless rounds can ever equal `H`.
pub const MAX_PRIORITY: u64 = u64::MAX;

/// Retry budget per step before a proposer gives up retrying (but stays
/// parked, ready to resume if fresh network activity reaches it) -- see the
/// module docs' "Retries" section for why this does not panic.
pub const MAX_RETRIES_PER_STEP: u32 = 64;

/// How many logical ticks a proposer waits after sending (or resending) a
/// step's requests before checking whether it needs to retry.
pub const RETRY_DELAY_TICKS: u64 = 20;

/// The timer id used to kick off a proposer's very first step. Distinct
/// from any real retry timer id (`TimerId(step)`) because step numbers stay
/// far below `u64::MAX` in any run this crate's tests exercise (A7: step
/// counts are practically bounded).
pub const KICKOFF_TIMER: TimerId = TimerId(u64::MAX);

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

/// One proposer's Algorithm-4 state for one slot.
pub struct Proposer<V> {
    self_id: NodeId,
    /// Total configured replica count `n` (not "currently live") -- quorum
    /// is always computed against this fixed membership, matching
    /// `crate::tcast`'s reasoning for why doing otherwise risks split-brain
    /// between two disjoint groups that each satisfy a "live-relative"
    /// majority.
    total_replicas: usize,
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
    retries_this_step: u32,
}

impl<V: Ord + Clone> Proposer<V> {
    /// Build a proposer that has not yet started; call [`Proposer::start`]
    /// (from a `KICKOFF_TIMER` callback) to begin round 1, phase 0.
    pub fn new(self_id: NodeId, total_replicas: usize, initial_value: V) -> Self {
        Self {
            self_id,
            total_replicas,
            step: 0,
            // Initial template per Algorithm 4: `p <- <H, i, v>`. The `H`
            // priority here is never actually sent (phase 0 always
            // redraws in leaderless mode -- see `begin_step`); it only
            // matters as a placeholder before the first real draw.
            proposal: Proposal {
                value: initial_value,
                priority: MAX_PRIORITY,
                origin: self_id,
            },
            decided: None,
            responses: BTreeMap::new(),
            sent: BTreeMap::new(),
            retries_this_step: 0,
        }
    }

    /// This replica's decision, if any.
    pub fn decided(&self) -> Option<&V> {
        self.decided.as_ref()
    }

    /// The current step (for tests/introspection).
    pub fn step(&self) -> u64 {
        self.step
    }

    fn quorum_threshold(&self) -> usize {
        self.total_replicas / 2 + 1
    }

    /// True majority of the full membership, matching `crate::tcast`.
    fn all_recorders(&self) -> impl Iterator<Item = NodeId> {
        (0..self.total_replicas as u32).map(NodeId)
    }

    /// Kick off round 1, phase 0 (`s <- 4*1 + 0`). Called once, from the
    /// driver injecting a `KICKOFF_TIMER`.
    pub fn start(&mut self, ctx: &mut NodeCtx<'_, ConcreteMsg<V>>) {
        self.step = 4;
        self.begin_step(ctx);
    }

    /// Prepare and send this step's `record` requests to every recorder,
    /// then arm the retry timer. Used both for the very first step (via
    /// `start`) and every subsequent step/catch-up.
    fn begin_step(&mut self, ctx: &mut NodeCtx<'_, ConcreteMsg<V>>) {
        self.responses.clear();
        self.sent.clear();
        self.retries_this_step = 0;

        let phase = self.step % 4;
        for recorder in self.all_recorders() {
            let proposal = if phase == 0 {
                // Proposal randomization (§4.2.4): a *fresh, independent*
                // random priority per recorder, always (leaderless: the
                // "i is not leader" disjunct in Algorithm 4's condition is
                // unconditionally true here -- there is no leader role in
                // this phase).
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
        if resp.req_step != self.step {
            // Stale (answers an earlier, already-superseded request) or
            // otherwise irrelevant -- ignore.
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

    /// A retry (or kickoff) timer has fired.
    pub fn on_timer(&mut self, timer_id: TimerId, ctx: &mut NodeCtx<'_, ConcreteMsg<V>>) {
        if timer_id == KICKOFF_TIMER {
            self.start(ctx);
            return;
        }
        if self.decided.is_some() || timer_id.0 != self.step {
            // Stale retry timer for a step we've since left (via advance,
            // catch-up, or decision) -- no-op.
            return;
        }
        if self.responses.len() >= self.quorum_threshold() {
            // Quorum already reached (the timer lost the race); nothing to
            // retry.
            return;
        }
        if self.retries_this_step >= MAX_RETRIES_PER_STEP {
            // Give up retrying this step -- see the module docs' "Retries"
            // section: this is the expected shape of a stall when fewer
            // than a majority of recorders are reachable, not a bug to
            // panic about.
            return;
        }
        self.retries_this_step += 1;
        for recorder in self.all_recorders() {
            if !self.responses.contains_key(&recorder) {
                let proposal = self.sent[&recorder].clone();
                self.send_request(recorder, proposal, ctx);
            }
        }
        ctx.schedule_timer(RETRY_DELAY_TICKS, retry_timer_id(self.step));
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
                // Phase 0: propose. (The leader fast-path check --
                // "if f'_j.priority = H in all replies" -- is intentionally
                // omitted: it cannot fire in leaderless mode, since no
                // drawn priority is ever H; see `draw_priority`.)
                let best = best_of(self.responses.values().map(|r| r.first.clone())).expect(
                    "a quorum of recorders that just recorded a proposal must report a first value",
                );
                self.proposal = best;
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
    ctx.rng().gen_range(1..MAX_PRIORITY)
}
