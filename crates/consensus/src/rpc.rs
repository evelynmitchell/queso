//! The wire messages for the concrete (Phase 2) protocol: proposer-to-
//! recorder `record` requests and recorder-to-proposer replies.
//!
//! Unlike Phase 1's [`crate::message::TcastMsg`] (one message kind, sent as
//! part of an externally-driven lock-step `tcast` call), the concrete
//! protocol is RPC-style and asynchronous: a [`crate::proposer::Proposer`]
//! sends [`RecordRequest`]s to every recorder in parallel and a
//! [`crate::recorder::Recorder`] answers each with exactly one
//! [`RecordResponse`], entirely driven by `Node` callbacks (see
//! `crate::concrete`). Both message kinds travel over the same wire type,
//! [`ConcreteMsg`], so a single [`queso_sim::node::Node`] impl can dispatch
//! on which one arrived.

use queso_sim::payload::{Inspectable, Payload};

use crate::proposal::Proposal;

/// `record(s, p)`, sent from a proposer to one recorder: "treat `proposal`
/// as arriving at logical step `req_step`". `req_step` is echoed back
/// verbatim in the matching [`RecordResponse`] so the proposer can tell a
/// reply to *this* request apart from a stale reply to an earlier one it has
/// since moved past (see `crate::proposer`'s module docs on why this
/// correlation is required for safety under reordering/duplication).
///
/// `slot` is an opaque routing tag, not part of Algorithm 4 itself: a single
/// slot's consensus (everything else in this crate) has no notion of
/// multiple slots at all. It exists so that a *multi-slot* driver (Phase 4,
/// `queso-smr`) can run many independent instances of this same per-slot
/// protocol over one shared node/recorder addressing space without the
/// wire-level ambiguity that would otherwise arise: two different slots'
/// [`crate::proposer::Proposer`]s both start their threshold clock at the
/// same step numbers (`s = 4, 5, 6, ...`), so a recorder receiving a bare
/// `(req_step, proposal)` pair would have no way to tell which slot's ISR it
/// belongs to, and a proposer receiving a bare `(req_step, ...)` reply would
/// have no way to reject a stale reply that happens to echo the same step
/// number but actually answers a *different* slot's earlier request.
/// Single-slot users (this crate's own Phase 2/3 tests,
/// [`crate::concrete::ConcreteCluster`]) always use slot `0` and never
/// inspect it -- it is inert plumbing here, load-bearing only once a caller
/// actually multiplexes more than one slot over the same replica addresses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordRequest<V> {
    pub slot: u64,
    pub req_step: u64,
    pub proposal: Proposal<V>,
}

/// The recorder's reply: the ISR's `(s', f', a')` summary (see
/// [`crate::isr::IsrSummary`]), plus `req_step` (and `slot`) echoed from the
/// [`RecordRequest`] this answers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordResponse<V> {
    /// Echoed from the request -- see [`RecordRequest::slot`]'s docs.
    pub slot: u64,
    /// Echoed from the request, for proposer-side correlation. *Not* the
    /// same thing as `step` below in general: `step` is the recorder's
    /// (possibly-further-advanced) internal `S`.
    pub req_step: u64,
    /// `s'`: the recorder's internal step after handling this call.
    pub step: u64,
    /// `f'`: `F[s']`, the first value recorded at step `s'`.
    pub first: Option<Proposal<V>>,
    /// `a'`: `A[s'-1]`, the aggregate of everything recorded during the
    /// immediately-prior step.
    pub prior_agg: Option<Proposal<V>>,
}

/// The one wire payload type the concrete protocol's kernel instantiation
/// uses: either a request (proposer -> recorder) or a response (recorder ->
/// proposer). No other message shapes exist -- per §4.2.1, "proposers never
/// interact directly with each other, and neither do recorders".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConcreteMsg<V> {
    Request(RecordRequest<V>),
    Response(RecordResponse<V>),
}

impl<V> Payload for ConcreteMsg<V> {
    fn size(&self) -> usize {
        // Opaque metric, as in Phase 1's `TcastMsg` -- a fixed small size
        // stands in for "one proposal-shaped triple plus a step number",
        // since neither carries a proposal *set*.
        match self {
            ConcreteMsg::Request(_) => 1,
            ConcreteMsg::Response(_) => 1,
        }
    }
}

impl<V> Inspectable for ConcreteMsg<V> {
    fn tag(&self) -> &'static str {
        match self {
            ConcreteMsg::Request(_) => "record_request",
            ConcreteMsg::Response(_) => "record_response",
        }
    }
}
