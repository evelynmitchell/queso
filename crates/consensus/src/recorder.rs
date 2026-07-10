//! The passive **recorder** role (§4.2.1): wraps one [`Isr`] per slot and
//! answers `record` RPCs. A recorder never initiates communication, never
//! talks to another recorder, and never talks to a proposer except by
//! replying to that proposer's own request -- all per §4.2.1's "all
//! communication is RPC-style, proposer-to-recorder".

use crate::isr::Isr;
use crate::rpc::{RecordRequest, RecordResponse};

/// One replica's recorder state for one slot: just an [`Isr`] plus the
/// glue to turn a [`RecordRequest`] into a [`RecordResponse`].
#[derive(Debug, Clone)]
pub struct Recorder<V> {
    isr: Isr<V>,
}

impl<V> Default for Recorder<V> {
    fn default() -> Self {
        Self {
            isr: Isr::default(),
        }
    }
}

impl<V: Ord + Clone> Recorder<V> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Handle one `record(s, p)` invocation, returning the reply to send
    /// back to the requesting proposer. `req.req_step` is echoed back
    /// unchanged so the proposer can correlate this reply to the specific
    /// request that produced it.
    pub fn handle(&mut self, req: RecordRequest<V>) -> RecordResponse<V> {
        let summary = self.isr.record(req.req_step, req.proposal);
        RecordResponse {
            slot: req.slot,
            req_step: req.req_step,
            step: summary.step,
            first: summary.first,
            prior_agg: summary.prior_agg,
        }
    }

    /// The current `(S, F[S], A[S-1])` view, without recording anything.
    /// Exposed for tests/introspection only.
    pub fn peek(&self) -> crate::isr::IsrSummary<V> {
        self.isr.summary()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proposal::Proposal;
    use queso_sim::ids::NodeId;

    /// Convenience: build a request for a given step and proposal, always
    /// tagged as slot 0 -- this crate's tests are single-slot throughout
    /// (see [`RecordRequest::slot`]'s docs), and slot-routing behavior is
    /// exercised in `queso-smr` instead.
    fn req<V>(step: u64, p: Proposal<V>) -> RecordRequest<V> {
        RecordRequest {
            slot: 0,
            req_step: step,
            proposal: p,
        }
    }

    fn p(value: u64, priority: u64, origin: u32) -> Proposal<u64> {
        Proposal {
            value,
            priority,
            origin: NodeId(origin),
        }
    }

    #[test]
    fn handle_echoes_req_step_and_returns_isr_summary() {
        let mut r: Recorder<u64> = Recorder::new();
        let resp = r.handle(req(4, p(1, 10, 0)));
        assert_eq!(resp.slot, 0);
        assert_eq!(resp.req_step, 4);
        assert_eq!(resp.step, 4);
        assert_eq!(resp.first, Some(p(1, 10, 0)));
        assert_eq!(resp.prior_agg, None);
    }

    #[test]
    fn handle_echoes_the_requests_slot_tag() {
        let mut r: Recorder<u64> = Recorder::new();
        let resp = r.handle(RecordRequest {
            slot: 7,
            req_step: 4,
            proposal: p(1, 10, 0),
        });
        assert_eq!(resp.slot, 7);
    }

    #[test]
    fn handle_is_equivalent_to_calling_isr_directly() {
        let mut r: Recorder<u64> = Recorder::new();
        let mut isr = Isr::new();

        let via_recorder = r.handle(req(5, p(1, 1, 0)));
        let via_isr = isr.record(5, p(1, 1, 0));
        assert_eq!(via_recorder.step, via_isr.step);
        assert_eq!(via_recorder.first, via_isr.first);
        assert_eq!(via_recorder.prior_agg, via_isr.prior_agg);

        let via_recorder2 = r.handle(req(6, p(2, 2, 1)));
        let via_isr2 = isr.record(6, p(2, 2, 1));
        assert_eq!(via_recorder2.step, via_isr2.step);
        assert_eq!(via_recorder2.first, via_isr2.first);
        assert_eq!(via_recorder2.prior_agg, via_isr2.prior_agg);
    }

    #[test]
    fn peek_reflects_state_without_recording() {
        let mut r: Recorder<u64> = Recorder::new();
        r.handle(req(4, p(1, 1, 0)));
        let before = r.peek();
        let after = r.peek();
        assert_eq!(before, after, "peek must not itself mutate state");
    }
}
