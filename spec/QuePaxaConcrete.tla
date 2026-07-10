------------------------- MODULE QuePaxaConcrete -------------------------
(***************************************************************************)
(* A TLA+ model of the *concrete* QuePaxa consensus protocol (Section 4.2 *)
(* of the QuePaxa paper) for a single SMR slot, in its leaderless form:   *)
(*                                                                         *)
(*   - passive RECORDERS, each holding the specialized constant-space     *)
(*     integer interval summary register (ISR) of Algorithm 3: current    *)
(*     step S, first-value F_c, current aggregate A_c (max), prior        *)
(*     aggregate A_p, with stale-step discard (s < S), the "+1" carry     *)
(*     of A_c into A_p, and the skip-to-nil rule;                         *)
(*   - active PROPOSERS running Algorithm 4: the threshold logical clock  *)
(*     step s = 4*round + phase, the four phases per round, a majority    *)
(*     quorum of recorder replies per step, the pipelined spread/gather   *)
(*     sequence, the phase-2 decision rule, and proposer catch-up;        *)
(*   - genuine ASYNCHRONY: proposers and recorders progress at            *)
(*     independent rates, recorders sit at different steps, requests and  *)
(*     replies are arbitrarily delayed, reordered, dropped and retried.   *)
(*                                                                         *)
(* This is the Phase-2 counterpart of spec/QuePaxaAbstract.tla (the       *)
(* abstract Algorithm-1 core): the concrete protocol SIMULATES the        *)
(* abstract one (paper Appendix C), and this model checks the same        *)
(* safety properties -- Agreement, Validity, Integrity/DecideOnce --      *)
(* directly against the concrete mechanism, plus in-action Asserts and    *)
(* invariants that mirror the Appendix C simulation lemmas:               *)
(*                                                                         *)
(*   Lemma C.2  (recorder reply step s' >= s)  -> Assert in Rpc, plus     *)
(*              the RepliesAhead state invariant;                          *)
(*   Lemma C.3  (catch-up adopts a valid (s', f') state) -> the CatchUp   *)
(*              branch of ProcessQuorum, with f' # nil Assert;            *)
(*   Lemma C.4  (phase 0 computes best of a majority's first values) ->   *)
(*              the ph = 0 branch;                                         *)
(*   Lemma C.5  (spread/gather realizes tcast property T2: at least one   *)
(*              gathered prior-step aggregate is non-nil, carrying the    *)
(*              successfully-spread proposal) -> Asserts in the ph = 2    *)
(*              and ph = 3 branches (the model would otherwise be free    *)
(*              to return an empty gather; the Assert checks that the     *)
(*              +1-carry/quorum-intersection argument of C.5 really      *)
(*              closes in every reachable interleaving);                   *)
(*   Lemma C.7/C.8 (phases 1-3 compute best(E) and best(C)) -> the        *)
(*              ph = 2 / ph = 3 gather computations over prior_agg;       *)
(*   Lemma C.9  (asynchronous decision path: deciding p.value in phase 2  *)
(*              when p = best of the gathered aggregates equals abstract  *)
(*              QuePaxa's best(E) = best(U) decision) -> the ph = 2       *)
(*              decision rule, plus the DecisionDominance invariant       *)
(*              below, the concrete analogue of the abstract model's      *)
(*              DecisionUnanimity glue: once anyone has decided v, every  *)
(*              proposer that reaches any LATER round carries value v.    *)
(*                                                                         *)
(* The leader fast path (Lemma C.10, priority H, s = 4 only) is NOT       *)
(* modeled: this mirrors the leaderless concrete core that is actually    *)
(* implemented in crates/consensus/src/proposer.rs, where no drawn        *)
(* priority can ever equal H and the fast-path test is statically dead.   *)
(*                                                                         *)
(* HOW THE NETWORK IS MODELED (and why no message queues are needed).     *)
(* All communication is RPC-style, proposer-to-recorder (Section 4.2.1).  *)
(* A recorder handles a record(s,p) request atomically, and the reply's   *)
(* content is fixed at that handling instant.  The single action          *)
(* Rpc(i, r) therefore performs one record() at recorder r with proposer  *)
(* i's current step and per-recorder proposal, and then EITHER delivers   *)
(* the reply into i's reply buffer OR loses it (nondeterministically).    *)
(* A proposer may re-invoke Rpc toward any recorder it has no buffered    *)
(* reply from -- the implementation's retry loop (the ISR is idempotent   *)
(* for repeated same-(s,p) records, so retries are safe, and the          *)
(* re-invocation reply carries the recorder's FRESH state, exactly like   *)
(* a real retry reply).  Requests and replies of steps a proposer has     *)
(* since abandoned need no explicit representation: see README.md         *)
(* ("Reduction C1") for the commutation argument that every               *)
(* delayed/stale-delivery schedule reaches the same states as some        *)
(* schedule in this model.                                                 *)
(*                                                                         *)
(* Quorum processing is folded into the Rpc that buffers the Quorum-th    *)
(* reply, matching the implementation (crates/consensus/src/proposer.rs   *)
(* processes exactly when responses.len() hits the threshold) and the     *)
(* paper's Await; the reply buffer is reset the moment it is consumed,    *)
(* so no stale intermediates multiply distinct states (the                *)
(* QuePaxaAbstract lesson).                                                *)
(***************************************************************************)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    NP,       \* number of active proposers (paper Appendix D baseline: 2)
    Recorders,\* the set of recorders, as symmetric model values (paper
              \* Appendix D baseline: 3).  Recorders are interchangeable:
              \* no recorder id ever enters a proposal or an ordering (the
              \* implementation's recorder-id tiebreak in catch-up is
              \* modeled as nondeterminism, a strict superset), so TLC's
              \* SYMMETRY reduction on this set is sound for the safety
              \* properties checked here.
    NV,       \* values are 1..NV
    PriMax,   \* random priorities are drawn from 1..PriMax (baseline: 2,
              \* i.e. the paper's "1-bit random priorities")
    MaxStep,  \* highest logical-clock step executed (baseline: 11 = two
              \* full rounds, steps 4..11, as in the paper's Appendix D)
    None      \* sentinel model value: nil proposal / not decided

ASSUME NP \in Nat /\ NP >= 2
ASSUME IsFiniteSet(Recorders) /\ Cardinality(Recorders) >= 3
ASSUME NV \in Nat /\ NV >= 2
ASSUME PriMax \in Nat /\ PriMax >= 2
ASSUME MaxStep \in Nat /\ MaxStep >= 7   \* at least one full round + phase 0

Proposers  == 1..NP
Values     == 1..NV
Priorities == 1..PriMax
NR         == Cardinality(Recorders)
Quorum     == (NR \div 2) + 1            \* majority of the fixed membership

RecSym     == Permutations(Recorders)    \* SYMMETRY set for TLC

(***************************************************************************)
(* Proposals.  Algorithm 4 packs <priority, proposer, value> into one     *)
(* fixed-width integer so that the ISR's integer max is a lexicographic   *)
(* comparison on (priority, proposer, value).  We keep the record form    *)
(* and compare (pri, org) lexicographically; the value never needs to    *)
(* act as a tiebreaker, because within any step a proposer sends one     *)
(* value, and every comparison the protocol performs is between          *)
(* proposals of the same step -- so (pri, org) already determines the    *)
(* proposal (see README.md, "Faithfulness of the proposal order").       *)
(* Priority ties across proposers are broken by proposer id: this is     *)
(* precisely the paper's "tiebreaking" scheme (Appendix A), the one its  *)
(* own Promela models employ, and what the Rust Proposal Ord implements. *)
(***************************************************************************)
Proposals == [pri : Priorities, org : Proposers, val : Values]

Better(p, q) == \/ p.pri > q.pri
                \/ /\ p.pri = q.pri
                   /\ p.org > q.org

\* Highest-(pri, org) proposal of a nonempty set S of same-step proposals.
\* The maximum is unique: two same-step proposals with equal (pri, org)
\* are the same proposal, so CHOOSE is deterministic.
BestOf(S) == CHOOSE p \in S : \A q \in S : ~Better(q, p)

\* Algorithm 3's aggregate: integer max, with nil as the identity.
AggMax(a, p) == IF a = None THEN p ELSE IF Better(p, a) THEN p ELSE a

VARIABLES
    \* --- recorder state: one integer ISR each (Algorithm 3) ---
    rS,       \* [Recorders -> step]: S, current logical clock step (init 0)
    rF,       \* [Recorders -> Proposal|None]: F_c, first value in step S
    rAc,      \* [Recorders -> Proposal|None]: A_c, max value seen in step S
    rAp,      \* [Recorders -> Proposal|None]: A_p, max value seen in S-1
    \* --- proposer state (Algorithm 4) ---
    pStep,    \* [Proposers -> step]: s, the threshold logical clock
    pProp,    \* [Proposers -> Proposal]: p, the working proposal template
    pDec,     \* [Proposers -> Values|None]: decision latch (decide once)
    pDecStep, \* [Proposers -> step|0]: step at which the decision was made
    pReplies, \* [Proposers -> partial map Recorders -> reply]: replies
              \* buffered for the CURRENT step (< Quorum entries: the
              \* Quorum-th reply is processed in the transition that
              \* buffers it, and the buffer is reset)
    inp       \* [Proposers -> Values]: original inputs (fixed; Validity)

vars == <<rS, rF, rAc, rAp, pStep, pProp, pDec, pDecStep, pReplies, inp>>

NoReplies == [r \in {} |-> None]      \* the empty reply buffer

\* Initial inputs, canonicalized under value permutation: no operator in
\* this module inspects a value other than by equality (Better and BestOf
\* order proposals by (pri, org) only), so every behavior is equivariant
\* under a bijection of Values, and it suffices to explore one input
\* vector per equivalence class -- the "restricted growth" vectors, e.g.
\* for NP = 2: <<1, 1>> and <<1, 2>> (covering <<2, 2>> and <<2, 1>>).
\* See README.md, "Reduction C6".
SetMax(S) == CHOOSE m \in S : \A x \in S : x <= m
CanonicalInputs ==
    { f \in [Proposers -> Values] :
        \A k \in Proposers : f[k] <= 1 + SetMax({0} \cup {f[j] : j \in 1..(k - 1)}) }

Init ==
    /\ inp \in CanonicalInputs        \* one representative per value-permutation class
    /\ rS  = [r \in Recorders |-> 0]
    /\ rF  = [r \in Recorders |-> None]
    /\ rAc = [r \in Recorders |-> None]
    /\ rAp = [r \in Recorders |-> None]
    /\ pStep = [i \in Proposers |-> 4]          \* round 1, phase 0
    \* Initial template p <- <H, i, v>; the placeholder priority is never
    \* sent (phase 0 always draws per-recorder priorities), so it is
    \* canonicalized to 1 rather than left as a distinct H value.
    /\ pProp = [i \in Proposers |-> [pri |-> 1, org |-> i, val |-> inp[i]]]
    /\ pDec      = [i \in Proposers |-> None]
    /\ pDecStep  = [i \in Proposers |-> 0]
    /\ pReplies  = [i \in Proposers |-> NoReplies]

(***************************************************************************)
(* ProcessQuorum(i, R): proposer i has gathered a full majority quorum R  *)
(* of replies for its current step (R includes the reply buffered in the  *)
(* same transition).  Exactly Algorithm 4's post-await logic:             *)
(*   - if every reply is at i's step s: run the phase body for s mod 4    *)
(*     and advance to s + 1 (unless a decision was delivered);            *)
(*   - else (some reply ahead; Lemma C.2 rules out behind): catch up to   *)
(*     the highest reported step, adopting that reply's first value as    *)
(*     the new working proposal (Lemma C.3).                              *)
(* The consumed reply buffer is reset in the same transition.             *)
(***************************************************************************)
ProcessQuorum(i, R) ==
    LET s    == pStep[i]
        ph   == s % 4
        Dom  == DOMAIN R
        allAt == \A x \in Dom : R[x].st = s
        \* advance/adopt: new step ns with working proposal np
        MoveTo(ns, np) ==
            /\ pStep'    = [pStep    EXCEPT ![i] = ns]
            /\ pProp'    = [pProp    EXCEPT ![i] = np]
            /\ pReplies' = [pReplies EXCEPT ![i] = NoReplies]
            /\ UNCHANGED <<pDec, pDecStep>>
    IN
    IF ~allAt
    THEN \* -------- proposer catch-up (Lemma C.3) --------
         \* Adopt (s', f') from a highest-step reply.  When several replies
         \* tie for the highest step, the choice is nondeterministic: the
         \* implementation breaks such ties by recorder id, which is one of
         \* the choices explored here (a strict superset, sound for safety,
         \* and what keeps this action symmetric in Recorders).
         \E x \in Dom :
            /\ \A y \in Dom : R[y].st <= R[x].st
            /\ Assert(R[x].st > s,
                      "Lemma C.2: a mixed quorum must contain a reply ahead of s")
            /\ Assert(R[x].f # None,
                      "catch-up: a recorder ahead of step 0 must report a first value")
            /\ MoveTo(R[x].st, R[x].f)
    ELSE \* -------- all replies at step s: run phase s mod 4 --------
         LET Fs == {R[x].f : x \in Dom}
             As == {R[x].a : x \in Dom} \ {None}
         IN CASE ph = 0 ->
                 \* Phase 0 (propose): p <- best_j of f'_j (Lemma C.4).
                 \* (Leader fast-path test omitted: leaderless rounds only,
                 \* no proposal ever carries priority H.)
                 /\ Assert(None \notin Fs,
                           "phase 0: an at-step reply must carry a first value")
                 /\ MoveTo(s + 1, BestOf(Fs))
            []  ph = 1 ->
                 \* Phase 1 (spread E): no action required.
                 MoveTo(s + 1, pProp[i])
            []  ph = 2 ->
                 \* Phase 2 (gather E, spread C): decide if p is best of
                 \* the gathered prior-step aggregates (Lemma C.9).
                 /\ Assert(As # {},
                           "Lemma C.5/C.7: some gathered prior aggregate must be non-nil in phase 2")
                 /\ IF BestOf(As) = pProp[i]
                    THEN \* deliver the decision; the proposer halts
                         /\ pDec'     = [pDec     EXCEPT ![i] = pProp[i].val]
                         /\ pDecStep' = [pDecStep EXCEPT ![i] = s]
                         /\ pReplies' = [pReplies EXCEPT ![i] = NoReplies]
                         /\ UNCHANGED <<pStep, pProp>>
                    ELSE MoveTo(s + 1, pProp[i])
            []  ph = 3 ->
                 \* Phase 3 (gather C): p <- best_j of a'_j (Lemma C.8);
                 \* this becomes the next round's preferred proposal.
                 /\ Assert(As # {},
                           "Lemma C.5/C.8: some gathered prior aggregate must be non-nil in phase 3")
                 /\ MoveTo(s + 1, BestOf(As))

(***************************************************************************)
(* The per-recorder proposal content for proposer i's current step:       *)
(* phases 1..3 send the working proposal p unchanged; phase 0 sends       *)
(* p with a fresh independent random priority PER RECORDER (Section       *)
(* 4.2.4, "Proposal randomization") and origin i, exactly like the        *)
(* implementation (proposer.rs begin_step).  The priority is drawn at     *)
(* RPC time; a re-contact of the same recorder within the same phase-0    *)
(* step (i.e. a retry after a lost reply) may draw a DIFFERENT priority,  *)
(* which over-approximates the implementation (whose `sent` map pins the  *)
(* retry content) -- a strict superset of behaviors, hence sound for the  *)
(* safety properties checked here; see README.md, "Reduction C4".         *)
(***************************************************************************)
PriChoices(i) ==
    IF pStep[i] % 4 # 0 THEN {0} ELSE Priorities   \* 0 = unused outside ph 0

(***************************************************************************)
(* Rpc(i, r, pr): the one protocol action.  Proposer i (undecided, step   *)
(* within bound, no buffered reply from r) invokes record(s, p) at        *)
(* recorder r.  The recorder's ISR handles it atomically (Algorithm 3),   *)
(* and the reply (s', f', a') = (S, F_c, A_p) is then nondeterministically*)
(* either LOST (recorder mutated, proposer learns nothing -- a dropped    *)
(* reply, or one still in flight) or DELIVERED into i's buffer; the       *)
(* delivery that completes the quorum triggers ProcessQuorum in the same  *)
(* transition.  Reply fields that no code path of the current phase can   *)
(* ever read are canonicalized to None before buffering ("Reduction C3"). *)
(***************************************************************************)
Rpc(i, r, pr) ==
    /\ pDecStep[i] = 0                    \* decided proposers halt
    /\ pStep[i] <= MaxStep                \* step bound (parked otherwise)
    /\ r \notin DOMAIN pReplies[i]
    /\ LET s  == pStep[i]
           ph == s % 4
           p  == IF ph = 0
                 THEN [pri |-> pr, org |-> i, val |-> pProp[i].val]
                 ELSE pProp[i]
           \* ---- the integer ISR, Algorithm 3: record(s, p) ----
           adv   == s > rS[r]                       \* advance to higher step
           newS  == IF adv THEN s ELSE rS[r]        \* (s < S: stale discard)
           newF  == IF adv THEN p ELSE rF[r]
           newAc == IF adv THEN p
                    ELSE IF s = rS[r] THEN AggMax(rAc[r], p)   \* aggregate
                    ELSE rAc[r]                                \* stale: drop
           newAp == IF adv
                    THEN IF s = rS[r] + 1 THEN rAc[r]  \* +1: carry A_c
                                          ELSE None    \* skip: saw nothing
                    ELSE rAp[r]
       \* ---- the reply summary (s', f', a') = (S, F_c, A_p) ----
       IN /\ Assert(newS >= s, "Lemma C.2: recorder reply step below request step")
          /\ rS'  = [rS  EXCEPT ![r] = newS]
          /\ rF'  = [rF  EXCEPT ![r] = newF]
          /\ rAc' = [rAc EXCEPT ![r] = newAc]
          /\ rAp' = [rAp EXCEPT ![r] = newAp]
          /\ UNCHANGED inp
          /\ \/ \* ---- reply lost (or still in flight): recorder-side
                \* effect only; the proposer learns nothing ----
                UNCHANGED <<pStep, pProp, pDec, pDecStep, pReplies>>
             \/ \* ---- reply delivered ----
                LET keepF  == newS > s \/ ph = 0
                    keepA  == newS = s /\ ph \in {2, 3}
                    stored == [st |-> newS,
                               f  |-> IF keepF THEN newF ELSE None,
                               a  |-> IF keepA THEN newAp ELSE None]
                    R == [x \in (DOMAIN pReplies[i]) \cup {r} |->
                             IF x = r THEN stored ELSE pReplies[i][x]]
                IN IF Cardinality(DOMAIN R) < Quorum
                   THEN /\ pReplies' = [pReplies EXCEPT ![i] = R]
                        /\ UNCHANGED <<pStep, pProp, pDec, pDecStep>>
                   ELSE ProcessQuorum(i, R)   \* Await satisfied: act on R

(***************************************************************************)
(* Once every proposer has decided or exhausted the step bound, stutter   *)
(* forever: MaxStep bounds the search, it is not part of the algorithm.   *)
(* Every non-terminated state has an enabled Rpc (an undecided, in-bound  *)
(* proposer always has a bufferless recorder to contact, since the buffer *)
(* holds < Quorum <= NR - 1 replies), so TLC's deadlock check is left ON  *)
(* and doubles as the paper's "never gets stuck" sanity property.         *)
(***************************************************************************)
Terminated == \A i \in Proposers : pDecStep[i] # 0 \/ pStep[i] > MaxStep

Done == Terminated /\ UNCHANGED vars

Next ==
    \/ \E i \in Proposers, r \in Recorders :
           \E pr \in PriChoices(i) : Rpc(i, r, pr)
    \/ Done

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(***************************************************************************)
(* Safety properties.                                                     *)
(***************************************************************************)

OptProposal == Proposals \cup {None}

TypeOK ==
    /\ inp \in [Proposers -> Values]
    /\ rS  \in [Recorders -> {0} \cup (4..MaxStep)]
    /\ rF  \in [Recorders -> OptProposal]
    /\ rAc \in [Recorders -> OptProposal]
    /\ rAp \in [Recorders -> OptProposal]
    /\ pStep \in [Proposers -> 4..(MaxStep + 1)]
    /\ pProp \in [Proposers -> Proposals]
    /\ pDec  \in [Proposers -> Values \cup {None}]
    /\ pDecStep \in [Proposers -> {0} \cup (4..MaxStep)]
    /\ \A i \in Proposers :
           /\ DOMAIN pReplies[i] \subseteq Recorders
           /\ Cardinality(DOMAIN pReplies[i]) < Quorum
           /\ \A x \in DOMAIN pReplies[i] :
                  /\ pReplies[i][x].st \in 4..MaxStep
                  /\ pReplies[i][x].f \in OptProposal
                  /\ pReplies[i][x].a \in OptProposal

\* Agreement: no two proposers ever decide different values.
Agreement ==
    \A i, j \in Proposers :
        (pDec[i] # None /\ pDec[j] # None) => pDec[i] = pDec[j]

\* Validity: a decided value was some proposer's original input.
Validity ==
    \A i \in Proposers :
        pDec[i] # None => \E j \in Proposers : pDec[i] = inp[j]

\* Integrity / decide-once: a decision, once made, is never changed.
\* Checked as an action property over the actual transition relation.
DecideOnce ==
    [][ \A i \in Proposers :
            pDec[i] # None => pDec'[i] = pDec[i] ]_vars

\* Logical clocks are monotone: proposer steps and recorder ISR steps
\* never decrease (the premise of Lemma C.2's proof).
Monotone ==
    [][ /\ \A i \in Proposers : pStep'[i] >= pStep[i]
        /\ \A r \in Recorders : rS'[r]  >= rS[r] ]_vars

\* Lemma C.2 as a state invariant: every buffered reply is at or ahead of
\* the awaiting proposer's step (a recorder can never answer from behind).
RepliesAhead ==
    \A i \in Proposers :
        \A x \in DOMAIN pReplies[i] : pReplies[i][x].st >= pStep[i]

\* Decisions happen only on the asynchronous path, in phase 2.
DecisionAtPhase2 ==
    \A i \in Proposers : pDecStep[i] # 0 => pDecStep[i] % 4 = 2

\* The concrete analogue of the abstract model's DecisionUnanimity, i.e.
\* the conclusion of Lemmas C.7-C.9 chained with Lemma B.5: the moment any
\* proposer has decided value v at step d, every proposer whose logical
\* clock has entered any LATER round carries a working proposal with
\* value v -- so later rounds can only ever propose, spread and decide v.
\* This is the glue that extends the bounded-round check to unbounded
\* rounds (README.md, "Reduction C7").
DecisionDominance ==
    \A i \in Proposers :
        pDecStep[i] # 0 =>
            \A k \in Proposers :
                pStep[k] >= 4 * ((pDecStep[i] \div 4) + 1) =>
                    pProp[k].val = pDec[i]

\* ISR sanity (the shape Algorithm 3 maintains): a recorder that has ever
\* recorded anything has a first value and an aggregate at its current
\* step, and the aggregate is at least the first value.
IsrConsistent ==
    \A r \in Recorders :
        /\ (rS[r] = 0) <=> (rF[r] = None)
        /\ (rS[r] = 0) => (rAc[r] = None /\ rAp[r] = None)
        /\ (rF[r] # None) => (rAc[r] # None /\ ~Better(rF[r], rAc[r]))

\* The engine behind Validity: every value the protocol ever materializes
\* (working proposals, ISR registers) is some proposer's original input.
ValuesFromInputs ==
    LET Rng == {inp[j] : j \in Proposers}
    IN /\ \A i \in Proposers : pProp[i].val \in Rng
       /\ \A r \in Recorders :
              /\ rF[r]  # None => rF[r].val  \in Rng
              /\ rAc[r] # None => rAc[r].val \in Rng
              /\ rAp[r] # None => rAp[r].val \in Rng

=============================================================================
