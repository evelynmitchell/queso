------------------------- MODULE QuePaxaAbstract -------------------------
(***************************************************************************)
(* A TLA+ model of the *abstract* QuePaxa consensus core (Algorithm 1,    *)
(* Section 4.1 of the QuePaxa paper), for a single SMR slot.              *)
(*                                                                         *)
(* Algorithm 1 (paper pseudocode), run by every replica:                  *)
(*                                                                         *)
(*   Input: v <- value preferred by this replica                          *)
(*   repeat                                    // iterate through rounds  *)
(*       p <- <v, random()>                    // prioritized proposal    *)
(*       (P, _)  <- tcast({p})                 // propagate our proposal  *)
(*       (E, P') <- tcast(P)                   // propagate existent sets *)
(*       (C, U)  <- tcast(P')                  // propagate common sets   *)
(*       v <- best(C).value                    // next candidate value    *)
(*       if best(E) = best(U) then deliver(v)  // detect consensus        *)
(*                                                                         *)
(* tcast (threshold synchronous broadcast, Section 4.1.1): at each        *)
(* lock-step time step every live replica i invokes tcast(In_i); one step *)
(* later each replica i receives a pair (R_i, B_i) such that:             *)
(*                                                                         *)
(*   (G1) R_i is the set of all proposals RECEIVED by replica i in this   *)
(*        step, and it includes the inputs of a majority of replicas:     *)
(*        there is some S with |S| > n/2 such that In_j \subseteq R_i     *)
(*        for all j in S.  (R_i is a union of whole input sets: inputs    *)
(*        travel as single messages, so a receiver gets In_j entirely or  *)
(*        not at all.)                                                    *)
(*   (G2) B_i is the input In_j of SOME replica j (possibly different    *)
(*        for each receiver i) that was broadcast to everyone:            *)
(*        In_j \subseteq R_k for ALL replicas k.                          *)
(*                                                                         *)
(* The three tcast steps give each replica its existent set E_i, its     *)
(* common set C_i and its universal set U_i with the cross-node subset   *)
(* relationship  U_i \subseteq C_j \subseteq E_i  for all i, j -- the    *)
(* property from which Agreement follows (Section 4.1.3).  This model    *)
(* re-derives that relationship from (G1)/(G2) alone and Assert()s it at *)
(* the end of every round, rather than assuming it.                      *)
(*                                                                         *)
(* MODEL STRUCTURE.  A round is three atomic lock-step actions, one per  *)
(* tcast (Phase1/Phase2/Phase3); each action resolves ONE tcast for all  *)
(* replicas simultaneously.  Splitting the round into three actions      *)
(* (instead of one action nesting all three tcasts) is what makes TLC    *)
(* terminate: the nondeterministic choices of successive tcasts compose  *)
(* additively through deduplicated intermediate states instead of        *)
(* multiplying inside a single action.  See spec/README.md for the full  *)
(* list of state-space reductions and the soundness argument for each.   *)
(*                                                                         *)
(* HOW A TCAST OUTCOME IS CHOSEN.  Rather than enumerating raw subset    *)
(* vectors and filtering (which explodes), each tcast's outcome is       *)
(* parametrized by exactly the choices the guarantees leave open:        *)
(*   - which majority of senders each receiver i heard from (G1), which  *)
(*     determines a "base" received set  base_i \in RVals(In) , the set  *)
(*     of possible majority-unions of inputs; and                        *)
(*   - which origin's input each receiver's B_i is (G2), i.e. a vector   *)
(*     u \in [Replicas -> InVals(In)]  of whole input sets.              *)
(* Every chosen origin's input must reach EVERYONE (G2), so with         *)
(*  M = UNION { u_m : m \in Replicas }  the delivered sets are           *)
(*  R_i = base_i \cup M  and  B_i = u_i.  This generates precisely the   *)
(* set of outcome vectors permitted by (G1)+(G2) -- no filtering, no     *)
(* invalid choices, no fabricated proposals -- see README.md,            *)
(* "Faithfulness of the tcast parametrization", for the completeness     *)
(* argument.  Enumerating over the VALUE spaces RVals/InVals (rather     *)
(* than over sender-majority and origin-index functions) is what makes   *)
(* TLC fast: distinct choice functions with identical delivered sets     *)
(* (e.g. whenever several replicas fed tcast the same input, as is       *)
(* typical from round 2 on) are enumerated once, not once per function.  *)
(***************************************************************************)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Replicas,   \* the fixed, static set of replicas for the slot
    Values,     \* small domain of proposable values (e.g. {v1, v2})
    MaxRound,   \* round bound for finite-state checking (2 suffices; see README)
    None        \* sentinel model value: "not decided yet" (not in Values)

ASSUME IsFiniteSet(Replicas) /\ Replicas # {}
ASSUME IsFiniteSet(Values)   /\ Values # {}
ASSUME MaxRound \in Nat /\ MaxRound >= 1

N == Cardinality(Replicas)

(***************************************************************************)
(* Priorities.  The paper attaches a fresh high-entropy random priority   *)
(* to each proposal, whose only safety-relevant property is that          *)
(* priorities never tie within a round (footnote 4).  Two reductions:     *)
(*                                                                         *)
(*  - Domain 1..N: only the relative order of the N priorities in a       *)
(*    round matters (best() compares them; nothing else reads them), and  *)
(*    proposals of different rounds are never compared -- every set the   *)
(*    algorithm builds contains only current-round proposals.  So an      *)
(*    injective assignment Replicas -> 1..N captures every possible       *)
(*    within-round ranking.  Exact, loses nothing.                        *)
(*                                                                         *)
(*  - Fixed across rounds: the assignment is chosen once, in Init (TLC    *)
(*    explores ALL N! injective assignments as distinct initial states)   *)
(*    and kept for the whole behavior, instead of being re-drawn every    *)
(*    round.  This is the key branching-factor fix versus re-permuting    *)
(*    each round.  Soundness argument in README.md ("Reduction R1"):      *)
(*    in brief, round-1 states already range over ALL value vectors       *)
(*    (inp is unconstrained) x ALL rankings, so every possible single     *)
(*    round of the varying-priority protocol appears as a round-1         *)
(*    instance here; round 2 exercises the only cross-round state,        *)
(*    the decided flags and carried v; Agreement for unbounded varying-  *)
(*    priority behaviors then follows by the induction spelled out in    *)
(*    the README, whose glue invariant (DecisionUnanimity) is checked.   *)
(***************************************************************************)
Priorities == 1..N

Injections == { f \in [Replicas -> Priorities] :
                  \A a, b \in Replicas : a # b => f[a] # f[b] }

Majorities == { S \in SUBSET Replicas : 2 * Cardinality(S) > N }

Proposals == [val : Values, pri : Priorities]

(***************************************************************************)
(* best(S): the highest-priority proposal in S.  Well-defined on every    *)
(* set the algorithm builds: those sets are nonempty (each contains at    *)
(* least a majority's inputs or a whole input) and contain only           *)
(* current-round proposals, whose priorities are pairwise distinct        *)
(* (injective assignment), so the maximum is unique and CHOOSE is         *)
(* deterministic.                                                          *)
(***************************************************************************)
Best(S) == CHOOSE p \in S : \A q \in S : q.pri <= p.pri

VARIABLES
    pri,      \* [Replicas -> Priorities]: injective, fixed at Init (see above)
    inp,      \* [Replicas -> Values]: original round-1 inputs (fixed; for Validity)
    v,        \* [Replicas -> Values]: current candidate value of each replica
    decided,  \* [Replicas -> Values \cup {None}]: decision flag (decide-once latch)
    round,    \* current round number (all replicas in lock-step)
    phase,    \* 1..3: which of the round's three tcast steps happens next
    P,        \* [Replicas -> SUBSET Proposals]: output of tcast 1 (valid in phase 2)
    E,        \* [Replicas -> SUBSET Proposals]: existent sets, output R of tcast 2
    PP        \* [Replicas -> SUBSET Proposals]: P', the per-replica output B of tcast 2

vars == <<pri, inp, v, decided, round, phase, P, E, PP>>

\* Canonical "not in use" value for the intermediate buffers.  Buffers are
\* reset to this as soon as they are consumed, so stale intermediate data
\* never multiplies distinct states.
NoSets == [i \in Replicas |-> {}]

\* This round's prioritized proposals:  p_i = <v_i, priority_i>.
Prop == [i \in Replicas |-> [val |-> v[i], pri |-> pri[i]]]

(***************************************************************************)
(* The two per-receiver choice spaces of a tcast with input vector In     *)
(* (see the header comment):                                              *)
(*   RVals(In):  the possible "base" received sets -- unions of the whole *)
(*               inputs of some majority of senders (G1);                 *)
(*   InVals(In): the possible B outputs -- the whole input of some        *)
(*               replica (G2).                                            *)
(* Both are sets of VALUES, so TLC never enumerates two choice functions  *)
(* that deliver identical sets.                                           *)
(***************************************************************************)
RVals(In)  == { UNION { In[k] : k \in Z } : Z \in Majorities }
InVals(In) == { In[k] : k \in Replicas }

TypeOK ==
    /\ pri \in Injections
    /\ inp \in [Replicas -> Values]
    /\ v \in [Replicas -> Values]
    /\ decided \in [Replicas -> Values \cup {None}]
    /\ round \in 1..(MaxRound + 1)
    /\ phase \in 1..3
    /\ P  \in [Replicas -> SUBSET Proposals]
    /\ E  \in [Replicas -> SUBSET Proposals]
    /\ PP \in [Replicas -> SUBSET Proposals]

Init ==
    /\ pri \in Injections               \* all N! rankings explored from Init
    /\ inp \in [Replicas -> Values]     \* all input assignments explored
    /\ v = inp
    /\ decided = [i \in Replicas |-> None]
    /\ round = 1
    /\ phase = 1
    /\ P = NoSets /\ E = NoSets /\ PP = NoSets

(***************************************************************************)
(* Phase 1:  (P, _) <- tcast({p}).                                        *)
(* Inputs are the singletons {p_i}.  Algorithm 1 discards this tcast's B  *)
(* output, but a real tcast always produces one, which constrains R: at   *)
(* least one input must have reached everybody.  A single existential     *)
(* broadcast input b captures this exactly: an outcome with several       *)
(* broadcast-to-all inputs is reproduced by putting the extra ones into   *)
(* the receivers' base majorities (Majorities contains every superset of  *)
(* a majority).                                                            *)
(***************************************************************************)
Phase1 ==
    /\ phase = 1
    /\ round <= MaxRound
    /\ LET In1 == [i \in Replicas |-> {Prop[i]}]
       IN \E base \in [Replicas -> RVals(In1)], b \in InVals(In1) :
              P' = [i \in Replicas |-> base[i] \cup b]
    /\ phase' = 2
    /\ UNCHANGED <<pri, inp, v, decided, round, E, PP>>

(***************************************************************************)
(* Phase 2:  (E, P') <- tcast(P).                                         *)
(* u[i] is receiver i's B output (G2 lets different receivers identify    *)
(* different broadcast origins, so u is a per-receiver choice -- this is  *)
(* what lets replicas enter the third tcast with DIFFERENT inputs P'_i,   *)
(* the scenario that genuinely stresses Agreement).  Every chosen         *)
(* origin's input reached everyone (M below), so it appears in every E_i. *)
(***************************************************************************)
Phase2 ==
    /\ phase = 2
    /\ \E base \in [Replicas -> RVals(P)], u \in [Replicas -> InVals(P)] :
           LET M == UNION { u[m] : m \in Replicas }
           IN /\ E'  = [i \in Replicas |-> base[i] \cup M]
              /\ PP' = u
    /\ P' = NoSets                       \* consumed; reset to avoid state blowup
    /\ phase' = 3
    /\ UNCHANGED <<pri, inp, v, decided, round>>

(***************************************************************************)
(* Phase 3:  (C, U) <- tcast(P');  v <- best(C).value;                    *)
(*           if best(E) = best(U) then deliver(v).                        *)
(* C and U are not stored in the state: they are consumed within the      *)
(* same lock-step tcast step that produces them, so they are computed     *)
(* and used inside this action.  The paper's cross-node subset            *)
(* relationship U_i \subseteq C_j \subseteq E_i (for ALL i,j) is a        *)
(* THEOREM of guarantees (G1)/(G2); we do not assume it anywhere, and we  *)
(* check it here with Assert -- TLC halts with an error if any reachable  *)
(* tcast outcome violates it.                                              *)
(* The decision latch implements the paper's "local decision flag ...     *)
(* to decide only once per slot" (Section 4.1.3): a replica that has      *)
(* already decided keeps participating (proposing, updating v) but never  *)
(* overwrites its decision.  The delivered value is v after the           *)
(* assignment v <- best(C).value, exactly as in Algorithm 1.              *)
(***************************************************************************)
Phase3 ==
    /\ phase = 3
    /\ \E base \in [Replicas -> RVals(PP)], u \in [Replicas -> InVals(PP)] :
           LET M == UNION { u[m] : m \in Replicas }
               C == [i \in Replicas |-> base[i] \cup M]
               U == u
           IN /\ Assert(\A i, j \in Replicas :
                            U[i] \subseteq C[j] /\ C[j] \subseteq E[i],
                        "cross-node containment U_i <= C_j <= E_i violated")
              /\ v' = [i \in Replicas |-> Best(C[i]).val]
              /\ decided' = [i \in Replicas |->
                                 IF decided[i] # None
                                 THEN decided[i]
                                 ELSE IF Best(E[i]) = Best(U[i])
                                      THEN Best(C[i]).val
                                      ELSE None]
    /\ E' = NoSets /\ PP' = NoSets       \* consumed; reset
    /\ round' = round + 1
    /\ phase' = 1
    /\ UNCHANGED <<pri, inp, P>>

(***************************************************************************)
(* Once the round bound is exhausted, stutter forever rather than         *)
(* deadlocking: MaxRound is a state-space bound for TLC, not part of the  *)
(* algorithm (whose "repeat" iterates unboundedly).                       *)
(***************************************************************************)
Done ==
    /\ phase = 1
    /\ round > MaxRound
    /\ UNCHANGED vars

Next == Phase1 \/ Phase2 \/ Phase3 \/ Done

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(***************************************************************************)
(* Safety properties.                                                     *)
(***************************************************************************)

\* Agreement: no two replicas ever decide different values.
Agreement ==
    \A i, j \in Replicas :
        (decided[i] # None /\ decided[j] # None) => decided[i] = decided[j]

\* Validity: a decided value was some replica's original round-1 input.
Validity ==
    \A i \in Replicas :
        decided[i] # None => \E j \in Replicas : decided[i] = inp[j]

\* Integrity / decide-once: a decision, once made, is never changed.
\* Checked as an action property over the actual transition relation, not
\* merely trusted from the shape of Phase3's IF.
DecideOnce ==
    [][ \A i \in Replicas :
            decided[i] # None => decided'[i] = decided[i] ]_vars

\* The crux of the paper's Agreement proof (Section 4.1.3), and the glue
\* of the induction that extends this bounded check to unbounded rounds
\* (README.md, Reduction R2): the moment any replica has decided x, every
\* replica's candidate value is x -- so all later rounds can only ever
\* propose, carry and decide x.
DecisionUnanimity ==
    \A i, j \in Replicas :
        decided[i] # None => v[j] = decided[i]

=============================================================================
