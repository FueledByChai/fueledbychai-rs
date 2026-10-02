# 0005 — The library owns order truth: a monotone order lattice, permits, and one ExecutionPlanner

Status: accepted
Date: 2026-10-02

## Context

The Java stack's order bugs: zombie orders, partial fills counted twice, a synthetic
PENDING_CANCEL arriving after CANCELED and regressing state, intents that never clear, NotFound
treated as terminal, and a reconnect storm that desynchronized the order map until orphaned
orders swept through a falling market (design §4.9, Appendix A items 1–5, 9–11). The fix must
sit where every consumer gets it, so it belongs in the library (0001).

## Decision

`fbc-oms` owns order truth (design D5, §4.9, §4.10):

- **A monotone lattice.** `OrderRecord` states are ranked (PendingNew/Unknown, Open,
  PartiallyFilled, Terminal); a terminal state is absorbing, and a terminal venue event always
  applies unless it carries a superseded venue id.
- **Two fill counters.** `cum_venue` is the maximum cumulative fill the venue reported;
  `cum_fills` is the sum of fill events deduplicated by the `FillLedger`. Filled quantity is
  their maximum, never their sum. Inventory changes only through deduplicated fills.
- **Unknown is never resent.** A timeout, NotFound or per-item Unknown moves an order to
  Unknown, which counts as fully resting and is resolved by the Unknown ladder (query, watermarked
  snapshot absence, tombstone cancel).
- **Permits.** Commands are built through type-state permits: `Live` for amend, `Cancellable`
  for any non-terminal order. Every command carries every field the venue needs.
- **One planner.** `ExecutionPlanner` turns a desired book into venue commands and is the only
  reader of `OrderCaps` (0003). It orders cancels, then reducing orders, then amends, then adds,
  and normal traffic stops at each rate scope's safety floor.
- **Invariants I1–I9** (design §4.9) are property-tested. Stated here so a ticket in this
  repository can test them without the design:
  - **I1** The final state does not depend on event order, duplicates or ties in ordering keys
    whenever a terminal event is present (cumulative order updates and fills for the same
    execution, interleaved in both orders, with equal keys).
  - **I2** A terminal order never leaves the terminal state; a terminal event always applies
    unless it carries a superseded venue id.
  - **I3** Inventory changes only through fills deduplicated by the `FillLedger` (keyed by the
    venue fill id, or by venue order id and cumulative quantity after the fill). Replayed or
    snapshot fills only reconcile: they apply only if absent from the ledger and newer than the
    session-start watermark. Order state follows `max(cum_venue, cum_fills)`, never the sum.
  - **I4** Orders from a foreign namespace are never cancelled individually and their fills are
    flagged, not counted; the one exception is I7's escalation.
  - **I5** NotFound, an RPC timeout or a per-item Unknown moves an order to Unknown, which the
    Unknown ladder resolves and which counts as fully resting.
  - **I6** Pre-trade resting cap: `|pos + Σ resting same side (incl. PendingNew and Unknown) +
    new| ≤ cap`; if it fails, the command is never built (0010).
  - **I7** Orphans: when a trustworthy resync finds own-namespace orders the OMS does not know,
    the adding side is suppressed for a cooldown the consumer configures and the orphans are
    cancelled by explicit reference in a cancel-many. Above the level limit it escalates to an
    instrument cancel-all, but only under an exclusive account-and-instrument lease with no
    foreign-namespace orders visible; otherwise it repeats explicit cancels on each resync. If
    the venue's snapshot source is not trustworthy, it acts only on orders two sources confirm.
  - **I8** (library form) Every raw order event is emitted to an audit sink in receive order,
    before it is applied, even if it is then ignored. Writing that sink to the consumer's
    `order_audit` table and backfilling gaps losslessly from the journal belong to the consumer;
    neither the table's schema nor the legacy status names live here (0001).
  - **I9** Every intent resolves within its intent timeout plus a query round trip, or escalates
    to the Unknown ladder; no order stays outside the permits indefinitely.
  The pre-trade checks are one of the safety rules in 0010.

## Alternatives

- Mutable tickets in a per-broker registry, as in the Java library: rejected for the bugs above.
- A single fill counter (venue cumulative only, or event sum only): rejected. Venues deliver
  cumulative order updates and incremental fills in either order, and either counter alone
  either misses or double-counts.

## Consequences

- Adapters hold no order registry; the OMS resolves every field of a command from its record.
- The planner's input type is defined in this library, so strategy code (private) produces it
  without `fbc-oms` depending on strategy crates.
- Proptests and the conformance fault scripts (reconnect storm, PENDING_CANCEL after CANCELED,
  fill before order event) are part of the check once `fbc-oms` exists.

## What would show this was wrong

- A property test or a live session shows a lattice violation: a terminal order leaving
  terminal, a partial fill counted twice, or inventory moving without a deduplicated fill.
- An order stays outside every permit beyond its intent timeout plus a query round trip (I9).
- An orphan or desync incident in live trading that the invariants claim to prevent.
