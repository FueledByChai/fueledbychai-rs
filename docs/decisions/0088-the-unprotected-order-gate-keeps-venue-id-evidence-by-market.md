# 0088 — The unprotected-order gate keeps venue-id evidence by market, after a resync only for markets an order is held on, augmenting 0080

Status: accepted
Date: 2026-10-08

## Context

Reviewer B on PR #127 (FBC-nvxn), filed as FBC-48j7. 0080 keeps what order events and query
answers show of venue ids while a resync runs, and after it for the markets an order is held
on, and drops it "once nothing is held after the resync". While one order stays held, that
reset never comes: every later epoch's resync window adds evidence for every market, and none
of it is dropped (RB-nvxn-5: one order held, 20 reconnects with 10 cancel events elsewhere
each, 200 ids kept). An order held for good (a lagging id on a venue whose amends issue new
ids, a query answering that the venue has no such order, or a consumer that does not implement
`on_unprotected`) grows it without bound, the cost 0080 says the runtime avoids. The gate's
check on the send path also built a list of the command's markets on every place, batch and
amend, even with nothing unprotected (RB-nvxn-3). And some of 0080's wording no longer matches
the code (RB-nvxn-6).

## Decision

- **Evidence is kept by market.** Each fact (an id shown ended or replaced, the id an amend
  replaced it by, an id evidence naming our client id showed) is kept under the market of the
  event or query answer that showed it, and is applied only to the orders held on that market.
  A venue order id names one order, and our client id one order and the orders its amends
  replaced it by, which all rest on one market.
- **It is pruned to the markets an order is held on** when a resync ends, and when, after the
  resync, the last order held on a market is released. What is kept is then bounded by the
  evidence heard on the markets held and that heard since the latest resync ended, not by the
  number of reconnects whose resyncs end. The rule 0080 states for nothing held (everything
  dropped) is the case of no market held.
- **An epoch replaced before its resync ends drops nothing.** Its snapshot never completed, so
  what it heard is kept into the next epoch's window and applies to that epoch's snapshot,
  which may show the order under an id older than one that evidence already linked to our
  client id or to an amend. Dropping it there would let an end of that older id release the
  market while the order may still rest under the linked id (Reviewer B RB-48j7-1 on PR #140),
  so it is kept, as 0080 kept it, until a resync ends.
- **The send-path check allocates nothing.** `Gate::admits` tests each item's market in place,
  and with nothing unprotected looks at no item of a batch. This holds by construction (it
  builds no heap value) and is not proven by a test: counting allocations needs a global
  allocator whose implementation is unsafe code, which the workspace has none of (0021).
  FBC-ehkm covers a counting harness.
- **0080's wording, as the code is.** Where 0080 differs, this record holds:
  - "What releases the market": the ends and replacements that count are those an order event
    or a query answer showed on an epoch while evidence was kept for the order's market (while
    a resync ran, or while an order was held there), not those of any epoch.
  - "Consequences": a consumer that does not implement `on_unprotected` leaves those markets
    held across epochs, not for the epoch: the order is told again on each later epoch and
    stays held until an event or a query answer shows it ended under every venue id it was
    seen under.
  - "What releases the market", last sentence: only FBC-066c (a trustworthy resync that leaves
    the order out releases it) covers releasing an order held under a lagging id. FBC-41iu
    covers an order completed by native fills alone. A query does not release a lagging id:
    an answer by our client id shows the current id, and one by the lagging id finds nothing.

## Alternatives

- Keep, after the resync, only the facts that reach a held order's ids or client id: rejected.
  An amend heard out of order (the second before the first) reaches the order only once the
  first arrives, so a fact that touches nothing yet may be needed later; the order's market is
  the narrowest set known to hold every such fact.
- Prune also when an epoch is replaced, or keep only the replaced window's facts for one more
  window: rejected (RB-48j7-1). Either drops evidence a later snapshot needs when epochs drop
  before their resyncs end (one in a row, or two), and releases a market early.
- Drop every fact at each resync's end, even on held markets: rejected. Evidence heard before
  an amend that links it to the order (0080) would be lost, and the order held longer than the
  venue's reports require.

## Consequences

- Proven in `exec_gate.rs`: `facts_stay_bounded_across_many_reconnects_while_one_order_stays_held`
  (40 reconnects, half dropped before their resync ends, ten amends and ends on another market
  in each window; a dropped window's facts, and only those, are carried into the next, nothing
  is kept after each resync ends, and the order is still released by its own end),
  `facts_are_kept_for_a_held_market_and_dropped_once_it_is_released` and
  `evidence_heard_in_resyncs_of_dropped_epochs_is_kept_for_a_later_snapshot` (two epochs drop
  before their resyncs end; the third's snapshot under the old id stays held after that id
  ends, until the id our client id was shown under ends).
- A run of reconnects none of whose resyncs ends keeps everything heard in it, on every
  market, as 0080 did. The facts are sets, so an order heard again on each reconnect adds
  nothing; they grow only with the distinct ids the account's orders show in that run, and
  while no resync ends the session admits no place or amend of ours. The first resync that
  ends prunes them to the markets held.
- Evidence on a market whose order was released, or heard in an earlier epoch's resync that
  ended on a market held only from a later epoch, is gone. A later snapshot showing an order
  there that an earlier end already ended holds it until the consumer queries it, as 0080
  already says of ends heard while nothing was held. A later snapshot staler than that
  evidence (one that omitted the order while the evidence was heard, then shows it under an id
  older than one the evidence linked it to) can be released by an end of the older id, as under
  0080 whenever no market was held as that resync ended. FBC-066c covers resyncs that lag the
  order.
- On a market held for good, evidence about orders the account's other systems trade there is
  still kept while it stays held; FBC-ed5z covers bounding it.

## What would show this was wrong

- A held order released on one market by evidence heard on another, or kept held because
  evidence of its own market was dropped while it was held.
- The gate's kept evidence growing with the number of reconnects whose resyncs end while the
  markets held stay the same.
- A held order released by an end of the id a snapshot showed while evidence heard in an
  earlier epoch's unfinished resync linked it to another id not shown ended.
