# 0120 — Venue-initiated fills carry their origin and exact price and reach the inventory once through the fill ledger on no order, counting after a snapshot seed unless their arrival or time places them in it, augmenting 0005, 0014, 0055 and 0095

Status: accepted
Date: 2026-10-20

## Context

Paradex's private `FillEvent` (SBE template 21, `paradex_1_0.xml` at the paradex-py commit 0022
cites) carries a `fillType`: FILL (1), LIQUIDATION (2), UNWIND_TRANSFER (3), SETTLE_MARKET (4),
RPI (5) and BLOCK_TRADE (6). FILL and RPI are executions of the account's orders. The other four
are changes the venue makes to the account's position with no order of ours: a liquidation, a
position transfer (whose `seq` is null), a market's settlement and a block trade. FBC-uiy's
decoder (`crates/venues/fbc-venue-paradex/src/exec/fill.rs`) pushes them keyed
`<fillType>:<fillId>`, with no client id, and left how the OMS counts them to FBC-5uk.

A fill with no client id was routed by its venue order id alone. One naming no order of ours was
`FillRouted::Unattributed` under 0005's I4: not counted, and its market put in view, for good
when it names no venue id, which refuses I7's instrument cancel-all. One whose venue id named an
order of ours would have been counted on that order. Meanwhile the venue's position moves, so
the next resync's comparison reports `PositionCheck::Desync` and 0094's stop rule stops the
session. A settlement or transfer is priced by the venue, not by a book, so its price can be off
the instrument's tick grid, and the decoder refused every fill price off the grid as misdecoded:
such a fill was a decode error, lost while the position moved.

The owner's question OQ-venue-fills offered three answers: (a) the codec marks such fills
venue-initiated, with a fill-origin field in fbc-core, and fbc-oms applies them to the inventory
through the `FillLedger` as this account's position changes, reporting each to the consumer,
with I3's deduplication holding; (b) flag them as foreign fills and force a resync that re-reads
the position, which overwrites the inventory mid-session against 0055; (c) treat any of them as
a desync that moves the market to Cancel-only. The owner's answer of 2026-10-04 was (a), given
by default (the owner did not object). That answer did not settle a venue fill delivered after
the seed that the seed does not place. On 2026-10-20 the owner chose, in session, that after a
snapshot seed such a fill counts on the inventory, accepting the residual item 5 states, rather
than leaving the market's position unknown.

## Decision

This augments 0005 (I3 and I4), 0014 item 7 (a fill's client id) and, for venue-initiated fills
only, 0055 and 0095 (placement around the seed). It applies 0004 to the price and 0012 and 0013
to market states. It supersedes nothing.

1. **A fill states its origin.** `FillEvent`'s `cid` and `px` are replaced by
   `origin: FillOrigin` (fbc-core `src/event.rs`). A fill is either
   `FillOrigin::Order { cid: Option<CidMatch>, px: Ticks }` or
   `FillOrigin::Venue { kind: VenueFillKind, px: PxExact }`, with `VenueFillKind` one of
   `Liquidation`, `Transfer`, `Settlement` or `BlockTrade`.
   - An `Order` fill is an execution of an order on the account: ours, another namespace's,
     another system's, or one whose fill echoes no client id. Its `cid` keeps 0014 item 7's
     meaning.
   - A `Venue` fill is a change the venue made itself. A codec declares `Venue` only for a fill
     the venue says it made with no order of the account's.
   - A venue fill names no client id, so one carrying ours cannot be built, and nothing that
     attributes a fill by its client id can take it for ours (0014 item 6's rule).
   - Readers use `FillEvent::cid()` (`None` for a venue fill), `venue_kind()` and `order_px()`,
     or match the origin.
   - Neither type has a `Default` or `#[non_exhaustive]`. No capability declares that a venue
     reports venue fills, since nothing generic would read it.
2. **Its price is exact (applying 0004).** A venue fill's price is a `PxExact`, exactly as the
   venue states it, on or off the grid, never rounded to ticks: 0004 already makes an off-grid
   value a `PxExact`. An order fill keeps its price on the grid, and a codec still refuses one
   off the grid as misdecoded. Sizes stay `Lots` on the size step for both. fbc-oms reads no
   fill price.
3. **Its key (I3 unchanged).** A venue fill is keyed by `FillEvent::key()`, as every fill is:
   by its fill id, or by its venue order id and cumulative quantity. A venue's sequence number
   is never part of the key, so a transfer's null `seq` changes nothing. A fill with neither
   cannot be built (`FillIdent`), so its codec refuses it. A redelivery is
   `Admission::Duplicate`, and a replayed venue fill follows the ledger's replay rules (with a
   publish time alone it is `Admission::Untimed`, never applied).
4. **It counts on the inventory and on no order (augmenting I3 and I4).**
   `Registry::apply_fill` reads the origin before any client-id or venue-id routing.
   - A venue fill moves the inventory once, through the `AcceptedFill` the `FillLedger` gave and
     the commit every counted fill takes, and is reported as
     `FillRouted::Venue { kind, in_snapshot }`.
   - It counts on no order, even one whose venue id it names. No order's `cum_fills`, lattice
     state or venue-id index moves, so an order's filled quantity stays
     `max(cum_venue, cum_fills)` of its own executions.
   - It puts nothing in view (`Registry::foreign_in_view`), is no sighting of an orphan (0108),
     and is not reported as untracked.
   - It is refused like any fill, counting nothing and leaving the ledger as it was, when the
     inventory would overflow (`OmsError::FillOverflow`) or the fill comes from another ledger
     (`OmsError::OtherLedger`).
   - I4 is unchanged for order fills. One with another namespace's or a non-canonical client
     id, or with none and no venue id of an order we hold, is still flagged, not counted, and
     put in view. Only the origin a codec declares exempts a fill, never a missing client id, a
     venue's name or a fill id's prefix: another namespace's order on a venue that echoes no
     client id looks like a fill with none.
5. **Placement around the seed (augmenting 0055 and 0095, for venue fills only).** A venue
   fill's time is its matching-engine time or, without one, its publish time, an upper bound on
   the execution, as the ledger already holds.
   - The seed's snapshot holds a venue fill only when the fill arrived before the resync's
     request was sent (0055's first rule) or its time is at or before the watermark, on either
     read mode. No order shows or omits a venue fill, so neither an order's shown cumulative
     fill nor its absence (0055's second and third rules) ever places one.
   - Before the seed, a venue fill is kept for the seed to place. One neither rule places is
     unsettled under 0055's rule (`ResyncReport::unsettled`): its market stays unseeded until a
     resync requested after the fill arrived.
   - After a snapshot seed, a venue fill the seed holds is kept by the ledger and not counted
     (`in_snapshot: true`). Every other one counts on the inventory, never `Unsettled` (the
     owner's choice of 2026-10-20). For this class of fill only, this departs from 0055's and
     0095's rule that a fill nothing places after the seed leaves the position unknown, as 0108
     item 1 does for an orphan's fills.
   - The accepted residual: a venue fill that the venue's read of the positions already held,
     that is delivered after the seed was applied, and that is timed after the watermark (on
     Paradex, by its publish time) or not timed at all, is held in the seeded position and
     counted again. A publish time bounds the execution only from above, so such a fill may
     have executed shortly before the seed's request as well as between the request and the
     read: these are the fills in flight while a market's seed is taken, and a market is seeded
     once per process (0055). The double count is never silent: the next resync's comparison
     reports it as `PositionCheck::Desync`, on which 0094's stop rule acts. Until then, the
     inventory cap (I6) and Exit's admission (0063) are judged against a position off by those
     fills: a liquidation counted twice can make an order look reducing that in fact grows the
     position. A consumer told `FillRouted::Venue { in_snapshot: false }` after a seed may
     request a resync at once to bring that comparison forward. The owner confirmed this
     residual, so stated, on 2026-10-20.
   - A venue fill's time is the venue's, aligned to the local wall clock (0014). An alignment
     error misplaces a venue fill against the watermark in either direction (one executed after
     the read held, or one the read held counted), and either shows as a `Desync` at the next
     resync. Order fills are placed by matching-engine times only, so this applies to venue
     fills placed by a publish time.
   - On a market seeded by hand (`Registry::seed_position`), a venue fill counts. On a market
     whose position is already unknown, it moves the inventory and the position stays unknown.
   - Later resyncs compare as 0055 has it, taking a venue fill by its arrival and its time.
6. **Every state, no arming (0012, 0013).** A venue fill counts in Killed, Cancel-only, Exit and
   Quoting alike. It changes no market's state, arming or `StateGeneration`, and arms nothing.
   - It moves the position, so an Exit or orphan-cooldown command built before it is refused at
     submit (`StaleAuthorization::PositionMoved`; 0066, 0108).
   - I6 and Exit's admission read the moved position, so a liquidation to flat leaves Exit
     admitting nothing.
   - The consumer runs `ExecutionPlanner::on_fill` after it as after any fill, so an emulated
     reduce-only order that it left too large is cancelled (0106).
7. **Reporting.** Each venue fill is reported by its route, `FillRouted::Venue`. One before the
   seed that nothing places goes in `ResyncReport::unsettled`, and agreement or desync is
   reported by `PositionCheck` at each later resync. Its fee comes only through
   `DecodeScope::fee` and is positive when paid, a liquidation's penalty included, never
   negated (0004). Its realized P&L and funding are the venue's numbers.
8. **Block trades** count, as the owner's answer lists them, though one may be another system's
   trade on the account: the position needs it. I4 still governs every order fill.
9. **Paradex.** `fillType` 2 is `Liquidation`, 3 `Transfer`, 4 `Settlement` and 6
   `BlockTrade`; 1 (FILL) and 5 (RPI) are order fills; any other value is refused, as before. A
   venue fill's `Price8` is its `PxExact` at exponent -8, refused only when null. An order
   fill's is checked against the grid as before. `side` is read as the account's side, as the
   Java library reads it. Paradex documents it as the taker's side, which is not verified for
   venue fills; a wrong reading would show as a `Desync` of twice the fill.
10. **Replay.** Routing and placement depend only on the ledger's `now`, the fill's times and
    the seed's request and watermark. Nothing reads a clock, so a replay routes and places each
    venue fill as the live session did.

## Alternatives

- **Keep `cid` beside a new origin field:** rejected. A venue fill carrying our client id could
  then be built, and every reader that attributes a fill by its client id (fbc-oms, the
  conformance suite, the samples, the private consumer) could take it for ours, an I4 hazard.
- **Exempt a fill from I4 because it has no client id:** rejected. Another namespace's order on
  a venue that echoes no client id looks the same.
- **Count a venue fill on the order its venue id names:** rejected. It would move that order's
  `cum_fills` for an execution that is not the order's.
- **Put it in view as before:** rejected. A fill with no venue id would keep its market in view
  for good and refuse I7's instrument cancel-all.
- **Round a venue fill's price to the grid by a stated rule:** rejected by 0004, which names
  prices rounded where they should not be among the bugs it exists to prevent; and price times
  quantity would no longer reconcile with the venue's realized P&L.
- **Refuse an off-grid price, as before:** rejected. The fill is lost while the position moves,
  which is the desync the owner's answer removes.
- **A `PxExact` price on every fill:** not taken. Order fills would lose the grid-checked
  `Ticks` and the decoder its misdecode guard.
- **A price type chosen by value (`Ticks` or `PxExact`) beside the origin:** not taken. An
  on-grid price would have two spellings, and an off-grid order fill could be built.
- **Unsettled after the seed, as 0055 and 0095 have it for an order fill:** the owner chose
  against it. Any liquidation, settlement or transfer after the seed would leave the market's
  position unknown for the rest of the process, refusing Flatten and quoting there, which is
  stronger than the (c) the owner rejected.
- **Placing by `GET /positions`' last fill id, or by the reads' instants:** not taken. The
  resync's events carry neither; this is left for a follow-up.
- **A resync forced by each venue fill:** the owner's (b), rejected.
- **Cancel-only on any venue fill:** the owner's (c), rejected.

## Consequences

- Every `FillEvent` states its origin, and its readers move from `.cid` and `.px` to `cid()`,
  `order_px()` and `venue_kind()` or a match on the origin. The private consumer migrates at
  the tag that ships this. The journal format does not change: no record holds a `FillEvent`.
- The samples' glue counts a venue fill as unexplained, so they Stop on one. That is
  conservative; what a consumer does on one is the consumer's call.
- SimVenue and the conformance toy emit no venue fills yet, so no conformance check covers them.
- Left for follow-up tickets: Paradex's venue-fill `side` checked on a recording (FBC-mfw4); a
  venue fill whose size is off the size step, which the decoder still refuses (FBC-w7ed);
  off-grid public LIQUIDATION and SETTLE_MARKET trades in market data (FBC-r0qi); trade busts
  (FBC-daer); the publish-time bound for order fills, which would change 0095 for them
  (FBC-4gsk); placing by `GET /positions`' last fill id (FBC-50kl); venue fills in SimVenue
  (FBC-w964) and the conformance toy (FBC-flns); whether Paradex's `createdAt` is a
  matching-engine time (FBC-ij33).
- Proof: `crates/fbc-oms/tests/venue_fills.rs` (a hand-built Paradex LIQUIDATION and position
  transfer on a market a resync seeded, the next resync agreeing with no desync; placement on
  each side of the seed, the residual reported as a desync, every state, and the error paths),
  `crates/fbc-oms/tests/fills.rs` (I3 with venue fills),
  `crates/venues/fbc-venue-paradex/tests/exec_fill_events.rs` and
  `fixtures/paradex/exec/fill-settle-off-grid-v2.sbe.txt` (an off-grid settlement decoded
  exactly).

## What would show this was wrong

- A `Desync` after a venue fill outside the residual item 5 states, or one inside it often
  enough that 0094's stop rule trips in the owner's sessions: a new record then places venue
  fills differently, or makes them `Unsettled` after the seed.
- A venue fill counted on an order, or putting its market in view.
- A recording showing that Paradex's `side` on a venue fill is the counterparty's.
- One execution reported both as our FILL and as a venue fill, counted twice under two keys.
