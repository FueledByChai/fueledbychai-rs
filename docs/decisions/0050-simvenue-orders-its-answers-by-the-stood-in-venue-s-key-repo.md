# 0050 — SimVenue orders its answers by the stood-in venue's key, reports realized P&L from the position its fills imply, and sends derived fills as order updates, augmenting 0046 and 0049

Status: accepted
Date: 2026-10-05

## Context

0046 left `SimCodec` refusing every placement for a venue whose `OrderCaps::ordering_key` is
not `VenueSeq`, whose `FillCaps` declare `realized_pnl` or `realized_funding`, or whose
`FillCaps::source` is `DerivedFromOrderStatus` (Codex r4182991971, r4182991978 on PR #58),
because the engine stamped every answer with a sequence number, reported no realized values
and sent fills of their own. Paradex's fills carry realized P&L and funding, so no Paradex
backtest or shadow could place an order (FBC-938, ahead of FBC-2zxk). 0049 gave the engine a
position per instrument from its fills, in lots only.

## Decision

- **Ordering.** Every answer frame carries the engine's sequence number and the wall time of
  the instant the venue acted: a command's encode time plus `to_venue`, or a trade's stamp
  (0046). The codec reports, as the stood-in venue's `ordering_key` says: `VenueSeq`, the
  sequence number as `venue_seq`; `VenueTs` and `BlockTime`, that instant as `exch_ts` with
  `ExchTsKind::MatchingEngine` and no `venue_seq` (every answer of one act shares it, as the
  events of one block share a block time); `None`, nothing (`VenueMeta::NONE`). A frame
  without the instant is malformed for a venue ordered by time.
- **Realized P&L.** The engine's position per instrument (0049) also keeps the notional its
  open lots were opened at, in nanos of the quote asset. A fill that opens or adds to the
  position realizes zero. A fill that reduces it realizes, on the lots it closes, their share of
  the fill's notional less their share of the position's cost for a long (the reverse for a
  short), positive for a gain; each share is truncated toward zero and taken off what it was
  taken from, so a position closed whole leaves no cost, and the lots past a flip open the new
  position at the rest of the fill's notional. Fees are not in it: each fill carries its own.
  Where the cost stops fitting an `i128` the engine writes no P&L until the position is flat
  or flips, and the codec reports `None`.
- **Realized funding.** The engine accrues no funding, so every fill realizes zero funding,
  written on the frame as zero; FBC-uki4 accrues it from the shard's funding envelopes.
- **What the codec says.** It reports realized P&L and funding on a fill only where the venue's
  `FillCaps` declare them, `None` otherwise, as 0046 does for client ids and flags.
- **Derived fills.** For a venue whose fills are `DerivedFromOrderStatus` the engine sends no
  fill frame: a taker order's update carries each fill's cumulative quantity (open, for each
  fill but the last, whose quantity the order's closing or resting event carries), and a maker
  fill's update is the order event that follows it on a native venue. The fills still move the
  position a resync reports.
- The codec places for every ordering key, realized values and fill source; its other refusals
  (0046) stand.

## Alternatives

- **An average entry price in ticks**: rejected; it is not a tick in general, and a rounded one
  leaves cost behind when the position closes. Notional in nanos is what fees already use.
- **Realized P&L computed by the codec**: rejected; the codec sees no position, and a real
  venue's codec reports what the venue sends.
- **Realized funding `None` where declared**: rejected; the venue declares its fills carry it,
  and the simulated venue's funding is truly zero until FBC-uki4 models it.
- **A block time on a coarser clock** (a configured block interval): not taken; no number lives
  in `fbc-sim` without the consumer's configuration, and none is needed for the OMS's ordering
  until a stood-in venue needs it.

## Consequences

- A Paradex stand-in's fills carry realized P&L and zero funding, so FBC-2zxk can place
  through SimVenue; its realized funding is wrong while positions are held across a funding
  time, until FBC-uki4.
- The engine answers a derived-fills venue with more order events for a crossing order than a
  native one.

## What would show this was wrong

Realized P&L from SimVenue that disagrees with the stood-in venue's own for the same fills
(the shadow's calibration, design §10.2), or an OMS that needs distinct timestamps for events
of one act to order them.
