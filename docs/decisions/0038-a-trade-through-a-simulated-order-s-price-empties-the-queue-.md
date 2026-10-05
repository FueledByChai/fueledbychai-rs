# 0038 — A trade through a simulated order's price empties the queue ahead and fills it from the trade's size; a trade's size is spent once in match order

Status: accepted
Date: 2026-10-04

## Context

BT-501's fill model (FBC-30g, design §10.2) keeps where each simulated resting order sits in
its level's queue, bracketed three ways because the venue's true queue is not observed: on
arrival the order is behind the level's displayed size less every modelled own order, a level
cancel advances it by none, the proportional share or all of the cancelled size, and a trade at
its price consumes the queue ahead and then fills it. The design leaves the trade itself
unwritten (`on_trade` is `todo!()`): it says nothing of a trade printed beyond the order's
price, of one trade meeting several modelled orders, or of which flow can fill an RPI order,
which the design says gets retail flow only.

## Decision

- **Through the price.** A trade printed through an order's price (sellers below a bid, buyers
  above an offer) means the venue's level at that price emptied first, so the order has nothing
  ahead of it afterwards, in every bracket. The trade's own size is what would have met the
  order had it rested, so it fills the order up to what remains. The order is not filled
  completely by default: the print says how much the taker had beyond the level, not more.
- **One trade, several orders.** A trade's size is spent once across the modelled orders it
  reaches, in the order the venue matches them: the best price for the taker first; at one
  price, every public order before any RPI order, each channel in arrival order. At the trade's
  price, size consumed ahead of an earlier modelled order counts as consumed ahead of the later
  ones, and what fills an earlier order is not available to a later one.
- **Flow and channel.** The caller says which flow a trade was: public flow meets the public
  book only and never fills an RPI order, though it still consumes the public size queued ahead
  of one (and a print through its price empties that queue); retail flow fills both, public
  first. A trade's taker side is the caller's too; one whose aggressor the venue does not give
  is classified by the caller before the model sees it.
- Fills are at the order's price, one per order per trade, never past what remains; a filled
  order leaves the model.

## Alternatives

- A trade through the price fills the order completely: overstates fills when the taker had
  only a little beyond the level, where an order resting there would have stopped the sweep.
- A trade through the price counts like one at the price (consume the recorded queue ahead,
  then fill): the queue at that price is known to be gone, so the Pessimistic bracket would
  keep waiting behind size that no longer exists.
- Each modelled order sees each trade independently (design §10.2's per-order `on_trade`):
  two modelled orders at one price would each be filled from the same lots.
- Fill RPI orders from any flow: overstates RPI fills, since the venue restricts RPI to retail
  takers.

## Consequences

The simulated venue (FBC-uoo) or another caller must classify each trade's taker side and flow
before handing it to the model. A study's fills from prints beyond the order's price follow the
taker's size beyond the level, which depends on how the venue prints a sweep (one print per
level, or one print at the worst price); a venue that prints a sweep as one trade at its worst
price gives the whole size to the levels it passed.

## What would show this was wrong

The shadow's calibration against Java's real queue (design §10.2 step 3) finding simulated
fills on trades through the price systematically above or below Java's real fills at the same
distance from the touch; or a venue whose RPI orders do meet non-retail flow.
