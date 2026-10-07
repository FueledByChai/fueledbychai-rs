# 0065 — The execution planner diffs a desired book per side and level, amends where the venue's caps allow and otherwise cancels and places after the terminal state, and emits each command authorized

Status: accepted
Date: 2026-10-07

## Context

0005 names one `ExecutionPlanner` in `fbc-oms`: it turns a desired book into venue commands,
is the only reader of `OrderCaps`, and orders cancels, then reducing orders, then amends, then
adds. Design §4.10 sketches its steps: (1) diff desired against live per side and level, with
basis-point replace thresholds, a minimum age and queue retention; (2) a change is an amend
where the caps allow one, otherwise cancel, await terminal, place, both orders counted as
exposure under I6 until terminal, and for many simultaneous changes a cost comparison under the
declared rate scopes; (3) and (4) channel and flag-conflict fallbacks; (5) batching; (6) the
pre-trade checks; (7) the order and the rate-scope safety floor. FBC-0j3 builds steps 1, 2 and
7's order; batching, venue modes and the safety floor are FBC-hht's, flag-conflict fallbacks
FBC-01w's. The design does not define the desired book's type, and this repository must not
depend on strategy code (0001), so the type is decided here. The pre-trade path (the market's
state, Exit's admission, both caps; 0012, 0013 rule 2, 0052, 0063), the permits (0005) and the
authorization (0045, 0060) already exist; the planner must go through them, never around them.

## Decision

1. **The desired book.** `fbc_oms::DesiredBook` is one market's quotes, a `DesiredQuote` per
   side and level (level 0 nearest the touch): price, the quantity to rest (an order's
   remaining quantity, its filled part not included; zero means no order), time in force,
   channel, post-only, the venue's reduce-only flag and the consumer's reducing
   classification. A level not in the book has no order.
2. **The planner holds its levels.** `ExecutionPlanner` remembers which of our orders it placed
   at each market, side and level, and reads each order's state from the `Registry`; an order
   seen terminal frees its level. Orders it did not place (an earlier run's, the consumer's own)
   are not its to change.
3. **The consumer's thresholds, no default.** `PlannerConfig` takes a price threshold in basis
   points of the order's price (finite, not negative), a quantity threshold in lots and a
   minimum age. A resting order is changed when its flags differ from the quote, or its price
   moved by at least the price threshold, or its resting quantity by at least the quantity
   threshold, and the planner placed or amended it at least the minimum age ago. A price is its
   tick index times the instrument's finest step, so basis points are computed from tick counts.
4. **A change.** An amend to the quote's price and a total of the order's filled quantity plus
   the quote's quantity where the venue's `OrderCaps` admit that amend (amendable at all, a
   limit order, partly filled where the venue allows it, the price or total the venue can
   change, a reference the amend names, the same flags); otherwise a cancel, the level then
   waiting until the order is terminal before the new order is placed. The old order counts as
   resting and as exposure until it is terminal and the new one from when it is built, so I6
   and the resting cap judge every command with both counted. This slice never sends the
   replacement while the old order may rest, so the cost comparison of N amends against a
   cancel batch plus a create batch is FBC-hht's with batching.
5. **Occupied levels.** A level whose order is PendingNew or Unknown, on the Unknown ladder,
   with an amend or cancel in flight, an amend built and not reported sent, or an amend
   replaced in flight (by a cancel the venue refused or that was never sent) not yet settled by
   a venue update ordered after it, gets no place, amend or replace until that settles: the
   replaced amend may still reach the venue, so no second amend goes over it (FBC-cit6). A level no longer wanted has its order cancelled unless
   a cancel is already in flight. A cancel waiting for the order's acknowledgement is carried
   through whether or not the level is wanted again: the planner tries it at every pass, builds
   it once the acknowledgement lands, and the level waits for the order's terminal state before
   anything is placed there, so a pulled order never rests at its old price. The consumer need
   not drive `Registry::cancels_due` for the planner's orders.
6. **The order and the checks.** One pass decides every level first, then builds in 0005's
   order (cancels, then places and amends that reduce, then amends, then adds; each group by
   side, bids first, then level), so each command is judged with the earlier ones counted.
   Every place goes through `Registry::place`, every amend through a `Live` permit, every
   cancel through a `Cancellable` one, and each command leaves as an `Authorization`. A place
   or amend the market's state or a cap refuses is never built and is reported; the planner
   never falls back from a refused amend to a cancel.
7. **The consumer reports what it sends.** As for any command, the consumer reports a place's
   outcome, an amend's `Registry::amend_sent` and a cancel's `Registry::cancel_sent` before the
   next pass; a cancel not reported sent is built again at the next pass.
8. **Exit cancels what adds.** In a market in Exit, the planner cancels each order it holds on
   a side that does not reduce the position, whatever the desired book wants at that level:
   the side that adds to it, and both sides while the position is flat or unknown (0063's
   Consequences). It never leaves such an order resting and never tries to amend it, since
   Exit would refuse the amend; a quote the book still wants there is placed once the order is
   terminal and is then refused by Exit's admission like any order. Orders on the reducing
   side are planned as in any state, each place and amend judged by Exit's admission; one
   placed there before Exit that is unmarked, or larger than the position, is left as it is
   for now (FBC-mfcm).

## Alternatives

- Price thresholds in ticks: rejected. The design states them in basis points, which mean the
  same across instruments; ticks convert exactly.
- The desired quantity as the order's total, filled part included: rejected. A strategy wants
  a quantity resting at a level; a partly filled order's total is the OMS's bookkeeping.
- Fall back to cancel and replace when an amend is refused by a cap or the market's state:
  rejected. A refusal means nothing new is admitted, and a cancel then a place admits no more.
- Place the replacement while the old order's cancel is in flight (counting both): deferred to
  FBC-hht, where batching makes it cheaper; awaiting the terminal state never doubles the
  exposure a level may hold.
- In Exit, leave an order on the adding side to the consumer (an empty book side) or refuse
  its amend and leave it resting: rejected. It can fill and grow the position the owner asked
  to exit, and 0063 assigns its cancel to the planner.
- Let the planner manage every order on the market, including ones it did not place: rejected.
  Orphans are 0005's I7 and the resync's (0055); the consumer's own orders are the consumer's.

## Consequences

- Strategy code produces a `DesiredBook` and calls `ExecutionPlanner::plan`; it never builds a
  place, amend or cancel itself, and every command it submits carries an authorization.
- A level being replaced on a venue without a usable amend is empty for a round trip (cancel,
  its terminal state, then the place), which costs queue time on such venues.
- The planner trusts the consumer to report sends before the next pass; a missed cancel report
  costs a duplicate cancel, never an unchecked order.

## What would show this was wrong

- An order the planner placed left resting in Exit on a side that does not reduce the
  position, after a pass over that market.
- A replacement left empty for so long that the quote's time at the touch matters, on a venue
  where an amend or a cancel-and-create batch would have kept it: then FBC-hht's cost
  comparison is needed earlier.
- A strategy that needs to change orders the planner did not place, or more than one order per
  level.
- Any command the planner emitted that a cap or the market's state should have refused.
