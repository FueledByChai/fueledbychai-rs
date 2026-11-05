# 0137 — The conformance suite's amend_ack amends inside the instrument's min_notional and max_order_size, augmenting 0083

Status: accepted
Date: 2026-11-05

## Context

0083 chose `amend_ack`'s amend: the price moved one valid step where the price is amendable and
such a price exists; otherwise a quantity amend to twice the order's size, or the instrument's
largest order where that is less, skipped where no other size fits. It names no direction for
the step, and the suite's code took the step below.

FBC-fuis sizes the orders the suite's checks place to the least size whose notional
(`InstrumentSpec::notional`) reaches the instrument's `min_notional` at their price, within its
`max_order_size`. One price step below, that order falls short of the minimum, so a venue or a
codec validating `min_notional` rightly refuses the amend, and `amend_ack` reported a venue that
works as broken (FBC-7s2a).

## Decision

`amend_ack` amends the harness's order inside the limits it was placed in:

- Where the price is amendable, the valid price one step below, where the order at its size
  still reaches `min_notional` there (any price, where none is stated) with a notional a `Money`
  holds; else the valid price one step above, under the same test. The size is kept.
- Otherwise, where the quantity is amendable, the quantity amend 0083 names, at the same price:
  now also where a price one step away is valid but neither reaches `min_notional`. Its notional
  must be one a `Money` holds, as the harness's order's is.
- Where none fits, the check is skipped, naming each spec field that ruled an amend out: no valid
  price one step away (as before); for a venue amending prices alone, every valid price one step
  away short of `min_notional`, or with a notional too large to hold; no larger size within
  `max_order_size` (as before, now naming `min_notional` too where it ruled the price steps out);
  and a larger size whose notional is too large to hold.
- The amend stays a real amend under the venue's `AmendCaps`: a price amend keeps the size, a
  quantity amend keeps the price. The rest of 0083 stands.

## Alternatives

- Always the step above: rejected. Where no `min_notional` is stated it changes, for nothing, the
  amend every suite sends today (Paradex's included), and the step above can be too large to hold
  where the step below is not.
- The step below with a size raised to reach the minimum: rejected. A venue whose `AmendCaps`
  allow the price alone could not take it, and it changes two fields where one shows the
  acknowledgement.
- Failing where no amend fits: rejected. The venue's own limits leave nothing to amend to, which
  is not a fault of its order entry; 0083 already skips where no other size fits.

## Consequences

- A venue stating `min_notional` is sent an amend it should accept; one stating none is sent the
  same amend as before.
- A skip names the field to look at. The tests are in
  `crates/fbc-conformance/tests/suite_orders.rs`.

## What would show this was wrong

A venue that judges `min_notional` on something other than `InstrumentSpec::notional` at the
order's price (a mark price, say), so the amend chosen here is still refused; or a venue whose
stub refuses the step above for a reason of its own, such as a post-only buy crossing a book the
stub keeps.
