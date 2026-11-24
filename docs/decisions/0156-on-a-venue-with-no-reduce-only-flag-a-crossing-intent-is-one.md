# 0156 — On a venue with no reduce-only flag, a crossing intent is one price-capped IOC without it, sized against every order on its side, its remainder cancelled, amending 0106, 0110 and 0112

Status: accepted
Date: 2026-11-24

## Context

- **No crossing exit on QFEX.** Its caps declare `OrderCaps::reduce_only` false, so 0110 point 2
  refuses every crossing intent there: no Flatten or disaster stop crosses through the planner.
- **What the flag did.** It kept 0110's cross, sized against the position less unaccounted fills and
  our other reducers' resting orders (0106 rule 6's measure), from passing zero. FBC-zsfz, open,
  would require it on Exit orders where the venue has one.
- **FBC-mfcm.** A pre-Exit order on the reducing side, unmarked or oversized, is left resting,
  and a cross beside it is refused `CrossesZero` (0063 rule 4).

## Decision

1. **Where.** On a venue whose `OrderCaps::reduce_only` is false, in every state that builds a
   crossing intent's order. Venues with the flag keep 0110.
2. **The order.** One limit at the cap, without the flag, classified reducing
   (`NewOrder::reducing`), never post-only; IOC where the caps take it, else 0110 point 2's
   synthetic IOC; reported with a new `Downgrade::NoReduceOnly` (0106 rule 3).
3. **The size.** At most `qty`, and at most the position's size on the side it reduces less every
   other order of ours there at its `OrderRecord::exposure`, Exit's admission's measure (0063
   rule 4), but the emulated ones, which never clip it: one whose exposure, with the cross's and
   the others', would pass the position's size is cancelled and the cross waits for it
   (`AwaitsEmulatedCancel`, 0106 rule 7 on this measure, not rule 6's). With nothing left,
   nothing is built (`NoRoomToReduce`).
4. **At most one live**, as 0110 point 5 holds: one order per attempt, the next once it ends.
5. **Its remainder cancelled.** A venue IOC is cancelled at every pass until terminal once a
   fill is reported on it or, on a two-phase venue, once the caps' risk-reject window has passed
   since its acknowledgement arrived (a time fbc-oms does not record today and FBC-a4wm adds; on
   QFEX, where no reply settles a place (0132 item 8), its first order update showing it Open),
   never before either, as an `ACK`
   may precede a speed bump's match; on a single-phase venue, from the first pass after its
   acknowledgement. A synthetic IOC is cancelled at once (0106 rule 4). Either is built only
   where one cancel names it by venue or client id (`NoSyntheticIoc`).
6. **Unchanged.** The state, Exit's admission and both caps judge it (0110 point 4), with no
   inventory-cap exemption (0112 point 2). Once a fill moved the position it is refused at submit
   (0066, 0153) and planned again. 0148 item 5's flatten is unchanged.
7. **Amended**, for a venue without the flag only, these are replaced: 0110 point 2's "reduce-only"
   and "A venue without reduce-only ... gets no crossing order"; point 3's "less the fills ... (0106
   rule 6's measure)", which point 3 here replaces; point 5's "an IOC ends by itself"; its
   Alternatives' rejection, for Quoting, of clipping by every order; its first falsifier, which this
   record's first replaces; and 0112's Consequences' "(refused there by 0110)". 0110 point 2's
   `NoSyntheticIoc` refusal now also covers a venue IOC (point 5). It amends 0106 rules 3 and 7 as
   points 2 and 3 say. FBC-zsfz is untouched.
8. **The build**, two tickets, FBC-a4wm waiting on FBC-224w and FBC-mfcm and FBC-xf9y on
   FBC-a4wm, both blocking FBC-cnyh's admission; neither blocks FBC-vvit, Paradex having the flag:
   - **FBC-a4wm, fbc-oms: the crossing IOC on a venue with no reduce-only flag.** `planner.rs` plans
     it instead of refusing, reporting `Downgrade::NoReduceOnly` (a public API change), recording
     when each place's acknowledgement arrives, its room and the emulated orders' wait read as
     `Exposure::admit_exit` reads them. Proof in `tests/crossing.rs` on QFEX-shaped caps, Exit and
     Quoting: beside ordinary, PendingNew, Unknown and emulated orders and unaccounted fills; an ACK
     with no fill cancelled only once the window has passed since it; a fill's remainder cancelled;
     refused at submit, then planned; flagged venues unchanged.
   - **FBC-xf9y, fbc-venue-qfex: that IOC encoded by the no-probe codec (FBC-aecm's, not yet built)
     without `reduce_only`, its remainder cancelled by venue id, against the conformance stub.** It
     also waits on the first, FBC-aecm and FBC-jdj7 (`IOC_PARTIALLY_FILLED`'s end), and proves a
     partial fill freeing the next attempt.

## What the owner decided

FBC-224w's comment of 2026-11-17 records the owner's answer:

> The owner chose "Capped IOC, no flag": the planner sends a price-capped crossing IOC without
> the venue flag, sized against every order on that side as Exit's admission measures it, so the
> position never crosses zero, with at most one in flight and its remainder cancelled. Capturing
> whether QFEX accepts reduce_only stays a later option. The record supersedes the part of 0110
> that requires the flag, and states the window left with no venue flag. Its build needs
> FBC-mfcm first.

Approving this text (0149) also takes points 1, 2, 5, 7 and 8 and the Consequences' window, later
orders and self-trades.

## Alternatives

- **Flag `reduce_only`:** left for later, as the owner chose; capturing it needs a record
  amending 0148 item 4.
- **An emulated reduce-only order** (0106): rejected; post-only, it waits for the market.
- **Flattening by hand:** rejected; a 24/7 disaster stop cannot wait for the owner.
- **Exit alone:** not taken; disaster stops cross outside it too.

## Consequences

- **The window left with no venue flag.** The IOC and every order of ours on its side when it is
  built, all filled, take the position at most to zero as the process holds it. What remains is a
  move the process has not counted: an untracked or venue-initiated fill (0120) landing after the
  check at submit or in flight, QFEX's 500 ms speed bump included, or a trade outside the process
  at any time.
  The position can then pass zero by that move at most, no more than our side still rests (0052).
  This is the window 0066 names for every Exit order without the flag, and 0153 for Quoting; a
  crossing intent on the other side reduces what passed.
- **Later orders.** In Quoting, an order of ours built on the side while the IOC is live faces the
  caps alone (an emulated one, 0106 rule 6's measure too) and can take the position past zero as any
  quote can; Exit refuses one that would (0063 rule 4).
- **Self-trades.** Our QFEX adapter's caps declare no self-trade prevention (`stp_scope: None`, 0148
  item 6), and QFEX's client agreement bars wash trading. The IOC can meet our other-side orders
  within its cap: Exit cancels them in its pass (0065 rule 8), not seen to end first; in Quoting
  nothing in fbc-oms cancels them; that rests on the consumer alone.
- **A partial fill on QFEX, until FBC-jdj7.** The adapter keeps an `IOC_PARTIALLY_FILLED` remainder
  Open (0151 item 8), and a `get_order` answer outside a resync pushes nothing, so the query ends
  Unknown at its deadline. The IOC's cancel would meet `NO_SUCH_ORDER` and the Unknown ladder
  (0005's I5), which no trustworthy snapshot, query or client-id cancel ends, and no later attempt
  would be planned: hence the QFEX build waits on FBC-jdj7. After it, a `NO_SUCH_ORDER` with no end
  event still holds later attempts until the ladder resolves it.

## What would show this was wrong

- An unflagged cross taking the position past zero with no uncounted move or later order of
  ours.
- One built while it and its side's exposure exceeded the position, beside a live one, or twice
  for one attempt.
- A remainder open past the first pass after its fill or window, or an IOC cancelled before
  either.
- Uncounted moves passing zero often on QFEX; the capture of `reduce_only` is then taken.
