# 0101 — The execution planner fits each quote to the venue's caps by generic fallbacks reported as downgrades, and keeps an emulated reduce-only order within what is left to reduce, augmenting 0065

Status: accepted
Date: 2026-10-18

## Context

Design §4.10 steps 3 and 4 make the `ExecutionPlanner` the one place a quote the venue does not
take as asked is changed, by generic rules chosen from `OrderCaps` alone, never by venue name
(0003, 0005): RPI asked for on a venue without it goes public; (Ioc, ReduceOnly) becomes a
capped reduce-only limit and an immediate cancel; (PostOnly, ReduceOnly) follows the quote's
fallback, emulated reduce-only (post-only without the flag, sized at most the position less the
same-direction resting reducers, re-checked on every fill, cancelled on overshoot) or kept
reduce-only (post-only dropped, a taker fill acceptable); (Rpi, ReduceOnly) becomes public with
reduce-only; each change is reported. 0065 left these to FBC-01w. The planner sees no book, so
it cannot compute a maker-safe price itself, and nothing in fbc-oms runs between a fill and the
consumer's next pass unless the consumer calls it. Paradex declares only (PostOnly, Ioc),
(Ioc, Rpi) and (Rpi, ReduceOnly) (0054), so on Paradex only the last fallback applies; an IOC it
refuses later is an asynchronous rejection that ends the order (0093). FBC-7aw4's crossing exit
builds on the synthetic IOC.

## Decision

1. **The fallback is the consumer's choice per quote, with no default.** `DesiredQuote` carries
   `reduce_fallback: ReduceFallback` (`EmulateReduceOnly` or `KeepReduceOnly`), read only
   where the venue refuses post-only with reduce-only (a maker exit keeps the flag, inventory
   relief emulates it, as the design suggests; the consumer decides).
2. **Fitting, in order, from the caps alone.** Each pass fits each wanted quote before deciding
   its level: RPI on a venue without the RPI channel goes public; then, for a reduce-only
   quote, a pair in `flag_conflicts` (listed in either order) falls back: (Rpi, ReduceOnly) to
   the public book; (Ioc, ReduceOnly) to a GTC limit at the quote's price; (PostOnly,
   ReduceOnly) by the quote's fallback. An order moved off the RPI channel stays post-only
   where the venue has post-only, since an RPI order only rests. The fitted quote is what the
   level is compared with, so an order placed from it is kept by later passes of the same
   quote. An order whose quote becomes a synthetic IOC or an emulated reduce-only order of the
   same wire shape takes that mode (cancelled at once, or protected from overshoot from then
   on); a mode is never dropped while the order rests, so an emulated order whose quote is
   now ordinary is still kept and amended within what is left to reduce.
3. **Each downgrade is reported on its command.** `Planned::downgrades` lists the
   `Downgrade`s (`RpiUnsupported`, `RpiReduceOnly`, `SynthIoc`, `EmulatedReduceOnly`,
   `DroppedPostOnly`) in the order applied, on every place and amend built from a fitted
   quote; the consumer journals them.
4. **A synthetic IOC is cancelled at once.** Its place is followed in the same pass by its
   cancel where the venue takes one before the acknowledgement; otherwise the cancel waits for
   the acknowledgement (`Plan::awaiting_ack`) and is built by the first pass after it. Its
   level is never amended or kept: every pass cancels it until it is terminal, then, if the
   quote is still wanted, places another, as an IOC would cross again.
5. **Kept reduce-only keeps the consumer's price.** Post-only is dropped and the order is sent
   at the quote's price: the planner has no book, so the price's maker safety is the
   consumer's, and a taker fill is acceptable by the fallback's definition.
6. **Emulated reduce-only stays within what is left to reduce.** The order is post-only
   without the venue flag and classified reducing. What is left is the position's size on the
   side the order reduces (nothing while the position is flat, unknown or on the order's
   side), less the fills on that side the venue reported on order updates that the inventory
   does not hold yet, less what our other orders that reduce it on that side may have resting
   (`OrderRecord::resting`, a cancel in flight counted until terminal). An order reduces it
   when placed reduce-only or classified reducing, once an amend classified reducing was
   built for it, or once the planner kept it for a quote that reduces the position, an
   emulated one's included (`OrderRecord::reduces`).
   Among a pass's reducing commands, those the venue flag or the consumer bounds are built
   before the emulated ones, whatever their levels. A place is sized to what is left when
   built, those commands counted, and refused (`PlanRefusal::NoRoomToReduce`) when nothing is
   left; an amend grows the order only to it. An order resting more than is left is cancelled
   at once, whatever its age or the book: at the pass's start (in 0005's order, with the other
   cancels), and by `ExecutionPlanner::on_fill`, which the consumer calls after every fill on
   the market, and every order update there that reports a fill (the room already counts
   those fills before their fill events), and which builds only those cancels. The level, if still wanted, is placed
   again at the smaller size once the order is terminal.
7. **A reducer never joins an emulated order it would leave overshooting.** A reducing place
   or amend (a synthetic IOC's included) whose worst case would leave an emulated order on its
   side resting more than is left to reduce is not built: the emulated order is cancelled in
   its place, and the reducer is refused `PlanRefusal::AwaitsEmulatedCancel` at every pass
   until that order is terminal, as a replace waits under 0065. Both could otherwise fill
   before the cancel lands and take the position past zero. The emulated order is cancelled
   only when the market's state and both caps would admit the reducer with that order ended;
   a reducer refused anyway (by the resting cap, say) reports its own refusal and the
   emulated order rests, so no order is placed and cancelled by turns.

## Alternatives

- A default fallback: rejected. The design's two uses want opposite defaults, and 0009 keeps
  safety-relevant settings the consumer's.
- Drop the synthetic IOC's level to a resting order the book can keep: rejected. An IOC that
  rests is a different order; the cancel is the IOC's half.
- Price a kept reduce-only order maker-safe inside the planner: deferred. It needs the book's
  touch, which the planner does not take; the consumer prices the quote from its book.
- Resize an overshooting emulated order by amend instead of cancelling it: rejected for now.
  The design says cancel on overshoot, and the v1 consumer runs with no amend
  (cancel-and-replace only).
- Count only the emulated orders' own sum, not every reducer: rejected. A venue-flagged
  reducer can fill first and leave the emulated order crossing zero.
- Clip the reducer to what the emulated order leaves, or build it and cancel the emulated
  order after it: rejected. Clipping starves a force-close or other exit the consumer sized,
  and cancelling after authorizes both orders at once; waiting costs one cancel round trip.
- Re-check emulated orders only in `plan`: rejected. A fill between passes could leave an
  order able to cross zero until the next pass; `on_fill` makes the check per fill.

## Consequences

- Every `DesiredQuote` names a `reduce_fallback`; the consumer journals `Planned::downgrades`.
- A consumer that emulates reduce-only calls `on_fill` after each fill on the market, and each
  order update reporting one, and submits what it returns, as it does a pass's commands.
- An order's emulated mode and reducing classification stay while it rests: one whose quote
  later turns ordinary is still clipped and cancelled on overshoot, and still counts against
  other emulated levels. Safe, at the cost of an extra cancel and replace.
- Two emulated orders on one side can both be cancelled by one overshoot, since each counts
  the other until it is terminal: safe, at the cost of queue time.
- On a venue that cancels only after the acknowledgement, a synthetic IOC can rest until its
  acknowledgement arrives and the next pass builds its cancel.
- A reducer added beside a resting emulated order it would leave overshooting, a force-close
  cross among them, goes out one cancel round trip later, and waits for as long as that order
  cannot reach a terminal state (while it is Unknown until a resync, say): safe, but a
  force-close waits with it.

## What would show this was wrong

- An emulated reduce-only order of ours that filled past zero.
- A downgraded order replaced at every pass while its quote did not change.
- A synthetic IOC that rested past the pass after its acknowledgement.
- A venue whose refused pair needs a fallback that depends on more than its caps.
