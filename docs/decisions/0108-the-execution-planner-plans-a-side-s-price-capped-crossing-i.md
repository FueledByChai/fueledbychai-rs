# 0108 — The execution planner plans a side's price-capped crossing intent as one reduce-only order at its cap, sized so the position never crosses zero, once per consumer attempt, augmenting 0065 and 0106

Status: accepted
Date: 2026-10-19

## Context

A consumer's disaster stop, news FLATTEN window and the operator's Flatten need an exit that
crosses the spread with a price cap (chaiwala-rs's force-close escalates its maker exit, after
10 s, to a reduce-only cross capped at 15 bps). 0065's `DesiredBook` is a per-(side, level) diff
of resting orders with no crossing intent, and 0005 forbids building an exit around the planner.
0106 gives the planner a synthetic IOC (a GTC limit at the quote's price cancelled at once)
where the venue refuses IOC with reduce-only, keeps an emulated reduce-only order within what is
left to reduce, and makes any reducer that would leave one overshooting wait for its cancel
rather than be clipped ("clipping starves a force-close"). On Paradex an IOC priced past the
price band is refused after acceptance with nothing filled (0093), and the minimum size is
judged at the mark (0096), so a cross sized to a small residual can be refused the same way.
A level quote that is IOC is placed again at every pass after its order ends (0106 rule 4), so
a cross the venue keeps refusing would be sent at every pass.

## Decision

1. **A crossing intent per side, at no level.** `DesiredBook::with_crossing(side, intent)` /
   `set_crossing` carry a `CrossingIntent { cap, qty, tag, attempt }`; the side is the order's
   (a sell crossing reduces a long); zero lots means no intent. Its commands, holds and
   refusals carry `crossing: true` and `level: CROSSING_LEVEL` (`u16::MAX`), and
   `ExecutionPlanner::crossing_order` names its live order. Among the reducing commands it is
   built after the side's levels, so a reducer at a level placed in the same pass is counted.
2. **One reduce-only order at the cap.** A limit priced at `cap`, reduce-only, classified
   reducing, on the public book, never post-only: immediate-or-cancel where `OrderCaps::tifs`
   has IOC, falling back as 0106 rule 2 does where the venue refuses IOC with reduce-only, and
   a good-till-cancelled limit at the cap cancelled at once (`Downgrade::SynthIoc`, 0106 rule
   4) where the venue has no IOC. A venue without reduce-only (`OrderCaps::reduce_only` false)
   gets no crossing order (`PlanRefusal::ReduceOnlyUnsupported`). A synthetic IOC, a
   crossing's or a level quote's (augmenting 0106 rule 4), is built only where the venue takes
   GTC limits and its single cancel can name the order once acknowledged (venue id or client
   id); otherwise its limit could rest and fill later, and nothing is built
   (`PlanRefusal::NoSyntheticIoc`).
3. **Sized so the position never crosses zero.** When built, the order is at most `qty` and at
   most what is left to reduce: the position's size on the side it reduces (nothing while the
   position is flat, unknown or on the intent's side), less the fills on that side reported on
   order updates that the inventory does not hold yet, less what our other orders reducing it
   on that side may have resting (0106 rule 6's measure), but for the emulated reduce-only
   orders: the crossing is never clipped by one; it waits for its cancel (0106 rule 7). With
   nothing left, nothing is built (`PlanRefusal::NoRoomToReduce`).
4. **Every state and cap still applies.** The place goes through the registry's one pre-trade
   path: refused in Killed and Cancel-only, judged by Exit's admission (0063) in Exit, and by
   the inventory and resting caps everywhere; no exemption.
5. **Never amended or replaced while live; one order per attempt.** A live crossing order is
   held (`HeldReason::Crossing`); an IOC ends by itself and a synthetic one is cancelled at
   once. Withdrawing the intent cancels a live order with the side's removal reason, as a
   level no longer wanted. The planner builds at most one order per `attempt`, and attempts
   only increase: once an order was built for an attempt, neither it nor an older attempt is
   planned again (an older one is refused `PlanRefusal::StaleAttempt`, so a consumer's stale
   or cycled counter never sends an extra cross), so a cross that ends unfilled
   or refused by the venue (price band, minimum notional, margin) is not resent at every pass;
   the consumer names a new attempt to cross again, planned once the previous order is
   terminal. An attempt refused before anything was built (state, cap, nothing left) sent
   nothing and is planned at a later pass, and an order that ended never sent
   (`TerminalKind::NotSent`, a stale authorization at submit included) reached no venue, so its
   attempt, the last one built, is planned again; older ones still are not.

## Alternatives

- A crossing quote at an ordinary level (a reduce-only IOC `DesiredQuote`): rejected. It is
  re-placed at every pass after it ends, is not clipped to the position, and shares a level
  index with the consumer's resting quotes.
- Re-cross at every pass while the intent stands: rejected. A cross the venue refuses (0093,
  0096) or that finds no liquidity within its cap would be resent at every pass, spending the
  rate budget; the consumer paces attempts, as its force-close timer already does.
- Clip by every order resting on the side, ordinary ones included: rejected for Quoting, where
  the venue's reduce-only flag keeps the cross itself from crossing zero and the inventory cap
  bounds the rest; in Exit, Exit's admission counts every order on the side and refuses a cross
  beside an ordinary order that would cross zero.
- Clip by emulated reduce-only orders too: rejected by 0106 (it starves a force-close).
- Remember every attempt built instead of requiring them to increase: rejected. It grows
  without bound on a 24/7 consumer, and an increasing counter is what a timer-paced consumer
  has anyway.
- Clip to the resting cap instead of refusing: rejected. The ticket keeps every cap applied as
  the cap's refusal; a consumer sizes its cross within the cap.

## Consequences

- A consumer escalating a maker exit to a cross removes the maker level or lets it rest: a
  venue-flagged reducer resting on the side clips the cross (it may fill too), so the cross
  goes out at full size one cancel round trip after the maker exit ends; an emulated one is
  cancelled by the planner and the cross waits for it.
- A cross larger than the resting cap is refused (`CapRefusal::RestingCap`): under the owner's
  $11 resting cap per side a $50 position is crossed in attempts of at most $11.
- The consumer bumps `attempt` to cross again after an attempt ends with the position not
  flat: after a partial fill, an unfilled IOC, or a venue refusal.
- The planner keeps the last attempt built per account, market and side for its lifetime; a
  consumer counts attempts up from any start within one planner's life (a new process starts a
  new planner).

## What would show this was wrong

- A crossing order of ours that took the position past zero.
- A crossing attempt sent twice, or a venue refusal of a cross repeated without a new attempt.
- A crossing intent that built an order while the market was Killed or Cancel-only, or past a
  cap.
- A force-close that never went out because its attempt was consumed by an order that never
  reached the venue.
