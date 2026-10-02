# 0011 — The kill switch blocks every order, reducing orders included; cancels always go through

Status: accepted
Date: 2026-10-02

## Context

0010's second rule puts the per-market kill switch in the pre-trade check every order command
takes, with cancels as the only exemption, and left one question open for the owner: whether a
reducing order (a flatten, a force-close or a wind-down exit) may be sent while a market's kill
switch is on. Until a new record answered it, 0010 had the kill switch block every place and
amend. This record is that answer. It resolves the open question only; 0010's three rules stand
as written, so this record does not supersede 0010.

The question is real because the design gives reducing orders privileges elsewhere. Invariant
I6 (design §4.9) admits an order that genuinely reduces the position, and the ExecutionPlanner
(design §4.10) sends cancels, then reducing orders, ahead of amends and adds, lets cancels and
reducers use each rate-limit scope's safety floor, and gives the force-close, wind-down and
news-exit maker exits their own reduce-only fallbacks. Without a decision, a reducing order
could look like a natural exception to the kill switch too.

The owner decided on 2026-10-02.

## Decision

While a market's kill switch is on, no place and no amend or replace for that market is built,
reducing orders included: a flatten, a force-close, a wind-down exit, a news exit, a
reduce-only order and every batch item are all blocked. Cancels always go through: no cap and
no kill switch ever blocks a cancel, a cancel-many or a cancel-all. To exit a position in a
killed market, the owner lifts the kill switch and then uses Flatten or Wind-down deliberately;
the library never sends a reducing order on its own while the switch is on.

The kill switch is checked in the same `fbc-oms` pre-trade path as the caps (0010; invariant
I6 in design §4.9 and the planner's pre-trade step in §4.10). Being reducing, reduce-only, a force-close or a
disaster stop is never a reason to skip it, and I6's admission of reducing orders applies to
the resting and inventory caps, not to the kill switch. The reducing-order privileges of design
§4.10 (ordering, the safety floor, the reduce-only fallbacks) apply only while the market's
kill switch is off.

## Alternatives

- Let reducing orders through while the kill switch is on, so a killed market can still be
  flattened automatically: rejected by the owner. A kill switch with exceptions is not a kill
  switch; whatever made the owner press it (a bad fill model, a desynced order map, a venue
  misbehaving) may be the same fault that makes an automatic exit go wrong, and "reducing" is
  judged from a position the library may have wrong at that moment.
- Let only a force-close (the disaster stop) through: rejected for the same reason, and because
  it would create the one classification-based bypass 0010 forbids.
- A separate "exit only" switch state beside the kill switch: not needed now. Lifting the kill
  switch and pressing Flatten or Wind-down gives the same result with a person deciding. A
  later record can add a state if the owner wants one.

## Consequences

- `fbc-oms` tests assert that while a market's kill switch is on, no place, amend, replace or
  batch item is built for it, including one marked reduce-only, one that would reduce the
  position under I6, and one issued by a flatten, force-close or wind-down path; and that
  cancels, cancel-many and cancel-all are still built.
- Flatten and Wind-down are owner actions taken after the kill switch is lifted. The consumer's
  UI has to present it that way: pressing Flatten in a killed market is refused (or asks the
  owner to lift the switch first), it does not lift the switch by itself.
- A position in a killed market stays open until the owner acts. That exposure is accepted:
  resting orders can still be cancelled, so the position cannot grow through our own orders.
- 0010's open question is closed. 0010 stays accepted and unedited.

## What would show this was wrong

- A loss that grew while a market's kill switch was on because a reducing order could not be
  sent, and that an automatic exit would have stopped without the fault that triggered the
  switch also corrupting that exit.
- The owner repeatedly lifting the kill switch only to flatten and then re-arming it, which
  would argue for an "exit only" state in a new record.
- Any place or amend sent for a market while its kill switch was on, or any cancel that a cap
  or the kill switch blocked.
