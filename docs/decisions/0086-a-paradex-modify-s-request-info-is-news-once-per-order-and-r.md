# 0086 — A Paradex modify's request_info is news once per order and request, a later event repeating it the order update alone, augmenting 0054

Status: accepted
Date: 2026-10-08

## Context

0054 makes a Paradex amend final on the `OrderEvent` whose request_info reports SUCCESS for
MODIFY_ORDER, and a REJECTED one an asynchronous reject of the amend (`AmendAck::ReplacedEvent`,
`reject_keeps_original`). FBC-aml's decoder read every version-2 event's request_info as fresh
and dropped `requestId`, the only field that ties a status to one modify. Paradex's "Get order"
REST page returns `request_info` as a field of the order, so it plausibly persists on later
updates of the same order, a fill's included. Reviewer B on PR #85 (RB85-1, filed as FBC-g3bw)
showed that such a later update then pushes a second `AsyncReject { op: Amend }`, or turns into
`Amended` instead of `Open`. With amend #1 rejected and amend #2, which raises the total, in
flight, a fill still carrying #1's REJECTED looks like #2's: a consumer settling #2 on it while
the venue applies #2 undercounts resting exposure against the inventory and per-side resting
caps. No page says whether request_info persists, nor what `requestId` echoes; the owner's
testnet run (FBC-8xr) is to show both.

## Decision

1. **News once per order and request.** The Paradex order-entry codec (the read-only codec and
   the order-entry codec, which carries it) holds, per venue order id, every `requestId` and
   `requestStatus` pair of MODIFY_ORDER request_info that order's events carried
   (`exec::ModifyRequests`), across all its frames and connections. An event whose MODIFY_ORDER
   request_info is one already held, the latest modify's or an earlier one's, reports nothing
   of the modify: it is the order update in the order's own state (Open, not Amended) and no
   `AsyncReject`. A pair not held, a new `requestId` or a new status of one (PENDING then
   SUCCESS), is news and is read as 0054 says. A `requestId` the frame does not carry is a value of its own: two modifies both
   without one cannot be told apart, so only the first one's outcome is reported.
2. **What leaves it unchanged.** An event with no MODIFY_ORDER request_info (version 1, the
   1:2 layout without it, or another request type) changes nothing held, so a repeat after it
   is still not news. A refused frame changes nothing held. An event that shows the order
   CLOSED is decoded as before (a REJECTED that is news still reported first), and what is held
   of that order is then dropped.
3. **What a consumer may rely on.** An `AsyncReject` of a Paradex amend is a REJECTED request_info
   this codec had not seen for that order, which is not proof that it answers the amend now in
   flight: `requestId` is not passed on (fbc-core's events carry none). A consumer must not
   settle an amend in flight on such a reject alone while another modify of the order may be
   unanswered; the Unknown ladder or a resync settles it.

## Alternatives

- Carry `requestId` to the consumer beside the reject and the amend: not now. fbc-core's
  `AsyncReject` and `OrderUpdate` have no field for it, and nothing yet says whether the venue
  echoes our JSON-RPC id or one of its own, so the consumer could not match it either.
- Hold nothing and treat every request_info as fresh (FBC-aml): rejected, RB85-1's failure.
- Hold only the latest pair per order: rejected (Codex on PR #139). With modify A rejected and
  modify B pending, a fill still carrying A's REJECTED would be news again and look like B's.
- Clear what is held on each new connection: rejected. The first update of an order after a
  reconnect would report a stale modify outcome again, which is the failure this record closes.
- Drop what is held of orders a resync does not list as open: not now. A REST answer read before
  an order's latest events could drop an entry still needed; see the consequences.

## Consequences

- A repeated request_info is harmless whatever the venue does, and one Paradex sends only once
  is reported as before.
- A second modify of an order that the venue reports without a `requestId`, with the same
  status as the one before it, is not reported; its amend stays in flight until the Unknown
  ladder or a resync settles it, the direction in which the OMS counts more resting exposure,
  not less.
- An order's pairs grow with each modify of it until it closes: one small entry per modify
  outcome seen, a few per amend.
- What is held of an order whose CLOSED event the codec never sees (closed while disconnected)
  stays for the codec's life: one small entry per such order, gone when the process restarts.
  A new codec (a restart) holds nothing, so a stale outcome on an order's first event after it
  is reported once, to a registry that has no amend of that order in flight.

## What would show this was wrong

- FBC-8xr's testnet run showing that a later update of an order never carries an earlier
  modify's request_info (this guard is then moot), or that one modify reports the same
  `requestId` and status on two events for two different amends.
- The entries of orders never seen closed growing without bound over a long session.
