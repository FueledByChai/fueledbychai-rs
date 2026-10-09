# 0091 — A Paradex order's modify outcomes are held in a window of its latest 32, a PENDING not at all, augmenting 0086

Status: accepted
Date: 2026-10-09

## Context

0086 makes a Paradex modify's request_info news once per order and `(requestId, requestStatus)`
pair: the codec's `exec::ModifyRequests` holds every MODIFY_ORDER pair each order's events
carried, and drops an order's pairs only on its CLOSED event. Reviewer B on PR #139 (RB139-1,
filed as FBC-ypq9) showed that this grows without bound for the normal maker pattern, a resting
order requoted by amending: about two pairs per amend (PENDING, then SUCCESS or REJECTED), so one
order amended every 250 ms holds some 690,000 pairs after a day. 0086's own consequences named
the growth. What must stay held is what a later re-delivery could still repeat; 0086 keeps more
than an earlier modify's outcome only because Codex on PR #139 showed one can be repeated after a
later modify's event (A REJECTED, B PENDING, a fill still carrying A's REJECTED).

## Decision

1. **Only outcomes are held.** A MODIFY_ORDER request_info whose status is not SUCCESS or
   REJECTED (PENDING, PROCESSED, any other) confirms nothing whether it is news or not, so it is
   not held and changes nothing held. What is reported is unchanged from 0086.
2. **A window of the order's latest 32 outcomes.** Each order holds at most
   `exec::MODIFY_OUTCOMES_HELD` (32) SUCCESS or REJECTED pairs, in the order they were first
   seen; a new one past that drops the order's oldest. A repeat of any held pair, the latest or
   an earlier one's, is not news, as 0086 decides. A repeat of one older than the window is news
   again: reported as 0054 reads it and held again as the order's latest.
3. **The rest of 0086 stands.** What is held is per venue order id, kept across the codec's
   frames and connections, untouched by a refused frame or an event without MODIFY_ORDER
   request_info, and dropped once an event shows the order CLOSED. `ModifyRequests::outcomes`
   counts what is held, at most 32 for each order held.

## Alternatives

- Hold every pair until the order closes (0086 as it stood): rejected, RB139-1's unbounded
  growth.
- Hold only the outcomes since the order's last settled modify (drop earlier ones once a later
  modify's outcome is seen): rejected. Codex's case on PR #139 is an earlier modify's outcome
  repeated after a later modify's event, nothing says the venue cannot also repeat it after the
  later one's outcome, and `requestId`s carry no order a decoder could compare.
- A window by time (outcomes first seen within the last N seconds): not now. The decoder has
  only the venue's stamps, and a count bounds memory whatever the amend rate.
- Refresh a held outcome on each repeat (least recently seen dropped first): not needed. The
  outcome an order keeps carrying is its latest, which a first-seen window already keeps.

## Consequences

- What is held of one order is bounded however often it is amended: 32 small entries at most,
  a few kilobytes. Orders whose CLOSED event the codec never sees still each keep theirs for the
  codec's life, as 0086 says (FBC-86uc is to drop them on a trustworthy resync).
- A re-delivery repeats what the order carried when the venue published it, its latest modify's
  outcome then; to repeat one outside the window, an event must have been published before 32
  later modifies of the order were answered and be decoded after them all. Should that happen,
  that one event is read as FBC-aml read every event: a stale REJECTED is another
  `AsyncReject { op: Amend }`, which 0086 already forbids a consumer to settle an amend in flight
  on alone, and a stale SUCCESS makes the update `Amended` (at the price and size the event
  states) instead of `Open`.
- Dropping PENDING pairs reports nothing differently, since a PENDING or PROCESSED is never
  more than the order update.

## What would show this was wrong

- FBC-8xr's testnet run, or a later session's log, showing an `OrderEvent` repeating a modify
  outcome more than a few modifies of the order old: the window is then too small, or the venue
  reorders more than 0086 assumed.
- A session's `ModifyRequests::outcomes` growing with the number of amends rather than with the
  number of orders held.
