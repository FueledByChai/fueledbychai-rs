# 0092 — Paradex's order events give a placement its final phase: a final acceptance once past the risk check, an asynchronous reject once closed unfilled, augmenting 0069

Status: accepted
Date: 2026-10-09

## Context

0054 makes Paradex `AckModel::TwoPhase` and 0069 has its codec accept a placement
`Accepted { ack: Provisional }` from the `order.create` reply (and each created
`order.create_batch` item), with no later outcome. 0085 has `two_phase_ack` require that a
provisional acceptance be followed, within `risk_reject_window`, by a final acceptance
(`Accepted` at `AckLevel::Final` for the placement's request) or an `ExecEvent::AsyncReject` of
the placement, and not by an order update alone; its Consequences leave Paradex's codec to
report the final phase, in FBC-6oj. Paradex reports the phase only on the order's
`OrderEvent`: status NEW is "accepted, before the risk check", OPEN rests, CLOSED ends it with
a `cancelReason` (0054; `fixtures/paradex/exec/README.md`). The event can come before or after
the reply.

## Decision

- **Passed.** An `OrderEvent` of our order showing it OPEN, or CLOSED with anything filled,
  gives a placement awaiting its final phase `Accepted { ack: Final }` for its request and
  item (the item its provisional acceptance named), pushed before that order update, under the
  event's `VenueMeta`.
- **Refused.** One showing it CLOSED with nothing filled, for any reason other than
  USER_CANCELED (an empty reason included), gives an `ExecEvent::AsyncReject` of the placement
  (`OpKind::Place`, our client id and its venue id), pushed before the order update, as a
  refused modify's is (0054). Its kind is `PostOnlyWouldCross` for POST_ONLY_WOULD_CROSS and
  `Other` for any other reason, the reason its `raw`, and it has no `venue_code`: a cancel
  reason is not a code 0069's table keys on.
- **Neither.** NEW waits for a later event. CLOSED by USER_CANCELED with nothing filled (our
  cancel before the risk check ended) settles the placement with nothing more.
- **Before the reply.** What the first such event shows of a placement sent and not yet
  answered is held until its reply: passed, the reply's outcome is the final acceptance at
  once (no provisional one); refused, the provisional acceptance followed by the asynchronous
  reject; withdrawn, the provisional acceptance alone.
- **Held no longer than needed.** A placement is held by our client id from its encode until
  its event settles it, its reply or timeout reports it anything but accepted, or a new
  connection opens (the resync then reads what rests, 0027).

## Alternatives

- OPEN as an order update only, the provisional acceptance standing: rejected by 0085, since
  an order update is not taken for the final phase.
- A final acceptance on NEW: rejected; NEW is before the risk check.
- Mapping more cancel reasons to finer kinds (NOT_ENOUGH_MARGIN to `Margin`): not done; no
  Paradex page lists the reasons, and the order update's `CancelReason` already carries the
  ones `closed` maps. The testnet run (FBC-8xr) records what the venue sends.

## Consequences

- Paradex passes `two_phase_ack` (FBC-6oj); nothing in fbc-oms or fbc-runtime reads
  `AsyncReject` or the second acceptance's level beyond what 0085 checks, and fbc-oms leaves an
  accepted order's state unchanged on a second acceptance.
- An order event arriving after a reconnect for a placement accepted before it gives no final
  phase; the resync and the order updates settle that order.

## What would show this was wrong

A testnet run (FBC-8xr) showing an OPEN order later refused by the risk check, or a CLOSED
order with nothing filled that the venue had accepted past its risk check (a self-trade
prevention cancel, say), which would make one of these phases the wrong one.
