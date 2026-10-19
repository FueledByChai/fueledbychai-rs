# 0102 — SimVenue sends a two-phase venue's amends, answered provisionally and settled by the amended order's event, and refuses an amend of a placement held for its risk check, augmenting 0099

Status: accepted
Date: 2026-10-18

## Context

FBC-2zxk's done line asks for a SimVenue built from fbc-venue-paradex's declared caps that answers
an amend and refuses no command as `Unsupported`. Paradex is `AckModel::TwoPhase` (0054), and 0099
left `SimCodec` refusing every amend for a two-phase venue (FBC-kbwj), so a Paradex stand-in could
place and cancel but never amend. Paradex answers `order.modify` provisionally and confirms the
amend on the order event reporting SUCCESS for MODIFY_ORDER (`AmendAck::ReplacedEvent`; its reply
table, 0054), with no later outcome; fbc-oms treats an amend accepted `Final` as answered and one
accepted `Provisional` as settled by the order's amended update. The engine already answers every
accepted amend with the amended order's event at the same instant. 0099's held placement (one the
consumer configured a risk reject for) is in no resting order, so an amend naming it was refused
`NotFound`, which sends fbc-oms's order to the Unknown ladder while the venue still holds it; with
amends sent for a two-phase venue that path becomes reachable.

## Decision

- **Sent, provisional, settled by the event.** The codec sends amends for a two-phase venue as for
  a single-phase one (still refusing them for a venue that gives the amended order a new venue
  id, FBC-kodq). The engine answers an accepted one with a provisional acceptance of the request,
  naming the order's client and venue ids, then the amended order's event (`Amended`, same venue
  id) and anything it takes, as before; nothing more is answered for it, so no second phase follows
  its risk window. The codec decodes that acceptance `Accepted { ack: Provisional }` and awaits
  nothing for it, so an amend holds no state on the connection. A refused amend is refused at once,
  as on a single-phase venue, and a single-phase venue's accepted amend is still `Final`.
- **A held placement is not amendable.** An amend naming a placement held for its risk check is
  refused `NotAmendable(Unsupported)`; the placement stays held, refused by the check at its
  instant or withdrawn by a cancel, as 0099 has it.

## Alternatives

- **Final at once, as cancels are answered** (lift the refusal only): rejected; fbc-oms takes a
  final amend as answered on its outcome and a provisional one on the amended update, so a Paradex
  stand-in would run another path than live Paradex does.
- **Provisional, then final once the window passes, as a placement** (FBC-kbwj's done line):
  not taken; Paradex reports no later outcome for a modify, and 0085's two-phase rule is about
  placements. FBC-kbwj stays deferred for a venue whose amends and cancels do have two phases of
  their own, including an asynchronous amend reject.
- **The codec tracking each amend request until answered**: rejected; the answer frame saying
  what it is needs no state, and a tracked request unanswered across a lost frame would stay held.
- **An amend of a held placement accepted, changing the held order**: not taken; the venue's
  behaviour is undocumented, and the held order is about to be refused anyway.

## Consequences

- A SimVenue built from Paradex's caps places, amends, cancels, queries, arms cancel-on-disconnect
  and fills through the codec; FBC-2zxk's test shows it (`tests/paradex_stand_in.rs`).
- A consumer over a two-phase SimVenue sees amends as Paradex shows them: provisional, then the
  amended update. Cancels are still answered final at once (FBC-kbwj), and the engine's own
  refusals stay synchronous (FBC-fsqn).
- The simulated stream gains one answer kind (`amended`).

## What would show this was wrong

A Paradex testnet or mainnet run in which a modify is confirmed by a later outcome, or refused
after its order event reported it amended; or a consumer path that behaves differently over
SimVenue's provisional amend than over Paradex's.
