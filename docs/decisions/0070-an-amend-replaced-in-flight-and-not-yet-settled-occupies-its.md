# 0070 — An amend replaced in flight and not yet settled occupies its level, augmenting 0065

Status: accepted
Date: 2026-10-07

## Context

0065 rule 5 holds a level whose order has "an amend or cancel in flight, or an amend built and
not reported sent". The `OrderRecord` also keeps an amend that a later command replaced in
flight as unsettled (its total still counted by both caps), since it may still reach the venue,
until a venue update ordered after it settles it or the order ends
(`OrderRecord::amend_unconfirmed`). Codex's P1 on PR #103 (FBC-0j3), filed as FBC-cit6: the
planner sends an amend; the level is pulled, or the market enters Exit, so a cancel replaces it
in flight; the cancel comes back Rejected or NotSent. Nothing is in flight, but the amend is
unconfirmed. The planner read only the intent and the amend built, so a level wanted again got
a second amend over the unconfirmed one (on a venue whose amend states the remaining quantity,
not idempotent), or was judged against the pre-amend price and left as satisfied.

## Decision

1. **An unconfirmed amend occupies its level.** A level whose order has an amend replaced in
   flight and not yet settled is held as one with a command in flight (`HeldReason::InFlight`):
   no place, amend or replace is built there until a venue update ordered after the replaced
   amend settles it, or the order ends. The planner reads `OrderRecord::amend_unconfirmed`,
   which also covers 0065 rule 5's amend in flight and amend built.
2. **Cancels still go.** A level no longer wanted, or an order Exit cancels (0065 rule 8), is
   cancelled as before: a cancel never adds exposure, whatever an earlier amend did.

The rest of 0065 stands.

## Alternatives

- Amend again over the unconfirmed amend, the caps counting both: rejected. A remaining-quantity
  amend is not idempotent at the venue, and the record's price may not be the venue's, so the
  planner would diff against a price the order may no longer have.
- Cancel the order to replace it when the level is wanted at another price: deferred to
  FBC-tp0h, which needs it for venues without ordering keys; holding is the smaller change and
  what 0065 rule 5 already does for a command in flight.

## Consequences

- After a refused or unsent cancel over an amend, a level wanted again waits for the venue's
  next ordered update stating the order's total (or the order's end) before the planner changes
  it again.
- On a venue whose order updates carry no venue ordering key, a replaced amend settles only
  when the order ends, so such a level stays held, its order resting where the venue has it,
  until then (FBC-tp0h).

## What would show this was wrong

- A planner pass that builds an amend for an order whose `amend_unconfirmed` is true.
- A level held this way long enough, on a venue we trade, that its stale price costs more than
  a cancel and a new place would: then FBC-tp0h's replace is needed now.
