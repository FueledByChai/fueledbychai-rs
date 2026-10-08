# 0075 — A replace whose cancel was sent and came back not sent or refused is over, augmenting 0065

Status: accepted
Date: 2026-10-07

## Context

0065 rule 4 replaces an order whose change no amend admits by cancelling it, the level then
waiting until the order is terminal before the new order is placed; rule 5 carries a cancel
that waited for the order's acknowledgement through, the level waiting the same way; rule 7
builds a cancel not reported sent again at the next pass. Nothing ended that wait except the
order's terminal state. Codex on PR #103 (r4206675085), filed as FBC-ubsw: the replace's
cancel is reported sent (`Registry::cancel_sent`) and then comes back `NotSent` or `Rejected`.
The order still rests with nothing in flight, yet every later pass discarded the desired
quote, even one equal to the resting order, and built another cancel, losing the order's queue
position for a cancel the level no longer needs.

## Decision

1. **A refused replace is over.** A level being replaced (0065 rule 4) or carrying a cancel
   through after the acknowledgement (0065 rule 5) whose cancel was reported sent, and whose
   order then rests with no cancel in flight (the cancel came back not sent or refused), is
   decided afresh at the next pass: a quote equal to the order builds nothing and keeps it, a
   later change amends it where the caps allow or replaces it again, and a level no longer
   wanted has it cancelled as usual.
2. **Sent is told apart by a count.** The `OrderRecord` counts the cancels reported sent for an
   order (`OrderRecord::cancels_sent`); the planner keeps that count when it decides the
   replace's cancel, and a count past it with no cancel in flight means that cancel was sent
   and answered without ending the order. A cancel built and not reported sent leaves the
   count where it was, so 0065 rule 7 still builds it again at the next pass.

The rest of 0065 stands, and so does 0070 (an unconfirmed amend still holds its level).

## Alternatives

- Keep cancelling until the order ends: rejected. The venue refused or never got the cancel,
  the order still rests where it was, and a level wanted at that very quote then loses its
  queue position for nothing.
- Clear the replace whenever no cancel is in flight: rejected. A cancel built and not reported
  sent also leaves none in flight, and 0065 rule 7 says it is built again; only the count tells
  the two apart.

## Consequences

- After a refused or unsent replace cancel, the planner may keep the old order, amend it, or
  build the replace's cancel again, as the desired book and the caps then say; a venue that
  keeps refusing a cancel is asked again only when the level is decided to need it.
- Each `OrderRecord` carries one more counter, bumped only by `Registry::cancel_sent`.

## What would show this was wrong

- A planner pass that builds a cancel for a level whose desired quote equals its resting order
  after that order's replace cancel came back not sent or refused.
- A replace cancel built and not reported sent that the next pass does not build again.
