# 0063 — Exit builds only orders marked reduce-only or reducing, on the side that reduces the position, judged against that side's exposure, between the state and the caps

Status: accepted
Date: 2026-10-06

## Context

0012 defines Exit: only orders on the side that reduces the position are built, sized so that
the position plus every resting order on that side, the new one included, never crosses zero;
each still passes every cap; the planner builds nothing else from the desired book; once the
position is flat Exit builds nothing more; and "any ordinary order built in Exit" would show
0012 wrong. 0013 rule 1 calls these "reduce-only exit orders as 0012's Exit state defines
them". FBC-c4v built the states and left Exit building nothing; FBC-7gl builds its admission
in `fbc-oms`'s one pre-trade path. The planner that would build from a desired book (FBC-0j3)
does not exist yet, so the gate cannot ask it where an order came from, and three readings of
0012 are left open at the gate:

- What tells an exit order from an ordinary one on the reducing side: a quote loop's ask on a
  long position is on the reducing side and may fit within the position.
- What "every resting order on that side" counts: an order the venue reported filled before
  its fill event reached the inventory no longer rests, yet the position does not hold it.
- Where the restriction sits relative to the state check and the two caps (0052), and what it
  does while the position is unknown (0055).

## Decision

In a market in Exit, a place, an amend or replace, and every batch item is built only when all
of these hold, judged in this order after the state admitted Exit and before either cap
(`StateRefusal::Exit(ExitRefusal)`):

1. The market's position is known (`ExitRefusal::PositionUnknown`) and not flat
   (`ExitRefusal::Flat`).
2. The order is on the side that reduces it: a sell on a long position, a buy on a short one
   (`ExitRefusal::Increasing`).
3. The order is marked as an exit: it carries the venue's reduce-only flag or the OMS's
   reducing classification (`NewOrder::reduces`; for an amend, the placement's reduce-only flag
   or the amend's `reducing`). An unmarked order on the reducing side is an ordinary order and
   is refused (`ExitRefusal::Ordinary`). Requiring the mark is an added condition, never an
   exemption: a marked order passes every other check as any order does (0013 rule 2).
4. What our orders on that side may still move the position by, the order included, is at most
   the position's size (`ExitRefusal::CrossesZero`). Each order counts its
   `OrderRecord::exposure`, the same sum the inventory cap counts: PendingNew and Unknown orders
   in full, a partly filled order's remainder, an amend at the larger of its old and new
   quantity while in flight, the earlier items of the same batch as PendingNew, and the fills
   the venue reported that the inventory does not hold yet.

Then both caps judge it as any order (0052). Nothing moves a market out of Exit when it goes
flat; it simply builds nothing more until the owner's next call.

## Alternatives

- Side and size alone, no mark: rejected. The quote loop's ordinary asks on a long position
  would be built in Exit as long as they fit, which is the "ordinary order built in Exit" 0012
  names as its falsifier.
- Require the venue's reduce-only flag alone: not taken. `NewOrder::reducing` exists for venues
  without the flag (it makes the order safety traffic there), and 0013 rule 1's "reduce-only"
  is defined by 0012's side and size, not by a wire flag. A consumer that wants the venue to
  enforce it too sets both.
- Count only what rests (`OrderRecord::resting`): rejected. A fill the venue reported before
  its fill event arrived has already moved the real position; leaving it out lets the side
  cross zero once that event lands.
- Judge Exit after the caps: not taken. Exit is part of the market's state, which 0012 checks
  before either cap; judging it first also names the more specific refusal.
- Refuse while the position is unknown through the caps' `PositionUnknown`: not taken. Exit's
  judgement needs the position itself, so it refuses on its own rather than rely on a later
  check.

## Consequences

- `tests/exit.rs` proves each rule through place, place_batch, amend and replace, long and
  short, from Flatten and from Wind-down, with PendingNew, Unknown, Open and partly filled
  orders and a venue-reported fill on the side; that each cap still refuses; and that nothing
  is built once fills flatten the position or while it is unknown. `tests/states.rs` runs every
  shape 0012 names through Exit: only the marked reducing ones are built.
- The ExecutionPlanner (FBC-0j3) marks every order it builds for Exit, and builds nothing else
  from the desired book there; an unmarked one is refused here regardless.
- An order on the side that adds to the position stays resting through Exit until cancelled;
  Exit does not cancel it, and its fills still move the position (0012). The planner cancels
  it.
- A position already past the inventory cap is exited only by orders that bring the worst case
  back within it (I6), so a small first exit can be refused; that is the caps' rule, unchanged.

## What would show this was wrong

- Any order built in Exit that is unmarked, on the side that adds to the position, or that
  would let the position plus its side's exposure cross zero; any built while flat or while the
  position is unknown; or any cap refusal that Exit skipped.
- An owner's exit that could not be sent because the consumer's exit path could not mark its
  orders, on a venue where neither the flag nor the classification can be set.
