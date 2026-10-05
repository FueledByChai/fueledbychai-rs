# 0052 — The OMS enforces two pre-trade caps per market, the inventory cap (0005's I6) and a gross resting cap per side, both required consumer configuration, augmenting 0005 and 0013

Status: accepted
Date: 2026-10-05

## Context

0013 rule 2 names two pre-trade caps, "the resting-order cap" and "the inventory cap", checked
on every place, amend or replace and batch item, reducing orders included, with values from the
consumer's configuration (0009). 0005 gives one formula, I6, and calls it the "pre-trade
resting cap": `|pos + Σ resting same side (incl. PendingNew and Unknown) + new| ≤ cap`. That
formula bounds the worst-case position if every resting order on the command's side filled, but
not the order stack: on the side that reduces the position it admits resting orders up to twice
the cap (long at the cap, offers up to `2 × cap` keep `|pos − resting| ≤ cap`). An order stack
the OMS lets grow invites the failure where orders pile up past the position limit and one move
sweeps them all. FBC-2e4 built I6; the owner's open question
(OQ-inventory-cap, FBC-zf7) asked which number bounds what. The owner answered on 2026-10-04
(sprint-3 decision A): both caps are enforced, on every order command, reducing ones and batch
items included, both from the consumer's configuration with no default in code.

## Decision

- **Names.** The **inventory cap** is 0005's I6, the worst case:
  `|pos + Σ resting same side + new| ≤ inventory cap`. 0005's name "pre-trade resting cap" for
  I6 is read as the inventory cap from now on. The **resting cap** is a separate gross bound
  per side: `Σ resting same side + new ≤ resting cap`, whatever the position. `fbc-oms` uses
  these names (`MarketCaps::inventory`, `MarketCaps::resting`, `CapRefusal::InventoryCap`,
  `CapRefusal::RestingCap`).
- **What rests.** Both caps count resting the same way, one implementation
  (`OrderRecord::resting`): PendingNew and Unknown orders in full, a partly filled order's
  remainder until it is terminal, an amend at the larger of its old and new quantity from when
  it is built until it is acknowledged, and the earlier items of the same batch as PendingNew.
  The inventory cap also counts the fills the venue reported that the inventory does not hold
  yet (`OrderRecord::exposure`); they no longer rest, so the resting cap does not.
- **Where.** Both are checked in the one path every place (`Registry::place`), batch item
  (`Registry::place_batch`) and amend or replace (`Live::amend`) is built through, the
  inventory cap first; a command either refuses is never built. Nothing is exempt as reducing.
  Cancels and cancel-many pass whatever the caps' state.
- **Configuration.** A market's caps are built only from a `MarketCapsConfig` stating both;
  one missing either is refused (`CapsConfigError`), and a market the configuration does not
  name admits no order. Zero is a value the consumer states: it admits nothing on that side.
  Quantities are lots; the consumer converts a notional cap at the price it chooses.

## Alternatives

- The inventory cap as the position alone (`|pos + new| ≤ cap`), I6 as the resting cap: the
  recommendation in the question. Not chosen: it leaves the reducing-side stack bounded only by
  `2 × cap`, and the owner chose a gross per-side bound.
- One number for both, I6 checked once: refused for the same reason.
- A resting cap on the adding side only: a stack on the reducing side is the one I6 leaves
  open, so the bound applies on both sides.

## Consequences

- A consumer configures two numbers per market; a quoter that ladders more than the resting
  cap on one side gets its later rungs refused, reducing rungs included. An exit after a
  restart (0013 rule 1) is sized under the resting cap too.
- A shrinking amend on a side already over the resting cap (orders a resync registered, say)
  is refused, being judged at its old quantity until acknowledged; a cancel is always built.
- The kill switch and market states (FBC-c4v) and the authorization's check at submit sit
  beside these caps in the same path.

## What would show this was wrong

- A place, amend or batch item built while its side's resting quantity, counted as above,
  exceeds the resting cap, or the worst case exceeds the inventory cap.
- A legitimate exit refused because the resting cap is smaller than the position it must
  close: the consumer would have to raise the cap or exit in steps, which a new record
  addresses.
