# 0098 — The execution planner copies each level's opaque consumer tag onto its places and amends and each side's removal reason onto the cancels of its levels no longer wanted, kept on the order for the audit verdicts, augmenting 0065

Status: accepted
Date: 2026-10-18

## Context

A consumer's `order_audit` needs, for every order, the origin the consumer gave it and, for
every cancel the planner builds, the reason the level went away: race-fill analysis splits
fills by that reason, and without it every planner cancel reads as `level_removed` (FBC-s6fp,
BT-402). 0065 defines the `DesiredBook` and the `ExecutionPlanner`, and 0005's I8 the audit
sink every raw order event and its verdict reach (FBC-cor). Neither carries anything of the
consumer's beyond the quote itself. What the consumer's values mean is the consumer's, never
this repository's (0001).

## Decision

1. **Tags and reasons in the book.** Each `DesiredQuote` carries the consumer's tag, a `u16`
   (`DesiredQuote::tag`); each side of a `DesiredBook` carries an optional removal reason, a
   `u16` (`DesiredBook::with_removal`, `set_removal`, `removal`).
2. **Onto the planned commands.** The planner copies a level's tag onto every place and amend
   it builds for that level (`Planned::tag`), and the side's reason onto every cancel it builds
   for a level the book no longer wants, absent or wanted at quantity zero
   (`Planned::removal`), the reason the pass that builds the cancel states. A cancel of a level
   still wanted (a replace, a cancel carried through after its acknowledgement, Exit's on the
   side that adds; 0065 rules 4, 5 and 8) carries none. A cancel's tag is the order's.
3. **Kept on the order, carried by the verdicts.** The registry keeps on each order the tag of
   the last place or amend and the reason of the last cancel the planner built for it
   (`OrderRecord::tag`, `OrderRecord::removal`), and the audit sink's verdicts carry both
   (`AuditEntry::Verdict`), `None` for an event routed to no order of ours and for an order the
   planner did not place.
4. **Opaque.** fbc-oms gives the values no meaning and no check reads them (0001).

## Alternatives

- Tags and reasons as strings or enums fbc-oms defines: rejected. Their meaning is the
  consumer's (0001), and a `u16` keeps the planned command and the verdict small and `Copy`.
- The tag on `fbc_core::NewOrder`, so every place carries one: rejected for now. Only the
  planner's orders need it; every adapter and sample would change for a value they never read.
- The reason remembered from the first pass that decided the removal: rejected. A cancel not
  reported sent, or waiting for its acknowledgement, is built again; the reason the pass that
  builds it states is the one the consumer last gave, and the rule needs no memory.

## Consequences

- A consumer writes its order origin and cancel reasons to `order_audit` from the verdicts it
  already receives, without a map from client ids of its own.
- An amend's tag is kept from its build, so an amend the venue refuses leaves the new tag on
  the order; orders placed outside the planner carry none.

## What would show this was wrong

- A consumer needing more than 65 536 distinct tags or reasons, or the tag of a refused amend
  misattributing a fill in the race-fill analysis: then the tag moves to the amend's
  confirmation, or widens.
