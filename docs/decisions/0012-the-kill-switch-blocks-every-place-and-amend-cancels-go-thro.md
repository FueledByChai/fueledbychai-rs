# 0012 — The kill switch blocks every place and amend; cancels go through within 0005's guards; lifting it leaves the market cancel-only

Status: accepted, supersedes 0011
Date: 2026-10-02

## Context

0010's second rule puts the per-market kill switch in the pre-trade check every order command
takes, with cancels as the only exemption, and left one question open for the owner: whether a
reducing order (a flatten, a force-close or a wind-down exit) may be sent while a market's kill
switch is on. The owner answered on 2026-10-02: no, the kill switch blocks every place and
amend, reducing orders included; cancels always go through; and a position in a killed market
is exited deliberately, by the owner. 0011 recorded that answer.

0011 was merged before its review finished, and the review then found four faults in it
(FBC-52u; pull request #2):

- 0011 told the owner to exit a killed market by lifting the kill switch and then pressing
  Flatten or Wind-down. If order entry is still armed (0010, rule 1), lifting the switch makes
  every ordinary place and amend admissible again at once, so the quote loop can add exposure
  in the gap before the owner's exit. That is the opposite of exiting deliberately.
- 0011 said no cap and no kill switch ever blocks a cancel-all, and its tests required a
  cancel-all to be built while the switch is on. Invariant I7 in 0005 allows an instrument
  cancel-all only under an exclusive account-and-instrument lease with no foreign-namespace
  orders visible, and I4 never cancels a foreign-namespace order individually. Read literally,
  0011 would cancel another process's orders.
- 0011 said that resting orders "can still be cancelled, so the position cannot grow through our
  own orders". A cancel is a request: a resting, PendingNew or Unknown order can fill before the
  cancel is acknowledged, so the position can still change after the switch goes on.
- The product backlog restated 0011's rule instead of citing it, so a later record would leave
  that copy stale.

Records are never edited in place, so this record supersedes 0011. It keeps the owner's
decision as it was given and corrects the three faults in the rule itself; the fourth is fixed
in the documents that cite it. 0010 stays accepted and its text is not edited, but this record
changes how two of its rules are read, and says so below. Rule 2's "cancels are never blocked
by a cap or by the kill switch" stands, and this record states which other rules a cancel
still obeys. Rule 1's "trading is off after any restart until the owner presses Start" is
widened to let the owner exit without quoting first (see "How this reads 0010's rule 1").

The question is real because the design gives reducing orders privileges elsewhere. Invariant
I6 (0005; design §4.9) admits an order that genuinely reduces the position, and the
ExecutionPlanner (design §4.10) sends cancels, then reducing orders, ahead of amends and adds,
lets cancels and reducers use each rate-limit scope's safety floor, and gives the force-close,
wind-down and news-exit maker exits their own reduce-only fallbacks.

## Decision

Each market's order entry is in one of four states, held by `fbc-oms` and checked in the one
pre-trade path every order command takes (0010; I6 in 0005):

- **Killed.** No place and no amend or replace for the market is built, reducing orders
  included: a flatten, a force-close or disaster stop, a wind-down exit, a news exit, a
  reduce-only order and every batch item are all blocked. Being reducing or reduce-only is
  never a reason to skip the kill switch; I6's admission of reducing orders applies to the
  resting and inventory caps, not to the kill switch.
- **Cancel-only.** The same as Killed for places and amends. Lifting the kill switch moves a
  market from Killed to Cancel-only and to no other state, so lifting it never resumes quoting
  by itself.
- **Exit.** Entered from Cancel-only or Quoting only by the owner's Flatten or Wind-down.
  Only orders on the side that reduces the position are built, sized so that the position
  plus every resting order on that side, the new one included, never crosses zero; each still
  passes every cap, and the planner builds nothing else from the desired book. Once the
  position is flat, Exit builds nothing more. This is an added restriction, not a bypass: it
  never lets an order skip a check.
- **Quoting.** Normal order entry under 0010. Entered from Cancel-only or Exit only by the
  owner's Start.

Turning the kill switch on moves a market to Killed from any state. Leaving Killed or
Cancel-only always takes an owner's choice; nothing in the library, and no reconnect, resync or
timer, moves a market toward Exit or Quoting on its own. Start, Flatten and Wind-down are each
one explicit consumer call made on the owner's action, and each is refused for a Killed market.

0010's armed state keeps 0010's meaning, and a place or amend is built only for a market that
is both armed and in Exit or Quoting. Arming is part of these three calls, and there is no arm
call that leaves a market in Cancel-only:

- **Start** is 0010's Start. On a disarmed market it arms the market (with 0010's leases) and
  moves it to Quoting in one call; on an armed market it moves it to Quoting. Start is the only
  call that reaches Quoting.
- **Flatten** and **Wind-down** on a disarmed market arm it (with the same leases) and move it
  to Exit in one call, so an exit after a restart never passes through Quoting; on an armed
  market they move it to Exit.
- A call refused for want of a lease changes neither the armed state nor the market's state.

A disarmed market is always in Killed or Cancel-only. A new runtime starts every market
disarmed (0010, rule 1) and in Cancel-only, and an explicit disarm call moves a market in Exit
or Quoting to Cancel-only, so arming again always takes Start, Flatten or Wind-down. Whether a
kill switch survives a restart is the consumer's; if it does, the consumer turns it on before
any of those calls.

### How this reads 0010's rule 1

Taken literally, 0010's rule 1 lets only Start arm a market after a restart, and its falsifier
is "any order sent after a restart before the owner pressed Start". Under that reading the
owner could exit after a restart only by pressing Start first, which reaches Quoting and lets
the quote loop add exposure before the exit: the gap this record exists to close. So this
record changes how rule 1 is read, without editing 0010:

- After a restart, an order may be built only once the owner has pressed Start, Flatten or
  Wind-down for that market. Each is one explicit consumer call made on the owner's action, so
  rule 1's purpose holds: nothing resumes order entry by itself, and a crash loop never trades
  unattended.
- Before the owner presses Start, the only orders that may be built are Exit's: on the side
  that reduces the position, never crossing zero, and under every cap. Quoting after a restart
  still takes Start, exactly as 0010 says.
- 0010's falsifier is read accordingly: any order sent after a restart before the owner pressed
  Start, Flatten or Wind-down, or any order other than an Exit order sent after a restart
  before the owner pressed Start, breaks rule 1.

This is how this record applies the owner's 2026-10-02 decision "trading off after restart":
quoting stays off until Start, and an exit happens only when the owner asks for it. If the
owner reads rule 1 as forbidding even an owner's exit before Start, a new record drops this
path and supersedes this one.

Cancels go through in every state: no cap and no kill switch blocks a cancel or a cancel-many.
Every cancel still obeys 0005: own-namespace orders are cancelled by explicit reference (I4),
and an instrument cancel-all is built only under the exclusive account-and-instrument lease
with no foreign-namespace orders visible (I7). When either condition fails, the kill switch's
"cancel everything" goes out as explicit cancel-many items for the own-namespace orders, repeated
on each resync, and never as a cancel-all.

A cancel does not stop the position changing. An order can fill until it is terminal under
0005's lattice: a venue event that ends it (cancelled, fully filled, rejected or expired), or
the Unknown ladder resolving it to a terminal state. A partial fill leaves the order
PartiallyFilled, and the Unknown ladder's query can find an order still Open; in both cases its
remainder can still fill. Every order that is not terminal (PendingNew, Unknown, Open or
PartiallyFilled) keeps counting against the caps with its unfilled quantity, Unknown ones as
fully resting (I5, I6), until it is terminal.

Own-namespace fills apply to inventory once, through the `FillLedger` (I3), in every state,
Killed included: no state drops, defers or ignores one. Foreign-namespace fills are flagged and
not counted (I4), in every state as well; the market's state changes neither rule. What the
kill switch guarantees is that no new or amended order is built for the market, not that its
position is frozen.

## Alternatives

- Let reducing orders through while the kill switch is on, so a killed market can still be
  flattened automatically: rejected by the owner. A kill switch with exceptions is not a kill
  switch; whatever made the owner press it (a bad fill model, a desynced order map, a venue
  misbehaving) may be the same fault that makes an automatic exit go wrong, and "reducing" is
  judged from a position the library may have wrong at that moment.
- Let only a force-close (the disaster stop) through: rejected for the same reason, and because
  it would create the one classification-based bypass 0010 forbids.
- Lift the switch, then press Flatten or Wind-down, with no state in between (0011): rejected.
  While armed, the gap lets the quote loop place new exposure before the exit.
- Disarm the market when the switch is lifted, so the owner has to arm it again: rejected.
  0010 lets only a process start or an explicit disarm call disarm order entry, and lifting the
  switch is neither; a Cancel-only state gives the same protection without changing what 0010's
  armed state means.
- Keep 0010's rule 1 literal, so only Start arms after a restart and Flatten or Wind-down on a
  disarmed market is refused: rejected. Start reaches Quoting, so the owner could exit after a
  restart only by letting the quote loop add exposure first.
- A separate arm call before Flatten or Wind-down: rejected. A Start that reached Quoting
  before the exit has the same gap, and an arm call that left the market in Cancel-only would
  give "Start" two meanings. Flatten and Wind-down arm straight into Exit instead, and this
  record says outright that it reads 0010's rule 1 that way.
- One call that lifts the switch and starts an exit together: not needed. The consumer may
  present the two steps as one confirmation, and because Cancel-only sits between them, no
  ordinary order is admissible at any moment.
- Treat a cancel-all as just another cancel the kill switch must not block: rejected, because
  I7's lease guard is about whose orders a cancel-all reaches, not about the kill switch.

## Consequences

- `fbc-oms` tests assert, for a market in Killed and in Cancel-only, that no place, amend,
  replace or batch item is built, including one marked reduce-only, one that would reduce the
  position under I6, and one issued by a flatten, force-close or wind-down path; and that
  cancels and cancel-many are still built.
- Tests assert that lifting the kill switch while armed leaves the market in Cancel-only and
  builds no order from the desired book; that a fresh runtime starts disarmed and in
  Cancel-only; that Flatten or Wind-down on a disarmed market arms it straight into Exit and no
  ordinary order is built at any point; that a disarm call leaves the market in Cancel-only;
  that Exit builds only orders that reduce the position without crossing zero and still applies
  every cap; and that only Start reaches Quoting.
- Tests assert that while Killed a cancel-all is built only with the exclusive lease and no
  foreign-namespace orders visible, and that otherwise the own-namespace orders are cancelled
  by explicit reference and no foreign-namespace order is cancelled.
- Tests assert that an own-namespace fill arriving in Killed or Cancel-only, for an order whose
  cancel is in flight, still moves inventory once through the `FillLedger`; that a
  foreign-namespace fill in those states is flagged and moves nothing; and that PendingNew,
  Unknown, Open and PartiallyFilled orders still count against the caps until terminal,
  including the remainder after a partial fill and an Unknown order the ladder finds Open.
- The consumer's UI presents Flatten and Wind-down as choices made from Cancel-only, including
  right after a restart: pressing
  either in a Killed market is refused or asks the owner to lift the switch first, and showing
  a killed market as flat is wrong until every order is terminal and the position is zero.
- A position in a killed market stays open until the owner acts, and can still change through
  fills that race the cancels. That exposure is accepted.
- 0011 is superseded. 0010 stays accepted and its text is not edited; its rule 1 and its
  falsifier are read as "How this reads 0010's rule 1" says, and AGENTS.md, which restates
  rule 1, cites this record for an exit after a restart.

## What would show this was wrong

- A loss that grew while a market's kill switch was on because a reducing order could not be
  sent, and that an automatic exit would have stopped without the fault that triggered the
  switch also corrupting that exit.
- The owner routinely going Killed, Cancel-only, Flatten in quick succession under stress and
  finding the extra step slower than the exposure it guards against.
- The owner reading 0010's rule 1 as forbidding any order after a restart before Start, an
  owner's Flatten or Wind-down included.
- Any place or amend built for a market in Killed or Cancel-only, any ordinary order built in
  Exit, any order built after a restart before the owner's Start, Flatten or Wind-down, any
  order other than an Exit order built after a restart before the owner's Start, any order
  built after the switch was lifted but before the owner chose Start, Flatten or Wind-down, any
  cancel that a cap or the kill switch blocked, any cancel-all built without the I7 lease, any
  non-terminal remainder that stopped counting against the caps, or any own-namespace fill that
  did not move inventory because the market was killed.
