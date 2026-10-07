# 0066 — An Exit place, batch or amend is also checked at submit against its market's position revision at build, augmenting 0060 and 0063

Status: accepted
Date: 2026-10-07

## Context

0063 judges Exit's admission (reducing side, the side's exposure within the position's size)
when a place, a batch or an amend is built, and 0060 re-checks its authorization at submit
only against the market's state generation, which no fill advances (0060's Alternatives). The
order-entry session encodes without the registry (0057) and the shard host applies fills
between the encodes of one decide pass (FBC-ct0), so a fill can land between an Exit
command's build and its write. Codex's P1 on PR #101 (FBC-pfds): seeded long 10 by hand,
Flatten, a reducing sell of 10 authorized, then an own fill of no order the registry holds
selling 10 (`FillRouted::OursUntracked`) leaves the inventory flat, and the sell, still passing
the check at submit, leaves the position short 10. The same holds for an amend and a batch.
The hand-seeded owner-assisted testnet run (FBC-x69b, declared to the registry under 0067)
reaches it.

## Decision

The registry keeps a per-market position revision, a shared counter advanced each time the
market's inventory is written with a different value: a fill that moves it (ours, tracked or
not), the seed of a resync, a hand seed. A place, a batch or an amend built while its market
is in Exit carries the revision at build in its guard besides the state generation, and
`Authorization::check_at_submit` refuses it once the revision moved
(`StaleAuthorization::PositionMoved`), the state generation judged first. A command built in
any other state carries no revision; cancels and the instrument cancel-all are unchanged.

## Alternatives

- Re-judge Exit's admission at submit: rejected for 0060's reason, the session does not hold
  the registry, and the judgement needs every order on the side.
- Advance the state generation on a fill: rejected. The generation counts state changes
  (0012), and every quoting place in flight would be refused on each fill.
- Refuse only when the fill moved the position towards zero by more than the command's
  margin: not taken. It needs the side's exposure at submit, which the session does not hold;
  refusing on any move is the fail-closed reading and the planner builds again next pass.
- Guard quoting places by the revision as well: not taken here. Exit is the state whose
  admission depends on the position's sign and size; the caps' re-judgement while quoting is
  FBC-0d9k's.

## Consequences

- `tests/exit_submit.rs` proves it through place, batch and amend, long and short, on a
  market seeded by hand in a registry built for a declared testnet run (0067) and on a
  resync-seeded market; a fill moving no inventory of the market, a later
  resync (which never overwrites the inventory, 0055) and a quoting place leave the
  authorization passing; cancels always pass.
- In Exit, a fill of one of our own exit orders also refuses every other Exit command built
  before it, though its own fill shrank the side's exposure by as much: the command comes
  back `NotSent(StaleAuthorization)` (0062) and the planner builds it again against the new
  position. Exit trades a little latency for never sending an order sized before a fill
  already applied in the process.
- A fill the venue executes while an Exit order is still on its way to it is outside any
  check in the process, however it is ordered: only the venue's reduce-only flag stops that
  order crossing zero (FBC-zsfz). The check at submit and the write to the socket run in one
  pass with no wait between them, the same gap 0060 accepts.
- The inventory is written through one registry method, which advances the revision.

## What would show this was wrong

- An Exit place, batch or amend built before a fill the registry had applied before the
  command was encoded reaching a venue.
- Exit unable to complete on a venue whose fills arrive so often that every Exit command is
  refused at submit before it is written.
