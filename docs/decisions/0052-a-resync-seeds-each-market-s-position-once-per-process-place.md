# 0052 — A resync seeds each market's position once per process, places a fill straddling the snapshot by its arrival, order and time, and later resyncs only compare, augmenting 0005 and 0013

Status: accepted
Date: 2026-10-05

## Context

0013's rule 1 re-reads open orders and positions from the venue after a restart, and nothing
is sent until the first resync has landed; 0005's I3 lets inventory change only through fills
the ledger deduplicated (FBC-38r, BT-402). A resync's position is the venue's at the instant
it read the account, which lies somewhere between the request (the watermark, the request's
`EncodeCtx.wall`, 0014's Alternatives) and the answer. A fill executed in that window may or
may not be in it, and its event can reach us before or after the answer, so neither "count
every fill after the watermark" nor "count every fill after the answer" counts it exactly once.
FBC-2e4's `seed_position` refused to seed a market once a fill had moved it, leaving this case
to the resync; and an order the first resync registers already has fills in the seeded
position, which its fill count must not count again (FBC-2e4, Codex r4189028838).

## Decision

- **The seed.** The first resync that reaches a market seeds its position, once per process:
  every market the consumer's caps configure, the snapshot lists or a fill moved. Until then
  the position is unknown (`Registry::position` is `None`) and no place or amend is built on
  it. The seed registers our namespace's open orders the snapshot shows that the registry does
  not hold (an earlier run's) with the snapshot's cumulative fill as the fills they have
  counted; such an order is cancelled, never amended, since its time in force and channel are
  not known. An instrument the snapshot does not list is flat (0049).
- **A fill straddling the snapshot.** Fills accepted before the seed are kept with their
  order, cumulative fill, matching-engine time and arrival (the ledger's monotonic `now`). The
  snapshot holds a fill when: it arrived before the request was sent (its `EncodeCtx.mono`,
  which the consumer gives beside the watermark); or the snapshot shows its order with a
  cumulative fill at least the fill's `cum_after` (a fill without one: its matching-engine time
  is at or before the watermark); or the snapshot does not show its order and the fill executed
  by the watermark, or the venue's snapshot source is trustworthy (the order was not open when
  the venue read the account, so every fill of it came before; nothing of ours is placed
  before the seed). It does not hold a fill of an order it shows with a lower cumulative fill,
  nor any fill of an order placed after the seed. The seed is the snapshot's position plus the
  fills it does not hold, and an order it shows counts its shown cumulative fill plus those.
  After the seed the same rules place each fill: one the seed holds is kept by the ledger and
  counted on its order, never on the inventory.
- **A fill nothing places.** Before the seed, the market is not seeded and stays unknown; a
  resync requested after the fill arrived places it. After the seed, the market's position is
  unknown for the rest of the process: nothing re-seeds it.
- **Later resyncs** never seed or overwrite the inventory. Each seeded market's position is
  compared with the inventory as of the watermark: the fills counted that arrived before the
  request, and those after it with a matching-engine time at or before the watermark. A
  difference is reported to the consumer as a desync; a resync requested before one already
  compared is not compared. Every resync applies its orders to the orders the registry holds
  as order updates (the cumulative fill raises `cum_venue` only), the Unknown ladder's through
  its own path.

## Alternatives

- **Count every fill executed after the watermark** (I3's session-start rule): rejected. The
  venue reads the account after the request, so a fill in between is in the position and
  would count twice.
- **Arrival order alone** (a fill before the answer is in it, after it is not): rejected. It
  holds only where the snapshot rides the fills' ordered stream, as SimVenue's does (0049); a
  REST snapshot beside a WebSocket fill stream gives no such order.
- **Re-seeding a market whose position became unknown** from a later resync: not taken. An
  order placed before that resync's request may reach the venue after its read, so the third
  rule no longer holds; the market stays unknown until a restart, which is the conservative
  side, rather than modelling placements in flight.
- **Overwriting the inventory with a later snapshot's position**: rejected (I3); a desync is
  the consumer's to judge, over more than one resync, since a fill executed between the request
  and the read shows as one.

## Consequences

- The consumer gives each resync's watermark and the monotonic instant of its request, and
  the fill ledger's `now` is on that clock.
- Before the seed, `Registry::inventory` is the sum of the fills accepted, not the position.
- A venue whose fills carry neither a cumulative fill nor a matching-engine time, with orders
  of ours resting across a restart (no cancel-on-disconnect), will often leave a market unknown
  after the seed; 0013's cancel-on-disconnect rule makes that rare.
- FBC-c4v refuses arming while the position is unknown; FBC-840 acts on the open orders of ours
  a later resync reports that the registry does not hold.

## What would show this was wrong

A desync that persists across resyncs on a venue whose fills and snapshots follow these rules,
or a market left unknown so often after the seed that quoting stops on a venue the owner means
to trade.
