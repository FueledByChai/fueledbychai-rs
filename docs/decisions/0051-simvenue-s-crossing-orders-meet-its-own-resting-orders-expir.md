# 0051 — SimVenue's crossing orders meet its own resting orders, expired under self-trade prevention, and deplete the displayed levels they take, augmenting 0046

Status: accepted
Date: 2026-10-05

## Context

0046's engine crosses an order against the trading book's displayed levels only (FBC-4qr).
SimVenue's own resting orders are never in that book, since the real venue never saw them, so
a crossing order passed straight through them (a crossed own book); and nothing a crossing
order took shows in the real book, so two crossing orders between two book updates took the
same displayed lots twice, as did two marketable items of one batch (FBC-nv2, Codex
r4186693194 on PR #70). The consumer's disaster stop and news flatten exits are crossing
orders in every backtest, so their fills and slippage must be modelled for the stop's tail to
be credible.

## Decision

- **What a crossing order meets.** It meets the opposite side best price first, within its
  limit, at every price where the displayed level still holds public size or one of SimVenue's
  own resting orders rests (one inside the spread included). At a price it takes the public
  size ahead of each of our orders there, in the order the venue matches them, as far as the
  level still holds public size, then meets that order; the public size behind them is taken
  last. A post-only order refused for crossing (0046) is refused for crossing our own order
  too.
- **Self-trade prevention.** SimVenue places every order for one account, so any
  `caps.matching.stp_scope` but `None` covers a crossing order meeting our own order. The
  venue then expires the crossing order there (`EXPIRE_TAKER`): what it took before stands,
  its rest is cancelled with `CancelReason::SelfTrade` and never rests, and our resting order
  is untouched but for the public size taken ahead of it. `VenueCaps` says across what a venue
  prevents self-trades, not what it does; the action is Paradex's documented default
  (docs.paradex.trade, `POST /orders`, `stp`: "if empty EXPIRE_TAKER"), taken here for every
  venue until the caps declare it (FBC-70pc). A fill-or-kill order expired before it could fill
  whole takes nothing. Under `None` the two orders trade: the crossing order's taker fill at
  that price includes them, and our resting order gets a maker fill at its price, charged at
  the maker rate (a placement whose self-trade has no rate is refused `no_fee`, as 0046 refuses
  a taker fill without one), its events after the crossing order's.
- **Depletion.** The public size a crossing order takes from a displayed level is subtracted
  from that level, as trades not shown yet are (0046), for every later crossing order, resting
  order or injection there, until the book's next event at that level replaces it, whatever
  size that event shows; a replacement snapshot replaces every level of its book. It explains
  no shrink: the real book never took it, so a later shrink there is the market's. A batch's
  items are placed in turn, so each finds what the items before it left.
- **Queue positions.** The public size a crossing order takes at a price was at the front of
  the level, so every order of ours there that it did not reach has that much less ahead of
  it, and one it reached has nothing ahead of it.
- **Injected orders** (FBC-nv2, another process's) are met only as the public size the book
  shows of them: a crossing order neither fills nor advances one (FBC-8k6h).

## Alternatives

- **Depletion as trades not shown yet** (`traded`): rejected. That credit explains the level's
  next shrink as a trade; the real book never takes SimVenue's lots, so its next shrink is
  real cancels or trades, and a repeat of the level's size, which keeps `traded`, must end the
  depletion as the ticket fixes.
- **Expiring the maker, or both**, as the action for every venue: not taken. The stood-in
  venue Paradex documents `EXPIRE_TAKER` as its default; a declared action is FBC-70pc's.
- **Refusing a placement that would meet our own order**: rejected; no venue answers that way,
  so a backtest would report a refusal the live venue never gives.
- **Modelling injected orders as another account's resting orders now**: deferred to
  FBC-8k6h; their shown size is in the displayed level, and taking it once needs the shown and
  unshown split modelled through the walk.

## Consequences

- A crossing order arriving while our own quote rests on the other side of the book is expired
  there in a Paradex backtest, as the live venue would, so a consumer that wants its exit to
  fill cancels its quotes first.
- Two exits between book updates, or a batch of them, slip through the levels as they would on
  the venue, rather than filling at the touch twice.

## What would show this was wrong

A stood-in venue whose self-trade prevention expires the maker or both by default (FBC-70pc),
or shadow fills showing a crossing order taking size a SimVenue crossing order is said to have
depleted before the book's next update at that level.
