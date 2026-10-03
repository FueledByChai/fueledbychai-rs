# 0010 — Three safety rules every change keeps: no trading after a restart until Start, pre-trade caps on every order, one quoter per market

Status: superseded by 0013
Date: 2026-10-02

## Context

The owner named three rules whose breach would end their trust in the system, and both
repositories enforce them in code and in review (first-day interview, round 6). Two incidents
from the Java stack stand behind them: a reconnect storm left orphaned orders resting until a
falling market filled six times the position limit, which only a pre-trade cap would have
stopped; and two quoters on one market fight each other's orders and double the exposure.
Restarts happen without the owner watching (a supervisor restarts a crashed process), so a
restart must not resume trading by itself.

## Decision

Every change keeps these three rules:

1. **Trading is off after any restart until the owner presses Start.** In this library a new
   runtime starts with order entry disarmed, and only an explicit arm call from the consumer
   arms it. A reconnect or a resync never re-places or resends an order by itself and never
   changes the armed state: open orders and positions are re-read from the venue snapshot
   (design §9). Order entry is disarmed only by a process start or by an explicit disarm call
   from the consumer.
2. **Pre-trade caps on every order.** The resting-order cap, the inventory cap and the
   per-market kill switch are checked before every order command is sent: every place, every
   amend or replace, and every item of a batch, reducing orders included. The checks run in
   the one path every order command takes (`fbc-oms`, invariant I6, design §4.9, §4.10), and no
   code path bypasses them. I6's formula already admits an order that genuinely reduces the
   position, so no order is classified as reducing in order to skip a check. While a market's
   kill switch is on, no place or amend for that market is built. Cancels are the only
   exemption: they are never blocked by a cap or by the kill switch. The cap values come from
   the consumer's configuration (0009).
3. **One quoter per market and account.** Two separate mechanisms serve it:
   - The namespace lease (one per account and namespace, design §4.3) prevents client-id
     collisions: no client id is minted without a held namespace lease. It does not by itself
     stop two processes with different namespaces quoting one market.
   - Arming order entry for a market requires a held market lease, plus an account lease when
     the venue's nonce scope is per account (design §13.2). The library provides both as file
     locks in a directory the consumer supplies and refuses to arm without them. Before it
     arms, the consumer checks that no other process, Java or Rust, already quotes the market
     on the account (design §13.2 steps 2 and 3); that check is the consumer's, not this
     library's.

## Alternatives

- Resume trading after a restart if it was on before: rejected by the owner. A crash loop would
  then trade unattended.
- Reactive cancels instead of pre-trade caps: rejected. The orphan incident showed that a
  reactive cancel arrives after the fill.
- Caps as an optional layer the consumer may wrap around the gateway: rejected. Optional means
  some path skips it.

## Consequences

- The order gateway has an armed state that starts disarmed. Tests prove a fresh runtime sends
  no order until armed, and that a reconnect or resync neither sends an order nor changes the
  armed state.
- Every order-building path goes through the pre-trade check. Tests assert that no place or
  amend exceeding the resting cap or the inventory cap is built, that none is built while the
  kill switch is on, reducing orders included, and that a cancel is always built. Unknown and
  pending orders count as resting.
- Client-id minting needs a held namespace lease and arming needs the market lease (and the
  account lease where the nonce is per account), so tests acquire them too.
- Open question for the owner: whether a reducing order (a force-close or flatten) may be sent
  while a market's kill switch is on. Until the owner decides, in a new record, the kill switch
  blocks every place and amend.

## What would show this was wrong

- Any order sent after a restart before the owner pressed Start.
- Any order that exceeded the resting cap, the inventory cap, or was sent while the kill switch
  was on.
- Two processes quoting the same market on the same account at the same time.
