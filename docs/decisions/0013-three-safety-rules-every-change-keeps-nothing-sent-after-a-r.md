# 0013 — Three safety rules every change keeps: nothing sent after a restart until Start, Flatten or Wind-down, pre-trade caps on every order, one quoter per market

Status: accepted, supersedes 0010
Date: 2026-10-02

## Context

0010 recorded the three rules whose breach would end the owner's trust in the system (first-day
interview, round 6). Two incidents from the Java stack stand behind them: a reconnect storm
left orphaned orders resting until a falling market filled six times the position limit, which
only a pre-trade cap would have stopped; and two quoters on one market fight each other's
orders and double the exposure. Restarts happen without the owner watching (a supervisor
restarts a crashed process), so a restart must not resume trading by itself.

0012 (the kill switch) changed how 0010's rule 1 is read, without editing 0010: after a restart
the owner's Flatten or Wind-down may also arm a market, straight into exit-only order entry,
so the owner can exit without first pressing Start and letting the quote loop add exposure.
0012 said that if the owner read rule 1 as forbidding even an owner's exit before Start, a new
record would drop that path and supersede 0012.

The owner confirmed 0012's reading on 2026-10-02: after a restart nothing is sent until the
owner presses Start, Flatten or Wind-down; Flatten and Wind-down send only reduce-only exit
orders that never cross zero and stay under every cap; quoting after a restart still requires
Start. 0012's fallback (drop the exit path and supersede 0012) is therefore not taken.

The review of pull request #3 (FBC-52u) noted that 0010's Status and its index entry still read
"no trading after a restart until Start — accepted", with no pointer to 0012's reading, so a
reader of 0010 alone misses it. Records are never edited in place, so this record supersedes
0010 and states the three rules as they now stand, in one place (FBC-xf5).

## Decision

Every change keeps these three rules:

1. **Nothing is sent after any restart until the owner presses Start, Flatten or Wind-down,
   and quoting needs Start.** In this library a new runtime starts with order entry disarmed,
   and only an explicit consumer call made on the owner's action arms a market: Start, which
   arms it into quoting, or Flatten or Wind-down, which arm it straight into exit-only order
   entry and never into quoting. Before the owner presses Start, the only orders built after a
   restart are reduce-only exit orders as 0012's Exit state defines them: on the side that
   reduces the position, sized so that the position plus every resting order on that side
   never crosses zero, and under every check of rule 2. Quoting after a restart takes Start.
   The market states, the arming calls and their leases are 0012's; follow that record. A
   reconnect or a resync never re-places or resends an order by itself and never changes the
   armed state: open orders and positions are re-read from the venue snapshot (design §9).
   Order entry is disarmed only by a process start or by an explicit disarm call from the
   consumer.
2. **Pre-trade caps on every order.** The resting-order cap, the inventory cap and the
   per-market kill switch are checked before every order command is sent: every place, every
   amend or replace, and every item of a batch, reducing orders included. The checks run in
   the one path every order command takes (`fbc-oms`, invariant I6 in 0005, design §4.9,
   §4.10), and no code path bypasses them. I6's formula already admits an order that genuinely
   reduces the position, so no order is classified as reducing in order to skip a check. While
   a market's kill switch is on, no place or amend for that market is built. Cancels are the
   only exemption: they are never blocked by a cap or by the kill switch, and every cancel
   still obeys 0005's I4 and I7. What the kill switch blocks, what lifting it does, and which
   rules a cancel still obeys are 0012's. The cap values come from the consumer's
   configuration (0009).
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

Rules 2 and 3 are 0010's, unchanged. Rule 1 is 0010's rule 1 as 0012's "How this reads 0010's
rule 1" reads it, now confirmed by the owner. 0012 stays accepted and is not edited; where it
cites 0010 (rule 1, the armed state, the leases, Start), it cites the same rules as this record
states them.

## Alternatives

- Resume trading after a restart if it was on before: rejected by the owner (0010). A crash
  loop would then trade unattended.
- Keep rule 1 literal, so only Start arms a market after a restart (0012's fallback): not
  taken. The owner confirmed 0012's reading; under the literal rule the owner could exit after
  a restart only by pressing Start first, which lets the quote loop add exposure before the
  exit.
- Leave 0010 accepted and let 0012 carry the reading of rule 1: rejected. A reader of 0010 or
  of the index alone misses the reading, which is what the PR #3 review found.
- Edit 0010 in place: rejected. Records are never edited in place.
- Supersede 0012 as well and fold the kill switch into this record: rejected. 0012's decision
  is unchanged, and restating it here would leave two copies to drift apart.
- Reactive cancels instead of pre-trade caps: rejected (0010). The orphan incident showed that
  a reactive cancel arrives after the fill.
- Caps as an optional layer the consumer may wrap around the gateway: rejected (0010).
  Optional means some path skips it.

## Consequences

- The order gateway has an armed state that starts disarmed. Tests prove a fresh runtime sends
  no order until the owner's Start, Flatten or Wind-down; that before Start only exit orders
  that reduce the position without crossing zero, under every cap, are built; that only Start
  reaches quoting; and that a reconnect or resync neither sends an order nor changes the armed
  state. 0012's Consequences name the state-machine tests.
- Every order-building path goes through the pre-trade check. Tests assert that no place or
  amend exceeding the resting cap or the inventory cap is built, that none is built while the
  kill switch is on, reducing orders included, and that a cancel is always built within 0005's
  guards. Unknown and pending orders count as resting.
- Client-id minting needs a held namespace lease and arming needs the market lease (and the
  account lease where the nonce is per account), so tests acquire them too.
- 0010's open question (may a reducing order be sent while the kill switch is on) is answered
  by 0012: no.
- 0010 is superseded. Documents that cite the safety rules (AGENTS.md, the product backlog)
  cite this record, and 0012 for the kill switch. This record changes no code.

## What would show this was wrong

- Any order sent after a restart before the owner pressed Start, Flatten or Wind-down, or any
  order other than a reduce-only exit order (0012's Exit) sent after a restart before the owner
  pressed Start.
- The owner later deciding that rule 1 must forbid any order after a restart before Start, an
  owner's Flatten or Wind-down included.
- Any order that exceeded the resting cap, the inventory cap, or was sent while the kill switch
  was on.
- Two processes quoting the same market on the same account at the same time.
