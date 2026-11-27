# 0158 — A place or amend that could add to the position is refused on a market while an order not ours is shown resting there, until its end or a trustworthy resync past the venue's lag bound shows it gone, amending 0065, 0111, 0154 and 0157, augmenting 0108

Status: accepted
Date: 2026-11-27

## Context

- **The residual (FBC-7793).** 0154 refuses to seed a market while a trustworthy snapshot shows
  an open order with no client id under a venue id that no order the registry holds had. Two cases
  remain, and neither cap counts either of them (0005's I6, 0052):
  - such an order shown on a market seeded before (0154 item 5);
  - on any market, an open order under another namespace's client id or a non-canonical one
    (0154's Consequences).
  Both only enter the view of orders not ours. The view shapes only an instrument cancel-all
  (0005's I7). With such a bid resting and the position flat, a bid of the whole inventory cap
  is admitted.
- **Which kinds are not ours.** A client id is ours, another namespace's, or non-canonical, or
  the order has none (0004).
  - Another namespace's id is minted only by another engine of this library on the account.
  - A non-canonical id is anything else: the Java brokers, another API key, a venue's UI or a
    venue's own generated id.
- **Venue UI orders.**
  - Paradex's decoders read a client id as none only when it is empty: a zero-length string on
    the private stream, and a missing, null or empty one over REST. An absent field on the
    private stream refuses the frame. Any other value is non-canonical unless this library
    minted it.
  - Paradex's public pages make client_id optional and client-assigned, and its Python SDK sends
    an empty one by default. No page says what its web UI sends, and no capture in this
    repository holds an order placed outside this library.
  - So whether a Paradex UI order has no client id or a non-canonical one is not established.
  - QFEX's ACK examples show an empty client id for an order that sent none (0132), which
    decodes as no client id. What QFEX shows for an order placed in its UI is not established.
  - A rule written for "no client id" alone would miss a venue UI order sent with a non-empty
    client id.
- **The view as built.**
  - An entry leaves only when an order event ends it under its venue id. A later snapshot does
    not clear it, because nothing orders a sighting against the venue's read (FBC-6m4q).
  - A fill opens an entry, even for an order that never rested or had already ended.
  - An order of ours seen before its venue id was learnt stays in the view for good.
  - A refusal keyed on that view would stop markets for the life of the process, the outcome
    0154's Alternatives rejected.

## Decision

1. **The kinds.** An order is *shown resting* on a market when it carries one of the client ids
   below and either an order event shows it open or amended (in place, or under a new venue id,
   which it then rests under), or a resync's snapshot, from any source, lists it open. Its venue
   id must be one that no order the registry holds had (a forgotten order of ours included, 0157
   item 6), or one of an order the registry holds as ended Lost or never sent, which counts on
   neither cap. The client ids are:
   - another namespace's client id;
   - a non-canonical client id;
   - no client id.
   An order under the venue id of any other order the registry holds is that order, whatever
   client id the event or row carries, and is not shown resting. An order seen this way with no
   venue id is shown resting on its market under no id. A fill shows that an order traded, not
   that it rests, so it shows nothing resting. The refusal reads its own set of these orders,
   apart from the view of orders not ours, which it leaves as it is.
2. **The refusal.** While an order is shown resting on a market and nothing in item 4 has
   shown it gone, the one pre-trade path refuses every place, batch item and amend on that
   market, except one on the side that reduces a known, non-zero position that either keeps
   that side's total within the position's size or carries the venue's reduce-only flag. For a
   batch judged again at authorization, the flag counts only when every order of the batch on
   that side carries it, as 0112 rules. This is 0108 item 2's test, except that an order
   carrying the flag passes on that side whatever its size, since the venue keeps it from
   crossing zero (0112's reason; the caps then judge it as 0112 says). It is judged in the same
   places:
   - at build, after the market's state and an orphan cooldown and before Exit's admission and
     the caps;
   - in the planner's previews;
   - again at authorization.
   While the refusal applies, such a build watches the market's position (0066, 0108). When the
   consumer moves an order of ours that the refusal would refuse as it rests, judged when the
   pass decides, the planner cancels it and, once it has ended, plans its new place, judged as
   above. Its amend would be refused too, since until it is acknowledged an amend counts at least
   what the order does. The planner does this on a venue that amends as well as on one that does
   not, and with an orphan cooldown on the market as well. Any other amend the refusal refuses is
   reported, and the order rests as it was. The refusal is not a market state and moves none
   (0012), so it lifts by itself.
3. **Never a cancel, and never the owner's Exit.**
   - No cancel, cancel-many, cancel everything, orphan cancel or tombstone reads the refusal.
   - Every order Exit admits passes it, so the owner's Flatten and Wind-down reduce as before
     (0012, 0063).
   - A crossing intent passes it. That covers one with the flag (0110), and one without it on a
     venue that has none, sized against every order of ours on its side (0156, once FBC-a4wm
     builds it). The planner never batches a crossing intent, so no batch that item 2 refuses
     carries one. While the refusal applies, it also sends alone any place carrying the venue's
     flag. A batch built before a sighting can still hold such a place; item 2 then refuses the
     whole batch, at authorization or at submit (item 7), and the planner plans its places
     again. An emulated reduce-only order carries no flag. It is refused when it and our other
     orders on its side, ordinary quotes included, would pass the position's size.
   - The registry never cancels the order not ours (0005's I4, 0154 item 6).
4. **What shows it gone.**
   - An order event that ends it under its venue id, whatever order the event is routed to:
     filled, cancelled, rejected or expired. An amend under a new venue id moves it to the new
     id.
   - A trustworthy resync whose snapshot does not list its venue id open, under any client id
     or none, requested at or after the sighting's stamp plus the venue's declared snapshot lag
     bound (0109). This also lifts an order shown under no id, since a snapshot would list it
     under its venue id if it still rested.
   - The registry coming to hold its venue id as an order of ours not ended Lost or never sent,
     learnt from an outcome, an order event or a fill, or registered from a resync.
   - An untrustworthy resync lifts nothing.
5. **The stamp.** fbc-oms reads no clock, and an order event carries no arrival time. A
   sighting is therefore stamped with the next of two instants the consumer reports after it:
   an order-entry epoch's end, or a retention pass's `now` (0157 item 3). It is never stamped
   earlier than the latest end already reported. The consumer reports each instant on the
   shard's clock, no earlier than the arrival of every order event and snapshot it has applied
   to the registry before the call, from any session on the account. fbc-live's link does this
   for an epoch's end on its one session. No resync lifts a sighting before it is stamped. Each
   new sighting of the same order is unstamped again.
6. **Ended stays ended.**
   - When an order event ends an order under a venue id that item 1 counts, the registry keeps
     that venue id as ended, whatever client id the event carries, stamped as item 5 says.
   - Each venue id of an order of ours not ended never sent that a retention pass forgets is
     kept as ended, stamped with the pass's `now`.
   - A later order event showing an order kept as ended open shows nothing resting. The same
     holds for any resync row listing it open, unless that row comes from a trustworthy
     snapshot requested at or after the end's stamp plus the lag bound. 0157 item 10 reads that
     bound as how long a trustworthy snapshot may still show open an order its stream ended.
   - A retention pass forgets an ended entry once its stamp is at least 0157 item 3's floor
     before `now`. Forgetting one early can only make it rest again.
7. **Earlier authorizations.** When a market goes from no order shown resting to one, a place,
   batch or amend authorized before is refused at submit if item 2 would have refused it when
   it was authorized, and the planner plans it again. One that item 2 admits beside such an
   order is not refused for the sighting, and that includes every order Exit admits and a
   crossing intent. It is still refused at submit for a change of the market's state, or, if it
   was authorized in Quoting, for a trip to Cancel-only (0060, 0139). It is also refused once a
   fill or a resync has moved the position: since its build, if it was built in Exit, under an
   orphan cooldown or under the refusal, and otherwise since its authorization (0066, 0108,
   0153). The market's state generation does not advance.
8. **Reported.**
   - The registry names the orders not ours shown resting on a market, by venue id, and says
     whether one is shown there under no id, so the owner can cancel them by hand.
   - Each resync names those it showed gone.
   - The planner reports a refused place or amend as it reports any other.

## What it amends and augments

- **0065.** Items 4 and 8 change: while an order not ours is shown resting on the market, an
  order of ours that item 2 would refuse as it rests is changed by a cancel and, once it has
  ended, a place, on a venue that amends as well. In Exit, an order on the reducing side placed
  before Exit that item 2 would refuse as it rests (one without the venue's flag that, with our
  other orders on its side, passes the position's size) is cancelled in this way when the
  consumer moves it, and not left as it is. The planner chooses this when the pass decides,
  before it builds anything, so item 6 still holds: an amend refused at build is never followed
  by a cancel.
- **0111.** Item 4 gains two kinds of place that are never batched. One is a crossing intent,
  on every venue, whether or not an order not ours is shown resting. The other, while the
  refusal applies, is any place carrying the venue's flag; a batch built before a sighting can
  still hold one, and item 2 then refuses the whole batch.
- **0154.** Item 5 changes: on a seeded market such an order now holds places and amends as
  above. The leftover its Consequences name, an open order under another namespace's or a
  non-canonical client id on any market, is covered by the same rule. So is item 4's orphan
  when the seed showed it open under such a client id, until it is registered as ours or shown
  gone; one the seed showed ended is unchanged. Items 1 to 4, the seed refusal included, and
  item 6 are unchanged. 0154's first Consequence changes: a market a later trustworthy resync
  seeds under item 3 still builds nothing that adds to its position until item 4 above shows
  the order gone.
- **0157.** Item 5 changes, taking item 11's residue in part: the venue ids of a forgotten
  order not ended never sent are kept as ended for one more floor, and nothing else of the
  order is kept. Item 6's update naming a forgotten order's venue id alone, and its snapshot
  row with no client id showing that order open, now hold places as item 6 above allows. Where
  the consumer runs no retention pass, item 6's ended entries are kept for the registry's life,
  as the other sets 0157 item 11 leaves growing are.
- **0108.** Its suppression's test gains a second cause, an order not ours shown resting.
  Under that cause, an order carrying the venue's reduce-only flag on the side that reduces a
  known, non-zero position also passes (item 2). Under an orphan cooldown, 0108's test is
  unchanged and has no flag exception. The two causes are reported apart, an orphan cooldown
  first. Unlike an orphan, a sighting advances no state generation (item 7).
- **0052 is not amended.** Neither cap counts anything new. The refusal sits beside them in the
  one path (0013 rule 2).

## What the owner decides

Approving this text (0149) approves the record, including seven choices:

- **The kinds (item 1):** all three, not no client id alone (Alternatives).
- **The shape (item 2):** 0108's test, with an exception for an order carrying the venue's
  reduce-only flag, not 0139's refusal (Alternatives).
- **No fill hold (item 1):** a fill of an order not ours holds nothing (Alternatives).
- **Our adding orders left (Consequences):** the refusal cancels none of them.
- **A moved order pulled (item 2):** the planner cancels an order the consumer moves that the
  refusal would refuse as it rests, and does not leave it at its old price and size. An order
  the refusal admits as it rests keeps its amend, which is built or reported refused.
- **A forgotten order's venue ids kept as ended (item 6):** for one more floor, unless it
  ended never sent, changing 0157 item 5 and taking its item 11's residue in part.
- **The Paradex UI finding deferred (Consequences):** FBC-7793 closes with the finding still
  open, and a separate owner-run capture ticket settles it.

## Alternatives

- **Count the order on the caps:** not taken. 0154 rejected it: it trusts a venue's quantities
  for orders we never placed, and the headroom their fills take is never recovered.
- **Refuse every place and amend in Quoting, as 0139 does:** not taken. It drops the crossing
  intent, the disaster stop. Under this rule, an order we build adds to the account's worst
  case only beside orders not ours on the side that reduces the position, and then never
  carries it past zero by more than their total size. An order carrying the flag never adds,
  but it can leave our other orders on its side room to pass zero by up to their own size, as
  in Quoting without this rule (0110).
- **Advance the market's state generation on a sighting, as 0108 does for an orphan:** not
  taken. It voids every authorization on the market, the owner's Exit orders and the crossing
  intent included. An order not ours that is cancelled and placed again on each requote would
  then void our reducing orders on every requote.
- **Hold the market after a fill of an order not ours until a resync agrees:** not taken. 0055
  never re-seeds, so a market another actor trades would stop for the life of the process. The
  next resync reports the fill as a Desync (0055).
- **No client id only:** rejected. It misses a Paradex UI order unless the UI sends an empty
  id, and it misses any venue's order under a non-canonical id.
- **Move the market to Cancel-only:** rejected. Only the owner leaves Cancel-only (0012, 0139).

## Consequences

- On a market where an order not ours is shown resting (item 1), the consumer quotes nothing
  that adds to its position until that order ends, or until a trustworthy resync requested at
  least the lag bound after the sighting's stamp shows it gone. A session asks for one resync
  per epoch and fbc-live starts none, so on a steady connection only the order's end, or its
  venue id coming to be held as an order of ours (item 4), lifts the hold.
  - An order whose end no order event shows (one that ends while the stream is down, say)
    holds its market until such a resync. On Paradex, whose bound is 6 s, that is the first
    resync requested at least 6 s after the sighting's stamp. The stamp is the `now` of the
    first retention pass after the sighting or, if no pass ran before the drop, the last
    envelope before the drop. If the next epoch's resync is requested sooner, the hold stays
    for that whole epoch.
  - On a venue with an untrustworthy snapshot (QFEX today), only an end event, or the venue id
    coming to be held as an order of ours (item 4), lifts the hold. Three kinds of sighting of
    an order that is not ours therefore hold the market until the process restarts:
    - one with no venue id;
    - one whose end no order event shows: it ended while the stream was down, the venue streams
      no event of it, or the adapter reads its end as open, as QFEX's adapter reads an
      `IOC_PARTIALLY_FILLED` with a remainder until FBC-jdj7;
    - one that an untrustworthy row or a late event shows again after its ended entry was
      forgotten.
    The owner's Exit still reduces.
- An engine of this library on a market where another engine's order rests builds nothing there
  that adds to its position. This backs 0013 rule 3's leases; it does not replace them. It does
  not stop two engines that place before either sees the other, and each engine still builds
  orders that item 2 admits.
- Residuals:
  - The refusal cancels none of our resting orders that add. These are the orders on the adding
    side, and those on the reducing side past the position's size or past it once an order
    carrying the flag is admitted beside them. They come down only by a cancel: the planner's,
    of a level the consumer moves (item 2) or no longer wants, or the kill switch's. The size of
    the order not ours still breaches 0005's I6 until it ends.
  - Exit's never-cross-zero rule counts our orders alone, and a fill of an order not ours moves
    the account but not the inventory. It is reported as a Desync at the next resync (0055).
  - Orders that never reach the view are not seen:
    - a stop or take-profit order on Paradex, or an open order there on a market its spec table
      does not hold, either of which makes the decoder refuse the whole resync, so while one
      rests no resync lifts a hold there and only an end event, or its venue id coming to be
      held as an order of ours (item 4), does;
    - on QFEX, a TWAP, a stop kind, or an order on a symbol the spec table does not hold.
  - An order not ours that the venue shows only by its fills, with no order event, holds
    nothing until a resync's snapshot lists it open, which on a steady connection is the next
    epoch's resync.
  - On a venue whose events need not echo our client id, an order of ours shown with no client
    id before its venue id is learnt holds its market. It is released when an order event, a
    fill or a resync row names it under our client id and that venue id, or when an event ends
    it under that id. On QFEX, one that ends with no order event showing its end holds its
    market until the process restarts, as the sightings of orders not ours above do. QFEX's
    codec settles no place, so no outcome teaches the id there, and quoting on QFEX needs the
    exact echo FBC-9sk2 measures.
- The Paradex UI finding stays open until an owner-run capture of one testnet UI order settles
  it. This rule holds under either reading.
- Proof: `crates/fbc-oms/tests/not_ours_resting.rs`, `crates/fbc-oms/tests/resync.rs`
  (0154's residual test now refuses) and `crates/fbc-oms/tests/retention.rs`.

## What would show this was wrong

- A place or amend that item 2 refuses, built or authorized on a market while an order not
  ours that an order event or a snapshot row showed open rests there, before anything in item
  4 shows it gone.
- One authorized before the sighting that item 2 would have refused, passing its check at
  submit after the sighting.
- A cancel refused by this rule.
- An order Exit admits refused by this rule at build, or at authorization or submit while the
  position is as it was at its build.
- A place or amend refused by this rule on a market after item 4 has shown every order not
  ours shown resting there gone.
