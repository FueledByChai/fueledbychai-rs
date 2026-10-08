# 0080 — An order resting from an earlier epoch holds its market until the venue shows it ended, unless the venue declares that an arm covers open orders, augmenting 0058

Status: accepted
Date: 2026-10-08

## Context

Reviewer B's RB-olg-5 on PR #81 (FBC-olg), filed as FBC-nvxn. Paradex's
`order.cancel_on_disconnect` page says the setting is "scoped to the connection and does not
persist across reconnects" and cancels orders "placed (or already tracked)" when the socket
disconnects "unexpectedly"; no page says whether an arm on a new connection covers the orders
that were already open when it was enabled. 0058 arms on every epoch before the epoch places,
which protects the orders placed on that epoch. An order that survives an expected close of
epoch N (a server close, or our own close on a stall reconnect), or that an earlier process
left, then rests on epoch N+1 under an arm that may not cover it, against the owner's rule that
no venue without cancel-on-disconnect may place orders (2026-10-04). 0005 (I4: we cancel only
orders of ours), 0013 rule 2 (every order-affecting command, cancels included, goes through an
fbc-oms authorization) and 0014 (the runtime hands every event to the consumer in ingest order)
bound the answer.

## Decision

- **A caps field.** `CancelOnDisconnect::PerConnection` gains `covers_open_orders`: an accepted
  arm also covers the orders already open on the account when it is accepted. A venue declares
  `true` only where its documents say so, or a testnet record shows it. Paradex declares
  `false` (no page says so; FBC-8xr's testnet run checks it, and a new record flips it). The
  conformance toy declares `false`.
- **Which orders are unprotected.** On an epoch on which the session sends an arm (0058: every
  epoch where the protection is re-armed on each connection; where it outlives a connection,
  each epoch until one arm is accepted), and whose venue declares `covers_open_orders: false`,
  every order the epoch's resync shows `Open` whose client id is ours (`CidMatch::Ours`) is
  unprotected. 0058 holds every place until the resync has ended, so each of them was placed on
  an earlier epoch, or by an earlier process. An order another namespace or system placed (I4),
  or one shown without a client id, is not ours to protect and is not counted. A later epoch
  keeps every order an earlier one found unprotected until an event or a query answer ends it:
  no later arm covers it (where the protection outlives the connection, a later epoch sends no
  arm at all), and a later snapshot may omit an order still resting (Paradex's is
  untrustworthy, 0054). A later epoch that sends an arm adds what its own resync shows.
- **Its market is held.** Once the epoch is placing (0058: its arm accepted and its resync
  ended), a place, a batch of places with any item, or an amend on the market of an
  unprotected order is `NotSent(Disconnected)`, with no nonce reserved and nothing written,
  counted apart from 0058's refusals (`ExecCounters::unprotected_refusals`). Cancels and control
  commands are not held, and other markets place. `ExecOrders::may_place_on(inst)` and
  `ExecOrders::unprotected()` say where it stands.
- **The consumer cancels it.** The session tells the consumer the unprotected orders once per
  epoch, right after it hands over the event that lets the epoch place, through
  `ExecHandler::on_unprotected`; the consumer cancels each with a Safety cancel fbc-oms builds
  and authorizes (the session cannot authorize a command itself, 0013 rule 2).
- **What releases the market.** The order having ended under every venue id it was seen
  under. For each unprotected order the gate keeps the ids it was seen under (the snapshot's,
  each new id an amend of it was reported under, each id an order event or an order query's
  answer naming our client id showed) and those an order event or a query answer, of any
  epoch, showed filled, cancelled, rejected or expired, or replaced by an amend under a new id.
  No evidence removes an id, so the order in which evidence arrives does not matter: an end
  reported late under an id an amend superseded, a snapshot lagging what was heard, a query
  answer showing a predecessor, and amends reported out of order each leave the market held
  while some id the order was seen under is not shown ended (Reviewer B RB-nvxn-4 on PR #127,
  after seven rounds in which ordering rules each exposed a sibling case). Evidence naming only
  our client id adds the id it shows and ends nothing, since on a venue whose amends issue new
  venue ids it may report an order an amend superseded; where the venue's amends keep the
  venue id (Paradex), or it amends nothing, our client id names one venue order and an end
  naming it ends the order. What order events and query answers show of venue ids is kept
  while a resync runs, and after it for the markets an order is held on, so evidence heard
  before the snapshot, or before the amend that links an id to the order, is applied when
  they arrive (a resync over HTTP while the stream runs); it is dropped once nothing is held
  after the resync. A later epoch's snapshot of a held order is matched to it by our client id
  and adds its id. Each order is told to the consumer under the latest id it was seen under not
  shown ended. Nothing else releases a market: a cancel the venue refuses (an order it no
  longer knows, or one already terminal) leaves it held until the consumer queries the order.
  The same holds for an order a snapshot shows resting after an event ended it while nothing
  was held (a snapshot lagging the stream): the consumer's registry holds it ended and builds
  no cancel, so the consumer queries it, and the venue's answer showing it ended releases the
  market. The runtime keeps no record of ends heard while nothing is held, which would grow
  with every order the session ever saw end. An order seen under an id the venue never reports
  ended (a lagging id on a venue whose amends issue new ids) stays held; FBC-066c (a
  trustworthy resync that leaves it out releases it) and FBC-41iu cover releasing it.

## Alternatives

- Keep the orders and trust the new arm: rejected. No Paradex page says an arm covers orders
  already open, and 0054 declares every undocumented value at the one under which the stack
  does less.
- Have the session cancel the orders itself: rejected. Every cancel needs an fbc-oms
  authorization (0013 rule 2), which only the consumer's registry issues.
- Refuse every place on the epoch until every unprotected order is gone: rejected. The risk is
  per market, and a market the account rests nothing on is protected by the epoch's arm.
- Release the market when the cancel is accepted: rejected. An accepted cancel is not the order
  ended (a two-phase venue may still refuse it, and a fill may race it); the order's own end is.
- A new `NotSentReason`: not taken, for 0058's reason. `Disconnected` tells the consumer that
  the stream cannot take it now and that it is never resent; the counters tell the two apart.

## Consequences

- Proven in fbc-runtime's `tests/exec_unprotected.rs`: an order placed on epoch 0 and resting
  when the connection drops is cancelled on epoch 1 after its arm and resync and before any
  place or amend on its market, and a place and an amend submitted meanwhile are held until an
  order event shows it cancelled; with `covers_open_orders: true` the same order is kept and a
  place goes out at once. The gate's own tests (`exec_gate.rs`) cover the rest of the rules.
- A consumer implements `on_unprotected`; one that does not leaves those markets held for the
  epoch, which is safe but quotes nothing there.
- An order of ours placed on an earlier epoch that the resync does not show (a snapshot
  lagging an order whose outcome was `Unknown`) and that an order event shows resting later is
  not counted here; FBC-etd0 covers it.
- FBC-8xr's testnet run checks whether Paradex's arm on a new connection covers the orders
  already open; a confirming record flips Paradex's `covers_open_orders` to `true`.

## What would show this was wrong

- A place or amend written on a market after its epoch's resync showed an order of ours
  resting there, before an event showed that order ended, on a venue declaring
  `covers_open_orders: false`.
- A market held for good on an epoch because the venue never reports the end of an order it
  refused to cancel and answers no query for it.
- Testnet evidence that Paradex's arm covers the orders already open, which would make the
  cancels needless there.
