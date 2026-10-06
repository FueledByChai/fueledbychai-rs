# 0054 — Paradex's order entry declares ExecCaps with every value cited, its socket negotiates SBE schema 1:2, an amend is final on its order event, and undocumented values are declared conservatively

Status: accepted
Date: 2026-10-05

## Context

0003 makes capabilities data with every field mandatory, each value a claim someone can check
against a venue document or a recorded fixture; 0015 folds order and fill capabilities into one
`exec` block; 0032 adds the references amends and batch cancels can name. Paradex has declared
`exec: None` since its market data landed (0022). FBC-olg (BT-402) declares its `ExecCaps`
before the order-entry codec (FBC-xvf) and the factory's wiring (FBC-xzp) exist, and the
consumer's SimVenue standing in for Paradex (FBC-2zxk) reads the same values, so a backtest and
a live session trade under one declaration. The owner's default of 2026-10-04 makes Paradex
order entry WebSocket-only for M2: every place, amend and cancel is one of the socket's
JSON-RPC methods, with no REST fallback.

The sources: the method pages on docs.paradex.trade, the SBE schema `paradex_1_0.xml` at the
paradex-py commit 0022 cites (`b8248fb747e278d2167ac2f056b339a287d5ef30`), which describes
schema 1:2, and FueledByChaiTrading's Paradex broker (the Java library), which ran WebSocket
order entry (`order.create`, `order.modify`, `order.cancel`, `order.cancel_batch`) but never ran
cancel-on-disconnect live. Several values the capability model needs are stated by none of
them.

Two sources disagree. The "Binary Encoding (SBE)" page says "private channel payloads remain
JSON" and offers `sbeSchemaVersion` 0 or 1, yet lists `OrderEvent` (template 20, `orders`) and
`FillEvent` (template 21, `fills`) among its SBE messages; the schema says 1:2 is "Negotiable on
nightly, testnet and prod"; and the Java library's transcoder decodes templates 20 and 21 off
the 1:1 socket.

## Decision

Paradex's `ExecCaps` are `fbc_venue_paradex::exec::exec_caps()`, each value cited where it is
declared (`crates/venues/fbc-venue-paradex/src/exec/caps.rs`). `factory::caps_with_order_entry()`
is the factory's declaration once order entry is wired (FBC-xzp): `caps()`'s market data and
matching with that `exec` block, the per-account order limits, and the per-IP limit counting
the order methods. `caps()` stays market data only until then.

### The order methods, each page cited

| Method | Page | What the caps take from it |
| --- | --- | --- |
| `order.create` | https://docs.paradex.trade/ws/web-socket-channels/order-create/order-create | kinds LIMIT and MARKET; instructions GTC, IOC, POST_ONLY and RPI; flag REDUCE_ONLY; `client_id` up to 64 characters (written as a UUID, 0004); rate limits shared with REST |
| `order.create_batch` | https://docs.paradex.trade/ws/web-socket-channels/order-create-batch/order-create-batch | `batch_place`; the rate limit "consumed once for the entire batch" |
| `order.modify` | https://docs.paradex.trade/ws/web-socket-channels/order-modify/order-modify | amend by `id` only (FBC-6 signs it); price and size; size the "New (or unchanged) size", so `AmendQty::TotalIncludingFilled`; no flags |
| `order.cancel` | https://docs.paradex.trade/ws/web-socket-channels/order-cancel/order-cancel | `cancel_refs` venue id and client id ("Either id or (client_id + market)"); unsigned |
| `order.cancel_batch` | https://docs.paradex.trade/ws/web-socket-channels/order-cancel-batch/order-cancel-batch | `batch_cancel` by `order_ids` only, so its refs are the venue id alone (0032) |
| `order.cancel_all` | https://docs.paradex.trade/ws/web-socket-channels/order-cancel-all/order-cancel-all | cancel-all per account and per market, both native; fbc-oms never builds the account one |
| `order.cancel_on_disconnect` | https://docs.paradex.trade/ws/web-socket-channels/order-cancel-on-disconnect/order-cancel-on-disconnect | `CancelOnDisconnect::PerConnection { rearm_on_reconnect: true }`: "scoped to the connection and does not persist across reconnects" |

The private channels `orders.{market}` and `fills.{market}` (their pages and the schema's
`OrderEvent` and `FillEvent`) give the rest: order events ordered by their `seq`
(`OrderingKey::VenueSeq`), echoing our client id and the order's instruction and flags; native
fills with liquidity, realized P&L, realized funding, a fill id, a fee "positive = paid,
negative = rebate" (`VenueFeeSign::PositiveIsCost`) and the fee's asset. Orders carry no nonce
(`NonceScope::None`: they are signed over a `signature_timestamp`). Flag conflicts follow from
one `instruction` field and the "Order Instructions" and "Retail Price Improvement" pages:
(PostOnly, Ioc), (Ioc, Rpi) and (Rpi, ReduceOnly). An RPI order is post-only by the venue's
rule, so the codec writes RPI whether or not the order also asks for post-only, as the Java
library does. Queries name the client id ("Get order by client id"). The order rate limits are
"`POST, DELETE, PUT /orders` | 800 req/s OR 17250 req/m | Account", both windows, counting
place, amend, cancel and cancel-all; the account limit charges a batch once.

### The order socket negotiates SBE schema 1:2

The order-entry socket asks for `?sbeSchemaId=1&sbeSchemaVersion=2`
(`exec::ORDER_SBE_SCHEMA_VERSION`). `OrderEvent`'s flattened `request_info` (`requestStatus`,
`requestType`, `requestId`, `requestMessage`) and `FillEvent`'s `feeCurrency` exist only at
`sinceVersion="2"`, and without them an amend could not be confirmed nor a fee's asset read.
Market data stays on 1:1 (`md::sbe::SCHEMA_VERSION`), as 0022 built it. The decoder reads every
block length from the frame header and takes appended var-data that is absent as missing, as
the schema requires of a 1:2 decoder.

### An amend is final on its order event

`order.modify`'s reply is not the amend's confirmation: `AmendAck::ReplacedEvent`. The amend is
final on the `OrderEvent` whose `requestType` is MODIFY_ORDER and `requestStatus` SUCCESS, which
carries the amended price and size. An event reporting REJECTED for MODIFY_ORDER leaves the
original order as that same event shows it (`reject_keeps_original: true`); PENDING and
PROCESSED confirm nothing. Nothing in the schema or the pages makes the reply final, so it is
not.

### Values no Paradex document states

Each is declared at the value under which the OMS and the planner do less, and is to be
confirmed or corrected on the owner's testnet run (FBC-8xr); a correction is a new record.

- `when_partially_filled`: whether `order.modify` accepts a partly filled order. Declared
  `false`: a partly filled order is cancelled and replaced, never amended. It also makes the
  next item moot for every amend sent.
- `qty_semantics`: `order.modify`'s size is the "New (or unchanged) size", and `OrderEvent`'s
  `size` is the order's size beside `sizeOpen`, the remaining part; no page says whether the
  size of a partly filled order's modify includes the filled part. Declared
  `AmendQty::TotalIncludingFilled`; with `when_partially_filled: false`, no amend depends on it.
- `reject_keeps_original`: that a REJECTED modify leaves the order resting. Declared `true`,
  read from the event's own status, since the event that reports the rejection is an update of
  the order itself; the codec reports the order's state as that event gives it.
- `cancel_before_ack`: whether `order.cancel` by client id and market finds an order Paradex
  has not yet acknowledged. Declared `false`: a cancel by client id waits for the
  acknowledgement.
- `replays_fills_on_reconnect`: whether `fills.{market}` sends past fills after a reconnect.
  Declared `false`: nothing waits for a replay; a fill missed across a reconnect comes from the
  resync, and a replayed one is a duplicate its fill id drops (0005's I3).
- `batch_place`: `order.create_batch` takes "up to the configured maximum number of orders"
  without the number; the REST batch's is "between 1-10". Declared 10.
- `batch_cancel`: `order.cancel_batch` states no maximum. Declared 10, as for placement.
- `cancel_on_disconnect`: orders are cancelled "if the WebSocket disconnects unexpectedly";
  which closes count (one this library starts, a server close, a lost TCP connection) is not
  stated, nor whether orders placed before it is enabled are covered ("placed (or already
  tracked)"). Declared per connection and re-armed on each: the runtime never assumes a close
  cancelled anything and resyncs on the next connection, and enables it before placing
  (FBC-w19).
- `risk_reject_window`: "Orders are queued for risk checking independently", so acceptance is
  provisional (`AckModel::TwoPhase`), but the time the risk check takes is not stated. Declared
  five seconds.
- `snapshot_source`: whether the open-orders snapshot is consistent with the order stream.
  Declared `SnapshotSource::Untrustworthy`: an order's absence from it never ends the order.
- `keeps_priority`: whether a modify keeps queue priority. `None`, as design §4.5 has it, until
  calibration measures it.
- `LimitScope::Ip`: whether the 1500 req/m per-IP limit on private requests counts the
  WebSocket order methods (design §13.3's check). Declared counted: one per-IP bucket of
  1500 req/m holds REST, queries and order methods together.
- `RateCharge::weight` of a batch: "consumed once for the entire batch" is stated for the
  account limit only, and no page says whether the per-IP limit counts a batch once or per
  item. A charge has one weight for every limit counting it, so the codec charges a batch its
  item count: the per-IP bucket, the binding one (1500 req/m against 800 req/s), then never
  admits more than the venue's budget if Paradex counts each item, at the cost of overcounting
  the account limit, which ten-item batches cannot approach.
- The encoding of private channel payloads, on which the "Binary Encoding (SBE)" page and the
  schema disagree. Declared SBE (`OrderEvent`, `FillEvent`), as the schema and the Java library
  have it; a JSON payload on a private channel is not guessed at by the decoder.

Not modelled: Paradex's 100 open orders per market ("Open Orders per Account"), far above the
consumer's caps; the STOP and TPSL order types; MMP; and the `on_behalf_of_account` isolated
margin accounts.

## Alternatives

- Order entry over REST, or REST as a fallback: rejected by the owner's default of 2026-10-04
  (WebSocket-only for M2). The REST pages are cited only where they state a fact the
  WebSocket pages leave out (the batch bound, that a modified order keeps its id, the order
  limits the WebSocket methods share).
- Negotiating 1:2 for market data too: not taken. 0022's decoder is pinned to 1:1, and 1:2's
  in-place additions change `TradeEvent` too; moving market data is its own change.
- Schema 1:1 on the order socket: rejected. An amend could then be confirmed only by the
  method's reply, which the schema's `request_info` shows is not final, and no fill could name
  its fee's asset.
- Undocumented values taken from the Java library where it ran them: not taken. It never
  amended a partly filled order on purpose, never cancelled before an acknowledgement and never
  ran cancel-on-disconnect live, so it shows how it called the venue, not what the venue does.
- A second per-IP bucket for order methods beside the one REST and queries use: rejected. Two
  buckets of 1500 would allow 3000 requests a minute from one IP where the venue may count one.

## Consequences

- FBC-xzp makes `ParadexFactory::caps` return `caps_with_order_entry()`, and its `plan_exec`
  asks for `sbeSchemaVersion=2` on the order socket; FBC-xvf's codec decodes `OrderEvent` and
  `FillEvent` at 1:2, confirms an amend on the order event that reports SUCCESS for
  MODIFY_ORDER, writes RPI for an order on the RPI channel, charges a batch its item count, and
  queries an order by client id through a path that also finds a closed order ("Get order by
  client id" returns only orders in `OPEN` status).
- A SimVenue standing in for Paradex (FBC-2zxk) refuses placements until it models a two-phase
  acknowledgement (FBC-zr1) and cancel-on-disconnect (FBC-fji), which these values trip.
- The conformance suite's `caps_truthful` holds the codec to these values once it exists.
- Each value in the list above is a question for FBC-8xr's run, and the capability values it
  contradicts are corrected by a new record.

## What would show this was wrong

- The testnet run (FBC-8xr) amending a partly filled order, cancelling by client id before the
  acknowledgement, replaying fills after a reconnect, accepting batches of more than ten,
  leaving orders resting after an unexpected disconnect, not counting WebSocket order
  methods per IP, or counting a batch once per IP: each corrects its value by a new record.
- An `order.modify` reply that later contradicts its order event, which would make the reply
  the confirmation (`AmendAck::RpcReplyOnly`), or a REJECTED modify that ends the order.
- Private channel payloads arriving as JSON on a 1:2 socket, or a 1:2 negotiation refused with
  HTTP 400 at connect, either of which reopens the schema choice.
