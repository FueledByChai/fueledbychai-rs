# 0132 — A QFEX order-entry session meets 0058 only on an arm reply a pre-production capture shows, resyncs from untrustworthy pages, opens only as a host-guarded pre-production probe, and settles a request only on a reply that names it, augmenting 0058, 0117, 0121, 0122 and 0126

Status: accepted
Date: 2026-10-30

## Context

0058 holds every place and amend on an epoch until two things are done: the venue has given a
Final answer to the session's cancel-on-disconnect arm, and the epoch's resync has ended. 0121
gives QFEX's account-wide protection its own capability, `CancelOnDisconnect::AccountWide`,
behind the consumer's explicit `AccountWideCodAuthority`. Under it the session skips 0080's
quarantine of orders resting from earlier epochs.

0117 admits QFEX order entry only once evidence exists for its signer, its outcomes, its
cancel-on-disconnect, its resync and its conformance. Until then QFEX declares `exec: None`.
0122's item 4 lets no `ExecSession` open QFEX's private plan until a later decision. FBC-lqa8
needs provider evidence that only a session that places can gather.

The owner chose, on 2026-10-29:

- to gather that evidence on QFEX's pre-production environment, with primary and subaccount
  credentials QFEX supplies;
- a v1 command scope of limit GTC, post-only (ALO) and IOC;
- to refuse native modify until a capture settles its quantity semantics.

FBC-n7kc asks four questions. QFEX's public pages, read on 2026-10-29, answer them only in
part. On 2026-10-30 the owner took the recommended option on each, in the words "Take all
four" (the ticket's notes):

- **The arm.** Capture QFEX's reply on pre-production first, and read an `ack` echoing the arm
  as its Final acceptance. Accept that arming may cancel the account's open orders, provided
  the account has one order-entry connection and nothing else trades on it.
- **The resync.** Three reads: `get_user_orders` pages, REST `GET /user/positions`, and the
  fills missed since the last fill, from `get_user_trades`. It is declared untrustworthy until
  pre-production evidence earns a record that makes it trustworthy, as 0094 did for Paradex.
- **The probe.** A host-guarded pre-production declaration with `ExecCaps` that only a
  pre-production configuration reaches. Production keeps `exec: None`, and every command still
  goes through fbc-oms.
- **Settlement.** A place settles only on an `order_response` whose symbol and
  `client_order_id` exactly match it, once captures show the echo. Cancels go by the venue's
  order id.

This record writes those choices down and fixes what they leave open. Where it goes further
than a choice, the item says so.

**The arm.** The
[cancel-on-disconnect page](https://docs.qfex.com/websocket/channels/trade/cancel_on_disconnect)
documents no reply to the arm.

- Its only example response is an `order_response` with status `CANCELLED`, one per order
  cancelled. With no open order, no reply is promised at all.
- The [trade socket's AsyncAPI](https://docs.qfex.com/websocket/trade.yaml) gives no operation
  a reply or a correlation id. It defines a generic `ack` message,
  `{"ack": {"incoming_message": {...}}}`, that echoes a request, but ties it to no command.
- The page contradicts itself on when orders are cancelled. Its note says "Enabling
  cancel-on-disconnect cancels all open orders across all symbols for the authenticated
  account". Its introduction says the engine cancels every open order on the account, not
  only this connection's, "if the connection drops unexpectedly".
- The flag is scoped to one connection: "you need to opt in again whenever you reconnect".
- The page does not say whether a graceful close cancels. It does not say whether orders
  placed after the arm through other connections are cancelled, as 0121's `AccountWide`
  assumes. It does not say whether stop orders and TWAPs are covered.
- "The authenticated account" is the subaccount that authentication's `account_id` selects, or
  the primary account when none is selected
  ([authenticate](https://docs.qfex.com/websocket/channels/trade/authenticate)). Subaccounts are
  "separate accounts when it comes to order entry"
  ([subaccounts](https://docs.qfex.com/qfex/subaccounts)).

**The resync.** No read has a documented snapshot boundary. None carries an as-of time, a
sequence number or a cursor.

- [`get_user_orders`](https://docs.qfex.com/websocket/channels/trade/get_user_orders) is the
  only documented read of all open orders; there is no REST one.
  - It pages by `limit` (default 1000) and `offset`.
  - Its answer, `all_orders_response`, holds `orders` and `twaps` and no count, total or
    has-more field.
  - It weighs 5.0 ([rate limits](https://docs.qfex.com/websocket/rate)).
  - Filled and fully cancelled orders are not included.
- The [order responses page](https://docs.qfex.com/websocket/channels/trade/order) says to
  authenticate, resubscribe and reconcile after reconnecting, since "Live updates are not a
  replay of missed events".
- [`get_order`](https://docs.qfex.com/websocket/channels/trade/get_order) needs a `symbol` and an
  `order_id`, never a client id, and returns only open orders.
- [REST `GET /user/positions`](https://docs.qfex.com/api-reference/rest/user/user-positions)
  returns every position in one answer, each with an undescribed integer `open_orders`. An
  optional `x-qfex-requested-account-id` header selects a subaccount.
- [`get_user_trades`](https://docs.qfex.com/websocket/channels/trade/get_user_trades) filters by
  an inclusive `start_ts` in seconds and pages by `limit` and `offset`. Its page shows rows
  carrying `aggressor_side` and no `side`, while the AsyncAPI gives them an undescribed `side`
  and no `aggressor_side`. Neither source says a row states our side. No page says what kind
  of time their `timestamp` is.
- [REST `GET /user/historic-orders`](https://docs.qfex.com/api-reference/rest/user/historic-orders)
  returns "filled or terminally closed" orders and can be filtered by `client_order_id`. Its
  `terminal_status` values are listed nowhere, and `FILLED` "could be partially filled"
  ([enums](https://docs.qfex.com/api-reference/enums)).
- [`GET /user/subaccounts`](https://docs.qfex.com/api-reference/subaccounts-api) "Returns the
  authenticated user's subaccount IDs excluding the master account ID". A key may be scoped to
  one account ([API keys](https://docs.qfex.com/api-reference/api-keys)).

**The environment.** The
[builder wallet registration page](https://docs.qfex.com/api-reference/builder-wallet-registration)
names a pre-production REST API at `https://api.qfex.io` and Trade WebSocket at
`wss://trade.qfex.io`. It also says to "Use the environment URLs and credentials supplied by
QFEX".

- QFEX's own [CLI README](https://github.com/QFEX-org/cli) gives the same environment's
  market-data socket as `wss://mds.qfex.io`. No page on docs.qfex.com names it.
- Production hosts are under `qfex.com`. The trading REST reference lists only
  `https://api.qfex.com`.

**The replies.** No reply carries a request id.

- A place is answered by an `order_response` with status `ACK`
  ([add order](https://docs.qfex.com/websocket/channels/trade/add_order)), "Order accepted"
  ([enums](https://docs.qfex.com/api-reference/enums)). Every `ACK` example shows an empty
  `client_order_id`, since none of those requests sent one.
- `client_order_id` is "not guaranteed unique". A cancel by it "Cancels all orders for that
  symbol that share this client order id", and the page disagrees with itself on which field
  carries it ([cancel order](https://docs.qfex.com/websocket/channels/trade/cancel_order)).
- A reject is either an `order_response` status code or an `err` frame. No page shows a
  rejected `order_response`, and no page lists which statuses are terminal.
- A crossing post-only order "is rejected" on the enums page and "is cancelled instead" on the
  [order types](https://docs.qfex.com/qfex/order-types) page.
- An `err` echoes its request "when available" ([errors](https://docs.qfex.com/websocket/errors)).
  - Its `incoming_message` is documented as an object, a string, `{}` or null.
  - Its envelope is documented both nested and flat.
- Non-ALO orders wait in a 500 ms speed bump
  ([architecture](https://docs.qfex.com/qfex/architecture)). No page says whether `ACK` comes
  before or after the hold.

**What the code already fixes.**

- Under 0126 every inbound frame that can echo a request is redacted whole. So an `ack`
  echoing the arm would reach neither the journal nor a replay.
- fbc-oms routes an order event or a fill that carries neither our client id nor a venue id it
  holds as an order not ours. Such a fill is not counted.
- No pre-production capture exists yet. Hibachi's FBC-jw1b asks the same questions of Hibachi;
  this record decides only QFEX.

## Decision

1. **What answers the arm.**
   - The session sends the arm as 0058 and 0121 say: itself, once the stream is
     authenticated, on every epoch, under the consumer's `AccountWideCodAuthority`.
   - QFEX's codec reports the stream authenticated only after two things: authentication has
     succeeded, and the `subscribed` frames have confirmed its `order_responses` and fills
     subscriptions. So, if QFEX reports what the arm cancels, the session hears it.
   - The codec encodes one `ArmCancelOnDisconnect(true)` per epoch. It refuses, unsent, any
     later arm on that epoch and every disarm. So at most one arm is in flight on a connection.
   - The arm is answered `Accepted` at `AckLevel::Final` only by an `ack` whose
     `incoming_message`, parsed as JSON, equals the arm the codec encoded on that epoch: the
     same members with the same values, order and whitespace ignored. An echo with a member
     added is not equal.
   - An `err` with such an echo is `Rejected`, unless its code is `RateLimited`. A
     `RateLimited` refusal holds the budget and settles nothing (0126), so the arm then fails
     at its deadline.
   - Nothing else answers the arm. A `CANCELLED` `order_response` is an order update, never
     the arm's answer.
   - The codec decodes this answer only once a pre-production capture holding it lands as a
     fixture under `fixtures/qfex/exec-recorded/`. Until then no frame answers the arm: every
     arm is unanswered at its deadline, every epoch ends (0058), and QFEX places nothing.
   - If the capture shows another reply shape, a new record decides whether that shape can name
     the arm. If it shows no reply at all, QFEX cannot meet 0058, and nothing places on it
     until a new record decides what answers the arm.
2. **An arm that cancels is within 0121's authority, widening it for QFEX.**
   - QFEX's note says enabling cancels every open order on the authenticated account. So the
     `AccountWideCodAuthority` for a QFEX account covers cancellation at the arm as well as at
     a drop. Its holder asserts that every open order on that account may be cancelled at
     either point.
   - As the owner's condition for accepting that, the holder also asserts that the account has
     one order-entry connection, the session's own, and that nothing else trades on it: no
     GUI, no second program, no other API client. The library cannot check this, and infers it
     from no lease (0121). The arm comes before any place on every epoch (0058), so it cancels
     only orders of ours left from an earlier epoch.
   - 0058 asks the resync together with the arm. So its pages may still list orders the arm is
     cancelling. Those cancels reach fbc-oms as order updates if QFEX reports them.
   - QFEX's `plan_exec` plans exactly one order-entry connection per account. A second
     connection's arm may cancel the first's orders, and no page says which connections hear an
     account's replies.
3. **The arm's echo is kept, narrowing 0126.**
   - A frame is not redacted when its only content is an `ack` or `err` envelope whose
     `incoming_message` equals the arm as item 1 defines it. The arm carries no credential,
     account identifier or order.
   - The codec decides the arm's answer from that frame and its own state, reading no redacted
     byte. So a capture keeps the reply's bytes, `message` aside, and replay hands the codec
     what it needs to decide the arm's answer.
   - An `err`'s free-text `message` is redacted as a span of its own.
   - Any other frame that can echo a request answers nothing when it arrives while the arm is
     the connection's only request in flight, unless it echoes authentication. That holds
     whatever its envelope, nested or flat, and whatever its `incoming_message` holds: an
     object of any `type` or none, a string, `{}` or null.
     - It is journaled with its envelope, member names and nesting kept.
     - Each of these is redacted as a span of its own: every scalar value inside
       `incoming_message`, a string echo as a whole, the `message`, an `error_code` QFEX does
       not document, and the value of any undocumented member.
     - So the capture shows the shape of whatever answers the arm, and a new record can decide
       from that shape (item 1).
   - Every other frame that can echo a request stays redacted whole. That includes one that
     arrives while another request is in flight, and an echo of authentication.
4. **What the arm's answer lets the probe do.**
   - fbc-core's `VenueCommand::ArmCancelOnDisconnect` is answered `Accepted` at
     `AckLevel::Final` "once the venue has turned the protection on". Under 0126 an echo by
     itself shows only that QFEX received the arm. The owner chose that, for QFEX, an `ack`
     echoing the arm as item 1 defines it is that Final acceptance. The drop test below is the
     evidence that this reading holds. If it fails, the probe stops (item 7), and a new record
     decides what answers the arm.
   - Once the arm is accepted, the session places, and `AccountWide` skips 0080's quarantine
     (0121 item 3).
   - The probe's first place phase is therefore the drop test. It places nothing further, on
     any epoch, unless the drop test passes. The test runs two sessions on one account and one
     registry, one after the other and never at once, so the account keeps one order-entry
     connection throughout.
     - **The first session** arms, and once its arm is accepted places one post-only order, X.
       It then closes its connection gracefully, with a WebSocket close, without cancelling X.
       A connection that sends only authentication and reads, never an arm, then reads
       `get_user_orders`. That shows whether a graceful close cancels. If X is gone, with no
       `get_user_trades` row naming it, a graceful close cancels. Whether the arm cancels the
       account's open orders is then recorded as not measured, and the second session still
       runs for Y.
     - **The second session** arms. If X is still open, a `CANCELLED` report of it shows that
       the arm cancels the account's open orders and that QFEX reports it. No report settles
       nothing until the next read. Once its arm is accepted, it places one post-only order, Y.
     - **The read before the drop.** Once Y is accepted, a connection that never arms reads
       `get_user_orders`. It enters no order, so the account still has one order-entry
       connection. X listed there survived the arm. X absent with no `CANCELLED` report is an
       arm-time cancel QFEX did not report: it is recorded, and the probe stops itself after
       the drop (item 7).
     - **The drop.** Then the second session's connection drops abruptly, with no WebSocket
       close.
     - **What the drop cancels.** The drop cancels Y. If the read before the drop listed X, the
       drop cancels X too. That is the reach over earlier epochs on which `AccountWide`'s skip
       of 0080's quarantine relies. If it did not list X, that part is recorded as not
       measured rather than failed.
     - **How each order is read.** Each order is shown cancelled, not filled. It is absent from
       `get_user_orders` on a connection that never arms, and no `get_user_trades` row names
       it.
     - **What happens meanwhile.** From the first session's arm to that read, no session sends
       a cancel of X or Y, no person cancels them, and nothing else trades on the account. From
       the second session's arm to that read, nothing reconnects and no later arm is sent. With
       the read before the drop separating the arm's cancels from the drop's, this rules out
       every other cause of the cancels.
   - The test does not measure an order placed through another live connection after the arm.
     The owner's condition (item 2) keeps that case out of operation. Nor does it measure
     whether an arm or a drop cancels another account's orders, since item 7 lets no order
     rest on any other account while a session runs. A record that admits QFEX for more than
     one account of a user needs that evidence first.
   - Production stays `exec: None` until a record admits QFEX order entry under 0117 on this
     evidence, the drop test included.
5. **The resync is untrustworthy, seeds nothing, and is bounded by the codec.**
   - **One bracket.** It runs inside one `ResyncBegin` … `ResyncEnd`, so 0058 holds places
     until all of it is done. The watermark is the instant the first read was sent, since no
     read carries an as-of time.
   - **The signed read, in the first round.** The resync call itself makes the one signed REST
     read, `GET /user/positions`, from the nonce and time its `EncodeCtx` gives. It carries
     `x-qfex-requested-account-id` when 0122 selects a subaccount, and marks its credential
     headers and that header `redact: true` (0024, 0122 item 3). QFEX documents no REST limit,
     and its two buckets count only authenticated WebSocket messages (0128). So the read is
     charged as `OpKind::Rest`, as this crate's other REST reads are, and no declared bucket
     counts it. A failed or refused answer asks to reconnect, and one that never comes is
     caught by the codec's own deadline (below).
   - **Later rounds.** These are socket reads only, which need no signature after
     authentication:
     - `get_user_orders` pages at the configured page size, from offset 0, until a page lists
       fewer orders and TWAPs together than that size. Orders are deduplicated by `order_id`.
       Every full page is flagged: the codec counts it, and the probe records each one with its
       page size and offset. At a page size above the orders of ours that can rest, a full page
       shows an order on the account that fbc-oms did not place, or one it no longer counts. At
       a smaller page size, which the probe sets to force page boundaries, a full page is
       expected and is only recorded.
     - `get_user_trades` pages from a start time (below), until a short page.
   - **One reply of a kind in flight.** The codec sends a read only while no other request
     that answers in the same kind of frame is in flight on the connection. That includes
     `get_order`, whose answer is also an `all_orders_response`. So each answer belongs to the
     one request that asked for it.
   - **Its own deadline.** The codec arms a timer of its own when it asks the resync. It asks
     to reconnect in two cases: `ResyncEnd` has not come when the timer fires, or a full page
     (one listing as many orders and TWAPs as the page size) adds no new id. A short page ends
     the paging first, even an empty one. So a read the buckets refused, or one QFEX never
     answers, ends the epoch rather than leaving it unable to place. The session's own
     deadline, FBC-nwpr, will bound every venue once it lands.
   - **Missed fills.**
     - The start time is the earlier of the latest fill time this codec decoded and the send
       time of the oldest order it placed and has not seen end. It is rounded down to whole
       seconds. With neither, nothing is read.
     - This goes further back than the owner's "since the last fill" whenever an order of ours
       that has not ended was sent before that fill. No page says what kind of time a trade's
       `timestamp` is, whether the fills channel and `get_user_trades` share it, or in what
       order fills arrive. An order's send time is on our own clock, and it comes before every
       fill of that order. The extra rows cost only trade pages, since the ledger drops them
       by key. The probe sets its ledger's age bound longer than all the sessions its registry
       serves, and its count bound above the fills those sessions can see, since a fill the
       ledger forgets by age or by count moves its horizon. So a fill it heard live is still
       held, and judged a duplicate, when it is read again.
     - These fills are replays (`FillEvent::replay`), with our side taken from the order the
       codec placed. A row naming an order this codec did not place is not pushed as a fill.
       It is reported in the evidence.
     - Both reads build the fill's key from `trade_id`, so the ledger removes the overlap with
       fills heard live. It judges a fill a duplicate before it judges the fill's time, so only
       a fill the live stream missed is refused below.
     - They carry a matching-engine time only once captures show two things: that QFEX's trade
       timestamp is one, and that a fill's `trade_id` is the same in `fill_response` and in
       `get_user_trades`. Until then the ledger refuses them as untimed
       (`Admission::Untimed`) and fbc-oms only counts the refusal. A hand-seeded market's
       position stays known and leaves the fill out, so item 7's probe stops itself.
   - **Missed ends.** The resync ends no order. An order of ours that ended while the stream was
     down is not in the pages, an untrustworthy page's absence ends nothing (0055), and QFEX
     replays no live update. Such an order stays counted as resting, which is the safe side,
     until a later record decides. `GET /user/historic-orders` lists ended orders by client id,
     but the owner chose three reads, and its `terminal_status` values are listed nowhere. The
     probe reads it as evidence, so that a later record can add it as a fourth read.
   - **Order of what is pushed.** The codec pushes what the reads show only once every read has
     answered: the open orders first, then the fills, so that a fill delivered again reaches
     its order.
   - **What is left out.** TWAPs and stop orders are left out of the snapshot, since we place
     neither.
   - **What it declares.** QFEX declares `SnapshotSource::Untrustworthy` and
     `SnapshotReads::Separate`. So no resync seeds a QFEX market's position (0055), and no
     absence counts on the Unknown ladder.
   - **The cross-check.** The probe compares, per symbol, the orders the resync showed with
     REST's `open_orders`. A disagreement is reported in its evidence and not repaired.
   - **A later flip.** A later record makes the source trustworthy on pre-production evidence,
     as 0094 did for Paradex, and live QFEX trading needs that record. Its evidence must show:
     - every order the stream showed open appearing in the pages, across page boundaries a
       small page size forces, with orders placed and cancelled between pages as well as at
       rest;
     - `open_orders` agreeing with the pages at rest;
     - a measured lag behind the stream, for 0109's `snapshot_lag_bound`;
     - positions following fills.
6. **A pre-production probe mode, narrowing 0117 and 0122's item 4.**
   - `qfex.exec.mode` is absent, or `preprod_probe`. Any other value is a configuration error.
   - Absent, the factory gives `exec: None` and no order-entry codec, as on main.
   - `preprod_probe` declares `ExecCaps` and gives the order-entry codec only when every QFEX
     origin the configuration names passes the guard. It also requires `qfex.exec.page_size`
     (1 to 1000), `qfex.exec.resync_timeout` and `qfex.exec.rpc_timeout`, none with a default.
     `qfex.exec.rpc_timeout` is each request's deadline, after which a place ends `Unknown`. A
     value not longer than the 500 ms speed bump is refused as a configuration error, since a
     limit GTC or IOC place may get no `ACK` until its hold ends. Each origin is one of:
     - its pre-production origin, with a trailing `/` ignored: `qfex.trade_url` exactly
       `wss://trade.qfex.io`, `qfex.rest_url` exactly `https://api.qfex.io` (reference data
       and every private REST read), or `qfex.mds_url` exactly `wss://mds.qfex.io`;
     - for a test stub, a loopback address (`127.0.0.0/8` or `[::1]`).

     Everything else is refused as a configuration error before anything is planned: a mix of
     loopback and other origins, any `qfex.com` host, and any other origin. A pre-production
     origin QFEX supplies outside this list needs a reviewed change, not a setting.
   - The loopback origin goes further than the owner's choice, which named only the
     pre-production host. It follows the guard that choice names as its model, Paradex's
     testnet guard, which admits a loopback stub. It lets the check, where no test touches a
     venue, run the probe mode against a stub. A loopback configuration names no venue, and
     the probe refuses a proxy with one (below).
   - The factory sees no proxy setting, so it checks origins only. The probe builds both the
     `VenueConfig` and the `Connector`. It refuses to start when any QFEX origin is loopback and
     the proxy is not `ProxyConfig::Direct`, as the Paradex testnet sample's `direct_to_stub`
     does. Nothing in fbc-runtime names QFEX.
   - `Readiness::Record` holds in both modes. Under `preprod_probe` an `ExecSession` may open
     QFEX's plan, departing from 0117's ordering and 0122's item 4 for pre-production alone.
     Everywhere else both are unchanged.
   - The probe mode's `ExecCaps` follow 0054's rule: each value cites its page, and where the
     pages are silent the value is the conservative one. Fixed here:
     - `CancelOnDisconnect::AccountWide`;
     - the snapshot as item 5 declares it;
     - no amend;
     - cancels and queries by the venue's order id only;
     - no batch, cancel-many or cancel-all;
     - limit GTC or IOC, and post-only GTC;
     - `cid_echoed_on_events: false`, until captures show the echo;
     - a two-phase acknowledgement whose risk window is longer than the 500 ms hold, with the
       margin its ticket cites.
7. **How the probe runs.** The probe (FBC-l931's arm-only stage, then FBC-9sk2's place stages)
   runs only as an owner-assisted run (0067).
   - **Its registry.** It builds its registry with
     `Registry::for_testnet_run(TestnetRun::owner_assisted())`.
   - **Its origins.** Every connection and request the probe makes outside `ExecSession` takes
     its origin from the same `VenueConfig` that passed item 6's guard, read through a
     venue-crate function that applies the guard. That covers its account listing, its checks,
     and the drop test's reads. The probe takes no origin of its own, and runs market data and
     discovery on that same config.
   - **Its accounts, before anything connects.** The probe refuses a run whose
     `qfex.account_id` or `qfex.account_key` 0122's item 1 refuses, or whose credentials for any
     account it reads are missing or empty, as the Paradex testnet sample's guard does. Its
     refusal names the setting, never its value. The venue-crate function that applies item
     6's guard applies 0122's check too, so the probe's own connections pass both before they
     open, as `ExecSession`'s do.
   - **Its credentials.** It reads every account of the provider user: the primary account,
     which `GET /user/subaccounts` leaves out, and every subaccount that endpoint lists. Each is
     read with credentials that reach it, and its REST reads select a subaccount as item 5's
     do. The account it trades must be the primary account or a listed subaccount. If any
     account cannot be read, the probe does not start.
   - **Its checks, before every session.** Each account must hold no open order:
     `get_user_orders` empty on a connection that sends only authentication and that read. Its
     REST positions must be flat. Only then does the probe seed the markets it trades flat by
     hand. The drop test's two sessions count as one session for these checks and the seed:
     before its second session, the read item 4 makes after the graceful close must show no
     open order but X on any account, and every position flat. The probe seeds nothing then,
     since the second session uses the first session's registry.
   - **One arm at a time.** No two connections of the provider user, on any account, have arms
     in flight at once anywhere in the probe. It runs one `ExecSession` at a time. No page says
     which connections hear an arm's `ack`.
   - **Its commands.** It gives `AccountWideCodAuthority` for the account it trades. It sends
     every place and cancel as an fbc-oms `Authorization`, through an unchanged `ExecSession`.
     The arm is the session's own request (0058, 0121), for which fbc-oms issues no
     `Authorization`. So this record reads the owner's "every arm, place and cancel goes
     through an fbc-oms Authorization and an unchanged ExecSession" as every arm through the
     unchanged `ExecSession`.
   - **Stage one, the arm only.** It opens a session and lets it arm. At the first arm failure
     it ends the session through its stop control, its reconnect pacing set so that no second
     attempt starts meanwhile. That stage records what answers the arm (item 1). Every frame
     that arrives while the arm is in flight is kept as item 3 says, so the capture shows
     whatever answers.
   - **Stage two, the places.** These come only after the arm's fixture and decoding have
     landed, beginning with item 4's drop test.
     - Until captures show our client id on QFEX's `ACK`, each session places at most one
       order, a post-only GTC far from the touch. In the drop test, X may still rest from the
       first session while the second places Y.
     - Once they do, a session may also place, against no order of any account of the user:
       - one minimum-size post-only GTC priced through the touch, to capture how QFEX refuses
         a crossing post-only order, on which its pages disagree, and whether that refusal
         echoes our client id;
       - at most one minimum-size limit IOC at the touch, to capture a taker fill.
     - The taker IOC is the last place of its session. A position it leaves is closed in the
       same session through fbc-oms. The market goes to Exit by the owner's Flatten (0012).
       The probe asks the planner for a price-capped crossing intent at or through the touch,
       planned as one reduce-only limit IOC sized so the position never crosses zero (0110),
       with reduce-only emulated since the probe's caps declare none (0106). Each is placed
       against no order of any account of the user. It makes at most the number of attempts
       the run sets beforehand, then reads REST positions flat before the session stops. This
       goes further than the one-order limits above, for flattening only.
   - **Stage three, the pages.** Once the drop test has passed and captures show our client id
     on `ACK`, one session rests up to three minimum-size post-only GTC orders far from the
     touch, each placed and cancelled through fbc-oms. Alongside it, a connection that never
     arms and never places pages `get_user_orders` at a page size of 1 while the session places
     and cancels between its pages. This gathers item 5's evidence for a later flip. It goes
     further than the one-order limits above.
   - **The streams against the reads.** After every place, cancel and fill of stages two and
     three, a connection that never arms reads `get_user_orders` and REST positions, and the
     probe compares them with the order and fill streams, as FBC-8xr's program did for
     Paradex. It times the first read that agrees with the stream, for 0109's
     `snapshot_lag_bound`.
   - **When the probe stops itself.** It stops placing, ends the session, and places nothing
     more on QFEX until a new record or the owner's reconciliation decides, when any of these
     happens:
     - an order event or fill of the probe's comes back without our client id, so fbc-oms can
       neither count nor cancel that order;
     - the ledger refuses a replayed fill as `Untimed` or `BeyondHorizon`;
     - a resync's position check reports `Desync` or `Unsettled`;
     - the drop test fails, or its read before the drop shows an arm-time cancel QFEX did not
       report;
     - a session stops with its account's REST positions not flat.

     In the drop test, a graceful close that leaves X resting is recorded, not taken as a
     failure. Every other session cancels our orders explicitly before it stops (below).
   - **After every session.** The drop test's two sessions count as one session here.
     - Before a session stops, it cancels every order of ours its registry can name, by venue
       id, as fbc-oms `Authorization`s (a Stop's explicit cancels). The drop test's sessions do
       not, since the drop is to cancel X and Y.
     - After any session that may still leave an order of ours resting, a connection that never
       arms reads `get_user_orders`.
     - If an order of ours is still resting, the probe stops itself. Only then may the owner
       cancel it by hand on QFEX's pre-production site, outside any session and while no
       connection of the user is armed. The probe reads it as gone before anything else runs.
     - That hand cancel is the one cancel outside fbc-oms, and the one action on the account
       through QFEX's site, that this record allows. It is an exception to two of the owner's
       conditions: that nothing else trades on the account, and that every order command goes
       through fbc-oms. A position left open is the owner's to reconcile, outside this record. It exists because fbc-oms cannot cancel an order it cannot name, such
       as one whose events lack our client id, and because a session that could reach the
       order would arm first.
8. **A reply settles only the request it names.** Nothing is matched by arrival order.
   - **A place:**
     - It is answered by the first `order_response`, while it is unsettled, whose
       `client_order_id` is exactly the place's and whose symbol is the place's. Our client ids
       are minted under a held namespace lease and never reused. QFEX does not keep a
       `client_order_id` unique, so that is enough only while nothing else trades on the
       account, as the `AccountWideCodAuthority` asserts (item 2).
     - `ACK` accepts it, at the level item 6's acknowledgement model gives. The outcome
       carries the `ACK`'s `order_id` as the order's venue id (`ItemRef::vid`), which every
       later cancel and query of the order names.
     - `FILLED`, `IOC_PARTIALLY_FILLED` or `IOC_CANCELLED` as the first matching reply accepts
       it as executed, since QFEX has acted on it, and carries its `order_id` the same way. The
       same frame is also the order's update: `FILLED` under the fill-status rule below, and an
       `IOC_*` status by the quantities it states.
     - A status that QFEX's enums page describes as refusing a new order rejects it. The codec
       cites that page for each status it maps, as 0054 asks of caps. `RATE_LIMITED` settles
       nothing (0126). These are not mapped until a capture shows them (FBC-jdj7): a status
       that refuses only a modify or a cancel, one the pages leave undescribed, and a crossing
       post-only order's, on which the pages disagree.
     - The codec settles places this way only once captures show that QFEX echoes a sent
       `client_order_id`, exactly, on its `ACK`. Until then every place ends `Unknown` at its
       deadline, item 6's `qfex.exec.rpc_timeout`. A reply without our client id matches no
       place. The Unknown ladder (0005) resolves an `Unknown` place from a later resync's pages
       or by the venue id once known.
   - **A fill's status.** A live `FILLED` `order_response` ends an order only when its
     `quantity_remaining` is zero. Otherwise it is a partial-fill update, since `FILLED` "could
     be partially filled".
   - **A cancel:** it names the venue's order id.
     - A `CANCELLED` `order_response` naming that id accepts it.
     - `NO_SUCH_ORDER` naming it rejects the cancel and ends nothing. The order may still be in
       the speed bump's hold.
   - **No settlement:**
     - An `err` settles no place and no cancel, whatever it echoes. 0126 holds that an echo
       does not show what the venue did with the command, and item 3 keeps such frames
       redacted whole, so no decoding may rest on them. The request ends `Unknown` at its
       deadline.
     - A frame that names no request in flight, or names more than one, settles nothing.
       Neither does a status this item does not map.
     - An `order_response` for an order with no request in flight is an order update.

## Alternatives

- **Take an arm unanswered for a quiet period as accepted:** rejected. 0058 wants a Final
  answer; an unanswered arm proves nothing, and taking it would place unprotected.
- **Add a capability now for a venue whose arm gets no reply:** not taken. The capture may show
  a reply. If it does not, what proves an arm is a decision of its own.
- **Skip the arm, or place unarmed, on pre-production:** rejected. The probe measures what a
  production session would do, and an unarmed session would bypass 0058.
- **Take an armed drop's effect from the next session's resync:** rejected. The next session
  arms before it reads, and QFEX's note says that arm itself cancels. A cancel at a drop could
  not be told from one at the next arm.
- **Read positions from the positions channel's snapshot:** rejected. It has no end marker, and
  a missing message cannot be told from a flat position. REST answers once, whole.
- **A fourth read, `GET /user/historic-orders`, to end orders that ended while the stream was
  down:** not taken now. The owner chose three reads, and its `terminal_status` values are
  listed nowhere. The probe reads it as evidence (item 5).
- **Run the drop test with two sessions open at once on one account:** not taken. It would
  measure an order placed through another live connection after the arm, a case the owner's
  condition keeps out of operation (item 2), and it would break that condition while it ran.
- **Map each reject status only from its own capture:** not taken. The owner chose that a
  reject status rejects. A status QFEX's pages describe as refusing a new order is mapped from
  that page, and FBC-jdj7 corrects the list from the captures.
- **Declare QFEX's snapshot trustworthy from its pages:** rejected. Offset paging over a moving
  set, with no has-more field, count or as-of time, cannot prove a snapshot complete.
- **Make the positions read in a later round, from a timer:** not taken. The resync call has
  the `EncodeCtx` the read is signed from, and making it first lets the watermark be the
  instant the first read was sent. A timer would add a step and change nothing else.
- **A write path for the probe outside `ExecSession`:** rejected. Every order command takes
  fbc-oms's one checked path, and the arm and resync gates are the session's. Item 7's hand
  cancel, made by the owner between sessions and not by the probe, is the one exception.
- **Declare QFEX's `ExecCaps` in every mode:** rejected, for 0122's reason.
- **A flag with no host guard:** rejected. A production URL configured with the flag would
  trade production silently. The venue crate knows QFEX's hosts, where fbc-oms does not (0067).
- **Correlate replies by arrival order:** rejected. Replies carry no request id, unsolicited
  updates carry the same identities, and no page says which connections hear them.
- **Cancel by client id:** rejected. It may cancel several orders, and the page disagrees with
  itself on the field that carries it.
- **Take an `err` echoing a place as its reject:** not taken (0126). A place taken as rejected
  while it rests would free its exposure under the caps, where an `Unknown` one keeps counting.
- **Redact the arm's echo as 0126 redacts every echo:** rejected. The capture this record waits
  for would be kept only as a hash, and replay could never see the arm accepted.

## Consequences

- **Production.** Nothing here lets QFEX place in production. Production stays `exec: None`
  and `Readiness::Record` (0117, 0122). Making it trade needs a record on the evidence this one
  lists. Among the rest, live QFEX trading needs item 5's later record, which makes QFEX's
  snapshot trustworthy with a measured `snapshot_lag_bound` (0109). Until that record, no
  resync seeds a QFEX market (0055), and a market seeded by hand arms only in a registry built
  for an owner-assisted run (0067), such as item 7's probe.
- **Order of work.** Each piece lands before the stage that needs it.
  - **FBC-mfey, filed with this record, before the arm-only stage.** It lands:
    - the `preprod_probe` mode and its guard;
    - one order-entry endpoint and codec under the mode;
    - the arm's encoding and item 1's one-arm rule;
    - item 3's redaction exception, which decodes nothing yet;
    - a codec that keeps its epoch open on the private frames that stage brings;
    - the REST reads' HMAC headers, from `src/auth`, a review path.

    FBC-blcw's refusal of the arm and FBC-zeb0's `exec_codec` `None` then hold only outside
    `preprod_probe`.
  - **FBC-jxau, between the stages.** It decodes the captured arm answer. Under
    `preprod_probe` it also lands, in their documented shapes:
    - item 5's resync, with its signed first round, its paging, its own deadline and its
      `ResyncBegin` and `ResyncEnd`;
    - item 8's cancel settlement;
    - the decoding of `order_response`, `fill_response`, page and position frames into order
      updates, fills and resync events, with places still ending `Unknown`.

    So stage two's sessions can end a resync, and the probe can see an event that lacks our
    client id. FBC-zeb0's rule that a private state frame ends the epoch then holds only
    outside `preprod_probe`.
  - **FBC-9sk2** splits: the arm-only stage is FBC-l931, and FBC-9sk2 keeps the place stages.
  - **FBC-jdj7** then refines these from the place stages' captures only: the settlement of
    places, the statuses and the reject list, the trade timestamp and the fill key, the place
    deadline's margin, the caps, and the replay tests.
  - **FBC-77aa** is unchanged.
- **What the probe's report answers,** for the primary account and a subaccount:
  - what answers an arm;
  - whether enabling cancels open orders, and whether those cancels are reported, or that
    this was not measured because a graceful close had already cancelled X (item 4);
  - the drop test of item 4;
  - whether a graceful close cancels;
  - whether every other account of the user still holds no open order and a flat position at
    item 7's checks, which shows that a session's places and fills land only on the account
    it selects;
  - whether QFEX accepts the client ids the probe mode's `ClientIdFormat` produces, at their
    full length and in every character they use, since QFEX publishes no limits and has an
    `INVALID_CLIENT_ORDER_ID` status;
  - whether `ACK`, rejects and fills echo a sent client id exactly, byte for byte;
  - how long `ACK` takes, for post-only and for other places, and whether it comes before or
    after the speed bump's hold;
  - which statuses are rejects and ends, and how QFEX refuses a crossing post-only order;
  - what `GET /user/historic-orders` shows for each order of ours that ended, its
    `terminal_status` included;
  - what kind of time a trade's `timestamp` is, and whether a fill's `trade_id` is the same in
    the fills channel and in `get_user_trades`;
  - which side field a `get_user_trades` row carries, and whether it is ours or the
    aggressor's;
  - how the pages behave at their boundaries;
  - what `open_orders` counts;
  - the lag between the stream and the reads.
- **The drop test's sessions.** They run one after the other on one account and one registry,
  X carried from the first to the second. The adapter's plan never opens two at once.
- **Cost.** A resync costs one `GET /user/positions` in its first round, which no declared
  bucket counts (item 5). Then come `get_user_orders` pages at 5.0 each, plus trade pages,
  against the user-scoped buckets (0123, 0128), charged one at a time.
- **Unknown places.** An Unknown QFEX place counts as resting until a resync's pages show it or
  the venue id resolves it. Because the snapshot is untrustworthy, nothing ends it Lost. That
  is the safe side, at the cost of capacity.
- **Landing order.** 0122 is in PR #217. This record lands after it.

## What would show this was wrong

- An `ack` echoing the arm, followed by an abrupt drop of that armed connection, after which an
  order of ours survived. Then acceptance proves no protection.
- An order of ours resting after an abrupt drop of an armed QFEX connection, or after the drop
  test passed.
- An arm-time cancellation QFEX never reports, so the registry kept counting an order the
  account no longer held.
- An `ACK` echoing our client id for a place QFEX later rejected outside the declared window,
  or an `order_response` echoing our client id for an order not ours.
- A status item 8 maps as a reject, from QFEX's enums page, sent for a place that then rests.
- A resync whose pages, deduplicated, missed an order the stream showed open at rest. That
  would not break this record, which seeds nothing from the pages, but it would bar any flip to
  trustworthy.
- A production QFEX origin passing the probe mode's guard, or a pre-production origin reaching
  production.
- QFEX documenting a request id or an arm reply. Either would simplify items 1 and 8, and a new
  record would replace them.
