# 0115 — An order-entry session drives every connection plan_exec plans, the first carrying the orders, each on its own connection number, epochs and pacing, with the codec's frames and reconnects routed to the stream they name, amending 0053

Status: accepted
Date: 2026-10-20

## Context

0053 let an `ExecSession` drive exactly one planned order-entry connection and refused a plan
of more (`ExecSessionError::Endpoints`), noting that driving several was FBC-tnsf. Hibachi's
order entry is two private sockets: `/ws/trade`, which takes orders and answers them as
JSON-RPC replies, and `/ws/account`, which carries fills, order events and the risk engine's
`order_request_rejected` about 10 ms after a trade-socket reply (Java `HibachiBroker`). The
trade socket closes after about 60 s without application traffic and ignores WebSocket pings,
so the codec keeps it alive with its own timer (`orders.status` every 14 s); the account
stream needs `stream.start`, then `stream.ping` every 14 s, and its listenKey expires silently
(`stream_expired`, then 404 pings), so the codec must be able to reconnect one socket while it
is open. The owner chose, on 2026-10-19, "MD + signer + runtime prep" for the Hibachi round,
which includes the runtime's two-socket session; order entry on Hibachi waits for the
cancel-on-disconnect record (0058's refusal stands). Per-socket 0058/0080 semantics (holding
places while the account stream is down or expired, the arm's socket) are FBC-rta5's, filed
deferred. This record decides how one session drives several connections.

## Decision

1. **The order connection.** The first endpoint `plan_exec` plans carries the orders: every
   command, the cancel-on-disconnect arm and the resync are encoded for it, as 0053, 0057 and
   0058 have them, and only the codec's report of its stream authenticates its epoch.
2. **Each endpoint its own connection.** Endpoint `i` of the plan is stamped connection
   `ExecSessionConfig::conn + i` (`SessionError::NoConnectionLeft` past `u16::MAX`), with its own
   epochs, reconnect pacing (the consumer's `ReconnectPacing`, one pacer each), timers and HTTP
   requests on a session core of its own, charged to the consumer's one `RateLimiter`. Two
   endpoints on one stream are refused (`ExecSessionError::SharedStream`), since an effect
   names a stream. Each URL is checked when the session is built. An in-process stream (0104)
   stands in for exactly one connection, so it refuses a plan of more
   (`ExecSessionError::Endpoints`); a plan of none is refused as before.
3. **One codec, `on_open` per epoch.** The one codec serves every endpoint: each epoch of each
   endpoint calls `on_open` with its stream and exactly the nonces `nonces_for(CtxCall::Open)`
   asks for, reserved from the consumer's one `NonceSource`, its frames on that endpoint and its
   HTTP requests charged together as 0053 has it (refused for now, the epoch ends as a drop held
   until the buckets would admit them; never, `OpenNeverFits`). Each frame is decoded with the
   stream it came on, inside the venue's decode scope. A timer or HTTP request belongs to the
   epoch of the endpoint whose input asked for it, and one of an ended epoch comes back into
   nothing, dropped and counted, as 0056 has it.
4. **Routing.** A frame or reconnect the codec asks for on another planned endpoint's stream
   is handed back by the core that took it and executed on that endpoint, charged there as its
   turn comes, once the effects for the endpoint that asked have run: so "nothing behind a
   reconnect is reached" holds per batch, and a frame for the other endpoint asked before it
   goes out. One for an endpoint with no open epoch (the order connection while it waits to
   reconnect, or another between epochs) is refused and counted where it was asked for; one
   for a stream no endpoint carries is a codec defect, as before.
5. **Reconnects alone.** An endpoint that drops, asks to reconnect (as for an expired
   listenKey) or fails a write ends its epoch and reconnects through its own pacing, while
   every other endpoint's epoch stays open; the handler is told each epoch's end, as 0039. While
   the order connection waits to reconnect, the other endpoints are still read, their timers
   fire and their results come back; their waits yield to the order connection's attempt after
   each one, so a busy account stream does not starve it. Commands submitted meanwhile are
   `NotSent(Disconnected)`, as 0057.
6. **One ingest order.** Every input of every endpoint is stamped from the shard's one
   `IngestClock` and journaled under its own connection's epochs (`Opened`, `Closed`, inbound
   and outbound records name it), so the endpoints' events share one ingest order.
7. **Readiness.** An event of another endpoint's input counts for the order connection's
   authenticated epoch, if one is, as the answer to its arm, the end of its resync or what it
   shows of the orders held unprotected (0058, 0080), since a venue's answers may arrive on an
   account stream; it never authenticates the order connection. Until FBC-rta5, an outage of
   another endpoint holds no place: no venue that plans more than one endpoint places orders
   yet (Hibachi is refused by 0058 until a record based on the cancel-on-disconnect probe lifts
   that).
8. **Stops, errors and drops.** A stop journals every open epoch closed (each frame already
   waiting stamped behind its close, reaching no codec), closes each connection and tells the
   handler; an error, a dropped run and the session's drop end each endpoint's epoch as they
   do the order connection's (0053), the handler not called while a panic unwinds.
9. **One thread.** The session's one thread reads no other endpoint while a write waits on a
   peer, within the consumer's write-stall window (0036).

## Alternatives

- One session per endpoint, each with a codec, as market data: rejected. The account's state
  (requests waiting for answers, a resync being read) spans both sockets, as Hibachi's replies
  and rejects do, and 0053 keeps one codec for the account's life.
- Holding places while another endpoint is down: deferred to FBC-rta5 with the per-socket arm
  semantics, which need the cancel-on-disconnect record first.
- Ignoring another endpoint's events in the 0058/0080 gate: rejected. An arm answered, or an
  order's end shown, on an account stream would then never count, leaving the epoch unready or
  a market held until a query on the order connection.

## Consequences

- `ExecSessionError::Endpoints` now refuses only a plan of none, and a plan of more than one for
  an in-process session; `ExecSessionError::SharedStream` is new; `ExecSession::current_on`
  names each endpoint's epoch; `stale` and `counters` sum over every endpoint.
- The conformance toy authenticates and pings each opened stream on its own (`ping_tag`), and
  only a reopened `EXEC_STREAM` cuts a resync short.
- A consumer gives an order-entry session as many connection numbers from `conn` on as the
  venue plans endpoints.

## What would show this was wrong

- A venue whose order entry must use a connection other than its first planned one, or whose
  authentication on one socket depends on another's.
- An account stream's frames arriving fast enough that the order connection's reconnect or a
  write's peer stalls the other endpoints' reads past what the consumer's latency gates allow.
