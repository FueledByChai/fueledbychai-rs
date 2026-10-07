# 0071 — Paradex order entry and the private channels share one connection, the codec's own JSON-RPC ids start at 2^52, and a failed resync read reconnects

Status: accepted
Date: 2026-10-07

## Context

FBC-xvf puts Paradex's order-entry pieces together as one `ExecCodec`: the read-only codec's
login, auth frame and private channels (0061), FBC-xe1's encoder, FBC-0l9's replies (0069) and
FBC-0sc's REST resync and order query. Three things were the ticket's to choose:

- **How many connections.** The Java library keeps order entry on a connection of its own
  (`ParadexOrderWebSocketClient`: "One dedicated connection, no channel subscriptions") and
  reads the private channels on another, though since 2026-09-23 its order socket subscribes a
  private channel (`balance_events`) all the same, because Paradex closes a connection with no
  active subscription (close 4031): that socket already carries the order methods and a private
  subscription side by side, on the one WebSocket endpoint. An order-entry session runs one stream,
  and every epoch of it arms cancel-on-disconnect and resyncs before it places (0058); Paradex's
  cancel-on-disconnect belongs to the connection that armed it.
- **Whose reply a text frame is.** The runtime numbers an account's requests `RpcId` 1, 2, 3...
  and the encoder writes the rpc as the JSON-RPC `id`; the read-only codec numbered its auth and
  subscribe frames from 1 too. On one connection the two would collide: the reply to the auth
  frame could be read as the reply to order request 1.
- **What a failed resync read does.** The runtime admits no place or amend on an epoch until its
  resync has ended (0058). FBC-0sc's notes have a failed read push nothing; PR #90's Reviewer B
  (B7) noted that a read that is never answered usefully leaves the epoch waiting for FBC-nwpr's
  deadline.

## Decision

1. **One connection.** Paradex order entry and the account's private channels (`orders.ALL`,
   `fills.ALL`, `positions`, `account`) share the order-entry session's one WebSocket: the
   read-only codec's connection with the order methods added. Order entry is WebSocket only: a
   frame command while that connection is not authenticated is `NotSent(Disconnected)`, with no
   REST fallback.
2. **Ids apart.** The codec's own frames (auth, subscribe) take JSON-RPC ids from 2^52 up
   (`CONTROL_IDS`); an order request's id is its `RpcId`, below 2^52. A text frame with an id from
   2^52 up is the codec's own; any other is an order reply or an id-less error. A command whose
   rpc is 2^52 or more is `NotSent(Unencodable)`. Every id stays below 2^53, so a venue that
   reads JSON numbers as doubles echoes it exactly.
3. **A failed resync read reconnects.** A resync read that fails, is answered with an error
   status or does not decode drops that resync with nothing pushed and asks for the connection
   again (with a fresh login next), so the epoch ends and the next one resyncs; each resync's two
   reads go out under tags of their own, so an answer to an earlier resync is never paired with a
   later one. A new connection drops the resync, the order queries and the order requests of
   the one before that still await a reply or answer, which never reaches a later epoch (0027);
   a deadline the runtime still names for one is `Unknown`. A request whose reply already
   reported `Unknown` is kept until its deadline, so that timeout adds nothing (Reviewer B on
   PR #109). The read-only codec resyncs the same way, so it can seed positions once a session
   asks it to resync.
4. **The token as built.** Every REST read (the resync's two, the order query) carries the
   token the latest login gave, read when the request is built, in a redacted header and
   nowhere else. The order query's read that was never sent is `NotSent(Disconnected)` for its
   rpc; one that failed afterwards, was answered with an error status or does not decode is
   `Unknown`.
5. **What the session refuses after an encode is held until the connection ends.** The
   session can refuse a request after its encode returned `Ok` (a frame for its rate budget, an
   encode carrying a read until FBC-m8vm), and nothing tells the codec, so the codec holds it
   until a new connection. It does not guess from elapsed time: a frame written during a long
   write stall would be dropped while its reply is still coming (Codex on PR #109). The
   runtime telling a codec of such refusals is FBC-9r5o.

## Alternatives

- Two connections, as the Java library has them: the private stream and the order stream
  would end at different moments, so an epoch's resync and cancel-on-disconnect would cover one
  and not the other, and the runtime's session would need a second stream it does not have.
- String ids for the codec's own frames: JSON-RPC 2.0 allows them, but no Paradex page or client
  we read sends one, and the Java library's numeric ids are known to work.
- Small fixed ids (Java's 0 and 1, with requests from 1000): they collide with the runtime's
  rpcs, which start at 1 and are not ours to move.
- Leaving a failed resync to FBC-nwpr's deadline: every failure would stall the epoch for the
  whole deadline, with no reason reported.
- Retrying a failed resync read on the same connection: a resync is per epoch (0058), and a new
  epoch already re-reads everything.

## Consequences

- A dropped connection loses the order stream and the private stream at once, and the next
  epoch's resync covers both; there is no second socket to keep alive.
- Any burst of private-channel traffic shares the connection's buffer with order frames; the
  runtime's backpressure (`NotSent(Backpressure)`) is what a full buffer gives.
- A transient REST error during a resync costs a reconnect (and with it the cancel-on-disconnect
  of whatever rests), paced by the session's `ReconnectPacing`.
- Within one connection, requests and queries the session refused after their encode are held
  until it ends: a long-lived connection under sustained rate refusals grows that record until
  FBC-9r5o.
- The order query is an HTTP request out of `encode`; until FBC-m8vm the runtime refuses such an
  encode `NotSent(Unencodable)`, so the Unknown ladder's query step cannot reach Paradex yet.

## What would show this was wrong

- A testnet run (FBC-8xr) in which Paradex refuses or drops order methods on a connection that
  holds private subscriptions, or private data stops while orders flow: then order entry needs a
  connection of its own.
- A reply whose `id` is not the number sent (a venue that rounds ids): then the codec's ids must
  move below 2^53 by a wider margin, or to another form.
- Resyncs that fail often enough on testnet that reconnects, not the failures, become the
  problem: then a failed read should be retried within the epoch.
