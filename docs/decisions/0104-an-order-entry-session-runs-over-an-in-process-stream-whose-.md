# 0104 — An order-entry session runs over an in-process stream whose peer sees the codec's frames and the shard's market data in ingest order, and SimVenue is that peer, one per bracket, augmenting 0046, 0053 and 0097

Status: accepted
Date: 2026-10-19

## Context

BT-501's first criterion (design §10.1, restated: in shadow, market data is live and execution
is SimVenue, one per bracket, all three running the same shard code as live). 0046 split
SimVenue into an unchanged `ExecCodec` (`SimCodec`) and a pure engine (`SimEngine`) fed the
shard's envelopes and the codec's frames, and left FBC-6mf to host them in the runtime: deliver
every envelope, every frame and each answer at its due time. 0097 gave the engine a stream that
opens and closes at the host's word. An `ExecSession` (0053) owned a WebSocket for each epoch,
opened through the consumer's connector, and took commands only as fbc-oms authorizations
(0057); it arms its cancel-on-disconnect and resyncs on every epoch once the codec reports the
stream authenticated, and places nothing before (0058). The shard host (0100) drives the
sessions as members on one task. `fbc-sim` depended on `fbc-core` and `fbc-book` only, and the
runtime must never depend on it (design §3).

## Decision

1. **The stream.** `fbc-runtime` defines `OrderEntryPeer`, the venue end of an in-process
   order-entry stream, which opens no socket: it is told the stream's opening and closing at
   stamps the shard clock read, each frame the session's codec writes as written, each of the
   shard's market-data envelopes in ingest order, and each end of a market-data connection
   epoch; it answers with frames due at instants on the shard's monotonic clock
   (`next_due`, `take_due`). `InProcessStream` is the shared handle (an `Rc`, the shard's one
   thread).
2. **The session.** `ExecSession::in_process(config, codec, stream, handler)` builds an
   ordinary session from the venue's caps and plan, with the consumer's codec in place of the
   venue's, opening the stream in place of the planned URL, which is never opened; the
   credentials are dropped unread. Every other path is the socket session's, unchanged: one
   codec for its life, epochs, the arm and resync of every epoch, buckets (a connection attempt
   is charged as for a socket), deadlines, journaling (each inbound frame is stamped from the
   shard's clock and journaled at that stamp before it is decoded, 0006, 0078), and fbc-oms's
   authorization re-checked at each encode. The session sleeps until the peer's earliest due
   instant, and reads each frame the peer gives as it reads one off a socket: text when it is
   UTF-8, binary otherwise, as it writes them. An epoch's end closes the stream, and so does a
   run dropped with its epoch open, as a dropped socket closes its connection. The session core
   is generic over what it writes and reads on (`Wire`) and how a connection attempt opens it
   (`Dial`); the market-data session still runs on its WebSocket.
3. **The market data.** `PeerFeed<H>`, an `MdHandler`, hands each envelope, and each end of a
   connection epoch, to the peers of its streams first and then to the consumer's handler, in
   the same call, and wakes each stream's session, since an envelope can make an answer due
   sooner. The consumer puts it in front of every market-data handler of the shard (a
   `BookKeeper` included).
4. **SimVenue as the peer.** `fbc-sim` takes `fbc-runtime` as a normal dependency (the runtime
   never depends on it; `scripts/check-deps.sh` is unchanged) and implements the peer as
   `SimPeer` over one `SimEngine`: frames, envelopes, opening and closing go to the engine as
   0046 and 0097 have them, and its answers are given at their due times. A frame or envelope
   the engine refuses, and an open or close it refuses, is counted (`PeerCounters`), never
   answered: a stream sends no word back for a frame its venue cannot read.
5. **The greeting.** The simulated venue asks no login, and `SimCodec::on_open` writes nothing
   (0046), but a session arms and resyncs only once its codec reports the stream
   authenticated (0058). So as the stream opens, `SimPeer` says it is ready: a `ready` frame,
   which the engine never writes and which carries no stamp of its, due `to_client` after the
   open, and which the codec reports as `Conn { Authenticated }` naming no venue order. A close
   forgets a greeting not yet due. Answers the engine gave and the session has not read at a
   close are given on the next connection (0097), in due order with its greeting.
6. **A dropped market-data connection.** `SimEngine::end_epoch` invalidates, as a gap until the
   next complete snapshot, every book of the engine last fed by the ended epoch or an earlier
   one of its connection, exactly as `MdBooks::end_epoch` does for the runtime's (0039); the
   peer calls it on each end the feed hands it, so no order crosses or rests on a stale book.
   A trade still fills resting orders through the queue model: a print is not the book.
7. **Brackets.** Each bracket is its own session over its own stream and peer, under its own
   simulated account (`SimConfig::account`, the session's `acct`), with its own fbc-oms
   registry whose leases are taken on that account's name, so three brackets arm on one market
   without contending.

## Alternatives

- **An in-memory duplex carrying the WebSocket protocol to a server task**: rejected. It would
  run a protocol nobody needs, spawn a task, and leave the answers' due times to the server's
  own clock rather than the shard's stamps, so two runs could differ.
- **A `VenueFactory` for SimVenue, its URL routed in process by the connector**: rejected. The
  factory would duplicate the stood-in venue's caps and plan, be leaked to `'static` once per
  bracket, and the routing would hide in the connector which connections open no socket. The
  consumer already has the stood-in venue's factory; only the codec and the stream change.
- **A login frame `SimCodec` writes on open, which the engine answers**: rejected. It changes
  what the codec and engine say on every open that the existing hosts and tests rely on, for a
  handshake no simulated venue needs; the greeting belongs to the hosting.
- **The engine emitting the greeting on `open`**: rejected for the same reason: a host driving
  the engine by hand would receive a frame it never asked for.
- **Stepping the session by hand from the shard host instead of a stream**: rejected, as 0100
  rejected steppable sessions; the session's own loop over a different `Wire` gives the same
  order with nothing rebuilt.

## Consequences

- chaiwala-rs's shadow builds one `SimPeer`, `InProcessStream` and `ExecSession::in_process`
  per bracket, with the live venue's `ExecSessionConfig` (its credentials unread) and a
  `SimCodec` of the bracket's `SimConfig`, whose `stream` must be the planned endpoint's, and
  puts a `PeerFeed` naming all three streams in front of its market-data handler.
- FBC-066n's closed-loop replay and FBC-2zxk's Paradex stand-in run on this hosting; FBC-w9g's
  replay through SimVenue feeds the same peer from a journal.
- A peer's answers wait for the shard clock: a quiet market moves nothing but the session's
  own sleep until the next due instant, so a host needs no `advance` call of its own.
- An HTTP request the codec asks for still goes through the consumer's connector (SimVenue's
  asks none).

## What would show this was wrong

A hosted bracket whose fills differ from the same envelopes and frames driven through the
engine by hand (0046's determinism), an answer read before its due instant or long after it
on an idle shard, or a session over an in-process stream that behaves differently from one
over a socket in anything but the absence of the socket.
