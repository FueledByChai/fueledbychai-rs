# 0056 — An order-entry session runs its codec's HTTP requests and timers for the epoch that asked, gives on_timer exactly its nonces, and keeps its stream alive by the codec's own timer, augmenting 0027 and 0053

Status: accepted
Date: 2026-10-05

## Context

FBC-bnl gives the order-entry session (0053) the codec's HTTP requests and timers, which it
refused as codec defects until now, and its keepalives and write-stall bound as FBC-djl and
FBC-ha3 gave the market-data session (0033, 0036). 0027 decided how a market-data codec's
requests run; three things differ for order entry and were the ticket's to decide: the codec
lives across epochs (0053), so "only the epoch that asked" no longer means "only the codec that
asked"; `on_timer` takes an `EncodeCtx`, so its nonces must be reserved (0014 item 1); and
`ExecCodec` declares no keepalive, where `MdCodec::keepalive` does (0033).

## Decision

- **HTTP, as 0027.** A request the order-entry codec asks for runs beside the session's reads
  on its `Connector`, charged to the buckets, with its timeout from the ask, its body bounded
  by the consumer's `ExecSessionConfig::http_max_body` and its failure classified as 0027 does.
  Its result is stamped as an input of the epoch that asked and handed to `on_http`, inside
  the venue's decode scope with the consumer's engine namespace (0015), only while that epoch
  is current; once the stream has moved to a newer epoch it is dropped and counted
  (`Input::Http`), although the same codec lives on. Requests still in flight when the session
  stops or fails are dropped.
- **Timers per epoch, with exactly their nonces.** A timer is set for the epoch that asked; one
  of an ended epoch fires into nothing, dropped and counted (`Input::Timer`). One of the current
  epoch is stamped as it fires, and the session asks `nonces_for(CtxCall::Timer(tag))`, reserves
  exactly that many from the consumer's `NonceSource` (none for none) and calls `on_timer` with
  an `EncodeCtx` holding them and the firing's stamp for its time, so replay can hand the call
  the same context. A source that reserves another count ends the session with
  `ExecSessionError::Nonces`, as for `on_open` (0053), and the timer reaches no codec; one that
  fires while a write waits ends it as that write completes or is abandoned, and no later
  firing or result reaches the codec meanwhile.
- **The keepalive is the codec's own timer.** `ExecCodec` gets no `keepalive` declaration: as
  the design's `ExecCodec` has it (`on_timer`: "JWT refresh, keepalives, dead-man"), an
  order-entry codec arms a timer on open, and as it fires sends its keepalive frame with the
  class and rate charge it chooses and arms it again. The runtime sends it as any other frame:
  charged to the buckets and written within the write-stall window.
- **During a write, as 0036.** While a write waits on a peer that stopped reading, timers keep
  firing into `on_timer` and results keep coming back into `on_http`, and what they ask for
  joins the rest of the write's batch, as 0027 and 0036 have it for market data.
- **A stop ends the effects too.** Once the control has dropped, nothing reaches the codec and
  no effect is executed. The session core now stops executing a batch's effects once the
  control has dropped and reports the epoch ended, for market data as for order entry, so an
  effect a timer or a result asked for while a write waited is not executed after a stop that
  came as the write completed, and a request one asks for after the stop is not started, so it
  is never charged or journaled. What woke the session as the control dropped, and a frame
  waiting then, is stamped, so it keeps its place in ingest order, and reaches no codec.
- **No input starves another.** Between writes the session waits on its frames, its timers,
  its results and the stop in one unbiased `select!`, which polls them from a random one each
  time, and checks the stop after every wake, so a peer that keeps sending frames holds back
  neither a due timer, nor a result, nor the stop.

## Alternatives

- `ExecCodec::keepalive`, sent by the runtime at a declared interval as `MdCodec::keepalive`
  is: rejected for now. The design gives order-entry keepalives to `on_timer`, a codec's
  keepalive may need its context's time or a nonce (a signed ping, a dead-man refresh), and
  FBC-ja3 already plans the conformance toy's exec ping as a timer armed on open and on each
  firing. It costs the runtime a WebSocket-protocol ping, which an order-entry codec cannot
  ask for.
- Handing an ended epoch's HTTP result to the codec anyway, since the codec lives on: rejected.
  An answer of a connection's earlier life must not be read as this one's (0027); a request
  with an `rpc` whose result is dropped is settled by its deadline (`on_rpc_timeout`, FBC-0ga)
  as `Unknown`, never resent.
- Ending the session at once when a timer's nonces are mis-reserved during a write: rejected;
  the write under way is bounded by the write-stall window, and abandoning it early would leave
  what the peer took unknown for no gain.

## Consequences

- A codec can now refresh a token, a dead-man switch or its keepalive by a timer, and ask for
  REST reads; FBC-ja3's toy exec ping runs on this session unchanged.
- An order-entry request whose epoch ends before its answer is never reported by `on_http`;
  its outcome comes from the rpc deadline (FBC-0ga) or the next resync (FBC-w19).
- A stale timer or result is counted but not yet journaled (FBC-2pr).
- The order-entry session still has no silence alarm or rotation (0033): FBC-ai32.

## What would show this was wrong

- A venue whose order entry needs WebSocket-protocol pings sent by the client, which a codec's
  timer cannot ask for; it would want `ExecCodec::keepalive` in a new record.
- A venue that answers an order-entry request over REST in a way the next epoch must still
  read (an answer the resync cannot recover).
