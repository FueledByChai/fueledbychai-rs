# 0052 — An order-entry session drives one connection with one ExecCodec across its epochs, hands each stamped event to the consumer's handler inline, and ends its epoch at once on a stop, augmenting 0023

Status: accepted
Date: 2026-10-05

## Context

FBC-oaz builds the runtime's order-entry counterpart of `MdSession` (0023) on the shared
session core (FBC-e73): `ExecSession`, which opens what `VenueFactory::plan_exec` plans through
the consumer's `Connector`, so its SOCKS5 proxy applies (0002, 0019). Three things differ from
market data and were the ticket's to decide: how the consumer receives the execution events,
what one `ExecCodec` living across epochs means for an epoch's start and end, and how many
planned connections one session drives. Command submission (FBC-0ga), HTTP requests, timers and
keepalives (FBC-bnl) and journaling (FBC-2pr) come later and are not decided here.

## Decision

- **The handler.** The consumer implements `ExecHandler::on_exec(&mut self,
  Envelope<ExecEvent>)` (any `FnMut(Envelope<ExecEvent>)` is one), called once per event,
  synchronously, as the codec pushes it, on the thread that drives the session and before the
  next frame is read, as 0023's `MdHandler::on_md`. `ExecHandler::on_epoch_end(ConnKey)` (by
  default nothing) is called once per epoch, after every event of it the handler is given and
  before any of the next, as 0039's. There is no `Outbox` or tick-to-wire on it yet: what the
  consumer sends goes through FBC-0ga's submission, and FBC-qfm measures it.
- **Stamps.** Every frame, pings, pongs and close frames included, is stamped from the shard's
  `IngestClock` before it is decoded, and every event it yields carries that stamp, as 0023.
  Frames are decoded inside `fbc_core::dispatch` with the venue's own `VenueCaps` and the
  engine namespace the consumer configures (`ExecSessionConfig::ns`), so client ids and fees
  decode as `caps.exec` declares them (0015).
- **One codec, epochs.** The session builds its codec once, from the consumer's credentials
  (`exec_codec`), and keeps it for its life: it holds the account's state (requests waiting for
  answers, a resync being read) across reconnects. On each new epoch the session asks it
  `nonces_for(CtxCall::Open(stream))`, reserves exactly that many from the consumer's
  `NonceSource` (calling it not at all for none), and calls `on_open` with an `EncodeCtx`
  holding them and the shard clock's wall and monotonic time (0014 item 1). A source that
  reserves another count ends the session with `ExecSessionError::Nonces`: a nonce the codec
  did not ask for would sign nothing it can account for.
- **A session runs once.** `ExecSession::run` is called once; a later call returns
  `ExecSessionError::Ended` and connects nothing, so no rerun can bypass the pacing or stamp
  another connection's life with an epoch already used. A run that ends in an error retires
  its epoch; a run whose future is dropped while connected leaves its epoch to be ended by
  the next call or by the session's drop, and the handler is told it ended either way, except
  when the session is dropped as a panic unwinds: then its buckets are forgotten but the
  handler is not called, since a second panic would abort the process. A consumer that wants
  order entry again after an error builds a new session.
- **A stop ends the epoch at once.** Dropping the `ExecControl` stops the session, and the
  epoch ends there, even inside the handler: every event the codec pushes after it (the rest of
  a resync pushed whole, say) is of an ended epoch, dropped and counted
  (`ExecSession::stale(Input::Event)`), no effect asked for meanwhile is executed, and a frame
  already waiting is stamped but reaches no codec. A consumer that stops the session (a kill,
  an account it will no longer trade) hears nothing more of it, and the handler is told the
  epoch ended. 0023 delivers a market-data frame's remaining events after a stop; order entry
  does not, since a consumer stopping order entry must not act on what arrives after.
- **Effects.** Effects run in order, each only while the session has not stopped, so a stop
  from another thread ends the epoch before the next one. A frame for the session's own stream
  is charged to the consumer's `RateLimiter` and written within its `WriteStall` window (0030,
  0036); a reconnect of it ends the epoch and the next opens through the consumer's
  `ReconnectPacing` (0023). What `on_open` asks for is charged together before any of it is
  written: buckets that refuse it for now end the epoch as a drop, so `on_open` (which may have
  changed the codec's state, as an authentication asked for) runs again on the next epoch
  rather than the codec believe it sent what it never did, and the next attempt starts no
  sooner than the moment the buckets would admit it as well as the pacing allows, so a refused
  open does not reconnect at every floor sending nothing; frames that can never fit together
  end the session (`ExecSessionError::OpenNeverFits`). A stop that came while `on_open` ran
  charges nothing. Nothing behind a reconnect of the
  session's stream is charged, since it is never reached. A frame or
  reconnect for another stream is a codec defect, refused and counted, and so, until FBC-bnl,
  is every `Timer` and `Http` effect: none reaches the core.
- **One connection.** A session drives exactly one planned order-entry connection. A plan of
  none (order entry over HTTP only, or none) or several is refused when the session is built
  (`ExecSessionError::Endpoints`), as is a venue that declares no `exec` block or builds no
  codec (`NoOrderEntry`). Paradex plans one; driving several is FBC-tnsf.

## Alternatives

- A fresh codec per epoch, as market data: rejected. The codec's per-request state must outlive
  a reconnect (FBC-0ga times out the requests in flight across one to `Unknown`), and
  `ExecCodec` takes the stream in each call so one codec can serve every epoch.
- Delivering the rest of a frame's events after a stop, as 0023: rejected for order entry; see
  the decision.
- Driving every planned connection now: deferred to FBC-tnsf. It needs one connection's epochs
  and pacing per endpoint and effects routed between them, which no venue built so far needs.
- Reserving nonces for `on_open` from a fixed count, or none: rejected. 0014 item 1 has the
  codec say how many each call needs, and replay must hand the call the same block.

## Consequences

- A consumer runs one `ExecSession` per account and gives it a nonce source, its engine
  namespace and a limiter shared with whatever counts against the same limits.
- An event pushed after the consumer's stop is lost to it; whatever it said is read again by
  the next session's resync (0013).
- The session does not call the codec's `resync`: running it once per epoch after
  authentication, before any place or amend, is FBC-w19's.
- A stop is checked before each effect; one that arrives from another thread while a write is
  already under way cannot unsend what the socket took, and is seen before the next effect.
- Until FBC-bnl, a codec that needs a timer or an HTTP request (a token refresh, a dead-man
  refresh) cannot run on this session; until FBC-0ga, nothing is submitted.
- Only `on_open`'s frames are charged together. A frame the codec asks for later (from
  `on_frame`, as a challenge's signed reply) is charged alone, as 0030 has it for market data,
  and one the buckets refuse is dropped unwritten and counted by the limiter while the codec is
  not told: the session could stay connected but never authenticated. FBC-m2xw decides whether
  such a refusal ends the epoch or reaches the codec, before FBC-0ga submits commands.

## What would show this was wrong

- A venue whose order entry needs two connections before FBC-tnsf lands.
- A consumer that needs the events of a frame it was handling when it stopped the session (a
  fill it must still record), which would want them delivered and the stop deferred to the
  frame's end.
