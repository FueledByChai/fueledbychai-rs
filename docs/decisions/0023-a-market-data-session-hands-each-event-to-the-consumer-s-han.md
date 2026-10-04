# 0023 — A market-data session hands each event to the consumer's handler inline and paces reconnects by consumer-configured backoff and attempt budget

Status: accepted
Date: 2026-10-04

## Context

FBC-ku8 builds `fbc-runtime`'s first session: `MdSession` drives one `MdTransport::Socket`
endpoint of a venue's plan through 0002's connection epochs and reconciler, a fresh `MdCodec`
per epoch, and the codec's effects. Two choices were the ticket's to record.

First, how the consumer receives the stamped envelopes. Design §4.8's `Engine::on_input` belongs
to the consumer's engine, which lives in the private consumer (0001); design §5.1 has one shard
thread read, decode and apply in one call stack. The Java library's cached thread pools and an
unbounded dispatch queue (1.7M queued tasks behind duplicate subscriptions) are the failure 0002
exists to fix.

Second, how reconnects are paced. The deployment box shares one egress IP with the Java
processes, and Binance USD-M limits new connections per IP (BT-201). A dropping endpoint must
not hammer that IP, while a single drop should still reconnect quickly. The owner accepted
option (a) on 2026-10-03 (FBC-ku8 notes): exponential backoff between a configured floor and
ceiling plus an attempt budget per window, every number from the consumer. The Codex review
of pull request #23 (r4177068887) added a deadline per attempt, since a connect or upgrade
that never answers would otherwise hold the session in one attempt indefinitely.

## Decision

- **The handler.** The consumer implements `MdHandler::on_md(&mut self, Envelope<MdEvent>)`
  (any `FnMut(Envelope<MdEvent>)` is one). The session calls it once per event, synchronously,
  as the codec pushes it (0014 item 2), on the thread that drives the session and before the
  next frame is read. No queue or task stands between decode and the handler, so events arrive
  in ingest order and a slow handler slows the reads rather than growing a backlog. The
  consumer's engine adapts `on_md` to its own `on_input`.
- **Stamps.** The runtime stamps each input (a frame, a timer firing) once, before decode, and
  every event that input yields carries that stamp; a timer firing of an ended epoch is stamped
  too, so it keeps its place in ingest order though it is dropped: `ingest_seq` from an `IngestClock` that the
  sessions of one shard share (cloning shares it), `recv_mono` from that clock's origin,
  `recv_wall` from the system clock, `conn` the session's connection number and current epoch,
  `kernel_rx` `None` until FBC-2y3.
- **Control.** `MdControl::set_desired` replaces the desired set through a watch channel that
  holds only the latest set, so nothing queues; the reconciler sends only the difference.
  Dropping the control stops the session.
- **Effects.** `Send` and `Reconnect` for the session's own stream are executed in order (a
  frame whose bytes are UTF-8 goes as a text frame, any other as binary); `Timer` fires into the
  epoch that set it, and into nothing once that epoch has ended (counted as stale). A frame or
  reconnect for another stream, and `Http` (FBC-klr), are codec defects: refused and counted.
- **Reconnect pacing.** `ReconnectPacing::new(floor, ceiling, budget, window, deadline)`, with
  no default in code and zeros refused. An attempt that has not opened within `deadline` is
  abandoned and counts as failed. The first attempt goes at once. After an open connection drops,
  the next attempt waits the floor; each attempt that fails to open doubles the wait, capped at
  the ceiling; a successful open starts again from the floor. Independently, no more than
  `budget` attempts start in any half-open window of `window`. A codec's `Reconnect` is paced
  the same way. A wait past the end of the clock never ends (the session then waits only for
  its control) rather than panicking. A URL no attempt could open (not `ws://` or `wss://`, no
  host, no TLS server name) is refused when the session is built, not retried.
- **Each epoch's codec** is built from the endpoint plan with the subscriptions wanted at that
  moment, so a codec that reads its plan's `subs` sees the set it will subscribe.
- **Thread and features.** `MdSession::run` spawns no task; it runs on the caller's
  current-thread runtime (design §5.1). `fbc-runtime` turns on tokio's `sync` and `time`
  features (and `test-util` for tests) and takes `futures-util` as a normal dependency, all
  already pinned in the workspace (0019).

## Alternatives

- A channel of envelopes to the consumer: rejected. It is the queue 0002 removes; it decouples
  the reader from the consumer's pace and lets a backlog grow.
- Handing the consumer a whole frame's events as a batch: rejected for now. 0014 item 2 has the
  runtime forward pushes as they come, and a batch would need a buffer per frame.
- A fixed configured reconnect delay (option b): rejected. It cannot both reconnect a single
  drop quickly and bound a storm against the shared IP.
- No pacing in the library, the consumer throttles (option c): rejected. Every consumer would
  have to rebuild it, and the runtime owns reconnects (0002).

## Consequences

- A handler that blocks stalls its session's reads; the consumer keeps `on_md` short.
- A server that accepts and immediately drops still gets reconnects at the floor, bounded by the
  budget; silence and keepalives are FBC-djl's.
- The consumer configures five pacing numbers per session; replay (0006) needs none of them.

## What would show this was wrong

A venue whose connection limit the budget cannot express (for example a limit across sessions of
one process, where per-session budgets add up), or a consumer that needs events off the shard
thread, which would want a shared pacer or a hand-off of its own, in a new record.
