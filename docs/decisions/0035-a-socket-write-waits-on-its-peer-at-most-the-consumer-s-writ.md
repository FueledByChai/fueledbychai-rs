# 0035 — A socket write waits on its peer at most the consumer's write-stall window, and timers due meanwhile fire

Status: accepted
Date: 2026-10-04

## Context

In 0023's market-data session a frame write waits on the socket under backpressure. A peer that
stops reading holds that write, and with it the session, until the control drops: nothing else
ends it, and the epoch's timers that fall due meanwhile (`on_timer`, its stale reports and any
reconnect it asks for) wait with it and are stamped late in the shard's ingest order (Codex
r4177260037 on PR #23; FBC-ha3). The attempt deadline (0023) bounds only an attempt to open,
and the silence window (0033) only what is received. The owner chose, on 2026-10-04 (FBC-ha3
notes), a separate write-stall window from the consumer's configuration over reusing either.

## Decision

- **Configuration.** `WriteStall::new(window)`, carried by `MdSessionConfig` and
  `MdVenueConfig` beside the reconnect pacing and the liveness settings, with no default in
  code (0009) and zero refused. A window past the end of the clock never runs out.
- **Bound.** A write not completed `window` after it began is abandoned and counted
  (`MdCounters::write_stalls`); the epoch ends as a drop, so the session reconnects through the
  pacing, waiting the floor within the budget. The abandoned write has no write result in the
  journal, as a failed one has none; the connection's `Closed` follows. The socket is dropped,
  not sent a close frame its peer would not read. A write the session finds completed when it
  next runs counts as completed, even if that is after the window (a starved or suspended
  task): its peer took the frame, and dropping a connection that just did would reconnect a
  healthy stream for the session's own latency.
- **Timers.** While a write waits, the session's timers fire as they fall due: each is stamped
  then, so it takes its place in ingest order, an ended epoch's into nothing, a current epoch's
  into its codec. The effects the codec asks for join the rest of the batch, as an HTTP result's
  that comes back during a write do (0027). A timer due no later than the window fires first,
  even when the session runs again only after both are past; one due after the window does not
  fire into the codec (the epoch ends at the window, and it fires into nothing). Keepalives, rotation and the silence alarm (0033) still wait for the write.

## Alternatives

- Bound the write by the attempt deadline: no new number, but an attempt to open and a write
  under backpressure are different waits, and the owner chose a number of their own.
- Bound it by the silence window: a write can stall while frames still arrive, and the silence
  alarm counts a frame waiting behind a write as heard (0033).
- Fire timers only after the write ends, bounded tightly enough: their stamps would still lag
  their deadlines by up to the window.
- Hold a write the session finds completed only after the window to the window and end the
  epoch (Codex r4180686922): the window would then also bound the session's own scheduling,
  and a suspended process would drop every healthy connection it resumes on.

## Consequences

Every consumer configures a write-stall window for each session or venue. A peer that stops
reading costs at most the window plus the reconnect pacing, and a codec's timers keep their
times meanwhile. A test on tokio's paused clock that lets the clock jump while a write is held
must set a window it never reaches (`Duration::MAX`), as it does for the attempt deadline and the
silence window.

## What would show this was wrong

A venue whose large writes legitimately take longer than any window the consumer can tolerate
for a stalled peer (then a window per traffic class or per frame size); or a keepalive or
silence alarm that must act during a long write (then they join the timers that fire meanwhile).
