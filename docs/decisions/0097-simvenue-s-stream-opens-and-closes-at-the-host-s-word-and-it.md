# 0097 — SimVenue's stream opens and closes at the host's word, and its engine cancels what the stood-in venue cancels on a disconnect or a lapsed dead-man timer, augmenting 0046

Status: accepted
Date: 2026-10-18

## Context

0046's simulated order-entry stream had no disconnect: `SimCodec::on_open` did nothing and
`SimEngine` took no close, so a session over SimVenue standing in for a venue whose
`ExecCaps.order.cancel_on_disconnect` is `PerConnection` or `DeadMan` would keep orders live,
and fillable, past a disconnect the venue would cancel them on (Codex r4184778435 on PR #58).
0046 therefore refused placements for such a venue (FBC-fji). An order-entry session takes
only per-connection venues and places behind an accepted `ArmCancelOnDisconnect(true)` on
every epoch where the protection lapses with the connection (0058), and Paradex declares
`PerConnection { rearm_on_reconnect: true, covers_open_orders: false }`, so a host running
SimVenue for Paradex (FBC-6mf) cannot place through it until the engine models the lifecycle.

## Decision

- **The stream's lifecycle is the host's.** `SimEngine::open(now)` and
  `SimEngine::close(now, wall)` say when the simulated stream opens and closes, at stamps the
  host read, since the engine reads no clock and `ExecCodec` has no close call (0014,
  unchanged). A new engine's stream is closed. While it is closed the engine takes no frame
  (`SimError::StreamClosed`); a second open is `SimError::StreamOpen`. Each acts after every
  command that arrived by its instant, as an envelope does.
- **A close loses what is in flight.** Every frame written on the closing connection that has
  not yet arrived at the venue never acts and is never answered, so the runtime's deadline
  reports it `Unknown` (0005), for every venue.
- **Per connection.** The codec writes `ArmCancelOnDisconnect(on)` as a control frame naming
  no instrument, answered as one outcome for the whole request. An accepted arm protects every
  order placed after it, and also the orders open then where the venue declares
  `covers_open_orders`; a close cancels every protected order (`CancelReason::Venue`), each
  answered `to_client` after the close. Turning it off unprotects every order. Where
  `rearm_on_reconnect`, the protection ends with the connection, so nothing placed on the next
  one is protected until it is asked for again; otherwise it covers every later connection
  until turned off.
- **Dead-man.** The codec writes `ArmCancelOnDisconnect(on)` and `RefreshDeadMan`. An arm starts
  a timer of the venue's `max_ttl`, the longest it accepts, since `RefreshDeadMan` names no
  length; a refresh restarts it from its arrival; turning it off stops it. Unrefreshed for
  that long it lapses, after every command that arrived by its instant (a refresh arriving at
  the deadline is in time) and before any envelope stamped after it, and the venue cancels
  every order SimVenue holds (`CancelReason::Venue`). A close alone cancels nothing there: the
  timer lapses once refreshes stop.
- **Refusals.** The codec refuses a protection command the venue's caps do not declare
  (`NotSent(Unsupported)`), as the conformance suite holds every codec to. A frame carrying one
  anyway, or a refresh with no timer running, is refused by the engine (`no_protection`,
  `RejectKind::Other`) and arms nothing.
- **Placements go.** The codec no longer refuses placements and amends for a venue that cancels
  on disconnect; the other refusals of 0046 stand. Injected orders are another process's and
  never end on SimVenue's protection.

## Alternatives

- **An `on_close` call on `ExecCodec`, or a close frame the codec writes**: rejected. It
  changes every exec codec's boundary for one (0014, as 0046 refused an `on_market` call), and
  a codec cannot write on a stream that has closed.
- **A stream that starts open**: rejected. The host opening the stream on the session's open,
  as a socket opens, is the same call on the first epoch as on every later one; an engine that
  starts open needs a different first call.
- **Letting in-flight commands act after the close**, as placed unprotected or cancelled at
  once: rejected. Either guesses at bytes the venue may never have read; losing them is what
  the client sees in both cases (no answer on a connection that is gone), and its deadline
  already reports that as `Unknown`.
- **A dead-man timer length in `SimConfig`**: not taken. `RefreshDeadMan` carries no length,
  no planned venue uses a dead-man timer (0058), and `max_ttl` is the stood-in venue's own
  number from the consumer's caps; a refresh carrying a length would bring one.
- **A refresh with no timer running arming one**: rejected; it would hide a consumer that
  never armed, and the conformance suite's caps already separate the arm from the refresh.

## Consequences

- FBC-6mf's host calls `open` when its simulated stream opens and `close` when it drops, and
  can place through SimVenue for Paradex's caps behind the session's arm (0058).
- The venue's cancellations at a close are answered after it, like any answer; a host
  delivers them on the next connection or lets the next resync show the orders ended (0080's
  unprotected-order gate still covers what a resync shows resting).
- Every host and test opens the stream before writing a frame.

## What would show this was wrong

A stood-in venue that processes, then cancels, the commands it read just before a disconnect,
so a replay or shadow shows fills the simulation lost; or one whose dead-man timer a disconnect
fires at once, rather than once refreshes stop.
