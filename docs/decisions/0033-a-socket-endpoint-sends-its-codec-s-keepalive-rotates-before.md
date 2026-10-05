# 0033 — A socket endpoint sends its codec's keepalive, rotates before the venue's connection lifetime and reports a silent stream stale before reconnecting

Status: accepted
Date: 2026-10-04

## Context

FBC-djl gives 0023's market-data session the three things that keep a socket endpoint alive
without the consumer's help. `MdCodec::keepalive` states what a codec needs sent to stay
connected (a WebSocket ping or a frame of the venue's own) and how often;
`MdCaps::max_conn_lifetime` the longest a venue keeps a connection (Binance USD-M: 24 h); design
§3 gives the runtime a silence alarm, and BT-201's silence script needs it. On a silent stream
the owner accepted option (a) on 2026-10-03 (FBC-djl notes): report the stream stale to the
consumer and reconnect under a new epoch through FBC-ku8's pacing. A write that waits on a
peer that stopped reading is bounded separately, by a write-stall window (owner, 2026-10-04;
FBC-ha3), not by the silence window.

## Decision

- **Configuration.** `Liveness::new(silence, rotation_margin)`, carried by `MdSessionConfig`
  and `MdVenueConfig`, with no default in code (0009) and zeros refused. A margin not below
  the venue's declared lifetime is refused when a socket endpoint's session or the venue is
  built (`SessionError::Liveness`); a poll endpoint's session opens no connection to rotate and
  is not checked. The venue checks eagerly because it may plan a socket endpoint at any time,
  and a refusal then would end its run.
- **Keepalive.** Each epoch of a socket endpoint sends its codec's keepalive every declared
  interval from the epoch's open: a WebSocket ping (not journaled, like every control frame),
  or the codec's frame as an `Effect::Send` of Safety traffic, journaled as any frame is. Either
  is charged the keepalive's own rate charge to 0030's buckets, and one they refuse is not
  sent, as any other frame they refuse. A keepalive with a zero interval is a codec defect:
  refused, counted with the refused effects, and the epoch runs without one.
- **Rotation.** Where the venue declares a lifetime, the connection closes `lifetime − margin`
  after it opened and the next epoch opens at once: a planned close is not a drop, so it does
  not wait the floor, but its attempt counts against the attempt budget like any other. The new
  epoch's codec subscribes the desired set once, through the reconciler. The old connection is
  closed before the new one opens (break before make).
- **Silence.** A socket stream that receives no frame (pings, pongs and close frames
  included) within the silence window, counted from the epoch's open and from each frame
  read, is reported stale: one `MdEvent::Health { h: Stale }` per subscription wanted when
  the window runs out (the control's latest set, even one the session has not applied yet),
  all under one stamp of the silent epoch, handed to the consumer's handler. A write the
  handler issues through its `Outbox` as it is told is not sent, on the closing connection
  or the next, and is counted with the refused effects. Then the connection is
  closed and the session reconnects as after any drop, waiting the floor within the budget. A
  frame that was already waiting when the window ran out (a write held the session) counts as
  heard. A rotation due once the window has run out as well is a silence, not a rotation. A
  poll endpoint has no keepalive, rotation or silence window.
- The alarm and the rotation are not journaled as inputs of their own yet; the journal shows
  the connection's `Closed` and the next `Opened`.

## Alternatives

- Report stale only and leave recovery to the consumer, or reconnect without reporting: the
  owner accepted reporting and reconnecting (FBC-djl notes).
- Make before break (open the next connection while the old one still delivers): no gap in
  the data at a rotation, but two live epochs of one stream, which the epochs and reconciler of
  0002 do not model. Rotation happens once a day on Binance; the gap is one reconnect.
- Rotation waits the floor like a drop: a planned close would then leave a gap of at least the
  floor every rotation for no protection the budget does not already give.
- A runtime-only stale signal (a new handler call): `MdEvent::Health` already carries a
  codec's cadence staleness to the consumer (0014 item 7), so one path serves both.

## Consequences

Every consumer configures a silence window and a rotation margin for each session or venue.
A consumer that stops quoting on `Stale` sees one stale report per desired subscription when a
stream goes silent, before the reconnect, and the new epoch's data afterwards. Replay cannot
yet reproduce the stale reports, since the alarm is not journaled: a follow-up ticket journals
it. A test on tokio's paused clock that relies on the clock jumping must set a silence window it
never reaches (`Duration::MAX`), as it does for the attempt deadline.

## What would show this was wrong

A venue's data gap at a rotation that the consumer cannot tolerate (then make before break);
false stale reports on a healthy but quiet stream at windows the consumer needs (then a
per-feed window); or a consumer that needs the alarm in replay before it is journaled.
