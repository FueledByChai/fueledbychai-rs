# 0049 — SimVenue answers a resync with its resting orders and the positions its fills imply, augmenting 0046

Status: accepted
Date: 2026-10-05

## Context

0046 left `SimCodec::resync` writing nothing (FBC-bq3): the engine kept its resting orders and
fills, but no resync answer existed. A host sends nothing after a restart until the first
resync has landed (0013), and every resync is reported as the `Resync*` events of 0014's
`ExecCodec::resync`, so a host running SimVenue in place of a venue needs that answer from it
too.

## Decision

- **The request.** `SimCodec::resync` writes one frame on the simulated stream, stamped with
  the call's `EncodeCtx` wall and monotonic time. It is a read: it names no RPC and awaits no
  deadline, is labelled `TrafficClass::Safety`, and charges one `OpKind::Query` against no
  instrument. One resync is in flight at a time: while one is unanswered, another call writes
  nothing. The simulated stream never loses a frame, so the answer always comes.
- **The answer.** The engine acts on the request like any command, at its encode time plus
  `to_venue` (0046), and answers `to_client` later in one frame, under one sequence number:
  the request's wall time as the watermark, one record per order SimVenue placed that rests at
  that instant (injected orders, FBC-nv2's, are another process's and never listed) with its
  cumulative fill, and one position per instrument SimVenue has a fill in: the signed sum of
  its fills, buys positive, taker and maker alike, still reported when it nets to zero.
- **Decoding.** The codec decodes that frame whole before pushing anything, through
  `DecodeScope`, into `ResyncBegin`, a `ResyncOrder` per order (open, at its limit price, its
  client id and flags only where the stood-in venue echoes them on events, as 0046's order
  events), a `ResyncPosition` per position (no entry price: the engine keeps none) and
  `ResyncEnd`, all in one call. It refuses as malformed an answer when none was asked, one
  whose watermark is not the request's wall time, and a position past an `i64` of lots (the
  engine writes the sum as an `i128`, never wrapped); a refused answer leaves the resync asked.

## Alternatives

- **One frame per record** (a begin, the orders, the positions, an end, each with its own
  sequence number): rejected. The codec would have to hold a half-read resync across frames
  and refuse records out of place; one frame decoded whole cannot be half applied, which is
  how the conformance toy venue answers too.
- **The watermark as the instant the venue acted** (the request's wall time plus `to_venue`):
  not taken. The ticket fixes the request's wall time, which precedes what the snapshot shows;
  a fill between the two is due before the answer, so it arrives before the begin.
- **A retry timer**, as the toy venue sets: not needed while the simulated stream cannot drop a
  frame; a host that can is FBC-6mf's to answer for.

## Consequences

- The engine tracks a position per instrument from every fill it answers, taker and maker.
- A position is reported only for instruments SimVenue has a fill in; one never traded is not
  listed.
- Reduce-only is still echoed, not enforced (0046), though the engine now keeps a position.

## What would show this was wrong

A host that needs a resync answered while another is in flight, or that loses the simulated
stream's frames (FBC-6mf); or a runtime that reads an instrument missing from the resync as
something other than flat.
