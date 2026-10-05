# 0034 — A codec marks its latency stages through write-only PathStamps and the runtime reads the clock

Status: accepted
Date: 2026-10-04

## Context

Design §4.7 passes `t: &mut PathStamps` to `ExecCodec::encode` and `OrderGateway::submit`, so
that the stages of the latency budget (design §5.3 and §11: the encode, the signer call inside
it, the socket write) are timed per command and kept with its record. But a codec reads no
clock (0002), and every time in a payload comes from a journaled `EncodeCtx` (0006), so that
replay reproduces the bytes. Handing a codec something that reads a clock, or that gives back
the time it recorded, would let a payload depend on an unjournaled time. FBC-5 left the
parameter out for that reason (0014). The design does not say what `PathStamps` holds.

## Decision

A codec marks a stage boundary; it never learns when that was.

- `fbc-core` (`src/stamps.rs`) defines the stages `PathStage::{Encode, Sign, Write}`, their
  ends `PathEdge::{Start, End}`, a `PathMark` of the two, and a trait `PathRecorder` whose one
  method, `mark(&mut self, PathMark)`, returns nothing. The runtime implements it: at each mark
  it reads its own clock and keeps the instant with the command's record, which it journals.
- `PathStamps<'a>` is a write-only handle on a `&mut dyn PathRecorder`, or off
  (`PathStamps::off()`, for a caller that times nothing). It has `start`, `end` and `span`
  (marks the start, runs a closure, marks the end whatever the closure returned) and no
  accessor: its recorder field is private, it holds no time and returns none, and nothing about
  it, its `Debug` included, shows whether it is on, so a codec cannot tell live from replay.
- `ExecCodec::encode` takes `t: &mut PathStamps<'_>` after its `EncodeCtx`, and
  `OrderGateway::submit` takes one last, as the design has them. Who marks what: the gateway
  marks `Encode` around its call to `encode`, the codec marks `Sign` around each signer call (a
  batch marks one per signed item, in call order), and the runtime marks `Write` around the
  socket write. Less its `Sign` stages, `Encode` is design §5.3's `pre_send`.
- Nothing in `fbc-core` reads a clock. Replay hands the codec a recorder of its own or
  `PathStamps::off()`, and the bytes are the same whatever was recorded.

This closes 0014's last item left out of FBC-5 (`PathStamps`, FBC-ji6). Where the runtime keeps
the recorded instants (design §11 puts `PathStamps` on the command record) and how the journal
writes them is the runtime's submit path's choice, made with it.

## Alternatives

- `PathStamps` as a value holding the instants, filled by the codec from a clock it is handed:
  rejected. The codec would read a clock, and could put what it read in a payload.
- `PathStamps` holding the instants the runtime recorded, with getters: rejected. Any public
  getter is callable by a codec, which could then read the time of its own earlier mark.
- Timing only around the whole `encode` call, outside the codec: rejected. The signer call is
  the stage the budget most needs (300–450 µs on Paradex against a 0.6 ms amend target,
  design §5.3), and only the codec knows where it starts and ends.
- A recorder that returns the instant from `mark`: rejected for the same reason as the getters.

## Consequences

- Every exec codec marks its signer calls; the conformance toy (FBC-7lx) and Paradex's order
  entry (FBC-xe1) do so, and a test can prove the bytes are the same whatever the stamps
  recorded.
- A mark is a virtual call on the hot path: two per signed item, cheap beside a signature.
- The stamps give no item index: a batch's `Sign` marks pair by order (each `Start` with the
  next `End`).

## What would show this was wrong

- A latency stage the budget needs that falls inside an encode but outside a signer call, which
  `Encode` less `Sign` cannot separate.
- Telemetry that needs per-item sign times in a batch the order of marks cannot recover.
- Encoded bytes found to differ with what the stamps recorded.
