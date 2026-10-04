# 0025 — Conformance fault scripts are typed Rust values played by a public stub server; a text script format is deferred

Status: accepted
Date: 2026-10-04

## Context

FBC-jcy builds the first part of the conformance kit (design §6, story BT-502): a stub venue
server that plays the failures that hurt the Java stack against `fbc-runtime`, starting with the
reconnect storm (340 reconnects in 8 minutes, which desynced Hibachi's order map and stacked
duplicate subscriptions). Design §3 names a `.cwscript` fault runner but gives it no grammar.
Venue crates are to reuse the stub from their tests (design §6 step 12) rather than write
servers of their own, as `fbc-runtime`'s tests did so far.

## Decision

- **Scripts are typed Rust values.** A `WsScript` is one timeline of `Step`s (accept, read,
  push, close, go silent), each naming the connection it acts on by accept order, so one script
  can drive and interleave several connections. A script is built in the test that plays it, or
  by a function such as `reconnect_storm`. A text format (`.cwscript`) is deferred: no parser,
  grammar or file is written until a script has to come from outside Rust code.
- **The stub server is public, in `crates/fbc-conformance`,** which depends on `fbc-runtime`
  (and on `fbc-core` in its tests) and never on a venue crate; venue crates take it as a
  dev-dependency. It binds a WebSocket and an HTTP/1.1 endpoint on 127.0.0.1 ephemeral ports,
  records every accepted connection (its accept instant on tokio's clock, so paused-time tests
  measure pacing exactly) and every data frame, and answers fixed HTTP responses by path. Its
  HTTP side is hand-written, so no server feature of hyper is added (0019's pins stand).
- **Pacing is checked from the server's side:** `check_pacing` holds the attempts a stub saw
  to the client's `ReconnectPacing` (0023): consecutive attempts at least the floor apart, and
  no more than the budget in any half-open window.

## Alternatives

- A text script format now: rejected. Its grammar would be designed before a second script
  exists, it needs a parser and its own errors, and typed values let the compiler check every
  script.
- Each venue crate keeps its own test servers: rejected. It is the duplication design §6
  step 12 removes, and each copy would drift from the faults the runtime is held to.
- hyper's server for the HTTP side: rejected for now. A fixed response by path needs a few
  dozen lines, and the extra feature would widen 0019's pinned surface for a test tool.

## Consequences

- Fault scripts are written in Rust by whoever writes the test; a non-programmer cannot edit
  one without a text format.
- The stub records frames as data (pings, pongs and closes are not recorded), and a script
  reads frames without matching them; tests assert on the record afterwards.
- Later scripts (duplicate acks and silence, FBC-53c) and the named conformance suite extend
  `Step` and this crate rather than a test's own server.

## What would show this was wrong

A fault that a sequence of these steps cannot express (one that needs byte-level control below
WebSocket framing, or timing within a step), or a need to share scripts with a tool outside
Rust, either of which would want a text format or a lower-level stub in a new record.
