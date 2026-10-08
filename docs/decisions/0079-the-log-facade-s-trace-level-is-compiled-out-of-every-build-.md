# 0079 — The log facade's TRACE level is compiled out of every build that links fbc-runtime, since tungstenite logs each frame at TRACE

Status: accepted
Date: 2026-10-07

## Context

0009 forbids a private key, JWT, API key or secret in a log. This workspace logs nothing
itself: it has no `tracing` or `log` call. What runs beneath fbc-runtime's WebSocket does:
tungstenite (0.30.0, under tokio-tungstenite, 0019) reports through the `log` facade, and at
TRACE it writes every frame it sends and receives into the record (`Sending frame: Frame {
.. payload: b"..." }`, `writing frame` with the payload in hex, `Received message ..`). A
consumer that installs any `log` logger at TRACE therefore gets the Paradex auth frame,
`{"method":"auth","params":{"bearer":"<session token>"}}`, verbatim, and every signed order
frame. FBC-8mv's offline rehearsal, which captures everything logged at TRACE while fbc-oms,
the order-entry session and the Paradex codec run against the conformance stub, found the
session token there on its first run. Nothing in this workspace can filter a record tungstenite
makes before the consumer's logger sees it.

## Decision

fbc-runtime depends on `log` (pinned exactly, already in the lockfile through tungstenite) with
its `max_level_debug` feature, and on nothing else of it. Cargo unifies a dependency's features
across a build, so in every build that links fbc-runtime, the consumer's included, the facade's
static maximum level is DEBUG: each `trace!` in any crate of that build compiles to nothing, and
no logger, whatever level it sets, can receive a TRACE record. What tungstenite logs at DEBUG
and above (handshake done, close frames) carries no frame payload.

## Alternatives

- Tell consumers never to enable TRACE for tungstenite's targets: a rule in a document, broken
  by the first logger set to TRACE while debugging a disconnect, and the token is then in a log
  file.
- `release_max_level_debug` only: debug builds, which an owner may run against testnet with
  real credentials, would still log every frame.
- Redact inside the runtime: the records are made inside tungstenite, before any code here sees
  them. Replacing or forking tungstenite to remove its logging is a far larger change than this.

## Consequences

- No crate in a build that links fbc-runtime logs at TRACE through `log`, the consumer's own
  `log::trace!` calls included. A `tracing` subscriber still receives `tracing`'s own TRACE
  events, which do not pass through the facade's static maximum.
- Proof: `crates/venues/fbc-venue-paradex/tests/rehearsal.rs` installs a logger at TRACE for
  the whole session and finds no token, login signature or key in what was logged;
  `crates/fbc-runtime/src/ws.rs`'s `the_log_facade_s_trace_level_is_compiled_out` fails if the
  feature is dropped.
- Moving tungstenite or `log` re-runs the rehearsal, which reads what the new version logs.

## What would show this was wrong

A record at DEBUG or above carrying a frame payload or a credential after a tungstenite or
hyper upgrade, which this level does not stop; or a consumer needing TRACE from `log` badly
enough to take the logging over by another route.
