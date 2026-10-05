# 0047 — The stub answers what it reads: a script step pushes frames a function computes from the frame read, and HTTP rules match method and path pattern, augmenting 0025

Status: accepted
Date: 2026-10-05

## Context

0025's stub plays fixed frames, answers fixed HTTP responses by path, and reads frames without
matching them. Order entry needs replies correlated to the request: a JSON-RPC reply echoing
the request id, a batch's outcome per item, an authentication or cancel-on-disconnect
acknowledgement, an order query by client id. Every order-entry conformance check and fault
script waits on that (FBC-xg7, story BT-502).

## Decision

- **A script step computes its reply from the frame it reads.** `Step::Respond { conn, with }`
  waits for the connection's next data frame, as `Read` does, and pushes, in order, the frames
  its `Responder` returns for it (none is allowed). A `Responder` is a typed Rust function
  (`Fn(&Frame) -> Result<Vec<Frame>, String>`, scripts stay typed values as 0025 has them). A
  refusal (`Err`) or a panic in it fails the script with `ScriptError::Respond`, naming the step
  and connection, so a responder that could not answer never reads as a finished script.
  Responders compare equal only when they share one function, so `Step` keeps its equality.
- **HTTP is answered by rules** in an `HttpRouter`, tried in the order added; the first whose
  method and `PathPattern` match answers, with a fixed `HttpReply` or one a function computes
  from the `HttpRequest` (method, path, query, headers, body, captured segments). A pattern is a
  prefix on a segment boundary or a template of literal and `{name}` segments, each variable
  matching one non-empty segment; paths are compared as sent, never percent-decoded. A request
  no rule matches is answered 404. 0025's `HttpRoutes` map converts to one exact-path rule per
  entry answering any method, so its scripts and calls run unchanged.
- **Bodies are read by Content-Length only.** A request naming a transfer coding is answered
  400, never handed to a function with its body missing. The HTTP side stays hand-written, with
  no server feature of hyper (0019's pins stand).
- A scripted venue state (orders the stub remembers between requests) is not built: a script
  states its replies, and a function that needs state keeps it itself.

## Alternatives

- Match each read frame against an expected frame and fail on a mismatch: rejected; a reply
  must carry what the request chose (an id, a batch's items), which a fixed expectation cannot
  supply.
- A plain `fn` pointer for the responder: rejected; a reply that numbers orders or remembers a
  session key needs captured state.
- Answer a method that matches no rule on a matching path with 405: not taken; no check needs
  it, and 404 for every unmatched request keeps one rule.
- hyper's server for routing and bodies: rejected again, as in 0025.

## Consequences

- Order-entry conformance checks and fault scripts (FBC-3il, FBC-y6y, FBC-6to, FBC-xx0,
  FBC-e5p, FBC-8mv) write their replies as functions in the test, parsing the venue's own
  request format there; the stub knows no venue protocol.
- A chunked request body cannot be served until a check needs one.

## What would show this was wrong

A check whose replies depend on state across requests that every test re-implements, which
would want a scripted venue state in the kit, or a client that must send chunked bodies.
