# 0029 — The WebSocket opening handshake is the runtime's own over hyper, with a token-aware Connection check

Status: accepted
Date: 2026-10-04

## Context

RFC 6455 §4.1 requires only that the server's upgrade response carry a `Connection` header
containing an `Upgrade` token, so `Connection: keep-alive, Upgrade` is valid. 0019 opened
WebSockets with tokio-tungstenite's `client_async`, and tungstenite 0.30.0's client handshake
compares the whole `Connection` value to `Upgrade`, so it fails such a response; 0019 recorded
this as a known gap and FBC-17h tracks it. Upstream tungstenite's main branch still compares the
whole value (checked 2026-10-04), so moving the pin does not fix it.

## Decision

- `Connector::websocket` makes the opening handshake itself, on the connector's stream: hyper's
  HTTP/1.1 client connection (0019's pin, title-case header names) sends the upgrade request
  in origin form with a Host header from the URL (never its user information), a fresh
  `Sec-WebSocket-Key`, and no subprotocol or extension, and reads the response head.
- The response is checked as RFC 6455 §4.1 says a client must: status 101 (another status is
  `Cause::Status`), `Upgrade: websocket` case aside, an `Upgrade` token among the
  comma-separated tokens of any `Connection` line, case aside, the `Sec-WebSocket-Accept`
  derived from the key, and no `Sec-WebSocket-Protocol` or `Sec-WebSocket-Extensions`, since
  the request asks for none. Every failure names `Step::WebSocketUpgrade` and never the URL
  (0014 item 8).
- On success hyper hands the stream over (`hyper::upgrade`) with any bytes it read past the
  response head, and tokio-tungstenite takes it from there
  (`WebSocketStream::from_partially_read`, client role), so a frame the server sent in the same
  write as its 101 is not lost. tokio-tungstenite keeps its 0019 pin and still frames the
  connection; tungstenite supplies the key and accept derivation.

## Alternatives

- Wait for tungstenite to check tokens, then move the pin: rejected for now; upstream still
  compares the whole value, and a venue or intermediary could send a token list first.
- Parse the response head by hand (or with `httparse` directly): rejected; hyper already
  parses HTTP/1.1 heads for the HTTP call, and its upgrade hands back the over-read bytes.

## Consequences

- The WebSocket and HTTP paths share hyper's HTTP/1.1 parsing; no dependency is added.
- tungstenite's own client-handshake checks no longer run, so this runtime owns the RFC 6455
  §4.1 response checks and their tests (`src/ws.rs`).
- A consumer that needs a subprotocol or an extension needs a ticket that adds it to the
  request and lets the check accept what was asked.

## What would show this was wrong

- A venue whose upgrade succeeds with tungstenite's handshake but fails this one (a header or
  response form hyper refuses).
- tungstenite gaining a token-aware client handshake, at which point handing the handshake back
  to `client_async` removes code here.
