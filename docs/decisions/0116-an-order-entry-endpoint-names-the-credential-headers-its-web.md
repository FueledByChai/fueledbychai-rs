# 0116 — An order-entry endpoint names the credential headers its WebSocket handshake carries, read from Secrets and sent on every connection, and the URL's account id is a redacted span the runtime never records, augmenting 0014 and 0029

Status: accepted
Date: 2026-10-20

## Context

Hibachi's private sockets (`/ws/account` and `/ws/trade` on api.hibachi.xyz) authenticate in
the opening handshake: a raw `Authorization: <apiKey>` header, with no `Bearer` and no request
signature, and `?accountId=<id>&hibachiClient=<tag>` in the URL; `/ws/market` is anonymous
(the public SDK's api_ws_trade.py and api_ws_account.py, the Java library's
HibachiWebSocketClient.java). fbc-runtime's upgrade request (0029) carried only Host,
Connection, Upgrade and the Sec-WebSocket headers, and `ExecEndpoint` was only `{stream, url}`,
so no venue could put a credential in a handshake. The owner, 2026-10-19, chose "MD + signer +
runtime prep (Recommended)" for the Hibachi round, the runtime's WS auth headers among them,
and a separate Hibachi account and key for the Rust stack. FBC-j8a4 adds the headers. 0009 puts
credential code on a review path (`src/auth`), 0043 hands credentials to a session only as
`Secrets`, and 0024/0028 keep them out of the journal as keyed hashes.

## Decision

This augments 0014 (the venue boundary) and 0029 (the handshake); it supersedes nothing.

1. **The endpoint names them; the value comes from `Secrets`.** `ExecEndpoint` gains a
   required `handshake: Vec<HandshakeHeader>`, and `HandshakeHeader { name, secret }`
   (fbc-core `src/auth`) names a header and the `Secrets` key whose value it carries, as it is,
   nothing added before or after it. A venue that authenticates in its frames (Paradex), or
   takes nothing, plans it empty. A value is never read from configuration or written into a
   URL. A market-data endpoint (`EndpointPlan`) has no such field: its handshake carries none.
2. **Read once, before the codec gets the credentials.** As an order-entry session is built,
   for each planned endpoint, the runtime (`crates/fbc-runtime/src/auth.rs`, on the
   `crates/*/src/auth` review path) checks each named header and copies its value into a
   `Secret` of its own, zeroed when the session drops; the venue's `exec_codec` then gets the
   whole `Secrets`, so a codec that also needs the key (to sign, or for an HTTP request) still
   has it. A session over an in-process stream (0104) opens no socket and reads none.
3. **Refusals name the key or the header, never the value.** A key the endpoint names that
   the credentials lack is `VenueError::Config(ConfigError::Missing(key))`, as 0043 has it for
   a codec; a value no header can carry (a line break, a NUL) is `ConfigError::Invalid` under
   that key, which also refuses a value that would smuggle a second header into the request; a
   name that is not a header name, one the handshake sets itself (`Host`, `Connection`,
   `Upgrade`, any `Sec-WebSocket-` header) or one that frames a body (`Content-Length`,
   `Transfer-Encoding`), or a header named twice on one endpoint, is a venue defect,
   `ExecSessionError::HandshakeHeader { stream, name }`. Each refuses the session before
   anything is opened.
4. **Sent on every connection.** Every connection attempt to the endpoint, a reconnect's and
   one through the consumer's SOCKS5 proxy included, and every endpoint 0115 plans after the
   first, adds the headers to its upgrade request, each value marked sensitive (`HeaderValue::set_sensitive`), so the request's `Debug` shows
   `Sensitive` in its place. `Connector::websocket`, the public call, still sends no
   credential header.
5. **Nothing records a value.** The journal records no handshake request (as before this
   record; the codec never sees the handshake, so replay needs none of it); were one recorded,
   its `Authorization` header is one of fbc-journal's `SECRET_HEADERS` and would be written as
   a keyed hash. A failed upgrade is a `NetError` with `Step::WebSocketUpgrade` and a status or
   protocol cause, never a header, a response body or the URL (0029); the runtime logs nothing
   of its own, and hyper's and tungstenite's TRACE records are compiled out (0079). The
   runtime's copies list as `[authorization: <redacted>]`.
6. **The URL's account id is private account data, not a credential.** Hibachi's `accountId`
   identifies the owner's account (0009) but authenticates nothing, so it comes from the
   venue's configuration, as Paradex's account does, not from `Secrets`. The venue's factory
   marks it as a redaction span of the endpoint's `WireUrl`, so `ExecEndpoint`'s `Debug` shows
   it by length (0014 item 8). The runtime writes no URL into the journal, a log or an error,
   so the account id appears nowhere else, and there is nothing to hash.
7. **Not zeroed on the wire path.** The bytes hyper encodes the request into, and the
   `HeaderValue` built for each attempt, are freed without zeroing; zeroing credential-bearing
   outbound bytes stays FBC-96h, deferred, as it is for frames and HTTP requests.

## Alternatives

- The codec adds the headers (an `ExecCodec` call returning the handshake's headers per
  epoch): the codec would keep the key and hand it back on each connect, a second path for the
  value through the boundary, and the runtime still needs code under `src/auth` to put it on
  the request. A static naming in the plan is checked once, before anything is opened.
- Header values as `WireSlice`s with spans, like `HttpRequest` headers: the endpoint is planned
  from configuration alone (`plan_exec(cfg)`), which has no credentials, and its value would
  then sit in a cloneable plan.
- The runtime takes the value out of `Secrets` rather than copying it: Hibachi's codec needs
  the same key for its HTTP requests, and `Secrets` cannot be cloned (0043), so the consumer
  would have to hand the key over twice under two names.
- A prefix in `HandshakeHeader` (`Bearer `): no planned venue needs one; Hibachi's header is the
  raw key. A venue that does adds it in a new record.
- The account id in `Secrets` too, so it never sits in configuration: it is not a credential,
  configuration already holds account addresses, and a URL built from configuration keeps the
  plan a pure function of `cfg`.
- Journaling the handshake request with its credential headers hashed: a new journal record
  and format version for bytes neither replay nor any reader needs; not taken.

## Consequences

- Every `ExecEndpoint` states its handshake headers; Paradex, the conformance toy and the test
  venues plan none.
- Hibachi's order-entry factory plans its `/ws/trade` and `/ws/account` endpoints with
  `HandshakeHeader { name: "Authorization", secret: <its api-key schema key> }` and the account
  id as a URL span; its market-data endpoint plans none.
- A session built from credentials that lack a named key is refused at once, not at the first
  connect.
- The runtime holds a second zeroed copy of each handshake credential for the session's life.

## What would show this was wrong

- A venue that wants a credential header whose value changes per connection (a signed or
  timestamped handshake): the static naming cannot express it, and the codec must supply the
  headers per epoch, in a new record.
- A credential or an account id found in a journal file, a log line or an error of a
  handshake.
- A venue that refuses the title-case header spelling hyper writes (0029).
