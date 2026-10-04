# 0019 — The runtime's network stack: tokio, tokio-tungstenite and hyper over one connector with a hand-written SOCKS5 CONNECT, each pinned

Status: accepted
Date: 2026-10-03

## Context

0002 makes `fbc-runtime` the only code that opens connections and requires SOCKS5 proxying for
WebSocket and HTTP from its first network code. The owner's laptop reaches the venues through a
SOCKS5 host and port; the deployment box connects directly, with the same binary. Design §5.1
runs the runtime on tokio's current-thread scheduler. FBC-2ds is the runtime's first network
code, so it brings the workspace's first network dependencies, and each must sit inside 0017's
licence allowlist. The owner decided on 2026-10-03 that the proxy is a host and port only, with
no username or password, and accepted the stack below as the default (FBC-2ds notes).

## Decision

- **One connector.** `Connector` opens every TCP connection, directly (`ProxyConfig::Direct`)
  or through a SOCKS5 CONNECT (`ProxyConfig::Socks5 { host, port }`), and the WebSocket client
  and the HTTP/1.1 call both take their stream from it, so the proxy cannot apply to one and
  not the other. `ProxyConfig` has no default and the code holds no proxy address.
- **SOCKS5 by hand.** RFC 1928's no-authentication method only, in `src/socks5.rs` over any
  byte stream. A target host name goes as DOMAINNAME, so the proxy resolves it (as `socks5h`
  does) and the process never resolves a venue host itself; an IP literal goes as an IPv4 or
  IPv6 address. There is no credential code in `fbc-runtime`.
- **Dependencies, each pinned exactly** in `[workspace.dependencies]` with default features
  off; moving one is a ticket:
  - `tokio =1.53.2` (`rt`, `net`, `io-util`, `macros`). The runtime never spawns a task and
    starts no runtime of its own, so its calls run on the caller's current-thread runtime
    (design §5.1) without a `LocalSet`; the HTTP connection is driven beside the exchange with
    `tokio::join!`.
  - `tokio-tungstenite =0.30.0` (`handshake` only, no `connect`, no TLS): `client_async` over the
    connector's stream.
  - `hyper =1.11.1` (`client`, `http1`), `hyper-util =0.1.21` (`tokio`, for `TokioIo`) and
    `http-body-util =0.1.5`: hyper's HTTP/1.1 client connection over the connector's stream.
  - `futures-util =0.3.34`, test-only, for the WebSocket `Stream` and `Sink` traits.
  Every crate these bring is on 0017's list; `cargo deny check licenses` passes on the tree.
- **Errors.** `NetError` names the step that failed (URL, TCP to the proxy or the target,
  SOCKS5 greeting, SOCKS5 CONNECT with the reply code and its RFC name, WebSocket upgrade,
  HTTP) and holds no URL, so neither `Display` nor `Debug` can show a URL's user information or
  query (0014 item 8). An I/O error keeps its kind only.
- **Bounded bodies.** `Connector::http` takes the caller's response-body limit and fails at
  the HTTP step once a body passes it (`http_body_util::Limited`), so a large or unending body
  cannot exhaust memory; the number comes from the consumer, not from code here.
- **Known gap.** tungstenite 0.30 accepts an upgrade response only when its `Connection` header
  is exactly `Upgrade` (case aside), not a token list such as `keep-alive, Upgrade` that RFC 6455
  allows; FBC-17h tracks a token-aware upgrade.
- **Not yet.** TLS (`wss://`, `https://`; FBC-27a), epochs, sessions and reconnects (FBC-5pt,
  FBC-ku8). No call has a deadline of its own; the caller bounds one.

## Alternatives

- The `tokio-socks` crate for the handshake: rejected. The no-authentication CONNECT is about a
  hundred lines that the tests drive byte by byte; the crate would add a dependency and its
  error type for no saving.
- A hand-written HTTP/1.1 client instead of hyper: rejected. Chunked bodies, keep-alive and
  response parsing are where hand-written clients go wrong, and hyper is the common choice.
- `reqwest` for HTTP: rejected. It owns its own connector and proxy handling, so the proxy would
  be configured twice and could differ between WebSocket and HTTP.
- Caret (`^`) requirements with only `Cargo.lock` pinning: rejected. A consumer's own lock
  resolves this library's requirements again, so only an exact requirement holds the version
  the tests ran against.

## Consequences

- A consumer opens connections only through `Connector`; CW-06 maps Java's `proxy.enabled`,
  `proxy.host` and `proxy.port` onto `ProxyConfig`.
- The public API exposes tokio-tungstenite's `WebSocketStream` and `Message` and hyper's
  `Request`, `Response` and `Bytes` (re-exported from `fbc_runtime::ws` and `fbc_runtime::http`),
  so moving one of those pins can change this library's API; the move is a ticket.
- Exact pins mean another crate in the workspace that needs a newer tokio, hyper or
  tokio-tungstenite moves the pin in its own ticket.
- `crates/fbc-runtime/tests/common/` holds the SOCKS5 stub, WebSocket server and HTTP server on
  127.0.0.1 ephemeral ports that later runtime tickets reuse.

## What would show this was wrong

- A proxy the owner uses that the hand-written handshake cannot speak (an authentication
  method, or a reply form RFC 1928 allows that the code refuses).
- hyper or tokio-tungstenite forcing a task spawn or a multi-thread runtime, against design
  §5.1's current-thread runtime.
- The SOCKS5 path measurably adding to protective-cancel tick-to-wire on a box that connects
  directly (0002 already says the proxy is then off by configuration there).
