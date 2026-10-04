# 0020 — TLS runs on the connector's stream with rustls and the ring provider, trusting webpki-roots plus consumer anchors

Status: accepted
Date: 2026-10-04

## Context

The venues are reached over `wss://` and `https://`. 0002 requires the consumer's SOCKS5 proxy
to apply to every connection, and 0019 put every connection behind one `Connector` but left TLS
out (FBC-27a). rustls needs a crypto provider: its default, aws-lc-rs, builds a C library with
cmake; ring (Apache-2.0 AND ISC) builds with `cc` only. The owner accepted ring as the default
on 2026-10-03 (FBC-27a notes). The tests must trust a server without reaching the internet and
without a committed key or certificate (0009).

## Decision

- **TLS on the connector's stream.** A `wss://` or `https://` target opens TCP through
  `Connector::connect`, directly or through the SOCKS5 CONNECT, and runs the rustls client
  handshake over that stream, so the proxy applies to encrypted traffic exactly as to plain.
  Both protocols run on one `Transport` (plain or TLS); the default port is 443.
- **rustls with ring**, each pinned exactly in `[workspace.dependencies]` with default features
  off: `rustls =0.23.43` (`ring`, `std`, `tls12`), `tokio-rustls =0.26.6` (`ring`, `tls12`),
  `webpki-roots =1.0.9` (CDLA-Permissive-2.0), and test-only `rcgen =0.14.10` (`ring`). The
  client configuration names the ring provider itself, so no process-wide default provider is
  installed and aws-lc-rs is not in the tree.
- **Trust.** webpki-roots (Mozilla's roots) plus any DER certificate the consumer adds with
  `Connector::add_trust_anchor`; the client configuration is rebuilt when an anchor is added,
  never per connection. Hostname verification is always on, with no switch to turn it off. The
  server name (SNI) is the target's host name from the URL, never the proxy's; an IP literal is
  verified against the certificate's IP addresses and sent without SNI (RFC 6066).
- **Errors.** Two steps: `TlsTrust` (an anchor that is not a usable certificate) and
  `TlsHandshake` (an invalid server name, a refused certificate, an alert, bytes that are not
  TLS, or the peer hanging up). The cause is rustls' own description, which may name the host
  but never a URL's user information or query (0014 item 8), or the I/O error's kind.
- **Tests** generate a CA and server certificates in memory with rcgen; nothing is written to
  disk or committed.

## Alternatives

- aws-lc-rs, rustls' default provider: rejected (owner default, 2026-10-03). It needs cmake and
  a larger C build on every machine that runs the check, and buys nothing the venues need.
- native-tls / OpenSSL: rejected. A system library whose version differs per machine, so the
  laptop and the deployment box could negotiate differently.
- rustls-native-certs (the OS trust store) instead of webpki-roots: rejected for now. The same
  binary would trust different roots on the laptop and the box; a consumer that needs a
  private CA adds it as an anchor.
- tokio-tungstenite's own TLS features: rejected. They connect by themselves, outside the
  connector, so the proxy would not apply (0002).

## Consequences

- `fbc_runtime::ws::WebSocket` is now `WebSocketStream<Transport>`; a consumer that named the
  old `TcpStream` form moves with this change.
- Moving rustls, tokio-rustls, webpki-roots or rcgen is a ticket; moving webpki-roots changes
  which servers are trusted.
- The build needs a C compiler for ring (`cc`), as it already does on every supported machine.
- Out of scope: client certificates and certificate pinning; a ticket adds either if a venue
  needs it.

## What would show this was wrong

- A venue whose certificate chains only to a root missing from webpki-roots, so every consumer
  must add an anchor by hand.
- A venue or proxy that needs TLS behaviour rustls does not offer (a cipher suite, or
  renegotiation).
- The TLS handshake measurably adding to reconnect time compared with another provider.
