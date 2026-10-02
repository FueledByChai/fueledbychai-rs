# 0002 — Venue adapters are sans-IO codecs driven by one generic runtime that proxies through SOCKS5 from day one

Status: accepted
Date: 2026-10-02

## Context

The Java library gives each venue an adapter that owns its own sockets, threads and order
registry. In practice that leaked: unordered event delivery from cached thread pools,
duplicate subscriptions after reconnects, per-venue reconnect bugs, and roughly thirty system
property bridges (design §2). A Rust replacement needs one place where connections, reconnects
and rate limits are done right, and adapters small enough that a venue author cannot
reintroduce those bugs. Separately, the owner develops and tests on a laptop that reaches the
venues through a SOCKS proxy (the existing Java configuration carries a proxy host and port),
and the consumer's deployment shares one egress IP with other trading processes.

## Decision

Venue adapters are sans-IO codecs (design D2, §2, §4.7): `MdCodec` turns market-data bytes into
normalized events, `ExecCodec` turns full commands into signed bytes and order bytes into
normalized events, and both receive HTTP results and timer firings back as inputs. An adapter
owns no socket, thread, queue or clock; its only outputs are effects (send a frame, make an
HTTP request, set a timer, reconnect) that the runtime executes and journals.

One generic runtime, `fbc-runtime`, owns WebSocket and TLS connections, reconnects and
connection epochs, subscription reconciliation, keepalives, RPC timeouts, scoped rate limits
with a safety floor, kernel receive timestamps, and the shard host: one pinned hot-shard
thread per group of markets that reads, decodes, applies, decides and signs in one call stack,
signing one order at a time so a protective cancel waits for at most one signature in progress
(design D6, §5.1, §5.2).

**The runtime supports SOCKS5 proxying for both WebSocket and HTTP traffic from its first
network code.** The proxy host and port are configuration supplied by the consumer per
process; the code has no default proxy and no proxy address in it.

A venue reachable only through a vendor SDK that owns its own socket uses `ManagedGateway`
(design §4.7), journaled at the event level; using it needs its own decision record first.

## Alternatives

- Adapters that own their sockets, as in the Java library: rejected for the bugs above.
- An async trait per venue, each running its own tasks: rejected. It keeps per-venue
  reconnect and ordering logic and makes replay depend on task scheduling.
- Proxying only HTTP, or tunnelling at the operating-system level: rejected. The owner's
  existing setup is a SOCKS5 host and port per process, and market data and order entry both
  run over WebSockets.

## Consequences

- Replay runs the real decoders and encoders against journaled inputs (0006).
- Writing a venue is writing codecs, a signer, a capability table and a symbol mapping (design
  §6); the conformance kit's stub server exercises reconnect storms, duplicate acks and silence
  against every adapter.
- The runtime is the hardest crate in the workspace and carries the most tests.
- On macOS (development) kernel receive timestamps are absent and recorded as `None`; latency
  gates are measured only on Linux.
- The runtime reports the numbers the consumer uses to judge its deployment: tick-to-wire for
  protective cancels measured from the kernel receive timestamp, and every rate-limit rejection
  counted per scope (design §5.3, §11).

## What would show this was wrong

- A venue whose protocol cannot be expressed without the adapter owning IO, other than an
  SDK-only venue that `ManagedGateway` covers.
- A venue name appearing in a branch inside `fbc-runtime`.
- SOCKS5 proxying measurably adding to protective-cancel tick-to-wire p99 on a box that does
  not need it (the proxy should then be off there, by configuration, not removed).
