# 0031 — Kernel receive timestamps come from SO_TIMESTAMPNS through nix 0.31.3 on Linux, and a session reports each Safety write's tick-to-wire to its handler

Status: accepted
Date: 2026-10-04

## Context

Design §5.3 starts every latency stage at the kernel receive timestamp, and tick-to-wire is
write done minus kernel receive; 0002 measures the latency gates on Linux only, while the
owner develops on macOS, where the timestamp is absent. `Stamp.kernel_rx` (`KernelRxNs`,
`CLOCK_REALTIME`) has been `None` since FBC-ku8. FBC-2y3 fills it beneath TLS and WebSocket,
for `ws://` and `wss://` alike, and records a Safety-class write's tick-to-wire, per stream,
for the consumer. It was the ticket's to decide the socket and control-message dependency
(libc, nix or socket2), its pin, and where any unsafe code sits. 0019 records the rest of the
network stack; records are never edited, so this one adds to it.

## Decision

- **Where the timestamp is read.** Every connection the `Connector` opens, WebSocket or HTTP,
  plain or TLS, reads through `fbc_runtime::Tcp`, which wraps the TCP stream. On Linux it turns
  on `SO_TIMESTAMPNS` as it wraps the stream and reads with `recvmsg`, keeping the timestamp
  of the last packet each read consumed (the `SCM_TIMESTAMPNS` control message). A refused
  `setsockopt` leaves the stream without timestamps rather than unusable. TLS and WebSocket
  read through it, so when a frame completes, the latest timestamp is that of the last packet
  read before it completed: its own last packet, or a later one that came in the same read.
  The session stamps each frame with it as `kernel_rx`, which is therefore never later than
  the frame's `recv_wall`. Elsewhere `Tcp` reads as a plain stream and `kernel_rx` is `None`,
  as it is for timer firings and HTTP results everywhere.
- **The dependency.** `nix =0.31.3` (MIT), default features off, `socket` and `uio` only,
  pinned exactly in `[workspace.dependencies]` and a dependency of `fbc-runtime` on Linux
  only (`[target.'cfg(target_os = "linux")'.dependencies]`). It brings `bitflags 2`,
  `cfg-if`, `libc` (already in the tree) and `memoffset`, and `cfg_aliases` at build time, all
  under 0017's allowlist.
- **No unsafe code here.** `setsockopt(ReceiveTimestampns)`, `recvmsg` and the control
  message parsing are nix's safe wrappers; the unsafe code sits in nix. Readiness goes through
  tokio's `TcpStream::try_io`, so a spurious wake-up is cleared and waited on again.
- **Attribution.** A write issued while an input is handled is attributed to that input: the
  codec's effects for it, and the writes the consumer's handler issues through the `Outbox`
  it is given (`MdHandler::on_md_with`, whose default is `on_md`). A handler's writes go on the
  session's own stream after the codec's effects for the same input, in the order issued,
  charged to the rate buckets (0030) and journaled (0006) as a codec's frames are; a reconnect
  the codec asks for first ends the epoch before them, as it does the codec's own later
  effects. Effects asked for by an HTTP result that comes back during a write are attributed
  to that result.
- **Tick-to-wire.** When a Safety-class frame's write completes and the input it is
  attributed to has a `kernel_rx`, the session calls the handler's
  `MdHandler::on_tick_to_wire` (by default nothing) with a `TickToWire`: the stream, the
  input's stamp and write done (the wall clock, `CLOCK_REALTIME`, read as the write
  completes) minus `kernel_rx` in signed nanoseconds, negative only if the wall clock stepped
  back. Nothing is reported where `kernel_rx` is `None`, for a Normal write, or for a write
  that failed. The runtime keeps no samples: the consumer aggregates them as its latency gates
  need. `MdVenue` forwards both calls to the consumer's one handler.

## Alternatives

- `libc` directly: rejected. `recvmsg` and the `CMSG_*` walk would put unsafe code in
  `fbc-runtime` for no gain over nix's tested wrappers.
- `socket2`: rejected. It sets the option only through a raw call and has no control-message
  parser, so the parse would still be unsafe code here.
- Hardware timestamps (`SO_TIMESTAMPING` with `SOF_TIMESTAMPING_RX_HARDWARE`): rejected for
  now. They need NIC support and configuration the deployment box has not been shown to have;
  software receive timestamps are what design §5.3 names.
- A per-frame timestamp exact to the frame's own last packet: rejected. TLS and WebSocket read
  ahead in whole reads, so only a read's timestamp is knowable beneath them; a later packet in
  the same read can only make `kernel_rx` later, never earlier than the frame's own.
- Keeping a histogram of tick-to-wire in the runtime: rejected. The buckets and percentiles are
  the consumer's latency gates (0002); a callback keeps the runtime free of them.

## Consequences

- `fbc_runtime::Transport`'s variants now wrap `Tcp` (`Plain(Tcp)`, `Tls(Box<TlsStream<Tcp>>)`)
  and `Transport::kernel_rx` reads the timestamp; a consumer that named the old `TcpStream`
  forms moves with this change.
- Moving nix is a ticket. The Linux code compiles and is tested only on Linux, in CI; a
  developer Mac runs the `None` path.
- BT-402 applies the same measurement, unchanged, to engine-issued protective cancels on an
  order-entry session.
- `BookKeeper`'s `BookHandler` is not given an outbox; a consumer that writes from it needs a
  ticket.

## What would show this was wrong

- A Linux kernel that delivers no `SCM_TIMESTAMPNS` on TCP reads, or one on a monotonic clock,
  so `kernel_rx` stays `None` or reads far from `recv_wall`.
- Read-ahead making `kernel_rx` routinely later than the frame's own last packet by an amount
  the latency gates notice.
- A latency gate that needs the runtime to aggregate tick-to-wire itself.
