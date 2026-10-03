# 0014 — The venue boundary as built refines design 4.6 to 4.8 where a sans-IO codec needs more than the design text gives it

Status: accepted
Date: 2026-10-03

## Context

0002 makes venue adapters sans-IO codecs and points at design §4.6–§4.8 (kept in the private
consumer's repository) for the events, commands and traits. FBC-5 built that boundary in
`fbc-core` (`event.rs`, `command.rs`, `codec.rs`, `venue.rs`) and proved it with a toy venue
(`crates/fbc-core/tests/toy_venue.rs`). Where the design text, taken literally, left a codec
unable to do its job without a clock, an order registry or IO, or let a value contradict 0005 or
0009, the build departed from it. The Codex review of pull request #8 (r4172231035) asked that
those departures be recorded rather than left in commit prose, since the runtime (`fbc-runtime`),
the journal (0006) and every venue crate will build on them. This record states them; the
design text stays as it is and this record wins where they differ.

## Decision

The venue boundary is the design's §4.6–§4.8 with these refinements, each needed for a codec to
stay sans-IO and deterministic:

1. **Nonces travel in `EncodeCtx`.** `EncodeCtx { wall, mono, nonces: NonceBlock }` is the only
   time and nonce source an encode sees. `NonceBlock` states one value per item (consecutive
   for a monotonic `NonceScope`, independent for `Random`); `NonceSource::reserve(len)` replaces
   `next()`, the runtime reserves `VenueCommand::items()` nonces per encode and journals the
   values, and `exec_codec` takes no `NonceSource`.
2. **Codecs report, the runtime stamps.** `MdSink`/`ExecSink` take `(VenueMeta, body)`; the
   runtime builds the `Envelope` with its `Stamp`, so a decoder cannot forge receive time or
   ingest order.
3. **`Effect::Http` carries `rpc`, `timeout` and `class`, and every HTTP request comes back.**
   `on_http` takes `Result<HttpResponse, HttpFailure>`: a request that got no response returns
   to its codec as `NotSent` (no byte written), `TimedOut` or `Lost` (written, connection
   failed), so a codec can retry a snapshot or recover. An order sent over REST carries an
   `rpc` naming it, and its failure is its outcome: `NotSent`, or `Unknown`, never resent
   (0005). The traffic class keeps a REST cancel on the safety floor. The journal's outbound
   HTTP record carries the `rpc` (0006).
4. **Codecs get the spec table wherever they spell an instrument.** `on_http` (both codecs),
   `MdCodec::subscribe` and `VenueFactory::plan_md` take the `SpecTable`; `on_http`,
   `subscribe` and `plan_md` return `Result`, and `VenueError::UnknownInstrument` names an
   instrument missing from the table (a refused `subscribe` pushes nothing).
5. **Commands carry what a stateless codec needs.** `AmendOrder` has no `kind` (an amend targets
   a resting limit order) and carries `cum_filled`, so `AmendOrder::wire_qty` gives the
   remaining quantity a venue with `AmendQty::Remaining` expects: the resting quantity the OMS
   checked. Fills the OMS has not seen can reach the venue first, so the OMS counts such an
   amend's resting as the wire quantity it sent until the venue reports the order's total
   (FBC-w5n). `QueryOrder` carries `placement_nonce`, as `CancelOrder` does, for venues that
   query by it.
6. **Values that would contradict 0005 are unrepresentable.** `RejectKind::AlreadyTerminal`
   takes a payload-free `TerminalHint` (the design's `TerminalKind(RejectKind)` would make
   `RejectKind` recursive); `VenueOrderState::Rejected` takes a `TerminalReject`, which refuses
   `RejectKind::NotFound`.
7. **Events say what they do not know.** `ExecEvent::Mode { scope, mode }` names the market
   (`ModeScope::Instrument`) or the whole account a venue mode applies to; `OrderUpdate`,
   `FillEvent` and `VenueOrderSnapshot` carry `cid: Option<CidMatch>`, `None` when the venue
   echoes no client id (not `Unparseable`, which means a non-canonical id was present).
8. **`Debug` never shows a credential (0009).** `WireSlice`, `Header`, `HttpRequest`,
   `HttpResponse`, `RawFrame` and `Reject` format redaction spans, redacted header values, a
   URL's user information, query and fragment, response bodies, inbound frames and a venue's
   reject text by length only.
9. **Configuration and traffic class.** `VenueConfig` keeps market-scoped keys per instrument
   (`insert_market`, `get_market`). A request is `Safety` only when every item is: a batch of
   reducing orders is `Safety`, a mixed batch `Normal`.
10. `Vec` stands where the design has `SmallVec` or `Bytes`, to add no dependency yet.

Calls the design lists but FBC-5 left out each have a ticket: `discover` and
`parse_fbc_common_symbol` (FBC-ahf), credentials for `exec_codec` and `test_connection` (FBC-b3b,
a review path), `PathStamps` (FBC-ji6).

## Alternatives

- Keep the design text literally: rejected for the reasons in each item above.
- `ResyncBegin.watermark` and `MdEvent::Funding.next` as `ExchNs` (Codex r4172231005,
  r4172231014): not taken; both stay `WallNs` as the design has them. The watermark is the
  instant the codec requested the snapshot, which it knows only from `EncodeCtx.wall`; the
  OMS compares it with a fill's `exch_ts` through the per-connection `ClockSkewEstimate`, the
  one bridge between the clocks (design §4.1). The next funding time is a calendar instant of
  the venue's schedule, used against the local wall clock, not the stamp of a venue event.
- A book-channel index on every book event (Codex r4172231010): not taken. An instrument
  subscribes to one book channel, chosen by configuration and treated as a strategy change
  (design §7.2); FBC-nij makes the runtime refuse a second one.
- An authorization parameter on `OrderGateway::submit` now (Codex r4172231026): not taken in
  FBC-5. The trait is declared as design §4.8 has it and nothing implements it yet; FBC-ob2
  makes order entry reachable only through `fbc-oms` (0013 rule 2) before a live gateway exists.

## Consequences

- `fbc-runtime` must reserve and journal nonces per encode, stamp sink output, time out HTTP
  requests that carry an `rpc`, and pass the spec table to planning and subscribing.
- The OMS supplies `cum_filled` on every amend and never sees a terminal `NotFound`.
- A later change to any of these items is a new record that supersedes this one.

## What would show this was wrong

- A venue whose protocol needs a codec input this boundary does not give (a clock, an order
  registry, a socket), other than an SDK-only venue under `ManagedGateway`.
- Replay of a journaled session producing different bytes because a value an encode used was
  not in `EncodeCtx`.
- A `Debug` or log line found carrying a credential despite item 8.
