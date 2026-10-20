# 0119 — Public trade IDs keep numeric and UUID identities in a copyable generic value

Status: accepted
Date: 2026-10-20

## Context

`MdEvent::Trade.id` was `Option<u64>`, so the QFEX adapter discarded its public UUID and
declared `TradeCaps.trade_id = false` (FBC-e3jy). The event is `Copy`, and public trade identity
must not be confused with an account fill id. Decision 0004 seals IDs at the decode boundary.

## Decision

`MdEvent::Trade.id` is `Option<TradeId>`. `TradeId` is a copyable, sealed, venue-neutral value
with distinct numeric `u64` and UUID `u128` representations. A codec constructs either through
`DecodeScope`; UUID wire text must be canonical 8-4-4-4-12 hex (either letter case) and formats
back as lowercase canonical text. QFEX requires and preserves the UUID it sends and declares
`TradeCaps.trade_id = true`.

## Alternatives

- Continue discarding QFEX's UUID: loses a supplied deduplication key and falsely suggests the
  feed has no trade IDs.
- Change every market-data event to an allocated string ID: supports arbitrary text but removes
  `MdEvent: Copy` and allocates on the hot path for the two observed identifier shapes.
- Coerce a UUID into `u64`: truncates identity and can collide.

## Consequences

Consumers can compare and hash normalized public trade IDs without a QFEX branch. Existing
numeric IDs remain representable through the numeric variant; a numeric value never equals a
UUID with the same low bits. This is a source-level API change for code that explicitly types or
constructs `MdEvent::Trade.id`. A venue with another ID format needs a new generic
representation and decision; it must not truncate or silently discard an advertised ID.
The QFEX live capture tests both UUID preservation and the feed's separate sequence metadata.

## What would show this was wrong

A supported venue sends an ID that is neither numeric nor UUID, or copying the additional
tagged 128-bit value proves too costly in measured decode-to-handler latency.
