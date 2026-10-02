# 0007 — Venue order: Paradex first, then Hibachi, with Binance USD-M futures as reference market data

Status: accepted
Date: 2026-10-02

## Context

The Java library supports many venues; this one starts with the ones the private consumer
trades first (design D9, §14). Paradex's market data is sequenced, so its book can be checked
against REST snapshots and its queue position modelled. Hibachi's nonce is per account, so one
process must own every market on a Hibachi account, which makes it a whole-account move rather
than a market-by-market one. Binance USD-M futures is the reference price feed and is never
traded through this library.

## Decision

Exchange venues arrive in this order:

1. **Paradex** (`fbc-venue-paradex`): market data first (the binary SBE WebSocket feed, gated
   on the schema's block length; best bid and offer, book deltas, trades), then the signer
   (a hand-written SNIP-12 revision 0 order hash and Stark-curve signature, checked against the
   Java library's vectors), then order entry (JWT authentication, the order WebSocket, batch
   create, cancel-on-disconnect, per-item outcomes, resync).
2. **Hibachi** (`fbc-venue-hibachi`), after Paradex order entry has traded live.

**Binance USD-M futures is reference market data only** (`fbc-venue-binance-usdm`: best bid
and offer, partial depth, and diff depth anchored by a REST snapshot), built alongside Paradex
market data (design §14 M0) and never traded through this library.

The other venues the Java library supports come after Hibachi, as design §14 schedules them:
GRVT in M4; QFEX, IBKR (through `ManagedGateway`) and Hyperliquid in M5. Each needs its own
story first.

## Alternatives

- Hibachi first, since its Java adapter caused the worst incident: rejected. Its account-wide
  nonce forces a whole-account cutover, which is the riskiest first step; Paradex can be proven
  one market at a time.
- Several venues in parallel: rejected for a one-person project; the conformance kit should be
  proven against one exchange venue before a second.

## Consequences

- The first epics are the core contract, the runtime, Binance market data and Paradex.
- The Paradex signer is the first code under a review path (0009).
- Hibachi-specific capabilities (flag conflicts, two-phase ack, quote grid distinct from the
  acceptance grid) are already in the capability model (0003) but get exercised only later.

## What would show this was wrong

- The private consumer's first live market moves to a venue other than Paradex.
- Paradex changes its API so that its feed or order path cannot be built as planned (for
  example, the SBE feed is withdrawn or order entry over WebSocket is rate-limited per IP in a
  way that makes it impractical).
