# 0016 — Venue order: Paradex first, its signer offline before market data, then order entry; then Hibachi; Binance USD-M as reference

Status: accepted, supersedes 0007
Date: 2026-10-03

## Context

0007 set the venue order and, inside Paradex, the sequence market data, then the signer, then
order entry. The Paradex signer (FBC-6, pull request #10) was built before Paradex market data,
and Codex's review of that pull request (thread
https://github.com/FueledByChai/fueledbychai-rs/pull/10#discussion_r4173573969) flagged the
difference: either defer the signer until market data lands, or record the change in a new
decision that supersedes 0007.

The signer is pure, network-free code: `ParadexSigner` maps a message to a signature, opens no
socket and sends nothing, and it is checked against 2,013 vectors from the Java library's own
signer (`fixtures/paradex/signing/paradex-vectors.tsv`). Nothing can send an order with it until
order entry lands. The owner decided on 2026-10-03 to accept the signer landing first, as
offline work. This record amends only 0007's Paradex sequence; the rest of 0007 is restated
here unchanged so that this record can stand in its place.

## Decision

Exchange venues arrive in this order:

1. **Paradex** (`fbc-venue-paradex`): the signer, as offline work (a hand-written SNIP-12
   revision 0 order hash and Stark-curve signature, checked against the Java library's
   vectors; it opens no connection and sends nothing), then market data (the binary SBE
   WebSocket feed, gated on the schema's block length; best bid and offer, book deltas,
   trades), then order entry (JWT authentication, the order WebSocket, batch create,
   cancel-on-disconnect, per-item outcomes, resync). **Order entry still comes after market
   data.**
2. **Hibachi** (`fbc-venue-hibachi`), after Paradex order entry has traded live.

**Binance USD-M futures is reference market data only** (`fbc-venue-binance-usdm`: best bid
and offer, partial depth, and diff depth anchored by a REST snapshot), built alongside Paradex
market data (design §14 M0) and never traded through this library.

The other venues the Java library supports come after Hibachi, as design §14 schedules them:
GRVT in M4; QFEX, IBKR (through `ManagedGateway`) and Hyperliquid in M5. Each needs its own
story first.

## Alternatives

- Keep 0007's sequence and defer the signer until Paradex market data lands: rejected. The
  signer is network-free and needs nothing from market data, so landing it early costs
  nothing, and it unblocks the order-entry work, which needs both.
- Treat the signer as offline work already consistent with 0007 and record nothing: rejected,
  since 0007 names the sequence explicitly and a reader of 0007 alone would see a rollout the
  code does not follow.
- The alternatives 0007 rejected stay rejected for its reasons: Hibachi first (its account-wide
  nonce forces a whole-account cutover, the riskiest first step) and several venues in
  parallel (the conformance kit should be proven against one exchange venue before a second).

## Consequences

- The first epics are the core contract, the runtime, Binance market data and Paradex; inside
  Paradex the signer is already in place before market data.
- The Paradex signer is the first code under a review path (0009).
- No Paradex order can be sent before market data lands: order entry, the only consumer that
  sends what the signer produces, still comes after it.
- Hibachi-specific capabilities (flag conflicts, two-phase ack, quote grid distinct from the
  acceptance grid) are already in the capability model (0003) but get exercised only later.

## What would show this was wrong

- Paradex order entry, or any code that opens a connection with the signer's output, lands
  before Paradex market data.
- The private consumer's first live market moves to a venue other than Paradex.
- Paradex changes its API so that its feed or order path cannot be built as planned (for
  example, the SBE feed is withdrawn or order entry over WebSocket is rate-limited per IP in a
  way that makes it impractical).
