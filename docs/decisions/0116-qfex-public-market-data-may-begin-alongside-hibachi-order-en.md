# 0116 — QFEX public market data may begin alongside Hibachi; order entry remains separately gated

Status: accepted, supersedes 0016
Date: 2026-10-20

## Context

Decision 0016 placed QFEX after Hibachi. The owner asked to start the Rust QFEX adapter while
another agent develops Hibachi (FBC-j9xw, BT-601). QFEX publishes public market data on a
separate, unauthenticated WebSocket. Work on that feed does not require the trading credentials,
the order-entry session, or a live order.

## Decision

Paradex remains the first traded venue; Hibachi's implementation continues. QFEX public
market-data decoding and instrument discovery may proceed alongside it. QFEX order entry is a
separate story and ticket, admitted only after its signer/authentication, order outcomes,
cancel-on-disconnect, resync, and conformance evidence exist. Until then its factory declares
`exec: None` and a `Readiness::Record` ceiling. Binance remains reference data only. GRVT,
IBKR and Hyperliquid retain their later place in the roadmap.

## Alternatives

- Wait for Hibachi to be live before any QFEX code: rejected for the public feed because it
  does not share Hibachi's credentials or order-entry state.
- Make the first QFEX slice trade-capable: rejected because public feed fixtures cannot prove
  order truth or safety on QFEX's private WebSocket.

## Consequences

BT-601 names the QFEX public-feed story. QFEX's capability values are cited to its current
documentation and revisited when live observations contradict them. A later ticket must remove
the read-only ceiling with independent private-path proof and a new decision if needed.

## What would show this was wrong

The public feed cannot be decoded deterministically from its documented messages, or parallel
venue work causes a safety or review failure. A new record would then revise this sequencing.
