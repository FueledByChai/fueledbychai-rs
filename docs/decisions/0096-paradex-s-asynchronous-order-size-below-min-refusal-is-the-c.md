# 0096 — Paradex's asynchronous ORDER_SIZE_BELOW_MIN refusal is the common kind MinNotional, read by its code, augmenting 0092

Status: accepted
Date: 2026-10-18

## Context

0092 gives a placement Paradex closed unfilled after accepting it an `ExecEvent::AsyncReject`
whose kind is `PostOnlyWouldCross` for POST_ONLY_WOULD_CROSS and `Other` for any other reason,
the reason in its `raw`. The owner's testnet acceptance run 20261016T115634Z (FBC-p0aa's and
FBC-tyft's notes) showed what ORDER_SIZE_BELOW_MIN means: Paradex judges an order's minimum
notional at the MARK price, not at the order's price, and refuses one below it only after the
RPC accepted it (a sell 0.09 HYPE at 118.5706, $10.67 at its price, refused with the mark at
81.73: $7.36). FBC-tyft's mainnet loop must treat that refusal as a sizing error, a permanent
rejection that Stops the run, not as a venue fault. With the kind `Other`, the sample could tell
it apart only by reading Paradex's reason text, venue-specific branching outside the venue
crate that the project rules forbid (Codex r4306449948 on PR #158). `fbc-core` already has the
kind for it: `RejectKind::MinNotional`, "below the venue's minimum notional".

## Decision

The Paradex adapter gives an asynchronous reject whose reason's code (the text before any `:`,
as 0093 reads testnet's `CODE: message` reasons) is ORDER_SIZE_BELOW_MIN the kind
`RejectKind::MinNotional`; POST_ONLY_WOULD_CROSS stays `PostOnlyWouldCross` and every other
reason stays `Other`, as 0092 has it. The reason stays the reject's `raw`, with no
`venue_code`. A consumer branches on the kind only.

## Alternatives

- Keep `Other` and let each consumer read the reason text: the venue's wording leaks into every
  consumer, and a changed wording silently changes what a consumer does.
- Map NOT_ENOUGH_MARGIN to `Margin` and EXCEEDS_PRICE_BAND to `InvalidPrice` in the same
  change: correct by the same argument, but no consumer needs them yet, and each wants its own
  evidence of Paradex's wording; a later record can add them.

## Consequences

`testnet_quote` (and any consumer) Stops on `MinNotional` as a sizing error by its kind. An IOC
or MARKET order closed unfilled for ORDER_SIZE_BELOW_MIN, which 0093 already takes as a
refusal, now carries `MinNotional` too. The adapter's exec tests pin the mapping
(`tests/exec_codec.rs`, `an_order_closed_unfilled_order_size_below_min_is_an_asynchronous_min_notional_refusal`).

## What would show this was wrong

Paradex stating ORDER_SIZE_BELOW_MIN for a refusal that is not about the order's notional (its
size step or minimum size, say): then the kind is `InvalidQty` for that wording, and a new
record says which.
