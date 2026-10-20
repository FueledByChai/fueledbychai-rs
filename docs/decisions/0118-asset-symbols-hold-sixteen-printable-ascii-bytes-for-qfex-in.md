# 0118 — Asset symbols hold sixteen printable ASCII bytes for QFEX instrument discovery

Status: accepted
Date: 2026-10-20

## Context

FBC-e3g7 must discover every active QFEX perpetual from public `/refdata`. Its current
`ANTHROPIC-USD` entry states `base_asset: "ANTHROPIC"`, nine ASCII bytes. The core's
`AssetSym` previously held at most eight, so a correct draft could not represent it.

## Decision

`AssetSym` holds one to sixteen printable ASCII bytes. Discovery reads the venue's exact
`base_asset`, `quote_asset` and `margin_asset` without shortening them. A future name over
sixteen bytes or outside printable ASCII is refused by QFEX discovery, not silently renamed.

## Alternatives

- Skip `ANTHROPIC-USD`: rejected because it is active and FBC-e3g7 calls for every active
  perpetual.
- Alias or truncate the asset name: rejected because that would make the venue's exact asset
  identity ambiguous.
- Use a heap string for every asset: rejected for this bounded extension because `AssetSym`
  is a small copied key on the event and order paths.

## Consequences

The copied key grows by eight bytes, and structs containing one or more asset names may grow
accordingly. The public constructor and `as_str` contract remain the same; no journal format
stores the fixed array directly. Existing venues may now discover names of nine to sixteen
bytes that they previously passed over, so their fixtures and tests must reflect the bound.

## What would show this was wrong

A venue lists an active required instrument whose asset name exceeds sixteen bytes or cannot
be represented as printable ASCII, or the larger copied key is measured to harm a hot path.
