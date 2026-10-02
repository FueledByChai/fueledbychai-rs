# 0004 — Ids, fees, time, prices and quantities are sealed newtypes, and a fee is positive when we paid it

Status: accepted
Date: 2026-10-02

## Context

The bugs that cost real money in the Java stack include ids passed as bare strings (a venue id
handed to a call that wanted a client id), four different fee-sign conventions with a GRVT fee
run through `.abs()`, mixed clocks, and prices rounded where they should not be (design §2,
§4.1–§4.3). Each is a type error that a type system can refuse.

## Decision

Ids, fees, time, prices and quantities are newtypes, and the dangerous ones have sealed
constructors (design D4, §4.1–§4.4):

- **Time.** `MonoNs`, `WallNs`, `ExchNs` and `KernelRxNs` are distinct; there is no subtraction
  between kinds. Exchange time is never synthesized.
- **Prices and quantities.** `Ticks` index the finest valid grid of an instrument (`PriceGrid`
  covers fixed, significant-figure and banded grids); `Lots` and `SignedLots` count size steps.
  Off-grid values (mark, index, average entry) are `PxExact` and never rounded to ticks. Money
  is integer nanos with an asset. `f64` appears only inside models.
- **Fees.** `Fee` is the cost to us: **positive means we paid, negative means a rebate**. It has
  no public constructor, no `abs`, no negation and no conversion from `f64`; adapters obtain one
  only through `DecodeScope::fee`, which applies the venue's declared fee sign. Fee rates belong
  to accounts (`FeeBook`), not instruments.
- **Ids.** `ClientOrderId` has private fields and is minted only by `CidMint` while a namespace
  lease is held; its seq floor survives restarts. `fbc-core` owns the canonical client-id codec
  (a format tag, the namespace and the sequence), so ids minted by other systems decode as
  `Unparseable`; adapters supply only the wire format. `VenueOrderId`, `FillId` and `Fee` are
  created only inside a codec callback through `DecodeScope`.

`trybuild` compile-fail tests prove the seals.

## Alternatives

- Strings and `f64` with conventions in comments, as in the Java library: rejected for the bugs
  above.
- One global fixed-point scale (say 1e-8): rejected. It cannot hold sub-1e-8 ticks and
  overflows on very large quantities; per-instrument indexing is exact (design §4.1).

## Consequences

- Every adapter decodes through `DecodeScope`; there is no back door for tests either, which use
  the same scope.
- A spec change (new tick or size step) bumps the instrument spec version and forces a book
  resync and re-quantization of resting orders.
- Converting to and from legacy representations (database columns, UI fields) is explicit and
  lives at the edge, mostly in the private consumer.

## What would show this was wrong

- A fill or order record with a wrong fee sign or a swapped id reaches storage despite the
  types.
- A minted client id collides with a live or recent one after a restart (the conformance
  `restart_cid` test exists to catch this).
- Tick and lot indexing per instrument produces a wrong book or order price on a spec change.
