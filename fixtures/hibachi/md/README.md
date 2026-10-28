# Hibachi market-data fixtures

Hand-built `/ws/market` frames, one per file, for `crates/venues/fbc-venue-hibachi`'s market-data
codec (FBC-2kmp, decision 0129). Nothing here was recorded from Hibachi, and no test sends
anything to it; `SYNTHETIC` says where the values come from. The tests compose the frames into
sequences (`tests/md_book.rs`, `tests/md_topics.rs`, `tests/md_stub.rs`).

Sources, cited below by name:

- **WS**: Hibachi's WebSocket market docs, <https://hibachi-docs.redocly.app/wsapi/market>
  (read 2026-10-27): the subscribe and unsubscribe frames (key `parameters`), the orderbook's
  optional `granularity` and `depth` ("one set of parameters" per subscription), "Notes for
  orderbook events" (a `Snapshot` on subscribe, then `Update`s; a null side removes all its
  levels; levels outside `startPrice`..`endPrice` removed; quantity 0 removes; absolute sizes),
  and the example frames of each topic, which list keys alphabetically, write every decimal as a
  string and carry no `timestamp_ms`.
- **SPEC**: Hibachi's Market API spec (`https://hibachi-docs.redocly.app/_spec/marketapi/index.yaml`):
  `PriceLevelResponseData` (`bid` and `ask`, each nullable), `PriceLevelsDetail` (`startPrice`,
  `endPrice`, `levels`, all required), `PriceLevelExternal` (`price`, `quantity`),
  `FundingRateEstimation` (`estimatedFundingRate`, `nextFundingTimestamp`) and `TakerSide`.
- **SDK**: Hibachi's Python SDK at 14a76b9,
  <https://github.com/hibachi-xyz/hibachi_sdk/tree/14a76b95230869dd622c31a1fc4132a6eac1a9fa/python/hibachi_xyz>:
  `api_ws_market.py` L95-L152 (subscribe and unsubscribe under `parameters`), `types.py`
  L233-L249 (`TakerSide`, the topics), L938-L955 (`FundingRateEstimation`, `spotPrice`) and
  L969-L975 (`Trade`).
- **JAVA**: the public Java library at
  [b4188a5](https://github.com/FueledByChai/FueledByChaiTrading/tree/b4188a57f7d5ea4e82260c9f3c4a0a359ad04c27),
  `implementations/market-data-api/hibachi-market-data-impl/src/main/java/com/fueledbychai/marketdata/hibachi/HibachiQuoteEngine.java`:
  `onAskBidUpdate` L548-L575 (`bidSize`, `askSize`), `onOrderBookUpdate` L588-L640,
  `onTradesUpdate` L840-L856 (`{"trade":{..}}` or `{"trades":[..]}`) and `onFundingRateUpdate`
  L904-L918 (`estimatedFundingRate` flat, else under `fundingRateEstimation`, divided by the
  ticker's funding interval, 8); it reads a root `timestamp_ms` (L550, L629).

The instruments are `fixtures/hibachi/rest/exchange_info.json`'s: ETH/USDT-P (tick 0.000001,
step 0.000000001, quote grid 0.01, so subscribed at granularity `0.01`), SOL/USDT-P (tick 0.001,
which it lists, so subscribed at `0.001`) and SEI/USDT-P (tick 0.01 and only finer
granularities, so subscribed on its tick, `0.01`). Every orderbook frame states the depth the
tests configure, 3, unless its name says otherwise.

## orderbook (WS example frame and notes; SPEC)

The ETH sequence, its bounds describing the book after each frame and the bid's running
downward:

- `eth_s0`: the Snapshot: asks 2000.10, 2000.20, 2000.30 and bids 2000.00, 1999.90, 1999.80.
  `eth_s0_timestamp_ms` is the same with a root `timestamp_ms`, the field the ticket's
  description says book frames carry and JAVA reads; WS's example shows none.
- `eth_u1`: the asks move to 2000.20..2000.40: a stated zero at 2000.10, better than the new
  best end (a level emptied), and 2000.40 scrolling in; the bid unchanged (`levels: []` with its
  bounds).
- `eth_u1_without_zero`: `eth_u1` without its zero, so the window removes the resting 2000.10
  unstated: what a lost frame leaves.
- `eth_u2`: the bid scrolls in at 2000.05; its stated scroll-out, 1999.80, lies past the downward
  `endPrice` 1999.90. After `eth_s0` alone (`eth_u1` lost) its ask window removes 2000.10
  unstated: a dropped frame.
- `eth_u3`: the ask's new best, 2000.02, would cross the bid's 2000.05 until the bid's zero
  for 2000.05 applies.
- `eth_u_inside_zeros`: an Update after `eth_s0` that empties a level strictly inside each
  side's range after the frame, neither at its best end nor past its deep end (the bid's
  1999.90 in 2000.00 down to 1999.70, the ask's 2000.20 in 2000.10..2000.40), while a level
  scrolls in at each deep end (bid 1999.70, ask 2000.40): a level emptied mid-window, where
  the window alone would keep it.
- `eth_unchanged`: both sides `levels: []` with `eth_s0`'s bounds; also the Update that arrives
  with no book.
- `eth_null_bid`: `"bid": null` beside an ask change (WS's example sends a null side beside the
  other's changes); `eth_bid_restore` sends the bid again with both bounds, and
  `eth_bid_restore_thin` with two levels, thinner than the depth.
- `eth_snapshot_null_bid`: a Snapshot whose bid is null and whose ask passes.
- Malformed Updates: `eth_absent_side` (no `bid`), `eth_levels_no_bound` (ask levels and no
  bound), `eth_empty_one_bound` (bid `levels: []` with `startPrice` only), `eth_empty_no_bound`
  (bid `levels: []` and no bound).
- Snapshots that never make a book valid: `eth_snapshot_empty` (both sides null),
  `eth_snapshot_empty_levels` (both sides `levels: []` with bounds), `eth_snapshot_empty_data`
  (`data: {}`), `eth_snapshot_absent_side`, `eth_snapshot_unbounded_side` (no `endPrice`),
  `eth_snapshot_thin` (two asks against a depth of 3, `endPrice` at the deeper of them),
  `eth_snapshot_thin_end_past` (the same two asks, `endPrice` 2000.30, past the deeper of them
  where a third level would sit) and `eth_snapshot_crossed` (best bid 2000.20 above best ask
  2000.10). `eth_snapshot_locked` (best bid equal to best ask, 2000.10) passes.
- Parameters not the subscription's: `eth_granularity_differs` (`0.1`), `eth_depth_differs`
  (5), `eth_granularity_missing`, `eth_depth_missing`.
- `eth_off_granularity`: a bid level at 1999.905, on the tick but off the 0.01 granularity.
  `eth_u_off_granularity` is an Update after `eth_s0` that would pass the bounds check (the ask
  window narrowed to 2000.10..2000.20, 2000.30 stated as scrolled out, three levels kept) but
  sets an ask at 2000.105, on the tick and off the granularity.
- `sei_s0` and `sei_off_tick`: SEI on its tick, and with an ask at 2.505, off its 0.01 tick and
  so off the granularity it is subscribed at. Sizes are whole, SEI's step being 1.
- `sol_s0` and `sol_u1`: SOL at granularity `0.001`, the second instrument of the stub test.

## ask_bid_price (WS example frame; JAVA for the sizes)

`abp_price_only` is WS's shape, prices only. `abp_sized` and `abp_sized_next` carry
`bidSize` and `askSize` as JAVA reads them (no public frame shows them); `abp_ask_size_missing`
lacks `askSize`; `abp_crossed` has its bid above its ask. Prices are on the tick (six places).

## trades (WS example frame; SPEC; SDK `Trade`; JAVA for the array)

`trade_single` is WS's `{"trade":{..}}` shape, printing at 2000.051234 inside `eth_s0`'s spread;
`trades_array` is the `{"trades":[..]}` shape JAVA reads, which REST's recent trades answer
uses. `timestamp` is whole seconds, as WS's example and SDK's `Trade` show. The tests also
edit them to carry `side` in place of `takerSide`, the alias JAVA reads (L865).

## mark_price, spot_price (WS example frames)

`mark_price` (`markPrice`) and `spot_price` (`spotPrice`). The tests also edit `mark_price` to
carry `price` in place of `markPrice`, the alias JAVA reads (L581).

## funding_rate_estimation (WS example frame; SPEC; JAVA for the flat shape)

`funding_nested` is WS's shape (`fundingRateEstimation.estimatedFundingRate`); `funding_flat`
is JAVA's first read (`estimatedFundingRate` under `data`). Each estimate is an eight-hour rate,
and the codec pushes its eighth as the rate per the one-hour interval (the owner's conclusion of
2026-10-28, FBC-204c; JAVA's `onFundingRateUpdate` L914-L918 divides it by 8), so the rates are
chosen against that division: `funding_nested`'s 0.000012 and `funding_flat`'s -0.000007 divide
exactly, `funding_finest` carries 8e-12, whose eighth, 1e-12, is the finest `MdEvent::Funding`
holds, `funding_indivisible` 12e-12, whose eighth, 1.5e-12, is finer and so refused, and
`funding_too_fine` 5e-13, finer than 1e-12 itself. Each carries a `nextFundingTimestamp`, which
the codec does not read: no source pins its unit.

## Control

`ack`: WS's answer to an unsubscribe, `{"id":..,"status":200}`.
