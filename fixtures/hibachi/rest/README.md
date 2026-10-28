# Hibachi REST fixtures

Hand-built answers in the shapes Hibachi publishes, for `crates/venues/fbc-venue-hibachi`'s
discovery and funding tests (FBC-jyyl). Nothing here was recorded from Hibachi, and no test
sends anything to it; `SYNTHETIC` says where the values come from.

Sources, cited below by name:

- **SPEC**: Hibachi's Market API spec, <https://hibachi-docs.redocly.app/marketapi/market>
  (the OpenAPI document `https://hibachi-docs.redocly.app/_spec/marketapi/index.yaml`):
  `ExchangeInfoResponse`, `ContractData` (`Contract` with `tickSize`, `stepSize`,
  `minOrderSize`, `minNotional`), `ContractStatus` (`LISTED`, `PRE_OPEN`, `LIVE`, `PAUSED`,
  `CLOSED`), `ExchangeStatus` (`NORMAL`; `SCHEDULED_MAINTENANCE` with `currentMaintenanceWindow`;
  `UNSCHEDULED_MAINTENANCE`), `MaintenanceInfo` and `MaintenanceWindow`.
- **SDK**: Hibachi's Python SDK at 14a76b9,
  <https://github.com/hibachi-xyz/hibachi_sdk/tree/14a76b95230869dd622c31a1fc4132a6eac1a9fa/python>:
  `hibachi_xyz/types.py` (`FutureContract`, `MaintenanceWindow`, `ExchangeInfo`) and
  `tests/unit/data/response.exchange_info.0.json`.
- **AD**: <https://api-doc.hibachi.xyz/>, Market API: the Exchange Listings folder and the
  `/market/exchange-info` and `/market/data/funding-rates` items.
- **JAVA**: the public Java library at
  [b4188a5](https://github.com/FueledByChai/FueledByChaiTrading/tree/b4188a57f7d5ea4e82260c9f3c4a0a359ad04c27):
  `commons/hibachi-common-api/src/main/java/com/fueledbychai/hibachi/common/api/HibachiRestApi.java`
  (`parseContract` L314-L342, `resolveEffectiveTickSize` L394-L435),
  `commons/hibachi-common-api/src/test/java/com/fueledbychai/hibachi/common/api/HibachiRestApiTickTest.java`,
  `implementations/broker-api/hibachi-broker-api-impl/src/test/java/com/fueledbychai/broker/hibachi/HibachiTranslatorTriggerOrderTest.java`
  L32-L39 (an `EUR/USDT-P` contract) and
  `market-data-collector/src/main/java/com/fueledbychai/datacollector/FundingSources.java`
  L139-L158 (the funding history's shape).
- **FX**: <https://docs.hibachi.xyz/hibachi-docs/trading/fx-trading> (FX pairs EUR/USD and
  GBP/USD; the weekend halt Friday 17:00 to Sunday 17:00 ET, then `PAUSED`).
- **TEMP**: <https://docs.hibachi.xyz/hibachi-docs/trading/temp-markets> (each temp market has
  "a predefined expiration time").

## `exchange_info.json`

The answer's top level: `feeConfig` (SPEC `ExchangeFeeConfig`, made-up rates, with no
`instantWithdrawDstPublicKey`: the published examples' value is key-shaped and is not copied),
`futureContracts`, `instantWithdrawalLimit`, `maintenanceWindow` (empty) and `status`
`NORMAL` (SPEC; AD's example). Each contract has the fields SPEC's `ContractData`, SDK's
`FutureContract` and AD's example share, but for `status`, which AD's older example states as
`"live": true` instead: `id` (a whole number, SPEC `u32`), `symbol`
(`<underlying>/<quote>-P`), `displayName`, `underlyingSymbol`, `settlementSymbol`,
`underlyingDecimals` and `settlementDecimals` (whole numbers), `tickSize`, `stepSize`,
`minOrderSize` and `minNotional` (decimal strings), `orderbookGranularities` (decimal strings),
`status`, `initialMarginRate` and `maintenanceMarginRate` (SDK, AD; not read), and
`marketOpenTimestamp` and `marketCloseTimestamp` where shown (SPEC: a string or null).

| Contract | What it pins |
| --- | --- |
| `ETH/USDT-P` (id 1) | `LIVE`, the exchange `NORMAL`: trading. AD's and SDK's public example values for its id, decimals (9, 6), tick (0.000001), step (0.000000001) and granularities (0.01 to 10), so its quote grid, 0.01, is coarser than its tick; a made-up `minOrderSize` of 1000 steps; both market timestamps `null`; SDK's `marketCreationTimestamp` (not read) |
| `SOL/USDT-P` | `LIVE`; its tick, 0.001, listed among its granularities, so its quote grid equals its price grid (`quote_grid: None`); both market timestamps absent; its own sizes and `minNotional` (5) |
| `EUR/USDT-P` (id 2) | `LIVE` FX (FX): the New York weekend calendar. JAVA's test contract's id, decimals (2, 6), tick and step |
| `GBP/USDT-P` | `PAUSED` FX: halted, with the weekend calendar (FX: the weekend halt "transitions to PAUSED") |
| `DOGE/USDT-P` | `LISTED`: halted; `underlyingDecimals` 0 |
| `AVAX/USDT-P` | `PRE_OPEN` with an advertised `marketOpenTimestamp`: halted, the calendar closed before it |
| `XRP/USDT-P` | `CLOSED`: halted |
| `WIF/USDT-P` | `LIVE` with an advertised open and close in decimal seconds (TEMP: a temp market's expiration): trading, the calendar closed before the open and from the close on |
| `SUI/USDT-P` | no granularity listed: quoted on its tick (JAVA `resolveEffectiveTickSize` falls back to it) |
| `APT/USDT-P` | no well-formed granularity (blank, unparsable, negative, zero, `null`): quoted on its tick |
| `SEI/USDT-P` | every granularity finer than its tick: quoted on its tick |

## Variants

Each is a complete answer of a few of the contracts above, changed in one way.

| File | Change | Expected |
| --- | --- | --- |
| `exchange_info_scheduled_maintenance.json` | `status` `SCHEDULED_MAINTENANCE`, with the `currentMaintenanceWindow` SPEC requires then and the window in `maintenanceWindow`, its bounds JSON numbers as in SDK's test data | every contract halted, ETH and EUR `LIVE` included |
| `exchange_info_unscheduled_maintenance.json` | `status` `UNSCHEDULED_MAINTENANCE` | every contract halted |
| `exchange_info_status_absent.json` | SOL without `status`, with `"live": true` instead, as AD's older example shows its contracts | refused: `status` missing |
| `exchange_info_status_null.json` | SOL's `status` `null` | refused: `status` missing |
| `exchange_info_status_undocumented.json` | SOL's `status` `live`, lowercase | refused: `status` malformed |
| `exchange_info_close_ms.json` | WIF's `marketCloseTimestamp` a count of milliseconds, after 2100-01-01 read as seconds | refused: `marketCloseTimestamp` malformed |
| `exchange_info_close_not_number.json` | WIF's `marketCloseTimestamp` an ISO 8601 date, not a number | refused: `marketCloseTimestamp` malformed |

## `funding_rates.json`

A `GET /market/data/funding-rates?symbol=ETH%2FUSDT-P` answer, `{"data": [...]}`, each record
`contractId`, `fundingTimestamp` (epoch seconds), `fundingRate` and `indexPrice`, the shape JAVA's
`FundingSources.java` reads (AD documents the request's parameters but shows no response body).
The JSON types are a choice no public source pins: whole-number `contractId` and
`fundingTimestamp`, decimal strings `fundingRate` and `indexPrice`, as Hibachi writes its other
decimals. Six consecutive settlements, each on the UTC hour and 3600 s after the one before; the
rates and prices are made up. Each `fundingRate` is a rate for its one-hour settlement, as JAVA
annualizes it (the owner's conclusion of 2026-10-28, FBC-204c), so the crate states funding
`Known` at the spacing the test derives from them.
