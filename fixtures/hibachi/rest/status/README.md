# Hibachi exchange-info answers for the status poll

Hand-built `GET /market/exchange-info` answers for `crates/venues/fbc-venue-hibachi`'s status
poll (FBC-1632, decision 0130), which re-reads each subscribed market's trading status after
discovery. Nothing here was recorded from Hibachi, and no test sends anything to it; `SYNTHETIC`
says where the values come from. The sources (SPEC, SDK, AD, FX, TEMP) are those of
[`../README.md`](../README.md), and the call-auction docs,
<https://docs.hibachi.xyz/hibachi-docs/trading/call-auctions>.

Every answer holds three contracts of [`../exchange_info.json`](../exchange_info.json), with its
`feeConfig` and `instantWithdrawalLimit`: `ETH/USDT-P` (crypto, no calendar), `EUR/USDT-P` (FX:
the New York weekend calendar, FX) and `WIF/USDT-P` (a temp market, TEMP), each `LIVE` with both
market timestamps `null` unless the table says otherwise, the exchange `NORMAL` and
`maintenanceWindow` empty. The tests discover their spec table from `live.json`, so no calendar
holds an advertised close or open; a later answer's advertised close or open is the poll's alone.

The instants are Unix seconds chosen for a test's timeline: 1792584000 is
2026-10-21T12:00:00Z, a Wednesday; 1792788600 is 2026-10-23T20:50:00Z, ten minutes before the FX
weekend closes (Friday 17:00 New York, EDT).

| File | Change | What the poll reports |
| --- | --- | --- |
| `live.json` | none | each market trading |
| `eth_paused.json` | ETH `PAUSED` (FX: the weekend halt "transitions to PAUSED") | ETH halted |
| `scheduled_maintenance.json` | the exchange `SCHEDULED_MAINTENANCE`, with SPEC's `currentMaintenanceWindow` (not read) and the same window, 1792584290 to 1792584330, in `maintenanceWindow`, its bounds JSON numbers as in SDK's test data | every market halted |
| `unscheduled_maintenance.json` | the exchange `UNSCHEDULED_MAINTENANCE` | every market halted |
| `window_number.json` | a window from 1792788690 to 1792788750.0 in `maintenanceWindow`, its bounds JSON numbers (a whole number and one with a fraction, SDK) | every market halted at the begin |
| `window_string.json` | the same window, its bounds strings (SPEC types both as strings) | the same as `window_number.json` |
| `window_ms.json` | the same window in milliseconds, after 2100-01-01 read as seconds | refused: `maintenanceWindow` malformed |
| `wif_close.json` | WIF's `marketCloseTimestamp` `"1792584150"` (TEMP: a predefined expiration time) | WIF halted at that instant |
| `eth_close_first.json` | ETH's `marketCloseTimestamp` `"1792584250"`, which no earlier answer advertised, beside WIF's close | ETH halted when it is judged after that close |
| `eur_left_out.json` | EUR not listed, beside WIF's close | EUR halted |
| `close_ms.json` | WIF's `marketCloseTimestamp` in milliseconds, after 2100-01-01 read as seconds | refused: `marketCloseTimestamp` malformed |
| `close_not_number.json` | WIF's `marketCloseTimestamp` an ISO 8601 date, not a number | refused: `marketCloseTimestamp` malformed |
| `status_null.json` | ETH's `status` `null` | refused: `status` missing |
