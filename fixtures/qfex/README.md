These public market-data frames are hand-written copies of the examples in QFEX's
[BBO](https://docs.qfex.com/websocket/channels/mds/bbo),
[Pulsed Order Book](https://docs.qfex.com/websocket/channels/mds/orderbook), and
[Public Trades](https://docs.qfex.com/websocket/channels/mds/trades) documentation as read on
2026-10-20, and `md/mark_price.json`, `md/underlier.json` and `md/funding.json` of the
[Mark Price](https://docs.qfex.com/websocket/channels/mds/mark_price),
[Underlier](https://docs.qfex.com/websocket/channels/mds/underlier) and
[Funding Rate](https://docs.qfex.com/websocket/channels/mds/funding_rate) examples as read on
2026-11-04 (the mark price example has no `sequence`; the funding example names no
`annualised_funding_rate`, which live frames carry). They contain no private account data or
credentials. Test instrument grids are synthetic so the published example numbers can be
represented exactly.

`array.json` and `contents.json` wrap those same examples in the two batch forms
handled by [QFEX's CLI MDS client](https://github.com/QFEX-org/cli/blob/main/internal/daemon/mds.go).
They are composition fixtures, not captured live frames.

`live/nvda-2026-10-20.raw.ndjson` and its field-filtered projection
`live/nvda-2026-10-20.ndjson` are one 45-second, read-only public MDS capture of
`NVDA-USD` on 2026-10-20, approximately 20:54:38–20:55:22 UTC. The capture
subscribed to `bbo`, `level2`, and `trade` on `wss://mds.qfex.com/`. It contains
89 BBO messages, 89 level2 messages and three public trades. The original
capture reported zero errors, but it classified error envelopes as other
messages; absence of subscription errors cannot be established from that run.
The server-time median gap was 500 ms on both pulsed feeds (observed range
453–547 ms). Sequence numbers were consecutive from 1 through 181 across the
three channels in this connection; the trades consumed 29, 126 and 155. The level2
book changed depth and prices between snapshots, giving a live replacement
case. This observation does not turn BBO's undocumented interval or the
sequence scope into a protocol guarantee.

This capture was published on main through [PR #198](https://github.com/FueledByChai/fueledbychai-rs/pull/198)
at commit `8faa0de324a93e5ab2eba2abf7f6835eb15cb64b`. SHA-256 digests of the
committed fixture bytes are shown in two halves; concatenate each row's halves
without a separator to obtain its 64-character digest:

| File | First 32 hex characters | Last 32 hex characters |
| --- | --- | --- |
| `live/nvda-2026-10-20.raw.ndjson` | `1d7bece00f914beca69c69bc3c0ecf56` | `6d5c61100f3e06cd17b7fe29b0209239` |
| `live/nvda-2026-10-20.ndjson` | `088645740e6439eb80e175f47f8392b1` | `68f177ede672e0dca6ea74fd2a1d19a5` |

These hashes identify the bytes committed to this repository; they are not a
server attestation of the captured frames.

`live/suig-2026-11-04.raw.ndjson` and its projection `live/suig-2026-11-04.ndjson` are one
60-second, read-only public MDS capture of `SUIG-USD` on 2026-11-04, started at
18:06:28.920 UTC (the instant `recv_ms` counts from), within US regular trading hours. It
subscribed to `mark_price`, `underlier`, `funding` and `market_stats` on
`wss://mds.qfex.com/` and holds 150 frames: 60 mark prices, 27 underliers, three funding
frames and 60 `market_stats` snapshots. The run reported zero errors; its four other
messages were the subscription acknowledgments. Sequence numbers run 1 through 150 across
the four channels in this connection. Mark prices came once a second, each with a
`sequence`, which the mark price page's example lacks. 26 of the 27 underliers have
`source: internal` and one `external`, so `internal` is not given only outside extended
hours, as the underlier page has it; the codec decodes the external one as the index and
counts the 26.

The funding frames carry `annualised_funding_rate` beside `funding_rate` and
`time_remaining`. The first, delivered on subscribing, is the last pulse before the capture
(`time` 18:06:17.593); the next two are stamped 30 seconds apart. All three give
`funding_rate` `-0.00010`, with `annualised_funding_rate` `-0.6345`, `-0.6340` and
`-0.6335` and `time_remaining` 3218, 3188 and 3158. The 60 `market_stats` snapshots repeat
those three states in their `funding_rates`. What they settle:

- **The unit.** `annualised_funding_rate` is not `funding_rate` x 8760 (that would be
  `-0.876`). Every rate is written to exactly five decimal places and every annualised rate
  to four, so allowing each its half-unit of rounding, the annualised rate is the rate
  times a factor between 6,042 and 6,669. That is fewer than the 8,760 hours of a year
  and more than its 1,095 eight-hour periods, and far from the 87.6 a percentage rate
  annualised as a fraction would give. Both values are therefore decimal fractions, as
  market_stats says ("decimal strings, not percentage strings") and the
  [funding page](https://docs.qfex.com/qfex/funding) says too as read on 2026-11-04
  ("decimal fractions", `0.001` being 10 bps), and the rate is per
  hourly settlement. The factor appears to count only the hours the symbol funds: the
  funding page counts samples outside funding sessions as zero. No rescaling is needed:
  `MdEvent::Funding.rate_e12` is `funding_rate` x 10^12 exactly. The wire's resolution
  is 1e-5, so `-0.00010` is `-100_000_000`.
- **`time_remaining`.** It is in seconds, as market_stats says. `time` +
  `time_remaining` is 18:59:55.59 for each of the three states, 4.4 seconds short of the
  19:00:00 hour, so it does not land on a settlement boundary. The second and third
  frames arrived at 18:06:52.47 and 18:07:22.43 (start plus `recv_ms`), and receipt plus
  `time_remaining` is 19:00:00.47 and 19:00:00.43. So `time_remaining` counts whole
  seconds from when a pulse is sent, about 4.9 seconds after its `time` here, to the top of
  the hour. The first, a replayed pulse, arrived 12 seconds after its `time` with its
  `time_remaining` unchanged. No field states the settlement instant, so the caps declare
  `next_time_reported: false` and `MdEvent::Funding.next` is `None`. The top of the UTC
  hour is an inference from the hourly settlement, not a field of the frame.

`crates/venues/fbc-venue-qfex/tests/md_prices.rs` proves the counts, the sequence, the
decoding and each finding on the unit and `time_remaining` against the committed files and
the start instant above.

| File | First 32 hex characters | Last 32 hex characters |
| --- | --- | --- |
| `live/suig-2026-11-04.raw.ndjson` | `1dcfe48bca0cf713f26bd59ce63f2ae5` | `1a5bf927bcc9ac5d042fbc55577023d0` |
| `live/suig-2026-11-04.ndjson` | `262c13acc37447ea24651354b79c8598` | `aa737799f033b5edb246794159c687db` |

Each `raw.ndjson` line holds an accepted public JSON payload string under `raw`.
For a single-item frame this is the exact received string; for an array or
`contents` batch, the script serializes each flattened public item separately so
every raw line has one paired projection line. It omits subscription
acknowledgments and counts error envelopes without saving their contents. An
unexpected early WebSocket close also increments the error count and ends the
capture. The paired projection carries type, symbol,
sequence, venue time, public bid/ask levels and `sig_figs` when present, and for trades the
public UUID, price, size, side and execution type. For mark prices and underliers it
carries the price, and the underlier's `source`. For funding it carries the rate,
`time_remaining` and, when present, the annualised rate. For `market_stats` it carries the
six component arrays. Each entry of those arrays must have exactly the fields the
market_stats page lists and name the captured symbol; otherwise the snapshot counts as an
error and is not saved. Both files carry the same
`recv_ms`, a local monotonic offset from the start of capture rounded to
milliseconds. The fixture test checks that parsing each saved raw payload gives
exactly its paired projection. The script connects with no credential, sends
no orders, and writes no private frames. To repeat a capture with Node 24 or later:

```sh
node fixtures/qfex/capture_public.mjs NVDA-USD 30000 /tmp/qfex-public.ndjson
node fixtures/qfex/capture_public.mjs SUIG-USD 60000 /tmp/qfex-public.ndjson \
  mark_price,underlier,funding,market_stats
```

The optional fourth argument is a comma-separated list of channels, one subscription each
(by default `bbo,level2,trade`). The summary the script prints gives the wall-clock instant
`recv_ms` counts from (`startedAt`), which neither file carries. The output path must end in
`.ndjson`; the script writes the companion file with `.raw.ndjson`. Its offline capture tests run with:

```sh
node --test fixtures/qfex/capture_public.test.mjs
```
