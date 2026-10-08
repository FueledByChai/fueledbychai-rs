# Paradex SBE market-data frames

Hand-built, except the captured frames: each `.sbe.txt` file is one binary frame written as
whitespace-separated hex bytes, one field per line, `#` to the end of a line a comment, each
`.sbe` file is a frame's raw bytes, and each `.jsonl` file a captured session's frames, one JSON
object per line (see below). No account, order or fill frame and no account value is
here (0009).

Layouts follow Paradex's published schema, `paradex_1_0.xml` in
[tradeparadex/paradex-py](https://github.com/tradeparadex/paradex-py/blob/b8248fb747e278d2167ac2f056b339a287d5ef30/paradex_py/api/sbe/paradex_1_0.xml)
at commit `b8248fb747e278d2167ac2f056b339a287d5ef30`, the schema docs.paradex.trade's
"Binary Encoding (SBE)" page names. The adapter negotiates schema 1:1
(`?sbeSchemaId=1&sbeSchemaVersion=1`), whose `BboEvent` and `TradeEvent` layouts are the ones
that file describes for versions below 2.

| File | What it is |
| --- | --- |
| `bbo.sbe.txt` | `BboEvent` (template 2), 48-byte root block |
| `bbo-ask-empty.sbe.txt` | The same with null ask price and size (no asks) |
| `bbo-longer-block.sbe.txt` | The same with a 56-byte root block: 8 bytes past the known fields, skipped |
| `trade.sbe.txt` | `TradeEvent` (template 1), 50-byte root block |
| `trade-longer-block.sbe.txt` | The same with a 58-byte root block and appended var data (`tradeIdStr`, as 1:2 appends it), both skipped |
| `heartbeat.sbe.txt` | `HeartbeatEvent` (template 40), skipped by the decoder |
| `book-snapshot.sbe.txt` | `BookEvent` (template 3), a SNAPSHOT at seq 1000: two bids, two asks |
| `book-delta-1001.sbe.txt` | A DELTA at seq 1001: one bid removed (size 0), one ask changed |
| `book-delta-1002.sbe.txt` | A DELTA at seq 1002: one bid added |
| `book-longer-entries.sbe.txt` | A DELTA at seq 1003 whose group entries are 24 bytes, 8 past the known price and size (skipped) |
| `book-delta-1004.sbe.txt` | A DELTA at seq 1004: after 1002, seq 1003 is skipped |
| `btc-book-delta-2026-09-23.sbe` | Captured, raw bytes: see below |
| `book15-snapshot-2000.sbe.txt` | A SNAPSHOT at seq 2000: 15 bids (62000.0 down by 0.5) and 15 asks (62000.5 up by 0.5) |
| `book15-delta-2001.sbe.txt` | A DELTA at seq 2001: one bid changed, one ask removed, one ask added below the 15th |
| `book15-delta-2002.sbe.txt` | A DELTA at seq 2002: a new best bid, the worst bid removed |
| `book15-delta-2003-empty.sbe.txt` | A DELTA at seq 2003 with no levels: the book unchanged, the sequence advanced |
| `eth-book-snapshot-500.sbe.txt` | An ETH-USD-PERP SNAPSHOT at seq 500: two bids, two asks |
| `eth-book-delta-501.sbe.txt` | An ETH-USD-PERP DELTA at seq 501: one bid changed, one ask added |
| `eth-book-snapshot-502.sbe.txt` | An ETH-USD-PERP SNAPSHOT at seq 502, as a new subscription receives it: two bids, one ask |
| `eth-book-delta-503.sbe.txt` | An ETH-USD-PERP DELTA at seq 503: one bid added, one ask removed, one ask added |
| `markets-summary-v0.sbe.txt` | `MarketSummaryEvent` (template 4) at schema version 0: a 216-byte root block, fundingRate only at 8 decimals |
| `markets-summary-v1.sbe.txt` | The same values at schema version 1: a 240-byte root block, with `forwardRate`, `riskFreeRate` and `fundingRatePrecise` appended |
| `eth-markets-summary-2026-09-23.sbe` | Captured, raw bytes, version 1: see below |
| `btc-order-book-deltas-2026-10-08.jsonl` | Captured: BTC-USD-PERP's `order_book.BTC-USD-PERP.deltas` session, see below |
| `btc-order-book-interactive-deltas-2026-10-08.jsonl` | Captured: BTC-USD-PERP's `order_book.BTC-USD-PERP.interactive_deltas` session, see below |
| `btc-bbo-interactive-2026-10-08.jsonl` | Captured: BTC-USD-PERP's `bbo.BTC-USD-PERP.interactive` session, see below |
| `btc-bbo-2026-10-08.jsonl` | Captured: BTC-USD-PERP's `bbo.BTC-USD-PERP` session, see below |

The snapshot and the deltas at 1001, 1002 and 1003 are a continuous sequence; 1004 after 1002
is a skipped seq_no, and 1001 or 1002 after 1002 a backwards one (`tests/md_book.rs`).

The `eth-book-` frames are ETH-USD-PERP's: 500 and 501 a continuous sequence, and 502 and 503
another, from the fresh snapshot a new subscription receives. With the BTC frames they give the
decoder-replay test (`tests/replay.rs`) books of two markets on one connection, through BTC's
gap and the reconnect that resyncs both.

The `book15-` frames are a continuous depth-15 sequence whose book at seq 2002 is the REST
snapshot `../rest/orderbook-btc-2002.json` (`tests/md_oracles.rs`).

The two `markets-summary-` frames carry the same values, field for field, each in the layout
its version describes (the schema's `sinceVersion="1"` fields are the three appended); the
`fundingRate` both carry, 0.00001234, is `fundingRatePrecise`, 0.000012345678, cut to 8
decimals. docs.paradex.trade ("Binary Encoding (SBE)", Schema versioning) names the two block
lengths: "Version 1 of `MarketSummaryEvent` has a 240-byte root block; version 0 has 216"
(`tests/md_summary.rs`).

## The captured frames

`btc-book-delta-2026-09-23.sbe` is one `BookEvent` DELTA for BTC-USD-PERP (seq_no 7678386728,
ts 1790187959762000 us, one ask level at 84209.4 removed), received from Paradex's public
production WebSocket (`wss://ws.api.prod.paradex.trade/v1?sbeSchemaId=1&sbeSchemaVersion=1`) on
2026-09-23 and kept as the `LIVE_BOOK_DELTA_BTC` hex constant of FueledByChaiTrading's
`ParadexSbeTranscoderTest` (branch `paradex-sbe`, `commons/paradex-common-api`). It is public
market data: an order book level of a public channel, with no account, order or fill in it. The
bytes here are that constant, unchanged.

`eth-markets-summary-2026-09-23.sbe` is one `MarketSummaryEvent` for ETH-USD-PERP at schema
version 1 (240-byte root block; mark 2662.8925496, fundingRatePrecise 0.000443605046, seq 0),
received from the same WebSocket and URL on 2026-09-23 and kept as the
`LIVE_MARKET_SUMMARY_ETH` hex constant of the same Java test. It is public market data, a
market's ticker, with no account, order or fill in it. The bytes here are that constant,
unchanged.

`btc-order-book-deltas-2026-10-08.jsonl` and `btc-order-book-interactive-deltas-2026-10-08.jsonl`
are two sessions of Paradex's public production WebSocket, at the same URL, captured on
2026-10-08 (UTC) for FBC-yj56 by subscribing one channel each: `order_book.BTC-USD-PERP.deltas`
(30.1 s) and `order_book.BTC-USD-PERP.interactive_deltas` (23.9 s). Each line is one
frame as received: `t_ns` (the capturing machine's receive time, nanoseconds since the Unix
epoch), `opcode` (`text` or `binary`), and the frame, in `text` or in standard base64 in `b64`.
The first line is the subscribe acknowledgement naming the channel; then 289 and 299
`BookEvent` frames (about 9.6 and 12.5 a second), each capture opening with a SNAPSHOT of the
whole book (115 bids and 60 asks; 120 and 64) followed by DELTAs at consecutive seq_nos (7687289234 to 7687289522; 7687289525 to
7687289823). It is public market data: order book levels of a public channel and the venue's
acknowledgement, with no account, order, fill, credential or token in it. The files are the
capture, unchanged (`tests/md_live_capture.rs`, decision 0074).

`btc-bbo-interactive-2026-10-08.jsonl` and `btc-bbo-2026-10-08.jsonl` are two more sessions of
the same WebSocket and URL, captured on 2026-10-08 (UTC) for FBC-taxd, one after the other, by
subscribing one channel each: `bbo.BTC-USD-PERP.interactive` (about 25 s, from 01:11:36) and
`bbo.BTC-USD-PERP` (about 12 s, from 01:12:03). The line format is the book captures'. The first
line is the subscribe acknowledgement naming the channel; then 299 `BboEvent` frames each
(template 2), seq 7687292177 to 7687292528 and 7687292570 to 7687292898. It is public market
data: the best bid and offer of a public channel and the venue's acknowledgement, with no
account, order, fill, credential or token in it. The files are the capture, unchanged
(`tests/md_touch_interactive.rs`, decision 0076).

`multimarket-order-book-2026-10-08.jsonl` is one session of the same WebSocket and URL, captured
on 2026-10-08 from 02:53:08 UTC (about 15 s) for FBC-2976. It subscribed three book channels of
three markets in turn: `order_book.BTC-USD-PERP.deltas` (JSON-RPC id 1),
`order_book.ETH-USD-PERP.deltas` (id 2) and `order_book.SOL-USD-PERP.interactive_deltas` (id 3).
The line format is the book captures'. Three lines are the acknowledgements, one per id, each
naming its channel; the first comes before BTC's first frame and the other two just after it.
The other 155 lines are `BookEvent` frames, interleaved as they arrived: 89 for BTC (seq
7687353140 to 7687353228), 63 for ETH (8106617644 to 8106617706) and 3 for SOL (6182946936 to
6182946938). Each market's frames open with a SNAPSHOT of its whole book (100 bids and 67 asks;
124 and 30; 87 and 29) followed by DELTAs at consecutive seq_nos. Every level lies on a 0.1
price tick and a 0.00001 size step for BTC, 0.01 and 0.0001 for ETH, and 0.001 and 0.01 for
SOL. It is public market data: order book levels of public channels and the venue's
acknowledgements, with no account, order, fill, credential or token in it. The file is the
capture, unchanged (`tests/md_live_multimarket.rs`, decision 0077).
