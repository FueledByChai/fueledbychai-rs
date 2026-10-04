# Paradex SBE market-data frames

Hand-built, except one captured frame: each `.sbe.txt` file is one binary frame written as
whitespace-separated hex bytes, one field per line, `#` to the end of a line a comment, and the
one `.sbe` file is a frame's raw bytes. No account, order or fill frame and no account value is
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

The snapshot and the deltas at 1001, 1002 and 1003 are a continuous sequence; 1004 after 1002
is a skipped seq_no, and 1001 or 1002 after 1002 a backwards one (`tests/md_book.rs`).

The `book15-` frames are a continuous depth-15 sequence whose book at seq 2002 is the REST
snapshot `../rest/orderbook-btc-2002.json` (`tests/md_oracles.rs`).

## The captured frame

`btc-book-delta-2026-09-23.sbe` is one `BookEvent` DELTA for BTC-USD-PERP (seq_no 7678386728,
ts 1790187959762000 us, one ask level at 84209.4 removed), received from Paradex's public
production WebSocket (`wss://ws.api.prod.paradex.trade/v1?sbeSchemaId=1&sbeSchemaVersion=1`) on
2026-09-23 and kept as the `LIVE_BOOK_DELTA_BTC` hex constant of FueledByChaiTrading's
`ParadexSbeTranscoderTest` (branch `paradex-sbe`, `commons/paradex-common-api`). It is public
market data: an order book level of a public channel, with no account, order or fill in it. The
bytes here are that constant, unchanged.
