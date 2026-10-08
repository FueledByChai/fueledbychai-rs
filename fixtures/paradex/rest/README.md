# Paradex REST responses

`orderbook-btc-2002.json` is hand-built in the shape docs.paradex.trade documents for "Get market orderbook"
(`GET /v1/orderbook/{market}`, query `depth`): `market`, `seq_no` ("Sequence number of the
orderbook"), `last_updated_at` (milliseconds), `bids` and `asks` as lists of `[price, size]`
decimal strings, and the `best_ask_api`, `best_ask_interactive`, `best_bid_api` and
`best_bid_interactive` pairs, which the decoder does not read. No account value is here (0009).

| File | What it is |
| --- | --- |
| `orderbook-btc-2002.json` | BTC-USD-PERP at depth 15, seq_no 2002: the book the hand-built frames `../md/book15-snapshot-2000.sbe.txt`, `book15-delta-2001.sbe.txt` and `book15-delta-2002.sbe.txt` build, 15 levels per side |
| `markets.json` | `GET /v1/markets` ("List available markets"): `{"results": [MarketResp, ...]}` with every field of the documented example. BTC-USD-PERP and ETH-USD-PERP are `STANDARD` perpetuals (BTC's `position_limit` is off its size step, to be floored; ETH funds hourly with a 2.5% price band); BTC-USD-67000-C is a `PERP_OPTION` and SOL-USD-PERP is `RFQ_ONLY`, both passed over. The values are made up, not Paradex's; the feed id is `SYNTHETIC-FEED-ID` |

`tests/md_oracles.rs` decodes the order book and checks it against the delta-built book at
seq_no 2002; `tests/discover.rs` decodes `markets.json` into instrument drafts (FBC-l5o).
