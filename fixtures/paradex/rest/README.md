# Paradex REST responses

Hand-built in the shape docs.paradex.trade documents for "Get market orderbook"
(`GET /v1/orderbook/{market}`, query `depth`): `market`, `seq_no` ("Sequence number of the
orderbook"), `last_updated_at` (milliseconds), `bids` and `asks` as lists of `[price, size]`
decimal strings, and the `best_ask_api`, `best_ask_interactive`, `best_bid_api` and
`best_bid_interactive` pairs, which the decoder does not read. No account value is here (0009).

| File | What it is |
| --- | --- |
| `orderbook-btc-2002.json` | BTC-USD-PERP at depth 15, seq_no 2002: the book the hand-built frames `../md/book15-snapshot-2000.sbe.txt`, `book15-delta-2001.sbe.txt` and `book15-delta-2002.sbe.txt` build, 15 levels per side |

`tests/md_oracles.rs` decodes it and checks it against the delta-built book at seq_no 2002.
