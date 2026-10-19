# Binance USD-M futures fixtures

Hand-written frames and REST responses in the shapes Binance documents for its USD-M futures
WebSocket market streams and market-data REST API. Nothing here was recorded from the venue; recorded fixtures follow in FBC-64r. The
values are illustrative: the field names, nesting and types follow the documentation, the
numbers are adapted from its examples so they sit on the synthetic grids the tests declare.

| File | Shape | Source |
| --- | --- | --- |
| `md/book_ticker.jsonl` | `<symbol>@bookTicker` payloads (`e`, `u`, `E`, `T`, `s`, `b`, `B`, `a`, `A`) inside the combined-stream wrapper `{"stream":...,"data":...}` | [Individual Symbol Book Ticker Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Individual-Symbol-Book-Ticker-Streams); wrapper from [Connect](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Connect) |
| `md/partial_depth.jsonl` | `<symbol>@depth<levels>@<speed>` payloads (`e: depthUpdate`, `E`, `T`, `s`, `U`, `u`, `pu`, `b`, `a`) inside the combined-stream wrapper | [Partial Book Depth Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Partial-Book-Depth-Streams) |
| `md/diff_depth.jsonl` | five `<symbol>@depth@100ms` events (`e: depthUpdate`, `E`, `T`, `s`, `U`, `u`, `pu`, `b`, `a`) whose `pu` chain is unbroken: e0 ends before `rest/depth_snapshot.json`'s `lastUpdateId`, e1 straddles it, e2 to e4 follow | [Diff. Book Depth Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams); the anchoring rules from [How to manage a local order book correctly](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly) |
| `md/diff_depth_gap.jsonl` | the clean sequence without e3: e4's `pu` names an event never received | as above |
| `md/diff_depth_duplicate.jsonl` | the clean sequence without e4, with e2 received twice | as above |
| `md/diff_depth_swapped.jsonl` | the clean sequence with e2 and e3 swapped | as above |
| `rest/depth_snapshot.json` | a `GET /fapi/v1/depth` response (`lastUpdateId`, `E`, `T`, `bids`, `asks`), the first anchor | [Order Book](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Order-Book) |
| `rest/depth_snapshot_resync.json` | the anchor asked for after a gap or the swapped pair: its `lastUpdateId` lies inside e4 | as above |
| `rest/exchange_info.json` | a `GET /fapi/v1/exchangeInfo` response (`timezone`, `serverTime`, `rateLimits`, `exchangeFilters`, `assets`, `symbols`), each symbol with the documented fields and its `filters` (`PRICE_FILTER`, `LOT_SIZE`, `MARKET_LOT_SIZE`, `MAX_NUM_ORDERS`, `MAX_NUM_ALGO_ORDERS`, `MIN_NOTIONAL`, `PERCENT_PRICE`): three `PERPETUAL`s trading (BTCUSDT, ETHUSDT, BTCUSDC), a `CURRENT_QUARTER` delivery contract, a `TRADIFI_PERPETUAL`, a settling symbol whose `contractType` is empty, and a closed `PERPETUAL` | [Exchange Information](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Exchange-Information); the `contractType` and `status` values and the filter rules from [Common Definition](https://developers.binance.com/docs/derivatives/usds-margined-futures/common-definition) |
| `md/replies.jsonl` | the reply to a live `SUBSCRIBE` (`{"result":null,"id":1}`) and an error reply (`{"code":2,"msg":...,"id":2}`) | [Live Subscribing/Unsubscribing to streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Live-Subscribing-Unsubscribing-to-streams) |

One frame per line in `md/`, one response body per file in `rest/`. In every diff-depth test the
snapshot answers after the first two frames, which the codec buffers. The tests in `crates/venues/fbc-venue-binance-usdm/tests/` read them and
state the events each must decode to.
