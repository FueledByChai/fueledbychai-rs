# Binance USD-M futures fixtures

Hand-written frames in the shapes Binance documents for its USD-M futures WebSocket market
streams. No frame here was recorded from the venue; recorded fixtures follow in FBC-64r. The
values are illustrative: the field names, nesting and types follow the documentation, the
numbers are adapted from its examples so they sit on the synthetic grids the tests declare.

| File | Shape | Source |
| --- | --- | --- |
| `md/book_ticker.jsonl` | `<symbol>@bookTicker` payloads (`e`, `u`, `E`, `T`, `s`, `b`, `B`, `a`, `A`) inside the combined-stream wrapper `{"stream":...,"data":...}` | [Individual Symbol Book Ticker Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Individual-Symbol-Book-Ticker-Streams); wrapper from [Connect](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Connect) |
| `md/partial_depth.jsonl` | `<symbol>@depth<levels>@<speed>` payloads (`e: depthUpdate`, `E`, `T`, `s`, `U`, `u`, `pu`, `b`, `a`) inside the combined-stream wrapper | [Partial Book Depth Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Partial-Book-Depth-Streams) |
| `md/replies.jsonl` | the reply to a live `SUBSCRIBE` (`{"result":null,"id":1}`) and an error reply (`{"code":2,"msg":...,"id":2}`) | [Live Subscribing/Unsubscribing to streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Live-Subscribing-Unsubscribing-to-streams) |

One frame per line. The tests in `crates/venues/fbc-venue-binance-usdm/tests/` read them and
state the events each must decode to.
