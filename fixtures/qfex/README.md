These public market-data frames are hand-written copies of the examples in QFEX's
[BBO](https://docs.qfex.com/websocket/channels/mds/bbo),
[Pulsed Order Book](https://docs.qfex.com/websocket/channels/mds/orderbook), and
[Public Trades](https://docs.qfex.com/websocket/channels/mds/trades) documentation as read on
2026-10-20. They contain no private account data or credentials. Test instrument grids are
synthetic so the published example numbers can be represented exactly.

`array.json` and `contents.json` wrap those same examples in the two batch forms
handled by [QFEX's CLI MDS client](https://github.com/QFEX-org/cli/blob/main/internal/daemon/mds.go).
They are composition fixtures, not captured live frames.
