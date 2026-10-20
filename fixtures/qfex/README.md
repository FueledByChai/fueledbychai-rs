These public market-data frames are hand-written copies of the examples in QFEX's
[BBO](https://docs.qfex.com/websocket/channels/mds/bbo),
[Pulsed Order Book](https://docs.qfex.com/websocket/channels/mds/orderbook), and
[Public Trades](https://docs.qfex.com/websocket/channels/mds/trades) documentation as read on
2026-10-20. They contain no private account data or credentials. Test instrument grids are
synthetic so the published example numbers can be represented exactly.

`array.json` and `contents.json` wrap those same examples in the two batch forms
handled by [QFEX's CLI MDS client](https://github.com/QFEX-org/cli/blob/main/internal/daemon/mds.go).
They are composition fixtures, not captured live frames.

`live/nvda-2026-10-20.raw.ndjson` and its field-filtered projection
`live/nvda-2026-10-20.ndjson` are one 45-second, read-only public MDS capture of
`NVDA-USD` on 2026-10-20, approximately 20:54:38–20:55:22 UTC. The capture
subscribed to `bbo`, `level2`, and `trade` on `wss://mds.qfex.com/`. It contains
89 BBO messages, 89 level2 messages and three public trades, with no feed errors.
The server-time median gap was 500 ms on both pulsed feeds (observed range
453–547 ms). Sequence numbers were consecutive from 1 through 181 across the
three channels in this connection; the trades consumed 29, 126 and 155. The level2
book changed depth and prices between snapshots, giving a live replacement
case. This observation does not turn BBO's undocumented interval or the
sequence scope into a protocol guarantee.

Each `raw.ndjson` line holds the exact received JSON payload string under `raw`,
after the capture script accepted only expected public market-data fields. It
omits subscription acknowledgments. The paired projection carries type, symbol,
sequence, venue time, public bid/ask levels and `sig_figs`, and for trades the
public UUID, price, size, side and execution type. Both files carry the same
`recv_ms`, a local monotonic offset from the start of capture rounded to
milliseconds. The fixture test checks that parsing each saved raw payload gives
exactly its paired projection. The script connects with no credential, sends
no orders, and writes no private frames. To repeat a capture with Node 24 or later:

```sh
node fixtures/qfex/capture_public.mjs NVDA-USD 30000 /tmp/qfex-public.ndjson
```
