# Paradex order-entry frames recorded on testnet (FBC-8xr)

Three runs of the owner-run acceptance program `paradex_testnet_acceptance`
(`crates/fbc-examples`, FBC-xji8) against Paradex testnet, by the owner from his own machine.
Each directory holds the program's `report.md` and its scrubbed `fixtures/` (with its own
README naming the method: only what the venue sent, account bytes replaced by `a0..bf`,
`account` and `master_account` values `SCRUBBED`, no login answer, no frame we wrote, so no
signature or token). The journal and `evidence.json` are not committed. Every file was copied
unchanged from the run and passed `scripts/privacy-check.sh` and a grep for account
addresses, keys, signatures, JWTs and tokens before it was committed; none was removed.
`crates/venues/fbc-venue-paradex/tests/exec_recorded.rs` decodes every file of every run
(order events, JSON-RPC replies, REST answers).

| Run | Date (UTC) | Market | Caps (resting per side / inventory) | Outcome |
| --- | --- | --- | --- | --- |
| [20261015T222347Z](20261015T222347Z/report.md) | 2026-10-15 22:23 | ETH-USD-PERP | $11 / $50 | every step passed; batch and cancel-many skipped (no ask resting) |
| [20261016T115634Z](20261016T115634Z/report.md) | 2026-10-16 11:56 | HYPE-USD-PERP | $40 / $50 | place, amend, cancel passed; batch accepted, item 2 refused asynchronously; later steps not run |
| [20261016T120633Z](20261016T120633Z/report.md) | 2026-10-16 12:06 | HYPE-USD-PERP | $90 / $100 | every step passed but `fill` (the IOC was refused by the price band, nothing filled) |

## What the runs showed

| Question | 20261015T222347Z (ETH) | 20261016T115634Z (HYPE) | 20261016T120633Z (HYPE) |
| --- | --- | --- | --- |
| Place, amend, cancel on the order lattice | PendingNew -> Open -> Open (amended) -> Canceled; no lattice violation | the same; no violation | the same; no violation |
| Batch create (`order.create_batch`) | skipped | first live bare-array batch ACCEPTED, both items provisional; item 1 then CLOSED `ORDER_SIZE_BELOW_MIN` (an asynchronous reject) | ACCEPTED, both items Final, both rest |
| Cancel-many (`order.cancel_batch`) | skipped | not run (Stop cancelled B2 as a one-item batch: `user_cancel_batch_ws`) | both items queued, both CLOSED `user_cancel_batch_ws` |
| Batch reply shape | - | `{"results":[{order}..]}` in request order | the same; cancel batch `{"results":[{id, market, status}..]}` |
| (a) GET /orders lists every order the stream acknowledged, omits every order it ended | yes, 7 of 7 comparisons at the first read | yes, 5 of 5 | yes, 12 of 12 |
| (b) REST lag behind the stream | at most 1705 ms | at most 1698 ms | at most 3480 ms |
| (c) GET /positions matches the fills | no fill arose: only the flat position was compared (`[]` throughout); **no evidence that positions follow fills** | the same | the same (the fill IOC took nothing) |
| (d) GET /orders paginates | no cursor key beside `results` | none | none |
| (e) GET /orders-history?client_id= lag | NEW/OPEN 2097 ms; CLOSED 1376 ms (B1), 1478 ms (IOC) | NEW/OPEN 2102 ms (B1), 1696 ms (B2); CLOSED 1575 ms; S2 never listed NEW/OPEN (it was CLOSED by then) | NEW/OPEN 1449-1970 ms; CLOSED 1354-2429 ms |
| Cancel-on-disconnect, dropped socket | Paradex cancelled the order 580 ms after the socket ended (`cancel_on_disconnect`); not resting at the next resync | not run | cancelled after 637 ms; not resting at the next resync |
| Cancel-on-disconnect, graceful close | cancelled after 390 ms (`cancel_on_disconnect`) | not run | cancelled after 327 ms |
| IOC expiring unfilled: cancel reason | `REMAINING_IOC_CANCEL` (stream and history) | not run | `REMAINING_IOC_CANCEL` |
| Our cancel's reason | `user_cancel_ws` | `user_cancel_ws`, `user_cancel_batch_ws` | `user_cancel_ws`, `user_cancel_batch_ws` |
| request_info persistence (FBC-g3bw) | the amend's request_info (SUCCESS, MODIFY_ORDER) persisted on every later event of the order, the CLOSED ones included | the same | the same |
| requestId echo | a venue id (`1792103034210201703980270000`), not our JSON-RPC id (3) | a venue id, not ours | a venue id, not ours |
| Minimum notional | - | judged at the mark price, not the order price: S2 sell 0.09 at 118.5706 ($10.67 at its own price, far above the mark) was accepted, then CLOSED `ORDER_SIZE_BELOW_MIN` | S2 sell 0.16 at 118.5706, sized larger, rested |
| Price band | - | - | IOC T1 at 115.117 CLOSED `EXCEEDS_PRICE_BAND: order price 115.117 above upper band 97.97398419599999` (reason is a code, then a message) |
| Per-IP limit (design §13.3) | `x-ratelimit-limit=20`, window 1 s, remaining 19 at every sample: one sample per comparison cannot say whether WebSocket order methods count | the same | the same |

Decision 0093 corrects what these contradicted: our WebSocket cancels' reasons, the named
cancel-on-disconnect and IOC-expiry reasons, and the price-band and minimum-size refusals of
an IOC (0092 took them for expiry). No declared capability value (cancel-on-disconnect per
connection and re-armed, two-phase acknowledgement, batch maximum 10, modify size the total)
was contradicted.

## Not exercised

- **Positions following fills.** No order filled in any run (testnet books rest outside the
  price band, so the fill IOC was refused). Owner decision 2026-10-16: verify it in the first
  mainnet sessions that get fills, with the kill switch ready if REST and the stream disagree
  (recorded with FBC-2f7w).
- **Modify of a partly filled order**, and so whether `order.modify`'s size is the total or the
  remaining part when they differ: the amend sent the total each time; no partial fill arose.
- **Fill fields** (FBC-uiy, FBC-4zz): fill side on a maker fill, realizedFunding sign,
  feeCurrency, fill ids across fill types, funding without fill ids, PositionEvent size and
  CLOSED: no fill arose.
- **The per-IP limit check from the deployment box (singapore8)**: these runs ran from the
  owner's machine, and each sample read 19 of 20 remaining.
- **Self-trade prevention**: the program never crosses its own orders.
- **Risk refusals** of an IOC or MARKET order other than the price band (NOT_ENOUGH_MARGIN,
  reduce-only): none arose.
- **Cancel by client id before the ack**: Paradex declares `cancel_before_ack` false, so
  fbc-oms waits for the ack.
- **Whether a new connection's arm covers orders already open** (FBC-nvxn): each connection
  cancels what its resync shows resting before it places (decision 0080).
- **Batch sizes beyond two items**, and **fills replayed after a reconnect**.
