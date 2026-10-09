# Paradex SBE order, fill, position and account frames

Hand-built, every one: each `.sbe.txt` file is one binary frame written as whitespace-separated
hex bytes, one field per line, `#` to the end of a line a comment. The `account` field of each
is 32 synthetic bytes (`SYNTHETIC` says so), so this directory holds no recorded frame and no
account value (0009).

Layouts follow Paradex's published schema, `paradex_1_0.xml` in
[tradeparadex/paradex-py](https://github.com/tradeparadex/paradex-py/blob/b8248fb747e278d2167ac2f056b339a287d5ef30/paradex_py/api/sbe/paradex_1_0.xml)
at commit `b8248fb747e278d2167ac2f056b339a287d5ef30`, the commit 0022 cites. `OrderEvent`
(template 20) has a 126-byte root block up to version 1; at version 2 `requestStatus` and
`requestType` are appended in place (a 128-byte block) and `requestId` and `requestMessage`
after `cancelReason`. The schema says version 2 describes both layouts, so a decoder reads the
block length from the header and takes absent appended var data as missing. `FillEvent`
(template 21) has a 107-byte root block up to version 1; at version 2 `flags` and
`orderbookSeqNo` are appended (a 116-byte block), `seq` is optional (null for position
transfers), and `feeCurrency` follows `market`. `PositionEvent` (template 22) has a 154-byte
root block at every version, then `market` and `lastFillId`. `AccountEvent` (template 23) has a
113-byte root block; at version 2 `lastSeenNotification` may be appended in place (a 121-byte
block), then `settlementAsset`. The order socket negotiates 1:2 (0054). The offsets match
FueledByChaiTrading's `ParadexSbeTranscoder` (which has no `PositionEvent`) and paradex-py's
generated decoder at that commit, which reads every position and account frame here as its
title says.

Every frame is for BTC-USD-PERP. The client id `01000700-199b-81ab-8200-00054d0aa3f5` (and
`...-00099b9bf518`) is the first (and second) id the tests mint in their namespace;
`3f2504e0-4f89-41d3-9a0c-0305e82c3301` is a random UUID, not ours.

| File | What it is |
| --- | --- |
| `order-new-v1.sbe.txt` | NEW at version 1: a post-only buy of 0.15 at 62000.0, nothing filled |
| `order-open-partial-v1.sbe.txt` | OPEN at version 1: a reduce-only GTC sell, 0.05 of 0.15 filled |
| `order-rpi-v1.sbe.txt` | OPEN at version 1 on the RPI instruction |
| `order-foreign-cid-v1.sbe.txt` | OPEN with a random UUID client id |
| `order-no-cid-v1.sbe.txt` | OPEN with an empty client id |
| `order-market-ioc-v1.sbe.txt` | CLOSED and filled: a MARKET IOC order with a null price |
| `order-v1-longer-block.sbe.txt` | OPEN at version 1 with a 128-byte block, its last two bytes a modify SUCCESS for MODIFY_ORDER (`04 01`) where version 2 puts request_info: a version-1 frame has no request_info, so they are skipped |
| `order-closed-filled-v2.sbe.txt` | CLOSED at version 2, nothing open and no cancel reason: filled |
| `order-closed-canceled-v2.sbe.txt` | CLOSED by `USER_CANCELED` after 0.05 filled |
| `order-closed-post-only-v2.sbe.txt` | CLOSED by `POST_ONLY_WOULD_CROSS` |
| `order-closed-margin-v2.sbe.txt` | CLOSED by `NOT_ENOUGH_MARGIN` |
| `order-v2-without-request-info.sbe.txt` | OPEN at version 2 in the layout without request_info: a 126-byte block, no `requestId` or `requestMessage` |
| `order-modify-pending-v2.sbe.txt` | OPEN while a modify is PENDING |
| `order-modify-success-v2.sbe.txt` | OPEN, modify SUCCESS for MODIFY_ORDER, at 61999.5 for 0.2 |
| `order-modify-rejected-v2.sbe.txt` | OPEN, modify REJECTED for MODIFY_ORDER with a request message; the order as it was |
| `order-modify-rejected-fill-v2.sbe.txt` | The same order, OPEN after 0.05 filled, still carrying `order-modify-rejected-v2`'s REJECTED for MODIFY_ORDER and its `requestId` (`req-7002`): a later update repeating an earlier modify's request_info |
| `order-modify-success-fill-v2.sbe.txt` | The same order, OPEN after 0.05 of the amended 0.2 filled, still carrying `order-modify-success-v2`'s SUCCESS for MODIFY_ORDER and its `requestId` (`req-7001`) |
| `order-modify-success-longer-block.sbe.txt` | The SUCCESS frame with a 136-byte block: 8 bytes past the known fields, skipped |
| `order-short-block.sbe.txt` | A version-2 header declaring a 128-byte block, the frame ending 60 bytes into it |
| `fill-maker-v1.sbe.txt` | FILL at version 1: our buy made 0.05 at 62000.0 for a 0.0031 rebate, realizedPnl null, no fee currency |
| `fill-taker-close-v2.sbe.txt` | FILL at version 2: our sell took 0.05 at 62000.0, fee 0.62 USDC, realizedPnl 3.5, realizedFunding -0.12 |
| `fill-rpi-v2.sbe.txt` | RPI fill at version 2: our buy made 0.05, fee 0.015 DIME |
| `fill-longer-block-v2.sbe.txt` | The taker fill with a 124-byte block: 8 bytes past the known fields, skipped |
| `fill-liquidation-v2.sbe.txt` | LIQUIDATION at version 2: 0.15 sold at 61000.0, realizedPnl -150; it carries one of our client ids, which a venue-initiated fill does not match |
| `fill-transfer-null-seq-v2.sbe.txt` | UNWIND_TRANSFER at version 2: a null `seq`, no order id or client id, liquidity NON_REPRESENTABLE |
| `fill-short-block.sbe.txt` | A version-2 header declaring a 116-byte block, the frame ending 43 bytes into it |
| `position-long-v2.sbe.txt` | PositionEvent at version 2: long (BUY) 0.15 at an average entry of 61234.56789012, off the 0.1 tick |
| `position-short-v1.sbe.txt` | PositionEvent at version 1: short (SELL) 0.05 at an average entry of 62000.05 |
| `position-closed-v2.sbe.txt` | PositionEvent CLOSED: size 0, its last average entry 61900.0 still stated |
| `position-longer-block-v2.sbe.txt` | The long position with a 162-byte block: 8 bytes past `status`, skipped |
| `position-short-block.sbe.txt` | A version-2 header declaring a 154-byte block, the frame ending 40 bytes into it |
| `account-v1.sbe.txt` | AccountEvent at version 1 (113-byte block): account value 612.34, free collateral 550.25, settlement asset USDC |
| `account-v2.sbe.txt` | AccountEvent at version 2 with `lastSeenNotification` (121-byte block): account value 598.76543211, free collateral 0 |
| `account-longer-block-v2.sbe.txt` | The version-2 account frame with a 129-byte block: 8 bytes past `lastSeenNotification`, skipped |
| `account-short-block.sbe.txt` | A version-2 header declaring a 121-byte block, the frame ending 30 bytes into it |

The cancel reasons are the ones a source names: `USER_CANCELED` (the example of the
`orders.{market_symbol}` channel in Paradex's AsyncAPI specification, and the Java library's
`CancelReason`), `POST_ONLY_WOULD_CROSS` (the Java library's `CancelReason`, met in its
production runs) and `NOT_ENOUGH_MARGIN` (the example of `cancel_reason` on the "Get order"
REST page). No Paradex page lists every cancel reason.

The fill and trade ids, and every position and account amount, are made up. The tests
(`crates/venues/fbc-venue-paradex/tests/exec_order_events.rs`, `exec_fill_events.rs` and
`exec_position_account_events.rs`) also change single bytes of these frames to reach the
decoders' refusals.

## REST responses

Hand-built in the shapes docs.paradex.trade documents: "Get open orders" (`GET /v1/orders`:
`results`, a list of `OrderResp`), "List open positions" (`GET /v1/positions`: `results`, a
list of `PositionResp`, whose `size` carries the position's sign) and "Get orders history"
(`GET /v1/orders-history`, query `client_id`: `next`, `prev` and `results`). Every field the
pages list is present; the `account` is the made-up bytes a0..bf in hex. Prices are on the test
specs' 0.1 tick and sizes on their 0.001 step.

| File | What it is |
| --- | --- |
| `rest-orders-open.json` | Three open orders: ours (the first client id), a reduce-only GTC sell of 0.15 at 62000 with 0.1 open; a POST_ONLY ETH buy of 1.25 at 3000.5, NEW, under a random UUID; and an RPI BTC buy of 0.05 at 61990.5 with an empty client id |
| `rest-positions.json` | Long 0.15 BTC at 61234.56789012, short 0.05 ETH (size `-0.05`) at 3000.05, and a CLOSED SOL position of size 0 (a market the specs do not hold) |
| `rest-positions-bad-second.json` | The BTC position, then an ETH one of `-0.0505`, off the size step: the answer fails on its last entry |
| `rest-orders-history-filled.json` | Our order in the history: a POST_ONLY buy of 0.15 at 62000, CLOSED with nothing open and no cancel reason (filled) |
| `rest-orders-history-canceled.json` | The same order CLOSED by `USER_CANCELED` with 0.1 open |
| `rest-orders-history-open.json` | The same order OPEN with 0.1 open |
| `rest-orders-history-empty.json` | A history listing no order |

`crates/venues/fbc-venue-paradex/tests/exec_rest.rs` decodes them and changes single fields to
reach the decoders' refusals.

## JSON-RPC replies

Hand-built, every one, in the shapes docs.paradex.trade documents for the order socket: the
envelope of the WebSocket "Error Handling" page (`jsonrpc`, `result` or `error`, `usIn`,
`usOut`, `usDiff`, `id`), and each method's result from its page under
`ws/web-socket-channels/`. Each order object carries every field the page lists; its `account`
is the made-up bytes a0..bf in hex, and its client id the first tests' id above. The JSON-RPC
ids are the request ids the tests send under. Order ids are `1759500000000000001` to `...03`.

| File | What it is |
| --- | --- |
| `reply-create.json` | `order.create` (id 11): the created order, NEW, a POST_ONLY buy of 0.15 at 62000 under our first client id |
| `reply-create-bare-id.json` | `order.create` (id 12) answered with a bare `{"id":..}`, which the Java library's `extractOrderId` tolerates |
| `reply-create-batch-mixed.json` | `order.create_batch` (id 13): the first item created as above, the second answered with an `error` message (no code: Unknown, 0069) |
| `reply-create-batch-short.json` | `order.create_batch` (id 14) with one result for a two-item batch |
| `reply-modify.json` | `order.modify` (id 15): the order OPEN at 61999.5 for 0.2 |
| `reply-cancel.json` | `order.cancel` (id 16): `QUEUED_FOR_CANCELLATION` for the first order id |
| `reply-cancel-batch.json` | `order.cancel_batch` (id 17): the three order ids QUEUED_FOR_CANCELLATION, ALREADY_CLOSED and NOT_FOUND (no `market`, as the page says) |
| `reply-cancel-all.json` | `order.cancel_all` (id 18): `{"status":"ok"}` |
| `reply-cancel-on-disconnect.json` | `order.cancel_on_disconnect` (id 19): `{"enabled":true}` |
| `error-method.json` | Error 100, method error, with `data` (id 20) |
| `error-internal.json` | Error -32603, internal error (id 21) |
| `error-undocumented.json` | Error 4290, a code no page documents (id 22) |
| `error-no-id.json` | Error -32700, parse error, with no `id` |
| `error-null-id.json` | Error 40111, invalid bearer token, with a null `id` |

`crates/venues/fbc-venue-paradex/tests/exec_replies.rs` decodes them and changes single fields
to reach the decoder's refusals.
