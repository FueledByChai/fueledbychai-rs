# Paradex SBE order-event and fill-event frames

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
transfers), and `feeCurrency` follows `market`. The order socket negotiates 1:2 (0054). The
offsets match FueledByChaiTrading's `ParadexSbeTranscoder`.

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
| `order-modify-success-longer-block.sbe.txt` | The SUCCESS frame with a 136-byte block: 8 bytes past the known fields, skipped |
| `order-short-block.sbe.txt` | A version-2 header declaring a 128-byte block, the frame ending 60 bytes into it |
| `fill-maker-v1.sbe.txt` | FILL at version 1: our buy made 0.05 at 62000.0 for a 0.0031 rebate, realizedPnl null, no fee currency |
| `fill-taker-close-v2.sbe.txt` | FILL at version 2: our sell took 0.05 at 62000.0, fee 0.62 USDC, realizedPnl 3.5, realizedFunding -0.12 |
| `fill-rpi-v2.sbe.txt` | RPI fill at version 2: our buy made 0.05, fee 0.015 DIME |
| `fill-longer-block-v2.sbe.txt` | The taker fill with a 124-byte block: 8 bytes past the known fields, skipped |
| `fill-liquidation-v2.sbe.txt` | LIQUIDATION at version 2: 0.15 sold at 61000.0, realizedPnl -150; it carries one of our client ids, which a venue-initiated fill does not match |
| `fill-transfer-null-seq-v2.sbe.txt` | UNWIND_TRANSFER at version 2: a null `seq`, no order id or client id, liquidity NON_REPRESENTABLE |
| `fill-short-block.sbe.txt` | A version-2 header declaring a 116-byte block, the frame ending 43 bytes into it |

The cancel reasons are the ones a source names: `USER_CANCELED` (the example of the
`orders.{market_symbol}` channel in Paradex's AsyncAPI specification, and the Java library's
`CancelReason`), `POST_ONLY_WOULD_CROSS` (the Java library's `CancelReason`, met in its
production runs) and `NOT_ENOUGH_MARGIN` (the example of `cancel_reason` on the "Get order"
REST page). No Paradex page lists every cancel reason.

The fill and trade ids are made up. The tests
(`crates/venues/fbc-venue-paradex/tests/exec_order_events.rs` and `exec_fill_events.rs`) also
change single bytes of these frames to reach the decoders' refusals.
