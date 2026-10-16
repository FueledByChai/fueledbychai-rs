# Paradex testnet acceptance (FBC-8xr)

Run 2026-10-16 11:56:43Z against Paradex testnet, market HYPE-USD-PERP, caps: resting $40 per side (33 lots), inventory $50 (42 lots); each order $11. Written by `paradex_testnet_acceptance` (FBC-xji8). Outcome: **a step failed**.

## Answers

| Question | Answer |
| --- | --- |
| (a) GET /orders lists every order the stream acknowledged and omits every one it ended | yes, eventually at every comparison (5 of 5 agreed at the first read) |
| (b) REST lag behind the stream | at most 1698 ms |
| (c) GET /positions matches the fills (signed size, LONG/SHORT, CLOSED at 0) | yes at every comparison; positions seen: []; no fill event arrived on the stream, so every comparison checked                      GET /positions against the position the resync seeded from REST: this is                      no evidence that GET /positions follows fills |
| (d) GET /orders paginates | no cursor key seen beside results |
| (e) GET /orders-history?client_id= lag | B1 NEW/OPEN: listed after 2102 ms; B1 CLOSED: listed after 1575 ms; B2 buy NEW/OPEN: listed after 1696 ms; S2 sell NEW/OPEN: never listed so |
| cancel-on-disconnect, dropped socket | not run |
| cancel-on-disconnect, graceful close (FBC-gkdz) | not run |
| 0092 (1): the cancel reason of an IOC that expires unfilled | not run |
| 0092 (2): an IOC/MARKET risk refusal other than NOT_ENOUGH_MARGIN | not exercised: --allow-fill was given, but the fill steps sent no IOC (skipped, or stopped before a placement) |
| 0092 (3): a GTC order closed unfilled by self-trade prevention | not exercised: the program never crosses its own orders |
| FBC-g3bw: request_info persistence and requestId echo | 3 of B1's events carried it: [OPEN sizeOpen 0.32 requestStatus Some(4) requestType Some(1) (MODIFY_ORDER yes) requestId Some("1792151807910201703995590000") (requestId is the amend's id 3: no); CLOSED sizeOpen 0.32 requestStatus Some(4) requestType Some(1) (MODIFY_ORDER yes) requestId Some("1792151807910201703995590000") (requestId is the amend's id 3: no); CLOSED sizeOpen 0.32 requestStatus Some(4) requestType Some(1) (MODIFY_ORDER yes) requestId Some("1792151807910201703995590000") (requestId is the amend's id 3: no)]; our order.modify JSON-RPC id was 3 |
| FBC-b0z9 / FBC-olg: order.modify size is the total; modify of a partly filled order | the amend sent the order's total; no partial fill arose (the fill-partial step did not run: it needs --allow-fill), so total-versus-remaining is not distinguished by this run |
| FBC-olg: cancel by client id before the ack | not tested: Paradex declares cancel_before_ack false, so fbc-oms answers AwaitAck and nothing bypasses it |
| FBC-nvxn: does a new connection's arm cover orders already open | not exercised: each connection Safety-cancels what its resync shows resting before it places (decision 0080), so no order rests unprotected long enough to tell |
| FBC-uiy / FBC-4zz: fill side, realizedFunding, feeCurrency, fill ids, funding without fill ids, PositionEvent size and CLOSED | not exercised: no fill event arrived (without --allow-fill every order rests far from the touch) |
| design §13.3: does the per-IP limit count WebSocket order methods | judged only from the deployment box: a remaining count that fell by more than every REST request sent between two samples (this program's and the order sessions' logins, resync reads and token refreshes, both counted below) counts the WebSocket order methods; the first GET /orders of each comparison: place: x-ratelimit-limit=20 x-ratelimit-remaining=19 x-ratelimit-reset=1792151806 x-ratelimit-window=1 (sent since the comparison before: 4 REST requests of this program's, 3 REST requests the order sessions asked for (0 of them never sent), 2 WebSocket order methods, 1 connections opened); amend: x-ratelimit-limit=20 x-ratelimit-remaining=19 x-ratelimit-reset=1792151808 x-ratelimit-window=1 (sent since the comparison before: 3 REST requests of this program's, 0 REST requests the order sessions asked for (0 of them never sent), 1 WebSocket order methods, 0 connections opened); cancel: x-ratelimit-limit=20 x-ratelimit-remaining=19 x-ratelimit-reset=1792151809 x-ratelimit-window=1 (sent since the comparison before: 2 REST requests of this program's, 0 REST requests the order sessions asked for (0 of them never sent), 1 WebSocket order methods, 0 connections opened); batch: x-ratelimit-limit=20 x-ratelimit-remaining=19 x-ratelimit-reset=1792151811 x-ratelimit-window=1 (sent since the comparison before: 4 REST requests of this program's, 0 REST requests the order sessions asked for (0 of them never sent), 1 WebSocket order methods, 0 connections opened); stop: x-ratelimit-limit=20 x-ratelimit-remaining=19 x-ratelimit-reset=1792151830 x-ratelimit-window=1 (sent since the comparison before: 23 REST requests of this program's, 3 REST requests the order sessions asked for (0 of them never sent), 2 WebSocket order methods, 1 connections opened) |

## Steps

| Step | Ran | Pass | Sent |
| --- | --- | --- | --- |
| place | yes | yes | place (request 2, 1 items) |
| amend | yes | yes | amend (request 3, 1 items) |
| cancel | yes | yes | cancel (request 4, 1 items) |
| batch | yes | no | batch (request 5, 2 items); stop cancel (request 6, 1 items) |
| cancel-many | not run | no |  |
| ioc | not run | no |  |
| fill | not run | no |  |
| fill-partial | not run | no |  |
| cod-dropped | not run | no |  |
| cod-graceful | not run | no |  |
| stop | yes | no |  |

### place

- sent place (request 2, 1 items): outcomes item 0 Accepted { ack: Provisional }, item 0 Accepted { ack: Final }
- order B1 (client id 01109200-1a14-8492-9706-8006e2ca7a2c, venue id 1792151805610201703995590000): lattice PendingNew @32.7869 x 0.33 -> Open @32.7869 x 0.33

| REST vs stream after | Stream showed | GET /orders agreed | First read (orders / positions) | Lag | Reads | Position (stream / REST) | Positions agreed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| place | B1: present 32.7869 x 0.33 | yes | yes / yes | 1698 ms | 1 | 0 / not listed | yes |
- GET /orders-history?client_id= for B1 (NEW/OPEN wanted): listed yes, after 2102 ms, 1 reads, status OPEN, cancel_reason 

### amend

- sent amend (request 3, 1 items): outcomes item 0 Accepted { ack: Provisional }
- order B1 (client id 01109200-1a14-8492-9706-8006e2ca7a2c, venue id 1792151805610201703995590000): lattice PendingNew @32.7869 x 0.33 -> Open @32.7869 x 0.33 -> Open @32.7541 x 0.32

| REST vs stream after | Stream showed | GET /orders agreed | First read (orders / positions) | Lag | Reads | Position (stream / REST) | Positions agreed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| amend | B1: present 32.7541 x 0.32 | yes | yes / yes | 1012 ms | 1 | 0 / not listed | yes |

### cancel

- sent cancel (request 4, 1 items): outcomes item 0 Accepted { ack: Provisional }
- order B1 (client id 01109200-1a14-8492-9706-8006e2ca7a2c, venue id 1792151805610201703995590000): lattice PendingNew @32.7869 x 0.33 -> Open @32.7869 x 0.33 -> Open @32.7541 x 0.32 -> Terminal(Canceled(Venue)) @32.7541 x 0.32

| REST vs stream after | Stream showed | GET /orders agreed | First read (orders / positions) | Lag | Reads | Position (stream / REST) | Positions agreed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| cancel | B1: absent | yes | yes / yes | 1047 ms | 1 | 0 / not listed | yes |
- GET /orders-history?client_id= for B1 (CLOSED wanted): listed yes, after 1575 ms, 1 reads, status CLOSED, cancel_reason user_cancel_ws

### batch

- sent batch (request 5, 2 items): outcomes item 0 Accepted { ack: Provisional }, item 1 Accepted { ack: Provisional }, item 0 Accepted { ack: Final }
- sent stop cancel (request 6, 1 items): outcomes item 0 Accepted { ack: Provisional }
- order B2 buy (client id 01109200-1a14-8492-9706-800b1b31ad10, venue id 1792151811390201703995590000): lattice PendingNew @32.7869 x 0.33 -> Open @32.7869 x 0.33
- order S2 sell (client id 01109200-1a14-8492-9706-800d2dd9ebb1, venue id 1792151811390201703995590001): lattice PendingNew @118.5706 x 0.09 -> Open @118.5706 x 0.09 -> Terminal(Canceled(Venue)) @118.5706 x 0.09

| REST vs stream after | Stream showed | GET /orders agreed | First read (orders / positions) | Lag | Reads | Position (stream / REST) | Positions agreed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| batch | B2 buy: present 32.7869 x 0.33; S2 sell: absent; B1 (ended earlier): absent | yes | yes / yes | 1171 ms | 1 | 0 / not listed | yes |
- GET /orders-history?client_id= for B2 buy (NEW/OPEN wanted): listed yes, after 1696 ms, 1 reads, status OPEN, cancel_reason 
- GET /orders-history?client_id= for S2 sell (NEW/OPEN wanted): listed no, after -, 19 reads, status CLOSED, cancel_reason ORDER_SIZE_BELOW_MIN
- note: an order did not reach the state the step expects within the step timeout

### cancel-many


### ioc


### fill


### fill-partial


### cod-dropped


### cod-graceful


### stop


| REST vs stream after | Stream showed | GET /orders agreed | First read (orders / positions) | Lag | Reads | Position (stream / REST) | Positions agreed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| stop | B1 (ended earlier): absent; B2 buy (ended earlier): absent; S2 sell (ended earlier): absent | yes | yes / yes | 0 ms | 1 | 0 / not listed | yes |
- note: the Main phase's link noted 1 problems (the run's notes): the run cannot say its order truth is complete

## Order events as received

| Venue id | Client id | Status | Price | Size | Open | Cancel reason | requestStatus | requestType | requestId | requestMessage |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1792151805610201703995590000 | 01109200-1a14-8492-9706-8006e2ca7a2c | OPEN | 32.7869 | 0.33 | 0.33 |  | 0 | 0 |  |  |
| 1792151805610201703995590000 | 01109200-1a14-8492-9706-8006e2ca7a2c | OPEN | 32.7541 | 0.32 | 0.32 |  | 4 | 1 | 1792151807910201703995590000 |  |
| 1792151805610201703995590000 | 01109200-1a14-8492-9706-8006e2ca7a2c | CLOSED | 32.7541 | 0.32 | 0.32 | user_cancel_ws | 4 | 1 | 1792151807910201703995590000 |  |
| 1792151805610201703995590000 | 01109200-1a14-8492-9706-8006e2ca7a2c | CLOSED | 32.7541 | 0.32 | 0.32 | user_cancel_ws | 4 | 1 | 1792151807910201703995590000 |  |
| 1792151811390201703995590000 | 01109200-1a14-8492-9706-800b1b31ad10 | OPEN | 32.7869 | 0.33 | 0.33 |  | 0 | 0 |  |  |
| 1792151811390201703995590001 | 01109200-1a14-8492-9706-800d2dd9ebb1 | CLOSED | 118.5706 | 0.09 | 0.09 | ORDER_SIZE_BELOW_MIN | 0 | 0 |  |  |
| 1792151811390201703995590000 | 01109200-1a14-8492-9706-800b1b31ad10 | CLOSED | 32.7869 | 0.33 | 0.33 | user_cancel_batch_ws | 0 | 0 |  |  |

## Notes

- the Main phase: the link noted a problem: asynchronous reject of a Place: Other (ORDER_SIZE_BELOW_MIN)
