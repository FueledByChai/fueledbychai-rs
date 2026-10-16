# Paradex testnet acceptance (FBC-8xr)

Run 2026-10-15 22:23:49Z against Paradex testnet, market ETH-USD-PERP, caps: resting $11 per side (56 lots), inventory $50 (250 lots); each order $11. Written by `paradex_testnet_acceptance` (FBC-xji8). Outcome: **every step passed**.

## Answers

| Question | Answer |
| --- | --- |
| (a) GET /orders lists every order the stream acknowledged and omits every one it ended | yes, eventually at every comparison (7 of 7 agreed at the first read) |
| (b) REST lag behind the stream | at most 1705 ms |
| (c) GET /positions matches the fills (signed size, LONG/SHORT, CLOSED at 0) | yes at every comparison; positions seen: []; no fill event arrived on the stream, so every comparison checked                      GET /positions against the position the resync seeded from REST: this is                      no evidence that GET /positions follows fills |
| (d) GET /orders paginates | no cursor key seen beside results |
| (e) GET /orders-history?client_id= lag | B1 NEW/OPEN: listed after 2097 ms; B1 CLOSED: listed after 1376 ms; IOC CLOSED: listed after 1478 ms; B3 CLOSED: listed 1094 ms after the socket ended (cancel-on-disconnect latency and                          the GET /orders wait included: not history lag); B4 CLOSED: listed 916 ms after the socket ended (cancel-on-disconnect latency and                          the GET /orders wait included: not history lag) |
| cancel-on-disconnect, dropped socket | dropped (no close frame): Paradex cancelled the order (GET /orders stopped listing it after 580 ms; history status CLOSED, cancel_reason cancel_on_disconnect); still resting at the next connection's resync: no |
| cancel-on-disconnect, graceful close (FBC-gkdz) | graceful close (the session's close frame; fbc-runtime does not yet report whether the frame was written, FBC-q40h): Paradex cancelled the order (GET /orders stopped listing it after 390 ms; history status CLOSED, cancel_reason cancel_on_disconnect); still resting at the next connection's resync: no |
| 0092 (1): the cancel reason of an IOC that expires unfilled | GET /orders-history: status CLOSED, cancel_reason REMAINING_IOC_CANCEL; the closing OrderEvent's cancelReason "REMAINING_IOC_CANCEL" |
| 0092 (2): an IOC/MARKET risk refusal other than NOT_ENOUGH_MARGIN | not exercised: the program never sends an order that could take |
| 0092 (3): a GTC order closed unfilled by self-trade prevention | not exercised: the program never crosses its own orders |
| FBC-g3bw: request_info persistence and requestId echo | 3 of B1's events carried it: [OPEN sizeOpen 0.0055 requestStatus Some(4) requestType Some(1) (MODIFY_ORDER yes) requestId Some("1792103034210201703980270000") (requestId is the amend's id 3: no); CLOSED sizeOpen 0.0055 requestStatus Some(4) requestType Some(1) (MODIFY_ORDER yes) requestId Some("1792103034210201703980270000") (requestId is the amend's id 3: no); CLOSED sizeOpen 0.0055 requestStatus Some(4) requestType Some(1) (MODIFY_ORDER yes) requestId Some("1792103034210201703980270000") (requestId is the amend's id 3: no)]; our order.modify JSON-RPC id was 3 |
| FBC-b0z9 / FBC-olg: order.modify size is the total; modify of a partly filled order | the amend sent the order's total; no partial fill arose (orders rest far from the touch), so total-versus-remaining is not distinguished by this run |
| FBC-olg: cancel by client id before the ack | not tested: Paradex declares cancel_before_ack false, so fbc-oms answers AwaitAck and nothing bypasses it |
| FBC-nvxn: does a new connection's arm cover orders already open | not exercised: each connection Safety-cancels what its resync shows resting before it places (decision 0080), so no order rests unprotected long enough to tell |
| FBC-uiy / FBC-4zz: fill side, realizedFunding, feeCurrency, fill ids, funding without fill ids, PositionEvent size and CLOSED | not exercised unless a fill arose (see the order traces): every order rests far from the touch |
| design §13.3: does the per-IP limit count WebSocket order methods | judged only from the deployment box: a remaining count that fell by more than every REST request sent between two samples (this program's and the order sessions' logins, resync reads and token refreshes, both counted below) counts the WebSocket order methods; the first GET /orders of each comparison: place: x-ratelimit-limit=20 x-ratelimit-remaining=19 x-ratelimit-reset=1792103033 x-ratelimit-window=1 (sent since the comparison before: 4 REST requests of this program's, 3 REST requests the order sessions asked for (0 of them never sent), 2 WebSocket order methods, 1 connections opened); amend: x-ratelimit-limit=20 x-ratelimit-remaining=19 x-ratelimit-reset=1792103034 x-ratelimit-window=1 (sent since the comparison before: 3 REST requests of this program's, 0 REST requests the order sessions asked for (0 of them never sent), 1 WebSocket order methods, 0 connections opened); cancel: x-ratelimit-limit=20 x-ratelimit-remaining=19 x-ratelimit-reset=1792103036 x-ratelimit-window=1 (sent since the comparison before: 2 REST requests of this program's, 0 REST requests the order sessions asked for (0 of them never sent), 1 WebSocket order methods, 0 connections opened); ioc: x-ratelimit-limit=20 x-ratelimit-remaining=19 x-ratelimit-reset=1792103038 x-ratelimit-window=1 (sent since the comparison before: 5 REST requests of this program's, 0 REST requests the order sessions asked for (0 of them never sent), 1 WebSocket order methods, 0 connections opened); cod-dropped: B3 resting: x-ratelimit-limit=20 x-ratelimit-remaining=19 x-ratelimit-reset=1792103041 x-ratelimit-window=1 (sent since the comparison before: 4 REST requests of this program's, 0 REST requests the order sessions asked for (0 of them never sent), 1 WebSocket order methods, 0 connections opened); cod-dropped: after the disconnect (position only): x-ratelimit-limit=20 x-ratelimit-remaining=19 x-ratelimit-reset=1792103043 x-ratelimit-window=1 (sent since the comparison before: 4 REST requests of this program's, 0 REST requests the order sessions asked for (0 of them never sent), 0 WebSocket order methods, 0 connections opened); cod-graceful: B4 resting: x-ratelimit-limit=20 x-ratelimit-remaining=19 x-ratelimit-reset=1792103046 x-ratelimit-window=1 (sent since the comparison before: 3 REST requests of this program's, 3 REST requests the order sessions asked for (0 of them never sent), 2 WebSocket order methods, 1 connections opened); cod-graceful: after the disconnect (position only): x-ratelimit-limit=20 x-ratelimit-remaining=19 x-ratelimit-reset=1792103048 x-ratelimit-window=1 (sent since the comparison before: 4 REST requests of this program's, 0 REST requests the order sessions asked for (0 of them never sent), 0 WebSocket order methods, 0 connections opened); stop: x-ratelimit-limit=20 x-ratelimit-remaining=19 x-ratelimit-reset=1792103051 x-ratelimit-window=1 (sent since the comparison before: 2 REST requests of this program's, 3 REST requests the order sessions asked for (0 of them never sent), 1 WebSocket order methods, 1 connections opened) |

## Steps

| Step | Ran | Pass | Sent |
| --- | --- | --- | --- |
| place | yes | yes | place (request 2, 1 items) |
| amend | yes | yes | amend (request 3, 1 items) |
| cancel | yes | yes | cancel (request 4, 1 items) |
| batch | skipped: the order book has no ask resting, so no order rests behind that side | no |  |
| cancel-many | skipped: the order book has no ask resting, so no order rests behind that side | no |  |
| ioc | yes | yes | place (request 5, 1 items) |
| cod-dropped | yes | yes | place (request 6, 1 items) |
| cod-graceful | yes | yes | place (request 8, 1 items) |
| stop | yes | yes |  |

### place

- sent place (request 2, 1 items): outcomes item 0 Accepted { ack: Provisional }, item 0 Accepted { ack: Final }
- order B1 (client id 01109200-1a14-81aa-8870-4006d7638fa7, venue id 1792103031920201703980270000): lattice PendingNew @1940 x 0.0056 -> Open @1940 x 0.0056

| REST vs stream after | Stream showed | GET /orders agreed | First read (orders / positions) | Lag | Reads | Position (stream / REST) | Positions agreed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| place | B1: present 1940 x 0.0056 | yes | yes / yes | 1705 ms | 1 | 0 / not listed | yes |
- GET /orders-history?client_id= for B1 (NEW/OPEN wanted): listed yes, after 2097 ms, 1 reads, status OPEN, cancel_reason 

### amend

- sent amend (request 3, 1 items): outcomes item 0 Accepted { ack: Provisional }
- order B1 (client id 01109200-1a14-81aa-8870-4006d7638fa7, venue id 1792103031920201703980270000): lattice PendingNew @1940 x 0.0056 -> Open @1940 x 0.0056 -> Open @1938.06 x 0.0055

| REST vs stream after | Stream showed | GET /orders agreed | First read (orders / positions) | Lag | Reads | Position (stream / REST) | Positions agreed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| amend | B1: present 1938.06 x 0.0055 | yes | yes / yes | 1052 ms | 1 | 0 / not listed | yes |

### cancel

- sent cancel (request 4, 1 items): outcomes item 0 Accepted { ack: Provisional }
- order B1 (client id 01109200-1a14-81aa-8870-4006d7638fa7, venue id 1792103031920201703980270000): lattice PendingNew @1940 x 0.0056 -> Open @1940 x 0.0056 -> Open @1938.06 x 0.0055 -> Terminal(Canceled(Venue)) @1938.06 x 0.0055

| REST vs stream after | Stream showed | GET /orders agreed | First read (orders / positions) | Lag | Reads | Position (stream / REST) | Positions agreed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| cancel | B1: absent | yes | yes / yes | 1030 ms | 1 | 0 / not listed | yes |
- GET /orders-history?client_id= for B1 (CLOSED wanted): listed yes, after 1376 ms, 1 reads, status CLOSED, cancel_reason user_cancel_ws

### batch


### cancel-many


### ioc

- sent place (request 5, 1 items): outcomes item 0 Accepted { ack: Provisional }, item 0 Accepted { ack: Final }
- order IOC (client id 01109200-1a14-81aa-8870-400bc0f2b636, venue id 1792103038150201703980270000): lattice PendingNew @1940 x 0.0056 -> Open @1940 x 0.0056 -> Terminal(Canceled(Venue)) @1940 x 0.0056

| REST vs stream after | Stream showed | GET /orders agreed | First read (orders / positions) | Lag | Reads | Position (stream / REST) | Positions agreed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ioc | IOC: absent; B1 (ended earlier): absent | yes | yes / yes | 954 ms | 1 | 0 / not listed | yes |
- GET /orders-history?client_id= for IOC (CLOSED wanted): listed yes, after 1478 ms, 1 reads, status CLOSED, cancel_reason REMAINING_IOC_CANCEL

### cod-dropped

- sent place (request 6, 1 items): outcomes item 0 Accepted { ack: Provisional }, item 0 Accepted { ack: Final }
- order B3 (client id 01109200-1a14-81aa-8870-400cf0679254, venue id 1792103040520201703980270000): lattice PendingNew @1940 x 0.0056 -> Open @1940 x 0.0056

| REST vs stream after | Stream showed | GET /orders agreed | First read (orders / positions) | Lag | Reads | Position (stream / REST) | Positions agreed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| cod-dropped: B3 resting | B3: present 1940 x 0.0056; B1 (ended earlier): absent; IOC (ended earlier): absent | yes | yes / yes | 1511 ms | 1 | 0 / not listed | yes |
| cod-dropped: after the disconnect (position only) |  | yes | yes / yes | - | 1 | 0 / not listed | yes |
- disconnect: dropped (no close frame); cancelled by Paradex: yes (after 580 ms, waited up to 30 s); still listed by the last GET /orders read: no; still resting at the next resync: no; GET /orders reads from the socket's end: +580 ms: gone

### cod-graceful

- sent place (request 8, 1 items): outcomes item 0 Accepted { ack: Provisional }, item 0 Accepted { ack: Final }
- order B4 (client id 01109200-1a14-81aa-8870-4011d64579d3, venue id 1792103046380201703950840000): lattice PendingNew @1940 x 0.0056 -> Open @1940 x 0.0056

| REST vs stream after | Stream showed | GET /orders agreed | First read (orders / positions) | Lag | Reads | Position (stream / REST) | Positions agreed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| cod-graceful: B4 resting | B4: present 1940 x 0.0056; B1 (ended earlier): absent; IOC (ended earlier): absent | yes | yes / yes | 847 ms | 1 | 0 / not listed | yes |
| cod-graceful: after the disconnect (position only) |  | yes | yes / yes | - | 1 | 0 / not listed | yes |
- disconnect: graceful close (the session's close frame; fbc-runtime does not yet report whether the frame was written, FBC-q40h); cancelled by Paradex: yes (after 390 ms, waited up to 30 s); still listed by the last GET /orders read: no; still resting at the next resync: no; GET /orders reads from the socket's end: +390 ms: gone

### stop


| REST vs stream after | Stream showed | GET /orders agreed | First read (orders / positions) | Lag | Reads | Position (stream / REST) | Positions agreed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| stop | B1 (ended earlier): absent; IOC (ended earlier): absent | yes | yes / yes | 1031 ms | 1 | 0 / not listed | yes |

## Order events as received

| Venue id | Client id | Status | Price | Size | Open | Cancel reason | requestStatus | requestType | requestId | requestMessage |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1792103031920201703980270000 | 01109200-1a14-81aa-8870-4006d7638fa7 | OPEN | 1940 | 0.0056 | 0.0056 |  | 0 | 0 |  |  |
| 1792103031920201703980270000 | 01109200-1a14-81aa-8870-4006d7638fa7 | OPEN | 1938.06 | 0.0055 | 0.0055 |  | 4 | 1 | 1792103034210201703980270000 |  |
| 1792103031920201703980270000 | 01109200-1a14-81aa-8870-4006d7638fa7 | CLOSED | 1938.06 | 0.0055 | 0.0055 | user_cancel_ws | 4 | 1 | 1792103034210201703980270000 |  |
| 1792103031920201703980270000 | 01109200-1a14-81aa-8870-4006d7638fa7 | CLOSED | 1938.06 | 0.0055 | 0.0055 | user_cancel_ws | 4 | 1 | 1792103034210201703980270000 |  |
| 1792103038150201703980270000 | 01109200-1a14-81aa-8870-400bc0f2b636 | CLOSED | 1940 | 0.0056 | 0.0056 | REMAINING_IOC_CANCEL | 0 | 0 |  |  |
| 1792103040520201703980270000 | 01109200-1a14-81aa-8870-400cf0679254 | OPEN | 1940 | 0.0056 | 0.0056 |  | 0 | 0 |  |  |
| 1792103046380201703950840000 | 01109200-1a14-81aa-8870-4011d64579d3 | OPEN | 1940 | 0.0056 | 0.0056 |  | 0 | 0 |  |  |
