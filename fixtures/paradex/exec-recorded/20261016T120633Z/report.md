# Paradex testnet acceptance (FBC-8xr)

Run 2026-10-16 12:06:35Z against Paradex testnet, market HYPE-USD-PERP, caps: resting $90 per side (75 lots), inventory $100 (84 lots); each order $20. Written by `paradex_testnet_acceptance` (FBC-xji8). Outcome: **a step failed**.

## Answers

| Question | Answer |
| --- | --- |
| (a) GET /orders lists every order the stream acknowledged and omits every one it ended | yes, eventually at every comparison (12 of 12 agreed at the first read) |
| (b) REST lag behind the stream | at most 3480 ms |
| (c) GET /positions matches the fills (signed size, LONG/SHORT, CLOSED at 0) | yes at every comparison; positions seen: []; no fill event arrived on the stream, so every comparison checked                      GET /positions against the position the resync seeded from REST: this is                      no evidence that GET /positions follows fills |
| (d) GET /orders paginates | no cursor key seen beside results |
| (e) GET /orders-history?client_id= lag | B1 NEW/OPEN: listed after 1934 ms; B1 CLOSED: listed after 1444 ms; B2 buy NEW/OPEN: listed after 1449 ms; S2 sell NEW/OPEN: listed after 1970 ms; IOC CLOSED: listed after 1354 ms; T1 take CLOSED: listed after 2429 ms; B3 CLOSED: listed 1168 ms after the socket ended (cancel-on-disconnect latency and                          the GET /orders wait included: not history lag); B4 CLOSED: listed 709 ms after the socket ended (cancel-on-disconnect latency and                          the GET /orders wait included: not history lag) |
| cancel-on-disconnect, dropped socket | dropped (no close frame): Paradex cancelled the order (GET /orders stopped listing it after 637 ms; history status CLOSED, cancel_reason cancel_on_disconnect); still resting at the next connection's resync: no |
| cancel-on-disconnect, graceful close (FBC-gkdz) | graceful close (the session's close frame; fbc-runtime does not yet report whether the frame was written, FBC-q40h): Paradex cancelled the order (GET /orders stopped listing it after 327 ms; history status CLOSED, cancel_reason cancel_on_disconnect); still resting at the next connection's resync: no |
| 0092 (1): the cancel reason of an IOC that expires unfilled | GET /orders-history: status CLOSED, cancel_reason REMAINING_IOC_CANCEL; the closing OrderEvent's cancelReason "REMAINING_IOC_CANCEL" |
| 0092 (2): an IOC/MARKET risk refusal other than NOT_ENOUGH_MARGIN | not observed: no IOC placement of the fill steps was refused (1 sent, each sized within the caps) |
| 0092 (3): a GTC order closed unfilled by self-trade prevention | not exercised: the program never crosses its own orders |
| FBC-g3bw: request_info persistence and requestId echo | 3 of B1's events carried it: [OPEN sizeOpen 0.59 requestStatus Some(4) requestType Some(1) (MODIFY_ORDER yes) requestId Some("1792152400180201703995590000") (requestId is the amend's id 3: no); CLOSED sizeOpen 0.59 requestStatus Some(4) requestType Some(1) (MODIFY_ORDER yes) requestId Some("1792152400180201703995590000") (requestId is the amend's id 3: no); CLOSED sizeOpen 0.59 requestStatus Some(4) requestType Some(1) (MODIFY_ORDER yes) requestId Some("1792152400180201703995590000") (requestId is the amend's id 3: no)]; our order.modify JSON-RPC id was 3 |
| FBC-b0z9 / FBC-olg: order.modify size is the total; modify of a partly filled order | not exercised: the touch's ask level (352 lots) is not smaller than the order (17 lots), so no order fills only in part |
| FBC-olg: cancel by client id before the ack | not tested: Paradex declares cancel_before_ack false, so fbc-oms answers AwaitAck and nothing bypasses it |
| FBC-nvxn: does a new connection's arm cover orders already open | not exercised: each connection Safety-cancels what its resync shows resting before it places (decision 0080), so no order rests unprotected long enough to tell |
| FBC-uiy / FBC-4zz: fill side, realizedFunding, feeCurrency, fill ids, funding without fill ids, PositionEvent size and CLOSED | not exercised: no fill event arrived (without --allow-fill every order rests far from the touch) |
| design §13.3: does the per-IP limit count WebSocket order methods | judged only from the deployment box: a remaining count that fell by more than every REST request sent between two samples (this program's and the order sessions' logins, resync reads and token refreshes, both counted below) counts the WebSocket order methods; the first GET /orders of each comparison: place: x-ratelimit-limit=20 x-ratelimit-remaining=19 x-ratelimit-reset=1792152399 x-ratelimit-window=1 (sent since the comparison before: 4 REST requests of this program's, 3 REST requests the order sessions asked for (0 of them never sent), 2 WebSocket order methods, 1 connections opened); amend: x-ratelimit-limit=20 x-ratelimit-remaining=19 x-ratelimit-reset=1792152400 x-ratelimit-window=1 (sent since the comparison before: 3 REST requests of this program's, 0 REST requests the order sessions asked for (0 of them never sent), 1 WebSocket order methods, 0 connections opened); cancel: x-ratelimit-limit=20 x-ratelimit-remaining=19 x-ratelimit-reset=1792152401 x-ratelimit-window=1 (sent since the comparison before: 2 REST requests of this program's, 0 REST requests the order sessions asked for (0 of them never sent), 1 WebSocket order methods, 0 connections opened); batch: x-ratelimit-limit=20 x-ratelimit-remaining=19 x-ratelimit-reset=1792152404 x-ratelimit-window=1 (sent since the comparison before: 4 REST requests of this program's, 0 REST requests the order sessions asked for (0 of them never sent), 1 WebSocket order methods, 0 connections opened); cancel-many: x-ratelimit-limit=20 x-ratelimit-remaining=19 x-ratelimit-reset=1792152406 x-ratelimit-window=1 (sent since the comparison before: 4 REST requests of this program's, 0 REST requests the order sessions asked for (0 of them never sent), 1 WebSocket order methods, 0 connections opened); ioc: x-ratelimit-limit=20 x-ratelimit-remaining=19 x-ratelimit-reset=1792152407 x-ratelimit-window=1 (sent since the comparison before: 3 REST requests of this program's, 0 REST requests the order sessions asked for (0 of them never sent), 1 WebSocket order methods, 0 connections opened); fill: before the IOC: x-ratelimit-limit=20 x-ratelimit-remaining=19 x-ratelimit-reset=1792152409 x-ratelimit-window=1 (sent since the comparison before: 4 REST requests of this program's, 0 REST requests the order sessions asked for (0 of them never sent), 0 WebSocket order methods, 0 connections opened); fill: after the IOC: x-ratelimit-limit=20 x-ratelimit-remaining=19 x-ratelimit-reset=1792152411 x-ratelimit-window=1 (sent since the comparison before: 2 REST requests of this program's, 0 REST requests the order sessions asked for (0 of them never sent), 1 WebSocket order methods, 0 connections opened); fill: after the flatten: x-ratelimit-limit=20 x-ratelimit-remaining=19 x-ratelimit-reset=1792152413 x-ratelimit-window=1 (sent since the comparison before: 3 REST requests of this program's, 0 REST requests the order sessions asked for (0 of them never sent), 0 WebSocket order methods, 0 connections opened); cod-dropped: B3 resting: x-ratelimit-limit=20 x-ratelimit-remaining=19 x-ratelimit-reset=1792152415 x-ratelimit-window=1 (sent since the comparison before: 4 REST requests of this program's, 0 REST requests the order sessions asked for (0 of them never sent), 1 WebSocket order methods, 0 connections opened); cod-dropped: after the disconnect (position only): x-ratelimit-limit=20 x-ratelimit-remaining=19 x-ratelimit-reset=1792152418 x-ratelimit-window=1 (sent since the comparison before: 4 REST requests of this program's, 0 REST requests the order sessions asked for (0 of them never sent), 0 WebSocket order methods, 0 connections opened); cod-graceful: B4 resting: x-ratelimit-limit=20 x-ratelimit-remaining=19 x-ratelimit-reset=1792152421 x-ratelimit-window=1 (sent since the comparison before: 3 REST requests of this program's, 3 REST requests the order sessions asked for (0 of them never sent), 2 WebSocket order methods, 1 connections opened); cod-graceful: after the disconnect (position only): x-ratelimit-limit=20 x-ratelimit-remaining=19 x-ratelimit-reset=1792152423 x-ratelimit-window=1 (sent since the comparison before: 4 REST requests of this program's, 0 REST requests the order sessions asked for (0 of them never sent), 0 WebSocket order methods, 0 connections opened); stop: x-ratelimit-limit=20 x-ratelimit-remaining=19 x-ratelimit-reset=1792152426 x-ratelimit-window=1 (sent since the comparison before: 2 REST requests of this program's, 3 REST requests the order sessions asked for (0 of them never sent), 1 WebSocket order methods, 1 connections opened) |

## Steps

| Step | Ran | Pass | Sent |
| --- | --- | --- | --- |
| place | yes | yes | place (request 2, 1 items) |
| amend | yes | yes | amend (request 3, 1 items) |
| cancel | yes | yes | cancel (request 4, 1 items) |
| batch | yes | yes | batch (request 5, 2 items) |
| cancel-many | yes | yes | cancel-many (request 6, 2 items) |
| ioc | yes | yes | place (request 7, 1 items) |
| fill | yes | no | place (request 8, 1 items) |
| fill-partial | skipped: the touch's ask level (352 lots) is not smaller than the order (17 lots), so no order fills only in part | no |  |
| cod-dropped | yes | yes | place (request 9, 1 items) |
| cod-graceful | yes | yes | place (request 11, 1 items) |
| stop | yes | yes |  |

### place

- sent place (request 2, 1 items): outcomes item 0 Accepted { ack: Provisional }, item 0 Accepted { ack: Final }
- order B1 (client id 01109200-1a14-849b-9994-800464c01569, venue id 1792152398060201703995590000): lattice PendingNew @32.7869 x 0.6 -> Open @32.7869 x 0.6

| REST vs stream after | Stream showed | GET /orders agreed | First read (orders / positions) | Lag | Reads | Position (stream / REST) | Positions agreed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| place | B1: present 32.7869 x 0.6 | yes | yes / yes | 1576 ms | 1 | 0 / not listed | yes |
- GET /orders-history?client_id= for B1 (NEW/OPEN wanted): listed yes, after 1934 ms, 1 reads, status OPEN, cancel_reason 

### amend

- sent amend (request 3, 1 items): outcomes item 0 Accepted { ack: Provisional }
- order B1 (client id 01109200-1a14-849b-9994-800464c01569, venue id 1792152398060201703995590000): lattice PendingNew @32.7869 x 0.6 -> Open @32.7869 x 0.6 -> Open @32.7541 x 0.59

| REST vs stream after | Stream showed | GET /orders agreed | First read (orders / positions) | Lag | Reads | Position (stream / REST) | Positions agreed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| amend | B1: present 32.7541 x 0.59 | yes | yes / yes | 1051 ms | 1 | 0 / not listed | yes |

### cancel

- sent cancel (request 4, 1 items): outcomes item 0 Accepted { ack: Provisional }
- order B1 (client id 01109200-1a14-849b-9994-800464c01569, venue id 1792152398060201703995590000): lattice PendingNew @32.7869 x 0.6 -> Open @32.7869 x 0.6 -> Open @32.7541 x 0.59 -> Terminal(Canceled(Venue)) @32.7541 x 0.59

| REST vs stream after | Stream showed | GET /orders agreed | First read (orders / positions) | Lag | Reads | Position (stream / REST) | Positions agreed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| cancel | B1: absent | yes | yes / yes | 920 ms | 1 | 0 / not listed | yes |
- GET /orders-history?client_id= for B1 (CLOSED wanted): listed yes, after 1444 ms, 1 reads, status CLOSED, cancel_reason user_cancel_ws

### batch

- sent batch (request 5, 2 items): outcomes item 0 Accepted { ack: Provisional }, item 1 Accepted { ack: Provisional }, item 0 Accepted { ack: Final }, item 1 Accepted { ack: Final }
- order B2 buy (client id 01109200-1a14-849b-9994-80088a326dec, venue id 1792152403500201703995590000): lattice PendingNew @32.7869 x 0.6 -> Open @32.7869 x 0.6
- order S2 sell (client id 01109200-1a14-849b-9994-800f923bbc24, venue id 1792152403500201703995590001): lattice PendingNew @118.5706 x 0.16 -> Open @118.5706 x 0.16

| REST vs stream after | Stream showed | GET /orders agreed | First read (orders / positions) | Lag | Reads | Position (stream / REST) | Positions agreed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| batch | B2 buy: present 32.7869 x 0.6; S2 sell: present 118.5706 x 0.16; B1 (ended earlier): absent | yes | yes / yes | 1047 ms | 1 | 0 / not listed | yes |
- GET /orders-history?client_id= for B2 buy (NEW/OPEN wanted): listed yes, after 1449 ms, 1 reads, status OPEN, cancel_reason 
- GET /orders-history?client_id= for S2 sell (NEW/OPEN wanted): listed yes, after 1970 ms, 1 reads, status OPEN, cancel_reason 

### cancel-many

- sent cancel-many (request 6, 2 items): outcomes item 0 Accepted { ack: Provisional }, item 1 Accepted { ack: Provisional }
- order B2 buy (client id 01109200-1a14-849b-9994-80088a326dec, venue id 1792152403500201703995590000): lattice PendingNew @32.7869 x 0.6 -> Open @32.7869 x 0.6 -> Terminal(Canceled(Venue)) @32.7869 x 0.6
- order S2 sell (client id 01109200-1a14-849b-9994-800f923bbc24, venue id 1792152403500201703995590001): lattice PendingNew @118.5706 x 0.16 -> Open @118.5706 x 0.16 -> Terminal(Canceled(Venue)) @118.5706 x 0.16

| REST vs stream after | Stream showed | GET /orders agreed | First read (orders / positions) | Lag | Reads | Position (stream / REST) | Positions agreed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| cancel-many | B2 buy: absent; S2 sell: absent; B1 (ended earlier): absent | yes | yes / yes | 1027 ms | 1 | 0 / not listed | yes |

### ioc

- sent place (request 7, 1 items): outcomes item 0 Accepted { ack: Provisional }, item 0 Accepted { ack: Final }
- order IOC (client id 01109200-1a14-849b-9994-8010b4596a91, venue id 1792152407280201703995590000): lattice PendingNew @32.7869 x 0.6 -> Open @32.7869 x 0.6 -> Terminal(Canceled(Venue)) @32.7869 x 0.6

| REST vs stream after | Stream showed | GET /orders agreed | First read (orders / positions) | Lag | Reads | Position (stream / REST) | Positions agreed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ioc | IOC: absent; B1 (ended earlier): absent; B2 buy (ended earlier): absent; S2 sell (ended earlier): absent | yes | yes / yes | 837 ms | 1 | 0 / not listed | yes |
- GET /orders-history?client_id= for IOC (CLOSED wanted): listed yes, after 1354 ms, 1 reads, status CLOSED, cancel_reason REMAINING_IOC_CANCEL

### fill

- sent place (request 8, 1 items): outcomes item 0 Accepted { ack: Provisional }, item 0 Accepted { ack: Final }
- order T1 take (client id 01109200-1a14-849b-9994-8017bbba5815, venue id 1792152410640201703995590000): lattice PendingNew @115.117 x 0.17 -> Open @115.117 x 0.17 -> Terminal(Canceled(Venue)) @115.117 x 0.17

| REST vs stream after | Stream showed | GET /orders agreed | First read (orders / positions) | Lag | Reads | Position (stream / REST) | Positions agreed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| fill: before the IOC | B1 (ended earlier): absent; B2 buy (ended earlier): absent; S2 sell (ended earlier): absent; IOC (ended earlier): absent | yes | yes / yes | 0 ms | 1 | 0 / not listed | yes |
| fill: after the IOC | T1 take: absent; B1 (ended earlier): absent; B2 buy (ended earlier): absent; S2 sell (ended earlier): absent; IOC (ended earlier): absent | yes | yes / yes | 1904 ms | 1 | 0 / not listed | yes |
| fill: after the flatten | T1 take: absent; B1 (ended earlier): absent; B2 buy (ended earlier): absent; S2 sell (ended earlier): absent; IOC (ended earlier): absent | yes | yes / yes | 3480 ms | 1 | 0 / not listed | yes |
- GET /orders-history?client_id= for T1 take (CLOSED wanted): listed yes, after 2429 ms, 1 reads, status CLOSED, cancel_reason EXCEEDS_PRICE_BAND: order price 115.117 above upper band 97.97398419599999
- note: T1 took nothing (the touch moved before it landed): GET /positions following fills was not exercised

### fill-partial


### cod-dropped

- sent place (request 9, 1 items): outcomes item 0 Accepted { ack: Provisional }, item 0 Accepted { ack: Final }
- order B3 (client id 01109200-1a14-849b-9994-801a946dd3f7, venue id 1792152415370201703995590000): lattice PendingNew @32.7869 x 0.6 -> Open @32.7869 x 0.6

| REST vs stream after | Stream showed | GET /orders agreed | First read (orders / positions) | Lag | Reads | Position (stream / REST) | Positions agreed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| cod-dropped: B3 resting | B3: present 32.7869 x 0.6; B1 (ended earlier): absent; B2 buy (ended earlier): absent; S2 sell (ended earlier): absent; IOC (ended earlier): absent; T1 take (ended earlier): absent | yes | yes / yes | 1049 ms | 1 | 0 / not listed | yes |
| cod-dropped: after the disconnect (position only) |  | yes | yes / yes | - | 1 | 0 / not listed | yes |
- disconnect: dropped (no close frame); cancelled by Paradex: yes (after 637 ms, waited up to 30 s); still listed by the last GET /orders read: no; still resting at the next resync: no; GET /orders reads from the socket's end: +637 ms: gone

### cod-graceful

- sent place (request 11, 1 items): outcomes item 0 Accepted { ack: Provisional }, item 0 Accepted { ack: Final }
- order B4 (client id 01109200-1a14-849b-9994-801ec4eec46b, venue id 1792152421130201703950840000): lattice PendingNew @32.7869 x 0.6 -> Open @32.7869 x 0.6

| REST vs stream after | Stream showed | GET /orders agreed | First read (orders / positions) | Lag | Reads | Position (stream / REST) | Positions agreed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| cod-graceful: B4 resting | B4: present 32.7869 x 0.6; B1 (ended earlier): absent; B2 buy (ended earlier): absent; S2 sell (ended earlier): absent; IOC (ended earlier): absent; T1 take (ended earlier): absent | yes | yes / yes | 868 ms | 1 | 0 / not listed | yes |
| cod-graceful: after the disconnect (position only) |  | yes | yes / yes | - | 1 | 0 / not listed | yes |
- disconnect: graceful close (the session's close frame; fbc-runtime does not yet report whether the frame was written, FBC-q40h); cancelled by Paradex: yes (after 327 ms, waited up to 30 s); still listed by the last GET /orders read: no; still resting at the next resync: no; GET /orders reads from the socket's end: +327 ms: gone

### stop


| REST vs stream after | Stream showed | GET /orders agreed | First read (orders / positions) | Lag | Reads | Position (stream / REST) | Positions agreed |
| --- | --- | --- | --- | --- | --- | --- | --- |
| stop | B1 (ended earlier): absent; B2 buy (ended earlier): absent; S2 sell (ended earlier): absent; IOC (ended earlier): absent; T1 take (ended earlier): absent | yes | yes / yes | 0 ms | 1 | 0 / not listed | yes |

## Order events as received

| Venue id | Client id | Status | Price | Size | Open | Cancel reason | requestStatus | requestType | requestId | requestMessage |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1792152398060201703995590000 | 01109200-1a14-849b-9994-800464c01569 | OPEN | 32.7869 | 0.6 | 0.6 |  | 0 | 0 |  |  |
| 1792152398060201703995590000 | 01109200-1a14-849b-9994-800464c01569 | OPEN | 32.7541 | 0.59 | 0.59 |  | 4 | 1 | 1792152400180201703995590000 |  |
| 1792152398060201703995590000 | 01109200-1a14-849b-9994-800464c01569 | CLOSED | 32.7541 | 0.59 | 0.59 | user_cancel_ws | 4 | 1 | 1792152400180201703995590000 |  |
| 1792152398060201703995590000 | 01109200-1a14-849b-9994-800464c01569 | CLOSED | 32.7541 | 0.59 | 0.59 | user_cancel_ws | 4 | 1 | 1792152400180201703995590000 |  |
| 1792152403500201703995590000 | 01109200-1a14-849b-9994-80088a326dec | OPEN | 32.7869 | 0.6 | 0.6 |  | 0 | 0 |  |  |
| 1792152403500201703995590001 | 01109200-1a14-849b-9994-800f923bbc24 | OPEN | 118.5706 | 0.16 | 0.16 |  | 0 | 0 |  |  |
| 1792152403500201703995590000 | 01109200-1a14-849b-9994-80088a326dec | CLOSED | 32.7869 | 0.6 | 0.6 | user_cancel_batch_ws | 0 | 0 |  |  |
| 1792152403500201703995590001 | 01109200-1a14-849b-9994-800f923bbc24 | CLOSED | 118.5706 | 0.16 | 0.16 | user_cancel_batch_ws | 0 | 0 |  |  |
| 1792152407280201703995590000 | 01109200-1a14-849b-9994-8010b4596a91 | CLOSED | 32.7869 | 0.6 | 0.6 | REMAINING_IOC_CANCEL | 0 | 0 |  |  |
| 1792152410640201703995590000 | 01109200-1a14-849b-9994-8017bbba5815 | CLOSED | 115.117 | 0.17 | 0.17 | EXCEEDS_PRICE_BAND: order price 115.117 above upper band 97.97398419599999 | 0 | 0 |  |  |
| 1792152415370201703995590000 | 01109200-1a14-849b-9994-801a946dd3f7 | OPEN | 32.7869 | 0.6 | 0.6 |  | 0 | 0 |  |  |
| 1792152421130201703950840000 | 01109200-1a14-849b-9994-801ec4eec46b | OPEN | 32.7869 | 0.6 | 0.6 |  | 0 | 0 |  |  |
