# QFEX production stage one: how a trade connection stays alive (the liveness hold)

FBC-l931, decision 0132 items 1, 3 and 7, and decision 0143. The liveness hold: FBC-39ks and decision 0146. Environment: QFEX production, under the probe mode `production_probe`. Started 2026-11-15 02:05:33Z against QFEX production. Result: INCOMPLETE.

**The probe stopped before its sessions ended:** after the hold: primary (held) could not be read: no read socket within 10000 ms; subaccount could not be read: no read socket within 10000 ms

Records the journal's sink dropped: 0 Normal, 0 Safety. Sessions whose capture is partial: none.

## The accounts read before each session and around the hold

Accounts named by the run's configuration, to be read before every session: 2 (primary, subaccount; ids not shown), not a list QFEX gave: the probe sends no `GET /user/subaccounts` (decision 0144). The table below lists the reads made. Before the run the owner must confirm in QFEX's app that the user has no subaccount the configuration does not name; the probe records no such confirmation and cannot check it, so the checks below, and what they show of the user's other accounts, cover the named accounts only. A session starts only when every account it reads shows no open order on `get_user_orders` (a socket that never arms) and flat REST positions (decision 0132 item 7), and, reading that strictly, no REST `open_orders` either. The liveness hold reads them as a session does before it opens, and again after it ends, whatever ended it (decision 0146 item 3).

| read | account | position rows | flat | REST open_orders | page orders | page TWAPs | page full |
| --- | --- | --- | --- | --- | --- | --- | --- |
| before the hold | primary (held) | 11 | yes | 0 | 0 | 0 | no |
| before the hold | subaccount | 0 | yes | 0 | 0 | 0 | no |

## The liveness hold (FBC-39ks, decision 0146)

One trade connection on the primary account, on QFEX production, to be held for 3600 s from its opening. It never armed, placed, cancelled or read. It wrote, in order: authentication, subscribe order_responses, subscribe fills, subscribe positions; then nothing but the pongs and the close reply the WebSocket layer writes to each server ping and server close and, at its end, its close (a session sends the same authentication and the same three subscriptions: order_responses, fills, positions). It sent no ping of its own. Each frame it wrote was charged to the run's shared limiter first, and each pong and close reply the WebSocket layer wrote for it recorded there, as a session's are (decision 0030). No session and no other connection of the probe was open while it was. Its frames are not journaled: each ping, pong and close is kept below by its time since the connection opened, a payload or a close reason by its length only and a close code in full (decision 0041), and its data frames are counted by kind, never kept. Result: complete.

Every account the run's configuration names was read before the hold opened and again after it ended, whatever ended it (the accounts table's rows `before the hold` and `after the hold`). **Conditions of the run, which the probe cannot check** (decision 0146 item 3): the owner confirmed before the run that the user has no subaccount the configuration leaves out (decision 0144 item 2); and the owner asserts that nothing traded on any account of the user from the first read before the hold to the last read after it.

- Connection opened: yes
- Authenticated: 351 ms since the connection opened
- Subscriptions confirmed: 3 of 3, the last at 480 ms
- Any ping before authentication: no
- Pings after authentication: 153, the first 10062 ms after it
- Intervals between the pings after authentication: 9961 ms, 9984 ms, 9997 ms, 9999 ms, 10062 ms, 9963 ms, 9977 ms, 10002 ms, 10001 ms, 10000 ms, 10000 ms, 10002 ms, 10077 ms, 9961 ms, 9961 ms, 10003 ms, 10002 ms, 10072 ms, 9958 ms, 9971 ms, 9999 ms, 10001 ms, 10002 ms, 9999 ms, 10001 ms, 10087 ms, 9961 ms, 9959 ms, 9996 ms, 9998 ms, 10081 ms, 9961 ms, 9961 ms, 9999 ms, 10000 ms, 10001 ms, 10003 ms, 10045 ms, 9953 ms, 10058 ms, 9962 ms, 9981 ms, 10002 ms, 9998 ms, 10057 ms, 9956 ms, 9990 ms, 10000 ms, 10001 ms, 10003 ms, 9998 ms, 10000 ms, 10070 ms, 9961 ms, 9970 ms, 10001 ms, 10002 ms, 10062 ms, 9964 ms, 9976 ms, 10000 ms, 10004 ms, 9997 ms, 10002 ms, 10002 ms, 10076 ms, 9964 ms, 9960 ms, 10000 ms, 10003 ms, 10071 ms, 9961 ms, 9970 ms, 9999 ms, 10002 ms, 9998 ms, 10001 ms, 10002 ms, 10090 ms, 9958 ms, 9963 ms, 9990 ms, 10003 ms, 10080 ms, 9960 ms, 9962 ms, 10000 ms, 10016 ms, 9986 ms, 10000 ms, 10005 ms, 9997 ms, 10054 ms, 9964 ms, 9985 ms, 10000 ms, 10001 ms, 10049 ms, 9959 ms, 9997 ms, 9996 ms, 10001 ms, 10002 ms, 10001 ms, 9998 ms, 10068 ms, 9964 ms, 9971 ms, 10000 ms, 10002 ms, 10062 ms, 9957 ms, 9983 ms, 10005 ms, 9997 ms, 9999 ms, 10000 ms, 10000 ms, 10077 ms, 9961 ms, 9964 ms, 10155 ms, 9845 ms, 10001 ms, 10000 ms, 10002 ms, 10003 ms, 9998 ms, 10000 ms, 10003 ms, 10005 ms, 10082 ms, 9959 ms, 9980 ms, 9976 ms, 10003 ms, 9999 ms, 10001 ms, 10002 ms, 9999 ms, 10000 ms, 10000 ms, 10000 ms, 10000 ms, 10008 ms, 10049 ms, 9963 ms, 9982 ms, 10000 ms, 10001 ms, 10002 ms, 10000 ms (shortest 9845 ms, longest 10155 ms)
- Pings in all: 153; pongs received: 0
- Server closes: code no code (a close with no body), a reason of 0 bytes, at 1535872 ms since the connection opened
- Ended by: the server's close, at 1535872 ms since the connection opened
- Its own close: it sent none
- Replies the WebSocket layer wrote for it, each recorded on the shared limiter: 154
- Data frames received, counted and never kept: 290; by kind, read from what a journal would keep of each: authenticated 1; position_response 286; subscribed 3
- Order or fill frames received: none
- Accounts read after it: STOPPED: primary (held) could not be read: no read socket within 10000 ms; subaccount could not be read: no read socket within 10000 ms
- Error: none

A lifetime longer than the hold was not measured: a hold that ended at its duration shows only that QFEX did not close the connection within it. A code 1002 with an 18-byte reason may stand for a close code the WebSocket layer does not allow (1004, 1005, 1006, 1015, one from 1016 to 2999, or one outside 1000 to 4999), which it reports in its place. The result speaks for QFEX production only (decision 0146 item 8).

| n | ms since the connection opened | frame | payload or reason bytes | close code |
| --- | --- | --- | --- | --- |
| 1 | 10413 | ping | 8 |  |
| 2 | 20374 | ping | 8 |  |
| 3 | 30358 | ping | 8 |  |
| 4 | 40356 | ping | 8 |  |
| 5 | 50356 | ping | 8 |  |
| 6 | 60418 | ping | 8 |  |
| 7 | 70381 | ping | 8 |  |
| 8 | 80359 | ping | 8 |  |
| 9 | 90361 | ping | 8 |  |
| 10 | 100362 | ping | 8 |  |
| 11 | 110363 | ping | 8 |  |
| 12 | 120363 | ping | 8 |  |
| 13 | 130365 | ping | 8 |  |
| 14 | 140442 | ping | 8 |  |
| 15 | 150404 | ping | 8 |  |
| 16 | 160366 | ping | 8 |  |
| 17 | 170369 | ping | 8 |  |
| 18 | 180371 | ping | 8 |  |
| 19 | 190444 | ping | 8 |  |
| 20 | 200402 | ping | 8 |  |
| 21 | 210374 | ping | 8 |  |
| 22 | 220374 | ping | 8 |  |
| 23 | 230376 | ping | 8 |  |
| 24 | 240379 | ping | 8 |  |
| 25 | 250378 | ping | 8 |  |
| 26 | 260379 | ping | 8 |  |
| 27 | 270467 | ping | 8 |  |
| 28 | 280429 | ping | 8 |  |
| 29 | 290389 | ping | 8 |  |
| 30 | 300386 | ping | 8 |  |
| 31 | 310384 | ping | 8 |  |
| 32 | 320465 | ping | 8 |  |
| 33 | 330427 | ping | 8 |  |
| 34 | 340389 | ping | 8 |  |
| 35 | 350389 | ping | 8 |  |
| 36 | 360389 | ping | 8 |  |
| 37 | 370391 | ping | 8 |  |
| 38 | 380394 | ping | 8 |  |
| 39 | 390439 | ping | 8 |  |
| 40 | 400393 | ping | 8 |  |
| 41 | 410451 | ping | 8 |  |
| 42 | 420414 | ping | 8 |  |
| 43 | 430396 | ping | 8 |  |
| 44 | 440398 | ping | 8 |  |
| 45 | 450397 | ping | 8 |  |
| 46 | 460454 | ping | 8 |  |
| 47 | 470410 | ping | 8 |  |
| 48 | 480400 | ping | 8 |  |
| 49 | 490401 | ping | 8 |  |
| 50 | 500403 | ping | 8 |  |
| 51 | 510406 | ping | 8 |  |
| 52 | 520405 | ping | 8 |  |
| 53 | 530405 | ping | 8 |  |
| 54 | 540476 | ping | 8 |  |
| 55 | 550438 | ping | 8 |  |
| 56 | 560408 | ping | 8 |  |
| 57 | 570410 | ping | 8 |  |
| 58 | 580412 | ping | 8 |  |
| 59 | 590474 | ping | 8 |  |
| 60 | 600439 | ping | 8 |  |
| 61 | 610415 | ping | 8 |  |
| 62 | 620415 | ping | 8 |  |
| 63 | 630419 | ping | 8 |  |
| 64 | 640417 | ping | 8 |  |
| 65 | 650419 | ping | 8 |  |
| 66 | 660422 | ping | 8 |  |
| 67 | 670498 | ping | 8 |  |
| 68 | 680462 | ping | 8 |  |
| 69 | 690423 | ping | 8 |  |
| 70 | 700423 | ping | 8 |  |
| 71 | 710427 | ping | 8 |  |
| 72 | 720498 | ping | 8 |  |
| 73 | 730460 | ping | 8 |  |
| 74 | 740430 | ping | 8 |  |
| 75 | 750429 | ping | 8 |  |
| 76 | 760432 | ping | 8 |  |
| 77 | 770430 | ping | 8 |  |
| 78 | 780431 | ping | 8 |  |
| 79 | 790434 | ping | 8 |  |
| 80 | 800524 | ping | 8 |  |
| 81 | 810483 | ping | 8 |  |
| 82 | 820446 | ping | 8 |  |
| 83 | 830437 | ping | 8 |  |
| 84 | 840441 | ping | 8 |  |
| 85 | 850521 | ping | 8 |  |
| 86 | 860482 | ping | 8 |  |
| 87 | 870445 | ping | 8 |  |
| 88 | 880446 | ping | 8 |  |
| 89 | 890462 | ping | 8 |  |
| 90 | 900448 | ping | 8 |  |
| 91 | 910449 | ping | 8 |  |
| 92 | 920454 | ping | 8 |  |
| 93 | 930451 | ping | 8 |  |
| 94 | 940505 | ping | 8 |  |
| 95 | 950469 | ping | 8 |  |
| 96 | 960454 | ping | 8 |  |
| 97 | 970455 | ping | 8 |  |
| 98 | 980457 | ping | 8 |  |
| 99 | 990507 | ping | 8 |  |
| 100 | 1000466 | ping | 8 |  |
| 101 | 1010464 | ping | 8 |  |
| 102 | 1020460 | ping | 8 |  |
| 103 | 1030462 | ping | 8 |  |
| 104 | 1040464 | ping | 8 |  |
| 105 | 1050465 | ping | 8 |  |
| 106 | 1060463 | ping | 8 |  |
| 107 | 1070531 | ping | 8 |  |
| 108 | 1080496 | ping | 8 |  |
| 109 | 1090467 | ping | 8 |  |
| 110 | 1100468 | ping | 8 |  |
| 111 | 1110470 | ping | 8 |  |
| 112 | 1120532 | ping | 8 |  |
| 113 | 1130490 | ping | 8 |  |
| 114 | 1140473 | ping | 8 |  |
| 115 | 1150478 | ping | 8 |  |
| 116 | 1160476 | ping | 8 |  |
| 117 | 1170476 | ping | 8 |  |
| 118 | 1180476 | ping | 8 |  |
| 119 | 1190477 | ping | 8 |  |
| 120 | 1200555 | ping | 8 |  |
| 121 | 1210517 | ping | 8 |  |
| 122 | 1220481 | ping | 8 |  |
| 123 | 1230636 | ping | 8 |  |
| 124 | 1240482 | ping | 8 |  |
| 125 | 1250484 | ping | 8 |  |
| 126 | 1260484 | ping | 8 |  |
| 127 | 1270486 | ping | 8 |  |
| 128 | 1280490 | ping | 8 |  |
| 129 | 1290488 | ping | 8 |  |
| 130 | 1300488 | ping | 8 |  |
| 131 | 1310491 | ping | 8 |  |
| 132 | 1320497 | ping | 8 |  |
| 133 | 1330579 | ping | 8 |  |
| 134 | 1340538 | ping | 8 |  |
| 135 | 1350519 | ping | 8 |  |
| 136 | 1360495 | ping | 8 |  |
| 137 | 1370498 | ping | 8 |  |
| 138 | 1380497 | ping | 8 |  |
| 139 | 1390499 | ping | 8 |  |
| 140 | 1400501 | ping | 8 |  |
| 141 | 1410501 | ping | 8 |  |
| 142 | 1420501 | ping | 8 |  |
| 143 | 1430501 | ping | 8 |  |
| 144 | 1440502 | ping | 8 |  |
| 145 | 1450503 | ping | 8 |  |
| 146 | 1460511 | ping | 8 |  |
| 147 | 1470561 | ping | 8 |  |
| 148 | 1480525 | ping | 8 |  |
| 149 | 1490507 | ping | 8 |  |
| 150 | 1500508 | ping | 8 |  |
| 151 | 1510510 | ping | 8 |  |
| 152 | 1520512 | ping | 8 |  |
| 153 | 1530512 | ping | 8 |  |
| 154 | 1535872 | close, the server's | 0 | no code (a close with no body) |

## The sessions

None: the run asked for no session (no --arm).

## Provider rate refusals

Decision 0135 item 2: how many provider rate refusals each session and each read socket met. The probe provokes none (0135 item 1). Each is counted three ways: how much the run's provider-user limiter's `counts().rejected` rose while it ran (a session's holds, which the runtime makes for its codec's `RateLimited`, decision 0126, and a read socket's, which the probe makes for each `RateLimited` err the adapter reads in one of its frames); its frames in an `err` whose `error_code` is `RateLimited`; and its `order_response`s whose `status` is `RATE_LIMITED`. A session's frames are those its journal kept; a read socket's, which are not journaled, the two it read (the authentication reply and the page), each read as a journal would keep it.

A frame the journal keeps whole (decision 0134 item 2: an echo of authentication, a JSON escape, a repeated member) shows no code and holds no budget (0134 item 6), so none of those counts can see a refusal in it: each such frame, and each that is not JSON, is counted unread. A read socket's unread frames are read once more in memory, as they arrived (nothing of them is written), for a refusal kept whole; a session's reach only its codec and the journal, so a refusal in one it keeps whole is not seen here. The REST reads' answers are not read for refusals: a 429 or 418 on one stops the probe, its status named where it stopped.

What one met is at least the larger of the limiter's count and the refusals its frames show: the two count the same refusals, but each can miss one the other sees (a hold whose frame a partial capture lost, a frame the codec held no budget for), so the larger is a lower bound, not a total.

The liveness hold's connection (decision 0146) is counted as a read socket is, every data frame it received read the same way, and is listed under "the hold" with the read sockets before it.

No provider rate refusal was counted.

Frames unread, kept whole or not JSON: 0. A refusal in a session's unread frame would not show in its counts.

Met by each session, at least, with the read sockets before it: the hold 0.

| session | on | limiter rejected | RateLimited errs | RATE_LIMITED order responses | frames unread | refusals in frames kept whole | met, at least | refusal frames |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| the hold | read socket: primary (held) | 0 | 0 | 0 | 0 | 0 | 0 | not journaled |
| the hold | read socket: subaccount | 0 | 0 | 0 | 0 | 0 | 0 | not journaled |
| the hold | the hold's connection: primary | 0 | 0 | 0 | 0 | 0 | 0 | not journaled |

## Files

Fixtures written: README.md. Withheld: none. Not written or not checked: none.

Members whose string value a fixture keeps as received: none. The privacy check knows credential and address shapes only: read every fixture for an identifying string (an email, a user name, an address in another form) before committing it.

Not measured by stage one (FBC-9sk2's stages): whether arming cancels open orders (a session starts only on accounts whose reads above show none), whether a graceful close or a drop cancels, and everything decision 0132 asks of a place. Not checked by the probe: whether the user has a subaccount the configuration does not name (the owner must confirm none before the run, which the probe does not record: decision 0144).
