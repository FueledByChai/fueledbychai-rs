# QFEX production stage one: how a trade connection stays alive (the liveness hold)

FBC-l931, decision 0132 items 1, 3 and 7, and decision 0143. The liveness hold: FBC-39ks and decision 0146. Environment: QFEX production, under the probe mode `production_probe`. Started 2026-11-15 03:01:53Z against QFEX production. Result: complete.

Records the journal's sink dropped: 0 Normal, 0 Safety. Sessions whose capture is partial: none.

## The accounts read before each session and around the hold

Accounts named by the run's configuration, to be read before every session: 2 (primary, subaccount; ids not shown), not a list QFEX gave: the probe sends no `GET /user/subaccounts` (decision 0144). The table below lists the reads made. Before the run the owner must confirm in QFEX's app that the user has no subaccount the configuration does not name; the probe records no such confirmation and cannot check it, so the checks below, and what they show of the user's other accounts, cover the named accounts only. A session starts only when every account it reads shows no open order on `get_user_orders` (a socket that never arms) and flat REST positions (decision 0132 item 7), and, reading that strictly, no REST `open_orders` either. The liveness hold reads them as a session does before it opens, and again after it ends, whatever ended it (decision 0146 item 3).

| read | account | position rows | flat | REST open_orders | page orders | page TWAPs | page full |
| --- | --- | --- | --- | --- | --- | --- | --- |
| before the hold | primary (held) | 11 | yes | 0 | 0 | 0 | no |
| before the hold | subaccount | 0 | yes | 0 | 0 | 0 | no |
| after the hold | primary (held) | 11 | yes | 0 | 0 | 0 | no |
| after the hold | subaccount | 0 | yes | 0 | 0 | 0 | no |

## The liveness hold (FBC-39ks, decision 0146)

One trade connection on the primary account, on QFEX production, to be held for 3600 s from its opening. It never armed, placed, cancelled or read. It wrote, in order: authentication, subscribe order_responses, subscribe fills, subscribe positions; then nothing but the pongs and the close reply the WebSocket layer writes to each server ping and server close and, at its end, its close (a session sends the same authentication and the same three subscriptions: order_responses, fills, positions). It sent no ping of its own. Each frame it wrote was charged to the run's shared limiter first, and each pong and close reply the WebSocket layer wrote for it recorded there, as a session's are (decision 0030). No session and no other connection of the probe was open while it was. Its frames are not journaled: each ping, pong and close is kept below by its time since the connection opened, a payload or a close reason by its length only and a close code in full (decision 0041), and its data frames are counted by kind, never kept. Result: complete.

Every account the run's configuration names was read before the hold opened and again after it ended, whatever ended it (the accounts table's rows `before the hold` and `after the hold`). **Conditions of the run, which the probe cannot check** (decision 0146 item 3): the owner confirmed before the run that the user has no subaccount the configuration leaves out (decision 0144 item 2); and the owner asserts that nothing traded on any account of the user from the first read before the hold to the last read after it.

- Connection opened: yes
- Authenticated: 368 ms since the connection opened
- Subscriptions confirmed: 3 of 3, the last at 589 ms
- Any ping before authentication: no
- Pings after authentication: 359, the first 10048 ms after it
- Intervals between the pings after authentication: 9959 ms, 9997 ms, 10001 ms, 9997 ms, 10001 ms, 10004 ms, 9998 ms, 10062 ms, 9963 ms, 9980 ms, 9998 ms, 9999 ms, 10056 ms, 9961 ms, 9987 ms, 9998 ms, 10003 ms, 9999 ms, 10001 ms, 10000 ms, 10072 ms, 9959 ms, 9977 ms, 9993 ms, 10002 ms, 10069 ms, 9956 ms, 9980 ms, 9997 ms, 10004 ms, 10005 ms, 10007 ms, 9992 ms, 10076 ms, 9972 ms, 9951 ms, 9998 ms, 10003 ms, 10074 ms, 9960 ms, 9968 ms, 9999 ms, 9999 ms, 10001 ms, 10000 ms, 9997 ms, 10008 ms, 10048 ms, 9962 ms, 9987 ms, 9999 ms, 10000 ms, 10055 ms, 9959 ms, 9989 ms, 10000 ms, 10001 ms, 10000 ms, 10004 ms, 10002 ms, 10064 ms, 9966 ms, 9968 ms, 10000 ms, 10002 ms, 10059 ms, 9965 ms, 9979 ms, 10001 ms, 10003 ms, 9997 ms, 9999 ms, 10002 ms, 10085 ms, 9953 ms, 9966 ms, 10000 ms, 9999 ms, 10070 ms, 9963 ms, 9969 ms, 10003 ms, 9999 ms, 10000 ms, 10000 ms, 10002 ms, 10094 ms, 9961 ms, 9955 ms, 9991 ms, 10003 ms, 10081 ms, 9966 ms, 9958 ms, 9996 ms, 10003 ms, 10002 ms, 9996 ms, 10001 ms, 10001 ms, 10056 ms, 9961 ms, 9987 ms, 9999 ms, 10001 ms, 10051 ms, 9962 ms, 9987 ms, 10004 ms, 9999 ms, 9999 ms, 10001 ms, 10000 ms, 10070 ms, 9958 ms, 9976 ms, 10002 ms, 10001 ms, 10064 ms, 9956 ms, 9982 ms, 10001 ms, 10000 ms, 10001 ms, 10002 ms, 10000 ms, 10074 ms, 9967 ms, 9981 ms, 9981 ms, 10001 ms, 10071 ms, 9960 ms, 9969 ms, 10000 ms, 10005 ms, 10000 ms, 10002 ms, 10001 ms, 10082 ms, 9961 ms, 9959 ms, 10001 ms, 9997 ms, 10076 ms, 9965 ms, 9961 ms, 9999 ms, 10006 ms, 9998 ms, 10004 ms, 9995 ms, 10000 ms, 10055 ms, 9959 ms, 9989 ms, 10006 ms, 9996 ms, 10046 ms, 9961 ms, 10001 ms, 9995 ms, 10001 ms, 10000 ms, 10000 ms, 10001 ms, 10062 ms, 9960 ms, 9981 ms, 10000 ms, 10000 ms, 10055 ms, 9979 ms, 9966 ms, 10005 ms, 10002 ms, 9995 ms, 10001 ms, 10002 ms, 10000 ms, 10001 ms, 9998 ms, 10002 ms, 10000 ms, 10000 ms, 10000 ms, 10002 ms, 10002 ms, 10002 ms, 9997 ms, 10003 ms, 9997 ms, 10004 ms, 9998 ms, 10000 ms, 10000 ms, 10001 ms, 10001 ms, 10000 ms, 10000 ms, 10001 ms, 10011 ms, 9992 ms, 10006 ms, 10011 ms, 9990 ms, 10004 ms, 9998 ms, 10000 ms, 10001 ms, 9999 ms, 10005 ms, 9998 ms, 9999 ms, 10005 ms, 9999 ms, 9999 ms, 10000 ms, 10002 ms, 9999 ms, 10001 ms, 10001 ms, 10000 ms, 10001 ms, 10000 ms, 10002 ms, 10156 ms, 9848 ms, 9997 ms, 10000 ms, 10001 ms, 10000 ms, 10001 ms, 10000 ms, 10003 ms, 10033 ms, 9966 ms, 10000 ms, 10001 ms, 10030 ms, 9971 ms, 9999 ms, 10001 ms, 10002 ms, 9999 ms, 9999 ms, 10002 ms, 10002 ms, 9998 ms, 10002 ms, 9998 ms, 10001 ms, 10000 ms, 10003 ms, 9997 ms, 10003 ms, 10003 ms, 10003 ms, 9998 ms, 9997 ms, 10002 ms, 9998 ms, 10000 ms, 10001 ms, 9999 ms, 10001 ms, 9999 ms, 10001 ms, 10001 ms, 10000 ms, 10004 ms, 9998 ms, 10002 ms, 10000 ms, 10001 ms, 10000 ms, 10001 ms, 10003 ms, 9997 ms, 10001 ms, 9999 ms, 10004 ms, 10000 ms, 10002 ms, 9999 ms, 10000 ms, 10000 ms, 10003 ms, 10000 ms, 9998 ms, 10003 ms, 9999 ms, 10100 ms, 9900 ms, 10000 ms, 10000 ms, 10000 ms, 10000 ms, 10003 ms, 9999 ms, 10001 ms, 10002 ms, 9998 ms, 10003 ms, 9998 ms, 10001 ms, 10000 ms, 10005 ms, 9999 ms, 9999 ms, 10002 ms, 10000 ms, 10005 ms, 9996 ms, 10000 ms, 10001 ms, 10000 ms, 10000 ms, 9981 ms, 9998 ms, 11208 ms, 8793 ms, 9999 ms, 10000 ms, 10000 ms, 10000 ms, 10002 ms, 10000 ms, 10002 ms, 10066 ms, 9935 ms, 9999 ms, 10001 ms, 10002 ms, 10073 ms, 9933 ms, 9994 ms, 10003 ms, 9999 ms, 10001 ms, 10000 ms, 10001 ms, 10001 ms, 10001 ms, 10001 ms, 10003 ms, 9996 ms, 10176 ms, 10032 ms, 9995 ms, 10008 ms, 9994 ms, 9992 ms, 10040 ms, 9791 ms, 10001 ms, 10001 ms, 10002 ms (shortest 8793 ms, longest 11208 ms)
- Pings in all: 359; pongs received: 0
- Server closes: none within the hold
- Ended by: its duration, --hold-secs, at 3600002 ms since the connection opened
- Its own close: sent at 3600003 ms; no echo of it was read within --read-wait-ms
- Replies the WebSocket layer wrote for it, each recorded on the shared limiter: 359
- Data frames received, counted and never kept: 675; by kind, read from what a journal would keep of each: authenticated 1; position_response 671; subscribed 3
- Order or fill frames received: none
- Accounts read after it: every named account showed no open order and flat positions, as before it
- Error: none

A lifetime longer than the hold was not measured: a hold that ended at its duration shows only that QFEX did not close the connection within it. A code 1002 with an 18-byte reason may stand for a close code the WebSocket layer does not allow (1004, 1005, 1006, 1015, one from 1016 to 2999, or one outside 1000 to 4999), which it reports in its place. The result speaks for QFEX production only (decision 0146 item 8).

| n | ms since the connection opened | frame | payload or reason bytes | close code |
| --- | --- | --- | --- | --- |
| 1 | 10417 | ping | 8 |  |
| 2 | 20376 | ping | 8 |  |
| 3 | 30374 | ping | 8 |  |
| 4 | 40375 | ping | 8 |  |
| 5 | 50373 | ping | 8 |  |
| 6 | 60374 | ping | 8 |  |
| 7 | 70378 | ping | 8 |  |
| 8 | 80376 | ping | 8 |  |
| 9 | 90439 | ping | 8 |  |
| 10 | 100402 | ping | 8 |  |
| 11 | 110382 | ping | 8 |  |
| 12 | 120381 | ping | 8 |  |
| 13 | 130380 | ping | 8 |  |
| 14 | 140437 | ping | 8 |  |
| 15 | 150399 | ping | 8 |  |
| 16 | 160387 | ping | 8 |  |
| 17 | 170385 | ping | 8 |  |
| 18 | 180388 | ping | 8 |  |
| 19 | 190388 | ping | 8 |  |
| 20 | 200389 | ping | 8 |  |
| 21 | 210390 | ping | 8 |  |
| 22 | 220463 | ping | 8 |  |
| 23 | 230422 | ping | 8 |  |
| 24 | 240399 | ping | 8 |  |
| 25 | 250393 | ping | 8 |  |
| 26 | 260395 | ping | 8 |  |
| 27 | 270465 | ping | 8 |  |
| 28 | 280422 | ping | 8 |  |
| 29 | 290402 | ping | 8 |  |
| 30 | 300400 | ping | 8 |  |
| 31 | 310404 | ping | 8 |  |
| 32 | 320410 | ping | 8 |  |
| 33 | 330417 | ping | 8 |  |
| 34 | 340409 | ping | 8 |  |
| 35 | 350486 | ping | 8 |  |
| 36 | 360458 | ping | 8 |  |
| 37 | 370409 | ping | 8 |  |
| 38 | 380408 | ping | 8 |  |
| 39 | 390412 | ping | 8 |  |
| 40 | 400486 | ping | 8 |  |
| 41 | 410447 | ping | 8 |  |
| 42 | 420415 | ping | 8 |  |
| 43 | 430415 | ping | 8 |  |
| 44 | 440415 | ping | 8 |  |
| 45 | 450416 | ping | 8 |  |
| 46 | 460417 | ping | 8 |  |
| 47 | 470414 | ping | 8 |  |
| 48 | 480422 | ping | 8 |  |
| 49 | 490471 | ping | 8 |  |
| 50 | 500433 | ping | 8 |  |
| 51 | 510421 | ping | 8 |  |
| 52 | 520420 | ping | 8 |  |
| 53 | 530421 | ping | 8 |  |
| 54 | 540476 | ping | 8 |  |
| 55 | 550435 | ping | 8 |  |
| 56 | 560425 | ping | 8 |  |
| 57 | 570425 | ping | 8 |  |
| 58 | 580427 | ping | 8 |  |
| 59 | 590428 | ping | 8 |  |
| 60 | 600432 | ping | 8 |  |
| 61 | 610434 | ping | 8 |  |
| 62 | 620499 | ping | 8 |  |
| 63 | 630465 | ping | 8 |  |
| 64 | 640433 | ping | 8 |  |
| 65 | 650434 | ping | 8 |  |
| 66 | 660436 | ping | 8 |  |
| 67 | 670496 | ping | 8 |  |
| 68 | 680461 | ping | 8 |  |
| 69 | 690441 | ping | 8 |  |
| 70 | 700442 | ping | 8 |  |
| 71 | 710445 | ping | 8 |  |
| 72 | 720442 | ping | 8 |  |
| 73 | 730441 | ping | 8 |  |
| 74 | 740444 | ping | 8 |  |
| 75 | 750530 | ping | 8 |  |
| 76 | 760483 | ping | 8 |  |
| 77 | 770450 | ping | 8 |  |
| 78 | 780450 | ping | 8 |  |
| 79 | 790449 | ping | 8 |  |
| 80 | 800520 | ping | 8 |  |
| 81 | 810483 | ping | 8 |  |
| 82 | 820453 | ping | 8 |  |
| 83 | 830456 | ping | 8 |  |
| 84 | 840455 | ping | 8 |  |
| 85 | 850456 | ping | 8 |  |
| 86 | 860456 | ping | 8 |  |
| 87 | 870459 | ping | 8 |  |
| 88 | 880553 | ping | 8 |  |
| 89 | 890514 | ping | 8 |  |
| 90 | 900470 | ping | 8 |  |
| 91 | 910461 | ping | 8 |  |
| 92 | 920465 | ping | 8 |  |
| 93 | 930547 | ping | 8 |  |
| 94 | 940513 | ping | 8 |  |
| 95 | 950471 | ping | 8 |  |
| 96 | 960468 | ping | 8 |  |
| 97 | 970472 | ping | 8 |  |
| 98 | 980474 | ping | 8 |  |
| 99 | 990471 | ping | 8 |  |
| 100 | 1000473 | ping | 8 |  |
| 101 | 1010475 | ping | 8 |  |
| 102 | 1020531 | ping | 8 |  |
| 103 | 1030492 | ping | 8 |  |
| 104 | 1040480 | ping | 8 |  |
| 105 | 1050480 | ping | 8 |  |
| 106 | 1060481 | ping | 8 |  |
| 107 | 1070533 | ping | 8 |  |
| 108 | 1080495 | ping | 8 |  |
| 109 | 1090482 | ping | 8 |  |
| 110 | 1100486 | ping | 8 |  |
| 111 | 1110486 | ping | 8 |  |
| 112 | 1120486 | ping | 8 |  |
| 113 | 1130487 | ping | 8 |  |
| 114 | 1140488 | ping | 8 |  |
| 115 | 1150558 | ping | 8 |  |
| 116 | 1160517 | ping | 8 |  |
| 117 | 1170493 | ping | 8 |  |
| 118 | 1180496 | ping | 8 |  |
| 119 | 1190497 | ping | 8 |  |
| 120 | 1200561 | ping | 8 |  |
| 121 | 1210517 | ping | 8 |  |
| 122 | 1220499 | ping | 8 |  |
| 123 | 1230501 | ping | 8 |  |
| 124 | 1240502 | ping | 8 |  |
| 125 | 1250503 | ping | 8 |  |
| 126 | 1260506 | ping | 8 |  |
| 127 | 1270506 | ping | 8 |  |
| 128 | 1280581 | ping | 8 |  |
| 129 | 1290548 | ping | 8 |  |
| 130 | 1300529 | ping | 8 |  |
| 131 | 1310510 | ping | 8 |  |
| 132 | 1320512 | ping | 8 |  |
| 133 | 1330583 | ping | 8 |  |
| 134 | 1340544 | ping | 8 |  |
| 135 | 1350514 | ping | 8 |  |
| 136 | 1360515 | ping | 8 |  |
| 137 | 1370521 | ping | 8 |  |
| 138 | 1380521 | ping | 8 |  |
| 139 | 1390523 | ping | 8 |  |
| 140 | 1400524 | ping | 8 |  |
| 141 | 1410607 | ping | 8 |  |
| 142 | 1420569 | ping | 8 |  |
| 143 | 1430529 | ping | 8 |  |
| 144 | 1440531 | ping | 8 |  |
| 145 | 1450529 | ping | 8 |  |
| 146 | 1460605 | ping | 8 |  |
| 147 | 1470570 | ping | 8 |  |
| 148 | 1480532 | ping | 8 |  |
| 149 | 1490531 | ping | 8 |  |
| 150 | 1500538 | ping | 8 |  |
| 151 | 1510536 | ping | 8 |  |
| 152 | 1520540 | ping | 8 |  |
| 153 | 1530536 | ping | 8 |  |
| 154 | 1540536 | ping | 8 |  |
| 155 | 1550591 | ping | 8 |  |
| 156 | 1560551 | ping | 8 |  |
| 157 | 1570541 | ping | 8 |  |
| 158 | 1580547 | ping | 8 |  |
| 159 | 1590543 | ping | 8 |  |
| 160 | 1600589 | ping | 8 |  |
| 161 | 1610551 | ping | 8 |  |
| 162 | 1620552 | ping | 8 |  |
| 163 | 1630548 | ping | 8 |  |
| 164 | 1640549 | ping | 8 |  |
| 165 | 1650550 | ping | 8 |  |
| 166 | 1660551 | ping | 8 |  |
| 167 | 1670552 | ping | 8 |  |
| 168 | 1680614 | ping | 8 |  |
| 169 | 1690575 | ping | 8 |  |
| 170 | 1700556 | ping | 8 |  |
| 171 | 1710557 | ping | 8 |  |
| 172 | 1720558 | ping | 8 |  |
| 173 | 1730614 | ping | 8 |  |
| 174 | 1740594 | ping | 8 |  |
| 175 | 1750560 | ping | 8 |  |
| 176 | 1760565 | ping | 8 |  |
| 177 | 1770568 | ping | 8 |  |
| 178 | 1780563 | ping | 8 |  |
| 179 | 1790565 | ping | 8 |  |
| 180 | 1800567 | ping | 8 |  |
| 181 | 1810567 | ping | 8 |  |
| 182 | 1820569 | ping | 8 |  |
| 183 | 1830568 | ping | 8 |  |
| 184 | 1840571 | ping | 8 |  |
| 185 | 1850571 | ping | 8 |  |
| 186 | 1860572 | ping | 8 |  |
| 187 | 1870572 | ping | 8 |  |
| 188 | 1880575 | ping | 8 |  |
| 189 | 1890577 | ping | 8 |  |
| 190 | 1900579 | ping | 8 |  |
| 191 | 1910577 | ping | 8 |  |
| 192 | 1920581 | ping | 8 |  |
| 193 | 1930579 | ping | 8 |  |
| 194 | 1940583 | ping | 8 |  |
| 195 | 1950581 | ping | 8 |  |
| 196 | 1960582 | ping | 8 |  |
| 197 | 1970582 | ping | 8 |  |
| 198 | 1980584 | ping | 8 |  |
| 199 | 1990585 | ping | 8 |  |
| 200 | 2000586 | ping | 8 |  |
| 201 | 2010586 | ping | 8 |  |
| 202 | 2020588 | ping | 8 |  |
| 203 | 2030600 | ping | 8 |  |
| 204 | 2040592 | ping | 8 |  |
| 205 | 2050599 | ping | 8 |  |
| 206 | 2060610 | ping | 8 |  |
| 207 | 2070601 | ping | 8 |  |
| 208 | 2080605 | ping | 8 |  |
| 209 | 2090603 | ping | 8 |  |
| 210 | 2100604 | ping | 8 |  |
| 211 | 2110605 | ping | 8 |  |
| 212 | 2120605 | ping | 8 |  |
| 213 | 2130610 | ping | 8 |  |
| 214 | 2140608 | ping | 8 |  |
| 215 | 2150608 | ping | 8 |  |
| 216 | 2160613 | ping | 8 |  |
| 217 | 2170612 | ping | 8 |  |
| 218 | 2180611 | ping | 8 |  |
| 219 | 2190612 | ping | 8 |  |
| 220 | 2200614 | ping | 8 |  |
| 221 | 2210614 | ping | 8 |  |
| 222 | 2220616 | ping | 8 |  |
| 223 | 2230617 | ping | 8 |  |
| 224 | 2240617 | ping | 8 |  |
| 225 | 2250619 | ping | 8 |  |
| 226 | 2260619 | ping | 8 |  |
| 227 | 2270621 | ping | 8 |  |
| 228 | 2280778 | ping | 8 |  |
| 229 | 2290626 | ping | 8 |  |
| 230 | 2300623 | ping | 8 |  |
| 231 | 2310624 | ping | 8 |  |
| 232 | 2320626 | ping | 8 |  |
| 233 | 2330626 | ping | 8 |  |
| 234 | 2340627 | ping | 8 |  |
| 235 | 2350628 | ping | 8 |  |
| 236 | 2360632 | ping | 8 |  |
| 237 | 2370665 | ping | 8 |  |
| 238 | 2380631 | ping | 8 |  |
| 239 | 2390632 | ping | 8 |  |
| 240 | 2400634 | ping | 8 |  |
| 241 | 2410665 | ping | 8 |  |
| 242 | 2420636 | ping | 8 |  |
| 243 | 2430636 | ping | 8 |  |
| 244 | 2440638 | ping | 8 |  |
| 245 | 2450641 | ping | 8 |  |
| 246 | 2460640 | ping | 8 |  |
| 247 | 2470640 | ping | 8 |  |
| 248 | 2480643 | ping | 8 |  |
| 249 | 2490645 | ping | 8 |  |
| 250 | 2500644 | ping | 8 |  |
| 251 | 2510647 | ping | 8 |  |
| 252 | 2520646 | ping | 8 |  |
| 253 | 2530647 | ping | 8 |  |
| 254 | 2540648 | ping | 8 |  |
| 255 | 2550651 | ping | 8 |  |
| 256 | 2560649 | ping | 8 |  |
| 257 | 2570652 | ping | 8 |  |
| 258 | 2580656 | ping | 8 |  |
| 259 | 2590659 | ping | 8 |  |
| 260 | 2600658 | ping | 8 |  |
| 261 | 2610656 | ping | 8 |  |
| 262 | 2620658 | ping | 8 |  |
| 263 | 2630657 | ping | 8 |  |
| 264 | 2640658 | ping | 8 |  |
| 265 | 2650660 | ping | 8 |  |
| 266 | 2660659 | ping | 8 |  |
| 267 | 2670661 | ping | 8 |  |
| 268 | 2680661 | ping | 8 |  |
| 269 | 2690663 | ping | 8 |  |
| 270 | 2700664 | ping | 8 |  |
| 271 | 2710664 | ping | 8 |  |
| 272 | 2720669 | ping | 8 |  |
| 273 | 2730667 | ping | 8 |  |
| 274 | 2740669 | ping | 8 |  |
| 275 | 2750670 | ping | 8 |  |
| 276 | 2760672 | ping | 8 |  |
| 277 | 2770672 | ping | 8 |  |
| 278 | 2780673 | ping | 8 |  |
| 279 | 2790676 | ping | 8 |  |
| 280 | 2800673 | ping | 8 |  |
| 281 | 2810675 | ping | 8 |  |
| 282 | 2820674 | ping | 8 |  |
| 283 | 2830679 | ping | 8 |  |
| 284 | 2840679 | ping | 8 |  |
| 285 | 2850682 | ping | 8 |  |
| 286 | 2860681 | ping | 8 |  |
| 287 | 2870682 | ping | 8 |  |
| 288 | 2880682 | ping | 8 |  |
| 289 | 2890686 | ping | 8 |  |
| 290 | 2900686 | ping | 8 |  |
| 291 | 2910685 | ping | 8 |  |
| 292 | 2920689 | ping | 8 |  |
| 293 | 2930688 | ping | 8 |  |
| 294 | 2940788 | ping | 8 |  |
| 295 | 2950689 | ping | 8 |  |
| 296 | 2960690 | ping | 8 |  |
| 297 | 2970690 | ping | 8 |  |
| 298 | 2980691 | ping | 8 |  |
| 299 | 2990692 | ping | 8 |  |
| 300 | 3000695 | ping | 8 |  |
| 301 | 3010694 | ping | 8 |  |
| 302 | 3020696 | ping | 8 |  |
| 303 | 3030699 | ping | 8 |  |
| 304 | 3040698 | ping | 8 |  |
| 305 | 3050701 | ping | 8 |  |
| 306 | 3060699 | ping | 8 |  |
| 307 | 3070701 | ping | 8 |  |
| 308 | 3080702 | ping | 8 |  |
| 309 | 3090707 | ping | 8 |  |
| 310 | 3100707 | ping | 8 |  |
| 311 | 3110706 | ping | 8 |  |
| 312 | 3120709 | ping | 8 |  |
| 313 | 3130709 | ping | 8 |  |
| 314 | 3140714 | ping | 8 |  |
| 315 | 3150711 | ping | 8 |  |
| 316 | 3160711 | ping | 8 |  |
| 317 | 3170713 | ping | 8 |  |
| 318 | 3180714 | ping | 8 |  |
| 319 | 3190714 | ping | 8 |  |
| 320 | 3200696 | ping | 8 |  |
| 321 | 3210695 | ping | 8 |  |
| 322 | 3221903 | ping | 8 |  |
| 323 | 3230696 | ping | 8 |  |
| 324 | 3240696 | ping | 8 |  |
| 325 | 3250697 | ping | 8 |  |
| 326 | 3260698 | ping | 8 |  |
| 327 | 3270698 | ping | 8 |  |
| 328 | 3280700 | ping | 8 |  |
| 329 | 3290701 | ping | 8 |  |
| 330 | 3300703 | ping | 8 |  |
| 331 | 3310770 | ping | 8 |  |
| 332 | 3320705 | ping | 8 |  |
| 333 | 3330705 | ping | 8 |  |
| 334 | 3340707 | ping | 8 |  |
| 335 | 3350709 | ping | 8 |  |
| 336 | 3360783 | ping | 8 |  |
| 337 | 3370716 | ping | 8 |  |
| 338 | 3380710 | ping | 8 |  |
| 339 | 3390713 | ping | 8 |  |
| 340 | 3400713 | ping | 8 |  |
| 341 | 3410714 | ping | 8 |  |
| 342 | 3420714 | ping | 8 |  |
| 343 | 3430716 | ping | 8 |  |
| 344 | 3440717 | ping | 8 |  |
| 345 | 3450718 | ping | 8 |  |
| 346 | 3460719 | ping | 8 |  |
| 347 | 3470723 | ping | 8 |  |
| 348 | 3480719 | ping | 8 |  |
| 349 | 3490896 | ping | 8 |  |
| 350 | 3500928 | ping | 8 |  |
| 351 | 3510924 | ping | 8 |  |
| 352 | 3520932 | ping | 8 |  |
| 353 | 3530926 | ping | 8 |  |
| 354 | 3540919 | ping | 8 |  |
| 355 | 3550959 | ping | 8 |  |
| 356 | 3560750 | ping | 8 |  |
| 357 | 3570752 | ping | 8 |  |
| 358 | 3580753 | ping | 8 |  |
| 359 | 3590756 | ping | 8 |  |

## The sessions

None: the run asked for no session (no --arm).

## Provider rate refusals

Decision 0135 item 2: how many provider rate refusals each session and each read socket met. The probe provokes none (0135 item 1). Each is counted three ways: how much the run's provider-user limiter's `counts().rejected` rose while it ran (a session's holds, which the runtime makes for its codec's `RateLimited`, decision 0126, and a read socket's, which the probe makes for each `RateLimited` err the adapter reads in one of its frames); its frames in an `err` whose `error_code` is `RateLimited`; and its `order_response`s whose `status` is `RATE_LIMITED`. A session's frames are those its journal kept; a read socket's, which are not journaled, the two it read (the authentication reply and the page), each read as a journal would keep it.

A frame the journal keeps whole (decision 0134 item 2: an echo of authentication, a JSON escape, a repeated member) shows no code and holds no budget (0134 item 6), so none of those counts can see a refusal in it: each such frame, and each that is not JSON, is counted unread. A read socket's unread frames are read once more in memory, as they arrived (nothing of them is written), for a refusal kept whole; a session's reach only its codec and the journal, so a refusal in one it keeps whole is not seen here. The REST reads' answers are not read for refusals: a 429 or 418 on one stops the probe, its status named where it stopped.

What one met is at least the larger of the limiter's count and the refusals its frames show: the two count the same refusals, but each can miss one the other sees (a hold whose frame a partial capture lost, a frame the codec held no budget for), so the larger is a lower bound, not a total.

The liveness hold's connection (decision 0146) is counted as a read socket is, every data frame it received read the same way, and is listed under "the hold" with the read sockets before it.

No provider rate refusal was counted.

Frames unread, kept whole or not JSON: 0. A refusal in a session's unread frame would not show in its counts.

Met by each session, at least, with the read sockets before it: the hold 0; after the hold 0.

| session | on | limiter rejected | RateLimited errs | RATE_LIMITED order responses | frames unread | refusals in frames kept whole | met, at least | refusal frames |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| the hold | read socket: primary (held) | 0 | 0 | 0 | 0 | 0 | 0 | not journaled |
| the hold | read socket: subaccount | 0 | 0 | 0 | 0 | 0 | 0 | not journaled |
| the hold | the hold's connection: primary | 0 | 0 | 0 | 0 | 0 | 0 | not journaled |
| after the hold | read socket: primary (held) | 0 | 0 | 0 | 0 | 0 | 0 | not journaled |
| after the hold | read socket: subaccount | 0 | 0 | 0 | 0 | 0 | 0 | not journaled |

## Files

Fixtures written: README.md. Withheld: none. Not written or not checked: none.

Members whose string value a fixture keeps as received: none. The privacy check knows credential and address shapes only: read every fixture for an identifying string (an email, a user name, an address in another form) before committing it.

Not measured by stage one (FBC-9sk2's stages): whether arming cancels open orders (a session starts only on accounts whose reads above show none), whether a graceful close or a drop cancels, and everything decision 0132 asks of a place. Not checked by the probe: whether the user has a subaccount the configuration does not name (the owner must confirm none before the run, which the probe does not record: decision 0144).
