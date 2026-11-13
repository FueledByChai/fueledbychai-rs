# QFEX production stage one: what answers the cancel-on-disconnect arm

FBC-l931, decision 0132 items 1, 3 and 7, and decision 0143. Environment: QFEX production, under the probe mode `production_probe`. Started 2026-11-13 05:04:48Z against QFEX production. Result: complete.

Records the journal's sink dropped: 0 Normal, 0 Safety. Sessions whose capture is partial: none.

## The accounts before each session

Accounts named by the run's configuration, to be read before every session: 2 (primary, subaccount; ids not shown), not a list QFEX gave: the probe sends no `GET /user/subaccounts` (decision 0144). The table below lists the reads made. Before the run the owner must confirm in QFEX's app that the user has no subaccount the configuration does not name; the probe records no such confirmation and cannot check it, so the checks below, and what they show of the user's other accounts, cover the named accounts only. A session starts only when every account it reads shows no open order on `get_user_orders` (a socket that never arms) and flat REST positions (decision 0132 item 7), and, reading that strictly, no REST `open_orders` either.

| before | account | position rows | flat | REST open_orders | page orders | page TWAPs | page full |
| --- | --- | --- | --- | --- | --- | --- | --- |
| primary | primary (armed) | 11 | yes | 0 | 0 | 0 | no |
| primary | subaccount | 0 | yes | 0 | 0 | 0 | no |
| subaccount | primary | 11 | yes | 0 | 0 | 0 | no |
| subaccount | subaccount (armed) | 0 | yes | 0 | 0 | 0 | no |

## The sessions

### primary

- Stream authenticated (authentication and the three subscriptions confirmed): yes
- Arm written: yes; its outcome as the session reported it: Unknown
- Ended by: the arm's outcome; connection attempts 1 (failed 0), arm failures 1, frames the codec could not decode 0
- Error: none
- Capture: whole: no loss the run found could hold this session's records (no gap the journal's markers show, no drop they leave unaccounted, no record its writer left unwritten, no end of the journal left unread)

**What it shows:** An ack echoing the arm arrived in its window: the shape decision 0132 item 1 reads as the arm's Final acceptance. FBC-jxau decodes it from this capture; the drop test (0132 item 4) then shows whether the reading holds.

| n | when | what | file |
| --- | --- | --- | --- |
| 1 | before-arm | {authenticated} | primary-001-before-arm.json |
| 2 | before-arm | {subscribed} | primary-002-before-arm.json |
| 3 | before-arm | {subscribed} | primary-003-before-arm.json |
| 4 | before-arm | {subscribed} | primary-004-before-arm.json |
| 5 | in-window | {position_response} | primary-005-in-window.json |
| 6 | in-window | {position_response} | primary-006-in-window.json |
| 7 | in-window | {position_response} | primary-007-in-window.json |
| 8 | in-window | {position_response} | primary-008-in-window.json |
| 9 | in-window | {position_response} | primary-009-in-window.json |
| 10 | in-window | {position_response} | primary-010-in-window.json |
| 11 | in-window | {position_response} | primary-011-in-window.json |
| 12 | in-window | {position_response} | primary-012-in-window.json |
| 13 | in-window | {position_response} | primary-013-in-window.json |
| 14 | in-window | {position_response} | primary-014-in-window.json |
| 15 | in-window | {position_response} | primary-015-in-window.json |
| 16 | in-window | ack echoing the arm | primary-016-in-window.json |

### subaccount

- Stream authenticated (authentication and the three subscriptions confirmed): yes
- Arm written: yes; its outcome as the session reported it: Unknown
- Ended by: the arm's outcome; connection attempts 1 (failed 0), arm failures 1, frames the codec could not decode 0
- Error: none
- Capture: whole: no loss the run found could hold this session's records (no gap the journal's markers show, no drop they leave unaccounted, no record its writer left unwritten, no end of the journal left unread)

**What it shows:** An ack echoing the arm arrived in its window: the shape decision 0132 item 1 reads as the arm's Final acceptance. FBC-jxau decodes it from this capture; the drop test (0132 item 4) then shows whether the reading holds.

| n | when | what | file |
| --- | --- | --- | --- |
| 1 | before-arm | {authenticated} | subaccount-001-before-arm.json |
| 2 | before-arm | {subscribed} | subaccount-002-before-arm.json |
| 3 | before-arm | {subscribed} | subaccount-003-before-arm.json |
| 4 | before-arm | {subscribed} | subaccount-004-before-arm.json |
| 5 | in-window | ack echoing the arm | subaccount-005-in-window.json |

## Provider rate refusals

Decision 0135 item 2: how many provider rate refusals each session and each read socket met. The probe provokes none (0135 item 1). Each is counted three ways: how much the run's provider-user limiter's `counts().rejected` rose while it ran (a session's holds, which the runtime makes for its codec's `RateLimited`, decision 0126, and a read socket's, which the probe makes for each `RateLimited` err the adapter reads in one of its frames); its frames in an `err` whose `error_code` is `RateLimited`; and its `order_response`s whose `status` is `RATE_LIMITED`. A session's frames are those its journal kept; a read socket's, which are not journaled, the two it read (the authentication reply and the page), each read as a journal would keep it.

A frame the journal keeps whole (decision 0134 item 2: an echo of authentication, a JSON escape, a repeated member) shows no code and holds no budget (0134 item 6), so none of those counts can see a refusal in it: each such frame, and each that is not JSON, is counted unread. A read socket's unread frames are read once more in memory, as they arrived (nothing of them is written), for a refusal kept whole; a session's reach only its codec and the journal, so a refusal in one it keeps whole is not seen here. The REST reads' answers are not read for refusals: a 429 or 418 on one stops the probe, its status named where it stopped.

What one met is at least the larger of the limiter's count and the refusals its frames show: the two count the same refusals, but each can miss one the other sees (a hold whose frame a partial capture lost, a frame the codec held no budget for), so the larger is a lower bound, not a total.

No provider rate refusal was counted.

Frames unread, kept whole or not JSON: 0. A refusal in a session's unread frame would not show in its counts.

Met by each session, at least, with the read sockets before it: primary 0; subaccount 0.

| session | on | limiter rejected | RateLimited errs | RATE_LIMITED order responses | frames unread | refusals in frames kept whole | met, at least | refusal frames |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| primary | read socket: primary (armed) | 0 | 0 | 0 | 0 | 0 | 0 | not journaled |
| primary | read socket: subaccount | 0 | 0 | 0 | 0 | 0 | 0 | not journaled |
| primary | the session | 0 | 0 | 0 | 0 | not seen | 0 | none |
| subaccount | read socket: primary | 0 | 0 | 0 | 0 | 0 | 0 | not journaled |
| subaccount | read socket: subaccount (armed) | 0 | 0 | 0 | 0 | 0 | 0 | not journaled |
| subaccount | the session | 0 | 0 | 0 | 0 | not seen | 0 | none |

## The fill ledger of each registry

Decision 0132 item 5: each registry keeps one fill ledger across every session it serves, its age bound longer than the registry's span bound and its count bound above the fills its sessions can see, so a fill heard live is still held, and judged a duplicate, when a resync reads it again. A session that reaches the span bound is stopped, and then takes a moment to end (past the span bound, below). The bounds held for a registry when, at each of its sessions' ends, its horizon is still full and the session ended within the age bound of the registry's first session's start: its ledger forgot no fill, and none it applied could have aged out. A registry is a declared owner-assisted testnet run (decision 0067) on pre-production, as decision 0132 item 7 says, and never on production (decision 0143 item 3): stage one seeds and arms no market.

| registry | max age | max entries | span bound | fill ceiling | what the ceiling counts | held | testnet run |
| --- | --- | --- | --- | --- | --- | --- | --- |
| primary | 3600s | 1000 | 300s | 0 | no order: its sessions place none | yes | no |
| subaccount | 3600s | 1000 | 300s | 0 | no order: its sessions place none | yes | no |

| registry | session ended | after its first session's start | past the span bound | horizon | fills held | replays applied | duplicate | before watermark | beyond horizon | untimed |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| primary | primary | 11.34282425s | no | full | 0 | 0 | 0 | 0 | 0 | 0 |
| subaccount | subaccount | 11.521804458s | no | full | 0 | 0 | 0 | 0 | 0 | 0 |

## Files

Fixtures written: primary-001-before-arm.json; primary-002-before-arm.json; primary-003-before-arm.json; primary-004-before-arm.json; primary-005-in-window.json; primary-006-in-window.json; primary-007-in-window.json; primary-008-in-window.json; primary-009-in-window.json; primary-010-in-window.json; primary-011-in-window.json; primary-012-in-window.json; primary-013-in-window.json; primary-014-in-window.json; primary-015-in-window.json; primary-016-in-window.json; subaccount-001-before-arm.json; subaccount-002-before-arm.json; subaccount-003-before-arm.json; subaccount-004-before-arm.json; subaccount-005-in-window.json; README.md. Withheld: none. Not written or not checked: none.

Members whose string value a fixture keeps as received: margin_mode; subscribed; symbol; type. The privacy check knows credential and address shapes only: read every fixture for an identifying string (an email, a user name, an address in another form) before committing it.

Not measured by stage one (FBC-9sk2's stages): whether arming cancels open orders (a session starts only on accounts whose reads above show none), whether a graceful close or a drop cancels, and everything decision 0132 asks of a place. Not checked by the probe: whether the user has a subaccount the configuration does not name (the owner must confirm none before the run, which the probe does not record: decision 0144).
