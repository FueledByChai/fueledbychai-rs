# QFEX production: how an authenticated trade connection stays alive (the liveness hold)

Recorded on 2026-11-15 on QFEX production, under the probe mode `production_probe`. The
owner-run program `qfex_preprod_probe` (crates/fbc-examples, FBC-39ks, decision 0146) ran with
`--production --hold-secs 3600` and no `--arm`.

Each run held one trade connection on the primary account. The connection:

- sent its authentication;
- sent the three subscriptions a session sends (`order_responses`, `fills`, `positions`), and
  QFEX confirmed all three;
- then sent nothing but the pongs the WebSocket layer writes to each server ping and, in the
  first run, its reply to the server's close;
- and, in the hold that reached its duration, sent its own close at the end.

It never armed, placed, cancelled or read. Both named accounts (the primary and the subaccount)
were read before each run's hold (decision 0146 item 3). After the hour-long hold they were read
again; after the first run they could not be (below).

These are recorded results, not synthetic ones, so this directory carries no `SYNTHETIC` marker.
No frame is committed (decision 0146 item 6), because the hold journals none. What the
connection heard is in each run's `report.md` and `evidence.json`, written as the probe wrote
them:

- each ping, pong and close, by its time since the connection opened;
- a payload or a close reason by its length only, and a close code in full;
- data frames counted by kind and never kept.

Neither file holds an account id, a symbol or an amount: position rows are counted, not listed.
The owner was given all four files to read before they were committed (decision 0146 item 6).
Asked "Commit both runs' reports under FBC-sbbl and take its no-keepalive branch?", the owner
answered "Yes, commit both" (FBC-sbbl).

| Run | Files | Started (UTC) | Ended by | Result |
| --- | --- | --- | --- | --- |
| The hold | `report.md`, `evidence.json` | 03:01:53Z | its duration, 3600002 ms after the connection opened | complete |
| The first run | `first-run/report.md`, `first-run/evidence.json` | 02:05:33Z | a server close, 1535872 ms after the connection opened, during a QFEX outage | incomplete: neither account could be read after it |

Times below are in milliseconds since the connection opened, unless they say otherwise. The probe
truncates each time and each interval to a whole millisecond on its own, so an interval can differ
by 1 ms from the difference of the two ping times around it.

## The hold (`report.md`, `evidence.json`)

- **Before authentication.** No ping came. The connection authenticated at 368 ms, and its last
  subscription was confirmed at 589 ms.
- **Pings after authentication.**
  - 359 server pings came, each with an 8-byte payload. The first was 10048 ms after
    authentication (at 10417 ms) and the last at 3590756 ms.
  - The 358 intervals between them have a median of 10000 ms. 352 lie between 9900 ms and
    10100 ms. The shortest is 8793 ms and the longest 11208 ms; those two are consecutive, one
    ping arriving late and the next on its usual time.
  - The WebSocket layer wrote a pong for each of the 359 pings. The hold received no pong,
    because it sent no ping.
- **Server closes.** None came within the hold.
- **How it ended.** At its duration (`--hold-secs`), at 3600002 ms. It sent its own close at
  3600003 ms, and no echo of that close was read within `--read-wait-ms`.
- **Data frames.** There were 675 data frames: `authenticated` 1, `subscribed` 3 and
  `position_response` 671, and no order or fill frame. The probe counts data frames by kind and
  records no arrival times. Until its own close at the end, our side sent only pongs.
- **Accounts.** Both named accounts read flat, with no open order, before the hold and again
  after it.
- **Rate refusals.** None were counted.

## The first run (`first-run/`)

It started at 02:05:33Z with the same flags.

- It authenticated at 351 ms, and its last subscription was confirmed at 480 ms. No ping came
  before authentication.
- 153 pings came after authentication, the first 10062 ms after it and the last at 1530512 ms.
  The 152 intervals between them lie between 9845 ms and 10155 ms.
- One server close came, at 1535872 ms, 5360 ms after the last ping. It had no close code (a
  close with no body) and a 0-byte reason. The hold had sent no close of its own. The WebSocket
  layer wrote 154 replies: 153 pongs and the close reply.
- The reads after it could not open a socket within 10000 ms on either account ("no read socket
  within 10000 ms"), so the probe stopped there and the run is incomplete.

The owner reported that QFEX was down for about 15 minutes at that time. The hold above started
about half an hour after that close. Its reads before it showed both accounts flat, with no open
order.

How this run reads:

- The server was still pinging every 10 s until 5.4 s before it closed, and new connections then
  failed to open. So the close is read as the outage's, not as an idle timeout or a lifetime, and
  a client keepalive would not have prevented it.
- It is kept because it shows that QFEX production can close an authenticated trade connection
  with no close code. Such a close would end an armed session's connection, and the next
  connection must arm again (decision 0121).
- Whether QFEX then cancels the account's orders was not measured, since neither run armed. Its
  cancel-on-disconnect page, as decision 0132's Context quotes it, says the engine cancels them
  if the connection drops unexpectedly, and says nothing of a close.

## What this settles (FBC-sbbl)

The hold shows server pings after authentication and no server close within it. This is
FBC-39ks's step 2, on production: the trade codec sends no client keepalive
(`crates/venues/fbc-venue-qfex/src/exec/mod.rs`).

This matches QFEX's
[authenticate](https://docs.qfex.com/websocket/channels/trade/authenticate) page as read on
2026-11-15. It says that once a client has authenticated, the server keeps the connection alive
with periodic WebSocket ping frames.

What was not measured:

- **A lifetime longer than the hold.** A hold that ends at its duration shows only that QFEX did
  not close the connection within 3600 s, and the first run lasted 1535872 ms.
- **An armed connection.** Neither run armed cancel-on-disconnect.
- **Pre-production.** The result speaks for QFEX production only (decision 0146 item 8).

The probe could not check two conditions of both runs, which each report states (decision 0146
item 3):

- the owner confirmed that the user has no subaccount the configuration leaves out (decision 0144
  item 2);
- the owner asserts that nothing traded on any account of the user from the first read to the
  last.

`crates/venues/fbc-venue-qfex/tests/hold_production.rs` holds every number this README takes
from the runs to both `evidence.json` files, and holds each report to its evidence. The outage's
15 minutes are the owner's report, not the probe's.
