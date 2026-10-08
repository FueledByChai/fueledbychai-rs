# 0081 — An order query or fee query may go as one HTTP request, its deadline standing once its epoch ends, amending 0057

Status: accepted
Date: 2026-10-08

## Context

0057 made everything an order-entry session encodes WebSocket-only: an encode whose effects make
an HTTP request is `NotSent(Unencodable)`, nothing requested, because an HTTP request got no
deadline in the RPC table, its result is dropped once its epoch ends (0027) and the rate
pre-charge covered frames only, so it could leave a command reported sent that never reaches
`Unknown`. That covers control commands too. Paradex's caps declare order queries by client id
from `GET /orders/by_client_id/{client_id}` (0054, `query_request`), so 0005's Unknown-ladder
query (`ControlCommand::Query`), and a fee query over REST, are always `NotSent(Unencodable)` on
Paradex: conservative, nothing sent, but the ladder has no query step there (PR #87 Reviewer B
B8, FBC-m8vm).

Within its epoch an HTTP request already settles itself: every request has a timeout, and the
runtime hands the codec's `on_http` its response or the `HttpFailure` that stands for one
(`TimedOut` at the timeout), and `on_http`'s contract (Codex 4212123422 on PR #109) has the codec
report an order-entry request's outcome from that call, `Unknown` included. What it lacks is the
epoch's end: a result that comes back after it is dropped, so the codec never hears it. 0073
charges every control command the consumer submits as normal traffic, stopping at each bucket's
safety floor, and FBC-e8i's note asks that an HTTP query, and the connection it opens, be charged
so too.

## Decision

1. **Read-only control commands may go over HTTP.** The session admits an encode of a
   `ControlCommand::Query` or `ControlCommand::FeeQuery` whose effects carry the request as
   exactly one `Effect::Http` naming it (`rpc: Some(rpc)`, labelled the command's class,
   `Effects::carry_request`) and no frame, with a timeout whose deadline the clock can
   represent. Any other HTTP-carrying encode stays `NotSent(Unencodable)` with nothing
   requested or written: an order-affecting command until FBC-4nfb, a consumer's
   cancel-on-disconnect arm (which would arm another connection than the session's), a
   dead-man refresh (a session takes no dead-man venue, 0058), the session's own arm, an encode
   that mixes frames and HTTP requests, or one that makes more than one HTTP request.
2. **Charged with the encode.** The request and the connection it opens (`http_of`) are charged
   with the encode's frames, together, as the command's budget class: normal traffic for every
   control command (0073), so an HTTP query stops at the safety floor. Buckets that refuse them
   leave the command `NotSent(RateBudget)`, nothing requested, no deadline set. The request is
   then started without being charged again.
3. **Its deadline stands once its epoch ends.** As the request starts, the RPC table holds its
   deadline (its timeout from then), standing only once the epoch that asked has ended. The
   call to `on_http` for the request, whatever the codec pushes, clears it, as does an event
   answering it (`ExecEvent::answers`). While its epoch lasts, the result always comes back
   (the request's own timeout bounds it), so the deadline never races `on_http`. Once its epoch
   has ended, its result is dropped (0027) and the deadline hands the request to the codec's
   `on_rpc_timeout` once, at its deadline, on a later epoch or while the session waits to
   reconnect, as 0057's table does for a frame. It is never requested again.

0057's rule stays for order-affecting commands, which remain WebSocket-only until FBC-4nfb.

## Alternatives

- Give an HTTP request a deadline that stands from the start, as a frame's does: its deadline
  and its own timeout fall at the same instant, so the session could hand the codec
  `on_rpc_timeout` and then `on_http`'s `TimedOut` for one request, two reports where the codec
  keeps one (ParadexExec drops a query once `on_http` answers it).
- Hand a dropped result to the codec anyway, on the later epoch: 0027 drops an ended epoch's
  results because the codec's state for that epoch is gone; the codec here lives on, but the
  journal and replay place every input in its epoch, and a result handed across that boundary
  would replay differently.
- Time out an ended epoch's HTTP requests at the epoch's end rather than at their deadline:
  0057 rejected that for frames; the deadline stays the one trigger.
- Let every control command, the consumer's arm included, go over HTTP: an arm over HTTP arms no
  connection the session holds, and nothing yet needs it.

## Consequences

- Paradex's REST order query and a REST fee query reach the venue, so the Unknown ladder has its
  query step on Paradex once a consumer submits it.
- `on_rpc_timeout`'s contract covers a request asked as an HTTP request whose result never
  reached `on_http`; a codec reporting an HTTP request's outcome keeps its state for it until
  either call.
- The journal records the request as it starts (0078), as any HTTP request; the deadline firing
  is not journaled yet (FBC-0hfl).

## What would show this was wrong

- One HTTP query reported twice (`on_http` and `on_rpc_timeout`), or reported never.
- An HTTP query requested twice, or one requested past the safety floor.
- A venue whose HTTP result outlives the connection that asked in a way a codec needs to read
  (an answer only the next epoch can take).
