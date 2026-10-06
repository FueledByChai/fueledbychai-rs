# 0057 — An order-entry session takes commands only through ExecOrders, as fbc-oms authorizations or control commands, reports each submission to its handler, and times out each unanswered request to Unknown once, augmenting 0045 and 0053

Status: accepted
Date: 2026-10-06

## Context

FBC-0ga builds the live gateway behind fbc-oms's authorization (0045) on the order-entry session
(0053, 0056): no order-affecting command reaches a codec except from fbc-oms (0013 rule 2), and
0005's rule that an Unknown order is never resent holds in the runtime. Four things were the
ticket's to decide. 0045 has the live gateway implement `fbc_oms::OrderGateway`, whose
`submit(auth, ctx, t)` is synchronous, takes the caller's `EncodeCtx` and returns a
`SubmitHandle`; but the session's codec lives inside `ExecSession::run`, which holds it for the
session's life, and a handler is called while the codec is still decoding the frame it handles,
so no synchronous call from a handler can reach the codec, and the nonces must come from the
session's own `NonceSource`, the one `on_open` and `on_timer` draw from (0014 item 1, 0053).
The runtime has to learn when a stream is authenticated, which only the codec knows. A request
can be in flight when its connection drops, and its answer can never come on the next one. And
an encode's frames go through the rate-limit buckets (0030), which until now dropped a refused
frame unwritten while the codec believed it sent (FBC-m2xw).

## Decision

- **`ExecOrders`, not `OrderGateway`.** `ExecSession::orders()` gives the session's
  `ExecOrders` (cloneable, on the session's thread). `submit(Authorization)` takes an
  order-affecting command only as the authorization fbc-oms issued, spending it, and refuses one
  for another account than the session's (`ExecSessionConfig::acct`) as
  `SubmitRefusal::OtherAccount`; `submit_control(ControlCommand)` takes one that affects no
  order. Neither takes a `VenueCommand`, and the session has no other way to write a command
  (`tests/submit_compile_fail.rs`). Each submission is given its `RpcId` at once (counted from 1
  per session) and is taken on the session's next turn, after the input being handled; once the
  session has run or dropped, both refuse with `SubmitRefusal::Ended`. The runtime does not
  implement `OrderGateway`: its `ctx` argument would let a caller choose nonces the session's
  source never reserved. The trait stays for gateways that take their context from outside
  (SimVenue, a managed gateway).
- **What became of it, once, to the handler.** `ExecHandler::on_submitted(SubmitHandle)` is
  called once per submission, in submission order, before any event answering its request: the
  receipt with the nonces the encode used, or `NotSent` and why. A command is
  `NotSent(Disconnected)` when it was submitted while the stream had no epoch the codec
  reported `Authenticated`, or is taken on another epoch than that one, including while the
  session waits to reconnect; `NotSent` for the codec's reason when `encode` refuses it;
  `NotSent(Unencodable)` when its effects do not carry its request (`Effects::carry_request`),
  name another stream or ask to reconnect, or a batch is longer than `u16::MAX` items; and
  `NotSent(RateBudget)` when the buckets do not admit its frames together. In each of those
  cases nothing is written and no deadline is set. Otherwise it is encoded with an `EncodeCtx`
  holding exactly `VenueCommand::items()` nonces from the session's source and the shard
  clock's time, the handler is told it was sent, and its effects are executed, its frames
  already charged. A source that reserves another count ends the session with
  `ExecSessionError::Nonces`, before `encode`, as for `on_timer`.
- **Authenticated is the codec's word.** The session's own stream's epoch is authenticated from
  the `Conn { Authenticated }` event the codec pushes for it until another `Conn` state for it
  or the epoch's end.
- **The RPC table.** A request's deadline runs from just before its first frame is written
  (`RpcCall::timeout`), for every frame with an `rpc`. The first event that answers it
  (`ExecEvent::answers`) clears it. One still unanswered at its deadline is handed to the codec's
  `on_rpc_timeout` once, whatever happened to its connection: on the epoch it was written on, on
  a later one, or while the session waits to reconnect, its events then stamped under the epoch
  the session waits to open. So a write that fails or stalls after bytes may have left, and a
  request in flight across a reconnect, both come back `Unknown`. The runtime never encodes or
  writes a command twice, and a reconnect re-sends nothing (0005, 0013 rule 1). A deadline that
  falls due while a write waits on a stalled peer is taken once the write ends, within the
  write-stall window (0036). Once the session has stopped, nothing more is reported (0053).

## Alternatives

- Implement `OrderGateway::submit(auth, ctx, t)` and encode synchronously: rejected. The codec is
  borrowed by the decode a handler is called from, so a submission from a handler could not
  reach it, and the caller's `ctx` would carry nonces the session's source did not reserve.
  FBC-ct0's shard host, which encodes one command at a time between polls (design §5.2), can
  give that shape when it owns the loop.
- Report `NotSent` as an `ExecEvent::Outcome` the runtime makes up: rejected. The receipt (the
  nonces a placement keeps) has no event, and two paths for one submission's fate invite a
  consumer to read one of them only.
- Time out a request in flight at the connection's drop rather than its deadline: rejected. The
  codec owns the request's state (a batch's held item outcomes) and `on_rpc_timeout` is its one
  way to report them, so the deadline stays the one trigger.
- Send a command queued while disconnected once the next epoch authenticates: rejected. Whatever
  decided it was decided before the drop; 0013 rule 1 has the venue re-read first, and FBC-w19
  gates places on each epoch's resync.

## Consequences

- A consumer runs `ExecSession::orders()` for its account and implements
  `ExecHandler::on_submitted` wherever its OMS waits for a command's outcome: a command not sent
  is reported there only. `ExecSessionConfig` names the account (`acct`).
- fbc-runtime depends on fbc-oms (0045); fbc-oms never depends on fbc-runtime.
- fbc-oms's check at submit (FBC-afd) is called where `ExecOrders::submit` takes the
  authorization, once FBC-afd defines it; until then an authorization is checked when it is
  issued only. Nothing outside fbc-oms's own tests issues one yet, so no order-affecting command
  can reach a session today; control commands can.
- The rate-limit refusal of a submitted command's frames is reported, not dropped; FBC-m2xw
  still decides it for the frames a codec asks for from `on_frame`.
- FBC-2pr journals the nonce blocks, encode contexts and deadline firings this record adds, so
  replay can take them where the live session did.

## What would show this was wrong

- A request written twice, or a request unanswered at its deadline not reported `Unknown`.
- A consumer that needs a command's receipt in the same call that submitted it (the shard host's
  decide pass), which would bring the synchronous `OrderGateway` shape back to the runtime.
- A venue whose request outlives its connection (answered on the next one), for which a
  `NotSent(Disconnected)` on a later epoch would refuse what it could still take.
