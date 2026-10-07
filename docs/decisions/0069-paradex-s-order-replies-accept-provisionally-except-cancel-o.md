# 0069 — Paradex's order replies accept provisionally except cancel-on-disconnect's, refuse by a table keyed by code, and leave every other error Unknown

Status: accepted
Date: 2026-10-07

## Context

FBC-0l9 (BT-402) decodes the JSON-RPC replies to Paradex's order methods into one
`ExecEvent::Outcome` per item, which the OMS maps onto the order lattice (0005; 0014 items 3
and 6). Two things are not fixed by an earlier record. First, the `AckLevel` of each acceptance:
0054 makes Paradex `AckModel::TwoPhase` and `AmendAck::ReplacedEvent`, but says nothing of a
cancel's `QUEUED_FOR_CANCELLATION`, cancel-all's `{"status":"ok"}` or cancel-on-disconnect's
`{"enabled":..}`; and the OMS ends an order Canceled on a tombstone cancel accepted at
`AckLevel::Final`, and the runtime counts an arm only at `Final` (0058). Second, which errors
are refusals. docs.paradex.trade's WebSocket "Error Handling" page
(`ws/general-information/error-handling`) documents the JSON-RPC codes -32700, -32600, -32601,
-32602 and -32603 and Paradex's 100 (method error), 40110, 40111 and 40112, with no finer reason
(no post-only, price, size or margin code); `order.create_batch`'s page gives a failed item an
`error` message and no code; `order.cancel_batch`'s gives each order QUEUED_FOR_CANCELLATION,
ALREADY_CLOSED or NOT_FOUND. A refusal of a placement ends the order (`TerminalKind::Rejected`),
so a refusal the venue did not mean leaves an order resting that the OMS believes ended.

## Decision

1. **Acceptances.** `order.create` and each created `order.create_batch` item, `order.modify`,
   `order.cancel` and each `order.cancel_batch` item QUEUED_FOR_CANCELLATION, and
   `order.cancel_all`'s `ok` are `Accepted { ack: Provisional }`: a placement waits for its
   risk check, an amend is final on its order event (0054), and a queued cancel or cancel-all
   is done only when the order events close the orders, which may fill first. The reply to
   `order.cancel_on_disconnect` states the connection's new state, so it is
   `Accepted { ack: Final }` when that state is the one asked for; any other reply to it is
   refused as malformed and the request times out `Unknown`.
2. **The reject table.** `fbc_venue_paradex::exec::REJECT_CODES`, keyed by the venue's code
   (design §6 step 9), never by message text: -32700, -32600, -32602, 100, 40110, 40111 and
   40112 are `RejectKind::Other`; -32601 (method not found) `RejectKind::Unsupported`;
   ALREADY_CLOSED `RejectKind::AlreadyTerminal(TerminalHint::Unspecified)` and NOT_FOUND
   `RejectKind::NotFound`, which leave the cancelled order as it was (0014 item 6). An error
   frame with the request's id refuses the whole request (`item: None`) as its code's kind. A
   batch item's `error` message refuses that item as `RejectKind::Other` with no code, as the
   page documents it a failure of the item.
3. **Everything else is Unknown.** -32603 (internal error) does not say the venue left the
   request undone, so it is not in the table; it and every code or item status no page
   documents are `SubmitOutcome::Unknown`, resolved by the Unknown ladder and never resent
   (0005). A request whose every item was reported `Unknown` from its reply gets nothing more
   at its timeout. A batch reply with fewer results than items answers its items and is
   `Unknown` for the rest, in the same call.
4. **No id.** An error frame whose id is absent or null answers no request: it is an
   `ExecEvent::UncorrelatedError` keeping its code, its kind from the table or
   `RejectKind::Other`.

## Alternatives

- A queued cancel or cancel-all accepted `Final`: rejected. A tombstone cancel accepted `Final`
  ends the order Canceled, and a queued cancel of an order that fills first would end a filled
  order Canceled.
- An undocumented error, or -32603, as `RejectKind::Other`: rejected. A placement refused on it
  ends, and if the venue acted it rests untracked; `Unknown` costs a query.
- Mapping message text (the Java library's `ORDER_IS_NOT_OPEN`, `no order parameters changed`)
  to finer kinds: rejected. No page documents those texts as codes, and `Reject` maps on the
  code (`Reject::venue_code`: "reject maps key on this, not on text"); a finer table waits for codes the testnet run (FBC-8xr) records.

## Consequences

- FBC-xvf's codec records each request it sends with `ParadexReplies::sent`, hands it every
  text frame and every deadline, and routes the frames it reports `NotOurs` (the auth and
  subscribe replies) to its own handling. The JSON-RPC ids of its own requests must not collide
  with the rpc ids the encoder writes.
- A post-only order that would cross, a price or size off the venue's rules and too little
  margin all arrive as `RejectKind::Other` (or as a CLOSED order event with its cancel reason),
  so nothing downstream can tell them apart from the reply yet.
- A cancel never ends an order from its reply; the order events (and, failing them, the Unknown
  ladder and resync) do.

## What would show this was wrong

- Paradex documenting finer error codes, or the testnet run (FBC-8xr) recording a code the
  table lacks for a refusal it should map: a new record extends the table.
- A testnet reply to `order.cancel` or `order.cancel_all` that is final (no order can fill
  after it), which would let a queued cancel be `Final`.
- An internal error (-32603) shown to always leave the request undone, which would make it a
  refusal.
