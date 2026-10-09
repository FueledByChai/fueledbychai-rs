# 0085 — The reconnect, two-phase and reject-table conformance checks run on the order-entry harness, a final acceptance being an outcome at AckLevel Final, augmenting 0083

Status: accepted
Date: 2026-10-08

## Context

FBC-y6y adds the last three order-entry checks BT-502's suite names (design §6):
`resync_after_reconnect` (a reconnect leads to a resync and to no order re-placed),
`two_phase_ack` (on a `TwoPhase` venue a provisional acceptance is followed by a final one or an
asynchronous reject within the risk window; on a `SinglePhase` venue no provisional acceptance
appears) and `reject_coverage` (every venue code in the fixture's reject table maps to a
`RejectKind`, `Other` only where the table says so; design §6 step 9 keys the map on the code).
The ticket has them run over the stub server and fbc-runtime's order-entry session as 0083's
checks do. Three things 0083 does not fix: how the stub forces a reconnect and answers the new
connection, what counts as the final phase of a two-phase acceptance (fbc-oms opens an order on
an acceptance at either level, and Paradex's placement reply is provisional with no later
outcome, 0069), and how the fixture states a kind with a payload.

## Decision

- **A reconnect is the stub closing the connection.** `Live::run_epochs` plays one connection
  per epoch: the opening, the requests, a barrier, then a close when another epoch follows. The
  session reconnects as its pacing says, on the clock the check moves (`Ctx::played` moves it
  until the stub's script has ended). Every epoch's opening is answered alike, by the setup's
  `OrderEntryStub::opening`: its resync shows the account flat, since the venue's
  cancel-on-disconnect cancelled what rested on the closed connection.
- **`resync_after_reconnect`** places one order the stub accepts, then the stub closes the
  connection and answers the next one's opening. It fails where the placement is not accepted
  once as `OrderCaps.ack` has it, where no resync ends (`ExecEvent::ResyncEnd`) on a later
  connection epoch than the placement's answer came on (the epoch the runtime stamps on each
  event's envelope, so a stray resync end on the first connection does not count), where the stub's script does not play to its end, and where
  the order is written more than once over both connections, counted as 0083 counts a request
  written again.
- **A final acceptance is `SubmitOutcome::Accepted` at `AckLevel::Final` for the placement's
  request; an asynchronous reject is `ExecEvent::AsyncReject` of a placement naming the order**,
  by our client id or a venue id the placement's outcomes name. An order update showing the
  order open is not taken for either: a venue's event may report an order received before its
  risk check. `two_phase_ack` places one order the stub accepts. On `SinglePhase` no outcome of
  any request (the arm's included) is provisional, and the placement is accepted once, final. On
  `TwoPhase` the placement is accepted once as the model has it; where provisionally, the check
  moves the clock no further than `risk_reject_window` once the stub has answered, and by then
  the final acceptance or the asynchronous reject has come after the provisional acceptance,
  not both. A venue accepting final at once
  waits for nothing, and the check says so.
- **The reject table is a fixture file**, `<fixtures>/reject_coverage/table.txt`: a code, then
  the kind as `RejectKind`'s `Debug` spells it, so a kind with a payload
  (`RateLimited { retry_after: None }`, `AlreadyTerminal(Unspecified)`) needs no parser.
  `reject_coverage` places one order per code, in the table's order, the stub refusing each under
  its code (`Answer::RejectCode`, so `Answer` is `Clone`, no longer `Copy`). Each placement's
  outcome is one `Rejected` of its one item (naming no other order's client id) or of the whole
  request, carrying the code as
  `Reject::venue_code` and exactly the table's kind. A table missing, listing no code, or with a
  line naming no kind fails. The check's registry has room for as many orders as the table has
  codes (`Live::orders`).

## Alternatives

- Forcing the reconnect from the check through the session (a control command): rejected; no
  such command exists, and a venue's own close is the reconnect the Java stack failed on.
- An `Open` order update as the final phase: rejected for the reason above; a venue whose codec
  reports no final outcome reports one, or its record says why the check cannot apply.
- A `RejectKind` name parser with payload syntax of the suite's own: rejected; the `Debug`
  spelling is the type's own and changes only with it.
- A stub-driven risk reject (`Answer` asking the venue to refuse after a provisional acceptance):
  not built; the ticket asks that the provisional acceptance be followed by either, and the
  toy's two-phase variants in the tests show both pass.

## Consequences

- Paradex's codec answers a placement `Accepted { ack: Provisional }` with no later outcome
  (0069), so Paradex fails `two_phase_ack` until its codec reports the final phase (a final
  outcome once the order event shows the risk check passed, or an `AsyncReject` when it did not);
  FBC-6oj, which has Paradex pass the suite, meets this.
- An adapter's setup answers `Answer::RejectCode` in its own protocol, and its fixtures list the
  codes it maps (its table from the venue's documents, 0069 for Paradex).
- A check that waits out a rate limit's window may let a keepalive fire, as 0083 records; the
  reconnect moves the clock only by the pacing delay, in 100 ms steps.

## What would show this was wrong

A venue whose two-phase acceptance is final only by an order event and cannot report a final
outcome; a venue that answers the opening of a reconnect differently from the first (a resync
showing orders the close did not cancel); a reject code a venue sends only to an operation other
than a placement, which this check cannot drive.
