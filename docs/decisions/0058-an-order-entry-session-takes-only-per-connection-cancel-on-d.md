# 0058 — An order-entry session takes only per-connection cancel-on-disconnect, arms it and resyncs on every epoch, and places or amends nothing until both are done, augmenting 0013, 0053 and 0057

Status: accepted
Date: 2026-10-06

## Context

FBC-w19 builds 0013 rule 1's "a reconnect or a resync never re-places or resends an order by
itself" into the order-entry session (0053, 0057): every resting order is protected by the
venue's cancel-on-disconnect before the first order goes out on a connection, and every new
epoch re-reads the venue before it adds anything. 0053 left the codec's `resync` uncalled and
0057 left places ungated on it, both for this ticket. The runtime reads
`caps.exec.order.cancel_on_disconnect` (0015), which is `None`, `PerConnection {
rearm_on_reconnect }` or `DeadMan { max_ttl }`. Paradex declares per-connection protection
re-armed on every connection (0054). The owner's default of 2026-10-04: a venue without
cancel-on-disconnect may not place orders, refused at session start until revisited for
Hibachi. No planned venue needs a dead-man timer.

## Decision

- **Only per-connection protection.** `ExecSession::new` refuses a venue that declares
  cancel-on-disconnect `None` or `DeadMan` with `ExecSessionError::CancelOnDisconnect`, naming
  what it declares; nothing is connected.
- **The arm, once authenticated, every epoch.** When the codec reports an epoch's stream
  `Authenticated`, the session sends `ArmCancelOnDisconnect(true)`, a Safety command, as a
  request of its own: its id from the account's `RpcIds`, one nonce reserved, encoded and
  checked as a submitted command is (0057), its frames charged together, its deadline in the
  RPC table. It arms on every epoch where `rearm_on_reconnect` holds; where it does not, until
  one arm is accepted. The arm's events reach the handler as any other.
- **The resync, once authenticated, every epoch.** Right after the arm, the session calls the
  codec's `resync` once for the epoch, with exactly the nonces `nonces_for(CtxCall::Resync)`
  asks for, its frames charged together as `on_open`'s are: buckets that refuse them for now
  end the epoch as a drop, opened again no sooner than they would admit them, and frames that
  never fit end the session (`ExecSessionError::ResyncNeverFits`).
- **No place or amend until both.** Until the venue has accepted the epoch's arm and the
  `ResyncEnd` of the resync the session asked for on the epoch has been handed to the handler
  (so fbc-oms has applied it), every place, batch of places and amend taken on the epoch is
  `NotSent(Disconnected)`, counted (`ExecCounters::unready_refusals`), with no nonce reserved
  and nothing written. Cancels, cancel-manys, instrument cancel-alls and control commands are
  not held. `ExecOrders::may_place` says whether the current epoch takes places; it is true
  already while the handler is handed the event that opens the epoch, and a place it submits
  then goes out only once it has returned.
- **Only a final acceptance arms.** A two-phase venue's provisional acceptance of the arm
  (`AckLevel::Provisional`) leaves it pending and does not clear its deadline: the venue's
  final acceptance accepts it, and a rejection or the deadline fails it.
- **A failed arm ends the epoch.** An arm the codec refuses, the buckets do not admit, the venue
  rejects, or that is unanswered at its deadline (whatever the codec reports for it) leaves the
  epoch refusing places and amends. Once the input being handled and the commands waiting are
  taken, so a cancel submitted meanwhile still goes out, the epoch ends as a drop, counted
  (`ExecCounters::arm_failures`), and the next opens through `ReconnectPacing`.
- **Nothing written again.** A reconnect re-sends nothing (0057): the next epoch writes its own
  authentication, a new arm under a new request id, and its own resync.
- **The conformance toy** declares cancel-on-disconnect `PerConnection { rearm_on_reconnect:
  true }`, writes an arm as `cod|rpc=<n>|on=<0|1>`, and refuses a dead-man refresh as
  `NotSent(Unsupported)`, as its caps now say.

## Alternatives

- Send the arm through the consumer's `ExecOrders` as a `ControlCommand`: rejected. Whether an
  epoch is protected is the session's to know before it takes any place, and a consumer that
  forgot to arm would place unprotected.
- Wait for the arm's acceptance before asking the resync: rejected. Both must be done before a
  place; asking them together shortens the time an epoch cannot place, and the resync is a read.
- Refuse a held place as a new `NotSentReason`: not taken here. `Disconnected` already tells
  the consumer that the stream cannot take it now and that it is never resent; a new reason
  would change fbc-core's enum every match names. The counter tells the two apart.
- Keep `DeadMan` venues, refreshing the timer from the session: rejected for now. No planned
  venue needs it, and the refresh schedule is a decision of its own.
- End the epoch at once on a failed arm, dropping the commands waiting: rejected. A cancel the
  consumer submitted on hearing the failure is the protection the arm would have given.

## Consequences

- No order-affecting command can reach a session today (0057: nothing outside fbc-oms issues an
  `Authorization` until FBC-afd), so the session's refusal of a place and an amend, and its
  letting a cancel through, are proven by a unit test inside fbc-runtime that queues those
  commands as a submission would (`exec_held_tests.rs`), on the gate itself (`exec_gate.rs`'s
  tests) and, in `tests/`, with control commands, which take the same path as a cancel.
- A resync the venue never ends leaves its epoch connected and refusing places with no
  deadline; FBC-nwpr gives it one.
- The handler hears the session's own arm's outcome, an `ExecEvent::Outcome` for a request it
  never submitted; a consumer ignores outcomes for request ids it does not hold.
- Every order-entry codec answers `ArmCancelOnDisconnect(true)` with an outcome naming its
  request, and implements `resync`.
- Tests that authenticate a toy session have the venue accept the arm and answer the resync
  before they rely on anything else being written.

## What would show this was wrong

- A place or amend written on an epoch before its arm was accepted and its `ResyncEnd` reached
  the handler, or a cancel held by the gate.
- An order resting through a disconnect that the venue did not cancel, on a venue whose
  protection was armed.
- A venue the owner wants to trade that offers no per-connection protection (Hibachi), which
  would need its own record before this one's refusal is lifted.
