# 0062 — An order-entry session re-checks each authorization just before encoding it and reports a stale one NotSent(StaleAuthorization), a new reason, augmenting 0057 and 0060

Status: accepted
Date: 2026-10-06

## Context

0057 keeps an `Authorization` queued in `ExecOrders` until the session encodes its command and
marks the point in `ExecSession::send` where fbc-oms's submit-time re-check belongs (PR #87
Reviewer B B7). 0060 defines that check, `Authorization::check_at_submit`, which reads shared
atomics and so runs on the session's thread without the registry. Until this ticket nothing
called it, so a place built before the kill switch went on could still be written (the window
DeepSeek's DS-1/DS-3 on PR #87 named; the owner's answer B to RB-0ga-1 moved it here,
FBC-j5bw). `fbc-core`'s `NotSentReason` had no reason for a refused authorization, and 0058
declined a new reason for a held place because `Disconnected` already says "not now, never
resent".

## Decision

- **Where.** `ExecSession::send` runs `check_at_submit` on every authorized command after the
  epoch and ready checks and the place gate (0058), and before any nonce is reserved or the
  codec is called. A control command is not checked; it carries no authorization.
- **What a refusal reports.** A new `NotSentReason::StaleAuthorization`: the handler's
  `on_submitted` gets `Err(StaleAuthorization)` for its request id, no nonce is reserved and no
  byte is written. The authorization is spent. `fbc-journal`'s `NOT_SENT` table gives it byte 7,
  after the seven reasons before it, so journals already written read the same.
- **Order of refusals.** A command the epoch does not take (not current, or a place or amend
  held by 0058) is `NotSent(Disconnected)` as before, and counted in `unready_refusals`; only one
  the epoch would take is re-checked. Either way it is never resent.

## Alternatives

- Report it `NotSent(Disconnected)` as 0058 does a held place: rejected. A held place may be
  built again unchanged once the epoch is ready; a stale one must be built again from the
  market's state now (it may be Killed or Cancel-only), and the consumer cannot tell the two
  apart without a reason of its own.
- Report it as an existing codec reason (`Unsupported`, `Unencodable`): rejected. Those say the
  venue or the codec cannot take the command, which is false and would mislead the ladder that
  reads them.
- Check before the place gate, so a stale place on an unready epoch reports `StaleAuthorization`:
  not taken. Both outcomes are not sent and never resent; keeping the gate first leaves 0058's
  counter counting every place the epoch held.
- Check in `ExecOrders::submit`: rejected by 0057 (B7). The state can change between queueing and
  the session's turn.

## Consequences

- Proven in fbc-runtime's `tests/exec_authorized.rs` with the conformance toy and authorizations
  issued through `Registry::authorize` only: an authorized place encoded with one reserved
  nonce, batches of three and two items with one nonce per item, and an authorization issued
  before the kill switch, a disarm or a Wind-down refused with nothing reserved or written while
  a cancel issued before the change goes out. The fixture those tests share with
  `tests/exec_held.rs` is `tests/armed_oms/`.
- `fbc-oms`'s recording gateway (`tests/authorization.rs`) reports the same reason.
- Any exhaustive match on `NotSentReason` outside this workspace gains a variant.

## What would show this was wrong

- A place, batch or amend built before a change of its market's state written by a session.
- A consumer that has to tell a stale authorization from another `NotSent` by anything but its
  reason.
