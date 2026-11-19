# 0152 — An authorization dropped before a gateway takes it over releases what its build reserved, and only its handed form is checked at submit, amending 0057, 0060, 0082 and 0084

Status: accepted
Date: 2026-11-17

## Context

- **The stranding one step later.** 0084 releases a held place whose command is dropped before
  it is authorized. Once `Registry::authorize` issues the authorization, the command is spent
  (0082 point 5): the place is no longer held, and its outcomes apply by client id. Before this
  record nothing held it after that. An authorization dropped before a gateway encoded it or
  reported anything of it left its place PendingNew and counted against both caps until restart.
  Nothing reported an outcome for it. The Unknown ladder never escalates it, since it was never
  sent. Its cancel waited for an acknowledgement that never came. Under a small resting cap per
  side, one such order blocks its side (FBC-eeb4).
- **The amend half (FBC-45et, which FBC-eeb4 absorbed).** `Live::amend` marks the order's amend
  built: it counts as resting, and the order gets no further Live permit, until
  `Registry::amend_sent` reports it sent or `Registry::amend_not_submitted` takes its command
  back. An authorized amend's command is spent, so an amend authorized and then dropped, or
  refused before a gateway gave it a request id, could never be released. Its order stayed
  counted at the larger total, and the execution planner held its level until the order ended.
- **Where the workspace itself drops one.** It is not only a consumer's early return:
  - `ExecOrders::submit` drops an authorization it refuses for another account or an ended
    session;
  - a session's end drops the commands still waiting, unreported (0057);
  - a session whose nonce source fails after it took a command ends with nothing reported
    (0057), and the shard host hands back only the ladder command it kept (0100);
  - the shard host hands unspent authorizations back through `Decide::unsubmitted`, whose
    default drops them, and through `Ladder::push`'s refusal (0100), and fbc-oms had no call
    to release them;
  - the glue's `Link::submit` drops one on any refusal.
- **Why a drop cannot simply release.** A command reaches a venue only through its
  authorization (0045), so an authorization, unlike a held command, may have been written. A
  release on drop is fail-open if a gateway can write a command and then drop its
  authorization unspent: the order would end not sent while the venue holds it.

## Decision

1. **An authorization holds what its build reserved.** A place's or a batch's authorization
   holds its orders, as its command did (0084), and an amend's holds its build. It holds them
   until a gateway takes it over (point 2). Dropped before then, wherever and on whatever
   thread, it hands them back to the registry that issued it. At its next mutating call (0084's
   list) the registry releases them:
   - each order of the place or batch ends `NotSent(StaleAuthorization)` and stops counting,
     while nothing was reported for it since. Nothing reported means: PendingNew, with no venue
     id, no send recorded, no command in flight, no cancel sent, no fill and no Unknown ladder;
   - the amend's build is withdrawn while it is still the build its order holds, so the order
     no longer counts it and gets a Live permit again.
   A read before that call still shows them held, as in 0084. An order something was reported
   for keeps what it counts, which fails closed: a gateway reports only what it took over.
   `Registry::withdraw(auth)` releases at once, and is true when it released anything. A cancel,
   a cancel-many and an instrument cancel-all hold nothing. Another registry's authorization
   releases nothing in this registry: withdrawn there, it is dropped, so its own registry
   releases it.
2. **The hand-over gates the check at submit.** A gateway takes the authorization over with
   `Authorization::hand_over`, which gives a `Handed`. It does so before it checks it at submit,
   encodes it or reports anything of it. `Handed` alone runs 0060's check at submit. It cannot
   be built or cloned outside fbc-oms, and its drop releases nothing. `Authorization` has no
   check (`tests/ui_authorization/check_before_hand_over.rs`). 0060 makes the check mandatory
   immediately before encoding. So a gateway that keeps 0060 encodes only a handed
   authorization's command, and never writes one while the authorization could still release
   its orders. What the gateway reports of a handed authorization, sent or not sent, settles its
   orders, as before.
   The hand-over gates the check, not the encode. `Authorization` keeps a read-only `command()`
   giving the whole `VenueCommand`, since the consumer, the glue and the tests read its kind,
   client ids, prices and sizes. A `VenueCommand` is an ordinary fbc-core value that any codec
   encodes and anyone can build, so a narrower view would not close this: a view carrying what
   a consumer reads carries the command, and a gateway can encode a command it built itself. A
   gateway that encodes from `Authorization::command()`, skipping the check, and then drops the
   authorization releases a written order. That is fail-open: the venue holds an order that
   counts against no cap. In this workspace the one gateway that writes, the order-entry
   session, takes each authorization over before its first refusal and encodes only the handed
   form's command (point 4); every other `OrderGateway` is a test double.
3. **One explicit way back.** `Handed::unencoded` gives the authorization back, holding again.
   A gateway calls it only when it took the authorization over and then encoded nothing of it and
   reports nothing of it. The order-entry session calls it in one place: when its nonce source
   fails before its codec sees the command (0057). It then keeps the command for the shard host
   to hand back unspent (0100), or else drops it, and either way the place is released. It is
   never called once any of the command reached a codec, or once an outcome was reported for it.
4. **In the order-entry session.**
   - `ExecSession::send` takes each submitted authorization over at its turn, before the gate
     (`Disconnected`), the check at submit (`StaleAuthorization`), the in-flight bound
     (`RateBudget`), the encode and every report.
   - `SubmitRefusal::Backpressure` takes the authorization over before it returns. The refusal
     is reported under its request id and settled as 0105 says.
   - Refused for another account or an ended session, or still waiting when the session ends,
     an authorization is dropped unspent, and its registry releases it.
5. **What this amends.**
   - 0082 point 5: once an authorization is issued, the command is spent and the gateway's
     outcome applies by client id, but the authorization holds the orders until a gateway takes
     it over.
   - 0084: its release on drop covers the authorization as well as the command, and its
     consequence that a test keeps a place counted by authorizing it now reads: by authorizing
     it and handing it over.
   - 0057's consequence that the consumer holds each submission not reported unresolved: a
     submission still waiting at a session's end, or given back unencoded on its nonce
     failure, is released. Only one reported sent and not answered stays unresolved until the
     venue is read again.
   - 0060: its check is unchanged in what it checks, but it is `Handed::check_at_submit`, run
     on the handed form, and no longer `Authorization::check_at_submit()`.

## What the owner decides

FBC-eeb4 asked for a structural hand-over, or else the owner's choice between release on drop
and an explicit `Registry::withdraw` alone. The hand-over is structural for the check at
submit, not for the encode (point 2). Approving this record takes release on drop with that
residual: a written order is released only by a gateway that breaks 0060, writing a command it
did not check, or breaks point 3, calling `Handed::unencoded` after an encode. The other choice
is the first alternative below: only `Registry::withdraw` releases, which fails closed but
leaves the runtime's own drops stranded until restart, as before this record.

## Alternatives

- **Only an explicit `Registry::withdraw`; a dropped authorization keeps its orders:**
  rejected. It fails closed, but it leaves every stranding path above to each consumer. The
  runtime's own drops (a refusal, a session's end) happen where no registry is held, so they
  could not withdraw.
- **Let the Unknown ladder escalate an issued place never sent:** rejected. It fails closed but
  is slow, and it has no send time to start from. It would query the order and send tombstone
  cancels for orders the venue never had, and an amend's build would stay held meanwhile.
- **Release on drop with no hand-over:** rejected. A gateway that writes the command and then
  drops its authorization would release an order the venue holds, which is fail-open.
- **No way back from the handed form:** rejected. The session's nonce failure takes the command
  over before its codec sees it. The shard host then could not hand its kept ladder command back
  unspent (0100; Codex P1 r4309350872 on PR #164), and the place would stay stranded.
- **Report every dropped submission not sent from the session instead:** rejected. It changes
  0057's rule that nothing is reported once the session ends, and it still leaves a consumer's
  own drops stranded.
- **A panicking `Drop`, or a compile-time bar on dropping:** rejected, as in 0084.

## Consequences

- An authorization a consumer loses, or that the runtime refuses or leaves unsent at a
  session's end, costs nothing but the authorization: its side is free again by the registry's
  next mutating call.
- **The cost.** Every gateway calls `hand_over` before its check at submit. This is one call,
  and the compiler enforces it, since only `Handed` has the check. The compiler does not enforce
  that a gateway checks before it writes (point 2). A gateway that writes from
  `Authorization::command()` and skips the check already breaks 0060, and can now also release
  a written order on its drop. `Handed::unencoded` is an explicit escape that a gateway could
  misuse the same way, so its one caller is the session's nonce failure. An authorization grows
  by its hold (one pointer) and its registry's instance (a `u64`).
- Tests and fixtures that kept a place or an amend counted by authorizing it and dropping the
  authorization now hand it over, as a gateway takes it. A planner pass's commands dropped
  unsent are released by the next pass.
- **Unchanged.** An amend's `PermittedCommand` dropped before it is authorized still keeps its
  build. `Registry::amend_not_submitted` remains the way to release one never authorized.
- The glue's `Link::submit` drops an authorization it refuses, which is now released.
  `Link::submit` does not settle `SubmitRefusal::Backpressure`, and a backpressured place stays
  until it is settled (FBC-sckz).

## What would show this was wrong

- An order a gateway wrote ending `NotSent(StaleAuthorization)` because its authorization was
  dropped or given back.
- An authorization dropped before any hand-over whose place or amend build still counts after
  the registry's next mutating call.
- A gateway or test that needs to check an authorization at submit without taking it over.
