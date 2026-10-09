# 0084 — A held place is never stranded: dropping its command releases it, and no outcome of it applies, amending 0082

Status: accepted
Date: 2026-10-08

## Context

0082 holds a place's or batch item's command from its build until an authorization is issued
from it, and withdraws such a place only with its command (`Registry::place_not_submitted`).
Reviewer B on PR #133 (FBC-0d9k), filed as FBC-657c, found two gaps:

- RB133-2: a held place whose `PermittedCommand` is dropped (an early return, `let _ =`, passed
  to `amend_not_submitted`, refused by another registry) can never be released. A not-sent
  outcome is refused while it is held; the Unknown ladder never escalates it, since it was never
  sent; its cancel waits for an acknowledgement that never comes. It stays PendingNew and keeps
  its cap headroom until restart. Under the owner's $11 per-side resting cap one such order
  blocks its side, which the testnet quoting loop (FBC-elg7) would hit.
- RB133-1: `Registry::on_outcome` refused only not-sent and refused outcomes for a held place.
  Accepted and unknown outcomes by client id still applied, and the held command stayed
  authorizable, so a misreporting consumer could send an order the ladder had already resolved.

## Decision

1. **A dropped place command releases its orders.** A place's or a batch's command holds its
   orders. Dropped while it still holds them, neither authorized nor withdrawn, wherever it was
   dropped and on whatever thread, it hands them back to the registry that built it. The
   registry ends each one still held `NotSent(StaleAuthorization)` at the start of its next
   mutating call (every build, event, outcome, authorization, ladder tick, resync, arming call,
   planner pass, and the builders `with_lease_keys` and `for_testnet_run`), freeing what it
   counted. A read of the registry before that call (`get`, `resting_on`) still shows the order
   held. This amends 0082's point 1: a command refused for another registry no longer keeps its
   reservation held there; it is released when the command is dropped.
2. **No outcome of a held place applies.** `Registry::on_outcome` refuses every placement
   outcome for a held place with `OmsError::NotIssued`. Only an authorization reaches a gateway
   (0045), so nothing of it was sent. A not-sent or refused outcome changes nothing, as in
   0082's point 5: the command can still be authorized or withdrawn. An accepted or unknown
   outcome says the venue may hold an order that was never authorized, so it voids the command:
   the order ends `NotSent(StaleAuthorization)`, freeing what it counted, and its command, or
   its batch's, is refused at authorization (`IssueRefusal::Released`). It is never sent on top
   of whatever the report claims.

## Alternatives

- Make the drop of an unspent place command a panic or a compile-time error: rejected. A
  panicking `Drop` turns a consumer's early return into a crash of the process that holds the
  orders, and Rust cannot make a value impossible to drop.
- Let the ladder escalate a held place: rejected. It was never sent, so the ladder would query
  and tombstone-cancel an order the venue cannot hold, and the command would stay authorizable
  meanwhile.
- Release a dropped command's orders through shared per-order flags that every read checks,
  so reads before the next mutating call are exact: rejected for now. Each count and state read
  would consult the flag, and every consumer pass already starts with a mutating call.
- Leave an accepted or unknown report's order counted after voiding its command: rejected. With
  no command and no send time nothing would ever release it, which is RB133-2 again.

## Consequences

- A consumer that loses a held place command loses nothing but the command: its side is free
  again by the next call.
- Tests and fixtures that kept a place counted by building it and dropping its command now
  authorize it, as a gateway would be handed it.
- `PermittedCommand` carries a shared handle to its registry's list of dropped places; commands
  still compare equal by what they command.

## What would show this was wrong

- A place that reached a gateway ending `NotSent` because its command was dropped.
- A held place, dropped or voided, still counting against a cap after the registry's next
  mutating call, or authorized after an accepted or unknown report.
