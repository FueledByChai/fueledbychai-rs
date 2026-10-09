# 0089 — mixed_batch judges a refused and an unanswered batch item as the setup declares its venue reports them, augmenting 0083

Status: accepted
Date: 2026-10-08

## Context

0083 has `mixed_batch` answer a batch of three placements accepted, refused and unanswered, and
require the refused item `Rejected` and the unanswered one `Unknown` only once its deadline has
passed. That is how the conformance toy reports them: it refuses an item with a code and
answers each item on its own record, so a reply to some items says nothing of the rest. 0069
has Paradex's codec report both differently: an `order.create_batch` item's `error` is a message
with no code, so it is `Unknown`, not a refusal; and a batch is answered in one `results` list,
so a reply holding fewer results than items is `Unknown` for the rest in the same decode call.
A codec conforming to 0069 therefore failed `mixed_batch` on both items (Codex r4223435597 on
PR #129; FBC-3pv6), and FBC-6oj runs the suite on Paradex.

## Decision

- `OrderEntryStub` gains `batch: BatchFailures`, the adapter's statement of how its codec
  reports a batch item the venue refuses (`RefusedItem::Rejected` or `RefusedItem::Unknown`) and
  one the venue's reply leaves unanswered (`UnansweredItem::AtDeadline` or
  `UnansweredItem::InReply`). There is no default: every setup states both.
- `mixed_batch` judges by it. The refused item is reported once, as declared. Under
  `AtDeadline` the unanswered item has no outcome before the check first moves the clock and is
  `Unknown` once after (0083 unchanged); under `InReply` it is `Unknown` exactly once by the time
  the stub's script has played (the reply read, the clock not yet moved), and nothing more is
  reported for it once the clock has run on by the longest deadline; the refused item, too, has
  its outcome by then, since the reply answers the batch whole (Codex r4227105351). Everything else 0083 judges
  (the accepted item, no outcome for the whole request, client ids, no venue id on the
  unanswered item, nothing resent) is judged alike under every declaration.
- The unanswered item stays the batch's last, so a venue answering a batch in one list, in item
  order, leaves it out by sending a shorter list.
- The toy declares `Rejected` and `AtDeadline`.

## Alternatives

- Skipping `mixed_batch` by name where the venue's batch protocol cannot answer an item
  silently: rejected; Paradex can (a shorter `results` list), and a skip would leave its batch
  decoding unchecked before mainnet.
- Declaring it in `OrderCaps`: rejected; the runtime and fbc-oms do not need it (both read an
  `Unknown` alike, whenever it comes), so it would be a capability no consumer reads. It is how
  the stub's answers are to be read, which is what `OrderEntryStub` states.
- Accepting either form under one rule (`Rejected` or `Unknown`; at the deadline or in the
  reply): rejected; a codec mapping a coded refusal to `Unknown`, or reporting a lost item early,
  would pass unseen.

## Consequences

- Every `OrderEntryStub` states `batch`; FBC-6oj's Paradex setup states `Unknown` and `InReply`.
- A venue reporting an unanswered item `Unknown` in its reply cannot also be checked for an
  early timeout of that item; `unknown_on_timeout` still checks a single request's deadline.

## What would show this was wrong

A venue whose batch protocol mixes the forms (some refusals coded, some not; a reply sometimes
whole, sometimes split across frames), which a single declaration cannot state.
