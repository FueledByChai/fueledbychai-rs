# 0032 — Amends and batch cancels declare the order references they can name, and a codec refuses a request with none of them

Status: accepted
Date: 2026-10-04

## Context

0003 makes capabilities data with every field mandatory, and grows the model with a field and
a record when a behaviour cannot be expressed. `OrderCaps` said which references a single
cancel (`cancel_refs`) and a query (`query_refs`) can name, but not an amend or a batch
cancel. A venue whose amend takes the venue's order id only cannot amend an order it has not
acknowledged, and nothing in the caps said so (Codex r4173187111 on pull request #8, deferred
from FBC-5 to FBC-6c9). Paradex's batch cancel (`order.cancel_batch`) takes order ids only,
while its single cancel also takes a client id with the market; `batch_cancel` was a `Batch`
with only `max_items`, shared with batched placement, so the caps could not say that either.
FBC-7lx, FBC-lrc and FBC-olg (Paradex order entry) need both before Paradex declares
`ExecCaps`.

## Decision

`AmendCaps` gains a mandatory `refs: TagSet<RefKind>`: the references an amend can name.
`OrderCaps::batch_cancel` becomes `Option<CancelBatch>`, a type of its own with `max_items`
and a mandatory `refs: TagSet<RefKind>`: the references each item of a batch cancel can name,
which may be narrower than `cancel_refs`. `Batch` stays for batched placement.

One rule chooses the reference a request carries: `AmendOrder::reference(&AmendCaps)` and
`CancelOrder::reference(declared)` return a `ChosenRef` (venue id, client id or placement
nonce), the first declared kind, in `RefKind` order, that the command carries, or `None`. A
codec writes the chosen reference and nothing else (0014 item 5's `AmendRef`/`CancelRef`); on
`None` it refuses the command as `NotSent(Unsupported)` with no effect pushed, and a batch
cancel with one such item is refused whole. The OMS planner, when it is built, reads the same
answer from the caps before it builds a command, and defers the operation or chooses another
(a single cancel by client id, or waiting for the acknowledgement) rather than sending it. An
amend carries no placement nonce, so `PlacementNonce` in `AmendCaps::refs` matches nothing.

The venues in the repository today declare `exec: None` (Binance USD-M and Paradex market
data), so no venue literal changes; the synthetic fixture in `crates/fbc-core/tests/common/`
declares both fields, and `tests/ui_caps/` proves a literal without either does not compile.

## Alternatives

- A `refs` field on `Batch`, shared by placement and cancel batches: rejected. A placement
  names no existing order, so the field would be a value with no meaning on half its uses.
- A separate tag type for amend references (venue and client only): rejected for now. It
  would make the placement-nonce case unrepresentable, but adds a second tag type beside
  `RefKind` for one impossible value, which the chooser already ignores.
- Splitting a batch cancel whose items need different references inside the codec: rejected.
  A codec does not plan; splitting or deferring is the planner's choice, made from the caps.

## Consequences

- Every adapter that declares an amend or a batch cancel states its references, with a
  citation (0003); the compiler lists each literal.
- Codecs share one reference rule, so a codec, the planner and the conformance kit's
  `caps_truthful` agree on what is sendable.
- A batch cancel of orders not yet acknowledged, on a venue whose batch cancel takes venue
  ids only, is refused rather than sent with a wrong or empty id.

## What would show this was wrong

- A venue whose amend or batch cancel takes a reference by preference that the
  first-declared-kind rule would not pick (for example a venue that rejects a venue id it has
  not yet propagated, where the client id would succeed).
- A planner that needs per-item reference choices inside one batch that one declared set per
  batch cannot express.
