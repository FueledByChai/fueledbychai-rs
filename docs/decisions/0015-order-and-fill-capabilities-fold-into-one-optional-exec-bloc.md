# 0015 — Order and fill capabilities fold into one optional exec block, so a venue cannot claim fills without orders or orders without fills

Status: accepted
Date: 2026-10-03

## Context

0003 makes capabilities data with every field mandatory, and wants each declared value to be a
claim someone can check against a venue document or a recorded fixture. Design §4.5 (kept in
the private consumer's repository, `docs/design/architecture-rev2.md`) gives `VenueCaps` an
`order: Option<OrderCaps>` (`None` for a market-data-only venue) beside a mandatory
`fills: FillCaps`, and FBC-4 built it that way. The Codex review of pull request #7 noticed the
consequence: a market-data-only adapter, such as the Binance USD-M reference feed (0007), must
still declare a `FillSource` and a `VenueFeeSign` for an account feed it never has. Nothing reads
them while `order` is `None` (no exec codec, so no `DecodeScope` fee decode and no planner), but
they are claims no fixture can verify, which is what 0003 set out to avoid. FBC-5bn put three
options to the owner:

- (a) keep the design, and document that a market-data-only venue's `FillCaps` are unused;
- (b) make `fills` an `Option<FillCaps>`, `None` for market data only;
- (c) fold both into `exec: Option<ExecCaps { order: OrderCaps, fills: FillCaps }>`.

The owner chose (c) on 2026-10-03.

## Decision

`VenueCaps` has `exec: Option<ExecCaps>` and no separate `order` or `fills` field, where
`ExecCaps { order: OrderCaps, fills: FillCaps }` has both fields mandatory. `None` states a
market-data-only venue, which then declares no order capability, no fill source and no fee sign.
A venue with order entry declares both halves together. Readers name the paths
`caps.exec.order.*` and `caps.exec.fills.*` where design §4.5 says `caps.order.*` and
`caps.fills.*`; this record amends design §4.5 to that shape, and wins where the design text
differs. Everything else in 0003 stands: no capability type has a `Default` or
`#[non_exhaustive]`, and `exec` itself must be stated (`None` is a declaration, not a default).

`matching` stays a top-level field: it describes the venue's matching engine (speed bump,
self-trade scope), which the consumer's `MicroFacts` and the simulator read from the venue's
documents whether or not this adapter places orders (design §4.5's reader table), so it is a
checkable claim for a market-data-only venue too.

## Alternatives

- (a) Keep the design and document the fill capabilities as unused: rejected. It keeps values
  that no fixture can check in every market-data-only adapter, and nothing stops a later reader
  from trusting them.
- (b) `fills: Option<FillCaps>`: rejected. It also allows `order: Some(..)` with `fills: None`,
  a venue that places orders and reports no fills, and `order: None` with fill claims; both are
  nonsense the type would accept unless a run-time check refused them, which is the kind of rule
  0004's sealed ids and fees moved into the compiler.
- (c) was chosen because the two inconsistent combinations cannot be written at all; the cost
  is one more path segment for every reader and this record.

## Consequences

- A market-data-only adapter writes `exec: None` and nothing about fills or fees.
- "Order capabilities without fill capabilities" and "fill capabilities without an exec block"
  do not compile; `crates/fbc-core/tests/ui_caps/` proves both.
- Every reader of order or fill capabilities (the `DecodeScope` dispatch's fee sign and client-id
  format, the `ExecutionPlanner` and permits in `fbc-oms`, the conformance kit, the toy venue)
  goes through `caps.exec`, and a reader that needs either half first handles `None` as "this
  venue has no exec path".
- The consumer (chaiwala-rs) reads `caps.exec.order.*` and `caps.exec.fills.*`; its design text
  still shows the §4.5 shape until it is revised.

## What would show this was wrong

- A venue that places orders but reports fills only through a separate product or account feed
  this library cannot reach, so its `ExecCaps` needs a `FillCaps` it cannot honestly declare.
- A venue whose fills can be read (for example a read-only account feed) without order entry,
  which the folded block cannot state.
