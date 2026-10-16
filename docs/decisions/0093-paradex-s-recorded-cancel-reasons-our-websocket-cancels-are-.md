# 0093 — Paradex's recorded cancel reasons: our WebSocket cancels are ours, cancel-on-disconnect and an IOC's unfilled rest are named, and a price-band or minimum-size close refuses an IOC, augmenting 0092

Status: accepted
Date: 2026-10-16

## Context

FBC-8xr's done line asks that any capability value a Paradex testnet run contradicted be
corrected by a new record. Three owner-run acceptance runs (2026-10-15 and 2026-10-16,
`fixtures/paradex/exec-recorded/`, summarised in its README) recorded the `cancelReason` of
every closing `OrderEvent` and the `cancel_reason` of every `GET /orders-history` answer.
0092 and the codec's `closed` mapping knew only the reasons another source names
(`fixtures/paradex/exec/README.md`): USER_CANCELED as ours, POST_ONLY_WOULD_CROSS, and
NOT_ENOUGH_MARGIN as the only refusal of an IOC or MARKET order. The runs stated:

- `user_cancel_ws` for our `order.cancel` and `user_cancel_batch_ws` for our
  `order.cancel_batch` (every run): not USER_CANCELED, so our own cancels were decoded
  `Canceled(Venue)`, and a placement we cancelled before its risk check ended would have been
  reported refused.
- `cancel_on_disconnect` for both cancel-on-disconnect cases (dropped socket and graceful
  close; runs 20261015T222347Z and 20261016T120633Z), decoded `Canceled(Venue)`.
- `REMAINING_IOC_CANCEL` for an IOC that expired unfilled (the same two runs): 0092's
  "What would show this was wrong" asked for this reason; 0092 already takes it for expiry.
- `EXCEEDS_PRICE_BAND: order price 115.117 above upper band 97.97398419599999` for an IOC
  priced past the band (run 20261016T120633Z): a refusal of an IOC other than
  NOT_ENOUGH_MARGIN, the case 0092's last paragraph names as the one that would show it wrong.
  0092 took it for expiry, a final acceptance. The reason is a code, then `: ` and a message.
- `ORDER_SIZE_BELOW_MIN` for a GTC batch item accepted provisionally and then closed unfilled
  (run 20261016T115634Z): an asynchronous reject under 0092 already, and a refusal an IOC can
  meet alike.

## Decision

- **Our cancels.** `user_cancel_ws` and `user_cancel_batch_ws` are our own cancels, as
  USER_CANCELED is: a CLOSED order stating one is `Canceled(Requested)` (stream and REST
  alike), and a placement it closes with nothing filled is withdrawn, neither passed nor
  refused (0092's "Neither").
- **Named venue cancels.** `cancel_on_disconnect` is `Canceled(Disconnect)` and
  `REMAINING_IOC_CANCEL` is `Canceled(Unfilled)`; every other unlisted reason stays
  `Canceled(Venue)`.
- **IOC and MARKET refusals.** An IOC or MARKET order closed with nothing filled is refused
  (an `ExecEvent::AsyncReject` of the placement, 0092) for NOT_ENOUGH_MARGIN,
  EXCEEDS_PRICE_BAND or ORDER_SIZE_BELOW_MIN, matched by the reason's code: the text before
  the first `:`, or the whole reason when it has none. The reject's `raw` is the whole reason.
  Any other reason is still expiry.

## Alternatives

- Taking every IOC closed unfilled for any reason but REMAINING_IOC_CANCEL as refused, now
  that the expiry reason is known: not done; one run per case is thin evidence that Paradex
  states it on every expiry, and a wrong refusal and a wrong expiry end the order alike
  (terminal, nothing filled); only the placement's final phase differs.
- Mapping the refusals to finer `RejectKind`s: not done, as 0092 decided; `raw` carries the
  reason.

## Consequences

- `fbc-venue-paradex`'s `closed` and placement phase read the reasons above; tests in
  `tests/exec_codec.rs` and `tests/exec_recorded.rs` (the recorded frames) hold them.
- An order fbc-oms cancelled now ends `Canceled(Requested)` on Paradex, as on the other
  venues; nothing in fbc-oms branches on the cancel reason.

## What would show this was wrong

A run stating another reason for our own cancel (a REST cancel, or `order.cancel_all`), an
expired IOC closed with a reason other than REMAINING_IOC_CANCEL, or an IOC refusal whose
reason is not one of the three codes, which this record would take for expiry.
