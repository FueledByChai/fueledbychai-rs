# 0059 — Paradex funding reaches the account only as each fill's realized funding; the funding_payments channel is not decoded into FundingPaid

Status: accepted
Date: 2026-10-06

## Context

FBC-4zz decodes Paradex's private `PositionEvent` and `AccountEvent` (SBE templates 22 and 23)
and had to decide, from the schema and the WebSocket pages, whether Paradex funding payments
are decoded into `ExecEvent::FundingPaid` from a private channel or reach the account only as
each fill's realized funding (`FillEvent.realized_funding`, decoded by FBC-uiy; 0054 declares
`FillCaps::realized_funding`).

What the sources show:

- The schema (`paradex_1_0.xml` at the paradex-py commit 0022 cites) has no funding-payment
  message. Its private templates are `OrderEvent` (20), `FillEvent` (21), `PositionEvent` (22)
  and `AccountEvent` (23); `FundingDataEvent` (5) is the public funding rate. `FillEvent`
  carries `realizedFunding` ("Realized funding PnL from this fill"); `PositionEvent` carries
  `unrealizedFundingPnl` and `cachedFundingIndex`.
- Paradex's "Binary Encoding (SBE)" page lists the channel of each template and says
  "Channels not listed here are delivered as JSON regardless of the negotiated encoding".
  `funding_payments.{market_symbol}` is not listed: it is a JSON channel.
- That channel's page (AsyncAPI `FundingPaymentsMarketSymbolSubscribe`) states each payment's
  `id`, `market`, `payment` ("Payment amount in settlement asset"), `index`, `created_at` and
  `fill_id`: "Fill id that triggered the payment (if any)".
- The `positions` page: `unrealized_funding_pnl` is the "Unrealized running funding P&L for
  the position", and `realized_positional_funding_pnl` is "Realized Funding PnL for the
  position. Reset to 0 when position is closed or flipped". Funding accrues on the position and
  is realized when a fill changes it.

So every payment that names a fill is the same money as that fill's `realizedFunding`, already
on the fill. The page's "(if any)" leaves open whether a payment can come with no fill, and no
Paradex page says when one would.

## Decision

Paradex funding reaches the account only as each fill's realized funding. The Paradex codec
does not subscribe to `funding_payments.{market}` and pushes no `ExecEvent::FundingPaid`; the
running funding in `PositionEvent.unrealizedFundingPnl` is not read (it is not yet P&L, and the
fill that realizes it carries it). Whether a payment can arrive without a fill is FBC-8xr's to
record on its testnet run.

## Alternatives

- Decode `funding_payments.{market}` into `FundingPaid` as well: every payment naming a fill
  would be counted twice, once on the fill and once as a payment, unless the OMS matched
  payments to fills by fill id. That matching is a model of Paradex's funding the library does
  not need while fills carry it, and the channel is JSON, a second decoder on the SBE socket.
- Decode only the payments with no fill id into `FundingPaid`: nothing shows such a payment
  exists, so this would be a decoder for a case no source describes, tested only on invented
  frames.
- Compute funding from `PositionEvent`'s running funding or funding index: the library never
  computes a value the venue did not send (the signs section of `fbc_core::event`); the venue
  sends the realized amount on the fill.

## Consequences

- A consumer's funding P&L on Paradex is the sum of its fills' `realized_funding`; there is no
  separate funding stream to reconcile it with.
- Funding that has accrued on an open position is not in the account's realized P&L until a
  fill realizes it; the balance (`AccountEvent.accountValue`, which includes unrealized P&L)
  already reflects it.
- The Paradex order-entry codec subscribes to no JSON channel for funding.

## What would show this was wrong

FBC-8xr's testnet run, or any later record, showing a funding payment with no fill id (on
`funding_payments.{market}` or in `GET /funding/payments`), or a payment naming a fill whose
amount differs from that fill's `realizedFunding`. Either means funding reaches the account
outside fills, and a new record decides how it is decoded.
