# 0099 — SimVenue answers a two-phase venue's placement provisionally, then final once its risk window passes or refused by a risk reject the consumer configures, augmenting 0046

Status: accepted
Date: 2026-10-18

## Context

0046's `SimCodec` decoded every acceptance at `AckLevel::Final`, so a SimVenue standing in for
a venue whose `OrderCaps.ack` is `AckModel::TwoPhase { risk_reject_window }` showed neither the
provisional acceptance nor the risk-reject window (Codex r4182678509 on PR #58), and the codec
refused placements for such a venue (FBC-zr1). Paradex is `TwoPhase` with a five-second window
(0054), and SimVenue for Paradex (FBC-2zxk) needs its placements. 0085 fixes what a consumer may
expect of a provisional acceptance: within the window, a final acceptance (`Accepted` at
`AckLevel::Final` for the placement's request and item) or an `ExecEvent::AsyncReject` of the
placement, not both, and never an order update alone. For Paradex the second phase comes from the
order event (0092): OPEN, or CLOSED with a fill, is the final acceptance; CLOSED unfilled for a
reason other than our cancel is the asynchronous reject; our cancel before the check withdraws
the placement with nothing more (0093); a new connection drops every placement awaiting its phase.
The engine reads no clock and models no risk check, so what ends the window must come from the
caps and the consumer.

## Decision

- **Provisional, then final at the window's end.** For a `TwoPhase` venue the engine answers each
  placement it accepts (a single placement, and each accepted item of a batch) `Accepted` at
  `Provisional` when it acts on it, after `to_venue`, matching and resting it as before; once
  `risk_reject_window` has passed since then it answers that request and item's final acceptance,
  naming the order's client and venue ids, whatever the order did meanwhile. A `SinglePhase`
  venue's placements are answered `Final` as before, with nothing after.
- **A risk reject is the consumer's.** `SimEngine::risk_reject(cid, after, refusal)` configures the
  venue's risk check to refuse the next placement naming `cid`, `after` its arrival, as a
  `RiskRefusal` (`PostOnlyWouldCross`, `MinNotional`, `Margin` or `Other`, each an order-ending
  `RejectKind`). Such a placement, unless a rule refuses it at once as it refuses an ordinary one
  (its matching's included: a post-only cross, a level the book does not know, a fill no rate
  charges; Codex r4309341209 on PR #165), is accepted provisionally and held: never matched, never resting, open to a query and a resync; at its
  instant the engine answers an `AsyncReject` of the placement (`OpKind::Place`, our client id and
  its venue id), then the order update that ends it `Rejected`, and no final acceptance. A cancel
  (or cancel-all, or the venue's cancel at a disconnect or a lapsed dead-man timer) before then
  ends it `Canceled` and nothing is refused, as Paradex's withdrawal (0093); at the instant the
  check would have refused it the engine answers that the placement was withdrawn, which reports
  nothing, so every provisional acceptance gets exactly one second-phase answer (Codex
  r4309341215). Refused for a
  `SinglePhase` venue or an `after` past the window (`OutsideRiskWindow`), and for a client id the
  venue's format cannot carry (`ClientId`).
- **Ordered in time.** Each second phase is due at its instant; at one instant the commands that
  arrived by it act first, then the second phases in the order they were scheduled, then a
  dead-man lapse. A close does not stop them: the venue settles a placement whatever the
  connection does.
- **The codec awaits by request and item.** `SimCodec` decodes `Provisional` and `Final` as the
  engine gives them, and from a provisional acceptance awaits its second phase by request and
  item. Like Paradex's codec (0092), it awaits nothing across a reconnect: each open forgets what it
  awaited; a withdrawal ends the wait and reports nothing; and a second phase it does not await
  reports nothing, the order update of a risk
  reject aside, which always reports the order's state. It places for a `TwoPhase` venue; it still
  refuses amends for one (FBC-kbwj), and the engine's own refusals stay synchronous `Rejected`
  outcomes (FBC-fsqn).
- **How the window lines up with Paradex's order event.** Paradex gives the final phase with the
  OPEN event, right after its risk check and well inside the window; SimVenue gives it at the
  window's end, the latest 0085 allows. So a consumer over SimVenue sees each placement provisional
  for the longest it can be, its order resting and filling meanwhile as Paradex's does once OPEN,
  and a risk reject arrives as Paradex's refused phase does: the asynchronous reject, then the order
  update. Two shapes differ: Paradex reports the refused order `Canceled` with its reason, SimVenue
  `Rejected` with its `RiskRefusal`; and Paradex's post-only cross is asynchronous (FBC-fsqn).

## Alternatives

- **Final on the order event, as Paradex does** (an Open order update, then the final acceptance at
  once): rejected for SimVenue; the engine's own order event follows the acceptance at the same
  instant, so the window would never be seen, which is the gap Codex r4182678509 named, and 0085
  does not take an order update for the final phase.
- **Holding every placement unmatched until the window passes**: rejected; Paradex's check takes
  milliseconds against a five-second window, so every simulated order would miss five seconds of
  queue and fills.
- **Risk rejects in `SimConfig`**: not taken; the consumer seldom knows a placement's client id
  before the run, and an engine call configures one as the run goes, like an injected order.
- **A Final the codec does not await as a decode error**: rejected; after a reconnect it is the
  venue's ordinary answer to a placement the codec has dropped.

## Consequences

- FBC-2zxk's SimVenue for Paradex places through the codec; a consumer sees `Provisional` then
  `Final` (or an `AsyncReject`) as it will from Paradex, in the shape 0085's `two_phase_ack` checks.
- Every placement on a two-phase SimVenue schedules one second phase, held until it is due.
- Amends for a two-phase venue and asynchronous refusals of the engine's own rules wait for FBC-kbwj
  and FBC-fsqn.

## What would show this was wrong

A consumer that behaves differently when the final phase comes early, as Paradex's does, from when
it comes at the window's end, so that SimVenue hides a timing bug; or a Paradex testnet run showing
an order resting before its risk check, so that holding a refused placement unmatched is wrong.
