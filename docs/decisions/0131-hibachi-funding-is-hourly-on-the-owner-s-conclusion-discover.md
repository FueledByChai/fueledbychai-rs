# 0131 — Hibachi funding is hourly on the owner's conclusion: discovery states it Known at one hour capped at 125 bps, and the codec carries the streamed eight-hour estimate's exact eighth per hour, amending 0129

Status: accepted
Date: 2026-10-28

## Context

`fbc-core`'s `MdEvent::Funding` carries `rate_e12`, a rate in units of 1e-12 per interval, with
the `interval` it is for, and an instrument's `FundingSpec::Known` states its funding `interval`
and the largest rate per interval, `cap`. FBC-jyyl stated Hibachi's settlement interval as one
hour (`FUNDING_SETTLEMENT_INTERVAL`), from the settled history the owner read on 2026-10-22
(`GET /market/data/funding-rates`: consecutive `fundingTimestamp`s 3600 s apart), against the
redocly trading page's "every 8 hours". What a published rate is a rate for stayed open: the
history's `fundingRate` and the `funding_rate_estimation` topic's `estimatedFundingRate` could
each be a rate for its hour or for eight hours, and a wrong reading misstates funding by a
factor of eight, in `fbc-sim`'s planned funding accrual too (FBC-uki4). So discovery stated
every perpetual's funding `FundingSpec::Unknown`, and 0129's item 9 pushed the estimate
"exactly as the frame carries it in units of 1e-12, `interval: None`, `next: None` (FBC-204c)".

### OWNER

The owner's conclusion, recorded in FBC-204c's notes on 2026-10-28 (given in chat; the owner
noted that Hibachi's UI may show something different from the API): compute funding the way the
public Java library does at b4188a5.

1. The settled `fundingRate` (`GET /market/data/funding-rates`) is a rate per one-hour
   settlement: `market-data-collector` `FundingSources.java` L139-L159 annualizes it over the
   interval derived from consecutive `fundingTimestamp`s, which are 3600 s apart
   (<https://github.com/FueledByChai/FueledByChaiTrading/blob/b4188a57f7d5ea4e82260c9f3c4a0a359ad04c27/market-data-collector/src/main/java/com/fueledbychai/datacollector/FundingSources.java#L139-L159>).
2. The streamed `estimatedFundingRate` (`funding_rate_estimation`) is an eight-hour rate whose
   hourly equivalent is the estimate divided by 8: `HibachiQuoteEngine.java` L914-L918 divides
   it by the ticker's funding interval, and no Hibachi ticker sets another, so 8 applies
   (<https://github.com/FueledByChai/FueledByChaiTrading/blob/b4188a57f7d5ea4e82260c9f3c4a0a359ad04c27/implementations/market-data-api/hibachi-market-data-impl/src/main/java/com/fueledbychai/marketdata/hibachi/HibachiQuoteEngine.java#L914-L918>;
   the descriptor states 8, `HibachiRestApi.java` L388, and L915-L917 fall back to 8 where a
   ticker has none).
3. The payment interval is one hour. `nextFundingTimestamp`'s unit is still unevidenced.

Supporting documentation: docs.hibachi.xyz's Funding page
(<https://docs.hibachi.xyz/hibachi-docs/trading/funding>) says payments are made every hour, with
`Funding Rate = (1/8) × mean(ρ_t)` and `Funding Payment = Q × Funding Rate × S_settlement` at each
hourly settlement. No account id, position, rate or price is recorded (0009).

Two choices the conclusion leaves to this record. The cap: the redocly trading page
(<https://hibachi-docs.redocly.app/docs/trading>, read 2026-10-28) states "Funding Rate Limits:
-10% to 10%" under Limits, naming no interval, and the one funding rate that page defines is
for its eight-hour period ("There are 28,800 seconds in an 8-hour interval"; its Predicted and
Final Funding Rates are over that period). docs.hibachi.xyz's Funding page caps each per-second
premium at ±100% and states no cap on the rate. And the units: an eighth of an estimate in 1e-12
is not always a whole number of 1e-12.

## Decision

`fbc-venue-hibachi` follows the owner's conclusion:

1. **Discovery.** Every perpetual's funding is `FundingSpec::Known { interval:
   FUNDING_SETTLEMENT_INTERVAL (one hour), cap: Some(FUNDING_RATE_CAP) }`, whatever the
   exchange's or the contract's status.
2. **The cap.** The docs' -10% to 10% is read as a limit on the eight-hour rate the page defines,
   the period the streamed estimate is a rate for (`FUNDING_ESTIMATE_PERIOD`), and rescaled to
   the one-hour interval as the estimate is: 1000 bps / 8 = **125 bps per hour**, either way.
3. **The codec.** `funding_rate_estimation`'s `estimatedFundingRate` becomes `MdEvent::Funding`
   with `rate_e12` its exact eighth, the rate per one-hour interval, and `interval:
   Some(FUNDING_SETTLEMENT_INTERVAL)`. This amends 0129's item 9, whose other rules stand.
4. **No rounding.** The estimate is read exactly in units of 1e-12, and its eighth must be a
   whole number of them; an estimate whose eighth is finer (one finer than 1e-12 itself
   included) is refused as malformed, the frame counted among the runtime's refused frames, so
   every rate pushed is exactly an eighth of what the venue stated. Any estimate stated to nine
   decimal places or fewer divides exactly (1e-9 is 1000 units), and Hibachi's example frame
   states six (`-0.000040`).
5. **Capabilities.** `FundingCaps::interval_reported` is `true`: every funding event states the
   one-hour interval its rate is per, which the codec supplies from the owner's conclusion since
   Hibachi's frames carry none. `next_time_reported` stays `false` and `next` stays `None`:
   `nextFundingTimestamp` is not read until a fixture shaped after evidence pins its unit.

## Alternatives

- Read the docs' limit as 10% per hour (a cap of 1000 bps per interval): not done. The page that
  states the limit defines no hourly rate; its only funding rate is the eight-hour one, the same
  period the owner's conclusion gives the streamed estimate. If the venue's settled rates show
  otherwise, this record is reopened (below).
- State no cap (`cap: None`): not done. The docs state a limit, and a cap per interval derived
  from it with its rescaling named is more useful to a consumer than silence; `None` means "the
  venue sets no cap", which the docs contradict.
- Round the eighth to the nearest 1e-12 (half to even) and push it, or count it apart: not done.
  The error would be below 1e-12 an hour, but the event would then carry a value the venue never
  implied, and this codec already refuses a rate the event's units cannot hold rather than round
  it (0129's item 9). At the six decimals of Hibachi's example frame, and at up to nine, every
  estimate divides exactly; a refusal would show the venue publishing finer rates.
- Keep `interval_reported: false` because Hibachi's frames carry no interval: not done. The
  capability describes what a consumer receives, and every event carries `Some(one hour)`;
  declaring `false` beside events that always state an interval would make the caps disagree
  with the stream. `fbc-core`'s field text says a funding message "states the funding interval"
  and `MdEvent::Funding::interval` is the one "the venue states"; this record reads both of the
  event, the venue's evidenced schedule stating the interval where its frames carry none, not
  of the frame alone. `fbc-core` does not yet say which it means, and nothing in the library
  reads the flag. Paradex declares `false` and its events carry `None`, which agrees with
  either reading.
- Push the estimate unscaled with `interval: Some(8 hours)`: not done. The payment interval is
  one hour, and `FundingSpec::Known`'s interval and the event's must be the same interval for a
  consumer (or `fbc-sim`'s accrual, FBC-uki4) to apply a rate at each payment.

## Consequences

- A consumer and `fbc-sim` can accrue Hibachi funding: each hour's payment is position × the
  event's rate × the settlement price, with no factor of eight left to guess.
- The settled history's `fundingRate` needs no rescaling, but nothing in this crate decodes it
  yet; a ticket that does states it per its one-hour settlement.
- A rate beyond 125 bps per hour is not refused: the cap is declared, not enforced, as every
  venue's `FundingSpec` cap is.
- `FUNDING_ESTIMATE_PERIOD` (eight hours), `FUNDING_RATE_CAP` (125 bps) and
  `FUNDING_SETTLEMENT_INTERVAL` (one hour) are public constants of the crate, each citing its
  evidence; the codec divides each estimate by the period over the interval, 8, checked at
  compile time to be a whole number.
- Proof: `crates/venues/fbc-venue-hibachi/tests/funding.rs` (the interval from the settled
  history's spacing, the eight-hour period, the cap rescaled, discovery's `Known` for every
  contract), `tests/discover.rs`, `tests/md_topics.rs` (both shapes as their exact eighth per
  hour with the interval, an indivisible estimate refused, `nextFundingTimestamp` never read)
  and `tests/factory.rs` (the caps), on the hand-built SYNTHETIC `fixtures/hibachi/md/funding_*`
  frames and `fixtures/hibachi/rest/`.

## What would show this was wrong

Hourly funding payments on a Hibachi account that are an eighth of, or eight times, position ×
that hour's settled `fundingRate` × its index price (the settled rate then not per hour);
payments that track eight times the event's rate rather than the event's rate (the estimate
then not an eight-hour rate); payments every eight hours rather than every hour; a settled rate,
or a streamed estimate's eighth, beyond 1.25% an hour in magnitude (the limit then not per eight
hours); or Hibachi stating the limit's period or the estimate's period otherwise. Any of them is
a new record superseding this one.
