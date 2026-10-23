# 0125 — Instrument specs carry signing parameters beside the native id and a clock-free session calendar that only closes, its weekly closures in wall time under an offset rule stated as data

Status: accepted
Date: 2026-10-23

## Context

Design §4.4 names two `InstrumentSpec` fields, `signing: Option<SigningParams>` and
`sessions: Option<SessionCalendar>`, but defines neither, and FBC-4 left both out until a
signer and a venue with sessions gave them a shape. Hibachi now gives both (FBC-zar, BT-102):

- Its signer packs a contract id (exchange-info `futureContracts[].id`), the underlying's and
  the settlement asset's decimals into every order (the public Java `HibachiPayloadPacker`).
- Its FX perpetuals close from Friday 17:00 to Sunday 17:00 America/New_York, a wall time that
  moves with daylight saving (docs.hibachi.xyz, FX trading). Its exchange-info publishes dated
  maintenance windows, and a temporary market's `marketOpenTimestamp` and
  `marketCloseTimestamp`: an earliest open and a close that never reopens.

A parser reads no clock and gets no `EncodeCtx` (0002, 0035, 0048). A plan's round can be
built from the wall time it is sent at (`HttpPlan::later`, 0048), but a weekly rule a venue
crate expanded into dated windows from then would mean what it means only as of when the spec
was fetched. `fbc-core` keeps its dependencies few, and any new one passes the licence gate
(0017). The same input must give the same answer live and in replay (0006). This record
augments 0003, 0004 and 0035.

## Decision

1. `InstrumentSpec` and `InstrumentSpecDraft` gain `sessions: Option<SessionCalendar>` and
   `signing: Option<SigningParams>`, mandatory like every other field, after `status`;
   `InstrumentResolver::spec` copies both from the draft. Neither type has a `Default` or
   `#[non_exhaustive]`.
2. `SigningParams` is an enum named by the shape of its values, never by a venue (0003). Its
   first variant is `AssetDecimals { underlying, settlement }`: the decimal places the venue
   states; how a signer scales by them is its venue crate's. The instrument's own key, such as
   Hibachi's contract id, stays in `native_id` and is not repeated here (one copy, as 0035 chose
   for the draft's quote and kind). `None` means the signer needs nothing beyond the spec. A new
   signing shape is a new variant under its own record.
3. `SessionCalendar` (`fbc-core`'s `calendar` module) is plain data: an earliest open
   (`first_open`), a close that never reopens (`final_close`), dated `ClosedWindow`s and
   `WeeklyClosure`s. It answers `is_open(at)` for a `WallNs` its caller supplies and reads no
   clock. It only ever closes: `is_open` is false exactly when `at` is before `first_open`, at
   or after `final_close`, inside a window, or inside a weekly occurrence, and whether the
   instrument trades is still its `status` too. `SessionCalendar::new` sorts and deduplicates
   the parts and returns `None` when they close nothing, so an instrument with no timetable has
   one spelling, `sessions: None`. A `first_open` at or after `final_close` is a calendar that
   never opens, accepted rather than refused.
4. A weekly closure is a start and an end in a zone's wall time (`WeekTime`), the end before the
   start wrapping past Sunday 24:00. Its zone is a `UtcOffsetRule`: a standard offset and an
   optional `DstRule` in POSIX `Mm.w.d/time` form (start read in standard time, end in daylight
   time, the fifth week the month's last, each time of day from 00:00 to 23:59:59), evaluated
   in `fbc-core` by integer civil-date arithmetic. The offset in force at an instant is
   standard plus the save when the latest transition at or before it, among those of its year
   and the years either side, is a start, and an end wins a tie. `fbc-core` holds no zone name
   and no time-zone data and reads no host zone; each venue crate states its zone's rule as
   cited data.
5. Each weekly occurrence closes the half-open interval from the first instant the wall clock
   reads its start to the last instant it reads its end; an edge the clock skips takes effect
   at the jump. Across a daylight-saving change this fails closed: an edge in a repeated hour,
   an end at the very wall time the clock falls back to among them, holds the instrument closed
   through both passes, and an occurrence wholly inside one skipped stretch closes nothing that
   week.
6. Neither field is part of what `version` indexes (0004): a spec whose calendar or signing
   decimals change bumps nothing, and books do not resync for it.
7. Nothing in `fbc-runtime` reads the calendar, so a closed session is never a disconnect, a
   feed fault or a liveness alarm.
8. The next close and the next reopening are fixed by 5 (the least instant at or after `at`
   that is closed, or open); the ticket that needs them adds them without a new record.
9. No new dependency.

## Alternatives

- A time-zone crate in `fbc-core` (jiff with its bundled database, or chrono-tz): rejected. A
  new exact pin and licence entry, and either bundled data that goes stale or a host database
  that makes replay depend on the host.
- A time-zone crate in the venue crate, expanding the weekly rule into dated windows: rejected.
  A new pin and licence entry there (0017); its parser gets no `EncodeCtx` (0048), and windows
  anchored on the time a plan is sent are the fixed horizon below, their meaning hanging on when
  the spec was fetched.
- Dated windows only, expanded over a fixed horizon: rejected. A hidden expiry that later fails
  closed, the same arithmetic in every venue crate, and large specs.
- A named-zone enum in `fbc-core` (`UsEastern`): rejected, zone rules in the core.
- A trait object or function pointer for the zone: rejected. Not data, and it loses the derived
  equality and `Debug` a spec has.
- A fixed UTC weekly window: rejected. It is wrong for half the year, and widening it to cover
  both offsets closes an hour of real trading.
- Judging a weekly closure by where the local clock reads at the instant: rejected. It reopens
  the instrument inside a repeated hour.
- Hibachi's contract id inside `SigningParams` beside `native_id`: rejected, two copies that
  could disagree.
- A struct of optional signing values in place of an enum: rejected. A new shape would not make
  its readers change.
- `Some` of an empty calendar as "always open": rejected, two spellings of one fact.
- Refusing a `first_open` at or after `final_close`: rejected. It would fail a whole discovery
  over a market that is truthfully never open.
- The next-close query now: rejected, beyond FBC-zar's done line; its first reader needs both
  edges and adds them.

## Consequences

- Every `InstrumentSpec` and `InstrumentSpecDraft` literal states both fields; the compiler
  lists any that do not. Paradex, Binance USD-M, QFEX and the toys state `None` for both, the
  first two with a source note in their discovery modules.
- Hibachi's crate states New York's rule as a cited const (US law since 2007,
  `EST5EDT,M3.2.0/2,M11.1.0/2`), maps `marketOpenTimestamp` and `marketCloseTimestamp` to
  `first_open` and `final_close` and its published windows to `ClosedWindow`s. Its signer reads
  the contract id from `native_id`, refuses one that is not a number within `u32`, and refuses a
  spec without `AssetDecimals`; it never assumes decimals.
- A calendar is the venue's statement as of `fetched_at`; one re-read later may close more, and
  whatever halts on it never lifts a halt on it.
- A change in a zone's law is a data edit in the venue crate, and a replay of an older day needs
  the spec in force then; decoder replay already takes the caller's `SpecTable`.
- Follow-ups: Paradex's `open_at` and `expiry_at` mapped into a calendar, and SimVenue honouring
  the calendar.

## What would show this was wrong

- A venue timetable that weekly closures and dated windows cannot state, such as a long holiday
  table.
- A zone rule outside this model: a transition at 24:00 or later, which POSIX allows
  (America/Santiago's `M9.1.6/24`), or before midnight, which RFC 9636 allows (America/Nuuk's
  `M3.5.0/-1`).
- A rule whose start can fall after the next year's end, such as one with near-whole-day offsets
  and a late-December start: point 4's latest-transition rule then keeps daylight time where libc
  and zoneinfo, evaluating each year's pair on its own, keep standard time. No real zone does
  this and nothing builds one (Reviewer B on PR #203, RB203-1).
- A replay of an older day disagreeing with what the venue did then because a zone's law
  changed.
- A feed silent through a closed session tripping a liveness alarm.
- A signer needing a per-instrument value that neither `native_id` nor a `SigningParams` shape
  holds.
- Replay disagreeing with live on an `is_open` answer.
