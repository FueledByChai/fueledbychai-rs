# 0114 — A wall-clock nonce source issues monotonic microseconds within the caller's window, resumes after the journal's last nonce, and a source's refusal ends the order-entry session

Status: superseded by 0133
Date: 2026-10-19

## Context

Every `NonceSource` the library had was a test counter, enough for Paradex, whose nonces are
`NonceScope::None` (0054), but not for Hibachi, where every place, modify, cancel, cancel-all
and the cancel-on-disconnect arm consumes a nonce. Hibachi's API docs (api-doc.hibachi.xyz,
Signing > Nonce) ask for a millisecond or microsecond Unix timestamp within 15 s of server time,
unique per account, with no monotonic rule; a reuse is refused ("Invalid input: Nonce already
used"). The Java library's Hibachi broker issues `max(wall microseconds, previous + 1)` from an
in-process counter, not persisted. The owner chose, on 2026-10-19, a separate Hibachi account
for the Rust stack, so there is no Java nonce history to stay above (FBC-3bq4). The
order-entry session already journals each nonce it reserves and the context it gives its codec
(0078), and `NonceSource::reserve` could not refuse: a source with no nonce to give could only
return a short block, which the session reports as a defect.

## Decision

1. **The source.** `fbc_runtime::WallClockNonces` issues each block from
   `max(clock microseconds, last + 1)`, increasing by one in item order, over a `WallClock` the
   caller injects (`SystemWallClock` reads the realtime clock). Monotonic microseconds satisfy
   Hibachi's rule conservatively, as the venue's `NonceScope::PerAccountMonotonic` declares.
2. **Restart.** A source is built with no history (`new`) or resumed after the last nonce an
   earlier journal holds for its source number (`resuming`, with `last_nonce` reading it from
   the records), so a restart never reissues a nonce the journal holds, whatever its clock
   reads. A nonce sent while its record was still queued, unwritten when the process died,
   leaves no trace and is not covered here (FBC-ak22). When a journal is set, the session
   writes first the nonce its source resumed after or last reserved
   (`NonceSource::high_water`, `None` by default), so a resumed process that reserves none
   still leaves its journal naming it for the next restart. Nothing else is
   persisted: the session's journal is the record. Records holding a gap (a `Degraded`
   marker) may have lost the last nonce sent, so `last_nonce` refuses them (`JournalGap`), and
   the consumer resumes after a bound of its own, such as its clock plus the window, which the
   source refuses until the clock has moved a window on.
3. **The window is the caller's.** The source refuses, reserving nothing, a block whose last
   nonce would lie further ahead of its clock than the window its caller gives
   (`NonceRefused::OutsideWindow`), since the venue would refuse it; it also refuses a clock
   before the Unix epoch and a block past `u64::MAX`. The window has no default: a consumer
   passes the venue's documented value, and one under a microsecond is refused when the source
   is built.
4. **Refusal ends the session.** `NonceSource` gains `try_reserve`, defaulting to `reserve`,
   so every existing source is unchanged. The order-entry session reserves through it for
   `on_open`, `on_timer`, the resync and every encode; a refusal ends the session with
   `ExecSessionError::NonceRefused`, the call not made and nothing written or journaled for it.
   Replay reads each encode's nonces from its journaled context and consults no clock.

## Alternatives

- Return `Result` from `reserve` itself: every source in the workspace and every consumer's
  would have to change for a refusal only one source can make.
- Report a refused encode `NotSent` and keep the session: a clock that has fallen more than
  the window behind its last nonce refuses every later command too (cancels included), so the
  session would stay connected unable to send; ending it lets the venue's cancel-on-disconnect
  act and the consumer rebuild once its clock is right.
- Persist the counter beside the journal: a second record of the same values, which could
  disagree with the journal after a crash.

## Consequences

`NonceSource` gains `high_water`, defaulting to `None`, so every existing source is unchanged.
`ExecSessionError` and `fbc_core` gain a variant and a type (`NonceRefused`), which a consumer
matching them exhaustively must handle. A consumer resuming after a restart reads its last
journal's records for the source number it gives `ExecSessionConfig::nonce_source`. The source
cannot see a clock running ahead of server time; measuring skew against the venue's time is
the consumer's, or a later ticket's. One process per account still holds (0007's reasoning,
unchanged here), since nonces are unique per account.

## What would show this was wrong

Hibachi refusing a nonce this source issued as already used, or as outside its window while
the host's clock was within the window of the venue's time; or a consumer needing a session to
survive a refused reservation.
