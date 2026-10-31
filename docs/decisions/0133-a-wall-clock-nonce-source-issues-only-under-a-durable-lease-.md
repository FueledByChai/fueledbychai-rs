# 0133 — A wall-clock nonce source issues only under a durable lease renewed ahead of use, and a restart resumes after its ceiling

Status: accepted, supersedes 0114
Date: 2026-10-30

## Context

0114's item 2 resumed a restarted `WallClockNonces` after the last nonce its journal holds. The
journal's sink never holds up a send (0006), so the session sends a request while that request's
`Nonce` record is still queued. A process that dies before the writer thread drains the queue
leaves a journal holding neither that nonce nor a `Degraded` marker, and `last_nonce` reads it
as complete. A restart whose clock reads at or behind the lost nonce (a restart within
microseconds, or a clock stepped back) then reissued it, and the venue refuses a reused nonce;
the request could be a cancel (Codex P2 r4321890282 on PR #187, FBC-ak22). The high-water
record the session writes when its journal is set (RB187-1) goes through the same queue and has
the same gap. Resuming after the clock plus the window does not close it: a clock stepped back
further than the window still reissues the lost nonce. Having the session wait for the journal
to write a nonce's record before sending would make a cancel wait on a disk write, which 0006
forbids. Hibachi order entry (FBC-x7bi) and its cancel-on-disconnect probe (FBC-xvtr) wait on
this.

## Decision

1. **The lease (this replaces 0114's item 2).** A `WallClockNonces` is built over a
   `NonceLease`: a `LeaseStore` and a span, both the consumer's and required, the span from a
   microsecond to the window. The source issues a nonce only at or below a ceiling the store has
   already persisted (`LeaseStore::persist` has returned). Its first ceiling, a span past its
   clock or past the nonce it resumes after, whichever is later, is persisted on the thread that
   builds it, before it issues anything; a store that cannot persist it builds no source
   (`NonceStartError::Lease`). After that a writer thread of the source's own persists the next
   ceiling, a span past the later of the clock and the last nonce, once half a span or less is
   left. It checks four times a span, and at once when a reservation finds half a span or less
   left. No reservation calls the store or waits for the writer. A reservation whose block would
   pass the ceiling persisted so far is refused, `NonceRefused::PastLease`, reserving nothing,
   and the session ends with that refusal as 0114's item 4 says. Once the source is dropped, the
   writer reads no clock and starts no write; the drop does not wait for a write under way.
2. **Restart.** A source resumes after the larger of the ceiling its store holds and, when the
   consumer passes one (`resuming`), the last nonce its journal holds (`last_nonce`, unchanged).
   Every nonce an earlier process sent lies at or below the ceiling it had persisted, including a
   nonce whose journal record was never written, so the restart reissues none of them whatever
   its clock reads. A clock more than the window behind that ceiling is refused
   (`OutsideWindow`) until it has moved on. Records holding a gap (`JournalGap`) no longer need
   a bound of the consumer's own: the consumer resumes over the lease alone (`new`). When a
   journal is set the session still writes the source's high-water first
   (`NonceSource::high_water`, from 0114's item 2), which is now the nonce the source resumed
   after, at or above the lease's ceiling.
3. **The lease file.** `LeaseFile` is the production store: one decimal ceiling in a file at a
   path the consumer gives, one file per nonce sequence (per account on Hibachi). Each persist
   writes a file beside it, syncs it, renames it over the lease file and, on Unix, syncs the
   directory, so a crash leaves either the old ceiling or the new one. `LeaseFile::create` is
   for a sequence that has never issued a nonce and refuses a path that exists.
   `LeaseFile::open` refuses a missing file or one that holds no ceiling, so a lost lease is
   never read as a fresh one. One process holds a lease file, as one process holds an account
   (0007's reasoning, as 0114 kept it).
4. **What stays in force from 0114.** Item 1 (the source issues `max(clock, last + 1)` over an
   injected `WallClock`), item 3 (the window is the caller's, a block outside it is refused, and
   a window under a microsecond is refused when the source is built) and item 4 (`try_reserve`;
   a refusal ends the session; replay reads nonces from journaled contexts and consults no
   clock) stand. Item 4's refusals now include `PastLease`. 0114's alternatives and consequences
   stand except where this record changes them, as its third alternative below shows.

## Alternatives

- Resume after the clock plus the window: a clock stepped back further than the window from the
  lost nonce still reissues it.
- Have the session wait until the journal has written a nonce's record before sending its
  request: a cancel would wait on a disk write (0006).
- Persist the counter beside the journal, the alternative 0114 rejected because the two could
  disagree after a crash. A ceiling ahead of use is not that counter: it is written about twice
  a span rather than once a nonce, and a restart takes the larger of the ceiling and the
  journal's last nonce, so the two never have to agree.
- Renew the lease on the reserving thread when it runs low: a cancel arriving then would wait on
  the write.
- Persist only on a clean shutdown: a crash, the case at issue, leaves nothing.

## Consequences

Building a `WallClockNonces` now takes a `NonceLease`, persists once, starts a thread and needs
a `Clone` clock. Its constructors return `NonceStartError`, which replaces `ZeroWindow`, and
`NonceRefused` gains `PastLease`, which an exhaustive match must handle. Each source writes its
lease about twice a span. A consumer keeps one lease file per nonce sequence beside its journal,
creates it once for a new account and opens it on every later start. A lost lease file puts the
consumer back where 0114 left it: resuming after a bound of its own, which this record does not
cover. A restart's first nonce can lie up to a span ahead of its clock, so a span well inside
the window leaves a quick restart room to issue at once. A disk that stalls for longer than a
quarter span lets the clock reach the ceiling. Reservations are then refused and the session
ends, leaving the venue's cancel-on-disconnect to act, rather than issuing a nonce a restart
could reuse; `WallClockNonces::lease_failure` reports a write that failed.

## What would show this was wrong

Hibachi refusing, after a restart, a nonce this source issued as already used; a session ended
by `PastLease` while its disk was healthy; or the lease's writes measurably delaying order
entry.
