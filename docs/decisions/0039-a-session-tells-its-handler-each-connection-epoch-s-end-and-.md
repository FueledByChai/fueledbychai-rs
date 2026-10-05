# 0039 — A session tells its handler each connection epoch's end, and the runtime's books invalidate every book that epoch fed until its next snapshot, augmenting 0023

Status: accepted
Date: 2026-10-05

## Context

FBC-nij's `MdBooks` keeps one `fbc-book` book per (instrument, `BookId`), fed through the
`BookKeeper` handler. 0023 has a session hand its handler envelopes only, and a session opens
its next epoch without saying the last one ended. So when a connection drops, or a codec asks
for a reconnect (Paradex resyncs a `seq_no` gap that way, 0022), the books that connection fed
keep their last levels and read as `Valid` until the new epoch's snapshot ends: the trading
book shows levels the venue may have changed meanwhile, and the deltas lost with the
connection are never reported as a gap (Codex r4179162283 on pull request #33). The runtime
knows the epoch ended; FBC-crh makes the books learn it too, and the way a session tells its
handler is the ticket's to decide.

## Decision

- **The signal.** `MdHandler` gains `on_epoch_end(&mut self, key: ConnKey)`, by default
  nothing, as 0031 added `on_tick_to_wire`. A session calls it once per epoch, as it journals
  that epoch's `Closed` (0006), whatever ended it: a drop, a reconnect its codec asked for, a
  rotation or silence (0033), a stalled write (0036), an error, or a stop. It comes after every
  event of that epoch the handler is given and before any event of the next, so it takes its
  place in ingest order without a stamp of its own. `MdVenue`'s shared handler forwards it.
- **Replay.** `MdReplay` calls it at every `Closed` record of its connection, and at an
  `Opened` that finds an epoch still open (its `Closed` was dropped), after the open epoch's
  held results are answered, so books built from a replay are invalidated where the live ones
  were.
- **The books.** `MdBooks::apply` takes the connection epoch (`ConnKey`) each event came from
  and remembers, per book, the epoch of the last event about it. `MdBooks::end_epoch(key)`
  invalidates, as a gap (`fbc-book`'s `gap`: levels and any snapshot in progress gone, `Gapped`
  until the next complete snapshot), every book whose last event came from `key` or an earlier
  epoch of the same connection; a book another connection feeds, or one a later epoch of the
  same connection already fed, is untouched. `BookKeeper` does that on `on_epoch_end`, then
  calls the consumer's `BookHandler::on_epoch_end(key, &books)` (by default nothing), so the
  consumer learns its trading book became invalid without waiting for the next event.

## Alternatives

- The session pushes a `Health { feed: Feed::Book(id), h: Gap }` per book subscription when an
  epoch ends, as it reports a silent stream stale (0033): rejected. A subscription is not a
  book (a feed may build no book, a book may outlive its subscription), the events would be
  the runtime's inventions in the codec's stream, and the handler still could not tell an
  epoch's end from a codec's gap.
- A new `MdEvent` variant for the end of an epoch: rejected. `MdEvent` is the venue boundary's
  contract (0014); an epoch's end is the runtime's, not a venue's, and every codec-facing match
  would grow an arm it can never produce.
- Invalidating every book on any epoch's end: rejected. One venue's sessions share one
  handler (0027), so a drop of one endpoint would blank the books another endpoint still feeds.
- Leaving the books valid until the codec reports a gap: rejected. A drop loses deltas no codec
  sees, which is the failure FBC-crh exists to fix.

## Consequences

- A consumer that implements `MdHandler` itself gets the end of each epoch; one that keeps its
  own books must invalidate them there, as `BookKeeper` does.
- `MdBooks::apply` now takes the event's `ConnKey`; a direct caller passes the stamp's `conn`.
- A rotation (0033) also invalidates the books its epoch fed until the next epoch's snapshot,
  though the venue did not lose a delta: a planned close is still the end of what that
  connection said.
- Stopping a session ends its epoch: its books are invalid afterwards.

## What would show this was wrong

- A venue whose books survive a reconnect (a sequence that continues across connections, so
  no new snapshot is sent), leaving its books gapped after every reconnect.
- A consumer that needs to know which books an epoch's end invalidated, rather than reading
  the books' states after it.
