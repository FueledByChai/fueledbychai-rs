# 0101 — A consumer journals its own inputs and annotations as opaque consumer records in the shard's total order, in format version 8, augmenting 0006 and 0090

Status: accepted
Date: 2026-10-18

## Context

0006 has the journal record everything that crosses the shard boundary so that a recorded day
replays through the same code that ran live. Design §4.8 lists, beside the records the runtime
writes, records of the consumer's own: `Control` (operator commands, the inventory budget,
configuration and spec changes), `Decision` (a decide pass's features and desired book) and
`Verdict`. fbc-journal had none of them: its control records are a connection's `Opened` and
`Closed` and a `Subscribe` call, all the runtime's. So a consumer had nowhere to journal an
operator's Start or Stop, a calendar or configuration reload, the hash of the configuration it
ran with, or a summary of what a decide pass chose. Replaying a live or shadow journal could not
reproduce what the consumer decided. FBC-hxkm (story BT-202) adds a home for them. What these
records hold is the consumer's strategy, which never enters this repository (0001). FBC-hvqi,
the consumer-inputs ticket, will stamp inputs into a running shard and hand them back to the
handler in replay; it needs this record first. Release v0.0.6 writes format version 6, and main
has written version 7 since 0090.

## Decision

This augments 0006 and 0090 (it supersedes nothing):

1. **One record kind for whatever the consumer journals.** `Record::Consumer { at, kind, input,
   bytes }`: `at` the consumer's monotonic time for it, `kind` a `u16` the consumer numbers its
   own records by, `input` whether it is an input (something a replay hands back to the
   consumer at its place) or an annotation (something the consumer derived, which a replay
   passes over as it does an outbound frame), and `bytes` its payload.
2. **The payload is opaque.** The library never reads, parses or names what `bytes` hold, and
   no type for them is defined here (0001). They are written verbatim, with no redaction span
   and no keyed hash, so the consumer puts no credential in them (0009); `Debug` shows them by
   length only.
3. **Written through the shard's sink, in its total order, under the class the consumer
   picks.** The consumer offers the record to the same `JournalSink` the runtime records into,
   from the shard's thread between the runtime's own records (fbc-runtime's `Journal` lets the
   consumer borrow the sink between a session's steps). It chooses Normal or Safety as for any
   record: a Safety record (an operator's Stop) is kept from the reserve when Normal records are
   dropped (0006). The reader returns each in place, between the records it was written
   between.
4. **Decoder replay passes over it.** `MdReplay` feeds no consumer record to a codec. One whose
   input flag is false is passed over for good; an input is passed over until FBC-hvqi hands it
   back to the handler at its place.
5. **Format version 8 holds it.** It is kind 14: `at` (`u64`), `kind` (`u16`), the input flag
   as a byte (0 or 1; any other byte is malformed) and the bytes (`u32` length, then the
   bytes). A segment of versions 2 to 7 holding kind 14 is malformed, and a reader of version 7
   refuses a version 8 segment at its header, naming the version
   (`JournalError::UnsupportedVersion`). The reader reads versions 2 to 8;
   `fixtures/journal/v7` holds a version 7 journal that shows it.

## Alternatives

- One record kind per design §4.8 record (`Control`, `Decision`, `Verdict`) with typed fields:
  the fields are the consumer's strategy (features, desired books, model hashes), which 0001
  keeps out of this public repository, and every new consumer record would be a format change.
- Carry consumer records in `Marker::SessionStart`'s opaque header or a reserved timer tag: a
  header is written once per session, not at an input's place, and the timer tag space is the
  codec's (0014), so replay would hand the record to `on_timer`.
- Share format version 7 with 0090's kind: version 7 has not shipped in a release, but main has
  written it since 0090, and a reader built from main before this change would read a kind 14
  record as a damaged segment part way through rather than refuse the segment at its header.
- Redact the payload as a whole, as a control frame's is (0041): replay would hand the consumer
  blanked inputs, which cannot reproduce what it decided; the consumer controls what it writes,
  as no venue controls what a codec receives.
- A consumer-chosen `Stamp` on every record: an annotation has no ingest sequence or connection,
  and a consumer has no ingest clock of its own; the position in the total order is what replay
  follows (design §4.8).

## Consequences

- A consumer can journal its inputs and annotations now, through the sink it already holds, and
  a reader finds them where they happened.
- A consumer that matches `Record` exhaustively handles `Consumer`.
- The next format change, once a version 8 journal exists, is version 9.
- The record carries no ingest sequence. If FBC-hvqi stamps consumer inputs from the shard's
  ingest clock, it decides how that sequence is kept (in the consumer's bytes, or a new record
  through a format change); this record does not settle it.

## What would show this was wrong

- A replay of a live or shadow journal whose decisions differ from the live ones because a
  consumer input could not be placed or handed back from this record.
- A journal found to hold a credential in a consumer record's bytes.
- FBC-hvqi finding that a consumer input needs its ingest sequence on the record for replay to
  hand it back at the same position, forcing another format change.
