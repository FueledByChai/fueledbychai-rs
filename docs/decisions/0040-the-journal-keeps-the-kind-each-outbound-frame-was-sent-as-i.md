# 0040 — The journal keeps the kind each outbound frame was sent as in format version 5, augmenting 0028

Status: accepted
Date: 2026-10-05

## Context

0006 journals every frame a session writes, and 0024 and 0028 write each redaction span only as
its keyed hash, the reader putting the blank byte of 0028 in its place. A session sends an
outbound frame as a WebSocket text frame when its bytes are UTF-8 and as a binary frame
otherwise, but `Record::Outbound` kept only the bytes and spans (Codex r4178646798 on PR #32).
When every byte of a binary frame that is not UTF-8 lies inside a span, the blanked bytes the
reader returns are UTF-8, so a replay that re-derived the kind would send text where the live
session sent binary. This is reachable once outbound frames carry credentials (authenticated
and order-entry sessions). FBC-q7b fixes it in the format.

## Decision

This augments 0028 (it supersedes nothing there):

1. **Format version 5 writes the kind.** `Record::Outbound` carries an `Opcode` (text or
   binary), as `Record::Inbound` already does, written as one byte after the record's `rpc`.
   The session journals the kind it chose for the message it wrote.
2. **A text frame is UTF-8 as sent.** The writer refuses an outbound text frame whose bytes are
   not UTF-8, in its writing pass, whatever room the bytes take. A span may split a character
   of a text frame (a `WireSlice`'s spans are any bytes), so the reader does not require a
   text frame to be UTF-8 once blanked.
3. **Older versions still read.** The reader reads versions 2 to 5. An outbound frame of
   versions 2 to 4 reads back with the kind its blanked bytes imply (`Opcode::of`): binary when
   a byte that is not UTF-8 lies outside its spans or a span splits a character, text
   otherwise. `fixtures/journal/v4` holds a version 4 journal that shows it.

## Alternatives

- Re-deriving the kind from the bytes at replay: the blanked bytes do not say it, which is the
  defect.
- Forcing every span of a text frame onto character boundaries, as inbound spans are (0028
  item 2): `WireSlice` promises no such thing, so a frame a codec may legally send would be
  dropped from the journal as unencodable, leaving a gap where its record belongs.
- Blanking with a byte that is never UTF-8: 0028 chose `2` so a replayed text form parses as
  it did live.
- A separate record beside `Outbound` for the kind: two records to keep in step for one write.

## Consequences

- Each outbound record costs one byte more.
- A replay sends, or compares against, the kind the live session sent.
- A consumer that builds `Record::Outbound` field by field names its kind.
- A version 4 or earlier journal's outbound binary frame whose only bytes that are not UTF-8
  were inside its spans reads back as text; the kind it was sent as is lost there.
- Another format change landing close to this one (FBC-drf) may share version 5 if neither has
  shipped a journal; once one has, the next change is version 6.

## What would show this was wrong

- A venue that sends a frame kind other than text and binary for data (a continuation stream
  the runtime exposes, say): then the opcode needs more values.
- A replayed session whose outbound kinds differ from those its live journal recorded.
