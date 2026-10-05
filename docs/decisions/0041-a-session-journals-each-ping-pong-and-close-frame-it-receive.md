# 0041 — A session journals each ping, pong and close frame it receives at its ingest place, in format version 5

Status: accepted
Date: 2026-10-05

## Context

0023 stamps every message a session receives, a ping, pong or close frame included, so each
keeps its place in ingest order though it yields no event. The session journaled only text and
binary frames (0006), so a control message took an `ingest_seq` no record carried (Codex
r4178802727 on PR #32). A reader could not tell that gap from a lost record: no `Degraded`
marker covers it, and marking one per keepalive would flag every healthy recording as
degraded. FBC-drf fixes it in the format. 0040 let this change share format version 5 with
FBC-q7b while neither had shipped a journal: no release carries version 5 (the latest release,
v0.0.4, writes version 4), so no released reader reads it.

## Decision

This augments 0006, 0023 and 0040 (it supersedes nothing):

1. **A received control message is a record.** `Record::InboundControl` holds the stamp the
   session gave the message and what it carried: a ping's or pong's payload, or a close
   frame's status code and reason (or none, for a close with no body). The session writes it
   at that stamp under Normal, as it does a data frame; a record the sink has no room for is
   dropped and counted, and the gap marked, as any other is.
2. **Its payload and reason are written only as a keyed hash.** No codec is asked about a
   control message (0028's `redact_inbound` names spans in data frames and responses), so,
   as FBC-s69 does for a response no codec is left to ask, the journal writes a ping's or
   pong's payload and a close frame's reason only as their length and the HMAC-SHA-256 of the
   whole under the consumer's key (0024), and the reader returns them as 0028's blank at that
   length. The close code is written plainly. `Debug` shows payload and reason by length only.
3. **Format version 5 holds it.** It is kind 12: the stamp, a byte (0 ping, 1 pong, 2 close
   with no body, 3 close with a body), then the payload, or the code (`u16`) and the reason,
   each written as a `WireSlice` is with one span over all of it (none when it is empty).
   Version 5 is shared with 0040's outbound opcode. A segment of versions 2 to 4 holding kind
   12 is malformed; their segments still read as before.
4. **What the session sends is not journaled as control.** The keepalive pings of 0033 and the
   pongs and close frames the WebSocket layer answers with stay out of the journal, as 0033
   says; they are outputs, not inputs, and take no ingest sequence.
5. **Replay skips it.** A control message reaches no codec live, so `MdReplay` feeds it to
   none; it is there so a reader can account for every ingest sequence.

## Alternatives

- Not stamping control messages: 0023 gives them their place so the ingest order of what
  follows is the order the session read it, and a ping's arrival is evidence of liveness a
  reader may want.
- A `Degraded` marker per control message: every healthy recording would read as degraded.
- Payload and reason verbatim: a venue's close reason is free text a venue may fill with
  what a request carried (an API key it refuses), and no codec vouches for it. A known value
  can still be checked against its hash.
- Version 6: the version mechanism exists so an older reader refuses a newer segment at its
  header. No reader of version 5 has shipped, so a second bump would cost a fixture and a
  version for no reader it protects.

## Consequences

- Every inbound ping, pong and close frame costs a record (a stamp, and a hash when it
  carries a payload or reason); keepalives on a busy feed are a small share of its frames.
- A close reason in the journal reads as blanks: a reader learns its code and length, not
  its text.
- A reader of one session's journal can check that its stamped records' ingest sequences run
  on, each gap lying before a `Degraded` marker, as `crates/fbc-runtime/tests/md_journal.rs`
  does. A silence alarm's stamp is still not journaled (FBC-9z0).
- A consumer that matches `Record` exhaustively handles `InboundControl`.
- The next format change, once a version 5 journal has been recorded, is version 6.

## What would show this was wrong

- A venue whose control frames carry data a replay needs (a ping payload a codec must echo):
  then the codec, not just the journal, needs them, and can name their spans.
- Disconnect diagnosis that needs close reasons the journal cannot show.
- A recording whose control records crowd out data frames under a full journal.
