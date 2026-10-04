# 0028 — Codecs name the credentials in inbound frames and HTTP responses, and the journal hashes them in format version 4, augmenting 0014

Status: accepted
Date: 2026-10-04

## Context

0006 journals every inbound frame and HTTP result, and 0009 allows a JWT, cookie or API key in
the journal only as a keyed hash (0024). The venue boundary of 0014 gives outbound bytes
redaction spans (`WireSlice`, `WireUrl`) and request headers a redaction flag, but inbound
bytes reach the runtime with none: a `RawFrame` and an `HttpResponse` mark nothing. So the
journal (FBC-aen, FBC-apz) could only write an authentication response's token, a cookie a
codec does not know by name, a key a frame echoes, or a key a proxy echoes in a response
header name verbatim, or drop the bytes replay needs (Codex r4172367659 on PR #8, r4176373433
on PR #13). Only the venue's codec knows where its credentials sit in what it receives.
FBC-7lm adds that knowledge to the boundary.

## Decision

This augments 0014 (it supersedes nothing there):

1. **The codec names inbound credentials.** `MdCodec` and `ExecCodec` each have a required
   `redact_inbound(&self, Inbound) -> InboundSpans`, where `Inbound` is a frame (`RawFrame`)
   or a response (`HttpTag`, `HttpResponse`). `InboundSpans` holds spans of the frame's bytes
   or the response's body, and response headers by index with a `HeaderMark` (`Value`, or
   `NameAndValue` for a credential in a header's name). The call is pure: it depends on the
   input and the codec's state, changes nothing, and is asked before the input is journaled
   and handed to the codec. It is required, not defaulted, so every codec states its answer;
   a public market-data codec returns `InboundSpans::NONE`.
2. **Spans are checked against their input.** `InboundSpans::check` (and the journal's
   writer) refuses a span that is empty, outside the bytes, out of order or, where the bytes
   are UTF-8, splitting a character (so blanking keeps a text frame text), and a header mark
   that names no header or comes out of order; a frame has no headers to mark.
3. **The journal hashes them in format version 4.** `Record::Inbound` carries its spans and
   `HttpResponseRec` its body spans; `HeaderRec` gains `redact_name`. An inbound frame's
   bytes and a response's body are written as a `WireSlice` is (each span's HMAC-SHA-256
   under the consumer's key, then the bytes outside the spans), and a header is a flag byte
   (0 plain, 1 value secret, 2 name and value secret) then its name and value, a secret one as
   its length and hash. The reader blanks the spans as it does every other span and still
   reads versions 2 and 3, whose inbound bytes and bodies were written whole.
4. **Replay compares modulo the spans.** Replay hands the codec the record as read back, the
   spans blanked, so a codec must decode the same events and effects whatever bytes the spans
   hold (a token it keeps for later requests is kept blanked, and outbound bytes that carry it
   compare modulo their own spans).

The runtime asking `redact_inbound` for each frame and response it journals is FBC-s69, a
follow-up ticket; it is not part of this record's boundary change.

## Alternatives

- A default `redact_inbound` returning nothing: a new exec codec could forget it and journal
  a token verbatim without a compile error; the few market-data codecs pay three lines each.
- Withholding inbound bytes after a credential is sent (what FBC-f3w's session does until this
  lands): replay loses exactly the authentication traffic it needs to decode.
- A side record of spans next to the verbatim inbound record: the verbatim bytes would still
  be written.
- Marking header names with a separate list rather than a per-header mark: two lists to keep
  in step for one header, and a name redacted without its value would leave a credential's
  context readable.
- Spans relative to a codec-parsed structure (a JSON path): the journal would need a parser per
  encoding; byte spans are what `WireSlice` already uses.

## Consequences

- A journal written by this version can hold a session's authentication traffic with no
  credential in it, and replay decodes that traffic.
- Every codec, including each new venue's, implements `redact_inbound`; an exec codec's tests
  should show its credential spans and its decoding of the blanked input.
- Version 4 costs 4 bytes more per inbound frame and response body (the span count) and 32 per
  span; a header's name is written after its flag.
- The runtime's record-size estimates (FBC-f3w) must follow the version 4 layout.
- A version 3 journal reads back unchanged (`fixtures/journal/v3`), its inbound bytes as they
  were written.

## What would show this was wrong

- A venue whose credential cannot be located by a pure function of the bytes and the codec's
  state (one that needs a later message to tell which part was secret): then the journal needs
  a deferred redaction pass, in a new record.
- A codec whose decoding depends on a span's bytes, so a replayed record decodes differently;
  then that codec must keep the value elsewhere or the spans must shrink.
- A credential that turns up in a journal file from inbound bytes a codec marked.
