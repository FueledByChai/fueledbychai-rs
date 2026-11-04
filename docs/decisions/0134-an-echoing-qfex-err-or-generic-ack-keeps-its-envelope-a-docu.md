# 0134 — An echoing QFEX err or generic ack keeps its envelope, a documented error code, a bucketed echo type and a documented retry hint in clear, so replay decodes it as live, and a request's err leaves the connection open, narrowing 0126 and 0132

Status: accepted
Date: 2026-11-04

## Context

0126 asks two things of QFEX's order-entry codec that cannot both hold under 0028.

- The codec emits a budget-only `Effect::RateLimited` "with the parsed duration and an
  operation only when the echo names a known bucket". The bucket comes from the echo's `type`,
  and the duration from the free-text `message`.
- 0126's last sentence says: "The entire inbound frame is redacted when it can echo a request."
- 0028 item 4, and the contract of fbc-core's `ExecCodec::redact_inbound`, hand replay the
  journaled frame with its spans blanked (decoder replay, 0006). So the codec must decode the
  same events and effects from the blanked frame as from the live one.
- The journal fills a span with the byte `2` (0028 item 5). A frame redacted whole therefore
  replays as a run of `2`s of the same length. serde_json reads up to 308 of them as a number
  and refuses a longer run.
- So replay sees neither the bucket nor the hint. It cannot even see that the frame was an
  `err`, or which. 0028 names the way out: a codec whose decoding depends on a span's bytes
  "must keep the value elsewhere or the spans must shrink".

What main holds:

- `crates/venues/fbc-venue-qfex/src/rate.rs` takes the bucket from `incoming_message.type` and
  the hint from `message`. It reads the flat envelope only, and the seconds form only.
- Its `echoed_frame_spans` puts one span over any frame that names `incoming_message`, after
  JSON escapes are decoded.
- Nothing on main decodes a private frame yet. Draft PR #196 does, and FBC-zeb0 lands its codec.
  - On that branch live holds the budget, while replay reconnects on the blanked frame.
  - It also ends the epoch on every frame it does not recognise once authenticated, so every
    `err` but a `RateLimited` one ends it.
- The same rule blanks the generic `ack` that FBC-zeb0 (d) must recognise. Replay would then
  end an epoch that live kept open.
- 0132 item 3 narrows 0126 only in the arm's window, and keeps every other echoing frame whole.
  Even in that window it redacts an `err`'s `message` and the echo's `type` as spans.

What QFEX documents (read 2026-11-04):

- The [rate page](https://docs.qfex.com/websocket/rate) names two per-user buckets, general and
  cancel, and shows a flat frame:

  ```json
  {"type":"Err","error_code":"RateLimited",
   "message":"Rate limit exceeded, retry after N seconds","incoming_message":{}}
  ```

- The [errors page](https://docs.qfex.com/websocket/errors) and the
  [trade socket's AsyncAPI](https://docs.qfex.com/websocket/trade.yaml) nest an err as
  `{"err":{...}}`.
  - Only `error_code` is required. `message` is optional and may be null.
  - `incoming_message` is the request's echo "when available", an object in the request's
    `{type, params}` form. The AsyncAPI also allows a string.
  - The envelope example is a `RateLimited` err whose message carries no hint. Its echo is an
    `add_order` with its params.
  - The `RateLimited` example's message is `Too many requests. Please retry after 200ms.`, and
    it echoes `{"type":"cancel_all_orders","params":{}}`.
  - Another example echoes a masked `to_api_key`. An echo carries the request's params, and
    0126 already treats it as untrusted and possibly holding credentials.
- The error codes. The errors page's table and the AsyncAPI's `ErrResponse` enum list the same
  ten: `RateLimited`, `InvalidJSONFormat`, `AlreadyAuthenticated`, `InvalidParameter`,
  `PermissionDenied`, `ServerError`, `InvalidOrder`, `InvalidOrderId`, `KycRequired` and
  `TncRequired`.
  - The page says they "map 1:1 to the server enum", without listing the server's enum.
  - Its example of `AlreadyAuthenticated` echoes `{"type":"auth","params":{}}`.
  - The page recommends no reconnect for any code, and says nothing of the server closing the
    connection on an error.
- [Authentication](https://docs.qfex.com/websocket/channels/trade/authenticate) documents only
  its success reply, and the AsyncAPI's authentication reply has no failure variant. Neither
  names a code for a failed authentication. The authenticate page says the server closes a
  connection that has not authenticated within one minute.
- The AsyncAPI's `ack` is `{"ack":{"incoming_message":{...}}}`, with an object echo. No page
  ties it to a command (0132).
- So a hint exists only in `message`, in two text forms, and a bucket only in the echo's `type`.

FBC-qyqw put three options to the owner:

- **A.** Narrow 0126's last sentence for a `RateLimited` err and the generic ack, keeping the
  bucket and the hint in clear.
- **B.** Keep only the envelope, member names and `error_code` in clear, and hold every frame
  budget for at least the declared window on every refusal.
- **C.** Keep 0126 as it is, and accept that replay diverges from live.

On 2026-11-04 the owner chose A, in the words "A: keep bucket in clear". Option A reads:

- Keep in clear, in either envelope (flat and nested `{"err":{...}}`): the envelope, member
  names and nesting; `error_code`; the echo's `type`, only when it is exactly a request type the
  codec maps to a bucket (never `auth`); and `message`, only when it is exactly one of the two
  documented hint forms.
- Redact as spans: every other scalar in `incoming_message` (every `params` value), a string
  echo, any other message, and the value of every undocumented member.
- Keep redacted whole: an echo of `auth`, a frame with JSON escapes or a repeated key, and a
  malformed frame.
- The codec decides only from the bytes it keeps in clear.

Drafting this record raised what live does with an echoing `err` that replay cannot read. On
2026-11-04 the owner was asked: "widen 0134 so every QFEX error keeps its envelope and error code
readable, while the echoed request stays redacted. A request error is then just counted, and the
connection stays open. Error codes that mean the session itself is broken can still reconnect.
The request it answered still ends Unknown at its deadline and is never resent, because QFEX
sends no request id to match it. Error codes carry no credentials." The owner answered, in chat:
"Yes, broaden it".

This record writes both choices down. It also settles points the owner's words leave open, and
each item that does so says so:

- which error codes mean the session itself is broken (item 5);
- what an undocumented code does (item 5);
- what live does with an echoing frame that still stays redacted whole (item 6);
- whether a `RateLimited` err echoing the arm keeps its hint (item 7);
- where a span sits inside a value (item 4).

## Decision

1. **Which frames.** This record governs the inbound frames of QFEX's order-entry codec on the
   trade socket (FBC-zeb0 onward) that can echo a request, as 0126 means it: frames that name
   `incoming_message`, after JSON escapes are decoded. Three envelopes are documented.
   - **The flat err.** A top-level object whose `type` is the string `Err`, with `error_code`,
     `message` and `incoming_message` beside it.
   - **The nested err.** A top-level object whose `err` is an object holding `error_code`,
     `message` and `incoming_message`.
   - **The ack.** A top-level object whose `ack` is an object holding `incoming_message`.

   An err is a flat or nested err, whatever its code. A RateLimited err is an err whose
   `error_code` is the string `RateLimited`. A documented code is one of the ten the errors page
   and the AsyncAPI list. The echo is the `incoming_message` member at the place its envelope
   gives it.
2. **What stays redacted whole.** A frame whose echo equals the arm stays as 0132 item 3 keeps
   it (item 7). Outside the arm's window, any other frame stays one span, as 0126 says, when any
   of these holds:
   - it is malformed: not one JSON text in UTF-8, not an object, or matching none of the three
     envelopes or more than one;
   - its bytes hold a backslash, that is, any JSON escape;
   - an object anywhere in it repeats a member name;
   - its echo is of authentication: an object whose `type` is the string `auth`;
   - blanking the spans items 3 and 4 give would leave bytes that do not parse as JSON with the
     same members and nesting. A number token long enough that its run of `2`s overflows is one
     such case.
3. **What a kept frame holds in clear.** An err or an ack whose echo is not the arm, and which
   item 2 does not keep whole, is journaled with these bytes in clear, and with nothing else in
   clear.
   - **Outside every value.** Braces, brackets, colons, commas and whitespace, and every member
     name at every depth. These are the envelope, its member names and its nesting.
   - **The flat err's `type`.** Its value, `"Err"`.
   - **An err's `error_code`.** Its value is kept only when it is a documented code: on
     2026-11-04, the ten listed in Context. The list is the codec's, cited to the errors page and
     the AsyncAPI, and a code QFEX adds is a span until the codec's list includes it.
   - **The echo's `type`, in each envelope.** Its value is kept only when it is exactly a request
     type the codec maps to an operation, and so to a bucket. On main these are the fourteen
     that `rate.rs` maps: `add_order`, `modify_order`, `modify_stop_order`, `get_order`,
     `get_user_orders`, `get_user_trades`, `get_user_leverage`,
     `get_available_leverage_levels`, `subscribe`, `unsubscribe`, `set_user_leverage`,
     `cancel_order`, `cancel_stop_order` and `cancel_all_orders`. The list follows the codec's
     mapping, but never includes `auth`.
   - **A RateLimited err's `message`.** Its value is kept only when the bytes between its quotes
     are exactly one of the two documented forms, with N one or more ASCII digits:
     - `Rate limit exceeded, retry after N seconds`, from the rate page;
     - `Too many requests. Please retry after Nms.`, from the errors page.
4. **Spans.** Every other value in a kept frame is a span of its own.
   - **What is spanned.**
     - Every scalar in the echo: every `params` value, and a `type` item 3 does not keep.
     - A string echo, and an echo that is a scalar.
     - An `error_code` that is not a documented code.
     - Every err's `message`, except a RateLimited err's in one of the two forms. That includes
       a null message.
     - Every value of a member its envelope does not document, at any depth.
   - **Where a span sits.**
     - A string's span covers the bytes between its quotes, and the quotes stay in clear. So the
       string replays as a string of `2`s of the same length. An empty string has no bytes to
       span.
     - A number, `true`, `false` or `null` is spanned whole, and replays as a run of `2`s.
     - An object or array value keeps its member names and nesting. Its scalars are spanned in
       the same way.
     - An echo that is `{}` holds nothing to span.
   - **0028's checks hold.** 0028 item 2 refuses an empty span and a span that splits a
     character, and no span here is either.
   - **Replay classifies the frame as live did.** A blanked `error_code`, `type` or `message` is
     never one that item 3 keeps. A blanked frame gains no backslash and no member name. So the
     replayed frame falls under the same item as the live one, and its spans are the same.
   - **Beyond the owner's words.** The owner chose which values are spans, not where a span sits
     inside a value. Spanning a string's quotes too would turn a long string into a run of
     digits that serde_json refuses. A frame live decoded would then fail in replay, so this
     record places the span inside the quotes.
5. **What the codec does with an err.** The codec decides only from the bytes the journal keeps
   in clear, live as in replay, and reads a spanned value as absent. No err settles a request,
   and no err makes the codec resend one (0005, 0132 item 8). This holds for every err, whether
   or not it echoes; an err that names no `incoming_message` is outside 0126's rule and this
   record's redaction, and the codec answers it from what the journal keeps of it.
   - **A RateLimited err** gives one budget-only `Effect::RateLimited` (0126), in every state of
     the codec.
     - Its operation comes from the echo's `type` when that is kept. Otherwise it has none, and
       every frame budget is held.
     - Its duration comes from a kept `message`: N seconds or N milliseconds. Otherwise it has
       none, and the hold lasts at least the declared window.
     - A kept N of zero, or one too large for the codec's duration, is an invalid hint (0126).
     - It asks for no reconnect.
   - **Before the stream is authenticated.** Until the codec reports the stream authenticated,
     every other err ends the epoch with `Effect::Reconnect` of the trade stream, that epoch
     only. This is FBC-zeb0's authentication failure, and FBC-bsou's refusal of a subscription
     before its confirmation. Such an err may answer the session's own authentication or
     subscriptions, without which the session cannot run.
   - **A request's err.** Once the stream is authenticated, an err whose code is documented and
     is none of the four below settles nothing. The epoch stays open, and the codec counts the
     err by its `error_code`, which it exposes for the consumer and the probe. These codes are
     `InvalidJSONFormat`, `InvalidParameter`, `ServerError`, `InvalidOrder` and
     `InvalidOrderId`. The request it answered stays as it is: unanswered, `Unknown` at its
     deadline (0057), and never resent (0005). QFEX sends no request id to match it with
     (0132).
   - **The session's err.** Once the stream is authenticated, these documented codes end the
     epoch with `Effect::Reconnect` of the trade stream, that epoch only. The codec counts them
     too, and settles nothing. Each says the session's own standing is wrong, not one request.
     This list goes further than the owner's words, which named no code.
     - **`AlreadyAuthenticated`.** The errors page: "Do not re-auth on an already authenticated
       socket." The codec authenticates once per epoch, so this code shows that the venue's view
       of the connection is not the codec's.
     - **`PermissionDenied`.** The errors page: the API key or user lacks permission for the
       action. This is an authorization failure. Every later command on the account may be
       refused alike, a cancel included.
     - **`KycRequired`.** The errors page: the user must complete KYC before this action. The
       account cannot act until its user does something outside the session.
     - **`TncRequired`.** The errors page: the user must agree to the latest Terms and
       Conditions. The account cannot act until its user does something outside the session.
   - **An undocumented code.** It is spanned, so the codec cannot tell what it means. It ends the
     epoch, like the four above, and is counted as undocumented. This choice is the conservative
     one, and goes further than the owner's words.
     - QFEX documents no code for a failed authentication or an expired session. A code it does
       not list may therefore mean the session itself is broken.
     - Ending the epoch is the side that authenticates again, arms again and resyncs (0058), and
       it resends nothing.
     - Keeping the epoch open on such a code could leave a session running on a connection QFEX
       no longer serves.
     - The cost is a reconnect for a request-level code QFEX adds without documenting it. The
       count shows it, and the codec's list can then add the code (item 3).
   - **An ack** is recognised from its envelope, and the epoch stays open (FBC-zeb0 (d)). It
     settles nothing and answers no request. The arm's answer is decided only from the frame
     0132 item 3 keeps (0132 item 1).
6. **A frame that stays redacted whole, live.** This item goes further than the owner's words.
   - **Live answers as replay can.** Replay hands the codec a run of `2`s for such a frame.
     That is never a JSON object: serde_json reads up to 308 of them as a number and refuses
     more. So the codec answers such a frame live exactly as it answers an input that is not a
     JSON object. It answers every such input alike, whether or not it parses as JSON, in every
     state of the codec, before anything else it does with a frame.
   - **fbc-core's contract.** A codec that cannot decode a frame returns a `DecodeError` and has
     pushed nothing. The effects it asked for stand. The runtime counts the error
     (`decode_errors`) and tells `ExecHandler::on_decode_error` for that epoch. Replay feeds the
     journaled frame to the same codec and gets the same error.
   - **The QFEX market-data codec on main.** For bytes that are not JSON, or a JSON scalar such
     as a run of `2`s, it returns `DecodeError::Malformed` and asks for no effect. Its epoch
     stays open. It redacts nothing, so no frame of its own is kept whole.
   - **The order-entry codec.** FBC-zeb0's done line has it reconnect on a malformed frame,
     within that frame's epoch only. So a frame it keeps whole reconnects the same way, live and
     in replay.
   - **Why ending the epoch is right here.** The frames kept whole are now only echoes of
     authentication, frames with an escape or a repeated member name, malformed frames, and
     frames whose blanked form would not parse.
     - An err echoing authentication concerns the session's own authentication. The documented
       `AlreadyAuthenticated` example is one, and it ends the epoch under item 5 anyway.
     - For the other frames kept whole, the codec cannot tell a session failure from a request
       error. So it takes item 5's conservative side.
   - **No hold.** The order-entry codec emits no `Effect::RateLimited` for a frame kept whole,
     even when the frame was live a refusal. Nothing the codec may read shows a refusal. This
     narrows 0126's "With absent or ambiguous echo it holds every frame budget" for those
     frames: they hold no budget.
7. **The arm's window, narrowing 0132 item 3.** This item goes further than the owner's words.
   - **The arm's own echo.** An `ack` or `err` whose echo equals the arm, as 0132 item 1
     defines it, stays as 0132 item 3 keeps it, with one exception. A RateLimited err's
     `message` is kept in clear when it is exactly one of item 3's two forms. Any other
     `message` stays a span. So live and replay hold for the same hint. The arm's echo maps to
     no bucket, so every frame budget is held (0126, 0132 item 1). An `err` with another code is
     the arm's rejection, as 0132 item 1 says, and the epoch then ends under 0058.
   - **Another echo in the window.** An err or an ack whose echo is not the arm also keeps in
     clear what item 3 keeps: a documented `error_code`, the echo's mapped `type` and a
     RateLimited err's `message` in one of the two forms. It still answers nothing (0132 item
     3), and item 5 says what the codec does with it.
   - **What 0132 item 3 still governs.** Within the arm's window, a frame that item 2 would keep
     whole is journaled as 0132 item 3 says, which this record does not change. An echo of
     authentication stays whole there too.
   - **Where spans sit.** Wherever 0132 item 3 keeps a frame partly in clear, its spans sit as
     item 4 places them.
8. **What this narrows.** Each sentence below is narrowed as the named items say.
   - **0126.**
     - "The entire inbound frame is redacted when it can echo a request." It no longer covers an
       err or an ack (items 1 to 4).
     - "A codec emits a budget-only `Effect::RateLimited` with the parsed duration and an
       operation only when the echo names a known bucket." With "a missing or invalid hint uses
       at least the declared window. With absent or ambiguous echo it holds every frame budget",
       it is now read through item 5. The duration is parsed only from a kept `message`, and the
       bucket only from a kept `type`. A spanned `message` is a missing hint, and a spanned
       `type` an ambiguous echo. None of these sentences applies to a frame kept whole (item 6).
   - **0132 item 3.**
     - "An `err`'s free-text `message` is redacted as a span of its own." It no longer covers a
       RateLimited err's message in one of the two forms (item 7).
     - "Each of these is redacted as a span of its own: every scalar value inside
       `incoming_message`, a string echo as a whole, the `message`, …". It no longer covers a
       mapped `type`, or a RateLimited err's `message` in one of the two forms, in an err or an
       ack (item 7).
     - "Every other frame that can echo a request stays redacted whole." It no longer covers an
       err or an ack (items 1 to 4).
   - **0132 item 8.** Its "no decoding may rest on" frames that item 3 keeps whole still holds
     for settlement: an `err` settles nothing. What now rests on a kept err is its budget
     feedback, its count, and whether it ends the epoch (item 5).
   - **FBC-zeb0's planned behaviour.**
     - Draft PR #196, which FBC-zeb0 re-lands, ends the epoch on every frame it does not
       recognise once authenticated, so on every err but a `RateLimited` one. A kept err with a
       documented code other than item 5's four now leaves the epoch open and is counted.
     - FBC-zeb0's done line, "an auth failure, an auth reply missing past 60 s and a malformed
       frame each reconnect within their own epoch only", stands. A malformed frame and a frame
       kept whole still end the epoch (item 6), and so does every err before authentication and
       every err of item 5's four (item 5).
   - **0028.** Unchanged. This record is 0028's remedy that "the spans must shrink".
   - **Out of scope.** An `order_response` whose status is `RATE_LIMITED` (FBC-zeb0 (b)) echoes
     no request. Its account fields fall under FBC-77aa's span redaction.

## Alternatives

- **B, keep less and stop reading the echo and the hint.** Not chosen by the owner. Every
  refusal would then also hold cancels (0126's consequences). A hint longer than the declared
  window would be ignored, so the local hold could end before QFEX's.
- **C, keep 0126 whole and let replay diverge.** Not chosen. Replay of a session would show a
  reconnect where live held the budget or kept the epoch open. FBC-zeb0, FBC-mfey and FBC-jxau
  would drop their same-effects clauses for these frames.
- **Keep only a RateLimited err and an ack readable, as option A alone did.** Superseded by the
  owner's "Yes, broaden it". Every other echoing err would stay whole, so live could not read its
  code. It would then end the epoch on each of them, and on an armed epoch that triggers
  cancel-on-disconnect and a fresh arm (0132), for one rejected request.
- **End the epoch on every err, as the draft does.** Not taken, for the same reason: a request's
  refusal says nothing about the connection.
- **Keep the epoch open on every documented code, the four included.** Not taken.
  `PermissionDenied`, `KycRequired` and `TncRequired` say that the account may not act, a cancel
  perhaps included. `AlreadyAuthenticated` says that the codec's view of the connection is
  wrong. Ending the epoch re-establishes the session, and an armed drop is QFEX's one path that
  still cancels.
- **Keep the epoch open on an undocumented code.** Not taken, for item 5's reasons.
- **Span a string with its quotes.** Rejected. A long string would replay as a run of digits
  that serde_json refuses, so replay would lose a frame live decoded, and every string would
  replay as a number.
- **Decode JSON escapes, and judge kept values after decoding.** Rejected. Placing spans over
  escaped bytes, and matching decoded text exactly, would make the redaction scan a second JSON
  parser. As the owner chose, a frame with a backslash stays whole.
- **Read a frame kept whole as 0126 says, live.** Rejected. Replay cannot, so live and replay
  would differ (item 6).
- **Hold every frame budget on every frame the codec cannot decode, so that a refusal kept
  whole still holds.** Not taken. Every garbled frame would then delay cancels too (0126), and
  most frames kept whole are not refusals. Captures showing refusals kept whole would reopen
  this.
- **Tell replay's blank, a frame of only `2`s, apart from a malformed frame.** Rejected. 0028
  item 5 says where a span was blanked is told by the record's spans, not by the bytes. 0028
  item 4 asks for decoding that does not depend on the bytes a span holds.
- **Leave the arm's `message` a span, so that the codec ignores the hint there.** Consistent
  too, but not taken. It would hold the arm's refusal for the declared window where QFEX named
  another, unlike every other RateLimited err. The arm carries no credential (0132 item 3), and
  the two forms carry only a number.

## Consequences

- **FBC-zeb0** implements items 1 to 6:
  - `redact_inbound`'s spans in both err envelopes and the ack;
  - the codec's reading of kept bytes only, with both hint forms;
  - item 5's answer to each err and its count by code;
  - the answer to a frame kept whole.

  Its tests show each frame's spans, and show that the blanked journal replays to the same events,
  effects and counts.
- **FBC-mfey** implements item 7 within 0132 item 3's exception.
- **FBC-jxau** decodes the arm's RateLimited err with its kept hint. Under `preprod_probe` a
  rejected place's or cancel's err now leaves the epoch open, and the request ends `Unknown` at
  its deadline.
- **FBC-bsou and FBC-h3z1** classify errs and acks under this record. FBC-bsou's refusal rule
  before the last confirmation is item 5's rule before authentication.
- **FBC-77aa** is unaffected. Its span redaction covers frames that echo nothing.
- **What a journal shows.** For these frames it now shows their shape, the documented code, the
  bucket and the hint. Every `params` value and every other message is still a keyed hash. An
  error code carries no credential.
- **The keep lists follow the codec's.** A request type the codec maps later, or a code it lists
  later on QFEX's documentation, is kept once listed. `auth` never is.
- **A request's err costs only its request.** That request counts as resting until the Unknown
  ladder resolves it (0005).
- **Some frames still end their epoch.** A frame kept whole, an err before authentication, and
  an err with one of the four codes or an undocumented one each end their epoch.
  - Each reconnect is paced as every reconnect is.
  - On an armed epoch it carries 0132's cancel-on-disconnect consequences.
  - A `PermissionDenied`, `KycRequired` or `TncRequired` that persists reconnects until the
    consumer's pacing budget is spent.
- **What captures must show.** If QFEX escapes characters in these frames, or repeats a key, each
  such frame ends its epoch. The probe's captures show whether it does.

## What would show this was wrong

- A credential, account identifier or order detail in a byte this record keeps in clear: a
  member name, an error code, a mapped `type`, or a `message` in one of the two forms.
- QFEX giving its retry hint in another form or member, or its bucket anywhere but the echo's
  `type`. The codec would then hold for the declared window, or hold every bucket, where QFEX
  said otherwise.
- A documented code this record keeps the epoch open on, arriving when the session itself is
  broken: one that answers every later request alike, or comes before the server closes the
  connection.
- One of the four arriving for a single request on a session that still serves the others.
- Errs or acks that QFEX routinely sends with an escape, a repeated key, or a shape none of the
  three envelopes matches. They would hold no budget and would end their epochs.
- A replayed journal whose events, effects or counts differ from live for a frame this record
  covers.
- An echoed request type that QFEX's buckets charge differently from the codec's mapping.
