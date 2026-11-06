# 0142 — Exact replay of an order-entry session matches each outbound frame on its connection epoch in the order its codec asked for it and each HTTP request on its epoch wherever it lies, both as the reader returns them, counts what the journal never holds unwritten and what a Degraded span may explain excluded, is exact with no difference and nothing unwritten, and is held to a golden the check rewrites only under its refresh flag, augmenting 0006 and 0138

Status: accepted
Date: 2026-11-07

## Context

0006 has the journal record outbound bytes so that a recorded day replays through the code that
ran live. Its consequences say exact replay of a recorded journal reproduces outbound bytes
modulo redaction spans, that this becomes a golden test once the journal and a venue codec exist,
and that `Degraded` spans are excluded from the golden test and always counted; its decision keeps
order signatures because replay needs them to compare bytes. It does not say how a replayed frame
or request is paired with a journaled one, what counts as not reproduced, what a `Degraded` span
covers, or how the golden is kept.

0138 builds decoder replay of an order-entry journal (`ExecReplay`, FBC-tgt). It feeds the records
in journal order to one codec through the calls the live session made, tells each context's call
by its place, its request id and the session's own gate (0138 item 3), and takes the consumer's
commands from the caller by request id (0138 item 1). Its consequences say FBC-qwq's comparison
of outbound bytes builds on that replay and needs the same commands. It compares nothing itself.

FBC-qwq (story BT-202) adds the comparison and the golden for an order-entry session. It relies on
what the journal holds and how the reader returns it:

- every frame an order-entry session writes, with its kind and spans, and every HTTP request it
  makes, with its request id (0078), each under its own connection's epoch (0115 item 6);
- each endpoint of a session's plan on a connection number and epochs of its own, an HTTP request
  of the epoch whose call asked for it, and a frame one endpoint's call asks for on another
  endpoint's stream written there once the asking call's own effects have run (0115 items 2 to 4);
- each span read back blanked at its place and length, its keyed hash beside the record (0024),
  with the blank byte `2`, and a replayed codec handed inbound bytes with their spans blanked
  (0028 items 4 and 5);
- the sink (`fbc-journal`'s `JournalSink`) numbering the records offered to it from 0, and a
  `Degraded` marker, written once space returns, naming the first it dropped and how many (0006;
  the sink names an open gap by that record, 0139).

Three things had to be chosen. The session writes an epoch's frames in the order its codec asked
for them, but starts an HTTP request asked while a write waits at once, ahead of the frames queued
behind that write, so the journal's order is not the order asked. The session writes nothing of
some of what its codec asks for (0030, 0057, 0058, 0073, 0115), and the replay cannot tell that
from a codec that asks for more than the live one did. And the sink keeps Safety records from its
reserve while a gap is open, before the gap's marker, so a gap's place in the journal is not where
its marker lies.

The owner decided on 2026-11-07 that the rules FBC-qwq's implementation chose take a record of
their own.

## Decision

This augments 0006 and 0138 (it supersedes nothing). It decides exact replay of an order-entry
session (`ExecReplay`); nothing compares a market-data session's outbound frames and requests yet
(What stays open).

1. **What is compared.** With `ExecReplayConfig::compare_outbound` set, the replay compares the
   frames (`Effect::Send`) and HTTP requests (`Effect::Http`) in the effects of each codec call it
   makes (`on_open`, `on_timer`, the resync, an encode, and the decoding of a frame or an HTTP
   result) with the outbound frames (`Record::Outbound`) and HTTP requests
   (`Record::HttpRequest`) the journal holds of the session, those naming one of its connections.
   An encode's effects are compared whether or not the codec refused it. 0006's outbound bytes
   are read as the journal holds them: the frames and requests the session journals, not their
   wire form (the WebSocket framing and masking, TLS, the HTTP/1.1 head the runtime writes around
   a request's method, URL, headers and body), and not the control frames the WebSocket layer
   writes itself (a pong, a close), of which the journal holds no outbound record. Nothing else is
   compared: not the write results, nonce records or contexts the journal holds (the contexts and
   nonces are what the replay feeds the codec, 0138); not the timers, reconnects or rate-limit
   effects a call asks for; and not what the journal does not hold of a frame or request: its
   traffic class, its rate charge, a frame's request deadline (`RpcCall::timeout`) or a request's
   timeout. A codec that sends a cancel as Normal rather than Safety, or moves a deadline, so
   replays exactly; that shows only in what the session then journals otherwise (a record kept or
   dropped while a gap is open, a deadline's firing, 0090), which the replay feeds rather than
   checks.
2. **As the reader returns a record.** Both sides are compared as `Record::blanked` gives them,
   the form the reader returns (0024), with the write instant set to zero on both. Each
   redaction span of a frame, URL or body is filled with the blank byte at its place and length,
   and each secret header's value (one the codec marked, or one secret by its name) is blank at
   its length and marked secret. So a frame's connection epoch, request id, opcode (text when its
   bytes are UTF-8, as the session sends it), bytes and spans' places and lengths are compared,
   and a request's epoch, tag, request id, method, URL, headers and body. What a span holds is
   not compared, neither its bytes nor the keyed hash 0024 keeps. A signature outside every span
   is compared byte for byte (0006); one a codec puts inside a span, or in a header secret by its
   name or its mark, is blanked, and so not compared (Consequences). The opcode the journal holds
   is the session's from format version 5; the reader gives an older journal's frames the opcode
   their blanked bytes imply, so there a frame that is UTF-8 before its spans are blanked and not
   after, or the reverse, reads as a difference. Held by
   `exact_replay_matches_every_byte_outside_the_spans`,
   `exact_replay_compares_where_each_span_lies` (a span cut in two, every byte the same once
   blanked, a difference) and
   `exact_replay_compares_a_requests_secret_headers_by_their_length`
   (`crates/fbc-runtime/tests/exec_replay.rs`), and by
   `a_signature_alone_other_than_the_journals_is_a_difference` and
   `a_command_or_context_other_than_the_journals_is_a_difference`
   (`crates/fbc-runtime/tests/exact_replay.rs`).
3. **Frames: on their epoch, in the order asked.** A frame is asked for on the epoch that the
   endpoint whose stream it names has open when the codec asks for it. One for a stream no endpoint
   of the session has open is found unwritten at once (live, the session refuses it, 0115 item 4).
   A journaled frame is matched with the first frame asked for on its epoch, and still unmatched,
   that equals it. Each frame asked for on that epoch before the matched one, and still unmatched,
   is then found unwritten: the session writes an epoch's frames in the order they were asked for,
   so the journal holding a later one shows the earlier was never written. Held by
   `exact_replay_reports_what_it_did_not_reproduce` (a query's frame passed over by the next one's)
   and `a_frame_for_a_stream_with_no_connection_open_is_unwritten`.
4. **HTTP requests: on their epoch, wherever they lie.** A request is asked for on the epoch of
   the call that asked for it. A journaled request is matched with the first request asked for on
   its epoch, and still unmatched, that equals it, wherever it lies among what was asked for, and
   its match finds nothing else unwritten. A request is matched by its whole record, not by its
   tag or its request id alone: the tag only picks what a difference is paired with (item 5).
   Held by `exact_replay_matches_a_request_journaled_ahead_of_a_frame_asked_before_it` (the
   resync's request, asked for after two queries and journaled between their frames, matched with
   both, nothing unwritten) and `exact_replay_reports_what_it_did_not_reproduce` (the resync's
   request, asked for before the query frames the journal holds ahead of it, still unmatched once
   they are).
5. **A difference.** A journaled frame or request that matches nothing asked for is a difference
   (`OutboundDiff`): the journal's record, blanked, and what the replay asked for in its place, if
   anything. For a frame, that is the first frame on its epoch still unmatched and in the place of
   no earlier difference; for a request, the first such request on its epoch under the same tag;
   `None` when there is none. What is so paired stays asked for: a later journaled record may still
   match it, so a record the replay did not reproduce leaves the frames after it on its epoch to
   match rather than each taking the next one's place, and once a later frame passes over it or
   its epoch ends it is not counted unwritten, its difference standing for it. So a frame or
   request written otherwise than asked (a signature changed) is one difference, and so is one
   journaled that nothing was asked for, however many frames follow it. Differences are reported
   in journal order. Held by `a_difference_leaves_what_it_was_paired_with_to_match`,
   `exact_replay_reports_what_it_did_not_reproduce` and
   `exact_replay_compares_a_requests_secret_headers_by_their_length` (the two differences paired
   with the requests of their tags).
6. **Unwritten.** What was asked for that no journaled record matches, and no difference is paired
   with, is unwritten (`ExactReport::unwritten`). A frame is found so once a later frame of its
   epoch is matched, or at once when no endpoint has its stream open (item 3); a frame or a request
   once its epoch ends (its `Closed`, or an `Opened` of its endpoint with no `Closed` before it);
   and whatever is still unmatched when the report is read (`ExecReplay::exact`), at the
   journal's end for a report read after it. Live, the session writes nothing of a frame its
   buckets refuse as its turn comes (0030); of what an encode asked for when the buckets refuse its
   frames and requests, charged together (0073), or when the codec refused it or its effects do not
   carry its request (0057); of what `on_open` or the resync asked for when the buckets refuse it,
   charged together, which ends the epoch as a drop; of what it had not written when its epoch
   ended or its control dropped; or of a frame whose stream has no open connection (0115 item 4).
   It journals an HTTP request any other call asks for as it starts it, before the buckets are
   charged, so one they refuse then (`HttpFailure::NotSent`) is matched. The replay cannot tell
   what the live session wrote nothing of from a codec that asks for what the live one did not, so
   each counts unwritten. Held by `exact_replay_reports_what_it_did_not_reproduce` (a frame still
   unmatched at its epoch's end) and `a_frame_for_a_stream_with_no_connection_open_is_unwritten`.
7. **Exact.** A replay is exact (`ExactReport::is_exact`) when it found no difference and nothing
   unwritten. What it matched and what it excluded (item 8) do not count against it. So exact
   speaks of the outbound bytes outside what a gap may explain: with `excluded` above zero, or
   across a gap (`ExecReplayCounters::gaps`), it does not show the session reproduced, since what
   the gap may explain went uncompared, and 0138 leaves a replay across a gap unreproduced (its
   item 3 (d)'s "not exact" means that). A caller reads `excluded` and the gaps beside it. Held by
   `exactness_is_no_difference_and_nothing_unwritten_outside_the_excluded` and
   `a_frame_for_a_stream_with_no_connection_open_is_unwritten` (nothing differs, and the replay is
   not exact).
8. **What a `Degraded` span may explain is excluded and counted.** The replay numbers every record
   it is fed, whatever its connection, as the sink numbered the records offered to it. Each record
   but a `Degraded` marker takes the next number, from 0; the markers the sink writes take no
   number, and a marker `{ from_seq, dropped }` moves the count past the `dropped` records it
   names. So each record before a gap is numbered as the sink numbered it, and each one the sink
   kept from its reserve while the gap was open is numbered at or past `from_seq`. At each marker,
   whatever was found since the marker before it (a difference, or a frame or request found
   unwritten) at a record numbered at or past `from_seq` is excluded and counted
   (`ExactReport::excluded`); the rest is reported. A frame or request asked for before a marker
   that the replay has read since, and then found unwritten or still unmatched at the report, is
   excluded too, since the gap may have dropped its record. The count is the sink's only for one
   sink's records fed from its first, every drop marked. A drop no marker names (a process that
   ended with a gap open writes none; the reader goes on past a segment it could not read to its
   end, reporting it once) leaves the count below the sink's from there: a later marker then
   reports some of what its gap may explain, and excludes nothing it does not; `ExecReplay::run`
   stops at the reader's first error. Held by `exact_replay_excludes_what_a_degraded_span_covers`,
   `exact_replay_numbers_records_past_every_degraded_span` and
   `exact_replay_places_a_gap_as_the_journals_sink_marked_it` (records journaled through a real
   journal queue whose sink drops at its soft limit and keeps a Safety record from its reserve).
9. **A gap's later effect is reported, not excluded.** Only what item 8 excludes is excluded. A
   record numbered past a gap whose difference the gap explains is reported: the frame of a Normal
   command whose context the sink dropped (0138 counts that command only by the gap), which may be
   the first record after the marker, or a frame the codec writes otherwise for an input the sink
   dropped. A replay across a gap may so read as not exact; 0138 leaves such a replay
   unreproduced. Held by `exact_replay_excludes_what_a_degraded_span_covers` (a difference after
   the span reported) and `exact_replay_places_a_gap_as_the_journals_sink_marked_it` (the record
   that closed the gap reported).
10. **A frame routed to another endpoint's stream can read as a difference.** The session writes
    such a frame once the asking call's own effects have run (0115 item 4), so possibly after a
    frame the stream's own calls asked for later. The replay then finds it unwritten where that
    later frame is matched (item 3), and reports its record a difference. A venue that plans one
    order-entry endpoint, or whose calls ask only for their own endpoint's stream, has no such
    frame. Held by `a_routed_frame_written_after_its_streams_later_frame_reads_as_a_difference`;
    FBC-mtnt places such a frame where the session wrote it.
11. **The comparison is opt-in.** It runs only when `ExecReplayConfig::compare_outbound` is set.
    It keeps each difference it finds, the journal's record and the replay's, to the end, but for
    those a `Degraded` marker excludes, which it drops there, and what the codec asked for until it
    is matched or settled. So a decoder replay that supplies no commands leaves it off and keeps
    nothing of it; `ExecReplay::exact` is then `None`. Held by the FBC-tgt replays in
    `exec_replay.rs` (`a_journaled_order_entry_session_replays_to_the_same_execution_events_every_time`
    and the stopping handler's), each of which asserts `exact()` is `None`.
12. **The golden, rewritten in the check only under its refresh flag.**
    `fixtures/conformance-toy/exact_replay/golden/` holds the journal of one conformance-toy
    order-entry session, what a replay is given besides it (`session.txt`) and a rendering of every
    outbound frame and HTTP request it holds (`outbound.txt`). It is recorded only against the
    test's own stubs on local ports, with made-up credentials and redaction key, and holds no key-
    or address-shaped value, so it sits unmarked under the privacy scan (0009); a later golden is
    held to 0009 as any recorded journal is. `tests/exact_replay.rs` replays the committed journal
    and one recorded afresh on every run, each exactly on two replays. The fresh recording replaces
    the committed golden, whole, only once it has replayed exactly twice and only when
    `FBC_REFRESH_GOLDENS` is exactly `1` (`the_golden_is_rewritten_only_when_its_refresh_is_asked`).
    `scripts/check.sh` unsets that variable at its start and sets it only under
    `--refresh-goldens`, for that one test, through `scripts/golden-refresh.sh refresh`, which
    fails unless every file of the golden was rewritten, judged by its modification time against a
    mark taken a second before the test starts. Its last step, with or without the flag, fails a
    run in which any step after the refresh changed the golden (`scripts/golden-refresh.sh
    unchanged`, against the checksum taken before the tests). `scripts/golden-refresh.sh
    --self-test`, run by every check, proves both on a golden of its own, and fails when
    `tests/exact_replay.rs` no longer holds the refresh's test under its name. The golden is
    refreshed only for a deliberate change to what the session writes (the toy's codec, the
    session or the journal's format), said in the commit body; after a change of format, no golden
    shows a journal of the older version still replaying exactly. A `cargo test` run outside the
    check with the variable set rewrites the golden too, and the check cannot tell; the next check
    replays what that run wrote. `--refresh-goldens` rewrites exact replay's goldens only, so far
    this one (`scripts/golden-refresh.sh` names its directory and test, and a later golden joins
    there); the conformance suite's `signing_golden` files are kept by hand, as before.
13. **The journal is unchanged.** No record kind or field is added, and the writer still writes
    format version 10.

## Alternatives

- **Compare each epoch's records as one sequence, in journal order.** A request asked for while a
  write waits starts at once, ahead of the frames queued behind that write, so the journal can
  hold it before frames asked for earlier. And one frame the session never wrote would shift every
  later position, reporting each later frame a difference.
- **Match frames wherever they lie on their epoch, as requests are.** The session writes an
  epoch's frames in the order asked, so their order is part of what is reproduced; a codec that
  reordered its frames would match.
- **Pair a difference with what was asked for in its place, and take that out.** One frame
  journaled that nothing was asked for would take the next frame's place, and that frame the one
  after it's, so every frame after it on its epoch would read as a difference beside the wrong
  frame, and none would match.
- **Pair a request with what was asked for in its place by its request id.** A request no
  consumer command asked for, such as the resync's, carries none. The tag is what a codec puts on
  every request to know its answer.
- **Compare the keyed hashes of the spans as well (0024).** A replayed codec is handed inbound
  credentials blanked (0028 item 4), so where live it wrote a token it was sent, it writes the
  blank, and that span would hash otherwise whatever the replay holds. Hashing would also put the
  consumer's redaction key in the replay. Whether a credential changed stays with the hashes the
  journal keeps, read apart from exact replay.
- **Leave unwritten frames and requests out of exactness.** A codec that asks for more than the
  live one did would then replay exactly. The count says how many, for a caller that expects the
  live session to have refused some.
- **Exclude everything between two markers, or everything after the first marker.** The first
  hides records the sink wrote before it dropped anything. The second hides every later
  difference of the journal. The sink's numbering places a gap where it opened.
- **Exclude what was asked for before a marker only when it was asked for near the gap.** The
  session journals a frame as it writes it and a request as it starts it, which may be well after
  the call that asked for it, behind a write that waits, so where its record would lie is not
  known. It is counted excluded instead, and read beside `is_exact` (item 7).
- **Always compare.** A decoder replay would keep both records of every difference, each
  unsupplied command's frame among them, for nothing.
- **Rewrite the golden whenever a run finds it stale.** A change to the codec, the session or the
  journal would then pass the check unseen.

## Consequences

- **What a caller does.** It sets `compare_outbound` and builds the replay as 0138 says, with the
  venue's configuration and credentials the session ran with wherever they reach outbound bytes
  outside a span (a REST base in a request's URL, or a key a signature covers). It supplies every
  command by request id (0138 item 1); a command not supplied leaves its frame or request a
  difference. It feeds the records unfiltered and in order: a journal it filters numbers records
  otherwise than the sink did, and item 8 excludes the wrong ones.
- **Whose count it is.** Item 8's count is one sink's, which numbers from 0 again in each process
  run that journals to a root and shard. `JournalReader` reads every run written there as one
  sequence, and nothing the runtime journals marks where a run's sink began. So the caller starts
  the replay at the run's first record, which it knows from outside what the runtime journals (a
  root per run, or a `Marker::SessionStart` it journals itself). Fed an earlier run's records first,
  the count runs past the sink's, and a marker may exclude what its gap does not explain, so a
  report can read exact wrongly. Fed a session run that is not its sink's first, as 0138 has a
  caller give a session built again in one process, the count runs below the sink's: a marker may
  then report what its gap explains, and never excludes what it does not.
- **What fails `is_exact` though the codec reproduced the session.** What the live session wrote
  nothing of counts unwritten (item 6); a difference a gap explains found after its marker is
  reported (item 9); and a frame routed to another endpoint's stream reads as a difference (item
  10), so until FBC-mtnt lands, a session that drives several connections and routes frames
  between them can fail `is_exact` while replaying faithfully. None shows 0006 wrong: in none did
  the session write a byte otherwise than its codec asked for it.
- **What `unwritten` does not say.** It counts what the live session refused (item 6) beside what
  the codec asked for beyond live, so a caller that expects refusals reads the count.
- **A signature inside a span is not compared.** A codec that carries a signature inside a span,
  or in a header secret by its name or its mark, has it blanked, so exact replay does not compare
  it. A venue's golden that is to compare its signatures shows they lie outside every span and
  secret header, as `exact_replay.rs` counts the toy's three.
- **What stays open.**
  - FBC-mtnt: a frame routed to another endpoint's stream placed where the session wrote it.
  - Exact replay of a market-data session (`MdReplay`): nothing compares its subscription frames,
    keepalives or snapshot requests.
  - Replaying a session run that is not its sink's first with the sink's count: numbering the
    sink's earlier records without replaying them, or a start of its count the replay can find.
  - Among what 0138 leaves unreproduced, these stay so here: a journal of more than one session
    (FBC-kdzs puts the connection on the context), an in-process session (FBC-8lfb), a stop the
    replayed handler does not make (FBC-s4bg), and an input another endpoint brings between the
    order connection's end and its `Closed` (0115).
  - Only the conformance toy's session has a golden.
- Proof: `crates/fbc-runtime/tests/exact_replay.rs`, `crates/fbc-runtime/tests/exec_replay.rs`'s
  exact replay tests, and `scripts/golden-refresh.sh --self-test`, as each item cites.

## What would show this was wrong

- A replay of one session run's journal by the code that recorded it, fed from its sink's first
  record, with no `Degraded` span and no frame routed to another endpoint's stream, every command
  supplied as the live session encoded it and the configuration and credentials it ran with, that
  reports a difference, or counts unwritten more than the live session wrote nothing of (item 6).
- A session found to write an epoch's frames in another order than its codec asked for them,
  other than item 10's frame.
- A frame or request written otherwise than asked for that leaves a later frame of its epoch,
  written as asked, reported a difference.
- In a replay fed one sink's records from its first, outside a `Degraded` span, a difference
  excluded, or one the span covers reported: a record the replay numbers otherwise than the sink
  did.
- A need to see in exact replay whether a span's credential changed; then the hashes are compared
  in a new record.
- A run of the check that changes the golden without `--refresh-goldens` and passes, or a refresh
  that leaves a file of the golden as it was and passes.
