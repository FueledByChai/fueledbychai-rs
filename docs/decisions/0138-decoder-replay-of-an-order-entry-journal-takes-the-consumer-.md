# 0138 — Decoder replay of an order-entry journal takes the consumer's commands from the caller by request id, encodes nothing for a context whose command it is not given, and tells each context's call by its place, its request id and the session's own gate, augmenting 0006, 0078 and 0101

Status: accepted
Date: 2026-11-05

## Context

FBC-tgt (story BT-202) builds `ExecReplay`, decoder replay of one order-entry session's journal
(0006). It feeds the records back in journal order to one codec of the venue, through the calls
the live session made, as `MdReplay` does for market data. It is not closed-loop (0107). Its done
line compares the replayed execution events, with their stamps and venue meta, against what the
live session's handler heard.

What the journal holds of an order-entry session (0078, 0090):

- each epoch's `Opened` and `Closed`;
- every frame and HTTP result the session received, each codec timer firing and each request
  deadline firing;
- each nonce it reserved;
- each context it gave a call (`Record::EncodeCtx`, format version 3), just before the call: an
  encode's with its request id, and `None` for `on_open`, `on_timer` and the resync, which 0078
  item 2 says replay matches by place;
- every frame it wrote and every HTTP request it made, each with the request id it carries, if
  any.

It does not hold the command an encode was given. The consumer submits it through `ExecOrders`,
as an fbc-oms authorization or a control command (0057). The session journals its context, its
nonces and, if it writes it, its outbound bytes, but never the `VenueCommand`.

Replay needs the command. One codec serves every epoch and holds the account's state, the
requests waiting for answers included (0053). An encode is where the codec learns a request:
what to decode its answer against, and what to report at its deadline (0014 item 3). The
conformance toy's codec records each request as it encodes it, and Paradex's records each
placement (0092). A replay that makes no encode where the live session made one leaves the codec
without the request. The frame that answers it may then decode otherwise than live. With the
conformance toy, the answer becomes a decode error, and the deadline reports `Unknown` for a
request the codec knows nothing of.

Replay must also tell which call each context went to. 0078 item 2 says the `None` contexts are
matched by place, but not how. And a context with a request id is not always the consumer's. The
session sends its own `ArmCancelOnDisconnect(true)` on each epoch where the arm is due, once the
codec reports the stream authenticated (0058). The arm takes a request id from the account's
`RpcIds`, and its context is journaled as any encode's is. The journal does not say which
request is the arm.

This record augments three records:

- **0006**, whose journal records what crosses the shard boundary so that a recorded day replays
  through the code that ran live, and whose exact replay reproduces outbound bytes. This record
  states what decoder replay of an order-entry journal takes from the journal, and what it needs
  from the caller. It also qualifies exact replay, and 0078's consequence that the journal holds
  what exact replay needs from the session: a consumer command's outbound bytes are reproduced
  only when the caller supplies the command (item 1).
- **0078**, whose item 2 writes the contexts that replay matches by place. This record states
  how the replay places each one, the session's own arm included.
- **0101**, whose consumer records are where a consumer journals what it decided. This record
  says that is where a consumer keeps its commands when it wants a journal that replays without
  it.

The owner accepted this on 2026-11-05, on the implementer's recommendation.

## Decision

This augments 0006, 0078 and 0101 (it supersedes nothing).

1. **The caller supplies the consumer's commands by request id.** `ExecReplay::set_commands`
   takes a function from an `RpcId` to an `Option<VenueCommand>`. For each context the replay
   attributes to a consumer's command (item 3 (e)), it asks the function for that request id's
   command. It encodes what comes back with the journaled context, the spec table and that
   request id, as the live session did. The function returns a `VenueCommand`, not an fbc-oms
   authorization: the replay executes none of the codec's effects, so nothing it encodes reaches
   a venue, and the live session ran fbc-oms's check of the authorization just before encoding
   it, journaling no context for one it refused (0060, 0062, 0078 item 2). The replay tells the
   handler nothing of an encode: it calls neither `on_submitted` nor `on_tick_to_wire`, so the
   handler hears only the events the codec pushes as later records answer the request.
2. **A command not supplied encodes nothing, and is counted.** When no function is set, or the
   function returns `None` for the id, the replay calls nothing for that context, counts it
   (`ExecReplayCounters::unsupplied`) and goes on. What answers that request, and any later
   event the codec reads against what that encode would have recorded (a placement's final
   phase, for one, 0092), may then decode otherwise than live. The count says how many; the
   function, asked with each id, sees which.
3. **Which call a context was given.** The replay reads each `EncodeCtx` record in journal
   order. The arm's checks are the live session's own code: the replay builds its gate with the
   function the session builds its gate with (`gate_flags`), from the caps the caller's factory
   and configuration give (`VenueFactory::caps`). The journal does not hold the caps, so a
   replay given other caps than the live session's, a changed cancel-on-disconnect declaration
   included, places the arm otherwise than live. The replay moves the gate and the epoch's
   readiness as live: through the function the session hands each event on through, and where
   the arm, the resync and each deadline move them.
   - **(a) `on_open` or `on_timer`.** An `Opened` of one of the session's endpoints, or a codec
     timer firing on its endpoint's open epoch, waits for the next context. A context with no
     request id then goes to that call. The wait ends at the session's next input (an `Opened`
     included), at a context with a request id, or at a `Closed` of one of its endpoints; a
     `Degraded` marker or another connection's record does not end it. A call whose context
     never came is not made, and is counted (`lost_contexts`) unless a `Closed` ended the wait.
   - **(b) The resync.** A context with no request id that no such call waits for is the
     resync's, asked on the order connection's open epoch.
   - **(c) The session's own arm.** A context with a request id is an encode on the order
     connection's open epoch. It is the session's own cancel-on-disconnect arm when that epoch
     is authenticated and the gate has its arm due. Live, the session encodes no command on an
     epoch until it is authenticated, and arms at the end of a turn, before it sends any command
     waiting (0058). So while an authenticated epoch's arm is due, the next encode on it is the
     arm. The replay encodes it as `ArmCancelOnDisconnect(true)`, and does not ask
     `set_commands`.
   - **(d) Whether the arm went out.** The arm counts as sent, so that its answer settles the
     epoch's arm, only where the record straight after its context is its frame, written on that
     epoch under its request id. Live, the session journals the frame as it writes it, straight
     after the context. It writes no frame for an arm whose effects do not carry it, or that the
     buckets refuse for now, and that arm fails the epoch's arm (0058, 0073); an arm the buckets
     can never admit ends the session (`ArmNeverFits`). So where that frame does not follow, or
     the codec refused to encode the arm, the replay fails the epoch's arm, as live; after an
     arm that ended the session, it does so before the `Closed` that follows. The sink may also
     write a `Degraded` marker between the context and the frame, closing a gap opened before
     the arm (0006): the replay then counts the arm not sent, and a replay across that gap is not
     exact.
   - **(e) The consumer's command.** Every other context with a request id is the consumer's
     command, encoded as items 1 and 2 say.
   - **(f) No open order connection.** A context of the resync or of an encode while the order
     connection has no open epoch, which no live session gives, calls nothing and is counted
     (`unplaced`).
4. **A consumer that wants self-contained journals journals its commands itself.** It writes
   each command it submits as one of 0101's consumer records, as an annotation (0101 item 1:
   something the consumer derived), keyed by the account it was submitted for and the request id
   that `ExecOrders::submit` or `submit_control` returned, since request ids are unique only per
   account (0057). It writes the record under the class its command's context goes under, the
   command's own (`VenueCommand::traffic_class`, 0078 item 3), so a cancel's record can use the
   safety reserve as its context does. The bytes are in an encoding of its own. On replay it
   reads them back itself, before the replay reaches their contexts, and supplies them through
   `set_commands`. The library defines no encoding for these records and reads none of them
   (0101 item 2): `ExecReplay` passes over every consumer record, and `MdReplay` every
   annotation (0101 item 4, 0103 item 7).

   This covers the commands the consumer submits through `ExecOrders` itself. A command a decide
   pass pushes onto the shard host's `Ladder` (0100) is submitted by the host, which keeps its
   request id. The consumer is told that id only in `ExecHandler::on_submitted`, after the
   encode, in a `SubmitHandle` that names no command. Until the host hands back each ladder
   command's request id with the command (FBC-tpyv), a consumer that decides through the ladder
   cannot key those commands as this item says, and a replay counts their contexts unsupplied.
5. **The journal is unchanged.** No record kind or field is added, and the writer still writes
   format version 9.

## Alternatives

- **Journal the `VenueCommand` itself, in a new format version.** Every journal would then
  replay without its consumer. The cost:
  - fbc-journal would hand-write an encoding of every command and every field of the orders,
    amends, cancels, references and scopes they carry, since fbc-core has no serialization
    dependency;
  - each field a later record adds to a command would be another format change;
  - every encode, the cancels in the Safety reserve included, would write its command beside its
    context and its frame, and the frame already holds its bytes;
  - journals written before that version would still need the caller.

  Not taken now. A record that needs every consumer's journals self-contained can take it, in a
  format version of its own, or with FBC-kdzs's version 10 if they land together.
- **Require 0101 records, and read them inside `ExecReplay`.** The library would have to reserve
  a kind from the consumer's own numbering (0101 item 1) and define bytes that 0101 item 2 keeps
  opaque. That is the format change above, without a version to say whether a journal has it.
  And a consumer that did not write such records could supply none of its commands, where the
  function lets it supply them from wherever it kept them.
- **Closed-loop replay through the consumer's own decide pass (0107).** It re-decides rather
  than reproduces:
  - the commands come from the consumer's strategy and state, which this repository never holds
    (0001), and match the live ones only if the decide pass is deterministic and is given
    everything it read live;
  - 0107 answers them with SimVenue over in-process sessions (0104), not with the frames the
    venue sent, so it tests the strategy against a simulated venue, not the codec against what
    the venue said;
  - it needs the shard host on a paused runtime, where decoder replay is a synchronous loop that
    any test can run.
- **Parse each command back from its outbound bytes.** Every codec would need an inverse of its
  encode, read through redaction spans the journal keeps only as keyed hashes (0006). And a
  command the codec encoded but the session did not write (effects that do not carry it, or the
  buckets' refusal, 0057) left a context and no bytes, although the codec saw the encode live.
- **Refuse a journal with a command not supplied.** Not taken. The count says how many requests
  may decode otherwise than live, and the rest of the session still replays: its openings, arm,
  resync, timers, deadlines and every decode.

## Consequences

- **What a consumer does.** To replay an order-entry session, it builds `ExecReplay` with what
  the session ran with: the venue's factory and configuration, the credentials, the engine
  namespace, the spec table and the connection number. It gives the replay the records of one
  run of the session. It keeps the control `ExecReplay::new` returns and hands it to the
  replayed handler where it handed the session's, so a stop its handler made live is made at
  the same event (0053). Once that control drops, no event is handed on and no epoch's readiness
  is set: a control dropped at once leaves the handler hearing nothing, and each arm's context
  is taken for a consumer's command. It supplies the commands it submitted, by account and
  request id. A consumer that journals them as item 4 says can do so from its journal alone, for
  the commands it can key.
- **A consumer that supplies nothing** still replays the session's openings, arm, resync,
  timers, deadlines and decodes. Each of its own requests is counted unsupplied, and its answer,
  and any later event read against it, may decode otherwise than live.
- **The replay cannot check what it is given.** A command supplied for an id other than the one
  live encoded under it shows only as events that differ from live.
- **What stays unreproduced.**
  - **A replay across a `Degraded` span** (0006). A call whose context the sink dropped is not
    made. An `on_open` or `on_timer` is counted lost (item 3 (a)), but a resync, or a Normal
    command (a place or amend that does not reduce, a fee query), only by the gap; such a
    command is not counted unsupplied. A context the sink kept whose timer firing it dropped is
    taken for the resync's. An arm whose frame follows a `Degraded` marker counts not sent
    (item 3 (d)).
  - **A journal holding records of more than one session.** Of sessions run at once: an
    `EncodeCtx` names no connection, so the replay takes every context as its own session's,
    and another session's contexts would call its codec otherwise than live. Requests that
    another connection's records name are counted (`foreign_requests`), a partial sign of one: a
    second session that writes no request leaves no count. FBC-kdzs puts the connection on the
    context, in format version 10. Of one session run after another (a restart, or a session
    built again on the same connection number), which a connection on the context does not tell
    apart: live, each run had a fresh codec and gate, and the replay drives one of each across
    the journal, so it replays one run, and the caller gives it that run's records. A request id
    names one command across runs only where the consumer resumed its account's `RpcIds` after
    a restart (0057).
  - **Where a stop the replayed handler does not make came** (FBC-s4bg). Where within an input a
    stop from another thread, or from a handler the replay does not call, came is not journaled,
    nor where one came while the session waited to reconnect, before a deadline it still fired.
    A replay whose control stands hands on the events the live session handed nobody.
  - **An in-process session** (0104). `ExecReplay` builds its codec from
    `VenueFactory::exec_codec`, and an in-process session runs a codec of the consumer's
    (FBC-8lfb).
  - **An input another endpoint brings between the order connection's end and its `Closed`**
    (0115). The replay ends the epoch at its `Closed`, so it counts such an input's events for
    that epoch.
- **The journal format is unchanged.** It is still version 9. A version 9 journal written before
  this record replays as one written after it.
- **Exact replay will need the same commands.** FBC-qwq's comparison of outbound bytes builds on
  this replay, and needs the commands from the same function until a journal holds them; its
  done line names them beside the journaled encode contexts and nonces.

## What would show this was wrong

- A replay of one session run's journal, with no `Degraded` span, every command supplied as the
  live session encoded it and none of the other cases the Consequences leave unreproduced, whose
  events, stamps or venue meta differ from what the live session's handler heard.
- Outside a `Degraded` span, a context the replay gives to another call than the live session
  did: an encode it takes for the session's arm that live gave a consumer's command, or the
  reverse; or a context with no request id that it gives the resync where live gave it to
  `on_open` or `on_timer`, or the reverse.
- Outside a `Degraded` span, an arm the replay counts as sent, or as not sent, otherwise than
  live: for instance, another record the live session journals between an arm's context and its
  frame.
- Once the shard host hands back each ladder command's request id (FBC-tpyv), a consumer that
  still cannot learn a command's request id when it journals the command, and so cannot key it.
- Consumers that each journal every command, in the same encoding, before they can replay at
  all. The journal should then hold the command itself (the first alternative).
