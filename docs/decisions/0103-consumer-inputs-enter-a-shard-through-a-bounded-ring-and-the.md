# 0103 — Consumer inputs enter a shard through a bounded ring and the consumer's timers, stamped with the next ingest sequence, journaled with it in format version 9 and then handled, and replay hands them back in place, augmenting 0006, 0023, 0100 and 0101

Status: accepted
Date: 2026-10-18

## Context

Design §5.4 has a queue from the consumer's UI to the shard. A consumer's HTTP or auxiliary
threads hand the shard operator commands (Start, Stop, Flatten, Wind-down, the kill switch) and
calendar and configuration reloads, and the consumer's own timers (a model's wake request, a
resume hold) fire on the shard. Both must sit in the journal at the ingest position where the
handler saw them, or a replay of a live or shadow journal cannot place them and cannot reproduce
what the consumer decided (0006). 0101 gave the consumer a record for its own inputs,
`Record::Consumer`, but no ingest sequence: its Consequences left to FBC-hvqi how the sequence
is kept, in the consumer's bytes or through a format change. FBC-hvqi (story BT-202) builds the
ring, the timers and their replay in `fbc-runtime`. The shard host (0100) drives a shard's
sessions as members on one task and runs a decide pass once an input has been stamped from the
shard's `IngestClock`.

## Decision

1. **A bounded ring, never blocking.** `ConsumerInputs::new` makes a ring of the consumer's
   capacity (1 to tokio's semaphore limit, no default) and the `InputSender` (`Send`, `Sync`,
   cloneable) the consumer's other threads push into. A push never waits: into a full ring it
   is refused (`InputRefusal::Full`) and counted (`InputSender::refused`, shared by every
   clone); after the inputs' run returned or was dropped it is refused (`InputRefusal::Closed`).
   The ring is tokio's bounded `mpsc` channel (`try_send` on the pushing thread, `poll_recv` on
   the shard), already pinned (0019).
2. **An input is opaque.** What a push carries is the consumer's bytes, its own `u16` kind for
   them, and the traffic class to journal it under (an operator's Stop is Safety, 0006). The
   library never reads them (0001). The handler is handed exactly what was journaled
   (`ConsumerInput { ingest_seq, at, kind, bytes }`), so replay hands back the same input by
   construction.
3. **Stamped, journaled, then handled.** `ConsumerInputs::run`, a shard member
   (`Member::inputs`), takes each input as the shard polls it: it takes the next ingest
   sequence of the shard's clock (so the input takes its place among every session's frames,
   and calls for a decide pass as a frame does, 0100), journals
   `Record::Consumer { at, kind, input: true, ingest_seq: Some(seq), bytes }` under the input's
   class, and only then calls `InputHandler::on_input`. The input names no connection, has no
   kernel receive time, and is not named as the input being handled for 0100's attribution; a
   command its handler submits is attributed to no input, as a ladder command is.
4. **The consumer's timers.** The consumer makes a `ConsumerTimers` (shard thread, cloneable),
   gives it to the inputs that fire it and to whichever handlers arm (`arm(after, class, kind,
   bytes)`). A firing is an input like any other: stamped, journaled and handed to the input
   handler with the kind and bytes it was armed with; the consumer tells it from a push by the
   kind. Timers fire by deadline, a tie in the order armed. A run fires only timers armed before
   its current poll began, so a handler that re-arms with no delay yields to the rest of the
   shard between firings. An arm is counted, and arms nothing, when the timers are inert, when
   their inputs have ended, or when the delay lies past the end of the clock (0023's rule for
   such waits). The run ends once every sender has dropped and no timer is pending; its end, or
   its drop, closes the ring and drops what was armed.
5. **Format version 9 keeps the sequence on the record.** 0101's kind 14 gains `ingest_seq`:
   flag byte 2 is an input the shard stamped, followed by the sequence (`u64`), then the bytes;
   0 (an annotation) and 1 (an input the consumer journaled itself) are written as version 8
   wrote them. An annotation with a sequence cannot be written (`JournalError::Unencodable`).
   A version 8 segment holding flag 2 is malformed, and a reader of version 8 refuses a version
   9 segment at its header, naming the version. The reader reads versions 2 to 9;
   `fixtures/journal/v8` holds a version 8 journal that shows it.
6. **Replay hands them back in place and arms nothing.** `MdReplay::set_inputs` gives a replay
   the consumer's input handler: each input with an ingest sequence goes back to it at its place
   in the journal, between the records it was written between, with its recorded sequence,
   time, kind and bytes, and is counted (`MdReplayCounters::inputs`). An annotation, an input
   with no sequence, or any input of a replay with no input handler is passed over. The
   consumer builds its replay handlers with `ConsumerTimers::inert()`, which counts each arm and
   arms nothing: every firing the live shard took is in the journal. The handler is set on one
   replay of a shard's journal, since every session's replay reads the same inputs.

## Alternatives

- Keep the sequence in the consumer's bytes: the library would have to write into bytes 0101
  keeps opaque, or each consumer would rebuild the stamping, and a replay could not place an
  input without parsing them.
- Infer an input's sequence from its neighbours in the journal: Normal records the sink dropped
  under pressure (0006) and inputs of sessions that journal nothing would leave the inference
  wrong exactly where a Safety input such as Stop was kept.
- A full `Stamp` on the record: an input has no connection or kernel receive time, so a
  connection number would have to be reserved from the consumer's own numbering.
- A ring that blocks the pushing thread when full, or grows: blocking hands the shard's
  backpressure to the operator's thread as a hang, and an unbounded queue is the backlog 0002
  removes; a refused, counted push tells the operator's thread at once.
- A reserved slot for Safety inputs: the ring is the consumer's, sized by it; a refused Stop
  comes back at once to the thread that pushed it, which retries or escalates.
- Timers on the session codecs' `Effect::Timer`: their tags are the codec's (0014), and a
  consumer timer belongs to no connection or epoch.

## Consequences

- A consumer pushes its operator commands and reloads through the `InputSender` and arms its
  wake requests and holds through `ConsumerTimers`, and decodes its inputs in `on_input`; what
  it decides from them is journaled where it happened.
- The journal's next format change is version 10. A consumer that matches `Record::Consumer`
  by its fields names `ingest_seq`.
- A consumer that wants Stop to wait for nothing sizes the ring for its bursts and checks the
  push's answer; `InputSender::refused` counts what it lost.

## What would show this was wrong

- A replay of a live or shadow journal whose consumer inputs reach the handler at other
  ingest positions, or with other bytes, than they did live.
- A push that blocked the pushing thread or the shard, or a full ring that took a push.
- A replay that fires a consumer timer, or a firing missing from the journal of the shard that
  took it.
