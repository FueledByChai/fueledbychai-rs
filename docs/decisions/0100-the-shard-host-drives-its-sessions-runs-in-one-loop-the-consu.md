# 0100 — The shard host drives its sessions' runs in one loop; the consumer's decide pass is a ladder the host submits one command at a time, polling every session between two, and the handlers' own submissions go first, augmenting 0023 and 0057

Status: accepted
Date: 2026-10-18

## Context

0002 has one current-thread loop per group of markets read, decode, apply, decide and sign,
signing one order at a time so a protective cancel waits for at most one signature in progress.
Design §5.1-§5.2, restated: the loop reads, decodes and applies every ready frame, writes a Cycle
record (FBC-ec9) and lets the consumer decide for each instrument that changed, then encodes and
signs one command at a time, and between two commands does a non-blocking read of the shard's
sockets and runs the per-event path, so a protective cancel issued then goes ahead of the
remaining planned commands. FBC-ct0 builds that host in `fbc-runtime`. It was the ticket's to
decide how the decide pass and the between-command poll reach the consumer alongside 0023's
inline handler.

What is already on main shapes the answer. `MdSession`, `MdVenue` and `ExecSession` each own
their loop (`run`), which hands every event to the consumer's handler inline (0023, 0053). An
order-affecting command reaches an order-entry session only as an fbc-oms `Authorization`
through its `ExecOrders` (0057), and `ExecSession::send` re-checks it immediately before its
encode (0060, 0066), so a kill switch, a disarm, a state change or, in Exit, a fill landing
between a command's build and its write refuses it there. fbc-oms judges a command against the
caps again when it is authorized (0082), and releases a held place whose command is dropped
(0084).

## Decision

1. **Members.** `ShardHost::run(members, decide)` drives the runs of the shard's sessions as its
   members (`Member::md`, `Member::md_venue`, `Member::exec`), all on the one task that awaits
   it, polling each itself. Every member's session and the host share one `IngestClock` (the
   consumer gives each session's configuration and the host the same one), which is how the host
   sees an input taken: every input is stamped from it. The host spawns no task (design §5.1).
2. **A pass.** Once an input has been stamped since the last pass, the host drains: it yields
   to the runtime, which polls the I/O and time drivers before it resumes the host, then polls
   every member once, until such a round stamps no input. It then journals one
   `Record::Cycle { last_ingest_seq, instruments }` under Normal (the last ingest sequence given,
   and the instruments the consumer marked changed through the host's `Changed` handle since the
   last pass, in the order first marked), and calls `Decide::decide(instruments, ladder)` once:
   the consumer's decide pass, a plain synchronous call, which a closure implements. A pass that
   decides nothing is journaled too, so replay decides where the live shard did.
3. **The ladder.** The decide pass hands its commands to the host as a `Ladder`, each an fbc-oms
   `Authorization` for an `ExecOrders` (refused there for another account, the authorization
   handed back) or a control command, in the order the consumer pushed them (the planner's
   cancels, reducing orders, amends, then adds). The host holds the ladder, not the session:
   it submits one command, lets the members run until that session has taken it (encoded,
   signed and written, or reported not sent) and holds nothing else of it, then polls between
   commands (point 4), and only then submits the next.
4. **Between two commands.** The host yields to the runtime and polls every member once: frames
   already read by the kernel are read, decoded and applied, and their events reach the
   consumer through the handlers it already has, `MdHandler::on_md` and `ExecHandler::on_exec`
   (0023, 0053). Nothing new reaches the consumer here; the per-event path is the handler.
5. **What a handler submits goes first.** A handler submits through `ExecOrders` as before
   (0057). Since the host submits the next ladder command only once that session holds nothing,
   whatever a handler submitted meanwhile is taken first, after at most the one command whose
   encode or write was in progress when it was submitted. A kill switch or any other state change
   a handler makes there refuses each remaining place, batch and amend at its encode
   (`NotSent(StaleAuthorization)`, nothing written, 0060, 0066), while cancels and the kill
   switch's cancel-all within 0005's I7 still go out.
6. **Attribution (for FBC-2y3's measurement).** The shard clock keeps the stamp of the input
   being handled, from its stamp until its session waits for the next input. A command submitted
   then is attributed to that input, whichever session took it: its write's tick-to-wire runs
   from that input's `kernel_rx` when the command is Safety traffic. Ladder commands come from
   the decide pass and are attributed to no input. Reporting it on order entry is FBC-qfm's.
7. **Ends.** A member that ends without an error is polled no more; the run returns once every
   member has ended. A member's error ends the run at once and is returned, as `MdVenue::run`
   does for its endpoints; dropping the other runs closes their connections, which the venue's
   per-connection cancel-on-disconnect protects (0058). A ladder command never submitted,
   because its session had ended when its turn came or the run ended first, is handed back to
   the consumer (`Decide::unsubmitted`, an authorization unspent) and counted: nothing of it
   reached a session, so no `on_submitted` reports it. Withdrawing such an authorization in
   fbc-oms is FBC-6ezy's.

## Alternatives

- Two queues in `ExecOrders`, a planned one the session takes a command of per turn and an
  urgent one it takes first: rejected. The session would have to yield to the other members
  between planned commands, putting the host's schedule into every session, and a planned
  command already queued would still sit behind nothing a handler submits later.
- Join the members with `tokio::join!` and leave the sessions to interleave: rejected. The
  combinator's rotation decides which member runs after a command, so a planned command could
  go out before a ready frame was read, and nothing would hold the next command until the
  session's queue is empty.
- Make the sessions steppable (a poll per input) and have the host call their steps: rejected
  for now. It would rebuild every session's loop, connection waits and write stalls as state
  machines for no change in what the consumer sees; polling their runs gives the same order.
- A decide pass that returns its ladder lazily, the host asking for the next command after each
  poll: rejected. The consumer can already react between commands through its handlers, and a
  ladder decided whole is what replay re-decides from one Cycle record.
- Journal the `Cycle` record under Safety: rejected. A pass's inputs are mostly market data,
  journaled under Normal and dropped first under pressure; a Cycle kept while its inputs were
  dropped would not let replay decide where the live shard did, and the sink's `Degraded`
  marker already tells replay the span is incomplete (0006).
- A between-command callback beside the handlers: rejected. The events of the frames read there
  are the per-event path's, already the handler's; a second callback would be a second path for
  the same inputs.

## Consequences

- Each ladder command reaches the consumer once: through `on_submitted` once its session took
  it (as 0057 has it), or through `Decide::unsubmitted`.
- A consumer builds its sessions with one `IngestClock`, gives the host the same one, marks the
  instruments its handlers saw change through `ShardHost::changed()`, and pushes the commands of
  each decide pass onto the `Ladder` it is given instead of submitting them itself. A command it
  submits from decide through `ExecOrders` directly bypasses the ladder and goes out with the
  next poll.
- The ladder holds authorizations only: a place reaches it spent, so 0084's release of a held
  place stays the consumer's (a place built in decide and not authorized is withdrawn or
  dropped). 0082's judgement at authorization runs in the decide pass; a fill between it and the
  encode outside Exit is FBC-zux3's.
- FBC-6mf's in-process order-entry stream, FBC-hvqi's consumer inputs and FBC-qfm's
  measurement build on the members, the clock's attribution and the passes.

## What would show this was wrong

- A ladder command encoded while a command a handler submitted earlier waits in its session's
  queue, or a protective cancel waiting behind more than one command in progress.
- A frame the kernel had delivered before a command's encode began, read only after the next
  command's encode.
- A pass whose Cycle record names an ingest sequence that had not been handed to the handlers,
  or a decide pass with no Cycle record.
- A consumer that needs its decide pass to run while a ladder is still going out, which would
  need the passes to overlap.
