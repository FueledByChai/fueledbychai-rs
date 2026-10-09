# 0090 — An order-entry session journals each request deadline it fires at the stamp its events carry, in format version 7, augmenting 0041, 0057 and 0078

Status: accepted
Date: 2026-10-09

## Context

0057 has an order-entry session hand each request still unanswered at its deadline to the
codec's `on_rpc_timeout` once, which reports it `Unknown`: in the epoch while connected
(`ExecSession::time_out`), or while the session waits to reconnect (`Between::apply`), its
events then stamped under the epoch the session waits to open. The session stamps that firing
from the shard's ingest clock, but 0078 journaled no record of it (FBC-2pr left it to FBC-0hfl).
So replay could not call `on_rpc_timeout` where the live session did, and the ingest sequence
the stamp took was a gap no `Degraded` marker explains, the gap 0041 closed for control frames.
A deadline firing is not a codec timer: `Record::Timer` carries a `TimerTag` from the codec's
own tag space, and a request deadline is the runtime's. Release v0.0.6 writes format version 6,
so, as 0040 and 0041 foresaw, the next format change is version 7.

## Decision

This augments 0041, 0057 and 0078 (it supersedes nothing):

1. **A deadline firing is a record.** `Record::RpcTimeout` holds the stamp the session gave the
   firing, which every event `on_rpc_timeout` pushes for it carries, and the `RpcId` of the
   request it names. Nothing in it is a credential, so nothing is redacted.
2. **Written where it is stamped, before the codec is called.** The session writes it right
   after it stamps the firing and before it calls `on_rpc_timeout`, as an inbound frame is
   journaled before it is decoded: in the connected epoch, between that epoch's `Opened` and
   `Closed`; while it waits to reconnect, after the dropped epoch's `Closed` and before the next
   one's `Opened`, under the epoch it waits to open. A firing stamped after the control dropped
   while the session waits to reconnect is journaled too: its stamp took an ingest sequence.
3. **Under Safety, filed under the stamp's wall time.** It settles a request, perhaps an
   order-affecting one, to `Unknown`, so a `Degraded` span, which drops Normal records, keeps
   it, as it keeps acks and fills (0006, 0078 item 3).
4. **Format version 7 holds it.** It is kind 13: the stamp, then the request id (`u64`). A
   segment of versions 2 to 6 holding kind 13 is malformed, and a reader of version 6 refuses a
   version 7 segment at its header. The reader reads versions 2 to 7; `fixtures/journal/v6`
   holds a version 6 journal that shows it.

## Alternatives

- `Record::Timer` with a reserved tag: the tag space is the codec's (0014), so any value could
  collide with a timer a codec sets, and replay would hand the firing to `on_timer`.
- Share version 6 with 0062's reason: v0.0.6 shipped a version 6 writer and reader, and that
  reader would read a kind 13 record as a damaged segment part way through rather than refuse
  the segment at its header.
- A `Degraded` marker for each firing: every healthy recording with an unanswered request
  would read as degraded, and replay still could not place the firing.
- Under Normal, as a codec timer is: a firing dropped in a `Degraded` span would leave replay
  with a request that never came back `Unknown`.

## Consequences

- A reader of an order-entry session's journal finds every ingest sequence the session gave
  on a record or inside a span a `Degraded` marker explains, deadline firings included
  (`crates/fbc-runtime/tests/exec_journal.rs`).
- An order-entry replay (not yet built) can call `on_rpc_timeout` where the record lies; the
  market-data replay skips the kind, which no market-data session writes.
- A consumer that matches `Record` exhaustively handles `RpcTimeout`.
- The next format change, once a version 7 journal has shipped, is version 8.

## What would show this was wrong

- A replay of an order-entry journal whose `Unknown` outcomes differ from the live session's.
- An ingest sequence an order-entry session gave that no record and no `Degraded` marker
  accounts for.
- A request whose deadline firing must carry more than its id (a batch's held items) for
  replay to reproduce what the codec reported.
