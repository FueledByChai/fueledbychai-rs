# 0139 — The journal's soft limit moves every Quoting market of the shard to Cancel-only, journaled from the reserve, leaving the owner's Exit as it is, and only the owner's Start, Flatten or Wind-down leaves Cancel-only, augmenting 0006 and 0012

Status: accepted
Date: 2026-11-05

## Context

Design §5.4, restated: when the journal reaches its soft limit, the market enters Cancel-only,
and the move is journaled from the reserve. 0006 decides from the same section. It gives the
journal's sink a soft limit for Normal records and a reserve above it for Safety records
(cancels, reducing orders, acks and fills). The sink never blocks. At the soft limit Normal
records are dropped and counted, and a `Degraded` marker is written once space returns. 0006
would be shown wrong by "a disk stall or full journal" that "delays a cancel or a reducing
order".

0012 lists the market states (Killed, Cancel-only, Exit, Quoting) and every transition between
them. It lists no transition the journal makes. It says that leaving Killed or Cancel-only always
takes an owner's choice, and that nothing in the library moves a market toward Exit or Quoting on
its own. Exit is the owner's Flatten or Wind-down, and builds only orders that reduce the
position. FBC-8pm (story BT-202) asked the owner whether the soft limit should move markets at all
(OQ-journal-cancel-only). The options were:

- (a) every market of the shard moves to Cancel-only, journaled from the reserve, and only the
  owner's Start, Flatten or Wind-down leaves it;
- (b) no state change, only counting and reporting;
- (c) disarm instead.

The owner's answer, recorded on 2026-10-04 by owner default, is (a), with cancels still written.
The reason given with it: losing the record of what we send is a reason to stop adding exposure.

The answer does not say who sees the soft limit, how a registry learns of it, what "every market
of the shard" means in code, whether the owner's Exit is moved too, what record the move is, or
when the move happens again. These pieces were already in place:

- The runtime's `Journal` is one handle per shard thread, shared by its sessions (0023, 0078). It
  offers every record the runtime writes, and the sink answers each offer `Recorded::Ok` or
  `Recorded::DroppedCounted`.
- The consumer offers its own records to the sink directly (0101).
- The registry is the consumer's. There is one per account (0082), used on the shard's thread.
- An authorization is re-checked just before its encode through shared counters, without the
  registry, on any thread (0060, 0062).
- An order-entry session takes its commands in submission order (0057), encoding one and
  writing it before it encodes the next (`ExecSession`'s send loop).
- fbc-runtime depends on fbc-oms (0045). fbc-oms depends on fbc-core only.

## Decision

This augments 0006 and 0012. It supersedes nothing. It reads the answer as narrowly as it allows:
the move stops what adds exposure, and leaves alone what only reduces it.

1. **When the journal reaches its soft limit.** The runtime sees the journal reach its soft limit
   when the sink drops a record the runtime's `Journal` offered, of either class. 0006's sink drops
   a Normal record only at its soft limit, and a Safety record only past it.

   A drop of a record the format cannot hold, of a record larger than all the room its class
   has, or of any record once the sink is closed, also counts. Such a drop can come with the
   queue nearly empty, so this reads the answer wider than the soft limit taken word for word. It
   is kept because the answer's reason, losing the record of what we send, holds for every lost
   record whatever the cause, and the sink's answer does not tell the causes apart. The owner
   confirmed this reading on 2026-11-05: every drop the sink reports moves the shard.

   The journal is back under its soft limit once the sink keeps a Normal record the runtime
   offers. 0006's sink keeps one only when the record and the gap's `Degraded` marker both fit
   under the soft limit. A sink that names the gap open now (`JournalSink::open_gap`, by the
   number of its first dropped record, as the journal's queue does) also shows the journal back
   under once the gap it reached the soft limit in is no longer open, whatever record closed it:
   a Safety record, or one the consumer offered itself. The sink closes a gap only when space
   returns under the soft limit. Reaching the soft limit is the move from under it to at it, so
   it happens at the first drop, at the first drop after the journal was back under, and at a
   drop into a gap other than the one the journal reached it in.

   A record the consumer offers to the sink itself (0101) is not seen. On a shard whose sessions
   journal market data or whose host journals `Cycle` records, the runtime's next record is
   dropped at once, and that drop moves the shard. A record the caller withholds (`omit`) is not
   offered, so it is never a drop.

2. **Every market of the shard.** The shard is the sessions that share one `Journal`. Its markets
   are the markets of each registry whose order entry goes out through those sessions. The
   consumer gives the shard's journal each such registry's trip
   (`Journal::cancel_only_at_soft_limit(registry.cancel_only_trip())`) before arming any of its
   markets. A trip given while the journal is at its soft limit is tripped at once, and that move
   is journaled too. A journal given no registry moves nothing and journals no marker.

3. **The move.** Each time the journal reaches its soft limit, it trips every registry it was
   given (`CancelOnlyTrip::trip`). In each of those registries:
   - Every market in Quoting moves to Cancel-only, with its armed flag unchanged, as the kill
     switch and its lift leave it (0012).
   - Exit stays Exit. Exit is the owner's Flatten or Wind-down and builds only orders that reduce
     the position, never cross zero and pass every cap (0012, 0063). The answer's reason is to
     stop adding exposure, which Exit never does, and 0006 has a full journal never hold back a
     reducing order. So Exit keeps building its reducing orders, and those authorized in Exit
     before the move go through.
   - Killed stays Killed: the kill switch is stricter, and lifting it still leads to Cancel-only.
     Cancel-only stays Cancel-only.
   - A market moved reads Cancel-only at once (`Registry::entry`) and builds no place, amend or
     batch item (`StateRefusal::CancelOnly`). Its state generation advances at the registry's
     next mutating call. That call applies the trip before anything else, so a Start, Flatten or
     Wind-down made after the trip is never undone by it.
   - A place, batch or amend authorized while its market was in Quoting, before the trip, is
     refused at its encode with `NotSent(StaleAuthorization)`, with no nonce reserved and nothing
     written. fbc-oms's check at submit reads the trip count the registry had when it authorized
     the command (`StaleAuthorization::CancelOnly`), from any thread, even before the registry
     applies the trip. Whether the market was in Quoting is read from the state the registry
     holds once it has applied the trips before, so a trip that comes after counts. A command
     built before the trip and authorized after it is refused as a change of state, because the
     authorization applied the trip.
   - Cancels, cancel-many and the kill switch's instrument cancel-all are built and written in
     every state as before (0012), and fills count as before.

   In a Quoting market the move also stops the reducing orders the quoter builds there (a
   reduce-only quote, an order 0005's I6 classes as reducing), because Cancel-only builds none
   (0012), and it refuses one authorized there before the move. That is the answer's
   Cancel-only. 0006's decision stands whole: the sink never blocks, and every command past its
   check is written whatever the sink answers. But read word for word, 0006's falsifier counts
   this as a full journal delaying a reducing order, for a Quoting market's own reducing orders
   only. From the move on, the owner's Flatten or Wind-down builds the reducing orders. The move
   never undoes either, and a full journal never holds back their orders. The owner decided on
   2026-11-05 that a Quoting market builds no reducing orders of its own after the move: the
   answer's Cancel-only stands, and reducing goes through the owner's Flatten or Wind-down. So
   this record narrows 0006's rule that the journal never delays a reducing order, for a Quoting
   market's own reducing orders after the move only.

   The trip lands while the dropped record's own call is still in progress. A command already
   past its check at encode goes ahead, because 0006 lets what a dropped record records proceed.
   That is the command whose record reached the soft limit, and, in each other order-entry
   session of the shard, the one it has encoded and is still writing, if any. A session encodes
   and writes its commands one at a time, so at most one place, amend or batch per order-entry
   session of the shard is written after the move.

4. **Journaled from the reserve.** At each move, the journal offers `Marker::CancelOnly` under
   Safety right after the dropped record, filed under that record's wall time. The sink keeps it
   from the reserve, ahead of the gap's `Degraded` marker. If the reserve is also full, the sink
   drops and counts the marker like any Safety record, and the move stands. The journal never
   blocks the move or the cancels after it.

   Format version 10 holds the marker as marker byte 3, with no field. A version 2 to 9 segment
   holding it is malformed, and a reader of version 9 refuses a version 10 segment at its header,
   naming the version. The reader reads versions 2 to 10, and `fixtures/journal/v9` holds a
   version 9 journal that shows it. Decoder replay passes the marker over, as it does every
   marker but `Degraded`.

5. **Only the owner leaves Cancel-only.** The move only ever goes toward Cancel-only, which 0012
   does not forbid: it forbids automatic moves toward Exit or Quoting. Leaving Cancel-only takes
   Start, Flatten or Wind-down, as 0012 has it:
   - on a market still armed, none of the three needs a lease;
   - on a market disarmed since the move, each arms it as 0012 says.

   None of these leaves Cancel-only: the kill switch's lift, a disarm, a reconnect, a resync, a
   fill, a timer, or the journal coming back under its soft limit. A Flatten or Wind-down
   pressed after the move stands through every later move, since a move leaves Exit as it is. A
   Start stands until the journal next reaches its soft limit.

6. **No second move while at the soft limit.** While the journal stays at its soft limit,
   nothing moves again. So a Start the owner presses then stands, whatever the journal still
   drops. Once the journal is back under its soft limit, the next time it reaches it moves the
   shard again.

## Alternatives

- **(b) count and report only, or (c) disarm:** the owner answered (a).
- **Move Exit too, reading "every market" widest:** it would stop the owner's Flatten or
  Wind-down, the reducing orders 0006 keeps the reserve for, at the moment a full journal makes
  an exit most wanted, for a reason (stop adding exposure) Exit does not touch.
- **The sink sees the soft limit and writes the marker:** every `JournalSink` would need a new
  method to say so, and the sink knows nothing of registries. The runtime's view of what it
  offers works with any sink, the consumer's own included. A sink names its open gap only to
  sharpen when the journal is back under, and one that does not still works.
- **The consumer moves its registries when the journal tells it (a callback):** a record call can
  run while the consumer holds its registry. A submit's backpressure refusal, for example, is
  journaled inside `ExecOrders::submit`, so the callback would borrow the registry again. A trip
  is a shared count, so the journal trips it from inside any record call with no code of the
  consumer's running there. Both depend on the consumer wiring them before it arms; nothing here
  checks that it did (Consequences).
- **The journal holds the registries:** the consumer owns them and borrows them in its handlers,
  which the runtime calls into, so the journal could not borrow them safely.
- **The runtime refuses every Normal command while the journal is at its soft limit:** that fails
  the journal closed for places. 0006 declined that for cancels, and the answer does not ask for
  it. Cancel-only gets the same result through the one path every order command takes (0013
  rule 2), and the owner's Start can end it.
- **Move the shard again at every drop while at the soft limit, so no Start stands until the
  journal recovers:** this would also undo a Start the owner pressed against a full journal, and
  every move would take a marker out of the reserve.
- **Keep the trip count in the command's guard from its build:** it made every `PermittedCommand`
  larger. Taking the count as the registry authorizes is enough, because the authorization
  applies any earlier trip.
- **A record kind of its own rather than a marker:** the move has no field and marks a place in
  the record stream, as `Degraded` does.
- **Journal the marker under Normal:** at the soft limit it would be dropped by definition.

## Consequences

- A consumer whose shard enters orders gives the shard's journal each registry's trip before
  arming. A registry whose orders go out on two shards is given to both, and either one moves it
  whole, which is the cautious side.
- Nothing checks that the consumer gave it. A registry whose trip the shard's journal was not
  given is not moved, and keeps quoting at the soft limit. A session with a journal could check
  each authorization's registry against the trips its journal holds; that is left for a later
  ticket.
- A consumer's own records do not trip the move, so a shard whose only Normal traffic is the
  consumer's own records moves on the runtime's next dropped record.
- `JournalSink` gains `open_gap`, defaulted to `None`. A sink that wraps the journal's queue
  without passing it on is counted back under its soft limit only when it keeps a Normal record.
- The consumer's UI shows Cancel-only from `Registry::entry` and finds the reason in the journal
  (`Marker::CancelOnly`). Before the owner presses Start it can show the sink's drop counts. A
  Start pressed while the journal is still at its soft limit quotes with records still being
  dropped, and that is the owner's choice: the owner decided on 2026-11-05 that Start is not
  refused then, and Flatten and Wind-down stay possible.
- A closed-loop replay (0107) does not move its registries at the marker yet. Until it does, a
  replay of a journal that holds one diverges from the live run there.
- The journal writes format version 10. Any exhaustive match on `Marker` outside this workspace
  gains a variant, and so does any exhaustive match on `StaleAuthorization`.
- Proof:
  - `crates/fbc-runtime/tests/journal_soft_limit.rs`
    (`the_journals_soft_limit_moves_every_quoting_market_of_the_shard_to_cancel_only_journaled_from_the_reserve_leaving_exit_and_cancels_written`);
  - the unit tests in `crates/fbc-runtime/src/journal.rs`;
  - `crates/fbc-oms/tests/cancel_only_trip.rs`;
  - `crates/fbc-journal/tests/sink.rs`
    (`the_sink_names_its_open_gap_from_its_first_drop_until_a_record_of_either_class_closes_it`);
  - `crates/fbc-journal/src/format.rs`
    (`a_cancel_only_marker_round_trips_and_is_refused_before_version_10`) and
    `crates/fbc-journal/tests/cycle_records.rs`
    (`a_journal_written_before_the_cancel_only_marker_reads_back_unchanged`).

## What would show this was wrong

- A place, amend or batch on a market that was Quoting, written after the journal reached its
  soft limit and before the owner pressed Start, Flatten or Wind-down, other than one already
  past its check at encode then (at most one per order-entry session of the shard).
- A reducing order of a market in Exit not built, or not written, because of the move.
- A cancel not written, or held back, because of the move.
- A market leaving Cancel-only after the move without the owner's call.
- The journal reaching its soft limit again, a new gap after one closed, with nothing moved.
- The owner finding that the move fires on short bursts often enough to stop quoting for
  nothing. The soft limit is the consumer's configuration (0021), so the first remedy is a
  larger budget; a new record otherwise.
- The owner wanting Start, or quoting, refused while the journal is still at its soft limit.
  That calls for a new record, and Flatten and Wind-down must stay possible under it.
