# 0106 — A closed-loop replay is a shard member feeding each record at its recorded instant on a replayed clock the paused runtime moves, restamped into the shard's ingest order, augmenting 0006, 0056, 0100, 0103 and 0104

Status: accepted
Date: 2026-10-19

## Context

BT-501 makes the consumer's backtest its source of truth for P&L, so it must run the same shard
host, fbc-oms and SimVenue code as live and shadow. FBC-w9g replayed a journal through SimVenue
for a fixed command script, driving the venue by hand. A market maker's backtest is closed-loop:
its commands depend on the fills it got, so they cannot be scripted, and they must come from the
consumer's own handlers and decide passes on the shard host (0100), answered by FBC-6mf's
in-process order-entry sessions (0104). FBC-066n builds that.

What is already on main shapes the answer. Decoder replay (`MdReplay`, 0006) is synchronous and
hands events the recorded stamps, live ingest sequences included. The shard host sees an input
taken only by the shared `IngestClock`'s sequence moving (0100 item 1). Every wait of a session
(reconnect pacing, request deadlines, write stalls, rate buckets, keepalives, and the in-process
stream's sleep until its peer's next due answer) is a tokio sleep on the runtime's clock, and
`IngestClock::now` read the system's wall clock. Consumer inputs and timer firings sit in the
journal at their ingest positions (0103).

## Decision

1. **A replayed clock.** `IngestClock::replayed(wall)` is a shard clock that reads no wall
   clock. Its monotonic time is the runtime's, from its origin; the consumer runs a closed-loop
   replay on a current-thread runtime started paused (`start_paused`, tokio's `test-util`
   feature in the consumer's build), so that time moves only when every task waits, straight to
   the earliest timer due: virtual time, as fast as the CPU. Its wall time is `wall` at the
   origin, and from the first replayed record on that record's recorded wall time plus the
   monotonic time since its recorded instant. No input is stamped behind the latest instant
   already stamped, so ingest order never goes back in time.
2. **The member.** `Member::md_replay(replay, ClosedLoop { clock, records, settle, stops })`
   drives an `MdReplay` as one of the shard's members. Each record it feeds (an input or control
   record of its connection, a ping, pong or close frame its live session stamped included, and
   a consumer input the shard stamped while an input handler is set) first waits for its
   recorded instant on the replayed clock, so every answer a session's
   peer has due before it is read first, the session's own sleep being the earlier timer. The
   record is then fed as `MdReplay::feed` feeds it, with each input restamped: the shard's next
   ingest sequence, its recorded instant on both clocks (moved on only as item 1 says), its
   kernel receive time and connection. A ping, pong or close reaches no codec but takes its
   sequence as live, and an HTTP result held until its epoch's codec is built takes its sequence
   as it is answered, so a sequence and its events always reach the shard together (Codex P2
   r4315555320, r4315555331 on PR #178). The clock is set for the replay only for the length of
   each synchronous feed, so a replay whose run is dropped mid-play stamps with the recorded
   stamps again when fed on its own (Codex P2 r4315555339). Its events reach the handler with the clock naming the
   input as handled (0100 item 6), and a consumer input goes back to the input handler with the
   next ingest sequence at its recorded time (0103). The host sees these inputs as it sees any
   member's: it drains, journals a `Cycle` record, runs the decide pass and submits its ladder,
   as live. Records of other connections, outputs, the consumer's annotations and the source
   journal's own `Cycle` records are passed over, as decoder replay passes them.
3. **The order-entry sessions are shadow's.** Each bracket is an unchanged
   `ExecSession::in_process` over its `SimPeer` (0104) on the same clock: a command is encoded at
   the replayed clock's now, so SimVenue answers it at the replayed instant it was written plus
   its configured latency, and the session reads the answer at that instant. The consumer's
   timers arm through `ConsumerTimers::inert`: each firing the live shard took comes back from
   the journal (0103).
4. **The end.** Once the records end, the member waits `settle` past the last instant the
   journal records, of a record fed or passed over (Codex P2 r4315555312), so the answers due by
   then are read; yields once, so every member woken at that instant is polled; waits until the
   host has run a decide pass, and taken its ladder, since the last input was stamped (the host
   marks the shard clock after each pass), so what was due at the settle instant is read and
   decided on (Codex P2 r4315555356); then drops `stops`, the order-entry sessions' controls,
   which ends them and the host's run with them. A live clock (`ReplayFailure::LiveClock`), a
   record the journal cannot read (`ReplayFailure::Journal`) and a damaged text frame
   (`ReplayFailure::NotUtf8`, naming the record's recorded ingest sequence) each end the member
   with `ShardError::Replay`, feeding nothing more, its stops dropped.
5. **Resolution.** The pinned tokio's timers count whole milliseconds, so a record or an answer
   due between two milliseconds is taken at the next one: a record keeps its recorded instant in
   its stamp, an answer is stamped with the millisecond it is read at, and inputs due within one
   millisecond are taken in the order the host polls its members. A journal whose instants and
   latencies are whole milliseconds replays at its instants exactly; one recorded live replays
   each command's latency up to a millisecond long. Either way two runs are identical.
6. **An order-entry session's turn is deterministic.** 0056's unbiased `select!` polled what
   can wake an epoch's turn (its frames, timers, HTTP results, submitted commands, request
   deadlines and stop) from a random one, so a command a replayed input submitted inline and an
   answer due on the same tick were taken in a random order, and two runs differed in their
   order-entry journals (Reviewer B's RB-066n-1 on PR #178). The turn now polls them from the
   one after the last turn's first, a rotation kept per session: no way to wake starves another,
   as 0056 requires, and the order is the same on every run.

## Alternatives

- **A virtual clock of the library's own, which the host advances**: rejected. Every session's
  waits are tokio sleeps; moving them to a second clock rebuilds the sessions' loops, which 0100
  and 0104 rejected, for what the paused runtime already gives.
- **The replay advancing the paused clock itself (`tokio::time::advance`)**: rejected. It takes
  tokio's `test-util` feature into the library, and timers still fire on whole milliseconds.
- **Keeping the recorded ingest sequences**: rejected. The sessions' answers interleave with the
  records, the host sees an input only by the clock's sequence moving, and a `Cycle` record must
  name the run's own sequence.
- **Stamping a record at the clock's now**: rejected. It rounds the recorded instant the
  consumer's model reads up to the next millisecond; the restamp keeps it, moved on only when
  ingest order needs it.
- **A consumer callback at the end instead of the controls**: rejected for now. The controls are
  all a closed loop needs to end, and typed.

## Consequences

- chaiwala-rs's backtest builds, on a paused current-thread runtime, an
  `IngestClock::replayed` with the journal's wall time at its origin, shadow's sessions on it
  (an `ExecSession::in_process` over a `SimPeer` per bracket and a `PeerFeed`), an `MdReplay`
  of the market-data connection with its input handler set, and runs `Member::md_replay` and the
  sessions on the `ShardHost` with its decide pass. Journals set on the host and the sessions
  hold the run's `Cycle` records and order entry; the market data stays in the source journal.
- A replay run on a runtime that is not paused waits in real time, its answers read when the
  runtime wakes, so two runs can differ; tokio offers no way to tell, so nothing refuses it.
- A journal of several market-data connections replays with one `MdReplay` member per
  connection over the same records, each waiting for its own; one of them sets the input
  handler.
- `tests/closed_loop.rs` in `fbc-sim` proves it (FBC-066n).

## What would show this was wrong

- Two closed-loop runs of one journal differing in a command, an outcome, a fill, a stamp or a
  `Cycle` record.
- A replayed stamp's wall time that does not follow from the recorded stamps: a wall clock read.
- With whole-millisecond instants, an answer due before a record read after it, or a command
  answered other than its written instant plus the latency.
- A consumer's backtest that needs latencies finer than a millisecond.
