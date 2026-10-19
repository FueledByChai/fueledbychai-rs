# 0105 — An order-entry session bounds the commands waiting for its connection and its requests in flight by the consumer's Normal bound plus a Safety reserve, refusing past them NotSent(Backpressure) and NotSent(RateBudget), augmenting 0053, 0057 and 0073

Status: accepted
Date: 2026-10-19

## Context

Design §5.4 names a bound and an overflow policy for every queue on the hot path. Two of them
are an order-entry session's: the outbound queue per connection ("8 frames Normal + 4 reserved
Safety", overflow `NotSent(Backpressure)`, journaled as a `WriteResult`, the OMS keeping the
intent) and the RPC table ("max in-flight per venue, Safety reserve", overflow
`NotSent(RateBudget)`). Until FBC-hwn neither was bounded: `ExecOrders` queued every
submission for the session's next turn (0057), so while a write waited on a peer that stopped
reading (0036) the handlers of the shard's other members (0100) could queue without limit, and
the session wrote any number of requests the venue had not answered. The ticket fixes the
numbers as the consumer's configuration (0009), not the design's example values. Paradex
states no retry time for its rate-limit code 42901, so the samples wait their own
`RATE_LIMITED_WAIT` before re-sending (PR #158): a refusal here carries no retry time either.

## Decision

1. **A bound with a reserve.** `ReservedBound::new(normal, reserve)` (no default, a Normal
   bound of zero refused, the two together fitting a `u32`). Normal traffic is admitted while
   fewer than `normal` entries are held; Safety traffic while fewer than `normal + reserve`
   are, as the rate buckets' safety reserve admits it (0030, 0073). Past the whole bound Safety
   traffic is refused too, as a bucket refuses a cancel once it is empty: a bound that a cancel
   could pass would not bound the queue. The class is the one 0073 charges the buckets in: an
   authorized order command's own (a cancel, a cancel-many, an instrument cancel-all and a
   reducing order are Safety), every control command Normal.
2. **The outbound queue, per connection.** `ExecSessionConfig::outbound` bounds the commands
   submitted through the session's `ExecOrders` and not yet taken by a turn. One submitted past
   the bound its class may use is refused as it is submitted:
   `ExecOrders::submit` (or `submit_control`) returns `SubmitRefusal::Backpressure { rpc }`, the
   runtime's form of `NotSent(Backpressure)`, naming the request id it was given and keeping
   nothing of it, so nothing grows past the bound however many are submitted while a write waits
   (Codex P1 r4312640254 on PR #174). The refusal is journaled at once as
   `Record::WriteResult { rpc: Some(rpc), result: NotSent(Backpressure) }` under its class, on
   the epoch the session is in or waits to open (once an epoch that dropped is journaled closed,
   the one it waits to open; Codex P2 r4312949575), and counted in
   `ExecCounters::backpressure_refusals`. Nothing of it reaches the codec, a nonce source or a
   socket, and its authorization is dropped. The OMS keeps the intent: the consumer settles it
   exactly as an `on_submitted` receipt for `rpc` not sent for `Backpressure` (a place or a
   batch item by `NotSent(Backpressure)` through `Registry::on_outcome`; an amend by
   `Registry::amend_sent` under `rpc` and then that outcome, which releases the reservation its
   build made, since `Registry::amend_not_submitted` needs the command the authorization spent;
   a cancel left nothing pending) and plans again (Codex P1 r4312949568). The shard
   host submits a ladder command only once every session holds nothing, so with a Normal bound
   of at least one its own commands always fit.
3. **The table of requests in flight, per venue or per connection.** `InFlight::new(bound)`,
   given as `ExecSessionConfig::in_flight`, counts the requests written (or asked as an HTTP
   request, 0081) and neither answered nor timed out. Clones share one count, so the consumer
   bounds a venue by handing one table to each of its sessions, or a connection by building one
   per session. At the encode of a submitted command, after fbc-oms's re-check (0062) and before
   any nonce is reserved, a command whose class finds no room is `NotSent(RateBudget)`, nothing
   reserved or written, counted in `ExecCounters::in_flight_refusals`; it is not journaled as a
   write result, as a bucket's `NotSent(RateBudget)` is not (0073). A place taken is held until
   the first event that answers the request, the call to `on_http` for an HTTP query, or its
   deadline's `on_rpc_timeout`; one encoded and never written (the codec's refusal, the
   buckets', an epoch that ends first) is given back at once. The session's own requests (its
   cancel-on-disconnect arm, 0058) are counted without being refused, since refusing the arm
   would fail the epoch. A session's run that ends, a run whose future is dropped (the guard
   that gives the places back lives in the run's future; Codex P1 r4312949559), or a session
   dropped, gives back every place its requests held: nothing more is reported for them, and a
   table shared with other or later sessions must not leak.

## Alternatives

- **The design's numbers as defaults.** Refused by 0009 and the ticket: a consumer submitting a
  ladder of many levels at once needs another bound than a single-quote sample.
- **Report the refusal through `on_submitted` on the next turn.** That keeps one path for every
  not-sent reason, and PR #174 first did it. But the record of each refusal waits in the queue
  until the turn, and while a write waits on a stalled peer the turn does not come, so the
  refusals grow without limit. That is the growth the bound exists to prevent (Codex P1
  r4312640254). A second bound on the refusals would need its own overflow policy, which could
  only be this one. Refusing as it is submitted is also how the consumer-input ring refuses a
  push into a full ring (0103).
- **Hand the authorization back with the refusal.** The consumer could then submit it again
  unchanged. But a refusal that carries the command changes the error type of every submission,
  and fbc-oms has no call that settles an authorization: an amend's reservation is released by
  `amend_not_submitted`, which needs the command the authorization spent, or by an outcome under
  a request id. A request id settles every kind of command the way a not-sent receipt does.
- **A refusal naming no request id.** PR #174's second head did this. An amend so refused left
  fbc-oms's reservation of it impossible to settle, so its order took no other amend until it
  ended (Codex P1 r4312949568).
- **Let Safety traffic past the whole outbound bound, as the journal's reserve does.** The
  journal never blocks and drops what it cannot keep; an outbound queue that a burst of cancels
  could grow without limit is the defect the bound exists to close. The consumer sizes the
  reserve for its worst sweep.
- **Count the in-flight table per session only.** Simpler, but the design bounds it per venue;
  the shared handle lets the consumer choose either.

## Consequences

- Every `ExecSessionConfig` states both bounds; the owner-run samples state 16 Normal and 16
  more for Safety for each (`fbc-examples`), the tests their own. The samples' glue treats a
  refused submission as a problem, as it treats `SubmitRefusal::OtherAccount` and `Ended`.
- A consumer's glue handles `SubmitRefusal::Backpressure` beside `on_submitted`'s not-sent
  receipts: two paths, so that the queue's memory stays bounded.
- A refusal is never retried by the runtime: the consumer re-plans (0005, 0013 rule 1), after
  its own wait where the venue states none (PR #158).
- The conformance order-entry checks build sessions with ample bounds, so their results do not
  change.

## What would show this was wrong

- A live consumer whose protective cancels are refused `NotSent(Backpressure)` or
  `NotSent(RateBudget)` although the bound it configured covers its worst sweep: then the
  reserve is being taken by something it should not be, or the classes are wrong.
- A shared table whose count stays above zero once every session sharing it has ended.
