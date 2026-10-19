# 0104 — An order-entry session bounds the commands waiting for its connection and its requests in flight by the consumer's Normal bound plus a Safety reserve, refusing past them NotSent(Backpressure) and NotSent(RateBudget), augmenting 0053, 0057 and 0073

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
   the bound its class may use is given its request id as before, but nothing of it is kept
   (an authorization is dropped unspent, which fbc-oms's held-place rule releases, 0084): its
   refusal waits in its place and is reported to `ExecHandler::on_submitted` as
   `NotSent(Backpressure)` on the session's next turn, in submission order, connected or
   waiting to reconnect, and journaled just before as
   `Record::WriteResult { rpc, result: NotSent(Backpressure) }` under its class on the epoch
   the turn runs on. Nothing of it reaches the codec, a nonce source or a socket. The OMS keeps
   the intent: the planner sees the order not sent and plans it again. Counted in
   `ExecCounters::backpressure_refusals`. A refused command is not one the shard host can take
   back (`ExecOrders::withdraw` finds none); the host submits a ladder command only once every
   session holds nothing, so with a Normal bound of at least one its own commands always fit.
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
   would fail the epoch. A session's run that ends, or a session dropped, gives back every
   place its requests held: nothing more is reported for them, and a table shared with later
   sessions must not leak.

## Alternatives

- **The design's numbers as defaults.** Refused by 0009 and the ticket: a consumer submitting a
  ladder of many levels at once needs another bound than a single-quote sample.
- **Refuse at submit with a `SubmitRefusal`.** `ExecOrders::submit` would return the refusal
  at once, but the consumer's fbc-oms glue learns what became of a command only through
  `on_submitted`, and `NotSent(Backpressure)` is already a `SubmitOutcome` fbc-oms applies
  (`place_not_submitted`). Reporting it there keeps one path for every not-sent reason.
- **Keep the refused command in the queue until its turn.** The authorization would be held
  longer for nothing, and the queue would hold what it refused; a refusal keeps only its id and
  class.
- **Let Safety traffic past the whole outbound bound, as the journal's reserve does.** The
  journal never blocks and drops what it cannot keep; an outbound queue that a burst of cancels
  could grow without limit is the defect the bound exists to close. The consumer sizes the
  reserve for its worst sweep.
- **Count the in-flight table per session only.** Simpler, but the design bounds it per venue;
  the shared handle lets the consumer choose either.

## Consequences

- Every `ExecSessionConfig` states both bounds; the owner-run samples state 16 Normal and 16
  more for Safety for each (`fbc-examples`), the tests their own.
- A refusal is never retried by the runtime: the consumer re-plans (0005, 0013 rule 1), after
  its own wait where the venue states none (PR #158).
- The conformance order-entry checks build sessions with ample bounds, so their results do not
  change.

## What would show this was wrong

- A live consumer whose protective cancels are refused `NotSent(Backpressure)` or
  `NotSent(RateBudget)` although the bound it configured covers its worst sweep: then the
  reserve is being taken by something it should not be, or the classes are wrong.
- A shared table whose count stays above zero once every session sharing it has ended.
