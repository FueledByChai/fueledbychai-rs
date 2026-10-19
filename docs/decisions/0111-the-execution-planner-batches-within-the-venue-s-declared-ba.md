# 0111 — The execution planner batches within the venue's declared batches, builds nothing a venue mode or trading status refuses, and withholds normal commands at each rate scope's safety floor, augmenting 0005 and 0065

Status: accepted
Date: 2026-10-19

## Context

0005 makes the `ExecutionPlanner` the one planner: it orders cancels, then reducing orders,
then amends, then adds, "and normal traffic stops at each rate scope's safety floor". 0065
built the diff and left batching, venue modes and the safety floor out (design §4.10 steps 6
and 7). 0002 and 0018 put rate limits with a safety reserve in `fbc-runtime`'s limiter, which
refuses normal traffic past the reserve at send; but a place the planner builds is registered
PendingNew and counted against both caps (0005's I6) from its build, so a place the limiter
then refuses held exposure for nothing and was built only to be ended not sent. `fbc-core`'s
`TrafficClass` says a batch is safety traffic only when every item is reducing, "so the planner
keeps reducing orders out of mixed batches". Venues report a venue mode for the account or one
market (`ExecEvent::Mode`) and a trading status per instrument (`InstrumentSpec::status`);
nothing in the library read either. For Paradex, 0054 declares `batch_place` and
`batch_cancel` of 10, `batch_cancel` by venue id only, a binding per-IP bucket of 1500
requests a minute counting REST, queries and order methods together, and a batch charged its
item count.

## Decision

1. **The pass takes the venue's facts.** `ExecutionPlanner::plan` takes a `PlanVenue`: the
   venue's `OrderCaps`, the account's and the market's `VenueMode` as last reported, the
   market's `TradingStatus`, a `RateView`, and a `Batching` choice. Every field is the
   consumer's and none has a default (0009): the consumer reads the modes from the venue's
   order events, the status from the instrument spec, and the view from its runtime's
   limiter. `on_fill` builds only cancels and keeps its signature.
2. **Modes and status gate places and amends only.** No place and no amend is built for a
   market whose trading status, account mode or market mode refuses it, judged in that order
   and reported `PlanRefusal::Status` or `PlanRefusal::VenueMode`: Cancel-only, Halted and
   Delisted refuse every place and amend; Post-only refuses an order that is not post-only;
   Reduce-only refuses one without the venue's reduce-only flag (an emulated reduce-only order
   or a consumer's reducing classification is not what the venue checks). An amend is judged by
   the flags its order was placed with. Cancels are built in every mode and status, as they are
   in every market state (0012): a stale mode never holds back a cancel, and a cancel a halted
   venue refuses costs at worst a reject. The check is made as the place or amend is built,
   before the market's state, the caps and a reducer's wait for an emulated order's cancel, so
   a refused place or amend builds nothing and cancels nothing on its account. The cancels a
   level's decision calls for whatever the new quote (a replace, Exit's, an emulated order's
   overshoot once its quote counts it reducing, a level no longer wanted) are cancels, and go
   out in every mode; the quote that would replace the order is then refused when built. An
   amend refused here, or at the floor (rule 3), leaves its order resting as it was, at its old
   price, where a replace has already cancelled it; the planner never falls back from a
   refused amend to a cancel (0065).
3. **The safety floor.** A `RateView` lists `RateScope`s, each a bucket's operations
   (`RateLimit::ops`), the one instrument a per-pair bucket counts (`None` for a shared
   bucket), its units left in the window, and the consumer's floor for it (`RateScope::of`
   builds one from a declared limit and the units used). A normal command (`TrafficClass::Normal`
   as `VenueCommand::traffic_class` classes it: a place that does not reduce, an amend neither
   reducing nor reduce-only) is withheld, never built, when charging it would leave a scope
   that counts it below its floor, reported `PlanRefusal::RateFloor { scope }` with the scope's
   index. Cancels, cancel-manys and reducing places and amends are never withheld; the runtime's
   limiter judges them. Every command the pass builds, safety traffic included, is charged
   against the view as it is built, so the later ones (amends and adds, after the cancels and
   reducing orders) are judged with it counted. A command is charged its item count, as
   Paradex's codec charges it (0054): a venue that charges a batch once costs less than
   counted, never more. A view with no scope tracks none and withholds nothing.
4. **Batching is the consumer's choice, within the venue's declaration.** Under
   `Batching::One` every place and cancel is a command of its own, as before. Under
   `Batching::Declared`, consecutive places of one stage, built under one guard, go in one
   `PlaceBatch` of at most `batch_place.max_items`; one stage is one traffic class, so a
   reducing order never shares a batch with an add, and any other command ends the run, so the
   output keeps 0005's order. Each place is still built alone through the one pre-trade path,
   judged with the earlier ones counted, then the run is merged into one held command whose
   authorization re-judges every item against both caps, as `Registry::place_batch`'s does. A
   synthetic IOC's place is never batched: its cancel follows it. The pass's cancels whose
   order the venue's cancel-many can name by a reference it declares (`batch_cancel.refs`) go
   in cancel-manys of at most `batch_cancel.max_items`; any other goes as a single cancel, or
   waits for its acknowledgement as before, after the cancels held before it, so the cancels
   too keep 0005's side and level order. A run of one goes as a single command (a cancel-many
   of one only where the single cancel cannot name the order). A batch with a size below two
   batches nothing. A batch's `Planned` describes its first order and lists the others in
   `Planned::more_items`; the consumer reports each item's outcome, and each cancel-many item's
   `cancel_sent`, as for a single command.

## Alternatives

- Batching whenever the venue declares batches: not taken. The owner's mainnet test programs
  run on one order per command, and a consumer moving to batches does so by its own choice,
  with the session's per-item outcomes (0089) proven for it; `Batching::Declared` is the
  library's, tested here.
- Gate on modes and the floor in the runtime alone: rejected. A place refused at send was
  registered and counted at its build; the planner is where nothing gets built.
- Hold the modes and status in the planner from events: not taken. A market with no report
  would need a default mode; the consumer states them per pass.
- Gate cancels in Halted: rejected for 0012's reason; a cancel the venue refuses is cheap and a
  lost mode report must never strand an order.
- Batch the places with `Registry::place_batch`: not taken. The planner sizes emulated
  reduce-only orders, crossing intents and overshoot waits from the registry after each place,
  so each is built alone first; merging the held places is equivalent to building the batch.

## Consequences

- Every caller of `plan` passes a `PlanVenue`; the testnet programs and the Paradex rehearsal
  pass Normal modes, Trading, no scope tracked and `Batching::One`, which is the behaviour they
  had: the runtime's limiter still holds their normal traffic at its reserve.
- A consumer that reads the runtime's limiter into a `RateView` each pass stops building normal
  commands before the limiter would refuse them, so a withheld add holds no exposure.
- A consumer under Post-only on a venue that has no post-only with reduce-only, quoting with
  `ReduceFallback::KeepReduceOnly`, cannot exit in that mode; one that emulates reduce-only
  cannot exit under Reduce-only. Both are the venue's rules, reported as refusals.

## What would show this was wrong

- A place or amend of ours sent while the reported status or mode refused it.
- A cancel held back by a mode, a status or the floor.
- A normal command built while a scope the consumer filled would have been left below its
  floor, or a venue rate-limit reject on traffic the view counted within its floors.
- A batch above the venue's declared size, a cancel-many item naming a reference the batch does
  not declare, or a reducing order sharing a batch with an add.
