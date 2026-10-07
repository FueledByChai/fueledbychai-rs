# 0073 — An order query is normal traffic at the safety floor, and the cancel-on-disconnect arm and the resync may use the reserve

Status: accepted
Date: 2026-10-07

## Context

0030 keeps a consumer-configured safety reserve in every rate bucket: normal traffic stops where
a bucket would drop into it, safety traffic may use it until the bucket is empty. Design §4.10
step 7 names who the reserve is for: cancels and reducing orders, so that a burst of quotes can
never take the budget a cancel needs. FBC-e8i charges an order-entry session's requests to the
buckets by their `RateCharge` (0018) and reports a request the buckets refuse
`NotSent(RateBudget)` with nothing written (0057).

`fbc_core::VenueCommand::traffic_class` labels more than cancels and reducing orders Safety: an
order query (`VenueCommand::Query`), the cancel-on-disconnect arm (`ArmCancelOnDisconnect(true)`)
and the dead-man refresh too, and `Effects::carry_request` requires an encode's effects to carry
that label. A label chosen for latency and journaling (a Safety frame is written even when the
journal is full, 0006) is not by itself a claim on the reserve. The ticket had to decide whether
order queries and the arm may use the floor, since §4.10 names only cancels and reducing orders.

The two differ in how many there can be. The Unknown ladder (0005) queries an order for as long
as it stays unresolved, and many orders can go Unknown at once (a stalled write, a reconnect, a
venue outage), so queries come in storms the runtime does not bound. The arm is sent by the
session itself, once per connection epoch (0058), and epochs are paced (`ReconnectPacing`) and,
where the venue counts them, charged `Connect`; the codec's resync, also once per epoch, is the
same.

## Decision

1. **An order query is charged as normal traffic.** The order-entry session charges a
   `VenueCommand::Query`'s frames as `TrafficClass::Normal`, whatever its label: at a bucket's
   safety floor it is `NotSent(RateBudget)`, nothing written, counted under the scope that
   refused it, as a normal place or amend is. The frames keep their Safety label for everything
   else (the journal, tick-to-wire). The fee query is normal already.
2. **The arm and the resync keep the reserve.** The session's `ArmCancelOnDisconnect(true)`
   and the codec's resync are charged as their codec labels them, Safety for the conformance
   toy's and Paradex's, so they may use the reserve: an epoch opened with its buckets at the
   floor still arms its protection and resyncs, and so still takes cancels and, once the
   buckets refill, places. The dead-man refresh would be charged as labelled too; no session
   takes a dead-man venue (0058).
3. **Cancels and reducing orders use the reserve, as §4.10 says.** A cancel, a cancel-many, an
   instrument cancel-all, and a place, batch or amend that only reduces the position are Safety
   by `traffic_class` and are charged so, until the bucket is empty.

The other traffic of a session (a codec's login, keepalive, own subscriptions) is charged as the
codec labels it, as 0030 already has it.

## Alternatives

- Every Safety label uses the reserve (the code before this record): a storm of Unknown-ladder
  queries could empty the reserve and leave a kill-switch cancel `NotSent(RateBudget)`, the
  failure §4.10 step 7 exists to prevent.
- Relabelling `VenueCommand::Query` Normal in `fbc-core`: the label also chooses the journal's
  and tick-to-wire's treatment, and the change would reach every codec and consumer; the budget
  is the runtime's concern, so the runtime decides it.
- A separate reserve for queries inside the safety reserve: a second consumer setting with no
  observed need; refused until a venue's numbers show one.
- Arming as normal traffic: an epoch opened at the floor would fail its arm, end as a drop and
  reconnect, which costs another `Connect` and leaves cancels and places waiting, to save at
  most one unit per epoch.

## Consequences

- While normal traffic holds a bucket at its floor, the Unknown ladder's queries wait with the
  places: an Unknown order resolves later, and fbc-oms keeps counting it at its worst case
  (0005), so the caps stay safe; the consumer's quoting rate, not the query, is what to slow.
- A reconnect storm can take at most one arm and one resync per epoch from the reserve, bounded
  by the pacing.
- When order entry over HTTP arrives (FBC-m8vm), an order query's HTTP request must be charged
  the same way: normal, whatever its label.

## What would show this was wrong

- A testnet or live session in which orders stay Unknown long enough to matter because their
  queries were refused at the floor while quoting held the bucket there: then queries need a
  share of the reserve of their own.
- A venue whose arm or resync weighs enough that a reconnect storm drains the reserve before a
  cancel: then those too stop at the floor, or the pacing must keep them under the reserve.
