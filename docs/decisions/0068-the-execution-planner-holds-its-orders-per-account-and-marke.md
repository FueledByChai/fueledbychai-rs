# 0068 — The execution planner holds its orders per account and market, and builds nothing through a registry that does not hold them, augmenting 0065

Status: accepted
Date: 2026-10-07

## Context

0065 rule 2 has the planner remember the orders it placed "at each market, side and level"
and free a level once its order is seen terminal in the `Registry`. `ExecutionPlanner::new`
takes only the consumer's thresholds, while `plan` takes the registry and the account on every
call, and its levels were keyed by market alone: an order the passed registry did not hold
freed its level as a terminal one would. Codex's P1 on PR #103 (FBC-0j3), filed as FBC-0qgb:
one planner planning account A, then account B on the same market through B's registry, drops
A's levels; A's orders keep resting and counting in A's caps but are never diffed again, never
cancelled when a level is pulled or in Exit (0065 rule 8), and the next pass for A places new
orders at their levels. The same happens to one account whose registry is rebuilt while the
planner lives on. Neither 0065 nor the planner's documentation restricted a planner to one
account or one registry.

## Decision

1. **Per account and market.** The planner holds the orders it placed per account, market,
   side and level; a pass for one account reads and frees only that account's levels on the
   market. `ExecutionPlanner::order_at` names the account. The consumer passes each account's
   own registry with that account; one planner may serve several accounts.
2. **A registry that does not hold the planner's orders.** A `Registry` never forgets an
   order, so one that does not hold an order the planner placed for the account on the market
   is not the registry it was placed through. Such a pass frees nothing and builds nothing for
   the market, reporting each such level as `PlanRefusal::NotInRegistry` with the order's
   client id; the planner keeps those levels, and a pass through the registry that holds them
   plans them as before. Only an order the registry holds and shows terminal frees its level.

## Alternatives

- Bind the planner to one account at construction and refuse a pass for any other: rejected
  as the only fix. It also stops a planner from serving two accounts, which nothing in 0005
  or 0065 forbids, and on its own it does not catch one account's registry being rebuilt.
- Hold a level whose order the registry does not hold, and plan the other levels: rejected.
  Placing through a registry that does not count the orders still resting lets the caps judge
  new orders without them.
- Treat an order the registry does not hold as terminal (as before): rejected; that is the
  failure.

## Consequences

- A consumer running several accounts can share one planner; each account's book is diffed
  against that account's orders only.
- A planner kept across a rebuilt registry plans nothing on a market where it holds orders the
  new registry does not know; the consumer builds a new planner with the new registry, and the
  orders the old one placed are orphans for the resync (0055), as any earlier run's are.

## What would show this was wrong

- A pass for one account that cancels, amends or places over another account's order, or
  frees a level whose order still rests.
- A legitimate deployment in which a registry drops an order the planner placed while it can
  still rest (then the registry-identity rule needs a different signal).
