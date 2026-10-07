# 0068 — The execution planner holds its orders per account and market and plans each account through one registry, augmenting 0065

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
   market. `ExecutionPlanner::order_at` names the account. One planner may serve several
   accounts.
2. **One registry per account, one account per registry.** Each `Registry` carries an
   identity drawn fresh when it is built. The planner binds an account to the registry of its
   first pass. `ExecutionPlanner::plan` returns `Result<Plan, PlanError>`: a pass for the
   account through another registry (`PlanError::OtherRegistry`: another account's, or one
   rebuilt) or through a registry bound to another account (`PlanError::OtherAccount`) is
   refused before anything is read, freed or built. Only an order the bound registry shows
   terminal frees its level.

## Alternatives

- Bind the planner to one account at construction: rejected. It stops a planner from serving
  two accounts, which nothing in 0005 or 0065 forbids, and on its own it does not catch one
  account's registry being rebuilt.
- Judge the registry by whether it holds the planner's client ids (the first version of this
  record's fix, PR #107): rejected on Codex's P1 there. Namespace leases are scoped by account,
  so two accounts may lease the same namespace and mint the same client ids; B's slot then
  finds A's order in A's registry, and A's terminal order frees B's level.
- Hold a level whose order the registry does not hold, and plan the other levels: rejected.
  Placing through a registry that does not count the orders still resting lets the caps judge
  new orders without them.
- Treat an order the registry does not hold as terminal (as before): rejected; that is the
  failure.

## Consequences

- A consumer running several accounts can share one planner; each account's book is diffed
  against that account's orders only, through that account's registry.
- A planner kept across a rebuilt registry refuses every pass for the account; the consumer
  builds a new planner with the new registry, and the orders the old one placed are orphans for
  the resync (0055), as any earlier run's are.
- A consumer must handle `PlanError`; ignoring it builds nothing, which fails closed.

## What would show this was wrong

- A pass for one account that cancels, amends or places over another account's order, or
  frees a level whose order still rests.
- A legitimate deployment that must move an account to a new registry without a new planner
  (then the binding needs an explicit hand-over).
