# 0128 — Separate public market-data and private execution rate declarations

Status: accepted
Date: 2026-10-26

## Context

FBC-bg3p found that `MdSession` and `ExecSession` checked one `VenueCaps.limits` list.
QFEX documents a per-user general bucket and a separate cancel bucket for authenticated
trading WebSocket messages, but publishes no numeric budget for its anonymous public MDS.
Putting those user-scoped limits in the common list would require a provider-user key for
public recording and charge public subscriptions against the private budget. Leaving the
list empty would make private `ExecSession` reject a correctly keyed limiter. Decision 0123
requires primary and subaccounts of one provider user to share their private budget;
decision 0126 handles provider `RateLimited` feedback. QFEX order entry remains recording-only.

## Decision

`VenueCaps.limits` declares market-data limits. Add mandatory
`VenueCaps.exec_limits: Option<Vec<RateLimit>>`: `None` means order entry uses the same
declaration; `Some(limits)` means it has a distinct declaration, including `Some(vec![])`
when it has no declared numeric limit. `MdSession` checks the public list and `ExecSession`
checks the private list or its fallback; a limiter mismatch fails before endpoint planning.
This field may be present while `caps.exec` is `None`, so a recording-only venue can declare
the private budget before its execution codec is accepted.

QFEX declares no numeric public MDS limit and declares two user-scoped private buckets,
general and cancel, each 12,000 provider weight units per 60 seconds. FBC expresses weights
in tenths, so each bucket has 120,000 local units. The consumer supplies the same attested
`RateUserKey` to `RateLimiter::for_user` for the provider user's primary and subaccounts,
and distinct keys for distinct users, as 0123 requires. The local sliding-window reserve is
an admission guard, not a promise that QFEX's EMA will accept a request; 0126's refusal
handling remains required.

## Alternatives

- Put the private buckets in `limits`: rejected because anonymous MDS would require a user
  identity and spend an unrelated private allowance.
- Leave `limits` empty and bypass the execution limiter check: rejected because a private
  session could start with an unshared or wrongly scoped budget.
- Add a venue-factory override rather than capability data: rejected because callers could
  inspect one declaration while runtime admission used another.

## Consequences

Every `VenueCaps` constructor must state the order-entry relationship explicitly. Existing
venues choose `None` and retain their common declaration. QFEX public recording can start
without a provider-user key, while a future private session must use its distinct keyed
budget. The private codec still must charge actual wire messages in scaled units, handle
`RateLimited` replies, and pass order-truth and provider acceptance gates before execution
is promoted. This decision does not change `Readiness::Record` or `caps.exec: None`.

## What would show this was wrong

QFEX documentation or provider evidence that public MDS uses the authenticated user's
trading buckets, or that the two private buckets have a different scope, would require a
new record. A future venue needing more than one declaration per traffic class would also
reopen the capability shape.
