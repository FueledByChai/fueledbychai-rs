# 0123 — Provider-user rate budgets share across QFEX accounts in one owner process

Status: accepted
Date: 2026-10-22

## Context

QFEX documents one general and one cancel rate budget per provider user across connections
and API keys. FBC-e5bd found that decision 0030's clone sharing can undercount when the
consumer constructs separate limiters for a primary account and subaccounts. A QFEX account
key is not a provider-user identity. The public QFEX adapter remains recording-only while
FBC-lqa8 develops order entry.

## Decision

Add `LimitScope::User`. `RateLimiter::new` refuses a user-scoped limit; the consumer must use
`RateLimiter::for_user(user_key, limits, reserve)`. The caller assigns one opaque
`RateUserKey` to the provider user and reuses it for every API key, connection, primary account
and subaccount of that user. Different users receive different keys. Independently constructed
limiters of the same key share one process-wide budget, even across shard threads and venue
instance IDs; keys for different provider users must be distinct across the process;
different declarations or safety reserves for that key fail closed. The registry retains
the budget for the process lifetime so dropping the last session cannot reset a live window.
The keyed constructor accepts only
user-scoped limits; a mixed declaration fails rather than silently sharing account or
connection buckets incorrectly.

For v1, one process owns private traffic for each QFEX provider user. An external owner lease
must prevent a second process from trading that user, because a process-local budget cannot
count another process's traffic. This library does not infer a user from an API key or a
subaccount UUID; the consumer attests the mapping. The QFEX order-entry adapter stays
`Readiness::Record` until its charges, `RateLimited` replies and acceptance are proved.

## Alternatives

- Treat each `AccountKey` as a separate budget: rejected because QFEX aggregates its
  primary account and subaccounts, allowing the consumer to exceed the user's allowance.
- Require the consumer to clone one limiter: rejected because separate configurations can
  accidentally create separate budgets.
- Add a distributed rate service now: not taken for v1's single owner process; the owner lease
  is the explicit deployment boundary. Multiple active owner processes would need a new record.

## Consequences

The runtime serializes each user's charge, refusal and response count across its sessions.
The consumer must assign stable, distinct user keys and hold one owner process per user.
QFEX's private codec must declare the two user-scoped limits and scaled message weights
before order entry is enabled; its public MDS has no documented numeric limit.

## What would show this was wrong

A QFEX `RateLimited` response on admitted traffic whose complete user traffic was within the
declared budget, or a deployment requiring concurrent writers in separate processes for one
user, would reopen the model. An observed provider identity that can replace the caller's
attestation would also justify a new record.
