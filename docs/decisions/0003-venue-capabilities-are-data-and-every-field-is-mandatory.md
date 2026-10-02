# 0003 — Venue capabilities are data and every field is mandatory

Status: accepted
Date: 2026-10-02

## Context

The Java library has a capability model that exists but is unused; behaviour that differs by
venue is chosen by `BrokerType` switches and venue-specific imports (design §2). Each new venue
then means finding every switch. Venues really do differ: amend support and semantics,
cancel-before-ack, batch sizes, client-id formats, nonce scope, fee sign, flag conflicts such as
Hibachi's (PostOnly, ReduceOnly), book cadence and continuity, rate-limit scopes (design §4.5).

## Decision

Capabilities are data, and every field is mandatory (design D3, §4.5). `VenueCaps` and its parts
(`OrderCaps`, `AmendCaps`, `FillCaps`, `MatchingCaps`, `MdCaps`, `BookCaps`, rate limits,
readiness ceiling) have no `Default` and are not `#[non_exhaustive]`, so a new field breaks the
build of every adapter until each one declares its value. Each declared value cites a venue
document or a recorded fixture (design §6, step 2).

Within the library, capabilities are read in three places: `fbc-runtime` (market data, limits,
keepalives, nonce scope, cancel-on-disconnect), `DecodeScope` in `fbc-core` (the fee sign and
the client-id format, 0004), and `fbc-oms` (the `ExecutionPlanner`, which turns a desired book
into commands under `OrderCaps`, and the permits, which take `OrderCaps`; design §4.9, §4.10).
Outside the library, the consumer reads them as numbers and flags (design D3: the consumer's
`MicroFacts` builder turns them into plain numbers for its strategy), and `VenueCaps` is public
for that. Nothing branches
on a venue name. A behaviour the model cannot express grows the model: a new mandatory field
and a decision record.

## Alternatives

- Optional capabilities with defaults: rejected. A default is a guess about a venue nobody
  checked, which is how the Java model drifted out of use.
- Venue-name branches where behaviour differs: rejected. They scatter one venue's truth over
  many files.

## Consequences

- Adding a field touches every adapter in the same change; the compiler lists them.
- The conformance kit's `caps_truthful` test holds adapters to what they declare: an absent
  capability yields `NotSent(Unsupported)` with zero bytes sent.
- Consumers read venue differences as numbers and flags, never as venue names.

## What would show this was wrong

- A venue name in a branch in `fbc-core`, `fbc-runtime`, `fbc-book`, `fbc-oms`, `fbc-journal`
  or `fbc-sim`, introduced because a capability could not be expressed.
- A conformance fixture recorded from a live venue contradicting a declared capability that the
  model had no way to state correctly.
