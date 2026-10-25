# 0126 — WebSocket rate refusals hold the shared provider budget without settling requests

Status: accepted
Date: 2026-10-25

## Context

FBC-ywjb must handle QFEX's `RateLimited` WebSocket error. Its `incoming_message` echo may
identify a general or cancel operation, but it is untrusted, may contain credentials, and does
not prove which in-flight command the venue acted on. Decision 0030 has local sliding-window
buckets and a safety reserve; 0123 shares user buckets across primary and subaccounts. QFEX
documents an EMA and a retry-after hint, so local admission alone does not predict refusal.

## Decision

A codec emits a budget-only `Effect::RateLimited` with the parsed duration and an operation only
when the echo names a known bucket. The runtime holds that bucket in the shared limiter until
the hint expires; a missing or invalid hint uses at least the declared window. With absent or
ambiguous echo it holds every frame budget. The effect carries no RPC id, settles no outcome,
and never resends a command; unanswered sent RPCs take the existing `Unknown` deadline path.
The entire inbound frame is redacted when it can echo a request.

## Alternatives

- Reconnect on every error without budget feedback: all accounts could immediately hit the
  same provider-user limit again.
- Treat the echo as an order acknowledgement or safe retry instruction: the echo names the
  incoming message, not whether an order reached matching or which in-flight RPC it answers.

## Consequences

QFEX private decoding must emit this effect before processing its ordinary private state.
The runtime's per-bucket hold applies after local usage and reserve checks, across sessions
sharing the provider-user key. An unidentifiable error also delays cancels; this is deliberate
until the provider gives a trustworthy bucket identifier. The private adapter remains
recording-only while order, fill, resync and provider acceptance gates remain open.

## What would show this was wrong

Provider evidence that the echo or retry hint has a different meaning, or a reproducible case
where an unclassified error must allow Safety traffic to unwind, would require a new record.
