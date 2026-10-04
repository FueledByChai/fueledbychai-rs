# 0030 — Rate buckets are sliding windows per declared limit and scope key, with a consumer-configured safety reserve; what each refusal does

Status: accepted
Date: 2026-10-04

## Context

0002 puts rate limiting with a safety floor in `fbc-runtime`; 0018 gives every frame, HTTP
request and keepalive a `RateCharge` and the limits a `Connection` scope and a `Connect`
operation, and leaves the limiter to FBC-bel. The deployment box shares one egress IP with the
Java processes, so an undercount risks an IP ban for all of them; the size of the safety floor
is the consumer's configuration (`rs.ratelimit.safety_reserve` in the design), never a number
in this library (0009). A market-data session can be refused at four points: a connection
attempt, a subscribe call, an HTTP request and any other frame, and each needs a defined
outcome. `LimitScope::AddressVolume` (Hyperliquid's volume-earned requests, design §14 M5) has
no venue that needs it yet.

## Decision

1. **One bucket per declared limit and scope key.** `RateLimiter` keeps, for each `RateLimit`
   a venue declares, one bucket for an `Account` or `Ip` limit, one per instrument for a `Pair`
   limit and one per connection epoch for a `Connection` limit (forgotten when the epoch ends).
   A request is charged its weight in every bucket whose limit counts it (0018's
   `RateLimit::counts`), all of them or none. A connection attempt is charged
   `RateCharge::one(Connect, None)` as normal traffic before the connector opens anything; so
   is the connection each HTTP request opens, together with the request and in its class
   (Codex r4179474175); no per-connection limit counts either.
2. **A bucket is a sliding window.** It holds what was charged in the last `per`, so no window
   of that length holds more than `units`. A refusal says when the refusing buckets will have
   room (`Refused::ready_at`), or that they never will (a weight over the cap).
3. **The safety reserve is a share the consumer configures.** `SafetyReserve::percent(p)`,
   `p` below 100, keeps `units × p / 100` of every bucket, rounded down, for safety traffic:
   normal traffic is refused once a bucket would go past `units` less the reserve, safety
   traffic once it would go past `units`. A batch charged together keeps out of the reserve of
   every bucket it charges if any of it is normal, even of a bucket only its safety requests
   fall in (Codex r4179380153). There is no default in code.
4. **What a refusal does in a market-data session.** A connection attempt waits for the
   buckets, then starts. The codec is asked once per subscribe call, and the call's frames are
   charged together; when the buckets refuse them, the call stays outstanding in the reconciler
   (its subscriptions pending, a change made meanwhile held for the call after it) and its
   effects wait, none of them executed, until the buckets have room for all of its frames, so
   a codec that changed its own state in `subscribe` never runs ahead of what the venue is
   sent (Codex r4179266579, r4179558357). Only frames that can never fit together (more than a
   bucket admits at once) go one by one, each when the buckets have room for it, with the
   frames left trying together again, so such a call still goes (Codex r4179474176). A
   subscribe call's frame that can never fit (its weight over the cap) ends the session with
   `RateError::NeverFits`: the call could never go, and settling it as sent would leave the
   reconciler believing what the venue was never told (Codex r4179682238); the ended
   connection's buckets are forgotten all the same (Codex r4179720972). An HTTP request
   comes back to `on_http` as `HttpFailure::NotSent`; one the runtime cannot make (a URL or
   header it refuses) does too, before anything is charged (Codex r4179682244). Any other
   frame is not written.
5. **Counting.** Every refusal is counted once under each scope whose bucket refused it, and
   every HTTP 429 or 418 under each scope its request was charged to (`RateCounts`), as its
   status arrives, so a body that then fails (over the limit, cut off) does not hide it (Codex
   r4179266570); not under the scopes of the connection it opened, which the venue did not
   reject (`RateLimiter::scopes`, Codex r4179558360).
6. **Frames the runtime does not choose.** The WebSocket layer answers each ping with a pong of
   its own. Each ping read is charged one `Control` frame of safety traffic on its connection
   (`RateLimiter::record`), past the cap if need be, so later traffic waits for it and the
   buckets never count less than the venue does (Codex r4179266588); a venue that counts pongs
   is served by a reserve the consumer sizes for them. So is each close read, which the layer
   answers with a close; a close the session sends itself (a reconnect, a stop) is charged the
   same and, refused, not written, the socket closing all the same (Codex r4179720976).
7. **One limiter per venue's limits, shared.** The consumer builds the limiter from the venue's
   declared limits and its reserve and hands it to `MdSessionConfig` or `MdVenueConfig`;
   a session or venue refuses a limiter built for other limits, so no venue runs with its
   limits uncounted. Clones share their buckets: every session of a venue shares one, and two
   venue instances that share an egress IP should share one too (their `Account` buckets are
   then shared as well, which overcounts and never undercounts).
8. **`AddressVolume` is refused.** `RateLimiter::new` refuses a limit of that scope, naming it,
   and so a venue that declares one cannot start; so is a limit of zero units, a zero window or
   no operation (Codex r4179474180), and a limit that lists an operation its scope never
   counts (`RateError::Unreachable`): `Connect` per pair or per connection, since a connection
   attempt names no instrument and is on no connection yet (Codex r4179648042), and `Rest` per
   connection, since an HTTP request is on none (Codex r4179682241).

## Alternatives

- A token bucket refilling at `units / per`: rejected; it lets nearly twice `units` through in a
  window that spans a full bucket's burst and its refill, the undercount 0002 must avoid.
- A fixed window reset on the clock: rejected for the same reason at the window edge, and the
  venue's window boundaries are unknown.
- Every refused frame waiting for the buckets, holding the frames behind it: not taken now.
  It keeps a codec's frames in order, but stops the session's reads behind a wait as long as
  a window; a market-data codec's other frames (hellos, resync requests) are rare and a refused
  one shows up as a counted refusal and, at worst, a reconnect. Subscribe calls wait because
  the reconciler and the codec's state depend on them. Order entry (BT-402) decides its own.
- A refused subscribe call settled as refused and the codec asked again later: rejected
  (Codex r4179266579); a codec that dropped its book for a removal before the frame was
  refused would keep dropping a stream the venue still sends, once the consumer wanted it
  back.
- Taking pongs away from the WebSocket layer so they pass the buckets: not taken now; a pong
  refused would end the connection at the venue, so it is sent and counted instead.
- A reserve rounded up: rejected; a small bucket (one connection per period) would then admit
  no normal traffic at all.
- Counting `AddressVolume` against a fixed allowance: rejected; its allowance moves with traded
  volume the runtime does not see yet, and a guessed one is a silent choice.

## Consequences

- Every `MdSessionConfig` and `MdVenueConfig` names its `RateLimiter`; consumers build it with
  `RateLimiter::new(&caps.limits, SafetyReserve::percent(..))`.
- A charge is recorded when the request is admitted, before its write or its connection
  completes; a write that waits on a full socket is counted from its admission.
- A subscribe call waiting for the buckets holds back the calls after it; the session keeps
  reading, firing timers and answering HTTP results meanwhile.
- The order-entry RPC table and `NotSent(RateBudget)` (BT-402) build on these buckets.

## What would show this was wrong

- A venue rate-limit reject (429, 418 or a venue error) on traffic the limiter admitted within
  the declared limits, which would mean the windows or the charge time undercount.
- Market data gaps traced to frames other than subscriptions being refused.
- A venue whose limits need a scope or a window shape these buckets cannot express.
