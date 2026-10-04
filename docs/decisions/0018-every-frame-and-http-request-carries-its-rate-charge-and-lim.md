# 0018 — Every frame and HTTP request carries its rate charge, and limits gain a per-connection scope and a connect operation

Status: accepted
Date: 2026-10-03

## Context

0002 puts rate limiting, with a safety floor, in `fbc-runtime`; 0003 makes a venue's rate limits
data (`VenueCaps::limits`, each a `RateLimit` of a `LimitScope`, a set of `OpKind`s, a window and
a number of units). 0014 item 3 gave `Effect::Send` and `Effect::Http` a `TrafficClass` and an
optional `rpc`, and listed rate-limit tags on effects as not taken in FBC-5 (Codex r4172367663 on
pull request #8). Without them the runtime cannot tell what a frame or request is: traffic a
codec sends on its own from `on_open`, `on_timer`, `on_http` or `on_frame` (authentication, a
resync, a token refresh, a resubscribe, a pong) has no `rpc` and no command, so it matches no
`RateLimit::ops` entry, and on a codec shared by several instruments a per-pair limit
(`LimitScope::Pair`) cannot tell which instrument a request counts against. The runtime would
charge the wrong bucket or none.

The model also could not state what Binance USD-M documents for its market data, which this
library records as reference data (0016): a request weight (a deeper `GET /fapi/v1/depth` costs
more against a per-IP weight budget), a cap on incoming messages per connection that counts
subscriptions, pings and pongs alike, and a cap on new connections per IP. 0003: a behaviour the
model cannot express grows the model. FBC-hof builds the `fbc-core` half; FBC-bel builds the
limiter that charges by it.

## Decision

This record augments 0014; where they differ, this one wins.

1. **Every request carries a `RateCharge`.** `Effect::Send` and `Effect::Http` carry a mandatory
   `charge: RateCharge { op, inst, weight }`, and so does every `Keepalive` the runtime sends for
   a codec. `op` is the `OpKind` the venue counts the request as; `inst` is the scope key, the
   instrument a per-pair limit counts it against, `None` for a request that names no single
   instrument (authentication, a resync, an account-wide cancel-all); `weight` is a
   `NonZeroU32` in the units of the limits that count the request, one for a venue that counts
   requests and more for a weighted request, never zero so no request rides free.
   `RateCharge::one(op, inst)` is the common case. A codec charges what it sends from every
   callback, not only from `encode`.
2. **A limit counts a charge by operation, keyed by its scope.** `RateLimit::counts(charge,
   via)` holds when the limit lists `charge.op`; for a `Pair` limit, the charge names an
   instrument; and for a `Connection` limit, `via` is `Via::Frame`. `Effect::charge()` gives a
   request's charge with its `Via` (`Frame` for `Send`, `Http` for `Http`; a keepalive is a
   frame), so the runtime has one matching rule for every request. The runtime charges `weight` to every limit that counts the request, in the
   bucket its scope picks: the account, the source IP, the named instrument, the connection, or
   the address's volume allowance. A per-pair limit counts only charges that name an
   instrument, so a codec names the instrument on every request its venue counts per pair; each
   codec's tests check its charges against its own caps (the toy venue's do,
   `the_toy_charges_its_encode_resync_keepalive_and_subscribe_traffic`). A charge that misses a
   limit costs at worst a venue-side rate-limit reject; it never blocks a cancel (0012).
3. **`LimitScope::Connection`** counts per connection, while it stays open: the frames written
   on it (`Effect::Send` to its stream) and its keepalives. It never counts an HTTP request,
   which goes on no connection a codec names, even of an operation it lists for frames: a venue
   that places orders over both a socket and REST has its socket's message cap count only the
   socket's (Codex r4176401174).
4. **`OpKind::Connect`** is opening a connection. The runtime charges
   `RateCharge::one(OpKind::Connect, None)` for each connection it opens, planned or after an
   `Effect::Reconnect`, against the limits that list it; no codec charges it.
5. **`OpKind::Control`** is any other frame written on a connection: authentication, a
   keepalive ping or pong, a session message. FBC-hof's ticket named only `Connect`; `Control`
   is added because a keepalive and a hello are neither an order, a query, a subscription nor a
   REST request, and charging them as one of those would put them in that operation's bucket.
6. The traffic class (0014 item 3) is unchanged and independent: it decides whether a request
   may use a limit's safety floor; the charge decides which limits it is counted by.

## Alternatives

- Keep the tag opaque and let the runtime infer the operation from `rpc` or the command: rejected;
  traffic without an `rpc` (resyncs, keepalives, resubscribes, token refreshes) is exactly what
  would go uncounted, and the runtime cannot read a venue's frames.
- One charge per item of a batch, or a list of instruments per request: not taken. Every planned
  venue counts a request once; a venue that counts one request against several pairs grows the
  charge with a record of its own.
- A weight per limit kind (requests, orders, weight) on each charge: not taken. With one weight
  for every limit that counts a request, Paradex, Hibachi and Binance USD-M market data are
  expressible; Binance's order entry, where a batch costs one weight against the IP but one
  order per item against the account, is not planned (0016).
- Refusing to send a request whose charge a per-pair limit lists but which names no instrument:
  rejected, since it could stop a safety cancel on a declaration mistake; the codec's tests
  catch it instead.
- `OpKind::Rest` or `Subscribe` for keepalives and authentication: rejected for the reason in
  item 5.

## Consequences

- Every `Effect::Send`, `Effect::Http` and `Keepalive` literal states its charge; a venue codec
  cannot compile without deciding how its venue counts each frame
  (`tests/ui_states/request_without_charge.rs`).
- Every `match` on `LimitScope` or `OpKind` handles `Connection`, `Connect` and `Control`.
- FBC-bel's limiter charges buckets by these charges, charges `Connect` itself, and keeps
  per-connection buckets per connection epoch.
- Venue crates declare `Connection` and `Connect` limits where their venue documents them, with
  the document cited (0003).

## What would show this was wrong

- A venue that counts one request in different units against different limits, or against
  several instruments at once, that a supported adapter must model.
- A request found charged to no limit, or to the wrong instrument's bucket, in a journaled
  session, despite its codec's tests.
- A rate-limit reject from a venue on traffic whose charges the runtime counted within the
  declared limits.
