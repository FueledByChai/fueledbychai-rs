# 0136 — Instrument specs carry the order kinds, post-only form and times in force a venue lists for each instrument, and QFEX refuses a place its symbol does not list before sending it, augmenting 0003, 0035 and 0125

Status: accepted
Date: 2026-11-04

## Context

QFEX states, per symbol, the order types and times in force its reference data lists (FBC-99w5,
BT-702). Its REST refdata (https://docs.qfex.com/api-reference/rest/market-data/refdata, the
OpenAPI contract of 2026-10-30) gives each `RefdataSymbol` an `order_types` and an
`order_time_in_force`, each an array of strings or null, both among its 20 required fields, with
no enum, description or example. The refdata channel page
(https://docs.qfex.com/websocket/channels/mds/refdata) says its payloads mirror the REST fields;
its example lists `"order_types": ["LIMIT", "MARKET", "ALO"]` and
`"order_time_in_force": ["GTC", "IOC", "FOK"]`, the OrderType and OrderTimeInForce names of the
enums page (https://docs.qfex.com/api-reference/enums). In QFEX's terms post-only is the `ALO`
order type, not a time in force, and the order-types page (https://docs.qfex.com/qfex/order-types)
admits ALO with GTC only, a limit with GTC, IOC and FOK, and a market order with IOC only. No
page says what a null list means, and none says in words that the lists are enforced; the enums
page names the refusals `INVALID_ORDER_TYPE` and `INVALID_TIME_IN_FORCE`.

FBC-blcw's encoder sends limit GTC, ALO GTC and limit IOC places (0132's v1 shapes) on every
symbol. A place its symbol does not list can be expected to be refused, but only once sent: it
has then been charged to the provider user's shared buckets (0123, 0128), and until captures
show our client id echoed it ends `Unknown` (0132). A list can reach an encoder only through the
spec: `ExecCodec::encode` sees an instrument only through its `SpecTable`, discovery's only output
is `InstrumentSpecDraft` (0035), and `OrderCaps`' `kinds`, `tifs` and `post_only` are venue-wide
and built before discovery (`VenueFactory::caps` takes only the configuration). 0003 grows the
model by a mandatory field and a record where it cannot express a behaviour; 0125 added
`sessions` and `signing` to both types that way. This record augments 0003, 0035 and 0125.

## Decision

1. `InstrumentSpec` and `InstrumentSpecDraft` gain `order_shapes: Option<OrderShapes>`, mandatory
   like every other field, after `signing`. `InstrumentResolver::spec` copies it from the draft,
   as it copies `sessions` and `signing`. Neither `OrderShapes` nor the types holding it has a
   `Default` or `#[non_exhaustive]`.
2. `OrderShapes { kinds: TagSet<OrderKindTag>, post_only: bool, tifs: TagSet<TifTag> }` is named by
   the shape of what it holds, never by a venue (0003): the kinds listed for an order that is not
   post-only (a limit here is one that may take liquidity), whether the post-only form of a limit
   is listed (as an order type of its own, as QFEX spells it, or as a flag on a limit), and the
   times in force listed. `OrderShapes::lists(kind, post_only, tif)` is true when the order's form
   is listed (the post-only form for a post-only limit, its kind otherwise; a post-only order that
   is not a limit never) and its time in force is too. The lists name no pairs: which kind a venue
   takes with which time in force stays its own rule, and its venue-wide `OrderCaps` still govern.
   The lists only ever narrow them.
3. The field is `None` where the venue states no per-instrument list, so the venue-wide
   `OrderCaps` alone govern. A venue that does state lists may read a `None` spec as listing
   nothing, and QFEX does (item 6).
4. The field is not part of what `version` indexes (0004): a change in the lists bumps nothing and
   books do not resync, since no tick or lot means anything else (as 0125 item 6 left `sessions`
   and `signing` out).
5. QFEX's discovery reads both lists of every symbol into `Some` of its shapes, with the names
   matched exactly as the enums page spells them:
   - `order_types`: `LIMIT` lists the plain limit (`kinds` holds `Limit`), `MARKET` the market
     order (`Market`), `ALO` the post-only form (`post_only`). A symbol listing `ALO` without
     `LIMIT` has `kinds` empty and `post_only` true: post-only limits only. One listing `LIMIT`
     without `ALO` has `Limit` and `post_only` false: never post-only. The example's
     `["LIMIT", "MARKET", "ALO"]` is `Limit` and `Market` with `post_only` true.
   - `order_time_in_force`: `GTC`, `IOC` and `FOK`, each its `TifTag`.
   - Any other name is ignored, since this adapter never sends it: the enums page's
     `TAKE_PROFIT`, `STOP_LOSS`, `STOP_MARKET` and `TWAP_LIMIT`, another spelling (`alo`) and
     anything unknown. A name listed twice is one member.
   - An empty or null list admits nothing of its kind on that symbol: no page says what null
     means, and 0054 declares an undocumented value conservatively. The field is read without
     discovery's `required`, which takes null as missing.
   - A missing list refuses the whole answer with `PlanError::Missing(name)` under
     `VenueFactory::discover`'s contract, market data included, as every required field discovery
     reads does (0035). A value that is neither null nor an array of strings, or an array holding
     anything but strings, refuses it as malformed (`DecodeError::Malformed(name)`).
6. QFEX's encoder refuses a place with `NotSent(Unsupported)`, pushing no effect, unless its
   instrument's spec lists it: `order_shapes` is `Some`, and `lists(Limit, post_only, tif)`, so its
   `order_types` names `LIMIT` for a plain limit or `ALO` for a post-only one and its
   `order_time_in_force` names its time in force (`GTC` or `IOC`). The check follows the v1 shape
   (a shape v1 never sends is refused for what it is, whatever the spec lists) and the spec
   lookup, and comes before the price and the quantity. QFEX signs no order (the add_order frame
   has no signature field, https://docs.qfex.com/websocket/channels/trade/add_order, and its HMAC
   signs only the authentication, FBC-77aa), so the refusal comes before any frame is written and
   nothing is charged. An amend a later ticket admits takes the same check. A cancel and a query
   are never held to the lists.
7. Its readers are the venue's encoder now (QFEX's); fbc-oms's planner and pre-trade path, and
   SimVenue, read it in later tickets. Until the planner reads it, it re-places a level the
   encoder refused at each pass under a new client id, the churn FBC-fto2 describes for
   `OrderCaps`.
8. Paradex, Binance USD-M, Hibachi, the toys and every test spec that does not exercise the lists
   state `None`. Binance USD-M's exchange info lists per-symbol `orderTypes` and `timeInForce` (its
   fixture carries them); its adapter is market data only and reads neither.

## Alternatives

- The QFEX codec reading `/refdata` itself: rejected. It repeats discovery's read, arrives after
  the epoch opens, and is invisible to fbc-oms and fbc-sim, which see an instrument only through
  its spec.
- Discovery dropping the symbols lacking the v1 shapes, or marking them `Halted`: rejected. It
  loses their market data or misstates their status, which is the venue's.
- Leaving the check to QFEX's reject: rejected. The place is still sent and charged to the shared
  buckets (0123, 0128), it ends `Unknown` until client ids are captured echoed (0132), and the
  pre-production stage's capture of how QFEX refuses a crossing post-only order must not be an
  unlisted-type refusal instead, since FBC-jdj7 maps rejects from those captures.
- Per-instrument capabilities on `VenueCaps`, keyed by instrument: rejected. `caps()` is built
  from the configuration before discovery, and a spec is where per-instrument venue truth lives.
- A whole `OrderCaps` in the spec: rejected. It repeats every venue-wide field to vary three.
- The venue's names as strings in the spec: rejected. A venue's spelling in the core, parsed again
  by every reader.
- A set of (kind, post-only, time in force) triples: rejected. QFEX lists the types and the times
  in force apart and states no pairs; a product of the two would claim pairs its order-types page
  refuses (a market order GTC), and the pairs are the venue's rule, not the symbol's list.
- `kinds` holding `Limit` whenever `ALO` is listed, post-only read as a flag on a listed limit as
  `OrderCaps` reads it: rejected. A symbol listing `ALO` without `LIMIT` would then admit a plain
  limit it does not list.
- A null list meaning the venue-wide caps govern (a `None` spec): rejected. No page says what null
  means; reading it as "everything" sends what the symbol may refuse.
- A name outside the mapping refusing the answer: rejected. It would lose every symbol's market
  data over a type this adapter never sends.
- The lists in `version`: rejected. No tick or lot changes meaning with them.

## Consequences

- Every `InstrumentSpec` and `InstrumentSpecDraft` literal states the field; the compiler lists
  any that do not.
- `fixtures/qfex/rest/refdata.json` and its variant `refdata_order_lists.json` carry both lists on
  every entry, and `fixtures/qfex/README.md` describes them.
- QFEX's discovery fails, market data included, on an answer whose symbol omits either list. The
  OpenAPI contract requires both; if the live answer omits one, a later record decides.
- The lists are discovery's snapshot, as the spec's status is: not refreshed after discovery.
- The pre-production probe runs discovery on its own configuration (0132 item 7), so the encoder
  sees pre-production's lists; FBC-jdj7's conformance setup for QFEX needs specs that list the v1
  shapes; production `ExecCaps` declared venue-wide (FBC-cnyh) are read beside the lists, never
  in place of them.

## What would show this was wrong

- QFEX refusing a place its symbol lists as `INVALID_ORDER_TYPE` or `INVALID_TIME_IN_FORCE`, or
  accepting a type or time in force its symbol does not list (the lists are then not what QFEX
  enforces, and the refusal costs places QFEX would take).
- A page or capture saying a null or empty list means every type or time in force is admitted.
- A live `/refdata` answer omitting `order_types` or `order_time_in_force`, failing discovery.
- A venue whose per-instrument lists pair kinds with times in force, or state a post-only form for
  some times in force only, which three independent lists cannot express.
- A symbol whose lists change between discoveries while orders rest, so a resting order's shape
  is no longer listed.
