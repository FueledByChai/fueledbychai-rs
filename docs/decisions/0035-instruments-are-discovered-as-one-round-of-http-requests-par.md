# 0035 — Instruments are discovered as one round of HTTP requests parsed in a decode scope, and resolved through a fixed alias table

Status: accepted
Date: 2026-10-04

## Context

Design §4.4 and §4.7 give `VenueFactory` two instrument calls FBC-5 left out (0014):
`discover(cfg) -> HttpPlan<Vec<InstrumentSpecDraft>>` and `parse_fbc_common_symbol(s) ->
Result<AssetKey, SymbolError>`, the hook the consumer's `LegacyKeyReader` calls for Java-era
tickers. The core then resolves `(VenueId, AssetKey)` to an `InstrumentId` through an alias
table seeded `USDT=USD, USDC=USD, GOLD=XAU` and overridable by configuration; a draft missing a
required field is refused loudly and nothing is guessed from prefixes. The design names
`HttpPlan`, `InstrumentSpecDraft`, `AssetKey` and `SymbolError` but does not define them, and a
parser, like a codec, may read no clock and do no IO (0002), and builds a `VenueSymbol` only
inside a `DecodeScope` (0004). FBC-ahf builds them in `fbc-core` (`src/resolve.rs`,
`src/venue.rs`).

## Decision

- `HttpPlan<T>` is one round of requests and a parser: `HttpPlan::new(effects, parser)`
  refuses an effect that is not an `Effect::Http`, or two requests under one tag;
  `HttpPlan::parse(answers, scope)` refuses answers that are not one per request in the plan's
  order (`PlanError::Answers`), a request with no response (`PlanError::Http`) and a status
  other than 2xx (`PlanError::Status`) before the parser runs, then hands the parser the
  responses in order with the `DecodeScope` its caller lends. A venue that pages its list needs
  a later record.
- `discover` returns `Result<HttpPlan<…>, VenueError>`, like `plan_md`, so a refused
  configuration is an error, not a plan; `VenueError::NoDiscovery` says an adapter discovers
  nothing (the consumer states its instruments). `SymbolError::NoRule` says a venue has no FBC
  common-symbol rule; `Unmapped` that the rule maps the ticker to no instrument;
  `NotCommonForm` that it is not `BASE/QUOTE` (`common_symbol_parts` splits that form).
- `AssetKey { base, quote, kind }` is what an instrument is a contract on, in the venue's own
  spelling. `InstrumentSpecDraft` is an `InstrumentSpec` without what the core assigns (id,
  venue, underlying, version, `fetched_at`) and with `asset: AssetKey`, whose quote and kind are
  the spec's `quote_ccy` and `kind`, stated once. Its fields are all mandatory, so a draft
  missing one does not compile; a parser that finds a required field missing from the venue's
  answer refuses the whole answer with `PlanError::Missing(field)`. The parser reads no clock,
  so `version` and `fetched_at` come from whoever ran the plan.
- `AliasTable` maps an alias to its canonical name in one hop: `set` refuses a name aliased to
  itself and any chain, so reading a name twice gives what reading it once gave.
  `AliasTable::seeded()` holds the three defaults; the consumer overrides them with `set` and
  `remove`. `InstrumentResolver` holds the consumer's `Listing`s (`InstrumentId` and
  `UnderlyingId`) by `(VenueId, canonical AssetKey)` under an alias table fixed when it is
  built, so a key listed and a key asked for are read under the same aliases; a venue lists a
  key and an id once each, and an unlisted key is `ResolveError::NotListed`, never guessed.
  `InstrumentResolver::spec` turns a draft into its `InstrumentSpec`.

## Alternatives

- A draft with optional fields that the core checks: rejected. It moves a missing field from
  the compiler to run time, against 0003's every-field-mandatory rule.
- The resolver numbering instruments itself, in discovery order: rejected. An id must not
  change with the order a venue lists its markets in, since journals and books are keyed by
  it; the consumer numbers them.
- `quote_ccy` and `kind` on the draft beside its `AssetKey`: rejected, two copies that could
  disagree.
- Following alias chains: rejected. A chain makes the canonical name depend on the order
  overrides are applied; refusing it keeps one hop.

## Consequences

- Every venue factory states both calls. Paradex's are FBC-l5o and Binance USD-M's FBC-fwf;
  until then they answer `NoRule` and `NoDiscovery`.
- The runtime (or the consumer) makes a plan's requests through the proxy, lends a
  `DecodeScope`, and stamps `version` and `fetched_at`.

## What would show this was wrong

- A venue whose instrument list needs a request that depends on an earlier answer (paging,
  per-market detail), which one round cannot express.
- Two venues whose spellings need different canonical names for the same alias, which one
  table per resolver cannot hold.
