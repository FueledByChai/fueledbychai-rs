# 0094 — Paradex's open-order snapshot is trustworthy on FBC-8xr's testnet evidence, so a resync seeds its positions, with positions following fills to be verified in the first mainnet sessions that get fills, amending 0054

Status: proposed
Date: 2026-10-16

## Context

0054 declared Paradex's `snapshot_source` `Untrustworthy`, the owner's answer C to RB-olg-3
(2026-10-06): no Paradex document says whether `GET /orders` and `GET /positions` agree with
the order and fill streams, so no resync seeded a Paradex position, an order's absence from
`GET /orders` never ended it, and only a hand seed under a declared owner-assisted testnet run
(0067, `TestnetRun::owner_assisted()`) could arm a Paradex market. 0054 named the way out: a
new record, on FBC-8xr's testnet evidence, before any live Paradex session. This is that record
(FBC-2f7w).

The evidence is the owner's three runs of the acceptance program `paradex_testnet_acceptance`
(FBC-xji8) on Paradex testnet, committed by FBC-8xr with their scrubbed fixtures and decoded by
`crates/venues/fbc-venue-paradex/tests/exec_recorded.rs`:

- `fixtures/paradex/exec-recorded/20261015T222347Z/report.md` (ETH-USD-PERP, $11 / $50 caps):
  every step passed; batch and cancel-many skipped (no ask resting).
- `fixtures/paradex/exec-recorded/20261016T115634Z/report.md` (HYPE-USD-PERP, $40 / $50):
  place, amend and cancel passed; the batch's sell was refused asynchronously
  (`ORDER_SIZE_BELOW_MIN`, judged at the mark price), so the later steps did not run.
- `fixtures/paradex/exec-recorded/20261016T120633Z/report.md` (HYPE-USD-PERP, $90 / $100):
  every step passed but `fill` (the IOC was refused by the price band; nothing filled).

FBC-2f7w's questions, answered from those reports (`fixtures/paradex/exec-recorded/README.md`
tabulates them):

| Question | Answer |
| --- | --- |
| (a) `GET /orders` lists every order the stream acknowledged and omits every order it ended, at every comparison | yes: 24 of 24 comparisons agreed at the first read (7 of 7, 5 of 5, 12 of 12), across place, amend, cancel, a two-item batch, a two-item cancel-many, an IOC expiry, both cancel-on-disconnect cases and Stop |
| (b) the lag between a stream event and its REST reflection | at most 1705 ms, 1698 ms and 3480 ms per run: at most 3480 ms |
| (c) `GET /positions` matches the position implied by fills (signed size, LONG/SHORT, CLOSED at 0) | not exercised: no order filled in any run (testnet books rest outside the price band, so the fill IOC was refused); only the flat position (`[]`) was compared, which is no evidence that positions follow fills |
| (d) `GET /orders` paginates | no: no cursor key beside `results` in any answer of any run |
| (e) `GET /orders-history?client_id=` lag | NEW/OPEN listed after 1449 to 2102 ms; CLOSED after 1354 to 2429 ms: at most 2429 ms (an order closed before it was read is never listed NEW/OPEN) |

Two more answers bear on what the flip enables: cancel-on-disconnect cancelled the resting
order after 580 ms and 637 ms on a dropped socket and after 390 ms and 327 ms on a graceful
close, and it was not resting at the next connection's resync, in both runs that tried it.

(c) is unanswered, and FBC-2f7w's description says an unmeasured answer keeps the value
`Untrustworthy`. The coordinator put that to the owner on 2026-10-16: testnet books sit outside
Paradex's price band, so no API order can fill there and (c) cannot be measured on testnet.

### OWNER

The owner's answer (2026-10-16, to the question "Testnet books sit outside Paradex's price
band, so no API order can fill there. How should we get the positions follow fills evidence
for the trust decision?"), quoted exactly:

> "Decide on current evidence" (option text: "Write the trust decision (FBC-2f7w) on the
> orders evidence collected so far, and record 'positions follow fills' as a condition to
> verify in the first mainnet sessions that get fills, with the kill switch ready if REST and
> the stream disagree.")

## Decision

Paradex's `snapshot_source` is `SnapshotSource::Trustworthy`
(`crates/venues/fbc-venue-paradex/src/exec/caps.rs`), on answers (a), (b), (d) and (e), on the
owner's answer above. A resync from `GET /orders` and `GET /positions` therefore seeds each
Paradex market's position once per process (0055), and an order on the Unknown ladder that
`absent_snapshots` resyncs in a row do not show, each at least `settle` after it was sent, is
Lost (0005).

**The owner's condition.** `GET /positions` following fills was NOT exercised on testnet. It
is verified in the first mainnet sessions that get fills, under this rule: any disagreement
between the position `GET /positions` reports at a resync and the inventory the stream's fills
gave the market stops the session at once, with the kill switch on and every order of ours
cancelled (Stop: kill switch + cancel all), and no order is placed after it; a `PositionEvent`
the inventory does not hold stops it too unless the fills bring them back into agreement
within the step timeout. Where the rule already holds:

- fbc-oms compares, at every resync after the seed, each seeded market's `GET /positions`
  position with its inventory as of the snapshot's watermark (`Registry::resync`), reporting
  `PositionCheck::Desync` (or `Stale`, `Unsettled`, and `ResyncReport::unsettled` for a fill it
  could not place). It reports; the consumer stops.
- `fbc_examples::testnet::resync_disagreements` collects every check other than `Agrees` and
  every unsettled fill. `testnet_quote` takes any of them as trouble and Stops (kill switch on,
  every order of ours cancelled, the session closed), tested end to end by
  `a_resync_position_mismatch_stops_it_with_no_place_after` (`crates/fbc-examples/tests/testnet_quote.rs`).
  `testnet_trade`'s `account_changes` reports them as a change of the account, which Stops the
  run, tested by `the_account_audit_counts_a_resync_that_disagrees_with_the_inventory`
  (`crates/fbc-examples/tests/testnet_trade.rs`, added with this record). The owner's
  `mainnet_smoke` (FBC-p0aa) is `testnet_trade`'s path, and FBC-tyft's mainnet mode of
  `testnet_quote` lists Stop on any resync mismatch in its done line.
- A `PositionEvent` the inventory does not hold is the stream against itself, not REST against
  the stream: the fills and positions channels are separate, so a fill can trail its
  `PositionEvent` for a moment. `testnet_quote` plans nothing while they differ and Stops when
  they have not agreed within the step timeout
  (`a_venue_position_the_inventory_does_not_hold_plans_nothing`); `testnet_trade` takes any
  `PositionEvent` other than the start position as a change of the account and Stops. One gap
  is already a required fix before any mainnet loop session: a `PositionEvent` arriving during
  a reconnect's resync is dropped when the resync lands (Codex r4277631505, in FBC-tyft).

## Alternatives

- Keep `Untrustworthy` until a fill is measured, as FBC-2f7w's description reads for an
  unmeasured answer: not done, on the owner's answer. Testnet cannot fill an API order, so the
  only place (c) can be measured is mainnet, and with `Untrustworthy` no mainnet Paradex market
  could ever arm (0067 refuses a hand seed outside a declared testnet run). The condition and
  its stop rule take the place of the missing answer.
- Flip only the seed and keep an order's absence from `GET /orders` from ending it: not done.
  One field decides both (0054), the absence half is what (a) measured best (24 of 24), and a
  Lost order needs `absent_snapshots` resyncs past `settle`, well beyond the 3.5 s lag seen.
- Hand-seed mainnet markets: refused by 0067 and by the owner's rule that nothing is sent after
  a restart before the first trustworthy resync.

## Consequences

- **What this enables.** A Paradex market can now be armed on mainnet: Start, Flatten and
  Wind-down take a market seeded by a trustworthy resync (0055, 0067), so after the first
  trustworthy resync of the process a mainnet consumer may arm it without a hand seed. FBC-vgl
  (the first live Paradex order) is no longer blocked by this record, and the owner's
  `mainnet_smoke` (FBC-p0aa) comes next.
- **What still guards mainnet.** The pre-trade caps on every order command from the consumer's
  configuration, the owner's $11 resting per side and $50 inventory (I6, decision 0005); the
  kill switch and Stop (kill switch + cancel all, 0012); cancel-on-disconnect armed on every
  connection and re-armed on reconnect (0054, confirmed by the runs above), with orders resting
  from an earlier epoch Safety-cancelled before anything is placed (0080); the single-use
  authorization re-checked at submit (0045, 0060); no account-wide cancel-all; the stop rule
  above; and FBC-tyft's required fixes (the nine Codex findings on PR #149 and the
  minimum-notional sizing at the mark price) before any mainnet loop session.
- **The instrument cancel-all.** A trustworthy resync shows the account's open orders, so
  0005's I7 guard can now hold on Paradex: `Registry::cancel_everything` builds an instrument
  cancel-all (`order.cancel_all` for the market) when the registry holds the market's exclusive
  lease and no order not ours is in view there, with explicit cancels of our PendingNew and
  Unknown orders beside it. `testnet_quote`, `testnet_trade` and `paradex_testnet_acceptance`
  take it whenever fbc-oms builds it (the owner's RB-elg7-2 answer: `testnet_quote` switches to
  it automatically): at Stop in all three, and at `testnet_quote`'s sweeps (the start's and
  each reconnect's), sent even when nothing of ours is known open, since it also ends what the
  registry does not hold; they fall back to explicit cancels otherwise. Never an account
  cancel-all. No testnet run has sent `order.cancel_all` yet (FBC-8xr's runs cancelled by
  explicit reference): the codec answers its documented `{"status":"ok"}` as a provisional
  acceptance and each order's end comes by its `OrderEvent`; the owner's next testnet run is
  the first to exercise it, and a reply of another shape comes back Unknown, which Stops the
  loop.
- **0067 stays.** `TestnetRun::owner_assisted()` and `Registry::seed_position` remain for an
  owner-assisted testnet run on a venue whose resync seeds nothing; on Paradex the resync now
  seeds the market, so the samples seed by hand only a market the resync left unknown, which
  on Paradex does not happen. The samples still declare the testnet run.
- **The ladder's settle.** An order absent from a lagging `GET /orders` must not be Lost: the
  samples' `LadderConfig` `settle` is `testnet::SNAPSHOT_SETTLE` (10 s), above the 3480 ms lag
  seen, and a consumer's must be too.
- **Residual: two reads.** Paradex's resync is two REST requests, not one instant. A fill
  landing between `GET /orders` and `GET /positions` can be counted wrongly by the seed's
  shown-cumulative-fill rule (FBC-k7t7, open): at the seed only, since later resyncs compare
  and a miscount there is a `Desync`, which stops the session. At the seed nothing of ours is
  sent yet and orders from an earlier epoch were cancelled by cancel-on-disconnect (under
  640 ms in the runs above), so the case needs a fill of an earlier order racing the first
  resync; FBC-k7t7 closes it.
- Proof: `crates/venues/fbc-venue-paradex/tests/exec_caps.rs` (the value and this record's
  citations), `tests/factory_exec.rs`, `tests/rehearsal.rs` (a resync seeds the market in a
  registry not declared a testnet run, Start and Flatten take it with no hand seed, and the
  kill switch's cancel everything is an instrument cancel-all), and the samples' tests in
  `crates/fbc-examples/tests/` (the resync's seed, the cancel-alls, the stop rule, and
  `a_lagging_snapshot_never_ends_an_order_the_samples_hold`).

## What would show this was wrong

In a mainnet session, `GET /positions` disagreeing with the position the fills gave
(`PositionCheck::Desync` that does not clear by the next resync once every fill arrived), a
CLOSED position reported at a size other than 0, a `GET /orders` answer that omits an order the
stream acknowledged or lists one it ended once the lag has passed, or one that paginates. Any of
them stops that session by the rule above and reopens this record: the value goes back to
`Untrustworthy` in a new record.
