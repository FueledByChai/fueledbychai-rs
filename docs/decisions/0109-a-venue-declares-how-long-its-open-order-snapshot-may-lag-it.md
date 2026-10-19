# 0109 — A venue declares how long its open-order snapshot may lag its order stream, and the Unknown ladder counts no absence within that bound of an order's send whatever the consumer's settle, augmenting 0005 and 0094

Status: accepted
Date: 2026-10-19

## Context

0005's Unknown ladder ends an order Lost when a trustworthy snapshot of the venue's open
orders misses it `absent_snapshots` times in a row, each with a watermark at least `settle`
after the order was sent (`Registry::on_resync`, applied by `Registry::resync`). Lost frees the
order's resting exposure: the pre-trade caps stop counting it, so the consumer may place
again on that side. An order that still rests at the venue would then be over the gross
per-side resting cap.

0094 made Paradex's snapshot trustworthy on FBC-8xr's three testnet runs, whose answer (b)
was the lag between a stream event and its reflection in `GET /orders`:

| Run | REST lag behind the stream |
| --- | --- |
| `fixtures/paradex/exec-recorded/20261015T222347Z/report.md` (ETH-USD-PERP) | at most 1705 ms |
| `fixtures/paradex/exec-recorded/20261016T115634Z/report.md` (HYPE-USD-PERP) | at most 1698 ms |
| `fixtures/paradex/exec-recorded/20261016T120633Z/report.md` (HYPE-USD-PERP) | at most 3480 ms |

Each lag runs from the stream's last change of the orders compared to the first read at which
`GET /orders` and `GET /positions` both agreed with it (`lag_ms` in the acceptance program's
`evidence.rs`), so it does not include the time from our send to the venue's event. 0094's "The ladder's settle"
noted that the samples' `settle` (`testnet::SNAPSHOT_SETTLE`, 10 s) is above the 3480 ms seen
but that nothing held another consumer to it: `LadderConfig::new` accepts any settle, zero
included, so a `LadderConfig` with a short settle would end a resting Paradex order Lost from
a lagging snapshot (Codex r4295929734 on PR #155, filed as FBC-i8lp). The owner wants this
closed before chaiwala-rs runs live (2026-10-16).

## Decision

- **Declared, with no default.** `OrderCaps` gains a mandatory field,
  `snapshot_lag_bound: Duration`: how long the venue's open-order snapshot may lag its order
  stream, so that an order sent less than this before a snapshot's watermark may be missing
  from it though the venue holds it. Like every capability (0003) `OrderCaps` cannot be
  written without it (`crates/fbc-core/tests/ui_caps/order_caps_missing_snapshot_lag_bound.rs`),
  and a venue cites the measured lag it declares, with margin. It is read only from a
  trustworthy source, the only one whose absences count.
- **The ladder ignores absences within it, whatever the settle.** An absence counts only from
  a snapshot whose watermark is at least the order's send plus
  `LadderConfig::absence_settle(caps)`, the longer of the consumer's `settle` and the venue's
  `snapshot_lag_bound`. `Registry::on_resync` applies it, and so `Registry::resync`, which
  applies the ladder's orders through it. A snapshot within the bound changes neither the
  order nor its absence count; one past it counts as before, and the configured number in a
  row still ends the order Lost. A settle longer than the bound still decides, and a zero
  bound leaves the settle alone, so no other number of the ladder changes.
- **The values.** Paradex declares 6 s: the 3480 ms the runs above measured at most, plus more
  than half again for the send's own latency and the runs' few samples, and below the
  samples' 10 s `SNAPSHOT_SETTLE`, so their behaviour does not change. The conformance toy
  venue, SimVenue's stubs and the core's and the OMS's synthetic stubs answer their resync from
  their own state at once and declare zero.

## Alternatives

- **Refuse the resync, or the `LadderConfig`, when the settle is below the bound:** not taken.
  `Registry::resync` also seeds positions, registers orphans and compares the inventory;
  refusing it whole for a ladder number would leave every market unknown and the consumer
  unable to Start, for a setting the OMS can make safe by itself. `LadderConfig::new` does not
  know the venue, and one `LadderConfig` may serve registries on several venues. Ignoring the
  absences inside the bound closes the hole with the consumer's settle kept as a floor for the
  rest.
- **Bound on `SnapshotSource::Trustworthy` itself** (`Trustworthy { lag_bound }`): not taken.
  It ties the two in one place but changes a value read in about a hundred places, the
  samples' among them, for no difference in what the ladder does.
- **A tighter Paradex bound (the 3480 ms seen, or 4 s):** not taken. A bound too short ends a
  resting order Lost and frees its exposure, a cap breach; one too long only keeps an order
  that ended counting as resting for a few seconds longer, which is on the safe side.

## Consequences

- No consumer of fbc-oms can end a Paradex order Lost from a snapshot requested within 6 s of
  its send, whatever its `LadderConfig`: chaiwala-rs may choose its settle for its own reasons
  without re-deriving Paradex's lag.
- Every venue states its bound when it declares its order capabilities, and a new venue whose
  snapshot is trustworthy must cite a measured lag for it.
- An order on the ladder that did end at the venue is Lost no sooner than 6 s after its send on
  Paradex, plus the further snapshots `absent_snapshots` asks for; it keeps counting against
  the caps until then.
- Proof: `crates/fbc-oms/tests/ladder.rs`
  (`an_order_absent_from_snapshots_within_the_venues_lag_bound_of_its_send_is_never_lost_whatever_the_settle`,
  `a_resync_within_the_venues_lag_bound_of_an_orders_send_never_ends_it_lost_and_one_past_it_does`,
  `a_settle_above_the_venues_lag_bound_still_governs_and_a_zero_bound_leaves_the_settle_alone`)
  and `crates/venues/fbc-venue-paradex/tests/exec_caps.rs`
  (`paradex_declares_its_snapshot_lag_bound_with_margin_over_every_recorded_lag_and_below_the_samples_settle`).

## What would show this was wrong

A Paradex `GET /orders` read, in a testnet or mainnet session, that omits an order the stream
acknowledged more than 6 s before the read and that the stream does not end: the bound is then
too short, and a new record raises it. An order ended Lost that the venue later shows resting,
or fills, reopens this record for that venue.
