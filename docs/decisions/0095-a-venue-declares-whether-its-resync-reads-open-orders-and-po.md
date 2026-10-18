# 0095 — A venue declares whether its resync reads open orders and positions at one instant, and on separate reads only a fill's arrival or matching-engine time places it against the seed, augmenting 0055

Status: accepted
Date: 2026-10-17

## Context

0055's seed holds a fill accepted around the first resync (executed after the request, reported
before or after the answer) by three rules: (1) it arrived before the request was sent; (2) the
snapshot shows its order with a cumulative fill at least the fill's `cum_after`, and counts it
after the snapshot when the shown fill is lower; (3) the snapshot does not show its order and
the order was at the venue before the request, so it closed before the venue read the account.
Rules 2 and 3 judge a fill by the open orders and trust the position read beside them, so they
hold only when the open orders and the positions are one read of the account. 0055 stated that
precondition and left it unenforced (Reviewer B's RB80-2 on PR #80): `OrderCaps::snapshot_source`
says whether the snapshot can be trusted, not whether it is one read (FBC-k7t7, BT-402).

Paradex's resync is two requests, `GET /orders` and `GET /positions`, sent in one round
(`resync_requests` in `crates/venues/fbc-venue-paradex/src/exec/rest.rs`, FBC-0sc), each
answered at its own instant. Since 0094 made its snapshot trustworthy, the seed applies there
(0094's "Residual: two reads", Codex r4295929715 on PR #155), and the owner gated FBC-tyft's
mainnet loop on this ticket landing (0094, the owner's gating of 2026-10-16).

A fill of 4 lots executing between the two reads, taking an order shown filled 6 to 10, with
the position 6 before it:

| Read order | What the reads show | Rule 2 or 3 on one-read trust | Seeded | True |
| --- | --- | --- | --- | --- |
| positions, then orders | position 6; the order filled 10 | rule 2 holds the fill in the snapshot | 6 (never counted) | 10 |
| positions, then orders, the fill ending the order | position 6; the order not shown | rule 3 holds it (an earlier run's order) | 6 (never counted) | 10 |
| orders, then positions | the order filled 6; position 10 | rule 2 counts it after the snapshot | 14 (counted twice) | 10 |

Each read order breaks one half of rule 2, and the first breaks rule 3 too, so with the order
of the reads unknown neither rule is sound. On Paradex specifically, fills carry no cumulative
fill (`cum_after` is `None`) and a publish time, not a matching-engine time
(`crates/venues/fbc-venue-paradex/src/exec/fill.rs`), so rule 2 already never placed a Paradex
fill by a cumulative fill; the hole there was rule 3's absence branch, the second row.

## Decision

- **Declared, with no default.** `OrderCaps` gains a mandatory field,
  `snapshot_reads: SnapshotReads`: `SnapshotReads::OneInstant` (one read of the account: one
  request answering both, or a snapshot riding the fills' ordered stream) or
  `SnapshotReads::Separate` (separate reads, in either order or at once). Like every capability
  (0003) it has no `Default` and `OrderCaps` cannot be written without it, so every venue and
  every stub states it (`crates/fbc-core/tests/ui_caps/order_caps_missing_snapshot_reads.rs`,
  `snapshot_reads_default.rs`). It is read only from a trustworthy source, the only one that
  seeds.
- **On it, and not on `ResyncBegin`.** How a resync reads the account is fixed by how the
  venue's codec builds it, as the snapshot source is, and `Registry::resync` already takes the
  `OrderCaps`. A field on `ResyncBegin` would have every codec set it on every resync and every
  consumer carry it into `ResyncSnapshot`, a value per event a consumer could drop or fill in
  wrongly; a capability is stated once, checked by the compiler, and reaches the seed without
  the consumer.
- **On separate reads, only arrival and time place a fill.** The seed records the declaration
  with its snapshot. On `Separate` a fill accepted before or after the seed, of an order the
  seed knew, is held in the snapshot when it arrived before the request (rule 1) or its
  matching-engine time is at or before the watermark (both reads came after the request);
  neither its order's shown cumulative fill nor the order's absence places it, either way.
  A fill neither places is unsettled under 0055's rule for a fill nothing places: before the
  seed its market is not seeded and stays unknown (`ResyncReport::unsettled`) until a resync
  requested after the fill arrived holds it by rule 1; after the seed the market's position is
  unknown for the rest of the process (`FillRouted::Unsettled`). A fill of an order placed after
  the seed counts on the inventory, as before. On `OneInstant` the placement is 0055's,
  unchanged.
- **The values.** Paradex declares `SnapshotReads::Separate`: its resync is `GET /orders` and
  `GET /positions`, two requests. The
  conformance toy venue (one answer, in frames or from one `<base>/resync` request), SimVenue's
  stubs (one frame, 0049) and the core's synthetic stubs declare `SnapshotReads::OneInstant`.

## Alternatives

- **A field the codec sets on `ResyncBegin`:** not taken, for the reasons above. A venue that
  resyncs in more than one way would need it; none does, and a new record can move the value
  there.
- **Declaring the order of the reads** (orders then positions keeps rule 2's held half and rule
  3; positions then orders keeps rule 2's counted-after half): not taken. Paradex sends both
  requests in one round, so neither order is guaranteed; sequencing them is a codec change, and
  each order still breaks one half of rule 2.
- **Keeping rule 3's absence branch on separate reads:** rejected, it under-counts (the
  table's second row).
- **Placing by the answers' read instants** (a server time on each answer against the fill's
  time): not taken. It needs each read's instant and each fill's time on the matching engine's
  clock; Paradex gives neither.
- **Placing by arrival relative to the answer:** rejected by 0055 already: a REST snapshot beside
  a WebSocket fill stream gives no such order.

## Consequences

- On Paradex a fill of an order the seed knew (an earlier run's order the snapshot shows, one
  absent from it the registry does not hold, or one the registry held before the seed) that
  arrives after the request leaves its market unknown: before the seed the owner's Start is
  refused (`PositionUnknown`) until a later resync seeds it; after the seed, quoting on that
  market stops for the rest of the process. Rare: 0013's cancel-on-disconnect ends an earlier
  run's orders (under 640 ms in FBC-8xr's runs, 0094) and 0080 cancels what a resync shows
  resting before anything is placed. A flat account with no fill in flight seeds and Starts as
  before.
- 0094's "Residual: two reads" is closed for the seed, the first of the owner's two conditions
  on FBC-tyft's loop. Later resyncs' comparison is unchanged: a fill executed between the
  request and a read shows as a `PositionCheck::Desync`, which the consumer judges (0094's stop
  rule).
- An adapter adding a venue states the value; a resync from one request answering both, or
  riding the fills' ordered stream, is `OneInstant`.
- Proof: `crates/fbc-oms/tests/resync.rs`
  (`a_snapshot_of_two_reads_never_places_a_fill_by_its_orders_shown_cumulative_fill`, every
  read order, fill slot, report shape and arrival; `a_venue_declaring_one_read_keeps_the_seeds_placement`;
  `a_snapshot_of_two_reads_still_places_a_fill_by_its_arrival_or_time`),
  `crates/fbc-core/tests/compile_fail.rs` (no default) and
  `crates/venues/fbc-venue-paradex/tests/exec_caps.rs` (Paradex's value and this record).

## What would show this was wrong

A venue declared `OneInstant` whose answer turns out to be assembled from separate reads (a
`Desync` at the first comparison after a seed that had a fill in flight), or Paradex markets
left unknown at start so often that the owner's sessions cannot Start; either reopens it, the
second with a record that sequences the reads or places by read instants.
