# 0151 — The QFEX probe's place stages run on production alone, cancel inside the drop test only at Ctrl-C or a fill of ours, count X and Y ended on three post-drop reads, carry a stop to the next run, and print the caps' values on stdout alone, augmenting 0132 and 0148

Status: accepted
Date: 2026-11-16

## Context

- **What the place stages rest on.** 0148 admits 0132 item 4's drop test and 0132 item 7's stages
  two and three on QFEX production. FBC-9sk2 builds them in slices: FBC-0lng, FBC-be57,
  FBC-oxjs, FBC-t45e, FBC-rpjn, FBC-c6kw, FBC-23ch, FBC-77r8, FBC-su9i, FBC-ou9g and FBC-4plj.
  Where the two records leave a choice, or two of their items pull apart, the slices need a
  reading. FBC-fqv5 puts thirteen readings to the owner, numbered as the slice tickets cite them
  ("item n of FBC-fqv5's record"). Items 5 and 10 decide the most.
- **X and Y.** They are 0132 item 4's two post-only orders, both far from the touch. X is placed
  in the drop test's first session and not cancelled before that session's graceful close. Y is
  placed in the second session after its arm, before that session's connection drops abruptly.
- **Where the records pull apart.**
  - The account listing: 0132 item 7 lists every account through `GET /user/subaccounts`, 0144
    replaced that for stage one only (0144 item 6), and 0148 items 1 and 6 settle it for
    production alone (item 1).
  - The drop test's cancels: 0132 items 4 and 7 send none of X or Y, and 0148 item 8's Ctrl-C
    cancels by venue id (item 2).
  - X's and Y's ends: QFEX's resync ends no order (0132 item 5), and 0148 item 6 asks each earlier
    order of ours to be seen to end before a crossing order or a flatten IOC (item 5).
  - The caps: 0148 item 10 lists "the caps" in the report, and also says "No amount" (0009;
    item 10).
- **The owner's ruling.** On 2026-11-16, in the main session's own chat, the owner was asked
  "Approve FBC-fqv5's 13 readings as proposed?" and answered "Approve all 13". The readings were
  put to the owner as FBC-fqv5 states them, each with a summary in plain terms. So every item
  below is decided as FBC-fqv5 proposes it, and no item was overruled.

## Decision

1. **Production only, with the fences alike in every place-stage run.**
   - 0148 item 1 keeps pre-production as 0132 has it, but for 0148 item 5. So on pre-production
     the place stages would list every account of the user through `GET /user/subaccounts`, as
     0132 item 7's "Its credentials" has it.
   - No path of the probe now makes that listing. 0144 replaced it for stage one (0144 item 6),
     and 0148 item 6 rests the other accounts on the named accounts' reads, on production alone
     (0148 item 1). The owner's keys have not made it either: production answered it HTTP 403
     (0144's Context), and pre-production refused the keys HTTP 401 (0148's Context).
   - So the probe refuses `--place-stages` without `--production`, naming the reason, and builds
     no pre-production place-stage path until a record decides that listing anew. Every
     place-stage run is a production run, under the same fences.
   - 0148 item 7's lease directory "apart from pre-production's" is met by a production
     place-stage lease directory that is tagged production on its first use. A lease directory
     tagged for another environment is refused, and so is the testnet samples' default lease
     directory.
2. **Which stops cancel inside the drop test.**
   - 0132 item 7 says the drop test's sessions send no Stop cancels, "since the drop is to cancel
     X and Y". 0132 item 4 says nothing cancels X or Y from the first session's arm to the read
     after the drop. 0148 item 8 has Ctrl-C stop with cancels by venue id.
   - Inside the drop test, only Ctrl-C and a fill of ours cancel, by venue id. Either one ends
     the measurement, and the drop test is then not passed.
   - Every other stop in a drop session sends no cancel and ends the session by a graceful close.
     The after-session read of every named account follows, and an order of ours still resting
     is reported for the owner's hand cancel (0132 item 7's, on production's site as 0143 item 4
     reads it).
   - The span bound inside the drop test cancels nothing (FBC-5hif) and fails the test. 0148 item
     8's cancels begun one `--rpc-timeout-ms` before the span bound are for sessions outside the
     drop test.
3. **The fill ledger's count bound, checked once the minimum size is known.**
   `--ledger-max-entries` is checked against each place-stage registry's ceiling after the public
   `/refdata` read gives the instrument's minimum size, and before any account connects (0132
   item 5). The ceiling counts the fills the registry's `MayPlace` list (0148 item 7) can bring,
   which depends on that minimum.
4. **One wait and one cadence for every read.**
   - `--agree-wait-ms` bounds how long a never-arming read may lag the streams, in 0132 item 7's
     comparisons of the streams against the reads, and how long the reads after the drop wait.
   - `--read-interval-ms`, at least 1000, spaces two reads of one comparison, and a comparison
     makes at most 10 reads.
   - That cadence also bounds REST `GET /user/positions`, which no declared bucket charges (0132
     item 5, and its Consequences' cost), so that no read provokes a rate limit (0135 item 1).
5. **Registries per step, and what "seen to end" means across them (0148 item 6).**
   - Each named account gets four registries: drop, cross, take and pages.
   - X and Y stay counted resting on the drop registry after the drop, since their ends come only
     from reads: the resync ends no order (0132 item 5's missed ends).
   - For the later registries' crossing orders (the post-only through the touch and the taker
     IOC) and flatten IOCs, X and Y count as ended once the reads after the drop show each of
     them absent from `get_user_orders`, named by no `get_user_trades` row, and terminal in
     `GET /user/historic-orders`.
   - Terminal there means a row of `GET /user/historic-orders` names the order by its client id,
     whatever its `terminal_status`, which is recorded. That endpoint "lists ended orders by
     client id", and its `terminal_status` values are listed nowhere (0132 item 5's missed
     ends). The `get_user_trades` condition already shows that neither filled.
   - For X and Y alone, this is the fourth read 0132 item 5 leaves to a later record. For every
     other order, the probe still reads `GET /user/historic-orders` as evidence (0132 item 5).
   - No other order counts as ended this way. Any other order of ours on the market, open or
     `Unknown` in any registry of any named account, a crossing post-only left `Unknown`
     included, still blocks a crossing order or a flatten IOC.
6. **A stop persists across runs.**
   - On any stop but `NoEcho`, the run writes a stop marker in its lease directory. `NoEcho`
     (FBC-su9i's stop) ends a run whose drop tests ran while no `ACK` in it echoed one of our
     client ids byte for byte, so that 0148 item 2 admits no crossing order. It is not a fault.
   - A later `--place-stages` run refuses to start without
     `--reconciled <hand-cancel|hand-close|both|none>`, and refuses `--reconciled` when no marker
     exists. The values name 0132 item 7's hand cancel (on production's site, 0143 item 4),
     0148 item 9's hand close, both, or neither.
   - With it, the run reads every named account first, clears the marker only if those reads are
     clean, and reports the declaration as unchecked.
   - This carries across runs 0132 item 7's "places nothing more on QFEX until a new record or the
     owner's reconciliation decides", and 0148 item 9's rule that the next run's first reads show
     every named account flat with no open order.
7. **The fixture slices' names and a fee's sign.** The fixture slices under
   `fixtures/qfex/exec-recorded/` are named `drop-production/`, `places-production/` and
   `pages-production/` (0148 item 10). A fee kept in a scrubbed capture is written as -1, 0 or 1
   by its sign, which is how 0148 item 10's "each fee keeps its sign" reads beside its "No
   amount".
8. **A taker IOC left Open with a fill.** The adapter decodes QFEX's `IOC_PARTIALLY_FILLED` with
   a remainder as Open (0132 item 8, "an `IOC_*` status by the quantities it states"). A taker
   IOC left Open with a fill stops the run not flat, with no adapter change, until FBC-jdj7.
9. **Unexpected fills.**
   - Any fill of ours but the taker IOC's and the flatten IOCs' stops the run: a maker fill of X,
     Y or a stage-three order, or a crossing post-only that fills.
   - While its session is open, every other order of ours is cancelled by venue id and seen to
     end. Then the owner's Flatten into Exit and the flatten IOCs follow, under one per-account
     budget of `--flatten-attempts` shared by the account's registries (0148 item 5).
   - A fill seen only after the drop, with no session open, stops the run not flat, and the
     position is the owner's (0148 items 5 and 9).
10. **What is said of the caps and the other fences.**
    - Both cap values are printed on stdout, in the `CAPS` line before the first request (0148
      item 7; 0113 item 2).
    - The report, the evidence and the fixture slices state the caps, the distance from the
      touch, the flatten cap and the attempts only as fences met, never as values: the caps set,
      positive and admitting three minimum sizes; the distance at least 20 bps; the flatten cap
      below the distance; the attempts 1 to 3.
    - This reads 0148 item 10's "the caps" against its "No amount" and 0009.
11. **Held Stop cancels.**
    - When a session's Stop cancels are held by a `RATE_LIMITED` hold or refused locally, the
      session ends by an abrupt drop if this account's drop test passed in this run, since the
      drop is then shown to cancel, and by a graceful close otherwise.
    - Either way the after-session read follows and the run stops.
    - This is how 0132 item 7's "Before a session stops, it cancels every order of ours its
      registry can name" reads when those cancels cannot go out.
12. **The proof files.** `place_stages.rs` and `place_registries.rs` under
    `crates/fbc-examples/tests/qfex_preprod_probe/`, each a submodule that
    `tests/qfex_preprod_probe.rs` declares by path and so part of its test binary, meet 0148's
    Consequences, which name `tests/qfex_preprod_probe.rs`.
13. **The drop verdict and stray orders, beyond 0132 items 4 and 7.**
    - The drop test passes only if `GET /user/historic-orders` also shows X and Y terminal, as
      item 5 reads it, besides 0132 item 4's two reads: absent from `get_user_orders` on a
      connection that never arms, and named by no `get_user_trades` row. A stricter verdict can
      only place less.
    - An `order_response` naming no order of ours, on accounts `--sole-trader` declares ours
      alone, stops the run, a stop named `StrayOrder`.

## Alternatives

- **A pre-production place-stage path reading the named accounts as production does (item 1):**
  not taken. 0148 item 1 keeps pre-production as 0132 has it, so the place stages there rest on
  0132 item 7's listing through `GET /user/subaccounts`, not on the named accounts' reads, and
  no record has decided that listing anew.
- **Stop cancels at the drop test's span bound, as 0148 item 8 has them elsewhere (item 2):** not
  taken. A cancel of X or Y would leave nothing for the drop to show (0132 item 4).
- **X and Y never count as ended (item 5):** not taken. 0148 item 6 would then refuse every
  crossing order and every flatten IOC on that account, and stage two's crossing post-only and
  taker IOC would never be placed.
- **A stop held in memory only (item 6):** not taken. The next run would start with nothing
  telling it that the last one stopped, and 0132 item 7's "places nothing more" would hold only
  within a run.
- **The cap values in the report, as 0148 item 10's "the caps" reads alone (item 10):** not
  taken. They are live sizes, which 0009 keeps out of the repository, and 0148 item 10 says "No
  amount".
- **A graceful close whenever Stop cancels are held (item 11):** not taken. Once this run's drop
  test has shown the drop to cancel, a graceful close would leave the orders for the owner's hand
  cancel, where the drop clears them.
- **`GET /user/historic-orders` as evidence only, and a stray `order_response` recorded rather
  than a stop (item 13):** not taken. A stricter verdict can only place less, and an order event
  naming no order of ours, on accounts declared ours alone, shows that something else trades.

## Consequences

- **Which slices rely on which item.** Item 1: FBC-oxjs, FBC-c6kw and FBC-23ch, which builds
  the lease directory's production tag. Item 2: FBC-23ch and FBC-77r8. Item 3: FBC-c6kw. Item
  4: FBC-c6kw and FBC-23ch. Item 5: FBC-su9i and FBC-ou9g. Item 6: FBC-23ch and FBC-9sk2. Item
  7: FBC-t45e and every slice that writes a fixture slice. Item 8: FBC-ou9g. Item 9: FBC-oxjs,
  FBC-23ch, FBC-77r8 and FBC-ou9g. Item 10: FBC-c6kw, FBC-ou9g and FBC-4plj. Item 11: FBC-23ch
  and FBC-su9i. Item 12: FBC-oxjs, FBC-c6kw, FBC-23ch, FBC-77r8, FBC-su9i, FBC-ou9g and
  FBC-4plj. Item 13: FBC-23ch and FBC-77r8.
- **Built before this record.** FBC-oxjs, landed as 3d8ab8b9, and FBC-t45e and FBC-c6kw were
  built to the proposed readings before this record. The loopback-only gate keeps every place
  off QFEX meanwhile: from FBC-c6kw through FBC-ou9g, `--place-stages` is refused, naming the
  reason, unless every origin the run uses is loopback, and FBC-4plj's ticket lifts it once
  every stop has landed. FBC-23ch, the first slice that places, waits for this record.
- **No item overruled.** No slice ticket changes for a ruling, and no new ticket is needed for a
  slice built to an overruled item. Once this record lands, every ticket that cites "item n of
  FBC-fqv5's record", whether or not listed above, is updated in Beads to cite this record's
  item n.
- **Code.** Nothing under `crates/` changes here. The slices carry each item and its proof.
- **What stays open.** Whether the place stages ever run on pre-production. A record deciding
  their account listing there anew would come first, and it does not block the build. As in 0148:
  FBC-cnyh's admission (0117), the resync's flip (0132 item 5) and accounts not named.

## What would show this was wrong

- **Item 1.** A place-stage run on pre-production, or one holding its leases in a directory
  tagged for another environment or in the testnet samples' default lease directory.
- **Item 2.** In the drop test, a cancel of X or Y sent other than at Ctrl-C or a fill of ours, a
  cancel at the span bound, or a drop test passed after either stop.
- **Item 3.** An account connected before the count bound was checked, or a place-stage registry
  whose fills passed its ledger's count bound.
- **Item 4.** Two reads of one comparison closer than `--read-interval-ms`, more than 10 reads in
  one comparison, a read taken as agreeing with the streams after lagging them past
  `--agree-wait-ms`, a wait for the reads after the drop that went on past it, or a provider rate
  refusal of REST `GET /user/positions`, which only this cadence bounds.
- **Item 5.** A crossing order or a flatten IOC of a registry after the drop registry (cross,
  take or pages) placed while X or Y was not shown ended by all three reads; a crossing order or
  a flatten IOC placed beside any other order of ours open or `Unknown`; X or Y found resting or
  filled after the reads counted it ended; or a row of `GET /user/historic-orders` naming an
  order of ours by its client id whose `terminal_status` shows the order had not ended.
- **Item 6.** A place-stage run that placed after an earlier run's stop without `--reconciled`
  and clean reads, or a marker cleared on reads that were not clean.
- **Item 7.** A committed capture holding a fee's amount, or a place-stage slice under another
  name.
- **Item 8.** A run that went on after a taker IOC was left Open with a fill, or FBC-jdj7's
  captures showing that status means something else.
- **Item 9.** A fill of ours other than the taker IOC's and the flatten IOCs' after which the
  run placed anything but cancels and flatten IOCs, or more flatten IOCs on one account in a run
  than `--flatten-attempts`.
- **Item 10.** A cap value, the distance, the flatten cap or the attempts in the report, the
  evidence, a fixture slice or anywhere in the repository, or a request sent before the `CAPS`
  line.
- **Item 11.** A session whose Stop cancels were held ending by an abrupt drop before its
  account's drop test passed in that run, or an order of ours left resting after a drop that the
  run's drop test had shown to cancel.
- **Item 12.** A proof 0148's Consequences ask for that is not in `tests/qfex_preprod_probe.rs`'s
  test binary, or Cargo building either file as a target of its own.
- **Item 13.** A drop test passed without `GET /user/historic-orders` showing X and Y terminal,
  a run that went on after an `order_response` naming no order of ours, or an order shown
  terminal there that a later read shows resting.
