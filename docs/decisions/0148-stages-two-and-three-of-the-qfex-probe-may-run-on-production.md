# 0148 — Stages two and three of the QFEX probe may run on production under production_probe behind --production and --sole-trader, each place post-only GTC or limit IOC at the instrument's minimum size, their registries probe runs on a flat hand seed, lifting 0145's refusal for them alone, augmenting 0003, 0067, 0132, 0140, 0143, 0144, 0145 and 0146

Status: accepted
Date: 2026-11-13

## Context

- **The owner's words.** On 2026-11-10 the owner chose production: "let's go ahead and test in
  prod, we'll use small positions sizes and i'll keep minimal amount in there" (in chat; 0143's
  Context and item 5 record it). The owner's keys are production keys, and pre-production
  refused them HTTP 401. Production answered stage one's arm with an `ack` echoing it on both
  accounts (FBC-291a).
- **What is left.** 0143 item 5 leaves stages two and three on production to "a record with
  FBC-9sk2". Only that record lifts 0145's refusal of places, "in the same change" (0145 item 5).
  0144 item 6 and 0146 item 8 also wait on it.
- **Nothing can arm.** The stages seed by hand (0132 item 7), since QFEX's resync seeds nothing
  (0055, 0132 item 5). A hand seed arms only in a registry declared a testnet run (0067), which
  no production registry may be (0143 item 3). 0140 item 6's `CodProbeRun` arms a flat hand seed
  on one market without declaring a testnet. FBC-80xo builds it, for the Hibachi probe.
- **The flatten cannot be planned.** 0132 item 7 closes the taker fill with a reduce-only crossing
  intent (0110). The probe's caps declare no reduce-only, so the planner refuses it in both
  environments.

**The owner's choice.** On 2026-11-13 the owner asked for this record ("Yes, draft it"). Asked
"Approve record 0148 as drafted?", the owner answered "Approve 0148". On 2026-11-14, asked "Add a
bullet to record 0148 saying the flat-only Start rule also covers the Hibachi probe?", the owner
answered "Keep it QFEX-only". Items 3 and 11, the Alternatives, the Consequences, the falsifiers
and the title were then revised to follow that answer and a review; asked "Approve 0148's revised
items 3 and 11, Alternatives, Consequences and falsifiers?", the owner answered "Approve the
revision".

## Decision

1. **The admission.** 0132 item 4's drop test and item 7's stages two and three may run on QFEX
   production under `production_probe`, behind `--production`, as an owner-assisted run with the
   owner present, a minimal balance on the accounts (no amount, 0009) and the stages on each
   named account in turn. Every rule 0132 items 4, 7 and 8 and 0135 items 1 and 2 set for them
   holds unless an item below changes it. Pre-production stays as 0132 has it, but for item 5.
   This is not FBC-cnyh's admission.
2. **The gates, judged in the run.** A run that places begins with the drop test and places
   nothing more unless its own reads in that run pass it. It places a crossing order (the
   post-only through the touch, the taker IOC) only after an `ACK` received in that run echoed,
   byte for byte, a client id it sent.
3. **Arming.** The probe declares `CodProbeRun::owner_supervised_places(market)`, a second
   constructor of 0140 item 6's declaration, on each production place-stage registry it builds,
   one market per registry, and on no other. It seeds that market flat only after every named
   account reads flat with no open order, before the registry's first session; the drop test's
   second session reuses the registry and seeds nothing. Flatten and Wind-down reach Exit after a
   fill has moved the position; Start arms only while it is flat. That Start is this
   declaration's alone: `CodProbeRun::owner_supervised(market)`, the Hibachi probe's, keeps 0140
   item 6's. No production registry carries `TestnetRun`.
4. **0145's refusal, lifted for the stages alone.**
   - A new key, `qfex.exec.production_places`, takes one value, `probe_place_stages`. With it,
     under `production_probe`, the codec encodes a single place only as post-only GTC or limit IOC
     at exactly the instrument's `min_size`, and refuses any other place unsent, `Unsupported`
     (one below the minimum stays `Unencodable`). Batches, amends and reduce-only stay refused.
     The caps are unchanged (0143 item 2), and the minimum is the venue's, so no number enters
     the repository. This is a second departure from 0003, beside 0145 item 3's.
   - Without the key, 0145 stands as written. The key under any other mode is a configuration
     error naming the key, never its value.
   - The probe sets the key only behind `--production`, `--sole-trader` and a place stage, and
     takes the traded instrument's spec only from the adapter's discovery in the same run, on the
     configuration the guard admitted.
5. **The flatten, in both environments.** It replaces 0132 item 7's crossing intent. A position
   any fill of ours leaves is closed by the owner's Flatten into Exit, then at most
   `--flatten-attempts` limit IOCs on the reducing side. Each is built through the registry's
   one pre-trade path outside the planner, as `paradex_testnet_acceptance`'s flatten is,
   classified reducing (`NewOrder::reducing`) with no reduce-only flag, priced no more than
   `--flatten-cap-bps` through the touch read just before it, and judged by Exit and both caps
   (0012, 0063). A residual below the minimum, a refusal or the attempts used up stop the run
   not flat, and the position is the owner's (item 9).
6. **No order of ours meets another.** Before the post-only through the touch, the taker IOC and
   each flatten IOC, the registry holds no other order of ours on the market, open or `Unknown`:
   each earlier one has been cancelled and seen to end, or the probe does not place it. On other
   accounts this rests on the named accounts' reads and the owner's confirmations (0144 item 2,
   0146 item 3). QFEX's client agreement bars wash trading through one or more accounts
   (FBC-9sk2), and the caps claim no self-trade prevention (`matching.stp_scope: None`).
7. **The fences.**
   - `--sole-trader` is required with `--production` and a place stage: no other process, Java or
     Rust, trades any account of the user, as `testnet_quote`'s flag says of its account. 0143
     declined it for stage one because it guards a run that places.
   - Required, with no defaults, refused before anything connects: `--resting-cap-usd` and
     `--inventory-cap-usd`, printed before the first request (0113 items 1 and 2); the distance
     from the touch in bps, at least 20 (0113 item 4); `--flatten-cap-bps`, positive and below
     that distance; `--flatten-attempts`, 1 to 3; the namespace, its lease held in a lease
     directory apart from pre-production's.
   - **The most per account in a run:** X and Y; one post-only through the touch; one taker IOC;
     the flatten IOCs; three post-only orders in stage three, in all. Each registry's `MayPlace`
     list states them, the probe authorizes no place beyond it, and the fill ledger's ceiling is
     built from it (0132 item 5).
   - **Time.** `--registry-span` at most 3600 s. Each session keeps stage one's pacing, so a
     connection's end ends it with no second attempt. FBC-sbbl's result decides any shorter bound
     or keepalive (0146 item 8).
8. **Stops, beyond 0132 item 7's.** Any provider rate refusal; a flatten refused, capped or used
   up; a read showing an order or position the streams did not; Ctrl-C (Stop, cancels by venue
   id); the span bound, cancels begun one `--rpc-timeout-ms` before it.
9. **Hand actions on production's site.** Besides 0143 item 4's hand cancel, the owner may close a
   position a run left, while no connection is armed. Either ends the run, the report names it,
   and the next run's first reads must show every named account flat with no open order.
10. **Report and fixtures.** The report answers 0132's list for each named account, and gives the
    environment, the key, the declarations, the caps, the refusal counts, the full pages
    (FBC-4h1p), the taker fee's sign, every hand action, and the owner's confirmations as
    unchecked. No amount. Captures go in `fixtures/qfex/exec-recorded/` as `drop-production/`,
    `places-production/` and `pages-production/`, scrubbed as FBC-291a's were (identifiers
    blanked, every number of a frame that echoes no request zeroed), except that each fee keeps
    its sign; the traded symbol becomes one placeholder. The owner reads every file first.
11. **How the earlier records read.**
    - 0140's "serves only `hibachi_cod_probe`" covers also these registries, for item 3 alone,
      through `owner_supervised_places`; `owner_supervised` still serves only that probe. 0067's
      rule and falsifier read with that exception. 0140 item 6's "Only
      `CodProbeRun::owner_supervised(market)` makes one" reads with that second constructor.
    - 0143 item 3's no-declaration rule, and 0143's and 0145's "only fbc-oms keeps a place off
      production", are stage one's. 0145 item 1, its item 3 departure, its two breaching probes
      and its falsifiers hold for a `production_probe` codec without the key.
    - 0132 item 7's "the one action on the account through QFEX's site" admits item 9's close.
    - 0143 item 5, 0144 item 6 and 0146 item 8 are settled here.

## Alternatives

- **`TestnetRun` on production:** rejected. It declares a testnet (0067, 0143 item 3).
- **A type apart from `CodProbeRun`:** not taken. These registries arm a flat hand seed as 0140
  item 6's do and differ only in Start, so the declaration takes a second constructor.
- **Item 3's Start under every `CodProbeRun`:** not taken. fbc-oms names no venue (0003), so one
  constructor would carry it to the Hibachi probe, which nothing here needs; the owner kept it
  QFEX-only.
- **Lift 0145 wholesale:** rejected. Every other `production_probe` codec keeps the refusal.
- **No taker fill on production:** not taken. The fee's sign, the IOC statuses and FBC-tfxk's
  fill ordering need one.
- **Keep our own amounts in the fixtures:** not taken. FBC-jdj7 needs the fee's sign, not amounts.
- **A balance bound checked in memory:** not taken. Which member is equity is untested.

## Consequences

- **Order of work.** FBC-80xo builds `CodProbeRun`. A new ticket lands this record with the key,
  the codec's fence and `CodProbeRun::owner_supervised_places` with item 3's flatten and Start
  rules, in one change (0145 item 5). FBC-9sk2 then builds the stages on them, after FBC-jxau,
  FBC-4h1p and FBC-sbbl.
- **The proof.** `tests/probe_mode.rs`: the key's admission and refusals, every place but a
  post-only GTC or a limit IOC at the minimum refused, and 0145 unchanged without the key;
  `tests/conformance.rs`: the probes the key breaches; fbc-oms: under `owner_supervised_places`,
  Flatten after a fill and Start refused off flat, and `owner_supervised`'s Start as 0140 item 6
  has it; `tests/qfex_preprod_probe.rs` (FBC-9sk2): the gates, the `MayPlace` lists, no crossing
  order beside another of ours, the flatten's cap and attempts, and every stop.
- **The docs.** FBC-9sk2, FBC-jdj7, FBC-p7rn and BT-702 read either environment; the examples'
  README, `AGENTS.md`'s Layout and `README.md`'s adapter row follow.
- **Not decided.** FBC-cnyh's admission (0117) and which environment's evidence it rests on; the
  resync's flip (0132 item 5); accounts not named.

## What would show this was wrong

- Any production place from a stage-one run; a production place other than a post-only GTC or a
  limit IOC at the minimum; or a place by a `production_probe` codec without the key.
- `CodProbeRun` on a `qfex_preprod_probe` registry other than a production place stage's, one
  there not made by `owner_supervised_places`, or `TestnetRun` on a production one; a market
  armed on a seed not flat, or Start on a moved position under `owner_supervised_places`.
- A production place after a drop test that did not pass in its run, a crossing order with no
  echo of our client id in its run, or a place beyond its registry's `MayPlace` list.
- A crossing order placed while another order of ours on the market was open or `Unknown`.
- A flatten IOC priced past `--flatten-cap-bps`, or more than `--flatten-attempts` of them.
- A production capture holding an identifying string or an amount, or a provoked rate limit.
