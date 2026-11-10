# 0143 — Stage one of the QFEX probe may run on production under a production_probe mode admitting exactly QFEX's production origins, selected by the probe's --production flag, its registries built with no testnet declaration, augmenting 0117, 0122 and 0132

Status: accepted
Date: 2026-11-10

## Context

0132 fixes how QFEX order entry is probed before production admits it.

- **The mode.** Item 6's `preprod_probe` declares the probe's `ExecCaps` and builds its codec
  only when every QFEX origin is exactly its pre-production origin, or all three are loopback
  for a test stub. It refuses every `qfex.com` host. Under it, and nowhere else, an
  `ExecSession` may open QFEX's plan, departing from 0117's ordering and 0122's item 4.
- **The probe.** Item 7's owner-assisted run (0067). Its stage one (FBC-l931,
  `qfex_preprod_probe` in `crates/fbc-examples`) arms cancel-on-disconnect once per account and
  places nothing. It builds each registry with
  `Registry::for_testnet_run(TestnetRun::owner_assisted())`.
- **The environment.** The owner chose pre-production on 2026-10-29, "with primary and
  subaccount credentials QFEX supplies" (0132's Context). The
  [builder wallet registration page](https://docs.qfex.com/api-reference/builder-wallet-registration)
  that names the pre-production origins also says to "Use the environment URLs and credentials
  supplied by QFEX".

The owner's keys are production keys. FBC-291a's first run of stage one was refused HTTP 401 at
`GET /user/subaccounts` on `https://api.qfex.io`, its first request, before anything armed. On
2026-11-10 the owner chose to run the probe on production instead, keeping a minimal balance on
the accounts it reads and arms (FBC-3prm). This record decides stage one only.

**The production origins.**

- REST: `https://api.qfex.com`, the one server the trading REST reference lists, as 0132's
  Context read it on 2026-10-29 from that reference's pages (for one,
  [user positions](https://docs.qfex.com/api-reference/rest/user/user-positions)).
- The trade socket: `wss://trade.qfex.com`, which FBC-3prm gives as the production server of
  the [trade AsyncAPI](https://docs.qfex.com/websocket/trade.yaml). This record did not read
  the AsyncAPI's servers again: no agent connects to a QFEX host for this ticket. The owner
  confirms the host before the first production run. The adapter's `qfex.trade_url` has named
  it as its example since FBC-77aa. A trade origin QFEX does not serve would refuse the
  connection, so the run would arm nothing.
- Market data: `wss://mds.qfex.com`, the origin both of the adapter's public captures were read
  from (`fixtures/qfex/README.md`, 2026-10-20 and 2026-11-04), and `qfex.mds_url`'s example
  since FBC-j9xw.

**What stands in the way.**

- 0132 item 6's guard refuses every production origin, and its "What would show this was wrong"
  names a production origin passing it.
- 0117 and 0122's item 4 let no `ExecSession` open QFEX's plan outside item 6's departure.
- 0067 says a consumer trading a live venue never makes the testnet declaration, which 0132 item
  7 gives the probe's registries.
- 0132 item 1 waits for "a pre-production capture", and item 7's hand cancel is "on QFEX's
  pre-production site".

**What stage one sends.** Its own reads, and each session's one arm, which may cancel the
account's open orders (0132 item 2). The probe starts a session only when every account of the
user shows no open order and flat positions (item 7). It submits no command. No market can arm
in its registries either: QFEX's resync is untrustworthy and seeds nothing (0055, 0132 item 5),
and a market seeded by hand arms only in a registry declared a testnet run (0067).

## Decision

1. **The owner's choice.** Stage one of 0132 item 7's probe may run on QFEX production, the
   owner keeping a minimal balance on the accounts it reads and arms. QFEX supplies
   pre-production credentials, and the owner's are production keys. This record gives no
   amount (0009).
2. **A production probe mode.** `qfex.exec.mode` is absent, `preprod_probe` or
   `production_probe`. Any other value is a configuration error.
   - Under `production_probe` each QFEX origin is exactly its production origin, a trailing `/`
     ignored: `qfex.trade_url` `wss://trade.qfex.com`, `qfex.rest_url` `https://api.qfex.com`
     and `qfex.mds_url` `wss://mds.qfex.com`. Or all three are loopback (`127.0.0.0/8` or
     `[::1]`) for a test stub, as `preprod_probe` admits.
   - Refused before anything is planned, naming the key and never its value: a mix of loopback
     and production origins, any `qfex.io` host (so every pre-production origin), and any other
     origin, a production host written otherwise included.
   - `preprod_probe` is unchanged. It admits exactly the pre-production origins or loopback, and
     still refuses every `qfex.com` host.
   - Everything else 0132 item 6 fixes holds under both modes: the settings it requires, the
     probe's `ExecCaps`, `Readiness::Record`, one order-entry connection per account, and the
     probe's refusal of loopback origins with a proxy.
   - Under `production_probe` an `ExecSession` may open QFEX's plan, for the owner-assisted
     probe alone. This departs from 0117's ordering and 0122's item 4 for production as 0132
     item 6 does for pre-production. A configuration with no probe mode stays `exec: None`.
   - The adapter says which environment a mode names and which side of its guard the origins
     are on (`probe::Environment`, `probe::Origins`), from the configuration the guard
     admitted.
3. **The probe's `--production` flag.**
   - With it, the probe sets `production_probe`, and `--trade-url`, `--rest-url` and
     `--mds-url` default to the production origins. Without it nothing changes.
   - A run without it naming a production origin, or with it naming a pre-production one, is
     refused by the adapter's guard before anything connects. Every origin the probe takes
     passes that guard first (0132 item 7).
   - Every check 0132 item 7 makes runs unchanged.
   - A production run builds each registry without the owner-assisted testnet declaration.
     0067 says a consumer trading a live venue never makes it, and stage one seeds and arms no
     market, so it needs none. A pre-production run builds its registries as 0132 item 7 says.
   - Its report and its evidence name the environment the mode names, beside where the origins
     pointed, and say for each registry whether it carries the declaration.
   - The program keeps its name, `qfex_preprod_probe`.
4. **0132's pre-production wording, for a production run.**
   - Item 1: the codec decodes the arm's answer once a probe capture holding it, from either
     environment, lands under `fixtures/qfex/exec-recorded/`. The capture's shape decides as
     item 1 says, whichever environment it came from.
   - Item 7's hand cancel: on QFEX's production site for a production run, under the same
     conditions. Stage one places nothing, so it should never be needed.
   - Fixtures from production are scrubbed exactly as now (0009). The owner reads every file
     before committing it, and the fixtures' README names the environment they came from.
   - A pre-production capture is committed as `fixtures/qfex/exec-recorded/arm/`, as 0132 and
     FBC-291a say, and a production one as `fixtures/qfex/exec-recorded/arm-production/`. So a
     capture from each environment can be kept and compared.
   - Item 7's "runs only as an owner-assisted run (0067)": a production run is owner-assisted,
     but it is not 0067's declared testnet run (item 3).
   - 0132's Context ("Production keeps `exec: None`"), item 4 and Consequences ("Production
     stays `exec: None`") hold for every production configuration but `production_probe`'s.
   - The Order of work's "only outside `preprod_probe`", for FBC-blcw's refusal of the arm,
     FBC-zeb0's `exec_codec` `None` and FBC-zeb0's rule that a private state frame ends the
     epoch, now means outside either probe mode. The codec is the same in both, so what
     FBC-jxau lands "under `preprod_probe`" it lands under `production_probe` too.
   - 0132's "A production QFEX origin passing the probe mode's guard" now reads as one passing
     `preprod_probe`'s guard. This record's own falsifiers say what would show either guard
     wrong.
5. **Not decided here.**
   - Stages two and three on production. Their place stages seed by hand, which 0067 admits
     only in a declared owner-assisted testnet run. A record with FBC-9sk2 decides it. The
     owner has said those stages would use the smallest sizes and a minimal balance.
   - 0132 item 5's later flip of the resync to trustworthy.
   - Admitting QFEX order entry on production under 0117 (FBC-cnyh). `production_probe` is a
     probe mode, not that admission.

## Alternatives

- **Wait for pre-production credentials:** the owner declined.
- **Widen `preprod_probe`'s origin list to the production origins:** rejected. A mode that names
  its environment keeps a pre-production configuration from reaching production silently: a
  production origin is still refused under `preprod_probe`, and a pre-production one under
  `production_probe`.
- **Separate environment-variable names for production credentials, as Paradex's mainnet
  samples have:** rejected. The `--production` flag and the exact origins carry the guard, and
  it would change `src/auth_qfex.rs`, a review path, for no added guard. QFEX refused a
  production key on pre-production (FBC-291a's 401). No run has shown the other direction, and
  the guard does not rest on it.
- **An acknowledgement flag beside `--production`, as `testnet_quote --mainnet` takes
  `--sole-trader`:** not taken. That flag guards a run that places. Stage one places nothing,
  and before every session it reads every account of the user and starts only when none holds
  an open order or a position. What the arm could still cancel is an order another client
  places between those reads and the arm. 0132 item 2's condition rules that out, and the
  `AccountWideCodAuthority` the probe gives asserts it, on production as on pre-production. The
  owner may still ask for such a flag.
- **Keep the testnet declaration on a production run:** rejected. 0067 rules it out for a live
  venue, and stage one arms no market that would need it.

## Consequences

- **What the mode opens.** Under `production_probe` the factory declares the probe's
  `ExecCaps`, the same as under `preprod_probe` and places included, and builds its codec on
  QFEX's production origins. The library cannot tell the probe from another consumer that sets
  the mode, as with `preprod_probe`.
  - **Today** nothing can place under either mode. No frame answers the arm (0132 item 1), so
    every arm ends `Unknown` at its deadline, the epoch ends, and 0058 holds every place.
  - **Once FBC-jxau decodes the arm's answer**, which item 4 lets a production capture drive,
    that bar is gone under `production_probe` too. What then keeps a place off production is
    fbc-oms refusing to arm: QFEX's resync seeds nothing (0055, 0132 item 5), and a market
    seeded by hand arms only in a registry declared a testnet run (0067).
  - **That declaration is the consumer's own call.** 0067 is a rule a consumer follows, not a
    check the library makes. Only that rule and the probe's own code, which builds its
    production registries with none, keep it off production. The adapter refuses no place
    under `production_probe`.
  - Whether the adapter should refuse every place under `production_probe` until a record
    decides stage two on production is left to FBC-9sk2's record. That record should settle it
    before FBC-jxau's decoding runs on production.
- **The proof.**
  - `crates/venues/fbc-venue-qfex/tests/probe_mode.rs`: `production_probe` admits exactly the
    three production origins, with or without a trailing `/`, and all three loopback. It refuses
    a pre-production origin, a mix of sides and any other origin (a trailing dot, another case,
    a punycode or non-ASCII lookalike and an IPv4-mapped loopback included), in every factory
    method and in the probe's own connections, each refusal naming the key and never the value.
    `preprod_probe` still refuses every `qfex.com` host.
  - `crates/fbc-examples/tests/qfex_preprod_probe.rs`: a `--production` run, its options
    parsed from that command line, against the QFEX-shaped stub completes stage one. Its report
    and evidence name production, and its registries carry no testnet declaration.
    `--production` with a pre-production origin, and a run without it naming a production
    origin, are refused before anything connects.
- **The docs.** `crates/fbc-examples/README.md` says how the owner runs it on production.
  FBC-291a's run text follows once this record is on main.
- **The balance.** What the accounts hold is the owner's, and stage one trades none of it. An
  arm may cancel an open order (0132 item 2). The reads before each session show none open,
  and 0132 item 2's condition rules out one placed by another client after them.

## What would show this was wrong

- A production origin passing `preprod_probe`'s guard, or a pre-production origin passing
  `production_probe`'s.
- A production run of the probe that places an order, or a market armed in one of its
  registries.
- A production run's registry built with the testnet declaration, or any `production_probe`
  session whose registry carries it.
- A place sent on QFEX production under `production_probe` before a record admits places
  there.
- A consumer other than the probe setting `production_probe`.
- QFEX answering the arm differently on production and on pre-production, once both are
  captured. A capture from one environment would then not decide item 1 for the other.
- A production fixture holding an identifying string or an amount of the account once
  scrubbed.
