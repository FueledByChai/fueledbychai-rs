# 0141 — Each cancel-on-disconnect variant that takes an arm states in a mandatory field whether the venue offers a disarm, and caps_truthful expects a disarm sent only where it does, augmenting 0003, 0054, 0058, 0097, 0121 and 0132

Status: accepted
Date: 2026-11-05

## Context

`VenueCommand::ArmCancelOnDisconnect(bool)` carries both requests: `true` turns a venue's
cancel-on-disconnect protection on (the arm), `false` turns it off (the disarm, here always this
command, never 0013's disarm of order entry). `OrderCaps::cancel_on_disconnect` says what an
armed connection's drop cancels: `None`, `PerConnection { rearm_on_reconnect,
covers_open_orders }` (0058, 0080), `AccountWide` (0121) or `DeadMan { max_ttl }`. It does not
say whether the venue offers a disarm.

`caps_truthful` (0003) holds a codec to its caps with one probe per capability. For
cancel-on-disconnect it sends the arm and the disarm as controls under every variant but `None`,
and expects both refused `NotSent(Unsupported)` with no effect under `None`. So it reads every
protecting variant as offering a disarm.

The venues' sources differ:

- **Paradex**, `PerConnection`: the `order-cancel-on-disconnect` method page (0054's table) takes
  `enabled`, `true` or `false`, and its encoder writes `{"enabled":false}` for the disarm.
- **Hibachi**, no order entry yet (`exec: None`): FueledByChaiTrading's Java library and
  Hibachi's SDK send `orders.enableCancelOnDisconnect`, and the sources this checkout holds
  (`fixtures/hibachi/signing/README.md`, `src/sign/pack.rs`) show no request that disables it. A
  per-connection venue may offer no disarm.
- **QFEX**, `AccountWide` under 0132 item 6's pre-production probe: its
  [cancel-on-disconnect page](https://docs.qfex.com/websocket/channels/trade/cancel_on_disconnect)
  documents the opt-in, "you need to opt in again whenever you reconnect", and no request that
  turns it off. 0132 item 1 has the codec refuse every disarm unsent, so at most one arm is in
  flight on a connection.

So `caps_truthful` on the QFEX probe breaches its disarm control, and FBC-jdj7's done line (every
suite check passes on the QFEX factory) cannot be met (FBC-3ymj).

No path in the library sends a disarm: the order-entry session sends only
`ArmCancelOnDisconnect(true)`, as its own request (0058, 0121), and fbc-oms's `ControlCommand`
has no disarm (0045). SimVenue's codec writes what the stood-in venue's caps declare, and its
engine turns the protection off on a disarm (0097, 0121 item 4).

On 2026-11-05 the owner chose that the cancel-on-disconnect capability grows a mandatory field
saying whether a disarm request is offered, rather than `caps_truthful` treating the disarm as
optional for `AccountWide` (FBC-3ymj's notes).

## Decision

1. **The field.** `CancelOnDisconnect::PerConnection`, `AccountWide` and `DeadMan` each gain
   `offers_disarm: bool`, mandatory like every capability field (0003): neither the type nor
   any type holding it has a `Default` or `#[non_exhaustive]`. `AccountWide` becomes
   `AccountWide { offers_disarm }`.
   - `true`: the venue documents a request that turns the protection off on the connection, and
     the codec sends `ArmCancelOnDisconnect(false)` as it.
   - `false`: no venue document names one, and the codec refuses every disarm
     `NotSent(Unsupported)` with no effect.
   - `None` takes no arm, so it has no disarm to state and carries no field.
   - `CancelOnDisconnect::offers_disarm()` reads the field, and is `false` for `None`.
   - A variant added later that takes an arm states the field too, or its record says why
     `offers_disarm()` is `false` for it by definition, and `caps_truthful` probes its disarm
     as that answer says.
2. **Every variant that takes an arm states it, not `AccountWide` alone.** This is the narrowest
   reading under which no variant's disarm is assumed. Hibachi's sources show a per-connection
   candidate with only an enable. No venue declares `DeadMan` (0058 refuses it), and a dead-man
   venue's disarm is no more certain than another's. A field on `AccountWide` alone would leave
   `caps_truthful` expecting a disarm from the other two.
3. **What each declares.** Each value cites its source (0003).
   - Paradex: `offers_disarm: true`, from the method page's `enabled` parameter, cited in its
     `ExecCaps` under 0054's rule that every value there is cited.
   - The conformance toy: `offers_disarm: true`. It writes the disarm as `cod|rpc=<n>|on=0`
     (0058).
   - The QFEX probe: `AccountWide { offers_disarm: false }`, from the page's silence and 0132
     item 1.
   - Hibachi, Binance USD-M and QFEX outside the probe declare `exec: None`, so there is nothing
     to state. A later Hibachi `ExecCaps` states the field from what FBC-xvtr's probe finds.
4. **What `caps_truthful` expects.**
   - The arm is unchanged: sent as a control under every variant but `None`, refused under
     `None`.
   - The disarm is sent as a control, `control: protection (disarm)`, where the variant offers
     one.
   - Where the variant takes an arm and offers no disarm, it is refused `NotSent(Unsupported)`
     with no effect, probed as `OrderCaps.cancel_on_disconnect offers no disarm`.
   - Under `None` it is refused as before, probed as
     `OrderCaps.cancel_on_disconnect has no protection (disarm)`.
   - The dead-man refresh is unchanged.
5. **No session behaviour changes.** The session sends no disarm, so `ExecSession` takes
   `PerConnection` and `AccountWide` whatever the field says, under the conditions 0058 and 0121
   set. A record that gives a consumer a disarm (0045) reads the field first.
6. **SimVenue keeps to it (0097).** Its codec refuses `ArmCancelOnDisconnect(false)`
   `NotSent(Unsupported)`, nothing written, where the stood-in caps offer no disarm. It already
   refuses an arm under `None` and a refresh without a dead-man timer. A disarm frame written
   anyway is refused by its engine (`no_protection`, `RejectKind::Other`), and the protection
   stays on. Where the caps offer a disarm, the engine turns the protection off as before.
   0121 item 4's "Disarm and reconnect remove the protection" now holds where a disarm is
   offered.

## Alternatives

- `caps_truthful` accepting the disarm sent or refused for `AccountWide`: the owner rejected it
  on 2026-11-05. Such a probe proves nothing either way, and a consumer reading the caps could
  not tell whether a disarm exists.
- The field on `AccountWide` alone: rejected. The disarm of the other protecting variants would
  stay an assumption the type cannot state, which 0003 rules out, and Hibachi's sources already
  show a per-connection candidate with no disarm.
- A field of `OrderCaps` beside `cancel_on_disconnect`, such as `cod_disarm: Support`: rejected.
  It could declare a disarm for `None`, which takes no arm, and the disarm belongs to the
  protection it turns off.
- A variant per combination, such as an account-wide variant without a disarm: rejected. It
  doubles the variants for each such fact.
- `Support` in place of `bool`: rejected. The variants' other fields are flags, and `Support`
  says nothing more here.
- QFEX's codec sending `{"cancel_on_disconnect":false}` as a disarm: rejected. No page documents
  it, and 0132 item 1 refuses it so that one arm at most is in flight.

## Consequences

- Every literal naming `PerConnection`, `AccountWide` or `DeadMan` states the field, and the
  compiler lists any that do not. fbc-core's `tests/ui_caps/` proves a literal without it does
  not compile. A match on `AccountWide` names its field or `..`.
- fbc-conformance's tests prove both cases. The toy, which declares a disarm and sends it,
  passes. A broken toy that declares none and still sends it fails, naming
  `OrderCaps.cancel_on_disconnect offers no disarm`. One that declares none and refuses it
  passes, and one that declares a disarm and refuses it fails its control.
- fbc-venue-qfex's `tests/conformance.rs` runs `caps_truthful` on the probe's caps, and it
  passes.
- A venue that later documents a disarm flips its value, citing the document.

## What would show this was wrong

- A venue whose disarm is offered only in some states, such as before any order rests, which one
  flag cannot express.
- A codec that sends a disarm its caps do not offer, or refuses one they do, and still passes
  `caps_truthful`.
- QFEX documenting a request that turns the protection off. The probe's caps still declare none
  while 0132 item 1 refuses every disarm, and a new record decides whether the codec sends it.
- A session, a consumer path or SimVenue sending a disarm where the caps offer none.
