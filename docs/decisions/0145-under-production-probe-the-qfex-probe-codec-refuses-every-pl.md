# 0145 — Under production_probe the QFEX probe codec refuses every place, batch place and amend unsent until a record admits places on production, its caps unchanged, augmenting 0003 and 0143

Status: accepted
Date: 2026-11-13

## Context

**What 0143 opened.** Under `production_probe` the factory declares the probe's `ExecCaps`,
places included and the same as under `preprod_probe`, and builds the same codec on QFEX's
production origins (0143 items 2 and 4 and its Consequences).

**What keeps a place off production.**

- **Today** two bars hold every place. No frame answers the arm, so every arm ends `Unknown` at
  its deadline and the epoch ends (0132 item 1). And the probe's resync pushes no `ResyncEnd`, so
  0058's gate never opens. FBC-jxau lifts both, and FBC-291a's production capture now lets it
  decode the arm's answer (0143 item 4).
- **After FBC-jxau** only fbc-oms keeps a place off production, by refusing to arm a market. That
  rests on 0067's testnet declaration, a rule the consumer follows and not a check the library
  makes (0143's Consequences).

**What 0143 left open.** 0143 says "The adapter refuses no place under `production_probe`". It
leaves whether the adapter should refuse every place there to FBC-9sk2's record, which "should
settle it before FBC-jxau's decoding runs on production". FBC-9sk2 waits on FBC-jxau, so it
cannot settle it first. FBC-5d7x settles it now, before FBC-jxau.

**What the codec refuses already.** In both modes it refuses batch places, amends, cancel-many
and cancel-all `NotSent(Unsupported)`, because the probe's caps leave them out (0132 item 6). So
the change under `production_probe` is to single places.

**What the caps promise.** 0003 has `caps_truthful` refuse unsent what the caps leave out, and
the conformance kit's controls hold an adapter to send each operation the caps declare
(`caps_truthful.rs`). 0072 is the nearest precedent for a codec refusing what its caps declare:
Paradex's read-only codec refuses every command under the same caps as its order-entry codec.

**The owner's choice.** On 2026-11-13 the owner was asked "Approve records 0145 and 0146 as
drafted?" and answered "Approve both".

## Decision

1. **The refusal.** Under `production_probe` the probe codec refuses every place, batch place
   and amend `NotSent(Unsupported)`. Nothing is written and the codec's state does not change.
   - This holds whatever the order's shape, and with loopback origins as with production ones,
     since the mode names the environment (`probe::Environment::Production`).
   - Reducing and reduce-only orders are refused too. No `production_probe` market can arm
     (0143's Consequences), so this blocks no Flatten the library could send. A position on such
     an account is the owner's to close on QFEX's site.
   - Called directly, the codec refuses a place before it checks the connection, as it refuses
     every command it does not send. Inside an `ExecSession`, 0058's gate still answers a place
     `Disconnected` until the arm is accepted and the resync ends.
2. **What does not change.**
   - Cancels, queries, the resync, and the arm with its one-arm rule are sent as under
     `preprod_probe`. Cancel-many and cancel-all stay refused as 0132 item 6 has them.
   - `preprod_probe` and the recording-only codec are unchanged.
   - The caps are unchanged and stay alike in both modes, as 0143 item 2 fixes them.
3. **The departure from 0003.** Under `production_probe` the codec does not send the placement
   its caps declare, and it refuses a placement carrying their declared conflict (post-only with
   IOC) `Unsupported` rather than `FlagConflict`. `caps_truthful` holds a `production_probe`
   codec to everything else, and the adapter's conformance suite runs on `preprod_probe` as
   before.
4. **How 0143 reads.**
   - Its "The adapter refuses no place under `production_probe`" and "The codec is the same in
     both" now read with this refusal as the one difference. What FBC-jxau lands under
     `preprod_probe` it lands under `production_probe` too, with places still refused there.
   - Its Consequences' leaving this question to FBC-9sk2's record now reads as settled here.
     FBC-9sk2's record decides only whether to lift the refusal (item 5).
5. **What lifts it.** Only a record that admits places under `production_probe` lifts the
   refusal, in the same change: FBC-9sk2's record for stages two and three on production.
   FBC-cnyh's admission configures no probe mode (0143 item 5), so this refusal neither bars it
   nor is lifted by it.

## Alternatives

- **Leave places to fbc-oms and 0067, as 0143 does:** not taken. That rests on a rule the
  consumer follows. The refusal adds a check in the adapter that holds whatever registry the
  consumer builds.
- **Caps that declare no place under `production_probe`:** rejected. Caps describe what the
  venue offers (0003). The refusal is this library's hold on production, not a fact about QFEX.
  It would also break 0143 item 2's caps alike in both modes, which every later production stage
  would have to undo.
- **No codec under `production_probe`:** rejected. Stage one needs the codec to arm.

## Consequences

- **The proof.**
  - `crates/venues/fbc-venue-qfex/tests/probe_mode.rs` shows a `production_probe` codec
    refusing every place, batch place and amend `NotSent(Unsupported)` with no effect and its
    counts unchanged, before and after authentication. It shows the same codec sending its
    cancels, queries, resync and arm as a `preprod_probe` codec does, and a `preprod_probe` codec
    encoding every command as before.
  - `crates/venues/fbc-venue-qfex/tests/conformance.rs` shows `caps_truthful` on a
    `production_probe` configuration breaching exactly two probes, its placement control and its
    placement with the declared post-only and IOC conflict, and passing every other.
- **The docs.** `AGENTS.md`'s Layout and `README.md`'s row for the adapter, and the codec's own
  docs, say the codec refuses places under `production_probe` and that `caps_truthful` passes on
  `preprod_probe`.

## What would show this was wrong

- A place, batch place or amend frame written by a `production_probe` codec.
- A cancel, query, resync or arm that a `production_probe` codec refuses and a `preprod_probe`
  codec sends.
