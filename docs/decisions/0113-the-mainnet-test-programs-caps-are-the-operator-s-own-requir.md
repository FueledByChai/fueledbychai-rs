# 0113 — The mainnet test programs' caps are the operator's own, required and positive with no ceiling

Status: accepted
Date: 2026-10-19

## Context

`mainnet_smoke` (FBC-p0aa) and `testnet_quote --mainnet` (FBC-tyft) both refused a
`--resting-cap-usd` above $11 or an `--inventory-cap-usd` above $50. The limits were
`RESTING_CAP_MAX` and `INVENTORY_CAP_MAX` in `crates/fbc-examples/src/mainnet.rs`. Those values
date from when the owner was trading small test positions. They were never library policy:
fbc-oms enforces the inventory cap (0005's I6) and the per-side resting cap with values the
consumer supplies and no defaults. On 2026-10-18 the owner answered Codex r4306524743 on PR #158
with "Keep them in code". Asked on 2026-10-19 "The fbc-examples mainnet test programs hard-code
$11/$50 as ceilings. What should they do?", the owner answered "no ceiling" (FBC-thks). That
answer supersedes the 2026-10-18 one for the two cap ceilings only.

## Decision

1. **No cap ceiling.** In the mainnet test programs, `--resting-cap-usd` and
   `--inventory-cap-usd` are the operator's own values. Both are required, have no defaults and
   must be positive. Any positive value is taken. `mainnet::mainnet_caps` refuses only a cap
   that is not positive. The command line also refuses one as it parses. The run checks again,
   before anything connects.
2. **Printed before anything connects.** `mainnet_smoke` prints a `CAPS` line right after its
   `MAINNET` line. `testnet_quote --mainnet` prints both caps on its `SUPERVISED` line. Both
   lines come before the first request.
3. **A cap past a lot count is the largest lot count.** A cap converted to lots that is past
   what a lot count holds (or what a decimal holds after the division) becomes `i64::MAX` lots
   (`testnet::cap_lots`). It is never zero and never a panic. fbc-oms's checked arithmetic then
   bounds every order as it would at any other large cap. This does not apply to an order's or a
   quote's size: a size that does not fit a lot count is still refused.
4. **The other mainnet limits stay in code.** The 20 bps minimum offset, the 3600 s maximum
   `--duration` and the 20000 ms maximum `--interval-ms` are unchanged.

## Alternatives

- Keep $11/$50 as ceilings (the owner's 2026-10-18 answer). Rejected: the values were relics
  of small test positions, and they made the public code hold an operator's sizing.
- Move the ceilings to private configuration. Rejected: no ceiling is wanted at all. The caps
  are already private values the operator types.
- Refuse a cap that does not fit a lot count. Rejected: that would bring back a ceiling, only
  an arbitrary one.

## Consequences

An operator can run the mainnet test programs at any size, so the size of a mistake is bounded
only by the caps that operator types. Both programs print the caps before connecting so the
operator can check them. The programs still bound each order by the resting cap, which fbc-oms
checks on every order command.

## What would show this was wrong

A mainnet session that ran with caps far larger than intended (for example a mistyped extra
zero) and lost money because of it. That would argue for a confirmation step or a cap the owner
sets per account, recorded in a new record.
