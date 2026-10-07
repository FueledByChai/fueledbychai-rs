# 0067 — A market seeded by hand is refused Start, Flatten and Wind-down unless the registry is built for a declared owner-assisted testnet run, augmenting 0055 and 0012

Status: accepted
Date: 2026-10-07

## Context

0055 seeds each market's position once per process, from the first trustworthy resync, and
FBC-c4v refuses Start, Flatten and Wind-down while the position is unknown, so nothing is
placed after a restart until that resync (the owner's rule; 0013 rule 1). `fbc-oms` also has
`Registry::seed_position`, which seeds a market by hand. A venue whose snapshot source is not
trustworthy (Paradex today) never has a market seeded by a resync, and the owner's answer C to
RB-olg-3 is that such a venue does not trade live until a new record flips its snapshot source
on testnet evidence; hand-seeding is for the owner-assisted testnet run only. 0055's
Consequences said so in prose (Reviewer B's RB80-10 on PR #80), but a hand seed made the
position known, and arming asked only that: a consumer could seed by hand and arm a live
market, around the rule (DeepSeek's DS-3 on PR #86; FBC-saw1). The testnet run still needs the
hand seed: the owner-run `testnet_trade` sample (FBC-x69b) trades Paradex's testnet, whose
snapshot source stays untrustworthy.

## Decision

Start, Flatten and Wind-down refuse a market whose position was seeded by hand, changing
nothing (`ArmRefusal::SeededByHand`), unless the registry was built for a declared
owner-assisted testnet run: `Registry::for_testnet_run(TestnetRun::owner_assisted())`. The
refusal is checked after the kill switch's and the unknown position's, before the leases'. A
market seeded by a trustworthy resync arms as before, declared or not, and the declaration
never arms a market whose position is unknown. A seed's origin is fixed for the process: a
later resync only compares the position (0055), so a hand-seeded market stays refused even
when a later trustworthy resync agrees with it.

## Alternatives

- Make `seed_position` available only under a test or testnet Cargo feature: a feature is
  unified across the workspace, so a consumer building the testnet sample beside a live
  binary would turn it on for both, and the tests that judge the caps would need it too.
  The refusal at arming holds in every build, and the declaration is one call a reviewer can
  find.
- Let a later trustworthy resync that agrees with the hand seed make the market armable: the
  seed would still not have placed the fills straddling a snapshot (0055 rule 3), and the
  owner's rule is a trustworthy resync seeding the market, not one agreeing with it.
- Take the declaration from the venue's configuration (a testnet URL, say): the OMS knows no
  venue's endpoints, and a mistaken URL would arm a live market silently; the consumer's
  explicit declaration is what the owner asked for.

## Consequences

- `TestnetRun` is made only by `TestnetRun::owner_assisted()`, given to
  `Registry::for_testnet_run` when the registry is built; `Registry::testnet_run` reports it.
  A consumer trading a live venue never calls it. FBC-x69b's `testnet_trade` sample calls it,
  then seeds each market by hand with `Registry::seed_position` and arms it as usual.
- A hand seed still makes the position known: the pre-trade caps and Exit judge against it
  as before; only arming refuses it. A market that is never armed builds no place or amend,
  so nothing but cancels is sent on it.
- Tests that arm a market seed it through a trustworthy resync (`tests/common/arm.rs`'s
  `seed`), or declare a testnet run when they need a hand seed's counting (`tests/caps.rs`).
- Proof: `crates/fbc-oms/tests/hand_seed.rs`.

## What would show this was wrong

A market seeded by hand armed in a registry not declared a testnet run, or a testnet run the
owner needs that cannot arm because its market can only be seeded by hand.
