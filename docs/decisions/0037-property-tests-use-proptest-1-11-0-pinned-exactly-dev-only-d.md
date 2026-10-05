# 0037 — Property tests use proptest 1.11.0, pinned exactly, dev-only, default features off, with a fixed seed per property

Status: accepted
Date: 2026-10-04

## Context

0005 requires its invariants I1–I9 to be property-tested, and states that the proptests are
part of the check once `fbc-oms` exists. FBC-iv2 creates `fbc-oms` and its first properties (I1
for order updates, I2), so the workspace needs a property-testing dependency for the first
time. Like every dependency it falls under 0017's licence gate, and the check has to give the
same answer, and the coverage ratchet (0008, 0.2 points of slack) the same figure, on every
machine and every run.

## Decision

- **The crate.** `proptest`, pinned exactly at `=1.11.0` in `[workspace.dependencies]` and
  taken only as a dev-dependency (`fbc-oms`'s first; later crates take it the same way), so it
  never reaches the consumer's build.
- **Features.** Default features off, `std` on: no `fork`, `timeout` or `tempfile`, so a
  property runs in the test process and writes no files. What `std` brings (`rand`,
  `rand_xorshift`, `regex-syntax`, `num-traits`, `unarray`) is MIT or Apache-2.0, inside 0017's
  allowlist; `cargo deny check licenses` checks it with the rest.
- **Determinism.** Each property names its case count and a fixed seed
  (`RngSeed::Fixed`) and persists no failures (`failure_persistence: None`): every run, local or
  CI, plays the same cases, so a red check is reproducible from the test alone and coverage
  does not move with the seed. A failure prints its shrunk minimal case; the fix adds that case
  as a unit test. Exploring with other seeds or more cases is a local edit, never committed.

## Alternatives

- `quickcheck`: rejected. Its shrinking works on values rather than on the strategies that
  built them, so a shrunk interleaving can stop being a valid history of one order (cumulative
  fills that fall, keys that go back); proptest shrinks within the strategy.
- Hand-written exhaustive enumeration: rejected. Interleavings with duplicates grow
  factorially, so only toy histories could be enumerated, and every later invariant (I3's
  fills, I6's caps) would need its own enumerator.
- A random seed per run (proptest's default) with regression files: rejected. Coverage and
  the result would depend on the run, which the ratchet's slack does not absorb and which
  makes a red CI run on Linux unreproducible on macOS without the file it wrote.

## Consequences

- `Cargo.lock` gains proptest and its `std` dependencies; moving the version is a ticket that
  says why, like the other exact pins.
- A property covers only the cases its seed generates, so its case count is chosen large
  enough for the space it explores (4096 for each of I1 and I2), and a new property states its
  own.
- `fbc-oms`'s later invariants (I3 with FBC-sq9, I6 with FBC-2e4) use the same dependency and
  the same fixed-seed configuration.

## What would show this was wrong

- A lattice bug found in live trading or by another test that the fixed seeds never reach,
  where a random seed would have: a new record would add seeded exploration runs outside the
  check.
- proptest's generation changing between patch versions so the same seed plays different
  cases: then the pin is not enough, and the cases need to be recorded instead.
