# 0001 — Two repositories: a public MIT library and a private consumer, split at the line between connectivity and quoting

Status: accepted
Date: 2026-10-02

## Context

The owner's sentence for this repository, from the first-day interview (2026-10-02):

> A Rust library that takes venue credentials and instrument names and gives a trading program
> one common interface for market data, order entry and account events on any supported venue
> (Paradex first, then Hibachi, with Binance futures as reference data). It replaces the Java
> FueledByChaiTrading library for new Rust work.

The approved architecture design (revision 2, kept with the private consumer and cited here as
"design §n") planned one private Cargo workspace and moved the venue-facing crates to a
permissive "fbc-rs" only when a second consumer appeared (design §3, §16, open question 8). The
owner overrode that before the interview: two repositories now. The Java FueledByChaiTrading
library is public under MIT; the market-making engine that will use this library is private.
Something has to say which code goes where, or the line blurs as code accretes.

## Decision

Two repositories under the FueledByChai GitHub organisation:

- **fueledbychai-rs** (this one) is public, MIT-licensed, and holds connectivity and order
  truth: the core types (ids, fees, instruments, capabilities), the runtime, the book, the order
  state machine and execution planner, the journal format with its writer and reader, the
  simulated venue and the queue-position fill-model code, the conformance kit, and the venue
  crates. The design's crates move here renamed: `cw-core` → `fbc-core`, `cw-runtime` →
  `fbc-runtime`, `cw-book` → `fbc-book`, `cw-oms` → `fbc-oms`, `cw-journal` → `fbc-journal`,
  `cw-sim` → `fbc-sim`, `cw-conformance` → `fbc-conformance`, and `venues/*` →
  `crates/venues/fbc-venues` (the registry), `fbc-venue-binance-usdm`, `fbc-venue-paradex`, and
  later `fbc-venue-hibachi`.
- **The private consumer** holds anything that decides what to quote (strategy, policies,
  gates), every parameter, the fill-model calibrations and their outputs, research, live
  configs, the UI and server, and the compatibility layer for the existing config and metrics
  databases.

The test for any new piece of code: if it decides what to quote, or carries a number tuned on
live trading, it is private; if it moves bytes, keeps order truth, or models a venue
mechanically, it belongs here. The private consumer depends on this repository by git tag
(0008); this repository never depends on, names, or assumes anything about the consumer's
crates.

## Alternatives

- One private repository, extracting the library later (design D1, §3): rejected by the owner.
  Extraction after the fact means splitting history and untangling imports that grew in the
  wrong direction in the meantime.
- One public repository for everything: rejected. Strategy, parameters and calibrations are the
  part that must stay private.
- A copyleft licence (as Tessera's AGPL): rejected. The library's consumer is closed source and
  the Java library it replaces is MIT.

## Consequences

- The design's `cw-oms` depended on `cw-strategy`; `fbc-oms` cannot. Its planner takes a desired
  book whose type is defined in this library, and nothing here imports strategy code.
- Public crates carry no parameters, spreads, sizes, calibration outputs or live configs; where
  a mechanism needs a number (a cap, a bracket, a rate limit floor), the consumer supplies it
  through configuration (0009).
- A change that spans both repositories lands here first, is tagged, and is then picked up by
  the consumer's tag bump (0008).
- The repository is public but has no outside users to support yet: no stability or semver
  promise is made until a later record makes one.

## What would show this was wrong

- A library ticket cannot be finished without importing a type, parameter, or behaviour that
  lives in the private consumer, more than once.
- Cross-repository lockstep changes (a library tag cut only to unblock one consumer ticket)
  become the norm rather than the exception, say more than half of the consumer's tag bumps in a
  month.
- An outside user appears who needs the library on its own: that calls for a record on
  stability and publishing, not a reversal of the split.
