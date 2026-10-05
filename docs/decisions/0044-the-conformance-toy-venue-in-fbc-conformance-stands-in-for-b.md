# 0044 — The conformance toy venue in fbc-conformance stands in for BT-502's toy venue from BT-102

Status: accepted
Date: 2026-10-04

## Context

BT-502's acceptance says "the toy venue from BT-102 passes the suite". That toy is
`crates/fbc-core/tests/toy_venue.rs`, written to prove FBC-5's, FBC-ji6's and FBC-ahf's done
lines and shrunk to them on pull request #8: it places limit orders and nothing else, and its
caps declare only that. `fbc-conformance`, where the named suite lives (0025), cannot use it: a
file in another crate's `tests/` is no library, and a dev-dependency back on `fbc-core`'s tests
would build two `fbc-core` instances whose types do not unify. FBC-z6v, which first meant to grow
that toy into the conformance venue, was split into slices homed in
`crates/fbc-conformance/src/toy/` (FBC-7lx order entry, then FBC-7ce, FBC-sal, FBC-u1d, FBC-z2s
and FBC-ja3). The runtime's order-entry tests need a toy with every order command too.

## Decision

- **The capability-complete conformance toy in `crates/fbc-conformance/src/toy/` is the toy
  venue BT-502 means.** It declares order, fill and market-data capabilities and exercises each
  through the codec traits alone; the named suite runs against it, and `fbc-core`'s toy stays
  the proof of its own tickets' done lines and is not grown.
- **It is public and path-includable.** The module uses `fbc_core` only and refers to its own
  files through `super::`, never `crate::`, so another crate's tests (the runtime's order-entry
  tests) include it with `#[path = ".../fbc-conformance/src/toy/mod.rs"]` and share the one toy
  rather than copying it. `fbc-conformance` takes `fbc-core` (and `rust_decimal`, for the toy's
  specs) as normal dependencies, still no venue crate (`scripts/check-deps.sh`).
- **Its protocol is its own** (`kind|key=value` records, as `fbc-core`'s toy) and describes no
  real venue; its signer is a keyless hash over the wire view it is shown.

## Alternatives

- Grow `crates/fbc-core/tests/toy_venue.rs` into the conformance venue: rejected; the suite in
  `fbc-conformance` cannot reach it (two `fbc-core` instances), and that file would outgrow the
  done lines it proves.
- Move `fbc-core`'s toy into `fbc-conformance` and have `fbc-core`'s tests use it: rejected;
  `fbc-core` cannot dev-depend on a crate that depends on it without the same duplication, and
  its tests must not wait on the conformance venue's slices.
- A separate `fbc-toy` crate: not taken; the toy exists to be held to the suite, and a crate
  of its own would add a workspace member for one module.

## Consequences

- Two toys exist: `fbc-core`'s, minimal, for the core's own tests, and the conformance toy,
  growing slice by slice until FBC-z6v's audit maps every capability it declares to a test.
- The conformance toy's caps describe the whole venue from the first slice, since every caps
  field is mandatory (0003): a field a later slice exercises is declared before its test exists,
  and FBC-z6v's audit is what holds each declaration to a test.
- A crate including the toy by path compiles it as its own module, so it must keep to
  `fbc_core` and `super::` paths; a `crate::` path breaks the including crate's build.

## What would show this was wrong

A suite case that cannot run against the conformance toy without reaching into `fbc-core`'s
toy, or a second copy of the toy appearing in another crate's tests, would reopen this.
