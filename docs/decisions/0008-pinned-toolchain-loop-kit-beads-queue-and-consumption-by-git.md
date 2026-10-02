# 0008 — Pinned toolchain, loop kit, Beads queue, and consumption by git tag with no crates.io release yet

Status: accepted
Date: 2026-10-02

## Context

Coding agents build this repository through the ticket loop, and the private consumer builds
against it. Both need the same compiler, formatting and checks on every machine, and the
consumer needs a stable reference to a known version of the library without a registry.

## Decision

- **Toolchain.** Stable Rust 1.97 pinned in `rust-toolchain.toml` (with rustfmt and clippy),
  edition 2024, workspace resolver 3, rustfmt `max_width = 100`. CI installs the same version.
  Upgrading the toolchain is a ticket of its own.
- **Loop kit.** The coding-agent-loop kit at tag v0.22.0 (`kit` and `kit_ref` in `.loop.toml`);
  `scripts/check.sh` is the definition of done locally and in CI.
- **Queue.** Beads with the prefix `FBC`; stories in `docs/PRODUCT_BACKLOG.md`; decisions in
  `docs/decisions/`.
- **Consumption.** The private consumer depends on this repository as a git dependency pinned
  by tag (`vMAJOR.MINOR.PATCH`). The first tag, `v0.0.1`, is cut once the workspace skeleton and
  `fbc-core`'s first types have landed. Nothing is published to crates.io until the API
  settles, and no stability promise is made before then (0001).
- **Coverage.** `cargo llvm-cov` measures line coverage for the coverage ratchet once the
  workspace exists.

## Alternatives

- A floating stable toolchain: rejected. A new release can change lints or formatting and turn
  the check red without a code change.
- Publishing to crates.io now: rejected. The API will change many times before the first live
  order.
- A git dependency pinned by branch or a path dependency: rejected. A branch moves under the
  consumer; a path dependency only works on one machine.
- A git submodule: rejected. Cargo's git dependency with a tag does the same with less
  machinery.

## Consequences

- A library change the consumer needs is tagged before the consumer's ticket can use it.
- CI must install toolchain 1.97 explicitly, and the coverage tool where coverage is measured.
- The repository is public, so the consumer's CI needs no token to fetch it.

## What would show this was wrong

- A dependency the library needs requires a newer compiler than 1.97 (upgrade by ticket, not a
  reversal).
- The consumer needs more than one library tag a week for lockstep changes over a month; that
  calls for reconsidering how the two repositories move together.
- An outside user asks for a crates.io release.
