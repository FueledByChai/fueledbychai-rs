# 0017 — A licence gate: cargo-deny pinned at 0.20.2 checks every dependency against a permissive allowlist on every machine that runs the check

Status: accepted
Date: 2026-10-03

## Context

Design §3 says no AGPL or GPL dependency, and `scripts/check.sh` carried that rule only as a
TODO, waiting on whether cargo-deny would run on every machine or only in CI (FBC-cu0). Sprint 2
brings the first large dependency trees (tokio, a WebSocket and an HTTP client, rustls with
ring, zstd, an HMAC), and they are not MIT/Apache-only: ring is Apache-2.0 AND ISC,
rustls-webpki is ISC, webpki-roots is CDLA-Permissive-2.0, the Unicode tables are Unicode-3.0,
and zstd's bundled C is BSD-3-Clause. The gate has to land before them so each lands under it.
The owner accepted the permissive allowlist and the on-every-machine rule on 2026-10-03.

## Decision

- **Allowlist.** `deny.toml` at the workspace root allows exactly MIT, Apache-2.0, Apache-2.0
  WITH LLVM-exception, ISC, BSD-2-Clause, BSD-3-Clause, Zlib, Unicode-3.0 and
  CDLA-Permissive-2.0, over the whole graph (all features, dev-dependencies, every platform).
  Anything else fails, the GPL and AGPL families included. There are no per-crate exceptions
  or clarifications; one is added only by a ticket that says why, with its id beside it.
- **Tool and version.** cargo-deny 0.20.2, installed once by hand with
  `cargo install cargo-deny --version 0.20.2 --locked`; CI installs the same version itself.
  Moving the version is a ticket, like the toolchain (0008).
- **Where it runs.** In the full and the fast check on every machine, as
  `scripts/licence-check.sh` inside `scripts/check.sh`: it fails when cargo-deny is missing or
  another version, and its self-test proves the gate refuses GPL-3.0-only path crates in `fixtures/licence-gate`,
  one a normal and one a dev-only dependency, and names them, and that CI pins the same
  version. Dev-dependencies are checked (`include-dev`, off by default in cargo-deny).
- **Scope.** Only `cargo deny check licenses`. Advisories, bans and sources are not checked by
  this record.

## Alternatives

- MIT and Apache-2.0 only: rejected. It rules out rustls with ring or aws-lc-rs and
  webpki-roots, and so forces another TLS stack for no licensing gain.
- The gate in CI only: rejected. A local green check would then not mean a green CI, which is
  the reason the check is one script.
- An unpinned cargo-deny: rejected. A new release can change licence parsing or defaults and
  turn the check red with no code change.
- `cargo-about` or a hand-written `cargo metadata` scan: rejected. cargo-deny is the common tool
  for this, reads SPDX expressions properly, and can take on advisories and bans later.

## Consequences

- Every machine that runs the check needs cargo-deny 0.20.2 installed, as it needs
  cargo-llvm-cov 0.9.1.
- A dependency with a licence outside the list, or none cargo-deny can identify, cannot land
  until a ticket changes the list (a new record) or adds a reasoned exception.
- The allowlist is policy, not an inventory: entries not yet used (ISC, Zlib,
  CDLA-Permissive-2.0 today) are not reported.

## What would show this was wrong

- A dependency the library needs carries a licence outside the list that the owner judges
  acceptable, more than once: the list is too narrow and a new record widens it.
- cargo-deny misreads a licence expression so the gate passes a copyleft crate or fails a
  permissive one, and only a version move fixes it.
- The pinned version stops installing on the pinned toolchain.
