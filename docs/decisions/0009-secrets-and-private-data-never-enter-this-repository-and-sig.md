# 0009 — Secrets and private data never enter this repository, and signing and auth are the only review paths

Status: accepted
Date: 2026-10-02

## Context

This repository is public and handles venue credentials at run time. A private key, JWT or API
secret committed here, written to a log, or carried in an error message is exposed to anyone.
Real account addresses, sub-account ids and balances identify the owner's trading. Live
configuration, parameters, spreads, sizes and calibration outputs are the private consumer's
edge (0001). Almost every pull request here auto-merges when its checks are green, so the rules
have to be checked by a script, and the code whose mistakes no test can see needs a person.

## Decision

- **Secrets never appear** in this repository, the Beads queue, logs, errors, or the journal:
  private keys, JWTs, API keys and secrets. The journal stores authorization headers and JWTs
  only as keyed hashes and keeps order signatures (0006). Errors name the failed action, never a
  secret value.
- **No real account data.** No real account addresses, sub-account ids or balances. Fixtures are
  scrubbed or synthetic; a fixture directory holding synthetic keys or addresses carries a
  `SYNTHETIC` file that says where the values come from and that no funded account uses them.
- **No live trading configuration.** No live configs, parameters, spreads, sizes or calibration
  outputs.
- **A check enforces it.** `scripts/privacy-check.sh`, run by `scripts/check.sh`, scans every
  file git would publish and fails on exactly these:
  - anywhere, synthetic fixtures included: a JWT (three dot-separated base64url parts starting
    `eyJ`) and a bearer token of 20 or more characters;
  - anywhere: the bytes of the local config master key, in hex or base64 or as a copied file,
    when that key file exists on the machine;
  - outside a directory marked `SYNTHETIC`: `0x` followed by 60 or more hex digits (a Starknet
    address, key or signature); a bare run of exactly 64 hex digits (a secp256k1 key written
    without `0x`; `Cargo.lock` checksums excepted); `0x` followed by exactly 40 hex digits (an
    EVM account address) unless it is listed in `scripts/privacy-allowlist.txt`, which holds
    public contract addresses only; and a PEM private-key header.
  A `SYNTHETIC` marker covers only the directory it sits in and below, and only under
  `fixtures/`. Synthetic signing vectors therefore sit in a directory of their own
  (`fixtures/paradex/signing/`), and recorded frames and journals beside it are scanned. The
  check cannot recognize every secret format; review of fixtures and the rules below still
  apply.
- **Review paths are signing and authentication only.** Changes to venue signers and to JWT and
  API-key handling (`crates/venues/*/src/sign*` and `crates/*/src/auth*`) wait for the
  owner's review instead of auto-merging. Everything else auto-merges when green.
- **Credential code lives on a review path.** All code that builds, signs, refreshes, stores or
  injects a private key, JWT or API key lives in a venue crate's `src/sign.rs` or `src/sign/`
  (the signer) or in a crate's `src/auth.rs` or `src/auth/` (JWT, API-key and credential
  handling); a venue's `exec.rs` only calls those modules. This departs from design §6 step 6,
  which puts auth in `exec.rs`. A ticket that needs credential code anywhere else, such as
  header injection in `fbc-runtime`, names that module `auth` so the existing `crates/*/src/auth`
  review path covers it, or adds its path to `review_paths` in the same change.
  `scripts/check.sh` fails when a venue crate's source outside `src/sign*` and `src/auth*`
  names a JWT, bearer token, API key, authorization header or private key.

## Alternatives

- Wider review paths as the design proposed (OMS, risk, journal reserve, goldens; design
  §12.2): rejected by the owner. Those areas are held by tests and invariants; signing and
  authentication are where a mistake is invisible to tests that use the same wrong vector, or
  leaks a credential.
- Relying on review alone for privacy: rejected, since most changes merge without a person.

## Consequences

- Golden signing vectors and any fixture with key-shaped hex live in a `SYNTHETIC` directory
  of their own, never in a directory that also holds recorded frames; tests read them from
  there.
- The first ticket that touches a review path is the Paradex signer.
- The check reports the file and line of a hit, never the matched value.
- Code outside the sign and auth modules refers to credentials only through those modules'
  functions, named without the words the placement check looks for.

## What would show this was wrong

- A secret, real address or live parameter is found on the default branch or in its history:
  rotate the credential, then tighten the check in a new record.
- A defect outside the review paths that a human review would plainly have caught causes a
  live loss: widen the review paths in a new record.
- The privacy check blocks legitimate work often enough that people start working around it.
