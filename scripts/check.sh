#!/usr/bin/env bash
# The definition of done for fueledbychai-rs, as a command: exits non-zero on the first
# failure. Written by grill-project from the kit's Rust skeleton; every stack step is skipped
# with a note until Cargo.toml exists, so this passes on an empty repository and starts
# failing as code arrives. AGENTS.md "The check" describes it; keep the two in step.
#
#   scripts/check.sh            everything
#   scripts/check.sh --fast     skip the coverage ratchet (the check to run while iterating)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
FAST=0
for arg in "$@"; do case "$arg" in --fast) FAST=1 ;; *) echo "unknown flag: $arg" >&2; exit 2 ;; esac; done
step() { printf '\n== %s\n' "$1"; }
started=$(date +%s)
. loop/templates/check/common.sh
loop_checks

# This repository is public (decision 0009): no secrets, real account addresses, or master key
# bytes outside fixtures marked SYNTHETIC. Runs on every tree, code or not.
step "privacy"
scripts/privacy-check.sh --self-test
scripts/privacy-check.sh

if [ -f Cargo.toml ]; then
  step "format"
  cargo fmt --all --check
  step "lint"
  cargo clippy --workspace --all-targets --quiet -- -D warnings
  step "tests"
  cargo test --workspace --quiet
  step "build"
  cargo build --workspace --release --quiet
  # Credential code stays on a review path (0009): a venue crate's source that names a JWT, a
  # bearer token, an API key, an authorization header or a private key lives under src/sign*
  # or src/auth*, and other modules call those modules by names without these words.
  step "credential code placement"
  if [ -d crates/venues ]; then
    hits=$(grep -rliE 'jwt|bearer|api_?key|authorization|private_?key' crates/venues/*/src 2>/dev/null |
      grep -vE '/src/(sign|auth)' || true)
    [ -z "$hits" ] || { echo "credential code outside src/sign* or src/auth* (0009):"; echo "$hits"; exit 1; }
    echo "ok"
  else
    echo "skipped: no venue crate yet"
  fi
  # Licence gate (0017, design §3): every dependency carries a licence on deny.toml's
  # permissive allowlist. Needs the pinned cargo-deny on every machine, as coverage needs
  # cargo-llvm-cov; the self-test proves the gate refuses a GPL-3.0-only crate and names it.
  step "licence gate"
  scripts/licence-check.sh --self-test
  scripts/licence-check.sh
  # TODO: dependency rules (design §3): fbc-core, fbc-book, fbc-oms, fbc-journal and fbc-sim
  # never depend on a venue crate; only crates/venues/fbc-venues sees concrete venues. Waits on:
  # the first venue crate (scripts/check-deps.sh reading `cargo metadata` lands with it).
  # TODO: exact-replay and signing goldens run as cargo tests; a refresh flag is needed only if
  # a golden must be regenerated. Waits on: where the golden journals live and who approves a
  # refresh (decide when fbc-journal lands, 0006).
else
  step "stack"; echo "skipped: no Cargo.toml yet"
fi

if [ "$FAST" = 0 ]; then
  # `coverage` in .loop.toml is scripts/coverage.sh (cargo-llvm-cov, workspace line coverage).
  # There is nothing to measure before the workspace exists; the ticket that creates it records
  # the first floor with `scripts/coverage-ratchet.sh --set`.
  if [ -f Cargo.toml ]; then ratchet; else step "coverage ratchet"; echo "skipped: no Cargo.toml yet"; fi
fi

printf '\nALL CHECKS PASSED (%ss)\n' "$(( $(date +%s) - started ))"
