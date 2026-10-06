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

# The external model reviewer (FBC-bk02): scripts/deepseek-review.py and its workflow, proven
# offline against local stubs of the chat completions endpoint and the GitHub comments API.
step "deepseek reviewer"
python3 scripts/deepseek-review-tests.py

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
  # Dependency direction (design §3): no crate outside crates/venues/ depends on a venue crate,
  # and a concrete venue crate depends on fbc-core (and protocol crates) only, so only
  # crates/venues/fbc-venues sees concrete venues; dev-dependencies are not checked. The
  # self-test proves the check fails naming each forbidden edge in fixtures/dep-direction.
  step "dependency direction"
  scripts/check-deps.sh --self-test
  scripts/check-deps.sh
  # TODO: exact-replay and signing goldens run as cargo tests; a refresh flag is needed only if
  # a golden must be regenerated. Waits on: the exec path (BT-402), since exact-replay goldens
  # compare outbound bytes; fbc-journal exists (0006), but nothing yet replays exec traffic.
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
