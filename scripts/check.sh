#!/usr/bin/env bash
# The definition of done for fueledbychai-rs, as a command: exits non-zero on the first
# failure. Written by grill-project from the kit's Rust skeleton; every stack step is skipped
# with a note until Cargo.toml exists, so this passes on an empty repository and starts
# failing as code arrives. AGENTS.md "The check" describes it; keep the two in step.
#
#   scripts/check.sh            everything
#   scripts/check.sh --fast     skip the coverage ratchet (the check to run while iterating)
#   scripts/check.sh --refresh-goldens
#                               first rewrite the exact-replay golden from a session recorded
#                               afresh (scripts/golden-refresh.sh refresh), then check as usual
#                               (FBC-qwq); it combines with --fast
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
FAST=0
REFRESH=0
for arg in "$@"; do
  case "$arg" in
    --fast) FAST=1 ;;
    --refresh-goldens) REFRESH=1 ;;
    *) echo "unknown flag: $arg" >&2; exit 2 ;;
  esac
done
# A golden test rewrites its golden only under this variable, which --refresh-goldens has
# scripts/golden-refresh.sh set for its refresh alone: never one the shell running the check
# exported.
unset FBC_REFRESH_GOLDENS
step() { printf '\n== %s\n' "$1"; }
started=$(date +%s)
. loop/templates/check/common.sh
loop_checks

# This repository is public (decision 0009): no secrets, real account addresses, or master key
# bytes outside fixtures marked SYNTHETIC. Runs on every tree, code or not.
step "privacy"
scripts/privacy-check.sh --self-test
scripts/privacy-check.sh

# Keep the dormant DeepSeek workflow's proof passing, and check the active Codex
# exact-head evidence parser against stale, running, and look-alike summaries (0127).
step "external model review evidence"
python3 scripts/deepseek-review-tests.py
python3 scripts/codex_review_evidence_tests.py

if [ -f Cargo.toml ]; then
  step "format"
  cargo fmt --all --check
  step "lint"
  cargo clippy --workspace --all-targets --quiet -- -D warnings
  # Exact replay's golden (decisions 0006 and 0142, FBC-qwq): a conformance-toy order-entry
  # session's journal, which crates/fbc-runtime/tests/exact_replay.rs replays to every outbound
  # byte it holds outside redaction spans, signatures included. --refresh-goldens rewrites every
  # file of it from the session that test records afresh; nothing else may change it, which the
  # "golden unchanged" step checks at the end of every run. The self-test proves the refresh fails
  # unless it rewrote every file, and the checksum sees any change, on a golden of its own.
  step "golden refresh self-test"
  scripts/golden-refresh.sh --self-test
  if [ "$REFRESH" = 1 ]; then
    step "golden refresh"
    scripts/golden-refresh.sh refresh
  fi
  golden_before="$(scripts/golden-refresh.sh sum)"
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
  # It also finds one `log` package in the graph, the one tungstenite logs through (0079).
  step "dependency direction"
  scripts/check-deps.sh --self-test
  scripts/check-deps.sh
else
  step "stack"; echo "skipped: no Cargo.toml yet"
fi

if [ "$FAST" = 0 ]; then
  # `coverage` in .loop.toml is scripts/coverage.sh (cargo-llvm-cov, workspace line coverage).
  # There is nothing to measure before the workspace exists; the ticket that creates it records
  # the first floor with `scripts/coverage-ratchet.sh --set`.
  if [ -f Cargo.toml ]; then ratchet; else step "coverage ratchet"; echo "skipped: no Cargo.toml yet"; fi
fi

if [ -n "${golden_before:-}" ]; then
  step "golden unchanged"
  scripts/golden-refresh.sh unchanged "$golden_before"
fi

printf '\nALL CHECKS PASSED (%ss)\n' "$(( $(date +%s) - started ))"
