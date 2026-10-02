#!/usr/bin/env bash
# Line coverage of the whole workspace, as one percentage: the `coverage` command in .loop.toml
# that scripts/coverage-ratchet.sh compares with coverage-floor.txt (decision 0008).
# Needs cargo-llvm-cov: `rustup component add llvm-tools-preview` and
# `cargo install cargo-llvm-cov --version 0.9.1 --locked` (CI installs the same version itself).
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f Cargo.toml ] || { echo "coverage: no Cargo.toml yet; nothing to measure" >&2; exit 1; }
cargo llvm-cov --workspace --json --summary-only --quiet |
  python3 -c 'import json,sys; print("%.1f" % json.load(sys.stdin)["data"][0]["totals"]["lines"]["percent"])'
