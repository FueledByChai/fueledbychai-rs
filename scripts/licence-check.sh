#!/usr/bin/env bash
# The licence gate (decision 0017, design §3): fails when a crate in the workspace's dependency
# graph carries a licence outside the allowlist in deny.toml, naming the crate and its licence.
#
#   scripts/licence-check.sh              `cargo deny check licenses` on this workspace
#   scripts/licence-check.sh --self-test  prove the gate refuses GPL-3.0-only path crates,
#                                         a normal and a dev-only dependency
#                                         (fixtures/licence-gate), and names them
#
# cargo-deny is required wherever the check runs, as cargo-llvm-cov is: install the pinned
# version once with `cargo install cargo-deny --version 0.20.2 --locked`. CI installs the same
# version itself (.github/workflows/loop.yml), and the self-test fails when the two disagree.
# Moving the version is a ticket, like the toolchain.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
PINNED="0.20.2"
FIXTURE="fixtures/licence-gate"
WORKFLOW=".github/workflows/loop.yml"

require_pinned() {
  local have
  have="$(cargo deny --version 2>/dev/null || true)"
  if [ "$have" != "cargo-deny $PINNED" ]; then
    echo "licence gate: needs cargo-deny $PINNED, found: ${have:-none}" >&2
    echo "  install it with: cargo install cargo-deny --version $PINNED --locked" >&2
    exit 1
  fi
}

gate() {  # the gate itself, on this workspace; Cargo.lock must not change
  cargo deny --config deny.toml --locked check licenses
}

self_test() {
  local out rc
  # CI must install the version this script requires, or the two machines judge differently.
  grep -q -- "cargo install cargo-deny --version $PINNED --locked" "$WORKFLOW" ||
    { echo "licence self-test: $WORKFLOW does not install cargo-deny $PINNED" >&2; exit 1; }
  # The fixture's graph is two path crates, so it needs no network and no registry.
  set +e
  out="$(cargo deny --manifest-path "$FIXTURE/Cargo.toml" --config deny.toml --frozen \
    --color never check licenses 2>&1)"
  rc=$?
  set -e
  if [ "$rc" = 0 ]; then
    echo "licence self-test: the gate passed a GPL-3.0-only dependency" >&2; echo "$out" >&2; exit 1
  fi
  # gpl-dev-dep is only a dev-dependency: test-only crates are held to the same allowlist.
  for want in "gpl-dep v0.0.0" "gpl-dev-dep v0.0.0" "GPL-3.0-only" \
    "license is not explicitly allowed"; do
    case "$out" in *"$want"*) ;; *)
      echo "licence self-test: the gate failed without naming '$want'" >&2; echo "$out" >&2; exit 1 ;;
    esac
  done
  # The same graph without those crates passes, so the failure above is their licence and not
  # a broken fixture or config.
  cargo deny --manifest-path "$FIXTURE/Cargo.toml" --config deny.toml --frozen \
    --exclude gpl-dep --exclude gpl-dev-dep check licenses >/dev/null 2>&1 ||
    { echo "licence self-test: the fixture fails even without its GPL crates" >&2; exit 1; }
  echo "licence self-test: ok (refused gpl-dep and dev-only gpl-dev-dep, GPL-3.0-only;" \
    "passed without them)"
}

case "${1:-}" in
  "") require_pinned; gate ;;
  --self-test) require_pinned; self_test ;;
  *) echo "usage: scripts/licence-check.sh [--self-test]" >&2; exit 2 ;;
esac
