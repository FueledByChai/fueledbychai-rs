#!/usr/bin/env bash
# The dependency-direction check (design §3, AGENTS.md Layout): fails (exit 1) naming each
# workspace edge that points the wrong way. Read from `cargo metadata --no-deps`; a crate is a
# venue crate when its manifest sits under crates/venues/, and fbc-venues is the registry.
#
#   1. A crate outside crates/venues/ (fbc-core, fbc-book, fbc-oms, fbc-journal, fbc-sim,
#      fbc-runtime, fbc-conformance and any later one) never depends on a venue crate,
#      the registry included.
#   2. A concrete venue crate (under crates/venues/, not fbc-venues) depends, among workspace
#      crates, on fbc-core only; its other dependencies are protocol crates.
#
# Together these leave fbc-venues the only crate that sees concrete venues. Normal and build
# dependencies are checked; dev-dependencies are not, since a venue crate's tests use
# fbc-runtime, fbc-journal, fbc-book and the conformance kit (design §6 step 12).
#
#   scripts/check-deps.sh                      check this workspace
#   scripts/check-deps.sh --manifest-path <p>  check another workspace (the self-test's)
#   scripts/check-deps.sh --self-test          prove the check fails on fixtures/dep-direction,
#                                              naming exactly its three forbidden edges, and
#                                              refuses a workspace with no venue crate
#                                              (fixtures/licence-gate)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
FIXTURE="fixtures/dep-direction"

check() {  # check [manifest-path]; prints each violation, exits 1 on any
  local manifest="${1:-Cargo.toml}" meta
  meta="$(cargo metadata --manifest-path "$manifest" --no-deps --format-version 1 --offline)"
  python3 - "$meta" <<'PY'
import json, os, sys

meta = json.loads(sys.argv[1])
root = meta["workspace_root"]
members = set(meta["workspace_members"])
pkgs = {p["name"]: p for p in meta["packages"] if p["id"] in members}

def is_venue(pkg):
    rel = os.path.relpath(pkg["manifest_path"], root)
    return rel.startswith("crates/venues" + os.sep)

venues = sorted(n for n, p in pkgs.items() if is_venue(p))
if not venues:
    # The rules key on the layout; a workspace with no crate under crates/venues/ would pass
    # them vacuously, so a moved layout fails here instead of passing for the wrong reason.
    print("dependency direction: no workspace crate under crates/venues/; the check cannot classify")
    sys.exit(1)

bad, edges = [], 0
for name in sorted(pkgs):
    pkg = pkgs[name]
    for dep in pkg["dependencies"]:
        if dep["kind"] == "dev" or dep["name"] not in pkgs:
            continue
        edges += 1
        target = pkgs[dep["name"]]
        kind = dep["kind"] or "normal"
        if not is_venue(pkg) and is_venue(target):
            bad.append(f"{name} -> {dep['name']} ({kind}): a crate outside crates/venues/ "
                       "never depends on a venue crate")
        elif is_venue(pkg) and name != "fbc-venues" and dep["name"] != "fbc-core":
            bad.append(f"{name} -> {dep['name']} ({kind}): a venue crate depends on fbc-core "
                       "and protocol crates only")

for line in bad:
    print("dependency direction: " + line)
if bad:
    sys.exit(1)
core = sorted(n for n, p in pkgs.items() if not is_venue(p))
print(f"dependency direction: ok ({edges} workspace edges; outside crates/venues/: "
      f"{', '.join(core)}; venue crates: {', '.join(venues)})")
PY
}

self_test() {
  local out rc
  set +e
  out="$(check "$FIXTURE/Cargo.toml" 2>&1)"
  rc=$?
  set -e
  if [ "$rc" != 1 ]; then
    echo "dependency self-test: expected exit 1 on $FIXTURE, got $rc" >&2; echo "$out" >&2; exit 1
  fi
  local want
  for want in "fbc-core -> fbc-venue-a (normal)" "fbc-book -> fbc-venues (build)" \
    "fbc-venue-b -> fbc-venue-a (normal)"; do
    case "$out" in *"$want"*) ;; *)
      echo "dependency self-test: the check did not name '$want'" >&2; echo "$out" >&2; exit 1 ;;
    esac
  done
  # Exactly those three: the dev-dependencies, fbc-venue-b -> fbc-core and the registry's
  # edges are allowed and must not be named.
  if [ "$(printf '%s\n' "$out" | grep -c '^dependency direction: ')" != 3 ]; then
    echo "dependency self-test: expected exactly three violations" >&2; echo "$out" >&2; exit 1
  fi
  # A workspace with no crate under crates/venues/ (the licence gate's fixture) is refused, not
  # passed vacuously.
  set +e
  out="$(check "fixtures/licence-gate/Cargo.toml" 2>&1)"
  rc=$?
  set -e
  case "$rc:$out" in 1:*"no workspace crate under crates/venues/"*) ;; *)
    echo "dependency self-test: a workspace without venue crates was not refused" >&2
    echo "$out" >&2; exit 1 ;;
  esac
  echo "dependency self-test: ok (refused fbc-core -> fbc-venue-a, fbc-book -> fbc-venues" \
    "(build), fbc-venue-b -> fbc-venue-a; allowed dev-dependencies and the registry;" \
    "refused a workspace without venue crates)"
}

case "${1:-}" in
  "") check ;;
  --manifest-path) [ -n "${2:-}" ] || { echo "usage: --manifest-path <Cargo.toml>" >&2; exit 2; }; check "$2" ;;
  --self-test) self_test ;;
  *) echo "usage: scripts/check-deps.sh [--self-test | --manifest-path <Cargo.toml>]" >&2; exit 2 ;;
esac
