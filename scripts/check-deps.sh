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
#   3. One `log` (decision 0079): the resolved graph, every target and dependency kind, holds
#      exactly one package named `log`, and tungstenite's `log` dependency resolves to the
#      package fbc-runtime depends on. fbc-runtime caps that facade's static level at DEBUG
#      because tungstenite logs each frame at TRACE; the cap holds only on the facade
#      tungstenite logs through. Cargo never selects two semver-compatible versions of one
#      package (a consumer pinning another 0.4 release fails to resolve), so a second `log`
#      would be another major (0.3) or another source; the check refuses either.
#
#   scripts/check-deps.sh                      check this workspace
#   scripts/check-deps.sh --manifest-path <p>  check another workspace (the self-test's)
#   scripts/check-deps.sh --self-test          prove the check fails on fixtures/dep-direction,
#                                              naming exactly its three forbidden edges, and
#                                              refuses a workspace with no venue crate
#                                              (fixtures/licence-gate), and that the one-log
#                                              check refuses a graph with two `log` packages
#                                              and one where tungstenite's `log` is not
#                                              fbc-runtime's (synthetic metadata)
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

one_log() {  # one_log [metadata-json]; reads `cargo metadata` of this workspace when none is given
  # The whole graph's metadata is too long for an argument; python reads it from a file.
  local file rc=0
  file="$(mktemp)"
  if [ -n "${1:-}" ]; then printf '%s' "$1" >"$file"
  else cargo metadata --format-version 1 --offline --locked >"$file"; fi
  python3 - "$file" <<'PY' || rc=$?
import json, sys

with open(sys.argv[1]) as f:
    meta = json.load(f)
names = {p["id"]: (p["name"], p["version"]) for p in meta["packages"]}
logs = sorted(i for i, (n, _) in names.items() if n == "log")
bad = []
if len(logs) != 1:
    found = ", ".join(f"log {names[i][1]} ({i})" for i in logs) or "none"
    bad.append(f"the graph holds {len(logs)} log packages, not one: {found}")

def log_of(name):  # the `log` package each package called `name` resolves its `log` edge to
    out = set()
    for node in meta["resolve"]["nodes"]:
        if names[node["id"]][0] != name:
            continue
        for dep in node["deps"]:
            if names[dep["pkg"]][0] == "log":
                out.add(dep["pkg"])
    return out

runtime, tungstenite = log_of("fbc-runtime"), log_of("tungstenite")
if not runtime or not tungstenite:
    bad.append("fbc-runtime or tungstenite has no log dependency; the check cannot compare them")
elif runtime != tungstenite:
    bad.append(f"tungstenite logs through {sorted(tungstenite)}, fbc-runtime caps {sorted(runtime)}")
for line in bad:
    print("one log: " + line)
if bad:
    sys.exit(1)
print(f"one log: ok (log {names[logs[0]][1]}, the facade tungstenite logs through and "
      "fbc-runtime caps)")
PY
  rm -f "$file"
  return "$rc"
}

# Synthetic `cargo metadata` for the one-log self-test: fbc-runtime and tungstenite each with
# a `log` edge, to the package ids given.
log_meta() {  # log_meta <fbc-runtime's log id> <tungstenite's log id>
  python3 - "$1" "$2" <<'PY'
import json, sys

rt, tg = sys.argv[1], sys.argv[2]
pkgs = [("fbc-runtime", "0.0.1", "rt"), ("tungstenite", "0.30.0", "tg")]
pkgs += [("log", i.split("@")[1], i) for i in sorted({rt, tg})]
nodes = [{"id": "rt", "deps": [{"pkg": rt}]}, {"id": "tg", "deps": [{"pkg": tg}]}]
nodes += [{"id": i, "deps": []} for i in sorted({rt, tg})]
print(json.dumps({"packages": [{"id": i, "name": n, "version": v} for n, v, i in pkgs],
                  "resolve": {"nodes": nodes}}))
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
  # One log: a graph where tungstenite's log is another package than fbc-runtime's is refused,
  # naming both, and one where they are the same package passes.
  set +e
  out="$(one_log "$(log_meta log@0.4.34 log@0.3.9)" 2>&1)"
  rc=$?
  set -e
  case "$rc:$out" in
    1:*"holds 2 log packages"*"tungstenite logs through ['log@0.3.9'], fbc-runtime caps ['log@0.4.34']"*) ;;
    *) echo "dependency self-test: two log packages were not refused" >&2; echo "$out" >&2; exit 1 ;;
  esac
  out="$(one_log "$(log_meta log@0.4.34 log@0.4.34)")" || {
    echo "dependency self-test: one shared log package was refused" >&2; echo "$out" >&2; exit 1; }
  echo "dependency self-test: ok (refused fbc-core -> fbc-venue-a, fbc-book -> fbc-venues" \
    "(build), fbc-venue-b -> fbc-venue-a; allowed dev-dependencies and the registry;" \
    "refused a workspace without venue crates; refused a second log package under" \
    "tungstenite, passed one shared)"
}

case "${1:-}" in
  "") check; one_log ;;
  --manifest-path) [ -n "${2:-}" ] || { echo "usage: --manifest-path <Cargo.toml>" >&2; exit 2; }; check "$2" ;;
  --self-test) self_test ;;
  *) echo "usage: scripts/check-deps.sh [--self-test | --manifest-path <Cargo.toml>]" >&2; exit 2 ;;
esac
