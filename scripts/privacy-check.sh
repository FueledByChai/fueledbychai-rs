#!/usr/bin/env bash
# The privacy check for this public repository (decision 0009): fails when a file in the
# checkout holds something that must never be published, and names the file and line only,
# never the value it found.
#
#   scripts/privacy-check.sh              scan the checkout (tracked and untracked, not ignored)
#   scripts/privacy-check.sh --self-test  prove every verdict against a fixture it builds
#
# It fails on:
#   - a hex value of 0x followed by 60 or more hex digits (the shape of a Starknet account
#     address, private key or signature), outside a fixtures directory marked synthetic;
#   - a bare run of exactly 64 hex digits (the shape of a secp256k1 private key written
#     without 0x, as Hibachi, GRVT and Hyperliquid keys are), outside a fixtures directory
#     marked synthetic; Cargo.lock files are skipped (their crate checksums have this shape);
#   - 0x followed by exactly 40 hex digits (the shape of an EVM account address, as Hibachi
#     and GRVT accounts are), outside a fixtures directory marked synthetic, unless the value
#     is listed in scripts/privacy-allowlist.txt (public contract addresses only, one per
#     line, each with a comment line above it saying whose contract it is);
#   - a JWT (three dot-separated base64url parts starting eyJ) or a bearer token of 20 or
#     more characters, anywhere, synthetic fixtures included;
#   - a PEM private-key header, outside a fixtures directory marked synthetic;
#   - the bytes of a local master key file (hex in either case, or base64), anywhere, and any
#     32-byte file identical to it. The key files are FBC_PRIVACY_KEY_FILES (colon-separated),
#     by default the config master key the private consumer and the Java app use. A key file
#     that does not exist (CI) or cannot be read is skipped with a note.
#
# A directory is marked synthetic by a file named SYNTHETIC in it, saying where the values come
# from and that no funded account uses them. Only a marker whose path contains a fixtures/
# directory counts, and it covers everything below its directory.
set -euo pipefail
ROOT="${LOOP_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
KEY_FILES="${FBC_PRIVACY_KEY_FILES-$HOME/.chaiwala/master.key}"
ALLOWLIST="scripts/privacy-allowlist.txt"

files() {  # every file the repository could publish, NUL-separated, relative to the root
  if git -C "$ROOT" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    git -C "$ROOT" ls-files -z --cached --others --exclude-standard
  else
    local p
    (cd "$ROOT" && find . -type f -not -path './.git/*' -print0) |
      while IFS= read -r -d '' p; do printf '%s\0' "${p#./}"; done
  fi
}

synthetic_dirs() {  # directories marked synthetic, one per line, with a trailing slash
  local f
  while IFS= read -r -d '' f; do
    case "$f" in
      SYNTHETIC|*/SYNTHETIC) case "/$f" in */fixtures/*) echo "$(dirname "$f")/" ;; esac ;;
    esac
  done < <(files)
}

scan() {
  local fail=0 f d line hits synth keyfile patterns size
  local hex_re='0x[0-9a-fA-F]{60,}'
  local pem_re='-----BEGIN ([A-Z0-9]+ )*PRIVATE KEY-----'
  local bare_re='(^|[^0-9A-Za-z_])[0-9a-fA-F]{64}([^0-9A-Za-z_]|$)'
  local addr_re='0x[0-9a-fA-F]{40}([^0-9A-Za-z_]|$)'
  local jwt_re='eyJ[A-Za-z0-9_-]{8,}[.][A-Za-z0-9_-]{8,}[.][A-Za-z0-9_-]+'
  local bearer_re='[Bb]earer [A-Za-z0-9._~+/-]{20,}'
  local allowed=""
  if [ -f "$ROOT/$ALLOWLIST" ]; then
    allowed="$(grep -E '^0x[0-9a-fA-F]{40}$' "$ROOT/$ALLOWLIST" | tr 'A-F' 'a-f' || true)"
  fi
  synth="$(synthetic_dirs)"
  patterns="$(mktemp)"; chmod 600 "$patterns"
  # shellcheck disable=SC2064
  trap "rm -f '$patterns'" EXIT
  local IFS_OLD="$IFS"; IFS=':'
  local keys=()
  for keyfile in $KEY_FILES; do [ -n "$keyfile" ] && keys+=("$keyfile"); done
  IFS="$IFS_OLD"
  local key_count=0
  for keyfile in ${keys[@]+"${keys[@]}"}; do
    if [ ! -e "$keyfile" ]; then echo "privacy: key file absent, skipped: $keyfile"; continue; fi
    if ! [ -r "$keyfile" ] || ! head -c 1 "$keyfile" >/dev/null 2>&1; then
      echo "privacy: key file unreadable, skipped: $keyfile"; continue
    fi
    od -An -tx1 -v "$keyfile" | tr -d ' \n' >> "$patterns"; echo >> "$patterns"
    od -An -tx1 -v "$keyfile" | tr -d ' \n' | tr 'a-f' 'A-F' >> "$patterns"; echo >> "$patterns"
    base64 < "$keyfile" | tr -d '\n' >> "$patterns"; echo >> "$patterns"
    key_count=$((key_count + 1))
  done
  while IFS= read -r -d '' f; do
    [ -f "$ROOT/$f" ] || continue
    if [ "$key_count" -gt 0 ]; then
      size="$(wc -c < "$ROOT/$f" | tr -d ' ')"
      if [ "$size" = 32 ]; then
        for keyfile in "${keys[@]}"; do
          if [ -r "$keyfile" ] && cmp -s "$keyfile" "$ROOT/$f"; then
            echo "privacy: $f is a copy of a master key file"; fail=1
          fi
        done
      fi
      if grep -IqF -f "$patterns" "$ROOT/$f" 2>/dev/null; then
        while IFS= read -r line; do
          echo "privacy: $f:$line holds master key bytes"; fail=1
        done < <(grep -InF -f "$patterns" "$ROOT/$f" | cut -d: -f1)
      fi
    fi
    hits="$(grep -InE -- "$jwt_re" "$ROOT/$f" 2>/dev/null | cut -d: -f1 || true)"
    for line in $hits; do
      echo "privacy: $f:$line has a JWT"; fail=1
    done
    hits="$(grep -InE -- "$bearer_re" "$ROOT/$f" 2>/dev/null | cut -d: -f1 || true)"
    for line in $hits; do
      echo "privacy: $f:$line has a bearer token"; fail=1
    done
    in_synth=0
    while IFS= read -r d; do
      [ -n "$d" ] || continue
      case "$f" in "$d"*) in_synth=1; break ;; esac
    done <<< "$synth"
    [ "$in_synth" = 1 ] && continue
    hits="$(grep -InE -- "$hex_re" "$ROOT/$f" 2>/dev/null | cut -d: -f1 || true)"
    for line in $hits; do
      echo "privacy: $f:$line has a 0x value of 60+ hex digits outside a SYNTHETIC fixtures directory"; fail=1
    done
    hits="$(grep -InE -- "$pem_re" "$ROOT/$f" 2>/dev/null | cut -d: -f1 || true)"
    for line in $hits; do
      echo "privacy: $f:$line has a PEM private-key header outside a SYNTHETIC fixtures directory"; fail=1
    done
    case "$f" in
      Cargo.lock|*/Cargo.lock) ;;
      *)
        hits="$(grep -InE -- "$bare_re" "$ROOT/$f" 2>/dev/null | cut -d: -f1 || true)"
        for line in $hits; do
          echo "privacy: $f:$line has a bare 64-hex value outside a SYNTHETIC fixtures directory"; fail=1
        done ;;
    esac
    hits="$(grep -InoE -- "$addr_re" "$ROOT/$f" 2>/dev/null | while IFS=: read -r line m; do
      m="$(printf '%s' "${m:0:42}" | tr 'A-F' 'a-f')"
      printf '%s\n' "$allowed" | grep -qxF -- "$m" || echo "$line"
    done | sort -un || true)"
    for line in $hits; do
      echo "privacy: $f:$line has a 0x value of 40 hex digits (an address) outside a SYNTHETIC fixtures directory and not in $ALLOWLIST"; fail=1
    done
  done < <(files)
  if [ "$fail" = 0 ]; then
    echo "privacy: clean ($(files | tr -cd '\0' | wc -c | tr -d ' ') files; synthetic: $(echo "$synth" | grep -c . || true) dir(s); master keys checked: $key_count)"
  fi
  return "$fail"
}

self_test() {
  local dir out rc hex
  dir="$(mktemp -d)"
  # shellcheck disable=SC2064
  trap "rm -rf '$dir'" EXIT
  hex="0x$(printf 'ab%.0s' $(seq 1 31))"   # 62 hex digits, built so this file never holds one
  mkdir -p "$dir/repo/src" "$dir/repo/fixtures/venue" "$dir/repo/fixtures/real"
  git -C "$dir/repo" init -q
  echo "fn main() {}" > "$dir/repo/src/lib.rs"
  # 1. A clean tree passes.
  out="$(LOOP_ROOT="$dir/repo" FBC_PRIVACY_KEY_FILES="" "$0")" || { echo "self-test: clean tree should pass: $out"; exit 1; }
  # 2. A long hex value in code fails, naming the file and line but not the value.
  printf 'let a = 1;\nlet k = "%s";\n' "$hex" > "$dir/repo/src/key.rs"
  rc=0; out="$(LOOP_ROOT="$dir/repo" FBC_PRIVACY_KEY_FILES="" "$0")" || rc=$?
  [ "$rc" = 1 ] && echo "$out" | grep -q 'src/key.rs:2 ' || { echo "self-test: hex in code should fail at src/key.rs:2 (rc $rc): $out"; exit 1; }
  echo "$out" | grep -q "$hex" && { echo "self-test: the value must never be printed"; exit 1; }
  rm "$dir/repo/src/key.rs"
  # 3. The same value in a fixtures directory marked SYNTHETIC passes; unmarked fails.
  printf '%s\n' "$hex" > "$dir/repo/fixtures/venue/vector.txt"
  echo "synthetic test vector" > "$dir/repo/fixtures/venue/SYNTHETIC"
  out="$(LOOP_ROOT="$dir/repo" FBC_PRIVACY_KEY_FILES="" "$0")" || { echo "self-test: synthetic fixture should pass: $out"; exit 1; }
  printf '%s\n' "$hex" > "$dir/repo/fixtures/real/vector.txt"
  rc=0; out="$(LOOP_ROOT="$dir/repo" FBC_PRIVACY_KEY_FILES="" "$0")" || rc=$?
  [ "$rc" = 1 ] && echo "$out" | grep -q 'fixtures/real/vector.txt:1 ' || { echo "self-test: unmarked fixture should fail (rc $rc): $out"; exit 1; }
  rm "$dir/repo/fixtures/real/vector.txt"
  # 4. A SYNTHETIC marker outside fixtures/ does not exempt anything.
  echo "not a fixture" > "$dir/repo/src/SYNTHETIC"
  printf '%s\n' "$hex" > "$dir/repo/src/v.txt"
  rc=0; out="$(LOOP_ROOT="$dir/repo" FBC_PRIVACY_KEY_FILES="" "$0")" || rc=$?
  [ "$rc" = 1 ] || { echo "self-test: a marker outside fixtures/ must not exempt (rc $rc): $out"; exit 1; }
  rm "$dir/repo/src/SYNTHETIC" "$dir/repo/src/v.txt"
  # 5. A PEM private-key header fails.
  printf -- '-----%s PRIVATE KEY-----\n' "BEGIN EC" > "$dir/repo/src/k.pem.txt"
  rc=0; out="$(LOOP_ROOT="$dir/repo" FBC_PRIVACY_KEY_FILES="" "$0")" || rc=$?
  [ "$rc" = 1 ] && echo "$out" | grep -q 'src/k.pem.txt:1 has a PEM' || { echo "self-test: PEM header should fail (rc $rc): $out"; exit 1; }
  rm "$dir/repo/src/k.pem.txt"
  # 6. Master key bytes fail in base64, in hex, and as a copied file; an absent key file is skipped.
  head -c 32 /dev/urandom > "$dir/master.key"
  printf 'value = "%s"\n' "$(base64 < "$dir/master.key" | tr -d '\n')" > "$dir/repo/src/b64.txt"
  rc=0; out="$(LOOP_ROOT="$dir/repo" FBC_PRIVACY_KEY_FILES="$dir/master.key" "$0")" || rc=$?
  [ "$rc" = 1 ] && echo "$out" | grep -q 'src/b64.txt:1 holds master key bytes' || { echo "self-test: base64 key should fail (rc $rc): $out"; exit 1; }
  rm "$dir/repo/src/b64.txt"
  od -An -tx1 -v "$dir/master.key" | tr -d ' \n' | tr 'a-f' 'A-F' > "$dir/repo/src/hex.txt"
  rc=0; out="$(LOOP_ROOT="$dir/repo" FBC_PRIVACY_KEY_FILES="$dir/master.key" "$0")" || rc=$?
  [ "$rc" = 1 ] && echo "$out" | grep -q 'src/hex.txt:1 holds master key bytes' || { echo "self-test: hex key should fail (rc $rc): $out"; exit 1; }
  rm "$dir/repo/src/hex.txt"
  cp "$dir/master.key" "$dir/repo/src/copy.bin"
  rc=0; out="$(LOOP_ROOT="$dir/repo" FBC_PRIVACY_KEY_FILES="$dir/master.key" "$0")" || rc=$?
  [ "$rc" = 1 ] && echo "$out" | grep -q 'src/copy.bin is a copy' || { echo "self-test: copied key should fail (rc $rc): $out"; exit 1; }
  rm "$dir/repo/src/copy.bin"
  out="$(LOOP_ROOT="$dir/repo" FBC_PRIVACY_KEY_FILES="$dir/absent.key" "$0")" && echo "$out" | grep -q 'key file absent, skipped' || { echo "self-test: absent key file should be skipped: $out"; exit 1; }
  # 7. A marker covers only its own directory: a sibling or the parent of a marked directory
  #    is still scanned (recorded frames beside synthetic signing vectors).
  mkdir -p "$dir/repo/fixtures/v2/signing" "$dir/repo/fixtures/v2/frames"
  echo "synthetic signing vectors" > "$dir/repo/fixtures/v2/signing/SYNTHETIC"
  printf '%s\n' "$hex" > "$dir/repo/fixtures/v2/signing/vector.txt"
  out="$(LOOP_ROOT="$dir/repo" FBC_PRIVACY_KEY_FILES="" "$0")" || { echo "self-test: marked signing dir should pass: $out"; exit 1; }
  printf '%s\n' "$hex" > "$dir/repo/fixtures/v2/frames/frame.txt"
  rc=0; out="$(LOOP_ROOT="$dir/repo" FBC_PRIVACY_KEY_FILES="" "$0")" || rc=$?
  [ "$rc" = 1 ] && echo "$out" | grep -q 'fixtures/v2/frames/frame.txt:1 ' || { echo "self-test: a sibling of a marked dir must be scanned (rc $rc): $out"; exit 1; }
  rm "$dir/repo/fixtures/v2/frames/frame.txt"
  printf '%s\n' "$hex" > "$dir/repo/fixtures/v2/frame.txt"
  rc=0; out="$(LOOP_ROOT="$dir/repo" FBC_PRIVACY_KEY_FILES="" "$0")" || rc=$?
  [ "$rc" = 1 ] && echo "$out" | grep -q 'fixtures/v2/frame.txt:1 ' || { echo "self-test: the parent of a marked dir must be scanned (rc $rc): $out"; exit 1; }
  rm "$dir/repo/fixtures/v2/frame.txt"
  # 8. A JWT or a bearer token fails anywhere, a SYNTHETIC directory included, and is never printed.
  local part jwt bearer bare addr
  part="$(printf 'Qx%.0s' $(seq 1 8))"
  jwt="eyJ${part}.eyJ${part}.${part}"
  printf 'auth = "%s"\n' "$jwt" > "$dir/repo/fixtures/venue/auth.txt"
  rc=0; out="$(LOOP_ROOT="$dir/repo" FBC_PRIVACY_KEY_FILES="" "$0")" || rc=$?
  [ "$rc" = 1 ] && echo "$out" | grep -q 'fixtures/venue/auth.txt:1 has a JWT' || { echo "self-test: a JWT in a synthetic dir should fail (rc $rc): $out"; exit 1; }
  echo "$out" | grep -qF "$jwt" && { echo "self-test: a JWT must never be printed"; exit 1; }
  rm "$dir/repo/fixtures/venue/auth.txt"
  bearer="Bearer ${part}${part}"
  printf 'x\nAuthorization: %s\n' "$bearer" > "$dir/repo/src/hdr.txt"
  rc=0; out="$(LOOP_ROOT="$dir/repo" FBC_PRIVACY_KEY_FILES="" "$0")" || rc=$?
  [ "$rc" = 1 ] && echo "$out" | grep -q 'src/hdr.txt:2 has a bearer token' || { echo "self-test: a bearer token should fail (rc $rc): $out"; exit 1; }
  rm "$dir/repo/src/hdr.txt"
  # 9. A bare 64-hex value fails in code, passes in a SYNTHETIC directory and in Cargo.lock.
  bare="$(printf 'cd%.0s' $(seq 1 32))"
  printf 'let k = "%s";\n' "$bare" > "$dir/repo/src/k.rs"
  rc=0; out="$(LOOP_ROOT="$dir/repo" FBC_PRIVACY_KEY_FILES="" "$0")" || rc=$?
  [ "$rc" = 1 ] && echo "$out" | grep -q 'src/k.rs:1 has a bare 64-hex' || { echo "self-test: a bare 64-hex value should fail (rc $rc): $out"; exit 1; }
  mv "$dir/repo/src/k.rs" "$dir/repo/fixtures/venue/k.rs"
  printf 'checksum = "%s"\n' "$bare" > "$dir/repo/Cargo.lock"
  out="$(LOOP_ROOT="$dir/repo" FBC_PRIVACY_KEY_FILES="" "$0")" || { echo "self-test: bare hex in a synthetic dir or Cargo.lock should pass: $out"; exit 1; }
  rm "$dir/repo/fixtures/venue/k.rs" "$dir/repo/Cargo.lock"
  # 10. An EVM-shaped address fails unless it is in the allow-list.
  addr="0x$(printf 'ef%.0s' $(seq 1 20))"
  printf 'account = "%s"\n' "$addr" > "$dir/repo/src/acct.txt"
  rc=0; out="$(LOOP_ROOT="$dir/repo" FBC_PRIVACY_KEY_FILES="" "$0")" || rc=$?
  [ "$rc" = 1 ] && echo "$out" | grep -q 'src/acct.txt:1 has a 0x value of 40' || { echo "self-test: an address should fail (rc $rc): $out"; exit 1; }
  echo "$out" | grep -qF "$addr" && { echo "self-test: an address must never be printed"; exit 1; }
  mkdir -p "$dir/repo/scripts"
  printf '# a public test contract\n%s\n' "$(echo "$addr" | tr 'a-f' 'A-F' | sed 's/^0X/0x/')" > "$dir/repo/scripts/privacy-allowlist.txt"
  out="$(LOOP_ROOT="$dir/repo" FBC_PRIVACY_KEY_FILES="" "$0")" || { echo "self-test: an allow-listed address should pass: $out"; exit 1; }
  rm "$dir/repo/src/acct.txt" "$dir/repo/scripts/privacy-allowlist.txt"
  # 11. Ignored files are not scanned.
  echo "target/" > "$dir/repo/.gitignore"; mkdir -p "$dir/repo/target"
  printf '%s\n' "$hex" > "$dir/repo/target/build.txt"
  out="$(LOOP_ROOT="$dir/repo" FBC_PRIVACY_KEY_FILES="" "$0")" || { echo "self-test: ignored files should not be scanned: $out"; exit 1; }
  echo "privacy-check self-test: ok"
}

case "${1:-}" in
  --self-test) self_test ;;
  "") scan ;;
  *) echo "usage: scripts/privacy-check.sh [--self-test]" >&2; exit 2 ;;
esac
