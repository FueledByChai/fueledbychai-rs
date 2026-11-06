#!/usr/bin/env bash
# Exact replay's golden (decisions 0006 and 0142 item 12, FBC-qwq): the conformance-toy
# order-entry session's journal in fixtures/conformance-toy/exact_replay/golden/, which
# crates/fbc-runtime/tests/exact_replay.rs replays to every outbound byte it holds outside
# redaction spans, signatures included. That test's fresh recording replaces the golden only when
# FBC_REFRESH_GOLDENS is 1, which scripts/check.sh unsets for every step and sets for this
# script's refresh alone, when it is given --refresh-goldens.
#
#   scripts/golden-refresh.sh sum            print the golden's checksum, file names included
#   scripts/golden-refresh.sh refresh        run that test with FBC_REFRESH_GOLDENS=1, and fail
#                                            unless it rewrote every file of the golden
#   scripts/golden-refresh.sh unchanged SUM  fail unless the golden's checksum is still SUM
#   scripts/golden-refresh.sh --self-test    prove the three on a golden of its own, the test
#                                            stood in for by a script that writes as its store
#                                            gate does, then check that the test refresh runs
#                                            is still in its file under its name
#
# A refresh judges a file rewritten by its modification time, against a mark taken a second
# before the test runs, so a file system that keeps times to the second still tells.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SELF="$ROOT/scripts/$(basename "$0")"
cd "$ROOT"
GOLDEN=fixtures/conformance-toy/exact_replay/golden
TEST=a_freshly_journaled_session_replays_to_its_outbound_bytes_on_every_replay
TEST_FILE=crates/fbc-runtime/tests/exact_replay.rs

usage() {
  echo "usage: scripts/golden-refresh.sh sum | refresh | unchanged SUM | --self-test" >&2
  exit 2
}

# sum DIR: a checksum of every file under DIR, by its path below DIR and its bytes.
sum() {
  (cd "$1" && find . -type f -exec cksum {} + | LC_ALL=C sort | cksum)
}

# refresh DIR CMD...: runs CMD with FBC_REFRESH_GOLDENS=1, for CMD alone, then fails naming
# every file under DIR that CMD did not rewrite, and fails when DIR then holds no file at all.
# CMD starts a second after the mark, so what it writes is newer than the mark whatever the file
# system's resolution.
refresh() {
  local dir="$1" marker stale
  shift
  marker="$(mktemp)"
  sleep 1
  if ! FBC_REFRESH_GOLDENS=1 "$@"; then
    rm -f "$marker"
    echo "golden refresh: the refresh failed: $*" >&2
    return 1
  fi
  stale="$(find "$dir" -type f ! -newer "$marker")"
  rm -f "$marker"
  if [ -n "$stale" ]; then
    echo "golden refresh: not rewritten:" >&2
    echo "$stale" >&2
    return 1
  fi
  if [ -z "$(find "$dir" -type f)" ]; then
    echo "golden refresh: $dir holds no file" >&2
    return 1
  fi
  echo "rewrote $dir"
}

# named FILE TEST: fails unless FILE declares the test TEST, so a renamed test fails every check,
# not only a refresh.
named() {
  if ! grep -B1 -x "fn $2() {" "$1" | grep -qx '#\[test\]'; then
    echo "golden refresh: $1 holds no test $2, which the refresh runs" >&2
    return 1
  fi
}

# unchanged DIR SUM: fails unless DIR's checksum is SUM.
unchanged() {
  if [ "$(sum "$1")" != "$2" ]; then
    echo "golden unchanged: a step rewrote $1 without --refresh-goldens (FBC-qwq)" >&2
    return 1
  fi
  echo "ok: $1 as committed or as --refresh-goldens left it"
}

self_test() {
  local tmp golden standin before out
  tmp="$(mktemp -d)"
  # shellcheck disable=SC2064 # removed as named now: the function's locals are gone at exit
  trap "rm -rf '$tmp'" EXIT
  golden="$tmp/golden"
  standin="$tmp/stand-in"
  fail() { echo "golden-refresh self-test: $1" >&2; exit 1; }
  # A golden as committed: a day's journal and the two text files, all older than any refresh.
  committed() {
    rm -rf "$golden"
    mkdir -p "$golden/journal/20000101"
    printf 'old journal' > "$golden/journal/20000101/4-000000.fbcj"
    printf 'old session' > "$golden/session.txt"
    printf 'old outbound' > "$golden/outbound.txt"
    find "$golden" -type f -exec touch -t 200001010000 {} +
  }
  # The test stood in for: as its store gate does, it writes only when FBC_REFRESH_GOLDENS is 1,
  # and then as MODE says: the whole golden replaced (a new day's journal), one file only, none
  # (a renamed test, which cargo runs as no test at all), a failure, or an empty golden.
  cat > "$standin" <<'STANDIN'
#!/usr/bin/env bash
set -euo pipefail
golden="$1" mode="$2"
[ "${FBC_REFRESH_GOLDENS:-}" = 1 ] || exit 0
case "$mode" in
  all)
    rm -rf "$golden"
    mkdir -p "$golden/journal/20000102"
    printf 'new journal' > "$golden/journal/20000102/4-000000.fbcj"
    printf 'new session' > "$golden/session.txt"
    printf 'new outbound' > "$golden/outbound.txt" ;;
  one) printf 'new session' > "$golden/session.txt" ;;
  none) ;;
  fail) exit 3 ;;
  empty) rm -rf "$golden" && mkdir -p "$golden" ;;
esac
STANDIN
  chmod +x "$standin"

  # Rewriting every file passes, the variable set for the refresh's command alone.
  committed
  before="$(sum "$golden")"
  unset FBC_REFRESH_GOLDENS
  out="$(refresh "$golden" "$standin" "$golden" all 2>&1)" || fail "a whole rewrite failed: $out"
  [ -z "${FBC_REFRESH_GOLDENS+set}" ] || fail "the variable outlived the refresh's command"
  [ "$(sum "$golden")" != "$before" ] || fail "a whole rewrite left the golden as it was"
  [ -f "$golden/journal/20000102/4-000000.fbcj" ] && [ ! -e "$golden/journal/20000101" ] ||
    fail "a whole rewrite did not replace the journal"

  # Every step after the refresh runs the test with the variable unset: the golden is unchanged.
  before="$(sum "$golden")"
  "$standin" "$golden" all
  out="$(unchanged "$golden" "$before" 2>&1)" || fail "an unset variable changed the golden: $out"

  # A rewrite of one file fails, naming each file left as it was.
  committed
  out="$(refresh "$golden" "$standin" "$golden" one 2>&1)" && fail "a partial rewrite passed"
  case "$out" in
    *outbound.txt*4-000000.fbcj* | *4-000000.fbcj*outbound.txt*) ;;
    *) fail "a partial rewrite did not name what it left: $out" ;;
  esac
  case "$out" in *session.txt*) fail "a partial rewrite named a file it rewrote: $out" ;; esac

  # A refresh that rewrites nothing, a renamed test among them, fails.
  committed
  out="$(refresh "$golden" "$standin" "$golden" none 2>&1)" && fail "an empty refresh passed"
  case "$out" in *"not rewritten"*) ;; *) fail "an empty refresh said otherwise: $out" ;; esac

  # A refresh whose command fails fails, and so does one that leaves no file.
  committed
  out="$(refresh "$golden" "$standin" "$golden" fail 2>&1)" && fail "a failed refresh passed"
  case "$out" in *"the refresh failed"*) ;; *) fail "a failed refresh said otherwise: $out" ;; esac
  out="$(refresh "$golden" "$standin" "$golden" empty 2>&1)" && fail "an emptied golden passed"
  case "$out" in *"holds no file"*) ;; *) fail "an emptied golden said otherwise: $out" ;; esac

  # The checksum sees a changed byte, an added, a removed and a renamed file.
  committed
  before="$(sum "$golden")"
  out="$(unchanged "$golden" "$before" 2>&1)" || fail "an untouched golden read as changed: $out"
  printf 'old sessioN' > "$golden/session.txt"
  unchanged "$golden" "$before" 2>/dev/null && fail "a changed byte went unseen"
  committed
  printf 'more' > "$golden/extra.txt"
  unchanged "$golden" "$before" 2>/dev/null && fail "an added file went unseen"
  committed
  rm "$golden/outbound.txt"
  unchanged "$golden" "$before" 2>/dev/null && fail "a removed file went unseen"
  # Renamed, a golden's one file keeps every byte: only its name tells.
  committed
  rm "$golden/outbound.txt" "$golden/journal/20000101/4-000000.fbcj"
  before="$(sum "$golden")"
  mv "$golden/session.txt" "$golden/session.text"
  unchanged "$golden" "$before" 2>/dev/null && fail "a renamed file went unseen"

  # An unknown request is refused.
  "$SELF" bogus 2>/dev/null && fail "an unknown request was taken"
  [ $? -eq 2 ] || fail "an unknown request did not exit 2"

  # A test file without the test, renamed or no longer a test, fails; this one's has it.
  printf '#[test]\nfn %s_renamed() {\n}\n#[test]\nfn other() {\n    // %s\n}\n' "$TEST" "$TEST" \
    > "$tmp/renamed.rs"
  printf 'fn %s() {\n}\n' "$TEST" > "$tmp/untested.rs"
  named "$tmp/renamed.rs" "$TEST" 2>/dev/null && fail "a renamed test went unseen"
  named "$tmp/untested.rs" "$TEST" 2>/dev/null && fail "a function that is no test passed"
  out="$(named "$TEST_FILE" "$TEST" 2>&1)" || fail "the refresh's test is missing: $out"
  echo "golden-refresh self-test: ok"
}

case "${1:-}" in
  sum) [ $# -eq 1 ] || usage; sum "$GOLDEN" ;;
  refresh)
    [ $# -eq 1 ] || usage
    refresh "$GOLDEN" cargo test -p fbc-runtime --test exact_replay --quiet -- --exact "$TEST" ;;
  unchanged) [ $# -eq 2 ] || usage; unchanged "$GOLDEN" "$2" ;;
  --self-test) [ $# -eq 1 ] || usage; self_test ;;
  *) usage ;;
esac
