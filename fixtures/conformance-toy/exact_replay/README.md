# Exact replay's golden

Decision 0006's exact replay: a journaled order-entry session, replayed through a fresh codec of
its venue, reproduces every outbound byte the journal holds outside its redaction spans,
signatures included. Decision 0142 states how a replayed frame or request is matched with a
journaled one, and how this golden is kept (its item 12). `golden/` is the journal of one
conformance-toy order-entry session, which `crates/fbc-runtime/tests/exact_replay.rs` replays
(FBC-qwq). The toy describes no real venue, and nothing here was recorded from one.

## What `golden/` holds

| File | What it is |
| --- | --- |
| `journal/<YYYYMMDD>/4-<seq>.fbcj` | the session's journal, shard 4, as `fbc-journal` wrote it: two epochs of the toy's order connection (connection 6). The first authenticates, arms cancel-on-disconnect, resyncs over REST, then carries the consumer's place, amend and cancel, an order query, a fee query that reaches its deadline, and a ping; the second authenticates, arms and resyncs again |
| `session.txt` | what a replay is given besides the journal: the REST base and its credential span, and the request ids the session gave the consumer's commands |
| `outbound.txt` | every outbound frame and HTTP request the journal holds, one per line, spans blanked as the reader returns them: what a person checks the journal by |

The test checks that `outbound.txt` renders the journal exactly, and that two replays of the
journal reproduce each line, nothing differing and nothing unwritten.

## How it was made

By the test `a_freshly_journaled_session_replays_to_its_outbound_bytes_on_every_replay`, run
with `FBC_REFRESH_GOLDENS=1`, the one value under which it replaces this directory.
`scripts/check.sh --refresh-goldens` runs it that way, through `scripts/golden-refresh.sh
refresh`, and fails unless every file here was rewritten; every other step of the check runs
with the variable unset. Any `cargo test` run with that variable set rewrites it too. The test
runs an `ExecSession` of the toy, as its factory builds it, against the test's own WebSocket and
HTTP stubs on 127.0.0.1 ephemeral ports, on a paused clock. It records the session's journal,
replays it twice, and copies it here with the two text files.

So each refresh changes these, and nothing else changes them:

- the REST port;
- the wall times in the contexts, frames and URLs;
- the signatures, which cover those times;
- the day directory.

The place, amend and cancel are built and authorized by fbc-oms (`tests/armed_oms/`), with client
ids from a mint the test starts at 1. The consumer's commands are therefore the same on every run,
and the test builds them again to supply by request id (decision 0138 item 1).

Refresh only when a deliberate change alters what the session writes: the toy's codec, the session,
or the journal's format (decision 0142 item 12). Say so in the commit body. Without the flag, every test run replays
this journal as it stands, and the check's last step (`scripts/golden-refresh.sh unchanged`)
fails if anything rewrote it.

## Why it holds no secret

- The toy's token (`auth|...|token=`) is a redaction span. The journal keeps only its keyed hash,
  under the test's made-up 32-byte redaction key.
- The REST base's last path segment is a made-up credential, marked as a span of its URL. The
  journal keeps only its hash, and `session.txt` holds it blanked (`2`, decision 0028 item 5).
  A replay needs only the bytes outside the spans.
- A signature is the toy signer's FNV-1a hash of the request's signed fields. The signer holds no
  key.
- Client ids are the toy's spelling of the test mint's sequence numbers in the toy's namespace.

Nothing here is key- or address-shaped, so this directory carries no `SYNTHETIC` marker. It holds
a recorded journal, which decision 0009 keeps out of a `SYNTHETIC` directory, so the privacy scan
covers it.
