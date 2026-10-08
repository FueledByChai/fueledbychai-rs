# 0079 — The log facade's TRACE level is compiled out of every build that links fbc-runtime, since tungstenite logs each frame at TRACE

Status: accepted
Date: 2026-10-07

## Context

0009 forbids a private key, JWT, API key or secret in a log. This workspace logs nothing
itself: it has no `tracing` or `log` call. What runs beneath fbc-runtime's WebSocket does:
tungstenite (0.30.0, under tokio-tungstenite, 0019) reports through the `log` facade, and at
TRACE it writes every frame it sends and receives into the record (`Sending frame: Frame {
.. payload: b"..." }`, `writing frame` with the payload in hex, `Received message ..`). A
consumer that installs any `log` logger at TRACE therefore gets the Paradex auth frame,
`{"method":"auth","params":{"bearer":"<session token>"}}`, verbatim, and every signed order
frame. FBC-8mv's offline rehearsal, which captures everything logged at TRACE while fbc-oms,
the order-entry session and the Paradex codec run against the conformance stub, found the
session token there on its first run. Nothing in this workspace can filter a record tungstenite
makes before the consumer's logger sees it.

## Decision

fbc-runtime depends on `log` (pinned exactly, already in the lockfile through tungstenite) with
its `max_level_debug` feature, and on nothing else of it. Cargo unifies a dependency's features
across a build, so in every build that links fbc-runtime, the consumer's included, the facade's
static maximum level is DEBUG: each `trace!` in any crate of that build compiles to nothing, and
no logger, whatever level it sets, can receive a TRACE record. What tungstenite logs at DEBUG
and above (handshake done, close frames) carries no frame payload.

The feature alone does not hold in every build. In a build without debug assertions (a release
build) `log` takes a `release_max_level_*` feature before any `max_level_*` one, so a crate
anywhere in the consumer's build that enables `release_max_level_trace` brings TRACE back in
release. fbc-runtime therefore also asserts at compile time (`const _: () = assert!(..)` in its
lib.rs) that the facade's static maximum is DEBUG or below. The guarantee is enforced at build
time: such a build fails to compile with an error naming this record, rather than running with
TRACE. A stricter level (`release_max_level_info` and below) passes.

The cap and the assertion act on one `log` package, and it is the one tungstenite logs through:
Cargo never selects two semver-compatible versions of one package in a build, so every `log`
0.4 requirement in the consumer's graph (tungstenite's `^0.4.17`, fbc-runtime's exact pin, the
consumer's own) resolves to the same package, and a consumer pinning another 0.4 release fails
to resolve rather than getting a second, uncapped copy (Codex r4217150467 on PR #119, checked
with a scratch consumer pinning `log = "=0.4.33"`: "failed to select a version for `log`").
`scripts/check-deps.sh` also refuses this workspace's graph if it ever holds a second `log`
package (another major or another source) or if tungstenite's `log` edge is not fbc-runtime's. fbc-runtime cannot also set
`release_max_level_debug`, since `log` refuses two `release_max_level_*` features in one build,
which would break a consumer that sets a stricter one.

## Alternatives

- Tell consumers never to enable TRACE for tungstenite's targets: a rule in a document, broken
  by the first logger set to TRACE while debugging a disconnect, and the token is then in a log
  file.
- `release_max_level_debug` only: debug builds, which an owner may run against testnet with
  real credentials, would still log every frame.
- `release_max_level_debug` beside `max_level_debug`: `log` refuses two `release_max_level_*`
  features in one build, so a consumer setting a stricter release level could not build.
- The feature without the compile-time assertion: a release build with
  `release_max_level_trace` anywhere in it would log every frame (Reviewer B RB-8mv-1 on PR #119,
  reproduced with log 0.4.34: DEBUG in the debug build, TRACE in the release build).
- Redact inside the runtime: the records are made inside tungstenite, before any code here sees
  them. Replacing or forking tungstenite to remove its logging is a far larger change than this.

## Consequences

- No crate in a build that links fbc-runtime logs at TRACE through `log`, the consumer's own
  `log::trace!` calls included; a build whose features would allow it does not compile. A `tracing` subscriber still receives `tracing`'s own TRACE
  events, which do not pass through the facade's static maximum.
- A consumer cannot set a `max_level_*` feature of its own, a stricter one included: `log`
  refuses two `max_level_*` features in one build (`compile_error!("multiple max_level_*
  features set")` in log 0.4.34), so that build does not compile. Such a consumer sets a
  stricter level with a `release_max_level_*` feature, which applies to release builds, or with
  its logger's runtime filter, which applies to every build.
- Proof: `crates/venues/fbc-venue-paradex/tests/rehearsal.rs` installs a logger at TRACE for
  the whole session and finds no token, login signature or key in what was logged;
  `crates/fbc-runtime/src/ws.rs`'s `the_log_facade_s_trace_level_is_compiled_out` fails if the
  feature is dropped, and the assertion in `crates/fbc-runtime/src/lib.rs` fails the build
  (error E0080) when a release build's features raise the level to TRACE;
  `crates/fbc-runtime/tests/log_facade.rs` installs a logger through fbc-runtime's `log` at
  TRACE, receives tungstenite's own DEBUG records over an in-memory WebSocket (so tungstenite
  logs through that facade) and none at TRACE or carrying the frame's payload; and
  `scripts/check-deps.sh` (with its self-test) finds exactly one `log` package in the graph.
- Moving tungstenite or `log` re-runs the rehearsal, which reads what the new version logs.

## What would show this was wrong

A record at DEBUG or above carrying a frame payload or a credential after a tungstenite or
hyper upgrade, which this level does not stop; or a consumer needing TRACE from `log` badly
enough to take the logging over by another route.
