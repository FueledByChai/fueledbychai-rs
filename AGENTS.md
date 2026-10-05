# Standing instructions for agents

This file is the contract for any agent working in this checkout, whatever harness or model
runs it. It replaces instructions that would otherwise have to be repeated in chat. The first
part is the ticket loop, which is the same in every project that uses it; the **Project rules**
below it are this project's own and are what the loop prompts mean when they say "Project rules".

## The loop

- **Settings.** `.loop.toml` holds everything the loop knows about this project:
  `default_branch`, `check` (the full check), `check_fast` (the check to run while iterating),
  `review_paths` (changes that need a human review),
  `trailer_required`, `kit` (where the loop kit lives), and `sprint_label` (the Beads label that
  marks the tickets to work now, by priority then id: either a chosen subset, leaving the rest as
  the pick list `--open` prints, or every open ticket, so an omission is a fault). `scripts/loop-config.sh --all` prints the effective values. Prompts and scripts read them from there; they never hard-code
  a branch, a path, or a build command.
- **Tickets.** The queue is Beads (`bd`), a hard dependency. A ticket is an issue whose id is
  `<PREFIX>-<suffix>`, where the suffix is a number until the prefix's numeric space is spent
  and then the alphanumeric id Beads mints instead (`LK-1af`): its `description` is the intent,
  its `acceptance_criteria` is the done line naming the test, fixture, or measurable output that
  proves it, its `blocks` dependencies are its blockers, and its `section:` and `story:` labels
  group it and name the story it serves. Git is the record of done: a ticket is done when a
  commit whose subject starts with its id is on the default branch, and
  `scripts/backlog-status.sh` derives every ticket's state
  from the commits and the queue (`--next` names the first ready ticket, the `sprint_label`
  tickets first by priority; `--sprint` their states; `--open` the tickets outside the sprint;
  `--sprint-check` the label pairing; `--reconcile` git against Beads; `--show <id>` a ticket
  or story in full; `--stories` every story with a status derived from the tickets that serve
  it; `scripts/sprint.sh add|remove|set` edits the Beads labels). A ticket Beads says is
  `closed` with no naming commit is a false close, and a landed commit naming a ticket Beads
  still has open is an orphan; `--reconcile` fails both. Anything discovered while working goes
  in as a new ticket (`bd create`), not into the current one.
- **Claims and hand-off.** Before work starts, claim the ticket in Beads (`bd update <id>
  --claim`) and push `ticket/<id>` to origin with `scripts/open-ticket-pr.sh <id> --claim`;
  `backlog-status.sh --next` passes over claimed tickets, so several agents can hold several
  tickets. After the commit, `scripts/open-ticket-pr.sh <id>` pushes
  the branch and opens the pull request. During queue rollout, use `--draft`, verify auto-merge
  is disabled, then mark ready for review; retain any required human review for `review_paths`.
  Several agents can implement at once. In the legacy profile, the named coordinator records
  merge order and selects one candidate in Beads (0019). In operator-enabled `github-v1` (0026),
  the App selects the candidate and owns CI admission, the merge gate and merging. Existing
  authors and independent reviewers use `queue-handoff` for nomination, selection verification
  and acceptance; they do not create or claim a legacy coordinator, reset legacy statuses or
  publish `Agent review`. Normal Beads ticket claims and proof requirements still apply.
  Waiting PRs do not rebase or request CI. Never run
  `scripts/open-ticket-pr.sh --update-all` as a review handoff. Only the selected candidate gets
  the project's sanctioned refresh after predecessors land, followed by completed head review,
  independent acceptance, then CI and final gate verification. Shadow planning grants no live
  admission. Never push the default branch. Never force-push. Never
  rewrite its history. For legacy gates, `scripts/pr-readiness.sh` prints, for every open pull request, the six
  facts a merge waits on (`--pr <number>` for one of them, `--ready` for the ones that pass all
  six): the project's check ran on the head, no review conversation is unresolved, no changes
  are requested, the agent review's status is success, the branch is current and clean, and the
  branch and the head commit subject name one claimed Beads ticket.
- **Commits.** One initial implementation commit per ticket. After publication, append review-fix
  commits on that ticket's branch with the same ticket id rather than rewriting published history.
  Keep unrelated work in another ticket. Every commit's subject starts with the ticket id (`AB-12: ...`); the
  body says what changed and how the done line is proven; the message ends with a
  `Co-Authored-By: <agent> <email>` trailer naming the agent and model that did the work when
  `trailer_required` is on (the PR script refuses a commit without one).
- **Definition of done.** The full check (`check` in `.loop.toml`) passes, and the commit
  includes the test or fixture that proves the ticket's done line; `scripts/proof-gate.sh`,
  run from the check, fails a code change that brings none (a commit body line
  `No new test: <reason>` is the stated exception), and `scripts/coverage-ratchet.sh` fails
  a drop in coverage below the committed floor (a ticket that raises coverage raises the
  floor with `--set` in its commit). Run the fast check while iterating and the full check
  before committing. CI runs the same script; there are no separate
  hand-written CI steps to keep in sync.
- **Isolation.** Prefer an isolated worktree per ticket. The check script knows how to run
  from one (see the project rules for what it resolves).
- **Releases.** Tag them: `scripts/release-notes.sh <from> <to>` lists what shipped, and
  `--archive <tag>` writes the release into `CHANGELOG.md` and closes its shipped tickets in
  Beads.
- **Decisions.** Architecture and product decisions live as records in the decisions
  directory (`decisions` in `.loop.toml`, `docs/decisions` by default), one numbered file each,
  never edited in place; a change is a new record that supersedes the old
  (`scripts/decisions.sh new "<title>" [--supersedes NNNN]`). Read the index before deciding
  anything; cite records by number; write one when a choice is made.
- **Prompts.** `loop/prompts/next-ticket.md` takes the next ticket to done;
  `loop/prompts/grill-me.md` turns a loose idea into stories, acceptance criteria, and
  tickets; `loop/prompts/grill-project.md` is the first-day interview that writes the
  Project rules, the decision records, the first epics, and the check skeleton for a new
  project; `loop/prompts/review-prs.md` reviews the open pull requests and posts the
  `review_context` status the branch rules require (red only for a missing proof, an unmet
  done line, a rules breach, or a named defect). A harness with slash commands wraps them
  in its command directory, and one that loads skills gets a `SKILL.md` pointer per prompt;
  both are pointers, so neither restates a rule; any other agent is pointed at the prompt
  file directly.
- **The kit.** The scripts and prompts are copies from the loop kit named by `kit` in
  `.loop.toml`; `scripts/loop-kit-sync.sh --check` fails when they drift, and
  `scripts/loop-kit-sync.sh` brings them up to the kit's tag. Change them in the kit, not here.

## Project rules

A Rust library that takes venue credentials and instrument names and gives a trading program
one common interface for market data, order entry and account events on any supported venue
(Paradex first, then Hibachi, with Binance futures as reference data). It replaces the Java
FueledByChaiTrading library for new Rust work.

This repository is public under MIT and holds connectivity and order truth; whatever decides
what to quote lives in a private consumer that depends on this one by git tag (decisions 0001,
0008). "Design §n" in the records and tickets is a section of the approved architecture design
(revision 2). It is kept in the private consumer's repository (chaiwala-rs,
`docs/design/architecture-rev2.md`), which is available on the owner's machine but not in this
checkout. The records restate what they decide (0005 lists the lattice invariants I1–I9), but
they do not carry the whole design. A ticket that cites a design section must restate what it
needs from it; if it does not, and the design is not readable where you work, ask the owner
rather than guess.

### Layout

- A Cargo workspace (0001, 0008). `crates/fbc-core` exists, with time, units,
  price grids, exact prices, sealed ids, the client-id codec, `NamespaceLease`, `CidMint`,
  `Fee`/`FeeBook`, `DecodeScope`, `InstrumentSpec` (maker-safe `quantize`), instrument
  resolution (`AssetKey`, `AliasTable`, `InstrumentResolver`, `InstrumentSpecDraft`; 0035),
  `VenueCaps`, the market-data and execution events, venue commands, and the codec, signer,
  nonce and factory traits with `Effects` and `EncodeCtx` (the factory's discovery an
  `HttpPlan`), and the write-only `PathStamps` a codec marks
  its latency stages through (0034), `crates/fbc-book` with its L2 book, `crates/fbc-sim` with its
  queue-position fill model (0038), `crates/fbc-journal` with its record
  format, day-grouped segment writer rolled hourly with zstd-compressed closed segments, in-order reader and never-blocking sink, `crates/fbc-runtime` with its first
  slices (the connector, SOCKS5 CONNECT, WebSocket and HTTP/1.1, plain or TLS, 0019 and 0020, the WebSocket handshake its own, 0029;
  connection epochs and the subscription reconciler; the market-data session, 0023; HTTP
  effects, poll endpoints and plans, 0027; journaling and decoder replay, 0006; rate-limit buckets with a safety reserve, 0030), `crates/fbc-conformance` with its stub venue server (0025),
  `crates/fbc-oms` with its first slice (the order lattice, item outcomes and the registry by
  client id; 0005, property-tested with proptest, 0037),
  `crates/venues/fbc-venue-paradex` with its signer (`src/sign`), SBE market data (`src/md`: bbo,
  trades and the order book) and its factory (`src/factory.rs`, market data only),
  and `crates/venues/fbc-venue-binance-usdm` with its market-data-only factory and codec; the
  rest are planned. Members are the `crates/fbc-*` and
  `crates/venues/fbc-*` globs, and `fixtures/` is excluded from the workspace:
  - `crates/fbc-core`: the contract. Time, units, price grids and exact prices, sealed ids and
    the canonical client-id codec, `Fee`/`FeeBook`, `InstrumentSpec`, `VenueCaps`, events,
    commands, the codec and factory traits, `DecodeScope` and `EncodeCtx` (0003, 0004), and
    `PathStamps`, whose marks reach the runtime's `PathRecorder` and give back no time (0034).
    `VenueCaps` keeps order and fill capabilities together in `exec: Option<ExecCaps>`,
    `None` for a market-data-only venue, which declares no fill source or fee sign (0015).
    Every codec names the credentials in what it receives (`redact_inbound`, `InboundSpans`;
    0028) so the journal keeps them only as keyed hashes.
    `Lots` is built only through `Lots::new`, which refuses a negative count, and `Ticks`,
    `Lots` and `SignedLots` have checked arithmetic only (`checked_*` and `abs_lots` return
    `None` rather than wrap; no `+`, `-` or unary `-`), so overflow behaves the same in every
    build profile (the `units` module documents the policy).
  - `crates/fbc-runtime`: the one generic runtime: WebSocket, TLS and HTTP through SOCKS5,
    reconnects and connection epochs, subscription reconciliation, rate limits with a safety
    floor, kernel receive timestamps, the shard host (0002). So far `ProxyConfig` (`Direct` or
    `Socks5 { host, port }`, no credentials), `Connector` (every TCP connection, directly or
    through a hand-written SOCKS5 CONNECT that sends the target's host name), TLS on that stream
    (rustls with the ring provider, webpki-roots plus `Connector::add_trust_anchor`, hostname
    verification always on; 0020), a WebSocket client (`ws://`, `wss://`) and an HTTP/1.1 call
    (`http://`, `https://`) over it, and `NetError` naming the failed step; its network
    dependencies are pinned exactly (0019, 0020). `tests/common/` holds the SOCKS5 stub,
    WebSocket server and HTTP server on 127.0.0.1 ephemeral ports, each server plain or behind
    TLS with a certificate from a CA the test generates in memory, that later runtime tests
    reuse. `Epochs` numbers a stream's connection lives and drops, counting per kind, a frame,
    timer, HTTP result or event of an older epoch; `Reconciler` yields a stream's subscribe
    calls as the difference between the desired and the active set, once per new epoch,
    keeping a refused or waiting subscription pending. Both are logic only, with no socket.
    `MdSession` drives one market-data endpoint over them (0023): a fresh codec per
    epoch, events stamped (an `IngestClock` shared per shard) and handed to the consumer's
    `MdHandler` inline, the codec's effects executed, reconnects paced by `ReconnectPacing` (backoff, attempt budget, attempt deadline);
    a socket endpoint's keepalive, rotation before the venue's `max_conn_lifetime` and silence
    alarm, from the consumer's `Liveness` (0033);
    a write a peer stopped reading bounded by the consumer's `WriteStall` window, ending the
    epoch as a drop, with timers firing meanwhile (0036);
    a codec's HTTP requests run with their timeouts and answer only the epoch that asked, and a
    poll endpoint opens no connection (0027); `MdVenue` applies `plan_md`'s endpoints by stream;
    `MdBooks` (fed by the `BookKeeper` handler) keeps one `fbc-book` book per (instrument,
    `BookId`), routing each book event by its channel, and exposes each instrument's trading
    book as the consumer's `TradingBooks` configures it (FBC-nij); a session tells its handler
    each connection epoch's end (`MdHandler::on_epoch_end`, live and in replay), and every book
    that epoch fed is then gapped until its next snapshot (0039);
    a `Journal` (the consumer's `fbc-journal` sink) set on a session or venue records every input,
    output and connection change under its traffic class and is never waited on (0006);
    `RateLimiter` keeps a sliding-window bucket per declared limit and scope key, charged by
    each frame's, request's and connection attempt's rate charge, normal traffic stopping at
    the consumer's `SafetyReserve`, refusals and 429/418 counted by scope (0030);
    `MdReplay` feeds one session's journal back to the venue's decoders through the same calls,
    with the recorded stamps, effects not executed (decoder replay; the caller supplies the
    `VenueConfig` and `SpecTable`);
    every connection reads through `Tcp`, which on Linux keeps the `SO_TIMESTAMPNS` kernel
    receive time beneath TLS and WebSocket (nix, Linux only), so a frame's stamp carries
    `kernel_rx`, and a session reports each Safety write attributed to such a frame, the
    codec's or one the handler issues through its `Outbox`, as a `TickToWire` (0031);
    `tests/common/toy.rs` is the toy market-data venue later runtime tests reuse.
  - `crates/fbc-book` (it depends on `fbc-core` only: one tick-indexed L2 book per
    instrument and `BookId`, built from book events the same live and in replay, with
    snapshots, deltas, windows and gap invalidation, touch, top-n, one level's size where it is
    known (inside the window, or no deeper than a capped book shows), an exact
    top-n comparison and canonical bytes; still planned: the ex-own projection, touch arbitration and
    continuity policies), `crates/fbc-oms` (it depends on `fbc-core` only, `proptest` as a
    dev-dependency, 0037: so far `OrderRecord`, whose ranked states, PendingNew and Unknown 0,
    Open 1, PartiallyFilled 2, Terminal 3, only move forward with a terminal state absorbing,
    `apply_update` under an `OrderKey` (the venue's ordering key, ingest tiebreak), amends that
    issue new venue ids and the superseded ids they leave, `on_outcome` per command item, and
    the `Registry` by client id that routes each update by client id or any venue id the order
    had; I1 for order updates and I2 property-tested in `tests/lattice.rs`; still planned: the
    `FillLedger` and second fill counter, permits, pre-trade caps and `ExecutionPlanner`; 0005,
    0013), `crates/fbc-journal` (0006; it depends on
    `fbc-core`, and `hmac` and `sha2` for its keyed hashes and `zstd` for its closed segments, only: the records the runtime writes, length-prefixed in a hand-written
    little-endian format with a version, a writer of one subdirectory per UTC day with segments
    named `<shard>-<seq>.fbcj` and time from the caller, rolled at every UTC hour, each closed
    segment compressed into `<shard>-<seq>.fbcj.zst` (0026), a reader in write order of both forms, and no byte of
    a redaction span or secret header written but each as its HMAC-SHA-256 under the
    consumer's `RedactionKey` (`src/redact.rs`, 0024; signatures kept verbatim), and a
    `JournalSink` that never blocks: a hand-written queue of atomic words (0021) with a consumer-configured byte budget, a soft
    limit for Normal records and a Safety reserve, drops counted by class, a `Degraded` marker
    once space returns, and a writer thread draining it; the nonce, encode-context and cycle
    records arrived in format version 3, and the keyed hashes of the credential spans a codec
    names in inbound frames and responses (`redact_inbound`, 0028) in version 4, and the kind
    each outbound frame was sent as in version 5, whose reader still reads version 2 to 4
    journals), `crates/fbc-sim` (simulated venue and queue-position
    fill-model code, never calibrations; it depends on `fbc-core` and `fbc-book` only: so far
    `QueueModel`, each modelled order's place in its level's queue under a `Bracket`
    (Pessimistic, Middle, Optimistic) from a `QueueConfig` with no `Default`, queued on arrival
    behind its level on the public book less every modelled public order, an RPI order also
    behind public size that joins later, advanced by level cancels per bracket and filled by
    trades at or through its price, 0038; still planned: the simulated venue,
    FBC-uoo), `crates/fbc-conformance` (the adapter conformance kit,
    0025; it depends on `fbc-runtime`, and on `fbc-core` in its tests, never on a venue crate: so
    far a public stub venue server on 127.0.0.1 ephemeral ports, a WebSocket endpoint that plays
    a fault script of typed steps and records every connection and data frame, and an HTTP/1.1
    endpoint with fixed responses by path; the reconnect-storm script and a check of connection
    attempts against `ReconnectPacing`; the duplicate-ack and silence-after-subscription
    scripts; venue crates take it as a dev-dependency instead of writing their own servers;
    still planned: the named suite and its macro, and the full conformance venue).
  - `crates/venues/fbc-venues` (the registry, the only crate that sees concrete venues),
    `crates/venues/fbc-venue-binance-usdm` (market data only:
    `exec: None` caps citing Binance's USD-M pages, `plan_md` on the `/public` combined-stream
    endpoint, live SUBSCRIBE/UNSUBSCRIBE, `bookTicker` touches, partial-depth snapshots, and
    the diff-depth book in `src/diff.rs`, anchored on a `GET /fapi/v1/depth` snapshot with
    `pu` continuity, duplicates ignored, a gap resynced from a new snapshot and a failed
    snapshot retried on a configured timer), `crates/venues/fbc-venue-paradex`
    (so far its signer: the SNIP-12 revision 0 hash and Stark-curve signature, tested against
    `fixtures/paradex/signing/paradex-vectors.tsv`; and market data: an SBE reader gated on each
    frame's block lengths, bbo and trades decoded into touches and trades, the order book
    deltas into book events with seq_no continuity (0022), held to the hand-built frames and one
    captured public frame in `fixtures/paradex/md/`; the REST `/orderbook` snapshot at depth 15
    (`src/md/rest.rs`); and, in `tests/oracle/` with `fbc-book` as a dev-dependency only, the
    BT-401 book and bbo-touch agreement checks), later `crates/venues/fbc-venue-hibachi`
    (0016).
  - Dependency direction (design §3): `fbc-core`, `fbc-book`, `fbc-oms`, `fbc-journal` and
    `fbc-sim` never depend on a venue crate; a venue crate depends on `fbc-core` and protocol
    crates only. Nothing here depends on the private consumer. `scripts/check-deps.sh` holds
    every crate outside `crates/venues/` (`fbc-runtime` and `fbc-conformance` included) to the
    first rule, in normal and build dependencies (not dev-dependencies, which venue tests use).
- Tests: unit tests beside the code (`#[cfg(test)]`), integration and property tests in
  `crates/<crate>/tests/`, `trybuild` compile-fail tests for the seals in `fbc-core`'s tests.
- Fixtures in `fixtures/<venue>/`: recorded frames, journals and signing vectors. Synthetic
  key- or address-shaped values sit in a directory of their own carrying a `SYNTHETIC` file
  that says where they come from (0009); that marker exempts only its own directory from the
  privacy scan, so recorded frames never go in it. `fixtures/paradex/signing/` already holds
  the signing benchmark and the Java hash oracle the Paradex signer is checked against;
  recorded Paradex frames go elsewhere under `fixtures/paradex/`; `fixtures/paradex/md/` holds
  SBE frames hand-built from Paradex's published schema, and one captured public book frame
  whose provenance its README gives; `fixtures/paradex/rest/` a hand-built `/orderbook` response
  in Paradex's documented shape. `fixtures/binance-usdm/` holds hand-written frames (`md/`) and REST
  responses (`rest/`) in Binance's documented shapes, its README citing the pages.
  `fixtures/licence-gate/` is a standalone two-crate workspace the licence gate's self-test
  runs against (0017); `fixtures/dep-direction/` a standalone workspace of empty crates in this
  layout, with three forbidden edges, that the dependency-direction check's self-test runs
  against. `fixtures/journal/` holds journals `fbc-journal` wrote in an older
  format version, from synthetic records, which later readers must still read; its README says
  how each was written.
- Docs: `docs/decisions/` (records, index in its `README.md`; cite by number, never restate one
  in a doc or a ticket), `docs/PRODUCT_BACKLOG.md` (stories), and the Beads queue in `.beads`
  (prefix `FBC`).
- What is private: nothing in this checkout. Strategy, parameters, calibrations, live configs,
  account data and credentials live with the private consumer or on the owner's machines, and
  never come here (0001, 0009). There is no UI here; the UI belongs to the consumer.

### Build, run, and restart

- Build and test: `cargo build --workspace` and `cargo test --workspace`. The toolchain is pinned in `rust-toolchain.toml` (Rust 1.97,
  edition 2024; 0008), so rustup installs the right compiler on first use.
- Run and restart: nothing. This is a library with no binary and no service; the private
  consumer runs it.
- Network tests reach the venues only through the runtime, with the SOCKS5 proxy the caller
  configures (0002); no test in the check touches a live venue.
- Release: the owner tags `vMAJOR.MINOR.PATCH` on `main`; the consumer pins that tag as a git
  dependency (`fbc-core = { git = "https://github.com/FueledByChai/fueledbychai-rs", tag =
  "v0.0.1" }`). The first tag, `v0.0.1`, follows the workspace skeleton and `fbc-core`'s first
  types. Nothing is published to crates.io (0008): every manifest, and `[workspace.package]`,
  sets `publish = false`, and `crates/fbc-core/tests/manifests.rs` fails a manifest without it.

### The check

`scripts/check.sh` is the definition of done, and CI runs the same script. In order it runs the
loop's checks (every loop script's `--self-test`, `scripts/prompt-check.sh`,
`scripts/decisions.sh --check`, `scripts/reference-check.sh`, `scripts/ruleset-check.sh` on
`ci/ruleset.json` and `.github/workflows/loop.yml`, `scripts/loop-kit-sync.sh --check`, and the
proof gate: a change under `crates/` brings a change under `crates/*/tests/` or `fixtures/`, a
`#[test]` or `#[cfg(test)]` line, or `No new test: <reason>` in the commit body), then the
privacy check (`scripts/privacy-check.sh --self-test`, then the scan for JWTs, bearer tokens,
key- and address-shaped hex, PEM headers and master-key bytes; 0009), then, once `Cargo.toml`
exists, `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
`cargo test --workspace`, `cargo build --workspace --release` and the credential placement
check (a venue crate's source outside `src/sign*` and `src/auth*` that names a JWT, bearer
token, API key, authorization header or private key fails; 0009), then the licence gate
(`scripts/licence-check.sh --self-test`, which proves the gate refuses the GPL-3.0-only path
crates in `fixtures/licence-gate`, a normal and a dev-only dependency, and names them, then
`cargo deny check licenses` on the workspace, dev-dependencies included, against `deny.toml`'s
permissive allowlist; 0017), then the dependency-direction check
(`scripts/check-deps.sh --self-test`, which proves the check fails naming each forbidden edge
in the `fixtures/dep-direction` workspace and refuses a workspace with no venue crate, then
`scripts/check-deps.sh` on the workspace: from `cargo metadata`, no crate outside
`crates/venues/` has a normal or build dependency on a venue crate, and a concrete venue crate
depends on no workspace crate but `fbc-core`; dev-dependencies are not checked; design §3),
then the coverage
ratchet (`scripts/coverage.sh`, workspace line coverage from cargo-llvm-cov, against
`coverage-floor.txt` with 0.2 points of slack; the workspace ticket records the first floor
with `scripts/coverage-ratchet.sh --set`). `scripts/check.sh --fast` skips the ratchet. On the
empty repository it takes about four minutes, almost all of it the loop's
self-tests. cargo-llvm-cov 0.9.1 is installed once by hand (`rustup component add
llvm-tools-preview`, `cargo install cargo-llvm-cov --version 0.9.1 --locked`); CI installs the
same version itself, and moving it is a ticket, like the toolchain. cargo-deny is required the
same way on every machine that runs the check, fast or full (0017): install it once by hand
with `cargo install cargo-deny --version 0.20.2 --locked`; CI installs the same version, the
licence step fails naming the install command when it is missing or another version, and
moving it is a ticket. The one planned step still marked TODO in the script is the
golden-refresh flag. It stays open now that `fbc-journal` exists: exact-replay goldens compare
outbound bytes, which need the exec path (BT-402).

### Rules

The owner's non-negotiables (0013, which supersedes 0010; the kill switch is 0012's):

- Never arm order entry except by an explicit call from the consumer: a new runtime starts
  disarmed, so a process restart leaves trading off until the owner presses Start. The one
  exception is 0013's rule 1, through 0012's Exit state: the owner's Flatten or Wind-down also
  arms a market after a restart, straight into exit-only order entry (reducing orders that
  never cross zero, under every cap), and never into quoting; only Start reaches quoting. A
  reconnect or resync never re-places or resends an order by itself and never changes the
  armed state; only a process start or an explicit disarm call leaves order entry disarmed.
  After a restart, open orders and positions are re-read from the venue.
- Always check the resting-order cap, the inventory cap and the per-market kill switch before
  every order command (every place, amend or replace, and batch item, reducing orders
  included), in the one path every such command takes (`fbc-oms`); never add a path that
  bypasses them, never classify an order as reducing to skip a check, and never let a cap or
  the kill switch block a cancel. What the kill switch blocks, which rules a cancel still
  obeys, and what lifting the switch does are decided in 0012; follow that record.
- Never mint a client id without a held namespace lease; this prevents client-id collisions,
  not two quoters.
- Always require a held market lease, plus the account lease when the venue's nonce scope is
  per account, before arming order entry for a market: one quoter per market and account
  (design §13.2). The consumer's check that no other process, Java or Rust, quotes the market
  on the account comes first.

Privacy and review (0001, 0009):

- Never put a private key, JWT, API key or secret in this repository, the Beads queue, a log,
  an error or the journal; the journal keeps authorization headers and JWTs only as keyed
  hashes and keeps order signatures (0006).
- Never put a real account address, sub-account id or balance here; fixtures are scrubbed or
  synthetic, and a directory of synthetic key- or address-shaped values carries `SYNTHETIC`.
- Never put live configs, parameters, spreads, sizes or calibration outputs here, or code that
  decides what to quote; a mechanism that needs a number takes it from the consumer's
  configuration.
- Always put a venue's signer in its `src/sign.rs` or `src/sign/`, and all code that builds,
  signs, refreshes, stores or injects a private key, JWT or API key in a crate's `src/auth.rs`
  or `src/auth/`; `exec.rs` and the runtime only call those modules, by names without the words
  the placement check looks for. This departs from design §6 step 6, which puts auth in
  `exec.rs`. A ticket that needs credential code anywhere else names that module `auth` or adds
  its path to `review_paths` in the same change (0009).
- Always leave changes to venue signers and to JWT and API-key handling
  (`crates/venues/*/src/sign*`, `crates/*/src/auth*`) for the owner's review; every other pull
  request auto-merges when green.

Design rules the records fix:

- Never let a venue crate own a socket, thread, queue or clock: codecs take bytes and return
  effects, and get time only from `EncodeCtx` or callback arguments (0002).
- Always send WebSocket and HTTP traffic through `fbc-runtime`, so the SOCKS5 proxy the
  consumer configures applies; never write a proxy address into code (0002).
- Never branch on a venue name outside the venue crates and the registry; a behaviour the
  capability model cannot express grows `VenueCaps` with a new mandatory field and a record
  (0003). Never give a capability type a `Default` or `#[non_exhaustive]`. Order and fill
  capabilities are declared together in `VenueCaps::exec` or not at all (0015); read them as
  `caps.exec.order.*` and `caps.exec.fills.*`, never as a fill claim of a market-data-only venue.
- Never create a `VenueOrderId`, `FillId` or `Fee` outside `DecodeScope`, and never take the
  absolute value or negation of a fee: a fee is positive when we paid it (0004).
- Never resend an order in `Unknown`; it counts as resting until the Unknown ladder resolves it,
  and filled quantity is the maximum of the venue's cumulative count and the deduplicated event
  sum, never their sum (0005).
- Never let the journal block a cancel, a reducing order, an ack or a fill (0006).
- Always use the pinned toolchain; moving it is a ticket of its own (0008).

### Docs to keep current

`README.md` (supported venues and status) when a venue or crate lands; this file's Layout when
a crate is added or moved; a venue's capability declarations with the document or fixture each
value cites (0003); a fixture directory's `SYNTHETIC` file when its values change; and a new
decision record, not an edit, when a decision changes.

<!-- BEGIN BEADS CODEX SETUP: generated by bd setup codex -->
## Beads Issue Tracker

Use Beads (`bd`) for durable task tracking in repositories that include it. Use the `beads` skill at `.agents/skills/beads/SKILL.md` (project install) or `~/.agents/skills/beads/SKILL.md` (global install) for Beads workflow guidance, then use the `bd` CLI for issue operations.

### Quick Reference

```bash
bd ready                # Find available work
bd show <id>            # View issue details
bd update <id> --claim  # Claim work
bd close <id>           # Complete work
bd prime                # Refresh Beads context
```

### Rules

- Use `bd` for all task tracking; do not create markdown TODO lists.
- Run `bd prime` when Beads context is missing or stale. Codex 0.129.0+ can load Beads context automatically through native hooks; use `/hooks` to inspect or toggle them.
- Keep persistent project memory in Beads via `bd remember`; do not create ad hoc memory files.

**Architecture in one line:** issues live in a local Dolt DB; sync uses `refs/dolt/data` on your git remote; `.beads/issues.jsonl` is a passive export. See https://github.com/gastownhall/beads/blob/main/docs/core-concepts/sync-concepts.md for details and anti-patterns.
<!-- END BEADS CODEX SETUP -->
