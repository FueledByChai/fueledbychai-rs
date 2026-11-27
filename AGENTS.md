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
  it; `scripts/sprint.sh add|remove|set` edits the Beads labels). A ticket leaves the queue
  without landing only as **dropped** (kit decision 0030): closed by
  `bd duplicate <id> --of <canonical>` when the same work was ticketed twice, or by
  `bd supersede <id> --with <replacement>` when another ticket carries the work (a fix that
  landed under its commit included), or, for an owner's decision that nothing carries, given the
  `loop:dropped` label (`bd update <id> --add-label loop:dropped`), which is the marker, and
  closed with the reason a label drop also needs (`bd close <id> --force --reason "<why>"`:
  `--force` because bd refuses to close a ticket that is still blocked, and bd keeps a closed
  ticket's first reason, so one already closed is reopened and closed again). Before dropping,
  remove any dependency the ticket already has on its replacement (a `blocks` or
  `discovered-from` edge, say), since `bd duplicate` and `bd supersede` refuse to add a link over
  one, re-point every ticket it blocks to the replacement (`bd dep remove <dependent> <id>`, then
  `bd dep add <dependent> <replacement>`; after a label drop, which nothing carries, only the
  remove), and give the replacement its `story:` labels; afterwards release its `ticket/<id>`
  branch and close any pull request it has. No agent drops a ticket another agent holds: it
  reports the ticket to that agent instead, and only that agent or the owner drops it. A dropped
  ticket is never done: it is never ready or offered, no omission from the sprint, and no part of
  a story's count, and a ticket carrying a drop marker that Beads has not closed is not offered
  either. A ticket Beads says is `closed` with no naming commit and no drop is a false close, and
  a landed commit naming a ticket Beads still has open
  is an orphan; `--reconcile` fails both, and every drop it cannot honour: a drop marker on a
  ticket Beads has not closed, a label drop with no reason (bd's placeholder `Closed` is none), a
  landed commit naming a ticket that carries a drop marker, a drop link to anything but a ticket
  in this queue that is not itself dropped, a ticket still to land blocked by a dropped one, and a
  story a link-dropped ticket serves that its replacement does not.
  These rules govern over the generic Beads guidance at the end of this file: a plain `bd close`
  is only for a ticket whose commit has landed, and `--next`, not `bd ready` (which takes any
  closed blocker as satisfied), names the work. Anything discovered while working goes in as a
  new ticket (`bd create`), not into the current one.
- **Merge profile.** A project's pull-request gates follow one of two profiles: staged/legacy
  (the project's existing handoff and gates under the named coordinator, including a staged
  rollout whose queue files are installed but whose controller is not live) or `github-v1` (the
  live App controller, kit decision 0026). The profile is staged/legacy unless the Project rules
  declare `github-v1` active on a line of its own, `Merge profile: github-v1`; a line
  `Merge profile: legacy` states the default, and a contract with no such line is staged/legacy.
  Activation and rollback are explicit edits to that line, made at the cutover and rollback in
  the queue runbook (`templates/queue-operations.md`, installed under `loop/templates/`) and
  recorded in the project's migration decision. Installed queue files, workflows, controller
  statuses or a running controller never change the profile (kit decision 0028). Reading it,
  the prompts stop before changing a gate only when the declaration is contradictory or
  unreadable (more than one `Merge profile:` line in the Project rules, a value other than these
  two, `github-v1` declared while the Project rules describe a legacy gate agents operate as
  active, or staged/legacy in force while they describe the App controller as the live merge
  actor); a check the operator alone arranges during migration or keeps for rollback is not a
  legacy gate agents operate, and the prompts' other stops still apply.
- **Claims and hand-off.** Before work starts, claim the ticket in Beads (`bd update <id>
  --claim`) and push `ticket/<id>` to origin with `scripts/open-ticket-pr.sh <id> --claim`;
  `backlog-status.sh --next` passes over claimed tickets, so several agents can hold several
  tickets. After the commit, `scripts/open-ticket-pr.sh <id>` pushes
  the branch and opens the pull request. When the Project rules declare `github-v1`, the live
  App controller, active, use `--draft`, verify auto-merge is disabled, then mark ready for
  review; retain any required human review for `review_paths`. In the staged/legacy profile,
  the default, preserve the project's complete existing handoff, including whether the PR opens
  as draft or ready and whether auto-merge is armed; copied queue files do not activate the
  controller.
  Several agents can implement at once. In the legacy profile, the named coordinator records
  merge order and selects one candidate in Beads (kit decision 0028). In `github-v1` (kit
  decision 0026), the App selects the candidate and owns CI admission, the merge gate and merging. Existing
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
  branch and the head commit subject name one claimed Beads ticket that carries no drop marker.
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

### Merge profile

Merge profile: legacy

This repository keeps the staged/legacy handoff, the default (kit decision 0028), because
pull requests are merged by hand, CI runs when the `ci:run` label is added to a ready pull
request (`.github/workflows/loop.yml`; a new head needs the label removed and added again), and no
merge-queue App runs here.

That handoff is the pull-request procedure the loop prompts follow here, and it requires
auto-merge disabled. Open each ticket's pull request as a draft with
`scripts/open-ticket-pr.sh <id> --draft`, which requests no auto-merge (a pull request touching
a review path still gets its `needs-review` label); verify auto-merge is disabled
(`gh pr view <number> --json autoMergeRequest` shows it null) before `gh pr ready <number>`
starts review; and keep it disabled through the final gate: never open the pull request without
`--draft`, and never run `gh pr merge --auto`. A green check merges nothing by itself. Agents
review and merge each pull request by hand (0149). Three agent roles take part, and no agent
holds two of them for one pull request. The rules after the three roles bind all of them:
- **The author** writes the pull request and answers its review. It names its session in the
  body (`Author session: <name>`), and never accepts or merges the pull request.
- **The reviewer** accepts or refuses it. It wrote no commit on it, and it is the named
  coordinator above, under `loop/prompts/review-prs.md`:
  - it claims the coordination epic and selects one candidate;
  - it adds `ci:run` only to the selected candidate, after accepting it;
  - it records the final gate and posts the `Agent review` status;
  - it never merges.
- **The merge agent** wrote no commit on the pull request and did not review it.
  - It takes only the pull request's number, changes nothing on the pull request, and waives
    nothing.
  - It runs `scripts/pr-readiness.sh` and `scripts/codex_review_evidence.py` from the default
    branch's tip.
  - It merges with `gh pr merge <number> --rebase --match-head-commit <head>` only once 0149's
    checks hold on that exact head, then records the merge on the epic.
- **Epic entries.** Every entry is an epic comment whose first line is
  `<KIND> #<number> <full head SHA> by <session>`, with the kinds 0149 lists.
- **The owner's word.** Only the owner waives a gate, approves a decision record, clears a pull
  request that waits for the owner, or directs an alternate external review (0127).
  - The session the owner said it to records it on the epic, and a relayed record is not the
    owner's word.
  - An owner review or waiver holds for the head it names only.
  - A waiver names one readiness fact on one head, and, for the `ci` fact, the one check run it
    waives. It never covers the external review, the acceptance, the required check's success
    on the head, the roles, or anything that waits for the owner below.
- **What waits for the owner.** These merge only once the owner has reviewed the current head and
  said to merge it, or the owner merges them:
  - a pull request touching a review path under Rules below (0009);
  - one changing a gate:
    - anything under `scripts/`, `loop/`, `ci/` or `.github/`;
    - `.loop.toml`, `loop.toml.example` or `deny.toml`;
    - the licence-gate or dep-direction fixtures;
    - a lowered `coverage-floor.txt`;
    - a `SYNTHETIC` file added, moved or removed;
    - this file outside its Layout section;
    - `CLAUDE.md`, `.claude/`, `.codex/`, `.cursor/`, `.agents/` or `.beads/hooks/`;
  - one touching `fixtures/*/exec-recorded/`.
- **Decision records.** A pull request adding or changing a decision record merges only once the
  owner has approved each changed record's text, as 0149 says.

### Layout

- A Cargo workspace (0001, 0008). `crates/fbc-core` exists, with time, units,
  price grids, exact prices, sealed ids, the client-id codec, `NamespaceLease`, `CidMint`,
  the `MarketLease` and `AccountLease` (0013; every fbc-core lease's drop unlocks before it
  closes, so a child spawned while one was held never keeps it from a retake, FBC-qtjn),
  `Fee`/`FeeBook`, `DecodeScope`, `InstrumentSpec` (maker-safe `quantize`; `SigningParams` and
  a clock-free `SessionCalendar`, 0125), instrument
  resolution (`AssetKey`, `AliasTable`, `InstrumentResolver`, `InstrumentSpecDraft`; 0035),
  `VenueCaps`, the market-data and execution events, venue commands, and the codec, signer,
  nonce and factory traits with `Effects` and `EncodeCtx` (the factory's discovery and
  `test_connection` each an `HttpPlan`, whose rounds may name the next, built from an `EncodeCtx` when sent, 0048), credentials as `Secrets` in `src/auth.rs` (0043), and the write-only `PathStamps` a codec marks
  its latency stages through (0034), `crates/fbc-book` with its L2 book, `crates/fbc-sim` with its
  queue-position fill model (0038) and SimVenue (0046), `crates/fbc-journal` with its record
  format, day-grouped segment writer rolled hourly with zstd-compressed closed segments, in-order reader and never-blocking sink, `crates/fbc-runtime` with its first
  slices (the connector, SOCKS5 CONNECT, WebSocket and HTTP/1.1, plain or TLS, 0019 and 0020, the WebSocket handshake its own, 0029;
  connection epochs and the subscription reconciler; the market-data session, 0023; HTTP
  effects, poll endpoints and plans, 0027; journaling and decoder replay, 0006; rate-limit buckets with a safety reserve, 0030), `crates/fbc-conformance` with its stub venue server (0025),
  `crates/fbc-oms` with its first slices (the order lattice, item outcomes and the registry by
  client id; the fill ledger, the two fill counters and the inventory; 0005, property-tested
  with proptest, 0037; the gateway traits and the authorization order entry needs, 0045;
  the execution planner, 0065; terminal-order retention, 0157),
  `crates/venues/fbc-venue-paradex` with its signer (`src/sign`), SBE market data (`src/md`: bbo,
  trades and the order book), its order-entry capabilities (`src/exec`, 0054) and its factory
  (`src/factory.rs`, market data and order entry, 0072),
  `crates/venues/fbc-venue-binance-usdm` with its market-data-only factory and codec,
  `crates/venues/fbc-venue-qfex` with its read-only public MDS factory and codec (0117) and,
  under `qfex.exec.mode` `preprod_probe` or `production_probe` alone, a host-guarded probe's
  order entry (`src/probe.rs`: each mode names its `Environment` and admits exactly that
  environment's three origins, QFEX's pre-production ones under `preprod_probe` (0132 item 6)
  and its production ones under `production_probe` (0143 item 2), or all three loopback,
  refusing the other environment's domain; its caps in `src/exec/caps.rs`, alike in both,
  account-wide cancel-on-disconnect offering no disarm, which `caps_truthful` holds it to in
  `tests/conformance.rs`, 0141; under `production_probe` alone its codec refusing every place,
  batch place and amend unsent, `NotSent(Unsupported)` before the epoch is read (FBC-5d7x,
  0145), unless `qfex.exec.production_places` is `probe_place_stages`, under which it encodes a
  single place only as a post-only GTC or a limit IOC at exactly the instrument's `min_size`
  (`exec::order::staged`) and refuses every other place, batch place and amend `Unsupported`
  unsent, one below the minimum `Unencodable` as in every mode, the key under any other mode or
  none a configuration error naming the key, never its value (FBC-qnv1, 0148 item 4); so
  `caps_truthful` there breaches exactly the placement control and the post-only IOC conflict,
  with the key or without (`tests/probe_mode.rs`, `tests/probe_resync.rs`,
  `tests/conformance.rs`); in either mode (FBC-jxau,
  0143 item 4) the arm answered by its own echo alone (`src/exec/echo.rs`, `arm_echo`: an ack
  accepting it at Final, as FBC-291a's capture shows, an err rejecting it unless `RateLimited`,
  which holds every frame budget for its kept hint and leaves the arm to its deadline, 0132 item
  1, 0147), the private frames decoded into order updates, fills and positions with the epoch kept
  open, a frame on a symbol the spec table does not hold counted and pushing nothing
  (`src/exec/private.rs`, 0147 item 2), a cancel settled by the `order_response` naming its order,
  `CANCELLED` accepting and `NO_SUCH_ORDER` rejecting `NotFound`, an err settling nothing and every
  place ending `Unknown` (0132 item 8), and item 5's resync (`src/exec/resync.rs`): a signed
  `GET /user/positions`, the `get_user_orders` pages deduplicated by order id until a short page,
  a full page adding no new id or its own deadline reconnecting, `get_user_trades` from the start
  time for the orders it placed as replayed fills, one bracket that seeds no position; FBC-zeb0's
  rule that a private state frame ends the epoch holding only for the recording-only codec
  (`tests/probe_settle.rs`, `tests/probe_resync.rs`, sharing `tests/probe_harness/`, with fbc-oms
  a dev-dependency); its REST reads' headers
  from `src/auth`, `TradeAuth::rest_headers`) and the probe's own account reads (`src/account_reads.rs`:
  `GET /user/positions` and a `get_user_orders` page after that account's authentication, and
  `GET /user/subaccounts`, which the probe no longer sends (0144), each built only under the guard
  and signed by `src/auth`'s `ReadAuth` for an account and credentials the caller names, and
  decoded exactly; 0132 item 7, FBC-h3z1; for item 4's drop test and the report, a
  `get_user_trades` page on a socket that never arms, its frame and charge the resync's, read in
  either envelope the resync reads (`user_trades` beside a whole `count`, or
  `user_trades_response`, a whole `count` optional) and the envelope kept, each row's fields
  read where it carries them and its member names kept, a row missing one read and not refused,
  and a frame that is not exactly one envelope holding an array of objects, with no other member
  but its `count`, refused, as an err or an ack is, and a signed
  `GET /user/historic-orders` page, the requested account named only for a subaccount, its
  `client_order_id` and `terminal_status` decoded exactly, both only under the guard (0132 items
  4 and 5, FBC-be57); and, for the probe's liveness hold, the three
  subscriptions a session sends, built and charged by the codec's own `exec::subscription` and
  only under the guard, a `{"subscribed":…}` confirmation read where the codec reads one
  (`exec::channel_reply`) and an err read as a page reads one, `SocketErr`, each from the bytes
  the journal keeps; 0146, FBC-39ks), its trade session's liveness on server pings alone held in
  `tests/liveness.rs` (an `ExecSession` under `preprod_probe`, and again under
  `production_probe`, against the conformance stub on a paused clock: its arm accepted and its
  resync answered before the first ping, then each ping
  answered, no frame of its own; FBC-39ks), and its codec sending no client keepalive, since the
  owner's hour-long hold on QFEX production showed server pings about every 10 s after
  authentication and no server close (for production only; FBC-sbbl, 0146 item 8;
  `fixtures/qfex/exec-recorded/hold-production/`, held by `tests/hold_production.rs`); and,
  apart from the probe, its clustered-move benchmark (`tests/clustered_move.rs`, FBC-d8uk,
  measured as Paradex's FBC-vgm: fbc-runtime's shard host runs this crate's MDS `bbo` from a
  reference server of the test's own (tokio's `net`, `tokio-tungstenite` and `futures-util` as
  dev-dependencies) beside an unchanged `ExecSession` on the codec under `preprod_probe`, every
  origin loopback, against the conformance stub, each protective cancel written behind at most
  one place encoded after its frame arrived; on Linux, with no journal set, as Paradex's runs,
  it reports their kernel-receive-to-write-done p50, p99 and max, and on every platform their
  ingest stamp to write done, off Linux read from the session's journal and labelled so),
  `crates/venues/fbc-venue-hibachi` with its market-data-only factory, discovery, caps,
  `/ws/market` codec (0129) and status poll (0130) and its offline signer (`src/sign`, built
  from `Secrets` by `src/auth`), `crates/fbc-live` with its link, its classifier of cancels not
  ours, its sizing, its sweep, its positions comparison and its stream check (`src/link.rs`,
  `src/not_ours.rs`, `src/sizing.rs`, `src/sweep.rs`, `src/positions.rs`, `src/reconcile.rs`, the
  samples' glue moved there, 0155), and
  `crates/fbc-examples` with the owner-run samples; the rest are planned.
  Members are the `crates/fbc-*` and `crates/venues/fbc-*` globs, and `fixtures/` is excluded
  from the workspace:
  - `crates/fbc-core`: the contract. Time, units, price grids and exact prices, sealed ids and
    the canonical client-id codec, `Fee`/`FeeBook`, `InstrumentSpec`, `VenueCaps`, events,
    commands, the codec and factory traits, `DecodeScope` and `EncodeCtx` (0003, 0004), and
    `PathStamps`, whose marks reach the runtime's `PathRecorder` and give back no time (0034).
    It re-exports `rust_decimal::Decimal`, which its API hands out, so a crate depending on it
    names the type with no dependency of its own (0155 item 1).
    A spec states `signing: Option<SigningParams>` (`AssetDecimals { underlying, settlement }`,
    its contract id kept in `native_id`) and `sessions: Option<SessionCalendar>`
    (`src/calendar.rs`: an earliest open, a close that never reopens, dated `ClosedWindow`s (a
    venue's published maintenance windows, Hibachi's from discovery, FBC-ord5) and
    `WeeklyClosure`s in a zone's wall time under a POSIX-style `UtcOffsetRule`, evaluated by
    integer civil-date arithmetic with no zone data and no clock; `is_open(at)` only ever
    closes, fail-closed across daylight-saving edges, and `next_closed(from)` gives the first
    instant at or after `from` it closes, never an occurrence a skipped hour swallows whole, so
    a codec can ask for a timer at a session's end, 0130; `None` where it closes nothing),
    copied from the draft by the resolver, neither in `version` (0125). It states
    `order_shapes: Option<OrderShapes>` too (`kinds: TagSet<OrderKindTag>` for orders that are
    not post-only, `post_only` for the post-only form, `tifs: TagSet<TifTag>`, each as the
    venue lists it for the instrument, and `OrderShapes::lists(kind, post_only, tif)`, which
    only ever narrows the venue-wide `OrderCaps`; `None` where the venue lists none per
    instrument, which a venue that lists them may read as nothing listed), copied by the
    resolver and not in `version` (0136).
    `MdEvent::Status { inst, status }` is an instrument's `TradingStatus` as the venue's market
    data shows it, reported each time it changes, a halt on the venue's schedule included: no
    book event and no feed's health, named in every match on `MdEvent` with a fallback arm, and
    what the consumer passes `PlanVenue::status` once it arrives in place of the spec's
    discovery snapshot; `MdCaps::status: FeedSource` says where it comes from, mandatory like
    every capability (`None` where the adapter reports none), and the conformance kit's
    market-data checks breach a status from a venue that declares `None` (0130).
    A `FillEvent` states its `origin: FillOrigin` once: an order's execution (`Order`, its
    client id and grid price) or a change the venue made itself (`Venue`, a `VenueFillKind`:
    liquidation, transfer, settlement or block trade, with no client id and the exact
    `PxExact` price the venue states; 0120).
    `VenueCaps` keeps order and fill capabilities together in `exec: Option<ExecCaps>`,
    `None` for a market-data-only venue, which declares no fill source or fee sign (0015).
    Each `CancelOnDisconnect` variant that takes an arm (`PerConnection`,
    `AccountWide { offers_disarm }`, `DeadMan`) states `offers_disarm`, whether the venue
    offers a disarm, `ArmCancelOnDisconnect(false)`; `None` takes neither (0141).
    Every codec names the credentials in what it receives (`redact_inbound`, `InboundSpans`;
    0028) so the journal keeps them only as keyed hashes. Credentials reach `exec_codec` and
    `test_connection` only as `Secrets` (`src/auth.rs`, a review path): no `Clone`, a `Debug`
    and `Display` that show no value, zeroed on drop with `zeroize` pinned (0043).
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
    `BookId`), routing each book event by its channel (a one-side prune or clear too, each
    feeding its book for its epoch's end, 0124; a trading status changes and feeds none, 0130),
    and exposes each instrument's trading
    book as the consumer's `TradingBooks` configures it (FBC-nij); a session tells its handler
    each connection epoch's end (`MdHandler::on_epoch_end`, live and in replay), and every book
    that epoch fed is then gapped until its next snapshot (0039);
    a `Journal` (the consumer's `fbc-journal` sink) set on a session or venue records every input,
    output and connection change under its traffic class and is never waited on (0006); given
    the `CancelOnlyTrip` of each registry whose order entry runs on the shard
    (`Journal::cancel_only_at_soft_limit`), it moves every market of them in Quoting to
    Cancel-only, leaving the owner's Exit and its reducing orders as they are, each time the
    sink drops one of its records from under the soft limit or into a gap other than the one it
    reached it in (`JournalSink::open_gap`), and journals `Marker::CancelOnly` under Safety, from
    the reserve, right after it (0139, `tests/journal_soft_limit.rs`);
    the `log` facade's TRACE level compiled out of every build that links it, since tungstenite
    logs each frame at TRACE (0079);
    `RateLimiter` keeps a sliding-window bucket per declared limit and scope key, charged by
    each frame's, request's and connection attempt's rate charge, normal traffic stopping at
    the consumer's `SafetyReserve`, refusals and 429/418 counted by scope (0030);
    `MdReplay` feeds one session's journal back to the venue's decoders through the same calls,
    with the recorded stamps, effects not executed (decoder replay; the caller supplies the
    `VenueConfig` and `SpecTable`), and runs closed-loop as a shard member (0107,
    `Member::md_replay` with a `ClosedLoop`): each record it feeds waits for its recorded instant
    on an `IngestClock::replayed` (no wall clock read; the consumer's runtime paused, so time is
    virtual), each input restamped with the shard's next ingest sequence and its recorded
    instant, so the host's passes, `Cycle` records and the sessions' answers run among the
    records as live, then it ends the sessions after a settle time;
    `ExecReplay` feeds an order-entry session's journal back as `MdReplay`'s decoder replay
    does, not closed-loop (FBC-tgt, 0138): one codec of its venue across the journal's epochs,
    each `on_open`, codec timer, resync and encode given the context the journal holds where it
    places it, the session's own cancel-on-disconnect arm where the live session's gate sends it
    (sent where its frame is journaled next) and a consumer's command as the caller supplies it
    by request id, frames, HTTP results and request deadlines at their recorded stamps, each
    event handed on through the live session's own call, effects not executed, and a control the
    replayed handler drops where it dropped its session's, after which it is handed nothing; a
    journal it replays holds one order-entry session's records, since a context names no
    connection (another's requests counted) (`tests/exec_replay.rs`); and its exact replay
    (FBC-qwq, 0006, 0142), when its configuration asks for it (`ExecReplayConfig::compare_outbound`;
    off, a decoder replay keeps nothing of it), compares every frame and HTTP request the codec
    asks for with those the journal holds of the session, outside their redaction spans (their
    places and lengths compared, a secret header's value blank at its length), signatures
    included: each journaled one matched on its connection epoch, a frame in the order asked
    for, a request wherever it lies, or reported a difference with what was asked for in its place
    (a request's under its tag), which a later record may still match, what was asked for and
    never journaled counted unwritten, and a difference or an unwritten one a `Degraded` span may
    explain, the sink's own numbering placing it, excluded and counted (`ExecReplay::exact`,
    `ExactReport`, exact when nothing differs and nothing is unwritten, `is_exact`; a difference
    a gap explains found after its marker, and a
    frame one endpoint asks for on another's stream, read as differences, as `ExactReport` says),
    held to a conformance-toy session recorded afresh on every run and to the committed golden in
    `fixtures/conformance-toy/exact_replay/` (`tests/exact_replay.rs`, a signature alone differing
    included) and to hand-built and sink-written records (`tests/exec_replay.rs`);
    every connection reads through `Tcp`, which on Linux keeps the `SO_TIMESTAMPNS` kernel
    receive time beneath TLS and WebSocket (nix, Linux only), so a frame's stamp carries
    `kernel_rx`, and a session reports each Safety write attributed to such a frame, the
    codec's or one the handler issues through its `Outbox`, as a `TickToWire` (0031);
    `ExecSession` drives one account's order-entry connections as `plan_exec` plans them (0053;
    the first carries the orders, each other on its own connection number, epochs and pacing,
    the codec's frames and reconnects routed to the stream they name, 0115):
    one `ExecCodec` across its epochs, `on_open` given exactly the nonces it asks for from the
    consumer's `NonceSource`, frames decoded inside `dispatch`, events stamped and handed to the
    consumer's `ExecHandler` inline, a stop ending the epoch at once, reconnects paced; the
    codec's HTTP requests and timers answered only to the epoch that asked, `on_timer` given
    exactly the nonces it asks for, and its keepalive its own timer (0056); its `ExecOrders`
    reads now on the session's shard clock as the session does (`ExecOrders::now`) and says
    whether another session stamps on the same clock (`ExecOrders::shares_clock`), 0155 item 3,
    unit-tested in `src/exec_orders.rs`; commands reach its
    codec only through its `ExecOrders`, as an fbc-oms `Authorization` for its account or a
    `ControlCommand` (an authorization taken over at its turn or as it is refused for want of
    room, and dropped unspent, so released, when refused for another account or an ended session
    or left waiting at the session's end; 0152, `tests/released_authorization.rs`), each encoded
    on its next turn with one reserved nonce per item and reported at most once to
    `ExecHandler::on_submitted` (sent, or `NotSent` with nothing written: no authenticated epoch,
    the codec's refusal, effects that do not carry the request,
    make an HTTP request (except a consumer's order query or fee query carried as exactly one
    HTTP request and no frame, its request and the connection it opens charged with the encode,
    0081) or give it a deadline past the end of the clock, the buckets: every
    control command, like a place or amend, stops at the safety reserve, while cancels,
    reducing orders, the session's own cancel-on-disconnect arm and the resync may use it,
    0073; the commands waiting for the connection past the consumer's `outbound`
    `ReservedBound` (a Normal bound plus a reserve only cancels and reducing orders use)
    refused at once, `SubmitRefusal::Backpressure { rpc }`, nothing kept, the refusal journaled
    as its write result under `rpc` and settled by the consumer as a not-sent receipt for it,
    and a command past its class's room in the consumer's `InFlight` table of requests in
    flight, shared per venue or per connection, `NotSent(RateBudget)`, its places given back
    when a run ends or its future is dropped, 0105), an
    authorization kept until its command is encoded, request ids from the account's `RpcIds`
    shared by its sessions, and each unanswered request handed to `on_rpc_timeout` once at its
    deadline, across a reconnect too, never written again (an HTTP query's deadline standing
    only once the epoch that asked has ended, and cleared by the `on_http` call for it, 0081)
    (0057; compile-fail cases in
    `tests/ui_submit/`) (its tests include the conformance toy by path); each Safety write it
    makes attributed to an input with a `kernel_rx`, a cancel a market-data handler of the shard
    submits on a frame included, reported to `ExecHandler::on_tick_to_wire` as a `TickToWire`
    (FBC-qfm, `tests/protective_cancel_tick_to_wire.rs`); with a `Journal` set,
    it journals what it sends and receives as a market-data session does, each epoch's opening
    and closing, each nonce it reserves and each context it gives its codec, what its stream
    brings under Safety (0078), and each request deadline it fires at its stamp (0090); it
    reserves through `NonceSource::try_reserve`, and a source's refusal ends it with
    `ExecSessionError::NonceRefused`, nothing reserved or written;
    an epoch whose resync's `ResyncEnd` has not reached the handler by the consumer's
    `ResyncDeadline` (no default, zero refused) after the session asked it ends as a drop,
    counted (`ExecCounters::resync_timeouts`), once the commands waiting have gone out, and
    reconnects through the pacing; its firing calls no codec, so the journal holds only the
    epoch's `Closed`, where decoder replay ends it (FBC-nwpr, 0013, 0053, 0058;
    `tests/exec_ready.rs`, `tests/exec_held.rs`, `tests/exec_replay.rs`);
    an account-wide cancel-on-disconnect cap requires explicit authority for that account
    before a session plans or opens its endpoints; once armed, a disconnect cancels that
    account's orders across connections, including orders placed after the arm, and each new
    epoch must arm again (0121);
    a venue whose configuration names the consumer account it is bound to
    (`VenueFactory::exec_account_key`, `Ok(None)` by default) refuses a session for any other
    account, `ExecSessionError::AccountBinding`, before caps or planning (0122);
    `WallClockNonces` is the production nonce source for a venue whose nonce is a wall-clock
    timestamp (Hibachi): `max(clock, last + 1)` microseconds over an injected `WallClock`,
    refusing a block further ahead of its clock than the caller's window, which has no default
    (FBC-3bq4, 0114), and issuing only at or below a ceiling its `NonceLease`'s `LeaseStore`
    (`LeaseFile` in production, locked against a second holder until a dropped source's last
    write has returned) has already persisted, none more than the window past its clock: the
    first when it is built, the next by a writer thread of its own once half the consumer's span
    is left, a block past it refused
    (`PastLease`) and never waited for; resumed after the larger of the lease's ceiling and a
    journal's `last_nonce` (refused, `JournalGap`, for records holding a gap), so a restart
    never reissues a nonce sent while its journal record was unwritten, whatever its clock reads
    (FBC-ak22, 0133, `tests/wall_nonces.rs`, `tests/exec_journal.rs`);
    `ShardHost` is the shard host (0100): one loop driving a shard's session runs as its
    `Member`s on one task, sharing one `IngestClock`, that drains every input they have ready
    (yield to the runtime, poll every member, again while a round spent the task's coop
    budget, until a round stamps nothing), journals a `Cycle`
    record under Normal with the instruments the consumer marked through `Changed`, runs the
    consumer's `Decide` pass, and submits the `Ladder` of authorizations and control commands it
    decides one at a time, each once every order-entry session holds nothing, with a yield and
    a poll of every member between two, so what a handler submits meanwhile goes first and every
    remaining command is re-checked at its encode; a submission made while a handler is handed
    an input's events is attributed to that input, on whichever session; a ladder command never
    submitted, or withdrawn unencoded when the run or its session ends, is handed back
    (`Decide::unsubmitted`); a member's error ends the run and every dropped member's orders
    (`tests/shard_host.rs`);
    `ConsumerInputs` carries the consumer's own inputs into the shard (0103): a bounded ring
    (`InputSender`, `Send` and `Sync`, from the consumer's capacity with no default) its other
    threads push opaque operator commands and reloads into, which refuses a push when full
    (counted) and never blocks, and the `ConsumerTimers` its handlers arm on the shard's thread;
    run as a `Member::inputs`, it stamps each input and each timer firing with the shard clock's
    next ingest sequence, journals it as `Record::Consumer { input: true, ingest_seq: Some(_) }`
    under its class and then hands it to the `InputHandler`; `MdReplay::set_inputs` hands each
    back at its place, the replay's handlers arming through `ConsumerTimers::inert`
    (`tests/consumer_inputs.rs`);
    `ExecSession::in_process` runs an order-entry session over an `InProcessStream` in place of
    the venue's planned WebSocket, with a codec of the consumer's (0104): no socket opens, the
    stream's `OrderEntryPeer` is told its opening and closing, each frame the codec writes and,
    through a `PeerFeed` in front of the consumer's market-data handler, the shard's envelopes in
    ingest order and each market-data epoch's end, and its answers are read at their due
    instants on the shard clock, stamped and journaled as a socket's; the session core is
    generic over what an epoch writes and reads on (`Wire`) and how an attempt opens it
    (`Dial`) (SimVenue's peer is tested in `fbc-sim`'s `tests/hosted.rs`);
    `tests/common/toy.rs` is the toy market-data venue later runtime tests reuse (and
    `fbc-sim`'s, by path).
  - `crates/fbc-book` (it depends on `fbc-core` only: one tick-indexed L2 book per
    instrument and `BookId`, built from book events the same live and in replay, with
    snapshots, deltas, windows, one-side prunes and clears and gap invalidation (`SidePrune`
    and `SideClear` keep no state and act where a delta would, a cleared side unknown with no
    touch on a valid book, 0124), touch, top-n, one level's size where it is
    known (inside the window, or no deeper than a capped book shows), an exact
    top-n comparison and canonical bytes; still planned: the ex-own projection, touch arbitration and
    continuity policies), `crates/fbc-oms` (it depends on `fbc-core` only, `proptest` as a
    dev-dependency, 0037: so far `OrderRecord`, whose ranked states, PendingNew and Unknown 0,
    Open 1, PartiallyFilled 2, Terminal 3, only move forward with a terminal state absorbing,
    `apply_update` under an `OrderKey` (the venue's ordering key, ingest tiebreak), amends that
    issue new venue ids and the superseded ids they leave, `on_outcome` per command item, and
    the `Registry` by client id that routes each update by client id or any venue id the order
    had, while it holds the order (0157); the `FillLedger` keyed by `FillEvent::key()` and
    bounded by an age and a count from
    the consumer's configuration, whose replayed fills apply only when absent, newer than the
    session-start watermark and newer than its retention horizon, and the `AcceptedFill` it
    hands `Registry::apply_fill`, the one path that moves an order's `cum_fills` and the
    inventory (filled is `max(cum_venue, cum_fills)`, never their sum); I1 for order updates
    and I2 property-tested in `tests/lattice.rs`, I1 with fills and I3 in `tests/fills.rs`;
    the gateway traits
    `OrderGateway` and `ManagedGateway`, moved here from `fbc-core`, whose `submit` takes an
    `Authorization` only `fbc-oms` issues, for one order-affecting command on one market with
    that market's `StateGeneration`, not `Clone` and consumed on submit, issued only by the
    registry that built the command, for the one account that registry is for, a place, batch
    or amend judged against the caps again at issue and released when refused, and refused at
    submit (`StaleAuthorization::PositionMoved`) once a fill or a resync moved its market's
    position since its issue (since its build, in Exit, under an orphan cooldown or under 0158's
    refusal; 0066, 0108,
    0153; `tests/quoting_submit.rs`), a place built
    and not yet authorized held by its command: withdrawn with it, or released by the
    registry's next mutating call once it is dropped unspent, with no outcome of it by client
    id applied and an accepted or unknown one voiding the command (0082, 0084;
    `tests/retained.rs`, `tests/held.rs`), and an issued place's, batch's or amend's
    authorization holding its orders or the amend's build until a gateway takes it over
    (`Authorization::hand_over`, whose `Handed` alone runs 0060's check at submit, so a gateway
    keeping 0060 writes only a handed one; `Authorization::command()` stays readable, and a
    gateway skipping the check could write from it): dropped before then, released by the
    registry's next mutating call, or at once by `Registry::withdraw`, an order something was
    reported for since kept (0152; `tests/dropped_authorization.rs`), and whose
    `submit_control` takes a `ControlCommand` that carries no order-affecting command (0045,
    compile-fail cases in `tests/ui_authorization/`); the `Live` and `Cancellable` permits
    the registry gives, through which alone an amend or a cancel is built, each cancel naming
    its order by design §4.9's reference from the venue's caps or waiting for the
    acknowledgement, a cancel-many batching only the items its batch declares a reference for,
    and no permit for a foreign-namespace or non-canonical order (I4; `tests/permits.rs`,
    compile-fail cases in `tests/ui_permits/`); the Unknown ladder (I5, I9; `src/ladder.rs`,
    `tests/ladder.rs`), run on the consumer's timer with its `LadderConfig`: a command past the
    intent timeout escalates, an order on the ladder is queried once by a declared reference,
    resolved by the answer or a resync, Lost after the configured trustworthy snapshots past its
    sent time plus the settle time (never less than the venue's declared snapshot lag bound,
    decision 0109), tombstone-cancelled by client id after the maximum, and
    never placed or amended again; resyncs (0055; `src/resync.rs`, `tests/resync.rs`): the
    first seeds each market's position once and registers our open orders it shows, the
    position unknown until then, a fill straddling the snapshot counted once, later ones
    compared and reported, never overwriting the inventory, and none seeding a market while its
    snapshot shows an open order with no client id under a venue id no order the registry holds
    had, a forgotten order of ours included (0157), each such order reported
    (`ResyncReport::without_cid`, 0154), and each order not ours that held a market and that it
    shows gone named (`ResyncReport::not_ours_lifted`); while an order not ours (another
    namespace's, non-canonical, or with no client id, under a venue id no order the registry holds
    had or one of an order it holds as ended Lost or never sent) that an order event or a snapshot
    row showed open rests on a market, a place, batch item or amend there refused
    (`OmsError::NotOursResting`, `AmendRefusal::NotOursResting`; at authorization
    `IssueRefusal::NotOursResting`) unless on the side that reduces the known, non-zero position and
    within its size or carrying the venue's reduce-only flag, at build, at authorization and, once
    authorized before the sighting, at submit (`StaleAuthorization::NotOursResting`), never a cancel
    or an order Exit admits, lifted by its end, a trustworthy resync not listing it open, requested
    at least the lag bound after the first order-entry epoch end or retention pass reported after it
    was last shown, never earlier than the latest end already reported, or its venue id held as an
    order of ours not ended Lost or never sent (0158;
    `tests/not_ours_resting.rs`; named by `Registry::not_ours_shown` as a `NotOursShown`); market
    states and arming (0012;
    `src/entry.rs`, `tests/states.rs`): each market armed or not and Killed, Cancel-only, Exit
    or Quoting, a fresh registry disarmed and Cancel-only, the state checked before the caps,
    Start, Flatten and Wind-down arming with the market lease (and the account lease where
    nonces are per account) and refused while Killed or the position is unknown, and for a
    market seeded by hand (`SeededByHand`, 0067; `tests/hand_seed.rs`) unless the registry is
    a declared `TestnetRun` and no probe's, or the seed, as seeded, is flat on the one market a
    declared `CodProbeRun` names (`CodProbeRun::owner_supervised(market)`, given to
    `Registry::for_cod_probe`, which keeps the first it is given, neither `Clone` nor `Copy`;
    0140 item 6; `tests/cod_probe.rs`, compile-fail cases in `tests/ui_cod_probe/`), on which
    market Start, Flatten and Wind-down arm after a fill moved the position off that flat seed,
    but under the place stages' `CodProbeRun::owner_supervised_places(market)` Start arms only
    while the position is flat (`SeededByHand` otherwise), a fill leaving a Quoting market
    Quoting, and Flatten and Wind-down still reach Exit (0148 item 3, the owner keeping Start's
    rule QFEX-only; `tests/cod_probe.rs`), every change advancing the market's
    `StateGeneration`; the
    registry's `CancelOnlyTrip`, which the shard's journal trips at its soft limit, moving
    every market in Quoting to Cancel-only, its armed flag unchanged, read at once and applied
    at the registry's next mutating call, and a place, batch or amend authorized in Quoting
    before it refused at submit
    (`StaleAuthorization::CancelOnly`), while a market in Exit stays in Exit, its reducing
    orders built and sent (0139, `tests/cancel_only_trip.rs`); cancels and fills in
    every state (0012;
    `src/sweep.rs`, `tests/sweep.rs`): the kill switch's cancel everything an instrument
    cancel-all only under the held market lease after a trustworthy resync with no order not
    ours in view, the resync requested after the latest order-entry epoch end the consumer
    reports (`Registry::order_entry_epoch_ended`, at the `recv_mono` of the last envelope
    before it, as fbc-live's link does; the orphan escalation alike; FBC-9svb),
    otherwise our orders cancelled by explicit reference, named again on each
    resync while Killed, never an account cancel-all, and own fills counted once in every state
    while foreign ones are flagged; venue-initiated fills (0120; `tests/venue_fills.rs`): a
    `Venue`-origin fill counted once on the inventory through the `FillLedger` in every state
    (`FillRouted::Venue`), on no order, putting nothing in view and arming nothing, placed
    against the seed only by its arrival or time and counted after a snapshot seed unless
    they place it there; Exit's admission (0012, 0063; `tests/exit.rs`): only reduce-only or
    reducing-classified orders on the side that reduces the position, the position plus that
    side's orders never crossing zero, every cap still applied, nothing once flat; the
    `ExecutionPlanner` (0005, 0065, 0068; `src/planner.rs`, `tests/planner.rs`): the
    consumer's `DesiredBook` diffed per side and level against the orders it placed for that
    account (each account's apart, planned through the one registry the account is bound to),
    under the consumer's `PlannerConfig` (basis-point and lot replace thresholds, a minimum
    age), a change an amend where the venue's `OrderCaps` admit it, and otherwise, or where 0158's
    refusal would refuse the order as it rests, a cancel
    then, once the old order is terminal, a place, a PendingNew, Unknown or in-flight order
    occupying its level, each slot pinning its order through a handle until the planner frees
    the slot or is dropped (0157), every command built through the one pre-trade path and the
    permits and emitted as an
    `Authorization`, cancels then reducing orders then amends then adds, in Exit every order
    on a side that does not reduce the position cancelled, each level's opaque consumer tag
    copied onto its places and amends and each side's optional removal reason onto the cancels
    of its levels no longer wanted, both kept on the order and carried by the audit sink's
    verdicts (0098; `tests/tags.rs`), each quote first fitted to the venue's `OrderCaps` by
    generic fallbacks reported as `Planned::downgrades` (RPI to public, a synthetic IOC
    cancelled at once, emulated or kept reduce-only per the quote's `ReduceFallback`), an
    emulated reduce-only order kept within what is left to reduce and cancelled on overshoot
    at each pass and by `ExecutionPlanner::on_fill` (0106; `tests/fallbacks.rs`), each side's
    price-capped `CrossingIntent` planned as one reduce-only order at its cap (IOC where the
    caps take it, else a synthetic IOC), sized so the position never crosses zero, through
    every state and cap, once per consumer attempt (0110; `tests/crossing.rs`), and each pass
    taking the consumer's `PlanVenue` (the caps, account and market venue modes, trading
    status, a `RateView` of each rate scope's room and floor, and a `Batching` choice): no
    place or amend a status or mode refuses, cancels in every mode, normal commands withheld at
    each scope's floor while cancels and reducing orders go out, and under
    `Batching::Declared` places of one stage, never a crossing intent nor, under 0158's refusal, a
    place carrying the venue's flag, and cancels batched within the venue's declared
    sizes and references (0111; `src/gates.rs`, `tests/gates.rs`); terminal-order retention
    (0157; `src/retain.rs`, `tests/retention.rs`, compile-fail cases in `tests/ui_retention/`):
    `RetentionConfig`, with no `Default`, refusing zero for each of its count, age and resync
    bound, and `Registry::retain_ended` at the consumer's `now`, the one call that forgets an
    order: each terminal record stamped at the first pass that sees it, kept while a pin holds it
    (ended Lost or by a tombstone, owing a fill or an answer, named by a command, an
    authorization or a planner slot through its handle, on a market not seeded, inserted after a
    resync's seed, or sharing a venue id with another order held), each counted by reason in
    `Retained`, and while it settles, until the venue's snapshot lag bound plus the resync bound
    after its stamp; of the others, each at least the age old forgotten, then the oldest while
    more than the count remain, each removed in one step with every index entry naming it, its venue
    ids, unless it ended never sent, kept as ended among the orders not ours for one more such floor
    (0158); and
    per namespace a built span and an answer span of what it forgot, a late fill under a client
    id the first covers placed as its held record's would be, an outcome or a sent report under
    one the second covers answered as a terminal record's, and a place or an insert under one
    the first covers refused `DuplicateCid`; 0005, 0013), `crates/fbc-journal` (0006; it depends on
    `fbc-core`, and `hmac` and `sha2` for its keyed hashes and `zstd` for its closed segments, only: the records the runtime writes, length-prefixed in a hand-written
    little-endian format with a version, a writer of one subdirectory per UTC day with segments
    named `<shard>-<seq>.fbcj` and time from the caller, rolled at every UTC hour, each closed
    segment compressed into `<shard>-<seq>.fbcj.zst` (0026), a reader in write order of both forms, and no byte of
    a redaction span or secret header written but each as its HMAC-SHA-256 under the
    consumer's `RedactionKey` (`src/redact.rs`, 0024; signatures kept verbatim), and a
    `JournalSink` that never blocks: a hand-written queue of atomic words (0021) with a consumer-configured byte budget, a soft
    limit for Normal records and a Safety reserve, drops counted by class, a `Degraded` marker
    once space returns, the gap open now named by its first dropped record (`open_gap`, 0139),
    and a writer thread draining it; the nonce, encode-context and cycle
    records arrived in format version 3, and the keyed hashes of the credential spans a codec
    names in inbound frames and responses (`redact_inbound`, 0028) in version 4, the kind
    each outbound frame was sent as and the control frames received in version 5, the
    stale-authorization reason in version 6, a request deadline's firing in version 7
    (0090), the consumer's own records in version 8 (`Record::Consumer`: its inputs and
    annotations as opaque bytes with a kind and an input flag, written through the sink in the
    shard's total order under the class the consumer picks; 0101), the ingest sequence of a
    consumer input the shard stamped in version 9 (0103), and the `CancelOnly` marker of the
    journal's move of the shard's Quoting markets at its soft limit in version 10 (0139), whose
    reader still reads version 2 to 9 journals), `crates/fbc-sim` (simulated venue and queue-position
    fill-model code, never calibrations; it depends on `fbc-core`, `fbc-book` and
    `fbc-runtime` (for the in-process stream's peer, 0104; the runtime never depends on it): so
    far `QueueModel`, each modelled order's place in its level's queue under a `Bracket`
    (Pessimistic, Middle, Optimistic) from a `QueueConfig` with no `Default`, queued on arrival
    behind its level on the public book less every modelled public order, an RPI order also
    behind public size that joins later, advanced by level cancels per bracket and filled by
    trades at or through its price, 0038; and SimVenue (0046): `SimCodec`, an unchanged
    `ExecCodec` that writes places and cancels as frames on a simulated order-entry stream and
    decodes the answers through `DecodeScope`, and `SimEngine`, a pure state machine fed the
    shard's envelopes (its own books) and those frames, crossing, queueing and filling orders
    and answering after the consumer's `SimLatency`, fees from the consumer's `FeeBook`, all from
    a `SimConfig` with no `Default`, and a resync answered with the resting orders and the
    positions the fills imply, 0049, answers ordered by the stood-in venue's key with realized
    P&L and funding on fills where it declares them and derived fills as order updates, 0050;
    a prune or a clear of one side moving no simulated order in its queue under any bracket, and
    an order to rest on a side the book shows no level of refused like unknown depth, 0124;
    crossing orders meeting SimVenue's own resting orders under the stood-in venue's
    `stp_scope` and depleting the displayed levels they take until the book's next event
    there, 0051; `SimPeer`, the venue end of the runtime's in-process order-entry stream over
    one engine, greeting each opened stream with a frame the codec reports as authenticated,
    counting what the engine refuses, and gapping the engine's books on each market-data
    epoch's end (`SimEngine::end_epoch`), one per bracket, 0104 (`tests/hosted.rs`, with
    `fbc-oms` and the runtime's toy market-data venue by path); a journaled toy market-data
    day replayed through decoder replay into a fresh peer per bracket, with a fixed command
    script authorized through `fbc-oms` at journal positions, to the same outcomes, order
    updates and fills on every run, latency measured in the recorded stamps (FBC-w9g,
    `tests/replayed.rs`); and a closed-loop replay of a toy market-data journal through the shard
    host, a SimVenue session per bracket answering the consumer's fill-dependent commands at
    their replayed instants plus the latency, identical commands, outcomes, fills and `Cycle`
    records on every run with no clock read (FBC-066n, 0107, `tests/closed_loop.rs`);
    still planned: amends, batches, queries and injected
    orders, FBC-nv2), `crates/fbc-conformance` (the adapter conformance kit,
    0025; it depends on `fbc-runtime`, `fbc-oms`, `fbc-journal` and `fbc-core`, never on a
    venue crate: so
    far a public stub venue server on 127.0.0.1 ephemeral ports: a WebSocket endpoint that
    records every connection and data frame and plays either a fault script of typed steps or a
    serving script (0150, FBC-0lng: `WsScript::serving`, which answers each data frame of every
    connection, several open at once, with what the responder made for that connection
    computes, each refused answer and a failed listener reported as a script failure
    (`ScriptError::Answer`, `ScriptError::Listen`, `StubServer::failures`), its `Pusher`
    sending an open connection unsolicited frames and closing it, with or without a code,
    refused on a closed one; `tests/serving.rs`; and, ordering by events, never by times
    (FBC-4ptk): `Pusher::barrier`, true on the client's pong to a ping sent after every frame
    asked before it on that connection, false on one closed or when its ping cannot be written,
    several pending resolving in the order asked, a pong to a later ping resolving the earlier
    ones too; `Responder::hold_first`, a connection's
    first answer computed only once the connection its hold names for the first frame shows
    the client's close frame or its end in the record, a hold naming no connection accepted or
    panicking failing that answer alone, one the stub's drop ends computing nothing, and a step
    script refusing such a responder; and TCP_NODELAY set on every socket either endpoint
    accepts, which no order rests on); and an HTTP/1.1 endpoint with fixed
    responses by path; the reconnect-storm script and a check of connection attempts against
    `ReconnectPacing`; the duplicate-ack and silence-after-subscription scripts; venue crates
    take it as a dev-dependency instead of writing their own servers;
    and `src/toy/`, the conformance toy venue BT-502 means (0044), public and includable by path
    from another crate's tests, so far its order entry: every command kind encoded through
    `ExecCodec`, each declared order capability checked before signing; and its order updates,
    fills (keyed by fill id, or `FillKey::Derived` when declared without one), request rejects
    through its code table and venue modes, decoded through `DecodeScope`; and its answers: order
    queries answered with the query's rpc, a request's items answered in separate frames held
    and pushed in one call (`on_rpc_timeout` pushing `Unknown` only for the unanswered ones), a
    resync answered in frames and pushed whole at its end, and an authentication whose token
    `redact_inbound` names; and its factory, `ToyFactory`; and its market data, `ToyMd`: two
    book channels on one connection kept apart, a snapshot frame decoded whole or not at all, a
    book id outside its declared channels refused with nothing sent, a `rest_anchor` channel
    anchored on an HTTP snapshot, a gap reported as the health of the channel that broke, a
    keepalive frame, and its trades, mark, index and funding decoded on that socket; and
    `ToyStats`, its stats polled over HTTP on a poll endpoint of their own under its own timer
    until the last stats subscription is removed, negative open interest or volume refused, each
    codec refusing a feed its transport does not carry (FBC-z2s); and `src/suite/`, the named
    suite:
    `suite!`, invoked from an adapter's `tests/conformance.rs` with its factory, its fixture
    directory (one subdirectory per check that reads recorded data) and the setup its fixtures
    assume, one test per check, each check also a public function returning what it probed or
    every breach by capability; so far `caps_truthful` (every absence the caps declare refused
    `NotSent(Unsupported)` with no effect, a disarm where the protection offers none included,
    every flag conflict `NotSent(FlagConflict)`, an instrument cancel-all never widened, a
    control per operation sent, a disarm only where one is offered, 0141) and
    `commands_selfcontained` (every amend, cancel and query by each declared reference encoded
    by a fresh codec, alike after it saw the placement), `signing_golden` (each golden command
    the setup lists, encoded under its own `EncodeCtx` by a fresh codec, is one frame equal to
    `<fixtures>/signing_golden/<name>.golden`, a directory marked SYNTHETIC with no golden no
    command names) and `legacy_symbols` (every ticker in
    `<fixtures>/legacy_symbols/tickers.txt` parses through `parse_fbc_common_symbol`; the toy
    reads `X/USDT` by a rule of its own); and the order-entry checks `amend_ack`, `mixed_batch`,
    `unknown_on_timeout`, `resync_after_reconnect` (the stub closes the connection; the
    reconnect resyncs and re-places nothing), `two_phase_ack` and `reject_coverage` (each code of
    `<fixtures>/reject_coverage/table.txt` refused by the stub maps to the kind listed), which
    run `ExecSession` against the stub as the setup's `OrderEntryStub` says, on a paused clock
    the check moves, fbc-oms authorizing every order command (0083, 0085), `mixed_batch` reading a refused and an unanswered batch item as the stub's `BatchFailures` declares (0089); still planned: the suite's other checks and the rest of the conformance
    toy).
  - `crates/fbc-live` (0155): the order-safety glue a live consumer runs beside its order-entry
    sessions, depending on `fbc-core`, `fbc-oms` and `fbc-runtime` only, never on a venue crate
    (`fbc-conformance`, `fbc-journal`, `fbc-sim`, tokio and `rust_decimal` dev-dependencies, its
    tests on the conformance toy, and its replay test on the runtime's toy venue by path), branching
    on no venue's name (0003), `publish = false`. So far `src/link.rs` (FBC-umqi), moved from
    fbc-examples whole with its behaviour: `Link`, the glue between one account's order-entry
    session and its fbc-oms `Registry` (outcomes and order events routed by request and item, fills
    through the `FillLedger`, each fill it refuses for its time kept in `Link::fill_refusals`
    until `Link::take_fill_refusals` hands it over, in order, one refused after a take held for
    the next and every one kept by a link never taken, FBC-2dr2, `tests/fill_refusals.rs`;
    resyncs applied at `ResyncEnd`, each epoch's end reported at its last `recv_mono`, FBC-9svb; the
    one path commands take to the session, `Link::submit`; each place's and batch place item's
    nonce its send receipt names for its item recorded as its order's placement nonce
    (`Registry::placement_nonce_used`), none for a place not sent, an amend or a cancel, a refusal
    a `Note::Problem` naming the order and its kind, never the nonce, so on a venue whose caps
    name that reference (the conformance toy's batch cancel and query) fbc-oms names an order by
    it before its acknowledgement, FBC-o0f0, 0057, 0155, `tests/paths.rs` and `tests/sweep.rs`;
    a Safety cancel for each unprotected order the registry holds open, 0080; `Coverage` of the
    orders a cancel of ours covers, so an order of ours a cancel not ours ends is noted,
    FBC-aoh5, and `Link::cancels_not_ours_seen` true from the first one noted on, never false
    again, `Link::forget` keeping it, FBC-21g6;
    `Link::live_on`, the orders of ours on a market it holds live in the order it learnt them, and
    `Link::is_cancel_on(rpc, inst)`, whether a request it holds is a cancel on that market (a
    cancel or a cancel-many naming an order of ours on it, ended since or not, by the market
    fbc-oms built the cancel from, or a cancel-all of it, which the link records at submit), its
    Safety cancels included, false once released or dropped, FBC-132g; each request's items held
    until `Link::release`, or dropped by `Link::forget` once every item is answered for good
    (accepted at `Final`, refused or not sent) or answered once its order ended (a cancel-all by
    any answer) and the `Coverage` waits on no answer to it (`Coverage::waits_on`: no order end
    held on it and no live order it covers unaccepted; until then it is kept, so its later answer
    still routes as ours), at the forget after the one that took the request's last note, so the
    notes a forget hands over are read against the link as it was, a consumer that never forgets
    its notes keeping them and the requests they name, FBC-mu68; `Link::retain_ended(cfg, now)`,
    fbc-oms's retention pass under the link's caps, which the consumer runs on a journaled timer
    after `Link::forget` and its reading of the notes that forget handed over, its resync bound
    covering the time from the socket's authentication to the `ResyncEnd` applied, a late event
    naming a forgotten order noted as one of an order never held (FBC-6pdo, 0157,
    `tests/retention.rs`); `Link::ladder(orders, now)`,
    one pass of fbc-oms's Unknown ladder on the consumer's timer under the `LadderConfig` given to
    `Link::new`, each query it plans sent as a control command and recorded with `query_sent`,
    each tombstone, by client id, authorized and sent as a cancel of ours and recorded with
    `tombstone_sent` once reported sent, what it could not send kept in its `LadderPass` with why
    and a resync it wants said there, none started; an unprotected order the registry holds ended
    or not at all queried through the attached session by client id and venue id where the caps'
    `query_refs` name one, after the notice's Safety cancels, its market held until the session's
    gate reads an answer showing it ended; each answer routed to `on_query_answer` or
    `on_query_outcome` for a ladder query; a query acknowledged and left unanswered forgotten at a
    pass once fbc-oms no longer awaits it, or, an unprotected order's, the intent timeout after it
    was sent; what was sent and what answered it noted as `LadderNote`s apart from `Link::notes`,
    until `Link::take_ladder_notes`, so a consumer's match on every `Note` compiles, while each
    request's report of being sent, a tombstone's answer and a problem recording a query stay
    ordinary notes, FBC-2cv0; venue text shown
    through `shown` and `described`, whose limits stay fixed, 0155 item 8), the session
    handler's five entry points public methods of it, and `LinkHandler`, which hands each call on
    and then calls the consumer's wake, a closure. It reads no clock but its session's shard clock
    (FBC-y7lq, 0155 item 3): the link times each submission it sends on the `ExecOrders::now` of the
    session it was last attached to or submitted through (`Link::now`, `None` before either), an
    `ExecOrders` on another clock noted as a `Note::Problem` and read from then on, its submission
    still going; its `clippy.toml` disallows the std `Instant` and `SystemTime` reads and
    `fbc_runtime::SystemWallClock`. And `src/not_ours.rs` (FBC-21g6, 0155 item 2), FBC-aoh5's
    classifier moved from `testnet_quote` with its behaviour: `CancelsNotOurs`, one per market,
    under the consumer's `NotOursConfig::new(span, window)` (each refused zero, no `Default`), told
    each order of ours placed, each place that never rested and the orders not ended, and
    classifying each cancel not ours (`on_cancel`) as `NotOurs::SameEvent`, `First` or `Repeat`,
    every call taking `now: MonoNs` from its caller; the WARN, the Stop and every text stay the
    consumer's. And `src/sweep.rs` (FBC-132g, 0155 items 2, 5 and 8), `testnet_quote`'s sweep and
    its rate-limited resend (PR #158) moved with their behaviour: `Sweep::start` for one market and
    account, ending as `SweepEnd::BeforePlaces` (every order of ours `live_on` the market ended and
    `ExecOrders::may_place_on` holds; a refusal of one of its own cancels other than for its
    rate, `AlreadyTerminal` or `NotFound` failing it) or `SweepEnd::AtStop` (every such order
    ended and every request it sent answered, no refusal ending it), sends fbc-oms's
    `cancel_everything`, or explicit cancels of
    the orders of ours `live_on` the market with none in flight or gone Unknown when fbc-oms builds
    no cancel-all or `cancels_not_ours_seen` holds, never an account cancel-all (0012, 0045), a
    round stopped short at a cancel not handed over leaving what it sent to the consumer;
    `Sweep::observe` takes each note the consumer reads, with the link, before it releases the
    note's request, and `Sweep::poll` sends each cancel due on an acknowledgement of an order on its
    market (another market's left due in the registry) and, once one of its own requests or a
    request the link says is a cancel on its market (the consumer's or a Safety cancel) was refused
    for its rate, whatever its order's state, a place's or another market's cancel's making nothing
    due, and the consumer's
    `RateHold::new(wait)` (zero refused, no `Default`) no longer holds, the cancel of everything
    again, at the `now` its caller passes; it keeps no timeout and reads no clock, and its module
    gives Stop's order, which the consumer runs. Its tests: `tests/link_epoch_end.rs`,
    `tests/coverage.rs` and `tests/shown.rs`,
    moved from the samples' tests, `tests/handler.rs`, `tests/paths.rs` (the paths no sample walks,
    through a toy `ExecSession`'s `ExecOrders` it never runs), a unit test of the kinds it names,
    `tests/manifest.rs`, which holds its normal dependencies to those three (0155 item 1),
    `tests/link_clock.rs` (sends timed on the session's stamps on a paused runtime against the
    conformance stub, and a second clock noted), `tests/replay_clock.rs` (two closed-loop replays
    through a `Link`, on SimVenue, with the same send times and notes, and the same orders
    forgotten at the same passes of `Link::retain_ended` (0157); 0155 item 4),
    `tests/retention.rs` (an ended order behind on its fills kept for the stream check, a
    cancel-many's orders kept while its authorization lives and answered with no problem, a
    straggler answer to a request the link still holds noted as ours with no problem after its
    order is forgotten, for one built here and one a resync registered, and late events naming
    a forgotten order noted as of an order never held; FBC-6pdo, 0157),
    `tests/clock_lint.rs` (the lint's five paths and reasons, and no std clock read in its source),
    `tests/fill_refusals.rs` (`Link::take_fill_refusals` in order, a refusal after a take held
    for the next, a link never taken keeping all; FBC-2dr2),
    `tests/ladder.rs` (on a paused runtime against the conformance stub's serving script, an
    unprotected order a lagging resync shows after our cancel ended it, and one the registry never
    held, queried by its venue id and its market released by an answer showing it ended, held on
    one showing it open or on none, none sent where the caps declare no query reference, and a
    notice's Safety cancel sent before its query; an Unknown order queried by its placement nonce
    and resolved by the answer; a tombstone sent once by client id and ended by its acceptance;
    on a session never run, each query or tombstone not sent noted with why, and an acknowledged
    query never answered forgotten as said; and no text in its source naming FBC-m8vm;
    FBC-2cv0),
    `tests/sweep.rs` (the sweep and `live_on` on a paused runtime against the conformance stub's
    serving script: each end, each refusal, one of a cancel not the sweep's failing nothing, a
    batch's and a whole request's answer, the cancel-all or explicit cancels in the order the link
    learnt the orders, which a later resync showing them in another order leaves, an order whose
    cancel came back Unknown cancelled again and one whose cancel awaits its acknowledgement left
    to the cancels due, a batch cancel naming an order the venue has not acknowledged by its
    placement nonce in the first round (FBC-o0f0), a round stopped short, the resend at the
    consumer's wait or a venue's retry time, for a cancel of the consumer's whose order already
    ended while another rests, for a Safety cancel and for a consumer's cancel-all of the market,
    never for a place or a cancel or cancel-all on another market, the cancels due, another
    market's never sent and its cancel unanswered holding no Stop, `Link::is_cancel_on`, and the
    rate hold) and
    `tests/not_ours.rs` (the classifier's test moved from `testnet_quote`'s, on `MonoNs`, a
    venue time absent on either side, a requote that never rested, the consumer's span read on
    both of its measures, each bound held where it falls, and a zero span or window refused); in
    `tests/paths.rs` too, `cancels_not_ours_seen` set by a cancel not ours noted at once and by one
    held until its covering cancel was refused, and kept through `Link::forget`; and, there too
    (FBC-mu68), a single request, a batch answered item by item, a whole-request reject, a report
    of a place not sent, a Safety cancel and a cancel-all each dropped once answered and
    forgotten, held through the forget that took their last note, a batch with an item
    unanswered kept, as is a place or an amend accepted provisionally until its final acceptance
    routes as ours, a cancel gone Unknown until its order ends and a request answered only for an
    item it does not have, a later answer to a dropped request the session's own, `sent_counts`
    across the drops, `Link::release` still dropping at once, and a sweep reading a rate refusal a
    forget handed over as a cancel on its market. And
    `src/sizing.rs` (FBC-6r81, 0155 items 2, 6 and 8), mark-aware sizing moved from the samples
    with its results: `cap_lots` and `inventory_price` (`testnet.rs` re-exporting both),
    `at_market`, the operator's `UsdCaps::new(resting, inventory)` (each cap refused when not
    positive, no ceiling, no `Default`, 0113) with `caps_in_lots`, and `admit_minimum` (the
    minimum the venue's, as the spec's `min_notional` or the consumer's own reading, `None` or zero
    admitting) and `admit_side`, which leave a side out as `LeftOut::AboveRestingCap`,
    `BelowMinimum`, `BelowMinimumAtMark` or `PastInventoryCap`, the inventory cap judged at no less
    than the mark; the mark is the consumer's argument, and it reads no venue and no clock. Its
    test, `tests/sizing.rs`, holds the samples' two moved tests, each `LeftOut`, a side refused at
    the mark alone, `None` and zero minimums, `at_market` and `cap_lots`'s results as before it
    converted with `i64::try_from`, on fbc-core's `Decimal`. And `src/positions.rs` (FBC-rujq,
    0155 items 2, 7 and 8), the samples' positions comparison and its re-read rule (FBC-hpg6,
    0094's condition) moved with its behaviour: a `Reading` (when the consumer's read landed, as
    `MonoNs`, the venue's size in lots or why it was unread, the registry's position and the fills
    of ours applied) agreeing only when read and exactly the registry's, `shown(read)` its text
    with the consumer's read named, and `PositionWatch::new(reread_after)` (zero refused,
    `ZeroReread`, no `Default`), whose `judge` says `Next::Done` with a `Verdict` (`Agrees`, or
    `Cleared` or `Persists` after the re-read, naming no reaction) or `Next::ReadAgainAt` the
    first reading's time plus the wait for a mismatch or an unread answer; `fills_in`, `fill_seen`
    and `resync_disagreements` (`testnet.rs` re-exporting it) over the link's notes. The read is
    the consumer's: it reads no venue and no clock. Its test, `tests/positions.rs`, holds each
    verdict, an unread answer first and on the re-read, the re-read's time, a zero wait refused,
    `shown`, the notes' counts and `resync_disagreements` on each `PositionCheck` and an
    unsettled fill. And `src/reconcile.rs` (FBC-1awz, 0094's condition, 0155 items 2 and 3),
    `testnet_quote`'s stream check moved with its behaviour: `unreconciled(registry, streamed,
    inventory, watched, ended)` says `Unreconciled::Position` for a position the venue's stream
    reported (`None` before any) other than the inventory's (Codex r4244080127 on PR #149), and
    otherwise `Unreconciled::Fills` for the first order of ours in `watched`, then in `ended`,
    whose cumulative fill on an order event is above the fills read (`fills_behind`, `cum_venue`
    above `cum_fills`; Codex r4244145969), trimming `ended`, the consumer's list of orders that
    ended so, to those the registry still holds behind once the positions agree, their order
    kept; it keeps no clock and no timeout, and the consumer's hold, its text and its Stop stay
    the consumer's. Its test, `tests/reconcile.rs`, holds a streamed position that differs, none
    streamed yet, an order's fills behind until both fills are read, the watched orders before
    the ended ones, each ended one dropped once not behind or not held, and the list left as it
    is while the positions differ.
  - `crates/fbc-examples`: sample programs the owner runs by hand, never CI (FBC-u4so). Its
    library target is empty and every dependency is a dev-dependency, so it is outside
    `crates/venues/` yet wires concrete venues without a normal dependency on one; each sample is
    an example target whose wiring a test in `tests/` includes by path and runs against the
    conformance stub. So far `examples/md_watch/`: a Paradex market's bbo, trades and `deltas`
    book, a Binance USD-M symbol's `bookTicker` and diff-depth book, and a Hibachi contract's
    `ask_bid_price` touch, trades and `orderbook` book (FBC-9o7e: the contract read first
    through the venue's discovery, its open or closed state, each trading status its status poll
    reports (FBC-1632) and its socket codec's counts printed, `examples/md_watch/hibachi.rs`) through `MdVenue`, one `MdBooks` per venue, a line per
    update; no order, no credential (`tests/md_watch.rs`); and
    `examples/testnet_trade/` (FBC-x69b): Paradex TESTNET only (the URLs, exactly the testnet's
    or a loopback stub's at `/v1`, and the chain id checked before anything connects), an
    fbc-oms registry declared an owner-assisted testnet run (decision 0067) behind one
    `ExecSession`, its events routed into the registry by `fbc_live::link` (the
    ExecHandler-to-Registry glue, 0155), one post-only order placed well away from the touch read
    from `GET /orderbook` (read again just before the place), its size, caps, side, distance and
    client-id namespace all required flags, cancelled, then Stop (`tests/testnet_trade.rs`,
    against a Paradex-shaped conformance stub). Its credentials come from environment variables
    through `src/auth.rs`, a review path included by path, not a module of the empty library
    target. And `examples/paradex_testnet_acceptance/` (FBC-xji8): FBC-8xr's owner-run
    acceptance run on Paradex TESTNET, reusing testnet_trade's flags and helpers and, by path, the
    testnet guard, its sessions routed through `fbc_live::link`: place, amend, cancel, batch create,
    cancel-many, an IOC expiry, behind `--allow-fill` a take flattened by a reduce-only IOC and a
    partly filled order's amend (FBC-6fnc), and cancel-on-disconnect on a dropped socket and on a
    graceful close, each through an fbc-oms authorization, in three phases (a fresh registry and
    session each); after every step its own authenticated REST reads (`src/auth_rest.rs`, a review
    path) compare GET /orders and GET /positions with the order stream; it writes `report.md`,
    `evidence.json`, its journal and a scrubbed fixture slice for `fixtures/paradex/exec-recorded/`
    (`tests/paradex_testnet_acceptance.rs`, against a stub model of the account; how the owner
    runs it, from the deployment box too: `crates/fbc-examples/README.md`).
    And `examples/testnet_quote/` (FBC-elg7): the same testnet guard and wiring, a
    PLUMBING TEST, NOT A STRATEGY: one post-only order a side a fixed offset behind the touch,
    requoted through fbc-oms's `ExecutionPlanner` given Paradex's caps with no amend
    (cancel-and-replace only), every order of ours still open cancelled after each reconnect
    before any place and at Stop, both through fbc-live's sweep, Stop on Ctrl-C, `--duration`,
    `--cycles`, a resync mismatch, any Unknown order, a refused login or a permanent refusal of a
    place or cancel, every touch read with the mark price and charged to the session's rate
    limiter (`tests/testnet_quote.rs`); with
    `--mainnet` (FBC-tyft) a supervised Paradex MAINNET session behind `--mainnet` and
    `--sole-trader`: mainnet's URLs and chain, the mainnet-only credentials, the operator's own
    positive caps (required, no defaults, no ceiling: FBC-thks), offsets of at least 20 bps,
    `--duration` of at most an hour, never seeded by hand.
    And `examples/qfex_preprod_probe/` (FBC-l931): stage one of decision 0132 item 7's QFEX probe,
    an owner-assisted run on QFEX PRE-PRODUCTION, a declared testnet run there (0067), or with
    `--production` (FBC-3prm, 0143) on QFEX PRODUCTION under the adapter's `production_probe`
    with the production origins as its defaults and registries built with no testnet
    declaration, arming cancel-on-disconnect once
    per account and placing nothing; its report, evidence and fixture README name the environment.
    Before anything connects it refuses any origin the adapter's probe guard refuses (so a
    production origin without `--production`, and a pre-production one with it), an account 0122
    item 1 refuses, a `QFEX_SUBACCOUNT_ID` selecting the primary account (0144), missing
    credentials, a proxied loopback origin and fill-ledger bounds that
    cannot outlast a registry (0132 item 5, FBC-5hif: the required `--registry-span`, a
    `--ledger-max-age` longer than it and a `--ledger-max-entries` above the fill ceiling, every
    order a registry's sessions may place at its largest size in size steps, zero in stage one);
    before each session it reads the accounts its configuration names, the primary and the
    subaccount `QFEX_SUBACCOUNT_ID` names, and sends no `GET /user/subaccounts`, the owner
    confirming before each run that no other subaccount exists (0144): each one's REST positions
    and one `get_user_orders` page on a socket that never arms (the adapter's `account_reads`),
    starting a session only when each is flat with no open order on the page and none counted by
    REST; one unchanged `ExecSession` per `--arm`, one after the other, with the
    `AccountWideCodAuthority` (0121) and a registry of its own whose one fill ledger it keeps
    (`ledger.rs`), its arm answered by the codec (Accepted on its ack, FBC-jxau) and its resync's
    reads sent (one `GET /user/positions` and the `get_user_orders` pages; no `get_user_trades`,
    since nothing places), stopped once its arm has its outcome and its resync has ended, at the
    connection's end or at the span bound, its pacing allowing no second attempt; it writes a
    report, evidence (each registry's ledger bounds, and its horizon and replay counts at each
    session's end; the provider rate refusals each read socket and session met, at least, 0135 item
    2, FBC-m9rj: the shared limiter's `counts().rejected`, the `RateLimited` errs and `RATE_LIMITED`
    order responses among its frames as a journal keeps them, and its frames unread (kept whole by
    0134 item 2, or not JSON), a read socket's read once more in memory for a refusal kept whole, a
    count above zero named as evidence for FBC-s5ox; `refusals.rs`; and each full
    `get_user_orders` page each session's resync read, by its page size and offset, none for a
    session whose pages were all short, read from the session's journal, each request paired with
    the page answering it, since the codec's own count is not reachable outside an `ExecSession`,
    0132 item 5, FBC-4h1p, `pages.rs`), its journal and a scrubbed fixture slice for
    `fixtures/qfex/exec-recorded/arm/` (a production run's for
    `arm-production/`, 0143 item 4) (`tests/qfex_preprod_probe.rs`, against a QFEX-shaped
    conformance stub).
    With `--hold-secs` (FBC-39ks, 0146), on either environment, it first holds one trade
    connection that never arms (`hold.rs`; `--arm` then optional, the ledger flags required only
    with it or `--place-stages`): after the named accounts' reads, before any session and with no other connection
    open, on the account `--hold-account` names (the primary by default), it sends its
    authentication and the three subscriptions a session sends through the adapter's
    `account_reads` and the shared limiter, then nothing but the pongs and close reply the
    WebSocket layer writes, each recorded on that limiter as a session records one (0030), its
    own close charged as a control frame (none, the connection dropped, when it has no room); it
    records each ping, pong and close with its time since the connection opened (lengths only,
    codes in full, its own close's echo never a server close), counts its data frames by kind
    (a kind name read in memory from what a journal would keep, never a value) and their provider
    rate refusals and keeps none (an err a journal would keep whole read in memory for its code
    alone), reports an order or fill frame on it, ends at its duration, the owner's Ctrl-C (an
    injected `stage::Interrupt`; `interrupt.rs` hands each Ctrl-C from tokio's `ctrl_c` to the
    hold from its connection's opening until its close is over, a press while it closes
    recorded, and otherwise ends the program with no report, as before, except in a place-stage
    run, whose `interrupt::Presses` take it), a server close, the
    connection's end or a refusal, with no reconnection, then reads every named account again,
    whatever ended it; the report gives both reads, states the owner's two unchecked conditions
    (no subaccount left out, 0144 item 2; nothing traded from the first read to the last) and
    reports whether any ping came before authentication, the intervals after it, each server
    close and that a lifetime longer than the hold was not measured; a hold that fails, an order
    or fill frame on it, an account read after it that is not clean, or Ctrl-C on the hold in a
    run that also arms stops the run, so no session starts. For FBC-9sk2's place stages
    `ledger.rs` also holds (FBC-oxjs; decision 0148 items 3 and 7, built to 0151 items 1, 9 and
    12) the place registries, each named account's drop test building its first: four per named
    account, one per `Step` (drop, cross, take, pages), each a `PlaceRegistry` built with its
    caps, declared `CodProbeRun::owner_supervised_places` on its one market and never a testnet
    run (production only), under its lease names and bound to its account, its `CancelOnlyTrip`
    given to the journal (`trip_on`, 0139), and handed with its one fill ledger to one `Link` for
    the registry's life (`into_link`, `PlaceRun::session_ended` reading the ledger back); its
    `MayPlace` list from `PlaceLists`, every order at the minimum size in size steps, the
    entries other than the flatten IOCs summing to 0148 item 7's most per account and every list
    carrying `--flatten-attempts` flatten IOCs under one budget per account, which the driver
    spends (FBC-23ch); and its count bound above that list's ceiling (`PlaceBounds::accept`), the
    report showing the list and the ceiling as fences met (`place_markdown`, 0151 item 10),
    held in `tests/qfex_preprod_probe/place_registries.rs`, which `tests/qfex_preprod_probe.rs`
    declares by path. With `--place-stages` (FBC-c6kw, FBC-23ch, FBC-77r8, FBC-su9i, FBC-ou9g and
    FBC-4plj, slices of FBC-9sk2) it runs the place stages' preflight, then each named account's
    drop test, then stage two's crossing post-only and taker IOC, then stage three's pages: on
    production alone
    (`--production`), on the origins the adapter's probe guard admits there, QFEX production's or
    every one loopback, any other refused before anything connects (FBC-4plj lifted the
    loopback-only gate once every stop had landed); every place-stage flag (`--symbol`, `--side`,
    `--sole-trader`, both caps, `--away-bps` 20 to 10000, `--flatten-cap-bps` below it,
    `--flatten-attempts` 1 to 3, `--namespace`, `--lease-dir` never the testnet samples',
    `--agree-wait-ms` and `--read-interval-ms` at most 10 reads per comparison,
    `--touch-max-age-ms`, the ledger flags, `--registry-span` at most an hour and above the drop
    test's two sessions and the reads between them) refused naming the flag, never its value, and
    refused given twice (`probe_args::Places`, `Parsed::Places`; `--lease-dir` refused with a `..`
    or as a symbolic link to the testnet samples' default); a
    `CAPS` line with the environment and both caps first, before any request, the caps on
    standard output alone (0113, 0148 item 7); the hold first, alone, when `--hold-secs` asks,
    the owner's Ctrl-C from its first read to its last stopping the run before its preflight
    with no stop marker (one before its connection opens keeping it from opening, one during the
    reads after it letting them end); then `market.rs`: the traded
    spec from this run's `GET /refdata` through the adapter's discovery on the guarded
    configuration, charged to the run's limiter and sent within `--read-wait-ms` (a symbol not
    listed, not trading or not listing both a post-only GTC and a limit IOC refused, never
    named), the count bound against each place
    registry's ceiling, the MDS touch (`bbo` on an `MdVenue` of its own on the guarded MDS origin,
    a touch from before the connection's current epoch, one-sided, crossed or locked, or older
    than `--touch-max-age-ms` refused) and each cap at least three minimum sizes at the touch,
    priced at the resting price; `market.rs` also holds the pure prices, tested, each moved and
    snapped to its safe side in whole ticks (a resting order `--away-bps` behind the touch, the
    crossing orders at the far touch, a flatten IOC at most `--flatten-cap-bps` through the touch
    it crosses), of which the resting price is used for the caps, X and Y and the far touch for
    the crossing post-only, the touch kept current by `Touch::driven`, which `stage.rs` runs the
    reads that clear a marker and every place stage beside, from the preflight's first touch until
    they end, so each server ping on the MDS socket is answered between the sessions as well as
    during them, and a touch connection that ended is not opened again (FBC-kllr).
    `stage::place_config` adds `qfex.exec.production_places` to a place stage's session
    configuration alone (0148 item 4).
    Its report and evidence give the fences met and the steps passed, never a value, and its
    evidence's ticket is FBC-9sk2. FBC-23ch's foundation is `places.rs` (0151 items 1, 2, 6 and
    9): before anything connects the run opens its lease directory, tagging it production on its
    first use and refusing one tagged otherwise, refusing to start while a stop marker (`stopped`,
    the stop's name and the run's start) is in it or in production's default unless
    `--reconciled <hand-cancel|hand-close|both|none>` is given, and `--reconciled` when there is
    none, and taking one `NamespaceLease` for the run beside one `CidMint` across its accounts,
    whose `HighWater` mark is written off the runtime's thread before an id is returned
    (`Mint`); `start` and `flatten` take the `MarketLease` (no account lease); `Stop` names every
    stop of 0132 item 7 and 0148 item 8 (plus `BeyondList`, `BeyondCaps` and `NoEcho`), none
    with an id or an amount, only `Interrupted` and a fill of ours cancelling in a drop session,
    whichever stop came first; `Allowance`
    spends each registry's list and the account's one flatten budget, refusing a place beyond
    them; with `--reconciled` the run reads every named account after the preflight and clears
    the marker only if every read is clean, the declaration reported unchecked; and `main.rs`
    hands a place-stage run each Ctrl-C the hold does not take (`interrupt::Presses`), so a press
    during the preflight or those reads stops it, `Interrupted`, with its report and its marker,
    which keeps the stops of a marker it replaces unless the run cleared it. `tee.rs` wraps the
    run's journal sink (`Tee`, every call delegated, `open_gap` included, so 0139's trip holds),
    reads each record with its spans blanked and keeps no frame: each `add_order`'s client id and
    whether it carried `take_profit`, `stop_loss` or `reduce_only`, each `order_response`'s status
    and each `fill_response`'s fee sign, whether each names one of our client ids byte for byte,
    and an echo's client-id span compared raw, for the report alone; it raises `NotOurCid` (an
    order event or fill of ours without our client id, but not a `NO_SUCH_ORDER` answering a
    cancel of ours, a cancel reject), `StrayOrder` (one naming no order of ours, 0151 item 13)
    and `RateRefused` (a `RateLimited` err, a frame whose blanked text is no JSON object, as
    0134 item 2 keeps one whole, read once more in memory as it arrived for that alone, as 0135
    item 2 reads a read socket's, a `RATE_LIMITED` status, a resync's 429 or 418); `StrayOrder`
    also covers a first frame of X naming no client id of ours before X's venue id is known.
    `compare.rs` compares the streams with never-arming reads after each step (0109, 0132 item
    7, 0151 item 4), each socket's limiter count the refusals it held the budget for itself, so
    a session's met meanwhile is counted once, as the session's: one socket
    per comparison, `get_user_orders` from offset 0 until a short page and REST positions under
    `--namespace`, every frame and request charged to the shared limiter first and one without
    room skipping that read, each read `--read-interval-ms` after the last began, at most 10
    within `--agree-wait-ms`, none beginning at its end or past it nor waited for, a read still
    running at its end sending nothing more (`Reader::until`) but reading the answer it asked for,
    so a refusal in it is heard, and otherwise not judged (`Took::Unfinished`), the first agreeing
    read timed, `ReadDisagrees` at the end of the wait and lag recorded, judged on the last read
    finished within it, a provider refusal on the read socket (one in a frame the journal keeps
    whole, which holds no budget, included: `Refusals::shown`) or REST a stop. Every read socket of
    the run, a comparison's and an account read's, charges its close to the shared limiter first as
    a control frame and, with no room (under a provider hold, say), is dropped with no close frame;
    the pong the WebSocket layer writes to a ping on one is recorded there (0030).
    `stage::check_accounts_allowing` admits one order of ours (`capture::AllowedOrder`, at most one
    row) on its account alone, X before drop session two: listed, and counted by REST as one open
    order on its symbol and none on another (`AccountRead::of`, 0132 item 7, 0144 item 4); every
    other read admits none. The drop test (`stage.rs`'s `Run::drop_tests`; 0132 items 4 and 7, 0148
    item 2, 0151 items 2, 4, 5, 9, 11 and 13) runs on each named account in turn, each account's
    only once the one before ended with no stop. Drop session one (FBC-23ch): every named account
    read clean (or by the reads that cleared a marker; a provider refusal in either, one kept whole
    or a REST 429 or 418 included, a stop, marked, `Run::unless_refused`), the account's drop
    registry built, its trip given to the journal before any market arms, seeded flat; one unchanged
    `ExecSession` on `place_config` with the discovered spec table, the run's `--namespace`, one
    `RpcIds` for the run, the run's next session number, an `AccountWideCodAuthority` for its own
    account and one connection attempt, its handler `places::PlaceHandler` (the shared
    `LinkHandler`, as `paradex_testnet_acceptance`'s `Tap` wraps it, noting authentication, the arm,
    the resync and the connection's close, and stopping on a resync's `Desync` or `Unsettled`, a
    ledger's `Untimed` or `BeyondHorizon` refusal, a fill of ours and an unplanned end); once the
    arm is accepted and the resync applied, Start under the market lease, then X, one post-only GTC
    at the minimum size `--away-bps` behind the touch, minted under the namespace lease and an
    fbc-oms authorization through `Link::submit`, the one place before an ACK echoes our client id,
    a stop or a press already made before it placing nothing, and sent only while both USD caps
    admit the registry's lot caps, fixed at the preflight's touch, and X at the touch X is priced at
    (`market::caps_admit`; otherwise `BeyondCaps`, nothing sent or resized, 0113); X's ACK or its
    deadline; a comparison; a graceful close, waited for the shared limiter's room after a provider
    refusal so it stays graceful (with no room within the patience, `stage::NO_ROOM_TO_CLOSE`, not
    counted graceful), a Ctrl-C during that wait, and a stop the tee or the handler raised during it
    once a fill of ours came, taken before the close, its cancels sent once there is room, or, with
    none within the patience, held as 0151 item 11 says; then the read of every named account and of
    X and its trades (`account_reads`' trades and historic-orders reads, FBC-be57; a full page
    repeating the one before it, as a venue ignoring the offset answers, failing the read, and the
    whole read cut `AFTER_PATIENCE` after it began, its pages not all read (`stage::HISTORY_CUT`,
    FBC-8o7a), and so in every such read after a session), waiting out a rate hold rather than
    skipping, a provider refusal (one kept whole included) or a Ctrl-C during them a stop, a trade
    naming X in them
    `NotFlat` (a fill seen with no session open, 0151 item 9), then X's own account not read for a
    reason that is no provider rate refusal `LeftResting` (FBC-gisx; decision 0005 as the owner
    ruled under 0151 item 6: X counted resting unless its registry holds it ended,
    `stage::ended_on`, or a historic-orders row named it, which survives a later page that failed;
    the read's step that failed named by `places::AccountUnread`, `GET /user/positions` not sent
    or not answered, the read socket or its page, recorded through `Run::read_one` and never its
    text; the hand cancel asked for as `places::HAND_CANCEL_UNREAD` and the stop said as
    `places::COUNTED_RESTING` through `Run::halt_as`, never as X seen resting, after the session's
    failure when it failed), then X's trades or historic-orders not read whole with no provider
    rate refusal `LeftResting`, X left unjudged and said so (`places::Unseen` in the report and
    evidence, the stop said as `places::LEFT_UNJUDGED`; X's status on a page read keeping it
    judged), each after `ReadDisagrees` for an order none of ours or a TWAP a read listed, then a
    failure of drop session one that was no stop, kept as the session's error, with
    `ReadDisagrees` marked for an order none of ours or a TWAP, else `LeftResting` when an order
    of ours rests, each said after the session's error; X's account not read once X's end was
    shown counts nothing, the report saying X's end was shown (`places::X_ENDED_AFTER_ONE`). Drop
    session
    two (FBC-77r8): every named account read again, admitting X alone (`Run::read_before_two`, a
    refusal kept whole on its sockets a stop, an account not read with X not listed after drop
    session one said as `places::COUNTED_UNREAD`); a session on the same `Link` and registry, seeded
    nothing and not Started again (its market armed from drop session one), whose handler watches
    only what follows its start (`places::Watched::after`): its arm accepted; Y, placed as X was
    (`Run::place_resting`, held to `market::caps_admit` at its own touch); Y's ACK or its deadline;
    the read before the drop, one never-arming read beside the session, a `CANCELLED` naming X heard
    by its end recorded as the arm's, reported (`tee::Kept::cancelled_of_from`), and a stop or a
    press met as its socket closes taken before the drop; then that read not made a stop,
    `LeftResting`, said as X and Y counted resting (`places::UNREAD_BEFORE_DROP`), and that read
    showing an order or a TWAP neither X nor Y (`verdict::others`) or a position the registry
    does not hold a stop, `ReadDisagrees` (0148 item 8), each before the
    drop and cancelling nothing; then the abrupt drop, the session's run dropped unpolled and its
    connection with it, no WebSocket close (`stage::DROPPED`), the capture taking the session's
    end as its close (`capture.rs`), with what the session showed as it dropped
    (`Run::at_drop`: a `CANCELLED` of X heard, which leaves the drop's reach over X not measured,
    and Y still open in the registry with no `CANCELLED` of it, which the verdict needs,
    `YEndedBeforeDrop` otherwise, and Y listed by the read before the drop, made after Y's ACK,
    which it needs too, `YNotListedBeforeDrop` otherwise). After the drop (`Run::judge_drop`):
    reads of the account at the comparisons' cadence until X and Y are absent
    (`verdict::until_absent`), cut at the end of the wait as a comparison's are (none begun at or
    past it, a read still running there sending
    nothing more, its answer still read so a refusal in it is heard, and otherwise not judged,
    `verdict::Shown::Unfinished`), the trades since X's send and `GET /user/historic-orders` for
    both (`Run::read_orders`, the member names a trades row carries kept as names alone), the
    verdict (`verdict::DropVerdict`, per account, in memory, never
    carried to another account or run; a row since X's send naming another order or none fails
    it, and so does an order or a TWAP neither X nor Y on any read after the drop
    (`verdict::Failure::OtherOrder`, `ReadDisagrees`); a fill or a status a page showed counts
    though a later page failed (`verdict::Facts::traded_seen`, `terminal_seen`); and X's absence,
    before the drop or after it, counts as the arm's or the drop's cancel
    only as `places::close_verdict` counts it, the trades read whole, no row naming X or another
    order or none, the position flat, and otherwise is not measured, its reason given,
    `verdict::Unmeasured`, 0132 item 4), then the read of every named account. From the first arm to
    the last read nothing cancels X or Y but Ctrl-C or a fill of ours, nothing reconnects and no
    later arm is sent. The first stop ends the steps, and one raised as a session closes is still
    the run's; only Ctrl-C and a fill of ours cancel in a drop session (Kill, `cancel_everything` by
    venue id, a wait up to `--rpc-timeout-ms` with a second Ctrl-C recorded, disarm; cancels held by
    a rate refusal or refused locally end by a graceful close, 0151 item 11), every other stop
    sending none and ending the session by a graceful close before any drop, either session's
    graceful close waiting for room as drop session one's does and taking a Ctrl-C or a fill of ours
    meanwhile first; a press between the sessions or after the drop is taken once the reads under
    way end. The run ends with its stop and marker, any order of ours still resting reported for the
    owner's hand cancel: a stop of either session; `NotFlat` on a position after a session or a
    trade naming X after drop session one; `DropFailed` for a trade naming another order or none
    after drop session one, Y never placed; `LeftResting` for X's own account not read after drop
    session one for a reason that is no provider rate refusal, X counted resting, then for X's
    trades or historic-orders not read whole after it, X left unjudged (FBC-gisx), each after
    `ReadDisagrees` for an order none of ours or a TWAP; a failure of drop session one that is no
    stop, with `ReadDisagrees` added when its reads show an order none of ours or a TWAP, else
    `LeftResting` when an order of ours rests; a stop of the read before drop session two; a
    failure of drop session two that is no stop, with X's trades, and Y's once submitted, read
    after the account reads, and `NotFlat`, `ReadDisagrees` or `LeftResting` added when those
    reads show a position or a trade naming X or Y, an order none of the drop test's or a TWAP, or
    an order resting (`stage::after_stop`, after `stage::read_stop`'s refusal and Ctrl-C), or,
    with none, `LeftResting` when X's account could not be read after it for a reason that is no
    provider rate refusal and no verdict judged X and Y, the orders sent whose end the streams did
    not show counted resting (`places::Counted`: X unless its registry holds it ended, Y once
    submitted, and any flatten IOC whose end the registry did not show), each named alone
    (`Counted::WithFlatten` naming X and Y only as they count), X's end shown said so
    (`places::X_ENDED_AFTER_TWO`), and reported for the owner's hand cancel as not read
    (`Counted::hand_cancel`, the account's step that failed in `TwoRun::unread`, the stop said
    as `places::COUNTED_RESTING`, FBC-gisx; none counted, no stop); the verdict's (`DropFailed`,
    `ArmCancelUnreported` recorded and stopped on after the drop, `UnexpectedFill` for a trades
    row naming X or Y, the run not flat and the position the owner's, 0151 item 9, said so
    beside a provider refusal that came first, `ReadDisagrees` for an order or a TWAP not ours
    after the drop, or a provider refusal), but the read of every
    named account after the drop outranking a stop after its own in the verdict's order
    (`stage::outranks`: `NotFlat` over `ReadDisagrees`, `ArmCancelUnreported` and `DropFailed`,
    `ReadDisagrees` over the last two); or `ReadDisagrees` or `LeftResting` after drop session
    two, then, last, `LeftResting` for a named account, X's or another, not read after it
    (FBC-8mwz, `stage::NOT_READ_AFTER_TWO`, said as `places::COUNTED_UNREAD`; decision 0132 item
    7), where a failed drop session two stops only on the orders `places::Counted` counts. A
    failed drop test is a stop, so nothing more is placed on its account or any other; the
    drop registry is disarmed once its second session
    ends, giving up its market lease. With none, stage two follows (FBC-su9i; `stage.rs`'s
    `Run::cross_tests`, `stage_two.rs`; decisions 0005, 0132 item 7, 0148 items 2, 6 and 8, 0151
    items 5, 9 and 11): the run's exact-echo gate (`stage_two::EchoGate`, open only once an
    `order_response` `ACK` in this run named one of our client ids byte for byte,
    `tee::Kept::exact_ack`, which fbc-core's `CidMatch` cannot judge; per run, in memory; an
    upper-cased or escaped id or an `ack` or `err` echo never opening it, each echo's exactness
    recorded); shut, the run ends `NoEcho`, not a fault, with no marker, every account's drop test
    having run. An `ACK` whose client id is absent, null or empty names no order of ours, so it
    never reaches the gate: it stops the run `StrayOrder` in drop session one, marked, before the
    gate is judged (0151 item 13, which wins over the ticket's `NoEcho` for these three). Open, each
    named account whose drop test passed in this run (`stage_two::crossers`), in turn
    (`Run::cross_test`): every named account read clean (a provider refusal in those reads, one kept
    whole or a REST 429 or 418 included, a stop, marked, `Run::unless_refused`), its cross registry
    built, tripped and seeded flat, spending the allowance its drop test spent from; one session
    (`Run::cross_session`) armed, resynced and Started under the market lease, its span bound met
    one `--rpc-timeout-ms` early (`stage::cancels_begin`); then `stage_two::no_other_order` (every
    registry of every named account built in the run holding no order of ours on the market open, in
    flight or `Unknown`, `Registry::resting_on` on both sides, X and Y counted ended only on 0151
    item 5's three reads after the drop, and only as `places::close_verdict` counts X's absence, the
    trades since X's send read whole with no row at all and the position flat,
    `stage_two::ended_after_drop`, and each named account's read before the session listing none),
    else no crossing order and `LeftResting` or `ReadDisagrees`; the crossing post-only, one
    post-only GTC at the minimum size at the far touch, an fbc-oms authorization spent from the
    cross registry's list, sent only while both USD caps admit the registry's lot caps and the order
    at the far touch it is priced at (`market::caps_admit`; otherwise `BeyondCaps`, nothing sent or
    resized, 0113); its first answer, its ACK timed, then its end or its deadline, a place never
    answered ending `Unknown`, never resent (no Unknown ladder; 0005); `stage_two::Crossed`, read
    before any cancel of ours (`CANCELLED`, an unmapped status or another mapped one, each saying
    whether it named our client id byte for byte; an err echo of the `add_order`, whatever client id
    it names, `tee::EchoCid` telling ours exactly, ours in another case or escaped, not ours, or
    none; a rate refusal; an err echoing no message after the `add_order`; a fill; an ACK then an
    end; rested; `Unknown`); a comparison; then the session machinery (`stage::Policy::Stage`,
    `Run::end_session`): every order of ours cancelled by venue id before the session stops, at
    every stop, Ctrl-C and the span bound included, and on a session that ran its course too, then
    the streams compared with the reads again, that comparison's provider rate refusal or
    `ReadDisagrees` the session's stop, marked, when none came before it, then a graceful close
    (waiting for room as a drop session's does, a Ctrl-C during that wait, after the cancels, left
    to the reads after the session), or the abrupt drop when those cancels were refused locally or
    met by a rate refusal (`places::Cancels::blocked`), since the account's drop test passed (0151
    item 11), those cancels then the session's stop in their own right when none came before them,
    `CancelsBlocked`, marked, which says nothing of an order resting (`stage::stage_session_stop`,
    FBC-3xzn, its local refusal held by the test hook `refuse_stop_cancels`); then the read of
    every named account, the session's orders (its own, then its
    flatten IOCs) known as ours on its own account, after the abrupt drop read again at the
    comparisons' cadence until a round lists no order of ours or `--agree-wait-ms` ends, or the
    round judged lists an order or a TWAP not ours, shows a position not flat, cannot read a named
    account, meets a provider rate refusal or takes the owner's Ctrl-C
    (`stage::AfterReads::read_again`), a round finished past the wait or one that cannot read an
    account folded into the round before it, erasing nothing that round showed
    (`stage::AfterReads::fold`), since QFEX cancels on a drop only once it learns of it
    (`Run::read_after_stage_drop`, 0151 item 4). A fill of it is `UnexpectedFill`, flattened first
    (below); a position not flat in those reads is said to be the owner's whatever stop came first
    (`stage_two::position_said`, 0151 item 9); an order or a TWAP not ours in those reads is
    `ReadDisagrees` (`stage::stage_after_stop`: after a provider rate refusal, Ctrl-C and `NotFlat`,
    before `LeftResting`, as `stage::after_stop` orders them); an order of ours still resting after
    the session, listed by a read, counted open by REST and listed by no page, or counted resting by
    the session's registry (`stage_two::counted_resting`: an `Unknown` one, from an unmapped status
    or an err echo too), is `LeftResting`, so it blocks every crossing order after it, and so is a
    named account that could not be read after the session, marked so the next run reconciles;
    one no read listed is said as counted resting, by the registry or on an account not read, never
    as seen resting (`stage::stage_counted_says`, `places::COUNTED_BY_REGISTRY`,
    `places::COUNTED_UNREAD`, through `Run::halt_as`; FBC-gisx), as is an order a registry holds
    that kept the session's order back (`places::COUNTED_BLOCKED`), and the report, the evidence
    and the `RESTING` line give the hand cancel alike (`stage_two::hand_cancel`: counted by the
    registry, `stage_two::HAND_CANCEL_CROSS_COUNTED` or `taker::HAND_CANCEL_TAKE_COUNTED`, unless
    the reads after the abrupt drop listed none, or the account not read once an order was sent,
    `stage_two::HAND_CANCEL_STAGE_UNREAD`).
    With no stop the account's taker IOC session follows (FBC-ou9g; `taker.rs`; `Run::cross_test`
    and `Run::cross_session` under `Which::Take`, `Run::take_steps`, `Run::place_taker`), before the
    next account's crossing session (0148 item 1: the stages on each named account in turn): every
    named account read clean (a provider refusal in those reads, one kept whole or a REST 429 or 418
    included, a stop, marked, `Run::unless_refused`), its take registry built, tripped and seeded
    flat; one session armed, resynced and Started; `stage_two::no_other_order`, else no taker IOC;
    one limit IOC at the minimum size at the far touch, through it, spent from the take registry's
    list, sent only while both USD caps admit the registry's lot caps and the IOC at that touch
    (`market::caps_admit`; otherwise `BeyondCaps`, nothing sent or resized, 0113), its fills
    expected (`places::Watched::expected`), so none is `UnexpectedFill`; its end with its fills
    settled and, once a `fill_response` named it, an `order_response` naming it with a status other
    than `ACK` (`Run::final_order_unheard`), or its deadline, never resent; how it came back
    (`flatten::Ioc`), each fill's fee sign as fbc-core's `Fee` holds it (`taker::FeeSign`, read in
    `places::PlaceHandler`) and the order of its final `order_response` and its `fill_response` from
    the tee (`taker::FrameOrder`, for FBC-tfxk; the final one a status other than `ACK`, so an `ACK`
    and a fill with none after are not measured), read before any cancel of ours (`Run::taken`); a
    comparison. Then (`taker::after_ioc`): a position off flat is flattened while the session is
    open, as below (`taker::AFTER_TAKER`), REST flat before the Stop, and stops nothing once flat;
    an IOC that expired unfilled flattens nothing; one left open with a fill (QFEX's
    `IOC_PARTIALLY_FILLED` with a remainder, which the adapter decodes as Open) is `IocRemainder`,
    not flat, with no flatten (0151 item 8, until FBC-jdj7); one whose fills the venue's count
    states but which had not all reached the registry by its deadline is `IocUnsettled`, not flat,
    with no flatten, whatever the registry's position. Then the Stop and the reads after it as after
    a crossing session (`AFTER_TAKE`, `stage::stage_after_stop`, which says each stop as met after
    the taker IOC session). With no stop, the taker IOC's and its flatten IOCs' trades since its
    send and their historic orders are read as X's are after drop session one (`Run::take_trades`,
    `Run::read_orders`, a read socket `<account> (the taker IOC and its trades)` under
    `AFTER_TAKE`), each row's `timestamp` kept as a kind by its magnitude (`places::time_kind`),
    its `trade_id` matched with those the tee kept of our `fill_response`s in memory
    (`places::TradeIds`, `tee::Kept::heard_trade`), its side fields against ours
    (`places::Sides`), and a row naming no order of the run counted; a provider rate refusal or
    Ctrl-C during it is a stop, marked, then a row naming none of its orders, or none at all,
    `ReadDisagrees` (0148 item 8; `stage::TAKE_TRADES_OTHERS`), and a read not made is recorded and
    stops nothing. With
    no stop the account's stage three follows (FBC-4plj; `stage.rs`'s
    `Run::pages_steps`, `pager.rs`; decisions 0132 items 5 and 7, 0148 items 7, 8 and 10), run as
    the crossing session is under `Which::Pages`, after its taker IOC session, so only once its
    drop test passed and the exact-echo gate is open: every named account read clean (a provider
    rate refusal a stop, marked), its pages registry built, tripped and seeded flat; one session
    armed, resynced and Started under the market lease; then `pager::PLAN`: P1 and P2, page 0, P3,
    page 1, P1's cancel, pages 2 and 3, P2's and P3's cancels, then every page from offset 0 until
    a short one, a full page repeating an earlier one of the walk, a page not made or
    `pager::WALK_PATIENCE` (`Run::walk`, `pager::walk_end`). Each order is a post-only GTC at the
    minimum size `--away-bps` behind the touch, an fbc-oms authorization spent from the pages
    registry's own list (`Run::place_resting`), sent only while both USD caps admit it at that
    touch; each cancel is by venue id through the `Link` (`Run::cancel_paged`: an order already
    ended, or one whose acknowledgement has not come, left to the Stop and recorded so), waited
    on until its order ends or `--rpc-timeout-ms`; each place and cancel is compared with
    never-arming reads (`Run::compare_paged`). The pages come from the pager, a second socket that
    never arms or places, on the session's account and the guarded configuration, at a page size
    of 1 (`pager::PAGE_SIZE`), every frame charged to the shared limiter first and one without
    room not sent (`compare::Socket::page`, `compare::page_attempt`); each is recorded with its
    size and offset, the names of ours the streams showed open as it was asked and what it listed
    by name (`Run::page_now`, `pager::named`, `pager::PageRead`), and page 0 shows the order the
    pages list ours in (`pager::order_shown`). A provider rate refusal on the pager, a
    `RateLimited` err or a rise in the limiter's rejected count, is the run's stop,
    `RateRefused(Pager)`, marked, and so is a page listing an order that is none of P1 to P3, or a
    TWAP, `ReadDisagrees` (0148 item 8; `pager::came`, a walk so ended `pager::WalkEnd::Disagreed`),
    and so is a page listing one of P1 to P3 that the streams showed ended past its lag, as
    `compare::judge` and `compare::compare` rule (0109, 0151 item 4): once a comparison begun after
    the run saw it ended has ended, or `--agree-wait-ms` from then has passed (`pager::Ends`,
    `pager::ended_listed`); one within its lag is recorded (`PagesRun::ended_listed`) and stops
    nothing; each before the plan's next step; any other page not made is recorded and stops
    nothing. Each order's `ACK`, exact echo, `ACK` latency and err echo are read by its own client
    id (`tee::Kept::exact_ack_of`, `acked_of` and `err_echo_of`), never another order's in the
    same session (P1 to P3, the flatten IOCs; PR #284's review). A maker
    fill of a stage-three order is flattened while the session is open, then stops the run, as
    below. Then the Stop and the reads after it as after a crossing session (`AFTER_PAGES`, each
    stop said as met after the stage-three session), the pager's refusals counted as a read
    socket's (`<account> (stage three's pager)`). With no stop the run ends `Ok` after the last
    account's stage three, its line `FINISHED` (`stage::FINISHED`), its outcome ok when its files
    and records are whole. The flatten (FBC-ou9g;
    `stage.rs`'s `Run::flatten`, `flatten.rs`; decisions 0012, 0063, 0148 items 5, 6, 8 and 9, 0151
    items 5, 8 and 9) is reached from every registry: once a fill of ours other than a flatten IOC's
    (`places::Watched::expected`) reached a session, a maker fill of X or Y, a crossing post-only
    that fills or the taker IOC's fill, while it is still open and before its Stop, every other
    order of ours on the market is cancelled by venue id with no Kill (`Run::cancels_of_ours`) and
    seen to end, else no flatten IOC; the owner's Flatten puts the market in Exit
    (`places::flatten`, under the market lease the armed registry holds), then a wait of up to
    `--rpc-timeout-ms` for the session's place gate (`Run::place_gate`, decision 0058; its line,
    `stage::PLACE_GATE`, printed when the gate is shut), so a fill heard before its resync ended
    is flattened once it may place; a press of Ctrl-C the flatten, or a Stop's cancels after a
    session that ran its course, took is the session's stop when none came before it
    (`Stop::Interrupted`, 0148 item 8); then, while the account's one flatten budget lasts
    (`places::Allowance`, carried across its registries), with
    `stage_two::no_other_order` over every registry and each named account's latest read before the
    session (the order a read admits, X, taken off its counts, since its registry holds it; clean
    for the session to start, so the session's own account is held fresher by each comparison), its
    client id minted first and nothing awaited from that check to the place, one limit IOC at the
    minimum size on the reducing side, `reducing` with no reduce-only flag, priced within
    `--flatten-cap-bps` of a touch read just before it (`flatten::ioc_price`, capped when the grid
    holds no such price that still crosses it), built through the registry's one pre-trade path and
    judged by Exit and both caps, and sent only while both USD caps admit the registry's lot caps
    and the IOC at the touch it is priced from (`market::caps_admit`, since fbc-oms applies the caps
    to reducing orders too, 0013 rule 2; otherwise `Flatten(BeyondCaps)`, nothing sent or resized,
    0113), recorded as sent before its wait (`flatten::IN_FLIGHT` when the connection ends
    meanwhile); its end with its fills settled or its deadline (`flatten::Ioc`; one acknowledged
    and left open with no fill, `Open`, told from one unanswered, `Unknown`, each never resent,
    keeping the next back); a comparison after it, which shows REST positions flat once the
    registry is. The reads after a drop session know its flatten IOCs as ours beside X and Y, so
    one left resting asks the owner's hand cancel (`places::HAND_CANCEL_ONE_ORDERS` after drop
    session one). A fill of ours that reaches a session only during its Stop's cancels or
    after them is flattened then, before its close, and so is one that reaches a drop session while
    its graceful close waits for room with no cancels run yet, after the Stop's cancels it raises
    (`Run::flatten_late`, `flatten::AFTER_LATE_FILL`), and so is the taker IOC's own fill, which is
    expected, when its IOC left nothing to follow it as the session's steps ended and the fill then
    leaves the registry's position off flat (`Run::late_fill`, `taker::AFTER_LATE_TAKER`; it stops
    nothing once flat): the Stop's Kill lifted first, the market still armed under its lease (the
    Stop's disarm, `Run::stop_disarm`, comes after), and Killed again after; one that reaches it
    after that disarm, while the close waits for room, is flattened once there is room, the owner's
    Flatten taking the market lease again, and the market disarmed again after it. The touch's
    connection is read while the close waits and through the Stop's cancels, so a late flatten's
    touch is current and its age counts the waits. Its stops
    (`Stop::Flatten`: `BelowMinimum`, `Refused`, a client id not minted among them, `Capped`,
    `BeyondCaps`, `UsedUp`, `Unsettled` for a flatten IOC whose fills had not all reached the
    registry by its deadline, after which no IOC follows, `NotEnded` for other orders not seen to
    end, and `NoTouch` for no touch to price from; and a comparison's or `no_other_order`'s) replace
    the fill's as the run's (`flatten::session_stop`; a stop that came before them, the comparison's
    after the Stop's cancels included, is kept), each marked, so a flatten that does not run its
    course always stops the run, the taker session's included; Ctrl-C meanwhile is recorded; a drop
    session that flattened ends by a graceful close (0151 items 2 and 11). Each session's record
    carries its flatten (`flatten::FlattenRun`), which the report and the evidence give with the
    cap, the attempts, prices and sizes as fences met. The report and the evidence (`"ticket"`
    FBC-9sk2) give each named account's drop test (what answered each arm, whether enabling cancels
    open orders and whether QFEX reports it, the verdict, the drop's reach over X, whether a
    graceful close cancels, measured on a read of X's own account alone, X found there by its venue
    id once known whatever client id its row carries (`AllowedOrder::names`, apart from the stricter
    `AllowedOrder::matches` a read before a session admits by), and unmeasured on a full page not
    listing it; X absent counted cancelled only after a graceful close with no stop that cancels
    before it, X resting as it was sent, X's account flat and X's trades since its send read whole
    with no row since then, 0132 item 4, and otherwise not measured, its reason given
    (`places::close_verdict`); whether the other named accounts stayed clean (`places::OthersClean`,
    not known when a read of one could not be made), each order's historic-orders
    `terminal_status`, the members a trades row carries, what followed; then each session: each
    frame's client-id echo as a verdict, whether `add_order` went out as encoded, the stop's
    cancels, each comparison, the reads before and after the drop, what the session showed of X and
    Y as it dropped, not measured when it did not reach its drop, and whether X rested when the run
    ended, from the read of X's account after drop session two once that session was built:
    resting, absent, or not known), the place registries, each session's and read socket's refusals
    (`refusals.rs`, a session named by `SessionRun::label` on each named account, counted together,
    the reads after the drop under `AFTER_DROP_TWO`) and its resync's full pages, read on the traded
    spec (`pages::rows_on`), and stage two
    (`stage_two::StageTwo`, read from the whole run's tee once its sessions end: the gate; the
    characters our client ids exercised by class of QFEX's format, the rest named as not measured;
    whether QFEX accepted `add_order` as encoded, without `take_profit`, `stop_loss` or
    `reduce_only`; how `ACK`s, rejects, ends, fills and echoes gave back our client id; which
    statuses are rejects and which ends; then each crossing session, how its crossing post-only was
    refused among it, and after it the account's taker IOC session, how its IOC came back, the taker
    fee's sign, paid, zero or received, and the order of its frames, each session saying whether it
    reached the check for another order of ours, not checked when it ended first, and whether the
    reads after it listed an order or a TWAP not ours or a position not flat, not known when one
    could not be made), and what the run declares and leaves the owner (`taker::markdown`,
    decision 0148 item 10, 0151 item 10: the key `qfex.exec.production_places =
    probe_place_stages`, each registry's `CodProbeRun` declaration, the caps, the distance, the
    flatten cap and the attempts as fences met and never values, the taker fee's sign by account,
    `--sole-trader` and the owner's confirmations as unchecked, and every hand action: the
    `--reconciled` declaration and the hand cancel or hand close the last reads leave,
    `taker::hand_actions`, not known when a named account was not read or its last read could not
    be made, `taker::hand_unknown` on `Evidence::last_unread`); its slices are `drop-production/`,
    both sessions of each account's drop test, `places-production/`, the crossing and taker IOC
    sessions (`stage::PLACES_SLICE`, `capture::split_crossing`), and `pages-production/`, the
    stage-three sessions and the pages each pager read, `<account>-the-pager-<n>.json`, which no
    journal holds (`stage::PAGES_SLICE`, `capture::split_paging`, `capture::pager_files`; the
    report and the evidence give stage three's pages by size, offset and name alone,
    `pager::markdown`), each scrubbed by `Scrub::Places`, each fee kept as its sign alone, with a
    README of its own. The report and the evidence (`answers_0132`) also answer decision 0132's
    list for each named account (FBC-4plj, 0148 item 10; `answers.rs`), each item in words or as
    fences met from what the run showed, never an id, a price, a size or an amount, and not
    measured, saying why, where it did not, the items read from the whole run's frames saying so,
    and an item read from pages or a trades history not all read saying it is of those read alone,
    never none or agreeing from them (a trades read not whole named by when it was made and why):
    what answers an arm, whether enabling cancels open orders and whether QFEX reports it, the
    drop test, whether a graceful close cancels, whether the other named accounts stayed clean,
    the client ids at the format's full length (`stage_two::FullLength`) and in every character,
    whether `ACK`, rejects and fills echo them exactly, `ACK`'s latency for post-only and other
    places from each `add_order`'s send on the journal's clock and against the 500 ms speed bump
    (`stage_two::Acks`, `tee::Kept::ack_latencies`), the statuses and the crossing post-only's
    refusal, each order's `terminal_status`, the kind of a trade's `timestamp` and whether its
    `trade_id` is the fills channel's, the side fields a trades row carries and whose side they
    are, the pager's page boundaries, what REST's `open_orders` counts against the orders a whole
    page listed on each account read, the comparisons' lag, `add_order` as encoded, the IOC's frame
    order, the taker fee's sign, each resync's orders against its own `GET /user/positions`'
    `open_orders` from the session's journal (`pages::Pages::orders`,
    `capture::SessionRun::resync_open_orders`, `capture::open_orders`), each resync's full pages,
    the trades rows naming an order the codec did not place, the refusals by session and read
    socket, and the fill ledgers (`ledger::PlaceRun::said`), beside the environment, the accounts
    as named, and the declarations and unchecked confirmations of `taker::markdown`. Held in
    `tests/qfex_preprod_probe/place_stages.rs` and `compare_reads.rs` (stage three's pure parts in
    `pages_run.rs`, the report's in `answers_run.rs`), against a QFEX model in
    `tests/qfex_preprod_probe/model.rs` on the conformance stub's serving scripts (0150), whose
    trade sockets learn each connection's account from its authentication, answer the arm,
    `add_order`, `cancel_order` and the two pages, cancel an armed account's orders on a graceful
    close or a drop as their knobs say, and take the knobs `ack_delay`, `rate_limit`, `stray_order`,
    `cid_absent_update`, `stray_fill`, `maker_fill`, `server_close`, `rest_lag`, `rest_fails`,
    `cancel_delay`, `close_on_place`, `arm_err`, `foreign_order`, `position_on_place`,
    `position_after_close`, `hold_on_place`, `cancel_place` (on a given place, `cancel_place_on`),
    `venue_cancel`, `close_on_cancel`, `stray_after`, `part_fill_after`, `close_after`,
    `fill_on_trades`, `fill_after_close`, `part_fill_after_close`, `trade_after_close`,
    `trades_err`, `historic_fails`, `touch_moves`, `hold_trades`, `trades_err_page` and
    `hold_trades_page` (the run's `n`th `get_user_trades` page answered an err or held; FBC-4plj),
    `netting_on_trades_page` (two trades of an order no run placed, one each way, as the run's
    `n`th `get_user_trades` page is asked), `stray_on_pager_page` (an order not ours, a TWAP, or
    the order cancelled last as if it rested, on the `n`th page at a limit of 1, in that answer
    alone; `model::PagerStray`),
    `page_order` (oldest or newest first; FBC-4plj, which reads its pages at a limit of 1) and
    `rate_limit`'s
    `Limited::PagerPage` (the `n`th page asked at a limit of 1, stage three's pager's), and, for
    drop session two
    (FBC-77r8, held in `tests/qfex_preprod_probe/drop_two.rs`), `arm_cancels_open` (no, reported,
    reported late, silent), the cod knobs `cod_on_close`, `cod_on_drop` and `cod_reach` (the
    account, its own connection, or the earlier ones), `foreign_after_close`, `ack_delay_on`,
    `fill_at_arm`, `arm_err_on`, `on_ack` (a venue cancel or a maker fill after one `ACK`),
    `fill_on_drop` and `historic_omits_last` (and `on_ack`'s maker fill holding the read sockets'
    pages until the placing connection's client writes again or ends, as `stray_order` holds
    them), and (held in `place_stages.rs` alone) `silent_auth`, `trade_on_drop` (two trades of an
    order the model never had, one each way, as a connection drops),
    `crossed_on_place` (a crossed touch pushed on the MDS), `touch_moves_at_arm` (the touch moved
    at a later arm), `stop_order_after_close` (an open order REST counts and no page lists, once a
    session on an account the model placed nothing on ended), `fill_on_trades_page` (X filled as a
    later `get_user_trades` page is asked), `Limited::RestAfterDrop`, `Limited::TradesPage` (a
    later `get_user_trades` page refused), `Limited::HistoricAfterDrop` (the first historic-orders
    read after a drop refused), `foreign_order_on` (an order no run placed beside a later place),
    `position_on_place_on` (a position moved with a later place), `foreign_on_drop` (one left as a
    connection drops), `twap_after_close` (a TWAP listed once a connection ended) and
    `after_drop_historic` (a position, an order not ours or a TWAP once the first historic-orders
    read after a drop is answered), and, for stage two
    (FBC-su9i, held in `place_stages.rs` and `tests/qfex_preprod_probe/crossing.rs`), `ack_echo`
    (exact, upper-cased, escaped, absent, null, empty), `alo_cross` (a post-only through the touch
    answered `CANCELLED` by default, an unmapped status, an err, an ACK then a cancel, a taker fill,
    or, unset, resting), `cancel_misses` (every cancel answered `NO_SUCH_ORDER`, the order left
    open, or each from a given one, `cancel_misses_from`), `cancel_lies` (every cancel answered
    `CANCELLED`, the order left open), `silent_after_cross` (the primary account's
    authentications left unanswered once a given crossing order arrived) and `hold_on_cancel` (the
    shared budget held as the run's first `cancel_order` is read, as a provider rate refusal
    echoing nothing would) and `after_cross_end`
    (a position, an order not ours or a TWAP once the crossing session's connection ended), and,
    for the flatten
    (FBC-ou9g, held in `place_stages.rs` and `tests/qfex_preprod_probe/flatten_run.rs`), `ioc`
    (every limit IOC filled whole against the touch it crosses by default, one lot of it, expired,
    refused by an err, unanswered, answered by a server close, or acked and left resting),
    `ioc_sells` (each sell limit IOC, the flatten IOC after a taker buy, answered apart when set),
    `hold_ioc_sell` (the first one's answer or close held for the test's release) and
    `rate_with_ioc_sell` (a `RateLimited` err echoing nothing sent with it), for the taker IOC
    (held in `place_stages.rs` and `tests/qfex_preprod_probe/taker_run.rs`)
    `after_take_end` (a position, an order not ours or a TWAP once the connection the run's first
    limit IOC arrived on ended), `fee_sign` (paid, zero or received) and `ioc_frame_order` (the
    `fill_response` or the final `order_response` first, or the `fill_response` withheld),
    `position_push` (a `position_response` before or after a fill's frames), `touch_on_place` (a
    touch pushed on the MDS before a place's answer), `fill_on_cancel` (an order filled as a maker
    as its cancel arrives; with `fill_on_cancel_after`, its `fill_response` pushed later; with
    `fill_behind_cancel`, the cancel answered `CANCELLED` and the order's maker fill reaching the
    session after that answer), `fill_after` (the first place filled whole as a maker later),
    `touch_after` (the touch pushed again on the MDS later), `fill_other_on_place` (another order
    filled as a place arrives, the place left unanswered), `rest_lag_from` (`rest_lag` held to the
    changes from a given place on), `ioc_order_after` (a filled IOC's final `order_response` pushed
    later), `ioc_acked` (every limit IOC it fills answered `ACK` first), `fill_in_resync` (an arm's
    fills sent before its connection's next `get_user_orders` page), `resync_page_held` (that page
    held for the test's release, `Venue::release_held`), the read sockets' pages held after a
    `maker_fill` the reads lag until its connection's client writes again (`knob_frames_unread`),
    and `Limited::ReadSocketAfterIoc` and `ReadAuthEchoAfterIoc` (a refusal counted from the run's
    first limit IOC), and, for FBC-gisx, `page_after_session` (the first `get_user_orders` page a
    read socket asks for once an armed connection ended answered with a frame that is no page, or
    by a server close), `page_after_ioc_end` (the same, once the connection the run's first limit
    IOC arrived on ended), `rest_fails_after_drop_reads` (once an armed connection dropped, the
    first `GET /user/positions` after a historic-orders read answered HTTP 500),
    `rest_fails_after_historic` (the first `GET /user/positions` after the run's first
    historic-orders read answered HTTP 500: the read before drop session two, however many reads
    the comparison after X's place made) and
    `twap_elsewhere_after_close` (once an armed connection ended, every other account's pages
    listing one TWAP), and, for FBC-8mwz, `silent_sub_after_drop_reads` (once an armed connection
    dropped and a historic-orders read came, the first authentication naming a subaccount left
    unanswered, once) and `stop_order_after_drop_reads` (from then on, every subaccount's
    `GET /user/positions` counting one open order no page lists), and, for FBC-kllr, `mds_pings`
    (the MDS reached through a relay of the test's own, `tests/qfex_preprod_probe/mds_pings.rs`,
    that pings each connection on the runtime's clock, streams the touch before each ping and closes
    a connection whose ping goes unanswered too long) and `hold_page` (the next read socket's page
    held for the test's release, set as the probe prints a line), and, for FBC-8o7a, `endless`
    (every read socket's `get_user_trades` page, or every historic-orders page, full of rows made
    up from its own offset and limit, without end), all declared by path; eleven paths
    no flag or answer reaches, X spent from its list, X not priced, Y not priced (FBC-gisx), a
    session failing after its order's answer,
    the journal past its soft limit, another order of ours held in a cross registry before a
    crossing order or a taker IOC, the account's flatten budget spent by its drop registry on the
    allowance its later registries are handed, the flatten's market Killed before the owner's
    Flatten or an IOC, a named account's read listing an order as the flatten judges an IOC, and the
    flatten's client id refused, are a test's `probe_args::TestHooks`, compiled into the test binary
    alone. The probe's credentials come from the environment through `src/auth_qfex.rs`, a review
    path, which also hands each signed read to the connector; the probe reuses
    `paradex_testnet_acceptance`'s privacy patterns by path, and `src/testnet.rs` (the caps'
    conversion to lots and the lease directory) by path. Every sample that trades, the probe
    included, routes its sessions through `fbc_live::link` and its `LinkHandler`, whose wake is a
    boxed closure; the re-export shim the probe included by path is gone (FBC-bddu, 0155 item 9).
    They share `src/testnet.rs` (guard and wiring), `testnet_trade`, `testnet_quote` and
    `mainnet_smoke` `src/positions.rs` (`PositionCheck`, their own `GET /positions` read over
    `src/auth_rest.rs` after the first fill and at Stop, judged by fbc-live's `PositionWatch`, and
    the other-markets refusal, `other_markets`; the `POSITION` lines, `HAND_CHECK` and the Stop
    text, `failure`, stay theirs; FBC-rujq; `paradex_testnet_acceptance` includes it and runs none
    of it), and the mainnet ones `src/mainnet.rs` (the mainnet guard), included by path.
  - `crates/venues/fbc-venues` (the registry, the only crate with a normal dependency on a
    concrete venue; `crates/fbc-examples` reaches them only as dev-dependencies),
    `crates/venues/fbc-venue-binance-usdm` (market data only:
    `exec: None` caps citing Binance's USD-M pages, `plan_md` on the `/public` combined-stream
    endpoint, live SUBSCRIBE/UNSUBSCRIBE, `bookTicker` touches, partial-depth snapshots, and
    the diff-depth book in `src/diff.rs`, anchored on a `GET /fapi/v1/depth` snapshot with
    `pu` continuity, duplicates ignored, a gap resynced from a new snapshot and a failed
    snapshot retried on a configured timer), `crates/venues/fbc-venue-paradex`
    (so far its signer: the SNIP-12 revision 0 hash and Stark-curve signature, tested against
    `fixtures/paradex/signing/paradex-vectors.tsv`; its authentication in `src/auth/`: the
    `/auth` login signed from `Secrets`, the session token read, named for the journal and sent
    only redacted, the refresh timer apart from the signature's expiry, and Test Connection as a
    two-round plan, 0048; and market data: an SBE reader gated on each
    frame's block lengths, bbo and trades decoded into touches and trades, the order book
    deltas into book events with seq_no continuity (0022), held to the hand-built frames and
    captured public frames and sessions in `fixtures/paradex/md/` (the bare `deltas` and
    `interactive_deltas` channels, 0074); the REST `/orderbook` snapshot at depth 15
    (`src/md/rest.rs`); order entry's `ExecCaps` with each value cited and the undocumented ones
    declared conservatively (`src/exec/`, 0054), the snapshot source trustworthy on FBC-8xr's
    testnet evidence (0094); the private `OrderEvent` at SBE 1:2 decoded
    into order updates, a modify's SUCCESS an amend and its REJECTED an asynchronous reject,
    each reported once per order and request, a later event repeating it the order update alone
    (`ModifyRequests`, kept by the codec, 0086; bounded to each order's latest outcomes, 0091)
    (`src/exec/order.rs`, held to the hand-built
    frames in `fixtures/paradex/exec/`); the private `FillEvent` decoded into fills keyed
    `<fillType>:<fillId>`, FILL and RPI an order's execution at its grid price and
    LIQUIDATION, UNWIND_TRANSFER, SETTLE_MARKET and BLOCK_TRADE the venue's own at the exact
    price it states, off the grid included (`src/exec/fill.rs`, 0120); the REST
    resync (`GET /orders`, `GET /positions`) and the order query by client id
    (`GET /orders-history`), as requests carrying only the caller's headers and as plans, their
    answers decoded whole into the `Resync*` events and a `QueryResult` (`src/exec/rest.rs`, held
    to the hand-built `rest-*.json` responses in `fixtures/paradex/exec/`); the read-only
    private-stream codec, an `ExecCodec` that logs in, authenticates its socket, subscribes the
    private channels once per connection, decodes them, resyncs over REST with the token in a
    redacted header and refuses every command (`src/exec/read_only.rs`, 0061, 0071); its
    order-entry commands encoded as the socket's signed JSON-RPC frames (`order.create`,
    `order.create_batch`, `order.modify`, `order.cancel`, `order.cancel_batch`,
    `order.cancel_all`, `order.cancel_on_disconnect`) from the command and `EncodeCtx` only
    (`src/exec/encode.rs`, `ParadexEncoder`); the order-entry codec, `ParadexExec`, which puts
    the read-only codec's connection, the encoder, the replies (`src/exec/reply.rs`, 0069), the
    resync and the order query together on one connection (`src/exec/codec.rs`, 0071), a
    placement's final phase given by its order events (`src/exec/placements.rs`, 0092); the
    factory's order entry: `caps()` with the `ExecCaps`, one order-entry endpoint on SBE 1:2,
    and the order-entry or read-only codec as `paradex.exec.mode` says, its order signer from
    `auth::order_signer` so no credential is read outside `src/auth` (`src/factory.rs`, 0072);
    the offline rehearsal (`tests/rehearsal.rs`, FBC-8mv): fbc-oms, the order-entry session and
    this codec against the conformance stub, the market seeded by the trustworthy resync with
    no hand seed (0094), every record the log facade emits read at TRACE; the named conformance suite
    (`tests/conformance.rs`, FBC-6oj) on `ParadexFactory`, its fixtures in
    `fixtures/paradex/conformance/` and what they assume in `tests/conformance/setup.rs`; the
    SimVenue stand-in (`tests/sim_stand_in.rs`, FBC-2zxk, `fbc-sim` as a dev-dependency so
    `fbc-sim` names no venue): a SimVenue built from `caps()` places, amends, cancels and fills
    (0102); the clustered-move benchmark (`tests/clustered_move.rs`, FBC-vgm, design §5.3):
    fbc-runtime's shard host runs Paradex market data from a reference server of the test's own
    (tokio's `net`, `tokio-tungstenite` and `futures-util` as dev-dependencies) beside this
    codec's order entry against the conformance stub, each protective cancel written after at
    most one signature in progress, and on Linux reports their kernel-receive-to-write-done p99
    (0100); and, in
    `tests/oracle/` with `fbc-book` as a dev-dependency only, the
    BT-401 book and bbo-touch agreement checks), and `crates/venues/fbc-venue-hibachi`
    (market data only, FBC-jyyl: `exec: None` and `Readiness::Record` caps in `src/caps.rs`
    citing Hibachi's docs, its SDK and the Java library: the `orderbook` channel at the required
    `hibachi.orderbook_depth` (1 to 100, no default), `Cadence::Unknown` (the owner's decision of
    2026-10-25, support's 250 ms cited as not taken), `Continuity::Unsequenced`, windowed and
    bracket-only, its levels buckets of the quote grid (FBC-jwn9 adds the capability), the
    funding stream stating its one-hour interval and no next time (0131), one-second timestamps,
    the per-IP REST limit and the taker speed bump; discovery in `src/discover.rs` from
    `GET /market/exchange-info`: the contract id
    as native id, `tickSize` as price grid and the finest listed granularity at or above it as
    quote grid where it is coarser (`None`, fbc-core's meaning, where it is the tick or there is
    none: quoted on the tick), the sizes,
    `SigningParams::AssetDecimals` and a session calendar from the advertised open and close,
    each published `maintenanceWindow` as a `ClosedWindow` on every contract's calendar (begin
    inclusive, end exclusive, rounded toward closed; a bound not decimal Unix seconds, a number or
    a string, no later than 2100, or an end not after its begin, refusing the answer naming
    `maintenanceWindow`; FBC-ord5) and, for the FX underlyings the docs name, the New York
    weekend (0125); only `LIVE` on a
    `NORMAL` exchange trading, every other listed status `Halted`, an absent, null or
    undocumented one refused; `FUNDING_SETTLEMENT_INTERVAL` one hour and every perpetual's
    funding `FundingSpec::Known` at it, capped at `FUNDING_RATE_CAP`, 125 bps an hour (the docs'
    -10% to 10% read for eight hours and rescaled), on the owner's conclusion of 2026-10-28
    (FBC-204c, 0131); `X/USDT` is `X/USDT-P`; held to the hand-built
    `fixtures/hibachi/rest/`; and, in
    `src/md/` (FBC-2kmp, 0129), the `/ws/market` codec `HibachiMd`: `plan_md` one connection to
    `/ws/market` on the required `hibachi.data_ws_url` origin, one frame per subscription under
    the `parameters` key charged to its instrument, each orderbook naming `granularity`
    (`quote_grid.unwrap_or(tick)`) and `depth`; the `orderbook` applied to the codec's own copy
    and bounds-checked before any event (each bounded side's best at `startPrice`, exactly the
    depth's levels within min..max with the last at `endPrice`, none outside, not crossed, a
    pruned level the frame does not zero failing; a locked book and a thin side counted apart),
    a side's window a `SidePrune` and a null side a `SideClear` with the removals of both sides
    before any set (0124), and every gap it finds itself (an empty Snapshot, an absent or
    unbounded side, a frame failing the check, a granularity or depth not the subscription's, a
    level off its granularity) a `Health` gap of that book alone, recovered after the required
    `hibachi.orderbook_resubscribe` by the codec's own unsubscribe and subscribe, then one paced
    reconnect if no Snapshot answers; `ask_bid_price` a Touch only when fully sized and not
    crossed, a stale one gapped on its touch feed; both `trades` shapes, `mark_price`,
    `spot_price` as the index and `funding_rate_estimation`'s eight-hour estimate
    (`FUNDING_ESTIMATE_PERIOD`) as its exact eighth per hour in 1e-12 units with the one-hour
    interval, one whose eighth is finer refused and never rounded, and no next time (0131); what
    it counts on `HibachiMd::counts`, and its own copy of each book,
    which the book built from its events equals, on `HibachiMd::book_copy`; held to the hand-built
    `fixtures/hibachi/md/` (`tests/md_book.rs`, `tests/md_topics.rs`), the named conformance suite
    on `fixtures/hibachi/conformance/` (`tests/conformance.rs`), and a stub test on a paused clock
    with decoder replay of its journal (`tests/md_stub.rs`, `fbc-runtime`, `fbc-conformance`,
    `fbc-journal`, `fbc-book` and tokio as dev-dependencies); and, in `src/status.rs` (FBC-1632,
    0130), the status poll `HibachiStatus`: `STATUS_STREAM`, a poll endpoint (0027) on the REST
    origin carrying the same subscriptions, each instrument they name a market, that `plan_md`
    plans for every set, beside `/ws/market` or, for the empty set, alone and carrying none, so it
    and its codec stay open while the venue's market data runs; `GET /market/exchange-info` sent
    from a timer every required `hibachi.status_interval` (whole seconds, zero refused), each
    answer judged at the wall time of a timer asked for once it arrives, read by discovery's own
    status mapping, timestamp parsing and `maintenanceWindow` reading (decimal Unix seconds, a
    number or a string, none after 2100); each market's status reported as `MdEvent::Status` once each time it changes,
    every market halted while a read fails or is refused, a market a read leaves out halted, a
    market halted between reads on a timer at a published window's begin, its calendar's next close
    and its advertised close (the schedule the endpoint's, whichever markets are subscribed: an
    answer replacing only those judged no later than its read's send, and of answers whose spans
    overlap, in either order, the earliest close, the latest open and every window kept), trading
    only on a read listing it `LIVE` on a `NORMAL` exchange sent after every such halt and after it
    was subscribed, after no advertised open and judged before any close (a read held back only by
    such a halt or the subscription changing nothing), and halted for the required
    `hibachi.auction_halt` after each reopening until FBC-vg2n, a scheduled one or one after a halt
    no schedule announces (from the judged time of the first read that can lift a market to show it
    trading after one showed it halted by status, left it out, or failed or was refused, or ended a
    halt by an advertised close or open other than through an open advertised after it and no
    earlier than every instant a close or an open showed it halted, or after it was subscribed once
    the reads had begun, that is once the poll had sent a read, for the first time or again), so
    never reported trading while such an auction may still run, except one after a halt nothing
    announces and no read shows when no read fails, is refused or leaves the market out, and the
    market is not subscribed once the reads have begun, from its begin until a read reports it
    trading; its caps' `MdCaps::status` is `FeedSource::Poll`; held to the hand-built
    `fixtures/hibachi/rest/status/` driven as the runtime drives it (`tests/status.rs`), to a seeded
    randomized oracle written from the guarantee, its exception and the window, against venues
    drawn at random (`tests/status_oracle.rs`), and in the runtime against the conformance
    stub on its own thread, on a paused clock, with decoder replay of its journal, a consumer's
    handler passing each status into fbc-oms's planner, and `MdVenue` opening the endpoint on an
    empty plan and keeping it and its codec open while every subscription is removed and one taken
    back (`tests/status_stub.rs`, `fbc-oms` as a dev-dependency);
    and its offline signer, FBC-8mzv:
    `src/sign` (a review path) packs a place, a modify (as a place, under its own nonce), a
    cancel by order id or placement nonce and a cancel-all big-endian as Java's
    `HibachiPayloadPacker` and the SDK do, and the cancel-on-disconnect arm's nonce as
    cancel-all's (neither Java nor the SDK signs the arm; FBC-xvtr's probe decides which form
    Hibachi takes), the contract id from
    `native_id` and the decimals from `SigningParams::AssetDecimals`, a price off the tick or a
    quantity off the step refused before packing, quantity and max fees scaled exactly (fees x
    10^8 only) and the price truncated toward zero, every field at most 2^63 - 1, the JSON-only
    fields never packed, max fees an input of each place and modify; and signs SHA-256 of the
    payload with ECDSA over secp256k1 (k256, pinned exactly), its nonce RFC 6979, r || s || v
    with v 0 or 1, no HMAC path and no client-id cancel; `src/auth` reads the key from `Secrets`
    and hands over an initialized `HibachiSigner` (`auth::order_signer`), which no codec calls
    yet (FBC-x7bi); held to the Java oracle's vectors and the docs' worked examples in
    `fixtures/hibachi/signing/`, with no key or signed payload in a log line or error
    (`tests/no_leak.rs`)).
  - Dependency direction (design §3): `fbc-core`, `fbc-book`, `fbc-oms`, `fbc-journal` and
    `fbc-sim` never depend on a venue crate; a venue crate depends on `fbc-core` and protocol
    crates only. Nothing here depends on the private consumer. `scripts/check-deps.sh` holds
    every crate outside `crates/venues/` (`fbc-runtime` and `fbc-conformance` included) to the
    first rule, in normal and build dependencies (not dev-dependencies, which venue tests use).
- Tests: unit tests beside the code (`#[cfg(test)]`), integration and property tests in
  `crates/<crate>/tests/`, `trybuild` compile-fail tests for the seals in `fbc-core`'s tests, and for the
  authorization, the permits, the probe declaration and the retention configuration in
  `fbc-oms`'s.
- Fixtures in `fixtures/<venue>/`: recorded frames, journals and signing vectors. Synthetic
  key- or address-shaped values sit in a directory of their own carrying a `SYNTHETIC` file
  that says where they come from (0009); that marker exempts only its own directory from the
  privacy scan, so recorded frames never go in it. `fixtures/paradex/signing/` already holds
  the signing benchmark and the Java hash oracle the Paradex signer is checked against;
  recorded Paradex frames go elsewhere under `fixtures/paradex/`; `fixtures/paradex/md/` holds
  SBE frames hand-built from Paradex's published schema, and one captured public book frame
  whose provenance its README gives; `fixtures/paradex/exec/` (marked `SYNTHETIC`: each frame's
  account is 32 made-up bytes) hand-built private `OrderEvent` frames and REST order and
  position responses; `fixtures/paradex/rest/` a hand-built `/orderbook` response
  in Paradex's documented shape. `fixtures/binance-usdm/` holds hand-written frames (`md/`) and REST
  responses (`rest/`) in Binance's documented shapes, its README citing the pages.
  `fixtures/hibachi/rest/` (marked `SYNTHETIC`) holds hand-built `/market/exchange-info` answers,
  with a variant for each status and timestamp refusal and one publishing two maintenance windows,
  and a funding-history answer, its README citing each field's source;
  `fixtures/hibachi/rest/status/` (marked `SYNTHETIC`) the status poll's answers, each a change of
  three of those contracts at instants its tests' timelines name (a pause, either maintenance, a
  window as numbers, as strings and in milliseconds, an advertised close, a market left out, each
  refusal), its README saying what each pins;
  `fixtures/hibachi/md/` (marked `SYNTHETIC`) hand-built `/ws/market`
  frames, one per file, shaped as Hibachi's docs, SDK and the Java library show them and as the
  owner's capture showed in words only, its README citing each shape; and
  `fixtures/hibachi/conformance/` (marked `SYNTHETIC`) the conformance suite's cases built from
  them.
  `fixtures/hibachi/signing/` (marked `SYNTHETIC`) holds the Java oracle
  (`HibachiSignOracle.java`, `run-oracle.sh`) and the vectors it wrote on a synthetic key, its
  README saying how it was run and which buffers Hibachi's docs print.
  `fixtures/qfex/private/` (marked `SYNTHETIC`) holds hand-built QFEX trade-socket frames in the
  shapes its trade AsyncAPI gives (an order, a fill, a page of open orders, a position, a
  balance and a stop order) and answers to `GET /user/subaccounts` and `GET /user/positions` in
  its REST reference's shapes, every id, `user_id` and value made up, its `SYNTHETIC` file
  citing each shape.
  `fixtures/qfex/exec-recorded/arm-production/` holds the frames QFEX production sent on the
  trade sockets of stage one's two sessions (FBC-291a), as the probe captured them: identifiers
  blanked to `2`s, every number of a frame that echoes no request zeroed, and each `symbol`
  replaced by one fixed placeholder after the run. The run's `report.md` sits beside them, and
  the README says what answered the arm on each account. Being recorded, it carries no
  `SYNTHETIC` marker.
  `fixtures/qfex/exec-recorded/hold-production/` holds the reports and evidence of the probe's
  liveness hold on production (FBC-sbbl, 0146 item 6): an hour-long hold at its top level, and
  in `first-run/` an earlier one that a server close ended during a QFEX outage. It holds no
  frame. Its README records the pings, the closes and what was not measured, and
  `tests/hold_production.rs` in `fbc-venue-qfex` holds that README to both runs' evidence. Being
  recorded, it carries no `SYNTHETIC` marker.
  `fixtures/licence-gate/` is a standalone two-crate workspace the licence gate's self-test
  runs against (0017); `fixtures/dep-direction/` a standalone workspace of empty crates in this
  layout, with three forbidden edges, that the dependency-direction check's self-test runs
  against. `fixtures/journal/` holds journals `fbc-journal` wrote in an older
  format version, from synthetic records, which later readers must still read; its README says
  how each was written. `fixtures/conformance-toy/exact_replay/golden/` holds exact replay's
  golden (FBC-qwq): a conformance-toy order-entry session's journal as `tests/exact_replay.rs`
  in `fbc-runtime` recorded it against the test's local stubs, the configuration and request ids
  its replay is given (`session.txt`, the REST credential blanked as the journal keeps it) and a
  rendering of every frame and request the journal holds (`outbound.txt`); it holds no key- or
  address-shaped value, so it is not marked `SYNTHETIC`, and it is rewritten only under
  `FBC_REFRESH_GOLDENS=1`, which `scripts/check.sh --refresh-goldens` sets for that one test and
  the check otherwise unsets (its README says how).
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
  consumer runs it. The owner-run samples in `crates/fbc-examples` are example targets, run by
  hand (`cargo run -p fbc-examples --example md_watch -- --help`); nothing restarts them.
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
key- and address-shaped hex, PEM headers and master-key bytes, and, by path alone, editor-history
copies and environment files, which `.gitignore` ignores; 0009), then the external
reviewer's offline proof (`scripts/deepseek-review-tests.py`, below), then, once `Cargo.toml`
exists, `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
the golden refresh's self-test (`scripts/golden-refresh.sh --self-test`, below), the golden
refresh itself only with `--refresh-goldens`, then
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
depends on no workspace crate but `fbc-core`; dev-dependencies are not checked; design §3;
and the resolved graph holds one `log` package, the one tungstenite logs through and
fbc-runtime caps; 0079),
then the coverage
ratchet (`scripts/coverage.sh`, workspace line coverage from cargo-llvm-cov, against
`coverage-floor.txt` with 0.2 points of slack; the workspace ticket records the first floor
with `scripts/coverage-ratchet.sh --set`), and last the "golden unchanged" step (below).
`scripts/check.sh --fast` skips the ratchet. On the
empty repository it takes about four minutes, almost all of it the loop's
self-tests. cargo-llvm-cov 0.9.1 is installed once by hand (`rustup component add
llvm-tools-preview`, `cargo install cargo-llvm-cov --version 0.9.1 --locked`); CI installs the
same version itself, and moving it is a ticket, like the toolchain. cargo-deny is required the
same way on every machine that runs the check, fast or full (0017): install it once by hand
with `cargo install cargo-deny --version 0.20.2 --locked`; CI installs the same version, the
licence step fails naming the install command when it is missing or another version, and
moving it is a ticket. The golden-refresh flag, `scripts/check.sh --refresh-goldens` (it
combines with `--fast`; FBC-qwq, 0006, 0142 item 12), rewrites exact replay's golden
(`fixtures/conformance-toy/exact_replay/golden/`) through `scripts/golden-refresh.sh refresh`:
before the tests it runs `fbc-runtime`'s `tests/exact_replay.rs` test that records the
conformance toy's session afresh with `FBC_REFRESH_GOLDENS=1`, the one variable under which that
test replaces the committed golden with the fresh one, set for that command alone, and fails
unless every file of the golden was rewritten (a renamed test, which cargo runs as no test,
included). The check unsets that variable first, so a shell that exported it rewrites nothing,
and at its end, with or without the flag, the "golden unchanged" step
(`scripts/golden-refresh.sh unchanged`, against the checksum `sum` took before the tests) fails
when any step after the refresh changed the golden. Without the flag, the tests replay the
committed golden as it stands. `scripts/golden-refresh.sh --self-test`, which every run of the
check runs, proves on a golden of its own, the test stood in for by a script that writes only
under that variable as its store gate does, that the refresh passes only when it rewrote every
file, which the stand-in does only when it sees the variable at 1, and that the self-test's own
shell, which runs that refresh and not a subshell, holds no such variable once the refresh
returns; that the refresh fails on one file left, none rewritten, a failed command or an
emptied golden; and that the checksum sees a changed byte and an added, removed or renamed
file; it then fails when `tests/exact_replay.rs` no longer holds the refresh's test under its
name. A `cargo test` run outside the check with `FBC_REFRESH_GOLDENS=1` rewrites the golden
too, and the next check's tests replay what it wrote.

### External model review (Codex; 0127)

The active external model gate is a completed Codex code review of the pull request's full,
current head SHA. The trusted identity is `chatgpt-codex-connector[bot]` with GitHub user type
`Bot`; a look-alike comment from any other account is not evidence. Request `@codex review`
for the selected candidate when the current head lacks a completed review. Before independent
acceptance and again at the final gate, run:

```bash
python3 scripts/codex_review_evidence.py --repo <owner/repo> --pr <number> --head <full-head-sha>
```

The helper reads every issue-comment page and accepts only the trusted bot's latest Codex
summary with a `codex-review-state:v1` marker naming the exact full head, `code_review.status`
`completed`, a known findings state, and `has_unresolved_findings` false. A newer review request,
queued or running state, stale head, malformed marker, unavailable API, or look-alike holds the
candidate. A pass means only that the external review completed: read its receipt, findings,
author replies and all review threads. Fix P1 and P2 findings with proof or answer each with
evidence; fix or answer P3s. Confirm no unresolved conversation or requested changes. The
independent reviewer still checks the whole diff, acceptance criteria and local proof; final-head
CI and all six `pr-readiness.sh` gates still apply. A model review is advisory and reads
untrusted pull request text.

If the trusted Codex service cannot complete a review of that head, hold the PR and report the
cause to the owner. An alternate external reviewer requires the owner's explicit per-PR direction
or a new decision record; independent acceptance alone does not waive this gate. Decision 0127
records the owner's #205 exception and makes this path the standing policy. Do not enable the
DeepSeek workflow as a side effect of this change.

#### Dormant DeepSeek workflow

The 2026-10-05 policy used DeepSeek while Codex credits were unavailable. The DeepSeek workflow
is now manually disabled; its code and offline tests remain for a deliberate future activation.
When enabled, `.github/workflows/deepseek-review.yml` runs
`scripts/deepseek-review.py` on every push to a ready pull request from this repository's own
branches (`pull_request` opened, ready_for_review, synchronize and reopened; drafts and forks
run nothing, and there is no `pull_request_target`). A new head cancels the run for the old one.
The script sends the diff against the base, the full text of each touched file, this file's
Project rules and the decisions index to DeepSeek's OpenAI-compatible chat completions endpoint,
splitting a change over the token budget into several requests and merging their findings, and
posts one comment headed `DeepSeek review` that names the reviewed head SHA and lists findings
`DS-1`, `DS-2`, ... with severity P1, P2 or P3, file, line, problem and fix, or says there are
none. Files that did not fit are named under **Not reviewed** (or **Reviewed from the diff
only**); a complete comment covers the rest of the change, and the independent reviewers, who
read the whole diff, cover those files as they cover every file. An answer cut off at the
output allowance (`finish_reason` `length`; a thinking model's `max_tokens` counts the
reasoning) is not final: that part is reviewed again as two halves of
its files, or a single file from its diff only, up to `DEEPSEEK_SPLIT_DEPTH` (default 2) times,
and the comment names the parts so split (FBC-yf0j). An answer that is not the asked JSON
although it was not cut off (`finish_reason` `stop`) is asked for once more, with the same
prompt and an instruction to answer with the JSON object only, and the comment names the parts
so asked (FBC-2alv). Every request, a piece's and an answer asked again alike, counts against
one ceiling per review, `DEEPSEEK_MAX_CHUNKS` x (2^(`DEEPSEEK_SPLIT_DEPTH` + 1) - 1) (28 at the
defaults: every part split to the limit); a review that would pass it is incomplete. The workflow passes `DEEPSEEK_MODEL`,
`DEEPSEEK_API_BASE`, `DEEPSEEK_TOKEN_BUDGET`, `DEEPSEEK_MAX_OUTPUT_TOKENS` (default 65536;
DeepSeek documents a 384K maximum), `DEEPSEEK_SPLIT_DEPTH`, `DEEPSEEK_MAX_CHUNKS`,
`DEEPSEEK_TIMEOUT`, `DEEPSEEK_RETRIES` and `DEEPSEEK_RETRY_DELAY` from repository variables of
the same name (unset: the script's default); the wall-clock bounds below are not variables.
A missing key, an API error, an
answer that is still not the asked JSON when asked again, the request ceiling, git work before the first request still running after 5
minutes, or requests still unanswered 45 minutes after the first one (these bounds leave room
inside the job's 60-minute limit to post) posts a comment saying the
review did not complete for that SHA, never a pass, and fails only that job. An API error is
reported by its status and the error's type and code only, never its body, which can echo part
of the key. No request follows a redirect, which would carry the key or the GitHub token to
another host; a 3xx is an API error like any other. Every finding, whole and redacted, and the full report of files reviewed from the
diff only or not reviewed are also printed to the job log as JSON lines (`deepseek-review:
findings for head <SHA>: [...]` and `deepseek-review: report for head <SHA>: {...}`); when the
comment has no room for all of it, its marker says `status=truncated findings=N shown=M` and the
job fails. Model text
in the comment cannot @-mention anyone (a zero-width space follows each `@`). The workflow runs
the script with `python3 -I`, so no file beside it can stand in for a standard-library module it
imports, and pins every action to a full commit SHA. The job is not a required status check.
`scripts/deepseek-review-tests.py` proves all of this offline in the check.

- Secret `DEEPSEEK_API_KEY` (the owner sets it: `gh secret set DEEPSEEK_API_KEY`). It is read
  only from the secret, sent only in the request's authorization header, removed from anything
  the script prints or posts, and never written to a file, a comment or the queue (0009). The
  script runs from the pull request's head, so the trust boundary is push access to this
  repository; the owner chose to keep it there with a CI-only DeepSeek key used for nothing
  else (owner decision 2026-10-05, FBC-omhd).
- Repository variable `DEEPSEEK_MODEL`, default `deepseek-flash` (DeepSeek-V4.1-Flash, the
  owner's choice of 2026-10-06, FBC-qo3h): DeepSeek's cheaper model on its OpenAI-compatible
  API, with JSON output, the same 1M context and 384K maximum output as `deepseek-v4-pro`, and
  about 4.4x cheaper (the API's change log of 2026-09-10 retired `deepseek-chat` and
  `deepseek-reasoner`; `deepseek-v4-pro`, the most capable model, stays selectable through this
  variable). Set it with `gh variable set DEEPSEEK_MODEL --body <model>`; an
  unset variable takes the default. Optional variables, each unset at the script's default:
  `DEEPSEEK_API_BASE` (default `https://api.deepseek.com`), `DEEPSEEK_TOKEN_BUDGET` (estimated
  prompt tokens per request, characters / 3, default 120000), `DEEPSEEK_MAX_CHUNKS` (parts per
  review, default 4), `DEEPSEEK_MAX_OUTPUT_TOKENS` (each answer's allowance, reasoning included,
  default 65536), `DEEPSEEK_SPLIT_DEPTH` (times a cut-off part is split again, 0 to 4, default
  2), `DEEPSEEK_TIMEOUT` (seconds per request, default 600), `DEEPSEEK_RETRIES` (default 2) and
  `DEEPSEEK_RETRY_DELAY` (seconds, default 10). A review that keeps ending cut off at max_tokens
  is fixed by raising `DEEPSEEK_MAX_OUTPUT_TOKENS` or `DEEPSEEK_SPLIT_DEPTH`.
- Historical DeepSeek comments, including complete ones by `github-actions[bot]`, do not satisfy
  the active Codex gate. Reinstating them as review evidence requires a new owner decision.

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
  (`crates/venues/*/src/sign*`, `crates/*/src/auth*`, and the DeepSeek reviewer's
  `scripts/deepseek-review.py` and `.github/workflows/deepseek-review.yml`, which inject its API
  key) for the owner's review; every other pull request is reviewed and merged by the agents,
  except as the Merge profile says (0149).

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
- Never let the journal block a cancel, a reducing order, an ack or a fill (0006); the one
  exception is 0139's: once the soft limit moves a Quoting market to Cancel-only, that market
  builds no reducing orders of its own, the owner's Flatten or Wind-down still reducing.
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
