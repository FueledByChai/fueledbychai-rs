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

This repository keeps the staged/legacy handoff, the default (kit decision 0028), because the
owner merges pull requests by hand, CI runs when the `ci:run` label is added to a ready pull
request (`.github/workflows/loop.yml`; a new head needs the label removed and added again), and no
merge-queue App runs here.

That handoff is the pull-request procedure the loop prompts follow here, and it requires
auto-merge disabled. Open each ticket's pull request as a draft with
`scripts/open-ticket-pr.sh <id> --draft`, which requests no auto-merge (a pull request touching
a review path still gets its `needs-review` label); verify auto-merge is disabled
(`gh pr view <number> --json autoMergeRequest` shows it null) before `gh pr ready <number>`
starts review; and keep it disabled through the final gate: never open the pull request without
`--draft`, and never run `gh pr merge --auto`. A green check merges nothing by itself; the owner
merges each pull request by hand. The review paths under Rules below (0009) still decide which
pull requests also wait for the owner's review; under this handoff no other one merges itself
when green. This paragraph governs over the Rules line's "every other pull request auto-merges
when green" (from 0009) until a record settles that clause (FBC-ftg0).

### Layout

- A Cargo workspace (0001, 0008). `crates/fbc-core` exists, with time, units,
  price grids, exact prices, sealed ids, the client-id codec, `NamespaceLease`, `CidMint`,
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
  the execution planner, 0065),
  `crates/venues/fbc-venue-paradex` with its signer (`src/sign`), SBE market data (`src/md`: bbo,
  trades and the order book), its order-entry capabilities (`src/exec`, 0054) and its factory
  (`src/factory.rs`, market data and order entry, 0072),
  `crates/venues/fbc-venue-binance-usdm` with its market-data-only factory and codec,
  `crates/venues/fbc-venue-qfex` with its read-only public MDS factory and codec (0117),
  `crates/venues/fbc-venue-hibachi` with its market-data-only factory, discovery, caps,
  `/ws/market` codec (0129) and status poll (0130) and its offline signer (`src/sign`, built
  from `Secrets` by `src/auth`), and `crates/fbc-examples` with the owner-run samples; the
  rest are planned. Members are the `crates/fbc-*` and
  `crates/venues/fbc-*` globs, and `fixtures/` is excluded from the workspace:
  - `crates/fbc-core`: the contract. Time, units, price grids and exact prices, sealed ids and
    the canonical client-id codec, `Fee`/`FeeBook`, `InstrumentSpec`, `VenueCaps`, events,
    commands, the codec and factory traits, `DecodeScope` and `EncodeCtx` (0003, 0004), and
    `PathStamps`, whose marks reach the runtime's `PathRecorder` and give back no time (0034).
    A spec states `signing: Option<SigningParams>` (`AssetDecimals { underlying, settlement }`,
    its contract id kept in `native_id`) and `sessions: Option<SessionCalendar>`
    (`src/calendar.rs`: an earliest open, a close that never reopens, dated `ClosedWindow`s and
    `WeeklyClosure`s in a zone's wall time under a POSIX-style `UtcOffsetRule`, evaluated by
    integer civil-date arithmetic with no zone data and no clock; `is_open(at)` only ever
    closes, fail-closed across daylight-saving edges, and `next_closed(from)` gives the first
    instant at or after `from` it closes, never an occurrence a skipped hour swallows whole, so
    a codec can ask for a timer at a session's end, 0130; `None` where it closes nothing),
    copied from the draft by the resolver, neither in `version` (0125).
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
    output and connection change under its traffic class and is never waited on (0006);
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
    exactly the nonces it asks for, and its keepalive its own timer (0056); commands reach its
    codec only through its `ExecOrders`, as an fbc-oms `Authorization` for its account or a
    `ControlCommand`, each encoded on its next turn with one reserved nonce per item and
    reported at most once to `ExecHandler::on_submitted` (sent, or `NotSent` with nothing
    written: no authenticated epoch, the codec's refusal, effects that do not carry the request,
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
    an account-wide cancel-on-disconnect cap requires explicit authority for that account
    before a session plans or opens its endpoints; once armed, a disconnect cancels that
    account's orders across connections, including orders placed after the arm, and each new
    epoch must arm again (0121);
    `WallClockNonces` is the production nonce source for a venue whose nonce is a wall-clock
    timestamp (Hibachi): `max(clock, last + 1)` microseconds over an injected `WallClock`,
    resumed after a journal's `last_nonce` (refused, `JournalGap`, for records holding a gap), refusing a block further ahead of its clock than
    the caller's window, which has no default (FBC-3bq4, 0114, `tests/wall_nonces.rs`);
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
    had; the `FillLedger` keyed by `FillEvent::key()` and bounded by an age and a count from
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
    or amend judged against the caps again at issue and released when refused, a place built
    and not yet authorized held by its command: withdrawn with it, or released by the
    registry's next mutating call once it is dropped unspent, with no outcome of it by client
    id applied and an accepted or unknown one voiding the command (0082, 0084;
    `tests/retained.rs`, `tests/held.rs`), and whose
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
    compared and reported, never overwriting the inventory; market states and arming (0012;
    `src/entry.rs`, `tests/states.rs`): each market armed or not and Killed, Cancel-only, Exit
    or Quoting, a fresh registry disarmed and Cancel-only, the state checked before the caps,
    Start, Flatten and Wind-down arming with the market lease (and the account lease where
    nonces are per account) and refused while Killed or the position is unknown, every change
    advancing the market's `StateGeneration`; cancels and fills in every state (0012;
    `src/sweep.rs`, `tests/sweep.rs`): the kill switch's cancel everything an instrument
    cancel-all only under the held market lease after a trustworthy resync with no order not
    ours in view, otherwise our orders cancelled by explicit reference, named again on each
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
    age), a change an amend where the venue's `OrderCaps` admit it and otherwise a cancel
    then, once the old order is terminal, a place, a PendingNew, Unknown or in-flight order occupying its level, every
    command built through the one pre-trade path and the permits and emitted as an
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
    `Batching::Declared` places of one stage and cancels batched within the venue's declared
    sizes and references (0111; `src/gates.rs`, `tests/gates.rs`); 0005, 0013), `crates/fbc-journal` (0006; it depends on
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
    names in inbound frames and responses (`redact_inbound`, 0028) in version 4, the kind
    each outbound frame was sent as and the control frames received in version 5, the
    stale-authorization reason in version 6, a request deadline's firing in version 7
    (0090), the consumer's own records in version 8 (`Record::Consumer`: its inputs and
    annotations as opaque bytes with a kind and an input flag, written through the sink in the
    shard's total order under the class the consumer picks; 0101), and the ingest sequence of a
    consumer input the shard stamped in version 9 (0103), whose reader still reads version 2 to
    8 journals), `crates/fbc-sim` (simulated venue and queue-position
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
    far a public stub venue server on 127.0.0.1 ephemeral ports, a WebSocket endpoint that plays
    a fault script of typed steps and records every connection and data frame, and an HTTP/1.1
    endpoint with fixed responses by path; the reconnect-storm script and a check of connection
    attempts against `ReconnectPacing`; the duplicate-ack and silence-after-subscription
    scripts; venue crates take it as a dev-dependency instead of writing their own servers;
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
    anchored on an HTTP snapshot, a gap reported as the health of the channel that broke, and a
    keepalive frame; and `src/suite/`, the named suite:
    `suite!`, invoked from an adapter's `tests/conformance.rs` with its factory, its fixture
    directory (one subdirectory per check that reads recorded data) and the setup its fixtures
    assume, one test per check, each check also a public function returning what it probed or
    every breach by capability; so far `caps_truthful` (every absence the caps declare refused
    `NotSent(Unsupported)` with no effect, every flag conflict `NotSent(FlagConflict)`, an
    instrument cancel-all never widened, a control per operation sent) and
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
  - `crates/fbc-examples`: sample programs the owner runs by hand, never CI (FBC-u4so). Its
    library target is empty and every dependency is a dev-dependency, so it is outside
    `crates/venues/` yet wires concrete venues without a normal dependency on one; each sample is
    an example target whose wiring a test in `tests/` includes by path and runs against the
    conformance stub. So far `examples/md_watch/`: a Paradex market's bbo, trades and `deltas`
    book, a Binance USD-M symbol's `bookTicker` and diff-depth book, and a Hibachi contract's
    `ask_bid_price` touch, trades and `orderbook` book (FBC-9o7e: the contract read first
    through the venue's discovery, its open or closed state and its codec's counts printed,
    `examples/md_watch/hibachi.rs`) through `MdVenue`, one `MdBooks` per venue, a line per
    update; no order, no credential (`tests/md_watch.rs`); and
    `examples/testnet_trade/` (FBC-x69b): Paradex TESTNET only (the URLs, exactly the testnet's
    or a loopback stub's at `/v1`, and the chain id checked before anything connects), an
    fbc-oms registry declared an owner-assisted testnet run (decision 0067) behind one
    `ExecSession`, its events routed into the registry by `src/link.rs` (the ExecHandler-to-Registry
    glue), one post-only order placed well away from the touch read
    from `GET /orderbook` (read again just before the place), its size, caps, side, distance and
    client-id namespace all required flags, cancelled, then Stop (`tests/testnet_trade.rs`,
    against a Paradex-shaped conformance stub). Its credentials come from environment variables
    through `src/auth.rs`, a review path included by path, not a module of the empty library
    target. And `examples/paradex_testnet_acceptance/` (FBC-xji8): FBC-8xr's owner-run
    acceptance run on Paradex TESTNET, reusing testnet_trade's flags and helpers and the shared
    glue and guard by path: place, amend, cancel, batch create, cancel-many, an IOC expiry,
    behind `--allow-fill` a take flattened by a reduce-only IOC and a partly filled order's
    amend (FBC-6fnc), and cancel-on-disconnect on a dropped socket and on a graceful close, each
    through an fbc-oms authorization, in three phases (a fresh registry and session each); after
    every step its own authenticated REST reads (`src/auth_rest.rs`, a review path) compare GET
    /orders and GET /positions with the order stream; it writes `report.md`, `evidence.json`,
    its journal and a scrubbed fixture slice for `fixtures/paradex/exec-recorded/`
    (`tests/paradex_testnet_acceptance.rs`, against a stub model of the account; how the owner
    runs it, from the deployment box too: `crates/fbc-examples/README.md`).
    And `examples/testnet_quote/` (FBC-elg7): the same testnet guard and wiring, a
    PLUMBING TEST, NOT A STRATEGY: one post-only order a side a fixed offset behind the touch,
    requoted through fbc-oms's `ExecutionPlanner` given Paradex's caps with no amend
    (cancel-and-replace only), every order of ours still open cancelled after each reconnect
    before any place, Stop on Ctrl-C, `--duration`, `--cycles`, a resync mismatch, any Unknown
    order, a refused login or a permanent refusal of a place or cancel, every touch read with
    the mark price and charged to the session's rate limiter (`tests/testnet_quote.rs`); with
    `--mainnet` (FBC-tyft) a supervised Paradex MAINNET session behind `--mainnet` and
    `--sole-trader`: mainnet's URLs and chain, the mainnet-only credentials, the operator's own
    positive caps (required, no defaults, no ceiling: FBC-thks), offsets of at least 20 bps,
    `--duration` of at most an hour, never seeded by hand. The samples that trade share
    `src/link.rs` (the glue) and `src/testnet.rs` (guard and wiring), and the mainnet ones
    `src/mainnet.rs` (the mainnet guard), included by path.
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
    `SigningParams::AssetDecimals` and a session calendar from the advertised open and close
    and, for the FX underlyings the docs name, the New York weekend (0125); only `LIVE` on a
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
    status mapping and timestamp parsing plus `maintenanceWindow` (decimal Unix seconds, a number
    or a string, none after 2100); each market's status reported as `MdEvent::Status` once each time it changes,
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
  authorization in `fbc-oms`'s.
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
  with a variant for each status and timestamp refusal, and a funding-history answer, its README
  citing each field's source; `fixtures/hibachi/rest/status/` (marked `SYNTHETIC`) the status
  poll's answers, each a change of three of those contracts at instants its tests' timelines
  name (a pause, either maintenance, a window as numbers, as strings and in milliseconds, an
  advertised close, a market left out, each refusal), its README saying what each pins;
  `fixtures/hibachi/md/` (marked `SYNTHETIC`) hand-built `/ws/market`
  frames, one per file, shaped as Hibachi's docs, SDK and the Java library show them and as the
  owner's capture showed in words only, its README citing each shape; and
  `fixtures/hibachi/conformance/` (marked `SYNTHETIC`) the conformance suite's cases built from
  them.
  `fixtures/hibachi/signing/` (marked `SYNTHETIC`) holds the Java oracle
  (`HibachiSignOracle.java`, `run-oracle.sh`) and the vectors it wrote on a synthetic key, its
  README saying how it was run and which buffers Hibachi's docs print.
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
key- and address-shaped hex, PEM headers and master-key bytes; 0009), then the external
reviewer's offline proof (`scripts/deepseek-review-tests.py`, below), then, once `Cargo.toml`
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
depends on no workspace crate but `fbc-core`; dev-dependencies are not checked; design §3;
and the resolved graph holds one `log` package, the one tungstenite logs through and
fbc-runtime caps; 0079),
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
  key) for the owner's review; every other pull request auto-merges when green.

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
