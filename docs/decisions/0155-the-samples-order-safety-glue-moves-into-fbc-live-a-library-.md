# 0155 — The samples' order-safety glue moves into fbc-live, a library that depends on no venue crate and reads only its session's shard clock, augmenting 0057, 0100 and 0140, relaxing 0002's one-signature bound

Status: accepted
Date: 2026-11-23

## Context

- **Where the glue is.** In `crates/fbc-examples`, whose library target is empty; every sample that
  trades includes it by path. FBC-bddu lists the pieces: `src/link.rs` with FBC-aoh5's cover
  tracking, `src/positions.rs` (FBC-hpg6), and `testnet_quote`'s sweep (`Driver::sweep`), its
  rate-limited resend (PR #158), its classifier of cancels not ours (`CancelsNotOurs`) and its
  mark-aware sizing, which `testnet_trade` repeats for its one order, both on `testnet.rs`'s
  `cap_lots` (0113 item 3) and `inventory_price`. Otherwise chaiwala-rs, the 24/7 consumer, rebuilds
  each piece and could miss one.
- **The clocks.** The link times events by their stamps, from the shard's `IngestClock` on tokio's
  clock (0100 item 1, 0107 item 1), whose `now` is `pub(crate)`; it times each epoch's end at the
  last `recv_mono` (FBC-9svb). A send has no stamp: `Link::now` reads `SystemTime` and a std
  `Instant` taken in `Link::new`. The two agree only where the link is built just before the
  session's clock (`testnet_trade`, so `mainnet_smoke`). Elsewhere the send times trail the stamps,
  and on a paused runtime (closed-loop replay, the samples' paused tests) they run in real time, the
  stamps in virtual time. So the registry compares send times with snapshot and ladder times on
  another clock, and `testnet_quote` hands `Link::now` to its planner. The classifier and the rate
  hold read tokio's clock, the position check the sample's start.

## Decision

1. **The crate.** `crates/fbc-live`, package `fbc-live`: a library, a member by the `crates/fbc-*`
   glob, `publish = false` (0008).
   - Normal dependencies: fbc-core, fbc-oms and fbc-runtime only. Sizing's `rust_decimal::Decimal`,
     which fbc-core's API hands out (`InstrumentSpec::multiplier`, `SizeStep::get`,
     `PxExact::to_decimal`), comes through its re-export; the moved `cap_lots` converts with
     `i64::try_from`, so nothing else is re-exported. Dev-dependencies: what its tests need
     (fbc-conformance, tokio with `test-util`, and for the closed-loop replay test fbc-sim,
     fbc-journal and rust_decimal), none a venue crate or depending on one.
   - `scripts/check-deps.sh` holds it unchanged. fbc-examples takes it as a dev-dependency; its
     library target stays empty.
   - It branches on no venue's name (0003), holds nothing that decides what to quote (0001; the
     samples' quote prices and sizes stay theirs), and, once point 3 lands, owns no socket, thread,
     task, queue or clock (0002).
2. **What it holds.** No piece warns, Stops, kills, arms or disarms. Only the link (a Safety cancel
   for each unprotected order of ours the registry holds open, as the session reports it, 0080) and
   the sweep, once started, send cancels.
   - **`link`**: `src/link.rs`, moved whole with its behaviour. The handler's five entry points
     (`on_exec`, `on_epoch_end`, `on_submitted`, `on_unprotected`, `on_decode_error`) become public
     methods of `Link`, for a consumer's own handler. `LinkHandler` hands each call on, then calls
     the consumer's wake, a closure. New reads: `live_on(inst)`, the orders of ours on a market the
     link holds live, and `cancels_not_ours_seen()`, true once a cancel not ours is noted, never
     false again.
   - **`not_ours`**: FBC-aoh5's classifier (`CancelsNotOurs`, `NotOurs`), one per market (its
     requote rule keeps one requote per side). With the consumer's span and window, it tells a
     cancel of the same event as the one before from the first of a new window and from a repeat
     (within that first's window, or of the loop's requote). The WARN and Stop these lead to stay
     the consumer's.
   - **`sweep`** (point 5), with `RateHold`, the hold a refusal for its rate puts on the consumer.
   - **`sizing`**: `cap_lots` and `inventory_price` with their results unchanged, the operator's
     caps in lots (0113), the inventory cap at no less than the mark, and the minimum and one side's
     judgement (point 6).
   - **`positions`**: `Reading` and `Verdict` (`Agrees`, `Cleared`, `Persists`), naming no reaction;
     `fills_in`, `fill_seen`, and `resync_disagreements` from `testnet.rs`; and `PositionWatch`: a
     mismatch or an unread answer is read once more after the consumer's wait, then cleared or
     persisting (FBC-hpg6). `HAND_CHECK`, the Stop text `Verdict::failure` builds (then a sample
     function) and the POSITION lines stay the samples'.
3. **The link's clock is its session's shard clock.**
   - fbc-runtime adds `ExecOrders::now() -> (MonoNs, WallNs)`, reading the session's own
     `IngestClock` as the session does, and `ExecOrders::shares_clock(&ExecOrders) -> bool` (0057).
     `IngestClock::now` stays `pub(crate)`.
   - The link takes the clock from the `ExecOrders` it was last attached to or submitted through,
     and reads it only in `Link::now()`, which times each request it submits. `Link::now()` becomes
     `Option<(MonoNs, WallNs)>`, `None` before the first attach or submit. `SubmitHandle` names no
     session, so a send is timed on the clock last taken: the reporting session's whenever the
     link's sessions share one clock, as every sample's do. An `ExecOrders` on another clock is
     noted as a `Note::Problem` and its clock read from then on; its submission still goes, so no
     cancel is held back.
   - Input times stay their stamps, and a fill's venue time its aligned wall time, unadjusted, which
     a replayed fill's admission and a fill's placement against a seed read (0055, 0095). The other
     pieces read no clock: each takes `now: MonoNs` from its caller (`Link::now`, a stamp, or a
     consumer input's or timer's `at`, 0103).
   - **The lint.** `crates/fbc-live/clippy.toml` disallows `std::time::Instant::now` and
     `::elapsed`, `std::time::SystemTime::now` and `::elapsed`, and `fbc_runtime::SystemWallClock`,
     each with a reason naming 0155. It misses a read through a tokio `Instant` fbc-runtime hands
     out (`Refused::ready_at`) or inside a dependency's function; review covers both.
4. **Replay.** In closed-loop replay the link reads `IngestClock::replayed` through its sessions, so
   two replays of one journal record the same send times, planner `now`s and wake decisions (0107
   items 1 and 3). Decoder replay calls no `on_submitted` (0138 item 1), so no send time is recorded
   there. A consumer on the shard host arms a piece's wake through `ConsumerTimers`, inert in
   replay, where the live shard's firings come back from the journal (0103 items 4 and 7).
5. **The sweep** does for Start, each reconnect, and Stop after `Registry::kill` what
   `testnet_quote`'s `Driver::sweep` and `stop` do today.
   - It sends fbc-oms's `cancel_everything` for the market (0012's instrument cancel-all under
     0005's I7, with 0094's explicit cancels beside it), or explicit cancels of our orders on the
     market, chosen as `Driver::cancel_ours` does, when fbc-oms builds no cancel-all or
     `cancels_not_ours_seen()` is true; never an account cancel-all (0045). It sends each cancel due
     on an acknowledgement (`Registry::cancels_due`). A cancel refused for its rate makes the cancel
     of everything due again once the consumer's `RateHold` passes: the venue's `retry_after`, or
     the consumer's wait when it states none (PR #158).
   - At Start and after a reconnect, an `AlreadyTerminal` or `NotFound` refusal waits for its
     order's end and any other not for its rate fails the sweep, which ends once every order of ours
     the link holds on the market has ended and `ExecOrders::may_place_on` holds. No resync holds it
     past the session's resync deadline, which ends the epoch (FBC-nwpr). An order fbc-oms puts on
     the Unknown ladder, as any `NotFound` does, may never end here (FBC-2cv0), and `testnet_quote`
     Stops on it.
   - At Stop no refusal ends it: it ends once every such order has ended and every request it sent
     is answered, `may_place_on` unread, and the consumer judges each answer (`rpcs()`), as the
     samples do.
   - It keeps no timeout; the consumer bounds its wait. The library documents Stop's order, which
     the consumer runs: the kill switch, the sweep, the comparison while the market lease is held,
     the disarm.
6. **Sizing reads no venue.** The mark is the consumer's argument (`MdEvent::Mark` where
   `MdCaps::mark` declares one, or its own read). A side is left out when its notional at its price
   is above the resting cap or below the minimum, when its notional at the mark is below the minimum
   (where Paradex judges it, 0096's Context), or when a fill of it would take the position past the
   inventory cap at the highest of the quote prices, the touch and the mark. One order's minimum is
   judged at its price and the mark, as `testnet_trade` judges its one order. The minimum is the
   venue's, not a bound: the spec's `min_notional` (a `Money` the consumer converts), stated by
   discovery where the venue sets one (Paradex's does), or else the consumer's own reading, as the
   samples, whose specs built from flags state none, take `--min-notional`. `None` or zero leaves no
   side out. It is judged at both prices for every venue, which only ever leaves out more, so no
   `VenueCaps` field is added. fbc-oms judges the caps at authorization (0082) and refuses a place,
   batch or amend at submit once a fill moved its market's position (0153).
7. **Positions read no venue.** The library compares; the read is the consumer's. The samples keep
   theirs (`PositionCheck` over `src/auth_rest.rs`, unchanged), and the other-markets refusal
   (FBC-tyft) with its parsing of Paradex's answer (`other_markets`): sample I/O, like
   `decode_mark`. A venue-generic read is a follow-up ticket, under a record of its own since a
   resync mid-epoch touches 0058's gate: an on-demand resync through the session, journaled,
   compared by fbc-oms at its watermark (0055).
8. **Bounds.** Every bound is the consumer's, with no `Default` (the owner's answer, 0009), and each
   `new` refuses zero: the classifier's span and window, the rate hold's wait when the venue states
   none, the position re-read's wait, and the operator's two caps (positive, no ceiling, 0113). The
   fill ledger's and the ladder's configurations stay the consumer's, and the samples pass today's
   values. `shown`'s and `described`'s word, number, code and length limits and `withheld`'s rules
   stay fixed: they keep an echoed credential out of what is printed (0009), so no consumer tunes
   them.
9. **The move**, one PR per ticket, each discovered from FBC-bddu and blocking it, this record
   (FBC-h64h) first:
   - FBC-umqi, the crate and the link, keeping its std clock, behind the shim, which re-exports
     `fbc_live::link` with a `LinkHandler` of its own on today's `Rc<Notify>` wake, so the probe's
     by-path includes compile unchanged; the Paradex samples use the library from then on.
   - FBC-y7lq, the clock (point 3) alone; then the pieces `testnet_quote` calls, `testnet_trade` (so
     `mainnet_smoke`) calling sizing and positions: FBC-21g6, the classifier; FBC-132g, the sweep;
     FBC-6r81, sizing; FBC-rujq, positions.
   - FBC-umqi waits on FBC-h64h, FBC-y7lq on FBC-umqi, FBC-21g6, FBC-6r81 and FBC-rujq on FBC-y7lq,
     and FBC-132g on FBC-21g6. FBC-mu68 and FBC-2cv0 wait on FBC-umqi.
   - FBC-bddu's own commit is the last: it moves the probe onto the library and deletes the shim.
     Its done line's `GET /positions` check is point 2's `positions`, not point 7's read.
   - Pure tests move with their code onto the conformance toy; the samples' stub tests stay, as the
     proof that behaviour held.

## What the owner decided

In the main session's chat, recorded on FBC-bddu:

- 2026-11-17: a new crate, fbc-live, with normal dependencies on fbc-core, fbc-oms and fbc-runtime
  only, holding the link with its cancel-cover tracking, the sweep with its rate-limited resend,
  mark-aware sizing and the positions comparison; every bound from the consumer's configuration,
  with no default; the reactions (warn, Stop) consumer policy; several PRs, with a re-export shim at
  `crates/fbc-examples/src/link.rs` until the QFEX probe stack lands, this record first.
- 2026-11-23: the draft's four recommended options: the classifier in fbc-live (point 2); fbc-core's
  re-export of `Decimal` (point 1); the shim until FBC-bddu's last PR, though PR #284 (FBC-4plj)
  landed the probe stack (point 9); and the position read's follow-up ticket (point 7).

Approving this text (0149) also takes, beyond both answers: point 3's `ExecOrders::now` and
`shares_clock`, point 6's at-the-mark minimum for every venue, point 8's fixed redaction limits,
point 9's shim `LinkHandler`, and the trade-off under Consequences.

## Alternatives

- **An existing crate:** rejected. fbc-oms makes a cycle, since fbc-runtime depends on it and it
  never on fbc-runtime (0045, 0057); fbc-runtime is the one generic runtime (0002), and no session
  needs the link; fbc-examples' library target stays empty, and pinning "examples" by tag is the
  wrong contract; fbc-conformance is a test kit, only ever a dev-dependency.
- **Keep the std clock:** rejected (Context).
- **A clock passed to `Link::new`:** rejected. A passed `IngestClock` is readable only if its `now`
  were public, another shard's clock could be passed, and every call of `Link::new` changes.
- **`IngestClock::now` made public, with `ExecOrders::clock`:** rejected for now. It hands the
  shard's time to code holding no session, where `ExecOrders::now` gives it only to holders of a
  session's handle, as every submitter is.
- **The encode context's time in `SubmitHandle`:** rejected for now. It changes fbc-core's contract
  for every gateway and test double, a submission not sent has none, and the decoder replay it would
  serve calls no `on_submitted` (0138).
- **A tokio `Notify` in fbc-live's `LinkHandler`, so the shim is a pure re-export:** rejected. Tokio
  is past the owner's list, and a consumer on the shard host needs none.
- **The classifier left in `testnet_quote`:** not taken (the owner's choice). chaiwala-rs would
  rebuild the rule PR #159's reviews shaped.
- **`rust_decimal` as a fourth normal dependency:** not taken (the owner's choice): no new package
  or licence, but past his list.
- **Exact `Money` arithmetic in fbc-core:** not taken. New API, its results possibly differing from
  the samples' at the nano.
- **No shim:** not taken (the owner's choice). FBC-umqi would move the probe while its follow-ups
  (FBC-6zqr, FBC-ydat, FBC-pr6l, FBC-ow6w) are open.
- **The position read as a factory method's `HttpPlan`** (the login, then `GET /positions`), as Test
  Connection is (0048), **or no ticket:** not taken. The plan keeps the login in the adapter's
  `src/auth`, but logs in again beside the session, fbc-oms comparing its answer at no watermark,
  and needs a record too. With no ticket, Paradex's login, `GET /positions` and the session token
  sit in chaiwala-rs, as in the samples' by-path `src/auth_rest.rs`: connectivity 0001 puts here.
- **Latching places off on a persisting mismatch:** rejected here. That is Stop's kill switch, a
  reaction the owner kept with the consumer.
- **A place gate in `Link::submit` after each resync:** not taken. The owner asks for none, and the
  session already holds places while an order its resync showed rests unprotected (0080). The
  library documents that no place goes before the sweep ends.

## Consequences

- chaiwala-rs depends on fbc-live by tag (0001, 0008) and builds its link over its own registry,
  fill ledger, caps and ladder configuration.
- **The clock change.** On a paused runtime the registry's send times move from real to virtual
  time; live, onto the stamps' clock where the link was built after it. A paused test that relied on
  the clocks differing changes, hence its own ticket, FBC-y7lq. Through the shim it reaches the QFEX
  probe, whose send times move to its sessions' `IngestClock`. That PR waits for any probe change in
  flight whose paused test it moves, and its body says it changes the probe FBC-9sk2's owner run
  uses.
- **The link and 0100's ladder.** A command the shard host submits from a decide pass's ladder
  (0100) is to the link the session's own: it gets no send time, a place of it the session reports
  not sent (0153's refusals after a fill among them) or the venue refuses stays PendingNew, though
  one handed back unsubmitted is released (0152), and its places are not among the link's live
  orders, so a cancel-all through the link does not cover them, their end by it is noted
  `Note::CancelNotOurs`, and `live_on` and the sweep miss them. So a consumer using the link submits
  through `Link::submit` only, outside the ladder, and a decide pass's commands submitted so are
  taken together on the session's next turn (0057): a protective cancel a handler submits meanwhile
  waits behind all of them, not at most one (0002, 0100 item 5), which 0100 says would show it
  wrong. That is what this record adds to 0100, and the trade-off approving it accepts: until
  FBC-tpyv hands the consumer each ladder command's request id and a follow-up's `Link` method
  records a ladder command with it as `Link::submit` does, a consumer using the link gives up 0002's
  one-signature bound. The method needs the id before the session reports the command sent, which it
  does as the host lets the members run after submitting it (0100 item 3), so FBC-tpyv must hand it
  over at the submission.
- **The link's gaps** stay as in the samples, each ticket retargeted to `fbc_live::link` when
  worked. The link:
  - holds each request's items until `Link::release` (FBC-mu68), and every note until `Link::forget`
    takes it, as `testnet_quote` does, which no ticket changes;
  - queries no order, so an Unknown order stays Unknown and an unprotected one the registry does not
    hold open keeps its market held (FBC-2cv0);
  - drops an authorization it refuses, which 0152 releases, but takes a `Backpressure` refusal as an
    error, the place left PendingNew until the consumer settles it (0105; FBC-sckz, deferred);
  - serves sessions of one order-entry connection only, as every venue here plans, until FBC-g0mc
    builds 0140 item 12's two sessions of one account; 0140's `link.rs` is `fbc_live::link` from now
    on;
  - keeps for life each fill the ledger refused for its time (`Link::fill_refusals`), and records no
    placement nonce, which a venue cancelling by nonce needs.
- **Follow-ups**, filed with FBC-umqi: the ladder method; a bounded `fill_refusals`; the placement
  nonce; the position read (point 7); and moving into fbc-live 0094's stream check,
  `testnet_quote`'s `unreconciled`, which this record leaves out. Which of them, and FBC-tpyv, block
  v0.0.8 is asked on FBC-vvit, not decided here.
- The venue crates' test glue (`fbc-venue-paradex`'s `tests/rehearsal/glue.rs`, `fbc-venue-qfex`'s
  `tests/clustered_move/glue.rs`) and the samples' own clocks for lines, sleeps and deadlines stay.
- AGENTS.md's Layout gains fbc-live, and drops the samples' by-path `src/link.rs` with the shim.

## What would show this was wrong

- The registry holding a send time earlier than the stamp of the input it was submitted on, on the
  same session.
- Two closed-loop replays of one journal through the link giving different send times, notes or
  verdicts.
- A piece of fbc-live that needs a clock read of its own, or a branch on a venue's name.
- chaiwala-rs having to copy a piece point 2 lists, the position read aside.
- A sample test whose expectations changed because of the move rather than the clock.
