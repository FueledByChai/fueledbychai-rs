# Changelog

What shipped, by release: the commits that carry a ticket id between two tags, with the
ticket text read from Beads (scripts/release-notes.sh --archive).

## v0.0.7 — 2026-10-19 (v0.0.6..89b939f)

66 tickets, PRs #95 to #182, decision records 0078 to 0111. This release is the library's
stages 1 and 2 on the way to a consumer's model trading live on Paradex: fbc-oms's
price-capped crossing exits, level tags and removal reasons, the audit sink and orphan
handling; order-entry robustness (bounded queues, reconnect storm, snapshot-lag settle,
kernel-receive latency of protective cancels); the shard host, consumer inputs and consumer
journal records; and SimVenue standing in for Paradex, hosted by the runtime and replayed
closed-loop. Paradex's open-order snapshot is now trusted (0094). The owner cuts the tag;
agents never push a tag or create a GitHub release.

### Pin bump from v0.0.6

What a consumer meets when it moves its pin from v0.0.6 to v0.0.7, crate by crate.
"Required" means a new field or argument with no default (0009): the build breaks until the
consumer gives a value. The enums named under "new variants" are exhaustive, so a `match`
over one needs the new arms.

#### fbc-core

- **Required: `OrderCaps::snapshot_reads: SnapshotReads`** (`OneInstant` or `Separate`;
  FBC-k7t7, PR #156, decision 0095). A venue states whether its resync reads open orders and
  positions at one instant. On `Separate`, the resync's seed no longer places a fill by an
  order's shown cumulative fill; only a fill's arrival or matching-engine time places it.
  Paradex declares `Separate`.
- **Required: `OrderCaps::snapshot_lag_bound: Duration`** (FBC-i8lp, PR #181, decision 0109).
  How long the venue's open-order snapshot may lag its order stream. The Unknown ladder
  counts no absence of an order from a snapshot within this bound of the order's send,
  whatever the consumer's settle time. Paradex declares 6 s; its recorded runs saw up to
  3480 ms. Use zero for a snapshot the venue answers from its own state at once.
- **Required: `CancelOnDisconnect::PerConnection { covers_open_orders: bool }`** (FBC-nvxn,
  PR #127, decision 0080). Set it `true` only where a venue document says an accepted arm
  also covers orders already open. Paradex declares `false`.
- **New export:** `SnapshotReads`.
- **Codec contract, `ExecCodec` implementors** (FBC-m8vm, PR #130, decision 0081): an
  `Effect::Http` that carries an `rpc` and whose connection epoch ended before its result
  arrives goes to `on_rpc_timeout` at its deadline, not to `on_http`. `redact_inbound` is
  asked only about inputs of the epoch the codec serves (FBC-2pr, PR #115, decision 0078).

#### fbc-oms

- **Changed signature: `ExecutionPlanner::plan(desired, reg, venue: &PlanVenue<'_>, acct,
  mint, now)`** takes a `PlanVenue` where it took `&OrderCaps` (FBC-hht, PR #182, decision
  0111). `PlanVenue { caps, account_mode, market_mode, status, rates: RateView, batching:
  Batching }` has no default. A place or amend that the market's `TradingStatus` or a
  `VenueMode` refuses is never built; cancels always go. Normal commands stop at each
  `RateScope`'s safety floor; cancels and reducing orders never stop there.
  `Batching::Declared` packs places and cancels into batches within the venue's declared
  sizes; `Batching::One` keeps one order per command.
- **Batches in a plan** (FBC-hht, decision 0111): under `Batching::Declared`, a `Planned` can
  be a batch place or a cancel-many. Its further orders are `Planned::more_items:
  Vec<BatchItem>` (`Planned::cids()` lists every one). Report each item: a cancel-many item's
  send with `Registry::cancel_sent`, and a batch item's outcome with `Registry::on_outcome`.
- **Required: `DesiredQuote::tag: u16`** (FBC-s6fp, PR #163, decision 0098). This is the
  consumer's opaque tag for the level's order. It is copied onto the level's places and
  amends (`Planned::tag`), kept on the order (`OrderRecord::tag`) and carried on audit
  verdicts.
- **Required: `DesiredQuote::reduce_fallback: ReduceFallback`** (`EmulateReduceOnly` or
  `KeepReduceOnly`; FBC-01w, PR #167, decision 0106). This is what a post-only, reduce-only
  quote becomes on a venue that refuses the two flags together. Each fallback the planner
  applies is reported in `Planned::downgrades` (`Downgrade`). New:
  `ExecutionPlanner::on_fill(reg, caps, acct, market)` re-checks emulated reduce-only orders
  after a fill and cancels any that would overshoot.
- **Removal reasons** (FBC-s6fp, decision 0098): `DesiredBook::with_removal`, `set_removal`
  and `removal` give each side an optional reason. A cancel of a level the book no longer
  wants carries its side's reason (`Planned::removal`, `OrderRecord::removal`).
- **Crossing exits** (FBC-7aw4, PR #180, decision 0110): `DesiredBook::with_crossing`,
  `set_crossing` and `crossing` take `CrossingIntent { cap, qty, tag, attempt }`. The planner
  plans one reduce-only order at `cap`, as an IOC where the venue takes IOC with reduce-only
  and otherwise as a synthetic IOC (a GTC limit at `cap` followed by its cancel). It sizes the
  order so the position never crosses zero, and plans at most one order per `attempt`. That
  order's level is `CROSSING_LEVEL`. `Planned`, `BatchItem`, `Held` and `Refused` gain a
  `crossing: bool` field.
- **New fields on planner outputs:** `Planned { crossing, tag, removal, downgrades,
  more_items }`, `Held::crossing` and `Refused::crossing`. Code that builds these structs by
  name, in tests for example, must set the new fields.
- **New variants:** `PlanRefusal::{NoRoomToReduce, ReduceOnlyUnsupported, NoSyntheticIoc,
  StaleAttempt, AwaitsEmulatedCancel, Status, VenueMode, RateFloor}` (FBC-01w, FBC-7aw4,
  FBC-hht); `HeldReason::Crossing` (FBC-7aw4); `OmsError::{NotIssued, AccountBound,
  OrphanCooldown}` (FBC-657c, FBC-0d9k, FBC-840); `IssueRefusal::{OtherRegistry,
  OtherAccount, Capped, OrphanCooldown, Released}` (FBC-0d9k, FBC-657c, FBC-840);
  `AmendRefusal::OrphanCooldown` (FBC-840).
- **Authorization binding** (FBC-0d9k, PR #133, decision 0082; FBC-657c, PR #137, decision
  0084): a command is authorized only by the registry that built it, for that registry's
  account (`Registry::bind_account`, `Registry::account`). A place, batch or amend is checked
  against the caps again when it is authorized, so a command retained while a fill moved the
  position is refused. A place that was built but not authorized is held. It is released by
  `Registry::place_not_submitted`, or by the registry's next mutating call once its
  `PermittedCommand` is dropped. An outcome reported for it while it is unissued is
  `OmsError::NotIssued`.
- **Orphans** (FBC-840, PR #171, decision 0108): a resync now registers open orders under our
  namespace that the registry does not hold, and names their markets in the new
  `ResyncReport::orphans: Vec<InstrumentId>`. On such a market the adding side stays
  suppressed until the consumer calls `Registry::cancel_orphans(market, &OrphanConfig, caps,
  now)` and the cooldown that call starts runs out (`Registry::end_orphan_cooldowns(now)`).
  A consumer that never calls `cancel_orphans` never adds on that market again.
  `OrphanConfig::new(cooldown, level_limit)` has no default. The cancels go by explicit
  reference in a cancel-many (`OrphanPlan`, `OrphanCancels`). They escalate to an instrument
  cancel-all only under the kill switch's guard, and never to an account cancel-all.
- **Changed signature: `Registry::on_query_answer(caps, answer, key)`** takes the venue's
  `&OrderCaps` (FBC-wua8, PR #134). The ladder uses it for amend-gives-new-id venues and for
  `snapshot_lag_bound` (FBC-i8lp; `LadderConfig::absence_settle(caps)`).
- **Audit sink** (FBC-cor, PR #161; 0005's I8 in its library form): a new `AuditSink` trait
  with `AuditEntry::{Received, Verdict}`, installed with `Registry::set_audit_sink`. Each raw
  order event goes to the sink in receive order, numbered, before it is applied, ignored ones
  included, and its verdict follows. Installing a sink is optional. The consumer owns its
  `order_audit` table and that table's schema (0001).
- **Unknown-ladder fixes** with no API change: FBC-e90m (PR #131), FBC-k2w9 (PR #132) and
  FBC-tjey (PR #146).

#### fbc-runtime

- **Required: `ExecSessionConfig::nonce_source: NonceSourceId`** (FBC-2pr, PR #115, decision
  0078). This names the nonce source the session's journal records each reservation against.
- **Required: `ExecSessionConfig::outbound: ReservedBound` and `ExecSessionConfig::in_flight:
  InFlight`** (FBC-hwn, PR #174, decision 0105). `ReservedBound::new(normal, reserve)` sets a
  Normal bound plus a Safety reserve and has no default. `InFlight::new(bound)` can be shared
  by every session of a venue. A submission past the bound is refused
  `SubmitRefusal::Backpressure { .. }` (new variant), which journals as
  `NotSent(Backpressure)`. Past the in-flight bound it is `NotSent(RateBudget)`. The consumer
  settles either one as a not-sent receipt. New `ExecCounters` fields:
  `unprotected_refusals`, `refused_redactions`, `backpressure_refusals` and
  `in_flight_refusals`.
- **Unprotected orders** (FBC-nvxn, PR #127, decision 0080; FBC-48j7, PR #140, decision 0088):
  an order resting from an earlier epoch holds its market's places and amends until the
  venue shows it ended, unless the venue declares `covers_open_orders`. New:
  `ExecHandler::on_unprotected` (default method), `ExecOrders::may_place_on` and
  `ExecOrders::unprotected`.
- **HTTP control commands** (FBC-m8vm, decision 0081): an order query or fee query can go as
  one HTTP request through the order-entry session. Paradex's REST order query runs the
  Unknown ladder this way.
- **Journaled request deadlines** (FBC-0hfl, PR #144, decision 0090): every
  `on_rpc_timeout` the session fires is journaled (journal format 7).
- **Shard host** (FBC-ct0, PR #164, decision 0100): `ShardHost`, `Member`, `Changed`,
  `Decide`, `Ladder`, `Unsubmitted` and `ShardError`. One loop runs a shard's sessions and
  writes a Cycle record per decide pass. It submits the consumer's ladder one command at a
  time and polls every session between two commands, so a protective cancel goes ahead of
  the rest of the ladder. Under the kill switch the remaining places and amends are left
  unwritten.
- **Consumer inputs and timers** (FBC-hvqi, PR #172, decision 0103): `ConsumerInputs`,
  `ConsumerInputsConfig { clock, capacity, timers }`, `InputSender`, `InputHandler`,
  `ConsumerInput`, `ConsumerTimers` (`arm` returns a `#[must_use]` `Result<(),
  ArmRefusal>`), `InputCounters`, `InputRefusal`, `InputsError` and `MAX_INPUT_CAPACITY`.
  Inputs enter through a bounded ring; a push into a full ring is refused and counted, and
  never blocks the shard. Each input is stamped with the next ingest sequence and journaled
  before the handler sees it. `MdReplay::set_inputs` hands inputs back in place on replay,
  and `MdReplayCounters` gains `inputs`.
- **In-process order entry** (FBC-6mf, PR #175, decision 0104): `ExecSession::in_process(..)`
  runs a session over an `InProcessStream` whose peer (`OrderEntryPeer`, `PeerFeed`) sees
  the codec's frames and the shard's market data in ingest order. SimVenue is that peer.
- **Closed-loop replay** (FBC-066n, PR #178, decision 0107): `ClosedLoop { clock, records,
  settle, stops }` and `ReplayFailure::{LiveClock, Journal(String), NotUtf8 { ingest_seq }}`. `IngestClock::replayed(wall)`
  and `IngestClock::is_replayed()` are new.
- **Tick-to-wire on order entry** (FBC-qfm, PR #177): `ExecHandler::on_tick_to_wire` (default
  method) reports each Safety write. A cancel that a market-data handler submits on a frame
  is measured from that frame's kernel receive time (Linux).
- **Build guard** (FBC-8mv, PR #119, decision 0079): a build that links fbc-runtime with the
  `log` crate's `release_max_level_trace` feature fails to compile, because tungstenite logs
  each frame, the session token included, at TRACE.

#### fbc-journal

- **Format version 6 to 9; the reader reads 2 to 9.** Version 7 adds
  `Record::RpcTimeout { stamp, rpc }` (FBC-0hfl, decision 0090). Version 8 adds
  `Record::Consumer { at, kind, input, ingest_seq, bytes }`, the consumer's opaque records,
  written through the `JournalSink` in the shard's total order (FBC-hxkm, PR #170, decision
  0101). Version 9 adds the consumer input's `ingest_seq` (FBC-hvqi, decision 0103). A
  v0.0.6 reader refuses these journals and names the version, so tools that read v0.0.7
  journals must move their pin too. A `match` over `Record` needs the two new arms. Decoder
  replay skips consumer records whose `input` flag is false.

#### fbc-sim

- **The stream starts closed** (FBC-fji, PR #162, decision 0097): `SimEngine::open(now)` and
  `SimEngine::close(now, wall)` follow the host. A frame sent before `open`, or after
  `close`, is refused with `SimError::StreamClosed`. On `close`, the engine cancels what the
  stood-in venue cancels on a disconnect or a lapsed dead-man timer. A consumer that drives
  `SimEngine` directly must call `open` first.
- **Two-phase acknowledgement** (FBC-zr1, PR #165, decision 0099; FBC-2zxk, PR #168, decision
  0102): a two-phase venue's placement is answered provisionally, then final once its risk
  window passes, or refused by a risk reject the consumer configures
  (`SimEngine::risk_reject`, `RiskRefusal`). Amends are answered provisionally and settled by
  the amended order's event.
- **SimVenue for Paradex** (FBC-2zxk): SimVenue now stands in for Paradex. It places and fills
  under Paradex's declared `ExecCaps` and `MatchingCaps` where it used to refuse. The
  stand-in test lives in fbc-venue-paradex's tests, so fbc-sim depends on no venue crate.
- **Hosting** (FBC-6mf): `SimPeer` and `PeerCounters`, with `SimEngine::next_due` and
  `SimEngine::end_epoch`. The runtime hosts one SimVenue per bracket on one shard, each on its
  own simulated account.
- **New variants:** `SimError::{StreamClosed, StreamOpen, OutsideRiskWindow, ClientId}`.
- **Deterministic replay** (FBC-w9g, PR #176): recorded market data and a fixed command
  script give the same fills on every run.

#### fbc-conformance (venue adapter authors)

- **`Setup` gains `goldens`, `exec_stream`, `order_entry` and `bootstrap`** (FBC-onw, FBC-whw,
  FBC-3il, FBC-648o). The named suite gains signing_golden, legacy_symbols, fee_sign,
  liquidity_reported, position_signed, decoder_deterministic, encode_deterministic,
  ids_roundtrip, restart_cid, price_grid, continuity, subscriptions_idempotent,
  no_exch_ts_synthesized, book_channels, amend_ack, mixed_batch, unknown_on_timeout,
  resync_after_reconnect, two_phase_ack and reject_coverage (decisions 0083, 0085, 0087,
  0089). An adapter's `suite!` invocation runs them all.
- **New export:** `ConnectionsView` (`StubServer::connections_view`); `ConnRecord` gains
  `client_closed`.
- **New variants:** the fault script's `Step::{Barrier, CloseWith}` (FBC-3il, FBC-xx0).
- **Removed:** the conformance toy's `NoMd` (FBC-u1d, PR #95), now that the toy serves
  market data.
- **New fault scripts:** the 340-reconnect storm against an order-entry session (FBC-e5p),
  the order-lattice faults (FBC-6to) and id-less errors, an idle close and a journal-consumer
  stall (FBC-xx0).

#### fbc-venue-paradex

- **The snapshot is trustworthy** (FBC-2f7w, PR #155, decision 0094): a resync now seeds a
  Paradex market's position. Paradex also declares `snapshot_reads: Separate` (0095),
  `snapshot_lag_bound: 6 s` (0109) and `covers_open_orders: false` (0080).
- **Cancel reasons** (FBC-8xr, PR #153, decision 0093): `user_cancel_ws` and
  `user_cancel_batch_ws` decode as our own cancels (`Canceled(Requested)`).
  `cancel_on_disconnect` and an IOC's `REMAINING_IOC_CANCEL` get their own names. A
  price-band or minimum-size close refuses an IOC.
- **`ORDER_SIZE_BELOW_MIN`** is the common kind `MinNotional`, read by its code (FBC-tyft,
  PR #158, decision 0096).
- **`order.create_batch` sends a bare JSON array** (FBC-mu1o, PR #152). In v0.0.6 the batch
  was sent in a form that Paradex refused live on testnet.
- **New API:** the `discover` module (FBC-l5o, PR #125: `markets_url`, `markets_request`,
  `plan`, `decode_markets`, `parse_fbc_common_symbol`, `MARKETS_PATH`, `MARKETS_TAG`,
  `REQUIRED`); `ModifyRequests` and `MODIFY_OUTCOMES_HELD` (FBC-g3bw, PR #139, decision 0086;
  FBC-ypq9, PR #148, decision 0091).
- **Login and stream errors** (FBC-3f8z, PR #135; FBC-z5om, PR #126): a refused REST login
  is reported with its HTTP status and Paradex's error code and message, never the token,
  signature or key. An auth or subscribe reply that cannot be read closes the stream and
  asks for a reconnect.
- **Conformance:** the crate passes the named conformance suite (FBC-6oj, PR #145, decision
  0092), and a clustered-move benchmark measures the protective-cancel p99 on Linux
  (FBC-vgm, PR #179).

#### fbc-venue-binance-usdm

- **New API:** the `discover` module (`EXCHANGE_INFO_PATH`, `EXCHANGE_INFO_TAG`,
  `decode_exchange_info`) and `parse_fbc_common_symbol` (FBC-fwf, PR #173).

#### fbc-examples (owner-run samples, never run by CI against a venue)

- New: `testnet_trade`, `testnet_quote` (with a supervised `--mainnet` mode),
  `paradex_testnet_acceptance` and `mainnet_smoke`. No library API.

### By crate

Each ticket is listed under the crate it mainly changed.

| Crate | Ticket | PR | What | Decision |
| --- | --- | --- | --- | --- |
| fbc-core + fbc-oms | FBC-k7t7 | #156 | `OrderCaps::snapshot_reads`; the shown-cumulative-fill rule is refused on separate reads | 0095 |
| fbc-core + fbc-oms | FBC-i8lp | #181 | `OrderCaps::snapshot_lag_bound`; no absence counted within it | 0109 |
| fbc-oms | FBC-e90m | #131 | An order with a tombstone cancel outstanding is never resolved off the Unknown ladder | |
| fbc-oms | FBC-k2w9 | #132 | A ladder answer naming another order's venue id is never applied | |
| fbc-oms | FBC-0d9k | #133 | A command is bound to its registry and account and checked against the caps again at authorization | 0082 |
| fbc-oms | FBC-wua8 | #134 | A snapshot under another venue id, with an amend unconfirmed, does not resolve the order | |
| fbc-oms | FBC-657c | #137 | A held place is never stranded | 0084 |
| fbc-oms | FBC-tjey | #146 | An order on the Unknown ladder is resolved once a later command is refused | |
| fbc-oms | FBC-cor | #161 | Audit sink: every raw order event in receive order before it is applied | (0005 I8) |
| fbc-oms | FBC-s6fp | #163 | Level tags and removal reasons on planned commands and audit verdicts | 0098 |
| fbc-oms | FBC-01w | #167 | Flag-conflict fallbacks and emulated reduce-only, from `OrderCaps` alone | 0106 |
| fbc-oms | FBC-840 | #171 | Orphans: adding side suppressed, cancelled by explicit reference | 0108 |
| fbc-oms | FBC-7aw4 | #180 | Price-capped reduce-only crossing intent, never crossing zero | 0110 |
| fbc-oms | FBC-hht | #182 | Declared batches, venue-mode and status gates, rate safety floor | 0111 |
| fbc-runtime | FBC-2pr | #115 | The order-entry session journals what it sends and receives, credential spans hashed | 0078 |
| fbc-runtime | FBC-nvxn | #127 | Orders from an earlier epoch are unprotected until an arm covers them | 0080 |
| fbc-runtime | FBC-m8vm | #130 | HTTP-carried order Query and FeeQuery through the order-entry session | 0081 |
| fbc-runtime | FBC-48j7 | #140 | The unprotected-order gate keeps its evidence by market | 0088 |
| fbc-runtime | FBC-0hfl | #144 | Request deadlines journaled (journal format 7) | 0090 |
| fbc-runtime | FBC-ct0 | #164 | Shard host | 0100 |
| fbc-runtime | FBC-hvqi | #172 | Consumer inputs and timers, stamped, journaled and replayed (journal format 9) | 0103 |
| fbc-runtime | FBC-hwn | #174 | Bounded outbound queue and in-flight table, each with a Safety reserve | 0105 |
| fbc-runtime | FBC-qfm | #177 | Kernel receive to write done for every protective cancel | |
| fbc-runtime + fbc-sim | FBC-066n | #178 | Closed-loop replay host in virtual time | 0107 |
| fbc-journal | FBC-hxkm | #170 | Consumer records (journal format 8) | 0101 |
| fbc-sim | FBC-fji | #162 | SimVenue cancels on disconnect | 0097 |
| fbc-sim | FBC-zr1 | #165 | SimVenue's two-phase acknowledgement | 0099 |
| fbc-sim | FBC-2zxk | #168 | SimVenue stands in for Paradex | 0102 |
| fbc-sim | FBC-6mf | #175 | The runtime hosts SimVenue: three brackets on one shard | 0104 |
| fbc-sim | FBC-w9g | #176 | Deterministic replay through SimVenue | |
| fbc-conformance | FBC-u1d | #95 | The toy's books and keepalive | |
| fbc-conformance | FBC-onw | #121 | signing_golden, legacy_symbols | |
| fbc-conformance | FBC-whw | #122 | fee_sign, liquidity_reported, position_signed, decoder_deterministic | |
| fbc-conformance | FBC-2re | #123 | encode_deterministic, ids_roundtrip, restart_cid, price_grid | |
| fbc-conformance | FBC-vmw | #124 | continuity, subscriptions_idempotent, no_exch_ts_synthesized, book_channels | |
| fbc-conformance | FBC-ja3 | #128 | The toy's REST resync, exec keepalive, URLs with redaction spans | |
| fbc-conformance | FBC-3il | #129 | amend_ack, mixed_batch, unknown_on_timeout | 0083 |
| fbc-conformance | FBC-y6y | #136 | resync_after_reconnect, two_phase_ack, reject_coverage | 0085 |
| fbc-conformance | FBC-648o | #138 | A Setup hook that authenticates each codec before the encoding checks | |
| fbc-conformance | FBC-wyv2 | #141 | The order-entry checks' registry starts untrustworthy | 0087 |
| fbc-conformance | FBC-3pv6 | #142 | mixed_batch takes the venue's batch failure semantics | 0089 |
| fbc-conformance | FBC-2905 | #143 | A case file answers a resync over HTTP | |
| fbc-conformance | FBC-pn85 | #147 | Event-driven waits with a wall-clock watchdog | |
| fbc-conformance | FBC-6to | #160 | Order-lattice fault scripts | |
| fbc-conformance | FBC-e5p | #166 | The 340-reconnect storm against an order-entry session | |
| fbc-conformance | FBC-xx0 | #169 | Id-less errors, idle close, journal-consumer stall | |
| fbc-venue-paradex | FBC-8mv | #119 | Offline rehearsal against the stub server; TRACE compiled out | 0079 |
| fbc-venue-paradex | FBC-l5o | #125 | Instrument discovery and the legacy-symbol hook | |
| fbc-venue-paradex | FBC-z5om | #126 | An unreadable auth or subscribe reply asks for a reconnect | |
| fbc-venue-paradex | FBC-3f8z | #135 | A refused REST login reported with status and error code | |
| fbc-venue-paradex | FBC-g3bw | #139 | A modify's request_info is news once | 0086 |
| fbc-venue-paradex | FBC-6oj | #145 | Passes the conformance suite | 0092 |
| fbc-venue-paradex | FBC-ypq9 | #148 | `ModifyRequests` stays bounded | 0091 |
| fbc-venue-paradex | FBC-mu1o | #152 | `order.create_batch` as a bare array | |
| fbc-venue-paradex | FBC-8xr | #153 | Testnet acceptance evidence; recorded cancel reasons | 0093 |
| fbc-venue-paradex | FBC-2f7w | #155 | The snapshot source flips to Trustworthy | 0094 |
| fbc-venue-paradex | FBC-vgm | #179 | Clustered-move protective-cancel p99 benchmark (Linux) | |
| fbc-venue-binance-usdm | FBC-fwf | #173 | Discovery and `parse_fbc_common_symbol` | |
| fbc-examples | FBC-x69b | #114 | `testnet_trade` | |
| fbc-examples | FBC-elg7 | #149 | `testnet_quote` | |
| fbc-examples | FBC-xji8 | #150 | `paradex_testnet_acceptance` | |
| fbc-examples | FBC-6fnc | #151 | The acceptance program's testnet fill step | |
| fbc-examples | FBC-p0aa | #154 | `mainnet_smoke` | |
| fbc-examples | FBC-hpg6 | #157 | Positions follow fills, checked against GET /positions | |
| fbc-examples | FBC-tyft | #158 | `testnet_quote --mainnet`; Paradex `ORDER_SIZE_BELOW_MIN` as MinNotional | 0096 |
| fbc-examples | FBC-aoh5 | #159 | A cancel of ours that we did not send warns once and Stops on a repeat | |

### Every ticket, in landing order (scripts/release-notes.sh)

The generated list: each ticket's first commit, date and title.

- **FBC-2pr** fbc-runtime: the order-entry session journals everything it sends and receives, credential spans hashed — 2026-10-07 · 5c17ced (the order-entry session journals what it sends and receives, each nonce it reserves and each context it gives its codec, credential spans hashed)
- **FBC-onw** fbc-conformance suite: signing_golden and legacy_symbols — 2026-10-08 · a0b441f (the named suite gains signing_golden and legacy_symbols; the conformance toy passes both)
- **FBC-whw** fbc-conformance suite: fee_sign, liquidity_reported, position_signed and decoder_deterministic from fixture frames — 2026-10-08 · d74b73b (the named suite gains fee_sign, liquidity_reported, position_signed and decoder_deterministic, read from fixture frames; the conformance toy passes all four)
- **FBC-u1d** fbc-conformance: the conformance toy's books and keepalive: two channels on one connection, whole-frame snapshots, refused book ids, a REST anchor, gap health — 2026-10-06 · d123703 (The conformance toy's books: two channels on one connection kept apart, whole-frame snapshots, refused book ids, a REST anchor, gap health and a keepalive)
- **FBC-2re** fbc-conformance suite: encode_deterministic, ids_roundtrip, restart_cid and price_grid — 2026-10-08 · 7333ad9 (the named suite gains encode_deterministic, ids_roundtrip, restart_cid and price_grid; the conformance toy passes all four)
- **FBC-8mv** Offline rehearsal: fbc-oms, the order-entry session and the Paradex codec against the stub server keep every safety rule — 2026-10-07 · 4cc2713 (the offline rehearsal runs fbc-oms, the order-entry session and the Paradex codec against the stub, and the log facade's TRACE is compiled out)
- **FBC-l5o** Paradex instrument discovery and the legacy-symbol hook: GET /markets into drafts, X/USDT into X-USD-PERP — 2026-10-08 · 2b91f56 (Paradex discovers its instruments from GET /markets and reads Java-era X/USDT tickers as X-USD-PERP)
- **FBC-z5om** Paradex exec codecs: an auth or subscribe reply that cannot be read closes the stream and asks for a reconnect, as a refused one does — 2026-10-08 · a95f37e (an auth or subscribe reply that cannot be read closes the Paradex exec stream and asks for a reconnect, as a refused one does (Reviewer B RB-4lp7-1 on PR #112))
- **FBC-vmw** fbc-conformance suite: continuity, subscriptions_idempotent, no_exch_ts_synthesized and book_channels — 2026-10-08 · dc4c0e9 (the named suite gains continuity, subscriptions_idempotent, no_exch_ts_synthesized and book_channels; the conformance toy passes all four)
- **FBC-ja3** fbc-conformance: the conformance toy's REST resync with retry, exec keepalive timer, configured URLs with redaction spans, and required fill fields — 2026-10-08 · cf292d4 (The conformance toy's REST resync with retry, exec ping, configured URLs with redaction spans, and required fill fields)
- **FBC-nvxn** fbc-runtime: orders resting from an earlier epoch are unprotected until the venue shows a new connection's cancel-on-disconnect arm covers them — 2026-10-08 · 0df5efd (an order of ours resting from an earlier epoch holds its market until the venue shows it ended, unless the venue declares that an arm covers open orders (RB-olg-5 on PR #81))
- **FBC-m8vm** fbc-runtime: HTTP-carried control commands (order Query, FeeQuery) through the order-entry session, so Paradex's REST order query can run the Unknown ladder — 2026-10-08 · a05b867 (an order query or fee query may go as one HTTP request, charged with its encode, its deadline standing once its epoch ends (decision 0081))
- **FBC-e90m** fbc-oms: an order with any tombstone cancel outstanding is never resolved off the Unknown ladder — 2026-10-08 · 4b31eaf (an order stays on the Unknown ladder until every tombstone cancel sent for it is answered)
- **FBC-k2w9** fbc-oms: a ladder query answer whose snapshot names a venue id another order of ours holds is never applied — 2026-10-08 · 1c1b8f3 (a ladder query answer whose snapshot names a venue id another order of ours holds applies nothing)
- **FBC-0d9k** fbc-oms: a capped command is bound to the registry and account that judged it, re-judged when authorized, and a place reported not sent frees its exposure only with its permit — 2026-10-08 · 85be3df (a permitted command is authorized only by its registry, for its account, judged against the caps again, and a place not sent is withdrawn only with its command (decision 0082))
- **FBC-wua8** fbc-oms: a ladder snapshot showing an order under a venue id other than its record's, with an amend unconfirmed on a venue whose amend gives a new id, does not resolve it — 2026-10-08 · 92d33be (a ladder snapshot showing an order under another venue id than its record's, while an unconfirmed amend may have replaced it, resolves nothing)
- **FBC-3il** fbc-conformance suite: amend_ack, mixed_batch and unknown_on_timeout over the stub server and the runtime's order-entry session — 2026-10-08 · 9b321bf (the named suite gains amend_ack, mixed_batch and unknown_on_timeout, run on fbc-runtime's order-entry session against the stub with fbc-oms authorizing)
- **FBC-3f8z** Paradex exec codecs report a refused REST login with its HTTP status and Paradex's error code/message (never the token, signature or key), as an event the consumer can show — 2026-10-08 · 6927738 (a refused Paradex login is reported with its HTTP status and Paradex's error code and message)
- **FBC-657c** fbc-oms: a held (unissued) place is never stranded — dropping its PermittedCommand releases it, and any outcome for it while unissued is refused NotIssued — 2026-10-08 · 53e89e7 (a held place is never stranded: dropping its command releases it, and no outcome of it applies (decision 0084))
- **FBC-y6y** fbc-conformance suite: resync_after_reconnect, two_phase_ack and reject_coverage — 2026-10-08 · 528177a (the conformance suite's resync_after_reconnect, two_phase_ack and reject_coverage over the stub server and the runtime's order-entry session)
- **FBC-g3bw** Paradex: a later OrderEvent still carrying an earlier modify's request_info is not a second reject or amend of the order — 2026-10-08 · 7195ab3 (a later Paradex OrderEvent repeating an earlier modify's request_info is the order update alone (decision 0086))
- **FBC-648o** fbc-conformance suite: a Setup hook that brings each freshly built codec to its authenticated state before the encoding checks run — 2026-10-08 · b60ea0e (the suite takes every order-entry codec it builds through the setup's bootstrap to its authenticated state before a check encodes)
- **FBC-wyv2** fbc-conformance: the order-entry checks' own registry starts on a venue whose OrderCaps.snapshot_source is Untrustworthy — 2026-10-08 · fca8ba7 (the order-entry checks' own registry starts on a venue whose snapshot is untrustworthy)
- **FBC-48j7** exec_gate: facts are pruned to markets with something held, so an order held for good does not grow them per reconnect; admits() allocates nothing when nothing is unprotected; 0080's wording matches the code — 2026-10-08 · 3a8d8d2 (the unprotected-order gate keeps venue-id evidence by market and prunes it to held markets; admits() allocates nothing (decision 0088))
- **FBC-3pv6** fbc-conformance: mixed_batch takes the venue's batch failure semantics (a per-item error Unknown, a missing item Unknown in the same reply) rather than a fixed accept/reject/silent scenario — 2026-10-09 · 0fff388 (mixed_batch judges a refused and an unanswered batch item as the setup declares its venue reports them (decision 0089))
- **FBC-2905** fbc-conformance: an order-entry case file answers a resync over HTTP, so restart_cid runs on a venue that resyncs over REST — 2026-10-09 · 8721ea1 (a case file answers a resync over HTTP with an http line, so restart_cid runs on a venue that resyncs over REST)
- **FBC-0hfl** fbc-runtime: an order-entry session journals each request deadline it fires into on_rpc_timeout, at the stamp its events carry — 2026-10-09 · 2d57152 (an order-entry session journals each request deadline it fires into on_rpc_timeout, at the stamp its events carry (decision 0090))
- **FBC-tjey** fbc-oms: an order whose amend or cancel that put it on the Unknown ladder is still unanswered is resolved off it once a later command is refused — 2026-10-09 · 7dca303 (an order stays on the Unknown ladder while the amend or cancel whose fate is unknown is unanswered)
- **FBC-pn85** fbc-conformance: the order-entry checks wait for the opening and a submission by an event with a wall-clock watchdog, not a count of yields — 2026-10-09 · 0916f1c (the order-entry checks wait for the stub's script position and the session by events, bounded by a wall-clock watchdog, never a count of yields)
- **FBC-ypq9** Paradex ModifyRequests stays bounded for a long-lived order requoted by amending: entries settled by a later event are dropped, not only on CLOSED — 2026-10-09 · 6287305 (a Paradex order's modify outcomes are held in a window of its latest 32, a PENDING not at all (decision 0091))
- **FBC-6oj** fbc-venue-paradex passes the conformance suite — 2026-10-09 · 8a11fc1 (fbc-venue-paradex passes the named conformance suite; its order events give a placement its final phase (decision 0092))
- **FBC-x69b** Sample testnet_trade: log in to Paradex TESTNET, resync, place one post-only order away from the touch through fbc-oms, confirm the ack, cancel it (owner-run) — 2026-10-07 · b2881e2 (testnet_trade places and cancels one post-only order on Paradex testnet through fbc-oms, owner-run)
- **FBC-xji8** Acceptance program paradex_testnet_acceptance: drives FBC-8xr's script on Paradex TESTNET (place, batch, amend, cancel, cancel-many, cancel-on-disconnect, REST-vs-stream snapshot comparison) and writes the evidence report (owner-run) — 2026-10-11 · 26564dd (paradex_testnet_acceptance runs FBC-8xr's script on Paradex testnet through fbc-oms and writes the evidence report, the REST-vs-stream table and scrubbed fixtures)
- **FBC-elg7** Sample testnet_quote: a TESTNET-ONLY fixed-offset two-sided quoting loop through the ExecutionPlanner and fbc-oms, cancel-and-replace only, a plumbing test not a strategy (owner-run) — 2026-10-11 · 80ffb20 (testnet_quote, a TESTNET-only fixed-offset two-sided quoting loop through the ExecutionPlanner and fbc-oms, cancel-and-replace only (a plumbing test, not a strategy))
- **FBC-mu1o** Paradex order.create_batch sends its params as a bare JSON array of signed order requests, not {"orders":[..]} (refused live on testnet) — 2026-10-15 · f08af27 (Paradex order.create_batch sends its params as the bare array of signed order requests, and reads the array form's documented reply)
- **FBC-6fnc** paradex_testnet_acceptance gains the owner-approved testnet fill step (one ~$11 IOC that fills, then a reduce-only flatten) so the report shows GET /positions following fills; plus Codex's evidence-accuracy findings on d3367da — 2026-10-15 · 2cc13c8 (paradex_testnet_acceptance's testnet fill steps behind --allow-fill (a take flattened by a reduce-only IOC, a partly filled order's amend), every round judged against what the stream shows once REST answered, a disconnect left resting never waited on twice, one deadline per book read, an off-grid book named as a flag to check (Codex r4285575623, r4285575635, r4285575668, r4285575649))
- **FBC-8xr** Paradex testnet acceptance with the owner: place, batch create, amend, cancel, cancel-many and cancel-on-disconnect, one run each (owner-assisted, not sprint) — 2026-10-16 · 9a4f4a8 (the three Paradex testnet acceptance runs' reports and scrubbed fixtures under fixtures/paradex/exec-recorded/, decoded by the exec decoders, and decision 0093 correcting the cancel reasons they contradicted)
- **FBC-2f7w** Record the decision: Paradex snapshot_source flips to Trustworthy (or stays Untrustworthy) on FBC-8xr's evidence — 2026-10-16 · 777450a (decision 0094 flips Paradex's snapshot_source to Trustworthy on FBC-8xr's testnet evidence, with the owner's condition that positions following fills is verified in the first mainnet sessions that get fills)
- **FBC-p0aa** Sample mainnet_smoke: the owner's first Paradex MAINNET order — testnet_trade's path on a separate small account, one ~$11 post-only order away from the touch, cancel, Stop, behind an explicit --mainnet confirmation (owner-run) — 2026-10-16 · 28185d7 (failing tests for the mainnet_smoke sample (the stub round trip on a trustworthy stand-in, the mainnet guards, the trust refusal, the mark's minimum notional, the mainnet-only credentials))
- **FBC-k7t7** fbc-core/fbc-oms: a venue declares whether its resync reads orders and positions at one instant; the shown-cumulative-fill rule refused where it does not — 2026-10-17 · 439923a (failing tests for a venue's declared resync reads (a snapshot of two reads never places a fill by its order's shown cumulative fill or absence, in either read order; one read keeps 0055's placement; arrival and time still place; no default; Paradex declares two reads))
- **FBC-hpg6** Verify positions follow fills (owner condition in 0094): after the first fill and at Stop the session compares GET /positions with the registry's position and Stops on a mismatch that a re-read confirms — 2026-10-17 · c1ce20d (failing tests for the position check (GET /positions compared with the registry after the first fill and at Stop): agreeing goes on, a mismatch the re-read clears goes on, one that persists Stops with no place after and fails the run, the Stop comparison on every run, in testnet_quote and testnet_trade (mainnet_smoke's path))
- **FBC-tyft** testnet_quote gains a supervised MAINNET mode: the fixed-offset loop on a separate small account, $11 per side / $50 inventory, offset at least 20 bps, cancel-and-replace only, behind --mainnet (owner-run, short sessions) — 2026-10-18 · 1686f1b (failing tests for testnet_quote's supervised MAINNET mode and the 9 Codex findings carried from PR #149)
- **FBC-cor** fbc-oms: every raw order event reaches the audit sink in receive order before it is applied (I8, library form) — 2026-10-18 · 72fc66c (failing tests for the audit sink: every raw order event in receive order before it is applied, each followed by its verdict)
- **FBC-fji** fbc-sim: SimVenue cancels on disconnect where the stood-in venue does — 2026-10-18 · 9a24e23 (failing tests for SimVenue cancelling on disconnect where the stood-in venue does)
- **FBC-6to** fbc-conformance: fault scripts for NEW or PENDING_CANCEL after CANCELED, an equal-key partial and cancel, and fills before order events, with the order lattice holding — 2026-10-18 · 3153eaa (fault scripts for a NEW and a PENDING_CANCEL after CANCELED, an equal-key partial fill and cancel, and a fill before its order's events, played against the order-entry session with the toy and fbc-oms)
- **FBC-s6fp** fbc-oms: DesiredBook levels carry an opaque consumer tag and each side a removal reason, copied onto planned commands and audit verdicts — 2026-10-18 · 90beaf4 (failing tests for consumer tags and removal reasons: each place and amend carries its level's tag, each cancel of a removed level its side's reason, and the audit sink's verdicts both)
- **FBC-zr1** fbc-sim: SimVenue acknowledges in two phases where the stood-in venue does — 2026-10-18 · e9a52b9 (failing tests for SimVenue's two-phase acknowledgement)
- **FBC-e5p** fbc-conformance: the 340-reconnect storm against an order-entry session: nothing resent, protection and resync before any order, every epoch — 2026-10-18 · f96721b (the 340-reconnect storm against the order-entry session: nothing resent, protection and resync before any order, every epoch)
- **FBC-ct0** fbc-runtime: a shard host that drains ready inputs, writes a Cycle record, and submits one command at a time with a poll of the shard's sockets between signatures — 2026-10-18 · 2c00241 (failing tests for the shard host: a ladder from one decide pass, reference frames read between two of its commands, a handler's cancel ahead of the rest, a kill switch leaving every remaining place and amend unwritten, one Cycle per pass)
- **FBC-xx0** fbc-conformance: fault scripts for id-less error frames, an idle close and a journal-consumer stall against the order-entry session — 2026-10-18 · c47ba0c (failing tests for the id-less error, the idle close with a venue code and the stalled journal writer)
- **FBC-aoh5** Mainnet test programs: a cancel of our order we did not send warns once and Stops on a repeat (another program on the account) — 2026-10-18 · 3866150 (failing tests for a cancel of our order the program did not send: WARN once, a repeat Stops a mainnet run)
- **FBC-hxkm** fbc-journal: consumer records, opaque to the library, written and read in the shard's total order — 2026-10-18 · 72187bb (failing tests for consumer records: written through the JournalSink among market-data records and read back in place, refused by the previous version's reader at the header naming the version, and passed over by decoder replay)
- **FBC-2zxk** fbc-sim: SimVenue stands in for Paradex, placing and filling under Paradex's declared ExecCaps and MatchingCaps instead of refusing — 2026-10-18 · 52d2432 (failing tests for SimVenue standing in for Paradex from its declared caps)
- **FBC-hvqi** fbc-runtime: consumer inputs and consumer timer firings enter a shard stamped, journaled and replayed at their ingest position — 2026-10-18 · a38ebe0 (failing tests for consumer inputs: pushed from another thread and fired from a handler's timer during a toy market-data session, stamped with the next ingest sequence, journaled before the handler sees them, replayed in place without a live timer, and a push into a full ring refused and counted)
- **FBC-fwf** Binance USD-M discover and parse_fbc_common_symbol — 2026-10-18 · b06bf4e (failing tests for Binance USD-M discovery and its Java-era ticker rule)
- **FBC-6mf** fbc-runtime hosts SimVenue in place of a real venue's order entry: three brackets on one shard, each on its own simulated account — 2026-10-19 · 16f5d37 (failing tests for SimVenue hosted by the runtime: three brackets on one shard, each an in-process order-entry session on its own simulated account)
- **FBC-hwn** fbc-runtime: bounded outbound queues per order-entry connection and a bounded RPC table, each with a Safety reserve — 2026-10-19 · cf457b1 (failing tests for the outbound queue's and the in-flight table's bounds, each with a Safety reserve)
- **FBC-w9g** Replaying recorded market data and a fixed command script through SimVenue gives the same fills on every run — 2026-10-19 · 3712a4f (a journaled toy market-data day replays through SimVenue, one fresh venue per bracket, to the same fills on every run)
- **FBC-qfm** fbc-runtime: kernel receive to write done recorded for every protective cancel on the order-entry session — 2026-10-19 · 79c889b (failing test: a cancel the handler submits on a market-data frame reports its tick-to-wire on the order-entry session)
- **FBC-vgm** Clustered-move benchmark: protective-cancel kernel-receive-to-write-done p99 through the shard host and the Paradex codec, on Linux — 2026-10-19 · 42a32db (failing test: a clustered move during a Paradex ladder through the shard host, every protective cancel after at most one signature in progress, its p99 reported)
- **FBC-01w** fbc-oms: the planner's generic flag-conflict fallbacks and emulated reduce-only, from OrderCaps alone — 2026-10-18 · 184ec3f (failing tests for the planner's flag-conflict fallbacks and emulated reduce-only)
- **FBC-066n** Closed-loop replay host: replayed market data drives a consumer whose own commands SimVenue answers in virtual time, identical on every run — 2026-10-19 · a61ad32 (failing tests for a closed-loop replay through the shard host, answered by a SimVenue per bracket in virtual time)
- **FBC-840** fbc-oms: orphans from a trustworthy resync: the adding side suppressed and the orphans cancelled by explicit reference, escalating only under the exclusive lease (I7) — 2026-10-18 · c35d47d (failing tests for orphans: the adding side suppressed for the cooldown, the orphans cancelled by explicit reference, escalating only under the exclusive lease)
- **FBC-i8lp** fbc-core/fbc-oms: a venue declares how long its open-order snapshot may lag its order stream, and the Unknown ladder's settle below it is refused — 2026-10-19 · d121e5d (failing tests for a venue's declared snapshot lag bound (an order absent from snapshots within the bound of its send is never Lost whatever the ladder's settle, one past it is; a longer settle still governs; no default; Paradex declares its bound over the recorded lags))
- **FBC-7aw4** fbc-oms: a price-capped reduce-only crossing intent in the DesiredBook, never crossing zero, every cap and state still applied — 2026-10-19 · 3236806 (failing tests for the DesiredBook's price-capped reduce-only crossing intent)
- **FBC-hht** fbc-oms: the planner batches within the declared sizes, gates on venue modes and instrument status, and stops Normal traffic at the safety floor — 2026-10-19 · ba93639 (failing tests: the planner batches within the declared sizes, gates on venue modes and status, and stops normal traffic at the safety floor)

### Archived tickets: the text of each ticket, from Beads

#### FBC-2pr fbc-runtime: the order-entry session journals everything it sends and receives, credential spans hashed — 2026-10-07 · 5c17ced

Intent: 0006 for order entry: everything that crosses the shard boundary is journaled, and no credential reaches it except as a keyed hash (0009, 0024, 0028).

Scope in crates/fbc-runtime: inbound frames and HTTP results with the spans the codec's redact_inbound names (as FBC-s69 does for market data); outbound frames with their opcode (FBC-q7b) and redaction spans; inbound control frames (FBC-drf); HTTP requests with their rpc; timer firings; nonce blocks, encode contexts and write results (FBC-ec9's records); acks, fills, cancels and reducing orders under the Safety class. Tests use the conformance toy and fbc-core's auth_toy, by path as fbc-journal's tests use it. The journal stall under order entry is proven by FBC-xx0.
Error paths are tested too, so the workspace stays above the coverage floor (coverage-floor.txt, 0.2 points of slack).

Serves BT-402. Decisions: 0006, 0009, 0021, 0024, 0028.

**Done when:** Done when a test in crates/fbc-runtime/tests/ journals order-entry sessions of the conformance toy and of fbc-core's auth_toy, reads back in order every inbound frame with its stamp, outbound frame with its opcode and spans, control frame, HTTP request and result, timer firing, nonce block, encode context and write result, and finds none of the synthetic credentials in the journal files.

#### FBC-onw fbc-conformance suite: signing_golden and legacy_symbols — 2026-10-08 · a0b441f

Intent: BT-502's first criterion (design §6, restated):
- signing_golden: the fixture's golden commands, encoded with the fixture's EncodeCtx, equal the committed golden bytes, signatures included.
- legacy_symbols: every Java-era ticker in the fixture parses through the factory's parse_fbc_common_symbol (FBC-ahf).

Scope in crates/fbc-conformance: both checks, and the conformance toy's own parse_fbc_common_symbol so legacy_symbols is not vacuous on it.
Error paths are tested too, so the workspace stays above the coverage floor (coverage-floor.txt, 0.2 points of slack).

Serves BT-502. Decisions: 0003, 0004, 0006.

**Done when:** Done when the conformance toy passes signing_golden and legacy_symbols through the suite macro with its fixture directory, and tests show signing_golden failing against a toy variant that changes one signed byte and legacy_symbols failing on a ticker its hook cannot parse.

#### FBC-whw fbc-conformance suite: fee_sign, liquidity_reported, position_signed and decoder_deterministic from fixture frames — 2026-10-08 · d74b73b

Intent: BT-502's first criterion, checks that read an adapter's fixture frames (design §6, restated):
- fee_sign: a maker rebate fill decodes to a Fee below zero and a taker fill to one above zero (0004).
- liquidity_reported and position_signed: fills carry the liquidity their frame states; positions are signed, positive long.
- decoder_deterministic: the same frames decode to identical events on two runs.
A check is skipped, by name and saying why, only where the venue's caps make it vacuous (a market-data-only venue has no fills).

Scope in crates/fbc-conformance, on the conformance toy with a fixture directory of its own.
Error paths are tested too, so the workspace stays above the coverage floor (coverage-floor.txt, 0.2 points of slack).

Serves BT-502. Decisions: 0004, 0006.

**Done when:** Done when the conformance toy passes fee_sign, liquidity_reported, position_signed and decoder_deterministic through the suite macro with its fixture directory, and tests show fee_sign and decoder_deterministic each failing against a toy variant that flips a fee's sign or decodes nondeterministically.

#### FBC-u1d fbc-conformance: the conformance toy's books and keepalive: two channels on one connection, whole-frame snapshots, refused book ids, a REST anchor, gap health — 2026-10-06 · d123703

Intent: FBC-z6v's third slice, the toy's MdCodec books: Codex r4172231010 (two BookIds on one connection, their levels never merged, 0014 item 7), r4172835744 (a snapshot frame decoded whole or not at all, 0014 item 2), r4172917339 (a book id outside the declared channels refused, nothing sent), r4172835757 (a REST anchor where BookCaps.rest_anchor), Health events naming the feed that broke, and MdCodec::keepalive. The runtime's market-data toy (crates/fbc-runtime/tests/common/toy.rs) stays the runtime tests' minimal venue; this one is the capability-complete one; neither is copied.

Scope in crates/fbc-conformance/src/toy/ and crates/fbc-conformance/tests/.
Error paths are tested too, so the workspace stays above the coverage floor (coverage-floor.txt, 0.2 points of slack).

Serves BT-502. Decisions: 0003, 0014, 0027.

**Done when:** Done when tests in crates/fbc-conformance/tests/ show the conformance toy's MdCodec keeping two BookIds' levels apart on one connection, pushing nothing from a snapshot frame it cannot decode whole, refusing a book id outside its declared channels with nothing sent, anchoring a rest_anchor book on its HTTP snapshot, pushing Health Gap naming the feed that broke, and declaring a keepalive that fbc-runtime sends at its interval to a local server.

#### FBC-2re fbc-conformance suite: encode_deterministic, ids_roundtrip, restart_cid and price_grid — 2026-10-08 · 7333ad9

Intent: BT-502's first criterion, the checks that need only the factory, its caps, a spec table and an EncodeCtx (design §6, restated):
- encode_deterministic: the same command and EncodeCtx encode to identical bytes at different real times (0006).
- ids_roundtrip: fbc-core's client-id codec under the adapter's ClientIdFormat round-trips minted ids, and the fixture's Java-era ids decode as Unparseable (0004).
- restart_cid: a mint restarted while own orders rest, as the venue's resync fixture shows them, never re-mints a live or recent id (0004).
- price_grid: every quantized price is valid under the venue's grid, and post-only prices stay maker-safe at grid boundaries.

Scope in crates/fbc-conformance, run on the conformance toy through the suite macro.
Error paths are tested too, so the workspace stays above the coverage floor (coverage-floor.txt, 0.2 points of slack).

Serves BT-502. Decisions: 0004, 0006, 0014.

**Done when:** Done when the conformance toy passes encode_deterministic, ids_roundtrip, restart_cid and price_grid through the suite macro, and a test shows encode_deterministic failing against a toy variant whose encode depends on the real time.

#### FBC-8mv Offline rehearsal: fbc-oms, the order-entry session and the Paradex codec against the stub server keep every safety rule — 2026-10-07 · 4cc2713

Intent: before anything touches testnet, the whole order path with the real Paradex codec runs offline against fbc-conformance's stub server, proving 0012 and 0013 end to end. Cancels, the orphan cancels of I7 included, may go out on a disarmed market before Start: 0012 starts every market disarmed in Cancel-only and lets cancels through in every state, and 0013 rule 2 exempts cancels.

Scope: a test in crates/venues/fbc-venue-paradex/tests/ with fbc-oms (planner, gate, states, leases in a temporary directory), fbc-runtime's order-entry session and the Paradex codec, against the stub server answering from the requests it reads (FBC-xg7) with Paradex's documented replies (login, auth acknowledgement, cancel-on-disconnect, create, fills, resync); tracing captured at TRACE level for the whole session.

Serves BT-402. Decisions: 0005, 0009, 0012, 0013.

**Done when:** Done when a test in crates/venues/fbc-venue-paradex/tests/ runs fbc-oms, fbc-runtime's order-entry session and the Paradex codec against fbc-conformance's stub server and shows no place or amend frame after a fresh start until Start, Start refused before the first resync's end, the arm accepted before the first order frame, a place over a cap, a place or amend while Killed and any ordinary order after the switch is lifted never written while a cancel is, Flatten after a restart writing only reduce-only exit orders that never cross zero, a placed order acknowledged and filled moving inventory once, a reconnect re-authenticating, re-arming and resyncing with nothing re-placed, a second holder of the market lease unable to arm, and no token, login signature or key in the TRACE output or the journal.

#### FBC-l5o Paradex instrument discovery and the legacy-symbol hook: GET /markets into drafts, X/USDT into X-USD-PERP — 2026-10-08 · 2b91f56

Intent: FBC-ahf declares VenueFactory::discover and parse_fbc_common_symbol and implements them for fbc-core's toy only; Paradex needs its own, which the conformance suite's legacy_symbols check (FBC-onw, run by FBC-6oj) calls.

Scope in crates/venues/fbc-venue-paradex: discover's plan as effects (GET /markets) with a parser building InstrumentSpecDrafts through DecodeScope, refusing a market missing a required field; parse_fbc_common_symbol mapping the Java-era X/USDT to X-USD-PERP and refusing anything else. A hand-built /markets response in fixtures/paradex/rest/.
Error paths are tested too, so the workspace stays above the coverage floor (coverage-floor.txt, 0.2 points of slack).

Serves BT-402. Decisions: 0003, 0004.

**Done when:** Done when tests in crates/venues/fbc-venue-paradex/tests/ decode a hand-built /markets response in Paradex's documented shape into drafts through DecodeScope, refuse one missing a required field, and map BTC/USDT to BTC-USD-PERP while refusing a ticker of another form.

#### FBC-z5om Paradex exec codecs: an auth or subscribe reply that cannot be read closes the stream and asks for a reconnect, as a refused one does — 2026-10-08 · a95f37e

From Reviewer B RB-4lp7-1 on PR #112 (FBC-4lp7). crates/venues/fbc-venue-paradex/src/exec/read_only.rs ~301-309: an auth reply that on_text still refuses as Malformed (result and error both present, an error whose code is not an integer, or neither member) leaves conn.auth pending with no signal: no Closed event, no Reconnect, every command NotSent(Disconnected), and the runtime has no auth watchdog (only decode_errors counts it). Fail-safe (nothing is sent) but a liveness hole: the owner's testnet run (FBC-x69b) would sit silent. Java's ParadexOrderWebSocketClient.onAuthResponse treats any non-null error as an auth failure and closes the socket.

**Done when:** Done when tests in crates/venues/fbc-venue-paradex/tests show an unreadable auth reply and an unreadable subscribe reply each end the stream with a Closed event and a reconnect request, for both ReadOnlyExec and ParadexExec, and a valid auth reply still authenticates.

#### FBC-vmw fbc-conformance suite: continuity, subscriptions_idempotent, no_exch_ts_synthesized and book_channels — 2026-10-08 · dc4c0e9

Intent: BT-502's first criterion, the market-data checks (design §6, restated):
- continuity: the fixture's sequence breaks are detected as the BookCaps declare; on the toy, its own sequence field. A frame whose binary block is longer than expected is a fixture-driven sub-case only a binary-protocol venue supplies (Paradex, FBC-6oj), skipped by name elsewhere.
- subscriptions_idempotent: subscribing the same set twice through the reconciler sends nothing the second time.
- no_exch_ts_synthesized: an event carries exch_ts only when its frame does.
- book_channels: a fixture frame's channels match the book's declared includes_channels.

Scope in crates/fbc-conformance, on the conformance toy.
Error paths are tested too, so the workspace stays above the coverage floor (coverage-floor.txt, 0.2 points of slack).

Serves BT-502. Decisions: 0003, 0006, 0022.

**Done when:** Done when the conformance toy passes continuity, subscriptions_idempotent, no_exch_ts_synthesized and book_channels through the suite macro with its fixture directory, the longer-block sub-case reports itself skipped by name for the toy's text protocol, and a test shows continuity failing against a toy variant that ignores a sequence break.

#### FBC-ja3 fbc-conformance: the conformance toy's REST resync with retry, exec keepalive timer, configured URLs with redaction spans, and required fill fields — 2026-10-08 · cf292d4

Intent: FBC-z6v's fifth slice: Codex r4172917335 (resync over REST, retried on HttpFailure, the response validated before any push), r4172835747 (an exec ping armed on open and re-armed on each firing), r4172917294 (a base URL from VenueConfig keeps the spans its configuration marks into WireUrl, and one with user information or a query the toy cannot mark is refused; the toy builds no credential, so nothing here is a review path), r4172917309 (every field its FillCaps declare required on the wire).

Scope in crates/fbc-conformance/src/toy/ and crates/fbc-conformance/tests/.
Error paths are tested too, so the workspace stays above the coverage floor (coverage-floor.txt, 0.2 points of slack).

Serves BT-502. Decisions: 0009, 0014, 0027.

**Done when:** Done when tests in crates/fbc-conformance/tests/ show the conformance toy's exec codec resyncing over REST, retrying on an HttpFailure and pushing nothing from a response it cannot decode whole, arming an exec ping on open and re-arming it on each firing, carrying a configured base URL's redaction spans into its WireUrl and refusing one with user information it cannot mark, and refusing a fill frame missing any field its FillCaps declare.

#### FBC-nvxn fbc-runtime: orders resting from an earlier epoch are unprotected until the venue shows a new connection's cancel-on-disconnect arm covers them — 2026-10-08 · 0df5efd

Intent: Reviewer B's RB-olg-5 on PR #81 (FBC-olg). Paradex's order.cancel_on_disconnect page says the setting is 'scoped to the connection and does not persist across reconnects' and cancels orders 'placed (or already tracked)' when the socket disconnects 'unexpectedly'; no page says whether an arm on a new connection covers orders that were already open when it was enabled. FBC-w19 arms before placing on each epoch, which protects orders placed on that epoch only. An order that survived an expected close of epoch N (a server close, or our own close on a stall reconnect) then rests on epoch N+1 under an arm that may not cover it, against the owner's rule that no venue without cancel-on-disconnect may place orders.

Scope in crates/fbc-runtime (beside FBC-w19): after an epoch's arm is accepted and its resync has ended, orders the resync shows resting that were placed on an earlier epoch are treated as unprotected unless the venue's caps declare that an arm covers already-open orders; unprotected orders are cancelled (a Safety cancel per order, through the OMS's authorization) before any place or amend on that market. The caps field that would declare coverage, if one is needed, is part of this ticket. FBC-8xr's testnet run confirms whether Paradex's arm on a new connection covers orders already open; a confirming record lets the runtime keep them.

Error paths are tested too, so the workspace stays above the coverage floor (coverage-floor.txt, 0.2 points of slack). Decisions: 0005, 0013, 0014.

**Done when:** Done when a runtime test with a scripted peer shows an order resting from an earlier epoch cancelled after the new epoch's arm and resync and before any place or amend on its market, unless the venue's caps declare that an arm covers already-open orders, in which case a second test shows it kept.

#### FBC-m8vm fbc-runtime: HTTP-carried control commands (order Query, FeeQuery) through the order-entry session, so Paradex's REST order query can run the Unknown ladder — 2026-10-08 · a05b867

Intent: FBC-0ga's decision record (PR #87) makes everything the order-entry session encodes WebSocket-only: carries() refuses any encode with an Effect::Http as NotSent(Unencodable), because an HTTP request gets no deadline in Rpcs, its result is dropped once its epoch ends and the rate pre-charge covers frames only. That covers control commands too. Paradex's caps declare query_refs from 'Get order by client id' (GET /orders/by_client_id/{client_id}, crates/venues/fbc-venue-paradex/src/exec/caps.rs), so 0005's Unknown-ladder order query (ControlCommand::Query), and a REST fee query, would always be NotSent(Unencodable) on Paradex. Conservative (nothing sent) but it leaves Paradex's Unknown ladder without its query step. FBC-4nfb covers HTTP order entry only. Scope in crates/fbc-runtime: a control command whose encode carries its request as Effect::Http is admitted, its HTTP request charged with the encode, given a deadline, and its result (or the epoch's end, or no result) resolved once; may share FBC-4nfb's machinery. Found by PR #87 Reviewer B B8.

**Done when:** Done when tests in crates/fbc-runtime/tests/ with a toy codec show a ControlCommand::Query (and FeeQuery) whose encode carries its request as Effect::Http requested once with its charge taken with the encode (a refusal NotSent(RateBudget), nothing requested), its result reaching the codec on the epoch that asked, and one whose result its epoch's end drops (or never comes) handed to on_rpc_timeout once at its deadline, never requested again; an order-affecting command over HTTP still NotSent(Unencodable) until FBC-4nfb; a decision record amending FBC-0ga's WebSocket-only rule for control commands.

#### FBC-e90m fbc-oms: an order with any tombstone cancel outstanding is never resolved off the Unknown ladder — 2026-10-08 · 4b31eaf

Discovered from FBC-711: Codex r4188432413 on PR #72 (fix rounds spent). The ladder remembers every tombstone request sent, but a later tombstone replaces the record's intent; when that later one is NotSent or refused (not AlreadyTerminal), the intent is None while an earlier tombstone is still unanswered. A query answer or resync then showing the order Open resolves it (resolve() looks only at state and intent), leave_ladder forgets every tombstone request, a late final acceptance of the earlier one is treated as an ordinary cancel's, and the order gets a Live permit although that cancel may have removed it. Treat any remembered tombstone without an answer as unresolved: the order stays on the ladder until every tombstone sent has been answered, or it ends.

Serves BT-402. Decisions: 0005.

**Done when:** Done when a test in crates/fbc-oms/tests/ shows an order with an earlier tombstone unanswered and a later one refused stays on the ladder (no Live permit) when a query answer or resync shows it Open, and the earlier tombstone's final acceptance then ends it Canceled.

#### FBC-k2w9 fbc-oms: a ladder query answer whose snapshot names a venue id another order of ours holds is never applied — 2026-10-08 · 1c1b8f3

Discovered from FBC-711: Codex r4188432397 on PR #72 (fix rounds spent). Registry::on_query_answer checks the answer's target against the queried order, but an Unknown order queried by client id alone (target OrderRef::Client) accepts a snapshot carrying any venue id, since QueryAnswer::new only checks the target's own ids. When snap.vid is already indexed (by_vid) to another order of ours, the queried record would learn that order's venue id and state and could leave the ladder, then name the wrong order in later commands. Refuse it as on_resync does for a conflicting entry: nothing applied, the query spent, resyncs decide (LadderResolution::TargetMismatch).

Serves BT-402. Decisions: 0005.

**Done when:** Done when a test in crates/fbc-oms/tests/ shows a ladder query by client id answered with a snapshot whose venue id another order of ours holds applies nothing to either order, the queried one staying Unknown on the ladder with resyncs deciding.

#### FBC-0d9k fbc-oms: a capped command is bound to the registry and account that judged it, re-judged when authorized, and a place reported not sent frees its exposure only with its permit — 2026-10-08 · 85be3df

Intent: 0005's I6 (the inventory cap) holds when a command is authorized and sent, not only when it is built. Codex on PR #74 (FBC-2e4) found four ways a PermittedCommand built under the cap can outlive the state it was judged on: (r4189335960) a fill moves the position (e.g. an own-namespace fill the registry does not hold) while a cap-sized place is retained, and the place is still authorizable; (r4189335969) a PermittedCommand carries no account or registry, so a place judged against account A's position can be authorized for account B; (r4189335956) a place or batch item reported NotSent through on_outcome frees its exposure by client id while the caller keeps the command; (r4189335949) an amend's build token is (client id, build number) only, so another registry's command with the same client id and build can release this registry's reservation.

Scope in crates/fbc-oms: a PermittedCommand records the registry (and through it the account) whose caps admitted it; Authorization issuance from it checks that origin and re-judges the cap against the registry's current position and resting; a place or batch item's not-sent report consumes its PermittedCommand (as Registry::amend_not_submitted does for an amend); build tokens are unique across registries. Error paths are tested too, so the workspace stays above the coverage floor (coverage-floor.txt, 0.2 points of slack).

Serves BT-402. Decisions: 0005, 0013, 0045.

**Done when:** Done when tests in crates/fbc-oms/tests/ show a retained place refused at authorization after a fill took the position to the cap, a command from one registry refused when authorized for another account, a place's not-sent report taking its PermittedCommand by value (no release by client id alone), and one registry's amend command unable to release another registry's reservation.

#### FBC-wua8 fbc-oms: a ladder snapshot showing an order under a venue id other than its record's, with an amend unconfirmed on a venue whose amend gives a new id, does not resolve it — 2026-10-08 · 92d33be

Discovered from FBC-711: Codex r4188432405 on PR #72 (fix rounds spent). On a venue whose amend replaces the venue id (keeps_venue_id false), a query answer or resync snapshot can show an order with an unconfirmed amend under its new id. The ladder turns the snapshot into an Open update; OrderRecord::apply_update adopts a venue id only when none is known or an Amended event names new_vid, so the record keeps the retired id while the snapshot's price and total confirm the amend and resolve the ladder; later commands then carry the retired id, which ChosenRef prefers to the client id. Prefer refusing over modelling: while OrderRecord::id_may_have_moved holds and the snapshot's venue id differs from the record's, the snapshot does not resolve the order (inconclusive; a tombstone by client id follows).

Serves BT-402. Decisions: 0005, 0014.

**Done when:** Done when a test in crates/fbc-oms/tests/ shows, on a keeps_venue_id false venue, an order with an amend unconfirmed shown by a ladder query answer and by a resync under a venue id other than its record's is not resolved off the ladder and no later command names the retired id.

#### FBC-3il fbc-conformance suite: amend_ack, mixed_batch and unknown_on_timeout over the stub server and the runtime's order-entry session — 2026-10-08 · 9b321bf

Intent: BT-502's first criterion, checks that need the venue answering over a connection (design §6, restated):
- amend_ack: an accepted amend surfaces as OrderUpdate Amended; where the venue's amend ack is RpcReplyOnly, synthesized from the reply.
- mixed_batch: a batch with some items accepted, some rejected and one unanswered gives each item its outcome, and the timeout marks every remaining item Unknown.
- unknown_on_timeout: an unanswered order request becomes Unknown and is never written a second time.

Scope in crates/fbc-conformance: the checks run fbc-runtime's order-entry session against the stub server answering from the requests it reads (FBC-xg7); commands are authorized through fbc-oms as live (FBC-ob2).
Error paths are tested too, so the workspace stays above the coverage floor (coverage-floor.txt, 0.2 points of slack).

Serves BT-502. Decisions: 0005, 0014, 0025.

**Done when:** Done when the conformance toy passes amend_ack, mixed_batch and unknown_on_timeout through the suite macro, each running fbc-runtime's order-entry session against the stub server with replies computed from the requests, and a test shows mixed_batch failing against a toy variant that reports every item of a timed-out batch Accepted.

#### FBC-3f8z Paradex exec codecs report a refused REST login with its HTTP status and Paradex's error code/message (never the token, signature or key), as an event the consumer can show — 2026-10-08 · 6927738

Found by the owner's first testnet run of testnet_trade (2026-10-09): the order socket's connection ended 5 times in 15 s and the run stopped with 'TIMEOUT login', printing no reason. The coordinator probed the testnet order socket (SBE 1:2 handshake opens and stays open) and read crates/venues/fbc-venue-paradex/src/exec/read_only.rs on_http: when the REST login gives no token, the codec closes the connection ('the Paradex login gave no token') and asks for a reconnect, but the HTTP status and Paradex's error body (e.g. an invalid-signature or account-not-found code) are reported nowhere — LoginCycle::on_answer's error is dropped — so neither the owner nor the sample can tell a wrong key, an L1 address, a mainnet key on testnet or a non-onboarded account apart. Java logs the code and message of a refusal. Note for the owner (not code): Paradex derives the L2 key per network, so a mainnet key does not log in on testnet. Probe also showed Paradex now offers SBE schema version 3 (its 400 for a query-less URL says 'append ?sbeSchemaId=1&sbeSchemaVersion=3'); version 2 still works — file a separate ticket if a move to 3 is needed.

**Done when:** Done when tests show a refused login (HTTP 4xx with a Paradex error body) and a transport failure each produce an event naming the HTTP status and Paradex's error code and message, with no token, signature, key or account secret in it (checked by the same secret search the rehearsal uses), and the connection still closes and reconnects as before.

#### FBC-657c fbc-oms: a held (unissued) place is never stranded — dropping its PermittedCommand releases it, and any outcome for it while unissued is refused NotIssued — 2026-10-08 · 53e89e7

Reviewer B on PR #133 (FBC-0d9k), open at merge. RB133-2: a held place whose PermittedCommand is dropped (early return, let _ =, passed to amend_not_submitted, an OtherRegistry refusal) can never be released: on_outcome(NotSent) returns NotIssued, the ladder never escalates it (no sent_at), and cancellable().cancel() gives AwaitAck, so no cancel is sent. The order stays PendingNew and keeps its cap headroom until restart; under the owner's $11 per-side resting cap, one leaked place blocks that side — exactly what the testnet_quote loop (FBC-elg7) would hit. RB133-1: on_outcome refuses only NotSent/Rejected for a held place; Accepted and Unknown outcomes by client id still apply and the held command stays authorizable (scratch-confirmed), so a misreporting consumer can send an order the ladder already resolved. crates/fbc-oms/src/permit.rs ~122, registry.rs ~662.

**Done when:** Done when tests in crates/fbc-oms/tests show a dropped PermittedCommand for a held place releases its order and cap headroom, and an Accepted or Unknown outcome for an unissued place is refused NotIssued with the command no longer authorizable.

#### FBC-y6y fbc-conformance suite: resync_after_reconnect, two_phase_ack and reject_coverage — 2026-10-08 · 528177a

Intent: BT-502's first criterion (design §6, restated):
- resync_after_reconnect: a reconnect leads to a resync and to no order re-placed.
- two_phase_ack: on a TwoPhase venue a provisional acceptance is followed by a final one or an asynchronous reject within the window; on a SinglePhase venue no provisional acceptance appears.
- reject_coverage: every venue code in the fixture's reject table maps to a RejectKind, Other only where the table says so.

Scope in crates/fbc-conformance: over the stub server and fbc-runtime's order-entry session as FBC-3il; the tests take the leases (0013).
Error paths are tested too, so the workspace stays above the coverage floor (coverage-floor.txt, 0.2 points of slack).

Serves BT-502. Decisions: 0005, 0013, 0014, 0025.

**Done when:** Done when the conformance toy passes resync_after_reconnect, two_phase_ack and reject_coverage through the suite macro over the stub server, and a test shows resync_after_reconnect failing against a toy variant that re-places an open order after a reconnect.

#### FBC-g3bw Paradex: a later OrderEvent still carrying an earlier modify's request_info is not a second reject or amend of the order — 2026-10-08 · 7195ab3

Intent: Reviewer B's RB85-1 on PR #85 (FBC-aml). Paradex's 'Get order' REST page returns request_info as a field of the order, so it is plausibly an attribute that persists on later updates of the same order, not news about one modify. FBC-aml's decoder treats every version-2 event's request_info as fresh: it throws away requestId (the only field tying a status to one modify), so a later update (a fill, say) still carrying an earlier amend's REJECTED pushes another AsyncReject{op: Amend}, and a stale SUCCESS turns a later update into Amended instead of Open (both confirmed by a scratch test in review).

Failure scenario: amend #1 is REJECTED; amend #2, which raises the total, is in flight; a fill update still carrying #1's REJECTED looks like a reject of #2. A consumer that settles #2 on it while the venue applies #2 undercounts resting exposure, putting the I6 inventory cap and the per-side resting cap at risk. Nothing consumes AsyncReject yet (fbc-core only defines it), so this is P2, not P1; it must land before any consumer settles a Paradex amend from an AsyncReject.

Scope in crates/venues/fbc-venue-paradex/src/exec/: either carry requestId to the consumer with the reject/amend, or have the codec report request_info only when an order's (requestId, requestStatus) changes, whichever the session's codec state allows (a choice beyond 0054 is a new decision record). The Paradex consumer must never settle an amend in flight on such a reject alone. FBC-8xr's testnet run supplies the evidence: does request_info persist on later updates, and what requestId echoes (our request's JSON-RPC id, or the venue's).

Serves BT-402. Decisions: 0054.

**Done when:** Done when a test in crates/venues/fbc-venue-paradex/tests/ decodes two hand-built OrderEvents of one order carrying the same requestId with REJECTED (and, separately, with SUCCESS) for MODIFY_ORDER, the second a later fill update, into exactly one AsyncReject of the amend (one Amended), the second event an order update in the order's own state, and a new requestId's status is reported again.

#### FBC-648o fbc-conformance suite: a Setup hook that brings each freshly built codec to its authenticated state before the encoding checks run — 2026-10-08 · b60ea0e

Discovered by Codex on PR #121 (FBC-onw, r4216200681). Every encoding check in the suite (caps_truthful, commands_selfcontained, signing_golden; and encode_deterministic and price_grid from FBC-2re) encodes on a codec straight from VenueFactory::exec_codec. ParadexExec::encode returns NotSent(Disconnected) for every command except a query until its session has authenticated on the connection (crates/venues/fbc-venue-paradex/src/exec/codec.rs, encode). So FBC-6oj cannot pass those checks on Paradex as the suite stands now. The conformance toy encodes without authenticating, which is why the suite has not hit this. Proposed: Setup gains an optional bootstrap that takes a freshly built codec through on_open and the venue's recorded authentication reply (fed through on_frame in the venue's DecodeScope) before any check encodes. The harness applies it wherever it builds a codec. commands_selfcontained's 'nothing seen' still means no order state; an authenticated session holds no order state. Serves BT-502 and FBC-6oj. Decisions: 0005, 0014, 0071.

**Done when:** Done when Setup carries an optional bootstrap that the suite's harness applies to every codec it builds before encoding, a conformance-toy variant that refuses NotSent(Disconnected) until authenticated passes caps_truthful, commands_selfcontained and signing_golden with the bootstrap and fails each without it, and the crate docs describe the hook.

#### FBC-wyv2 fbc-conformance: the order-entry checks' own registry starts on a venue whose OrderCaps.snapshot_source is Untrustworthy — 2026-10-08 · fca8ba7

Found running the suite on Paradex for FBC-6oj (a local run on the FBC-648o head). Live::run's Oms::new (crates/fbc-conformance/src/suite/live.rs) arms fbc-oms on the setup's first instrument after a flat ResyncSnapshot of the suite's own making, resynced under the venue's own OrderCaps. Paradex declares snapshot_source Untrustworthy (decision 0054), so that resync never seeds the position (decision 0055) and Registry::start refuses with PositionUnknown(InstrumentId(1)). Every order-entry check (amend_ack, mixed_batch, unknown_on_timeout, resync_after_reconnect, two_phase_ack, reject_coverage) then fails before it exercises anything, on any venue that declares Untrustworthy or None. With the suite's own snapshot taken under a copy of the caps whose snapshot_source is Trustworthy (a local probe), resync_after_reconnect and unknown_on_timeout pass on Paradex. Options: (a) take the suite's own flat snapshot as trustworthy, since the suite made it and the stub answers flat by construction; (b) seed by hand under Registry::for_testnet_run(TestnetRun::owner_assisted()), which decision 0067 reserves for the owner's testnet run. Recommendation (a): it is test-only and leaves 0067's TestnetRun to the consumer. No production code changes either way. Blocks FBC-6oj, which blocks FBC-8xr's chain.

**Done when:** Done when the six order-entry checks start fbc-oms (no PositionUnknown refusal) on a conformance-toy variant declaring OrderCaps.snapshot_source Untrustworthy, shown by a test in crates/fbc-conformance/tests/, and the toy declaring Trustworthy still passes them.

#### FBC-48j7 exec_gate: facts are pruned to markets with something held, so an order held for good does not grow them per reconnect; admits() allocates nothing when nothing is unprotected; 0080's wording matches the code — 2026-10-08 · 3a8d8d2

Reviewer B on PR #127 (FBC-nvxn), open at merge. RB-nvxn-5 (P3, raised here to P2 as an unbounded growth): crates/fbc-runtime/src/exec_gate.rs ~411-413 resets facts only when nothing is held after a resync; while an order stays held, every later epoch's resync window adds facts for every market and none are pruned (probe: one order held on INST, 20 reconnects with 10 cancel events each elsewhere -> facts.ended.len() 200). An order held for good (a lagging id where amends issue new ids, a query answering found: None, or a consumer without on_unprotected) grows it without bound — the cost decision 0080 says the runtime avoids. RB-nvxn-3: Gate::admits builds a Vec<InstrumentId> on every place/batch/amend even when nothing is unprotected (latency path). RB-nvxn-6: 0080 says a consumer without on_unprotected leaves markets held 'for the epoch' (they stay held across epochs) and other wording no longer matches.

**Done when:** Done when a test shows facts stay bounded across many reconnects while one order stays held, admits() returns without allocating when nothing is unprotected, and 0080 (or an augmenting record) describes the code as it is.

#### FBC-3pv6 fbc-conformance: mixed_batch takes the venue's batch failure semantics (a per-item error Unknown, a missing item Unknown in the same reply) rather than a fixed accept/reject/silent scenario — 2026-10-09 · 0fff388

Codex r4223435597 on PR #129 (FBC-3il). mixed_batch answers a batch of three Accept, Reject, Silent and requires item 1 Rejected and item 2 Unknown only after its deadline. Under decision 0069 the Paradex codec maps a per-item batch error to Unknown and a reply holding fewer results than items to Unknown for the rest in the same decode call (crates/venues/fbc-venue-paradex/src/exec/reply.rs), while a coded top-level error rejects the whole request. A conforming Paradex codec therefore fails mixed_batch on item 1 (not Rejected) and item 2 (reported before the clock moves). The toy, which the FBC-3il done line covers, holds answered items until the request completes and is unaffected. Scope: let the adapter's OrderEntryStub (or its caps) state how its venue answers a refused item and an unanswered item of a batch (refused item: Rejected or Unknown; unanswered item: at the deadline or in the reply), and judge mixed_batch by that statement; or skip by name where the venue's batch protocol cannot answer an item silently. Must land before FBC-6oj runs the suite on Paradex.

**Done when:** Done when mixed_batch passes a toy variant answering as decision 0069 has Paradex answer (a per-item error Unknown, a missing item Unknown in the reply), declared so by its setup, still fails the toy that reports every item of a timed-out batch Accepted, and a test shows a variant breaking its declared semantics fails.

#### FBC-2905 fbc-conformance: an order-entry case file answers a resync over HTTP, so restart_cid runs on a venue that resyncs over REST — 2026-10-09 · 8721ea1

Found preparing FBC-6oj. restart_cid (crates/fbc-conformance/src/suite/ids.rs) needs restart_cid/resting.frames to show orders of ours resting in a resync's answer (ExecEvent::ResyncOrder). A case file's `resync` line calls ExecCodec::resync, and the case format (src/suite/frames.rs) has only `text` and `hex` frame lines: it cannot answer the HTTP reads that resync asks for. Paradex resyncs over REST only (GET /orders and GET /positions, crates/venues/fbc-venue-paradex/src/exec/read_only.rs, start_resync), and its order events (OrderEvent, schema 1:2) give ExecEvent::Order, never ResyncOrder. So restart_cid cannot pass on Paradex: it reports 'shows no order of ours resting in a resync's answer'. The frames.rs docs already say a venue whose fills or positions arrive only in HTTP responses is not modelled. Scope: a case line that answers the codec's oldest unanswered HTTP request (or the one its tag names), for example `http <status> <body>` with the body as text on one line, handed to ExecCodec::on_http in the venue's decode scope; restart_cid, position_signed and decoder_deterministic read it like any other line. Blocks FBC-6oj.

**Done when:** Done when a conformance-toy variant that resyncs over HTTP passes restart_cid from a case whose resync is answered by an http line, a variant whose HTTP answer shows nothing of ours resting still fails restart_cid, and frames.rs documents the line.

#### FBC-0hfl fbc-runtime: an order-entry session journals each request deadline it fires into on_rpc_timeout, at the stamp its events carry — 2026-10-09 · 2d57152

Found while building FBC-2pr: the order-entry session stamps each request deadline that falls due unanswered from the shard clock (ExecSession::time_out while connected, Between::apply while it waits to reconnect, decision 0057) and hands it to the codec's on_rpc_timeout, which reports the request Unknown. No journal record carries that firing: replay cannot call on_rpc_timeout where the live session did, and the ingest sequence the stamp took is a gap no Degraded marker explains (the gap 0041 closed for control frames). FBC-2pr's done line names codec timers, which Record::Timer carries; a deadline firing is not a codec timer, and its TimerTag space is the codec's, so it needs a record kind of its own. Scope: in crates/fbc-journal a record holding the stamp and the request id, in a new format version whose reader still reads the older ones (or the unreleased version 6, as 0040/0041 shared version 5, if no version-6 journal has shipped); in crates/fbc-runtime the session writes it under Safety (it settles an order-affecting request to Unknown) where it stamps the firing, connected or waiting to reconnect. Serves BT-402. Decisions: 0006, 0041, 0057.

**Done when:** Done when a test in crates/fbc-runtime/tests/ journals an order-entry session whose request is unanswered at its deadline, once while connected and once while waiting to reconnect, reads back a record naming the request at the stamp its Unknown event carried, finds the session's ingest sequences with no gap a Degraded marker does not explain, and segments of the previous format version still read.

#### FBC-tjey fbc-oms: an order whose amend or cancel that put it on the Unknown ladder is still unanswered is resolved off it once a later command is refused — 2026-10-09 · 7dca303

Discovered from FBC-e90m (PR #131, Reviewer B RB-e90m-1). The PR keeps an order on the ladder while any tombstone is unanswered, but the same reasoning applies to the ordinary amend or cancel whose Unknown (or NotFound) outcome put the order on the ladder (OrderRecord::ladder_cause). A later command replaces the intent; when it is refused or not sent (a safety cancel, or a tombstone answered that way), intent is None, and resolve() (ladder.rs) lets a query answer or resync showing the order Open take it off the ladder with a Live permit, although the first cancel may still remove it (its late final acceptance then lands as (_, Accepted) => Unchanged, the record staying Open). Reproduced on f68a1f7: cancel_unanswered (cancel 50 Unknown), then cancel 4 refused (Other) and a resync showing Open => resolved Open, live() Ok; same with tombstone 9 refused and a query answer. intent_settled already documents that a later command settled leaves the first one's fate open, but resolve() does not consult it. Decide whether resolve() should also require the ladder cause answered (an Unknown cause then holding the order on the ladder until a tombstone ends it), consistent with FBC-e90m. Serves BT-402. Decisions: 0005.

**Done when:** Done when a test in crates/fbc-oms/tests/ shows an Open order whose cancel went Unknown, after a later safety cancel or tombstone is refused, stays on the ladder (no Live permit) when a query answer or resync shows it Open, and the first cancel's late final acceptance then ends it Canceled.

#### FBC-pn85 fbc-conformance: the order-entry checks wait for the opening and a submission by an event with a wall-clock watchdog, not a count of yields — 2026-10-09 · 0916f1c

Codex r4225615869 on PR #129 (head f4c48fb). Ctx::ready and Ctx::send (Ctx::settle) and Ctx::answered give up after TURNS (100,000) scheduler yields. On a loaded host the loopback connection, the opening's replies or the request's write could take longer, and a conforming adapter would then fail loudly (opening never took places / request not sent). This is never a silent pass (unlike the gate failure FBC-3il fixed with Step::Barrier), but it is load-sensitive. Replace the yield bound with waits on events (the stub's script position, the handler's on_submitted, may_place) bounded by a wall-clock watchdog, keeping the failure-path tests fast (the watchdog configurable per test).

**Done when:** Done when ready(), send() and answered() end on an event or a wall-clock watchdog rather than a yield count, the existing failure-path tests (mute opening, refusing reply, request not sent) still fail within 5 s each, and suite_orders passes 50 runs in a row with another cargo build running.

#### FBC-ypq9 Paradex ModifyRequests stays bounded for a long-lived order requoted by amending: entries settled by a later event are dropped, not only on CLOSED — 2026-10-09 · 6287305

Reviewer B RB139-1 on PR #139 (FBC-g3bw). crates/venues/fbc-venue-paradex/src/exec/order.rs ~106: ModifyRequests::is_news only inserts; entries leave only by forget on CLOSED. A resting order requoted by amending (the normal maker pattern) adds about two (requestId, status) entries per amend without bound — one order amended every 250 ms grows ~690k entries a day. Keep only what a later re-delivery could still repeat (e.g. the pairs since the order's last settled event, or a bounded window with a documented consequence), with decision 0085's rule unchanged.

**Done when:** Done when a test amends one resting order thousands of times and shows ModifyRequests stays within a stated bound while a repeated old request_info is still not reported twice within that bound.

#### FBC-6oj fbc-venue-paradex passes the conformance suite — 2026-10-09 · 8a11fc1

Intent: design §6 step 12 and §14 M2's full conformance suite for Paradex, before any testnet or live order (BT-402 depends on BT-502).

Scope: crates/venues/fbc-venue-paradex/tests/conformance.rs invoking fbc-conformance's suite macro with ParadexFactory and a fixture directory of hand-built frames in Paradex's documented shapes (the continuity check's longer-block sub-case included) and golden encodings whose signatures are the Java vectors' (fixtures/paradex/signing); fbc-conformance and fbc-runtime as dev-dependencies. A defect a check finds that is larger than a local fix becomes a follow-up ticket that blocks FBC-8xr, never a skipped check.

Serves BT-402, and BT-502's suite. Decisions: 0003, 0016, 0025.

**Done when:** Done when crates/venues/fbc-venue-paradex/tests/conformance.rs invokes fbc-conformance's suite macro with ParadexFactory and a fixture directory of hand-built frames and golden encodings whose signatures are the Java vectors', and every check passes or is skipped only as vacuous under Paradex's caps, by name.

#### FBC-x69b Sample testnet_trade: log in to Paradex TESTNET, resync, place one post-only order away from the touch through fbc-oms, confirm the ack, cancel it (owner-run) — 2026-10-07 · b2881e2

Intent: the owner (2026-10-06) wants to try a sample app that places a trade on Paradex; first on TESTNET only (owner choice C: no live Paradex session before a record flips snapshot_source to Trustworthy). Scope: owner-run example (same examples crate as md_watch): refuses any non-testnet base URL or chain id (enforced in code, tested); reads the testnet account address and key from environment variables into fbc-core Secrets in the example's src/auth* (credential code lives on a review path, decision 0009, so this ticket waits for the owner's review); logs in (FBC-mz1), opens the Paradex order-entry session, arms cancel-on-disconnect, resyncs (snapshot_source Untrustworthy: hand-seeding is allowed ONLY for this owner-assisted testnet run per choice C, stated in output), Starts the market under caps (inventory $50, resting one $11 L0 per side), places ONE post-only limit order of about $11 well away from the touch through an fbc-oms Authorization, waits for the ack, cancels it, waits for the cancel ack, prints each step with timings, then Stops (kill switch + cancel all) and exits. Never a crossing order. Decisions: 0005, 0009, 0012, 0013, 0048, 0054.

**Done when:** Done when an integration test runs testnet_trade against a local Paradex stub (conformance stub with scripted login, order acks and cancel acks) with synthetic credentials and asserts the step sequence (login, arm, resync, start, place, ack, cancel, cancel ack, stop) with exactly one place and one cancel sent, a mainnet URL or chain id is refused before any connection, no credential appears in output or logs, and scripts/check.sh passes.

#### FBC-xji8 Acceptance program paradex_testnet_acceptance: drives FBC-8xr's script on Paradex TESTNET (place, batch, amend, cancel, cancel-many, cancel-on-disconnect, REST-vs-stream snapshot comparison) and writes the evidence report (owner-run) — 2026-10-11 · 26564dd

Discovered from FBC-x69b (owner answers 2026-10-07; Beads allows one dependency type per target, so FBC-x69b is recorded as a blocker).

Intent: owner answer Q3 (2026-10-07): "fbc-examples program (Recommended)" — "A new owner-run acceptance program in fueledbychai-rs. About 1-1.5 weeks to the flip. It isn't ticketed yet; I'd file it." This re-answers OQ-smoke-tool for FBC-8xr (owner default 2026-10-04: chaiwala-rs cw-venue smoke, CW-a21). FBC-8xr's run is performed with this program; the 'trust Paradex snapshots' decision ticket cites its report.

Scope: an owner-run, TESTNET-ONLY program in the fbc-examples crate, never run in CI, that drives FBC-8xr's script on Paradex testnet, journaled, one run each, under caps from flags (inventory $50, resting $11 per side) and a registry built for TestnetRun::owner_assisted() (decision 0067, hand seed after the first resync):
1. place (post-only, away from the touch), 2. batch create, 3. amend (one order; records modify of a partly filled order if one arises, order.modify size as the total, request_info persistence and requestId echo for FBC-g3bw), 4. cancel (incl. cancel by client id before the ack if attempted), 5. cancel-many, 6. cancel-on-disconnect (drop the socket; confirm the venue cancelled).
- The REST snapshot comparison for owner answer C: after each step, GET /orders and GET /positions against the orders/fills stream — does the snapshot include every order the stream has acknowledged and omit every order it has ended, how long it lags, do positions match fills; also GET /orders pagination, GET /positions signed size with LONG/SHORT and CLOSED at size 0, and GET /orders-history?client_id= lag (FBC-0sc's items).
- Record every item FBC-8xr's notes list (FBC-uiy fill side/realizedFunding/feeCurrency/fill id repeats; FBC-4zz funding without fill ids, PositionEvent size and CLOSED events; FBC-olg's undocumented values) and map each outcome to the order lattice.
- Per-IP limit check (design §13.3): run from the deployment box; records whether Paradex's per-IP limit covers WebSocket order methods.
- Scrubbed fixture capture: writes a scrubbed slice of the journal for fixtures/paradex/exec-recorded/ (account values replaced, no token; journal keeps keyed hashes only) with a README naming date and method; scripts/privacy-check.sh must pass on it.
- Writes an evidence report (markdown) the snapshot-flip decision record can cite: per step, the commands sent, outcomes, lattice states, the REST-vs-stream table, and pass/fail per question.
- Refuses any non-testnet URL or chain id before connecting; credentials from environment variables via FBC-x69b's src/auth reading (review path, decision 0009); synthetic credentials only in tests.
- docs/: a grep of docs/ on origin/main 87f4e6f finds no OQ-smoke-tool or cw-venue smoke text (it is recorded only in Beads, FBC-8xr and FBC-vgl, and in the design's §6 step 15, outside this repo). This ticket's PR re-greps docs/ and updates any text then naming FBC-8xr's run tool, and documents the program in the fbc-examples README; that docs/ edit is in this ticket's scope.

Blockers: the open tickets FBC-8xr lists before its run (FBC-6oj, FBC-2pr, FBC-8mv, FBC-e8i; FBC-dm9 is closed) plus FBC-x69b. FBC-8xr depends on this ticket.

Decisions: 0005, 0006, 0009, 0016, 0054, 0067.


**Done when:** Done when an integration test runs the program against fbc-conformance's stub server with synthetic credentials and asserts each scripted step (place, batch create, amend, cancel, cancel-many, cancel-on-disconnect) is sent once and its outcome mapped to the order lattice in the written report, the report includes the GET /orders and GET /positions versus stream comparison table, the scrubbed fixture output passes scripts/privacy-check.sh with no token or account value, a mainnet URL or chain id is refused before any connection, and scripts/check.sh passes.

#### FBC-elg7 Sample testnet_quote: a TESTNET-ONLY fixed-offset two-sided quoting loop through the ExecutionPlanner and fbc-oms, cancel-and-replace only, a plumbing test not a strategy (owner-run) — 2026-10-11 · 80ffb20

Discovered from FBC-x69b (owner answers 2026-10-07; Beads allows one dependency type per target, so FBC-x69b is recorded as a blocker).

Intent: owner answer Q2 (2026-10-07): "fbc-examples sample (Recommended)" — "A testnet-only testnet_quote sample in fueledbychai-rs that quotes a fixed offset around the touch, labelled as a plumbing test, not a strategy. About 3-4 days after the first order. It's a public repo, but a fixed offset reveals nothing about your model." Per owner answer Q1 (2026-10-07), FBC-8mv must pass before this continuous loop.

Scope: an owner-run, TESTNET-ONLY program in the fbc-examples crate (beside md_watch and FBC-x69b's testnet_trade), never run in CI. Pipeline, repeatedly: Paradex market data (the touch) -> a fixed-offset two-sided fbc-oms DesiredBook (one ~$11 L0 post-only GTC limit per side; bid/ask offsets from flags) -> fbc-oms ExecutionPlanner -> fbc-oms (caps from flags: inventory $50, resting $11 per side; no defaults) -> fbc-runtime's order-entry session -> Paradex testnet.
- Cancel-and-replace only, never an amend: the planner is handed Paradex's OrderCaps with amend set to None, so permit::amend_shape answers NotAmendable and every change is the planner's cancel, then a place once the order is terminal (no live Paradex amend before FBC-8xr, per FBC-xzp's notes). Paradex declares cancel_before_ack false, so one order never has two commands in flight.
- Cancel-on-disconnect armed and a resync on every epoch (fbc-runtime, FBC-w19); nothing re-placed after a reconnect until the resync ends.
- snapshot_source is Untrustworthy (owner answer C to RB-olg-3): the registry is built with Registry::with_caps(caps).for_testnet_run(fbc_oms::TestnetRun::owner_assisted()) (decision 0067) and each market is seeded by hand after the first resync; a restart needs a new hand seed (nothing re-uses an earlier seed). Before Start, any own open order on the instrument is cancelled (instrument cancel-all under the exclusive lease, never an account cancel-all).
- Stop (kill switch + cancel all) on Ctrl-C, after a --duration flag, on any resync position mismatch, on any order going Unknown (the sample does not drive the Unknown ladder: Paradex has no order query in it yet, FBC-m8vm), and when the order-entry session ends.
- Refuses any non-testnet base URL or chain id (testnet is PRIVATE_SN_POTC_SEPOLIA) before connecting; credentials from environment variables into fbc-core Secrets (reuse FBC-x69b's src/auth reading; a review path, decision 0009); synthetic credentials only in tests.
- Prints each step (plan, each cancel/place with its client id, acks, fills, resyncs, stop) with timings; README and --help say it is a plumbing test, not a strategy, and quote nothing about any model.
- Reuse FBC-x69b's ExecHandler -> Registry glue, promoting it into the fbc-examples library (src/) if both programs need it.

Blockers (verified 2026-10-07 on origin/main 87f4e6f with bd show and the code each names):
- FBC-x69b (first testnet order; the auth/glue it builds) and FBC-8mv (owner Q1: must pass before the continuous loop).
- FBC-ubsw: the replace path sets slot.replacing at crates/fbc-oms/src/planner.rs (the amend_shape-refused branch) and never clears it when the replace-cancel is NotSent/Rejected, so the loop's every replace hits it.
Not blockers, and why:
- FBC-45et, FBC-5fma, FBC-i7mf, FBC-wua8: amend-only; the loop never amends.
- FBC-fto2: Paradex's OrderCaps include Limit, GTC and post_only, so the loop's one shape is never excluded; the sample checks its shape against the caps at startup and refuses to run otherwise.
- FBC-tno9: needs two commands of one order in flight (an amend, or a cancel before the ack); with amend None and cancel_before_ack false that never happens.
- FBC-e90m, FBC-k2w9: Unknown-ladder paths (tombstones, query answers); the sample never calls Registry::ladder and stops on Unknown; Paradex has no order query (FBC-m8vm).
- FBC-6ezy: SubmitRefusal::OtherAccount cannot occur (one account), and Ended means the session ended, on which the loop stops.
- FBC-0d9k: one account, one registry, every command authorized and submitted in the pass that built it and never retained; worst-case inventory counts own resting orders, and unheld own orders are cancelled before Start.
If the loop is ever extended to amend or to drive the Unknown ladder, those tickets become blockers.

Decisions: 0005, 0009, 0012, 0013, 0054, 0060, 0065, 0067.


**Done when:** Done when an integration test runs testnet_quote against fbc-conformance's stub server with synthetic credentials for several requote cycles and asserts no amend frame is ever sent, every replace is a cancel acknowledged before the next place on that level and every place is within the $50 inventory cap and the $11 resting cap per side, after a scripted reconnect an instrument cancel-all is sent before any place, a resync position mismatch Stops it (kill switch + cancel all) with no place after, a mainnet URL or chain id is refused before any connection, no credential appears in output or logs, and scripts/check.sh passes.

#### FBC-mu1o Paradex order.create_batch sends its params as a bare JSON array of signed order requests, not {"orders":[..]} (refused live on testnet) — 2026-10-15 · f08af27

Found by the owner's acceptance run on Paradex TESTNET (2026-10-16, report 20261016T002121Z, PR #151 head 126579d, HYPE-USD-PERP): the batch step's order.create_batch (request 5: buy 0.33 @ 32.7869 and sell 0.09 @ 118.5706, post-only) was refused with JSON-RPC error {code: -32602, message: 'invalid method parameters', data: 'json: cannot unmarshal object into Go value of type []*requests.OrderRequest'} (scrubbed fixture 0016-frame.json). crates/venues/fbc-venue-paradex/src/exec/encode.rs encodes it as {"orders":[..]} (module doc line 17); Paradex wants params to BE the array of order requests. The Java client never implemented create_batch, so the shape came from the docs and was never checked live. order.create, order.modify, order.cancel and order.cancel_batch work live (testnet_quote's Stop batch-cancelled two orders). Also verify the reply shape the decoder expects ({"results":[..]}, reply.rs) against a live batch answer in the next acceptance run, and keep the item-level outcome mapping (decision 0069) unchanged.

**Done when:** Done when a test shows order.create_batch's frame has params as a JSON array of signed order requests (each item as order.create's params), the decoder accepts the batch reply in the shape Paradex documents for the array form, the existing batch tests are updated, and the owner's next acceptance run passes its batch step.

#### FBC-6fnc paradex_testnet_acceptance gains the owner-approved testnet fill step (one ~$11 IOC that fills, then a reduce-only flatten) so the report shows GET /positions following fills; plus Codex's evidence-accuracy findings on d3367da — 2026-10-15 · 2cc13c8

Owner decision 2026-10-15, quoted exactly: question 'To get the positions-follow-fills evidence the trust decision needs, may the acceptance program send one small order that actually fills on testnet?' -> 'Yes, one ~$11 testnet fill (Recommended)' (option text: 'Testnet only: one IOC of about $11 against a resting order, under the $50 inventory cap, then flatten with a reduce-only IOC. That proves GET /positions follows fills, and whether amend size is the total or the remainder on a partly filled order. A small agent round adds the step.'). Context: the owner's first full run (2026-10-15, ETH-USD-PERP, report 20261015T222347Z, DONE ok) answered (a),(b),(d),(e), both cancel-on-disconnect cases and the IOC cancel reason, but (c) positions-follow-fills and the FBC-b0z9 total-vs-remaining amend were 'not exercised'. Scope: (1) a FILL step, TESTNET ONLY, behind an explicit --allow-fill flag: one IOC of about --order-usd that takes resting liquidity, inventory never past --inventory-cap-usd; then compare GET /positions with the stream's fills and PositionEvent; then flatten with a reduce-only IOC on the other side and compare again. It runs only when BOTH sides of the book have resting liquidity (so it can always flatten); otherwise it is skipped with the reason, never leaving a position it cannot flatten. (2) if a book level smaller than the order exists, a partly-filled resting order (a non-post-only GTC limit at that level) is amended to record whether order.modify's size is the total or the remainder (FBC-b0z9); otherwise 'not exercised'. Record fill ids, side, fee currency, realized P&L/funding fields (FBC-uiy/4zz questions). (3) Codex on d3367da: r4285575623 P1 (check positions too before each cancel-on-disconnect, so 'every step passed' cannot coexist with (c) NO); r4285575635 P1 (compare must re-read what the stream expects after REST polling starts, so a lagging REST answer never matches an old snapshot); r4285575668 P1 (a failed cancel-on-disconnect must not keep the order up another --cod-wait before the reconnect's Safety cancel); r4285575649 P2 (the order-book read must not take ~2x --step-timeout). (4) a book size or price off the --tick/--step grid is reported as 'not on the --step/--tick grid; check them against GET /markets', not Malformed. The FBC-iaft resting-cap conversion stays deferred.

**Done when:** Done when stub tests show: the fill step runs only with --allow-fill on a two-sided book, never exceeds the inventory cap, flattens, and its report compares GET /positions with the fills before and after; the partly-filled amend is recorded or 'not exercised'; each Codex item is fixed with a failing-first test; an off-grid book gives the grid message; and scripts/check.sh passes.

#### FBC-8xr Paradex testnet acceptance with the owner: place, batch create, amend, cancel, cancel-many and cancel-on-disconnect, one run each (owner-assisted, not sprint) — 2026-10-16 · 9a4f4a8

Intent: BT-402's third criterion (design §12.1: one Paradex testnet run per command type before M2). It needs a funded testnet account and its credentials, which agents never hold (0009), so this ticket is deferred and is not a sprint ticket: only the owner un-defers it.

Scope, with the owner:
- Run through the consumer's smoke tool (owner question OQ-smoke-tool; recommended chaiwala-rs cw-venue smoke, design §6 step 15), journaled: one run each of place, batch create, amend, cancel, cancel-many and cancel-on-disconnect.
- Confirm or correct FBC-olg's undocumented values (modify of a partly filled order, cancel by client id before the ack, fills replayed after a reconnect, batch sizes, cancel-on-disconnect behaviour) and that order.modify's size is the total; a correction is a new decision record.
- From the deployment box, design §13.3's check of whether Paradex's per-IP limit covers WebSocket order methods, setting the IP rate limit from its result.
- Commit a scrubbed slice of the journal as fixtures under fixtures/paradex/exec-recorded/ (not a SYNTHETIC directory), account values replaced and no token (the journal keeps only keyed hashes), with a README naming the date and the method, and rerun the Paradex exec decoder tests on it.

Serves BT-402. Decisions: 0006, 0009, 0016.

**Done when:** Done when the committed report of one Paradex testnet run per command type (place, batch create, amend, cancel, cancel-many, cancel-on-disconnect) shows each outcome mapped to the order lattice with no lattice violation, the scrubbed recorded frames pass the Paradex exec decoder tests, scripts/privacy-check.sh passes, and any capability value the run contradicted is corrected by a new decision record.

#### FBC-2f7w Record the decision: Paradex snapshot_source flips to Trustworthy (or stays Untrustworthy) on FBC-8xr's evidence — 2026-10-16 · 777450a

Intent: owner answer C to RB-olg-3 (2026-10-06): Paradex's snapshot_source stays Untrustworthy until a NEW decision record flips it on testnet evidence; no mainnet Paradex session before that record. This ticket writes that record: flip snapshot_source to Trustworthy, or keep it Untrustworthy, on FBC-8xr's evidence.

Scope: one decision record under docs/decisions/ (next free number at merge) and, if it flips, the one-line change to Paradex's declared snapshot_source in crates/venues/fbc-venue-paradex with its test. The record cites FBC-8xr's committed evidence report (written by the FBC-8xr acceptance program) for: (a) whether GET /orders includes every order the orders stream has acknowledged and omits every order the stream has ended, at every comparison point; (b) the observed lag between a stream event and its REST reflection; (c) whether GET /positions matches the position implied by fills, signed size with LONG/SHORT side and CLOSED positions at size 0; (d) whether GET /orders paginates; (e) GET /orders-history?client_id= lag. If any answer is no or unmeasured, the record keeps Untrustworthy and names what would change it. Until this record is accepted, hand-seeding (decision 0067, TestnetRun::owner_assisted()) remains the only way to arm a Paradex market, and only for owner-assisted testnet runs; FBC-vgl (first live Paradex order) must not run before it.

Review path: decision records and caps are owner-reviewed; agents never accept this record.

Decisions: 0005, 0009, 0067.


**Done when:** Done when a new decision record under docs/decisions/ cites FBC-8xr's committed testnet evidence report for each of: GET /orders completeness against the orders stream (acknowledged orders present, ended orders absent), REST lag, GET /positions against the fills, GET /orders pagination and orders-history lag; states Trustworthy or Untrustworthy for Paradex's snapshot_source with the code and its test matching; and the owner has accepted the record.

#### FBC-p0aa Sample mainnet_smoke: the owner's first Paradex MAINNET order — testnet_trade's path on a separate small account, one ~$11 post-only order away from the touch, cancel, Stop, behind an explicit --mainnet confirmation (owner-run) — 2026-10-16 · 28185d7

Discovered from FBC-x69b. Owner decision 2026-10-09, quoted exactly: Q1 "How should the first small mainnet (prod) test happen, once the snapshot-trust decision is accepted?" -> "Library smoke first (Recommended)" (option text: "Right after the trust decision: a mainnet version of testnet_trade on a separate small account. One $11 post-only order away from the touch, then cancel and stop, with an explicit --mainnet confirmation flag. About 1-1.5 weeks from now. Proves the order path on prod before any model exists."). Scope: an owner-run fbc-examples program reusing FBC-x69b's path (login, cancel-on-disconnect arm, resync, Start under caps, one post-only order well away from the touch through an fbc-oms Authorization, ack, cancel, cancel ack, Stop) on Paradex MAINNET. Guards: refuses to run without --mainnet AND --sole-trader; URLs exactly https://api.prod.paradex.trade/v1 and wss://ws.api.prod.paradex.trade/v1; chain id read from GET /system/config and required to be PRIVATE_SN_PARACLEAR_MAINNET (verified 2026-10-09; the testnet's is PRIVATE_SN_PARACLEAR_TESTNET); caps $11 resting per side and $50 inventory unless the owner passes smaller; never a crossing order; every step timed out with a printed reason; login refusals reported with Paradex's status/code/message (FBC-3f8z). Seeding: under owner answer C a MAINNET market is armed only after a trustworthy resync, so this runs only once FBC-2f7w's record has made snapshot_source Trustworthy (no hand seed on mainnet; TestnetRun stays testnet-only). A SEPARATE Paradex mainnet account from the one Java trades (one quoter per account). Credential code is a review path (src/auth*): parks for the owner.

**Done when:** Done when an integration test runs mainnet_smoke against a local stub with synthetic credentials and asserts the step sequence with exactly one place and one cancel, a run without --mainnet or --sole-trader or with any non-mainnet URL or chain id (and a /system/config chain id other than PRIVATE_SN_PARACLEAR_MAINNET) is refused before any order, Start is refused unless the resync was trustworthy, no credential appears in output, and scripts/check.sh passes.

#### FBC-k7t7 fbc-core/fbc-oms: a venue declares whether its resync reads orders and positions at one instant; the shown-cumulative-fill rule refused where it does not — 2026-10-17 · 439923a

Context: Reviewer B's RB80-2 on PR #80 (FBC-38r), P2. The seed's rule 2 (the snapshot holds a fill when it shows the fill's order with a cumulative fill at least the fill's cum_after) is sound only if the snapshot's open orders and positions were read at one instant. PR #80 states that precondition in its decision record and in ResyncSnapshot's docs, but nothing enforces it: OrderCaps::snapshot_source declares trust in the open-order snapshot only. On a venue whose resync is two REST reads (positions, then open orders), a fill landing between them shows in the order's cumulative fill but not in the position, is held InSnapshot and never counted, so the inventory is too small and the $50 cap can be exceeded; the reverse read order counts it twice. Make it declared: a capability (or a field the codec sets on ResyncBegin) saying whether the snapshot is one read; where it is not, rule 2 does not place a fill (only arrival and time do, or the market stays unknown). No default (0009). Needs FBC-38r on main. Error paths are tested too, so the workspace stays above the coverage floor (coverage-floor.txt, 0.2 points of slack). Serves BT-402. Decisions: 0005, 0009, 0013; FBC-38r's decision record (PR #80, once on main).

**Done when:** Done when tests show a venue declaring a snapshot of two reads never has a fill placed in the seed by its order's shown cumulative fill (the fill counted, or the market left unknown), a venue declaring one read keeps today's placement, and the declaration has no default.

#### FBC-hpg6 Verify positions follow fills (owner condition in 0094): after the first fill and at Stop the session compares GET /positions with the registry's position and Stops on a mismatch that a re-read confirms — 2026-10-17 · c1ce20d

Discovered from FBC-2f7w (decision 0094, accepted by the owner 2026-10-16; Reviewer B RB155-1 and RB155-2 on PR #155). A session resyncs only once per connection epoch, so a mainnet session with fills and no reconnect never reads GET /positions after a fill and the owner's condition (positions follow fills) is never checked. Add, to the sessions that can fill (testnet_quote and its mainnet mode FBC-tyft; testnet_trade/mainnet_smoke where a fill is seen): after the first fill and at Stop, read GET /positions and compare it with the registry's position for the market. On a mismatch, re-read once after --step-timeout and log both readings with their times (so GET /positions lag is told apart from a real disagreement, RB155-2); if they still disagree, Stop (kill switch + cancel all) and place nothing after it. Owner gating (0094): this ticket and FBC-k7t7 land before FBC-tyft's continuous loop runs. Tests against the conformance stub only, synthetic credentials, deterministic and event-driven; never a live venue.

**Done when:** Done when, against the conformance stub with synthetic credentials, a session reads GET /positions after its first fill and at Stop and compares it with the registry's position; a mismatch that clears on the one re-read after --step-timeout logs both readings and continues; a mismatch that persists logs both readings and Stops (kill switch on, every order of ours cancelled, nothing placed after); and an agreeing reading continues, each proven by a test.

#### FBC-tyft testnet_quote gains a supervised MAINNET mode: the fixed-offset loop on a separate small account, $11 per side / $50 inventory, offset at least 20 bps, cancel-and-replace only, behind --mainnet (owner-run, short sessions) — 2026-10-18 · 1686f1b

Discovered from FBC-elg7. Owner decision 2026-10-09, quoted exactly: Q2 "After that first mainnet order, may the fixed-offset testnet_quote loop also run on mainnet for short, supervised sessions?" -> "Yes, small and supervised (Recommended)" (option text: "Same caps as testnet ($11 per side, $50 inventory), a wide offset (e.g. 20+ bps from the touch), cancel-and-replace only, you run it and watch it. Loss is bounded to a few dollars of fees and adverse fills. It tests requoting, reconnects and Stop on prod before your model."). Scope: the FBC-elg7 loop's mainnet mode, only after the owner's first mainnet order (FBC-p0aa) has run: --mainnet plus --sole-trader required; mainnet URLs and the /system/config chain id PRIVATE_SN_PARACLEAR_MAINNET; caps $11 resting per side and $50 inventory (smaller allowed, larger refused); offset from the touch at least 20 bps; a session duration limit; cancel-and-replace only (no amend before FBC-8xr evidence); Stop (kill switch + cancel all) on Ctrl-C, the duration, any resync mismatch, any Unknown order and any refused login; trustworthy resync required to Start (no hand seed on mainnet). Plumbing test, not a strategy.

**Done when:** Done when a stub test runs the loop in mainnet mode for several requote cycles and asserts the mainnet guards (refused without --mainnet/--sole-trader, offset under 20 bps refused, caps above $11/$50 refused, untrustworthy resync refuses Start), cancel-then-place replaces only, and Stop on each listed condition.

#### FBC-cor fbc-oms: every raw order event reaches the audit sink in receive order before it is applied (I8, library form) — 2026-10-18 · 72fc66c

Intent: 0005's I8 in its library form. Writing the sink to the consumer's order_audit table, its schema and the legacy status names stay with the consumer (0001).

Scope in crates/fbc-oms: an audit sink the consumer supplies; every raw order event goes to it in receive order before it is applied, including those then ignored as late, stale or for a superseded venue id, and the Applied verdict follows.
Error paths are tested too, so the workspace stays above the coverage floor (coverage-floor.txt, 0.2 points of slack).

Serves BT-402. Decisions: 0001, 0005.

**Done when:** Done when a test in crates/fbc-oms/tests/ feeds order events including duplicates, stale and late ones and shows every raw order event reaching the audit sink in receive order before it is applied, ignored ones included, each followed by its Applied verdict.

#### FBC-fji fbc-sim: SimVenue cancels on disconnect where the stood-in venue does — 2026-10-18 · 9a24e23

Codex r4184778435 on PR #58 (FBC-uoo): the simulated order-entry stream has no disconnect, SimCodec's on_open does nothing and SimEngine takes no connection-close input, so a session over SimVenue standing in for a venue whose ExecCaps.order.cancel_on_disconnect is PerConnection or DeadMan would keep orders live, and fillable, after a disconnect the venue would cancel them on. FBC-uoo's codec refuses placements (NotSent(Unsupported)) for such a venue; cancels still go. Scope in crates/fbc-sim: the engine learns of the simulated stream's open and close (and, for DeadMan, of the timer's arming and refreshes, by the envelopes' stamps since it reads no clock), cancels with CancelReason::Venue every order the venue would cancel (those placed on the closed connection for PerConnection; everything once the timer lapses for DeadMan), honours rearm_on_reconnect, and then the codec stops refusing those venues. Note: the owner's rule that a venue without cancel-on-disconnect may not place orders (FBC-w19) means FBC-6mf's host can only place through SimVenue once this lands, unless its toy configuration declares CancelOnDisconnect::None.

**Done when:** Done when unit tests in crates/fbc-sim/tests/sim_venue.rs show, for a PerConnection venue, an order placed through the codec (not refused) answered Canceled by the venue when the simulated stream closes, and not filled by a later trade; for a rearm_on_reconnect venue, no protection after reconnect until it is requested again; and for a DeadMan venue, every order cancelled once the timer lapses unrefreshed and none while it is refreshed in time.

#### FBC-6to fbc-conformance: fault scripts for NEW or PENDING_CANCEL after CANCELED, an equal-key partial and cancel, and fills before order events, with the order lattice holding — 2026-10-18 · 3153eaa

Intent: the fault scripts behind the Java stack's order-map bugs (design §6 and Appendix A), played against the runtime's order-entry session, the conformance toy and fbc-oms, so 0005's I1-I3 are proven under the scripts as BT-402 asks.

Scope in crates/fbc-conformance: scripts as typed Step values (0025) using FBC-xg7's replies: a NEW and a PENDING_CANCEL event pushed after CANCELED for the same order; a partial fill and a cancel with equal ordering keys, in both orders; a fill pushed before the order's acknowledgement and first update. Tests in crates/fbc-conformance/tests/ with fbc-oms applying the events (fbc-oms a dev-dependency).
Error paths are tested too, so the workspace stays above the coverage floor (coverage-floor.txt, 0.2 points of slack).

Serves BT-502, and BT-402's lattice criterion. Decisions: 0005, 0025.

**Done when:** Done when tests in crates/fbc-conformance/tests/ play each script against fbc-runtime's order-entry session with the conformance toy and fbc-oms and show the order staying terminal after a late NEW or PENDING_CANCEL, the equal-key partial and cancel ending in the same state and filled quantity in both arrival orders, and a fill that arrives before its order's events moving inventory once with filled quantity max(cum_venue, cum_fills).

#### FBC-s6fp fbc-oms: DesiredBook levels carry an opaque consumer tag and each side a removal reason, copied onto planned commands and audit verdicts — 2026-10-18 · 90beaf4

Intent: a consumer's order_audit needs, for every order, the origin the consumer gave it and, for every cancel the planner builds, the reason the level went away. Race-fill analysis splits by that reason, and without tags every planner cancel reads as level_removed.

Scope in crates/fbc-oms:
- Each DesiredBook level carries an opaque consumer tag (u16); each side carries an optional removal reason (u16).
- The planner copies a level's tag onto every command it builds for that level, and a side's reason onto the cancels it builds for removed levels.
- The audit sink's verdicts carry both.
- The values are opaque: fbc-oms gives them no meaning (0001).
Error paths are tested too, so the workspace stays above the coverage floor (coverage-floor.txt, 0.2 points of slack).

Serves BT-402. Decisions: 0001, 0005.

**Done when:** Done when tests in crates/fbc-oms/tests/ plan desired books whose levels carry tags and whose sides carry removal reasons and show each place and amend carrying its level's tag, each cancel of a removed level carrying its side's reason, and the audit sink's verdicts carrying both.

#### FBC-zr1 fbc-sim: SimVenue acknowledges in two phases where the stood-in venue does — 2026-10-18 · e9a52b9

Codex r4182678509 on PR #58 (FBC-uoo): SimCodec decoded every acceptance as AckLevel::Final, so a SimVenue standing in for a venue whose OrderCaps.ack is AckModel::TwoPhase { risk_reject_window } never showed the provisional acceptance and the risk-reject window. FBC-uoo now refuses placements for such a venue (NotSent(Unsupported)). No consumer in the workspace handles AckLevel::Provisional yet, so how a provisional acceptance is finalized (a later Outcome, an order event, or the window passing) must be fixed with the runtime's expectations first. Scope in crates/fbc-sim: the engine answers a placement Provisional, then Final once the configured window passes without a reject (or a consumer-configured risk reject inside it), and the codec decodes both and stops refusing two-phase venues.

**Done when:** Done when unit tests in crates/fbc-sim show a placement for a two-phase stood-in venue answered Accepted Provisional after the latency, then finalized after its risk_reject_window, a configured risk reject inside the window ending the order Rejected, and a single-phase venue answered Final as before.

#### FBC-e5p fbc-conformance: the 340-reconnect storm against an order-entry session: nothing resent, protection and resync before any order, every epoch — 2026-10-18 · f96721b

Intent: the reconnect storm that desynced the Java stack's order map (FBC-jcy plays it against market data) played against order entry, where it did the damage: 0005 (Unknown never resent), 0013 rule 1 (a reconnect never re-places an order).

Scope in crates/fbc-conformance: reconnect_storm driving fbc-runtime's order-entry session with the conformance toy and fbc-oms on paused time, with check_pacing; the script's last resync reply states the venue's final open orders and position, which is what fbc-oms is compared with (the stub keeps no venue state).
Error paths are tested too, so the workspace stays above the coverage floor (coverage-floor.txt, 0.2 points of slack).

Serves BT-502, and BT-402's lattice criterion. Decisions: 0005, 0013, 0023, 0025.

**Done when:** Done when a test in crates/fbc-conformance/tests/ plays the reconnect storm on paused time against fbc-runtime's order-entry session with the conformance toy and fbc-oms and shows no place, amend or cancel frame written twice, on every epoch the cancel-on-disconnect arm accepted and the resync's end applied before any place or amend frame, one resync per epoch, connection attempts within the configured pacing, and fbc-oms's open orders and position equal to those the script's last resync reply states.

#### FBC-ct0 fbc-runtime: a shard host that drains ready inputs, writes a Cycle record, and submits one command at a time with a poll of the shard's sockets between signatures — 2026-10-18 · 2c00241

Intent: 0002's shard host. Design §5.1-§5.2, restated: one current-thread loop per group of markets reads, decodes and applies every ready frame, writes a Cycle record and lets the consumer decide for each instrument that changed, then encodes and signs one command at a time, and between two commands does a non-blocking read of the shard's sockets and runs the per-event path, so a protective cancel issued then waits for at most one signature in progress and goes ahead of the remaining planned commands.

Scope in crates/fbc-runtime: the loop over a shard's market-data and order-entry sessions; Cycle records as FBC-ec9 defined them; the consumer's decide pass and between-command polls exposed beside 0023's inline handler; a command the handler submits while handling any session's frame goes out on the order-entry session, attributed to that frame for FBC-2y3's measurement; remaining planned commands re-checked at submit (FBC-afd).
Error paths are tested too, so the workspace stays above the coverage floor (coverage-floor.txt, 0.2 points of slack).

Decides, in a new decision record: how the decide pass and the between-command poll reach the consumer alongside 0023's handler.

Serves BT-402. Decisions: 0002, 0006, 0023.

**Done when:** Done when a test in crates/fbc-runtime/tests/ runs one shard over the toy market-data venue and the conformance toy's order entry, submits a ladder of commands from one decide pass, and shows reference frames arriving meanwhile handed to the handler between two commands' encodes, a cancel the handler then submits written before the ladder's remaining commands, a kill switch the handler turns on between two commands leaving every remaining place and amend unwritten while the cancels go out, and one Cycle record journaled per decide pass.

#### FBC-xx0 fbc-conformance: fault scripts for id-less error frames, an idle close and a journal-consumer stall against the order-entry session — 2026-10-18 · c47ba0c

Intent: three more of design §6's fault scripts, restated: an error frame that names no request; the venue closing an idle order connection with a venue close code (Paradex's 4031 'no active subscriptions', which the Java library met on 2026-09-23); and the journal's consumer stalling until the sink's reserve is spent (0006). This ticket is the one place the order-entry journal stall is proven (FBC-2pr proves the journal's contents).

Scope in crates/fbc-conformance: the scripts as Step values, and a stalled journal writer for the sink, played against fbc-runtime's order-entry session with the conformance toy.
Error paths are tested too, so the workspace stays above the coverage floor (coverage-floor.txt, 0.2 points of slack).

Serves BT-502. Decisions: 0005, 0006, 0021, 0025.

**Done when:** Done when tests in crates/fbc-conformance/tests/ show an id-less error frame reaching the handler as UncorrelatedError and resolving no request, the stub's idle close with a venue code followed by a reconnect, a new authentication, a resync and no order frame written again, and, with the journal's writer stalled until the reserve is spent, a cancel still written to the stub, the dropped records counted and a Degraded marker written once the writer resumes.

#### FBC-aoh5 Mainnet test programs: a cancel of our order we did not send warns once and Stops on a repeat (another program on the account) — 2026-10-18 · 3866150

Live finding 2026-10-18 (owner's first supervised mainnet loop session, testnet_quote --mainnet on BTC-USD-PERP, main 32294a3): 70 orders placed, 70 cancelled, only 4 cancels sent. Paradex reported user cancels (USER_CANCELED / user_cancel_ws / user_cancel_batch_ws, which the codec maps to Canceled(Requested)) on an exact 3.000 s grid: a Java chaiwala instance on the SAME account runs an orphan reconcile every 3 s that cancels every order on its ticker it did not place. It was flat with no orders at the Rust start, so the resync and --sole-trader could not see it; the loop just requoted. Paradex does not say who sent a user cancel, so a cancel from the Paradex UI and one from another program look the same.

OWNER DECISION 2026-10-18, quoted exactly: to 'In the mainnet test programs, what should happen when an order of ours is cancelled by a cancel the program didn't send?' he chose 'Warn once, Stop on repeat (Recommended)' (option text: 'A one-off cancel (e.g. you in the UI) prints a WARN line and the loop requotes. A second one on a later cycle within 30 s Stops the run. That catches a co-tenant like Java's 3 s reconcile within about 6 s, while still allowing a manual cancel.'). Earlier context he asked: 'what if i want to manually cancel/place an order in the account....or is this just for the smoke test?' — scope is the fbc-examples mainnet test programs (testnet_quote --mainnet and mainnet_smoke); chaiwala-rs gets its own policy later. A manual PLACE already Stops (an order not ours).

Scope: an order of ours that ends Canceled(Requested) while no cancel of ours covering it was sent (no explicit cancel, no cancel-all / instrument cancel-all, no Start/Stop/reconnect sweep in flight or answered for it) is a cancel not ours. Cancels of the same cycle count as one event (one reconcile cancels both sides). First event: print a WARN line naming it (another program on the account, or the Paradex UI; one quoter per account; a second within 30 s Stops) and requote. A second event on a LATER cycle within 30 s of the first: in mainnet mode Stop (kill switch + cancel all) and fail the run with 'another program cancelled our orders: one quoter per account'; since another program is on the account, that Stop cancels our orders explicitly and never sends an instrument cancel-all (it would cancel the other program's orders). After 30 s with no repeat the window resets. Testnet mode: WARN only. mainnet_smoke: its one order ended by a cancel not ours prints the WARN line (no repeat is possible). SUMMARY counts the cancels not ours. --help and the README say so.

**Done when:** Done when stub tests (synthetic credentials, deterministic, event-driven) show, in testnet_quote's mainnet mode: a one-off cancel not ours prints WARN and the loop requotes and ends DONE ok; a second one on a later cycle within 30 s Stops the run with explicit cancels of our orders and no instrument cancel-all, nothing placed after, the run failed; a stub that cancels our resting orders every 3 s like Java's reconcile is Stopped within two events; one after the 30 s window only warns again; a cancel of ours whose reply is still in flight, and our own cancel-all or sweep, are never counted; testnet mode only warns; mainnet_smoke warns; SUMMARY counts them; scripts/check.sh passes.

#### FBC-hxkm fbc-journal: consumer records, opaque to the library, written and read in the shard's total order — 2026-10-18 · 72187bb

Intent: a consumer must journal its own inputs (operator commands, calendar and config reloads, config hashes) and annotations (decide-pass summaries) at their ingest positions, so that replaying a live or shadow journal reproduces what the consumer decided. fbc-journal's control records carry only Opened, Closed and Subscribe today, so design §4.8's Control, Decision and Verdict records have no home.

Scope in crates/fbc-journal:
- A Record::Consumer { at, kind: u16, input: bool, bytes } variant, written through the JournalSink in the shard's total order; the writer picks the traffic class.
- JournalReader returns consumer records in place, between the records they were written between.
- fbc-runtime's decoder replay passes over records whose input flag is false (FBC consumer-inputs ticket replays the true ones).
- A journal format version bump; an older reader refuses the new version by name.
- The payload is opaque bytes: nothing about the consumer's strategy enters this repository (0001).
- A new decision record: the consumer record and its version bump.
Error paths are tested too, so the workspace stays above the coverage floor (coverage-floor.txt, 0.2 points of slack).

Out of scope: delivering inputs into a running shard (the consumer-inputs ticket).

Serves BT-202. Decisions: 0001, 0006.

**Done when:** Done when tests in crates/fbc-journal/tests/ write consumer records interleaved with market-data records through the JournalSink and read them back in place with kind, input flag and bytes unchanged, a reader of the previous format version refuses the new one naming the version, and fbc-runtime's decoder replay over that journal passes over every record whose input flag is false.

#### FBC-2zxk fbc-sim: SimVenue stands in for Paradex, placing and filling under Paradex's declared ExecCaps and MatchingCaps instead of refusing — 2026-10-18 · 52d2432

Intent: FBC-uoo's codec refuses every placement (NotSent(Unsupported)) unless the stood-in venue has no speed bump, single-phase acks, VenueSeq ordering, fills without realized P&L or funding, native fills and no fill replay on reconnect. Paradex's fills carry realized P&L and funding (FBC-uiy), so a SimVenue standing in for Paradex refuses everything, and no Paradex backtest or shadow can run.

Scope in crates/fbc-sim: a SimVenue configured from fbc-venue-paradex's declared caps (FBC-olg) places, amends, cancels and fills. FBC-938 supplies the realized values. Once FBC-olg declares Paradex's ack model, ordering key and reconnect behaviour, whichever of FBC-zr1 (two-phase ack), FBC-3q6 (fill replay on reconnect), FBC-7y8 (speed bump) and FBC-2g7 (fill caps) those values trip are added here as blockers.
Error paths are tested too, so the workspace stays above the coverage floor (coverage-floor.txt, 0.2 points of slack).

Serves BT-501. Decisions: 0014.

**Done when:** Done when a test in crates/venues/fbc-venue-paradex/tests/ (fbc-sim depends on no venue crate, not even as a dev-dependency: AGENTS.md dependency direction; Codex r4309936545 on PR #168) builds a SimVenue from fbc-venue-paradex's declared ExecCaps and MatchingCaps and shows a post-only place acknowledged and filled through the queue model with realized P&L reported as Paradex reports it, an amend and a cancel answered, and no command refused as Unsupported.

#### FBC-hvqi fbc-runtime: consumer inputs and consumer timer firings enter a shard stamped, journaled and replayed at their ingest position — 2026-10-18 · a38ebe0

Intent: design §5.4's UI-to-shard queue. A consumer's HTTP or auxiliary threads hand operator commands (Start, Stop, Flatten, Wind-down, kill switch) and reloads to the shard, and the consumer's own timers (a model's wake request, a resume hold) fire inside the shard. Both must sit in the journal at the ingest position where the handler saw them, or a replay of a live or shadow journal cannot place them identically and cannot reproduce the consumer's decisions.

Scope in crates/fbc-runtime:
- A bounded ring from other threads into the shard; a full ring refuses the push and counts it, never blocks the shard.
- Each input is stamped with the next ingest sequence and journaled as an input consumer record (the consumer-records ticket) before the handler sees it.
- A handler can arm a consumer timer; its firing is an input like any other: stamped, journaled, then handed to the handler.
- Decoder replay hands journaled inputs and timer firings back to the handler at the same ingest positions and arms no live timer.
Error paths are tested too, so the workspace stays above the coverage floor (coverage-floor.txt, 0.2 points of slack).

Out of scope: closed-loop replay through SimVenue (the closed-loop replay host ticket).

Serves BT-202. Decisions: 0006, 0023.

**Done when:** Done when a test in crates/fbc-runtime/tests/ pushes consumer inputs from another thread and arms a handler timer during a toy market-data session, shows each input and timer firing reaching the handler stamped with the next ingest sequence and journaled before the handler sees it, shows decoder replay of that journal handing the same inputs and firings to the handler at the same ingest positions without arming a live timer, and shows a push into a full ring refused and counted.

#### FBC-fwf Binance USD-M discover and parse_fbc_common_symbol — 2026-10-18 · b06bf4e

Intent: FBC-ahf declares VenueFactory::discover and parse_fbc_common_symbol and implements them for fbc-core's toy only; the Binance USD-M factory answers SymbolError::NoRule and VenueError::NoDiscovery for now. The consumer's LegacyKeyReader reads Java-era reference.exchanges values naming Binance (FBC's rule: X/USDT is XUSDT), and the core resolves Binance drafts through the alias table like any venue's.

Scope in crates/venues/fbc-venue-binance-usdm: discover's plan as effects (GET /fapi/v1/exchangeInfo under the configured REST base URL, weighted as Binance documents) with a parser building InstrumentSpecDrafts through DecodeScope from each PERPETUAL symbol's filters (PRICE_FILTER tickSize, LOT_SIZE stepSize/minQty, MIN_NOTIONAL), refusing a symbol missing a required filter with PlanError::Missing; parse_fbc_common_symbol mapping X/USDT to the XUSDT perpetual's AssetKey and refusing other forms. A hand-built exchangeInfo response in fixtures/binance-usdm/rest/ citing the page. Error paths tested so the workspace stays above the coverage floor.

Serves BT-102. Decisions: 0003, 0004, 0016.

**Done when:** Done when tests in crates/venues/fbc-venue-binance-usdm/tests/ decode a hand-built /fapi/v1/exchangeInfo response in Binance's documented shape into drafts through DecodeScope, refuse one missing a required filter, and map BTC/USDT to the BTCUSDT perpetual's asset key while refusing a ticker of another form.

#### FBC-6mf fbc-runtime hosts SimVenue in place of a real venue's order entry: three brackets on one shard, each on its own simulated account — 2026-10-19 · 16f5d37

Intent: BT-501's first criterion (design §10.1, restated: in shadow, market data is live and execution is SimVenue, one per bracket, all three running the same shard code as live).

Scope:
- fbc-runtime gains a generic in-process order-entry stream (a trait the runtime defines, opening no socket) whose peer receives the frames the codec writes and the shard's market-data envelopes in ingest order, and answers with frames; its inbound frames are stamped like any other (0006's journaling applies when a journal is set).
- fbc-sim implements it for FBC-nv2's engine, so fbc-sim takes fbc-runtime as a normal dependency and fbc-runtime never depends on fbc-sim (no cycle; scripts/check-deps.sh unchanged).
- Each bracket is its own order-entry session (FBC-oaz: one ExecCodec per session) under its own simulated account name, with its own fbc-oms whose leases (FBC-hwe) are taken on that name, so three brackets arm on one market without contending; commands still need fbc-oms's authorization (FBC-ob2).
- The engine's books are invalidated with the runtime's after a dropped market-data connection (FBC-crh), so nothing fills against a stale book.
Tests in crates/fbc-sim/tests/ with fbc-oms and the runtime's toy market-data venue (by path) as dev-dependencies, on the shard host (FBC-ct0).
Error paths are tested too, so the workspace stays above the coverage floor (coverage-floor.txt, 0.2 points of slack).

Serves BT-501. Decisions: 0002, 0006, 0013, 0023.

**Done when:** Done when a test in crates/fbc-sim/tests/ runs fbc-runtime's shard host over the toy market-data venue with three SimVenue order-entry sessions (Pessimistic, Middle, Optimistic), each armed through its own fbc-oms under a distinct simulated account, submits the same authorized order to each, and shows their answers and fills reaching the handler stamped in the shard's ingest order with no order-entry socket opened, the Optimistic bracket filling no later than the Middle and the Middle no later than the Pessimistic.

#### FBC-hwn fbc-runtime: bounded outbound queues per order-entry connection and a bounded RPC table, each with a Safety reserve — 2026-10-19 · cf457b1

Intent: design §5.4's bounds, restated: the outbound queue per connection holds a number of Normal frames plus a reserve for Safety frames, overflow reported NotSent(Backpressure) and journaled as a write result while the OMS keeps the intent; the RPC table caps requests in flight per venue with a Safety reserve, overflow NotSent(RateBudget). The bounds are the consumer's configuration (0009), not the design's example numbers.

Scope in crates/fbc-runtime.
Error paths are tested too, so the workspace stays above the coverage floor (coverage-floor.txt, 0.2 points of slack).

Serves BT-402. Decisions: 0006, 0009, 0014.

**Done when:** Done when tests in crates/fbc-runtime/tests/ with the conformance toy and a stalled peer show a Normal command past the connection's configured Normal bound returned NotSent(Backpressure) with nothing written while a cancel uses the Safety reserve, and a Normal request past the configured in-flight bound returned NotSent(RateBudget) while a cancel still goes out.

#### FBC-w9g Replaying recorded market data and a fixed command script through SimVenue gives the same fills on every run — 2026-10-19 · 3712a4f

Intent: BT-501's fourth criterion and design §10.1's Counterfactual and Parity modes (journal market data, SimVenue execution): a recorded day replays through SimVenue to the same fills every time, through the code that ran live. Exact replay of a live order-entry journal is FBC-tgt's and FBC-qwq's, not this.

Scope: fbc-runtime's decoder replay (FBC-3kz) drives the market-data session from a journal of market data only; a fixed command script (commands at given journal positions, authorized through fbc-oms) drives a fresh SimVenue per bracket on FBC-6mf's hosting; SimVenue's latency is measured in the recorded stamps, and nothing reads a clock. Test in crates/fbc-sim/tests/.
Error paths are tested too, so the workspace stays above the coverage floor (coverage-floor.txt, 0.2 points of slack).

Serves BT-501. Decisions: 0006, 0014.

**Done when:** Done when a test in crates/fbc-sim/tests/ journals a toy-venue market-data session, replays it twice through fbc-runtime's decoder replay with the same command script driving a fresh SimVenue per bracket, and shows both replays producing identical outcomes, order updates and fills, in the same order with the same quantities, prices and stamps.

#### FBC-qfm fbc-runtime: kernel receive to write done recorded for every protective cancel on the order-entry session — 2026-10-19 · 79c889b

Intent: BT-402's measurement. Design §5.3, restated: a protective cancel's tick-to-wire is its write-done time minus the kernel receive time of the frame that triggered it, measured on Linux only; FBC-2y3 attributes a write to the frame being handled, and the shard host (FBC-ct0) carries that attribution across sessions. The 0.75 ms target is the consumer's deployment trigger, not a test here.

Scope in crates/fbc-runtime: a cancel submitted while the handler handles a market-data frame is recorded against that frame's kernel_rx and exposed to the consumer; nothing is recorded where kernel_rx is None.
Error paths are tested too, so the workspace stays above the coverage floor (coverage-floor.txt, 0.2 points of slack).

Serves BT-402. Decisions: 0002.

**Done when:** Done when a test in crates/fbc-runtime/tests/, run on Linux as CI does, shows a cancel that the handler submits while handling a market-data frame recorded with a non-negative kernel-receive-to-write-done time on the order-entry session, and none recorded where kernel_rx is None.

#### FBC-vgm Clustered-move benchmark: protective-cancel kernel-receive-to-write-done p99 through the shard host and the Paradex codec, on Linux — 2026-10-19 · 42a32db

Intent: BT-402's last criterion: a Linux benchmark on a clustered-move fixture reports the protective cancel's p99 (design §5.3, restated: a clustered reference move arriving during a ladder rebuild; the cancel waits for at most one Paradex signature, about 0.3-0.45 ms). The 0.75 ms target on the shared box is the consumer's deployment trigger, not this done line.

Scope: a benchmark test in crates/venues/fbc-venue-paradex/tests/ (the Paradex signer is the cost being measured), with fbc-runtime's shard host (FBC-ct0) and FBC-qfm's measurement; it runs in CI on Linux and reports its numbers; on other systems it reports that kernel_rx is absent.

Serves BT-402. Decisions: 0002.

**Done when:** Done when a benchmark test in crates/venues/fbc-venue-paradex/tests/, run on Linux in CI, plays a clustered-move fixture (a burst of reference frames arriving while a ladder of Paradex places is being signed) through fbc-runtime's shard host and the Paradex codec, shows every protective cancel written after at most one signature in progress, and reports the p99 of their kernel-receive-to-write-done times.

#### FBC-01w fbc-oms: the planner's generic flag-conflict fallbacks and emulated reduce-only, from OrderCaps alone — 2026-10-18 · 184ec3f

Intent: design §4.10 steps 3 and 4, restated: an RPI order on a venue without RPI goes Public; flag conflicts fall back generically, never by venue name: (Ioc, ReduceOnly) becomes a capped reduce-only limit and an immediate cancel; (PostOnly, ReduceOnly) follows the quote's reduce fallback, EmulateReduceOnly (post-only without the flag, sized at most |pos| less same-direction resting reducers, re-checked on every fill and cancelled on overshoot) or KeepReduceOnly (post-only dropped, maker-safe price); (Rpi, ReduceOnly) becomes Public with reduce-only; each downgrade is reported.

Scope in crates/fbc-oms.
Error paths are tested too, so the workspace stays above the coverage floor (coverage-floor.txt, 0.2 points of slack).

Serves BT-402. Decisions: 0003, 0005, 0013.

**Done when:** Done when tests in crates/fbc-oms/tests/ show each flag-conflict fallback of design §4.10 chosen from OrderCaps alone with its downgrade reported, and an emulated reduce-only order resized on every fill and cancelled on overshoot.

#### FBC-066n Closed-loop replay host: replayed market data drives a consumer whose own commands SimVenue answers in virtual time, identical on every run — 2026-10-19 · a61ad32

Intent: FBC-w9g proves replay determinism for a fixed command script. A consumer's backtest is closed-loop: its decisions depend on the fills it got, so its commands cannot be scripted in advance. chaiwala-rs makes this backtest its source of truth for P&L, so it must run the same shard host, fbc-oms and SimVenue code as live and shadow.

Scope (fbc-runtime and fbc-sim):
- Decoder-replayed market data drives the shard host's handler and its decide passes, writing Cycle records as live does.
- One SimVenue order-entry session per bracket (FBC-6mf's hosting); the consumer's commands are answered at the replayed time plus the configured latency.
- Journaled consumer inputs and timer firings (the consumer-inputs ticket) are handed back at their positions.
- Time is virtual only: the replay runs as fast as the CPU and reads no clock.
Error paths are tested too, so the workspace stays above the coverage floor (coverage-floor.txt, 0.2 points of slack).

Serves BT-501. Decisions: 0006, 0014.

**Done when:** Done when a test in crates/fbc-sim/tests/ replays a toy-venue market-data journal through the shard host with a handler whose commands depend on the fills it has received, answered by a SimVenue per bracket at the replayed time plus the configured latency, and shows two runs producing identical commands, outcomes, fills and Cycle records with no clock read.

#### FBC-840 fbc-oms: orphans from a trustworthy resync: the adding side suppressed and the orphans cancelled by explicit reference, escalating only under the exclusive lease (I7) — 2026-10-18 · c35d47d

Intent: 0005's I7, the library side of the fix for the Hibachi orphan blowout, with 0012's cancel-all guard. Orphan cancels go out in every market state, a disarmed one after a restart included: 0012 starts every market disarmed in Cancel-only and lets cancels through in every state, and 0013 rule 2 exempts cancels.

Scope in crates/fbc-oms: a resync diff (FBC-38r) that finds own-namespace orders the OMS does not know suppresses the adding side for a cooldown the consumer configures and cancels them by explicit reference in a cancel-many; above the consumer's level limit it escalates to an instrument cancel-all only under the exclusive market lease with no foreign-namespace order in the snapshot, and otherwise repeats explicit cancels on each resync; with an untrustworthy snapshot source it acts only on orders a live account event also shows.
Error paths are tested too, so the workspace stays above the coverage floor (coverage-floor.txt, 0.2 points of slack).

Serves BT-402. Decisions: 0005, 0009, 0012, 0013.

**Done when:** Done when tests in crates/fbc-oms/tests/ feed resync snapshots holding unknown own-namespace orders and show the adding side suppressed for the configured cooldown, the orphans cancelled by explicit reference in one cancel-many, also on a disarmed market, an instrument cancel-all built only above the level limit under the exclusive market lease with no foreign-namespace order in the snapshot, explicit cancels repeated on each resync otherwise, no foreign-namespace order ever cancelled, and, with an untrustworthy snapshot source, action only on orders a live account event also shows.

#### FBC-i8lp fbc-core/fbc-oms: a venue declares how long its open-order snapshot may lag its order stream, and the Unknown ladder's settle below it is refused — 2026-10-19 · d121e5d

Discovered from FBC-2f7w (Codex r4295929734 on PR #155, P1 there, filed as a safety follow-up under TRIAGE). FBC-2f7w's decision record makes Paradex's snapshot Trustworthy, so an order on the Unknown ladder absent from absent_snapshots resyncs, each at least settle after it was sent, is Lost; FBC-8xr's runs saw GET /orders lag the stream by up to 3480 ms. The samples use testnet::SNAPSHOT_SETTLE (10 s), but LadderConfig::new accepts any settle (even zero), so another consumer could mark a resting order Lost and free its resting exposure. Make it declared: the venue's order caps state the snapshot's lag bound (Paradex: from FBC-8xr's evidence, with margin), and Registry::resync / on_resync refuse (or the ladder ignores absences under) a settle below it. No default (0009). Decisions: 0005, 0009; FBC-2f7w's record once on main.

**Done when:** Done when a venue declares its snapshot lag bound in its order caps (Paradex's cited to fixtures/paradex/exec-recorded/), and a test shows an order absent from a snapshot taken within that bound of its send is never Lost whatever LadderConfig's settle, while one past it still is.

#### FBC-7aw4 fbc-oms: a price-capped reduce-only crossing intent in the DesiredBook, never crossing zero, every cap and state still applied — 2026-10-19 · 3236806

Intent: a consumer's disaster stop, news FLATTEN window and operator Flatten need a crossing exit with a price cap. FBC-0j3's DesiredBook is a per-(side, level) diff of resting orders with no crossing intent, so today such an exit could only be built around the planner, which 0005 forbids.

Scope in crates/fbc-oms:
- A per-side crossing intent in the DesiredBook: reduce-only, a price cap, a size.
- Planned as one reduce-only order at the cap with an immediate-or-cancel time in force where OrderCaps declare it, otherwise a limit at the cap followed by a cancel (FBC-01w's emulation).
- The size is clipped so the position never crosses zero; nothing is planned while flat.
- Refused in Killed and Cancel-only; admitted in Exit and Quoting through FBC-7gl's admission; both caps still applied.
Error paths are tested too, so the workspace stays above the coverage floor (coverage-floor.txt, 0.2 points of slack).

Serves BT-402. Decisions: 0005, 0012, 0013.

**Done when:** Done when tests in crates/fbc-oms/tests/ show a desired crossing intent planned as one reduce-only order priced at its cap, immediate-or-cancel where OrderCaps declare it and otherwise a limit at the cap followed by a cancel, its size clipped so the position never crosses zero, nothing planned while flat, refused in Killed and Cancel-only, and every cap still applied.

#### FBC-hht fbc-oms: the planner batches within the declared sizes, gates on venue modes and instrument status, and stops Normal traffic at the safety floor — 2026-10-19 · ba93639

Intent: design §4.10 steps 6 and 7, restated: batch per batch_place and batch_cancel (cancel-many items only with the batch's declared references, FBC-6c9); nothing is built for a market whose venue mode or instrument status refuses it; Normal traffic stops at each rate scope's safety floor, while cancels and reducing orders may use it.

Scope in crates/fbc-oms: a RateView the consumer fills from the runtime's limiter counts; the safety floor's size is the consumer's (0009).
Error paths are tested too, so the workspace stays above the coverage floor (coverage-floor.txt, 0.2 points of slack).

Serves BT-402. Decisions: 0003, 0005, 0009, 0018.

**Done when:** Done when tests in crates/fbc-oms/tests/ show the planner batching within the declared batch sizes and references, emitting no order for a market in a venue mode or instrument status that refuses it, and withholding Normal commands once a rate scope reaches its safety floor while cancels and reducing orders still go out.

