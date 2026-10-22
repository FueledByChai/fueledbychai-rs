# Product backlog

The stories fueledbychai-rs builds first, in the venue order of decision 0117 (which
supersedes 0016). Stories are intent with acceptance criteria; the tickets that build them live
in Beads and name the story they serve (`story:BT-nnn`). The decisions they rest on are in
`docs/decisions/`; "design §n" is a section of the approved architecture design (0001). The
user throughout is a trading program built on this library, written and run by the owner.

Build order follows design D9 (recorder, then shadow, then the first live order): Epic E
(simulation and conformance) comes before Paradex order entry (BT-402), and BT-201's
fault-script criterion runs on the stub server BT-502 delivers.

## Epic A: Core contract types and traits

### BT-101 — Exact, sealed value types for time, prices, ids and fees

**Status:** Proposed  
**User story:** As the author of a trading program, I want time, prices, quantities, ids and
fees as distinct types that cannot be mixed up or built wrongly, so that the bugs that cost
money in the Java stack (swapped ids, flipped fee signs, mixed clocks, rounded marks) fail to
compile instead of reaching a venue.

**Acceptance criteria:**

- `MonoNs`, `WallNs`, `ExchNs` and `KernelRxNs` are distinct and cannot be subtracted across
  kinds; `Ticks` index an instrument's finest `PriceGrid` (fixed, significant-figure and banded
  grids), `Lots`/`SignedLots` count size steps, and off-grid values are `PxExact`, never
  rounded (decision 0004, design §4.1).
- `ClientOrderId` is minted only by `CidMint` under a held namespace lease, its sequence floor
  survives a restart, and the canonical codec round-trips every id it mints and reports ids
  minted elsewhere as `Unparseable` (0004, 0013, design §4.3).
- `Fee` is positive when we paid and negative for a rebate, with no public constructor, `abs`,
  negation or `f64` conversion; fee rates live in a `FeeBook` per account (0004, design §4.2).
- `trybuild` compile-fail tests prove each seal: a `VenueOrderId`, `FillId` or `Fee` cannot be
  built outside `DecodeScope`, and a `ClientOrderId` cannot be built outside `CidMint`.

### BT-102 — One venue contract: instruments, capabilities, events, commands and codecs

**Status:** Proposed  
**User story:** As the author of a venue adapter, I want one contract that says what an
instrument is, what a venue can do, and which events and commands cross the boundary, so that
adding a venue is writing codecs and a capability table, never touching the runtime or the
order manager.

**Acceptance criteria:**

- `InstrumentSpec` and `VenueCaps` with all its parts have no `Default` and are not
  `#[non_exhaustive]`; a test fixture declares every field, and adding a field breaks the
  build of every adapter (0003, design §4.4, §4.5).
- `MdEvent`, `ExecEvent`, `VenueCommand` and the outcome types carry every field a venue
  needs; `MdCodec`, `ExecCodec` and `VenueFactory` take bytes, HTTP results and timer firings
  and return effects, with no socket, thread or clock in the trait (0002, design §4.6, §4.7).
- A toy codec in the tests implements the traits against recorded-shape bytes and passes
  through `DecodeScope` and `EncodeCtx` only.
- Account events arrive as normalized `ExecEvent`s like order events do: positions, balances
  and funding payments; the resync sequence (begin with its watermark, each open order, each
  position, end); query results and fee rates; and every fill carries the venue's realized
  P&L and realized funding when the venue reports them (design §4.6).

## Epic B: Runtime with SOCKS5

### BT-201 — A generic runtime that connects through SOCKS5 and survives reconnect storms

**Status:** Proposed  
**User story:** As the owner developing on a laptop behind a SOCKS proxy and deploying on a
box that shares an IP with other trading processes, I want one runtime that owns every
connection, reconnect and rate limit for every venue, so that reconnect and ordering bugs are
fixed once and no venue can reintroduce them.

**Acceptance criteria:**

- WebSocket and HTTP traffic both go through a SOCKS5 proxy when the caller configures one,
  and directly when it does not; a test proves both paths against a local stub (0002).
- Reconnects open a new connection epoch, reconcile subscriptions to the desired set without
  duplicates, and drop events from a stale epoch; the conformance stub server's reconnect-storm,
  duplicate-ack and silence scripts pass (design §4.7, §6).
- Rate limits are scoped as the venue's capabilities declare, normal traffic stops at each
  scope's safety floor, and every rejection is counted per scope (design §4.10, §5.3).
- Kernel receive timestamps are recorded on Linux and are `None` on macOS; protective-cancel
  tick-to-wire is measured from them (0002).

### BT-202 — A journal of everything that crosses the shard boundary

**Status:** Proposed  
**User story:** As the owner, I want every inbound frame, outbound byte, HTTP result and timer
recorded in daily compressed segments, so that a recorded day replays through the same code
that ran live.

**Acceptance criteria:**

- The writer records raw frames with their stamps, outbound bytes, HTTP results with headers,
  timers, nonces, encode wall times and cycle boundaries; the reader returns them in order
  (0006, design §4.8).
- Safety records (cancels, reducing orders, acks, fills) use a reserve: a test fills the
  journal to its soft limit and then its reserve and shows a cancel is still sent, the drops
  are counted, and a `Degraded` marker is written when space returns (0006, design §5.4).
- Authorization headers, bearer tokens, JWTs, cookies and API keys appear only as keyed
  hashes; order signatures are kept (0006, 0009).

## Epic C: Binance USD-M market data

### BT-301 — A Binance USD-M reference book and touch

**Status:** Proposed  
**User story:** As a trading program that prices off Binance futures, I want its best bid and
offer and a diff-depth book anchored on a REST snapshot, so that I have a reference price I can
trust and can tell when it is stale or broken.

**Acceptance criteria:**

- `fbc-venue-binance-usdm` decodes `bookTicker`, partial depth and diff depth, and anchors the
  diff-depth book on a REST snapshot; it never sends an order (0016, design §6).
- A gap in the diff-depth update ids is detected and resynced from a new snapshot; recorded
  fixtures with a gap, a duplicate and out-of-order updates prove it.
- Decoder replay of a recorded journal rebuilds the book byte-identically (0006).

## Epic D: Paradex

### BT-401 — Paradex market data from the SBE feed

**Status:** Proposed  
**User story:** As a trading program quoting on Paradex, I want its best bid and offer, book
deltas and trades decoded from the binary SBE feed into a book I can check against the venue,
so that every quote rests on a correct book.

**Acceptance criteria:**

- The SBE decoder is gated on the schema's block length and handles fixtures from both schema
  versions; `bbo`, book deltas and `trades` decode to normalized events (0016, design §6, §15).
- Every `seq_no` discontinuity is detected and resynced (design §14 M0).
- REST `/orderbook` snapshots match the delta-built book at the same `seq_no` for the top 15
  levels exactly, and the unthrottled `bbo` channel agrees with the delta-built touch in at
  least 99.9% of samples over a recorded soak (design §14 M0).
- Decoder replay of the journal rebuilds the books byte-identically (0006).

### BT-402 — Paradex signing and order entry through the order lattice

**Status:** Proposed  
**User story:** As the owner, I want to place, amend and cancel Paradex orders through a
hand-written signer and an order manager that cannot lose track of an order, so that the first
live Rust market trades with the Java stack's order bugs designed out.

Depends on BT-501 and BT-502: the shadow stage and the conformance suite come before the
first live order (design D9).

**Acceptance criteria:**

- The SNIP-12 revision 0 hash and Stark-curve signature r and s equal the Java library's for
  every vector `fixtures/paradex/signing/ParadexHashOracle.java` emits: buy and sell, limit and
  market orders, a modify (which signs the order id), the auth `Request`, and random decimal
  intents with more than eight decimal places, so the decimal-to-felt ×1e8 truncation is
  compared with Java's own scaling; the recovered benchmark (`fixtures/paradex/signing/signbench`)
  seeds the port (0016, 0009, design §6 `signing_golden`, §15).
- JWT authentication lives in the venue crate's `src/auth.rs` or `src/auth/` (a review path),
  and `exec.rs` only calls it; it runs as effects through the runtime, and no token appears in
  a log, an error or the journal (0009).
- Order WebSocket, batch create, amend, cancel, cancel-many and cancel-on-disconnect each pass
  one Paradex testnet acceptance run, and per-item outcomes map to the order lattice (design
  §12.1, §14 M2).
- The lattice invariants I1–I9 as 0005 states them hold under property tests and the fault
  scripts (reconnect storm, PENDING_CANCEL after CANCELED, fill before order event); I8 is
  tested in its library form (every raw order event reaches the audit sink in receive order
  before apply), and an order in `Unknown` is never resent (0005).
- Order entry starts disarmed after any restart, and no place or amend that would exceed the
  resting cap or the inventory cap is ever built (0013); the kill switch passes the tests that
  0012's Consequences name.
- Arming order entry for a market requires the market lease, plus the account lease when the
  nonce scope is per account; a test shows a second holder of either lease is refused and
  cannot arm (0013, design §13.2).
- `fbc-runtime` records kernel receive to write done for every protective cancel (design §5.3),
  and a Linux benchmark on a clustered-move fixture reports its p99. The 0.75 ms target on the
  shared deployment box is the consumer's deployment trigger, not this story's done line.

## Epic E: Simulation and conformance

### BT-501 — A simulated venue with a queue-position fill model, and journal replay through it

**Status:** Proposed  
**User story:** As the owner, I want a simulated venue that answers orders the way a real one
would, with fills from a queue-position model, so that a recorded day can be replayed and live
data shadow-quoted through the same code before any real order is sent.

**Acceptance criteria:**

- `fbc-sim`'s `SimVenue` implements `ExecCodec` and plugs into the runtime in place of a real
  venue, so replay and shadow run the same shard code as live (design §10.1, §10.2).
- The queue model brackets fills as Pessimistic, Middle and Optimistic: a new order queues
  behind the level's size excluding all modelled own orders, a level cancel advances it by
  none, a proportional share, or all of the cancelled size, and a trade fills it once the
  queue ahead is consumed; unit tests prove each bracket on hand-built books (design §10.2).
- An amend resets queue position unless the venue's capabilities say amends keep priority.
- Replaying a recorded journal through `SimVenue` produces the same fills on every run.
- It ships code only: no calibration, parameter or fitted output lives here (0001, 0009).

### BT-502 — An adapter conformance kit and a stub venue server with fault scripts

**Status:** Proposed  
**User story:** As the author of a venue adapter, I want one named suite and a stub server that
replays faults, so that every adapter is held to the contract and the runtime's reconnect and
ordering fixes are proven against the failures that hurt the Java stack.

**Acceptance criteria:**

- `fbc-conformance` provides the named suite of design §6 as a macro an adapter crate invokes
  with its factory and fixture directory: `fee_sign`, `ids_roundtrip`, `restart_cid`,
  `caps_truthful`, `commands_selfcontained`, `amend_ack`, `mixed_batch`, `unknown_on_timeout`,
  `resync_after_reconnect`, `subscriptions_idempotent`, `continuity`, `decoder_deterministic`,
  `encode_deterministic`, `price_grid` and `signing_golden`, among the others design §6 names.
- The stub server plays fault scripts: a reconnect storm (340 reconnects in 8 minutes),
  duplicate subscription acks, NEW or PENDING_CANCEL after CANCELED, equal-key partial and
  cancel, fills before order events, id-less error frames, an idle close, silence after
  subscription, and a journal-consumer stall (design §6).
- The toy venue from BT-102 passes the suite, and a deliberately broken toy (an absent
  capability that still sends bytes) fails `caps_truthful`.

## Epic F: Hibachi

Decision 0016 ordered Hibachi (`fbc-venue-hibachi`) after Paradex order entry had traded live,
which it did on 2026-10-17/18; decision 0117 supersedes 0016 and keeps Hibachi's implementation
going. The owner set this epic's scope on 2026-10-19 (to the coordinator). He asked "while that
is going on, can we start implementing hibachi in FBC?"; to "What should the Hibachi round build
now?" he chose "MD + signer + runtime prep (Recommended)": "Market data, the md_watch sample,
signing parameters, the offline signer, and the runtime's WS auth headers, two-socket session
and nonce source. Order entry waits for the cancel-on-disconnect decision." To "How do we settle
Hibachi's cancel-on-disconnect, which decision 0058 still refuses?" he chose "Probe only": "Skip
asking. Rely on a supervised probe's evidence for the record." To "Which Hibachi account should
the Rust stack use?" he chose "Separate account + key (Recommended)": "A new Hibachi account
only Rust uses, so nonces, the account-wide cancel-all and rate limits never collide with the
Java instances." To "First Hibachi market for Rust?" he answered "ETH" (the other options
offered, and why, are in the owner's private notes).

The coordinator's defaults, which the owner was told on 2026-10-19 and did not object to:
ECDSA secp256k1 signing only at first (Java production's scheme; HMAC later if ever);
Hibachi's MM-partner-only book topic stays out of this public repository until its entitlement
is clear; and, since no public Hibachi testnet is confirmed, the cancel-on-disconnect probe is
an owner-run, supervised, minimum-size post-only order on Hibachi mainnet from the separate
account unless the owner obtains testnet access.

BT-601 to BT-603 open no order path. Decision 0058's refusal stands for Hibachi order entry
until a new record, based on the probe's evidence, supersedes its Hibachi sentence; BT-604
begins with that record. The facts below come from the Java library
(`commons/hibachi-common-api`, `implementations/*/hibachi-*`, `market-data-collector`), the
public SDK (`hibachi-xyz/hibachi_sdk` at 14a76b9, `python/hibachi_xyz`) and Hibachi's API docs,
cited by file, and from the owner's reads of two public Hibachi sources on 2026-10-22, the
`orderbook` feed and the settled funding history, cited only for what they show: beyond the
spacing of the funding settlements, which BT-601 states as the settlement interval, their
measurements stay in his private notes (0009). Fixtures are hand-built from those shapes and
SYNTHETIC: never a recorded production frame, a credential or an account id (0009). No test or
agent sends anything to Hibachi.

### BT-601 — Hibachi market data from the anonymous feed, and md_watch on ETH

**Status:** Proposed  
**User story:** As a trading program that will quote on Hibachi, I want its instruments
discovered with the grid the book actually quotes on, and its public book, touch, trades, mark,
funding and spot decoded into normalized events, so that I can watch a Hibachi market beside
Paradex and Binance before any order path exists.

**Acceptance criteria:**

- `fbc-venue-hibachi` discovers instruments from REST `/market/exchange-info`
  (`futureContracts[]`): the contract `id` is the `native_id`, `tickSize` the price grid, and
  the quote grid the finest `orderbookGranularities` entry at or above `tickSize`, which is
  `tickSize` itself only when Hibachi lists that tick and is coarser otherwise, a fixture
  pinning both cases (a contract with no such entry is refused, or quoted on `tickSize` as Java
  does, as discovery states and a fixture pins); `stepSize`, `minOrderSize` and `minNotional`
  fill `size_step`, `min_size` and `min_notional` from the contract's own values, never a
  placeholder, with a fixture pinning each; `underlyingDecimals` and `settlementDecimals` fill
  the instrument's signing parameters, and an FX contract carries a weekend session calendar,
  both in the `signing` and `sessions` fields `InstrumentSpec` gains under BT-102 with no
  `Default` (Java `HibachiRestApi.java`, `HibachiRestApiTickTest.java`; 0003).
- Discovery maps each contract's `status` into `InstrumentSpec::status` conservatively.
  Hibachi's Market API spec makes it a required `ContractStatus`, one of `LISTED`, `PRE_OPEN`,
  `LIVE`, `PAUSED` and `CLOSED`, and describes none of them, so only `LIVE` is
  `TradingStatus::Trading`, and `LISTED`, `PRE_OPEN`, `PAUSED` and `CLOSED` are each `Halted`,
  never `PostOnly`, `ReduceOnly` or `Delisted` on a guess at a state the venue does not describe
  (Hibachi's FX trading docs say a pair's weekend halt "transitions to PAUSED"; the SDK's
  `types.py` keeps the field a plain string, and Java's `HibachiRestApi.java` reads it without
  acting on it). A contract whose `status` is absent or null refuses the answer as missing, and
  any other value, matched exactly, as malformed, never read as trading, as the Binance USD-M
  and QFEX discoveries refuse an undocumented status; neither `marketOpenTimestamp` nor the
  session calendar ever makes a contract trading. While the answer's exchange-level `status` is
  anything but `NORMAL` (the spec lists `SCHEDULED_MAINTENANCE` and `UNSCHEDULED_MAINTENANCE`),
  every contract is `Halted`. What reads that status is `fbc-oms`'s `ExecutionPlanner`, which
  plans no place or amend on a market whose status, in the `PlanVenue` the consumer passes it,
  is `Halted`, and still plans its cancels (0111); a command the consumer authorizes through
  the registry directly is not checked against status. Discovery's status is a snapshot of one
  answer, so a contract that goes `PAUSED` after it (an FX pair each weekend) or an exchange
  that enters maintenance is not seen until a ticket under this story re-reads
  `/market/exchange-info` on an interval from a required venue configuration key and reports
  each market's status as it changes, mapped as discovery maps it and `Halted` while a read
  fails, for the consumer to pass the planner. Between reads it also reports a market `Halted`,
  without waiting for the next read, from the start of each maintenance window the last read
  published (`maintenanceWindow`, SDK `types.py` `MaintenanceWindow`), from the end of an FX
  contract's session in its calendar, and from a contract's advertised close, never `Trading` on
  a schedule alone, so only a change no schedule announces reaches the consumer up to one
  interval late. A window's `begin` and `end` are each read as a decimal count of Unix seconds
  in a number or a string: the spec types both as strings, while the SDK's `MaintenanceWindow`
  and its test data hold numbers and its README says seconds since the Unix epoch. Hibachi's
  trading docs give each temp market "a predefined expiration time", when its open positions
  settle at the final mark price and "a brief settlement phase occurs before the market is
  removed", and its Market API spec gives every contract an optional `marketCloseTimestamp` and
  `marketOpenTimestamp`, each a string or null with no description and no unit. No source shows
  one that is not null: the SDK's `types.py` `FutureContract` keeps both as optional strings,
  its exchange-info test data has both null on every contract, and Java's `HibachiRestApi.java`
  `parseContract` stores them and never reads them. Each is read as Unix seconds in a decimal
  string, the form that test data gives the sibling `marketCreationTimestamp`. One absent or
  null advertises nothing. A close or an open that is not a decimal count of seconds, or that
  read as seconds lies after 2100-01-01 UTC, as a count of milliseconds would (its halt would
  then never come), refuses the answer as malformed naming the field, and the re-read refuses a
  window bound the same way. Discovery, whose parser reads no clock (0035), closes the
  contract's session calendar from its advertised close on and before its advertised open. The
  re-read halts a market at the earlier of its calendar's close and the close its last read
  advertised, and reports a subscribed market a read no longer lists `Halted`. A codec learns
  the wall time only from `MdCodec::on_timer` (`on_open` and `on_http` carry none; 0002), and a
  read shows the venue at some instant between its send and its answer, so the re-read sends
  each read from a timer and judges each answer at the wall time of a timer it asks for once the
  answer arrives. A market is `Trading` after a read only if it was throughout that span: the
  read lists it `LIVE` and the exchange `NORMAL`, advertises no open after the read's send and
  no close at or before the answer's judged time, and no halt of that market was reported after
  the read's send, on a schedule or by another read. A read sent before the market's open or
  judged at or after its close is `Halted` whatever its `status` says (the docs do not say which
  status a settling temp shows), and a read in flight when a close, a window's begin or a
  session's end halts the market never lifts that halt; only a read sent after it can. That
  ticket lands before any Hibachi order path quotes. Hand-built fixtures pin a contract in each
  of the four non-live states, the exchange in each maintenance state, a contract `status`
  absent, null and undocumented, a window bound as a number, as a string and in milliseconds, a
  `LIVE` contract whose close falls between two reads, a close in milliseconds and one that is
  not a number, a read sent before a close, a window's begin or a session's end and answered
  after its halt, still `Halted`, a read answered after a close it first advertises, `Halted`,
  and a market a later read leaves out (0003).
- Funding's interval comes from the venue's own record, against its docs. Hibachi's trading docs
  say funding is paid every eight hours (00:00, 08:00 and 16:00 UTC) and give the rate's formula
  over that period, but the settled history it publishes on the market-data host
  (`GET /market/data/funding-rates`, not in its published Market API spec), read by the owner on
  2026-10-22, lists consecutive settlements, each with a `fundingRate` and an `indexPrice`, each
  on the UTC hour and its `fundingTimestamp` (epoch seconds) exactly 3600 s after the one
  before, and the Java library derives an hourly interval from the same spacing
  (`market-data-collector` `FundingSources.java`). The crate states the settlement interval as
  one hour, cited to that history, records the docs' eight hours as contradicted by it, and pins
  it with a hand-built fixture in that endpoint's shape, a test deriving the interval from
  consecutive settlements. The history does not show what a rate is for: a rate for its hour, or
  an eight-hour rate settled in hourly slices. The docs' limit of -10% to 10% names no interval,
  and Java reads the rate both ways (`HibachiQuoteEngine.java` `onFundingRateUpdate` divides the
  estimate by an interval that defaults to eight hours, while `FundingSources.java` annualizes
  each settled rate as hourly). `MdEvent::Funding`'s rate is per interval and
  `FundingSpec::Known` carries a cap per interval (`fbc-core` `event.rs`, `instrument.rs`), so
  until the owner's evidence settles what the rate is for (a funding payment on an account he
  holds a position on, set against the position, that hour's settled rate and its index price,
  compared on his machine with only the conclusion recorded), discovery states each perpetual's
  funding as `FundingSpec::Unknown`, and the codec pushes `funding_rate_estimation`'s rate
  exactly as the frame carries it, with `interval: None`, so nothing scales it by an interval.
  Once that evidence is recorded, discovery states `FundingSpec::Known` with the interval and
  the cap per interval it shows.
- Its capabilities declare `exec: None` and `Readiness::Record` with a source on every value:
  JSON encoding, the order book `Continuity::Unsequenced` on a pulse, the funding stream with no
  interval in its frames, and a timestamp precision no finer than the frames carry (Java
  `HibachiQuoteEngine.java`; 0003, 0015). The owner's capture of the public feed, 2026-10-22
  (the ETH/USDT-P `orderbook` on the anonymous `/ws/market`; its measurements are in his private
  notes, never here), showed Updates coming on a steady venue tick a little slower than Hibachi
  support's 250 ms (2026-05-27), sent even when nothing changed, so a silence of a few ticks is
  abnormal (the consumer's `Liveness`, 0033). The pulse declared is support's 250 ms, or
  `Cadence::Unknown` if the owner judges that disagreement decisive, never a value measured from
  the capture.
- The market-data codec subscribes on the anonymous `/ws/market` with the `parameters` key and
  decodes `orderbook` Snapshot and Update frames (absolute sizes, a zero quantity removes a level,
  no sequence numbers, so recovery is a fresh Snapshot only, and a Snapshot with no level never
  makes a book valid, as a criterion below says), `ask_bid_price`, both `trades` shapes,
  `mark_price`, `funding_rate_estimation` and `spot_price`, synthesizing no exchange timestamp
  where a frame has none; `ask_bid_price` becomes
  an `MdEvent::Touch` only when the frame carries each side's price and its size
  (`bidPrice`/`bidSize`, `askPrice`/`askSize`, each optional in Java's `HibachiQuoteEngine.java`
  `onAskBidUpdate`, and the docs' example shows prices only), while a frame with a price but no
  size produces no Touch, since a `None` side reads as an empty one, and is counted, never given a
  synthesized size, a fixture pinning each case (Java `HibachiMarketSubscribeMessage.java`,
  `HibachiQuoteEngine.java`; SDK `api_ws_market.py`, `types.py`).
- Each `orderbook` subscription names its `granularity`, the instrument's quote grid as its
  contract lists it among `orderbookGranularities`, and its `depth`, from a required venue
  configuration key with no default, refused outside 1 to 100, so the venue's undocumented
  default never decides what the book means. Hibachi's WebSocket market docs make both optional,
  take one set per subscription (a subscribe with another set replaces the earlier one) and refer
  to REST `/market/data/orderbook`, whose depth the SDK bounds to 1 to 100 and whose granularity
  to the contract's `orderbookGranularities` (`api.py` `get_orderbook`); Java subscribes with
  neither. No source shows a subscription carrying them by example, so each frame's own
  `granularity` and `depth`, which the docs' example frame carries, confirm it: a frame whose
  `granularity` or `depth` differs from its subscription's, or lacks either, is counted and gaps
  the book as a frame failing the bounds check below does, so the `depth` that check reads is
  the subscription's. An instrument whose quote grid is not among its listed granularities gets
  no `orderbook` subscription and is counted, never subscribed at the venue's default. The
  levels are aggregated at that granularity (SDK `api.py` `get_orderbook`; Java
  `HibachiRestApi.java` `resolveEffectiveTickSize`) while orders rest on the finer `tickSize`:
  where the quote grid is coarser than the tick, a level's price is a bucket's label, not a
  price an order rests at, its size is the bucket's sum, the touch can sit up to one granularity
  from the venue's own, and which way a bucket rounds is undocumented. The crate docs state
  this, and the `orderbook` channel's caps declare `max_depth` as the configured depth,
  `windowed` and `QueueModelQuality::BracketOnly`; since no capability says that a book's levels
  are buckets, a ticket under this story adds one, a mandatory field under a decision record
  (0003), before a SimVenue stand-in or a quote reads Hibachi's book. A book is crossed when its
  best bid is above its best ask. A locked book, best bid equal to best ask, is not, since two
  buckets can share a label while no order crosses: it passes the bounds check below and is
  counted apart. The owner's capture shows the bounds rule only at the parameters it was taken
  with, so a frame failing it at the configured ones gaps the book, the safe direction, and is
  counted.
- An Update applies each side's window as Hibachi's WebSocket market docs specify: levels
  outside the min..max of the side's `startPrice` and `endPrice` are removed before its listed
  levels apply (Java `HibachiQuoteEngine.java` `pruneOutsideWindow` prunes to the same min..max,
  and not at all without both bounds, only on its partner feed). In the owner's capture of the
  public feed, 2026-10-22, the venue sent every level that scrolled into or out of a window
  itself, so the prune never changed a book there: a zero quantity naming a level the prune
  already removed, as many of the capture's removals did, is not an error, while a level the
  prune removes that the frame does not itself set to zero is one a lost frame left behind, a
  gap (the bounds check below). In that capture a side with nothing to change came as
  `levels: []` with both bounds present and unchanged, often and on both sides, and no side of
  an Update or a Snapshot came null, absent or missing a bound. Hibachi's docs say a side sent
  as null removes all its levels, but their own Update example sends `"bid": null` beside
  changes to the ask side, and Java's `onOrderBookUpdate` reads a null or absent side as no
  levels, leaving it unchanged; the capture gives that reading no support, since "unchanged" has
  its own encoding. So a side sent as null clears that side, and since it carries no bounds to
  check and has never been seen, the codec counts it, emits that side's clear and then a gap for
  that book's feed, and pushes nothing else of the frame or the book until a resubscribe, as the
  criterion on an empty Snapshot below makes one, brings a Snapshot that makes it valid. A side
  absent from the frame, or one without both bounds, which neither the docs nor the capture
  show, is malformed: it is counted and gaps the book the same way, never read as unchanged or
  left unchecked. An Update for an instrument with no book (the capture showed such an Update
  after an unsubscribe, before the next Snapshot) is ignored. Since `MdEvent::Window` bounds
  both sides at once and no event clears one side (`fbc-core` `event.rs`, `fbc-book`
  `L2Book::set_window`), a side-aware window and a one-side clear are added to the common event
  and the book model, under a decision record, before the codec maps an Update onto them, so a
  one-sided Update never prunes or clears the other side. The codec never stands in for them
  with zero-quantity levels: a level that only left a side's window is unknown, not emptied, and
  `fbc-sim` reads a level set to zero as public cancels ahead of a simulated order.
- The feed has no sequence number, so its bounds are its gap detector. They describe the book
  after the frame: after every Update of the capture, and in each of its Snapshots, each side's
  best level sat at `startPrice` and its `depth`-th level (the frame's `depth`) at `endPrice`,
  and the bid's window runs downward (`startPrice` above `endPrice` in every frame), so every
  window test uses min..max. Once both sides of a frame apply, each side must hold its best
  level at `startPrice`, exactly `depth` levels within the min..max of its bounds, the last of
  them at `endPrice`, and none outside, the book must not be crossed, and the prune must have
  removed no level the frame does not itself set to zero; a frame that fails is counted and gaps
  the book as a null side does. The check runs on a Snapshot too, so a Snapshot that fails it,
  one with a null, absent or unbounded side included, never makes a book valid. A side thinner
  than `depth`, which the capture never showed, fails the check: what `endPrice` names on one is
  unknown, so the codec guesses nothing and counts it apart, and a market that thin on a side
  stays gapped, resubscribed at each interval, until evidence of a thin side settles it; that is
  an assumption, and a fixture pins it. The check detects gaps and proves no continuity:
  replaying the capture offline with each Update that carried a level dropped in turn, it caught
  most drops, usually at the very next frame, but missed some, leaving books that looked valid
  with wrong levels, while the capture as recorded raised none. The codec keeps its own copy of
  each subscribed book's levels to run it, since a venue crate holds no `fbc-book` book
  (`fbc-runtime`'s `MdBooks` does) and `MdHandler` receives a frame's events one at a time
  (0023): it applies the whole frame to that copy and checks it before it emits any event of the
  frame; when the check fails it emits a null side's clear, if the frame has one, then the gap,
  and nothing else of the frame, and otherwise it emits the windows and every removal of both
  sides before any level it sets, so the book `MdBooks` holds is never crossed between two
  events of one frame (applying one side alone crossed the capture's book in some frames,
  whichever side went first, and in none once both sides applied). A quoting decision reads the
  touch in the shard host's decide pass, after the frame's last event (0100); a handler that
  reads it between two events of one frame sees the book partway through that frame, never
  crossed. A hand-built fixture pins each case, a bid side whose bounds descend among them, and
  a stale level outside the window never becomes the touch.
- An `orderbook` Snapshot with no level on either side never makes a book valid. Hibachi's
  WebSocket market docs send a Snapshot, which "contains all price levels", when a subscription
  is first made, and the Updates after it carry only the levels that changed (Java
  `HibachiQuoteEngine.java`, whose `BookState` comment blames a book built from Updates alone for a
  wrong best ask, and whose `onOrderBookUpdate` saw empty Snapshots on reconnect, cleared its
  book and so rebuilt it from the Updates alone). The frame cannot tell a market with no resting
  order from a Snapshot sent before the venue's book was ready: a book made valid by it and
  filled by the Updates after it would show a touch, and call levels empty, from deltas alone,
  while a book left invalid costs a truly empty market nothing, since neither has a touch to
  quote. So on such a Snapshot, on a first subscription, a resubscribe or a reconnect, and on
  every gap the codec finds itself (a null, absent or unbounded side, or a Snapshot or Update
  that fails the bounds check above), the codec reports that book's feed as a gap (`fbc-book`
  then drops its Updates until a complete snapshot, 0039) and pushes no level or window of it
  until a Snapshot with a level that passes the check, which it applies as a fresh snapshot. It
  then asks for a fresh Snapshot on the same connection, leaving the other books there valid:
  after the retry interval of a required venue configuration key, with no default and zero
  refused (as `binance_usdm.depth_snapshot_retry`), it unsubscribes that instrument's
  `orderbook` and subscribes it again with the same `granularity` and `depth`, each frame
  charged as a subscription of that instrument (0018), again at each interval while Snapshots
  come back empty or failing the check and never for a subscription the consumer has removed;
  when a resubscribe brings back no Snapshot within the interval, it asks for a reconnect paced
  by the consumer's `ReconnectPacing` (0023), which gaps every book that connection fed.
  Decision 0022 rejected a Paradex codec writing its own subscribe frames, since the reconciler
  would no longer know what the connection has, so a decision record states why this codec may,
  for every gap it finds itself, before it does. A hand-built fixture pins the empty Snapshot,
  and a test against a local stub on a paused clock shows the book not valid through the Updates
  after it while another instrument's book on the connection stays valid, the resubscribe at
  the interval, the same resubscribe after an Update whose bounds disagree with the book, the
  book equal to the populated Snapshot that answers each, and the reconnect when a resubscribe
  brings none.
- A trade printing inside the book changes no book state and never holds or rejects a book
  update (the Java library's trades-versus-book check froze its book and was deleted), and a
  reconnect replays every subscription.
- The market-data conformance checks pass on fixtures under `fixtures/hibachi`, with the
  order-entry checks skipped by name, and decoder replay of a journal rebuilds the book
  byte-identically (0006, 0009).
- Hibachi's MM-partner-only book topic is neither planned nor decoded.
- `md_watch` takes `--hibachi <symbol>`, ETH/USDT-P first beside `--binance ETHUSDT`; its test
  drives the Hibachi wiring against a local stub server, and nothing in the repository runs it
  against Hibachi.

### BT-602 — The offline Hibachi signer

**Status:** Proposed  
**User story:** As the owner, I want Hibachi's orders, modifies, cancels and cancel-all signed by
a hand-written offline signer checked byte for byte against the Java library, so that order
entry starts from a proven signer that never needed a connection to be tested.

**Acceptance criteria:**

- `crates/venues/fbc-venue-hibachi/src/sign` (a review path) packs a place and a modify
  big-endian as nonce u64, contract id u32, quantity u64, side u32 (ASK 0, BID 1), price u64
  (omitted for a market order) and max fees u64: 40 bytes for a limit order, 32 for a market
  order (Java `HibachiPayloadPacker.java`; SDK `api.py`; 0009). Max fees is an input the caller
  passes with each place and modify; the signer holds no value and no default for it. It scales
  whatever decimal the request's `maxFeesPercent` carries, whose unit the sources disagree on:
  the signing docs' field list says basis points while their worked example passes a fraction
  (0.0005, "5 basis points"), the SDK's docstrings say a percentage while its examples pass
  0.0005 and 0.00045, and Java's `HibachiConfiguration.java` reads it as a percent (0.05 for
  5 bps).
- A cancel signs the 8-byte order id, or the order's placement nonce as 8 bytes (SDK `api.py`
  `__cancel_order_payload`; Java cancels by order id or nonce); a cancel by client id (its ASCII
  bytes, which only the API docs describe) is declared only once a golden vector or the owner's
  evidence backs it. Cancel-all signs its 8-byte nonce, and so can the cancel-on-disconnect arm
  (`orders.enableCancelOnDisconnect`, params `{nonce}`), with its own golden vector: Hibachi's API
  collection is cited as requiring a signature over the arm's nonce, while the SDK at 14a76b9
  sends the arm unsigned (`api_ws_trade.py` `enable_cancel_on_disconnect`, `types.py`
  `EnableCancelOnDisconnectParams`), so the probe records which form Hibachi accepts and the
  order-entry codec sends that form. `creationDeadline`, `orderFlags`, trigger, TWAP and
  parent-order fields are JSON-only and never signed.
- The signature is ECDSA secp256k1 over SHA-256 of the pack, 65 bytes r, s, v with v in {0, 1};
  ECDSA only, with no HMAC path (Java `EcdsaHibachiSigner.java`; the coordinator's default of
  2026-10-19).
- Golden vectors from a Java oracle on a SYNTHETIC key under `fixtures/hibachi/signing` match:
  the packed bytes of every command, the worked example buffer of Hibachi's signing docs byte
  for byte, and each signature equal to the oracle's or, if the Java signer's nonce is shown not
  to be RFC 6979, verifying and recovering to the synthetic public key (design §6
  `signing_golden`). That example's fee arithmetic is wrong: the docs say rate fees scale by
  10^8, but pack the example's `max_fees` 0.0005 as 5000 (`0x1388`), which is 0.0005 × 10^7.
  The SDK (`api.py` `__create_or_update_order_payload`) and Java (`HibachiPayloadPacker.java`
  `scaleMaxFeesPercent`, through which Java's broker signs its orders, and whose test packs 0.5
  as `0x2faf080`) both scale by 10^8. So the worked buffer is reproduced from max fees 0.00005,
  a second vector with the example's other inputs and max fees 0.0005 packs the fee as 50000
  (`0xc350`), and the signer has no 10^7 path.
- The decimal inputs are checked against the grid before packing: a price off `tickSize` is
  refused, never rounded (SDK `types.py` `check_tick_size`), and so is a quantity off
  `stepSize`, a stricter rule than the SDK's, which truncates the quantity silently (`api.py`). The
  scaled price, price × 2^32 × 10^(settlementDecimals − underlyingDecimals), is then truncated
  toward zero as the SDK's `price_to_bytes` and Java's packer do, since that truncation is part
  of the wire format and is rarely exact for an on-grid price; quantity (× 10^underlyingDecimals)
  and max fees (× 10^8) must scale exactly or are refused. A golden vector with a price whose
  scaling is not exact pins the truncation: on a synthetic instrument with ETH's decimals
  (underlying 9, settlement 6) and a 0.01 tick, 2500.01 packs to price 10737461189 (SDK
  `api.py` `price_to_bytes` and its order payload).
- The venue crate's `src/auth` (a review path) takes the private key from `Secrets`, validates
  it and hands an initialized signer to the codec, as Paradex's `auth::order_signer` does;
  `src/sign` holds only the cryptographic signer. Neither the key nor a signed payload is
  logged, and the crypto crate passes the licence gate (0009, 0017).

### BT-603 — Runtime prerequisites for a venue on two authenticated sockets

**Status:** Proposed  
**User story:** As the author of the Hibachi adapter, I want the runtime to authenticate a
WebSocket upgrade with headers, run one order-entry session over two sockets, and issue nonces
from the wall clock, so that Hibachi order entry, once a record allows it, is codecs and a
capability table only, as BT-102 promises.

**Acceptance criteria:**

- A planned endpoint's credential headers (Hibachi's `Authorization` API key) are sent on the
  WebSocket upgrade of its first connection and of every reconnect, a market-data socket sends
  none, and the key appears in none of the journal's bytes, log lines or errors; the header code
  lives under `crates/fbc-runtime/src/auth` (a review path), as decision 0116 states (0006,
  0009).
- The venue crate's `src/auth` (a review path) puts the API-key headers on private REST and on
  the `/ws/trade` and `/ws/account` upgrades with the account id in their URLs, keeps the
  account id and the account stream's listen key out of the journal's bytes, log lines and
  errors as it keeps the key (a test proves each absent), and decodes a test-connection reply into
  `AccountSummary` and a refused key into a typed error, all with synthetic credentials (0009).
- One order-entry session runs one `ExecCodec` over every connection its plan names: each
  endpoint opens and reconnects in its own epoch while the other's stays open, a frame goes to
  the endpoint its effect names, and the events of both are stamped in one ingest order, tested
  with the conformance toy on two local servers (0115).
- A production `NonceSource` issues monotonic wall-clock microseconds over an injected clock:
  strictly increasing when the clock stalls or steps back, never reissuing a nonce an earlier
  journal holds or one an earlier process sent while its journal record was still unwritten
  (the case 0114's item 2 leaves out), and refusing rather than issuing a nonce outside the
  window the caller passes (Hibachi documents 15 s either side of server time); its nonces are
  journaled and replay without the clock byte-identically (Java `HibachiBroker.java`; 0006,
  0114).
- Nothing in this story places a Hibachi order: Hibachi's capabilities keep `exec: None`, and
  this story leaves the session's cancel-on-disconnect gate as 0058 and 0121 set it.

### BT-604 — Hibachi order entry, gated on the cancel-on-disconnect probe

**Status:** Proposed  
**User story:** As the owner, I want to place, modify and cancel Hibachi orders from the
separate Rust-only account, but only after a supervised probe has shown what Hibachi's
cancel-on-disconnect does, so that no Hibachi order rests unprotected through a dropped
connection.

Depends on BT-601, BT-602 and BT-603.

**Acceptance criteria:**

- Before any order-entry ticket other than the probe's starts, a merged decision record, based
  on the evidence of the owner-run cancel-on-disconnect probe, supersedes 0058's Hibachi
  sentence. It cites the probe's run record (date, scenarios, observed order states and times,
  and which scenarios it marks inconclusive and why). It rests on no inconclusive scenario, a
  scenario being inconclusive unless reads of `/market/exchange-info` through its window showed
  the exchange `NORMAL` and the market `LIVE`; on none the probe's two runs did not both find
  conclusive and agreeing; and on no cancel reason alone, since Hibachi has been seen to report
  a cancel it started itself as `USER_CANCELED` (Java `HibachiConfiguration.java`). It states
  the cancel delay observed for each kind of drop the probe runs, a silent loss of the link
  included (one that sends Hibachi nothing, so only Hibachi's own idle close of the connection
  can notice it: Java `HibachiTradeWebSocketClient.java`); what a consumer must assume for a
  silent loss, declaring no protection the probe did not show; and what the probe's silent loss
  cannot show, since its REST reads still reach Hibachi meanwhile, so it shows the idle close
  over a working path rather than a partition. It declares what
  `orders.enableCancelOnDisconnect` does (its scope, per connection as 0080 has it or
  account-wide as 0121 has it; whether it re-arms per connection; whether it covers orders
  already open) and so what cancel-on-disconnect Hibachi declares, or that 0058's refusal
  stands; and where the probe did not show an order cancelled after a silent loss, it states
  whether 0058's refusal may still be lifted, and on what grounds, against 0058's "What would
  show this was wrong". Until it merges, 0058's refusal stands for Hibachi order entry.

## Epic G: QFEX

### BT-701 — QFEX public market data before order entry

**Status:** Proposed  
**User story:** As a trading program evaluating QFEX, I want its public touch, depth and
trades normalized through the same runtime, so I can record and replay the venue before any
private trading path is enabled.

**Acceptance criteria:**

- `fbc-venue-qfex` plans the public MDS WebSocket, sends documented subscriptions and decodes
  BBO, the pulsed level2 window and public trades into exact normalized events. Its factory
  declares `exec: None` and `Readiness::Record` (0117).
- Hand-built fixtures from QFEX's published schemas prove timestamps, exact price and size
  conversion, malformed-frame atomicity, and subscription validation.
- Instrument discovery and private order entry are separate tickets; only private-path evidence
  can lift the recording ceiling.
