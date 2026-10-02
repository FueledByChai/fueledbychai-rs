# Product backlog

The stories fueledbychai-rs builds first, in the venue order of decision 0007. Stories are
intent with acceptance criteria; the tickets that build them live in Beads and name the story
they serve (`story:BT-nnn`). The decisions they rest on are in `docs/decisions/`; "design §n"
is a section of the approved architecture design (0001). The user throughout is a trading
program built on this library, written and run by the owner.

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
  minted elsewhere as `Unparseable` (0004, 0010, design §4.3).
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
  diff-depth book on a REST snapshot; it never sends an order (0007, design §6).
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
  versions; `bbo`, book deltas and `trades` decode to normalized events (0007, design §6, §15).
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
  seeds the port (0007, 0009, design §6 `signing_golden`, §15).
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
- Order entry starts disarmed after any restart; no place or amend that would exceed the
  resting cap or the inventory cap, or that arrives while the market's kill switch is on, is
  ever built, reducing orders (flatten, force-close, wind-down exits) included; a cancel is
  always built (0010, 0011).
- Arming order entry for a market requires the market lease, plus the account lease when the
  nonce scope is per account; a test shows a second holder of either lease is refused and
  cannot arm (0010, design §13.2).
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
