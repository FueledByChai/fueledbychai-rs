# 0044 — SimVenue is an unchanged ExecCodec writing frames on a simulated order stream to a pure engine fed the shard's envelopes

Status: accepted
Date: 2026-10-05

## Context

BT-501 asks for a simulated venue that "implements `ExecCodec` and plugs into the runtime in
place of a real venue, so replay and shadow run the same shard code as live" (design §10.1:
counterfactual, parity and shadow modes all run SimVenue for execution, three brackets at a
time). A simulated venue must see market data: it matches orders against the book and fills
resting orders from trades through FBC-30g's queue model (0038). An `ExecCodec` (0014) sees
only the frames, HTTP results and timers of its own order-entry session, and its decode calls
take no market data. FBC-uoo had to fix where the market data enters and where the matching
state lives, before FBC-6mf hosts it in the runtime and FBC-w9g replays through it.

## Decision

SimVenue is two halves joined by a simulated order-entry stream:

- **`SimCodec` is an `ExecCodec`, unchanged from 0014.** `encode` writes each place and cancel
  as one frame on the stream the consumer names (an `Effect::Send` with the command's RPC and
  timeout, traffic class and rate charge, like any venue), stamped with the encode's `EncodeCtx`
  wall and monotonic time; `on_frame` decodes each answer frame into an `Outcome`, `OrderUpdate`
  or `FillEvent` through `DecodeScope` only, so venue ids, fill ids and fees are built as a real
  codec builds them (0004), and says only what the stood-in venue's `FillCaps` report: without
  fill ids a fill is `FillIdent::Derived`, and without a liquidity flag its liquidity is
  `Unknown`. Its order and fill events carry our client id only where the venue's
  `cid_echoed_on_events` says they do, and the order's flags only where `events_echo_flags`
  does. It refuses what the stood-in venue's `OrderCaps` do not offer, and an order combining
  features they declare in `flag_conflicts` (`NotSent(FlagConflict)`). The frames are text
  records whose values are escaped, so no value (an asset symbol, a venue id) can split a
  record.
- **`SimEngine` is a pure state machine.** It is fed the shard's market-data envelopes in ingest
  order and the codec's frames, holds its own `fbc-book` books built from those envelopes (it
  never reads the runtime's `MdBooks`), one queue model per instrument, and its orders. It reads
  no clock: a command acts at its frame's encode time plus the consumer's `to_venue` latency,
  before any envelope stamped at or after that instant; an envelope acts at its stamp; every
  answer is a frame due `to_client` after the venue acted, which the host hands to the codec's
  `on_frame` then. `advance(now)` lets a host move time with a stamp it read when no envelope
  comes.
- **Matching.** A placement for an instrument the spec table does not list is refused
  (`no_book`), since its size cannot be judged. A placement of zero lots is refused `InvalidQty`,
  since zero lots is never an order (`InstrumentSpec::floor_qty`), and so is one below the spec's
  `min_size` or above its `max_order_size`, and one that would rest where the level's size, the
  simulated orders there and its own would not fit in lots; a limit price the spec's `price_grid`
  does not accept is refused `InvalidPrice`. A placement crosses the trading book's displayed
  levels as the taker; a post-only order that would cross is refused `PostOnlyWouldCross`; a
  good-till-cancelled limit order's rest queues (0038), on the level's size as the venue would
  show it with the simulated orders it holds there (`QueueModel::accept_shown`), since the real
  book never shows them, less the trades printed at its price whose shrink the level has not
  shown yet, which took their size before the order arrived (a trade printed through a level took
  all of it); immediate-or-cancel, fill-or-kill and market remainders are cancelled unfilled. A
  good-till-cancelled order that would rest where the book does not know the size (past its depth
  or window) is refused whole (`no_book`), crossing part included. A trade fills resting orders
  through the queue model as the maker; one without an aggressor is classified against the touch
  and ignored inside the spread. A level that shrinks by more than the trades printed at its
  price since it last changed is a level cancel, whether a delta or a replacement snapshot
  shrinks it; each change of the level ends what those trades explain, so none carries to a later
  change, while an update repeating its size is no change; a replacement snapshot restates every
  level, so it ends what was traded at each, even where it repeats a level's size or cannot
  compare sizes (a level the old or the new book does not reach), and a delta that first shows a
  level the book did not reach ends what was traded there. The trades at a level saturate at an
  `i64` of lots rather than drop a print.
- **Fees.** A fill's fee is the consumer's `FeeBook` rate for the simulated account, instrument,
  public channel and liquidity at the fill's wall time, times its notional
  (`InstrumentSpec::notional`), rounded to the nano, written in the stood-in venue's fee sign so
  `DecodeScope::fee` reads it back as a cost. A fee that is not a finite number of nanos
  strictly inside `i128`'s range (a NaN or infinite rate, an overflowing product, or
  `i128::MIN`, which `DecodeScope::fee` refuses) is no fee. A placement that would take a fill
  without a fee is refused (a fill-or-kill order that cannot fill whole takes nothing and needs
  none); a resting order that a trade would fill without one is cancelled by the venue, never
  filled with an invented fee, and the trade's size it would have taken goes to the orders
  behind it.
- **Configuration.** `SimConfig` carries the stood-in venue's `ExecCaps` and `MatchingCaps`, the
  latency, the RPC timeout, the stream, the bracket, the account, the fee book, the spec table
  and each instrument's trading book. None of them has a default and no number lives in `fbc-sim`
  (0001, 0009).
- **Not yet.** RPI orders are refused, since a public trade feed does not say which flow was
  retail and the queue model fills RPI orders from retail flow only (FBC-njk). Placements for a
  venue whose events the engine cannot say yet are refused (`NotSent(Unsupported)`) rather than
  answered with events unlike its own: one whose `ack` is `AckModel::TwoPhase`, since the engine
  accepts in one phase and would report a provisional acceptance as final (FBC-zr1), and one
  whose `ordering_key` is not `VenueSeq`, whose fills carry realized P&L or funding, or whose
  fills are derived from order status (FBC-938), one whose fills replay on reconnect (FBC-3q6),
  one with a `MatchingCaps::speed_bump`, since a command acts `to_venue` after it was sent
  and no later (FBC-7y8), and one whose `cancel_on_disconnect` is not `None`, since the
  simulated stream never disconnects and nothing would cancel what the venue cancels when it
  does (FBC-fji); cancels still go. A fill with a venue fill id always names its order's
  venue id and cumulative quantity, which `FillCaps` cannot yet say a venue's fills omit
  (FBC-2g7). `resync` answers nothing (FBC-bq3). A crossing order meets only the displayed book:
  not the venue's own resting orders, and two crossing orders between book updates can take the
  same lots (FBC-4qr). Reduce-only is echoed, not enforced, since the engine keeps no position.
  Of the spec's limits only the order size is judged; minimum notional, price band, trading
  status, position limit and open-order cap are FBC-205's.

## Alternatives

- **SimVenue as an `OrderGateway` only**, taking commands and market data in process with no
  codec: rejected. BT-501's first criterion is that it implements `ExecCodec` and plugs in
  where a real venue does, so the session, its RPC deadlines, journaling and decode path are
  the ones live trading runs; a gateway would skip all of them.
- **An `ExecCodec` grown with market-data input** (an `on_market` call on the trait): rejected.
  It changes the boundary of every exec codec for the sake of one (0014), and puts matching
  state, books and a fill model inside a codec, which 0002 keeps to encoding and decoding.
- **The engine reading the runtime's `MdBooks`**: rejected. It would tie `fbc-sim` to the
  runtime's book wiring and make the engine's answers depend on when the host read them; its
  own books built from the same envelopes are the same live and in replay.
- **Answers emitted at once, latency left to the host**: rejected. Where in the market-data
  stream a command acts decides what it fills; that order must be the engine's, from stamps,
  so two runs over the same inputs give the same answers.

## Consequences

- FBC-6mf's host delivers every envelope of the shard, the codec's frames and each answer at
  its due time; a quiet feed delays answers unless it calls `advance` with a stamp.
- A venue that publishes a book delta before the trade that caused it makes the engine count
  that shrink as a cancel and the trade again, over-advancing Middle and Optimistic queues; the
  shadow's calibration against Java (design §10.2) is where that shows.
- The frame format is internal to `fbc-sim` (`src/wire.rs`), one source for both halves; a
  journal of a SimVenue session records those frames like any venue's.

## What would show this was wrong

A host that cannot deliver the engine's answers at their due times through an ordinary
order-entry session (FBC-6mf); replay through SimVenue giving different answers for the same
envelopes and frames (FBC-w9g); or shadow fills whose fees disagree with the fee book's rates
times the fills' notionals.
