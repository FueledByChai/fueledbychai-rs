# 0129 — Hibachi's orderbook codec bounds-checks every frame on its own copy, gaps a book it finds wrong and recovers it by its own resubscribe and then a reconnect, augmenting 0022 and 0124

Status: accepted
Date: 2026-10-27

## Context

FBC-2kmp (BT-601) decodes Hibachi's anonymous `/ws/market` WebSocket. Its `orderbook` topic
carries no sequence number (`Continuity::Unsequenced`), so nothing chains its frames. Each frame
does state, per side, a `startPrice` and an `endPrice`, and the owner's capture of the public
feed (2026-10-22, stated qualitatively in FBC-2kmp's notes) showed them describing the book
after the frame: each side's best level at `startPrice`, its depth-th level at `endPrice`, the
bid's range running downward, an unchanged side sent as `levels: []` with both bounds, every
scroll-in and scroll-out stated, and no side ever null, absent or missing a bound. Replayed
offline with each Update dropped in turn, a check of those bounds caught most lost frames: a gap
detector, not a proof of continuity. Hibachi's WebSocket market docs send a Snapshot ("all price
levels") on subscribe, define a null side as the removal of all its levels, take a subscription's
`granularity` and `depth` as one set per subscription, and document no error frame, no
subscription acknowledgement and no sequence. The Java library rebuilt books from Updates alone
after an empty Snapshot on reconnect, the delta-only book its own `BookState` comment blames for
a wrong touch.

Decision 0022 rejected a Paradex codec writing its own subscribe and unsubscribe frames from
`on_frame`: the reconciler would no longer know what the connection has, and the frames would
bypass the limiter's subscription charge. Paradex recovers a broken book by reconnecting its
stream, which takes the connection's other books with it. 0124 gave the core a stateless
one-side prune and clear for a venue that bounds each side on its own, and fixed the emission
order; how Hibachi's codec uses them, and how it recovers, was left to FBC-2kmp.

## Decision

1. **Subscriptions.** Each subscription is one frame under Hibachi's `parameters` key naming one
   instrument and one topic, charged `RateCharge::one(OpKind::Subscribe, Some(inst))` (0018). An
   `orderbook` subscription also names `granularity`, the instrument's
   `quote_grid.unwrap_or(tick)` written as the spec holds it (the owner's decision of 2026-10-25
   on FBC-jyyl), and `depth`, the required `hibachi.orderbook_depth`, as fields beside `symbol`
   and `topic`: no source shows where they go, so every frame's own `granularity` (compared by
   value) and `depth` must equal the subscription's. The codec subscribes nothing on `on_open`,
   so a reconnect's fresh codec sends exactly the reconciler's set once.
2. **The check, on the codec's own copy, before any event.** The codec keeps a copy of each
   subscribed book's levels (a venue crate holds no `fbc-book` book), applies a whole frame to a
   copy of it and checks the result before it pushes anything: on each side that carries bounds,
   its best level at `startPrice`, exactly the subscription's depth levels within the min..max of
   its bounds, the last at `endPrice`, none outside; the book not crossed (best bid above best
   ask). A level the window removes that the frame does not itself set to zero fails it: that is
   what a lost frame leaves. A locked book (best bid equal to best ask) passes and is counted
   apart, since two buckets can share a label while no order crosses. A side thinner than the
   depth, its best level at `startPrice` and every level within its bounds, fails and is counted
   apart wherever its `endPrice` lies: what `endPrice` names on a thin side is unknown, so the
   codec guesses nothing. That is an assumption no frame has shown. It is counted so only when
   the frame fails the check by nothing else: both sides and the crossed test are checked first,
   so a crossed book beside a thin side counts as a failed check. A lost frame that leaves a side
   short is counted as thin too, since the two cannot be told apart; the book gaps either way.
3. **The gaps the codec finds itself.** Each of these is counted under its own name, reports
   `MdEvent::Health { feed: Feed::Book(BOOK_ORDERBOOK), h: Gap }` for that book only, and pushes
   nothing else of the frame: a Snapshot with no level on either side (the empty-Snapshot rule:
   the frame cannot tell an empty market from a Snapshot sent before the venue's book was ready,
   and a book made valid by it and filled by Updates would be a delta-only book; a truly empty
   market costs nothing left invalid, having no touch to quote); an absent side; a side that is
   not null and lacks either bound, with levels or without; a level or bound that cannot be read,
   is off the tick or listed twice; a frame whose `granularity` or `depth` differs from its
   subscription's or lacks either; a level off the subscription's granularity (for a contract
   with no qualifying granularity, subscribed on its tick, this is the only guard against the
   venue's default); and a frame failing the check, a thin side included. A Snapshot failing any
   of them never makes a book valid. From the gap until a Snapshot with a level that passes, every
   Update of that book is passed over, and an Update reaching a book with no valid copy (before
   its first Snapshot, or after an unsubscribe) is ignored.
4. **A null side is a one-side clear.** A side sent as null is emitted as 0124's
   `MdEvent::SideClear` and counted; the frame's other side is applied and checked like any side
   carrying bounds, and the book stays valid with no touch on the cleared side: no gap and no
   resubscribe. The side stays empty and unchecked while frames send it null, and is checked from
   the first frame that sends it with both bounds. A Snapshot with one side null and the other
   passing makes the book valid with that side empty.
5. **Emission order (0124 item 6).** A side's bounds become `MdEvent::SidePrune` with its lower
   price first. A stated zero inside the side's range after the frame, or better than its best
   end, for a level the book has is a zero `Level` before that side's prune; one past the deep end
   is not emitted (the prune removes it, and it may only have scrolled out). Every removal of the
   frame, on both sides, comes before any level it sets, so a book read between two events of one
   frame is never crossed. Never a both-sides `Window`, and never zero quantities in place of a
   prune or a clear.
6. **Recovery: the codec's own resubscribe, then a reconnect.** A gap arms a timer of the required
   `hibachi.orderbook_resubscribe` (whole milliseconds, zero refused, no default); when it fires
   the codec writes an `unsubscribe` and then a `subscribe` of that instrument's orderbook with the
   same granularity and depth, each charged one subscription of the instrument, and waits the same
   interval for a Snapshot. A Snapshot that comes back empty or failing gaps the book again and
   arms the next retry; one that passes makes the book valid; any other orderbook frame meanwhile
   leaves the wait standing (an Update is passed over, and a frame of no known kind is counted and
   reports the gap), so no frame but a Snapshot keeps the reconnect from being asked for; none
   within the interval asks for one `Effect::Reconnect`, paced by the consumer's `ReconnectPacing`
   (0023), whose fresh codec gets every subscription again from the reconciler. The first
   subscription's Snapshot is waited for the same way: none within the interval reports the gap and
   resubscribes. A subscription the consumer removes takes its book and its timer with it, so it is
   never resubscribed. Timers cannot be cancelled: each carries a fresh tag, and a tag that is no
   longer its book's is ignored.
7. **Why this codec may write its own frames where 0022's could not.** The unsubscribe and
   subscribe name a subscription the reconciler already counts as active and leave it active, so
   its set stays true of the connection, as it does after a reconnect. Both are charged as
   subscriptions of the instrument, so a limit on subscriptions would count them; Hibachi declares
   none, so the limiter never refuses them (a frame a codec writes outside a subscribe call is
   dropped when refused, not retried). They are journaled as the codec's outbound frames, and
   decoder replay reproduces them because the codec is deterministic, its timer tags included,
   and the timer firings are journaled. And Hibachi documents an unsubscribe that needs no
   parameters and one set of parameters per subscription, so a resubscribe asks for exactly the
   book that broke and leaves every other book on the connection valid, where a reconnect would
   gap them all (0039). The reconnect stays the backstop when the venue does not answer.
8. **The touch.** `ask_bid_price` becomes a `Touch` only from a frame carrying each side's price
   and size (`bidSize` and `askSize`, as the Java library reads them; the docs' example shows
   prices only), the bid below the ask. Any other frame (a size missing, the bid at or above the
   ask, as a call auction after maintenance or the FX weekend can show, or a value unreadable) is
   counted, and if a Touch is in force it is invalidated with
   `MdEvent::Health { feed: Feed::Touch(TOUCH_ASK_BID_PRICE), h: Gap }`; the next usable frame's
   Touch restores it. `Lvl` carries a mandatory size, so no Touch says a size is unknown and none
   is ever synthesized. Removing the subscription pushes nothing (a codec's `subscribe` has no
   sink), so a Touch stays recorded as in force through a removal, and the subscription added
   again on the same connection invalidates it on its first unusable frame.
9. **The rest.** `trades` (both shapes) carry the taker side and their own `timestamp` in whole
   seconds; `mark_price` is `MdEvent::Mark`; `spot_price` is `MdEvent::Index`, the nearest feed the
   core has: Hibachi's Funding page computes funding from a spot index, the median across oracle
   sources, but no source says the `spot_price` topic is that index. The documented keys are read
   (`markPrice`; a trade's `takerSide`, `Buy` or `Sell` as the SDK's `TakerSide`), and where one is
   absent the alias the Java library reads and FBC-2kmp names (`price`; `side`, held to the same
   two values, any other refused rather than defaulted to a buy as the Java library does); no other
   alias is read. `funding_rate_estimation` is `MdEvent::Funding` exactly as the frame carries it
   in units of 1e-12, `interval: None`, `next: None` (FBC-204c). An event carries an exchange
   timestamp only from its frame (a root `timestamp_ms`, which no example frame shows, or a trade's
   `timestamp`), with `ExchTsKind::Unknown`. A frame with no topic is an acknowledgement
   (`{"id":..,"status":200}`) or refused; no refusal frame is documented, so the codec reports no
   `FeedHealth::Refused` (0042).
10. **Counting.** What the codec counts is read on the concrete `HibachiMd` (`counts()`):
    `MdCodec` has no counter beyond the runtime's refused frames, and a gap or a passed-over frame
    is not a refusal.

## Alternatives

- **Reconnect on every gap, as 0022 does for Paradex.** Every gap would take the connection's
  other books, the touches and the trades with it, and an instrument whose Snapshots keep coming
  back empty would hold every other market on the connection in a reconnect loop.
- **A REST `/market/data/orderbook` snapshot as the anchor.** Its levels carry no sequence either,
  so it cannot be merged with the Updates in flight; a fresh WebSocket Snapshot is the only clean
  restart.
- **Apply an empty Snapshot as a valid empty book.** The Updates after it would build a delta-only
  book that shows a touch, the Java library's bug.
- **Gap the book on a null side.** Hibachi's docs make null an authoritative clear, and gapping
  would discard a valid other side and could resubscribe again and again while a market's side
  is legitimately empty (Codex r4354155576).
- **Read a null or absent side as unchanged, as the Java library does.** "Unchanged" has its own
  encoding (`levels: []` with bounds), and an absent side is not documented: reading it as
  unchanged would leave a side unchecked.
- **Arm no timer for a first subscription.** A Snapshot that never comes would leave the book
  silently without a copy for the connection's life.
- **A counter hook in `fbc-core`.** A change to every codec under 0003 that FBC-2kmp does not
  name; the counts stay on the concrete codec until a ticket adds one.

## Consequences

- A book on Hibachi is invalid from a gap until a passing Snapshot; the connection's other books,
  touches and trades are untouched by it unless the resubscribe goes unanswered.
- The codec carries a copy of each subscribed book and applies every frame twice (to the copy and,
  through its events, to the consumer's book); the depth is at most 100 levels a side.
- The conformance suite's `continuity` check skips Hibachi by name (`Unsequenced`), and its
  `decoder_deterministic` skips a venue with no order entry: the bounds check and the decoder's
  determinism are proven by the crate's own tests and its stub test's decoder replay.
- Should Hibachi ever declare a subscription limit, the codec's own frames would be charged
  against it and a refused one would be dropped, not retried: that change needs a record of its
  own.
- What the codec counts reaches a consumer only through the concrete type.

## What would show this was wrong

- A capture showing a side's range that cuts inside its best level after the frame, a side null,
  absent or unbounded in normal operation, or a side thinner than the depth in a live market:
  the check would gap good books, and needs revisiting with that evidence.
- A resubscribe that Hibachi answers with no Snapshot while the connection stays healthy, so the
  codec reconnects routinely: recovery would need another path.
- Books the check passes that differ from the Snapshot a resubscribe returns right after: lost
  frames the bounds do not show, so a periodic resubscribe or another detector is needed.
- A frame shape for the subscription's `granularity` and `depth` other than the one this codec
  writes, shown by every frame coming back at the venue's default and gapping.
