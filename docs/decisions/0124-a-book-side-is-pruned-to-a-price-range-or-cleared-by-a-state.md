# 0124 — A book side is pruned to a price range or cleared by a stateless one-side event and read as any side its levels leave, augmenting 0014, 0038 and 0039

Status: accepted
Date: 2026-10-23

## Context

`MdEvent::Window` bounds both sides of a book at once, holds across snapshots, and says that
anything outside it is unknown. `fbc-book`'s `L2Book::set_window` prunes both sides together,
and no event clears one side. Hibachi's orderbook Update frame bounds each side on its own (a
`startPrice` and an `endPrice` per side, levels outside them removed before the listed levels
apply) and sends a side as null to clear it (BT-601; Hibachi's WebSocket market documentation;
the Java `HibachiQuoteEngine`'s `onOrderBookUpdate` and `pruneOutsideWindow`). Mapped onto
`Window`, a one-sided Update deletes valid levels on the other side; left out, it leaves stale
levels that can become the touch (Codex r4323052671 on PR #186). A codec standing in with
zero-quantity levels was rejected there: a level that only left a side's range is unknown, not
emptied, and `fbc-sim` reads a level set to zero as public cancels ahead of a simulated order
(0038).

FBC-2kmp's capture evidence, stated qualitatively: after each frame a side's best level sits at
one of its bounds and its deepest shown level at the other; the venue states every level that
scrolls in or out; an unchanged side comes with its bounds and no level. A null side is an
authoritative clear of a book that stays valid, with no gap (Codex r4354155576). FBC-n35m left
this record one question: does a one-side range keep `Window`'s "outside is unknown" meaning,
or is it a stateless prune after which the side reads as any side reads? The owner decided on
2026-10-23 that a cleared side reads unknown, not known-empty, with no new book state.

## Decision

1. **Two events.** `MdEvent::SidePrune { inst, book, side, lo, hi }` removes `side`'s levels
   outside `lo..=hi`, inclusive; `MdEvent::SideClear { inst, book, side }` removes all of
   `side`'s levels. `L2Book::prune_side` and `L2Book::clear_side` apply them. `lo > hi` is
   refused as `BookError::InvertedWindow` in every state, with nothing changed. A codec gives a
   range that runs downward (Hibachi's bids) as its lower price, then its higher.
2. **Stateless.** Neither event stores a range, a flag or an epoch, and neither touches the
   other side, the both-sides window, the book's state or whether a snapshot is in progress. A
   level set after a prune is kept and reported wherever it lies; a venue whose range holds
   across frames sends it again with each. `L2Book::canonical_bytes` keeps layout version 1,
   since there is nothing new to write: a prune or a clear changes the bytes exactly when it
   removed a level, and equal bytes still mean equal books, so equal reads (0006). The converse
   still does not hold: a level stored outside a window, and the snapshot in progress, are
   written but not read.
3. **Reads.** `L2Book::level`'s rules, applied to the levels left. With no both-sides window, a
   price better than a side's best level, or between two of its levels, reads empty; a price
   past its deepest level reads unknown; a side with no level, a cleared one included, reads
   unknown at every price and has no touch. Under a both-sides window the window decides, as it
   does after any delta. A clear never changes whether the book is valid: it stays valid with
   no touch on that side, and no gap follows.
4. **A delta's routing.** While a snapshot is in progress the event acts on the replacement
   only, and the book being replaced reads as it did until the snapshot ends; otherwise it acts
   on a valid book; otherwise it is dropped (`Applied::DroppedNotValid`), changing nothing. As
   `set_level` does, it reports `Changed` when it acted, even if it removed nothing. This
   departs from `set_window`, which bounds the book and the replacement alike: a window is the
   channel's state and these events are not, and pruning the book being replaced would change
   the sizes `fbc-sim` compares at the snapshot's end, erasing the shrink that snapshot shows.
5. **Never a cancel (augments 0038).** In `fbc-sim` a prune or a clear reaches only the
   engine's books, never its level-change path, so no simulated order's queue moves under any
   bracket. A removed level shown again later reads as one the book did not know, or as growth,
   so its return credits no cancel either. A resting order on a side the book shows no level
   of, or past a side's deepest level, is refused `no_book`, as unknown depth already is.
6. **Emission order** (for FBC-2kmp and any later codec). A zero the venue states for a level
   is read by where that level lies against its side's range after the frame. Inside the
   range, or better than its best end, the level emptied: it is emitted as a zero `Level`
   before its side's prune, so `fbc-sim` credits that shrink. Past the range's deep end it may
   only have scrolled out, its orders still resting at the venue, so it is unknown, not
   emptied: it is dropped, or emitted after the prune, where it reads unknown and credits no
   cancel. A codec that checks that its prune removes only levels the venue stated counts both
   as stated. Every removal of a frame, on both sides (zeros, prunes and clears), goes before
   any level the frame sets, so a consumer reading the book between two events of one frame
   never sees it crossed. The stateless reading is truthful only where a side's range does not
   cut inside that side's best level after the frame; a venue whose range can needs a one-side
   window with state, under a new record.
7. **Routing everywhere (augments 0014 item 7 and 0039).** Each event reaches only the book of
   its (instrument, `BookId`). `MdBooks` and `SimEngine` record its connection epoch as the
   last to feed that book whether the book took it, dropped it or refused it, so an epoch's end
   gaps a book a prune or a clear fed last; a refused one is counted in `MdBooks::refused` and
   still reaches the handler. The conformance suite's `continuity` counts either as the codec
   taking the sequence and holds either while the book is broken; `book_channels` requires both
   on the channel's own `BookId` and reads neither as showing liquidity; md_watch prints the
   book a frame of only such events leaves. `Books::apply`, the runtime's and `SimEngine`'s
   `book_of`, `continuity`'s `on_book` and `book_channels`' `book_of` name every event, so the
   next variant is a compile error there rather than a silent drop. Two matches keep a
   fallback: `SimEngine::on_market`'s hands every event it does not act on to `Books::apply`,
   and `continuity`'s hold of a broken book has guarded arms, so a later book event is held
   there only once it is added by hand.
8. **Nothing else changes.** `Window` keeps its both-sides meaning and its persistence for the
   venues that use it. The journal format does not change: `fbc-journal` records frames, and
   decoder replay decodes them again (0006). `BookCaps` gains no field: `windowed` covers a
   channel bounded by either form, and nothing branches on which form a channel uses (0003).

## Alternatives

- **A one-side window that keeps "outside is unknown".** With Hibachi's bounds its inner edge
  is the touch, so every price inside the spread would read unknown on that side and `fbc-sim`
  would refuse every resting order that improves the touch. It needs per-side state in the
  book and in the snapshot in progress, canonical layout version 2, and rules for how it holds
  across snapshots, gaps and clears; a stale side window hides valid levels.
- **A cleared side read as known-empty.** It needs a per-side flag in the book and in the
  replacement, layout version 2 and an exact reset rule; a flag reset wrongly calls a capped
  feed's unpublished depth empty, which 0038 refuses. The owner chose unknown (2026-10-23).
- **Zero-quantity levels.** `fbc-sim` reads them as cancels, and a level that only left the
  range is unknown, not empty.
- **`Window` with an optional side.** It changes every builder of `Window` (Binance, QFEX) and
  mixes a stateful and a stateless meaning in one event.
- **One event with an optional range.** A missing range reads as "unbounded", not as "clear".
- **Applying to the book being replaced too, as `set_window` does.** It changes what `fbc-sim`
  compares at a snapshot's end and erases that snapshot's cancel credit; FBC-n35m's
  `a_side_prune_inside_a_snapshot_leaves_the_snapshots_shrink_credited` fails that way.
- **Refusing one-side events on a windowed book.** Nothing needs it, and no venue mixes them.

## Consequences

`MdEvent` has two more variants: a match on it that names every variant must handle them, and
the matches item 7 names as naming every event now do. `L2Book` gains `prune_side` and
`clear_side`, and `set_level` shares their routing. Decoder replay needs no change. `fbc-sim`
cannot rest an order on a side the venue cleared until a level shows there, and keeps a resting
order's place behind public size a prune removed; both are conservative. With a side cleared,
`fbc-sim` also cannot classify a trade print whose aggressor is unknown by that side's touch, so a
print at the price of an order already resting there fills nothing, as on any side zero levels
emptied; that too is conservative, and a venue whose trades name the taker, as Hibachi's do, is
unaffected (Reviewer B on PR #202, RB202-1). FBC-2kmp emits each
stated zero as item 6 reads it, an emptied level's before its side's prune and a scrolled-out
one's after it or not at all, and every removal before any level it sets. Two books that differ
by a prune or a clear serialize differently through the levels it removed; one that removed
nothing leaves the books equal field for field. A channel mixing a both-sides window and
one-side events lets the window decide what is known; nothing enforces one form per channel, and
no venue mixes them.

## What would show this was wrong

- A venue whose one-side range excludes its own best level, so that prices better than the
  range would read empty.
- `fbc-sim` on markets with a routinely empty side, where refusing orders on that side skews
  results: that calls for a known-empty side, with state in the canonical bytes.
- 0038's calibration finding fills at pruned-then-shown-again levels systematically below real
  fills: cancels are being missed, and the emission order or the prune's meaning must change.
- Two books with equal canonical bytes that read differently.
