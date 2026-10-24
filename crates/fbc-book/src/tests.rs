//! FBC-qfi's done line: a complete snapshot replaces the book; deltas set and remove levels; a
//! gap invalidates the book until the next complete snapshot; a windowed book reports no level
//! outside its window; touch() and top(15) match a hand-built book; the top-n comparison names
//! a planted difference; and two books fed the same events have identical canonical bytes,
//! while a one-lot difference changes them. The error paths are here too.
//!
//! FBC-n35m's book half (decision 0124): a prune of one side to a price range removes only that
//! side's levels outside it, and a clear of one side empties only that side, the book staying
//! valid with no touch there; both act where a delta would (the snapshot in progress, else a
//! valid book); an inverted range is refused in every state; a pruned or cleared side reads as
//! any side its levels leave; the both-sides window keeps its behaviour; and the canonical bytes
//! change exactly when a prune or clear removed a level, their layout unchanged.

use fbc_core::{BookId, BookSide, Feed, FeedHealth, InstrumentId, Lots, Lvl, MdEvent, Ticks};

use crate::{Applied, BookError, BookState, Books, L2Book, LevelDiff, Top, Touch, Window};

const INST: InstrumentId = InstrumentId::new(7);
const BOOK: BookId = BookId(0);

fn lots(n: i64) -> Lots {
    Lots::new(n).unwrap()
}

fn lvl(px: i64, qty: i64) -> Lvl {
    Lvl {
        px: Ticks(px),
        qty: lots(qty),
    }
}

fn begin(epoch: u32) -> MdEvent {
    MdEvent::BookSnapshotBegin {
        inst: INST,
        book: BOOK,
        epoch,
    }
}

fn end() -> MdEvent {
    MdEvent::BookSnapshotEnd {
        inst: INST,
        book: BOOK,
    }
}

fn level(side: BookSide, px: i64, qty: i64) -> MdEvent {
    MdEvent::Level {
        inst: INST,
        book: BOOK,
        side,
        px: Ticks(px),
        qty: lots(qty),
    }
}

fn bid(px: i64, qty: i64) -> MdEvent {
    level(BookSide::Bid, px, qty)
}

fn ask(px: i64, qty: i64) -> MdEvent {
    level(BookSide::Ask, px, qty)
}

fn window(lo: i64, hi: i64) -> MdEvent {
    MdEvent::Window {
        inst: INST,
        book: BOOK,
        lo: Ticks(lo),
        hi: Ticks(hi),
    }
}

/// A prune of `side` to `lo..=hi` (decision 0124).
fn prune(side: BookSide, lo: i64, hi: i64) -> MdEvent {
    MdEvent::SidePrune {
        inst: INST,
        book: BOOK,
        side,
        lo: Ticks(lo),
        hi: Ticks(hi),
    }
}

/// A clear of `side` (decision 0124).
fn clear(side: BookSide) -> MdEvent {
    MdEvent::SideClear {
        inst: INST,
        book: BOOK,
        side,
    }
}

fn health(feed: Feed, h: FeedHealth) -> MdEvent {
    MdEvent::Health {
        inst: INST,
        feed,
        h,
    }
}

fn gap() -> MdEvent {
    health(Feed::Book(BOOK), FeedHealth::Gap)
}

fn feed(books: &mut Books, events: &[MdEvent]) {
    for ev in events {
        books.apply(ev).unwrap();
    }
}

fn book(books: &Books) -> &L2Book {
    books.get(INST, BOOK).unwrap()
}

/// A snapshot of 100x2 / 101x3 against 102x4 / 103x5 under epoch 1.
fn small_snapshot() -> Vec<MdEvent> {
    vec![
        begin(1),
        bid(100, 2),
        bid(101, 3),
        ask(102, 4),
        ask(103, 5),
        end(),
    ]
}

fn top_all(books: &Books) -> Top {
    book(books).top(usize::MAX).unwrap()
}

#[test]
fn a_complete_snapshot_replaces_the_book() {
    let mut books = Books::new();
    feed(&mut books, &small_snapshot());
    assert_eq!(book(&books).state(), BookState::Valid { epoch: 1 });

    // A second snapshot: until it ends, the old book is still the book.
    feed(&mut books, &[begin(2), bid(90, 1), ask(95, 9)]);
    assert_eq!(book(&books).snapshot_in_progress(), Some(2));
    assert_eq!(
        top_all(&books),
        Top {
            bids: vec![lvl(101, 3), lvl(100, 2)],
            asks: vec![lvl(102, 4), lvl(103, 5)],
        }
    );

    // At its end it is the whole book: nothing of the old one is left.
    assert_eq!(books.apply(&end()), Ok(Applied::Changed));
    assert_eq!(book(&books).state(), BookState::Valid { epoch: 2 });
    assert_eq!(book(&books).snapshot_in_progress(), None);
    assert_eq!(
        top_all(&books),
        Top {
            bids: vec![lvl(90, 1)],
            asks: vec![lvl(95, 9)],
        }
    );
}

#[test]
fn a_zero_level_inside_a_snapshot_removes_it_from_the_replacement() {
    let mut books = Books::new();
    feed(
        &mut books,
        &[begin(3), bid(100, 2), bid(99, 1), bid(100, 0), end()],
    );
    assert_eq!(
        top_all(&books),
        Top {
            bids: vec![lvl(99, 1)],
            asks: vec![],
        }
    );
}

#[test]
fn a_new_begin_abandons_an_unfinished_snapshot() {
    let mut books = Books::new();
    feed(
        &mut books,
        &[begin(1), bid(100, 2), begin(2), bid(50, 1), end()],
    );
    assert_eq!(book(&books).state(), BookState::Valid { epoch: 2 });
    assert_eq!(top_all(&books).bids, vec![lvl(50, 1)]);
}

#[test]
fn deltas_set_and_remove_levels() {
    let mut books = Books::new();
    feed(&mut books, &small_snapshot());

    // Set a new level, change one, remove one.
    assert_eq!(books.apply(&bid(99, 7)), Ok(Applied::Changed));
    assert_eq!(books.apply(&ask(102, 6)), Ok(Applied::Changed));
    assert_eq!(books.apply(&bid(101, 0)), Ok(Applied::Changed));
    // Removing a level that is not there leaves the book as it was.
    assert_eq!(books.apply(&ask(110, 0)), Ok(Applied::Changed));

    assert_eq!(
        top_all(&books),
        Top {
            bids: vec![lvl(100, 2), lvl(99, 7)],
            asks: vec![lvl(102, 6), lvl(103, 5)],
        }
    );
    assert_eq!(book(&books).state(), BookState::Valid { epoch: 1 });
}

#[test]
fn deltas_before_the_first_snapshot_are_dropped() {
    let mut books = Books::new();
    assert_eq!(books.apply(&bid(100, 1)), Ok(Applied::DroppedNotValid));
    assert_eq!(book(&books).state(), BookState::AwaitingSnapshot);
    assert_eq!(
        book(&books).touch(),
        Err(BookError::NotValid(BookState::AwaitingSnapshot))
    );
    feed(&mut books, &small_snapshot());
    assert_eq!(top_all(&books).bids, vec![lvl(101, 3), lvl(100, 2)]);
}

#[test]
fn a_gap_invalidates_the_book_until_the_next_complete_snapshot() {
    let mut books = Books::new();
    feed(&mut books, &small_snapshot());
    assert_eq!(books.apply(&gap()), Ok(Applied::Changed));
    let invalid = Err(BookError::NotValid(BookState::Gapped));
    assert_eq!(book(&books).state(), BookState::Gapped);
    assert_eq!(book(&books).touch(), invalid);
    assert_eq!(
        book(&books).top(15),
        Err(BookError::NotValid(BookState::Gapped))
    );

    // Deltas after the gap are dropped.
    assert_eq!(books.apply(&bid(100, 9)), Ok(Applied::DroppedNotValid));

    // A snapshot that has begun but not ended does not make it valid.
    feed(&mut books, &[begin(4), bid(200, 1), ask(201, 1)]);
    assert_eq!(book(&books).state(), BookState::Gapped);
    assert_eq!(book(&books).touch(), invalid);

    // A gap in the middle of a snapshot throws that snapshot away.
    feed(&mut books, &[gap()]);
    assert_eq!(book(&books).snapshot_in_progress(), None);
    assert_eq!(books.apply(&end()), Err(BookError::NoSnapshotInProgress));
    assert_eq!(book(&books).state(), BookState::Gapped);

    // Only a complete snapshot makes it valid again, with nothing from before the gap.
    feed(&mut books, &[begin(5), bid(300, 1), ask(301, 2), end()]);
    assert_eq!(book(&books).state(), BookState::Valid { epoch: 5 });
    assert_eq!(
        book(&books).touch(),
        Ok(Touch {
            bid: Some(lvl(300, 1)),
            ask: Some(lvl(301, 2)),
        })
    );
    assert_eq!(top_all(&books).bids, vec![lvl(300, 1)]);
}

#[test]
fn health_other_than_a_book_gap_changes_nothing() {
    let mut books = Books::new();
    feed(&mut books, &small_snapshot());
    let before = book(&books).canonical_bytes();
    for ev in [
        health(Feed::Book(BOOK), FeedHealth::Stale),
        health(Feed::Book(BOOK), FeedHealth::Live),
        health(Feed::Trades, FeedHealth::Gap),
        health(Feed::Book(BookId(1)), FeedHealth::Gap),
        MdEvent::Mark {
            inst: INST,
            px: fbc_core::PxExact::new(1, 0),
        },
    ] {
        assert_eq!(books.apply(&ev), Ok(Applied::Unchanged), "{ev:?}");
    }
    assert_eq!(book(&books).state(), BookState::Valid { epoch: 1 });
    assert_eq!(book(&books).canonical_bytes(), before);
    // A gap on another book channel of the instrument made that channel's book, not this one.
    assert_eq!(
        books.get(INST, BookId(1)).map(L2Book::state),
        None,
        "a health event alone creates no book"
    );
}

#[test]
fn each_instrument_and_book_channel_keeps_its_own_book() {
    let mut books = Books::new();
    feed(&mut books, &small_snapshot());
    let other_inst = InstrumentId::new(8);
    let other_book = BookId(1);
    for (inst, book_id, px) in [(other_inst, BOOK, 500), (INST, other_book, 600)] {
        books
            .apply(&MdEvent::BookSnapshotBegin {
                inst,
                book: book_id,
                epoch: 9,
            })
            .unwrap();
        books
            .apply(&MdEvent::Level {
                inst,
                book: book_id,
                side: BookSide::Bid,
                px: Ticks(px),
                qty: lots(1),
            })
            .unwrap();
        books
            .apply(&MdEvent::BookSnapshotEnd {
                inst,
                book: book_id,
            })
            .unwrap();
    }
    // A gap on one channel leaves the others valid.
    books
        .apply(&health(Feed::Book(other_book), FeedHealth::Gap))
        .unwrap();
    assert_eq!(book(&books).state(), BookState::Valid { epoch: 1 });
    assert_eq!(
        books.get(INST, other_book).unwrap().state(),
        BookState::Gapped
    );
    assert_eq!(
        books.get(other_inst, BOOK).unwrap().touch().unwrap().bid,
        Some(lvl(500, 1))
    );
    assert_eq!(top_all(&books).bids, vec![lvl(101, 3), lvl(100, 2)]);
    assert!(books.get(InstrumentId::new(99), BOOK).is_none());
}

#[test]
fn a_windowed_book_reports_no_level_outside_its_window() {
    let mut books = Books::new();
    feed(
        &mut books,
        &[
            begin(1),
            bid(95, 1),
            bid(98, 2),
            bid(100, 3),
            ask(101, 4),
            ask(103, 5),
            ask(106, 6),
            end(),
        ],
    );
    // The window narrows: what falls outside is unknown and goes.
    assert_eq!(books.apply(&window(97, 104)), Ok(Applied::Changed));
    assert_eq!(
        book(&books).window(),
        Some(Window::new(Ticks(97), Ticks(104)).unwrap())
    );
    assert_eq!(
        top_all(&books),
        Top {
            bids: vec![lvl(100, 3), lvl(98, 2)],
            asks: vec![lvl(101, 4), lvl(103, 5)],
        }
    );

    // A level set outside the window is not reported, by touch or top.
    feed(&mut books, &[bid(96, 9), ask(105, 9)]);
    assert_eq!(
        top_all(&books),
        Top {
            bids: vec![lvl(100, 3), lvl(98, 2)],
            asks: vec![lvl(101, 4), lvl(103, 5)],
        }
    );
    // Outside on both sides of the touch: the window hides even the best levels.
    feed(&mut books, &[window(99, 100)]);
    assert_eq!(
        book(&books).touch(),
        Ok(Touch {
            bid: Some(lvl(100, 3)),
            ask: None,
        })
    );
    for t in top_all(&books).bids.iter().chain(&top_all(&books).asks) {
        assert!((99..=100).contains(&t.px.0), "{t:?} is outside the window");
    }

    // The window holds across a snapshot: a replacement is windowed too.
    feed(
        &mut books,
        &[
            begin(2),
            bid(10, 1),
            bid(99, 1),
            ask(100, 2),
            ask(300, 1),
            end(),
        ],
    );
    assert_eq!(
        top_all(&books),
        Top {
            bids: vec![lvl(99, 1)],
            asks: vec![lvl(100, 2)],
        }
    );
}

/// FBC-30g: one level's size, which the queue model in `fbc-sim` reads when an order arrives.
#[test]
fn one_level_reads_its_size_zero_when_empty_and_unknown_past_what_the_book_shows() {
    let mut b = L2Book::new();
    assert_eq!(
        b.level(BookSide::Bid, Ticks(100)),
        Err(BookError::NotValid(BookState::AwaitingSnapshot))
    );
    b.begin_snapshot(1);
    b.set_level(BookSide::Bid, Ticks(100), lots(3));
    b.set_level(BookSide::Bid, Ticks(97), lots(2));
    b.set_level(BookSide::Ask, Ticks(101), lots(4));
    b.set_level(BookSide::Ask, Ticks(104), lots(1));
    b.end_snapshot().unwrap();
    assert_eq!(b.level(BookSide::Bid, Ticks(100)), Ok(Some(lots(3))));
    assert_eq!(b.level(BookSide::Bid, Ticks(97)), Ok(Some(lots(2))));
    assert_eq!(b.level(BookSide::Ask, Ticks(101)), Ok(Some(lots(4))));
    assert_eq!(b.level(BookSide::Ask, Ticks(104)), Ok(Some(lots(1))));
    // Between shown levels, and better than the touch, a price is empty.
    assert_eq!(b.level(BookSide::Bid, Ticks(98)), Ok(Some(Lots::ZERO)));
    assert_eq!(b.level(BookSide::Bid, Ticks(102)), Ok(Some(Lots::ZERO)));
    assert_eq!(b.level(BookSide::Ask, Ticks(103)), Ok(Some(Lots::ZERO)));
    assert_eq!(b.level(BookSide::Ask, Ticks(99)), Ok(Some(Lots::ZERO)));
    // Past the deepest level a capped book shows, the size is unknown, not empty.
    assert_eq!(b.level(BookSide::Bid, Ticks(96)), Ok(None));
    assert_eq!(b.level(BookSide::Ask, Ticks(105)), Ok(None));
    // A side with no level shows nothing known.
    let mut one_sided = b.clone();
    one_sided.set_level(BookSide::Ask, Ticks(101), Lots::ZERO);
    one_sided.set_level(BookSide::Ask, Ticks(104), Lots::ZERO);
    assert_eq!(one_sided.level(BookSide::Ask, Ticks(101)), Ok(None));
    assert_eq!(
        one_sided.level(BookSide::Bid, Ticks(100)),
        Ok(Some(lots(3)))
    );
    // A windowed book knows every price inside its window, past its deepest level too, and
    // none outside it.
    b.set_window(Ticks(98), Ticks(102)).unwrap();
    assert_eq!(b.level(BookSide::Bid, Ticks(100)), Ok(Some(lots(3))));
    assert_eq!(b.level(BookSide::Bid, Ticks(98)), Ok(Some(Lots::ZERO)));
    assert_eq!(b.level(BookSide::Ask, Ticks(102)), Ok(Some(Lots::ZERO)));
    assert_eq!(b.level(BookSide::Bid, Ticks(97)), Ok(None));
    assert_eq!(b.level(BookSide::Ask, Ticks(104)), Ok(None));
    // A level set outside the window is not known either.
    b.set_level(BookSide::Bid, Ticks(90), lots(5));
    assert_eq!(b.level(BookSide::Bid, Ticks(90)), Ok(None));
    // A windowed side with no level is known empty inside the window.
    b.set_level(BookSide::Bid, Ticks(100), Lots::ZERO);
    assert_eq!(b.level(BookSide::Bid, Ticks(100)), Ok(Some(Lots::ZERO)));
    b.gap();
    assert_eq!(
        b.level(BookSide::Bid, Ticks(100)),
        Err(BookError::NotValid(BookState::Gapped))
    );
}

#[test]
fn a_window_applies_to_a_snapshot_in_progress() {
    let mut books = Books::new();
    feed(
        &mut books,
        &[begin(1), bid(90, 1), bid(99, 2), window(95, 105), end()],
    );
    assert_eq!(top_all(&books).bids, vec![lvl(99, 2)]);
}

#[test]
fn an_inverted_window_is_refused_and_changes_nothing() {
    let mut books = Books::new();
    feed(&mut books, &small_snapshot());
    let before = book(&books).canonical_bytes();
    assert_eq!(
        books.apply(&window(105, 95)),
        Err(BookError::InvertedWindow {
            lo: Ticks(105),
            hi: Ticks(95),
        })
    );
    assert_eq!(book(&books).window(), None);
    assert_eq!(book(&books).canonical_bytes(), before);
    // A one-tick window is fine.
    let w = Window::new(Ticks(5), Ticks(5)).unwrap();
    assert_eq!((w.lo(), w.hi()), (Ticks(5), Ticks(5)));
    assert!(w.contains(Ticks(5)) && !w.contains(Ticks(4)) && !w.contains(Ticks(6)));
}

/// Twenty levels a side, bids from 1000 down and asks from 1001 up, quantities that differ by
/// level so a misordered top shows.
fn deep_book_events() -> Vec<MdEvent> {
    let mut events = vec![begin(1)];
    for i in 0..20 {
        events.push(bid(1000 - i, 10 + i));
        events.push(ask(1001 + i, 100 + i));
    }
    events.push(end());
    events
}

#[test]
fn touch_and_top_15_match_a_hand_built_book() {
    let mut books = Books::new();
    // Fed out of price order, to prove the book orders them.
    let mut events = deep_book_events();
    events[1..41].reverse();
    feed(&mut books, &events);

    let hand_bids: Vec<Lvl> = (0..15).map(|i| lvl(1000 - i, 10 + i)).collect();
    let hand_asks: Vec<Lvl> = (0..15).map(|i| lvl(1001 + i, 100 + i)).collect();
    assert_eq!(
        book(&books).touch(),
        Ok(Touch {
            bid: Some(lvl(1000, 10)),
            ask: Some(lvl(1001, 100)),
        })
    );
    assert_eq!(
        book(&books).top(15),
        Ok(Top {
            bids: hand_bids,
            asks: hand_asks,
        })
    );
    // Fewer levels than asked for: all of them.
    assert_eq!(book(&books).top(25).unwrap().bids.len(), 20);
    assert_eq!(book(&books).top(0), Ok(Top::default()));
}

#[test]
fn an_empty_side_has_no_touch() {
    let mut books = Books::new();
    feed(&mut books, &[begin(1), ask(5, 1), end()]);
    assert_eq!(
        book(&books).touch(),
        Ok(Touch {
            bid: None,
            ask: Some(lvl(5, 1)),
        })
    );
}

#[test]
fn the_top_n_comparison_names_a_planted_difference() {
    let mut left = Books::new();
    let mut right = Books::new();
    feed(&mut left, &deep_book_events());
    feed(&mut right, &deep_book_events());
    let (l, r) = (book(&left), book(&right));
    assert_eq!(
        l.top(15).unwrap().first_difference(&r.top(15).unwrap()),
        None
    );

    // Plant one lot more at the seventh ask level (rank 6, price 1007).
    right.apply(&ask(1007, 107)).unwrap();
    let r = book(&right);
    assert_eq!(
        l.top(15).unwrap().first_difference(&r.top(15).unwrap()),
        Some(LevelDiff {
            side: BookSide::Ask,
            rank: 6,
            left: Some(lvl(1007, 106)),
            right: Some(lvl(1007, 107)),
        })
    );

    // A level nearer the touch differs too: the comparison names the first, by rank, bids
    // before asks at the same rank.
    right.apply(&bid(997, 0)).unwrap();
    let diff = l
        .top(15)
        .unwrap()
        .first_difference(&book(&right).top(15).unwrap())
        .unwrap();
    assert_eq!(
        diff,
        LevelDiff {
            side: BookSide::Bid,
            rank: 3,
            left: Some(lvl(997, 13)),
            right: Some(lvl(996, 14)),
        }
    );
    assert_eq!(diff.to_string(), "bid rank 3: left 997x13, right 996x14");
}

#[test]
fn the_top_n_comparison_names_a_missing_level() {
    let shorter = Top {
        bids: vec![lvl(10, 1)],
        asks: vec![lvl(11, 1)],
    };
    let longer = Top {
        bids: vec![lvl(10, 1)],
        asks: vec![lvl(11, 1), lvl(12, 4)],
    };
    let diff = shorter.first_difference(&longer).unwrap();
    assert_eq!(
        diff,
        LevelDiff {
            side: BookSide::Ask,
            rank: 1,
            left: None,
            right: Some(lvl(12, 4)),
        }
    );
    assert_eq!(diff.to_string(), "ask rank 1: left none, right 12x4");
    assert_eq!(
        longer.first_difference(&shorter).unwrap().to_string(),
        "ask rank 1: left 12x4, right none"
    );
}

#[test]
fn two_books_fed_the_same_events_have_identical_canonical_bytes() {
    let mut events = deep_book_events();
    events.extend([
        bid(1000, 0),
        ask(1001, 55),
        window(985, 1015),
        bid(990, 3),
        begin(2),
        bid(999, 4),
    ]);
    let mut a = Books::new();
    let mut b = Books::new();
    feed(&mut a, &events);
    feed(&mut b, &events);
    assert_eq!(book(&a).canonical_bytes(), book(&b).canonical_bytes());
    assert_eq!(book(&a), book(&b));

    // One lot more at one level changes them, live or in the snapshot in progress.
    let mut c = Books::new();
    feed(&mut c, &events);
    c.apply(&ask(1001, 56)).unwrap(); // goes to the snapshot in progress
    assert_ne!(book(&a).canonical_bytes(), book(&c).canonical_bytes());

    let mut one_lot = events.clone();
    let i = events.iter().position(|e| *e == ask(1001, 55)).unwrap();
    one_lot[i] = ask(1001, 56); // the live book's best ask
    let mut d = Books::new();
    feed(&mut d, &one_lot);
    assert_ne!(book(&a).canonical_bytes(), book(&d).canonical_bytes());
}

#[test]
fn canonical_bytes_tell_every_state_apart() {
    let awaiting = L2Book::new();
    let mut gapped = L2Book::new();
    gapped.gap();
    let mut valid = L2Book::new();
    valid.begin_snapshot(0);
    valid.end_snapshot().unwrap();
    let mut valid_e1 = L2Book::new();
    valid_e1.begin_snapshot(1);
    valid_e1.end_snapshot().unwrap();
    let mut windowed = valid.clone();
    windowed.set_window(Ticks(0), Ticks(1)).unwrap();
    let mut pending = valid.clone();
    pending.begin_snapshot(0);
    let all = [awaiting, gapped, valid, valid_e1, windowed, pending];
    for (i, x) in all.iter().enumerate() {
        for y in &all[i + 1..] {
            assert_ne!(x.canonical_bytes(), y.canonical_bytes(), "{x:?} vs {y:?}");
        }
    }
    assert_eq!(L2Book::default(), L2Book::new());
}

#[test]
fn canonical_bytes_have_a_fixed_layout() {
    let mut b = L2Book::new();
    b.begin_snapshot(0x0102_0304);
    b.set_level(BookSide::Bid, Ticks(-2), lots(3));
    b.set_level(BookSide::Ask, Ticks(5), lots(1));
    b.end_snapshot().unwrap();
    b.set_window(Ticks(-10), Ticks(10)).unwrap();
    let mut want = Vec::new();
    want.extend_from_slice(b"FBCB");
    want.push(1); // format version
    want.push(1); // valid
    want.extend_from_slice(&0x0102_0304u32.to_le_bytes());
    want.push(1); // windowed
    want.extend_from_slice(&(-10i64).to_le_bytes());
    want.extend_from_slice(&10i64.to_le_bytes());
    want.extend_from_slice(&1u32.to_le_bytes()); // one bid
    want.extend_from_slice(&(-2i64).to_le_bytes());
    want.extend_from_slice(&3i64.to_le_bytes());
    want.extend_from_slice(&1u32.to_le_bytes()); // one ask
    want.extend_from_slice(&5i64.to_le_bytes());
    want.extend_from_slice(&1i64.to_le_bytes());
    want.push(0); // no snapshot in progress
    assert_eq!(b.canonical_bytes(), want);
}

#[test]
fn ending_a_snapshot_that_never_began_is_an_error() {
    let mut books = Books::new();
    let err = books.apply(&end()).unwrap_err();
    assert_eq!(err, BookError::NoSnapshotInProgress);
    assert_eq!(
        err.to_string(),
        "book snapshot end without a snapshot in progress"
    );
    assert_eq!(book(&books).state(), BookState::AwaitingSnapshot);
}

#[test]
fn errors_say_what_went_wrong() {
    assert_eq!(
        BookError::NotValid(BookState::Gapped).to_string(),
        "book is not valid: Gapped"
    );
    assert_eq!(
        BookError::InvertedWindow {
            lo: Ticks(3),
            hi: Ticks(2),
        }
        .to_string(),
        "book window 3..=2 is inverted"
    );
    let e: &dyn std::error::Error = &BookError::NoSnapshotInProgress;
    assert!(e.source().is_none());
}

#[test]
fn events_that_are_not_about_a_book_change_nothing() {
    let mut books = Books::new();
    let trade = MdEvent::Trade {
        inst: INST,
        id: None,
        aggressor: fbc_core::Aggressor::Buyer,
        px: Ticks(1),
        qty: lots(1),
    };
    let touch = MdEvent::Touch {
        inst: INST,
        bid: None,
        ask: None,
        source: fbc_core::TouchSourceId(0),
    };
    for ev in [trade, touch] {
        assert_eq!(books.apply(&ev), Ok(Applied::Unchanged));
    }
    assert!(books.get(INST, BOOK).is_none());
}

// ---------------------------------------------------------------------------------------------
// FBC-n35m: a prune or a clear of one side (decision 0124).
// ---------------------------------------------------------------------------------------------

/// Bids 95x1, 98x2, 100x3 against asks 101x4, 103x5, 106x6 under epoch 1.
fn wide_snapshot() -> Vec<MdEvent> {
    vec![
        begin(1),
        bid(95, 1),
        bid(98, 2),
        bid(100, 3),
        ask(101, 4),
        ask(103, 5),
        ask(106, 6),
        end(),
    ]
}

#[test]
fn a_side_prune_removes_only_that_sides_levels_outside_its_range_and_keeps_the_book_valid() {
    let mut books = Books::new();
    feed(&mut books, &wide_snapshot());
    assert_eq!(
        books.apply(&prune(BookSide::Bid, 97, 99)),
        Ok(Applied::Changed)
    );
    // Only the bids outside 97..=99 went; every ask stays, those outside the range included.
    assert_eq!(
        top_all(&books),
        Top {
            bids: vec![lvl(98, 2)],
            asks: vec![lvl(101, 4), lvl(103, 5), lvl(106, 6)],
        }
    );
    assert_eq!(book(&books).state(), BookState::Valid { epoch: 1 });
    // It keeps no range: the book has no window.
    assert_eq!(book(&books).window(), None);

    // The asks' side prunes the asks alone.
    assert_eq!(
        books.apply(&prune(BookSide::Ask, 103, 200)),
        Ok(Applied::Changed)
    );
    assert_eq!(
        top_all(&books),
        Top {
            bids: vec![lvl(98, 2)],
            asks: vec![lvl(103, 5), lvl(106, 6)],
        }
    );

    // A later both-sides window still bounds both sides, as it always has.
    assert_eq!(books.apply(&window(97, 104)), Ok(Applied::Changed));
    assert_eq!(
        book(&books).window(),
        Some(Window::new(Ticks(97), Ticks(104)).unwrap())
    );
    assert_eq!(
        top_all(&books),
        Top {
            bids: vec![lvl(98, 2)],
            asks: vec![lvl(103, 5)],
        }
    );
}

#[test]
fn a_side_clear_empties_only_that_side_and_leaves_the_book_valid_with_no_touch_there() {
    let mut books = Books::new();
    feed(&mut books, &small_snapshot());
    assert_eq!(books.apply(&clear(BookSide::Ask)), Ok(Applied::Changed));
    assert_eq!(
        book(&books).touch(),
        Ok(Touch {
            bid: Some(lvl(101, 3)),
            ask: None,
        })
    );
    assert_eq!(
        top_all(&books),
        Top {
            bids: vec![lvl(101, 3), lvl(100, 2)],
            asks: vec![],
        }
    );
    // Valid, with no gap: a cleared side is a routine state of a valid book.
    assert_eq!(book(&books).state(), BookState::Valid { epoch: 1 });
    assert_eq!(book(&books).window(), None);

    // A level set on the cleared side later shows as any delta does.
    assert_eq!(books.apply(&ask(104, 1)), Ok(Applied::Changed));
    assert_eq!(
        book(&books).touch(),
        Ok(Touch {
            bid: Some(lvl(101, 3)),
            ask: Some(lvl(104, 1)),
        })
    );

    // A book that is not valid, with no snapshot in progress, takes neither, as it takes no
    // delta.
    let mut gapped = Books::new();
    feed(&mut gapped, &small_snapshot());
    feed(&mut gapped, &[gap()]);
    let mut awaiting = Books::new();
    awaiting.apply(&bid(1, 1)).unwrap();
    for books in [&mut gapped, &mut awaiting] {
        let before = book(books).canonical_bytes();
        let state = book(books).state();
        assert!(!matches!(state, BookState::Valid { .. }));
        assert_eq!(
            books.apply(&clear(BookSide::Ask)),
            Ok(Applied::DroppedNotValid)
        );
        assert_eq!(
            books.apply(&prune(BookSide::Bid, 0, 1)),
            Ok(Applied::DroppedNotValid)
        );
        assert_eq!(book(books).canonical_bytes(), before);
        assert_eq!(book(books).state(), state);

        // With a snapshot in progress, the replacement takes them.
        books.apply(&begin(1)).unwrap();
        assert_eq!(books.apply(&clear(BookSide::Ask)), Ok(Applied::Changed));
        assert_eq!(
            books.apply(&prune(BookSide::Bid, 0, 1)),
            Ok(Applied::Changed)
        );
        assert_eq!(book(books).state(), state);
    }
}

#[test]
fn a_side_prune_or_clear_in_a_snapshot_in_progress_changes_only_that_side_of_the_replacement() {
    let mut books = Books::new();
    feed(&mut books, &small_snapshot());
    let old = top_all(&books);
    feed(
        &mut books,
        &[
            begin(2),
            bid(90, 1),
            bid(95, 2),
            ask(103, 3),
            ask(110, 4),
            prune(BookSide::Bid, 94, 96),
        ],
    );
    // As a delta in a snapshot does, it acts on the replacement only: the book it replaces
    // reads as it did until the snapshot ends.
    assert_eq!(top_all(&books), old);
    assert_eq!(book(&books).state(), BookState::Valid { epoch: 1 });
    assert_eq!(books.apply(&end()), Ok(Applied::Changed));
    assert_eq!(book(&books).state(), BookState::Valid { epoch: 2 });
    // It bounds the replacement's bids only: its asks, all outside 94..=96, stay.
    assert_eq!(
        top_all(&books),
        Top {
            bids: vec![lvl(95, 2)],
            asks: vec![lvl(103, 3), lvl(110, 4)],
        }
    );

    // A clear of the asks in the next snapshot empties the replacement's asks only.
    let old = top_all(&books);
    feed(
        &mut books,
        &[
            begin(3),
            bid(90, 1),
            bid(95, 2),
            ask(103, 3),
            clear(BookSide::Ask),
        ],
    );
    assert_eq!(top_all(&books), old);
    assert_eq!(books.apply(&end()), Ok(Applied::Changed));
    assert_eq!(book(&books).state(), BookState::Valid { epoch: 3 });
    assert_eq!(
        top_all(&books),
        Top {
            bids: vec![lvl(95, 2), lvl(90, 1)],
            asks: vec![],
        }
    );

    // A prune before any level of a snapshot keeps no range: the snapshot's later levels stay,
    // wherever they lie.
    feed(
        &mut books,
        &[
            begin(4),
            prune(BookSide::Bid, 0, 0),
            clear(BookSide::Ask),
            bid(50, 1),
            ask(60, 1),
            end(),
        ],
    );
    assert_eq!(
        top_all(&books),
        Top {
            bids: vec![lvl(50, 1)],
            asks: vec![lvl(60, 1)],
        }
    );
}

#[test]
fn an_inverted_side_prune_is_refused_in_every_state_and_changes_nothing() {
    let inverted = Err(BookError::InvertedWindow {
        lo: Ticks(105),
        hi: Ticks(95),
    });
    let refused = |books: &mut Books| {
        let before = book(books).canonical_bytes();
        let window = book(books).window();
        for side in [BookSide::Bid, BookSide::Ask] {
            assert_eq!(books.apply(&prune(side, 105, 95)), inverted);
        }
        assert_eq!(book(books).canonical_bytes(), before);
        assert_eq!(book(books).window(), window);
    };
    let mut books = Books::new();
    // Awaiting its first snapshot.
    books.apply(&bid(1, 1)).unwrap();
    refused(&mut books);
    // Valid, and windowed.
    feed(&mut books, &small_snapshot());
    refused(&mut books);
    feed(&mut books, &[window(90, 110)]);
    refused(&mut books);
    // With a snapshot in progress.
    feed(&mut books, &[begin(2), bid(99, 1)]);
    refused(&mut books);
    // Gapped.
    feed(&mut books, &[gap()]);
    refused(&mut books);
    // A one-tick range is fine.
    feed(&mut books, &small_snapshot());
    assert_eq!(
        books.apply(&prune(BookSide::Bid, 101, 101)),
        Ok(Applied::Changed)
    );
    assert_eq!(top_all(&books).bids, vec![lvl(101, 3)]);
}

#[test]
fn a_pruned_or_cleared_side_reads_as_any_side_its_levels_leave() {
    let mut b = L2Book::new();
    b.begin_snapshot(1);
    for (px, qty) in [(100, 3), (99, 2), (97, 2)] {
        b.set_level(BookSide::Bid, Ticks(px), lots(qty));
    }
    b.set_level(BookSide::Ask, Ticks(101), lots(4));
    b.set_level(BookSide::Ask, Ticks(104), lots(1));
    b.end_snapshot().unwrap();
    let prices = 85..=115;
    let asks = |b: &L2Book| -> Vec<_> {
        prices
            .clone()
            .map(|px| b.level(BookSide::Ask, Ticks(px)))
            .collect()
    };
    let asks_before = asks(&b);

    // The best bid leaves: a price better than the new touch, or between two levels, is empty,
    // one past the deepest level unknown.
    assert_eq!(
        b.prune_side(BookSide::Bid, Ticks(97), Ticks(99)),
        Ok(Applied::Changed)
    );
    assert_eq!(b.level(BookSide::Bid, Ticks(100)), Ok(Some(Lots::ZERO)));
    assert_eq!(b.level(BookSide::Bid, Ticks(99)), Ok(Some(lots(2))));
    assert_eq!(b.level(BookSide::Bid, Ticks(98)), Ok(Some(Lots::ZERO)));
    assert_eq!(b.level(BookSide::Bid, Ticks(97)), Ok(Some(lots(2))));
    assert_eq!(b.level(BookSide::Bid, Ticks(96)), Ok(None));

    // The deepest bid leaves: what lay past the level now deepest is unknown.
    assert_eq!(
        b.prune_side(BookSide::Bid, Ticks(99), Ticks(200)),
        Ok(Applied::Changed)
    );
    assert_eq!(b.level(BookSide::Bid, Ticks(97)), Ok(None));
    assert_eq!(b.level(BookSide::Bid, Ticks(98)), Ok(None));
    assert_eq!(b.level(BookSide::Bid, Ticks(99)), Ok(Some(lots(2))));

    // No range is kept: a level set outside the last prune is kept and reported.
    assert_eq!(
        b.set_level(BookSide::Bid, Ticks(90), lots(1)),
        Applied::Changed
    );
    assert_eq!(b.level(BookSide::Bid, Ticks(90)), Ok(Some(lots(1))));
    assert_eq!(b.level(BookSide::Bid, Ticks(95)), Ok(Some(Lots::ZERO)));
    assert_eq!(b.top(15).unwrap().bids, vec![lvl(99, 2), lvl(90, 1)]);

    // A cleared side is unknown at every price, and the other side reads as it did.
    assert_eq!(b.clear_side(BookSide::Bid), Applied::Changed);
    for px in prices.clone() {
        assert_eq!(b.level(BookSide::Bid, Ticks(px)), Ok(None), "bid {px}");
    }
    assert_eq!(asks(&b), asks_before);
    assert_eq!(
        b.touch(),
        Ok(Touch {
            bid: None,
            ask: Some(lvl(101, 4)),
        })
    );
    assert_eq!(b.state(), BookState::Valid { epoch: 1 });

    // Under a both-sides window the window decides what is known: a clear leaves it, and the
    // cleared side reads empty inside it and unknown outside it.
    let mut windowed = L2Book::new();
    windowed.begin_snapshot(1);
    windowed.set_level(BookSide::Bid, Ticks(100), lots(3));
    windowed.set_level(BookSide::Ask, Ticks(101), lots(4));
    windowed.end_snapshot().unwrap();
    windowed.set_window(Ticks(98), Ticks(102)).unwrap();
    let w = windowed.window();
    assert_eq!(windowed.clear_side(BookSide::Ask), Applied::Changed);
    assert_eq!(windowed.window(), w);
    assert_eq!(
        windowed.level(BookSide::Ask, Ticks(101)),
        Ok(Some(Lots::ZERO))
    );
    assert_eq!(windowed.level(BookSide::Ask, Ticks(104)), Ok(None));
    assert_eq!(windowed.level(BookSide::Bid, Ticks(100)), Ok(Some(lots(3))));

    // A book that is not valid takes neither, and reads as not valid.
    let mut gapped = b.clone();
    gapped.gap();
    assert_eq!(
        gapped.prune_side(BookSide::Ask, Ticks(0), Ticks(1)),
        Ok(Applied::DroppedNotValid)
    );
    assert_eq!(gapped.clear_side(BookSide::Ask), Applied::DroppedNotValid);
    assert_eq!(
        gapped.level(BookSide::Ask, Ticks(101)),
        Err(BookError::NotValid(BookState::Gapped))
    );
}

#[test]
fn side_events_change_the_canonical_bytes_exactly_when_they_remove_a_level() {
    // Bids only: the asks' side is already empty.
    let snapshot = [begin(1), bid(98, 1), bid(99, 2), bid(100, 3), end()];
    let fed = |extra: &[MdEvent]| {
        let mut books = Books::new();
        feed(&mut books, &snapshot);
        feed(&mut books, extra);
        books
    };
    let a = fed(&[]);
    // A prune that removes a level, and a clear.
    let b = fed(&[prune(BookSide::Bid, 99, 100)]);
    let c = fed(&[clear(BookSide::Bid)]);
    // A prune whose range holds every bid, and a clear of the empty asks' side: nothing left.
    let d = fed(&[prune(BookSide::Bid, 90, 110), clear(BookSide::Ask)]);
    let bytes = |books: &Books| book(books).canonical_bytes();
    assert_ne!(bytes(&a), bytes(&b));
    assert_ne!(bytes(&a), bytes(&c));
    assert_ne!(bytes(&b), bytes(&c));
    // Equal bytes, equal books field for field, and equal reads.
    assert_eq!(bytes(&d), bytes(&a));
    assert_eq!(book(&d), book(&a));
    assert_eq!(book(&d).touch(), book(&a).touch());
    assert_eq!(top_all(&d), top_all(&a));
    for side in [BookSide::Bid, BookSide::Ask] {
        for px in 90..=110 {
            assert_eq!(
                book(&d).level(side, Ticks(px)),
                book(&a).level(side, Ticks(px)),
                "{side:?} {px}"
            );
        }
    }
    // The layout keeps its version: a prune or a clear stores nothing of its own.
    for books in [&a, &b, &c, &d] {
        assert_eq!(&bytes(books)[..5], b"FBCB\x01");
    }
    // Inside a snapshot in progress too, the bytes change when the replacement lost a level.
    let e = fed(&[begin(2), bid(10, 1), ask(20, 1)]);
    let f = fed(&[begin(2), bid(10, 1), ask(20, 1), clear(BookSide::Ask)]);
    assert_ne!(bytes(&e), bytes(&f));
    assert_eq!(top_all(&e), top_all(&f), "the replaced book reads alike");
}
