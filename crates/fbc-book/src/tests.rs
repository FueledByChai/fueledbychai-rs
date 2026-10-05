//! FBC-qfi's done line: a complete snapshot replaces the book; deltas set and remove levels; a
//! gap invalidates the book until the next complete snapshot; a windowed book reports no level
//! outside its window; touch() and top(15) match a hand-built book; the top-n comparison names
//! a planted difference; and two books fed the same events have identical canonical bytes,
//! while a one-lot difference changes them. The error paths are here too.

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
fn one_level_reads_its_size_zero_when_empty_and_unknown_outside_the_window() {
    let mut fresh = L2Book::new();
    assert_eq!(
        fresh.level(BookSide::Bid, Ticks(100)),
        Err(BookError::NotValid(BookState::AwaitingSnapshot))
    );
    fresh.begin_snapshot(1);
    fresh.set_level(BookSide::Bid, Ticks(100), lots(3));
    fresh.set_level(BookSide::Ask, Ticks(101), lots(4));
    fresh.end_snapshot().unwrap();
    assert_eq!(fresh.level(BookSide::Bid, Ticks(100)), Ok(Some(lots(3))));
    assert_eq!(fresh.level(BookSide::Ask, Ticks(101)), Ok(Some(lots(4))));
    // The other side at the same price, and a price with no level, are empty.
    assert_eq!(fresh.level(BookSide::Ask, Ticks(100)), Ok(Some(Lots::ZERO)));
    assert_eq!(fresh.level(BookSide::Bid, Ticks(99)), Ok(Some(Lots::ZERO)));
    // Outside a window a level is unknown, not empty.
    fresh.set_window(Ticks(100), Ticks(100)).unwrap();
    assert_eq!(fresh.level(BookSide::Bid, Ticks(100)), Ok(Some(lots(3))));
    assert_eq!(fresh.level(BookSide::Ask, Ticks(101)), Ok(None));
    fresh.gap();
    assert_eq!(
        fresh.level(BookSide::Bid, Ticks(100)),
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
