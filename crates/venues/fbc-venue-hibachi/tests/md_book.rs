//! FBC-2kmp's done line, the orderbook: Hibachi's `orderbook` frames, hand-built under
//! `fixtures/hibachi/md/`, decode through the side-aware window and one-side clear (decision
//! 0124) into a book a consumer can trust as far as the feed allows; every frame is applied to
//! the codec's own copy and bounds-checked before any of its events is pushed, and a frame that
//! shows the book may be wrong gaps it, counted, and is recovered by the codec's own resubscribe
//! and then a reconnect (decision 0129).

mod common;

use fbc_book::BookState;
use fbc_core::{
    BookSide, Effect, Feed, FeedHealth, MdEvent, OpKind, RateCharge, StreamId, TimerTag, VenueMeta,
    WireSlice,
};
use fbc_venue_hibachi::BOOK_ORDERBOOK;

use common::{Driver, ETH, SEI, SOL, book_gap, book_sub, eth_lvl, frame, level, prune, sub};

use BookSide::{Ask, Bid};

fn eth() -> Driver {
    let mut d = Driver::new(&[book_sub(ETH)]);
    d.take_fx();
    d
}

/// The book's levels best first, each `(hundredths, thousandths)`.
/// One side's levels best first, each `(hundredths, thousandths)`.
type Side = Vec<(i64, i64)>;

fn sides(d: &Driver) -> (Side, Side) {
    let top = d.book(ETH).top(10).unwrap();
    let read = |lvls: Vec<fbc_core::Lvl>| {
        lvls.into_iter()
            .map(|l| (l.px.0 / 10_000, l.qty.get() / 1_000_000))
            .collect()
    };
    (read(top.bids), read(top.asks))
}

/// The one timer asked for.
fn one(tags: Vec<TimerTag>) -> TimerTag {
    assert_eq!(tags.len(), 1, "{tags:?}");
    tags[0]
}

const S0_BIDS: [(i64, i64); 3] = [(200_000, 1_250), (199_990, 3_000), (199_980, 500)];
const S0_ASKS: [(i64, i64); 3] = [(200_010, 1_500), (200_020, 2_000), (200_030, 750)];

fn send(text: &str) -> Effect {
    Effect::Send {
        stream: StreamId(0),
        frame: WireSlice::plain(text.as_bytes().to_vec()),
        rpc: None,
        class: fbc_core::TrafficClass::Normal,
        charge: RateCharge::one(OpKind::Subscribe, Some(ETH)),
    }
}

const ETH_UNSUBSCRIBE: &str = r#"{"method":"unsubscribe","parameters":{"subscriptions":[{"symbol":"ETH/USDT-P","topic":"orderbook"}]}}"#;
const ETH_SUBSCRIBE: &str = r#"{"method":"subscribe","parameters":{"subscriptions":[{"depth":3,"granularity":"0.01","symbol":"ETH/USDT-P","topic":"orderbook"}]}}"#;

#[test]
fn a_snapshot_then_updates_apply_absolute_sizes_and_a_zero_quantity_removes_a_level() {
    let mut d = eth();
    let s0 = d.feed("eth_s0");
    let begin = MdEvent::BookSnapshotBegin {
        inst: ETH,
        book: BOOK_ORDERBOOK,
        epoch: 1,
    };
    let end = MdEvent::BookSnapshotEnd {
        inst: ETH,
        book: BOOK_ORDERBOOK,
    };
    assert_eq!(
        s0,
        [
            begin,
            level(Bid, 199_980, 500),
            level(Bid, 199_990, 3_000),
            level(Bid, 200_000, 1_250),
            level(Ask, 200_010, 1_500),
            level(Ask, 200_020, 2_000),
            level(Ask, 200_030, 750),
            end,
        ]
    );
    assert_eq!(d.state(ETH), BookState::Valid { epoch: 1 });
    assert_eq!(sides(&d), (S0_BIDS.to_vec(), S0_ASKS.to_vec()));

    // U1: the zero at 2000.10 removes that level, the scroll-in at 2000.40 is set.
    d.feed("eth_u1");
    assert_eq!(
        sides(&d),
        (
            S0_BIDS.to_vec(),
            vec![(200_020, 2_000), (200_030, 750), (200_040, 1_100)]
        )
    );
    d.feed("eth_u2");
    d.feed("eth_u3");
    assert_eq!(
        sides(&d),
        (
            vec![(200_000, 1_250), (199_990, 3_000), (199_985, 900)],
            vec![(200_002, 600), (200_020, 2_000), (200_030, 750)]
        )
    );
    assert_eq!(d.counts().bounds_failures, 0);
    assert!(
        d.take_fx().is_empty(),
        "nothing asked for while the book is valid"
    );
}

#[test]
fn the_window_removes_a_stale_level_before_the_listed_levels_and_it_never_becomes_the_touch() {
    let mut d = eth();
    d.feed("eth_s0");
    // U1's ask window starts at 2000.20: the resting 2000.10, which the frame zeroes, is gone
    // and the touch is 2000.20, never the stale level. The zero empties a level better than the
    // range's best end, so it is a zero Level before the ask's prune (0124 item 6); every
    // removal comes before the level the frame sets.
    let u1 = d.feed("eth_u1");
    assert_eq!(
        u1,
        [
            prune(Bid, 199_980, 200_000),
            level(Ask, 200_010, 0),
            prune(Ask, 200_020, 200_040),
            level(Ask, 200_040, 1_100),
        ]
    );
    let touch = d.book(ETH).touch().unwrap();
    assert_eq!(touch.ask, Some(eth_lvl(200_020, 2_000)));
    assert_eq!(touch.bid, Some(eth_lvl(200_000, 1_250)));
}

#[test]
fn a_level_the_window_removes_that_the_frame_does_not_zero_gaps_the_book() {
    let mut d = eth();
    d.feed("eth_s0");
    // The same window without the zero: 2000.10 would go unstated, what a lost frame leaves.
    assert_eq!(d.feed("eth_u1_without_zero"), [book_gap(ETH)]);
    assert_eq!(d.counts().bounds_failures, 1);
    assert!(!d.valid(ETH));
    assert!(d.book(ETH).touch().is_err(), "no touch on a gapped book");
}

#[test]
fn an_update_with_empty_levels_and_unchanged_bounds_changes_nothing() {
    let mut d = eth();
    d.feed("eth_s0");
    let before = d.book(ETH).canonical_bytes();
    let events = d.feed("eth_unchanged");
    // Each side's window, which removes nothing: never a both-sides Window, never a zero level.
    assert_eq!(
        events,
        [prune(Bid, 199_980, 200_000), prune(Ask, 200_010, 200_030)]
    );
    assert_eq!(d.book(ETH).canonical_bytes(), before);
    assert_eq!(d.counts(), Default::default());
}

#[test]
fn a_null_bid_beside_ask_changes_clears_the_bid_and_leaves_the_book_valid_with_no_bid_touch() {
    let mut d = eth();
    d.feed("eth_s0");
    let events = d.feed("eth_null_bid");
    assert_eq!(
        events,
        [
            MdEvent::SideClear {
                inst: ETH,
                book: BOOK_ORDERBOOK,
                side: Bid,
            },
            prune(Ask, 200_010, 200_030),
            level(Ask, 200_020, 1_800),
        ]
    );
    assert_eq!(d.counts().null_sides, 1);
    assert_eq!(d.state(ETH), BookState::Valid { epoch: 1 });
    let touch = d.book(ETH).touch().unwrap();
    assert_eq!(touch.bid, None);
    assert_eq!(touch.ask, Some(eth_lvl(200_010, 1_500)));
    // The ask side, carrying bounds, was checked: its three levels sit within them.
    assert_eq!(
        sides(&d).1,
        vec![(200_010, 1_500), (200_020, 1_800), (200_030, 750)]
    );
    // No gap and no resubscribe.
    assert!(d.take_fx().is_empty());
}

#[test]
fn a_side_cleared_by_null_stays_empty_and_unchecked_until_a_frame_sends_it_with_both_bounds() {
    let mut d = eth();
    d.feed("eth_s0");
    d.feed("eth_null_bid");
    // Null again: still empty, still valid, counted again.
    d.feed("eth_null_bid");
    assert_eq!(d.book(ETH).touch().unwrap().bid, None);
    assert_eq!(d.counts().null_sides, 2);
    // Sent with both bounds: restored, and checked from that frame on.
    let restore = d.feed("eth_bid_restore");
    assert!(!restore.contains(&book_gap(ETH)));
    assert_eq!(
        sides(&d).0,
        vec![(200_000, 1_000), (199_990, 2_000), (199_980, 500)]
    );
    d.feed("eth_null_bid");
    // Bounds with fewer levels than the depth: the side is checked, and fails as thin.
    assert_eq!(d.feed("eth_bid_restore_thin"), [book_gap(ETH)]);
    assert_eq!(d.counts().thin_sides, 1);
}

#[test]
fn a_snapshot_with_one_side_null_and_the_other_passing_makes_the_book_valid_that_side_empty() {
    let mut d = eth();
    let events = d.feed("eth_snapshot_null_bid");
    assert!(!events.contains(&book_gap(ETH)));
    assert_eq!(d.state(ETH), BookState::Valid { epoch: 1 });
    assert_eq!(sides(&d), (vec![], S0_ASKS.to_vec()));
    assert_eq!(d.counts().null_sides, 1);
}

#[test]
fn a_malformed_update_gaps_the_book_and_pushes_nothing_else_until_a_snapshot() {
    for name in [
        "eth_absent_side",
        "eth_levels_no_bound",
        "eth_empty_one_bound",
        "eth_empty_no_bound",
    ] {
        let mut d = eth();
        d.feed("eth_s0");
        // Never read as unchanged: the gap, and nothing else of the frame.
        assert_eq!(d.feed(name), [book_gap(ETH)], "{name}");
        assert_eq!(d.counts().malformed_books, 1, "{name}");
        assert!(!d.valid(ETH), "{name}");
        // Nothing of that book until a Snapshot: an Update is passed over.
        assert!(d.feed("eth_u1").is_empty(), "{name}");
        assert_eq!(d.counts().updates_without_book, 1, "{name}");
        // The retry timer was armed: the resubscribe follows (see the timer tests).
        assert_eq!(d.timers().len(), 1, "{name}");
        // A Snapshot with a level that passes makes it valid again.
        d.feed("eth_s0");
        assert_eq!(d.state(ETH), BookState::Valid { epoch: 2 }, "{name}");
    }
}

#[test]
fn a_downward_bid_window_prunes_by_min_max_and_its_stated_scroll_outs_raise_nothing() {
    let mut d = eth();
    d.feed("eth_s0");
    d.feed("eth_u1");
    // U2's bid window runs down from 2000.05 to 1999.90; its zero for 1999.80 lies past that
    // deep end, a level the prune removes (it may only have scrolled out): not emitted, and no
    // error (0124 item 6).
    let u2 = d.feed("eth_u2");
    assert_eq!(
        u2,
        [
            prune(Bid, 199_990, 200_005),
            prune(Ask, 200_020, 200_040),
            level(Bid, 200_005, 400),
        ]
    );
    assert_eq!(
        sides(&d).0,
        vec![(200_005, 400), (200_000, 1_250), (199_990, 3_000)]
    );
    assert_eq!(d.counts(), Default::default());
}

#[test]
fn a_frame_one_side_of_which_alone_would_cross_emits_every_removal_of_both_sides_first() {
    let mut d = eth();
    for name in ["eth_s0", "eth_u1", "eth_u2"] {
        d.feed(name);
    }
    let mut books = d.books.clone();
    let u3 = d.feed("eth_u3");
    assert_eq!(
        u3,
        [
            // The bid's zero at 2000.05, better than its new best end: emptied, before its prune.
            level(Bid, 200_005, 0),
            prune(Bid, 199_985, 200_000),
            // The ask's zero at 2000.40 lies past its deep end: not emitted.
            prune(Ask, 200_002, 200_030),
            level(Bid, 199_985, 900),
            level(Ask, 200_002, 600),
        ]
    );
    // Applied one event at a time, as MdBooks does, the book is never crossed.
    for ev in &u3 {
        books.apply(ev).unwrap();
        let touch = books.get(ETH, BOOK_ORDERBOOK).unwrap().touch().unwrap();
        if let (Some(bid), Some(ask)) = (touch.bid, touch.ask) {
            assert!(bid.px < ask.px, "crossed after {ev:?}");
        }
    }
}

#[test]
fn an_update_arriving_with_no_book_is_ignored() {
    let mut d = eth();
    // Before any Snapshot.
    assert!(d.feed("eth_unchanged").is_empty());
    assert_eq!(d.counts().updates_without_book, 1);
    assert_eq!(d.state(ETH), BookState::AwaitingSnapshot);
    // After the subscription is removed, its frames are passed over too.
    d.feed("eth_s0");
    d.remove(&[book_sub(ETH)]);
    assert!(d.feed("eth_unchanged").is_empty());
    assert_eq!(d.counts().unsubscribed_frames, 1);
}

#[test]
fn the_bounds_check_runs_before_any_event_and_catches_a_dropped_frame() {
    let mut d = eth();
    d.feed("eth_s0");
    // U1 lost: U2's ask window (2000.20..2000.40) would remove 2000.10, which it does not zero.
    // Nothing of the frame is pushed but the gap: not its bid's prune or its bid's level.
    assert_eq!(d.feed("eth_u2"), [book_gap(ETH)]);
    assert_eq!(d.counts().bounds_failures, 1);
    assert!(!d.valid(ETH));
    // The other instrument's book, on its own subscription, is untouched by it.
    let mut both = Driver::new(&[book_sub(ETH), book_sub(SOL)]);
    both.feed("eth_s0");
    both.feed("sol_s0");
    both.feed("eth_u2");
    assert!(!both.valid(ETH));
    assert_eq!(both.state(SOL), BookState::Valid { epoch: 1 });
}

#[test]
fn a_side_thinner_than_depth_fails_the_check_and_is_counted_apart() {
    let mut d = eth();
    assert_eq!(d.feed("eth_snapshot_thin"), [book_gap(ETH)]);
    let counts = d.counts();
    assert_eq!((counts.thin_sides, counts.bounds_failures), (1, 0));
    assert!(!d.valid(ETH));
}

#[test]
fn a_snapshot_with_an_absent_or_unbounded_side_never_makes_the_book_valid() {
    for name in ["eth_snapshot_absent_side", "eth_snapshot_unbounded_side"] {
        let mut d = eth();
        assert_eq!(d.feed(name), [book_gap(ETH)], "{name}");
        assert_eq!(d.counts().malformed_books, 1, "{name}");
        assert!(!d.valid(ETH), "{name}");
    }
}

#[test]
fn a_snapshot_with_no_level_on_either_side_reports_a_gap_and_no_level_or_window() {
    for (name, empty, malformed) in [
        ("eth_snapshot_empty", 1, 0),
        ("eth_snapshot_empty_levels", 1, 0),
        ("eth_snapshot_empty_data", 0, 1),
    ] {
        let mut d = eth();
        assert_eq!(d.feed(name), [book_gap(ETH)], "{name}");
        let counts = d.counts();
        assert_eq!(
            (counts.empty_snapshots, counts.malformed_books),
            (empty, malformed),
            "{name}"
        );
        // Not valid through the Updates after it.
        assert!(d.feed("eth_unchanged").is_empty(), "{name}");
        assert!(d.feed("eth_u1").is_empty(), "{name}");
        assert!(!d.valid(ETH), "{name}");
    }
    // Once valid, an empty Snapshot gaps the book too.
    let mut d = eth();
    d.feed("eth_s0");
    assert_eq!(d.feed("eth_snapshot_empty"), [book_gap(ETH)]);
    assert!(!d.valid(ETH));
}

#[test]
fn a_locked_book_passes_counted_apart_and_a_crossed_one_fails() {
    let mut d = eth();
    let locked = d.feed("eth_snapshot_locked");
    assert!(!locked.contains(&book_gap(ETH)));
    assert_eq!(d.state(ETH), BookState::Valid { epoch: 1 });
    let counts = d.counts();
    assert_eq!((counts.locked_books, counts.bounds_failures), (1, 0));

    let mut d = eth();
    assert_eq!(d.feed("eth_snapshot_crossed"), [book_gap(ETH)]);
    assert_eq!(d.counts().bounds_failures, 1);
    assert!(!d.valid(ETH));
}

#[test]
fn a_frame_whose_granularity_or_depth_differs_from_its_subscriptions_or_lacks_either_gaps() {
    for name in [
        "eth_granularity_differs",
        "eth_depth_differs",
        "eth_granularity_missing",
        "eth_depth_missing",
    ] {
        let mut d = eth();
        d.feed("eth_s0");
        assert_eq!(d.feed(name), [book_gap(ETH)], "{name}");
        assert_eq!(d.counts().param_mismatches, 1, "{name}");
        assert!(!d.valid(ETH), "{name}");
    }
    // An Update is held to them too: `eth_u1`, which passes as sent, with each changed.
    let u1 = frame("eth_u1");
    for (what, from, to) in [
        (
            "granularity",
            r#""granularity":"0.01""#,
            r#""granularity":"0.1""#,
        ),
        ("depth", r#""depth":3"#, r#""depth":5"#),
        ("no granularity", r#","granularity":"0.01""#, ""),
        ("no depth", r#","depth":3"#, ""),
    ] {
        let mut d = eth();
        d.feed("eth_s0");
        let text = u1.replacen(from, to, 1);
        assert_ne!(text, u1, "{what}");
        assert_eq!(
            d.try_text(&text).unwrap(),
            [(VenueMeta::NONE, book_gap(ETH))],
            "{what}"
        );
        assert_eq!(d.counts().param_mismatches, 1, "{what}");
        assert!(!d.valid(ETH), "{what}");
    }
    // The granularity is compared by value: "0.010" is the subscription's 0.01.
    let mut d = eth();
    let text = frame("eth_s0").replace(r#""granularity":"0.01""#, r#""granularity":"0.010""#);
    d.try_text(&text).unwrap();
    assert_eq!(d.state(ETH), BookState::Valid { epoch: 1 });
    // A number is not the string the docs write.
    let text = frame("eth_s0").replace(r#""granularity":"0.01""#, r#""granularity":0.01"#);
    d.try_text(&text).unwrap();
    assert_eq!(d.counts().param_mismatches, 1);
}

#[test]
fn a_level_off_the_subscription_granularity_is_counted_and_gaps_the_book() {
    // ETH, subscribed at its quote grid 0.01: a level at 1999.905 is on the tick but off it.
    let mut d = eth();
    assert_eq!(d.feed("eth_off_granularity"), [book_gap(ETH)]);
    assert_eq!(d.counts().off_granularity, 1);
    // An Update is held to it too: one that would pass the bounds check but sets an ask at
    // 2000.105, on the tick and off the granularity.
    let mut d = eth();
    d.feed("eth_s0");
    assert_eq!(d.feed("eth_u_off_granularity"), [book_gap(ETH)]);
    let counts = d.counts();
    assert_eq!((counts.off_granularity, counts.bounds_failures), (1, 0));
    assert!(!d.valid(ETH));
    // SEI lists no granularity at or above its 0.01 tick, so it is subscribed on the tick; its
    // book is trusted only while each level sits on it.
    let mut d = Driver::new(&[book_sub(SEI)]);
    let sent = d.take_fx();
    let Effect::Send { frame, .. } = &sent[0] else {
        panic!("{sent:?}")
    };
    let text = std::str::from_utf8(frame.bytes()).unwrap();
    assert!(text.contains(r#""granularity":"0.01","symbol":"SEI/USDT-P""#));
    d.feed("sei_s0");
    assert_eq!(d.state(SEI), BookState::Valid { epoch: 1 });
    let gap = MdEvent::Health {
        inst: SEI,
        feed: Feed::Book(BOOK_ORDERBOOK),
        h: FeedHealth::Gap,
    };
    assert_eq!(d.feed("sei_off_tick"), [gap]);
    assert_eq!(d.counts().off_granularity, 1);
    assert!(!d.valid(SEI));
}

#[test]
fn a_trade_printing_inside_the_book_changes_no_book_state_and_holds_no_update() {
    let mut with_trade = Driver::new(&[book_sub(ETH), sub(ETH, Feed::Trades)]);
    let mut without = eth();
    with_trade.feed("eth_s0");
    without.feed("eth_s0");
    let before = with_trade.book(ETH).canonical_bytes();
    // 2000.051234, strictly inside the 2000.00 / 2000.10 touch.
    let trade = with_trade.feed("trade_single");
    assert!(matches!(trade.as_slice(), [MdEvent::Trade { .. }]));
    assert_eq!(with_trade.book(ETH).canonical_bytes(), before);
    // The Update after it is applied as if the trade had not printed.
    let u1 = with_trade.feed("eth_u1");
    assert_eq!(u1, without.feed("eth_u1"));
    assert_eq!(
        with_trade.book(ETH).canonical_bytes(),
        without.book(ETH).canonical_bytes()
    );
    assert_eq!(with_trade.counts(), without.counts());
}

#[test]
fn a_gap_resubscribes_after_the_interval_with_the_same_granularity_and_depth_then_reconnects() {
    let mut d = eth();
    d.feed("eth_s0");
    d.feed("eth_u2");
    let retry = one(d.timers());
    // After the interval: an unsubscribe and then a subscribe of ETH's orderbook, the same
    // granularity and depth, each charged one subscription of ETH (0018).
    assert!(d.fire(retry).is_empty());
    let wait = one(d.timers());
    assert_eq!(d.take_fx(), [send(ETH_UNSUBSCRIBE), send(ETH_SUBSCRIBE)]);
    assert_eq!(d.counts().resubscribes, 1);
    // The retry's tag is spent: firing it again does nothing.
    d.fire(retry);
    assert!(d.take_fx().is_empty());
    // No Snapshot within the interval: one reconnect, once.
    d.fire(wait);
    assert_eq!(
        d.take_fx(),
        [Effect::Reconnect {
            stream: StreamId(0),
            reason: "no Hibachi orderbook Snapshot answered a resubscribe",
        }]
    );
    d.fire(wait);
    assert!(d.take_fx().is_empty());
    assert_eq!(d.counts().reconnects, 1);
}

#[test]
fn books_whose_resubscribes_all_go_unanswered_ask_for_one_reconnect() {
    let mut d = Driver::new(&[book_sub(ETH), book_sub(SOL)]);
    let first = d.timers();
    assert_eq!(first.len(), 2);
    d.take_fx();
    // Neither first Snapshot comes: each book is gapped and resubscribed.
    for tag in first {
        d.fire(tag);
    }
    let waits = d.timers();
    assert_eq!(waits.len(), 2);
    d.take_fx();
    assert_eq!(d.counts().resubscribes, 2);
    // Neither resubscribe is answered: one reconnect for the connection, not one per book.
    for tag in waits {
        d.fire(tag);
    }
    let fx = d.take_fx();
    assert!(
        matches!(fx.as_slice(), [Effect::Reconnect { .. }]),
        "{fx:?}"
    );
    assert_eq!(d.counts().reconnects, 1);
}

#[test]
fn an_orderbook_frame_of_no_known_kind_leaves_a_wait_for_a_snapshot_standing() {
    let heartbeat =
        frame("eth_unchanged").replace(r#""messageType":"Update""#, r#""messageType":"Heartbeat""#);
    // Waiting for the Snapshot a resubscribe asked for: each such frame is counted and reported,
    // and the wait stands, so its expiry still asks for the reconnect.
    let mut d = eth();
    d.feed("eth_s0");
    d.feed("eth_u2");
    let retry = one(d.timers());
    d.fire(retry);
    let wait = one(d.timers());
    d.take_fx();
    for n in 1..=3 {
        let pushed = d.try_text(&heartbeat).unwrap();
        assert_eq!(pushed, [(VenueMeta::NONE, book_gap(ETH))]);
        assert_eq!(d.counts().malformed_books, n);
        assert!(d.timers().is_empty(), "no retry armed in place of the wait");
    }
    d.fire(wait);
    let fx = d.take_fx();
    assert!(
        matches!(fx.as_slice(), [Effect::Reconnect { .. }]),
        "{fx:?}"
    );
    assert_eq!(d.counts().reconnects, 1);
    // Waiting for a first subscription's Snapshot: the wait stands too, and resubscribes.
    let mut d = Driver::new(&[book_sub(ETH)]);
    let first = one(d.timers());
    d.take_fx();
    d.try_text(&heartbeat).unwrap();
    assert!(d.timers().is_empty());
    assert_eq!(d.fire(first), [book_gap(ETH)]);
    one(d.timers());
    assert_eq!(d.take_fx(), [send(ETH_UNSUBSCRIBE), send(ETH_SUBSCRIBE)]);
    // A Snapshot that comes back failing still gaps the book and arms the next retry.
    assert_eq!(d.feed("eth_snapshot_thin"), [book_gap(ETH)]);
    one(d.timers());
}

#[test]
fn snapshots_coming_back_empty_or_failing_resubscribe_again_at_each_interval() {
    let mut d = eth();
    assert_eq!(d.feed("eth_snapshot_empty"), [book_gap(ETH)]);
    let mut retry = one(d.timers());
    // Each resubscribe answered by a Snapshot that is empty, thin or crossed: the gap again, and
    // the next interval resubscribes again.
    for (n, answer) in [
        "eth_snapshot_empty",
        "eth_snapshot_thin",
        "eth_snapshot_crossed",
    ]
    .into_iter()
    .enumerate()
    {
        assert!(d.fire(retry).is_empty());
        let wait = one(d.timers());
        assert_eq!(d.take_fx(), [send(ETH_UNSUBSCRIBE), send(ETH_SUBSCRIBE)]);
        assert_eq!(d.counts().resubscribes, n as u64 + 1);
        // An Update before the Snapshot (the empty one the capture showed after an
        // unsubscribe) is passed over.
        assert!(d.feed("eth_unchanged").is_empty(), "{answer}");
        assert_eq!(d.feed(answer), [book_gap(ETH)], "{answer}");
        assert!(!d.valid(ETH), "{answer}");
        // Nor is the book valid through the Updates after it.
        assert!(d.feed("eth_unchanged").is_empty(), "{answer}");
        assert!(d.feed("eth_u1").is_empty(), "{answer}");
        assert!(!d.valid(ETH), "{answer}");
        retry = one(d.timers());
        // The wait is stale: no reconnect.
        d.fire(wait);
        assert!(d.take_fx().is_empty(), "{answer}");
    }
    d.fire(retry);
    d.take_fx();
    d.feed("eth_s0");
    assert_eq!(d.state(ETH), BookState::Valid { epoch: 1 });
}

#[test]
fn a_populated_snapshot_after_a_resubscribe_makes_the_book_equal_to_it_and_stale_timers_idle() {
    let mut d = eth();
    d.feed("eth_s0");
    d.feed("eth_u1_without_zero");
    let retry = one(d.timers());
    d.fire(retry);
    let wait = one(d.timers());
    d.take_fx();
    d.feed("eth_s0");
    assert_eq!(d.state(ETH), BookState::Valid { epoch: 2 });
    assert_eq!(sides(&d), (S0_BIDS.to_vec(), S0_ASKS.to_vec()));
    d.feed("eth_u1");
    assert_eq!(
        sides(&d).1,
        vec![(200_020, 2_000), (200_030, 750), (200_040, 1_100)]
    );
    // The wait timer is stale once the Snapshot came: no reconnect.
    assert!(d.fire(wait).is_empty());
    assert!(d.take_fx().is_empty());
}

#[test]
fn a_first_subscription_with_no_snapshot_within_the_interval_is_gapped_and_resubscribed() {
    let mut d = Driver::new(&[book_sub(ETH)]);
    let first = one(d.timers());
    assert_eq!(d.take_fx(), [send(ETH_SUBSCRIBE)]);
    assert_eq!(d.fire(first), [book_gap(ETH)]);
    assert_eq!(d.timers().len(), 1);
    assert_eq!(d.take_fx(), [send(ETH_UNSUBSCRIBE), send(ETH_SUBSCRIBE)]);
}

#[test]
fn a_subscription_the_consumer_removes_is_never_resubscribed() {
    let mut d = eth();
    d.feed("eth_s0");
    d.feed("eth_u2");
    let retry = one(d.timers());
    d.remove(&[book_sub(ETH)]);
    assert_eq!(d.take_fx(), [send(ETH_UNSUBSCRIBE)]);
    assert!(d.fire(retry).is_empty());
    assert!(d.take_fx().is_empty());
    assert_eq!(d.counts().resubscribes, 0);
}

#[test]
fn a_frame_failing_the_check_while_gapped_or_reconnecting_keeps_the_wait_that_stands() {
    let mut d = eth();
    d.feed("eth_s0");
    d.feed("eth_u2");
    assert_eq!(d.timers().len(), 1);
    // Gapped already: another failing Snapshot reports the gap and arms nothing more.
    assert_eq!(d.feed("eth_snapshot_thin"), [book_gap(ETH)]);
    assert!(d.timers().is_empty());
    // Reconnecting: a failing Snapshot arms nothing either.
    let mut d = Driver::new(&[book_sub(ETH)]);
    let first = one(d.timers());
    d.fire(first);
    let wait = one(d.timers());
    d.fire(wait);
    d.take_fx();
    assert_eq!(d.feed("eth_snapshot_empty"), [book_gap(ETH)]);
    assert!(d.take_fx().is_empty());
}

#[test]
fn a_book_frame_with_an_unreadable_part_gaps_the_book() {
    let s0 = frame("eth_s0");
    for (what, from, to) in [
        (
            "messageType",
            r#""messageType":"Snapshot""#,
            r#""messageType":"Delta""#,
        ),
        ("data", r#""data":{"ask""#, r#""data":[],"x":{"ask""#),
        ("side", r#""bid":{"#, r#""bid":7,"x":{"#),
        (
            "levels",
            r#""levels":[{"price":"2000.10""#,
            r#""levels":{},"y":[{"price":"2000.10""#,
        ),
        (
            "level",
            r#"{"price":"2000.10","quantity":"1.500000000"}"#,
            "7",
        ),
        ("price", r#""price":"2000.10""#, r#""price":2000.10"#),
        (
            "quantity",
            r#""quantity":"1.500000000""#,
            r#""quantity":"1.5000000001""#,
        ),
        (
            "negative",
            r#""quantity":"1.500000000""#,
            r#""quantity":"-1.5""#,
        ),
        (
            "bound",
            r#""startPrice":"2000.10""#,
            r#""startPrice":"2000.1000001""#,
        ),
        ("twice", r#""price":"2000.30""#, r#""price":"2000.20""#),
        (
            "timestamp",
            r#""topic":"orderbook""#,
            r#""topic":"orderbook","timestamp_ms":"1""#,
        ),
    ] {
        let mut d = eth();
        let text = s0.replacen(from, to, 1);
        assert_ne!(text, s0, "{what}");
        let pushed = d.try_text(&text).unwrap();
        assert_eq!(pushed, [(VenueMeta::NONE, book_gap(ETH))], "{what}");
        assert_eq!(d.counts().malformed_books, 1, "{what}");
    }
}

#[test]
fn every_orderbook_event_carries_the_frames_timestamp_and_none_where_it_has_none() {
    let mut d = eth();
    let text = frame("eth_s0");
    for (meta, _) in d.try_text(&text).unwrap() {
        assert_eq!(meta.exch_ts, None);
    }
    let text = frame("eth_s0_timestamp_ms");
    for (meta, _) in d.try_text(&text).unwrap() {
        assert_eq!(
            meta.exch_ts,
            Some(fbc_core::ExchNs(1_767_225_600_123_000_000))
        );
        assert_eq!(meta.exch_ts_kind, fbc_core::ExchTsKind::Unknown);
    }
    assert_eq!(d.state(ETH), BookState::Valid { epoch: 2 });
}

/// A named edit of `eth_s0`, and whether the side it breaks is thin rather than out of bounds.
type Case = (&'static str, fn(&mut serde_json::Value), bool);

/// `eth_s0` with `edit` applied to its JSON.
fn s0_with(edit: impl FnOnce(&mut serde_json::Value)) -> String {
    let mut value: serde_json::Value = serde_json::from_str(&frame("eth_s0")).unwrap();
    edit(&mut value);
    value.to_string()
}

#[test]
fn each_part_of_the_bounds_check_fails_a_side_that_breaks_it() {
    let level = |px: &str| serde_json::json!({"price": px, "quantity": "1.000000000"});
    let cases: [Case; 6] = [
        // A level outside its side's bounds.
        (
            "outside",
            |v| v["data"]["ask"]["endPrice"] = "2000.20".into(),
            false,
        ),
        // The best level not at startPrice (2000.05 is on the granularity, no level there).
        (
            "best",
            |v| v["data"]["bid"]["startPrice"] = "2000.05".into(),
            false,
        ),
        // The deepest level not at endPrice.
        (
            "deepest",
            |v| v["data"]["bid"]["endPrice"] = "1999.70".into(),
            false,
        ),
        // More levels than the depth within the bounds.
        (
            "deeper",
            |v| {
                let levels = v["data"]["ask"]["levels"].as_array_mut().unwrap();
                levels.push(serde_json::json!({"price": "2000.25", "quantity": "1.000000000"}));
            },
            false,
        ),
        // A side with its bounds and no level: thinner than the depth.
        (
            "empty side",
            |v| v["data"]["bid"]["levels"] = serde_json::json!([]),
            true,
        ),
        // A zero in a Snapshot is no level: with the bounds at the two left, the side is thin.
        (
            "zero",
            |v| {
                v["data"]["bid"]["levels"][2]["quantity"] = "0.000000000".into();
                v["data"]["bid"]["endPrice"] = "1999.90".into();
            },
            true,
        ),
    ];
    for (what, edit, thin) in cases {
        let mut d = eth();
        let pushed = d.try_text(&s0_with(edit)).unwrap();
        assert_eq!(pushed, [(VenueMeta::NONE, book_gap(ETH))], "{what}");
        let counts = d.counts();
        let expected = if thin { (0, 1) } else { (1, 0) };
        assert_eq!(
            (counts.bounds_failures, counts.thin_sides),
            expected,
            "{what}"
        );
        assert!(!d.valid(ETH), "{what}");
    }
    // An Update that sets a level outside its own window.
    let mut d = eth();
    d.feed("eth_s0");
    let mut u1: serde_json::Value = serde_json::from_str(&frame("eth_u1")).unwrap();
    u1["data"]["ask"]["levels"]
        .as_array_mut()
        .unwrap()
        .push(level("2000.50"));
    let pushed = d.try_text(&u1.to_string()).unwrap();
    assert_eq!(pushed, [(VenueMeta::NONE, book_gap(ETH))]);
    assert_eq!(d.counts().bounds_failures, 1);
}

/// A named edit of `eth_s0`.
type Edit = (&'static str, fn(&mut serde_json::Value));

#[test]
fn a_thin_side_is_counted_apart_only_when_the_frame_fails_the_check_by_nothing_else() {
    let cases: [Edit; 2] = [
        // A thin bid, self-consistent (two levels from 2000.20 down to 2000.15), crossing the
        // 2000.10 ask.
        ("crossed", |v| {
            v["data"]["bid"] = serde_json::json!({
                "endPrice": "2000.15",
                "levels": [
                    {"price": "2000.20", "quantity": "1.000000000"},
                    {"price": "2000.15", "quantity": "1.000000000"},
                ],
                "startPrice": "2000.20",
            });
        }),
        // A thin bid (bounds and no level) beside an ask with a level outside its bounds.
        ("outside", |v| {
            v["data"]["bid"]["levels"] = serde_json::json!([]);
            v["data"]["ask"]["endPrice"] = "2000.20".into();
        }),
    ];
    for (what, edit) in cases {
        let mut d = eth();
        let pushed = d.try_text(&s0_with(edit)).unwrap();
        assert_eq!(pushed, [(VenueMeta::NONE, book_gap(ETH))], "{what}");
        let counts = d.counts();
        assert_eq!(
            (counts.bounds_failures, counts.thin_sides),
            (1, 0),
            "{what}"
        );
    }
}

#[test]
fn a_null_side_in_a_frame_failing_the_check_emits_only_the_gap_never_its_clear_first() {
    let mut d = eth();
    d.feed("eth_s0");
    d.feed("eth_u1");
    // eth_null_bid's ask window, 2000.10..2000.30, would remove the resting 2000.40 unstated.
    assert_eq!(d.feed("eth_null_bid"), [book_gap(ETH)]);
    let counts = d.counts();
    assert_eq!((counts.bounds_failures, counts.null_sides), (1, 0));
}

#[test]
fn a_stated_zero_for_a_level_already_gone_raises_nothing() {
    let mut d = eth();
    d.feed("eth_s0");
    // The unchanged frame with zeros for levels the book never had: past the bid's deep end
    // (1999.70) and inside the ask's range (2000.25). Neither is emitted, and nothing fails.
    let text = frame("eth_unchanged")
        .replacen(
            r#""levels":[]"#,
            r#""levels":[{"price":"2000.25","quantity":"0.000000000"}]"#,
            1,
        )
        .replacen(
            r#""levels":[]"#,
            r#""levels":[{"price":"1999.70","quantity":"0.000000000"}]"#,
            1,
        );
    let before = d.book(ETH).canonical_bytes();
    let events: Vec<MdEvent> = d
        .try_text(&text)
        .unwrap()
        .into_iter()
        .map(|(_, e)| e)
        .collect();
    assert_eq!(
        events,
        [prune(Bid, 199_980, 200_000), prune(Ask, 200_010, 200_030)]
    );
    assert_eq!(d.book(ETH).canonical_bytes(), before);
    assert_eq!(d.counts(), Default::default());
}

#[test]
fn no_frame_becomes_a_both_sides_window() {
    let mut d = Driver::new(&common::every_feed(ETH));
    let mut events = Vec::new();
    for name in [
        "eth_s0",
        "eth_u1",
        "eth_u2",
        "eth_u3",
        "eth_unchanged",
        "eth_s0",
        "eth_null_bid",
        "eth_bid_restore",
        "eth_snapshot_null_bid",
        "eth_snapshot_locked",
        "abp_sized",
        "trade_single",
    ] {
        events.extend(d.feed(name));
    }
    assert!(!events.is_empty());
    assert!(!events.iter().any(|ev| matches!(ev, MdEvent::Window { .. })));
}
