//! FBC-30g's done line, on hand-built `fbc-book` books and for each bracket: a new order queues
//! behind its level's size less every modelled own order; a level cancel advances it by none,
//! the floor of the proportional share, or all of the cancelled size; and a trade fills it only
//! once the queue ahead is consumed. Decision 0038's trade-through and flow rules, and the
//! error paths, are here too. The configuration's missing `Default` is `tests/compile_fail.rs`.

use fbc_book::{BookError, BookState, L2Book};
use fbc_core::{BookSide, Channel, Lots, Side, Ticks};

use crate::{
    Bracket, NewOrder, OrderKey, QueueConfig, QueueError, QueueModel, QueuePos, SimFill, TradeView,
};

const BRACKETS: [Bracket; 3] = [Bracket::Pessimistic, Bracket::Middle, Bracket::Optimistic];

fn lots(n: i64) -> Lots {
    Lots::new(n).unwrap()
}

fn model(bracket: Bracket) -> QueueModel {
    QueueModel::new(QueueConfig { bracket })
}

/// A valid book with these bid and ask levels, `(px, qty)`.
fn book(bids: &[(i64, i64)], asks: &[(i64, i64)]) -> L2Book {
    let mut b = L2Book::new();
    b.begin_snapshot(1);
    for &(px, qty) in bids {
        b.set_level(BookSide::Bid, Ticks(px), lots(qty));
    }
    for &(px, qty) in asks {
        b.set_level(BookSide::Ask, Ticks(px), lots(qty));
    }
    b.end_snapshot().unwrap();
    b
}

fn order(side: Side, px: i64, channel: Channel, qty: i64) -> NewOrder {
    NewOrder {
        side,
        px: Ticks(px),
        channel,
        qty: lots(qty),
    }
}

fn buy(px: i64, qty: i64) -> NewOrder {
    order(Side::Buy, px, Channel::Public, qty)
}

fn sell(px: i64, qty: i64) -> NewOrder {
    order(Side::Sell, px, Channel::Public, qty)
}

fn trade(taker: Side, px: i64, qty: i64, channel: Channel) -> TradeView {
    TradeView {
        taker,
        px: Ticks(px),
        qty: lots(qty),
        channel,
    }
}

/// Public flow selling into the bids at `px`.
fn sold(px: i64, qty: i64) -> TradeView {
    trade(Side::Sell, px, qty, Channel::Public)
}

fn fill(key: u64, side: Side, px: i64, qty: i64, remaining: i64) -> SimFill {
    SimFill {
        key: OrderKey(key),
        side,
        px: Ticks(px),
        qty: lots(qty),
        remaining: lots(remaining),
    }
}

fn ahead(m: &QueueModel, key: u64) -> i64 {
    m.position(OrderKey(key)).unwrap().ahead.get()
}

/// A model under `bracket` holding one public buy (key 1) at 100 for `qty`, queued behind
/// `queue` lots.
fn queued(bracket: Bracket, queue: i64, qty: i64) -> QueueModel {
    let mut m = model(bracket);
    m.accept(OrderKey(1), buy(100, qty), &book(&[(100, queue)], &[]))
        .unwrap();
    m
}

#[test]
fn a_new_order_queues_behind_its_level_less_every_modelled_own_order() {
    for bracket in BRACKETS {
        let mut m = model(bracket);
        assert_eq!(m.bracket(), bracket);
        // Nothing modelled yet: all 12 lots at 100 are ahead.
        let pos = m
            .accept(
                OrderKey(1),
                buy(100, 2),
                &book(&[(100, 12), (99, 5)], &[(101, 4)]),
            )
            .unwrap();
        assert_eq!(
            pos,
            QueuePos {
                side: Side::Buy,
                px: Ticks(100),
                channel: Channel::Public,
                remaining: lots(2),
                ahead: lots(12),
            }
        );
        assert_eq!(m.position(OrderKey(1)), Some(pos));
        // An injected order joins once the level shows the first (14 = 12 + 2): the first is
        // modelled, so it is not ahead.
        let injected = m
            .accept(OrderKey(2), buy(100, 3), &book(&[(100, 14)], &[(101, 4)]))
            .unwrap();
        assert_eq!(injected.ahead, lots(12), "{bracket:?}");
        // A third behind both, the level now showing them (17 = 12 + 2 + 3).
        let third = m
            .accept(OrderKey(3), buy(100, 1), &book(&[(100, 17)], &[(101, 4)]))
            .unwrap();
        assert_eq!(third.ahead, lots(12), "{bracket:?}");
        // Modelled orders at another price or on the other side are not subtracted.
        let lower = m
            .accept(
                OrderKey(4),
                buy(99, 1),
                &book(&[(100, 18), (99, 5)], &[(101, 4)]),
            )
            .unwrap();
        assert_eq!(lower.ahead, lots(5), "{bracket:?}");
        let offer = m
            .accept(OrderKey(5), sell(101, 1), &book(&[(100, 18)], &[(101, 4)]))
            .unwrap();
        assert_eq!(offer.ahead, lots(4), "{bracket:?}");
        // An empty level: nothing ahead.
        let alone = m
            .accept(OrderKey(6), sell(105, 1), &book(&[(100, 18)], &[(101, 5)]))
            .unwrap();
        assert_eq!(alone.ahead, Lots::ZERO, "{bracket:?}");
    }
}

#[test]
fn modelled_orders_the_level_does_not_show_yet_leave_nothing_ahead() {
    let mut m = model(Bracket::Middle);
    m.accept(OrderKey(1), buy(100, 5), &book(&[(100, 2)], &[]))
        .unwrap();
    // The level still shows 3, less than the 5 modelled there.
    let pos = m
        .accept(OrderKey(2), buy(100, 1), &book(&[(100, 3)], &[]))
        .unwrap();
    assert_eq!(pos.ahead, Lots::ZERO);
}

#[test]
fn an_rpi_order_queues_behind_every_public_order_at_its_price() {
    let mut m = model(Bracket::Pessimistic);
    // The public level shows 6 with nothing modelled: the RPI bid has 6 ahead.
    let rpi = m
        .accept(
            OrderKey(1),
            order(Side::Buy, 100, Channel::Rpi, 2),
            &book(&[(100, 6)], &[]),
        )
        .unwrap();
    assert_eq!((rpi.channel, rpi.ahead), (Channel::Rpi, lots(6)));
    // A public bid arriving later at 100, with 3 of the public size gone (6 + 2 shown, now
    // 3 + 2), is matched before the earlier RPI bid.
    m.accept(OrderKey(2), buy(100, 4), &book(&[(100, 5)], &[]))
        .unwrap();
    assert_eq!(ahead(&m, 2), 3);
    // Retail flow of 8 at 100: the 3 public lots ahead, the public bid's 4, then 1 more lot
    // of the public size ahead of the RPI bid, which keeps 2 ahead.
    let fills = m.trade(trade(Side::Sell, 100, 8, Channel::Rpi));
    assert_eq!(fills, vec![fill(2, Side::Buy, 100, 4, 0)]);
    assert_eq!(ahead(&m, 1), 2);
    // Retail flow of 3: the last 2 ahead, then 1 fills it.
    assert_eq!(
        m.trade(trade(Side::Sell, 100, 3, Channel::Rpi)),
        vec![fill(1, Side::Buy, 100, 1, 1)]
    );
}

#[test]
fn public_flow_consumes_the_queue_ahead_of_an_rpi_order_but_never_fills_it() {
    let mut m = model(Bracket::Optimistic);
    m.accept(
        OrderKey(1),
        order(Side::Sell, 101, Channel::Rpi, 2),
        &book(&[], &[(101, 5)]),
    )
    .unwrap();
    assert!(
        m.trade(trade(Side::Buy, 101, 9, Channel::Public))
            .is_empty()
    );
    assert_eq!(ahead(&m, 1), 0);
    // Through its price too: nothing ahead, still no fill.
    m.accept(
        OrderKey(2),
        order(Side::Sell, 102, Channel::Rpi, 2),
        &book(&[], &[(101, 2), (102, 7)]),
    )
    .unwrap();
    assert!(
        m.trade(trade(Side::Buy, 103, 9, Channel::Public))
            .is_empty()
    );
    assert_eq!((ahead(&m, 1), ahead(&m, 2)), (0, 0));
    // Retail flow fills them, the better price first.
    assert_eq!(
        m.trade(trade(Side::Buy, 102, 3, Channel::Rpi)),
        vec![
            fill(1, Side::Sell, 101, 2, 0),
            fill(2, Side::Sell, 102, 1, 1),
        ]
    );
}

#[test]
fn a_level_cancel_advances_the_order_by_its_bracket() {
    // 6 ahead in a level of 10; 4 cancelled: none (Pessimistic), floor(4 × 6 / 10) = 2
    // (Middle), all 4 (Optimistic).
    for (bracket, after) in [
        (Bracket::Pessimistic, 6),
        (Bracket::Middle, 4),
        (Bracket::Optimistic, 2),
    ] {
        let mut m = queued(bracket, 6, 1);
        m.level_cancel(BookSide::Bid, Ticks(100), lots(4), lots(10))
            .unwrap();
        assert_eq!(ahead(&m, 1), after, "{bracket:?}");
    }
    // The floor, not the rounding: 3 × 5 / 4 = 3.75 advances 3.
    let mut middle = queued(Bracket::Middle, 5, 1);
    middle
        .level_cancel(BookSide::Bid, Ticks(100), lots(3), lots(4))
        .unwrap();
    assert_eq!(ahead(&middle, 1), 2);
    // Optimistic never advances past the size ahead.
    let mut optimistic = queued(Bracket::Optimistic, 3, 1);
    optimistic
        .level_cancel(BookSide::Bid, Ticks(100), lots(8), lots(9))
        .unwrap();
    assert_eq!(ahead(&optimistic, 1), 0);
    // Middle on lot counts whose product passes an i64.
    let mut big = queued(Bracket::Middle, i64::MAX, 1);
    big.level_cancel(
        BookSide::Bid,
        Ticks(100),
        lots(i64::MAX / 2),
        lots(i64::MAX),
    )
    .unwrap();
    assert_eq!(ahead(&big, 1), i64::MAX - i64::MAX / 2);
}

#[test]
fn a_level_cancel_elsewhere_or_of_nothing_moves_no_order() {
    for bracket in BRACKETS {
        let mut m = queued(bracket, 6, 1);
        m.level_cancel(BookSide::Bid, Ticks(99), lots(6), lots(6))
            .unwrap();
        m.level_cancel(BookSide::Ask, Ticks(100), lots(6), lots(6))
            .unwrap();
        m.level_cancel(BookSide::Bid, Ticks(100), Lots::ZERO, Lots::ZERO)
            .unwrap();
        assert_eq!(ahead(&m, 1), 6, "{bracket:?}");
    }
}

#[test]
fn a_level_cannot_shrink_by_more_than_its_size() {
    let mut m = queued(Bracket::Middle, 6, 1);
    let err = m
        .level_cancel(BookSide::Bid, Ticks(100), lots(7), lots(6))
        .unwrap_err();
    assert_eq!(
        err,
        QueueError::CancelExceedsLevel {
            cancelled: lots(7),
            level_before: lots(6),
        }
    );
    assert_eq!(err.to_string(), "a level of 6 lots cannot shrink by 7");
    assert_eq!(ahead(&m, 1), 6);
}

#[test]
fn a_trade_fills_an_order_only_once_the_queue_ahead_is_consumed() {
    for bracket in BRACKETS {
        // 5 ahead, 3 to fill.
        let mut m = queued(bracket, 5, 3);
        assert!(m.trade(sold(100, 4)).is_empty(), "{bracket:?}");
        assert_eq!(ahead(&m, 1), 1, "{bracket:?}");
        // 1 more consumes the queue; the other 2 fill the order.
        assert_eq!(m.trade(sold(100, 3)), vec![fill(1, Side::Buy, 100, 2, 1)]);
        assert_eq!(m.position(OrderKey(1)).unwrap().remaining, lots(1));
        // Never past what remains, and a filled order is reported once.
        assert_eq!(m.trade(sold(100, 5)), vec![fill(1, Side::Buy, 100, 1, 0)]);
        assert_eq!(m.position(OrderKey(1)), None);
        assert!(m.trade(sold(100, 5)).is_empty(), "{bracket:?}");
    }
}

#[test]
fn a_trade_the_order_cannot_meet_changes_nothing() {
    for bracket in BRACKETS {
        let mut m = queued(bracket, 5, 3);
        // Buyers lifting at the bid's price, and sellers above it.
        assert!(
            m.trade(trade(Side::Buy, 100, 9, Channel::Public))
                .is_empty()
        );
        assert!(m.trade(sold(101, 9)).is_empty());
        assert_eq!(ahead(&m, 1), 5, "{bracket:?}");
    }
}

#[test]
fn a_level_cancel_then_a_trade_fills_sooner_the_more_optimistic_the_bracket() {
    // 6 ahead of 2 in a level of 10; 5 cancelled leaves 6, 3 or 1 ahead; then 4 trade at 100.
    for (bracket, filled) in [
        (Bracket::Pessimistic, vec![]),
        (Bracket::Middle, vec![fill(1, Side::Buy, 100, 1, 1)]),
        (Bracket::Optimistic, vec![fill(1, Side::Buy, 100, 2, 0)]),
    ] {
        let mut m = queued(bracket, 6, 2);
        m.level_cancel(BookSide::Bid, Ticks(100), lots(5), lots(10))
            .unwrap();
        assert_eq!(m.trade(sold(100, 4)), filled, "{bracket:?}");
    }
}

#[test]
fn a_trade_through_the_orders_price_clears_the_queue_and_fills_it() {
    for bracket in BRACKETS {
        // A bid at 100 with 5 ahead: sellers print 2 at 99, so 100 emptied first.
        let mut m = queued(bracket, 5, 3);
        assert_eq!(m.trade(sold(99, 2)), vec![fill(1, Side::Buy, 100, 2, 1)]);
        assert_eq!(ahead(&m, 1), 0, "{bracket:?}");
        // The ask side mirrors it: buyers printing above an offer fill it.
        let mut asks = model(bracket);
        asks.accept(OrderKey(7), sell(101, 4), &book(&[], &[(101, 9)]))
            .unwrap();
        assert_eq!(
            asks.trade(trade(Side::Buy, 103, 6, Channel::Public)),
            vec![fill(7, Side::Sell, 101, 4, 0)]
        );
    }
}

#[test]
fn a_sweep_meets_the_better_price_first_and_spends_its_size_once() {
    // Bids at 101 (key 1, 2 lots, 4 ahead) and at 100 (key 2, 3 lots, 5 ahead). Sellers
    // print 4 at 100: 101 emptied before the print, so key 1 fills 2, and the other 2 lots
    // consume 100's queue, leaving key 2 with 3 ahead.
    let mut m = model(Bracket::Pessimistic);
    let b = book(&[(101, 4), (100, 5)], &[]);
    m.accept(OrderKey(1), buy(101, 2), &b).unwrap();
    m.accept(OrderKey(2), buy(100, 3), &b).unwrap();
    assert_eq!(m.trade(sold(100, 4)), vec![fill(1, Side::Buy, 101, 2, 0)]);
    assert_eq!(ahead(&m, 2), 3);
}

#[test]
fn modelled_orders_at_one_price_share_a_trade_in_arrival_order() {
    // Order 1: 5 ahead, 2 lots. Order 2 arrives once 3 more public lots joined behind order 1
    // (5 + 2 + 3 = 10 shown, 8 not modelled). A trade of 10: 5 public, order 1's 2, then the
    // 3 public between them; order 2 is at the front with nothing filled.
    for bracket in BRACKETS {
        let mut m = queued(bracket, 5, 2);
        m.accept(OrderKey(2), buy(100, 4), &book(&[(100, 10)], &[]))
            .unwrap();
        assert_eq!(ahead(&m, 2), 8);
        assert_eq!(m.trade(sold(100, 10)), vec![fill(1, Side::Buy, 100, 2, 0)]);
        assert_eq!(ahead(&m, 2), 0, "{bracket:?}");
        assert_eq!(m.trade(sold(100, 1)), vec![fill(2, Side::Buy, 100, 1, 3)]);
    }
}

#[test]
fn a_removed_order_is_no_longer_modelled() {
    let mut m = queued(Bracket::Middle, 5, 3);
    let pos = m.remove(OrderKey(1)).unwrap();
    assert_eq!((pos.remaining, pos.ahead), (lots(3), lots(5)));
    assert_eq!(m.remove(OrderKey(1)), None);
    assert!(m.trade(sold(100, 20)).is_empty());
    // And no longer subtracted from a new order's queue.
    let next = m
        .accept(OrderKey(2), buy(100, 1), &book(&[(100, 8)], &[]))
        .unwrap();
    assert_eq!(next.ahead, lots(8));
}

#[test]
fn accept_refuses_and_changes_nothing() {
    let mut m = queued(Bracket::Optimistic, 5, 3);
    let b = book(&[(100, 8)], &[]);

    let dup = m.accept(OrderKey(1), buy(100, 1), &b).unwrap_err();
    assert_eq!(dup, QueueError::DuplicateOrder(OrderKey(1)));
    assert_eq!(dup.to_string(), "order 1 is already queued");
    assert_eq!(m.position(OrderKey(1)).unwrap().remaining, lots(3));

    let zero = m.accept(OrderKey(2), buy(100, 0), &b).unwrap_err();
    assert_eq!(zero, QueueError::ZeroQuantity(OrderKey(2)));
    assert_eq!(zero.to_string(), "order 2 has no size");

    let unread = m
        .accept(OrderKey(3), buy(100, 1), &L2Book::new())
        .unwrap_err();
    assert_eq!(
        unread,
        QueueError::Book(BookError::NotValid(BookState::AwaitingSnapshot))
    );
    assert_eq!(
        unread.to_string(),
        "queue model cannot read the book: book is not valid: AwaitingSnapshot"
    );

    let mut windowed = book(&[(100, 8)], &[(110, 1)]);
    windowed.set_window(Ticks(95), Ticks(105)).unwrap();
    let outside = m.accept(OrderKey(4), sell(110, 1), &windowed).unwrap_err();
    assert_eq!(
        outside,
        QueueError::OutsideWindow {
            side: BookSide::Ask,
            px: Ticks(110),
        }
    );
    assert_eq!(
        outside.to_string(),
        "Ask level 110 is outside the book's window"
    );

    for key in 2..=4 {
        assert_eq!(m.position(OrderKey(key)), None);
    }
}

#[test]
fn modelled_size_past_an_i64_is_refused() {
    let mut m = model(Bracket::Pessimistic);
    let b = book(&[(100, 1)], &[]);
    m.accept(OrderKey(1), buy(100, i64::MAX), &b).unwrap();
    m.accept(OrderKey(2), buy(100, 1), &b).unwrap();
    let err = m.accept(OrderKey(3), buy(100, 1), &b).unwrap_err();
    assert_eq!(
        err,
        QueueError::OwnSizeOverflow {
            side: BookSide::Bid,
            px: Ticks(100),
        }
    );
    assert_eq!(err.to_string(), "modelled orders at Bid level 100 overflow");
    assert_eq!(m.position(OrderKey(3)), None);
    // The error is a std error.
    let _: &dyn std::error::Error = &err;
}
