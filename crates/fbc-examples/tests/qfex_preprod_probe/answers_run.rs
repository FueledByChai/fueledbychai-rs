//! FBC-4plj: the full report's pure parts, at function level: decision 0132's list for each named
//! account ([`answers`]), each item's every branch, measured or not, from evidence built by hand;
//! the trade measures the reads keep ([`places::time_kind`], [`places::TradeIds`],
//! [`places::Sides`]); the client ids' full length and ACK's latency ([`stage_two::FullLength`],
//! [`stage_two::Acks`]); and the resync's cross-check's two counts ([`capture::open_orders`],
//! [`pages::Pages::orders`]). Declared by path in `tests/qfex_preprod_probe.rs`, so it is part of
//! that test binary and Cargo builds no target of its own for it. Every value is made up.

use super::*;

use std::str::FromStr as _;

use fbc_core::{ClientIdFormat, Side};
use rust_decimal::Decimal;

use answers::{ITEMS, RUN_WIDE};
use capture::AccountRead;
use compare::{Compared, Ended};
use pager::{Came, PageRead, PagesRun, WalkEnd};
use places::{DropRun, Sides, TradeIds, Traded, TwoRun, XAfter};
use stage_two::{Acks, CrossRun, FullLength, StageTwo};

/// The answer `ev` gives `item`, found by its first words, on `account`.
fn said(ev: &Evidence, account: &str, starts: &str) -> String {
    let at = ITEMS.iter().position(|i| i.starts_with(starts)).unwrap();
    answers::answers(ev, &ev.sessions, account)[at].clone()
}

/// Evidence naming `primary` alone.
fn named() -> Evidence {
    Evidence {
        accounts: vec!["primary"],
        ..Evidence::default()
    }
}

#[test]
fn an_account_no_stage_ran_on_answers_each_item_not_measured_saying_why() {
    let ev = named();
    let all = answers::answers(&ev, &ev.sessions, "primary");
    assert_eq!(all.len(), ITEMS.len());
    let expected = [
        "not measured: no session armed on this account",
        "not measured: no drop test ran on this account",
        "not measured: no drop test ran on this account",
        "not measured: no drop test ran on this account",
        "not measured: no drop test ran on this account",
        "not measured: no drop session ran",
        "not measured: no drop session ran",
        "not measured: no drop session ran",
        "not measured: no drop session ran; this account's crossing post-only: not measured: no \
         crossing session ran on this account",
        "not measured: no order of ours was read on GET /user/historic-orders",
        "not measured: no get_user_trades read was made on this account",
        "not measured: no get_user_trades read was made on this account",
        "not measured: stage three did not run on this account",
        "not measured: no read of this account listed a whole page",
        "not measured: no comparison ran on this account",
        "not measured: no drop session ran",
        "not measured: no taker IOC was placed on this account",
        "not measured: no taker IOC was placed on this account",
        "not measured: no resync on this account was journaled",
        "not measured: no resync on this account asked for a page",
        "not measured: no get_user_trades read was made on this account",
        "at least 0 on this account's 0 sessions and 0 on its 0 read sockets, each by name in \
         the provider rate refusals section",
        "not measured: no registry was built on this account",
    ];
    assert_eq!(all, expected);
    // The report's section and the evidence's entry give the same, under each account's name.
    let mut s = String::new();
    answers::markdown(&mut s, &ev, &ev.sessions);
    assert!(s.contains("### Decision 0132's list on primary\n\n- What answers an arm: not"));
    let json = answers::to_json(&ev, &ev.sessions);
    let items = json["by_account"]["primary"].as_object().unwrap();
    assert_eq!(items.len(), ITEMS.len());
    assert_eq!(json["run_wide_items_say"], RUN_WIDE);
}

#[test]
fn the_run_wide_items_say_so_and_the_crossing_and_taker_items_read_their_sessions() {
    let mut ev = named();
    let cross = CrossRun {
        account: "primary",
        ..CrossRun::default()
    };
    let take = CrossRun {
        account: "primary",
        taken: Some(taker::Taken::default()),
        ..CrossRun::default()
    };
    ev.stage_two = Some(StageTwo {
        crosses: vec![cross],
        takes: vec![take],
        ..StageTwo::default()
    });
    let ids = said(&ev, "primary", "Whether QFEX accepts");
    assert_eq!(
        ids,
        format!("{RUN_WIDE} not measured: no client id was sent; the characters: not measured")
    );
    let added = said(&ev, "primary", "Whether QFEX accepted add_order");
    assert_eq!(
        added,
        format!("{RUN_WIDE} not measured: no add_order was sent")
    );
    assert!(said(&ev, "primary", "Which statuses").ends_with(
        "this account's crossing post-only: not measured: no crossing post-only was placed"
    ));
    let frames = said(&ev, "primary", "The order of the taker IOC's");
    assert_eq!(frames, "not measured: the taker IOC's frames were not read");
    let fee = said(&ev, "primary", "The taker fee's sign");
    assert_eq!(fee, "not measured: the taker IOC brought no fill");
}

/// A trades read that named `naming` rows of its first order, kept as `Traded`.
fn traded(sides: Sides, foreign: usize, fields: &[&str]) -> Traded {
    Traded {
        trades: sides.rows() + foreign,
        naming: vec![sides.rows()],
        terminal: vec![None],
        fields: fields.iter().map(|f| (*f).to_owned()).collect(),
        sides,
        foreign,
        ..Traded::default()
    }
}

/// Evidence whose drop test read X's trades after drop session one as `after`, and whose taker
/// IOC session read `take`'s.
fn with_trades(after: Option<XAfter>, take: Option<Traded>) -> Evidence {
    let mut ev = named();
    ev.drops = vec![DropRun {
        account: "primary",
        after,
        ..DropRun::default()
    }];
    let run = CrossRun {
        account: "primary",
        traded: take,
        ..CrossRun::default()
    };
    ev.stage_two = Some(StageTwo {
        takes: vec![run],
        ..StageTwo::default()
    });
    ev
}

#[test]
fn which_side_field_a_row_carries_and_whose_side_it_is() {
    let side = |side: [usize; 3]| Sides {
        side,
        aggressor: [0, 0, side.iter().sum()],
    };
    // A maker fill of ours whose row states our side: side is ours; the other side: the
    // aggressor's; both: not told apart.
    for (sides, whose) in [
        (
            [2, 0, 0],
            "ours: on 2 rows of a maker fill of ours, side was our side",
        ),
        (
            [1, 0, 0],
            "ours: on 1 row of a maker fill of ours, side was our side",
        ),
        (
            [0, 1, 0],
            "the aggressor's: on 1 row of a maker fill of ours, side was the other side",
        ),
        (
            [1, 1, 0],
            "not told apart: on 2 rows of a maker fill of ours, side ours on 1, the other side on \
             1, absent on 0; aggressor_side ours on 0, the other side on 0, absent on 2",
        ),
    ] {
        let after = traded(side(sides), 0, &["side", "trade_id"]).x_after();
        let ev = with_trades(Some(after), None);
        assert_eq!(
            said(&ev, "primary", "Which side field"),
            format!("carried: side; side is {whose}")
        );
    }
    // Only a taker's rows, on which ours is the aggressor's: not told apart.
    let take = traded(side([1, 0, 0]), 0, &["aggressor_side", "side"]);
    let ev = with_trades(None, Some(take));
    assert_eq!(
        said(&ev, "primary", "Which side field"),
        "carried: aggressor_side and side; side is not told apart: on the taker IOC's 1 row (side \
         ours on 1, the other side on 0, absent on 0; aggressor_side ours on 0, the other side on \
         0, absent on 1) ours is the aggressor's, and no row of a maker fill of ours was read"
    );
    // A read with no row naming an order of ours, and one with no row at all.
    let ev = with_trades(None, Some(traded(Sides::default(), 1, &["trade_id"])));
    assert_eq!(
        said(&ev, "primary", "Which side field"),
        "carried: none: no member whose name holds side; side is not measured: no row naming an \
         order of ours was read"
    );
    assert_eq!(
        said(&ev, "primary", "get_user_trades rows naming"),
        "1 row: an order of another's, or one this run did not name, traded on the account (none \
         is pushed as a fill)"
    );
    let ev = with_trades(None, Some(Traded::default()));
    assert_eq!(
        said(&ev, "primary", "Which side field"),
        "carried: not shown: no row was read; side is not measured: no row naming an order of \
         ours was read"
    );
    assert_eq!(said(&ev, "primary", "get_user_trades rows naming"), "none");
}

#[test]
fn the_kind_of_a_trades_time_and_whether_its_trade_id_is_the_fills_channels() {
    let at = |kinds: Vec<&'static str>, ids: TradeIds| {
        let take = Traded {
            time_kinds: kinds,
            trade_ids: ids,
            ..Traded::default()
        };
        said(
            &with_trades(None, Some(take)),
            "primary",
            "What kind of time",
        )
    };
    let none = "not measured: no get_user_trades row naming an order of ours was read";
    assert_eq!(
        at(Vec::new(), TradeIds::default()),
        format!("the timestamp: {none}; the trade_id: {none}")
    );
    let ids = TradeIds {
        matched: 1,
        unmatched: 1,
        absent: 1,
    };
    let kinds = vec![
        places::time_kind(Some(&Decimal::from(1_790_000_000))),
        places::time_kind(None),
    ];
    assert_eq!(
        at(kinds, ids),
        "the timestamp: whole Unix seconds, by its magnitude, and none read: absent, or not a \
         number; whether it is the matching engine's time its magnitude does not show; the \
         trade_id: not shown the same: of the 3 get_user_trades rows naming an order of ours, 1 \
         carried a trade_id a fill_response of ours carried, 1 one none did, and 1 none"
    );
    // The drop test's reads and the taker IOC's are added up, each kind once.
    let after = XAfter {
        time_kinds: vec!["whole Unix seconds, by its magnitude"],
        trade_ids: TradeIds {
            matched: 1,
            ..TradeIds::default()
        },
        ..XAfter::default()
    };
    let two = Traded {
        time_kinds: vec!["whole Unix seconds, by its magnitude"],
        trade_ids: TradeIds {
            matched: 2,
            ..TradeIds::default()
        },
        ..Traded::default()
    };
    let mut ev = with_trades(Some(after), None);
    ev.drops[0].two = Some(TwoRun {
        after: Some(two),
        ..TwoRun::default()
    });
    assert_eq!(
        said(&ev, "primary", "What kind of time"),
        "the timestamp: whole Unix seconds, by its magnitude; whether it is the matching engine's \
         time its magnitude does not show; the trade_id: the same: each of the 3 get_user_trades \
         rows naming an order of ours carried a trade_id a fill_response of ours carried, \
         exactly"
    );
}

#[test]
fn a_timestamps_kind_is_read_from_its_magnitude_alone() {
    let kind = |text: &str| places::time_kind(Some(&Decimal::from_str(text).unwrap()));
    for (text, said) in [
        ("1790000000", "whole Unix seconds, by its magnitude"),
        (
            "1790000000.25",
            "Unix seconds with a fraction, by its magnitude",
        ),
        ("1790000000123", "whole Unix milliseconds, by its magnitude"),
        (
            "1790000000123.5",
            "Unix milliseconds with a fraction, by its magnitude",
        ),
        (
            "1790000000123456",
            "whole Unix microseconds, by its magnitude",
        ),
        (
            "1790000000123456.5",
            "Unix microseconds with a fraction, by its magnitude",
        ),
        (
            "1790000000123456789",
            "whole Unix nanoseconds, by its magnitude",
        ),
        (
            "1790000000123456789.5",
            "Unix nanoseconds with a fraction, by its magnitude",
        ),
        ("42", "a number no Unix unit fits by its magnitude"),
        ("0", "a number no Unix unit fits by its magnitude"),
    ] {
        assert_eq!(kind(text), said, "{text}");
    }
    assert_eq!(
        places::time_kind(None),
        "none read: absent, or not a number"
    );
}

#[test]
fn trade_ids_and_sides_are_counted_row_by_row() {
    let mut ids = TradeIds::default();
    let heard = |id: &str| id == "t-1";
    ids.count(Some("t-1"), heard);
    ids.count(Some("t-2"), heard);
    ids.count(None, heard);
    assert_eq!(
        ids.plus(ids),
        TradeIds {
            matched: 2,
            unmatched: 2,
            absent: 2
        }
    );
    let one = TradeIds {
        matched: 1,
        ..TradeIds::default()
    };
    assert_eq!(
        one.says(),
        "the same: each of the 1 get_user_trades row naming an order of ours carried a trade_id a \
         fill_response of ours carried, exactly"
    );
    let mut sides = Sides::default();
    sides.count(Side::Buy, Some(Side::Buy), None);
    sides.count(Side::Buy, Some(Side::Sell), Some(Side::Buy));
    sides.count(Side::Buy, None, Some(Side::Sell));
    assert_eq!(sides.side, [1, 1, 1]);
    assert_eq!(sides.aggressor, [1, 1, 1]);
    assert_eq!(sides.rows(), 3);
}

/// One page of the pager: `ours` listed with `open` open as it was asked.
fn page(open: &[&'static str], ours: &[&'static str], full: bool) -> PageRead {
    PageRead {
        walk: false,
        size: 1,
        offset: 0,
        open: open.to_vec(),
        came: Came::Read {
            ours: ours.to_vec(),
            others: 0,
            twaps: 0,
            full,
        },
    }
}

#[test]
fn how_the_pages_behaved_comes_from_stage_threes_pages() {
    let mut ev = named();
    let run = |pages: Vec<PageRead>, walk: Option<WalkEnd>| PagesRun {
        session: CrossRun {
            account: "primary",
            ..CrossRun::default()
        },
        pages,
        walk,
        ..PagesRun::default()
    };
    ev.stage_three = vec![run(Vec::new(), None)];
    let pages = said(&ev, "primary", "How the pages");
    assert_eq!(pages, "not measured: the pager asked for no page");
    // A page listing an order the streams showed ended, a page not read, a newest-first page 0.
    let mut skipped = page(&[], &[], false);
    skipped.came = Came::Skipped;
    let listed = vec![
        page(&["P1", "P2"], &["P2"], true),
        page(&["P2"], &["P1"], true),
        skipped,
    ];
    ev.stage_three = vec![run(listed, Some(WalkEnd::NotRead))];
    assert_eq!(
        said(&ev, "primary", "How the pages"),
        "3 pages asked at a page size of 1, 2 read, 2 of them full; the order QFEX's pages list \
         ours in: newest first; a page listing an order of ours the streams did not show open: \
         yes; the walk from offset 0: a page that could not be made"
    );
    let one = vec![page(&["P1"], &["P1"], false)];
    ev.stage_three = vec![run(one, None)];
    assert_eq!(
        said(&ev, "primary", "How the pages"),
        "1 page asked at a page size of 1, 1 read, 0 of them full; the order QFEX's pages list \
         ours in: not shown; a page listing an order of ours the streams did not show open: no; \
         the walk from offset 0: not begun"
    );
}

/// An account read of `label`'s: REST's `open_orders`, the page's orders, and whether it was
/// full.
fn read(label: &str, rest: u64, listed: usize, full: bool) -> AccountRead {
    AccountRead {
        label: label.to_owned(),
        rest_open_orders: rest,
        page_orders: listed,
        page_full: full,
        ..AccountRead::default()
    }
}

#[test]
fn what_open_orders_counts_is_judged_on_the_accounts_whole_reads() {
    let mut ev = named();
    // A full page lists not every order: never judged; another account's read is not this one's.
    ev.reads = vec![
        read("primary (drop)", 3, 1, true),
        read("subaccount", 0, 0, false),
    ];
    let counts = said(&ev, "primary", "What open_orders");
    assert_eq!(
        counts,
        "not measured: no read of this account listed a whole page"
    );
    ev.reads = vec![
        read("primary", 0, 0, false),
        read("primary (drop)", 0, 0, false),
    ];
    assert_eq!(
        said(&ev, "primary", "What open_orders"),
        "nothing open on every whole read, as the pages list nothing: not shown with an order \
         open: it equalled the orders the read's page listed on 2 of 2 whole reads of this \
         account, 0 of them with an order open"
    );
    ev.reads.push(read("primary (pages)", 2, 1, false));
    assert_eq!(
        said(&ev, "primary", "What open_orders"),
        "not what the pages list on every whole read: it equalled the orders the read's page \
         listed on 2 of 3 whole reads of this account, 0 of them with an order open"
    );
}

/// A comparison that ended `ended`, its first agreeing read at `first`.
fn compared(ended: Ended, first: Option<u64>) -> Compared {
    Compared {
        step: "X's place",
        reads: Vec::new(),
        first_agreed: first.map(Duration::from_millis),
        ended,
        stop: None,
    }
}

#[test]
fn the_lag_counts_every_comparison_on_the_accounts_sessions() {
    let mut ev = named();
    ev.drops = vec![DropRun {
        account: "primary",
        compared: vec![compared(Ended::Lagged, None)],
        two: Some(TwoRun {
            compared: vec![compared(Ended::Unread, None)],
            ..TwoRun::default()
        }),
        ..DropRun::default()
    }];
    let cross = CrossRun {
        account: "primary",
        compared: vec![
            compared(Ended::Disagreed, None),
            compared(Ended::Refused, None),
        ],
        ..CrossRun::default()
    };
    ev.stage_two = Some(StageTwo {
        crosses: vec![cross],
        ..StageTwo::default()
    });
    assert_eq!(
        said(&ev, "primary", "The lag"),
        "4 comparisons: 0 agreed; 1 ended lagged; 1 disagreed; 1 met a provider rate refusal; 1 \
         with no read made"
    );
    let pages = PagesRun {
        session: CrossRun {
            account: "primary",
            compared: vec![
                compared(Ended::Agreed, Some(3)),
                compared(Ended::Agreed, Some(250)),
            ],
            ..CrossRun::default()
        },
        ..PagesRun::default()
    };
    ev.stage_three = vec![pages];
    assert!(said(&ev, "primary", "The lag").starts_with(
        "6 comparisons: 2 agreed, the first agreeing read ending from 3 ms to 250 ms after \
             its step;"
    ));
}

#[test]
fn historic_orders_name_x_y_the_taker_ioc_and_its_flatten_iocs() {
    let mut ev = named();
    ev.drops = vec![DropRun {
        account: "primary",
        two: Some(TwoRun {
            y: places::XRun {
                submitted: true,
                ..places::XRun::default()
            },
            ..TwoRun::default()
        }),
        ..DropRun::default()
    }];
    // The taker IOC not listed on a whole read, and two flatten IOCs, one listed.
    let take = Traded {
        terminal: vec![None, Some("FILLED".to_owned()), None],
        historic_whole: true,
        ..Traded::default()
    };
    ev.stage_two = Some(StageTwo {
        takes: vec![CrossRun {
            account: "primary",
            traded: Some(take),
            ..CrossRun::default()
        }],
        ..StageTwo::default()
    });
    assert_eq!(
        said(&ev, "primary", "What GET"),
        "X not read; Y not read; the taker IOC not listed; its flatten IOCs 1 of 2 listed"
    );
}

#[test]
fn each_resync_is_set_against_rests_open_orders_and_gives_its_full_pages() {
    let mut ev = named();
    let session = |name: &'static str, asked: u64, orders: u64, open: Option<u64>| {
        let mut pages = pages::Pages::default();
        pages.asked = asked;
        pages.read = asked;
        pages.orders = orders;
        SessionRun {
            account: "primary",
            name,
            pages,
            resync_open_orders: open,
            ..SessionRun::default()
        }
    };
    ev.sessions = vec![
        session(places::DROP_ONE, 1, 1, Some(2)),
        session(places::DROP_TWO, 1, 0, None),
        session(places::CROSS, 0, 0, None),
    ];
    assert_eq!(
        said(&ev, "primary", "Each resync against"),
        "drop session one: its pages listed 1 order, its GET /user/positions counted open_orders \
         2: they differ; drop session two: its pages listed 0 orders, its GET /user/positions \
         counted open_orders not journaled"
    );
    assert_eq!(
        said(&ev, "primary", "Each resync's full pages"),
        "drop session one: none; drop session two: none"
    );
    // The refusals the account's sessions and read sockets met, another account's not counted.
    ev.read_sockets = vec![
        refusals::ReadSocket {
            before: places::DROP_ONE,
            label: "primary (drop)".to_owned(),
            refusals: Refusals {
                limiter: 2,
                ..Refusals::default()
            },
        },
        refusals::ReadSocket {
            before: places::DROP_ONE,
            label: "subaccount".to_owned(),
            refusals: Refusals {
                limiter: 5,
                ..Refusals::default()
            },
        },
    ];
    assert_eq!(
        said(&ev, "primary", "The provider rate refusals"),
        "at least 0 on this account's 3 sessions and 2 on its 1 read socket, each by name in the \
         provider rate refusals section"
    );
}

#[test]
fn the_resyncs_counts_come_from_its_pages_and_its_positions_answer() {
    // GET /user/positions: every row's open_orders summed; null positions, none; anything else
    // no answer of one.
    let body = |text: &str| capture::open_orders(text.as_bytes());
    let rows = r#"{"balance":{},"positions":[{"symbol":"A","open_orders":2},{"open_orders":1}]}"#;
    assert_eq!(body(rows), Some(3));
    assert_eq!(body(r#"{"balance":{},"positions":null}"#), Some(0));
    assert_eq!(
        body(r#"{"balance":{},"positions":[{"open_orders":-1}]}"#),
        None
    );
    assert_eq!(body(r#"{"positions":{}}"#), None);
    assert_eq!(body(r#"{"balance":{}}"#), None);
    assert_eq!(body("[]"), None);
    assert_eq!(body("not json"), None);
    // A page's orders, TWAPs aside, added to the resync's count.
    let mut seen = pages::Pages::default();
    seen.sent(br#"{"type":"get_user_orders","params":{"limit":5,"offset":0}}"#);
    let one = concat!(
        r#"{"all_orders_response":{"orders":[{"order_id":"o-1","type":"TAKE_PROFIT"}],"#,
        r#""twaps":[{}]}}"#
    );
    seen.heard(one);
    assert_eq!((seen.read, seen.orders), (1, 1));
}

#[test]
fn the_client_ids_full_length_is_the_formats_and_ack_latency_is_judged_against_the_bump() {
    assert_eq!(stage_two::full_length(&ClientIdFormat::Uuid), 36);
    assert_eq!(stage_two::full_length(&ClientIdFormat::Hex { len: 20 }), 20);
    let alnum = ClientIdFormat::Alnum {
        max_len: 32,
        charset: fbc_core::Charset::Alphanumeric,
    };
    assert_eq!(stage_two::full_length(&alnum), 32);
    let numeric = ClientIdFormat::Numeric { max_digits: 19 };
    assert_eq!(stage_two::full_length(&numeric), 19);
    let length = |sent, full, echoed| FullLength {
        length: 36,
        sent,
        full,
        echoed,
    };
    assert_eq!(
        length(0, 0, false).says(),
        "not measured: no client id was sent"
    );
    assert_eq!(
        length(2, 1, true).says(),
        "1 of the 2 client ids the run sent were the format's full length, 36 characters; an ACK \
         echoed one byte for byte, but not every id was at the full length"
    );
    assert_eq!(
        length(2, 2, false).says(),
        "2 of the 2 client ids the run sent were the format's full length, 36 characters; no ACK \
         echoed one byte for byte, so QFEX's acceptance at that length is not shown"
    );
    let ms = Duration::from_millis;
    let acks = |other: Vec<Duration>| Acks {
        post_only: vec![ms(3)],
        other,
    };
    assert_eq!(
        acks(vec![ms(10), ms(499)]).says(),
        "post-only places: 1 ACK, from 3 ms to 3 ms after its send; other places: 2 ACKs, from 10 \
         ms to 499 ms after its send; against the speed bump's 500 ms hold on a place that is not \
         post-only: every one came before the hold's end, within 500 ms of its send"
    );
    assert_eq!(
        acks(vec![ms(500)]).bump_said(),
        "every one came after the hold, 500 ms or more after its send"
    );
    assert_eq!(
        acks(vec![ms(10), ms(700)]).bump_said(),
        "1 came before the hold's end, within 500 ms of its send, and 1 after it"
    );
    assert_eq!(
        acks(Vec::new()).bump_said(),
        "not measured: no ACK of a place that is not post-only was heard"
    );
    assert_eq!(stage_two::SPEED_BUMP, ms(500));
}
