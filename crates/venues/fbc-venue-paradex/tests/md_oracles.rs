//! FBC-9cf: the book and touch oracles of BT-401, offline. A REST `/orderbook` response at depth
//! 15 decodes to its levels on the instrument's grid and matches the book built from the
//! fixture deltas at its seq_no, top 15 levels per side, exactly; an altered level is named;
//! and over a generated stream of 1,000 bbo samples the touch-agreement check, sampling at the
//! equal seq_no that bbo's shared sequence gives (decision 0022), reports the agreeing fraction
//! and names every disagreeing sample.

mod md;
mod oracle;

use std::collections::BTreeMap;

use fbc_book::{BookError, BookState, LevelDiff, Touch};
use fbc_core::{
    Aggressor, BookSide, DecodeError, ExchNs, ExchTsKind, Lots, Lvl, MdCodec, MdEvent, RawFrame,
    SeqDomain, StreamId, Subscription, Ticks, VenueMeta,
};
use fbc_venue_paradex::factory::caps;
use fbc_venue_paradex::md::rest::{
    ORDERBOOK_DEPTH, OrderbookSnapshot, decode_orderbook, orderbook_path,
};
use fbc_venue_paradex::md::{BBO, DELTAS, ParadexMd};
use md::{BTC, ETH, decode_with, frame, specs};
use oracle::{BookMismatch, DeltaBook, Disagreement, OracleError, check_snapshot, touch_agreement};

/// The hand-built REST response at seq_no 2002.
fn rest_text() -> String {
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../fixtures/paradex/rest/orderbook-btc-2002.json");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

fn lvl(px: i64, qty: i64) -> Lvl {
    Lvl {
        px: Ticks(px),
        qty: Lots::new(qty).unwrap(),
    }
}

/// The delta-built book of BTC's deltas channel after the hand-built depth-15 frames `names`,
/// decoded through the codec.
fn delta_book(names: &[&str]) -> DeltaBook {
    let mut codec = ParadexMd::new(StreamId(0));
    let mut fx = fbc_core::Effects::new();
    let subs = [Subscription {
        inst: BTC,
        feed: fbc_core::Feed::Book(DELTAS),
    }];
    codec.subscribe(&subs, &[], &specs(), &mut fx).unwrap();
    let mut book = DeltaBook::new(BTC, DELTAS);
    for name in names {
        let out = decode_with(&mut codec, RawFrame::Binary(&frame(name)));
        assert_eq!(out.result, Ok(()), "{name}");
        assert!(out.fx.is_empty(), "{name}: {:?}", out.fx);
        for (meta, ev) in &out.events {
            book.apply(meta, ev).unwrap();
        }
    }
    book
}

const FRAMES_TO_2002: [&str; 3] = [
    "book15-snapshot-2000.sbe.txt",
    "book15-delta-2001.sbe.txt",
    "book15-delta-2002.sbe.txt",
];

#[test]
fn the_request_asks_for_depth_15_of_the_market() {
    assert_eq!(ORDERBOOK_DEPTH, 15);
    assert_eq!(
        orderbook_path("BTC-USD-PERP"),
        "/v1/orderbook/BTC-USD-PERP?depth=15"
    );
}

#[test]
fn a_rest_orderbook_response_decodes_to_its_15_levels_per_side_on_the_grid() {
    let snap = decode_orderbook(rest_text().as_bytes(), &specs()).unwrap();
    assert_eq!(snap.inst, BTC);
    assert_eq!(snap.seq_no, 2002);
    assert_eq!(snap.updated_at, ExchNs(1_759_500_000_123 * 1_000_000));
    assert_eq!((snap.bids.len(), snap.asks.len()), (15, 15));
    // Best first: 62000.2 x 0.333, then 62000.0 x 0.1; the deepest bid 61993.5 x 1.4.
    assert_eq!(snap.bids[0], lvl(620_002, 333));
    assert_eq!(snap.bids[1], lvl(620_000, 100));
    assert_eq!(snap.bids[14], lvl(619_935, 1_400));
    // 62000.5 x 0.1, then 62001.5 x 0.3 (62001.0 was removed at 2001); the deepest 62008.0.
    assert_eq!(snap.asks[0], lvl(620_005, 100));
    assert_eq!(snap.asks[1], lvl(620_015, 300));
    assert_eq!(snap.asks[14], lvl(620_080, 1_600));
}

#[test]
fn the_rest_snapshot_matches_the_book_built_from_the_fixture_deltas_at_its_seq_no() {
    let snap = decode_orderbook(rest_text().as_bytes(), &specs()).unwrap();
    let book = delta_book(&FRAMES_TO_2002);
    assert_eq!(book.seq(), Some(2002));
    assert_eq!(check_snapshot(&book, &snap), Ok(()));
}

#[test]
fn a_response_with_one_altered_level_is_reported_naming_that_level() {
    // The seventh bid (rank 6), 61997.5 x 0.6, altered to 61997.5 x 0.7.
    let text = rest_text().replace(r#"["61997.5", "0.6"]"#, r#"["61997.5", "0.7"]"#);
    let snap = decode_orderbook(text.as_bytes(), &specs()).unwrap();
    let book = delta_book(&FRAMES_TO_2002);
    let err = check_snapshot(&book, &snap).unwrap_err();
    let diff = LevelDiff {
        side: BookSide::Bid,
        rank: 6,
        left: Some(lvl(619_975, 700)),
        right: Some(lvl(619_975, 600)),
    };
    assert_eq!(err, BookMismatch::Differs { seq_no: 2002, diff });
    assert_eq!(
        err.to_string(),
        "REST snapshot at seq_no 2002 differs from the delta-built book: \
         bid rank 6: left 619975x700, right 619975x600"
    );
    // A level the snapshot lacks is named too: the deepest ask removed.
    let text = rest_text().replace(",\n    [\"62008\", \"1.6\"]", "");
    let snap = decode_orderbook(text.as_bytes(), &specs()).unwrap();
    let diff = LevelDiff {
        side: BookSide::Ask,
        rank: 14,
        left: None,
        right: Some(lvl(620_080, 1_600)),
    };
    let err = check_snapshot(&book, &snap).unwrap_err();
    assert_eq!(err, BookMismatch::Differs { seq_no: 2002, diff });
}

#[test]
fn the_book_check_refuses_a_book_at_another_seq_no_another_market_or_not_valid() {
    let snap = decode_orderbook(rest_text().as_bytes(), &specs()).unwrap();
    // One delta short of the snapshot's seq_no.
    let book = delta_book(&FRAMES_TO_2002[..2]);
    let err = check_snapshot(&book, &snap).unwrap_err();
    assert_eq!(
        err,
        BookMismatch::NotAtSeq {
            seq_no: 2002,
            book: Some(2001)
        }
    );
    assert_eq!(
        err.to_string(),
        "REST snapshot at seq_no 2002, delta-built book at Some(2001)"
    );
    // A book of another market.
    let other = DeltaBook::new(ETH, DELTAS);
    let err = check_snapshot(&other, &snap).unwrap_err();
    assert_eq!(err, BookMismatch::OtherInstrument);
    assert_eq!(
        err.to_string(),
        "REST snapshot and delta-built book are of different markets"
    );
    // No frame yet: no seq_no.
    let empty = DeltaBook::new(BTC, DELTAS);
    let err = check_snapshot(&empty, &snap).unwrap_err();
    let none = BookMismatch::NotAtSeq {
        seq_no: 2002,
        book: None,
    };
    assert_eq!(err, none);
    // A gap reported before any snapshot leaves no book to read.
    let mut empty = DeltaBook::new(BTC, DELTAS);
    empty.apply(&meta(Some(2002)), &gap()).unwrap();
    let awaiting = BookError::NotValid(BookState::AwaitingSnapshot);
    let err = check_snapshot(&empty, &snap).unwrap_err();
    assert_eq!(err, BookMismatch::NotValid(awaiting));
    // A gap at the snapshot's seq_no invalidates the book.
    let mut book = delta_book(&FRAMES_TO_2002);
    book.apply(&meta(Some(2002)), &gap()).unwrap();
    let err = check_snapshot(&book, &snap).unwrap_err();
    let not_valid = BookError::NotValid(BookState::Gapped);
    assert_eq!(err, BookMismatch::NotValid(not_valid));
    assert_eq!(
        err.to_string(),
        "delta-built book cannot be read: book is not valid: Gapped"
    );
}

fn gap() -> MdEvent {
    MdEvent::Health {
        inst: BTC,
        feed: fbc_core::Feed::Book(DELTAS),
        h: fbc_core::FeedHealth::Gap,
    }
}

#[test]
fn the_delta_book_refuses_a_snapshot_end_without_a_begin() {
    let mut book = DeltaBook::new(BTC, DELTAS);
    let end = MdEvent::BookSnapshotEnd {
        inst: BTC,
        book: DELTAS,
    };
    assert_eq!(
        book.apply(&meta(Some(1)), &end),
        Err(BookError::NoSnapshotInProgress)
    );
}

/// Asserts `json` is refused with `err`.
fn refused(json: &str, err: DecodeError) {
    assert_eq!(
        decode_orderbook(json.as_bytes(), &specs()).map(|_| ()),
        Err(err),
        "{json}"
    );
}

#[test]
fn malformed_rest_orderbook_responses_are_refused_naming_the_part() {
    let m = DecodeError::Malformed;
    refused("not json", m("orderbook response is not JSON"));
    refused("[]", m("orderbook response is not an object"));
    let ok = r#""last_updated_at": 1, "seq_no": 7, "bids": [], "asks": []"#;
    refused(&format!("{{{ok}}}"), m("market"));
    refused(
        &format!(r#"{{"market": "SOL-USD-PERP", {ok}}}"#),
        DecodeError::UnknownInstrument,
    );
    let with = |fields: &str| format!(r#"{{"market": "BTC-USD-PERP", {fields}}}"#);
    refused(
        &with(r#""last_updated_at": 1, "bids": [], "asks": []"#),
        m("seq_no"),
    );
    refused(
        &with(r#""last_updated_at": 1, "seq_no": -1, "bids": [], "asks": []"#),
        m("seq_no"),
    );
    refused(
        &with(r#""seq_no": 7, "bids": [], "asks": []"#),
        m("last_updated_at"),
    );
    refused(
        &with(r#""last_updated_at": 9223372036854775807, "seq_no": 7, "bids": [], "asks": []"#),
        m("last_updated_at"),
    );
    let head = r#""last_updated_at": 1, "seq_no": 7"#;
    refused(&with(&format!(r#"{head}, "asks": []"#)), m("bids"));
    refused(&with(&format!(r#"{head}, "bids": []"#)), m("asks"));
    refused(
        &with(&format!(r#"{head}, "bids": {{}}, "asks": []"#)),
        m("bids"),
    );
    let bids = |levels: &str| with(&format!(r#"{head}, "bids": [{levels}], "asks": []"#));
    refused(&bids(r#"["1.0"]"#), m("bids: a level is not [price, size]"));
    refused(
        &bids(r#"["1.0", "1", "2"]"#),
        m("bids: a level is not [price, size]"),
    );
    refused(&bids(r#"[1.0, "1"]"#), m("bids: price"));
    refused(&bids(r#"["1.05", "1"]"#), m("bids: price"));
    refused(&bids(r#"["1.0", 1]"#), m("bids: size"));
    refused(&bids(r#"["1.0", "0.0001"]"#), m("bids: size"));
    refused(&bids(r#"["1.0", "-1"]"#), m("bids: size"));
    refused(&bids(r#"["1.0", "x"]"#), m("bids: size"));
    refused(&bids(r#"["1.0", "0"]"#), m("bids: a level of zero size"));
    // Bids run from the highest price down, asks from the lowest up, each price once.
    refused(
        &bids(r#"["1.0", "1"], ["1.1", "1"]"#),
        m("bids: levels out of order"),
    );
    refused(
        &bids(r#"["1.0", "1"], ["1.0", "1"]"#),
        m("bids: levels out of order"),
    );
    let asks = |levels: &str| with(&format!(r#"{head}, "bids": [], "asks": [{levels}]"#));
    refused(
        &asks(r#"["1.1", "1"], ["1.0", "1"]"#),
        m("asks: levels out of order"),
    );
    refused(&asks(r#"["1.0", "x"]"#), m("asks: size"));
    // More levels than depth 15 asked for.
    let sixteen: Vec<String> = (0..16)
        .map(|i| format!(r#"["{}.0", "1"]"#, 100 + i))
        .collect();
    refused(
        &asks(&sixteen.join(", ")),
        m("asks: more levels than depth 15"),
    );
    // Fifteen, and an empty side, decode.
    let fifteen = sixteen[..15].join(", ");
    let snap = decode_orderbook(asks(&fifteen).as_bytes(), &specs()).unwrap();
    assert_eq!((snap.bids.len(), snap.asks.len()), (0, 15));
}

fn meta(seq: Option<u64>) -> VenueMeta {
    VenueMeta {
        exch_ts: Some(ExchNs(1)),
        exch_ts_kind: ExchTsKind::Publish,
        venue_seq: seq,
    }
}

/// A deterministic generator (an LCG) of the book's deltas and the bbo samples a correct venue
/// would send with them, from its own model of the book.
struct Gen {
    state: u64,
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
}

impl Gen {
    /// Starts from `snap`'s levels.
    fn new(snap: &OrderbookSnapshot) -> Gen {
        let side = |levels: &[Lvl]| levels.iter().map(|l| (l.px.0, l.qty.get())).collect();
        Gen {
            state: 0x9cf,
            bids: side(&snap.bids),
            asks: side(&snap.asks),
        }
    }

    fn next(&mut self, n: u64) -> i64 {
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((self.state >> 33) % n) as i64
    }

    /// One delta at `seq` near the touch, never crossing: a bid between 61999.0 and 62000.4 or
    /// an ask between 62000.5 and 62001.9, set to 0 to 5 tenths (0 removes it).
    fn delta(&mut self, seq: u64) -> (VenueMeta, MdEvent) {
        let (side, px) = match self.next(2) {
            0 => (BookSide::Bid, 619_990 + self.next(15)),
            _ => (BookSide::Ask, 620_005 + self.next(15)),
        };
        let qty = self.next(6) * 100;
        let levels = match side {
            BookSide::Bid => &mut self.bids,
            BookSide::Ask => &mut self.asks,
        };
        if qty == 0 {
            levels.remove(&px);
        } else {
            levels.insert(px, qty);
        }
        let ev = MdEvent::Level {
            inst: BTC,
            book: DELTAS,
            side,
            px: Ticks(px),
            qty: Lots::new(qty).unwrap(),
        };
        (meta(Some(seq)), ev)
    }

    /// The bbo sample at `seq` from the model's touch.
    fn bbo(&self, seq: u64) -> (VenueMeta, MdEvent) {
        let best = |l: Option<(&i64, &i64)>| l.map(|(px, qty)| lvl(*px, *qty));
        let ev = MdEvent::Touch {
            inst: BTC,
            bid: best(self.bids.last_key_value()),
            ask: best(self.asks.first_key_value()),
            source: BBO,
        };
        (meta(Some(seq)), ev)
    }
}

#[test]
fn touch_agreement_over_1000_generated_bbo_samples_reports_99_9_percent_and_names_the_planted_one()
{
    // bbo shares the book's sequence (0022), so a sample is compared at its equal seq_no.
    let domain = caps().md.touch_sources[usize::from(BBO.0)].seq_domain;
    assert_eq!(domain, SeqDomain::SharedWithBook);

    let snap = decode_orderbook(rest_text().as_bytes(), &specs()).unwrap();
    let mut book = delta_book(&FRAMES_TO_2002);
    let mut generator = Gen::new(&snap);
    let mut bbo = Vec::new();
    for seq in 2003..3003 {
        let (meta, ev) = generator.delta(seq);
        book.apply(&meta, &ev).unwrap();
        bbo.push(generator.bbo(seq));
    }
    // A trade on the bbo connection is not a sample.
    let trade = MdEvent::Trade {
        inst: BTC,
        id: None,
        aggressor: Aggressor::Buyer,
        px: Ticks(620_005),
        qty: Lots::new(1).unwrap(),
    };
    bbo.insert(10, (meta(Some(2012)), trade));
    // The planted disagreement: sample 617 (seq_no 2620) reports one lot more on its bid.
    let planted = 617;
    let (meta_617, mut ev) = bbo[planted + 1];
    let MdEvent::Touch { bid, .. } = &mut ev else {
        unreachable!()
    };
    let true_bid = *bid;
    let more = true_bid.map(|l| Lvl {
        px: l.px,
        qty: l.qty.checked_add(Lots::new(1).unwrap()).unwrap(),
    });
    *bid = more;
    bbo[planted + 1] = (meta_617, ev);

    let agreement = touch_agreement(domain, &book, &bbo).unwrap();
    assert_eq!(agreement.samples, 1000);
    assert_eq!(agreement.agreeing(), 999);
    assert_eq!(agreement.percent(), 99.9);
    let MdEvent::Touch { ask, .. } = ev else {
        unreachable!()
    };
    assert_eq!(
        agreement.disagreements,
        [Disagreement {
            sample: planted,
            seq_no: Some(2003 + planted as u64),
            bbo: Touch { bid: more, ask },
            book: Some(Touch { bid: true_bid, ask }),
        }]
    );
    assert_eq!(
        agreement.to_string(),
        "999 of 1000 bbo samples agree (99.9%)"
    );
}

#[test]
fn a_bbo_sample_with_no_book_at_its_seq_no_disagrees_and_other_domains_are_refused() {
    let book = delta_book(&FRAMES_TO_2002);
    let touch = |seq| {
        let ev = MdEvent::Touch {
            inst: BTC,
            bid: None,
            ask: None,
            source: BBO,
        };
        (meta(seq), ev)
    };
    // Before the book's first seq_no, and with no seq_no at all.
    let samples = [touch(Some(1999)), touch(None)];
    let agreement = touch_agreement(SeqDomain::SharedWithBook, &book, &samples).unwrap();
    assert_eq!(agreement.agreeing(), 0);
    assert_eq!(agreement.percent(), 0.0);
    let none = Touch {
        bid: None,
        ask: None,
    };
    assert_eq!(
        agreement.disagreements,
        [
            Disagreement {
                sample: 0,
                seq_no: Some(1999),
                bbo: none,
                book: None
            },
            Disagreement {
                sample: 1,
                seq_no: None,
                bbo: none,
                book: None
            },
        ]
    );
    // No samples: nothing to agree with.
    let agreement = touch_agreement(SeqDomain::SharedWithBook, &book, &[]).unwrap();
    assert_eq!(agreement.percent(), 0.0);
    // A bbo with its own sequence, or none, has no sample point in the book's.
    for domain in [SeqDomain::Own, SeqDomain::None] {
        let err = touch_agreement(domain, &book, &samples).unwrap_err();
        assert_eq!(err, OracleError::NoSamplePoint(domain));
    }
    assert_eq!(
        OracleError::NoSamplePoint(SeqDomain::Own).to_string(),
        "no sample point in the book's sequence for a bbo in Own"
    );
}
