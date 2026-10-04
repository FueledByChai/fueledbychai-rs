//! FBC-z0l's done line, the decode half: the hand-written bookTicker and partial-depth fixtures
//! decode through the core's market-data dispatch into the expected touches and complete book
//! snapshots, with Binance's update id as the venue sequence and its transaction time as the
//! exchange time; subscription replies are consumed; and a frame for an instrument missing from
//! the spec table, or any other frame the codec cannot read, is refused with nothing pushed.

mod common;

use std::collections::BTreeSet;

use common::{BTC, ETH, Sink, config, fixture, specs};
use fbc_core::{
    BookSide, DecodeError, Effects, EndpointPlan, ExchNs, ExchTsKind, Feed, HttpFailure, HttpTag,
    Lots, Lvl, MdCodec, MdEvent, MonoNs, RawFrame, SpecTable, Subscription, Ticks, TimerTag,
    VenueFactory, VenueMeta, WallNs, dispatch_market_data,
};
use fbc_venue_binance_usdm::{BOOK_PARTIAL, BinanceUsdm, TOUCH_BOOK_TICKER};

fn plan() -> EndpointPlan {
    let subs: BTreeSet<_> = [BTC, ETH]
        .into_iter()
        .flat_map(|inst| {
            [Feed::Touch(TOUCH_BOOK_TICKER), Feed::Book(BOOK_PARTIAL)]
                .map(|feed| Subscription { inst, feed })
        })
        .collect();
    BinanceUsdm
        .plan_md(&config(), &specs(), &subs)
        .unwrap()
        .remove(0)
}

/// A codec that has subscribed its plan (request id 1).
fn subscribed() -> Box<dyn MdCodec> {
    let plan = plan();
    let mut codec = BinanceUsdm.md_codec(&config(), &plan);
    let mut fx = Effects::new();
    codec.subscribe(&plan.subs, &[], &specs(), &mut fx).unwrap();
    codec
}

/// Decodes `frame` through the core's market-data dispatch, under Binance's own caps.
fn decode(
    codec: &mut dyn MdCodec,
    frame: &str,
    specs: &SpecTable,
) -> (Result<(), DecodeError>, Sink, Effects) {
    let caps = BinanceUsdm.caps(&config()).unwrap();
    let (mut sink, mut fx) = (Sink::default(), Effects::new());
    let result = dispatch_market_data(&caps, |scope| {
        codec.on_frame(RawFrame::Text(frame), scope, specs, &mut sink, &mut fx)
    });
    (result, sink, fx)
}

fn lvl(px: i64, qty: i64) -> Lvl {
    Lvl {
        px: Ticks(px),
        qty: Lots::new(qty).unwrap(),
    }
}

fn meta(u: u64, t_ms: i64) -> VenueMeta {
    VenueMeta {
        exch_ts: Some(ExchNs(t_ms * 1_000_000)),
        exch_ts_kind: ExchTsKind::MatchingEngine,
        venue_seq: Some(u),
    }
}

#[test]
fn book_ticker_frames_decode_into_touches_with_their_update_id_and_transaction_time() {
    let mut codec = subscribed();
    let frames = fixture("book_ticker.jsonl");
    let expected = [
        (
            meta(400_900_217, 1_568_014_460_891),
            MdEvent::Touch {
                inst: BTC,
                bid: Some(lvl(2_535_190, 31_210)),
                ask: Some(lvl(2_536_520, 40_660)),
                source: TOUCH_BOOK_TICKER,
            },
        ),
        (
            meta(400_900_218, 1_568_014_460_899),
            MdEvent::Touch {
                inst: ETH,
                bid: Some(lvl(182_015, 12_500)),
                ask: Some(lvl(182_016, 4)),
                source: TOUCH_BOOK_TICKER,
            },
        ),
    ];
    assert_eq!(frames.len(), expected.len());
    for (frame, want) in frames.iter().zip(expected) {
        let (result, sink, fx) = decode(codec.as_mut(), frame, &specs());
        assert_eq!(result, Ok(()));
        assert_eq!(sink.0, [want]);
        assert!(fx.is_empty());
    }
}

#[test]
fn a_book_ticker_side_with_no_quantity_is_an_empty_side() {
    let mut codec = subscribed();
    let frame = fixture("book_ticker.jsonl")[0].replace(r#""B":"31.210""#, r#""B":"0""#);
    let (result, sink, _) = decode(codec.as_mut(), &frame, &specs());
    assert_eq!(result, Ok(()));
    let [(_, MdEvent::Touch { bid, ask, .. })] = sink.0[..] else {
        panic!("{:?}", sink.0)
    };
    assert_eq!((bid, ask), (None, Some(lvl(2_536_520, 40_660))));
}

#[test]
fn partial_depth_frames_decode_into_complete_snapshots_of_their_book_channel() {
    let mut codec = subscribed();
    let frames = fixture("partial_depth.jsonl");
    let level = |inst, side, px, qty| MdEvent::Level {
        inst,
        book: BOOK_PARTIAL,
        side,
        px: Ticks(px),
        qty: Lots::new(qty).unwrap(),
    };
    let (bid, ask) = (BookSide::Bid, BookSide::Ask);
    let btc = vec![
        MdEvent::BookSnapshotBegin {
            inst: BTC,
            book: BOOK_PARTIAL,
            epoch: 1,
        },
        level(BTC, bid, 740_543, 2_562),
        level(BTC, bid, 740_485, 5_239),
        level(BTC, bid, 740_400, 1_428),
        level(BTC, bid, 740_390, 3_906),
        level(BTC, bid, 740_389, 2),
        level(BTC, ask, 740_596, 3_340),
        level(BTC, ask, 740_663, 4_525),
        level(BTC, ask, 740_708, 2_475),
        level(BTC, ask, 740_715, 4_800),
        level(BTC, ask, 740_720, 175),
        MdEvent::Window {
            inst: BTC,
            book: BOOK_PARTIAL,
            lo: Ticks(740_389),
            hi: Ticks(740_720),
        },
        MdEvent::BookSnapshotEnd {
            inst: BTC,
            book: BOOK_PARTIAL,
        },
    ];
    let eth = vec![
        MdEvent::BookSnapshotBegin {
            inst: ETH,
            book: BOOK_PARTIAL,
            epoch: 1,
        },
        level(ETH, bid, 18_195, 10_500),
        level(ETH, bid, 18_194, 2_000),
        level(ETH, ask, 18_196, 7_250),
        MdEvent::Window {
            inst: ETH,
            book: BOOK_PARTIAL,
            lo: Ticks(18_194),
            hi: Ticks(18_196),
        },
        MdEvent::BookSnapshotEnd {
            inst: ETH,
            book: BOOK_PARTIAL,
        },
    ];
    for (frame, (want, m)) in frames.iter().zip([
        (btc, meta(390_497_878, 1_571_889_248_276)),
        (eth, meta(390_497_901, 1_571_889_248_376)),
    ]) {
        let (result, sink, fx) = decode(codec.as_mut(), frame, &specs());
        assert_eq!(result, Ok(()));
        assert!(fx.is_empty());
        assert!(sink.0.iter().all(|(got, _)| *got == m), "{:?}", sink.0);
        let events: Vec<_> = sink.0.into_iter().map(|(_, ev)| ev).collect();
        assert_eq!(events, want);
    }

    // Each message replaces the book whole, under the instrument's next epoch.
    let (result, sink, _) = decode(codec.as_mut(), &frames[0], &specs());
    assert_eq!(result, Ok(()));
    assert_eq!(
        sink.0[0].1,
        MdEvent::BookSnapshotBegin {
            inst: BTC,
            book: BOOK_PARTIAL,
            epoch: 2,
        }
    );
    // An empty book is a snapshot with no levels and no window.
    let empty = frames[1]
        .replace(r#"[["181.95","10.500"],["181.94","2.000"]]"#, "[]")
        .replace(r#"[["181.96","7.250"]]"#, "[]");
    let (result, sink, _) = decode(codec.as_mut(), &empty, &specs());
    assert_eq!(result, Ok(()));
    let events: Vec<_> = sink.0.into_iter().map(|(_, ev)| ev).collect();
    assert_eq!(
        events,
        [
            MdEvent::BookSnapshotBegin {
                inst: ETH,
                book: BOOK_PARTIAL,
                epoch: 2,
            },
            MdEvent::BookSnapshotEnd {
                inst: ETH,
                book: BOOK_PARTIAL,
            },
        ]
    );
}

#[test]
fn a_frame_for_an_instrument_missing_from_the_spec_table_is_refused_with_nothing_pushed() {
    let mut codec = subscribed();
    let mut only_eth = SpecTable::new();
    only_eth.insert(specs().get(ETH).unwrap().clone());
    for name in ["book_ticker.jsonl", "partial_depth.jsonl"] {
        let frame = &fixture(name)[0];
        let (result, sink, fx) = decode(codec.as_mut(), frame, &only_eth);
        assert_eq!(result, Err(DecodeError::UnknownInstrument), "{name}");
        assert!(sink.0.is_empty() && fx.is_empty(), "{name}");
    }
    // A refused snapshot takes no epoch: the next one for the instrument is still its first.
    let (result, sink, _) = decode(codec.as_mut(), &fixture("partial_depth.jsonl")[0], &specs());
    assert_eq!(result, Ok(()));
    assert!(matches!(
        sink.0[0].1,
        MdEvent::BookSnapshotBegin { epoch: 1, .. }
    ));
}

#[test]
fn subscription_replies_are_consumed_and_a_refusal_is_an_error() {
    let mut codec = subscribed();
    let replies = fixture("replies.jsonl");
    // The reply to request 1, the subscribe: consumed, nothing pushed.
    let (result, sink, fx) = decode(codec.as_mut(), &replies[0], &specs());
    assert_eq!(result, Ok(()));
    assert!(sink.0.is_empty() && fx.is_empty());
    // Request 1 is answered; a second reply to it answers nothing.
    let (result, _, _) = decode(codec.as_mut(), &replies[0], &specs());
    assert_eq!(
        result,
        Err(DecodeError::Malformed("a reply to no pending request"))
    );

    // The venue refuses request 2, an unsubscribe.
    let mut fx = Effects::new();
    let eth = Subscription {
        inst: ETH,
        feed: Feed::Book(BOOK_PARTIAL),
    };
    codec.subscribe(&[], &[eth], &specs(), &mut fx).unwrap();
    let (result, sink, _) = decode(codec.as_mut(), &replies[1], &specs());
    assert_eq!(
        result,
        Err(DecodeError::Malformed(
            "the venue refused a subscription request"
        ))
    );
    assert!(sink.0.is_empty());
    for (frame, why) in [
        (
            r#"{"code":3,"msg":"Invalid JSON"}"#,
            "the venue refused a subscription request",
        ),
        (r#"{"result":null}"#, "a reply without its id"),
        (
            r#"{"result":["x"],"id":9}"#,
            "a reply to no pending request",
        ),
    ] {
        let (result, sink, _) = decode(codec.as_mut(), frame, &specs());
        assert_eq!(result, Err(DecodeError::Malformed(why)), "{frame}");
        assert!(sink.0.is_empty());
    }
    // A reply with a result other than null to a pending request is not a subscribe's reply.
    codec.subscribe(&[eth], &[], &specs(), &mut fx).unwrap();
    let (result, _, _) = decode(codec.as_mut(), r#"{"result":[],"id":3}"#, &specs());
    assert_eq!(
        result,
        Err(DecodeError::Malformed("a reply without its result"))
    );
}

#[test]
fn frames_the_codec_cannot_read_are_refused_with_nothing_pushed() {
    let ticker = &fixture("book_ticker.jsonl")[0];
    let depth = &fixture("partial_depth.jsonl")[0];
    let cases: Vec<(String, &str)> = vec![
        ("not json".into(), "not JSON"),
        (r#"[1,2]"#.into(), "not a JSON object"),
        (r#"{"stream":1,"data":{}}"#.into(), "stream"),
        (r#"{"stream":"btcusdt@bookTicker"}"#.into(), "data"),
        (r#"{"stream":"btcusdt","data":{}}"#.into(), "stream"),
        (
            ticker.replace("btcusdt@bookTicker", "btcusdt@aggTrade"),
            "a stream this codec did not subscribe",
        ),
        (
            depth.replace("btcusdt@depth5@100ms", "btcusdt@depth10@100ms"),
            "a stream this codec did not subscribe",
        ),
        (
            ticker.replace(r#""e":"bookTicker""#, r#""e":"depthUpdate""#),
            "e",
        ),
        (
            depth.replace(r#""e":"depthUpdate""#, r#""e":"bookTicker""#),
            "e",
        ),
        (ticker.replace(r#""s":"BTCUSDT""#, r#""s":1"#), "s"),
        (
            ticker.replace("btcusdt@", "ethusdt@"),
            "the stream names another symbol",
        ),
        (ticker.replace(r#""u":400900217"#, r#""u":-1"#), "u"),
        (ticker.replace(r#""T":1568014460891"#, r#""T":"x""#), "T"),
        (
            ticker.replace(r#""T":1568014460891"#, r#""T":9223372036855"#),
            "T",
        ),
        (ticker.replace(r#""b":"25351.90""#, r#""b":25351.9"#), "b"),
        (
            ticker.replace(r#""b":"25351.90""#, r#""b":"25351.905""#),
            "b",
        ),
        (ticker.replace(r#""b":"25351.90""#, r#""b":"1e3""#), "b"),
        (ticker.replace(r#""B":"31.210""#, r#""B":"31.2101""#), "B"),
        (ticker.replace(r#""B":"31.210""#, r#""B":"-1""#), "B"),
        (ticker.replace(r#""A":"40.660""#, r#""A":"x""#), "A"),
        (
            ticker.replace(r#""A":"40.660""#, r#""A":"99999999999999999.999""#),
            "A",
        ),
        (depth.replace(r#""b":[["7405.43""#, r#""b":[[7405.43"#), "b"),
        (
            depth.replace(r#""b":[["7405.43","2.562"],"#, r#""b":[["7405.43"],"#),
            "b",
        ),
        (depth.replace(r#""a":[["7405.96""#, r#""a":[["x""#), "a"),
        (depth.replace(r#""a":"#, r#""x":"#), "a"),
        (
            depth.replace(r#""b":[["#, r#""b":[["1.00","1.000"],["#),
            "b: more levels than the channel carries",
        ),
        (
            depth.replace(r#""a":[["#, r#""a":[["9999.00","1.000"],["#),
            "a: more levels than the channel carries",
        ),
    ];
    let mut codec = subscribed();
    for (frame, why) in cases {
        let (result, sink, fx) = decode(codec.as_mut(), &frame, &specs());
        assert_eq!(result, Err(DecodeError::Malformed(why)), "{frame}");
        assert!(sink.0.is_empty() && fx.is_empty(), "{frame}");
    }
    let caps = BinanceUsdm.caps(&config()).unwrap();
    let (mut sink, mut fx) = (Sink::default(), Effects::new());
    let binary = dispatch_market_data(&caps, |scope| {
        codec.on_frame(
            RawFrame::Binary(ticker.as_bytes()),
            scope,
            &specs(),
            &mut sink,
            &mut fx,
        )
    });
    assert_eq!(binary, Err(DecodeError::Malformed("a binary frame")));
    assert!(sink.0.is_empty());
}

#[test]
fn a_codec_whose_depth_configuration_was_refused_reads_no_depth_frame() {
    let mut cfg = config();
    cfg.insert(fbc_venue_binance_usdm::KEY_DEPTH_SPEED, "1s");
    let mut codec = BinanceUsdm.md_codec(&cfg, &plan());
    let depth = &fixture("partial_depth.jsonl")[0];
    let (result, sink, _) = decode(codec.as_mut(), depth, &specs());
    assert_eq!(
        result,
        Err(DecodeError::Malformed(
            "a stream this codec did not subscribe"
        ))
    );
    assert!(sink.0.is_empty());
}

#[test]
fn without_a_diff_depth_book_no_http_is_awaited_and_timers_do_nothing() {
    let mut codec = subscribed();
    let caps = BinanceUsdm.caps(&config()).unwrap();
    let (mut sink, mut fx) = (Sink::default(), Effects::new());
    let result = dispatch_market_data(&caps, |scope| {
        codec.on_http(
            HttpTag(1),
            Err(HttpFailure::TimedOut),
            scope,
            &specs(),
            &mut sink,
            &mut fx,
        )
    });
    assert_eq!(
        result,
        Err(DecodeError::Malformed(
            "a response to no pending snapshot request"
        ))
    );
    codec.on_timer(TimerTag(1), MonoNs(1), WallNs(1), &mut sink, &mut fx);
    assert!(sink.0.is_empty() && fx.is_empty());
}
