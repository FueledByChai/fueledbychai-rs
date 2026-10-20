use std::collections::BTreeSet;

use fbc_core::{
    Aggressor, AssetSym, BookSide, Cadence, DecodeError, Effect, Effects, ExchNs, ExchTsKind, Feed,
    FeedHealth, FundingSpec, HttpFailure, HttpTag, Inbound, InboundSpans, InstrumentId,
    InstrumentKind, InstrumentSpec, Lots, Lvl, MdCodec, MdEvent, MdSink, MdTransport, MonoNs,
    PriceGrid, RawFrame, Readiness, Secrets, SizeStep, SpecTable, Subscription, SymbolError, Ticks,
    TimerTag, TradingStatus, UnderlyingId, VenueConfig, VenueError, VenueFactory, VenueId,
    VenueMeta, WallNs, dispatch_market_data,
};
use fbc_venue_qfex::{BOOK_LEVEL2, KEY_MDS_URL, Qfex, TOUCH_BBO};
use rust_decimal::Decimal;
use serde_json::{Value, json};

const AAPL: InstrumentId = InstrumentId::new(1);

#[test]
fn bbo_cadence_remains_unknown_until_measured() {
    let caps = Qfex.caps(&cfg()).unwrap();
    assert_eq!(caps.md.touch_sources[0].cadence, Cadence::Unknown);
    assert_eq!(
        caps.md.books[0].cadence,
        Cadence::Pulsed(std::time::Duration::from_millis(500))
    );
}

fn cfg() -> VenueConfig {
    let mut cfg = VenueConfig::new();
    cfg.insert(KEY_MDS_URL, "wss://mds.qfex.com");
    cfg
}

fn specs() -> SpecTable {
    let caps = Qfex.caps(&cfg()).unwrap();
    let venue_symbol = dispatch_market_data(&caps, |scope| scope.venue_symbol("AAPL-USD")).unwrap();
    let usd = AssetSym::new("USD").unwrap();
    let mut specs = SpecTable::new();
    specs.insert(InstrumentSpec {
        id: AAPL,
        venue: VenueId::new(4),
        venue_symbol,
        native_id: None,
        underlying: UnderlyingId::new(1),
        kind: InstrumentKind::Perpetual,
        price_grid: PriceGrid::fixed("0.01".parse().unwrap()).unwrap(),
        quote_grid: None,
        size_step: SizeStep::new("0.00000001".parse::<Decimal>().unwrap()).unwrap(),
        min_size: Lots::new(1).unwrap(),
        min_notional: None,
        max_order_size: None,
        position_limit: None,
        price_band: None,
        max_open_orders: None,
        multiplier: Decimal::ONE,
        quote_ccy: usd,
        settle_ccy: usd,
        funding: FundingSpec::Unknown,
        public_fees: None,
        status: TradingStatus::Trading,
        version: 1,
        fetched_at: WallNs(0),
    });
    specs
}

fn sub(feed: Feed) -> Subscription {
    Subscription { inst: AAPL, feed }
}

#[derive(Default)]
struct Sink(Vec<(VenueMeta, MdEvent)>);
impl MdSink for Sink {
    fn push(&mut self, meta: VenueMeta, event: MdEvent) {
        self.0.push((meta, event));
    }
}

fn subscribed() -> Box<dyn MdCodec> {
    let set: BTreeSet<_> = [
        Feed::Touch(TOUCH_BBO),
        Feed::Book(BOOK_LEVEL2),
        Feed::Trades,
    ]
    .map(sub)
    .into_iter()
    .collect();
    let ep = Qfex.plan_md(&cfg(), &specs(), &set).unwrap().remove(0);
    let mut codec = Qfex.md_codec(&cfg(), &ep);
    codec
        .subscribe(&ep.subs, &[], &specs(), &mut Effects::new())
        .unwrap();
    codec
}

fn decode(codec: &mut dyn MdCodec, frame: &str) -> (Result<(), DecodeError>, Sink) {
    let mut sink = Sink::default();
    let caps = Qfex.caps(&cfg()).unwrap();
    let result = dispatch_market_data(&caps, |scope| {
        codec.on_frame(
            RawFrame::Text(frame),
            scope,
            &specs(),
            &mut sink,
            &mut Effects::new(),
        )
    });
    (result, sink)
}

fn frame(name: &str) -> &'static str {
    match name {
        "bbo" => include_str!("../../../../fixtures/qfex/md/bbo.json"),
        "level2" => include_str!("../../../../fixtures/qfex/md/level2.json"),
        "trade" => include_str!("../../../../fixtures/qfex/md/trade.json"),
        _ => unreachable!(),
    }
}

#[test]
fn public_factory_plans_one_read_only_mds_connection() {
    let caps = Qfex.caps(&cfg()).unwrap();
    assert_eq!(Qfex.id(), "QFEX");
    assert_eq!(caps.exec, None);
    assert_eq!(caps.readiness_ceiling, Readiness::Record);
    assert!(!caps.md.trades.aggressor);
    assert_eq!(caps.md.books[0].max_depth, 20);
    assert!(Qfex.plan_exec(&cfg()).unwrap().is_empty());
    assert!(Qfex.exec_codec(&cfg(), Secrets::new()).is_none());
    assert!(Qfex.test_connection(&cfg(), Secrets::new()).is_none());
    let subs: BTreeSet<_> = [
        Feed::Touch(TOUCH_BBO),
        Feed::Book(BOOK_LEVEL2),
        Feed::Trades,
    ]
    .map(sub)
    .into_iter()
    .collect();
    let plans = Qfex.plan_md(&cfg(), &specs(), &subs).unwrap();
    assert_eq!(plans.len(), 1);
    assert!(
        matches!(&plans[0].transport, MdTransport::Socket { url } if url == "wss://mds.qfex.com")
    );
    let mut codec = Qfex.md_codec(&cfg(), &plans[0]);
    let mut fx = Effects::new();
    codec
        .subscribe(&plans[0].subs, &[], &specs(), &mut fx)
        .unwrap();
    let sent: BTreeSet<_> = fx
        .as_slice()
        .iter()
        .map(|effect| match effect {
            Effect::Send { frame, .. } => serde_json::from_slice::<Value>(frame.bytes()).unwrap(),
            _ => panic!("not a subscription frame"),
        })
        .map(|v| v["channels"][0].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(
        sent,
        BTreeSet::from(["bbo".to_owned(), "level2".to_owned(), "trade".to_owned()])
    );
    assert_eq!(fx.len(), 3);
}

#[test]
fn documented_bbo_depth_and_trade_frames_decode_exactly() {
    let mut codec = subscribed();
    let meta = VenueMeta {
        exch_ts: Some(ExchNs(1_756_977_999_545_268_322)),
        exch_ts_kind: ExchTsKind::Unknown,
        venue_seq: Some(30_272_539),
    };
    let (result, sink) = decode(codec.as_mut(), frame("bbo"));
    assert_eq!(result, Ok(()));
    assert_eq!(
        sink.0,
        [(
            meta,
            MdEvent::Touch {
                inst: AAPL,
                bid: Some(Lvl {
                    px: Ticks(1_010_110),
                    qty: Lots::new(45_054_140).unwrap()
                }),
                ask: Some(Lvl {
                    px: Ticks(1_010_255),
                    qty: Lots::new(57_753_524).unwrap()
                }),
                source: TOUCH_BBO,
            }
        )]
    );
    let (result, sink) = decode(codec.as_mut(), frame("level2"));
    assert_eq!(result, Ok(()));
    assert!(sink.0.iter().all(|(m, _)| *m == meta));
    let events: Vec<_> = sink.0.iter().map(|(_, e)| *e).collect();
    assert_eq!(
        events[0],
        MdEvent::BookSnapshotBegin {
            inst: AAPL,
            book: BOOK_LEVEL2,
            epoch: 1
        }
    );
    assert_eq!(
        events[1],
        MdEvent::Level {
            inst: AAPL,
            book: BOOK_LEVEL2,
            side: BookSide::Bid,
            px: Ticks(1_010_110),
            qty: Lots::new(45_054_140).unwrap()
        }
    );
    assert_eq!(
        events[5],
        MdEvent::Window {
            inst: AAPL,
            book: BOOK_LEVEL2,
            lo: Ticks(1_010_100),
            hi: Ticks(1_010_265)
        }
    );
    assert_eq!(
        events[6],
        MdEvent::BookSnapshotEnd {
            inst: AAPL,
            book: BOOK_LEVEL2
        }
    );
    let (result, sink) = decode(codec.as_mut(), frame("trade"));
    assert_eq!(result, Ok(()));
    assert_eq!(
        sink.0[0].1,
        MdEvent::Trade {
            inst: AAPL,
            id: None,
            aggressor: Aggressor::Unknown,
            px: Ticks(40_023),
            qty: Lots::new(523_512_000).unwrap()
        }
    );
    assert_eq!(sink.0[0].0.venue_seq, Some(50));
    assert!(sink.0[0].0.exch_ts.is_some());
}

#[test]
fn malformed_or_unsubscribed_frames_never_emit_partial_events() {
    let mut codec = subscribed();
    let book: Value = serde_json::from_str(frame("level2")).unwrap();
    let mut bad = book.clone();
    bad["ask"][1][0] = json!("10102.651"); // off the tick grid, after valid levels
    for raw in [
        bad.to_string(),
        "not json".to_owned(),
        frame("trade").replace("\"side\":\"sell\"", "\"side\":\"neither\""),
    ] {
        let (result, sink) = decode(codec.as_mut(), &raw);
        assert!(result.is_err());
        assert!(sink.0.is_empty());
    }
    // A failed snapshot did not consume an epoch.
    let (_, sink) = decode(codec.as_mut(), frame("level2"));
    assert!(matches!(
        sink.0[0].1,
        MdEvent::BookSnapshotBegin { epoch: 1, .. }
    ));
    let mut fx = Effects::new();
    codec
        .subscribe(&[], &[sub(Feed::Trades)], &specs(), &mut fx)
        .unwrap();
    let (result, sink) = decode(codec.as_mut(), frame("trade"));
    assert_eq!(result, Err(DecodeError::Malformed("an unsubscribed feed")));
    assert!(sink.0.is_empty());
}

#[test]
fn bad_subscription_is_rejected_before_any_frame_is_sent() {
    let mut codec = subscribed();
    let mut fx = Effects::new();
    let err = codec.subscribe(
        &[sub(Feed::Trades), sub(Feed::Funding)],
        &[],
        &specs(),
        &mut fx,
    );
    assert_eq!(err, Err(VenueError::UnsupportedFeed(sub(Feed::Funding))));
    assert!(fx.is_empty());
    let mut config = cfg();
    config.insert(KEY_MDS_URL, "wss://mds.qfex.com/path");
    assert!(Qfex.caps(&config).is_err());
}

#[test]
fn a_rejected_subscription_reports_health_without_retry_or_duplicate_subscribe() {
    let mut codec = subscribed();
    let mut fx = Effects::new();
    codec
        .subscribe(&[sub(Feed::Trades)], &[], &specs(), &mut fx)
        .unwrap();
    assert!(fx.is_empty());
    let error = json!({"err": {"error_code": "InvalidParameter", "message": "bad channel",
        "incoming_message": {"type": "subscribe", "channels": ["trade"], "symbols": ["AAPL-USD"]}}});
    let (result, sink) = decode(codec.as_mut(), &error.to_string());
    assert_eq!(result, Ok(()));
    assert_eq!(
        sink.0,
        [(
            VenueMeta::NONE,
            MdEvent::Health {
                inst: AAPL,
                feed: Feed::Trades,
                h: FeedHealth::Refused,
            }
        )]
    );
    let (result, sink) = decode(codec.as_mut(), frame("trade"));
    assert_eq!(result, Err(DecodeError::Malformed("an unsubscribed feed")));
    assert!(sink.0.is_empty());
}

#[test]
fn transient_subscription_errors_reconnect_without_permanently_refusing_feed() {
    for code in ["RateLimited", "ServerError"] {
        let mut codec = subscribed();
        let error = json!({"err": {"error_code": code,
            "incoming_message": {"type": "subscribe", "channels": ["trade"],
                "symbols": ["AAPL-USD"]}}});
        let mut sink = Sink::default();
        let mut fx = Effects::new();
        let caps = Qfex.caps(&cfg()).unwrap();
        let result = dispatch_market_data(&caps, |scope| {
            codec.on_frame(
                RawFrame::Text(&error.to_string()),
                scope,
                &specs(),
                &mut sink,
                &mut fx,
            )
        });
        assert_eq!(result, Ok(()), "{code}");
        assert!(sink.0.is_empty(), "{code}");
        assert_eq!(
            fx.as_slice(),
            &[Effect::Reconnect {
                stream: fbc_core::StreamId(0),
                reason: "transient QFEX subscription error",
            }],
            "{code}"
        );
        let (result, sink) = decode(codec.as_mut(), frame("trade"));
        assert_eq!(result, Ok(()), "{code}");
        assert_eq!(sink.0.len(), 1, "{code}");
    }
}

#[test]
fn factory_refuses_bad_endpoints_and_unavailable_feeds_before_planning() {
    assert_eq!(Qfex.config_schema()[0].key, KEY_MDS_URL);
    assert_eq!(
        Qfex.parse_fbc_common_symbol("AAPL/USD"),
        Err(SymbolError::NoRule)
    );
    assert!(matches!(
        Qfex.discover(&cfg()),
        Err(VenueError::NoDiscovery)
    ));
    assert!(Qfex.caps(&VenueConfig::new()).is_err());
    for url in [
        "http://mds.qfex.com",
        "ws://",
        "wss://mds.qfex.com/path",
        "wss://user@mds.qfex.com",
        "wss://mds.qfex.com?x=1",
        "wss://[::1",
        "wss://mds.qfex.com:70000",
        "wss://mds.qfex.com:0",
        "wss://mds.qfex.com:",
        "ws://::1",
    ] {
        let mut bad = cfg();
        bad.insert(KEY_MDS_URL, url);
        assert!(
            Qfex.plan_md(&bad, &specs(), &BTreeSet::new()).is_err(),
            "{url}"
        );
    }
    assert!(
        Qfex.plan_md(&cfg(), &specs(), &BTreeSet::new())
            .unwrap()
            .is_empty()
    );
    let mut local = cfg();
    local.insert(KEY_MDS_URL, "ws://[::1]:9000");
    assert!(Qfex.caps(&local).is_ok());
    let unknown = Subscription {
        inst: InstrumentId::new(99),
        feed: Feed::Trades,
    };
    assert_eq!(
        Qfex.plan_md(&cfg(), &specs(), &BTreeSet::from([unknown])),
        Err(VenueError::UnknownInstrument(unknown.inst))
    );
    assert_eq!(
        Qfex.plan_md(&cfg(), &specs(), &BTreeSet::from([sub(Feed::Funding)])),
        Err(VenueError::UnsupportedFeed(sub(Feed::Funding)))
    );
}

#[test]
fn malformed_public_messages_are_atomic_and_a_valid_empty_book_replaces_the_old_window() {
    let mut codec = subscribed();
    let mut cases = vec![
        json!({}),
        json!({"type":"mystery","symbol":"AAPL-USD"}),
        json!({"type":"bbo","symbol":"UNKNOWN-USD"}),
        json!({"type":"bbo","symbol":"AAPL-USD","time":"not a time","bid":[],"ask":[]}),
        json!({"type":"bbo","symbol":"AAPL-USD","time":"2025-09-04T09:26:39Z","sequence":-1,"bid":[],"ask":[]}),
        json!({"type":"bbo","symbol":"AAPL-USD","time":"2025-09-04T09:26:39Z","bid":[["1.00","1.0"],["0.99","1.0"]],"ask":[]}),
    ];
    let book: Value = serde_json::from_str(frame("level2")).unwrap();
    let mut bad = book.clone();
    bad["bid"] = json!([["10101.10", "1.0"], ["10101.10", "2.0"]]);
    cases.push(bad);
    let mut bad = book.clone();
    bad["ask"] = json!([["10102.65", "1.0"], ["10102.55", "2.0"]]);
    cases.push(bad);
    let mut bad = book.clone();
    bad["ask"] = json!([["10100.00", "1.0"]]);
    cases.push(bad);
    let mut bad = book.clone();
    bad["bid"] = Value::Array(vec![json!(["1.00", "1.0"]); 21]);
    cases.push(bad);
    let mut bad = book.clone();
    bad["bid"] = json!([["10101.10"]]);
    cases.push(bad);
    let mut bad = book.clone();
    bad["bid"] = json!([null]);
    cases.push(bad);
    let mut bad = book.clone();
    bad["bid"] = json!([["10101.10", "0"]]);
    cases.push(bad);
    let mut bad = book.clone();
    bad["bid"] = json!([["10101.10", "0.000000001"]]);
    cases.push(bad);
    let mut bad = book.clone();
    bad["bid"] = json!([["x", "1.0"]]);
    cases.push(bad);
    let mut bad = book.clone();
    bad["bid"] = json!([[10101.10, "1.0"]]);
    cases.push(bad);
    let trade: Value = serde_json::from_str(frame("trade")).unwrap();
    let mut bad = trade.clone();
    bad["size"] = json!("0");
    cases.push(bad);
    let mut bad = trade.clone();
    bad["size"] = json!("-1");
    cases.push(bad);
    let mut bad = trade.clone();
    bad["size"] = json!("x");
    cases.push(bad);
    let mut bad = trade.clone();
    bad["price"] = json!("400.231");
    cases.push(bad);
    let mut bad = trade.clone();
    bad["price"] = Value::Null;
    cases.push(bad);
    let mut bad = trade.clone();
    bad["side"] = json!("neither");
    cases.push(bad);
    for case in cases {
        let raw = case.to_string();
        let (result, sink) = decode(codec.as_mut(), &raw);
        assert!(result.is_err(), "{raw}");
        assert!(sink.0.is_empty(), "{raw}");
    }
    let caps = Qfex.caps(&cfg()).unwrap();
    let mut sink = Sink::default();
    let result = dispatch_market_data(&caps, |scope| {
        codec.on_frame(
            RawFrame::Binary(frame("bbo").as_bytes()),
            scope,
            &specs(),
            &mut sink,
            &mut Effects::new(),
        )
    });
    assert_eq!(result, Err(DecodeError::Malformed("binary frame")));
    let mut empty = book;
    empty["bid"] = json!([]);
    empty["ask"] = json!([]);
    let (result, sink) = decode(codec.as_mut(), &empty.to_string());
    assert_eq!(result, Ok(()));
    assert_eq!(sink.0.len(), 2); // begin and end, no levels or window
    assert!(matches!(
        sink.0[0].1,
        MdEvent::BookSnapshotBegin { epoch: 1, .. }
    ));
    let (result, sink) = decode(codec.as_mut(), frame("level2"));
    assert_eq!(result, Ok(()));
    assert!(matches!(
        sink.0[0].1,
        MdEvent::BookSnapshotBegin { epoch: 2, .. }
    ));
    let mut buy = trade;
    buy["side"] = json!("BUY");
    let (result, sink) = decode(codec.as_mut(), &buy.to_string());
    assert_eq!(result, Ok(()));
    assert!(matches!(
        sink.0[0].1,
        MdEvent::Trade {
            aggressor: Aggressor::Unknown,
            ..
        }
    ));
    let mut no_sequence: Value = serde_json::from_str(frame("bbo")).unwrap();
    no_sequence.as_object_mut().unwrap().remove("sequence");
    let (result, sink) = decode(codec.as_mut(), &no_sequence.to_string());
    assert_eq!(result, Ok(()));
    assert_eq!(sink.0[0].0.venue_seq, None);
}

#[test]
fn malformed_subscription_errors_never_change_feed_state() {
    let mut codec = subscribed();
    let base = json!({"err": {"error_code": "InvalidParameter",
        "incoming_message": {"type": "subscribe", "channels": ["trade"], "symbols": ["AAPL-USD"]}}});
    let mut cases = vec![json!({"err": null}), json!({"err": {}})];
    let mut bad = base.clone();
    bad["err"]["incoming_message"] = Value::Null;
    cases.push(bad);
    let mut bad = base.clone();
    bad["err"]["incoming_message"]["type"] = json!("unsubscribe");
    cases.push(bad);
    let mut bad = base.clone();
    bad["err"]["incoming_message"]["channels"] = Value::Null;
    cases.push(bad);
    let mut bad = base.clone();
    bad["err"]["incoming_message"]["symbols"] = Value::Null;
    cases.push(bad);
    let mut bad = base.clone();
    bad["err"]["incoming_message"]["channels"] = json!(["other"]);
    cases.push(bad);
    let mut bad = base.clone();
    bad["err"]["incoming_message"]["symbols"] = json!(["UNKNOWN-USD"]);
    cases.push(bad);
    let mut bad = base.clone();
    bad["err"]["incoming_message"]["symbols"] = json!([]);
    cases.push(bad);
    let mut bad = base.clone();
    bad["err"]["incoming_message"]["symbols"] = json!([1]);
    cases.push(bad);
    for case in cases {
        let raw = case.to_string();
        let (result, sink) = decode(codec.as_mut(), &raw);
        assert!(result.is_err(), "{raw}");
        assert!(sink.0.is_empty(), "{raw}");
    }
    let (result, sink) = decode(codec.as_mut(), frame("trade"));
    assert_eq!(result, Ok(()));
    assert_eq!(sink.0.len(), 1);
    let caps = Qfex.caps(&cfg()).unwrap();
    let result = dispatch_market_data(&caps, |scope| {
        codec.on_http(
            HttpTag(7),
            Err(HttpFailure::TimedOut),
            scope,
            &specs(),
            &mut Sink::default(),
            &mut Effects::new(),
        )
    });
    assert_eq!(
        result,
        Err(DecodeError::Malformed("unexpected HTTP response"))
    );
    codec.on_open(&mut Effects::new());
    codec.on_timer(
        TimerTag(1),
        MonoNs(1),
        WallNs(1),
        &mut Sink::default(),
        &mut Effects::new(),
    );
    assert!(codec.keepalive().is_none());
    assert_eq!(
        codec.redact_inbound(Inbound::Frame(RawFrame::Text(frame("bbo")))),
        InboundSpans::NONE
    );
}
