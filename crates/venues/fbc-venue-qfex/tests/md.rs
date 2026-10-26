use std::collections::BTreeSet;

use fbc_book::{Books, Top};
use fbc_core::{
    Aggressor, AssetSym, BookSide, Cadence, DecodeError, Effect, Effects, ExchNs, ExchTsKind, Feed,
    FeedHealth, FundingSpec, HttpFailure, HttpTag, Inbound, InboundSpans, InstrumentId,
    InstrumentKind, InstrumentSpec, Lots, Lvl, MdCodec, MdEvent, MdSink, MdTransport, MonoNs,
    PriceGrid, RawFrame, Readiness, Secrets, SizeStep, SpecTable, Subscription, Ticks, TimerTag,
    TradingStatus, UnderlyingId, VenueConfig, VenueError, VenueFactory, VenueId, VenueMeta, WallNs,
    dispatch_market_data,
};
use fbc_runtime::{
    Connector, IngestClock, Liveness, MdSession, MdSessionConfig, ProxyConfig, RateLimiter,
    ReconnectPacing, SafetyReserve, WriteStall,
};
use fbc_venue_qfex::{BOOK_LEVEL2, KEY_MDS_URL, Qfex, TOUCH_BBO};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde_json::{Value, json};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

const AAPL: InstrumentId = InstrumentId::new(1);

#[test]
fn bbo_cadence_remains_unpromised_by_the_venue() {
    let caps = Qfex.caps(&cfg()).unwrap();
    assert_eq!(caps.md.touch_sources[0].cadence, Cadence::Unknown);
    assert_eq!(
        caps.md.books[0].cadence,
        Cadence::Pulsed(std::time::Duration::from_millis(500))
    );
}

#[test]
fn public_mds_session_starts_without_a_provider_user_key() {
    let cfg = cfg();
    let caps = Qfex.caps(&cfg).unwrap();
    let reserve = SafetyReserve::percent(20).unwrap();
    let limiter = RateLimiter::new(&caps.limits, reserve).unwrap();
    let subs = [sub(Feed::Trades)].into_iter().collect();
    let plan = Qfex.plan_md(&cfg, &specs(), &subs).unwrap().remove(0);
    let config = MdSessionConfig {
        venue: &Qfex,
        cfg,
        plan,
        specs: specs(),
        connector: Connector::new(ProxyConfig::Direct),
        pacing: ReconnectPacing::new(
            std::time::Duration::from_millis(10),
            std::time::Duration::from_millis(100),
            3,
            std::time::Duration::from_secs(10),
            std::time::Duration::from_secs(5),
        )
        .unwrap(),
        clock: IngestClock::new(),
        http_max_body: 1024,
        conn: 1,
        limiter,
        liveness: Liveness::new(
            std::time::Duration::MAX,
            std::time::Duration::from_millis(1),
        )
        .unwrap(),
        write_stall: WriteStall::new(std::time::Duration::from_secs(10)).unwrap(),
    };
    assert!(MdSession::new(config, |_: fbc_core::Envelope<MdEvent>| {}).is_ok());
}

fn cfg() -> VenueConfig {
    let mut cfg = VenueConfig::new();
    cfg.insert(KEY_MDS_URL, "wss://mds.qfex.com");
    cfg
}

fn specs() -> SpecTable {
    specs_for("AAPL-USD")
}

fn specs_for(symbol: &str) -> SpecTable {
    let caps = Qfex.caps(&cfg()).unwrap();
    let venue_symbol = dispatch_market_data(&caps, |scope| scope.venue_symbol(symbol)).unwrap();
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
        sessions: None,
        signing: None,
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
    subscribed_for(&specs())
}

fn subscribed_for(specs: &SpecTable) -> Box<dyn MdCodec> {
    let set: BTreeSet<_> = [
        Feed::Touch(TOUCH_BBO),
        Feed::Book(BOOK_LEVEL2),
        Feed::Trades,
    ]
    .map(sub)
    .into_iter()
    .collect();
    let ep = Qfex.plan_md(&cfg(), specs, &set).unwrap().remove(0);
    let mut codec = Qfex.md_codec(&cfg(), &ep);
    codec
        .subscribe(&ep.subs, &[], specs, &mut Effects::new())
        .unwrap();
    codec
}

fn decode(codec: &mut dyn MdCodec, frame: &str) -> (Result<(), DecodeError>, Sink) {
    decode_with_specs(codec, frame, &specs())
}

fn decode_with_specs(
    codec: &mut dyn MdCodec,
    frame: &str,
    specs: &SpecTable,
) -> (Result<(), DecodeError>, Sink) {
    let mut sink = Sink::default();
    let caps = Qfex.caps(&cfg()).unwrap();
    let result = dispatch_market_data(&caps, |scope| {
        codec.on_frame(
            RawFrame::Text(frame),
            scope,
            specs,
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
        "array" => include_str!("../../../../fixtures/qfex/md/array.json"),
        "contents" => include_str!("../../../../fixtures/qfex/md/contents.json"),
        _ => unreachable!(),
    }
}

#[test]
fn array_and_contents_envelopes_decode_in_order_and_reject_partial_frames() {
    let mut singles = subscribed();
    let mut expected = Vec::new();
    for name in ["bbo", "level2", "trade"] {
        let (result, events) = decode(singles.as_mut(), frame(name));
        result.unwrap();
        expected.extend(events.0);
    }
    let (result, events) = decode(subscribed().as_mut(), frame("array"));
    result.unwrap();
    assert_eq!(events.0, expected);

    let mut singles = subscribed();
    let mut expected = Vec::new();
    for name in ["bbo", "trade"] {
        let (result, events) = decode(singles.as_mut(), frame(name));
        result.unwrap();
        expected.extend(events.0);
    }
    let (result, events) = decode(subscribed().as_mut(), frame("contents"));
    result.unwrap();
    assert_eq!(events.0, expected);

    let mut bad_contents: Value = serde_json::from_str(frame("contents")).unwrap();
    bad_contents["contents"][1]["size"] = json!("0.000000001");
    let (result, events) = decode(subscribed().as_mut(), &bad_contents.to_string());
    assert_eq!(result, Err(DecodeError::Malformed("size")));
    assert!(events.0.is_empty());

    let mut bad_trade: Value = serde_json::from_str(frame("trade")).unwrap();
    bad_trade["size"] = json!("0.000000001");
    let level2: Value = serde_json::from_str(frame("level2")).unwrap();
    let mut codec = subscribed();
    let (result, events) = decode(codec.as_mut(), &json!([level2, bad_trade]).to_string());
    assert_eq!(result, Err(DecodeError::Malformed("size")));
    assert!(events.0.is_empty());
    let transient = json!({"err": {"error_code": "RateLimited"}});
    let mut effects = Effects::new();
    let mut sink = Sink::default();
    let bad_batch = json!([transient, bad_trade]);
    let caps = Qfex.caps(&cfg()).unwrap();
    let result = dispatch_market_data(&caps, |scope| {
        codec.on_frame(
            RawFrame::Text(&bad_batch.to_string()),
            scope,
            &specs(),
            &mut sink,
            &mut effects,
        )
    });
    assert_eq!(result, Err(DecodeError::Malformed("size")));
    assert!(sink.0.is_empty());
    assert!(effects.as_slice().is_empty());
    let (result, after) = decode(codec.as_mut(), frame("level2"));
    result.unwrap();
    let (result, fresh) = decode(subscribed().as_mut(), frame("level2"));
    result.unwrap();
    assert_eq!(after.0, fresh.0);
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
    let trade_id = dispatch_market_data(&Qfex.caps(&cfg()).unwrap(), |scope| {
        scope
            .trade_id_uuid("ac928c66-ca53-498f-9c13-a110027a60e8")
            .unwrap()
    });
    assert_eq!(
        sink.0[0].1,
        MdEvent::Trade {
            inst: AAPL,
            id: Some(trade_id),
            aggressor: Aggressor::Unknown,
            px: Ticks(40_023),
            qty: Lots::new(523_512_000).unwrap()
        }
    );
    assert_eq!(sink.0[0].0.venue_seq, Some(50));
    assert!(sink.0[0].0.exch_ts.is_some());
}

#[test]
fn recorded_public_mds_establishes_cadence_sequence_replacement_and_trade_identity() {
    let frames: Vec<Value> = include_str!("../../../../fixtures/qfex/live/nvda-2026-10-20.ndjson")
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let raw_frames: Vec<Value> =
        include_str!("../../../../fixtures/qfex/live/nvda-2026-10-20.raw.ndjson")
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
    assert_eq!(frames.len(), raw_frames.len());
    assert!(frames.len() >= 100);
    for (projected, recorded) in frames.iter().zip(&raw_frames) {
        assert_eq!(projected["recv_ms"], recorded["recv_ms"]);
        let raw: Value = serde_json::from_str(recorded["raw"].as_str().unwrap()).unwrap();
        let mut expected = projected.clone();
        expected.as_object_mut().unwrap().remove("recv_ms");
        assert_eq!(
            raw, expected,
            "projection must retain the public raw frame exactly"
        );
    }
    let specs = specs_for("NVDA-USD");
    let spec = specs.get(AAPL).unwrap();
    let mut codec = subscribed_for(&specs);
    let mut books = Books::new();
    let mut bbo_times = Vec::new();
    let mut depth_times = Vec::new();
    let mut depth_count = 0;
    let mut trade_count = 0;
    let mut seen_trade_ids = Vec::new();
    let mut removed_levels = false;
    let mut prior_top: Option<Top> = None;
    let mut last_bbo = None;

    for (index, frame) in frames.iter().enumerate() {
        let sequence = frame["sequence"].as_u64().unwrap();
        // This capture's sequence is connection-wide: a trade consumes a number between
        // BBO/depth pairs. A +1 rule on either individual channel would be wrong.
        assert_eq!(sequence, index as u64 + 1);
        let timestamp = OffsetDateTime::parse(frame["time"].as_str().unwrap(), &Rfc3339)
            .unwrap()
            .unix_timestamp_nanos();
        let (result, sink) = decode_with_specs(codec.as_mut(), &frame.to_string(), &specs);
        result.unwrap();
        assert!(!sink.0.is_empty());
        assert!(sink.0.iter().all(|(meta, _)| {
            meta.venue_seq == Some(sequence)
                && meta.exch_ts == Some(ExchNs(i64::try_from(timestamp).unwrap()))
        }));

        match frame["type"].as_str().unwrap() {
            "bbo" => {
                bbo_times.push(timestamp);
                assert_eq!(sink.0.len(), 1);
                let MdEvent::Touch { bid, ask, .. } = sink.0[0].1 else {
                    panic!("BBO did not decode to a touch");
                };
                last_bbo = Some((timestamp, bid, ask));
            }
            "level2" => {
                depth_times.push(timestamp);
                depth_count += 1;
                assert_eq!(
                    sink.0.first().unwrap().1,
                    MdEvent::BookSnapshotBegin {
                        inst: AAPL,
                        book: BOOK_LEVEL2,
                        epoch: depth_count,
                    }
                );
                assert_eq!(
                    sink.0.last().unwrap().1,
                    MdEvent::BookSnapshotEnd {
                        inst: AAPL,
                        book: BOOK_LEVEL2,
                    }
                );
                let expected_bids = captured_levels(spec, &frame["bid"]);
                let expected_asks = captured_levels(spec, &frame["ask"]);
                assert_eq!(sink.0.len(), expected_bids.len() + expected_asks.len() + 3);
                for (_, event) in sink.0 {
                    books.apply(&event).unwrap();
                }
                let top = books.get(AAPL, BOOK_LEVEL2).unwrap().top(20).unwrap();
                assert_eq!(top.bids, expected_bids);
                assert_eq!(top.asks, expected_asks);
                let (bbo_time, bid, ask) = last_bbo.unwrap();
                assert_eq!(timestamp, bbo_time);
                assert_eq!(top.bids.first().copied(), bid);
                assert_eq!(top.asks.first().copied(), ask);
                if let Some(old) = &prior_top {
                    removed_levels |= old
                        .bids
                        .iter()
                        .any(|level| !top.bids.iter().any(|new| new.px == level.px))
                        || old
                            .asks
                            .iter()
                            .any(|level| !top.asks.iter().any(|new| new.px == level.px));
                }
                prior_top = Some(top);
            }
            "trade" => {
                trade_count += 1;
                let expected = dispatch_market_data(&Qfex.caps(&cfg()).unwrap(), |scope| {
                    scope
                        .trade_id_uuid(frame["trade_id"].as_str().unwrap())
                        .unwrap()
                });
                assert!(
                    matches!(sink.0[0].1, MdEvent::Trade { id: Some(id), aggressor: Aggressor::Unknown, .. } if id == expected)
                );
                assert!(!seen_trade_ids.contains(&expected));
                seen_trade_ids.push(expected);
                assert_eq!(expected.to_string(), frame["trade_id"].as_str().unwrap());
            }
            other => panic!("unexpected captured feed: {other}"),
        }
    }
    assert!(bbo_times.len() >= 50 && depth_times.len() >= 50 && trade_count >= 1);
    assert!(
        removed_levels,
        "successive depth snapshots must replace changed levels"
    );
    for times in [&bbo_times, &depth_times] {
        let mut gaps: Vec<_> = times
            .windows(2)
            .map(|pair| (pair[1] - pair[0]) / 1_000_000)
            .collect();
        gaps.sort_unstable();
        assert_eq!(gaps[gaps.len() / 2], 500);
        assert!(gaps[0] >= 450 && gaps[gaps.len() - 1] <= 550);
    }
    // A single short recording establishes observed cadence, not a venue guarantee.
    assert_eq!(
        Qfex.caps(&cfg()).unwrap().md.touch_sources[0].cadence,
        Cadence::Unknown
    );
    assert!(Qfex.caps(&cfg()).unwrap().md.trades.trade_id);
}

fn captured_levels(spec: &InstrumentSpec, raw: &Value) -> Vec<Lvl> {
    raw.as_array()
        .unwrap()
        .iter()
        .map(|pair| {
            let price = pair[0].as_str().unwrap().parse().unwrap();
            let qty: Decimal = pair[1].as_str().unwrap().parse().unwrap();
            Lvl {
                px: spec.price_grid.ticks_exact(price).unwrap(),
                qty: Lots::new(
                    qty.checked_div(spec.size_step.get())
                        .unwrap()
                        .to_i64()
                        .unwrap(),
                )
                .unwrap(),
            }
        })
        .collect()
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
    let mut bad_trade: Value = serde_json::from_str(frame("trade")).unwrap();
    bad_trade["trade_id"] = json!("not-a-uuid");
    let (result, sink) = decode(codec.as_mut(), &bad_trade.to_string());
    assert_eq!(result, Err(DecodeError::Malformed("trade_id")));
    assert!(sink.0.is_empty());
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
        for incoming in [
            Some(json!({"type": "subscribe", "channels": ["trade"],
                "symbols": ["AAPL-USD"]})),
            Some(Value::Null),
            None,
        ] {
            let mut codec = subscribed();
            let mut error = json!({"err": {"error_code": code}});
            if let Some(incoming) = incoming {
                error["err"]["incoming_message"] = incoming;
            }
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
            assert_eq!(result, Ok(()), "{error}");
            assert!(sink.0.is_empty(), "{error}");
            assert_eq!(
                fx.as_slice(),
                &[Effect::Reconnect {
                    stream: fbc_core::StreamId(0),
                    reason: "transient QFEX subscription error",
                }],
                "{error}"
            );
            let (result, sink) = decode(codec.as_mut(), frame("trade"));
            assert_eq!(result, Ok(()), "{error}");
            assert_eq!(sink.0.len(), 1, "{error}");
        }
    }
}

#[test]
fn permanent_subscription_error_without_echo_reconnects_instead_of_silencing_feed() {
    for incoming in [None, Some(Value::Null)] {
        let mut codec = subscribed();
        let mut error = json!({"err": {"error_code": "InvalidParameter"}});
        if let Some(incoming) = incoming {
            error["err"]["incoming_message"] = incoming;
        }
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
        assert_eq!(result, Ok(()), "{error}");
        assert!(sink.0.is_empty(), "{error}");
        assert_eq!(
            fx.as_slice(),
            &[Effect::Reconnect {
                stream: fbc_core::StreamId(0),
                reason: "unattributed QFEX error",
            }],
            "{error}"
        );
    }
}

#[test]
fn factory_refuses_bad_endpoints_and_unavailable_feeds_before_planning() {
    assert_eq!(Qfex.config_schema()[0].key, KEY_MDS_URL);
    assert_eq!(
        Qfex.parse_fbc_common_symbol("AAPL/USD"),
        Ok(fbc_core::AssetKey {
            base: AssetSym::new("AAPL").unwrap(),
            quote: AssetSym::new("USD").unwrap(),
            kind: InstrumentKind::Perpetual,
        })
    );
    assert!(matches!(
        Qfex.discover(&cfg()),
        Err(VenueError::Config(fbc_core::ConfigError::Missing(_)))
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
        "wss://-bad.example",
        "wss://bad-.example",
        "wss://bad..example",
        "wss://bad.example.",
        "wss://999.999.999.999",
        "wss://zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz.example",
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
    local.insert(KEY_MDS_URL, "ws://127.0.0.1:9000");
    assert!(Qfex.caps(&local).is_ok());
    local.insert(KEY_MDS_URL, "wss://mds.qfex.com/");
    assert!(Qfex.caps(&local).is_ok());
    let planned = Qfex
        .plan_md(&local, &specs(), &BTreeSet::from([sub(Feed::Trades)]))
        .unwrap();
    assert!(matches!(&planned[0].transport, MdTransport::Socket { url }
        if url == "wss://mds.qfex.com/"));
    local.insert(KEY_MDS_URL, "wss://mds.qfex.io/");
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
fn off_step_quantity_that_division_rounds_to_whole_lots_is_rejected() {
    let mut spec = specs().get(AAPL).unwrap().clone();
    spec.size_step = SizeStep::new("0.00000003".parse().unwrap()).unwrap();
    let mut table = SpecTable::new();
    table.insert(spec);
    let mut trade: Value = serde_json::from_str(frame("trade")).unwrap();
    trade["size"] = json!("6.0000000000000000000000000001");
    let (result, sink) = decode_with_specs(subscribed().as_mut(), &trade.to_string(), &table);
    assert_eq!(result, Err(DecodeError::Malformed("size")));
    assert!(sink.0.is_empty());
}

#[test]
fn rejected_unsubscribe_reconnects_to_reconcile_the_removed_feed() {
    let mut codec = subscribed();
    let mut sent = Effects::new();
    codec
        .subscribe(&[], &[sub(Feed::Trades)], &specs(), &mut sent)
        .unwrap();
    assert!(sent.as_slice().iter().any(|effect| {
        matches!(effect, Effect::Send { frame, .. }
            if serde_json::from_slice::<Value>(frame.bytes()).unwrap()["type"] == "unsubscribe")
    }));
    let error = json!({"err": {"error_code": "InvalidParameter",
        "incoming_message": {"type": "unsubscribe", "channels": ["trade"],
            "symbols": ["AAPL-USD"]}}});
    let mut effects = Effects::new();
    let mut sink = Sink::default();
    let caps = Qfex.caps(&cfg()).unwrap();
    let result = dispatch_market_data(&caps, |scope| {
        codec.on_frame(
            RawFrame::Text(&error.to_string()),
            scope,
            &specs(),
            &mut sink,
            &mut effects,
        )
    });
    assert_eq!(result, Ok(()));
    assert!(sink.0.is_empty());
    assert_eq!(
        effects.as_slice(),
        &[Effect::Reconnect {
            stream: fbc_core::StreamId(0),
            reason: "rejected QFEX unsubscribe",
        }]
    );
}

#[test]
fn malformed_subscription_errors_never_change_feed_state() {
    let mut codec = subscribed();
    let base = json!({"err": {"error_code": "InvalidParameter",
        "incoming_message": {"type": "subscribe", "channels": ["trade"], "symbols": ["AAPL-USD"]}}});
    let mut cases = vec![json!({"err": null}), json!({"err": {}})];
    let mut bad = base.clone();
    bad["err"]["incoming_message"]["type"] = json!("ping");
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
