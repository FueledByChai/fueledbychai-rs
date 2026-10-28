//! FBC-jyyl's done line, the factory half, as FBC-2kmp extends it: Hibachi is declared market
//! data only (`exec: None`, `Readiness::Record`, no order-entry codec or endpoint), its
//! capabilities state the orderbook channel at the configured depth with an unknown cadence, no
//! sequence, a window and brackets only, the `ask_bid_price` touch, the trades with their taker
//! side, the mark and the spot price as the index, the funding stream stating its one-hour
//! interval and no next time (FBC-204c, decision 0131), JSON and
//! one-second timestamps, the REST limit and the taker speed bump; every configuration key is
//! required and checked, the orderbook depth refused when absent or outside 1 to 100 and the
//! resubscribe interval when absent or zero; and `plan_md` plans one `/ws/market` connection
//! carrying every feed the codec decodes.
//!
//! FBC-1632's factory half (decision 0130): `plan_md` also plans the status poll, a poll
//! endpoint on the REST origin carrying the same subscriptions, whose codec is the status poll's;
//! its interval and the window after a reopening are required whole seconds, each with no
//! default and zero refused.

use std::collections::BTreeSet;
use std::time::Duration;

use fbc_core::{
    BookCaps, BookId, Cadence, Channel, ConfigError, ConfigScope, ConnTopology, Continuity,
    Encoding, EndpointPlan, ExchTsKind, Feed, FeedSource, FieldUnit, FundingCaps, InstrumentId,
    LimitScope, MdTransport, OpKind, QueueModelQuality, RateLimit, Readiness, Secrets, SeqDomain,
    SpecTable, SpeedBump, SpeedBumpScope, StpScope, StreamId, Subscription, SymbolError, TagSet,
    TouchSourceCaps, TouchSourceId, TradeCaps, VenueConfig, VenueError, VenueFactory, WireUrl,
};
use fbc_venue_hibachi::{
    BOOK_ORDERBOOK, Hibachi, KEY_AUCTION_HALT, KEY_DATA_REST_URL, KEY_DATA_WS_URL,
    KEY_ORDERBOOK_DEPTH, KEY_ORDERBOOK_RESUBSCRIBE, KEY_REST_TIMEOUT, KEY_STATUS_INTERVAL,
    STATUS_STREAM, TOUCH_ASK_BID_PRICE,
};

const CONFIG: [(&str, &str); 7] = [
    (KEY_DATA_REST_URL, "https://data-api.hibachi.xyz"),
    (KEY_REST_TIMEOUT, "5000ms"),
    (KEY_ORDERBOOK_DEPTH, "25"),
    (KEY_DATA_WS_URL, "wss://data-api.hibachi.xyz"),
    (KEY_ORDERBOOK_RESUBSCRIBE, "2000ms"),
    (KEY_STATUS_INTERVAL, "30s"),
    (KEY_AUCTION_HALT, "900s"),
];

const ETH: InstrumentId = InstrumentId::new(1);

fn config() -> VenueConfig {
    let mut cfg = VenueConfig::new();
    for (key, value) in CONFIG {
        cfg.insert(key, value);
    }
    cfg
}

fn without(key: &str) -> VenueConfig {
    let mut cfg = VenueConfig::new();
    for (k, v) in CONFIG {
        if k != key {
            cfg.insert(k, v);
        }
    }
    cfg
}

fn sub(feed: Feed) -> Subscription {
    Subscription { inst: ETH, feed }
}

fn orderbook(depth: u16) -> BookCaps {
    BookCaps {
        channel: "orderbook",
        max_depth: depth,
        cadence: Cadence::Unknown,
        continuity: Continuity::Unsequenced,
        windowed: true,
        rest_anchor: false,
        includes_channels: TagSet::of(&[Channel::Public]),
        queue_model: QueueModelQuality::BracketOnly,
    }
}

#[test]
fn the_factory_declares_market_data_only_at_record_readiness() {
    let cfg = config();
    let caps = Hibachi.caps(&cfg).unwrap();
    assert_eq!(caps.exec, None);
    assert_eq!(caps.readiness_ceiling, Readiness::Record);
    assert!(Hibachi.exec_codec(&cfg, Secrets::new()).is_none());
    assert!(Hibachi.test_connection(&cfg, Secrets::new()).is_none());
    assert_eq!(Hibachi.plan_exec(&cfg), Ok(Vec::new()));
    assert_eq!(Hibachi.id(), "HIBACHI");
    // Discovery and the ticker rule exist: neither answers that it has none.
    assert!(Hibachi.discover(&cfg).is_ok());
    assert_ne!(
        Hibachi.parse_fbc_common_symbol("ETH/USDT"),
        Err(SymbolError::NoRule)
    );
}

#[test]
fn the_orderbook_channel_is_declared_at_the_configured_depth() {
    for depth in [1_u16, 25, 100] {
        let mut cfg = config();
        cfg.insert(KEY_ORDERBOOK_DEPTH, &depth.to_string());
        let caps = Hibachi.caps(&cfg).unwrap();
        // One book channel, BOOK_ORDERBOOK: max_depth the configured depth, cadence Unknown
        // (the owner's decision of 2026-10-25), no sequence, windowed, brackets only.
        assert_eq!(caps.md.books, vec![orderbook(depth)], "{depth}");
        assert_eq!(
            caps.md.books[usize::from(BOOK_ORDERBOOK.0)].max_depth,
            depth
        );
    }
    assert_eq!(BOOK_ORDERBOOK, BookId(0));
    // Leading zeros are digits; the value is what counts.
    let mut cfg = config();
    cfg.insert(KEY_ORDERBOOK_DEPTH, "050");
    assert_eq!(Hibachi.caps(&cfg).unwrap().md.books[0].max_depth, 50);
}

#[test]
fn the_market_data_caps_declare_json_the_funding_stream_and_second_timestamps() {
    let md = Hibachi.caps(&config()).unwrap().md;
    assert_eq!(md.encoding, Encoding::Json);
    // The funding_rate_estimation stream: each event states the one-hour interval its rate is
    // per (the codec's, on the owner's conclusion, since the frames carry none), and no next
    // time until its unit is evidenced.
    assert_eq!(
        md.funding,
        FundingCaps {
            source: FeedSource::Stream,
            interval_reported: true,
            next_time_reported: false,
        }
    );
    // The coarsest exchange timestamp the frames carry: a trade's whole seconds.
    assert_eq!(md.ts_precision, Duration::from_secs(1));
    // ask_bid_price, TOUCH_ASK_BID_PRICE: cadence Unknown, no sequence, no timestamp.
    assert_eq!(
        md.touch_sources,
        [TouchSourceCaps {
            channel: "ask_bid_price",
            cadence: Cadence::Unknown,
            seq_domain: SeqDomain::None,
            ts_kind: ExchTsKind::Unknown,
            includes_channels: TagSet::of(&[Channel::Public]),
        }]
    );
    assert_eq!(TOUCH_ASK_BID_PRICE, TouchSourceId(0));
    // Trades streamed with their taker side and no trade id; mark and spot (as the index)
    // streamed.
    assert_eq!(
        md.trades,
        TradeCaps {
            source: FeedSource::Stream,
            aggressor: true,
            trade_id: false,
        }
    );
    assert_eq!(md.mark, FeedSource::Stream);
    assert_eq!(md.index, FeedSource::Stream);
    assert_eq!(md.stats, FeedSource::None);
    // The trading status re-read by the status poll on REST (decision 0130).
    assert_eq!(md.status, FeedSource::Poll);
    assert_eq!(
        md.topology,
        ConnTopology::Shared {
            max_subscriptions: None
        }
    );
    assert_eq!(md.max_conn_lifetime, None);
}

#[test]
fn the_caps_declare_the_rest_limit_and_the_taker_speed_bump() {
    let caps = Hibachi.caps(&config()).unwrap();
    assert_eq!(
        caps.limits,
        vec![RateLimit {
            scope: LimitScope::Ip,
            ops: TagSet::of(&[OpKind::Rest]),
            per: Duration::from_secs(10),
            units: 350,
        }]
    );
    assert_eq!(
        caps.matching.speed_bump,
        Some(SpeedBump {
            delay: Duration::from_millis(25),
            applies_to: SpeedBumpScope::TakersOnly,
        })
    );
    assert_eq!(caps.matching.stp_scope, StpScope::None);
}

#[test]
fn the_schema_names_every_key_the_venue_reads() {
    let schema = Hibachi.config_schema();
    let keys: Vec<_> = schema.iter().map(|f| f.key).collect();
    assert_eq!(
        keys,
        [
            KEY_DATA_REST_URL,
            KEY_REST_TIMEOUT,
            KEY_ORDERBOOK_DEPTH,
            KEY_DATA_WS_URL,
            KEY_ORDERBOOK_RESUBSCRIBE,
            KEY_STATUS_INTERVAL,
            KEY_AUCTION_HALT
        ]
    );
    let units: Vec<_> = schema.iter().map(|f| f.unit).collect();
    assert_eq!(
        units,
        [
            FieldUnit::Dimensionless,
            FieldUnit::Duration,
            FieldUnit::Count,
            FieldUnit::Dimensionless,
            FieldUnit::Duration,
            FieldUnit::Duration,
            FieldUnit::Duration
        ]
    );
    assert!(schema.iter().all(|f| f.scope == ConfigScope::Process));
    assert!(schema.iter().all(|f| !f.doc.is_empty()));
}

#[test]
fn every_key_is_required_with_no_default() {
    let subs = BTreeSet::new();
    for (key, _) in CONFIG {
        let cfg = without(key);
        assert_eq!(Hibachi.caps(&cfg), Err(ConfigError::Missing(key)), "{key}");
        assert_eq!(
            Hibachi.plan_md(&cfg, &SpecTable::new(), &subs),
            Err(VenueError::Config(ConfigError::Missing(key))),
            "{key}"
        );
    }
    // Discovery reads only the REST origin and timeout.
    for key in [KEY_DATA_REST_URL, KEY_REST_TIMEOUT] {
        assert_eq!(
            Hibachi.discover(&without(key)).err(),
            Some(VenueError::Config(ConfigError::Missing(key)))
        );
    }
    for key in [
        KEY_ORDERBOOK_DEPTH,
        KEY_DATA_WS_URL,
        KEY_ORDERBOOK_RESUBSCRIBE,
        KEY_STATUS_INTERVAL,
        KEY_AUCTION_HALT,
    ] {
        assert!(Hibachi.discover(&without(key)).is_ok(), "{key}");
    }
}

#[test]
fn the_status_interval_and_the_reopening_window_are_whole_seconds_and_never_zero() {
    for key in [KEY_STATUS_INTERVAL, KEY_AUCTION_HALT] {
        for value in [
            "0s",
            "",
            "s",
            "30",
            "30000ms",
            "-1s",
            "+1s",
            "1.5s",
            " 1s",
            "1 s",
            "4294967296s",
        ] {
            let mut cfg = config();
            cfg.insert(key, value);
            let refused = ConfigError::Invalid {
                key,
                reason: "not a positive whole number of seconds (<n>s)",
            };
            assert_eq!(Hibachi.caps(&cfg), Err(refused), "{key}={value:?}");
            assert_eq!(
                Hibachi.plan_md(&cfg, &specs(), &BTreeSet::from([sub(Feed::Trades)])),
                Err(VenueError::Config(refused)),
                "{key}={value:?}"
            );
        }
        for value in ["1s", "007s", "4294967295s"] {
            let mut cfg = config();
            cfg.insert(key, value);
            assert!(Hibachi.caps(&cfg).is_ok(), "{key}={value:?}");
        }
    }
}

#[test]
fn the_resubscribe_interval_is_whole_milliseconds_and_never_zero() {
    for value in [
        "0ms", "", "ms", "2s", "2000", "-1ms", "+1ms", "1.5ms", " 1ms",
    ] {
        let mut cfg = config();
        cfg.insert(KEY_ORDERBOOK_RESUBSCRIBE, value);
        assert_eq!(
            Hibachi.caps(&cfg),
            Err(ConfigError::Invalid {
                key: KEY_ORDERBOOK_RESUBSCRIBE,
                reason: "not a positive whole number of milliseconds (<n>ms)",
            }),
            "{value:?}"
        );
    }
    let mut cfg = config();
    cfg.insert(KEY_ORDERBOOK_RESUBSCRIBE, "1ms");
    assert!(Hibachi.caps(&cfg).is_ok());
}

#[test]
fn the_websocket_origin_is_wss_or_ws_to_a_loopback_host() {
    for value in [
        "https://data-api.hibachi.xyz",
        "data-api.hibachi.xyz",
        "wss://",
        "wss://data-api.hibachi.xyz/ws/market",
        "wss://data-api.hibachi.xyz?hibachiClient=x",
        "ws://data-api.hibachi.xyz",
        "ws://10.0.0.1:8080",
    ] {
        let mut cfg = config();
        cfg.insert(KEY_DATA_WS_URL, value);
        assert_eq!(
            Hibachi.caps(&cfg),
            Err(ConfigError::Invalid {
                key: KEY_DATA_WS_URL,
                reason: "not a wss:// origin, or a ws:// origin on a loopback host",
            }),
            "{value}"
        );
    }
    for (origin, url) in [
        (
            "wss://data-api.hibachi.xyz/",
            "wss://data-api.hibachi.xyz/ws/market",
        ),
        ("ws://127.0.0.1:9001", "ws://127.0.0.1:9001/ws/market"),
        ("ws://localhost", "ws://localhost/ws/market"),
    ] {
        let mut cfg = config();
        cfg.insert(KEY_DATA_WS_URL, origin);
        let plans = Hibachi
            .plan_md(&cfg, &specs(), &BTreeSet::from([sub(Feed::Trades)]))
            .unwrap();
        assert_eq!(
            plans[0].transport,
            MdTransport::Socket {
                url: WireUrl::plain(url)
            }
        );
    }
}

#[test]
fn the_orderbook_depth_is_refused_outside_1_to_100() {
    for value in [
        "0", "101", "65536", "-1", "+5", " 5", "5 ", "5.0", "", "x", "1e2",
    ] {
        let mut cfg = config();
        cfg.insert(KEY_ORDERBOOK_DEPTH, value);
        assert_eq!(
            Hibachi.caps(&cfg),
            Err(ConfigError::Invalid {
                key: KEY_ORDERBOOK_DEPTH,
                reason: "not a whole number of levels from 1 to 100",
            }),
            "{value:?}"
        );
    }
}

#[test]
fn an_invalid_origin_or_timeout_is_refused() {
    for (key, value) in [
        (KEY_DATA_REST_URL, "wss://data-api.hibachi.xyz"),
        (KEY_DATA_REST_URL, "data-api.hibachi.xyz"),
        (KEY_DATA_REST_URL, "https://"),
        (KEY_DATA_REST_URL, "https://data-api.hibachi.xyz/market"),
        (KEY_DATA_REST_URL, "https://data-api.hibachi.xyz?x=1"),
        (KEY_DATA_REST_URL, "https://user@data-api.hibachi.xyz"),
        (KEY_DATA_REST_URL, "https://data-api..hibachi.xyz"),
        (KEY_DATA_REST_URL, "https://data-api.hibachi.xyz:"),
        (KEY_DATA_REST_URL, "https://data-api.hibachi.xyz:0"),
        (KEY_DATA_REST_URL, "https://data-api.hibachi.xyz:65536"),
        (KEY_DATA_REST_URL, "https://data-api.hibachi.xyz:x"),
        (KEY_DATA_REST_URL, "https://999.1.1.1"),
        (KEY_DATA_REST_URL, "https://[::1"),
        (KEY_DATA_REST_URL, "https://[g::1]"),
        (KEY_DATA_REST_URL, "https://[::1]x"),
        (KEY_DATA_REST_URL, "https://[::1]:"),
        // Plain HTTP only to a loopback test stub.
        (KEY_DATA_REST_URL, "http://data-api.hibachi.xyz"),
        (KEY_DATA_REST_URL, "http://10.0.0.1:8080"),
        (KEY_DATA_REST_URL, "http://[2001:db8::1]"),
        (KEY_REST_TIMEOUT, "5s"),
        (KEY_REST_TIMEOUT, "5000"),
        (KEY_REST_TIMEOUT, "ms"),
        (KEY_REST_TIMEOUT, "0ms"),
        (KEY_REST_TIMEOUT, "+5ms"),
        (KEY_REST_TIMEOUT, "99999999999999999999ms"),
    ] {
        let mut cfg = config();
        cfg.insert(key, value);
        match Hibachi.caps(&cfg) {
            Err(ConfigError::Invalid { key: named, .. }) => assert_eq!(named, key, "{value}"),
            other => panic!("{key}={value}: {other:?}"),
        }
        match Hibachi.discover(&cfg) {
            Err(VenueError::Config(ConfigError::Invalid { key: named, .. })) => {
                assert_eq!(named, key, "{value}")
            }
            Err(other) => panic!("{key}={value}: {other:?}"),
            Ok(_) => panic!("{key}={value}: planned"),
        }
    }
    for origin in [
        "https://data-api.hibachi.xyz/",
        "https://127.0.0.1",
        "https://[2001:db8::1]:443",
        "http://127.0.0.2:80",
        "http://localhost",
    ] {
        let mut cfg = config();
        cfg.insert(KEY_DATA_REST_URL, origin);
        assert!(Hibachi.caps(&cfg).is_ok(), "{origin}");
    }
}

/// ETH/USDT-P alone, as a spec table the factory can spell.
fn specs() -> SpecTable {
    use fbc_core::{
        AssetSym, FundingSpec, InstrumentKind, InstrumentSpec, Lots, PriceGrid, SizeStep,
        TradingStatus, UnderlyingId, VenueId, WallNs, dispatch_market_data,
    };
    let caps = Hibachi.caps(&config()).unwrap();
    let venue_symbol =
        dispatch_market_data(&caps, |scope| scope.venue_symbol("ETH/USDT-P")).unwrap();
    let usdt = AssetSym::new("USDT").unwrap();
    let mut table = SpecTable::new();
    table.insert(InstrumentSpec {
        id: ETH,
        venue: VenueId::new(6),
        venue_symbol,
        native_id: None,
        underlying: UnderlyingId::new(1),
        kind: InstrumentKind::Perpetual,
        price_grid: PriceGrid::fixed("0.000001".parse().unwrap()).unwrap(),
        quote_grid: Some("0.01".parse().unwrap()),
        size_step: SizeStep::new("0.000000001".parse().unwrap()).unwrap(),
        min_size: Lots::new(1).unwrap(),
        min_notional: None,
        max_order_size: None,
        position_limit: None,
        price_band: None,
        max_open_orders: None,
        multiplier: rust_decimal::Decimal::ONE,
        quote_ccy: usdt,
        settle_ccy: usdt,
        funding: FundingSpec::Known {
            interval: fbc_venue_hibachi::FUNDING_SETTLEMENT_INTERVAL,
            cap: Some(fbc_venue_hibachi::FUNDING_RATE_CAP),
        },
        public_fees: None,
        status: TradingStatus::Trading,
        sessions: None,
        signing: None,
        version: 1,
        fetched_at: WallNs(0),
    });
    table
}

#[test]
fn plan_md_plans_one_ws_market_connection_carrying_every_feed_the_codec_decodes() {
    let cfg = config();
    let specs = specs();
    assert_eq!(
        Hibachi.plan_md(&cfg, &specs, &BTreeSet::new()),
        Ok(Vec::new())
    );
    let feeds = [
        Feed::Book(BOOK_ORDERBOOK),
        Feed::Touch(TOUCH_ASK_BID_PRICE),
        Feed::Trades,
        Feed::Mark,
        Feed::Index,
        Feed::Funding,
    ];
    let subs: BTreeSet<Subscription> = feeds.into_iter().map(sub).collect();
    // The connection, and the status poll on the REST origin carrying the same subscriptions
    // (FBC-1632, decisions 0027 and 0130).
    assert_eq!(
        Hibachi.plan_md(&cfg, &specs, &subs),
        Ok(vec![
            EndpointPlan {
                stream: StreamId(0),
                transport: MdTransport::Socket {
                    url: WireUrl::plain("wss://data-api.hibachi.xyz/ws/market"),
                },
                subs: subs.iter().copied().collect(),
            },
            EndpointPlan {
                stream: STATUS_STREAM,
                transport: MdTransport::Poll {
                    base_url: WireUrl::plain("https://data-api.hibachi.xyz"),
                },
                subs: subs.iter().copied().collect(),
            },
        ])
    );
    assert_eq!(STATUS_STREAM, StreamId(1));
    // Stats, another book or touch channel, and an instrument missing from the table are
    // refused, nothing planned.
    for feed in [
        Feed::Stats,
        Feed::Book(BookId(1)),
        Feed::Touch(TouchSourceId(1)),
    ] {
        let s = sub(feed);
        let mut with = subs.clone();
        with.insert(s);
        assert_eq!(
            Hibachi.plan_md(&cfg, &specs, &with),
            Err(VenueError::UnsupportedFeed(s)),
            "{feed:?}"
        );
    }
    let missing = InstrumentId::new(9);
    let s = Subscription {
        inst: missing,
        feed: Feed::Trades,
    };
    assert_eq!(
        Hibachi.plan_md(&cfg, &specs, &BTreeSet::from([s])),
        Err(VenueError::UnknownInstrument(missing))
    );
}

#[test]
fn the_factorys_codec_takes_the_planned_endpoints_stream() {
    let cfg = config();
    let specs = specs();
    let subs = BTreeSet::from([sub(Feed::Trades)]);
    let plan = Hibachi.plan_md(&cfg, &specs, &subs).unwrap().remove(0);
    let mut codec = Hibachi.md_codec(&cfg, &plan);
    let mut fx = fbc_core::Effects::new();
    codec.on_open(&mut fx);
    codec.subscribe(&plan.subs, &[], &specs, &mut fx).unwrap();
    let [fbc_core::Effect::Send { stream, .. }] = fx.as_slice() else {
        panic!("{fx:?}")
    };
    assert_eq!(*stream, plan.stream);
    // A configuration plan_md refuses gives a codec that refuses every subscription the same.
    let bad = without(KEY_DATA_WS_URL);
    let mut codec = Hibachi.md_codec(&bad, &plan);
    assert_eq!(
        codec.subscribe(&plan.subs, &[], &specs, &mut fx),
        Err(VenueError::Config(ConfigError::Missing(KEY_DATA_WS_URL)))
    );
}

#[test]
fn the_status_polls_codec_reads_exchange_info_from_a_timer_on_the_rest_origin() {
    let cfg = config();
    let specs = specs();
    let subs = BTreeSet::from([sub(Feed::Trades)]);
    let poll = Hibachi.plan_md(&cfg, &specs, &subs).unwrap().remove(1);
    let mut codec = Hibachi.md_codec(&cfg, &poll);
    let mut fx = fbc_core::Effects::new();
    // on_open asks for the timer the first read is sent from, at once; subscribing sends
    // nothing.
    codec.on_open(&mut fx);
    codec.subscribe(&poll.subs, &[], &specs, &mut fx).unwrap();
    let [fbc_core::Effect::Timer { tag, after }] = fx.take()[..] else {
        panic!("not one timer")
    };
    assert_eq!(after, Duration::ZERO);
    struct Nothing;
    impl fbc_core::MdSink for Nothing {
        fn push(&mut self, _: fbc_core::VenueMeta, ev: fbc_core::MdEvent) {
            panic!("{ev:?}")
        }
    }
    let wall = fbc_core::WallNs(1_800_000_000_000_000_000);
    codec.on_timer(tag, fbc_core::MonoNs(0), wall, &mut Nothing, &mut fx);
    // One GET of /market/exchange-info, then the next read's timer at the interval.
    let effects = fx.take();
    let [
        fbc_core::Effect::Http {
            req, timeout, rpc, ..
        },
        fbc_core::Effect::Timer { after, .. },
    ] = &effects[..]
    else {
        panic!("{effects:?}")
    };
    assert_eq!(req.method, fbc_core::HttpMethod::Get);
    assert_eq!(
        req.url,
        WireUrl::plain("https://data-api.hibachi.xyz/market/exchange-info")
    );
    assert!(req.headers.is_empty());
    assert_eq!((*timeout, *rpc), (Duration::from_millis(5_000), None));
    assert_eq!(*after, Duration::from_secs(30));
    // A configuration plan_md refuses gives a status codec that refuses every subscription the
    // same, and reads nothing.
    let bad = without(KEY_STATUS_INTERVAL);
    let mut codec = Hibachi.md_codec(&bad, &poll);
    codec.on_open(&mut fx);
    assert_eq!(
        codec.subscribe(&poll.subs, &[], &specs, &mut fx),
        Err(VenueError::Config(ConfigError::Missing(
            KEY_STATUS_INTERVAL
        )))
    );
    assert!(fx.is_empty());
}
