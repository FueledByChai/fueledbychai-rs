//! FBC-jyyl's done line, the factory half: Hibachi is declared market data only (`exec: None`,
//! `Readiness::Record`, no order-entry codec or endpoint), its capabilities state the orderbook
//! channel at the configured depth with an unknown cadence, no sequence, a window and brackets
//! only, the funding stream with no interval, JSON and one-second timestamps, the REST limit and
//! the taker speed bump; every configuration key is required and checked, the orderbook depth
//! refused when absent or outside 1 to 100; and until the codec lands no feed is planned.

use std::collections::BTreeSet;
use std::time::Duration;

use fbc_core::{
    BookCaps, BookId, Cadence, Channel, ConfigError, ConfigScope, ConnTopology, Continuity,
    DecodeError, Effects, Encoding, EndpointPlan, Feed, FeedSource, FieldUnit, FundingCaps,
    HttpFailure, HttpResponse, HttpTag, Inbound, InboundSpans, InstrumentId, LimitScope, MdEvent,
    MdSink, MdTransport, MonoNs, OpKind, QueueModelQuality, RateLimit, RawFrame, Readiness,
    Secrets, SpecTable, SpeedBump, SpeedBumpScope, StpScope, StreamId, Subscription, SymbolError,
    TagSet, TimerTag, TouchSourceId, TradeCaps, VenueConfig, VenueError, VenueFactory, VenueMeta,
    WallNs, WireUrl, dispatch_market_data,
};
use fbc_venue_hibachi::{
    BOOK_ORDERBOOK, Hibachi, KEY_DATA_REST_URL, KEY_ORDERBOOK_DEPTH, KEY_REST_TIMEOUT,
};

const CONFIG: [(&str, &str); 3] = [
    (KEY_DATA_REST_URL, "https://data-api.hibachi.xyz"),
    (KEY_REST_TIMEOUT, "5000ms"),
    (KEY_ORDERBOOK_DEPTH, "25"),
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
    // The funding_rate_estimation stream: no interval, and no next time until its unit is
    // pinned; MdCaps carries no funding interval.
    assert_eq!(
        md.funding,
        FundingCaps {
            source: FeedSource::Stream,
            interval_reported: false,
            next_time_reported: false,
        }
    );
    // The coarsest exchange timestamp the frames carry: a trade's whole seconds.
    assert_eq!(md.ts_precision, Duration::from_secs(1));
    // Declared with the codec that decodes them (FBC-2kmp).
    assert!(md.touch_sources.is_empty());
    assert_eq!(
        md.trades,
        TradeCaps {
            source: FeedSource::None,
            aggressor: false,
            trade_id: false,
        }
    );
    assert_eq!(md.mark, FeedSource::None);
    assert_eq!(md.index, FeedSource::None);
    assert_eq!(md.stats, FeedSource::None);
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
        [KEY_DATA_REST_URL, KEY_REST_TIMEOUT, KEY_ORDERBOOK_DEPTH]
    );
    let units: Vec<_> = schema.iter().map(|f| f.unit).collect();
    assert_eq!(
        units,
        [
            FieldUnit::Dimensionless,
            FieldUnit::Duration,
            FieldUnit::Count
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
    assert!(Hibachi.discover(&without(KEY_ORDERBOOK_DEPTH)).is_ok());
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

#[test]
fn plan_md_plans_nothing_and_refuses_every_feed_until_the_codec_lands() {
    let cfg = config();
    let specs = SpecTable::new();
    assert_eq!(
        Hibachi.plan_md(&cfg, &specs, &BTreeSet::new()),
        Ok(Vec::new())
    );
    for feed in [
        Feed::Book(BOOK_ORDERBOOK),
        Feed::Touch(TouchSourceId(0)),
        Feed::Trades,
        Feed::Mark,
        Feed::Index,
        Feed::Funding,
        Feed::Stats,
    ] {
        let s = sub(feed);
        assert_eq!(
            Hibachi.plan_md(&cfg, &specs, &BTreeSet::from([s])),
            Err(VenueError::UnsupportedFeed(s)),
            "{feed:?}"
        );
    }
}

/// A sink that keeps what it is handed.
#[derive(Default)]
struct Kept(Vec<MdEvent>);

impl MdSink for Kept {
    fn push(&mut self, _meta: VenueMeta, event: MdEvent) {
        self.0.push(event);
    }
}

#[test]
fn the_codec_refuses_every_subscription_and_decodes_nothing() {
    let cfg = config();
    let ep = EndpointPlan {
        stream: StreamId(0),
        transport: MdTransport::Socket {
            url: WireUrl::plain("wss://data-api.hibachi.xyz/ws/market"),
        },
        subs: Vec::new(),
    };
    let mut codec = Hibachi.md_codec(&cfg, &ep);
    let specs = SpecTable::new();
    let mut fx = Effects::new();
    codec.on_open(&mut fx);
    assert!(fx.is_empty());
    assert_eq!(codec.subscribe(&[], &[], &specs, &mut fx), Ok(()));
    let book = sub(Feed::Book(BOOK_ORDERBOOK));
    let funding = sub(Feed::Funding);
    assert_eq!(
        codec.subscribe(&[book, funding], &[], &specs, &mut fx),
        Err(VenueError::UnsupportedFeed(book))
    );
    assert_eq!(
        codec.subscribe(&[], &[funding], &specs, &mut fx),
        Err(VenueError::UnsupportedFeed(funding))
    );
    assert!(fx.is_empty());
    let caps = Hibachi.caps(&cfg).unwrap();
    let mut sink = Kept::default();
    let frame = r#"{"topic":"orderbook","symbol":"ETH/USDT-P","data":{}}"#;
    let decoded = dispatch_market_data(&caps, |scope| {
        codec.on_frame(RawFrame::Text(frame), scope, &specs, &mut sink, &mut fx)
    });
    assert_eq!(
        decoded,
        Err(DecodeError::Malformed("no Hibachi feed is decoded yet"))
    );
    let answered = dispatch_market_data(&caps, |scope| {
        codec.on_http(
            HttpTag(1),
            Err(HttpFailure::NotSent),
            scope,
            &specs,
            &mut sink,
            &mut fx,
        )
    });
    assert_eq!(
        answered,
        Err(DecodeError::Malformed(
            "no Hibachi market-data request is made"
        ))
    );
    codec.on_timer(TimerTag(1), MonoNs(1), WallNs(1), &mut sink, &mut fx);
    assert!(sink.0.is_empty());
    assert!(fx.is_empty());
    assert_eq!(codec.keepalive(), None);
    let response = HttpResponse {
        status: 200,
        headers: &[],
        body: b"{}",
    };
    assert_eq!(
        codec.redact_inbound(Inbound::Frame(RawFrame::Text(frame))),
        InboundSpans::NONE
    );
    assert_eq!(
        codec.redact_inbound(Inbound::Http(HttpTag(1), response)),
        InboundSpans::NONE
    );
}
