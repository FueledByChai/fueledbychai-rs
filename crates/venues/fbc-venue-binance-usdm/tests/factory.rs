//! FBC-z0l's done line, the factory half: Binance USD-M is declared market data only
//! (`exec: None`, no exec codec, no exec endpoint) with the rate limits FBC-hof models, and
//! `plan_md` puts touch and partial-depth subscriptions for two instruments on one endpoint
//! whose subscribe frame names each stream once. FBC-tfb adds the diff-depth book's caps
//! (PrevId continuity, a REST anchor, the snapshot's depth) and its configuration.

mod common;

use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::time::Duration;

use common::{BTC, CONFIG, ETH, SOL, config, specs};
use fbc_core::{
    BookId, Cadence, ConfigError, ConfigScope, ConnTopology, Continuity, Effect, Effects, Encoding,
    ExchTsKind, Feed, FeedSource, FieldUnit, Inbound, InboundSpans, LimitScope, MdTransport,
    OpKind, QueueModelQuality, RateLimit, RawFrame, Readiness, SeqDomain, StpScope, StreamId,
    Subscription, TagSet, TouchSourceId, TrafficClass, VenueConfig, VenueError, VenueFactory,
};
use fbc_venue_binance_usdm::{
    BOOK_DIFF, BOOK_PARTIAL, BinanceUsdm, KEY_DEPTH_LEVELS, KEY_DEPTH_SPEED, KEY_REST_BASE_URL,
    KEY_SNAPSHOT_LIMIT, KEY_SNAPSHOT_RETRY, KEY_SNAPSHOT_TIMEOUT, KEY_WS_BASE_URL,
    TOUCH_BOOK_TICKER, rest_depth_weight,
};
use serde_json::Value;

fn sub(inst: fbc_core::InstrumentId, feed: Feed) -> Subscription {
    Subscription { inst, feed }
}

#[test]
fn the_factory_declares_market_data_only_with_no_exec_codec_or_endpoint() {
    let cfg = config();
    let caps = BinanceUsdm.caps(&cfg).unwrap();
    assert_eq!(caps.exec, None);
    assert!(BinanceUsdm.exec_codec(&cfg).is_none());
    assert_eq!(BinanceUsdm.plan_exec(&cfg), Ok(Vec::new()));
    assert_eq!(BinanceUsdm.id(), "BINANCE_FUTURES");
    // Never promoted past recording: it is the reference feed, not a traded venue.
    assert_eq!(caps.readiness_ceiling, Readiness::Record);
    // STP prevents matching within one account or the accounts of one tradeGroupId (Codex
    // r4176739116): the owner's scope, though an order opts into it.
    assert_eq!(caps.matching.speed_bump, None);
    assert_eq!(caps.matching.stp_scope, StpScope::Owner);
}

#[test]
fn the_factory_declares_the_weight_message_and_connection_limits() {
    let caps = BinanceUsdm.caps(&config()).unwrap();
    let weight = RateLimit {
        scope: LimitScope::Ip,
        ops: TagSet::of(&[OpKind::Rest]),
        per: Duration::from_secs(60),
        units: 2400,
    };
    let messages = RateLimit {
        scope: LimitScope::Connection,
        ops: TagSet::of(&[OpKind::Subscribe, OpKind::Control]),
        per: Duration::from_secs(1),
        units: 10,
    };
    let connects = RateLimit {
        scope: LimitScope::Ip,
        ops: TagSet::of(&[OpKind::Connect]),
        per: Duration::from_secs(300),
        units: 300,
    };
    assert_eq!(caps.limits, [weight, messages, connects]);

    // GET /fapi/v1/depth costs more at larger limits, and only documented limits have a weight.
    let w = |n| NonZeroU32::new(n).unwrap();
    for (limit, weight) in [
        (5, 2),
        (10, 2),
        (20, 2),
        (50, 2),
        (100, 5),
        (500, 10),
        (1000, 20),
    ] {
        assert_eq!(rest_depth_weight(limit), Some(w(weight)), "limit {limit}");
    }
    for limit in [0, 1, 25, 200, 1001] {
        assert_eq!(rest_depth_weight(limit), None, "limit {limit}");
    }
}

#[test]
fn the_factory_declares_its_market_data_with_each_feed_it_does_not_decode_none() {
    let md = BinanceUsdm.caps(&config()).unwrap().md;
    assert_eq!(md.encoding, Encoding::Json);
    assert_eq!(md.touch_sources.len(), 1);
    let touch = md.touch_sources[usize::from(TOUCH_BOOK_TICKER.0)];
    assert_eq!(touch.channel, "bookTicker");
    assert_eq!(touch.cadence, Cadence::Realtime);
    assert_eq!(touch.seq_domain, SeqDomain::SharedWithBook);
    assert_eq!(touch.ts_kind, ExchTsKind::MatchingEngine);

    assert_eq!(BOOK_PARTIAL, BookId(0));
    let partial = md.books[usize::from(BOOK_PARTIAL.0)];
    assert_eq!(partial.channel, "depth5@100ms");
    assert_eq!(partial.max_depth, 5);
    assert_eq!(partial.cadence, Cadence::Pulsed(Duration::from_millis(100)));
    assert_eq!(partial.continuity, Continuity::Windowed);
    assert!(partial.windowed && !partial.rest_anchor);
    assert_eq!(partial.queue_model, QueueModelQuality::BracketOnly);
    // The diff-depth book, anchored on a REST snapshot whose limit sets its depth (FBC-tfb).
    assert_eq!(BOOK_DIFF, BookId(1));
    let diff = md.books[usize::from(BOOK_DIFF.0)];
    assert_eq!(diff.channel, "depth@100ms");
    assert_eq!(diff.max_depth, 1000);
    assert_eq!(diff.cadence, Cadence::Pulsed(Duration::from_millis(100)));
    assert_eq!(diff.continuity, Continuity::PrevId);
    assert!(diff.rest_anchor && !diff.windowed);
    assert_eq!(diff.queue_model, QueueModelQuality::BracketOnly);
    assert_eq!(md.books.len(), 2);

    assert_eq!(md.trades.source, FeedSource::None);
    assert_eq!(md.funding.source, FeedSource::None);
    for source in [md.stats, md.mark, md.index] {
        assert_eq!(source, FeedSource::None);
    }
    assert_eq!(md.ts_precision, Duration::from_millis(1));
    assert_eq!(
        md.topology,
        ConnTopology::Shared {
            max_subscriptions: Some(1024)
        }
    );
    assert_eq!(md.max_conn_lifetime, Some(Duration::from_secs(24 * 3600)));
}

#[test]
fn the_partial_depth_channel_follows_the_configured_levels_and_speed() {
    let mut cfg = config();
    for (levels, speed, channel, millis) in [
        ("5", "100ms", "depth5@100ms", 100),
        ("5", "250ms", "depth5", 250),
        ("5", "500ms", "depth5@500ms", 500),
        ("10", "100ms", "depth10@100ms", 100),
        ("10", "250ms", "depth10", 250),
        ("10", "500ms", "depth10@500ms", 500),
        ("20", "100ms", "depth20@100ms", 100),
        ("20", "250ms", "depth20", 250),
        ("20", "500ms", "depth20@500ms", 500),
    ] {
        cfg.insert(KEY_DEPTH_LEVELS, levels);
        cfg.insert(KEY_DEPTH_SPEED, speed);
        let book = BinanceUsdm.caps(&cfg).unwrap().md.books[0];
        assert_eq!(book.channel, channel);
        assert_eq!(book.max_depth.to_string(), levels);
        assert_eq!(book.cadence, Cadence::Pulsed(Duration::from_millis(millis)));
    }
}

#[test]
fn the_config_schema_names_the_base_url_and_the_stream_choices() {
    let schema = BinanceUsdm.config_schema();
    let keys: Vec<_> = schema.iter().map(|f| (f.key, f.unit)).collect();
    assert_eq!(
        keys,
        [
            (KEY_WS_BASE_URL, FieldUnit::Dimensionless),
            (KEY_DEPTH_LEVELS, FieldUnit::Count),
            (KEY_DEPTH_SPEED, FieldUnit::Duration),
            (KEY_REST_BASE_URL, FieldUnit::Dimensionless),
            (KEY_SNAPSHOT_LIMIT, FieldUnit::Count),
            (KEY_SNAPSHOT_TIMEOUT, FieldUnit::Duration),
            (KEY_SNAPSHOT_RETRY, FieldUnit::Duration),
        ]
    );
    assert!(schema.iter().all(|f| f.scope == ConfigScope::Process));
    assert!(schema.iter().all(|f| !f.doc.is_empty()));
}

#[test]
fn a_missing_or_invalid_key_is_refused_by_caps_and_plan_md() {
    let without = |key| {
        let mut cfg = VenueConfig::new();
        for (k, v) in CONFIG {
            if k != key {
                cfg.insert(k, v);
            }
        }
        // A trailing slash on the base URL is not doubled.
        if key != KEY_WS_BASE_URL {
            cfg.insert(KEY_WS_BASE_URL, "wss://fstream.binance.com/");
        }
        cfg
    };
    let subs = BTreeSet::from([sub(BTC, Feed::Touch(TOUCH_BOOK_TICKER))]);
    for (key, _) in CONFIG {
        let cfg = without(key);
        assert_eq!(BinanceUsdm.caps(&cfg), Err(ConfigError::Missing(key)));
        let planned = BinanceUsdm.plan_md(&cfg, &specs(), &subs);
        assert_eq!(planned, Err(VenueError::Config(ConfigError::Missing(key))));
    }
    for (key, value) in [
        (KEY_WS_BASE_URL, "https://fstream.binance.com"),
        (KEY_WS_BASE_URL, "wss://"),
        // Codex r4176739112: only an origin, scheme://host[:port], is a base URL.
        (KEY_WS_BASE_URL, "ws://host:abc"),
        (KEY_WS_BASE_URL, "ws://bad host"),
        (KEY_WS_BASE_URL, "wss://host?x=1"),
        (KEY_WS_BASE_URL, "wss://host#x"),
        (KEY_WS_BASE_URL, "wss://host/ws"),
        (KEY_WS_BASE_URL, "wss://user@host"),
        (KEY_WS_BASE_URL, "wss://host:"),
        (KEY_WS_BASE_URL, "wss://host:0"),
        (KEY_WS_BASE_URL, "wss://host:65536"),
        (KEY_WS_BASE_URL, "wss://:443"),
        (KEY_WS_BASE_URL, "ws://[::1"),
        (KEY_WS_BASE_URL, "ws://[]:80"),
        (KEY_WS_BASE_URL, "ws://[::1]x"),
        (KEY_WS_BASE_URL, "ws://[g::1]:80"),
        // Codex r4176771986: bracket contents parse as an IPv6 address.
        (KEY_WS_BASE_URL, "ws://[:::]"),
        (KEY_WS_BASE_URL, "ws://[1:2:3:4:5:6:7:8:9]:80"),
        (KEY_DEPTH_LEVELS, "50"),
        (KEY_DEPTH_SPEED, "1s"),
        (KEY_REST_BASE_URL, "wss://fapi.binance.com"),
        (KEY_REST_BASE_URL, "https://fapi.binance.com/fapi"),
        (KEY_REST_BASE_URL, "https://"),
        (KEY_SNAPSHOT_LIMIT, "25"),
        (KEY_SNAPSHOT_LIMIT, "x"),
        (KEY_SNAPSHOT_TIMEOUT, "5s"),
        (KEY_SNAPSHOT_TIMEOUT, "ms"),
        (KEY_SNAPSHOT_TIMEOUT, "0ms"),
        (KEY_SNAPSHOT_TIMEOUT, "+5ms"),
        (KEY_SNAPSHOT_TIMEOUT, "99999999999999999999ms"),
        (KEY_SNAPSHOT_RETRY, "-1ms"),
    ] {
        let mut cfg = config();
        cfg.insert(key, value);
        match BinanceUsdm.caps(&cfg) {
            Err(ConfigError::Invalid { key: named, .. }) => assert_eq!(named, key),
            other => panic!("{key}={value}: {other:?}"),
        }
    }
    // An origin with a port, by name or address, is a base URL.
    for base in [
        "ws://127.0.0.1:9000",
        "ws://[::1]:9000",
        "wss://fstream.binance.com:443",
    ] {
        let mut cfg = config();
        cfg.insert(KEY_WS_BASE_URL, base);
        let plans = BinanceUsdm.plan_md(&cfg, &specs(), &subs).unwrap();
        let MdTransport::Socket { url } = &plans[0].transport else {
            panic!("a socket")
        };
        assert_eq!(url.as_str(), format!("{base}/public/stream"));
    }
    // A trailing slash on the base URL is not doubled.
    let plans = BinanceUsdm
        .plan_md(&without("none"), &specs(), &subs)
        .unwrap();
    let MdTransport::Socket { url } = &plans[0].transport else {
        panic!("a socket")
    };
    assert_eq!(url.as_str(), "wss://fstream.binance.com/public/stream");
}

#[test]
fn plan_md_puts_two_instruments_touch_and_depth_on_one_endpoint_subscribed_in_one_frame() {
    let cfg = config();
    let touch = Feed::Touch(TOUCH_BOOK_TICKER);
    let depth = Feed::Book(BOOK_PARTIAL);
    let subs = BTreeSet::from([
        sub(BTC, touch),
        sub(BTC, depth),
        sub(ETH, touch),
        sub(ETH, depth),
    ]);
    let plans = BinanceUsdm.plan_md(&cfg, &specs(), &subs).unwrap();
    assert_eq!(plans.len(), 1);
    let plan = &plans[0];
    assert_eq!(plan.stream, StreamId(0));
    // The combined-stream endpoint on the /public route, which carries bookTicker and depth.
    assert_eq!(
        plan.transport,
        MdTransport::Socket {
            url: fbc_core::WireUrl::plain("wss://fstream.binance.com/public/stream")
        }
    );
    assert_eq!(plan.subs, subs.iter().copied().collect::<Vec<_>>());

    // The codec subscribes the plan's subscriptions, asked twice over, in one frame.
    let mut codec = BinanceUsdm.md_codec(&cfg, plan);
    let mut fx = Effects::new();
    codec.on_open(&mut fx);
    assert!(fx.is_empty());
    let twice: Vec<_> = plan.subs.iter().chain(&plan.subs).copied().collect();
    codec.subscribe(&twice, &[], &specs(), &mut fx).unwrap();
    let frames = fx.take();
    assert_eq!(frames.len(), 1);
    let Effect::Send {
        stream,
        frame,
        rpc,
        class,
        charge,
    } = &frames[0]
    else {
        panic!("a frame: {frames:?}")
    };
    assert_eq!(
        (*stream, *rpc, *class),
        (StreamId(0), None, TrafficClass::Normal)
    );
    // One message on the connection, counted as a subscription against its message limit.
    assert_eq!(charge.op, OpKind::Subscribe);
    assert_eq!((charge.inst, charge.weight.get()), (None, 1));
    let caps = BinanceUsdm.caps(&cfg).unwrap();
    let counted: Vec<_> = caps
        .limits
        .iter()
        .filter(|l| l.counts(charge, fbc_core::Via::Frame))
        .map(|l| l.scope)
        .collect();
    assert_eq!(counted, [LimitScope::Connection]);
    let sent: Value = serde_json::from_slice(frame.bytes()).unwrap();
    assert_eq!(sent["method"], "SUBSCRIBE");
    assert_eq!(sent["id"], 1);
    assert_eq!(
        sent["params"],
        serde_json::json!([
            "btcusdt@bookTicker",
            "btcusdt@depth5@100ms",
            "ethusdt@bookTicker",
            "ethusdt@depth5@100ms"
        ])
    );
    assert!(frame.redactions().is_empty());
    assert_eq!(codec.keepalive(), None);
    // Public market data: nothing inbound to redact.
    let frame = Inbound::Frame(RawFrame::Text("{}"));
    assert_eq!(codec.redact_inbound(frame), InboundSpans::NONE);

    // An unsubscribe is its own frame under the next request id; nothing to send sends nothing.
    codec
        .subscribe(&[], &[sub(ETH, depth)], &specs(), &mut fx)
        .unwrap();
    codec.subscribe(&[], &[], &specs(), &mut fx).unwrap();
    let frames = fx.take();
    assert_eq!(frames.len(), 1);
    let Effect::Send { frame, .. } = &frames[0] else {
        panic!("a frame")
    };
    let sent: Value = serde_json::from_slice(frame.bytes()).unwrap();
    assert_eq!(sent["method"], "UNSUBSCRIBE");
    assert_eq!(sent["id"], 2);
    assert_eq!(sent["params"], serde_json::json!(["ethusdt@depth5@100ms"]));
}

#[test]
fn plan_md_spreads_more_streams_than_one_connection_carries_over_endpoints() {
    let mut table = fbc_core::SpecTable::new();
    let base = specs();
    let template = base.get(BTC).unwrap().clone();
    let caps = BinanceUsdm.caps(&config()).unwrap();
    let mut subs = BTreeSet::new();
    for n in 0..1030u32 {
        let id = fbc_core::InstrumentId::new(100 + n);
        let symbol = format!("S{n}USDT");
        let venue_symbol =
            fbc_core::dispatch_market_data(&caps, |s| s.venue_symbol(&symbol)).unwrap();
        table.insert(fbc_core::InstrumentSpec {
            id,
            venue_symbol,
            ..template.clone()
        });
        subs.insert(sub(id, Feed::Touch(TOUCH_BOOK_TICKER)));
    }
    let plans = BinanceUsdm.plan_md(&config(), &table, &subs).unwrap();
    let sizes: Vec<_> = plans.iter().map(|p| (p.stream, p.subs.len())).collect();
    assert_eq!(sizes, [(StreamId(0), 1024), (StreamId(1), 6)]);
    assert_eq!(
        BinanceUsdm.plan_md(&config(), &table, &BTreeSet::new()),
        Ok(vec![])
    );
}

#[test]
fn feeds_it_does_not_decode_and_unknown_instruments_are_refused_with_nothing_sent() {
    let cfg = config();
    let refused = [
        Feed::Book(BookId(2)),
        Feed::Touch(TouchSourceId(1)),
        Feed::Trades,
        Feed::Mark,
        Feed::Index,
        Feed::Funding,
        Feed::Stats,
    ];
    let plan = &BinanceUsdm
        .plan_md(
            &cfg,
            &specs(),
            &BTreeSet::from([sub(BTC, Feed::Touch(TOUCH_BOOK_TICKER))]),
        )
        .unwrap()[0];
    let mut codec = BinanceUsdm.md_codec(&cfg, plan);
    for feed in refused {
        let s = sub(BTC, feed);
        let planned = BinanceUsdm.plan_md(&cfg, &specs(), &BTreeSet::from([s]));
        assert_eq!(planned, Err(VenueError::UnsupportedFeed(s)), "{feed:?}");
        let mut fx = Effects::new();
        let ok = sub(ETH, Feed::Touch(TOUCH_BOOK_TICKER));
        assert_eq!(
            codec.subscribe(&[ok, s], &[], &specs(), &mut fx),
            Err(VenueError::UnsupportedFeed(s))
        );
        assert!(fx.is_empty());
    }
    let unknown = sub(SOL, Feed::Touch(TOUCH_BOOK_TICKER));
    let planned = BinanceUsdm.plan_md(&cfg, &specs(), &BTreeSet::from([unknown]));
    assert_eq!(planned, Err(VenueError::UnknownInstrument(SOL)));
    let mut fx = Effects::new();
    let removed = codec.subscribe(&[], &[unknown], &specs(), &mut fx);
    assert_eq!(removed, Err(VenueError::UnknownInstrument(SOL)));
    assert!(fx.is_empty());
}

#[test]
fn a_codec_built_under_a_refused_configuration_subscribes_to_nothing() {
    let plan = &BinanceUsdm
        .plan_md(
            &config(),
            &specs(),
            &BTreeSet::from([sub(BTC, Feed::Touch(TOUCH_BOOK_TICKER))]),
        )
        .unwrap()[0];
    let mut cfg = config();
    cfg.insert(KEY_DEPTH_LEVELS, "7");
    let mut codec = BinanceUsdm.md_codec(&cfg, plan);
    let mut fx = Effects::new();
    let refused = codec.subscribe(&plan.subs, &[], &specs(), &mut fx);
    assert!(
        matches!(
            refused,
            Err(VenueError::Config(ConfigError::Invalid {
                key: KEY_DEPTH_LEVELS,
                ..
            }))
        ),
        "{refused:?}"
    );
    assert!(fx.is_empty());
}
