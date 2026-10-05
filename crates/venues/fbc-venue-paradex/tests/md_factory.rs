//! FBC-jlr: the Paradex factory declares market data only, plans one connection whose URL
//! negotiates SBE, and its codec sends one JSON-RPC subscribe text frame per channel.

mod md;

use std::collections::BTreeSet;
use std::time::Duration;

use fbc_core::Secrets;
use fbc_core::{
    ConfigError, ConnTopology, Effect, Effects, Encoding, Feed, FeedSource, HttpFailure, HttpTag,
    Inbound, InboundSpans, LimitScope, MdCodec, MdTransport, MonoNs, OpKind, RateCharge, RawFrame,
    StreamId, Subscription, SymbolError, TagSet, TimerTag, TrafficClass, VenueConfig, VenueError,
    VenueFactory, WallNs, dispatch_market_data,
};
use fbc_venue_paradex::ParadexFactory;
use fbc_venue_paradex::factory::{MD_STREAM, MD_URL, caps};
use fbc_venue_paradex::md::BBO;
use md::{BTC, Collect, ETH, specs};
use serde_json::Value;

const URL: &str = "wss://ws.api.prod.paradex.trade/v1";

fn cfg() -> VenueConfig {
    let mut cfg = VenueConfig::new();
    cfg.insert(MD_URL, URL);
    cfg
}

fn sub(inst: fbc_core::InstrumentId, feed: Feed) -> Subscription {
    Subscription { inst, feed }
}

#[test]
fn plan_md_negotiates_sbe_and_the_codec_subscribes_once_per_channel() {
    let subs: BTreeSet<_> = [
        sub(BTC, Feed::Touch(BBO)),
        sub(BTC, Feed::Trades),
        sub(ETH, Feed::Touch(BBO)),
    ]
    .into();
    let plans = ParadexFactory.plan_md(&cfg(), &specs(), &subs).unwrap();
    assert_eq!(plans.len(), 1, "bbo and trades share one connection");
    let plan = &plans[0];
    let MdTransport::Socket { url } = &plan.transport else {
        panic!("not a socket: {plan:?}");
    };
    assert_eq!(
        url.as_str(),
        "wss://ws.api.prod.paradex.trade/v1?sbeSchemaId=1&sbeSchemaVersion=1"
    );
    assert_eq!(plan.stream, MD_STREAM);

    let mut codec = ParadexFactory.md_codec(&cfg(), plan);
    let mut fx = Effects::new();
    codec.on_open(&mut fx);
    assert!(fx.is_empty());
    codec.subscribe(&plan.subs, &[], &specs(), &mut fx).unwrap();
    let mut channels = Vec::new();
    let mut ids = BTreeSet::new();
    for (effect, sub) in fx.as_slice().iter().zip(&plan.subs) {
        let Effect::Send {
            stream,
            frame,
            rpc,
            class,
            charge,
        } = effect
        else {
            panic!("not a frame: {effect:?}");
        };
        assert_eq!(
            (*stream, *rpc, *class),
            (MD_STREAM, None, TrafficClass::Normal)
        );
        assert_eq!(*charge, RateCharge::one(OpKind::Subscribe, Some(sub.inst)));
        let text: Value = serde_json::from_slice(frame.bytes()).unwrap();
        assert_eq!(text["jsonrpc"], "2.0");
        assert_eq!(text["method"], "subscribe");
        ids.insert(text["id"].as_u64().unwrap());
        channels.push(text["params"]["channel"].as_str().unwrap().to_owned());
    }
    assert_eq!(
        channels,
        [
            "bbo.BTC-USD-PERP",
            "trades.BTC-USD-PERP",
            "bbo.ETH-USD-PERP"
        ]
    );
    assert_eq!(ids.len(), 3, "each request has its own id");
    assert_eq!(fx.len(), 3, "one frame per channel");
}

#[test]
fn nothing_is_planned_or_sent_for_a_feed_or_instrument_it_cannot_spell() {
    let specs = specs();
    let index = sub(BTC, Feed::Index);
    let unknown = sub(fbc_core::InstrumentId::new(9), Feed::Trades);
    let other_touch = sub(BTC, Feed::Touch(fbc_core::TouchSourceId(1)));
    for (bad, err) in [
        (index, VenueError::UnsupportedFeed(index)),
        (other_touch, VenueError::UnsupportedFeed(other_touch)),
        (unknown, VenueError::UnknownInstrument(unknown.inst)),
    ] {
        let subs: BTreeSet<_> = [sub(BTC, Feed::Trades), bad].into();
        assert_eq!(ParadexFactory.plan_md(&cfg(), &specs, &subs), Err(err));
        let mut codec = fbc_venue_paradex::md::ParadexMd::new(StreamId(0));
        let mut fx = Effects::new();
        let all: Vec<_> = subs.iter().copied().collect();
        assert_eq!(codec.subscribe(&all, &[], &specs, &mut fx), Err(err));
        assert!(fx.is_empty(), "a refusal sends nothing");
    }
    // No subscriptions, no connection.
    let none = ParadexFactory.plan_md(&cfg(), &specs, &BTreeSet::new());
    assert_eq!(none, Ok(Vec::new()));
}

#[test]
fn the_url_is_configuration_and_the_adapter_writes_its_query() {
    let specs = specs();
    let subs = BTreeSet::new();
    let missing = ParadexFactory.plan_md(&VenueConfig::new(), &specs, &subs);
    assert_eq!(
        missing,
        Err(VenueError::Config(ConfigError::Missing(MD_URL)))
    );
    for url in [
        "https://ws.api.prod.paradex.trade/v1",
        "wss://x.invalid/v1?a=1",
    ] {
        let mut cfg = VenueConfig::new();
        cfg.insert(MD_URL, url);
        let refused = ParadexFactory::md_url(&cfg);
        assert!(
            matches!(refused, Err(ConfigError::Invalid { key: MD_URL, .. })),
            "{url}: {refused:?}"
        );
    }
    let schema = ParadexFactory.config_schema();
    assert_eq!(schema.iter().map(|f| f.key).collect::<Vec<_>>(), [MD_URL]);
}

#[test]
fn the_factory_declares_market_data_only_with_its_cited_limits() {
    let declared = ParadexFactory.caps(&cfg()).unwrap();
    assert_eq!(declared, caps());
    assert_eq!(ParadexFactory.id(), "PARADEX");
    assert!(declared.exec.is_none());
    assert!(ParadexFactory.exec_codec(&cfg(), Secrets::new()).is_none());
    assert!(
        ParadexFactory
            .test_connection(&cfg(), Secrets::new())
            .is_none()
    );
    assert_eq!(ParadexFactory.plan_exec(&cfg()), Ok(Vec::new()));
    // Discovery and the Java-era ticker rule are not built yet (FBC-l5o): both say so.
    let ticker = ParadexFactory.parse_fbc_common_symbol("BTC/USDT");
    assert_eq!(ticker, Err(SymbolError::NoRule));
    let discovered = ParadexFactory.discover(&cfg()).err();
    assert_eq!(discovered, Some(VenueError::NoDiscovery));
    let md = &declared.md;
    assert_eq!(md.encoding, Encoding::Sbe);
    assert_eq!(md.touch_sources.len(), 1);
    assert_eq!(md.touch_sources[0].channel, "bbo");
    assert_eq!(md.trades.source, FeedSource::Stream);
    assert_eq!(
        md.books.len(),
        2,
        "deltas and interactive_deltas (md_book.rs)"
    );
    assert_eq!(
        md.topology,
        ConnTopology::SharedOneBookPerInstrument {
            max_subscriptions: None
        }
    );
    // "20 connections per second or 600 connections per minute per IP address".
    let connects: Vec<_> = declared
        .limits
        .iter()
        .map(|l| (l.scope, l.ops, l.per, l.units))
        .collect();
    let connect = TagSet::of(&[OpKind::Connect]);
    assert_eq!(
        connects,
        [
            (LimitScope::Ip, connect, Duration::from_secs(1), 20),
            (LimitScope::Ip, connect, Duration::from_secs(60), 600),
        ]
    );
}

#[test]
fn the_codec_needs_no_keepalive_timer_or_http() {
    let plan = &ParadexFactory
        .plan_md(&cfg(), &specs(), &[sub(BTC, Feed::Trades)].into())
        .unwrap()[0];
    let mut codec = ParadexFactory.md_codec(&cfg(), plan);
    assert!(
        codec.keepalive().is_none(),
        "the server pings; pongs are the socket's"
    );
    // Public market data: nothing inbound to redact.
    let frame = Inbound::Frame(RawFrame::Binary(&[1, 2]));
    assert_eq!(codec.redact_inbound(frame), InboundSpans::NONE);
    let (mut sink, mut fx) = (Collect::default(), Effects::new());
    codec.on_timer(TimerTag(1), MonoNs(0), WallNs(0), &mut sink, &mut fx);
    let specs = specs();
    let http = dispatch_market_data(&caps(), |scope| {
        let failed = Err(HttpFailure::NotSent);
        codec.on_http(HttpTag(1), failed, scope, &specs, &mut sink, &mut fx)
    });
    assert!(http.is_err());
    assert!(sink.0.is_empty() && fx.is_empty());
}
