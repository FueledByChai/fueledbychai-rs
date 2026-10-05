//! The conformance toy's factory, which the named suite builds the toy from (FBC-8ew): it
//! declares the toy's caps, builds a fresh order-entry codec each time, takes no credentials,
//! and refuses what is not modelled yet (its order-entry URL, FBC-ja3; every market-data feed,
//! FBC-u1d and FBC-z2s) rather than guessing it.

use std::collections::BTreeSet;

use fbc_conformance::toy::{self, EXEC_URL_KEY, INST_A, NoMd, ToyFactory};
use fbc_core::{
    ConfigError, DecodeError, Effects, EndpointPlan, Feed, HttpFailure, HttpTag, Inbound,
    InboundSpans, MdEvent, MdSink, MdTransport, MonoNs, RawFrame, Secrets, StreamId, Subscription,
    SymbolError, TimerTag, VenueConfig, VenueError, VenueFactory, VenueMeta, WallNs, WireUrl,
};

/// A sink that keeps nothing: the toy's market data pushes nothing.
struct Nothing;

impl MdSink for Nothing {
    fn push(&mut self, _meta: VenueMeta, ev: MdEvent) {
        panic!("pushed {ev:?}");
    }
}

fn trades() -> Subscription {
    Subscription {
        inst: INST_A,
        feed: Feed::Trades,
    }
}

#[test]
fn the_toy_factory_declares_the_toys_caps_and_builds_its_codec_without_credentials() {
    let cfg = VenueConfig::new();
    assert_eq!(ToyFactory.id(), "TOY-CONFORMANCE");
    assert!(ToyFactory.config_schema().is_empty());
    assert_eq!(ToyFactory.caps(&cfg), Ok(toy::caps()));
    assert_eq!(
        ToyFactory.parse_fbc_common_symbol("BTC/USDT"),
        Err(SymbolError::NoRule)
    );
    assert!(matches!(
        ToyFactory.discover(&cfg),
        Err(VenueError::NoDiscovery)
    ));
    let codec = ToyFactory.exec_codec(&cfg, Secrets::new());
    assert!(matches!(codec, Some(Ok(_))));
    assert!(ToyFactory.test_connection(&cfg, Secrets::new()).is_none());
}

#[test]
fn the_toy_factory_refuses_what_it_does_not_model_yet() {
    let cfg = VenueConfig::new();
    let refused = ToyFactory.plan_exec(&cfg).unwrap_err();
    let VenueError::Config(ConfigError::Invalid { key, reason }) = refused else {
        panic!("{refused:?}");
    };
    assert_eq!(key, EXEC_URL_KEY);
    assert!(reason.contains("FBC-ja3"), "{reason}");
    let specs = toy::specs();
    let none = ToyFactory.plan_md(&cfg, &specs, &BTreeSet::new());
    assert!(matches!(none, Ok(plans) if plans.is_empty()));
    let subs = BTreeSet::from([trades()]);
    let refused = ToyFactory.plan_md(&cfg, &specs, &subs);
    assert!(matches!(refused, Err(VenueError::UnsupportedFeed(sub)) if sub == trades()));
}

#[test]
fn the_toys_market_data_codec_subscribes_decodes_and_asks_for_nothing() {
    let ep = EndpointPlan {
        stream: StreamId(2),
        transport: MdTransport::Socket {
            url: WireUrl::plain("ws://127.0.0.1/md"),
        },
        subs: Vec::new(),
    };
    let mut md = ToyFactory.md_codec(&VenueConfig::new(), &ep);
    let specs = toy::specs();
    let mut fx = Effects::new();
    md.on_open(&mut fx);
    assert_eq!(md.subscribe(&[], &[trades()], &specs, &mut fx), Ok(()));
    let refused = md.subscribe(&[trades()], &[], &specs, &mut fx);
    assert_eq!(refused, Err(VenueError::UnsupportedFeed(trades())));
    let frame = RawFrame::Text("trade|sym=TOYA-PERP");
    toy::with_scope(|scope| {
        let decoded = md.on_frame(frame, scope, &specs, &mut Nothing, &mut fx);
        assert_eq!(
            decoded,
            Err(DecodeError::Malformed("the toy publishes no market data"))
        );
        let resp = Err(HttpFailure::NotSent);
        let answered = md.on_http(HttpTag(1), resp, scope, &specs, &mut Nothing, &mut fx);
        assert_eq!(
            answered,
            Err(DecodeError::Malformed(
                "the toy's market data asks for no HTTP"
            ))
        );
    });
    md.on_timer(TimerTag(1), MonoNs(1), WallNs(1), &mut Nothing, &mut fx);
    assert!(md.keepalive().is_none());
    assert_eq!(md.redact_inbound(Inbound::Frame(frame)), InboundSpans::NONE);
    assert!(fx.is_empty());
    assert_eq!(format!("{:?}", NoMd), "NoMd");
}
