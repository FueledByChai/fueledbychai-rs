//! The conformance toy's factory, which the named suite builds the toy from (FBC-8ew): it
//! declares the toy's caps, builds a fresh order-entry codec each time, takes no credentials,
//! and refuses what is not modelled yet (its order-entry URL, FBC-ja3; the market-data feeds
//! other than books, FBC-z2s) rather than guessing it. Its books are `tests/toy_books.rs`.

use std::collections::BTreeSet;

use fbc_conformance::toy::{self, BOOK, EXEC_URL_KEY, INST_A, ToyFactory};
use fbc_core::{
    ConfigError, Effects, EndpointPlan, Feed, InstrumentId, InstrumentKind, MdTransport, Secrets,
    StreamId, Subscription, SymbolError, VenueConfig, VenueError, VenueFactory, WireUrl,
};

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
    let usdt = ToyFactory.parse_fbc_common_symbol("TOYA/USDT").unwrap();
    assert_eq!(
        (usdt.base.as_str(), usdt.quote.as_str(), usdt.kind),
        ("TOYA", "USDT", InstrumentKind::Perpetual)
    );
    assert_eq!(
        ToyFactory.parse_fbc_common_symbol("TOYA/USD"),
        Err(SymbolError::Unmapped)
    );
    assert_eq!(
        ToyFactory.parse_fbc_common_symbol("TOYA-PERP"),
        Err(SymbolError::NotCommonForm)
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
    // An instrument missing from the spec table is named as such, before its feed or the
    // market-data URL (Codex r4203051296).
    let unknown = InstrumentId::new(99);
    for feed in [Feed::Book(BOOK), Feed::Trades] {
        let subs = BTreeSet::from([Subscription {
            inst: unknown,
            feed,
        }]);
        let refused = ToyFactory.plan_md(&cfg, &specs, &subs);
        assert_eq!(refused, Err(VenueError::UnknownInstrument(unknown)));
    }
    let ep = EndpointPlan {
        stream: StreamId(2),
        transport: MdTransport::Socket {
            url: WireUrl::plain("ws://127.0.0.1/md"),
        },
        subs: Vec::new(),
    };
    let mut md = ToyFactory.md_codec(&cfg, &ep);
    let mut fx = Effects::new();
    let refused = md.subscribe(&[trades()], &[], &specs, &mut fx);
    assert_eq!(refused, Err(VenueError::UnsupportedFeed(trades())));
    assert!(fx.is_empty());
}
