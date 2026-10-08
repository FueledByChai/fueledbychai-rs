//! The conformance toy's factory, which the named suite builds the toy from (FBC-8ew): it
//! declares the toy's caps, builds a fresh order-entry codec each time, takes no credentials,
//! and refuses what is not modelled yet (the market-data feeds other than books, FBC-z2s) or not
//! configured rather than guessing it. Its books are `tests/toy_books.rs`, its configured URLs
//! `tests/toy_urls.rs`.

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
fn the_toy_factory_refuses_what_it_does_not_model_yet_or_is_not_configured() {
    let cfg = VenueConfig::new();
    let refused = ToyFactory.plan_exec(&cfg);
    assert_eq!(
        refused,
        Err(VenueError::Config(ConfigError::Missing(EXEC_URL_KEY)))
    );
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
    // Even when a known instrument's undeclared feed sorts first (Codex r4216473740).
    let mixed = BTreeSet::from([
        trades(),
        Subscription {
            inst: unknown,
            feed: Feed::Book(BOOK),
        },
    ]);
    assert_eq!(mixed.first(), Some(&trades()));
    let refused = ToyFactory.plan_md(&cfg, &specs, &mixed);
    assert_eq!(refused, Err(VenueError::UnknownInstrument(unknown)));
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
