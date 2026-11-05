//! FBC-3ymj: fbc-conformance's `caps_truthful` (decision 0003) runs on the pre-production
//! probe's caps (decision 0132 item 6) and passes. They declare `CancelOnDisconnect::AccountWide`
//! offering no disarm (decision 0141), so the check expects the arm sent and every disarm
//! refused `NotSent(Unsupported)` with no effect, as the codec does (decision 0132 item 1).
//!
//! The setup is the probe on QFEX's pre-production origins (nothing connects: the check drives
//! the codec itself), one instrument listing every v1 shape (decision 0136), the AsyncAPI's
//! authentication and subscription replies as its bootstrap, so each codec the check builds is
//! authenticated before it encodes, and venue order ids written as UUIDs, the only form QFEX's
//! encoder names an order by. Every key, account, id and frame here is synthetic.

use fbc_conformance::suite::{self, BootReply, Bootstrap, Setup, Subject, Verdict};
use fbc_core::{
    AssetSym, FundingSpec, InstrumentId, InstrumentKind, InstrumentSpec, Lots, OrderKindTag,
    OrderShapes, PriceGrid, Secret, Secrets, SizeStep, SpecTable, TagSet, Tif, TradingStatus,
    UnderlyingId, VenueConfig, VenueFactory, VenueId, WallNs, dispatch_market_data,
};
use fbc_venue_qfex::exec::{CHANNELS, TRADE};
use fbc_venue_qfex::probe::{
    KEY_EXEC_MODE, KEY_PAGE_SIZE, KEY_RESYNC_TIMEOUT, KEY_RPC_TIMEOUT, PREPROD_MDS, PREPROD_PROBE,
    PREPROD_REST, PREPROD_TRADE,
};
use fbc_venue_qfex::{KEY_MDS_URL, KEY_REST_TIMEOUT, KEY_REST_URL, Qfex, auth};

const FIXTURES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../fixtures/qfex/conformance"
);
/// A made-up public key, in the alphabet QFEX's examples show.
const PUBLIC: &str = "qfex_pub_SYNTHETIC_0001";
/// A made-up secret.
const SECRET: &str = "synthetic-qfex-secret-0001";
/// A fixed 0.01 grid, lots of 0.01: `AAPL-USD`.
const AAPL: InstrumentId = InstrumentId::new(7);

/// The probe on QFEX's pre-production origins, for the primary account.
fn probe_config() -> VenueConfig {
    let mut cfg = VenueConfig::new();
    cfg.insert(KEY_EXEC_MODE, PREPROD_PROBE);
    cfg.insert(auth::TRADE_URL, PREPROD_TRADE);
    cfg.insert(KEY_REST_URL, PREPROD_REST);
    cfg.insert(KEY_MDS_URL, PREPROD_MDS);
    cfg.insert(KEY_REST_TIMEOUT, "5000ms");
    cfg.insert(KEY_PAGE_SIZE, "1000");
    cfg.insert(KEY_RESYNC_TIMEOUT, "30s");
    cfg.insert(KEY_RPC_TIMEOUT, "2500ms");
    cfg.insert(auth::ACCOUNT_ID, "primary");
    cfg.insert(auth::ACCOUNT_KEY, "4");
    cfg.insert(auth::PUBLIC_KEY, PUBLIC);
    cfg
}

fn secrets() -> Secrets {
    let mut secrets = Secrets::new();
    secrets.insert(auth::PUBLIC_KEY, Secret::new(PUBLIC.to_owned()));
    secrets.insert(auth::SECRET_KEY, Secret::new(SECRET.to_owned()));
    secrets
}

/// `AAPL-USD`, listing every v1 shape: LIMIT and ALO, GTC and IOC (decision 0136).
fn spec() -> InstrumentSpec {
    let caps = Qfex.caps(&probe_config()).unwrap();
    let venue_symbol = dispatch_market_data(&caps, |scope| scope.venue_symbol("AAPL-USD")).unwrap();
    let usd = AssetSym::new("USD").unwrap();
    InstrumentSpec {
        id: AAPL,
        venue: VenueId::new(4),
        venue_symbol,
        native_id: None,
        underlying: UnderlyingId::new(AAPL.get()),
        kind: InstrumentKind::Perpetual,
        price_grid: PriceGrid::fixed("0.01".parse().unwrap()).unwrap(),
        quote_grid: None,
        size_step: SizeStep::new("0.01".parse().unwrap()).unwrap(),
        min_size: Lots::new(1).unwrap(),
        min_notional: None,
        max_order_size: None,
        position_limit: None,
        price_band: None,
        max_open_orders: None,
        multiplier: rust_decimal::Decimal::ONE,
        quote_ccy: usd,
        settle_ccy: usd,
        funding: FundingSpec::Unknown,
        public_fees: None,
        status: TradingStatus::Trading,
        sessions: None,
        signing: None,
        order_shapes: Some(OrderShapes {
            kinds: TagSet::of(&[OrderKindTag::Limit]),
            post_only: true,
            tifs: TagSet::of(&[Tif::Gtc, Tif::Ioc]),
        }),
        version: 1,
        fetched_at: WallNs(0),
    }
}

/// What the check assumes: the probe, its one instrument, synthetic credentials, the AsyncAPI's
/// `{"authenticated":true}` then a `subscribed` confirmation of each private channel, after
/// which the codec reports its stream authenticated (FBC-bsou), and venue order ids written as
/// UUIDs (`suite::uuid_venue_order_id`, FBC-6rfe), since QFEX's encoder refuses a cancel or a
/// query naming any other form (`src/exec/ids.rs`).
fn probe() -> Setup {
    let mut specs = SpecTable::new();
    specs.insert(spec());
    let mut replies = vec![BootReply::Text(r#"{"authenticated":true}"#.to_owned())];
    replies.extend(
        CHANNELS
            .iter()
            .map(|channel| BootReply::Text(format!(r#"{{"subscribed":"{channel}"}}"#))),
    );
    Setup {
        specs,
        cfg: probe_config(),
        creds: secrets(),
        goldens: Vec::new(),
        exec_stream: TRADE,
        order_entry: None,
        bootstrap: Some(Bootstrap { replies }),
        venue_order_ids: suite::uuid_venue_order_id,
    }
}

/// The done line's QFEX half: `caps_truthful` passes on the probe's caps, probing the arm as a
/// control and the disarm as an absence the codec refuses.
#[test]
fn caps_truthful_passes_on_the_probe_its_disarm_refused_as_the_caps_declare() {
    let subject = Subject::new(&Qfex, FIXTURES, probe).unwrap();
    let verdict = suite::caps_truthful(&subject).unwrap_or_else(|f| panic!("{f}"));
    let Verdict::Passed { probed, .. } = verdict else {
        panic!("{verdict:?}");
    };
    for name in [
        "control: protection (arm)",
        "OrderCaps.cancel_on_disconnect offers no disarm",
        "OrderCaps.cancel_on_disconnect has no dead-man timer (refresh)",
        "control: a placement",
        "control: a cancel",
        "control: a query",
    ] {
        assert!(probed.iter().any(|p| p == name), "{name}: {probed:?}");
    }
}

/// The probe with venue order ids `conformance-<i>`, which are not UUIDs.
fn unspelled() -> Setup {
    Setup {
        venue_order_ids: |i| format!("conformance-{i}"),
        ..probe()
    }
}

/// Why the setup writes UUIDs: with any other form the codec refuses the cancel and the query
/// controls as unencodable, so the check could not pass on the probe whatever its disarm.
#[test]
fn without_uuid_venue_order_ids_the_cancel_and_query_controls_fail() {
    let subject = Subject::new(&Qfex, FIXTURES, unspelled).unwrap();
    let failure = suite::caps_truthful(&subject).unwrap_err();
    let named: Vec<&str> = failure
        .breaches
        .iter()
        .map(|b| b.capability.as_str())
        .collect();
    assert_eq!(
        named,
        ["control: a cancel", "control: a query"],
        "{failure}"
    );
    for breach in &failure.breaches {
        assert!(
            breach.what.starts_with("refused as NotSent(Unencodable)"),
            "{failure}"
        );
    }
}
