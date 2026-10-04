//! The signer behind fbc-core's `OrderSigner`: a `PlaceWire` or `AmendWire` built from an
//! instrument spec (ticks and lots) signs to the same r and s the Java signer wrote for the same
//! order, cancels are unsigned, and what the Paradex message cannot carry is refused. The spec
//! is synthetic; the vectors are the Java ones.

mod common;

use std::time::Duration;

use common::{Row, Vectors};
use fbc_core::{
    AmendRef, AmendWire, AssetSym, CancelRef, CancelWire, Channel, ConnTopology, Encoding,
    FeedSource, FundingCaps, FundingSpec, InstrumentId, InstrumentKind, InstrumentSpec, Lots,
    MatchingCaps, MdCaps, OrderKind, OrderSigner, PlaceWire, PriceGrid, Readiness, Side, Sig,
    SignError, SizeStep, StpScope, Ticks, Tif, TradeCaps, TradingStatus, UnderlyingId, VenueCaps,
    VenueId, VenueOrderId, WallNs, dispatch_market_data,
};
use rust_decimal::Decimal;

fn dec(text: &str) -> Decimal {
    text.parse().unwrap()
}

/// BTC-USD-PERP on a `tick` grid with a 0.00001 size step.
fn spec_on(tick: &str) -> InstrumentSpec {
    let venue_symbol =
        dispatch_market_data(&decode_caps(), |scope| scope.venue_symbol("BTC-USD-PERP")).unwrap();
    let usd = AssetSym::new("USD").unwrap();
    InstrumentSpec {
        id: InstrumentId::new(1),
        venue: VenueId::new(1),
        venue_symbol,
        native_id: None,
        underlying: UnderlyingId::new(1),
        kind: InstrumentKind::Perpetual,
        price_grid: PriceGrid::fixed(dec(tick)).unwrap(),
        quote_grid: None,
        size_step: SizeStep::new(dec("0.00001")).unwrap(),
        min_size: Lots::new(1).unwrap(),
        min_notional: None,
        max_order_size: None,
        position_limit: None,
        price_band: None,
        max_open_orders: None,
        multiplier: Decimal::ONE,
        quote_ccy: usd,
        settle_ccy: AssetSym::new("USDC").unwrap(),
        funding: FundingSpec::Known {
            interval: Duration::from_secs(3_600),
            cap: None,
        },
        public_fees: None,
        status: TradingStatus::Trading,
        version: 1,
        fetched_at: WallNs(0),
    }
}

/// What the tests decode venue symbols and order ids under. A `DecodeScope` is lent only for a
/// venue's caps; these are synthetic and market-data-only, since the signer tests decode no
/// client id or fee. They are not Paradex's declaration, which lands with its codecs.
fn decode_caps() -> VenueCaps {
    VenueCaps {
        exec: None,
        matching: MatchingCaps {
            speed_bump: None,
            stp_scope: StpScope::None,
        },
        md: MdCaps {
            encoding: Encoding::Json,
            touch_sources: vec![],
            books: vec![],
            trades: TradeCaps {
                source: FeedSource::None,
                aggressor: false,
                trade_id: false,
            },
            funding: FundingCaps {
                source: FeedSource::None,
                interval_reported: false,
                next_time_reported: false,
            },
            stats: FeedSource::None,
            mark: FeedSource::None,
            index: FeedSource::None,
            ts_precision: Duration::from_millis(1),
            topology: ConnTopology::PerInstrument,
            max_conn_lifetime: None,
        },
        limits: vec![],
        readiness_ceiling: Readiness::Record,
    }
}

fn spec() -> InstrumentSpec {
    spec_on("0.1")
}

fn venue_id(text: &str) -> VenueOrderId {
    dispatch_market_data(&decode_caps(), |scope| scope.venue_order_id(text)).unwrap()
}

fn wall(row: &Row) -> WallNs {
    WallNs(i64::try_from(row.u64("timestamp")).unwrap() * 1_000_000)
}

/// The signature as Paradex's wire carries it and the Java signer writes it
/// (`ParadexTypedDataSigner.toParadexArray`): `["r","s"]`, both in decimal.
fn expected(row: &Row) -> Sig {
    let wire = format!(r#"["{}","{}"]"#, row.felt("r"), row.felt("s"));
    Sig::new(wire.as_bytes()).unwrap()
}

fn place<'a>(
    spec: &'a InstrumentSpec,
    side: Side,
    kind: OrderKind,
    lots: i64,
    wall: WallNs,
) -> PlaceWire<'a> {
    PlaceWire {
        spec,
        cid: "c1",
        side,
        kind,
        qty: Lots::new(lots).unwrap(),
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        wall,
        nonce: None,
    }
}

#[test]
fn a_place_wire_signs_as_java_signed_the_same_order() {
    let vectors = Vectors::read();
    let mut signer = vectors.signer();
    // buy_limit: 0.01 BTC at 65123.45, so 1,000 lots at tick 6,512,345 of a 0.01 grid.
    let fine = spec_on("0.01");
    let row = vectors.row("buy_limit");
    let w = place(
        &fine,
        Side::Buy,
        OrderKind::Limit {
            px: Ticks(6_512_345),
        },
        1_000,
        wall(row),
    );
    assert_eq!(signer.sign_place(&w), Ok(expected(row)));

    // market_sell: 0.5 BTC at market, so 50,000 lots and price 0.
    let spec = spec();
    let row = vectors.row("market_sell");
    let w = place(&spec, Side::Sell, OrderKind::Market, 50_000, wall(row));
    assert_eq!(signer.sign_place(&w), Ok(expected(row)));
}

#[test]
fn an_amend_wire_signs_the_venue_order_id_as_java_signed_the_modify() {
    let vectors = Vectors::read();
    let mut signer = vectors.signer();
    let spec = spec();
    let row = vectors.row("modify");
    let id = venue_id(row.get("order_id"));
    let w = AmendWire {
        spec: &spec,
        target: AmendRef::Venue(&id),
        side: Side::Sell,
        px: Ticks(652_001),
        qty: Lots::new(2_000).unwrap(),
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        wall: wall(row),
        nonce: None,
    };
    assert_eq!(signer.sign_amend(&w), Ok(expected(row)));
    // Paradex modifies by its own order id; a client-id amend has nothing Java ever signed.
    let by_client = AmendWire {
        target: AmendRef::Client("c1"),
        ..w
    };
    assert_eq!(
        signer.sign_amend(&by_client),
        Err(SignError::Unsignable("amend by client id"))
    );
}

#[test]
fn cancels_are_not_signed() {
    let vectors = Vectors::read();
    let mut signer = vectors.signer();
    let spec = spec();
    let id = venue_id("1");
    for target in [
        CancelRef::Venue(&id),
        CancelRef::Client("c1"),
        CancelRef::PlacementNonce(1),
    ] {
        let w = CancelWire {
            spec: &spec,
            target,
            side: Side::Buy,
            wall: WallNs(1),
            nonce: None,
        };
        assert_eq!(signer.sign_cancel(&w), Ok(None));
    }
}

#[test]
fn what_the_message_cannot_carry_is_refused() {
    let vectors = Vectors::read();
    let mut signer = vectors.signer();
    let spec = spec();
    let before_epoch = place(&spec, Side::Buy, OrderKind::Market, 1, WallNs(-1));
    assert_eq!(
        signer.sign_place(&before_epoch),
        Err(SignError::Unsignable("timestamp before 1970"))
    );
    let negative_price = place(
        &spec,
        Side::Buy,
        OrderKind::Limit { px: Ticks(-1) },
        1,
        WallNs(0),
    );
    assert_eq!(
        signer.sign_place(&negative_price),
        Err(SignError::Unsignable("negative price"))
    );
    let big_step = InstrumentSpec {
        size_step: SizeStep::new(dec("10000000000")).unwrap(),
        ..spec.clone()
    };
    let huge = place(&big_step, Side::Buy, OrderKind::Market, i64::MAX, WallNs(0));
    assert_eq!(
        signer.sign_place(&huge),
        Err(SignError::Unsignable("size out of range"))
    );
    let tiny = InstrumentSpec {
        price_grid: PriceGrid::fixed(Decimal::new(1, 28)).unwrap(),
        ..spec.clone()
    };
    let past_decimal = place(
        &tiny,
        Side::Buy,
        OrderKind::Limit {
            px: Ticks(i64::MAX),
        },
        1,
        WallNs(0),
    );
    assert!(
        signer.sign_place(&past_decimal).is_ok(),
        "fits: 9.2e-10, truncates to 0"
    );
    let coarse = InstrumentSpec {
        price_grid: PriceGrid::fixed(dec("1000000000000000000")).unwrap(),
        ..spec.clone()
    };
    let overflow = place(
        &coarse,
        Side::Buy,
        OrderKind::Limit {
            px: Ticks(i64::MAX),
        },
        1,
        WallNs(0),
    );
    assert_eq!(
        signer.sign_place(&overflow),
        Err(SignError::Unsignable("price out of range"))
    );
}
