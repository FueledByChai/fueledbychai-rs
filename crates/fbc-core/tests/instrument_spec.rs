//! Design §4.4, decisions 0003 and 0004: `InstrumentSpec::quantize` is the only place a model
//! price becomes an order price, and it is maker-safe on every kind of grid: a bid floors and
//! an ask ceils onto the grid valid at the resulting price. `floor_qty` floors a size onto the
//! size step and refuses one below the minimum. The spec below is synthetic.

use std::time::Duration;

mod common;

use common::market_data_only_caps;
use fbc_core::{
    AssetSym, Bps, FeeRate, FeeSchedule, FundingSpec, InstrumentId, InstrumentKind, InstrumentSpec,
    Lots, Money, PriceGrid, PublishedRates, QtyError, QuantizeError, Side, SizeStep, Ticks,
    TradingStatus, UnderlyingId, VenueId, VenueNativeId, WallNs, dispatch_market_data,
};
use rust_decimal::Decimal;

fn dec(text: &str) -> Decimal {
    text.parse().unwrap()
}

fn usd() -> AssetSym {
    AssetSym::new("USD").unwrap()
}

/// A synthetic perpetual on `grid`: size step 0.001, minimum 10 lots.
fn spec(grid: PriceGrid) -> InstrumentSpec {
    // A venue symbol comes only from the decode scope, in tests as well (decision 0004).
    let venue_symbol = dispatch_market_data(&market_data_only_caps(), |scope| {
        scope.venue_symbol("SYN-USD-PERP")
    })
    .unwrap();
    InstrumentSpec {
        id: InstrumentId::new(1),
        venue: VenueId::new(1),
        venue_symbol,
        native_id: Some(VenueNativeId::Number(2)),
        underlying: UnderlyingId::new(3),
        kind: InstrumentKind::Perpetual,
        price_grid: grid,
        quote_grid: None,
        size_step: SizeStep::new(dec("0.001")).unwrap(),
        min_size: Lots::new(10).unwrap(),
        min_notional: Some(Money::new(10_000_000_000, usd())),
        max_order_size: Some(Lots::new(1_000_000).unwrap()),
        position_limit: None,
        price_band: Some(Bps(500.0)),
        max_open_orders: Some(100),
        multiplier: Decimal::ONE,
        quote_ccy: usd(),
        settle_ccy: AssetSym::new("USDC").unwrap(),
        funding: FundingSpec::Known {
            interval: Duration::from_secs(3_600),
            cap: None,
        },
        public_fees: Some(FeeSchedule {
            public: PublishedRates {
                maker: FeeRate(Bps(-0.5)),
                taker: FeeRate(Bps(2.0)),
            },
            rpi: None,
        }),
        status: TradingStatus::Trading,
        version: 1,
        fetched_at: WallNs(1_759_363_200_000_000_000),
    }
}

fn fixed(tick: &str) -> InstrumentSpec {
    spec(PriceGrid::fixed(dec(tick)).unwrap())
}

fn quote(spec: &InstrumentSpec, px: f64) -> (Ticks, Ticks) {
    (
        spec.quantize(Side::Buy, px).unwrap(),
        spec.quantize(Side::Sell, px).unwrap(),
    )
}

#[test]
fn quantize_floors_bids_and_ceils_asks_on_a_fixed_grid() {
    let half = fixed("0.5");
    // 65432.7 lies between 65432.5 (130_865 ticks) and 65433.0 (130_866).
    assert_eq!(quote(&half, 65_432.7), (Ticks(130_865), Ticks(130_866)));
    // On the grid, both sides keep the price.
    assert_eq!(quote(&half, 65_432.5), (Ticks(130_865), Ticks(130_865)));
    // Below zero the bid still moves down and the ask up.
    assert_eq!(quote(&half, -1.25), (Ticks(-3), Ticks(-2)));
    // 0.3 means 0.3, not its binary value 0.2999...: on a 0.1 grid it is three ticks a side.
    let tenth = fixed("0.1");
    assert_eq!(quote(&tenth, 0.3), (Ticks(3), Ticks(3)));
    // 0.1 + 0.2 prints as 0.30000000000000004: above 0.3, so the ask rises to 0.4.
    assert_eq!(quote(&tenth, 0.1 + 0.2), (Ticks(3), Ticks(4)));
}

#[test]
fn quantize_floors_bids_and_ceils_asks_on_a_significant_figure_grid() {
    // Five figures, six decimals: 1.234567 steps by 0.0001 (100 finest ticks).
    let five = spec(PriceGrid::sig_figs(5, 6, false).unwrap());
    assert_eq!(
        quote(&five, 1.234_567),
        (Ticks(1_234_500), Ticks(1_234_600))
    );
    // An ask just under 10 rises to 10.000, valid on the coarser grid of the next decade.
    assert_eq!(five.quantize(Side::Sell, 9.999_96), Ok(Ticks(10_000_000)));
    // A bid just over 10 falls by the step valid there (0.001), not the step below 10.
    assert_eq!(five.quantize(Side::Buy, 10.000_49), Ok(Ticks(10_000_000)));
    assert_eq!(five.quantize(Side::Sell, 10.000_49), Ok(Ticks(10_001_000)));
    // Hyperliquid-like: five figures, one decimal, integers always valid.
    let hl = spec(PriceGrid::sig_figs(5, 1, true).unwrap());
    assert_eq!(quote(&hl, 65_432.17), (Ticks(654_320), Ticks(654_330)));
    assert_eq!(quote(&hl, 1_234.56), (Ticks(12_345), Ticks(12_346)));
}

#[test]
fn quantize_floors_bids_and_ceils_asks_on_a_banded_grid() {
    // 0.0001 below 1, 0.01 from 1.
    let bands =
        spec(PriceGrid::banded(&[(dec("0"), dec("0.0001")), (dec("1"), dec("0.01"))]).unwrap());
    assert_eq!(quote(&bands, 1.005_67), (Ticks(10_000), Ticks(10_100)));
    assert_eq!(quote(&bands, 0.999_95), (Ticks(9_999), Ticks(10_000)));
    // 0.3 below 1.3, 1 from 1.3: a bid at 1.5 finds no multiple of 1 between 1.3 and 1.5, so it
    // falls into the band below, to 1.2; an ask at 1.25 rises past 1.3 to 2.0.
    let coarse =
        spec(PriceGrid::banded(&[(dec("0"), dec("0.3")), (dec("1.3"), dec("1"))]).unwrap());
    assert_eq!(coarse.quantize(Side::Buy, 1.5), Ok(Ticks(12)));
    assert_eq!(coarse.quantize(Side::Sell, 1.25), Ok(Ticks(20)));
    // Below the first band an ask rises to it and a bid has nowhere maker-safe to go.
    let from_one = spec(PriceGrid::banded(&[(dec("1"), dec("0.25"))]).unwrap());
    assert_eq!(from_one.quantize(Side::Sell, 0.5), Ok(Ticks(4)));
    assert_eq!(
        from_one.quantize(Side::Buy, 0.5),
        Err(QuantizeError::NoValidPrice)
    );
}

/// Deterministic pseudo-random prices in `lo..hi`.
fn prices(lo: f64, hi: f64, n: usize) -> impl Iterator<Item = f64> {
    let mut state: u64 = 0x2545_f491_4f6c_dd1d;
    (0..n).map(move |_| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        lo + (hi - lo) * ((state >> 11) as f64 / (1u64 << 53) as f64)
    })
}

#[test]
fn every_quantized_price_is_valid_and_the_nearest_on_its_maker_safe_side() {
    let specs = [
        fixed("0.25"),
        spec(PriceGrid::sig_figs(3, 4, false).unwrap()),
        spec(PriceGrid::banded(&[(dec("0"), dec("0.01")), (dec("5"), dec("0.05"))]).unwrap()),
    ];
    for spec in &specs {
        let grid = &spec.price_grid;
        let price = |t: Ticks| Decimal::from(t.0) * grid.finest();
        for px in prices(0.05, 20.0, 2_000) {
            let exact: Decimal = px.to_string().parse().unwrap();
            let bid = spec.quantize(Side::Buy, px).unwrap();
            let ask = spec.quantize(Side::Sell, px).unwrap();
            assert!(grid.valid_at(bid) && grid.valid_at(ask), "{grid:?} {px}");
            assert!(price(bid) <= exact && exact <= price(ask), "{grid:?} {px}");
            // No valid price lies strictly between the quote and the model price.
            let between = (bid.0 + 1..ask.0).map(Ticks).filter(|&t| grid.valid_at(t));
            for t in between {
                assert_eq!(price(t), exact, "{grid:?} {px}: {t:?} is closer");
            }
        }
    }
}

#[test]
fn quantize_refuses_what_is_not_a_price_and_keeps_tiny_ones_maker_safe() {
    let tenth = fixed("0.1");
    for side in [Side::Buy, Side::Sell] {
        assert_eq!(
            tenth.quantize(side, f64::NAN),
            Err(QuantizeError::NotFinite)
        );
        assert_eq!(
            tenth.quantize(side, f64::INFINITY),
            Err(QuantizeError::NotFinite)
        );
        assert_eq!(tenth.quantize(side, 1e300), Err(QuantizeError::OutOfRange));
        assert_eq!(tenth.quantize(side, 1e18), Err(QuantizeError::OutOfRange));
        assert_eq!(tenth.quantize(side, 0.0), Ok(Ticks(0)));
        assert_eq!(tenth.quantize(side, -0.0), Ok(Ticks(0)));
    }
    // Far below one tick: a bid floors to the tick at or below, an ask ceils to the one above.
    assert_eq!(quote(&tenth, 1e-300), (Ticks(0), Ticks(1)));
    assert_eq!(quote(&tenth, -1e-300), (Ticks(-1), Ticks(0)));
    assert_eq!(quote(&tenth, f64::MIN_POSITIVE), (Ticks(0), Ticks(1)));
    // The largest index that fits is reachable; one tick more is not.
    let one = fixed("1");
    assert_eq!(
        one.quantize(Side::Buy, 9.0e18),
        Ok(Ticks(9_000_000_000_000_000_000))
    );
    assert_eq!(
        one.quantize(Side::Buy, 1.0e19),
        Err(QuantizeError::OutOfRange)
    );
    for error in [
        QuantizeError::NotFinite,
        QuantizeError::OutOfRange,
        QuantizeError::NoValidPrice,
    ] {
        assert!(!error.to_string().is_empty());
    }
}

#[test]
fn floor_qty_floors_onto_the_size_step_and_rejects_a_size_below_min_size() {
    let spec = fixed("0.5");
    let lots = |n: i64| Lots::new(n).unwrap();
    assert_eq!(spec.floor_qty(0.0123), Ok(lots(12)));
    assert_eq!(spec.floor_qty(0.01), Ok(lots(10)));
    assert_eq!(
        spec.floor_qty(0.0099),
        Err(QtyError::BelowMinSize {
            lots: lots(9),
            min_size: lots(10)
        })
    );
    assert_eq!(
        spec.floor_qty(0.0),
        Err(QtyError::BelowMinSize {
            lots: lots(0),
            min_size: lots(10)
        })
    );
    assert_eq!(
        spec.floor_qty(1e-300),
        Err(QtyError::BelowMinSize {
            lots: lots(0),
            min_size: lots(10)
        })
    );
    // 0.3 on a 0.1 step is three lots, not two.
    let tenth_step = InstrumentSpec {
        size_step: SizeStep::new(dec("0.1")).unwrap(),
        min_size: lots(1),
        ..fixed("0.5")
    };
    assert_eq!(tenth_step.floor_qty(0.3), Ok(lots(3)));
    // Zero lots is never an order, even where the declared minimum is zero.
    let no_minimum = InstrumentSpec {
        min_size: lots(0),
        ..fixed("0.5")
    };
    assert_eq!(no_minimum.floor_qty(0.001), Ok(lots(1)));
    assert_eq!(
        no_minimum.floor_qty(0.0009),
        Err(QtyError::BelowMinSize {
            lots: lots(0),
            min_size: lots(0)
        })
    );
    assert_eq!(spec.floor_qty(-0.01), Err(QtyError::Negative));
    assert_eq!(spec.floor_qty(f64::NAN), Err(QtyError::NotFinite));
    assert_eq!(spec.floor_qty(1e300), Err(QtyError::OutOfRange));
    let message = QtyError::BelowMinSize {
        lots: lots(9),
        min_size: lots(10),
    }
    .to_string();
    assert!(
        message.contains("9 lots") && message.contains("10"),
        "{message}"
    );
    for error in [
        QtyError::NotFinite,
        QtyError::Negative,
        QtyError::OutOfRange,
    ] {
        assert!(!error.to_string().is_empty());
    }
}

#[test]
fn notional_is_price_times_size_times_multiplier_in_quote_nanos() {
    let half = fixed("0.5");
    let lots = |n: i64| Lots::new(n).unwrap();
    // 65432.5 × 12 × 0.001 = 785.19 USD.
    assert_eq!(
        half.notional(Ticks(130_865), lots(12)),
        Some(Money::new(785_190_000_000, usd()))
    );
    let doubled = InstrumentSpec {
        multiplier: dec("2"),
        ..fixed("0.5")
    };
    assert_eq!(
        doubled.notional(Ticks(130_865), lots(12)),
        Some(Money::new(1_570_380_000_000, usd()))
    );
    // Below a nano the notional rounds half to even.
    let fine = InstrumentSpec {
        size_step: SizeStep::new(dec("1")).unwrap(),
        ..fixed("0.0000000001")
    };
    assert_eq!(fine.notional(Ticks(5), lots(1)), Some(Money::new(0, usd())));
    assert_eq!(
        fine.notional(Ticks(15), lots(1)),
        Some(Money::new(2, usd()))
    );
    assert_eq!(half.notional(Ticks(i64::MAX), lots(i64::MAX)), None);
    // Far below a nano (1e-56 USD), the notional is zero.
    let tiny = InstrumentSpec {
        size_step: SizeStep::new(dec("0.0000000000000000000000000001")).unwrap(),
        ..fixed("0.0000000000000000000000000001")
    };
    assert_eq!(tiny.notional(Ticks(1), lots(1)), Some(Money::new(0, usd())));
}

#[test]
fn tick_bps_is_the_step_at_a_price_in_basis_points_of_it() {
    let half = fixed("0.5");
    let bps = half.tick_bps(Ticks(130_865)).unwrap().0;
    assert!((bps - 0.5 / 65_432.5 * 1e4).abs() < 1e-12, "{bps}");
    assert_eq!(half.tick_bps(Ticks(-200)), Some(Bps(50.0)));
    assert_eq!(half.tick_bps(Ticks(0)), None);
    // On a significant-figure grid the step, and so the tick in bps, depends on the price.
    let five = spec(PriceGrid::sig_figs(5, 6, false).unwrap());
    assert_eq!(
        five.tick_bps(Ticks(1_234_500)),
        Some(Bps(100.0 / 1_234_500.0 * 1e4))
    );
}

#[test]
fn a_spec_states_every_field_and_a_size_step_is_positive() {
    assert_eq!(SizeStep::new(dec("0")), None);
    assert_eq!(SizeStep::new(dec("-0.1")), None);
    assert_eq!(
        SizeStep::new(dec("0.0010")).unwrap().get().to_string(),
        "0.001"
    );
    let spec = fixed("0.5");
    assert_eq!(spec.venue_symbol.as_wire(), "SYN-USD-PERP");
    assert_eq!(spec.clone(), spec);
    let dated = InstrumentSpec {
        kind: InstrumentKind::Future {
            expiry: WallNs(1_767_225_600_000_000_000),
        },
        funding: FundingSpec::NotApplicable,
        native_id: Some(VenueNativeId::Text("asset-7".into())),
        status: TradingStatus::ReduceOnly,
        version: 2,
        ..fixed("0.5")
    };
    assert_ne!(dated, spec);
    assert_eq!(dated.funding, FundingSpec::NotApplicable);
    assert_ne!(FundingSpec::Unknown, FundingSpec::NotApplicable);
}

#[test]
fn a_high_precision_unit_does_not_overflow_an_index_that_fits() {
    // A 28-digit tick and size step: 1e11 / 0.1234567890123456789012345678 is
    // 810_000_007_290.00006..., far inside an i64.
    let unit = dec("0.1234567890123456789012345678");
    let precise = InstrumentSpec {
        size_step: SizeStep::new(unit).unwrap(),
        min_size: Lots::new(1).unwrap(),
        ..spec(PriceGrid::fixed(unit).unwrap())
    };
    assert_eq!(
        precise.floor_qty(1e11),
        Ok(Lots::new(810_000_007_290).unwrap())
    );
    assert_eq!(
        quote(&precise, 1e11),
        (Ticks(810_000_007_290), Ticks(810_000_007_291))
    );
    assert_eq!(
        quote(&precise, -1e11),
        (Ticks(-810_000_007_291), Ticks(-810_000_007_290))
    );
    // Exactly on the grid, both sides keep the index.
    assert_eq!(quote(&precise, 0.0), (Ticks(0), Ticks(0)));
}

#[test]
fn a_notional_beyond_decimal_nanos_still_fits_money() {
    // 1e10 ticks of 1 times 1e10 lots of 1 is 1e20 USD, 1e29 nanos: past a Decimal, inside
    // an i128.
    let unit = InstrumentSpec {
        size_step: SizeStep::new(dec("1")).unwrap(),
        ..fixed("1")
    };
    assert_eq!(
        unit.notional(Ticks(10_000_000_000), Lots::new(10_000_000_000).unwrap()),
        Some(Money::new(100_000_000_000_000_000_000_000_000_000, usd()))
    );
}

#[test]
fn a_notional_whose_intermediate_product_is_beyond_a_decimal_still_fits() {
    // A price of 1e28 (1e18 ticks of 1e10) times 100 lots of 1 is past a Decimal, but the
    // 0.0001 multiplier brings the notional to 1e26 USD, 1e35 nanos.
    let huge = InstrumentSpec {
        size_step: SizeStep::new(dec("1")).unwrap(),
        multiplier: dec("0.0001"),
        ..fixed("10000000000")
    };
    assert_eq!(
        huge.notional(Ticks(1_000_000_000_000_000_000), Lots::new(100).unwrap()),
        Some(Money::new(10_i128.pow(35), usd()))
    );
    // Mantissas whose product passes i128 fall back to Decimal arithmetic, which rounds at
    // 28 significant digits but still answers.
    let wide = InstrumentSpec {
        size_step: SizeStep::new(dec("0.123")).unwrap(),
        multiplier: dec("1.2345"),
        ..fixed("0.000000001")
    };
    let money = wide
        .notional(
            Ticks(123_456_789_012_345_678),
            Lots::new(1_000_000_000_000_000_000).unwrap(),
        )
        .unwrap();
    // 123456789.012345678 USD a unit times 1e18 lots of 0.123 times 1.2345, in nanos.
    let expected = 123_456_789.012_345_7 * 1e18 * 0.123 * 1.2345 * 1e9;
    assert!(
        ((money.nanos as f64) / expected - 1.0).abs() < 1e-12,
        "{money:?}"
    );
}

#[test]
fn a_maker_safe_price_past_the_end_of_i64_is_out_of_range() {
    // Two figures, integer finest step: 9.21e18 fits an i64, but the ask's grid price 9.3e18
    // does not; that is out of range, not "no valid price".
    let two = spec(PriceGrid::sig_figs(2, 0, false).unwrap());
    assert_eq!(
        two.quantize(Side::Sell, 9.21e18),
        Err(QuantizeError::OutOfRange)
    );
    assert_eq!(
        two.quantize(Side::Buy, -9.21e18),
        Err(QuantizeError::OutOfRange)
    );
    assert_eq!(
        two.quantize(Side::Buy, 9.21e18),
        Ok(Ticks(9_200_000_000_000_000_000))
    );
}
