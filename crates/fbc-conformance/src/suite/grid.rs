//! `price_grid` (design §4.4 and §6, decisions 0003 and 0004): every price quantized on the
//! venue's grids is a valid order price, and a post-only order stays maker-safe at the grids'
//! boundaries, where the tick changes, from the model's price to the bytes the codec sends.
//!
//! For every instrument in the [`Setup`](super::Setup)'s spec table, model prices are taken
//! around each boundary of its grid: every power of ten from 10 to 10⁹ finest ticks (where a
//! significant-figure grid's step changes) and every band start of a banded grid, at the
//! boundary and half a finest tick and one finest tick either side. Each is quantized as a bid
//! and as an ask ([`InstrumentSpec::quantize`]): the price it gives must be valid on the grid
//! and never more aggressive than the model's, a bid at or below it and an ask at or above. A
//! bid with no valid price below it (under a banded grid's first band) is refused, as it must
//! be, and not sent.
//!
//! Then, where the caps allow a post-only limit order, each price quantized is placed as a
//! post-only order on its side by a codec built fresh from the factory: it must be sent,
//! carrying its request, and no two of them may encode alike. A codec that rounds a price it
//! is given onto a coarser grid sends two quantized prices as one, and a bid it rounds up or
//! an ask it rounds down is no longer maker-safe. A breach names the instrument, the side and
//! the price, never the bytes.
//!
//! Never skipped: every venue's grids are quantized. A venue whose caps declare no order entry,
//! or no post-only limit order, sends nothing, and says so in what it probed.

use std::collections::{BTreeSet, HashMap};

use fbc_core::{
    Channel, Effects, InstrumentSpec, Lots, NewOrder, OrderCaps, OrderKind, OrderKindTag,
    PriceGrid, QuantizeError, Side, Ticks, TifTag, VenueCommand,
};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use super::harness::{Harness, Ids, RPC, Shape};
use super::{Breach, Failure, Subject, Verdict};

const CHECK: &str = "price_grid";

/// The largest power of ten, in finest ticks, a boundary is taken at.
const TOP_POWER: u32 = 9;

/// Runs `price_grid` against `subject`.
pub fn price_grid(subject: &Subject<'_>) -> Result<Verdict, Failure> {
    let h = Harness::new(CHECK, subject)?;
    let post_only = h.caps.exec.as_ref().and_then(|e| post_only_limit(&e.order));
    let ids = Ids::new(&h, 1)?;
    let mut breaches = Vec::new();
    let mut probed = Vec::new();
    for spec in h.specs().iter() {
        let symbol = spec.venue_symbol.as_wire();
        let capability = format!("InstrumentSpec.price_grid of {symbol}");
        // Each order price quantized, once, in the order first quantized.
        let mut quoted: Vec<(Side, Ticks)> = Vec::new();
        let models = model_prices(&spec.price_grid);
        for &model in &models {
            for side in [Side::Buy, Side::Sell] {
                let got = spec.quantize(side, model.to_f64().unwrap_or(f64::NAN));
                match judge(&spec.price_grid, side, model, got) {
                    Ok(Some(px)) if !quoted.contains(&(side, px)) => quoted.push((side, px)),
                    Ok(_) => {}
                    Err(what) => breaches.push(Breach::new(&capability, what)),
                }
            }
        }
        let sent = match post_only {
            Some(shape) => {
                let sent = send(&h, spec, shape, &ids, &quoted, &mut breaches)?;
                format!("{sent} post-only orders sent")
            }
            None => "no post-only limit order the caps allow to send".to_owned(),
        };
        probed.push(format!(
            "{symbol}: {} model prices quantized to {} order prices, {sent}",
            models.len(),
            quoted.len()
        ));
    }
    if breaches.is_empty() {
        Ok(Verdict::Passed {
            check: CHECK,
            probed,
        })
    } else {
        Err(Failure {
            check: CHECK,
            breaches,
        })
    }
}

/// The first post-only limit order the caps allow, of their times in force and channels in
/// declaration order; `None` when they allow none.
fn post_only_limit(o: &OrderCaps) -> Option<Shape> {
    if !o.kinds.contains(OrderKindTag::Limit) {
        return None;
    }
    let tifs: Vec<TifTag> = o.tifs.iter().collect();
    let channels: Vec<Channel> = o.channels.iter().collect();
    tifs.iter()
        .flat_map(|&tif| channels.iter().map(move |&channel| (tif, channel)))
        .map(|(tif, channel)| Shape {
            kind: OrderKindTag::Limit,
            tif,
            channel,
            post_only: true,
            reduce_only: false,
        })
        .find(|shape| shape.refusal(o).is_none())
}

/// The model prices taken around `grid`'s boundaries, each above zero, in ascending order.
pub(crate) fn model_prices(grid: &PriceGrid) -> BTreeSet<Decimal> {
    let finest = grid.finest();
    let mut anchors: BTreeSet<i64> = (1..=TOP_POWER).map(|p| 10_i64.pow(p)).collect();
    if let PriceGrid::Banded(banded) = grid {
        let starts = banded
            .bands()
            .filter_map(|(from, _)| (from / finest).ceil().to_i64());
        anchors.extend(starts);
    }
    let mut prices = BTreeSet::new();
    for anchor in anchors {
        // In half finest ticks: one tick and half a tick either side of the boundary.
        for offset in -2..=2 {
            let halves = anchor.saturating_mul(2).saturating_add(offset);
            if halves > 0 {
                prices.insert(Decimal::from(halves) * finest / Decimal::TWO);
            }
        }
    }
    prices
}

/// What quantizing `model` as a `side` order gave on `grid`: the order price, `None` for a bid
/// with no valid price at or below it, or why the price breaks the grid or maker safety.
pub(crate) fn judge(
    grid: &PriceGrid,
    side: Side,
    model: Decimal,
    got: Result<Ticks, QuantizeError>,
) -> Result<Option<Ticks>, String> {
    let name = side_name(side);
    let below_grid = grid
        .lowest_valid()
        .is_some_and(|lowest| model < Decimal::from(lowest.0) * grid.finest());
    let px = match got {
        Ok(px) => px,
        Err(QuantizeError::NoValidPrice) if side == Side::Buy && below_grid => return Ok(None),
        Err(e) => return Err(format!("the {name} at {model} is not quantized: {e}")),
    };
    let price = Decimal::from(px.0) * grid.finest();
    if !grid.valid_at(px) {
        return Err(format!(
            "the {name} at {model} quantizes to {price}, off the grid"
        ));
    }
    let safe = match side {
        Side::Buy => price <= model,
        Side::Sell => price >= model,
    };
    if !safe {
        return Err(format!(
            "the {name} at {model} quantizes to {price}, past the model's price: not maker-safe"
        ));
    }
    Ok(Some(px))
}

/// Places every quantized price in `quoted` on `spec` as a post-only order of `shape`, each by
/// a codec built fresh, and returns how many were sent; a refusal, a send without its request
/// and two prices sent alike are breaches.
fn send(
    h: &Harness<'_>,
    spec: &InstrumentSpec,
    shape: Shape,
    ids: &Ids,
    quoted: &[(Side, Ticks)],
    breaches: &mut Vec<Breach>,
) -> Result<usize, Failure> {
    let symbol = spec.venue_symbol.as_wire();
    let qty = Lots::new(spec.min_size.get().max(1)).expect("a positive count");
    let mut seen: HashMap<Effects, (Side, Ticks)> = HashMap::new();
    let mut sent = 0;
    for &(side, px) in quoted {
        let cmd = VenueCommand::Place(NewOrder {
            cid: ids.cids[0],
            inst: spec.id,
            side,
            qty,
            kind: OrderKind::Limit { px },
            tif: shape.tif,
            channel: shape.channel,
            post_only: true,
            reduce_only: false,
            reducing: false,
        });
        let label = format!(
            "a post-only {} on {symbol} at tick {}",
            side_name(side),
            px.0
        );
        let encoded = h.encode(h.exec_codec()?.as_mut(), &cmd, RPC);
        let what = match encoded.result {
            Err(why) => Some(format!(
                "refused as NotSent({why:?}) by a freshly built codec, though the price is \
                 valid"
            )),
            Ok(_) if !encoded.fx.carry_request(RPC, cmd.traffic_class()) => Some(format!(
                "encoded, but its effects do not carry request {}",
                RPC.0
            )),
            Ok(_) => seen.insert(encoded.fx, (side, px)).map(|(other, at)| {
                format!(
                    "encoded exactly as a post-only {} at tick {}: the codec does not send \
                         the price it is given",
                    side_name(other),
                    at.0
                )
            }),
        };
        match what {
            Some(what) => breaches.push(Breach::new(&label, what)),
            None => sent += 1,
        }
    }
    Ok(sent)
}

/// A side as an order on it is called.
fn side_name(side: Side) -> &'static str {
    match side {
        Side::Buy => "bid",
        Side::Sell => "ask",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixed(tick: Decimal) -> PriceGrid {
        PriceGrid::fixed(tick).unwrap()
    }

    #[test]
    fn model_prices_straddle_each_power_of_ten_and_band_start() {
        let half = Decimal::new(5, 1);
        let prices = model_prices(&fixed(half));
        // Around 10 ticks of 0.5: 4.5, 4.75, 5, 5.25 and 5.5.
        for (m, scale) in [(45, 1), (475, 2), (5, 0), (525, 2), (55, 1)] {
            let p = Decimal::new(m, scale);
            assert!(prices.contains(&p), "{p} missing");
        }
        assert_eq!(prices.len(), 5 * TOP_POWER as usize);
        let bands = [(Decimal::ZERO, half), (Decimal::new(1000, 1), Decimal::ONE)];
        let banded = model_prices(&PriceGrid::banded(&bands).unwrap());
        // The first band starts at zero: only the prices above zero around it, 0.25 and 0.5.
        assert!(banded.contains(&Decimal::new(25, 2)));
        assert!(banded.contains(&Decimal::new(5, 1)));
        assert!(banded.iter().all(|p| *p > Decimal::ZERO));
        // The second starts at 100.0, 200 ticks of 0.5: 99.5, 99.75, 100.25 and 100.5.
        for p in [995, 9975, 10025, 1005].map(|m| Decimal::new(m, if m > 9000 { 2 } else { 1 })) {
            assert!(banded.contains(&p), "{p} missing");
        }
    }

    #[test]
    fn judge_refuses_an_off_grid_or_aggressive_price_and_a_quantize_error() {
        let grid = fixed(Decimal::ONE);
        let ten = Decimal::TEN;
        assert_eq!(
            judge(&grid, Side::Buy, ten, Ok(Ticks(10))),
            Ok(Some(Ticks(10)))
        );
        assert_eq!(
            judge(&grid, Side::Buy, ten, Ok(Ticks(11))).unwrap_err(),
            "the bid at 10 quantizes to 11, past the model's price: not maker-safe"
        );
        assert_eq!(
            judge(&grid, Side::Sell, ten, Ok(Ticks(9))).unwrap_err(),
            "the ask at 10 quantizes to 9, past the model's price: not maker-safe"
        );
        // From 100 the tick is 5, so tick 103 is off the grid.
        let bands = [
            (Decimal::ONE, Decimal::ONE),
            (Decimal::ONE_HUNDRED, Decimal::new(5, 0)),
        ];
        let banded = PriceGrid::banded(&bands).unwrap();
        assert_eq!(
            judge(&banded, Side::Sell, Decimal::new(102, 0), Ok(Ticks(103))).unwrap_err(),
            "the ask at 102 quantizes to 103, off the grid"
        );
        assert_eq!(
            judge(&grid, Side::Buy, ten, Err(QuantizeError::NoValidPrice)).unwrap_err(),
            "the bid at 10 is not quantized: no valid price lies on the maker-safe side"
        );
    }

    #[test]
    fn a_bid_below_a_banded_grid_has_no_price_and_an_ask_there_must_have_one() {
        let bands = [(Decimal::TEN, Decimal::ONE)];
        let grid = PriceGrid::banded(&bands).unwrap();
        let five = Decimal::new(5, 0);
        let none = Err(QuantizeError::NoValidPrice);
        assert_eq!(judge(&grid, Side::Buy, five, none), Ok(None));
        assert!(judge(&grid, Side::Sell, five, none).is_err());
        // At the band start a bid has a price, so having none is a breach.
        assert!(judge(&grid, Side::Buy, Decimal::TEN, none).is_err());
    }

    #[test]
    fn post_only_limit_skips_a_time_in_force_that_conflicts_and_needs_a_limit_order() {
        let mut o = crate::toy::caps().exec.unwrap().order;
        o.tifs = fbc_core::TagSet::of(&[TifTag::Ioc, TifTag::Gtc]);
        assert_eq!(post_only_limit(&o).map(|s| s.tif), Some(TifTag::Gtc));
        o.post_only = false;
        assert_eq!(post_only_limit(&o), None);
        o.post_only = true;
        o.kinds = fbc_core::TagSet::of(&[OrderKindTag::Market]);
        assert_eq!(post_only_limit(&o), None);
    }
}
