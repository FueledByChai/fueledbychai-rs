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
//! be, and not sent. Nor is a bid that quantizes, validly and maker-safe, to a price of zero
//! (below one tick on a banded grid whose first band starts at zero): zero is no order price,
//! and no size reaches a `min_notional` there.
//!
//! Then, where the caps allow a post-only limit order, each price quantized is placed as a
//! post-only order on its side by a codec built fresh from the factory: it must be sent,
//! carrying its request, and no two of them may encode alike. A codec that rounds a price it
//! is given onto a coarser grid sends two quantized prices as one, and a bid it rounds up or
//! an ask it rounds down is no longer maker-safe. A breach names the instrument, the side and
//! the price, never the bytes.
//!
//! Each order is sized inside its instrument's limits (FBC-7y5e), so a codec that refuses an
//! order below `min_notional` or above `max_order_size` is not taken for one that refuses the
//! price. The prices quantized around one boundary (from model prices half a finest tick apart)
//! go at one size, so that two of them encoded alike are one price sent twice: the least size
//! inside the limits at the lowest of them ([`least_size`]), the instrument's `min_size` (one
//! lot at least) raised until the notional ([`InstrumentSpec::notional`]) reaches
//! `min_notional`, never past `max_order_size`. The notional rising with the price, that size
//! reaches the minimum at every higher price there; a price at which its notional is too large
//! to hold takes its own least size. Where some price quantized takes no size inside the limits
//! (`max_order_size` below `min_size`, or no size within it reaching `min_notional` there), the
//! setup is refused for that instrument, naming the spec field and the prices, and nothing is
//! sent on it.
//!
//! Never skipped: every venue's grids are quantized. A venue whose caps declare no order entry,
//! or no post-only limit order, sends nothing, and says so in what it probed.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use fbc_core::{
    Channel, ClientOrderId, Effects, InstrumentSpec, Lots, NewOrder, OrderCaps, OrderKind,
    OrderKindTag, PriceGrid, QuantizeError, Side, Ticks, TifTag, VenueCommand,
};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use super::harness::{Harness, RPC, Shape, fits, inverted, least_size, within};
use super::{Breach, Failure, Subject, Verdict};

const CHECK: &str = "price_grid";

/// The largest power of ten, in finest ticks, a boundary is taken at.
const TOP_POWER: u32 = 9;

/// Runs `price_grid` against `subject`.
pub fn price_grid(subject: &Subject<'_>) -> Result<Verdict, Failure> {
    let h = Harness::new(CHECK, subject)?;
    let post_only = h.caps.exec.as_ref().and_then(|e| post_only_limit(&e.order));
    // It only places, so it mints our one client id and reads no venue id (FBC-6rfe).
    let cid = h.cids(1)?[0];
    let mut breaches = Vec::new();
    let mut probed = Vec::new();
    for spec in h.specs().iter() {
        let symbol = spec.venue_symbol.as_wire();
        let capability = format!("InstrumentSpec.price_grid of {symbol}");
        // Each order price quantized, once, in the order first quantized, with the boundary
        // it was first quantized around.
        let mut quoted: Vec<Quote> = Vec::new();
        let around = boundaries(&spec.price_grid);
        for (boundary, models) in around.iter().enumerate() {
            for &model in models {
                for side in [Side::Buy, Side::Sell] {
                    let got = spec.quantize(side, model.to_f64().unwrap_or(f64::NAN));
                    match judge(&spec.price_grid, side, model, got) {
                        Ok(Some(px)) if !quoted.iter().any(|q| (q.side, q.px) == (side, px)) => {
                            quoted.push(Quote { boundary, side, px });
                        }
                        Ok(_) => {}
                        Err(what) => breaches.push(Breach::new(&capability, what)),
                    }
                }
            }
        }
        let sent = match post_only.map(|shape| (shape, orders(spec, &quoted))) {
            Some((shape, Ok(orders))) => {
                let sent = send(&h, spec, shape, cid, &orders, &mut breaches)?;
                format!("{sent} post-only orders sent")
            }
            Some((_, Err(refused))) => {
                breaches.push(refused);
                "none sent: no order fits the instrument's limits".to_owned()
            }
            None => "no post-only limit order the caps allow to send".to_owned(),
        };
        probed.push(format!(
            "{symbol}: {} model prices quantized to {} order prices, {sent}",
            around.iter().map(Vec::len).sum::<usize>(),
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

/// A price quantized: its side and price, and the run of model prices ([`boundaries`]) it was
/// first quantized from.
struct Quote {
    boundary: usize,
    side: Side,
    px: Ticks,
}

/// The model prices taken around `grid`'s boundaries ([`model_prices`]), in ascending order, in
/// runs of prices half a finest tick apart: those around one boundary, or around boundaries
/// close enough to share a run.
pub(crate) fn boundaries(grid: &PriceGrid) -> Vec<Vec<Decimal>> {
    let half = grid.finest() / Decimal::TWO;
    let mut runs: Vec<Vec<Decimal>> = Vec::new();
    for model in model_prices(grid) {
        match runs.last_mut() {
            Some(run) if run.last().is_some_and(|&last| model - last <= half) => run.push(model),
            _ => runs.push(vec![model]),
        }
    }
    runs
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
/// with no valid price above zero at or below it (none at all below a banded grid's first band,
/// or zero itself, valid where the first band starts there), or why the price breaks the grid or
/// maker safety.
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
    // A bid floored to zero, valid and maker-safe, is judged but never sent: zero is no order
    // price, and its notional reaches no minimum (an ask, at or above a model price above zero,
    // never quantizes there safely).
    Ok((px.0 > 0).then_some(px))
}

/// The post-only order each price in `quoted` is sent as, in order, sized inside `spec`'s
/// limits: around each boundary one size, the least inside them at the lowest price quantized
/// there ([`least_size`]), which reaches `min_notional` at every higher price there too, and a
/// price that size does not fit (its notional too large to hold) at its own least size. Where
/// some price takes no size inside the limits, the breach refusing the setup for `spec`, naming
/// the spec field. A size reaches `min_notional` as [`least_size`] means it, with a notional
/// `spec` can hold: a minimum that only a notional too large to hold would pass is reached at no
/// price, and named as such.
fn orders(spec: &InstrumentSpec, quoted: &[Quote]) -> Result<Vec<(Side, Ticks, Lots)>, Breach> {
    let symbol = spec.venue_symbol.as_wire();
    if let Some((field, what)) = inverted(spec) {
        return Err(Breach::new(&format!("{field} of {symbol}"), what));
    }
    let mut lowest: BTreeMap<usize, Ticks> = BTreeMap::new();
    for q in quoted {
        let low = lowest.entry(q.boundary).or_insert(q.px);
        *low = (*low).min(q.px);
    }
    let shared: BTreeMap<usize, Option<Lots>> = lowest
        .into_iter()
        .map(|(boundary, px)| (boundary, least_size(spec, px)))
        .collect();
    let mut unfit = BTreeSet::new();
    let mut orders = Vec::with_capacity(quoted.len());
    for q in quoted {
        let size = shared[&q.boundary]
            .filter(|&size| fits(spec, q.px, size))
            .or_else(|| least_size(spec, q.px));
        match size {
            Some(size) => orders.push((q.side, q.px, size)),
            None => {
                unfit.insert(q.px.0);
            }
        }
    }
    if unfit.is_empty() {
        return Ok(orders);
    }
    let (prices, them) = match unfit.len() {
        1 => ("a price", "it"),
        _ => ("prices", "them"),
    };
    let what = format!(
        "no order of {symbol}{} reaches its min_notional at {}, {prices} its grid's boundaries \
         quantize to: no order fits to probe {them} with",
        within(spec),
        listed(&unfit)
    );
    Err(Breach::new(
        &format!("InstrumentSpec.min_notional of {symbol}"),
        what,
    ))
}

/// `ticks` named in a sentence: "tick 9", "ticks 9 and 10", "ticks 9, 10 and 11".
fn listed(ticks: &BTreeSet<i64>) -> String {
    let named: Vec<String> = ticks.iter().map(i64::to_string).collect();
    match named.as_slice() {
        [] => "no tick".to_owned(),
        [one] => format!("tick {one}"),
        [rest @ .., last] => format!("ticks {} and {last}", rest.join(", ")),
    }
}

/// Places each order in `orders`, a side, price and size, on `spec` as a post-only order of
/// `shape` under our client id `cid`, each by a codec built fresh, and returns how many were
/// sent; a refusal, a send without its request and two prices sent alike are breaches.
fn send(
    h: &Harness<'_>,
    spec: &InstrumentSpec,
    shape: Shape,
    cid: ClientOrderId,
    orders: &[(Side, Ticks, Lots)],
    breaches: &mut Vec<Breach>,
) -> Result<usize, Failure> {
    let symbol = spec.venue_symbol.as_wire();
    let mut seen: HashMap<Effects, (Side, Ticks)> = HashMap::new();
    let mut sent = 0;
    for &(side, px, qty) in orders {
        let cmd = VenueCommand::Place(NewOrder {
            cid,
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
    fn boundaries_split_the_model_prices_into_a_run_around_each_boundary() {
        let runs = boundaries(&fixed(Decimal::new(5, 1)));
        assert_eq!(runs.len(), TOP_POWER as usize);
        assert!(runs.iter().all(|run| run.len() == 5));
        // A band starting at 50.5, tick 101 of the finest 0.5, shares the run around tick 100:
        // ticks 99 to 102 by halves. The first band's start, tick 1, has a run of its own.
        let bands = [
            (Decimal::new(5, 1), Decimal::new(5, 1)),
            (Decimal::new(505, 1), Decimal::ONE),
        ];
        let runs = boundaries(&PriceGrid::banded(&bands).unwrap());
        let lens: Vec<usize> = runs.iter().map(Vec::len).collect();
        assert_eq!(lens, [4, 5, 7, 5, 5, 5, 5, 5, 5, 5]);
        assert_eq!(runs[2][0], Decimal::new(495, 1));
        assert_eq!(runs[2][6], Decimal::new(51, 0));
    }

    #[test]
    fn ticks_are_listed_as_a_sentence_names_them() {
        let listed = |ticks: &[i64]| listed(&ticks.iter().copied().collect());
        assert_eq!(listed(&[]), "no tick");
        assert_eq!(listed(&[9]), "tick 9");
        assert_eq!(listed(&[10, 9]), "ticks 9 and 10");
        assert_eq!(listed(&[11, 9, 10]), "ticks 9, 10 and 11");
    }

    #[test]
    fn a_price_whose_notional_at_its_boundarys_size_is_too_large_to_hold_takes_its_own_least() {
        // A tick of 10^12 and a step of one: at tick 11 a notional overflows at fewer lots than
        // at tick 9.
        let mut spec = crate::toy::specs().get(crate::toy::INST_A).unwrap().clone();
        spec.price_grid = fixed(Decimal::new(1_000_000_000_000, 0));
        spec.size_step = fbc_core::SizeStep::new(Decimal::ONE).unwrap();
        let (low, high) = (Ticks(9), Ticks(11));
        let at = |px: Ticks, q: i64| spec.notional(px, Lots::new(q).unwrap());
        // The least size whose notional at tick 11 is too large to hold.
        let (mut lo, mut hi) = (1_i64, i64::MAX);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if at(high, mid).is_none() {
                hi = mid;
            } else {
                lo = mid + 1;
            }
        }
        let held = at(low, lo).expect("held at tick 9");
        assert!(at(high, lo).is_none());
        // The minimum is that size's notional at tick 9, so it is the least size there.
        spec.min_notional = Some(held);
        let quoted = [
            Quote {
                boundary: 0,
                side: Side::Buy,
                px: low,
            },
            Quote {
                boundary: 0,
                side: Side::Sell,
                px: high,
            },
        ];
        let orders = orders(&spec, &quoted).unwrap();
        assert_eq!(orders[0], (Side::Buy, low, Lots::new(lo).unwrap()));
        let (side, px, own) = orders[1];
        assert_eq!((side, px), (Side::Sell, high));
        assert!(own.get() < lo && fits(&spec, high, own), "{own:?}");
        assert_eq!(least_size(&spec, high), Some(own));
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
    fn a_bid_floored_to_zero_is_judged_but_has_no_order_price() {
        // A first band starting at zero: the bid at 0.25 floors to tick 0, on the grid and
        // maker-safe, but zero is no order price.
        let bands = [
            (Decimal::ZERO, Decimal::new(5, 1)),
            (Decimal::ONE_HUNDRED, Decimal::ONE),
        ];
        let grid = PriceGrid::banded(&bands).unwrap();
        let quarter = Decimal::new(25, 2);
        assert!(grid.valid_at(Ticks(0)));
        assert_eq!(judge(&grid, Side::Buy, quarter, Ok(Ticks(0))), Ok(None));
        // Still judged before it is dropped: a price above the model's is a breach.
        assert_eq!(
            judge(&grid, Side::Buy, quarter, Ok(Ticks(1))).unwrap_err(),
            "the bid at 0.25 quantizes to 0.5, past the model's price: not maker-safe"
        );
        // The ask there has a price above zero, and an ask at zero is not maker-safe.
        assert_eq!(
            judge(&grid, Side::Sell, quarter, Ok(Ticks(1))),
            Ok(Some(Ticks(1)))
        );
        assert!(judge(&grid, Side::Sell, quarter, Ok(Ticks(0))).is_err());
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
