//! Price grids: which prices a venue accepts, and the tick unit books and orders index by
//! (decision 0004, design §4.1).
//!
//! [`Ticks`] always index the grid's [`finest`](PriceGrid::finest) step, so book keys are exact
//! integers on every kind of grid. Where the valid set depends on the price (significant-figure
//! and banded grids), [`valid_at`](PriceGrid::valid_at) says whether a tick index is an
//! acceptable order price and [`step_at`](PriceGrid::step_at) gives the grid's spacing there in
//! finest units. Off-grid values stay [`PxExact`]; [`ticks_exact`](PriceGrid::ticks_exact)
//! converts one only when it lies exactly on the finest grid and never rounds.

use core::fmt;

use rust_decimal::Decimal;

use crate::units::{PxExact, Ticks};

/// The largest number of decimals or significant figures a grid may use, so that every step in
/// finest units fits an `i64`.
pub const MAX_GRID_DIGITS: u8 = 18;

/// The set of valid prices of an instrument.
#[derive(Clone, Eq, PartialEq, Debug)]
pub enum PriceGrid {
    /// One tick size at every price.
    Fixed(FixedGrid),
    /// At most `sig` significant figures and at most `max_decimals` decimals, optionally with
    /// every integer price valid (Hyperliquid).
    SigFigs(SigFigsGrid),
    /// A tick size per price band, `[(price_from, tick)]` ascending (IBKR market rules).
    Banded(BandedGrid),
}

/// Why a grid description is not usable.
#[derive(Clone, Eq, PartialEq, Debug)]
pub enum GridError {
    /// A tick size is zero or negative.
    NonPositiveTick,
    /// A banded grid has no bands.
    NoBands,
    /// Band starts are not strictly ascending.
    BandsNotAscending,
    /// Significant figures outside `1..=MAX_GRID_DIGITS`.
    SigFigsOutOfRange,
    /// Decimals above `MAX_GRID_DIGITS`.
    DecimalsOutOfRange,
    /// A band start or step cannot be expressed in finest units as an `i64`.
    NotRepresentable,
}

impl fmt::Display for GridError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reason = match self {
            GridError::NonPositiveTick => "a tick size is not positive",
            GridError::NoBands => "a banded grid needs at least one band",
            GridError::BandsNotAscending => "band starts are not strictly ascending",
            GridError::SigFigsOutOfRange => "significant figures are outside 1..=18",
            GridError::DecimalsOutOfRange => "decimals are above 18",
            GridError::NotRepresentable => "a band does not fit the finest grid as an i64",
        };
        write!(f, "invalid price grid: {reason}")
    }
}

impl std::error::Error for GridError {}

/// One tick size at every price.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct FixedGrid {
    tick: Decimal,
}

impl FixedGrid {
    pub fn tick(&self) -> Decimal {
        self.tick
    }
}

/// At most `sig` significant figures and `max_decimals` decimals.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct SigFigsGrid {
    sig: u8,
    max_decimals: u8,
    integers_always_valid: bool,
}

impl SigFigsGrid {
    pub fn sig(&self) -> u8 {
        self.sig
    }

    pub fn max_decimals(&self) -> u8 {
        self.max_decimals
    }

    pub fn integers_always_valid(&self) -> bool {
        self.integers_always_valid
    }
}

/// A tick size per price band.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct BandedGrid {
    finest: Decimal,
    bands: Vec<Band>,
}

#[derive(Clone, Eq, PartialEq, Debug)]
struct Band {
    from: Decimal,
    tick: Decimal,
    /// The first tick index at or above `from`.
    from_ticks: i64,
    /// `tick` in finest units.
    step: i64,
}

impl BandedGrid {
    /// The bands as given: `(price_from, tick)`, ascending.
    pub fn bands(&self) -> impl Iterator<Item = (Decimal, Decimal)> + '_ {
        self.bands.iter().map(|band| (band.from, band.tick))
    }
}

impl PriceGrid {
    /// A grid with one tick size.
    pub fn fixed(tick: Decimal) -> Result<PriceGrid, GridError> {
        if tick <= Decimal::ZERO {
            return Err(GridError::NonPositiveTick);
        }
        Ok(PriceGrid::Fixed(FixedGrid {
            tick: tick.normalize(),
        }))
    }

    /// A significant-figure grid: at most `sig` significant figures and `max_decimals`
    /// decimals; with `integers_always_valid`, every integer price is valid too.
    pub fn sig_figs(
        sig: u8,
        max_decimals: u8,
        integers_always_valid: bool,
    ) -> Result<PriceGrid, GridError> {
        if sig == 0 || sig > MAX_GRID_DIGITS {
            return Err(GridError::SigFigsOutOfRange);
        }
        if max_decimals > MAX_GRID_DIGITS {
            return Err(GridError::DecimalsOutOfRange);
        }
        Ok(PriceGrid::SigFigs(SigFigsGrid {
            sig,
            max_decimals,
            integers_always_valid,
        }))
    }

    /// A banded grid from `(price_from, tick)` pairs with strictly ascending starts. Prices
    /// below the first start are not on the grid. The finest step is the greatest common
    /// divisor of the ticks, so every band's prices are whole numbers of finest steps.
    pub fn banded(bands: &[(Decimal, Decimal)]) -> Result<PriceGrid, GridError> {
        if bands.is_empty() {
            return Err(GridError::NoBands);
        }
        if bands.iter().any(|(_, tick)| *tick <= Decimal::ZERO) {
            return Err(GridError::NonPositiveTick);
        }
        if bands.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
            return Err(GridError::BandsNotAscending);
        }
        let finest = bands
            .iter()
            .skip(1)
            .try_fold(bands[0].1, |acc, (_, tick)| decimal_gcd(acc, *tick))
            .ok_or(GridError::NotRepresentable)?;
        let bands = bands
            .iter()
            .map(|&(from, tick)| {
                let from_ticks = from.checked_div(finest).map(|q| q.ceil());
                let step = tick.checked_div(finest);
                match (from_ticks.and_then(to_i64), step.and_then(to_i64)) {
                    (Some(from_ticks), Some(step)) => Ok(Band {
                        from: from.normalize(),
                        tick: tick.normalize(),
                        from_ticks,
                        step,
                    }),
                    _ => Err(GridError::NotRepresentable),
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(PriceGrid::Banded(BandedGrid { finest, bands }))
    }

    /// The price of one [`Ticks`] unit: the finest step anywhere on the grid.
    pub fn finest(&self) -> Decimal {
        match self {
            PriceGrid::Fixed(grid) => grid.tick,
            PriceGrid::SigFigs(grid) => Decimal::new(1, u32::from(grid.max_decimals)),
            PriceGrid::Banded(grid) => grid.finest,
        }
    }

    /// Whether the price at tick index `px` is an acceptable order price.
    pub fn valid_at(&self, px: Ticks) -> bool {
        match self {
            PriceGrid::Banded(grid) if px.0 < grid.bands[0].from_ticks => false,
            _ => {
                px.0.unsigned_abs()
                    .is_multiple_of(self.step_at(px).0.unsigned_abs())
            }
        }
    }

    /// The spacing of valid prices at `px`, in finest units. A fixed grid steps by one tick
    /// everywhere; a banded grid steps by its band's tick (the first band's below the first
    /// start); a significant-figure grid steps by one unit in its last significant figure, or
    /// by one whole price unit where integers are always valid and that is finer.
    pub fn step_at(&self, px: Ticks) -> Ticks {
        match self {
            PriceGrid::Fixed(_) => Ticks(1),
            PriceGrid::SigFigs(grid) => {
                let digits = decimal_digits(px.0.unsigned_abs());
                let sig_step = if digits > u32::from(grid.sig) {
                    10i64.pow(digits - u32::from(grid.sig))
                } else {
                    1
                };
                if grid.integers_always_valid {
                    Ticks(sig_step.min(10i64.pow(u32::from(grid.max_decimals))))
                } else {
                    Ticks(sig_step)
                }
            }
            PriceGrid::Banded(grid) => {
                let after = grid.bands.partition_point(|band| band.from_ticks <= px.0);
                Ticks(grid.bands[after.saturating_sub(1)].step)
            }
        }
    }

    /// The greatest valid order price at or below `px`: where a bid quantizes to. `None` when
    /// there is none (below a banded grid's first band) or it does not fit an `i64`.
    ///
    /// The grid valid at the result is the one that counts, so on a banded grid a price just
    /// above a band start whose multiples miss that start falls through to the band below.
    pub fn floor_valid(&self, px: Ticks) -> Option<Ticks> {
        match self {
            PriceGrid::Fixed(_) => Some(px),
            // Steps are powers of ten that divide every decade boundary, so the multiple below
            // `px` of the step at `px` is valid even when it lands in another decade.
            PriceGrid::SigFigs(_) => floor_multiple(px.0, self.step_at(px).0).map(Ticks),
            PriceGrid::Banded(grid) => {
                // Bands starting at or below `px`; none means no valid price is that low.
                let mut band = grid
                    .bands
                    .partition_point(|band| band.from_ticks <= px.0)
                    .checked_sub(1)?;
                let mut top = px.0;
                loop {
                    let Band {
                        from_ticks, step, ..
                    } = grid.bands[band];
                    // A multiple that overflows lies below i64::MIN, so below this band too.
                    if let Some(candidate) = floor_multiple(top, step).filter(|&c| c >= from_ticks)
                    {
                        return Some(Ticks(candidate));
                    }
                    // Nothing valid in this band at or below `top`: the band below ends just
                    // under this band's start.
                    band = band.checked_sub(1)?;
                    top = from_ticks.checked_sub(1)?;
                }
            }
        }
    }

    /// The least valid order price at or above `px`: where an ask quantizes to. `None` only
    /// when it does not fit an `i64`.
    ///
    /// On a banded grid a price below the first band rises to the first band, and a multiple
    /// that would pass the next band's start moves into that band and takes its step.
    pub fn ceil_valid(&self, px: Ticks) -> Option<Ticks> {
        match self {
            PriceGrid::Fixed(_) => Some(px),
            PriceGrid::SigFigs(_) => ceil_multiple(px.0, self.step_at(px).0).map(Ticks),
            PriceGrid::Banded(grid) => {
                let mut band = grid
                    .bands
                    .partition_point(|band| band.from_ticks <= px.0)
                    .saturating_sub(1);
                let mut bottom = px.0;
                loop {
                    let Band {
                        from_ticks, step, ..
                    } = grid.bands[band];
                    // A multiple that overflows lies above i64::MAX, so past any next band's
                    // start: that band may still hold a valid price.
                    let candidate = ceil_multiple(bottom.max(from_ticks), step);
                    match grid.bands.get(band + 1) {
                        Some(next) if candidate.is_none_or(|c| c >= next.from_ticks) => {
                            band += 1;
                            bottom = next.from_ticks;
                        }
                        _ => return candidate.map(Ticks),
                    }
                }
            }
        }
    }

    /// The tick index of `px` when it lies exactly on the finest grid; `None` otherwise. This
    /// never rounds: an off-grid mark stays a [`PxExact`].
    pub fn ticks_exact(&self, px: PxExact) -> Option<Ticks> {
        let value = px.to_decimal()?;
        let finest = self.finest();
        let index = value.checked_div(finest)?;
        if !index.fract().is_zero() || index.checked_mul(finest)? != value {
            return None;
        }
        to_i64(index).map(Ticks)
    }

    /// The exact price of tick index `ticks`, or `None` when it does not fit a [`PxExact`].
    pub fn px_of(&self, ticks: Ticks) -> Option<PxExact> {
        PxExact::from_decimal(Decimal::from(ticks.0).checked_mul(self.finest())?)
    }
}

/// The greatest multiple of `step` (positive) at or below `x`, if it fits an `i64`.
fn floor_multiple(x: i64, step: i64) -> Option<i64> {
    let step = i128::from(step);
    i64::try_from(i128::from(x).div_euclid(step) * step).ok()
}

/// The least multiple of `step` (positive) at or above `x`, if it fits an `i64`.
fn ceil_multiple(x: i64, step: i64) -> Option<i64> {
    let step = i128::from(step);
    i64::try_from(-(-i128::from(x)).div_euclid(step) * step).ok()
}

/// A whole decimal as an `i64`; `None` when it has a fraction or does not fit.
fn to_i64(value: Decimal) -> Option<i64> {
    if !value.fract().is_zero() {
        return None;
    }
    i64::try_from(value).ok()
}

/// Number of decimal digits in `n`, counting zero as one digit.
fn decimal_digits(n: u64) -> u32 {
    n.checked_ilog10().map_or(1, |log| log + 1)
}

/// The greatest common divisor of two positive decimals.
fn decimal_gcd(a: Decimal, b: Decimal) -> Option<Decimal> {
    let scale = a.scale().max(b.scale());
    let (mut x, mut y) = (a, b);
    x.rescale(scale);
    y.rescale(scale);
    if x.scale() != scale || y.scale() != scale {
        return None;
    }
    let (mut m, mut n) = (x.mantissa().unsigned_abs(), y.mantissa().unsigned_abs());
    while n != 0 {
        (m, n) = (n, m % n);
    }
    Decimal::try_from_i128_with_scale(i128::try_from(m).ok()?, scale)
        .ok()
        .map(|d| d.normalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(text: &str) -> Decimal {
        text.parse().unwrap()
    }

    fn px(text: &str) -> PxExact {
        text.parse().unwrap()
    }

    #[test]
    fn fixed_grid_has_one_step_everywhere() {
        let grid = PriceGrid::fixed(dec("0.50")).unwrap();
        assert_eq!(grid.finest(), dec("0.5"));
        assert_eq!(grid.finest().to_string(), "0.5");
        for ticks in [-3, 0, 1, 130_865, i64::MAX] {
            assert!(grid.valid_at(Ticks(ticks)));
            assert_eq!(grid.step_at(Ticks(ticks)), Ticks(1));
        }
        let PriceGrid::Fixed(fixed) = &grid else {
            panic!("not fixed")
        };
        assert_eq!(fixed.tick(), dec("0.5"));
        assert_eq!(grid.ticks_exact(px("65432.5")), Some(Ticks(130_865)));
        assert_eq!(grid.px_of(Ticks(130_865)), Some(px("65432.5")));
    }

    #[test]
    fn sig_fig_grid_steps_by_the_last_significant_figure() {
        // Five significant figures, at most six decimals, no integer exception.
        let grid = PriceGrid::sig_figs(5, 6, false).unwrap();
        assert_eq!(grid.finest(), dec("0.000001"));
        // 0.001234 = 1234 ticks: four figures, every finest step valid.
        assert_eq!(grid.step_at(Ticks(1_234)), Ticks(1));
        assert!(grid.valid_at(Ticks(1_234)));
        // 1.23456 = 1_234_560 ticks: seven digits, so the step is 100 ticks (0.0001).
        assert_eq!(grid.step_at(Ticks(1_234_560)), Ticks(100));
        assert!(!grid.valid_at(Ticks(1_234_560)));
        assert!(grid.valid_at(Ticks(1_234_500)));
        // The step widens at each power of ten: 9.9999 steps by 100, 10.000 by 1000.
        assert_eq!(grid.step_at(Ticks(9_999_900)), Ticks(100));
        assert_eq!(grid.step_at(Ticks(10_000_000)), Ticks(1_000));
        // 123456 = 123_456_000_000 ticks has six figures and is not valid here.
        assert!(!grid.valid_at(Ticks(123_456_000_000)));
        assert!(grid.valid_at(Ticks(-1_234_500)));
        assert!(grid.valid_at(Ticks(0)));
        let PriceGrid::SigFigs(sig) = &grid else {
            panic!("not sig figs")
        };
        assert_eq!(
            (sig.sig(), sig.max_decimals(), sig.integers_always_valid()),
            (5, 6, false)
        );
    }

    #[test]
    fn sig_fig_grid_accepts_every_integer_when_told_to() {
        // Hyperliquid perp with five size decimals: five figures, one decimal, integers valid.
        let grid = PriceGrid::sig_figs(5, 1, true).unwrap();
        assert_eq!(grid.finest(), dec("0.1"));
        // 1234.5 = 12_345 ticks: five figures, step one tick.
        assert_eq!(grid.step_at(Ticks(12_345)), Ticks(1));
        assert!(grid.valid_at(Ticks(12_345)));
        // 65432.1 has six figures; the step is one whole unit (10 ticks), so 65432 is valid.
        assert_eq!(grid.step_at(Ticks(654_321)), Ticks(10));
        assert!(!grid.valid_at(Ticks(654_321)));
        assert!(grid.valid_at(Ticks(654_320)));
        // 123456 has six figures but is an integer: valid, where the plain rule would refuse.
        assert_eq!(grid.step_at(Ticks(1_234_560)), Ticks(10));
        assert!(grid.valid_at(Ticks(1_234_560)));
        assert!(!grid.valid_at(Ticks(1_234_565)));
        let plain = PriceGrid::sig_figs(5, 1, false).unwrap();
        assert_eq!(plain.step_at(Ticks(1_234_560)), Ticks(100));
        assert!(!plain.valid_at(Ticks(1_234_560)));
        assert!(plain.valid_at(Ticks(1_234_500)));
    }

    #[test]
    fn banded_grid_steps_by_its_band() {
        // IBKR-style: 0.0001 below 1, 0.01 from 1.
        let grid =
            PriceGrid::banded(&[(dec("0"), dec("0.0001")), (dec("1"), dec("0.01"))]).unwrap();
        assert_eq!(grid.finest(), dec("0.0001"));
        assert_eq!(grid.step_at(Ticks(5_000)), Ticks(1));
        assert_eq!(grid.step_at(Ticks(9_999)), Ticks(1));
        assert!(grid.valid_at(Ticks(9_999)));
        assert_eq!(grid.step_at(Ticks(10_000)), Ticks(100));
        assert!(grid.valid_at(Ticks(10_000)));
        assert!(!grid.valid_at(Ticks(10_050)));
        assert!(grid.valid_at(Ticks(10_100)));
        assert!(!grid.valid_at(Ticks(-1)));
        let PriceGrid::Banded(banded) = &grid else {
            panic!("not banded")
        };
        assert_eq!(
            banded.bands().collect::<Vec<_>>(),
            vec![(dec("0"), dec("0.0001")), (dec("1"), dec("0.01"))]
        );
    }

    #[test]
    fn banded_grid_finest_is_the_common_divisor_of_its_ticks() {
        // 0.25 from 1 and 0.1 from 10: neither tick divides the other; finest is 0.05.
        let grid = PriceGrid::banded(&[(dec("1"), dec("0.25")), (dec("10"), dec("0.1"))]).unwrap();
        assert_eq!(grid.finest(), dec("0.05"));
        // 2.50 = 50 ticks, in the first band (step 5): valid; 2.55 = 51 ticks: not.
        assert_eq!(grid.step_at(Ticks(50)), Ticks(5));
        assert!(grid.valid_at(Ticks(50)));
        assert!(!grid.valid_at(Ticks(51)));
        // 10.05 = 201 ticks, second band (step 2): not valid; 10.10 = 202: valid.
        assert_eq!(grid.step_at(Ticks(201)), Ticks(2));
        assert!(!grid.valid_at(Ticks(201)));
        assert!(grid.valid_at(Ticks(202)));
        // Below the first band (0.50 = 10 ticks) nothing is valid; the step is the first band's.
        assert!(!grid.valid_at(Ticks(10)));
        assert_eq!(grid.step_at(Ticks(10)), Ticks(5));
    }

    /// The greatest valid price in `lowest..=px`, found by scanning every finest step.
    fn scan_floor(grid: &PriceGrid, px: i64, lowest: i64) -> Option<Ticks> {
        (lowest..=px).rev().map(Ticks).find(|&t| grid.valid_at(t))
    }

    /// The least valid price in `px..=highest`, found by scanning every finest step.
    fn scan_ceil(grid: &PriceGrid, px: i64, highest: i64) -> Option<Ticks> {
        (px..=highest).map(Ticks).find(|&t| grid.valid_at(t))
    }

    #[test]
    fn floor_and_ceil_valid_agree_with_a_scan_on_every_kind_of_grid() {
        let grids = [
            PriceGrid::fixed(dec("0.5")).unwrap(),
            // Two figures: the step is 1, 10 and 100 ticks in successive decades.
            PriceGrid::sig_figs(2, 3, false).unwrap(),
            // Integers (10 ticks) always valid, finer than the figure step from 1000 ticks.
            PriceGrid::sig_figs(2, 1, true).unwrap(),
            // A first band whose start (1 tick) is not a multiple of its step (2 ticks).
            PriceGrid::banded(&[(dec("0.1"), dec("0.2")), (dec("1"), dec("0.1"))]).unwrap(),
            // A second band whose start (11 ticks) is not a multiple of its step (2 ticks).
            PriceGrid::banded(&[(dec("0"), dec("0.3")), (dec("1.05"), dec("0.2"))]).unwrap(),
            // Below 13 ticks the band under it holds 12, above the band's own multiple 10.
            PriceGrid::banded(&[(dec("0"), dec("0.3")), (dec("1.3"), dec("1"))]).unwrap(),
            PriceGrid::banded(&[(dec("1"), dec("0.25")), (dec("10"), dec("0.1"))]).unwrap(),
        ];
        for grid in &grids {
            for px in -2_500..=2_500 {
                let floor = grid.floor_valid(Ticks(px));
                let ceil = grid.ceil_valid(Ticks(px));
                assert_eq!(floor, scan_floor(grid, px, -4_000), "{grid:?} floor {px}");
                assert_eq!(ceil, scan_ceil(grid, px, 4_000), "{grid:?} ceil {px}");
                if grid.valid_at(Ticks(px)) {
                    assert_eq!((floor, ceil), (Some(Ticks(px)), Some(Ticks(px))));
                }
            }
        }
    }

    #[test]
    fn floor_and_ceil_valid_refuse_what_does_not_fit() {
        let sig = PriceGrid::sig_figs(2, 3, false).unwrap();
        assert_eq!(sig.floor_valid(Ticks(i64::MIN)), None);
        assert_eq!(sig.ceil_valid(Ticks(i64::MAX)), None);
        let fixed = PriceGrid::fixed(dec("0.5")).unwrap();
        assert_eq!(fixed.floor_valid(Ticks(i64::MIN)), Some(Ticks(i64::MIN)));
        assert_eq!(fixed.ceil_valid(Ticks(i64::MAX)), Some(Ticks(i64::MAX)));
        // The last band steps by 3 finest ticks, and i64::MAX is not a multiple of 3.
        let banded = PriceGrid::banded(&[(dec("0"), dec("0.2")), (dec("1"), dec("0.3"))]).unwrap();
        assert_eq!(banded.ceil_valid(Ticks(i64::MAX)), None);
        assert_eq!(banded.floor_valid(Ticks(-1)), None);
        assert_eq!(banded.ceil_valid(Ticks(-1)), Some(Ticks(0)));
    }

    #[test]
    fn a_multiple_past_the_end_of_i64_moves_on_to_the_next_band() {
        // Steps of 10 up to i64::MAX - 5, then steps of 1. From i64::MAX - 6 the first band's
        // next multiple overflows, but the second band's start is valid and above it.
        let start = i64::MAX - 5;
        let grid =
            PriceGrid::banded(&[(dec("0"), dec("10")), (Decimal::from(start), dec("1"))]).unwrap();
        assert_eq!(grid.ceil_valid(Ticks(start - 1)), Some(Ticks(start)));
        // Below zero the mirror case: steps of 1 from i64::MIN, then steps of 10 from
        // i64::MIN + 5. From i64::MIN + 6 the second band's multiple below overflows, but the
        // first band holds i64::MIN + 4.
        let low = i64::MIN + 5;
        let grid = PriceGrid::banded(&[
            (Decimal::from(i64::MIN), dec("1")),
            (Decimal::from(low), dec("10")),
        ])
        .unwrap();
        assert_eq!(grid.floor_valid(Ticks(low + 1)), Some(Ticks(low - 1)));
    }

    #[test]
    fn px_exact_keeps_an_off_grid_mark_exact() {
        let grid = PriceGrid::fixed(dec("0.5")).unwrap();
        let mark = px("65432.123456789");
        // The grid refuses to index it rather than rounding it to 65432.0 or 65432.5.
        assert_eq!(grid.ticks_exact(mark), None);
        assert_eq!((mark.mantissa, mark.exp), (65_432_123_456_789, -9));
        assert_eq!(mark.to_string(), "65432.123456789");
        assert_eq!(mark.to_decimal(), Some(dec("65432.123456789")));
        // Paradex SBE form: mantissa with exponent -8, off a 0.1 grid, kept to the last digit.
        let sbe_mark = PxExact::new(6_543_212_345_678, -8);
        let tenth = PriceGrid::fixed(dec("0.1")).unwrap();
        assert_eq!(tenth.ticks_exact(sbe_mark), None);
        assert_eq!(sbe_mark.to_string(), "65432.12345678");
        // An on-grid price converts, and back, exactly.
        assert_eq!(tenth.ticks_exact(px("65432.1")), Some(Ticks(654_321)));
        assert_eq!(tenth.px_of(Ticks(654_321)), Some(px("65432.1")));
        let banded = PriceGrid::banded(&[(dec("0"), dec("0.25"))]).unwrap();
        assert_eq!(banded.ticks_exact(px("1.30")), None);
        assert_eq!(banded.ticks_exact(px("1.25")), Some(Ticks(5)));
    }

    #[test]
    fn conversions_refuse_values_that_do_not_fit() {
        let grid = PriceGrid::sig_figs(5, 18, false).unwrap();
        assert_eq!(grid.ticks_exact(px("100000000000")), None);
        assert_eq!(grid.ticks_exact(PxExact::new(1, 100)), None);
        assert_eq!(
            grid.px_of(Ticks(i64::MAX)),
            PxExact::from_decimal(dec("9.223372036854775807"))
        );
        let coarse = PriceGrid::fixed(dec("1000000000000")).unwrap();
        assert_eq!(coarse.px_of(Ticks(i64::MAX)), None);
    }

    #[test]
    fn grid_descriptions_are_validated() {
        assert_eq!(PriceGrid::fixed(dec("0")), Err(GridError::NonPositiveTick));
        assert_eq!(
            PriceGrid::fixed(dec("-0.1")),
            Err(GridError::NonPositiveTick)
        );
        assert_eq!(
            PriceGrid::sig_figs(0, 2, true),
            Err(GridError::SigFigsOutOfRange)
        );
        assert_eq!(
            PriceGrid::sig_figs(19, 2, true),
            Err(GridError::SigFigsOutOfRange)
        );
        assert_eq!(
            PriceGrid::sig_figs(5, 19, true),
            Err(GridError::DecimalsOutOfRange)
        );
        assert_eq!(PriceGrid::banded(&[]), Err(GridError::NoBands));
        assert_eq!(
            PriceGrid::banded(&[(dec("0"), dec("0.1")), (dec("1"), dec("0"))]),
            Err(GridError::NonPositiveTick)
        );
        assert_eq!(
            PriceGrid::banded(&[(dec("1"), dec("0.1")), (dec("1"), dec("0.2"))]),
            Err(GridError::BandsNotAscending)
        );
        assert_eq!(
            PriceGrid::banded(&[
                (dec("0"), dec("0.0000000000000000000000000001")),
                (dec("1"), dec("1"))
            ]),
            Err(GridError::NotRepresentable)
        );
        for error in [
            GridError::NonPositiveTick,
            GridError::NoBands,
            GridError::BandsNotAscending,
            GridError::SigFigsOutOfRange,
            GridError::DecimalsOutOfRange,
            GridError::NotRepresentable,
        ] {
            assert!(error.to_string().starts_with("invalid price grid: "));
        }
    }
}
