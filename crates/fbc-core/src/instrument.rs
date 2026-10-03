//! What an instrument is, as data (decisions 0003 and 0004, design §4.4).
//!
//! An [`InstrumentSpec`] is the venue's truth about one instrument: its grid of valid prices,
//! its size step and limits, its currencies and its funding. Every field is mandatory and none
//! has a default: a spec missing a value does not compile, and nothing is guessed from a
//! symbol's prefix. Where the venue states nothing, the field says so explicitly (`None`,
//! [`FundingSpec::Unknown`]).
//!
//! [`InstrumentSpec::quantize`] is the only place a model's `f64` price becomes an order price.
//! It is side-aware and maker-safe: a bid floors and an ask ceils, onto the grid valid at the
//! resulting price ([`PriceGrid::floor_valid`], [`PriceGrid::ceil_valid`]), so rounding never
//! moves a quote toward the other side of the book. [`InstrumentSpec::floor_qty`] floors a
//! size onto the size step and refuses one below the minimum.
//!
//! # Reading an `f64`
//!
//! A model's `f64` is read as the shortest decimal that converts back to the same `f64` (what
//! Rust prints for it), not as its exact binary value: `0.3` means 0.3 and floors to 0.3 on a
//! 0.1 grid, where its binary value, 0.29999999999999998…, would floor to 0.2. From that
//! decimal on, the arithmetic is exact integer arithmetic.
//!
//! [`InstrumentSpec::version`] ties [`Ticks`] and [`Lots`] to the spec they index: a new tick or
//! size step is a new version, and the books resync and resting orders re-quantize (decision
//! 0004).

use core::fmt;
use core::fmt::Write as _;
use core::time::Duration;

use arrayvec::ArrayString;
use compact_str::CompactString;
use rust_decimal::{Decimal, RoundingStrategy};

use crate::fee::FeeSchedule;
use crate::grid::PriceGrid;
use crate::ids::{InstrumentId, UnderlyingId, VenueId, VenueSymbol};
use crate::time::WallNs;
use crate::units::{AssetSym, Bps, Lots, Money, Side, Ticks};

/// One instrument on one venue, every field stated by its adapter.
///
/// No `Default`: a missing field is a compile error, so no spec carries a guessed tick, lot or
/// funding interval.
#[derive(Clone, PartialEq, Debug)]
pub struct InstrumentSpec {
    /// The core's number for the instrument.
    pub id: InstrumentId,
    /// The venue that lists it.
    pub venue: VenueId,
    /// Its name on the venue's wire.
    pub venue_symbol: VenueSymbol,
    /// The venue's own key for it where orders or signatures need one (a contract id, an asset
    /// hash, an asset index), `None` where the symbol is enough.
    pub native_id: Option<VenueNativeId>,
    /// What it is a contract on.
    pub underlying: UnderlyingId,
    /// Perpetual, dated future or spot.
    pub kind: InstrumentKind,
    /// The prices the venue accepts for orders: the acceptance grid, venue truth.
    pub price_grid: PriceGrid,
    /// A coarser grid the venue publishes its book on, where it differs from the acceptance
    /// grid; `None` where the book uses the acceptance grid.
    pub quote_grid: Option<Decimal>,
    /// The size of one [`Lots`] unit.
    pub size_step: SizeStep,
    /// The smallest order size the venue accepts.
    pub min_size: Lots,
    /// The smallest order notional the venue accepts, `None` where it sets none.
    pub min_notional: Option<Money>,
    /// The largest single order, `None` where the venue sets none.
    pub max_order_size: Option<Lots>,
    /// The largest position, `None` where the venue sets none.
    pub position_limit: Option<Lots>,
    /// How far from the mark the venue accepts an order price, `None` where it sets no band.
    pub price_band: Option<Bps>,
    /// How many open orders the venue allows on the instrument, `None` where it sets no limit.
    pub max_open_orders: Option<u32>,
    /// Quote currency per unit of size per unit of price (1 for a linear perpetual).
    pub multiplier: Decimal,
    /// The currency prices are quoted in.
    pub quote_ccy: AssetSym,
    /// The currency the instrument settles in.
    pub settle_ccy: AssetSym,
    /// The funding schedule, stated or explicitly unknown; never a default interval.
    pub funding: FundingSpec,
    /// The venue's published fee schedule: a prior only, since account rates live in a
    /// [`FeeBook`](crate::FeeBook).
    pub public_fees: Option<FeeSchedule>,
    /// Whether the instrument is trading.
    pub status: TradingStatus,
    /// The spec's version: a new tick or size step bumps it, and books resync and resting
    /// orders re-quantize.
    pub version: u32,
    /// When the venue was asked for this spec.
    pub fetched_at: WallNs,
}

/// The venue's own key for an instrument.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum VenueNativeId {
    /// A number (a contract id, an asset index).
    Number(u64),
    /// A string (an asset hash).
    Text(CompactString),
}

/// What kind of contract an instrument is.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum InstrumentKind {
    /// A perpetual future.
    Perpetual,
    /// A future that expires at `expiry`.
    Future {
        /// When it expires.
        expiry: WallNs,
    },
    /// A spot pair.
    Spot,
}

/// The size of one [`Lots`] unit: a positive decimal.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct SizeStep(Decimal);

impl SizeStep {
    /// The size step `step`, or `None` when it is zero or negative.
    pub fn new(step: Decimal) -> Option<SizeStep> {
        (step > Decimal::ZERO).then(|| SizeStep(step.normalize()))
    }

    /// The step as a decimal.
    pub fn get(self) -> Decimal {
        self.0
    }
}

/// An instrument's funding schedule. There is no default: a venue whose schedule is not known
/// says [`Unknown`](FundingSpec::Unknown), never a guessed eight hours.
#[derive(Copy, Clone, PartialEq, Debug)]
pub enum FundingSpec {
    /// Funding every `interval`, capped at `cap` per interval where the venue caps it.
    Known {
        /// The time between funding payments.
        interval: Duration,
        /// The largest rate per interval, `None` where the venue sets no cap.
        cap: Option<Bps>,
    },
    /// The instrument pays no funding (spot, dated futures).
    NotApplicable,
    /// The instrument pays funding on a schedule nobody has confirmed.
    Unknown,
}

/// Whether an instrument is trading, as its venue reports.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum TradingStatus {
    /// Orders of every kind are accepted.
    Trading,
    /// Only post-only orders are accepted.
    PostOnly,
    /// Only reducing orders are accepted.
    ReduceOnly,
    /// Only cancels are accepted.
    CancelOnly,
    /// Trading is halted.
    Halted,
    /// The instrument is delisted.
    Delisted,
}

/// Why a model price could not become an order price.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum QuantizeError {
    /// The price is NaN or infinite.
    NotFinite,
    /// The price, in finest ticks, does not fit an `i64`.
    OutOfRange,
    /// No valid price lies on the maker-safe side: a bid below a banded grid's first band.
    NoValidPrice,
}

impl fmt::Display for QuantizeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            QuantizeError::NotFinite => "the price is not a finite number",
            QuantizeError::OutOfRange => "the price does not fit the instrument's tick index",
            QuantizeError::NoValidPrice => "no valid price lies on the maker-safe side",
        })
    }
}

impl std::error::Error for QuantizeError {}

/// Why a model size could not become an order size.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum QtyError {
    /// The size is NaN or infinite.
    NotFinite,
    /// The size is negative.
    Negative,
    /// The size, in size steps, does not fit an `i64`.
    OutOfRange,
    /// The size floors to `lots`, below the venue's minimum `min_size`, or to zero lots.
    BelowMinSize {
        /// The size floored onto the size step.
        lots: Lots,
        /// The smallest size the venue accepts.
        min_size: Lots,
    },
}

impl fmt::Display for QtyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QtyError::NotFinite => f.write_str("the size is not a finite number"),
            QtyError::Negative => f.write_str("the size is negative"),
            QtyError::OutOfRange => f.write_str("the size does not fit the instrument's lot index"),
            QtyError::BelowMinSize { lots, min_size } => write!(
                f,
                "the size floors to {} lots, below the minimum of {}",
                lots.get(),
                min_size.get()
            ),
        }
    }
}

impl std::error::Error for QtyError {}

impl InstrumentSpec {
    /// The order price for a model price `px` on `side`: a bid floors and an ask ceils, onto
    /// the grid valid at the resulting price, so the order is never more aggressive than `px`.
    pub fn quantize(&self, side: Side, px: f64) -> Result<Ticks, QuantizeError> {
        if !px.is_finite() {
            return Err(QuantizeError::NotFinite);
        }
        let finest = self.price_grid.finest();
        let ticks = match side {
            Side::Buy => index_of(px, finest, Round::Down)
                .map(|ticks| self.price_grid.floor_valid(Ticks(ticks))),
            Side::Sell => index_of(px, finest, Round::Up)
                .map(|ticks| self.price_grid.ceil_valid(Ticks(ticks))),
        };
        ticks
            .ok_or(QuantizeError::OutOfRange)?
            .ok_or(QuantizeError::NoValidPrice)
    }

    /// The order size for a model size `q`, floored onto the size step, or refused when it is
    /// below [`min_size`](InstrumentSpec::min_size) or is zero lots (never an order, even where
    /// the declared minimum is zero).
    pub fn floor_qty(&self, q: f64) -> Result<Lots, QtyError> {
        if !q.is_finite() {
            return Err(QtyError::NotFinite);
        }
        if q < 0.0 {
            return Err(QtyError::Negative);
        }
        let lots = index_of(q, self.size_step.get(), Round::Down)
            .and_then(Lots::new)
            .ok_or(QtyError::OutOfRange)?;
        if lots < self.min_size || lots == Lots::ZERO {
            return Err(QtyError::BelowMinSize {
                lots,
                min_size: self.min_size,
            });
        }
        Ok(lots)
    }

    /// The notional of `q` at `px` in the quote currency, multiplier included, to the nearest
    /// nano (half to even). `None` when it does not fit a [`Money`].
    pub fn notional(&self, px: Ticks, q: Lots) -> Option<Money> {
        let price = Decimal::from(px.0).checked_mul(self.price_grid.finest())?;
        let size = Decimal::from(q.get()).checked_mul(self.size_step.get())?;
        let nanos = price
            .checked_mul(size)?
            .checked_mul(self.multiplier)?
            .checked_mul(Decimal::from(1_000_000_000_u32))?
            .round_dp_with_strategy(0, RoundingStrategy::MidpointNearestEven);
        Some(Money::new(i128::try_from(nanos).ok()?, self.quote_ccy))
    }

    /// The grid step at `ref_px` in basis points of `ref_px`; `None` at a zero price.
    pub fn tick_bps(&self, ref_px: Ticks) -> Option<Bps> {
        if ref_px.0 == 0 {
            return None;
        }
        let step = self.price_grid.step_at(ref_px).0 as f64;
        Some(Bps(step / ref_px.0.unsigned_abs() as f64 * 10_000.0))
    }
}

/// Which way [`index_of`] rounds.
#[derive(Copy, Clone)]
enum Round {
    Down,
    Up,
}

/// `x / unit`, rounded `round` to a whole number, where `x` is read as the shortest decimal
/// that converts back to it; `None` when the result does not fit an `i64`. `x` is finite and
/// `unit` positive.
fn index_of(x: f64, unit: Decimal, round: Round) -> Option<i64> {
    // `{:e}` prints the shortest round-trip digits: "6.543215e4", "-3e-1".
    let mut text = ArrayString::<32>::new();
    write!(text, "{x:e}").ok()?;
    let (mantissa, exp) = text.split_once('e')?;
    let exp: i32 = exp.parse().ok()?;
    let (negative, mantissa) = match mantissa.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, mantissa),
    };
    let (whole, frac) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let mut digits: i128 = 0;
    for byte in whole.bytes().chain(frac.bytes()) {
        digits = digits * 10 + i128::from(byte - b'0');
    }
    // x = ±digits × 10^(exp − frac digits) and unit = mantissa × 10^(−scale), so
    // x / unit = ±digits × 10^power / mantissa.
    let unit = unit.normalize();
    let power = exp - frac.len() as i32 + unit.scale() as i32;
    let (mut num, den) = if power >= 0 {
        // An overflow here means a quotient above i128::MAX / mantissa, which is beyond i64
        // for every unit whose normalized mantissa is below 10^19 (every real tick and step).
        (
            digits.checked_mul(10_i128.checked_pow(power.unsigned_abs())?)?,
            unit.mantissa(),
        )
    } else {
        match 10_i128
            .checked_pow(power.unsigned_abs())
            .and_then(|scale| unit.mantissa().checked_mul(scale))
        {
            Some(den) => (digits, den),
            // The divisor is beyond i128 while the digits (not zero: zero prints as "0e0",
            // which takes the branch above) are below 10^17, so 0 < |x / unit| < 1.
            None => {
                return Some(match (round, negative) {
                    (Round::Down, false) | (Round::Up, true) => 0,
                    (Round::Down, true) => -1,
                    (Round::Up, false) => 1,
                });
            }
        }
    };
    if negative {
        num = -num;
    }
    let quotient = match round {
        Round::Down => num.div_euclid(den),
        Round::Up => -(-num).div_euclid(den),
    };
    i64::try_from(quotient).ok()
}
