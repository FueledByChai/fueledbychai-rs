//! Exact units for books, orders and money (decision 0004, design §4.1).
//!
//! Prices on an instrument's grid are [`Ticks`] (an index on the grid's finest step), sizes are
//! [`Lots`] (an index on the size step) and positions are [`SignedLots`]. Off-grid prices such
//! as mark, index and average entry are [`PxExact`] and are never rounded to ticks. Money is
//! integer nanos of an asset. `f64` appears only in [`Bps`], which belongs to models.

use core::fmt;
use core::hash::{Hash, Hasher};
use core::ops::{Add, Neg, Sub};
use core::str::FromStr;

use rust_decimal::Decimal;

/// A price as an index on the instrument's finest price step (`PriceGrid::finest`).
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Ticks(pub i64);

/// A size as a count of the instrument's size step; never negative.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct Lots(pub i64);

/// A position in size steps; positive is long.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct SignedLots(pub i64);

/// Basis points. A model quantity; never a price, size or fee amount.
#[derive(Copy, Clone, PartialEq, PartialOrd, Debug)]
pub struct Bps(pub f64);

impl Add for Ticks {
    type Output = Ticks;
    fn add(self, rhs: Ticks) -> Ticks {
        Ticks(self.0 + rhs.0)
    }
}

impl Sub for Ticks {
    type Output = Ticks;
    fn sub(self, rhs: Ticks) -> Ticks {
        Ticks(self.0 - rhs.0)
    }
}

impl Add for Lots {
    type Output = Lots;
    fn add(self, rhs: Lots) -> Lots {
        Lots(self.0 + rhs.0)
    }
}

impl Lots {
    /// `self - rhs`, or `None` when that would be negative.
    pub fn checked_sub(self, rhs: Lots) -> Option<Lots> {
        self.0
            .checked_sub(rhs.0)
            .filter(|lots| *lots >= 0)
            .map(Lots)
    }
}

impl SignedLots {
    /// The position change of a fill of `lots` on `side`.
    pub fn of(side: Side, lots: Lots) -> SignedLots {
        SignedLots(side.sign() * lots.0)
    }

    /// The size of the position, whichever its direction.
    pub fn abs_lots(self) -> Lots {
        Lots(self.0.abs())
    }
}

impl Add for SignedLots {
    type Output = SignedLots;
    fn add(self, rhs: SignedLots) -> SignedLots {
        SignedLots(self.0 + rhs.0)
    }
}

impl Sub for SignedLots {
    type Output = SignedLots;
    fn sub(self, rhs: SignedLots) -> SignedLots {
        SignedLots(self.0 - rhs.0)
    }
}

impl Neg for SignedLots {
    type Output = SignedLots;
    fn neg(self) -> SignedLots {
        SignedLots(-self.0)
    }
}

/// An exact decimal price off the tick grid: mark, index, average entry, VWAP.
/// Its value is `mantissa × 10^exp` (Paradex SBE sends exponent −8). It is never rounded to a
/// grid; `PriceGrid::ticks_exact` converts it only when it lies exactly on one.
///
/// Equality and hashing are by value, so `1.0` (10 × 10⁻¹) equals `1` (1 × 10⁰).
#[derive(Copy, Clone, Debug)]
pub struct PxExact {
    pub mantissa: i64,
    pub exp: i8,
}

impl PxExact {
    pub const fn new(mantissa: i64, exp: i8) -> PxExact {
        PxExact { mantissa, exp }
    }

    /// The same value with trailing zeros moved from the mantissa into the exponent.
    pub fn normalized(self) -> PxExact {
        let PxExact {
            mut mantissa,
            mut exp,
        } = self;
        if mantissa == 0 {
            return PxExact {
                mantissa: 0,
                exp: 0,
            };
        }
        while mantissa % 10 == 0 && exp < i8::MAX {
            mantissa /= 10;
            exp += 1;
        }
        PxExact { mantissa, exp }
    }

    /// The value as a [`Decimal`], or `None` when it does not fit one exactly.
    pub fn to_decimal(self) -> Option<Decimal> {
        let px = self.normalized();
        if px.exp <= 0 {
            Decimal::try_from_i128_with_scale(
                i128::from(px.mantissa),
                u32::from(px.exp.unsigned_abs()),
            )
            .ok()
        } else {
            let scale = 10i128.checked_pow(u32::from(px.exp.unsigned_abs()))?;
            Decimal::try_from_i128_with_scale(i128::from(px.mantissa).checked_mul(scale)?, 0).ok()
        }
    }

    /// The exact value of `decimal`, or `None` when its mantissa does not fit an `i64`.
    pub fn from_decimal(decimal: Decimal) -> Option<PxExact> {
        let decimal = decimal.normalize();
        let mantissa = i64::try_from(decimal.mantissa()).ok()?;
        let exp = i8::try_from(decimal.scale()).ok()?;
        Some(PxExact {
            mantissa,
            exp: -exp,
        })
    }
}

impl PartialEq for PxExact {
    fn eq(&self, other: &PxExact) -> bool {
        let (a, b) = (self.normalized(), other.normalized());
        a.mantissa == b.mantissa && a.exp == b.exp
    }
}

impl Eq for PxExact {}

impl Hash for PxExact {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let px = self.normalized();
        px.mantissa.hash(state);
        px.exp.hash(state);
    }
}

/// Why a string is not an exact price.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct PxParseError(String);

impl fmt::Display for PxParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "not an exact price: {:?}", self.0)
    }
}

impl std::error::Error for PxParseError {}

impl FromStr for PxExact {
    type Err = PxParseError;

    /// Parses a plain decimal (`-65432.123456789`) exactly; no exponent notation, no rounding.
    fn from_str(text: &str) -> Result<PxExact, PxParseError> {
        let error = || PxParseError(text.to_owned());
        let (negative, unsigned) = match text.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, text),
        };
        let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
        let digits_ok = |part: &str| part.bytes().all(|b| b.is_ascii_digit());
        if (whole.is_empty() && fraction.is_empty()) || !digits_ok(whole) || !digits_ok(fraction) {
            return Err(error());
        }
        let exp = i8::try_from(fraction.len()).map_err(|_| error())?;
        let mut mantissa: i64 = 0;
        for digit in whole.bytes().chain(fraction.bytes()) {
            mantissa = mantissa
                .checked_mul(10)
                .and_then(|m| m.checked_add(i64::from(digit - b'0')))
                .ok_or_else(error)?;
        }
        Ok(PxExact {
            mantissa: if negative { -mantissa } else { mantissa },
            exp: -exp,
        })
    }
}

impl fmt::Display for PxExact {
    /// The exact value as a plain decimal, with as many fraction digits as the exponent holds.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sign = if self.mantissa < 0 { "-" } else { "" };
        let digits = self.mantissa.unsigned_abs().to_string();
        if self.mantissa == 0 && self.exp >= 0 {
            return write!(f, "0");
        }
        if self.exp >= 0 {
            let zeros = "0".repeat(usize::from(self.exp.unsigned_abs()));
            return write!(f, "{sign}{digits}{zeros}");
        }
        let places = usize::from(self.exp.unsigned_abs());
        let padded = format!("{digits:0>width$}", width = places + 1);
        let (whole, fraction) = padded.split_at(padded.len() - places);
        write!(f, "{sign}{whole}.{fraction}")
    }
}

/// An asset symbol of at most eight ASCII bytes, such as `USDC`.
#[derive(Copy, Clone, Eq, PartialEq, Hash)]
pub struct AssetSym([u8; 8]);

impl AssetSym {
    /// The symbol, or `None` when it is empty, longer than eight bytes or not printable ASCII.
    pub fn new(symbol: &str) -> Option<AssetSym> {
        let bytes = symbol.as_bytes();
        if bytes.is_empty() || bytes.len() > 8 || !bytes.iter().all(|b| b.is_ascii_graphic()) {
            return None;
        }
        let mut sym = [0u8; 8];
        sym[..bytes.len()].copy_from_slice(bytes);
        Some(AssetSym(sym))
    }

    pub fn as_str(&self) -> &str {
        let len = self.0.iter().position(|b| *b == 0).unwrap_or(8);
        core::str::from_utf8(&self.0[..len]).unwrap_or_default()
    }
}

impl fmt::Debug for AssetSym {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AssetSym({})", self.as_str())
    }
}

/// An amount of an asset in integer nanos (10⁻⁹ of one unit).
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct Money {
    pub nanos: i128,
    pub asset: AssetSym,
}

impl Money {
    pub const fn new(nanos: i128, asset: AssetSym) -> Money {
        Money { nanos, asset }
    }

    /// The sum, or `None` when the assets differ or it overflows.
    pub fn checked_add(self, rhs: Money) -> Option<Money> {
        self.same_asset(rhs)?;
        Some(Money {
            nanos: self.nanos.checked_add(rhs.nanos)?,
            asset: self.asset,
        })
    }

    /// The difference, or `None` when the assets differ or it overflows.
    pub fn checked_sub(self, rhs: Money) -> Option<Money> {
        self.same_asset(rhs)?;
        Some(Money {
            nanos: self.nanos.checked_sub(rhs.nanos)?,
            asset: self.asset,
        })
    }

    fn same_asset(self, rhs: Money) -> Option<()> {
        (self.asset == rhs.asset).then_some(())
    }
}

/// The side of an order or fill.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    /// `+1` for a buy, `-1` for a sell: the sign of the position change.
    #[inline]
    pub const fn sign(self) -> i64 {
        match self {
            Side::Buy => 1,
            Side::Sell => -1,
        }
    }

    pub const fn opposite(self) -> Side {
        match self {
            Side::Buy => Side::Sell,
            Side::Sell => Side::Buy,
        }
    }

    /// The book side a resting order on this side joins.
    pub const fn book_side(self) -> BookSide {
        match self {
            Side::Buy => BookSide::Bid,
            Side::Sell => BookSide::Ask,
        }
    }
}

/// A side of the order book.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum BookSide {
    Bid,
    Ask,
}

/// Who took liquidity in a public trade, when the venue says.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum Aggressor {
    Buyer,
    Seller,
    Unknown,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;

    fn hash_of(px: PxExact) -> u64 {
        let mut hasher = DefaultHasher::new();
        px.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn px_exact_parses_and_prints_without_losing_a_digit() {
        let mark: PxExact = "65432.123456789".parse().unwrap();
        assert_eq!((mark.mantissa, mark.exp), (65_432_123_456_789, -9));
        assert_eq!(mark.to_string(), "65432.123456789");
        for text in ["-0.00000001", "0.5", "-12", "7.000", "0.000123"] {
            assert_eq!(text.parse::<PxExact>().unwrap().to_string(), text);
        }
        assert_eq!(PxExact::new(12, 3).to_string(), "12000");
        assert_eq!(PxExact::new(0, 3).to_string(), "0");
        assert_eq!(".5".parse::<PxExact>().unwrap(), PxExact::new(5, -1));
    }

    #[test]
    fn px_exact_rejects_what_it_cannot_hold_exactly() {
        for text in [
            "",
            "-",
            ".",
            "1e5",
            "1.2.3",
            "abc",
            "+1",
            "99999999999999999999",
        ] {
            let err = text.parse::<PxExact>().unwrap_err();
            assert!(err.to_string().contains("not an exact price"));
        }
    }

    #[test]
    fn px_exact_compares_and_hashes_by_value() {
        assert_eq!(PxExact::new(10, -1), PxExact::new(1, 0));
        assert_eq!(hash_of(PxExact::new(10, -1)), hash_of(PxExact::new(1, 0)));
        assert_eq!(PxExact::new(0, -8), PxExact::new(0, 3));
        assert_ne!(PxExact::new(1, -1), PxExact::new(1, 0));
        assert_eq!(
            PxExact::new(1_200, -8).normalized(),
            PxExact {
                mantissa: 12,
                exp: -6
            }
        );
    }

    #[test]
    fn px_exact_converts_to_and_from_decimal_exactly() {
        let mark = PxExact::new(-123_456_789, -8);
        let decimal = mark.to_decimal().unwrap();
        assert_eq!(decimal.to_string(), "-1.23456789");
        assert_eq!(PxExact::from_decimal(decimal), Some(mark));
        assert_eq!(PxExact::new(25, 2).to_decimal(), Some(Decimal::from(2_500)));
        assert_eq!(PxExact::new(1, 100).to_decimal(), None);
        assert_eq!(PxExact::from_decimal(Decimal::MAX), None);
    }

    #[test]
    fn tick_lot_and_position_arithmetic() {
        assert_eq!(Ticks(5) + Ticks(3) - Ticks(10), Ticks(-2));
        assert_eq!(Lots(2) + Lots(3), Lots(5));
        assert_eq!(Lots(5).checked_sub(Lots(2)), Some(Lots(3)));
        assert_eq!(Lots(2).checked_sub(Lots(5)), None);
        let long = SignedLots::of(Side::Buy, Lots(4));
        let short = SignedLots::of(Side::Sell, Lots(6));
        assert_eq!(long + short, SignedLots(-2));
        assert_eq!(long - short, SignedLots(10));
        assert_eq!(-long, SignedLots(-4));
        assert_eq!(short.abs_lots(), Lots(6));
        assert!(Bps(1.5) > Bps(1.0));
    }

    #[test]
    fn money_adds_only_within_one_asset() {
        let usdc = AssetSym::new("USDC").unwrap();
        let usdt = AssetSym::new("USDT").unwrap();
        let a = Money::new(1_500_000_000, usdc);
        assert_eq!(
            a.checked_add(Money::new(-500_000_000, usdc)),
            Some(Money::new(1_000_000_000, usdc))
        );
        assert_eq!(
            a.checked_sub(Money::new(2_000_000_000, usdc)),
            Some(Money::new(-500_000_000, usdc))
        );
        assert_eq!(a.checked_add(Money::new(1, usdt)), None);
        assert_eq!(a.checked_sub(Money::new(1, usdt)), None);
        assert_eq!(
            Money::new(i128::MAX, usdc).checked_add(Money::new(1, usdc)),
            None
        );
    }

    #[test]
    fn asset_symbols_are_short_printable_ascii() {
        assert_eq!(AssetSym::new("USDC").unwrap().as_str(), "USDC");
        assert_eq!(AssetSym::new("ABCDEFGH").unwrap().as_str(), "ABCDEFGH");
        assert_eq!(
            format!("{:?}", AssetSym::new("ETH").unwrap()),
            "AssetSym(ETH)"
        );
        assert_eq!(AssetSym::new(""), None);
        assert_eq!(AssetSym::new("ABCDEFGHI"), None);
        assert_eq!(AssetSym::new("US DC"), None);
    }

    #[test]
    fn sides_map_to_signs_and_book_sides() {
        assert_eq!(Side::Buy.sign(), 1);
        assert_eq!(Side::Sell.sign(), -1);
        assert_eq!(Side::Buy.opposite(), Side::Sell);
        assert_eq!(Side::Sell.opposite(), Side::Buy);
        assert_eq!(Side::Buy.book_side(), BookSide::Bid);
        assert_eq!(Side::Sell.book_side(), BookSide::Ask);
        assert_ne!(Aggressor::Buyer, Aggressor::Unknown);
        assert_ne!(Aggressor::Seller, Aggressor::Buyer);
    }
}
