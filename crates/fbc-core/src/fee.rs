//! Fees with one sign convention, and fee rates per account (decision 0004, design §4.2).
//!
//! A [`Fee`] is the cost to us: **positive means we paid, negative means a rebate**. Venues
//! report fees in one of two conventions ([`VenueFeeSign`]); a fee exists only after
//! [`DecodeScope::fee`](crate::DecodeScope::fee) has applied the venue's declared sign, so no
//! code past the decoder ever sees a raw venue amount. `Fee` has no public constructor, no
//! `abs`, no negation and no conversion from `f64` (`tests/ui_fee/` proves each), so a sign
//! cannot be thrown away or flipped by hand. [`Fee::cost`] is the amount as a cost and
//! [`Fee::pnl`] the same amount as P&L.
//!
//! Converting to a legacy database column or UI field (some of which count a rebate as
//! positive) is the consumer's job at its edge, not this crate's.
//!
//! Fee rates belong to accounts, not instruments: a [`FeeBook`] holds one [`FeeEntry`] per
//! [`FeeKey`] (account, instrument, channel, liquidity), with where the rate came from
//! ([`FeeSource`]). A rebate tier configured by hand ends with its epoch; from then on the book
//! reports it as [`FeeLookup::Expired`] and no longer returns its rate.

use core::fmt;
use std::collections::BTreeMap;

use crate::ids::{AccountKey, InstrumentId};
use crate::time::WallNs;
use crate::units::{AssetSym, Bps, Channel, Liquidity, Money};

/// The cost of a fill's fee to us: positive when we paid, negative for a rebate.
///
/// Built only by [`DecodeScope::fee`](crate::DecodeScope::fee). Its magnitude is at most
/// `i128::MAX` nanos, so [`cost`](Fee::cost) and [`pnl`](Fee::pnl) are both always exact.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct Fee(Money);

/// The sign convention a venue reports fees in, declared in its capabilities.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum VenueFeeSign {
    /// A positive amount is a fee we paid; a rebate arrives negative.
    PositiveIsCost,
    /// A positive amount is a rebate we received; a fee we paid arrives negative.
    PositiveIsRebate,
}

/// Why a raw fee amount could not become a [`Fee`].
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum FeeError {
    /// The raw amount was `i128::MIN` nanos, the one value whose negation does not exist, so
    /// it cannot be held as an exact cost and P&L pair under either convention.
    OutOfRange,
}

impl fmt::Display for FeeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FeeError::OutOfRange => {
                f.write_str("fee amount out of range: i128::MIN nanos has no negation")
            }
        }
    }
}

impl std::error::Error for FeeError {}

impl Fee {
    /// The fee a venue declaring `sign` means by `raw_nanos` of `asset`. Crate-private:
    /// [`DecodeScope::fee`](crate::DecodeScope::fee) is its only caller (`tests/no_back_door.rs`).
    pub(crate) fn from_declared(
        raw_nanos: i128,
        asset: AssetSym,
        sign: VenueFeeSign,
    ) -> Result<Fee, FeeError> {
        if raw_nanos == i128::MIN {
            return Err(FeeError::OutOfRange);
        }
        let nanos = match sign {
            VenueFeeSign::PositiveIsCost => raw_nanos,
            VenueFeeSign::PositiveIsRebate => -raw_nanos,
        };
        Ok(Fee(Money::new(nanos, asset)))
    }

    /// The fee as a cost: positive when we paid, negative for a rebate.
    pub fn cost(&self) -> Money {
        self.0
    }

    /// The fee's effect on P&L: the negation of [`cost`](Fee::cost), so a rebate is a gain.
    pub fn pnl(&self) -> Money {
        // Exact: from_declared refuses i128::MIN, so the magnitude is at most i128::MAX.
        Money::new(-self.0.nanos, self.0.asset)
    }
}

/// A fee rate in basis points of notional, with the same sign as [`Fee`]: positive is a cost,
/// negative a rebate.
#[derive(Copy, Clone, PartialEq, PartialOrd, Debug)]
pub struct FeeRate(pub Bps);

/// A venue's published rates for an instrument, per order channel: a prior, the last resort a
/// [`FeeBook`] falls back to ([`FeeSource::PublicPrior`]), never an account's rate. Rates are
/// kept per channel, as [`FeeKey`] keys them, so an RPI rate is never borrowed from the public
/// book's.
#[derive(Copy, Clone, PartialEq, Debug)]
pub struct FeeSchedule {
    /// The public book's rates.
    pub public: PublishedRates,
    /// The RPI channel's rates, `None` where the venue publishes none.
    pub rpi: Option<PublishedRates>,
}

/// One channel's published maker and taker rates.
#[derive(Copy, Clone, PartialEq, Debug)]
pub struct PublishedRates {
    pub maker: FeeRate,
    pub taker: FeeRate,
}

impl FeeSchedule {
    /// The published rate for `channel` and `liquidity`, `None` where the venue publishes none
    /// for that channel.
    pub fn rate(&self, channel: Channel, liquidity: Liquidity) -> Option<FeeRate> {
        let rates = match channel {
            Channel::Public => self.public,
            Channel::Rpi => self.rpi?,
        };
        Some(match liquidity {
            Liquidity::Maker => rates.maker,
            Liquidity::Taker => rates.taker,
        })
    }
}

/// What a fee rate applies to: one account's orders on one instrument, through one order
/// channel, taking or making liquidity.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct FeeKey {
    pub account: AccountKey,
    pub instrument: InstrumentId,
    pub channel: Channel,
    pub liquidity: Liquidity,
}

/// Where a fee rate came from.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum FeeSource {
    /// The venue's authenticated fee endpoint.
    VenueQuery,
    /// The rate realized over the last `n` fills.
    ObservedFills { n: u32 },
    /// A program rebate that fill fees do not show, configured by hand. It ends at
    /// `epoch_end` when one is given (rebate tiers roll per epoch); from then on it is expired.
    ConfiguredTier { epoch_end: Option<WallNs> },
    /// A rate the consumer's configuration forces.
    ConfigOverride,
    /// The venue's published schedule for the instrument: the last resort.
    PublicPrior,
}

/// A fee rate, where it came from and when it was learned.
#[derive(Copy, Clone, PartialEq, Debug)]
pub struct FeeEntry {
    pub rate: FeeRate,
    pub source: FeeSource,
    pub as_of: WallNs,
}

impl FeeEntry {
    /// Whether the rate is past its end at `now`: only a [`FeeSource::ConfiguredTier`] with an
    /// epoch end expires, at that instant and after it.
    pub fn expired_at(&self, now: WallNs) -> bool {
        match self.source {
            FeeSource::ConfiguredTier {
                epoch_end: Some(end),
            } => now >= end,
            _ => false,
        }
    }
}

/// What the [`FeeBook`] holds for a key at an instant.
#[derive(Copy, Clone, PartialEq, Debug)]
pub enum FeeLookup<'a> {
    /// A rate that can be used now.
    Current(&'a FeeEntry),
    /// A configured tier whose epoch has ended: shown so the expiry can be reported, never to
    /// be trusted as the rate.
    Expired(&'a FeeEntry),
    /// No rate for this exact key; the book does not fall back to another key.
    Missing,
}

/// Fee rates per account, instrument, channel and liquidity.
#[derive(Clone, Debug, Default)]
pub struct FeeBook {
    entries: BTreeMap<FeeKey, FeeEntry>,
}

impl FeeBook {
    /// An empty book.
    pub fn new() -> FeeBook {
        FeeBook::default()
    }

    /// Sets the entry for `key`, returning the one it replaces.
    pub fn insert(&mut self, key: FeeKey, entry: FeeEntry) -> Option<FeeEntry> {
        self.entries.insert(key, entry)
    }

    /// The entry for exactly `key` at `now`, reporting an expired configured tier as
    /// [`FeeLookup::Expired`].
    pub fn lookup(&self, key: &FeeKey, now: WallNs) -> FeeLookup<'_> {
        match self.entries.get(key) {
            None => FeeLookup::Missing,
            Some(entry) if entry.expired_at(now) => FeeLookup::Expired(entry),
            Some(entry) => FeeLookup::Current(entry),
        }
    }

    /// The rate for exactly `key` at `now`, or `None` when the book has none or it has expired.
    pub fn rate(&self, key: &FeeKey, now: WallNs) -> Option<FeeRate> {
        match self.lookup(key, now) {
            FeeLookup::Current(entry) => Some(entry.rate),
            FeeLookup::Expired(_) | FeeLookup::Missing => None,
        }
    }

    /// Every entry of one account, in key order.
    pub fn account(&self, account: AccountKey) -> impl Iterator<Item = (&FeeKey, &FeeEntry)> {
        self.entries
            .iter()
            .filter(move |(key, _)| key.account == account)
    }

    /// How many keys the book holds.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the book holds no key.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cid::ClientIdFormat;
    use crate::ids::Namespace;
    use crate::scope::dispatch;

    // Fees come only from the decode scope, in unit tests as well (decision 0004).
    fn fee(sign: VenueFeeSign, raw: i128) -> Fee {
        let usdc = AssetSym::new("USDC").unwrap();
        dispatch(&ClientIdFormat::Uuid, Namespace::new(1), sign, |scope| {
            scope.fee(raw, usdc)
        })
        .unwrap()
    }

    #[test]
    fn a_rebate_venue_flips_the_sign_and_a_cost_venue_keeps_it() {
        assert_eq!(fee(VenueFeeSign::PositiveIsCost, 5).cost().nanos, 5);
        assert_eq!(fee(VenueFeeSign::PositiveIsRebate, 5).cost().nanos, -5);
        assert_eq!(fee(VenueFeeSign::PositiveIsRebate, -5).pnl().nanos, -5);
    }
}
