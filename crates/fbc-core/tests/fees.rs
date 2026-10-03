//! Decision 0004, design §4.2: a `Fee` is the cost to us (positive means we paid, negative is a
//! rebate) whatever sign the venue reports in, it comes only from `DecodeScope::fee` applying
//! the venue's declared sign, and fee rates belong to accounts in a `FeeBook`.

use fbc_core::{
    AccountKey, AssetSym, Bps, Channel, ClientIdFormat, Fee, FeeBook, FeeEntry, FeeError, FeeKey,
    FeeLookup, FeeRate, FeeSource, InstrumentId, Liquidity, Money, Namespace, VenueFeeSign, WallNs,
    dispatch,
};

fn usdc() -> AssetSym {
    AssetSym::new("USDC").unwrap()
}

/// What the scope of a venue declaring `sign` makes of a raw fee amount.
fn decode(sign: VenueFeeSign, raw_nanos: i128) -> Result<Fee, FeeError> {
    dispatch(&ClientIdFormat::Uuid, Namespace::new(1), sign, |scope| {
        scope.fee(raw_nanos, usdc())
    })
}

const BOTH_SIGNS: [VenueFeeSign; 2] =
    [VenueFeeSign::PositiveIsCost, VenueFeeSign::PositiveIsRebate];

/// The same economic fact as each convention reports it: (declared sign, raw amount).
fn rebate_of(nanos: i128) -> [(VenueFeeSign, i128); 2] {
    [
        (VenueFeeSign::PositiveIsCost, -nanos),
        (VenueFeeSign::PositiveIsRebate, nanos),
    ]
}

fn paid_of(nanos: i128) -> [(VenueFeeSign, i128); 2] {
    [
        (VenueFeeSign::PositiveIsCost, nanos),
        (VenueFeeSign::PositiveIsRebate, -nanos),
    ]
}

#[test]
fn a_raw_rebate_is_a_negative_fee_under_either_declared_sign() {
    // A maker rebate of 0.0125 USDC, reported by a venue of each convention.
    for (sign, raw) in rebate_of(12_500_000) {
        let fee = decode(sign, raw).unwrap();
        assert_eq!(
            fee.cost(),
            Money::new(-12_500_000, usdc()),
            "{sign:?} raw {raw}"
        );
        assert!(
            fee.cost().nanos < 0,
            "{sign:?}: a rebate must cost us less than nothing"
        );
    }
}

#[test]
fn a_raw_paid_fee_is_a_positive_fee_under_either_declared_sign() {
    // A taker fee of 0.042 USDC, reported by a venue of each convention.
    for (sign, raw) in paid_of(42_000_000) {
        let fee = decode(sign, raw).unwrap();
        assert_eq!(
            fee.cost(),
            Money::new(42_000_000, usdc()),
            "{sign:?} raw {raw}"
        );
        assert!(fee.cost().nanos > 0, "{sign:?}: a paid fee must be a cost");
    }
}

#[test]
fn both_conventions_agree_on_every_amount_including_zero_and_the_extremes() {
    for nanos in [0, 1, 7, 1_000_000_000, i128::MAX, -i128::MAX] {
        let as_rebate = rebate_of(nanos).map(|(sign, raw)| decode(sign, raw).unwrap());
        assert_eq!(as_rebate[0], as_rebate[1], "rebate of {nanos}");
        let as_paid = paid_of(nanos).map(|(sign, raw)| decode(sign, raw).unwrap());
        assert_eq!(as_paid[0], as_paid[1], "paid {nanos}");
        assert_eq!(as_paid[0].cost().nanos, nanos);
    }
}

#[test]
fn pnl_is_the_negation_of_cost() {
    for sign in BOTH_SIGNS {
        for raw in [0, 1, -1, 12_500_000, -42_000_000, i128::MAX, -i128::MAX] {
            let fee = decode(sign, raw).unwrap();
            let (cost, pnl) = (fee.cost(), fee.pnl());
            assert_eq!(pnl.nanos, -cost.nanos, "{sign:?} raw {raw}");
            assert_eq!(pnl.asset, cost.asset);
            assert_eq!(pnl.asset, usdc());
        }
    }
    // A rebate is a gain: positive P&L.
    let rebate = decode(VenueFeeSign::PositiveIsRebate, 12_500_000).unwrap();
    assert_eq!(rebate.pnl(), Money::new(12_500_000, usdc()));
}

#[test]
fn the_one_raw_amount_without_a_negation_is_refused_not_wrapped() {
    // i128::MIN has no negation, so neither convention can hold it as an exact cost and P&L
    // pair; it is refused under both rather than wrapped or saturated (decision 0004).
    for sign in BOTH_SIGNS {
        assert_eq!(
            decode(sign, i128::MIN),
            Err(FeeError::OutOfRange),
            "{sign:?}"
        );
    }
    assert_eq!(
        FeeError::OutOfRange.to_string(),
        "fee amount out of range: i128::MIN nanos has no negation"
    );
    assert!(std::error::Error::source(&FeeError::OutOfRange).is_none());
}

// FeeBook: rates per (account, instrument, channel, liquidity).

const T0: WallNs = WallNs(1_759_363_200_000_000_000);

fn key(account: u16, instrument: u32, channel: Channel, liquidity: Liquidity) -> FeeKey {
    FeeKey {
        account: AccountKey::new(account),
        instrument: InstrumentId::new(instrument),
        channel,
        liquidity,
    }
}

fn entry(bps: f64, source: FeeSource) -> FeeEntry {
    FeeEntry {
        rate: FeeRate(Bps(bps)),
        source,
        as_of: T0,
    }
}

/// Sixteen keys that differ from each other in exactly the dimensions the book is keyed by,
/// each with its own rate.
fn every_key() -> Vec<(FeeKey, f64)> {
    let mut keys = Vec::new();
    let mut bps = 0.0;
    for account in [1, 2] {
        for instrument in [10, 11] {
            for channel in [Channel::Public, Channel::Rpi] {
                for liquidity in [Liquidity::Maker, Liquidity::Taker] {
                    bps += 0.25;
                    keys.push((key(account, instrument, channel, liquidity), bps));
                }
            }
        }
    }
    keys
}

#[test]
fn the_book_returns_the_rate_for_exactly_the_key_asked() {
    let mut book = FeeBook::new();
    assert!(book.is_empty());
    for (k, bps) in every_key() {
        assert!(book.insert(k, entry(bps, FeeSource::VenueQuery)).is_none());
    }
    assert_eq!(book.len(), 16);
    for (k, bps) in every_key() {
        assert_eq!(book.rate(&k, T0), Some(FeeRate(Bps(bps))), "{k:?}");
        match book.lookup(&k, T0) {
            FeeLookup::Current(found) => {
                assert_eq!(found.rate, FeeRate(Bps(bps)));
                assert_eq!(found.source, FeeSource::VenueQuery);
                assert_eq!(found.as_of, T0);
            }
            other => panic!("{k:?}: expected a current rate, got {other:?}"),
        }
    }
    // A key the book does not hold, in any one dimension, is missing; nothing falls back.
    for missing in [
        key(3, 10, Channel::Public, Liquidity::Maker),
        key(1, 12, Channel::Public, Liquidity::Maker),
    ] {
        assert_eq!(book.lookup(&missing, T0), FeeLookup::Missing);
        assert_eq!(book.rate(&missing, T0), None);
    }
}

#[test]
fn inserting_a_key_again_replaces_its_entry() {
    let mut book = FeeBook::default();
    let k = key(1, 10, Channel::Public, Liquidity::Maker);
    book.insert(k, entry(-0.5, FeeSource::PublicPrior));
    let old = book.insert(k, entry(-0.25, FeeSource::ObservedFills { n: 40 }));
    assert_eq!(old, Some(entry(-0.5, FeeSource::PublicPrior)));
    assert_eq!(book.rate(&k, T0), Some(FeeRate(Bps(-0.25))));
    assert_eq!(book.len(), 1);
}

#[test]
fn an_expired_configured_tier_is_reported_as_expired_and_its_rate_not_trusted() {
    let mut book = FeeBook::new();
    let epoch_end = WallNs(T0.0 + 14 * 86_400 * 1_000_000_000);
    let k = key(1, 10, Channel::Public, Liquidity::Maker);
    let tier = entry(
        -0.3,
        FeeSource::ConfiguredTier {
            epoch_end: Some(epoch_end),
        },
    );
    book.insert(k, tier);

    // Before the epoch ends the configured rate is current.
    let before = WallNs(epoch_end.0 - 1);
    assert_eq!(book.lookup(&k, before), FeeLookup::Current(&tier));
    assert_eq!(book.rate(&k, before), Some(FeeRate(Bps(-0.3))));
    assert!(!tier.expired_at(before));

    // From the epoch end on it is expired: still shown, never returned as a rate.
    for now in [epoch_end, WallNs(epoch_end.0 + 1), WallNs(i64::MAX)] {
        assert_eq!(book.lookup(&k, now), FeeLookup::Expired(&tier), "{now:?}");
        assert_eq!(book.rate(&k, now), None, "{now:?}");
        assert!(tier.expired_at(now));
    }
}

#[test]
fn only_a_configured_tier_with_an_epoch_end_expires() {
    let far = WallNs(i64::MAX);
    for source in [
        FeeSource::VenueQuery,
        FeeSource::ObservedFills { n: 1 },
        FeeSource::ConfiguredTier { epoch_end: None },
        FeeSource::ConfigOverride,
        FeeSource::PublicPrior,
    ] {
        let e = entry(0.2, source);
        assert!(!e.expired_at(far), "{source:?}");
        let mut book = FeeBook::new();
        let k = key(2, 11, Channel::Rpi, Liquidity::Taker);
        book.insert(k, e);
        assert_eq!(book.lookup(&k, far), FeeLookup::Current(&e), "{source:?}");
    }
}

#[test]
fn the_book_lists_one_accounts_rates_and_no_other() {
    let mut book = FeeBook::new();
    for (k, bps) in every_key() {
        book.insert(k, entry(bps, FeeSource::ConfigOverride));
    }
    let account_two: Vec<_> = book.account(AccountKey::new(2)).collect();
    assert_eq!(account_two.len(), 8);
    assert!(
        account_two
            .iter()
            .all(|(k, _)| k.account == AccountKey::new(2))
    );
    assert_eq!(book.account(AccountKey::new(9)).count(), 0);
}

#[test]
fn instrument_ids_channels_and_liquidity_are_plain_values() {
    assert_eq!(InstrumentId::new(77).get(), 77);
    assert!(InstrumentId::new(1) < InstrumentId::new(2));
    assert_ne!(Channel::Public, Channel::Rpi);
    assert_ne!(Liquidity::Maker, Liquidity::Taker);
}
