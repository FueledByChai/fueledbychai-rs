//! Mark-aware sizing (decisions 0096, 0113 and 0155 item 6): the caps in lots, the inventory
//! cap's price, and why a side is left out. Every value is a decimal or a lot count chosen here;
//! the decimal is fbc-core's re-export (0155 item 1). The first two tests moved from the samples'
//! (`testnet_trade`'s and `mainnet_smoke`'s), their assertions unchanged but for the path.

use std::str::FromStr;

use fbc_core::{Decimal, Lots, Side, SignedLots};
use fbc_live::sizing;
use fbc_live::sizing::{
    Cap, LeftOut, LotCaps, Market, NotPositive, SideQuote, UsdCaps, admit_minimum, admit_side,
    at_market, cap_lots, caps_in_lots, inventory_price,
};

fn d(s: &str) -> Decimal {
    Decimal::from_str(s).unwrap()
}

fn lots(n: i64) -> Lots {
    Lots::new(n).unwrap()
}

#[test]
fn fbc_cores_decimal_is_rust_decimals() {
    // The re-export is the one type, so a value fbc-core hands out is the consumer's decimal.
    let ours: rust_decimal::Decimal = d("1.5");
    assert_eq!(ours, rust_decimal::Decimal::new(15, 1));
}

#[test]
fn the_inventory_cap_is_at_the_highest_of_the_orders_price_and_the_touch() {
    let d = |v: &str| Decimal::from_str(v).unwrap();
    // A buy below a bid with no ask: the bid (Codex r4226246459).
    assert_eq!(
        sizing::inventory_price(d("104.5"), Some(d("110")), None),
        d("110")
    );
    // A buy on a full book: the ask.
    assert_eq!(
        sizing::inventory_price(d("104.5"), Some(d("110")), Some(d("110.1"))),
        d("110.1")
    );
    // A sell above the ask, with or without a bid: its own price.
    assert_eq!(
        sizing::inventory_price(d("115.6"), None, Some(d("110.1"))),
        d("115.6")
    );
    assert_eq!(
        sizing::inventory_price(d("115.6"), Some(d("110")), Some(d("110.1"))),
        d("115.6")
    );
}

#[test]
fn a_cap_in_lots_is_floored_and_one_past_a_lot_count_is_the_largest() {
    let d = |s: &str| Decimal::from_str(s).unwrap();
    let lots =
        |usd: Decimal, price: Decimal, step: Decimal| sizing::cap_lots(usd, price, step).get();
    // $5000 at 60140.1 on the 0.00001 step: 8313.9 lots, floored.
    assert_eq!(lots(d("5000"), d("60140.1"), d("0.00001")), 8_313);
    // Past what a lot count holds, or a decimal once divided: the largest lot count.
    assert_eq!(lots(d("1e20"), d("1"), d("1")), i64::MAX);
    assert_eq!(lots(Decimal::MAX, d("0.5"), d("0.00001")), i64::MAX);
    // A lot whose cost does not fit a decimal costs more than any cap: no lot, never the
    // largest count (Codex r4320317907 on PR #184).
    assert_eq!(lots(Decimal::MAX, Decimal::MAX, d("2")), 0);
    // A cap, a price or a step that is not positive: no lot.
    assert_eq!(lots(Decimal::ZERO, d("1"), d("1")), 0);
    assert_eq!(lots(d("-5"), d("1"), d("1")), 0);
    assert_eq!(lots(d("5"), Decimal::ZERO, d("1")), 0);
    assert_eq!(lots(d("5"), d("1"), d("-1")), 0);
}

/// The conversion `cap_lots` made before it moved (FBC-6r81): `ToPrimitive::to_i64` of the
/// floored quotient, where the library converts with `i64::try_from`.
fn cap_lots_as_before(usd: Decimal, price: Decimal, step: Decimal) -> i64 {
    use rust_decimal::prelude::ToPrimitive;
    let per_lot = match price.checked_mul(step) {
        Some(per) if per > Decimal::ZERO => per,
        _ => return 0,
    };
    if usd <= Decimal::ZERO {
        return 0;
    }
    let lots = usd
        .checked_div(per_lot)
        .and_then(|n| n.floor().to_i64())
        .unwrap_or(i64::MAX);
    Lots::new(lots).unwrap_or(Lots::ZERO).get()
}

#[test]
fn cap_lots_gives_what_it_gave_before_it_converted_with_try_from() {
    let max = Decimal::from(i64::MAX);
    let caps = [
        d("0"),
        d("-5"),
        d("0.000001"),
        d("1"),
        d("5000"),
        d("1e20"),
        max,
        max - d("0.5"),
        max + d("0.5"),
        max + Decimal::ONE,
        -max,
        Decimal::MAX,
        Decimal::MIN,
    ];
    let prices = [
        d("0"),
        d("-1"),
        d("0.5"),
        d("1"),
        d("60140.1"),
        Decimal::MAX,
    ];
    let steps = [d("-1"), d("0"), d("0.00001"), d("1"), d("2")];
    for usd in caps {
        for price in prices {
            for step in steps {
                assert_eq!(
                    cap_lots(usd, price, step).get(),
                    cap_lots_as_before(usd, price, step),
                    "{usd} at {price} on {step}"
                );
            }
        }
    }
    // A cap of exactly i64::MAX lots, and one just past it, at a price of one: the largest.
    assert_eq!(cap_lots(max, d("1"), d("1")).get(), i64::MAX);
    assert_eq!(cap_lots(max + Decimal::ONE, d("1"), d("1")).get(), i64::MAX);
}

#[test]
fn the_market_price_is_the_highest_of_the_price_the_touch_and_the_mark() {
    // No mark: the inventory price of the order's price and the touch.
    assert_eq!(at_market(d("104.5"), Some(d("110")), None, None), d("110"));
    // A mark above both: the mark.
    assert_eq!(
        at_market(d("104.5"), Some(d("110")), Some(d("110.1")), Some(d("112"))),
        d("112")
    );
    // A mark below the touch: the touch.
    assert_eq!(
        at_market(d("104.5"), Some(d("110")), Some(d("110.1")), Some(d("100"))),
        d("110.1")
    );
    // No touch: the higher of the price and the mark.
    assert_eq!(at_market(d("104.5"), None, None, Some(d("90"))), d("104.5"));
    assert_eq!(at_market(d("104.5"), None, None, Some(d("120"))), d("120"));
    assert_eq!(inventory_price(d("104.5"), None, None), d("104.5"));
}

#[test]
fn the_caps_are_positive_with_no_ceiling() {
    let caps = UsdCaps::new(d("20"), d("50")).unwrap();
    assert_eq!((caps.resting(), caps.inventory()), (d("20"), d("50")));
    // No ceiling.
    let huge = UsdCaps::new(Decimal::MAX, Decimal::MAX).unwrap();
    assert_eq!(huge.inventory(), Decimal::MAX);
    // A cap that is not positive is refused, the resting cap first.
    let refused = |r: &str, i: &str| UsdCaps::new(d(r), d(i)).unwrap_err();
    assert_eq!(
        refused("0", "50"),
        NotPositive {
            cap: Cap::Resting,
            usd: d("0")
        }
    );
    assert_eq!(
        refused("20", "-1"),
        NotPositive {
            cap: Cap::Inventory,
            usd: d("-1")
        }
    );
    assert_eq!(
        refused("20", "0"),
        NotPositive {
            cap: Cap::Inventory,
            usd: d("0")
        }
    );
    assert_eq!(refused("-3", "0").cap, Cap::Resting);
    let said = refused("0", "50").to_string();
    assert!(
        said.starts_with("the resting cap of $0 is not positive"),
        "{said}"
    );
    assert!(said.contains("no default and no ceiling"), "{said}");
    let said = refused("20", "-1").to_string();
    assert!(
        said.starts_with("the inventory cap of $-1 is not positive"),
        "{said}"
    );
    let error: &dyn std::error::Error = &refused("0", "1");
    assert!(error.source().is_none());
}

#[test]
fn the_caps_in_lots_are_each_at_their_own_price() {
    let caps = UsdCaps::new(d("20"), d("50")).unwrap();
    // $20 at 60140.1 on the 0.00001 step is 33 lots; $50 at 62000.2 is 80.
    assert_eq!(
        caps_in_lots(&caps, d("60140.1"), d("62000.2"), d("0.00001")),
        LotCaps {
            resting: lots(33),
            inventory: lots(80)
        }
    );
    // At a mark above the touch: $50 at 70000 is 71 lots.
    let at = at_market(d("60140.1"), Some(d("62000.2")), None, Some(d("70000")));
    assert_eq!(
        caps_in_lots(&caps, d("60140.1"), at, d("0.00001")).inventory,
        lots(71)
    );
    // Caps past what a lot count holds: the largest lot count.
    let huge = UsdCaps::new(Decimal::MAX, Decimal::MAX).unwrap();
    assert_eq!(
        caps_in_lots(&huge, d("1"), d("1"), d("1")),
        LotCaps {
            resting: lots(i64::MAX),
            inventory: lots(i64::MAX)
        }
    );
}

#[test]
fn a_minimum_that_is_none_or_zero_admits_every_order() {
    let tiny = lots(1);
    let step = d("0.00001");
    for min in [None, Some(Decimal::ZERO), Some(d("-1"))] {
        assert_eq!(
            admit_minimum(tiny, step, d("0.01"), Some(d("0.01")), min),
            Ok(()),
            "{min:?}"
        );
        assert_eq!(admit_minimum(Lots::ZERO, step, d("1"), None, min), Ok(()));
    }
}

#[test]
fn the_minimum_is_judged_at_the_price_and_then_at_the_mark() {
    let step = d("0.00001");
    let min = Some(d("10"));
    // 16 lots: $9.92 at 62000, $10.08 at 63000.
    assert_eq!(
        admit_minimum(lots(16), step, d("62000"), None, min),
        Err(LeftOut::BelowMinimum)
    );
    assert_eq!(admit_minimum(lots(16), step, d("63000"), None, min), Ok(()));
    // Below at its price: that, though the mark admits it.
    assert_eq!(
        admit_minimum(lots(16), step, d("62000"), Some(d("70000")), min),
        Err(LeftOut::BelowMinimum)
    );
    // Above at its price and below at the mark: refused at the mark alone.
    assert_eq!(
        admit_minimum(lots(16), step, d("63000"), Some(d("62000")), min),
        Err(LeftOut::BelowMinimumAtMark)
    );
    // Exactly the minimum is not below it, at the price or at the mark.
    assert_eq!(
        admit_minimum(lots(16), step, d("62500"), Some(d("62500")), min),
        Ok(())
    );
    // A notional past what a decimal holds is above any minimum.
    assert_eq!(
        admit_minimum(lots(i64::MAX), Decimal::MAX, d("2"), Some(d("2")), min),
        Ok(())
    );
}

/// The judgement the samples' desired book makes: $11 resting, $50 inventory, a $5 minimum on
/// the 0.00001 step, the mark at 62000.
fn judged(quote: SideQuote, position: i64, mark: &str, inventory_at: &str) -> Result<(), LeftOut> {
    let caps = UsdCaps::new(d("11"), d("50")).unwrap();
    let market = Market {
        step: d("0.00001"),
        mark: d(mark),
        inventory_at: d(inventory_at),
        min_notional: Some(d("5")),
    };
    admit_side(&caps, &market, quote, SignedLots(position))
}

fn bid(qty: i64, px: &str) -> SideQuote {
    SideQuote {
        side: Side::Buy,
        qty: lots(qty),
        px: d(px),
    }
}

fn ask(qty: i64, px: &str) -> SideQuote {
    SideQuote {
        side: Side::Sell,
        qty: lots(qty),
        px: d(px),
    }
}

#[test]
fn a_side_within_every_cap_and_above_the_minimum_is_admitted() {
    assert_eq!(judged(bid(16, "61814.1"), 0, "62000", "62186.6"), Ok(()));
    assert_eq!(judged(ask(16, "62186.6"), 0, "62000", "62186.6"), Ok(()));
}

#[test]
fn a_side_above_the_resting_cap_at_its_price_is_left_out() {
    // 18 lots at 62186.6 is $11.19, above the $11 cap.
    assert_eq!(
        judged(ask(18, "62186.6"), 0, "62000", "62186.6"),
        Err(LeftOut::AboveRestingCap)
    );
    // Judged before the minimum and the inventory cap: long past the cap, a bid still says this.
    assert_eq!(
        judged(bid(18, "62186.6"), 1_000, "62000", "62186.6"),
        Err(LeftOut::AboveRestingCap)
    );
    // At a mark of 20000 the same ask is $3.60, below the minimum there: the cap still says it.
    assert_eq!(
        judged(ask(18, "62186.6"), 0, "20000", "62186.6"),
        Err(LeftOut::AboveRestingCap)
    );
    // A notional past what a decimal holds is above any cap.
    let caps = UsdCaps::new(Decimal::MAX, Decimal::MAX).unwrap();
    let market = Market {
        step: Decimal::MAX,
        mark: d("1"),
        inventory_at: d("1"),
        min_notional: None,
    };
    assert_eq!(
        admit_side(&caps, &market, bid(i64::MAX, "2"), SignedLots(0)),
        Err(LeftOut::AboveRestingCap)
    );
}

#[test]
fn a_side_below_the_minimum_at_its_price_is_left_out() {
    // 5 lots at 61814.1 is $3.09, below the $5 minimum.
    assert_eq!(
        judged(bid(5, "61814.1"), 0, "62000", "62186.6"),
        Err(LeftOut::BelowMinimum)
    );
    // Judged before the inventory cap: long 80 lots, a fill would take the position to 85 lots,
    // $52.86 at 62186.6, past the $50 cap, and the bid still says this.
    assert_eq!(
        judged(bid(5, "61814.1"), 80, "62000", "62186.6"),
        Err(LeftOut::BelowMinimum)
    );
}

#[test]
fn a_side_above_the_minimum_at_its_price_and_below_it_at_the_mark_is_left_out() {
    // 9 lots at 62186.6 is $5.60 at its price; at a mark of 50000 it is $4.50.
    let quote = ask(9, "62186.6");
    assert_eq!(
        judged(quote, 0, "50000", "62186.6"),
        Err(LeftOut::BelowMinimumAtMark)
    );
    // The same quote with the mark at 62000 ($5.58): admitted, so the mark alone refused it.
    assert_eq!(judged(quote, 0, "62000", "62186.6"), Ok(()));
}

#[test]
fn a_side_whose_fill_takes_the_position_past_the_inventory_cap_is_left_out() {
    // Long 70 lots: a bid fill would take the position to 86 lots, $53.48 at 62186.6, past the
    // $50 cap; the ask, reducing it, is admitted.
    assert_eq!(
        judged(bid(16, "61814.1"), 70, "62000", "62186.6"),
        Err(LeftOut::PastInventoryCap)
    );
    assert_eq!(judged(ask(16, "62186.6"), 70, "62000", "62186.6"), Ok(()));
    // Short 70: the ask is left out and the bid admitted.
    assert_eq!(
        judged(ask(16, "62186.6"), -70, "62000", "62186.6"),
        Err(LeftOut::PastInventoryCap)
    );
    assert_eq!(judged(bid(16, "61814.1"), -70, "62000", "62186.6"), Ok(()));
}

#[test]
fn the_inventory_cap_is_judged_at_no_less_than_the_mark() {
    // Long 60 lots: 76 after a bid fill is $47.26 at 62186.6, within the $50 cap; at a mark of
    // 66000 it is $50.16, past it, though the consumer's price to judge at is lower.
    assert_eq!(judged(bid(16, "61814.1"), 60, "62000", "62186.6"), Ok(()));
    assert_eq!(
        judged(bid(16, "61814.1"), 60, "66000", "62186.6"),
        Err(LeftOut::PastInventoryCap)
    );
    // A position at the end of the lot count saturates there, and its worth past what a decimal
    // holds is past any cap.
    let caps = UsdCaps::new(Decimal::MAX, Decimal::MAX).unwrap();
    let market = Market {
        step: d("1e10"),
        mark: d("1"),
        inventory_at: d("1"),
        min_notional: None,
    };
    assert_eq!(
        admit_side(&caps, &market, bid(1, "1"), SignedLots(i64::MAX)),
        Err(LeftOut::PastInventoryCap)
    );
    assert_eq!(
        admit_side(&caps, &market, ask(1, "1"), SignedLots(i64::MIN)),
        Err(LeftOut::PastInventoryCap)
    );
    // A worth whose last product overflows: past any cap too.
    let market = Market {
        step: d("1"),
        mark: Decimal::MAX,
        inventory_at: d("1"),
        min_notional: None,
    };
    assert_eq!(
        admit_side(&caps, &market, bid(1, "1"), SignedLots(5)),
        Err(LeftOut::PastInventoryCap)
    );
}
