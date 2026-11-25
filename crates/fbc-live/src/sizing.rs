//! Mark-aware sizing (decisions 0096, 0113 and 0155 item 6): the operator's caps in lots, the
//! price a position's worth is judged at, and whether one side's quote is left out.
//!
//! Sizing reads no venue and no clock. The mark is the consumer's argument: `MdEvent::Mark`
//! where `MdCaps::mark` declares one, or its own read. The minimum notional is the venue's, not
//! a bound: the spec's `min_notional` where discovery states one, or else the consumer's own
//! reading of it. A spec's `min_notional` is a `Money` in nanos, which converts exactly with
//! `Decimal::try_from_i128_with_scale(money.nanos, 9)`; `None` or zero leaves no side out. The
//! minimum is judged at a quote's price and at the mark for every venue, which only ever leaves
//! out more, so no capability says where a venue judges it (0155 item 6). fbc-oms still judges
//! the caps at authorization (0082) and refuses a place, batch or amend at submit once a fill
//! moved its market's position (0153).
//!
//! The operator's caps are positive with no ceiling (0113): [`UsdCaps::new`] refuses a cap that
//! is not positive and has no `Default` (0009). A cap past what a lot count holds is the largest
//! lot count ([`cap_lots`]), and fbc-oms's checked arithmetic then bounds every order as at any
//! large cap.

use std::fmt;

use fbc_core::{Decimal, Lots, Side, SignedLots};

/// The price the inventory cap is converted at: the highest of the order's price `px` and the
/// touch's sides that rest, so a position is never worth more than the cap at the market. A
/// buy's own price is below the bid, so on a book with no asks the bid is the market's price
/// (Codex r4226246459 on PR #114).
pub fn inventory_price(px: Decimal, bid: Option<Decimal>, ask: Option<Decimal>) -> Decimal {
    [bid, ask].into_iter().flatten().fold(px, Decimal::max)
}

/// [`inventory_price`] of the order's price `px` and the touch, or the consumer's `mark` when it
/// is higher: the price the inventory cap is judged at, never less than the mark (0096).
pub fn at_market(
    px: Decimal,
    bid: Option<Decimal>,
    ask: Option<Decimal>,
    mark: Option<Decimal>,
) -> Decimal {
    mark.into_iter()
        .fold(inventory_price(px, bid, ask), Decimal::max)
}

/// A cap of `usd` in lots of `step` at `price`, floored. The caps are the operator's own, with
/// no ceiling (FBC-thks, decision 0113), so a cap past what a lot count holds (or a decimal
/// holds, once divided) is the largest lot count, never zero and never a panic (Codex
/// r4320205456 on PR #184); fbc-oms's checked arithmetic then bounds every order as at any
/// large cap. A lot whose own cost does not fit a decimal costs more than any cap, so it gives
/// no lot (Codex r4320317907), and a re-check at such a price never admits an order; so does a
/// price, a step or a cap that is not positive.
pub fn cap_lots(usd: Decimal, price: Decimal, step: Decimal) -> Lots {
    let per_lot = match price.checked_mul(step) {
        Some(per) if per > Decimal::ZERO => per,
        _ => return Lots::ZERO,
    };
    if usd <= Decimal::ZERO {
        return Lots::ZERO;
    }
    let lots = usd
        .checked_div(per_lot)
        .and_then(|n| i64::try_from(n.floor()).ok())
        .unwrap_or(i64::MAX);
    Lots::new(lots).unwrap_or(Lots::ZERO)
}

/// Which of the operator's caps.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cap {
    /// The resting cap per side, judged at a quote's own price.
    Resting,
    /// The inventory cap, judged at no less than the mark.
    Inventory,
}

/// A cap [`UsdCaps::new`] refused: it is not positive.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NotPositive {
    pub cap: Cap,
    pub usd: Decimal,
}

impl fmt::Display for NotPositive {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let which = match self.cap {
            Cap::Resting => "resting",
            Cap::Inventory => "inventory",
        };
        write!(
            f,
            "the {which} cap of ${} is not positive: the caps are the operator's, required, with \
             no default and no ceiling",
            self.usd
        )
    }
}

impl std::error::Error for NotPositive {}

/// The operator's two caps in USD (0113): the resting cap per side and the inventory cap, each
/// positive, with no ceiling and no `Default`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UsdCaps {
    resting: Decimal,
    inventory: Decimal,
}

impl UsdCaps {
    /// The caps, or the first that is not positive (the resting cap judged first).
    pub fn new(resting: Decimal, inventory: Decimal) -> Result<UsdCaps, NotPositive> {
        for (cap, usd) in [(Cap::Resting, resting), (Cap::Inventory, inventory)] {
            if usd <= Decimal::ZERO {
                return Err(NotPositive { cap, usd });
            }
        }
        Ok(UsdCaps { resting, inventory })
    }

    /// The resting cap per side.
    pub fn resting(&self) -> Decimal {
        self.resting
    }

    /// The inventory cap.
    pub fn inventory(&self) -> Decimal {
        self.inventory
    }
}

/// The operator's caps in lots, as fbc-oms's pre-trade caps hold them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LotCaps {
    pub resting: Lots,
    pub inventory: Lots,
}

/// `caps` in lots of `step` ([`cap_lots`]): the resting cap at `resting_at`, the price an order
/// rests at, and the inventory cap at `inventory_at`, the price a position's worth is judged at
/// ([`at_market`], never less than the mark).
pub fn caps_in_lots(
    caps: &UsdCaps,
    resting_at: Decimal,
    inventory_at: Decimal,
    step: Decimal,
) -> LotCaps {
    LotCaps {
        resting: cap_lots(caps.resting, resting_at, step),
        inventory: cap_lots(caps.inventory, inventory_at, step),
    }
}

/// Why one side's quote is left out ([`admit_minimum`], [`admit_side`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeftOut {
    /// Its notional at its own price is above the resting cap.
    AboveRestingCap,
    /// Its notional at its own price is below the minimum notional.
    BelowMinimum,
    /// Its notional at the mark is below the minimum notional, where a venue may judge it and
    /// refuse the order only after accepting it (0096).
    BelowMinimumAtMark,
    /// A fill of it would take the position past the inventory cap, judged at no less than the
    /// mark.
    PastInventoryCap,
}

/// `qty` lots of `step` at `px`, or `None` when it does not fit a decimal.
fn notional(qty: Lots, step: Decimal, px: Decimal) -> Option<Decimal> {
    Decimal::from(qty.get()).checked_mul(step)?.checked_mul(px)
}

/// Whether `qty` lots of `step` reach the minimum notional `min_notional` at the order's price
/// `px` and, when the consumer has one, at the `mark`: `BelowMinimum` or `BelowMinimumAtMark`
/// when not, judged in that order. A minimum that is `None`, zero or less admits every order,
/// and a notional too large for a decimal is above any minimum.
pub fn admit_minimum(
    qty: Lots,
    step: Decimal,
    px: Decimal,
    mark: Option<Decimal>,
    min_notional: Option<Decimal>,
) -> Result<(), LeftOut> {
    let Some(min) = min_notional.filter(|min| *min > Decimal::ZERO) else {
        return Ok(());
    };
    let below = |at: Decimal| notional(qty, step, at).is_some_and(|n| n < min);
    if below(px) {
        return Err(LeftOut::BelowMinimum);
    }
    if mark.is_some_and(below) {
        return Err(LeftOut::BelowMinimumAtMark);
    }
    Ok(())
}

/// One side's quote: `qty` lots at the price `px`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SideQuote {
    pub side: Side,
    pub qty: Lots,
    pub px: Decimal,
}

/// What a side's quote is judged against, beside the caps: the size of one lot, the
/// consumer's mark, the price a position's worth is judged at (the highest of the quote prices
/// and the touch, [`at_market`]; [`admit_side`] takes the mark when it is higher) and the
/// minimum notional.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Market {
    pub step: Decimal,
    pub mark: Decimal,
    pub inventory_at: Decimal,
    pub min_notional: Option<Decimal>,
}

/// Whether `quote` is left out with the position at `position`, judged in this order: its
/// notional at its price above the resting cap, below the minimum at its price or at the mark
/// ([`admit_minimum`]), or a fill of it taking the position past the inventory cap at no less
/// than the mark (0096, 0155 item 6). A notional or a position's worth too large for a decimal
/// is above any cap.
pub fn admit_side(
    caps: &UsdCaps,
    market: &Market,
    quote: SideQuote,
    position: SignedLots,
) -> Result<(), LeftOut> {
    let step = market.step;
    if notional(quote.qty, step, quote.px).is_none_or(|n| n > caps.resting) {
        return Err(LeftOut::AboveRestingCap);
    }
    admit_minimum(
        quote.qty,
        step,
        quote.px,
        Some(market.mark),
        market.min_notional,
    )?;
    let after = match quote.side {
        Side::Buy => position.0.saturating_add(quote.qty.get()),
        Side::Sell => position.0.saturating_sub(quote.qty.get()),
    };
    let at = market.inventory_at.max(market.mark);
    let worth = Decimal::from(after.unsigned_abs())
        .checked_mul(step)
        .and_then(|w| w.checked_mul(at));
    if worth.is_none_or(|w| w > caps.inventory) {
        return Err(LeftOut::PastInventoryCap);
    }
    Ok(())
}
