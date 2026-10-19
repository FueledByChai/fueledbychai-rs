//! The venue facts of a planner pass for the tests that judge none of them (decision 0111):
//! the account and the market in Normal mode, trading, no rate scope tracked, one order per
//! command.

use fbc_core::{OrderCaps, TradingStatus, VenueMode};
use fbc_oms::{Batching, PlanVenue, RateView};

/// `caps` with the account and the market in Normal mode, trading, no rate scope tracked and
/// one order per command.
pub fn of(caps: &OrderCaps) -> PlanVenue<'_> {
    PlanVenue {
        caps,
        account_mode: VenueMode::Normal,
        market_mode: VenueMode::Normal,
        status: TradingStatus::Trading,
        rates: RateView::new(Vec::new()),
        batching: Batching::One,
    }
}
