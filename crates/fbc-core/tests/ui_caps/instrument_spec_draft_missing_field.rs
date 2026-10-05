use fbc_core::{
    AssetKey, AssetSym, FundingSpec, InstrumentKind, InstrumentSpecDraft, Lots, PriceGrid,
    SizeStep, TradingStatus, VenueSymbol,
};
use rust_decimal::Decimal;

fn draft(asset: AssetKey, venue_symbol: VenueSymbol, settle_ccy: AssetSym) -> InstrumentSpecDraft {
    // A draft without its size step does not compile: discovery states every field, and no
    // size step is guessed (decisions 0003, 0004).
    InstrumentSpecDraft {
        asset,
        venue_symbol,
        native_id: None,
        price_grid: PriceGrid::fixed(Decimal::ONE).unwrap(),
        quote_grid: None,
        min_size: Lots::new(1).unwrap(),
        min_notional: None,
        max_order_size: None,
        position_limit: None,
        price_band: None,
        max_open_orders: None,
        multiplier: Decimal::ONE,
        settle_ccy,
        funding: FundingSpec::Unknown,
        public_fees: None,
        status: TradingStatus::Trading,
    }
}

fn main() {
    let _ = (draft, InstrumentKind::Perpetual, SizeStep::new(Decimal::ONE));
}
