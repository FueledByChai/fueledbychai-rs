//! What the Binance USD-M tests share: a configuration, a spec table of synthetic instruments
//! (their grids are the tests', not Binance's), a sink that keeps what it is pushed, and the
//! hand-written fixtures under `fixtures/binance-usdm/`.

#![allow(dead_code)]

use std::path::PathBuf;

use fbc_core::{
    AssetSym, FundingSpec, InstrumentId, InstrumentKind, InstrumentSpec, Lots, MdEvent, MdSink,
    PriceGrid, SizeStep, SpecTable, TradingStatus, UnderlyingId, VenueConfig, VenueFactory,
    VenueId, VenueMeta, WallNs, dispatch_market_data,
};
use fbc_venue_binance_usdm::{BinanceUsdm, KEY_DEPTH_LEVELS, KEY_DEPTH_SPEED, KEY_WS_BASE_URL};
use rust_decimal::Decimal;

pub const BTC: InstrumentId = InstrumentId::new(1);
pub const ETH: InstrumentId = InstrumentId::new(2);
/// In no spec table the tests build.
pub const SOL: InstrumentId = InstrumentId::new(3);

/// The configuration the tests run under: five levels every 100 ms.
pub fn config() -> VenueConfig {
    let mut cfg = VenueConfig::new();
    cfg.insert(KEY_WS_BASE_URL, "wss://fstream.binance.com");
    cfg.insert(KEY_DEPTH_LEVELS, "5");
    cfg.insert(KEY_DEPTH_SPEED, "100ms");
    cfg
}

fn spec(id: InstrumentId, symbol: &str, tick: &str) -> InstrumentSpec {
    let caps = BinanceUsdm.caps(&config()).unwrap();
    let venue_symbol = dispatch_market_data(&caps, |scope| scope.venue_symbol(symbol)).unwrap();
    let usdt = AssetSym::new("USDT").unwrap();
    InstrumentSpec {
        id,
        venue: VenueId::new(3),
        venue_symbol,
        native_id: None,
        underlying: UnderlyingId::new(id.get()),
        kind: InstrumentKind::Perpetual,
        price_grid: PriceGrid::fixed(tick.parse().unwrap()).unwrap(),
        quote_grid: None,
        size_step: SizeStep::new("0.001".parse::<Decimal>().unwrap()).unwrap(),
        min_size: Lots::new(1).unwrap(),
        min_notional: None,
        max_order_size: None,
        position_limit: None,
        price_band: None,
        max_open_orders: None,
        multiplier: Decimal::ONE,
        quote_ccy: usdt,
        settle_ccy: usdt,
        funding: FundingSpec::Unknown,
        public_fees: None,
        status: TradingStatus::Trading,
        version: 1,
        fetched_at: WallNs(0),
    }
}

/// BTCUSDT and ETHUSDT, both on a 0.01 grid with 0.001 lots.
pub fn specs() -> SpecTable {
    let mut table = SpecTable::new();
    table.insert(spec(BTC, "BTCUSDT", "0.01"));
    table.insert(spec(ETH, "ETHUSDT", "0.01"));
    table
}

/// Keeps every event it is pushed, in order.
#[derive(Default)]
pub struct Sink(pub Vec<(VenueMeta, MdEvent)>);

impl MdSink for Sink {
    fn push(&mut self, meta: VenueMeta, ev: MdEvent) {
        self.0.push((meta, ev));
    }
}

/// The frames of fixture `name`, one per line.
pub fn fixture(name: &str) -> Vec<String> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../../fixtures/binance-usdm/md")
        .join(name);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{path:?}: {e}"));
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .map(str::to_owned)
        .collect()
}
