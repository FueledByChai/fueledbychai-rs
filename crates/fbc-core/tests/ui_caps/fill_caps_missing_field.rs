use fbc_core::{FillCaps, FillSource};

fn main() {
    // A fill capability without its fee sign does not compile: no fee convention is assumed
    // (decisions 0003, 0004).
    let _ = FillCaps {
        source: FillSource::Native,
        liquidity_flag: true,
        realized_pnl: true,
        realized_funding: true,
        fee_asset_reported: true,
        fill_id: true,
        replays_fills_on_reconnect: false,
    };
}
