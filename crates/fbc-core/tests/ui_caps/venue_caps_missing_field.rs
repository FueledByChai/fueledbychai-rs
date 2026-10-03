use std::time::Duration;

use fbc_core::{
    Cadence, ConnTopology, Encoding, ExchTsKind, FeedSource, FillCaps, FillSource,
    FundingCaps, MatchingCaps, MdCaps, SeqDomain, StpScope, TagSet, TouchSourceCaps, TradeCaps,
    VenueCaps, VenueFeeSign,
};

fn main() {
    // Every field but readiness_ceiling is declared; with no Default to fill it, this does not
    // compile (decision 0003).
    let _ = VenueCaps {
        order: None,
        fills: FillCaps {
            source: FillSource::Native,
            liquidity_flag: true,
            realized_pnl: true,
            realized_funding: true,
            fee_sign: VenueFeeSign::PositiveIsCost,
            fee_asset_reported: true,
            fill_id: true,
            replays_fills_on_reconnect: false,
        },
        matching: MatchingCaps {
            speed_bump: None,
            stp_scope: StpScope::None,
        },
        md: MdCaps {
            encoding: Encoding::Json,
            touch_sources: vec![TouchSourceCaps {
                channel: "bbo",
                cadence: Cadence::Realtime,
                seq_domain: SeqDomain::Own,
                ts_kind: ExchTsKind::Publish,
                includes_channels: TagSet::none(),
            }],
            books: vec![],
            trades: TradeCaps {
                source: FeedSource::Stream,
                aggressor: true,
                trade_id: true,
            },
            funding: FundingCaps {
                source: FeedSource::None,
                interval_reported: false,
                next_time_reported: false,
            },
            stats: FeedSource::None,
            mark: FeedSource::Stream,
            index: FeedSource::None,
            ts_precision: Duration::from_millis(1),
            topology: ConnTopology::PerChannel,
            max_conn_lifetime: None,
        },
        limits: vec![],
    };
}
