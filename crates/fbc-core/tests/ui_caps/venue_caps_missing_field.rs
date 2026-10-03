use std::time::Duration;

use fbc_core::{
    Cadence, ConnTopology, Encoding, ExchTsKind, FeedSource, FundingCaps, MatchingCaps, MdCaps,
    SeqDomain, StpScope, TagSet, TouchSourceCaps, TradeCaps, VenueCaps,
};

fn main() {
    // Every field but readiness_ceiling is declared; with no Default to fill it, this does not
    // compile (decision 0003).
    let _ = VenueCaps {
        exec: None,
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
