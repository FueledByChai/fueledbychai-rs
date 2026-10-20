//! Public MDS capabilities from QFEX's [WebSocket overview](https://docs.qfex.com/websocket/main),
//! [pulsed BBO](https://docs.qfex.com/websocket/channels/mds/bbo),
//! [pulsed book](https://docs.qfex.com/websocket/channels/mds/orderbook), and
//! [trade](https://docs.qfex.com/websocket/channels/mds/trades) pages.

use std::time::Duration;

use fbc_core::{
    BookCaps, Cadence, Channel, ConnTopology, Continuity, Encoding, ExchTsKind, FeedSource,
    FundingCaps, MatchingCaps, MdCaps, QueueModelQuality, Readiness, SeqDomain, StpScope, TagSet,
    TouchSourceCaps, TradeCaps, VenueCaps,
};

pub(crate) fn caps() -> VenueCaps {
    VenueCaps {
        exec: None,
        matching: MatchingCaps {
            speed_bump: None,
            // No protection is claimed until the private trading path is documented and tested.
            stp_scope: StpScope::None,
        },
        md: MdCaps {
            encoding: Encoding::Json,
            touch_sources: vec![TouchSourceCaps {
                channel: "bbo",
                // The BBO page calls the stream pulsed but does not state its interval.
                cadence: Cadence::Unknown,
                // Published examples have a sequence, but do not promise cross-feed ordering.
                seq_domain: SeqDomain::Own,
                ts_kind: ExchTsKind::Unknown,
                includes_channels: TagSet::of(&[Channel::Public]),
            }],
            books: vec![BookCaps {
                channel: "level2",
                max_depth: 20,
                cadence: Cadence::Pulsed(Duration::from_millis(500)),
                continuity: Continuity::Windowed,
                windowed: true,
                rest_anchor: false,
                includes_channels: TagSet::of(&[Channel::Public]),
                queue_model: QueueModelQuality::BracketOnly,
            }],
            trades: TradeCaps {
                source: FeedSource::Stream,
                // The public trade's side is documented as BUY/SELL, without saying
                // whether it names the taker or the resting order.
                aggressor: false,
                // QFEX sends UUID trade ids; MdEvent::Trade has an optional u64 id.
                trade_id: false,
            },
            funding: FundingCaps {
                source: FeedSource::None,
                interval_reported: false,
                next_time_reported: false,
            },
            stats: FeedSource::None,
            mark: FeedSource::None,
            index: FeedSource::None,
            ts_precision: Duration::from_nanos(1),
            topology: ConnTopology::Shared {
                max_subscriptions: None,
            },
            max_conn_lifetime: None,
        },
        // QFEX's published 12,000-unit limit covers the authenticated trading WebSocket,
        // not this public MDS connection. No public numeric limit is documented.
        limits: Vec::new(),
        readiness_ceiling: Readiness::Record,
    }
}
