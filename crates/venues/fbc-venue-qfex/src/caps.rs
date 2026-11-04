//! Public MDS capabilities from QFEX's [WebSocket overview](https://docs.qfex.com/websocket/main),
//! [pulsed BBO](https://docs.qfex.com/websocket/channels/mds/bbo),
//! [pulsed book](https://docs.qfex.com/websocket/channels/mds/orderbook),
//! [trade](https://docs.qfex.com/websocket/channels/mds/trades),
//! [mark price](https://docs.qfex.com/websocket/channels/mds/mark_price),
//! [underlier](https://docs.qfex.com/websocket/channels/mds/underlier) and
//! [funding rate](https://docs.qfex.com/websocket/channels/mds/funding_rate) pages, and the
//! 2026-11-04 public capture in `fixtures/qfex/live/` (its README).

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
                // The public trade UUID is retained in the normalized TradeId.
                trade_id: true,
            },
            funding: FundingCaps {
                // The `funding` channel.
                source: FeedSource::Stream,
                // No frame states the interval; the funding page's hourly settlement is not a
                // field of the frame.
                interval_reported: false,
                // `time_remaining` counts from when a frame was sent, so the capture's
                // `time` + `time_remaining` falls seconds short of the settlement: no frame
                // states the next funding time.
                next_time_reported: false,
            },
            stats: FeedSource::None,
            // The `mark_price` channel.
            mark: FeedSource::Stream,
            // The `underlier` channel, its `external` prices only (the codec's docs).
            index: FeedSource::Stream,
            // The public MDS codec decodes no market's trading status, so none is reported and
            // discovery's snapshot stands (decision 0130).
            status: FeedSource::None,
            ts_precision: Duration::from_nanos(1),
            topology: ConnTopology::Shared {
                max_subscriptions: None,
            },
            max_conn_lifetime: None,
        },
        // QFEX's published 12,000-unit limit covers the authenticated trading WebSocket,
        // not this public MDS connection. No public numeric limit is documented.
        limits: Vec::new(),
        exec_limits: Some(crate::trade_rate::limits().to_vec()),
        readiness_ceiling: Readiness::Record,
    }
}
