//! What Hibachi offers as market data, and the limits it enforces (decision 0003: each value
//! cites its source; decision 0015: order and fill capabilities are declared together or not at
//! all, and this crate declares none).
//!
//! Sources cited:
//!
//! - [WS] Hibachi's Market WebSocket API, <https://hibachi-docs.redocly.app/wsapi/market>: the
//!   public topics (`mark_price`, `spot_price`, `ask_bid_price`, `funding_rate_estimation`,
//!   `trades`, `klines`, `orderbook`) on one connection; the orderbook's optional `granularity`
//!   and `depth`, one set per subscription; "Notes for orderbook events" (a `Snapshot` on
//!   subscribe, then `Update`s "periodically", each side's levels bounded by its `startPrice`
//!   and `endPrice`); and its example frames, whose orderbook frame carries `depth` and
//!   `granularity` and no sequence number or timestamp, whose trade's `timestamp` is whole seconds
//!   (`1712782611`), and whose `funding_rate_estimation` carries `estimatedFundingRate` and
//!   `nextFundingTimestamp` and no interval.
//! - [SDK] Hibachi's Python SDK at 14a76b9, `get_orderbook`:
//!   <https://github.com/hibachi-xyz/hibachi_sdk/blob/14a76b95230869dd622c31a1fc4132a6eac1a9fa/python/hibachi_xyz/api.py#L698-L735>:
//!   price levels "aggregated based on the specified granularity", `depth` "between 1 and 100,
//!   inclusive", the granularity one of the contract's `orderbookGranularities`.
//! - [QE] The Java library's `HibachiQuoteEngine.java`,
//!   <https://github.com/FueledByChai/FueledByChaiTrading/blob/b4188a57f7d5ea4e82260c9f3c4a0a359ad04c27/implementations/market-data-api/hibachi-market-data-impl/src/main/java/com/fueledbychai/marketdata/hibachi/HibachiQuoteEngine.java>:
//!   L100-L104, "No sequence number is available"; L108-L114 and L561-L567, Hibachi's word,
//!   recorded when the L1 sanity checker was removed on 2026-05-27, that "both L1 and trades
//!   pulse on the same 250ms heartbeat".
//! - [LIMITS] <https://docs.hibachi.xyz/hibachi-docs/trading/limits>: "an overall rate limit of
//!   350 requests / 10 seconds per IP".
//! - [BUMP] <https://docs.hibachi.xyz/hibachi-docs/trading/taker-speed-bump>: a 25 millisecond
//!   speed bump on liquidity-taking orders across all markets; resting and post-only orders and
//!   cancels are not delayed; "This value may change".
//!
//! The orderbook and funding entries describe those feeds as Hibachi documents them, as this
//! crate's first done line (FBC-jyyl) requires; `plan_md` plans neither until the codec that
//! decodes them lands (FBC-2kmp), refusing each as an unsupported feed. The touch, trade, mark
//! and index feeds are declared with that codec.

use core::time::Duration;

use fbc_core::{
    BookCaps, Cadence, Channel, ConnTopology, Continuity, Encoding, FeedSource, FundingCaps,
    LimitScope, MatchingCaps, MdCaps, OpKind, QueueModelQuality, RateLimit, Readiness, SpeedBump,
    SpeedBumpScope, StpScope, TagSet, TradeCaps, VenueCaps,
};

use crate::config::Settings;

/// The orderbook topic's name [WS].
pub(crate) const ORDERBOOK_CHANNEL: &str = "orderbook";

/// Hibachi's capabilities under `settings`.
pub(crate) fn caps(settings: &Settings) -> VenueCaps {
    VenueCaps {
        // Market data only: order entry waits for its own decision record (BT-604), so no order
        // or fill capability is declared (0015).
        exec: None,
        matching: MatchingCaps {
            // 25 ms on liquidity-taking orders only; cancels are not delayed [BUMP].
            speed_bump: Some(SpeedBump {
                delay: Duration::from_millis(25),
                applies_to: SpeedBumpScope::TakersOnly,
            }),
            // No self-trade prevention is documented, so none is claimed.
            stp_scope: StpScope::None,
        },
        md: MdCaps {
            // Every frame is JSON [WS].
            encoding: Encoding::Json,
            // `ask_bid_price` [WS] is declared with the codec that decodes it (FBC-2kmp).
            touch_sources: Vec::new(),
            // BOOK_ORDERBOOK.
            books: vec![BookCaps {
                channel: ORDERBOOK_CHANNEL,
                // The depth each subscription names [WS], from the required
                // `hibachi.orderbook_depth`, 1 to 100 [SDK].
                max_depth: settings.depth,
                // The owner's decision of 2026-10-25: Unknown. Hibachi support's 250 ms
                // heartbeat (2026-05-27, stated for L1 and trades [QE] L108-L114, L561-L567) is
                // not taken, since the owner judged decisive that his own capture of the public
                // feed (2026-10-22, its measurements in his private notes) disagreed with it;
                // no value measured from that capture is declared either. [WS] says only that
                // Updates come "periodically".
                cadence: Cadence::Unknown,
                // No sequence number in any frame [WS]; "No sequence number is available"
                // [QE] L100-L104.
                continuity: Continuity::Unsequenced,
                // Each side's levels lie between its startPrice and endPrice, at most the
                // subscription's depth [WS].
                windowed: true,
                // A Snapshot comes first on each subscription [WS]; no REST anchor.
                rest_anchor: false,
                // The public book; no other order channel is documented.
                includes_channels: TagSet::of(&[Channel::Public]),
                // Levels aggregated at the subscription's granularity [SDK], the quote grid,
                // while orders rest on the finer tickSize: a level is a bucket and its size the
                // bucket's sum, so brackets only (BT-601; FBC-jwn9 adds the capability that
                // says a book's levels are buckets).
                queue_model: QueueModelQuality::BracketOnly,
            }],
            // The `trades` topic [WS] is declared with the codec that decodes it (FBC-2kmp).
            trades: TradeCaps {
                source: FeedSource::None,
                aggressor: false,
                trade_id: false,
            },
            funding: FundingCaps {
                // The `funding_rate_estimation` topic [WS].
                source: FeedSource::Stream,
                // Its frames carry no interval [WS].
                interval_reported: false,
                // They carry `nextFundingTimestamp`, but no public source states its unit (the
                // Market API spec types it int64 with none), so it is not declared reported
                // until a fixture shaped after evidence pins it (FBC-2kmp).
                next_time_reported: false,
            },
            // REST /market/data/stats is not polled.
            stats: FeedSource::None,
            // `mark_price` and `spot_price` [WS] are declared with the codec that decodes them
            // (FBC-2kmp).
            mark: FeedSource::None,
            index: FeedSource::None,
            // The coarsest exchange timestamp the streamed frames carry: a trade's `timestamp`
            // is whole seconds [WS]. No public example frame shows a timestamp on the orderbook,
            // `ask_bid_price` or `mark_price` topics [WS], so the value rests on the trade's
            // alone. One value covers the venue.
            ts_precision: Duration::from_secs(1),
            // Several symbols and topics share one connection, and no cap is stated [WS].
            topology: ConnTopology::Shared {
                max_subscriptions: None,
            },
            // No connection lifetime is stated [WS].
            max_conn_lifetime: None,
        },
        // 350 requests per 10 seconds per IP [LIMITS], each REST request charging one; no
        // WebSocket message or connection limit is documented, and the page's sub-account
        // order limits belong to order entry.
        limits: vec![RateLimit {
            scope: LimitScope::Ip,
            ops: TagSet::of(&[OpKind::Rest]),
            per: Duration::from_secs(10),
            units: 350,
        }],
        // Recording only: no order path exists (BT-601 to BT-603 open none).
        exec_limits: None,
        readiness_ceiling: Readiness::Record,
    }
}
