//! What Binance USD-M offers as market data, and the limits it enforces (decision 0003: each
//! value cites the venue's documentation).
//!
//! Pages cited, all under
//! `https://developers.binance.com/docs/derivatives/usds-margined-futures/`:
//!
//! - [CONNECT] `websocket-market-streams/Connect`: the routed paths (`/public`, `/market`,
//!   `/private`), the combined-stream wrapper, lowercase symbols, "A single connection is only
//!   valid for 24 hours", the server's ping every 3 minutes, "WebSocket connections have a limit
//!   of 10 incoming messages per second", "A single connection can listen to a maximum of 1024
//!   streams".
//! - [TICKER] `websocket-market-streams/Individual-Symbol-Book-Ticker-Streams`: `<symbol>@bookTicker`,
//!   real time; `u` is the order book update id, `E` the event time, `T` the transaction time.
//! - [PARTIAL] `websocket-market-streams/Partial-Book-Depth-Streams`: `<symbol>@depth<levels>`,
//!   `@depth<levels>@500ms`, `@depth<levels>@100ms`; levels 5, 10 or 20; 250, 500 or 100 ms.
//!
//! Only the book channel the codec decodes is declared: the diff-depth channel (`depth@100ms`)
//! and its REST anchor come with their codec (FBC-tfb).
//!
//! - [DEPTH] `market-data/rest-api/Order-Book`: `GET /fapi/v1/depth`, weight 2 for limits 5, 10,
//!   20 and 50, 5 for 100, 10 for 500, 20 for 1000.
//! - [INFO] `market-data/rest-api/Exchange-Information`: `rateLimits` holds `REQUEST_WEIGHT`,
//!   interval MINUTE, intervalNum 1, limit 2400; general-info: the limits are per IP.
//! - [STP] `trade/rest-api` (New Order): `selfTradePreventionMode` defaults to `NONE`.
//! - [CHANGES] `https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/change-log`,
//!   2023-08-29: Self-Trade Prevention prevents matching within one account or the accounts of
//!   one `tradeGroupId`; effective 2023-09-05.

use core::num::NonZeroU32;
use core::time::Duration;

use fbc_core::{
    BookCaps, Cadence, Channel, ConnTopology, Continuity, Encoding, ExchTsKind, FeedSource,
    FundingCaps, LimitScope, MatchingCaps, MdCaps, OpKind, QueueModelQuality, RateLimit, Readiness,
    SeqDomain, StpScope, TagSet, TouchSourceCaps, TradeCaps, VenueCaps,
};

use crate::config::Settings;

/// The most streams one connection carries [CONNECT].
pub(crate) const MAX_STREAMS: usize = 1024;

/// The documented weight of `GET /fapi/v1/depth?limit=<limit>` against the IP's request-weight
/// budget [DEPTH], for the charge on a REST book anchor; `None` for a limit Binance does not
/// accept (5, 10, 20, 50, 100, 500 and 1000 are its values).
pub fn rest_depth_weight(limit: u16) -> Option<NonZeroU32> {
    let weight = match limit {
        5 | 10 | 20 | 50 => 2,
        100 => 5,
        500 => 10,
        1000 => 20,
        _ => return None,
    };
    NonZeroU32::new(weight)
}

/// Binance USD-M's capabilities under `settings`.
pub(crate) fn caps(settings: &Settings) -> VenueCaps {
    let depth = settings.depth;
    VenueCaps {
        // Reference data only: this library never trades here (0015, 0016).
        exec: None,
        matching: MatchingCaps {
            // No delay on incoming orders is documented.
            speed_bump: None,
            // STP "will prevent orders from matching with orders from the same account, or
            // accounts under the same tradeGroupId" [CHANGES, 2023-08-29]: the owner's scope.
            // An order opts in (selfTradePreventionMode defaults to NONE [STP]); this library
            // places none here.
            stp_scope: StpScope::Owner,
        },
        md: MdCaps {
            encoding: Encoding::Json,
            touch_sources: vec![TouchSourceCaps {
                channel: "bookTicker",
                // "Update Speed: Real-time" [TICKER].
                cadence: Cadence::Realtime,
                // Its `u` is the order book's update id, the depth streams' `u` [TICKER].
                seq_domain: SeqDomain::SharedWithBook,
                // The codec reports `T`, the transaction time [TICKER].
                ts_kind: ExchTsKind::MatchingEngine,
                // The public book; no retail-price-improvement stream is decoded here.
                includes_channels: TagSet::of(&[Channel::Public]),
            }],
            books: vec![BookCaps {
                channel: depth.name,
                max_depth: depth.levels,
                // Published every 100, 250 or 500 ms [PARTIAL].
                cadence: Cadence::Pulsed(depth.speed),
                // Each message is the top `levels` per side: nothing to chain.
                continuity: Continuity::Windowed,
                windowed: true,
                rest_anchor: false,
                includes_channels: TagSet::of(&[Channel::Public]),
                // Aggregated top-of-book snapshots: brackets only.
                queue_model: QueueModelQuality::BracketOnly,
            }],
            // Published by Binance but not decoded by this adapter.
            trades: TradeCaps {
                source: FeedSource::None,
                aggressor: false,
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
            // `E` and `T` are milliseconds [TICKER], [PARTIAL].
            ts_precision: Duration::from_millis(1),
            topology: ConnTopology::Shared {
                max_subscriptions: Some(MAX_STREAMS as u32),
            },
            // "A single connection is only valid for 24 hours" [CONNECT].
            max_conn_lifetime: Some(Duration::from_secs(24 * 60 * 60)),
        },
        limits: vec![
            // REQUEST_WEIGHT 2400 per minute per IP [INFO]; each REST request charges its
            // documented weight (rest_depth_weight for a book anchor).
            RateLimit {
                scope: LimitScope::Ip,
                ops: TagSet::of(&[OpKind::Rest]),
                per: Duration::from_secs(60),
                units: 2400,
            },
            // 10 incoming messages per second per connection [CONNECT]: every frame written
            // on it, the SUBSCRIBE and UNSUBSCRIBE requests and any control frame.
            RateLimit {
                scope: LimitScope::Connection,
                ops: TagSet::of(&[OpKind::Subscribe, OpKind::Control]),
                per: Duration::from_secs(1),
                units: 10,
            },
            // New connections per IP: the USD-M pages state no number. Binance documents 300
            // connection attempts every 5 minutes per IP for its WebSocket streams (Spot,
            // "General WebSocket Streams information"); the same cap is declared here so the
            // runtime never opens connections faster than any documented Binance limit, from
            // an egress IP the Java processes share.
            RateLimit {
                scope: LimitScope::Ip,
                ops: TagSet::of(&[OpKind::Connect]),
                per: Duration::from_secs(5 * 60),
                units: 300,
            },
        ],
        // Recording only: the reference feed, never traded.
        readiness_ceiling: Readiness::Record,
    }
}
