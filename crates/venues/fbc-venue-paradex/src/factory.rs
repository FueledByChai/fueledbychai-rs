//! The Paradex venue factory: capabilities, configuration schema and market-data plan
//! (decisions 0003, 0015, 0016).
//!
//! Paradex is market data only here until order entry lands (BT-402): `exec` is `None`, and
//! there is no order-entry codec or endpoint. Each capability cites the document it comes
//! from: docs.paradex.trade, or the SBE schema `paradex_1_0.xml` in tradeparadex/paradex-py at
//! commit `b8248fb747e278d2167ac2f056b339a287d5ef30` ("the schema" below).

use core::time::Duration;
use std::collections::BTreeSet;

use fbc_core::{
    Cadence, Channel, ConfigError, ConfigScope, ConnTopology, Encoding, EndpointPlan, ExchTsKind,
    ExecCodec, ExecEndpoint, FeedSource, FieldSpec, FieldUnit, FundingCaps, LimitScope,
    MatchingCaps, MdCaps, MdCodec, MdTransport, OpKind, RateLimit, Readiness, SeqDomain, SpecTable,
    StpScope, StreamId, Subscription, TagSet, TouchSourceCaps, TradeCaps, VenueCaps, VenueConfig,
    VenueError, VenueFactory, WireUrl,
};

use crate::md::{self, ParadexMd, sbe};

/// The configuration key of the public WebSocket URL, without the SBE negotiation
/// parameters, which the adapter adds: `wss://ws.api.prod.paradex.trade/v1` on mainnet,
/// `wss://ws.api.testnet.paradex.trade/v1` on testnet (docs.paradex.trade, WebSocket
/// "Introduction").
pub const MD_URL: &str = "paradex.md.url";

/// The stream the one market-data connection is planned on.
pub const MD_STREAM: StreamId = StreamId(0);

const SCHEMA: &[FieldSpec] = &[FieldSpec {
    key: MD_URL,
    scope: ConfigScope::Account,
    unit: FieldUnit::Dimensionless,
    doc: "Public WebSocket URL (wss://), without query parameters; the adapter appends the SBE \
          negotiation (sbeSchemaId=1&sbeSchemaVersion=1).",
}];

/// The Paradex venue.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct ParadexFactory;

impl ParadexFactory {
    /// The public WebSocket URL from `cfg`, with the SBE negotiation parameters appended.
    pub fn md_url(cfg: &VenueConfig) -> Result<WireUrl, ConfigError> {
        let url = cfg.get(MD_URL).ok_or(ConfigError::Missing(MD_URL))?;
        let invalid = |reason| ConfigError::Invalid {
            key: MD_URL,
            reason,
        };
        if !url.starts_with("wss://") && !url.starts_with("ws://") {
            return Err(invalid("not a ws:// or wss:// URL"));
        }
        if url.contains(['?', '#']) {
            return Err(invalid(
                "the adapter writes the query; give the URL without one",
            ));
        }
        Ok(WireUrl::plain(format!(
            "{url}?sbeSchemaId={}&sbeSchemaVersion={}",
            sbe::SCHEMA_ID,
            sbe::SCHEMA_VERSION
        )))
    }
}

impl VenueFactory for ParadexFactory {
    fn id(&self) -> &'static str {
        "PARADEX"
    }

    fn config_schema(&self) -> &'static [FieldSpec] {
        SCHEMA
    }

    fn caps(&self, _cfg: &VenueConfig) -> Result<VenueCaps, ConfigError> {
        Ok(caps())
    }

    /// Every subscription on one connection: a binary frame names its channel by its template
    /// id and its `market` field, so bbo and trades frames need no connection of their own
    /// (see [`caps`], `topology`).
    fn plan_md(
        &self,
        cfg: &VenueConfig,
        specs: &SpecTable,
        subs: &BTreeSet<Subscription>,
    ) -> Result<Vec<EndpointPlan>, VenueError> {
        let url = ParadexFactory::md_url(cfg)?;
        for sub in subs {
            md::channel(*sub, specs)?;
        }
        if subs.is_empty() {
            return Ok(Vec::new());
        }
        Ok(vec![EndpointPlan {
            stream: MD_STREAM,
            transport: MdTransport::Socket { url },
            subs: subs.iter().copied().collect(),
        }])
    }

    fn md_codec(&self, _cfg: &VenueConfig, ep: &EndpointPlan) -> Box<dyn MdCodec> {
        Box::new(ParadexMd::new(ep.stream))
    }

    /// None until order entry lands (BT-402).
    fn plan_exec(&self, _cfg: &VenueConfig) -> Result<Vec<ExecEndpoint>, VenueError> {
        Ok(Vec::new())
    }

    /// None: market data only until order entry lands (BT-402).
    fn exec_codec(&self, _cfg: &VenueConfig) -> Option<Result<Box<dyn ExecCodec>, VenueError>> {
        None
    }
}

/// What Paradex market data offers through this adapter, each value cited.
pub fn caps() -> VenueCaps {
    let per = |secs, units| RateLimit {
        scope: LimitScope::Ip,
        ops: TagSet::of(&[OpKind::Connect]),
        per: Duration::from_secs(secs),
        units,
    };
    VenueCaps {
        // Order entry is BT-402's (decision 0015: no order or fill claims until then).
        exec: None,
        matching: MatchingCaps {
            // docs.paradex.trade describes no speed bump.
            speed_bump: None,
            // "Self Trade Prevention": "self-trading through the same account is not possible".
            stp_scope: StpScope::Account,
        },
        md: MdCaps {
            // "Binary Encoding (SBE)": public channel payloads are SBE since 2026-09-21.
            encoding: Encoding::Sbe,
            touch_sources: vec![TouchSourceCaps {
                channel: "bbo",
                // The schema's BboEvent: "Emitted on every best-price change".
                cadence: Cadence::Realtime,
                // The schema's TradeEvent.seq is "the same counter BboEvent.seq reports", the
                // orderbook sequence number (the bbo channel's seq_no, "Sequence number of the
                // orderbook").
                seq_domain: SeqDomain::SharedWithBook,
                // BboEvent has one timestamp, `ts`; the schema's TradeEvent calls the same
                // field the "Feed publish timestamp".
                ts_kind: ExchTsKind::Publish,
                // The schema carries prices including RPI only on the interactive order book
                // (BookEvent.bestBidPrice); bbo is the public book's.
                includes_channels: TagSet::of(&[Channel::Public]),
            }],
            // The order_book channel is decoded by FBC-70f; none is offered until then.
            books: Vec::new(),
            trades: TradeCaps {
                // The trades.{market} channel, TradeEvent (template 1).
                source: FeedSource::Stream,
                // TradeEvent.side is the "Aggressor side" (the channel's "Taker side").
                aggressor: true,
                // Not under schema 1:1: its int64 tradeId is deprecated as the truncated low 64
                // bits of the 28-digit trade id; the full id (tradeIdStr) is 1:2's.
                trade_id: false,
            },
            // markets_summary and funding_data are not decoded yet (FBC-9a4).
            funding: FundingCaps {
                source: FeedSource::None,
                interval_reported: false,
                next_time_reported: false,
            },
            stats: FeedSource::None,
            mark: FeedSource::None,
            index: FeedSource::None,
            // The schema: "Timestamps: all int64, microseconds since Unix epoch (UTC)".
            ts_precision: Duration::from_micros(1),
            // A frame names its message by template id and its instrument by `market`, but a
            // BookEvent does not say which order-book channel (snapshot, deltas, interactive)
            // it belongs to ("Binary Encoding (SBE)": "Frames do not explicitly identify their
            // channel"). bbo and trades share a connection; at most one book channel per market
            // and connection, enforced with the book in FBC-70f. docs.paradex.trade states no
            // cap on subscriptions per connection.
            topology: ConnTopology::Shared {
                max_subscriptions: None,
            },
            // The introduction states none: the server's 55-second ping keeps it open.
            max_conn_lifetime: None,
        },
        // "Websocket Rate Limits": "a maximum of 20 connections per second or 600 connections
        // per minute per IP address". No limit on subscribe frames is documented.
        limits: vec![per(1, 20), per(60, 600)],
        readiness_ceiling: Readiness::Record,
    }
}
