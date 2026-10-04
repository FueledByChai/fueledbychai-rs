//! The Paradex venue factory: capabilities, configuration schema and market-data plan
//! (decisions 0003, 0015, 0016).
//!
//! Paradex is market data only here until order entry lands (BT-402): `exec` is `None`, and
//! there is no order-entry codec or endpoint. Each capability cites the document it comes
//! from: docs.paradex.trade, or the SBE schema `paradex_1_0.xml` in tradeparadex/paradex-py at
//! commit `b8248fb747e278d2167ac2f056b339a287d5ef30` ("the schema" below).

use core::time::Duration;
use std::collections::{BTreeMap, BTreeSet};

use fbc_core::{
    BookCaps, Cadence, Channel, ConfigError, ConfigScope, ConnTopology, Continuity, Encoding,
    EndpointPlan, ExchTsKind, ExecCodec, ExecEndpoint, Feed, FeedSource, FieldSpec, FieldUnit,
    FundingCaps, LimitScope, MatchingCaps, MdCaps, MdCodec, MdTransport, OpKind, QueueModelQuality,
    RateLimit, Readiness, SeqDomain, SpecTable, StpScope, StreamId, Subscription, TagSet,
    TouchSourceCaps, TradeCaps, VenueCaps, VenueConfig, VenueError, VenueFactory, WireUrl,
};

use crate::md::book::BOOK_CHANNELS;
use crate::md::{self, ParadexMd, sbe};

/// The configuration key of the public WebSocket URL, without the SBE negotiation
/// parameters, which the adapter adds: `wss://ws.api.prod.paradex.trade/v1` on mainnet,
/// `wss://ws.api.testnet.paradex.trade/v1` on testnet (docs.paradex.trade, WebSocket
/// "Introduction").
pub const MD_URL: &str = "paradex.md.url";

/// The stream of the first market-data connection; a market's second book channel goes on
/// `StreamId(1)`, and so on.
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

    /// As few connections as can carry `subs` with at most one book channel per market on
    /// each: a binary frame names its message by template id and its market by `market`, but
    /// not which book channel it is on (see [`caps`], `topology`). The first connection
    /// ([`MD_STREAM`]) carries every bbo and trades subscription and each market's first book
    /// channel; connection `k` carries each market's book channel number `k`, in `BookId`
    /// order.
    fn plan_md(
        &self,
        cfg: &VenueConfig,
        specs: &SpecTable,
        subs: &BTreeSet<Subscription>,
    ) -> Result<Vec<EndpointPlan>, VenueError> {
        let url = ParadexFactory::md_url(cfg)?;
        let mut conns: Vec<Vec<Subscription>> = Vec::new();
        let mut books_of: BTreeMap<_, usize> = BTreeMap::new();
        for sub in subs {
            md::channel(*sub, specs)?;
            let conn = match sub.feed {
                Feed::Book(_) => {
                    let books = books_of.entry(sub.inst).or_insert(0);
                    *books += 1;
                    *books - 1
                }
                _ => 0,
            };
            if conns.len() <= conn {
                conns.resize_with(conn + 1, Vec::new);
            }
            conns[conn].push(*sub);
        }
        // At most as many connections as declared book channels, so the index fits a u16.
        let plans = conns
            .into_iter()
            .zip(0u16..)
            .map(|(subs, stream)| EndpointPlan {
                stream: StreamId(stream),
                transport: MdTransport::Socket { url: url.clone() },
                subs,
            });
        Ok(plans.collect())
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

/// One `order_book` channel at depth 15 and the 50ms refresh rate (decision 0022).
fn book(channel: &'static str, includes: &[Channel]) -> BookCaps {
    BookCaps {
        channel,
        // The channel's "@15".
        max_depth: 15,
        // The channel's refresh rate: changes published at most every 50ms.
        cadence: Cadence::Capped(Duration::from_millis(50)),
        // Decision 0022: the schema's BookEvent.seq ("DELTA must be applied in order") advances
        // by one per frame, as FueledByChaiTrading's recorder (BookEpochSequencer) holds it.
        continuity: Continuity::PlusOne,
        // The venue keeps the top 15 itself with deltas; it sends no window bounds.
        windowed: false,
        // The channel starts with a snapshot (update type "s"); no REST anchor.
        rest_anchor: false,
        includes_channels: TagSet::of(includes),
        // Aggregated levels at a 50ms cadence: queue position only brackets.
        queue_model: QueueModelQuality::BracketOnly,
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
                // orderbook"); decision 0022 takes BookEvent.seq to be that counter too.
                seq_domain: SeqDomain::SharedWithBook,
                // BboEvent has one timestamp, `ts`; the schema's TradeEvent calls the same
                // field the "Feed publish timestamp".
                ts_kind: ExchTsKind::Publish,
                // The schema carries prices including RPI only on the interactive order book
                // (BookEvent.bestBidPrice); bbo is the public book's.
                includes_channels: TagSet::of(&[Channel::Public]),
            }],
            // docs.paradex.trade, "order_book.{market_symbol}.{feed_type}@15@{refresh_rate}":
            // feed types `deltas` and `interactive_deltas` at the 50ms refresh rate, both
            // carried as the schema's BookEvent (template 3).
            // The schema's BookEvent: the interactive feed's best prices are "including RPI",
            // so its levels show RPI liquidity too.
            books: vec![
                book(BOOK_CHANNELS[0], &[Channel::Public]),
                book(BOOK_CHANNELS[1], &[Channel::Public, Channel::Rpi]),
            ],
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
            // channel"). bbo, trades and one book channel per market share a connection; a
            // market's second book channel goes on another (decision 0022), as plan_md plans
            // and the codec's subscribe enforces. docs.paradex.trade states no cap on
            // subscriptions per connection.
            topology: ConnTopology::SharedOneBookPerInstrument {
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
