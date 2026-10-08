//! The Paradex venue factory: capabilities, configuration schema, market-data plan, and order
//! entry's endpoint, codec and Test Connection (decisions 0003, 0015, 0016, 0072).
//!
//! [`caps`] declares Paradex's market data with order entry: [`exec::exec_caps`], the order
//! rate limits, and the per-IP limit counting the order methods too (decision 0054).
//! [`market_data_caps`] is the same without order entry. Each capability cites the document it
//! comes from: docs.paradex.trade, or the SBE schema `paradex_1_0.xml` in tradeparadex/paradex-py
//! at commit `b8248fb747e278d2167ac2f056b339a287d5ef30` ("the schema" below).
//!
//! Order entry (decision 0072): [`plan_exec`](VenueFactory::plan_exec) plans one WebSocket
//! connection ([`EXEC_STREAM`]) at [`EXEC_URL`] with the SBE 1:2 negotiation;
//! [`exec_codec`](VenueFactory::exec_codec) builds, as [`EXEC_MODE`] says, the order-entry codec
//! ([`exec::ParadexExec`], each request awaiting its reply for [`RPC_TIMEOUT`]) or the read-only
//! one ([`exec::ReadOnlyExec`]), handing the consumer's `Secrets` to src/auth's functions and
//! reading none of them here. The snapshot source stays `Untrustworthy` (decision 0054): no
//! resync seeds a Paradex position, so every place and amend is refused until a market is
//! seeded by hand for the owner-assisted testnet run.

use core::time::Duration;
use std::collections::{BTreeMap, BTreeSet};

use fbc_core::{AccountSummary, Secrets};
use fbc_core::{
    AssetKey, BookCaps, Cadence, Channel, ConfigError, ConfigScope, ConnTopology, Continuity,
    Encoding, EndpointPlan, ExchTsKind, ExecCaps, ExecCodec, ExecEndpoint, Feed, FeedSource,
    FieldSpec, FieldUnit, FundingCaps, HttpPlan, InstrumentSpecDraft, LimitScope, MatchingCaps,
    MdCaps, MdCodec, MdTransport, OpKind, QueueModelQuality, RateLimit, Readiness, SeqDomain,
    SpecTable, StpScope, StreamId, Subscription, SymbolError, TagSet, TouchSourceCaps, TradeCaps,
    VenueCaps, VenueConfig, VenueError, VenueFactory, WireUrl,
};

use crate::auth;
use crate::exec::{self, ParadexExec, ReadOnlyExec};
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

/// The configuration key of the order-entry WebSocket URL, without the SBE negotiation
/// parameters, which the adapter adds: the same endpoint as market data,
/// `wss://ws.api.prod.paradex.trade/v1` on mainnet and `wss://ws.api.testnet.paradex.trade/v1`
/// on testnet (docs.paradex.trade, WebSocket "Introduction").
pub const EXEC_URL: &str = "paradex.exec.url";

/// The configuration key of what the order-entry session's codec may do: `orders` (place,
/// amend, cancel, with the private channels) or `read-only` (the private channels alone, every
/// command refused). Required: there is no default.
pub const EXEC_MODE: &str = "paradex.exec.mode";

/// The configuration key of how long an order request awaits its reply before it is `Unknown`
/// (`2500ms` or `3s`), in the `orders` mode.
pub const RPC_TIMEOUT: &str = "paradex.exec.rpc.timeout";

/// The stream of the one order-entry connection [`plan_exec`](VenueFactory::plan_exec) plans.
pub const EXEC_STREAM: StreamId = StreamId(0);

/// What the order-entry session's codec may do ([`EXEC_MODE`]).
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum ExecMode {
    /// `orders`: the order-entry codec ([`exec::ParadexExec`]).
    Orders,
    /// `read-only`: the read-only private-stream codec ([`exec::ReadOnlyExec`]).
    ReadOnly,
}

const SCHEMA: &[FieldSpec] = &[
    FieldSpec {
        key: MD_URL,
        scope: ConfigScope::Account,
        unit: FieldUnit::Dimensionless,
        doc: "Public WebSocket URL (wss://), without query parameters; the adapter appends the \
              SBE negotiation (sbeSchemaId=1&sbeSchemaVersion=1).",
    },
    FieldSpec {
        key: EXEC_URL,
        scope: ConfigScope::Account,
        unit: FieldUnit::Dimensionless,
        doc: "Order-entry WebSocket URL (wss://, or ws:// to a loopback test stub only), \
              without query parameters; the adapter appends the SBE negotiation \
              (sbeSchemaId=1&sbeSchemaVersion=2).",
    },
    FieldSpec {
        key: EXEC_MODE,
        scope: ConfigScope::Account,
        unit: FieldUnit::Dimensionless,
        doc: "What the order-entry session may do: orders (place, amend and cancel, with the \
              private channels) or read-only (the private channels alone); no default",
    },
    FieldSpec {
        key: RPC_TIMEOUT,
        scope: ConfigScope::Account,
        unit: FieldUnit::Duration,
        doc: "how long an order request awaits its reply before it is Unknown (e.g. 2500ms or \
              3s); read in the orders mode",
    },
    auth::REST_URL_FIELD,
    auth::CHAIN_ID_FIELD,
    auth::SIGNATURE_LIFETIME_FIELD,
    auth::REFRESH_FIELD,
    auth::TIMEOUT_FIELD,
    auth::ACCOUNT_ADDRESS_FIELD,
    auth::SIGNING_KEY_FIELD,
];

/// The Paradex venue.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct ParadexFactory;

impl ParadexFactory {
    /// The public WebSocket URL from `cfg`, with the SBE negotiation parameters appended.
    pub fn md_url(cfg: &VenueConfig) -> Result<WireUrl, ConfigError> {
        socket_url(cfg, MD_URL, sbe::SCHEMA_VERSION)
    }

    /// The order-entry WebSocket URL from `cfg`, with the SBE 1:2 negotiation appended
    /// ([`exec::ORDER_SBE_SCHEMA_VERSION`], decision 0054). Plain `ws://` only to a loopback
    /// test stub: the socket carries the session token and every signed order, as src/auth
    /// refuses plain `http://` for the REST base (the owner's review).
    pub fn exec_url(cfg: &VenueConfig) -> Result<WireUrl, ConfigError> {
        let url = socket_url(cfg, EXEC_URL, exec::ORDER_SBE_SCHEMA_VERSION)?;
        if let Some(after) = cfg
            .get(EXEC_URL)
            .and_then(|text| text.strip_prefix("ws://"))
        {
            let authority = &after[..after.find('/').unwrap_or(after.len())];
            if !auth::is_loopback_authority(authority) {
                return Err(ConfigError::Invalid {
                    key: EXEC_URL,
                    reason: "ws:// is for a loopback test stub only (127.0.0.0/8, ::1 or \
                             localhost): give a wss:// URL",
                });
            }
        }
        Ok(url)
    }

    /// What the order-entry session's codec may do, from `cfg` ([`EXEC_MODE`]).
    pub fn exec_mode(cfg: &VenueConfig) -> Result<ExecMode, ConfigError> {
        match cfg.get(EXEC_MODE).ok_or(ConfigError::Missing(EXEC_MODE))? {
            "orders" => Ok(ExecMode::Orders),
            "read-only" => Ok(ExecMode::ReadOnly),
            _ => Err(ConfigError::Invalid {
                key: EXEC_MODE,
                reason: "not orders or read-only",
            }),
        }
    }

    /// How long an order request awaits its reply, from `cfg` ([`RPC_TIMEOUT`]).
    pub fn rpc_timeout(cfg: &VenueConfig) -> Result<Duration, ConfigError> {
        let text = cfg
            .get(RPC_TIMEOUT)
            .ok_or(ConfigError::Missing(RPC_TIMEOUT))?;
        duration(text).ok_or(ConfigError::Invalid {
            key: RPC_TIMEOUT,
            reason: "not a positive whole <n>s or <n>ms",
        })
    }
}

/// The WebSocket URL under `key` in `cfg`, with the SBE negotiation for schema version
/// `version` appended.
fn socket_url(cfg: &VenueConfig, key: &'static str, version: u16) -> Result<WireUrl, ConfigError> {
    let url = cfg.get(key).ok_or(ConfigError::Missing(key))?;
    let invalid = |reason| ConfigError::Invalid { key, reason };
    if !url.starts_with("wss://") && !url.starts_with("ws://") {
        return Err(invalid("not a ws:// or wss:// URL"));
    }
    if url.contains(['?', '#']) {
        return Err(invalid(
            "the adapter writes the query; give the URL without one",
        ));
    }
    Ok(WireUrl::plain(format!(
        "{url}?sbeSchemaId={}&sbeSchemaVersion={version}",
        sbe::SCHEMA_ID,
    )))
}

/// A positive, whole number of seconds (`<n>s`) or milliseconds (`<n>ms`).
fn duration(text: &str) -> Option<Duration> {
    let (digits, unit): (&str, fn(u64) -> Duration) = match text.strip_suffix("ms") {
        Some(digits) => (digits, Duration::from_millis),
        None => (text.strip_suffix('s')?, Duration::from_secs),
    };
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let n: u64 = digits.parse().ok()?;
    (n > 0).then(|| unit(n))
}

/// The codec [`EXEC_MODE`] names for the account `creds` hold. The credentials go to src/auth's
/// functions only: the order signer reads them ([`auth::order_signer`]), then the login takes
/// them (`Login::new`, inside the codec).
fn exec_codec(cfg: &VenueConfig, creds: Secrets) -> Result<Box<dyn ExecCodec>, VenueError> {
    match ParadexFactory::exec_mode(cfg)? {
        ExecMode::ReadOnly => Ok(Box::new(ReadOnlyExec::new(cfg, creds)?)),
        ExecMode::Orders => {
            let rpc_timeout = ParadexFactory::rpc_timeout(cfg)?;
            let signer = Box::new(auth::order_signer(cfg, &creds)?);
            let codec = ParadexExec::new(cfg, creds, signer, EXEC_STREAM, rpc_timeout)?;
            Ok(Box::new(codec))
        }
    }
}

impl VenueFactory for ParadexFactory {
    fn id(&self) -> &'static str {
        "PARADEX"
    }

    fn config_schema(&self) -> &'static [FieldSpec] {
        SCHEMA
    }

    /// [`caps`]: market data and order entry, whatever the mode.
    fn caps(&self, _cfg: &VenueConfig) -> Result<VenueCaps, ConfigError> {
        Ok(caps())
    }

    /// None yet: Paradex's FBC rule (`X/USDT` is `X-USD-PERP`) is FBC-l5o.
    fn parse_fbc_common_symbol(&self, _s: &str) -> Result<AssetKey, SymbolError> {
        Err(SymbolError::NoRule)
    }

    /// None yet: discovery from `GET /markets` is FBC-l5o.
    fn discover(
        &self,
        _cfg: &VenueConfig,
    ) -> Result<HttpPlan<Vec<InstrumentSpecDraft>>, VenueError> {
        Err(VenueError::NoDiscovery)
    }

    /// Connections that carry `subs` with at most one book channel and one touch source per
    /// market on each: a binary frame names its message by template id and its market by
    /// `market`, but not which book or bbo channel it is on (see [`caps`], `topology`;
    /// decision 0076). The first connection ([`MD_STREAM`]) carries every trades, mark and
    /// funding subscription and each market's first book channel; connection `k` carries each
    /// market's book channel number `k`, in `BookId` order. A touch source goes on the
    /// connection of its own index, whatever else is subscribed: `bbo` on the first,
    /// `bbo.{market}.interactive` on the second, so a change of the desired set never moves
    /// a touch source to a connection whose codec holds the market's other one (Codex
    /// r4214305885). A connection with nothing to carry is not planned.
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
                Feed::Touch(source) => usize::from(source.0),
                _ => 0,
            };
            if conns.len() <= conn {
                conns.resize_with(conn + 1, Vec::new);
            }
            conns[conn].push(*sub);
        }
        // At most as many connections as declared book channels or touch sources, so the index
        // fits a u16.
        let plans = conns
            .into_iter()
            .zip(0u16..)
            .filter(|(subs, _)| !subs.is_empty())
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

    /// One connection, [`EXEC_STREAM`] at [`ParadexFactory::exec_url`]: order entry and the
    /// private channels share it (decision 0071), in either mode.
    fn plan_exec(&self, cfg: &VenueConfig) -> Result<Vec<ExecEndpoint>, VenueError> {
        let url = ParadexFactory::exec_url(cfg)?;
        Ok(vec![ExecEndpoint {
            stream: EXEC_STREAM,
            url,
        }])
    }

    /// The order-entry codec, or the read-only one, as [`EXEC_MODE`] says; refused naming a
    /// missing or invalid key, never a value. A refusal drops the credentials, and so zeroes
    /// them.
    fn exec_codec(
        &self,
        cfg: &VenueConfig,
        creds: Secrets,
    ) -> Option<Result<Box<dyn ExecCodec>, VenueError>> {
        Some(exec_codec(cfg, creds))
    }

    /// The login, then the account read with the token it gave ([`auth::connection_plan`],
    /// decision 0048).
    fn test_connection(
        &self,
        cfg: &VenueConfig,
        creds: Secrets,
    ) -> Option<Result<HttpPlan<AccountSummary>, VenueError>> {
        Some(auth::connection_plan(cfg, creds))
    }
}

/// One bbo channel: BboEvent (template 2) on every best-price change. `bbo.{market}` is the
/// public book's touch; `bbo.{market}.interactive` carries the same template, including RPI
/// orders (decision 0076).
fn touch(channel: &'static str, includes: &[Channel]) -> TouchSourceCaps {
    TouchSourceCaps {
        channel,
        // The schema's BboEvent: "Emitted on every best-price change".
        cadence: Cadence::Realtime,
        // The schema's TradeEvent.seq is "the same counter BboEvent.seq reports", the
        // orderbook sequence number (the bbo channel's seq_no, "Sequence number of the
        // orderbook"); decision 0022 takes BookEvent.seq to be that counter too. The interactive
        // touch's is the same BboEvent.seq: the 2026-10-08 captures of the two run on in one
        // series (decision 0076).
        seq_domain: SeqDomain::SharedWithBook,
        // BboEvent has one timestamp, `ts`; the schema's TradeEvent calls the same field the
        // "Feed publish timestamp".
        ts_kind: ExchTsKind::Publish,
        // The schema carries prices including RPI on the interactive order book
        // (BookEvent.bestBidPrice); bbo is the public book's, and its interactive twin shows
        // what the interactive book does.
        includes_channels: TagSet::of(includes),
    }
}

/// One `order_book` channel: the whole book on every change (decision 0074).
fn book(channel: &'static str, includes: &[Channel]) -> BookCaps {
    BookCaps {
        channel,
        // No depth: the bare channel's snapshot is the whole book (the 2026-10-08 captures:
        // 115 bids and 60 asks, 120 and 64), and its deltas keep it whole.
        max_depth: u16::MAX,
        // A frame per change of the orderbook sequence: consecutive seq_nos, frames under a
        // millisecond apart in the captures (no refresh-rate parameter is accepted).
        cadence: Cadence::Realtime,
        // Decision 0022: the schema's BookEvent.seq ("DELTA must be applied in order") advances
        // by one per frame, as FueledByChaiTrading's recorder (BookEpochSequencer) holds it,
        // and as the captures show (decision 0074).
        continuity: Continuity::PlusOne,
        // The whole book: no window, and no window bounds sent.
        windowed: false,
        // The channel starts with a snapshot (update type "s"); no REST anchor.
        rest_anchor: false,
        includes_channels: TagSet::of(includes),
        // Aggregated price levels: queue position only brackets.
        queue_model: QueueModelQuality::BracketOnly,
    }
}

/// What [`ParadexFactory`] declares, each value cited: [`market_data_caps`]'s market data and
/// matching, with [`exec::exec_caps`], the per-account order limits ([`exec::order_limits`]),
/// and the per-IP limit counting the order methods too (decision 0054).
pub fn caps() -> VenueCaps {
    venue_caps(Some(exec::exec_caps()))
}

/// What Paradex market data offers through this adapter, without order entry (`exec: None`):
/// what [`ParadexFactory`] declared before order entry was wired.
pub fn market_data_caps() -> VenueCaps {
    venue_caps(None)
}

/// Paradex's caps with `exec` as given; the order limits come with order entry.
fn venue_caps(exec: Option<ExecCaps>) -> VenueCaps {
    let per = |secs, units| RateLimit {
        scope: LimitScope::Ip,
        ops: TagSet::of(&[OpKind::Connect]),
        per: Duration::from_secs(secs),
        units,
    };
    let limit = |scope, ops: &[OpKind], secs, units| RateLimit {
        scope,
        ops: TagSet::of(ops),
        per: Duration::from_secs(secs),
        units,
    };
    // Decision 0054: whether Paradex's per-IP limit counts the WebSocket order methods is
    // undocumented; with order entry they are counted, in the one bucket REST and queries share.
    let mut ip_ops = vec![OpKind::Rest, OpKind::Query];
    let mut order_limits = Vec::new();
    if exec.is_some() {
        ip_ops.extend(exec::ORDER_OPS);
        order_limits.extend(exec::order_limits());
    }
    let mut limits = vec![
        // "Websocket Rate Limits": "a maximum of 20 connections per second or 600 connections
        // per minute per IP address". No limit on subscribe frames is documented.
        per(1, 20),
        per(60, 600),
        // "API Rate Limits": "POST /auth | 600 req/m | IP address". The login (auth.rs)
        // is this adapter's only `Rest` request.
        limit(LimitScope::Ip, &[OpKind::Rest], 60, 600),
        // "GET /* | 120 req/s OR 600 req/m | Account": the account read (auth.rs) and
        // every later private GET charge `Query`. Both windows are declared.
        limit(LimitScope::Account, &[OpKind::Query], 1, 120),
        limit(LimitScope::Account, &[OpKind::Query], 60, 600),
        // Public requests default to 1500 req/m per IP, and private ones are "also
        // subject to an additional IP-based rate limit of 1500 req/m across all accounts
        // from the same IP address".
        limit(LimitScope::Ip, &ip_ops, 60, 1500),
    ];
    limits.extend(order_limits);
    VenueCaps {
        // None for market data only (decision 0015: no order or fill claims without order
        // entry); Paradex's order entry otherwise (decision 0054).
        exec,
        matching: MatchingCaps {
            // docs.paradex.trade describes no speed bump.
            speed_bump: None,
            // "Self Trade Prevention": "self-trading through the same account is not possible".
            stp_scope: StpScope::Account,
        },
        md: MdCaps {
            // "Binary Encoding (SBE)": public channel payloads are SBE since 2026-09-21.
            encoding: Encoding::Sbe,
            // Indexed by TouchSourceId: md::BBO, then md::BBO_INTERACTIVE.
            touch_sources: vec![
                touch("bbo", &[Channel::Public]),
                // `bbo.{market}.interactive`, exactly so spelled (decision 0076): the venue
                // acknowledges it and streams BboEvent (template 2) on it, the touch including
                // RPI orders, as the interactive order book shows them; its REST twin is
                // `GET /v1/bbo/{market}/interactive`, not used here.
                touch("bbo.interactive", &[Channel::Public, Channel::Rpi]),
            ],
            // `order_book.{market_symbol}.{feed_type}` with feed types `deltas` and
            // `interactive_deltas`, both carried as the schema's BookEvent (template 3). The
            // `@15@{refresh_rate}` suffix docs.paradex.trade spells is refused on the SBE socket
            // (decision 0074).
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
            // The markets_summary.{market} channel, MarketSummaryEvent (template 4): its
            // fundingRate (fundingRatePrecise from schema version 1). It states neither the
            // funding interval nor the next funding time. funding_data (FundingDataEvent, the
            // per-period settlement record) is not decoded.
            funding: FundingCaps {
                source: FeedSource::Stream,
                interval_reported: false,
                next_time_reported: false,
            },
            // MarketSummaryEvent carries volume and open interest, and the index price, but this
            // adapter does not decode them yet.
            stats: FeedSource::None,
            // The markets_summary.{market} channel, MarketSummaryEvent.markPrice.
            mark: FeedSource::Stream,
            index: FeedSource::None,
            // The schema: "Timestamps: all int64, microseconds since Unix epoch (UTC)".
            ts_precision: Duration::from_micros(1),
            // A frame names its message by template id and its instrument by `market`, but a
            // BookEvent does not say which order-book channel (snapshot, deltas, interactive)
            // it belongs to ("Binary Encoding (SBE)": "Frames do not explicitly identify their
            // channel"), nor does a BboEvent say whether it is bbo or bbo's interactive twin.
            // Trades and one book channel and one touch source per market share a connection;
            // a market's second book channel (decision 0022) or second touch source (decision
            // 0076) goes on another, as plan_md plans and the codec's subscribe enforces. The
            // venue refuses a second book channel too: a market's second order_book channel
            // "cannot share an SBE session" (decision 0074).
            // docs.paradex.trade states no cap on subscriptions per connection.
            topology: ConnTopology::SharedOneBookPerInstrument {
                max_subscriptions: None,
            },
            // The introduction states none: the server's 55-second ping keeps it open.
            max_conn_lifetime: None,
        },
        limits,
        readiness_ceiling: Readiness::Record,
    }
}
