//! The Hibachi venue adapter: so far instrument discovery and market data, with no order entry.
//!
//! Its capabilities declare `exec: None` (decision 0015) and
//! [`Readiness::Record`](fbc_core::Readiness): [`Hibachi`] builds no order-entry codec, plans no
//! order-entry connection and takes no credential. Order entry waits for its own decision record
//! (BT-604). What it does:
//!
//! - [`caps`](VenueFactory::caps): the matching, market-data and rate-limit values, each citing
//!   Hibachi's documentation, its SDK or the Java library (decision 0003; the citations are in
//!   `caps.rs`).
//! - [`config_schema`](VenueFactory::config_schema): the market-data REST origin and its timeout
//!   ([`KEY_DATA_REST_URL`], [`KEY_REST_TIMEOUT`]), the orderbook subscriptions' depth
//!   ([`KEY_ORDERBOOK_DEPTH`], 1 to 100), the market-data WebSocket origin
//!   ([`KEY_DATA_WS_URL`]) and how long the codec waits before resubscribing a broken orderbook
//!   ([`KEY_ORDERBOOK_RESUBSCRIBE`]), each required with no default.
//! - [`discover`](VenueFactory::discover): one `GET /market/exchange-info`, read into an
//!   instrument draft per contract, and
//!   [`parse_fbc_common_symbol`](VenueFactory::parse_fbc_common_symbol), FBC's rule that
//!   `X/USDT` is `X/USDT-P` ([`discover`](crate::discover) says what is read and how).
//! - [`plan_md`](VenueFactory::plan_md): one connection to `/ws/market` on the WebSocket origin
//!   carrying every subscription, and [`md_codec`](VenueFactory::md_codec): its codec,
//!   [`HibachiMd`], which decodes the `orderbook` ([`BOOK_ORDERBOOK`]), `ask_bid_price`
//!   ([`TOUCH_ASK_BID_PRICE`]), `trades`, `mark_price`, `spot_price` (as the index) and
//!   `funding_rate_estimation` topics. The orderbook has no sequence number, so the codec checks
//!   every frame against the bounds the venue states on its own copy of the book before pushing
//!   any of its events, and recovers a book it finds broken from a fresh Snapshot, resubscribing
//!   on the same connection (decision 0129; the `md` module says what each topic becomes).
//!
//! # The quote grid, and what the book's levels are
//!
//! Hibachi accepts orders on a contract's `tickSize`, often absurdly fine (a tick of 0.0000001
//! on a price near 100), and aggregates its public orderbook at a granularity a subscription
//! chooses among the contract's `orderbookGranularities` (the SDK's `get_orderbook`: levels
//! "aggregated based on the specified granularity"). A draft's `price_grid` is the tick; its
//! `quote_grid` is, as `fbc-core` defines it, the coarser grid the book is published on: the
//! finest listed granularity at or above the tick, exactly as listed, where that is coarser than
//! the tick (the Java library's `HibachiRestApi.resolveEffectiveTickSize`), and `None` where the
//! book can be on the acceptance grid itself. The orderbook is subscribed at
//! `quote_grid.unwrap_or(tick)`, so where the quote grid is coarser than the tick its levels are
//! buckets of the subscription's granularity while orders rest on the finer tickSize: a level's
//! price is a bucket's label rather than a price an order rests at, its size is the bucket's sum,
//! and the touch can sit up to one granularity from the venue's own. No capability says so yet
//! (FBC-jwn9 adds one); meanwhile the orderbook channel declares `max_depth` the configured
//! depth, `windowed` and `QueueModelQuality::BracketOnly`.
//!
//! `quote_grid` is `None` in two cases, both quoted on the tick: a contract that lists its tick
//! among its granularities, and a contract with no granularity at or above its tick (none listed,
//! none well formed, or all finer), which `resolveEffectiveTickSize` also falls back to the tick
//! for. The second is not refused, since refusing it would refuse the whole discovery answer
//! (decision 0035). Its orderbook subscription then names a granularity Hibachi does not list, so
//! the codec counts a level whose price is off that granularity and gaps that book, as it does a
//! frame failing its bounds check, never trusting the venue's default; every subscription's
//! levels are held to its granularity the same way.
//!
//! # Trading status
//!
//! Only a `LIVE` contract on an exchange whose status is `NORMAL` is
//! [`TradingStatus::Trading`](fbc_core::TradingStatus); `LISTED`, `PRE_OPEN`, `PAUSED` and
//! `CLOSED` are each `Halted`, and every contract is `Halted` while the exchange's status is
//! anything else (scheduled or unscheduled maintenance). A contract status that is absent, null
//! or not one of those five refuses the answer. Neither an advertised open nor a session
//! calendar ever makes a contract trading. Discovery's status is a snapshot of one answer; the
//! re-read that reports each change (an FX pair `PAUSED` each weekend, an exchange entering
//! maintenance) is FBC-1632's, before any Hibachi order path quotes. Of this library only
//! `fbc-oms`'s execution planner reads a market's status, from what the consumer passes it
//! (decision 0111).
//!
//! # Sessions
//!
//! An FX contract (an underlying [`discover::FX_UNDERLYINGS`] lists) closes every week from
//! Friday 17:00 to Sunday 17:00 New York time, a rule stated as data (decision 0125); a
//! contract's advertised `marketOpenTimestamp` and `marketCloseTimestamp` are its calendar's first
//! open and final close. ETH, like every crypto contract with neither, has no calendar. The
//! published maintenance windows are not in any calendar yet (decision 0125's Consequences
//! expect them as `ClosedWindow`s; FBC-ord5 maps them), so `is_open` does not see them; FBC-1632's
//! re-read halts each market at a window's begin.
//!
//! # Funding
//!
//! Hibachi settles funding every hour: [`FUNDING_SETTLEMENT_INTERVAL`]. docs.hibachi.xyz's
//! Funding page (<https://docs.hibachi.xyz/hibachi-docs/trading/funding>, Formulas) defines the
//! rate of each hourly settlement as `Funding Rate = (1/8) × mean(ρ_t)` over the trailing hour's
//! premium samples and its payment as `Funding Payment = Q × Funding Rate × S_settlement`: the
//! rate it defines is the hour's, paid unscaled. No public source shows that the rates Hibachi
//! publishes, the settled history's `fundingRate` and the `funding_rate_estimation` topic's
//! `estimatedFundingRate`, are that rate, and the Java library at b4188a5 reads them two ways:
//! its quote engine divides the estimate by the instrument descriptor's interval, eight hours by
//! default (`HibachiQuoteEngine.java` L904-L918, linked from `caps.rs`; `HibachiRestApi.java`
//! L388), while `FundingSources.java` annualizes each settled rate over the hour it derives
//! (L139-L158 and L173, linked from [`FUNDING_SETTLEMENT_INTERVAL`]). So each perpetual's
//! funding is [`FundingSpec::Unknown`](fbc_core::FundingSpec) until the owner's evidence settles
//! it (FBC-204c).
//!
//! Hibachi's market-maker-partner book topic is not planned, decoded or named here: it stays out
//! of this public repository until its entitlement is clear.

mod caps;
mod config;
pub mod discover;
mod md;

use core::time::Duration;
use std::collections::BTreeSet;

use fbc_core::{
    AccountSummary, AssetKey, BookId, ConfigError, EndpointPlan, ExecCodec, ExecEndpoint,
    FieldSpec, HttpPlan, InstrumentSpecDraft, MdCodec, MdTransport, Secrets, SpecTable, StreamId,
    Subscription, SymbolError, TouchSourceId, VenueCaps, VenueConfig, VenueError, VenueFactory,
    WireUrl,
};

pub use config::{
    KEY_DATA_REST_URL, KEY_DATA_WS_URL, KEY_ORDERBOOK_DEPTH, KEY_ORDERBOOK_RESUBSCRIBE,
    KEY_REST_TIMEOUT,
};
pub use md::{HibachiMd, HibachiMdCounts};

use crate::config::Settings;

/// The `orderbook` channel: [`MdCaps::books`](fbc_core::MdCaps)`[0]`.
pub const BOOK_ORDERBOOK: BookId = BookId(0);

/// The `ask_bid_price` channel: [`MdCaps::touch_sources`](fbc_core::MdCaps)`[0]`.
pub const TOUCH_ASK_BID_PRICE: TouchSourceId = TouchSourceId(0);

/// The one market-data connection's stream.
const MD_STREAM: StreamId = StreamId(0);

/// The interval between Hibachi's funding settlements: one hour.
///
/// Hibachi's settled funding history, `GET /market/data/funding-rates?symbol=ETH%2FUSDT-P` on the
/// market-data host (documented on <https://api-doc.hibachi.xyz/>, Market API, but not in the
/// published Market API spec), read by the owner on 2026-10-22, lists consecutive settlements,
/// each `fundingTimestamp` (epoch seconds) on the UTC hour and 3600 s after the one before; the
/// Java library derives an hourly interval from the same spacing (`market-data-collector`
/// `FundingSources.java` L139-L158,
/// <https://github.com/FueledByChai/FueledByChaiTrading/blob/b4188a57f7d5ea4e82260c9f3c4a0a359ad04c27/market-data-collector/src/main/java/com/fueledbychai/datacollector/FundingSources.java#L139-L158>),
/// and docs.hibachi.xyz's Funding page says "funding payments are made every hour"
/// (<https://docs.hibachi.xyz/hibachi-docs/trading/funding>).
///
/// Contradicted: the redocly trading page says "every 8 hours—at midnight UTC, 8:00 AM UTC, and
/// 4:00 PM UTC" (<https://hibachi-docs.redocly.app/docs/trading>, Funding), as the Java library's
/// instrument descriptor default of 8 hours (`HibachiRestApi.java` L388) does; the settled
/// history and the Funding page both show hourly settlements.
pub const FUNDING_SETTLEMENT_INTERVAL: Duration = Duration::from_secs(3_600);

/// The Hibachi venue: discovery and market-data capabilities, no order entry.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Default)]
pub struct Hibachi;

impl VenueFactory for Hibachi {
    /// As the Java library spells it (`Exchange.HIBACHI`).
    fn id(&self) -> &'static str {
        "HIBACHI"
    }

    fn config_schema(&self) -> &'static [FieldSpec] {
        config::SCHEMA
    }

    /// Every key is read and checked; the orderbook channel's depth is the configured one.
    fn caps(&self, cfg: &VenueConfig) -> Result<VenueCaps, ConfigError> {
        Settings::read(cfg).map(|settings| caps::caps(&settings))
    }

    /// FBC's Java-era rule: `X/USDT` is `X/USDT-P` ([`discover`]).
    fn parse_fbc_common_symbol(&self, s: &str) -> Result<AssetKey, SymbolError> {
        discover::parse_fbc_common_symbol(s)
    }

    /// One `GET /market/exchange-info` on the market-data REST origin, read into a draft per
    /// contract ([`discover`]). Only the REST origin and timeout are read.
    fn discover(
        &self,
        cfg: &VenueConfig,
    ) -> Result<HttpPlan<Vec<InstrumentSpecDraft>>, VenueError> {
        discover::plan(cfg)
    }

    /// One endpoint, `/ws/market` on the WebSocket origin, carrying every subscription: the
    /// topics share one connection with no stated cap (Hibachi's WebSocket market docs, cited in
    /// `caps.rs`). Each subscription is checked first: a feed the codec does not decode, or an
    /// instrument missing from `specs`, is refused. An empty set plans nothing.
    fn plan_md(
        &self,
        cfg: &VenueConfig,
        specs: &SpecTable,
        subs: &BTreeSet<Subscription>,
    ) -> Result<Vec<EndpointPlan>, VenueError> {
        let settings = Settings::read(cfg)?;
        for sub in subs {
            md::plannable(&settings, specs, *sub)?;
        }
        if subs.is_empty() {
            return Ok(Vec::new());
        }
        Ok(vec![EndpointPlan {
            stream: MD_STREAM,
            transport: MdTransport::Socket {
                url: WireUrl::plain(settings.ws_market),
            },
            subs: subs.iter().copied().collect(),
        }])
    }

    /// A [`HibachiMd`] for `ep`. A configuration [`plan_md`](VenueFactory::plan_md) would refuse
    /// gives a codec that refuses every subscription with the same error.
    fn md_codec(&self, cfg: &VenueConfig, ep: &EndpointPlan) -> Box<dyn MdCodec> {
        Box::new(HibachiMd::new(ep.stream, cfg))
    }

    /// None: no order entry (decision 0015).
    fn plan_exec(&self, _cfg: &VenueConfig) -> Result<Vec<ExecEndpoint>, VenueError> {
        Ok(Vec::new())
    }

    /// None: market data only (decision 0015); the credentials are dropped, and so zeroed,
    /// unread.
    fn exec_codec(
        &self,
        _cfg: &VenueConfig,
        _creds: Secrets,
    ) -> Option<Result<Box<dyn ExecCodec>, VenueError>> {
        None
    }

    /// None: market data only, it takes no credentials.
    fn test_connection(
        &self,
        _cfg: &VenueConfig,
        _creds: Secrets,
    ) -> Option<Result<HttpPlan<AccountSummary>, VenueError>> {
        None
    }
}
