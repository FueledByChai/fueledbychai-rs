//! The Binance USD-M futures venue adapter: market data only.
//!
//! Binance USD-M is the reference price feed; this library never trades through it (decision
//! 0016), so its capabilities declare `exec: None` (decision 0015): [`BinanceUsdm`] builds no
//! order-entry codec and plans no order-entry connection. What it does:
//!
//! - [`caps`](VenueFactory::caps): every market-data value and rate limit, each citing
//!   Binance's USD-M documentation (decision 0003; the citations are in `caps.rs`).
//! - [`config_schema`](VenueFactory::config_schema): the WebSocket base URL and the partial-depth
//!   stream's levels and speed are configuration ([`KEY_WS_BASE_URL`], [`KEY_DEPTH_LEVELS`],
//!   [`KEY_DEPTH_SPEED`]), and so are the REST base URL and the diff-depth snapshot's limit,
//!   timeout and retry interval ([`KEY_REST_BASE_URL`], [`KEY_SNAPSHOT_LIMIT`],
//!   [`KEY_SNAPSHOT_TIMEOUT`], [`KEY_SNAPSHOT_RETRY`]).
//! - [`plan_md`](VenueFactory::plan_md): subscriptions on the combined-stream endpoint of the
//!   `/public` route, at most 1024 streams per connection, each instrument spelled as the
//!   [`SpecTable`](fbc_core::SpecTable) says.
//! - [`md_codec`](VenueFactory::md_codec): a sans-IO codec that sends live `SUBSCRIBE` and
//!   `UNSUBSCRIBE` requests with request ids and consumes their replies, decodes `bookTicker`
//!   into touches and each partial-depth message into a complete snapshot of its book channel,
//!   and keeps the diff-depth book ([`BOOK_DIFF`]) anchored on a REST snapshot as Binance
//!   documents, detecting gaps, duplicates and events out of order (`diff.rs`).
//!
//! Trades, funding, mark, index and statistics are not decoded and are declared
//! [`FeedSource::None`](fbc_core::FeedSource).

mod caps;
mod config;
mod diff;
mod md;

use std::collections::BTreeSet;

use fbc_core::{
    BookId, ConfigError, EndpointPlan, ExecCodec, ExecEndpoint, FieldSpec, MdCodec, MdTransport,
    SpecTable, StreamId, Subscription, TouchSourceId, VenueCaps, VenueConfig, VenueError,
    VenueFactory, WireUrl,
};

pub use caps::rest_depth_weight;
pub use config::{
    KEY_DEPTH_LEVELS, KEY_DEPTH_SPEED, KEY_REST_BASE_URL, KEY_SNAPSHOT_LIMIT, KEY_SNAPSHOT_RETRY,
    KEY_SNAPSHOT_TIMEOUT, KEY_WS_BASE_URL,
};

use crate::config::Settings;
use crate::md::{BinanceUsdmMd, stream_name};

/// The `bookTicker` channel: the one touch source ([`MdCaps::touch_sources`](fbc_core::MdCaps)).
pub const TOUCH_BOOK_TICKER: TouchSourceId = TouchSourceId(0);
/// The partial-depth channel (`depth<levels>@<speed>`), each message a snapshot of the top
/// levels: [`MdCaps::books`](fbc_core::MdCaps)`[0]`.
pub const BOOK_PARTIAL: BookId = BookId(0);
/// The diff-depth channel (`depth@100ms`), anchored on a `GET /fapi/v1/depth` snapshot:
/// [`MdCaps::books`](fbc_core::MdCaps)`[1]`.
pub const BOOK_DIFF: BookId = BookId(1);

/// More subscriptions than `u16::MAX + 1` endpoints of 1024 streams each carry.
const TOO_MANY_ENDPOINTS: ConfigError = ConfigError::Invalid {
    key: KEY_WS_BASE_URL,
    reason: "more endpoints than stream ids",
};

/// The Binance USD-M futures venue, market data only.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Default)]
pub struct BinanceUsdm;

impl VenueFactory for BinanceUsdm {
    /// As the Java library spells it (`Exchange.BINANCE_FUTURES`).
    fn id(&self) -> &'static str {
        "BINANCE_FUTURES"
    }

    fn config_schema(&self) -> &'static [FieldSpec] {
        config::SCHEMA
    }

    fn caps(&self, cfg: &VenueConfig) -> Result<VenueCaps, ConfigError> {
        Settings::read(cfg).map(|settings| caps::caps(&settings))
    }

    /// One endpoint per [`caps::MAX_STREAMS`] subscriptions, in the order of `subs`, every one
    /// on the `/public` combined-stream endpoint. Each subscription is checked before anything
    /// is planned: a feed this adapter does not decode, or an instrument missing from `specs`,
    /// is refused.
    fn plan_md(
        &self,
        cfg: &VenueConfig,
        specs: &SpecTable,
        subs: &BTreeSet<Subscription>,
    ) -> Result<Vec<EndpointPlan>, VenueError> {
        let settings = Settings::read(cfg)?;
        for sub in subs {
            stream_name(&settings, specs, *sub)?;
        }
        let subs: Vec<Subscription> = subs.iter().copied().collect();
        subs.chunks(caps::MAX_STREAMS)
            .enumerate()
            .map(|(n, chunk)| {
                let stream = u16::try_from(n).map_err(|_| TOO_MANY_ENDPOINTS)?;
                Ok(EndpointPlan {
                    stream: StreamId(stream),
                    transport: MdTransport::Socket {
                        url: WireUrl::plain(settings.endpoint()),
                    },
                    subs: chunk.to_vec(),
                })
            })
            .collect()
    }

    /// A codec for `ep`. A configuration [`plan_md`](VenueFactory::plan_md) would refuse gives a
    /// codec that refuses every subscription with the same error.
    fn md_codec(&self, cfg: &VenueConfig, ep: &EndpointPlan) -> Box<dyn MdCodec> {
        Box::new(BinanceUsdmMd::new(ep.stream, Settings::read(cfg)))
    }

    /// None: this library never trades on Binance (decisions 0015, 0016).
    fn plan_exec(&self, _cfg: &VenueConfig) -> Result<Vec<ExecEndpoint>, VenueError> {
        Ok(Vec::new())
    }

    /// None: market data only (decision 0015).
    fn exec_codec(&self, _cfg: &VenueConfig) -> Option<Result<Box<dyn ExecCodec>, VenueError>> {
        None
    }
}
