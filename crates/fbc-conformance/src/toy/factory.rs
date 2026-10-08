//! The toy's factory: what the named suite builds the toy from (`suite!`), as an adapter
//! crate's suite builds its venue. It declares [`caps`], discovers nothing (the tests state the
//! instruments, [`specs`]), reads Java-era tickers by a rule of its own, takes no credentials
//! (its signer holds no key) and builds a fresh [`ToyExec`] each time it is asked, pinging, and
//! resyncing over REST when [`REST_URL_KEY`] is configured (in frames otherwise).
//!
//! Its URLs come from the configuration with the credential spans it marks (`url.rs`, FBC-ja3):
//! the order-entry URL ([`EXEC_URL_KEY`]), the market-data URL ([`MD_URL_KEY`]), the anchors'
//! base ([`ANCHOR_URL_KEY`]) and the REST base ([`REST_URL_KEY`]). Its market-data codec is
//! [`ToyMd`], anchored under the configured base when there is one. Not modelled yet, and
//! refused rather than guessed: the feeds other than books, which arrive with FBC-z2s.
//!
//! [`specs`]: super::specs

use std::collections::BTreeSet;

use fbc_core::{
    AccountSummary, AssetKey, ConfigError, ConfigScope, EndpointPlan, ExecCodec, ExecEndpoint,
    Feed, FieldSpec, FieldUnit, HttpPlan, InstrumentKind, InstrumentSpecDraft, MdCodec,
    MdTransport, Secrets, SpecTable, StreamId, Subscription, SymbolError, VenueCaps, VenueConfig,
    VenueError, VenueFactory, common_symbol_parts,
};

use super::url::{UrlKey, configured};
use super::{
    ANCHOR_URL_KEY, ANCHOR_URL_REDACT_KEY, ANCHORED_BOOK, EXEC_STREAM, ToyExec, ToyMd, ToySigner,
    caps,
};

/// The configuration key of the toy's order-entry URL (`ws://` or `wss://`).
pub const EXEC_URL_KEY: &str = "toy.exec.url";
/// The configuration key of the credential spans in [`EXEC_URL_KEY`]'s URL.
pub const EXEC_URL_REDACT_KEY: &str = "toy.exec.url.redact";
/// The configuration key of the toy's market-data URL (`ws://` or `wss://`).
pub const MD_URL_KEY: &str = "toy.md.url";
/// The configuration key of the credential spans in [`MD_URL_KEY`]'s URL.
pub const MD_URL_REDACT_KEY: &str = "toy.md.url.redact";
/// The configuration key of the REST base a resync is asked for under (`http://` or
/// `https://`), at `<base>/resync?ts=<wall>`; without it the toy resyncs in frames.
pub const REST_URL_KEY: &str = "toy.exec.rest_url";
/// The configuration key of the credential spans in [`REST_URL_KEY`]'s URL.
pub const REST_URL_REDACT_KEY: &str = "toy.exec.rest_url.redact";

/// The stream of the toy's one market-data connection, every book channel on it.
pub const MD_STREAM: StreamId = StreamId(0);

const EXEC_URL: UrlKey = UrlKey {
    url: EXEC_URL_KEY,
    redact: EXEC_URL_REDACT_KEY,
    socket: true,
};
const MD_URL: UrlKey = UrlKey {
    url: MD_URL_KEY,
    redact: MD_URL_REDACT_KEY,
    socket: true,
};
const ANCHOR_URL: UrlKey = UrlKey {
    url: ANCHOR_URL_KEY,
    redact: ANCHOR_URL_REDACT_KEY,
    socket: false,
};
const REST_URL: UrlKey = UrlKey {
    url: REST_URL_KEY,
    redact: REST_URL_REDACT_KEY,
    socket: false,
};

/// An account-scoped key of no unit.
const fn field(key: &'static str, doc: &'static str) -> FieldSpec {
    FieldSpec {
        key,
        scope: ConfigScope::Account,
        unit: FieldUnit::Dimensionless,
        doc,
    }
}

/// A URL's key and its spans' key.
const fn url_fields(key: UrlKey, doc: &'static str) -> [FieldSpec; 2] {
    [
        field(key.url, doc),
        field(
            key.redact,
            "the credential spans in that URL: comma-separated start..end byte ranges, each in \
             its path; empty or absent marks none",
        ),
    ]
}

const SCHEMA: [[FieldSpec; 2]; 4] = [
    url_fields(
        EXEC_URL,
        "order-entry WebSocket URL (ws:// or wss://), without user information, query or \
         fragment",
    ),
    url_fields(
        MD_URL,
        "market-data WebSocket URL (ws:// or wss://), without user information, query or \
         fragment",
    ),
    url_fields(
        ANCHOR_URL,
        "the anchored book channel's REST base (http:// or https://), without user \
         information, query, fragment or trailing /; read when that channel is subscribed",
    ),
    url_fields(
        REST_URL,
        "the REST base a resync is asked for under (http:// or https://), without user \
         information, query, fragment or trailing /; without it the toy resyncs in frames",
    ),
];

/// The conformance toy's factory.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Default)]
pub struct ToyFactory;

impl VenueFactory for ToyFactory {
    fn id(&self) -> &'static str {
        "TOY-CONFORMANCE"
    }

    /// The four URLs and their credential spans.
    fn config_schema(&self) -> &'static [FieldSpec] {
        SCHEMA.as_flattened()
    }

    fn caps(&self, _cfg: &VenueConfig) -> Result<VenueCaps, ConfigError> {
        Ok(caps())
    }

    /// The toy's own rule, made up as the toy is, so `legacy_symbols` has something to read:
    /// `X/USDT` is the perpetual on `X` quoted in `USDT` (`TOYA/USDT` names `TOYA-PERP`); any
    /// other quote maps to no instrument, and a ticker not in `BASE/QUOTE` form is refused.
    fn parse_fbc_common_symbol(&self, s: &str) -> Result<AssetKey, SymbolError> {
        let (base, quote) = common_symbol_parts(s)?;
        if quote.as_str() != "USDT" {
            return Err(SymbolError::Unmapped);
        }
        let kind = InstrumentKind::Perpetual;
        Ok(AssetKey { base, quote, kind })
    }

    /// The tests state the toy's instruments.
    fn discover(
        &self,
        _cfg: &VenueConfig,
    ) -> Result<HttpPlan<Vec<InstrumentSpecDraft>>, VenueError> {
        Err(VenueError::NoDiscovery)
    }

    /// Nothing to plan for no subscription. An instrument missing from `specs` is refused as
    /// unknown before any feed is looked at (Codex r4203051296, r4216473740), then a feed the
    /// caps do not declare as unsupported. Every declared book channel goes on one connection,
    /// [`MD_STREAM`] at the configured market-data URL; the anchored channel needs the anchors'
    /// base configured too.
    fn plan_md(
        &self,
        cfg: &VenueConfig,
        specs: &SpecTable,
        subs: &BTreeSet<Subscription>,
    ) -> Result<Vec<EndpointPlan>, VenueError> {
        let books = caps().md.books.len();
        if let Some(sub) = subs.iter().find(|s| specs.get(s.inst).is_none()) {
            return Err(VenueError::UnknownInstrument(sub.inst));
        }
        let declared =
            |s: &Subscription| matches!(s.feed, Feed::Book(b) if usize::from(b.0) < books);
        if let Some(sub) = subs.iter().find(|s| !declared(s)) {
            return Err(VenueError::UnsupportedFeed(*sub));
        }
        if subs.is_empty() {
            return Ok(Vec::new());
        }
        let url = configured(cfg, MD_URL)?;
        if subs.iter().any(|s| s.feed == Feed::Book(ANCHORED_BOOK)) {
            configured(cfg, ANCHOR_URL)?;
        }
        Ok(vec![EndpointPlan {
            stream: MD_STREAM,
            transport: MdTransport::Socket { url },
            subs: subs.iter().copied().collect(),
        }])
    }

    /// A fresh [`ToyMd`] on the endpoint's stream, anchored under the configured base; with
    /// none, or one refused, the anchored channel is refused when subscribed.
    fn md_codec(&self, cfg: &VenueConfig, ep: &EndpointPlan) -> Box<dyn MdCodec> {
        Box::new(match configured(cfg, ANCHOR_URL) {
            Ok(base) => ToyMd::with_anchor_url(ep.stream, base),
            Err(_) => ToyMd::new(ep.stream),
        })
    }

    /// One connection, [`EXEC_STREAM`] at the configured order-entry URL.
    fn plan_exec(&self, cfg: &VenueConfig) -> Result<Vec<ExecEndpoint>, VenueError> {
        let url = configured(cfg, EXEC_URL)?;
        Ok(vec![ExecEndpoint {
            stream: EXEC_STREAM,
            url,
        }])
    }

    /// A fresh codec signing with [`ToySigner`], which holds no key: the credentials are
    /// dropped, and so zeroed, unread. It pings, and resyncs over REST under the configured
    /// base when there is one; one configured and refused refuses the codec.
    fn exec_codec(
        &self,
        cfg: &VenueConfig,
        _creds: Secrets,
    ) -> Option<Result<Box<dyn ExecCodec>, VenueError>> {
        let codec = ToyExec::new(Box::new(ToySigner)).pinging();
        let codec = match cfg.get(REST_URL_KEY) {
            None => Ok(codec),
            Some(_) => configured(cfg, REST_URL).map(|base| codec.rest_resync(base)),
        };
        Some(
            codec
                .map(|c| Box::new(c) as Box<dyn ExecCodec>)
                .map_err(VenueError::Config),
        )
    }

    /// None: the toy takes no credentials, so there is nothing to prove.
    fn test_connection(
        &self,
        _cfg: &VenueConfig,
        _creds: Secrets,
    ) -> Option<Result<HttpPlan<AccountSummary>, VenueError>> {
        None
    }
}
