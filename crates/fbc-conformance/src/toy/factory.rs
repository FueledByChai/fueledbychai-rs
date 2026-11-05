//! The toy's factory: what the named suite builds the toy from (`suite!`), as an adapter
//! crate's suite builds its venue. It declares [`caps`], discovers nothing (the tests state the
//! instruments, [`specs`]), reads Java-era tickers by a rule of its own, takes no credentials
//! (its signer holds no key) and builds a fresh [`ToyExec`] each time it is asked, pinging, and
//! resyncing over REST when [`REST_URL_KEY`] is configured (in frames otherwise).
//!
//! Its URLs come from the configuration with the credential spans it marks (`url.rs`, FBC-ja3):
//! the order-entry URL ([`EXEC_URL_KEY`]), the market-data URL ([`MD_URL_KEY`]), the anchors'
//! base ([`ANCHOR_URL_KEY`]), the REST base ([`REST_URL_KEY`]) and the stats base
//! ([`STATS_URL_KEY`]). It plans each market-data feed on the endpoint its caps' source names
//! (FBC-z2s; Codex r4172917344, r4172649862): the streamed ones (books, trades, mark, index and
//! funding) on the socket at the market-data URL, its codec [`ToyMd`], anchored under the
//! configured base when there is one; stats on a poll endpoint under the stats base, which opens
//! no connection (record 0014 item 9, decision 0027), its codec [`ToyStats`]. A feed its caps do
//! not declare (a touch channel, a book id past its two) is refused rather than guessed.
//!
//! [`specs`]: super::specs

use std::collections::BTreeSet;

use fbc_core::{
    AccountSummary, AssetKey, ConfigError, ConfigScope, EndpointPlan, ExecCodec, ExecEndpoint,
    Feed, FeedSource, FieldSpec, FieldUnit, HttpPlan, InstrumentKind, InstrumentSpecDraft, MdCodec,
    MdTransport, Secrets, SpecTable, StreamId, Subscription, SymbolError, VenueCaps, VenueConfig,
    VenueError, VenueFactory, common_symbol_parts,
};

use super::url::{UrlKey, configured};
use super::{
    ANCHOR_URL_KEY, ANCHOR_URL_REDACT_KEY, ANCHORED_BOOK, EXEC_STREAM, ToyExec, ToyMd, ToySigner,
    ToyStats, caps, feed_source,
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

/// The configuration key of the REST base stats are polled under (`http://` or `https://`), at
/// `<base>/stats/<sym>`.
pub const STATS_URL_KEY: &str = "toy.md.stats_url";
/// The configuration key of the credential spans in [`STATS_URL_KEY`]'s URL.
pub const STATS_URL_REDACT_KEY: &str = "toy.md.stats_url.redact";

/// The stream of the toy's one market-data connection, every streamed feed on it.
pub const MD_STREAM: StreamId = StreamId(0);
/// The stream of the toy's stats poll endpoint, which opens no connection.
pub const STATS_STREAM: StreamId = StreamId(2);

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
const STATS_URL: UrlKey = UrlKey {
    url: STATS_URL_KEY,
    redact: STATS_URL_REDACT_KEY,
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
             its path, at least 16 bytes long and clear of %XX escapes; empty or absent marks \
             none",
        ),
    ]
}

const SCHEMA: [[FieldSpec; 2]; 5] = [
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
    url_fields(
        STATS_URL,
        "the REST base stats are polled under (http:// or https://), without user \
         information, query, fragment or trailing /; read when stats are subscribed",
    ),
];

/// The conformance toy's factory.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Default)]
pub struct ToyFactory;

impl VenueFactory for ToyFactory {
    fn id(&self) -> &'static str {
        "TOY-CONFORMANCE"
    }

    /// The five URLs and their credential spans.
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
    /// caps do not declare as unsupported. Every streamed feed goes on one connection,
    /// [`MD_STREAM`] at the configured market-data URL, the anchored channel needing the
    /// anchors' base configured too; stats go on one poll endpoint, [`STATS_STREAM`] under the
    /// configured stats base. An endpoint with nothing to carry is not planned, so stats alone
    /// open no socket, and polling ends once no stats subscription is left.
    fn plan_md(
        &self,
        cfg: &VenueConfig,
        specs: &SpecTable,
        subs: &BTreeSet<Subscription>,
    ) -> Result<Vec<EndpointPlan>, VenueError> {
        if let Some(sub) = subs.iter().find(|s| specs.get(s.inst).is_none()) {
            return Err(VenueError::UnknownInstrument(sub.inst));
        }
        if let Some(sub) = subs
            .iter()
            .find(|s| feed_source(s.feed) == FeedSource::None)
        {
            return Err(VenueError::UnsupportedFeed(*sub));
        }
        let (polled, streamed): (Vec<Subscription>, Vec<Subscription>) = subs
            .iter()
            .copied()
            .partition(|s| feed_source(s.feed) == FeedSource::Poll);
        let mut plans = Vec::new();
        if !streamed.is_empty() {
            let url = configured(cfg, MD_URL)?;
            if streamed.iter().any(|s| s.feed == Feed::Book(ANCHORED_BOOK)) {
                configured(cfg, ANCHOR_URL)?;
            }
            plans.push(EndpointPlan {
                stream: MD_STREAM,
                transport: MdTransport::Socket { url },
                subs: streamed,
            });
        }
        if !polled.is_empty() {
            let base_url = configured(cfg, STATS_URL)?;
            plans.push(EndpointPlan {
                stream: STATS_STREAM,
                transport: MdTransport::Poll { base_url },
                subs: polled,
            });
        }
        Ok(plans)
    }

    /// For a socket, a fresh [`ToyMd`] on the endpoint's stream, anchored under the configured
    /// base (with none, or one refused, the anchored channel is refused when subscribed), the
    /// endpoint's URL's credentials named wherever a frame echoes them. For a poll endpoint, a
    /// fresh [`ToyStats`] asking under the endpoint's base, its credentials named wherever a
    /// response echoes them.
    fn md_codec(&self, cfg: &VenueConfig, ep: &EndpointPlan) -> Box<dyn MdCodec> {
        match &ep.transport {
            MdTransport::Poll { base_url } => Box::new(ToyStats::new(ep.stream, base_url.clone())),
            MdTransport::Socket { url } => {
                let codec = match configured(cfg, ANCHOR_URL) {
                    Ok(base) => ToyMd::with_anchor_url(ep.stream, base),
                    Err(_) => ToyMd::new(ep.stream),
                };
                Box::new(codec.socket_url(url))
            }
        }
    }

    /// One connection, [`EXEC_STREAM`] at the configured order-entry URL.
    fn plan_exec(&self, cfg: &VenueConfig) -> Result<Vec<ExecEndpoint>, VenueError> {
        let url = configured(cfg, EXEC_URL)?;
        Ok(vec![ExecEndpoint {
            stream: EXEC_STREAM,
            url,
            handshake: Vec::new(),
        }])
    }

    /// A fresh codec signing with [`ToySigner`], which holds no key: the credentials are
    /// dropped, and so zeroed, unread. It pings, resyncs over REST under the configured base
    /// when there is one, and names the configured URLs' credentials wherever a frame or
    /// response echoes them; a URL configured and refused refuses the codec.
    fn exec_codec(
        &self,
        cfg: &VenueConfig,
        _creds: Secrets,
    ) -> Option<Result<Box<dyn ExecCodec>, VenueError>> {
        let codec = ToyExec::new(Box::new(ToySigner)).pinging();
        let codec = match cfg.get(EXEC_URL_KEY) {
            None => Ok(codec),
            Some(_) => configured(cfg, EXEC_URL).map(|url| codec.socket_url(&url)),
        };
        let codec = codec.and_then(|codec| match cfg.get(REST_URL_KEY) {
            None => Ok(codec),
            Some(_) => configured(cfg, REST_URL).map(|base| codec.rest_resync(base)),
        });
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

#[cfg(test)]
mod tests {
    use super::*;

    /// The schema the factory declares is each URL key it reads, in order, followed by that
    /// URL's credential spans' key, every key account-scoped and of no unit, every spans' key
    /// sharing one description: the pairs built here at run time from the same keys match it.
    #[test]
    fn the_schema_is_each_url_key_the_factory_reads_then_its_spans_key() {
        let schema = ToyFactory.config_schema();
        let keys = [EXEC_URL, MD_URL, ANCHOR_URL, REST_URL, STATS_URL];
        assert_eq!(schema.len(), 2 * keys.len());
        for (pair, key) in schema.chunks(2).zip(keys) {
            assert_eq!(pair, url_fields(key, pair[0].doc));
            assert!(!pair[0].doc.is_empty(), "{pair:?}");
        }
        let spans_docs: BTreeSet<&str> = schema.iter().skip(1).step_by(2).map(|f| f.doc).collect();
        assert_eq!(spans_docs.len(), 1, "{spans_docs:?}");
        assert!(
            schema
                .iter()
                .all(|f| f.scope == ConfigScope::Account && f.unit == FieldUnit::Dimensionless)
        );
        let keys: BTreeSet<&str> = schema.iter().map(|f| f.key).collect();
        assert_eq!(keys.len(), schema.len(), "no key is declared twice");
    }
}
