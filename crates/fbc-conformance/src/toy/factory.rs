//! The toy's factory: what the named suite builds the toy from (`suite!`), as an adapter
//! crate's suite builds its venue. It declares [`caps`], discovers nothing (the tests state the
//! instruments, [`specs`]), reads Java-era tickers by a rule of its own, takes no credentials
//! (its signer holds no key) and builds a fresh [`ToyExec`] each time it is asked.
//!
//! Its market-data codec is [`ToyMd`], with no anchor URL. Not modelled yet, and refused rather
//! than guessed: the order-entry, market-data and anchor URLs, which FBC-ja3 takes from the
//! configuration with their redaction spans; and the feeds other than books, which arrive with
//! FBC-z2s.
//!
//! [`specs`]: super::specs

use std::collections::BTreeSet;

use fbc_core::{
    AccountSummary, AssetKey, ConfigError, EndpointPlan, ExecCodec, ExecEndpoint, Feed, FieldSpec,
    HttpPlan, InstrumentKind, InstrumentSpecDraft, MdCodec, Secrets, SpecTable, Subscription,
    SymbolError, VenueCaps, VenueConfig, VenueError, VenueFactory, common_symbol_parts,
};

use super::{ToyExec, ToyMd, ToySigner, caps};

/// The configuration key FBC-ja3 reads the toy's order-entry URL from.
pub const EXEC_URL_KEY: &str = "toy.exec.url";
/// The configuration key FBC-ja3 reads the toy's market-data URL from.
pub const MD_URL_KEY: &str = "toy.md.url";

/// The conformance toy's factory.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Default)]
pub struct ToyFactory;

impl VenueFactory for ToyFactory {
    fn id(&self) -> &'static str {
        "TOY-CONFORMANCE"
    }

    /// No key is read yet.
    fn config_schema(&self) -> &'static [FieldSpec] {
        &[]
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

    /// Nothing to plan for no subscription. A feed the caps do not declare is refused as
    /// unsupported; a declared book channel as configuration, since the market-data URL is not
    /// modelled until FBC-ja3.
    fn plan_md(
        &self,
        _cfg: &VenueConfig,
        _specs: &SpecTable,
        subs: &BTreeSet<Subscription>,
    ) -> Result<Vec<EndpointPlan>, VenueError> {
        let books = caps().md.books.len();
        let declared =
            |s: &Subscription| matches!(s.feed, Feed::Book(b) if usize::from(b.0) < books);
        if let Some(sub) = subs.iter().find(|s| !declared(s)) {
            return Err(VenueError::UnsupportedFeed(*sub));
        }
        match subs.is_empty() {
            true => Ok(Vec::new()),
            false => Err(VenueError::Config(ConfigError::Invalid {
                key: MD_URL_KEY,
                reason: "the conformance toy plans no market-data connection yet (FBC-ja3)",
            })),
        }
    }

    /// A fresh [`ToyMd`] on the endpoint's stream, with no anchor URL (FBC-ja3).
    fn md_codec(&self, _cfg: &VenueConfig, ep: &EndpointPlan) -> Box<dyn MdCodec> {
        Box::new(ToyMd::new(ep.stream))
    }

    /// Refused, whatever the configuration holds: the toy's order-entry URL is not modelled
    /// until FBC-ja3, so a runtime given this factory opens nothing rather than a connection to
    /// a made-up address.
    fn plan_exec(&self, _cfg: &VenueConfig) -> Result<Vec<ExecEndpoint>, VenueError> {
        Err(VenueError::Config(ConfigError::Invalid {
            key: EXEC_URL_KEY,
            reason: "the conformance toy plans no order-entry connection yet (FBC-ja3)",
        }))
    }

    /// A fresh codec signing with [`ToySigner`], which holds no key: the credentials are
    /// dropped, and so zeroed, unread.
    fn exec_codec(
        &self,
        _cfg: &VenueConfig,
        _creds: Secrets,
    ) -> Option<Result<Box<dyn ExecCodec>, VenueError>> {
        let codec: Box<dyn ExecCodec> = Box::new(ToyExec::new(Box::new(ToySigner)));
        Some(Ok(codec))
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
