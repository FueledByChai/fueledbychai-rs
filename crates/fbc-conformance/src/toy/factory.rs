//! The toy's factory: what the named suite builds the toy from (`suite!`), as an adapter
//! crate's suite builds its venue. It declares [`caps`], discovers nothing (the tests state the
//! instruments, [`specs`]), reads Java-era tickers by a rule of its own, takes no credentials
//! (its signer holds no key) and builds a fresh [`ToyExec`] each time it is asked.
//!
//! Not modelled yet, and refused rather than guessed: the order-entry URL, which FBC-ja3 takes
//! from the configuration with its redaction spans, and market data, which arrives with
//! FBC-u1d and FBC-z2s; until then [`NoMd`] stands in, and every feed is refused, as the caps'
//! empty `md` declares.
//!
//! [`specs`]: super::specs

use std::collections::BTreeSet;

use fbc_core::{
    AccountSummary, AssetKey, ConfigError, DecodeError, DecodeScope, Effects, EndpointPlan,
    ExecCodec, ExecEndpoint, FieldSpec, HttpFailure, HttpPlan, HttpResponse, HttpTag, Inbound,
    InboundSpans, InstrumentKind, InstrumentSpecDraft, Keepalive, MdCodec, MdSink, MonoNs,
    RawFrame, Secrets, SpecTable, Subscription, SymbolError, TimerTag, VenueCaps, VenueConfig,
    VenueError, VenueFactory, WallNs, common_symbol_parts,
};

use super::{ToyExec, ToySigner, caps};

/// The configuration key FBC-ja3 reads the toy's order-entry URL from.
pub const EXEC_URL_KEY: &str = "toy.exec.url";

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

    /// Nothing to plan for no subscription; every feed is refused, since the toy declares none.
    fn plan_md(
        &self,
        _cfg: &VenueConfig,
        _specs: &SpecTable,
        subs: &BTreeSet<Subscription>,
    ) -> Result<Vec<EndpointPlan>, VenueError> {
        match subs.first() {
            Some(sub) => Err(VenueError::UnsupportedFeed(*sub)),
            None => Ok(Vec::new()),
        }
    }

    fn md_codec(&self, _cfg: &VenueConfig, _ep: &EndpointPlan) -> Box<dyn MdCodec> {
        Box::new(NoMd)
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

/// The toy's market-data codec until FBC-u1d: it subscribes to nothing, decodes nothing and
/// asks for nothing, as the toy's caps declare no feed.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Default)]
pub struct NoMd;

impl MdCodec for NoMd {
    fn on_open(&mut self, _fx: &mut Effects) {}

    /// Refuses the first subscription asked for; an unsubscription of nothing subscribed is
    /// nothing to do.
    fn subscribe(
        &mut self,
        add: &[Subscription],
        _remove: &[Subscription],
        _specs: &SpecTable,
        _fx: &mut Effects,
    ) -> Result<(), VenueError> {
        match add.first() {
            Some(sub) => Err(VenueError::UnsupportedFeed(*sub)),
            None => Ok(()),
        }
    }

    fn on_frame(
        &mut self,
        _f: RawFrame<'_>,
        _scope: &DecodeScope<'_>,
        _specs: &SpecTable,
        _sink: &mut dyn MdSink,
        _fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        Err(DecodeError::Malformed("the toy publishes no market data"))
    }

    fn on_http(
        &mut self,
        _tag: HttpTag,
        _resp: Result<HttpResponse<'_>, HttpFailure>,
        _scope: &DecodeScope<'_>,
        _specs: &SpecTable,
        _sink: &mut dyn MdSink,
        _fx: &mut Effects,
    ) -> Result<(), DecodeError> {
        Err(DecodeError::Malformed(
            "the toy's market data asks for no HTTP",
        ))
    }

    fn on_timer(
        &mut self,
        _tag: TimerTag,
        _now: MonoNs,
        _wall: WallNs,
        _sink: &mut dyn MdSink,
        _fx: &mut Effects,
    ) {
    }

    fn keepalive(&self) -> Option<Keepalive> {
        None
    }

    fn redact_inbound(&self, _input: Inbound<'_>) -> InboundSpans {
        InboundSpans::NONE
    }
}
