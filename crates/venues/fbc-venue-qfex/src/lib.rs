//! QFEX public market data. Order entry is deliberately unavailable (decision 0116).
//!
//! The public WebSocket and its `bbo`, `level2`, and `trade` messages are documented at
//! <https://docs.qfex.com/websocket/main> and the corresponding MDS channel pages.

mod caps;
mod md;

use std::collections::BTreeSet;
use std::net::Ipv6Addr;

use fbc_core::{
    AccountSummary, AssetKey, BookId, ConfigError, ConfigScope, EndpointPlan, ExecCodec,
    ExecEndpoint, FieldSpec, FieldUnit, HttpPlan, InstrumentSpecDraft, MdCodec, MdTransport,
    Secrets, SpecTable, StreamId, Subscription, SymbolError, TouchSourceId, VenueCaps, VenueConfig,
    VenueError, VenueFactory, WireUrl,
};

use md::{QfexMd, channel};

/// The public MDS WebSocket URL, e.g. `wss://mds.qfex.com`.
pub const KEY_MDS_URL: &str = "qfex.mds_url";
/// The `bbo` channel's touch source.
pub const TOUCH_BBO: TouchSourceId = TouchSourceId(0);
/// The `level2` channel's 20-level windowed book.
pub const BOOK_LEVEL2: BookId = BookId(0);

const SCHEMA: &[FieldSpec] = &[FieldSpec {
    key: KEY_MDS_URL,
    scope: ConfigScope::Process,
    unit: FieldUnit::Dimensionless,
    doc: "public MDS WebSocket URL (ws:// or wss:// with host and optional port)",
}];

fn mds_url(cfg: &VenueConfig) -> Result<&str, ConfigError> {
    let url = cfg
        .get(KEY_MDS_URL)
        .ok_or(ConfigError::Missing(KEY_MDS_URL))?;
    let authority = url
        .strip_prefix("ws://")
        .or_else(|| url.strip_prefix("wss://"));
    if !authority.is_some_and(valid_host_port) {
        return Err(ConfigError::Invalid {
            key: KEY_MDS_URL,
            reason: "expected a WebSocket origin",
        });
    }
    Ok(url)
}

/// WebSocket origin authority: DNS name, IPv4, or bracketed IPv6, with an optional valid port.
fn valid_host_port(authority: &str) -> bool {
    let (host_ok, port) = match authority.strip_prefix('[') {
        Some(rest) => {
            let Some((host, after)) = rest.split_once(']') else {
                return false;
            };
            let port = match after {
                "" => None,
                _ => match after.strip_prefix(':') {
                    Some(port) => Some(port),
                    None => return false,
                },
            };
            (host.parse::<Ipv6Addr>().is_ok(), port)
        }
        None => {
            let (host, port) = match authority.split_once(':') {
                Some((host, port)) => (host, Some(port)),
                None => (authority, None),
            };
            let name = |c: char| c.is_ascii_alphanumeric() || c == '.' || c == '-';
            (!host.is_empty() && host.chars().all(name), port)
        }
    };
    host_ok
        && port.is_none_or(|p| {
            p.bytes().all(|b| b.is_ascii_digit()) && p.parse::<u16>().is_ok_and(|n| n > 0)
        })
}

/// A read-only QFEX venue factory.
#[derive(Copy, Clone, Eq, PartialEq, Debug, Default)]
pub struct Qfex;

impl VenueFactory for Qfex {
    fn id(&self) -> &'static str {
        "QFEX"
    }

    fn config_schema(&self) -> &'static [FieldSpec] {
        SCHEMA
    }

    fn caps(&self, cfg: &VenueConfig) -> Result<VenueCaps, ConfigError> {
        mds_url(cfg)?;
        Ok(caps::caps())
    }

    fn parse_fbc_common_symbol(&self, _s: &str) -> Result<AssetKey, SymbolError> {
        Err(SymbolError::NoRule)
    }

    fn discover(
        &self,
        _cfg: &VenueConfig,
    ) -> Result<HttpPlan<Vec<InstrumentSpecDraft>>, VenueError> {
        Err(VenueError::NoDiscovery)
    }

    fn plan_md(
        &self,
        cfg: &VenueConfig,
        specs: &SpecTable,
        subs: &BTreeSet<Subscription>,
    ) -> Result<Vec<EndpointPlan>, VenueError> {
        let url = mds_url(cfg)?;
        for sub in subs {
            channel(specs, *sub)?;
        }
        if subs.is_empty() {
            return Ok(Vec::new());
        }
        Ok(vec![EndpointPlan {
            stream: StreamId(0),
            transport: MdTransport::Socket {
                url: WireUrl::plain(url.to_owned()),
            },
            subs: subs.iter().copied().collect(),
        }])
    }

    fn md_codec(&self, _cfg: &VenueConfig, ep: &EndpointPlan) -> Box<dyn MdCodec> {
        Box::new(QfexMd::new(ep.stream))
    }

    fn plan_exec(&self, _cfg: &VenueConfig) -> Result<Vec<ExecEndpoint>, VenueError> {
        Ok(Vec::new())
    }

    fn exec_codec(
        &self,
        _cfg: &VenueConfig,
        _creds: Secrets,
    ) -> Option<Result<Box<dyn ExecCodec>, VenueError>> {
        None
    }

    fn test_connection(
        &self,
        _cfg: &VenueConfig,
        _creds: Secrets,
    ) -> Option<Result<HttpPlan<AccountSummary>, VenueError>> {
        None
    }
}
