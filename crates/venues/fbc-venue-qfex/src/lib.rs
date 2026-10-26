//! QFEX public market data and instrument discovery, with an offline private auth codec.
//! Order entry remains unavailable to runtime sessions (`caps.exec: None`, Record readiness)
//! until private order, fill and resync truth is established (decisions 0117 and 0122).
//!
//! The public WebSocket and its `bbo`, `level2`, and `trade` messages are documented at
//! <https://docs.qfex.com/websocket/main> and the corresponding MDS channel pages.

mod auth;
mod caps;
pub mod discover;
mod exec;
mod md;
mod sign;

pub use sign::QfexNonces;
#[doc(hidden)]
pub mod rate;
mod trade_rate;

use std::collections::BTreeSet;
use std::net::{Ipv4Addr, Ipv6Addr};

use fbc_core::{
    AccountSummary, AssetKey, BookId, ConfigError, ConfigScope, EndpointPlan, ExecCodec,
    ExecEndpoint, FieldSpec, FieldUnit, HttpPlan, InstrumentSpecDraft, MdCodec, MdTransport,
    Secrets, SpecTable, StreamId, Subscription, SymbolError, TouchSourceId, VenueCaps, VenueConfig,
    VenueError, VenueFactory, WireUrl,
};

use md::{QfexMd, channel};

/// The public MDS WebSocket URL, e.g. `wss://mds.qfex.com`.
pub const KEY_MDS_URL: &str = "qfex.mds_url";
/// The public REST origin used for `GET /refdata`, e.g. `https://api.qfex.com`.
pub const KEY_REST_URL: &str = "qfex.rest_url";
/// The public reference-data request timeout, e.g. `5000ms`.
pub const KEY_REST_TIMEOUT: &str = "qfex.rest_timeout";
/// The `bbo` channel's touch source.
pub const TOUCH_BBO: TouchSourceId = TouchSourceId(0);
/// The `level2` channel's 20-level windowed book.
pub const BOOK_LEVEL2: BookId = BookId(0);

const SCHEMA: &[FieldSpec] = &[
    FieldSpec {
        key: KEY_MDS_URL,
        scope: ConfigScope::Process,
        unit: FieldUnit::Dimensionless,
        doc: "public MDS WebSocket URL (ws:// or wss:// with host and optional port)",
    },
    FieldSpec {
        key: KEY_REST_URL,
        scope: ConfigScope::Process,
        unit: FieldUnit::Dimensionless,
        doc: "public REST origin (https://, or http:// loopback for a test stub)",
    },
    FieldSpec {
        key: KEY_REST_TIMEOUT,
        scope: ConfigScope::Process,
        unit: FieldUnit::Duration,
        doc: "positive public REST request timeout as <n>ms or <n>s",
    },
    FieldSpec {
        key: auth::PUBLIC_KEY,
        scope: ConfigScope::Account,
        unit: FieldUnit::Dimensionless,
        doc: "public half of the QFEX API credential, matched exactly before private use",
    },
    FieldSpec {
        key: auth::TRADE_URL,
        scope: ConfigScope::Account,
        unit: FieldUnit::Dimensionless,
        doc: "private trade WebSocket origin",
    },
    FieldSpec {
        key: auth::ACCOUNT_ID,
        scope: ConfigScope::Account,
        unit: FieldUnit::Dimensionless,
        doc: "explicit primary or subaccount UUID selection; never defaults to primary",
    },
    FieldSpec {
        key: auth::ACCOUNT_KEY,
        scope: ConfigScope::Account,
        unit: FieldUnit::Dimensionless,
        doc: "caller-attested decimal AccountKey of the selected provider account",
    },
    FieldSpec {
        key: exec::RPC_TIMEOUT_KEY,
        scope: ConfigScope::Account,
        unit: FieldUnit::Duration,
        doc: "positive private RPC timeout as <n>ms or <n>s",
    },
];

fn mds_url(cfg: &VenueConfig) -> Result<&str, ConfigError> {
    let url = cfg
        .get(KEY_MDS_URL)
        .ok_or(ConfigError::Missing(KEY_MDS_URL))?;
    let authority = url
        .strip_prefix("ws://")
        .or_else(|| url.strip_prefix("wss://"))
        .map(|authority| authority.strip_suffix('/').unwrap_or(authority));
    if !authority.is_some_and(valid_host_port) {
        return Err(ConfigError::Invalid {
            key: KEY_MDS_URL,
            reason: "expected a WebSocket origin",
        });
    }
    Ok(url)
}

/// WebSocket origin authority: DNS name, IPv4, or bracketed IPv6, with an optional valid port.
pub(crate) fn valid_host_port(authority: &str) -> bool {
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
            let dns_label = |label: &str| {
                !label.is_empty()
                    && label.len() <= 63
                    && label
                        .as_bytes()
                        .first()
                        .is_some_and(u8::is_ascii_alphanumeric)
                    && label
                        .as_bytes()
                        .last()
                        .is_some_and(u8::is_ascii_alphanumeric)
                    && label
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            };
            let valid = if host.bytes().all(|b| b.is_ascii_digit() || b == b'.') {
                host.parse::<Ipv4Addr>().is_ok()
            } else {
                host.len() <= 253 && host.split('.').all(dns_label)
            };
            (valid, port)
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

    fn exec_account_key(
        &self,
        cfg: &VenueConfig,
    ) -> Result<Option<fbc_core::AccountKey>, ConfigError> {
        auth::account_key(cfg).map(Some)
    }

    fn parse_fbc_common_symbol(&self, s: &str) -> Result<AssetKey, SymbolError> {
        discover::parse_fbc_common_symbol(s)
    }

    fn discover(
        &self,
        cfg: &VenueConfig,
    ) -> Result<HttpPlan<Vec<InstrumentSpecDraft>>, VenueError> {
        discover::plan(cfg)
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

    fn plan_exec(&self, cfg: &VenueConfig) -> Result<Vec<ExecEndpoint>, VenueError> {
        exec::plan(cfg)
    }

    fn exec_codec(
        &self,
        cfg: &VenueConfig,
        creds: Secrets,
    ) -> Option<Result<Box<dyn ExecCodec>, VenueError>> {
        Some(exec::codec(cfg, creds).map(|codec| Box::new(codec) as Box<dyn ExecCodec>))
    }

    fn test_connection(
        &self,
        _cfg: &VenueConfig,
        _creds: Secrets,
    ) -> Option<Result<HttpPlan<AccountSummary>, VenueError>> {
        None
    }
}
