//! The configuration Hibachi reads: where its public market-data REST API is, how long a request
//! there waits, and how many levels each orderbook subscription asks for. Every key is required
//! and none has a default.

use core::time::Duration;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use fbc_core::{ConfigError, ConfigScope, FieldSpec, FieldUnit, VenueConfig};

/// The market-data REST origin, such as `https://data-api.hibachi.xyz` (the host the Market API
/// spec names as its server, and the SDK's `helpers.py` and the Java library's
/// `HibachiConfiguration.java` call the data API): scheme, host and optional port, nothing
/// more. HTTPS, or plain HTTP to a loopback host for a test stub. Discovery is
/// `GET /market/exchange-info` on it.
pub const KEY_DATA_REST_URL: &str = "hibachi.data_rest_url";
/// How long a request to the market-data REST API waits for its answer, in whole milliseconds
/// (`5000ms`).
pub const KEY_REST_TIMEOUT: &str = "hibachi.rest_timeout";
/// The levels per side each orderbook subscription asks for, a whole number from 1 to 100: the
/// bound the SDK puts on the REST orderbook's `depth` (`api.py` `get_orderbook`), which the
/// WebSocket market docs refer the topic's `depth` to. No default, so the venue's undocumented
/// one never decides what the book holds; the orderbook channel's `max_depth` is this value.
pub const KEY_ORDERBOOK_DEPTH: &str = "hibachi.orderbook_depth";

/// The deepest orderbook subscription (SDK `api.py` `get_orderbook`: "between 1 and 100").
const MAX_DEPTH: u16 = 100;

pub(crate) const SCHEMA: &[FieldSpec] = &[
    FieldSpec {
        key: KEY_DATA_REST_URL,
        scope: ConfigScope::Process,
        unit: FieldUnit::Dimensionless,
        doc: "market-data REST origin (https://host[:port], or http:// to a loopback host for a \
              test stub, e.g. https://data-api.hibachi.xyz); discovery is GET \
              /market/exchange-info on it",
    },
    FieldSpec {
        key: KEY_REST_TIMEOUT,
        scope: ConfigScope::Process,
        unit: FieldUnit::Duration,
        doc: "how long a market-data REST request waits for its answer, in whole milliseconds \
              (e.g. 5000ms)",
    },
    FieldSpec {
        key: KEY_ORDERBOOK_DEPTH,
        scope: ConfigScope::Process,
        unit: FieldUnit::Count,
        doc: "levels per side each orderbook subscription asks for: a whole number from 1 to 100",
    },
];

/// A configuration read and checked.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) struct Settings {
    /// The orderbook subscriptions' depth, 1 to 100.
    pub depth: u16,
}

impl Settings {
    /// Every key, each checked: the REST origin and timeout as discovery reads them, and the
    /// orderbook depth.
    pub fn read(cfg: &VenueConfig) -> Result<Settings, ConfigError> {
        rest_origin(cfg)?;
        rest_timeout(cfg)?;
        Ok(Settings { depth: depth(cfg)? })
    }
}

/// The market-data REST origin without a trailing slash.
pub(crate) fn rest_origin(cfg: &VenueConfig) -> Result<&str, ConfigError> {
    let raw = cfg
        .get(KEY_DATA_REST_URL)
        .ok_or(ConfigError::Missing(KEY_DATA_REST_URL))?;
    let bad = ConfigError::Invalid {
        key: KEY_DATA_REST_URL,
        reason: "not an https:// origin, or an http:// origin on a loopback host",
    };
    let origin = raw.strip_suffix('/').unwrap_or(raw);
    let (authority, plain) = match origin.strip_prefix("https://") {
        Some(authority) => (authority, false),
        None => (origin.strip_prefix("http://").ok_or(bad)?, true),
    };
    let host = host_of(authority).ok_or(bad)?;
    if plain && !is_loopback(host) {
        return Err(bad);
    }
    Ok(origin)
}

/// How long a market-data REST request waits: a positive whole number of milliseconds written
/// `<n>ms`.
pub(crate) fn rest_timeout(cfg: &VenueConfig) -> Result<Duration, ConfigError> {
    let raw = cfg
        .get(KEY_REST_TIMEOUT)
        .ok_or(ConfigError::Missing(KEY_REST_TIMEOUT))?;
    let millis = raw
        .strip_suffix("ms")
        .filter(|digits| !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()))
        .and_then(|digits| digits.parse::<u64>().ok())
        .filter(|ms| *ms > 0);
    millis
        .map(Duration::from_millis)
        .ok_or(ConfigError::Invalid {
            key: KEY_REST_TIMEOUT,
            reason: "not a positive whole number of milliseconds (<n>ms)",
        })
}

/// The orderbook depth: ASCII digits only (no sign), from 1 to 100.
fn depth(cfg: &VenueConfig) -> Result<u16, ConfigError> {
    let raw = cfg
        .get(KEY_ORDERBOOK_DEPTH)
        .ok_or(ConfigError::Missing(KEY_ORDERBOOK_DEPTH))?;
    Some(raw)
        .filter(|digits| !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()))
        .and_then(|digits| digits.parse::<u16>().ok())
        .filter(|levels| (1..=MAX_DEPTH).contains(levels))
        .ok_or(ConfigError::Invalid {
            key: KEY_ORDERBOOK_DEPTH,
            reason: "not a whole number of levels from 1 to 100",
        })
}

/// The host of `authority` when it is `host[:port]` and nothing more: a DNS name of letters,
/// digits and hyphens between dots, a dotted IPv4 address, or a bracketed IPv6 address, and a
/// port from 1 to 65535. No user information, path, query or fragment: the request path is
/// appended to the origin.
fn host_of(authority: &str) -> Option<&str> {
    let (host, port) = match authority.strip_prefix('[') {
        Some(rest) => {
            let (host, after) = rest.split_once(']')?;
            host.parse::<Ipv6Addr>().ok()?;
            let port = match after {
                "" => None,
                _ => Some(after.strip_prefix(':')?),
            };
            (host, port)
        }
        None => {
            let (host, port) = match authority.split_once(':') {
                Some((host, port)) => (host, Some(port)),
                None => (authority, None),
            };
            let label = |l: &str| {
                !l.is_empty() && l.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
            };
            let valid = if host.bytes().all(|b| b.is_ascii_digit() || b == b'.') {
                host.parse::<Ipv4Addr>().is_ok()
            } else {
                host.split('.').all(label)
            };
            if !valid {
                return None;
            }
            (host, port)
        }
    };
    let port_ok = port.is_none_or(|p| {
        !p.is_empty()
            && p.bytes().all(|b| b.is_ascii_digit())
            && p.parse::<u16>().is_ok_and(|n| n > 0)
    });
    port_ok.then_some(host)
}

/// Whether `host` (unbracketed) names this machine: `localhost` or a loopback address.
fn is_loopback(host: &str) -> bool {
    host == "localhost" || host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
}
