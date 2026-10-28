//! The configuration Hibachi reads: where its public market-data REST API and WebSocket are, how
//! long a REST request waits, how many levels each orderbook subscription asks for, how long
//! the market-data codec waits before it asks again for an orderbook it found broken, how often
//! the status poll re-reads each market's trading status, and how long a market stays halted
//! after a reopening. Every key is required and none has a default.

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
/// The market-data WebSocket origin, such as `wss://data-api.hibachi.xyz` (the origin the
/// WebSocket market docs give `/ws/market` on, and the SDK's `api_ws_market.py` and the Java
/// library's `HibachiConfiguration.java` connect to): scheme, host and optional port, nothing
/// more. WSS, or plain WS to a loopback host for a test stub. The market-data codec's one
/// connection is `/ws/market` on it. Not derived from [`KEY_DATA_REST_URL`]: a test stub serves
/// its WebSocket and its HTTP on two ports.
pub const KEY_DATA_WS_URL: &str = "hibachi.data_ws_url";
/// How long the market-data codec waits, in whole milliseconds (`<n>ms`, zero refused), after it
/// finds an orderbook broken (a Snapshot with no level, a frame failing the bounds check or
/// malformed) before it unsubscribes and subscribes that instrument's orderbook again on the same
/// connection, and how long it then waits for that Snapshot before it asks for a reconnect
/// (decision 0129). No default, as `binance_usdm.depth_snapshot_retry` has none.
pub const KEY_ORDERBOOK_RESUBSCRIBE: &str = "hibachi.orderbook_resubscribe";
/// The levels per side each orderbook subscription asks for, a whole number from 1 to 100: the
/// bound the SDK puts on the REST orderbook's `depth` (`api.py` `get_orderbook`), which the
/// WebSocket market docs refer the topic's `depth` to. No default, so the venue's undocumented
/// one never decides what the book holds; the orderbook channel's `max_depth` is this value.
pub const KEY_ORDERBOOK_DEPTH: &str = "hibachi.orderbook_depth";

/// How often the status poll re-reads `GET /market/exchange-info` on the market-data REST origin
/// for each subscribed market's trading status, in whole seconds (`<n>s`, zero refused; FBC-1632,
/// decision 0130). No default: the interval bounds how late a change no schedule announces
/// reaches the consumer, which is the consumer's choice against the IP's REST limit.
pub const KEY_STATUS_INTERVAL: &str = "hibachi.status_interval";
/// How long a market stays halted after each reopening the status poll knows of (a published
/// maintenance window's end, a contract's session reopening in its calendar, an advertised open,
/// and a reopening its reads show, counted from the judged time of the first read that shows the
/// market trading again after one showed it halted by status), in whole seconds (`<n>s`, zero
/// refused). Hibachi runs a call auction then
/// (its call-auction docs), and no source says which status a contract shows during one, so
/// until FBC-vg2n records it from evidence the poll reports the market halted for this window
/// and only a read sent after it ends can report it trading (decision 0130). No default.
pub const KEY_AUCTION_HALT: &str = "hibachi.auction_halt";

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
    FieldSpec {
        key: KEY_DATA_WS_URL,
        scope: ConfigScope::Process,
        unit: FieldUnit::Dimensionless,
        doc: "market-data WebSocket origin (wss://host[:port], or ws:// to a loopback host for a \
              test stub, e.g. wss://data-api.hibachi.xyz); the market-data connection is \
              /ws/market on it",
    },
    FieldSpec {
        key: KEY_ORDERBOOK_RESUBSCRIBE,
        scope: ConfigScope::Process,
        unit: FieldUnit::Duration,
        doc: "how long, in whole milliseconds (e.g. 2000ms; zero refused), the market-data codec \
              waits after finding an orderbook broken before it resubscribes it, and then for \
              that Snapshot before it reconnects",
    },
    FieldSpec {
        key: KEY_STATUS_INTERVAL,
        scope: ConfigScope::Process,
        unit: FieldUnit::Duration,
        doc: "how often, in whole seconds (e.g. 30s; zero refused), the status poll re-reads GET \
              /market/exchange-info for each subscribed market's trading status",
    },
    FieldSpec {
        key: KEY_AUCTION_HALT,
        scope: ConfigScope::Process,
        unit: FieldUnit::Duration,
        doc: "how long, in whole seconds (e.g. 900s; zero refused), a market is reported halted \
              after a maintenance window's end, a session's reopening, an advertised open or a \
              reopening the status poll's reads show, for the call auction Hibachi runs then",
    },
];

/// The path of the market-data WebSocket on [`KEY_DATA_WS_URL`]'s origin (the WebSocket market
/// docs; SDK `api_ws_market.py`).
const WS_MARKET_PATH: &str = "/ws/market";

/// A configuration read and checked.
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct Settings {
    /// The orderbook subscriptions' depth, 1 to 100.
    pub depth: u16,
    /// The market-data WebSocket's URL: `/ws/market` on the configured origin.
    pub ws_market: String,
    /// How long the codec waits before resubscribing a broken orderbook, and then for its
    /// Snapshot before reconnecting.
    pub resubscribe: Duration,
    /// The market-data REST origin, without a trailing slash: the status poll's base.
    pub rest_origin: String,
    /// How long a market-data REST request waits.
    pub rest_timeout: Duration,
    /// How often the status poll re-reads the exchange information.
    pub status_interval: Duration,
    /// How long a market stays halted after a reopening.
    pub auction_halt: Duration,
}

impl Settings {
    /// Every key, each checked: the REST origin and timeout as discovery reads them, the
    /// orderbook depth, the WebSocket origin, the resubscribe interval, the status interval and
    /// the window after a reopening.
    pub fn read(cfg: &VenueConfig) -> Result<Settings, ConfigError> {
        let rest = rest_origin(cfg)?.to_owned();
        let rest_timeout = rest_timeout(cfg)?;
        let depth = depth(cfg)?;
        let ws_origin = origin(cfg, KEY_DATA_WS_URL, WS_SCHEMES)?;
        Ok(Settings {
            depth,
            ws_market: format!("{ws_origin}{WS_MARKET_PATH}"),
            resubscribe: millis(cfg, KEY_ORDERBOOK_RESUBSCRIBE)?,
            rest_origin: rest,
            rest_timeout,
            status_interval: seconds(cfg, KEY_STATUS_INTERVAL)?,
            auction_halt: seconds(cfg, KEY_AUCTION_HALT)?,
        })
    }
}

/// An origin's secure scheme, its plain one (loopback only), and why a value is refused.
struct Schemes {
    secure: &'static str,
    plain: &'static str,
    refused: &'static str,
}

const REST_SCHEMES: Schemes = Schemes {
    secure: "https://",
    plain: "http://",
    refused: "not an https:// origin, or an http:// origin on a loopback host",
};

const WS_SCHEMES: Schemes = Schemes {
    secure: "wss://",
    plain: "ws://",
    refused: "not a wss:// origin, or a ws:// origin on a loopback host",
};

/// The market-data REST origin without a trailing slash.
pub(crate) fn rest_origin(cfg: &VenueConfig) -> Result<&str, ConfigError> {
    origin(cfg, KEY_DATA_REST_URL, REST_SCHEMES)
}

/// The origin under `key` without a trailing slash: the secure scheme, or the plain one to a
/// loopback host, then `host[:port]` and nothing more.
fn origin<'a>(
    cfg: &'a VenueConfig,
    key: &'static str,
    schemes: Schemes,
) -> Result<&'a str, ConfigError> {
    let raw = cfg.get(key).ok_or(ConfigError::Missing(key))?;
    let bad = ConfigError::Invalid {
        key,
        reason: schemes.refused,
    };
    let origin = raw.strip_suffix('/').unwrap_or(raw);
    let (authority, plain) = match origin.strip_prefix(schemes.secure) {
        Some(authority) => (authority, false),
        None => (origin.strip_prefix(schemes.plain).ok_or(bad)?, true),
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
    millis(cfg, KEY_REST_TIMEOUT)
}

/// The duration under `key`: a positive whole number of milliseconds written `<n>ms`.
fn millis(cfg: &VenueConfig, key: &'static str) -> Result<Duration, ConfigError> {
    let raw = cfg.get(key).ok_or(ConfigError::Missing(key))?;
    let millis = raw
        .strip_suffix("ms")
        .filter(|digits| !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()))
        .and_then(|digits| digits.parse::<u64>().ok())
        .filter(|ms| *ms > 0);
    millis
        .map(Duration::from_millis)
        .ok_or(ConfigError::Invalid {
            key,
            reason: "not a positive whole number of milliseconds (<n>ms)",
        })
}

/// The duration under `key`: a positive whole number of seconds written `<n>s`, at most
/// `u32::MAX`, so that it is a whole count of nanoseconds a wall time can be moved by.
fn seconds(cfg: &VenueConfig, key: &'static str) -> Result<Duration, ConfigError> {
    let raw = cfg.get(key).ok_or(ConfigError::Missing(key))?;
    let secs = raw
        .strip_suffix('s')
        .filter(|digits| !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()))
        .and_then(|digits| digits.parse::<u32>().ok())
        .filter(|secs| *secs > 0);
    secs.map(|secs| Duration::from_secs(u64::from(secs)))
        .ok_or(ConfigError::Invalid {
            key,
            reason: "not a positive whole number of seconds (<n>s)",
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
