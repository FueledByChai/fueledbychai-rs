//! The configuration Binance USD-M reads: where to connect, which partial-depth stream, and how
//! the diff-depth book's REST snapshot is asked for.

use core::num::NonZeroU32;
use core::time::Duration;
use std::net::Ipv6Addr;

use fbc_core::{ConfigError, ConfigScope, FieldSpec, FieldUnit, VenueConfig};

use crate::caps::rest_depth_weight;

/// The WebSocket base URL, an origin such as `wss://fstream.binance.com` (scheme, host and
/// optional port, nothing more); the codec connects to its `/public/stream` combined-stream
/// endpoint.
pub const KEY_WS_BASE_URL: &str = "binance_usdm.ws_base_url";
/// The partial-depth stream's levels per side: 5, 10 or 20.
pub const KEY_DEPTH_LEVELS: &str = "binance_usdm.depth_levels";
/// The partial-depth stream's update speed: `100ms`, `250ms` or `500ms`.
pub const KEY_DEPTH_SPEED: &str = "binance_usdm.depth_speed";
/// The REST base URL, an origin such as `https://fapi.binance.com` (scheme, host and optional
/// port, nothing more); the diff-depth book's snapshot is `GET /fapi/v1/depth` on it.
pub const KEY_REST_BASE_URL: &str = "binance_usdm.rest_base_url";
/// The levels per side a diff-depth snapshot asks for (`limit`): 5, 10, 20, 50, 100, 500 or
/// 1000, the values Binance accepts; its weight follows [`rest_depth_weight`](crate::rest_depth_weight).
pub const KEY_SNAPSHOT_LIMIT: &str = "binance_usdm.depth_snapshot_limit";
/// How long a diff-depth snapshot request waits for its response, in milliseconds (`5000ms`).
pub const KEY_SNAPSHOT_TIMEOUT: &str = "binance_usdm.depth_snapshot_timeout";
/// How long after a failed or refused diff-depth snapshot request the codec asks again, in
/// milliseconds (`2000ms`).
pub const KEY_SNAPSHOT_RETRY: &str = "binance_usdm.depth_snapshot_retry";

pub(crate) const SCHEMA: &[FieldSpec] = &[
    FieldSpec {
        key: KEY_WS_BASE_URL,
        scope: ConfigScope::Process,
        unit: FieldUnit::Dimensionless,
        doc: "WebSocket base URL, an origin (ws:// or wss://, host and optional port, e.g. \
              wss://fstream.binance.com); market data connects to its /public/stream endpoint",
    },
    FieldSpec {
        key: KEY_DEPTH_LEVELS,
        scope: ConfigScope::Process,
        unit: FieldUnit::Count,
        doc: "levels per side of the partial-depth stream: 5, 10 or 20",
    },
    FieldSpec {
        key: KEY_DEPTH_SPEED,
        scope: ConfigScope::Process,
        unit: FieldUnit::Duration,
        doc: "update speed of the partial-depth stream: 100ms, 250ms or 500ms",
    },
    FieldSpec {
        key: KEY_REST_BASE_URL,
        scope: ConfigScope::Process,
        unit: FieldUnit::Dimensionless,
        doc: "REST base URL, an origin (http:// or https://, host and optional port, e.g. \
              https://fapi.binance.com); the diff-depth snapshot is GET /fapi/v1/depth on it",
    },
    FieldSpec {
        key: KEY_SNAPSHOT_LIMIT,
        scope: ConfigScope::Process,
        unit: FieldUnit::Count,
        doc: "levels per side of the diff-depth book's REST snapshot: 5, 10, 20, 50, 100, 500 \
              or 1000",
    },
    FieldSpec {
        key: KEY_SNAPSHOT_TIMEOUT,
        scope: ConfigScope::Process,
        unit: FieldUnit::Duration,
        doc: "how long a diff-depth snapshot request waits for its response, in whole \
              milliseconds (e.g. 5000ms)",
    },
    FieldSpec {
        key: KEY_SNAPSHOT_RETRY,
        scope: ConfigScope::Process,
        unit: FieldUnit::Duration,
        doc: "how long after a failed or refused diff-depth snapshot request it is asked \
              again, in whole milliseconds (e.g. 2000ms)",
    },
];

/// The partial-depth stream chosen by configuration.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) struct DepthChannel {
    /// Levels per side.
    pub levels: u16,
    /// How often it is published.
    pub speed: Duration,
    /// Its stream-name suffix: `depth<levels>`, with `@<speed>` unless the speed is the
    /// default 250 ms, which Binance names without a suffix.
    pub name: &'static str,
}

/// How the diff-depth book's REST snapshot is asked for.
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct SnapshotSettings {
    /// The REST base URL without a trailing slash.
    pub base_url: String,
    /// Levels per side (`limit`), one Binance accepts.
    pub limit: u16,
    /// The request's documented weight at `limit`.
    pub weight: NonZeroU32,
    /// How long a request waits for its response.
    pub timeout: Duration,
    /// How long after a failure the request is made again.
    pub retry: Duration,
}

/// A configuration read and checked.
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct Settings {
    /// The base URL without a trailing slash.
    base_url: String,
    pub depth: DepthChannel,
    pub snapshot: SnapshotSettings,
}

/// `<scheme>://host[:port]` for one of `schemes`, without a trailing slash, or `None`.
fn origin<'a>(value: &'a str, schemes: [&str; 2]) -> Option<&'a str> {
    let base = value.trim_end_matches('/');
    let authority = schemes
        .iter()
        .find_map(|scheme| base.strip_prefix(scheme)?.strip_prefix("://"));
    authority.is_some_and(is_host_port).then_some(base)
}

/// A whole, positive number of milliseconds written `<n>ms`.
fn millis(value: &str) -> Option<Duration> {
    let digits = value.strip_suffix("ms")?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let n: u64 = digits.parse().ok()?;
    (n > 0).then(|| Duration::from_millis(n))
}

/// Whether `authority` is `host[:port]` and nothing more: a host name or IPv4 address of
/// letters, digits, dots and hyphens, or a bracketed address that parses as IPv6, and a port from 1 to 65535.
/// No user information, path, query or fragment: the endpoint path is appended to it.
fn is_host_port(authority: &str) -> bool {
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
    let port_ok = port.is_none_or(|p| {
        p.bytes().all(|b| b.is_ascii_digit()) && p.parse::<u16>().is_ok_and(|n| n > 0)
    });
    host_ok && port_ok
}

impl Settings {
    pub fn read(cfg: &VenueConfig) -> Result<Settings, ConfigError> {
        let get = |key| cfg.get(key).ok_or(ConfigError::Missing(key));
        let base = origin(get(KEY_WS_BASE_URL)?, ["wss", "ws"]).ok_or(ConfigError::Invalid {
            key: KEY_WS_BASE_URL,
            reason: "not a ws:// or wss:// origin (scheme://host[:port])",
        })?;
        let levels = match get(KEY_DEPTH_LEVELS)? {
            "5" => 5,
            "10" => 10,
            "20" => 20,
            _ => {
                return Err(ConfigError::Invalid {
                    key: KEY_DEPTH_LEVELS,
                    reason: "not 5, 10 or 20",
                });
            }
        };
        let speed_ms = match get(KEY_DEPTH_SPEED)? {
            "100ms" => 100,
            "250ms" => 250,
            "500ms" => 500,
            _ => {
                return Err(ConfigError::Invalid {
                    key: KEY_DEPTH_SPEED,
                    reason: "not 100ms, 250ms or 500ms",
                });
            }
        };
        let name = match (levels, speed_ms) {
            (5, 100) => "depth5@100ms",
            (5, 250) => "depth5",
            (5, _) => "depth5@500ms",
            (10, 100) => "depth10@100ms",
            (10, 250) => "depth10",
            (10, _) => "depth10@500ms",
            (_, 100) => "depth20@100ms",
            (_, 250) => "depth20",
            (_, _) => "depth20@500ms",
        };
        let rest =
            origin(get(KEY_REST_BASE_URL)?, ["https", "http"]).ok_or(ConfigError::Invalid {
                key: KEY_REST_BASE_URL,
                reason: "not an http:// or https:// origin (scheme://host[:port])",
            })?;
        let limit = get(KEY_SNAPSHOT_LIMIT)?.parse::<u16>().ok();
        let weighed = limit.and_then(|l| Some((l, rest_depth_weight(l)?)));
        let (limit, weight) = weighed.ok_or(ConfigError::Invalid {
            key: KEY_SNAPSHOT_LIMIT,
            reason: "not 5, 10, 20, 50, 100, 500 or 1000",
        })?;
        let duration = |key| {
            millis(get(key)?).ok_or(ConfigError::Invalid {
                key,
                reason: "not a positive whole number of milliseconds (<n>ms)",
            })
        };
        Ok(Settings {
            base_url: base.to_owned(),
            depth: DepthChannel {
                levels,
                speed: Duration::from_millis(speed_ms),
                name,
            },
            snapshot: SnapshotSettings {
                base_url: rest.to_owned(),
                limit,
                weight,
                timeout: duration(KEY_SNAPSHOT_TIMEOUT)?,
                retry: duration(KEY_SNAPSHOT_RETRY)?,
            },
        })
    }

    /// The combined-stream endpoint of the `/public` route, which carries `bookTicker` and
    /// depth since Binance split its USD-M streams by route (Connect page: connections without
    /// a routed path receive only the public endpoint's data).
    pub fn endpoint(&self) -> String {
        format!("{}/public/stream", self.base_url)
    }
}
