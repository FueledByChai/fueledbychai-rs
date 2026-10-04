//! The configuration Binance USD-M reads: where to connect, and which partial-depth stream.

use core::time::Duration;

use fbc_core::{ConfigError, ConfigScope, FieldSpec, FieldUnit, VenueConfig};

/// The WebSocket base URL, such as `wss://fstream.binance.com`; the codec connects to its
/// `/public/stream` combined-stream endpoint.
pub const KEY_WS_BASE_URL: &str = "binance_usdm.ws_base_url";
/// The partial-depth stream's levels per side: 5, 10 or 20.
pub const KEY_DEPTH_LEVELS: &str = "binance_usdm.depth_levels";
/// The partial-depth stream's update speed: `100ms`, `250ms` or `500ms`.
pub const KEY_DEPTH_SPEED: &str = "binance_usdm.depth_speed";

pub(crate) const SCHEMA: &[FieldSpec] = &[
    FieldSpec {
        key: KEY_WS_BASE_URL,
        scope: ConfigScope::Process,
        unit: FieldUnit::Dimensionless,
        doc: "WebSocket base URL (ws:// or wss://, e.g. wss://fstream.binance.com); market data \
              connects to its /public/stream endpoint",
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

/// A configuration read and checked.
#[derive(Clone, Eq, PartialEq, Debug)]
pub(crate) struct Settings {
    /// The base URL without a trailing slash.
    base_url: String,
    pub depth: DepthChannel,
}

impl Settings {
    pub fn read(cfg: &VenueConfig) -> Result<Settings, ConfigError> {
        let get = |key| cfg.get(key).ok_or(ConfigError::Missing(key));
        let base = get(KEY_WS_BASE_URL)?.trim_end_matches('/');
        let host = base
            .strip_prefix("wss://")
            .or_else(|| base.strip_prefix("ws://"));
        if host.is_none_or(str::is_empty) {
            return Err(ConfigError::Invalid {
                key: KEY_WS_BASE_URL,
                reason: "not a ws:// or wss:// URL",
            });
        }
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
        let millis = match get(KEY_DEPTH_SPEED)? {
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
        let name = match (levels, millis) {
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
        Ok(Settings {
            base_url: base.to_owned(),
            depth: DepthChannel {
                levels,
                speed: Duration::from_millis(millis),
                name,
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
