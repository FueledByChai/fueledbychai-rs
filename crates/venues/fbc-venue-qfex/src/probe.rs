//! QFEX's probe modes (decision 0132 item 6, FBC-mfey; decision 0143 item 2, FBC-3prm): the
//! two configurations under which this adapter declares order entry, each for the owner-assisted
//! probe alone (0132 item 7), and the guard that keeps each on the environment it names.
//!
//! - **The modes.** [`KEY_EXEC_MODE`] is absent, [`PREPROD_PROBE`] or [`PRODUCTION_PROBE`]; any
//!   other value is a configuration error. Absent, the factory declares `exec: None` and builds
//!   no order-entry codec, as before these modes existed. Readiness stays `Record` in every mode.
//!   Each mode names its [`Environment`]; it is not the admission of QFEX order entry on
//!   production (decision 0143 item 5).
//! - **The guard.** Under a mode, every QFEX origin the configuration names is required and must
//!   pass it: either each is exactly its origin in the mode's environment, a trailing `/` ignored
//!   ([`Environment::origins`]: [`PREPROD_TRADE`], [`PREPROD_REST`] and [`PREPROD_MDS`], or
//!   [`PRODUCTION_TRADE`], [`PRODUCTION_REST`] and [`PRODUCTION_MDS`]), or each is a loopback
//!   address (`127.0.0.0/8` or `[::1]`, never a host name) for a test stub. A mix of the two,
//!   any host of the other environment's domain (`qfex.com` under [`PREPROD_PROBE`], `qfex.io`
//!   under [`PRODUCTION_PROBE`]) and any other origin are refused before anything is planned. An
//!   origin QFEX supplies outside these lists needs a reviewed change, not a setting.
//! - **Its settings**, each required with no default under either mode: [`KEY_PAGE_SIZE`] (1 to
//!   1000, item 5's `get_user_orders` page), [`KEY_RESYNC_TIMEOUT`] (item 5's own resync
//!   deadline) and [`KEY_RPC_TIMEOUT`] (each request's deadline, after which a place ends
//!   `Unknown`), the last refused unless longer than QFEX's 500 ms speed bump ([`SPEED_BUMP`]),
//!   since a limit GTC or IOC place may get no `ACK` until its hold ends.
//! - **The probe's own connections** ([`connections`]): every connection and request the probe
//!   makes outside an order-entry session takes its origin from here, after the guard and
//!   decision 0122 item 1's account check (item 7).
//!
//! The factory sees no proxy setting, so the guard checks origins only. A probe that builds its
//! connector refuses to start when the origins are loopback and the proxy is not direct
//! ([`Origins::Loopback`]); nothing in fbc-runtime names QFEX.
//!
//! The pre-production origins are the
//! [builder wallet registration page](https://docs.qfex.com/api-reference/builder-wallet-registration)'s
//! REST API and Trade WebSocket, and QFEX's [CLI README](https://github.com/QFEX-org/cli)'s
//! market-data socket for the same environment. The production origins are the REST
//! reference's one server, the [trade AsyncAPI](https://docs.qfex.com/websocket/trade.yaml)'s
//! production server, and the market-data socket this adapter's public captures were read from
//! (`fixtures/qfex/README.md`), as decision 0143 cites them.

use std::net::{Ipv4Addr, Ipv6Addr};
use std::time::Duration;

use fbc_core::{AccountKey, ConfigError, ConfigScope, FieldSpec, FieldUnit, VenueConfig, WireUrl};

use crate::{KEY_MDS_URL, KEY_REST_URL, auth, valid_host_port};

/// What the order-entry side of this configuration may do: absent, [`PREPROD_PROBE`] or
/// [`PRODUCTION_PROBE`].
pub const KEY_EXEC_MODE: &str = "qfex.exec.mode";
/// The pre-production probe mode (decision 0132 item 6).
pub const PREPROD_PROBE: &str = "preprod_probe";
/// The production probe mode (decision 0143 item 2): the probe's stage one on production.
pub const PRODUCTION_PROBE: &str = "production_probe";
/// The page size of item 5's `get_user_orders` reads: 1 to 1000, QFEX's default and largest
/// documented page being 1000 ([get_user_orders](https://docs.qfex.com/websocket/channels/trade/get_user_orders)).
pub const KEY_PAGE_SIZE: &str = "qfex.exec.page_size";
/// Item 5's resync deadline, after which the codec asks to reconnect.
pub const KEY_RESYNC_TIMEOUT: &str = "qfex.exec.resync_timeout";
/// Each request's deadline, after which it ends `Unknown`: longer than [`SPEED_BUMP`].
pub const KEY_RPC_TIMEOUT: &str = "qfex.exec.rpc_timeout";

/// The pre-production Trade WebSocket.
pub const PREPROD_TRADE: &str = "wss://trade.qfex.io";
/// The pre-production REST origin: reference data and every private REST read.
pub const PREPROD_REST: &str = "https://api.qfex.io";
/// The pre-production market-data socket.
pub const PREPROD_MDS: &str = "wss://mds.qfex.io";

/// The production Trade WebSocket: the trade AsyncAPI's production server.
pub const PRODUCTION_TRADE: &str = "wss://trade.qfex.com";
/// The production REST origin: the REST reference's one server.
pub const PRODUCTION_REST: &str = "https://api.qfex.com";
/// The production market-data socket, which this adapter's public captures were read from.
pub const PRODUCTION_MDS: &str = "wss://mds.qfex.com";

/// The hold every order but an ALO waits in ([architecture](https://docs.qfex.com/qfex/architecture)).
pub const SPEED_BUMP: Duration = Duration::from_millis(500);

/// The largest page size QFEX documents.
const MAX_PAGE_SIZE: u16 = 1000;

/// The mode's schema entry.
pub const EXEC_MODE_FIELD: FieldSpec = FieldSpec {
    key: KEY_EXEC_MODE,
    scope: ConfigScope::Process,
    unit: FieldUnit::Dimensionless,
    doc: "absent (market data only, no order entry); preprod_probe: order entry against \
          QFEX's pre-production origins or a loopback stub only (decision 0132); or \
          production_probe: the same against QFEX's production origins or a loopback stub \
          only, for the owner-assisted probe alone (decision 0143)",
};

/// The page size's schema entry.
pub const PAGE_SIZE_FIELD: FieldSpec = FieldSpec {
    key: KEY_PAGE_SIZE,
    scope: ConfigScope::Account,
    unit: FieldUnit::Dimensionless,
    doc: "either probe mode only, required: the get_user_orders page size, 1 to 1000",
};

/// The resync deadline's schema entry.
pub const RESYNC_TIMEOUT_FIELD: FieldSpec = FieldSpec {
    key: KEY_RESYNC_TIMEOUT,
    scope: ConfigScope::Account,
    unit: FieldUnit::Duration,
    doc: "either probe mode only, required: the resync's own deadline as positive <n>ms or \
          <n>s",
};

/// The request deadline's schema entry.
pub const RPC_TIMEOUT_FIELD: FieldSpec = FieldSpec {
    key: KEY_RPC_TIMEOUT,
    scope: ConfigScope::Account,
    unit: FieldUnit::Duration,
    doc: "either probe mode only, required: each request's deadline as <n>ms or <n>s, longer \
          than the 500 ms speed bump",
};

/// The environment a probe mode names: its guard admits that environment's origins, or a
/// loopback stub, and nothing else.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum Environment {
    /// [`PREPROD_PROBE`]: QFEX's pre-production environment (decision 0132 item 6).
    PreProduction,
    /// [`PRODUCTION_PROBE`]: QFEX's production environment (decision 0143 item 2).
    Production,
}

impl Environment {
    /// The [`KEY_EXEC_MODE`] value that names it.
    pub fn mode(self) -> &'static str {
        match self {
            Environment::PreProduction => PREPROD_PROBE,
            Environment::Production => PRODUCTION_PROBE,
        }
    }

    /// Its trade, REST and market-data origins, which its mode's guard admits exactly.
    pub fn origins(self) -> [&'static str; 3] {
        match self {
            Environment::PreProduction => [PREPROD_TRADE, PREPROD_REST, PREPROD_MDS],
            Environment::Production => [PRODUCTION_TRADE, PRODUCTION_REST, PRODUCTION_MDS],
        }
    }

    /// The side of the guard its own origins are on.
    fn side(self) -> Origins {
        match self {
            Environment::PreProduction => Origins::PreProduction,
            Environment::Production => Origins::Production,
        }
    }

    /// What its guard says of each refused origin: a mixed set, a host of the other
    /// environment's domain (and that domain), and any other origin.
    fn refusals(self) -> Refusals {
        match self {
            Environment::PreProduction => Refusals {
                mixed: "mixes a loopback origin with a pre-production one",
                domain: "qfex.com",
                elsewhere: "a production qfex.com host",
                other: "neither its pre-production origin nor a loopback origin",
            },
            Environment::Production => Refusals {
                mixed: "mixes a loopback origin with a production one",
                domain: "qfex.io",
                elsewhere: "a pre-production qfex.io host",
                other: "neither its production origin nor a loopback origin",
            },
        }
    }
}

/// What one mode's guard refuses an origin with.
struct Refusals {
    mixed: &'static str,
    /// The other environment's domain, every host of which is refused.
    domain: &'static str,
    elsewhere: &'static str,
    other: &'static str,
}

/// Which side of the guard a probe configuration's origins are on.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum Origins {
    /// Every origin is exactly its pre-production origin, under [`PREPROD_PROBE`].
    PreProduction,
    /// Every origin is exactly its production origin, under [`PRODUCTION_PROBE`].
    Production,
    /// Every origin is a loopback address: a test stub, under either mode. A probe refuses to
    /// start with a proxy that is not direct (decision 0132 item 6).
    Loopback,
}

/// A configuration the guard admitted under [`PREPROD_PROBE`] or [`PRODUCTION_PROBE`], and its
/// settings.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct Probe {
    environment: Environment,
    origins: Origins,
    page_size: u16,
    resync_timeout: Duration,
    rpc_timeout: Duration,
}

impl Probe {
    /// The environment its mode names, whichever side of the guard its origins are on.
    pub fn environment(&self) -> Environment {
        self.environment
    }

    /// Which side of the guard its origins are on.
    pub fn origins(&self) -> Origins {
        self.origins
    }

    /// The `get_user_orders` page size, 1 to 1000.
    pub fn page_size(&self) -> u16 {
        self.page_size
    }

    /// The resync's own deadline.
    pub fn resync_timeout(&self) -> Duration {
        self.resync_timeout
    }

    /// Each request's deadline, longer than [`SPEED_BUMP`].
    pub fn rpc_timeout(&self) -> Duration {
        self.rpc_timeout
    }
}

/// The mode `cfg` sets: `None` when [`KEY_EXEC_MODE`] is absent, the guarded probe when it is
/// [`PREPROD_PROBE`] or [`PRODUCTION_PROBE`]. Refused, naming the key and never its value, for
/// any other mode, an origin the mode's guard refuses, origins that mix the two sides, and a
/// setting missing or out of range. The factory asks this before it plans or builds anything.
pub fn mode(cfg: &VenueConfig) -> Result<Option<Probe>, ConfigError> {
    let environment = match cfg.get(KEY_EXEC_MODE) {
        None => return Ok(None),
        Some(PREPROD_PROBE) => Environment::PreProduction,
        Some(PRODUCTION_PROBE) => Environment::Production,
        Some(_) => {
            return Err(ConfigError::Invalid {
                key: KEY_EXEC_MODE,
                reason: "expected preprod_probe, production_probe, or no value",
            });
        }
    };
    let origins = guard(cfg, environment)?;
    let page_size = page_size(cfg)?;
    let resync_timeout = duration(cfg, KEY_RESYNC_TIMEOUT)?;
    let rpc_timeout = duration(cfg, KEY_RPC_TIMEOUT)?;
    if rpc_timeout <= SPEED_BUMP {
        return Err(ConfigError::Invalid {
            key: KEY_RPC_TIMEOUT,
            reason: "must be longer than QFEX's 500 ms speed bump",
        });
    }
    Ok(Some(Probe {
        environment,
        origins,
        page_size,
        resync_timeout,
        rpc_timeout,
    }))
}

/// The origins the probe's own connections take (decision 0132 item 7): read only from a
/// configuration under [`PREPROD_PROBE`] or [`PRODUCTION_PROBE`] that the mode's guard admits
/// and whose provider account decision 0122 item 1 admits, so each of them passes both before
/// it opens, as an order-entry session's do.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct Connections<'a> {
    /// The trade socket's URL, as an order-entry session opens it (its key a redaction span).
    pub trade: WireUrl,
    /// The REST origin, without a trailing `/`.
    pub rest: &'a str,
    /// The market-data socket's origin, without a trailing `/`.
    pub mds: &'a str,
    /// The environment the mode names.
    pub environment: Environment,
    /// Which side of the guard they are on.
    pub origins: Origins,
    /// The consumer's account key the configuration binds its provider account to.
    pub account: AccountKey,
}

/// The probe's own connections under `cfg`: refused unless [`KEY_EXEC_MODE`] is a probe mode
/// and [`mode`] admits it, and unless its provider account and account key are explicit and
/// valid ([`auth::account_key`], decision 0122 item 1). Refusals name the setting, never its
/// value.
pub fn connections(cfg: &VenueConfig) -> Result<Connections<'_>, ConfigError> {
    let probe = mode(cfg)?.ok_or(ConfigError::Missing(KEY_EXEC_MODE))?;
    let account = auth::account_key(cfg)?;
    let trade = auth::opening_url(cfg)?;
    Ok(Connections {
        trade,
        rest: origin(cfg, KEY_REST_URL)?,
        mds: origin(cfg, KEY_MDS_URL)?,
        environment: probe.environment,
        origins: probe.origins,
        account,
    })
}

/// The three origins, each on one side of `environment`'s guard, all on the same side.
fn guard(cfg: &VenueConfig, environment: Environment) -> Result<Origins, ConfigError> {
    let sockets = ["ws://", "wss://"];
    let [trade, rest, mds] = environment.origins();
    let checked = [
        (auth::TRADE_URL, trade, sockets),
        (KEY_REST_URL, rest, ["http://", "https://"]),
        (KEY_MDS_URL, mds, sockets),
    ];
    let mut first = None;
    for (key, exact, schemes) in checked {
        let side = side(key, origin(cfg, key)?, environment, exact, schemes)?;
        match first {
            None => first = Some(side),
            Some(seen) if seen == side => {}
            Some(_) => {
                return Err(ConfigError::Invalid {
                    key,
                    reason: environment.refusals().mixed,
                });
            }
        }
    }
    Ok(first.expect("three origins were checked"))
}

/// The origin `key` names, one trailing `/` ignored.
fn origin<'a>(cfg: &'a VenueConfig, key: &'static str) -> Result<&'a str, ConfigError> {
    let raw = cfg.get(key).ok_or(ConfigError::Missing(key))?;
    Ok(raw.strip_suffix('/').unwrap_or(raw))
}

/// Which side of `environment`'s guard `origin` is on: exactly `exact`, its origin in that
/// environment, or a loopback address under one of `schemes` with an optional port and nothing
/// else.
fn side(
    key: &'static str,
    origin: &str,
    environment: Environment,
    exact: &str,
    schemes: [&str; 2],
) -> Result<Origins, ConfigError> {
    if origin == exact {
        return Ok(environment.side());
    }
    let says = environment.refusals();
    let refused = |reason| ConfigError::Invalid { key, reason };
    let authority = schemes
        .iter()
        .find_map(|scheme| origin.strip_prefix(scheme))
        .filter(|authority| valid_host_port(authority));
    let Some(authority) = authority else {
        return Err(refused(says.other));
    };
    let host = match authority.strip_prefix('[') {
        Some(rest) => rest.split_once(']').map_or("", |(host, _)| host),
        None => authority.split(':').next().unwrap_or(""),
    };
    let loopback = match authority.starts_with('[') {
        true => host.parse::<Ipv6Addr>().is_ok_and(|ip| ip.is_loopback()),
        false => host.parse::<Ipv4Addr>().is_ok_and(|ip| ip.is_loopback()),
    };
    if loopback {
        return Ok(Origins::Loopback);
    }
    let host = host.to_ascii_lowercase();
    let elsewhere = host
        .strip_suffix(says.domain)
        .is_some_and(|sub| sub.is_empty() || sub.ends_with('.'));
    if elsewhere {
        return Err(refused(says.elsewhere));
    }
    Err(refused(says.other))
}

/// [`KEY_PAGE_SIZE`]: a whole number from 1 to 1000, written as it prints.
fn page_size(cfg: &VenueConfig) -> Result<u16, ConfigError> {
    let raw = cfg
        .get(KEY_PAGE_SIZE)
        .ok_or(ConfigError::Missing(KEY_PAGE_SIZE))?;
    match raw.parse::<u16>() {
        Ok(n) if (1..=MAX_PAGE_SIZE).contains(&n) && n.to_string() == raw => Ok(n),
        _ => Err(ConfigError::Invalid {
            key: KEY_PAGE_SIZE,
            reason: "expected a whole number from 1 to 1000",
        }),
    }
}

/// A positive whole `<n>ms` or `<n>s`, as `qfex.rest_timeout` is written.
fn duration(cfg: &VenueConfig, key: &'static str) -> Result<Duration, ConfigError> {
    let raw = cfg.get(key).ok_or(ConfigError::Missing(key))?;
    let (number, unit) = if let Some(n) = raw.strip_suffix("ms") {
        (n, 1u64)
    } else if let Some(n) = raw.strip_suffix('s') {
        (n, 1000u64)
    } else {
        ("", 0)
    };
    number
        .parse::<u64>()
        .ok()
        .filter(|n| *n > 0 && number.bytes().all(|b| b.is_ascii_digit()))
        .and_then(|n| n.checked_mul(unit))
        .map(Duration::from_millis)
        .ok_or(ConfigError::Invalid {
            key,
            reason: "expected positive whole <n>ms or <n>s",
        })
}
