//! A venue as a whole: its factory, its configuration schema, and the gateway traits
//! (decisions 0002, 0003 and 0014, design §4.7, §4.8).
//!
//! A [`VenueFactory`] is the one value a venue crate exports. From the consumer's
//! configuration it states the venue's capabilities, plans market-data connections, and builds
//! the codecs the runtime drives. Nothing outside the venue crates and the registry names a
//! venue; everything else reads [`VenueCaps`].
//!
//! [`OrderGateway`] is what submits commands: the live gateway (runtime, exec codec and
//! signer), the simulated venue, and a [`ManagedGateway`] for a venue reachable only through a
//! vendor SDK that owns its own socket (journaled at the event level; using one needs a
//! decision record first, 0002).

use core::fmt;
use std::collections::{BTreeMap, BTreeSet};

use crate::caps::VenueCaps;
use crate::codec::{EncodeCtx, EncodeReceipt, ExecCodec, MdCodec, SpecTable, Subscription};
use crate::command::{NotSentReason, VenueCommand};
use crate::event::{RpcId, StreamId};
use crate::ids::{AccountKey, InstrumentId};

/// Where a configuration key lives.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum ConfigScope {
    /// One market's settings.
    Market,
    /// Shared by every market on one account (transport knobs, account ids).
    Account,
    /// One process's settings.
    Process,
}

/// The unit of a configuration value. Mechanism parameters are in bps, sigma multiples or
/// dimensionless units, never raw ticks, so one value means the same on every grid.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum FieldUnit {
    Bps,
    SigmaMult,
    Dimensionless,
    PerTick,
    /// A count of ticks.
    InTicks,
    Count,
    Duration,
}

/// One key of a venue's configuration schema.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct FieldSpec {
    pub key: &'static str,
    pub scope: ConfigScope,
    pub unit: FieldUnit,
    /// What the key does.
    pub doc: &'static str,
}

/// A venue's configuration, as text values by key; the consumer supplies it.
#[derive(Clone, Eq, PartialEq, Hash, Debug, Default)]
pub struct VenueConfig {
    values: BTreeMap<String, String>,
}

impl VenueConfig {
    /// No values.
    pub fn new() -> VenueConfig {
        VenueConfig::default()
    }

    /// Sets `key` to `value`, returning the value it replaced.
    pub fn insert(&mut self, key: &str, value: &str) -> Option<String> {
        self.values.insert(key.to_owned(), value.to_owned())
    }

    /// The value of `key`.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.values.get(key).map(String::as_str)
    }
}

/// Why a venue's configuration was refused.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum ConfigError {
    /// A required key is missing.
    Missing(&'static str),
    /// A key's value is invalid; says why.
    Invalid {
        key: &'static str,
        reason: &'static str,
    },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::Missing(key) => write!(f, "missing configuration key {key}"),
            ConfigError::Invalid { key, reason } => {
                write!(f, "invalid configuration key {key}: {reason}")
            }
        }
    }
}

impl std::error::Error for ConfigError {}

/// Why a venue could not plan connections, subscribe or build a codec.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum VenueError {
    /// The configuration was refused.
    Config(ConfigError),
    /// The instrument is missing from the spec table, so the venue cannot spell it.
    UnknownInstrument(InstrumentId),
}

impl fmt::Display for VenueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VenueError::Config(err) => write!(f, "venue configuration refused: {err}"),
            VenueError::UnknownInstrument(inst) => {
                write!(f, "instrument {} is not in the spec table", inst.get())
            }
        }
    }
}

impl std::error::Error for VenueError {}

impl From<ConfigError> for VenueError {
    fn from(err: ConfigError) -> VenueError {
        VenueError::Config(err)
    }
}

/// One market-data connection the runtime opens: its stream, its URL and what it carries.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct EndpointPlan {
    pub stream: StreamId,
    pub url: String,
    pub subs: Vec<Subscription>,
}

/// The one value a venue crate exports.
///
/// Instrument discovery and the legacy-symbol hook (FBC-ahf), and the credential-bearing calls
/// (`exec_codec`'s credentials and `test_connection`, under `src/auth`; FBC-b3b) are not
/// declared yet.
pub trait VenueFactory: Sync + 'static {
    /// The venue's name as FBC spells it ("PARADEX").
    fn id(&self) -> &'static str;
    /// Every configuration key the venue reads, with its scope and unit.
    fn config_schema(&self) -> &'static [FieldSpec];
    /// What the venue can do under `cfg`.
    fn caps(&self, cfg: &VenueConfig) -> Result<VenueCaps, ConfigError>;
    /// How `subs` spread over connections, with each instrument spelled as `specs` says (a
    /// venue may put its symbols in the URL). `Err` names an instrument missing from `specs`.
    fn plan_md(
        &self,
        cfg: &VenueConfig,
        specs: &SpecTable,
        subs: &BTreeSet<Subscription>,
    ) -> Result<Vec<EndpointPlan>, VenueError>;
    /// A market-data codec for one connection epoch of `ep`.
    fn md_codec(&self, cfg: &VenueConfig, ep: &EndpointPlan) -> Box<dyn MdCodec>;
    /// An order-entry codec, or `None` for a market-data-only venue.
    fn exec_codec(&self, cfg: &VenueConfig) -> Option<Result<Box<dyn ExecCodec>, VenueError>>;
}

/// What submitting a command gave: its request id, and the nonces it used or why it was not
/// sent.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct SubmitHandle {
    pub rpc: RpcId,
    pub receipt: Result<EncodeReceipt, NotSentReason>,
}

/// Submits commands for accounts: the live gateway, the simulated venue, a managed gateway.
///
/// Every order command reaches a gateway only through `fbc-oms`, after its caps and the kill
/// switch (0013 rule 2, 0012). Nothing implements this trait yet; FBC-ob2 makes that path the
/// only one (an OMS-issued authorization, or a gateway built only inside `fbc-oms`) before a
/// live gateway exists.
pub trait OrderGateway {
    fn submit(&mut self, acct: AccountKey, cmd: VenueCommand, ctx: &EncodeCtx) -> SubmitHandle;
}

/// A gateway for a venue reachable only through a vendor SDK that owns its own socket. It emits
/// the same execution events and is journaled at the event level, so its replay is event-level,
/// not frame-level. Using one needs its own decision record first (0002).
pub trait ManagedGateway: OrderGateway + Send {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_venue_config_holds_text_values_by_key() {
        let mut cfg = VenueConfig::new();
        assert_eq!(cfg.get("a"), None);
        assert_eq!(cfg.insert("a", "1"), None);
        assert_eq!(cfg.insert("a", "2"), Some("1".to_owned()));
        assert_eq!(cfg.get("a"), Some("2"));
    }

    #[test]
    fn config_and_venue_errors_name_the_key() {
        let missing = ConfigError::Missing("url");
        assert_eq!(missing.to_string(), "missing configuration key url");
        let invalid = ConfigError::Invalid {
            key: "ttl",
            reason: "not a duration",
        };
        assert_eq!(
            invalid.to_string(),
            "invalid configuration key ttl: not a duration"
        );
        let venue: VenueError = invalid.into();
        assert_eq!(
            venue.to_string(),
            "venue configuration refused: invalid configuration key ttl: not a duration"
        );
        assert_eq!(
            VenueError::UnknownInstrument(InstrumentId::new(9)).to_string(),
            "instrument 9 is not in the spec table"
        );
    }
}
