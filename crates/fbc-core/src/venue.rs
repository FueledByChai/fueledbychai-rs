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
use crate::codec::{
    EncodeCtx, EncodeReceipt, ExecCodec, MdCodec, SpecTable, Subscription, WireUrl,
};
use crate::command::{NotSentReason, VenueCommand};
use crate::event::{RpcId, StreamId};
use crate::ids::{AccountKey, InstrumentId};
use crate::stamps::PathStamps;

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

/// A venue's configuration, as text values by key; the consumer supplies it. Account- and
/// process-scoped keys hold one value each ([`insert`](VenueConfig::insert)); market-scoped
/// keys ([`ConfigScope::Market`]) hold one value per instrument
/// ([`insert_market`](VenueConfig::insert_market)), so markets sharing a connection keep
/// their own settings.
///
/// Its `Debug` shows each key and its value's length only: a value can be a credential-bearing
/// URL or a private account id (0009).
#[derive(Clone, Eq, PartialEq, Hash, Default)]
pub struct VenueConfig {
    values: BTreeMap<String, String>,
    markets: BTreeMap<InstrumentId, BTreeMap<String, String>>,
}

/// Configuration values by key, each shown by its length only.
struct ShownValues<'a>(&'a BTreeMap<String, String>);

/// A value's length, standing in for the value.
struct Len(usize);

impl fmt::Debug for Len {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<{} bytes>", self.0)
    }
}

impl fmt::Debug for ShownValues<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map()
            .entries(self.0.iter().map(|(key, value)| (key, Len(value.len()))))
            .finish()
    }
}

impl fmt::Debug for VenueConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let markets: BTreeMap<&InstrumentId, ShownValues<'_>> = self
            .markets
            .iter()
            .map(|(inst, values)| (inst, ShownValues(values)))
            .collect();
        f.debug_struct("VenueConfig")
            .field("values", &ShownValues(&self.values))
            .field("markets", &markets)
            .finish()
    }
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

    /// Sets market-scoped `key` to `value` for `inst` alone, returning the value it replaced.
    pub fn insert_market(&mut self, inst: InstrumentId, key: &str, value: &str) -> Option<String> {
        self.markets
            .entry(inst)
            .or_default()
            .insert(key.to_owned(), value.to_owned())
    }

    /// The value of market-scoped `key` for `inst`.
    pub fn get_market(&self, inst: InstrumentId, key: &str) -> Option<&str> {
        self.markets.get(&inst)?.get(key).map(String::as_str)
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
    /// The venue does not offer this feed ([`MdCaps`](crate::MdCaps) says so): nothing is
    /// planned or sent for it.
    UnsupportedFeed(Subscription),
}

impl fmt::Display for VenueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VenueError::Config(err) => write!(f, "venue configuration refused: {err}"),
            VenueError::UnknownInstrument(inst) => {
                write!(f, "instrument {} is not in the spec table", inst.get())
            }
            VenueError::UnsupportedFeed(sub) => write!(
                f,
                "instrument {} has no {:?} feed on this venue",
                sub.inst.get(),
                sub.feed
            ),
        }
    }
}

impl std::error::Error for VenueError {}

impl From<ConfigError> for VenueError {
    fn from(err: ConfigError) -> VenueError {
        VenueError::Config(err)
    }
}

/// One market-data endpoint: its stream, how its data arrives, and what it carries. The runtime
/// builds one [`MdCodec`] per endpoint (per connection epoch for a socket), calls its `on_open`
/// once the endpoint is ready, then `subscribe` with `subs`.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct EndpointPlan {
    /// Names the endpoint in effects, the journal and the codec's timers and requests.
    pub stream: StreamId,
    pub transport: MdTransport,
    pub subs: Vec<Subscription>,
}

/// How a market-data endpoint's data arrives.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum MdTransport {
    /// A streaming connection the runtime opens at `url`; `on_open` is called when it is open.
    Socket { url: WireUrl },
    /// No connection: a venue that publishes these feeds only over REST
    /// ([`FeedSource::Poll`](crate::FeedSource::Poll)). The runtime opens nothing and calls
    /// `on_open` as soon as the codec is built; the codec gets its data only through the
    /// [`Effect::Http`](crate::Effect::Http) requests it asks for under `base_url`, paced by its
    /// own [`Effect::Timer`](crate::Effect::Timer)s. Its `keepalive` is `None`, and an
    /// [`Effect::Send`](crate::Effect::Send) or [`Effect::Reconnect`](crate::Effect::Reconnect)
    /// naming its stream is a codec defect the runtime refuses.
    Poll { base_url: WireUrl },
}

/// One order-entry connection the runtime opens for an account session; when it is open the
/// runtime calls [`ExecCodec::on_open`] with its stream.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct ExecEndpoint {
    pub stream: StreamId,
    pub url: WireUrl,
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
    /// How `subs` spread over endpoints: connections, and for feeds the venue offers only over
    /// REST, connectionless poll endpoints ([`MdTransport`]). Each instrument is spelled as
    /// `specs` says (a venue may put its symbols in the URL). `Err` names an instrument missing
    /// from `specs`, or the configuration refused.
    fn plan_md(
        &self,
        cfg: &VenueConfig,
        specs: &SpecTable,
        subs: &BTreeSet<Subscription>,
    ) -> Result<Vec<EndpointPlan>, VenueError>;
    /// A market-data codec for `ep`: for a socket, one connection epoch of it.
    fn md_codec(&self, cfg: &VenueConfig, ep: &EndpointPlan) -> Box<dyn MdCodec>;
    /// The order-entry connections to open under `cfg`; empty for a venue whose order entry is
    /// HTTP only, or that has none.
    fn plan_exec(&self, cfg: &VenueConfig) -> Result<Vec<ExecEndpoint>, VenueError>;
    /// An order-entry codec, or `None` for a market-data-only venue. It writes client ids in
    /// the format its capabilities declare ([`OrderCaps::client_id`](crate::OrderCaps)), the one
    /// source the runtime also decodes with.
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
///
/// `t` carries the command's path marks (0034): a live gateway marks
/// [`PathStage::Encode`](crate::PathStage::Encode) around its call to [`ExecCodec::encode`],
/// which it hands `t` to mark its signer calls, and its runtime marks
/// [`PathStage::Write`](crate::PathStage::Write) around the socket write.
pub trait OrderGateway {
    fn submit(
        &mut self,
        acct: AccountKey,
        cmd: VenueCommand,
        ctx: &EncodeCtx,
        t: &mut PathStamps<'_>,
    ) -> SubmitHandle;
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
    fn market_scoped_values_are_kept_per_instrument() {
        let (btc, eth) = (InstrumentId::new(1), InstrumentId::new(2));
        let mut cfg = VenueConfig::new();
        assert_eq!(cfg.insert_market(btc, "book", "deltas"), None);
        assert_eq!(cfg.insert_market(eth, "book", "interactive_deltas"), None);
        // The second market's value does not overwrite the first's.
        assert_eq!(cfg.get_market(btc, "book"), Some("deltas"));
        assert_eq!(cfg.get_market(eth, "book"), Some("interactive_deltas"));
        assert_eq!(
            cfg.insert_market(btc, "book", "snapshots"),
            Some("deltas".to_owned())
        );
        assert_eq!(cfg.get_market(btc, "book"), Some("snapshots"));
        // A market value is not an account value, and a market without one has none.
        assert_eq!(cfg.get("book"), None);
        assert_eq!(cfg.get_market(InstrumentId::new(3), "book"), None);
        assert_eq!(cfg.get_market(btc, "other"), None);
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

    #[test]
    fn a_venue_config_debug_shows_keys_and_value_lengths_only() {
        // A consumer's configuration can hold a credential-bearing URL or an account address;
        // a diagnostic that formats it shows which keys are set, not what they hold.
        let secret = "SYNTHETIC-CONFIG-SECRET";
        let mut cfg = VenueConfig::new();
        cfg.insert("x.url", &format!("https://{secret}@venue.invalid"));
        cfg.insert_market(InstrumentId::new(7), "x.account", secret);
        let shown = format!("{cfg:?}");
        assert!(!shown.contains(secret), "{shown}");
        assert!(
            shown.contains("x.url") && shown.contains("x.account"),
            "{shown}"
        );
        assert!(
            shown.contains(&format!("<{} bytes>", secret.len())),
            "{shown}"
        );
    }

    #[test]
    fn endpoint_plans_never_show_a_credential_in_their_urls() {
        let secret = "SYNTHETIC-URL-TOKEN";
        let plain = WireUrl::plain(format!("https://u:{secret}@venue.invalid/x?key={secret}"));
        let start = u32::try_from("wss://venue.invalid/".len()).unwrap();
        let span = start..start + u32::try_from(secret.len()).unwrap();
        let text = format!("wss://venue.invalid/{secret}");
        let spanned = WireUrl::redacted(text, vec![span]).unwrap();
        let exec = ExecEndpoint {
            stream: StreamId(1),
            url: spanned.clone(),
        };
        let md = EndpointPlan {
            stream: StreamId(0),
            transport: MdTransport::Socket { url: plain },
            subs: vec![],
        };
        let poll = MdTransport::Poll { base_url: spanned };
        for shown in [format!("{exec:?}"), format!("{md:?}"), format!("{poll:?}")] {
            assert!(!shown.contains(secret), "{shown}");
            assert!(shown.contains("venue.invalid"), "{shown}");
        }
    }

    #[test]
    fn an_unsupported_feed_names_the_instrument_and_the_feed() {
        let sub = Subscription {
            inst: InstrumentId::new(7),
            feed: crate::codec::Feed::Index,
        };
        let shown = VenueError::UnsupportedFeed(sub).to_string();
        assert_eq!(shown, "instrument 7 has no Index feed on this venue");
    }
}
