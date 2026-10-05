//! A venue as a whole: its factory, its configuration schema, and the gateway traits
//! (decisions 0002, 0003 and 0014, design §4.7, §4.8).
//!
//! A [`VenueFactory`] is the one value a venue crate exports. From the consumer's
//! configuration it states the venue's capabilities, plans market-data connections, and builds
//! the codecs the runtime drives. It also discovers the venue's instruments, as an [`HttpPlan`]
//! (REST requests as effects, and a parser that reads their answers inside a
//! [`DecodeScope`]), and reads the Java-era tickers through its FBC common-symbol rule (design
//! §4.4; the resolution itself is [`resolve`](crate::resolve)). Nothing outside the venue crates
//! and the registry names a venue; everything else reads [`VenueCaps`].
//!
//! Credentials reach a venue only as [`Secrets`] (`src/auth`, 0009): an order-entry codec takes
//! them, and [`VenueFactory::test_connection`] proves them with an [`HttpPlan`] whose result is
//! an [`AccountSummary`] (decision 0043).
//!
//! [`OrderGateway`] is what submits commands: the live gateway (runtime, exec codec and
//! signer), the simulated venue, and a [`ManagedGateway`] for a venue reachable only through a
//! vendor SDK that owns its own socket (journaled at the event level; using one needs a
//! decision record first, 0002).

use core::fmt;
use std::collections::{BTreeMap, BTreeSet};

use crate::auth::Secrets;
use crate::caps::VenueCaps;
use crate::codec::{
    DecodeError, Effect, Effects, EncodeCtx, EncodeReceipt, ExecCodec, HttpFailure, HttpResponse,
    HttpTag, MdCodec, SpecTable, Subscription, WireUrl,
};
use crate::command::{NotSentReason, VenueCommand};
use crate::event::{RpcId, StreamId};
use crate::ids::{AccountKey, IdError, InstrumentId};
use crate::resolve::{AssetKey, InstrumentSpecDraft, SymbolError};
use crate::scope::DecodeScope;
use crate::stamps::PathStamps;
use crate::units::Money;

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
    /// The adapter discovers no instruments: the consumer states them.
    NoDiscovery,
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
            VenueError::NoDiscovery => f.write_str("the venue adapter discovers no instruments"),
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

/// What a venue says about the account a set of credentials reaches, for the consumer's Test
/// Connection. Both fields are private account data (0009): its `Debug` shows the account by
/// length and the equity only by whether the venue reported one, so a log of it shows that the
/// credentials work and nothing of whose they are or what the account holds.
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct AccountSummary {
    /// The account as the venue names it (an address, an account id).
    pub account: String,
    /// The account's equity, where the venue reports one.
    pub equity: Option<Money>,
}

impl fmt::Debug for AccountSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let equity = self.equity.as_ref().map(|_| format_args!("<redacted>"));
        f.debug_struct("AccountSummary")
            .field("account", &Len(self.account.len()))
            .field("equity", &equity)
            .finish()
    }
}

/// Why an [`HttpPlan`] was refused, or its answers could not be read.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum PlanError {
    /// A plan's effect is not an HTTP request: a plan asks for requests and nothing else.
    NotHttp,
    /// Two of a plan's requests carry this tag, so their answers could not be told apart.
    DuplicateTag(HttpTag),
    /// The answers are not one per request, in the plan's order, under the requests' tags.
    Answers,
    /// The request tagged `tag` got no response.
    Http { tag: HttpTag, failure: HttpFailure },
    /// The request tagged `tag` was answered with a status other than 2xx.
    Status { tag: HttpTag, status: u16 },
    /// An answer is not what the venue documents; names the part.
    Decode(DecodeError),
    /// An answer leaves out a field the result requires, named here: the whole answer is
    /// refused, and nothing is guessed in its place.
    Missing(&'static str),
}

impl fmt::Display for PlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlanError::NotHttp => f.write_str("a plan's effect is not an HTTP request"),
            PlanError::DuplicateTag(tag) => write!(f, "two requests carry tag {}", tag.0),
            PlanError::Answers => f.write_str("the answers do not match the plan's requests"),
            PlanError::Http { tag, failure } => {
                write!(f, "request {} got no response: {failure:?}", tag.0)
            }
            PlanError::Status { tag, status } => {
                write!(f, "request {} was answered with status {status}", tag.0)
            }
            PlanError::Decode(err) => write!(f, "answer refused: {err}"),
            PlanError::Missing(field) => {
                write!(f, "answer refused: required field {field} is missing")
            }
        }
    }
}

impl std::error::Error for PlanError {}

impl From<DecodeError> for PlanError {
    fn from(err: DecodeError) -> PlanError {
        PlanError::Decode(err)
    }
}

impl From<IdError> for PlanError {
    fn from(err: IdError) -> PlanError {
        PlanError::Decode(err.into())
    }
}

/// The answer the runtime got for one request of an [`HttpPlan`]: the response, or why none
/// came.
pub type HttpAnswer<'a> = (HttpTag, Result<HttpResponse<'a>, HttpFailure>);

/// Reads the 2xx responses to a plan's requests, in the plan's order, inside a decode scope.
type Parser<T> =
    Box<dyn FnOnce(&[HttpResponse<'_>], &DecodeScope<'_>) -> Result<T, PlanError> + Send>;

/// A factory call that needs REST answers, as data (decision 0002): the requests, as
/// [`Effect::Http`]s the runtime makes through the consumer's proxy, and the parser that reads
/// their answers. The parser runs inside a [`DecodeScope`], the only builder of a
/// [`VenueSymbol`](crate::VenueSymbol), and does no IO and reads no clock.
pub struct HttpPlan<T> {
    requests: Vec<Effect>,
    parser: Parser<T>,
}

impl<T> fmt::Debug for HttpPlan<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpPlan")
            .field("requests", &self.requests)
            .finish_non_exhaustive()
    }
}

impl<T> HttpPlan<T> {
    /// A plan of `requests` read by `parser`. Refused when an effect is not an
    /// [`Effect::Http`] ([`PlanError::NotHttp`]) or two share a tag.
    pub fn new(
        mut requests: Effects,
        parser: impl FnOnce(&[HttpResponse<'_>], &DecodeScope<'_>) -> Result<T, PlanError>
        + Send
        + 'static,
    ) -> Result<HttpPlan<T>, PlanError> {
        let requests = requests.take();
        let mut tags = BTreeSet::new();
        for effect in &requests {
            let Effect::Http { tag, .. } = effect else {
                return Err(PlanError::NotHttp);
            };
            if !tags.insert(tag.0) {
                return Err(PlanError::DuplicateTag(*tag));
            }
        }
        Ok(HttpPlan {
            requests,
            parser: Box::new(parser),
        })
    }

    /// The requests to make, every one an [`Effect::Http`].
    pub fn requests(&self) -> &[Effect] {
        &self.requests
    }

    /// Reads `answers`, one per request in the plan's order, inside `scope`. Refused before the
    /// parser runs when the answers do not match the requests, a request got no response, or a
    /// response's status is not 2xx; otherwise the parser's result.
    pub fn parse(
        self,
        answers: &[HttpAnswer<'_>],
        scope: &DecodeScope<'_>,
    ) -> Result<T, PlanError> {
        if answers.len() != self.requests.len() {
            return Err(PlanError::Answers);
        }
        let mut responses = Vec::with_capacity(answers.len());
        for (effect, (tag, answer)) in self.requests.iter().zip(answers) {
            if !matches!(effect, Effect::Http { tag: asked, .. } if asked == tag) {
                return Err(PlanError::Answers);
            }
            let resp = answer.map_err(|failure| PlanError::Http { tag: *tag, failure })?;
            if !(200..300).contains(&resp.status) {
                let status = resp.status;
                return Err(PlanError::Status { tag: *tag, status });
            }
            responses.push(resp);
        }
        (self.parser)(&responses, scope)
    }
}

/// The one value a venue crate exports.
pub trait VenueFactory: Sync + 'static {
    /// The venue's name as FBC spells it ("PARADEX").
    fn id(&self) -> &'static str;
    /// Every configuration key the venue reads, with its scope and unit.
    fn config_schema(&self) -> &'static [FieldSpec];
    /// What the venue can do under `cfg`.
    fn caps(&self, cfg: &VenueConfig) -> Result<VenueCaps, ConfigError>;
    /// The asset key of the instrument a Java-era ticker names on this venue, by FBC's rule for
    /// it (Paradex `X/USDT` is `X-USD-PERP`, Hibachi `X/USDT` is `X/USDT-P`, Binance `X/USDT` is
    /// `XUSDT`): the key the venue's discovery states for that instrument, which the consumer's
    /// legacy reader then resolves (design §4.4). [`SymbolError::NoRule`] for a venue the Java
    /// library never traded.
    fn parse_fbc_common_symbol(&self, s: &str) -> Result<AssetKey, SymbolError>;
    /// The venue's instruments: REST requests as effects, and a parser that reads their answers
    /// into one [`InstrumentSpecDraft`] per instrument inside a [`DecodeScope`], refusing an
    /// answer that leaves out a required field ([`PlanError::Missing`]). `Err` when the
    /// configuration is refused, or [`VenueError::NoDiscovery`] for an adapter that discovers
    /// nothing.
    fn discover(&self, cfg: &VenueConfig)
    -> Result<HttpPlan<Vec<InstrumentSpecDraft>>, VenueError>;
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
    /// An order-entry codec for the account `creds` reach, or `None` for a market-data-only
    /// venue, which drops them unread. It writes client ids in the format its capabilities
    /// declare ([`OrderCaps::client_id`](crate::OrderCaps)), the one source the runtime also
    /// decodes with. `Err` names a credential key missing from `creds`, or the configuration
    /// refused ([`ConfigError`]); it never carries a credential's value.
    fn exec_codec(
        &self,
        cfg: &VenueConfig,
        creds: Secrets,
    ) -> Option<Result<Box<dyn ExecCodec>, VenueError>>;
    /// A plan that proves `creds` against the venue and ends in an [`AccountSummary`], for the
    /// consumer's Test Connection: one round of requests, built here and carrying the
    /// credentials they need, and a parser that reads the account from their 2xx answers inside
    /// a [`DecodeScope`]. A refused request ends it with [`PlanError::Status`] before the parser
    /// runs. `None` for a venue that takes no credentials, which drops them unread. `Err` as
    /// for [`exec_codec`](VenueFactory::exec_codec).
    fn test_connection(
        &self,
        cfg: &VenueConfig,
        creds: Secrets,
    ) -> Option<Result<HttpPlan<AccountSummary>, VenueError>>;
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

    fn get(tag: u64) -> Effect {
        Effect::Http {
            tag: HttpTag(tag),
            req: crate::codec::HttpRequest {
                method: crate::codec::HttpMethod::Get,
                url: WireUrl::plain("https://venue.invalid/list?key=SYNTHETIC-PLAN-KEY"),
                headers: vec![],
                body: crate::codec::WireSlice::plain(vec![]),
            },
            rpc: None,
            timeout: core::time::Duration::from_secs(1),
            class: crate::codec::TrafficClass::Normal,
            charge: crate::caps::RateCharge::one(crate::caps::OpKind::Rest, None),
        }
    }

    /// A plan of `tags` whose parser gives the bodies it was handed, in order.
    fn plan(tags: &[u64]) -> Result<HttpPlan<Vec<Vec<u8>>>, PlanError> {
        let mut fx = Effects::new();
        tags.iter().for_each(|tag| fx.push(get(*tag)));
        HttpPlan::new(fx, |responses, _scope| {
            Ok(responses.iter().map(|r| r.body.to_vec()).collect())
        })
    }

    fn ok(body: &[u8]) -> Result<HttpResponse<'_>, HttpFailure> {
        Ok(HttpResponse {
            status: 200,
            headers: &[],
            body,
        })
    }

    fn parse(
        plan: HttpPlan<Vec<Vec<u8>>>,
        answers: &[HttpAnswer<'_>],
    ) -> Result<Vec<Vec<u8>>, PlanError> {
        crate::scope::dispatch_market_data(&crate::caps::testing::caps(), |scope| {
            plan.parse(answers, scope)
        })
    }

    #[test]
    fn a_plan_holds_only_http_requests_each_under_its_own_tag() {
        let timer = Effect::Timer {
            tag: crate::codec::TimerTag(1),
            after: core::time::Duration::from_secs(1),
        };
        let mut fx = Effects::new();
        fx.push(get(1));
        fx.push(timer);
        let refused = HttpPlan::<()>::new(fx, |_, _| Ok(()));
        assert_eq!(refused.err(), Some(PlanError::NotHttp));
        let duplicate = plan(&[1, 2, 1]).err();
        assert_eq!(duplicate, Some(PlanError::DuplicateTag(HttpTag(1))));
        assert_eq!(plan(&[]).unwrap().requests(), []);
        assert_eq!(plan(&[1, 2]).unwrap().requests(), [get(1), get(2)]);
    }

    #[test]
    fn a_plan_parses_only_one_2xx_answer_per_request_in_its_order() {
        let both = [(HttpTag(1), ok(b"a")), (HttpTag(2), ok(b"b"))];
        let parsed = parse(plan(&[1, 2]).unwrap(), &both);
        assert_eq!(parsed, Ok(vec![b"a".to_vec(), b"b".to_vec()]));
        let swapped = [both[1], both[0]];
        for answers in [&both[..1], &swapped[..], &[both[0], both[1], both[1]][..]] {
            let refused = parse(plan(&[1, 2]).unwrap(), answers);
            assert_eq!(refused, Err(PlanError::Answers));
        }
        let lost = [(HttpTag(1), ok(b"a")), (HttpTag(2), Err(HttpFailure::Lost))];
        let (tag, failure) = (HttpTag(2), HttpFailure::Lost);
        assert_eq!(
            parse(plan(&[1, 2]).unwrap(), &lost),
            Err(PlanError::Http { tag, failure })
        );
        for status in [199, 301, 404] {
            let resp = Ok(HttpResponse {
                status,
                headers: &[],
                body: b"",
            });
            let refused = parse(plan(&[1]).unwrap(), &[(HttpTag(1), resp)]);
            let tag = HttpTag(1);
            assert_eq!(refused, Err(PlanError::Status { tag, status }), "{status}");
        }
    }

    #[test]
    fn plan_errors_say_what_was_refused_and_a_plan_shows_no_credential() {
        let id: PlanError = IdError::Empty.into();
        assert_eq!(
            id,
            PlanError::Decode(DecodeError::IdRefused(IdError::Empty))
        );
        let (tag, failure) = (HttpTag(4), HttpFailure::TimedOut);
        let cases = [
            (PlanError::NotHttp, "not an HTTP request"),
            (PlanError::DuplicateTag(tag), "two requests carry tag 4"),
            (PlanError::Answers, "do not match"),
            (
                PlanError::Http { tag, failure },
                "request 4 got no response: TimedOut",
            ),
            (PlanError::Status { tag, status: 503 }, "with status 503"),
            (id, "answer refused: venue id refused"),
            (PlanError::Missing("tick"), "required field tick is missing"),
        ];
        for (err, text) in cases {
            assert!(err.to_string().contains(text), "{err}");
        }
        assert_eq!(
            VenueError::NoDiscovery.to_string(),
            "the venue adapter discovers no instruments"
        );
        let shown = format!("{:?}", plan(&[1]).unwrap());
        assert!(shown.starts_with("HttpPlan { requests: ["), "{shown}");
        assert!(!shown.contains("SYNTHETIC-PLAN-KEY"), "{shown}");
    }
}
