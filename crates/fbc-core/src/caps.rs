//! What a venue can do, as data (decision 0003, design §4.5).
//!
//! [`VenueCaps`] and every type it is made of are plain data with every field mandatory: none
//! has a `Default` and none is `#[non_exhaustive]`, so adding a field breaks the build of every
//! adapter until each declares its value, and no value is a guess about a venue nobody checked.
//! Each declared value cites a venue document or a recorded fixture (design §6, step 2).
//!
//! Nothing outside the venue crates and the registry branches on a venue name; behaviour that
//! differs by venue is read from here. The runtime reads market data, rate limits, nonce scope
//! and cancel-on-disconnect; [`DecodeScope`](crate::DecodeScope) the fee sign and client-id
//! format; the OMS's planner and permits the order and fill capabilities; the consumer reads
//! them as numbers and flags. A behaviour these types cannot express grows them with a new
//! mandatory field and a decision record.
//!
//! Order and fill capabilities come together in one [`ExecCaps`] block, `None` for a
//! market-data-only venue (decision 0015, amending design §4.5's separate `order` and `fills`):
//! a venue cannot declare orders without saying how it reports fills, nor fill claims (a fill
//! source, a fee sign) for an account feed it does not have.
//!
//! Sets of flags are [`TagSet`]s, built by listing their members ([`TagSet::of`]) or stated
//! empty ([`TagSet::none`]).

use core::fmt;
use core::marker::PhantomData;
use core::num::NonZeroU32;
use core::time::Duration;

use crate::cid::ClientIdFormat;
use crate::fee::VenueFeeSign;
use crate::ids::InstrumentId;
use crate::time::ExchTsKind;
use crate::units::Channel;

/// Everything a venue can do. `None` for [`exec`](VenueCaps::exec) means market data only.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct VenueCaps {
    /// Order entry and the fills it reports, or `None` for a market-data-only venue, which then
    /// declares no order capability, fill source or fee sign (decision 0015).
    pub exec: Option<ExecCaps>,
    /// How the venue matches.
    pub matching: MatchingCaps,
    /// What market data the venue publishes, and how.
    pub md: MdCaps,
    /// Every rate limit the venue enforces.
    pub limits: Vec<RateLimit>,
    /// The furthest this adapter may be promoted: recording, shadow, paper or live.
    pub readiness_ceiling: Readiness,
}

/// What a venue with order entry can do: its orders and the fills they report, declared
/// together so neither half exists without the other (decision 0015).
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct ExecCaps {
    /// What order entry the venue offers.
    pub order: OrderCaps,
    /// What the venue reports about fills.
    pub fills: FillCaps,
}

/// What order entry a venue offers.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct OrderCaps {
    /// The order kinds accepted.
    pub kinds: TagSet<OrderKindTag>,
    /// The times in force accepted.
    pub tifs: TagSet<TifTag>,
    /// The order channels (public book, RPI) accepted.
    pub channels: TagSet<Channel>,
    /// Post-only orders are accepted.
    pub post_only: bool,
    /// Reduce-only orders are accepted.
    pub reduce_only: bool,
    /// Pairs of features the venue refuses together on one order.
    pub flag_conflicts: Vec<(Feature, Feature)>,
    /// How orders can be amended, or `None` where they cannot.
    pub amend: Option<AmendCaps>,
    /// The references a cancel can name natively.
    pub cancel_refs: TagSet<RefKind>,
    /// The references an order query can name natively.
    pub query_refs: TagSet<RefKind>,
    /// A cancel may be sent before the order's acknowledgement arrives.
    pub cancel_before_ack: bool,
    /// Cancels are signed like orders.
    pub cancel_is_signed: bool,
    /// Batched placement, or `None` where orders go one at a time.
    pub batch_place: Option<Batch>,
    /// Batched cancels and the references their items can name, or `None` where cancels go one
    /// at a time.
    pub batch_cancel: Option<CancelBatch>,
    /// Cancelling every order on the account in one request.
    pub cancel_all_account: Support,
    /// Cancelling every order on one instrument in one request; never widened to the account.
    pub cancel_all_instrument: Support,
    /// What the venue cancels when a connection drops.
    pub cancel_on_disconnect: CancelOnDisconnect,
    /// How the venue acknowledges an order.
    pub ack: AckModel,
    /// How the venue spells a client id on the wire.
    pub client_id: ClientIdFormat,
    /// Order and fill events carry our client id back.
    pub cid_echoed_on_events: bool,
    /// How the venue scopes order nonces.
    pub nonce_scope: NonceScope,
    /// What the venue's order events are ordered by.
    pub ordering_key: OrderingKey,
    /// Whether the venue's open-order snapshot can be trusted on resync.
    pub snapshot_source: SnapshotSource,
    /// Order events carry the order's flags (post-only, reduce-only) back.
    pub events_echo_flags: bool,
    /// The expected cost of signing one order, in microseconds.
    pub sign_cost_hint_us: u32,
}

/// What a venue cancels when a connection drops.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum CancelOnDisconnect {
    /// Nothing: orders outlive the connection.
    None,
    /// The orders placed on the connection; with `rearm_on_reconnect`, the protection must be
    /// requested again on each new connection.
    PerConnection {
        /// The protection lapses with the connection and is requested again on the next.
        rearm_on_reconnect: bool,
    },
    /// Everything, unless a dead-man timer of at most `max_ttl` is refreshed in time.
    DeadMan {
        /// The longest timer the venue accepts.
        max_ttl: Duration,
    },
}

/// How a venue amends an order.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct AmendCaps {
    /// The references an amend can name its order by natively: [`RefKind::Venue`],
    /// [`RefKind::Client`], or both. An amend carries no placement nonce, so
    /// [`RefKind::PlacementNonce`] here matches no amend; [`AmendOrder::reference`] picks from
    /// this set, and a codec refuses an amend it finds no declared reference for (0031).
    ///
    /// [`AmendOrder::reference`]: crate::AmendOrder::reference
    pub refs: TagSet<RefKind>,
    /// The price can be amended.
    pub price: bool,
    /// The quantity can be amended.
    pub qty: bool,
    /// The flags can be amended.
    pub flags: bool,
    /// A partially filled order can be amended.
    pub when_partially_filled: bool,
    /// A rejected amend leaves the original order resting.
    pub reject_keeps_original: bool,
    /// The amended order keeps its venue order id.
    pub keeps_venue_id: bool,
    /// How the venue confirms an amend.
    pub ack: AmendAck,
    /// What an amend's quantity means.
    pub qty_semantics: AmendQty,
    /// Whether an amend keeps queue priority: measured in calibration, `None` until it is (the
    /// simulator then assumes priority resets).
    pub keeps_priority: Option<bool>,
}

/// How a venue confirms an amend.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum AmendAck {
    /// An order event reports the replaced order.
    ReplacedEvent,
    /// Only the request's reply says so; the codec turns it into the amended-order event.
    RpcReplyOnly,
}

/// What an amend's quantity means on the venue's wire; codecs normalize to the total.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum AmendQty {
    /// The order's total quantity, filled part included.
    TotalIncludingFilled,
    /// The quantity still to fill.
    Remaining,
}

/// What a venue reports about fills.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct FillCaps {
    /// Where fills come from.
    pub source: FillSource,
    /// A fill says whether we made or took liquidity.
    pub liquidity_flag: bool,
    /// A fill carries the venue's realized P&L.
    pub realized_pnl: bool,
    /// A fill carries the venue's realized funding.
    pub realized_funding: bool,
    /// The sign convention fees are reported in; [`DecodeScope`](crate::DecodeScope) applies it.
    pub fee_sign: VenueFeeSign,
    /// A fill names the asset its fee is in.
    pub fee_asset_reported: bool,
    /// A fill carries a venue fill id; without one, fills are keyed by order and cumulative
    /// quantity.
    pub fill_id: bool,
    /// The venue sends past fills again after a reconnect.
    pub replays_fills_on_reconnect: bool,
}

/// Where a venue's fills come from.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum FillSource {
    /// Fill events of their own.
    Native,
    /// Changes in an order's filled quantity.
    DerivedFromOrderStatus,
}

/// How a venue matches orders.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct MatchingCaps {
    /// A deliberate delay on incoming orders, or `None`.
    pub speed_bump: Option<SpeedBump>,
    /// Across what self-trades are prevented.
    pub stp_scope: StpScope,
}

/// A deliberate delay a venue puts on incoming orders.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct SpeedBump {
    /// How long orders wait.
    pub delay: Duration,
    /// Which orders wait.
    pub applies_to: SpeedBumpScope,
}

/// Which orders a speed bump delays.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum SpeedBumpScope {
    /// Only orders that would take liquidity.
    TakersOnly,
    /// Every order and amend; cancels are not delayed.
    AllButCancels,
    /// Everything, cancels included.
    Everything,
}

/// Across what a venue prevents self-trades.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum StpScope {
    /// It does not.
    None,
    /// Orders of one account (sub-account).
    Account,
    /// Orders of every account under one owner.
    Owner,
}

/// What market data a venue publishes, and how.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct MdCaps {
    /// The wire encoding of market data.
    pub encoding: Encoding,
    /// Every channel that carries the best bid and offer.
    pub touch_sources: Vec<TouchSourceCaps>,
    /// One entry per selectable book channel.
    pub books: Vec<BookCaps>,
    /// The public trade feed.
    pub trades: TradeCaps,
    /// The funding feed.
    pub funding: FundingCaps,
    /// Where volume and open interest come from.
    pub stats: FeedSource,
    /// Where the mark price ([`Feed::Mark`](crate::Feed::Mark)) comes from.
    pub mark: FeedSource,
    /// Where the index price ([`Feed::Index`](crate::Feed::Index)) comes from.
    pub index: FeedSource,
    /// The resolution of the venue's timestamps.
    pub ts_precision: Duration,
    /// How subscriptions spread over connections.
    pub topology: ConnTopology,
    /// The longest a connection may live before the venue closes it, so the runtime rotates
    /// it first; `None` where there is no limit.
    pub max_conn_lifetime: Option<Duration>,
}

/// A wire encoding.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum Encoding {
    Json,
    /// Simple Binary Encoding.
    Sbe,
    Protobuf,
    /// Delimited text that is not JSON: `key=value` fields, as FIX tag=value is.
    Text,
}

/// A channel that carries the best bid and offer.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct TouchSourceCaps {
    /// The venue's name for the channel.
    pub channel: &'static str,
    /// How often it updates.
    pub cadence: Cadence,
    /// What its sequence numbers are ordered against.
    pub seq_domain: SeqDomain,
    /// Which instant its timestamps mark.
    pub ts_kind: ExchTsKind,
    /// The order channels whose liquidity it shows.
    pub includes_channels: TagSet<Channel>,
}

/// What a feed's sequence numbers are ordered against.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum SeqDomain {
    /// The feed carries no sequence numbers.
    None,
    /// Its own sequence, comparable only within the feed.
    Own,
    /// The same sequence as the instrument's book, so the two can be ordered together.
    SharedWithBook,
}

/// One selectable book channel.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct BookCaps {
    /// The venue's name for the channel.
    pub channel: &'static str,
    /// The most levels per side it carries.
    pub max_depth: u16,
    /// How often it updates.
    pub cadence: Cadence,
    /// How a gap in it is detected.
    pub continuity: Continuity,
    /// It carries only a window of levels around the touch.
    pub windowed: bool,
    /// It needs a REST snapshot to anchor its deltas.
    pub rest_anchor: bool,
    /// The order channels whose liquidity it shows.
    pub includes_channels: TagSet<Channel>,
    /// How well a queue-position model can be calibrated on it.
    pub queue_model: QueueModelQuality,
}

/// How often a feed updates.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum Cadence {
    /// On every change.
    Realtime,
    /// On a fixed pulse.
    Pulsed(Duration),
    /// On changes, at most once per interval.
    Capped(Duration),
}

/// How a gap in a feed is detected.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum Continuity {
    /// Each message's sequence is the previous one plus one.
    PlusOne,
    /// Each message names the previous message's id.
    PrevId,
    /// Each message replaces a window; nothing to chain.
    Windowed,
    /// Gaps cannot be detected.
    Unsequenced,
}

/// How well a queue-position model can be calibrated on a book channel.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum QueueModelQuality {
    /// Well enough to calibrate one.
    Calibratable,
    /// Only well enough for pessimistic and optimistic brackets.
    BracketOnly,
}

/// The public trade feed.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct TradeCaps {
    /// Where trades come from.
    pub source: FeedSource,
    /// A trade says which side was the aggressor.
    pub aggressor: bool,
    /// A trade carries a venue trade id.
    pub trade_id: bool,
}

/// The funding feed.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct FundingCaps {
    /// Where funding rates come from.
    pub source: FeedSource,
    /// A funding message states the funding interval.
    pub interval_reported: bool,
    /// A funding message states the next funding time.
    pub next_time_reported: bool,
}

/// Where a kind of market data comes from.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum FeedSource {
    /// A streamed channel.
    Stream,
    /// A REST endpoint the runtime polls.
    Poll,
    /// The venue does not publish it.
    None,
}

/// How a venue's market-data subscriptions spread over connections.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum ConnTopology {
    /// Any subscriptions share a connection, up to `max_subscriptions` where the venue caps
    /// them.
    Shared {
        /// The most subscriptions one connection carries, `None` where there is no cap.
        max_subscriptions: Option<u32>,
    },
    /// As `Shared`, except that a connection carries at most one book channel
    /// ([`Feed::Book`](crate::Feed::Book)) per instrument: the venue's book frames name their
    /// instrument but not their channel, so two book channels of one instrument on one
    /// connection could not be told apart (decision 0022). A second book channel of an
    /// instrument goes on another connection.
    SharedOneBookPerInstrument {
        /// The most subscriptions one connection carries, `None` where there is no cap.
        max_subscriptions: Option<u32>,
    },
    /// One connection per instrument.
    PerInstrument,
    /// One connection per channel.
    PerChannel,
}

/// One rate limit: at most `units` of the operations in `ops` per `per`, counted across
/// `scope`. Each request counts its [`RateCharge::weight`] in units: one for a venue that
/// counts requests, the request's weight for one that budgets weight (decision 0018).
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct RateLimit {
    /// What the limit is counted across.
    pub scope: LimitScope,
    /// The operations it counts.
    pub ops: TagSet<OpKind>,
    /// The window.
    pub per: Duration,
    /// The units allowed per window: requests, or weight where requests are weighted.
    pub units: u32,
}

/// What a rate limit is counted across.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum LimitScope {
    /// One account.
    Account,
    /// One source IP address.
    Ip,
    /// One instrument.
    Pair,
    /// One address, with one request allowed per `usdc_per_req` of traded volume.
    AddressVolume {
        /// Traded volume, in USDC, that earns one request.
        usdc_per_req: u32,
    },
    /// One connection, while it stays open: a cap on the frames written to it (a venue's limit
    /// on incoming messages per connection). It counts frames and keepalives on that
    /// connection, never an HTTP request (decision 0018).
    Connection,
}

impl RateLimit {
    /// Whether this limit counts `charge`, sent `via` a frame or an HTTP request: it lists the
    /// charge's operation; when it is counted per pair, the charge names the instrument; and
    /// when it is counted per connection, the request is a frame on one (decision 0018). Which
    /// bucket the charge falls in (the account, the IP, the instrument, the connection) is the
    /// runtime's to pick from [`scope`](RateLimit::scope).
    pub fn counts(&self, charge: &RateCharge, via: Via) -> bool {
        let keyed = match self.scope {
            LimitScope::Pair => charge.inst.is_some(),
            LimitScope::Connection => via == Via::Frame,
            LimitScope::Account | LimitScope::Ip | LimitScope::AddressVolume { .. } => true,
        };
        keyed && self.ops.contains(charge.op)
    }
}

/// How a charged request goes out: as a frame on a connection (an
/// [`Effect::Send`](crate::Effect::Send) or a keepalive), or as an HTTP request
/// ([`Effect::Http`](crate::Effect::Http)), which no per-connection limit counts.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum Via {
    Frame,
    Http,
}

/// What one request costs against a venue's rate limits (decision 0018): the operation it is,
/// the instrument it is counted against where a limit is per pair, and its weight in the units
/// of the limits that count it. Every frame and HTTP request a codec asks for
/// ([`Effect::Send`](crate::Effect::Send), [`Effect::Http`](crate::Effect::Http)) and every
/// [`Keepalive`](crate::Keepalive) carries one, so the runtime charges the right bucket for
/// what a codec sends on its own (a resync, a token refresh, a resubscribe, a pong) as well as
/// for orders. The runtime charges [`OpKind::Connect`] itself for each connection it opens.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct RateCharge {
    /// The operation, matched against each limit's [`ops`](RateLimit::ops).
    pub op: OpKind,
    /// The scope key: the instrument a per-pair limit ([`LimitScope::Pair`]) counts the request
    /// against, `None` for a request that names no single instrument (authentication, a
    /// resync, an account-wide cancel-all). A per-pair limit counts only charges that name one,
    /// so a codec names the instrument on every request its venue counts per pair.
    pub inst: Option<InstrumentId>,
    /// The cost in the units of every limit that counts the request: one for a venue that
    /// counts requests, more for a weighted request (a deeper book snapshot against a weight
    /// budget). Never zero, so no request rides free.
    pub weight: NonZeroU32,
}

impl RateCharge {
    /// A request of `op` that costs one unit, counted against `inst` where a limit is per pair.
    pub const fn one(op: OpKind, inst: Option<InstrumentId>) -> RateCharge {
        RateCharge {
            op,
            inst,
            weight: NonZeroU32::MIN,
        }
    }
}

/// How far an adapter may be promoted, in order: [`Record`](Readiness::Record) <
/// [`Shadow`](Readiness::Shadow) < [`Paper`](Readiness::Paper) < [`Live`](Readiness::Live).
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum Readiness {
    /// Record market data only.
    Record,
    /// Compute quotes without sending them.
    Shadow,
    /// Trade against a simulated venue fed live data.
    Paper,
    /// Trade.
    Live,
}

/// Whether a venue offers an operation natively. There is no emulated variant: a missing
/// operation is never widened into another.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum Support {
    Native,
    Unsupported,
}

/// A batch request's limits.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct Batch {
    /// The most items in one request.
    pub max_items: u16,
}

/// A batch cancel's limits and the references its items can name. A venue's batch cancel can
/// take fewer kinds than its single cancel ([`OrderCaps::cancel_refs`]): one that names orders
/// by venue id only cannot cancel an order not yet acknowledged.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct CancelBatch {
    /// The most cancels in one request.
    pub max_items: u16,
    /// The references each item can name natively; [`CancelOrder::reference`] picks from this
    /// set, and a codec refuses a batch with an item it finds no declared reference for (0031).
    ///
    /// [`CancelOrder::reference`]: crate::CancelOrder::reference
    pub refs: TagSet<RefKind>,
}

/// How a venue acknowledges an order.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum AckModel {
    /// One acknowledgement: accepted is final.
    SinglePhase,
    /// Accepted first, then possibly rejected by a risk check within `risk_reject_window`.
    TwoPhase {
        /// How long after acceptance a risk reject can still arrive.
        risk_reject_window: Duration,
    },
}

/// How a venue scopes order nonces.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum NonceScope {
    /// One increasing sequence per account, shared by every process on it.
    PerAccountMonotonic,
    /// Random nonces.
    Random,
    /// One sequence per signing key.
    PerSigner,
    /// The venue uses no nonces.
    None,
}

/// What a venue's order events are ordered by.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum OrderingKey {
    /// A venue sequence number.
    VenueSeq,
    /// A venue timestamp.
    VenueTs,
    /// A block time.
    BlockTime,
    /// Nothing: only a terminal state wins over another.
    None,
}

/// Whether a venue's open-order snapshot can be trusted on resync.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum SnapshotSource {
    /// It shows the venue's open orders as the venue sees them.
    Trustworthy,
    /// It can be stale or incomplete.
    Untrustworthy,
    /// The venue offers none.
    None,
}

/// A tag a [`TagSet`] can hold: a fieldless enum with fewer than 32 variants.
pub trait CapTag: Copy + Eq + fmt::Debug + 'static {
    /// Every variant, in declaration order.
    const ALL: &'static [Self];
    /// The variant's bit in a [`TagSet`].
    fn bit(self) -> u32;
}

macro_rules! cap_tags {
    ($ty:ty: $($variant:path),+ $(,)?) => {
        impl CapTag for $ty {
            const ALL: &'static [Self] = &[$($variant),+];
            fn bit(self) -> u32 {
                // Exhaustive over the listed variants: a variant added to the enum but left out
                // of the list does not compile, so `ALL` (and `iter`, `Debug`) cannot fall
                // behind `bit`.
                match self {
                    $($variant)|+ => 1 << (self as u32),
                }
            }
        }
    };
}

/// A set of capability tags, built by listing its members. There is no default: an empty set is
/// stated with [`TagSet::none`].
#[derive(Copy, Clone, Eq, PartialEq, Hash)]
pub struct TagSet<T: CapTag> {
    bits: u32,
    _tags: PhantomData<T>,
}

impl<T: CapTag> TagSet<T> {
    /// The set of exactly `tags`.
    pub fn of(tags: &[T]) -> TagSet<T> {
        TagSet {
            bits: tags.iter().fold(0, |bits, tag| bits | tag.bit()),
            _tags: PhantomData,
        }
    }

    /// The empty set, stated explicitly.
    pub fn none() -> TagSet<T> {
        TagSet::of(&[])
    }

    /// Whether `tag` is in the set.
    pub fn contains(self, tag: T) -> bool {
        self.bits & tag.bit() != 0
    }

    /// Whether the set is empty.
    pub fn is_empty(self) -> bool {
        self.bits == 0
    }

    /// The members, in declaration order.
    pub fn iter(self) -> impl Iterator<Item = T> {
        T::ALL
            .iter()
            .copied()
            .filter(move |&tag| self.contains(tag))
    }
}

impl<T: CapTag> fmt::Debug for TagSet<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(self.iter()).finish()
    }
}

/// An order kind.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum OrderKindTag {
    Limit,
    Market,
}

/// A time in force.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum TifTag {
    /// Good till cancelled.
    Gtc,
    /// Immediate or cancel.
    Ioc,
    /// Fill or kill.
    Fok,
}

/// An order feature that may conflict with another on one venue.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum Feature {
    PostOnly,
    ReduceOnly,
    Ioc,
    Fok,
    /// The retail price improvement channel.
    Rpi,
}

/// A kind of reference that names an order.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum RefKind {
    /// The venue's order id.
    Venue,
    /// Our client id.
    Client,
    /// The nonce the order was placed with.
    PlacementNonce,
}

/// A kind of request a rate limit counts.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum OpKind {
    Place,
    Amend,
    Cancel,
    CancelAll,
    Query,
    /// A market-data subscription.
    Subscribe,
    /// Any other REST request.
    Rest,
    /// Opening a connection (a venue's limit on new connections): the runtime charges one for
    /// each connection it opens, planned or reconnected.
    Connect,
    /// Any other frame written on a connection: authentication, a keepalive ping or pong, a
    /// session message.
    Control,
}

cap_tags!(OrderKindTag: OrderKindTag::Limit, OrderKindTag::Market);
cap_tags!(TifTag: TifTag::Gtc, TifTag::Ioc, TifTag::Fok);
cap_tags!(Channel: Channel::Public, Channel::Rpi);
cap_tags!(RefKind: RefKind::Venue, RefKind::Client, RefKind::PlacementNonce);
cap_tags!(
    OpKind: OpKind::Place,
    OpKind::Amend,
    OpKind::Cancel,
    OpKind::CancelAll,
    OpKind::Query,
    OpKind::Subscribe,
    OpKind::Rest,
    OpKind::Connect,
    OpKind::Control,
);

#[cfg(test)]
mod tests {
    use super::*;

    fn declared_in_order<T: CapTag>() {
        for (n, tag) in T::ALL.iter().enumerate() {
            assert_eq!(tag.bit(), 1 << n, "{tag:?} is not in declaration order");
        }
    }

    #[test]
    fn every_tag_list_names_each_variant_once_in_order() {
        declared_in_order::<OrderKindTag>();
        declared_in_order::<TifTag>();
        declared_in_order::<Channel>();
        declared_in_order::<RefKind>();
        declared_in_order::<OpKind>();
        assert_eq!(OpKind::ALL.len(), 9);
    }

    #[test]
    fn a_limit_counts_the_charges_for_its_operations_keyed_as_its_scope_needs() {
        // Decision 0018: a limit counts a charge whose operation it lists; a per-pair limit only
        // one that names the instrument, every other scope whatever the charge names.
        let inst = Some(InstrumentId::new(3));
        let limit = |scope| RateLimit {
            scope,
            ops: TagSet::of(&[OpKind::Place, OpKind::Control]),
            per: Duration::from_secs(1),
            units: 10,
        };
        let scopes = [
            LimitScope::Account,
            LimitScope::Ip,
            LimitScope::Pair,
            LimitScope::AddressVolume { usdc_per_req: 1 },
            LimitScope::Connection,
        ];
        for scope in scopes {
            let limit = limit(scope);
            let place = RateCharge::one(OpKind::Place, inst);
            assert!(limit.counts(&place, Via::Frame), "{scope:?}");
            let cancel = RateCharge::one(OpKind::Cancel, inst);
            assert!(!limit.counts(&cancel, Via::Frame), "{scope:?}");
            let unkeyed = limit.counts(&RateCharge::one(OpKind::Control, None), Via::Frame);
            assert_eq!(unkeyed, scope != LimitScope::Pair, "{scope:?}");
            // Codex r4176401174: a per-connection limit never counts an HTTP request, even of
            // an operation it lists for frames; every other scope counts both.
            let http = limit.counts(&place, Via::Http);
            assert_eq!(http, scope != LimitScope::Connection, "{scope:?}");
        }
        // A charge costs one unit unless it says more; its weight is never zero.
        assert_eq!(RateCharge::one(OpKind::Connect, None).weight.get(), 1);
        let deep = RateCharge {
            weight: NonZeroU32::new(20).unwrap(),
            ..RateCharge::one(OpKind::Rest, None)
        };
        let deep_place = RateCharge {
            op: OpKind::Place,
            ..deep
        };
        assert!(limit(LimitScope::Ip).counts(&deep_place, Via::Http));
    }

    #[test]
    fn a_tag_set_holds_exactly_what_it_lists() {
        let refs = TagSet::of(&[RefKind::Client, RefKind::Venue, RefKind::Client]);
        assert!(refs.contains(RefKind::Venue) && refs.contains(RefKind::Client));
        assert!(!refs.contains(RefKind::PlacementNonce));
        assert!(!refs.is_empty());
        assert_eq!(
            refs.iter().collect::<Vec<_>>(),
            [RefKind::Venue, RefKind::Client]
        );
        assert_eq!(format!("{refs:?}"), "{Venue, Client}");
        let none = TagSet::<Channel>::none();
        assert!(none.is_empty());
        assert_eq!(none.iter().count(), 0);
        assert_eq!(none, TagSet::of(&[]));
        assert_ne!(none, TagSet::of(&[Channel::Public]));
    }
}

/// Synthetic capabilities for the crate's unit tests, which get venue ids and fees only from a
/// [`DecodeScope`](crate::DecodeScope) lent for a venue's caps (decision 0004). The values
/// describe no real venue.
#[cfg(test)]
pub(crate) mod testing {
    use super::*;

    /// A synthetic venue with order entry: UUID client ids and fees reported under `fee_sign`.
    pub(crate) fn exec_caps(fee_sign: VenueFeeSign) -> VenueCaps {
        VenueCaps {
            exec: Some(ExecCaps {
                order: OrderCaps {
                    kinds: TagSet::of(&[OrderKindTag::Limit]),
                    tifs: TagSet::of(&[TifTag::Gtc]),
                    channels: TagSet::of(&[Channel::Public]),
                    post_only: true,
                    reduce_only: true,
                    flag_conflicts: Vec::new(),
                    amend: None,
                    cancel_refs: TagSet::of(&[RefKind::Venue]),
                    query_refs: TagSet::none(),
                    cancel_before_ack: false,
                    cancel_is_signed: false,
                    batch_place: None,
                    batch_cancel: None,
                    cancel_all_account: Support::Unsupported,
                    cancel_all_instrument: Support::Unsupported,
                    cancel_on_disconnect: CancelOnDisconnect::None,
                    ack: AckModel::SinglePhase,
                    client_id: ClientIdFormat::Uuid,
                    cid_echoed_on_events: true,
                    nonce_scope: NonceScope::None,
                    ordering_key: OrderingKey::None,
                    snapshot_source: SnapshotSource::None,
                    events_echo_flags: false,
                    sign_cost_hint_us: 0,
                },
                fills: FillCaps {
                    source: FillSource::Native,
                    liquidity_flag: true,
                    realized_pnl: false,
                    realized_funding: false,
                    fee_sign,
                    fee_asset_reported: true,
                    fill_id: true,
                    replays_fills_on_reconnect: false,
                },
            }),
            matching: MatchingCaps {
                speed_bump: None,
                stp_scope: StpScope::None,
            },
            md: MdCaps {
                encoding: Encoding::Json,
                touch_sources: Vec::new(),
                books: Vec::new(),
                trades: TradeCaps {
                    source: FeedSource::None,
                    aggressor: false,
                    trade_id: false,
                },
                funding: FundingCaps {
                    source: FeedSource::None,
                    interval_reported: false,
                    next_time_reported: false,
                },
                stats: FeedSource::None,
                mark: FeedSource::None,
                index: FeedSource::None,
                ts_precision: Duration::from_millis(1),
                topology: ConnTopology::PerInstrument,
                max_conn_lifetime: None,
            },
            limits: Vec::new(),
            readiness_ceiling: Readiness::Record,
        }
    }

    /// The same venue as a cost-signed [`exec_caps`], which is all a test that only needs
    /// venue ids cares about.
    pub(crate) fn caps() -> VenueCaps {
        exec_caps(VenueFeeSign::PositiveIsCost)
    }
}
