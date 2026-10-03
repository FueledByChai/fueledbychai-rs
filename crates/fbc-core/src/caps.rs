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
//! Sets of flags are [`TagSet`]s, built by listing their members ([`TagSet::of`]) or stated
//! empty ([`TagSet::none`]).

use core::fmt;
use core::marker::PhantomData;
use core::time::Duration;

use crate::cid::ClientIdFormat;
use crate::fee::VenueFeeSign;
use crate::time::ExchTsKind;
use crate::units::Channel;

/// Everything a venue can do. `None` for [`order`](VenueCaps::order) means market data only.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct VenueCaps {
    /// Order entry, or `None` for a market-data-only venue.
    pub order: Option<OrderCaps>,
    /// What the venue reports about fills.
    pub fills: FillCaps,
    /// How the venue matches.
    pub matching: MatchingCaps,
    /// What market data the venue publishes, and how.
    pub md: MdCaps,
    /// Every rate limit the venue enforces.
    pub limits: Vec<RateLimit>,
    /// The furthest this adapter may be promoted: recording, shadow, paper or live.
    pub readiness_ceiling: Readiness,
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
    /// Batched cancels, or `None` where cancels go one at a time.
    pub batch_cancel: Option<Batch>,
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
    /// One connection per instrument.
    PerInstrument,
    /// One connection per channel.
    PerChannel,
}

/// One rate limit: at most `units` of the operations in `ops` per `per`, counted across
/// `scope`.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct RateLimit {
    /// What the limit is counted across.
    pub scope: LimitScope,
    /// The operations it counts.
    pub ops: TagSet<OpKind>,
    /// The window.
    pub per: Duration,
    /// The units allowed per window.
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
        assert_eq!(OpKind::ALL.len(), 7);
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
