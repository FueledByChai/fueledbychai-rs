//! What the venue and the consumer's rate limiter state for one planner pass, besides the
//! orders (design §4.10 steps 6 and 7; decision 0111): the venue's [`OrderCaps`], the venue
//! modes of the account and of the market, the market's trading status, each rate scope's
//! room with the consumer's safety floor ([`RateView`]), and whether the planner batches
//! ([`Batching`]). Every field is the consumer's, with no default: the consumer reads the
//! modes from the venue's order events ([`ExecEvent::Mode`](fbc_core::ExecEvent)), the status
//! from the market's [`InstrumentSpec`](fbc_core::InstrumentSpec), and the rate scopes from
//! its runtime's limiter, with each floor from its own configuration (0009).
//!
//! - **Modes and status.** No place and no amend is built for a market whose trading status,
//!   account mode or market mode refuses it: Cancel-only, Halted and Delisted refuse every
//!   place and amend; Post-only refuses one whose order is not post-only, Reduce-only one whose
//!   order does not carry the venue's reduce-only flag (an emulated reduce-only order or a
//!   consumer's reducing classification is not the flag the venue checks). Cancels are built in
//!   every mode and status, as they are in every market state (0012): a stale mode never holds
//!   back a cancel, and a cancel a halted venue refuses costs at worst a reject.
//! - **The safety floor.** A command of [`TrafficClass::Normal`](fbc_core::TrafficClass::Normal) is withheld, never built, when
//!   charging it would leave a scope that counts it with less than the scope's floor; a
//!   [`TrafficClass::Safety`](fbc_core::TrafficClass::Safety) command (a cancel, a cancel-many, a reducing place or amend) is
//!   never withheld here, the runtime's limiter judging it. Each command the pass builds is
//!   charged against the view as it is built, so the later ones are judged with it counted. A
//!   command is charged its item count (a batch of n items costs n), as Paradex's codec charges
//!   it (0054): a venue that charges a batch once costs less than counted, never more.

use fbc_core::{
    InstrumentId, OpKind, OrderCaps, RateLimit, TagSet, TradingStatus, VenueCommand, VenueMode,
};

/// How the planner groups places and cancels into requests (decision 0111): the consumer's
/// choice, no default.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum Batching {
    /// One order per command: every place a single place, every cancel a single cancel.
    One,
    /// Within the batches the venue's [`OrderCaps`] declare: consecutive places of one stage
    /// (so of one traffic class: reducing orders never share a batch with adds) go in a batch
    /// of at most `batch_place.max_items`, and the pass's cancels in cancel-manys of at most
    /// `batch_cancel.max_items`, each item naming a reference the batch declares
    /// (`batch_cancel.refs`), any other going as a single cancel. A run of one goes as a
    /// single command.
    Declared,
}

/// One rate scope's room as the consumer reads it from its runtime's limiter: a bucket of a
/// declared [`RateLimit`], with the consumer's safety floor for it.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct RateScope {
    /// The operations the bucket counts: its limit's [`RateLimit::ops`].
    pub ops: TagSet<OpKind>,
    /// The one instrument a per-pair bucket counts; `None` for a bucket every instrument's
    /// requests share (the account's, the IP's, a connection's).
    pub inst: Option<InstrumentId>,
    /// The units the bucket has left in its window now.
    pub left: u32,
    /// The consumer's safety floor: the units of the bucket normal traffic never uses (0009).
    pub floor: u32,
}

impl RateScope {
    /// The room of `limit`'s bucket for `inst` (`None` but for a per-pair limit) with `used`
    /// units charged in its window now, and the consumer's floor of `floor` units.
    pub fn of(limit: &RateLimit, inst: Option<InstrumentId>, used: u64, floor: u32) -> RateScope {
        let left = u64::from(limit.units).saturating_sub(used);
        RateScope {
            ops: limit.ops,
            inst,
            left: u32::try_from(left).unwrap_or(u32::MAX),
            floor,
        }
    }

    /// Whether the scope counts `op` on `market`.
    fn counts(&self, op: OpKind, market: InstrumentId) -> bool {
        self.ops.contains(op) && self.inst.is_none_or(|inst| inst == market)
    }
}

/// Every rate scope's room for one pass, as the consumer filled it ([`RateScope`]). No
/// default: a view with no scope states that no scope is tracked, and withholds nothing.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct RateView {
    scopes: Vec<RateScope>,
}

impl RateView {
    /// The view of `scopes`.
    pub fn new(scopes: Vec<RateScope>) -> RateView {
        RateView { scopes }
    }

    /// The scopes, in the order given.
    pub fn scopes(&self) -> &[RateScope] {
        &self.scopes
    }

    /// The first scope counting `op` on `market` that a normal command of `weight` units would
    /// leave below its floor.
    pub(crate) fn floor_hit(&self, op: OpKind, market: InstrumentId, weight: u32) -> Option<usize> {
        self.scopes.iter().position(|s| {
            s.counts(op, market) && s.left.checked_sub(weight).is_none_or(|left| left < s.floor)
        })
    }

    /// Charges `cmd`, built for `market`, against every scope that counts it.
    pub(crate) fn charge(&mut self, cmd: &VenueCommand, market: InstrumentId) {
        self.charge_units(op_of(cmd), market, weight_of(cmd));
    }

    /// Charges `weight` units of `op` on `market` against every scope that counts it.
    pub(crate) fn charge_units(&mut self, op: OpKind, market: InstrumentId, weight: u32) {
        for s in self.scopes.iter_mut().filter(|s| s.counts(op, market)) {
            s.left = s.left.saturating_sub(weight);
        }
    }
}

/// The operation a venue counts `cmd` as: a place or a batch of places, an amend, or (the
/// only other commands the planner builds) a cancel or a cancel-many.
fn op_of(cmd: &VenueCommand) -> OpKind {
    match cmd {
        VenueCommand::Place(_) | VenueCommand::PlaceBatch(_) => OpKind::Place,
        VenueCommand::Amend(_) => OpKind::Amend,
        _ => OpKind::Cancel,
    }
}

/// What `cmd` is charged: its item count.
fn weight_of(cmd: &VenueCommand) -> u32 {
    cmd.items().map_or(u32::MAX, u32::from)
}

/// What the venue and the consumer's limiter state for one pass ([`ExecutionPlanner::plan`];
/// decision 0111). Every field is the consumer's, with no default.
///
/// [`ExecutionPlanner::plan`]: crate::ExecutionPlanner::plan
#[derive(Clone, Debug)]
pub struct PlanVenue<'a> {
    /// The venue's order capabilities.
    pub caps: &'a OrderCaps,
    /// The account's venue mode ([`ModeScope::Account`](fbc_core::ModeScope)), as the venue
    /// last reported it.
    pub account_mode: VenueMode,
    /// The market's venue mode ([`ModeScope::Instrument`](fbc_core::ModeScope)), as the venue
    /// last reported it.
    pub market_mode: VenueMode,
    /// The market's trading status, from its instrument spec.
    pub status: TradingStatus,
    /// Each rate scope's room, with the consumer's floors.
    pub rates: RateView,
    /// Whether the planner batches.
    pub batching: Batching,
}

/// Why the venue refuses a place or an amend of an order carrying `post_only` and
/// `reduce_only`, if it does: the market's trading status first, then the account's mode,
/// then the market's.
pub(crate) fn venue_refusal(
    venue: &PlanVenue<'_>,
    post_only: bool,
    reduce_only: bool,
) -> Option<crate::PlanRefusal> {
    let admits = |posts: bool, reduces: bool, none: bool| {
        !none && (!posts || post_only) && (!reduces || reduce_only)
    };
    let status = venue.status;
    let by_status = match status {
        TradingStatus::Trading => admits(false, false, false),
        TradingStatus::PostOnly => admits(true, false, false),
        TradingStatus::ReduceOnly => admits(false, true, false),
        TradingStatus::CancelOnly | TradingStatus::Halted | TradingStatus::Delisted => {
            admits(false, false, true)
        }
    };
    if !by_status {
        return Some(crate::PlanRefusal::Status(status));
    }
    for mode in [venue.account_mode, venue.market_mode] {
        let ok = match mode {
            VenueMode::Normal => admits(false, false, false),
            VenueMode::PostOnly => admits(true, false, false),
            VenueMode::ReduceOnly => admits(false, true, false),
            VenueMode::CancelOnly | VenueMode::Halted => admits(false, false, true),
        };
        if !ok {
            return Some(crate::PlanRefusal::VenueMode(mode));
        }
    }
    None
}
