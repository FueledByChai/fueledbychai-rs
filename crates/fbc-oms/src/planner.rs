//! The execution planner (decision 0005's one planner, 0065): the consumer's desired book in,
//! authorized commands out. It diffs a [`DesiredBook`] against the orders it placed, per side
//! and level, and builds what closes the difference through the [`Registry`]'s one pre-trade
//! path (the market's state, Exit's admission and both caps; 0012, 0013 rule 2) and its
//! permits, each command leaving as an [`Authorization`] (0045, 0060).
//!
//! Per level, in one [`ExecutionPlanner::plan`] pass:
//!
//! - a level wanted with no order of ours there is placed (an add, or a reducing order when
//!   the quote reduces);
//! - a level whose order is PendingNew or Unknown, on the Unknown ladder, has a command in
//!   flight, an amend built and not reported sent, or an amend replaced in flight and not yet
//!   settled by an ordered venue update ([`OrderRecord::amend_unconfirmed`]; 0070), is occupied:
//!   nothing is placed, amended or replaced there until that settles ([`HeldReason`]);
//! - a level whose order has a cancel waiting for its acknowledgement (the level was pulled
//!   before it) is replaced, wanted again or not: the cancel is built once the acknowledgement
//!   lands and the level waits for the order's terminal state;
//! - a replace whose cancel was reported sent and answered without ending the order (not sent,
//!   or refused) is over: the order rests as it was and its level is decided afresh, so a
//!   quote equal to it keeps it and a later change amends it where the caps allow (0075);
//! - a resting order whose price moved by at least the configured basis points of its price,
//!   or whose resting quantity moved by at least the configured lots, or whose flags differ,
//!   is changed once it is at least the configured minimum age (since the planner placed or
//!   amended it): amended where the venue's [`OrderCaps`] admit that amend, otherwise
//!   cancelled, the level then waiting for the order's terminal state before the new order is
//!   placed, so the old order counts as resting and as exposure until it is terminal and the
//!   new one from when it is built (0005's I6);
//! - a level no longer wanted (or wanted at quantity zero) has its order cancelled;
//! - in a market in Exit, an order on a side that does not reduce the position (either side
//!   while the position is flat or unknown) is cancelled, wanted or not: Exit builds nothing
//!   there, so the planner never leaves it resting nor tries to amend it (0063).
//!
//! Each side may also carry a [`CrossingIntent`], a reduce-only exit that crosses at a price
//! cap (a disaster stop, a news flatten window, the operator's Flatten), at no level: it is
//! planned as one reduce-only order priced at its cap, immediate-or-cancel where the venue's
//! [`OrderCaps`] take IOC with reduce-only and otherwise a good-till-cancelled limit at the cap
//! and its cancel (the synthetic IOC below); sized at most the position less the other orders
//! reducing it on its side, so the position never crosses zero, and planned not at all while
//! nothing is left to reduce; built through the same one pre-trade path, so refused in Killed
//! and Cancel-only and judged by Exit's admission and both caps; never amended or replaced while
//! it is live; and at most one order per [`CrossingIntent::attempt`], attempts only
//! increasing, so an order the venue refuses or leaves unfilled is not sent again until the
//! consumer names a later attempt (decision 0107). A synthetic IOC, a crossing's or a level
//! quote's, is built only where the venue takes good-till-cancelled limits and can cancel the
//! order by venue or client id once acknowledged ([`PlanRefusal::NoSyntheticIoc`]).
//!
//! Each level's quote carries the consumer's opaque tag and each side an optional removal
//! reason: every place and amend built for a level carries its tag, every cancel of a level no
//! longer wanted (absent, or at quantity zero) its side's reason as the pass that builds it
//! states it, a cancel of a level still wanted none, and the order keeps both for the audit
//! sink's verdicts. fbc-oms gives them no meaning (0001; 0098).
//!
//! The commands come out in 0005's order: cancels, then reducing orders (places and amends
//! that reduce), then amends, then adds, each group in side (bids first) and level order, and
//! are built in that order, so each is judged with the earlier ones counted. A place or amend
//! the market's state or a cap refuses is never built and is reported ([`Refused`]); the
//! planner never falls back from a refused amend to a cancel, and nothing it emits bypasses a
//! check. Batching, venue modes and the rate-scope safety floor are not here (FBC-hht).
//!
//! Each wanted quote is first fitted to the venue's [`OrderCaps`], from the caps alone, never
//! the venue's name (design §4.10 steps 3 and 4; 0106): RPI on a venue without it goes on the
//! public book; a reduce-only quote whose flags the venue refuses together falls back,
//! (Rpi, ReduceOnly) to the public book, (Ioc, ReduceOnly) to a synthetic IOC (a
//! good-till-cancelled limit at the quote's price, cancelled at once, its cancel following its
//! place in the same pass where the venue takes it before the acknowledgement), and
//! (PostOnly, ReduceOnly) by the quote's [`ReduceFallback`]. Each change is reported on the
//! places and amends built from the quote ([`Planned::downgrades`]). An emulated reduce-only
//! order never rests more than is left to reduce (the position less the other orders reducing
//! it on its side): it is sized to that when built, and cancelled at once when it would rest
//! more, at a pass's start, after its builds, and by [`ExecutionPlanner::on_fill`], which the
//! consumer runs after every fill and every order update that reports one.
//!
//! The planner reads the registry for every order's state, so the consumer reports what it
//! sends before the next pass, as for any command: a place's outcome, an amend's
//! [`Registry::amend_sent`] and a cancel's [`Registry::cancel_sent`]. A cancel not reported
//! sent is built again at the next pass.
//!
//! One planner may serve several accounts: it holds the orders it placed per account and
//! market, so a pass for one account never frees another's levels, and binds each account to
//! the registry of its first pass. A pass for an account through another registry, or through
//! a registry bound to another account, is refused with nothing built or freed ([`PlanError`];
//! 0068).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::time::Duration;

use fbc_core::{
    AccountKey, Bps, Channel, CidMint, ClientOrderId, Feature, IdError, InstrumentId, Lots, MonoNs,
    NewOrder, OrderCaps, OrderKind, RefKind, Side, Ticks, Tif,
};

use crate::entry::EntryState;
use crate::grant::Authorization;
use crate::permit::{self, AmendRefusal, CancelChoice, PermitRefusal, PermittedCommand};
use crate::record::{Intent, OrdState, OrderRecord, TerminalKind};
use crate::registry::{Instance, OmsError, Registry};

/// The [`Planned::level`], [`Held::level`] and [`Refused::level`] of a side's crossing intent's
/// order, which is at no level of the book ([`Planned::crossing`]).
pub const CROSSING_LEVEL: u16 = u16::MAX;

/// A side's price-capped reduce-only crossing exit (decision 0107): a disaster stop, a news
/// flatten window or the operator's Flatten. The side is the order's: a sell crossing reduces a
/// long. Planned as one reduce-only order priced at `cap`, immediate-or-cancel where the venue
/// takes IOC with reduce-only and otherwise a good-till-cancelled limit at `cap` followed by its
/// cancel ([`Downgrade::SynthIoc`]), sized at most `qty` and at most the position less the other
/// orders reducing it on the side, so the position never crosses zero.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct CrossingIntent {
    /// The worst price the order may trade at: a sell's lowest, a buy's highest. The order's
    /// limit price.
    pub cap: Ticks,
    /// The most to trade; zero means no intent.
    pub qty: Lots,
    /// The consumer's opaque tag, as [`DesiredQuote::tag`].
    pub tag: u16,
    /// The consumer's attempt, counted up: at most one order is planned per attempt, so an
    /// order that ended (filled, its unfilled rest expired, or refused by the venue) is not
    /// crossed again until the consumer names a later attempt, which is planned once the
    /// previous order is terminal; an attempt older than the last one built is never planned
    /// ([`PlanRefusal::StaleAttempt`]). An attempt refused before anything was built (by the
    /// market's state, a cap, or with nothing left to reduce) sent nothing, and is planned
    /// again at a later pass, as is one whose order ended never sent.
    pub attempt: u64,
}

/// What the consumer wants resting at one level of one side.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct DesiredQuote {
    /// The limit price.
    pub px: Ticks,
    /// The quantity to rest: an order's remaining quantity, its filled part not included. Zero
    /// means no order at the level.
    pub qty: Lots,
    pub tif: Tif,
    pub channel: Channel,
    pub post_only: bool,
    /// The venue's reduce-only flag.
    pub reduce_only: bool,
    /// The consumer's classification of the order as one that can only reduce the position,
    /// as on [`NewOrder::reducing`]: it orders the command among the reducing ones and exempts
    /// it from no check (0013 rule 2).
    pub reducing: bool,
    /// The consumer's opaque tag for the level's order (its origin, say): copied onto every
    /// place and amend the planner builds for the level ([`Planned::tag`]), kept on the order
    /// ([`OrderRecord::tag`]) and carried by the audit sink's verdicts. fbc-oms gives it no
    /// meaning (0001; 0098).
    pub tag: u16,
    /// What a post-only, reduce-only quote becomes on a venue that refuses the two flags
    /// together ([`ReduceFallback`]); unused where the venue takes them together. No default:
    /// the consumer chooses per quote (a maker exit keeps the flag, inventory relief emulates
    /// it; design §4.10).
    pub reduce_fallback: ReduceFallback,
}

/// What a quote that is both post-only and reduce-only becomes on a venue whose
/// [`OrderCaps::flag_conflicts`] pair the two (design §4.10 step 4).
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum ReduceFallback {
    /// Post-only without the venue's reduce-only flag, classified reducing and sized by the
    /// OMS at most the position less the other orders resting that reduce it on the same
    /// side; re-checked after every fill (or order update reporting one) and every pass, and
    /// cancelled at once when it would overshoot ([`Downgrade::EmulatedReduceOnly`]).
    EmulateReduceOnly,
    /// Reduce-only without post-only, at the quote's price: a taker fill is acceptable (a
    /// force-close or a disaster stop; [`Downgrade::DroppedPostOnly`]).
    KeepReduceOnly,
}

/// A change the planner made to a quote the venue's [`OrderCaps`] do not take as asked
/// (design §4.10 steps 3 and 4), reported on each place and amend built from that quote
/// ([`Planned::downgrades`]). Chosen from the caps alone, never from the venue's name.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum Downgrade {
    /// The RPI channel was asked for on a venue without it: the order goes on the public book.
    RpiUnsupported,
    /// The venue refuses RPI with reduce-only: the order goes on the public book, still
    /// reduce-only.
    RpiReduceOnly,
    /// The venue refuses IOC with reduce-only, or, for a crossing intent ([`CrossingIntent`]),
    /// takes no IOC: a good-till-cancelled reduce-only limit at the quote's price (its cap),
    /// cancelled at once.
    SynthIoc,
    /// The venue refuses post-only with reduce-only and the quote asked to emulate it
    /// ([`ReduceFallback::EmulateReduceOnly`]).
    EmulatedReduceOnly,
    /// The venue refuses post-only with reduce-only and the quote asked to keep reduce-only
    /// ([`ReduceFallback::KeepReduceOnly`]).
    DroppedPostOnly,
}

impl DesiredQuote {
    /// The quote reduces the position: the venue's flag or the consumer's classification.
    fn reduces(&self) -> bool {
        self.reducing || self.reduce_only
    }
}

/// The book a consumer wants resting on one market: a quote per side and level, level 0
/// nearest the touch. Defined here so strategy code produces it and this crate depends on no
/// strategy (0001, 0005).
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct DesiredBook {
    market: InstrumentId,
    levels: BTreeMap<Level, DesiredQuote>,
    /// Each side's removal reason, bids first.
    removal: [Option<u16>; 2],
    /// Each side's crossing intent, bids first.
    crossing: [Option<CrossingIntent>; 2],
}

impl DesiredBook {
    /// An empty book on `market`: planned, it cancels every order the planner has there.
    pub fn new(market: InstrumentId) -> DesiredBook {
        DesiredBook {
            market,
            levels: BTreeMap::new(),
            removal: [None; 2],
            crossing: [None; 2],
        }
    }

    /// The book with `reason` as `side`'s removal reason, replacing any there.
    pub fn with_removal(mut self, side: Side, reason: Option<u16>) -> DesiredBook {
        self.set_removal(side, reason);
        self
    }

    /// Sets `side`'s removal reason, the consumer's opaque reason its levels no longer wanted
    /// went away: copied onto every cancel the planner builds for a level of `side` the book
    /// no longer wants (absent, or wanted at quantity zero; [`Planned::removal`]), kept on the
    /// order ([`OrderRecord::removal`]) and carried by the audit sink's verdicts. A cancel the
    /// planner builds for a level still wanted (a replace, or Exit's on the side that adds)
    /// carries none. fbc-oms gives it no meaning (0001; 0098).
    pub fn set_removal(&mut self, side: Side, reason: Option<u16>) {
        self.removal[usize::from(Level::new(side, 0).side)] = reason;
    }

    /// `side`'s removal reason.
    pub fn removal(&self, side: Side) -> Option<u16> {
        self.removal[usize::from(Level::new(side, 0).side)]
    }

    /// The book with `quote` at `level` of `side`, replacing any quote there.
    pub fn with(mut self, side: Side, level: u16, quote: DesiredQuote) -> DesiredBook {
        self.set(side, level, quote);
        self
    }

    /// Sets `quote` at `level` of `side`, replacing any quote there.
    pub fn set(&mut self, side: Side, level: u16, quote: DesiredQuote) {
        self.levels.insert(Level::new(side, level), quote);
    }

    /// Removes the quote at `level` of `side`.
    pub fn remove(&mut self, side: Side, level: u16) {
        self.levels.remove(&Level::new(side, level));
    }

    /// The book with `intent` as `side`'s crossing intent, replacing any there.
    pub fn with_crossing(mut self, side: Side, intent: CrossingIntent) -> DesiredBook {
        self.set_crossing(side, Some(intent));
        self
    }

    /// Sets `side`'s crossing intent (decision 0107), replacing any there; `None` withdraws
    /// it, so a crossing order of the side still live is cancelled with the side's removal
    /// reason, as a level no longer wanted is.
    pub fn set_crossing(&mut self, side: Side, intent: Option<CrossingIntent>) {
        self.crossing[usize::from(Level::new(side, 0).side)] = intent;
    }

    /// `side`'s crossing intent.
    pub fn crossing(&self, side: Side) -> Option<&CrossingIntent> {
        self.crossing[usize::from(Level::new(side, 0).side)].as_ref()
    }

    /// The market.
    pub fn market(&self) -> InstrumentId {
        self.market
    }

    /// The quote at `level` of `side`.
    pub fn quote(&self, side: Side, level: u16) -> Option<&DesiredQuote> {
        self.levels.get(&Level::new(side, level))
    }
}

/// One level of one side, or its crossing intent's order: bids before asks, then the levels,
/// then the crossing intent's.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
struct Level {
    /// 0 for bids, 1 for asks.
    side: u8,
    /// The side's crossing intent's order, at [`CROSSING_LEVEL`].
    crossing: bool,
    level: u16,
}

impl Level {
    fn new(side: Side, level: u16) -> Level {
        let side = match side {
            Side::Buy => 0,
            Side::Sell => 1,
        };
        Level {
            side,
            crossing: false,
            level,
        }
    }

    /// `side`'s crossing intent's order.
    fn crossing(side: Side) -> Level {
        Level {
            crossing: true,
            level: CROSSING_LEVEL,
            ..Level::new(side, 0)
        }
    }

    fn side(self) -> Side {
        if self.side == 0 {
            Side::Buy
        } else {
            Side::Sell
        }
    }
}

/// The consumer's replace thresholds and minimum age (0009): no default.
#[derive(Copy, Clone, PartialEq, Debug)]
pub struct PlannerConfig {
    px_bps: Bps,
    qty: Lots,
    min_age: Duration,
}

/// Why a [`PlannerConfig`] was refused.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum PlannerConfigError {
    /// The price threshold is negative, infinite or not a number.
    PriceThreshold,
}

impl fmt::Display for PlannerConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlannerConfigError::PriceThreshold => write!(
                f,
                "the price replace threshold must be a finite, non-negative number of basis points"
            ),
        }
    }
}

impl std::error::Error for PlannerConfigError {}

impl PlannerConfig {
    /// A resting order is changed when its price moved by at least `px_bps` basis points of
    /// its price, or its resting quantity by at least `qty`, and it is at least `min_age` old
    /// (since the planner placed or amended it). A threshold of zero changes the order on any
    /// difference. Refused when `px_bps` is negative, infinite or not a number.
    pub fn new(
        px_bps: Bps,
        qty: Lots,
        min_age: Duration,
    ) -> Result<PlannerConfig, PlannerConfigError> {
        if !px_bps.0.is_finite() || px_bps.0 < 0.0 {
            return Err(PlannerConfigError::PriceThreshold);
        }
        Ok(PlannerConfig {
            px_bps,
            qty,
            min_age,
        })
    }

    /// The price replace threshold.
    pub fn px_bps(&self) -> Bps {
        self.px_bps
    }

    /// The quantity replace threshold.
    pub fn qty(&self) -> Lots {
        self.qty
    }

    /// The minimum age of an order before it is changed.
    pub fn min_age(&self) -> Duration {
        self.min_age
    }
}

/// Where in 0005's order a planned command goes.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub enum Stage {
    /// A cancel: of a level no longer wanted, or of an order being replaced.
    Cancel,
    /// A place or amend that reduces the position (the venue's reduce-only flag or the
    /// consumer's reducing classification).
    Reducing,
    /// An amend that does not reduce.
    Amend,
    /// A place that does not reduce.
    Add,
}

/// One command the planner built, authorized.
#[derive(Debug)]
pub struct Planned {
    pub stage: Stage,
    pub side: Side,
    /// The level, or [`CROSSING_LEVEL`] for the crossing intent's order.
    pub level: u16,
    /// The command is for the side's crossing intent's order ([`CrossingIntent`]), at no level.
    pub crossing: bool,
    /// The order the command is for.
    pub cid: ClientOrderId,
    /// The consumer's tag: a place's or an amend's is its level's [`DesiredQuote::tag`]; a
    /// cancel's is the order's, from the last place or amend built for it (0098), `None` for an
    /// order the planner did not tag, so a cancel of one is still built (Stop included).
    pub tag: Option<u16>,
    /// A cancel's removal reason: its side's [`DesiredBook::removal`] when the book no longer
    /// wants the level; `None` for a place, an amend, and a cancel of a level still wanted.
    pub removal: Option<u16>,
    /// What the planner changed in the level's quote to suit the venue's caps, in the order it
    /// applied them: a place's and an amend's; empty for a cancel and for a quote sent as asked.
    pub downgrades: Vec<Downgrade>,
    /// The authorization to submit it under.
    pub auth: Authorization,
}

/// Why a level's order was left as it is.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum HeldReason {
    /// The order is PendingNew or Unknown, or on the Unknown ladder: the level is occupied
    /// until it is terminal or resting.
    Unsettled(OrdState),
    /// An amend or cancel is in flight, an amend was built and not reported sent, or an amend
    /// replaced in flight is not yet settled ([`OrderRecord::amend_unconfirmed`]).
    InFlight,
    /// The order differs from the quote but is younger than the minimum age.
    Young,
    /// The order is being replaced: the new one is placed once it is terminal.
    Replacing,
    /// The crossing intent's order is live: it is never amended or replaced, and the next
    /// attempt is planned once it is terminal (decision 0107).
    Crossing,
}

/// A level whose order the planner left as it is this pass.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct Held {
    pub side: Side,
    /// The level, or [`CROSSING_LEVEL`] for the crossing intent's order.
    pub level: u16,
    /// The crossing intent's order ([`CrossingIntent`]).
    pub crossing: bool,
    pub cid: ClientOrderId,
    pub why: HeldReason,
}

/// Why the planner built nothing for a level.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum PlanRefusal {
    /// The place was refused (the market's state, a cap, or its client id): never built.
    Place(OmsError),
    /// The amend was refused (the market's state or a cap): never built.
    Amend(AmendRefusal),
    /// The order has no permit for the command.
    Permit(PermitRefusal),
    /// No client id could be minted.
    Mint(IdError),
    /// An emulated reduce-only quote ([`ReduceFallback::EmulateReduceOnly`]) or a crossing
    /// intent ([`CrossingIntent`]) has nothing to reduce: the position is flat, unknown or on
    /// its side, or the other orders reducing it on that side already cover it.
    NoRoomToReduce,
    /// The venue takes no reduce-only order ([`OrderCaps::reduce_only`]): a crossing intent is
    /// never sent without the flag (decision 0107).
    ReduceOnlyUnsupported,
    /// The venue takes no reduce-only IOC and cannot emulate one: it has no good-till-cancelled
    /// limit, or no single cancel that can name the order once acknowledged (by venue id or
    /// client id), so the synthetic IOC's limit could rest and fill later (decision 0107).
    NoSyntheticIoc,
    /// A crossing attempt older than the last one built for the side
    /// ([`CrossingIntent::attempt`]): attempts only increase, so a stale one is never planned
    /// (decision 0107).
    StaleAttempt {
        /// The last attempt an order was built for.
        last: u64,
    },
    /// A place or amend that reduces the position would leave an emulated reduce-only order
    /// resting on its side more than is left to reduce, so both could fill and take the
    /// position past zero: the emulated order is cancelled, and the command is built by the
    /// first pass after that order is terminal (decision 0106). Only a command the market's
    /// state and both caps would admit with that order ended waits so; one refused anyway is
    /// refused for its own reason, and the emulated order rests.
    AwaitsEmulatedCancel,
}

/// Why a whole [`ExecutionPlanner::plan`] pass was refused, nothing built or freed: the
/// planner plans each account through one registry and each registry for one account
/// (decision 0068).
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum PlanError {
    /// The account was planned through another registry before: this one does not hold its
    /// orders (another account's registry, or one rebuilt).
    OtherRegistry { acct: AccountKey },
    /// The registry was planned for another account before.
    OtherAccount { acct: AccountKey, bound: AccountKey },
}

impl fmt::Display for PlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PlanError::OtherRegistry { acct } => write!(
                f,
                "account {acct:?} was planned through another registry; nothing is planned through this one"
            ),
            PlanError::OtherAccount { acct, bound } => write!(
                f,
                "the registry was planned for account {bound:?}; nothing is planned through it for {acct:?}"
            ),
        }
    }
}

impl std::error::Error for PlanError {}

/// A level the planner built nothing for this pass, and why.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct Refused {
    pub side: Side,
    /// The level, or [`CROSSING_LEVEL`] for the crossing intent.
    pub level: u16,
    /// The crossing intent ([`CrossingIntent`]).
    pub crossing: bool,
    pub why: PlanRefusal,
}

/// What one [`ExecutionPlanner::plan`] pass built.
#[derive(Debug, Default)]
pub struct Plan {
    /// The commands, in 0005's order: cancels, reducing orders (those the venue or the consumer
    /// bounds before the emulated reduce-only ones), amends, adds; a synthetic IOC's cancel
    /// follows its place, and the cancel of an emulated reduce-only order a reducer would leave
    /// resting more than is left to reduce comes where that reducer would have been built, the
    /// reducer waiting ([`PlanRefusal::AwaitsEmulatedCancel`]; 0106).
    pub commands: Vec<Planned>,
    /// The orders whose cancel waits for their acknowledgement: tried again at every later pass
    /// until it is built, whether or not the level is wanted again.
    pub awaiting_ack: Vec<ClientOrderId>,
    /// The levels left as they are.
    pub held: Vec<Held>,
    /// The levels a command was refused for.
    pub refused: Vec<Refused>,
}

/// The order the planner holds at a level.
#[derive(Copy, Clone, Debug)]
struct Slot {
    cid: ClientOrderId,
    /// When the planner last placed or amended it.
    changed_at: MonoNs,
    /// It is cancelled to be replaced, or its cancel waited for the acknowledgement when the
    /// level was wanted again: the level waits for its terminal state.
    replacing: bool,
    /// The order's [`OrderRecord::cancels_sent`] when the replace's cancel was last decided: a
    /// count past it with no cancel in flight means that cancel was sent and answered without
    /// ending the order (not sent, or refused), so the replace is over.
    cancels_sent: u64,
    /// How the order stands in for a quote the venue did not take as asked.
    mode: Mode,
}

/// How an order the planner placed stands in for a downgraded quote (design §4.10 step 4).
#[derive(Copy, Clone, Default, Eq, PartialEq, Debug)]
struct Mode {
    /// A synthetic IOC ([`Downgrade::SynthIoc`]): cancelled at once, whatever the book wants.
    synth_ioc: bool,
    /// An emulated reduce-only order ([`Downgrade::EmulatedReduceOnly`]): it never rests more
    /// than is left to reduce, and is cancelled when it would.
    emulated: bool,
}

/// A quote as the venue's caps take it: the quote changed by each fallback, how its order
/// stands in for it, and the downgrades applied, in order.
#[derive(Clone, Debug)]
struct Fitted {
    quote: DesiredQuote,
    mode: Mode,
    downgrades: Vec<Downgrade>,
    /// A crossing intent's attempt: the place is sized to what is left to reduce when built,
    /// and the attempt recorded once it is.
    attempt: Option<u64>,
}

/// What a pass decided for a level, before anything is built.
enum Action {
    /// A cancel, with the side's removal reason when the book no longer wants the level.
    Cancel {
        cid: ClientOrderId,
        removal: Option<u16>,
    },
    Amend {
        cid: ClientOrderId,
        px: Ticks,
        total: Lots,
        reducing: bool,
        tag: u16,
        /// For an emulated reduce-only order, the quantity it should rest, clipped again when
        /// the amend is built to what is left to reduce then.
        emulated: Option<Lots>,
        downgrades: Vec<Downgrade>,
    },
    Place(Fitted),
}

impl Action {
    /// Whether the action places or amends an emulated reduce-only order.
    fn emulated(&self) -> bool {
        match self {
            Action::Cancel { .. } => false,
            Action::Amend { emulated, .. } => emulated.is_some(),
            Action::Place(f) => f.mode.emulated,
        }
    }
}

/// The planner (decision 0005's one planner; 0065): the orders it placed, by account, market,
/// side and level, the registry each account is planned through (0068), and the consumer's
/// thresholds.
#[derive(Debug)]
pub struct ExecutionPlanner {
    config: PlannerConfig,
    slots: HashMap<(AccountKey, InstrumentId), BTreeMap<Level, Slot>>,
    /// Each account's registry, bound at the account's first pass.
    bound: HashMap<AccountKey, Instance>,
    /// The last crossing attempt an order was built for, by account, market and side
    /// (decision 0107).
    fired: HashMap<(AccountKey, InstrumentId, Level), Fired>,
}

/// The last crossing attempt an order was built for on one side (decision 0107).
#[derive(Copy, Clone, Debug)]
struct Fired {
    attempt: u64,
    /// The order may have reached the venue; false once it ended never sent, so the attempt
    /// is planned again.
    spent: bool,
}

impl ExecutionPlanner {
    /// A planner holding no order, under the consumer's thresholds.
    pub fn new(config: PlannerConfig) -> ExecutionPlanner {
        ExecutionPlanner {
            config,
            slots: HashMap::new(),
            bound: HashMap::new(),
            fired: HashMap::new(),
        }
    }

    /// The order the planner holds for `acct` at `level` of `side` on `market`, until it is seen
    /// terminal.
    pub fn order_at(
        &self,
        acct: AccountKey,
        market: InstrumentId,
        side: Side,
        level: u16,
    ) -> Option<ClientOrderId> {
        self.slots
            .get(&(acct, market))?
            .get(&Level::new(side, level))
            .map(|s| s.cid)
    }

    /// The order the planner holds for `acct` for `side`'s crossing intent on `market`, until
    /// it is seen terminal (decision 0107).
    pub fn crossing_order(
        &self,
        acct: AccountKey,
        market: InstrumentId,
        side: Side,
    ) -> Option<ClientOrderId> {
        self.slots
            .get(&(acct, market))?
            .get(&Level::crossing(side))
            .map(|s| s.cid)
    }

    /// One pass over `desired`'s market at `now`: decides each level, then builds, in 0005's
    /// order, each command through `reg` (places with client ids from `mint`, amends from
    /// [`Live`](crate::Live) permits, cancels from [`Cancellable`](crate::Cancellable) ones,
    /// all under the venue's `caps`) and authorizes it for `acct`.
    ///
    /// `reg` is `acct`'s registry: the planner holds each account's orders apart, so a pass for
    /// one account never frees another's levels, and binds `acct` to `reg` at its first pass.
    /// A pass for `acct` through another registry, or through `reg` for another account, is
    /// refused with nothing built or freed ([`PlanError`]; 0068). Like every mutating call on
    /// `reg`, a pass, a refused one included, first releases the places whose commands were
    /// dropped unspent (decision 0084).
    pub fn plan(
        &mut self,
        desired: &DesiredBook,
        reg: &mut Registry,
        caps: &OrderCaps,
        acct: AccountKey,
        mint: &mut CidMint,
        now: MonoNs,
    ) -> Result<Plan, PlanError> {
        reg.reap_dropped();
        self.bind(acct, reg)?;
        let market = desired.market;
        let config = self.config;
        let slots = self.slots.entry((acct, market)).or_default();
        let mut plan = Plan::default();
        // An order seen terminal frees its level.
        free_terminal(slots, reg, &mut self.fired, (acct, market));

        // In Exit, the side that reduces the known position; none while it is flat or unknown,
        // when every order adds to it (0063).
        let exit_reduces = matches!(reg.entry(market).state(), EntryState::Exit(_)).then(|| {
            reg.position(market).and_then(|pos| match pos.0.signum() {
                1 => Some(Side::Sell),
                -1 => Some(Side::Buy),
                _ => None,
            })
        });

        let mut actions: Vec<(Stage, Level, Action)> = Vec::new();
        // Orders whose level is wanted again with no cancel in flight: a removal cancel built
        // for them and never reported sent ended nothing, so they keep no reason (0098).
        let mut kept: Vec<ClientOrderId> = Vec::new();
        let crossings = [Side::Buy, Side::Sell]
            .into_iter()
            .filter(|&side| desired.crossing(side).is_some())
            .map(Level::crossing);
        let levels: BTreeSet<Level> = slots
            .keys()
            .chain(desired.levels.keys())
            .copied()
            .chain(crossings)
            .collect();
        for at in levels {
            // The quote as the venue's caps take it (design §4.10 steps 3 and 4), or the
            // crossing intent's order (0107).
            let fitted = if at.crossing {
                desired
                    .crossing(at.side())
                    .filter(|c| c.qty > Lots::ZERO)
                    .map(|c| fit_crossing(c, caps))
            } else {
                desired
                    .levels
                    .get(&at)
                    .filter(|q| q.qty > Lots::ZERO)
                    .map(|q| fit(q, caps))
            };
            let Some(slot) = slots.get_mut(&at) else {
                // A crossing attempt an order was built for is never planned again, nor one
                // older than it (0107): attempts only increase.
                let last = self.fired.get(&(acct, market, at)).copied();
                let fitted = match (fitted, last) {
                    (Some(f), Some(last)) if f.attempt.is_some_and(|a| a < last.attempt) => {
                        plan.refused.push(Refused {
                            side: at.side(),
                            level: at.level,
                            crossing: at.crossing,
                            why: PlanRefusal::StaleAttempt { last: last.attempt },
                        });
                        None
                    }
                    (Some(f), Some(last)) if f.attempt == Some(last.attempt) && last.spent => None,
                    (fitted, _) => fitted,
                };
                if let Some(f) = fitted {
                    let stage = if f.quote.reduces() {
                        Stage::Reducing
                    } else {
                        Stage::Add
                    };
                    actions.push((stage, at, Action::Place(f)));
                }
                continue;
            };
            // An order standing in for a quote that is now a synthetic IOC or an emulated
            // reduce-only order of the same wire shape takes that mode: cancelled at once as
            // an IOC, or protected from overshoot from now on. A mode is never dropped while
            // the order rests. An order whose quote reduces the position (an emulated one's
            // included) counts from then on as an order reducing it, though no amend records
            // the classification, so emulated orders are sized with it (0106).
            if let Some(f) = &fitted {
                slot.mode.synth_ioc |= f.mode.synth_ioc;
                slot.mode.emulated |= f.mode.emulated;
                if f.quote.reduces() {
                    reg.with_record(slot.cid, OrderRecord::set_counted_reducing);
                }
            }
            let rec = reg
                .get(slot.cid)
                .expect("terminal and unknown orders were freed");
            // A replace whose cancel was sent and answered without ending the order (not sent,
            // or refused) is over: the order rests as it was and the level is decided afresh
            // (0075). A cancel built and not reported sent is built again (0065 rule 7).
            if slot.replacing
                && !matches!(rec.intent(), Intent::PendingCancel { .. })
                && rec.cancels_sent() > slot.cancels_sent
            {
                slot.replacing = false;
            }
            let hold = |why| Held {
                side: at.side(),
                level: at.level,
                crossing: at.crossing,
                cid: slot.cid,
                why,
            };
            // In Exit, an order on a side that does not reduce the position is cancelled
            // whatever the book wants there: Exit builds nothing on that side (0063).
            let adds_in_exit = exit_reduces.is_some_and(|reduces| reduces != Some(at.side()));
            // An emulated reduce-only order rests at most what is left to reduce; one resting
            // more would take the position past zero, and is cancelled at once, whatever its
            // age or the book (FBC-01w).
            let room = slot
                .mode
                .emulated
                .then(|| reduce_room(reg, market, at.side(), Some(slot.cid)));
            let overshoots = room.is_some_and(|room| rec.resting() > room);
            // A synthetic IOC is cancelled at once, whatever the book wants (FBC-01w).
            let Some(f) = fitted.as_ref().filter(|_| {
                !slot.replacing && !adds_in_exit && !slot.mode.synth_ioc && !overshoots
            }) else {
                if overshoots && fitted.is_some() {
                    slot.replacing = true;
                }
                if matches!(rec.intent(), Intent::PendingCancel { .. }) {
                    if slot.replacing {
                        plan.held.push(hold(HeldReason::Replacing));
                    }
                } else {
                    slot.cancels_sent = rec.cancels_sent();
                    // The book no longer wants the level: the side's reason goes with the
                    // cancel; a level still wanted (a replace, Exit's, an overshoot or a
                    // synthetic IOC's) has none (0098).
                    let removal = fitted.as_ref().map_or(desired.removal(at.side()), |_| None);
                    let cancel = Action::Cancel {
                        cid: slot.cid,
                        removal,
                    };
                    actions.push((Stage::Cancel, at, cancel));
                }
                continue;
            };
            // An emulated order, whatever its quote now asks (the mode is never dropped while
            // it rests), is compared with the quote clipped to what is left to reduce, and is
            // amended as the reducer it still is: among the reducers, classified reducing, so
            // Exit admits it (Codex r4315460452).
            let clipped;
            let q = match room {
                Some(room) => {
                    clipped = DesiredQuote {
                        qty: f.quote.qty.min(room),
                        reducing: true,
                        ..f.quote.clone()
                    };
                    &clipped
                }
                None => &f.quote,
            };
            if rec.removal().is_some() && !matches!(rec.intent(), Intent::PendingCancel { .. }) {
                kept.push(slot.cid);
            }
            // A cancel decided for the order and waiting for its acknowledgement is carried
            // through: the level is replaced once the order is terminal, never left resting at
            // the price the cancel was decided against.
            if rec.cancel_awaits_ack() {
                slot.replacing = true;
                slot.cancels_sent = rec.cancels_sent();
                let cancel = Action::Cancel {
                    cid: slot.cid,
                    removal: None,
                };
                actions.push((Stage::Cancel, at, cancel));
                continue;
            }
            if let Some(why) = unsettled(rec) {
                plan.held.push(hold(why));
                continue;
            }
            // A live crossing order ends by itself (an IOC) or by its cancel (a synthetic
            // one, above): never amended or replaced (0107).
            if at.crossing {
                plan.held.push(hold(HeldReason::Crossing));
                continue;
            }
            if !config.differs(rec, q) {
                continue;
            }
            let age = u128::from(now.0.saturating_sub(slot.changed_at.0));
            if age < config.min_age.as_nanos() {
                plan.held.push(hold(HeldReason::Young));
                continue;
            }
            let total = rec.filled().checked_add(q.qty).filter(|&total| {
                same_flags(rec.placed(), q)
                    && permit::amend_shape(rec, caps, q.px, total, q.reducing).is_ok()
            });
            match total {
                Some(total) => {
                    // An order counted a reducer is amended among the reducers whatever its
                    // quote now asks, so it is built before the emulated orders sized against
                    // it (Reviewer B B13).
                    let reducing = q.reducing || rec.reduces();
                    let stage = if reducing {
                        Stage::Reducing
                    } else {
                        Stage::Amend
                    };
                    let amend = Action::Amend {
                        cid: slot.cid,
                        px: q.px,
                        total,
                        reducing: q.reducing,
                        tag: q.tag,
                        emulated: slot.mode.emulated.then_some(f.quote.qty),
                        downgrades: f.downgrades.clone(),
                    };
                    actions.push((stage, at, amend));
                }
                None => {
                    slot.replacing = true;
                    slot.cancels_sent = rec.cancels_sent();
                    let cancel = Action::Cancel {
                        cid: slot.cid,
                        removal: None,
                    };
                    actions.push((Stage::Cancel, at, cancel));
                }
            }
        }

        for cid in kept {
            reg.with_record(cid, |rec| rec.set_removal(None));
        }

        // Built in 0005's order, so each is judged with the earlier ones counted; among the
        // reducing commands, those the venue or the consumer bounds come before the emulated
        // ones, so an emulated order is sized and grown with every other reducer counted.
        actions.sort_by_key(|(stage, at, action)| (*stage, action.emulated(), *at));
        // The orders a cancel was decided for this pass, which the overshoot check skips.
        let mut cancelling: Vec<ClientOrderId> = Vec::new();
        for (stage, at, action) in actions {
            let side = at.side();
            let refuse = |why| Refused {
                side,
                level: at.level,
                crossing: at.crossing,
                why,
            };
            // Each built command's order keeps the consumer's tag and removal reason the
            // command carries (0098). A synthetic IOC's place is followed by its cancel.
            let mut synth_ioc = false;
            let built = match action {
                Action::Cancel { cid, removal } => {
                    cancelling.push(cid);
                    match reg.cancellable(cid) {
                        Ok(permit) => match permit.cancel(caps) {
                            CancelChoice::Send(cmd) => {
                                let tag = reg.with_record(cid, |rec| {
                                    rec.set_removal(removal);
                                    rec.tag()
                                });
                                Ok((cid, cmd, tag, removal, Vec::new()))
                            }
                            CancelChoice::AwaitAck => {
                                plan.awaiting_ack.push(cid);
                                continue;
                            }
                        },
                        Err(why) => Err(PlanRefusal::Permit(why)),
                    }
                }
                Action::Amend {
                    cid,
                    px,
                    total,
                    reducing,
                    tag,
                    emulated,
                    downgrades,
                } => {
                    // An emulated order grows only to what is left to reduce once the
                    // commands before it are counted, and is not amended with nothing left
                    // (the overshoot checks decide) or when a reducer's wait cancelled it
                    // this pass.
                    let clipped = match emulated {
                        Some(want) => {
                            let rests = want.min(reduce_room(reg, market, side, Some(cid)));
                            let filled = reg.get(cid).map_or(Lots::ZERO, OrderRecord::filled);
                            filled.checked_add(rests).filter(|_| rests > Lots::ZERO)
                        }
                        None => Some(total),
                    };
                    let Some(total) = clipped.filter(|_| !cancelling.contains(&cid)) else {
                        continue;
                    };
                    // A reducer amended without shrinking, a price-only amend included,
                    // waits for the cancel of each emulated order it would leave
                    // overshooting: one the amend makes a reducer, or one counted as a
                    // reducer since a pass that could not amend it, is a new reducer, and an
                    // amend over a cancel not yet settled would let both fill. One that
                    // shrinks lowers what may fill and never waits (Codex r4310495940).
                    if emulated.is_none() {
                        // An order amended as reducing was counted a reducer when its quote
                        // was read above, so the other orders' room already counts what it
                        // rests: only its growth past that is new.
                        let grows = reg.get(cid).filter(|rec| rec.reduces()).and_then(|rec| {
                            let rests = total.checked_sub(rec.filled()).unwrap_or(Lots::ZERO);
                            rests.checked_sub(rec.resting())
                        });
                        let overshot = grows.map_or_else(Vec::new, |grows| {
                            overshot_by(slots, reg, (market, side), grows)
                        });
                        if !overshot.is_empty() {
                            // Judged first with those orders ended: one refused anyway reports
                            // its own refusal and cancels nothing.
                            let ending: Vec<ClientOrderId> =
                                overshot.iter().map(|&(_, cid)| cid).collect();
                            let why = match reg.admits_amend_once_ended(
                                cid,
                                caps,
                                (px, total, reducing),
                                &ending,
                            ) {
                                Ok(()) => {
                                    let cancels = (&mut cancelling, &mut plan);
                                    cancel_overshot(slots, reg, caps, acct, &overshot, cancels);
                                    PlanRefusal::AwaitsEmulatedCancel
                                }
                                Err(why) => why,
                            };
                            plan.refused.push(refuse(why));
                            continue;
                        }
                    }
                    reg.live(cid)
                        .map_err(PlanRefusal::Permit)
                        .and_then(|live| {
                            live.amend(caps, px, total, reducing)
                                .map_err(PlanRefusal::Amend)
                        })
                        .map(|cmd| {
                            if let Some(slot) = slots.get_mut(&at) {
                                slot.changed_at = now;
                            }
                            reg.with_record(cid, |rec| rec.set_tag(tag));
                            (cid, cmd, Some(tag), None, downgrades)
                        })
                }
                Action::Place(Fitted {
                    mut quote,
                    mode,
                    downgrades,
                    attempt,
                }) => {
                    // A synthetic IOC needs a good-till-cancelled limit and a cancel that can
                    // name the order once acknowledged; without either its limit could rest
                    // and fill later (Codex r4317142581).
                    if mode.synth_ioc && !synthesizes_ioc(caps) {
                        plan.refused.push(refuse(PlanRefusal::NoSyntheticIoc));
                        continue;
                    }
                    // A crossing intent is sent only with the venue's reduce-only flag, and is
                    // sized to what is left to reduce, the commands before it counted (0107).
                    if attempt.is_some() {
                        if !caps.reduce_only {
                            plan.refused
                                .push(refuse(PlanRefusal::ReduceOnlyUnsupported));
                            continue;
                        }
                        quote.qty = quote.qty.min(crossing_room(slots, reg, market, side));
                        if quote.qty == Lots::ZERO {
                            plan.refused.push(refuse(PlanRefusal::NoRoomToReduce));
                            continue;
                        }
                    }
                    // A reducer waits for the cancel of each emulated order it would leave
                    // overshooting, once judged admitted with those orders ended: one refused
                    // anyway reports its own refusal and cancels nothing.
                    let overshot = if !mode.emulated && quote.reduces() {
                        overshot_by(slots, reg, (market, side), quote.qty)
                    } else {
                        Vec::new()
                    };
                    if !overshot.is_empty() {
                        let ending: Vec<ClientOrderId> =
                            overshot.iter().map(|&(_, cid)| cid).collect();
                        let why = match reg
                            .admits_place_once_ended(market, side, quote.qty, true, &ending)
                        {
                            Ok(()) => {
                                let cancels = (&mut cancelling, &mut plan);
                                cancel_overshot(slots, reg, caps, acct, &overshot, cancels);
                                PlanRefusal::AwaitsEmulatedCancel
                            }
                            Err(why) => PlanRefusal::Place(why),
                        };
                        plan.refused.push(refuse(why));
                        continue;
                    }
                    // An emulated order is sized to what is left to reduce, the commands
                    // before it counted.
                    if mode.emulated {
                        quote.qty = quote.qty.min(reduce_room(reg, market, side, None));
                        if quote.qty == Lots::ZERO {
                            plan.refused.push(refuse(PlanRefusal::NoRoomToReduce));
                            continue;
                        }
                    }
                    synth_ioc = mode.synth_ioc;
                    mint.mint()
                        .map_err(PlanRefusal::Mint)
                        .and_then(|cid| {
                            reg.place(new_order(market, side, cid, &quote))
                                .map_err(PlanRefusal::Place)
                                .map(|cmd| (cid, cmd))
                        })
                        .map(|(cid, cmd)| {
                            slots.insert(
                                at,
                                Slot {
                                    cid,
                                    changed_at: now,
                                    replacing: false,
                                    cancels_sent: 0,
                                    mode,
                                },
                            );
                            reg.with_record(cid, |rec| rec.set_tag(quote.tag));
                            if let Some(attempt) = attempt {
                                let spent = Fired {
                                    attempt,
                                    spent: true,
                                };
                                self.fired.insert((acct, market, at), spent);
                            }
                            (cid, cmd, Some(quote.tag), None, downgrades)
                        })
                }
            };
            match built {
                Ok((cid, cmd, tag, removal, downgrades)) => {
                    plan.commands.push(Planned {
                        stage,
                        side,
                        level: at.level,
                        crossing: at.crossing,
                        cid,
                        tag,
                        removal,
                        downgrades,
                        auth: authorize(reg, acct, cmd),
                    });
                    if synth_ioc {
                        cancelling.push(cid);
                        cancel_now(reg, caps, acct, at, cid, &mut plan);
                    }
                }
                Err(why) => plan.refused.push(refuse(why)),
            }
        }
        // Built last, the cancels of any emulated order the pass's builds still left resting
        // more than is left to reduce; none should be, as a reducer that would waits above.
        overshoot_cancels(slots, reg, caps, acct, market, &cancelling, &mut plan);
        Ok(plan)
    }

    /// The check to run after every fill on `market` for `acct`, and after every order update
    /// there that reports a fill before its fill event (design §4.10 step 4): the
    /// cancel of each emulated reduce-only order the planner holds there that now rests more
    /// than is left to reduce, so it never takes the position past zero. What is left is the
    /// position's size on the side the order reduces (none while the position is flat,
    /// unknown or on the order's side), less the fills on that side the venue reported on
    /// order updates that the inventory does not hold yet, less what the other orders reducing
    /// it on that side may have resting, a cancel in flight included until it is terminal. An order with a
    /// cancel in flight or waiting for its acknowledgement is skipped; a cancel that waits for
    /// the acknowledgement is listed in [`Plan::awaiting_ack`] and carried through by the
    /// next [`Self::plan`] pass, which runs the same check. Nothing is placed or amended, and
    /// the level, if still wanted, is placed again at the smaller size once the order is
    /// terminal. Refused as a pass is ([`PlanError`]) through a registry other than the
    /// account's; like every mutating call on `reg`, it first releases the places whose
    /// commands were dropped unspent (decision 0084).
    pub fn on_fill(
        &mut self,
        reg: &mut Registry,
        caps: &OrderCaps,
        acct: AccountKey,
        market: InstrumentId,
    ) -> Result<Plan, PlanError> {
        reg.reap_dropped();
        self.bind(acct, reg)?;
        let mut plan = Plan::default();
        if let Some(slots) = self.slots.get_mut(&(acct, market)) {
            free_terminal(slots, reg, &mut self.fired, (acct, market));
            overshoot_cancels(slots, reg, caps, acct, market, &[], &mut plan);
        }
        Ok(plan)
    }

    /// Binds `acct` to the registry `reg` at its first pass; refuses another registry for
    /// `acct`, or `reg` for another account: one the planner bound it to, or the one the
    /// registry is for (decision 0082). Client ids alone cannot tell two accounts' registries
    /// apart: accounts may lease the same namespace (0068).
    fn bind(&mut self, acct: AccountKey, reg: &Registry) -> Result<(), PlanError> {
        let instance = reg.instance();
        match self.bound.get(&acct) {
            Some(bound) if *bound != instance => return Err(PlanError::OtherRegistry { acct }),
            Some(_) => {}
            None => {
                if let Some((&bound, _)) = self.bound.iter().find(|(_, i)| **i == instance) {
                    return Err(PlanError::OtherAccount { acct, bound });
                }
            }
        }
        if let Some(bound) = reg.account().filter(|bound| *bound != acct) {
            return Err(PlanError::OtherAccount { acct, bound });
        }
        self.bound.insert(acct, instance);
        Ok(())
    }
}

impl PlannerConfig {
    /// Whether the resting order `rec` differs from `q` enough to change it: its flags, or its
    /// price or resting quantity by at least the thresholds.
    fn differs(&self, rec: &OrderRecord, q: &DesiredQuote) -> bool {
        if !same_flags(rec.placed(), q) {
            return true;
        }
        let px_moved = match rec.px() {
            Some(px) if px == q.px => false,
            Some(px) => moved_bps(px, q.px) >= self.px_bps.0,
            None => true,
        };
        let resting = rec.qty().checked_sub(rec.filled()).unwrap_or(Lots::ZERO);
        let qty_moved = resting != q.qty && lots_apart(resting, q.qty) >= self.qty;
        px_moved || qty_moved
    }
}

/// `q` as the venue's `caps` take it (design §4.10 steps 3 and 4), from the caps alone: RPI on
/// a venue without it goes on the public book; then, for a reduce-only quote, each pair the
/// venue refuses together (in either order) falls back: (Rpi, ReduceOnly) to the public book,
/// (Ioc, ReduceOnly) to a good-till-cancelled limit at the quote's price cancelled at once, and
/// (PostOnly, ReduceOnly) by the quote's [`ReduceFallback`]. An order moved off the RPI channel,
/// which only rests, stays post-only where the venue has post-only, so it never takes.
fn fit(q: &DesiredQuote, caps: &OrderCaps) -> Fitted {
    let refused = |a: Feature, b: Feature| {
        caps.flag_conflicts
            .iter()
            .any(|&(x, y)| (x, y) == (a, b) || (x, y) == (b, a))
    };
    let mut quote = q.clone();
    let mut mode = Mode::default();
    let mut downgrades = Vec::new();
    let mut go_public = |quote: &mut DesiredQuote, why| {
        quote.channel = Channel::Public;
        quote.post_only |= caps.post_only;
        downgrades.push(why);
    };
    if quote.channel == Channel::Rpi && !caps.channels.contains(Channel::Rpi) {
        go_public(&mut quote, Downgrade::RpiUnsupported);
    }
    if quote.reduce_only {
        if quote.channel == Channel::Rpi && refused(Feature::Rpi, Feature::ReduceOnly) {
            go_public(&mut quote, Downgrade::RpiReduceOnly);
        }
        if quote.tif == Tif::Ioc && refused(Feature::Ioc, Feature::ReduceOnly) {
            quote.tif = Tif::Gtc;
            mode.synth_ioc = true;
            downgrades.push(Downgrade::SynthIoc);
        }
        if quote.post_only && refused(Feature::PostOnly, Feature::ReduceOnly) {
            match q.reduce_fallback {
                ReduceFallback::EmulateReduceOnly => {
                    quote.reduce_only = false;
                    quote.reducing = true;
                    mode.emulated = true;
                    downgrades.push(Downgrade::EmulatedReduceOnly);
                }
                ReduceFallback::KeepReduceOnly => {
                    quote.post_only = false;
                    downgrades.push(Downgrade::DroppedPostOnly);
                }
            }
        }
    }
    Fitted {
        quote,
        mode,
        downgrades,
        attempt: None,
    }
}

/// The order a crossing intent asks for, as the venue's `caps` take it (decision 0107): a
/// reduce-only limit at the cap on the public book, never post-only, classified reducing;
/// immediate-or-cancel where the venue takes IOC, falling back as any reduce-only IOC quote
/// does where the venue refuses the two together, and otherwise a good-till-cancelled limit
/// cancelled at once ([`Downgrade::SynthIoc`]).
fn fit_crossing(c: &CrossingIntent, caps: &OrderCaps) -> Fitted {
    let ioc = caps.tifs.contains(Tif::Ioc);
    let quote = DesiredQuote {
        px: c.cap,
        qty: c.qty,
        tif: if ioc { Tif::Ioc } else { Tif::Gtc },
        channel: Channel::Public,
        post_only: false,
        reduce_only: true,
        reducing: true,
        tag: c.tag,
        reduce_fallback: ReduceFallback::KeepReduceOnly,
    };
    let mut fitted = fit(&quote, caps);
    if !ioc {
        fitted.mode.synth_ioc = true;
        fitted.downgrades.push(Downgrade::SynthIoc);
    }
    fitted.attempt = Some(c.attempt);
    fitted
}

/// Whether the venue can stand in for an IOC with a good-till-cancelled limit cancelled at once:
/// it takes GTC limits, and its single cancel can name the order once acknowledged, by venue
/// id or client id (a placement nonce alone is not counted on).
fn synthesizes_ioc(caps: &OrderCaps) -> bool {
    caps.tifs.contains(Tif::Gtc)
        && (caps.cancel_refs.contains(RefKind::Venue) || caps.cancel_refs.contains(RefKind::Client))
}

/// What is left for a crossing intent on `side` of `market` to reduce (decision 0107): as
/// [`reduce_room`] with no order excepted, but for the emulated reduce-only orders in `slots`,
/// which a crossing order is never clipped by: it waits for their cancel instead, as any
/// reducer that would leave one overshooting does (0106 rule 7).
fn crossing_room(
    slots: &BTreeMap<Level, Slot>,
    reg: &Registry,
    market: InstrumentId,
    side: Side,
) -> Lots {
    let emulated: Vec<ClientOrderId> = slots
        .iter()
        .filter(|(at, slot)| slot.mode.emulated && at.side() == side)
        .map(|(_, slot)| slot.cid)
        .collect();
    room_except(reg, market, side, &emulated)
}

/// What is left for an emulated reduce-only order on `side` of `market` to reduce: the
/// position's size when `side` reduces it (none while it is flat, unknown or on `side`), less
/// the fills on `side` the venue reported on order updates that the inventory does not hold
/// yet (they reduced it already), less what our other orders reducing it on `side` (all but
/// `except`) may have resting; zero when they cover it or a sum does not fit a lot count.
fn reduce_room(
    reg: &Registry,
    market: InstrumentId,
    side: Side,
    except: Option<ClientOrderId>,
) -> Lots {
    room_except(reg, market, side, except.as_slice())
}

/// [`reduce_room`] but for every order in `except`.
fn room_except(reg: &Registry, market: InstrumentId, side: Side, except: &[ClientOrderId]) -> Lots {
    let reducible = reg.position(market).and_then(|pos| {
        let reduces = match side {
            Side::Sell => pos.0 > 0,
            Side::Buy => pos.0 < 0,
        };
        reduces.then(|| pos.abs_lots()).flatten()
    });
    reducible
        .zip(reg.unaccounted_fills_on(market, side))
        .and_then(|(size, unaccounted)| size.checked_sub(unaccounted))
        .zip(reg.reducers_resting_on(market, side, except))
        .and_then(|(size, others)| size.checked_sub(others))
        .unwrap_or(Lots::ZERO)
}

/// Builds the cancel of our order `cid` at `at` into `plan`, at once: sent, waiting for the
/// acknowledgement, or refused its permit. It carries no removal reason: the level may still
/// be wanted.
fn cancel_now(
    reg: &mut Registry,
    caps: &OrderCaps,
    acct: AccountKey,
    at: Level,
    cid: ClientOrderId,
    plan: &mut Plan,
) {
    let (side, level) = (at.side(), at.level);
    match reg.cancellable(cid) {
        Ok(permit) => match permit.cancel(caps) {
            CancelChoice::Send(cmd) => {
                let tag = reg.with_record(cid, |rec| {
                    rec.set_removal(None);
                    rec.tag()
                });
                plan.commands.push(Planned {
                    stage: Stage::Cancel,
                    side,
                    level,
                    crossing: at.crossing,
                    cid,
                    tag,
                    removal: None,
                    downgrades: Vec::new(),
                    auth: authorize(reg, acct, cmd),
                });
            }
            CancelChoice::AwaitAck => plan.awaiting_ack.push(cid),
        },
        Err(why) => plan.refused.push(Refused {
            side,
            level,
            crossing: at.crossing,
            why: PlanRefusal::Permit(why),
        }),
    }
}

/// The emulated reduce-only orders in `slots` on `side` of `market`, not yet terminal, that a
/// reducer about to rest `grows` more (none for one amended at its size, whose resting the
/// room already counts) would leave resting more than is left to reduce, so both could fill
/// and take the position past zero, the emulated order having no venue flag to stop it. The
/// reducer waits for their cancel ([`cancel_overshot`]) when it is admitted with them ended,
/// and is built by the first pass after they are terminal (decision 0106).
fn overshot_by(
    slots: &BTreeMap<Level, Slot>,
    reg: &Registry,
    (market, side): (InstrumentId, Side),
    grows: Lots,
) -> Vec<(Level, ClientOrderId)> {
    slots
        .iter()
        .filter(|(at, slot)| slot.mode.emulated && at.side() == side)
        .filter(|(_, slot)| {
            reg.get(slot.cid).is_some_and(|rec| {
                // What is left for the emulated order once the reducer rests `grows` more.
                let left = reduce_room(reg, market, side, Some(rec.cid()))
                    .checked_sub(grows)
                    .unwrap_or(Lots::ZERO);
                !rec.state().is_terminal() && rec.resting() > left
            })
        })
        .map(|(&at, slot)| (at, slot.cid))
        .collect()
}

/// Builds into `plan` the cancel of each of the emulated orders `overshot` a reducer waits for
/// that is not already cancelling (listed in `cancelling`, which it joins, or with a cancel in
/// flight or waiting for its acknowledgement); its level then waits for its terminal state.
fn cancel_overshot(
    slots: &mut BTreeMap<Level, Slot>,
    reg: &mut Registry,
    caps: &OrderCaps,
    acct: AccountKey,
    overshot: &[(Level, ClientOrderId)],
    (cancelling, plan): (&mut Vec<ClientOrderId>, &mut Plan),
) {
    for &(at, cid) in overshot {
        let in_flight = reg.get(cid).is_some_and(|rec| {
            matches!(rec.intent(), Intent::PendingCancel { .. }) || rec.cancel_awaits_ack()
        });
        if in_flight || cancelling.contains(&cid) {
            continue;
        }
        if let Some(slot) = slots.get_mut(&at) {
            slot.replacing = true;
            slot.cancels_sent = reg.get(cid).map_or(0, OrderRecord::cancels_sent);
        }
        cancelling.push(cid);
        cancel_now(reg, caps, acct, at, cid, plan);
    }
}

/// Builds into `plan` the cancel of each emulated reduce-only order in `slots` that rests more
/// than is left to reduce, but those in `skip` and those whose cancel is in flight or waits
/// for the acknowledgement; the level waits for the order's terminal state.
fn overshoot_cancels(
    slots: &mut BTreeMap<Level, Slot>,
    reg: &mut Registry,
    caps: &OrderCaps,
    acct: AccountKey,
    market: InstrumentId,
    skip: &[ClientOrderId],
    plan: &mut Plan,
) {
    for (&at, slot) in slots.iter_mut() {
        if !slot.mode.emulated || skip.contains(&slot.cid) {
            continue;
        }
        let Some(rec) = reg.get(slot.cid) else {
            continue;
        };
        let cancelling =
            matches!(rec.intent(), Intent::PendingCancel { .. }) || rec.cancel_awaits_ack();
        if rec.state().is_terminal()
            || cancelling
            || rec.resting() <= reduce_room(reg, market, at.side(), Some(slot.cid))
        {
            continue;
        }
        slot.replacing = true;
        slot.cancels_sent = rec.cancels_sent();
        cancel_now(reg, caps, acct, at, slot.cid, plan);
    }
}

/// Frees each level of `slots` whose order `reg` shows terminal (or no longer holds). A
/// crossing order that ended never sent reached no venue, so its attempt, the last one built
/// for its side, is planned again; older ones still are not (decision 0107).
fn free_terminal(
    slots: &mut BTreeMap<Level, Slot>,
    reg: &Registry,
    fired: &mut HashMap<(AccountKey, InstrumentId, Level), Fired>,
    (acct, market): (AccountKey, InstrumentId),
) {
    slots.retain(|&at, slot| {
        let state = reg.get(slot.cid).map(OrderRecord::state);
        if at.crossing
            && matches!(state, Some(OrdState::Terminal(TerminalKind::NotSent(_))))
            && let Some(fired) = fired.get_mut(&(acct, market, at))
        {
            fired.spent = false;
        }
        state.is_some_and(|state| !state.is_terminal())
    });
}

/// Why a resting order cannot be changed now, if it cannot.
fn unsettled(rec: &OrderRecord) -> Option<HeldReason> {
    match rec.state() {
        state @ (OrdState::PendingNew | OrdState::Unknown) => {
            return Some(HeldReason::Unsettled(state));
        }
        state if rec.unknown_since().is_some() => return Some(HeldReason::Unsettled(state)),
        _ => {}
    }
    // An amend replaced in flight (by a cancel the venue then refused, or never sent) may
    // still reach the venue until an ordered update settles it: no second amend goes over it,
    // and the record's price may not be the venue's (0070).
    let in_flight = rec.intent() != Intent::None || rec.amend_unconfirmed();
    in_flight.then_some(HeldReason::InFlight)
}

/// Whether `placed` carries `q`'s flags, which no amend changes.
fn same_flags(placed: &NewOrder, q: &DesiredQuote) -> bool {
    placed.tif == q.tif
        && placed.channel == q.channel
        && placed.post_only == q.post_only
        && placed.reduce_only == q.reduce_only
}

/// How far `to` is from `from`, in basis points of `from`: a price is its tick index times the
/// instrument's finest step, so the ratio of tick counts is the ratio of prices.
fn moved_bps(from: Ticks, to: Ticks) -> f64 {
    let apart = (i128::from(to.0) - i128::from(from.0)).unsigned_abs() as f64;
    let base = i128::from(from.0).unsigned_abs() as f64;
    if base == 0.0 {
        f64::INFINITY
    } else {
        apart / base * 10_000.0
    }
}

/// How many lots apart `a` and `b` are.
fn lots_apart(a: Lots, b: Lots) -> Lots {
    a.checked_sub(b)
        .or_else(|| b.checked_sub(a))
        .unwrap_or(Lots::ZERO)
}

/// The limit order `q` asks for at `side` of `market` under `cid`.
fn new_order(market: InstrumentId, side: Side, cid: ClientOrderId, q: &DesiredQuote) -> NewOrder {
    NewOrder {
        cid,
        inst: market,
        side,
        qty: q.qty,
        kind: OrderKind::Limit { px: q.px },
        tif: q.tif,
        channel: q.channel,
        post_only: q.post_only,
        reduce_only: q.reduce_only,
        reducing: q.reducing,
    }
}

/// The authorization of a place, an amend or a cancel of one market the registry built, which
/// it always issues: the registry built it, for the account it is bound to or with none bound
/// ([`ExecutionPlanner::bind`]), and nothing changed between its build and now, so judged again
/// it is admitted as it was at its build (decision 0082).
fn authorize(reg: &mut Registry, acct: AccountKey, cmd: PermittedCommand) -> Authorization {
    reg.authorize(acct, cmd)
        .expect("a place, an amend or a cancel of one market is always authorized")
}
