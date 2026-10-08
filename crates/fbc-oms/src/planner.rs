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
//! The commands come out in 0005's order: cancels, then reducing orders (places and amends
//! that reduce), then amends, then adds, each group in side (bids first) and level order, and
//! are built in that order, so each is judged with the earlier ones counted. A place or amend
//! the market's state or a cap refuses is never built and is reported ([`Refused`]); the
//! planner never falls back from a refused amend to a cancel, and nothing it emits bypasses a
//! check. Batching, venue modes, the rate-scope safety floor and flag-conflict fallbacks are
//! not here (FBC-hht, FBC-01w).
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
    AccountKey, Bps, Channel, CidMint, ClientOrderId, IdError, InstrumentId, Lots, MonoNs,
    NewOrder, OrderCaps, OrderKind, Side, Ticks, Tif,
};

use crate::entry::EntryState;
use crate::grant::Authorization;
use crate::permit::{self, AmendRefusal, CancelChoice, PermitRefusal, PermittedCommand};
use crate::record::{Intent, OrdState, OrderRecord};
use crate::registry::{Instance, OmsError, Registry};

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
}

impl DesiredBook {
    /// An empty book on `market`: planned, it cancels every order the planner has there.
    pub fn new(market: InstrumentId) -> DesiredBook {
        DesiredBook {
            market,
            levels: BTreeMap::new(),
        }
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

    /// The market.
    pub fn market(&self) -> InstrumentId {
        self.market
    }

    /// The quote at `level` of `side`.
    pub fn quote(&self, side: Side, level: u16) -> Option<&DesiredQuote> {
        self.levels.get(&Level::new(side, level))
    }
}

/// One level of one side: bids before asks, then by level.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
struct Level {
    /// 0 for bids, 1 for asks.
    side: u8,
    level: u16,
}

impl Level {
    fn new(side: Side, level: u16) -> Level {
        let side = match side {
            Side::Buy => 0,
            Side::Sell => 1,
        };
        Level { side, level }
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
    pub level: u16,
    /// The order the command is for.
    pub cid: ClientOrderId,
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
}

/// A level whose order the planner left as it is this pass.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct Held {
    pub side: Side,
    pub level: u16,
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
    pub level: u16,
    pub why: PlanRefusal,
}

/// What one [`ExecutionPlanner::plan`] pass built.
#[derive(Debug, Default)]
pub struct Plan {
    /// The commands, in 0005's order: cancels, reducing orders, amends, adds.
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
}

/// What a pass decided for a level, before anything is built.
enum Action {
    Cancel(ClientOrderId),
    Amend {
        cid: ClientOrderId,
        px: Ticks,
        total: Lots,
        reducing: bool,
    },
    Place(DesiredQuote),
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
}

impl ExecutionPlanner {
    /// A planner holding no order, under the consumer's thresholds.
    pub fn new(config: PlannerConfig) -> ExecutionPlanner {
        ExecutionPlanner {
            config,
            slots: HashMap::new(),
            bound: HashMap::new(),
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

    /// One pass over `desired`'s market at `now`: decides each level, then builds, in 0005's
    /// order, each command through `reg` (places with client ids from `mint`, amends from
    /// [`Live`](crate::Live) permits, cancels from [`Cancellable`](crate::Cancellable) ones,
    /// all under the venue's `caps`) and authorizes it for `acct`.
    ///
    /// `reg` is `acct`'s registry: the planner holds each account's orders apart, so a pass for
    /// one account never frees another's levels, and binds `acct` to `reg` at its first pass.
    /// A pass for `acct` through another registry, or through `reg` for another account, is
    /// refused with nothing built or freed ([`PlanError`]; 0068).
    pub fn plan(
        &mut self,
        desired: &DesiredBook,
        reg: &mut Registry,
        caps: &OrderCaps,
        acct: AccountKey,
        mint: &mut CidMint,
        now: MonoNs,
    ) -> Result<Plan, PlanError> {
        self.bind(acct, reg.instance())?;
        let market = desired.market;
        let config = self.config;
        let slots = self.slots.entry((acct, market)).or_default();
        let mut plan = Plan::default();
        // An order seen terminal frees its level.
        slots.retain(|_, slot| reg.get(slot.cid).is_some_and(|r| !r.state().is_terminal()));

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
        let levels: BTreeSet<Level> = slots.keys().chain(desired.levels.keys()).copied().collect();
        for at in levels {
            let want = desired.levels.get(&at).filter(|q| q.qty > Lots::ZERO);
            let Some(slot) = slots.get_mut(&at) else {
                if let Some(q) = want {
                    let stage = if q.reduces() {
                        Stage::Reducing
                    } else {
                        Stage::Add
                    };
                    actions.push((stage, at, Action::Place(q.clone())));
                }
                continue;
            };
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
                cid: slot.cid,
                why,
            };
            // In Exit, an order on a side that does not reduce the position is cancelled
            // whatever the book wants there: Exit builds nothing on that side (0063).
            let adds_in_exit = exit_reduces.is_some_and(|reduces| reduces != Some(at.side()));
            let Some(q) = want.filter(|_| !slot.replacing && !adds_in_exit) else {
                if matches!(rec.intent(), Intent::PendingCancel { .. }) {
                    if slot.replacing {
                        plan.held.push(hold(HeldReason::Replacing));
                    }
                } else {
                    slot.cancels_sent = rec.cancels_sent();
                    actions.push((Stage::Cancel, at, Action::Cancel(slot.cid)));
                }
                continue;
            };
            // A cancel decided for the order and waiting for its acknowledgement is carried
            // through: the level is replaced once the order is terminal, never left resting at
            // the price the cancel was decided against.
            if rec.cancel_awaits_ack() {
                slot.replacing = true;
                slot.cancels_sent = rec.cancels_sent();
                actions.push((Stage::Cancel, at, Action::Cancel(slot.cid)));
                continue;
            }
            if let Some(why) = unsettled(rec) {
                plan.held.push(hold(why));
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
                    let reducing = q.reducing || rec.placed().reduce_only;
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
                    };
                    actions.push((stage, at, amend));
                }
                None => {
                    slot.replacing = true;
                    slot.cancels_sent = rec.cancels_sent();
                    actions.push((Stage::Cancel, at, Action::Cancel(slot.cid)));
                }
            }
        }

        // Built in 0005's order, so each is judged with the earlier ones counted.
        actions.sort_by_key(|(stage, at, _)| (*stage, *at));
        for (stage, at, action) in actions {
            let side = at.side();
            let refuse = |why| Refused {
                side,
                level: at.level,
                why,
            };
            let built = match action {
                Action::Cancel(cid) => match reg.cancellable(cid) {
                    Ok(permit) => match permit.cancel(caps) {
                        CancelChoice::Send(cmd) => Ok((cid, cmd)),
                        CancelChoice::AwaitAck => {
                            plan.awaiting_ack.push(cid);
                            continue;
                        }
                    },
                    Err(why) => Err(PlanRefusal::Permit(why)),
                },
                Action::Amend {
                    cid,
                    px,
                    total,
                    reducing,
                } => reg
                    .live(cid)
                    .map_err(PlanRefusal::Permit)
                    .and_then(|live| {
                        live.amend(caps, px, total, reducing)
                            .map_err(PlanRefusal::Amend)
                    })
                    .map(|cmd| {
                        if let Some(slot) = slots.get_mut(&at) {
                            slot.changed_at = now;
                        }
                        (cid, cmd)
                    }),
                Action::Place(q) => mint
                    .mint()
                    .map_err(PlanRefusal::Mint)
                    .and_then(|cid| {
                        reg.place(new_order(market, side, cid, &q))
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
                            },
                        );
                        (cid, cmd)
                    }),
            };
            match built {
                Ok((cid, cmd)) => plan.commands.push(Planned {
                    stage,
                    side,
                    level: at.level,
                    cid,
                    auth: authorize(reg, acct, cmd),
                }),
                Err(why) => plan.refused.push(refuse(why)),
            }
        }
        Ok(plan)
    }

    /// Binds `acct` to the registry `instance` at its first pass; refuses another registry for
    /// `acct`, or `instance` for another account. Client ids alone cannot tell two accounts'
    /// registries apart: accounts may lease the same namespace (0068).
    fn bind(&mut self, acct: AccountKey, instance: Instance) -> Result<(), PlanError> {
        if let Some(bound) = self.bound.get(&acct) {
            return if *bound == instance {
                Ok(())
            } else {
                Err(PlanError::OtherRegistry { acct })
            };
        }
        if let Some((&bound, _)) = self.bound.iter().find(|(_, i)| **i == instance) {
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
/// it always issues.
fn authorize(reg: &Registry, acct: AccountKey, cmd: PermittedCommand) -> Authorization {
    reg.authorize(acct, cmd)
        .expect("a place, an amend or a cancel of one market is always authorized")
}
