//! Each market's order-entry state and its armed flag (decision 0012; 0013 rules 1 and 3),
//! held by the [`Registry`] and checked in the one path every place, amend, replace and batch
//! item is built through, before either pre-trade cap.
//!
//! A market is in one of four [`EntryState`]s:
//!
//! - **Killed**: the kill switch is on. No place, amend, replace or batch item is built,
//!   reduce-only, I6-reducing, flatten, force-close and wind-down ones included.
//! - **Cancel-only**: the same for places and amends. Lifting the kill switch leads here and
//!   nowhere else, and a disarm leads here from Exit or Quoting.
//! - **Exit**: entered only by the owner's Flatten or Wind-down. Only exit orders are built: a
//!   place, amend, replace or batch item carrying the venue's reduce-only flag or the OMS's
//!   reducing classification ([`NewOrder::reduces`](fbc_core::NewOrder::reduces)), on the side
//!   that reduces the position, sized so that the position plus every order on that side that
//!   may still move it, the new one included, never crosses zero; each still passes both
//!   pre-trade caps, judged after it. Once the position is flat, or while it is unknown, Exit
//!   builds nothing ([`ExitRefusal`], decision 0063). This is an added restriction, never a bypass: the
//!   classification it asks for exempts an order from no check.
//! - **Quoting**: entered only by the owner's Start.
//!
//! A place or amend is built only on a market that is armed and in Exit or Quoting, under
//! leases the registry's current names still cover ([`StateRefusal::Unleased`]); a disarmed
//! market is always Killed or Cancel-only. A fresh registry has every market disarmed and in
//! Cancel-only. Cancels are built in every state.
//!
//! The consumer's calls, each made on the owner's action: [`Registry::start`],
//! [`Registry::flatten`], [`Registry::wind_down`], [`Registry::disarm`], [`Registry::kill`] and
//! [`Registry::lift_kill`]. Start, Flatten and Wind-down arm a disarmed market: they take its
//! [`MarketLease`], and its [`AccountLease`] where the venue's nonce scope is per account
//! ([`NonceScope::PerAccountMonotonic`]), each checked against the names the consumer gave the
//! registry ([`LeaseKeys`]); they are refused for a Killed market and while the market's
//! position is unknown ([`Registry::position`](crate::Registry::position): no trustworthy resync
//! has seeded it yet, or a fill since could not be placed against the seed), and for a market
//! seeded by hand ([`Registry::seed_position`](crate::Registry::seed_position)) unless the
//! registry was built for a declared owner-assisted testnet run ([`Registry::for_testnet_run`],
//! decision 0067). A refused call changes nothing. The registry holds the leases while the market is armed and drops them when
//! it is disarmed; the account lease while any market is armed.
//!
//! Every change of a market's armed flag or state advances its [`StateGeneration`], which an
//! [`Authorization`](crate::Authorization) carries; a call that changes nothing does not. Lease
//! names given again advance it too for every armed market whose held leases they cover
//! differently, since what the market admits changed (decision 0060). Nothing else in the
//! library, and no resync, fill, outcome or ladder timer, changes either.
//!
//! A second count per market follows only whether the registry holds the market's exclusive
//! lease under the names it has now (0005's I7, [`Registry::cancel_everything`]): it advances
//! each time that changes, on arming a disarmed market, on disarming an armed one, and on
//! lease names given again that cover its held leases differently. An instrument cancel-all is
//! checked against it at submit, not against the state generation, so the kill switch and its
//! lift, which leave the lease as it was, never hold one back (decision 0060; Reviewer B's
//! RB94-1 on PR #94).

use std::collections::{BTreeMap, HashMap};
use std::fmt;

use fbc_core::{
    AccountLease, InstrumentId, Lots, MarketLease, NonceScope, OrderCaps, Side, VenueSymbol,
};

use crate::caps::Exposure;
use crate::grant::{Counters, Generations, StateGeneration, Watch};
use crate::registry::Registry;

/// A market's order-entry state (decision 0012).
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum EntryState {
    /// The kill switch is on: no place or amend is built.
    Killed,
    /// No place or amend is built; the state a fresh registry, a lifted kill switch and a
    /// disarm leave a market in.
    CancelOnly,
    /// The owner's exit: entered only by [`Registry::flatten`] or [`Registry::wind_down`].
    Exit(ExitKind),
    /// Normal order entry: entered only by [`Registry::start`].
    Quoting,
}

/// Which of the owner's calls put a market in Exit.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum ExitKind {
    /// [`Registry::flatten`].
    Flatten,
    /// [`Registry::wind_down`].
    WindDown,
}

/// A market's armed flag, state and state generation ([`Registry::entry`]).
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct MarketEntry {
    armed: bool,
    state: EntryState,
    generation: StateGeneration,
}

impl MarketEntry {
    /// Whether the market is armed: the registry holds its market lease.
    pub fn armed(&self) -> bool {
        self.armed
    }

    /// The market's state.
    pub fn state(&self) -> EntryState {
        self.state
    }

    /// How many times the market's armed flag or state changed.
    pub fn generation(&self) -> StateGeneration {
        self.generation
    }
}

/// The stable names the consumer's leases are taken under (decision 0013 rule 3, FBC-hwe), for
/// the registry to check a lease given to an arming call against: the venue
/// ([`VenueFactory::id`](fbc_core::VenueFactory::id)), the consumer's name for the account,
/// the venue's nonce scope, taken from the venue's own [`OrderCaps`] (`caps.exec.order`, the
/// caps every cancel, amend and resync is judged against) and never given on its own, and
/// each market's [`VenueSymbol`]. A market it does not name is never armed.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct LeaseKeys {
    venue: String,
    account: String,
    nonce_scope: NonceScope,
    symbols: BTreeMap<InstrumentId, VenueSymbol>,
}

impl LeaseKeys {
    /// The names of `account` on `venue`, whose order caps are `order` (the nonce scope is
    /// theirs: DeepSeek's DS-4 on PR #86), for no market yet.
    pub fn new(venue: &str, account: &str, order: &OrderCaps) -> LeaseKeys {
        LeaseKeys {
            venue: venue.to_owned(),
            account: account.to_owned(),
            nonce_scope: order.nonce_scope,
            symbols: BTreeMap::new(),
        }
    }

    /// These names with `market`'s symbol, replacing any it had.
    pub fn with_market(mut self, market: InstrumentId, symbol: VenueSymbol) -> LeaseKeys {
        self.symbols.insert(market, symbol);
        self
    }

    /// The venue.
    pub fn venue(&self) -> &str {
        &self.venue
    }

    /// The account's name.
    pub fn account(&self) -> &str {
        &self.account
    }

    /// The venue's nonce scope, from its order caps.
    pub fn nonce_scope(&self) -> NonceScope {
        self.nonce_scope
    }

    /// The symbol of `market`, if named.
    pub fn symbol(&self, market: InstrumentId) -> Option<&VenueSymbol> {
        self.symbols.get(&market)
    }

    /// Whether arming needs the account lease too: the venue's nonces are one sequence per
    /// account.
    fn needs_account_lease(&self) -> bool {
        self.nonce_scope == NonceScope::PerAccountMonotonic
    }

    fn covers_market(&self, market: InstrumentId, lease: &MarketLease) -> bool {
        lease.venue() == self.venue
            && lease.account() == self.account
            && self.symbols.get(&market) == Some(lease.symbol())
    }

    fn covers_account(&self, lease: &AccountLease) -> bool {
        lease.venue() == self.venue && lease.account() == self.account
    }
}

/// The leases given to an arming call. On a market already armed they are not needed, and
/// are dropped (released); so are those given to a refused call.
#[derive(Debug, Default)]
pub struct Leases {
    market: Option<MarketLease>,
    account: Option<AccountLease>,
}

impl Leases {
    /// No lease: enough only for a market already armed.
    pub fn none() -> Leases {
        Leases::default()
    }

    /// The market's lease.
    pub fn market(lease: MarketLease) -> Leases {
        Leases {
            market: Some(lease),
            account: None,
        }
    }

    /// These leases with the account's lease too.
    pub fn with_account(mut self, lease: AccountLease) -> Leases {
        self.account = Some(lease);
        self
    }
}

/// The consumer's declaration that this process is an owner-assisted testnet run, the only
/// run in which a market seeded by hand ([`Registry::seed_position`]) may be armed (decision
/// 0067; the owner's answer C to RB-olg-3). Given to [`Registry::for_testnet_run`] when the
/// registry is built; nothing in the library makes one, and a consumer trading a live venue
/// never does.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct TestnetRun {
    _declared: (),
}

impl TestnetRun {
    /// Declares an owner-assisted testnet run: the owner is present, the venue is its testnet,
    /// and positions may be seeded by hand because the venue's snapshot source is not yet
    /// trustworthy (decision 0055).
    pub fn owner_assisted() -> TestnetRun {
        TestnetRun { _declared: () }
    }
}

/// How a market's position stands for arming (decision 0067).
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) enum Seeding {
    /// Seeded by a trustworthy resync, or by hand in a declared testnet run.
    Armable,
    /// Not known ([`ArmRefusal::PositionUnknown`]).
    Unknown,
    /// Seeded by hand outside a declared testnet run ([`ArmRefusal::SeededByHand`]).
    ByHand,
}

/// Why Start, Flatten or Wind-down was refused: the market's armed flag, state and generation
/// are as they were.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum ArmRefusal {
    /// The market's kill switch is on: it is lifted first ([`Registry::lift_kill`]).
    Killed(InstrumentId),
    /// The market's position is not known: no trustworthy resync seeded it yet, or a fill since
    /// could not be placed against the seed (decision 0055).
    PositionUnknown(InstrumentId),
    /// The market's position was seeded by hand ([`Registry::seed_position`]), not by a
    /// trustworthy resync, and the registry was not built for a declared owner-assisted testnet
    /// run ([`Registry::for_testnet_run`]): a hand seed never arms a live market (decision
    /// 0067). A later resync only compares the seed, so this holds for the process's life.
    SeededByHand(InstrumentId),
    /// The registry has no [`LeaseKeys`] naming the market, so no lease can be checked.
    NotNamed(InstrumentId),
    /// The market is disarmed and the call carried no market lease.
    NoMarketLease(InstrumentId),
    /// The market lease given, or the one an armed market holds, is for another venue, account
    /// or symbol than the names the registry has now.
    WrongMarketLease(InstrumentId),
    /// The venue's nonce scope is per account and neither the call nor the registry has the
    /// account lease.
    NoAccountLease(InstrumentId),
    /// The account lease given, or the one the registry holds for a market armed earlier, is
    /// for another venue or account than the names the registry has now.
    WrongAccountLease(InstrumentId),
}

impl fmt::Display for ArmRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ArmRefusal::Killed(inst) => {
                write!(f, "the kill switch is on for {inst:?}: lift it first")
            }
            ArmRefusal::PositionUnknown(inst) => write!(
                f,
                "the position on {inst:?} is not known: no trustworthy resync has seeded it"
            ),
            ArmRefusal::SeededByHand(inst) => write!(
                f,
                "the position on {inst:?} was seeded by hand, not by a trustworthy resync: \
                 only a declared owner-assisted testnet run arms such a market"
            ),
            ArmRefusal::NotNamed(inst) => {
                write!(f, "no lease names are configured for {inst:?}")
            }
            ArmRefusal::NoMarketLease(inst) => {
                write!(f, "arming {inst:?} needs its market lease")
            }
            ArmRefusal::WrongMarketLease(inst) => {
                write!(f, "the market lease given is not {inst:?}'s")
            }
            ArmRefusal::NoAccountLease(inst) => write!(
                f,
                "arming {inst:?} needs the account lease: the venue's nonces are per account"
            ),
            ArmRefusal::WrongAccountLease(inst) => {
                write!(
                    f,
                    "the account lease given to arm {inst:?} is not this account's"
                )
            }
        }
    }
}

impl std::error::Error for ArmRefusal {}

/// Why a market's state built no place, amend, replace or batch item; checked before either
/// pre-trade cap.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum StateRefusal {
    /// The market's kill switch is on.
    Killed(InstrumentId),
    /// The market is in Cancel-only.
    CancelOnly(InstrumentId),
    /// The market is in Exit, which builds only exit orders, and this is not one, or would
    /// take its side past zero (decision 0012).
    Exit(ExitRefusal),
    /// The market is armed under leases the registry's lease names no longer cover: names
    /// given since ([`Registry::with_lease_keys`]) are for another venue or account, leave the
    /// market out, or make the nonces per account while the registry holds no account lease.
    /// It is disarmed and armed again under the names it has now (Reviewer B's RB86-1 on
    /// PR #86).
    Unleased(InstrumentId),
}

impl fmt::Display for StateRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StateRefusal::Killed(inst) => write!(f, "the kill switch is on for {inst:?}"),
            StateRefusal::CancelOnly(inst) => write!(f, "{inst:?} is cancel-only"),
            StateRefusal::Exit(why) => write!(f, "{why}"),
            StateRefusal::Unleased(inst) => write!(
                f,
                "{inst:?} is armed under leases its lease names no longer cover: disarm it and arm it again"
            ),
        }
    }
}

impl std::error::Error for StateRefusal {}

/// Why a market in Exit built no place, amend, replace or batch item (decisions 0012, 0063): Exit
/// builds only exit orders, on the side that reduces the position, never past zero. Judged
/// after the market's state admitted Exit and before either pre-trade cap.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum ExitRefusal {
    /// The market's position is not known (a fill since its seed could not be placed against
    /// it; decision 0055): which side reduces it, and by how much, is not known.
    PositionUnknown(InstrumentId),
    /// The position is flat: Exit builds nothing more.
    Flat(InstrumentId),
    /// The order is on `side`, which adds to the position.
    Increasing { inst: InstrumentId, side: Side },
    /// The order carries neither the venue's reduce-only flag nor the OMS's reducing
    /// classification: an ordinary order, which Exit never builds, even on the side that
    /// reduces the position.
    Ordinary { inst: InstrumentId, side: Side },
    /// Our orders on `side` that may still move the position would total `total` lots with
    /// the order (`None` when that does not fit a lot count), more than the position's
    /// `position`: the position would cross zero if they all filled.
    CrossesZero {
        inst: InstrumentId,
        side: Side,
        total: Option<Lots>,
        position: Lots,
    },
}

impl fmt::Display for ExitRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ExitRefusal::PositionUnknown(inst) => write!(
                f,
                "{inst:?} is in Exit and its position is not known, so no order reduces it"
            ),
            ExitRefusal::Flat(inst) => {
                write!(f, "{inst:?} is in Exit and flat: nothing more is built")
            }
            ExitRefusal::Increasing { inst, side } => write!(
                f,
                "{inst:?} is in Exit and a {side:?} order adds to its position"
            ),
            ExitRefusal::Ordinary { inst, side } => write!(
                f,
                "{inst:?} is in Exit and the {side:?} order is neither reduce-only nor \
                 classified reducing: Exit builds no ordinary order"
            ),
            ExitRefusal::CrossesZero {
                inst,
                side,
                total: Some(total),
                position,
            } => write!(
                f,
                "{inst:?} is in Exit and our {side:?} orders would total {} lots with the \
                 order, past the position's {} lots: it would cross zero",
                total.get(),
                position.get()
            ),
            ExitRefusal::CrossesZero {
                inst,
                side,
                total: None,
                position,
            } => write!(
                f,
                "{inst:?} is in Exit and what our {side:?} orders would total with the order \
                 does not fit a lot count, so it cannot be held within the position's {} lots",
                position.get()
            ),
        }
    }
}

impl std::error::Error for ExitRefusal {}

/// What a market's state admits once it admits a place or amend at all.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) enum Admits {
    /// Quoting: any order the caps admit.
    Any,
    /// Exit: only exit orders ([`ExitRefusal`]).
    ExitOnly,
}

impl Admits {
    /// Whether an order on `exposure`'s side, which `marked` says is reduce-only or classified
    /// reducing, adding `adds` to what the side may still move the position by, is admitted by
    /// the market's state: anything in Quoting; in Exit, only an exit order on the side that
    /// reduces the position, the side's total within the position's size (decision 0012).
    pub(crate) fn judge(
        self,
        exposure: &Exposure,
        marked: bool,
        adds: Lots,
    ) -> Result<(), StateRefusal> {
        match self {
            Admits::Any => Ok(()),
            Admits::ExitOnly => exposure
                .admit_exit(marked, adds)
                .map_err(StateRefusal::Exit),
        }
    }
}

/// Every market's state and the leases its arming took.
#[derive(Debug, Default)]
pub(crate) struct Entries {
    keys: Option<LeaseKeys>,
    /// A market not listed is in Cancel-only.
    states: HashMap<InstrumentId, EntryState>,
    /// The armed markets' leases: a market is armed exactly when it holds one.
    market_leases: HashMap<InstrumentId, MarketLease>,
    /// The account lease, while any market is armed.
    account_lease: Option<AccountLease>,
    generations: Generations,
    /// Per market, how many times whether the registry holds its exclusive lease changed.
    exclusive: Counters,
}

impl Entries {
    /// Gives the registry the names `keys`, advancing the generation of every armed market
    /// whose held leases they cover differently from the names before: what it admits changed
    /// (decision 0060; Reviewer B's RB86-3 on PR #86), so a command built before is refused at
    /// submit.
    pub(crate) fn set_keys(&mut self, keys: LeaseKeys) {
        let before = self.exclusive_held();
        self.keys = Some(keys);
        // The armed markets are the same, so each whose exclusive hold changed is one whose
        // held leases the new names cover differently.
        for market in self.exclusive_changed(&before) {
            self.generations.advance(market);
        }
    }

    /// The armed markets, each with whether the registry holds its exclusive lease now.
    fn exclusive_held(&self) -> BTreeMap<InstrumentId, bool> {
        self.market_leases
            .keys()
            .map(|m| (*m, self.holds_exclusive(*m)))
            .collect()
    }

    /// Advances the exclusive-lease count of every market whose exclusive hold changed since
    /// `before` ([`Entries::exclusive_held`]), and returns them in id order.
    fn exclusive_changed(&mut self, before: &BTreeMap<InstrumentId, bool>) -> Vec<InstrumentId> {
        let after = self.exclusive_held();
        let held = |map: &BTreeMap<InstrumentId, bool>, m| map.get(m).copied().unwrap_or(false);
        let mut changed: Vec<InstrumentId> = before
            .keys()
            .chain(after.keys())
            .filter(|m| held(before, m) != held(&after, m))
            .copied()
            .collect();
        changed.sort();
        changed.dedup();
        for market in &changed {
            self.exclusive.advance(*market);
        }
        changed
    }

    /// How many times whether the registry holds `market`'s exclusive lease changed, watched
    /// for an instrument cancel-all about to be built under it (decision 0060).
    pub(crate) fn watch_exclusive(&mut self, market: InstrumentId) -> Watch {
        self.exclusive.watch(market)
    }

    pub(crate) fn keys(&self) -> Option<&LeaseKeys> {
        self.keys.as_ref()
    }

    fn state(&self, market: InstrumentId) -> EntryState {
        self.states
            .get(&market)
            .copied()
            .unwrap_or(EntryState::CancelOnly)
    }

    fn armed(&self, market: InstrumentId) -> bool {
        self.market_leases.contains_key(&market)
    }

    pub(crate) fn entry(&self, market: InstrumentId) -> MarketEntry {
        MarketEntry {
            armed: self.armed(market),
            state: self.state(market),
            generation: self.generations.of(market),
        }
    }

    /// `market`'s state generation now.
    pub(crate) fn generation(&self, market: InstrumentId) -> StateGeneration {
        self.generations.of(market)
    }

    /// `market`'s state generation now, watched for a command about to be built under it,
    /// which its authorization is checked against at submit (decision 0060).
    pub(crate) fn watch(&mut self, market: InstrumentId) -> Watch {
        self.generations.watch(market)
    }

    /// Whether `market`'s state admits a place or amend at all, before the caps, and which:
    /// any in Quoting, exit orders alone in Exit ([`Admits::judge`]).
    pub(crate) fn admits(&self, market: InstrumentId) -> Result<Admits, StateRefusal> {
        match (self.armed(market), self.state(market)) {
            (_, EntryState::Killed) => Err(StateRefusal::Killed(market)),
            (true, _) if self.held_covered(market).is_err() => Err(StateRefusal::Unleased(market)),
            (true, EntryState::Quoting) => Ok(Admits::Any),
            (true, EntryState::Exit(_)) => Ok(Admits::ExitOnly),
            _ => Err(StateRefusal::CancelOnly(market)),
        }
    }

    /// The names the registry has for `market`, if it has any.
    fn named(&self, market: InstrumentId) -> Result<&LeaseKeys, ArmRefusal> {
        self.keys
            .as_ref()
            .filter(|k| k.symbols.contains_key(&market))
            .ok_or(ArmRefusal::NotNamed(market))
    }

    /// Whether the registry holds `market`'s exclusive lease under the names it has now: the
    /// market is armed and its held leases are still covered (0005's I7).
    pub(crate) fn holds_exclusive(&self, market: InstrumentId) -> bool {
        self.armed(market) && self.held_covered(market).is_ok()
    }

    /// The markets in Killed, in id order.
    pub(crate) fn killed(&self) -> Vec<InstrumentId> {
        let mut killed: Vec<InstrumentId> = self
            .states
            .iter()
            .filter(|(_, s)| **s == EntryState::Killed)
            .map(|(m, _)| *m)
            .collect();
        killed.sort();
        killed
    }

    /// Whether the leases armed `market` holds are still covered by the names the registry has
    /// now: its market lease, and the account lease where the nonce scope is per account
    /// (Reviewer B's RB86-1 on PR #86). The account lease must be the current names' account,
    /// not merely some account's: a market armed while no account lease was needed never
    /// checked the one the registry holds now (Reviewer B's RB86-5 on PR #86).
    fn held_covered(&self, market: InstrumentId) -> Result<(), ArmRefusal> {
        let keys = self.named(market)?;
        if !self
            .market_leases
            .get(&market)
            .is_some_and(|held| keys.covers_market(market, held))
        {
            return Err(ArmRefusal::WrongMarketLease(market));
        }
        if keys.needs_account_lease() {
            match &self.account_lease {
                None => return Err(ArmRefusal::NoAccountLease(market)),
                Some(held) if !keys.covers_account(held) => {
                    return Err(ArmRefusal::WrongAccountLease(market));
                }
                Some(_) => {}
            }
        }
        Ok(())
    }

    /// Moves `market` to `state`, armed or not, advancing its generation when either changed.
    fn set(&mut self, market: InstrumentId, state: EntryState, was_armed: bool) -> MarketEntry {
        let changed = was_armed != self.armed(market) || state != self.state(market);
        self.states.insert(market, state);
        if changed {
            self.generations.advance(market);
        }
        self.entry(market)
    }

    /// Arms `market` into `to` (Quoting or Exit), as Start, Flatten and Wind-down do, advancing
    /// the exclusive-lease count of each market whose exclusive hold that changed.
    fn arm(
        &mut self,
        market: InstrumentId,
        to: EntryState,
        leases: Leases,
        seeding: Seeding,
    ) -> Result<MarketEntry, ArmRefusal> {
        let before = self.exclusive_held();
        let armed = self.arm_leased(market, to, leases, seeding);
        self.exclusive_changed(&before);
        armed
    }

    fn arm_leased(
        &mut self,
        market: InstrumentId,
        to: EntryState,
        leases: Leases,
        seeding: Seeding,
    ) -> Result<MarketEntry, ArmRefusal> {
        if self.state(market) == EntryState::Killed {
            return Err(ArmRefusal::Killed(market));
        }
        match seeding {
            Seeding::Armable => {}
            Seeding::Unknown => return Err(ArmRefusal::PositionUnknown(market)),
            Seeding::ByHand => return Err(ArmRefusal::SeededByHand(market)),
        }
        let was_armed = self.armed(market);
        if was_armed {
            // Armed under names given again since, it is disarmed and armed again instead.
            self.held_covered(market)?;
        } else {
            let keys = self.named(market)?;
            let lease = leases.market.ok_or(ArmRefusal::NoMarketLease(market))?;
            if !keys.covers_market(market, &lease) {
                return Err(ArmRefusal::WrongMarketLease(market));
            }
            // The account lease held for a market armed earlier counts only while it is this
            // account's: names given again since cannot let it stand in for another
            // (DeepSeek's DS-1 on PR #86).
            if self
                .account_lease
                .as_ref()
                .is_some_and(|held| !keys.covers_account(held))
            {
                return Err(ArmRefusal::WrongAccountLease(market));
            }
            let account = match leases.account {
                Some(given) if !keys.covers_account(&given) => {
                    return Err(ArmRefusal::WrongAccountLease(market));
                }
                given => self.account_lease.is_none().then_some(given).flatten(),
            };
            if keys.needs_account_lease() && self.account_lease.is_none() && account.is_none() {
                return Err(ArmRefusal::NoAccountLease(market));
            }
            if let Some(account) = account {
                self.account_lease = Some(account);
            }
            self.market_leases.insert(market, lease);
        }
        Ok(self.set(market, to, was_armed))
    }

    /// Disarms `market`: Exit and Quoting become Cancel-only, Killed stays; its market lease is
    /// dropped, and the account lease with the last armed market's.
    fn disarm(&mut self, market: InstrumentId) -> MarketEntry {
        let before = self.exclusive_held();
        let was_armed = self.market_leases.remove(&market).is_some();
        if self.market_leases.is_empty() {
            self.account_lease = None;
        }
        let state = match self.state(market) {
            EntryState::Killed => EntryState::Killed,
            _ => EntryState::CancelOnly,
        };
        self.exclusive_changed(&before);
        self.set(market, state, was_armed)
    }
}

impl Registry {
    /// This registry with the names its arming calls check leases against: without them no
    /// market is armed ([`ArmRefusal::NotNamed`]). Given once, before any market is armed.
    /// Names given later leave the leases already held held, but an armed market builds no
    /// place or amend while its held leases are not covered by them
    /// ([`StateRefusal::Unleased`]), Start, Flatten and Wind-down on it are refused, and while
    /// an account lease held under other names is held no market is armed
    /// ([`ArmRefusal::WrongAccountLease`]): such a market is disarmed and armed again. Names
    /// that cover an armed market's held leases differently from the names before advance its
    /// [`StateGeneration`], so a command built before is refused at submit (decision 0060).
    pub fn with_lease_keys(mut self, keys: LeaseKeys) -> Registry {
        self.entries.set_keys(keys);
        self
    }

    /// This registry for a declared owner-assisted testnet run (decision 0067): a market seeded
    /// by hand ([`Registry::seed_position`]) may then be armed by Start, Flatten and Wind-down,
    /// which otherwise refuse it ([`ArmRefusal::SeededByHand`]). Given when the registry is
    /// built, for the owner's testnet runs only (the `testnet_trade` sample, FBC-x69b); it
    /// changes nothing else, and never arms a market whose position is unknown.
    pub fn for_testnet_run(mut self, run: TestnetRun) -> Registry {
        self.testnet_run = Some(run);
        self
    }

    /// Whether the registry was built for a declared testnet run ([`Registry::for_testnet_run`]).
    pub fn testnet_run(&self) -> bool {
        self.testnet_run.is_some()
    }

    /// How `market`'s position stands for arming: known and seeded by a trustworthy resync, or
    /// by hand in a declared testnet run (decision 0067).
    fn seeding(&self, market: InstrumentId) -> Seeding {
        if !self.position_known(market) {
            Seeding::Unknown
        } else if self.seeded_by_hand(market) && !self.testnet_run() {
            Seeding::ByHand
        } else {
            Seeding::Armable
        }
    }

    /// The names arming checks leases against, if given.
    pub fn lease_keys(&self) -> Option<&LeaseKeys> {
        self.entries.keys()
    }

    /// `market`'s armed flag, state and state generation: disarmed, Cancel-only and generation
    /// 0 until a call changes it.
    pub fn entry(&self, market: InstrumentId) -> MarketEntry {
        self.entries.entry(market)
    }

    /// The owner's Start (decision 0012): arms a disarmed market with `leases` and moves it to
    /// Quoting; on an armed market, moves it to Quoting. The only call that reaches Quoting.
    /// Refused, changing nothing, for a Killed market, while the market's position is unknown,
    /// for a market seeded by hand outside a declared testnet run (decision 0067), and, on a
    /// disarmed market, without the market's lease, or without the account's lease
    /// where the venue's nonce scope is per account and the registry does not hold it.
    pub fn start(
        &mut self,
        market: InstrumentId,
        leases: Leases,
    ) -> Result<MarketEntry, ArmRefusal> {
        let seeding = self.seeding(market);
        self.entries
            .arm(market, EntryState::Quoting, leases, seeding)
    }

    /// The owner's Flatten (decision 0012): arms a disarmed market straight into Exit, never
    /// through Quoting; moves an armed one to Exit. Refused as [`Registry::start`] is.
    pub fn flatten(
        &mut self,
        market: InstrumentId,
        leases: Leases,
    ) -> Result<MarketEntry, ArmRefusal> {
        let seeding = self.seeding(market);
        self.entries
            .arm(market, EntryState::Exit(ExitKind::Flatten), leases, seeding)
    }

    /// The owner's Wind-down (decision 0012): as [`Registry::flatten`], into Exit.
    pub fn wind_down(
        &mut self,
        market: InstrumentId,
        leases: Leases,
    ) -> Result<MarketEntry, ArmRefusal> {
        let seeding = self.seeding(market);
        self.entries.arm(
            market,
            EntryState::Exit(ExitKind::WindDown),
            leases,
            seeding,
        )
    }

    /// The consumer's disarm: the market is disarmed and its leases dropped; Exit and Quoting
    /// become Cancel-only, so arming again takes Start, Flatten or Wind-down. A Killed market
    /// stays Killed. On a disarmed market it changes nothing.
    pub fn disarm(&mut self, market: InstrumentId) -> MarketEntry {
        self.entries.disarm(market)
    }

    /// Turns `market`'s kill switch on: Killed from any state, armed or not, the armed flag
    /// unchanged.
    pub fn kill(&mut self, market: InstrumentId) -> MarketEntry {
        let armed = self.entries.armed(market);
        self.entries.set(market, EntryState::Killed, armed)
    }

    /// Lifts `market`'s kill switch: Killed becomes Cancel-only and nothing else, so lifting it
    /// never resumes order entry; the armed flag is unchanged. On a market not Killed it
    /// changes nothing.
    pub fn lift_kill(&mut self, market: InstrumentId) -> MarketEntry {
        let armed = self.entries.armed(market);
        match self.entries.state(market) {
            EntryState::Killed => self.entries.set(market, EntryState::CancelOnly, armed),
            _ => self.entries.entry(market),
        }
    }
}
