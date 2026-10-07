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
//! - **Exit**: entered only by the owner's Flatten or Wind-down. Exit's admission (orders on the
//!   side that reduces the position only, never crossing zero, every cap still applied) is
//!   FBC-7gl's; until it is built, Exit builds no place or amend ([`StateRefusal::Exit`]).
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
//! has seeded it yet, or a fill since could not be placed against the seed). A refused call
//! changes nothing. The registry holds the leases while the market is armed and drops them when
//! it is disarmed; the account lease while any market is armed.
//!
//! Every change of a market's armed flag or state advances its [`StateGeneration`], which an
//! [`Authorization`](crate::Authorization) carries; a call that changes nothing does not. Lease
//! names given again advance it too for every armed market whose held leases they cover
//! differently, since what the market admits changed (decision 0060). Nothing else in the
//! library, and no resync, fill, outcome or ladder timer, changes either.

use std::collections::{BTreeMap, HashMap};
use std::fmt;

use fbc_core::{AccountLease, InstrumentId, MarketLease, NonceScope, OrderCaps, VenueSymbol};

use crate::grant::{Generations, StateGeneration, Watch};
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

/// Why Start, Flatten or Wind-down was refused: the market's armed flag, state and generation
/// are as they were.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum ArmRefusal {
    /// The market's kill switch is on: it is lifted first ([`Registry::lift_kill`]).
    Killed(InstrumentId),
    /// The market's position is not known: no trustworthy resync seeded it yet, or a fill since
    /// could not be placed against the seed (decision 0055).
    PositionUnknown(InstrumentId),
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
    /// The market is in Exit, whose admission (FBC-7gl) is not built yet: nothing is built.
    Exit(InstrumentId),
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
            StateRefusal::Exit(inst) => {
                write!(f, "{inst:?} is in Exit, which builds no order yet")
            }
            StateRefusal::Unleased(inst) => write!(
                f,
                "{inst:?} is armed under leases its lease names no longer cover: disarm it and arm it again"
            ),
        }
    }
}

impl std::error::Error for StateRefusal {}

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
}

impl Entries {
    /// Gives the registry the names `keys`, advancing the generation of every armed market
    /// whose held leases they cover differently from the names before: what it admits changed
    /// (decision 0060; Reviewer B's RB86-3 on PR #86), so a command built before is refused at
    /// submit.
    pub(crate) fn set_keys(&mut self, keys: LeaseKeys) {
        let mut armed: Vec<InstrumentId> = self.market_leases.keys().copied().collect();
        armed.sort();
        let before: Vec<bool> = armed
            .iter()
            .map(|m| self.held_covered(*m).is_ok())
            .collect();
        self.keys = Some(keys);
        for (market, was) in armed.into_iter().zip(before) {
            if self.held_covered(market).is_ok() != was {
                self.generations.advance(market);
            }
        }
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

    /// Whether `market`'s state admits a place or amend at all, before the caps.
    pub(crate) fn admits(&self, market: InstrumentId) -> Result<(), StateRefusal> {
        match (self.armed(market), self.state(market)) {
            (_, EntryState::Killed) => Err(StateRefusal::Killed(market)),
            (true, _) if self.held_covered(market).is_err() => Err(StateRefusal::Unleased(market)),
            (true, EntryState::Quoting) => Ok(()),
            (true, EntryState::Exit(_)) => Err(StateRefusal::Exit(market)),
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

    /// Arms `market` into `to` (Quoting or Exit), as Start, Flatten and Wind-down do.
    fn arm(
        &mut self,
        market: InstrumentId,
        to: EntryState,
        leases: Leases,
        position_known: bool,
    ) -> Result<MarketEntry, ArmRefusal> {
        if self.state(market) == EntryState::Killed {
            return Err(ArmRefusal::Killed(market));
        }
        if !position_known {
            return Err(ArmRefusal::PositionUnknown(market));
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
        let was_armed = self.market_leases.remove(&market).is_some();
        if self.market_leases.is_empty() {
            self.account_lease = None;
        }
        let state = match self.state(market) {
            EntryState::Killed => EntryState::Killed,
            _ => EntryState::CancelOnly,
        };
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
    /// and, on a disarmed market, without the market's lease, or without the account's lease
    /// where the venue's nonce scope is per account and the registry does not hold it.
    pub fn start(
        &mut self,
        market: InstrumentId,
        leases: Leases,
    ) -> Result<MarketEntry, ArmRefusal> {
        let known = self.position_known(market);
        self.entries.arm(market, EntryState::Quoting, leases, known)
    }

    /// The owner's Flatten (decision 0012): arms a disarmed market straight into Exit, never
    /// through Quoting; moves an armed one to Exit. Refused as [`Registry::start`] is.
    pub fn flatten(
        &mut self,
        market: InstrumentId,
        leases: Leases,
    ) -> Result<MarketEntry, ArmRefusal> {
        let known = self.position_known(market);
        self.entries
            .arm(market, EntryState::Exit(ExitKind::Flatten), leases, known)
    }

    /// The owner's Wind-down (decision 0012): as [`Registry::flatten`], into Exit.
    pub fn wind_down(
        &mut self,
        market: InstrumentId,
        leases: Leases,
    ) -> Result<MarketEntry, ArmRefusal> {
        let known = self.position_known(market);
        self.entries
            .arm(market, EntryState::Exit(ExitKind::WindDown), leases, known)
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
