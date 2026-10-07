//! The authorization every order-affecting command needs to reach a gateway (decision 0045),
//! and its check at submit (decision 0060).
//!
//! 0013 rule 2 puts the caps and the kill switch in the one path every order command takes,
//! and that path is `fbc-oms`. [`VenueCommand`] is a plain value anyone can build, so the
//! gateway does not take one for an order: [`OrderGateway::submit`](crate::OrderGateway::submit)
//! takes an [`Authorization`], which only this crate issues ([`Registry::authorize`]). It is
//! issued for one account and one market, for a place, an amend, a batch of places, a cancel, a
//! cancel-many or an instrument cancel-all, each only as the [`PermittedCommand`] this crate
//! built (the instrument cancel-all under 0005's I7 guard); never for an account cancel-all,
//! which no record admits (0005's I7 admits only the instrument one). A command a cap or the
//! market's state refused was never built, so it never receives one. It has no `Clone`, no
//! public constructor and no way to edit its command, and submitting consumes it, so each one
//! is spent once (the compile-fail cases in `tests/ui_authorization/`).
//!
//! **At submit** (decision 0060). The shard host interleaves handler work between the commands
//! of one decide pass, so the kill switch can go on between an authorization's issue and its
//! write. The gateway calls [`Authorization::check_at_submit`] immediately before encoding and
//! writes nothing when it refuses:
//!
//! - a place, a batch of places or an amend goes through only while its market's
//!   [`StateGeneration`] is still the one it was built under: the kill switch, a disarm, an
//!   arming call, lease names given again that change what the market admits, or any other
//!   change of the market's state since its build refuses it;
//! - an instrument cancel-all, as a place, and also only while no event has shown an order not
//!   ours on the market since it was built (0005's I7, [`Registry::cancel_everything`]);
//! - a cancel or a cancel-many always goes through: cancels are built in every state, within
//!   0005's I4 (they name our orders only) and I7 (no account cancel-all is ever built), and
//!   the kill switch must never hold one back (0012).
//!
//! The check reads the market's live counters through a shared handle the authorization
//! carries, so a gateway on another task or thread than the registry's runs it without the
//! registry.
//!
//! The module is `grant`, not `auth`: `src/auth*` is 0009's review path for credential code,
//! and this holds none.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use fbc_core::{AccountKey, CancelScope, InstrumentId, VenueCommand};

use crate::{PermittedCommand, Registry};

/// How many times a market's state has changed: the kill switch, an arming call, a disarm or
/// any other move of its order-entry state (0012). An [`Authorization`] carries the value its
/// market had when its command was built.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct StateGeneration(u64);

impl StateGeneration {
    /// The generation as a number, starting at 0 for a market whose state never changed.
    pub fn get(self) -> u64 {
        self.0
    }
}

/// A count per market that only grows, each shared with the commands built under it so that
/// a check at submit reads its current value without the registry.
#[derive(Default, Debug)]
pub(crate) struct Counters {
    by_market: BTreeMap<InstrumentId, Arc<AtomicU64>>,
}

impl Counters {
    /// The market's count: 0 until it first advances.
    pub(crate) fn of(&self, market: InstrumentId) -> u64 {
        self.by_market
            .get(&market)
            .map_or(0, |cell| cell.load(Ordering::SeqCst))
    }

    /// Advances the market's count and returns the new value. A value is never reused: at a
    /// billion changes a second, `u64` lasts five centuries.
    pub(crate) fn advance(&mut self, market: InstrumentId) -> u64 {
        let cell = self.by_market.entry(market).or_default();
        let next = cell
            .load(Ordering::SeqCst)
            .checked_add(1)
            .expect("a market's count never reaches u64::MAX");
        cell.store(next, Ordering::SeqCst);
        next
    }

    /// The market's count now, with the shared handle that tells later whether it moved.
    pub(crate) fn watch(&mut self, market: InstrumentId) -> Watch {
        let cell = Arc::clone(self.by_market.entry(market).or_default());
        let at = cell.load(Ordering::SeqCst);
        Watch { cell, at }
    }
}

/// A market's count as a command was built under it, and the handle to its current value.
#[derive(Debug)]
pub(crate) struct Watch {
    cell: Arc<AtomicU64>,
    at: u64,
}

impl Watch {
    /// The count when the command was built.
    pub(crate) fn at(&self) -> u64 {
        self.at
    }

    /// The count now, when it moved since the command was built.
    fn moved(&self) -> Option<u64> {
        let now = self.cell.load(Ordering::SeqCst);
        (now != self.at).then_some(now)
    }
}

/// Two commands built under the same count compare equal, whichever handle they hold.
impl PartialEq for Watch {
    fn eq(&self, other: &Watch) -> bool {
        self.at == other.at
    }
}

impl Eq for Watch {}

/// Each market's current [`StateGeneration`]. The market states advance a market's generation
/// on every change of its armed flag or state (`src/entry.rs`); nothing outside this crate can.
#[derive(Default, Debug)]
pub(crate) struct Generations {
    counts: Counters,
}

impl Generations {
    /// The market's current generation: 0 until its state first changes.
    pub(crate) fn of(&self, market: InstrumentId) -> StateGeneration {
        StateGeneration(self.counts.of(market))
    }

    /// Records a change of the market's state and returns its new generation.
    pub(crate) fn advance(&mut self, market: InstrumentId) -> StateGeneration {
        StateGeneration(self.counts.advance(market))
    }

    /// The market's generation now, watched for the command about to be built.
    pub(crate) fn watch(&mut self, market: InstrumentId) -> Watch {
        self.counts.watch(market)
    }
}

/// What a built command is re-checked against at submit ([`Authorization::check_at_submit`]):
/// nothing for a cancel or a cancel-many, the market's state generation for a place, a batch or
/// an amend, and that and the foreign orders seen for an instrument cancel-all.
#[derive(Default, Eq, PartialEq, Debug)]
pub(crate) struct Guard {
    /// The market's state generation at build.
    pub(crate) state: Option<Watch>,
    /// The count of events that showed an order not ours on the market, at build.
    pub(crate) foreign: Option<Watch>,
}

/// Why `fbc-oms` issues no authorization for a command.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum IssueRefusal {
    /// The command affects no order (a query, a fee query, a dead-man refresh, cancel-on-
    /// disconnect): it goes through the gateway's control path, unauthorized.
    NotOrderAffecting,
    /// An account cancel-all: no record admits one (0005's I7 admits only the instrument one).
    AccountCancelAll,
    /// A batch with no item names no market.
    EmptyBatch,
    /// A batch whose items name more than one market: an authorization carries one market's
    /// generation.
    MixedMarkets,
}

impl fmt::Display for IssueRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            IssueRefusal::NotOrderAffecting => "the command affects no order",
            IssueRefusal::AccountCancelAll => "no record admits an account cancel-all",
            IssueRefusal::EmptyBatch => "a batch with no item names no market",
            IssueRefusal::MixedMarkets => "the batch's items name more than one market",
        })
    }
}

impl std::error::Error for IssueRefusal {}

/// Why [`Authorization::check_at_submit`] refused an authorization: nothing is written for it.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum StaleAuthorization {
    /// The market's state changed since the command was built: the kill switch, a disarm, an
    /// arming call or lease names given again (0012).
    StateChanged {
        /// The market.
        market: InstrumentId,
        /// The generation the command was built under.
        built: StateGeneration,
        /// The market's generation at submit.
        now: StateGeneration,
    },
    /// An instrument cancel-all's market showed an order not ours since it was built, which
    /// the cancel-all would reach (0005's I7).
    ForeignSeen(InstrumentId),
}

impl fmt::Display for StaleAuthorization {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StaleAuthorization::StateChanged { market, built, now } => write!(
                f,
                "market {} changed state since the command was built (generation {} then, {} now)",
                market.get(),
                built.get(),
                now.get()
            ),
            StaleAuthorization::ForeignSeen(market) => write!(
                f,
                "an order not ours was seen on market {} since the cancel-all was built",
                market.get()
            ),
        }
    }
}

impl std::error::Error for StaleAuthorization {}

/// An order-affecting command `fbc-oms` admitted, for one account and one market, with that
/// market's state generation when the command was built. Only `fbc-oms` builds one
/// ([`Registry::authorize`]); it cannot be cloned or edited,
/// [`OrderGateway::submit`](crate::OrderGateway::submit) consumes it, and the gateway checks it
/// at submit ([`Authorization::check_at_submit`]).
#[must_use = "an authorization is spent by submitting it"]
#[derive(Debug)]
pub struct Authorization {
    acct: AccountKey,
    market: InstrumentId,
    generation: StateGeneration,
    cmd: VenueCommand,
    guard: Guard,
}

impl Registry {
    /// Issues the authorization for `cmd`, a command this registry built: the place or batch
    /// the market's state and the pre-trade caps admitted ([`Registry::place`],
    /// [`Registry::place_batch`]), the amend a [`Live`](crate::Live) permit built, the cancel or
    /// cancel-many a [`Cancellable`](crate::Cancellable) permit built, or the instrument
    /// cancel-all built under 0005's I7 guard ([`Registry::cancel_everything`]), for the
    /// account `acct`. A command a cap or the market's state refused was never built, so it
    /// never receives one.
    ///
    /// It carries the market's [`StateGeneration`] when the command was built (when the
    /// authorization was issued, for a cancel or a cancel-many, which no state holds back).
    /// It does not refuse a command whose market changed state since its build: the check at
    /// submit does ([`Authorization::check_at_submit`]), so a stale command has one fate, not
    /// sent, whichever comes first (decision 0060).
    ///
    /// Refused for a command that affects no order, an account cancel-all, an empty batch or a
    /// batch naming more than one market, none of which this crate builds.
    pub fn authorize(
        &self,
        acct: AccountKey,
        cmd: PermittedCommand,
    ) -> Result<Authorization, IssueRefusal> {
        let generation = self.entries.generation(market_of(cmd.command())?);
        Authorization::issue(acct, cmd, generation)
    }
}

impl Authorization {
    /// Issues an authorization for the command this crate built, carrying the generation of
    /// its build where it was watched, else `current`.
    pub(crate) fn issue(
        acct: AccountKey,
        cmd: PermittedCommand,
        current: StateGeneration,
    ) -> Result<Authorization, IssueRefusal> {
        let market = market_of(cmd.command())?;
        let (cmd, guard) = cmd.into_parts();
        let generation = guard
            .state
            .as_ref()
            .map_or(current, |w| StateGeneration(w.at()));
        Ok(Authorization {
            acct,
            market,
            generation,
            cmd,
            guard,
        })
    }

    /// The check at submit (decision 0060), which the gateway runs immediately before it
    /// encodes the command, writing nothing when it refuses: a place, a batch or an amend whose
    /// market changed state since the command was built is refused
    /// ([`StaleAuthorization::StateChanged`]), an instrument cancel-all also once an order not
    /// ours was seen on its market since ([`StaleAuthorization::ForeignSeen`]), and a cancel
    /// or a cancel-many always goes through. It reads the market's live state without the
    /// registry, from any thread.
    pub fn check_at_submit(&self) -> Result<(), StaleAuthorization> {
        let cancel_all = match &self.cmd {
            VenueCommand::Cancel(_) | VenueCommand::CancelMany(_) => return Ok(()),
            VenueCommand::CancelAll(_) => true,
            _ => false,
        };
        let stale = |now| StaleAuthorization::StateChanged {
            market: self.market,
            built: self.generation,
            now: StateGeneration(now),
        };
        // A command that needs a guard and carries none is refused: it was not built here.
        match &self.guard.state {
            None => return Err(stale(self.generation.get())),
            Some(state) => {
                if let Some(now) = state.moved() {
                    return Err(stale(now));
                }
            }
        }
        if cancel_all
            && self
                .guard
                .foreign
                .as_ref()
                .is_none_or(|seen| seen.moved().is_some())
        {
            return Err(StaleAuthorization::ForeignSeen(self.market));
        }
        Ok(())
    }

    /// The account the command is for.
    pub fn account(&self) -> AccountKey {
        self.acct
    }

    /// The market every item of the command names.
    pub fn market(&self) -> InstrumentId {
        self.market
    }

    /// The market's state generation when the command was built (for a cancel or a
    /// cancel-many, when the authorization was issued).
    pub fn generation(&self) -> StateGeneration {
        self.generation
    }

    /// The command to encode, read only.
    pub fn command(&self) -> &VenueCommand {
        &self.cmd
    }
}

/// The one market every item of an order-affecting command names.
fn market_of(cmd: &VenueCommand) -> Result<InstrumentId, IssueRefusal> {
    fn one(mut items: impl Iterator<Item = InstrumentId>) -> Result<InstrumentId, IssueRefusal> {
        let first = items.next().ok_or(IssueRefusal::EmptyBatch)?;
        if items.all(|inst| inst == first) {
            Ok(first)
        } else {
            Err(IssueRefusal::MixedMarkets)
        }
    }
    match cmd {
        VenueCommand::Place(order) => Ok(order.inst),
        VenueCommand::PlaceBatch(orders) => one(orders.iter().map(|o| o.inst)),
        VenueCommand::Amend(amend) => Ok(amend.inst),
        VenueCommand::Cancel(cancel) => Ok(cancel.inst),
        VenueCommand::CancelMany(cancels) => one(cancels.iter().map(|c| c.inst)),
        VenueCommand::CancelAll(CancelScope::Instrument(inst)) => Ok(*inst),
        VenueCommand::CancelAll(CancelScope::Account) => Err(IssueRefusal::AccountCancelAll),
        VenueCommand::ArmCancelOnDisconnect(_)
        | VenueCommand::RefreshDeadMan
        | VenueCommand::Query(_)
        | VenueCommand::FeeQuery => Err(IssueRefusal::NotOrderAffecting),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fbc_core::{
        AmendOrder, CancelOrder, Channel, OrderRef, QueryOrder, Side, Ticks, Tif, VenueCommand,
    };

    use crate::common::{cid, lots, placement};

    const ACCT: AccountKey = AccountKey::new(3);

    fn on(inst: InstrumentId, mut order: fbc_core::NewOrder) -> fbc_core::NewOrder {
        order.inst = inst;
        order
    }

    fn cancel(inst: InstrumentId) -> CancelOrder {
        CancelOrder {
            target: OrderRef::Client(cid()),
            inst,
            side: Side::Buy,
            placement_nonce: None,
        }
    }

    fn amend(inst: InstrumentId) -> AmendOrder {
        AmendOrder {
            target: OrderRef::Client(cid()),
            inst,
            side: Side::Buy,
            tif: Tif::Gtc,
            channel: Channel::Public,
            post_only: true,
            reduce_only: false,
            reducing: false,
            px: Ticks(101),
            qty: lots(2),
            cum_filled: lots(0),
        }
    }

    /// Every order-affecting command the OMS authorizes, each naming only `inst`.
    fn order_commands(inst: InstrumentId) -> Vec<VenueCommand> {
        vec![
            VenueCommand::Place(on(inst, placement(cid(), 100, 1))),
            VenueCommand::PlaceBatch(vec![
                on(inst, placement(cid(), 100, 1)),
                on(inst, placement(cid(), 99, 1)),
            ]),
            VenueCommand::Amend(amend(inst)),
            VenueCommand::Cancel(cancel(inst)),
            VenueCommand::CancelMany(vec![cancel(inst), cancel(inst)]),
            VenueCommand::CancelAll(CancelScope::Instrument(inst)),
        ]
    }

    /// Issues `cmd` as this crate builds it: a place, a batch, an amend or an instrument
    /// cancel-all guarded by the market's generation (and, for the last, the foreign orders
    /// seen), a cancel or a cancel-many unguarded, carrying `current`.
    fn issue(
        cmd: VenueCommand,
        generations: &mut Generations,
        seen: &mut Counters,
    ) -> Result<Authorization, IssueRefusal> {
        let market = market_of(&cmd).ok();
        let current = market.map_or(StateGeneration(0), |m| generations.of(m));
        let guard = match (&cmd, market) {
            (VenueCommand::Cancel(_) | VenueCommand::CancelMany(_), _) | (_, None) => {
                Guard::default()
            }
            (VenueCommand::CancelAll(_), Some(m)) => Guard {
                state: Some(generations.watch(m)),
                foreign: Some(seen.watch(m)),
            },
            (_, Some(m)) => Guard {
                state: Some(generations.watch(m)),
                foreign: None,
            },
        };
        Authorization::issue(ACCT, PermittedCommand::for_test(cmd, guard), current)
    }

    #[test]
    fn an_authorization_carries_the_command_as_this_crate_built_it() {
        let (mut generations, mut seen) = (Generations::default(), Counters::default());
        for cmd in order_commands(InstrumentId::new(1)) {
            let auth = issue(cmd.clone(), &mut generations, &mut seen).unwrap();
            assert_eq!(auth.command(), &cmd);
            assert_eq!(auth.account(), ACCT);
        }
    }

    #[test]
    fn an_authorization_carries_the_state_generation_of_the_market_it_was_issued_for() {
        let (btc, eth) = (InstrumentId::new(1), InstrumentId::new(2));
        let (mut generations, mut seen) = (Generations::default(), Counters::default());
        assert_eq!(generations.of(btc).get(), 0);
        // BTC's state changes twice, ETH's once: each market keeps its own count.
        assert_eq!(generations.advance(btc).get(), 1);
        assert_eq!(generations.advance(eth).get(), 1);
        assert_eq!(generations.advance(btc).get(), 2);

        for (market, generation) in [(btc, 2), (eth, 1)] {
            for cmd in order_commands(market) {
                let auth = issue(cmd.clone(), &mut generations, &mut seen).unwrap();
                assert_eq!(auth.market(), market, "{cmd:?}");
                assert_eq!(auth.generation().get(), generation, "{cmd:?}");
                assert_eq!(auth.check_at_submit(), Ok(()), "{cmd:?}");
            }
        }

        // One built before a state change keeps the generation it was built under, so the
        // check at submit can tell it is stale; one built after carries the new one.
        let place = || VenueCommand::Place(on(btc, placement(cid(), 100, 1)));
        let before = issue(place(), &mut generations, &mut seen).unwrap();
        let killed = generations.advance(btc);
        let after = issue(place(), &mut generations, &mut seen).unwrap();
        assert_eq!(before.generation().get(), 2);
        assert_eq!((after.generation(), killed.get()), (killed, 3));
        assert!(before.generation() < after.generation());
        // ETH's generation did not move with BTC's.
        assert_eq!(generations.of(eth).get(), 1);
    }

    #[test]
    fn at_submit_a_place_batch_amend_or_cancel_all_built_before_a_state_change_is_refused() {
        let (btc, eth) = (InstrumentId::new(1), InstrumentId::new(2));
        let (mut generations, mut seen) = (Generations::default(), Counters::default());
        let auths: Vec<Authorization> = order_commands(btc)
            .into_iter()
            .map(|cmd| issue(cmd, &mut generations, &mut seen).unwrap())
            .collect();
        // Another market's change leaves them all standing.
        generations.advance(eth);
        assert!(auths.iter().all(|a| a.check_at_submit().is_ok()));

        generations.advance(btc);
        for auth in &auths {
            let checked = auth.check_at_submit();
            match auth.command() {
                VenueCommand::Cancel(_) | VenueCommand::CancelMany(_) => {
                    assert_eq!(checked, Ok(()), "{auth:?}");
                }
                _ => assert_eq!(
                    checked,
                    Err(StaleAuthorization::StateChanged {
                        market: btc,
                        built: StateGeneration(0),
                        now: StateGeneration(1),
                    }),
                    "{auth:?}"
                ),
            }
        }
    }

    #[test]
    fn at_submit_an_instrument_cancel_all_is_refused_once_an_order_not_ours_was_seen() {
        let (btc, eth) = (InstrumentId::new(1), InstrumentId::new(2));
        let (mut generations, mut seen) = (Generations::default(), Counters::default());
        let cancel_all = issue(
            VenueCommand::CancelAll(CancelScope::Instrument(btc)),
            &mut generations,
            &mut seen,
        )
        .unwrap();
        let place = issue(
            VenueCommand::Place(on(btc, placement(cid(), 100, 1))),
            &mut generations,
            &mut seen,
        )
        .unwrap();
        seen.advance(eth);
        assert_eq!(cancel_all.check_at_submit(), Ok(()));
        seen.advance(btc);
        assert_eq!(
            cancel_all.check_at_submit(),
            Err(StaleAuthorization::ForeignSeen(btc))
        );
        // A place does not reach orders not ours: what is seen does not hold it back.
        assert_eq!(place.check_at_submit(), Ok(()));
    }

    #[test]
    fn at_submit_a_command_that_needs_a_guard_and_carries_none_is_refused() {
        // Fail closed: every place, batch, amend and cancel-all this crate builds is guarded.
        let btc = InstrumentId::new(1);
        let unguarded = |cmd| {
            Authorization::issue(
                ACCT,
                PermittedCommand::for_test(cmd, Guard::default()),
                StateGeneration(4),
            )
            .unwrap()
        };
        for cmd in order_commands(btc) {
            let checked = unguarded(cmd.clone()).check_at_submit();
            match cmd {
                VenueCommand::Cancel(_) | VenueCommand::CancelMany(_) => {
                    assert_eq!(checked, Ok(()));
                }
                _ => assert_eq!(
                    checked,
                    Err(StaleAuthorization::StateChanged {
                        market: btc,
                        built: StateGeneration(4),
                        now: StateGeneration(4),
                    }),
                    "{cmd:?}"
                ),
            }
        }
        // A cancel-all guarded by the state but not by what is seen is refused too.
        let mut generations = Generations::default();
        let half = Authorization::issue(
            ACCT,
            PermittedCommand::for_test(
                VenueCommand::CancelAll(CancelScope::Instrument(btc)),
                Guard {
                    state: Some(generations.watch(btc)),
                    foreign: None,
                },
            ),
            StateGeneration(0),
        )
        .unwrap();
        assert_eq!(
            half.check_at_submit(),
            Err(StaleAuthorization::ForeignSeen(btc))
        );
    }

    #[test]
    fn refusals_say_what_was_refused() {
        let btc = InstrumentId::new(1);
        let stale = StaleAuthorization::StateChanged {
            market: btc,
            built: StateGeneration(2),
            now: StateGeneration(5),
        };
        assert_eq!(
            stale.to_string(),
            "market 1 changed state since the command was built (generation 2 then, 5 now)"
        );
        assert_eq!(
            StaleAuthorization::ForeignSeen(btc).to_string(),
            "an order not ours was seen on market 1 since the cancel-all was built"
        );
        for (refusal, text) in [
            (
                IssueRefusal::NotOrderAffecting,
                "the command affects no order",
            ),
            (
                IssueRefusal::AccountCancelAll,
                "no record admits an account cancel-all",
            ),
            (
                IssueRefusal::EmptyBatch,
                "a batch with no item names no market",
            ),
            (
                IssueRefusal::MixedMarkets,
                "the batch's items name more than one market",
            ),
        ] {
            assert_eq!(refusal.to_string(), text);
        }
    }

    #[test]
    fn no_authorization_for_a_command_that_names_no_single_market_or_affects_no_order() {
        let (mut generations, mut seen) = (Generations::default(), Counters::default());
        let (btc, eth) = (InstrumentId::new(1), InstrumentId::new(2));
        let mut refused = |cmd| issue(cmd, &mut generations, &mut seen).unwrap_err();
        // No record admits an account cancel-all (0005's I7 admits only the instrument one).
        assert_eq!(
            refused(VenueCommand::CancelAll(CancelScope::Account)),
            IssueRefusal::AccountCancelAll
        );
        // A batch carries one market's generation, so it names exactly one market.
        assert_eq!(
            refused(VenueCommand::PlaceBatch(vec![])),
            IssueRefusal::EmptyBatch
        );
        assert_eq!(
            refused(VenueCommand::CancelMany(vec![])),
            IssueRefusal::EmptyBatch
        );
        assert_eq!(
            refused(VenueCommand::PlaceBatch(vec![
                on(btc, placement(cid(), 100, 1)),
                on(eth, placement(cid(), 100, 1)),
            ])),
            IssueRefusal::MixedMarkets
        );
        assert_eq!(
            refused(VenueCommand::CancelMany(vec![cancel(btc), cancel(eth)])),
            IssueRefusal::MixedMarkets
        );
        // Commands that affect no order take the gateway's control path instead.
        let query = QueryOrder {
            target: OrderRef::Client(cid()),
            inst: btc,
            placement_nonce: None,
        };
        for cmd in [
            VenueCommand::Query(query),
            VenueCommand::FeeQuery,
            VenueCommand::RefreshDeadMan,
            VenueCommand::ArmCancelOnDisconnect(true),
            VenueCommand::ArmCancelOnDisconnect(false),
        ] {
            assert_eq!(refused(cmd), IssueRefusal::NotOrderAffecting);
        }
    }
}
