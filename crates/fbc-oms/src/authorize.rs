//! The authorization every order-affecting command needs to reach a gateway (decision 0045).
//!
//! 0013 rule 2 puts the caps and the kill switch in the one path every order command takes,
//! and that path is `fbc-oms`. [`VenueCommand`] is a plain value anyone can build, so the
//! gateway does not take one for an order: [`OrderGateway::submit`](crate::OrderGateway::submit)
//! takes an [`Authorization`], which only this crate can issue. It is issued for one account
//! and one market, for a place, an amend, a batch of places, a cancel, a cancel-many or an
//! instrument cancel-all; never for an account cancel-all, which no record admits (0005's I7
//! admits only the instrument one). It carries the market's [`StateGeneration`] at issue, so
//! the check at submit can refuse it once the market's state has moved on (FBC-afd). It has
//! no `Clone`, no public constructor and no way to edit its command, and submitting consumes
//! it, so each one is spent once (the compile-fail cases in `tests/ui_authorization/`).

use std::collections::BTreeMap;

use fbc_core::{AccountKey, CancelScope, InstrumentId, VenueCommand};

/// How many times a market's state has changed: the kill switch, an arming call, a disarm or
/// any other move of its order-entry state (0012). An [`Authorization`] carries the value its
/// market had when it was issued.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug)]
pub struct StateGeneration(u64);

impl StateGeneration {
    /// The generation as a number, starting at 0 for a market whose state never changed.
    pub fn get(self) -> u64 {
        self.0
    }
}

/// Each market's current [`StateGeneration`]. The market states (FBC-c4v) advance a market's
/// generation on every change of its state; nothing outside this crate can.
// Issued from only by the authorization's tests until the pre-trade path (FBC-afd) and the
// market states (FBC-c4v) use it.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Default, Debug)]
pub(crate) struct Generations {
    by_market: BTreeMap<InstrumentId, StateGeneration>,
}

#[cfg_attr(not(test), allow(dead_code))]
impl Generations {
    /// The market's current generation: 0 until its state first changes.
    pub(crate) fn of(&self, market: InstrumentId) -> StateGeneration {
        self.by_market
            .get(&market)
            .copied()
            .unwrap_or(StateGeneration(0))
    }

    /// Records a change of the market's state and returns its new generation. A generation is
    /// never reused: at a billion changes a second, `u64` lasts five centuries.
    pub(crate) fn advance(&mut self, market: InstrumentId) -> StateGeneration {
        let next = StateGeneration(
            self.of(market)
                .0
                .checked_add(1)
                .expect("a market's state generation never reaches u64::MAX"),
        );
        self.by_market.insert(market, next);
        next
    }
}

/// Why `fbc-oms` issues no authorization for a command.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) enum IssueRefusal {
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

/// An order-affecting command `fbc-oms` admitted, for one account and one market, with that
/// market's state generation when it was issued. Only `fbc-oms` builds one; it cannot be cloned
/// or edited, and [`OrderGateway::submit`](crate::OrderGateway::submit) consumes it.
#[must_use = "an authorization is spent by submitting it"]
#[derive(Debug)]
pub struct Authorization {
    acct: AccountKey,
    market: InstrumentId,
    generation: StateGeneration,
    cmd: VenueCommand,
}

impl Authorization {
    /// Issues an authorization for `cmd` on `acct`, carrying the generation `generations` holds
    /// for the command's market. Refused for a command that affects no order, an account
    /// cancel-all, an empty batch and a batch over several markets. The caps and the kill
    /// switch are checked before this is called (FBC-2e4, FBC-c4v, FBC-afd).
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn issue(
        acct: AccountKey,
        cmd: VenueCommand,
        generations: &Generations,
    ) -> Result<Authorization, IssueRefusal> {
        let market = market_of(&cmd)?;
        Ok(Authorization {
            acct,
            market,
            generation: generations.of(market),
            cmd,
        })
    }

    /// The account the command is for.
    pub fn account(&self) -> AccountKey {
        self.acct
    }

    /// The market every item of the command names.
    pub fn market(&self) -> InstrumentId {
        self.market
    }

    /// The market's state generation when the authorization was issued.
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

    #[test]
    fn an_authorization_carries_the_state_generation_of_the_market_it_was_issued_for() {
        let (btc, eth) = (InstrumentId::new(1), InstrumentId::new(2));
        let mut generations = Generations::default();
        assert_eq!(generations.of(btc).get(), 0);
        // BTC's state changes twice, ETH's once: each market keeps its own count.
        assert_eq!(generations.advance(btc).get(), 1);
        assert_eq!(generations.advance(eth).get(), 1);
        assert_eq!(generations.advance(btc).get(), 2);

        for (market, generation) in [(btc, 2), (eth, 1)] {
            for cmd in order_commands(market) {
                let auth = Authorization::issue(ACCT, cmd.clone(), &generations).unwrap();
                assert_eq!(auth.market(), market, "{cmd:?}");
                assert_eq!(auth.generation().get(), generation, "{cmd:?}");
                assert_eq!(auth.account(), ACCT);
                assert_eq!(auth.command(), &cmd);
            }
        }

        // One issued before a state change keeps the generation it was issued under, so the
        // check at submit (FBC-afd) can tell it is stale; one issued after carries the new one.
        let before = Authorization::issue(
            ACCT,
            VenueCommand::Place(on(btc, placement(cid(), 100, 1))),
            &generations,
        )
        .unwrap();
        let killed = generations.advance(btc);
        let after = Authorization::issue(
            ACCT,
            VenueCommand::Place(on(btc, placement(cid(), 100, 1))),
            &generations,
        )
        .unwrap();
        assert_eq!(before.generation().get(), 2);
        assert_eq!((after.generation(), killed.get()), (killed, 3));
        assert!(before.generation() < after.generation());
        // ETH's generation did not move with BTC's.
        assert_eq!(generations.of(eth).get(), 1);
    }

    #[test]
    fn no_authorization_for_a_command_that_names_no_single_market_or_affects_no_order() {
        let generations = Generations::default();
        let (btc, eth) = (InstrumentId::new(1), InstrumentId::new(2));
        let refused = |cmd| Authorization::issue(ACCT, cmd, &generations).unwrap_err();
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
