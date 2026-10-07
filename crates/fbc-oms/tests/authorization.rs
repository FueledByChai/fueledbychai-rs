//! Authorizations and their check at submit (decisions 0045 and 0060; 0005's I4, I6 and I7;
//! 0012; 0013 rule 2), through a recording gateway that runs the check immediately before it
//! would encode, as a live gateway does, and records only what passes.
//!
//! A command a cap or the market's state refuses is never built, so it never receives an
//! authorization and never reaches the gateway. An authorization carries its market's state
//! generation at the command's build; a place, a batch or an amend built before the kill switch
//! went on, before a disarm, or before any other change of its market's state (an arming call,
//! lifting the kill switch, lease names given again that uncover the market) is refused at
//! submit with nothing recorded, so a kill switch turned on in the middle of a quote ladder
//! stops its remaining authorized places. A cancel and a cancel-many still reach the gateway
//! under the kill switch, built before it or after; the kill switch's instrument cancel-all
//! reaches it while 0005's I7 inputs are as they were when it was built (the registry's hold of
//! the market's exclusive lease unchanged: no disarm, no arming again, no lease names given
//! again that uncover it; no order not ours seen on the market since), whatever else of the
//! market's state changed: built before the kill switch went on, or under it and then the
//! switch lifted, it is still written (Reviewer B's RB94-1). No authorization is ever issued
//! for an account-scope cancel-all, in any state, though the venue declares one.
//!
//! The values are the owner's first test values: a $50 inventory cap on a synthetic market
//! where one lot is worth $1, so 50 lots; the resting cap is $11 per side, 11 lots.

#[path = "common/arm.rs"]
mod arm;
mod common;

use std::time::Duration;

use arm::{lease_keys, start};
use common::{cid, lots, order_caps, placement, vid};
use fbc_core::{
    AccountKey, AckLevel, AmendAck, AmendCaps, AmendQty, CancelBatch, CancelScope, CidMatch,
    ClientOrderId, EncodeCtx, EncodeReceipt, InstrumentId, ItemRef, MonoNs, Namespace, NewOrder,
    NonceBlock, NonceScope, NotSentReason, OrderCaps, OrderKind, OrderUpdate, PathStamps, RefKind,
    RpcId, Side, SignedLots, SubmitHandle, SubmitOutcome, Support, TagSet, Ticks, VenueCommand,
    VenueOrderState, WallNs,
};
use fbc_oms::{
    AmendRefusal, Authorization, CancelChoice, CancelEverything, CapRefusal, ControlCommand,
    EntryState, ExitKind, LadderConfig, Leases, MarketCapsConfig, OmsError, OrderGateway, OrderKey,
    OrderOp, PermittedCommand, PreTradeCaps, Registry, ResyncSnapshot, StaleAuthorization,
    StateRefusal,
};

const INST: InstrumentId = InstrumentId::new(1);
const OTHER: InstrumentId = InstrumentId::new(2);
const ACCT: AccountKey = AccountKey::new(1);
/// The owner's first inventory cap, in lots of $1.
const CAP: i64 = 50;
/// The owner's first resting cap per side, in lots of $1.
const RESTING: i64 = 11;

/// What the recording gateway did with one submission.
#[derive(Debug, PartialEq)]
enum Seen {
    /// Passed the check at submit: what would have been encoded and written.
    Written(AccountKey, VenueCommand),
    /// Refused by the check at submit: nothing encoded, nothing written.
    Refused(Why),
}

/// A refusal at submit, its generations as numbers.
#[derive(Copy, Clone, Debug, PartialEq)]
enum Why {
    /// The market's state changed: the generation the command was built under, and now.
    State(InstrumentId, u64, u64),
    /// An order not ours was seen on the cancel-all's market.
    Foreign(InstrumentId),
    /// The registry's hold of the cancel-all's market's exclusive lease changed.
    Lease(InstrumentId),
}

impl From<StaleAuthorization> for Why {
    fn from(why: StaleAuthorization) -> Why {
        match why {
            StaleAuthorization::StateChanged { market, built, now } => {
                Why::State(market, built.get(), now.get())
            }
            StaleAuthorization::ForeignSeen(market) => Why::Foreign(market),
            StaleAuthorization::ExclusiveLeaseChanged(market) => Why::Lease(market),
        }
    }
}

/// A gateway that runs fbc-oms's check at submit immediately before it would encode, as a live
/// gateway does (decision 0060), and records what it would have written. A refused one is
/// reported `NotSent(StaleAuthorization)`, as fbc-runtime's order-entry session reports it
/// (decision 0062).
#[derive(Default)]
struct Recording {
    seen: Vec<Seen>,
    next: u64,
}

impl Recording {
    fn rpc(&mut self) -> RpcId {
        self.next += 1;
        RpcId(self.next)
    }

    /// The commands written, in order.
    fn written(&self) -> Vec<&VenueCommand> {
        self.seen
            .iter()
            .filter_map(|s| match s {
                Seen::Written(_, cmd) => Some(cmd),
                Seen::Refused(_) => None,
            })
            .collect()
    }

    fn refused(&self) -> Vec<Why> {
        self.seen
            .iter()
            .filter_map(|s| match s {
                Seen::Refused(why) => Some(*why),
                Seen::Written(..) => None,
            })
            .collect()
    }

    /// Submits `auth` as a consumer does.
    fn send(&mut self, auth: Authorization) -> SubmitHandle {
        let ctx = EncodeCtx {
            wall: WallNs(0),
            mono: MonoNs(0),
            nonces: NonceBlock::EMPTY,
        };
        self.submit(auth, &ctx, &mut PathStamps::off())
    }
}

impl OrderGateway for Recording {
    fn submit(
        &mut self,
        auth: Authorization,
        _ctx: &EncodeCtx,
        _t: &mut PathStamps<'_>,
    ) -> SubmitHandle {
        let rpc = self.rpc();
        match auth.check_at_submit() {
            Ok(()) => {
                self.seen
                    .push(Seen::Written(auth.account(), auth.command().clone()));
                SubmitHandle {
                    rpc,
                    receipt: Ok(EncodeReceipt::new()),
                }
            }
            Err(why) => {
                self.seen.push(Seen::Refused(why.into()));
                SubmitHandle {
                    rpc,
                    receipt: Err(NotSentReason::StaleAuthorization),
                }
            }
        }
    }

    fn submit_control(
        &mut self,
        _acct: AccountKey,
        _cmd: ControlCommand,
        _ctx: &EncodeCtx,
        _t: &mut PathStamps<'_>,
    ) -> SubmitHandle {
        unreachable!("these tests submit no control command")
    }
}

fn caps() -> PreTradeCaps {
    [INST, OTHER]
        .into_iter()
        .try_fold(PreTradeCaps::new(), |caps, m| {
            caps.with_market(
                m,
                MarketCapsConfig {
                    inventory: Some(lots(CAP)),
                    resting: Some(lots(RESTING)),
                },
            )
        })
        .unwrap()
}

/// A venue that amends keeping the venue id, cancels by venue id singly and in batches, and
/// declares both an instrument and an account cancel-all, so that never building the account
/// one is the OMS's choice, not the venue's.
fn venue() -> OrderCaps {
    OrderCaps {
        amend: Some(AmendCaps {
            refs: TagSet::of(&[RefKind::Venue]),
            price: true,
            qty: true,
            flags: false,
            when_partially_filled: true,
            reject_keeps_original: true,
            keeps_venue_id: true,
            ack: AmendAck::ReplacedEvent,
            qty_semantics: AmendQty::TotalIncludingFilled,
            keeps_priority: None,
        }),
        batch_cancel: Some(CancelBatch {
            max_items: 10,
            refs: TagSet::of(&[RefKind::Venue]),
        }),
        cancel_all_account: Support::Native,
        cancel_all_instrument: Support::Native,
        ..order_caps()
    }
}

fn ladder_cfg() -> LadderConfig {
    LadderConfig::new(
        Duration::from_secs(1),
        Duration::ZERO,
        Duration::from_secs(10),
        1,
    )
    .unwrap()
}

/// A named registry under the caps whose first trustworthy resync showed no order and seeded
/// both markets flat, with both armed and Quoting.
fn quoting() -> Registry {
    let mut reg = arm::named(Registry::with_caps(caps()));
    let snap = ResyncSnapshot {
        watermark: WallNs(1_000),
        requested_at: MonoNs(1_000),
        orders: vec![],
        positions: vec![(INST, SignedLots(0)), (OTHER, SignedLots(0))],
    };
    let key = OrderKey {
        venue: None,
        ingest: 1,
    };
    reg.resync(&ladder_cfg(), &venue(), &snap, key).unwrap();
    start(&mut reg, INST);
    start(&mut reg, OTHER);
    reg
}

fn bid(px: i64, qty: i64) -> NewOrder {
    placement(cid(), px, qty)
}

fn on(market: InstrumentId, order: NewOrder) -> NewOrder {
    NewOrder {
        inst: market,
        ..order
    }
}

fn sell(qty: i64, px: i64) -> NewOrder {
    NewOrder {
        side: Side::Sell,
        kind: OrderKind::Limit { px: Ticks(px) },
        ..placement(cid(), px, qty)
    }
}

/// Places `order` through the gateway and acknowledges it under the venue id `v`: Open.
fn open(reg: &mut Registry, gw: &mut Recording, order: NewOrder, v: &str) -> ClientOrderId {
    let c = order.cid;
    let cmd = reg.place(order).unwrap();
    let auth = reg.authorize(ACCT, cmd).unwrap();
    assert!(gw.send(auth).receipt.is_ok());
    let item = ItemRef {
        idx: 0,
        cid: None,
        vid: Some(vid(v)),
    };
    let accepted = SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    };
    reg.on_outcome(c, OrderOp::Place, &item, &accepted, MonoNs(1))
        .unwrap();
    c
}

/// The command of a single cancel `cid`'s permit built.
fn cancel_of(reg: &mut Registry, c: ClientOrderId) -> PermittedCommand {
    match reg.cancellable(c).unwrap().cancel(&venue()) {
        CancelChoice::Send(cmd) => cmd,
        CancelChoice::AwaitAck => panic!("the order is acknowledged"),
    }
}

fn stale(market: InstrumentId, built: u64, now: u64) -> Why {
    Why::State(market, built, now)
}

#[test]
fn a_command_a_cap_or_the_market_state_refuses_never_reaches_the_gateway() {
    let mut reg = quoting();
    let mut gw = Recording::default();

    // The resting cap (11 per side): a buy of 12 is refused, never built, never authorized.
    assert!(matches!(
        reg.place(bid(100, 12)),
        Err(OmsError::Capped(CapRefusal::RestingCap { .. }))
    ));
    // A batch of ten buys of 2 is cut where it would breach it: five built, five refused.
    let plan = reg
        .place_batch((0..10).map(|i| bid(100 - i, 2)).collect())
        .unwrap();
    assert_eq!(plan.refused.len(), 5);
    let batch = plan.command.unwrap();
    let VenueCommand::PlaceBatch(items) = batch.command() else {
        panic!("a batch")
    };
    assert_eq!(items.len(), 5);
    let built: Vec<ClientOrderId> = items.iter().map(|o| o.cid).collect();
    gw.send(reg.authorize(ACCT, batch).unwrap());
    let VenueCommand::PlaceBatch(written) = gw.written()[0] else {
        panic!("a batch")
    };
    assert_eq!(written.iter().map(|o| o.cid).collect::<Vec<_>>(), built);

    // The kill switch: no place, batch item or amend is built on a Killed market, reducing
    // ones included, so none receives an authorization.
    let resting = open(&mut reg, &mut gw, on(OTHER, sell(3, 101)), "v-ask");
    reg.kill(INST);
    reg.kill(OTHER);
    assert_eq!(
        reg.place(bid(100, 1)).unwrap_err(),
        OmsError::State(StateRefusal::Killed(INST))
    );
    let refused = reg.place_batch(vec![bid(100, 1), bid(99, 1)]).unwrap();
    assert_eq!(refused.command, None);
    assert!(
        refused
            .refused
            .iter()
            .all(|(_, why)| *why == OmsError::State(StateRefusal::Killed(INST)))
    );
    assert_eq!(
        reg.live(resting)
            .unwrap()
            .amend(&venue(), Ticks(100), lots(2), true)
            .unwrap_err(),
        AmendRefusal::State(StateRefusal::Killed(OTHER))
    );

    // Only the batch and the ask reached the gateway, and nothing was refused there: the
    // refusals came before any authorization existed.
    assert_eq!(gw.written().len(), 2);
    assert!(gw.refused().is_empty());
}

#[test]
fn a_place_authorized_before_the_kill_switch_went_on_is_refused_at_submit_with_nothing_written() {
    // A quote ladder of four places, each built and authorized in one decide pass; the kill
    // switch goes on after the second is written: the remaining two are refused.
    let mut reg = quoting();
    let mut gw = Recording::default();
    let built = reg.entry(INST).generation().get();
    let ladder: Vec<Authorization> = (0..4)
        .map(|i| {
            let cmd = reg.place(bid(100 - i, 1)).unwrap();
            reg.authorize(ACCT, cmd).unwrap()
        })
        .collect();
    let mut ladder = ladder.into_iter();
    for auth in ladder.by_ref().take(2) {
        assert!(gw.send(auth).receipt.is_ok());
    }
    reg.kill(INST);
    for auth in ladder {
        assert_eq!(auth.generation().get(), built);
        assert_eq!(
            gw.send(auth).receipt,
            Err(NotSentReason::StaleAuthorization),
            "the reason the order-entry session reports (decision 0062)"
        );
    }
    assert_eq!(gw.written().len(), 2);
    assert_eq!(
        gw.refused(),
        vec![stale(INST, built, built + 1), stale(INST, built, built + 1)]
    );
}

#[test]
fn a_place_batch_or_amend_authorized_before_a_disarm_or_any_state_change_is_refused_at_submit() {
    type Change = fn(&mut Registry);
    let changes: [(&str, Change); 6] = [
        ("kill switch", |reg| {
            reg.kill(INST);
        }),
        ("disarm", |reg| {
            reg.disarm(INST);
        }),
        ("flatten", |reg| {
            reg.flatten(INST, Leases::none()).unwrap();
        }),
        ("wind-down", |reg| {
            reg.wind_down(INST, Leases::none()).unwrap();
        }),
        ("kill switch on and lifted", |reg| {
            reg.kill(INST);
            reg.lift_kill(INST);
        }),
        ("disarmed and started again", |reg| {
            reg.disarm(INST);
            start(reg, INST);
        }),
    ];
    for (name, change) in changes {
        let mut reg = quoting();
        let mut gw = Recording::default();
        let resting = open(&mut reg, &mut gw, sell(3, 101), "v-ask");
        let built = reg.entry(INST).generation().get();
        let place = reg.place(bid(100, 1)).unwrap();
        let batch = reg
            .place_batch(vec![bid(99, 1), bid(98, 1)])
            .unwrap()
            .command
            .unwrap();
        let amend = reg
            .live(resting)
            .unwrap()
            .amend(&venue(), Ticks(102), lots(3), false)
            .unwrap();
        // Authorized before the change for some, after it for the others: either way the
        // authorization carries the generation the command was built under.
        let place = reg.authorize(ACCT, place).unwrap();
        change(&mut reg);
        let now = reg.entry(INST).generation().get();
        assert!(now > built, "{name}");
        let batch = reg.authorize(ACCT, batch).unwrap();
        let amend = reg.authorize(ACCT, amend).unwrap();
        for auth in [place, batch, amend] {
            assert_eq!(auth.generation().get(), built, "{name}");
            gw.send(auth);
        }
        assert_eq!(gw.written().len(), 1, "{name}: only the ask's place");
        assert_eq!(gw.refused(), vec![stale(INST, built, now); 3], "{name}");
    }
}

#[test]
fn another_market_changing_state_holds_nothing_back() {
    let mut reg = quoting();
    let mut gw = Recording::default();
    let place = reg.place(bid(100, 1)).unwrap();
    let auth = reg.authorize(ACCT, place).unwrap();
    reg.kill(OTHER);
    reg.disarm(OTHER);
    gw.send(auth);
    assert_eq!(gw.written().len(), 1);
    assert!(gw.refused().is_empty());
}

#[test]
fn lease_names_given_again_that_uncover_an_armed_market_refuse_what_was_built_before() {
    // Reviewer B's RB86-3 on PR #86: the market stays armed, so only the names moved; what it
    // admits changed, and so does its generation.
    let mut reg = quoting();
    let mut gw = Recording::default();
    let built = reg.entry(INST).generation().get();
    let place = reg.place(bid(100, 1)).unwrap();
    let mut reg = reg.with_lease_keys(lease_keys(NonceScope::PerAccountMonotonic));
    assert_eq!(
        reg.place(bid(100, 1)).unwrap_err(),
        OmsError::State(StateRefusal::Unleased(INST))
    );
    let now = reg.entry(INST).generation().get();
    assert_eq!(now, built + 1);
    gw.send(reg.authorize(ACCT, place).unwrap());
    assert_eq!(gw.refused(), vec![stale(INST, built, now)]);
    assert!(gw.written().is_empty());
}

#[test]
fn cancels_still_reach_the_gateway_under_the_kill_switch_built_before_it_or_after() {
    let mut reg = quoting();
    let mut gw = Recording::default();
    let a = open(&mut reg, &mut gw, bid(100, 1), "v-a");
    let b = open(&mut reg, &mut gw, bid(99, 1), "v-b");
    let c = open(&mut reg, &mut gw, bid(98, 1), "v-c");
    // Built before the kill switch, authorized before or after it.
    let before = cancel_of(&mut reg, a);
    let before = reg.authorize(ACCT, before).unwrap();
    let many = reg.cancel_many(&[b], &venue()).commands.remove(0);
    reg.kill(INST);
    assert_eq!(reg.entry(INST).state(), EntryState::Killed);
    let many = reg.authorize(ACCT, many).unwrap();
    // Built after it.
    let after = cancel_of(&mut reg, c);
    let after = reg.authorize(ACCT, after).unwrap();
    let mut cmds = vec![];
    for auth in [before, many, after] {
        cmds.push(auth.command().clone());
        assert!(gw.send(auth).receipt.is_ok());
    }
    assert!(gw.refused().is_empty());
    assert_eq!(gw.written()[3..].to_vec(), cmds.iter().collect::<Vec<_>>());
    // Each names one of our orders by its venue id (0005's I4).
    for cmd in &cmds {
        let targets = match cmd {
            VenueCommand::Cancel(one) => vec![one.target.clone()],
            VenueCommand::CancelMany(many) => many.iter().map(|c| c.target.clone()).collect(),
            other => panic!("not a cancel: {other:?}"),
        };
        assert!(targets.iter().all(|t| t.venue().is_some()), "{cmd:?}");
    }
}

#[test]
fn the_kill_switchs_cancel_everything_reaches_the_gateway_within_0005s_guards() {
    let mut reg = quoting();
    let mut gw = Recording::default();
    open(&mut reg, &mut gw, bid(100, 1), "v-a");
    reg.kill(INST);
    // The registry holds the market's lease and nothing foreign is in view: an instrument
    // cancel-all, which reaches the gateway under the kill switch.
    let CancelEverything::CancelAll {
        command,
        unanswered,
    } = reg.cancel_everything(INST, &venue())
    else {
        panic!("the guard holds")
    };
    assert!(unanswered.commands.is_empty());
    gw.send(reg.authorize(ACCT, command).unwrap());
    assert_eq!(
        gw.written().last(),
        Some(&&VenueCommand::CancelAll(CancelScope::Instrument(INST)))
    );

    // Built again, then an order not ours comes into view before it is written: the cancel-all
    // would reach it, so it is refused at submit, and the cancel everything built now is
    // explicit, which reaches the gateway.
    let CancelEverything::CancelAll { command, .. } = reg.cancel_everything(INST, &venue()) else {
        panic!("the guard holds")
    };
    let cancel_all = reg.authorize(ACCT, command).unwrap();
    let theirs = OrderUpdate {
        cid: Some(CidMatch::Foreign(Namespace::new(8))),
        vid: Some(vid("v-theirs")),
        inst: INST,
        side: Side::Buy,
        state: VenueOrderState::Open,
        cum_filled: lots(0),
        px: Some(Ticks(99)),
        qty: Some(lots(3)),
        post_only: None,
        reduce_only: None,
    };
    reg.apply_update(
        &theirs,
        OrderKey {
            venue: None,
            ingest: 9,
        },
    );
    gw.send(cancel_all);
    assert_eq!(gw.refused(), vec![Why::Foreign(INST)]);
    let CancelEverything::Explicit { plan, .. } = reg.cancel_everything(INST, &venue()) else {
        panic!("an order not ours is in view")
    };
    let written = gw.written().len();
    for cmd in plan.commands {
        gw.send(reg.authorize(ACCT, cmd).unwrap());
    }
    assert_eq!(gw.written().len(), written + 1);
    assert_eq!(gw.refused().len(), 1);

    // A cancel-all built, then the market disarmed (its lease dropped) before it is written:
    // refused too, as the guard it was built under no longer holds.
    let mut reg = quoting();
    reg.kill(INST);
    let CancelEverything::CancelAll { command, .. } = reg.cancel_everything(INST, &venue()) else {
        panic!("the guard holds")
    };
    let auth = reg.authorize(ACCT, command).unwrap();
    reg.disarm(INST);
    let mut gw = Recording::default();
    gw.send(auth);
    assert_eq!(gw.refused(), vec![Why::Lease(INST)]);
}

#[test]
fn a_cancel_all_still_reaches_the_gateway_after_a_state_change_that_leaves_0005s_i7_guard_holding()
{
    // Reviewer B's RB94-1 on PR #94: I7's inputs are the market's exclusive lease (armed, its
    // held leases covered by the names now), the trustworthy resync, and the orders not ours in
    // view. The kill switch, its lift, Flatten and Wind-down on an armed market change none of
    // them, so a cancel-all built before them is still written: holding it back would leave
    // our Open orders resting on a Killed market.
    type Change = fn(&mut Registry);
    let changes: [(&str, Change, EntryState); 4] = [
        (
            "kill switch",
            |reg| {
                reg.kill(INST);
            },
            EntryState::Killed,
        ),
        (
            "flatten",
            |reg| {
                reg.flatten(INST, Leases::none()).unwrap();
            },
            EntryState::Exit(ExitKind::Flatten),
        ),
        (
            "wind-down",
            |reg| {
                reg.wind_down(INST, Leases::none()).unwrap();
            },
            EntryState::Exit(ExitKind::WindDown),
        ),
        (
            "kill switch on and lifted",
            |reg| {
                reg.kill(INST);
                reg.lift_kill(INST);
            },
            EntryState::CancelOnly,
        ),
    ];
    for (name, change, state) in changes {
        let mut reg = quoting();
        let mut gw = Recording::default();
        open(&mut reg, &mut gw, bid(100, 1), "v-a");
        let CancelEverything::CancelAll { command, .. } = reg.cancel_everything(INST, &venue())
        else {
            panic!("{name}: the guard holds")
        };
        let auth = reg.authorize(ACCT, command).unwrap();
        let built = reg.entry(INST).generation();
        change(&mut reg);
        assert_eq!(reg.entry(INST).state(), state, "{name}");
        assert!(reg.entry(INST).armed(), "{name}");
        assert!(reg.entry(INST).generation() > built, "{name}");
        gw.send(auth);
        assert!(gw.refused().is_empty(), "{name}: {:?}", gw.refused());
        assert_eq!(
            gw.written().last(),
            Some(&&VenueCommand::CancelAll(CancelScope::Instrument(INST))),
            "{name}"
        );
    }

    // Built under the kill switch, then the switch lifted before it is written: written too.
    let mut reg = quoting();
    let mut gw = Recording::default();
    open(&mut reg, &mut gw, bid(100, 1), "v-a");
    reg.kill(INST);
    let CancelEverything::CancelAll { command, .. } = reg.cancel_everything(INST, &venue()) else {
        panic!("the guard holds")
    };
    let auth = reg.authorize(ACCT, command).unwrap();
    reg.lift_kill(INST);
    gw.send(auth);
    assert!(gw.refused().is_empty(), "{:?}", gw.refused());
    assert_eq!(
        gw.written().last(),
        Some(&&VenueCommand::CancelAll(CancelScope::Instrument(INST)))
    );
}

#[test]
fn a_cancel_all_built_before_the_market_lease_was_dropped_taken_again_or_uncovered_is_refused() {
    // What does undo I7's guard: a disarm (the lease dropped), a disarm and Start (a lease
    // taken again since), lease names given again that no longer cover the held leases.
    type Change = fn(Registry) -> Registry;
    let changes: [(&str, Change); 4] = [
        ("disarm", |mut reg| {
            reg.disarm(INST);
            reg
        }),
        ("kill switch on, then disarm", |mut reg| {
            reg.kill(INST);
            reg.disarm(INST);
            reg
        }),
        ("disarmed and started again", |mut reg| {
            reg.disarm(INST);
            start(&mut reg, INST);
            reg
        }),
        ("lease names given again", |reg| {
            reg.with_lease_keys(lease_keys(NonceScope::PerAccountMonotonic))
        }),
    ];
    for (name, change) in changes {
        let mut reg = quoting();
        let CancelEverything::CancelAll { command, .. } = reg.cancel_everything(INST, &venue())
        else {
            panic!("{name}: the guard holds")
        };
        let auth = reg.authorize(ACCT, command).unwrap();
        let reg = change(reg);
        let mut gw = Recording::default();
        gw.send(auth);
        assert_eq!(gw.refused(), vec![Why::Lease(INST)], "{name}");
        assert!(gw.written().is_empty(), "{name}");
        drop(reg);
    }
}

#[test]
fn no_authorization_is_ever_issued_for_an_account_scope_cancel_all() {
    // Whatever the market's state, everything the registry builds for the kill switch, a
    // disarm, a ladder of places and the cancels of our orders authorizes to a command naming
    // one market; none is an account cancel-all, though the venue declares one.
    type Change = fn(&mut Registry);
    let states: [(&str, Change); 5] = [
        ("quoting", |_| {}),
        ("killed", |reg| {
            reg.kill(INST);
        }),
        ("cancel-only", |reg| {
            reg.disarm(INST);
        }),
        ("exit", |reg| {
            reg.flatten(INST, Leases::none()).unwrap();
        }),
        ("killed and disarmed", |reg| {
            reg.kill(INST);
            reg.disarm(INST);
        }),
    ];
    for (name, change) in states {
        let mut reg = quoting();
        let mut gw = Recording::default();
        let a = open(&mut reg, &mut gw, bid(100, 1), "v-a");
        let b = open(&mut reg, &mut gw, bid(99, 1), "v-b");
        change(&mut reg);
        let mut built: Vec<PermittedCommand> = vec![];
        match reg.cancel_everything(INST, &venue()) {
            CancelEverything::CancelAll {
                command,
                unanswered,
            } => {
                built.push(command);
                built.extend(unanswered.commands);
            }
            CancelEverything::Explicit { plan, .. } => built.extend(plan.commands),
        }
        built.extend(reg.cancel_many(&[a, b], &venue()).commands);
        built.push(cancel_of(&mut reg, a));
        if let Ok(place) = reg.place(bid(98, 1)) {
            built.push(place);
        }
        assert!(!built.is_empty(), "{name}");
        for cmd in built {
            assert_ne!(
                cmd.command(),
                &VenueCommand::CancelAll(CancelScope::Account),
                "{name}"
            );
            let auth = reg.authorize(ACCT, cmd).unwrap();
            assert_eq!(auth.market(), INST, "{name}");
            assert_eq!(auth.account(), ACCT, "{name}");
            gw.send(auth);
        }
        assert!(
            gw.written()
                .iter()
                .all(|cmd| !matches!(cmd, VenueCommand::CancelAll(CancelScope::Account))),
            "{name}"
        );
    }
}
