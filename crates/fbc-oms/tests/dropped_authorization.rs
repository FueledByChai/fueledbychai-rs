//! An authorization no gateway took over is never stranded (decision 0152, FBC-eeb4, with the
//! amend half FBC-45et).
//!
//! Once `Registry::authorize` issues a place's, a batch's or an amend's authorization, the
//! command is spent (decision 0082), so before 0152 an authorization dropped before a gateway
//! encoded it or reported anything of it left its orders PendingNew and counted, or the amend's
//! build reserved, until restart: nothing reported an outcome, the Unknown ladder never
//! escalates an order never sent, and the order got no further Live permit.
//!
//! - An authorization dropped before a gateway takes it over (`Authorization::hand_over`)
//!   hands its orders, or the amend's build, back to the registry that issued it, which releases
//!   them at its next mutating call: each place or batch item ends not sent
//!   (`NotSentReason::StaleAuthorization`) and frees what it counted, and the amend's build is
//!   withdrawn, so the order is amended again. A read before that call still shows them held.
//! - `Registry::withdraw` releases them at once.
//! - One a gateway took over is never released by its drop: what the gateway reports of it
//!   settles it. Nor is an order anything was reported for since, which fails closed.
//! - A cancel, a cancel-many or an instrument cancel-all holds nothing.
//!
//! The caps are test values: an inventory cap of 60 lots and a resting cap of 10 lots a side.

#[path = "common/arm.rs"]
mod arm;
mod common;
#[path = "common/venue.rs"]
mod venue;

use std::time::Duration;

use common::{cid, lots, order_caps, placement};
use fbc_core::{
    AccountKey, AckLevel, AmendAck, AmendCaps, AmendQty, Bps, Channel, CidMint, ClientOrderId,
    EncodeCtx, EncodeReceipt, InstrumentId, ItemRef, Lots, MonoNs, Namespace, NamespaceLease,
    NewOrder, NonceBlock, NotSentReason, OrderCaps, PathStamps, RefKind, Reject, RejectKind, RpcId,
    Side, SignedLots, SubmitHandle, SubmitOutcome, TagSet, Ticks, Tif, VenueCommand,
    VenueOrderState, WallNs,
};
use fbc_oms::{
    Admission, Authorization, CancelChoice, ControlCommand, DesiredBook, DesiredQuote,
    ExecutionPlanner, FillLedger, Intent, LedgerConfig, MarketCapsConfig, OmsError, OrdState,
    OrderGateway, OrderOp, PermitRefusal, PlannerConfig, PreTradeCaps, ReduceFallback, Registry,
    TerminalKind, TestnetRun,
};

const INST: InstrumentId = InstrumentId::new(1);
const ACCT: AccountKey = arm::TEST_ACCT;
/// The inventory cap, a test value.
const CAP: i64 = 60;
/// The resting cap a side, a test value.
const RESTING: i64 = 10;

/// A registry under the test caps, flat: seeded by hand and started, declared an
/// owner-assisted testnet run (decision 0067).
fn registry() -> Registry {
    let caps = PreTradeCaps::new()
        .with_market(
            INST,
            MarketCapsConfig {
                inventory: Some(lots(CAP)),
                resting: Some(lots(RESTING)),
            },
        )
        .unwrap();
    let mut reg =
        arm::named(Registry::with_caps(caps).for_testnet_run(TestnetRun::owner_assisted()));
    reg.seed_position(INST, SignedLots(0)).unwrap();
    arm::start(&mut reg, INST);
    reg
}

fn buy(qty: i64) -> NewOrder {
    placement(cid(), 100, qty)
}

/// A venue that amends a resting order's price and total by its venue id, and cancels by it.
fn amending() -> OrderCaps {
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
        ..order_caps()
    }
}

fn item(v: Option<&str>) -> ItemRef {
    ItemRef {
        idx: 0,
        cid: None,
        vid: v.map(common::vid),
    }
}

fn accepted() -> SubmitOutcome {
    SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    }
}

/// How an order ends once what held it is released.
fn released() -> OrdState {
    OrdState::Terminal(TerminalKind::NotSent(NotSentReason::StaleAuthorization))
}

/// Places `order`, hands its authorization to a gateway and acknowledges it under the venue id
/// `v`: Open.
fn open(reg: &mut Registry, order: NewOrder, v: &str) -> ClientOrderId {
    let c = order.cid;
    let cmd = reg.place(order).unwrap();
    arm::issue(reg, cmd);
    reg.on_outcome(c, OrderOp::Place, &item(Some(v)), &accepted(), MonoNs(1))
        .unwrap();
    assert_eq!(reg.get(c).unwrap().state(), OrdState::Open);
    c
}

/// The authorization of the amend of our resting order `c` to the total `qty`.
fn amend(reg: &mut Registry, c: ClientOrderId, qty: i64) -> Authorization {
    let cmd = reg
        .live(c)
        .unwrap()
        .amend(&amending(), Ticks(100), lots(qty), false)
        .unwrap();
    reg.authorize(ACCT, cmd).unwrap()
}

/// The authorization of the cancel of our acknowledged order `c`.
fn cancel(reg: &mut Registry, c: ClientOrderId) -> Authorization {
    let CancelChoice::Send(cmd) = reg.cancellable(c).unwrap().cancel(&amending()) else {
        panic!("an acknowledged order's cancel is sent");
    };
    reg.authorize(ACCT, cmd).unwrap()
}

/// What is resting on the buy side.
fn bids(reg: &Registry) -> Lots {
    reg.resting_on(INST, Side::Buy).unwrap()
}

/// A gateway that takes each authorization over, checks it at submit and keeps what it wrote,
/// as a live gateway does (decision 0060).
#[derive(Default)]
struct Writing {
    written: Vec<VenueCommand>,
}

impl OrderGateway for Writing {
    fn submit(
        &mut self,
        auth: Authorization,
        _ctx: &EncodeCtx,
        _t: &mut PathStamps<'_>,
    ) -> SubmitHandle {
        let handed = auth.hand_over();
        handed.check_at_submit().unwrap();
        self.written.push(handed.command().clone());
        SubmitHandle {
            rpc: RpcId(self.written.len() as u64),
            receipt: Ok(EncodeReceipt::new()),
        }
    }

    fn submit_control(
        &mut self,
        _acct: AccountKey,
        _cmd: ControlCommand,
        _ctx: &EncodeCtx,
        _t: &mut PathStamps<'_>,
    ) -> SubmitHandle {
        unreachable!("no control command is submitted here")
    }
}

impl Writing {
    fn send(&mut self, auth: Authorization) -> SubmitHandle {
        let ctx = EncodeCtx {
            wall: WallNs(0),
            mono: MonoNs(0),
            nonces: NonceBlock::EMPTY,
        };
        self.submit(auth, &ctx, &mut PathStamps::off())
    }
}

#[test]
fn a_place_whose_authorization_is_dropped_is_released_at_the_next_mutating_call() {
    let mut reg = registry();
    let order = buy(RESTING);
    let c = order.cid;
    let cmd = reg.place(order).unwrap();
    let auth = reg.authorize(ACCT, cmd).unwrap();
    drop(auth);
    // A read before the next mutating call still shows it held, as decision 0084 has it.
    assert_eq!(reg.get(c).unwrap().state(), OrdState::PendingNew);
    assert_eq!(bids(&reg), lots(RESTING));
    // The next build frees the side: the whole resting cap is placed again.
    let again = reg.place(buy(RESTING)).expect("the side is free again");
    assert_eq!(reg.get(c).unwrap().state(), released());
    assert_eq!(bids(&reg), lots(RESTING));
    drop(reg.authorize(ACCT, again).unwrap().hand_over());
}

#[test]
fn every_item_of_a_batch_whose_authorization_is_dropped_is_released() {
    let mut reg = registry();
    let (first, second) = (buy(4), buy(6));
    let cids = [first.cid, second.cid];
    let plan = reg.place_batch(vec![first, second]).unwrap();
    assert!(plan.refused.is_empty());
    drop(reg.authorize(ACCT, plan.command.unwrap()).unwrap());
    assert_eq!(bids(&reg), lots(RESTING));
    // Any mutating call releases them: here an outcome for an order of another kind.
    let o = open(&mut reg, buy(3), "o");
    for c in cids {
        assert_eq!(reg.get(c).unwrap().state(), released(), "{c:?}");
    }
    assert_eq!(reg.get(o).unwrap().state(), OrdState::Open);
    assert_eq!(bids(&reg), lots(3));
}

#[test]
fn an_amend_whose_authorization_is_dropped_releases_its_build_and_is_amended_again() {
    // FBC-45et: the build no longer counts as resting, and the order gets a Live permit again.
    let mut reg = registry();
    let c = open(&mut reg, buy(4), "a");
    let auth = amend(&mut reg, c, 8);
    assert_eq!(reg.get(c).unwrap().amend_built(), Some(lots(8)));
    assert_eq!(bids(&reg), lots(8));
    assert!(matches!(
        reg.live(c).map(|_| ()),
        Err(PermitRefusal::IntentPending(_))
    ));
    drop(auth);
    // Still counted until the next mutating call, which `live` is.
    assert_eq!(bids(&reg), lots(8));
    assert!(reg.live(c).is_ok());
    assert_eq!(reg.get(c).unwrap().amend_built(), None);
    assert_eq!(bids(&reg), lots(4));
    // Amended again, taken over by a gateway and dropped: the build stays.
    let mut gateway = Writing::default();
    gateway.send(amend(&mut reg, c, 9));
    assert!(reg.live(c).is_err());
    assert_eq!(reg.get(c).unwrap().amend_built(), Some(lots(9)));
    assert!(matches!(
        gateway.written[..],
        [VenueCommand::Amend(ref a)] if a.qty == lots(9)
    ));
}

#[test]
fn an_amend_given_back_unencoded_releases_its_build_when_dropped_and_is_amended_again() {
    // Handed back: a gateway that took the amend over and encoded nothing of it gives it back
    // (`Handed::unencoded`, as the session does on its nonce source's refusal), and what holds
    // it then drops it, as the decide pass's default `unsubmitted` does (decision 0100).
    let mut reg = registry();
    let c = open(&mut reg, buy(4), "a");
    let back = amend(&mut reg, c, 8).hand_over().unencoded();
    assert_eq!(reg.get(c).unwrap().amend_built(), Some(lots(8)));
    assert_eq!(bids(&reg), lots(8));
    drop(back);
    assert!(reg.live(c).is_ok());
    assert_eq!(reg.get(c).unwrap().amend_built(), None);
    assert_eq!(bids(&reg), lots(4));
}

#[test]
fn withdraw_releases_a_place_a_batch_or_an_amend_at_once() {
    let mut reg = registry();
    let order = buy(RESTING);
    let c = order.cid;
    let cmd = reg.place(order).unwrap();
    let auth = reg.authorize(ACCT, cmd).unwrap();
    assert!(reg.withdraw(auth));
    assert_eq!(reg.get(c).unwrap().state(), released());
    assert_eq!(bids(&reg), Lots::ZERO);

    let (first, second) = (buy(2), buy(3));
    let cids = [first.cid, second.cid];
    let plan = reg.place_batch(vec![first, second]).unwrap();
    let auth = reg.authorize(ACCT, plan.command.unwrap()).unwrap();
    assert!(reg.withdraw(auth));
    assert!(
        cids.iter()
            .all(|c| reg.get(*c).unwrap().state() == released())
    );

    let a = open(&mut reg, buy(4), "a");
    let auth = amend(&mut reg, a, 7);
    assert!(reg.withdraw(auth));
    assert_eq!(reg.get(a).unwrap().amend_built(), None);
    assert_eq!(bids(&reg), lots(4));
    assert!(reg.live(a).is_ok());
}

#[test]
fn withdraw_releases_nothing_already_released_and_a_cancel_holds_nothing() {
    let mut reg = registry();
    let a = open(&mut reg, buy(4), "a");
    // A cancel, withdrawn or dropped, changes nothing: it reserved nothing.
    let auth = cancel(&mut reg, a);
    assert!(!reg.withdraw(auth));
    drop(cancel(&mut reg, a));
    let _ = reg.place(buy(1)).unwrap();
    let rec = reg.get(a).unwrap();
    assert_eq!(rec.state(), OrdState::Open);
    assert_eq!((rec.intent(), rec.cancels_sent()), (Intent::None, 0));
    // An amend whose order ended since, and a place whose order ended since.
    let auth = amend(&mut reg, a, 6);
    let mut ended = common::update(Some(a), common::canceled(), 0);
    ended.vid = Some(common::vid("a"));
    reg.apply_update(
        &ended,
        fbc_oms::OrderKey {
            venue: None,
            ingest: 2,
        },
    );
    assert!(!reg.withdraw(auth));
    let order = buy(2);
    let c = order.cid;
    let cmd = reg.place(order).unwrap();
    let auth = reg.authorize(ACCT, cmd).unwrap();
    let not_sent = SubmitOutcome::NotSent(NotSentReason::Disconnected);
    reg.on_outcome(c, OrderOp::Place, &item(None), &not_sent, MonoNs(2))
        .unwrap();
    assert!(!reg.withdraw(auth));
    assert_eq!(
        reg.get(c).unwrap().state(),
        OrdState::Terminal(TerminalKind::NotSent(NotSentReason::Disconnected))
    );
}

#[test]
fn an_authorization_a_gateway_took_over_is_not_released_by_its_drop() {
    let mut reg = registry();
    let order = buy(RESTING);
    let c = order.cid;
    let cmd = reg.place(order).unwrap();
    let mut gateway = Writing::default();
    let sent = gateway.send(reg.authorize(ACCT, cmd).unwrap());
    assert!(sent.receipt.is_ok());
    // The gateway dropped what it took over: the place stays PendingNew and counted, its side
    // full, until the gateway's outcome settles it.
    assert!(matches!(reg.place(buy(1)), Err(OmsError::Capped(_))));
    assert_eq!(reg.get(c).unwrap().state(), OrdState::PendingNew);
    assert_eq!(bids(&reg), lots(RESTING));
    reg.on_outcome(c, OrderOp::Place, &item(Some("c")), &accepted(), MonoNs(1))
        .unwrap();
    assert_eq!(reg.get(c).unwrap().state(), OrdState::Open);
}

#[test]
fn a_handed_authorization_carries_what_it_was_and_given_back_unencoded_releases_again() {
    let mut reg = registry();
    let order = buy(RESTING);
    let c = order.cid;
    let cmd = reg.place(order.clone()).unwrap();
    let auth = reg.authorize(ACCT, cmd).unwrap();
    let (acct, market, generation) = (auth.account(), auth.market(), auth.generation());
    let handed = auth.hand_over();
    assert_eq!(
        (handed.account(), handed.market(), handed.generation()),
        (acct, market, generation)
    );
    assert_eq!(handed.command(), &VenueCommand::Place(order));
    assert_eq!(handed.check_at_submit(), Ok(()));
    // Given back by a gateway that encoded and reported nothing of it, it holds its order
    // again: dropped, it is released at the next mutating call.
    drop(handed.unencoded());
    let _ = reg.place(buy(1)).unwrap();
    assert_eq!(reg.get(c).unwrap().state(), released());
    // Given back and withdrawn, released at once.
    let order = buy(2);
    let w = order.cid;
    let cmd = reg.place(order).unwrap();
    let back = reg.authorize(ACCT, cmd).unwrap().hand_over().unencoded();
    assert!(reg.withdraw(back));
    assert_eq!(reg.get(w).unwrap().state(), released());
}

#[test]
fn another_registry_s_authorization_is_released_only_in_its_own() {
    let mut ours = registry();
    let mut theirs = registry();
    let order = buy(RESTING);
    let c = order.cid;
    let cmd = theirs.place(order).unwrap();
    let auth = theirs.authorize(ACCT, cmd).unwrap();
    assert!(!ours.withdraw(auth));
    assert!(ours.get(c).is_none());
    // Dropped by the refused withdrawal, it hands its order back to its own registry.
    assert_eq!(theirs.get(c).unwrap().state(), OrdState::PendingNew);
    let _ = theirs
        .place(buy(RESTING))
        .expect("their side is free again");
    assert_eq!(theirs.get(c).unwrap().state(), released());
}

#[test]
fn an_authorization_dropped_on_another_thread_is_released() {
    let mut reg = registry();
    let order = buy(RESTING);
    let c = order.cid;
    let cmd = reg.place(order).unwrap();
    let auth = reg.authorize(ACCT, cmd).unwrap();
    // The join is the event the registry waits on: the drop happened before it.
    std::thread::spawn(move || drop(auth)).join().unwrap();
    let _ = reg.place(buy(RESTING)).expect("the side is free again");
    assert_eq!(reg.get(c).unwrap().state(), released());
}

#[test]
fn an_order_anything_was_reported_for_is_not_released_by_its_dropped_authorization() {
    // Fail closed: a gateway reports only what it took over, so a report means it may have
    // been sent; the order keeps what it counts, and its outcome or the ladder settles it.
    let mut reg = registry();
    let order = buy(3);
    let c = order.cid;
    let cmd = reg.place(order).unwrap();
    let auth = reg.authorize(ACCT, cmd).unwrap();
    reg.on_outcome(c, OrderOp::Place, &item(Some("c")), &accepted(), MonoNs(1))
        .unwrap();
    drop(auth);
    let _ = reg.place(buy(1)).unwrap();
    assert_eq!(reg.get(c).unwrap().state(), OrdState::Open);
    // Reported sent, with no outcome yet.
    let order = buy(3);
    let s = order.cid;
    let cmd = reg.place(order).unwrap();
    let auth = reg.authorize(ACCT, cmd).unwrap();
    reg.placement_sent(s, MonoNs(2), WallNs(2)).unwrap();
    assert!(!reg.withdraw(auth));
    assert_eq!(reg.get(s).unwrap().state(), OrdState::PendingNew);
    assert!(reg.get(s).unwrap().sent_at().is_some());
    // An amend reported sent is the amend in flight, which its outcome resolves.
    let auth = amend(&mut reg, c, 5);
    reg.amend_sent(c, Ticks(100), lots(5), RpcId(9), MonoNs(3))
        .unwrap();
    drop(auth);
    let _ = reg.place(buy(1)).unwrap();
    assert!(matches!(
        reg.get(c).unwrap().intent(),
        Intent::PendingAmend { qty, .. } if qty == lots(5)
    ));
}

#[test]
fn a_planner_pass_s_dropped_authorization_is_released_by_the_next_pass() {
    let mut reg = registry();
    let lease = NamespaceLease::acquire(&common::lease_dir(), ACCT, Namespace::new(953)).unwrap();
    let mut mint = CidMint::new(lease, 0, 0, WallNs(0));
    let mut p = ExecutionPlanner::new(
        PlannerConfig::new(Bps(2.0), lots(2), Duration::from_millis(100)).unwrap(),
    );
    let book = DesiredBook::new(INST).with(
        Side::Buy,
        0,
        DesiredQuote {
            px: Ticks(100),
            qty: lots(RESTING),
            tif: Tif::Gtc,
            channel: Channel::Public,
            post_only: true,
            reduce_only: false,
            reducing: false,
            tag: 0,
            reduce_fallback: ReduceFallback::KeepReduceOnly,
        },
    );
    let caps = order_caps();
    let mut pass = |reg: &mut Registry, at| {
        p.plan(&book, reg, &venue::of(&caps), ACCT, &mut mint, MonoNs(at))
            .unwrap()
    };
    let first = pass(&mut reg, 0);
    assert_eq!(first.commands.len(), 1);
    let c = first.commands[0].cid;
    // The consumer drops the pass's commands, sending none: the next pass quotes the side.
    drop(first);
    let second = pass(&mut reg, 1);
    assert_eq!(reg.get(c).unwrap().state(), released());
    assert!(second.refused.is_empty(), "{:?}", second.refused);
    assert_eq!(second.commands.len(), 1);
    assert_ne!(second.commands[0].cid, c);
    assert_eq!(bids(&reg), lots(RESTING));
}

#[test]
fn each_report_on_a_pending_new_order_keeps_its_held_authorization_from_releasing_it() {
    // Fail closed, report by report: whatever a gateway or the venue reported of an order since
    // its authorization was issued, the authorization, held and not handed over, releases
    // nothing, and the order stays as the report left it.
    let mut reg = registry();
    let held = |reg: &mut Registry, qty| {
        let order = buy(qty);
        let c = order.cid;
        let cmd = reg.place(order).unwrap();
        (c, reg.authorize(ACCT, cmd).unwrap())
    };
    let key = |ingest| fbc_oms::OrderKey {
        venue: None,
        ingest,
    };
    let amended = VenueOrderState::Amended { new_vid: None };

    // An amended update naming a venue id: PendingNew, with a venue id.
    let (c, auth) = held(&mut reg, 1);
    let mut named = common::update(Some(c), amended.clone(), 0);
    named.vid = Some(common::vid("n"));
    reg.apply_update(&named, key(1));
    assert!(!reg.withdraw(auth));
    let rec = reg.get(c).unwrap();
    assert_eq!(
        (rec.state(), rec.vid()),
        (OrdState::PendingNew, named.vid.as_ref())
    );

    // An amended update stating a cumulative fill: PendingNew, with no venue id.
    let (c, auth) = held(&mut reg, 2);
    reg.apply_update(&common::update(Some(c), amended, 1), key(2));
    assert!(!reg.withdraw(auth));
    let rec = reg.get(c).unwrap();
    assert_eq!((rec.state(), rec.vid()), (OrdState::PendingNew, None));
    assert_eq!(rec.cum_venue(), lots(1));

    // An amend reported sent: the amend in flight.
    let (c, auth) = held(&mut reg, 1);
    reg.amend_sent(c, Ticks(100), lots(2), RpcId(5), MonoNs(3))
        .unwrap();
    assert!(!reg.withdraw(auth));
    let rec = reg.get(c).unwrap();
    assert_eq!(rec.state(), OrdState::PendingNew);
    assert!(matches!(rec.intent(), Intent::PendingAmend { .. }));
    assert_eq!(rec.cancels_sent(), 0);

    // A cancel reported sent, then not sent: nothing in flight, one cancel sent.
    let (c, auth) = held(&mut reg, 1);
    reg.cancel_sent(c, RpcId(6), MonoNs(4)).unwrap();
    let not_sent = SubmitOutcome::NotSent(NotSentReason::Disconnected);
    reg.on_outcome(
        c,
        OrderOp::Cancel(RpcId(6)),
        &item(None),
        &not_sent,
        MonoNs(5),
    )
    .unwrap();
    assert!(!reg.withdraw(auth));
    let rec = reg.get(c).unwrap();
    assert_eq!(rec.state(), OrdState::PendingNew);
    assert_eq!((rec.intent(), rec.cancels_sent()), (Intent::None, 1));

    // A cancel the venue does not know the order for: on the Unknown ladder, still PendingNew.
    let (c, auth) = held(&mut reg, 1);
    let unknown = SubmitOutcome::Rejected(Reject {
        kind: RejectKind::NotFound,
        venue_code: None,
        raw: "no such order".into(),
    });
    reg.on_outcome(
        c,
        OrderOp::Cancel(RpcId(7)),
        &item(None),
        &unknown,
        MonoNs(6),
    )
    .unwrap();
    assert!(!reg.withdraw(auth));
    let rec = reg.get(c).unwrap();
    assert_eq!(rec.state(), OrdState::PendingNew);
    assert_eq!((rec.intent(), rec.cancels_sent()), (Intent::None, 0));
    assert_eq!(rec.unknown_since(), Some(MonoNs(6)));

    // A fill: it promotes the order, which keeps what it counts.
    let (c, auth) = held(&mut reg, 2);
    let mut ledger = FillLedger::new(
        LedgerConfig {
            max_age: Duration::from_secs(3600),
            max_entries: 16,
        },
        WallNs(0),
    )
    .unwrap();
    let f = common::fill(Some(c), common::ident("f"), Side::Buy, 1, false);
    let Admission::Apply(accepted) = ledger.admit(&f, None, MonoNs(7)) else {
        panic!("a fresh fill applies");
    };
    reg.apply_fill(accepted).unwrap();
    assert!(!reg.withdraw(auth));
    let rec = reg.get(c).unwrap();
    assert_eq!(
        (rec.state(), rec.cum_fills()),
        (OrdState::PartiallyFilled, lots(1))
    );
}
