//! A command the pre-trade caps admitted outlives nothing it was judged on (FBC-0d9k; Codex's
//! four P1s on PR #74, decision 0082): 0005's I6 holds when a command is authorized, not only
//! when it is built.
//!
//! - A place, a batch or an amend retained after its build is judged again at authorization
//!   against the registry's position and resting then: a fill that took the position to the
//!   cap since refuses it, and its reservation is released, as it is never sent.
//! - A command records the registry that built it, and a registry is one account's: a command
//!   authorized through another registry, or for another account than the one the registry is
//!   bound to, is refused.
//! - A place or batch item built and not authorized is reported not sent only with its command,
//!   by value ([`Registry::place_not_submitted`]); an outcome naming it by client id that would
//!   end it is refused and releases nothing.
//! - An amend's build token names its registry: one registry's command releases nothing of
//!   another's, though the two hold the same client id at the same build number.
//!
//! The values are the owner's first test values: a $50 inventory cap on a synthetic market
//! where one lot is worth $1, so 50 lots; the tests that rest more than $11 a side set the
//! resting cap past any order (`WIDE`).

#[path = "common/arm.rs"]
mod arm;
mod common;

use std::time::Duration;

use common::{canceled, cid, fill, ident, lots, order_caps, placement, update};
use fbc_core::{
    AccountKey, AckLevel, AmendAck, AmendCaps, AmendQty, Bps, Channel, CidMint, ClientOrderId,
    InstrumentId, ItemRef, Lots, MonoNs, Namespace, NamespaceLease, NewOrder, NotSentReason,
    OrderCaps, OrderKind, RefKind, Reject, RejectKind, Side, SignedLots, SubmitOutcome, TagSet,
    Ticks, Tif, VenueCommand, WallNs,
};
use fbc_oms::{
    CancelChoice, CapRefusal, DesiredBook, DesiredQuote, ExecutionPlanner, FillLedger, FillRouted,
    IssueRefusal, LedgerConfig, MarketCapsConfig, OmsError, OrdState, OrderKey, OrderOp,
    PermittedCommand, PlanError, PlannerConfig, PreTradeCaps, Registry, TerminalKind, TestnetRun,
};

const INST: InstrumentId = InstrumentId::new(1);
const ACCT: AccountKey = AccountKey::new(1);
const OTHER_ACCT: AccountKey = AccountKey::new(2);
/// The owner's first inventory cap, in lots of $1.
const CAP: i64 = 50;
/// A resting cap past any order of these tests.
const WIDE: i64 = 1_000_000;

/// A registry under the inventory cap `CAP`, flat: seeded by hand and started, declared an
/// owner-assisted testnet run (decision 0067), so a venue-initiated fill of no order it holds
/// moves the position ([`position`]).
fn registry() -> Registry {
    let caps = PreTradeCaps::new()
        .with_market(
            INST,
            MarketCapsConfig {
                inventory: Some(lots(CAP)),
                resting: Some(lots(WIDE)),
            },
        )
        .unwrap();
    let mut reg =
        arm::named(Registry::with_caps(caps).for_testnet_run(TestnetRun::owner_assisted()));
    reg.seed_position(INST, SignedLots(0)).unwrap();
    arm::start(&mut reg, INST);
    reg
}

fn ledger() -> FillLedger {
    FillLedger::new(
        LedgerConfig {
            max_age: Duration::from_secs(3600),
            max_entries: 10_000,
        },
        WallNs(0),
    )
    .unwrap()
}

/// Moves the position by a venue-initiated fill of `qty` on `side` under our namespace's client
/// id of no order the registry holds (Codex's r4189335960).
fn position(reg: &mut Registry, l: &mut FillLedger, side: Side, qty: i64, fid: &str) {
    let stray = cid();
    let f = fill(Some(stray), ident(fid), side, qty, false);
    match l.admit(&f, None, MonoNs(0)) {
        fbc_oms::Admission::Apply(a) => {
            assert_eq!(reg.apply_fill(a).unwrap(), FillRouted::OursUntracked(stray));
        }
        other => panic!("expected the fill accepted, got {other:?}"),
    }
}

fn buy(qty: i64) -> NewOrder {
    placement(cid(), 100, qty)
}

fn sell(qty: i64) -> NewOrder {
    NewOrder {
        side: Side::Sell,
        kind: OrderKind::Limit { px: Ticks(101) },
        ..placement(cid(), 101, qty)
    }
}

/// A venue that amends price and total by venue id, keeping the venue id.
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

fn not_sent() -> SubmitOutcome {
    SubmitOutcome::NotSent(NotSentReason::Backpressure)
}

/// Places `order`, authorizes it for `acct` and acknowledges it under the venue id `v`: Open.
fn open(reg: &mut Registry, acct: AccountKey, order: NewOrder, v: &str) -> ClientOrderId {
    let c = order.cid;
    let cmd = reg.place(order).unwrap();
    drop(reg.authorize(acct, cmd).unwrap());
    reg.on_outcome(c, OrderOp::Place, &item(Some(v)), &accepted(), MonoNs(1))
        .unwrap();
    assert_eq!(reg.get(c).unwrap().state(), OrdState::Open);
    c
}

fn amend(reg: &mut Registry, c: ClientOrderId, qty: i64) -> PermittedCommand {
    reg.live(c)
        .unwrap()
        .amend(&amending(), Ticks(100), lots(qty), false)
        .unwrap()
}

fn breach(side: Side, worst: i64) -> IssueRefusal {
    IssueRefusal::Capped(CapRefusal::InventoryCap {
        inst: INST,
        side,
        worst: Some(lots(worst)),
        cap: lots(CAP),
    })
}

fn withdrawn() -> OrdState {
    OrdState::Terminal(TerminalKind::NotSent(NotSentReason::StaleAuthorization))
}

// ---- Codex r4189335960: a retained command is judged again when it is authorized ----

#[test]
fn a_retained_place_is_refused_at_authorization_after_a_fill_took_the_position_to_the_cap() {
    let mut reg = registry();
    let mut l = ledger();
    // From flat, a buy of the whole cap is admitted and built, then retained.
    let order = buy(CAP);
    let c = order.cid;
    let retained = reg.place(order).unwrap();
    // A venue-initiated buy of the cap: the position is +50, and the retained buy would take
    // the worst case to +100.
    position(&mut reg, &mut l, Side::Buy, CAP, "f1");
    assert_eq!(reg.position(INST), Some(SignedLots(CAP)));
    assert_eq!(
        reg.authorize(ACCT, retained).unwrap_err(),
        breach(Side::Buy, 100)
    );
    // Refused, it is never sent: its order ends not sent and no longer counts.
    assert_eq!(reg.get(c).unwrap().state(), withdrawn());
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(Lots::ZERO));
    // What the position leaves room for is still admitted and authorized: a sell of 100
    // takes the worst case to -50.
    let cmd = reg.place(sell(2 * CAP)).unwrap();
    assert!(reg.authorize(ACCT, cmd).is_ok());
}

#[test]
fn a_retained_batch_or_amend_is_refused_at_authorization_once_a_fill_breaches_the_cap() {
    let mut reg = registry();
    let mut l = ledger();
    // A batch of two buys of 20, built from flat: worst case +40.
    let plan = reg.place_batch(vec![buy(20), buy(20)]).unwrap();
    assert!(plan.refused.is_empty());
    let batch = plan.command.unwrap();
    let VenueCommand::PlaceBatch(items) = batch.command() else {
        panic!("a batch")
    };
    let cids: Vec<ClientOrderId> = items.iter().map(|o| o.cid).collect();
    // A buy fill of 11: the batch together would take the worst case to +51.
    position(&mut reg, &mut l, Side::Buy, 11, "f1");
    assert_eq!(
        reg.authorize(ACCT, batch).unwrap_err(),
        breach(Side::Buy, 51)
    );
    for c in cids {
        assert_eq!(reg.get(c).unwrap().state(), withdrawn());
    }
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(Lots::ZERO));

    // An amend from 10 to 30 on an order resting at 10, built at +11: worst case +41.
    let a = open(&mut reg, ACCT, buy(10), "a");
    let retained = amend(&mut reg, a, 30);
    position(&mut reg, &mut l, Side::Buy, 10, "f2");
    assert_eq!(
        reg.authorize(ACCT, retained).unwrap_err(),
        breach(Side::Buy, 51)
    );
    // Its reservation is released; the order rests as it was and may be amended again.
    let rec = reg.get(a).unwrap();
    assert_eq!((rec.state(), rec.amend_built()), (OrdState::Open, None));
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(10)));
    let within = amend(&mut reg, a, 29);
    assert!(reg.authorize(ACCT, within).is_ok());
}

#[test]
fn a_retained_place_is_still_authorized_when_nothing_it_was_judged_on_breaches_the_cap() {
    let mut reg = registry();
    let mut l = ledger();
    let retained = reg.place(buy(CAP)).unwrap();
    // A sell fill moves the position away from the buy's side: the worst case shrinks.
    position(&mut reg, &mut l, Side::Sell, 5, "f1");
    let auth = reg.authorize(ACCT, retained).unwrap();
    assert!(auth.check_at_submit().is_ok());
    // Two places built back to back, each counted when the other was judged: authorized in
    // either order.
    let mut reg = registry();
    let first = reg.place(buy(20)).unwrap();
    let second = reg.place(buy(30)).unwrap();
    assert!(reg.authorize(ACCT, second).is_ok());
    assert!(reg.authorize(ACCT, first).is_ok());
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(50)));
}

#[test]
fn a_retained_place_whose_order_ended_since_is_refused_at_authorization() {
    // An update naming the order ended frees what it counted, so its command is never sent.
    let mut reg = registry();
    let order = buy(5);
    let c = order.cid;
    let retained = reg.place(order).unwrap();
    reg.apply_update(
        &update(Some(c), canceled(), 0),
        OrderKey {
            venue: None,
            ingest: 1,
        },
    );
    assert!(reg.get(c).unwrap().state().is_terminal());
    assert_eq!(
        reg.authorize(ACCT, retained).unwrap_err(),
        IssueRefusal::Released(c)
    );
    // An amend overtaken by a cancel reported sent: its reservation moved to the cancel's
    // watch, so its command is never sent.
    let a = open(&mut reg, ACCT, buy(10), "a");
    let retained = amend(&mut reg, a, 20);
    reg.cancel_sent(a, fbc_core::RpcId(1), MonoNs(2)).unwrap();
    assert_eq!(
        reg.authorize(ACCT, retained).unwrap_err(),
        IssueRefusal::Released(a)
    );
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(20)));
}

// ---- Codex r4189335969: a command is authorized only through its registry, for its account ----

#[test]
fn a_command_from_one_registry_is_refused_when_authorized_for_another_account() {
    let mut reg = registry();
    // The first authorization binds the registry to ACCT.
    open(&mut reg, ACCT, buy(1), "a");
    assert_eq!(reg.account(), Some(ACCT));
    // A place judged against ACCT's position, authorized for another account: refused, and
    // released, since it is never sent.
    let order = buy(CAP - 1);
    let c = order.cid;
    let cmd = reg.place(order).unwrap();
    let refused = reg.authorize(OTHER_ACCT, cmd).unwrap_err();
    assert_eq!(
        refused,
        IssueRefusal::OtherAccount {
            acct: OTHER_ACCT,
            bound: ACCT
        }
    );
    assert!(
        refused.to_string().contains("bound to account"),
        "{refused}"
    );
    assert_eq!(reg.get(c).unwrap().state(), withdrawn());
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(1)));
    // A batch and a cancel are refused for it too.
    let plan = reg.place_batch(vec![buy(1), buy(1)]).unwrap();
    assert!(matches!(
        reg.authorize(OTHER_ACCT, plan.command.unwrap()),
        Err(IssueRefusal::OtherAccount { .. })
    ));
    let resting = open(&mut reg, ACCT, buy(1), "b");
    let CancelChoice::Send(cancel) = reg.cancellable(resting).unwrap().cancel(&amending()) else {
        panic!("a cancel")
    };
    assert!(matches!(
        reg.authorize(OTHER_ACCT, cancel),
        Err(IssueRefusal::OtherAccount { .. })
    ));
    // For its own account it is still authorized.
    let cmd = reg.place(buy(1)).unwrap();
    assert_eq!(reg.authorize(ACCT, cmd).unwrap().account(), ACCT);
}

#[test]
fn a_registry_built_for_an_account_refuses_its_first_authorization_for_another() {
    // Bound at construction, not only at its first authorization.
    let mut reg = registry();
    reg.bind_account(ACCT).unwrap();
    assert_eq!(reg.account(), Some(ACCT));
    let cmd = reg.place(buy(5)).unwrap();
    assert_eq!(
        reg.authorize(OTHER_ACCT, cmd).unwrap_err(),
        IssueRefusal::OtherAccount {
            acct: OTHER_ACCT,
            bound: ACCT
        }
    );
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(Lots::ZERO));
}

#[test]
fn a_command_another_registry_built_is_refused_and_changes_nothing_here() {
    let mut ours = registry();
    let mut theirs = registry();
    let order = buy(CAP);
    let c = order.cid;
    let foreign = theirs.place(order).unwrap();
    let refused = ours.authorize(ACCT, foreign).unwrap_err();
    assert_eq!(refused, IssueRefusal::OtherRegistry);
    assert!(
        refused.to_string().contains("another registry"),
        "{refused}"
    );
    // Nothing here was judged or bound; there, the reservation stays held (fail closed).
    assert_eq!(ours.account(), None);
    assert!(ours.get(c).is_none());
    assert_eq!(theirs.resting_on(INST, Side::Buy), Some(lots(CAP)));
    assert_eq!(theirs.get(c).unwrap().state(), OrdState::PendingNew);
    // Another registry's cancel is refused too.
    let t = open(&mut theirs, ACCT, sell(1), "t");
    let CancelChoice::Send(cancel) = theirs.cancellable(t).unwrap().cancel(&amending()) else {
        panic!("a cancel")
    };
    assert_eq!(
        ours.authorize(ACCT, cancel).unwrap_err(),
        IssueRefusal::OtherRegistry
    );
}

#[test]
fn a_planner_refuses_a_registry_an_authorization_bound_to_another_account() {
    // The registry's binding is the planner's too: a pass for another account is refused
    // before anything is built (decision 0068).
    let mut reg = registry();
    open(&mut reg, OTHER_ACCT, buy(1), "a");
    let lease = NamespaceLease::acquire(&common::lease_dir(), ACCT, Namespace::new(950)).unwrap();
    let mut mint = CidMint::new(lease, 0, 0, WallNs(0));
    let mut p = ExecutionPlanner::new(
        PlannerConfig::new(Bps(2.0), lots(2), Duration::from_millis(100)).unwrap(),
    );
    let book = DesiredBook::new(INST).with(
        Side::Buy,
        0,
        DesiredQuote {
            px: Ticks(100),
            qty: lots(1),
            tif: Tif::Gtc,
            channel: Channel::Public,
            post_only: true,
            reduce_only: false,
            reducing: false,
        },
    );
    assert_eq!(
        p.plan(&book, &mut reg, &amending(), ACCT, &mut mint, MonoNs(0))
            .unwrap_err(),
        PlanError::OtherAccount {
            acct: ACCT,
            bound: OTHER_ACCT
        }
    );
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(1)));
}

// ---- Codex r4189335956: a place not sent is released only with its command ----

#[test]
fn a_places_not_sent_report_takes_its_permitted_command_by_value() {
    let mut reg = registry();
    let order = buy(CAP);
    let c = order.cid;
    let cmd = reg.place(order).unwrap();
    // By client id alone: refused, nothing released, while its command is held.
    for outcome in [
        not_sent(),
        SubmitOutcome::Rejected(Reject {
            kind: RejectKind::InvalidQty,
            venue_code: None,
            raw: "refused".into(),
        }),
    ] {
        assert_eq!(
            reg.on_outcome(c, OrderOp::Place, &item(None), &outcome, MonoNs(1)),
            Err(OmsError::NotIssued(c))
        );
    }
    assert_eq!(reg.get(c).unwrap().state(), OrdState::PendingNew);
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(CAP)));
    assert!(reg.place(buy(1)).is_err());
    // With its command: released, and the command is spent.
    assert!(reg.place_not_submitted(cmd, NotSentReason::Backpressure));
    assert_eq!(
        reg.get(c).unwrap().state(),
        OrdState::Terminal(TerminalKind::NotSent(NotSentReason::Backpressure))
    );
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(Lots::ZERO));
    assert!(reg.place(buy(CAP)).is_ok());
}

#[test]
fn a_batchs_items_not_sent_are_released_only_with_the_batch() {
    let mut reg = registry();
    let plan = reg.place_batch(vec![buy(20), buy(20)]).unwrap();
    let batch = plan.command.unwrap();
    let VenueCommand::PlaceBatch(items) = batch.command() else {
        panic!("a batch")
    };
    let cids: Vec<ClientOrderId> = items.iter().map(|o| o.cid).collect();
    for &c in &cids {
        assert_eq!(
            reg.on_outcome(c, OrderOp::Place, &item(None), &not_sent(), MonoNs(1)),
            Err(OmsError::NotIssued(c))
        );
    }
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(40)));
    assert!(reg.place_not_submitted(batch, NotSentReason::RateBudget));
    for &c in &cids {
        assert!(reg.get(c).unwrap().state().is_terminal());
    }
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(Lots::ZERO));
}

#[test]
fn a_place_handed_to_a_gateway_is_reported_not_sent_by_client_id() {
    // Its command was spent by the authorization the gateway took: the outcome ends it.
    let mut reg = registry();
    let order = buy(5);
    let c = order.cid;
    let cmd = reg.place(order).unwrap();
    drop(reg.authorize(ACCT, cmd).unwrap());
    reg.on_outcome(c, OrderOp::Place, &item(None), &not_sent(), MonoNs(1))
        .unwrap();
    assert!(reg.get(c).unwrap().state().is_terminal());
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(Lots::ZERO));
}

#[test]
fn only_this_registrys_unauthorized_place_is_released_by_place_not_submitted() {
    let mut reg = registry();
    let mut other = registry();
    // Another registry's place, though it holds the same client id here.
    let shared = cid();
    let ours = reg.place(placement(shared, 100, 5)).unwrap();
    let theirs = other.place(placement(shared, 100, 5)).unwrap();
    assert!(!reg.place_not_submitted(theirs, NotSentReason::Backpressure));
    assert_eq!(reg.get(shared).unwrap().state(), OrdState::PendingNew);
    assert_eq!(other.get(shared).unwrap().state(), OrdState::PendingNew);
    // Neither an amend nor a cancel is a place.
    let a = open(&mut reg, ACCT, buy(1), "a");
    let amend_cmd = amend(&mut reg, a, 2);
    assert!(!reg.place_not_submitted(amend_cmd, NotSentReason::Backpressure));
    assert_eq!(reg.get(a).unwrap().amend_built(), Some(lots(2)));
    assert!(reg.place_not_submitted(ours, NotSentReason::Backpressure));
    assert!(reg.get(shared).unwrap().state().is_terminal());
}

// ---- Codex r4189335949: an amend's build token names its registry ----

#[test]
fn one_registrys_amend_command_cannot_release_another_registrys_reservation() {
    // Two accounts' registries may hold the same client id (accounts may lease the same
    // namespace), each at build number 0.
    let mut a = registry();
    let mut b = registry();
    let shared = cid();
    open(&mut a, ACCT, placement(shared, 100, 10), "va");
    open(&mut b, OTHER_ACCT, placement(shared, 100, 10), "vb");
    let in_a = amend(&mut a, shared, 30);
    let in_b = amend(&mut b, shared, 30);
    // B's command releases nothing of A's.
    assert!(!a.amend_not_submitted(in_b));
    assert_eq!(a.get(shared).unwrap().amend_built(), Some(lots(30)));
    assert_eq!(a.resting_on(INST, Side::Buy), Some(lots(30)));
    // A's own does.
    assert!(a.amend_not_submitted(in_a));
    assert_eq!(a.resting_on(INST, Side::Buy), Some(lots(10)));
    // B's reservation is still held: its command was spent without releasing it.
    assert_eq!(b.get(shared).unwrap().amend_built(), Some(lots(30)));
}

#[test]
fn the_refusals_say_what_was_refused() {
    let c = cid();
    assert_eq!(
        breach(Side::Buy, 100).to_string(),
        format!(
            "refused by a pre-trade cap at authorization: {}",
            CapRefusal::InventoryCap {
                inst: INST,
                side: Side::Buy,
                worst: Some(lots(100)),
                cap: lots(CAP),
            }
        )
    );
    assert_eq!(
        IssueRefusal::Released(c).to_string(),
        format!("{c:?} no longer holds what the command's build reserved")
    );
    assert_eq!(
        OmsError::NotIssued(c).to_string(),
        format!("the place of {c:?} was never authorized: it is withdrawn only with its command")
    );
}

#[test]
fn a_registry_bound_to_an_account_is_never_bound_to_another() {
    // Codex P1 r4224412861 on PR #133: bound by its first authorization, a registry bound to
    // another account afterwards would authorize commands judged against the first one's
    // position. Refused, and the binding and the registry stay as they were.
    let mut reg = registry();
    let order = buy(5);
    let c = order.cid;
    let retained = reg.place(order).unwrap();
    open(&mut reg, ACCT, buy(1), "a");
    let refused = reg.bind_account(OTHER_ACCT).unwrap_err();
    assert_eq!(
        refused,
        OmsError::AccountBound {
            acct: OTHER_ACCT,
            bound: ACCT
        }
    );
    assert!(
        refused.to_string().contains("already bound to account"),
        "{refused}"
    );
    assert_eq!(reg.account(), Some(ACCT));
    assert!(matches!(
        reg.authorize(OTHER_ACCT, retained),
        Err(IssueRefusal::OtherAccount { .. })
    ));
    assert!(reg.get(c).unwrap().state().is_terminal());
    // Bound before any authorization, the same: the same account again is accepted.
    let mut reg = registry();
    reg.bind_account(ACCT).unwrap();
    reg.bind_account(ACCT).unwrap();
    assert_eq!(
        reg.bind_account(OTHER_ACCT),
        Err(OmsError::AccountBound {
            acct: OTHER_ACCT,
            bound: ACCT
        })
    );
    assert_eq!(reg.account(), Some(ACCT));
}
