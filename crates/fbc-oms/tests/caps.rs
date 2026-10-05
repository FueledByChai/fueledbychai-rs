//! The inventory cap, decision 0005's I6 (0013 rule 2; the owner's decision A names it the
//! inventory cap): no place, amend, replace or batch item is built whose admission would take
//! `|pos + Σ resting same side + new|` past the cap the consumer configured for its market,
//! reducing and reduce-only ones included. Resting counts PendingNew and Unknown orders fully,
//! a partly filled order's remainder until it is terminal, an amend in flight at the larger
//! of its old and new quantity, and earlier items of the same batch; an order that genuinely
//! reduces the position is admitted by the formula itself; cancels and cancel-many are built
//! whatever the cap's state; and a market with no cap configured admits nothing.
//!
//! The values are the owner's first test values: an inventory cap of $50 and one $11 L0 order
//! per side, on a synthetic market where one lot is worth $1 at the test price, so 50 and 11
//! lots.

mod common;

use std::time::Duration;

use common::{canceled, cid, fill, ident, lots, order_caps, placement, update, vid};
use fbc_core::{
    AckLevel, AmendAck, AmendCaps, AmendQty, CancelBatch, ClientOrderId, FillIdent, InstrumentId,
    ItemRef, Lots, MonoNs, NewOrder, OrderCaps, OrderKind, RefKind, RpcId, Side, SignedLots,
    SubmitOutcome, TagSet, Ticks, VenueCommand, VenueOrderState, WallNs,
};
use fbc_oms::{
    AmendRefusal, CancelChoice, CapRefusal, FillLedger, FillRouted, LedgerConfig, MarketCaps,
    OmsError, OrdState, OrderKey, OrderOp, PermittedCommand, PreTradeCaps, Registry,
};
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, RngSeed};

const INST: InstrumentId = InstrumentId::new(1);
const OTHER: InstrumentId = InstrumentId::new(2);
/// The owner's first test values, in lots of $1: a $50 inventory cap and an $11 L0 order.
const CAP: i64 = 50;
const L0: i64 = 11;

fn caps(cap: i64) -> PreTradeCaps {
    PreTradeCaps::new().with_market(
        INST,
        MarketCaps {
            inventory: lots(cap),
        },
    )
}

/// A registry under the inventory cap `cap`, flat: its position seeded from the venue as 0.
fn registry(cap: i64) -> Registry {
    let mut reg = Registry::with_caps(caps(cap));
    reg.seed_position(INST, SignedLots(0)).unwrap();
    reg
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

fn reduce_only(order: NewOrder) -> NewOrder {
    NewOrder {
        reduce_only: true,
        reducing: true,
        ..order
    }
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

/// Applies a live fill of `qty` on `side` for `cid` (an order of ours, or our namespace's
/// client id of none the registry holds, which moves the inventory only), named `fid`.
fn fill_of(
    reg: &mut Registry,
    l: &mut FillLedger,
    cid: ClientOrderId,
    side: Side,
    qty: i64,
    fid: &str,
) -> FillRouted {
    let f = fill(Some(cid), ident(fid), side, qty, false);
    match l.admit(&f, None, MonoNs(0)) {
        fbc_oms::Admission::Apply(a) => reg.apply_fill(a).unwrap(),
        other => panic!("expected the fill accepted, got {other:?}"),
    }
}

/// Moves the position by a venue-initiated fill of no order the registry holds.
fn position(reg: &mut Registry, l: &mut FillLedger, side: Side, qty: i64, fid: &str) {
    let stray = cid();
    assert_eq!(
        fill_of(reg, l, stray, side, qty, fid),
        FillRouted::OursUntracked(stray)
    );
}

fn outcome(reg: &mut Registry, c: ClientOrderId, op: OrderOp, o: SubmitOutcome, v: Option<&str>) {
    let item = ItemRef {
        idx: 0,
        cid: None,
        vid: v.map(vid),
    };
    reg.on_outcome(c, op, &item, &o, MonoNs(1)).unwrap();
}

fn accepted() -> SubmitOutcome {
    SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    }
}

/// Places `order` and acknowledges it under the venue id `v`: Open.
fn open(reg: &mut Registry, order: NewOrder, v: &str) -> ClientOrderId {
    let c = order.cid;
    reg.place(order).unwrap();
    outcome(reg, c, OrderOp::Place, accepted(), Some(v));
    assert_eq!(reg.get(c).unwrap().state(), OrdState::Open);
    c
}

fn breach(side: Side, worst: Option<i64>, cap: i64) -> CapRefusal {
    CapRefusal::InventoryCap {
        inst: INST,
        side,
        worst: worst.map(lots),
        cap: lots(cap),
    }
}

fn capped(side: Side, worst: i64) -> OmsError {
    OmsError::Capped(breach(side, Some(worst), CAP))
}

/// A venue that amends price and quantity, by venue id; `keeps_venue_id` false is a replace.
fn amending(keeps_venue_id: bool) -> OrderCaps {
    OrderCaps {
        amend: Some(AmendCaps {
            refs: TagSet::of(&[RefKind::Venue]),
            price: true,
            qty: true,
            flags: false,
            when_partially_filled: true,
            reject_keeps_original: true,
            keeps_venue_id,
            ack: AmendAck::ReplacedEvent,
            qty_semantics: AmendQty::TotalIncludingFilled,
            keeps_priority: None,
        }),
        ..order_caps()
    }
}

fn amend(
    reg: &mut Registry,
    caps: &OrderCaps,
    c: ClientOrderId,
    qty: i64,
    reducing: bool,
) -> Result<PermittedCommand, AmendRefusal> {
    reg.live(c)
        .unwrap()
        .amend(caps, Ticks(100), lots(qty), reducing)
}

// ---- places ----

#[test]
fn a_place_whose_admission_would_breach_the_inventory_cap_is_never_built_reduce_only_included() {
    let mut reg = registry(CAP);
    // Four $11 bids rest: 44 of the 50.
    for _ in 0..4 {
        let order = buy(L0);
        let built = reg.place(order.clone()).unwrap();
        assert_eq!(built.command(), &VenueCommand::Place(order.clone()));
        assert_eq!(reg.get(order.cid).unwrap().state(), OrdState::PendingNew);
    }
    // A fifth would take the worst case to 55: refused, never built, never registered, the
    // venue's reduce-only flag and the OMS's reducing class making no difference.
    for order in [buy(L0), reduce_only(buy(L0))] {
        let c = order.cid;
        assert_eq!(reg.place(order), Err(capped(Side::Buy, 55)));
        assert!(reg.get(c).is_none());
    }
    assert_eq!(reg.len(), 4);
    // Six more fit exactly; seven do not.
    assert_eq!(reg.place(buy(7)), Err(capped(Side::Buy, 51)));
    reg.place(buy(6)).unwrap();
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(CAP)));
    // The other side has its own worst case: 50 lots of offers rest against no position.
    for _ in 0..4 {
        reg.place(sell(L0)).unwrap();
    }
    assert_eq!(reg.place(sell(7)), Err(capped(Side::Sell, 51)));
    assert_eq!(reg.place(reduce_only(sell(7))), Err(capped(Side::Sell, 51)));
    reg.place(sell(6)).unwrap();
    assert_eq!(reg.resting_on(INST, Side::Sell), Some(lots(CAP)));
    // Another market's orders count only on their own market.
    assert_eq!(reg.resting_on(OTHER, Side::Buy), Some(Lots::ZERO));
}

#[test]
fn pending_new_and_unknown_orders_count_as_fully_resting() {
    let mut reg = registry(CAP);
    let a = buy(30);
    let a_cid = a.cid;
    reg.place(a).unwrap();
    // PendingNew: all 30 count.
    assert_eq!(reg.place(buy(21)), Err(capped(Side::Buy, 51)));
    // Unanswered by its deadline: Unknown, still all 30.
    outcome(
        &mut reg,
        a_cid,
        OrderOp::Place,
        SubmitOutcome::Unknown,
        None,
    );
    assert_eq!(reg.get(a_cid).unwrap().state(), OrdState::Unknown);
    assert_eq!(reg.place(buy(21)), Err(capped(Side::Buy, 51)));
    reg.place(buy(20)).unwrap();
}

#[test]
fn a_partly_filled_orders_remainder_counts_until_it_is_terminal() {
    let mut reg = registry(CAP);
    let mut l = ledger();
    let a = open(&mut reg, buy(30), "a");
    // Ten fill: the position is 10, the remainder 20, the worst case 30.
    assert!(matches!(
        fill_of(&mut reg, &mut l, a, Side::Buy, 10, "f1"),
        FillRouted::Ours(..)
    ));
    assert_eq!(reg.get(a).unwrap().state(), OrdState::PartiallyFilled);
    assert_eq!(reg.inventory(INST), SignedLots(10));
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(20)));
    assert_eq!(reg.place(buy(21)), Err(capped(Side::Buy, 51)));
    // Cancelled: nothing of it rests, and the position alone counts.
    let mut done = update(Some(a), canceled(), 10);
    done.vid = Some(vid("a"));
    reg.apply_update(
        &done,
        OrderKey {
            venue: None,
            ingest: 1,
        },
    );
    assert!(reg.get(a).unwrap().state().is_terminal());
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(Lots::ZERO));
    assert_eq!(reg.place(buy(41)), Err(capped(Side::Buy, 51)));
    reg.place(buy(40)).unwrap();
}

#[test]
fn a_fill_the_venue_reported_counts_until_its_fill_event_moves_the_inventory() {
    let mut reg = registry(CAP);
    let mut l = ledger();
    let a = open(&mut reg, buy(50), "a");
    // The order update reports all 50 filled before the fill event arrives: nothing rests,
    // but the inventory does not hold the 50 yet, so they still count.
    let mut filled = update(Some(a), VenueOrderState::Filled, 50);
    filled.vid = Some(vid("a"));
    reg.apply_update(
        &filled,
        OrderKey {
            venue: None,
            ingest: 1,
        },
    );
    assert!(reg.get(a).unwrap().state().is_terminal());
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(Lots::ZERO));
    assert_eq!(reg.get(a).unwrap().exposure(), lots(50));
    assert_eq!(reg.place(buy(1)), Err(capped(Side::Buy, 51)));
    // Part reported on a live order counts the same way.
    let mut reg2 = registry(CAP);
    let b = open(&mut reg2, buy(30), "b");
    let mut part = update(Some(b), VenueOrderState::Open, 20);
    part.vid = Some(vid("b"));
    reg2.apply_update(
        &part,
        OrderKey {
            venue: None,
            ingest: 1,
        },
    );
    assert_eq!(reg2.resting_on(INST, Side::Buy), Some(lots(10)));
    assert_eq!(reg2.place(buy(21)), Err(capped(Side::Buy, 51)));
    // The fill event arrives: the inventory holds the 50, and the order no longer adds them.
    fill_of(&mut reg, &mut l, a, Side::Buy, 50, "fa");
    assert_eq!(reg.inventory(INST), SignedLots(50));
    assert_eq!(reg.get(a).unwrap().exposure(), Lots::ZERO);
    assert_eq!(reg.place(buy(1)), Err(capped(Side::Buy, 51)));
    reg.place(sell(L0)).unwrap();
}

#[test]
fn no_amend_is_built_on_a_venue_whose_amend_states_the_remaining_quantity() {
    let mut reg = registry(CAP);
    let a = open(&mut reg, buy(20), "a");
    let mut remaining = amending(true);
    if let Some(caps) = remaining.amend.as_mut() {
        caps.qty_semantics = AmendQty::Remaining;
    }
    // What it may rest once fills arrive while it is on its way is not modelled (FBC-b0z9).
    for qty in [20, 30, 10] {
        assert_eq!(
            amend(&mut reg, &remaining, a, qty, false),
            Err(AmendRefusal::RemainingQty)
        );
    }
    assert_eq!(reg.get(a).unwrap().amend_built(), None);
}

// ---- amends and replaces ----

#[test]
fn an_amend_or_a_replace_whose_admission_would_breach_the_inventory_cap_is_never_built() {
    for keeps_venue_id in [true, false] {
        let venue = amending(keeps_venue_id);
        let mut reg = registry(CAP);
        let a = open(&mut reg, buy(20), "a");
        reg.place(buy(20)).unwrap();
        // 20 more than the other 20 rest is 40; 31 would make 51: refused, the reducing
        // class included, and nothing is in flight on the order.
        for reducing in [false, true] {
            assert_eq!(
                amend(&mut reg, &venue, a, 31, reducing),
                Err(AmendRefusal::Capped(breach(Side::Buy, Some(51), CAP)))
            );
        }
        assert_eq!(reg.get(a).unwrap().intent(), fbc_oms::Intent::None);
        let built = amend(&mut reg, &venue, a, 30, false).unwrap();
        assert!(matches!(built.command(), VenueCommand::Amend(am) if am.qty == lots(30)));
    }
    // A reduce-only offer growing against a long position: the formula bounds it too.
    let mut reg = registry(CAP);
    let mut l = ledger();
    position(&mut reg, &mut l, Side::Buy, 50, "p");
    let s = open(&mut reg, reduce_only(sell(60)), "s");
    let venue = amending(true);
    assert_eq!(
        amend(&mut reg, &venue, s, 101, true),
        Err(AmendRefusal::Capped(breach(Side::Sell, Some(51), CAP)))
    );
    amend(&mut reg, &venue, s, 100, true).unwrap();
}

#[test]
fn an_amend_in_flight_counts_at_the_larger_of_its_old_and_new_quantity() {
    let venue = amending(true);
    let mut reg = registry(CAP);
    // Growing 20 to 30: the 30 counts while it is in flight.
    let a = open(&mut reg, buy(20), "a");
    amend(&mut reg, &venue, a, 30, false).unwrap();
    reg.amend_sent(a, Ticks(100), lots(30), RpcId(1), MonoNs(2))
        .unwrap();
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(30)));
    assert_eq!(reg.place(buy(21)), Err(capped(Side::Buy, 51)));
    // Shrinking 30 to 10 on another order: the 30 still count until it is acknowledged.
    let mut reg = registry(CAP);
    let b = open(&mut reg, buy(30), "b");
    amend(&mut reg, &venue, b, 10, false).unwrap();
    reg.amend_sent(b, Ticks(100), lots(10), RpcId(2), MonoNs(2))
        .unwrap();
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(30)));
    assert_eq!(reg.place(buy(21)), Err(capped(Side::Buy, 51)));
    // Acknowledged at 10: only 10 count.
    let mut confirmed = update(Some(b), VenueOrderState::Open, 0);
    confirmed.vid = Some(vid("b"));
    confirmed.px = Some(Ticks(100));
    confirmed.qty = Some(lots(10));
    reg.apply_update(
        &confirmed,
        OrderKey {
            venue: Some(5),
            ingest: 2,
        },
    );
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(10)));
    reg.place(buy(40)).unwrap();
}

#[test]
fn an_amend_is_judged_at_the_larger_of_its_old_and_new_quantity_so_shrinking_past_the_cap_is_not_admitted()
 {
    let venue = amending(true);
    let mut reg = registry(CAP);
    let mut l = ledger();
    let a = open(&mut reg, buy(30), "a");
    // Fills elsewhere take the buy side's worst case to 60, past the cap.
    position(&mut reg, &mut l, Side::Buy, 30, "p");
    // Shrinking to 10 leaves the 30 able to fill until the venue acknowledges it: the worst
    // case stays 60, so the amend is not built; a cancel, never capped, is.
    assert_eq!(
        amend(&mut reg, &venue, a, 10, true),
        Err(AmendRefusal::Capped(breach(Side::Buy, Some(60), CAP)))
    );
    assert!(matches!(
        reg.cancellable(a).unwrap().cancel(&venue),
        CancelChoice::Send(_)
    ));
    // Within the cap, a shrinking amend is built, still judged at its old 30.
    let mut reg = registry(CAP);
    let b = open(&mut reg, buy(30), "b");
    reg.place(buy(20)).unwrap();
    amend(&mut reg, &venue, b, 10, false).unwrap();
}

#[test]
fn an_amend_counts_from_when_it_is_built_so_two_built_before_either_is_sent_cannot_together_breach()
{
    let venue = amending(true);
    let mut reg = registry(CAP);
    let a = open(&mut reg, buy(20), "a");
    let b = open(&mut reg, buy(20), "b");
    // Built, not yet reported sent: its 30 count at once.
    let built_a = amend(&mut reg, &venue, a, 30, false).unwrap();
    assert_eq!(reg.get(a).unwrap().amend_built(), Some(lots(30)));
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(50)));
    assert_eq!(
        amend(&mut reg, &venue, b, 30, false),
        Err(AmendRefusal::Capped(breach(Side::Buy, Some(60), CAP)))
    );
    assert_eq!(reg.place(buy(1)), Err(capped(Side::Buy, 51)));
    // Nothing more is built on it until it is reported sent or withdrawn.
    assert_eq!(
        reg.live(a).unwrap_err(),
        fbc_oms::PermitRefusal::IntentPending(a)
    );
    // Never handed to a gateway: its command, withdrawn, is spent and no longer counts.
    assert!(reg.amend_not_submitted(built_a));
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(40)));
    let first_b = amend(&mut reg, &venue, b, 30, false).unwrap();
    // Reported sent, the amend built is the one in flight.
    reg.amend_sent(b, Ticks(100), lots(30), RpcId(1), MonoNs(2))
        .unwrap();
    let rec = reg.get(b).unwrap();
    assert_eq!(rec.amend_built(), None);
    assert!(matches!(rec.intent(), fbc_oms::Intent::PendingAmend { qty, .. } if qty == lots(30)));
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(50)));
    // Refused by the venue, it is resolved; a second amend is built.
    let refused = SubmitOutcome::Rejected(fbc_core::Reject {
        kind: fbc_core::RejectKind::InvalidQty,
        venue_code: None,
        raw: "refused".into(),
    });
    outcome(&mut reg, b, OrderOp::Amend(RpcId(1)), refused, None);
    let second_b = amend(&mut reg, &venue, b, 25, false).unwrap();
    // Only the command of the build not yet reported releases it: not the first, already
    // sent, nor a cancel, nor another registry's amend.
    assert!(!reg.amend_not_submitted(first_b));
    let CancelChoice::Send(cancel) = reg.cancellable(a).unwrap().cancel(&venue) else {
        panic!("a cancel")
    };
    assert!(!reg.amend_not_submitted(cancel));
    let mut other = registry(CAP);
    let o = open(&mut other, buy(1), "o");
    let foreign = amend(&mut other, &venue, o, 2, false).unwrap();
    assert!(!reg.amend_not_submitted(foreign));
    assert_eq!(reg.get(b).unwrap().amend_built(), Some(lots(25)));
    assert!(reg.amend_not_submitted(second_b));
    assert_eq!(reg.get(b).unwrap().amend_built(), None);
}

#[test]
fn an_amend_built_and_overtaken_by_another_command_still_counts() {
    let venue = amending(true);
    // A cancel reported sent over an amend built and never reported: the amend may have
    // reached the venue, so its 30 still count.
    let mut reg = registry(CAP);
    let a = open(&mut reg, buy(20), "a");
    amend(&mut reg, &venue, a, 30, false).unwrap();
    reg.cancel_sent(a, RpcId(1), MonoNs(2)).unwrap();
    assert_eq!(reg.get(a).unwrap().amend_built(), None);
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(30)));
    assert_eq!(reg.place(buy(21)), Err(capped(Side::Buy, 51)));
    // Another amend reported sent than the one built: both count.
    let mut reg = registry(CAP);
    let b = open(&mut reg, buy(20), "b");
    amend(&mut reg, &venue, b, 30, false).unwrap();
    reg.amend_sent(b, Ticks(100), lots(25), RpcId(2), MonoNs(2))
        .unwrap();
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(30)));
    // The order ending releases everything.
    let mut done = update(Some(b), canceled(), 0);
    done.vid = Some(vid("b"));
    reg.apply_update(
        &done,
        OrderKey {
            venue: None,
            ingest: 9,
        },
    );
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(Lots::ZERO));
}

// ---- batches ----

#[test]
fn ten_batch_items_each_under_the_cap_are_refused_once_together_they_breach_it() {
    let mut reg = registry(CAP);
    let items: Vec<NewOrder> = (0..10).map(|_| buy(L0)).collect();
    let cids: Vec<ClientOrderId> = items.iter().map(|o| o.cid).collect();
    let plan = reg.place_batch(items.clone()).unwrap();
    // The first four make 44; each later one would make 55 with the earlier ones counted as
    // PendingNew, so it is refused and never built.
    let built = plan.command.expect("the first four are built");
    assert_eq!(
        built.command(),
        &VenueCommand::PlaceBatch(items[..4].to_vec())
    );
    assert_eq!(
        plan.refused,
        cids[4..]
            .iter()
            .map(|c| (*c, capped(Side::Buy, 55)))
            .collect::<Vec<_>>()
    );
    for c in &cids[..4] {
        assert_eq!(reg.get(*c).unwrap().state(), OrdState::PendingNew);
    }
    for c in &cids[4..] {
        assert!(reg.get(*c).is_none());
    }
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(44)));
    // A batch a placed order already crowds: none of it fits.
    let plan = reg
        .place_batch(vec![buy(L0), reduce_only(buy(L0))])
        .unwrap();
    assert!(plan.command.is_none());
    assert_eq!(plan.refused.len(), 2);
    // A later smaller item still fits after a refused larger one.
    let small = buy(6);
    let plan = reg.place_batch(vec![buy(7), small.clone()]).unwrap();
    assert_eq!(
        plan.command.unwrap().command(),
        &VenueCommand::PlaceBatch(vec![small])
    );
}

#[test]
fn a_batch_over_several_markets_or_repeating_a_client_id_is_refused_as_built() {
    let mut reg = Registry::with_caps(caps(CAP).with_market(
        OTHER,
        MarketCaps {
            inventory: lots(CAP),
        },
    ));
    for market in [INST, OTHER] {
        reg.seed_position(market, SignedLots(0)).unwrap();
    }
    let mut eth = buy(1);
    eth.inst = OTHER;
    let btc = buy(1);
    let cids = [btc.cid, eth.cid];
    assert_eq!(
        reg.place_batch(vec![btc, eth]).err(),
        Some(OmsError::MixedMarkets)
    );
    assert!(cids.iter().all(|c| reg.get(*c).is_none()));
    // An empty batch builds nothing.
    let empty = reg.place_batch(vec![]).unwrap();
    assert!(empty.command.is_none() && empty.refused.is_empty());
    // A client id twice: the second is refused, as is one already registered.
    let once = buy(1);
    let plan = reg.place_batch(vec![once.clone(), once.clone()]).unwrap();
    assert_eq!(
        plan.command.unwrap().command(),
        &VenueCommand::PlaceBatch(vec![once.clone()])
    );
    assert_eq!(
        plan.refused,
        vec![(once.cid, OmsError::DuplicateCid(once.cid))]
    );
    assert_eq!(
        reg.place(once.clone()).err(),
        Some(OmsError::DuplicateCid(once.cid))
    );
}

// ---- reducing orders ----

#[test]
fn an_order_that_genuinely_reduces_the_position_is_admitted_by_the_formula() {
    let mut reg = registry(CAP);
    let mut l = ledger();
    // Fills took the position past the cap, to 60 long.
    position(&mut reg, &mut l, Side::Buy, 60, "p");
    // Nothing that adds is built, whatever it is called.
    assert_eq!(reg.place(buy(1)), Err(capped(Side::Buy, 61)));
    let mislabelled = NewOrder {
        reducing: true,
        ..buy(1)
    };
    assert_eq!(reg.place(mislabelled), Err(capped(Side::Buy, 61)));
    // An offer that reduces it is admitted by the formula, |60 - 30| = 30, without any flag.
    reg.place(sell(30)).unwrap();
    // The reducing side may rest past the position, until the worst case on the far side of
    // zero reaches the cap: |60 - 30 - 80| = 50.
    reg.place(sell(80)).unwrap();
    assert_eq!(reg.place(sell(1)), Err(capped(Side::Sell, 51)));
    assert_eq!(reg.place(reduce_only(sell(1))), Err(capped(Side::Sell, 51)));
}

// ---- cancels ----

#[test]
fn cancels_and_cancel_many_are_built_whatever_the_caps_state() {
    let mut reg = registry(CAP);
    let mut l = ledger();
    let a = open(&mut reg, buy(30), "a");
    let b = open(&mut reg, buy(20), "b");
    // A fill past the cap: the buy side's worst case is 150, far over.
    position(&mut reg, &mut l, Side::Buy, 100, "p");
    assert_eq!(reg.place(buy(1)), Err(capped(Side::Buy, 151)));
    let caps = OrderCaps {
        batch_cancel: Some(CancelBatch {
            max_items: 10,
            refs: TagSet::of(&[RefKind::Venue]),
        }),
        ..order_caps()
    };
    assert!(matches!(
        reg.cancellable(a).unwrap().cancel(&caps),
        CancelChoice::Send(_)
    ));
    let plan = reg.cancel_many(&[a, b], &caps);
    assert_eq!(plan.commands.len(), 1);
    assert!(plan.refused.is_empty() && plan.awaiting_ack.is_empty());
    // With no cap configured at all, cancels are built too.
    let mut bare = Registry::new();
    let c = bare.insert(buy(5)).unwrap().cid();
    outcome(&mut bare, c, OrderOp::Place, accepted(), Some("c"));
    assert!(matches!(
        bare.cancellable(c).unwrap().cancel(&caps),
        CancelChoice::Send(_)
    ));
    assert_eq!(bare.cancel_many(&[c], &caps).commands.len(), 1);
}

// ---- configuration and arithmetic ----

#[test]
fn a_market_without_a_configured_cap_admits_nothing() {
    // No configuration at all, and a configuration naming another market only.
    for mut reg in [
        Registry::new(),
        Registry::with_caps(PreTradeCaps::new().with_market(
            OTHER,
            MarketCaps {
                inventory: lots(CAP),
            },
        )),
    ] {
        let order = buy(1);
        let c = order.cid;
        assert_eq!(
            reg.place(order),
            Err(OmsError::Capped(CapRefusal::NoCap(INST)))
        );
        assert!(reg.get(c).is_none());
        let plan = reg.place_batch(vec![buy(1)]).unwrap();
        assert!(plan.command.is_none());
        assert_eq!(plan.refused[0].1, OmsError::Capped(CapRefusal::NoCap(INST)));
        // An order registered without a command (as a resync might) is never amended.
        let a = reg.insert(buy(1)).unwrap().cid();
        outcome(&mut reg, a, OrderOp::Place, accepted(), Some("a"));
        assert_eq!(
            amend(&mut reg, &amending(true), a, 2, false),
            Err(AmendRefusal::Capped(CapRefusal::NoCap(INST)))
        );
    }
    assert_eq!(
        caps(CAP).market(INST),
        Some(MarketCaps {
            inventory: lots(CAP)
        })
    );
    assert_eq!(caps(CAP).market(OTHER), None);
}

#[test]
fn a_market_admits_nothing_until_its_position_is_seeded_from_the_venue() {
    // After a restart the account may already hold a position: until the venue's is seeded,
    // the worst case is unknown and nothing is built.
    let mut reg = Registry::with_caps(caps(CAP));
    assert!(!reg.position_known(INST));
    let unknown = OmsError::Capped(CapRefusal::PositionUnknown(INST));
    assert_eq!(reg.place(buy(1)), Err(unknown.clone()));
    assert_eq!(reg.place_batch(vec![buy(1)]).unwrap().refused[0].1, unknown);
    let a = reg.insert(buy(1)).unwrap().cid();
    outcome(&mut reg, a, OrderOp::Place, accepted(), Some("a"));
    assert_eq!(
        amend(&mut reg, &amending(true), a, 2, false),
        Err(AmendRefusal::Capped(CapRefusal::PositionUnknown(INST)))
    );
    // Seeded long 50 under a cap of 50: no more bids, though offers reduce it.
    reg.seed_position(INST, SignedLots(50)).unwrap();
    assert!(reg.position_known(INST));
    assert_eq!(reg.inventory(INST), SignedLots(50));
    assert_eq!(reg.place(buy(1)), Err(capped(Side::Buy, 52)));
    reg.place(sell(L0)).unwrap();
    // Seeded once only.
    assert_eq!(
        reg.seed_position(INST, SignedLots(0)),
        Err(OmsError::PositionSeeded(INST))
    );
    // A fill before the seed: how it counts against the snapshot is the resync's (FBC-38r),
    // so the seed is refused and the market stays unknown.
    let mut reg = Registry::with_caps(caps(CAP));
    let mut l = ledger();
    position(&mut reg, &mut l, Side::Buy, 5, "early");
    assert_eq!(
        reg.seed_position(INST, SignedLots(5)),
        Err(OmsError::PositionMoved(INST))
    );
    assert!(!reg.position_known(INST));
    assert_eq!(reg.place(sell(1)), Err(unknown));
}

#[test]
fn a_worst_case_that_does_not_fit_is_refused() {
    let max = i64::MAX;
    // Resting that would overflow.
    let mut reg = registry(max);
    reg.place(buy(max)).unwrap();
    assert_eq!(
        reg.place(buy(1)),
        Err(OmsError::Capped(breach(Side::Buy, None, max)))
    );
    // The sum over orders already resting overflows (orders registered without a command).
    reg.insert(buy(max)).unwrap();
    assert_eq!(reg.resting_on(INST, Side::Buy), None);
    assert_eq!(
        reg.place(buy(1)),
        Err(OmsError::Capped(breach(Side::Buy, None, max)))
    );
    // The position and the order together overflow.
    let mut reg = registry(max);
    let mut l = ledger();
    position(&mut reg, &mut l, Side::Buy, max, "p");
    assert_eq!(
        reg.place(buy(1)),
        Err(OmsError::Capped(breach(Side::Buy, None, max)))
    );
    // A short position whose worst case is i64::MIN has no magnitude a cap can hold.
    let mut reg = registry(max);
    let mut l = ledger();
    position(&mut reg, &mut l, Side::Sell, max, "p");
    assert_eq!(
        reg.place(sell(1)),
        Err(OmsError::Capped(breach(Side::Sell, None, max)))
    );
}

#[test]
fn the_refusals_say_what_was_refused() {
    let inst = format!("{INST:?}");
    let no_cap = OmsError::Capped(CapRefusal::NoCap(INST)).to_string();
    assert!(
        no_cap.contains(&inst) && no_cap.contains("no inventory cap"),
        "{no_cap}"
    );
    let over = OmsError::Capped(breach(Side::Buy, Some(51), CAP)).to_string();
    assert!(
        over.contains("51") && over.contains("50") && over.contains("Buy"),
        "{over}"
    );
    let overflow = breach(Side::Sell, None, CAP).to_string();
    assert!(overflow.contains("does not fit"), "{overflow}");
    assert!(OmsError::MixedMarkets.to_string().contains("market"));
    let unknown = OmsError::Capped(CapRefusal::PositionUnknown(INST)).to_string();
    assert!(
        unknown.contains("not seeded") && unknown.contains(&inst),
        "{unknown}"
    );
    assert!(
        OmsError::PositionSeeded(INST)
            .to_string()
            .contains("already seeded")
    );
    assert!(
        OmsError::PositionMoved(INST)
            .to_string()
            .contains("before its position was seeded")
    );
    let err: &dyn std::error::Error = &breach(Side::Buy, Some(51), CAP);
    assert!(err.source().is_none());
}

// ---- I6 over generated sequences ----

fn config(cases: u32, seed: u64) -> ProptestConfig {
    ProptestConfig {
        cases,
        rng_seed: RngSeed::Fixed(seed),
        failure_persistence: None,
        ..ProptestConfig::default()
    }
}

#[derive(Clone, Debug)]
enum Op {
    Place(Side, i64),
    Batch(Vec<(Side, i64)>),
    Ack(usize),
    Unknown(usize),
    Fill(usize, i64),
    Cancelled(usize),
    Amend(usize, i64),
    Venue(Side, i64),
}

fn side() -> impl Strategy<Value = Side> {
    prop_oneof![Just(Side::Buy), Just(Side::Sell)]
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        3 => (side(), 1..30i64).prop_map(|(s, q)| Op::Place(s, q)),
        1 => proptest::collection::vec((side(), 1..15i64), 0..10).prop_map(Op::Batch),
        2 => any::<usize>().prop_map(Op::Ack),
        1 => any::<usize>().prop_map(Op::Unknown),
        2 => (any::<usize>(), 1..20i64).prop_map(|(i, q)| Op::Fill(i, q)),
        1 => any::<usize>().prop_map(Op::Cancelled),
        2 => (any::<usize>(), 1..40i64).prop_map(|(i, q)| Op::Amend(i, q)),
        1 => (side(), 1..40i64).prop_map(|(s, q)| Op::Venue(s, q)),
    ]
}

/// Every tracked order's resting quantity on `side`, from the registry's public state.
fn resting(reg: &Registry, tracked: &[ClientOrderId], side: Side) -> i64 {
    tracked
        .iter()
        .filter_map(|c| reg.get(*c))
        .filter(|r| r.placed().side == side)
        .map(|r| r.resting().get())
        .sum()
}

/// The worst case on `side` the registry's public state shows: the position plus every
/// tracked order's resting quantity on that side, plus `new`.
fn worst(reg: &Registry, tracked: &[ClientOrderId], side: Side, new: i64) -> i64 {
    (reg.inventory(INST).0 + side.sign() * (resting(reg, tracked, side) + new)).abs()
}

fn order(side: Side, qty: i64) -> NewOrder {
    match side {
        Side::Buy => buy(qty),
        Side::Sell => sell(qty),
    }
}

proptest! {
    #![proptest_config(config(1024, 0x0005_0006))]

    /// I6: after every place, batch item and amend the OMS built, the worst case on its side
    /// is within the cap; every place or batch item refused would have taken it past.
    #[test]
    fn i6_nothing_built_takes_the_worst_case_past_the_inventory_cap(
        ops in proptest::collection::vec(op(), 1..60)
    ) {
        let mut reg = registry(CAP);
        let mut l = ledger();
        let venue = amending(true);
        let mut tracked: Vec<ClientOrderId> = Vec::new();
        let pick = |tracked: &[ClientOrderId], i: usize| {
            (!tracked.is_empty()).then(|| tracked[i % tracked.len()])
        };
        for (n, op) in ops.into_iter().enumerate() {
            match op {
                Op::Place(s, q) => {
                    let o = order(s, q);
                    let before = worst(&reg, &tracked, s, q);
                    match reg.place(o.clone()) {
                        Ok(_) => {
                            tracked.push(o.cid);
                            prop_assert!(worst(&reg, &tracked, s, 0) <= CAP);
                        }
                        Err(e) => {
                            prop_assert_eq!(e, capped(s, before));
                            prop_assert!(before > CAP);
                        }
                    }
                }
                Op::Batch(items) => {
                    let orders: Vec<NewOrder> =
                        items.iter().map(|(s, q)| order(*s, *q)).collect();
                    // Each item is judged with the earlier ones built counted as resting.
                    let inv = reg.inventory(INST).0;
                    let mut rest = [Side::Buy, Side::Sell].map(|s| resting(&reg, &tracked, s));
                    let plan = reg.place_batch(orders.clone()).unwrap();
                    let built: Vec<NewOrder> = match plan.command.as_ref().map(|c| c.command()) {
                        Some(VenueCommand::PlaceBatch(b)) => b.clone(),
                        None => vec![],
                        Some(other) => panic!("{other:?}"),
                    };
                    prop_assert_eq!(built.len() + plan.refused.len(), orders.len());
                    for o in &orders {
                        let i = usize::from(o.side == Side::Sell);
                        let w = (inv + o.side.sign() * (rest[i] + o.qty.get())).abs();
                        if built.iter().any(|b| b.cid == o.cid) {
                            prop_assert!(w <= CAP);
                            rest[i] += o.qty.get();
                            tracked.push(o.cid);
                        } else {
                            prop_assert!(w > CAP);
                        }
                    }
                    for (i, s) in [Side::Buy, Side::Sell].into_iter().enumerate() {
                        prop_assert_eq!(resting(&reg, &tracked, s), rest[i]);
                    }
                }
                Op::Ack(i) => if let Some(c) = pick(&tracked, i)
                    && reg.get(c).unwrap().state() == OrdState::PendingNew
                {
                    outcome(&mut reg, c, OrderOp::Place, accepted(), None);
                },
                Op::Unknown(i) => if let Some(c) = pick(&tracked, i)
                    && reg.get(c).unwrap().state() == OrdState::PendingNew
                {
                    outcome(&mut reg, c, OrderOp::Place, SubmitOutcome::Unknown, None);
                },
                Op::Fill(i, q) => if let Some(c) = pick(&tracked, i) {
                    let rec = reg.get(c).unwrap();
                    let q = q.min(rec.resting().get());
                    if q > 0 {
                        let s = rec.placed().side;
                        fill_of(&mut reg, &mut l, c, s, q, &format!("f{n}"));
                    }
                },
                Op::Cancelled(i) => if let Some(c) = pick(&tracked, i) {
                    let rec = reg.get(c).unwrap();
                    let mut u = update(Some(c), canceled(), rec.filled().get());
                    u.side = rec.placed().side;
                    reg.apply_update(&u, OrderKey { venue: None, ingest: n as u64 });
                },
                Op::Amend(i, q) => if let Some(c) = pick(&tracked, i)
                    && let Ok(live) = reg.live(c)
                {
                    let s = live.order().placed().side;
                    if live.amend(&venue, Ticks(100), lots(q), false).is_ok() {
                        reg.amend_sent(c, Ticks(100), lots(q), RpcId(n as u64), MonoNs(2))
                            .unwrap();
                        prop_assert!(worst(&reg, &tracked, s, 0) <= CAP);
                    }
                },
                Op::Venue(s, q) => position(&mut reg, &mut l, s, q, &format!("v{n}")),
            }
        }
    }
}

/// A fill naming the order by its venue id alone stays the order's: the cap counts what is
/// left of it, not the fill twice.
#[test]
fn a_fill_named_by_venue_id_reduces_the_remainder_it_counts() {
    let mut reg = registry(CAP);
    let mut l = ledger();
    let a = open(&mut reg, buy(30), "a");
    let f = fbc_core::FillEvent {
        cid: None,
        ..fill(
            None,
            FillIdent::Venue {
                fill: common::fill_id("fv"),
                vid: Some(vid("a")),
                cum_after: None,
            },
            Side::Buy,
            30,
            false,
        )
    };
    match l.admit(&f, None, MonoNs(0)) {
        fbc_oms::Admission::Apply(acc) => {
            assert!(matches!(reg.apply_fill(acc), Ok(FillRouted::Ours(c, _)) if c == a));
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(Lots::ZERO));
    assert_eq!(reg.place(buy(21)), Err(capped(Side::Buy, 51)));
    reg.place(buy(20)).unwrap();
}
