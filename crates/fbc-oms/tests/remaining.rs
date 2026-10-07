//! Amends on a venue whose amend states the quantity still to fill (`AmendQty::Remaining`;
//! FBC-w5n, decision 0064). The wire carries the new total less the fills the record held at
//! build; fills the OMS has not seen can reach the venue first, and the venue then rests the
//! whole wire quantity on top of them. So the OMS counts the order's resting as at least that
//! wire quantity until the venue settles the amend, and, for the inventory cap (0005's I6),
//! what the order may add now plus the wire quantity; once an amended update confirms it, the
//! count runs from the venue's total (or the wire quantity on top of the fills then known),
//! less the fills after it.

#[path = "common/arm.rs"]
mod arm;
mod common;

use std::collections::VecDeque;
use std::time::Duration;

use common::{cid, fill, ident, lots, order_caps, placement, update, vid};
use fbc_core::{
    AckLevel, AmendAck, AmendCaps, AmendQty, ClientOrderId, InstrumentId, ItemRef, Lots, MonoNs,
    NewOrder, NotAmendable, OrderCaps, OrderUpdate, RefKind, Reject, RejectKind, RpcId, Side,
    SignedLots, SubmitOutcome, TagSet, Ticks, VenueCommand, VenueOrderState, WallNs,
};
use fbc_oms::{
    AmendRefusal, CapRefusal, FillLedger, FillRouted, Intent, LedgerConfig, MarketCapsConfig,
    OmsError, OrdState, OrderKey, OrderOp, PreTradeCaps, Registry, TerminalKind,
};
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, RngSeed};

const INST: InstrumentId = InstrumentId::new(1);
/// The owner's first inventory cap, in lots of $1.
const CAP: i64 = 50;
/// A resting cap past any order, for the tests that judge the inventory cap alone.
const WIDE: i64 = 1_000_000;

fn registry(inventory: i64, resting: i64) -> Registry {
    let caps = PreTradeCaps::new()
        .with_market(
            INST,
            MarketCapsConfig {
                inventory: Some(lots(inventory)),
                resting: Some(lots(resting)),
            },
        )
        .unwrap();
    let mut reg = arm::named(Registry::with_caps(caps));
    arm::seed(&mut reg, &[(INST, 0)]);
    arm::start(&mut reg, INST);
    reg
}

/// A venue that amends price and quantity of partly filled orders, by venue id, keeping the
/// id, its amend stating the remaining quantity.
fn remaining() -> OrderCaps {
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
            qty_semantics: AmendQty::Remaining,
            keeps_priority: None,
        }),
        ..order_caps()
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

fn fill_of(reg: &mut Registry, l: &mut FillLedger, c: ClientOrderId, qty: i64, fid: &str) {
    let f = fill(Some(c), ident(fid), Side::Buy, qty, false);
    match l.admit(&f, None, MonoNs(0)) {
        fbc_oms::Admission::Apply(a) => {
            assert!(matches!(reg.apply_fill(a), Ok(FillRouted::Ours(..))));
        }
        other => panic!("expected the fill accepted, got {other:?}"),
    }
}

fn outcome(reg: &mut Registry, c: ClientOrderId, op: OrderOp, o: &SubmitOutcome) {
    let item = ItemRef {
        idx: 0,
        cid: None,
        vid: Some(vid("a")),
    };
    reg.on_outcome(c, op, &item, o, MonoNs(1)).unwrap();
}

fn refused() -> SubmitOutcome {
    SubmitOutcome::Rejected(Reject {
        kind: RejectKind::NotAmendable(NotAmendable::Other),
        venue_code: None,
        raw: "refused".into(),
    })
}

/// Places a buy of `qty` at 100 and acknowledges it under the venue id `a`: Open.
fn open(reg: &mut Registry, qty: i64) -> ClientOrderId {
    let order: NewOrder = placement(cid(), 100, qty);
    let c = order.cid;
    reg.place(order).unwrap();
    outcome(
        reg,
        c,
        OrderOp::Place,
        &SubmitOutcome::Accepted {
            ack: AckLevel::Final,
        },
    );
    c
}

/// Builds the amend of `c` to `px` and the total `qty` and reports it sent under `rpc`;
/// returns the quantity its wire carries.
fn amend(reg: &mut Registry, c: ClientOrderId, px: i64, qty: i64, rpc: u64) -> Lots {
    let built = reg
        .live(c)
        .unwrap()
        .amend(&remaining(), Ticks(px), lots(qty), false)
        .unwrap();
    let wire = match built.command() {
        VenueCommand::Amend(a) => a.wire_qty(AmendQty::Remaining).unwrap(),
        other => panic!("{other:?}"),
    };
    assert!(
        reg.amend_sent(c, Ticks(px), lots(qty), RpcId(rpc), MonoNs(2))
            .unwrap()
    );
    wire
}

/// The venue's update for our order `c` (venue id `a`) in `state`, with cumulative fill
/// `cum`, its price and total when stated.
fn venue_update(
    c: ClientOrderId,
    state: VenueOrderState,
    cum: i64,
    px: Option<i64>,
    qty: Option<i64>,
) -> OrderUpdate {
    let mut u = update(Some(c), state, cum);
    u.vid = Some(vid("a"));
    u.px = px.map(Ticks);
    u.qty = qty.map(lots);
    u
}

fn amended() -> VenueOrderState {
    VenueOrderState::Amended { new_vid: None }
}

fn keyed(venue: u64) -> OrderKey {
    OrderKey {
        venue: Some(venue),
        ingest: venue,
    }
}

fn resting(reg: &Registry) -> Lots {
    reg.resting_on(INST, Side::Buy).unwrap()
}

fn unkeyed(ingest: u64) -> OrderKey {
    OrderKey {
        venue: None,
        ingest,
    }
}

fn in_flight(reg: &Registry, c: ClientOrderId) -> bool {
    matches!(reg.get(c).unwrap().intent(), Intent::PendingAmend { .. })
}

#[test]
fn the_venue_rests_the_wire_quantity_on_top_of_fills_it_took_before_the_amend_applied() {
    // Codex r4172303663 on PR #8: 4 lots filled, the total amended to 10, the wire 6; 2 more
    // fill first, and the venue rests 6 with 6 filled.
    let mut reg = registry(CAP, WIDE);
    let mut l = ledger();
    let a = open(&mut reg, 10);
    reg.apply_update(
        &venue_update(a, VenueOrderState::Open, 0, None, None),
        keyed(1),
    );
    fill_of(&mut reg, &mut l, a, 4, "f1");
    assert_eq!(resting(&reg), lots(6));
    assert_eq!(amend(&mut reg, a, 101, 10, 7), lots(6));
    assert_eq!(resting(&reg), lots(6));
    // The 2 fills reach the OMS before the acknowledgement: the venue may rest the whole 6.
    fill_of(&mut reg, &mut l, a, 2, "f2");
    assert_eq!(reg.get(a).unwrap().filled(), lots(6));
    assert_eq!(resting(&reg), lots(6), "not 10 - 6");
    // Nor do fills that cover the old total complete the order while the amend is unsettled.
    fill_of(&mut reg, &mut l, a, 4, "f3");
    assert_eq!(reg.get(a).unwrap().state(), OrdState::PartiallyFilled);
    assert_eq!(resting(&reg), lots(6));
    // The venue acknowledges the amend with its cumulative fill when it applied (6), stating
    // no total: the total is at most the 10 filled now plus the wire 6.
    let ack = venue_update(a, amended(), 6, None, None);
    reg.apply_update(&ack, keyed(2));
    let rec = reg.get(a).unwrap();
    assert_eq!(rec.intent(), Intent::None);
    assert_eq!((rec.px(), rec.qty()), (Some(Ticks(101)), lots(16)));
    assert_eq!(resting(&reg), lots(6));
    // A fill after the acknowledgement comes out of the wire quantity.
    fill_of(&mut reg, &mut l, a, 1, "f4");
    assert_eq!(resting(&reg), lots(5));
    // The venue states the order's total: 12, of which 11 filled.
    reg.apply_update(
        &venue_update(a, VenueOrderState::Open, 11, Some(101), Some(12)),
        keyed(3),
    );
    assert_eq!(reg.get(a).unwrap().qty(), lots(12));
    assert_eq!(resting(&reg), lots(1));
    fill_of(&mut reg, &mut l, a, 1, "f5");
    assert_eq!(
        reg.get(a).unwrap().state(),
        OrdState::Terminal(TerminalKind::Filled)
    );
}

#[test]
fn a_fill_taken_before_the_amend_applied_but_reported_after_its_acknowledgement_does_not_reduce_the_count()
 {
    let mut reg = registry(CAP, WIDE);
    let mut l = ledger();
    let a = open(&mut reg, 10);
    fill_of(&mut reg, &mut l, a, 4, "f1");
    assert_eq!(amend(&mut reg, a, 100, 10, 7), lots(6));
    // The venue took 2 more before the amend applied; its acknowledgement says 6 filled.
    reg.apply_update(&venue_update(a, amended(), 6, None, None), keyed(2));
    assert_eq!(reg.get(a).unwrap().intent(), Intent::None);
    assert_eq!(reg.get(a).unwrap().qty(), lots(12));
    assert_eq!(resting(&reg), lots(6));
    // Their fill events arrive after it: the venue still rests the whole 6.
    fill_of(&mut reg, &mut l, a, 2, "f2");
    assert_eq!(resting(&reg), lots(6));
    // An acknowledgement that states the total takes it as it is.
    let mut reg = registry(CAP, WIDE);
    let b = open(&mut reg, 10);
    fill_of(&mut reg, &mut l, b, 4, "g1");
    assert_eq!(amend(&mut reg, b, 100, 10, 8), lots(6));
    reg.apply_update(
        &venue_update(b, amended(), 6, Some(100), Some(12)),
        keyed(2),
    );
    assert_eq!(reg.get(b).unwrap().intent(), Intent::None);
    assert_eq!(reg.get(b).unwrap().qty(), lots(12));
    assert_eq!(resting(&reg), lots(6));
}

#[test]
fn the_inventory_cap_counts_what_the_order_may_add_now_and_the_wire_quantity_together() {
    let mut reg = registry(CAP, WIDE);
    let mut l = ledger();
    let a = open(&mut reg, 30);
    fill_of(&mut reg, &mut l, a, 10, "f1");
    // The position is 10 and 20 rest. A wire of 21 could follow 19 unseen fills: refused.
    let refused = reg
        .live(a)
        .unwrap()
        .amend(&remaining(), Ticks(100), lots(31), false);
    assert_eq!(
        refused.map(|_| ()),
        Err(AmendRefusal::Capped(CapRefusal::InventoryCap {
            inst: INST,
            side: Side::Buy,
            worst: Some(lots(51)),
            cap: lots(CAP),
        }))
    );
    assert_eq!(reg.get(a).unwrap().amend_built(), None);
    // A wire of 20 makes exactly 50: built, and counted from when it is built.
    let built = reg
        .live(a)
        .unwrap()
        .amend(&remaining(), Ticks(100), lots(30), false)
        .unwrap();
    assert_eq!(reg.get(a).unwrap().exposure(), lots(40));
    assert_eq!(resting(&reg), lots(20));
    assert_eq!(
        reg.place(placement(cid(), 100, 1)).map(|_| ()),
        Err(OmsError::Capped(CapRefusal::InventoryCap {
            inst: INST,
            side: Side::Buy,
            worst: Some(lots(51)),
            cap: lots(CAP),
        }))
    );
    // Never handed to a gateway, its reservation is released.
    assert!(reg.amend_not_submitted(built));
    assert_eq!(reg.get(a).unwrap().exposure(), lots(20));
    // The resting cap counts the larger of what rests now and the wire quantity.
    let mut tight = registry(CAP, 12);
    let b = open(&mut tight, 12);
    fill_of(&mut tight, &mut l, b, 4, "g1");
    assert_eq!(
        tight
            .live(b)
            .unwrap()
            .amend(&remaining(), Ticks(100), lots(17), false)
            .map(|_| ()),
        Err(AmendRefusal::Capped(CapRefusal::RestingCap {
            inst: INST,
            side: Side::Buy,
            resting: Some(lots(13)),
            cap: lots(12),
        }))
    );
    assert_eq!(amend(&mut tight, b, 100, 16, 3), lots(12));
    assert_eq!(tight.resting_on(INST, Side::Buy), Some(lots(12)));
}

#[test]
fn a_refused_remaining_amend_leaves_the_order_as_it_was_and_completes_a_covered_one() {
    let mut reg = registry(CAP, WIDE);
    let mut l = ledger();
    let a = open(&mut reg, 10);
    fill_of(&mut reg, &mut l, a, 4, "f1");
    amend(&mut reg, a, 101, 14, 7);
    assert_eq!(resting(&reg), lots(10));
    assert_eq!(reg.get(a).unwrap().exposure(), lots(16));
    outcome(&mut reg, a, OrderOp::Amend(RpcId(7)), &refused());
    assert_eq!(reg.get(a).unwrap().intent(), Intent::None);
    assert_eq!(resting(&reg), lots(6));
    assert_eq!(reg.get(a).unwrap().exposure(), lots(6));
    // Fills that covered the old total while it was in flight complete the order once the
    // amend is refused.
    amend(&mut reg, a, 101, 14, 8);
    fill_of(&mut reg, &mut l, a, 6, "f2");
    assert_eq!(reg.get(a).unwrap().state(), OrdState::PartiallyFilled);
    assert_eq!(resting(&reg), lots(10));
    outcome(&mut reg, a, OrderOp::Amend(RpcId(8)), &refused());
    assert_eq!(
        reg.get(a).unwrap().state(),
        OrdState::Terminal(TerminalKind::Filled)
    );
}

#[test]
fn without_venue_ordering_keys_only_an_acknowledgement_stating_the_wire_as_remainder_confirms() {
    let mut reg = registry(CAP, WIDE);
    let mut l = ledger();
    let a = open(&mut reg, 10);
    fill_of(&mut reg, &mut l, a, 4, "f1");
    amend(&mut reg, a, 101, 10, 7);
    // A bare acknowledgement may be a duplicate of an older one: the amend stays in flight.
    reg.apply_update(&venue_update(a, amended(), 4, None, None), unkeyed(1));
    assert!(in_flight(&reg, a));
    // Nor does an update other than an amended one confirm it, whatever it states.
    reg.apply_update(
        &venue_update(a, VenueOrderState::Open, 6, Some(101), Some(12)),
        unkeyed(2),
    );
    assert!(in_flight(&reg, a));
    assert_eq!(resting(&reg), lots(6));
    // An amended update whose total leaves another remainder is not this amend's.
    reg.apply_update(
        &venue_update(a, amended(), 5, Some(101), Some(10)),
        unkeyed(3),
    );
    assert!(in_flight(&reg, a));
    // Nor is one leaving the wire quantity at a cumulative fill an amended update applied
    // already stated: it may repeat an earlier amend's (RA102-1, RB-w5n-1 on PR #102).
    reg.apply_update(
        &venue_update(a, amended(), 5, Some(101), Some(11)),
        unkeyed(4),
    );
    assert!(in_flight(&reg, a));
    // One leaving the wire quantity as the remainder at a larger cumulative fill is.
    reg.apply_update(
        &venue_update(a, amended(), 6, Some(101), Some(12)),
        unkeyed(5),
    );
    assert_eq!(reg.get(a).unwrap().intent(), Intent::None);
    assert_eq!(reg.get(a).unwrap().qty(), lots(12));
    fill_of(&mut reg, &mut l, a, 2, "f2");
    assert_eq!(resting(&reg), lots(6));
}

#[test]
fn a_remaining_amend_replaced_in_flight_counts_until_the_venue_states_the_total_later() {
    let mut reg = registry(CAP, WIDE);
    let mut l = ledger();
    let a = open(&mut reg, 10);
    reg.apply_update(
        &venue_update(a, VenueOrderState::Open, 0, None, None),
        keyed(1),
    );
    fill_of(&mut reg, &mut l, a, 4, "f1");
    amend(&mut reg, a, 101, 12, 7);
    // A cancel replaces it in flight, and is refused: the amend may still rest its 8.
    assert!(reg.cancel_sent(a, RpcId(8), MonoNs(3)).unwrap());
    reg.apply_update(
        &venue_update(a, VenueOrderState::Open, 4, None, None),
        keyed(2),
    );
    outcome(&mut reg, a, OrderOp::Cancel(RpcId(8)), &refused());
    assert!(reg.get(a).unwrap().amend_unconfirmed());
    fill_of(&mut reg, &mut l, a, 6, "f2");
    assert_eq!(resting(&reg), lots(8));
    assert_eq!(reg.get(a).unwrap().state(), OrdState::PartiallyFilled);
    // A total stated under a key no later than one applied while the amend was on its way
    // may predate it: it settles nothing.
    reg.apply_update(
        &venue_update(a, VenueOrderState::Open, 10, Some(100), Some(10)),
        keyed(2),
    );
    assert_eq!(resting(&reg), lots(8));
    // Stated later, with nothing in flight: the amend did not take, and the order is filled.
    reg.apply_update(
        &venue_update(a, VenueOrderState::Open, 10, Some(100), Some(10)),
        keyed(3),
    );
    assert!(!reg.get(a).unwrap().amend_unconfirmed());
    assert_eq!(
        reg.get(a).unwrap().state(),
        OrdState::Terminal(TerminalKind::Filled)
    );
}

#[test]
fn the_acknowledgement_of_an_amend_replaced_in_flight_does_not_confirm_the_amend_after_it() {
    // Reviewer A RA102-1 (1) and Reviewer B RB-w5n-1 (3) on PR #102: A1 is replaced in flight
    // by a refused cancel, and A2, at the same price and wire quantity, follows it. A1's
    // acknowledgement leaves A2's wire quantity as the remainder: it is still not A2's.
    let mut reg = registry(CAP, WIDE);
    let mut l = ledger();
    let a = open(&mut reg, 10);
    reg.apply_update(
        &venue_update(a, VenueOrderState::Open, 0, None, None),
        keyed(1),
    );
    fill_of(&mut reg, &mut l, a, 4, "f1");
    assert_eq!(amend(&mut reg, a, 101, 10, 7), lots(6));
    assert!(reg.cancel_sent(a, RpcId(8), MonoNs(3)).unwrap());
    outcome(&mut reg, a, OrderOp::Cancel(RpcId(8)), &refused());
    assert_eq!(amend(&mut reg, a, 101, 10, 9), lots(6));
    reg.apply_update(
        &venue_update(a, amended(), 4, Some(101), Some(10)),
        keyed(2),
    );
    assert!(in_flight(&reg, a), "A1's acknowledgement confirmed A2");
    // 3 fill after A1 applied; A2 then rests its whole 6.
    fill_of(&mut reg, &mut l, a, 3, "f2");
    assert_eq!(resting(&reg), lots(6));
    reg.apply_update(
        &venue_update(a, amended(), 7, Some(101), Some(13)),
        keyed(3),
    );
    assert_eq!(resting(&reg), lots(6));
}

#[test]
fn a_duplicate_of_an_earlier_amends_acknowledgement_does_not_confirm_the_amend_in_flight() {
    // Reviewer B RB-w5n-1 (1) and Reviewer A RA102-1 (2) on PR #102: A0 rests 6 and is
    // confirmed; 2 fill; A1 tops the order back up to 6 at the same price. A0's
    // acknowledgement delivered again leaves 6 as the remainder too.
    for keys in [true, false] {
        let key = |n: u64| if keys { keyed(n) } else { unkeyed(n) };
        let mut reg = registry(CAP, WIDE);
        let mut l = ledger();
        let a = open(&mut reg, 6);
        assert_eq!(amend(&mut reg, a, 101, 6, 7), lots(6));
        let ack = venue_update(a, amended(), 0, Some(101), Some(6));
        reg.apply_update(&ack, key(2));
        assert!(!in_flight(&reg, a));
        fill_of(&mut reg, &mut l, a, 2, "f1");
        assert_eq!(resting(&reg), lots(4));
        assert_eq!(amend(&mut reg, a, 101, 8, 8), lots(6));
        // Delivered again: under the same venue key, or, without keys, later in ingest order.
        let again = OrderKey {
            venue: key(2).venue,
            ingest: 3,
        };
        reg.apply_update(&ack, again);
        assert!(
            in_flight(&reg, a),
            "a duplicate confirmed A1 (keys: {keys})"
        );
        assert_eq!(resting(&reg), lots(6));
        // The venue applies A1 on its 2 filled, resting 6, then fills 4 more: 2 still rest.
        fill_of(&mut reg, &mut l, a, 4, "f2");
        assert_eq!(reg.get(a).unwrap().state(), OrdState::PartiallyFilled);
        assert!(resting(&reg) >= lots(2));
        // A1's own acknowledgement, at a cumulative fill no earlier one stated, confirms it.
        reg.apply_update(&venue_update(a, amended(), 2, Some(101), Some(8)), key(4));
        assert!(!in_flight(&reg, a));
        assert_eq!(resting(&reg), lots(2));
    }
}

// ---- generated interleavings ----

fn config(cases: u32, seed: u64) -> ProptestConfig {
    ProptestConfig {
        cases,
        rng_seed: RngSeed::Fixed(seed),
        failure_persistence: None,
        ..ProptestConfig::default()
    }
}

/// One step of a run: the OMS builds an amend, or sends a cancel over the amend in flight; the
/// venue fills, applies the amend in its queue, or reports the order; the OMS reads its fill
/// feed or its order feed, or an order event it already read is delivered again.
#[derive(Clone, Debug)]
enum Step {
    /// The OMS amends the order to `qty` total at price 100 + `px`.
    Amend(i64, i64),
    /// The OMS tops the order back up: an amend at the last amend's price whose wire carries
    /// the last amend's wire quantity again (a market maker keeping the same size resting).
    TopUp,
    /// The venue fills up to `qty` of what it rests.
    VenueFill(i64),
    /// The venue takes the oldest command sent to it: it applies an amend, its
    /// acknowledgement stating the total when `states`, and refuses a cancel.
    Apply(bool),
    /// The venue reports the order, stating its total.
    Report,
    /// The OMS reads the next fill.
    ReadFill,
    /// The OMS reads the next order event or command outcome.
    ReadOrder,
    /// The OMS sends a cancel over the amend in flight, which the venue refuses when it
    /// takes it: the amend is replaced in flight, and the venue applies it first.
    Cancel,
    /// An order event the OMS already read is delivered again (the `n`th of them, cycling):
    /// a duplicate under its venue key, or, without venue keys, later in ingest order.
    Redeliver(usize),
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        2 => (1..30i64, 0..3i64).prop_map(|(q, p)| Step::Amend(q, p)),
        1 => Just(Step::TopUp),
        3 => (1..8i64).prop_map(Step::VenueFill),
        2 => any::<bool>().prop_map(Step::Apply),
        1 => Just(Step::Report),
        3 => Just(Step::ReadFill),
        3 => Just(Step::ReadOrder),
        1 => Just(Step::Cancel),
        1 => any::<usize>().prop_map(Step::Redeliver),
    ]
}

/// What the OMS's order feed delivers.
enum OrderMsg {
    Update(OrderUpdate, OrderKey),
    Refused(RpcId),
    CancelRefused(RpcId),
}

/// A command the venue has not taken yet: an amend (request, price and wire quantity) or a
/// cancel.
enum Cmd {
    Amend(RpcId, i64, i64),
    Cancel(RpcId),
}

/// The venue: one order, its total, its fills and its price, and what it has sent on each
/// feed; it takes commands in the order sent, each feed delivers in order, the two in any
/// order between them.
struct Venue {
    qty: i64,
    cum: i64,
    px: i64,
    live: bool,
    /// The commands sent to it and not yet taken.
    inbox: VecDeque<Cmd>,
    fills: VecDeque<i64>,
    orders: VecDeque<OrderMsg>,
    /// The venue's ordering key of the next order event, when it gives them.
    seq: Option<u64>,
}

impl Venue {
    fn resting(&self) -> i64 {
        if self.live { self.qty - self.cum } else { 0 }
    }

    fn key(&mut self, ingest: u64) -> OrderKey {
        let venue = self.seq;
        self.seq = self.seq.map(|s| s + 1);
        OrderKey { venue, ingest }
    }
}

proptest! {
    #![proptest_config(config(2048, 0x0064_0001))]

    /// FBC-w5n's property: on a venue whose amend states the remaining quantity, whatever
    /// order the venue's fills, its amend acknowledgements and its reports of the order reach
    /// the OMS in, the OMS counts on the order's side at least what the venue rests (an
    /// applied amend's wire quantity less the fills after it), the OMS's worst case (its
    /// inventory plus what the side may add) is at least the venue's (its fills plus what it
    /// rests), and so I6 holds against what the venue actually rests: its fills plus its
    /// resting quantity never pass the inventory cap that admitted every amend.
    #[test]
    fn the_oms_never_counts_less_resting_than_a_remaining_quantity_venue_rests(
        start in 1..30i64,
        keys in any::<bool>(),
        steps in proptest::collection::vec(step(), 1..80),
    ) {
        let mut reg = registry(CAP, WIDE);
        let mut l = ledger();
        let a = open(&mut reg, start);
        // The order events the OMS read, for delivering again.
        let mut read: Vec<(OrderUpdate, OrderKey)> = Vec::new();
        // The last amend's price offset and wire quantity, for topping up.
        let mut last: Option<(i64, i64)> = None;
        let mut v = Venue {
            qty: start,
            cum: 0,
            px: 100,
            live: true,
            inbox: VecDeque::new(),
            fills: VecDeque::new(),
            orders: VecDeque::new(),
            seq: keys.then_some(1),
        };
        for (n, step) in steps.into_iter().enumerate() {
            let n = n as u64 + 10;
            let step = match (step, last) {
                (Step::TopUp, Some((p, wire))) => {
                    Step::Amend(reg.get(a).unwrap().filled().get() + wire, p)
                }
                (Step::TopUp, None) => continue,
                (other, _) => other,
            };
            match step {
                Step::Amend(qty, p) => {
                    let worst = reg.inventory(INST).0 + reg.get(a).unwrap().exposure().get();
                    let Ok(live) = reg.live(a) else { continue };
                    let Ok(cmd) = live.amend(&remaining(), Ticks(100 + p), lots(qty), false)
                    else {
                        continue;
                    };
                    let wire = match cmd.command() {
                        VenueCommand::Amend(am) => am.wire_qty(AmendQty::Remaining).unwrap(),
                        other => panic!("{other:?}"),
                    };
                    // Admitted only when what the order may add now and the wire quantity,
                    // on the position, fit the cap.
                    prop_assert!(worst + wire.get() <= CAP);
                    reg.amend_sent(a, Ticks(100 + p), lots(qty), RpcId(n), MonoNs(n)).unwrap();
                    v.inbox.push_back(Cmd::Amend(RpcId(n), 100 + p, wire.get()));
                    last = Some((p, wire.get()));
                }
                Step::TopUp => unreachable!("made an amend above"),
                Step::VenueFill(q) => {
                    let q = q.min(v.resting());
                    if q > 0 {
                        v.cum += q;
                        v.fills.push_back(q);
                        if v.resting() == 0 {
                            v.live = false;
                            let key = v.key(n);
                            let done = venue_update(
                                a, VenueOrderState::Filled, v.cum, Some(v.px), Some(v.qty),
                            );
                            v.orders.push_back(OrderMsg::Update(done, key));
                        }
                    }
                }
                Step::Apply(states) => {
                    let (rpc, px, wire) = match v.inbox.pop_front() {
                        Some(Cmd::Amend(rpc, px, wire)) => (rpc, px, wire),
                        Some(Cmd::Cancel(rpc)) => {
                            v.orders.push_back(OrderMsg::CancelRefused(rpc));
                            continue;
                        }
                        None => continue,
                    };
                    if !v.live {
                        v.orders.push_back(OrderMsg::Refused(rpc));
                        continue;
                    }
                    // The venue rests the whole wire quantity on top of what it has filled.
                    v.qty = v.cum + wire;
                    v.px = px;
                    let key = v.key(n);
                    let ack = venue_update(
                        a, amended(), v.cum, states.then_some(px), states.then_some(v.qty),
                    );
                    v.orders.push_back(OrderMsg::Update(ack, key));
                }
                Step::Report => {
                    if v.live {
                        let key = v.key(n);
                        let open = venue_update(
                            a, VenueOrderState::Open, v.cum, Some(v.px), Some(v.qty),
                        );
                        v.orders.push_back(OrderMsg::Update(open, key));
                    }
                }
                Step::ReadFill => {
                    if let Some(q) = v.fills.pop_front() {
                        fill_of(&mut reg, &mut l, a, q, &format!("f{n}"));
                    }
                }
                Step::ReadOrder => match v.orders.pop_front() {
                    Some(OrderMsg::Update(u, key)) => {
                        reg.apply_update(&u, key);
                        read.push((u, key));
                    }
                    Some(OrderMsg::Refused(rpc)) => {
                        outcome(&mut reg, a, OrderOp::Amend(rpc), &refused());
                    }
                    Some(OrderMsg::CancelRefused(rpc)) => {
                        outcome(&mut reg, a, OrderOp::Cancel(rpc), &refused());
                    }
                    None => {}
                },
                Step::Cancel => {
                    if matches!(reg.get(a).unwrap().intent(), Intent::PendingAmend { .. }) {
                        reg.cancel_sent(a, RpcId(n), MonoNs(n)).unwrap();
                        v.inbox.push_back(Cmd::Cancel(RpcId(n)));
                    }
                }
                Step::Redeliver(i) => {
                    // Without venue keys, an older event read again while nothing is in flight
                    // may lower any total (0005's unordered feeds, not this ticket's): only the
                    // last one read is delivered again then.
                    let pending = matches!(
                        reg.get(a).unwrap().intent(),
                        Intent::PendingAmend { .. }
                    );
                    let pick = match read.len() {
                        0 => None,
                        len if keys || pending => Some(i % len),
                        len => Some(len - 1),
                    };
                    if let Some(i) = pick {
                        let (u, key) = read[i].clone();
                        reg.apply_update(&u, OrderKey { venue: key.venue, ingest: n });
                    }
                }
            }
            let counted = reg.resting_on(INST, Side::Buy).unwrap().get();
            prop_assert!(
                counted >= v.resting(),
                "the OMS counts {} resting while the venue rests {}", counted, v.resting()
            );
            let oms_worst =
                reg.inventory(INST).0 + reg.get(a).unwrap().exposure().get();
            prop_assert!(oms_worst >= v.cum + v.resting());
            prop_assert!(v.cum + v.resting() <= CAP, "I6 against the venue's resting");
        }
    }
}

/// A fixed run of what the generated ones interleave: an amend built, fills on both sides of
/// its acknowledgement, the count running from the wire quantity.
#[test]
fn a_run_with_fills_on_both_sides_of_an_acknowledgement_keeps_the_count() {
    let mut reg = registry(CAP, WIDE);
    let mut l = ledger();
    let a = open(&mut reg, 20);
    fill_of(&mut reg, &mut l, a, 5, "f1");
    assert_eq!(amend(&mut reg, a, 102, 25, 7), lots(20));
    // Venue: 3 fill before the amend applies, which rests 20; 4 after.
    reg.apply_update(&venue_update(a, amended(), 8, None, None), keyed(2));
    fill_of(&mut reg, &mut l, a, 3, "f2");
    assert_eq!(resting(&reg), lots(20));
    fill_of(&mut reg, &mut l, a, 4, "f3");
    assert_eq!(resting(&reg), lots(16));
    assert_eq!(reg.inventory(INST), SignedLots(12));
    assert_eq!(reg.get(a).unwrap().exposure(), lots(16));
}
