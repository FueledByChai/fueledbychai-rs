//! Consumer tags and removal reasons (FBC-s6fp; decisions 0001, 0005, 0098): each level
//! of a desired book carries an opaque consumer tag and each side an optional removal reason.
//! The planner copies a level's tag onto every place and amend it builds for that level, and
//! the side's reason onto every cancel it builds for a level the book no longer wants; the
//! registry keeps both on the order, and the audit sink's verdicts carry them. fbc-oms gives
//! the values no meaning.
//!
//! The values are the owner's first test values: a $50 inventory cap on a synthetic market
//! where one lot is worth $1, so 50 lots, and the $11 resting cap per side where a test judges
//! it.

#[path = "common/arm.rs"]
mod arm;
mod common;

use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arm::{leases, start};
use common::{cid, lease_dir, lots, order_caps, placement, vid};
use fbc_core::{
    AccountKey, AckLevel, AmendAck, AmendCaps, AmendQty, Bps, CancelReason, Channel, CidMatch,
    CidMint, ClientOrderId, InstrumentId, ItemRef, MonoNs, Namespace, NamespaceLease, OrderCaps,
    OrderUpdate, RefKind, RpcId, Side, SignedLots, SubmitOutcome, TagSet, Ticks, Tif, VenueCommand,
    VenueOrderState, WallNs,
};
use fbc_oms::{
    Applied, AuditEntry, AuditSink, CapRefusal, DesiredBook, DesiredQuote, ExecutionPlanner,
    LadderConfig, MarketCapsConfig, OmsError, OrderKey, OrderOp, Plan, PlanRefusal, PlannerConfig,
    PreTradeCaps, Registry, ResyncSnapshot, Routed, Stage,
};

const INST: InstrumentId = InstrumentId::new(1);
const ACCT: AccountKey = AccountKey::new(1);
/// The owner's first inventory cap, in lots of $1.
const CAP: i64 = 50;
/// The owner's first resting cap per side, in lots of $1.
const RESTING: i64 = 11;
/// A resting cap past any order of the tests that judge the other rules.
const WIDE: i64 = 1_000_000;
const MIN_AGE: Duration = Duration::from_millis(100);
/// An instant past the minimum age of anything placed at `MonoNs(0)`.
const LATER: MonoNs = MonoNs(1_000_000_000);

/// The consumer's tags and reasons in these tests: values fbc-oms gives no meaning.
const BID_TAG: u16 = 7;
const BID1_TAG: u16 = 8;
const ASK_TAG: u16 = 9;
const REPRICED_TAG: u16 = 17;
const BID_GONE: u16 = 3;
const ASK_GONE: u16 = 4;

fn mint() -> CidMint {
    static NEXT: AtomicU16 = AtomicU16::new(300);
    let ns = Namespace::new(NEXT.fetch_add(1, Ordering::Relaxed));
    let lease = NamespaceLease::acquire(&lease_dir(), AccountKey::new(2), ns).unwrap();
    CidMint::new(lease, 0, 0, WallNs(0))
}

fn planner() -> ExecutionPlanner {
    ExecutionPlanner::new(PlannerConfig::new(Bps(2.0), lots(2), MIN_AGE).unwrap())
}

/// A venue that amends price and total by venue id, keeping the venue id; cancels by venue id.
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

fn key(n: u64) -> OrderKey {
    OrderKey {
        venue: Some(n),
        ingest: n,
    }
}

/// A named registry under the inventory cap and the resting cap `resting` on `INST`, seeded
/// at `pos` by a trustworthy resync showing no order, armed and Quoting.
fn quoting(resting: i64, pos: i64) -> Registry {
    let caps = PreTradeCaps::new()
        .with_market(
            INST,
            MarketCapsConfig {
                inventory: Some(lots(CAP)),
                resting: Some(lots(resting)),
            },
        )
        .unwrap();
    let mut reg = arm::named(Registry::with_caps(caps));
    let snap = ResyncSnapshot {
        watermark: WallNs(1_000),
        requested_at: MonoNs(1_000),
        orders: vec![],
        positions: vec![(INST, SignedLots(pos))],
    };
    let ladder = LadderConfig::new(
        Duration::from_secs(1),
        Duration::ZERO,
        Duration::from_secs(10),
        1,
    )
    .unwrap();
    reg.resync(
        &ladder,
        &amending(),
        &snap,
        OrderKey {
            venue: None,
            ingest: 0,
        },
    )
    .unwrap();
    start(&mut reg, INST);
    reg
}

/// A post-only GTC limit quote of `qty` at `px` under the consumer tag `tag`.
fn quote(px: i64, qty: i64, tag: u16) -> DesiredQuote {
    DesiredQuote {
        px: Ticks(px),
        qty: lots(qty),
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        reducing: false,
        tag,
    }
}

/// Each command's stage, kind, side, level, tag and removal reason, in the order planned.
type Shape = (Stage, &'static str, Side, u16, u16, Option<u16>);

fn shape(plan: &Plan) -> Vec<Shape> {
    plan.commands
        .iter()
        .map(|p| {
            let kind = match p.auth.command() {
                VenueCommand::Place(_) => "place",
                VenueCommand::Amend(_) => "amend",
                VenueCommand::Cancel(_) => "cancel",
                other => panic!("the planner built {other:?}"),
            };
            let tag = p
                .tag
                .expect("the planner tagged every order in these tests");
            (p.stage, kind, p.side, p.level, tag, p.removal)
        })
        .collect()
}

fn ack(reg: &mut Registry, c: ClientOrderId, v: &str) {
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
}

fn update(c: ClientOrderId, side: Side, v: &str, state: VenueOrderState) -> OrderUpdate {
    OrderUpdate {
        cid: Some(CidMatch::Ours(c)),
        vid: Some(vid(v)),
        inst: INST,
        side,
        state,
        cum_filled: lots(0),
        px: None,
        qty: None,
        post_only: None,
        reduce_only: None,
    }
}

/// Plans `desired` and reports every amend and cancel sent, as a gateway would.
fn plan_and_send(
    p: &mut ExecutionPlanner,
    desired: &DesiredBook,
    reg: &mut Registry,
    caps: &OrderCaps,
    mint: &mut CidMint,
    now: MonoNs,
) -> Plan {
    let plan = p.plan(desired, reg, caps, ACCT, mint, now).unwrap();
    for (n, c) in plan.commands.iter().enumerate() {
        let rpc = RpcId(1_000 + n as u64);
        match c.auth.command() {
            VenueCommand::Cancel(_) => assert!(reg.cancel_sent(c.cid, rpc, now).unwrap()),
            VenueCommand::Amend(a) => {
                assert!(reg.amend_sent(c.cid, a.px, a.qty, rpc, now).unwrap())
            }
            _ => {}
        }
    }
    plan
}

fn venue_id(side: Side, level: u16) -> String {
    format!("v-{side:?}-{level}")
}

/// Places `desired` at `MonoNs(0)` and acknowledges each order it placed.
fn open_book(
    p: &mut ExecutionPlanner,
    desired: &DesiredBook,
    reg: &mut Registry,
    caps: &OrderCaps,
    mint: &mut CidMint,
) -> Plan {
    let plan = plan_and_send(p, desired, reg, caps, mint, MonoNs(0));
    for c in &plan.commands {
        ack(reg, c.cid, &venue_id(c.side, c.level));
    }
    plan
}

/// Bids at levels 0 and 1, an ask at level 0, each under its own tag.
fn three_levels() -> DesiredBook {
    DesiredBook::new(INST)
        .with(Side::Buy, 0, quote(10_000, 5, BID_TAG))
        .with(Side::Buy, 1, quote(9_990, 5, BID1_TAG))
        .with(Side::Sell, 0, quote(10_010, 5, ASK_TAG))
}

#[test]
fn each_place_and_amend_carries_its_levels_tag_and_the_order_keeps_it() {
    let mut reg = quoting(WIDE, 0);
    let mut mint = mint();
    let mut p = planner();
    let opened = open_book(&mut p, &three_levels(), &mut reg, &amending(), &mut mint);
    assert_eq!(
        shape(&opened),
        vec![
            (Stage::Add, "place", Side::Buy, 0, BID_TAG, None),
            (Stage::Add, "place", Side::Buy, 1, BID1_TAG, None),
            (Stage::Add, "place", Side::Sell, 0, ASK_TAG, None),
        ]
    );
    for c in &opened.commands {
        let rec = reg.get(c.cid).unwrap();
        assert_eq!(rec.tag(), c.tag);
        assert_eq!(rec.removal(), None);
    }

    // The bid at level 0 moves under a new tag: its amend carries the new tag, and so does the
    // order from then on. The ask moves keeping its tag.
    let moved = three_levels()
        .with(Side::Buy, 0, quote(9_995, 5, REPRICED_TAG))
        .with(Side::Sell, 0, quote(10_020, 5, ASK_TAG));
    let plan = plan_and_send(&mut p, &moved, &mut reg, &amending(), &mut mint, LATER);
    assert_eq!(
        shape(&plan),
        vec![
            (Stage::Amend, "amend", Side::Buy, 0, REPRICED_TAG, None),
            (Stage::Amend, "amend", Side::Sell, 0, ASK_TAG, None),
        ]
    );
    let bid = p.order_at(ACCT, INST, Side::Buy, 0).unwrap();
    assert_eq!(plan.commands[0].cid, bid);
    assert_eq!(reg.get(bid).unwrap().tag(), Some(REPRICED_TAG));
    let bid1 = p.order_at(ACCT, INST, Side::Buy, 1).unwrap();
    assert_eq!(reg.get(bid1).unwrap().tag(), Some(BID1_TAG));
}

#[test]
fn a_replace_cancels_under_the_orders_tag_with_no_reason_and_places_under_the_levels() {
    // A venue with no amend: a moved level is cancelled, then placed once the old order is
    // terminal. The side carries a removal reason, but the level is still wanted, so the
    // replace's cancel carries none: only a level the book no longer wants is removed.
    let mut reg = quoting(WIDE, 0);
    let mut mint = mint();
    let mut p = planner();
    let caps = order_caps();
    let first = DesiredBook::new(INST).with(Side::Buy, 0, quote(10_000, 5, BID_TAG));
    open_book(&mut p, &first, &mut reg, &caps, &mut mint);
    let old = p.order_at(ACCT, INST, Side::Buy, 0).unwrap();
    let moved = DesiredBook::new(INST)
        .with(Side::Buy, 0, quote(9_990, 5, REPRICED_TAG))
        .with_removal(Side::Buy, Some(BID_GONE));
    let plan = plan_and_send(&mut p, &moved, &mut reg, &caps, &mut mint, LATER);
    assert_eq!(
        shape(&plan),
        vec![(Stage::Cancel, "cancel", Side::Buy, 0, BID_TAG, None)]
    );
    assert_eq!(reg.get(old).unwrap().removal(), None);
    let gone = update(
        old,
        Side::Buy,
        &venue_id(Side::Buy, 0),
        VenueOrderState::Canceled(CancelReason::Requested),
    );
    reg.apply_update(&gone, key(10));
    let plan = plan_and_send(&mut p, &moved, &mut reg, &caps, &mut mint, LATER);
    assert_eq!(
        shape(&plan),
        vec![(Stage::Add, "place", Side::Buy, 0, REPRICED_TAG, None)]
    );
    assert_eq!(
        reg.get(plan.commands[0].cid).unwrap().tag(),
        Some(REPRICED_TAG)
    );
}

#[test]
fn each_cancel_of_a_removed_level_carries_its_sides_reason() {
    // Bid level 1 leaves the book, the ask is wanted at zero lots: both are removed. Each side
    // carries its own reason; each cancel carries its side's, and the tag of the order.
    let mut reg = quoting(WIDE, 0);
    let mut mint = mint();
    let mut p = planner();
    open_book(&mut p, &three_levels(), &mut reg, &amending(), &mut mint);
    let bid1 = p.order_at(ACCT, INST, Side::Buy, 1).unwrap();
    let ask = p.order_at(ACCT, INST, Side::Sell, 0).unwrap();
    let mut removed = DesiredBook::new(INST)
        .with(Side::Buy, 0, quote(10_000, 5, BID_TAG))
        .with(Side::Sell, 0, quote(10_010, 0, ASK_TAG))
        .with_removal(Side::Buy, Some(BID_GONE));
    removed.set_removal(Side::Sell, Some(ASK_GONE));
    assert_eq!(removed.removal(Side::Buy), Some(BID_GONE));
    assert_eq!(removed.removal(Side::Sell), Some(ASK_GONE));
    let plan = plan_and_send(&mut p, &removed, &mut reg, &amending(), &mut mint, LATER);
    assert_eq!(
        shape(&plan),
        vec![
            (
                Stage::Cancel,
                "cancel",
                Side::Buy,
                1,
                BID1_TAG,
                Some(BID_GONE)
            ),
            (
                Stage::Cancel,
                "cancel",
                Side::Sell,
                0,
                ASK_TAG,
                Some(ASK_GONE)
            ),
        ]
    );
    assert_eq!(plan.commands[0].cid, bid1);
    assert_eq!(plan.commands[1].cid, ask);
    assert_eq!(reg.get(bid1).unwrap().removal(), Some(BID_GONE));
    assert_eq!(reg.get(ask).unwrap().removal(), Some(ASK_GONE));
    // The level still wanted keeps no reason.
    let bid = p.order_at(ACCT, INST, Side::Buy, 0).unwrap();
    assert_eq!(reg.get(bid).unwrap().removal(), None);
}

#[test]
fn a_side_with_no_reason_cancels_its_removed_levels_with_none_and_a_reason_can_be_cleared() {
    let mut reg = quoting(WIDE, 0);
    let mut mint = mint();
    let mut p = planner();
    open_book(&mut p, &three_levels(), &mut reg, &amending(), &mut mint);
    // A reason set and then cleared: the side has none.
    let mut empty = DesiredBook::new(INST).with_removal(Side::Buy, Some(BID_GONE));
    empty.set_removal(Side::Buy, None);
    assert_eq!(empty.removal(Side::Buy), None);
    assert_eq!(empty.removal(Side::Sell), None);
    let plan = plan_and_send(&mut p, &empty, &mut reg, &amending(), &mut mint, LATER);
    assert_eq!(
        shape(&plan),
        vec![
            (Stage::Cancel, "cancel", Side::Buy, 0, BID_TAG, None),
            (Stage::Cancel, "cancel", Side::Buy, 1, BID1_TAG, None),
            (Stage::Cancel, "cancel", Side::Sell, 0, ASK_TAG, None),
        ]
    );
}

#[test]
fn a_cancel_waiting_for_the_acknowledgement_carries_the_reason_of_the_pass_that_builds_it() {
    // A venue that cancels only by venue id: the removed level's order, not yet acknowledged,
    // waits for it. The pass that builds the cancel gives it the side's reason then.
    let mut reg = quoting(WIDE, 0);
    let mut mint = mint();
    let mut p = planner();
    let caps = order_caps();
    let first = DesiredBook::new(INST).with(Side::Buy, 0, quote(10_000, 5, BID_TAG));
    let placed = plan_and_send(&mut p, &first, &mut reg, &caps, &mut mint, MonoNs(0));
    let bid = placed.commands[0].cid;
    let gone = DesiredBook::new(INST).with_removal(Side::Buy, Some(BID_GONE));
    let plan = plan_and_send(&mut p, &gone, &mut reg, &caps, &mut mint, LATER);
    assert!(plan.commands.is_empty());
    assert_eq!(plan.awaiting_ack, vec![bid]);
    ack(&mut reg, bid, &venue_id(Side::Buy, 0));
    let later = DesiredBook::new(INST).with_removal(Side::Buy, Some(ASK_GONE));
    let plan = plan_and_send(&mut p, &later, &mut reg, &caps, &mut mint, LATER);
    assert_eq!(
        shape(&plan),
        vec![(
            Stage::Cancel,
            "cancel",
            Side::Buy,
            0,
            BID_TAG,
            Some(ASK_GONE)
        )]
    );
    assert_eq!(reg.get(bid).unwrap().removal(), Some(ASK_GONE));
}

#[test]
fn in_exit_a_wanted_level_on_the_adding_side_is_cancelled_with_no_reason() {
    // Long 10, Flatten: the bid adds, so it is cancelled though the book still wants it, and
    // the bid side's reason is not given: the book did not remove the level.
    let mut reg = quoting(WIDE, 10);
    let mut mint = mint();
    let mut p = planner();
    let quoted = DesiredBook::new(INST)
        .with(Side::Buy, 0, quote(10_000, 5, BID_TAG))
        .with_removal(Side::Buy, Some(BID_GONE));
    open_book(&mut p, &quoted, &mut reg, &amending(), &mut mint);
    reg.disarm(INST);
    let given = leases(&reg, INST);
    reg.flatten(INST, given).unwrap();
    let plan = plan_and_send(&mut p, &quoted, &mut reg, &amending(), &mut mint, LATER);
    assert_eq!(
        shape(&plan),
        vec![(Stage::Cancel, "cancel", Side::Buy, 0, BID_TAG, None)]
    );
}

#[test]
fn a_place_a_cap_refuses_is_never_built_and_tags_nothing() {
    // The $11 resting cap per side: a 12-lot bid is refused, never built, so no order carries
    // its tag; the ask within the cap is placed under its own.
    let mut reg = quoting(RESTING, 0);
    let mut mint = mint();
    let mut p = planner();
    let desired = DesiredBook::new(INST)
        .with(Side::Buy, 0, quote(10_000, RESTING + 1, BID_TAG))
        .with(Side::Sell, 0, quote(10_010, RESTING, ASK_TAG));
    let plan = plan_and_send(
        &mut p,
        &desired,
        &mut reg,
        &amending(),
        &mut mint,
        MonoNs(0),
    );
    assert_eq!(
        shape(&plan),
        vec![(Stage::Add, "place", Side::Sell, 0, ASK_TAG, None)]
    );
    assert_eq!(plan.refused.len(), 1);
    assert!(matches!(
        plan.refused[0].why,
        PlanRefusal::Place(OmsError::Capped(CapRefusal::RestingCap { .. }))
    ));
    assert_eq!(p.order_at(ACCT, INST, Side::Buy, 0), None);
}

/// What a sink saw of each verdict: the event's number, where it went, and the tag and removal
/// reason it carried.
#[derive(Clone, Eq, PartialEq, Debug)]
struct Verdict {
    seq: u64,
    routed: Routed,
    tag: Option<u16>,
    removal: Option<u16>,
}

#[derive(Clone, Default)]
struct Verdicts(Arc<Mutex<Vec<Verdict>>>);

impl Verdicts {
    fn take(&self) -> Vec<Verdict> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}

impl AuditSink for Verdicts {
    fn record(&mut self, entry: AuditEntry<'_>) {
        if let AuditEntry::Verdict {
            seq,
            routed,
            tag,
            removal,
        } = entry
        {
            self.0.lock().unwrap().push(Verdict {
                seq,
                routed: routed.clone(),
                tag,
                removal,
            });
        }
    }
}

#[test]
fn the_audit_sinks_verdicts_carry_the_orders_tag_and_its_cancels_removal_reason() {
    let mut reg = quoting(WIDE, 0);
    let sink = Verdicts::default();
    reg.set_audit_sink(Box::new(sink.clone()));
    let mut mint = mint();
    let mut p = planner();
    let opened = open_book(&mut p, &three_levels(), &mut reg, &amending(), &mut mint);
    let bid1 = opened.commands[1].cid;
    let ask = opened.commands[2].cid;
    let open = |c, side, level| update(c, side, &venue_id(side, level), VenueOrderState::Open);
    assert_eq!(
        reg.apply_update(&open(bid1, Side::Buy, 1), key(10)),
        Routed::Ours(bid1, Applied::Advanced)
    );
    assert_eq!(
        sink.take(),
        vec![Verdict {
            seq: 0,
            routed: Routed::Ours(bid1, Applied::Advanced),
            tag: Some(BID1_TAG),
            removal: None,
        }]
    );

    // Bid level 1 is removed under its side's reason: the cancel's confirmation carries both,
    // and so does a late event after it.
    let removed = DesiredBook::new(INST)
        .with(Side::Buy, 0, quote(10_000, 5, BID_TAG))
        .with(Side::Sell, 0, quote(10_010, 5, ASK_TAG))
        .with_removal(Side::Buy, Some(BID_GONE));
    plan_and_send(&mut p, &removed, &mut reg, &amending(), &mut mint, LATER);
    let gone = update(
        bid1,
        Side::Buy,
        &venue_id(Side::Buy, 1),
        VenueOrderState::Canceled(CancelReason::Requested),
    );
    reg.apply_update(&gone, key(11));
    reg.apply_update(&open(bid1, Side::Buy, 1), key(12));
    // The ask, still wanted, carries its tag and no reason.
    reg.apply_update(&open(ask, Side::Sell, 0), key(13));
    assert_eq!(
        sink.take(),
        vec![
            Verdict {
                seq: 1,
                routed: Routed::Ours(bid1, Applied::Advanced),
                tag: Some(BID1_TAG),
                removal: Some(BID_GONE),
            },
            Verdict {
                seq: 2,
                routed: Routed::Ours(bid1, Applied::IgnoredLate),
                tag: Some(BID1_TAG),
                removal: Some(BID_GONE),
            },
            Verdict {
                seq: 3,
                routed: Routed::Ours(ask, Applied::Advanced),
                tag: Some(ASK_TAG),
                removal: None,
            },
        ]
    );
}

#[test]
fn a_verdict_carries_neither_for_an_event_routed_to_no_order_or_an_order_the_planner_did_not_place()
{
    let mut reg = Registry::new();
    let direct = reg.insert(placement(cid(), 100, 10)).unwrap().cid();
    let sink = Verdicts::default();
    reg.set_audit_sink(Box::new(sink.clone()));
    let mut ours = update(direct, Side::Buy, "d1", VenueOrderState::Open);
    ours.inst = InstrumentId::new(1);
    reg.apply_update(&ours, key(1));
    let mut foreign = update(direct, Side::Buy, "f1", VenueOrderState::Open);
    foreign.cid = Some(CidMatch::Foreign(Namespace::new(9)));
    reg.apply_update(&foreign, key(2));
    let mut untracked = update(direct, Side::Buy, "zz", VenueOrderState::Open);
    untracked.cid = None;
    reg.apply_update(&untracked, key(3));
    let seen = sink.take();
    assert_eq!(seen.len(), 3);
    assert_eq!(seen[0].routed, Routed::Ours(direct, Applied::Advanced));
    assert_eq!(seen[1].routed, Routed::Foreign(Namespace::new(9)));
    assert_eq!(seen[2].routed, Routed::Untracked);
    for v in &seen {
        assert_eq!((v.tag, v.removal), (None, None), "{v:?}");
    }
    assert_eq!(reg.get(direct).unwrap().tag(), None);
    assert_eq!(reg.get(direct).unwrap().removal(), None);
}

/// Bid level 1's order filled to `filled` lots while it rests: a fill as the venue reports it.
fn filled(c: ClientOrderId, filled: i64) -> OrderUpdate {
    OrderUpdate {
        cum_filled: lots(filled),
        ..update(c, Side::Buy, &venue_id(Side::Buy, 1), VenueOrderState::Open)
    }
}

/// The book of `three_levels` with bid level 1 removed under its side's reason.
fn bid1_removed() -> DesiredBook {
    DesiredBook::new(INST)
        .with(Side::Buy, 0, quote(10_000, 5, BID_TAG))
        .with(Side::Sell, 0, quote(10_010, 5, ASK_TAG))
        .with_removal(Side::Buy, Some(BID_GONE))
}

/// A cancel of `c` sent under `rpc`, answered with `outcome`.
fn cancel_answered(reg: &mut Registry, c: ClientOrderId, rpc: RpcId, outcome: SubmitOutcome) {
    let item = ItemRef {
        idx: 0,
        cid: None,
        vid: Some(vid(&venue_id(Side::Buy, 1))),
    };
    reg.on_outcome(c, OrderOp::Cancel(rpc), &item, &outcome, LATER)
        .unwrap();
}

#[test]
fn a_removal_cancel_refused_leaves_no_reason_on_the_order_its_later_fills_or_its_amend() {
    // RB163-1 on PR #163: a removal cancel the venue refuses ends no order. While it is in
    // flight a fill races it under its reason; once refused, the order rests with no cancel
    // and no reason, and its fills after, an amend under a new tag included, carry none.
    let mut reg = quoting(WIDE, 0);
    let sink = Verdicts::default();
    reg.set_audit_sink(Box::new(sink.clone()));
    let mut mint = mint();
    let mut p = planner();
    open_book(&mut p, &three_levels(), &mut reg, &amending(), &mut mint);
    let bid1 = p.order_at(ACCT, INST, Side::Buy, 1).unwrap();
    let plan = plan_and_send(
        &mut p,
        &bid1_removed(),
        &mut reg,
        &amending(),
        &mut mint,
        LATER,
    );
    assert_eq!(
        shape(&plan),
        vec![(
            Stage::Cancel,
            "cancel",
            Side::Buy,
            1,
            BID1_TAG,
            Some(BID_GONE)
        )]
    );
    // Wanted again while the cancel is in flight: the order is held, its reason kept, and a
    // fill racing the cancel carries it.
    let again = plan_and_send(
        &mut p,
        &three_levels(),
        &mut reg,
        &amending(),
        &mut mint,
        LATER,
    );
    assert!(again.commands.is_empty(), "{:?}", shape(&again));
    assert_eq!(reg.get(bid1).unwrap().removal(), Some(BID_GONE));
    sink.take();
    reg.apply_update(&filled(bid1, 1), key(20));
    let race = sink.take();
    assert_eq!(
        (race[0].tag, race[0].removal),
        (Some(BID1_TAG), Some(BID_GONE))
    );

    let refused = SubmitOutcome::Rejected(fbc_core::Reject {
        kind: fbc_core::RejectKind::RateLimited { retry_after: None },
        venue_code: None,
        raw: "refused".into(),
    });
    cancel_answered(&mut reg, bid1, RpcId(1_000), refused);
    assert_eq!(reg.get(bid1).unwrap().removal(), None);
    reg.apply_update(&filled(bid1, 2), key(21));
    let after = sink.take();
    assert_eq!((after[0].tag, after[0].removal), (Some(BID1_TAG), None));

    // Wanted again under a new tag at a new price: amended, and its fills carry the new tag
    // and no reason.
    let moved = three_levels().with(Side::Buy, 1, quote(9_985, 5, REPRICED_TAG));
    let plan = plan_and_send(&mut p, &moved, &mut reg, &amending(), &mut mint, LATER);
    assert_eq!(
        shape(&plan),
        vec![(Stage::Amend, "amend", Side::Buy, 1, REPRICED_TAG, None)]
    );
    reg.apply_update(&filled(bid1, 3), key(22));
    let amended = sink.take();
    assert_eq!(
        (amended[0].tag, amended[0].removal),
        (Some(REPRICED_TAG), None)
    );
}

#[test]
fn a_removal_cancel_not_sent_leaves_no_reason_on_the_order_or_its_later_fills() {
    let mut reg = quoting(WIDE, 0);
    let sink = Verdicts::default();
    reg.set_audit_sink(Box::new(sink.clone()));
    let mut mint = mint();
    let mut p = planner();
    open_book(&mut p, &three_levels(), &mut reg, &amending(), &mut mint);
    let bid1 = p.order_at(ACCT, INST, Side::Buy, 1).unwrap();
    plan_and_send(
        &mut p,
        &bid1_removed(),
        &mut reg,
        &amending(),
        &mut mint,
        LATER,
    );
    assert_eq!(reg.get(bid1).unwrap().removal(), Some(BID_GONE));
    let not_sent = SubmitOutcome::NotSent(fbc_core::NotSentReason::Disconnected);
    cancel_answered(&mut reg, bid1, RpcId(1_000), not_sent);
    assert_eq!(reg.get(bid1).unwrap().removal(), None);
    // Wanted again where it rests: nothing to build, and its fills carry no reason.
    let again = plan_and_send(
        &mut p,
        &three_levels(),
        &mut reg,
        &amending(),
        &mut mint,
        LATER,
    );
    assert!(again.commands.is_empty(), "{:?}", shape(&again));
    sink.take();
    reg.apply_update(&filled(bid1, 1), key(20));
    let seen = sink.take();
    assert_eq!((seen[0].tag, seen[0].removal), (Some(BID1_TAG), None));
}

#[test]
fn a_removal_cancel_dropped_unspent_leaves_no_reason_once_the_level_is_wanted_again() {
    // A cancel built and never reported sent is no cancel (0065 rule 7): once a pass wants
    // the level again where the order rests, the order keeps no reason, and its fills carry
    // none.
    let mut reg = quoting(WIDE, 0);
    let sink = Verdicts::default();
    reg.set_audit_sink(Box::new(sink.clone()));
    let mut mint = mint();
    let mut p = planner();
    open_book(&mut p, &three_levels(), &mut reg, &amending(), &mut mint);
    let bid1 = p.order_at(ACCT, INST, Side::Buy, 1).unwrap();
    let plan = p
        .plan(
            &bid1_removed(),
            &mut reg,
            &amending(),
            ACCT,
            &mut mint,
            LATER,
        )
        .unwrap();
    assert_eq!(
        shape(&plan),
        vec![(
            Stage::Cancel,
            "cancel",
            Side::Buy,
            1,
            BID1_TAG,
            Some(BID_GONE)
        )]
    );
    drop(plan);
    assert_eq!(reg.get(bid1).unwrap().removal(), Some(BID_GONE));
    let again = plan_and_send(
        &mut p,
        &three_levels(),
        &mut reg,
        &amending(),
        &mut mint,
        LATER,
    );
    assert!(again.commands.is_empty(), "{:?}", shape(&again));
    assert_eq!(reg.get(bid1).unwrap().removal(), None);
    sink.take();
    reg.apply_update(&filled(bid1, 1), key(20));
    let seen = sink.take();
    assert_eq!((seen[0].tag, seen[0].removal), (Some(BID1_TAG), None));
}

#[test]
fn a_removal_cancel_whose_submit_fails_before_it_is_sent_leaves_no_reason_on_the_orders_end() {
    // RB163-5 on PR #163: a gateway whose submit fails reports the cancel not sent without
    // ever reporting it sent (fbc-examples link.rs). No cancel is in flight, so the order
    // keeps no reason, and the venue's later cancel (cancel-on-disconnect, Stop) carries none.
    let mut reg = quoting(WIDE, 0);
    let sink = Verdicts::default();
    reg.set_audit_sink(Box::new(sink.clone()));
    let mut mint = mint();
    let mut p = planner();
    open_book(&mut p, &three_levels(), &mut reg, &amending(), &mut mint);
    let bid1 = p.order_at(ACCT, INST, Side::Buy, 1).unwrap();
    let plan = p
        .plan(
            &bid1_removed(),
            &mut reg,
            &amending(),
            ACCT,
            &mut mint,
            LATER,
        )
        .unwrap();
    assert_eq!(
        shape(&plan),
        vec![(
            Stage::Cancel,
            "cancel",
            Side::Buy,
            1,
            BID1_TAG,
            Some(BID_GONE)
        )]
    );
    drop(plan);
    assert_eq!(reg.get(bid1).unwrap().removal(), Some(BID_GONE));
    let not_sent = SubmitOutcome::NotSent(fbc_core::NotSentReason::Disconnected);
    cancel_answered(&mut reg, bid1, RpcId(1_000), not_sent);
    assert_eq!(reg.get(bid1).unwrap().removal(), None);
    sink.take();
    reg.apply_update(
        &update(
            bid1,
            Side::Buy,
            &venue_id(Side::Buy, 1),
            VenueOrderState::Canceled(fbc_core::CancelReason::Disconnect),
        ),
        key(20),
    );
    let ended = sink.take();
    assert_eq!((ended[0].tag, ended[0].removal), (Some(BID1_TAG), None));
}

#[test]
fn a_removal_cancel_whose_submit_fails_keeps_the_reason_of_a_cancel_still_in_flight() {
    // The other side of RB163-5: an earlier removal cancel is in flight when a later one's
    // submit fails; the cancel in flight may still end the order, so its reason is kept.
    let mut reg = quoting(WIDE, 0);
    let mut mint = mint();
    let mut p = planner();
    open_book(&mut p, &three_levels(), &mut reg, &amending(), &mut mint);
    let bid1 = p.order_at(ACCT, INST, Side::Buy, 1).unwrap();
    plan_and_send(
        &mut p,
        &bid1_removed(),
        &mut reg,
        &amending(),
        &mut mint,
        LATER,
    );
    assert_eq!(reg.get(bid1).unwrap().removal(), Some(BID_GONE));
    let not_sent = SubmitOutcome::NotSent(fbc_core::NotSentReason::Disconnected);
    cancel_answered(&mut reg, bid1, RpcId(2_000), not_sent);
    assert_eq!(reg.get(bid1).unwrap().removal(), Some(BID_GONE));
}
