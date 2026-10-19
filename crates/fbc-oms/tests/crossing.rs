//! The DesiredBook's price-capped crossing intent (FBC-7aw4; decisions 0005, 0012, 0013,
//! 0106): a per-side reduce-only exit that crosses, with a price cap and a size, for a
//! consumer's disaster stop, news FLATTEN window or operator Flatten.
//!
//! - It is planned as one reduce-only order priced at its cap: an immediate-or-cancel order
//!   where the venue's `OrderCaps` declare one with reduce-only, otherwise a good-till-cancelled
//!   limit at the cap followed by its cancel (FBC-01w's synthetic IOC).
//! - Its size is clipped so the position never crosses zero (the position less the other
//!   orders resting that reduce it on its side); nothing is planned while the position is flat,
//!   unknown or on the intent's side.
//! - It is refused in Killed and Cancel-only, admitted in Exit and Quoting through FBC-7gl's
//!   admission, and both caps still apply.
//! - At most one order is planned per attempt: an order that ended (filled, expired, or
//!   refused by the venue, a price band's or a minimum notional's refusal included) is not
//!   crossed again until the consumer names a new attempt.
//!
//! The values are the owner's first test values: a $50 inventory cap on a synthetic market
//! where one lot is worth $1, so 50 lots, and a wide resting cap unless a test judges it.

#[path = "common/arm.rs"]
mod arm;
mod common;

use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use arm::{leases, start};
use common::{fill, fill_id, lease_dir, lots, order_caps, rejected, vid};
use fbc_core::{
    AccountKey, AckLevel, Bps, CancelReason, Channel, CidMatch, CidMint, ClientOrderId, ExchNs,
    ExchTsKind, Feature, FillIdent, InstrumentId, ItemRef, MonoNs, Namespace, NamespaceLease,
    NewOrder, OrderCaps, OrderKind, OrderUpdate, RefKind, RejectKind, RpcId, Side, SignedLots,
    SubmitOutcome, TagSet, Ticks, Tif, VenueCommand, VenueOrderState, WallNs,
};
use fbc_oms::{
    Admission, CROSSING_LEVEL, CapRefusal, CrossingIntent, DesiredBook, DesiredQuote, Downgrade,
    ExecutionPlanner, ExitRefusal, FillLedger, FillTime, HeldReason, LadderConfig, LedgerConfig,
    MarketCapsConfig, OmsError, OrderKey, OrderOp, Plan, PlanRefusal, Planned, PlannerConfig,
    PreTradeCaps, ReduceFallback, Registry, ResyncSnapshot, Stage, StateRefusal,
};

const INST: InstrumentId = InstrumentId::new(1);
const ACCT: AccountKey = AccountKey::new(1);
/// The owner's first inventory cap, in lots of $1.
const CAP: i64 = 50;
/// A resting cap past any order of these tests.
const WIDE: i64 = 1_000_000;
const MIN_AGE: Duration = Duration::from_millis(100);
const LATER: MonoNs = MonoNs(1_000_000_000);
const LATER2: MonoNs = MonoNs(2_000_000_000);
const LATER3: MonoNs = MonoNs(3_000_000_000);

fn mint() -> CidMint {
    static NEXT: AtomicU16 = AtomicU16::new(700);
    let ns = Namespace::new(NEXT.fetch_add(1, Ordering::Relaxed));
    let lease = NamespaceLease::acquire(&lease_dir(), AccountKey::new(2), ns).unwrap();
    CidMint::new(lease, 0, 0, WallNs(0))
}

fn planner() -> ExecutionPlanner {
    ExecutionPlanner::new(PlannerConfig::new(Bps(2.0), lots(2), MIN_AGE).unwrap())
}

/// A venue taking GTC and IOC limits on the public book, post-only and reduce-only, refusing
/// each of `conflicts` together; no amend, cancels by venue id once acknowledged.
fn ioc_venue(conflicts: &[(Feature, Feature)]) -> OrderCaps {
    OrderCaps {
        tifs: TagSet::of(&[Tif::Gtc, Tif::Ioc]),
        flag_conflicts: conflicts.to_vec(),
        ..order_caps()
    }
}

/// A venue taking GTC limits only (no IOC), that cancels by client id before the
/// acknowledgement.
fn gtc_venue_cancelling_early() -> OrderCaps {
    let caps = OrderCaps {
        cancel_refs: TagSet::of(&[RefKind::Venue, RefKind::Client]),
        cancel_before_ack: true,
        ..order_caps()
    };
    assert!(!caps.tifs.contains(Tif::Ioc));
    caps
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

/// A named registry under the inventory cap and a wide resting cap on `INST`, seeded at `pos`
/// by its first trustworthy resync, armed and Quoting.
fn quoting(pos: i64) -> Registry {
    quoting_resting(pos, WIDE)
}

/// [`quoting`] under the resting cap `resting` per side.
fn quoting_resting(pos: i64, resting: i64) -> Registry {
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
    reg.resync(&ladder_cfg(), &order_caps(), &snap, key(1))
        .unwrap();
    start(&mut reg, INST);
    reg
}

/// Moves the Quoting market `INST` to Exit by the owner's Flatten.
fn flatten(reg: &mut Registry) {
    reg.disarm(INST);
    let given = leases(reg, INST);
    reg.flatten(INST, given).unwrap();
}

fn key(n: u64) -> OrderKey {
    OrderKey {
        venue: None,
        ingest: n,
    }
}

/// A crossing intent of `qty` lots capped at `cap`, tagged 7, for the consumer's `attempt`.
fn cross(cap: i64, qty: i64, attempt: u64) -> CrossingIntent {
    CrossingIntent {
        cap: Ticks(cap),
        qty: lots(qty),
        tag: 7,
        attempt,
    }
}

/// A GTC limit on the public book of `qty` at `px`, neither post-only nor reduce-only.
fn limit(px: i64, qty: i64) -> DesiredQuote {
    DesiredQuote {
        px: Ticks(px),
        qty: lots(qty),
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: false,
        reduce_only: false,
        reducing: false,
        tag: 0,
        reduce_fallback: ReduceFallback::KeepReduceOnly,
    }
}

/// `q` reduce-only and classified reducing.
fn reduce_only(q: DesiredQuote) -> DesiredQuote {
    DesiredQuote {
        reduce_only: true,
        reducing: true,
        ..q
    }
}

fn book() -> DesiredBook {
    DesiredBook::new(INST)
}

/// The place `p` carries.
fn placed(p: &Planned) -> &NewOrder {
    match p.auth.command() {
        VenueCommand::Place(o) => o,
        other => panic!("expected a place, got {other:?}"),
    }
}

/// Each command's stage, kind, side, level and whether it is the crossing intent's.
fn shape(plan: &Plan) -> Vec<(Stage, &'static str, Side, u16, bool)> {
    plan.commands
        .iter()
        .map(|p| {
            let kind = match p.auth.command() {
                VenueCommand::Place(_) => "place",
                VenueCommand::Amend(_) => "amend",
                VenueCommand::Cancel(_) => "cancel",
                other => panic!("the planner built {other:?}"),
            };
            (p.stage, kind, p.side, p.level, p.crossing)
        })
        .collect()
}

/// The crossing levels refused this pass, and why.
fn refused_crossing(plan: &Plan) -> Vec<(Side, PlanRefusal)> {
    plan.refused
        .iter()
        .filter(|r| r.crossing)
        .map(|r| {
            assert_eq!(r.level, CROSSING_LEVEL);
            (r.side, r.why.clone())
        })
        .collect()
}

/// Plans `desired` and reports every cancel sent, as a gateway would.
fn plan_and_send(
    planner: &mut ExecutionPlanner,
    desired: &DesiredBook,
    reg: &mut Registry,
    caps: &OrderCaps,
    mint: &mut CidMint,
    now: MonoNs,
) -> Plan {
    let plan = planner.plan(desired, reg, caps, ACCT, mint, now).unwrap();
    for (n, p) in plan.commands.iter().enumerate() {
        if let VenueCommand::Cancel(_) = p.auth.command() {
            let rpc = RpcId(1_000 + now.0 + n as u64);
            assert!(reg.cancel_sent(p.cid, rpc, now).unwrap());
        }
    }
    plan
}

/// Acknowledges the placement of `c` under the venue id `v`: Open.
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

/// Ends our order `c` on `side` (venue id `v`) in `state`, under the ordering key `n`.
fn ended(
    reg: &mut Registry,
    c: ClientOrderId,
    side: Side,
    v: &str,
    state: VenueOrderState,
    n: u64,
) {
    let u = OrderUpdate {
        cid: Some(CidMatch::Ours(c)),
        vid: Some(vid(v)),
        inst: INST,
        side,
        state,
        cum_filled: reg.get(c).unwrap().filled(),
        px: None,
        qty: None,
        post_only: None,
        reduce_only: None,
    };
    reg.apply_update(&u, key(n));
    assert!(reg.get(c).unwrap().state().is_terminal());
}

/// An IOC's unfilled rest, cancelled by the venue.
fn expired() -> VenueOrderState {
    VenueOrderState::Canceled(CancelReason::Unfilled)
}

fn ledger() -> FillLedger {
    FillLedger::new(
        LedgerConfig {
            max_age: Duration::from_secs(3600),
            max_entries: 1_000,
        },
        WallNs(0),
    )
    .unwrap()
}

/// Applies a fill of `qty` on `side` of our order `c`, named `fid`.
fn fill_of(
    reg: &mut Registry,
    l: &mut FillLedger,
    c: ClientOrderId,
    side: Side,
    qty: i64,
    fid: &str,
) {
    let f = fill(
        Some(c),
        FillIdent::Venue {
            fill: fill_id(fid),
            vid: None,
            cum_after: None,
        },
        side,
        qty,
        false,
    );
    let time = Some(FillTime {
        exch: ExchNs(1_500),
        kind: ExchTsKind::MatchingEngine,
        aligned: WallNs(1_500),
    });
    match l.admit(&f, time, MonoNs(1_100)) {
        Admission::Apply(a) => {
            reg.apply_fill(a).unwrap();
        }
        other => panic!("expected the fill accepted, got {other:?}"),
    }
}

/// The crossing order's wire shape: a reduce-only limit at `cap` of `qty`, on the public book,
/// never post-only, classified reducing.
fn assert_crossing(o: &NewOrder, side: Side, cap: i64, qty: i64, tif: Tif) {
    assert_eq!(o.side, side);
    assert_eq!(o.kind, OrderKind::Limit { px: Ticks(cap) });
    assert_eq!(o.qty, lots(qty));
    assert_eq!(o.tif, tif);
    assert_eq!(o.channel, Channel::Public);
    assert!(o.reduce_only && o.reducing && !o.post_only, "{o:?}");
}

#[test]
fn a_crossing_intent_is_one_reduce_only_ioc_at_its_cap_where_the_caps_declare_ioc() {
    // Long 10: a sell crossing of 10 capped at 9_900, on a venue taking IOC with reduce-only.
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let caps = ioc_venue(&[]);
    let desired = book().with_crossing(Side::Sell, cross(9_900, 10, 1));
    assert_eq!(desired.crossing(Side::Sell), Some(&cross(9_900, 10, 1)));
    assert_eq!(desired.crossing(Side::Buy), None);
    let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(0));
    assert_eq!(
        shape(&plan),
        vec![(Stage::Reducing, "place", Side::Sell, CROSSING_LEVEL, true)]
    );
    let c = plan.commands[0].cid;
    assert_crossing(placed(&plan.commands[0]), Side::Sell, 9_900, 10, Tif::Ioc);
    assert_eq!(plan.commands[0].tag, Some(7));
    assert!(plan.commands[0].downgrades.is_empty());
    assert!(plan.refused.is_empty() && plan.awaiting_ack.is_empty());
    assert_eq!(p.crossing_order(ACCT, INST, Side::Sell), Some(c));
    assert_eq!(p.crossing_order(ACCT, INST, Side::Buy), None);
    // The levels are untouched: the crossing order is at no level.
    assert_eq!(p.order_at(ACCT, INST, Side::Sell, 0), None);
    // While it is live, nothing more is built for it: never a second order, an amend or a
    // replace, even with its cap moved.
    let moved = book().with_crossing(Side::Sell, cross(9_890, 10, 1));
    let again = plan_and_send(&mut p, &moved, &mut reg, &caps, &mut mint, LATER);
    assert!(again.commands.is_empty(), "{:?}", shape(&again));
    let held: Vec<_> = again
        .held
        .iter()
        .map(|h| (h.side, h.crossing, h.cid))
        .collect();
    assert_eq!(held, vec![(Side::Sell, true, c)]);
    ack(&mut reg, c, "v1");
    let acked = plan_and_send(&mut p, &moved, &mut reg, &caps, &mut mint, LATER2);
    assert!(acked.commands.is_empty(), "{:?}", shape(&acked));
    assert_eq!(acked.held[0].why, HeldReason::Crossing);
    // Its unfilled rest expired, the same attempt is never crossed again.
    ended(&mut reg, c, Side::Sell, "v1", expired(), 2);
    let after = plan_and_send(&mut p, &moved, &mut reg, &caps, &mut mint, LATER3);
    assert!(after.commands.is_empty(), "{:?}", shape(&after));
    assert!(after.refused.is_empty() && after.held.is_empty());
    assert_eq!(p.crossing_order(ACCT, INST, Side::Sell), None);
}

#[test]
fn a_crossing_intent_without_ioc_is_a_limit_at_its_cap_followed_by_its_cancel() {
    // A venue with no IOC at all, and one that refuses IOC with reduce-only: both get the
    // synthetic IOC, a GTC limit at the cap and its cancel in the same pass.
    let no_ioc = gtc_venue_cancelling_early();
    let refuses = OrderCaps {
        cancel_refs: TagSet::of(&[RefKind::Venue, RefKind::Client]),
        cancel_before_ack: true,
        ..ioc_venue(&[(Feature::ReduceOnly, Feature::Ioc)])
    };
    for caps in [no_ioc, refuses] {
        // Short 6: a buy crossing capped at 10_100.
        let mut reg = quoting(-6);
        let mut mint = mint();
        let mut p = planner();
        let desired = book().with_crossing(Side::Buy, cross(10_100, 6, 1));
        let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(0));
        assert_eq!(
            shape(&plan),
            vec![
                (Stage::Reducing, "place", Side::Buy, CROSSING_LEVEL, true),
                (Stage::Cancel, "cancel", Side::Buy, CROSSING_LEVEL, true),
            ]
        );
        assert_crossing(placed(&plan.commands[0]), Side::Buy, 10_100, 6, Tif::Gtc);
        assert_eq!(plan.commands[0].downgrades, vec![Downgrade::SynthIoc]);
        assert_eq!(plan.commands[1].cid, plan.commands[0].cid);
        assert_eq!(plan.commands[1].tag, Some(7));
        assert_eq!(plan.commands[1].removal, None);
    }
}

#[test]
fn a_synthetic_crossing_on_a_venue_cancelling_after_the_ack_is_cancelled_once_acknowledged() {
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let caps = order_caps();
    assert!(!caps.tifs.contains(Tif::Ioc) && !caps.cancel_before_ack);
    let desired = book().with_crossing(Side::Sell, cross(9_900, 4, 1));
    let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(0));
    assert_eq!(
        shape(&plan),
        vec![(Stage::Reducing, "place", Side::Sell, CROSSING_LEVEL, true)]
    );
    let c = plan.commands[0].cid;
    assert_eq!(plan.awaiting_ack, vec![c]);
    // Acknowledged, the next pass cancels it, the intent still there.
    ack(&mut reg, c, "v1");
    let next = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, LATER);
    assert_eq!(
        shape(&next),
        vec![(Stage::Cancel, "cancel", Side::Sell, CROSSING_LEVEL, true)]
    );
    assert_eq!(next.commands[0].cid, c);
    // Terminal, the same attempt is not crossed again.
    ended(
        &mut reg,
        c,
        Side::Sell,
        "v1",
        VenueOrderState::Canceled(CancelReason::Requested),
        2,
    );
    let after = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, LATER2);
    assert!(after.commands.is_empty(), "{:?}", shape(&after));
}

#[test]
fn a_crossing_intent_is_clipped_so_the_position_never_crosses_zero() {
    let caps = ioc_venue(&[]);
    // Long 4, a sell crossing of 10: 4.
    let mut reg = quoting(4);
    let mut mint = mint();
    let mut p = planner();
    let desired = book().with_crossing(Side::Sell, cross(9_900, 10, 1));
    let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(0));
    assert_crossing(placed(&plan.commands[0]), Side::Sell, 9_900, 4, Tif::Ioc);
    // Short 3, a buy crossing of 10: 3.
    let mut reg = quoting(-3);
    let mut p = planner();
    let desired = book().with_crossing(Side::Buy, cross(10_100, 10, 1));
    let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(0));
    assert_crossing(placed(&plan.commands[0]), Side::Buy, 10_100, 3, Tif::Ioc);
    // Long 10 with a reduce-only ask of 4 resting at level 0: the crossing takes the 6 left,
    // the reducer built first in the same pass counted.
    let mut reg = quoting(10);
    let mut p = planner();
    let desired = book()
        .with(Side::Sell, 0, reduce_only(limit(10_010, 4)))
        .with_crossing(Side::Sell, cross(9_900, 10, 1));
    let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(0));
    assert_eq!(
        shape(&plan),
        vec![
            (Stage::Reducing, "place", Side::Sell, 0, false),
            (Stage::Reducing, "place", Side::Sell, CROSSING_LEVEL, true),
        ]
    );
    assert_crossing(placed(&plan.commands[1]), Side::Sell, 9_900, 6, Tif::Ioc);
    // A crossing smaller than the position is sent at its size.
    let mut reg = quoting(10);
    let mut p = planner();
    let desired = book().with_crossing(Side::Sell, cross(9_900, 3, 1));
    let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(0));
    assert_crossing(placed(&plan.commands[0]), Side::Sell, 9_900, 3, Tif::Ioc);
}

#[test]
fn a_crossing_intent_counts_the_fills_the_inventory_does_not_hold_yet() {
    // Long 10, a reduce-only ask of 10 resting; the venue reports 6 filled on an order update
    // before the fill event, and the ask is then cancelled: 4 are left to reduce, not 10.
    let caps = ioc_venue(&[]);
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let first = book().with(Side::Sell, 0, reduce_only(limit(10_010, 10)));
    let plan = plan_and_send(&mut p, &first, &mut reg, &caps, &mut mint, MonoNs(0));
    let ask = plan.commands[0].cid;
    ack(&mut reg, ask, "v1");
    let u = OrderUpdate {
        cid: Some(CidMatch::Ours(ask)),
        vid: Some(vid("v1")),
        inst: INST,
        side: Side::Sell,
        state: VenueOrderState::Canceled(CancelReason::Requested),
        cum_filled: lots(6),
        px: None,
        qty: None,
        post_only: None,
        reduce_only: None,
    };
    reg.apply_update(&u, key(2));
    assert_eq!(reg.inventory(INST), SignedLots(10));
    let desired = book().with_crossing(Side::Sell, cross(9_900, 10, 1));
    let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, LATER);
    assert_crossing(placed(&plan.commands[0]), Side::Sell, 9_900, 4, Tif::Ioc);
}

#[test]
fn nothing_is_planned_for_a_crossing_intent_while_flat_or_on_the_side_that_adds() {
    let caps = ioc_venue(&[]);
    // Flat: neither side reduces.
    let mut reg = quoting(0);
    let mut mint = mint();
    let mut p = planner();
    let desired = book()
        .with_crossing(Side::Sell, cross(9_900, 5, 1))
        .with_crossing(Side::Buy, cross(10_100, 5, 1));
    let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(0));
    assert!(plan.commands.is_empty(), "{:?}", shape(&plan));
    assert_eq!(
        refused_crossing(&plan),
        vec![
            (Side::Buy, PlanRefusal::NoRoomToReduce),
            (Side::Sell, PlanRefusal::NoRoomToReduce),
        ]
    );
    // Long 10: a buy crossing adds, so nothing is built for it; the sell is.
    let mut reg = quoting(10);
    let mut p = planner();
    let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(0));
    assert_eq!(
        shape(&plan),
        vec![(Stage::Reducing, "place", Side::Sell, CROSSING_LEVEL, true)]
    );
    assert_eq!(
        refused_crossing(&plan),
        vec![(Side::Buy, PlanRefusal::NoRoomToReduce)]
    );
    // Long 10 with reduce-only asks resting for all of it: nothing is left to cross.
    let mut reg = quoting(10);
    let mut p = planner();
    let full = book().with(Side::Sell, 0, reduce_only(limit(10_010, 10)));
    plan_and_send(&mut p, &full, &mut reg, &caps, &mut mint, MonoNs(0));
    let both = full.with_crossing(Side::Sell, cross(9_900, 5, 1));
    let plan = plan_and_send(&mut p, &both, &mut reg, &caps, &mut mint, LATER);
    assert!(plan.commands.is_empty(), "{:?}", shape(&plan));
    assert_eq!(
        refused_crossing(&plan),
        vec![(Side::Sell, PlanRefusal::NoRoomToReduce)]
    );
    // An intent at zero lots is no intent.
    let mut reg = quoting(10);
    let mut p = planner();
    let zero = book().with_crossing(Side::Sell, cross(9_900, 0, 1));
    let plan = plan_and_send(&mut p, &zero, &mut reg, &caps, &mut mint, MonoNs(0));
    assert!(plan.commands.is_empty() && plan.refused.is_empty());
}

#[test]
fn a_crossing_intent_flat_then_long_is_crossed_once_the_position_has_a_size() {
    // An intent left in the book while flat is planned at the first pass the position reduces:
    // nothing was sent for its attempt.
    let caps = ioc_venue(&[]);
    let mut reg = quoting(0);
    let mut mint = mint();
    let mut p = planner();
    let mut l = ledger();
    let bid = book().with(Side::Buy, 0, limit(10_000, 3));
    let plan = plan_and_send(&mut p, &bid, &mut reg, &caps, &mut mint, MonoNs(0));
    let b = plan.commands[0].cid;
    ack(&mut reg, b, "b1");
    let desired = bid.clone().with_crossing(Side::Sell, cross(9_900, 10, 1));
    let flat = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, LATER);
    assert!(flat.commands.is_empty(), "{:?}", shape(&flat));
    // The bid fills: long 3, and the consumer stops bidding.
    fill_of(&mut reg, &mut l, b, Side::Buy, 3, "f1");
    let desired = book().with_crossing(Side::Sell, cross(9_900, 10, 1));
    let long = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, LATER2);
    assert_eq!(
        shape(&long),
        vec![(Stage::Reducing, "place", Side::Sell, CROSSING_LEVEL, true)]
    );
    assert_crossing(placed(&long.commands[0]), Side::Sell, 9_900, 3, Tif::Ioc);
}

#[test]
fn a_crossing_intent_is_refused_in_killed_and_cancel_only() {
    for kill in [false, true] {
        let mut reg = quoting(10);
        let mut mint = mint();
        let mut p = planner();
        let state = if kill {
            reg.kill(INST);
            StateRefusal::Killed(INST)
        } else {
            reg.disarm(INST);
            StateRefusal::CancelOnly(INST)
        };
        let desired = book().with_crossing(Side::Sell, cross(9_900, 10, 1));
        let plan = plan_and_send(
            &mut p,
            &desired,
            &mut reg,
            &ioc_venue(&[]),
            &mut mint,
            LATER,
        );
        assert!(plan.commands.is_empty(), "{:?}", shape(&plan));
        assert_eq!(
            refused_crossing(&plan),
            vec![(Side::Sell, PlanRefusal::Place(OmsError::State(state)))]
        );
        assert_eq!(p.crossing_order(ACCT, INST, Side::Sell), None);
        assert!(reg.is_empty());
    }
}

#[test]
fn a_crossing_intent_refused_by_the_state_is_crossed_once_the_market_admits_it() {
    // Refused attempts send nothing, so the same attempt is planned once the market is
    // started again.
    let caps = ioc_venue(&[]);
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    reg.disarm(INST);
    let desired = book().with_crossing(Side::Sell, cross(9_900, 10, 1));
    let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(0));
    assert!(plan.commands.is_empty());
    start(&mut reg, INST);
    let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, LATER);
    assert_eq!(
        shape(&plan),
        vec![(Stage::Reducing, "place", Side::Sell, CROSSING_LEVEL, true)]
    );
}

#[test]
fn a_crossing_intent_is_admitted_in_exit_on_the_reducing_side_only() {
    let caps = ioc_venue(&[]);
    // Long 10, Flatten: the sell crossing is built; a buy crossing adds and builds nothing.
    let mut reg = quoting(10);
    flatten(&mut reg);
    let mut mint = mint();
    let mut p = planner();
    let desired = book()
        .with_crossing(Side::Sell, cross(9_900, 10, 1))
        .with_crossing(Side::Buy, cross(10_100, 10, 1));
    let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(0));
    assert_eq!(
        shape(&plan),
        vec![(Stage::Reducing, "place", Side::Sell, CROSSING_LEVEL, true)]
    );
    assert_crossing(placed(&plan.commands[0]), Side::Sell, 9_900, 10, Tif::Ioc);
    assert_eq!(
        refused_crossing(&plan),
        vec![(Side::Buy, PlanRefusal::NoRoomToReduce)]
    );
    // Short 5 in Exit: the buy crossing.
    let mut reg = quoting(-5);
    flatten(&mut reg);
    let mut p = planner();
    let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(0));
    assert_eq!(
        shape(&plan),
        vec![(Stage::Reducing, "place", Side::Buy, CROSSING_LEVEL, true)]
    );
    assert_crossing(placed(&plan.commands[0]), Side::Buy, 10_100, 5, Tif::Ioc);
}

#[test]
fn a_crossing_intent_in_exit_beside_an_ordinary_order_on_its_side_is_refused_by_exit() {
    // Long 10 while Quoting: an ordinary ask of 8 rests (it adds nothing to the clip, as it is
    // not a reducer). In Exit, the crossing of 10 would leave the side's orders crossing zero,
    // so Exit's admission refuses it: Exit's rule is never bypassed.
    let caps = ioc_venue(&[]);
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let ask = book().with(Side::Sell, 0, limit(10_010, 8));
    let plan = plan_and_send(&mut p, &ask, &mut reg, &caps, &mut mint, MonoNs(0));
    ack(&mut reg, plan.commands[0].cid, "a1");
    flatten(&mut reg);
    let desired = ask.with_crossing(Side::Sell, cross(9_900, 10, 1));
    let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, LATER);
    assert!(
        !shape(&plan).iter().any(|s| s.4),
        "no crossing command: {:?}",
        shape(&plan)
    );
    assert_eq!(
        refused_crossing(&plan),
        vec![(
            Side::Sell,
            PlanRefusal::Place(OmsError::State(StateRefusal::Exit(
                ExitRefusal::CrossesZero {
                    inst: INST,
                    side: Side::Sell,
                    total: Some(lots(18)),
                    position: lots(10),
                }
            )))
        )]
    );
}

#[test]
fn a_crossing_intent_is_refused_by_the_resting_cap_and_the_inventory_cap() {
    let caps = ioc_venue(&[]);
    // The resting cap: 5 lots per side, a crossing of 10 on a long of 10.
    let mut reg = quoting_resting(10, 5);
    let mut mint = mint();
    let mut p = planner();
    let desired = book().with_crossing(Side::Sell, cross(9_900, 10, 1));
    let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(0));
    assert!(plan.commands.is_empty(), "{:?}", shape(&plan));
    assert_eq!(
        refused_crossing(&plan),
        vec![(
            Side::Sell,
            PlanRefusal::Place(OmsError::Capped(CapRefusal::RestingCap {
                inst: INST,
                side: Side::Sell,
                resting: Some(lots(10)),
                cap: lots(5),
            }))
        )]
    );
    // Within the resting cap, the same intent sized 5 is built.
    let within = book().with_crossing(Side::Sell, cross(9_900, 5, 1));
    let plan = plan_and_send(&mut p, &within, &mut reg, &caps, &mut mint, LATER);
    assert_eq!(
        shape(&plan),
        vec![(Stage::Reducing, "place", Side::Sell, CROSSING_LEVEL, true)]
    );
    // The inventory cap: long 40 with ordinary asks of 80 resting, the worst case on the ask
    // side is short 40; a crossing of 40 would make it short 80, past the cap of 50.
    let mut reg = quoting(40);
    let mut p = planner();
    let asks = book().with(Side::Sell, 0, limit(10_010, 80));
    plan_and_send(&mut p, &asks, &mut reg, &caps, &mut mint, MonoNs(0));
    let desired = asks.with_crossing(Side::Sell, cross(9_900, 40, 1));
    let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, LATER);
    assert!(plan.commands.is_empty(), "{:?}", shape(&plan));
    assert_eq!(
        refused_crossing(&plan),
        vec![(
            Side::Sell,
            PlanRefusal::Place(OmsError::Capped(CapRefusal::InventoryCap {
                inst: INST,
                side: Side::Sell,
                worst: Some(lots(80)),
                cap: lots(CAP),
            }))
        )]
    );
}

#[test]
fn a_crossing_intent_on_a_venue_without_reduce_only_is_never_sent() {
    let caps = OrderCaps {
        reduce_only: false,
        ..ioc_venue(&[])
    };
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let desired = book().with_crossing(Side::Sell, cross(9_900, 10, 1));
    let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(0));
    assert!(plan.commands.is_empty(), "{:?}", shape(&plan));
    assert_eq!(
        refused_crossing(&plan),
        vec![(Side::Sell, PlanRefusal::ReduceOnlyUnsupported)]
    );
    assert!(reg.is_empty());
}

#[test]
fn a_crossing_order_refused_by_the_venue_is_not_crossed_again_until_a_new_attempt() {
    // Decisions 0093 and 0096: a venue refuses an IOC priced past its price band, or a small
    // residual below its minimum notional judged at the mark, after accepting it; the order
    // ends with nothing filled. The same attempt is not sent again, so a refusal never loops;
    // a new attempt is crossed again.
    for kind in [RejectKind::Other, RejectKind::MinNotional] {
        let caps = ioc_venue(&[]);
        let mut reg = quoting(10);
        let mut mint = mint();
        let mut p = planner();
        let desired = book().with_crossing(Side::Sell, cross(9_900, 10, 1));
        let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(0));
        let c = plan.commands[0].cid;
        ack(&mut reg, c, "v1");
        ended(&mut reg, c, Side::Sell, "v1", rejected(kind), 2);
        for now in [LATER, LATER2] {
            let again = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, now);
            assert!(again.commands.is_empty(), "{kind:?}: {:?}", shape(&again));
        }
        let retry = book().with_crossing(Side::Sell, cross(9_950, 10, 2));
        let plan = plan_and_send(&mut p, &retry, &mut reg, &caps, &mut mint, LATER3);
        assert_eq!(
            shape(&plan),
            vec![(Stage::Reducing, "place", Side::Sell, CROSSING_LEVEL, true)]
        );
        assert_ne!(plan.commands[0].cid, c);
        assert_crossing(placed(&plan.commands[0]), Side::Sell, 9_950, 10, Tif::Ioc);
    }
}

#[test]
fn a_new_attempt_waits_for_the_previous_crossing_order_and_is_clipped_to_what_it_left() {
    // Long 10: attempt 1 crosses 10 and fills 4. Attempt 2 waits while that order is live,
    // then crosses the 6 left.
    let caps = ioc_venue(&[]);
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let mut l = ledger();
    let first = book().with_crossing(Side::Sell, cross(9_900, 10, 1));
    let plan = plan_and_send(&mut p, &first, &mut reg, &caps, &mut mint, MonoNs(0));
    let c = plan.commands[0].cid;
    ack(&mut reg, c, "v1");
    fill_of(&mut reg, &mut l, c, Side::Sell, 4, "f1");
    let second = book().with_crossing(Side::Sell, cross(9_890, 10, 2));
    let wait = plan_and_send(&mut p, &second, &mut reg, &caps, &mut mint, LATER);
    assert!(wait.commands.is_empty(), "{:?}", shape(&wait));
    assert_eq!(wait.held.len(), 1);
    assert_eq!(wait.held[0].why, HeldReason::Crossing);
    ended(&mut reg, c, Side::Sell, "v1", expired(), 3);
    let plan = plan_and_send(&mut p, &second, &mut reg, &caps, &mut mint, LATER2);
    assert_eq!(
        shape(&plan),
        vec![(Stage::Reducing, "place", Side::Sell, CROSSING_LEVEL, true)]
    );
    assert_crossing(placed(&plan.commands[0]), Side::Sell, 9_890, 6, Tif::Ioc);
}

#[test]
fn a_crossing_intent_withdrawn_while_its_order_is_live_cancels_it_with_the_sides_reason() {
    let caps = ioc_venue(&[]);
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let desired = book().with_crossing(Side::Sell, cross(9_900, 10, 1));
    let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(0));
    let c = plan.commands[0].cid;
    ack(&mut reg, c, "v1");
    let mut withdrawn = desired.with_removal(Side::Sell, Some(3));
    withdrawn.set_crossing(Side::Sell, None);
    assert_eq!(withdrawn.crossing(Side::Sell), None);
    let plan = plan_and_send(&mut p, &withdrawn, &mut reg, &caps, &mut mint, LATER);
    assert_eq!(
        shape(&plan),
        vec![(Stage::Cancel, "cancel", Side::Sell, CROSSING_LEVEL, true)]
    );
    assert_eq!(plan.commands[0].cid, c);
    assert_eq!(plan.commands[0].removal, Some(3));
    assert_eq!(plan.commands[0].tag, Some(7));
}

#[test]
fn a_crossing_intent_beside_an_emulated_exit_waits_for_its_cancel_and_is_not_clipped_by_it() {
    // Decision 0106 rule 7: long 10, an emulated maker exit of 8 resting on a venue refusing
    // post-only with reduce-only. The crossing of 10 is not clipped by the emulated order (a
    // force-close is never starved); it waits for that order's cancel, then crosses the 10.
    let caps = ioc_venue(&[(Feature::PostOnly, Feature::ReduceOnly)]);
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let maker = DesiredQuote {
        post_only: true,
        reduce_fallback: ReduceFallback::EmulateReduceOnly,
        ..reduce_only(limit(10_020, 8))
    };
    let first = book().with(Side::Sell, 0, maker);
    let plan = plan_and_send(&mut p, &first, &mut reg, &caps, &mut mint, MonoNs(0));
    let emulated = plan.commands[0].cid;
    assert_eq!(
        plan.commands[0].downgrades,
        vec![Downgrade::EmulatedReduceOnly]
    );
    ack(&mut reg, emulated, "e1");
    // The consumer escalates: the maker exit goes, the crossing comes.
    let escalate = book().with_crossing(Side::Sell, cross(9_900, 10, 1));
    let plan = plan_and_send(&mut p, &escalate, &mut reg, &caps, &mut mint, LATER);
    assert_eq!(
        shape(&plan),
        vec![(Stage::Cancel, "cancel", Side::Sell, 0, false)]
    );
    assert_eq!(plan.commands[0].cid, emulated);
    assert_eq!(
        refused_crossing(&plan),
        vec![(Side::Sell, PlanRefusal::AwaitsEmulatedCancel)]
    );
    ended(
        &mut reg,
        emulated,
        Side::Sell,
        "e1",
        VenueOrderState::Canceled(CancelReason::Requested),
        2,
    );
    let plan = plan_and_send(&mut p, &escalate, &mut reg, &caps, &mut mint, LATER2);
    assert_eq!(
        shape(&plan),
        vec![(Stage::Reducing, "place", Side::Sell, CROSSING_LEVEL, true)]
    );
    assert_crossing(placed(&plan.commands[0]), Side::Sell, 9_900, 10, Tif::Ioc);
}

#[test]
fn a_crossing_order_never_sent_leaves_its_attempt_to_be_planned_again() {
    // A place built and then not sent (the stream down, the rate budget spent) reached no
    // venue: the same attempt is planned again once the order is terminal, by a pass or after
    // an `on_fill` check freed it.
    for via_on_fill in [false, true] {
        let caps = ioc_venue(&[]);
        let mut reg = quoting(10);
        let mut mint = mint();
        let mut p = planner();
        let desired = book().with_crossing(Side::Sell, cross(9_900, 10, 1));
        let plan = p
            .plan(&desired, &mut reg, &caps, ACCT, &mut mint, MonoNs(0))
            .unwrap();
        let mut commands = plan.commands;
        let c = commands[0].cid;
        drop(commands.pop());
        let item = ItemRef {
            idx: 0,
            cid: Some(c),
            vid: None,
        };
        let not_sent = SubmitOutcome::NotSent(fbc_core::NotSentReason::Disconnected);
        reg.on_outcome(c, OrderOp::Place, &item, &not_sent, MonoNs(1))
            .unwrap();
        assert!(reg.get(c).unwrap().state().is_terminal());
        if via_on_fill {
            assert!(
                p.on_fill(&mut reg, &caps, ACCT, INST)
                    .unwrap()
                    .commands
                    .is_empty()
            );
            assert_eq!(p.crossing_order(ACCT, INST, Side::Sell), None);
        }
        let again = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, LATER);
        assert_eq!(
            shape(&again),
            vec![(Stage::Reducing, "place", Side::Sell, CROSSING_LEVEL, true)],
            "via on_fill: {via_on_fill}"
        );
        assert_ne!(again.commands[0].cid, c);
    }
}

#[test]
fn a_crossing_attempt_already_built_or_older_than_the_last_built_is_never_planned_again() {
    // Codex r4317142569: attempts 1, 2, then 1 again, each order terminal before the next: the
    // last is never planned, as an attempt not past the last one built is stale; a NotSent
    // order frees its own attempt only, never an older one.
    let caps = ioc_venue(&[]);
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let mut now = 0;
    let mut cross_once = |attempt: u64, p: &mut ExecutionPlanner, reg: &mut Registry| {
        now += 1_000_000_000;
        let desired = book().with_crossing(Side::Sell, cross(9_900, 2, attempt));
        plan_and_send(p, &desired, reg, &caps, &mut mint, MonoNs(now))
    };
    for (n, attempt) in [1, 2].into_iter().enumerate() {
        let plan = cross_once(attempt, &mut p, &mut reg);
        assert_eq!(plan.commands.len(), 1, "attempt {attempt}");
        let c = plan.commands[0].cid;
        let v = format!("v{attempt}");
        ack(&mut reg, c, &v);
        ended(&mut reg, c, Side::Sell, &v, expired(), 10 + n as u64);
    }
    let stale = cross_once(1, &mut p, &mut reg);
    assert!(stale.commands.is_empty(), "{:?}", shape(&stale));
    assert_eq!(
        refused_crossing(&stale),
        vec![(Side::Sell, PlanRefusal::StaleAttempt { last: 2 })]
    );
    // The last attempt built, standing, is silent: nothing more to do for it.
    let same = cross_once(2, &mut p, &mut reg);
    assert!(same.commands.is_empty() && same.refused.is_empty());
    // Attempt 3 is built and never sent: 3 is planned again, 1 and 2 still are not.
    let plan = cross_once(3, &mut p, &mut reg);
    let c = plan.commands[0].cid;
    drop(plan);
    let item = ItemRef {
        idx: 0,
        cid: Some(c),
        vid: None,
    };
    let not_sent = SubmitOutcome::NotSent(fbc_core::NotSentReason::RateBudget);
    reg.on_outcome(c, OrderOp::Place, &item, &not_sent, MonoNs(1))
        .unwrap();
    for older in [1, 2] {
        let stale = cross_once(older, &mut p, &mut reg);
        assert!(stale.commands.is_empty(), "{older}: {:?}", shape(&stale));
        assert_eq!(
            refused_crossing(&stale),
            vec![(Side::Sell, PlanRefusal::StaleAttempt { last: 3 })]
        );
    }
    let again = cross_once(3, &mut p, &mut reg);
    assert_eq!(
        shape(&again),
        vec![(Stage::Reducing, "place", Side::Sell, CROSSING_LEVEL, true)]
    );
}

#[test]
fn a_crossing_intent_is_never_sent_where_the_venue_can_neither_take_nor_emulate_an_ioc() {
    // Codex r4317142581: a synthetic IOC needs a GTC limit and a cancel that can name the
    // order once acknowledged (its venue id or our client id). Without either, the limit could
    // rest and fill later, so nothing is built.
    let no_gtc = OrderCaps {
        tifs: TagSet::of(&[Tif::Fok]),
        ..order_caps()
    };
    let no_cancel_ref = OrderCaps {
        cancel_refs: TagSet::of(&[]),
        ..order_caps()
    };
    let nonce_only_conflicting = OrderCaps {
        cancel_refs: TagSet::of(&[RefKind::PlacementNonce]),
        ..ioc_venue(&[(Feature::Ioc, Feature::ReduceOnly)])
    };
    for caps in [no_gtc, no_cancel_ref, nonce_only_conflicting] {
        let mut reg = quoting(10);
        let mut mint = mint();
        let mut p = planner();
        let desired = book().with_crossing(Side::Sell, cross(9_900, 10, 1));
        let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(0));
        assert!(plan.commands.is_empty(), "{caps:?}: {:?}", shape(&plan));
        assert_eq!(
            refused_crossing(&plan),
            vec![(Side::Sell, PlanRefusal::NoSyntheticIoc)]
        );
        assert!(reg.is_empty());
        // A reduce-only IOC quote at a level the venue would take only as a synthetic IOC is
        // refused the same way.
        let level = book().with(
            Side::Sell,
            0,
            DesiredQuote {
                tif: Tif::Ioc,
                ..reduce_only(limit(9_900, 4))
            },
        );
        let caps = OrderCaps {
            tifs: TagSet::of(&caps.tifs.iter().chain([Tif::Ioc]).collect::<Vec<_>>()),
            flag_conflicts: vec![(Feature::Ioc, Feature::ReduceOnly)],
            ..caps
        };
        let plan = plan_and_send(&mut p, &level, &mut reg, &caps, &mut mint, LATER);
        assert!(plan.commands.is_empty(), "{caps:?}: {:?}", shape(&plan));
        assert_eq!(plan.refused.len(), 1);
        assert_eq!(plan.refused[0].why, PlanRefusal::NoSyntheticIoc);
    }
}
