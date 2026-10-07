//! The execution planner (decision 0005's one planner, 0065): desired books planned against a
//! registry. Its commands come out cancels, then reducing orders, then amends, then adds, each
//! authorized; a change is an amend only where the venue's `OrderCaps` admit that amend, and
//! otherwise a cancel, the level then waiting for the old order's terminal state before the
//! new one is placed, the old order counted as resting and as exposure until it is terminal
//! and the new one from when it is built; a level whose order is PendingNew or Unknown is
//! occupied until it is terminal; and no place or amend is built for a market in Cancel-only
//! or Killed, or for an item over a cap, while cancels still are.
//!
//! The values are the owner's first test values: a $50 inventory cap on a synthetic market
//! where one lot is worth $1, so 50 lots; the resting cap is $11 per side, 11 lots, where a
//! test judges it, and wide otherwise.

#[path = "common/arm.rs"]
mod arm;
mod common;

use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use arm::{leases, start};
use common::{lease_dir, lots, order_caps, vid};
use fbc_core::{
    AccountKey, AckLevel, AmendAck, AmendCaps, AmendQty, Bps, CancelReason, Channel, CidMatch,
    CidMint, ClientOrderId, InstrumentId, ItemRef, Lots, MonoNs, Namespace, NamespaceLease,
    OrderCaps, OrderKind, OrderRef, OrderUpdate, RefKind, RpcId, Side, SignedLots, SubmitOutcome,
    TagSet, Ticks, Tif, VenueCommand, VenueOrderState, WallNs,
};
use fbc_oms::{
    AmendRefusal, CapRefusal, DesiredBook, DesiredQuote, ExecutionPlanner, ExitRefusal, HeldReason,
    LadderConfig, MarketCapsConfig, OmsError, OrdState, OrderKey, OrderOp, Plan, PlanRefusal,
    PlannerConfig, PlannerConfigError, PreTradeCaps, Refused, Registry, ResyncSnapshot, Stage,
    StateRefusal,
};

const INST: InstrumentId = InstrumentId::new(1);
const ACCT: AccountKey = AccountKey::new(1);
/// The owner's first inventory cap, in lots of $1.
const CAP: i64 = 50;
/// The owner's first resting cap per side, in lots of $1.
const RESTING: i64 = 11;
/// A resting cap past any order of the tests that judge the other rules.
const WIDE: i64 = 1_000_000;
/// The minimum age the tests configure.
const MIN_AGE: Duration = Duration::from_millis(100);
/// An instant past the minimum age of anything placed at `MonoNs(0)`.
const LATER: MonoNs = MonoNs(1_000_000_000);

/// A client-id mint of its own under a namespace lease of its own, so no two tests share one.
fn mint() -> CidMint {
    static NEXT: AtomicU16 = AtomicU16::new(100);
    let ns = Namespace::new(NEXT.fetch_add(1, Ordering::Relaxed));
    let lease = NamespaceLease::acquire(&lease_dir(), AccountKey::new(2), ns).unwrap();
    CidMint::new(lease, 0, 0, WallNs(0))
}

/// Replaced when the price moved 2 basis points (2 ticks at the tests' prices around 10 000)
/// or the resting quantity 2 lots, once 100 ms old.
fn config() -> PlannerConfig {
    PlannerConfig::new(Bps(2.0), lots(2), MIN_AGE).unwrap()
}

fn planner() -> ExecutionPlanner {
    ExecutionPlanner::new(config())
}

/// A venue that amends price and total, partly filled orders included, by venue id, keeping
/// the venue id; single cancels by venue id.
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

fn ladder_cfg() -> LadderConfig {
    LadderConfig::new(
        Duration::from_secs(1),
        Duration::ZERO,
        Duration::from_secs(10),
        1,
    )
    .unwrap()
}

/// A named registry under the inventory cap and the resting cap `resting` on `INST`, whose
/// first trustworthy resync showed no order and seeded `INST` at `pos`, armed and Quoting.
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
    reg.resync(&ladder_cfg(), &amending(), &snap, key(1))
        .unwrap();
    start(&mut reg, INST);
    reg
}

fn key(n: u64) -> OrderKey {
    OrderKey {
        venue: None,
        ingest: n,
    }
}

/// A post-only GTC limit quote on the public book of `qty` at `px`.
fn quote(px: i64, qty: i64) -> DesiredQuote {
    DesiredQuote {
        px: Ticks(px),
        qty: lots(qty),
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        reducing: false,
    }
}

/// `q` as an exit sends it: reduce-only and classified reducing.
fn reducing(q: DesiredQuote) -> DesiredQuote {
    DesiredQuote {
        reduce_only: true,
        reducing: true,
        ..q
    }
}

fn book() -> DesiredBook {
    DesiredBook::new(INST)
}

/// Each command's stage, kind and level, in the order planned.
fn shape(plan: &Plan) -> Vec<(Stage, &'static str, Side, u16)> {
    plan.commands
        .iter()
        .map(|p| {
            let kind = match p.auth.command() {
                VenueCommand::Place(_) => "place",
                VenueCommand::Amend(_) => "amend",
                VenueCommand::Cancel(_) => "cancel",
                other => panic!("the planner built {other:?}"),
            };
            assert_eq!(p.auth.market(), INST);
            assert_eq!(p.auth.account(), ACCT);
            (p.stage, kind, p.side, p.level)
        })
        .collect()
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

/// An update of our order `c` on `side`, venue id `v`, in `state` with `cum` filled.
fn update(c: ClientOrderId, side: Side, v: &str, state: VenueOrderState, cum: i64) -> OrderUpdate {
    OrderUpdate {
        cid: Some(CidMatch::Ours(c)),
        vid: Some(vid(v)),
        inst: INST,
        side,
        state,
        cum_filled: lots(cum),
        px: None,
        qty: None,
        post_only: None,
        reduce_only: None,
    }
}

/// Ends our order `c` on `side` (venue id `v`) cancelled.
fn cancelled(reg: &mut Registry, c: ClientOrderId, side: Side, v: &str, n: u64) {
    let u = update(
        c,
        side,
        v,
        VenueOrderState::Canceled(CancelReason::Requested),
        0,
    );
    reg.apply_update(&u, key(n));
    assert!(reg.get(c).unwrap().state().is_terminal());
}

/// Plans `desired` and reports every command sent as a gateway would: each cancel sent under a
/// request of its own.
fn plan_and_send(
    planner: &mut ExecutionPlanner,
    desired: &DesiredBook,
    reg: &mut Registry,
    caps: &OrderCaps,
    mint: &mut CidMint,
    now: MonoNs,
) -> Plan {
    let plan = planner.plan(desired, reg, caps, ACCT, mint, now);
    for (n, p) in plan.commands.iter().enumerate() {
        let rpc = RpcId(1_000 + n as u64);
        match p.auth.command() {
            VenueCommand::Cancel(_) => assert!(reg.cancel_sent(p.cid, rpc, now).unwrap()),
            VenueCommand::Amend(a) => {
                assert!(reg.amend_sent(p.cid, a.px, a.qty, rpc, now).unwrap())
            }
            _ => {}
        }
    }
    plan
}

/// Places `desired` at `MonoNs(0)` and acknowledges each order it placed, under venue ids
/// `v-<side>-<level>`.
fn open_book(
    planner: &mut ExecutionPlanner,
    desired: &DesiredBook,
    reg: &mut Registry,
    caps: &OrderCaps,
    mint: &mut CidMint,
) -> Plan {
    let plan = plan_and_send(planner, desired, reg, caps, mint, MonoNs(0));
    for p in &plan.commands {
        assert!(matches!(p.auth.command(), VenueCommand::Place(_)));
        ack(reg, p.cid, &venue_id(p.side, p.level));
    }
    plan
}

fn venue_id(side: Side, level: u16) -> String {
    format!("v-{side:?}-{level}")
}

fn held(plan: &Plan) -> Vec<(Side, u16, HeldReason)> {
    plan.held.iter().map(|h| (h.side, h.level, h.why)).collect()
}

fn refused(plan: &Plan) -> Vec<(Side, u16, PlanRefusal)> {
    plan.refused
        .iter()
        .map(|r: &Refused| (r.side, r.level, r.why.clone()))
        .collect()
}

#[test]
fn the_plan_comes_out_cancels_then_reducing_orders_then_amends_then_adds() {
    // Long 10: asks reduce. Bids at levels 0 and 1, and a reduce-only ask at level 1, resting.
    let mut reg = quoting(WIDE, 10);
    let mut mint = mint();
    let mut p = planner();
    let first = book()
        .with(Side::Buy, 0, quote(10_000, 5))
        .with(Side::Buy, 1, quote(9_990, 5))
        .with(Side::Sell, 1, reducing(quote(10_020, 3)));
    let opened = open_book(&mut p, &first, &mut reg, &amending(), &mut mint);
    // A first pass is only adds and reducing places, the reducing one first.
    assert_eq!(
        shape(&opened),
        vec![
            (Stage::Reducing, "place", Side::Sell, 1),
            (Stage::Add, "place", Side::Buy, 0),
            (Stage::Add, "place", Side::Buy, 1),
        ]
    );

    // The bid at level 0 moves (an amend), level 1 goes (a cancel), a bid at level 2 comes (an
    // add), a reduce-only ask at level 0 comes (a reducing place) and the reduce-only ask at
    // level 1 moves (a reducing amend).
    let next = book()
        .with(Side::Buy, 0, quote(9_995, 5))
        .with(Side::Buy, 2, quote(9_980, 4))
        .with(Side::Sell, 0, reducing(quote(10_010, 2)))
        .with(Side::Sell, 1, reducing(quote(10_015, 3)));
    let plan = p.plan(&next, &mut reg, &amending(), ACCT, &mut mint, LATER);
    assert_eq!(
        shape(&plan),
        vec![
            (Stage::Cancel, "cancel", Side::Buy, 1),
            (Stage::Reducing, "place", Side::Sell, 0),
            (Stage::Reducing, "amend", Side::Sell, 1),
            (Stage::Amend, "amend", Side::Buy, 0),
            (Stage::Add, "place", Side::Buy, 2),
        ]
    );
    assert!(plan.refused.is_empty() && plan.held.is_empty() && plan.awaiting_ack.is_empty());
    // Each command is the level's: the cancel names the old bid at level 1, the amend carries
    // the new price and total, the places the quotes' fields.
    let cmds: Vec<&VenueCommand> = plan.commands.iter().map(|c| c.auth.command()).collect();
    match cmds[0] {
        VenueCommand::Cancel(c) => {
            assert_eq!(
                c.target,
                OrderRef::Both(opened.commands[2].cid, vid("v-Buy-1"))
            );
        }
        other => panic!("expected a cancel, got {other:?}"),
    }
    match cmds[1] {
        VenueCommand::Place(o) => {
            assert_eq!(
                (o.side, o.qty, o.kind),
                (Side::Sell, lots(2), OrderKind::Limit { px: Ticks(10_010) })
            );
            assert!(o.reduce_only && o.reducing);
        }
        other => panic!("expected a place, got {other:?}"),
    }
    match cmds[2] {
        VenueCommand::Amend(a) => {
            assert_eq!((a.px, a.qty, a.reduce_only), (Ticks(10_015), lots(3), true));
        }
        other => panic!("expected an amend, got {other:?}"),
    }
    match cmds[3] {
        VenueCommand::Amend(a) => {
            assert_eq!(
                (a.side, a.px, a.qty, a.reducing),
                (Side::Buy, Ticks(9_995), lots(5), false)
            );
        }
        other => panic!("expected an amend, got {other:?}"),
    }
    match cmds[4] {
        VenueCommand::Place(o) => {
            assert_eq!(
                (o.side, o.qty, o.kind),
                (Side::Buy, lots(4), OrderKind::Limit { px: Ticks(9_980) })
            );
            assert!(o.post_only && !o.reduce_only && !o.reducing);
        }
        other => panic!("expected a place, got {other:?}"),
    }
    // The planner holds the new orders at their levels.
    assert_eq!(p.order_at(INST, Side::Sell, 0), Some(plan.commands[1].cid));
    assert_eq!(p.order_at(INST, Side::Buy, 2), Some(plan.commands[4].cid));
}

#[test]
fn a_change_amends_where_the_venues_order_caps_allow_it() {
    let mut reg = quoting(WIDE, 0);
    let mut mint = mint();
    let mut p = planner();
    let opened = open_book(
        &mut p,
        &book().with(Side::Buy, 0, quote(10_000, 5)),
        &mut reg,
        &amending(),
        &mut mint,
    );
    let c = opened.commands[0].cid;
    // Price and quantity both moved: one amend to the new price and total.
    let plan = plan_and_send(
        &mut p,
        &book().with(Side::Buy, 0, quote(10_004, 8)),
        &mut reg,
        &amending(),
        &mut mint,
        LATER,
    );
    assert_eq!(shape(&plan), vec![(Stage::Amend, "amend", Side::Buy, 0)]);
    assert_eq!(plan.commands[0].cid, c);
    // In flight: the level is held, nothing more is built for it.
    let again = p.plan(
        &book().with(Side::Buy, 0, quote(10_010, 8)),
        &mut reg,
        &amending(),
        ACCT,
        &mut mint,
        LATER,
    );
    assert!(again.commands.is_empty());
    assert_eq!(held(&again), vec![(Side::Buy, 0, HeldReason::InFlight)]);
}

#[test]
fn a_change_the_venue_cannot_amend_is_a_cancel_then_a_place_once_the_old_order_is_terminal() {
    let no_price = OrderCaps {
        amend: amending().amend.map(|a| AmendCaps { price: false, ..a }),
        ..order_caps()
    };
    let no_ref = OrderCaps {
        amend: amending().amend.map(|a| AmendCaps {
            refs: TagSet::of(&[]),
            ..a
        }),
        ..order_caps()
    };
    let cases: [(&str, OrderCaps, DesiredQuote); 4] = [
        ("no amend at all", order_caps(), quote(10_004, 5)),
        ("the price is not amendable", no_price, quote(10_004, 5)),
        ("no reference the amend can name", no_ref, quote(10_000, 8)),
        // The venue amends, but no amend changes an order's flags.
        (
            "the flags changed",
            amending(),
            DesiredQuote {
                tif: Tif::Ioc,
                ..quote(10_000, 5)
            },
        ),
    ];
    for (name, caps, moved) in cases {
        let mut reg = quoting(WIDE, 0);
        let mut mint = mint();
        let mut p = planner();
        let opened = open_book(
            &mut p,
            &book().with(Side::Buy, 0, quote(10_000, 5)),
            &mut reg,
            &caps,
            &mut mint,
        );
        let old = opened.commands[0].cid;
        let desired = book().with(Side::Buy, 0, moved.clone());
        let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, LATER);
        assert_eq!(
            shape(&plan),
            vec![(Stage::Cancel, "cancel", Side::Buy, 0)],
            "{name}"
        );
        assert_eq!(plan.commands[0].cid, old, "{name}");
        // Until the old order is terminal the level is replacing: nothing placed, the old order
        // still counted in full.
        let waiting = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, LATER);
        assert!(waiting.commands.is_empty(), "{name}");
        assert_eq!(
            held(&waiting),
            vec![(Side::Buy, 0, HeldReason::Replacing)],
            "{name}"
        );
        assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(5)), "{name}");
        // Terminal: the new order is placed at the level and counted from its build.
        cancelled(&mut reg, old, Side::Buy, "v-Buy-0", 10);
        let placed = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, LATER);
        assert_eq!(
            shape(&placed),
            vec![(Stage::Add, "place", Side::Buy, 0)],
            "{name}"
        );
        let new = placed.commands[0].cid;
        assert_ne!(new, old, "{name}");
        match placed.commands[0].auth.command() {
            VenueCommand::Place(o) => {
                assert_eq!(
                    (o.kind, o.qty, o.tif),
                    (OrderKind::Limit { px: moved.px }, moved.qty, moved.tif),
                    "{name}"
                );
            }
            other => panic!("{name}: expected a place, got {other:?}"),
        }
        assert_eq!(p.order_at(INST, Side::Buy, 0), Some(new), "{name}");
        assert_eq!(reg.resting_on(INST, Side::Buy), Some(moved.qty), "{name}");
    }
}

#[test]
fn a_partly_filled_order_is_amended_to_its_fill_plus_the_quote_or_replaced_where_the_venue_cannot()
{
    let no_partial = OrderCaps {
        amend: amending().amend.map(|a| AmendCaps {
            when_partially_filled: false,
            ..a
        }),
        ..order_caps()
    };
    for (caps, amends) in [(amending(), true), (no_partial, false)] {
        let mut reg = quoting(WIDE, 0);
        let mut mint = mint();
        let mut p = planner();
        let opened = open_book(
            &mut p,
            &book().with(Side::Buy, 0, quote(10_000, 5)),
            &mut reg,
            &caps,
            &mut mint,
        );
        let c = opened.commands[0].cid;
        // 3 of 5 filled: 2 rest; the quote asks for 5 resting.
        let fill = update(c, Side::Buy, "v-Buy-0", VenueOrderState::Open, 3);
        reg.apply_update(&fill, key(5));
        assert_eq!(reg.get(c).unwrap().state(), OrdState::PartiallyFilled);
        let plan = plan_and_send(
            &mut p,
            &book().with(Side::Buy, 0, quote(10_000, 5)),
            &mut reg,
            &caps,
            &mut mint,
            LATER,
        );
        if amends {
            assert_eq!(shape(&plan), vec![(Stage::Amend, "amend", Side::Buy, 0)]);
            match plan.commands[0].auth.command() {
                VenueCommand::Amend(a) => assert_eq!((a.qty, a.cum_filled), (lots(8), lots(3))),
                other => panic!("expected an amend, got {other:?}"),
            }
        } else {
            assert_eq!(shape(&plan), vec![(Stage::Cancel, "cancel", Side::Buy, 0)]);
        }
    }
}

#[test]
fn a_remaining_quantity_amend_is_counted_as_its_wire_quantity() {
    // FBC-w5n: the venue rests the wire quantity whole on top of fills it takes first.
    let remaining = OrderCaps {
        amend: amending().amend.map(|a| AmendCaps {
            qty_semantics: AmendQty::Remaining,
            ..a
        }),
        ..order_caps()
    };
    let mut reg = quoting(WIDE, 0);
    let mut mint = mint();
    let mut p = planner();
    let opened = open_book(
        &mut p,
        &book().with(Side::Buy, 0, quote(10_000, 5)),
        &mut reg,
        &remaining,
        &mut mint,
    );
    let c = opened.commands[0].cid;
    reg.apply_update(
        &update(c, Side::Buy, "v-Buy-0", VenueOrderState::Open, 3),
        key(5),
    );
    let plan = plan_and_send(
        &mut p,
        &book().with(Side::Buy, 0, quote(10_000, 6)),
        &mut reg,
        &remaining,
        &mut mint,
        LATER,
    );
    match plan.commands[0].auth.command() {
        VenueCommand::Amend(a) => {
            assert_eq!((a.qty, a.cum_filled), (lots(9), lots(3)));
            assert_eq!(a.wire_qty(AmendQty::Remaining), Some(lots(6)));
        }
        other => panic!("expected an amend, got {other:?}"),
    }
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(6)));
}

#[test]
fn the_old_order_counts_until_terminal_and_the_new_one_from_its_build() {
    // The resting cap of 11 per side: an old bid of 6 being replaced still counts, so a new bid
    // of 6 at another level is refused until the old one is terminal; then the replacement is
    // built first and counts, and the other bid is refused again.
    let mut reg = quoting(RESTING, 0);
    let mut mint = mint();
    let mut p = planner();
    let opened = open_book(
        &mut p,
        &book().with(Side::Buy, 0, quote(10_000, 6)),
        &mut reg,
        &order_caps(),
        &mut mint,
    );
    let old = opened.commands[0].cid;
    let desired = book()
        .with(Side::Buy, 0, quote(10_004, 6))
        .with(Side::Buy, 1, quote(9_990, 6));
    let capped = |resting: i64| {
        PlanRefusal::Place(OmsError::Capped(CapRefusal::RestingCap {
            inst: INST,
            side: Side::Buy,
            resting: Some(lots(resting)),
            cap: lots(RESTING),
        }))
    };
    let plan = plan_and_send(&mut p, &desired, &mut reg, &order_caps(), &mut mint, LATER);
    assert_eq!(shape(&plan), vec![(Stage::Cancel, "cancel", Side::Buy, 0)]);
    assert_eq!(refused(&plan), vec![(Side::Buy, 1, capped(12))]);
    let waiting = plan_and_send(&mut p, &desired, &mut reg, &order_caps(), &mut mint, LATER);
    assert!(waiting.commands.is_empty());
    assert_eq!(refused(&waiting), vec![(Side::Buy, 1, capped(12))]);
    cancelled(&mut reg, old, Side::Buy, "v-Buy-0", 10);
    let placed = plan_and_send(&mut p, &desired, &mut reg, &order_caps(), &mut mint, LATER);
    assert_eq!(shape(&placed), vec![(Stage::Add, "place", Side::Buy, 0)]);
    assert_eq!(refused(&placed), vec![(Side::Buy, 1, capped(12))]);
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(6)));
}

#[test]
fn nothing_is_placed_at_a_level_whose_order_is_pending_new_or_unknown_until_it_is_terminal() {
    for ends_unknown in [false, true] {
        let mut reg = quoting(WIDE, 0);
        let mut mint = mint();
        let mut p = planner();
        let first = plan_and_send(
            &mut p,
            &book().with(Side::Buy, 0, quote(10_000, 5)),
            &mut reg,
            &amending(),
            &mut mint,
            MonoNs(0),
        );
        let c = first.commands[0].cid;
        let moved = book().with(Side::Buy, 0, quote(10_010, 7));
        // PendingNew: occupied, however long the wait.
        let plan = p.plan(&moved, &mut reg, &amending(), ACCT, &mut mint, LATER);
        assert!(plan.commands.is_empty());
        assert_eq!(
            held(&plan),
            vec![(Side::Buy, 0, HeldReason::Unsettled(OrdState::PendingNew))]
        );
        if ends_unknown {
            let item = ItemRef {
                idx: 0,
                cid: Some(c),
                vid: None,
            };
            reg.on_outcome(c, OrderOp::Place, &item, &SubmitOutcome::Unknown, MonoNs(2))
                .unwrap();
            let plan = p.plan(&moved, &mut reg, &amending(), ACCT, &mut mint, LATER);
            assert!(plan.commands.is_empty());
            assert_eq!(
                held(&plan),
                vec![(Side::Buy, 0, HeldReason::Unsettled(OrdState::Unknown))]
            );
        }
        assert_eq!(p.order_at(INST, Side::Buy, 0), Some(c));
        // Terminal (the venue ended it): the level is free and the quote is placed.
        cancelled(&mut reg, c, Side::Buy, "v-pending", 10);
        let plan = p.plan(&moved, &mut reg, &amending(), ACCT, &mut mint, LATER);
        assert_eq!(shape(&plan), vec![(Stage::Add, "place", Side::Buy, 0)]);
        assert_ne!(plan.commands[0].cid, c);
    }
}

#[test]
fn an_order_on_the_unknown_ladder_holds_its_level() {
    let mut reg = quoting(WIDE, 0);
    let mut mint = mint();
    let mut p = planner();
    let opened = open_book(
        &mut p,
        &book().with(Side::Buy, 0, quote(10_000, 5)),
        &mut reg,
        &amending(),
        &mut mint,
    );
    let c = opened.commands[0].cid;
    // An amend unanswered: the resting order goes to the ladder.
    let plan = plan_and_send(
        &mut p,
        &book().with(Side::Buy, 0, quote(10_004, 5)),
        &mut reg,
        &amending(),
        &mut mint,
        LATER,
    );
    let rpc = RpcId(1_000);
    assert_eq!(shape(&plan), vec![(Stage::Amend, "amend", Side::Buy, 0)]);
    let item = ItemRef {
        idx: 0,
        cid: Some(c),
        vid: Some(vid("v-Buy-0")),
    };
    reg.on_outcome(
        c,
        OrderOp::Amend(rpc),
        &item,
        &SubmitOutcome::Unknown,
        LATER,
    )
    .unwrap();
    assert!(reg.get(c).unwrap().unknown_since().is_some());
    let plan = p.plan(
        &book().with(Side::Buy, 0, quote(10_020, 5)),
        &mut reg,
        &amending(),
        ACCT,
        &mut mint,
        LATER,
    );
    assert!(plan.commands.is_empty());
    assert_eq!(
        held(&plan),
        vec![(Side::Buy, 0, HeldReason::Unsettled(OrdState::Open))]
    );
}

#[test]
fn a_level_no_longer_wanted_is_cancelled_once_and_its_order_awaits_its_acknowledgement_first() {
    let mut reg = quoting(WIDE, 0);
    let mut mint = mint();
    let mut p = planner();
    let first = plan_and_send(
        &mut p,
        &book().with(Side::Buy, 0, quote(10_000, 5)),
        &mut reg,
        &amending(),
        &mut mint,
        MonoNs(0),
    );
    let c = first.commands[0].cid;
    // Not acknowledged, on a venue that cancels by venue id only: the cancel waits.
    let empty = book();
    let plan = plan_and_send(&mut p, &empty, &mut reg, &amending(), &mut mint, LATER);
    assert!(plan.commands.is_empty());
    assert_eq!(plan.awaiting_ack, vec![c]);
    // A quantity of zero is no order at the level, the same as none.
    let zero = book().with(Side::Buy, 0, quote(10_000, 0));
    assert_eq!(
        p.plan(&zero, &mut reg, &amending(), ACCT, &mut mint, LATER)
            .awaiting_ack,
        vec![c]
    );
    // Acknowledged: the cancel is built, and once sent not built again.
    ack(&mut reg, c, "v-Buy-0");
    let plan = plan_and_send(&mut p, &empty, &mut reg, &amending(), &mut mint, LATER);
    assert_eq!(shape(&plan), vec![(Stage::Cancel, "cancel", Side::Buy, 0)]);
    let again = plan_and_send(&mut p, &empty, &mut reg, &amending(), &mut mint, LATER);
    assert!(again.commands.is_empty() && again.held.is_empty() && again.awaiting_ack.is_empty());
    // Terminal: the planner forgets the level.
    cancelled(&mut reg, c, Side::Buy, "v-Buy-0", 10);
    assert!(
        p.plan(&empty, &mut reg, &amending(), ACCT, &mut mint, LATER)
            .commands
            .is_empty()
    );
    assert_eq!(p.order_at(INST, Side::Buy, 0), None);
}

/// Reviewer A RA103-1, Reviewer B RB-0j3-1 (PR #103): a level pulled before its order's
/// acknowledgement and wanted again has its cancel carried through, then the new quote placed;
/// the order is never left resting at its old price.
#[test]
fn a_level_pulled_before_the_acknowledgement_and_wanted_again_is_cancelled_then_placed() {
    let mut reg = quoting(WIDE, 0);
    let mut mint = mint();
    let mut p = planner();
    let first = plan_and_send(
        &mut p,
        &book().with(Side::Buy, 0, quote(10_000, 5)),
        &mut reg,
        &amending(),
        &mut mint,
        MonoNs(0),
    );
    let old = first.commands[0].cid;
    // Pulled before the acknowledgement: the cancel waits for it.
    let pulled = plan_and_send(&mut p, &book(), &mut reg, &amending(), &mut mint, LATER);
    assert_eq!(pulled.awaiting_ack, vec![old]);
    // Wanted again at a new price, still unacknowledged: the cancel still waits.
    let moved = book().with(Side::Buy, 0, quote(10_020, 5));
    let again = plan_and_send(&mut p, &moved, &mut reg, &amending(), &mut mint, LATER);
    assert!(again.commands.is_empty());
    assert_eq!(again.awaiting_ack, vec![old]);
    // Acknowledged: the cancel is built, not an amend and not a hold.
    ack(&mut reg, old, "v-Buy-0");
    let plan = plan_and_send(&mut p, &moved, &mut reg, &amending(), &mut mint, LATER);
    assert_eq!(shape(&plan), vec![(Stage::Cancel, "cancel", Side::Buy, 0)]);
    assert_eq!(plan.commands[0].cid, old);
    assert!(plan.held.is_empty() && plan.awaiting_ack.is_empty());
    // Sent: the level is replacing until the old order is terminal.
    let waiting = plan_and_send(&mut p, &moved, &mut reg, &amending(), &mut mint, LATER);
    assert!(waiting.commands.is_empty());
    assert_eq!(held(&waiting), vec![(Side::Buy, 0, HeldReason::Replacing)]);
    // Terminal: the wanted quote is placed.
    cancelled(&mut reg, old, Side::Buy, "v-Buy-0", 10);
    let placed = plan_and_send(&mut p, &moved, &mut reg, &amending(), &mut mint, LATER);
    assert_eq!(shape(&placed), vec![(Stage::Add, "place", Side::Buy, 0)]);
    match placed.commands[0].auth.command() {
        VenueCommand::Place(o) => assert_eq!(o.kind, OrderKind::Limit { px: Ticks(10_020) }),
        other => panic!("expected a place, got {other:?}"),
    }
}

#[test]
fn an_order_is_changed_only_past_a_threshold_and_once_it_is_the_minimum_age() {
    let mut reg = quoting(WIDE, 0);
    let mut mint = mint();
    let mut p = planner();
    open_book(
        &mut p,
        &book().with(Side::Buy, 0, quote(10_000, 5)),
        &mut reg,
        &amending(),
        &mut mint,
    );
    // 1 tick (1 bp) and 1 lot: under both thresholds, kept, however old.
    let small = book().with(Side::Buy, 0, quote(10_001, 6));
    let plan = p.plan(&small, &mut reg, &amending(), ACCT, &mut mint, LATER);
    assert!(plan.commands.is_empty() && plan.held.is_empty());
    // 2 ticks (2 bp), or 2 lots: changed, but not before the minimum age.
    let young = MonoNs(MIN_AGE.as_nanos() as u64 - 1);
    for desired in [
        book().with(Side::Buy, 0, quote(10_002, 5)),
        book().with(Side::Buy, 0, quote(10_000, 3)),
    ] {
        let plan = p.plan(&desired, &mut reg, &amending(), ACCT, &mut mint, young);
        assert!(plan.commands.is_empty());
        assert_eq!(held(&plan), vec![(Side::Buy, 0, HeldReason::Young)]);
    }
    let old_enough = MonoNs(MIN_AGE.as_nanos() as u64);
    let plan = plan_and_send(
        &mut p,
        &book().with(Side::Buy, 0, quote(10_000, 3)),
        &mut reg,
        &amending(),
        &mut mint,
        old_enough,
    );
    assert_eq!(shape(&plan), vec![(Stage::Amend, "amend", Side::Buy, 0)]);
}

#[test]
fn a_market_in_cancel_only_or_killed_gets_no_place_or_amend_but_gets_its_cancels() {
    for kill in [false, true] {
        let mut reg = quoting(WIDE, 0);
        let mut mint = mint();
        let mut p = planner();
        open_book(
            &mut p,
            &book()
                .with(Side::Buy, 0, quote(10_000, 5))
                .with(Side::Buy, 1, quote(9_990, 5)),
            &mut reg,
            &amending(),
            &mut mint,
        );
        let state = if kill {
            reg.kill(INST);
            StateRefusal::Killed(INST)
        } else {
            reg.disarm(INST);
            StateRefusal::CancelOnly(INST)
        };
        // Level 0 moves, level 1 goes, level 2 and an ask come.
        let desired = book()
            .with(Side::Buy, 0, quote(9_995, 5))
            .with(Side::Buy, 2, quote(9_980, 5))
            .with(Side::Sell, 0, reducing(quote(10_010, 5)));
        let plan = p.plan(&desired, &mut reg, &amending(), ACCT, &mut mint, LATER);
        assert_eq!(shape(&plan), vec![(Stage::Cancel, "cancel", Side::Buy, 1)]);
        assert_eq!(
            refused(&plan),
            vec![
                (Side::Sell, 0, PlanRefusal::Place(OmsError::State(state))),
                (Side::Buy, 0, PlanRefusal::Amend(AmendRefusal::State(state))),
                (Side::Buy, 2, PlanRefusal::Place(OmsError::State(state))),
            ]
        );
    }
}

#[test]
fn a_market_in_exit_gets_only_its_exit_orders() {
    // Long 10, Flatten: an ask that reduces is built, a bid is not.
    let mut reg = quoting(WIDE, 10);
    reg.disarm(INST);
    let given = leases(&reg, INST);
    reg.flatten(INST, given).unwrap();
    let mut mint = mint();
    let mut p = planner();
    let desired =
        book()
            .with(Side::Buy, 0, quote(9_990, 5))
            .with(Side::Sell, 0, reducing(quote(10_010, 5)));
    let plan = p.plan(&desired, &mut reg, &amending(), ACCT, &mut mint, LATER);
    assert_eq!(
        shape(&plan),
        vec![(Stage::Reducing, "place", Side::Sell, 0)]
    );
    assert_eq!(
        refused(&plan),
        vec![(
            Side::Buy,
            0,
            PlanRefusal::Place(OmsError::State(StateRefusal::Exit(
                ExitRefusal::Increasing {
                    inst: INST,
                    side: Side::Buy
                }
            )))
        )]
    );
}

#[test]
fn an_item_over_a_cap_is_never_built() {
    let mut reg = quoting(RESTING, 0);
    let mut mint = mint();
    let mut p = planner();
    // A bid over the resting cap and an ask over the inventory cap: neither is built.
    let over = book().with(Side::Buy, 0, quote(10_000, RESTING + 1)).with(
        Side::Sell,
        0,
        quote(10_010, CAP + 1),
    );
    let plan = p.plan(&over, &mut reg, &amending(), ACCT, &mut mint, MonoNs(0));
    assert!(plan.commands.is_empty());
    let why: Vec<_> = refused(&plan)
        .into_iter()
        .map(|(s, l, w)| (s, l, matches!(w, PlanRefusal::Place(OmsError::Capped(_)))))
        .collect();
    assert_eq!(why, vec![(Side::Buy, 0, true), (Side::Sell, 0, true)]);
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(Lots::ZERO));
    // An amend over the resting cap is refused, never built; the order is left as it was.
    let opened = open_book(
        &mut p,
        &book().with(Side::Buy, 0, quote(10_000, 5)),
        &mut reg,
        &amending(),
        &mut mint,
    );
    let c = opened.commands[0].cid;
    let plan = p.plan(
        &book().with(Side::Buy, 0, quote(10_000, RESTING + 1)),
        &mut reg,
        &amending(),
        ACCT,
        &mut mint,
        LATER,
    );
    assert!(plan.commands.is_empty());
    assert_eq!(
        refused(&plan),
        vec![(
            Side::Buy,
            0,
            PlanRefusal::Amend(AmendRefusal::Capped(CapRefusal::RestingCap {
                inst: INST,
                side: Side::Buy,
                resting: Some(lots(RESTING + 1)),
                cap: lots(RESTING),
            }))
        )]
    );
    assert_eq!(reg.get(c).unwrap().amend_built(), None);
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(5)));
}

#[test]
fn a_planner_config_takes_a_finite_non_negative_price_threshold() {
    for bad in [f64::NAN, f64::INFINITY, -0.5] {
        assert_eq!(
            PlannerConfig::new(Bps(bad), lots(1), MIN_AGE),
            Err(PlannerConfigError::PriceThreshold)
        );
    }
    assert!(
        PlannerConfigError::PriceThreshold
            .to_string()
            .contains("basis points")
    );
    let cfg = PlannerConfig::new(Bps(0.0), Lots::ZERO, Duration::ZERO).unwrap();
    assert_eq!(
        (cfg.px_bps(), cfg.qty(), cfg.min_age()),
        (Bps(0.0), Lots::ZERO, Duration::ZERO)
    );
    // Thresholds of zero change an order on any difference, at once.
    let mut reg = quoting(WIDE, 0);
    let mut mint = mint();
    let mut p = ExecutionPlanner::new(cfg);
    open_book(
        &mut p,
        &book().with(Side::Buy, 0, quote(10_000, 5)),
        &mut reg,
        &amending(),
        &mut mint,
    );
    let plan = p.plan(
        &book().with(Side::Buy, 0, quote(10_001, 5)),
        &mut reg,
        &amending(),
        ACCT,
        &mut mint,
        MonoNs(0),
    );
    assert_eq!(shape(&plan), vec![(Stage::Amend, "amend", Side::Buy, 0)]);
}

#[test]
fn a_desired_book_sets_and_removes_quotes_by_side_and_level() {
    let mut b = book().with(Side::Buy, 0, quote(10_000, 5));
    b.set(Side::Sell, 3, quote(10_010, 2));
    assert_eq!(b.market(), INST);
    assert_eq!(b.quote(Side::Buy, 0), Some(&quote(10_000, 5)));
    assert_eq!(b.quote(Side::Sell, 3), Some(&quote(10_010, 2)));
    b.remove(Side::Buy, 0);
    assert_eq!(b.quote(Side::Buy, 0), None);
}
