//! Exit's admission (decision 0012; 0013 rule 1): in a market in Exit, only exit orders on the
//! side that reduces the position are built: a place, a batch item, an amend or a replace
//! carrying the venue's reduce-only flag or the OMS's reducing classification, sized so that
//! the position plus every order on that side that may still fill (PendingNew, Unknown, Open,
//! a partly filled order's remainder, and the fills the venue reported that the inventory does
//! not hold yet), the new one included, never crosses zero. Each is still refused when a
//! pre-trade cap refuses it; once the position is flat, or while it is unknown, nothing is
//! built; and no ordinary order (one on the side that adds to the position, or one not marked
//! as an exit) is built at all. It is an added restriction inside the one pre-trade path,
//! never a bypass.
//!
//! The values are the owner's first test values: a $50 inventory cap on a synthetic market
//! where one lot is worth $1, so 50 lots; the position each scenario exits is 20 lots, long
//! and short.

#[path = "common/arm.rs"]
mod arm;
mod common;

use std::time::Duration;

use arm::{leases, named, start};
use common::{cid, fill, fill_id, ident, lots, order_caps, placement, update, vid};
use fbc_core::{
    AckLevel, AmendAck, AmendCaps, AmendQty, ClientOrderId, FillIdent, InstrumentId, ItemRef,
    MonoNs, NewOrder, OrderCaps, OrderKind, RefKind, Side, SignedLots, SnapshotSource,
    SubmitOutcome, TagSet, Ticks, Tif, VenueCommand, VenueOrderState, WallNs,
};
use fbc_oms::{
    Admission, AmendRefusal, ArmRefusal, CapRefusal, EntryState, ExitKind, ExitRefusal, FillLedger,
    FillRouted, FillTime, LadderConfig, Leases, LedgerConfig, MarketCapsConfig, MarketEntry,
    OmsError, OrderKey, OrderOp, PreTradeCaps, Registry, ResyncSnapshot, StateRefusal,
};

const INST: InstrumentId = InstrumentId::new(1);
const OTHER: InstrumentId = InstrumentId::new(2);
/// The owner's first inventory cap, in lots of $1.
const CAP: i64 = 50;
/// A resting cap past any order of the tests that judge the other refusals.
const WIDE: i64 = 1_000_000;
/// The size of the position each scenario exits.
const POS: i64 = 20;

/// A registry named for its leases, under the inventory cap `inventory` and the resting cap
/// `resting` on `INST` (and no caps on `OTHER`), its position not seeded.
fn registry(inventory: i64, resting: i64) -> Registry {
    named(Registry::with_caps(
        PreTradeCaps::new()
            .with_market(
                INST,
                MarketCapsConfig {
                    inventory: Some(lots(inventory)),
                    resting: Some(lots(resting)),
                },
            )
            .unwrap(),
    ))
}

/// A registry under the owner's caps, `INST` seeded at `pos`.
fn seeded(pos: i64) -> Registry {
    seeded_under(CAP, WIDE, pos)
}

fn seeded_under(inventory: i64, resting: i64, pos: i64) -> Registry {
    let mut reg = registry(inventory, resting);
    arm::seed(&mut reg, &[(INST, pos)]);
    reg
}

type Call = fn(&mut Registry, InstrumentId, Leases) -> Result<MarketEntry, ArmRefusal>;

/// The owner's two exits.
fn exits() -> [(&'static str, Call, ExitKind); 2] {
    [
        ("flatten", Registry::flatten, ExitKind::Flatten),
        ("wind-down", Registry::wind_down, ExitKind::WindDown),
    ]
}

/// Moves `market` into Exit by `call`, arming it with its leases when it is disarmed.
fn into_exit(reg: &mut Registry, market: InstrumentId, (_, call, kind): (&str, Call, ExitKind)) {
    let given = if reg.entry(market).armed() {
        Leases::none()
    } else {
        leases(reg, market)
    };
    let e = call(reg, market, given).unwrap();
    assert_eq!((e.armed(), e.state()), (true, EntryState::Exit(kind)));
}

/// An ordinary post-only limit order of `qty` on `side` at `px`: neither the venue's
/// reduce-only flag nor the OMS's reducing classification.
fn order(side: Side, qty: i64, px: i64) -> NewOrder {
    NewOrder {
        side,
        kind: OrderKind::Limit { px: Ticks(px) },
        ..placement(cid(), px, qty)
    }
}

/// The price an order on `side` rests at: a bid below an ask.
fn px(side: Side) -> i64 {
    match side {
        Side::Buy => 100,
        Side::Sell => 101,
    }
}

/// `order` as an exit sends it: reduce-only and classified reducing.
fn exit(order: NewOrder) -> NewOrder {
    NewOrder {
        reduce_only: true,
        reducing: true,
        ..order
    }
}

/// An exit order of `qty` on `side`.
fn exit_of(side: Side, qty: i64) -> NewOrder {
    exit(order(side, qty, px(side)))
}

fn refused(why: ExitRefusal) -> OmsError {
    OmsError::State(StateRefusal::Exit(why))
}

fn crosses(side: Side, total: i64, position: i64) -> ExitRefusal {
    ExitRefusal::CrossesZero {
        inst: INST,
        side,
        total: Some(lots(total)),
        position: lots(position),
    }
}

fn accepted() -> SubmitOutcome {
    SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    }
}

fn key(n: u64) -> OrderKey {
    OrderKey {
        venue: None,
        ingest: n,
    }
}

/// Places `order` (admitted) and acknowledges it under the venue id `v`: Open.
fn open(reg: &mut Registry, order: NewOrder, v: &str) -> ClientOrderId {
    let c = order.cid;
    reg.place(order).unwrap();
    let item = ItemRef {
        idx: 0,
        cid: None,
        vid: Some(vid(v)),
    };
    reg.on_outcome(c, OrderOp::Place, &item, &accepted(), MonoNs(1))
        .unwrap();
    c
}

/// Amends on a venue whose amend keeps the venue id, or, for a replace, gives a new one.
fn venue(replace: bool) -> OrderCaps {
    OrderCaps {
        amend: Some(AmendCaps {
            refs: TagSet::of(&[RefKind::Venue]),
            price: true,
            qty: true,
            flags: false,
            when_partially_filled: true,
            reject_keeps_original: true,
            keeps_venue_id: !replace,
            ack: AmendAck::ReplacedEvent,
            qty_semantics: AmendQty::TotalIncludingFilled,
            keeps_priority: None,
        }),
        ..order_caps()
    }
}

fn amend(
    reg: &mut Registry,
    c: ClientOrderId,
    replace: bool,
    qty: i64,
    reducing: bool,
) -> Result<VenueCommand, AmendRefusal> {
    let px = reg.get(c).unwrap().px().unwrap();
    reg.live(c)
        .unwrap()
        .amend(&venue(replace), px, lots(qty), reducing)
        .map(|cmd| cmd.command().clone())
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

/// Applies a live fill of `qty` of our order `c` on `side`, named `fid`.
fn fill_of(
    reg: &mut Registry,
    l: &mut FillLedger,
    c: ClientOrderId,
    side: Side,
    qty: i64,
    fid: &str,
) -> FillRouted {
    let f = fill(Some(c), ident(fid), side, qty, false);
    match l.admit(&f, None, MonoNs(0)) {
        Admission::Apply(a) => reg.apply_fill(a).unwrap(),
        other => panic!("expected the fill accepted, got {other:?}"),
    }
}

/// Every kind of place an Exit may be asked to build on a position that `reduces` reduces,
/// with what Exit does with it: `None` when it is built.
fn shapes(reduces: Side) -> Vec<(&'static str, NewOrder, Option<ExitRefusal>)> {
    let adds = reduces.opposite();
    let cross = match reduces {
        Side::Buy => 110,
        Side::Sell => 90,
    };
    vec![
        (
            "ordinary, adding",
            order(adds, 2, px(adds)),
            Some(ExitRefusal::Increasing {
                inst: INST,
                side: adds,
            }),
        ),
        (
            "ordinary, on the reducing side",
            order(reduces, 2, px(reduces)),
            Some(ExitRefusal::Ordinary {
                inst: INST,
                side: reduces,
            }),
        ),
        (
            "reduce-only, adding",
            exit_of(adds, 2),
            Some(ExitRefusal::Increasing {
                inst: INST,
                side: adds,
            }),
        ),
        (
            "the venue's reduce-only flag alone",
            NewOrder {
                reduce_only: true,
                ..order(reduces, 2, px(reduces))
            },
            None,
        ),
        (
            "the OMS's reducing classification alone",
            NewOrder {
                reducing: true,
                ..order(reduces, 2, px(reduces))
            },
            None,
        ),
        ("flatten", exit_of(reduces, 4), None),
        (
            "force-close",
            NewOrder {
                tif: Tif::Ioc,
                post_only: false,
                ..exit(order(reduces, 4, cross))
            },
            None,
        ),
        ("wind-down", exit_of(reduces, 4), None),
    ]
}

#[test]
fn exit_builds_only_exit_orders_on_the_side_that_reduces_the_position() {
    for (pos, reduces) in [(POS, Side::Sell), (-POS, Side::Buy)] {
        for call in exits() {
            let case = format!("{} from {pos}", call.0);
            // One at a time, each on a market of its own.
            for (name, o, why) in shapes(reduces) {
                let mut reg = seeded(pos);
                into_exit(&mut reg, INST, call);
                let built = reg.place(o.clone()).map(|cmd| cmd.command().clone());
                match why {
                    None => {
                        assert_eq!(built, Ok(VenueCommand::Place(o)), "{case}: {name}");
                        assert_eq!(reg.len(), 1, "{case}: {name}");
                    }
                    Some(why) => {
                        assert_eq!(built, Err(refused(why)), "{case}: {name}");
                        assert!(reg.is_empty(), "{case}: {name}");
                    }
                }
            }
            // All in one batch: the exits are built, in the order given, and nothing else.
            let mut reg = seeded(pos);
            into_exit(&mut reg, INST, call);
            let all = shapes(reduces);
            let plan = reg
                .place_batch(all.iter().map(|(_, o, _)| o.clone()).collect())
                .unwrap();
            let admitted: Vec<NewOrder> = all
                .iter()
                .filter(|(_, _, why)| why.is_none())
                .map(|(_, o, _)| o.clone())
                .collect();
            let refusals: Vec<(ClientOrderId, OmsError)> = all
                .iter()
                .filter_map(|(_, o, why)| why.map(|why| (o.cid, refused(why))))
                .collect();
            assert_eq!(
                plan.command.map(|cmd| cmd.command().clone()),
                Some(VenueCommand::PlaceBatch(admitted.clone())),
                "{case}"
            );
            assert_eq!(plan.refused, refusals, "{case}");
            assert_eq!(reg.len(), admitted.len(), "{case}");
        }
    }
}

#[test]
fn exit_sizes_each_order_so_the_position_plus_every_order_on_its_side_never_crosses_zero() {
    for (pos, reduces) in [(POS, Side::Sell), (-POS, Side::Buy)] {
        let adds = reduces.opposite();
        for call in exits() {
            let case = format!("{} from {pos}", call.0);
            let mut reg = seeded(pos);
            start(&mut reg, INST);
            // On the reducing side, while Quoting: an Open order of 5, a PendingNew one of 3,
            // an Unknown one of 2, and an Open one of 6 the venue reported 2 of filled before
            // the fill event arrived (4 resting, 2 the inventory does not hold yet): 16 that
            // may still move the position. The other side's order counts for nothing here.
            open(&mut reg, order(reduces, 5, px(reduces)), "v-open");
            reg.place(order(reduces, 3, px(reduces))).unwrap();
            let unknown = order(reduces, 2, px(reduces));
            let u = unknown.cid;
            reg.place(unknown).unwrap();
            let item = ItemRef {
                idx: 0,
                cid: None,
                vid: None,
            };
            reg.on_outcome(u, OrderOp::Place, &item, &SubmitOutcome::Unknown, MonoNs(1))
                .unwrap();
            let part = open(&mut reg, order(reduces, 6, px(reduces)), "v-part");
            let mut reported = update(Some(part), VenueOrderState::Open, 2);
            reported.vid = Some(vid("v-part"));
            reported.side = reduces;
            reg.apply_update(&reported, key(1));
            assert_eq!(reg.resting_on(INST, reduces), Some(lots(14)), "{case}");
            open(&mut reg, order(adds, 7, px(adds)), "v-adds");
            into_exit(&mut reg, INST, call);

            assert_eq!(
                reg.place(exit_of(reduces, 5)),
                Err(refused(crosses(reduces, 21, POS))),
                "{case}"
            );
            reg.place(exit_of(reduces, 4)).unwrap();
            assert_eq!(
                reg.place(exit_of(reduces, 1)),
                Err(refused(crosses(reduces, 21, POS))),
                "{case}"
            );
        }
    }
}

#[test]
fn a_batch_in_exit_judges_each_item_with_the_earlier_ones_admitted() {
    for (pos, reduces) in [(POS, Side::Sell), (-POS, Side::Buy)] {
        let mut reg = seeded(pos);
        into_exit(&mut reg, INST, exits()[0]);
        let items = [8, 8, 5, 4].map(|q| exit_of(reduces, q));
        let plan = reg.place_batch(items.to_vec()).unwrap();
        assert_eq!(
            plan.command.map(|cmd| cmd.command().clone()),
            Some(VenueCommand::PlaceBatch(vec![
                items[0].clone(),
                items[1].clone(),
                items[3].clone(),
            ])),
            "{pos}"
        );
        assert_eq!(
            plan.refused,
            vec![(items[2].cid, refused(crosses(reduces, 21, POS)))],
            "{pos}"
        );
        // Nothing more on that side: the three admitted are the whole position.
        assert_eq!(
            reg.place(exit_of(reduces, 1)),
            Err(refused(crosses(reduces, 21, POS))),
            "{pos}"
        );
    }
}

#[test]
fn exit_builds_nothing_once_the_position_is_flat() {
    for call in exits() {
        // Fills bring the position to zero: the order that exits it shrinks with each fill
        // and nothing more is built once it is flat.
        let mut reg = seeded(POS);
        let mut l = ledger();
        into_exit(&mut reg, INST, call);
        let c = reg.place(exit_of(Side::Sell, POS)).unwrap();
        let c = match c.command() {
            VenueCommand::Place(o) => o.cid,
            other => panic!("expected a place, got {other:?}"),
        };
        assert_eq!(
            reg.place(exit_of(Side::Sell, 1)),
            Err(refused(crosses(Side::Sell, 21, POS)))
        );
        assert!(matches!(
            fill_of(&mut reg, &mut l, c, Side::Sell, 8, "f1"),
            FillRouted::Ours(..)
        ));
        assert_eq!(reg.position(INST), Some(SignedLots(12)));
        assert_eq!(
            reg.place(exit_of(Side::Sell, 1)),
            Err(refused(crosses(Side::Sell, 13, 12)))
        );
        fill_of(&mut reg, &mut l, c, Side::Sell, 12, "f2");
        assert_eq!(reg.position(INST), Some(SignedLots(0)));
        for (name, o, _) in shapes(Side::Sell).into_iter().chain(shapes(Side::Buy)) {
            assert_eq!(
                reg.place(o),
                Err(refused(ExitRefusal::Flat(INST))),
                "{}: {name}",
                call.0
            );
        }
        let plan = reg
            .place_batch(vec![exit_of(Side::Sell, 1), exit_of(Side::Buy, 1)])
            .unwrap();
        assert_eq!(plan.command, None);
        assert_eq!(plan.refused.len(), 2);
        assert!(
            plan.refused
                .iter()
                .all(|(_, why)| *why == refused(ExitRefusal::Flat(INST)))
        );
    }
    // Flat when the owner pressed it, with an order resting from Quoting: nothing is placed
    // or amended.
    let mut reg = seeded(0);
    start(&mut reg, INST);
    let ask = open(&mut reg, exit_of(Side::Sell, 5), "v-ask");
    into_exit(&mut reg, INST, exits()[1]);
    assert_eq!(
        reg.place(exit_of(Side::Sell, 1)),
        Err(refused(ExitRefusal::Flat(INST)))
    );
    assert_eq!(
        amend(&mut reg, ask, false, 4, true),
        Err(AmendRefusal::State(StateRefusal::Exit(ExitRefusal::Flat(
            INST
        ))))
    );
}

#[test]
fn exit_amends_only_exit_orders_on_the_reducing_side_never_crossing_zero() {
    for replace in [false, true] {
        let mut reg = seeded(POS);
        start(&mut reg, INST);
        let bid = open(&mut reg, order(Side::Buy, 5, 100), "v-bid");
        let ask = open(&mut reg, exit_of(Side::Sell, 5), "v-ask");
        let plain = open(&mut reg, order(Side::Sell, 3, 102), "v-plain");
        into_exit(&mut reg, INST, exits()[0]);
        let state = |why| Err(AmendRefusal::State(StateRefusal::Exit(why)));
        // The side that adds to the position: never, whatever the amend's classification.
        for reducing in [false, true] {
            assert_eq!(
                amend(&mut reg, bid, replace, 6, reducing),
                state(ExitRefusal::Increasing {
                    inst: INST,
                    side: Side::Buy,
                }),
                "{replace}"
            );
        }
        // An ordinary order on the reducing side, amended as an ordinary one: never.
        assert_eq!(
            amend(&mut reg, plain, replace, 3, false),
            state(ExitRefusal::Ordinary {
                inst: INST,
                side: Side::Sell,
            }),
            "{replace}"
        );
        // The reduce-only ask may grow until the side holds the whole position, no further.
        assert_eq!(
            amend(&mut reg, ask, replace, 18, false),
            state(crosses(Side::Sell, 21, POS)),
            "{replace}"
        );
        assert!(
            amend(&mut reg, ask, replace, 17, false).is_ok(),
            "{replace}"
        );
        // The ordinary one, amended as an exit, fits beside it exactly: 17 + 3.
        assert!(
            amend(&mut reg, plain, replace, 3, true).is_ok(),
            "{replace}"
        );
        assert_eq!(reg.resting_on(INST, Side::Sell), Some(lots(20)));
    }
}

#[test]
fn exit_still_refuses_every_order_a_cap_refuses() {
    // The inventory cap (0005's I6): a position already past it, 60 of 50, is exited only by
    // an order that brings the worst case back within it.
    let mut reg = seeded_under(CAP, WIDE, 60);
    into_exit(&mut reg, INST, exits()[0]);
    assert_eq!(
        reg.place(exit_of(Side::Sell, 5)),
        Err(OmsError::Capped(CapRefusal::InventoryCap {
            inst: INST,
            side: Side::Sell,
            worst: Some(lots(55)),
            cap: lots(CAP),
        }))
    );
    reg.place(exit_of(Side::Sell, 10)).unwrap();

    // The resting cap, on places, batch items and amends.
    let resting = |total| CapRefusal::RestingCap {
        inst: INST,
        side: Side::Sell,
        resting: Some(lots(total)),
        cap: lots(8),
    };
    let mut reg = seeded_under(CAP, 8, POS);
    into_exit(&mut reg, INST, exits()[1]);
    assert_eq!(
        reg.place(exit_of(Side::Sell, 9)),
        Err(OmsError::Capped(resting(9)))
    );
    let items = [5, 4, 3].map(|q| exit_of(Side::Sell, q));
    let plan = reg.place_batch(items.to_vec()).unwrap();
    assert_eq!(
        plan.command.map(|cmd| cmd.command().clone()),
        Some(VenueCommand::PlaceBatch(vec![
            items[0].clone(),
            items[2].clone()
        ]))
    );
    assert_eq!(
        plan.refused,
        vec![(items[1].cid, OmsError::Capped(resting(9)))]
    );
    assert_eq!(
        reg.place(exit_of(Side::Sell, 1)),
        Err(OmsError::Capped(resting(9)))
    );
    let mut reg = seeded_under(CAP, 8, POS);
    start(&mut reg, INST);
    let ask = open(&mut reg, exit_of(Side::Sell, 5), "v-ask");
    into_exit(&mut reg, INST, exits()[0]);
    assert_eq!(
        amend(&mut reg, ask, false, 9, true),
        Err(AmendRefusal::Capped(resting(9)))
    );
    assert!(amend(&mut reg, ask, false, 8, true).is_ok());

    // A market the configuration gives no caps admits no exit either.
    let mut reg = seeded(POS);
    arm::seed(&mut reg, &[(OTHER, POS)]);
    into_exit(&mut reg, OTHER, exits()[0]);
    assert_eq!(
        reg.place(NewOrder {
            inst: OTHER,
            ..exit_of(Side::Sell, 1)
        }),
        Err(OmsError::Capped(CapRefusal::NoCap(OTHER)))
    );
}

#[test]
fn exit_builds_nothing_while_the_position_is_unknown() {
    // A fill that falls between a resync's request and its answer, of an order the snapshot
    // does not show, leaves the market's position unknown after it was seeded (decision
    // 0055): Exit cannot judge which side reduces it, nor by how much.
    let mut reg = registry(CAP, WIDE);
    let mut l = ledger();
    let early = cid();
    reg.insert(placement(early, 100, 5)).unwrap();
    let snap = ResyncSnapshot {
        watermark: WallNs(1_000),
        requested_at: MonoNs(1_000),
        orders: vec![],
        positions: vec![(INST, SignedLots(POS))],
    };
    let cfg = LadderConfig::new(
        Duration::from_secs(1),
        Duration::ZERO,
        Duration::from_secs(10),
        1,
    )
    .unwrap();
    let caps = OrderCaps {
        snapshot_source: SnapshotSource::Trustworthy,
        ..order_caps()
    };
    reg.resync(&cfg, &caps, &snap, key(1)).unwrap();
    into_exit(&mut reg, INST, exits()[0]);
    let straddling = fill(
        Some(early),
        FillIdent::Venue {
            fill: fill_id("x"),
            vid: Some(vid("g")),
            cum_after: Some(lots(5)),
        },
        Side::Buy,
        5,
        false,
    );
    let time = Some(FillTime {
        exch: fbc_core::ExchNs(1_500),
        kind: fbc_core::ExchTsKind::MatchingEngine,
        aligned: WallNs(1_500),
    });
    match l.admit(&straddling, time, MonoNs(1_100)) {
        Admission::Apply(a) => assert_eq!(reg.apply_fill(a), Ok(FillRouted::Unsettled(early))),
        other => panic!("expected the fill accepted, got {other:?}"),
    }
    assert_eq!(reg.position(INST), None);
    for side in [Side::Sell, Side::Buy] {
        assert_eq!(
            reg.place(exit_of(side, 1)),
            Err(refused(ExitRefusal::PositionUnknown(INST)))
        );
    }
}

#[test]
fn every_exit_refusal_says_what_refused_it() {
    let refusals = [
        ExitRefusal::PositionUnknown(INST),
        ExitRefusal::Flat(INST),
        ExitRefusal::Increasing {
            inst: INST,
            side: Side::Buy,
        },
        ExitRefusal::Ordinary {
            inst: INST,
            side: Side::Sell,
        },
        crosses(Side::Sell, 21, POS),
        ExitRefusal::CrossesZero {
            inst: INST,
            side: Side::Sell,
            total: None,
            position: lots(POS),
        },
    ];
    let texts: Vec<String> = refusals
        .iter()
        .map(|r| StateRefusal::Exit(*r).to_string())
        .collect();
    for (r, text) in refusals.iter().zip(&texts) {
        assert!(text.contains("InstrumentId(1)"), "{text}");
        let source: &dyn std::error::Error = r;
        assert_eq!(&source.to_string(), text);
    }
    let unique: std::collections::HashSet<&String> = texts.iter().collect();
    assert_eq!(unique.len(), texts.len(), "{texts:?}");
}
