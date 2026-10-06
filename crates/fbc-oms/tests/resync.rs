//! Resync snapshots (decision 0055): the first resync of a process seeds each market's position
//! and registers our open orders it shows, the position is unknown until then, a later resync
//! compares and never overwrites the inventory, and a fill straddling the snapshot (executed
//! after the request, reported before or after the answer) is counted exactly once.

mod common;

use std::time::Duration;

use common::{cid, fill, fill_id, lots, order_caps, placement, update, vid};
use fbc_core::{
    CidMatch, ClientOrderId, ExchNs, ExchTsKind, FillEvent, FillIdent, InstrumentId, MonoNs,
    Namespace, NewOrder, OrderCaps, Side, SignedLots, SnapshotSource, Ticks, VenueOrderSnapshot,
    VenueOrderState, WallNs,
};
use fbc_oms::{
    Admission, CapRefusal, FillLedger, FillRouted, FillTime, LadderConfig, LedgerConfig,
    MarketCapsConfig, OmsError, OrdState, OrderKey, PermitRefusal, PositionCheck, PreTradeCaps,
    Registry, ResyncError, ResyncReport, ResyncSnapshot,
};

const INST: InstrumentId = InstrumentId::new(1);
const OTHER: InstrumentId = InstrumentId::new(2);
/// The owner's first test values, in lots of $1: a $50 inventory cap and one $11 L0 order
/// resting per side.
const CAP: i64 = 50;
const L0: i64 = 11;
/// A resting cap past any order of the tests that judge the inventory cap alone.
const WIDE: i64 = 1_000_000;
/// The resync's request: on the wall clock (its watermark) and the monotonic clock.
const W: i64 = 1_000;
const REQ: u64 = 1_000;

/// A registry under the inventory cap `CAP` and a resting cap no test's orders reach.
fn registry() -> Registry {
    registry_resting(WIDE)
}

/// A registry under the inventory cap `CAP` and the resting cap `resting`, not seeded.
fn registry_resting(resting: i64) -> Registry {
    Registry::with_caps(
        PreTradeCaps::new()
            .with_market(
                INST,
                MarketCapsConfig {
                    inventory: Some(lots(CAP)),
                    resting: Some(lots(resting)),
                },
            )
            .unwrap(),
    )
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

fn cfg() -> LadderConfig {
    LadderConfig::new(
        Duration::from_secs(1),
        Duration::ZERO,
        Duration::from_secs(10),
        1,
    )
    .unwrap()
}

fn caps(source: SnapshotSource) -> OrderCaps {
    OrderCaps {
        snapshot_source: source,
        ..order_caps()
    }
}

fn key(n: u64) -> OrderKey {
    OrderKey {
        venue: None,
        ingest: n,
    }
}

/// Our open buy `c` of `qty` at 100 as the venue shows it, under `v`, filled `cum`.
fn shown(c: ClientOrderId, v: &str, qty: i64, cum: i64) -> VenueOrderSnapshot {
    VenueOrderSnapshot {
        cid: Some(CidMatch::Ours(c)),
        vid: vid(v),
        inst: INST,
        side: Side::Buy,
        state: VenueOrderState::Open,
        px: Some(Ticks(100)),
        qty: lots(qty),
        cum_filled: lots(cum),
        post_only: None,
        reduce_only: None,
    }
}

/// A snapshot requested at the watermark `W` (monotonic `REQ`), of these orders and positions.
fn snapshot(orders: Vec<VenueOrderSnapshot>, pos: &[(InstrumentId, i64)]) -> ResyncSnapshot {
    snapshot_at(W, REQ, orders, pos)
}

fn snapshot_at(
    w: i64,
    req: u64,
    orders: Vec<VenueOrderSnapshot>,
    pos: &[(InstrumentId, i64)],
) -> ResyncSnapshot {
    ResyncSnapshot {
        watermark: WallNs(w),
        requested_at: MonoNs(req),
        orders,
        positions: pos.iter().map(|&(i, p)| (i, SignedLots(p))).collect(),
    }
}

fn resync(reg: &mut Registry, snap: &ResyncSnapshot) -> ResyncReport {
    resync_with(reg, SnapshotSource::Trustworthy, snap)
}

fn resync_with(reg: &mut Registry, source: SnapshotSource, snap: &ResyncSnapshot) -> ResyncReport {
    reg.resync(&cfg(), &caps(source), snap, key(1)).unwrap()
}

/// A buy fill of `qty` of our order `c` (venue id `v`), named `fid`, bringing the order's
/// cumulative fill to `cum` when given, executed at `exec` on the matching engine when given.
fn order_fill(c: ClientOrderId, v: &str, fid: &str, qty: i64, cum: Option<i64>) -> FillEvent {
    let ident = FillIdent::Venue {
        fill: fill_id(fid),
        vid: Some(vid(v)),
        cum_after: cum.map(lots),
    };
    fill(Some(c), ident, Side::Buy, qty, false)
}

fn exec(at: i64) -> Option<FillTime> {
    Some(FillTime {
        exch: ExchNs(at),
        kind: ExchTsKind::MatchingEngine,
        aligned: WallNs(at),
    })
}

/// Hands `f`, timed `time`, to the ledger at `now` and applies it.
fn apply(
    reg: &mut Registry,
    l: &mut FillLedger,
    f: &FillEvent,
    time: Option<FillTime>,
    now: u64,
) -> FillRouted {
    match l.admit(f, time, MonoNs(now)) {
        Admission::Apply(a) => reg.apply_fill(a).unwrap(),
        other => panic!("expected the fill accepted, got {other:?}"),
    }
}

fn capped(worst: i64) -> OmsError {
    OmsError::Capped(CapRefusal::InventoryCap {
        inst: INST,
        side: Side::Buy,
        worst: Some(lots(worst)),
        cap: lots(CAP),
    })
}

#[test]
fn the_first_resync_seeds_the_positions_and_registers_our_open_orders() {
    let mut reg = registry();
    let (a, gone, other) = (cid(), cid(), cid());
    let mut foreign = shown(cid(), "f", 5, 0);
    foreign.cid = Some(CidMatch::Foreign(Namespace::new(9)));
    let mut ended = shown(gone, "g", 5, 5);
    ended.state = VenueOrderState::Filled;
    let mut elsewhere = shown(other, "o", 3, 0);
    elsewhere.inst = OTHER;
    let snap = snapshot(
        vec![shown(a, "a", 50, 25), foreign, ended, elsewhere],
        &[(INST, 25)],
    );
    let report = resync(&mut reg, &snap);
    // Every market the caps configure, the snapshot names or our orders rest on is seeded;
    // one the snapshot lists no position for is flat.
    assert_eq!(
        report.seeded,
        vec![(INST, SignedLots(25)), (OTHER, SignedLots(0))]
    );
    let mut registered = vec![a, other];
    registered.sort();
    assert_eq!(report.registered, registered);
    assert!(report.checks.is_empty() && report.unsettled.is_empty());
    assert_eq!(reg.position(INST), Some(SignedLots(25)));
    assert_eq!(reg.position(OTHER), Some(SignedLots(0)));
    // Our open order is registered as the venue shows it; another namespace's order and an
    // ended one are not.
    let rec = reg.get(a).unwrap();
    assert_eq!(rec.state(), OrdState::PartiallyFilled);
    assert_eq!(rec.vid(), Some(&vid("a")));
    assert_eq!((rec.cum_venue(), rec.cum_fills()), (lots(25), lots(25)));
    assert!(rec.from_snapshot());
    assert_eq!(reg.cid_of(&vid("a")), Some(a));
    assert!(reg.get(gone).is_none());
    assert_eq!(reg.len(), 2);
    // An earlier run's order is cancelled, never amended: its placement is not fully known.
    assert_eq!(reg.live(a).err(), Some(PermitRefusal::FromSnapshot(a)));
    assert!(reg.cancellable(a).is_ok());
    // The inventory cap counts its 25 unfilled lots once beside the seeded 25 (not its 25
    // filled ones again): a bid of one more lot is refused at 51, not 76.
    assert_eq!(reg.place(placement(cid(), 100, 1)), Err(capped(51)));
    // Ended, it counts nothing more: its filled part is in the seeded position.
    let done = update(Some(a), common::canceled(), 25);
    reg.apply_update(&done, key(2));
    reg.place(placement(cid(), 100, 25)).unwrap();
    // A market is seeded once: the next resync compares.
    let report = resync(&mut reg, &snapshot(vec![], &[(INST, 25)]));
    assert!(report.seeded.is_empty() && report.registered.is_empty());
    assert_eq!(
        report.checks,
        vec![
            PositionCheck::Agrees {
                inst: INST,
                position: SignedLots(25)
            },
            PositionCheck::Agrees {
                inst: OTHER,
                position: SignedLots(0)
            },
        ]
    );
}

#[test]
fn an_order_a_resync_registers_counts_against_the_resting_cap() {
    let mut reg = registry_resting(L0);
    let earlier = cid();
    let report = resync(
        &mut reg,
        &snapshot(vec![shown(earlier, "e", L0, 0)], &[(INST, 0)]),
    );
    assert_eq!(report.registered, vec![earlier]);
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(L0)));
    // An earlier run's bid fills the side's one L0 order: one more lot is refused at 12.
    assert_eq!(
        reg.place(placement(cid(), 100, 1)),
        Err(OmsError::Capped(CapRefusal::RestingCap {
            inst: INST,
            side: Side::Buy,
            resting: Some(lots(L0 + 1)),
            cap: lots(L0),
        }))
    );
    // The other side is its own: an L0 offer is admitted.
    let mut offer = placement(cid(), 101, L0);
    offer.side = Side::Sell;
    reg.place(offer).unwrap();
    // Ended, it rests nothing: an L0 bid is admitted.
    reg.apply_update(&update(Some(earlier), common::canceled(), 0), key(2));
    reg.place(placement(cid(), 100, L0)).unwrap();
}

#[test]
fn the_position_is_unknown_and_nothing_is_built_until_the_first_resync_applies() {
    let mut reg = registry();
    let mut l = ledger();
    let unknown = OmsError::Capped(CapRefusal::PositionUnknown(INST));
    assert_eq!(reg.position(INST), None);
    assert!(!reg.position_known(INST));
    assert_eq!(reg.place(placement(cid(), 100, 1)), Err(unknown.clone()));
    // A fill before the seed moves no position: it stays unknown, and only a resync's
    // snapshot can place the fill, so a seed by hand is refused.
    let stray = cid();
    let f = order_fill(stray, "s", "early", 4, Some(4));
    assert_eq!(
        apply(&mut reg, &mut l, &f, None, 10),
        FillRouted::OursUntracked(stray)
    );
    assert_eq!(reg.position(INST), None);
    assert_eq!(
        reg.seed_position(INST, SignedLots(4)),
        Err(OmsError::PositionMoved(INST))
    );
    // A resync refused leaves it unknown.
    let twice = snapshot(vec![], &[(INST, 4), (INST, 4)]);
    assert_eq!(
        reg.resync(&cfg(), &caps(SnapshotSource::Trustworthy), &twice, key(1)),
        Err(ResyncError::DuplicatePosition(INST))
    );
    assert_eq!(reg.place(placement(cid(), 100, 1)), Err(unknown));
    // Applied, the position is the venue's and orders are admitted under it.
    let report = resync(&mut reg, &snapshot(vec![], &[(INST, 4)]));
    assert_eq!(report.seeded, vec![(INST, SignedLots(4))]);
    assert_eq!(reg.position(INST), Some(SignedLots(4)));
    assert!(reg.position_known(INST));
    reg.place(placement(cid(), 100, 1)).unwrap();
    assert_eq!(
        reg.seed_position(INST, SignedLots(0)),
        Err(OmsError::PositionSeeded(INST))
    );
}

#[test]
fn a_later_resync_reports_a_desync_and_never_changes_the_inventory() {
    let mut reg = registry();
    let mut l = ledger();
    resync(&mut reg, &snapshot(vec![], &[(INST, 0)]));
    let a = cid();
    reg.place(placement(a, 100, 20)).unwrap();
    // Before the next request: 5 lots; after it, 3 executed before its watermark and 2 after.
    apply(
        &mut reg,
        &mut l,
        &order_fill(a, "a", "f1", 5, Some(5)),
        exec(1_500),
        2_000,
    );
    apply(
        &mut reg,
        &mut l,
        &order_fill(a, "a", "f2", 3, Some(8)),
        exec(2_900),
        3_100,
    );
    apply(
        &mut reg,
        &mut l,
        &order_fill(a, "a", "f3", 2, Some(10)),
        exec(3_200),
        3_200,
    );
    assert_eq!(reg.inventory(INST), SignedLots(10));
    // As of the watermark the ledger holds 8: a venue showing 8 agrees.
    let report = resync(&mut reg, &snapshot_at(3_000, 3_000, vec![], &[(INST, 8)]));
    assert_eq!(
        report.checks,
        vec![PositionCheck::Agrees {
            inst: INST,
            position: SignedLots(8)
        }]
    );
    // A venue showing 6 is a desync, reported; the inventory is unchanged.
    let report = resync(&mut reg, &snapshot_at(3_000, 3_000, vec![], &[(INST, 6)]));
    assert_eq!(
        report.checks,
        vec![PositionCheck::Desync {
            inst: INST,
            venue: SignedLots(6),
            ledger: Some(SignedLots(8))
        }]
    );
    assert_eq!(reg.inventory(INST), SignedLots(10));
    assert_eq!(reg.position(INST), Some(SignedLots(10)));
    // A resync requested before one already compared is not compared.
    let report = resync(&mut reg, &snapshot_at(2_500, 2_500, vec![], &[(INST, 5)]));
    assert_eq!(report.checks, vec![PositionCheck::Stale(INST)]);
    // A market not listed is flat; an untimed fill after the request is after the watermark.
    let report = resync(&mut reg, &snapshot_at(4_000, 4_000, vec![], &[]));
    assert_eq!(
        report.checks,
        vec![PositionCheck::Desync {
            inst: INST,
            venue: SignedLots(0),
            ledger: Some(SignedLots(10))
        }]
    );
    apply(
        &mut reg,
        &mut l,
        &order_fill(a, "a", "f4", 1, Some(11)),
        None,
        4_100,
    );
    let report = resync(&mut reg, &snapshot_at(4_050, 4_050, vec![], &[(INST, 10)]));
    assert_eq!(
        report.checks,
        vec![PositionCheck::Agrees {
            inst: INST,
            position: SignedLots(10)
        }]
    );
    assert_eq!(reg.inventory(INST), SignedLots(11));
}

/// A fill of an earlier run's order resting at the request, executed after the request (at
/// 1 500) and so straddling the snapshot, delivered before the answer (at 900 the request is
/// not yet sent; at 1 100 it is) or after it.
#[test]
fn a_fill_between_the_request_and_the_snapshot_counts_exactly_once() {
    for before in [true, false] {
        // The snapshot shows the order filled 10 of 20 and the position 10: it holds the fill
        // that took the order from 6 to 10.
        let mut reg = registry();
        let mut l = ledger();
        let a = cid();
        let f = order_fill(a, "a", "x", 4, Some(10));
        let snap = snapshot(vec![shown(a, "a", 20, 10)], &[(INST, 10)]);
        if before {
            apply(&mut reg, &mut l, &f, exec(1_500), 1_100);
            let report = resync(&mut reg, &snap);
            assert_eq!(report.seeded, vec![(INST, SignedLots(10))]);
        } else {
            resync(&mut reg, &snap);
            assert_eq!(
                apply(&mut reg, &mut l, &f, exec(1_500), 1_100),
                FillRouted::InSnapshot(a)
            );
        }
        assert_eq!(reg.position(INST), Some(SignedLots(10)), "before: {before}");
        assert_eq!(reg.get(a).unwrap().cum_fills(), lots(10));
        // Delivered again, the ledger holds it.
        assert!(matches!(
            l.admit(&f, exec(1_500), MonoNs(1_200)),
            Admission::Duplicate
        ));

        // The snapshot shows the order filled 6: the fill came after it and counts on top.
        let mut reg = registry();
        let mut l = ledger();
        let snap = snapshot(vec![shown(a, "a", 20, 6)], &[(INST, 6)]);
        if before {
            apply(&mut reg, &mut l, &f, exec(1_500), 1_100);
            let report = resync(&mut reg, &snap);
            assert_eq!(report.seeded, vec![(INST, SignedLots(10))]);
        } else {
            resync(&mut reg, &snap);
            assert!(matches!(
                apply(&mut reg, &mut l, &f, exec(1_500), 1_100),
                FillRouted::Ours(c, _) if c == a
            ));
        }
        assert_eq!(reg.position(INST), Some(SignedLots(10)), "before: {before}");
        assert_eq!(reg.get(a).unwrap().cum_fills(), lots(10));
        // The order's 10 filled lots count once against the cap: 10 resting beside 10 held.
        assert_eq!(reg.place(placement(cid(), 100, 31)), Err(capped(51)));
    }
}

#[test]
fn a_fill_of_an_order_the_snapshot_does_not_show_is_in_it_from_a_trustworthy_source() {
    for before in [true, false] {
        // The order ended between the request and the venue's read: the snapshot shows the
        // position with its fill and not the order.
        let mut reg = registry();
        let mut l = ledger();
        let gone = cid();
        let f = order_fill(gone, "g", "x", 5, Some(5));
        let snap = snapshot(vec![], &[(INST, 5)]);
        if before {
            apply(&mut reg, &mut l, &f, exec(1_500), 1_100);
            resync(&mut reg, &snap);
        } else {
            resync(&mut reg, &snap);
            assert_eq!(
                apply(&mut reg, &mut l, &f, exec(1_500), 1_100),
                FillRouted::InSnapshot(gone)
            );
        }
        assert_eq!(reg.position(INST), Some(SignedLots(5)), "before: {before}");
    }
    // An order placed after the seed is never in it.
    let mut reg = registry();
    let mut l = ledger();
    resync(&mut reg, &snapshot(vec![], &[]));
    let a = cid();
    reg.place(placement(a, 100, 5)).unwrap();
    apply(
        &mut reg,
        &mut l,
        &order_fill(a, "a", "x", 5, Some(5)),
        exec(900),
        1_100,
    );
    assert_eq!(reg.position(INST), Some(SignedLots(5)));
}

#[test]
fn a_fill_nothing_places_leaves_the_market_unknown() {
    // A fill of an order the registry held before the seed and the snapshot does not show,
    // executed after the request, may or may not be in it: the order may have reached the
    // venue after its read.
    let mut reg = registry();
    let mut l = ledger();
    let early = cid();
    reg.insert(placement(early, 100, 5)).unwrap();
    let f = order_fill(early, "g", "x", 5, Some(5));
    apply(&mut reg, &mut l, &f, exec(1_500), 1_100);
    let snap = snapshot(vec![], &[(INST, 5)]);
    let report = resync(&mut reg, &snap);
    assert!(report.seeded.is_empty());
    assert_eq!(report.unsettled, vec![(INST, f.key())]);
    assert_eq!(reg.position(INST), None);
    // A resync requested after it arrived places it: the snapshot holds it.
    let later = snapshot_at(2_000, 2_000, vec![], &[(INST, 5)]);
    let report = resync(&mut reg, &later);
    assert_eq!(report.seeded, vec![(INST, SignedLots(5))]);
    assert_eq!(reg.position(INST), Some(SignedLots(5)));
    // Executed at or before the watermark, it is in the snapshot.
    let mut reg = registry();
    let mut l = ledger();
    reg.insert(placement(early, 100, 5)).unwrap();
    apply(&mut reg, &mut l, &f, exec(1_000), 1_100);
    resync(&mut reg, &snap);
    assert_eq!(reg.position(INST), Some(SignedLots(5)));

    // After the seed, such a fill makes the position unknown for the rest of the process.
    let mut reg = registry();
    let mut l = ledger();
    reg.insert(placement(early, 100, 5)).unwrap();
    resync(&mut reg, &snapshot(vec![], &[]));
    assert_eq!(
        apply(&mut reg, &mut l, &f, exec(1_500), 1_100),
        FillRouted::Unsettled(early)
    );
    assert_eq!(reg.position(INST), None);
    assert_eq!(
        reg.place(placement(cid(), 100, 1)),
        Err(OmsError::Capped(CapRefusal::PositionUnknown(INST)))
    );
    let report = resync(&mut reg, &snapshot_at(3_000, 3_000, vec![], &[(INST, 5)]));
    assert!(report.seeded.is_empty());
    assert_eq!(report.checks, vec![PositionCheck::Unsettled(INST)]);
    assert_eq!(
        reg.seed_position(INST, SignedLots(5)),
        Err(OmsError::PositionSeeded(INST))
    );
    // Later fills still count on the ledger, the position staying unknown.
    let g2 = cid();
    apply(
        &mut reg,
        &mut l,
        &order_fill(g2, "h", "y", 1, None),
        None,
        3_100,
    );
    assert_eq!(reg.position(INST), None);
}

#[test]
fn a_fill_without_a_cumulative_fill_is_placed_by_its_time() {
    // The snapshot shows the order; the fill reports no cumulative fill.
    let a = cid();
    let snap = snapshot(vec![shown(a, "a", 20, 10)], &[(INST, 10)]);
    // Executed before the watermark: in the snapshot.
    let mut reg = registry();
    let mut l = ledger();
    resync(&mut reg, &snap);
    let f = order_fill(a, "a", "x", 4, None);
    assert_eq!(
        apply(&mut reg, &mut l, &f, exec(900), 1_100),
        FillRouted::InSnapshot(a)
    );
    assert_eq!(reg.position(INST), Some(SignedLots(10)));
    // Delivered before the request was sent: in the snapshot, whatever its time.
    let mut reg = registry();
    let mut l = ledger();
    apply(&mut reg, &mut l, &f, None, 900);
    resync(&mut reg, &snap);
    assert_eq!(reg.position(INST), Some(SignedLots(10)));
    // Executed after it, or untimed, nothing places it.
    let mut reg = registry();
    let mut l = ledger();
    apply(&mut reg, &mut l, &f, None, 1_100);
    let report = resync(&mut reg, &snap);
    assert_eq!(report.unsettled, vec![(INST, f.key())]);
    assert_eq!(reg.position(INST), None);
    // The order is registered all the same, so a cancel reaches it.
    assert_eq!(report.registered, vec![a]);
    assert!(reg.get(a).unwrap().from_snapshot());
}

#[test]
fn an_order_registered_before_the_seed_counts_the_fill_the_snapshot_shows() {
    // The consumer registered an order before the seed; its fills arrived before the answer.
    let mut reg = registry();
    let mut l = ledger();
    let a = cid();
    reg.insert(placement(a, 100, 20)).unwrap();
    apply(
        &mut reg,
        &mut l,
        &order_fill(a, "a", "f1", 4, Some(4)),
        exec(900),
        900,
    );
    apply(
        &mut reg,
        &mut l,
        &order_fill(a, "a", "f2", 6, Some(10)),
        exec(1_500),
        1_100,
    );
    apply(
        &mut reg,
        &mut l,
        &order_fill(a, "a", "f3", 2, Some(12)),
        exec(1_600),
        1_200,
    );
    // The snapshot shows it filled 10: f3 came after it.
    let report = resync(
        &mut reg,
        &snapshot(vec![shown(a, "a", 20, 10)], &[(INST, 10)]),
    );
    assert_eq!(report.seeded, vec![(INST, SignedLots(12))]);
    assert!(report.registered.is_empty());
    let rec = reg.get(a).unwrap();
    assert_eq!(rec.cum_fills(), lots(12));
    assert!(!rec.from_snapshot());
    assert_eq!(rec.state(), OrdState::PartiallyFilled);
    // Its later fills count; one the snapshot already showed does not.
    apply(
        &mut reg,
        &mut l,
        &order_fill(a, "a", "f4", 8, Some(20)),
        exec(2_000),
        2_000,
    );
    assert_eq!(reg.position(INST), Some(SignedLots(20)));
    assert_eq!(
        reg.get(a).unwrap().state(),
        OrdState::Terminal(fbc_oms::TerminalKind::Filled)
    );
    // A fill of an order not shown, executed after the watermark and arriving after the seed:
    // the order may have reached the venue after its read, so nothing places the fill.
    let b = cid();
    let mut reg2 = registry();
    let mut l2 = ledger();
    reg2.insert(placement(b, 100, 5)).unwrap();
    resync(&mut reg2, &snapshot(vec![], &[(INST, 5)]));
    assert_eq!(
        apply(
            &mut reg2,
            &mut l2,
            &order_fill(b, "b", "g", 5, Some(5)),
            exec(1_500),
            1_100
        ),
        FillRouted::Unsettled(b)
    );
    assert_eq!(reg2.position(INST), None);
    assert_eq!(reg2.get(b).unwrap().cum_fills(), lots(5));
    // Its seeded fill count covering the order ends it.
    let mut reg3 = registry();
    let mut l3 = ledger();
    let c = cid();
    reg3.insert(placement(c, 100, 10)).unwrap();
    apply(
        &mut reg3,
        &mut l3,
        &order_fill(c, "c", "h", 4, Some(10)),
        exec(1_500),
        1_100,
    );
    resync(
        &mut reg3,
        &snapshot(vec![shown(c, "c", 10, 6)], &[(INST, 6)]),
    );
    assert_eq!(
        reg3.get(c).unwrap().state(),
        OrdState::Terminal(fbc_oms::TerminalKind::Filled)
    );
    assert_eq!(reg3.position(INST), Some(SignedLots(10)));
}

#[test]
fn every_resync_reconciles_the_orders_the_registry_holds() {
    let mut reg = registry();
    resync(&mut reg, &snapshot(vec![], &[]));
    let a = cid();
    reg.place(placement(a, 100, 10)).unwrap();
    // The snapshot shows it resting, filled 3: an order update, which never moves the
    // inventory (decision 0005, I3).
    let report = resync(
        &mut reg,
        &snapshot_at(2_000, 2_000, vec![shown(a, "a", 10, 3)], &[]),
    );
    let rec = reg.get(a).unwrap();
    assert_eq!(rec.state(), OrdState::PartiallyFilled);
    assert_eq!((rec.cum_venue(), rec.cum_fills()), (lots(3), lots(0)));
    assert_eq!(reg.inventory(INST), SignedLots(0));
    assert!(report.untracked.is_empty());
    // An open order of ours the registry does not hold, on a seeded market, is reported for
    // 0005's I7, not registered; one whose venue id another order of ours holds neither.
    let (orphan, clash) = (cid(), cid());
    let report = resync(
        &mut reg,
        &snapshot_at(
            3_000,
            3_000,
            vec![shown(orphan, "o", 5, 0), shown(clash, "a", 5, 0)],
            &[],
        ),
    );
    assert_eq!(report.untracked, vec![(orphan, vid("o"))]);
    assert!(reg.get(orphan).is_none() && reg.get(clash).is_none());
    // At the seed, an order whose venue id another order holds is not registered either.
    let mut reg = registry();
    let b = cid();
    reg.insert(placement(b, 100, 5)).unwrap();
    let mut ack = update(Some(b), VenueOrderState::Open, 0);
    ack.vid = Some(vid("b"));
    reg.apply_update(&ack, key(0));
    let report = resync(&mut reg, &snapshot(vec![shown(clash, "b", 5, 0)], &[]));
    assert!(report.registered.is_empty());
    assert!(reg.get(clash).is_none());
}

#[test]
fn a_resync_applies_to_the_unknown_ladder_too() {
    let mut reg = registry();
    resync(&mut reg, &snapshot(vec![], &[]));
    let a = cid();
    reg.place(placement(a, 100, 5)).unwrap();
    reg.placement_sent(a, MonoNs(0), WallNs(0)).unwrap();
    // Unanswered past the intent timeout: on the ladder.
    let plan = reg.ladder(&cfg(), &order_caps(), MonoNs(2_000_000_000));
    assert_eq!(plan.escalated, vec![a]);
    // Absent from a trustworthy snapshot past the settle time: lost.
    let report = resync(&mut reg, &snapshot_at(5_000, 5_000, vec![], &[]));
    assert_eq!(report.ladder.lost, vec![a]);
    // Shown, an order on the ladder is resolved there and not applied a second time.
    let b = cid();
    reg.place(placement(b, 100, 5)).unwrap();
    reg.placement_sent(b, MonoNs(0), WallNs(0)).unwrap();
    reg.ladder(&cfg(), &order_caps(), MonoNs(2_000_000_000));
    let report = resync(
        &mut reg,
        &snapshot_at(6_000, 6_000, vec![shown(b, "b", 5, 0)], &[]),
    );
    assert_eq!(report.ladder.resolved, vec![(b, OrdState::Open)]);
}

#[test]
fn a_resync_that_cannot_be_applied_is_refused_whole() {
    let a = cid();
    let mut reg = registry();
    let twice = snapshot(vec![shown(a, "a", 5, 0), shown(a, "b", 5, 0)], &[]);
    assert_eq!(
        reg.resync(&cfg(), &order_caps(), &twice, key(1)),
        Err(ResyncError::DuplicateOrder(a))
    );
    assert!(reg.is_empty() && reg.position(INST).is_none());
    // A position past a lot count once the fill after the snapshot is added.
    let mut l = ledger();
    apply(
        &mut reg,
        &mut l,
        &order_fill(a, "a", "x", 2, Some(i64::MAX)),
        exec(1_500),
        1_100,
    );
    let huge = snapshot(vec![shown(a, "a", 5, 0)], &[(INST, i64::MAX)]);
    assert_eq!(
        reg.resync(&cfg(), &order_caps(), &huge, key(1)),
        Err(ResyncError::Overflow(INST))
    );
    // An order's fill count past a lot count: the cumulative fill shown plus the fill after.
    let near = snapshot(vec![shown(a, "a", i64::MAX, i64::MAX - 1)], &[]);
    assert_eq!(
        reg.resync(&cfg(), &order_caps(), &near, key(1)),
        Err(ResyncError::Overflow(INST))
    );
    assert!(reg.is_empty() && reg.position(INST).is_none());
    // The refusals say what was refused.
    assert!(
        ResyncError::DuplicatePosition(INST)
            .to_string()
            .contains("twice")
    );
    assert!(ResyncError::DuplicateOrder(a).to_string().contains("twice"));
    assert!(
        ResyncError::Overflow(INST)
            .to_string()
            .contains("overflows")
    );
    let err: &dyn std::error::Error = &ResyncError::Overflow(INST);
    assert!(err.source().is_none());
}

#[test]
fn a_ledger_position_past_a_lot_count_is_reported_as_such() {
    // Seeded at the largest position; then a sell of all of it executed after the next
    // watermark and a buy back executed before it, both arriving after its request: as of the
    // watermark the ledger would hold twice the largest position.
    let mut reg = registry();
    let mut l = ledger();
    resync(&mut reg, &snapshot(vec![], &[(INST, i64::MAX)]));
    let (s, b) = (cid(), cid());
    let sell = NewOrder {
        side: Side::Sell,
        ..placement(s, 100, i64::MAX)
    };
    reg.insert(sell).unwrap();
    reg.insert(placement(b, 100, i64::MAX)).unwrap();
    let mut out = order_fill(s, "s", "s", i64::MAX, None);
    out.side = Side::Sell;
    apply(&mut reg, &mut l, &out, exec(3_500), 3_100);
    apply(
        &mut reg,
        &mut l,
        &order_fill(b, "b", "b", i64::MAX, None),
        exec(2_900),
        3_200,
    );
    assert_eq!(reg.position(INST), Some(SignedLots(i64::MAX)));
    let report = resync(&mut reg, &snapshot_at(3_000, 3_000, vec![], &[(INST, 0)]));
    assert_eq!(
        report.checks,
        vec![PositionCheck::Desync {
            inst: INST,
            venue: SignedLots(0),
            ledger: None
        }]
    );
}

#[test]
fn an_order_shown_without_our_client_id_is_still_shown() {
    // The venue's snapshot names the order by its venue id only: a fill of it after the read
    // is not in the snapshot, though no order under its client id is shown.
    for before in [true, false] {
        let mut reg = registry();
        let mut l = ledger();
        let a = cid();
        let mut bare = shown(a, "a", 20, 6);
        bare.cid = None;
        let snap = snapshot(vec![bare], &[(INST, 6)]);
        let f = order_fill(a, "a", "x", 4, Some(10));
        if before {
            apply(&mut reg, &mut l, &f, exec(1_500), 1_100);
            let report = resync(&mut reg, &snap);
            assert!(report.registered.is_empty());
        } else {
            resync(&mut reg, &snap);
            assert_eq!(
                apply(&mut reg, &mut l, &f, exec(1_500), 1_100),
                FillRouted::OursUntracked(a)
            );
        }
        assert_eq!(reg.position(INST), Some(SignedLots(10)), "before: {before}");
    }
}

/// Reviewer B's P1 on PR #80: an order the registry held before the first resync may reach the
/// venue after it read the account, so the snapshot not showing it says nothing of its fills
/// executed after the watermark. Counted as in the snapshot, a 50-lot fill would vanish and
/// the $50 inventory cap admit 50 lots more.
#[test]
fn a_fill_of_an_order_registered_before_the_seed_and_not_shown_is_never_assumed_in_it() {
    // Its fill arrives after the answer: the position becomes unknown, nothing is built.
    let mut reg = registry();
    let mut l = ledger();
    let b = cid();
    reg.insert(placement(b, 100, CAP)).unwrap();
    resync(&mut reg, &snapshot(vec![], &[(INST, 0)]));
    let f = order_fill(b, "b", "x", CAP, Some(CAP));
    assert_eq!(
        apply(&mut reg, &mut l, &f, exec(1_500), 1_100),
        FillRouted::Unsettled(b)
    );
    assert_eq!(reg.position(INST), None);
    assert_eq!(
        reg.place(placement(cid(), 100, CAP)),
        Err(OmsError::Capped(CapRefusal::PositionUnknown(INST)))
    );
    // Its fill arrives before the answer: the market is not seeded.
    let mut reg = registry();
    let mut l = ledger();
    reg.insert(placement(b, 100, CAP)).unwrap();
    apply(&mut reg, &mut l, &f, exec(1_500), 1_100);
    let report = resync(&mut reg, &snapshot(vec![], &[(INST, 0)]));
    assert!(report.seeded.is_empty());
    assert_eq!(report.unsettled, vec![(INST, f.key())]);
    assert_eq!(reg.position(INST), None);
    // Executed by the watermark, the snapshot holds it all the same.
    let mut reg = registry();
    let mut l = ledger();
    reg.insert(placement(b, 100, CAP)).unwrap();
    resync(&mut reg, &snapshot(vec![], &[(INST, CAP)]));
    assert_eq!(
        apply(&mut reg, &mut l, &f, exec(W), 1_100),
        FillRouted::InSnapshot(b)
    );
    assert_eq!(reg.position(INST), Some(SignedLots(CAP)));
}

/// Reviewer A's finding on PR #80: a snapshot source that can be stale or incomplete, or none,
/// seeds no market, so order entry stays held until a trustworthy resync.
#[test]
fn a_resync_seeds_nothing_unless_the_snapshot_source_is_trustworthy() {
    for source in [SnapshotSource::Untrustworthy, SnapshotSource::None] {
        let mut reg = registry();
        let report = resync_with(&mut reg, source, &snapshot(vec![], &[(INST, 0)]));
        assert!(report.untrustworthy, "{source:?}");
        assert!(report.seeded.is_empty() && report.unsettled.is_empty());
        assert_eq!(reg.position(INST), None);
        assert_eq!(
            reg.place(placement(cid(), 100, 1)),
            Err(OmsError::Capped(CapRefusal::PositionUnknown(INST)))
        );
        // A trustworthy one then seeds it.
        let report = resync(&mut reg, &snapshot_at(2_000, 2_000, vec![], &[(INST, 0)]));
        assert!(!report.untrustworthy);
        assert_eq!(report.seeded, vec![(INST, SignedLots(0))]);
        reg.place(placement(cid(), 100, 1)).unwrap();
    }
}

/// Reviewer B's P2 on PR #80: our open orders on a market the resync does not seed must still
/// be registered, or a Stop's cancel of every order the registry holds cannot reach them.
#[test]
fn our_open_orders_on_a_market_left_unseeded_are_registered_and_cancellable() {
    // Left unseeded by a fill nothing places (shown, no cumulative fill, untimed).
    let mut reg = registry();
    let mut l = ledger();
    let a = cid();
    let f = order_fill(a, "a", "x", 4, None);
    apply(&mut reg, &mut l, &f, None, 1_100);
    let report = resync(
        &mut reg,
        &snapshot(vec![shown(a, "a", 20, 10)], &[(INST, 10)]),
    );
    assert_eq!(report.unsettled, vec![(INST, f.key())]);
    assert_eq!(report.registered, vec![a]);
    assert_eq!(reg.position(INST), None);
    assert_eq!(reg.cid_of(&vid("a")), Some(a));
    assert_eq!(reg.cancel_many(&[a], &order_caps()).commands.len(), 1);
    // A later resync seeds the market, counting the order's shown fill and the one after.
    let report = resync(
        &mut reg,
        &snapshot_at(2_000, 2_000, vec![shown(a, "a", 20, 14)], &[(INST, 14)]),
    );
    assert_eq!(report.seeded, vec![(INST, SignedLots(14))]);
    assert_eq!(reg.get(a).unwrap().cum_fills(), lots(14));

    // Left unseeded by an untrustworthy source.
    let mut reg = registry();
    let b = cid();
    let report = resync_with(
        &mut reg,
        SnapshotSource::Untrustworthy,
        &snapshot(vec![shown(b, "b", 5, 0)], &[]),
    );
    assert_eq!(report.registered, vec![b]);
    assert_eq!(reg.cancel_many(&[b], &order_caps()).commands.len(), 1);

    // On a market whose position a fill made unknown after the seed, an open order of ours
    // the registry does not hold is reported (0005's I7), as on a seeded one.
    let mut reg = registry();
    let mut l = ledger();
    let early = cid();
    reg.insert(placement(early, 100, 5)).unwrap();
    resync(&mut reg, &snapshot(vec![], &[]));
    apply(
        &mut reg,
        &mut l,
        &order_fill(early, "e", "y", 5, Some(5)),
        exec(1_500),
        1_100,
    );
    assert_eq!(reg.position(INST), None);
    let orphan = cid();
    let report = resync(
        &mut reg,
        &snapshot_at(3_000, 3_000, vec![shown(orphan, "o", 5, 0)], &[]),
    );
    assert_eq!(report.untracked, vec![(orphan, vid("o"))]);
}
