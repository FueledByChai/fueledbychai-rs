//! The fill ledger and the fills it accepts (decision 0005, I3): deduplication, replays under
//! the session-start watermark and the retention horizon, the configuration it refuses, and
//! what an accepted fill does to its order and to the inventory.

mod common;

use std::sync::OnceLock;
use std::time::Duration;

use common::{cid, fill, ident, lots, placement, update};
use fbc_core::{
    AckLevel, CidMatch, ClientOrderId, ExchNs, ExchTsKind, FillEvent, FillIdent, InstrumentId,
    ItemRef, Lots, MonoNs, Namespace, Side, SignedLots, SubmitOutcome, VenueOrderState, WallNs,
};
use fbc_oms::{
    AcceptedFill, Admission, FillApplied, FillLedger, FillRouted, FillTime, Horizon, LedgerConfig,
    LedgerConfigError, OmsError, OrdState, OrderKey, OrderOp, Registry, ReplayCounts, TerminalKind,
};

const INST: InstrumentId = InstrumentId::new(1);
const WATERMARK: WallNs = WallNs(1_000);

fn ledger(max_entries: usize, max_age_ns: u64) -> FillLedger {
    FillLedger::new(
        LedgerConfig {
            max_age: Duration::from_nanos(max_age_ns),
            max_entries,
        },
        WATERMARK,
    )
    .unwrap()
}

/// A matching-engine time `exch`, aligned to the same wall-clock number.
fn engine(exch: i64) -> Option<FillTime> {
    timed(exch, ExchTsKind::MatchingEngine)
}

fn timed(exch: i64, kind: ExchTsKind) -> Option<FillTime> {
    Some(FillTime {
        exch: ExchNs(exch),
        kind,
        aligned: WallNs(exch),
    })
}

/// A client id of our namespace under which no test registers an order.
fn stray() -> ClientOrderId {
    static STRAY: OnceLock<ClientOrderId> = OnceLock::new();
    *STRAY.get_or_init(cid)
}

/// A live (`replay` false) or replayed buy of `qty` named by fill id `fid`, for our namespace's
/// client id of an order the registry does not hold: counted in the inventory only.
fn untracked(fid: &str, qty: i64, replay: bool) -> FillEvent {
    fill(Some(stray()), ident(fid), Side::Buy, qty, replay)
}

fn accepted<'l, 'f>(admission: Admission<'l, 'f>) -> AcceptedFill<'l, 'f> {
    match admission {
        Admission::Apply(a) => a,
        other => panic!("expected the fill accepted, got {other:?}"),
    }
}

fn key(o: u64) -> OrderKey {
    OrderKey {
        venue: None,
        ingest: o,
    }
}

// ---- configuration ----

#[test]
fn a_ledger_that_would_hold_nothing_is_refused() {
    let zero_age = LedgerConfig {
        max_age: Duration::ZERO,
        max_entries: 8,
    };
    let zero_entries = LedgerConfig {
        max_age: Duration::from_secs(60),
        max_entries: 0,
    };
    let e = FillLedger::new(zero_age, WATERMARK).unwrap_err();
    assert_eq!(e, LedgerConfigError::ZeroAge);
    assert_eq!(e.to_string(), "the fill ledger's max_age is zero");
    let e = FillLedger::new(zero_entries, WATERMARK).unwrap_err();
    assert_eq!(e, LedgerConfigError::ZeroEntries);
    assert_eq!(e.to_string(), "the fill ledger's max_entries is zero");
    let as_error: &dyn std::error::Error = &e;
    assert!(as_error.source().is_none());
}

#[test]
fn a_new_ledger_holds_nothing_and_vouches_for_everything() {
    let l = ledger(4, 100);
    assert!(l.is_empty());
    assert_eq!(l.len(), 0);
    assert_eq!(l.watermark(), WATERMARK);
    assert_eq!(l.horizon(), Horizon::Full);
    assert_eq!(l.replays(), ReplayCounts::default());
}

// ---- deduplication ----

#[test]
fn a_fill_delivered_twice_moves_inventory_once() {
    let mut l = ledger(4, 100);
    let mut reg = Registry::new();
    let f = untracked("f1", 3, false);
    let a = accepted(l.admit(&f, engine(1_100), MonoNs(0)));
    assert_eq!(a.fill(), &f);
    assert!(format!("{a:?}").contains("AcceptedFill"));
    assert_eq!(reg.apply_fill(a), Ok(FillRouted::OursUntracked(stray())));
    assert!(l.contains(&f.key()));
    assert_eq!(l.len(), 1);
    assert!(matches!(
        l.admit(&f, engine(1_100), MonoNs(1)),
        Admission::Duplicate
    ));
    // A replay of a fill the ledger holds is a counted duplicate.
    let again = untracked("f1", 3, true);
    assert!(matches!(
        l.admit(&again, engine(1_100), MonoNs(2)),
        Admission::Duplicate
    ));
    assert_eq!(l.replays().duplicate, 1);
    assert_eq!(reg.inventory(INST), SignedLots(3));
}

#[test]
fn a_fill_without_a_fill_id_is_keyed_by_its_order_and_cumulative_quantity() {
    let mut l = ledger(4, 100);
    let mut reg = Registry::new();
    let derived = |cum: i64, qty: i64| {
        let id = FillIdent::Derived {
            vid: common::vid("v9"),
            cum_after: lots(cum),
        };
        fill(Some(stray()), id, Side::Sell, qty, false)
    };
    reg.apply_fill(accepted(l.admit(&derived(2, 2), None, MonoNs(0))))
        .unwrap();
    assert!(matches!(
        l.admit(&derived(2, 2), None, MonoNs(0)),
        Admission::Duplicate
    ));
    reg.apply_fill(accepted(l.admit(&derived(5, 3), None, MonoNs(0))))
        .unwrap();
    assert_eq!(reg.inventory(INST), SignedLots(-5));
}

#[test]
fn an_accepted_fill_not_applied_leaves_nothing_in_the_ledger() {
    let mut l = ledger(4, 100);
    let f = untracked("f1", 1, true);
    {
        let unapplied = accepted(l.admit(&f, engine(1_100), MonoNs(0)));
        assert_eq!(unapplied.fill(), &f);
    }
    assert!(l.is_empty());
    assert_eq!(l.replays(), ReplayCounts::default());
    // So it is accepted again, and counts once it is applied.
    let mut reg = Registry::new();
    reg.apply_fill(accepted(l.admit(&f, engine(1_100), MonoNs(1))))
        .unwrap();
    assert!(l.contains(&f.key()));
    assert_eq!(l.replays().applied, 1);
}

#[test]
fn a_registry_takes_fills_from_one_ledger_only() {
    let (mut first, mut second) = (ledger(4, 100), ledger(4, 100));
    let mut reg = Registry::new();
    let f = untracked("f1", 2, false);
    reg.apply_fill(accepted(first.admit(&f, None, MonoNs(0))))
        .unwrap();
    // Another ledger has never seen the fill and would accept it again.
    let e = reg
        .apply_fill(accepted(second.admit(&f, None, MonoNs(0))))
        .unwrap_err();
    assert_eq!(e, OmsError::OtherLedger);
    assert!(e.to_string().contains("another fill ledger"));
    assert!(second.is_empty());
    assert_eq!(reg.inventory(INST), SignedLots(2));
}

// ---- replays: the watermark ----

#[test]
fn a_replayed_fill_absent_and_newer_than_the_watermark_is_applied_once() {
    let mut l = ledger(4, 100);
    let mut reg = Registry::new();
    let f = untracked("f1", 2, true);
    reg.apply_fill(accepted(l.admit(&f, engine(1_001), MonoNs(0))))
        .unwrap();
    assert!(matches!(
        l.admit(&f, engine(1_001), MonoNs(1)),
        Admission::Duplicate
    ));
    assert_eq!(reg.inventory(INST), SignedLots(2));
    assert_eq!(
        l.replays(),
        ReplayCounts {
            applied: 1,
            duplicate: 1,
            ..ReplayCounts::default()
        }
    );
}

#[test]
fn a_replayed_fill_at_or_before_the_watermark_only_reconciles() {
    let mut l = ledger(4, 100);
    for (fid, at) in [("f1", 999), ("f2", 1_000)] {
        assert!(matches!(
            l.admit(&untracked(fid, 1, true), engine(at), MonoNs(0)),
            Admission::BeforeWatermark
        ));
    }
    // The watermark compares the aligned wall-clock time, not the venue's own number.
    let skewed = Some(FillTime {
        exch: ExchNs(5_000),
        kind: ExchTsKind::MatchingEngine,
        aligned: WallNs(1_000),
    });
    assert!(matches!(
        l.admit(&untracked("f3", 1, true), skewed, MonoNs(0)),
        Admission::BeforeWatermark
    ));
    assert!(l.is_empty());
    assert_eq!(l.replays().before_watermark, 3);
}

#[test]
fn a_replayed_fill_without_a_matching_engine_time_only_reconciles() {
    let mut l = ledger(4, 100);
    let times = [
        None,
        timed(2_000, ExchTsKind::Publish),
        timed(2_000, ExchTsKind::Unknown),
    ];
    for (n, time) in times.into_iter().enumerate() {
        let f = untracked(&format!("f{n}"), 1, true);
        assert!(matches!(l.admit(&f, time, MonoNs(0)), Admission::Untimed));
    }
    assert!(l.is_empty());
    assert_eq!(l.replays().untimed, 3);
}

#[test]
fn a_live_fill_applies_whatever_its_time() {
    let mut l = ledger(8, 100);
    let times = [None, engine(10), timed(10, ExchTsKind::Unknown)];
    for (n, time) in times.into_iter().enumerate() {
        let f = untracked(&format!("f{n}"), 1, false);
        assert!(matches!(l.admit(&f, time, MonoNs(0)), Admission::Apply(_)));
    }
    assert_eq!(l.replays(), ReplayCounts::default());
}

// ---- replays: the retention horizon ----

#[test]
fn a_fill_forgotten_by_count_and_then_replayed_never_moves_inventory_again() {
    let mut l = ledger(1, 1_000);
    let mut reg = Registry::new();
    let a = untracked("a", 4, false);
    reg.apply_fill(accepted(l.admit(&a, engine(1_100), MonoNs(0))))
        .unwrap();
    let b = untracked("b", 1, false);
    reg.apply_fill(accepted(l.admit(&b, engine(1_050), MonoNs(1))))
        .unwrap();
    assert!(!l.contains(&a.key()));
    assert_eq!(l.horizon(), Horizon::At(ExchNs(1_100)));

    let replayed = untracked("a", 4, true);
    assert!(matches!(
        l.admit(&replayed, engine(1_100), MonoNs(2)),
        Admission::BeyondHorizon
    ));
    // A fill the ledger never saw, executed before the horizon, cannot be told apart.
    assert!(matches!(
        l.admit(&untracked("c", 1, true), engine(1_090), MonoNs(3)),
        Admission::BeyondHorizon
    ));
    assert_eq!(reg.inventory(INST), SignedLots(5));
    assert_eq!(l.replays().beyond_horizon, 2);

    // Newer than the horizon, absent and newer than the watermark: applied.
    reg.apply_fill(accepted(l.admit(
        &untracked("d", 2, true),
        engine(1_101),
        MonoNs(4),
    )))
    .unwrap();
    assert_eq!(reg.inventory(INST), SignedLots(7));
    // The horizon never moves back.
    assert_eq!(l.horizon(), Horizon::At(ExchNs(1_100)));
}

#[test]
fn a_fill_forgotten_by_age_and_then_replayed_never_moves_inventory_again() {
    let mut l = ledger(100, 50);
    let mut reg = Registry::new();
    let a = untracked("a", 3, false);
    reg.apply_fill(accepted(l.admit(
        &a,
        timed(1_200, ExchTsKind::Publish),
        MonoNs(0),
    )))
    .unwrap();
    // Before its age runs out, a replay is a duplicate.
    let replayed = untracked("a", 3, true);
    assert!(matches!(
        l.admit(&replayed, engine(1_190), MonoNs(49)),
        Admission::Duplicate
    ));
    // The next fill, past the age, makes the ledger forget it; a publish time bounds it.
    let b = untracked("b", 1, false);
    reg.apply_fill(accepted(l.admit(&b, engine(1_300), MonoNs(50))))
        .unwrap();
    assert!(!l.contains(&a.key()));
    assert_eq!(l.horizon(), Horizon::At(ExchNs(1_200)));
    assert!(matches!(
        l.admit(&replayed, engine(1_190), MonoNs(51)),
        Admission::BeyondHorizon
    ));
    assert_eq!(reg.inventory(INST), SignedLots(4));
}

#[test]
fn forgetting_a_fill_with_no_usable_time_loses_the_horizon_for_good() {
    let mut l = ledger(1, 1_000);
    let mut reg = Registry::new();
    for (fid, time) in [
        ("a", timed(1_100, ExchTsKind::Unknown)),
        ("b", engine(1_200)),
        ("c", engine(1_300)),
    ] {
        reg.apply_fill(accepted(l.admit(
            &untracked(fid, 1, false),
            time,
            MonoNs(0),
        )))
        .unwrap();
    }
    assert_eq!(l.horizon(), Horizon::Lost);
    assert!(matches!(
        l.admit(&untracked("z", 1, true), engine(9_999), MonoNs(1)),
        Admission::BeyondHorizon
    ));
}

// ---- what an accepted fill does to its order ----

fn session() -> (FillLedger, Registry) {
    (ledger(64, 1_000_000), Registry::new())
}

/// Admits and applies a live fill of `qty` for `cid`, named by `fid`.
fn apply(
    l: &mut FillLedger,
    reg: &mut Registry,
    cid: fbc_core::ClientOrderId,
    fid: &str,
    qty: i64,
) -> Result<FillRouted, OmsError> {
    let f = fill(Some(cid), ident(fid), Side::Buy, qty, false);
    reg.apply_fill(accepted(l.admit(&f, None, MonoNs(0))))
}

#[test]
fn a_fill_before_any_answer_promotes_a_pending_order() {
    let (mut l, mut reg) = session();
    let c = cid();
    reg.insert(placement(c, 100, 10)).unwrap();
    assert_eq!(
        apply(&mut l, &mut reg, c, "f1", 4),
        Ok(FillRouted::Ours(c, FillApplied::Live))
    );
    let rec = reg.get(c).unwrap();
    assert_eq!(rec.state(), OrdState::PartiallyFilled);
    assert_eq!(rec.cum_fills(), lots(4));
    assert_eq!(rec.filled(), lots(4));
    assert_eq!(rec.resting(), lots(6));
}

#[test]
fn a_fill_resolves_an_unknown_order() {
    let (mut l, mut reg) = session();
    let c = cid();
    reg.insert(placement(c, 100, 10)).unwrap();
    let item = ItemRef {
        idx: 0,
        cid: Some(c),
        vid: None,
    };
    reg.on_outcome(c, OrderOp::Place, &item, &SubmitOutcome::Unknown, MonoNs(5))
        .unwrap();
    assert_eq!(reg.get(c).unwrap().state(), OrdState::Unknown);
    apply(&mut l, &mut reg, c, "f1", 1).unwrap();
    let rec = reg.get(c).unwrap();
    assert_eq!(rec.state(), OrdState::PartiallyFilled);
    assert_eq!(rec.unknown_since(), None);
}

#[test]
fn an_unanswered_cancel_stays_on_the_ladder_through_a_fill() {
    let (mut l, mut reg) = session();
    let c = cid();
    reg.insert(placement(c, 100, 10)).unwrap();
    let item = ItemRef {
        idx: 0,
        cid: Some(c),
        vid: None,
    };
    let ack = SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    };
    reg.on_outcome(c, OrderOp::Place, &item, &ack, MonoNs(1))
        .unwrap();
    reg.on_outcome(
        c,
        OrderOp::Cancel,
        &item,
        &SubmitOutcome::Unknown,
        MonoNs(7),
    )
    .unwrap();
    apply(&mut l, &mut reg, c, "f1", 1).unwrap();
    assert_eq!(reg.get(c).unwrap().unknown_since(), Some(MonoNs(7)));
}

#[test]
fn the_order_is_filled_only_when_the_fills_alone_cover_it() {
    let (mut l, mut reg) = session();
    let c = cid();
    reg.insert(placement(c, 100, 10)).unwrap();
    // The venue says all ten are filled, but no fill event has arrived: still resting.
    reg.apply_update(&update(Some(c), VenueOrderState::Open, 10), key(1));
    let rec = reg.get(c).unwrap();
    assert_eq!(rec.state(), OrdState::PartiallyFilled);
    assert_eq!(rec.filled(), lots(10));
    assert_eq!(rec.cum_fills(), Lots::ZERO);

    assert_eq!(
        apply(&mut l, &mut reg, c, "f1", 6),
        Ok(FillRouted::Ours(c, FillApplied::Live))
    );
    // Filled is the larger counter, never their sum.
    assert_eq!(reg.get(c).unwrap().filled(), lots(10));
    assert_eq!(
        apply(&mut l, &mut reg, c, "f2", 4),
        Ok(FillRouted::Ours(c, FillApplied::Completed))
    );
    let rec = reg.get(c).unwrap();
    assert_eq!(rec.state(), OrdState::Terminal(TerminalKind::Filled));
    assert_eq!(rec.filled(), lots(10));
    assert_eq!(rec.resting(), Lots::ZERO);
    assert_eq!(reg.inventory(INST), SignedLots(10));
}

#[test]
fn a_fill_after_the_order_ended_is_counted_without_moving_its_state() {
    let (mut l, mut reg) = session();
    let c = cid();
    reg.insert(placement(c, 100, 10)).unwrap();
    reg.apply_update(&update(Some(c), common::canceled(), 3), key(1));
    assert_eq!(
        apply(&mut l, &mut reg, c, "f1", 3),
        Ok(FillRouted::Ours(c, FillApplied::AfterEnd))
    );
    let rec = reg.get(c).unwrap();
    assert!(rec.state().is_terminal());
    assert_eq!(rec.cum_fills(), lots(3));
    assert_eq!(rec.filled(), lots(3));
    assert_eq!(reg.inventory(INST), SignedLots(3));
}

#[test]
fn a_fill_naming_only_the_venue_id_reaches_its_order_and_teaches_the_id() {
    let (mut l, mut reg) = session();
    let c = cid();
    reg.insert(placement(c, 100, 10)).unwrap();
    // The first fill names our client id and the venue id: the record learns the id.
    let first = FillIdent::Venue {
        fill: common::fill_id("f1"),
        vid: Some(common::vid("v7")),
        cum_after: None,
    };
    let f = fill(Some(c), first, Side::Sell, 2, false);
    reg.apply_fill(accepted(l.admit(&f, None, MonoNs(0))))
        .unwrap();
    assert_eq!(reg.get(c).unwrap().vid(), Some(&common::vid("v7")));
    assert_eq!(reg.cid_of(&common::vid("v7")), Some(c));
    // A later fill names only the venue id.
    let second = FillIdent::Derived {
        vid: common::vid("v7"),
        cum_after: lots(5),
    };
    let f = fill(None, second, Side::Sell, 3, false);
    assert_eq!(
        reg.apply_fill(accepted(l.admit(&f, None, MonoNs(0)))),
        Ok(FillRouted::Ours(c, FillApplied::Live))
    );
    assert_eq!(reg.get(c).unwrap().cum_fills(), lots(5));
    assert_eq!(reg.inventory(INST), SignedLots(-5));
}

#[test]
fn foreign_non_canonical_and_unattributed_fills_are_flagged_not_counted_and_not_kept() {
    let (mut l, mut reg) = session();
    let mut foreign = untracked("f1", 5, false);
    foreign.cid = Some(CidMatch::Foreign(Namespace::new(9)));
    assert_eq!(
        reg.apply_fill(accepted(l.admit(&foreign, None, MonoNs(0)))),
        Ok(FillRouted::Foreign(Namespace::new(9)))
    );
    let mut other = untracked("f2", 5, false);
    other.cid = Some(CidMatch::Unparseable);
    assert_eq!(
        reg.apply_fill(accepted(l.admit(&other, None, MonoNs(0)))),
        Ok(FillRouted::NotCanonical)
    );
    // No client id, and a venue id that names no order the registry holds: another
    // namespace's order on a venue that echoes no client id would look the same.
    let anonymous = FillIdent::Venue {
        fill: common::fill_id("f3"),
        vid: Some(common::vid("v-unknown")),
        cum_after: None,
    };
    let unattributed = fill(None, anonymous, Side::Buy, 5, false);
    assert_eq!(
        reg.apply_fill(accepted(l.admit(&unattributed, None, MonoNs(0)))),
        Ok(FillRouted::Unattributed)
    );
    assert_eq!(reg.inventory(INST), SignedLots(0));
    // None of them is kept: delivered again, each is flagged again.
    assert!(l.is_empty());
    assert!(matches!(
        l.admit(&unattributed, None, MonoNs(1)),
        Admission::Apply(_)
    ));
    // Ours, but for an order the registry does not hold: our namespace's position moved.
    let orphan = untracked("f4", 2, false);
    assert_eq!(
        reg.apply_fill(accepted(l.admit(&orphan, None, MonoNs(0)))),
        Ok(FillRouted::OursUntracked(stray()))
    );
    assert_eq!(reg.inventory(INST), SignedLots(2));
}

#[test]
fn an_unattributed_fill_counts_once_its_order_is_known() {
    let (mut l, mut reg) = session();
    let c = cid();
    reg.insert(placement(c, 100, 10)).unwrap();
    let anonymous = FillIdent::Venue {
        fill: common::fill_id("f1"),
        vid: Some(common::vid("v5")),
        cum_after: None,
    };
    let early = fill(None, anonymous, Side::Buy, 3, false);
    assert_eq!(
        reg.apply_fill(accepted(l.admit(&early, None, MonoNs(0)))),
        Ok(FillRouted::Unattributed)
    );
    let item = ItemRef {
        idx: 0,
        cid: Some(c),
        vid: Some(common::vid("v5")),
    };
    let ack = SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    };
    reg.on_outcome(c, OrderOp::Place, &item, &ack, MonoNs(1))
        .unwrap();
    // Delivered again (a replay after a reconnect, say), it now reaches its order.
    let mut again = early.clone();
    again.replay = true;
    assert_eq!(
        reg.apply_fill(accepted(l.admit(&again, engine(1_100), MonoNs(2)))),
        Ok(FillRouted::Ours(c, FillApplied::Live))
    );
    assert_eq!(reg.get(c).unwrap().cum_fills(), lots(3));
    assert_eq!(reg.inventory(INST), SignedLots(3));
}

#[test]
fn flagged_fills_never_move_the_horizon() {
    let mut l = ledger(1, 1_000);
    let mut reg = Registry::new();
    // A foreign fill with no usable time, then one of ours: nothing ours was forgotten.
    let mut foreign = untracked("f1", 5, false);
    foreign.cid = Some(CidMatch::Foreign(Namespace::new(9)));
    reg.apply_fill(accepted(l.admit(&foreign, None, MonoNs(0))))
        .unwrap();
    reg.apply_fill(accepted(l.admit(
        &untracked("f2", 1, false),
        engine(1_100),
        MonoNs(1),
    )))
    .unwrap();
    assert_eq!(l.horizon(), Horizon::Full);
    // An absent replay of ours newer than the watermark still applies.
    assert_eq!(
        reg.apply_fill(accepted(l.admit(
            &untracked("f3", 2, true),
            engine(1_050),
            MonoNs(2)
        ),)),
        Ok(FillRouted::OursUntracked(stray()))
    );
    assert_eq!(reg.inventory(INST), SignedLots(3));
}

#[test]
fn a_fills_cumulative_quantity_raises_the_venues_count() {
    let (mut l, mut reg) = session();
    let c = cid();
    reg.insert(placement(c, 100, 12)).unwrap();
    reg.apply_update(&update(Some(c), VenueOrderState::Open, 6), key(1));
    let named = FillIdent::Venue {
        fill: common::fill_id("f1"),
        vid: None,
        cum_after: Some(lots(10)),
    };
    let f = fill(Some(c), named, Side::Buy, 4, false);
    reg.apply_fill(accepted(l.admit(&f, None, MonoNs(0))))
        .unwrap();
    let rec = reg.get(c).unwrap();
    assert_eq!(rec.cum_venue(), lots(10));
    assert_eq!(rec.cum_fills(), lots(4));
    assert_eq!(rec.filled(), lots(10));
    assert_eq!(rec.resting(), lots(2));
    // The inventory moves by the fill's own quantity only.
    assert_eq!(reg.inventory(INST), SignedLots(4));
}

#[test]
fn a_fill_that_would_overflow_counts_nothing() {
    let (mut l, mut reg) = session();
    let c = cid();
    reg.insert(placement(c, 100, i64::MAX)).unwrap();
    apply(&mut l, &mut reg, c, "f1", i64::MAX).unwrap();
    // The inventory would overflow too, but the order's sum is checked first.
    let e = apply(&mut l, &mut reg, c, "f2", 1).unwrap_err();
    assert_eq!(e, OmsError::FillOverflow(INST));
    assert!(e.to_string().contains("overflows"));
    assert_eq!(reg.get(c).unwrap().cum_fills(), lots(i64::MAX));
    assert_eq!(reg.inventory(INST), SignedLots(i64::MAX));

    // The refused fill is not in the ledger, so a redelivery is tried again.
    let refused = fill(Some(c), ident("f2"), Side::Buy, 1, false);
    assert!(!l.contains(&refused.key()));

    // Fills of no order the registry holds overflow only the inventory.
    let (mut l, mut reg) = session();
    let big = untracked("u0", i64::MAX, false);
    reg.apply_fill(accepted(l.admit(&big, None, MonoNs(0))))
        .unwrap();
    let one = untracked("u1", 1, false);
    assert_eq!(
        reg.apply_fill(accepted(l.admit(&one, None, MonoNs(0)))),
        Err(OmsError::FillOverflow(INST))
    );
    assert_eq!(reg.inventory(INST), SignedLots(i64::MAX));
    // Once a sell makes room, the redelivered fill applies, once.
    let sell = fill(Some(stray()), ident("u2"), Side::Sell, 5, false);
    reg.apply_fill(accepted(l.admit(&sell, None, MonoNs(0))))
        .unwrap();
    assert_eq!(
        reg.apply_fill(accepted(l.admit(&one, None, MonoNs(0)))),
        Ok(FillRouted::OursUntracked(stray()))
    );
    assert!(matches!(
        l.admit(&one, None, MonoNs(0)),
        Admission::Duplicate
    ));
    assert_eq!(reg.inventory(INST), SignedLots(i64::MAX - 4));
}

#[test]
fn inventory_is_kept_per_instrument() {
    let (mut l, mut reg) = session();
    let mut other = untracked("f1", 2, false);
    other.inst = InstrumentId::new(2);
    reg.apply_fill(accepted(l.admit(&other, None, MonoNs(0))))
        .unwrap();
    assert_eq!(reg.inventory(InstrumentId::new(2)), SignedLots(2));
    assert_eq!(reg.inventory(INST), SignedLots(0));
}
