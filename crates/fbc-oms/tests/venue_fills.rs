//! Venue-initiated fills (decision 0120, FBC-5uk): a change the venue made to the account's
//! position with no order of the account's (Paradex's LIQUIDATION, UNWIND_TRANSFER,
//! SETTLE_MARKET and BLOCK_TRADE) names no client id and reaches the inventory once, through
//! the fill ledger and [`Registry::apply_fill`] (0005's I3), on no order: it steps no order's
//! lattice or fill count, indexes no venue id, puts nothing in view and is no orphan sighting,
//! while an order's fill naming nothing of ours is still flagged (I4 unchanged). It counts in
//! every market state and changes none (0012, 0013), and an Exit authorization built before it
//! is refused at submit (0066).
//!
//! Against a resync's seed (0055 and 0095, as 0120 augments them) only its arrival at or before
//! the request, or its time at or before the watermark (a matching-engine time, or a publish
//! time as an upper bound on execution), places it in the snapshot, on either read mode. Before
//! the seed, one neither places leaves the market unseeded; after a snapshot seed it counts on
//! the inventory (the owner's decision of 2026-10-20), and the double count 0120 accepts, for a
//! fill in flight while the seed was taken (held by the seed's position read, timed after its
//! watermark or not timed, and delivered after the seed), shows as a desync at the next resync.
//!
//! The fills are built by hand, since fbc-oms depends on fbc-core only. The done line's
//! liquidation and transfer have the shapes the Paradex decoder pushes for the hand-built frames
//! `fixtures/paradex/exec/fill-liquidation-v2.sbe.txt` and
//! `fixtures/paradex/exec/fill-transfer-null-seq-v2.sbe.txt`, which
//! `crates/venues/fbc-venue-paradex/tests/exec_fill_events.rs` pins; the transfer here carries
//! the off-grid price of `fill-settle-off-grid-v2.sbe.txt` instead, which fbc-oms never reads.
//!
//! The values are synthetic: an inventory cap of 50 lots on a synthetic market, and made-up
//! positions, none from any account.

#[path = "common/arm.rs"]
mod arm;
mod common;

use std::time::Duration;

use common::{
    canceled, cid, fee_paid, fill, fill_id, ident, lots, no_fee, order_caps, placement, update,
    venue_fill, vid,
};
use fbc_core::{
    AckLevel, AssetSym, CidMatch, ClientOrderId, ExchNs, ExchTsKind, FillEvent, FillIdent,
    FillOrigin, InstrumentId, ItemRef, Liquidity3, Money, MonoNs, Namespace, NewOrder, OrderCaps,
    OrderKind, PxExact, Side, SignedLots, SnapshotReads, SnapshotSource, SubmitOutcome, Support,
    Ticks, VenueFillKind, VenueOrderSnapshot, VenueOrderState, WallNs,
};
use fbc_oms::{
    Admission, CancelAllRefusal, CancelEverything, EntryState, ExitKind, ExitRefusal, FillLedger,
    FillRouted, FillTime, LadderConfig, LedgerConfig, MarketCapsConfig, OmsError, OrdState,
    OrderKey, OrderOp, PositionCheck, PreTradeCaps, Registry, ReplayCounts, ResyncReport,
    ResyncSnapshot, Routed, StaleAuthorization, StateRefusal,
};

const INST: InstrumentId = InstrumentId::new(1);
const OTHER: InstrumentId = InstrumentId::new(2);
/// An inventory cap of 50 lots: no fill is held back by it, only orders.
const CAP: i64 = 50;
/// A resting cap past any order of these tests.
const WIDE: i64 = 1_000_000;
/// The seeding resync's request: on the wall clock (its watermark) and the monotonic clock.
const W: i64 = 1_000;
const REQ: u64 = 1_000;
/// Paradex's price exponent (Price8).
const EXP: i8 = -8;
/// 61000.0 at Paradex's exponent.
const PX: PxExact = PxExact::new(6_100_000_000_000, EXP);
/// 61234.56789012, off a 0.1 tick, as a settlement's spot TWAP can be.
const OFF_GRID: PxExact = PxExact::new(6_123_456_789_012, EXP);
/// Another engine's namespace on the account.
const THEIRS: Namespace = Namespace::new(8);

/// A named registry under the inventory cap on `INST` and `OTHER`, not seeded, every market
/// disarmed in Cancel-only.
fn registry() -> Registry {
    let caps = [INST, OTHER]
        .into_iter()
        .try_fold(PreTradeCaps::new(), |caps, m| {
            caps.with_market(
                m,
                MarketCapsConfig {
                    inventory: Some(lots(CAP)),
                    resting: Some(lots(WIDE)),
                },
            )
        })
        .unwrap();
    arm::named(Registry::with_caps(caps))
}

/// A trustworthy snapshot source, its orders and positions read at one instant or apart
/// (`reads`; Paradex's are two REST reads, `SnapshotReads::Separate`, decision 0095); an
/// instrument cancel-all, so the kill switch's cancel everything shows what is in view.
fn venue(reads: SnapshotReads) -> OrderCaps {
    OrderCaps {
        snapshot_source: SnapshotSource::Trustworthy,
        snapshot_reads: reads,
        cancel_all_instrument: Support::Native,
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

fn key(ingest: u64) -> OrderKey {
    OrderKey {
        venue: None,
        ingest,
    }
}

/// A resync requested at `w` on the wall clock and `req` on the monotonic clock, showing no
/// order and the positions `pos`.
fn resync(
    reg: &mut Registry,
    reads: SnapshotReads,
    (w, req): (i64, u64),
    pos: &[(InstrumentId, i64)],
) -> ResyncReport {
    let snap = ResyncSnapshot {
        watermark: WallNs(w),
        requested_at: MonoNs(req),
        orders: vec![],
        positions: pos.iter().map(|&(m, p)| (m, SignedLots(p))).collect(),
    };
    reg.resync(&ladder_cfg(), &venue(reads), &snap, key(req))
        .unwrap()
}

/// [`resync`] of `INST`'s position `pos` alone.
fn resync_at(reg: &mut Registry, reads: SnapshotReads, at: (i64, u64), pos: i64) -> ResyncReport {
    resync(reg, reads, at, &[(INST, pos)])
}

/// A registry whose first trustworthy resync, requested at `W` and `REQ`, seeded `INST` at
/// `pos` and `OTHER` flat.
fn seeded(reads: SnapshotReads, pos: i64) -> Registry {
    let mut reg = registry();
    let report = resync_at(&mut reg, reads, (W, REQ), pos);
    assert_eq!(
        report.seeded,
        vec![(INST, SignedLots(pos)), (OTHER, SignedLots(0))]
    );
    reg
}

fn ledger() -> FillLedger {
    ledger_from(WallNs(0))
}

/// A ledger for a session that started at `watermark`.
fn ledger_from(watermark: WallNs) -> FillLedger {
    FillLedger::new(
        LedgerConfig {
            max_age: Duration::from_secs(3600),
            max_entries: 1_000,
        },
        watermark,
    )
    .unwrap()
}

/// The venue's time `at` of the kind `kind`, aligned with no skew.
fn timed(kind: ExchTsKind, at: i64) -> Option<FillTime> {
    Some(FillTime {
        exch: ExchNs(at),
        kind,
        aligned: WallNs(at),
    })
}

/// Published at `at`: an upper bound on when it executed.
fn published(at: i64) -> Option<FillTime> {
    timed(ExchTsKind::Publish, at)
}

/// Executed at `at` on the matching engine.
fn executed(at: i64) -> Option<FillTime> {
    timed(ExchTsKind::MatchingEngine, at)
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

/// Hands `f` to the ledger at `now` and applies it, returning what the registry said.
fn try_apply(
    reg: &mut Registry,
    l: &mut FillLedger,
    f: &FillEvent,
    time: Option<FillTime>,
    now: u64,
) -> Result<FillRouted, OmsError> {
    match l.admit(f, time, MonoNs(now)) {
        Admission::Apply(a) => reg.apply_fill(a),
        other => panic!("expected the fill accepted, got {other:?}"),
    }
}

/// A liquidation of `qty` on `side`, named `fid`, of the venue's order `vid` when given, at
/// 61000.0 with no fee.
fn liquidation(fid: &str, vid: Option<&str>, side: Side, qty: i64) -> FillEvent {
    venue_fill(
        VenueFillKind::Liquidation,
        fid,
        vid,
        side,
        qty,
        PX,
        no_fee(),
    )
}

/// What a venue fill of `kind` the seed does not hold routes to.
fn counted(kind: VenueFillKind) -> FillRouted {
    FillRouted::Venue {
        kind,
        in_snapshot: false,
    }
}

/// What a venue fill of `kind` the seed holds routes to.
fn in_snapshot(kind: VenueFillKind) -> FillRouted {
    FillRouted::Venue {
        kind,
        in_snapshot: true,
    }
}

fn usd(nanos: i128) -> Money {
    Money::new(nanos, AssetSym::new("USD").unwrap())
}

/// `INST` is as a fresh registry has it, disarmed in Cancel-only at generation 0.
fn untouched(reg: &Registry) {
    let e = reg.entry(INST);
    assert_eq!(
        (e.armed(), e.state(), e.generation().get()),
        (false, EntryState::CancelOnly, 0),
        "a resync or a fill moved the market"
    );
}

/// Places `order` (admitted) and acknowledges it under the venue id `v`: Open.
fn open(reg: &mut Registry, order: NewOrder, v: &str) -> ClientOrderId {
    let c = order.cid;
    arm::place_issued(reg, order);
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
    c
}

/// The done line: a market seeded by a trustworthy resync, then a liquidation and a position
/// transfer, each built as the Paradex decoder pushes it, reach the inventory once each, and
/// the next resync's position is the inventory: it agrees, nothing is unsettled, and neither
/// fill touched an order, the view of orders not ours or the market's state.
#[test]
fn a_paradex_liquidation_and_a_position_transfer_reach_the_inventory_and_the_next_resync_agrees() {
    let mut reg = registry();
    let mut l = ledger();
    // Paradex's caps: a trustworthy source, its orders and positions two reads.
    let seed = resync_at(&mut reg, SnapshotReads::Separate, (W, REQ), 200);
    assert_eq!(
        seed.seeded,
        vec![(INST, SignedLots(200)), (OTHER, SignedLots(0))]
    );
    assert!(seed.unsettled.is_empty() && seed.checks.is_empty());

    // fill-liquidation-v2: the venue's order 1759500000000000009 sells 0.15 (150 lots) of the
    // account's long at 61000.0, a taker, the account paying 0.04575 USDC and realizing -150 of
    // P&L and -0.3 of funding; published after the seed's watermark.
    let liquidation = FillEvent {
        liquidity: Liquidity3::Taker,
        realized_pnl: Some(usd(-150_000_000_000)),
        realized_funding: Some(usd(-300_000_000)),
        ..venue_fill(
            VenueFillKind::Liquidation,
            "LIQUIDATION:8615262148007718005",
            Some("1759500000000000009"),
            Side::Sell,
            150,
            PX,
            fee_paid(45_750_000, "USDC"),
        )
    };
    // fill-transfer-null-seq-v2: no order, the account buying 0.05 (50 lots), the liquidity
    // not said, no fee, realizing -0.2 of P&L.
    let transfer = FillEvent {
        realized_pnl: Some(usd(-200_000_000)),
        realized_funding: Some(usd(0)),
        ..venue_fill(
            VenueFillKind::Transfer,
            "UNWIND_TRANSFER:8615262148007718006",
            None,
            Side::Buy,
            50,
            OFF_GRID,
            fee_paid(0, "USDC"),
        )
    };
    for f in [&liquidation, &transfer] {
        assert_eq!((f.cid(), f.order_px()), (None, None));
    }

    assert_eq!(
        apply(&mut reg, &mut l, &liquidation, published(2_000), 2_000),
        counted(VenueFillKind::Liquidation)
    );
    assert_eq!(reg.position(INST), Some(SignedLots(50)));
    assert_eq!(
        apply(&mut reg, &mut l, &transfer, published(2_100), 2_100),
        counted(VenueFillKind::Transfer)
    );
    assert_eq!(reg.position(INST), Some(SignedLots(100)));
    // Each delivered again is a duplicate: counted once (I3).
    for f in [&liquidation, &transfer] {
        assert!(matches!(
            l.admit(f, published(2_200), MonoNs(2_200)),
            Admission::Duplicate
        ));
    }
    assert_eq!(reg.position(INST), Some(SignedLots(100)));
    // Neither named an order of ours or put an order in view.
    assert_eq!(reg.cid_of(&vid("1759500000000000009")), None);
    assert!(reg.is_empty());
    assert!(!reg.foreign_in_view(INST));

    // The next resync: the venue's position is the inventory.
    let next = resync_at(&mut reg, SnapshotReads::Separate, (3_000, 3_000), 100);
    assert_eq!(
        next.checks,
        vec![
            PositionCheck::Agrees {
                inst: INST,
                position: SignedLots(100),
            },
            PositionCheck::Agrees {
                inst: OTHER,
                position: SignedLots(0),
            },
        ]
    );
    assert!(next.unsettled.is_empty() && next.seeded.is_empty());
    assert!(next.untracked.is_empty() && next.orphans.is_empty());
    assert_eq!(reg.position(INST), Some(SignedLots(100)));
    assert!(!reg.foreign_in_view(INST));
    untouched(&reg);
}

/// Each kind of venue fill is reported by its kind and moves the inventory of its own market
/// by its signed size, whatever its price.
#[test]
fn each_kind_of_venue_fill_is_reported_by_its_kind_and_counted() {
    let mut reg = seeded(SnapshotReads::Separate, 20);
    let mut l = ledger();
    let mut pos = 20;
    let kinds = [
        VenueFillKind::Liquidation,
        VenueFillKind::Transfer,
        VenueFillKind::Settlement,
        VenueFillKind::BlockTrade,
    ];
    for (n, kind) in (0i64..).zip(kinds) {
        let side = if n % 2 == 0 { Side::Sell } else { Side::Buy };
        let f = venue_fill(
            kind,
            &format!("{kind:?}:{n}"),
            None,
            side,
            3,
            OFF_GRID,
            fee_paid(1_000, "USDC"),
        );
        assert_eq!((f.venue_kind(), f.cid()), (Some(kind), None));
        let now = 1_500 + n.unsigned_abs();
        assert_eq!(
            apply(&mut reg, &mut l, &f, published(1_100 + n), now),
            counted(kind)
        );
        pos += SignedLots::of(side, lots(3)).0;
        assert_eq!(reg.position(INST), Some(SignedLots(pos)), "{kind:?}");
    }
    // One on another market moves that market only.
    let elsewhere = FillEvent {
        inst: OTHER,
        ..liquidation("LIQUIDATION:other", None, Side::Sell, 2)
    };
    assert_eq!(
        apply(&mut reg, &mut l, &elsewhere, published(1_200), 1_600),
        counted(VenueFillKind::Liquidation)
    );
    assert_eq!(reg.position(OTHER), Some(SignedLots(-2)));
    assert_eq!(reg.position(INST), Some(SignedLots(pos)));
    assert_eq!(l.len(), 5);
}

/// No fill changes a market's state or arming (0012, 0013): a venue fill counts once in
/// Cancel-only, Quoting, Exit and Killed alike, and leaves the market's armed flag, state and
/// generation as they were.
#[test]
fn a_venue_fill_counts_once_in_every_state_and_changes_none() {
    for state in [
        EntryState::CancelOnly,
        EntryState::Quoting,
        EntryState::Exit(ExitKind::Flatten),
        EntryState::Killed,
    ] {
        let mut reg = seeded(SnapshotReads::OneInstant, 20);
        match state {
            EntryState::CancelOnly => {}
            EntryState::Quoting => {
                arm::start(&mut reg, INST);
            }
            EntryState::Exit(_) => {
                let leases = arm::leases(&reg, INST);
                reg.flatten(INST, leases).unwrap();
            }
            EntryState::Killed => {
                arm::start(&mut reg, INST);
                reg.kill(INST);
            }
        }
        let before = reg.entry(INST);
        assert_eq!(before.state(), state);
        let mut l = ledger();
        let f = liquidation("LIQUIDATION:1", None, Side::Sell, 5);
        assert_eq!(
            apply(&mut reg, &mut l, &f, published(1_500), 1_500),
            counted(VenueFillKind::Liquidation),
            "{state:?}"
        );
        assert_eq!(reg.position(INST), Some(SignedLots(15)), "{state:?}");
        assert_eq!(
            reg.entry(INST),
            before,
            "{state:?}: a fill moved the market"
        );
        // Delivered again, it is a duplicate.
        assert!(matches!(
            l.admit(&f, published(1_600), MonoNs(1_600)),
            Admission::Duplicate
        ));
        assert_eq!(reg.position(INST), Some(SignedLots(15)), "{state:?}");
    }
}

/// I3: a venue fill delivered again, live or replayed, is a duplicate and counts once; an
/// order's fill under another fill type's key (Paradex keys each fill `<TYPE>:<fillId>`) is
/// another fill.
#[test]
fn a_venue_fill_delivered_again_counts_once_and_apart_from_an_order_fill_of_the_same_id() {
    let mut reg = seeded(SnapshotReads::Separate, 20);
    arm::start(&mut reg, INST);
    let a = open(&mut reg, placement(cid(), 100, 10), "v-a");
    let mut l = ledger();
    let f = liquidation("LIQUIDATION:861", None, Side::Sell, 5);
    assert_eq!(
        apply(&mut reg, &mut l, &f, published(1_500), 1_500),
        counted(VenueFillKind::Liquidation)
    );
    assert!(matches!(
        l.admit(&f, published(1_600), MonoNs(1_600)),
        Admission::Duplicate
    ));
    let replayed = FillEvent {
        replay: true,
        ..f.clone()
    };
    assert!(matches!(
        l.admit(&replayed, executed(1_700), MonoNs(1_700)),
        Admission::Duplicate
    ));
    assert_eq!(l.replays().duplicate, 1);
    assert_eq!(reg.position(INST), Some(SignedLots(15)));
    // FILL:861, an execution of our order placed after the seed.
    let own = fill(
        Some(a),
        FillIdent::Venue {
            fill: fill_id("FILL:861"),
            vid: Some(vid("v-a")),
            cum_after: None,
        },
        Side::Buy,
        2,
        false,
    );
    assert!(matches!(
        apply(&mut reg, &mut l, &own, published(1_800), 1_800),
        FillRouted::Ours(c, _) if c == a
    ));
    assert_eq!(reg.get(a).unwrap().cum_fills(), lots(2));
    assert_eq!(reg.position(INST), Some(SignedLots(17)));
    assert_eq!(l.len(), 2);
}

/// A venue fill whose venue order id is one of our orders' counts on no order: the order's
/// fill count, its venue-reported fill and its state stay as they were, terminal or not, the
/// venue id still names the order for its own events, and the inventory moves once.
#[test]
fn a_venue_fill_naming_one_of_our_orders_venue_ids_counts_on_no_order() {
    let mut reg = seeded(SnapshotReads::OneInstant, 20);
    arm::start(&mut reg, INST);
    let a = open(&mut reg, placement(cid(), 100, 10), "v-a");
    let mut l = ledger();
    // On the order's own side, so a fill routed to it would count on it.
    let f = liquidation("LIQUIDATION:a", Some("v-a"), Side::Buy, 3);
    assert_eq!(
        apply(&mut reg, &mut l, &f, published(1_500), 1_500),
        counted(VenueFillKind::Liquidation)
    );
    let rec = reg.get(a).unwrap();
    assert_eq!(
        (rec.state(), rec.cum_fills(), rec.cum_venue()),
        (OrdState::Open, lots(0), lots(0))
    );
    assert_eq!(reg.position(INST), Some(SignedLots(23)));
    assert_eq!(reg.cid_of(&vid("v-a")), Some(a));
    assert!(!reg.foreign_in_view(INST));
    // The venue id still names our order: its own events, naming it alone, reach it, and only
    // they move what the venue says the order filled.
    let mut partly = update(None, VenueOrderState::Open, 4);
    partly.vid = Some(vid("v-a"));
    assert!(matches!(
        reg.apply_update(&partly, key(2)),
        Routed::Ours(c, _) if c == a
    ));
    let mut ended = update(None, canceled(), 4);
    ended.vid = Some(vid("v-a"));
    assert!(matches!(
        reg.apply_update(&ended, key(3)),
        Routed::Ours(c, _) if c == a
    ));
    let rec = reg.get(a).unwrap();
    assert!(rec.state().is_terminal());
    assert_eq!((rec.cum_fills(), rec.cum_venue()), (lots(0), lots(4)));
    // A terminal order named the same way stays as it ended.
    let again = liquidation("LIQUIDATION:a2", Some("v-a"), Side::Buy, 2);
    assert_eq!(
        apply(&mut reg, &mut l, &again, published(1_600), 1_600),
        counted(VenueFillKind::Liquidation)
    );
    let rec = reg.get(a).unwrap();
    assert!(rec.state().is_terminal());
    assert_eq!((rec.cum_fills(), rec.cum_venue()), (lots(0), lots(4)));
    assert_eq!(reg.position(INST), Some(SignedLots(25)));
    assert!(!reg.foreign_in_view(INST));
}

/// I4 unchanged: a venue fill puts nothing in view, so a Killed market under its exclusive
/// lease, the account's open orders viewed, still cancels everything with one instrument
/// cancel-all; an order's fill naming another namespace's client id, a non-canonical one, or
/// none and no venue id of ours is flagged, moves nothing, is not kept and puts its order in
/// view, as before.
#[test]
fn a_venue_fill_puts_no_order_in_view_and_an_order_fill_naming_nothing_ours_still_does() {
    let cases: [(&str, Option<CidMatch>, Option<&str>, FillRouted); 4] = [
        (
            "another namespace's",
            Some(CidMatch::Foreign(THEIRS)),
            Some("v-theirs"),
            FillRouted::Foreign(THEIRS),
        ),
        (
            "non-canonical",
            Some(CidMatch::Unparseable),
            Some("v-theirs"),
            FillRouted::NotCanonical,
        ),
        (
            "no client id, another venue id",
            None,
            Some("v-theirs"),
            FillRouted::Unattributed,
        ),
        (
            "no client id, no venue id",
            None,
            None,
            FillRouted::Unattributed,
        ),
    ];
    for (case, who, named, routed) in cases {
        let mut reg = seeded(SnapshotReads::OneInstant, 20);
        arm::start(&mut reg, INST);
        reg.kill(INST);
        let mut l = ledger();
        // A transfer naming no order, and a liquidation of the venue's own order.
        let transfer = venue_fill(
            VenueFillKind::Transfer,
            "UNWIND_TRANSFER:1",
            None,
            Side::Buy,
            2,
            OFF_GRID,
            no_fee(),
        );
        let liq = liquidation("LIQUIDATION:1", Some("v-venue"), Side::Sell, 4);
        assert_eq!(
            apply(&mut reg, &mut l, &transfer, published(1_500), 1_500),
            counted(VenueFillKind::Transfer)
        );
        assert_eq!(
            apply(&mut reg, &mut l, &liq, published(1_600), 1_600),
            counted(VenueFillKind::Liquidation)
        );
        assert_eq!(reg.position(INST), Some(SignedLots(18)), "{case}");
        assert!(!reg.foreign_in_view(INST), "{case}");
        assert_eq!(reg.cid_of(&vid("v-venue")), None);
        let out = reg.cancel_everything(INST, &venue(SnapshotReads::OneInstant));
        assert!(
            matches!(out, CancelEverything::CancelAll { .. }),
            "{case}: {out:?}"
        );

        // An order's fill naming nothing of ours.
        let theirs = FillEvent {
            origin: FillOrigin::Order {
                cid: who,
                px: Ticks(100),
            },
            ..fill(
                None,
                FillIdent::Venue {
                    fill: fill_id("FILL:theirs"),
                    vid: named.map(vid),
                    cum_after: None,
                },
                Side::Buy,
                3,
                false,
            )
        };
        assert_eq!(
            apply(&mut reg, &mut l, &theirs, published(1_700), 1_700),
            routed,
            "{case}"
        );
        assert_eq!(reg.position(INST), Some(SignedLots(18)), "{case}");
        assert!(!l.contains(&theirs.key()), "{case}: kept");
        assert!(
            matches!(
                l.admit(&theirs, published(1_800), MonoNs(1_800)),
                Admission::Apply(_)
            ),
            "{case}: delivered again, it is routed again"
        );
        assert!(reg.foreign_in_view(INST), "{case}");
        let out = reg.cancel_everything(INST, &venue(SnapshotReads::OneInstant));
        assert!(
            matches!(
                out,
                CancelEverything::Explicit {
                    why: CancelAllRefusal::ForeignInView,
                    ..
                }
            ),
            "{case}: {out:?}"
        );
    }
}

/// Before the seed (0055's pre-seed rule, as 0120 places a venue fill): the snapshot holds a
/// venue fill only by its arrival at or before the request or its time at or before the
/// watermark, on either read mode; never by an order shown or not shown (rules 2 and 3), so on
/// a one-instant read too a fill neither places leaves the market unseeded, until a resync
/// requested after it arrived.
#[test]
fn before_the_seed_a_venue_fill_is_placed_only_by_its_arrival_or_time_on_either_read_mode() {
    for reads in [SnapshotReads::OneInstant, SnapshotReads::Separate] {
        let held = [
            ("arrived before the request, untimed", None, 900),
            ("arrived at the request, untimed", None, REQ),
            ("published before the watermark", published(900), 1_100),
            ("published at the watermark", published(W), 1_100),
            ("executed at the watermark", executed(W), 1_100),
        ];
        for (case, time, now) in held {
            let mut reg = registry();
            let mut l = ledger();
            let f = liquidation(case, None, Side::Sell, 5);
            // Not seeded: nothing places it yet, and the inventory is the fills' sum.
            assert_eq!(
                apply(&mut reg, &mut l, &f, time, now),
                counted(VenueFillKind::Liquidation)
            );
            assert_eq!(reg.inventory(INST), SignedLots(-5), "{reads:?} {case}");
            assert_eq!(reg.position(INST), None, "{reads:?} {case}");
            let seed = resync_at(&mut reg, reads, (W, REQ), 20);
            assert_eq!(
                seed.seeded,
                vec![(INST, SignedLots(20)), (OTHER, SignedLots(0))],
                "{reads:?} {case}"
            );
            assert!(seed.unsettled.is_empty(), "{reads:?} {case}");
            // Counted once: the next resync agrees with the snapshot's position.
            let next = resync_at(&mut reg, reads, (2_000, 2_000), 20);
            assert_eq!(
                next.checks[0],
                PositionCheck::Agrees {
                    inst: INST,
                    position: SignedLots(20)
                },
                "{reads:?} {case}"
            );
        }
        let unplaced = [
            ("published after the watermark", published(1_100), 1_100),
            ("executed after the watermark", executed(1_100), 1_100),
            (
                "timed before the watermark, of no stated kind",
                timed(ExchTsKind::Unknown, 900),
                1_100,
            ),
            ("arrived after the request, untimed", None, 1_100),
        ];
        for (case, time, now) in unplaced {
            let mut reg = registry();
            let mut l = ledger();
            let f = liquidation(case, None, Side::Sell, 5);
            apply(&mut reg, &mut l, &f, time, now);
            let seed = resync_at(&mut reg, reads, (W, REQ), 20);
            // `OTHER` holds no fill: it is seeded; `INST` is not.
            assert_eq!(
                seed.seeded,
                vec![(OTHER, SignedLots(0))],
                "{reads:?} {case}"
            );
            assert_eq!(seed.unsettled, vec![(INST, f.key())], "{reads:?} {case}");
            assert_eq!(reg.position(INST), None, "{reads:?} {case}");
            // A resync requested after it arrived holds it.
            let later = resync_at(&mut reg, reads, (1_200, 1_200), 15);
            assert_eq!(
                later.seeded,
                vec![(INST, SignedLots(15))],
                "{reads:?} {case}"
            );
            assert!(later.unsettled.is_empty(), "{reads:?} {case}");
            assert_eq!(reg.position(INST), Some(SignedLots(15)), "{reads:?} {case}");
        }
    }
}

/// An order's fills and a venue fill naming the order's venue id, all held before a seed whose
/// snapshot shows the order: the seed sets the order's fill count to the cumulative fill the
/// snapshot showed plus the order's own fills it places after the snapshot (on one instant's
/// read, a fill past the cumulative fill shown), never adding the venue fill, which the seed
/// holds by its arrival and which counts on no order.
#[test]
fn before_the_seed_a_venue_fill_naming_our_order_adds_nothing_to_its_fill_count() {
    for reads in [SnapshotReads::OneInstant, SnapshotReads::Separate] {
        let mut reg = registry();
        let mut l = ledger();
        let a = cid();
        reg.insert(placement(a, 100, 10)).unwrap();
        let own = |fid: &str, cum: i64, qty: i64| {
            fill(
                Some(a),
                FillIdent::Venue {
                    fill: fill_id(fid),
                    vid: Some(vid("v-a")),
                    cum_after: Some(lots(cum)),
                },
                Side::Buy,
                qty,
                false,
            )
        };
        // The order's first fill, of 2, executed and arrived before the request: the snapshot
        // holds it (0055's first rule).
        assert!(matches!(
            apply(&mut reg, &mut l, &own("FILL:a1", 2, 2), executed(900), 900),
            FillRouted::Ours(c, _) if c == a
        ));
        // A liquidation naming the order's venue id, arrived before the request.
        let liq = liquidation("LIQUIDATION:a", Some("v-a"), Side::Buy, 3);
        assert_eq!(
            apply(&mut reg, &mut l, &liq, None, 950),
            counted(VenueFillKind::Liquidation)
        );
        assert_eq!(reg.get(a).unwrap().cum_fills(), lots(2), "{reads:?}");
        // On one instant's read, the order's second fill, of 2, executed after the watermark and
        // arrived after the request, past the cumulative fill of 2 the snapshot shows: the seed
        // places it after the snapshot. On separate reads nothing would place it, so it is not
        // sent there.
        let second = reads == SnapshotReads::OneInstant;
        if second {
            assert!(matches!(
                apply(&mut reg, &mut l, &own("FILL:a2", 4, 2), executed(1_100), 1_100),
                FillRouted::Ours(c, _) if c == a
            ));
        }
        // The snapshot shows the order filled 2, and the position long 5: the first fill and
        // the liquidation.
        let snap = ResyncSnapshot {
            watermark: WallNs(W),
            requested_at: MonoNs(REQ),
            orders: vec![VenueOrderSnapshot {
                cid: Some(CidMatch::Ours(a)),
                vid: vid("v-a"),
                inst: INST,
                side: Side::Buy,
                state: VenueOrderState::Open,
                px: Some(Ticks(100)),
                qty: lots(10),
                cum_filled: lots(2),
                post_only: None,
                reduce_only: None,
            }],
            positions: [(INST, SignedLots(5))].into_iter().collect(),
        };
        let seed = reg
            .resync(&ladder_cfg(), &venue(reads), &snap, key(REQ))
            .unwrap();
        let (pos, cum) = if second { (7, 4) } else { (5, 2) };
        assert_eq!(seed.seeded[0], (INST, SignedLots(pos)), "{reads:?}");
        assert!(seed.unsettled.is_empty(), "{reads:?}");
        assert_eq!(reg.get(a).unwrap().cum_fills(), lots(cum), "{reads:?}");
        assert_eq!(reg.position(INST), Some(SignedLots(pos)), "{reads:?}");
    }
}

/// After a snapshot seed (the owner's decision of 2026-10-20, decision 0120): the seed holds a
/// venue fill by its arrival at or before the request, or by a matching-engine or publish time
/// at or before the watermark, and it moves nothing more; otherwise it counts on the inventory,
/// never leaving the position unknown. The next resync agrees either way.
#[test]
fn after_the_seed_a_venue_fill_is_held_by_its_arrival_or_time_and_otherwise_counts() {
    for reads in [SnapshotReads::OneInstant, SnapshotReads::Separate] {
        let held = [
            ("arrived at the request, untimed", None, REQ),
            ("published before the watermark", published(900), 1_500),
            ("executed at the watermark", executed(W), 1_500),
        ];
        for (case, time, now) in held {
            let mut reg = seeded(reads, 20);
            let mut l = ledger();
            let f = liquidation(case, None, Side::Sell, 5);
            assert_eq!(
                apply(&mut reg, &mut l, &f, time, now),
                in_snapshot(VenueFillKind::Liquidation),
                "{reads:?} {case}"
            );
            assert_eq!(reg.position(INST), Some(SignedLots(20)), "{reads:?} {case}");
            // Kept, so it never counts later.
            assert!(l.contains(&f.key()), "{reads:?} {case}");
            let next = resync_at(&mut reg, reads, (2_000, 2_000), 20);
            assert_eq!(
                next.checks[0],
                PositionCheck::Agrees {
                    inst: INST,
                    position: SignedLots(20)
                },
                "{reads:?} {case}"
            );
        }
        let after = [
            ("published after the watermark", published(1_100), 1_500),
            ("executed after the watermark", executed(1_100), 1_500),
            (
                "timed before the watermark, of no stated kind",
                timed(ExchTsKind::Unknown, 900),
                1_500,
            ),
            ("arrived after the request, untimed", None, 1_500),
        ];
        for (case, time, now) in after {
            let mut reg = seeded(reads, 20);
            let mut l = ledger();
            let f = liquidation(case, None, Side::Sell, 5);
            assert_eq!(
                apply(&mut reg, &mut l, &f, time, now),
                counted(VenueFillKind::Liquidation),
                "{reads:?} {case}"
            );
            assert_eq!(reg.position(INST), Some(SignedLots(15)), "{reads:?} {case}");
            let next = resync_at(&mut reg, reads, (2_000, 2_000), 15);
            assert_eq!(
                next.checks[0],
                PositionCheck::Agrees {
                    inst: INST,
                    position: SignedLots(15)
                },
                "{reads:?} {case}"
            );
        }
    }
}

/// A later resync compares the inventory as of its watermark: a venue fill counted after the
/// seed is in it by its arrival before that resync's request, or by a publish time (an upper
/// bound on its execution) at or before its watermark; one published after it is not, and the
/// resync after that holds it by its arrival.
#[test]
fn a_later_resync_holds_a_venue_fill_by_its_arrival_or_its_time_at_or_before_its_watermark() {
    let mut reg = seeded(SnapshotReads::Separate, 20);
    let mut l = ledger();
    // Arriving after the next resync's request, published before its watermark.
    let liq = liquidation("LIQUIDATION:1", None, Side::Sell, 5);
    apply(&mut reg, &mut l, &liq, published(2_500), 3_500);
    // Arriving after it too, published after its watermark.
    let transfer = venue_fill(
        VenueFillKind::Transfer,
        "UNWIND_TRANSFER:1",
        None,
        Side::Buy,
        2,
        OFF_GRID,
        no_fee(),
    );
    apply(&mut reg, &mut l, &transfer, published(3_200), 3_600);
    assert_eq!(reg.position(INST), Some(SignedLots(17)));
    let next = resync_at(&mut reg, SnapshotReads::Separate, (3_000, 3_000), 15);
    assert_eq!(
        next.checks[0],
        PositionCheck::Agrees {
            inst: INST,
            position: SignedLots(15)
        }
    );
    let last = resync_at(&mut reg, SnapshotReads::Separate, (4_000, 4_000), 17);
    assert_eq!(
        last.checks[0],
        PositionCheck::Agrees {
            inst: INST,
            position: SignedLots(17)
        }
    );
    assert_eq!(reg.position(INST), Some(SignedLots(17)));
}

/// The residual 0120 accepts: a liquidation the seed's position read already held, delivered
/// after the seed applied and timed after its watermark or not timed, is counted again. A
/// publish time bounds the execution only from above, so the liquidation may have executed
/// between the request and the read or shortly before the request: a fill in flight while the
/// seed was taken. It is reported at the next resync, never silent, and the inventory is never
/// overwritten (0055).
#[test]
fn a_venue_fill_in_flight_across_the_seed_counted_twice_is_reported_as_a_desync() {
    let cases = [
        (
            "executed between the request and the read, published after the watermark",
            published(1_100),
        ),
        (
            "executed before the request, published after the watermark",
            published(1_010),
        ),
        ("untimed, executed before the read", None),
    ];
    for reads in [SnapshotReads::OneInstant, SnapshotReads::Separate] {
        for (case, time) in cases {
            // Long 20, then liquidated 15 before the venue read the positions: the seed shows 5.
            let mut reg = seeded(reads, 5);
            let mut l = ledger();
            let liq = liquidation("LIQUIDATION:1", None, Side::Sell, 15);
            assert_eq!(
                apply(&mut reg, &mut l, &liq, time, 1_500),
                counted(VenueFillKind::Liquidation),
                "{reads:?} {case}"
            );
            assert_eq!(
                reg.position(INST),
                Some(SignedLots(-10)),
                "{reads:?} {case}"
            );
            let next = resync_at(&mut reg, reads, (2_000, 2_000), 5);
            assert_eq!(
                next.checks[0],
                PositionCheck::Desync {
                    inst: INST,
                    venue: SignedLots(5),
                    ledger: Some(SignedLots(-10)),
                },
                "{reads:?} {case}"
            );
            assert_eq!(
                reg.position(INST),
                Some(SignedLots(-10)),
                "{reads:?} {case}"
            );
        }
    }
}

/// On a market seeded by hand a venue fill counts, whatever its time, and a later resync agrees;
/// on one whose position an order's fill made unknown it moves the inventory and the position
/// stays unknown.
#[test]
fn a_venue_fill_on_a_market_seeded_by_hand_or_unsettled_moves_the_inventory() {
    let mut reg = registry();
    reg.seed_position(INST, SignedLots(20)).unwrap();
    let mut l = ledger();
    let liq = liquidation("LIQUIDATION:1", None, Side::Sell, 5);
    assert_eq!(
        apply(&mut reg, &mut l, &liq, published(500), 500),
        counted(VenueFillKind::Liquidation)
    );
    assert_eq!(reg.position(INST), Some(SignedLots(15)));
    let next = resync_at(&mut reg, SnapshotReads::OneInstant, (W, REQ), 15);
    assert_eq!(
        next.checks[0],
        PositionCheck::Agrees {
            inst: INST,
            position: SignedLots(15)
        }
    );

    // Unsettled: an order held before the seed, not shown, filled after it with nothing to
    // place the fill.
    let mut reg = registry();
    let mut l = ledger();
    let early = cid();
    reg.insert(placement(early, 100, 10)).unwrap();
    resync_at(&mut reg, SnapshotReads::OneInstant, (W, REQ), 20);
    let own = fill(Some(early), ident("FILL:early"), Side::Buy, 3, false);
    assert_eq!(
        apply(&mut reg, &mut l, &own, None, 1_500),
        FillRouted::Unsettled(early)
    );
    assert_eq!(reg.position(INST), None);
    assert_eq!(
        apply(&mut reg, &mut l, &liq, published(1_600), 1_600),
        counted(VenueFillKind::Liquidation)
    );
    assert_eq!(reg.inventory(INST), SignedLots(18));
    assert_eq!(reg.position(INST), None);
    let next = resync_at(&mut reg, SnapshotReads::OneInstant, (2_000, 2_000), 18);
    assert_eq!(next.checks[0], PositionCheck::Unsettled(INST));
}

/// A venue fill moves the position revision (0066): an Exit place authorized before a
/// liquidation shrank the position it was sized against is refused at submit, while one the
/// seed held, moving nothing, leaves it passing; a liquidation to flat leaves Exit admitting
/// nothing.
#[test]
fn a_liquidation_voids_an_exit_authorization_built_before_it() {
    let mut reg = seeded(SnapshotReads::Separate, 10);
    let leases = arm::leases(&reg, INST);
    reg.flatten(INST, leases).unwrap();
    let exit = |qty| NewOrder {
        side: Side::Sell,
        kind: OrderKind::Limit { px: Ticks(101) },
        reduce_only: true,
        reducing: true,
        ..placement(cid(), 101, qty)
    };
    let cmd = reg.place(exit(10)).unwrap();
    let auth = reg.authorize(arm::TEST_ACCT, cmd).unwrap();
    assert_eq!(auth.check_at_submit(), Ok(()));
    let mut l = ledger();
    let held = liquidation("LIQUIDATION:held", None, Side::Sell, 4);
    assert_eq!(
        apply(&mut reg, &mut l, &held, published(900), 1_500),
        in_snapshot(VenueFillKind::Liquidation)
    );
    assert_eq!(auth.check_at_submit(), Ok(()));
    let after = liquidation("LIQUIDATION:after", None, Side::Sell, 4);
    assert_eq!(
        apply(&mut reg, &mut l, &after, published(1_100), 1_500),
        counted(VenueFillKind::Liquidation)
    );
    assert_eq!(reg.position(INST), Some(SignedLots(6)));
    assert_eq!(
        auth.check_at_submit(),
        Err(StaleAuthorization::PositionMoved(INST))
    );
    let flat = liquidation("LIQUIDATION:flat", None, Side::Sell, 6);
    apply(&mut reg, &mut l, &flat, published(1_200), 1_600);
    assert_eq!(reg.position(INST), Some(SignedLots(0)));
    match reg.place(exit(1)) {
        Err(why) => assert_eq!(
            why,
            OmsError::State(StateRefusal::Exit(ExitRefusal::Flat(INST)))
        ),
        Ok(cmd) => panic!("Exit built an order on a flat position: {cmd:?}"),
    }
    assert_eq!(reg.entry(INST).state(), EntryState::Exit(ExitKind::Flatten));
}

/// A venue fill that would overflow the inventory is refused: nothing counted, and the ledger
/// does not record it, so delivered again it is judged again. One the seed holds moves
/// nothing, so it never overflows.
#[test]
fn a_venue_fill_that_would_overflow_the_inventory_is_refused_and_not_recorded() {
    let top = i64::MAX - 1;
    let mut reg = registry();
    reg.seed_position(INST, SignedLots(top)).unwrap();
    let mut l = ledger();
    let settle = venue_fill(
        VenueFillKind::Settlement,
        "SETTLE_MARKET:1",
        None,
        Side::Buy,
        5,
        OFF_GRID,
        no_fee(),
    );
    assert_eq!(
        try_apply(&mut reg, &mut l, &settle, published(1_500), 1_500),
        Err(OmsError::FillOverflow(INST))
    );
    assert_eq!(reg.inventory(INST), SignedLots(top));
    assert!(!l.contains(&settle.key()) && l.is_empty());
    assert!(matches!(
        l.admit(&settle, published(1_600), MonoNs(1_600)),
        Admission::Apply(_)
    ));

    let mut reg = seeded(SnapshotReads::OneInstant, top);
    let mut l = ledger();
    assert_eq!(
        apply(&mut reg, &mut l, &settle, published(900), 1_500),
        in_snapshot(VenueFillKind::Settlement)
    );
    assert_eq!(reg.position(INST), Some(SignedLots(top)));
}

/// A venue fill accepted by another ledger than the registry's is refused, counting nothing and
/// recorded nowhere.
#[test]
fn a_venue_fill_from_another_ledger_is_refused() {
    let mut reg = seeded(SnapshotReads::Separate, 20);
    let (mut first, mut other) = (ledger(), ledger());
    let liq = liquidation("LIQUIDATION:1", None, Side::Sell, 5);
    apply(&mut reg, &mut first, &liq, published(1_500), 1_500);
    let next = liquidation("LIQUIDATION:2", None, Side::Sell, 5);
    assert_eq!(
        try_apply(&mut reg, &mut other, &next, published(1_600), 1_600),
        Err(OmsError::OtherLedger)
    );
    assert!(other.is_empty());
    assert_eq!(reg.position(INST), Some(SignedLots(15)));
}

/// A replayed venue fill follows the ledger's replay rules (I3): with only a publish time
/// nothing shows it executed after the session started; executed at or before the session's
/// watermark, its starting position holds it; executed after it and absent, it applies once.
#[test]
fn a_replayed_venue_fill_follows_the_ledgers_replay_rules() {
    let mut reg = seeded(SnapshotReads::Separate, 20);
    let mut l = ledger_from(WallNs(W));
    let replay = |fid: &str| FillEvent {
        replay: true,
        ..liquidation(fid, None, Side::Sell, 5)
    };
    assert!(matches!(
        l.admit(&replay("LIQUIDATION:pub"), published(2_000), MonoNs(2_000)),
        Admission::Untimed
    ));
    assert!(matches!(
        l.admit(&replay("LIQUIDATION:old"), executed(W), MonoNs(2_000)),
        Admission::BeforeWatermark
    ));
    let fresh = replay("LIQUIDATION:new");
    assert_eq!(
        apply(&mut reg, &mut l, &fresh, executed(1_500), 2_000),
        counted(VenueFillKind::Liquidation)
    );
    assert!(matches!(
        l.admit(&fresh, executed(1_500), MonoNs(2_100)),
        Admission::Duplicate
    ));
    assert_eq!(reg.position(INST), Some(SignedLots(15)));
    assert_eq!(
        l.replays(),
        ReplayCounts {
            applied: 1,
            duplicate: 1,
            before_watermark: 1,
            beyond_horizon: 0,
            untimed: 1,
        }
    );
}
