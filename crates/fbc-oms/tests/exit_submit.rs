//! Exit's admission rechecked at submit (decisions 0012, 0060, 0063 and 0066; Codex's P1 on PR
//! #101, FBC-pfds): a place, a batch or an amend built in Exit is judged against the position
//! when it is built, so its authorization also carries the market's position revision then,
//! and the check at submit refuses it once any fill moved the market's inventory since
//! ([`StaleAuthorization::PositionMoved`]; after its seed, only a fill moves it, decision
//! 0055). Otherwise an exit sized to the position before a fill would cross zero after it:
//! seeded long 10 by hand (in a registry built for a declared owner-assisted testnet run,
//! decision 0067), a reducing sell of 10 authorized, then an own fill of no order the
//! registry holds selling 10, and the sell, sent, leaves the position short 10.
//!
//! An Exit authorization with no position change since its build still passes; a fill that
//! moves no inventory (another namespace's) or moves another market's leaves it passing; a
//! cancel always passes; and an authorization built while Quoting carries no position
//! revision, so only 0060's state generation holds it back (the caps there are FBC-0d9k's).
//!
//! The values are the owner's first test values: a $50 inventory cap on a synthetic market
//! where one lot is worth $1, so 50 lots; the position each scenario exits is 10 lots, long
//! and short.

#[path = "common/arm.rs"]
mod arm;
mod common;

use std::time::Duration;

use arm::{leases, named, start};
use common::{cid, fill, ident, lots, order_caps, placement, vid};
use fbc_core::{
    AccountKey, AckLevel, AmendAck, AmendCaps, AmendQty, CidMatch, ClientOrderId, InstrumentId,
    ItemRef, MonoNs, Namespace, NewOrder, OrderCaps, OrderKind, RefKind, Side, SignedLots,
    SubmitOutcome, TagSet, Ticks, WallNs,
};
use fbc_oms::{
    Admission, Authorization, CancelChoice, FillLedger, FillRouted, LadderConfig, Leases,
    LedgerConfig, MarketCapsConfig, OrderKey, OrderOp, PermittedCommand, PreTradeCaps, Registry,
    ResyncSnapshot, StaleAuthorization, TestnetRun,
};

const INST: InstrumentId = InstrumentId::new(1);
const OTHER: InstrumentId = InstrumentId::new(2);
const ACCT: AccountKey = AccountKey::new(1);
/// The owner's first inventory cap, in lots of $1.
const CAP: i64 = 50;
/// A resting cap past any order of these tests.
const WIDE: i64 = 1_000_000;
/// The size of the position each scenario exits.
const POS: i64 = 10;

fn caps() -> PreTradeCaps {
    [INST, OTHER]
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
        .unwrap()
}

/// A named registry under the owner's caps, built for a declared owner-assisted testnet run
/// (decision 0067), the only registry in which a market seeded by hand is armed: the
/// `testnet_trade` sample's (FBC-x69b).
fn testnet_run() -> Registry {
    named(Registry::with_caps(caps()).for_testnet_run(TestnetRun::owner_assisted()))
}

/// [`testnet_run`], both markets seeded by hand at `pos`, as that run seeds them, `INST` then
/// moved into Exit by Flatten.
fn flattening(pos: i64) -> Registry {
    let mut reg = testnet_run();
    for m in [INST, OTHER] {
        reg.seed_position(m, SignedLots(pos)).unwrap();
    }
    flatten(&mut reg, INST);
    reg
}

/// Moves `market` into Exit by Flatten, arming it with its leases when it is disarmed.
fn flatten(reg: &mut Registry, market: InstrumentId) {
    let given = if reg.entry(market).armed() {
        Leases::none()
    } else {
        leases(reg, market)
    };
    reg.flatten(market, given).unwrap();
}

/// The side that reduces a position of `pos`.
fn reducing(pos: i64) -> Side {
    if pos > 0 { Side::Sell } else { Side::Buy }
}

/// An exit order of `qty` on `side`: reduce-only and classified reducing.
fn exit_of(side: Side, qty: i64) -> NewOrder {
    let px = match side {
        Side::Buy => 100,
        Side::Sell => 101,
    };
    NewOrder {
        side,
        kind: OrderKind::Limit { px: Ticks(px) },
        reduce_only: true,
        reducing: true,
        ..placement(cid(), px, qty)
    }
}

/// A venue whose amend keeps the venue id.
fn venue() -> OrderCaps {
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

fn authorize(reg: &mut Registry, cmd: PermittedCommand) -> Authorization {
    reg.authorize(ACCT, cmd).unwrap()
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

/// Applies a live fill of `qty` on `side`, named `fid`, `on` `(inst, cid, foreign)`: on the
/// market `inst` under our namespace's client id `cid` (an order the registry may not hold)
/// or, when `foreign`, another namespace's, arriving at `MonoNs(0)`: before any resync of
/// these tests was requested.
fn apply(
    reg: &mut Registry,
    l: &mut FillLedger,
    on: (InstrumentId, ClientOrderId, bool),
    side: Side,
    qty: i64,
    fid: &str,
) -> FillRouted {
    apply_at(reg, l, on, side, qty, fid, MonoNs(0))
}

/// [`apply`], the fill arriving at `at`.
fn apply_at(
    reg: &mut Registry,
    l: &mut FillLedger,
    (inst, cid, foreign): (InstrumentId, ClientOrderId, bool),
    side: Side,
    qty: i64,
    fid: &str,
    at: MonoNs,
) -> FillRouted {
    let mut f = fill(Some(cid), ident(fid), side, qty, false);
    f.inst = inst;
    if foreign {
        f.cid = Some(CidMatch::Foreign(Namespace::new(99)));
    }
    match l.admit(&f, None, at) {
        Admission::Apply(a) => reg.apply_fill(a).unwrap(),
        other => panic!("expected the fill accepted, got {other:?}"),
    }
}

/// The Codex sequence: seeded by hand, Flatten, a reducing place, batch and amend authorized,
/// then an own fill of no order the registry holds on the reducing side flattening the
/// position. Each authorization is refused at submit: sent, it would cross zero.
#[test]
fn an_exit_place_batch_or_amend_authorized_before_a_fill_moved_the_position_is_refused_at_submit() {
    for pos in [POS, -POS] {
        let side = reducing(pos);
        // Each kind on registries of its own, so the fill reaches the one it was built on.
        for kind in ["place", "batch", "amend"] {
            let mut reg = flattening(pos);
            let (name, auth) = exits_one(&mut reg, pos, kind);
            assert_eq!(
                auth.check_at_submit(),
                Ok(()),
                "{pos} {name}: before the fill"
            );
            let mut l = ledger();
            let routed = apply(&mut reg, &mut l, (INST, cid(), false), side, POS, "f-1");
            assert!(matches!(routed, FillRouted::OursUntracked(_)), "{routed:?}");
            assert_eq!(reg.inventory(INST), SignedLots(0), "{pos} {name}");
            assert_eq!(
                auth.check_at_submit(),
                Err(StaleAuthorization::PositionMoved(INST)),
                "{pos} {name}: the fill flattened the position it was sized against"
            );
        }
    }
}

/// One of Exit's three builds on `reg`: `kind` is "place", "batch" or "amend".
fn exits_one(reg: &mut Registry, pos: i64, kind: &'static str) -> (&'static str, Authorization) {
    let side = reducing(pos);
    let cmd = match kind {
        "place" => reg.place(exit_of(side, POS)).unwrap(),
        "batch" => {
            let plan = reg
                .place_batch(vec![exit_of(side, POS / 2), exit_of(side, POS / 2)])
                .unwrap();
            assert!(plan.refused.is_empty(), "{:?}", plan.refused);
            plan.command.unwrap()
        }
        "amend" => {
            let c = open(reg, exit_of(side, 4), "v-amend");
            let px = reg.get(c).unwrap().px().unwrap();
            reg.live(c)
                .unwrap()
                .amend(&venue(), px, lots(POS), true)
                .unwrap()
        }
        other => unreachable!("{other}"),
    };
    (kind, authorize(reg, cmd))
}

/// The same with no position change in between, or with fills that move no inventory on the
/// market (another namespace's) or move another market's: each still passes.
#[test]
fn an_exit_authorization_with_no_position_change_since_its_build_still_passes() {
    for pos in [POS, -POS] {
        let side = reducing(pos);
        for kind in ["place", "batch", "amend"] {
            let mut reg = flattening(pos);
            let (name, auth) = exits_one(&mut reg, pos, kind);
            assert_eq!(auth.check_at_submit(), Ok(()), "{pos} {name}");
            let mut l = ledger();
            let routed = apply(
                &mut reg,
                &mut l,
                (INST, cid(), true),
                side,
                POS,
                "f-foreign",
            );
            assert!(matches!(routed, FillRouted::Foreign(_)), "{routed:?}");
            let routed = apply(
                &mut reg,
                &mut l,
                (OTHER, cid(), false),
                side,
                POS,
                "f-other",
            );
            assert!(matches!(routed, FillRouted::OursUntracked(_)), "{routed:?}");
            assert_eq!(reg.inventory(INST), SignedLots(pos), "{pos} {name}");
            assert_eq!(reg.inventory(OTHER), SignedLots(0), "{pos} {name}");
            assert_eq!(
                auth.check_at_submit(),
                Ok(()),
                "{pos} {name}: nothing moved INST"
            );
        }
    }
}

/// A fill of one of our own exit orders moves the position too, so every other Exit
/// authorization built before it is refused, and the planner builds again against the new
/// position; the cancel of an order, built before the fill or after, still passes.
#[test]
fn after_any_fill_that_moved_the_position_exit_builds_again_and_cancels_still_pass() {
    for pos in [POS, -POS] {
        let side = reducing(pos);
        let mut reg = flattening(pos);
        let resting = open(&mut reg, exit_of(side, 4), "v-rest");
        let place = reg.place(exit_of(side, 3)).unwrap();
        let place = authorize(&mut reg, place);
        let early = match reg.cancellable(resting).unwrap().cancel(&order_caps()) {
            CancelChoice::Send(cmd) => authorize(&mut reg, cmd),
            other => panic!("expected a cancel to send, got {other:?}"),
        };

        let mut l = ledger();
        let routed = apply(&mut reg, &mut l, (INST, resting, false), side, 2, "f-own");
        assert!(
            matches!(routed, FillRouted::Ours(c, _) if c == resting),
            "{routed:?}"
        );
        assert_eq!(
            place.check_at_submit(),
            Err(StaleAuthorization::PositionMoved(INST)),
            "{pos}: our own exit's fill moved the position"
        );
        assert_eq!(
            early.check_at_submit(),
            Ok(()),
            "{pos}: a cancel always passes"
        );

        // Built again after the fill, against the position it left, it passes.
        let again = reg.place(exit_of(side, 3)).unwrap();
        let again = authorize(&mut reg, again);
        assert_eq!(
            again.check_at_submit(),
            Ok(()),
            "{pos}: built after the fill"
        );
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

fn resync(reg: &mut Registry, pos: i64, n: u32) {
    let snap = ResyncSnapshot {
        watermark: WallNs(1_000 * i64::from(n)),
        requested_at: MonoNs(1_000 * u64::from(n)),
        orders: vec![],
        positions: vec![(INST, SignedLots(pos)), (OTHER, SignedLots(0))],
    };
    let key = OrderKey {
        venue: None,
        ingest: u64::from(n),
    };
    reg.resync(&ladder_cfg(), &venue(), &snap, key).unwrap();
}

/// A market seeded by a resync instead of by hand: a fill of our exit order placed after the
/// seed refuses the Exit place built before it, as on a hand-seeded market. A later resync,
/// which never overwrites the inventory (decision 0055), leaves an Exit place built before it
/// passing.
#[test]
fn on_a_resync_seeded_market_a_fill_refuses_an_exit_built_before_it_and_a_later_resync_does_not() {
    for pos in [POS, -POS] {
        let side = reducing(pos);
        let mut reg = named(Registry::with_caps(caps()));
        resync(&mut reg, pos, 1);
        flatten(&mut reg, INST);
        let resting = open(&mut reg, exit_of(side, 4), "v-seeded");
        let place = reg.place(exit_of(side, POS - 4)).unwrap();
        let place = authorize(&mut reg, place);
        resync(&mut reg, pos, 2);
        assert_eq!(place.check_at_submit(), Ok(()), "{pos}: a later resync");
        let mut l = ledger();
        // Arriving after both resyncs were requested, of an order placed after the seed.
        let routed = apply_at(
            &mut reg,
            &mut l,
            (INST, resting, false),
            side,
            1,
            "f-r",
            MonoNs(5_000),
        );
        assert!(
            matches!(routed, FillRouted::Ours(c, _) if c == resting),
            "{routed:?}"
        );
        assert_eq!(
            place.check_at_submit(),
            Err(StaleAuthorization::PositionMoved(INST)),
            "{pos}: a fill after the seed"
        );
    }
}

/// Outside Exit the position revision is not part of the guard: a place built while Quoting
/// passes after a fill moved the position (only 0060's state generation holds it back).
#[test]
fn a_quoting_place_is_not_held_back_by_a_fill() {
    let mut reg = testnet_run();
    reg.seed_position(INST, SignedLots(POS)).unwrap();
    start(&mut reg, INST);
    let place = reg.place(placement(cid(), 100, 2)).unwrap();
    let place = authorize(&mut reg, place);
    let mut l = ledger();
    apply(&mut reg, &mut l, (INST, cid(), false), Side::Sell, 3, "f-q");
    assert_eq!(reg.inventory(INST), SignedLots(POS - 3));
    assert_eq!(place.check_at_submit(), Ok(()));
}

/// A fill applied while a market was quoting moves the revision, and an Exit command built
/// after it, in the same market, is judged against the moved position and passes.
#[test]
fn an_exit_built_after_the_fill_passes() {
    let mut reg = testnet_run();
    reg.seed_position(INST, SignedLots(POS)).unwrap();
    start(&mut reg, INST);
    let mut l = ledger();
    apply(&mut reg, &mut l, (INST, cid(), false), Side::Sell, 3, "f-x");
    assert_eq!(reg.inventory(INST), SignedLots(POS - 3));
    flatten(&mut reg, INST);
    let place = reg.place(exit_of(Side::Sell, POS - 3)).unwrap();
    let place = authorize(&mut reg, place);
    assert_eq!(place.check_at_submit(), Ok(()));
}
