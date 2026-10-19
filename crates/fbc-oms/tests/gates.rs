//! The planner's batching, venue gates and safety floor (design §4.10 steps 6 and 7; decision
//! 0111): under `Batching::Declared` it batches places within `batch_place`'s size, one stage
//! (so one traffic class) per batch, and cancels within `batch_cancel`'s size, each item naming
//! a reference the batch declares; it builds no place or amend for a market whose trading
//! status or venue mode refuses it, while its cancels still go out; and it withholds normal
//! commands once a rate scope would drop below the consumer's floor, while cancels and
//! reducing orders still go out.
//!
//! The values are 0054's for Paradex where a test needs a batch size (batch_place and
//! batch_cancel of 10, batch_cancel by venue id only) and the owner's first test values
//! otherwise: a $50 inventory cap, one lot worth $1.

#[path = "common/arm.rs"]
mod arm;
mod common;

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use arm::start;
use common::{lease_dir, lots, order_caps, vid};
use fbc_core::{
    AccountKey, AckLevel, Batch, Bps, CancelBatch, Channel, ChosenRef, CidMint, ClientOrderId,
    InstrumentId, ItemRef, LimitScope, MonoNs, Namespace, NamespaceLease, OpKind, OrderCaps,
    RateLimit, RefKind, Side, SignedLots, SubmitOutcome, TagSet, Ticks, Tif, TradingStatus,
    TrafficClass, VenueCommand, VenueMode, WallNs,
};
use fbc_oms::{
    Batching, DesiredBook, DesiredQuote, ExecutionPlanner, LadderConfig, MarketCapsConfig,
    OrderKey, OrderOp, Plan, PlanRefusal, PlanVenue, Planned, PlannerConfig, PreTradeCaps,
    RateScope, RateView, ReduceFallback, Registry, ResyncSnapshot, Stage,
};

const INST: InstrumentId = InstrumentId::new(1);
const OTHER: InstrumentId = InstrumentId::new(2);
const ACCT: AccountKey = AccountKey::new(1);
/// The owner's first inventory cap, in lots of $1.
const CAP: i64 = 50;
/// A resting cap past any order of these tests.
const WIDE: i64 = 1_000_000;
/// 0054's Paradex batch sizes.
const PARADEX_BATCH: u16 = 10;

fn mint() -> CidMint {
    static NEXT: AtomicU16 = AtomicU16::new(400);
    let ns = Namespace::new(NEXT.fetch_add(1, Ordering::Relaxed));
    let lease = NamespaceLease::acquire(&lease_dir(), AccountKey::new(2), ns).unwrap();
    CidMint::new(lease, 0, 0, WallNs(0))
}

fn planner() -> ExecutionPlanner {
    ExecutionPlanner::new(PlannerConfig::new(Bps(2.0), lots(2), Duration::ZERO).unwrap())
}

/// A venue that batches places and cancels as 0054 declares Paradex's: ten places a batch, ten
/// cancels a cancel-many naming venue ids only; single cancels by venue id or client id once
/// acknowledged.
fn batching() -> OrderCaps {
    OrderCaps {
        batch_place: Some(Batch {
            max_items: PARADEX_BATCH,
        }),
        batch_cancel: Some(CancelBatch {
            max_items: PARADEX_BATCH,
            refs: TagSet::of(&[RefKind::Venue]),
        }),
        cancel_refs: TagSet::of(&[RefKind::Venue, RefKind::Client]),
        ..order_caps()
    }
}

/// `caps` as `batching` with batches of `n`.
fn batches_of(n: u16) -> OrderCaps {
    OrderCaps {
        batch_place: Some(Batch { max_items: n }),
        batch_cancel: Some(CancelBatch {
            max_items: n,
            refs: TagSet::of(&[RefKind::Venue]),
        }),
        ..batching()
    }
}

/// A pass on `caps`, the account and market in Normal mode, trading, no rate scope tracked,
/// batching as declared.
fn declared(caps: &OrderCaps) -> PlanVenue<'_> {
    PlanVenue {
        caps,
        account_mode: VenueMode::Normal,
        market_mode: VenueMode::Normal,
        status: TradingStatus::Trading,
        rates: RateView::new(Vec::new()),
        batching: Batching::Declared,
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

/// A named registry under the inventory cap and a wide resting cap on `INST`, whose first
/// trustworthy resync showed no order and seeded `INST` at `pos`, armed and Quoting.
fn quoting(pos: i64) -> Registry {
    let caps = PreTradeCaps::new()
        .with_market(
            INST,
            MarketCapsConfig {
                inventory: Some(lots(CAP)),
                resting: Some(lots(WIDE)),
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
    reg.resync(&ladder_cfg(), &batching(), &snap, key(1))
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

/// A post-only GTC limit quote on the public book of `qty` at `px`, tagged `tag`.
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
        reduce_fallback: ReduceFallback::KeepReduceOnly,
    }
}

/// `q` as an exit sends it: reduce-only, classified reducing, not post-only.
fn exit(q: DesiredQuote) -> DesiredQuote {
    DesiredQuote {
        post_only: false,
        reduce_only: true,
        reducing: true,
        ..q
    }
}

/// `levels` bids, level n at 10 000 - n, one lot each, tagged their level.
fn bids(levels: u16) -> DesiredBook {
    (0..levels).fold(DesiredBook::new(INST), |book, n| {
        book.with(Side::Buy, n, quote(10_000 - i64::from(n), 1, n))
    })
}

/// Acknowledges every order `plan` placed, under venue ids `v-<side>-<level>`.
fn ack_all(reg: &mut Registry, plan: &Plan) {
    for p in &plan.commands {
        for (cid, side, level) in items(p) {
            let item = ItemRef {
                idx: 0,
                cid: None,
                vid: Some(vid(&format!("v-{side:?}-{level}"))),
            };
            let accepted = SubmitOutcome::Accepted {
                ack: AckLevel::Final,
            };
            reg.on_outcome(cid, OrderOp::Place, &item, &accepted, MonoNs(1))
                .unwrap();
        }
    }
}

/// Each order of a planned command: its client id, side and level, in the command's order.
fn items(p: &Planned) -> Vec<(ClientOrderId, Side, u16)> {
    std::iter::once((p.cid, p.side, p.level))
        .chain(p.more_items.iter().map(|i| (i.cid, i.side, i.level)))
        .collect()
}

/// The client ids a command carries, as the venue command names them.
fn command_cids(cmd: &VenueCommand) -> Vec<ClientOrderId> {
    match cmd {
        VenueCommand::Place(o) => vec![o.cid],
        VenueCommand::PlaceBatch(os) => os.iter().map(|o| o.cid).collect(),
        VenueCommand::Amend(a) => vec![a.target.client().unwrap()],
        VenueCommand::Cancel(c) => vec![c.target.client().unwrap()],
        VenueCommand::CancelMany(cs) => cs.iter().map(|c| c.target.client().unwrap()).collect(),
        other => panic!("the planner built {other:?}"),
    }
}

/// Each command's kind and its number of orders, in the order planned; every command's items
/// match the orders its venue command carries.
fn kinds(plan: &Plan) -> Vec<(&'static str, usize)> {
    plan.commands
        .iter()
        .map(|p| {
            let cmd = p.auth.command();
            let ids: Vec<ClientOrderId> = items(p).iter().map(|i| i.0).collect();
            assert_eq!(command_cids(cmd), ids, "{p:?}");
            assert_eq!(p.cids(), ids);
            let kind = match cmd {
                VenueCommand::Place(_) => "place",
                VenueCommand::PlaceBatch(_) => "batch",
                VenueCommand::Amend(_) => "amend",
                VenueCommand::Cancel(_) => "cancel",
                VenueCommand::CancelMany(_) => "cancel-many",
                other => panic!("the planner built {other:?}"),
            };
            (kind, ids.len())
        })
        .collect()
}

#[test]
fn places_go_in_batches_of_at_most_the_declared_size() {
    // Twelve bids against 0054's ten: a batch of ten, then a batch of two.
    let mut reg = quoting(0);
    let caps = batching();
    let plan = planner()
        .plan(
            &bids(12),
            &mut reg,
            &declared(&caps),
            ACCT,
            &mut mint(),
            MonoNs(0),
        )
        .unwrap();
    assert!(plan.refused.is_empty(), "{:?}", plan.refused);
    assert_eq!(kinds(&plan), vec![("batch", 10), ("batch", 2)]);
    // Each item keeps its level and tag, the batch's in the levels' order.
    let all: Vec<(Side, u16)> = plan
        .commands
        .iter()
        .flat_map(|p| items(p).into_iter().map(|(_, s, l)| (s, l)))
        .collect();
    assert_eq!(all, (0..12).map(|n| (Side::Buy, n)).collect::<Vec<_>>());
    for p in &plan.commands {
        assert_eq!(p.stage, Stage::Add);
        assert_eq!(p.tag, Some(p.level));
        assert!(p.more_items.iter().all(|i| i.tag == Some(i.level)));
        assert_eq!(p.auth.command().traffic_class(), TrafficClass::Normal);
    }
    // Every order is registered, PendingNew, and counted.
    assert_eq!(reg.len(), 12);

    // Batches of three: 3, 3, 1, and a run of one is a single place.
    let mut reg = quoting(0);
    let caps = batches_of(3);
    let plan = planner()
        .plan(
            &bids(7),
            &mut reg,
            &declared(&caps),
            ACCT,
            &mut mint(),
            MonoNs(0),
        )
        .unwrap();
    assert_eq!(kinds(&plan), vec![("batch", 3), ("batch", 3), ("place", 1)]);
}

#[test]
fn reducing_orders_never_share_a_batch_with_adds() {
    // Long 10: two reduce-only asks reduce it, two bids add to it.
    let mut reg = quoting(10);
    let caps = batching();
    let book = bids(2)
        .with(Side::Sell, 0, exit(quote(10_010, 1, 20)))
        .with(Side::Sell, 1, exit(quote(10_011, 1, 21)));
    let plan = planner()
        .plan(
            &book,
            &mut reg,
            &declared(&caps),
            ACCT,
            &mut mint(),
            MonoNs(0),
        )
        .unwrap();
    assert!(plan.refused.is_empty(), "{:?}", plan.refused);
    assert_eq!(kinds(&plan), vec![("batch", 2), ("batch", 2)]);
    let reducing = &plan.commands[0];
    assert_eq!(reducing.stage, Stage::Reducing);
    assert_eq!(
        reducing.auth.command().traffic_class(),
        TrafficClass::Safety
    );
    assert!(items(reducing).iter().all(|i| i.1 == Side::Sell));
    let adds = &plan.commands[1];
    assert_eq!(adds.stage, Stage::Add);
    assert_eq!(adds.auth.command().traffic_class(), TrafficClass::Normal);
    assert!(items(adds).iter().all(|i| i.1 == Side::Buy));
}

#[test]
fn batching_one_sends_one_order_per_command_whatever_the_caps() {
    let mut reg = quoting(0);
    let caps = batching();
    let venue = PlanVenue {
        batching: Batching::One,
        ..declared(&caps)
    };
    let mut p = planner();
    let mut m = mint();
    let plan = p
        .plan(&bids(3), &mut reg, &venue, ACCT, &mut m, MonoNs(0))
        .unwrap();
    assert_eq!(kinds(&plan), vec![("place", 1); 3]);
    ack_all(&mut reg, &plan);
    let pulled = p
        .plan(
            &DesiredBook::new(INST),
            &mut reg,
            &venue,
            ACCT,
            &mut m,
            MonoNs(2),
        )
        .unwrap();
    assert_eq!(kinds(&pulled), vec![("cancel", 1); 3]);
}

#[test]
fn cancels_go_in_cancel_manys_of_at_most_the_declared_size_naming_only_its_references() {
    // Five bids, four acknowledged (venue ids known) and one not: batches of two by venue id.
    let mut reg = quoting(0);
    let caps = batches_of(2);
    let venue = declared(&caps);
    let mut p = planner();
    let mut m = mint();
    let placed = p
        .plan(&bids(5), &mut reg, &venue, ACCT, &mut m, MonoNs(0))
        .unwrap();
    // Acknowledge all but the order at level 2.
    let mut unacked = None;
    for planned in &placed.commands {
        for (cid, side, level) in items(planned) {
            if level == 2 {
                unacked = Some(cid);
                continue;
            }
            let item = ItemRef {
                idx: 0,
                cid: None,
                vid: Some(vid(&format!("v-{side:?}-{level}"))),
            };
            let accepted = SubmitOutcome::Accepted {
                ack: AckLevel::Final,
            };
            reg.on_outcome(cid, OrderOp::Place, &item, &accepted, MonoNs(1))
                .unwrap();
        }
    }
    let unacked = unacked.unwrap();
    let pulled = DesiredBook::new(INST).with_removal(Side::Buy, Some(7));
    let plan = p
        .plan(&pulled, &mut reg, &venue, ACCT, &mut m, MonoNs(2))
        .unwrap();
    assert!(plan.refused.is_empty(), "{:?}", plan.refused);
    // The order not acknowledged has no venue id: no cancel-many item can name it, and the
    // venue's single cancel takes its client id only once acknowledged: it waits.
    assert_eq!(plan.awaiting_ack, vec![unacked]);
    let mut cancelled = BTreeSet::new();
    for planned in &plan.commands {
        assert_eq!(planned.stage, Stage::Cancel);
        assert_eq!(planned.removal, Some(7));
        assert!(planned.more_items.iter().all(|i| i.removal == Some(7)));
        match planned.auth.command() {
            VenueCommand::CancelMany(cs) => {
                assert!(cs.len() <= 2 && cs.len() > 1, "{cs:?}");
                for c in cs {
                    let batch_refs = caps.batch_cancel.unwrap().refs;
                    assert!(
                        matches!(c.reference(batch_refs), Some(ChosenRef::Venue(_))),
                        "{c:?}"
                    );
                }
            }
            VenueCommand::Cancel(_) => {}
            other => panic!("the planner built {other:?}"),
        }
        for cid in planned.cids() {
            assert!(cancelled.insert(cid), "cancelled twice");
        }
    }
    assert_eq!(cancelled.len(), 4);
    assert!(!cancelled.contains(&unacked));
    // The four acknowledged go as two cancel-manys of two.
    assert_eq!(kinds(&plan), vec![("cancel-many", 2), ("cancel-many", 2)]);
}

#[test]
fn a_cancel_whose_order_the_batch_cannot_name_goes_alone() {
    // A venue whose single cancel takes a client id before the acknowledgement while its
    // cancel-many takes venue ids only: the order not acknowledged is cancelled alone.
    let mut reg = quoting(0);
    let caps = OrderCaps {
        cancel_before_ack: true,
        ..batches_of(2)
    };
    let venue = declared(&caps);
    let mut p = planner();
    let mut m = mint();
    let placed = p
        .plan(&bids(3), &mut reg, &venue, ACCT, &mut m, MonoNs(0))
        .unwrap();
    let mut unacked = None;
    for planned in &placed.commands {
        for (cid, side, level) in items(planned) {
            if level == 1 {
                unacked = Some(cid);
                continue;
            }
            let item = ItemRef {
                idx: 0,
                cid: None,
                vid: Some(vid(&format!("v-{side:?}-{level}"))),
            };
            let accepted = SubmitOutcome::Accepted {
                ack: AckLevel::Final,
            };
            reg.on_outcome(cid, OrderOp::Place, &item, &accepted, MonoNs(1))
                .unwrap();
        }
    }
    let plan = p
        .plan(
            &DesiredBook::new(INST),
            &mut reg,
            &venue,
            ACCT,
            &mut m,
            MonoNs(2),
        )
        .unwrap();
    let singles: Vec<ClientOrderId> = plan
        .commands
        .iter()
        .filter(|c| matches!(c.auth.command(), VenueCommand::Cancel(_)))
        .map(|c| c.cid)
        .collect();
    assert_eq!(singles, vec![unacked.unwrap()]);
    let many: Vec<usize> = plan
        .commands
        .iter()
        .filter_map(|c| match c.auth.command() {
            VenueCommand::CancelMany(cs) => Some(cs.len()),
            _ => None,
        })
        .collect();
    assert_eq!(many, vec![2]);
}

/// A book on a long position of 10: a post-only bid (adds), a bid that is not post-only
/// (adds), and a reduce-only ask that is not post-only (reduces).
fn mixed() -> DesiredBook {
    DesiredBook::new(INST)
        .with(Side::Buy, 0, quote(9_990, 1, 0))
        .with(
            Side::Buy,
            1,
            DesiredQuote {
                post_only: false,
                ..quote(9_989, 1, 1)
            },
        )
        .with(Side::Sell, 0, exit(quote(10_010, 1, 2)))
}

/// The (side, level) of every order `plan` places.
fn placed(plan: &Plan) -> Vec<(Side, u16)> {
    plan.commands
        .iter()
        .filter(|p| p.stage != Stage::Cancel)
        .flat_map(|p| items(p).into_iter().map(|(_, s, l)| (s, l)))
        .collect()
}

/// The (side, level, why) of every refusal.
fn refusals(plan: &Plan) -> Vec<(Side, u16, PlanRefusal)> {
    plan.refused
        .iter()
        .map(|r| (r.side, r.level, r.why.clone()))
        .collect()
}

#[test]
fn nothing_is_built_that_the_trading_status_or_a_venue_mode_refuses() {
    let caps = batching();
    let all = vec![(Side::Sell, 0), (Side::Buy, 0), (Side::Buy, 1)];
    let post_only = vec![(Side::Buy, 0)];
    let reduce_only = vec![(Side::Sell, 0)];
    let none: Vec<(Side, u16)> = vec![];
    type Case = (TradingStatus, VenueMode, VenueMode);
    type Expect = (Case, Vec<(Side, u16)>, Option<PlanRefusal>);
    let cases: Vec<Expect> = vec![
        (
            (TradingStatus::Trading, VenueMode::Normal, VenueMode::Normal),
            all.clone(),
            None,
        ),
        (
            (
                TradingStatus::PostOnly,
                VenueMode::Normal,
                VenueMode::Normal,
            ),
            post_only.clone(),
            Some(PlanRefusal::Status(TradingStatus::PostOnly)),
        ),
        (
            (
                TradingStatus::ReduceOnly,
                VenueMode::Normal,
                VenueMode::Normal,
            ),
            reduce_only.clone(),
            Some(PlanRefusal::Status(TradingStatus::ReduceOnly)),
        ),
        (
            (
                TradingStatus::CancelOnly,
                VenueMode::Normal,
                VenueMode::Normal,
            ),
            none.clone(),
            Some(PlanRefusal::Status(TradingStatus::CancelOnly)),
        ),
        (
            (TradingStatus::Halted, VenueMode::Normal, VenueMode::Normal),
            none.clone(),
            Some(PlanRefusal::Status(TradingStatus::Halted)),
        ),
        (
            (
                TradingStatus::Delisted,
                VenueMode::Normal,
                VenueMode::Normal,
            ),
            none.clone(),
            Some(PlanRefusal::Status(TradingStatus::Delisted)),
        ),
        (
            (
                TradingStatus::Trading,
                VenueMode::PostOnly,
                VenueMode::Normal,
            ),
            post_only.clone(),
            Some(PlanRefusal::VenueMode(VenueMode::PostOnly)),
        ),
        (
            (
                TradingStatus::Trading,
                VenueMode::Normal,
                VenueMode::PostOnly,
            ),
            post_only.clone(),
            Some(PlanRefusal::VenueMode(VenueMode::PostOnly)),
        ),
        (
            (
                TradingStatus::Trading,
                VenueMode::ReduceOnly,
                VenueMode::Normal,
            ),
            reduce_only.clone(),
            Some(PlanRefusal::VenueMode(VenueMode::ReduceOnly)),
        ),
        (
            (
                TradingStatus::Trading,
                VenueMode::Normal,
                VenueMode::ReduceOnly,
            ),
            reduce_only.clone(),
            Some(PlanRefusal::VenueMode(VenueMode::ReduceOnly)),
        ),
        (
            (
                TradingStatus::Trading,
                VenueMode::CancelOnly,
                VenueMode::Normal,
            ),
            none.clone(),
            Some(PlanRefusal::VenueMode(VenueMode::CancelOnly)),
        ),
        (
            (TradingStatus::Trading, VenueMode::Normal, VenueMode::Halted),
            none.clone(),
            Some(PlanRefusal::VenueMode(VenueMode::Halted)),
        ),
        // The account post-only and the market reduce-only: no order is both.
        (
            (
                TradingStatus::Trading,
                VenueMode::PostOnly,
                VenueMode::ReduceOnly,
            ),
            none.clone(),
            None,
        ),
    ];
    for ((status, account_mode, market_mode), built, why) in cases {
        let name = format!("{status:?} {account_mode:?} {market_mode:?}");
        // A resting bid at level 5, no longer wanted, so its cancel is due in every case.
        let mut reg = quoting(10);
        let mut p = planner();
        let mut m = mint();
        let open = p
            .plan(
                &DesiredBook::new(INST).with(Side::Buy, 5, quote(9_900, 1, 5)),
                &mut reg,
                &declared(&caps),
                ACCT,
                &mut m,
                MonoNs(0),
            )
            .unwrap();
        ack_all(&mut reg, &open);
        let before = reg.len();
        let venue = PlanVenue {
            status,
            account_mode,
            market_mode,
            ..declared(&caps)
        };
        let plan = p
            .plan(&mixed(), &mut reg, &venue, ACCT, &mut m, MonoNs(2))
            .unwrap();
        assert_eq!(placed(&plan), built, "{name}");
        // The cancel goes out whatever the status and modes.
        let cancels: Vec<u16> = plan
            .commands
            .iter()
            .filter(|c| c.stage == Stage::Cancel)
            .map(|c| c.level)
            .collect();
        assert_eq!(cancels, vec![5], "{name}");
        // What was refused was never built: no order registered for it.
        assert_eq!(reg.len(), before + built.len(), "{name}");
        let refused = refusals(&plan);
        assert_eq!(refused.len(), 3 - built.len(), "{name}: {refused:?}");
        if let Some(why) = why {
            assert!(refused.iter().all(|r| r.2 == why), "{name}: {refused:?}");
        }
    }
}

#[test]
fn an_amend_the_venue_mode_refuses_is_never_built() {
    // A resting post-only bid to move, on a venue that amends: in Reduce-only it is not
    // amended (it carries no reduce-only flag), and in Post-only it is.
    let caps = OrderCaps {
        amend: Some(fbc_core::AmendCaps {
            refs: TagSet::of(&[RefKind::Venue]),
            price: true,
            qty: true,
            flags: false,
            when_partially_filled: true,
            reject_keeps_original: true,
            keeps_venue_id: true,
            ack: fbc_core::AmendAck::ReplacedEvent,
            qty_semantics: fbc_core::AmendQty::TotalIncludingFilled,
            keeps_priority: None,
        }),
        ..batching()
    };
    for (mode, amended) in [(VenueMode::ReduceOnly, false), (VenueMode::PostOnly, true)] {
        let mut reg = quoting(0);
        let mut p = planner();
        let mut m = mint();
        let open = p
            .plan(
                &bids(1),
                &mut reg,
                &declared(&caps),
                ACCT,
                &mut m,
                MonoNs(0),
            )
            .unwrap();
        ack_all(&mut reg, &open);
        let moved = DesiredBook::new(INST).with(Side::Buy, 0, quote(9_900, 1, 0));
        let venue = PlanVenue {
            market_mode: mode,
            ..declared(&caps)
        };
        let plan = p
            .plan(&moved, &mut reg, &venue, ACCT, &mut m, MonoNs(2))
            .unwrap();
        if amended {
            assert_eq!(kinds(&plan), vec![("amend", 1)], "{mode:?}");
        } else {
            assert!(plan.commands.is_empty(), "{mode:?}: {:?}", kinds(&plan));
            assert_eq!(
                refusals(&plan),
                vec![(Side::Buy, 0, PlanRefusal::VenueMode(mode))]
            );
            let c = open.commands[0].cid;
            assert_eq!(reg.get(c).unwrap().px(), Some(Ticks(10_000)));
        }
    }
}

/// The per-IP bucket 0054 declares for Paradex, 1500 requests a minute counting every order
/// method, with `used` units charged and the consumer's floor of `floor`.
fn per_ip(used: u64, floor: u32) -> RateScope {
    let limit = RateLimit {
        scope: LimitScope::Ip,
        ops: TagSet::of(&[
            OpKind::Place,
            OpKind::Amend,
            OpKind::Cancel,
            OpKind::CancelAll,
            OpKind::Query,
            OpKind::Rest,
        ]),
        per: Duration::from_secs(60),
        units: 1_500,
    };
    RateScope::of(&limit, None, used, floor)
}

#[test]
fn normal_commands_stop_at_the_safety_floor_while_cancels_and_reducing_orders_go_out() {
    const FLOOR: u32 = 150;
    // Long 10, a resting bid at level 5 no longer wanted: a cancel (safety), a reduce-only ask
    // (safety), and four bids (normal).
    let book = bids(4).with(Side::Sell, 0, exit(quote(10_010, 1, 9)));
    for grouping in [Batching::One, Batching::Declared] {
        // Room for the cancel, the ask and two bids above the floor: two bids withheld.
        let mut reg = quoting(10);
        let caps = batching();
        let mut p = planner();
        let mut m = mint();
        let open = p
            .plan(
                &DesiredBook::new(INST).with(Side::Buy, 5, quote(9_900, 1, 5)),
                &mut reg,
                &declared(&caps),
                ACCT,
                &mut m,
                MonoNs(0),
            )
            .unwrap();
        ack_all(&mut reg, &open);
        let before = reg.len();
        let left = FLOOR + 4;
        let venue = PlanVenue {
            rates: RateView::new(vec![per_ip(u64::from(1_500 - left), FLOOR)]),
            batching: grouping,
            ..declared(&caps)
        };
        let plan = p
            .plan(&book, &mut reg, &venue, ACCT, &mut m, MonoNs(2))
            .unwrap();
        assert_eq!(
            placed(&plan),
            vec![(Side::Sell, 0), (Side::Buy, 0), (Side::Buy, 1)],
            "{grouping:?}"
        );
        assert_eq!(
            refusals(&plan),
            vec![
                (Side::Buy, 2, PlanRefusal::RateFloor { scope: 0 }),
                (Side::Buy, 3, PlanRefusal::RateFloor { scope: 0 }),
            ],
            "{grouping:?}"
        );
        assert!(plan.commands.iter().any(|c| c.stage == Stage::Cancel));
        assert_eq!(reg.len(), before + 3, "{grouping:?}");

        // At or below the floor (here none left at all), only the cancel and the reducing
        // order go out.
        let mut reg = quoting(10);
        let mut p = planner();
        let mut m = mint();
        let open = p
            .plan(
                &DesiredBook::new(INST).with(Side::Buy, 5, quote(9_900, 1, 5)),
                &mut reg,
                &declared(&caps),
                ACCT,
                &mut m,
                MonoNs(0),
            )
            .unwrap();
        ack_all(&mut reg, &open);
        let venue = PlanVenue {
            rates: RateView::new(vec![per_ip(1_500, FLOOR)]),
            batching: grouping,
            ..declared(&caps)
        };
        let plan = p
            .plan(&book, &mut reg, &venue, ACCT, &mut m, MonoNs(2))
            .unwrap();
        assert_eq!(placed(&plan), vec![(Side::Sell, 0)], "{grouping:?}");
        assert_eq!(refusals(&plan).len(), 4, "{grouping:?}");
        assert!(
            refusals(&plan)
                .iter()
                .all(|r| r.2 == PlanRefusal::RateFloor { scope: 0 })
        );
        assert!(plan.commands.iter().any(|c| c.stage == Stage::Cancel));
    }
}

#[test]
fn a_batch_is_charged_its_item_count_against_the_floor() {
    // 0054: a batch is charged its item count. Five bids, room for three above the floor: a
    // batch of three, two withheld.
    const FLOOR: u32 = 10;
    let mut reg = quoting(0);
    let caps = batching();
    let venue = PlanVenue {
        rates: RateView::new(vec![per_ip(u64::from(1_500 - FLOOR - 3), FLOOR)]),
        ..declared(&caps)
    };
    let plan = planner()
        .plan(&bids(5), &mut reg, &venue, ACCT, &mut mint(), MonoNs(0))
        .unwrap();
    assert_eq!(kinds(&plan), vec![("batch", 3)]);
    assert_eq!(
        refusals(&plan),
        vec![
            (Side::Buy, 3, PlanRefusal::RateFloor { scope: 0 }),
            (Side::Buy, 4, PlanRefusal::RateFloor { scope: 0 }),
        ]
    );
}

#[test]
fn a_scope_that_does_not_count_a_command_does_not_withhold_it() {
    // A per-pair bucket of another market, and a bucket counting only queries, both empty:
    // neither counts this market's places.
    let mut reg = quoting(0);
    let caps = batching();
    let other_pair = RateScope {
        ops: TagSet::of(&[OpKind::Place]),
        inst: Some(OTHER),
        left: 0,
        floor: 5,
    };
    let queries = RateScope {
        ops: TagSet::of(&[OpKind::Query]),
        inst: None,
        left: 0,
        floor: 5,
    };
    // This market's own pair bucket, with room for one above its floor.
    let own_pair = RateScope {
        ops: TagSet::of(&[OpKind::Place]),
        inst: Some(INST),
        left: 6,
        floor: 5,
    };
    let venue = PlanVenue {
        rates: RateView::new(vec![other_pair, queries, own_pair]),
        ..declared(&caps)
    };
    let plan = planner()
        .plan(&bids(2), &mut reg, &venue, ACCT, &mut mint(), MonoNs(0))
        .unwrap();
    assert_eq!(kinds(&plan), vec![("place", 1)]);
    assert_eq!(
        refusals(&plan),
        vec![(Side::Buy, 1, PlanRefusal::RateFloor { scope: 2 })]
    );
}

#[test]
fn a_normal_amend_stops_at_the_floor() {
    let caps = OrderCaps {
        amend: Some(fbc_core::AmendCaps {
            refs: TagSet::of(&[RefKind::Venue]),
            price: true,
            qty: true,
            flags: false,
            when_partially_filled: true,
            reject_keeps_original: true,
            keeps_venue_id: true,
            ack: fbc_core::AmendAck::ReplacedEvent,
            qty_semantics: fbc_core::AmendQty::TotalIncludingFilled,
            keeps_priority: None,
        }),
        ..batching()
    };
    let mut reg = quoting(0);
    let mut p = planner();
    let mut m = mint();
    let open = p
        .plan(
            &bids(1),
            &mut reg,
            &declared(&caps),
            ACCT,
            &mut m,
            MonoNs(0),
        )
        .unwrap();
    ack_all(&mut reg, &open);
    let venue = PlanVenue {
        rates: RateView::new(vec![per_ip(1_500 - 5, 5)]),
        ..declared(&caps)
    };
    let moved = DesiredBook::new(INST).with(Side::Buy, 0, quote(9_900, 1, 0));
    let plan = p
        .plan(&moved, &mut reg, &venue, ACCT, &mut m, MonoNs(2))
        .unwrap();
    assert!(plan.commands.is_empty(), "{:?}", kinds(&plan));
    assert_eq!(
        refusals(&plan),
        vec![(Side::Buy, 0, PlanRefusal::RateFloor { scope: 0 })]
    );
    // The amend was never built: nothing counts it, and the order rests as it was.
    let c = open.commands[0].cid;
    assert_eq!(reg.get(c).unwrap().px(), Some(Ticks(10_000)));
    // With room, it is built.
    let venue = PlanVenue {
        rates: RateView::new(vec![per_ip(0, 5)]),
        ..declared(&caps)
    };
    let plan = p
        .plan(&moved, &mut reg, &venue, ACCT, &mut m, MonoNs(3))
        .unwrap();
    assert_eq!(kinds(&plan), vec![("amend", 1)]);
}

#[test]
fn a_rate_scope_reads_its_room_from_its_limit_and_the_consumers_floor() {
    let scope = per_ip(1_400, 150);
    assert_eq!(scope.left, 100);
    assert_eq!(scope.floor, 150);
    assert_eq!(scope.inst, None);
    assert!(scope.ops.contains(OpKind::Place));
    // Used past the limit's units: none left.
    assert_eq!(per_ip(9_999, 1).left, 0);
    let view = RateView::new(vec![scope]);
    assert_eq!(view.scopes(), &[scope]);
}

#[test]
fn a_batch_carries_its_places_guard_to_submit() {
    // A batch is checked at submit as each of its places was: once the market's state changed
    // (the kill switch), it is stale and is never sent.
    let caps = batching();
    let mut reg = quoting(0);
    let plan = planner()
        .plan(
            &bids(3),
            &mut reg,
            &declared(&caps),
            ACCT,
            &mut mint(),
            MonoNs(0),
        )
        .unwrap();
    assert_eq!(kinds(&plan), vec![("batch", 3)]);
    let batch = &plan.commands[0].auth;
    assert!(batch.check_at_submit().is_ok());
    reg.kill(INST);
    assert!(batch.check_at_submit().is_err());
}

#[test]
fn a_cancel_left_alone_in_its_batch_goes_as_a_single_cancel_where_one_can_name_it() {
    // Three acknowledged bids pulled, cancel-manys of two: a cancel-many of two, then the
    // third alone as a single cancel.
    let caps = batches_of(2);
    let venue = declared(&caps);
    let mut reg = quoting(0);
    let mut p = planner();
    let mut m = mint();
    let open = p
        .plan(&bids(3), &mut reg, &venue, ACCT, &mut m, MonoNs(0))
        .unwrap();
    ack_all(&mut reg, &open);
    let plan = p
        .plan(
            &DesiredBook::new(INST),
            &mut reg,
            &venue,
            ACCT,
            &mut m,
            MonoNs(2),
        )
        .unwrap();
    assert_eq!(kinds(&plan), vec![("cancel-many", 2), ("cancel", 1)]);

    // A venue whose single cancel names only a placement nonce, which these orders lack: the
    // one left goes as a cancel-many of one, which names its venue id.
    let caps = OrderCaps {
        cancel_refs: TagSet::of(&[RefKind::PlacementNonce]),
        ..batches_of(2)
    };
    let venue = declared(&caps);
    let mut reg = quoting(0);
    let mut p = planner();
    let mut m = mint();
    let open = p
        .plan(&bids(1), &mut reg, &venue, ACCT, &mut m, MonoNs(0))
        .unwrap();
    ack_all(&mut reg, &open);
    let plan = p
        .plan(
            &DesiredBook::new(INST),
            &mut reg,
            &venue,
            ACCT,
            &mut m,
            MonoNs(2),
        )
        .unwrap();
    assert_eq!(kinds(&plan), vec![("cancel-many", 1)]);
}

#[test]
fn a_cancel_a_quote_change_calls_for_goes_out_though_a_venue_mode_refuses_the_new_quote() {
    // Long 10, an ordinary ask of 15 resting. The book now wants it as an emulated reduce-only
    // quote (the venue refuses post-only with reduce-only), so it counts as reducing and rests
    // more than is left to reduce: it is cancelled at once (0106), a cancel no venue mode
    // gates (0012, 0111), though the market's Reduce-only mode refuses the quote that replaces
    // it, which is never built (Codex r4319820327 on PR #182).
    let caps = OrderCaps {
        flag_conflicts: vec![(fbc_core::Feature::PostOnly, fbc_core::Feature::ReduceOnly)],
        ..batching()
    };
    let mut reg = quoting(10);
    let mut p = planner();
    let mut m = mint();
    let open = p
        .plan(
            &DesiredBook::new(INST).with(Side::Sell, 0, quote(10_010, 15, 0)),
            &mut reg,
            &declared(&caps),
            ACCT,
            &mut m,
            MonoNs(0),
        )
        .unwrap();
    ack_all(&mut reg, &open);
    let emulated = DesiredQuote {
        reduce_only: true,
        reducing: true,
        reduce_fallback: ReduceFallback::EmulateReduceOnly,
        ..quote(10_010, 15, 0)
    };
    let venue = PlanVenue {
        market_mode: VenueMode::ReduceOnly,
        ..declared(&caps)
    };
    let book = DesiredBook::new(INST).with(Side::Sell, 0, emulated);
    let plan = p
        .plan(&book, &mut reg, &venue, ACCT, &mut m, MonoNs(2))
        .unwrap();
    assert_eq!(kinds(&plan), vec![("cancel", 1)]);
    assert_eq!(plan.commands[0].cid, open.commands[0].cid);
    assert!(plan.refused.is_empty(), "{:?}", plan.refused);
    // Once it is terminal, the emulated quote (no venue reduce-only flag) is refused by the
    // mode and nothing is placed.
    let c = open.commands[0].cid;
    let ended = fbc_core::OrderUpdate {
        cid: Some(fbc_core::CidMatch::Ours(c)),
        vid: Some(vid("v-Sell-0")),
        inst: INST,
        side: Side::Sell,
        state: fbc_core::VenueOrderState::Canceled(fbc_core::CancelReason::Requested),
        cum_filled: lots(0),
        px: None,
        qty: None,
        post_only: None,
        reduce_only: None,
    };
    reg.apply_update(&ended, key(9));
    let plan = p
        .plan(&book, &mut reg, &venue, ACCT, &mut m, MonoNs(3))
        .unwrap();
    assert!(plan.commands.is_empty(), "{:?}", kinds(&plan));
    assert_eq!(
        refusals(&plan),
        vec![(Side::Sell, 0, PlanRefusal::VenueMode(VenueMode::ReduceOnly))]
    );
}
