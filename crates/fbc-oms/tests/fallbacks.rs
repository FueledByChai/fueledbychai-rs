//! The planner's generic fallbacks (FBC-01w; design §4.10 steps 3 and 4; decisions 0003, 0005,
//! 0013): a quote the venue's `OrderCaps` do not take as asked is changed by a rule chosen from
//! the caps alone, never from the venue's name, and each change is reported on the command
//! built from it ([`Planned::downgrades`]):
//!
//! - RPI asked for on a venue without the RPI channel goes on the public book;
//! - (Rpi, ReduceOnly) refused together: the public book, still reduce-only;
//! - (Ioc, ReduceOnly) refused together: a good-till-cancelled reduce-only limit at the quote's
//!   price (its cap), then its cancel at once;
//! - (PostOnly, ReduceOnly) refused together: the quote's fallback, either post-only without
//!   the flag, classified reducing and sized at most the position less the other orders
//!   resting that reduce it on its side, re-checked after every fill and cancelled on
//!   overshoot (emulated reduce-only), or reduce-only without post-only at the quote's price.
//!
//! The values are the owner's first test values: a $50 inventory cap on a synthetic market
//! where one lot is worth $1, so 50 lots, and a wide resting cap (no test here judges it).

#[path = "common/arm.rs"]
mod arm;
mod common;
#[path = "common/venue.rs"]
mod venue;

use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use arm::start;
use common::{fill, fill_id, lease_dir, lots, no_fee, order_caps, venue_fill, vid};
use fbc_core::{
    AccountKey, AckLevel, AmendAck, AmendCaps, AmendQty, Bps, CancelReason, Channel, CidMatch,
    CidMint, ClientOrderId, ExchNs, ExchTsKind, Feature, FillIdent, InstrumentId, ItemRef, MonoNs,
    Namespace, NamespaceLease, NewOrder, OrderCaps, OrderKind, OrderUpdate, PxExact, RefKind,
    RpcId, Side, SignedLots, SubmitOutcome, TagSet, Ticks, Tif, VenueCommand, VenueFillKind,
    VenueOrderSnapshot, VenueOrderState, WallNs,
};
use fbc_oms::{
    Admission, AmendRefusal, CapRefusal, DesiredBook, DesiredQuote, Downgrade, ExecutionPlanner,
    FillLedger, FillRouted, FillTime, LadderConfig, Leases, LedgerConfig, MarketCapsConfig,
    OmsError, OrderKey, OrderOp, Plan, PlanError, PlanRefusal, Planned, PlannerConfig,
    PreTradeCaps, ReduceFallback, Registry, ResyncSnapshot, Stage,
};

const INST: InstrumentId = InstrumentId::new(1);
const ACCT: AccountKey = AccountKey::new(1);
/// The owner's first inventory cap, in lots of $1.
const CAP: i64 = 50;
/// A resting cap past any order of these tests.
const WIDE: i64 = 1_000_000;
const MIN_AGE: Duration = Duration::from_millis(100);
/// An instant past the minimum age of anything placed at `MonoNs(0)`.
const LATER: MonoNs = MonoNs(1_000_000_000);
const LATER2: MonoNs = MonoNs(2_000_000_000);
const LATER3: MonoNs = MonoNs(3_000_000_000);

fn mint() -> CidMint {
    static NEXT: AtomicU16 = AtomicU16::new(500);
    let ns = Namespace::new(NEXT.fetch_add(1, Ordering::Relaxed));
    let lease = NamespaceLease::acquire(&lease_dir(), AccountKey::new(2), ns).unwrap();
    CidMint::new(lease, 0, 0, WallNs(0))
}

fn planner() -> ExecutionPlanner {
    ExecutionPlanner::new(PlannerConfig::new(Bps(2.0), lots(2), MIN_AGE).unwrap())
}

/// A venue taking GTC and IOC limits on the public book and the RPI channel, post-only and
/// reduce-only, that refuses each of `conflicts` together; no amend, cancels by venue id once
/// acknowledged.
fn conflicting(conflicts: &[(Feature, Feature)]) -> OrderCaps {
    OrderCaps {
        tifs: TagSet::of(&[Tif::Gtc, Tif::Ioc]),
        channels: TagSet::of(&[Channel::Public, Channel::Rpi]),
        flag_conflicts: conflicts.to_vec(),
        ..order_caps()
    }
}

/// `caps` with an amend of price and total by venue id, partly filled orders included.
fn amending(caps: OrderCaps) -> OrderCaps {
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
        ..caps
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

fn key(n: u64) -> OrderKey {
    OrderKey {
        venue: None,
        ingest: n,
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

/// A post-only, reduce-only maker exit of `qty` at `px` with the fallback `fallback`.
fn maker_exit(px: i64, qty: i64, fallback: ReduceFallback) -> DesiredQuote {
    DesiredQuote {
        post_only: true,
        reduce_fallback: fallback,
        ..reduce_only(limit(px, qty))
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

/// Each command's stage, kind, side and level, in the order planned.
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
            (p.stage, kind, p.side, p.level)
        })
        .collect()
}

/// Plans `desired` and reports every cancel and amend sent, as a gateway would.
fn plan_and_send(
    planner: &mut ExecutionPlanner,
    desired: &DesiredBook,
    reg: &mut Registry,
    caps: &OrderCaps,
    mint: &mut CidMint,
    now: MonoNs,
) -> Plan {
    let plan = planner
        .plan(desired, reg, &venue::of(caps), ACCT, mint, now)
        .unwrap();
    send(reg, &plan.commands, now);
    plan
}

/// Reports each of `commands` sent: each cancel and amend under a request of its own.
fn send(reg: &mut Registry, commands: &[Planned], now: MonoNs) {
    for (n, p) in commands.iter().enumerate() {
        let rpc = RpcId(1_000 + now.0 + n as u64);
        match p.auth.command() {
            VenueCommand::Cancel(_) => assert!(reg.cancel_sent(p.cid, rpc, now).unwrap()),
            VenueCommand::Amend(a) => {
                assert!(reg.amend_sent(p.cid, a.px, a.qty, rpc, now).unwrap())
            }
            _ => {}
        }
    }
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

/// Ends our order `c` on `side` (venue id `v`) cancelled, under the ordering key `n`.
fn cancelled(reg: &mut Registry, c: ClientOrderId, side: Side, v: &str, n: u64) {
    let u = OrderUpdate {
        cid: Some(CidMatch::Ours(c)),
        vid: Some(vid(v)),
        inst: INST,
        side,
        state: VenueOrderState::Canceled(CancelReason::Requested),
        cum_filled: reg.get(c).unwrap().filled(),
        px: None,
        qty: None,
        post_only: None,
        reduce_only: None,
    };
    reg.apply_update(&u, key(n));
    assert!(reg.get(c).unwrap().state().is_terminal());
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

/// The levels whose reducer waits for an emulated order's cancel this pass.
fn waiting(plan: &Plan) -> Vec<(Side, u16)> {
    plan.refused
        .iter()
        .filter(|r| r.why == PlanRefusal::AwaitsEmulatedCancel)
        .map(|r| (r.side, r.level))
        .collect()
}

/// The emulated reduce-only order's shape: post-only, without the venue flag, classified
/// reducing.
fn assert_emulated(o: &NewOrder, qty: i64) {
    assert!(o.post_only && !o.reduce_only && o.reducing, "{o:?}");
    assert_eq!(o.qty, lots(qty));
}

#[test]
fn an_rpi_quote_on_a_venue_without_rpi_goes_on_the_public_book_and_says_so() {
    let mut reg = quoting(0);
    let mut mint = mint();
    let mut p = planner();
    let caps = order_caps();
    assert!(!caps.channels.contains(Channel::Rpi));
    let rpi = DesiredQuote {
        channel: Channel::Rpi,
        post_only: true,
        ..limit(10_000, 3)
    };
    let desired = book().with(Side::Buy, 0, rpi);
    let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(0));
    assert_eq!(shape(&plan), vec![(Stage::Add, "place", Side::Buy, 0)]);
    let o = placed(&plan.commands[0]);
    assert_eq!(o.channel, Channel::Public);
    assert!(o.post_only);
    assert_eq!(plan.commands[0].downgrades, vec![Downgrade::RpiUnsupported]);
    // The order rests as the venue took it: a later pass of the same quote keeps it.
    ack(&mut reg, plan.commands[0].cid, "v1");
    let again = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, LATER);
    assert!(again.commands.is_empty(), "{:?}", shape(&again));
    // An RPI order only rests: moved to the public book it stays post-only, so it never takes.
    let mut reg2 = quoting(0);
    let mut p2 = planner();
    let resting_only = book().with(
        Side::Buy,
        0,
        DesiredQuote {
            channel: Channel::Rpi,
            ..limit(10_000, 3)
        },
    );
    let plan = plan_and_send(
        &mut p2,
        &resting_only,
        &mut reg2,
        &caps,
        &mut mint,
        MonoNs(0),
    );
    let o = placed(&plan.commands[0]);
    assert!(o.channel == Channel::Public && o.post_only);
    assert_eq!(plan.commands[0].downgrades, vec![Downgrade::RpiUnsupported]);
    // The same quote on a venue with RPI is sent as asked, with no downgrade.
    let mut reg = quoting(0);
    let mut p = planner();
    let rpi_venue = conflicting(&[]);
    let plan = plan_and_send(&mut p, &desired, &mut reg, &rpi_venue, &mut mint, MonoNs(0));
    assert_eq!(placed(&plan.commands[0]).channel, Channel::Rpi);
    assert!(plan.commands[0].downgrades.is_empty());
}

#[test]
fn an_rpi_reduce_only_quote_where_the_venue_refuses_both_goes_public_still_reduce_only() {
    // Long 10: asks reduce.
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let caps = conflicting(&[(Feature::Rpi, Feature::ReduceOnly)]);
    let exit = DesiredQuote {
        channel: Channel::Rpi,
        post_only: true,
        ..reduce_only(limit(10_010, 4))
    };
    let desired = book().with(Side::Sell, 0, exit);
    let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(0));
    assert_eq!(
        shape(&plan),
        vec![(Stage::Reducing, "place", Side::Sell, 0)]
    );
    let o = placed(&plan.commands[0]);
    assert_eq!(o.channel, Channel::Public);
    assert!(o.reduce_only && o.post_only);
    assert_eq!(o.kind, OrderKind::Limit { px: Ticks(10_010) });
    assert_eq!(plan.commands[0].downgrades, vec![Downgrade::RpiReduceOnly]);
    ack(&mut reg, plan.commands[0].cid, "v1");
    let again = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, LATER);
    assert!(again.commands.is_empty(), "{:?}", shape(&again));
    // The pair listed the other way round is the same conflict.
    let mut reg = quoting(10);
    let mut p = planner();
    let flipped = conflicting(&[(Feature::ReduceOnly, Feature::Rpi)]);
    let plan = plan_and_send(&mut p, &desired, &mut reg, &flipped, &mut mint, MonoNs(0));
    assert_eq!(placed(&plan.commands[0]).channel, Channel::Public);
    assert_eq!(plan.commands[0].downgrades, vec![Downgrade::RpiReduceOnly]);
}

#[test]
fn several_downgrades_of_one_quote_are_reported_in_the_order_applied() {
    // A venue with neither RPI nor post-only with reduce-only: the RPI maker exit first goes
    // public, then follows its fallback.
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let caps = OrderCaps {
        flag_conflicts: vec![(Feature::PostOnly, Feature::ReduceOnly)],
        ..order_caps()
    };
    let exit = DesiredQuote {
        channel: Channel::Rpi,
        ..maker_exit(10_010, 4, ReduceFallback::KeepReduceOnly)
    };
    let plan = plan_and_send(
        &mut p,
        &book().with(Side::Sell, 0, exit),
        &mut reg,
        &caps,
        &mut mint,
        MonoNs(0),
    );
    let o = placed(&plan.commands[0]);
    assert_eq!(o.channel, Channel::Public);
    assert!(o.reduce_only && !o.post_only);
    assert_eq!(
        plan.commands[0].downgrades,
        vec![Downgrade::RpiUnsupported, Downgrade::DroppedPostOnly]
    );
}

#[test]
fn an_ioc_reduce_only_quote_where_the_venue_refuses_both_is_a_capped_limit_cancelled_at_once() {
    // A venue that takes a cancel by client id before the acknowledgement: the cancel follows
    // its place in the same pass.
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let caps = OrderCaps {
        cancel_refs: TagSet::of(&[RefKind::Venue, RefKind::Client]),
        cancel_before_ack: true,
        ..conflicting(&[(Feature::Ioc, Feature::ReduceOnly)])
    };
    let cross = DesiredQuote {
        tif: Tif::Ioc,
        ..reduce_only(limit(9_990, 4))
    };
    let desired = book().with(Side::Sell, 0, cross);
    let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(0));
    assert_eq!(
        shape(&plan),
        vec![
            (Stage::Reducing, "place", Side::Sell, 0),
            (Stage::Cancel, "cancel", Side::Sell, 0),
        ]
    );
    let o = placed(&plan.commands[0]);
    assert_eq!(o.tif, Tif::Gtc);
    assert!(o.reduce_only && o.reducing && !o.post_only);
    // Capped: a limit at the quote's price, never a market order.
    assert_eq!(o.kind, OrderKind::Limit { px: Ticks(9_990) });
    assert_eq!(o.qty, lots(4));
    assert_eq!(plan.commands[0].downgrades, vec![Downgrade::SynthIoc]);
    assert_eq!(plan.commands[1].cid, plan.commands[0].cid);
    assert!(plan.commands[1].downgrades.is_empty());
    assert!(plan.awaiting_ack.is_empty());
}

#[test]
fn a_synthetic_ioc_on_a_venue_that_cancels_after_the_ack_is_cancelled_once_acknowledged() {
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let caps = conflicting(&[(Feature::Ioc, Feature::ReduceOnly)]);
    assert!(!caps.cancel_before_ack);
    let cross = DesiredQuote {
        tif: Tif::Ioc,
        ..reduce_only(limit(9_990, 4))
    };
    let desired = book().with(Side::Sell, 0, cross);
    let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(0));
    assert_eq!(
        shape(&plan),
        vec![(Stage::Reducing, "place", Side::Sell, 0)]
    );
    let c = plan.commands[0].cid;
    assert_eq!(plan.commands[0].downgrades, vec![Downgrade::SynthIoc]);
    // Its cancel waits for the acknowledgement.
    assert_eq!(plan.awaiting_ack, vec![c]);
    assert!(reg.get(c).unwrap().cancel_awaits_ack());
    // Unacknowledged, a later pass places nothing more and still waits.
    let waiting = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, LATER);
    assert!(waiting.commands.is_empty(), "{:?}", shape(&waiting));
    assert_eq!(waiting.awaiting_ack, vec![c]);
    // Acknowledged, it is cancelled at the next pass though the level is still wanted.
    ack(&mut reg, c, "v1");
    let cancel = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, LATER);
    assert_eq!(
        shape(&cancel),
        vec![(Stage::Cancel, "cancel", Side::Sell, 0)]
    );
    assert_eq!(cancel.commands[0].cid, c);
    // In flight, nothing is sent again.
    let flight = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, LATER2);
    assert!(flight.commands.is_empty(), "{:?}", shape(&flight));
    // Once it is terminal, the level still wanted is crossed again, as an IOC would be.
    cancelled(&mut reg, c, Side::Sell, "v1", 2);
    let next = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, LATER3);
    assert_eq!(
        shape(&next),
        vec![(Stage::Reducing, "place", Side::Sell, 0)]
    );
    assert_ne!(next.commands[0].cid, c);
    assert_eq!(next.commands[0].downgrades, vec![Downgrade::SynthIoc]);
}

#[test]
fn a_synthetic_ioc_no_longer_wanted_is_still_cancelled_and_never_amended() {
    // A venue that amends: a synthetic IOC whose quote moves is cancelled, never amended to
    // rest at a new price.
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let caps = amending(conflicting(&[(Feature::Ioc, Feature::ReduceOnly)]));
    let cross = |px| DesiredQuote {
        tif: Tif::Ioc,
        ..reduce_only(limit(px, 4))
    };
    let plan = plan_and_send(
        &mut p,
        &book().with(Side::Sell, 0, cross(9_990)),
        &mut reg,
        &caps,
        &mut mint,
        MonoNs(0),
    );
    let c = plan.commands[0].cid;
    ack(&mut reg, c, "v1");
    let moved = plan_and_send(
        &mut p,
        &book().with(Side::Sell, 0, cross(9_980)),
        &mut reg,
        &caps,
        &mut mint,
        LATER,
    );
    assert_eq!(
        shape(&moved),
        vec![(Stage::Cancel, "cancel", Side::Sell, 0)]
    );
    assert_eq!(moved.commands[0].cid, c);
}

#[test]
fn a_reduce_only_ioc_on_a_venue_that_takes_both_is_sent_as_asked() {
    // Paradex's declared conflicts (decision 0054's caps): IOC with reduce-only and post-only
    // with reduce-only are both taken, so nothing changes; an IOC the venue refuses later
    // (decision 0093) ends the order like any refusal.
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let caps = conflicting(&[
        (Feature::PostOnly, Feature::Ioc),
        (Feature::Ioc, Feature::Rpi),
        (Feature::Rpi, Feature::ReduceOnly),
    ]);
    let cross = DesiredQuote {
        tif: Tif::Ioc,
        ..reduce_only(limit(9_990, 4))
    };
    let exit = maker_exit(10_010, 3, ReduceFallback::EmulateReduceOnly);
    let desired = book().with(Side::Sell, 0, cross).with(Side::Sell, 1, exit);
    let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(0));
    assert_eq!(
        shape(&plan),
        vec![
            (Stage::Reducing, "place", Side::Sell, 0),
            (Stage::Reducing, "place", Side::Sell, 1),
        ]
    );
    let ioc = placed(&plan.commands[0]);
    assert!(ioc.tif == Tif::Ioc && ioc.reduce_only);
    let maker = placed(&plan.commands[1]);
    assert!(maker.post_only && maker.reduce_only);
    assert!(plan.commands.iter().all(|c| c.downgrades.is_empty()));
    assert!(plan.awaiting_ack.is_empty());
}

#[test]
fn a_post_only_reduce_only_quote_kept_reduce_only_drops_post_only_at_its_price() {
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let caps = conflicting(&[(Feature::PostOnly, Feature::ReduceOnly)]);
    let desired = book().with(
        Side::Sell,
        0,
        maker_exit(10_010, 4, ReduceFallback::KeepReduceOnly),
    );
    let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(0));
    assert_eq!(
        shape(&plan),
        vec![(Stage::Reducing, "place", Side::Sell, 0)]
    );
    let o = placed(&plan.commands[0]);
    assert!(o.reduce_only && !o.post_only && o.tif == Tif::Gtc);
    assert_eq!(o.kind, OrderKind::Limit { px: Ticks(10_010) });
    assert_eq!(o.qty, lots(4));
    assert_eq!(
        plan.commands[0].downgrades,
        vec![Downgrade::DroppedPostOnly]
    );
    ack(&mut reg, plan.commands[0].cid, "v1");
    let again = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, LATER);
    assert!(again.commands.is_empty(), "{:?}", shape(&again));
}

#[test]
fn an_emulated_reduce_only_quote_is_post_only_without_the_flag_sized_to_what_is_left_to_reduce() {
    // Long 10: a reduce-only ask of 3 at level 0, and at level 1 an emulated maker exit
    // wanting 20, which gets the 7 left.
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let caps = conflicting(&[(Feature::PostOnly, Feature::ReduceOnly)]);
    let desired = book()
        .with(Side::Sell, 0, reduce_only(limit(10_010, 3)))
        .with(
            Side::Sell,
            1,
            maker_exit(10_020, 20, ReduceFallback::EmulateReduceOnly),
        );
    let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(0));
    assert_eq!(
        shape(&plan),
        vec![
            (Stage::Reducing, "place", Side::Sell, 0),
            (Stage::Reducing, "place", Side::Sell, 1),
        ]
    );
    assert!(plan.commands[0].downgrades.is_empty());
    assert_emulated(placed(&plan.commands[1]), 7);
    assert_eq!(
        plan.commands[1].downgrades,
        vec![Downgrade::EmulatedReduceOnly]
    );
    // Resting at the most it may, it is kept as it is by a later pass.
    ack(&mut reg, plan.commands[0].cid, "v0");
    ack(&mut reg, plan.commands[1].cid, "v1");
    let again = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, LATER);
    assert!(again.commands.is_empty(), "{:?}", shape(&again));
    assert!(again.refused.is_empty());
}

#[test]
fn two_emulated_levels_share_what_is_left_to_reduce() {
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let caps = conflicting(&[(Feature::PostOnly, Feature::ReduceOnly)]);
    let desired = book()
        .with(
            Side::Sell,
            0,
            maker_exit(10_010, 6, ReduceFallback::EmulateReduceOnly),
        )
        .with(
            Side::Sell,
            1,
            maker_exit(10_020, 6, ReduceFallback::EmulateReduceOnly),
        );
    let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(0));
    assert_emulated(placed(&plan.commands[0]), 6);
    assert_emulated(placed(&plan.commands[1]), 4);
}

#[test]
fn an_emulated_quote_with_nothing_to_reduce_is_refused() {
    let caps = conflicting(&[(Feature::PostOnly, Feature::ReduceOnly)]);
    let bid = maker_exit(9_990, 3, ReduceFallback::EmulateReduceOnly);
    let ask = maker_exit(10_010, 3, ReduceFallback::EmulateReduceOnly);
    // Flat: neither side reduces. Long: the bid adds.
    for (pos, desired, refused) in [
        (
            0,
            book()
                .with(Side::Buy, 0, bid.clone())
                .with(Side::Sell, 0, ask.clone()),
            vec![Side::Buy, Side::Sell],
        ),
        (5, book().with(Side::Buy, 0, bid.clone()), vec![Side::Buy]),
    ] {
        let mut reg = quoting(pos);
        let mut mint = mint();
        let mut p = planner();
        let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(0));
        assert!(plan.commands.is_empty(), "{:?}", shape(&plan));
        let why: Vec<(Side, PlanRefusal)> = plan
            .refused
            .iter()
            .map(|r| (r.side, r.why.clone()))
            .collect();
        let want: Vec<(Side, PlanRefusal)> = refused
            .into_iter()
            .map(|s| (s, PlanRefusal::NoRoomToReduce))
            .collect();
        assert_eq!(why, want);
    }
    // Long 5 with a reduce-only ask of 5 resting: nothing is left for the emulated one.
    let mut reg = quoting(5);
    let mut mint = mint();
    let mut p = planner();
    let full = book()
        .with(Side::Sell, 0, reduce_only(limit(10_010, 5)))
        .with(Side::Sell, 1, ask);
    let plan = plan_and_send(&mut p, &full, &mut reg, &caps, &mut mint, MonoNs(0));
    assert_eq!(
        shape(&plan),
        vec![(Stage::Reducing, "place", Side::Sell, 0)]
    );
    assert_eq!(plan.refused.len(), 1);
    assert_eq!(plan.refused[0].level, 1);
    assert_eq!(plan.refused[0].why, PlanRefusal::NoRoomToReduce);
}

#[test]
fn an_emulated_order_is_cancelled_on_the_fill_that_makes_it_overshoot_then_replaced_smaller() {
    // Long 10: an emulated exit of 10 at level 0 and a plain ask of 4 at level 1.
    let mut reg = quoting(10);
    let mut l = ledger();
    let mut mint = mint();
    let mut p = planner();
    let caps = conflicting(&[(Feature::PostOnly, Feature::ReduceOnly)]);
    let desired = book()
        .with(
            Side::Sell,
            0,
            maker_exit(10_010, 10, ReduceFallback::EmulateReduceOnly),
        )
        .with(Side::Sell, 1, limit(10_020, 4));
    let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(0));
    let (emulated, plain) = (plan.commands[0].cid, plan.commands[1].cid);
    assert_emulated(placed(&plan.commands[0]), 10);
    ack(&mut reg, emulated, "v0");
    ack(&mut reg, plain, "v1");
    // Nothing overshoots before the fill.
    assert!(
        p.on_fill(&mut reg, &caps, ACCT, INST)
            .unwrap()
            .commands
            .is_empty()
    );
    // The plain ask fills 4: long 6, and the emulated exit of 10 would cross zero.
    fill_of(&mut reg, &mut l, plain, Side::Sell, 4, "f1");
    assert_eq!(reg.position(INST), Some(SignedLots(6)));
    let cancels = p.on_fill(&mut reg, &caps, ACCT, INST).unwrap().commands;
    assert_eq!(cancels.len(), 1);
    assert_eq!(cancels[0].cid, emulated);
    assert_eq!(cancels[0].stage, Stage::Cancel);
    assert!(matches!(cancels[0].auth.command(), VenueCommand::Cancel(_)));
    send(&mut reg, &cancels, LATER);
    // In flight, the level waits for the order's end.
    let waiting = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, LATER);
    assert!(
        waiting.commands.iter().all(|c| c.cid != emulated),
        "{:?}",
        shape(&waiting)
    );
    assert!(
        p.on_fill(&mut reg, &caps, ACCT, INST)
            .unwrap()
            .commands
            .is_empty()
    );
    // Cancelled, the level is placed again at the 6 left.
    cancelled(&mut reg, emulated, Side::Sell, "v0", 2);
    let resized = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, LATER2);
    let again: Vec<&Planned> = resized.commands.iter().filter(|c| c.level == 0).collect();
    assert_eq!(again.len(), 1);
    assert_emulated(placed(again[0]), 6);
    assert_eq!(again[0].downgrades, vec![Downgrade::EmulatedReduceOnly]);
}

/// A venue-initiated fill (decision 0120) is a fill like any other for the check after a fill:
/// a liquidation that shrinks the position leaves an emulated reduce-only order resting more
/// than is left to reduce, and the check cancels it; the order counted none of it.
#[test]
fn after_a_liquidation_the_check_cancels_an_emulated_order_it_left_overshooting() {
    // Long 10: an emulated exit of 10 at level 0.
    let mut reg = quoting(10);
    let mut l = ledger();
    let mut mint = mint();
    let mut p = planner();
    let caps = conflicting(&[(Feature::PostOnly, Feature::ReduceOnly)]);
    let desired = book().with(
        Side::Sell,
        0,
        maker_exit(10_010, 10, ReduceFallback::EmulateReduceOnly),
    );
    let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(0));
    let emulated = plan.commands[0].cid;
    assert_emulated(placed(&plan.commands[0]), 10);
    ack(&mut reg, emulated, "v0");
    assert!(
        p.on_fill(&mut reg, &caps, ACCT, INST)
            .unwrap()
            .commands
            .is_empty()
    );
    // The venue liquidates 4 of the long, after the seed: 6 left, and the emulated exit of 10
    // would take the position past zero.
    let liq = venue_fill(
        VenueFillKind::Liquidation,
        "LIQUIDATION:1",
        None,
        Side::Sell,
        4,
        PxExact::new(10_010, 0),
        no_fee(),
    );
    let time = Some(FillTime {
        exch: ExchNs(1_500),
        kind: ExchTsKind::MatchingEngine,
        aligned: WallNs(1_500),
    });
    match l.admit(&liq, time, MonoNs(1_100)) {
        Admission::Apply(a) => assert_eq!(
            reg.apply_fill(a).unwrap(),
            FillRouted::Venue {
                kind: VenueFillKind::Liquidation,
                in_snapshot: false,
            }
        ),
        other => panic!("expected the fill accepted, got {other:?}"),
    }
    assert_eq!(reg.position(INST), Some(SignedLots(6)));
    let cancels = p.on_fill(&mut reg, &caps, ACCT, INST).unwrap().commands;
    assert_eq!(cancels.len(), 1);
    assert_eq!(cancels[0].cid, emulated);
    assert_eq!(cancels[0].stage, Stage::Cancel);
    assert!(matches!(cancels[0].auth.command(), VenueCommand::Cancel(_)));
    assert_eq!(reg.get(emulated).unwrap().cum_fills(), lots(0));
}

#[test]
fn a_pass_cancels_an_emulated_order_a_fill_left_overshooting_before_anything_else() {
    // The same overshoot, found by the next pass when no check ran after the fill: its cancel
    // comes first.
    let mut reg = quoting(10);
    let mut l = ledger();
    let mut mint = mint();
    let mut p = planner();
    let caps = conflicting(&[(Feature::PostOnly, Feature::ReduceOnly)]);
    let desired = book()
        .with(
            Side::Sell,
            0,
            maker_exit(10_010, 10, ReduceFallback::EmulateReduceOnly),
        )
        .with(Side::Sell, 1, limit(10_020, 4));
    let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(0));
    let (emulated, plain) = (plan.commands[0].cid, plan.commands[1].cid);
    ack(&mut reg, emulated, "v0");
    ack(&mut reg, plain, "v1");
    fill_of(&mut reg, &mut l, plain, Side::Sell, 4, "f1");
    // Even younger than the minimum age.
    let pass = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(1));
    assert_eq!(pass.commands[0].stage, Stage::Cancel);
    assert_eq!(pass.commands[0].cid, emulated);
    assert!(pass.commands[0].removal.is_none());
}

#[test]
fn an_emulated_order_s_own_fills_never_overshoot() {
    let mut reg = quoting(10);
    let mut l = ledger();
    let mut mint = mint();
    let mut p = planner();
    let caps = conflicting(&[(Feature::PostOnly, Feature::ReduceOnly)]);
    let desired = book().with(
        Side::Sell,
        0,
        maker_exit(10_010, 10, ReduceFallback::EmulateReduceOnly),
    );
    let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(0));
    let emulated = plan.commands[0].cid;
    ack(&mut reg, emulated, "v0");
    // It fills 4 of its 10: long 6 and 6 resting, which is what is left.
    fill_of(&mut reg, &mut l, emulated, Side::Sell, 4, "f1");
    assert_eq!(reg.position(INST), Some(SignedLots(6)));
    assert!(
        p.on_fill(&mut reg, &caps, ACCT, INST)
            .unwrap()
            .commands
            .is_empty()
    );
    let pass = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, LATER);
    assert!(pass.commands.is_empty(), "{:?}", shape(&pass));
}

#[test]
fn a_reducer_that_would_leave_an_emulated_order_overshooting_waits_for_its_cancel() {
    // Codex r4309822832: long 10, an emulated exit of 8 resting at level 1; a reduce-only ask
    // of 5 comes at level 0. Placed now, both could fill and take the position to -3, the
    // emulated order having no venue flag to stop it: the pass cancels the emulated order and
    // builds the reducer only once that order is terminal, then the emulated level again at
    // what is left.
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let caps = conflicting(&[(Feature::PostOnly, Feature::ReduceOnly)]);
    let exit = maker_exit(10_020, 8, ReduceFallback::EmulateReduceOnly);
    let first = book().with(Side::Sell, 1, exit.clone());
    let plan = plan_and_send(&mut p, &first, &mut reg, &caps, &mut mint, MonoNs(0));
    let emulated = plan.commands[0].cid;
    ack(&mut reg, emulated, "v1");
    let next = first.with(Side::Sell, 0, reduce_only(limit(10_010, 5)));
    let pass = plan_and_send(&mut p, &next, &mut reg, &caps, &mut mint, LATER);
    assert_eq!(shape(&pass), vec![(Stage::Cancel, "cancel", Side::Sell, 1)]);
    assert_eq!(pass.commands[0].cid, emulated);
    assert_eq!(waiting(&pass), vec![(Side::Sell, 0)]);
    // The cancel in flight still counts: the reducer waits.
    let again = plan_and_send(&mut p, &next, &mut reg, &caps, &mut mint, LATER2);
    assert!(again.commands.is_empty(), "{:?}", shape(&again));
    assert_eq!(waiting(&again), vec![(Side::Sell, 0)]);
    cancelled(&mut reg, emulated, Side::Sell, "v1", 2);
    let after = plan_and_send(&mut p, &next, &mut reg, &caps, &mut mint, LATER3);
    assert_eq!(
        shape(&after),
        vec![
            (Stage::Reducing, "place", Side::Sell, 0),
            (Stage::Reducing, "place", Side::Sell, 1),
        ]
    );
    assert!(placed(&after.commands[0]).reduce_only);
    assert_emulated(placed(&after.commands[1]), 5);
}

#[test]
fn a_reducer_amend_that_would_leave_an_emulated_order_overshooting_waits_for_its_cancel() {
    // Long 10: a reduce-only ask of 2 at level 0 and an emulated exit of 8 at level 1 rest. The
    // ask grows to 10 by amend: amended now, both could fill. The pass cancels the emulated
    // order and amends the ask only once it is terminal.
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let caps = amending(conflicting(&[(Feature::PostOnly, Feature::ReduceOnly)]));
    let exit = maker_exit(10_020, 8, ReduceFallback::EmulateReduceOnly);
    let first = book()
        .with(Side::Sell, 0, reduce_only(limit(10_010, 2)))
        .with(Side::Sell, 1, exit.clone());
    let plan = plan_and_send(&mut p, &first, &mut reg, &caps, &mut mint, MonoNs(0));
    assert_eq!(
        shape(&plan),
        vec![
            (Stage::Reducing, "place", Side::Sell, 0),
            (Stage::Reducing, "place", Side::Sell, 1),
        ]
    );
    let (native, emulated) = (plan.commands[0].cid, plan.commands[1].cid);
    assert_emulated(placed(&plan.commands[1]), 8);
    ack(&mut reg, native, "v0");
    ack(&mut reg, emulated, "v1");
    let grown = book()
        .with(Side::Sell, 0, reduce_only(limit(10_010, 10)))
        .with(Side::Sell, 1, exit);
    let pass = plan_and_send(&mut p, &grown, &mut reg, &caps, &mut mint, LATER);
    assert_eq!(shape(&pass), vec![(Stage::Cancel, "cancel", Side::Sell, 1)]);
    assert_eq!(pass.commands[0].cid, emulated);
    assert_eq!(waiting(&pass), vec![(Side::Sell, 0)]);
    cancelled(&mut reg, emulated, Side::Sell, "v1", 2);
    let after = plan_and_send(&mut p, &grown, &mut reg, &caps, &mut mint, LATER2);
    assert_eq!(
        shape(&after),
        vec![(Stage::Reducing, "amend", Side::Sell, 0)]
    );
    assert_eq!(after.commands[0].cid, native);
    assert_eq!(
        after
            .refused
            .iter()
            .map(|r| (r.side, r.level, r.why.clone()))
            .collect::<Vec<_>>(),
        vec![(Side::Sell, 1, PlanRefusal::NoRoomToReduce)]
    );
}

#[test]
fn a_synthetic_ioc_that_would_leave_an_emulated_order_overshooting_waits_for_its_cancel() {
    // Long 10, an emulated exit of 8 resting; a reduce-only IOC of 5 the venue takes only as a
    // synthetic IOC: it waits for the emulated order's cancel like any reducer.
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let caps = conflicting(&[
        (Feature::PostOnly, Feature::ReduceOnly),
        (Feature::Ioc, Feature::ReduceOnly),
    ]);
    let first = book().with(
        Side::Sell,
        1,
        maker_exit(10_020, 8, ReduceFallback::EmulateReduceOnly),
    );
    let plan = plan_and_send(&mut p, &first, &mut reg, &caps, &mut mint, MonoNs(0));
    let emulated = plan.commands[0].cid;
    ack(&mut reg, emulated, "v1");
    let ioc = DesiredQuote {
        tif: Tif::Ioc,
        ..reduce_only(limit(9_990, 5))
    };
    let next = first.with(Side::Sell, 0, ioc);
    let pass = plan_and_send(&mut p, &next, &mut reg, &caps, &mut mint, LATER);
    assert_eq!(shape(&pass), vec![(Stage::Cancel, "cancel", Side::Sell, 1)]);
    assert_eq!(pass.commands[0].cid, emulated);
    assert_eq!(waiting(&pass), vec![(Side::Sell, 0)]);
}

#[test]
fn a_reducer_at_a_farther_level_is_counted_before_an_emulated_place_is_sized() {
    // Reviewer B (revb_t4): long 10, nothing resting; an emulated exit of 10 at level 0 and a
    // reduce-only ask of 10 at level 1. The reducer is built first, so the emulated exit has
    // nothing left and is refused: never 20 lots of reducing asks on a long of 10.
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let caps = conflicting(&[(Feature::PostOnly, Feature::ReduceOnly)]);
    let desired = book()
        .with(
            Side::Sell,
            0,
            maker_exit(10_010, 10, ReduceFallback::EmulateReduceOnly),
        )
        .with(Side::Sell, 1, reduce_only(limit(10_020, 10)));
    let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(0));
    assert_eq!(
        shape(&plan),
        vec![(Stage::Reducing, "place", Side::Sell, 1)]
    );
    assert!(placed(&plan.commands[0]).reduce_only);
    assert_eq!(plan.refused.len(), 1);
    assert_eq!(
        (plan.refused[0].level, plan.refused[0].why.clone()),
        (0, PlanRefusal::NoRoomToReduce)
    );
}

#[test]
fn a_resting_order_promoted_to_emulated_counts_against_a_second_emulated_level() {
    // Codex r4309822831: an unclassified post-only ask of 10 rests on a long of 10; its quote
    // becomes an emulated exit of the same wire shape, and a second emulated exit of 10 comes
    // at level 1. The promoted order now reduces the position, so level 1 has nothing left.
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let caps = conflicting(&[(Feature::PostOnly, Feature::ReduceOnly)]);
    let plain = DesiredQuote {
        post_only: true,
        ..limit(10_010, 10)
    };
    let plan = plan_and_send(
        &mut p,
        &book().with(Side::Sell, 0, plain),
        &mut reg,
        &caps,
        &mut mint,
        MonoNs(0),
    );
    let c = plan.commands[0].cid;
    ack(&mut reg, c, "v0");
    let next = book()
        .with(
            Side::Sell,
            0,
            maker_exit(10_010, 10, ReduceFallback::EmulateReduceOnly),
        )
        .with(
            Side::Sell,
            1,
            maker_exit(10_020, 10, ReduceFallback::EmulateReduceOnly),
        );
    let pass = plan_and_send(&mut p, &next, &mut reg, &caps, &mut mint, LATER);
    assert!(pass.commands.is_empty(), "{:?}", shape(&pass));
    assert!(reg.get(c).unwrap().reduces());
    assert_eq!(pass.refused.len(), 1);
    assert_eq!(
        (pass.refused[0].level, pass.refused[0].why.clone()),
        (1, PlanRefusal::NoRoomToReduce)
    );
}

#[test]
fn an_emulated_order_grows_by_amend_only_to_what_is_left_to_reduce() {
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let caps = amending(conflicting(&[(Feature::PostOnly, Feature::ReduceOnly)]));
    let plan = plan_and_send(
        &mut p,
        &book().with(
            Side::Sell,
            0,
            maker_exit(10_010, 4, ReduceFallback::EmulateReduceOnly),
        ),
        &mut reg,
        &caps,
        &mut mint,
        MonoNs(0),
    );
    let emulated = plan.commands[0].cid;
    ack(&mut reg, emulated, "v0");
    let grow = book().with(
        Side::Sell,
        0,
        maker_exit(10_010, 20, ReduceFallback::EmulateReduceOnly),
    );
    let pass = plan_and_send(&mut p, &grow, &mut reg, &caps, &mut mint, LATER);
    assert_eq!(
        shape(&pass),
        vec![(Stage::Reducing, "amend", Side::Sell, 0)]
    );
    match pass.commands[0].auth.command() {
        VenueCommand::Amend(a) => {
            assert_eq!(a.qty, lots(10));
            assert!(a.post_only && !a.reduce_only && a.reducing);
        }
        other => panic!("expected an amend, got {other:?}"),
    }
    assert_eq!(
        pass.commands[0].downgrades,
        vec![Downgrade::EmulatedReduceOnly]
    );
}

#[test]
fn the_check_after_a_fill_is_refused_through_another_registry_and_ignores_other_orders() {
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let caps = conflicting(&[(Feature::PostOnly, Feature::ReduceOnly)]);
    // No pass yet: nothing to check.
    assert!(
        p.on_fill(&mut reg, &caps, ACCT, INST)
            .unwrap()
            .commands
            .is_empty()
    );
    // A plain reduce-only ask and a kept-reduce-only exit are the venue's to bound, not the
    // check's: long 10 with 10 + 10 resting, nothing is cancelled.
    let desired = book()
        .with(Side::Sell, 0, reduce_only(limit(10_010, 10)))
        .with(
            Side::Sell,
            1,
            maker_exit(10_020, 10, ReduceFallback::KeepReduceOnly),
        );
    plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(0));
    assert!(
        p.on_fill(&mut reg, &caps, ACCT, INST)
            .unwrap()
            .commands
            .is_empty()
    );
    let mut other = quoting(10);
    assert_eq!(
        p.on_fill(&mut other, &caps, ACCT, INST).err(),
        Some(PlanError::OtherRegistry { acct: ACCT })
    );
}

#[test]
fn an_emulated_amend_a_reducer_would_leave_overshooting_is_not_built_and_its_order_is_cancelled() {
    // Long 10, an emulated exit of 3 resting asked to grow to 6, and a reduce-only ask of 10
    // coming at the other level: the emulated order is cancelled and its amend not built, and
    // the reducer waits for that cancel, whichever level is nearer (Reviewer B, revb_t3).
    for (native_at, emulated_at) in [(0, 1), (1, 0)] {
        let mut reg = quoting(10);
        let mut mint = mint();
        let mut p = planner();
        let caps = amending(conflicting(&[(Feature::PostOnly, Feature::ReduceOnly)]));
        let first = book().with(
            Side::Sell,
            emulated_at,
            maker_exit(10_020, 3, ReduceFallback::EmulateReduceOnly),
        );
        let plan = plan_and_send(&mut p, &first, &mut reg, &caps, &mut mint, MonoNs(0));
        let emulated = plan.commands[0].cid;
        ack(&mut reg, emulated, "v1");
        let next = book()
            .with(Side::Sell, native_at, reduce_only(limit(10_010, 10)))
            .with(
                Side::Sell,
                emulated_at,
                maker_exit(10_020, 6, ReduceFallback::EmulateReduceOnly),
            );
        let pass = plan_and_send(&mut p, &next, &mut reg, &caps, &mut mint, LATER);
        assert_eq!(
            shape(&pass),
            vec![(Stage::Cancel, "cancel", Side::Sell, emulated_at)]
        );
        assert_eq!(pass.commands[0].cid, emulated);
        assert_eq!(waiting(&pass), vec![(Side::Sell, native_at)]);
        cancelled(&mut reg, emulated, Side::Sell, "v1", 2);
        let after = plan_and_send(&mut p, &next, &mut reg, &caps, &mut mint, LATER2);
        assert_eq!(
            shape(&after),
            vec![(Stage::Reducing, "place", Side::Sell, native_at)]
        );
        assert_eq!(
            after
                .refused
                .iter()
                .map(|r| (r.level, r.why.clone()))
                .collect::<Vec<_>>(),
            vec![(emulated_at, PlanRefusal::NoRoomToReduce)]
        );
    }
}

#[test]
fn fills_the_venue_reported_before_their_fill_events_count_against_an_emulated_order() {
    // Long 10: an emulated exit of 10 and a plain ask of 4. The venue reports the plain ask
    // filled on an order update before the fill event arrives: the position is 6 in truth,
    // though the inventory still holds 10, so the emulated exit is cancelled.
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let caps = conflicting(&[(Feature::PostOnly, Feature::ReduceOnly)]);
    let desired = book()
        .with(
            Side::Sell,
            0,
            maker_exit(10_010, 10, ReduceFallback::EmulateReduceOnly),
        )
        .with(Side::Sell, 1, limit(10_020, 4));
    let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(0));
    let (emulated, plain) = (plan.commands[0].cid, plan.commands[1].cid);
    ack(&mut reg, emulated, "v0");
    ack(&mut reg, plain, "v1");
    let u = OrderUpdate {
        cid: Some(CidMatch::Ours(plain)),
        vid: Some(vid("v1")),
        inst: INST,
        side: Side::Sell,
        state: VenueOrderState::Filled,
        cum_filled: lots(4),
        px: None,
        qty: None,
        post_only: None,
        reduce_only: None,
    };
    reg.apply_update(&u, key(2));
    assert_eq!(reg.position(INST), Some(SignedLots(10)));
    // The filled ask's level is placed again; the emulated exit is cancelled first.
    let pass = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(1));
    assert_eq!(
        shape(&pass),
        vec![
            (Stage::Cancel, "cancel", Side::Sell, 0),
            (Stage::Add, "place", Side::Sell, 1),
        ]
    );
    assert_eq!(pass.commands[0].cid, emulated);
}

#[test]
fn a_resting_order_whose_quote_becomes_a_synthetic_ioc_of_the_same_shape_is_cancelled() {
    // Codex r4309743199: a GTC reduce-only ask rests; the quote becomes an IOC with reduce-only
    // at the same price, which the venue takes only as a synthetic IOC of the same wire shape.
    // The resting order is cancelled as an IOC's would be, never kept.
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let caps = conflicting(&[(Feature::Ioc, Feature::ReduceOnly)]);
    let resting = reduce_only(limit(9_990, 4));
    let plan = plan_and_send(
        &mut p,
        &book().with(Side::Sell, 0, resting.clone()),
        &mut reg,
        &caps,
        &mut mint,
        MonoNs(0),
    );
    let c = plan.commands[0].cid;
    assert!(plan.commands[0].downgrades.is_empty());
    ack(&mut reg, c, "v0");
    let ioc = book().with(
        Side::Sell,
        0,
        DesiredQuote {
            tif: Tif::Ioc,
            ..resting
        },
    );
    let pass = plan_and_send(&mut p, &ioc, &mut reg, &caps, &mut mint, MonoNs(1));
    assert_eq!(shape(&pass), vec![(Stage::Cancel, "cancel", Side::Sell, 0)]);
    assert_eq!(pass.commands[0].cid, c);
}

#[test]
fn a_resting_order_whose_quote_becomes_emulated_reduce_only_of_the_same_shape_is_protected() {
    // Codex r4309743199: a post-only ask classified reducing rests without the venue flag; the
    // quote becomes an emulated reduce-only exit of the same wire shape. The order is kept,
    // and from then on cancelled on overshoot.
    let mut reg = quoting(10);
    let mut l = ledger();
    let mut mint = mint();
    let mut p = planner();
    let caps = conflicting(&[(Feature::PostOnly, Feature::ReduceOnly)]);
    let classified = DesiredQuote {
        post_only: true,
        reducing: true,
        ..limit(10_010, 10)
    };
    let first = book()
        .with(Side::Sell, 0, classified)
        .with(Side::Sell, 1, limit(10_020, 4));
    let plan = plan_and_send(&mut p, &first, &mut reg, &caps, &mut mint, MonoNs(0));
    let (exit, plain) = (plan.commands[0].cid, plan.commands[1].cid);
    assert!(plan.commands[0].downgrades.is_empty());
    ack(&mut reg, exit, "v0");
    ack(&mut reg, plain, "v1");
    let emulated = book()
        .with(
            Side::Sell,
            0,
            maker_exit(10_010, 10, ReduceFallback::EmulateReduceOnly),
        )
        .with(Side::Sell, 1, limit(10_020, 4));
    let kept = plan_and_send(&mut p, &emulated, &mut reg, &caps, &mut mint, MonoNs(1));
    assert!(kept.commands.is_empty(), "{:?}", shape(&kept));
    // The plain ask fills 4: long 6, and the exit of 10 would cross zero.
    fill_of(&mut reg, &mut l, plain, Side::Sell, 4, "f1");
    let cancels = p.on_fill(&mut reg, &caps, ACCT, INST).unwrap().commands;
    assert_eq!(cancels.len(), 1);
    assert_eq!(cancels[0].cid, exit);
}

#[test]
fn an_order_amended_as_reducing_counts_against_an_emulated_order() {
    // Codex r4309743205: a post-only ask placed unclassified is amended with the reducing
    // classification; it then reduces the position like any reducer, so an emulated exit placed
    // after it gets only what is left.
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let caps = amending(conflicting(&[(Feature::PostOnly, Feature::ReduceOnly)]));
    let ask = |px, reducing| DesiredQuote {
        post_only: true,
        reducing,
        ..limit(px, 4)
    };
    let plan = plan_and_send(
        &mut p,
        &book().with(Side::Sell, 0, ask(10_010, false)),
        &mut reg,
        &caps,
        &mut mint,
        MonoNs(0),
    );
    let c = plan.commands[0].cid;
    ack(&mut reg, c, "v0");
    let next = book().with(Side::Sell, 0, ask(10_020, true)).with(
        Side::Sell,
        1,
        maker_exit(10_030, 20, ReduceFallback::EmulateReduceOnly),
    );
    let pass = plan_and_send(&mut p, &next, &mut reg, &caps, &mut mint, LATER);
    assert_eq!(
        shape(&pass),
        vec![
            (Stage::Reducing, "amend", Side::Sell, 0),
            (Stage::Reducing, "place", Side::Sell, 1),
        ]
    );
    assert_emulated(placed(&pass.commands[1]), 6);
}

#[test]
fn a_resting_order_whose_quote_becomes_reducing_counts_against_an_emulated_order() {
    // Codex r4309991135: long 10, an unclassified ask of 8 rests; its quote is now classified
    // reducing with nothing else changed, so no amend records it, and an emulated exit of 5
    // comes at level 1. The ask now reduces the position, so the exit gets only the 2 left.
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let caps = conflicting(&[(Feature::PostOnly, Feature::ReduceOnly)]);
    let ask = limit(10_010, 8);
    let plan = plan_and_send(
        &mut p,
        &book().with(Side::Sell, 0, ask.clone()),
        &mut reg,
        &caps,
        &mut mint,
        MonoNs(0),
    );
    let c = plan.commands[0].cid;
    ack(&mut reg, c, "v0");
    let next = book()
        .with(
            Side::Sell,
            0,
            DesiredQuote {
                reducing: true,
                ..ask
            },
        )
        .with(
            Side::Sell,
            1,
            maker_exit(10_020, 5, ReduceFallback::EmulateReduceOnly),
        );
    let pass = plan_and_send(&mut p, &next, &mut reg, &caps, &mut mint, LATER);
    assert_eq!(
        shape(&pass),
        vec![(Stage::Reducing, "place", Side::Sell, 1)]
    );
    assert!(reg.get(c).unwrap().reduces());
    assert_emulated(placed(&pass.commands[0]), 2);
}

#[test]
fn in_exit_an_emulated_order_whose_quote_becomes_ordinary_is_still_amended_as_a_reducer() {
    // Codex r4315460452: long 10 in Flatten, an emulated exit of 5 rests. Its quote becomes an
    // ordinary post-only ask of 5 at a new price: the order keeps its emulated mode, so it
    // still cannot take the position past zero, and its amend is a reducer's, which Exit
    // admits, never an ordinary amend Exit refuses.
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let caps = amending(conflicting(&[(Feature::PostOnly, Feature::ReduceOnly)]));
    let first = book().with(
        Side::Sell,
        0,
        maker_exit(10_010, 5, ReduceFallback::EmulateReduceOnly),
    );
    let plan = plan_and_send(&mut p, &first, &mut reg, &caps, &mut mint, MonoNs(0));
    let emulated = plan.commands[0].cid;
    assert_emulated(placed(&plan.commands[0]), 5);
    ack(&mut reg, emulated, "v0");
    Registry::flatten(&mut reg, INST, Leases::none()).unwrap();
    let next = book().with(Side::Sell, 0, post_only_ask(10_020, 5, false));
    let pass = plan_and_send(&mut p, &next, &mut reg, &caps, &mut mint, LATER);
    assert!(pass.refused.is_empty(), "{:?}", pass.refused);
    assert_eq!(
        shape(&pass),
        vec![(Stage::Reducing, "amend", Side::Sell, 0)]
    );
    assert_eq!(pass.commands[0].cid, emulated);
    match pass.commands[0].auth.command() {
        VenueCommand::Amend(a) => {
            assert_eq!((a.px, a.qty, a.reducing), (Ticks(10_020), lots(5), true));
        }
        other => panic!("expected an amend, got {other:?}"),
    }
}

/// A post-only ask of `qty` at `px` without the venue flag, classified reducing or not.
fn post_only_ask(px: i64, qty: i64, reducing: bool) -> DesiredQuote {
    DesiredQuote {
        post_only: true,
        reducing,
        ..limit(px, qty)
    }
}

/// The command `plan` built at `level` of `side`.
fn at_level(plan: &Plan, side: Side, level: u16) -> &Planned {
    plan.commands
        .iter()
        .find(|c| c.side == side && c.level == level)
        .unwrap_or_else(|| panic!("nothing built at {side:?} {level}: {:?}", shape(plan)))
}

/// The total an amend `p` carries.
fn amended_total(p: &Planned) -> fbc_core::Lots {
    match p.auth.command() {
        VenueCommand::Amend(a) => a.qty,
        other => panic!("expected an amend, got {other:?}"),
    }
}

#[test]
fn a_price_only_amend_that_makes_a_farther_order_a_reducer_waits_for_the_emulated_cancel() {
    // Codex r4310495940: long 10, an emulated exit of 6 at level 0 and an unclassified ask of 5
    // at level 1 rest. The ask's quote is now classified reducing at a new price, its size
    // unchanged: amended now, it would be authorized before the emulated order's cancel has
    // settled, and both could fill and take the position to -1. It waits like any new reducer.
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let caps = amending(conflicting(&[(Feature::PostOnly, Feature::ReduceOnly)]));
    let exit = maker_exit(10_010, 6, ReduceFallback::EmulateReduceOnly);
    let first = book().with(Side::Sell, 0, exit.clone()).with(
        Side::Sell,
        1,
        post_only_ask(10_030, 5, false),
    );
    let plan = plan_and_send(&mut p, &first, &mut reg, &caps, &mut mint, MonoNs(0));
    let emulated = at_level(&plan, Side::Sell, 0).cid;
    let ask = at_level(&plan, Side::Sell, 1).cid;
    assert_emulated(placed(at_level(&plan, Side::Sell, 0)), 6);
    ack(&mut reg, emulated, "v0");
    ack(&mut reg, ask, "v1");
    let next = book()
        .with(Side::Sell, 0, exit)
        .with(Side::Sell, 1, post_only_ask(10_040, 5, true));
    let pass = plan_and_send(&mut p, &next, &mut reg, &caps, &mut mint, LATER);
    assert_eq!(shape(&pass), vec![(Stage::Cancel, "cancel", Side::Sell, 0)]);
    assert_eq!(pass.commands[0].cid, emulated);
    assert_eq!(waiting(&pass), vec![(Side::Sell, 1)]);
    // The cancel in flight still counts: the reducer waits.
    let again = plan_and_send(&mut p, &next, &mut reg, &caps, &mut mint, LATER2);
    assert!(again.commands.is_empty(), "{:?}", shape(&again));
    assert_eq!(waiting(&again), vec![(Side::Sell, 1)]);
    cancelled(&mut reg, emulated, Side::Sell, "v0", 2);
    let after = plan_and_send(&mut p, &next, &mut reg, &caps, &mut mint, LATER3);
    assert_eq!(
        shape(&after),
        vec![
            (Stage::Reducing, "amend", Side::Sell, 1),
            (Stage::Reducing, "place", Side::Sell, 0),
        ]
    );
    assert_eq!(after.commands[0].cid, ask);
    assert_eq!(amended_total(&after.commands[0]), lots(5));
    assert_emulated(placed(&after.commands[1]), 5);
}

#[test]
fn a_price_only_amend_that_makes_a_nearer_order_a_reducer_waits_for_the_emulated_cancel() {
    // The same at the other order of levels: the unclassified ask of 5 rests at level 0, the
    // emulated exit of 6 at level 1. The exit's cancel is built first; the ask's amend still
    // waits until the exit is terminal, never authorized beside a cancel not yet settled.
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let caps = amending(conflicting(&[(Feature::PostOnly, Feature::ReduceOnly)]));
    let exit = maker_exit(10_030, 6, ReduceFallback::EmulateReduceOnly);
    let first = book()
        .with(Side::Sell, 0, post_only_ask(10_010, 5, false))
        .with(Side::Sell, 1, exit.clone());
    let plan = plan_and_send(&mut p, &first, &mut reg, &caps, &mut mint, MonoNs(0));
    let ask = at_level(&plan, Side::Sell, 0).cid;
    let emulated = at_level(&plan, Side::Sell, 1).cid;
    assert_emulated(placed(at_level(&plan, Side::Sell, 1)), 6);
    ack(&mut reg, ask, "v0");
    ack(&mut reg, emulated, "v1");
    let next = book()
        .with(Side::Sell, 0, post_only_ask(10_020, 5, true))
        .with(Side::Sell, 1, exit);
    let pass = plan_and_send(&mut p, &next, &mut reg, &caps, &mut mint, LATER);
    assert_eq!(shape(&pass), vec![(Stage::Cancel, "cancel", Side::Sell, 1)]);
    assert_eq!(pass.commands[0].cid, emulated);
    assert_eq!(waiting(&pass), vec![(Side::Sell, 0)]);
    cancelled(&mut reg, emulated, Side::Sell, "v1", 2);
    let after = plan_and_send(&mut p, &next, &mut reg, &caps, &mut mint, LATER2);
    assert_eq!(
        shape(&after),
        vec![
            (Stage::Reducing, "amend", Side::Sell, 0),
            (Stage::Reducing, "place", Side::Sell, 1),
        ]
    );
    assert_eq!(after.commands[0].cid, ask);
    assert_emulated(placed(&after.commands[1]), 5);
}

#[test]
fn a_reducer_classified_while_too_young_to_amend_still_waits_for_the_emulated_cancel() {
    // Long 10, the emulated exit of 6 at level 0 and the unclassified ask of 5 at level 1. The
    // ask's quote turns reducing at a new price while the ask is too young to amend: the pass
    // counts it and cancels the exit, which now overshoots. The next pass may amend the ask,
    // but the exit's cancel is still in flight: the ask, a reducer since the pass before but
    // never amended as one, waits all the same.
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let caps = amending(conflicting(&[(Feature::PostOnly, Feature::ReduceOnly)]));
    let exit = maker_exit(10_010, 6, ReduceFallback::EmulateReduceOnly);
    let first = book().with(Side::Sell, 0, exit.clone()).with(
        Side::Sell,
        1,
        post_only_ask(10_030, 5, false),
    );
    let plan = plan_and_send(&mut p, &first, &mut reg, &caps, &mut mint, MonoNs(0));
    let emulated = at_level(&plan, Side::Sell, 0).cid;
    let ask = at_level(&plan, Side::Sell, 1).cid;
    ack(&mut reg, emulated, "v0");
    ack(&mut reg, ask, "v1");
    let next = book()
        .with(Side::Sell, 0, exit)
        .with(Side::Sell, 1, post_only_ask(10_040, 5, true));
    let young = MonoNs(10_000_000);
    let pass = plan_and_send(&mut p, &next, &mut reg, &caps, &mut mint, young);
    assert_eq!(shape(&pass), vec![(Stage::Cancel, "cancel", Side::Sell, 0)]);
    assert_eq!(pass.commands[0].cid, emulated);
    let again = plan_and_send(&mut p, &next, &mut reg, &caps, &mut mint, LATER);
    assert!(again.commands.is_empty(), "{:?}", shape(&again));
    assert_eq!(waiting(&again), vec![(Side::Sell, 1)]);
    cancelled(&mut reg, emulated, Side::Sell, "v0", 2);
    let after = plan_and_send(&mut p, &next, &mut reg, &caps, &mut mint, LATER2);
    assert_eq!(
        shape(&after),
        vec![
            (Stage::Reducing, "amend", Side::Sell, 1),
            (Stage::Reducing, "place", Side::Sell, 0),
        ]
    );
    assert_eq!(after.commands[0].cid, ask);
    assert_emulated(placed(&after.commands[1]), 5);
}

#[test]
fn a_counted_reducer_grown_by_an_unclassified_quote_is_built_before_the_emulated_place() {
    // Reviewer B B13: long 10, an ask of 3 placed classified reducing at level 1 and an
    // emulated exit at level 0, sized to the 7 left. The ask's quote turns unclassified and
    // grows to 5: the ask is still counted a reducer, so its growth waits for the exit's cancel.
    // Once the exit is terminal, the ask's amend is built among the reducers, before the
    // emulated place, which is sized with it counted: never an emulated place sized against the
    // ask's old resting, then cancelled by the ask's growth, pass after pass.
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let caps = amending(conflicting(&[(Feature::PostOnly, Feature::ReduceOnly)]));
    let exit = maker_exit(10_010, 10, ReduceFallback::EmulateReduceOnly);
    let first = book().with(Side::Sell, 0, exit.clone()).with(
        Side::Sell,
        1,
        post_only_ask(10_030, 3, true),
    );
    let plan = plan_and_send(&mut p, &first, &mut reg, &caps, &mut mint, MonoNs(0));
    let emulated = at_level(&plan, Side::Sell, 0).cid;
    let ask = at_level(&plan, Side::Sell, 1).cid;
    assert_emulated(placed(at_level(&plan, Side::Sell, 0)), 7);
    ack(&mut reg, emulated, "v0");
    ack(&mut reg, ask, "v1");
    let next =
        book()
            .with(Side::Sell, 0, exit)
            .with(Side::Sell, 1, post_only_ask(10_030, 5, false));
    let pass = plan_and_send(&mut p, &next, &mut reg, &caps, &mut mint, LATER);
    assert_eq!(shape(&pass), vec![(Stage::Cancel, "cancel", Side::Sell, 0)]);
    assert_eq!(pass.commands[0].cid, emulated);
    assert_eq!(waiting(&pass), vec![(Side::Sell, 1)]);
    cancelled(&mut reg, emulated, Side::Sell, "v0", 2);
    let after = plan_and_send(&mut p, &next, &mut reg, &caps, &mut mint, LATER2);
    assert!(after.refused.is_empty(), "{:?}", refusals(&after));
    assert_eq!(
        shape(&after),
        vec![
            (Stage::Reducing, "amend", Side::Sell, 1),
            (Stage::Reducing, "place", Side::Sell, 0),
        ]
    );
    assert_eq!(after.commands[0].cid, ask);
    assert_eq!(amended_total(&after.commands[0]), lots(5));
    assert_emulated(placed(&after.commands[1]), 5);
    let replaced = after.commands[1].cid;
    ack(&mut reg, replaced, "v2");
    // Nothing churns: the ask's amend in flight, the replacement exit rests untouched.
    let settled = plan_and_send(&mut p, &next, &mut reg, &caps, &mut mint, LATER3);
    assert!(settled.commands.is_empty(), "{:?}", shape(&settled));
}

#[test]
fn an_emulated_order_whose_quote_becomes_ordinary_stays_within_what_is_left_to_reduce() {
    // Codex r4310102183: long 10, an emulated exit of 5 and a reduce-only ask of 5 rest. The
    // exit's quote becomes an ordinary post-only ask of 10 with the same wire flags, at a new
    // price: the order keeps its emulated mode, so its amend is clipped to the 5 left and no
    // cancel follows; never an unflagged 10 beside the other reducer.
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let caps = amending(conflicting(&[(Feature::PostOnly, Feature::ReduceOnly)]));
    let first = book()
        .with(
            Side::Sell,
            0,
            maker_exit(10_010, 5, ReduceFallback::EmulateReduceOnly),
        )
        .with(Side::Sell, 1, reduce_only(limit(10_030, 5)));
    let plan = plan_and_send(&mut p, &first, &mut reg, &caps, &mut mint, MonoNs(0));
    assert_eq!(
        shape(&plan),
        vec![
            (Stage::Reducing, "place", Side::Sell, 1),
            (Stage::Reducing, "place", Side::Sell, 0),
        ]
    );
    let (native, emulated) = (plan.commands[0].cid, plan.commands[1].cid);
    assert_emulated(placed(&plan.commands[1]), 5);
    ack(&mut reg, native, "v1");
    ack(&mut reg, emulated, "v0");
    let ordinary = DesiredQuote {
        post_only: true,
        ..limit(10_020, 10)
    };
    let next = book().with(Side::Sell, 0, ordinary.clone()).with(
        Side::Sell,
        1,
        reduce_only(limit(10_030, 5)),
    );
    let pass = plan_and_send(&mut p, &next, &mut reg, &caps, &mut mint, LATER);
    // Still emulated, it is still a reducer (Codex r4315460452): built among the reducers and
    // classified reducing.
    assert_eq!(
        shape(&pass),
        vec![(Stage::Reducing, "amend", Side::Sell, 0)]
    );
    assert_eq!(pass.commands[0].cid, emulated);
    match pass.commands[0].auth.command() {
        VenueCommand::Amend(a) => assert_eq!((a.qty, a.reducing), (lots(5), true)),
        other => panic!("expected an amend, got {other:?}"),
    }
    // Its amend in flight, nothing more is built: no cancel, no second amend.
    let again = plan_and_send(&mut p, &next, &mut reg, &caps, &mut mint, LATER2);
    assert!(again.commands.is_empty(), "{:?}", shape(&again));
}

/// Each refused level and why, in the order refused.
fn refusals(plan: &Plan) -> Vec<(Side, u16, PlanRefusal)> {
    plan.refused
        .iter()
        .map(|r| (r.side, r.level, r.why.clone()))
        .collect()
}

/// The resting-cap refusal on the sell side of `INST`, `resting` against the cap of 12.
fn resting_capped(resting: i64) -> CapRefusal {
    CapRefusal::RestingCap {
        inst: INST,
        side: Side::Sell,
        resting: Some(lots(resting)),
        cap: lots(12),
    }
}

#[test]
fn a_reducer_refused_for_its_own_cap_leaves_the_emulated_order_resting() {
    // Reviewer B B8: long 10 under a resting cap of 12 per side; a reduce-only ask of 15 at
    // level 0 and an emulated exit of 10 at level 1. The ask is refused by the resting cap on
    // its own, the emulated order counted or not: cancelling the emulated order for it would
    // only see it placed again the next pass, and cancelled again the one after, forever. The
    // ask reports its own refusal and the emulated order rests.
    let mut reg = quoting_resting(10, 12);
    let mut mint = mint();
    let mut p = planner();
    let caps = conflicting(&[(Feature::PostOnly, Feature::ReduceOnly)]);
    let desired = book()
        .with(Side::Sell, 0, reduce_only(limit(10_010, 15)))
        .with(
            Side::Sell,
            1,
            maker_exit(10_020, 10, ReduceFallback::EmulateReduceOnly),
        );
    let plan = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, MonoNs(0));
    assert_eq!(
        shape(&plan),
        vec![(Stage::Reducing, "place", Side::Sell, 1)]
    );
    let emulated = plan.commands[0].cid;
    assert_emulated(placed(&plan.commands[0]), 10);
    ack(&mut reg, emulated, "v1");
    for now in [LATER, LATER2, LATER3] {
        let pass = plan_and_send(&mut p, &desired, &mut reg, &caps, &mut mint, now);
        assert!(pass.commands.is_empty(), "{:?}", shape(&pass));
        assert_eq!(
            refusals(&pass),
            vec![(
                Side::Sell,
                0,
                PlanRefusal::Place(OmsError::Capped(resting_capped(15)))
            )]
        );
        assert!(!reg.get(emulated).unwrap().state().is_terminal());
    }
}

#[test]
fn a_reducer_the_resting_cap_admits_once_the_emulated_order_ends_waits_for_its_cancel() {
    // Reviewer B B8, the other way: long 10 under a resting cap of 12 per side, an emulated
    // exit of 10 resting at level 1; a reduce-only ask of 5 at level 0 is refused with the
    // emulated order resting (15 > 12) but admitted once it ends. The emulated order is
    // cancelled for it, and the ask built once that order is terminal, then the emulated
    // level again at what is left.
    let mut reg = quoting_resting(10, 12);
    let mut mint = mint();
    let mut p = planner();
    let caps = conflicting(&[(Feature::PostOnly, Feature::ReduceOnly)]);
    let exit = maker_exit(10_020, 10, ReduceFallback::EmulateReduceOnly);
    let first = book().with(Side::Sell, 1, exit);
    let plan = plan_and_send(&mut p, &first, &mut reg, &caps, &mut mint, MonoNs(0));
    let emulated = plan.commands[0].cid;
    ack(&mut reg, emulated, "v1");
    let next = first.with(Side::Sell, 0, reduce_only(limit(10_010, 5)));
    let pass = plan_and_send(&mut p, &next, &mut reg, &caps, &mut mint, LATER);
    assert_eq!(shape(&pass), vec![(Stage::Cancel, "cancel", Side::Sell, 1)]);
    assert_eq!(pass.commands[0].cid, emulated);
    assert_eq!(waiting(&pass), vec![(Side::Sell, 0)]);
    cancelled(&mut reg, emulated, Side::Sell, "v1", 2);
    let after = plan_and_send(&mut p, &next, &mut reg, &caps, &mut mint, LATER2);
    assert_eq!(
        shape(&after),
        vec![
            (Stage::Reducing, "place", Side::Sell, 0),
            (Stage::Reducing, "place", Side::Sell, 1),
        ]
    );
    assert_emulated(placed(&after.commands[1]), 5);
}

#[test]
fn a_reducer_amend_refused_for_its_own_cap_leaves_the_emulated_order_resting() {
    // Reviewer B B8 for an amend: long 10 under a resting cap of 12 per side, a reduce-only
    // ask of 2 at level 0 and an emulated exit of 8 at level 1 rest. The ask grows to 14 by
    // amend, which the resting cap refuses on its own: the amend reports its own refusal and
    // the emulated order rests.
    let mut reg = quoting_resting(10, 12);
    let mut mint = mint();
    let mut p = planner();
    let caps = amending(conflicting(&[(Feature::PostOnly, Feature::ReduceOnly)]));
    let exit = maker_exit(10_020, 8, ReduceFallback::EmulateReduceOnly);
    let first = book()
        .with(Side::Sell, 0, reduce_only(limit(10_010, 2)))
        .with(Side::Sell, 1, exit.clone());
    let plan = plan_and_send(&mut p, &first, &mut reg, &caps, &mut mint, MonoNs(0));
    let (native, emulated) = (plan.commands[0].cid, plan.commands[1].cid);
    ack(&mut reg, native, "v0");
    ack(&mut reg, emulated, "v1");
    let grown = book()
        .with(Side::Sell, 0, reduce_only(limit(10_010, 14)))
        .with(Side::Sell, 1, exit);
    for now in [LATER, LATER2] {
        let pass = plan_and_send(&mut p, &grown, &mut reg, &caps, &mut mint, now);
        assert!(pass.commands.is_empty(), "{:?}", shape(&pass));
        assert_eq!(
            refusals(&pass),
            vec![(
                Side::Sell,
                0,
                PlanRefusal::Amend(AmendRefusal::Capped(resting_capped(14)))
            )]
        );
        assert!(!reg.get(emulated).unwrap().state().is_terminal());
    }
}

#[test]
fn a_reducer_judged_with_the_emulated_order_ended_still_counts_the_side_s_other_orders() {
    // Reviewer B B8: long 10 under a resting cap of 12 per side; an emulated exit of 4 at
    // level 1 and a plain post-only ask of 8 at level 2 rest (12). A reduce-only ask of 7 at
    // level 0 would leave the emulated order overshooting (3 left for its 4), but with that
    // order ended it still rests 15 beside the plain ask: it is refused for its own cap, and
    // the emulated order rests.
    let mut reg = quoting_resting(10, 12);
    let mut mint = mint();
    let mut p = planner();
    let caps = conflicting(&[(Feature::PostOnly, Feature::ReduceOnly)]);
    let plain = DesiredQuote {
        post_only: true,
        ..limit(10_030, 8)
    };
    let first = book()
        .with(
            Side::Sell,
            1,
            maker_exit(10_020, 4, ReduceFallback::EmulateReduceOnly),
        )
        .with(Side::Sell, 2, plain);
    let plan = plan_and_send(&mut p, &first, &mut reg, &caps, &mut mint, MonoNs(0));
    assert_eq!(
        shape(&plan),
        vec![
            (Stage::Reducing, "place", Side::Sell, 1),
            (Stage::Add, "place", Side::Sell, 2),
        ]
    );
    let (emulated, ask) = (plan.commands[0].cid, plan.commands[1].cid);
    ack(&mut reg, emulated, "v1");
    ack(&mut reg, ask, "v2");
    let next = first.with(Side::Sell, 0, reduce_only(limit(10_010, 7)));
    let pass = plan_and_send(&mut p, &next, &mut reg, &caps, &mut mint, LATER);
    assert!(pass.commands.is_empty(), "{:?}", shape(&pass));
    assert_eq!(
        refusals(&pass),
        vec![(
            Side::Sell,
            0,
            PlanRefusal::Place(OmsError::Capped(resting_capped(15)))
        )]
    );
    assert!(!reg.get(emulated).unwrap().state().is_terminal());
}

/// A trustworthy resync at 5 000 showing, beside nothing else, an order of ours the registry
/// does not hold: a sell of `qty` at 10 030, registered as an orphan (decision 0108), so the
/// adding side is suppressed and only what reduces the position within its size is built.
fn sell_orphan(reg: &mut Registry, pos: i64, qty: i64) {
    let o = common::cid();
    let snap = ResyncSnapshot {
        watermark: WallNs(5_000),
        requested_at: MonoNs(5_000),
        orders: vec![VenueOrderSnapshot {
            cid: Some(CidMatch::Ours(o)),
            vid: vid("orphan"),
            inst: INST,
            side: Side::Sell,
            state: VenueOrderState::Open,
            px: Some(Ticks(10_030)),
            qty: lots(qty),
            cum_filled: lots(0),
            post_only: None,
            reduce_only: None,
        }],
        positions: vec![(INST, SignedLots(pos))],
    };
    let report = reg
        .resync(&ladder_cfg(), &order_caps(), &snap, key(5_000))
        .unwrap();
    assert_eq!(report.orphans, vec![INST], "{report:?}");
    assert_eq!(reg.orphans(INST), vec![o]);
}

#[test]
fn during_an_orphan_cooldown_a_reducer_refused_anyway_cancels_no_emulated_order_it_waits_for() {
    // Reviewer A on PR #171, at the rebase onto FBC-01w's planner: long 10, an emulated exit of
    // 8 rests at level 1, and an orphan sell of 8 suppresses the adding side. A reduce-only ask
    // of 5 at level 0 would wait for the emulated order's cancel, but even with that order
    // ended the orphan and the ask would sell 13 against a position of 10: the suppression
    // refuses the ask, so the pass cancels nothing for it (decision 0108).
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let caps = conflicting(&[(Feature::PostOnly, Feature::ReduceOnly)]);
    let exit = maker_exit(10_020, 8, ReduceFallback::EmulateReduceOnly);
    let first = book().with(Side::Sell, 1, exit);
    let plan = plan_and_send(&mut p, &first, &mut reg, &caps, &mut mint, MonoNs(0));
    let emulated = plan.commands[0].cid;
    ack(&mut reg, emulated, "v1");
    sell_orphan(&mut reg, 10, 8);
    let next = first.with(Side::Sell, 0, reduce_only(limit(10_010, 5)));
    let pass = plan_and_send(&mut p, &next, &mut reg, &caps, &mut mint, LATER);
    assert!(
        pass.commands.iter().all(|c| c.cid != emulated),
        "{:?}",
        shape(&pass)
    );
    assert!(waiting(&pass).is_empty(), "{:?}", pass.refused);
    assert!(
        refusals(&pass).contains(&(
            Side::Sell,
            0,
            PlanRefusal::Place(OmsError::OrphanCooldown {
                inst: INST,
                side: Side::Sell,
            })
        )),
        "{:?}",
        pass.refused
    );
}

#[test]
fn during_an_orphan_cooldown_a_reducer_amend_refused_anyway_cancels_no_emulated_order_it_waits_for()
{
    // Reviewer A on PR #171, at the rebase onto FBC-01w's planner: long 10, a reduce-only ask
    // of 2 at level 0 and an emulated exit of 8 at level 1 rest, then an orphan sell of 8
    // suppresses the adding side. The ask grows to 10 by amend: even with the emulated order
    // ended, the orphan and the ask would sell 18 against a position of 10, so the suppression
    // refuses the amend and the pass cancels nothing for it (decision 0108).
    let mut reg = quoting(10);
    let mut mint = mint();
    let mut p = planner();
    let caps = amending(conflicting(&[(Feature::PostOnly, Feature::ReduceOnly)]));
    let exit = maker_exit(10_020, 8, ReduceFallback::EmulateReduceOnly);
    let first = book()
        .with(Side::Sell, 0, reduce_only(limit(10_010, 2)))
        .with(Side::Sell, 1, exit.clone());
    let plan = plan_and_send(&mut p, &first, &mut reg, &caps, &mut mint, MonoNs(0));
    let (native, emulated) = (plan.commands[0].cid, plan.commands[1].cid);
    ack(&mut reg, native, "v0");
    ack(&mut reg, emulated, "v1");
    sell_orphan(&mut reg, 10, 8);
    let grown = book()
        .with(Side::Sell, 0, reduce_only(limit(10_010, 10)))
        .with(Side::Sell, 1, exit);
    let pass = plan_and_send(&mut p, &grown, &mut reg, &caps, &mut mint, LATER);
    assert!(
        pass.commands.iter().all(|c| c.cid != emulated),
        "{:?}",
        shape(&pass)
    );
    assert!(waiting(&pass).is_empty(), "{:?}", pass.refused);
    assert!(
        refusals(&pass).contains(&(
            Side::Sell,
            0,
            PlanRefusal::Amend(AmendRefusal::OrphanCooldown {
                inst: INST,
                side: Side::Sell,
            })
        )),
        "{:?}",
        pass.refused
    );
}
