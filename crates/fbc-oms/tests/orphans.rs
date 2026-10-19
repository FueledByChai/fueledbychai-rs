//! Orphans (0005's I7; FBC-840): a resync that finds open orders under our namespace that the
//! registry does not hold, on a market a resync already seeded, registers them, suppresses the
//! market's adding side for the consumer's cooldown and cancels them by explicit reference in
//! a cancel-many, in every market state, a disarmed market included. Above the consumer's level
//! limit it escalates to an instrument cancel-all, only while the registry holds the market's
//! exclusive lease and no order not ours is in view (the snapshot's included); otherwise the
//! explicit cancels repeat on each resync. No foreign-namespace order is ever cancelled. From an
//! untrustworthy snapshot source it acts only on the orders a live account event also showed.
//!
//! The values are the owner's first test values: a $50 inventory cap on a synthetic market
//! where one lot is worth $1, so 50 lots; the resting cap is set past any order here.

#[path = "common/arm.rs"]
mod arm;
mod common;

use std::time::Duration;

use common::{canceled, cid, fill, fill_id, lots, order_caps, placement, update, vid};
use fbc_core::{
    AmendAck, AmendCaps, AmendQty, CancelBatch, CancelScope, CidMatch, ClientOrderId, ExchNs,
    ExchTsKind, FillIdent, InstrumentId, ItemRef, MonoNs, Namespace, NewOrder, OrderCaps,
    OrderKind, OrderRef, RefKind, Reject, RejectKind, RpcId, Side, SignedLots, SnapshotReads,
    SnapshotSource, SubmitOutcome, Support, TagSet, TerminalHint, Ticks, VenueCommand,
    VenueOrderSnapshot, VenueOrderState, WallNs,
};
use fbc_oms::{
    Admission, AmendRefusal, CancelAllRefusal, CapRefusal, Cooldown, EntryState, FillLedger,
    FillRouted, FillTime, IssueRefusal, LadderConfig, LedgerConfig, MarketCapsConfig, OmsError,
    OrdState, OrderKey, OrderOp, OrphanCancels, OrphanConfig, OrphanConfigError, OrphanPlan,
    PositionCheck, PreTradeCaps, Registry, ResyncReport, ResyncSnapshot, Routed,
    StaleAuthorization, TerminalKind,
};

const INST: InstrumentId = InstrumentId::new(1);
const OTHER: InstrumentId = InstrumentId::new(2);
/// The owner's first inventory cap, in lots of $1.
const CAP: i64 = 50;
/// Another engine's namespace on the account (FBC-aoh5: a Java instance shares the account).
const THEIRS: Namespace = Namespace::new(8);
/// The consumer's cooldown in these tests: 5 s.
const COOLDOWN: Duration = Duration::from_secs(5);
/// The consumer's level limit in these tests.
const LIMIT: u32 = 3;

fn caps() -> PreTradeCaps {
    [INST, OTHER]
        .into_iter()
        .try_fold(PreTradeCaps::new(), |caps, m| {
            caps.with_market(
                m,
                MarketCapsConfig {
                    inventory: Some(lots(CAP)),
                    resting: Some(lots(1_000_000)),
                },
            )
        })
        .unwrap()
}

/// A venue that cancels one instrument's orders, or the whole account's, in one request, and
/// batches cancels by venue id, with a trustworthy snapshot: the account cancel-all is declared
/// so that never building one is the OMS's choice, not the venue's.
fn venue() -> OrderCaps {
    OrderCaps {
        cancel_all_account: Support::Native,
        cancel_all_instrument: Support::Native,
        batch_cancel: Some(CancelBatch {
            max_items: 10,
            refs: TagSet::of(&[RefKind::Venue]),
        }),
        snapshot_source: SnapshotSource::Trustworthy,
        ..order_caps()
    }
}

/// [`venue`] whose snapshot source is `source`.
fn venue_from(source: SnapshotSource) -> OrderCaps {
    OrderCaps {
        snapshot_source: source,
        ..venue()
    }
}

fn cfg() -> OrphanConfig {
    OrphanConfig::new(COOLDOWN, LIMIT).unwrap()
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

/// A resync requested at `at` (on both clocks) of `orders`, positions `pos`, from `source`.
fn resync_at(
    reg: &mut Registry,
    source: SnapshotSource,
    at: u64,
    orders: Vec<VenueOrderSnapshot>,
    pos: i64,
) -> ResyncReport {
    resync_on(reg, &venue_from(source), at, orders, pos)
}

/// A resync requested at `at` (on both clocks) of `orders`, positions `pos`, on a venue whose
/// order caps are `venue`.
fn resync_on(
    reg: &mut Registry,
    venue: &OrderCaps,
    at: u64,
    orders: Vec<VenueOrderSnapshot>,
    pos: i64,
) -> ResyncReport {
    let snap = ResyncSnapshot {
        watermark: WallNs(i64::try_from(at).unwrap()),
        requested_at: MonoNs(at),
        orders,
        positions: vec![(INST, SignedLots(pos))],
    };
    reg.resync(&ladder_cfg(), venue, &snap, key(at)).unwrap()
}

/// [`venue`] whose resync reads the open orders and the positions separately (decision 0095),
/// as Paradex's does.
fn separate() -> OrderCaps {
    OrderCaps {
        snapshot_reads: SnapshotReads::Separate,
        ..venue()
    }
}

/// A named registry under the caps whose first trustworthy resync, at 1 000, seeded `INST` at
/// `pos` and `OTHER` flat, showing no order: disarmed, in Cancel-only.
fn seeded(pos: i64) -> Registry {
    seeded_on(&venue(), pos)
}

/// [`seeded`], on a venue whose order caps are `venue`.
fn seeded_on(venue: &OrderCaps, pos: i64) -> Registry {
    let mut reg = arm::named(Registry::with_caps(caps()));
    let report = resync_on(&mut reg, venue, 1_000, vec![], pos);
    assert_eq!(report.seeded.len(), 2, "{report:?}");
    assert!(report.orphans.is_empty());
    assert_eq!(reg.position(INST), Some(SignedLots(pos)));
    reg
}

/// Our order `c` as a snapshot shows it, open on `INST` under `v`: 5 lots on `side`.
fn ours(c: ClientOrderId, v: &str, side: Side) -> VenueOrderSnapshot {
    VenueOrderSnapshot {
        cid: Some(CidMatch::Ours(c)),
        vid: vid(v),
        inst: INST,
        side,
        state: VenueOrderState::Open,
        px: Some(Ticks(100)),
        qty: lots(5),
        cum_filled: lots(0),
        post_only: None,
        reduce_only: None,
    }
}

/// An order not ours as a snapshot shows it on `INST`, under `cid` (`None`: no client id).
fn not_ours(cid: Option<CidMatch>, v: &str) -> VenueOrderSnapshot {
    VenueOrderSnapshot {
        cid,
        ..ours(common::cid(), v, Side::Buy)
    }
}

fn sell(qty: i64) -> NewOrder {
    NewOrder {
        side: Side::Sell,
        kind: OrderKind::Limit { px: Ticks(101) },
        ..placement(cid(), 101, qty)
    }
}

fn buy(qty: i64) -> NewOrder {
    placement(cid(), 100, qty)
}

/// Builds and authorizes the place of `order`, or says why it was refused.
fn place(reg: &mut Registry, order: NewOrder) -> Result<(), OmsError> {
    reg.place(order).map(|cmd| arm::issue(reg, cmd))
}

fn suppressed(side: Side) -> OmsError {
    OmsError::OrphanCooldown { inst: INST, side }
}

/// The venue ids a cancel-many names, in order; every item is on `INST`.
fn cancelled(cmd: &VenueCommand) -> Vec<String> {
    let VenueCommand::CancelMany(items) = cmd else {
        panic!("expected a cancel-many, got {cmd:?}");
    };
    items
        .iter()
        .map(|i| {
            assert_eq!(i.inst, INST);
            match &i.target {
                OrderRef::Venue(v) | OrderRef::Both(_, v) => v.as_str().to_owned(),
                OrderRef::Client(c) => panic!("named by client id alone: {c:?}"),
            }
        })
        .collect()
}

/// The explicit cancels `plan` built: one cancel-many naming `vids` by reference, nothing
/// refused or waiting, with `escalation` as the reason no cancel-all was built.
fn assert_explicit(plan: &OrphanPlan, vids: &[&str], escalation: Option<CancelAllRefusal>) {
    let OrphanCancels::Explicit {
        plan: cancels,
        escalation: given,
    } = &plan.cancels
    else {
        panic!("expected explicit cancels, got {plan:?}");
    };
    assert_eq!(*given, escalation, "{plan:?}");
    assert!(cancels.refused.is_empty(), "{cancels:?}");
    assert!(cancels.awaiting_ack.is_empty(), "{cancels:?}");
    assert_eq!(cancels.commands.len(), 1, "one cancel-many: {cancels:?}");
    assert_eq!(cancelled(cancels.commands[0].command()), vids);
}

/// A live fill of our order `c` under `v`, its `qty` lots on `side` taking it to `cum` filled,
/// executed at `exec` on the venue's matching engine (`None`: no such time), arriving at
/// `arrived`, applied through `l`.
#[allow(clippy::too_many_arguments)]
fn fill_at(
    reg: &mut Registry,
    l: &mut FillLedger,
    c: ClientOrderId,
    v: &str,
    side: Side,
    qty: i64,
    cum: i64,
    exec: Option<i64>,
    arrived: u64,
) -> FillRouted {
    let f = fill(
        Some(c),
        FillIdent::Venue {
            fill: fill_id(&format!("{v}-{cum}")),
            vid: Some(vid(v)),
            cum_after: Some(lots(cum)),
        },
        side,
        qty,
        false,
    );
    let time = exec.map(|at| FillTime {
        exch: ExchNs(at),
        kind: ExchTsKind::MatchingEngine,
        aligned: WallNs(at),
    });
    let Admission::Apply(accepted) = l.admit(&f, time, MonoNs(arrived)) else {
        panic!("a new fill is admitted");
    };
    reg.apply_fill(accepted).unwrap()
}

/// Places our sell `order`, authorizes it and has the venue cancel it under `v`: a later fill
/// of it moves the position, and nothing of it counts on the side any more.
fn cancelled_sell(reg: &mut Registry, order: NewOrder, v: &str) -> ClientOrderId {
    let c = order.cid;
    place(reg, order).unwrap();
    let mut ended = update(Some(c), canceled(), 0);
    ended.vid = Some(vid(v));
    reg.apply_update(&ended, key(1_500));
    assert!(reg.get(c).unwrap().state().is_terminal());
    c
}

fn ledger() -> FillLedger {
    FillLedger::new(
        LedgerConfig {
            max_age: Duration::from_secs(3600),
            max_entries: 10_000,
        },
        WallNs(0),
    )
    .unwrap()
}

#[test]
fn the_consumer_states_a_cooldown_and_a_level_limit() {
    assert_eq!(
        OrphanConfig::new(Duration::ZERO, LIMIT),
        Err(OrphanConfigError::ZeroCooldown)
    );
    assert_eq!(
        OrphanConfig::new(COOLDOWN, 0),
        Err(OrphanConfigError::ZeroLevelLimit)
    );
    let cfg = cfg();
    assert_eq!((cfg.cooldown(), cfg.level_limit()), (COOLDOWN, LIMIT));
    for e in [
        OrphanConfigError::ZeroCooldown,
        OrphanConfigError::ZeroLevelLimit,
    ] {
        assert!(!e.to_string().is_empty());
    }
}

#[test]
fn an_orphan_suppresses_the_adding_side_for_the_configured_cooldown() {
    let mut reg = seeded(0);
    arm::start(&mut reg, INST);
    place(&mut reg, buy(5)).unwrap();
    let before = reg.entry(INST).generation();
    // A place built and authorized before the resync, not yet sent.
    let cmd = reg.place(buy(1)).unwrap();
    let waiting = reg.authorize(arm::TEST_ACCT, cmd).unwrap();
    assert_eq!(reg.orphan_cooldown(INST), None);

    let o = cid();
    let report = resync_at(
        &mut reg,
        SnapshotSource::Trustworthy,
        2_000,
        vec![ours(o, "o", Side::Buy)],
        0,
    );
    assert_eq!(report.untracked, vec![(o, vid("o"))]);
    assert_eq!(report.orphans, vec![INST]);
    // Registered, from the snapshot, counting as it rests.
    let rec = reg.get(o).unwrap();
    assert!(rec.from_snapshot());
    assert_eq!(rec.state(), OrdState::Open);
    assert_eq!(reg.orphans(INST), vec![o]);
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(5 + 1 + 5)));

    // From the resync on, until the consumer's cancel of the orphans starts the cooldown: the
    // market is flat, so both sides add to its position.
    assert_eq!(reg.orphan_cooldown(INST), Some(Cooldown::Pending));
    assert_eq!(place(&mut reg, buy(1)), Err(suppressed(Side::Buy)));
    assert_eq!(place(&mut reg, sell(1)), Err(suppressed(Side::Sell)));
    assert_eq!(reg.end_orphan_cooldowns(MonoNs(u64::MAX)), vec![]);
    // The market's state generation moved, so what was authorized before is not sent.
    assert_ne!(reg.entry(INST).generation(), before);
    assert_eq!(reg.entry(INST).state(), EntryState::Quoting);
    assert!(matches!(
        waiting.check_at_submit(),
        Err(StaleAuthorization::StateChanged { market: INST, .. })
    ));
    // The other market is untouched.
    assert_eq!(reg.orphan_cooldown(OTHER), None);

    let now = MonoNs(10_000);
    let plan = reg.cancel_orphans(INST, &cfg(), &venue(), now);
    let until = MonoNs(10_000 + 5_000_000_000);
    assert_eq!(plan.cooldown, Some(Cooldown::Until(until)));
    assert_eq!(reg.orphan_cooldown(INST), Some(Cooldown::Until(until)));
    // A call at an earlier time never shortens it.
    let again = reg.cancel_orphans(INST, &cfg(), &venue(), MonoNs(9_000));
    assert_eq!(again.cooldown, Some(Cooldown::Until(until)));
    assert_eq!(reg.end_orphan_cooldowns(MonoNs(until.0 - 1)), vec![]);
    assert_eq!(place(&mut reg, buy(1)), Err(suppressed(Side::Buy)));
    assert!(!suppressed(Side::Buy).to_string().is_empty());

    assert_eq!(reg.end_orphan_cooldowns(until), vec![INST]);
    assert_eq!(reg.orphan_cooldown(INST), None);
    place(&mut reg, buy(1)).unwrap();
    place(&mut reg, sell(1)).unwrap();
}

#[test]
fn during_the_cooldown_only_the_side_that_reduces_the_position_takes_orders_never_past_zero() {
    // Long 20: buys add, sells reduce.
    let mut reg = seeded(20);
    arm::start(&mut reg, INST);
    let o = cid();
    resync_at(
        &mut reg,
        SnapshotSource::Trustworthy,
        2_000,
        vec![ours(o, "o", Side::Buy)],
        20,
    );
    assert_eq!(place(&mut reg, buy(1)), Err(suppressed(Side::Buy)));
    place(&mut reg, sell(15)).unwrap();
    // 15 + 6 sold would take long 20 past zero: refused; 15 + 5 is flat.
    assert_eq!(place(&mut reg, sell(6)), Err(suppressed(Side::Sell)));
    place(&mut reg, sell(5)).unwrap();
    // A batch item is judged the same way, the earlier items counted.
    let plan = reg.place_batch(vec![buy(1)]).unwrap();
    assert_eq!(
        plan.refused,
        vec![(plan.refused[0].0, suppressed(Side::Buy))]
    );
    assert!(plan.command.is_none());
}

#[test]
fn during_the_cooldown_an_amend_on_the_adding_side_is_refused() {
    let amending = OrderCaps {
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
        ..venue()
    };
    let mut reg = seeded(20);
    arm::start(&mut reg, INST);
    let (b, s) = (buy(5), sell(5));
    let (bc, sc) = (b.cid, s.cid);
    place(&mut reg, b).unwrap();
    place(&mut reg, s).unwrap();
    // Both acknowledged and resting, as the trustworthy resync showing the orphan shows them.
    let o = cid();
    let mut shown_b = ours(bc, "b", Side::Buy);
    shown_b.px = Some(Ticks(100));
    let mut shown_s = ours(sc, "s", Side::Sell);
    shown_s.px = Some(Ticks(101));
    resync_at(
        &mut reg,
        SnapshotSource::Trustworthy,
        2_000,
        vec![shown_b, shown_s, ours(o, "o", Side::Buy)],
        20,
    );
    assert_eq!(reg.orphans(INST), vec![o]);
    let refused = reg
        .live(bc)
        .unwrap()
        .amend(&amending, Ticks(99), lots(5), false)
        .unwrap_err();
    assert_eq!(
        refused,
        AmendRefusal::OrphanCooldown {
            inst: INST,
            side: Side::Buy
        }
    );
    // On the side that reduces the long position, within it: built.
    reg.live(sc)
        .unwrap()
        .amend(&amending, Ticks(102), lots(5), false)
        .unwrap();
}

#[test]
fn the_orphans_are_cancelled_by_explicit_reference_in_one_cancel_many_also_on_a_disarmed_market() {
    // Never armed since the restart: disarmed, in Cancel-only.
    let mut reg = seeded(0);
    let (a, b, c) = (cid(), cid(), cid());
    let report = resync_at(
        &mut reg,
        SnapshotSource::Trustworthy,
        2_000,
        vec![
            ours(c, "c", Side::Sell),
            ours(a, "a", Side::Buy),
            ours(b, "b", Side::Buy),
        ],
        0,
    );
    assert_eq!(reg.entry(INST).state(), EntryState::CancelOnly);
    assert!(!reg.entry(INST).armed());
    assert_eq!(report.orphans, vec![INST]);
    assert_eq!(reg.orphans(INST), vec![a, b, c]);
    let plan = reg.cancel_orphans(INST, &cfg(), &venue(), MonoNs(3_000));
    assert_eq!(plan.market, INST);
    assert_eq!(plan.orphans, vec![a, b, c]);
    // At the level limit: explicit, no escalation asked.
    assert_explicit(&plan, &["a", "b", "c"], None);

    // In Killed too.
    let mut reg = seeded(0);
    arm::start(&mut reg, INST);
    reg.kill(INST);
    let d = cid();
    resync_at(
        &mut reg,
        SnapshotSource::Trustworthy,
        2_000,
        vec![ours(d, "d", Side::Buy)],
        0,
    );
    let plan = reg.cancel_orphans(INST, &cfg(), &venue(), MonoNs(3_000));
    assert_explicit(&plan, &["d"], None);
    // Each cancel is authorized like any other cancel.
    let OrphanCancels::Explicit { plan: cancels, .. } = plan.cancels else {
        unreachable!()
    };
    for cmd in cancels.commands {
        arm::issue(&mut reg, cmd);
    }
}

#[test]
fn above_the_level_limit_an_instrument_cancel_all_is_built_only_under_the_exclusive_lease_with_nothing_foreign_in_the_snapshot()
 {
    let four = || -> Vec<ClientOrderId> { (0..4).map(|_| cid()).collect() };
    let shown = |cids: &[ClientOrderId]| -> Vec<VenueOrderSnapshot> {
        cids.iter()
            .enumerate()
            .map(|(i, &c)| ours(c, &format!("o{i}"), Side::Buy))
            .collect()
    };
    let names = ["o0", "o1", "o2", "o3"];

    // Armed (the exclusive lease held), a trustworthy view, nothing not ours: the cancel-all,
    // with the explicit cancel of our order the venue may not hold yet.
    let mut reg = seeded(0);
    arm::start(&mut reg, INST);
    let pending = buy(1);
    let pending_cid = pending.cid;
    place(&mut reg, pending).unwrap();
    let orphans = four();
    resync_at(
        &mut reg,
        SnapshotSource::Trustworthy,
        2_000,
        shown(&orphans),
        0,
    );
    let plan = reg.cancel_orphans(INST, &cfg(), &venue(), MonoNs(3_000));
    assert_eq!(plan.orphans, orphans);
    let OrphanCancels::CancelAll {
        command,
        unanswered,
    } = plan.cancels
    else {
        panic!("expected a cancel-all, got {plan:?}");
    };
    assert_eq!(
        command.command(),
        &VenueCommand::CancelAll(CancelScope::Instrument(INST))
    );
    assert!(unanswered.awaiting_ack.contains(&pending_cid) || !unanswered.commands.is_empty());
    arm::issue(&mut reg, command);

    // Disarmed: the exclusive lease is not held.
    let mut reg = seeded(0);
    let orphans = four();
    resync_at(
        &mut reg,
        SnapshotSource::Trustworthy,
        2_000,
        shown(&orphans),
        0,
    );
    let plan = reg.cancel_orphans(INST, &cfg(), &venue(), MonoNs(3_000));
    assert_explicit(&plan, &names, Some(CancelAllRefusal::NoExclusiveLease));

    // Armed, but another engine's order is in the snapshot.
    let mut reg = seeded(0);
    arm::start(&mut reg, INST);
    let orphans = four();
    let mut orders = shown(&orphans);
    orders.push(not_ours(Some(CidMatch::Foreign(THEIRS)), "theirs"));
    resync_at(&mut reg, SnapshotSource::Trustworthy, 2_000, orders, 0);
    let plan = reg.cancel_orphans(INST, &cfg(), &venue(), MonoNs(3_000));
    assert_explicit(&plan, &names, Some(CancelAllRefusal::ForeignInView));

    // A venue with no instrument cancel-all.
    let mut reg = seeded(0);
    arm::start(&mut reg, INST);
    let orphans = four();
    resync_at(
        &mut reg,
        SnapshotSource::Trustworthy,
        2_000,
        shown(&orphans),
        0,
    );
    let without = OrderCaps {
        cancel_all_instrument: Support::Unsupported,
        ..venue()
    };
    let plan = reg.cancel_orphans(INST, &cfg(), &without, MonoNs(3_000));
    assert_explicit(&plan, &names, Some(CancelAllRefusal::Unsupported));

    // At the limit, armed, nothing foreign: explicit.
    let mut reg = seeded(0);
    arm::start(&mut reg, INST);
    let three: Vec<ClientOrderId> = (0..3).map(|_| cid()).collect();
    resync_at(
        &mut reg,
        SnapshotSource::Trustworthy,
        2_000,
        shown(&three),
        0,
    );
    let plan = reg.cancel_orphans(INST, &cfg(), &venue(), MonoNs(3_000));
    assert_explicit(&plan, &names[..3], None);
}

#[test]
fn the_explicit_cancels_repeat_on_each_resync_until_the_orphans_end() {
    let mut reg = seeded(0);
    let (a, b) = (cid(), cid());
    let orders = || vec![ours(a, "a", Side::Buy), ours(b, "b", Side::Buy)];
    resync_at(&mut reg, SnapshotSource::Trustworthy, 2_000, orders(), 0);
    let plan = reg.cancel_orphans(INST, &cfg(), &venue(), MonoNs(3_000));
    assert_explicit(&plan, &["a", "b"], None);
    reg.cancel_sent(a, RpcId(1), MonoNs(3_000)).unwrap();
    reg.cancel_sent(b, RpcId(1), MonoNs(3_000)).unwrap();

    // The next resync still shows them: the market is named again, its cooldown waits for the
    // cancel again, and the cancels are built again, those in flight included.
    let report = resync_at(&mut reg, SnapshotSource::Trustworthy, 4_000, orders(), 0);
    assert!(report.untracked.is_empty(), "registered: {report:?}");
    assert_eq!(report.orphans, vec![INST]);
    assert_eq!(reg.orphan_cooldown(INST), Some(Cooldown::Pending));
    let plan = reg.cancel_orphans(INST, &cfg(), &venue(), MonoNs(5_000));
    assert_explicit(&plan, &["a", "b"], None);
    assert_eq!(
        plan.cooldown,
        Some(Cooldown::Until(MonoNs(5_000 + 5_000_000_000)))
    );

    // One ends: only the other is cancelled again.
    let mut ended = update(Some(a), canceled(), 0);
    ended.vid = Some(vid("a"));
    reg.apply_update(&ended, key(4_500));
    let report = resync_at(
        &mut reg,
        SnapshotSource::Trustworthy,
        6_000,
        vec![ours(b, "b", Side::Buy)],
        0,
    );
    assert_eq!(report.orphans, vec![INST]);
    let plan = reg.cancel_orphans(INST, &cfg(), &venue(), MonoNs(7_000));
    assert_explicit(&plan, &["b"], None);

    // Both ended: nothing is named, nothing built, and the cooldown runs out.
    let mut ended = update(Some(b), canceled(), 0);
    ended.vid = Some(vid("b"));
    reg.apply_update(&ended, key(7_500));
    let report = resync_at(&mut reg, SnapshotSource::Trustworthy, 8_000, vec![], 0);
    assert!(report.orphans.is_empty());
    assert!(reg.orphans(INST).is_empty());
    let plan = reg.cancel_orphans(INST, &cfg(), &venue(), MonoNs(9_000));
    assert!(plan.orphans.is_empty());
    let OrphanCancels::Explicit {
        plan: cancels,
        escalation: None,
    } = &plan.cancels
    else {
        panic!("{plan:?}");
    };
    assert!(cancels.commands.is_empty());
    assert_eq!(
        reg.end_orphan_cooldowns(MonoNs(7_000 + 5_000_000_000)),
        vec![INST]
    );
    assert_eq!(reg.orphan_cooldown(INST), None);
}

#[test]
fn no_foreign_namespace_order_is_ever_cancelled() {
    let mut reg = seeded(0);
    arm::start(&mut reg, INST);
    let orphans: Vec<ClientOrderId> = (0..5).map(|_| cid()).collect();
    let mut orders: Vec<VenueOrderSnapshot> = orphans
        .iter()
        .enumerate()
        .map(|(i, &c)| ours(c, &format!("o{i}"), Side::Buy))
        .collect();
    orders.push(not_ours(Some(CidMatch::Foreign(THEIRS)), "theirs"));
    orders.push(not_ours(Some(CidMatch::Unparseable), "java"));
    orders.push(not_ours(None, "nameless"));
    let report = resync_at(&mut reg, SnapshotSource::Trustworthy, 2_000, orders, 0);
    assert_eq!(report.orphans, vec![INST]);
    assert_eq!(reg.orphans(INST), orphans);
    assert!(reg.cid_of(&vid("theirs")).is_none());
    assert!(reg.cid_of(&vid("java")).is_none());
    assert!(reg.cid_of(&vid("nameless")).is_none());
    // Above the limit, armed: the orders not ours keep it explicit, and name none of them.
    let plan = reg.cancel_orphans(INST, &cfg(), &venue(), MonoNs(3_000));
    assert_explicit(
        &plan,
        &["o0", "o1", "o2", "o3", "o4"],
        Some(CancelAllRefusal::ForeignInView),
    );
    // An event under another namespace's client id never makes an orphan either.
    let mut theirs = update(None, VenueOrderState::Open, 0);
    theirs.cid = Some(CidMatch::Foreign(THEIRS));
    theirs.vid = Some(vid("theirs-2"));
    assert_eq!(
        reg.apply_update(&theirs, key(3_500)),
        Routed::Foreign(THEIRS)
    );
    let report = resync_at(
        &mut reg,
        SnapshotSource::Trustworthy,
        4_000,
        vec![not_ours(Some(CidMatch::Foreign(THEIRS)), "theirs-2")],
        0,
    );
    assert!(report.untracked.is_empty());
    assert!(reg.cid_of(&vid("theirs-2")).is_none());
}

#[test]
fn from_an_untrustworthy_source_only_the_orders_a_live_account_event_also_shows_are_acted_on() {
    // A market seeded by hand (tests and an owner-assisted testnet run only; decision 0067),
    // whose venue's snapshot source is not trustworthy, never armed.
    let mut reg = arm::named(Registry::with_caps(caps()));
    reg.seed_position(INST, SignedLots(0)).unwrap();
    let (a, b, c) = (cid(), cid(), cid());
    // A live order event shows `a` open; a live one shows `c` open and then cancelled.
    let mut live_a = update(Some(a), VenueOrderState::Open, 0);
    live_a.vid = Some(vid("a"));
    assert_eq!(reg.apply_update(&live_a, key(10)), Routed::Untracked);
    let mut live_c = update(Some(c), VenueOrderState::Amended { new_vid: None }, 0);
    live_c.vid = Some(vid("c"));
    reg.apply_update(&live_c, key(11));
    let mut ended_c = update(Some(c), canceled(), 0);
    ended_c.vid = Some(vid("c"));
    reg.apply_update(&ended_c, key(12));

    let shown = || {
        vec![
            ours(a, "a", Side::Buy),
            ours(b, "b", Side::Buy),
            ours(c, "c", Side::Buy),
        ]
    };
    let report = resync_at(&mut reg, SnapshotSource::Untrustworthy, 2_000, shown(), 0);
    assert!(report.untrustworthy);
    assert_eq!(
        report.untracked,
        vec![(a, vid("a")), (b, vid("b")), (c, vid("c"))]
    );
    // `b` and `c` (whose cancel withdrew its sighting) are not acted on: still not held.
    assert_eq!(report.orphans, vec![INST]);
    assert_eq!(reg.orphans(INST), vec![a]);
    assert!(reg.get(b).is_none() && reg.get(c).is_none());
    let plan = reg.cancel_orphans(
        INST,
        &cfg(),
        &venue_from(SnapshotSource::Untrustworthy),
        MonoNs(3_000),
    );
    assert_explicit(&plan, &["a"], None);

    // A live fill of `b` then shows it: the next resync acts on it.
    let mut l = ledger();
    let f = fill(
        Some(b),
        FillIdent::Venue {
            fill: fill_id("fb"),
            vid: Some(vid("b")),
            cum_after: Some(lots(1)),
        },
        Side::Buy,
        1,
        false,
    );
    let Admission::Apply(accepted) = l.admit(&f, None, MonoNs(3_500)) else {
        panic!("a new fill is admitted");
    };
    assert_eq!(
        reg.apply_fill(accepted).unwrap(),
        FillRouted::OursUntracked(b)
    );
    let report = resync_at(&mut reg, SnapshotSource::Untrustworthy, 4_000, shown(), 0);
    assert_eq!(report.untracked, vec![(b, vid("b")), (c, vid("c"))]);
    assert!(reg.get(c).is_none());
    assert_eq!(reg.orphans(INST), vec![a, b]);

    // A market whose only candidates are unconfirmed is neither named nor suppressed.
    let mut reg = arm::named(Registry::with_caps(caps()));
    reg.seed_position(INST, SignedLots(0)).unwrap();
    let report = resync_at(
        &mut reg,
        SnapshotSource::Untrustworthy,
        2_000,
        vec![ours(cid(), "x", Side::Buy)],
        0,
    );
    assert_eq!(report.untracked.len(), 1);
    assert!(reg.get(report.untracked[0].0).is_none());
    assert!(report.orphans.is_empty());
    assert_eq!(reg.orphan_cooldown(INST), None);
}

#[test]
fn an_order_a_first_resync_registers_on_a_market_it_seeds_is_no_orphan() {
    // An earlier run's order the seed registers (decision 0055) is not 0005's I7 orphan: the
    // kill switch's cancel everything reaches it, and nothing is suppressed.
    let mut reg = arm::named(Registry::with_caps(caps()));
    let e = cid();
    let report = resync_at(
        &mut reg,
        SnapshotSource::Trustworthy,
        1_000,
        vec![ours(e, "e", Side::Buy)],
        0,
    );
    assert_eq!(report.registered, vec![e]);
    assert!(report.orphans.is_empty());
    assert!(reg.orphans(INST).is_empty());
    assert_eq!(reg.orphan_cooldown(INST), None);
}

#[test]
fn an_untracked_order_whose_ids_another_order_of_ours_holds_or_that_is_not_open_is_not_acted_on() {
    let mut reg = seeded(0);
    arm::start(&mut reg, INST);
    let (x, y) = (buy(5), buy(5));
    let (xc, yc) = (x.cid, y.cid);
    place(&mut reg, x).unwrap();
    place(&mut reg, y).unwrap();
    resync_at(
        &mut reg,
        SnapshotSource::Trustworthy,
        2_000,
        vec![ours(xc, "x", Side::Buy), ours(yc, "y", Side::Buy)],
        0,
    );
    // Our `x`'s client id shown with `y`'s venue id: reported, not registered, no orphan.
    // An order of ours shown filled is not open: reported, not acted on either.
    let gone = cid();
    let mut filled = ours(gone, "gone", Side::Buy);
    filled.state = VenueOrderState::Filled;
    let report = resync_at(
        &mut reg,
        SnapshotSource::Trustworthy,
        3_000,
        vec![ours(xc, "y", Side::Buy), filled],
        0,
    );
    let mut expected = vec![(xc, vid("y")), (gone, vid("gone"))];
    expected.sort_by_key(|(c, _)| *c);
    assert_eq!(report.untracked, expected);
    assert!(report.orphans.is_empty());
    assert!(reg.get(gone).is_none());
    assert_eq!(reg.cid_of(&vid("y")), Some(yc));
    assert_eq!(reg.orphan_cooldown(INST), None);
}

#[test]
fn a_registered_orphans_later_fills_move_the_inventory_on_either_read_mode() {
    // Reviewer B's RB840-1 on PR #171: a later snapshot showing it open proves the orphan was
    // live after the seed, so the seed holds none of its fills but those its time places at
    // or before the seed's watermark (or that arrived before the seed was requested).
    for venue in [venue(), separate()] {
        let mut reg = seeded_on(&venue, 0);
        let mut l = ledger();
        let (o, p) = (cid(), cid());
        let report = resync_on(
            &mut reg,
            &venue,
            2_000,
            vec![ours(o, "o", Side::Buy), ours(p, "p", Side::Buy)],
            0,
        );
        let mut both = vec![o, p];
        both.sort();
        assert_eq!(reg.orphans(INST), both, "{report:?}");
        // After the seed: counted on the order and on the inventory.
        let routed = fill_at(
            &mut reg,
            &mut l,
            o,
            "o",
            Side::Buy,
            2,
            2,
            Some(2_500),
            3_000,
        );
        assert!(
            matches!(routed, FillRouted::Ours(c, _) if c == o),
            "{routed:?}"
        );
        assert_eq!(reg.position(INST), Some(SignedLots(2)), "{venue:?}");
        // With no matching-engine time: after the seed too.
        let routed = fill_at(&mut reg, &mut l, o, "o", Side::Buy, 3, 5, None, 3_100);
        assert!(
            matches!(routed, FillRouted::Ours(c, _) if c == o),
            "{routed:?}"
        );
        assert_eq!(reg.position(INST), Some(SignedLots(5)));
        assert!(reg.get(o).unwrap().state().is_terminal());
        // Executed at or before the seed's watermark: the seeded position holds it.
        let routed = fill_at(&mut reg, &mut l, p, "p", Side::Buy, 1, 1, Some(900), 3_200);
        assert_eq!(routed, FillRouted::InSnapshot(p));
        assert_eq!(reg.position(INST), Some(SignedLots(5)));
    }
}

#[test]
fn a_registered_orphans_fill_counts_against_the_reducing_side_during_the_cooldown() {
    // Reviewer B's RB840-1: long 20, a sell orphan of 5 fills; the position is 15, so a sell of
    // 16 would cross zero.
    let mut reg = seeded(20);
    arm::start(&mut reg, INST);
    let mut l = ledger();
    let o = cid();
    resync_at(
        &mut reg,
        SnapshotSource::Trustworthy,
        2_000,
        vec![ours(o, "o", Side::Sell)],
        20,
    );
    fill_at(
        &mut reg,
        &mut l,
        o,
        "o",
        Side::Sell,
        5,
        5,
        Some(2_500),
        3_000,
    );
    assert_eq!(reg.position(INST), Some(SignedLots(15)));
    assert_eq!(place(&mut reg, sell(16)), Err(suppressed(Side::Sell)));
    place(&mut reg, sell(15)).unwrap();
}

#[test]
fn during_the_cooldown_a_place_built_before_a_fill_moved_the_position_is_refused_at_authorization_and_at_submit()
 {
    // Reviewer B's RB840-2 on PR #171: long 20, a sell of 20 is built inside the cooldown, then
    // a late fill of a cancelled sell moves the position to 17 under it.
    let setup = || {
        let mut reg = seeded(20);
        arm::start(&mut reg, INST);
        let late = cancelled_sell(&mut reg, sell(3), "late");
        resync_at(
            &mut reg,
            SnapshotSource::Trustworthy,
            2_000,
            vec![ours(cid(), "o", Side::Buy)],
            20,
        );
        assert_eq!(reg.orphan_cooldown(INST), Some(Cooldown::Pending));
        let cmd = reg.place(sell(20)).unwrap();
        (reg, late, cmd)
    };

    // Moved before its authorization: refused there, and what it reserved released.
    let (mut reg, late, cmd) = setup();
    let mut l = ledger();
    fill_at(
        &mut reg,
        &mut l,
        late,
        "late",
        Side::Sell,
        3,
        3,
        Some(2_500),
        3_000,
    );
    assert_eq!(reg.position(INST), Some(SignedLots(17)));
    let refused = reg.authorize(arm::TEST_ACCT, cmd).unwrap_err();
    assert_eq!(
        refused,
        IssueRefusal::OrphanCooldown {
            inst: INST,
            side: Side::Sell
        }
    );
    assert!(!refused.to_string().is_empty());
    assert_eq!(place(&mut reg, sell(20)), Err(suppressed(Side::Sell)));
    place(&mut reg, sell(17)).unwrap();

    // Moved after its authorization: refused at submit.
    let (mut reg, late, cmd) = setup();
    let mut l = ledger();
    let auth = reg.authorize(arm::TEST_ACCT, cmd).unwrap();
    assert_eq!(auth.check_at_submit(), Ok(()));
    fill_at(
        &mut reg,
        &mut l,
        late,
        "late",
        Side::Sell,
        3,
        3,
        Some(2_500),
        3_000,
    );
    assert_eq!(
        auth.check_at_submit(),
        Err(StaleAuthorization::PositionMoved(INST))
    );
}

#[test]
fn during_the_cooldown_an_amend_built_before_a_fill_moved_the_position_is_refused_at_authorization_and_at_submit()
 {
    let amending = OrderCaps {
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
        ..venue()
    };
    let mut reg = seeded(20);
    arm::start(&mut reg, INST);
    let mut l = ledger();
    let (first, second) = (
        cancelled_sell(&mut reg, sell(3), "first"),
        cancelled_sell(&mut reg, sell(2), "second"),
    );
    let s = sell(5);
    let sc = s.cid;
    place(&mut reg, s).unwrap();
    let mut shown_s = ours(sc, "s", Side::Sell);
    shown_s.px = Some(Ticks(101));
    resync_at(
        &mut reg,
        SnapshotSource::Trustworthy,
        2_000,
        vec![shown_s, ours(cid(), "o", Side::Buy)],
        20,
    );
    assert_eq!(reg.orphan_cooldown(INST), Some(Cooldown::Pending));

    // Built to 20 against long 20; a fill moves it to 17 before its authorization.
    let cmd = reg
        .live(sc)
        .unwrap()
        .amend(&amending, Ticks(102), lots(20), false)
        .unwrap();
    fill_at(
        &mut reg,
        &mut l,
        first,
        "first",
        Side::Sell,
        3,
        3,
        Some(2_500),
        3_000,
    );
    assert_eq!(
        reg.authorize(arm::TEST_ACCT, cmd).unwrap_err(),
        IssueRefusal::OrphanCooldown {
            inst: INST,
            side: Side::Sell
        }
    );

    // Built to 17, authorized, and a fill moves it to 15 before it is sent.
    let cmd = reg
        .live(sc)
        .unwrap()
        .amend(&amending, Ticks(102), lots(17), false)
        .unwrap();
    let auth = reg.authorize(arm::TEST_ACCT, cmd).unwrap();
    fill_at(
        &mut reg,
        &mut l,
        second,
        "second",
        Side::Sell,
        2,
        2,
        Some(2_600),
        3_100,
    );
    assert_eq!(reg.position(INST), Some(SignedLots(15)));
    assert_eq!(
        auth.check_at_submit(),
        Err(StaleAuthorization::PositionMoved(INST))
    );
}

#[test]
fn an_orphans_fills_before_it_was_registered_move_the_inventory_once_a_snapshot_shows_it_open() {
    // Codex's r4310559340 on PR #171: a fill of our order the registry does not hold, after a
    // seed that did not show it, is placed in the seed (an earlier run's order, 0055's rule 3);
    // a later snapshot showing it open proves it came after, so registering the orphan counts
    // it on the inventory, once.
    let mut reg = seeded(0);
    let mut l = ledger();
    let (o, p) = (cid(), cid());
    let routed = fill_at(
        &mut reg,
        &mut l,
        o,
        "o",
        Side::Buy,
        2,
        2,
        Some(1_400),
        1_500,
    );
    assert_eq!(routed, FillRouted::InSnapshot(o));
    // Executed at or before the seed's watermark: the seed holds it.
    let routed = fill_at(&mut reg, &mut l, p, "p", Side::Buy, 1, 1, Some(900), 1_600);
    assert_eq!(routed, FillRouted::InSnapshot(p));
    assert_eq!(reg.position(INST), Some(SignedLots(0)));

    let mut shown_o = ours(o, "o", Side::Buy);
    shown_o.cum_filled = lots(2);
    let mut shown_p = ours(p, "p", Side::Buy);
    shown_p.cum_filled = lots(1);
    resync_at(
        &mut reg,
        SnapshotSource::Trustworthy,
        2_000,
        vec![shown_o.clone(), shown_p.clone()],
        3,
    );
    let mut both = vec![o, p];
    both.sort();
    assert_eq!(reg.orphans(INST), both);
    assert_eq!(reg.position(INST), Some(SignedLots(2)));
    // Shown again, it is counted no second time, and the next check agrees with the venue's.
    let report = resync_at(
        &mut reg,
        SnapshotSource::Trustworthy,
        3_000,
        vec![shown_o, shown_p],
        2,
    );
    assert_eq!(reg.position(INST), Some(SignedLots(2)));
    assert!(
        report.checks.contains(&PositionCheck::Agrees {
            inst: INST,
            position: SignedLots(2)
        }),
        "{report:?}"
    );
}

#[test]
fn a_place_authorized_during_the_cooldown_is_refused_at_submit_once_another_orphan_is_registered() {
    // Codex's r4310559351 on PR #171: long 20, a sell of 20 authorized inside the cooldown;
    // a later resync registers a sell orphan of 5 on the market, which with the sell would
    // take the position past zero.
    let mut reg = seeded(20);
    arm::start(&mut reg, INST);
    let first = cid();
    resync_at(
        &mut reg,
        SnapshotSource::Trustworthy,
        2_000,
        vec![ours(first, "first", Side::Buy)],
        20,
    );
    assert_eq!(reg.orphan_cooldown(INST), Some(Cooldown::Pending));
    let cmd = reg.place(sell(20)).unwrap();
    let auth = reg.authorize(arm::TEST_ACCT, cmd).unwrap();
    assert_eq!(auth.check_at_submit(), Ok(()));
    resync_at(
        &mut reg,
        SnapshotSource::Trustworthy,
        3_000,
        vec![
            ours(first, "first", Side::Buy),
            ours(cid(), "second", Side::Sell),
        ],
        20,
    );
    assert!(matches!(
        auth.check_at_submit(),
        Err(StaleAuthorization::StateChanged { market: INST, .. })
    ));
}

/// A named registry under the caps whose first trustworthy resync, at 2 000, seeded `INST` at
/// `pos` and `OTHER` flat, showing no order.
fn seeded_at_2000(pos: i64) -> Registry {
    let mut reg = arm::named(Registry::with_caps(caps()));
    let report = resync_at(&mut reg, SnapshotSource::Trustworthy, 2_000, vec![], pos);
    assert_eq!(report.seeded.len(), 2, "{report:?}");
    assert_eq!(reg.position(INST), Some(SignedLots(pos)));
    reg
}

#[test]
fn a_snapshot_requested_before_the_seed_registers_no_orphan_and_counts_no_fill_again() {
    // Codex's r4310661297 on PR #171: the seed, requested at 2 000, holds the venue's long 2,
    // which includes an untimed fill of our order x that it did not show; the fill arrives at
    // 2 500. A snapshot requested at 1 000 and answered late still shows x open: it was read
    // before the seed, so it proves nothing about the seed, and x's fill is not counted again.
    let mut reg = seeded_at_2000(2);
    let mut l = ledger();
    let x = cid();
    let routed = fill_at(&mut reg, &mut l, x, "x", Side::Buy, 2, 2, None, 2_500);
    assert_eq!(routed, FillRouted::InSnapshot(x));
    let report = resync_at(
        &mut reg,
        SnapshotSource::Trustworthy,
        1_000,
        vec![ours(x, "x", Side::Buy)],
        0,
    );
    assert!(
        report.checks.contains(&PositionCheck::Stale(INST)),
        "{report:?}"
    );
    assert!(report.orphans.is_empty(), "{report:?}");
    assert!(reg.get(x).is_none(), "not registered from a stale snapshot");
    assert!(reg.orphans(INST).is_empty());
    assert_eq!(reg.position(INST), Some(SignedLots(2)));
    // A later snapshot, x ended, agrees with the venue's long 2.
    let report = resync_at(&mut reg, SnapshotSource::Trustworthy, 3_000, vec![], 2);
    assert_eq!(reg.position(INST), Some(SignedLots(2)));
    assert!(
        report.checks.contains(&PositionCheck::Agrees {
            inst: INST,
            position: SignedLots(2)
        }),
        "{report:?}"
    );
}

#[test]
fn a_snapshot_requested_before_the_seed_keeps_the_fills_an_orphan_took_before_it_was_registered() {
    // Codex's r4310661297 on PR #171: the seed at 2 000 does not show x (it reached the venue
    // after the read); x fills 2, untimed, arriving at 2 500, which the seed holds only because
    // it did not show x. A snapshot requested at 1 000 not showing x proves nothing, so the fill
    // is kept; the snapshot at 3 000 shows x open and the venue's long 2: x is registered and
    // its fill counted.
    let mut reg = seeded_at_2000(0);
    let mut l = ledger();
    let x = cid();
    let routed = fill_at(&mut reg, &mut l, x, "x", Side::Buy, 2, 2, None, 2_500);
    assert_eq!(routed, FillRouted::InSnapshot(x));
    let report = resync_at(&mut reg, SnapshotSource::Trustworthy, 1_000, vec![], 0);
    assert!(
        report.checks.contains(&PositionCheck::Stale(INST)),
        "{report:?}"
    );
    let mut shown = ours(x, "x", Side::Buy);
    shown.cum_filled = lots(2);
    resync_at(&mut reg, SnapshotSource::Trustworthy, 3_000, vec![shown], 2);
    assert_eq!(reg.orphans(INST), vec![x]);
    assert_eq!(reg.position(INST), Some(SignedLots(2)));
}

#[test]
fn the_resync_that_registers_an_orphan_compares_the_position_with_its_earlier_fills_counted() {
    // Codex's r4310661291 on PR #171: a fill of our order o after the seed, which the seed holds
    // only because it did not show o, is counted as the snapshot at 2 000 shows o open; the same
    // resync compares the venue's long 2 with the inventory that counts it, and agrees.
    let mut reg = seeded(0);
    let mut l = ledger();
    let o = cid();
    let routed = fill_at(
        &mut reg,
        &mut l,
        o,
        "o",
        Side::Buy,
        2,
        2,
        Some(1_400),
        1_500,
    );
    assert_eq!(routed, FillRouted::InSnapshot(o));
    let mut shown = ours(o, "o", Side::Buy);
    shown.cum_filled = lots(2);
    let report = resync_at(&mut reg, SnapshotSource::Trustworthy, 2_000, vec![shown], 2);
    assert_eq!(reg.orphans(INST), vec![o]);
    assert_eq!(reg.position(INST), Some(SignedLots(2)));
    assert!(
        report.checks.contains(&PositionCheck::Agrees {
            inst: INST,
            position: SignedLots(2)
        }),
        "{report:?}"
    );
}

#[test]
fn an_order_of_ours_the_ladder_ended_lost_that_a_later_snapshot_shows_open_is_an_orphan() {
    // Reviewer B's RB840-8 on PR #171: a buy of 5 sent with no answer goes Unknown; a
    // trustworthy snapshot at 2 000 that does not show it, past the settle, ends it Lost. The
    // snapshot lagged (decision 0094 measured Paradex's open orders up to 3.5 s behind the
    // stream): the one at 3 000 shows it open. It is an orphan: counted on both caps, the
    // adding side suppressed, and cancelled by explicit reference.
    let mut reg = seeded(0);
    arm::start(&mut reg, INST);
    let order = buy(5);
    let c = order.cid;
    place(&mut reg, order).unwrap();
    reg.placement_sent(c, MonoNs(1_500), WallNs(1_500)).unwrap();
    reg.on_outcome(
        c,
        OrderOp::Place,
        &ItemRef {
            idx: 0,
            cid: Some(c),
            vid: None,
        },
        &SubmitOutcome::Unknown,
        MonoNs(1_600),
    )
    .unwrap();
    let report = resync_at(&mut reg, SnapshotSource::Trustworthy, 2_000, vec![], 0);
    assert_eq!(report.ladder.lost, vec![c], "{report:?}");
    assert_eq!(
        reg.get(c).unwrap().state(),
        OrdState::Terminal(TerminalKind::Lost)
    );
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(0)));
    // A source that is not trustworthy confirms nothing: reported, not acted on.
    let report = resync_at(
        &mut reg,
        SnapshotSource::Untrustworthy,
        2_500,
        vec![ours(c, "l", Side::Buy)],
        0,
    );
    assert_eq!(report.untracked, vec![(c, vid("l"))], "{report:?}");
    assert!(report.orphans.is_empty(), "{report:?}");
    assert!(reg.get(c).unwrap().state().is_terminal());

    let report = resync_at(
        &mut reg,
        SnapshotSource::Trustworthy,
        3_000,
        vec![ours(c, "l", Side::Buy)],
        0,
    );
    assert_eq!(report.untracked, vec![(c, vid("l"))], "{report:?}");
    assert_eq!(report.orphans, vec![INST]);
    assert_eq!(reg.orphans(INST), vec![c]);
    assert!(!reg.get(c).unwrap().state().is_terminal());
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(5)));
    assert_eq!(place(&mut reg, buy(1)), Err(suppressed(Side::Buy)));
    let plan = reg.cancel_orphans(INST, &cfg(), &venue(), MonoNs(3_100));
    assert_explicit(&plan, &["l"], None);
    // After the cooldown the inventory cap still counts it: a buy of 46 would take the worst
    // case to 51 against the 50-lot cap.
    let until = 3_100 + u64::try_from(COOLDOWN.as_nanos()).unwrap();
    assert_eq!(reg.end_orphan_cooldowns(MonoNs(until)), vec![INST]);
    assert_eq!(
        place(&mut reg, buy(46)),
        Err(OmsError::Capped(CapRefusal::InventoryCap {
            inst: INST,
            side: Side::Buy,
            worst: Some(lots(51)),
            cap: lots(CAP),
        }))
    );
    place(&mut reg, buy(45)).unwrap();
}

#[test]
fn an_orphans_earlier_fills_that_overflow_the_inventory_leave_its_position_unknown() {
    // Reviewer A on PR #171: seeded long i64::MAX lots, a fill of our order o the registry does
    // not hold arrives after the seed's request, untimed: the seed holds it only because it did
    // not show o. A snapshot showing o open registers it, and counting the fill would overflow
    // the inventory: the market's position is unknown from then on.
    let mut reg = seeded(i64::MAX);
    let mut l = ledger();
    let o = cid();
    let routed = fill_at(&mut reg, &mut l, o, "o", Side::Buy, 1, 1, None, 1_500);
    assert_eq!(routed, FillRouted::InSnapshot(o));
    let mut shown = ours(o, "o", Side::Buy);
    shown.cum_filled = lots(1);
    resync_at(
        &mut reg,
        SnapshotSource::Trustworthy,
        2_000,
        vec![shown],
        i64::MAX,
    );
    assert_eq!(reg.orphans(INST), vec![o]);
    assert_eq!(reg.position(INST), None);
}

#[test]
fn a_lost_order_shown_open_again_keeps_the_venue_ids_an_amend_replaced() {
    // Codex's r4315681141 on PR #171: our buy, open as `a` and amended onto `b`, goes on the
    // ladder with an unanswered cancel and ends Lost; a later snapshot shows it open as `b`.
    // Registered again, it still knows `a` was replaced: a delayed cancel naming only `a` does
    // not end it, so it stays an orphan, counted on both caps.
    let mut reg = seeded(0);
    arm::start(&mut reg, INST);
    let order = buy(5);
    let c = order.cid;
    place(&mut reg, order).unwrap();
    reg.placement_sent(c, MonoNs(1_100), WallNs(1_100)).unwrap();
    let mut open = update(Some(c), VenueOrderState::Open, 0);
    open.vid = Some(vid("a"));
    reg.apply_update(&open, key(1_200));
    let mut amended = update(
        None,
        VenueOrderState::Amended {
            new_vid: Some(vid("b")),
        },
        0,
    );
    amended.vid = Some(vid("a"));
    reg.apply_update(&amended, key(1_300));
    reg.cancel_sent(c, RpcId(7), MonoNs(1_400)).unwrap();
    reg.on_outcome(
        c,
        OrderOp::Cancel(RpcId(7)),
        &ItemRef {
            idx: 0,
            cid: Some(c),
            vid: None,
        },
        &SubmitOutcome::Unknown,
        MonoNs(1_500),
    )
    .unwrap();
    let report = resync_at(&mut reg, SnapshotSource::Trustworthy, 2_000, vec![], 0);
    assert_eq!(report.ladder.lost, vec![c], "{report:?}");
    resync_at(
        &mut reg,
        SnapshotSource::Trustworthy,
        3_000,
        vec![ours(c, "b", Side::Buy)],
        0,
    );
    assert_eq!(reg.orphans(INST), vec![c]);

    let mut late = update(None, canceled(), 0);
    late.vid = Some(vid("a"));
    reg.apply_update(&late, key(3_500));
    assert!(!reg.get(c).unwrap().state().is_terminal());
    assert_eq!(reg.orphans(INST), vec![c]);
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(5)));
}

#[test]
fn a_lost_order_shown_open_again_under_another_venue_id_takes_its_earlier_ids_as_replaced() {
    // Codex's r4315910340 on PR #171: our buy, open as `a`, with a fill naming `t`, goes on
    // the ladder with an unanswered cancel and ends Lost; a later snapshot shows it open as
    // `b` (an amend's update was missed). Registered again, its earlier ids `a` and `t` count
    // as replaced by `b`: a delayed cancel naming only one of them does not end it.
    let mut reg = seeded(0);
    let mut l = ledger();
    arm::start(&mut reg, INST);
    let order = buy(5);
    let c = order.cid;
    place(&mut reg, order).unwrap();
    reg.placement_sent(c, MonoNs(1_100), WallNs(1_100)).unwrap();
    let mut open = update(Some(c), VenueOrderState::Open, 0);
    open.vid = Some(vid("a"));
    reg.apply_update(&open, key(1_200));
    let routed = fill_at(&mut reg, &mut l, c, "t", Side::Buy, 1, 1, None, 1_250);
    assert!(matches!(routed, FillRouted::Ours(..)), "{routed:?}");
    reg.cancel_sent(c, RpcId(7), MonoNs(1_400)).unwrap();
    reg.on_outcome(
        c,
        OrderOp::Cancel(RpcId(7)),
        &ItemRef {
            idx: 0,
            cid: Some(c),
            vid: None,
        },
        &SubmitOutcome::Unknown,
        MonoNs(1_500),
    )
    .unwrap();
    let report = resync_at(&mut reg, SnapshotSource::Trustworthy, 2_000, vec![], 1);
    assert_eq!(report.ladder.lost, vec![c], "{report:?}");
    let mut shown = ours(c, "b", Side::Buy);
    shown.cum_filled = lots(1);
    resync_at(&mut reg, SnapshotSource::Trustworthy, 3_000, vec![shown], 1);
    assert_eq!(reg.orphans(INST), vec![c]);
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(4)));

    for (n, old) in [(3_500, "a"), (3_600, "t")] {
        let mut late = update(None, canceled(), 1);
        late.vid = Some(vid(old));
        reg.apply_update(&late, key(n));
        assert!(!reg.get(c).unwrap().state().is_terminal(), "{old}");
    }
    assert_eq!(reg.orphans(INST), vec![c]);
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(4)));
    assert_eq!(reg.position(INST), Some(SignedLots(1)));
}

#[test]
fn a_lost_order_shown_open_again_by_a_lagging_snapshot_keeps_the_fill_the_venue_reported() {
    // Codex's r4316124597 on PR #171: our buy of 5, open as `l` with the venue reporting 2
    // filled, goes on the ladder with an unanswered cancel and ends Lost; a later snapshot,
    // lagging, shows it open with nothing filled. Registered again, it keeps the 2 the venue
    // reported: 3 rest.
    let mut reg = seeded(0);
    arm::start(&mut reg, INST);
    let order = buy(5);
    let c = order.cid;
    place(&mut reg, order).unwrap();
    reg.placement_sent(c, MonoNs(1_100), WallNs(1_100)).unwrap();
    let mut open = update(Some(c), VenueOrderState::Open, 2);
    open.vid = Some(vid("l"));
    reg.apply_update(&open, key(1_200));
    reg.cancel_sent(c, RpcId(7), MonoNs(1_400)).unwrap();
    reg.on_outcome(
        c,
        OrderOp::Cancel(RpcId(7)),
        &ItemRef {
            idx: 0,
            cid: Some(c),
            vid: None,
        },
        &SubmitOutcome::Unknown,
        MonoNs(1_500),
    )
    .unwrap();
    let report = resync_at(&mut reg, SnapshotSource::Trustworthy, 2_000, vec![], 0);
    assert_eq!(report.ladder.lost, vec![c], "{report:?}");
    resync_at(
        &mut reg,
        SnapshotSource::Trustworthy,
        3_000,
        vec![ours(c, "l", Side::Buy)],
        0,
    );
    assert_eq!(reg.orphans(INST), vec![c]);
    assert_eq!(reg.get(c).unwrap().filled(), lots(2));
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(3)));
}

#[test]
fn an_orphan_whose_venue_id_the_seed_showed_has_its_fills_placed_by_what_the_seed_showed() {
    // Codex's r4316124604 on PR #171: the seed at 1 000 shows an order `v` with no client id,
    // 1 filled, and the venue's long 1. A later snapshot shows `v` under our client id o:
    // an orphan. A delayed, untimed fill of o naming `v` at a cumulative 1 is one the seed's
    // position already holds (0055's rule 2): it is not counted again.
    let mut reg = arm::named(Registry::with_caps(caps()));
    let mut unnamed = not_ours(None, "v");
    unnamed.cum_filled = lots(1);
    let report = resync_at(
        &mut reg,
        SnapshotSource::Trustworthy,
        1_000,
        vec![unnamed],
        1,
    );
    assert_eq!(report.seeded.len(), 2, "{report:?}");
    let o = cid();
    let mut shown = ours(o, "v", Side::Buy);
    shown.cum_filled = lots(1);
    resync_at(&mut reg, SnapshotSource::Trustworthy, 2_000, vec![shown], 1);
    assert_eq!(reg.orphans(INST), vec![o]);
    let mut l = ledger();
    let routed = fill_at(&mut reg, &mut l, o, "v", Side::Buy, 1, 1, None, 2_500);
    assert_eq!(routed, FillRouted::InSnapshot(o));
    assert_eq!(reg.position(INST), Some(SignedLots(1)));
}

#[test]
fn an_orphan_whose_venue_id_the_seed_showed_has_a_later_fill_naming_only_its_client_id_counted() {
    // Codex's r4316366360 on PR #171: as above, the seed showed `v`, 1 filled, with no client
    // id, and a later snapshot shows it under our client id o. A fill of o naming no venue id,
    // at a cumulative 2, came after the seed: what the seed showed of `v` places it, so it
    // moves the inventory.
    let mut reg = arm::named(Registry::with_caps(caps()));
    let mut unnamed = not_ours(None, "v");
    unnamed.cum_filled = lots(1);
    resync_at(
        &mut reg,
        SnapshotSource::Trustworthy,
        1_000,
        vec![unnamed],
        1,
    );
    let o = cid();
    let mut shown = ours(o, "v", Side::Buy);
    shown.cum_filled = lots(1);
    resync_at(&mut reg, SnapshotSource::Trustworthy, 2_000, vec![shown], 1);
    assert_eq!(reg.orphans(INST), vec![o]);
    let mut l = ledger();
    let f = fill(
        Some(o),
        FillIdent::Venue {
            fill: fill_id("v-2"),
            vid: None,
            cum_after: Some(lots(2)),
        },
        Side::Buy,
        1,
        false,
    );
    let Admission::Apply(accepted) = l.admit(&f, None, MonoNs(2_500)) else {
        panic!("a new fill is admitted");
    };
    let routed = reg.apply_fill(accepted).unwrap();
    assert!(matches!(routed, FillRouted::Ours(..)), "{routed:?}");
    assert_eq!(reg.position(INST), Some(SignedLots(2)));
}

/// A named registry seeded flat at 1 000 holding our buy of 5, sent at 1 500 with no answer
/// and ended Lost by the trustworthy snapshot at 2 000 that did not show it (the Unknown
/// ladder's settle and one absence): its client id.
fn ended_lost() -> (Registry, ClientOrderId) {
    let mut reg = seeded(0);
    arm::start(&mut reg, INST);
    let order = buy(5);
    let c = order.cid;
    place(&mut reg, order).unwrap();
    reg.placement_sent(c, MonoNs(1_500), WallNs(1_500)).unwrap();
    reg.on_outcome(
        c,
        OrderOp::Place,
        &ItemRef {
            idx: 0,
            cid: Some(c),
            vid: None,
        },
        &SubmitOutcome::Unknown,
        MonoNs(1_600),
    )
    .unwrap();
    let report = resync_at(&mut reg, SnapshotSource::Trustworthy, 2_000, vec![], 0);
    assert_eq!(report.ladder.lost, vec![c], "{report:?}");
    (reg, c)
}

/// The venue's refusal of the cancel of `c` sent under `rpc` at `now`: it already ended.
fn cancel_refused_closed(reg: &mut Registry, c: ClientOrderId, v: &str, rpc: u64, now: u64) {
    reg.cancel_sent(c, RpcId(rpc), MonoNs(now)).unwrap();
    reg.on_outcome(
        c,
        OrderOp::Cancel(RpcId(rpc)),
        &ItemRef {
            idx: 0,
            cid: Some(c),
            vid: Some(vid(v)),
        },
        &SubmitOutcome::Rejected(Reject {
            kind: RejectKind::AlreadyTerminal(TerminalHint::Canceled),
            venue_code: None,
            raw: "ALREADY_CLOSED".into(),
        }),
        MonoNs(now + 10),
    )
    .unwrap();
}

#[test]
fn a_lost_order_the_venue_reported_ended_is_not_registered_again_from_a_lagging_snapshot() {
    // Reviewer B's RB840-11 on PR #171: the ladder ended our buy Lost; the venue's own
    // Canceled update for it then arrives, too late to apply. A lagging snapshot at 3 000 still
    // shows it open: the venue said it ended, so it is no orphan. Nothing is registered, it
    // counts on neither cap, and the adding side is not suppressed.
    let (mut reg, c) = ended_lost();
    let mut ended = update(Some(c), canceled(), 0);
    ended.vid = Some(vid("l"));
    reg.apply_update(&ended, key(2_500));
    let report = resync_at(
        &mut reg,
        SnapshotSource::Trustworthy,
        3_000,
        vec![ours(c, "l", Side::Buy)],
        0,
    );
    assert!(report.orphans.is_empty(), "{report:?}");
    assert!(reg.orphans(INST).is_empty());
    assert_eq!(
        reg.get(c).unwrap().state(),
        OrdState::Terminal(TerminalKind::Lost)
    );
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(0)));
    place(&mut reg, buy(1)).unwrap();
}

#[test]
fn an_orphans_cancel_refused_as_already_ended_puts_it_on_the_ladder_so_a_trustworthy_absence_ends_it()
 {
    // Reviewer B's RB840-11 on PR #171: an orphan, here the Lost buy a lagging snapshot shows
    // open, has its cancel refused because the venue already ended it. It was registered from a
    // snapshot, so no terminal event of it may come: it goes on the Unknown ladder, and the
    // trustworthy snapshot at 4 000 that does not show it ends it Lost. Its market holds no
    // orphan from then on, and a later lagging snapshot showing it open again under the id the
    // venue said ended registers nothing.
    let (mut reg, c) = ended_lost();
    resync_at(
        &mut reg,
        SnapshotSource::Trustworthy,
        3_000,
        vec![ours(c, "l", Side::Buy)],
        0,
    );
    assert_eq!(reg.orphans(INST), vec![c]);
    let plan = reg.cancel_orphans(INST, &cfg(), &venue(), MonoNs(3_100));
    assert_explicit(&plan, &["l"], None);
    cancel_refused_closed(&mut reg, c, "l", 9, 3_200);
    assert!(
        reg.get(c).unwrap().unknown_since().is_some(),
        "on the ladder"
    );
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(5)));

    let report = resync_at(&mut reg, SnapshotSource::Trustworthy, 4_000, vec![], 0);
    assert_eq!(report.ladder.lost, vec![c], "{report:?}");
    assert!(report.orphans.is_empty(), "{report:?}");
    assert!(reg.orphans(INST).is_empty());
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(0)));

    let report = resync_at(
        &mut reg,
        SnapshotSource::Trustworthy,
        5_000,
        vec![ours(c, "l", Side::Buy)],
        0,
    );
    assert!(report.orphans.is_empty(), "{report:?}");
    assert!(reg.get(c).unwrap().state().is_terminal());
    let until = 3_100 + u64::try_from(COOLDOWN.as_nanos()).unwrap();
    assert_eq!(reg.end_orphan_cooldowns(MonoNs(until)), vec![INST]);
    place(&mut reg, buy(1)).unwrap();
}

#[test]
fn a_first_registered_orphans_cancel_refused_as_already_ended_puts_it_on_the_ladder() {
    // Reviewer B's RB840-11 on PR #171, for an orphan the registry never held: its cancel is
    // refused as already ended, so it goes on the Unknown ladder and the next trustworthy
    // snapshot not showing it ends it Lost.
    let mut reg = seeded(0);
    let o = cid();
    resync_at(
        &mut reg,
        SnapshotSource::Trustworthy,
        2_000,
        vec![ours(o, "o", Side::Buy)],
        0,
    );
    assert_eq!(reg.orphans(INST), vec![o]);
    reg.cancel_orphans(INST, &cfg(), &venue(), MonoNs(2_100));
    cancel_refused_closed(&mut reg, o, "o", 4, 2_200);
    let report = resync_at(&mut reg, SnapshotSource::Trustworthy, 3_000, vec![], 0);
    assert_eq!(report.ladder.lost, vec![o], "{report:?}");
    assert!(report.orphans.is_empty(), "{report:?}");
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(0)));
}

#[test]
fn a_place_authorized_during_the_cooldown_is_refused_at_submit_once_a_resync_shows_an_orphan_grown()
{
    // Codex's r4317002497 on PR #171: long 20, an orphan sell of 5 suppresses the adding side
    // and a sell of 15 is authorized within the position's size. The next resync shows the
    // orphan grown to 8 (the first snapshot lagged an earlier run's amend): with the sell it
    // would take the position past zero, so the authorization is refused at submit.
    let mut reg = seeded(20);
    arm::start(&mut reg, INST);
    let o = cid();
    let shown = |qty: i64| VenueOrderSnapshot {
        qty: lots(qty),
        ..ours(o, "o", Side::Sell)
    };
    resync_at(
        &mut reg,
        SnapshotSource::Trustworthy,
        2_000,
        vec![shown(5)],
        20,
    );
    assert_eq!(reg.orphans(INST), vec![o]);
    let cmd = reg.place(sell(15)).unwrap();
    let auth = reg.authorize(arm::TEST_ACCT, cmd).unwrap();
    assert_eq!(auth.check_at_submit(), Ok(()));
    resync_at(
        &mut reg,
        SnapshotSource::Trustworthy,
        3_000,
        vec![shown(8)],
        20,
    );
    assert_eq!(reg.resting_on(INST, Side::Sell), Some(lots(23)));
    assert!(matches!(
        auth.check_at_submit(),
        Err(StaleAuthorization::StateChanged { market: INST, .. })
    ));
}

#[test]
fn ending_the_cooldowns_reaps_a_dropped_place_first() {
    // Codex's r4317002504 on PR #171: a place built and dropped unspent is released by the
    // next mutating call, ending the orphan cooldowns included.
    let mut reg = seeded(0);
    arm::start(&mut reg, INST);
    let order = buy(5);
    let c = order.cid;
    let cmd = reg.place(order).unwrap();
    drop(cmd);
    reg.end_orphan_cooldowns(MonoNs(1));
    assert!(reg.get(c).unwrap().state().is_terminal());
    assert_eq!(reg.resting_on(INST, Side::Buy), Some(lots(0)));
}
