//! A place or amend that could add to the position is refused on a market while an order not
//! ours is shown resting there (decision 0158, FBC-7793):
//!
//! - an order under another namespace's, a non-canonical or no client id, under a venue id no
//!   order the registry holds had (or one of an order it holds as ended Lost or never sent),
//!   shown open by an order event or by a resync's snapshot from any source, holds every place,
//!   batch item and amend on its market at build and at authorization, and one authorized before
//!   it that the refusal would have refused is refused at submit (items 1, 2 and 7);
//! - an order on the side that reduces a known, non-zero position within its size, or carrying
//!   the venue's reduce-only flag there, every cancel and every order Exit admits pass it (items
//!   2 and 3);
//! - it lifts by the order's end under its venue id, a trustworthy resync requested at least the
//!   venue's lag bound after its stamp that does not list it open, or its venue id coming to be
//!   held as an order of ours not ended Lost or never sent; a venue id an order ended under is
//!   kept as ended until a retention pass forgets it (items 4 to 6);
//! - a fill shows nothing resting;
//! - the execution planner cancels an order the consumer moves that the refusal would refuse as
//!   it rests, and plans its new place once it has ended, on a venue that amends too, while an
//!   order the refusal admits as it rests keeps its amend; it never batches a crossing intent,
//!   nor, under the refusal, a place carrying the venue's flag, and it emits the places held for
//!   a batch before any such lone place or amend is built (item 2 and 3, decisions 0065, 0111).
//!
//! The orders are built as order events and snapshot rows directly, since the conformance toy
//! cannot express a missing client id. Every number is a test input on a synthetic market: a lag
//! bound of 200 ms, a resync bound of 300 ms and caps of 100 lots.

#[path = "common/arm.rs"]
mod arm;
mod common;
#[path = "common/venue.rs"]
mod pass_venue;

use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::time::Duration;

use arm::{TEST_ACCT, place_issued};
use common::{canceled, cid, fill, fill_id, lots, order_caps, placement, rejected, vid};
use fbc_core::{
    AccountKey, AckLevel, AmendAck, AmendCaps, AmendQty, Batch, Bps, CancelBatch, Channel,
    CidMatch, CidMint, ClientOrderId, Feature, FillIdent, InstrumentId, ItemRef, MonoNs, Namespace,
    NamespaceLease, NewOrder, NotSentReason, OrderCaps, OrderKind, OrderRef, OrderUpdate, RefKind,
    RejectKind, RpcId, Side, SignedLots, SnapshotSource, SubmitOutcome, Support, TagSet, Ticks,
    Tif, TifTag, VenueCommand, VenueOrderSnapshot, VenueOrderState, WallNs,
};
use fbc_oms::{
    Admission, AmendRefusal, Batching, CROSSING_LEVEL, CancelAllRefusal, CancelChoice,
    CancelEverything, CrossingIntent, DesiredBook, DesiredQuote, EntryState, ExecutionPlanner,
    FillLedger, FillRouted, Handed, IssueRefusal, LadderConfig, LedgerConfig, MarketCapsConfig,
    NotOursShown, OmsError, OrdState, OrderKey, OrderOp, OrphanCancels, OrphanConfig,
    PermittedCommand, Plan, PlanRefusal, PlanVenue, PlannerConfig, PreTradeCaps, ReduceFallback,
    Registry, ResyncReport, ResyncSnapshot, RetentionConfig, Stage, StaleAuthorization,
    TerminalKind,
};

const INST: InstrumentId = InstrumentId::new(1);
/// Another engine's namespace on the account.
const THEIRS: Namespace = Namespace::new(9);
/// The venue's declared snapshot lag bound in these tests.
const LAG: Duration = Duration::from_millis(200);
/// The consumer's resync bound in these tests.
const RESYNC: Duration = Duration::from_millis(300);
/// Both caps, in lots.
const CAP: i64 = 100;
/// One nanosecond.
const NS: Duration = Duration::from_nanos(1);

/// `n` milliseconds on the shard's monotonic clock.
fn ms(n: u64) -> MonoNs {
    MonoNs(n * 1_000_000)
}

/// The synthetic venue: GTC and IOC limits, amends of price and quantity by venue id, cancels
/// and queries by venue or client id, a trustworthy snapshot lagging at most [`LAG`].
fn venue() -> OrderCaps {
    OrderCaps {
        tifs: TagSet::of(&[TifTag::Gtc, TifTag::Ioc]),
        cancel_refs: TagSet::of(&[RefKind::Venue, RefKind::Client]),
        query_refs: TagSet::of(&[RefKind::Venue, RefKind::Client]),
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
        snapshot_lag_bound: LAG,
        ..order_caps()
    }
}

/// [`venue`] with an untrustworthy snapshot source.
fn untrustworthy() -> OrderCaps {
    OrderCaps {
        snapshot_source: SnapshotSource::Untrustworthy,
        ..venue()
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

/// An ingest order no other event of this test binary shares.
fn key() -> OrderKey {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    OrderKey {
        venue: None,
        ingest: NEXT.fetch_add(1, Ordering::Relaxed),
    }
}

/// A snapshot requested at `at`, its watermark the same instant (the last one the wall clock
/// holds past it), of `orders` and the position `pos` on [`INST`].
fn snap(at: MonoNs, orders: Vec<VenueOrderSnapshot>, pos: i64) -> ResyncSnapshot {
    ResyncSnapshot {
        watermark: WallNs(i64::try_from(at.0).unwrap_or(i64::MAX)),
        requested_at: at,
        orders,
        positions: vec![(INST, SignedLots(pos))],
    }
}

fn resync_on(
    reg: &mut Registry,
    caps: &OrderCaps,
    at: MonoNs,
    orders: Vec<VenueOrderSnapshot>,
    pos: i64,
) -> ResyncReport {
    reg.resync(&ladder_cfg(), caps, &snap(at, orders, pos), key())
        .unwrap()
}

/// A trustworthy resync of [`venue`] at `at`.
fn resync(
    reg: &mut Registry,
    at: MonoNs,
    orders: Vec<VenueOrderSnapshot>,
    pos: i64,
) -> ResyncReport {
    resync_on(reg, &venue(), at, orders, pos)
}

/// A named registry under both caps, seeded at `pos` by a trustworthy resync of `caps`
/// requested at 1 ms, armed and Quoting.
fn seeded_on(caps: &OrderCaps, pos: i64) -> Registry {
    let mut reg = arm::named(Registry::with_caps(
        PreTradeCaps::new()
            .with_market(
                INST,
                MarketCapsConfig {
                    inventory: Some(lots(CAP)),
                    resting: Some(lots(CAP)),
                },
            )
            .unwrap(),
    ));
    let report = resync_on(&mut reg, caps, ms(1), vec![], pos);
    assert_eq!(report.seeded, vec![(INST, SignedLots(pos))]);
    arm::start(&mut reg, INST);
    assert_eq!(reg.entry(INST).state(), EntryState::Quoting);
    reg
}

fn seeded(pos: i64) -> Registry {
    seeded_on(&venue(), pos)
}

/// The client ids an order not ours carries: none, another namespace's, a non-canonical one.
fn kinds() -> [Option<CidMatch>; 3] {
    [
        None,
        Some(CidMatch::Foreign(THEIRS)),
        Some(CidMatch::Unparseable),
    ]
}

/// An order event on [`INST`], a buy, under `cid` and `v`, in `state`.
fn event(cid: Option<CidMatch>, v: Option<&str>, state: VenueOrderState) -> OrderUpdate {
    OrderUpdate {
        cid,
        vid: v.map(vid),
        inst: INST,
        side: Side::Buy,
        state,
        cum_filled: lots(0),
        px: None,
        qty: None,
        post_only: None,
        reduce_only: None,
    }
}

/// Applies [`event`].
fn show(reg: &mut Registry, cid: Option<CidMatch>, v: Option<&str>, state: VenueOrderState) {
    reg.apply_update(&event(cid, v, state), key());
}

/// An order not ours, a buy of 5 under `cid` (none: no client id), open under `v`.
fn not_ours(cid: Option<CidMatch>, v: &str) -> VenueOrderSnapshot {
    VenueOrderSnapshot {
        cid,
        vid: vid(v),
        inst: INST,
        side: Side::Buy,
        state: VenueOrderState::Open,
        px: Some(Ticks(100)),
        qty: lots(5),
        cum_filled: lots(0),
        post_only: None,
        reduce_only: None,
    }
}

/// Our order `c`, a buy of 5, open under `v` as a snapshot shows it.
fn ours_row(c: ClientOrderId, v: &str) -> VenueOrderSnapshot {
    not_ours(Some(CidMatch::Ours(c)), v)
}

/// A limit buy of `qty` at 100.
fn bid(qty: i64) -> NewOrder {
    placement(cid(), 100, qty)
}

/// A limit sell of `qty` at 101, neither reduce-only nor classified reducing.
fn ask(qty: i64) -> NewOrder {
    NewOrder {
        side: Side::Sell,
        kind: OrderKind::Limit { px: Ticks(101) },
        ..placement(cid(), 101, qty)
    }
}

/// [`ask`], classified reducing, without the venue's flag.
fn reducing_ask(qty: i64) -> NewOrder {
    NewOrder {
        reducing: true,
        ..ask(qty)
    }
}

/// [`ask`], carrying the venue's reduce-only flag.
fn flagged_ask(qty: i64) -> NewOrder {
    NewOrder {
        reduce_only: true,
        ..ask(qty)
    }
}

/// A crossing intent's order: a reduce-only IOC sell of `qty` through the touch, classified
/// reducing (decision 0110).
fn crossing(qty: i64) -> NewOrder {
    NewOrder {
        tif: Tif::Ioc,
        post_only: false,
        reduce_only: true,
        reducing: true,
        ..ask(qty)
    }
}

/// Builds and authorizes the place of `order`, handed to a gateway, or says why it was refused.
fn place(reg: &mut Registry, order: NewOrder) -> Result<(), OmsError> {
    reg.place(order).map(|cmd| arm::issue(reg, cmd))
}

/// Authorizes `cmd` and hands it to a gateway, which checks it at submit.
fn authorize(reg: &mut Registry, cmd: PermittedCommand) -> Handed {
    reg.authorize(TEST_ACCT, cmd).unwrap().hand_over()
}

/// Authorizes `cmd` and checks it at submit: it passes.
fn passes(reg: &mut Registry, cmd: PermittedCommand) {
    let what = format!("{:?}", cmd.command());
    assert_eq!(authorize(reg, cmd).check_at_submit(), Ok(()), "{what}");
}

fn accepted() -> SubmitOutcome {
    SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    }
}

fn item(c: ClientOrderId, v: Option<&str>) -> ItemRef {
    ItemRef {
        idx: 0,
        cid: Some(c),
        vid: v.map(vid),
    }
}

/// Places `order`, authorizes it and has the venue accept it under `v`: Open.
fn open(reg: &mut Registry, order: NewOrder, v: &str) -> ClientOrderId {
    let c = order.cid;
    place_issued(reg, order);
    reg.on_outcome(c, OrderOp::Place, &item(c, Some(v)), &accepted(), ms(1))
        .unwrap();
    assert_eq!(reg.get(c).unwrap().state(), OrdState::Open);
    c
}

fn refused(side: Side) -> OmsError {
    OmsError::NotOursResting { inst: INST, side }
}

fn shown(ids: &[&str], nameless: bool) -> NotOursShown {
    NotOursShown {
        ids: ids.iter().map(|v| vid(v)).collect(),
        nameless,
    }
}

fn ledger() -> FillLedger {
    FillLedger::new(
        LedgerConfig {
            max_age: Duration::from_secs(3_600),
            max_entries: 10_000,
        },
        WallNs(0),
    )
    .unwrap()
}

/// A fill id no other fill of this test binary shares.
fn fid() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!("f-{}", NEXT.fetch_add(1, Ordering::Relaxed))
}

/// A live buy fill of 1 lot under `cid` naming `v`, applied through `l` arriving at `at`.
fn fill_of(
    reg: &mut Registry,
    l: &mut FillLedger,
    cid: Option<CidMatch>,
    v: &str,
    at: MonoNs,
) -> FillRouted {
    let mut f = fill(
        None,
        FillIdent::Venue {
            fill: fill_id(&fid()),
            vid: Some(vid(v)),
            cum_after: None,
        },
        Side::Buy,
        1,
        false,
    );
    if let fbc_core::FillOrigin::Order { cid: named, .. } = &mut f.origin {
        *named = cid;
    }
    match l.admit(&f, None, at) {
        Admission::Apply(a) => reg.apply_fill(a).unwrap(),
        other => panic!("expected the fill accepted, got {other:?}"),
    }
}

/// Puts our order `c`, placed and authorized, on the Unknown ladder and ends it Lost on a
/// trustworthy snapshot requested at `at` that does not show it.
fn lose(reg: &mut Registry, c: ClientOrderId, at: MonoNs) {
    reg.placement_sent(c, ms(2), WallNs(2_000_000)).unwrap();
    reg.on_outcome(
        c,
        OrderOp::Place,
        &item(c, None),
        &SubmitOutcome::Unknown,
        ms(2),
    )
    .unwrap();
    let report = resync(reg, at, vec![], 0);
    assert_eq!(report.ladder.lost, vec![c]);
    assert_eq!(
        reg.get(c).unwrap().state(),
        OrdState::Terminal(TerminalKind::Lost)
    );
}

// ---- Test 1: refused, for each kind and each source ----

/// How an order not ours is shown open in [`refused_for_each_kind_and_each_source`].
#[derive(Copy, Clone, Debug)]
enum Source {
    Open,
    AmendedInPlace,
    TrustworthyRow,
    UntrustworthyRow,
}

fn sight(reg: &mut Registry, source: Source, cid: Option<CidMatch>, v: &str) {
    match source {
        Source::Open => show(reg, cid, Some(v), VenueOrderState::Open),
        Source::AmendedInPlace => show(
            reg,
            cid,
            Some(v),
            VenueOrderState::Amended { new_vid: None },
        ),
        Source::TrustworthyRow => {
            resync(reg, ms(50), vec![not_ours(cid, v)], 0);
        }
        Source::UntrustworthyRow => {
            resync_on(reg, &untrustworthy(), ms(50), vec![not_ours(cid, v)], 0);
        }
    }
}

#[test]
fn a_place_a_batch_item_and_an_amend_are_refused_for_each_kind_and_each_source() {
    let sources = [
        Source::Open,
        Source::AmendedInPlace,
        Source::TrustworthyRow,
        Source::UntrustworthyRow,
    ];
    for cid_kind in kinds() {
        for source in sources {
            let what = format!("{cid_kind:?} by {source:?}");
            let mut reg = seeded(0);
            let ours = open(&mut reg, bid(1), "ours-1");
            sight(&mut reg, source, cid_kind, "v");
            assert!(reg.not_ours_resting(INST), "{what}");
            // The view of orders not ours holds it too, as before.
            assert!(reg.foreign_in_view(INST), "{what}");
            assert_eq!(reg.not_ours_shown(INST), shown(&["v"], false), "{what}");
            assert_eq!(place(&mut reg, bid(1)), Err(refused(Side::Buy)), "{what}");
            let batched = bid(1);
            let plan = reg.place_batch(vec![batched.clone()]).unwrap();
            assert!(plan.command.is_none(), "{what}");
            assert_eq!(
                plan.refused,
                vec![(batched.cid, refused(Side::Buy))],
                "{what}"
            );
            let amend = reg
                .live(ours)
                .unwrap()
                .amend(&venue(), Ticks(100), lots(2), false);
            assert_eq!(
                amend.err(),
                Some(AmendRefusal::NotOursResting {
                    inst: INST,
                    side: Side::Buy
                }),
                "{what}"
            );
            // Ended under its venue id, it holds nothing.
            show(&mut reg, cid_kind, Some("v"), canceled());
            assert_eq!(reg.not_ours_shown(INST), shown(&[], false), "{what}");
            place(&mut reg, bid(1)).unwrap();
        }
    }
}

#[test]
fn an_order_not_ours_shown_with_no_venue_id_holds_the_market_and_is_reported_nameless() {
    let mut reg = seeded(0);
    show(&mut reg, None, None, VenueOrderState::Open);
    assert!(reg.not_ours_resting(INST));
    assert_eq!(reg.not_ours_shown(INST), shown(&[], true));
    assert_eq!(place(&mut reg, bid(1)), Err(refused(Side::Buy)));
    // An end with no venue id names nothing to end.
    show(&mut reg, None, None, canceled());
    assert!(reg.not_ours_resting(INST));
    // Named beside it, by venue id in order.
    show(&mut reg, None, Some("b"), VenueOrderState::Open);
    show(&mut reg, None, Some("a"), VenueOrderState::Open);
    assert_eq!(reg.not_ours_shown(INST), shown(&["a", "b"], true));
}

#[test]
fn each_refusal_says_what_it_refused() {
    let texts = [
        refused(Side::Buy).to_string(),
        IssueRefusal::NotOursResting {
            inst: INST,
            side: Side::Sell,
        }
        .to_string(),
        StaleAuthorization::NotOursResting(INST).to_string(),
    ];
    for text in texts {
        assert!(text.contains("not ours"), "{text}");
    }
}

#[test]
fn an_order_under_the_venue_id_of_an_order_we_hold_is_that_order_whatever_its_client_id() {
    for cid_kind in [Some(CidMatch::Unparseable), Some(CidMatch::Foreign(THEIRS))] {
        let mut reg = seeded(0);
        open(&mut reg, bid(1), "ours-1");
        show(&mut reg, cid_kind, Some("ours-1"), VenueOrderState::Open);
        assert!(!reg.not_ours_resting(INST), "{cid_kind:?} by an event");
        resync(&mut reg, ms(50), vec![not_ours(cid_kind, "ours-1")], 0);
        assert!(!reg.not_ours_resting(INST), "{cid_kind:?} by a row");
        place(&mut reg, bid(1)).unwrap();
    }
}

#[test]
fn an_orphan_cooldown_is_reported_before_an_order_not_ours_at_build_and_at_authorization() {
    let mut reg = seeded(0);
    let early = reg.place(bid(1)).unwrap();
    let orphan = cid();
    resync(
        &mut reg,
        ms(50),
        vec![ours_row(orphan, "o-1"), not_ours(None, "v")],
        0,
    );
    assert!(reg.orphan_cooldown(INST).is_some() && reg.not_ours_resting(INST));
    let cooldown = OmsError::OrphanCooldown {
        inst: INST,
        side: Side::Buy,
    };
    assert_eq!(place(&mut reg, bid(1)), Err(cooldown));
    assert_eq!(
        reg.authorize(TEST_ACCT, early).err(),
        Some(IssueRefusal::OrphanCooldown {
            inst: INST,
            side: Side::Buy
        })
    );
}

// ---- Test 2: still built ----

#[test]
fn cancels_of_every_kind_are_built_and_pass_at_submit_while_an_order_not_ours_rests() {
    let caps = venue();
    let mut reg = seeded(0);
    let ours = open(&mut reg, bid(1), "ours-1");
    show(&mut reg, None, Some("v"), VenueOrderState::Open);
    assert!(reg.not_ours_resting(INST));
    match reg.cancellable(ours).unwrap().cancel(&caps) {
        CancelChoice::Send(cmd) => passes(&mut reg, cmd),
        CancelChoice::AwaitAck => panic!("the cancel names the venue id"),
    }
    let plan = reg.cancel_many(&[ours], &caps);
    assert!(plan.refused.is_empty() && !plan.commands.is_empty());
    for cmd in plan.commands {
        passes(&mut reg, cmd);
    }
    let CancelEverything::Explicit { plan, why } = reg.cancel_everything(INST, &caps) else {
        panic!("the venue has no instrument cancel-all");
    };
    assert_eq!(why, CancelAllRefusal::Unsupported);
    assert!(!plan.commands.is_empty());
    for cmd in plan.commands {
        passes(&mut reg, cmd);
    }
}

#[test]
fn orphan_cancels_are_built_beside_an_order_not_ours_and_no_cancel_all_while_it_is_in_view() {
    let caps = OrderCaps {
        cancel_all_instrument: Support::Native,
        batch_cancel: Some(CancelBatch {
            max_items: 4,
            refs: TagSet::of(&[RefKind::Venue]),
        }),
        ..venue()
    };
    let mut reg = seeded_on(&caps, 0);
    let (a, b) = (cid(), cid());
    resync_on(
        &mut reg,
        &caps,
        ms(50),
        vec![
            ours_row(a, "o-1"),
            ours_row(b, "o-2"),
            not_ours(Some(CidMatch::Foreign(THEIRS)), "v"),
        ],
        0,
    );
    assert!(reg.not_ours_resting(INST) && reg.orphan_cooldown(INST).is_some());
    let within = OrphanConfig::new(Duration::from_secs(5), 2).unwrap();
    let plan = reg.cancel_orphans(INST, &within, &caps, ms(60));
    let OrphanCancels::Explicit {
        plan: cancels,
        escalation: None,
    } = plan.cancels
    else {
        panic!("expected explicit cancels within the level limit, got {plan:?}");
    };
    assert_eq!(cancels.commands.len(), 1);
    assert!(matches!(
        cancels.commands[0].command(),
        VenueCommand::CancelMany(items) if items.len() == 2
    ));
    for cmd in cancels.commands {
        passes(&mut reg, cmd);
    }
    // Above the level limit, the order not ours in view keeps the cancels explicit.
    let above = OrphanConfig::new(Duration::from_secs(5), 1).unwrap();
    let plan = reg.cancel_orphans(INST, &above, &caps, ms(61));
    let OrphanCancels::Explicit {
        plan: cancels,
        escalation: Some(CancelAllRefusal::ForeignInView),
    } = plan.cancels
    else {
        panic!("expected explicit cancels above the level limit, got {plan:?}");
    };
    for cmd in cancels.commands {
        passes(&mut reg, cmd);
    }
}

#[test]
fn a_tombstone_cancel_is_built_and_passes_at_submit_while_an_order_not_ours_rests() {
    let caps = venue();
    let mut reg = seeded(0);
    let c = bid(1).cid;
    place_issued(&mut reg, placement(c, 100, 1));
    reg.placement_sent(c, ms(2), WallNs(2_000_000)).unwrap();
    reg.on_outcome(
        c,
        OrderOp::Place,
        &item(c, None),
        &SubmitOutcome::Unknown,
        ms(2),
    )
    .unwrap();
    show(&mut reg, None, Some("v"), VenueOrderState::Open);
    assert!(reg.not_ours_resting(INST));
    let cfg = LadderConfig::new(
        Duration::from_millis(100),
        Duration::ZERO,
        Duration::from_secs(1),
        2,
    )
    .unwrap();
    reg.ladder(&cfg, &caps, ms(3));
    let plan = reg.ladder(&cfg, &caps, ms(2_000));
    assert_eq!(plan.tombstones.len(), 1);
    let (named, cmd) = plan.tombstones.into_iter().next().unwrap();
    assert_eq!(named, c);
    assert!(matches!(
        cmd.command(),
        VenueCommand::Cancel(cancel) if cancel.target == OrderRef::Client(c)
    ));
    passes(&mut reg, cmd);
}

#[test]
fn flatten_reaches_exit_and_exit_places_and_amends_are_built_and_pass() {
    let caps = venue();
    let mut reg = seeded(10);
    let resting = open(&mut reg, reducing_ask(5), "ask-1");
    show(&mut reg, None, Some("v"), VenueOrderState::Open);
    reg.flatten(INST, fbc_oms::Leases::none()).unwrap();
    assert!(matches!(reg.entry(INST).state(), EntryState::Exit(_)));
    let exit = NewOrder {
        reduce_only: true,
        ..reducing_ask(3)
    };
    let cmd = reg.place(exit).unwrap();
    passes(&mut reg, cmd);
    let cmd = reg
        .live(resting)
        .unwrap()
        .amend(&caps, Ticks(102), lots(4), true)
        .unwrap();
    passes(&mut reg, cmd);
}

#[test]
fn an_order_reducing_the_position_within_its_size_is_built_in_quoting_and_one_past_it_is_not() {
    let mut reg = seeded(10);
    show(&mut reg, None, Some("v"), VenueOrderState::Open);
    let cmd = reg.place(reducing_ask(5)).unwrap();
    passes(&mut reg, cmd);
    // An ordinary sell within what is left of the position's size.
    let cmd = reg.place(ask(5)).unwrap();
    passes(&mut reg, cmd);
    // One more lot takes the side past the position's size.
    assert_eq!(place(&mut reg, ask(1)), Err(refused(Side::Sell)));
    assert_eq!(place(&mut reg, reducing_ask(1)), Err(refused(Side::Sell)));
}

/// Long 10, our ordinary asks of 2 and 2 resting, an order not ours shown resting when `held`.
fn long_with_asks(held: bool) -> Registry {
    let mut reg = seeded(10);
    place(&mut reg, ask(2)).unwrap();
    place(&mut reg, ask(2)).unwrap();
    if held {
        show(&mut reg, None, Some("v"), VenueOrderState::Open);
        assert!(reg.not_ours_resting(INST));
    }
    reg
}

#[test]
fn an_order_carrying_the_venue_flag_passes_on_the_reducing_side_alone_whatever_its_size() {
    let mut reg = long_with_asks(true);
    let cmd = reg.place(flagged_ask(10)).unwrap();
    passes(&mut reg, cmd);
    let mut reg = long_with_asks(true);
    assert_eq!(place(&mut reg, reducing_ask(10)), Err(refused(Side::Sell)));
    let flagged_bid = NewOrder {
        reduce_only: true,
        ..bid(1)
    };
    assert_eq!(place(&mut reg, flagged_bid), Err(refused(Side::Buy)));
    // On a flat position nothing reduces.
    let mut flat = seeded(0);
    show(&mut flat, None, Some("v"), VenueOrderState::Open);
    assert_eq!(place(&mut flat, flagged_ask(1)), Err(refused(Side::Sell)));
}

// ---- Test 3: lifted by a terminal event ----

#[test]
fn each_terminal_event_under_its_venue_id_lifts_the_refusal() {
    let ends = [
        VenueOrderState::Filled,
        canceled(),
        rejected(RejectKind::PostOnlyWouldCross),
        VenueOrderState::Expired,
    ];
    for end in ends {
        let mut reg = seeded(0);
        show(&mut reg, None, Some("v"), VenueOrderState::Open);
        assert_eq!(place(&mut reg, bid(1)), Err(refused(Side::Buy)));
        show(&mut reg, None, Some("v"), end.clone());
        assert!(!reg.not_ours_resting(INST), "{end:?}");
        place(&mut reg, bid(CAP)).unwrap();
    }
}

#[test]
fn an_amend_under_a_new_venue_id_moves_the_sighting_and_its_end_there_lifts_it() {
    let moved = |to: &str| VenueOrderState::Amended {
        new_vid: Some(vid(to)),
    };
    let mut reg = seeded(0);
    show(&mut reg, None, Some("v"), VenueOrderState::Open);
    show(&mut reg, None, Some("v"), moved("v2"));
    assert_eq!(reg.not_ours_shown(INST), shown(&["v2"], false));
    show(&mut reg, None, Some("v"), canceled());
    assert!(reg.not_ours_resting(INST));
    show(&mut reg, None, Some("v2"), canceled());
    assert!(!reg.not_ours_resting(INST));
    place(&mut reg, bid(CAP)).unwrap();
    // First seen as an amend under a new id, its old id never sighted: held under the new id.
    let mut reg = seeded(0);
    show(
        &mut reg,
        Some(CidMatch::Foreign(THEIRS)),
        Some("w"),
        moved("w2"),
    );
    assert_eq!(reg.not_ours_shown(INST), shown(&["w2"], false));
    assert_eq!(place(&mut reg, bid(1)), Err(refused(Side::Buy)));
}

// ---- Test 4: lifted by a trustworthy resync ----

#[test]
fn a_trustworthy_resync_lifts_a_sighting_only_once_requested_the_lag_bound_after_its_stamp() {
    let mut reg = seeded(0);
    show(&mut reg, None, Some("v"), VenueOrderState::Open);
    // Not yet stamped: no resync lifts it, however late.
    let report = resync(&mut reg, ms(50), vec![], 0);
    assert!(report.not_ours_lifted.is_empty() && reg.not_ours_resting(INST));
    let at = ms(100);
    reg.order_entry_epoch_ended(at);
    let report = resync(&mut reg, at + (LAG - NS), vec![], 0);
    assert!(report.not_ours_lifted.is_empty() && reg.not_ours_resting(INST));
    let report = resync(&mut reg, at + LAG, vec![], 0);
    assert_eq!(report.not_ours_lifted, vec![(INST, Some(vid("v")))]);
    assert!(!reg.not_ours_resting(INST));
    place(&mut reg, bid(CAP)).unwrap();
}

#[test]
fn a_sighting_shown_again_after_its_stamp_waits_for_the_next_one() {
    let mut reg = seeded(0);
    show(&mut reg, None, Some("v"), VenueOrderState::Open);
    let at = ms(100);
    reg.order_entry_epoch_ended(at);
    show(&mut reg, None, Some("v"), VenueOrderState::Open);
    resync(&mut reg, at + LAG, vec![], 0);
    assert!(reg.not_ours_resting(INST));
    let at2 = ms(400);
    reg.order_entry_epoch_ended(at2);
    resync(&mut reg, at2 + (LAG - NS), vec![], 0);
    assert!(reg.not_ours_resting(INST));
    let report = resync(&mut reg, at2 + LAG, vec![], 0);
    assert_eq!(report.not_ours_lifted, vec![(INST, Some(vid("v")))]);
    assert!(!reg.not_ours_resting(INST));
}

#[test]
fn a_resync_still_listing_the_venue_id_open_keeps_the_refusal_under_any_client_id() {
    for listed in [Some(CidMatch::Foreign(THEIRS)), None] {
        let mut reg = seeded(0);
        show(&mut reg, None, Some("v"), VenueOrderState::Open);
        reg.order_entry_epoch_ended(ms(100));
        let report = resync(&mut reg, ms(1_000), vec![not_ours(listed, "v")], 0);
        assert!(report.not_ours_lifted.is_empty(), "{listed:?}");
        assert!(reg.not_ours_resting(INST), "{listed:?}");
    }
    // Under one of our client ids the registry does not hold, on a market the resync finds
    // stale (requested before the seed's request), so it registers no orphan: still listed.
    let mut reg = arm::named(Registry::with_caps(
        PreTradeCaps::new()
            .with_market(
                INST,
                MarketCapsConfig {
                    inventory: Some(lots(CAP)),
                    resting: Some(lots(CAP)),
                },
            )
            .unwrap(),
    ));
    resync(&mut reg, ms(10_000), vec![], 0);
    show(&mut reg, None, Some("v"), VenueOrderState::Open);
    reg.order_entry_epoch_ended(ms(100));
    let unheld = cid();
    let report = resync(&mut reg, ms(5_000), vec![ours_row(unheld, "v")], 0);
    assert!(report.not_ours_lifted.is_empty());
    assert!(reg.get(unheld).is_none() && reg.not_ours_resting(INST));
}

#[test]
fn an_untrustworthy_resync_never_lifts_a_sighting() {
    let mut reg = seeded(0);
    show(&mut reg, None, Some("v"), VenueOrderState::Open);
    reg.order_entry_epoch_ended(ms(100));
    let report = resync_on(&mut reg, &untrustworthy(), ms(10_000), vec![], 0);
    assert!(report.not_ours_lifted.is_empty() && reg.not_ours_resting(INST));
}

#[test]
fn a_nameless_sighting_lifts_the_same_way() {
    let mut reg = seeded(0);
    show(&mut reg, None, None, VenueOrderState::Open);
    let at = ms(100);
    reg.order_entry_epoch_ended(at);
    resync(&mut reg, at + (LAG - NS), vec![], 0);
    assert!(reg.not_ours_resting(INST));
    let report = resync(&mut reg, at + LAG, vec![], 0);
    assert_eq!(report.not_ours_lifted, vec![(INST, None)]);
    assert!(!reg.not_ours_resting(INST));
}

#[test]
fn a_retention_pass_stamps_a_sighting_too() {
    let cfg = RetentionConfig::new(10, Duration::from_secs(3_600), RESYNC).unwrap();
    let mut reg = seeded(0);
    show(&mut reg, None, Some("v"), VenueOrderState::Open);
    let at = ms(100);
    reg.retain_ended(&cfg, &venue(), at);
    resync(&mut reg, at + (LAG - NS), vec![], 0);
    assert!(reg.not_ours_resting(INST));
    let report = resync(&mut reg, at + LAG, vec![], 0);
    assert_eq!(report.not_ours_lifted, vec![(INST, Some(vid("v")))]);
}

#[test]
fn a_sighting_is_never_stamped_earlier_than_the_latest_end_reported() {
    let (t1, t2) = (ms(100), ms(500));
    let mut reg = seeded(0);
    reg.order_entry_epoch_ended(t2);
    show(&mut reg, None, Some("v"), VenueOrderState::Open);
    reg.order_entry_epoch_ended(t1);
    resync(&mut reg, t1 + LAG, vec![], 0);
    assert!(reg.not_ours_resting(INST));
    resync(&mut reg, t2 + (LAG - NS), vec![], 0);
    assert!(reg.not_ours_resting(INST));
    let report = resync(&mut reg, t2 + LAG, vec![], 0);
    assert_eq!(report.not_ours_lifted, vec![(INST, Some(vid("v")))]);
    // A retention pass's `now` below the latest end stamps at the end too.
    let cfg = RetentionConfig::new(10, Duration::from_secs(3_600), RESYNC).unwrap();
    show(&mut reg, None, Some("w"), VenueOrderState::Open);
    reg.retain_ended(&cfg, &venue(), t1);
    resync(&mut reg, t1 + LAG, vec![], 0);
    assert!(reg.not_ours_resting(INST));
    let report = resync(&mut reg, t2 + LAG, vec![], 0);
    assert_eq!(report.not_ours_lifted, vec![(INST, Some(vid("w")))]);
}

#[test]
fn a_lag_bound_past_the_clock_end_never_lifts_a_sighting() {
    let caps = OrderCaps {
        snapshot_lag_bound: Duration::MAX,
        ..venue()
    };
    let mut reg = seeded_on(&caps, 0);
    show(&mut reg, None, Some("v"), VenueOrderState::Open);
    reg.order_entry_epoch_ended(ms(100));
    let report = resync_on(&mut reg, &caps, MonoNs(u64::MAX), vec![], 0);
    assert!(report.not_ours_lifted.is_empty() && reg.not_ours_resting(INST));
}

// ---- Test 5: ended entries ----

#[test]
fn a_venue_id_an_order_not_ours_ended_under_shows_nothing_resting_until_the_lag_bound_past_it() {
    let mut reg = seeded(0);
    show(&mut reg, None, Some("v"), VenueOrderState::Open);
    show(&mut reg, None, Some("v"), canceled());
    let at = ms(100);
    reg.order_entry_epoch_ended(at);
    // A late open event adds nothing.
    show(&mut reg, None, Some("v"), VenueOrderState::Open);
    assert!(!reg.not_ours_resting(INST));
    // A lagging trustworthy row, requested before the end's stamp plus the bound, adds nothing.
    resync(&mut reg, at + (LAG - NS), vec![not_ours(None, "v")], 0);
    assert!(!reg.not_ours_resting(INST));
    // Nor does an untrustworthy one, however late.
    resync_on(
        &mut reg,
        &untrustworthy(),
        ms(10_000),
        vec![not_ours(None, "v")],
        0,
    );
    assert!(!reg.not_ours_resting(INST));
    // A trustworthy row requested at or after it shows it resting again.
    resync(&mut reg, at + LAG, vec![not_ours(None, "v")], 0);
    assert!(reg.not_ours_resting(INST));
    assert_eq!(place(&mut reg, bid(1)), Err(refused(Side::Buy)));
}

#[test]
fn an_ended_venue_id_is_forgotten_by_the_retention_pass_at_its_floor() {
    let cfg = RetentionConfig::new(10, Duration::from_secs(3_600), RESYNC).unwrap();
    let mut reg = seeded(0);
    show(&mut reg, None, Some("v"), VenueOrderState::Open);
    show(&mut reg, None, Some("v"), canceled());
    let at = ms(100);
    reg.retain_ended(&cfg, &venue(), at);
    reg.retain_ended(&cfg, &venue(), at + (LAG + RESYNC - NS));
    show(&mut reg, None, Some("v"), VenueOrderState::Open);
    assert!(!reg.not_ours_resting(INST));
    reg.retain_ended(&cfg, &venue(), at + LAG + RESYNC);
    show(&mut reg, None, Some("v"), VenueOrderState::Open);
    assert!(reg.not_ours_resting(INST));
}

#[test]
fn a_late_amend_of_an_order_kept_as_ended_shows_nothing_resting_under_its_new_venue_id() {
    for by in kinds() {
        let mut reg = seeded(0);
        show(&mut reg, by, Some("v"), VenueOrderState::Open);
        show(&mut reg, by, Some("v"), canceled());
        assert!(!reg.not_ours_resting(INST), "{by:?}");
        show(
            &mut reg,
            by,
            Some("v"),
            VenueOrderState::Amended {
                new_vid: Some(vid("v2")),
            },
        );
        assert!(!reg.not_ours_resting(INST), "{by:?}");
        place(&mut reg, bid(CAP)).unwrap();
    }
}

#[test]
fn a_venue_id_kept_as_ended_then_learnt_as_ours_takes_the_forget_stamp_once_forgotten() {
    let keep = RetentionConfig::new(10, Duration::from_millis(1), RESYNC).unwrap();
    let mut reg = seeded(0);
    show(&mut reg, None, Some("v"), VenueOrderState::Open);
    show(&mut reg, None, Some("v"), canceled());
    // Our order learns the venue id kept as ended, which drops the entry, then ends.
    let b = open(&mut reg, bid(1), "v");
    cancelled(&mut reg, b, Side::Buy, "v");
    let t1 = ms(10_000);
    reg.retain_ended(&keep, &venue(), t1);
    let forget = t1 + LAG + RESYNC;
    assert_eq!(reg.retain_ended(&keep, &venue(), forget).forgotten, vec![b]);
    // Kept as ended from the forget's `now`, not from the earlier entry's stamp.
    show(&mut reg, None, Some("v"), VenueOrderState::Open);
    assert!(!reg.not_ours_resting(INST));
    resync(&mut reg, forget + (LAG - NS), vec![not_ours(None, "v")], 0);
    assert!(!reg.not_ours_resting(INST));
    resync(&mut reg, forget + LAG, vec![not_ours(None, "v")], 0);
    assert!(reg.not_ours_resting(INST));
}

// ---- Test 6: dropped on learn ----

#[test]
fn an_outcome_naming_the_venue_id_as_ours_lifts_the_sighting() {
    let mut reg = seeded(0);
    let c = bid(1).cid;
    place_issued(&mut reg, placement(c, 100, 1));
    show(&mut reg, None, Some("v"), VenueOrderState::Open);
    assert_eq!(place(&mut reg, bid(1)), Err(refused(Side::Buy)));
    reg.on_outcome(c, OrderOp::Place, &item(c, Some("v")), &accepted(), ms(2))
        .unwrap();
    assert!(!reg.not_ours_resting(INST));
    place(&mut reg, bid(1)).unwrap();
}

#[test]
fn a_fill_naming_the_venue_id_as_ours_lifts_the_sighting() {
    let mut l = ledger();
    let mut reg = seeded(0);
    let c = bid(1).cid;
    place_issued(&mut reg, placement(c, 100, 1));
    show(&mut reg, None, Some("v"), VenueOrderState::Open);
    assert!(reg.not_ours_resting(INST));
    fill_of(&mut reg, &mut l, Some(CidMatch::Ours(c)), "v", ms(2));
    assert!(!reg.not_ours_resting(INST));
    // One naming a venue id the order did not know yet (an amend's new id, say) lifts it too.
    let mut reg = seeded(0);
    let c = open(&mut reg, bid(2), "v1");
    show(&mut reg, None, Some("v2"), VenueOrderState::Open);
    assert!(reg.not_ours_resting(INST));
    fill_of(&mut reg, &mut l, Some(CidMatch::Ours(c)), "v2", ms(3));
    assert_eq!(reg.cid_of(&vid("v2")), Some(c));
    assert!(!reg.not_ours_resting(INST));
}

#[test]
fn where_no_outcome_names_the_venue_id_an_update_under_our_client_id_or_its_end_lifts_it() {
    let quiet = || {
        let mut reg = seeded(0);
        let c = bid(1).cid;
        place_issued(&mut reg, placement(c, 100, 1));
        reg.on_outcome(c, OrderOp::Place, &item(c, None), &accepted(), ms(2))
            .unwrap();
        show(&mut reg, None, Some("v"), VenueOrderState::Open);
        assert!(reg.not_ours_resting(INST));
        (reg, c)
    };
    let (mut reg, c) = quiet();
    show(
        &mut reg,
        Some(CidMatch::Ours(c)),
        Some("v"),
        VenueOrderState::Open,
    );
    assert!(!reg.not_ours_resting(INST));
    let (mut reg, _) = quiet();
    show(&mut reg, None, Some("v"), canceled());
    assert!(!reg.not_ours_resting(INST));
}

/// A registry with an order not ours shown under `v`, then our order ended Lost knowing `v` by a
/// late fill under its client id: the sighting stays.
fn lost_knowing(v: &str) -> (Registry, ClientOrderId) {
    let mut l = ledger();
    let mut reg = seeded(0);
    let c = bid(1).cid;
    place_issued(&mut reg, placement(c, 100, 1));
    lose(&mut reg, c, ms(1_000));
    show(&mut reg, None, Some(v), VenueOrderState::Open);
    fill_of(&mut reg, &mut l, Some(CidMatch::Ours(c)), v, ms(1_001));
    assert_eq!(reg.cid_of(&vid(v)), Some(c));
    assert!(reg.not_ours_resting(INST));
    (reg, c)
}

#[test]
fn a_sighting_under_the_venue_id_of_an_order_ended_lost_stays_until_it_ends_or_is_registered() {
    for named in [false, true] {
        let (mut reg, c) = lost_knowing("v");
        let by = named.then_some(CidMatch::Ours(c));
        show(&mut reg, by, Some("v"), canceled());
        assert!(!reg.not_ours_resting(INST), "named {named}");
    }
    // A trustworthy resync showing it open under our client id registers it as an orphan, which
    // drops the sighting; the orphan cooldown still suppresses the adding side.
    let (mut reg, c) = lost_knowing("v");
    resync(&mut reg, ms(2_000), vec![ours_row(c, "v")], 1);
    assert!(reg.orphans(INST).contains(&c));
    assert!(!reg.not_ours_resting(INST));
    assert_eq!(
        place(&mut reg, bid(1)),
        Err(OmsError::OrphanCooldown {
            inst: INST,
            side: Side::Buy
        })
    );
}

#[test]
fn an_amend_under_our_client_id_moves_a_sighting_under_the_venue_id_of_an_order_ended_lost() {
    let (mut reg, c) = lost_knowing("v");
    show(
        &mut reg,
        Some(CidMatch::Ours(c)),
        Some("v"),
        VenueOrderState::Amended {
            new_vid: Some(vid("w")),
        },
    );
    assert_eq!(reg.not_ours_shown(INST), shown(&["w"], false));
    assert_eq!(place(&mut reg, bid(1)), Err(refused(Side::Buy)));
    show(&mut reg, None, Some("w"), canceled());
    assert!(!reg.not_ours_resting(INST));
    // Long 1 from the Lost order's late fill.
    place(&mut reg, bid(CAP - 1)).unwrap();
}

#[test]
fn a_live_order_of_ours_holds_a_venue_id_an_order_ended_lost_learnt_first() {
    let mut l = ledger();
    let mut reg = seeded(0);
    let a = bid(1).cid;
    place_issued(&mut reg, placement(a, 100, 1));
    lose(&mut reg, a, ms(1_000));
    fill_of(&mut reg, &mut l, Some(CidMatch::Ours(a)), "v", ms(1_001));
    assert_eq!(reg.cid_of(&vid("v")), Some(a));
    // A live order of ours comes to hold the same venue id; the index still names the first.
    let b = open(&mut reg, bid(1), "v");
    assert_eq!(reg.cid_of(&vid("v")), Some(a));
    for by in kinds() {
        show(&mut reg, by, Some("v"), VenueOrderState::Open);
        assert!(!reg.not_ours_resting(INST), "{by:?}");
    }
    // An amend of an order not ours onto it moves nothing there either.
    show(
        &mut reg,
        None,
        Some("x"),
        VenueOrderState::Amended {
            new_vid: Some(vid("v")),
        },
    );
    assert!(!reg.not_ours_resting(INST));
    assert_eq!(reg.get(b).unwrap().state(), OrdState::Open);
    place(&mut reg, bid(1)).unwrap();
}

#[test]
fn an_order_not_ours_under_the_venue_id_of_an_order_ended_lost_holds_the_market() {
    let fresh = || {
        let mut l = ledger();
        let mut reg = seeded(0);
        let c = bid(1).cid;
        place_issued(&mut reg, placement(c, 100, 1));
        lose(&mut reg, c, ms(1_000));
        fill_of(&mut reg, &mut l, Some(CidMatch::Ours(c)), "v", ms(1_001));
        assert!(!reg.not_ours_resting(INST));
        (reg, c)
    };
    let (mut reg, _) = fresh();
    show(&mut reg, None, Some("v"), VenueOrderState::Open);
    assert!(reg.not_ours_resting(INST));
    let (mut reg, _) = fresh();
    resync(&mut reg, ms(2_000), vec![not_ours(None, "v")], 1);
    assert!(reg.not_ours_resting(INST));
    let (mut reg, _) = fresh();
    show(
        &mut reg,
        Some(CidMatch::Foreign(THEIRS)),
        Some("v"),
        VenueOrderState::Open,
    );
    assert!(reg.not_ours_resting(INST));
    // Its end under our client id keeps the venue id as ended: a row shows it again only from a
    // trustworthy snapshot requested the lag bound after the end's stamp.
    let (mut reg, c) = fresh();
    show(&mut reg, Some(CidMatch::Ours(c)), Some("v"), canceled());
    let at = ms(1_500);
    reg.order_entry_epoch_ended(at);
    resync_on(
        &mut reg,
        &untrustworthy(),
        ms(10_000),
        vec![not_ours(None, "v")],
        1,
    );
    assert!(!reg.not_ours_resting(INST));
    resync(&mut reg, at + (LAG - NS), vec![not_ours(None, "v")], 1);
    assert!(!reg.not_ours_resting(INST));
    resync(&mut reg, at + LAG, vec![not_ours(None, "v")], 1);
    assert!(reg.not_ours_resting(INST));
}

/// A registry bound to the tests' account with our order `c` ended never sent (its
/// authorization refused for another account), then knowing `v` by a late fill under `c`.
fn never_sent_knowing(v: &str) -> (Registry, ClientOrderId) {
    let mut l = ledger();
    let mut reg = seeded(0);
    reg.bind_account(TEST_ACCT).unwrap();
    let order = bid(1);
    let c = order.cid;
    let cmd = reg.place(order).unwrap();
    assert!(matches!(
        reg.authorize(AccountKey::new(99), cmd).err(),
        Some(IssueRefusal::OtherAccount { .. })
    ));
    assert_eq!(
        reg.get(c).unwrap().state(),
        OrdState::Terminal(TerminalKind::NotSent(NotSentReason::StaleAuthorization))
    );
    fill_of(&mut reg, &mut l, Some(CidMatch::Ours(c)), v, ms(2));
    assert_eq!(reg.cid_of(&vid(v)), Some(c));
    (reg, c)
}

#[test]
fn an_order_not_ours_under_the_venue_id_of_an_order_never_sent_holds_until_it_ends() {
    let (mut reg, _) = never_sent_knowing("v");
    assert!(!reg.not_ours_resting(INST));
    show(&mut reg, None, Some("v"), VenueOrderState::Open);
    assert!(reg.not_ours_resting(INST));
    show(&mut reg, None, Some("v"), canceled());
    assert!(!reg.not_ours_resting(INST));
}

// ---- Test 7: authorization ----

#[test]
fn a_place_built_before_a_sighting_and_authorized_after_is_refused_and_released() {
    let mut reg = seeded(0);
    let order = bid(1);
    let c = order.cid;
    let cmd = reg.place(order).unwrap();
    show(&mut reg, None, Some("v"), VenueOrderState::Open);
    assert_eq!(
        reg.authorize(TEST_ACCT, cmd).err(),
        Some(IssueRefusal::NotOursResting {
            inst: INST,
            side: Side::Buy
        })
    );
    assert_eq!(
        reg.get(c).unwrap().state(),
        OrdState::Terminal(TerminalKind::NotSent(NotSentReason::StaleAuthorization))
    );
}

#[test]
fn an_adding_place_authorized_before_a_sighting_is_refused_at_submit() {
    let mut reg = seeded(0);
    let cmd = reg.place(bid(1)).unwrap();
    let handed = authorize(&mut reg, cmd);
    assert_eq!(handed.check_at_submit(), Ok(()));
    show(&mut reg, None, Some("v"), VenueOrderState::Open);
    assert_eq!(
        handed.check_at_submit(),
        Err(StaleAuthorization::NotOursResting(INST))
    );
}

#[test]
fn an_exit_place_and_a_crossing_intent_authorized_before_a_sighting_pass_at_submit() {
    let mut reg = seeded(10);
    reg.flatten(INST, fbc_oms::Leases::none()).unwrap();
    let cmd = reg.place(reducing_ask(5)).unwrap();
    let handed = authorize(&mut reg, cmd);
    show(&mut reg, None, Some("v"), VenueOrderState::Open);
    assert_eq!(handed.check_at_submit(), Ok(()));
    // In Quoting, a crossing intent within the position's size.
    let mut reg = seeded(10);
    let cmd = reg.place(crossing(5)).unwrap();
    let handed = authorize(&mut reg, cmd);
    show(&mut reg, None, Some("v"), VenueOrderState::Open);
    assert_eq!(handed.check_at_submit(), Ok(()));
}

#[test]
fn beside_resting_asks_a_flagged_cross_passes_at_submit_and_an_unflagged_sell_does_not() {
    let mut reg = long_with_asks(false);
    let cmd = reg.place(crossing(10)).unwrap();
    let handed = authorize(&mut reg, cmd);
    show(&mut reg, None, Some("v"), VenueOrderState::Open);
    assert_eq!(handed.check_at_submit(), Ok(()));
    let mut reg = long_with_asks(false);
    let cmd = reg.place(reducing_ask(10)).unwrap();
    let handed = authorize(&mut reg, cmd);
    show(&mut reg, None, Some("v"), VenueOrderState::Open);
    assert_eq!(
        handed.check_at_submit(),
        Err(StaleAuthorization::NotOursResting(INST))
    );
}

#[test]
fn a_batch_counts_the_venue_flag_only_when_every_order_of_it_on_the_side_carries_it() {
    let mut reg = long_with_asks(false);
    let (flagged, plain) = (flagged_ask(5), ask(5));
    let cids = [flagged.cid, plain.cid];
    let plan = reg.place_batch(vec![flagged, plain]).unwrap();
    assert!(plan.refused.is_empty());
    show(&mut reg, None, Some("v"), VenueOrderState::Open);
    assert_eq!(
        reg.authorize(TEST_ACCT, plan.command.unwrap()).err(),
        Some(IssueRefusal::NotOursResting {
            inst: INST,
            side: Side::Sell
        })
    );
    for c in cids {
        assert_eq!(
            reg.get(c).unwrap().state(),
            OrdState::Terminal(TerminalKind::NotSent(NotSentReason::StaleAuthorization))
        );
    }
    let mut reg = long_with_asks(false);
    let plan = reg
        .place_batch(vec![flagged_ask(5), flagged_ask(5)])
        .unwrap();
    show(&mut reg, None, Some("v"), VenueOrderState::Open);
    passes(&mut reg, plan.command.unwrap());
}

#[test]
fn an_adding_amend_authorized_before_a_sighting_is_refused_at_submit_and_a_reducing_one_is_not() {
    let caps = venue();
    let mut reg = seeded(0);
    let c = open(&mut reg, bid(1), "b-1");
    let cmd = reg
        .live(c)
        .unwrap()
        .amend(&caps, Ticks(100), lots(2), false)
        .unwrap();
    let handed = authorize(&mut reg, cmd);
    show(&mut reg, None, Some("v"), VenueOrderState::Open);
    assert_eq!(
        handed.check_at_submit(),
        Err(StaleAuthorization::NotOursResting(INST))
    );
    let mut reg = seeded(10);
    let c = open(&mut reg, ask(5), "a-1");
    let cmd = reg
        .live(c)
        .unwrap()
        .amend(&caps, Ticks(101), lots(3), false)
        .unwrap();
    let handed = authorize(&mut reg, cmd);
    show(&mut reg, None, Some("v"), VenueOrderState::Open);
    assert_eq!(handed.check_at_submit(), Ok(()));
}

#[test]
fn sightings_coming_and_going_never_move_the_state_generation_or_void_an_exit_place() {
    let mut reg = seeded(10);
    reg.flatten(INST, fbc_oms::Leases::none()).unwrap();
    let generation = reg.entry(INST).generation();
    let cmd = reg.place(reducing_ask(5)).unwrap();
    let handed = authorize(&mut reg, cmd);
    for n in 0..3 {
        let v = format!("v-{n}");
        show(&mut reg, None, Some(&v), VenueOrderState::Open);
        show(&mut reg, None, Some(&v), canceled());
    }
    assert_eq!(handed.check_at_submit(), Ok(()));
    assert_eq!(reg.entry(INST).generation(), generation);
}

#[test]
fn a_place_built_under_the_refusal_watches_the_position_from_its_build() {
    for held in [true, false] {
        let mut l = ledger();
        let mut reg = seeded(5);
        let ours = open(&mut reg, bid(1), "b-1");
        if held {
            show(&mut reg, None, Some("v"), VenueOrderState::Open);
        }
        let cmd = reg.place(reducing_ask(2)).unwrap();
        // A fill of our bid takes the position to long 6, which still admits the sell.
        fill_of(&mut reg, &mut l, Some(CidMatch::Ours(ours)), "b-1", ms(2));
        assert_eq!(reg.position(INST), Some(SignedLots(6)));
        let handed = authorize(&mut reg, cmd);
        let expected = if held {
            Err(StaleAuthorization::PositionMoved(INST))
        } else {
            Ok(())
        };
        assert_eq!(handed.check_at_submit(), expected, "held {held}");
    }
}

// ---- Test 8: a fill alone ----

#[test]
fn a_fill_of_an_order_not_ours_shows_nothing_resting() {
    for named in [
        Some(CidMatch::Foreign(THEIRS)),
        Some(CidMatch::Unparseable),
        None,
    ] {
        let mut l = ledger();
        let mut reg = seeded(0);
        fill_of(&mut reg, &mut l, named, "v", ms(2));
        assert!(reg.foreign_in_view(INST), "{named:?}");
        assert!(!reg.not_ours_resting(INST), "{named:?}");
        place(&mut reg, bid(CAP)).unwrap();
    }
}

// ---- Test 9: the execution planner ----

/// The planner's minimum age before it changes an order.
const MIN_AGE: Duration = Duration::from_millis(100);

/// A client-id mint under a namespace of its own, which no other test of this binary uses.
fn mint() -> CidMint {
    static NEXT: AtomicU16 = AtomicU16::new(1_100);
    let ns = Namespace::new(NEXT.fetch_add(1, Ordering::Relaxed));
    let lease = NamespaceLease::acquire(&common::lease_dir(), AccountKey::new(2), ns).unwrap();
    CidMint::new(lease, 0, 0, WallNs(0))
}

fn planner() -> ExecutionPlanner {
    ExecutionPlanner::new(PlannerConfig::new(Bps(2.0), lots(2), MIN_AGE).unwrap())
}

/// [`venue`] with batches of four places, as the batching tests declare one (decision 0111).
fn batching() -> OrderCaps {
    OrderCaps {
        batch_place: Some(Batch { max_items: 4 }),
        ..venue()
    }
}

/// [`venue`] refusing post-only with reduce-only, so a maker exit can be emulated (0106).
fn emulating() -> OrderCaps {
    OrderCaps {
        flag_conflicts: vec![(Feature::PostOnly, Feature::ReduceOnly)],
        ..venue()
    }
}

/// A pass on `caps`, the account and market in Normal mode, trading, no rate scope tracked,
/// batching as `batching` says.
fn pass_on(caps: &OrderCaps, batching: Batching) -> PlanVenue<'_> {
    PlanVenue {
        batching,
        ..pass_venue::of(caps)
    }
}

/// A GTC limit on the public book of `qty` at `px`, neither post-only, reduce-only nor
/// classified reducing.
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

/// `q` classified reducing, without the venue's flag.
fn marked(q: DesiredQuote) -> DesiredQuote {
    DesiredQuote {
        reducing: true,
        ..q
    }
}

/// `q` carrying the venue's reduce-only flag.
fn flagged(q: DesiredQuote) -> DesiredQuote {
    DesiredQuote {
        reduce_only: true,
        ..q
    }
}

/// A post-only reduce-only exit of `qty` at `px`, emulated where the venue refuses both.
fn maker_exit(px: i64, qty: i64) -> DesiredQuote {
    DesiredQuote {
        post_only: true,
        reduce_fallback: ReduceFallback::EmulateReduceOnly,
        ..flagged(marked(limit(px, qty)))
    }
}

/// A sell crossing intent of `qty` capped at 90 (decision 0110), the consumer's first attempt.
fn cross(qty: i64) -> CrossingIntent {
    CrossingIntent {
        cap: Ticks(90),
        qty: lots(qty),
        tag: 7,
        attempt: 1,
    }
}

fn book() -> DesiredBook {
    DesiredBook::new(INST)
}

/// Plans `desired` and reports every cancel sent, as a gateway would.
fn plan_on(
    p: &mut ExecutionPlanner,
    desired: &DesiredBook,
    reg: &mut Registry,
    venue: &PlanVenue<'_>,
    mint: &mut CidMint,
    now: MonoNs,
) -> Plan {
    let plan = p.plan(desired, reg, venue, TEST_ACCT, mint, now).unwrap();
    for (n, c) in plan.commands.iter().enumerate() {
        if let VenueCommand::Cancel(_) = c.auth.command() {
            let rpc = RpcId(1_000 + now.0 + n as u64);
            assert!(reg.cancel_sent(c.cid, rpc, now).unwrap());
        }
    }
    plan
}

/// [`plan_on`] one order per command.
fn plan_one(
    p: &mut ExecutionPlanner,
    desired: &DesiredBook,
    reg: &mut Registry,
    caps: &OrderCaps,
    mint: &mut CidMint,
    now: MonoNs,
) -> Plan {
    plan_on(p, desired, reg, &pass_on(caps, Batching::One), mint, now)
}

/// Each command's stage, kind, side, level, whether it is the crossing intent's, and how many
/// items it carries.
fn shape(plan: &Plan) -> Vec<(Stage, &'static str, Side, u16, bool, usize)> {
    plan.commands
        .iter()
        .map(|c| {
            let kind = match c.auth.command() {
                VenueCommand::Place(_) => "place",
                VenueCommand::PlaceBatch(_) => "batch",
                VenueCommand::Amend(_) => "amend",
                VenueCommand::Cancel(_) => "cancel",
                other => panic!("the planner built {other:?}"),
            };
            (
                c.stage,
                kind,
                c.side,
                c.level,
                c.crossing,
                1 + c.more_items.len(),
            )
        })
        .collect()
}

/// The levels refused this pass and why.
fn refusals(plan: &Plan) -> Vec<(Side, u16, PlanRefusal)> {
    plan.refused
        .iter()
        .map(|r| (r.side, r.level, r.why.clone()))
        .collect()
}

/// Hands every command of `plan` to a gateway, which takes it over (decision 0152).
fn hand_all(plan: Plan) -> Vec<Handed> {
    plan.commands
        .into_iter()
        .map(|c| c.auth.hand_over())
        .collect()
}

/// Acknowledges the placement of `c` under the venue id `v`: Open.
fn ack(reg: &mut Registry, c: ClientOrderId, v: &str) {
    reg.on_outcome(c, OrderOp::Place, &item(c, Some(v)), &accepted(), ms(1))
        .unwrap();
    assert_eq!(reg.get(c).unwrap().state(), OrdState::Open);
}

/// Ends our order `c` on `side`, under the venue id `v`, by a cancel.
fn cancelled(reg: &mut Registry, c: ClientOrderId, side: Side, v: &str) {
    let u = OrderUpdate {
        cid: Some(CidMatch::Ours(c)),
        side,
        ..event(None, Some(v), canceled())
    };
    reg.apply_update(&u, key());
    assert!(reg.get(c).unwrap().state().is_terminal());
}

/// The one level `desired` wants on `side` placed by the planner at `now` and acknowledged
/// under `v`.
fn placed_by(
    p: &mut ExecutionPlanner,
    desired: &DesiredBook,
    reg: &mut Registry,
    caps: &OrderCaps,
    mint: &mut CidMint,
    v: &str,
) -> ClientOrderId {
    let plan = plan_one(p, desired, reg, caps, mint, ms(10));
    assert_eq!(plan.commands.len(), 1, "{:?}", shape(&plan));
    let c = plan.commands[0].cid;
    drop(hand_all(plan));
    ack(reg, c, v);
    c
}

fn not_ours_on(side: Side) -> PlanRefusal {
    PlanRefusal::Place(refused(side))
}

#[test]
fn a_reducer_the_refusal_refuses_cancels_no_emulated_order_it_would_wait_for() {
    // Long 10, an ordinary ask of 8 resting and an emulated exit of 8 at level 1. A reducing
    // ask of 5 at level 0 would wait for the emulated order's cancel, but even with it ended,
    // the ask beside the ordinary one sells 13 against a position of 10: under an order not
    // ours the refusal refuses it in the planner's preview, so the pass cancels nothing for it
    // (FBC-01w).
    for held in [false, true] {
        let caps = emulating();
        let mut reg = seeded_on(&caps, 10);
        let mut mint = mint();
        let mut p = planner();
        place(&mut reg, ask(8)).unwrap();
        let first = book().with(Side::Sell, 1, maker_exit(120, 8));
        let emulated = placed_by(&mut p, &first, &mut reg, &caps, &mut mint, "e-1");
        assert!(!reg.get(emulated).unwrap().placed().reduce_only);
        if held {
            show(&mut reg, None, Some("x"), VenueOrderState::Open);
        }
        let next = first.with(Side::Sell, 0, marked(limit(110, 5)));
        let pass = plan_one(&mut p, &next, &mut reg, &caps, &mut mint, ms(500));
        if !held {
            // The control: with no order not ours, the reducer waits for the emulated cancel.
            assert_eq!(
                shape(&pass),
                vec![(Stage::Cancel, "cancel", Side::Sell, 1, false, 1)]
            );
            assert_eq!(pass.commands[0].cid, emulated);
            assert_eq!(
                refusals(&pass),
                vec![(Side::Sell, 0, PlanRefusal::AwaitsEmulatedCancel)]
            );
            continue;
        }
        assert!(pass.commands.is_empty(), "{:?}", shape(&pass));
        assert_eq!(
            refusals(&pass),
            vec![(Side::Sell, 0, not_ours_on(Side::Sell))]
        );
        assert_eq!(reg.get(emulated).unwrap().state(), OrdState::Open);
    }
}

#[test]
fn a_reducers_amend_the_refusal_refuses_cancels_no_emulated_order_it_would_wait_for() {
    // Long 10, an ordinary ask of 2 resting, an emulated exit of 7 at level 1 and a reducing
    // ask of 1 at level 0. Grown to 9, the reducer would wait for the emulated order's cancel,
    // but even with it ended, the reducer beside the ordinary ask sells 11 against a position of
    // 10: under an order not ours the refusal refuses the amend in the planner's preview, so the
    // pass cancels nothing for it (FBC-01w).
    for held in [false, true] {
        let caps = emulating();
        let mut reg = seeded_on(&caps, 10);
        let mut mint = mint();
        let mut p = planner();
        place(&mut reg, ask(2)).unwrap();
        let first = book().with(Side::Sell, 1, maker_exit(120, 7));
        let emulated = placed_by(&mut p, &first, &mut reg, &caps, &mut mint, "e-1");
        assert!(!reg.get(emulated).unwrap().placed().reduce_only);
        let second = first.with(Side::Sell, 0, marked(limit(110, 1)));
        let reducer = placed_by(&mut p, &second, &mut reg, &caps, &mut mint, "r-1");
        if held {
            show(&mut reg, None, Some("x"), VenueOrderState::Open);
        }
        let next = second.with(Side::Sell, 0, marked(limit(110, 9)));
        let pass = plan_one(&mut p, &next, &mut reg, &caps, &mut mint, ms(500));
        if !held {
            // The control: with no order not ours, the grown reducer waits for the emulated
            // cancel.
            assert_eq!(
                shape(&pass),
                vec![(Stage::Cancel, "cancel", Side::Sell, 1, false, 1)]
            );
            assert_eq!(pass.commands[0].cid, emulated);
            assert_eq!(
                refusals(&pass),
                vec![(Side::Sell, 0, PlanRefusal::AwaitsEmulatedCancel)]
            );
            continue;
        }
        assert!(pass.commands.is_empty(), "{:?}", shape(&pass));
        assert_eq!(
            refusals(&pass),
            vec![(
                Side::Sell,
                0,
                PlanRefusal::Amend(AmendRefusal::NotOursResting {
                    inst: INST,
                    side: Side::Sell
                })
            )]
        );
        assert_eq!(reg.get(emulated).unwrap().state(), OrdState::Open);
        assert_eq!(reg.get(reducer).unwrap().state(), OrdState::Open);
    }
}

#[test]
fn beside_resting_asks_a_crossing_intent_of_the_position_is_built_and_an_emulated_exit_is_not() {
    // Long 10, our ordinary asks of 2 and 2 resting and an order not ours shown resting.
    let mut reg = long_with_asks(true);
    let caps = venue();
    let mut p = planner();
    let mut mint = mint();
    let desired = book().with_crossing(Side::Sell, cross(10));
    let pass = plan_one(&mut p, &desired, &mut reg, &caps, &mut mint, ms(10));
    assert_eq!(
        shape(&pass),
        vec![(
            Stage::Reducing,
            "place",
            Side::Sell,
            CROSSING_LEVEL,
            true,
            1
        )]
    );
    match pass.commands[0].auth.command() {
        VenueCommand::Place(o) => assert!(o.qty == lots(10) && o.reduce_only, "{o:?}"),
        other => panic!("expected a place, got {other:?}"),
    }
    for handed in hand_all(pass) {
        assert_eq!(handed.check_at_submit(), Ok(()));
    }
    // An emulated exit carries no flag: beside the asks it would sell 14 against 10.
    let mut p = planner();
    let caps = emulating();
    let mut reg = seeded_on(&caps, 10);
    place(&mut reg, ask(2)).unwrap();
    place(&mut reg, ask(2)).unwrap();
    show(&mut reg, None, Some("x"), VenueOrderState::Open);
    let desired = book().with(Side::Sell, 0, maker_exit(120, 10));
    let pass = plan_one(&mut p, &desired, &mut reg, &caps, &mut mint, ms(10));
    assert!(pass.commands.is_empty(), "{:?}", shape(&pass));
    assert_eq!(
        refusals(&pass),
        vec![(Side::Sell, 0, not_ours_on(Side::Sell))]
    );
}

#[test]
fn a_moved_order_the_refusal_would_refuse_is_cancelled_on_a_venue_that_amends_then_refused() {
    // Flat, an ordinary bid resting past the minimum age: its move is an amend with no order
    // not ours, and with one shown resting a cancel, then a place the refusal refuses.
    for held in [false, true] {
        let caps = venue();
        let mut reg = seeded(0);
        let mut mint = mint();
        let mut p = planner();
        let first = book().with(Side::Buy, 0, limit(100, 5));
        let b = placed_by(&mut p, &first, &mut reg, &caps, &mut mint, "b-1");
        if held {
            show(&mut reg, None, Some("x"), VenueOrderState::Open);
        }
        let moved = book().with(Side::Buy, 0, limit(98, 5));
        let pass = plan_one(&mut p, &moved, &mut reg, &caps, &mut mint, ms(500));
        if !held {
            assert_eq!(
                shape(&pass),
                vec![(Stage::Amend, "amend", Side::Buy, 0, false, 1)]
            );
            continue;
        }
        assert_eq!(
            shape(&pass),
            vec![(Stage::Cancel, "cancel", Side::Buy, 0, false, 1)]
        );
        assert_eq!(pass.commands[0].cid, b);
        assert!(pass.refused.is_empty(), "{:?}", pass.refused);
        drop(hand_all(pass));
        cancelled(&mut reg, b, Side::Buy, "b-1");
        let after = plan_one(&mut p, &moved, &mut reg, &caps, &mut mint, ms(600));
        assert!(after.commands.is_empty(), "{:?}", shape(&after));
        assert_eq!(
            refusals(&after),
            vec![(Side::Buy, 0, not_ours_on(Side::Buy))]
        );
    }
}

#[test]
fn under_an_orphan_cooldown_a_moved_order_is_cancelled_only_beside_an_order_not_ours() {
    // Flat, an ordinary bid resting past the minimum age, and an orphan of ours on the market:
    // the cooldown alone refuses the bid's amend, which is reported and leaves it resting
    // (0065 item 6). With an order not ours shown resting too, the planner reads the sighting
    // itself and cancels the bid; its new place is then refused for the cooldown, reported
    // first.
    for held in [false, true] {
        let caps = venue();
        let mut reg = seeded(0);
        let mut mint = mint();
        let mut p = planner();
        let first = book().with(Side::Buy, 0, limit(100, 5));
        let b = placed_by(&mut p, &first, &mut reg, &caps, &mut mint, "b-1");
        let report = resync(
            &mut reg,
            ms(5),
            vec![ours_row(b, "b-1"), ours_row(cid(), "orphan")],
            0,
        );
        assert_eq!(report.orphans, vec![INST], "{report:?}");
        if held {
            show(&mut reg, None, Some("x"), VenueOrderState::Open);
        }
        let moved = book().with(Side::Buy, 0, limit(98, 5));
        let pass = plan_one(&mut p, &moved, &mut reg, &caps, &mut mint, ms(500));
        let cooldown = OmsError::OrphanCooldown {
            inst: INST,
            side: Side::Buy,
        };
        if !held {
            assert!(pass.commands.is_empty(), "{:?}", shape(&pass));
            assert_eq!(
                refusals(&pass),
                vec![(
                    Side::Buy,
                    0,
                    PlanRefusal::Amend(AmendRefusal::OrphanCooldown {
                        inst: INST,
                        side: Side::Buy,
                    })
                )]
            );
            continue;
        }
        assert_eq!(
            shape(&pass),
            vec![(Stage::Cancel, "cancel", Side::Buy, 0, false, 1)]
        );
        assert!(pass.refused.is_empty(), "{:?}", pass.refused);
        drop(hand_all(pass));
        cancelled(&mut reg, b, Side::Buy, "b-1");
        let after = plan_one(&mut p, &moved, &mut reg, &caps, &mut mint, ms(600));
        assert!(after.commands.is_empty(), "{:?}", shape(&after));
        assert_eq!(
            refusals(&after),
            vec![(Side::Buy, 0, PlanRefusal::Place(cooldown))]
        );
    }
}

#[test]
fn an_order_the_refusal_admits_as_it_rests_keeps_its_amend_which_is_reported_refused() {
    // Long 10, an ask of 5 resting past the minimum age, in Quoting (ordinary) and in Exit
    // (classified reducing): within the position's size it is admitted as it rests, so its
    // growth to 12 is an amend, refused by the refusal and reported, the ask resting as it was.
    for exit in [false, true] {
        let caps = venue();
        let mut reg = seeded(10);
        let mut mint = mint();
        let mut p = planner();
        let quote = if exit {
            marked(limit(110, 5))
        } else {
            limit(110, 5)
        };
        let first = book().with(Side::Sell, 0, quote.clone());
        let a = placed_by(&mut p, &first, &mut reg, &caps, &mut mint, "a-1");
        if exit {
            reg.flatten(INST, fbc_oms::Leases::none()).unwrap();
            assert!(matches!(reg.entry(INST).state(), EntryState::Exit(_)));
        }
        show(&mut reg, None, Some("x"), VenueOrderState::Open);
        let grown = book().with(
            Side::Sell,
            0,
            DesiredQuote {
                qty: lots(12),
                ..quote
            },
        );
        let pass = plan_one(&mut p, &grown, &mut reg, &caps, &mut mint, ms(500));
        assert!(pass.commands.is_empty(), "exit {exit}: {:?}", shape(&pass));
        assert_eq!(
            refusals(&pass),
            vec![(
                Side::Sell,
                0,
                PlanRefusal::Amend(AmendRefusal::NotOursResting {
                    inst: INST,
                    side: Side::Sell,
                })
            )],
            "exit {exit}"
        );
        assert_eq!(reg.get(a).unwrap().state(), OrdState::Open);
        assert_eq!(p.order_at(TEST_ACCT, INST, Side::Sell, 0), Some(a));
    }
}

#[test]
fn in_exit_a_moved_order_past_the_position_is_cancelled_beside_an_order_not_ours() {
    // Long 10, an ordinary ask of 12 placed in Quoting, then the owner's Flatten. Moved, its
    // amend is refused by Exit's admission and reported, the ask left as it is (0065 item 8),
    // unless an order not ours is shown resting: then the planner cancels it (0158's amendment
    // of 0065).
    for held in [false, true] {
        let caps = venue();
        let mut reg = seeded(10);
        let mut mint = mint();
        let mut p = planner();
        let first = book().with(Side::Sell, 0, limit(110, 12));
        let a = placed_by(&mut p, &first, &mut reg, &caps, &mut mint, "a-1");
        reg.flatten(INST, fbc_oms::Leases::none()).unwrap();
        if held {
            show(&mut reg, None, Some("x"), VenueOrderState::Open);
        }
        let moved = book().with(Side::Sell, 0, limit(112, 12));
        let pass = plan_one(&mut p, &moved, &mut reg, &caps, &mut mint, ms(500));
        if held {
            assert_eq!(
                shape(&pass),
                vec![(Stage::Cancel, "cancel", Side::Sell, 0, false, 1)]
            );
            assert_eq!(pass.commands[0].cid, a);
        } else {
            assert!(pass.commands.is_empty(), "{:?}", shape(&pass));
            assert!(
                matches!(
                    refusals(&pass)[..],
                    [(Side::Sell, 0, PlanRefusal::Amend(_))]
                ),
                "{:?}",
                pass.refused
            );
        }
    }
}

/// Long 10 on [`batching`]'s caps, our ordinary asks of 2 and 2 resting, and, when `held`, an
/// order not ours shown resting.
fn batching_long_with_asks(held: bool) -> (Registry, OrderCaps) {
    let caps = batching();
    let mut reg = seeded_on(&caps, 10);
    place(&mut reg, ask(2)).unwrap();
    place(&mut reg, ask(2)).unwrap();
    if held {
        show(&mut reg, None, Some("x"), VenueOrderState::Open);
    }
    (reg, caps)
}

#[test]
fn under_declared_batching_a_crossing_intent_goes_alone_after_the_places_held_beside_it() {
    // Long 10, ordinary asks of 2 and 2 resting, an order not ours shown resting: the reducing
    // ask of 3 is built and emitted, authorized as built, before the crossing intent, sized 7
    // after it, which goes alone. Both pass at submit.
    let (mut reg, caps) = batching_long_with_asks(true);
    let mut p = planner();
    let mut mint = mint();
    let desired = book()
        .with(Side::Sell, 0, marked(limit(110, 3)))
        .with_crossing(Side::Sell, cross(10));
    let venue = pass_on(&caps, Batching::Declared);
    let pass = plan_on(&mut p, &desired, &mut reg, &venue, &mut mint, ms(10));
    assert_eq!(
        shape(&pass),
        vec![
            (Stage::Reducing, "place", Side::Sell, 0, false, 1),
            (
                Stage::Reducing,
                "place",
                Side::Sell,
                CROSSING_LEVEL,
                true,
                1
            ),
        ]
    );
    match pass.commands[1].auth.command() {
        VenueCommand::Place(o) => assert_eq!(o.qty, lots(7)),
        other => panic!("expected a place, got {other:?}"),
    }
    for handed in hand_all(pass) {
        assert_eq!(handed.check_at_submit(), Ok(()));
    }
}

#[test]
fn under_declared_batching_a_crossing_intent_is_never_batched_with_no_order_not_ours() {
    // The same state with no order not ours: the ask of 3 and the crossing intent of 7 are two
    // commands, both authorized. The ask, authorized with the crossing intent built and counted,
    // would be refused beside an order not ours, so one shown resting later refuses it at
    // submit, while the crossing intent passes.
    let (mut reg, caps) = batching_long_with_asks(false);
    let mut p = planner();
    let mut mint = mint();
    let desired = book()
        .with(Side::Sell, 0, marked(limit(110, 3)))
        .with_crossing(Side::Sell, cross(10));
    let venue = pass_on(&caps, Batching::Declared);
    let pass = plan_on(&mut p, &desired, &mut reg, &venue, &mut mint, ms(10));
    assert_eq!(
        shape(&pass),
        vec![
            (Stage::Reducing, "place", Side::Sell, 0, false, 1),
            (
                Stage::Reducing,
                "place",
                Side::Sell,
                CROSSING_LEVEL,
                true,
                1
            ),
        ]
    );
    let handed = hand_all(pass);
    show(&mut reg, None, Some("x"), VenueOrderState::Open);
    assert_eq!(
        handed[0].check_at_submit(),
        Err(StaleAuthorization::NotOursResting(INST))
    );
    assert_eq!(handed[1].check_at_submit(), Ok(()));
    // The control: a desired ask carrying the venue's flag in the crossing intent's place, at
    // level 1, joins the ask of 3 in one batch.
    let mut p = planner();
    let (mut reg, caps) = batching_long_with_asks(false);
    let desired = book().with(Side::Sell, 0, marked(limit(110, 3))).with(
        Side::Sell,
        1,
        flagged(limit(111, 7)),
    );
    let venue = pass_on(&caps, Batching::Declared);
    let pass = plan_on(&mut p, &desired, &mut reg, &venue, &mut mint, ms(10));
    assert_eq!(
        shape(&pass),
        vec![(Stage::Reducing, "batch", Side::Sell, 0, false, 2)]
    );
}

#[test]
fn under_declared_batching_a_flagged_place_goes_alone_while_an_order_not_ours_rests() {
    // Long 10, the caps taking the venue's flag with every other: a reducing ask of 3 at level
    // 0 and a flagged ask of 10 at level 1.
    let desired = book().with(Side::Sell, 0, marked(limit(110, 3))).with(
        Side::Sell,
        1,
        flagged(limit(111, 10)),
    );
    let caps = batching();
    let venue = pass_on(&caps, Batching::Declared);
    let mut p = planner();
    let mut mint = mint();
    // With an order not ours shown resting, the ask of 3 is emitted, authorized as built,
    // before the flagged ask is built, which goes alone. Both pass at submit.
    let mut reg = seeded_on(&caps, 10);
    show(&mut reg, None, Some("x"), VenueOrderState::Open);
    let pass = plan_on(&mut p, &desired, &mut reg, &venue, &mut mint, ms(10));
    assert_eq!(
        shape(&pass),
        vec![
            (Stage::Reducing, "place", Side::Sell, 0, false, 1),
            (Stage::Reducing, "place", Side::Sell, 1, false, 1),
        ]
    );
    for handed in hand_all(pass) {
        assert_eq!(handed.check_at_submit(), Ok(()));
    }
    // With none, the two are one batch, authorized. An order not ours shown resting then
    // refuses the whole batch at submit; reported not sent, the next pass plans its places
    // again, and the flagged ask goes alone and passes.
    let mut p = planner();
    let mut reg = seeded_on(&caps, 10);
    let pass = plan_on(&mut p, &desired, &mut reg, &venue, &mut mint, ms(10));
    assert_eq!(
        shape(&pass),
        vec![(Stage::Reducing, "batch", Side::Sell, 0, false, 2)]
    );
    let cids: Vec<ClientOrderId> = std::iter::once(pass.commands[0].cid)
        .chain(pass.commands[0].more_items.iter().map(|i| i.cid))
        .collect();
    let handed = hand_all(pass);
    show(&mut reg, None, Some("x"), VenueOrderState::Open);
    assert_eq!(
        handed[0].check_at_submit(),
        Err(StaleAuthorization::NotOursResting(INST))
    );
    drop(handed);
    let not_sent = SubmitOutcome::NotSent(NotSentReason::StaleAuthorization);
    for (idx, &c) in cids.iter().enumerate() {
        let it = ItemRef {
            idx: u16::try_from(idx).unwrap(),
            ..item(c, None)
        };
        reg.on_outcome(c, OrderOp::Place, &it, &not_sent, ms(20))
            .unwrap();
        assert!(reg.get(c).unwrap().state().is_terminal());
    }
    let again = plan_on(&mut p, &desired, &mut reg, &venue, &mut mint, ms(30));
    assert_eq!(
        shape(&again),
        vec![
            (Stage::Reducing, "place", Side::Sell, 0, false, 1),
            (Stage::Reducing, "place", Side::Sell, 1, false, 1),
        ]
    );
    for handed in hand_all(again) {
        assert_eq!(handed.check_at_submit(), Ok(()));
    }
}

#[test]
fn under_declared_batching_the_places_held_are_emitted_before_a_flagged_amend_is_built() {
    // Long 10, our flagged ask of 1 resting at level 1 past the minimum age, and an order not
    // ours shown resting. The pass builds a reducing ask of 3 at level 0 and grows the flagged
    // ask to 10 by amend, which passes the refusal by its flag. The ask of 3 is emitted,
    // authorized as built, before the amend is built, so it is never judged with the amend
    // counted. Both pass at submit.
    let caps = batching();
    let venue = pass_on(&caps, Batching::Declared);
    let mut reg = seeded_on(&caps, 10);
    let mut p = planner();
    let mut mint = mint();
    let first = book().with(Side::Sell, 1, flagged(limit(111, 1)));
    let f = placed_by(&mut p, &first, &mut reg, &caps, &mut mint, "f-1");
    show(&mut reg, None, Some("x"), VenueOrderState::Open);
    let desired = book().with(Side::Sell, 0, marked(limit(110, 3))).with(
        Side::Sell,
        1,
        flagged(limit(111, 10)),
    );
    let pass = plan_on(&mut p, &desired, &mut reg, &venue, &mut mint, ms(500));
    assert_eq!(
        shape(&pass),
        vec![
            (Stage::Reducing, "place", Side::Sell, 0, false, 1),
            (Stage::Reducing, "amend", Side::Sell, 1, false, 1),
        ]
    );
    assert_eq!(pass.commands[1].cid, f);
    for handed in hand_all(pass) {
        assert_eq!(handed.check_at_submit(), Ok(()));
    }
}
