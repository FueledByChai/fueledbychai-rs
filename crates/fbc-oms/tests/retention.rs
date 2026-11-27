//! The retention of terminal orders (decision 0157, FBC-6pdo): a registry forgets a terminal
//! order only at the consumer's pass, `Registry::retain_ended`, within the consumer's count,
//! age and resync bound, once neither it, its planner nor a command can still read the order
//! and no answer can still name it.
//!
//! - After a pass the registry holds at most the count of eligible ended records, and never
//!   forgets an order that is not terminal, is pinned or is settling (item 2, the done line).
//! - Each pin of item 3 keeps its record through every pass that would otherwise forget it,
//!   counted under its reason, beside a control record the same pass forgets.
//! - A forgotten order is forgotten whole (item 4), and what is kept, two spans per namespace,
//!   places a late fill under its client id as its held record's would be, answers a late
//!   outcome or sent report as a terminal record does, and refuses its client id to a place or
//!   an insert (items 5 to 7).
//! - Every number a cap, Exit's admission, the orphan suppression or the planner reads stays
//!   what a twin registry that never runs the pass reads, under the record's conditions; a
//!   proptest twin with a fixed seed and case count holds it (0037).
//!
//! Every number here is a test input on a synthetic market: a lag bound of 200 ms, a resync
//! bound of 300 ms, and caps of 100 lots.

#[path = "common/arm.rs"]
mod arm;
mod common;
#[path = "common/venue.rs"]
mod venue;

use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};
use std::time::Duration;

use arm::{TEST_ACCT, place_issued};
use common::{canceled, cid, fill, fill_id, lease_dir, lots, order_caps, placement, vid};
use fbc_core::{
    AccountKey, AckLevel, AmendAck, AmendCaps, AmendQty, Batch, Bps, CancelBatch, Channel,
    CidMatch, CidMint, ClientOrderId, FillIdent, InstrumentId, ItemRef, MonoNs, Namespace,
    NamespaceLease, NewOrder, NotSentReason, OrderCaps, OrderRef, OrderUpdate, QueryAnswer,
    RefKind, RpcId, Side, SignedLots, SnapshotReads, SnapshotSource, SubmitOutcome, TagSet, Ticks,
    Tif, TifTag, VenueOrderSnapshot, VenueOrderState, WallNs,
};
use fbc_oms::{
    Admission, Applied, Batching, CancelChoice, DesiredBook, DesiredQuote, ExecutionPlanner,
    FillLedger, FillRouted, LadderConfig, LadderResolution, LedgerConfig, MarketCapsConfig,
    OmsError, OrdState, OrderKey, OrderOp, OutcomeApplied, Pinned, PlanVenue, PlannerConfig,
    PositionCheck, PreTradeCaps, ReduceFallback, Registry, ResyncReport, ResyncSnapshot, Retained,
    RetentionConfig, Routed, Stage, TerminalKind, TestnetRun,
};

const INST: InstrumentId = InstrumentId::new(1);
/// The venue's declared snapshot lag bound in these tests.
const LAG: Duration = Duration::from_millis(200);
/// The consumer's resync bound in these tests.
const RESYNC: Duration = Duration::from_millis(300);
/// An age no test reaches.
const LONG: Duration = Duration::from_secs(3_600);
/// Both caps, in lots.
const CAP: i64 = 100;

/// `n` milliseconds on the shard's monotonic clock.
fn ms(n: u64) -> MonoNs {
    MonoNs(n * 1_000_000)
}

/// The instant a record stamped at `at` stops settling: the lag bound plus the resync bound.
fn floor(at: MonoNs) -> MonoNs {
    at + LAG + RESYNC
}

/// The synthetic venue: GTC and IOC limits, amends by venue id, cancels and queries by venue
/// or client id, a trustworthy snapshot on one read lagging at most [`LAG`].
fn caps() -> OrderCaps {
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

/// [`caps`] on a venue whose resync reads the orders and the positions apart.
fn separate() -> OrderCaps {
    OrderCaps {
        snapshot_reads: SnapshotReads::Separate,
        ..caps()
    }
}

/// [`caps`] with an untrustworthy snapshot source.
fn untrustworthy() -> OrderCaps {
    OrderCaps {
        snapshot_source: SnapshotSource::Untrustworthy,
        ..caps()
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

/// The retention's count and age, under the tests' resync bound.
fn cfg(max_ended: usize, max_age: Duration) -> RetentionConfig {
    RetentionConfig::new(max_ended, max_age, RESYNC).unwrap()
}

/// A pass at `now` on the venue `caps` describe.
fn pass_on(reg: &mut Registry, cfg: &RetentionConfig, caps: &OrderCaps, now: MonoNs) -> Retained {
    reg.retain_ended(cfg, caps, now)
}

/// A pass at `now` on [`caps`].
fn pass(reg: &mut Registry, cfg: &RetentionConfig, now: MonoNs) -> Retained {
    pass_on(reg, cfg, &caps(), now)
}

/// An ingest order no other event of this test binary shares.
fn key() -> OrderKey {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    OrderKey {
        venue: None,
        ingest: NEXT.fetch_add(1, Ordering::Relaxed),
    }
}

fn pre_trade() -> PreTradeCaps {
    PreTradeCaps::new()
        .with_market(
            INST,
            MarketCapsConfig {
                inventory: Some(lots(CAP)),
                resting: Some(lots(CAP)),
            },
        )
        .unwrap()
}

/// A snapshot requested at `at`, its watermark the same instant, of `orders` and the position
/// `pos` on [`INST`].
fn snap(at: MonoNs, orders: Vec<VenueOrderSnapshot>, pos: i64) -> ResyncSnapshot {
    ResyncSnapshot {
        watermark: WallNs(i64::try_from(at.0).unwrap()),
        requested_at: at,
        orders,
        positions: vec![(INST, SignedLots(pos))],
    }
}

fn resync(reg: &mut Registry, caps: &OrderCaps, s: &ResyncSnapshot) -> ResyncReport {
    reg.resync(&ladder_cfg(), caps, s, key()).unwrap()
}

/// A named registry under the caps, seeded flat by a trustworthy resync of `caps`'s read mode
/// requested at 1 ms, armed and Quoting.
fn quoting_on(caps: &OrderCaps) -> Registry {
    let mut reg = arm::named(Registry::with_caps(pre_trade()));
    let report = resync(&mut reg, caps, &snap(ms(1), vec![], 0));
    assert_eq!(report.seeded, vec![(INST, SignedLots(0))]);
    arm::start(&mut reg, INST);
    reg
}

fn quoting() -> Registry {
    quoting_on(&caps())
}

/// A snapshot row of our order `c` (or one with no client id) under `v`, in `state`, with the
/// venue's cumulative fill `cum`, a buy of 1 at 100.
fn row(c: Option<ClientOrderId>, v: &str, state: VenueOrderState, cum: i64) -> VenueOrderSnapshot {
    row_of(c, v, state, 1, cum)
}

fn row_of(
    c: Option<ClientOrderId>,
    v: &str,
    state: VenueOrderState,
    qty: i64,
    cum: i64,
) -> VenueOrderSnapshot {
    VenueOrderSnapshot {
        cid: c.map(CidMatch::Ours),
        vid: vid(v),
        inst: INST,
        side: Side::Buy,
        state,
        px: Some(Ticks(100)),
        qty: lots(qty),
        cum_filled: lots(cum),
        post_only: None,
        reduce_only: None,
    }
}

/// An update of our order `c` (or one naming its venue id alone) on [`INST`], a buy, in
/// `state` with the venue's cumulative fill `cum`.
fn upd(
    reg: &mut Registry,
    c: Option<ClientOrderId>,
    v: Option<&str>,
    state: VenueOrderState,
    cum: i64,
) -> Routed {
    let u = OrderUpdate {
        cid: c.map(CidMatch::Ours),
        vid: v.map(vid),
        inst: INST,
        side: Side::Buy,
        state,
        cum_filled: lots(cum),
        px: None,
        qty: None,
        post_only: None,
        reduce_only: None,
    };
    reg.apply_update(&u, key())
}

fn item(c: ClientOrderId, v: Option<&str>) -> ItemRef {
    ItemRef {
        idx: 0,
        cid: Some(c),
        vid: v.map(vid),
    }
}

fn accepted() -> SubmitOutcome {
    SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    }
}

fn provisional() -> SubmitOutcome {
    SubmitOutcome::Accepted {
        ack: AckLevel::Provisional,
    }
}

/// Accepts the place of `c` under `v`.
fn accept(reg: &mut Registry, c: ClientOrderId, v: &str) {
    reg.on_outcome(c, OrderOp::Place, &item(c, Some(v)), &accepted(), ms(2))
        .unwrap();
}

/// Accepts the place of `c`, naming no venue id.
fn answered(reg: &mut Registry, c: ClientOrderId) {
    reg.on_outcome(c, OrderOp::Place, &item(c, None), &accepted(), ms(2))
        .unwrap();
}

/// A buy of `qty` at 100, placed and authorized: PendingNew, its outcome owed.
fn place_qty(reg: &mut Registry, c: ClientOrderId, qty: i64) {
    place_issued(reg, placement(c, 100, qty));
}

/// A buy of 1, placed, authorized and accepted under `v`, shown Open.
fn open_as(reg: &mut Registry, c: ClientOrderId, v: &str) {
    place_qty(reg, c, 1);
    accept(reg, c, v);
    upd(reg, Some(c), Some(v), VenueOrderState::Open, 0);
}

fn open(reg: &mut Registry, v: &str) -> ClientOrderId {
    let c = cid();
    open_as(reg, c, v);
    c
}

/// [`open`], then cancelled by the venue: terminal, owing nothing.
fn ended(reg: &mut Registry, v: &str) -> ClientOrderId {
    let c = open(reg, v);
    end(reg, c);
    c
}

/// Ends our order `c` Canceled by an update under its client id alone.
fn end(reg: &mut Registry, c: ClientOrderId) {
    let filled = reg.get(c).unwrap().filled().get();
    upd(reg, Some(c), None, canceled(), filled);
    assert!(reg.get(c).unwrap().state().is_terminal());
}

/// Builds the cancel of `c` and hands its authorization to a gateway, which reports it sent
/// under `rpc`.
fn cancel_sent(reg: &mut Registry, c: ClientOrderId, rpc: u64) {
    let cmd = cancel_cmd(reg, c);
    drop(reg.authorize(TEST_ACCT, cmd).unwrap().hand_over());
    assert_eq!(reg.cancel_sent(c, RpcId(rpc), ms(3)), Ok(true));
}

fn cancel_cmd(reg: &mut Registry, c: ClientOrderId) -> fbc_oms::PermittedCommand {
    match reg.cancellable(c).unwrap().cancel(&caps()) {
        CancelChoice::Send(cmd) => cmd,
        CancelChoice::AwaitAck => panic!("the cancel waits for the acknowledgement"),
    }
}

fn ledger() -> FillLedger {
    FillLedger::new(
        LedgerConfig {
            max_age: LONG,
            max_entries: 100_000,
        },
        WallNs(0),
    )
    .unwrap()
}

/// A live fill of 1 lot, a buy, of our order `c` (or none), naming `v` when given and the
/// cumulative fill `cum` after it when given, its fill id `fid`.
fn fill_event(
    c: Option<ClientOrderId>,
    v: Option<&str>,
    cum: Option<i64>,
    fid: &str,
) -> fbc_core::FillEvent {
    fill(
        c,
        FillIdent::Venue {
            fill: fill_id(fid),
            vid: v.map(vid),
            cum_after: cum.map(lots),
        },
        Side::Buy,
        1,
        false,
    )
}

/// Applies `f` through `l`, arriving at `at` with no venue time.
fn apply(
    reg: &mut Registry,
    l: &mut FillLedger,
    f: &fbc_core::FillEvent,
    at: MonoNs,
) -> FillRouted {
    match l.admit(f, None, at) {
        Admission::Apply(a) => reg.apply_fill(a).unwrap(),
        other => panic!("expected the fill accepted, got {other:?}"),
    }
}

/// A fill id no other fill of this test binary shares.
fn fid() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!("f-{}", NEXT.fetch_add(1, Ordering::Relaxed))
}

/// The numbers a cap, Exit's admission, the orphan suppression and the planner read on
/// [`INST`], and the exposure and uncounted fills of each of `cids` (a forgotten one zero).
fn numbers(reg: &Registry, cids: &[ClientOrderId]) -> Vec<String> {
    let mut out = vec![
        format!("inventory {:?}", reg.inventory(INST)),
        format!("position {:?}", reg.position(INST)),
        format!("orphans {:?}", reg.orphans(INST)),
        format!("cooldown {:?}", reg.orphan_cooldown(INST)),
    ];
    for side in [Side::Buy, Side::Sell] {
        out.push(format!("resting {side:?} {:?}", reg.resting_on(INST, side)));
    }
    for &c in cids {
        let (exposure, owed) = reg.get(c).map_or((0, 0), |rec| {
            let owed = rec
                .cum_venue()
                .checked_sub(rec.cum_fills())
                .unwrap_or(lots(0));
            (rec.exposure().get(), owed.get())
        });
        out.push(format!("{c:?} exposure {exposure} owed {owed}"));
    }
    out
}

/// Whether `reg` admits a buy of `qty` under a fresh client id, judged as a place is and then
/// withdrawn, so it changes nothing that counts.
fn admits(reg: &mut Registry, qty: i64) -> Result<(), OmsError> {
    admits_order(reg, placement(cid(), 100, qty))
}

fn admits_order(reg: &mut Registry, order: NewOrder) -> Result<(), OmsError> {
    let cmd = reg.place(order)?;
    reg.place_not_submitted(cmd, NotSentReason::Disconnected);
    Ok(())
}

// ---- the done line: at most the bound ----

#[test]
fn holds_at_most_the_bound() {
    const N: usize = 5;
    const B: usize = 2;
    const K: usize = 2;
    let mut reg = quoting();
    // A cancel sent and unanswered, and a cancel whose authorization is still held.
    let unanswered = open(&mut reg, "b-1");
    cancel_sent(&mut reg, unanswered, 1);
    end(&mut reg, unanswered);
    let held = open(&mut reg, "b-2");
    let cmd = cancel_cmd(&mut reg, held);
    // What a command carries of the orders it names is no part of what it is.
    assert!(format!("{cmd:?}").contains("names: Names(1)"), "{cmd:?}");
    let auth = reg.authorize(TEST_ACCT, cmd).unwrap();
    end(&mut reg, held);
    let gone: Vec<ClientOrderId> = (0..N).map(|n| ended(&mut reg, &format!("n-{n}"))).collect();
    let live: Vec<ClientOrderId> = (0..K).map(|n| open(&mut reg, &format!("k-{n}"))).collect();
    let before = reg.len();
    let bound = cfg(B, LONG);

    let t1 = ms(10_000);
    let first = pass(&mut reg, &bound, t1);
    assert_eq!(first.forgotten, vec![]);
    assert_eq!(first.settling, N);
    assert_eq!(first.kept, 0);
    assert_eq!(
        first.pinned,
        Pinned {
            answer_owed: 1,
            named: 1,
            ..Pinned::default()
        }
    );
    assert_eq!(reg.len(), before);

    let second = pass(&mut reg, &bound, floor(t1));
    // Every one stamped at once: the oldest by client id go, the newest B stay.
    assert_eq!(second.forgotten, gone[..N - B].to_vec());
    assert_eq!((second.kept, second.early, second.settling), (B, N - B, 0));
    assert_eq!(second.pinned.answer_owed + second.pinned.named, 2);
    assert_eq!(reg.len(), B + K + 2);
    for c in live
        .iter()
        .chain(&gone[N - B..])
        .chain([&unanswered, &held])
    {
        assert!(reg.get(*c).is_some(), "{c:?} kept");
    }

    // Answered and dropped, the two go: the oldest by stamp, then by client id.
    reg.on_outcome(
        unanswered,
        OrderOp::Cancel(RpcId(1)),
        &item(unanswered, None),
        &accepted(),
        ms(4),
    )
    .unwrap();
    drop(auth);
    let third = pass(&mut reg, &bound, floor(t1));
    assert_eq!(third.forgotten, vec![unanswered, held]);
    assert_eq!((third.kept, third.early), (B, 2));
    assert_eq!(third.pinned, Pinned::default());
    assert_eq!(reg.len(), B + K);
    for c in &live {
        assert_eq!(reg.get(*c).unwrap().state(), OrdState::Open);
    }
}

// ---- the age and the count ----

#[test]
fn age_and_count() {
    // Age alone: each eligible record goes at the age from its first stamp.
    let mut reg = quoting();
    let by_age = cfg(1_000, Duration::from_secs(1));
    let t0 = ms(10_000);
    let (a, b) = (ended(&mut reg, "a-1"), ended(&mut reg, "a-2"));
    assert_eq!(pass(&mut reg, &by_age, t0).settling, 2);
    let c = ended(&mut reg, "a-3");
    let at = t0 + Duration::from_millis(400);
    assert_eq!(pass(&mut reg, &by_age, at).settling, 3);
    let aged = pass(&mut reg, &by_age, t0 + Duration::from_secs(1));
    assert_eq!(aged.forgotten, vec![a, b]);
    assert_eq!((aged.kept, aged.early), (1, 0));
    assert!(reg.get(c).is_some());
    let just_short = pass(&mut reg, &by_age, at + Duration::from_millis(999));
    assert_eq!((just_short.forgotten.len(), just_short.kept), (0, 1));
    assert_eq!(
        pass(&mut reg, &by_age, at + Duration::from_secs(1)).forgotten,
        vec![c]
    );

    // Count alone: the newest by stamp stay, then by client id; the count's early ones are
    // counted.
    let mut reg = quoting();
    let by_count = cfg(2, LONG);
    let cids: Vec<ClientOrderId> = (0..4).map(|n| open(&mut reg, &format!("c-{n}"))).collect();
    end(&mut reg, cids[3]);
    end(&mut reg, cids[2]);
    assert_eq!(pass(&mut reg, &by_count, t0).settling, 2);
    end(&mut reg, cids[1]);
    pass(&mut reg, &by_count, t0 + Duration::from_millis(100));
    end(&mut reg, cids[0]);
    pass(&mut reg, &by_count, t0 + Duration::from_millis(200));
    let counted = pass(&mut reg, &by_count, t0 + Duration::from_secs(1));
    assert_eq!(counted.forgotten, vec![cids[2], cids[3]]);
    assert_eq!((counted.kept, counted.early), (2, 2));
    assert!(reg.get(cids[0]).is_some() && reg.get(cids[1]).is_some());

    // Two registries fed the same calls forget the same client ids in the same order.
    let (mut x, mut y) = (quoting(), quoting());
    let mut reports = (Vec::new(), Vec::new());
    for round in 0..6u64 {
        for n in 0..5u64 {
            let c = cid();
            let v = format!("t-{round}-{n}");
            open_as(&mut x, c, &v);
            open_as(&mut y, c, &v);
            if n % 2 == 0 {
                end(&mut x, c);
                end(&mut y, c);
            }
        }
        let now = t0 + Duration::from_millis(250 * round);
        let small = cfg(2, Duration::from_millis(700));
        reports.0.push(pass(&mut x, &small, now));
        reports.1.push(pass(&mut y, &small, now));
    }
    assert_eq!(reports.0, reports.1);
    assert!(reports.0.iter().any(|r| r.early > 0));
    assert!(reports.0.iter().any(|r| !r.forgotten.is_empty()));

    // At the end of every clock nothing overflows, and nothing is forgotten.
    let mut reg = quoting();
    let e = ended(&mut reg, "m-1");
    let max = RetentionConfig::new(1_000, Duration::MAX, Duration::MAX).unwrap();
    let endless = OrderCaps {
        snapshot_lag_bound: Duration::MAX,
        ..caps()
    };
    for now in [ms(1), ms(10_000), MonoNs(u64::MAX - 1)] {
        let r = pass_on(&mut reg, &max, &endless, now);
        assert!(r.forgotten.is_empty(), "{r:?}");
    }
    assert!(reg.get(e).is_some());

    // At the clock's last instant, a floor or an age that would end past it is never reached:
    // a record the pass first stamps there is settling, and an age of `Duration::MAX` keeps
    // one stamped earlier.
    let last = MonoNs(u64::MAX);
    let mut reg = quoting();
    let fresh = ended(&mut reg, "m-2");
    let r = pass(&mut reg, &cfg(1_000, LONG), last);
    assert_eq!((r.settling, r.forgotten.len()), (1, 0));
    assert!(reg.get(fresh).is_some());
    let mut reg = quoting();
    let aged = ended(&mut reg, "m-3");
    let forever = cfg(1_000, Duration::MAX);
    pass(&mut reg, &forever, t0);
    let r = pass(&mut reg, &forever, last);
    assert_eq!((r.kept, r.forgotten.len()), (1, 0));
    assert!(reg.get(aged).is_some());
}

// ---- pins ----

#[test]
fn pins_by_reason() {
    let mut reg = quoting();
    let keep = cfg(1_000, Duration::from_millis(1));
    let t1 = ms(10_000);

    // Ended Canceled by an accepted tombstone.
    let tomb = cid();
    place_qty(&mut reg, tomb, 1);
    reg.on_outcome(
        tomb,
        OrderOp::Place,
        &item(tomb, None),
        &SubmitOutcome::Unknown,
        ms(2),
    )
    .unwrap();
    let plan = reg.ladder(&ladder_cfg(), &caps(), ms(20_000));
    let (named, cmd) = plan.tombstones.into_iter().next().expect("a tombstone");
    assert_eq!(named, tomb);
    drop(reg.authorize(TEST_ACCT, cmd).unwrap().hand_over());
    reg.tombstone_sent(tomb, RpcId(7), ms(3)).unwrap();
    assert_eq!(
        reg.on_outcome(
            tomb,
            OrderOp::Cancel(RpcId(7)),
            &item(tomb, None),
            &accepted(),
            ms(4)
        ),
        Ok(OutcomeApplied::TombstoneResolved)
    );

    // Inserted after the market's resync seed.
    let inserted = cid();
    reg.insert(placement(inserted, 100, 1)).unwrap();
    accept(&mut reg, inserted, "i-1");
    end(&mut reg, inserted);

    // Two ended orders each knowing a venue id the index gives to the other.
    let (x, y) = (open(&mut reg, "x-1"), open(&mut reg, "y-1"));
    let amended = |to: &str| VenueOrderState::Amended {
        new_vid: Some(vid(to)),
    };
    upd(&mut reg, Some(x), Some("x-1"), amended("y-1"), 0);
    upd(&mut reg, Some(y), Some("y-1"), amended("x-1"), 0);
    assert_eq!(reg.cid_of(&vid("x-1")), Some(x));
    assert_eq!(reg.cid_of(&vid("y-1")), Some(y));
    end(&mut reg, x);
    end(&mut reg, y);

    // A control the same passes forget, though a clone of its record is kept: a clone takes
    // a fresh handle and pins nothing.
    let control = ended(&mut reg, "c-1");
    let copy = reg.get(control).unwrap().clone();

    let first = pass(&mut reg, &keep, t1);
    assert_eq!(first.settling, 1);
    for now in [floor(t1), floor(t1) + LONG] {
        let r = pass(&mut reg, &keep, now);
        assert_eq!(
            r.pinned,
            Pinned {
                tombstoned: 1,
                inserted: 1,
                shared_vid: 2,
                ..Pinned::default()
            }
        );
        assert_eq!(r.settling, 0);
    }
    assert!(reg.get(control).is_none());
    assert_eq!(copy.cid(), control);
    for c in [tomb, inserted, x, y] {
        assert!(reg.get(c).is_some(), "{c:?} kept");
    }
    assert_eq!(
        reg.get(tomb).unwrap().state(),
        OrdState::Terminal(TerminalKind::Canceled(fbc_core::CancelReason::Requested))
    );

    // Settling: kept until the lag bound plus the resync bound from its stamp, then forgotten.
    let settling = ended(&mut reg, "s-1");
    let t2 = floor(t1) + LONG + LONG;
    assert_eq!(pass(&mut reg, &keep, t2).settling, 1);
    let r = pass(&mut reg, &keep, MonoNs(floor(t2).0 - 1));
    assert_eq!((r.settling, r.forgotten.len()), (1, 0));
    assert!(reg.get(settling).is_some());
    assert_eq!(pass(&mut reg, &keep, floor(t2)).forgotten, vec![settling]);

    // A voided place is kept while its command lives, forgotten once it is dropped, and a
    // trustworthy snapshot then showing it open registers it as an orphan, on both caps.
    let voided = cid();
    let cmd = reg.place(placement(voided, 100, 3)).unwrap();
    assert_eq!(
        reg.on_outcome(
            voided,
            OrderOp::Place,
            &item(voided, Some("v-1")),
            &accepted(),
            ms(5)
        ),
        Err(OmsError::NotIssued(voided))
    );
    assert_eq!(
        reg.get(voided).unwrap().state(),
        OrdState::Terminal(TerminalKind::NotSent(NotSentReason::StaleAuthorization))
    );
    let t3 = floor(t2) + LONG;
    pass(&mut reg, &keep, t3);
    let r = pass(&mut reg, &keep, floor(t3));
    assert_eq!(r.pinned.named, 1);
    assert!(reg.get(voided).is_some());
    drop(cmd);
    assert_eq!(pass(&mut reg, &keep, floor(t3)).forgotten, vec![voided]);
    let before = reg.resting_on(INST, Side::Buy).unwrap();
    let shown = row_of(Some(voided), "v-1", VenueOrderState::Open, 3, 0);
    let report = resync(&mut reg, &caps(), &snap(ms(40_000), vec![shown], 0));
    assert_eq!(report.untracked, vec![(voided, vid("v-1"))]);
    assert_eq!(reg.orphans(INST), vec![voided]);
    assert_eq!(
        reg.resting_on(INST, Side::Buy).unwrap().get(),
        before.get() + 3
    );
    assert_eq!(reg.get(voided).unwrap().exposure().get(), 3);

    unseeded_market_keeps_its_records_for_its_seed();
}

/// [`pins_by_reason`]'s market not yet seeded: its seed's sets read every record there. Kept,
/// the record makes the seed's `unsure` set whole, so a fill of it after the seed is placed as
/// on a twin.
fn unseeded_market_keeps_its_records_for_its_seed() {
    let inst2 = InstrumentId::new(2);
    let keep = cfg(1_000, Duration::from_millis(1));
    let mut regs = [quoting(), quoting()];
    let c = cid();
    for reg in &mut regs {
        let mut order = placement(c, 100, 1);
        order.inst = inst2;
        reg.insert(order).unwrap();
        reg.on_outcome(
            c,
            OrderOp::Place,
            &item(c, None),
            &SubmitOutcome::NotSent(NotSentReason::Disconnected),
            ms(2),
        )
        .unwrap();
    }
    let [reg, _] = &mut regs;
    pass(reg, &keep, ms(10_000));
    let r = pass(reg, &keep, floor(ms(10_000)) + LONG);
    assert_eq!(r.pinned.unseeded, 1);
    assert!(reg.get(c).is_some());
    let mut ledgers = [ledger(), ledger()];
    let f = fill(
        Some(c),
        FillIdent::Venue {
            fill: fill_id(&fid()),
            vid: None,
            cum_after: None,
        },
        Side::Buy,
        1,
        false,
    );
    let mut f = f;
    f.inst = inst2;
    for (reg, l) in regs.iter_mut().zip(&mut ledgers) {
        let s = ResyncSnapshot {
            watermark: WallNs(50),
            requested_at: ms(50),
            orders: vec![],
            positions: vec![(inst2, SignedLots(0))],
        };
        let report = reg.resync(&ladder_cfg(), &caps(), &s, key()).unwrap();
        assert!(report.seeded.contains(&(inst2, SignedLots(0))));
        // Arrived after the seed's request, untimed, of an order the seed held but did not
        // show: nothing places it.
        assert_eq!(apply(reg, l, &f, ms(60)), FillRouted::Unsettled(c));
        assert_eq!(reg.position(inst2), None);
    }
}

#[test]
fn fill_owed_pins_until_the_fill() {
    let mut reg = quoting();
    let mut l = ledger();
    let keep = cfg(1_000, Duration::from_millis(1));
    // A limit IOC whose final update reports a fill before its fill event.
    let c = cid();
    let mut order = placement(c, 100, 2);
    order.tif = Tif::Ioc;
    place_issued(&mut reg, order);
    accept(&mut reg, c, "ioc-1");
    upd(
        &mut reg,
        Some(c),
        Some("ioc-1"),
        VenueOrderState::Canceled(fbc_core::CancelReason::Unfilled),
        1,
    );
    let control = ended(&mut reg, "c-1");
    let t1 = ms(10_000);
    pass(&mut reg, &keep, t1);
    for now in [floor(t1), floor(t1) + LONG] {
        let r = pass(&mut reg, &keep, now);
        assert_eq!(r.pinned.fills_owed, 1);
    }
    assert!(reg.get(control).is_none());
    assert_eq!(reg.get(c).unwrap().exposure().get(), 1);
    // Its fill naming the venue id alone counts once, and the next pass may forget it.
    let by_vid = fill_event(None, Some("ioc-1"), Some(1), &fid());
    assert!(matches!(
        apply(&mut reg, &mut l, &by_vid, ms(20_000)),
        FillRouted::Ours(got, _) if got == c
    ));
    assert_eq!(reg.inventory(INST), SignedLots(1));
    assert_eq!(pass(&mut reg, &keep, floor(t1) + LONG).forgotten, vec![c]);
    // After it, a fill naming that venue id alone is unattributed, its id in view.
    assert!(!reg.foreign_in_view(INST));
    let late = fill_event(None, Some("ioc-1"), Some(2), &fid());
    assert_eq!(
        apply(&mut reg, &mut l, &late, ms(30_000)),
        FillRouted::Unattributed
    );
    assert_eq!(reg.inventory(INST), SignedLots(1));
    assert!(reg.foreign_in_view(INST));
}

#[test]
fn pending_outcome_pins() {
    let keep = cfg(1_000, Duration::from_millis(1));
    let t1 = ms(10_000);
    let keeps = |reg: &mut Registry, c: ClientOrderId, why: fn(&Pinned) -> usize| {
        let control = ended(reg, &format!("ctl-{}", fid()));
        let at = t1;
        pass(reg, &keep, at);
        let r = pass(reg, &keep, floor(at));
        assert_eq!(why(&r.pinned), 1, "{r:?}");
        assert!(reg.get(c).is_some(), "{c:?} kept");
        assert!(reg.get(control).is_none(), "the control is forgotten");
        floor(at)
    };
    let forgets = |reg: &mut Registry, c: ClientOrderId, now: MonoNs| {
        assert_eq!(pass(reg, &keep, now).forgotten, vec![c]);
    };

    // A place authorized and ended by an update before its outcome: its outcome frees it,
    // an Unknown at its deadline included.
    for outcome in [accepted(), SubmitOutcome::Unknown] {
        let mut reg = quoting();
        let c = cid();
        place_qty(&mut reg, c, 1);
        upd(&mut reg, Some(c), Some("p-1"), canceled(), 0);
        let now = keeps(&mut reg, c, |p| p.answer_owed);
        assert_eq!(
            reg.on_outcome(c, OrderOp::Place, &item(c, Some("p-1")), &outcome, ms(5)),
            Ok(OutcomeApplied::Unchanged)
        );
        forgets(&mut reg, c, now);
    }

    // A place authorized and released unsent, its authorization dropped before a gateway took
    // it over, owes no outcome: the pass forgets it.
    let mut reg = quoting();
    let c = cid();
    let cmd = reg.place(placement(c, 100, 1)).unwrap();
    drop(reg.authorize(TEST_ACCT, cmd).unwrap());
    pass(&mut reg, &keep, t1);
    assert_eq!(
        reg.get(c).unwrap().state(),
        OrdState::Terminal(TerminalKind::NotSent(NotSentReason::StaleAuthorization))
    );
    forgets(&mut reg, c, floor(t1));

    // An amend, a cancel and a tombstone, each reported sent and unanswered.
    let mut reg = quoting();
    let c = open(&mut reg, "a-1");
    let amend = reg
        .live(c)
        .unwrap()
        .amend(&caps(), Ticks(101), lots(1), false)
        .unwrap();
    drop(reg.authorize(TEST_ACCT, amend).unwrap().hand_over());
    assert_eq!(
        reg.amend_sent(c, Ticks(101), lots(1), RpcId(11), ms(3)),
        Ok(true)
    );
    end(&mut reg, c);
    let now = keeps(&mut reg, c, |p| p.answer_owed);
    let refused = SubmitOutcome::Rejected(fbc_core::Reject {
        kind: fbc_core::RejectKind::Other,
        venue_code: None,
        raw: "synthetic".into(),
    });
    reg.on_outcome(
        c,
        OrderOp::Amend(RpcId(11)),
        &item(c, None),
        &refused,
        ms(6),
    )
    .unwrap();
    forgets(&mut reg, c, now);

    let mut reg = quoting();
    let c = open(&mut reg, "c-1");
    cancel_sent(&mut reg, c, 12);
    end(&mut reg, c);
    let now = keeps(&mut reg, c, |p| p.answer_owed);
    reg.on_outcome(
        c,
        OrderOp::Cancel(RpcId(12)),
        &item(c, None),
        &SubmitOutcome::Unknown,
        ms(6),
    )
    .unwrap();
    forgets(&mut reg, c, now);

    let mut reg = quoting();
    let c = cid();
    place_qty(&mut reg, c, 1);
    reg.on_outcome(
        c,
        OrderOp::Place,
        &item(c, None),
        &SubmitOutcome::Unknown,
        ms(2),
    )
    .unwrap();
    let plan = reg.ladder(&ladder_cfg(), &caps(), ms(20_000));
    let (_, cmd) = plan.tombstones.into_iter().next().expect("a tombstone");
    drop(reg.authorize(TEST_ACCT, cmd).unwrap().hand_over());
    assert_eq!(reg.tombstone_sent(c, RpcId(13), ms(20_001)), Ok(true));
    upd(&mut reg, Some(c), Some("t-1"), canceled(), 0);
    let now = keeps(&mut reg, c, |p| p.answer_owed);
    reg.on_outcome(
        c,
        OrderOp::Cancel(RpcId(13)),
        &item(c, None),
        &accepted(),
        ms(6),
    )
    .unwrap();
    forgets(&mut reg, c, now);

    // An amend and a cancel built and not yet reported: as a command, an authorization or a
    // handed one.
    for (amend, held) in [false, true]
        .into_iter()
        .flat_map(|a| (0..3).map(move |h| (a, h)))
    {
        let mut reg = quoting();
        let c = open(&mut reg, "h-1");
        let cmd = if amend {
            reg.live(c)
                .unwrap()
                .amend(&caps(), Ticks(101), lots(1), false)
                .unwrap()
        } else {
            cancel_cmd(&mut reg, c)
        };
        let kept: Box<dyn std::any::Any> = match held {
            0 => Box::new(cmd),
            1 => Box::new(reg.authorize(TEST_ACCT, cmd).unwrap()),
            _ => Box::new(reg.authorize(TEST_ACCT, cmd).unwrap().hand_over()),
        };
        end(&mut reg, c);
        let now = keeps(&mut reg, c, |p| p.named);
        drop(kept);
        forgets(&mut reg, c, now);
    }

    // The documented gap: a pass after a handed cancel is dropped and before its report
    // forgets the order; its report and its answer then change nothing. For an order built
    // here and one a resync registered from its snapshot.
    for from_snapshot in [false, true] {
        let mut reg = arm::named(Registry::with_caps(pre_trade()));
        let c = cid();
        if from_snapshot {
            let shown = row(Some(c), "g-1", VenueOrderState::Open, 0);
            let report = resync(&mut reg, &caps(), &snap(ms(1), vec![shown], 0));
            assert_eq!(report.registered, vec![c]);
        } else {
            resync(&mut reg, &caps(), &snap(ms(1), vec![], 0));
            arm::start(&mut reg, INST);
            open_as(&mut reg, c, "g-1");
        }
        let cmd = cancel_cmd(&mut reg, c);
        drop(reg.authorize(TEST_ACCT, cmd).unwrap().hand_over());
        end(&mut reg, c);
        pass(&mut reg, &keep, t1);
        assert_eq!(pass(&mut reg, &keep, floor(t1)).forgotten, vec![c]);
        assert_eq!(reg.cancel_sent(c, RpcId(14), ms(7)), Ok(false));
        assert_eq!(
            reg.on_outcome(
                c,
                OrderOp::Cancel(RpcId(14)),
                &item(c, None),
                &accepted(),
                ms(8)
            ),
            Ok(OutcomeApplied::Unchanged)
        );
    }

    // A second outcome of one request naming a forgotten order changes nothing: a final
    // acceptance after a provisional one, and a late answer after an Unknown.
    for first in [provisional(), SubmitOutcome::Unknown] {
        let mut reg = quoting();
        let c = open(&mut reg, "s-1");
        cancel_sent(&mut reg, c, 15);
        reg.on_outcome(c, OrderOp::Cancel(RpcId(15)), &item(c, None), &first, ms(4))
            .unwrap();
        end(&mut reg, c);
        pass(&mut reg, &keep, t1);
        assert_eq!(pass(&mut reg, &keep, floor(t1)).forgotten, vec![c]);
        assert_eq!(
            reg.on_outcome(
                c,
                OrderOp::Cancel(RpcId(15)),
                &item(c, None),
                &accepted(),
                ms(9)
            ),
            Ok(OutcomeApplied::Unchanged)
        );
    }

    // The answer span runs from the lowest client id forgotten to the highest: a pass forgets
    // an earlier run's order a resync registered and one this registry built; a client id
    // outside it is still unknown, and inside it a second send time or nonce is not refused.
    let below = cid();
    let earlier = cid();
    let mut reg = arm::named(Registry::with_caps(pre_trade()));
    let mut twin = arm::named(Registry::with_caps(pre_trade()));
    let built = cid();
    for r in [&mut reg, &mut twin] {
        let shown = row(Some(earlier), "e-1", VenueOrderState::Open, 0);
        assert_eq!(
            resync(r, &caps(), &snap(ms(1), vec![shown], 0)).registered,
            vec![earlier]
        );
        arm::start(r, INST);
        open_as(r, built, "e-2");
        r.placement_sent(built, ms(2), WallNs(2)).unwrap();
        r.placement_nonce_used(built, 7).unwrap();
        end(r, earlier);
        end(r, built);
    }
    let above = cid();
    pass(&mut reg, &keep, t1);
    assert_eq!(
        pass(&mut reg, &keep, floor(t1)).forgotten,
        vec![earlier, built]
    );
    for outside in [below, above] {
        let unknown = OmsError::UnknownCid(outside);
        assert_eq!(
            reg.on_outcome(
                outside,
                OrderOp::Place,
                &item(outside, None),
                &accepted(),
                ms(9)
            ),
            Err(unknown.clone())
        );
        assert_eq!(
            reg.cancel_sent(outside, RpcId(16), ms(9)),
            Err(unknown.clone())
        );
        assert_eq!(reg.placement_sent(outside, ms(9), WallNs(9)), Err(unknown));
    }
    assert_eq!(reg.placement_sent(built, ms(9), WallNs(9)), Ok(()));
    assert_eq!(
        reg.amend_sent(built, Ticks(101), lots(1), RpcId(17), ms(9)),
        Ok(false)
    );
    assert_eq!(reg.placement_nonce_used(built, 8), Ok(()));
    assert_eq!(
        twin.placement_sent(built, ms(9), WallNs(9)),
        Err(OmsError::SentRecorded(built))
    );
    assert_eq!(
        twin.placement_nonce_used(built, 8),
        Err(OmsError::NonceRecorded(built))
    );
}

#[test]
fn slot_pins_until_freed_or_dropped() {
    let keep = cfg(1_000, Duration::from_millis(1));
    let t1 = ms(10_000);
    let caps = caps();
    let quote = DesiredQuote {
        px: Ticks(100),
        qty: lots(1),
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        reducing: false,
        tag: 0,
        reduce_fallback: ReduceFallback::KeepReduceOnly,
    };
    let wanted = DesiredBook::new(INST).with(Side::Buy, 0, quote.clone());
    for drop_planner in [false, true] {
        let mut reg = quoting();
        let mut mint = mint();
        let mut planner = planner();
        let plan = planner
            .plan(
                &wanted,
                &mut reg,
                &venue::of(&caps),
                TEST_ACCT,
                &mut mint,
                ms(5),
            )
            .unwrap();
        let c = plan.commands[0].cid;
        for p in plan.commands {
            drop(p.auth.hand_over());
        }
        accept(&mut reg, c, "s-1");
        end(&mut reg, c);
        if drop_planner {
            drop(planner);
            pass(&mut reg, &keep, t1);
            assert_eq!(pass(&mut reg, &keep, floor(t1)).forgotten, vec![c]);
            continue;
        }
        pass(&mut reg, &keep, t1);
        for now in [floor(t1), floor(t1) + LONG] {
            let r = pass(&mut reg, &keep, now);
            assert_eq!((r.pinned.named, r.forgotten.len()), (1, 0));
        }
        assert_eq!(planner.order_at(TEST_ACCT, INST, Side::Buy, 0), Some(c));
        let freed = planner
            .plan(
                &DesiredBook::new(INST),
                &mut reg,
                &venue::of(&caps),
                TEST_ACCT,
                &mut mint,
                ms(6),
            )
            .unwrap();
        assert!(freed.commands.is_empty());
        assert_eq!(planner.order_at(TEST_ACCT, INST, Side::Buy, 0), None);
        assert_eq!(pass(&mut reg, &keep, floor(t1) + LONG).forgotten, vec![c]);
    }

    // Under declared batching, the planner merges its places into one batch and its cancels
    // into one cancel-many. Each keeps its orders while its authorization lives, after the
    // planner is dropped (item 3d).
    let batching = OrderCaps {
        batch_place: Some(Batch { max_items: 10 }),
        batch_cancel: Some(CancelBatch {
            max_items: 10,
            refs: TagSet::of(&[RefKind::Venue]),
        }),
        ..caps.clone()
    };
    let declared = PlanVenue {
        batching: Batching::Declared,
        ..venue::of(&batching)
    };
    let lower = DesiredQuote {
        px: Ticks(99),
        ..quote.clone()
    };
    let two = DesiredBook::new(INST)
        .with(Side::Buy, 0, quote)
        .with(Side::Buy, 1, lower);

    // The cancel-many, both its orders acknowledged and Open.
    let mut reg = quoting();
    let control = ended(&mut reg, "s-ctl");
    let mut ids = mint();
    let mut planning = planner();
    let mut plan = planning
        .plan(&two, &mut reg, &declared, TEST_ACCT, &mut ids, ms(5))
        .unwrap();
    let placed = plan.commands.remove(0);
    assert!(plan.commands.is_empty());
    let cids = placed.cids();
    drop(placed.auth.hand_over());
    for (idx, &c) in (0u16..).zip(&cids) {
        let v = format!("s-{idx}");
        let named = ItemRef {
            idx,
            cid: Some(c),
            vid: Some(vid(&v)),
        };
        reg.on_outcome(c, OrderOp::Place, &named, &accepted(), ms(6))
            .unwrap();
        upd(&mut reg, Some(c), Some(&v), VenueOrderState::Open, 0);
    }
    let mut plan = planning
        .plan(
            &DesiredBook::new(INST),
            &mut reg,
            &declared,
            TEST_ACCT,
            &mut ids,
            ms(1_000),
        )
        .unwrap();
    assert_eq!(plan.commands.len(), 1);
    let cancel = plan.commands.remove(0);
    assert_eq!((cancel.stage, cancel.cids()), (Stage::Cancel, cids.clone()));
    drop(planning);
    for &c in &cids {
        end(&mut reg, c);
    }
    pass(&mut reg, &keep, t1);
    let r = pass(&mut reg, &keep, floor(t1));
    assert_eq!((r.pinned.named, r.forgotten), (2, vec![control]));
    drop(cancel);
    let mut both = cids;
    both.sort();
    assert_eq!(pass(&mut reg, &keep, floor(t1)).forgotten, both);

    // The batch, both its places answered.
    let mut reg = quoting();
    let control = ended(&mut reg, "b-ctl");
    let mut ids = mint();
    let mut batcher = planner();
    let mut plan = batcher
        .plan(&two, &mut reg, &declared, TEST_ACCT, &mut ids, ms(5))
        .unwrap();
    assert_eq!(plan.commands.len(), 1);
    let batch = plan.commands.remove(0);
    let cids = batch.cids();
    assert_eq!(cids.len(), 2);
    let handed = batch.auth.hand_over();
    drop(batcher);
    for (idx, &c) in (0u16..).zip(&cids) {
        let named = ItemRef {
            idx,
            cid: Some(c),
            vid: Some(vid(&format!("b-{idx}"))),
        };
        reg.on_outcome(c, OrderOp::Place, &named, &accepted(), ms(6))
            .unwrap();
        end(&mut reg, c);
    }
    pass(&mut reg, &keep, t1);
    let r = pass(&mut reg, &keep, floor(t1));
    assert_eq!((r.pinned.named, r.forgotten), (2, vec![control]));
    drop(handed);
    let mut both = cids;
    both.sort();
    assert_eq!(pass(&mut reg, &keep, floor(t1)).forgotten, both);
}

#[test]
fn ladder_query_out_when_forgotten() {
    let keep = cfg(1_000, Duration::from_millis(1));
    let mut regs = [quoting(), quoting()];
    let mut ledgers = [ledger(), ledger()];
    let c = cid();
    let rpc = RpcId(21);
    for (reg, l) in regs.iter_mut().zip(&mut ledgers) {
        place_qty(reg, c, 2);
        reg.on_outcome(
            c,
            OrderOp::Place,
            &item(c, None),
            &SubmitOutcome::Unknown,
            ms(2),
        )
        .unwrap();
        let plan = reg.ladder(&ladder_cfg(), &caps(), ms(3));
        assert_eq!(plan.queries.len(), 1);
        reg.query_sent(c, rpc).unwrap();
        // Its venue id, and another a fill taught it.
        upd(reg, Some(c), Some("q-1"), VenueOrderState::Open, 0);
        let taught = fill_event(Some(c), Some("q-2"), Some(1), "q-fill");
        assert!(matches!(
            apply(reg, l, &taught, ms(4)),
            FillRouted::Ours(..)
        ));
        assert_eq!(reg.cid_of(&vid("q-2")), Some(c));
        upd(reg, Some(c), Some("q-1"), canceled(), 1);
    }
    let [reg, twin] = &mut regs;
    pass(reg, &keep, ms(10_000));
    assert_eq!(pass(reg, &keep, floor(ms(10_000))).forgotten, vec![c]);
    assert_eq!(reg.cid_of(&vid("q-1")), None);
    assert_eq!(reg.cid_of(&vid("q-2")), None);
    assert_eq!(reg.queries_awaited(), 0);

    // Nothing panics on the next ladder pass, the query's answer or outcome, or a resync naming
    // it or either venue id.
    reg.ladder(&ladder_cfg(), &caps(), ms(20_000));
    let shown = row(Some(c), "q-1", VenueOrderState::Open, 1);
    let answer = QueryAnswer::new(rpc, OrderRef::Client(c), Some(shown.clone())).unwrap();
    assert_eq!(
        reg.on_query_answer(&caps(), &answer, key()),
        LadderResolution::Ignored
    );
    assert_eq!(
        reg.on_query_outcome(rpc, &SubmitOutcome::Unknown),
        LadderResolution::Ignored
    );
    let rows = [shown, row(None, "q-2", VenueOrderState::Open, 1)];
    reg.on_resync(&ladder_cfg(), &caps(), WallNs(20), &rows, key());

    // A late query report and tombstone report answer as the twin's terminal record does.
    let awaited = reg.queries_awaited();
    assert_eq!(reg.query_sent(c, RpcId(22)), Ok(()));
    assert_eq!(twin.query_sent(c, RpcId(22)), Ok(()));
    assert_eq!(reg.queries_awaited(), awaited);
    assert_eq!(reg.tombstone_sent(c, RpcId(23), ms(21_000)), Ok(false));
    assert_eq!(twin.tombstone_sent(c, RpcId(23), ms(21_000)), Ok(false));
    reg.ladder(&ladder_cfg(), &caps(), ms(22_000));
    twin.ladder(&ladder_cfg(), &caps(), ms(22_000));
}

// ---- 0108's re-registration ----

/// Puts our order `c`, placed and sent at 2 ms, on the Unknown ladder and ends it Lost on a
/// trustworthy snapshot that does not show it, requested at `at`.
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
    let report = resync(reg, &caps(), &snap(at, vec![], 0));
    assert_eq!(report.ladder.lost, vec![c]);
}

#[test]
fn lost_is_kept_and_re_registers() {
    let keep = cfg(1_000, Duration::from_millis(1));
    let t1 = ms(10_000);
    for reads in [SnapshotReads::OneInstant, SnapshotReads::Separate] {
        let venue = OrderCaps {
            snapshot_reads: reads,
            ..caps()
        };
        let mut regs = [quoting_on(&venue), quoting_on(&venue)];
        let mut ledgers = [ledger(), ledger()];
        let c = cid();
        let mut tombs = Vec::new();
        for reg in &mut regs {
            place_qty(reg, c, 1);
            reg.placement_sent(c, ms(2), WallNs(2_000_000)).unwrap();
            reg.on_outcome(
                c,
                OrderOp::Place,
                &item(c, None),
                &SubmitOutcome::Unknown,
                ms(2),
            )
            .unwrap();
            // A tombstone built and held, and one sent and not answered.
            let plan = reg.ladder(&ladder_cfg(), &caps(), ms(20_000));
            let (_, cmd) = plan.tombstones.into_iter().next().expect("a tombstone");
            drop(reg.authorize(TEST_ACCT, cmd).unwrap().hand_over());
            reg.tombstone_sent(c, RpcId(31), ms(20_001)).unwrap();
            let plan = reg.ladder(&ladder_cfg(), &caps(), ms(30_001));
            tombs.push(plan.tombstones.into_iter().next().expect("a tombstone").1);
            let report = resync(reg, &venue, &snap(ms(30_002), vec![], 0));
            assert_eq!(report.ladder.lost, vec![c]);
        }
        let [reg, twin] = &mut regs;
        pass_on(reg, &keep, &venue, t1);
        for now in [floor(t1), floor(t1) + LONG] {
            let r = pass_on(reg, &keep, &venue, now);
            assert_eq!(r.pinned.lost, 1);
        }
        // A trustworthy snapshot showing it open registers it again, as on the twin.
        let shown = row(Some(c), "l-1", VenueOrderState::Open, 0);
        for r in [&mut *reg, &mut *twin] {
            let report = resync(r, &venue, &snap(ms(40_000), vec![shown.clone()], 0));
            assert_eq!(report.untracked, vec![(c, vid("l-1"))]);
        }
        let (a, b) = (reg.get(c).unwrap(), twin.get(c).unwrap());
        assert_eq!(a.state(), b.state());
        assert_eq!(
            (a.cum_fills(), a.cum_venue()),
            (b.cum_fills(), b.cum_venue())
        );
        assert_eq!(
            a.superseded_vids().collect::<Vec<_>>(),
            b.superseded_vids().collect::<Vec<_>>()
        );
        assert!(a.from_snapshot());
        // It ends; the held tombstone, then the tombstone unanswered, keep it.
        for r in [&mut *reg, &mut *twin] {
            end(r, c);
        }
        let t2 = floor(t1) + LONG;
        pass_on(reg, &keep, &venue, t2);
        let r = pass_on(reg, &keep, &venue, floor(t2));
        assert_eq!((r.pinned.answer_owed, r.forgotten.len()), (1, 0));
        assert_eq!(
            reg.on_outcome(
                c,
                OrderOp::Cancel(RpcId(31)),
                &item(c, None),
                &SubmitOutcome::Unknown,
                ms(41_000)
            ),
            Ok(OutcomeApplied::Unchanged)
        );
        let r = pass_on(reg, &keep, &venue, floor(t2));
        assert_eq!((r.pinned.named, r.forgotten.len()), (1, 0));
        tombs.clear();
        // Forgotten, as the highest client id in its namespace's built span.
        assert_eq!(pass_on(reg, &keep, &venue, floor(t2)).forgotten, vec![c]);
        // A late fill under its client id moves the inventory once; the position stays known.
        let f = fill_event(Some(c), None, None, &fid());
        let [la, lb] = &mut ledgers;
        assert_eq!(apply(reg, la, &f, ms(50_000)), FillRouted::OursUntracked(c));
        assert!(matches!(
            apply(twin, lb, &f, ms(50_000)),
            FillRouted::Ours(..)
        ));
        assert_eq!(reg.inventory(INST), SignedLots(1));
        assert_eq!(reg.position(INST), twin.position(INST));
        assert_eq!(reg.position(INST), Some(SignedLots(1)));
        // A straggler answer to its tombstone changes nothing.
        assert_eq!(
            reg.on_outcome(
                c,
                OrderOp::Cancel(RpcId(31)),
                &item(c, None),
                &accepted(),
                ms(51_000)
            ),
            Ok(OutcomeApplied::Unchanged)
        );
    }

    lost_is_kept_by_the_planner_slot_that_held_it();
    lost_owing_its_place_outcome();
}

/// [`lost_is_kept_and_re_registers`]'s owed place outcome: a Lost order whose place outcome
/// never came, the ladder's intent timeout having sent it Unknown, gives its replacement that
/// owed outcome, which keeps it once it ends until the outcome comes (item 5).
fn lost_owing_its_place_outcome() {
    let keep = cfg(1_000, Duration::from_millis(1));
    let mut reg = quoting();
    let control = ended(&mut reg, "w-ctl");
    let c = cid();
    place_qty(&mut reg, c, 1);
    reg.placement_sent(c, ms(2), WallNs(2_000_000)).unwrap();
    reg.ladder(&ladder_cfg(), &caps(), ms(1_500));
    assert_eq!(reg.get(c).unwrap().state(), OrdState::Unknown);
    let report = resync(&mut reg, &caps(), &snap(ms(5_000), vec![], 0));
    assert_eq!(report.ladder.lost, vec![c]);
    let shown = row(Some(c), "w-1", VenueOrderState::Open, 0);
    let report = resync(&mut reg, &caps(), &snap(ms(6_000), vec![shown], 0));
    assert_eq!(report.untracked, vec![(c, vid("w-1"))]);
    assert!(reg.get(c).unwrap().from_snapshot());
    end(&mut reg, c);
    let t1 = ms(10_000);
    pass(&mut reg, &keep, t1);
    let r = pass(&mut reg, &keep, floor(t1));
    assert_eq!((r.pinned.answer_owed, r.forgotten), (1, vec![control]));
    assert_eq!(
        reg.on_outcome(
            c,
            OrderOp::Place,
            &item(c, None),
            &SubmitOutcome::Unknown,
            ms(10_001)
        ),
        Ok(OutcomeApplied::Unchanged)
    );
    assert_eq!(pass(&mut reg, &keep, floor(t1)).forgotten, vec![c]);
}

/// [`lost_is_kept_and_re_registers`]'s planner slot: one that held the Lost record holds its
/// replacement.
fn lost_is_kept_by_the_planner_slot_that_held_it() {
    let keep = cfg(1_000, Duration::from_millis(1));
    let caps = caps();
    let mut reg = quoting();
    let mut mint = mint();
    let mut planner = planner();
    let quote = DesiredQuote {
        px: Ticks(100),
        qty: lots(1),
        tif: Tif::Gtc,
        channel: Channel::Public,
        post_only: true,
        reduce_only: false,
        reducing: false,
        tag: 0,
        reduce_fallback: ReduceFallback::KeepReduceOnly,
    };
    let wanted = DesiredBook::new(INST).with(Side::Buy, 0, quote);
    let plan = planner
        .plan(
            &wanted,
            &mut reg,
            &venue::of(&caps),
            TEST_ACCT,
            &mut mint,
            ms(1),
        )
        .unwrap();
    let c = plan.commands[0].cid;
    for p in plan.commands {
        drop(p.auth.hand_over());
    }
    lose(&mut reg, c, ms(30_000));
    let shown = row(Some(c), "l-1", VenueOrderState::Open, 0);
    resync(&mut reg, &caps, &snap(ms(40_000), vec![shown], 0));
    end(&mut reg, c);
    let t1 = ms(50_000);
    pass(&mut reg, &keep, t1);
    let r = pass(&mut reg, &keep, floor(t1));
    assert_eq!((r.pinned.named, r.forgotten.len()), (1, 0));
    planner
        .plan(
            &DesiredBook::new(INST),
            &mut reg,
            &venue::of(&caps),
            TEST_ACCT,
            &mut mint,
            ms(50_001),
        )
        .unwrap();
    assert_eq!(pass(&mut reg, &keep, floor(t1)).forgotten, vec![c]);
}

// ---- late events ----

#[test]
fn late_fill_by_client_id_counts_as_held() {
    let keep = cfg(1_000, Duration::from_millis(1));
    let t1 = ms(10_000);
    // On separate reads and on one read, a forgotten order built after its market's seed.
    for venue in [caps(), separate()] {
        let mut regs = [quoting_on(&venue), quoting_on(&venue)];
        let mut ledgers = [ledger(), ledger()];
        let c = cid();
        for reg in &mut regs {
            open_as(reg, c, "o-1");
            end(reg, c);
        }
        let [reg, twin] = &mut regs;
        pass_on(reg, &keep, &venue, t1);
        assert_eq!(pass_on(reg, &keep, &venue, floor(t1)).forgotten, vec![c]);
        let f = fill_event(Some(c), None, None, &fid());
        let [la, lb] = &mut ledgers;
        assert_eq!(apply(reg, la, &f, ms(20_000)), FillRouted::OursUntracked(c));
        assert_eq!(
            apply(twin, lb, &f, ms(20_000)),
            FillRouted::Ours(c, fbc_oms::FillApplied::AfterEnd)
        );
        assert_eq!(reg.inventory(INST), SignedLots(1));
        assert_eq!(reg.position(INST), Some(SignedLots(1)));
        assert_eq!(numbers(reg, &[]), numbers(twin, &[]));
    }

    // An order whose terminal update understated its fills, then two late fills out of order.
    let mut regs = [quoting(), quoting()];
    let mut ledgers = [ledger(), ledger()];
    let c = cid();
    for (reg, l) in regs.iter_mut().zip(&mut ledgers) {
        place_qty(reg, c, 3);
        accept(reg, c, "u-1");
        let first = fill_event(Some(c), Some("u-1"), Some(1), "u-f1");
        apply(reg, l, &first, ms(3));
        upd(reg, Some(c), None, canceled(), 1);
    }
    let [reg, twin] = &mut regs;
    pass(reg, &keep, t1);
    assert_eq!(pass(reg, &keep, floor(t1)).forgotten, vec![c]);
    let [la, lb] = &mut ledgers;
    let third = fill_event(Some(c), None, Some(3), "u-f3");
    apply(reg, la, &third, ms(20_000));
    apply(twin, lb, &third, ms(20_000));
    assert_eq!(twin.get(c).unwrap().exposure().get(), 1);
    assert_eq!(reg.inventory(INST), twin.inventory(INST));
    let room = CAP - reg.inventory(INST).0;
    assert!(matches!(admits(twin, room), Err(OmsError::Capped(_))));
    assert_eq!(admits(reg, room), Ok(()));
    let second = fill_event(Some(c), None, Some(2), "u-f2");
    apply(reg, la, &second, ms(20_001));
    apply(twin, lb, &second, ms(20_001));
    assert_eq!(reg.inventory(INST), twin.inventory(INST));
    let room = CAP - reg.inventory(INST).0;
    assert_eq!(admits(twin, room), Ok(()));
    assert_eq!(admits(reg, room), Ok(()));

    // An order an insert registered after its market's resync seed is kept while that seed
    // stands; once the market is unsettled, it may go, and its late fill counts as held.
    let mut regs = [quoting_on(&separate()), quoting_on(&separate())];
    let mut ledgers = [ledger(), ledger()];
    let c = cid();
    for reg in &mut regs {
        reg.insert(placement(c, 100, 1)).unwrap();
        accept(reg, c, "i-1");
        end(reg, c);
    }
    let [reg, twin] = &mut regs;
    pass_on(reg, &keep, &separate(), t1);
    for now in [floor(t1), floor(t1) + LONG] {
        assert_eq!(pass_on(reg, &keep, &separate(), now).pinned.inserted, 1);
    }
    let stranger = fill_event(Some(cid()), None, None, &fid());
    let [la, lb] = &mut ledgers;
    for (r, l) in [(&mut *reg, &mut *la), (&mut *twin, &mut *lb)] {
        assert!(matches!(
            apply(r, l, &stranger, ms(20_000)),
            FillRouted::Unsettled(_)
        ));
        assert_eq!(r.position(INST), None);
    }
    assert_eq!(
        pass_on(reg, &keep, &separate(), floor(t1) + LONG).forgotten,
        vec![c]
    );
    let late = fill_event(Some(c), None, None, &fid());
    assert_eq!(
        apply(reg, la, &late, ms(20_001)),
        FillRouted::OursUntracked(c)
    );
    assert!(matches!(
        apply(twin, lb, &late, ms(20_001)),
        FillRouted::Ours(..)
    ));
    assert_eq!(reg.inventory(INST), twin.inventory(INST));

    // An order inserted before its market's seed, and one a resync registered from its
    // snapshot: each places its late fill as its held record did, its client id inside the
    // built span and outside it.
    for inside in [false, true] {
        for from_snapshot in [false, true] {
            late_fill_of_an_order_held_at_the_seed(inside, from_snapshot);
        }
    }
}

/// The last case of [`late_fill_by_client_id_counts_as_held`].
fn late_fill_of_an_order_held_at_the_seed(inside: bool, from_snapshot: bool) {
    let keep = cfg(1_000, Duration::from_millis(1));
    let t1 = ms(10_000);
    let low = cid();
    let c = cid();
    let high = cid();
    let (low, c) = if inside { (low, c) } else { (c, low) };
    let mut regs = [
        arm::named(Registry::with_caps(pre_trade())),
        arm::named(Registry::with_caps(pre_trade())),
        arm::named(Registry::with_caps(pre_trade())),
    ];
    let mut ledgers = [ledger(), ledger()];
    for reg in &mut regs {
        let shown = row(Some(c), "h-1", VenueOrderState::Open, 0);
        if from_snapshot {
            let report = resync(reg, &caps(), &snap(ms(1), vec![shown], 0));
            assert_eq!(report.registered, vec![c]);
        } else {
            reg.insert(placement(c, 100, 1)).unwrap();
            accept(reg, c, "h-1");
            resync(reg, &caps(), &snap(ms(1), vec![shown], 0));
        }
        arm::start(reg, INST);
        open_as(reg, low, "h-low");
        open_as(reg, high, "h-high");
        for o in [low, c, high] {
            end(reg, o);
        }
    }
    let [reg, twin, spare] = &mut regs;
    let mut all = vec![low, c, high];
    all.sort();
    for r in [&mut *reg, &mut *spare] {
        pass(r, &keep, t1);
        assert_eq!(pass(r, &keep, floor(t1)).forgotten, all);
    }
    // After the seed's request, with a cumulative fill past what the seed showed: after it.
    let f = fill_event(Some(c), None, Some(1), &fid());
    let [la, lb] = &mut ledgers;
    assert_eq!(apply(reg, la, &f, ms(20_000)), FillRouted::OursUntracked(c));
    assert!(matches!(
        apply(twin, lb, &f, ms(20_000)),
        FillRouted::Ours(..)
    ));
    assert_eq!(numbers(reg, &[]), numbers(twin, &[]));
    assert_eq!(reg.inventory(INST), SignedLots(1));
    // Only the orders this registry built widen the built span (items 5 to 7): inside it, the
    // late fill records no live sighting and an insert under the client id is refused; outside
    // it, the fill records one, as for an order never held, so an untrustworthy snapshot
    // showing the order open registers it as an orphan, and the insert is admitted.
    let shown = row(Some(c), "h-1", VenueOrderState::Open, 1);
    let report = resync(reg, &untrustworthy(), &snap(ms(30_000), vec![shown], 1));
    assert!(report.untrustworthy);
    let inserted = spare.insert(placement(c, 100, 1)).map(|_| ());
    if inside {
        assert!(reg.orphans(INST).is_empty());
        assert_eq!(inserted, Err(OmsError::DuplicateCid(c)));
    } else {
        assert_eq!(reg.orphans(INST), vec![c]);
        assert_eq!(inserted, Ok(()));
    }
}

#[test]
fn shared_vid_pins() {
    let keep = cfg(1_000, Duration::from_millis(1));
    let t1 = ms(10_000);
    // An update under B's client id names A's venue id, and A has ended.
    let mut regs = [quoting(), quoting()];
    let mut ledgers = [ledger(), ledger()];
    let (a, b) = (cid(), cid());
    for reg in &mut regs {
        open_as(reg, a, "va");
        end(reg, a);
        place_qty(reg, b, 1);
        answered(reg, b);
        upd(reg, Some(b), Some("va"), VenueOrderState::Open, 0);
        assert_eq!(reg.cid_of(&vid("va")), Some(a));
    }
    let [reg, twin] = &mut regs;
    pass(reg, &keep, t1);
    for now in [floor(t1), floor(t1) + LONG] {
        assert_eq!(pass(reg, &keep, now).pinned.shared_vid, 1);
    }
    let [la, lb] = &mut ledgers;
    for (r, l) in [(&mut *reg, &mut *la), (&mut *twin, &mut *lb)] {
        assert_eq!(
            upd(r, None, Some("va"), canceled(), 0),
            Routed::Ours(a, Applied::IgnoredLate)
        );
        let f = fill_event(None, Some("va"), None, "va-fill");
        assert_eq!(
            apply(r, l, &f, ms(20_000)),
            FillRouted::Ours(a, fbc_oms::FillApplied::AfterEnd)
        );
    }
    assert_eq!(numbers(reg, &[a, b]), numbers(twin, &[a, b]));
    for r in [&mut *reg, &mut *twin] {
        end(r, b);
    }
    let t2 = floor(t1) + LONG;
    pass(reg, &keep, t2);
    assert_eq!(pass(reg, &keep, floor(t2)).forgotten, vec![b]);
    assert_eq!(pass(reg, &keep, floor(t2)).forgotten, vec![a]);

    // An update under X's client id names Y's venue id while Y rests, and X then ends.
    let mut regs = [quoting(), quoting()];
    let (x, y) = (cid(), cid());
    for reg in &mut regs {
        open_as(reg, y, "vy");
        place_qty(reg, x, 1);
        answered(reg, x);
        upd(reg, Some(x), Some("vy"), VenueOrderState::Open, 0);
        end(reg, x);
    }
    let [reg, twin] = &mut regs;
    pass(reg, &keep, t1);
    for now in [floor(t1), floor(t1) + LONG] {
        assert_eq!(pass(reg, &keep, now).pinned.shared_vid, 1);
    }
    for r in [&mut *reg, &mut *twin] {
        upd(r, Some(x), Some("vy"), canceled(), 0);
        let shown = row(Some(x), "vy", canceled(), 0);
        resync(r, &caps(), &snap(ms(20_000), vec![shown], 0));
        assert_eq!(r.get(y).unwrap().state(), OrdState::Open);
    }
    assert_eq!(numbers(reg, &[x, y]), numbers(twin, &[x, y]));

    // With Y ended Lost instead, X is kept, and a late terminal update under X's client id
    // leaves Y to be registered again by a snapshot showing it open, as on the twin.
    let mut regs = [quoting(), quoting()];
    let mut ledgers = [ledger(), ledger()];
    let (x, y) = (cid(), cid());
    for reg in &mut regs {
        place_qty(reg, y, 1);
        accept(reg, y, "vy");
        reg.placement_sent(y, ms(2), WallNs(2_000_000)).unwrap();
        cancel_sent(reg, y, 41);
        reg.on_outcome(
            y,
            OrderOp::Cancel(RpcId(41)),
            &item(y, None),
            &SubmitOutcome::Unknown,
            ms(3),
        )
        .unwrap();
        let report = resync(reg, &caps(), &snap(ms(5_000), vec![], 0));
        assert_eq!(report.ladder.lost, vec![y]);
        place_qty(reg, x, 1);
        answered(reg, x);
        upd(reg, Some(x), Some("vy"), VenueOrderState::Open, 0);
        end(reg, x);
    }
    let [reg, twin] = &mut regs;
    pass(reg, &keep, t1);
    for now in [floor(t1), floor(t1) + LONG] {
        let r = pass(reg, &keep, now);
        assert_eq!((r.pinned.lost, r.pinned.shared_vid), (1, 1));
    }
    for r in [&mut *reg, &mut *twin] {
        upd(r, Some(x), Some("vy"), canceled(), 0);
        let shown = row(Some(y), "vy", VenueOrderState::Open, 0);
        let report = resync(r, &caps(), &snap(ms(20_000), vec![shown], 0));
        assert_eq!(report.untracked, vec![(y, vid("vy"))]);
        assert_eq!(r.get(y).unwrap().state(), OrdState::Open);
        assert_eq!(r.orphans(INST), vec![y]);
    }
    assert_eq!(numbers(reg, &[x, y]), numbers(twin, &[x, y]));
    // Once Y ends otherwise and a resync drops it from the orphans, a pass forgets X, and the
    // pass after it forgets Y.
    for r in [&mut *reg, &mut *twin] {
        end(r, y);
    }
    let t2 = floor(t1) + LONG;
    pass(reg, &keep, t2);
    let r = pass(reg, &keep, floor(t2));
    assert_eq!((r.pinned.shared_vid, r.forgotten.len()), (2, 0));
    for r in [&mut *reg, &mut *twin] {
        resync(r, &caps(), &snap(ms(30_000), vec![], 0));
        assert!(r.orphans(INST).is_empty());
    }
    assert_eq!(pass(reg, &keep, floor(t2)).forgotten, vec![x]);
    assert_eq!(pass(reg, &keep, floor(t2)).forgotten, vec![y]);
    // With both forgotten and no order holding that id, a late fill under X's client id
    // naming it counts as its held record's would be placed; the twin's conflicts.
    let f = fill_event(Some(x), Some("vy"), None, "vy-fill");
    let [la, lb] = &mut ledgers;
    assert_eq!(apply(reg, la, &f, ms(40_000)), FillRouted::OursUntracked(x));
    assert_eq!(
        apply(twin, lb, &f, ms(40_000)),
        FillRouted::Conflicting { cid: x, by_vid: y }
    );
    // An open update under a held order Z's client id, Z with no venue id, names that id: a
    // late Canceled update under X's client id naming it then ends Z (the residual of item
    // 11), where on the twin it reaches X and changes nothing. Z is inserted: the orphan's
    // cooldown suppresses a place on its side.
    let z = cid();
    for r in [&mut *reg, &mut *twin] {
        r.insert(placement(z, 100, 1)).unwrap();
        upd(r, Some(z), Some("vy"), VenueOrderState::Open, 0);
        upd(r, Some(x), Some("vy"), canceled(), 0);
    }
    assert!(reg.get(z).unwrap().state().is_terminal());
    assert_eq!(twin.get(z).unwrap().state(), OrdState::Open);
}

#[test]
fn residual_vid_guard() {
    let keep = cfg(1_000, Duration::from_millis(1));
    let t1 = ms(10_000);
    let mut regs = [quoting(), quoting()];
    let mut ledgers = [ledger(), ledger()];
    let (f, g, h, b) = (cid(), cid(), cid(), cid());
    for reg in &mut regs {
        open_as(reg, f, "vf");
        end(reg, f);
        open_as(reg, g, "vg");
        end(reg, g);
        open_as(reg, h, "vh");
        place_qty(reg, b, 1);
    }
    let [reg, twin] = &mut regs;
    pass(reg, &keep, t1);
    assert_eq!(pass(reg, &keep, floor(t1)).forgotten, vec![f, g]);
    let [la, lb] = &mut ledgers;
    // A fill naming a held order's client id with the forgotten order's venue id.
    let mixed = fill_event(Some(h), Some("vf"), None, "mixed");
    assert!(matches!(apply(reg, la, &mixed, ms(20_000)), FillRouted::Ours(got, _) if got == h));
    assert_eq!(
        apply(twin, lb, &mixed, ms(20_000)),
        FillRouted::Conflicting { cid: h, by_vid: f }
    );
    // B takes another forgotten order's venue id; a terminal update naming it alone ends B.
    for r in [&mut *reg, &mut *twin] {
        upd(r, Some(b), Some("vg"), VenueOrderState::Open, 0);
    }
    assert_eq!(reg.cid_of(&vid("vg")), Some(b));
    assert_eq!(twin.cid_of(&vid("vg")), Some(g));
    // A late fill under that forgotten order's client id naming that id conflicts; the twin
    // counts it.
    let late = fill_event(Some(g), Some("vg"), None, "late");
    let before = reg.inventory(INST);
    assert_eq!(
        apply(reg, la, &late, ms(20_001)),
        FillRouted::Conflicting { cid: g, by_vid: b }
    );
    assert_eq!(reg.inventory(INST), before);
    assert!(matches!(apply(twin, lb, &late, ms(20_001)), FillRouted::Ours(got, _) if got == g));
    for r in [&mut *reg, &mut *twin] {
        upd(r, None, Some("vg"), canceled(), 0);
    }
    assert!(reg.get(b).unwrap().state().is_terminal());
    assert_eq!(twin.get(b).unwrap().state(), OrdState::Open);
}

#[test]
fn forgotten_ids_come_into_view() {
    let keep = cfg(1_000, Duration::from_millis(1));
    let t1 = ms(10_000);
    let fresh = || {
        let mut regs = [quoting(), quoting()];
        let c = cid();
        for reg in &mut regs {
            open_as(reg, c, "vf");
            end(reg, c);
        }
        let [reg, _] = &mut regs;
        pass(reg, &keep, t1);
        assert_eq!(pass(reg, &keep, floor(t1)).forgotten, vec![c]);
        (regs, c)
    };
    // An open update naming only its venue id comes into view.
    let ([mut reg, mut twin], c) = fresh();
    assert_eq!(
        upd(&mut reg, None, Some("vf"), VenueOrderState::Open, 0),
        Routed::Untracked
    );
    assert!(reg.foreign_in_view(INST));
    assert_eq!(
        upd(&mut twin, None, Some("vf"), VenueOrderState::Open, 0),
        Routed::Ours(c, Applied::IgnoredLate)
    );
    assert!(!twin.foreign_in_view(INST));
    // A snapshot's open row with no client id naming it is one without our client id, in view.
    let ([mut reg, mut twin], _) = fresh();
    let nameless = row(None, "vf", VenueOrderState::Open, 0);
    let s = snap(ms(20_000), vec![nameless], 0);
    assert_eq!(
        resync(&mut reg, &caps(), &s).without_cid,
        vec![(INST, vid("vf"))]
    );
    assert!(reg.foreign_in_view(INST));
    assert_eq!(resync(&mut twin, &caps(), &s).without_cid, vec![]);
    assert!(!twin.foreign_in_view(INST));
    // A row under its client id showing it ended is untracked and registers no orphan.
    let ([mut reg, mut twin], c) = fresh();
    let s = snap(ms(20_000), vec![row(Some(c), "vf", canceled(), 0)], 0);
    assert_eq!(
        resync(&mut reg, &caps(), &s).untracked,
        vec![(c, vid("vf"))]
    );
    assert!(reg.orphans(INST).is_empty() && reg.get(c).is_none());
    assert_eq!(resync(&mut twin, &caps(), &s).untracked, vec![]);
}

/// Decision 0158 item 6: each venue id of a forgotten order not ended never sent is kept as
/// ended among the orders not ours for one more floor, stamped with the pass's `now`. While it
/// is kept, a late open event naming that id alone shows nothing resting, and only a trustworthy
/// row requested at or after the pass's `now` plus the lag bound shows it again; once a later
/// pass prunes it, the late event holds the market. A forgotten order that ended never sent
/// leaves no ended entry: a late event under a venue id a fill taught it holds the market.
#[test]
fn a_forgotten_orders_venue_ids_are_kept_as_ended() {
    let keep = cfg(1_000, Duration::from_millis(1));
    let t1 = ms(10_000);
    let one = Duration::from_nanos(1);
    let forgotten = || {
        let mut reg = quoting();
        let c = cid();
        open_as(&mut reg, c, "vf");
        end(&mut reg, c);
        pass(&mut reg, &keep, t1);
        let forget = floor(t1);
        assert_eq!(pass(&mut reg, &keep, forget).forgotten, vec![c]);
        (reg, forget)
    };
    let (mut reg, forget) = forgotten();
    upd(&mut reg, None, Some("vf"), VenueOrderState::Open, 0);
    assert!(!reg.not_ours_resting(INST));
    // A pass short of the floor past the forget keeps the ended entry.
    pass(&mut reg, &keep, forget + (LAG + RESYNC - one));
    upd(&mut reg, None, Some("vf"), VenueOrderState::Open, 0);
    assert!(!reg.not_ours_resting(INST));
    // One at the floor prunes it: the late event holds the market.
    pass(&mut reg, &keep, floor(forget));
    upd(&mut reg, None, Some("vf"), VenueOrderState::Open, 0);
    assert!(reg.not_ours_resting(INST));
    assert_eq!(
        admits(&mut reg, 1),
        Err(OmsError::NotOursResting {
            inst: INST,
            side: Side::Buy
        })
    );
    // A trustworthy row requested before the forget's `now` plus the lag bound holds nothing;
    // one requested at or after it holds the market.
    let (mut reg, forget) = forgotten();
    let shown = || vec![row(None, "vf", VenueOrderState::Open, 0)];
    resync(&mut reg, &caps(), &snap(forget + (LAG - one), shown(), 0));
    assert!(!reg.not_ours_resting(INST));
    resync(&mut reg, &caps(), &snap(forget + LAG, shown(), 0));
    assert!(reg.not_ours_resting(INST));
    // An order ended never sent, its authorization refused, then taught a venue id by a late
    // fill under its client id: forgotten, it leaves no ended entry.
    let mut l = ledger();
    let mut reg = quoting();
    reg.bind_account(TEST_ACCT).unwrap();
    let c = cid();
    let cmd = reg.place(placement(c, 100, 1)).unwrap();
    assert!(reg.authorize(AccountKey::new(99), cmd).is_err());
    assert_eq!(
        reg.get(c).unwrap().state(),
        OrdState::Terminal(TerminalKind::NotSent(NotSentReason::StaleAuthorization))
    );
    apply(
        &mut reg,
        &mut l,
        &fill_event(Some(c), Some("vn"), None, &fid()),
        ms(3),
    );
    assert_eq!(reg.cid_of(&vid("vn")), Some(c));
    pass(&mut reg, &keep, t1);
    assert_eq!(pass(&mut reg, &keep, floor(t1)).forgotten, vec![c]);
    upd(&mut reg, None, Some("vn"), VenueOrderState::Open, 0);
    assert!(reg.not_ours_resting(INST));
}

#[test]
fn untrustworthy_fill_sights_nothing() {
    let keep = cfg(1_000, Duration::from_millis(1));
    let t1 = ms(10_000);
    let venue = untrustworthy();
    let hand_seeded = || {
        let mut reg = arm::named(Registry::with_caps(pre_trade()))
            .for_testnet_run(TestnetRun::owner_assisted());
        reg.seed_position(INST, SignedLots(0)).unwrap();
        arm::start(&mut reg, INST);
        reg
    };
    let mut regs = [hand_seeded(), hand_seeded()];
    let mut ledgers = [ledger(), ledger()];
    let c = cid();
    for reg in &mut regs {
        open_as(reg, c, "vu");
        end(reg, c);
    }
    let [reg, twin] = &mut regs;
    pass_on(reg, &keep, &venue, t1);
    assert_eq!(pass_on(reg, &keep, &venue, floor(t1)).forgotten, vec![c]);
    let f = fill_event(Some(c), None, None, &fid());
    let [la, lb] = &mut ledgers;
    assert_eq!(apply(reg, la, &f, ms(20_000)), FillRouted::OursUntracked(c));
    apply(twin, lb, &f, ms(20_000));
    let shown = row(Some(c), "vu", VenueOrderState::Open, 1);
    for r in [&mut *reg, &mut *twin] {
        let report = resync(r, &venue, &snap(ms(30_000), vec![shown.clone()], 1));
        assert!(report.untrustworthy);
        assert!(r.orphans(INST).is_empty());
    }
    assert!(reg.get(c).is_none());
    // An open update under its client id is a live sighting: the next such snapshot registers
    // it.
    upd(reg, Some(c), Some("vu"), VenueOrderState::Open, 1);
    resync(reg, &venue, &snap(ms(40_000), vec![shown], 1));
    assert_eq!(reg.orphans(INST), vec![c]);
}

#[test]
fn forgotten_cid_refused() {
    let keep = cfg(1_000, Duration::from_millis(1));
    let t1 = ms(10_000);
    let mut reg = quoting();
    let f = ended(&mut reg, "r-1");
    // Minted and refused by a cap before registration.
    let r = cid();
    assert!(matches!(
        reg.place(placement(r, 100, CAP + 1)),
        Err(OmsError::Capped(_))
    ));
    let g = ended(&mut reg, "r-2");
    pass(&mut reg, &keep, t1);
    assert_eq!(pass(&mut reg, &keep, floor(t1)).forgotten, vec![f, g]);
    let dup = Err(OmsError::DuplicateCid(f));
    assert_eq!(reg.place(placement(f, 100, 1)).map(|_| ()), dup);
    let plan = reg.place_batch(vec![placement(f, 100, 1)]).unwrap();
    assert_eq!(plan.refused, vec![(f, OmsError::DuplicateCid(f))]);
    assert!(plan.command.is_none());
    assert_eq!(reg.insert(placement(f, 100, 1)).map(|_| ()), dup);
    assert_eq!(
        reg.place(placement(r, 100, 1)).map(|_| ()),
        Err(OmsError::DuplicateCid(r))
    );
    // Outside the span, a client id is admitted.
    assert_eq!(admits(&mut reg, 1), Ok(()));
    // A trustworthy snapshot showing the forgotten order open registers it as an orphan.
    let shown = row(Some(f), "r-1", VenueOrderState::Open, 0);
    let report = resync(&mut reg, &caps(), &snap(ms(20_000), vec![shown], 0));
    assert_eq!(report.untracked, vec![(f, vid("r-1"))]);
    assert_eq!(reg.orphans(INST), vec![f]);
}

#[test]
fn lagging_snapshot_after_forget() {
    let t_req = ms(100);
    let applied_at = t_req + RESYNC;
    // Within the bounds: a resync requested before the order ends, applied after passes run
    // within the resync bound, finds it held. A floor of the lag bound alone would not.
    let short =
        RetentionConfig::new(1_000, Duration::from_millis(1), Duration::from_nanos(1)).unwrap();
    let keep = cfg(1_000, Duration::from_millis(1));
    let mut regs = [quoting(), quoting(), quoting()];
    let c = cid();
    for reg in &mut regs {
        open_as(reg, c, "lag-1");
        end(reg, c);
    }
    let [reg, twin, lag_only] = &mut regs;
    for now in [ms(150), ms(250), applied_at] {
        assert!(pass(reg, &keep, now).forgotten.is_empty());
        pass(lag_only, &short, now);
    }
    assert!(lag_only.get(c).is_none());
    let shown = row(Some(c), "lag-1", VenueOrderState::Open, 0);
    let s = snap(t_req, vec![shown], 0);
    for r in [&mut *reg, &mut *twin, &mut *lag_only] {
        resync(r, &caps(), &s);
    }
    assert_eq!(numbers(reg, &[c]), numbers(twin, &[c]));
    assert!(reg.orphans(INST).is_empty());
    assert_eq!(lag_only.orphans(INST), vec![c]);

    // Past them: a resync applied past the resync bound registers the forgotten order as an
    // orphan, which owes the snapshot's fill, stays pinned and suppresses the adding side.
    let mut reg = quoting();
    let mut l = ledger();
    let c = cid();
    place_qty(&mut reg, c, 2);
    accept(&mut reg, c, "lag-2");
    apply(
        &mut reg,
        &mut l,
        &fill_event(Some(c), Some("lag-2"), Some(1), &fid()),
        ms(120),
    );
    end(&mut reg, c);
    pass(&mut reg, &keep, ms(150));
    assert_eq!(pass(&mut reg, &keep, floor(ms(150))).forgotten, vec![c]);
    let shown = row_of(Some(c), "lag-2", VenueOrderState::Open, 2, 1);
    resync(&mut reg, &caps(), &snap(t_req, vec![shown], 1));
    assert_eq!(reg.orphans(INST), vec![c]);
    assert!(reg.orphan_cooldown(INST).is_some());
    let rec = reg.get(c).unwrap();
    assert_eq!((rec.cum_venue().get(), rec.cum_fills().get()), (1, 0));
    upd(&mut reg, Some(c), None, canceled(), 1);
    let t2 = ms(10_000);
    pass(&mut reg, &keep, t2);
    let r = pass(&mut reg, &keep, floor(t2));
    assert_eq!((r.pinned.fills_owed, r.forgotten.len()), (1, 0));

    // On a one-read venue, an order a resync registered before its market's seed, which the
    // seed did not show, takes a late fill once forgotten: the lagging snapshot's registration
    // moves the inventory by it, and that resync's check reports a desync; the twin's stays.
    let mut regs = [
        arm::named(Registry::with_caps(pre_trade())),
        arm::named(Registry::with_caps(pre_trade())),
    ];
    let mut ledgers = [ledger(), ledger()];
    let c = cid();
    for reg in &mut regs {
        let rows = vec![
            row(Some(c), "e-1", VenueOrderState::Open, 0),
            row(None, "nameless", VenueOrderState::Open, 0),
        ];
        let report = resync(reg, &caps(), &snap(ms(10), rows, 0));
        assert_eq!(
            (report.registered.clone(), report.seeded.len()),
            (vec![c], 0)
        );
        let report = resync(reg, &caps(), &snap(ms(20), vec![], 1));
        assert_eq!(report.seeded, vec![(INST, SignedLots(1))]);
        end(reg, c);
    }
    let [reg, twin] = &mut regs;
    pass(reg, &keep, ms(100));
    assert_eq!(pass(reg, &keep, floor(ms(100))).forgotten, vec![c]);
    let f = fill_event(Some(c), None, None, &fid());
    let [la, lb] = &mut ledgers;
    assert_eq!(apply(reg, la, &f, ms(1_000)), FillRouted::InSnapshot(c));
    assert_eq!(apply(twin, lb, &f, ms(1_000)), FillRouted::InSnapshot(c));
    let s = snap(
        ms(2_000),
        vec![row(Some(c), "e-1", VenueOrderState::Open, 0)],
        1,
    );
    let report = resync(reg, &caps(), &s);
    assert!(matches!(report.checks[..], [PositionCheck::Desync { .. }]));
    assert_eq!(reg.inventory(INST), SignedLots(2));
    let report = resync(twin, &caps(), &s);
    assert!(matches!(report.checks[..], [PositionCheck::Agrees { .. }]));
    assert_eq!(twin.inventory(INST), SignedLots(1));
}

#[test]
fn never_asked_changes_nothing() {
    let mut reg = quoting();
    let mut l = ledger();
    let cids: Vec<ClientOrderId> = (0..20)
        .map(|n| ended(&mut reg, &format!("na-{n}")))
        .collect();
    assert_eq!(reg.len(), 20);
    for c in &cids {
        assert!(reg.get(*c).unwrap().state().is_terminal());
    }
    let stranger = cid();
    assert_eq!(
        reg.on_outcome(
            stranger,
            OrderOp::Place,
            &item(stranger, None),
            &accepted(),
            ms(9)
        ),
        Err(OmsError::UnknownCid(stranger))
    );
    assert_eq!(
        reg.cancel_sent(stranger, RpcId(1), ms(9)),
        Err(OmsError::UnknownCid(stranger))
    );
    let f = fill_event(Some(stranger), None, None, &fid());
    assert_eq!(
        apply(&mut reg, &mut l, &f, ms(10)),
        FillRouted::InSnapshot(stranger)
    );
    assert_eq!(reg.inventory(INST), SignedLots(0));
    assert_eq!(reg.len(), 20);
}

// ---- the planner's helpers ----

fn mint() -> CidMint {
    static NEXT: AtomicU16 = AtomicU16::new(900);
    let ns = Namespace::new(NEXT.fetch_add(1, Ordering::Relaxed));
    let lease = NamespaceLease::acquire(&lease_dir(), AccountKey::new(2), ns).unwrap();
    CidMint::new(lease, 0, 0, WallNs(0))
}

fn planner() -> ExecutionPlanner {
    ExecutionPlanner::new(
        PlannerConfig::new(Bps(2.0), lots(2), Duration::from_millis(100)).unwrap(),
    )
}

// ---- the inventory cap and every number beside it: a twin that never runs the pass ----

/// One step the twin check takes on both registries (decision 0157's falsifier): `idx`
/// picks an order placed so far, modulo their number.
#[derive(Clone, Debug)]
enum Step {
    /// A fresh client id from one rising mint, never retried after a refusal.
    Place {
        buy: bool,
        qty: u8,
        hold: bool,
    },
    /// A deliberate retry of a client id this registry built, held or forgotten.
    Retry {
        idx: usize,
    },
    Accept {
        idx: usize,
    },
    Unknown {
        idx: usize,
    },
    Open {
        idx: usize,
    },
    /// Ends it Canceled, the venue's cumulative fill `extra` past the fills it sent.
    End {
        idx: usize,
        extra: bool,
    },
    Fill {
        idx: usize,
        by_vid: bool,
        cum: bool,
    },
    /// An update under one order's client id, with no venue id of its own yet, naming
    /// another order's venue id.
    Cross {
        idx: usize,
        other: usize,
    },
    Cancel {
        idx: usize,
        hold: bool,
    },
    Amend {
        idx: usize,
    },
    Answer {
        k: usize,
        how: u8,
    },
    DropHeld {
        k: usize,
    },
    Ladder,
    /// Requests a snapshot of the open orders now.
    Capture,
    /// Applies the snapshot captured, if it is within the resync bound of its request.
    Apply,
    Pass,
    Tick {
        ms: u16,
    },
}

fn step() -> impl Strategy<Value = Step> {
    let idx = 0usize..64;
    prop_oneof![
        4 => (any::<bool>(), 1u8..=3, any::<bool>())
            .prop_map(|(buy, qty, hold)| Step::Place { buy, qty, hold }),
        1 => idx.clone().prop_map(|idx| Step::Retry { idx }),
        3 => idx.clone().prop_map(|idx| Step::Accept { idx }),
        1 => idx.clone().prop_map(|idx| Step::Unknown { idx }),
        1 => idx.clone().prop_map(|idx| Step::Open { idx }),
        5 => (idx.clone(), any::<bool>()).prop_map(|(idx, extra)| Step::End { idx, extra }),
        3 => (idx.clone(), any::<bool>(), any::<bool>())
            .prop_map(|(idx, by_vid, cum)| Step::Fill { idx, by_vid, cum }),
        1 => (idx.clone(), idx.clone()).prop_map(|(idx, other)| Step::Cross { idx, other }),
        2 => (idx.clone(), any::<bool>()).prop_map(|(idx, hold)| Step::Cancel { idx, hold }),
        1 => idx.clone().prop_map(|idx| Step::Amend { idx }),
        2 => (0usize..16, 0u8..3).prop_map(|(k, how)| Step::Answer { k, how }),
        1 => (0usize..16).prop_map(|k| Step::DropHeld { k }),
        1 => Just(Step::Ladder),
        1 => Just(Step::Capture),
        1 => Just(Step::Apply),
        3 => Just(Step::Pass),
        3 => (1u16..=400).prop_map(|ms| Step::Tick { ms }),
    ]
}

/// An order the twin check placed, which this registry built: its client id, side and total,
/// and the fills sent of it.
struct Placed {
    cid: ClientOrderId,
    side: Side,
    qty: i64,
    fills: i64,
}

/// Runs `steps` on a registry that runs the pass and a twin that never does, comparing after
/// each every number a cap, Exit's admission, the orphan suppression or the planner reads;
/// how many of the orders its steps placed the passes forgot.
fn twin_check(steps: &[Step]) -> Result<usize, TestCaseError> {
    let venue = caps();
    let bound = RetentionConfig::new(3, Duration::from_millis(600), RESYNC).unwrap();
    let (mut a, mut b) = (quoting(), quoting());
    a.bind_account(TEST_ACCT).unwrap();
    b.bind_account(TEST_ACCT).unwrap();
    let (mut la, mut lb) = (ledger(), ledger());
    let mut orders: Vec<Placed> = Vec::new();
    let mut held: Vec<Box<dyn std::any::Any>> = Vec::new();
    let mut outstanding: Vec<(ClientOrderId, RpcId)> = Vec::new();
    let mut captured: Option<ResyncSnapshot> = None;
    let mut now = ms(10);
    let mut rpc = 0u64;
    for (n, s) in steps.iter().enumerate() {
        let pick = |idx: usize| (!orders.is_empty()).then(|| idx % orders.len());
        // An event naming a forgotten order, by client id or venue id, is never fed.
        let gone = |c: ClientOrderId| a.get(c).is_none();
        match *s {
            Step::Place { buy, qty, hold } => {
                let c = cid();
                let side = if buy { Side::Buy } else { Side::Sell };
                let mut order = placement(c, 100, i64::from(qty));
                order.side = side;
                let (pa, pb) = (a.place(order.clone()), b.place(order));
                prop_assert_eq!(pa.is_ok(), pb.is_ok(), "place {:?}", s);
                if let (Ok(ca), Ok(cb)) = (pa, pb) {
                    let (aa, ab) = (
                        a.authorize(TEST_ACCT, ca).unwrap(),
                        b.authorize(TEST_ACCT, cb).unwrap(),
                    );
                    if hold {
                        held.push(Box::new((aa, ab)));
                    } else {
                        drop((aa.hand_over(), ab.hand_over()));
                    }
                    orders.push(Placed {
                        cid: c,
                        side,
                        qty: i64::from(qty),
                        fills: 0,
                    });
                }
            }
            Step::Retry { idx } => {
                let Some(i) = pick(idx) else { continue };
                let o = &orders[i];
                let mut order = placement(o.cid, 100, o.qty);
                order.side = o.side;
                let dup = Err(OmsError::DuplicateCid(o.cid));
                prop_assert_eq!(a.place(order.clone()).map(|_| ()), dup.clone());
                prop_assert_eq!(b.place(order).map(|_| ()), dup);
            }
            Step::Accept { idx } | Step::Unknown { idx } => {
                let Some(i) = pick(idx) else { continue };
                let c = orders[i].cid;
                if gone(c) {
                    continue;
                }
                let v = format!("p-{}", c.seq());
                let outcome = match s {
                    Step::Accept { .. } => accepted(),
                    _ => SubmitOutcome::Unknown,
                };
                let it = item(c, Some(&v));
                let ra = a.on_outcome(c, OrderOp::Place, &it, &outcome, now);
                let rb = b.on_outcome(c, OrderOp::Place, &it, &outcome, now);
                prop_assert_eq!(ra, rb);
            }
            Step::Open { idx } | Step::End { idx, .. } => {
                let Some(i) = pick(idx) else { continue };
                let o = &orders[i];
                if gone(o.cid) {
                    continue;
                }
                let (state, extra) = match s {
                    Step::End { extra, .. } => (canceled(), i64::from(*extra)),
                    _ => (VenueOrderState::Open, 0),
                };
                let cum = (o.fills + extra).min(o.qty);
                let v = b.get(o.cid).and_then(|r| r.vid().cloned());
                let u = OrderUpdate {
                    cid: Some(CidMatch::Ours(o.cid)),
                    vid: v,
                    inst: INST,
                    side: o.side,
                    state,
                    cum_filled: lots(cum),
                    px: None,
                    qty: None,
                    post_only: None,
                    reduce_only: None,
                };
                let k = key();
                prop_assert_eq!(a.apply_update(&u, k), b.apply_update(&u, k));
            }
            Step::Fill { idx, by_vid, cum } => {
                let Some(i) = pick(idx) else { continue };
                let o = &mut orders[i];
                if gone(o.cid) || o.fills >= o.qty {
                    continue;
                }
                let v = b.get(o.cid).and_then(|r| r.vid().cloned());
                if by_vid && v.is_none() {
                    continue;
                }
                if v.as_ref().and_then(|v| a.cid_of(v)).is_some_and(gone) {
                    continue;
                }
                o.fills += 1;
                let f = fbc_core::FillEvent {
                    side: o.side,
                    ..fill(
                        (!by_vid).then_some(o.cid),
                        FillIdent::Venue {
                            fill: fill_id(&fid()),
                            vid: v,
                            cum_after: cum.then(|| lots(o.fills)),
                        },
                        o.side,
                        1,
                        false,
                    )
                };
                let ra = apply(&mut a, &mut la, &f, now);
                let rb = apply(&mut b, &mut lb, &f, now);
                prop_assert_eq!(
                    std::mem::discriminant(&ra) == std::mem::discriminant(&rb)
                        || matches!(
                            (&ra, &rb),
                            (FillRouted::OursUntracked(_), FillRouted::Ours(..))
                        ),
                    true,
                    "{:?} against {:?}",
                    ra,
                    rb
                );
            }
            Step::Cross { idx, other } => {
                let (Some(i), Some(j)) = (pick(idx), pick(other)) else {
                    continue;
                };
                let (x, y) = (orders[i].cid, orders[j].cid);
                if x == y || gone(x) || gone(y) {
                    continue;
                }
                let (Some(rx), Some(vy)) = (b.get(x), b.get(y).and_then(|r| r.vid().cloned()))
                else {
                    continue;
                };
                if rx.vid().is_some() || rx.state().is_terminal() {
                    continue;
                }
                let u = OrderUpdate {
                    cid: Some(CidMatch::Ours(x)),
                    vid: Some(vy),
                    inst: INST,
                    side: orders[i].side,
                    state: VenueOrderState::Open,
                    cum_filled: lots(orders[i].fills),
                    px: None,
                    qty: None,
                    post_only: None,
                    reduce_only: None,
                };
                let k = key();
                prop_assert_eq!(a.apply_update(&u, k), b.apply_update(&u, k));
            }
            Step::Cancel { idx, hold } => {
                let Some(i) = pick(idx) else { continue };
                let c = orders[i].cid;
                if gone(c) {
                    continue;
                }
                let built = |r: &mut Registry| {
                    r.cancellable(c)
                        .map(|p| match p.cancel(&venue) {
                            CancelChoice::Send(cmd) => Some(cmd),
                            CancelChoice::AwaitAck => None,
                        })
                        .map_err(|e| format!("{e:?}"))
                };
                let (ca, cb) = (built(&mut a), built(&mut b));
                prop_assert_eq!(ca.is_ok(), cb.is_ok());
                if let (Ok(Some(ca)), Ok(Some(cb))) = (ca, cb) {
                    let (aa, ab) = (
                        a.authorize(TEST_ACCT, ca).unwrap(),
                        b.authorize(TEST_ACCT, cb).unwrap(),
                    );
                    if hold {
                        held.push(Box::new((aa, ab)));
                    } else {
                        drop((aa.hand_over(), ab.hand_over()));
                        rpc += 1;
                        prop_assert_eq!(
                            a.cancel_sent(c, RpcId(rpc), now),
                            b.cancel_sent(c, RpcId(rpc), now)
                        );
                        outstanding.push((c, RpcId(rpc)));
                    }
                }
            }
            Step::Amend { idx } => {
                let Some(i) = pick(idx) else { continue };
                let o = &orders[i];
                if gone(o.cid) {
                    continue;
                }
                let qty = lots(o.qty + 1);
                let built = |r: &mut Registry| {
                    r.live(o.cid).map_err(|e| format!("{e:?}")).and_then(|p| {
                        p.amend(&venue, Ticks(100), qty, false)
                            .map_err(|e| format!("{e:?}"))
                    })
                };
                let (ca, cb) = (built(&mut a), built(&mut b));
                prop_assert_eq!(ca.is_ok(), cb.is_ok(), "{:?} against {:?}", ca, cb);
                if let (Ok(ca), Ok(cb)) = (ca, cb) {
                    prop_assert!(a.amend_not_submitted(ca) && b.amend_not_submitted(cb));
                }
            }
            Step::Answer { k, how } => {
                if outstanding.is_empty() {
                    continue;
                }
                let (c, r) = outstanding.remove(k % outstanding.len());
                if gone(c) {
                    continue;
                }
                let outcome = match how {
                    0 => accepted(),
                    1 => provisional(),
                    _ => SubmitOutcome::Unknown,
                };
                let it = item(c, None);
                prop_assert_eq!(
                    a.on_outcome(c, OrderOp::Cancel(r), &it, &outcome, now),
                    b.on_outcome(c, OrderOp::Cancel(r), &it, &outcome, now)
                );
            }
            Step::DropHeld { k } => {
                if !held.is_empty() {
                    drop(held.remove(k % held.len()));
                }
            }
            Step::Ladder => {
                let pa = a.ladder(&ladder_cfg(), &venue, now);
                let pb = b.ladder(&ladder_cfg(), &venue, now);
                prop_assert_eq!(
                    (pa.queries.len(), pa.tombstones.len(), pa.resync),
                    (pb.queries.len(), pb.tombstones.len(), pb.resync)
                );
            }
            Step::Capture => {
                // The venue's view now: every order the twin holds open under a venue id.
                let rows = orders
                    .iter()
                    .filter_map(|o| {
                        let rec = b.get(o.cid)?;
                        let v = rec.vid()?;
                        (!rec.state().is_terminal()).then(|| VenueOrderSnapshot {
                            side: o.side,
                            ..row_of(
                                Some(o.cid),
                                v.as_str(),
                                VenueOrderState::Open,
                                o.qty,
                                o.fills,
                            )
                        })
                    })
                    .collect();
                captured = Some(snap(now, rows, b.inventory(INST).0));
            }
            Step::Apply => {
                let Some(s) = captured.take() else { continue };
                if now > s.requested_at + RESYNC {
                    continue;
                }
                let ra = a.resync(
                    &ladder_cfg(),
                    &venue,
                    &s,
                    OrderKey {
                        venue: None,
                        ingest: n as u64,
                    },
                );
                let rb = b.resync(
                    &ladder_cfg(),
                    &venue,
                    &s,
                    OrderKey {
                        venue: None,
                        ingest: n as u64,
                    },
                );
                prop_assert_eq!(ra, rb);
            }
            Step::Pass => {
                a.retain_ended(&bound, &venue, now);
            }
            Step::Tick { ms } => now = now + Duration::from_millis(u64::from(ms)),
        }
        // The twin makes a mutating call that only reaps, so both applied the same releases.
        b.bind_account(TEST_ACCT).unwrap();
        a.bind_account(TEST_ACCT).unwrap();
        let cids: Vec<ClientOrderId> = orders.iter().map(|o| o.cid).collect();
        prop_assert_eq!(numbers(&a, &cids), numbers(&b, &cids), "after {:?}", s);
        // A place on each side sized to the inventory cap's room, judged by both and withdrawn.
        for side in [Side::Buy, Side::Sell] {
            let pos = b.inventory(INST).0;
            let room = match side {
                Side::Buy => CAP - pos,
                Side::Sell => CAP + pos,
            };
            let mut order = placement(cid(), 100, room.clamp(1, CAP));
            order.side = side;
            prop_assert_eq!(
                admits_order(&mut a, order.clone()),
                admits_order(&mut b, order),
                "a probe on {:?} after {:?}",
                side,
                s
            );
        }
    }
    Ok(orders.iter().filter(|o| a.get(o.cid).is_none()).count())
}

use proptest::prelude::*;
use proptest::test_runner::{RngSeed, TestCaseError};

/// Fixed cases and seed, so the check and its coverage are the same on every run and machine
/// (decision 0037).
fn config(cases: u32, seed: u64) -> ProptestConfig {
    ProptestConfig {
        cases,
        rng_seed: RngSeed::Fixed(seed),
        failure_persistence: None,
        ..ProptestConfig::default()
    }
}

proptest! {
    #![proptest_config(config(256, 0x0157_0001))]

    /// Decision 0157's falsifier: no number a cap, Exit's admission, the orphan suppression or
    /// the planner reads differs between a registry that runs the pass and a twin that does
    /// not, both fed the same events, none naming a forgotten order but snapshots lagging
    /// within the venue's bound from their own request.
    #[test]
    fn forgetting_changes_no_cap_number(steps in prop::collection::vec(step(), 1..120)) {
        twin_check(&steps)?;
    }
}

/// The proptest's body on one fixed case, which forgets: a run forgetting nothing cannot meet
/// the property.
#[test]
fn forgetting_changes_no_cap_number_fixed_case_forgets() {
    let mut steps = Vec::new();
    for n in 0..6 {
        steps.push(Step::Place {
            buy: n % 2 == 0,
            qty: 2,
            hold: false,
        });
        steps.push(Step::Accept { idx: n });
        steps.push(Step::Fill {
            idx: n,
            by_vid: n % 3 == 0,
            cum: true,
        });
    }
    steps.extend([
        Step::Capture,
        Step::Cross { idx: 1, other: 0 },
        Step::End {
            idx: 0,
            extra: true,
        },
        Step::End {
            idx: 1,
            extra: false,
        },
        Step::End {
            idx: 2,
            extra: false,
        },
        Step::Cancel {
            idx: 3,
            hold: false,
        },
        Step::Cancel { idx: 4, hold: true },
        Step::End {
            idx: 3,
            extra: false,
        },
        Step::End {
            idx: 4,
            extra: false,
        },
        Step::Tick { ms: 100 },
        Step::Pass,
        Step::Apply,
        Step::Tick { ms: 400 },
        Step::Pass,
        Step::Tick { ms: 400 },
        Step::Pass,
        Step::Fill {
            idx: 0,
            by_vid: false,
            cum: true,
        },
        Step::Answer { k: 0, how: 0 },
        Step::DropHeld { k: 0 },
        Step::Tick { ms: 400 },
        Step::Pass,
        Step::Tick { ms: 400 },
        Step::Pass,
        Step::Retry { idx: 2 },
        Step::Ladder,
        Step::Capture,
        Step::Apply,
    ]);
    let forgotten = twin_check(&steps).unwrap();
    assert!(forgotten > 0, "the fixed case forgot none of its orders");
}
