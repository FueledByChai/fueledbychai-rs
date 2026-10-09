//! The Unknown ladder (decision 0005's I5 and I9; design §4.9): an order driven to Unknown by a
//! timeout, a NotFound or a per-item Unknown counts as fully resting; the ladder queries it by
//! a reference the venue's queries declare, resolves it by the answer (an open order still
//! counting against the caps), ends it Lost after the configured number of trustworthy
//! snapshots past its sent time and the settle time, and tombstone-cancels it by client id
//! after the configured maximum; nothing places or amends it again, and a command in flight
//! past the intent timeout escalates within that timeout plus one query (I9).

mod common;

use std::time::Duration;

use common::{canceled, cid, lots, order_caps, placement, update, vid};
use fbc_core::{
    AckLevel, AmendAck, AmendCaps, AmendQty, CancelReason, ChosenRef, CidMatch, ClientOrderId,
    ItemRef, MonoNs, Namespace, OrderCaps, OrderRef, QueryAnswer, Reject, RejectKind, RpcId, Side,
    SnapshotSource, SubmitOutcome, TagSet, TerminalHint, Ticks, VenueCommand, VenueOrderId,
    VenueOrderSnapshot, VenueOrderState, WallNs,
};
use fbc_core::{InstrumentId, RefKind};
use fbc_oms::{
    ControlCommand, Intent, LadderConfig, LadderConfigError, LadderPlan, LadderResolution,
    LadderStep, OmsError, OrdState, OrderKey, OrderOp, OutcomeApplied, PermitRefusal, Registry,
    ResyncApplied, TerminalKind,
};

const INST: InstrumentId = InstrumentId::new(1);

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}

/// `n` milliseconds on the shard's monotonic clock.
fn at(n: u64) -> MonoNs {
    MonoNs(n * 1_000_000)
}

/// `n` milliseconds on the wall clock.
fn wall(n: i64) -> WallNs {
    WallNs(n * 1_000_000)
}

/// The consumer's numbers: commands escalate after 100 ms, a snapshot counts an absence from
/// 50 ms after the order was sent, a tombstone after a second on the ladder, lost after two.
fn cfg() -> LadderConfig {
    LadderConfig::new(ms(100), ms(50), ms(1_000), 2).unwrap()
}

/// A venue's amend by venue id, keeping the order's venue id or giving it a new one.
fn amend_caps(keeps_venue_id: bool) -> AmendCaps {
    AmendCaps {
        refs: TagSet::of(&[RefKind::Venue]),
        price: true,
        qty: true,
        flags: false,
        when_partially_filled: true,
        reject_keeps_original: true,
        keeps_venue_id,
        ack: AmendAck::ReplacedEvent,
        qty_semantics: AmendQty::TotalIncludingFilled,
        keeps_priority: None,
    }
}

/// A venue that queries and cancels by client id or venue id, with a trustworthy snapshot.
fn caps() -> OrderCaps {
    OrderCaps {
        query_refs: TagSet::of(&[RefKind::Client]),
        cancel_refs: TagSet::of(&[RefKind::Venue, RefKind::Client]),
        snapshot_source: SnapshotSource::Trustworthy,
        ..order_caps()
    }
}

fn key(ingest: u64) -> OrderKey {
    OrderKey {
        venue: None,
        ingest,
    }
}

fn item(cid: Option<ClientOrderId>, vid: Option<VenueOrderId>) -> ItemRef {
    ItemRef { idx: 0, cid, vid }
}

fn accepted() -> SubmitOutcome {
    SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    }
}

fn refused(kind: RejectKind) -> SubmitOutcome {
    SubmitOutcome::Rejected(Reject {
        kind,
        venue_code: None,
        raw: "refused".into(),
    })
}

/// A registered buy of 10 at 100, sent at 0 ms (wall 1000 ms), its placement unanswered at
/// 5 ms: Unknown, on the ladder.
fn unknown(reg: &mut Registry) -> ClientOrderId {
    let c = reg.insert(placement(cid(), 100, 10)).unwrap().cid();
    reg.placement_sent(c, at(0), wall(1_000)).unwrap();
    reg.on_outcome(
        c,
        OrderOp::Place,
        &item(None, None),
        &SubmitOutcome::Unknown,
        at(5),
    )
    .unwrap();
    c
}

/// A registered buy of 10 at 100, sent at 0 ms (wall 1000 ms) and accepted as `v`: Open.
fn open(reg: &mut Registry, v: &str) -> ClientOrderId {
    let c = reg.insert(placement(cid(), 100, 10)).unwrap().cid();
    reg.placement_sent(c, at(0), wall(1_000)).unwrap();
    reg.on_outcome(
        c,
        OrderOp::Place,
        &item(None, Some(vid(v))),
        &accepted(),
        at(1),
    )
    .unwrap();
    c
}

/// The open order `v` with a cancel sent at 10 ms that went unanswered at 15 ms: on the ladder.
fn cancel_unanswered(reg: &mut Registry, v: &str) -> ClientOrderId {
    let c = open(reg, v);
    reg.cancel_sent(c, RpcId(50), at(10)).unwrap();
    reg.on_outcome(
        c,
        OrderOp::Cancel(RpcId(50)),
        &item(Some(c), None),
        &SubmitOutcome::Unknown,
        at(15),
    )
    .unwrap();
    c
}

/// The venue's view of our order `c` as `v`, in `state`, 10 at 100 with `cum` filled.
fn snap(c: Option<ClientOrderId>, v: &str, state: VenueOrderState, cum: i64) -> VenueOrderSnapshot {
    VenueOrderSnapshot {
        cid: c.map(CidMatch::Ours),
        vid: vid(v),
        inst: INST,
        side: Side::Buy,
        state,
        px: Some(Ticks(100)),
        qty: lots(10),
        cum_filled: lots(cum),
        post_only: None,
        reduce_only: None,
    }
}

/// The query the plan built for `c`.
fn query_of(plan: &LadderPlan, c: ClientOrderId) -> &fbc_core::QueryOrder {
    let found = plan.queries.iter().find(|(q, _)| *q == c);
    match found {
        Some((_, ControlCommand::Query(query))) => query,
        other => panic!("no query for {c:?}: {other:?}"),
    }
}

/// Every command a plan holds builds no placement and no amend.
fn places_or_amends_nothing(plan: &LadderPlan) {
    for (_, query) in &plan.queries {
        assert!(matches!(query, ControlCommand::Query(_)), "{query:?}");
    }
    for (_, cmd) in &plan.tombstones {
        assert!(
            matches!(cmd.command(), VenueCommand::Cancel(_)),
            "{:?}",
            cmd.command()
        );
    }
}

// ---- into Unknown, counted fully resting ----

#[test]
fn an_order_driven_to_unknown_by_a_timeout_a_not_found_or_a_per_item_unknown_counts_as_fully_resting()
 {
    let mut reg = Registry::new();
    // The request's deadline passed: the runtime's Unknown for every item, naming none.
    let timed_out = unknown(&mut reg);
    // The venue does not know the placement.
    let not_found = reg.insert(placement(cid(), 100, 10)).unwrap().cid();
    reg.on_outcome(
        not_found,
        OrderOp::Place,
        &item(None, None),
        &refused(RejectKind::NotFound),
        at(5),
    )
    .unwrap();
    // One item of a batch reply is Unknown.
    let per_item = reg.insert(placement(cid(), 100, 10)).unwrap().cid();
    reg.on_outcome(
        per_item,
        OrderOp::Place,
        &item(Some(per_item), None),
        &SubmitOutcome::Unknown,
        at(5),
    )
    .unwrap();
    // A placement sent at 0 ms with no outcome at all: the ladder's pass times it out at the
    // intent timeout.
    let silent = reg.insert(placement(cid(), 100, 10)).unwrap().cid();
    reg.placement_sent(silent, at(0), wall(1_000)).unwrap();

    let plan = reg.ladder(&cfg(), &caps(), at(99));
    assert!(plan.escalated.is_empty());
    assert_eq!(reg.get(silent).unwrap().state(), OrdState::PendingNew);
    let plan = reg.ladder(&cfg(), &caps(), at(100));
    assert_eq!(plan.escalated, vec![silent]);

    for c in [timed_out, not_found, per_item, silent] {
        let rec = reg.get(c).unwrap();
        assert_eq!(rec.state(), OrdState::Unknown, "{c:?}");
        assert_eq!(rec.resting(), lots(10), "Unknown counts as fully resting");
        assert!(rec.ladder_step().is_some(), "on the ladder");
    }
    assert_eq!(reg.get(silent).unwrap().unknown_since(), Some(at(100)));
    // A placement whose sent time was never recorded is timed out by its outcome alone.
    let unrecorded = reg.insert(placement(cid(), 100, 10)).unwrap().cid();
    let plan = reg.ladder(&cfg(), &caps(), at(10_000));
    assert!(!plan.escalated.contains(&unrecorded));
    assert_eq!(reg.get(unrecorded).unwrap().state(), OrdState::PendingNew);
}

// ---- the query ----

#[test]
fn an_order_on_the_ladder_is_queried_once_by_a_reference_the_venues_queries_declare() {
    // By client id: an Unknown order has no venue id.
    let mut reg = Registry::new();
    let c = unknown(&mut reg);
    let plan = reg.ladder(&cfg(), &caps(), at(6));
    let query = query_of(&plan, c);
    assert_eq!(query.target, OrderRef::Client(c));
    assert_eq!(query.inst, INST);
    assert_eq!(
        query.reference(caps().query_refs),
        Some(ChosenRef::Client(c))
    );
    assert_eq!(
        reg.get(c).unwrap().ladder_step(),
        Some(LadderStep::Querying)
    );
    assert!(!plan.resync);
    // Once: the next pass waits for the answer.
    let plan = reg.ladder(&cfg(), &caps(), at(7));
    assert!(plan.queries.is_empty());

    // By venue id, for an order the venue named.
    let by_vid = OrderCaps {
        query_refs: TagSet::of(&[RefKind::Venue]),
        ..caps()
    };
    let mut reg = Registry::new();
    let c = cancel_unanswered(&mut reg, "q1");
    let plan = reg.ladder(&cfg(), &by_vid, at(16));
    let query = query_of(&plan, c);
    assert_eq!(query.target, OrderRef::Both(c, vid("q1")));
    assert_eq!(
        query.reference(by_vid.query_refs),
        Some(ChosenRef::Venue(&vid("q1")))
    );

    // By placement nonce, for a venue that queries by it.
    let by_nonce = OrderCaps {
        query_refs: TagSet::of(&[RefKind::PlacementNonce]),
        ..caps()
    };
    let mut reg = Registry::new();
    let c = unknown(&mut reg);
    reg.placement_nonce_used(c, 77).unwrap();
    let plan = reg.ladder(&cfg(), &by_nonce, at(6));
    assert_eq!(
        query_of(&plan, c).reference(by_nonce.query_refs),
        Some(ChosenRef::PlacementNonce(77))
    );

    // A venue whose queries name only a reference the order lacks: no query, resyncs decide.
    let mut reg = Registry::new();
    let c = unknown(&mut reg);
    let plan = reg.ladder(&cfg(), &by_vid, at(6));
    assert!(plan.queries.is_empty());
    assert!(plan.resync);
    assert_eq!(reg.get(c).unwrap().ladder_step(), Some(LadderStep::Resync));
    // A resync is asked for on every pass while resyncs decide.
    assert!(reg.ladder(&cfg(), &by_vid, at(7)).resync);
}

#[test]
fn a_query_answer_resolves_the_order_and_one_finding_it_open_still_counts_against_the_caps() {
    let mut reg = Registry::new();
    let c = unknown(&mut reg);
    let plan = reg.ladder(&cfg(), &caps(), at(6));
    let target = query_of(&plan, c).target.clone();
    reg.query_sent(c, RpcId(1)).unwrap();
    // The venue holds it, 3 of 10 filled: it rests, and its 7 count against the caps.
    let shown = snap(Some(c), "r1", VenueOrderState::Open, 3);
    let answer = QueryAnswer::new(RpcId(1), target, Some(shown)).unwrap();
    assert_eq!(
        reg.on_query_answer(&caps(), &answer, key(1)),
        LadderResolution::Resolved(OrdState::PartiallyFilled)
    );
    let rec = reg.get(c).unwrap();
    assert_eq!(rec.resting(), lots(7));
    assert_eq!(rec.vid(), Some(&vid("r1")));
    assert_eq!(rec.ladder_step(), None);
    assert_eq!(rec.unknown_since(), None);
    assert_eq!(reg.cid_of(&vid("r1")), Some(c));
    // Off the ladder, it can be amended again.
    assert!(reg.live(c).is_ok());
    // A second answer to the same query changes nothing.
    assert_eq!(
        reg.on_query_answer(&caps(), &answer, key(2)),
        LadderResolution::Ignored
    );

    // The venue shows it ended: it leaves the ladder terminal, resting nothing.
    let c = unknown(&mut reg);
    reg.ladder(&cfg(), &caps(), at(6));
    reg.query_sent(c, RpcId(2)).unwrap();
    let answer = QueryAnswer::new(
        RpcId(2),
        OrderRef::Client(c),
        Some(snap(Some(c), "r2", canceled(), 0)),
    )
    .unwrap();
    assert_eq!(
        reg.on_query_answer(&caps(), &answer, key(3)),
        LadderResolution::Resolved(OrdState::Terminal(TerminalKind::Canceled(
            CancelReason::Requested
        )))
    );
    assert_eq!(reg.get(c).unwrap().resting(), lots(0));

    // The venue does not find it: inconclusive, still fully resting, resyncs decide.
    let c = unknown(&mut reg);
    reg.ladder(&cfg(), &caps(), at(6));
    reg.query_sent(c, RpcId(3)).unwrap();
    let answer = QueryAnswer::new(RpcId(3), OrderRef::Client(c), None).unwrap();
    assert_eq!(
        reg.on_query_answer(&caps(), &answer, key(4)),
        LadderResolution::Inconclusive
    );
    let rec = reg.get(c).unwrap();
    assert_eq!(rec.state(), OrdState::Unknown, "NotFound is never terminal");
    assert_eq!(rec.resting(), lots(10));
    assert_eq!(rec.ladder_step(), Some(LadderStep::Resync));
    assert!(reg.ladder(&cfg(), &caps(), at(7)).resync);
}

#[test]
fn a_query_unanswered_unsent_or_refused_is_inconclusive_and_an_accepted_one_waits_for_its_answer() {
    for outcome in [
        SubmitOutcome::Unknown,
        SubmitOutcome::NotSent(fbc_core::NotSentReason::Disconnected),
        refused(RejectKind::Other),
    ] {
        let mut reg = Registry::new();
        let c = unknown(&mut reg);
        reg.ladder(&cfg(), &caps(), at(6));
        reg.query_sent(c, RpcId(1)).unwrap();
        assert_eq!(
            reg.on_query_outcome(RpcId(1), &outcome),
            LadderResolution::Inconclusive
        );
        assert_eq!(reg.get(c).unwrap().ladder_step(), Some(LadderStep::Resync));
        // Its request is spent.
        assert_eq!(
            reg.on_query_outcome(RpcId(1), &outcome),
            LadderResolution::Ignored
        );
    }

    let mut reg = Registry::new();
    let c = unknown(&mut reg);
    reg.ladder(&cfg(), &caps(), at(6));
    reg.query_sent(c, RpcId(1)).unwrap();
    assert_eq!(
        reg.on_query_outcome(RpcId(1), &accepted()),
        LadderResolution::Ignored
    );
    assert_eq!(
        reg.get(c).unwrap().ladder_step(),
        Some(LadderStep::Querying)
    );
    let answer = QueryAnswer::new(
        RpcId(1),
        OrderRef::Client(c),
        Some(snap(Some(c), "a1", VenueOrderState::Open, 0)),
    )
    .unwrap();
    assert_eq!(
        reg.on_query_answer(&caps(), &answer, key(1)),
        LadderResolution::Resolved(OrdState::Open)
    );

    // An answer or outcome no ladder query of ours asked for changes nothing.
    assert_eq!(
        reg.on_query_outcome(RpcId(99), &SubmitOutcome::Unknown),
        LadderResolution::Ignored
    );
    let stray = QueryAnswer::new(RpcId(98), OrderRef::Client(c), None).unwrap();
    assert_eq!(
        reg.on_query_answer(&caps(), &stray, key(2)),
        LadderResolution::Ignored
    );

    // Nor one for an order an event resolved while the query was out.
    let c = unknown(&mut reg);
    reg.ladder(&cfg(), &caps(), at(6));
    reg.query_sent(c, RpcId(4)).unwrap();
    reg.apply_update(&update(Some(c), VenueOrderState::Open, 0), key(3));
    assert_eq!(reg.get(c).unwrap().ladder_step(), None);
    let late = QueryAnswer::new(RpcId(4), OrderRef::Client(c), None).unwrap();
    assert_eq!(
        reg.on_query_answer(&caps(), &late, key(4)),
        LadderResolution::Ignored
    );
    assert_eq!(reg.get(c).unwrap().state(), OrdState::Open);
}

#[test]
fn a_query_showing_the_order_with_its_command_still_in_flight_is_inconclusive() {
    let mut reg = Registry::new();
    let c = cancel_unanswered(&mut reg, "f1");
    reg.ladder(&cfg(), &caps(), at(16));
    reg.query_sent(c, RpcId(1)).unwrap();
    let answer = QueryAnswer::new(
        RpcId(1),
        OrderRef::Both(c, vid("f1")),
        Some(snap(Some(c), "f1", VenueOrderState::Open, 0)),
    )
    .unwrap();
    assert_eq!(
        reg.on_query_answer(&caps(), &answer, key(1)),
        LadderResolution::Inconclusive
    );
    let rec = reg.get(c).unwrap();
    assert!(matches!(rec.intent(), Intent::PendingCancel { .. }));
    assert_eq!(rec.ladder_step(), Some(LadderStep::Resync));
}

// ---- absent from trustworthy snapshots: Lost ----

#[test]
fn an_order_absent_from_the_configured_trustworthy_snapshots_past_its_sent_time_and_settle_is_lost_and_counted()
 {
    let mut reg = Registry::new();
    let c = unknown(&mut reg);
    let other = cancel_unanswered(&mut reg, "s1");
    let fine = open(&mut reg, "s2");
    let none: &[VenueOrderSnapshot] = &[];
    // Sent at wall 1000 ms with 50 ms to settle: a snapshot at 1049 ms may predate it.
    let applied = reg.on_resync(&cfg(), &caps(), wall(1_049), none, key(1));
    assert_eq!(applied, ResyncApplied::default());
    assert_eq!(reg.get(c).unwrap().absent_snapshots(), 0);
    // From 1050 ms its absence counts: one is not enough.
    reg.on_resync(&cfg(), &caps(), wall(1_050), none, key(2));
    assert_eq!(reg.get(c).unwrap().absent_snapshots(), 1);
    assert_eq!(reg.get(c).unwrap().state(), OrdState::Unknown);
    assert_eq!(reg.lost(), 0);
    assert_eq!(reg.get(other).unwrap().absent_snapshots(), 1);
    // The second in a row, which shows only the other (its cancel still in flight), ends the
    // first Lost, counted, resting nothing; the other's count starts again.
    let shown = [snap(None, "s1", VenueOrderState::Open, 0)];
    let applied = reg.on_resync(&cfg(), &caps(), wall(1_060), &shown, key(3));
    assert_eq!(applied.lost, vec![c]);
    let rec = reg.get(c).unwrap();
    assert_eq!(rec.state(), OrdState::Terminal(TerminalKind::Lost));
    assert_eq!(rec.resting(), lots(0));
    assert_eq!(reg.lost(), 1);
    assert_eq!(reg.get(other).unwrap().absent_snapshots(), 0);
    assert_eq!(
        reg.get(other).unwrap().ladder_step(),
        Some(LadderStep::Resync)
    );
    reg.on_resync(&cfg(), &caps(), wall(1_070), none, key(4));
    assert_eq!(reg.get(other).unwrap().state(), OrdState::Open);
    reg.on_resync(&cfg(), &caps(), wall(1_080), none, key(5));
    assert_eq!(
        reg.get(other).unwrap().state(),
        OrdState::Terminal(TerminalKind::Lost)
    );
    assert_eq!(reg.lost(), 2);
    // An order not on the ladder is not the ladder's to end.
    assert_eq!(reg.get(fine).unwrap().state(), OrdState::Open);
    assert_eq!(reg.get(fine).unwrap().absent_snapshots(), 0);
}

#[test]
fn a_snapshot_showing_an_order_on_the_ladder_resolves_it_as_its_update_would() {
    let mut reg = Registry::new();
    let by_cid = unknown(&mut reg);
    let by_vid = cancel_unanswered(&mut reg, "w2");
    let shown = [
        snap(Some(by_cid), "w1", VenueOrderState::Open, 2),
        snap(None, "w2", canceled(), 0),
    ];
    let applied = reg.on_resync(&cfg(), &caps(), wall(2_000), &shown, key(1));
    assert_eq!(
        applied.resolved,
        vec![
            (by_cid, OrdState::PartiallyFilled),
            (
                by_vid,
                OrdState::Terminal(TerminalKind::Canceled(CancelReason::Requested))
            ),
        ]
    );
    assert!(applied.lost.is_empty());
    assert_eq!(reg.get(by_cid).unwrap().resting(), lots(8));
    assert_eq!(reg.get(by_cid).unwrap().ladder_step(), None);
}

#[test]
fn no_order_is_lost_on_a_snapshot_that_cannot_vouch_for_its_absence() {
    let none: &[VenueOrderSnapshot] = &[];
    // An untrustworthy source, or none: absences never count.
    for source in [SnapshotSource::Untrustworthy, SnapshotSource::None] {
        let venue = OrderCaps {
            snapshot_source: source,
            ..caps()
        };
        let mut reg = Registry::new();
        let c = unknown(&mut reg);
        for n in 0..5 {
            reg.on_resync(&cfg(), &venue, wall(5_000 + n), none, key(n as u64));
        }
        assert_eq!(reg.get(c).unwrap().state(), OrdState::Unknown);
        assert_eq!(reg.lost(), 0);
    }

    // No recorded sent time: nothing shows the snapshot was taken after the order was sent.
    let mut reg = Registry::new();
    let c = reg.insert(placement(cid(), 100, 10)).unwrap().cid();
    reg.on_outcome(
        c,
        OrderOp::Place,
        &item(None, None),
        &SubmitOutcome::Unknown,
        at(5),
    )
    .unwrap();
    for n in 0..5 {
        reg.on_resync(&cfg(), &caps(), wall(5_000 + n), none, key(n as u64));
    }
    assert_eq!(reg.get(c).unwrap().state(), OrdState::Unknown);

    // An amend in flight on a venue whose amend gives a new id: the snapshot may show the
    // order under an id the record does not know.
    let replacing = OrderCaps {
        amend: Some(amend_caps(false)),
        ..caps()
    };
    let mut reg = Registry::new();
    let c = open(&mut reg, "m1");
    reg.amend_sent(c, Ticks(101), lots(10), RpcId(7), at(10))
        .unwrap();
    reg.on_outcome(
        c,
        OrderOp::Amend(RpcId(7)),
        &item(Some(c), None),
        &SubmitOutcome::Unknown,
        at(15),
    )
    .unwrap();
    for n in 0..5 {
        reg.on_resync(&cfg(), &replacing, wall(5_000 + n), none, key(n as u64));
    }
    assert_eq!(reg.get(c).unwrap().state(), OrdState::Open);
    assert_eq!(reg.get(c).unwrap().absent_snapshots(), 0);
}

#[test]
fn a_snapshot_entry_naming_no_order_of_ours_or_two_of_them_resolves_nothing() {
    let mut reg = Registry::new();
    let a = unknown(&mut reg);
    let b = cancel_unanswered(&mut reg, "x2");
    let mut foreign = snap(None, "x9", VenueOrderState::Open, 0);
    foreign.cid = Some(CidMatch::Foreign(Namespace::new(9)));
    let mut odd = snap(None, "x8", VenueOrderState::Open, 0);
    odd.cid = Some(CidMatch::Unparseable);
    // Our client id `a` with the venue id of our order `b`: neither is applied, both shown.
    let conflicting = snap(Some(a), "x2", canceled(), 0);
    let shown = [foreign, odd, conflicting];
    let applied = reg.on_resync(&cfg(), &caps(), wall(2_000), &shown, key(1));
    assert_eq!(applied, ResyncApplied::default());
    for c in [a, b] {
        let rec = reg.get(c).unwrap();
        assert!(!rec.state().is_terminal());
        assert_eq!(rec.absent_snapshots(), 0);
    }
    // An order of ours the snapshot shows while it is off the ladder is left as it is.
    let fine = open(&mut reg, "x3");
    let shown = [snap(Some(fine), "x3", canceled(), 0)];
    let applied = reg.on_resync(&cfg(), &caps(), wall(2_000), &shown, key(2));
    assert!(applied.resolved.is_empty());
    assert_eq!(reg.get(fine).unwrap().state(), OrdState::Open);
}

// ---- the tombstone cancel ----

#[test]
fn an_order_still_on_the_ladder_after_the_maximum_is_tombstone_cancelled_by_client_id() {
    let mut reg = Registry::new();
    let c = unknown(&mut reg);
    // On the ladder since 5 ms, for a second at most.
    let plan = reg.ladder(&cfg(), &caps(), at(1_004));
    assert!(plan.tombstones.is_empty());
    let plan = reg.ladder(&cfg(), &caps(), at(1_005));
    places_or_amends_nothing(&plan);
    assert_eq!(plan.tombstones.len(), 1);
    let (named, cmd) = &plan.tombstones[0];
    assert_eq!(*named, c);
    let VenueCommand::Cancel(cancel) = cmd.command() else {
        panic!("a tombstone is a single cancel: {:?}", cmd.command());
    };
    assert_eq!(cancel.target, OrderRef::Client(c));
    assert_eq!((cancel.inst, cancel.side), (INST, Side::Buy));
    assert_eq!(
        cancel.reference(caps().cancel_refs),
        Some(ChosenRef::Client(c))
    );
    assert_eq!(reg.tombstone_sent(c, RpcId(9), at(1_006)), Ok(true));
    // Not again until another maximum has passed.
    assert!(reg.ladder(&cfg(), &caps(), at(2_004)).tombstones.is_empty());
    assert_eq!(reg.ladder(&cfg(), &caps(), at(2_005)).tombstones.len(), 1);
    // Its acknowledgement, final, resolves the order.
    let outcome = reg.on_outcome(
        c,
        OrderOp::Cancel(RpcId(9)),
        &item(Some(c), None),
        &accepted(),
        at(2_010),
    );
    assert_eq!(outcome, Ok(OutcomeApplied::TombstoneResolved));
    let rec = reg.get(c).unwrap();
    assert_eq!(
        rec.state(),
        OrdState::Terminal(TerminalKind::Canceled(CancelReason::Requested))
    );
    assert_eq!(rec.ladder_step(), None);
    assert_eq!(reg.lost(), 0);
}

#[test]
fn a_tombstone_refused_as_already_terminal_ends_the_order_lost_and_a_provisional_ack_waits() {
    let mut reg = Registry::new();
    let c = unknown(&mut reg);
    reg.ladder(&cfg(), &caps(), at(1_005));
    reg.tombstone_sent(c, RpcId(9), at(1_006)).unwrap();
    let provisional = SubmitOutcome::Accepted {
        ack: AckLevel::Provisional,
    };
    assert_eq!(
        reg.on_outcome(
            c,
            OrderOp::Cancel(RpcId(9)),
            &item(None, None),
            &provisional,
            at(1_007)
        ),
        Ok(OutcomeApplied::Unchanged)
    );
    assert_eq!(reg.get(c).unwrap().state(), OrdState::Unknown);
    let gone = refused(RejectKind::AlreadyTerminal(TerminalHint::Unspecified));
    assert_eq!(
        reg.on_outcome(
            c,
            OrderOp::Cancel(RpcId(9)),
            &item(None, None),
            &gone,
            at(1_008)
        ),
        Ok(OutcomeApplied::TombstoneResolved)
    );
    assert_eq!(
        reg.get(c).unwrap().state(),
        OrdState::Terminal(TerminalKind::Lost)
    );
    assert_eq!(reg.lost(), 1);
    // A terminal order takes no tombstone.
    assert_eq!(reg.tombstone_sent(c, RpcId(10), at(1_009)), Ok(false));

    // An ordinary cancel's acknowledgement still waits for the order's terminal event.
    let other = cancel_unanswered(&mut reg, "t2");
    assert_eq!(
        reg.on_outcome(
            other,
            OrderOp::Cancel(RpcId(50)),
            &item(None, None),
            &accepted(),
            at(20)
        ),
        Ok(OutcomeApplied::Unchanged)
    );
    assert_eq!(reg.get(other).unwrap().state(), OrdState::Open);
    // A tombstone unanswered or refused otherwise leaves the order on the ladder.
    reg.tombstone_sent(other, RpcId(11), at(1_100)).unwrap();
    reg.on_outcome(
        other,
        OrderOp::Cancel(RpcId(11)),
        &item(None, None),
        &refused(RejectKind::Other),
        at(1_101),
    )
    .unwrap();
    assert!(reg.get(other).unwrap().ladder_step().is_some());
}

#[test]
fn a_venue_whose_cancels_cannot_name_a_client_id_gets_no_tombstone() {
    let by_vid = OrderCaps {
        cancel_refs: TagSet::of(&[RefKind::Venue]),
        ..caps()
    };
    let mut reg = Registry::new();
    let c = unknown(&mut reg);
    let plan = reg.ladder(&cfg(), &by_vid, at(1_005));
    assert!(plan.tombstones.is_empty());
    assert_eq!(plan.no_tombstone, vec![c]);
    // Reported once per maximum.
    assert!(
        reg.ladder(&cfg(), &by_vid, at(1_006))
            .no_tombstone
            .is_empty()
    );
}

// ---- never placed or amended again ----

#[test]
fn nothing_for_an_unknown_order_is_ever_placed_or_amended_again() {
    let mut reg = Registry::new();
    let c = unknown(&mut reg);
    let order = reg.get(c).unwrap().placed().clone();
    // Its client id is never placed again.
    assert_eq!(reg.insert(order).err(), Some(OmsError::DuplicateCid(c)));
    // It gets no permit to amend.
    assert_eq!(
        reg.live(c).unwrap_err(),
        PermitRefusal::NotResting(c, OrdState::Unknown)
    );
    // Every pass of its ladder, to the tombstone and after, builds queries and cancels only.
    for t in [6, 7, 500, 1_005, 2_005, 3_005] {
        places_or_amends_nothing(&reg.ladder(&cfg(), &caps(), at(t)));
    }

    // A cancel's outcome Unknown for an order the record holds no cancel in flight for: the
    // order is off the permits until the ladder resolves it, and the venue showing it Open
    // does not while that cancel may still remove it (FBC-tjey); once refused, it does.
    let c = open(&mut reg, "n1");
    reg.on_outcome(
        c,
        OrderOp::Cancel(RpcId(3)),
        &item(None, None),
        &SubmitOutcome::Unknown,
        at(15),
    )
    .unwrap();
    assert_eq!(reg.get(c).unwrap().intent(), Intent::None);
    assert_eq!(reg.live(c).unwrap_err(), PermitRefusal::OnLadder(c));
    let plan = reg.ladder(&cfg(), &caps(), at(16));
    places_or_amends_nothing(&plan);
    reg.query_sent(c, RpcId(4)).unwrap();
    let shown = snap(Some(c), "n1", VenueOrderState::Open, 0);
    let answer =
        QueryAnswer::new(RpcId(4), OrderRef::Both(c, vid("n1")), Some(shown.clone())).unwrap();
    assert_eq!(
        reg.on_query_answer(&caps(), &answer, key(6)),
        LadderResolution::Inconclusive
    );
    assert_eq!(reg.live(c).unwrap_err(), PermitRefusal::OnLadder(c));
    reg.on_outcome(
        c,
        OrderOp::Cancel(RpcId(3)),
        &item(None, None),
        &refused(RejectKind::Other),
        at(17),
    )
    .unwrap();
    let applied = reg.on_resync(&cfg(), &caps(), wall(2_000), &[shown], key(7));
    assert_eq!(applied.resolved, vec![(c, OrdState::Open)]);
    assert!(reg.live(c).is_ok());
}

#[test]
fn a_live_order_leaves_the_ladder_once_the_venue_settles_the_command_that_put_it_there() {
    let amendable = OrderCaps {
        amend: Some(amend_caps(true)),
        ..caps()
    };
    // An amend that went unanswered, then confirmed by the venue's update.
    let mut reg = Registry::new();
    let c = open(&mut reg, "l1");
    reg.amend_sent(c, Ticks(101), lots(10), RpcId(3), at(10))
        .unwrap();
    reg.on_outcome(
        c,
        OrderOp::Amend(RpcId(3)),
        &item(None, None),
        &SubmitOutcome::Unknown,
        at(15),
    )
    .unwrap();
    assert_eq!(reg.live(c).unwrap_err(), PermitRefusal::IntentPending(c));
    let mut amended = update(Some(c), VenueOrderState::Amended { new_vid: None }, 0);
    (amended.px, amended.qty) = (Some(Ticks(101)), Some(lots(10)));
    let later = OrderKey {
        venue: Some(5),
        ingest: 5,
    };
    reg.apply_update(&amended, later);
    let rec = reg.get(c).unwrap();
    assert_eq!(rec.intent(), Intent::None);
    assert_eq!(rec.ladder_step(), None);
    assert!(reg.live(c).is_ok());
    assert!(
        reg.ladder(&cfg(), &amendable, at(5_000))
            .tombstones
            .is_empty()
    );

    // An amend past the intent timeout, then refused: the original rests, nothing in flight.
    let c = open(&mut reg, "l2");
    reg.amend_sent(c, Ticks(101), lots(10), RpcId(5), at(10))
        .unwrap();
    assert_eq!(reg.ladder(&cfg(), &amendable, at(110)).escalated, vec![c]);
    assert_eq!(
        reg.on_outcome(
            c,
            OrderOp::Amend(RpcId(5)),
            &item(None, None),
            &refused(RejectKind::Other),
            at(120)
        ),
        Ok(OutcomeApplied::IntentCleared)
    );
    assert_eq!(reg.get(c).unwrap().ladder_step(), None);
    assert!(reg.live(c).is_ok());

    // An Unknown order's settled cancel leaves an Unknown order on the ladder: its placement
    // is still unknown.
    let c = unknown(&mut reg);
    reg.cancel_sent(c, RpcId(6), at(6)).unwrap();
    reg.on_outcome(
        c,
        OrderOp::Cancel(RpcId(6)),
        &item(None, None),
        &refused(RejectKind::Other),
        at(7),
    )
    .unwrap();
    assert_eq!(reg.get(c).unwrap().ladder_step(), Some(LadderStep::Query));
}

#[test]
fn a_query_acknowledged_but_never_answered_leaves_the_order_to_resyncs_after_the_intent_timeout() {
    let mut reg = Registry::new();
    let c = unknown(&mut reg);
    reg.ladder(&cfg(), &caps(), at(6));
    reg.query_sent(c, RpcId(1)).unwrap();
    assert_eq!(
        reg.on_query_outcome(RpcId(1), &accepted()),
        LadderResolution::Ignored
    );
    // The acknowledgement cleared the request's deadline; the ladder keeps its own.
    let plan = reg.ladder(&cfg(), &caps(), at(105));
    assert!(!plan.resync);
    assert_eq!(
        reg.get(c).unwrap().ladder_step(),
        Some(LadderStep::Querying)
    );
    let plan = reg.ladder(&cfg(), &caps(), at(106));
    assert!(plan.resync);
    assert_eq!(reg.get(c).unwrap().ladder_step(), Some(LadderStep::Resync));
    // A result arriving after that is no longer awaited.
    let late = QueryAnswer::new(RpcId(1), OrderRef::Client(c), None).unwrap();
    assert_eq!(
        reg.on_query_answer(&caps(), &late, key(1)),
        LadderResolution::Ignored
    );
}

#[test]
fn every_tombstone_out_resolves_the_order_whichever_answers() {
    for (answer, ends) in [
        (
            accepted(),
            OrdState::Terminal(TerminalKind::Canceled(CancelReason::Requested)),
        ),
        (
            refused(RejectKind::AlreadyTerminal(TerminalHint::Canceled)),
            OrdState::Terminal(TerminalKind::Lost),
        ),
    ] {
        let mut reg = Registry::new();
        let c = unknown(&mut reg);
        reg.ladder(&cfg(), &caps(), at(1_005));
        reg.tombstone_sent(c, RpcId(9), at(1_005)).unwrap();
        // Unanswered a whole maximum: another goes out.
        assert_eq!(reg.ladder(&cfg(), &caps(), at(2_005)).tombstones.len(), 1);
        reg.tombstone_sent(c, RpcId(10), at(2_005)).unwrap();
        // The first one's answer arrives.
        assert_eq!(
            reg.on_outcome(
                c,
                OrderOp::Cancel(RpcId(9)),
                &item(None, None),
                &answer,
                at(2_006)
            ),
            Ok(OutcomeApplied::TombstoneResolved)
        );
        assert_eq!(reg.get(c).unwrap().state(), ends);
    }
}

// ---- I9: a stale intent escalates within its timeout plus one query ----

#[test]
fn a_stale_intent_escalates_within_its_timeout_plus_one_query() {
    for amend in [true, false] {
        let mut reg = Registry::new();
        let c = open(&mut reg, "i1");
        if amend {
            reg.amend_sent(c, Ticks(101), lots(10), RpcId(3), at(10))
                .unwrap();
        } else {
            reg.cancel_sent(c, RpcId(3), at(10)).unwrap();
        }
        // Nothing answers it. Before its timeout, the ladder leaves it alone.
        let plan = reg.ladder(&cfg(), &caps(), at(109));
        assert_eq!(plan, LadderPlan::default());
        assert_eq!(reg.get(c).unwrap().ladder_step(), None);
        // At its timeout it escalates and is queried.
        let plan = reg.ladder(&cfg(), &caps(), at(110));
        assert_eq!(plan.escalated, vec![c]);
        assert_eq!(query_of(&plan, c).target, OrderRef::Both(c, vid("i1")));
        assert_eq!(reg.get(c).unwrap().unknown_since(), Some(at(110)));
        assert_eq!(reg.get(c).unwrap().resting(), lots(10));
        reg.query_sent(c, RpcId(4)).unwrap();
        // One query round trip later, unanswered: on the ladder, resyncs deciding.
        assert_eq!(
            reg.on_query_outcome(RpcId(4), &SubmitOutcome::Unknown),
            LadderResolution::Inconclusive
        );
        let plan = reg.ladder(&cfg(), &caps(), at(130));
        assert!(plan.resync);
        assert!(plan.escalated.is_empty(), "escalated once");
        assert_eq!(reg.get(c).unwrap().ladder_step(), Some(LadderStep::Resync));
    }

    // A cancel the venue answers within the query: resolved, the order ended.
    let mut reg = Registry::new();
    let c = open(&mut reg, "i2");
    reg.cancel_sent(c, RpcId(3), at(10)).unwrap();
    reg.ladder(&cfg(), &caps(), at(110));
    reg.query_sent(c, RpcId(4)).unwrap();
    let answer = QueryAnswer::new(
        RpcId(4),
        OrderRef::Both(c, vid("i2")),
        Some(snap(Some(c), "i2", canceled(), 0)),
    )
    .unwrap();
    assert!(matches!(
        reg.on_query_answer(&caps(), &answer, key(1)),
        LadderResolution::Resolved(OrdState::Terminal(_))
    ));
}

// ---- configuration and refusals ----

#[test]
fn the_ladder_refuses_numbers_that_would_make_it_act_at_once() {
    let ok = LadderConfig::new(ms(100), ms(0), ms(1_000), 1).unwrap();
    assert_eq!(
        (
            ok.intent_timeout(),
            ok.settle(),
            ok.max_unknown(),
            ok.absent_snapshots()
        ),
        (ms(100), ms(0), ms(1_000), 1)
    );
    let cases = [
        (
            LadderConfig::new(ms(0), ms(50), ms(1_000), 2),
            LadderConfigError::ZeroIntentTimeout,
            "the ladder's intent timeout is zero",
        ),
        (
            LadderConfig::new(ms(100), ms(50), ms(0), 2),
            LadderConfigError::ZeroMaxUnknown,
            "the ladder's maximum time unknown is zero",
        ),
        (
            LadderConfig::new(ms(100), ms(50), ms(1_000), 0),
            LadderConfigError::ZeroAbsentSnapshots,
            "the ladder needs no absent snapshot to declare an order lost",
        ),
    ];
    for (made, error, text) in cases {
        assert_eq!(made, Err(error));
        assert_eq!(error.to_string(), text);
    }
}

#[test]
fn the_ladders_records_are_refused_for_an_order_the_registry_does_not_hold_or_a_conflicting_send() {
    let mut reg = Registry::new();
    let stranger = cid();
    assert_eq!(
        reg.placement_sent(stranger, at(0), wall(0)),
        Err(OmsError::UnknownCid(stranger))
    );
    assert_eq!(
        reg.query_sent(stranger, RpcId(1)),
        Err(OmsError::UnknownCid(stranger))
    );
    assert_eq!(
        reg.tombstone_sent(stranger, RpcId(1), at(0)),
        Err(OmsError::UnknownCid(stranger))
    );
    let c = reg.insert(placement(cid(), 100, 10)).unwrap().cid();
    assert_eq!(reg.get(c).unwrap().sent_at(), None);
    reg.placement_sent(c, at(1), wall(2)).unwrap();
    // The same instants again change nothing; others are refused.
    assert_eq!(reg.placement_sent(c, at(1), wall(2)), Ok(()));
    let refusal = reg.placement_sent(c, at(1), wall(3)).unwrap_err();
    assert_eq!(refusal, OmsError::SentRecorded(c));
    assert!(refusal.to_string().contains("already recorded as sent"));
    assert_eq!(reg.get(c).unwrap().sent_at(), Some((at(1), wall(2))));
}

// ---- review: answers and snapshots that name another order, ladder kept while unsettled ----

#[test]
fn a_query_answer_naming_another_order_than_the_one_queried_is_never_applied() {
    let mut reg = Registry::new();
    let c = unknown(&mut reg);
    let other = open(&mut reg, "g9");
    reg.ladder(&cfg(), &caps(), at(6));
    reg.query_sent(c, RpcId(1)).unwrap();
    // The reply carries our query's request but names another of our orders: its snapshot
    // agrees with what it names, not with what we asked.
    let answer = QueryAnswer::new(
        RpcId(1),
        OrderRef::Client(other),
        Some(snap(Some(other), "g9", VenueOrderState::Open, 4)),
    )
    .unwrap();
    assert_eq!(
        reg.on_query_answer(&caps(), &answer, key(1)),
        LadderResolution::TargetMismatch
    );
    let rec = reg.get(c).unwrap();
    assert_eq!(rec.state(), OrdState::Unknown);
    assert_eq!(rec.vid(), None);
    assert_eq!(rec.ladder_step(), Some(LadderStep::Resync));
    assert_eq!(reg.get(other).unwrap().filled(), lots(0));

    // Our client id with a venue id another order of ours holds: not applied either.
    let c = unknown(&mut reg);
    reg.ladder(&cfg(), &caps(), at(6));
    reg.query_sent(c, RpcId(2)).unwrap();
    let answer = QueryAnswer::new(
        RpcId(2),
        OrderRef::Both(c, vid("g9")),
        Some(snap(Some(c), "g9", canceled(), 0)),
    )
    .unwrap();
    assert_eq!(
        reg.on_query_answer(&caps(), &answer, key(2)),
        LadderResolution::TargetMismatch
    );
    assert_eq!(reg.get(c).unwrap().state(), OrdState::Unknown);
    assert_eq!(reg.get(other).unwrap().state(), OrdState::Open);
}

#[test]
fn a_query_by_client_id_answered_with_a_venue_id_another_order_of_ours_holds_applies_nothing() {
    // FBC-k2w9 (Codex r4188432397 on PR #72): an Unknown order has no venue id, so its query
    // names it by client id alone and QueryAnswer::new accepts a snapshot carrying any venue
    // id. One another order of ours holds names that order: applying it would give the
    // queried record the other order's id and state, take it off the ladder, and later
    // commands on it would name the wrong order.
    for shown_cid in [true, false] {
        let mut reg = Registry::new();
        let c = unknown(&mut reg);
        let other = open(&mut reg, "g9");
        reg.ladder(&cfg(), &caps(), at(6));
        reg.query_sent(c, RpcId(1)).unwrap();
        let shown = snap(shown_cid.then_some(c), "g9", canceled(), 4);
        let answer = QueryAnswer::new(RpcId(1), OrderRef::Client(c), Some(shown)).unwrap();
        assert_eq!(
            reg.on_query_answer(&caps(), &answer, key(1)),
            LadderResolution::TargetMismatch,
            "client id shown: {shown_cid}"
        );
        // Nothing applies to the queried order: still Unknown, no venue id, on the ladder.
        let rec = reg.get(c).unwrap();
        assert_eq!(rec.state(), OrdState::Unknown);
        assert_eq!(rec.vid(), None);
        assert_eq!(rec.filled(), lots(0));
        assert_eq!(rec.unknown_since(), Some(at(5)));
        assert_eq!(rec.ladder_step(), Some(LadderStep::Resync));
        // Nor to the order holding that venue id.
        let held = reg.get(other).unwrap();
        assert_eq!(held.state(), OrdState::Open);
        assert_eq!(held.vid(), Some(&vid("g9")));
        assert_eq!(held.filled(), lots(0));
        assert_eq!(held.unknown_since(), None);
        // The query is spent: the same answer again changes nothing.
        assert_eq!(
            reg.on_query_answer(&caps(), &answer, key(2)),
            LadderResolution::Ignored
        );
        assert_eq!(reg.queries_awaited(), 0);
        // Resyncs decide: the next pass asks for one and queries nothing again.
        let plan = reg.ladder(&cfg(), &caps(), at(7));
        assert!(plan.resync);
        assert!(plan.queries.is_empty() && plan.escalated.is_empty());
        // A resync showing the order under its own venue id resolves it; the other is untouched.
        let entries = [
            snap(Some(c), "g10", VenueOrderState::Open, 0),
            snap(Some(other), "g9", VenueOrderState::Open, 0),
        ];
        let applied = reg.on_resync(&cfg(), &caps(), wall(2_000), &entries, key(3));
        assert_eq!(applied.resolved, vec![(c, OrdState::Open)]);
        assert_eq!(reg.get(c).unwrap().vid(), Some(&vid("g10")));
        assert_eq!(reg.get(other).unwrap().vid(), Some(&vid("g9")));
        assert_eq!(reg.get(other).unwrap().state(), OrdState::Open);
    }
}

#[test]
fn a_snapshot_entry_under_another_namespaces_or_a_non_canonical_client_id_is_never_routed_by_venue_id()
 {
    for seen in [CidMatch::Foreign(Namespace::new(9)), CidMatch::Unparseable] {
        let mut reg = Registry::new();
        let c = cancel_unanswered(&mut reg, "h1");
        let mut entry = snap(None, "h1", canceled(), 0);
        entry.cid = Some(seen);
        let none: &[VenueOrderSnapshot] = &[];
        reg.on_resync(&cfg(), &caps(), wall(2_000), none, key(1));
        assert_eq!(reg.get(c).unwrap().absent_snapshots(), 1);
        let applied = reg.on_resync(&cfg(), &caps(), wall(2_010), &[entry], key(2));
        assert_eq!(applied, ResyncApplied::default());
        let rec = reg.get(c).unwrap();
        assert_eq!(
            rec.state(),
            OrdState::Open,
            "{seen:?} moves nothing of ours"
        );
        // Its venue id is ours: it is not counted absent either.
        assert_eq!(rec.absent_snapshots(), 0);
    }
}

#[test]
fn an_order_shown_resting_with_its_cancel_still_unresolved_stays_on_the_ladder() {
    // An Unknown order with a tombstone out: the venue shows it Open. Its placement is
    // settled, its cancel is not: it stays on the ladder, the tombstone clock running on.
    let mut reg = Registry::new();
    let c = unknown(&mut reg);
    reg.ladder(&cfg(), &caps(), at(6));
    reg.query_sent(c, RpcId(1)).unwrap();
    reg.on_query_outcome(RpcId(1), &SubmitOutcome::Unknown);
    reg.ladder(&cfg(), &caps(), at(1_005));
    reg.tombstone_sent(c, RpcId(9), at(1_005)).unwrap();
    let shown = [snap(Some(c), "k1", VenueOrderState::Open, 0)];
    let applied = reg.on_resync(&cfg(), &caps(), wall(2_000), &shown, key(1));
    assert!(applied.resolved.is_empty());
    let rec = reg.get(c).unwrap();
    assert_eq!(rec.state(), OrdState::Open);
    assert_eq!(rec.unknown_since(), Some(at(5)));
    assert_eq!(rec.ladder_step(), Some(LadderStep::Resync));
    let plan = reg.ladder(&cfg(), &caps(), at(2_004));
    assert!(plan.escalated.is_empty() && plan.queries.is_empty());
    assert!(plan.tombstones.is_empty());
    assert_eq!(reg.ladder(&cfg(), &caps(), at(2_005)).tombstones.len(), 1);

    // A placement cancelled before its acknowledgement, both past the intent timeout: the
    // query shows it Open, its cancel still in flight.
    let mut reg = Registry::new();
    let c = reg.insert(placement(cid(), 100, 10)).unwrap().cid();
    reg.placement_sent(c, at(0), wall(1_000)).unwrap();
    reg.cancel_sent(c, RpcId(2), at(1)).unwrap();
    let plan = reg.ladder(&cfg(), &caps(), at(101));
    assert_eq!(plan.escalated, vec![c]);
    reg.query_sent(c, RpcId(3)).unwrap();
    let answer = QueryAnswer::new(
        RpcId(3),
        OrderRef::Client(c),
        Some(snap(Some(c), "k2", VenueOrderState::Open, 0)),
    )
    .unwrap();
    assert_eq!(
        reg.on_query_answer(&caps(), &answer, key(1)),
        LadderResolution::Inconclusive
    );
    let rec = reg.get(c).unwrap();
    assert_eq!(rec.unknown_since(), Some(at(101)));
    let plan = reg.ladder(&cfg(), &caps(), at(102));
    assert!(plan.escalated.is_empty(), "not escalated again");
    assert!(plan.queries.is_empty(), "not queried again");
    assert!(plan.resync);
}

// ---- review round 3: what put the order on the ladder, and the queries awaited ----

#[test]
fn a_placement_shown_resting_takes_a_pending_new_order_with_nothing_in_flight_off_the_ladder() {
    let mut reg = Registry::new();
    let c = reg.insert(placement(cid(), 100, 10)).unwrap().cid();
    reg.cancel_sent(c, RpcId(2), at(1)).unwrap();
    reg.on_outcome(
        c,
        OrderOp::Cancel(RpcId(2)),
        &item(None, None),
        &SubmitOutcome::Unknown,
        at(2),
    )
    .unwrap();
    assert_eq!(reg.get(c).unwrap().state(), OrdState::PendingNew);
    // The cancel's late refusal settles it, but the placement is still unknown.
    reg.on_outcome(
        c,
        OrderOp::Cancel(RpcId(2)),
        &item(None, None),
        &refused(RejectKind::Other),
        at(3),
    )
    .unwrap();
    assert!(reg.get(c).unwrap().ladder_step().is_some());
    // The acknowledgement shows it resting with nothing in flight: off the ladder.
    reg.on_outcome(
        c,
        OrderOp::Place,
        &item(None, Some(vid("p1"))),
        &accepted(),
        at(4),
    )
    .unwrap();
    let rec = reg.get(c).unwrap();
    assert_eq!(rec.state(), OrdState::Open);
    assert_eq!(rec.ladder_step(), None);
    assert!(reg.live(c).is_ok());
}

#[test]
fn a_later_command_settled_leaves_the_order_on_the_ladder_while_the_one_that_put_it_there_is_not() {
    // What names the command whose fate puts an order on the ladder.
    assert_eq!(OrderOp::Place.rpc(), None);
    assert_eq!(OrderOp::Cancel(RpcId(3)).rpc(), Some(RpcId(3)));
    for first_amend in [true, false] {
        let amendable = OrderCaps {
            amend: Some(amend_caps(false)),
            ..caps()
        };
        let mut reg = Registry::new();
        let c = open(&mut reg, "b1");
        // A goes unanswered: the ladder takes the order.
        let a = OrderOp::Amend(RpcId(3));
        if first_amend {
            reg.amend_sent(c, Ticks(101), lots(12), RpcId(3), at(10))
                .unwrap();
        } else {
            reg.cancel_sent(c, RpcId(3), at(10)).unwrap();
        }
        let a = if first_amend {
            a
        } else {
            OrderOp::Cancel(RpcId(3))
        };
        reg.on_outcome(c, a, &item(None, None), &SubmitOutcome::Unknown, at(15))
            .unwrap();
        // A safety cancel B fails: A's fate is still unknown.
        for b in [
            refused(RejectKind::Other),
            SubmitOutcome::NotSent(fbc_core::NotSentReason::Disconnected),
        ] {
            reg.cancel_sent(c, RpcId(4), at(20)).unwrap();
            assert_eq!(
                reg.on_outcome(c, OrderOp::Cancel(RpcId(4)), &item(None, None), &b, at(21)),
                Ok(OutcomeApplied::IntentCleared)
            );
            let rec = reg.get(c).unwrap();
            assert_eq!(rec.unknown_since(), Some(at(15)));
            assert_eq!(reg.live(c).unwrap_err(), PermitRefusal::OnLadder(c));
        }
        // Still counted at the largest total A may have left resting.
        let resting = if first_amend { lots(12) } else { lots(10) };
        assert_eq!(reg.get(c).unwrap().resting(), resting);
        places_or_amends_nothing(&reg.ladder(&cfg(), &amendable, at(30)));
    }
}

#[test]
fn a_query_awaited_for_an_order_that_left_the_ladder_is_forgotten_on_the_next_pass() {
    let mut reg = Registry::new();
    let cids: Vec<ClientOrderId> = (0..3).map(|_| unknown(&mut reg)).collect();
    assert_eq!(reg.ladder(&cfg(), &caps(), at(6)).queries.len(), 3);
    for (n, &c) in cids.iter().enumerate() {
        let n = n as u64;
        reg.query_sent(c, RpcId(n)).unwrap();
        reg.on_query_outcome(RpcId(n), &accepted());
        // An event resolves it before the result, which never comes.
        reg.apply_update(&update(Some(c), VenueOrderState::Open, 0), key(n));
        assert_eq!(reg.get(c).unwrap().ladder_step(), None);
    }
    assert_eq!(reg.queries_awaited(), 3);
    reg.ladder(&cfg(), &caps(), at(7));
    assert_eq!(reg.queries_awaited(), 0);

    // An order back on the ladder never takes its earlier query's late result.
    let c = cancel_unanswered(&mut reg, "z1");
    reg.ladder(&cfg(), &caps(), at(16));
    reg.query_sent(c, RpcId(20)).unwrap();
    reg.on_query_outcome(RpcId(20), &accepted());
    let shown = [snap(Some(c), "z1", VenueOrderState::Open, 0)];
    reg.on_resync(&cfg(), &caps(), wall(2_000), &shown, key(10));
    // Its cancel is settled (refused): off the ladder, then a new cancel goes unanswered.
    reg.on_outcome(
        c,
        OrderOp::Cancel(RpcId(50)),
        &item(None, None),
        &refused(RejectKind::Other),
        at(17),
    )
    .unwrap();
    assert_eq!(reg.get(c).unwrap().ladder_step(), None);
    reg.cancel_sent(c, RpcId(51), at(18)).unwrap();
    reg.on_outcome(
        c,
        OrderOp::Cancel(RpcId(51)),
        &item(None, None),
        &SubmitOutcome::Unknown,
        at(19),
    )
    .unwrap();
    let late = QueryAnswer::new(
        RpcId(20),
        OrderRef::Both(c, vid("z1")),
        Some(snap(Some(c), "z1", canceled(), 0)),
    )
    .unwrap();
    assert_eq!(
        reg.on_query_answer(&caps(), &late, key(11)),
        LadderResolution::Ignored
    );
    assert_eq!(reg.get(c).unwrap().state(), OrdState::Open);
}

// ---- FBC-e90m: every tombstone sent must be answered before the order leaves the ladder ----

/// The ways a later tombstone is answered without ending the order: not sent, or refused for
/// another reason than the order having ended.
fn later_tombstone_answers() -> [SubmitOutcome; 2] {
    [
        SubmitOutcome::NotSent(fbc_core::NotSentReason::Backpressure),
        refused(RejectKind::Other),
    ]
}

/// The consumer's numbers with a query awaited 5 s, so its answer can arrive after two
/// tombstones a second apart.
fn slow_query_cfg() -> LadderConfig {
    LadderConfig::new(ms(5_000), ms(50), ms(1_000), 2).unwrap()
}

/// `c` on the ladder (entered at 5 or 15 ms) queried at its first pass under request 1, with a
/// tombstone 9 sent at 1020 ms and left unanswered, and a later tombstone 10 sent at 2020 ms
/// answered by `later`: whatever the venue shows next, tombstone 9 may still remove it.
fn earlier_tombstone_unanswered(reg: &mut Registry, c: ClientOrderId, later: &SubmitOutcome) {
    let cfg = slow_query_cfg();
    let plan = reg.ladder(&cfg, &caps(), at(20));
    assert_eq!(plan.queries.len(), 1);
    reg.query_sent(c, RpcId(1)).unwrap();
    assert_eq!(reg.ladder(&cfg, &caps(), at(1_020)).tombstones.len(), 1);
    reg.tombstone_sent(c, RpcId(9), at(1_020)).unwrap();
    assert_eq!(reg.ladder(&cfg, &caps(), at(2_020)).tombstones.len(), 1);
    reg.tombstone_sent(c, RpcId(10), at(2_020)).unwrap();
    reg.on_outcome(
        c,
        OrderOp::Cancel(RpcId(10)),
        &item(Some(c), None),
        later,
        at(2_021),
    )
    .unwrap();
    let rec = reg.get(c).unwrap();
    assert!(rec.unknown_since().is_some());
    assert_eq!(rec.intent(), Intent::None);
}

#[test]
fn an_order_with_an_earlier_tombstone_unanswered_stays_on_the_ladder_until_it_answers() {
    for later in later_tombstone_answers() {
        for by_query in [true, false] {
            for placement_unknown in [true, false] {
                let case = format!("{later:?}, query {by_query}, unknown {placement_unknown}");
                let mut reg = Registry::new();
                let c = if placement_unknown {
                    unknown(&mut reg)
                } else {
                    cancel_unanswered(&mut reg, "e1")
                };
                earlier_tombstone_unanswered(&mut reg, c, &later);

                // The venue shows it Open: the earlier tombstone may yet remove it.
                let shown = snap(Some(c), "e1", VenueOrderState::Open, 0);
                if by_query {
                    let answer =
                        QueryAnswer::new(RpcId(1), OrderRef::Client(c), Some(shown)).unwrap();
                    assert_eq!(
                        reg.on_query_answer(&caps(), &answer, key(1)),
                        LadderResolution::Inconclusive,
                        "{case}"
                    );
                } else {
                    let applied =
                        reg.on_resync(&slow_query_cfg(), &caps(), wall(3_000), &[shown], key(1));
                    assert!(applied.resolved.is_empty(), "{case}");
                }
                let rec = reg.get(c).unwrap();
                assert_eq!(rec.state(), OrdState::Open, "{case}");
                assert!(rec.unknown_since().is_some(), "{case}");
                assert_eq!(rec.ladder_step(), Some(LadderStep::Resync), "{case}");
                assert_eq!(rec.resting(), lots(10), "{case}");
                assert_eq!(
                    reg.live(c).unwrap_err(),
                    PermitRefusal::OnLadder(c),
                    "{case}"
                );
                // A late acknowledgement of the placement settles nothing more.
                reg.on_outcome(
                    c,
                    OrderOp::Place,
                    &item(None, Some(vid("e1"))),
                    &accepted(),
                    at(2_028),
                )
                .unwrap();
                assert!(reg.get(c).unwrap().unknown_since().is_some(), "{case}");
                assert!(
                    reg.ladder(&slow_query_cfg(), &caps(), at(2_030)).resync,
                    "{case}"
                );

                // The earlier tombstone's final acceptance ends it Canceled.
                assert_eq!(
                    reg.on_outcome(
                        c,
                        OrderOp::Cancel(RpcId(9)),
                        &item(Some(c), None),
                        &accepted(),
                        at(2_040)
                    ),
                    Ok(OutcomeApplied::TombstoneResolved),
                    "{case}"
                );
                assert_eq!(
                    reg.get(c).unwrap().state(),
                    OrdState::Terminal(TerminalKind::Canceled(CancelReason::Requested)),
                    "{case}"
                );
            }
        }
    }
}

#[test]
fn an_order_whose_every_tombstone_was_answered_leaves_the_ladder_as_the_venue_shows_it() {
    for later in later_tombstone_answers() {
        let mut reg = Registry::new();
        let c = unknown(&mut reg);
        earlier_tombstone_unanswered(&mut reg, c, &later);
        // The earlier tombstone refused too, but not because the order ended.
        reg.on_outcome(
            c,
            OrderOp::Cancel(RpcId(9)),
            &item(Some(c), None),
            &refused(RejectKind::Other),
            at(2_027),
        )
        .unwrap();
        let shown = snap(Some(c), "e2", VenueOrderState::Open, 0);
        let answer = QueryAnswer::new(RpcId(1), OrderRef::Client(c), Some(shown)).unwrap();
        assert_eq!(
            reg.on_query_answer(&caps(), &answer, key(1)),
            LadderResolution::Resolved(OrdState::Open),
            "{later:?}"
        );
        assert!(reg.live(c).is_ok(), "{later:?}");
    }
}

#[test]
fn a_tombstone_whose_fate_is_unknown_or_only_provisionally_accepted_stays_outstanding() {
    for answer in [
        SubmitOutcome::Unknown,
        SubmitOutcome::Accepted {
            ack: AckLevel::Provisional,
        },
    ] {
        let mut reg = Registry::new();
        let c = unknown(&mut reg);
        earlier_tombstone_unanswered(&mut reg, c, &refused(RejectKind::Other));
        reg.on_outcome(
            c,
            OrderOp::Cancel(RpcId(9)),
            &item(Some(c), None),
            &answer,
            at(2_027),
        )
        .unwrap();
        let shown = [snap(Some(c), "e3", VenueOrderState::Open, 0)];
        let applied = reg.on_resync(&slow_query_cfg(), &caps(), wall(3_000), &shown, key(1));
        assert!(applied.resolved.is_empty(), "{answer:?}");
        assert_eq!(
            reg.live(c).unwrap_err(),
            PermitRefusal::OnLadder(c),
            "{answer:?}"
        );
    }
}

/// The open order `m1` with an amend to 101 sent at 10 ms that went unanswered at 15 ms, on the
/// ladder with its amend unconfirmed, and queried at 16 ms on the venue `venue`: the query's
/// target.
fn amend_unanswered(reg: &mut Registry, venue: &OrderCaps) -> (ClientOrderId, OrderRef) {
    let c = open(reg, "m1");
    reg.amend_sent(c, Ticks(101), lots(10), RpcId(7), at(10))
        .unwrap();
    reg.on_outcome(
        c,
        OrderOp::Amend(RpcId(7)),
        &item(Some(c), None),
        &SubmitOutcome::Unknown,
        at(15),
    )
    .unwrap();
    let rec = reg.get(c).unwrap();
    assert!(rec.amend_unconfirmed());
    assert_eq!(rec.unknown_since(), Some(at(15)));
    assert_eq!(reg.live(c).unwrap_err(), PermitRefusal::IntentPending(c));
    let plan = reg.ladder(&cfg(), venue, at(16));
    let target = query_of(&plan, c).target.clone();
    reg.query_sent(c, RpcId(8)).unwrap();
    (c, target)
}

/// Every command the ladder builds for `c` in passes at `passes` ms names it by client id,
/// never by the venue id an unconfirmed amend may have retired, and a tombstone is among them.
fn names_only_the_client_id(
    reg: &mut Registry,
    replacing: &OrderCaps,
    c: ClientOrderId,
    passes: &[u64],
) {
    let mut tombstoned = false;
    for t in passes {
        let plan = reg.ladder(&cfg(), replacing, at(*t));
        places_or_amends_nothing(&plan);
        for (_, query) in &plan.queries {
            let ControlCommand::Query(query) = query else {
                panic!("not a query: {query:?}");
            };
            assert_eq!(query.target, OrderRef::Client(c));
        }
        for (named, cmd) in &plan.tombstones {
            assert_eq!(*named, c);
            let VenueCommand::Cancel(cancel) = cmd.command() else {
                panic!("a tombstone is a single cancel: {:?}", cmd.command());
            };
            assert_eq!(cancel.target, OrderRef::Client(c));
            assert_eq!(
                cancel.reference(replacing.cancel_refs),
                Some(ChosenRef::Client(c))
            );
            tombstoned = true;
        }
    }
    assert!(tombstoned, "a tombstone by client id follows");
    // Nor can a permit build an amend or a cancel naming it: it stays off the permits.
    assert!(reg.live(c).is_err());
}

#[test]
fn a_snapshot_showing_an_order_under_another_venue_id_while_its_amend_may_have_moved_it_resolves_nothing()
 {
    // FBC-wua8 (Codex r4188432405 on PR #72): on a venue whose amend replaces the venue id, a
    // query answer or a resync may show an order with an unconfirmed amend under its new id.
    // Applied, its price and total would confirm the amend and take the order off the ladder
    // while the record keeps the retired id, which later commands would then name. The
    // snapshot is inconclusive instead: the order stays on the ladder, the amend in flight,
    // and a tombstone by client id follows.
    let replacing = OrderCaps {
        amend: Some(amend_caps(false)),
        ..caps()
    };
    // Under a venue ordering key later than any applied: nothing marks the snapshot as stale.
    let later = OrderKey {
        venue: Some(5),
        ingest: 5,
    };
    // Our order `c` shown resting as `v` at the amend's price and total.
    let amended = |c: ClientOrderId, v: &str| {
        let mut shown = snap(Some(c), v, VenueOrderState::Open, 0);
        shown.px = Some(Ticks(101));
        shown
    };

    // A query answer: the query named the order by client id alone, the amend may have
    // replaced `m1`.
    let mut reg = Registry::new();
    let (c, target) = amend_unanswered(&mut reg, &replacing);
    assert_eq!(target, OrderRef::Client(c));
    let shown = amended(c, "m2");
    let answer = QueryAnswer::new(RpcId(8), OrderRef::Client(c), Some(shown.clone())).unwrap();
    assert_eq!(
        reg.on_query_answer(&replacing, &answer, later),
        LadderResolution::Inconclusive
    );
    let rec = reg.get(c).unwrap();
    assert_eq!(rec.vid(), Some(&vid("m1")));
    assert!(matches!(rec.intent(), Intent::PendingAmend { .. }));
    assert_eq!((rec.px(), rec.qty()), (Some(Ticks(100)), lots(10)));
    assert_eq!(rec.unknown_since(), Some(at(15)));
    assert_eq!(rec.ladder_step(), Some(LadderStep::Resync));
    // A resync showing it so resolves nothing either, nor counts it absent.
    for n in 0..3 {
        let applied = reg.on_resync(
            &cfg(),
            &replacing,
            wall(5_000 + n),
            std::slice::from_ref(&shown),
            OrderKey {
                venue: Some(6 + n as u64),
                ingest: 6 + n as u64,
            },
        );
        assert_eq!(applied, ResyncApplied::default());
    }
    let rec = reg.get(c).unwrap();
    assert_eq!(rec.vid(), Some(&vid("m1")));
    assert!(matches!(rec.intent(), Intent::PendingAmend { .. }));
    assert_eq!(rec.unknown_since(), Some(at(15)));
    assert_eq!(rec.absent_snapshots(), 0);
    assert_eq!(reg.lost(), 0);
    names_only_the_client_id(&mut reg, &replacing, c, &[17, 500, 1_015, 1_016]);

    // A resync, the query having gone unanswered.
    let mut reg = Registry::new();
    let (c, _) = amend_unanswered(&mut reg, &replacing);
    let shown = amended(c, "m2");
    assert_eq!(
        reg.on_query_outcome(RpcId(8), &SubmitOutcome::Unknown),
        LadderResolution::Inconclusive
    );
    let applied = reg.on_resync(
        &cfg(),
        &replacing,
        wall(5_000),
        std::slice::from_ref(&shown),
        later,
    );
    assert_eq!(applied, ResyncApplied::default());
    let rec = reg.get(c).unwrap();
    assert_eq!(rec.vid(), Some(&vid("m1")));
    assert!(matches!(rec.intent(), Intent::PendingAmend { .. }));
    assert_eq!(rec.unknown_since(), Some(at(15)));
    assert_eq!(rec.ladder_step(), Some(LadderStep::Resync));
    assert_eq!(rec.absent_snapshots(), 0);
    names_only_the_client_id(&mut reg, &replacing, c, &[17, 500, 1_015]);

    // Shown under the record's own id, the snapshot applies as its update would: here it
    // confirms the amend and settles the order.
    let mut reg = Registry::new();
    let (c, _) = amend_unanswered(&mut reg, &replacing);
    let answer = QueryAnswer::new(RpcId(8), OrderRef::Client(c), Some(amended(c, "m1"))).unwrap();
    assert_eq!(
        reg.on_query_answer(&replacing, &answer, later),
        LadderResolution::Resolved(OrdState::Open)
    );
    assert_eq!(reg.get(c).unwrap().vid(), Some(&vid("m1")));
    assert_eq!(reg.get(c).unwrap().px(), Some(Ticks(101)));

    // A record that holds no venue id yet (the placement accepted without one) has none to
    // retire: the snapshot teaches it the id and settles the order as its update would (Codex
    // r4224963871 on PR #134).
    let mut reg = Registry::new();
    let c = reg.insert(placement(cid(), 100, 10)).unwrap().cid();
    reg.placement_sent(c, at(0), wall(1_000)).unwrap();
    reg.on_outcome(c, OrderOp::Place, &item(None, None), &accepted(), at(1))
        .unwrap();
    assert_eq!(reg.get(c).unwrap().vid(), None);
    reg.amend_sent(c, Ticks(101), lots(10), RpcId(7), at(10))
        .unwrap();
    reg.on_outcome(
        c,
        OrderOp::Amend(RpcId(7)),
        &item(Some(c), None),
        &SubmitOutcome::Unknown,
        at(15),
    )
    .unwrap();
    let plan = reg.ladder(&cfg(), &replacing, at(16));
    assert_eq!(query_of(&plan, c).target, OrderRef::Client(c));
    reg.query_sent(c, RpcId(8)).unwrap();
    let answer = QueryAnswer::new(RpcId(8), OrderRef::Client(c), Some(amended(c, "m2"))).unwrap();
    assert_eq!(
        reg.on_query_answer(&replacing, &answer, later),
        LadderResolution::Resolved(OrdState::Open)
    );
    let rec = reg.get(c).unwrap();
    assert_eq!(rec.vid(), Some(&vid("m2")));
    assert_eq!(rec.intent(), Intent::None);
    assert_eq!(rec.px(), Some(Ticks(101)));

    // On a venue whose amend keeps the id, the record's id is never retired: a snapshot under
    // another id applies as its update would.
    let keeping = OrderCaps {
        amend: Some(amend_caps(true)),
        ..caps()
    };
    let mut reg = Registry::new();
    let (c, target) = amend_unanswered(&mut reg, &keeping);
    assert_eq!(target, OrderRef::Both(c, vid("m1")));
    let answer = QueryAnswer::new(RpcId(8), OrderRef::Client(c), Some(amended(c, "m2"))).unwrap();
    assert_eq!(
        reg.on_query_answer(&keeping, &answer, later),
        LadderResolution::Resolved(OrdState::Open)
    );
}

#[test]
fn a_snapshot_under_another_venue_id_while_an_amend_may_have_moved_the_order_still_ends_it_and_counts_its_fills()
 {
    // Codex r4225065388 and r4225065395 on PR #134: the snapshot an unconfirmed amend makes
    // ambiguous is a resting one. One showing the order ended under another venue id ends it
    // as its update would (it names no id later commands could carry), and one showing it
    // resting there still counts the venue's cumulative fill, the amend left unconfirmed.
    let replacing = OrderCaps {
        amend: Some(amend_caps(false)),
        ..caps()
    };
    let later = OrderKey {
        venue: Some(5),
        ingest: 5,
    };

    // A query answer showing it filled under the replacement id.
    let mut reg = Registry::new();
    let (c, _) = amend_unanswered(&mut reg, &replacing);
    let filled = snap(Some(c), "m2", VenueOrderState::Filled, 10);
    let answer = QueryAnswer::new(RpcId(8), OrderRef::Client(c), Some(filled)).unwrap();
    let ended = OrdState::Terminal(TerminalKind::Filled);
    assert_eq!(
        reg.on_query_answer(&replacing, &answer, later),
        LadderResolution::Resolved(ended)
    );
    let rec = reg.get(c).unwrap();
    assert_eq!(rec.state(), ended);
    assert_eq!(rec.vid(), Some(&vid("m2")));
    assert_eq!(rec.cum_venue(), lots(10));
    assert_eq!(rec.ladder_step(), None);

    // A resync showing it canceled under the replacement id.
    let mut reg = Registry::new();
    let (c, _) = amend_unanswered(&mut reg, &replacing);
    reg.on_query_outcome(RpcId(8), &SubmitOutcome::Unknown);
    let gone = snap(Some(c), "m2", canceled(), 3);
    let applied = reg.on_resync(
        &cfg(),
        &replacing,
        wall(5_000),
        std::slice::from_ref(&gone),
        later,
    );
    let ended = OrdState::Terminal(TerminalKind::Canceled(CancelReason::Requested));
    assert_eq!(applied.resolved, vec![(c, ended)]);
    assert_eq!(reg.get(c).unwrap().vid(), Some(&vid("m2")));
    assert_eq!(reg.get(c).unwrap().cum_venue(), lots(3));

    // Shown resting there, partly filled: the fill counts, nothing else applies.
    let mut reg = Registry::new();
    let (c, _) = amend_unanswered(&mut reg, &replacing);
    let mut part = snap(Some(c), "m2", VenueOrderState::Open, 4);
    part.px = Some(Ticks(101));
    let answer = QueryAnswer::new(RpcId(8), OrderRef::Client(c), Some(part.clone())).unwrap();
    assert_eq!(
        reg.on_query_answer(&replacing, &answer, later),
        LadderResolution::Inconclusive
    );
    let rec = reg.get(c).unwrap();
    assert_eq!((rec.cum_venue(), rec.filled()), (lots(4), lots(4)));
    assert_eq!(rec.resting(), lots(6));
    // Partly filled, as a fill or an update would show it (Codex r4225165100).
    assert_eq!(rec.state(), OrdState::PartiallyFilled);
    assert_eq!(rec.vid(), Some(&vid("m1")));
    assert!(matches!(rec.intent(), Intent::PendingAmend { .. }));
    assert_eq!(rec.px(), Some(Ticks(100)));
    assert_eq!(rec.unknown_since(), Some(at(15)));
    // A resync showing more filled counts it; one showing less takes nothing back.
    for (cum, n) in [(6, 6), (2, 7)] {
        part.cum_filled = lots(cum);
        let applied = reg.on_resync(
            &cfg(),
            &replacing,
            wall(5_000),
            std::slice::from_ref(&part),
            OrderKey {
                venue: Some(n),
                ingest: n,
            },
        );
        assert_eq!(applied, ResyncApplied::default());
        assert_eq!(reg.get(c).unwrap().cum_venue(), lots(6));
    }
    let rec = reg.get(c).unwrap();
    assert_eq!(rec.vid(), Some(&vid("m1")));
    assert!(matches!(rec.intent(), Intent::PendingAmend { .. }));
    assert_eq!(rec.unknown_since(), Some(at(15)));
}

// ---- FBC-tjey: the amend or cancel whose fate is unknown must be answered too ----

/// What a later command does to an order on the ladder without ending it: a safety cancel 4
/// sent at 30 ms and not sent or refused, or a tombstone 9 sent at 1015 ms and refused.
#[derive(Debug, Clone, Copy)]
enum Later {
    SafetyNotSent,
    SafetyRefused,
    TombstoneRefused,
}

const LATER: [Later; 3] = [
    Later::SafetyNotSent,
    Later::SafetyRefused,
    Later::TombstoneRefused,
];

/// `c`, on the ladder since 15 ms and queried at its first pass under request 1, takes the
/// `later` command, which clears the command in flight without answering the first one.
fn later_command_settled(reg: &mut Registry, c: ClientOrderId, later: Later) {
    let cfg = slow_query_cfg();
    assert_eq!(reg.ladder(&cfg, &caps(), at(20)).queries.len(), 1);
    reg.query_sent(c, RpcId(1)).unwrap();
    let (rpc, outcome, now) = match later {
        Later::SafetyNotSent | Later::SafetyRefused => {
            reg.cancel_sent(c, RpcId(4), at(30)).unwrap();
            let outcome = match later {
                Later::SafetyNotSent => {
                    SubmitOutcome::NotSent(fbc_core::NotSentReason::Disconnected)
                }
                _ => refused(RejectKind::Other),
            };
            (RpcId(4), outcome, at(31))
        }
        Later::TombstoneRefused => {
            assert_eq!(reg.ladder(&cfg, &caps(), at(1_015)).tombstones.len(), 1);
            reg.tombstone_sent(c, RpcId(9), at(1_015)).unwrap();
            (RpcId(9), refused(RejectKind::Other), at(1_016))
        }
    };
    reg.on_outcome(c, OrderOp::Cancel(rpc), &item(Some(c), None), &outcome, now)
        .unwrap();
    let rec = reg.get(c).unwrap();
    assert_eq!(rec.intent(), Intent::None, "{later:?}");
    assert_eq!(rec.unknown_since(), Some(at(15)), "{later:?}");
}

/// The venue shows `c` Open, by the answer to query 1 or by a resync: whether it resolved it.
fn shown_open(reg: &mut Registry, c: ClientOrderId, by_query: bool, v: &str) -> bool {
    let shown = snap(Some(c), v, VenueOrderState::Open, 0);
    if by_query {
        let answer = QueryAnswer::new(RpcId(1), OrderRef::Client(c), Some(shown)).unwrap();
        match reg.on_query_answer(&caps(), &answer, key(1)) {
            LadderResolution::Resolved(state) => {
                assert_eq!(state, OrdState::Open);
                true
            }
            other => {
                assert_eq!(other, LadderResolution::Inconclusive);
                false
            }
        }
    } else {
        let applied = reg.on_resync(&slow_query_cfg(), &caps(), wall(3_000), &[shown], key(1));
        assert!(applied.lost.is_empty());
        !applied.resolved.is_empty()
    }
}

/// `c` stays on the ladder, Open and counted fully resting, with no Live permit.
fn held_on_the_ladder(reg: &mut Registry, c: ClientOrderId, case: &str) {
    let rec = reg.get(c).unwrap();
    assert_eq!(rec.state(), OrdState::Open, "{case}");
    assert_eq!(rec.unknown_since(), Some(at(15)), "{case}");
    assert_eq!(rec.ladder_step(), Some(LadderStep::Resync), "{case}");
    assert_eq!(rec.resting(), lots(10), "{case}");
    assert_eq!(
        reg.live(c).unwrap_err(),
        PermitRefusal::OnLadder(c),
        "{case}"
    );
}

#[test]
fn an_open_order_whose_cancel_went_unanswered_stays_on_the_ladder_after_a_later_command_is_refused()
{
    for later in LATER {
        for by_query in [true, false] {
            let case = format!("{later:?}, query {by_query}");
            let mut reg = Registry::new();
            // Cancel 50, sent at 10 ms, went unanswered at 15 ms.
            let c = cancel_unanswered(&mut reg, "u1");
            later_command_settled(&mut reg, c, later);

            // The venue shows it Open: cancel 50 may yet remove it.
            assert!(!shown_open(&mut reg, c, by_query, "u1"), "{case}");
            held_on_the_ladder(&mut reg, c, &case);
            assert!(
                reg.ladder(&slow_query_cfg(), &caps(), at(1_020)).resync,
                "{case}"
            );

            // Cancel 50's late final acceptance is not the order ended (a fill may race it,
            // 0080; Codex r4228419828 on PR #146): the order stays held, counted fully
            // resting, a resync showing it Open still settling nothing, until its terminal
            // event ends it Canceled.
            assert_eq!(
                reg.on_outcome(
                    c,
                    OrderOp::Cancel(RpcId(50)),
                    &item(Some(c), None),
                    &accepted(),
                    at(1_030)
                ),
                Ok(OutcomeApplied::Unchanged),
                "{case}"
            );
            assert!(!shown_open(&mut reg, c, false, "u1"), "{case}");
            held_on_the_ladder(&mut reg, c, &case);
            reg.apply_update(&update(Some(c), canceled(), 0), key(2));
            let rec = reg.get(c).unwrap();
            assert_eq!(
                rec.state(),
                OrdState::Terminal(TerminalKind::Canceled(CancelReason::Requested)),
                "{case}"
            );
            assert_eq!(rec.unknown_since(), None, "{case}");
            assert_eq!(rec.resting(), lots(0), "{case}");
            assert_eq!(reg.lost(), 0, "{case}");
        }
    }
}

#[test]
fn an_order_whose_unanswered_amend_or_cancel_is_answered_leaves_the_ladder_as_the_venue_shows_it() {
    // Refused (NotFound included) or not sent, the first command changed nothing: the order
    // leaves the ladder as the venue shows it, with a Live permit.
    for answer in [
        refused(RejectKind::Other),
        refused(RejectKind::NotFound),
        SubmitOutcome::NotSent(fbc_core::NotSentReason::Backpressure),
    ] {
        for later in LATER {
            let case = format!("{answer:?}, {later:?}");
            let mut reg = Registry::new();
            let c = cancel_unanswered(&mut reg, "a1");
            later_command_settled(&mut reg, c, later);
            reg.on_outcome(
                c,
                OrderOp::Cancel(RpcId(50)),
                &item(Some(c), None),
                &answer,
                at(1_020),
            )
            .unwrap();
            assert!(shown_open(&mut reg, c, false, "a1"), "{case}");
            assert_eq!(reg.get(c).unwrap().unknown_since(), None, "{case}");
            assert!(reg.live(c).is_ok(), "{case}");
        }
    }

    // An amend whose fate went unknown holds the order the same way until it is answered:
    // accepted for good, it changed the order but removed nothing.
    for answer in [refused(RejectKind::Other), accepted()] {
        let case = format!("amend {answer:?}");
        let mut reg = Registry::new();
        let c = open(&mut reg, "a2");
        reg.amend_sent(c, Ticks(101), lots(10), RpcId(50), at(10))
            .unwrap();
        reg.on_outcome(
            c,
            OrderOp::Amend(RpcId(50)),
            &item(Some(c), None),
            &SubmitOutcome::Unknown,
            at(15),
        )
        .unwrap();
        later_command_settled(&mut reg, c, Later::SafetyRefused);
        assert!(!shown_open(&mut reg, c, true, "a2"), "{case}");
        held_on_the_ladder(&mut reg, c, &case);
        reg.on_outcome(
            c,
            OrderOp::Amend(RpcId(50)),
            &item(Some(c), None),
            &answer,
            at(40),
        )
        .unwrap();
        assert!(shown_open(&mut reg, c, false, "a2"), "{case}");
        assert!(reg.live(c).is_ok(), "{case}");
    }
}

#[test]
fn an_amend_or_cancel_whose_fate_is_unknown_holds_the_order_whichever_way_it_got_there() {
    // A cancel past the intent timeout, never answered; a cancel on an order already on the
    // ladder for its placement, unanswered; a first cancel answered only provisionally,
    // unanswered again, or refused as already ended (its terminal event or a tombstone
    // settles that): each may still remove the order, or have.
    for how in 0..5 {
        let mut reg = Registry::new();
        let c = match how {
            0 => {
                let c = open(&mut reg, "w1");
                reg.cancel_sent(c, RpcId(50), at(10)).unwrap();
                // Past the 100 ms intent timeout: escalated, its query built.
                assert_eq!(reg.ladder(&cfg(), &caps(), at(110)).escalated, vec![c]);
                c
            }
            1 => {
                let c = unknown(&mut reg);
                reg.cancel_sent(c, RpcId(50), at(10)).unwrap();
                reg.on_outcome(
                    c,
                    OrderOp::Cancel(RpcId(50)),
                    &item(Some(c), None),
                    &SubmitOutcome::Unknown,
                    at(12),
                )
                .unwrap();
                c
            }
            _ => {
                let c = cancel_unanswered(&mut reg, "w1");
                let again = match how {
                    2 => SubmitOutcome::Accepted {
                        ack: AckLevel::Provisional,
                    },
                    3 => SubmitOutcome::Unknown,
                    _ => refused(RejectKind::AlreadyTerminal(TerminalHint::Unspecified)),
                };
                reg.on_outcome(
                    c,
                    OrderOp::Cancel(RpcId(50)),
                    &item(Some(c), None),
                    &again,
                    at(16),
                )
                .unwrap();
                c
            }
        };
        let case = format!("case {how}");
        let since = reg.get(c).unwrap().unknown_since().unwrap();
        let cfg = slow_query_cfg();
        if how == 0 {
            // The pass that escalated it built its query.
            reg.query_sent(c, RpcId(1)).unwrap();
        } else {
            assert_eq!(reg.ladder(&cfg, &caps(), at(20)).queries.len(), 1);
            reg.query_sent(c, RpcId(1)).unwrap();
        }
        reg.cancel_sent(c, RpcId(4), at(30)).unwrap();
        reg.on_outcome(
            c,
            OrderOp::Cancel(RpcId(4)),
            &item(Some(c), None),
            &refused(RejectKind::Other),
            at(31),
        )
        .unwrap();
        assert!(!shown_open(&mut reg, c, how % 2 == 0, "w1"), "{case}");
        let rec = reg.get(c).unwrap();
        assert_eq!(rec.state(), OrdState::Open, "{case}");
        assert_eq!(rec.unknown_since(), Some(since), "{case}");
        assert_eq!(
            reg.live(c).unwrap_err(),
            PermitRefusal::OnLadder(c),
            "{case}"
        );
        // A late placement acknowledgement settles nothing more.
        reg.on_outcome(
            c,
            OrderOp::Place,
            &item(None, Some(vid("w1"))),
            &accepted(),
            at(32),
        )
        .unwrap();
        assert!(reg.get(c).unwrap().unknown_since().is_some(), "{case}");
    }
}

#[test]
fn a_command_replaced_on_the_ladder_holds_the_order_until_answered_or_settled_by_a_later_total() {
    // Codex r4228419870 on PR #146: an order already on the ladder for its placement, whose
    // cancel 50 is still in flight (past the intent timeout or not) when a safety cancel
    // replaces it and is refused: cancel 50 may still remove it.
    for pass in [false, true] {
        let case = format!("timed-out pass {pass}");
        let mut reg = Registry::new();
        let c = unknown(&mut reg);
        reg.cancel_sent(c, RpcId(50), at(10)).unwrap();
        if pass {
            places_or_amends_nothing(&reg.ladder(&cfg(), &caps(), at(200)));
        }
        reg.cancel_sent(c, RpcId(4), at(300)).unwrap();
        reg.on_outcome(
            c,
            OrderOp::Cancel(RpcId(4)),
            &item(Some(c), None),
            &refused(RejectKind::Other),
            at(301),
        )
        .unwrap();
        let shown = snap(Some(c), "r1", VenueOrderState::Open, 0);
        let applied = reg.on_resync(
            &cfg(),
            &caps(),
            wall(2_000),
            std::slice::from_ref(&shown),
            key(1),
        );
        assert!(applied.resolved.is_empty(), "{case}");
        assert_eq!(
            reg.live(c).unwrap_err(),
            PermitRefusal::OnLadder(c),
            "{case}"
        );
        // Refused, cancel 50 changed nothing: the next resync resolves it.
        reg.on_outcome(
            c,
            OrderOp::Cancel(RpcId(50)),
            &item(Some(c), None),
            &refused(RejectKind::Other),
            at(302),
        )
        .unwrap();
        let applied = reg.on_resync(&cfg(), &caps(), wall(2_100), &[shown], key(2));
        assert_eq!(applied.resolved, vec![(c, OrdState::Open)], "{case}");
        assert!(reg.live(c).is_ok(), "{case}");
    }

    // Codex r4228419857 on PR #146: an amend whose fate went unknown, replaced by a cancel
    // that is refused, is settled once the venue states the total under a later venue key with
    // nothing in flight, as every amend replaced in flight is; an earlier key settles nothing.
    let ordered = |venue| OrderKey {
        venue: Some(venue),
        ingest: venue,
    };
    let mut reg = Registry::new();
    let c = open(&mut reg, "s1");
    reg.apply_update(&update(Some(c), VenueOrderState::Open, 0), ordered(1));
    reg.amend_sent(c, Ticks(101), lots(10), RpcId(50), at(10))
        .unwrap();
    reg.on_outcome(
        c,
        OrderOp::Amend(RpcId(50)),
        &item(Some(c), None),
        &SubmitOutcome::Unknown,
        at(15),
    )
    .unwrap();
    later_command_settled(&mut reg, c, Later::SafetyRefused);
    let mut shown = snap(Some(c), "s1", VenueOrderState::Open, 0);
    shown.px = Some(Ticks(101));
    let stale = reg.on_resync(
        &cfg(),
        &caps(),
        wall(3_000),
        std::slice::from_ref(&shown),
        ordered(1),
    );
    assert!(stale.resolved.is_empty());
    assert_eq!(reg.live(c).unwrap_err(), PermitRefusal::OnLadder(c));
    let applied = reg.on_resync(&cfg(), &caps(), wall(3_100), &[shown], ordered(2));
    assert_eq!(applied.resolved, vec![(c, OrdState::Open)]);
    assert!(reg.live(c).is_ok());
}

#[test]
fn a_command_answered_while_still_in_flight_is_not_awaited_once_replaced() {
    // Codex r4228627001 on PR #146: a cancel the venue does not know (NotFound) changed
    // nothing, and an amend accepted for good removed nothing, though each stays the command
    // in flight; replaced by a safety cancel that is refused, neither holds the order on the
    // ladder.
    for amend in [false, true] {
        let case = format!("amend {amend}");
        let mut reg = Registry::new();
        let c = open(&mut reg, "x1");
        let (op, answer) = if amend {
            reg.amend_sent(c, Ticks(100), lots(10), RpcId(50), at(10))
                .unwrap();
            (OrderOp::Amend(RpcId(50)), accepted())
        } else {
            reg.cancel_sent(c, RpcId(50), at(10)).unwrap();
            (OrderOp::Cancel(RpcId(50)), refused(RejectKind::NotFound))
        };
        // On the ladder: the amend by its timeout, the cancel by the venue not knowing it.
        if amend {
            assert_eq!(reg.ladder(&cfg(), &caps(), at(110)).escalated, vec![c]);
        }
        reg.on_outcome(c, op, &item(Some(c), None), &answer, at(111))
            .unwrap();
        let rec = reg.get(c).unwrap();
        assert!(rec.unknown_since().is_some(), "{case}");
        assert_ne!(rec.intent(), Intent::None, "{case}");
        reg.cancel_sent(c, RpcId(4), at(120)).unwrap();
        reg.on_outcome(
            c,
            OrderOp::Cancel(RpcId(4)),
            &item(Some(c), None),
            &refused(RejectKind::Other),
            at(121),
        )
        .unwrap();
        let shown = [snap(Some(c), "x1", VenueOrderState::Open, 0)];
        let applied = reg.on_resync(&cfg(), &caps(), wall(2_000), &shown, key(1));
        assert_eq!(applied.resolved, vec![(c, OrdState::Open)], "{case}");
        assert!(reg.live(c).is_ok(), "{case}");
    }
}

#[test]
fn an_amend_confirmed_before_its_unknown_outcome_does_not_hold_the_order() {
    // RB-tjey-1 on PR #146: an update confirmed amend 50 (or amend 51, which replaced it in
    // flight, settling it with its total) before amend 50's outcome came back Unknown. Nothing
    // can answer amend 50 after that, and it can no longer move the order: the late Unknown
    // puts the order on the ladder, as before FBC-tjey, and the venue showing it resting
    // resolves it, with a Live permit.
    let ordered = |venue| OrderKey {
        venue: Some(venue),
        ingest: venue,
    };
    for replaced in [false, true] {
        let case = format!("replaced {replaced}");
        let mut reg = Registry::new();
        let c = open(&mut reg, "z1");
        reg.amend_sent(c, Ticks(101), lots(10), RpcId(50), at(10))
            .unwrap();
        let px = if replaced {
            reg.amend_sent(c, Ticks(102), lots(10), RpcId(51), at(11))
                .unwrap();
            102
        } else {
            101
        };
        let mut confirmed = update(Some(c), VenueOrderState::Amended { new_vid: None }, 0);
        confirmed.px = Some(Ticks(px));
        confirmed.qty = Some(lots(10));
        reg.apply_update(&confirmed, ordered(1));
        let rec = reg.get(c).unwrap();
        assert_eq!(rec.intent(), Intent::None, "{case}");
        assert!(!rec.amend_unconfirmed(), "{case}");
        reg.on_outcome(
            c,
            OrderOp::Amend(RpcId(50)),
            &item(Some(c), None),
            &SubmitOutcome::Unknown,
            at(15),
        )
        .unwrap();
        assert_eq!(reg.get(c).unwrap().unknown_since(), Some(at(15)), "{case}");
        assert_eq!(
            reg.ladder(&cfg(), &caps(), at(20)).queries.len(),
            1,
            "{case}"
        );
        reg.query_sent(c, RpcId(1)).unwrap();
        let mut shown = snap(Some(c), "z1", VenueOrderState::Open, 0);
        shown.px = Some(Ticks(px));
        let answer = QueryAnswer::new(RpcId(1), OrderRef::Client(c), Some(shown)).unwrap();
        assert_eq!(
            reg.on_query_answer(&caps(), &answer, ordered(2)),
            LadderResolution::Resolved(OrdState::Open),
            "{case}"
        );
        assert_eq!(reg.get(c).unwrap().unknown_since(), None, "{case}");
        assert!(reg.live(c).is_ok(), "{case}");
    }
}
