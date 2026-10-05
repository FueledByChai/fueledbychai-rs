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
        reg.on_query_answer(&answer, key(1)),
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
        reg.on_query_answer(&answer, key(2)),
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
        reg.on_query_answer(&answer, key(3)),
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
        reg.on_query_answer(&answer, key(4)),
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
        reg.on_query_answer(&answer, key(1)),
        LadderResolution::Resolved(OrdState::Open)
    );

    // An answer or outcome no ladder query of ours asked for changes nothing.
    assert_eq!(
        reg.on_query_outcome(RpcId(99), &SubmitOutcome::Unknown),
        LadderResolution::Ignored
    );
    let stray = QueryAnswer::new(RpcId(98), OrderRef::Client(c), None).unwrap();
    assert_eq!(
        reg.on_query_answer(&stray, key(2)),
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
        reg.on_query_answer(&late, key(4)),
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
        reg.on_query_answer(&answer, key(1)),
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

    // An amend that went unanswered, then confirmed by the venue: the order is off the
    // permits until the ladder resolves it.
    let amendable = OrderCaps {
        amend: Some(amend_caps(true)),
        ..caps()
    };
    let c = open(&mut reg, "n1");
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
    reg.apply_update(
        &amended,
        OrderKey {
            venue: Some(5),
            ingest: 5,
        },
    );
    assert_eq!(reg.get(c).unwrap().intent(), Intent::None);
    assert_eq!(reg.live(c).unwrap_err(), PermitRefusal::OnLadder(c));
    let plan = reg.ladder(&cfg(), &amendable, at(16));
    places_or_amends_nothing(&plan);
    reg.query_sent(c, RpcId(4)).unwrap();
    let mut shown = snap(Some(c), "n1", VenueOrderState::Open, 0);
    shown.px = Some(Ticks(101));
    let answer = QueryAnswer::new(RpcId(4), OrderRef::Both(c, vid("n1")), Some(shown)).unwrap();
    assert_eq!(
        reg.on_query_answer(&answer, key(6)),
        LadderResolution::Resolved(OrdState::Open)
    );
    assert!(reg.live(c).is_ok());
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
        reg.on_query_answer(&answer, key(1)),
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
        reg.on_query_answer(&answer, key(1)),
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
        reg.on_query_answer(&answer, key(2)),
        LadderResolution::TargetMismatch
    );
    assert_eq!(reg.get(c).unwrap().state(), OrdState::Unknown);
    assert_eq!(reg.get(other).unwrap().state(), OrdState::Open);
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
        reg.on_query_answer(&answer, key(1)),
        LadderResolution::Inconclusive
    );
    let rec = reg.get(c).unwrap();
    assert_eq!(rec.unknown_since(), Some(at(101)));
    let plan = reg.ladder(&cfg(), &caps(), at(102));
    assert!(plan.escalated.is_empty(), "not escalated again");
    assert!(plan.queries.is_empty(), "not queried again");
    assert!(plan.resync);
}
