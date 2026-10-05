//! Each command item's outcome moves an order as decision 0005 states: a placement not sent or
//! refused ends it, an accepted one opens it under the item's venue id, an unanswered one moves
//! it to Unknown, which counts as fully resting and is never resent, and a refused amend or
//! cancel leaves the original alive.

mod common;

use common::{canceled, cid, lots, placement, update, vid};
use fbc_core::{
    AckLevel, ItemRef, MonoNs, NotSentReason, Reject, RejectKind, RpcId, SubmitOutcome,
    TerminalHint, Ticks, VenueOrderState,
};
use fbc_oms::{
    Applied, Intent, OmsError, OrdState, OrderKey, OrderOp, OrderRecord, OutcomeApplied, Registry,
    TerminalKind,
};

fn accepted() -> SubmitOutcome {
    SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    }
}

fn refused(kind: RejectKind) -> SubmitOutcome {
    SubmitOutcome::Rejected(Reject {
        kind,
        venue_code: Some("E1".into()),
        raw: "refused".into(),
    })
}

fn key(ingest: u64) -> OrderKey {
    OrderKey {
        venue: None,
        ingest,
    }
}

/// A record whose placement the venue accepted as `v1`: Open, 10 lots at 100.
fn open_record() -> OrderRecord {
    let mut rec = OrderRecord::new(placement(cid(), 100, 10));
    let v1 = vid("v1");
    assert_eq!(
        rec.on_outcome(OrderOp::Place, Some(&v1), &accepted(), MonoNs(1)),
        OutcomeApplied::Opened
    );
    rec
}

#[test]
fn a_placement_not_sent_ends_the_order() {
    let mut rec = OrderRecord::new(placement(cid(), 100, 10));
    let outcome = SubmitOutcome::NotSent(NotSentReason::RateBudget);
    assert_eq!(
        rec.on_outcome(OrderOp::Place, None, &outcome, MonoNs(5)),
        OutcomeApplied::Ended
    );
    assert_eq!(
        rec.state(),
        OrdState::Terminal(TerminalKind::NotSent(NotSentReason::RateBudget))
    );
    assert_eq!(rec.resting(), lots(0));
}

#[test]
fn an_accepted_placement_opens_the_order_under_the_items_venue_id() {
    let rec = open_record();
    assert_eq!(rec.state(), OrdState::Open);
    assert_eq!(rec.vid(), Some(&vid("v1")));
    assert_eq!(rec.resting(), lots(10));
    assert_eq!(rec.unknown_since(), None);
}

#[test]
fn a_refused_placement_ends_the_order() {
    let mut rec = OrderRecord::new(placement(cid(), 100, 10));
    assert_eq!(
        rec.on_outcome(
            OrderOp::Place,
            None,
            &refused(RejectKind::PostOnlyWouldCross),
            MonoNs(5)
        ),
        OutcomeApplied::Ended
    );
    assert_eq!(
        rec.state(),
        OrdState::Terminal(TerminalKind::Rejected(RejectKind::PostOnlyWouldCross))
    );
}

#[test]
fn an_unanswered_placement_is_unknown_fully_resting_and_never_resent() {
    let mut registry = Registry::new();
    let order = placement(cid(), 100, 10);
    let cid = order.cid;
    registry.insert(order.clone()).unwrap();
    let item = ItemRef {
        idx: 0,
        cid: Some(cid),
        vid: None,
    };

    let moved = registry.on_outcome(
        cid,
        OrderOp::Place,
        &item,
        &SubmitOutcome::Unknown,
        MonoNs(7),
    );
    assert_eq!(moved, Ok(OutcomeApplied::MovedToUnknown));
    let rec = registry.get(cid).unwrap();
    assert_eq!(rec.state(), OrdState::Unknown);
    assert_eq!(rec.unknown_since(), Some(MonoNs(7)));
    assert_eq!(rec.resting(), lots(10), "Unknown counts as fully resting");

    // The outcome is not a command, and nothing places the order again: its client id is
    // taken for good.
    assert_eq!(
        registry.insert(order).err(),
        Some(OmsError::DuplicateCid(cid))
    );
    // A second Unknown changes nothing and keeps when the order became unknown.
    let again = registry.on_outcome(
        cid,
        OrderOp::Place,
        &item,
        &SubmitOutcome::Unknown,
        MonoNs(9),
    );
    assert_eq!(again, Ok(OutcomeApplied::Unchanged));
    assert_eq!(registry.get(cid).unwrap().unknown_since(), Some(MonoNs(7)));

    // The venue's own event resolves it.
    let routed = registry.apply_update(&update(Some(cid), VenueOrderState::Open, 0), key(1));
    assert_eq!(routed, fbc_oms::Routed::Ours(cid, Applied::Advanced));
    let rec = registry.get(cid).unwrap();
    assert_eq!(rec.state(), OrdState::Open);
    assert_eq!(rec.unknown_since(), None);
}

#[test]
fn a_placement_the_venue_does_not_know_is_unknown_never_terminal() {
    let mut rec = OrderRecord::new(placement(cid(), 100, 10));
    assert_eq!(
        rec.on_outcome(
            OrderOp::Place,
            None,
            &refused(RejectKind::NotFound),
            MonoNs(3)
        ),
        OutcomeApplied::MovedToUnknown
    );
    assert_eq!(rec.state(), OrdState::Unknown);
}

#[test]
fn an_acceptance_after_a_reported_fill_opens_the_order_partly_filled() {
    let mut rec = OrderRecord::new(placement(cid(), 100, 10));
    // An amended update can report a fill before the placement's acceptance arrives.
    let amended = update(None, VenueOrderState::Amended { new_vid: None }, 4);
    assert_eq!(rec.apply_update(&amended, key(0)), Applied::Amended);
    assert_eq!(rec.state(), OrdState::PendingNew);
    assert_eq!(
        rec.on_outcome(OrderOp::Place, None, &accepted(), MonoNs(2)),
        OutcomeApplied::Opened
    );
    assert_eq!(rec.state(), OrdState::PartiallyFilled);
    assert_eq!(rec.resting(), lots(6));
}

#[test]
fn a_late_acceptance_opens_an_unknown_order() {
    let mut rec = OrderRecord::new(placement(cid(), 100, 10));
    rec.on_outcome(OrderOp::Place, None, &SubmitOutcome::Unknown, MonoNs(3));
    let v = vid("late");
    assert_eq!(
        rec.on_outcome(OrderOp::Place, Some(&v), &accepted(), MonoNs(4)),
        OutcomeApplied::Opened
    );
    assert_eq!(rec.state(), OrdState::Open);
    assert_eq!(rec.unknown_since(), None);
    assert_eq!(rec.vid(), Some(&v));
}

#[test]
fn an_outcome_never_moves_an_order_the_venue_already_showed_back_down() {
    let mut rec = OrderRecord::new(placement(cid(), 100, 10));
    let mut fill = update(None, VenueOrderState::Open, 4);
    fill.vid = Some(vid("v9"));
    assert_eq!(rec.apply_update(&fill, key(0)), Applied::Advanced);
    assert_eq!(rec.state(), OrdState::PartiallyFilled);

    // The acceptance raced the order event: it opens nothing and keeps the venue id known.
    assert_eq!(
        rec.on_outcome(OrderOp::Place, Some(&vid("other")), &accepted(), MonoNs(2)),
        OutcomeApplied::Unchanged
    );
    assert_eq!(rec.vid(), Some(&vid("v9")));
    // The placement's deadline passing after the venue showed the order moves nothing.
    assert_eq!(
        rec.on_outcome(OrderOp::Place, None, &SubmitOutcome::Unknown, MonoNs(3)),
        OutcomeApplied::Unchanged
    );
    assert_eq!(rec.state(), OrdState::PartiallyFilled);
    assert_eq!(rec.unknown_since(), None);
}

#[test]
fn a_refused_or_unsent_amend_leaves_the_original_alive() {
    for outcome in [
        refused(RejectKind::NoChange),
        refused(RejectKind::AlreadyTerminal(TerminalHint::Filled)),
        SubmitOutcome::NotSent(NotSentReason::Backpressure),
    ] {
        let mut rec = open_record();
        assert!(rec.amend_sent(Ticks(101), lots(12), RpcId(2), MonoNs(2)));
        assert!(matches!(rec.intent(), Intent::PendingAmend { .. }));
        assert_eq!(
            rec.on_outcome(OrderOp::Amend, Some(&vid("v1")), &outcome, MonoNs(3)),
            OutcomeApplied::IntentCleared
        );
        assert_eq!(rec.state(), OrdState::Open);
        assert_eq!(rec.intent(), Intent::None);
        assert_eq!((rec.px(), rec.qty()), (Some(Ticks(100)), lots(10)));
        assert_eq!(rec.resting(), lots(10));
    }
}

#[test]
fn an_accepted_amend_waits_for_the_amended_event() {
    let mut rec = open_record();
    rec.amend_sent(Ticks(101), lots(12), RpcId(2), MonoNs(2));
    assert_eq!(
        rec.on_outcome(OrderOp::Amend, None, &accepted(), MonoNs(3)),
        OutcomeApplied::Unchanged
    );
    assert!(matches!(rec.intent(), Intent::PendingAmend { .. }));

    let mut amended = update(None, VenueOrderState::Amended { new_vid: None }, 0);
    (amended.px, amended.qty) = (Some(Ticks(101)), Some(lots(12)));
    assert_eq!(rec.apply_update(&amended, key(1)), Applied::Amended);
    assert_eq!(rec.intent(), Intent::None);
    assert_eq!((rec.px(), rec.qty()), (Some(Ticks(101)), lots(12)));
    assert_eq!(rec.state(), OrdState::Open);
}

#[test]
fn an_unanswered_amend_or_cancel_is_left_to_the_unknown_ladder() {
    for (op, outcome) in [
        (OrderOp::Amend, SubmitOutcome::Unknown),
        (OrderOp::Cancel, SubmitOutcome::Unknown),
        (OrderOp::Amend, refused(RejectKind::NotFound)),
        (OrderOp::Cancel, refused(RejectKind::NotFound)),
    ] {
        let mut rec = open_record();
        rec.cancel_sent(RpcId(2), MonoNs(2));
        assert_eq!(
            rec.on_outcome(op, None, &outcome, MonoNs(4)),
            OutcomeApplied::AwaitingLadder
        );
        assert_eq!(
            rec.on_outcome(op, None, &outcome, MonoNs(6)),
            OutcomeApplied::AwaitingLadder
        );
        assert_eq!(rec.unknown_since(), Some(MonoNs(4)));
        assert_eq!(
            rec.state(),
            OrdState::Open,
            "never resent, never moved down"
        );
        assert!(matches!(rec.intent(), Intent::PendingCancel { .. }));
    }
}

#[test]
fn a_cancel_refused_as_already_terminal_waits_for_the_terminal_event() {
    let mut rec = open_record();
    rec.cancel_sent(RpcId(2), MonoNs(2));
    let outcome = refused(RejectKind::AlreadyTerminal(TerminalHint::Unspecified));
    assert_eq!(
        rec.on_outcome(OrderOp::Cancel, None, &outcome, MonoNs(3)),
        OutcomeApplied::Unchanged
    );
    assert_eq!(
        rec.intent(),
        Intent::PendingCancel {
            rpc: RpcId(2),
            since: MonoNs(2)
        }
    );

    assert_eq!(
        rec.apply_update(&update(None, canceled(), 0), key(1)),
        Applied::Advanced
    );
    assert!(rec.state().is_terminal());
    assert_eq!(rec.intent(), Intent::None);
}

#[test]
fn a_cancel_refused_otherwise_or_unsent_leaves_the_order_resting() {
    for outcome in [
        refused(RejectKind::Other),
        SubmitOutcome::NotSent(NotSentReason::Disconnected),
    ] {
        let mut rec = open_record();
        rec.cancel_sent(RpcId(2), MonoNs(2));
        assert_eq!(
            rec.on_outcome(OrderOp::Cancel, None, &outcome, MonoNs(3)),
            OutcomeApplied::IntentCleared
        );
        assert_eq!(rec.intent(), Intent::None);
        assert_eq!(rec.state(), OrdState::Open);
    }
    let mut rec = open_record();
    rec.cancel_sent(RpcId(2), MonoNs(2));
    assert_eq!(
        rec.on_outcome(OrderOp::Cancel, None, &accepted(), MonoNs(3)),
        OutcomeApplied::Unchanged,
        "an accepted cancel waits for the order's terminal event"
    );
}

#[test]
fn a_terminal_order_takes_no_outcome_and_no_intent() {
    let mut rec = open_record();
    rec.apply_update(&update(None, VenueOrderState::Filled, 10), key(1));
    let ended = rec.state();
    assert_eq!(ended, OrdState::Terminal(TerminalKind::Filled));
    for (op, outcome) in [
        (OrderOp::Place, accepted()),
        (OrderOp::Place, SubmitOutcome::Unknown),
        (
            OrderOp::Place,
            SubmitOutcome::NotSent(NotSentReason::Disconnected),
        ),
        (OrderOp::Cancel, refused(RejectKind::NotFound)),
    ] {
        assert_eq!(
            rec.on_outcome(op, None, &outcome, MonoNs(9)),
            OutcomeApplied::Unchanged
        );
        assert_eq!(rec.state(), ended);
    }
    assert!(!rec.amend_sent(Ticks(1), lots(1), RpcId(3), MonoNs(9)));
    assert!(!rec.cancel_sent(RpcId(3), MonoNs(9)));
    assert_eq!(rec.intent(), Intent::None);
    assert_eq!(rec.resting(), lots(0));
}
