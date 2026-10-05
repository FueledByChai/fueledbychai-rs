//! Order updates on one record: ranks, ordering keys, venue truth for price and total, amends
//! that issue new venue ids, and superseded ids.

mod common;

use common::{canceled, cid, lots, placement, rejected, update, vid};
use fbc_core::{
    Channel, InstrumentId, MonoNs, NewOrder, OrderKind, RejectKind, RpcId, Side, Ticks, Tif,
    VenueOrderState,
};
use fbc_oms::{Applied, Intent, OrdState, OrderKey, OrderRecord, TerminalKind};

fn at(venue: Option<u64>, ingest: u64) -> OrderKey {
    OrderKey { venue, ingest }
}

#[test]
fn ranks_order_the_states_and_only_terminal_is_terminal() {
    let ranks = [
        (OrdState::PendingNew, 0),
        (OrdState::Unknown, 0),
        (OrdState::Open, 1),
        (OrdState::PartiallyFilled, 2),
        (OrdState::Terminal(TerminalKind::Expired), 3),
    ];
    for (state, rank) in ranks {
        assert_eq!(state.rank(), rank);
        assert_eq!(state.is_terminal(), rank == 3);
    }
}

#[test]
fn keys_compare_by_venue_key_then_ingest_or_by_ingest_alone() {
    assert!(at(Some(1), 9).is_older_than(at(Some(2), 0)));
    assert!(
        at(Some(2), 3).is_older_than(at(Some(2), 4)),
        "a tie goes to ingest"
    );
    assert!(
        !at(Some(2), 4).is_older_than(at(Some(2), 4)),
        "equal is not older"
    );
    assert!(!at(Some(3), 0).is_older_than(at(Some(2), 9)));
    assert!(at(None, 1).is_older_than(at(Some(0), 2)));
    assert!(!at(Some(9), 2).is_older_than(at(None, 1)));
}

#[test]
fn a_new_record_is_pending_at_its_placement() {
    let order = placement(cid(), 100, 10);
    let rec = OrderRecord::new(order.clone());
    assert_eq!(rec.cid(), order.cid);
    assert_eq!(rec.placed(), &order);
    assert_eq!(rec.state(), OrdState::PendingNew);
    assert_eq!((rec.px(), rec.qty()), (Some(Ticks(100)), lots(10)));
    assert_eq!((rec.cum_venue(), rec.filled()), (lots(0), lots(0)));
    assert_eq!(rec.resting(), lots(10));
    assert_eq!(rec.vid(), None);
    assert_eq!(rec.last_key(), None);
    assert_eq!(rec.intent(), Intent::None);
    assert_eq!(rec.superseded_vids().count(), 0);

    let market = OrderRecord::new(NewOrder {
        kind: OrderKind::Market,
        inst: InstrumentId::new(2),
        side: Side::Sell,
        tif: Tif::Ioc,
        channel: Channel::Public,
        ..order
    });
    assert_eq!(market.px(), None);
}

#[test]
fn fills_on_open_updates_move_the_order_to_partially_filled_and_never_back() {
    let mut rec = OrderRecord::new(placement(cid(), 100, 10));
    assert_eq!(
        rec.apply_update(&update(None, VenueOrderState::Open, 0), at(None, 0)),
        Applied::Advanced
    );
    assert_eq!(rec.state(), OrdState::Open);
    rec.apply_update(&update(None, VenueOrderState::Open, 3), at(None, 1));
    assert_eq!(rec.state(), OrdState::PartiallyFilled);
    assert_eq!(rec.resting(), lots(7));
    // A later update reporting less cumulative fill keeps the most the venue reported.
    rec.apply_update(&update(None, VenueOrderState::Open, 0), at(None, 2));
    assert_eq!(rec.state(), OrdState::PartiallyFilled);
    assert_eq!(rec.cum_venue(), lots(3));
    assert_eq!(rec.last_key(), Some(at(None, 2)));
}

#[test]
fn a_stale_update_is_ignored_and_a_terminal_one_applies_whatever_its_key() {
    let mut rec = OrderRecord::new(placement(cid(), 100, 10));
    rec.apply_update(&update(None, VenueOrderState::Open, 2), at(Some(5), 0));
    assert_eq!(
        rec.apply_update(&update(None, VenueOrderState::Open, 4), at(Some(4), 1)),
        Applied::IgnoredStale
    );
    assert_eq!(rec.cum_venue(), lots(2));
    assert_eq!(
        rec.apply_update(
            &update(None, VenueOrderState::Amended { new_vid: None }, 2),
            at(Some(4), 2)
        ),
        Applied::IgnoredStale
    );
    // Equal venue keys: the later arrival applies.
    assert_eq!(
        rec.apply_update(&update(None, VenueOrderState::Open, 3), at(Some(5), 3)),
        Applied::Advanced
    );
    let ended = update(None, rejected(RejectKind::Margin), 3);
    assert_eq!(rec.apply_update(&ended, at(Some(1), 4)), Applied::Advanced);
    assert_eq!(
        rec.state(),
        OrdState::Terminal(TerminalKind::Rejected(RejectKind::Margin))
    );
    assert_eq!(
        rec.apply_update(&update(None, VenueOrderState::Open, 9), at(Some(9), 5)),
        Applied::IgnoredLate
    );
    assert_eq!(rec.cum_venue(), lots(3));
}

#[test]
fn the_venue_states_price_and_total_and_an_open_update_matching_an_amend_clears_it() {
    // Under venue ordering keys: an update lowering the total while an amend is in flight
    // applies only under a key later than the last applied.
    let mut rec = OrderRecord::new(placement(cid(), 100, 10));
    rec.apply_update(&update(None, VenueOrderState::Open, 0), at(Some(1), 0));
    rec.amend_sent(Ticks(102), lots(8), RpcId(1), MonoNs(1));

    let mut other = update(None, VenueOrderState::Open, 0);
    other.px = Some(Ticks(101));
    rec.apply_update(&other, at(Some(2), 1));
    assert_eq!((rec.px(), rec.qty()), (Some(Ticks(101)), lots(10)));
    assert!(
        matches!(rec.intent(), Intent::PendingAmend { .. }),
        "not the amend's values"
    );

    let mut px_only = update(None, VenueOrderState::Open, 0);
    px_only.px = Some(Ticks(102));
    rec.apply_update(&px_only, at(Some(3), 2));
    assert!(
        matches!(rec.intent(), Intent::PendingAmend { .. }),
        "the total still differs"
    );

    let mut matching = update(None, VenueOrderState::Open, 0);
    (matching.px, matching.qty) = (Some(Ticks(102)), Some(lots(8)));
    rec.apply_update(&matching, at(Some(4), 3));
    assert_eq!((rec.px(), rec.qty()), (Some(Ticks(102)), lots(8)));
    assert_eq!(rec.intent(), Intent::None);

    // An amended update with nothing pending leaves the intent alone.
    rec.cancel_sent(RpcId(2), MonoNs(2));
    let amended = update(None, VenueOrderState::Amended { new_vid: None }, 0);
    assert_eq!(rec.apply_update(&amended, at(Some(5), 4)), Applied::Amended);
    assert!(matches!(rec.intent(), Intent::PendingCancel { .. }));
}

#[test]
fn an_amend_with_a_new_venue_id_supersedes_the_old_one() {
    let mut rec = OrderRecord::new(placement(cid(), 100, 10));
    let mut open = update(None, VenueOrderState::Open, 0);
    open.vid = Some(vid("a"));
    rec.apply_update(&open, at(None, 0));
    assert_eq!(rec.vid(), Some(&vid("a")));

    let mut amended = update(
        None,
        VenueOrderState::Amended {
            new_vid: Some(vid("b")),
        },
        0,
    );
    amended.vid = Some(vid("a"));
    (amended.px, amended.qty) = (Some(Ticks(99)), Some(lots(6)));
    assert_eq!(rec.apply_update(&amended, at(None, 1)), Applied::Amended);
    assert_eq!(rec.vid(), Some(&vid("b")));
    assert!(rec.is_superseded(&vid("a")));
    assert_eq!((rec.px(), rec.qty()), (Some(Ticks(99)), lots(6)));

    // Anything naming the old id is ignored, the duplicate amend and a terminal update alike.
    assert_eq!(
        rec.apply_update(&amended, at(None, 2)),
        Applied::IgnoredSupersededVid
    );
    let mut old_cancel = update(None, canceled(), 0);
    old_cancel.vid = Some(vid("a"));
    assert_eq!(
        rec.apply_update(&old_cancel, at(None, 3)),
        Applied::IgnoredSupersededVid
    );
    assert_eq!(rec.state(), OrdState::Open);

    // An amended update naming no old id replaces the current one.
    let unnamed = update(
        None,
        VenueOrderState::Amended {
            new_vid: Some(vid("c")),
        },
        0,
    );
    rec.apply_update(&unnamed, at(None, 4));
    assert_eq!(rec.vid(), Some(&vid("c")));
    let superseded: Vec<_> = rec.superseded_vids().cloned().collect();
    assert_eq!(superseded, vec![vid("a"), vid("b")]);
}

#[test]
fn a_new_venue_id_alone_does_not_tie_a_confirmation_to_the_amend_in_flight() {
    // Codex r4182826886: A1's replacement notice arrives late, after an update stated A1's
    // values and A2 was sent.
    let mut rec = OrderRecord::new(placement(cid(), 100, 5));
    let mut open = update(None, VenueOrderState::Open, 0);
    open.vid = Some(vid("v1"));
    rec.apply_update(&open, at(None, 0));
    rec.amend_sent(Ticks(101), lots(6), RpcId(1), MonoNs(1));
    let mut a1 = update(None, VenueOrderState::Open, 0);
    (a1.px, a1.qty) = (Some(Ticks(101)), Some(lots(6)));
    rec.apply_update(&a1, at(None, 1));
    assert_eq!(rec.intent(), Intent::None);
    rec.amend_sent(Ticks(102), lots(9), RpcId(2), MonoNs(2));
    let mut late = update(
        None,
        VenueOrderState::Amended {
            new_vid: Some(vid("v2")),
        },
        0,
    );
    late.vid = Some(vid("v1"));
    assert_eq!(rec.apply_update(&late, at(None, 2)), Applied::Amended);
    // The replacement is recorded; A2 stays in flight, counted, and its refusal leaves A1's.
    assert_eq!(rec.vid(), Some(&vid("v2")));
    assert!(rec.is_superseded(&vid("v1")));
    assert!(matches!(rec.intent(), Intent::PendingAmend { .. }));
    assert_eq!((rec.px(), rec.qty()), (Some(Ticks(101)), lots(6)));
    assert_eq!(rec.resting(), lots(9));
    let refusal = fbc_core::SubmitOutcome::NotSent(fbc_core::NotSentReason::Backpressure);
    rec.on_outcome(fbc_oms::OrderOp::Amend(RpcId(2)), None, &refusal, MonoNs(3));
    assert_eq!((rec.px(), rec.qty()), (Some(Ticks(101)), lots(6)));
    assert_eq!(rec.resting(), lots(6));
}

#[test]
fn replacements_arriving_out_of_order_still_end_at_the_newest_id() {
    let mut rec = OrderRecord::new(placement(cid(), 100, 10));
    // The newer replacement (b -> c) arrives first: the order is known as c.
    let mut second = update(
        None,
        VenueOrderState::Amended {
            new_vid: Some(vid("c")),
        },
        0,
    );
    second.vid = Some(vid("b"));
    rec.apply_update(&second, at(None, 0));
    assert_eq!(rec.vid(), Some(&vid("c")));
    // The older one (a -> b) arrives late: c stays current, a and b are superseded.
    let mut first = update(
        None,
        VenueOrderState::Amended {
            new_vid: Some(vid("b")),
        },
        0,
    );
    first.vid = Some(vid("a"));
    rec.apply_update(&first, at(None, 1));
    assert_eq!(rec.vid(), Some(&vid("c")));
    assert!(rec.is_superseded(&vid("a")) && rec.is_superseded(&vid("b")));
    assert!(!rec.is_superseded(&vid("c")));

    // An update for c that arrived before any amend kept c current and ends the order.
    let mut filled = update(None, VenueOrderState::Filled, 10);
    filled.vid = Some(vid("c"));
    assert_eq!(rec.apply_update(&filled, at(None, 2)), Applied::Advanced);
    assert_eq!(rec.state(), OrdState::Terminal(TerminalKind::Filled));
}

#[test]
fn a_known_current_id_is_not_moved_back_by_a_stale_amend() {
    let mut rec = OrderRecord::new(placement(cid(), 100, 10));
    let mut newest = update(None, VenueOrderState::Open, 0);
    newest.vid = Some(vid("c"));
    rec.apply_update(&newest, at(None, 0));
    let mut stale = update(
        None,
        VenueOrderState::Amended {
            new_vid: Some(vid("b")),
        },
        0,
    );
    stale.vid = Some(vid("a"));
    rec.apply_update(&stale, at(None, 1));
    assert_eq!(rec.vid(), Some(&vid("c")), "c never named a replacement");
    assert!(rec.is_superseded(&vid("a")));
}

#[test]
fn a_replacement_that_would_close_a_cycle_is_not_recorded() {
    let mut rec = OrderRecord::new(placement(cid(), 100, 10));
    let mut forward = update(
        None,
        VenueOrderState::Amended {
            new_vid: Some(vid("y")),
        },
        0,
    );
    forward.vid = Some(vid("x"));
    rec.apply_update(&forward, at(None, 0));
    // y -> x would make x replace itself through y.
    let mut back = update(
        None,
        VenueOrderState::Amended {
            new_vid: Some(vid("x")),
        },
        0,
    );
    back.vid = Some(vid("y"));
    rec.apply_update(&back, at(None, 1));
    assert_eq!(rec.vid(), Some(&vid("y")));
    assert!(!rec.is_superseded(&vid("y")));
    // Naming its own id as the new one records nothing either.
    let mut same = update(
        None,
        VenueOrderState::Amended {
            new_vid: Some(vid("y")),
        },
        0,
    );
    same.vid = Some(vid("y"));
    rec.apply_update(&same, at(None, 2));
    assert_eq!(rec.superseded_vids().count(), 1);
}

#[test]
fn a_terminal_update_names_the_orders_last_venue_id() {
    let mut rec = OrderRecord::new(placement(cid(), 100, 10));
    let mut open = update(None, VenueOrderState::Open, 0);
    open.vid = Some(vid("a"));
    rec.apply_update(&open, at(None, 0));
    let mut expired = update(None, VenueOrderState::Expired, 0);
    expired.vid = Some(vid("b"));
    assert_eq!(rec.apply_update(&expired, at(None, 1)), Applied::Advanced);
    assert_eq!(rec.vid(), Some(&vid("b")));
    assert_eq!(rec.state(), OrdState::Terminal(TerminalKind::Expired));
}

#[test]
fn an_open_update_resolves_an_unknown_order() {
    let mut rec = OrderRecord::new(placement(cid(), 100, 10));
    rec.on_outcome(
        fbc_oms::OrderOp::Place,
        None,
        &fbc_core::SubmitOutcome::Unknown,
        MonoNs(1),
    );
    let amended = update(None, VenueOrderState::Amended { new_vid: None }, 0);
    rec.apply_update(&amended, at(None, 0));
    assert_eq!(
        rec.state(),
        OrdState::Unknown,
        "an amended update moves no state"
    );
    rec.apply_update(&update(None, VenueOrderState::Open, 1), at(None, 1));
    assert_eq!(rec.state(), OrdState::PartiallyFilled);
    assert_eq!(rec.unknown_since(), None);
}
