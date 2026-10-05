//! The registry by client id: one order per client id, updates routed by our client id or by
//! any venue id the order had, and outcomes refused for an order it does not hold.

mod common;

use common::{cid, placement, update, vid};
use fbc_core::{AckLevel, CidMatch, ItemRef, MonoNs, Namespace, SubmitOutcome, VenueOrderState};
use fbc_oms::{Applied, OmsError, OrdState, OrderKey, OrderOp, OutcomeApplied, Registry, Routed};

fn key(ingest: u64) -> OrderKey {
    OrderKey {
        venue: None,
        ingest,
    }
}

fn accepted() -> SubmitOutcome {
    SubmitOutcome::Accepted {
        ack: AckLevel::Final,
    }
}

#[test]
fn one_order_per_client_id() {
    let mut registry = Registry::new();
    assert!(registry.is_empty());
    let order = placement(cid(), 100, 10);
    let cid = order.cid;
    assert_eq!(
        registry.insert(order.clone()).unwrap().state(),
        OrdState::PendingNew
    );
    assert_eq!(
        registry.insert(order).err(),
        Some(OmsError::DuplicateCid(cid))
    );
    assert_eq!(registry.len(), 1);
    assert!(!registry.is_empty());
    assert_eq!(registry.get(cid).unwrap().cid(), cid);
}

#[test]
fn updates_route_by_our_client_id_or_by_any_venue_id_the_order_had() {
    let mut registry = Registry::new();
    let a = registry.insert(placement(cid(), 100, 10)).unwrap().cid();
    let item = ItemRef {
        idx: 0,
        cid: None,
        vid: Some(vid("a1")),
    };
    assert_eq!(
        registry.on_outcome(a, OrderOp::Place, &item, &accepted(), MonoNs(1)),
        Ok(OutcomeApplied::Opened)
    );
    assert_eq!(registry.cid_of(&vid("a1")), Some(a));

    // No client id on the event: the venue id names the order.
    let mut by_vid = update(
        None,
        VenueOrderState::Amended {
            new_vid: Some(vid("a2")),
        },
        0,
    );
    by_vid.vid = Some(vid("a1"));
    assert_eq!(
        registry.apply_update(&by_vid, key(0)),
        Routed::Ours(a, Applied::Amended)
    );
    assert_eq!(registry.cid_of(&vid("a2")), Some(a));
    // The superseded id still routes to the order, which ignores it.
    assert_eq!(
        registry.apply_update(&by_vid, key(1)),
        Routed::Ours(a, Applied::IgnoredSupersededVid)
    );
    // Our client id names it directly.
    assert_eq!(
        registry.apply_update(&update(Some(a), VenueOrderState::Open, 2), key(2)),
        Routed::Ours(a, Applied::Advanced)
    );
    assert_eq!(registry.get(a).unwrap().state(), OrdState::PartiallyFilled);

    // One of our client ids the registry does not hold, on a venue id it does: the venue id.
    let stranger = cid();
    let mut mixed = update(Some(stranger), VenueOrderState::Open, 2);
    mixed.vid = Some(vid("a2"));
    assert_eq!(
        registry.apply_update(&mixed, key(3)),
        Routed::Ours(a, Applied::Advanced)
    );
}

#[test]
fn updates_naming_none_of_our_orders_go_nowhere() {
    let mut registry = Registry::new();
    let a = registry.insert(placement(cid(), 100, 10)).unwrap().cid();

    let mut foreign = update(None, VenueOrderState::Open, 0);
    foreign.cid = Some(CidMatch::Foreign(Namespace::new(9)));
    assert_eq!(
        registry.apply_update(&foreign, key(0)),
        Routed::Foreign(Namespace::new(9))
    );

    let mut other_system = update(None, VenueOrderState::Open, 0);
    other_system.cid = Some(CidMatch::Unparseable);
    assert_eq!(
        registry.apply_update(&other_system, key(1)),
        Routed::NotCanonical
    );

    assert_eq!(
        registry.apply_update(&update(Some(cid()), VenueOrderState::Open, 0), key(2)),
        Routed::Untracked
    );
    let mut unknown_vid = update(None, VenueOrderState::Filled, 0);
    unknown_vid.vid = Some(vid("nobody"));
    assert_eq!(
        registry.apply_update(&unknown_vid, key(3)),
        Routed::Untracked
    );
    assert_eq!(registry.get(a).unwrap().state(), OrdState::PendingNew);
}

#[test]
fn an_outcome_for_an_order_not_held_or_naming_another_is_refused() {
    let mut registry = Registry::new();
    let a = registry.insert(placement(cid(), 100, 10)).unwrap().cid();
    let missing = cid();
    let item = ItemRef {
        idx: 0,
        cid: None,
        vid: None,
    };
    let unknown = registry.on_outcome(missing, OrderOp::Place, &item, &accepted(), MonoNs(1));
    assert_eq!(unknown, Err(OmsError::UnknownCid(missing)));

    let crossed = ItemRef {
        idx: 1,
        cid: Some(missing),
        vid: None,
    };
    let refused = registry.on_outcome(a, OrderOp::Place, &crossed, &accepted(), MonoNs(1));
    assert_eq!(
        refused,
        Err(OmsError::ItemNamesAnother {
            cid: a,
            item: missing
        })
    );
    assert_eq!(registry.get(a).unwrap().state(), OrdState::PendingNew);

    let named = ItemRef {
        idx: 1,
        cid: Some(a),
        vid: None,
    };
    assert_eq!(
        registry.on_outcome(a, OrderOp::Place, &named, &accepted(), MonoNs(1)),
        Ok(OutcomeApplied::Opened)
    );
}

#[test]
fn errors_say_what_was_refused() {
    let (a, b) = (cid(), cid());
    let messages = [
        OmsError::DuplicateCid(a).to_string(),
        OmsError::UnknownCid(a).to_string(),
        OmsError::ItemNamesAnother { cid: a, item: b }.to_string(),
    ];
    assert!(messages[0].starts_with("an order is already registered under client id"));
    assert!(messages[1].starts_with("no order is registered under"));
    assert!(messages[2].contains("names another order"));
    let source: &dyn std::error::Error = &OmsError::UnknownCid(a);
    assert!(source.source().is_none());
}
