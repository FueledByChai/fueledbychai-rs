//! The audit sink (decision 0005, I8, library form): every raw order event the registry
//! receives reaches the consumer's sink in receive order, before it is applied, the ones then
//! ignored as late, stale or for a superseded venue id included, and the verdict of each
//! follows it before the next event is received.

mod common;

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex};

use common::{cid, placement, update, vid};
use fbc_core::{CancelReason, CidMatch, Namespace, OrderUpdate, VenueOrderState};
use fbc_oms::{Applied, AuditEntry, AuditSink, OrdState, OrderKey, Registry, Routed};
use proptest::prelude::*;
use proptest::test_runner::RngSeed;

/// What a sink saw, owned so a test can compare it after the registry is done.
#[derive(Clone, Eq, PartialEq, Debug)]
enum Seen {
    Received(u64, OrderUpdate, OrderKey),
    Verdict(u64, Routed),
}

/// A sink that keeps every entry, shared with the test.
#[derive(Clone, Default)]
struct Recording(Arc<Mutex<Vec<Seen>>>);

impl Recording {
    fn seen(&self) -> Vec<Seen> {
        self.0.lock().unwrap().clone()
    }
}

impl AuditSink for Recording {
    fn record(&mut self, entry: AuditEntry<'_>) {
        let seen = match entry {
            AuditEntry::Received { seq, update, key } => Seen::Received(seq, update.clone(), key),
            AuditEntry::Verdict { seq, routed } => Seen::Verdict(seq, routed.clone()),
        };
        self.0.lock().unwrap().push(seen);
    }
}

fn at(venue: u64, ingest: u64) -> OrderKey {
    OrderKey {
        venue: Some(venue),
        ingest,
    }
}

/// `update` naming the venue id `v` too.
fn with_vid(mut u: OrderUpdate, v: &str) -> OrderUpdate {
    u.vid = Some(vid(v));
    u
}

/// A registry holding one order, under a recording sink.
fn audited() -> (Registry, Recording, fbc_core::ClientOrderId) {
    let mut registry = Registry::new();
    let a = registry.insert(placement(cid(), 100, 10)).unwrap().cid();
    let sink = Recording::default();
    assert!(registry.set_audit_sink(Box::new(sink.clone())).is_none());
    (registry, sink, a)
}

/// The sink's log a feed must produce: each event received, then its verdict, in order.
fn expected(feed: &[(OrderUpdate, OrderKey)], verdicts: &[Routed], first: u64) -> Vec<Seen> {
    feed.iter()
        .zip(verdicts)
        .zip(first..)
        .flat_map(|(((u, k), routed), seq)| {
            [
                Seen::Received(seq, u.clone(), *k),
                Seen::Verdict(seq, routed.clone()),
            ]
        })
        .collect()
}

#[test]
fn every_raw_order_event_reaches_the_sink_in_receive_order_each_followed_by_its_verdict() {
    let (mut registry, sink, a) = audited();
    let open = with_vid(update(Some(a), VenueOrderState::Open, 0), "v1");
    let amended = with_vid(
        update(
            Some(a),
            VenueOrderState::Amended {
                new_vid: Some(vid("v2")),
            },
            0,
        ),
        "v1",
    );
    let canceled = with_vid(
        update(
            Some(a),
            VenueOrderState::Canceled(CancelReason::Disconnect),
            0,
        ),
        "v2",
    );
    let mut foreign = update(None, VenueOrderState::Open, 0);
    foreign.cid = Some(CidMatch::Foreign(Namespace::new(9)));
    let mut not_canonical = update(None, VenueOrderState::Open, 0);
    not_canonical.cid = Some(CidMatch::Unparseable);
    let untracked = with_vid(update(None, VenueOrderState::Open, 0), "zz");
    let feed = vec![
        (open.clone(), at(5, 1)),
        // A duplicate: the same event delivered again under the same venue key.
        (open.clone(), at(5, 2)),
        // Stale: a non-terminal event under an older venue key.
        (open.clone(), at(3, 3)),
        (amended, at(6, 4)),
        // Names the venue id the amend replaced.
        (open.clone(), at(7, 5)),
        (canceled.clone(), at(8, 6)),
        // Late: after the order ended, and a duplicate of the terminal event.
        (
            with_vid(update(Some(a), VenueOrderState::Open, 0), "v2"),
            at(9, 7),
        ),
        (canceled, at(8, 8)),
        (foreign, at(1, 9)),
        (not_canonical, at(1, 10)),
        (untracked, at(1, 11)),
    ];
    let verdicts: Vec<Routed> = feed
        .iter()
        .map(|(u, k)| registry.apply_update(u, *k))
        .collect();
    assert_eq!(
        verdicts,
        vec![
            Routed::Ours(a, Applied::Advanced),
            Routed::Ours(a, Applied::Advanced),
            Routed::Ours(a, Applied::IgnoredStale),
            Routed::Ours(a, Applied::Amended),
            Routed::Ours(a, Applied::IgnoredSupersededVid),
            Routed::Ours(a, Applied::Advanced),
            Routed::Ours(a, Applied::IgnoredLate),
            Routed::Ours(a, Applied::IgnoredLate),
            Routed::Foreign(Namespace::new(9)),
            Routed::NotCanonical,
            Routed::Untracked,
        ],
        "the feed covers every verdict, ignored ones included"
    );
    assert_eq!(sink.seen(), expected(&feed, &verdicts, 0));
}

#[test]
fn each_cancel_reason_reaches_the_sink_as_the_venue_stated_it() {
    let (mut registry, sink, _) = audited();
    let reasons = [
        CancelReason::Requested,
        CancelReason::Disconnect,
        CancelReason::Unfilled,
        CancelReason::Venue,
    ];
    let mut feed = Vec::new();
    for (i, reason) in reasons.into_iter().enumerate() {
        let c = registry.insert(placement(cid(), 100, 10)).unwrap().cid();
        feed.push((
            update(Some(c), VenueOrderState::Canceled(reason), 0),
            at(i as u64, i as u64),
        ));
    }
    let verdicts: Vec<Routed> = feed
        .iter()
        .map(|(u, k)| registry.apply_update(u, *k))
        .collect();
    assert_eq!(sink.seen(), expected(&feed, &verdicts, 0));
}

/// A sink that refuses to return from the event it is told to stop at.
struct Stops(OrderKey);

impl AuditSink for Stops {
    fn record(&mut self, entry: AuditEntry<'_>) {
        if let AuditEntry::Received { key, .. } = entry {
            assert_ne!(key, self.0, "the sink stops here");
        }
    }
}

#[test]
fn the_sink_receives_an_event_before_any_of_it_is_applied() {
    let mut registry = Registry::new();
    let a = registry.insert(placement(cid(), 100, 10)).unwrap().cid();
    let stop = at(2, 2);
    registry.set_audit_sink(Box::new(Stops(stop)));
    let open = with_vid(update(Some(a), VenueOrderState::Open, 0), "v1");
    assert_eq!(
        registry.apply_update(&open, at(1, 1)),
        Routed::Ours(a, Applied::Advanced)
    );
    // The sink stops at the cancel's receipt: nothing of the cancel was applied.
    let canceled = with_vid(
        update(
            Some(a),
            VenueOrderState::Canceled(CancelReason::Requested),
            0,
        ),
        "v1",
    );
    let stopped = catch_unwind(AssertUnwindSafe(|| registry.apply_update(&canceled, stop)));
    assert!(stopped.is_err());
    let rec = registry.get(a).unwrap();
    assert_eq!(rec.state(), OrdState::Open);
    assert_eq!(rec.last_key(), Some(at(1, 1)));
    // Nor did a foreign order's event reach the foreign view before the sink had it.
    let mut foreign = with_vid(update(None, VenueOrderState::Open, 0), "f1");
    foreign.cid = Some(CidMatch::Foreign(Namespace::new(9)));
    let stopped = catch_unwind(AssertUnwindSafe(|| registry.apply_update(&foreign, stop)));
    assert!(stopped.is_err());
    assert!(!registry.foreign_in_view(foreign.inst));
}

#[test]
fn the_sequence_counts_every_event_received_so_a_sink_set_later_sees_what_it_missed() {
    let mut registry = Registry::new();
    let a = registry.insert(placement(cid(), 100, 10)).unwrap().cid();
    let open = update(Some(a), VenueOrderState::Open, 0);
    // Two events before any sink.
    registry.apply_update(&open, at(1, 1));
    registry.apply_update(&open, at(2, 2));
    let first = Recording::default();
    assert!(registry.set_audit_sink(Box::new(first.clone())).is_none());
    let third = registry.apply_update(&open, at(3, 3));
    let second = Recording::default();
    assert!(
        registry.set_audit_sink(Box::new(second.clone())).is_some(),
        "setting a sink hands back the one it replaces"
    );
    let fourth = registry.apply_update(&open, at(4, 4));
    assert_eq!(
        first.seen(),
        expected(&[(open.clone(), at(3, 3))], &[third], 2)
    );
    assert_eq!(second.seen(), expected(&[(open, at(4, 4))], &[fourth], 3));
}

#[test]
fn a_registry_shows_whether_it_has_a_sink_and_applies_events_the_same_either_way() {
    let mut plain = Registry::new();
    let (mut audited, _, a) = audited();
    let b = plain.insert(placement(a, 100, 10)).unwrap().cid();
    assert_eq!(a, b);
    assert!(format!("{plain:?}").contains("sink: None"));
    assert!(format!("{audited:?}").contains("sink: Some"));
    let feed = [
        (update(Some(a), VenueOrderState::Open, 0), at(2, 1)),
        (update(Some(a), VenueOrderState::Open, 0), at(1, 2)),
        (update(Some(a), VenueOrderState::Filled, 10), at(3, 3)),
    ];
    for (u, k) in &feed {
        assert_eq!(plain.apply_update(u, *k), audited.apply_update(u, *k));
    }
    let (p, q) = (plain.get(a).unwrap(), audited.get(a).unwrap());
    assert_eq!(
        (p.state(), p.cum_venue(), p.last_key()),
        (q.state(), q.cum_venue(), q.last_key())
    );
}

#[test]
fn a_registry_with_a_sink_can_still_move_and_be_shared_between_threads() {
    fn send_sync<T: Send + Sync>() {}
    send_sync::<Registry>();
    send_sync::<Box<dyn AuditSink>>();
}

/// Fixed cases and seed, so the check and its coverage are the same on every run and machine
/// (decision 0037); a failure names its minimal case.
fn config(cases: u32, seed: u64) -> ProptestConfig {
    ProptestConfig {
        cases,
        rng_seed: RngSeed::Fixed(seed),
        failure_persistence: None,
        ..ProptestConfig::default()
    }
}

/// One generated event: which order or stranger it names, its state, and its venue key.
fn event() -> impl Strategy<Value = (u8, u8, u64)> {
    (0u8..5, 0u8..4, 0u64..6)
}

proptest! {
    #![proptest_config(config(256, 0xA0D1_7008))]

    /// Whatever the events (duplicates, stale and late ones, superseded venue ids, others'
    /// orders), the sink holds each one received, in order and numbered, followed by the very
    /// verdict the registry returned for it.
    #[test]
    fn the_sink_mirrors_every_event_and_its_verdict(events in prop::collection::vec(event(), 0..40)) {
        let mut registry = Registry::new();
        let ours = [
            registry.insert(placement(cid(), 100, 10)).unwrap().cid(),
            registry.insert(placement(cid(), 101, 10)).unwrap().cid(),
        ];
        let sink = Recording::default();
        registry.set_audit_sink(Box::new(sink.clone()));
        let mut feed = Vec::new();
        let mut verdicts = Vec::new();
        for (ingest, (who, state, venue)) in events.into_iter().enumerate() {
            let state = match state {
                0 => VenueOrderState::Open,
                1 => VenueOrderState::Amended { new_vid: Some(vid(&format!("n{venue}"))) },
                2 => VenueOrderState::Canceled(CancelReason::Requested),
                _ => VenueOrderState::Filled,
            };
            let mut u = match who {
                0 | 1 => update(Some(ours[who as usize]), state, 0),
                2 => update(None, state, 0),
                3 => {
                    let mut u = update(None, state, 0);
                    u.cid = Some(CidMatch::Foreign(Namespace::new(9)));
                    u
                }
                _ => {
                    let mut u = update(None, state, 0);
                    u.cid = Some(CidMatch::Unparseable);
                    u
                }
            };
            u.vid = Some(vid(&format!("n{}", venue % 3)));
            let key = at(venue, ingest as u64);
            verdicts.push(registry.apply_update(&u, key));
            feed.push((u, key));
        }
        prop_assert_eq!(sink.seen(), expected(&feed, &verdicts, 0));
    }
}
