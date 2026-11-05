//! The shard's journal as the runtime records into it (decision 0006, design §4.8, §5.4).
//!
//! A [`Journal`] is a handle on the consumer's [`JournalSink`], shared by the sessions of one
//! shard thread. A record is offered under the traffic class of what it records and the session
//! goes on whatever the sink answers: the sink never blocks, and a record it drops is counted
//! there, so a Safety frame whose records found no room in the reserve is still written.
//!
//! **The soft limit (decision 0139).** The consumer gives the shard's journal the
//! [`CancelOnlyTrip`] of each registry whose order entry goes out on the shard
//! ([`Journal::cancel_only_at_soft_limit`]). The journal reaches its soft limit when the sink
//! drops a record the runtime offered it, of either class: a sink drops a Normal record only at
//! its soft limit and a Safety record only past it (0006). Each time it does, from below, the
//! journal trips every registry it was given, moving each of their markets in Quoting to
//! Cancel-only (a market in Exit, which builds only reducing orders, stays in Exit), and offers
//! [`Marker::CancelOnly`] under Safety right after the dropped record, so the sink keeps it from
//! the reserve. It is back under its soft limit once the sink keeps a Normal record the runtime
//! offers, or, for a sink that names its gaps ([`JournalSink::open_gap`], as the journal's
//! queue does), once the gap it reached it in has closed, whichever record closed it; only then
//! can it reach it again. While it stays at its soft limit nothing moves again, so the owner's
//! Start, Flatten or Wind-down, the only calls that leave Cancel-only, stand. A record the
//! consumer offers the sink itself (decision 0101) is not seen; the runtime's next record the
//! sink drops moves the shard. A journal given no registry moves nothing and journals no
//! marker.

use std::cell::{Cell, RefCell};
use std::fmt;
use std::rc::Rc;

use fbc_core::{TrafficClass, WallNs};
use fbc_journal::{JournalSink, Marker, Record, RecordRef, Recorded};
use fbc_oms::CancelOnlyTrip;

/// The journal the sessions of one shard thread record into. Cloning it shares the sink and
/// the registries it moves at its soft limit.
#[derive(Clone)]
pub struct Journal {
    sink: Rc<RefCell<dyn JournalSink>>,
    limit: Rc<SoftLimit>,
}

/// What the journal does at its soft limit (decision 0139).
#[derive(Default)]
struct SoftLimit {
    /// The registries it moves to Cancel-only, by their trips.
    trips: RefCell<Vec<CancelOnlyTrip>>,
    /// While at the soft limit, how it reached it.
    reached: Cell<Option<Reached>>,
}

/// How the journal reached its soft limit.
#[derive(Copy, Clone)]
struct Reached {
    /// The wall time of the record whose drop reached it.
    at: WallNs,
    /// The gap the sink named open after that drop ([`JournalSink::open_gap`]), `None` for a
    /// sink that names none.
    gap: Option<u64>,
}

impl SoftLimit {
    /// When the journal reached the soft limit it is still at, the sink naming `gap` open now:
    /// until the sink keeps a Normal record the runtime offers, and, for a sink that named the
    /// gap it reached it in, while that gap is still open.
    fn holds(&self, gap: Option<u64>) -> Option<WallNs> {
        let reached = self.reached.get()?;
        match reached.gap {
            Some(open) if gap != Some(open) => None,
            _ => Some(reached.at),
        }
    }
}

impl fmt::Debug for Journal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Journal")
    }
}

impl Journal {
    /// A journal into `sink`, which the consumer keeps a handle on to read its drop counts. A
    /// session borrows it only for the length of one record and never across an await, so the
    /// consumer may borrow it between a session's steps on the same thread.
    pub fn new<S: JournalSink + 'static>(sink: Rc<RefCell<S>>) -> Journal {
        Journal {
            sink,
            limit: Rc::default(),
        }
    }

    /// Gives the journal `trip`, the [`CancelOnlyTrip`] of a registry whose order entry goes out
    /// on this shard (fbc-oms's `Registry::cancel_only_trip`): each time the journal reaches its
    /// soft limit it trips it, moving every market of the registry in Quoting to Cancel-only and
    /// leaving Exit as it is, and journals the move (decision 0139). The consumer gives it
    /// before arming any market of the registry. Given while the journal is at its soft limit,
    /// it is tripped at once and the move journaled, filed under the time the journal reached
    /// it; a sink borrowed now, as a record call a panic unwinds out of may leave it, is taken
    /// to be where it was.
    pub fn cancel_only_at_soft_limit(&self, trip: CancelOnlyTrip) {
        let at = match self.sink.try_borrow() {
            Ok(sink) => self.limit.holds(sink.open_gap()),
            Err(_) => self.limit.reached.get().map(|reached| reached.at),
        };
        if let Some(at) = at {
            trip.trip();
            self.journal_move(at);
        }
        self.limit.trips.borrow_mut().push(trip);
    }

    /// Offers `record` under `class`, filed under the UTC day of `now`; what it records goes
    /// ahead whether the sink kept or dropped (and counted) it.
    pub(crate) fn record(&self, class: TrafficClass, now: WallNs, record: &Record) {
        let mut sink = self.sink.borrow_mut();
        let recorded = sink.record(class, now, record);
        let gap = sink.open_gap();
        drop(sink);
        self.answered(class, now, recorded, gap);
    }

    /// Offers `record` under `class` if the sink is not borrowed now, as it may be when a panic
    /// unwinds out of a record call: a best effort that never panics on the borrow.
    pub(crate) fn try_record(&self, class: TrafficClass, now: WallNs, record: &Record) {
        let (recorded, gap) = match self.sink.try_borrow_mut() {
            Ok(mut sink) => (sink.record(class, now, record), sink.open_gap()),
            Err(_) => return,
        };
        self.answered(class, now, recorded, gap);
    }

    /// Offers `record`, whose large contents it borrows, so a sink with no room for it
    /// refuses it before they are copied ([`JournalSink::record_ref`]).
    pub(crate) fn record_ref(&self, class: TrafficClass, now: WallNs, record: RecordRef<'_>) {
        let mut sink = self.sink.borrow_mut();
        let recorded = sink.record_ref(class, now, record);
        let gap = sink.open_gap();
        drop(sink);
        self.answered(class, now, recorded, gap);
    }

    /// What the sink answered a record of `class` offered at `now`, naming `gap` open after it:
    /// a drop from below the soft limit, or into a gap other than the one the journal reached
    /// it in, reaches it and moves the shard, and a Normal record kept says it is back under it.
    fn answered(&self, class: TrafficClass, now: WallNs, recorded: Recorded, gap: Option<u64>) {
        match recorded {
            Recorded::DroppedCounted if self.limit.holds(gap).is_none() => {
                self.limit.reached.set(Some(Reached { at: now, gap }));
                let trips = self.limit.trips.borrow();
                if trips.is_empty() {
                    return;
                }
                for trip in trips.iter() {
                    trip.trip();
                }
                drop(trips);
                self.journal_move(now);
            }
            Recorded::Ok if class == TrafficClass::Normal => self.limit.reached.set(None),
            Recorded::Ok | Recorded::DroppedCounted => {}
        }
    }

    /// Offers the move's marker under Safety, so the sink keeps it from the reserve; a sink
    /// with no room left there drops and counts it, and the move stands. Never panics on the
    /// borrow, which no record call holds once its sink answered.
    fn journal_move(&self, now: WallNs) {
        if let Ok(mut sink) = self.sink.try_borrow_mut() {
            let _ = sink.record(
                TrafficClass::Safety,
                now,
                &Record::Marker(Marker::CancelOnly),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fbc_oms::Registry;

    /// A sink that keeps what it is offered and refuses every Normal record.
    #[derive(Default)]
    struct Picky(Vec<(TrafficClass, Record)>);

    impl JournalSink for Picky {
        fn record(&mut self, class: TrafficClass, _: WallNs, record: &Record) -> Recorded {
            self.0.push((class, record.clone()));
            match class {
                TrafficClass::Normal => Recorded::DroppedCounted,
                TrafficClass::Safety => Recorded::Ok,
            }
        }

        fn omit(&mut self, _: TrafficClass, _: WallNs) -> Recorded {
            Recorded::DroppedCounted
        }
    }

    #[test]
    fn a_journal_offers_each_record_under_its_class_and_goes_on_when_it_is_dropped() {
        let sink = Rc::new(RefCell::new(Picky::default()));
        let journal = Journal::new(sink.clone());
        let shared = journal.clone();
        let marker = Record::Marker(Marker::Recovered);
        journal.record(TrafficClass::Normal, WallNs(1), &marker);
        shared.record(TrafficClass::Safety, WallNs(2), &marker);
        let classes: Vec<_> = sink.borrow().0.iter().map(|(c, _)| *c).collect();
        // Given no registry, the drop journals no move.
        assert_eq!(classes, [TrafficClass::Normal, TrafficClass::Safety]);
        assert_eq!(
            sink.borrow_mut().omit(TrafficClass::Normal, WallNs(3)),
            Recorded::DroppedCounted
        );
        assert_eq!(format!("{journal:?}"), "Journal");
    }

    /// A sink whose room the test sets: with `full`, it drops every record of the class named
    /// (every record, for Safety); it names the gap the test sets open, or none.
    #[derive(Default)]
    struct Room {
        offered: Vec<(TrafficClass, WallNs, Record)>,
        full: Option<TrafficClass>,
        gap: Option<u64>,
    }

    impl JournalSink for Room {
        fn record(&mut self, class: TrafficClass, now: WallNs, record: &Record) -> Recorded {
            self.offered.push((class, now, record.clone()));
            match self.full {
                Some(full) if full == class || full == TrafficClass::Safety => {
                    Recorded::DroppedCounted
                }
                _ => Recorded::Ok,
            }
        }

        fn omit(&mut self, _: TrafficClass, _: WallNs) -> Recorded {
            Recorded::DroppedCounted
        }

        fn open_gap(&self) -> Option<u64> {
            self.gap
        }
    }

    /// The moves the sink was offered, each with its class and wall time.
    fn moves(sink: &RefCell<Room>) -> Vec<(TrafficClass, WallNs)> {
        sink.borrow()
            .offered
            .iter()
            .filter(|(_, _, r)| matches!(r, Record::Marker(Marker::CancelOnly)))
            .map(|(c, at, _)| (*c, *at))
            .collect()
    }

    #[test]
    fn the_journal_moves_the_registries_given_once_each_time_it_reaches_its_soft_limit() {
        let sink = Rc::new(RefCell::new(Room::default()));
        let journal = Journal::new(sink.clone());
        let (first, second) = (Registry::default(), Registry::default());
        journal.cancel_only_at_soft_limit(first.cancel_only_trip());
        let record = Record::Marker(Marker::Recovered);
        let borrowed = || RecordRef::Owned(&record);
        // Room for everything: nothing moves.
        journal.record(TrafficClass::Normal, WallNs(1), &record);
        journal.record_ref(TrafficClass::Safety, WallNs(2), borrowed());
        assert_eq!(first.cancel_only_trip().trips(), 0);
        assert!(moves(&sink).is_empty());

        // A Normal record dropped reaches the soft limit: the registry moves once, and the move
        // is offered under Safety right after it, at its time.
        sink.borrow_mut().full = Some(TrafficClass::Normal);
        journal.record_ref(TrafficClass::Normal, WallNs(3), borrowed());
        assert_eq!(first.cancel_only_trip().trips(), 1);
        assert_eq!(moves(&sink), [(TrafficClass::Safety, WallNs(3))]);
        let offered = sink.borrow().offered.len();
        assert!(matches!(
            sink.borrow().offered[offered - 1],
            (_, _, Record::Marker(Marker::CancelOnly))
        ));
        // Still at it: more drops, and a Safety record kept, move nothing again.
        journal.record(TrafficClass::Normal, WallNs(4), &record);
        journal.try_record(TrafficClass::Normal, WallNs(5), &record);
        journal.record(TrafficClass::Safety, WallNs(6), &record);
        assert_eq!(first.cancel_only_trip().trips(), 1);
        assert_eq!(moves(&sink).len(), 1);

        // A registry given while at it moves at once, the move filed under the time the journal
        // reached it; the one given before does not move again.
        journal.cancel_only_at_soft_limit(second.cancel_only_trip());
        assert_eq!(second.cancel_only_trip().trips(), 1);
        assert_eq!(first.cancel_only_trip().trips(), 1);
        assert_eq!(
            moves(&sink).last(),
            Some(&(TrafficClass::Safety, WallNs(3)))
        );

        // A Normal record kept: back under it. The next drop, a Safety record's past the
        // reserve, reaches it again and moves both.
        sink.borrow_mut().full = None;
        journal.try_record(TrafficClass::Normal, WallNs(7), &record);
        sink.borrow_mut().full = Some(TrafficClass::Safety);
        journal.record(TrafficClass::Safety, WallNs(8), &record);
        assert_eq!(first.cancel_only_trip().trips(), 2);
        assert_eq!(second.cancel_only_trip().trips(), 2);
        // Its marker was offered too, and dropped and counted like any Safety record there.
        assert_eq!(
            moves(&sink).last(),
            Some(&(TrafficClass::Safety, WallNs(8)))
        );
        assert_eq!(moves(&sink).len(), 3);

        // A record offered while the sink is borrowed is not offered, and moves nothing.
        sink.borrow_mut().full = None;
        journal.record(TrafficClass::Normal, WallNs(9), &record);
        sink.borrow_mut().full = Some(TrafficClass::Normal);
        let held = sink.borrow_mut();
        journal.try_record(TrafficClass::Normal, WallNs(10), &record);
        drop(held);
        assert_eq!(first.cancel_only_trip().trips(), 2);
    }

    #[test]
    fn a_registry_given_while_the_sink_is_borrowed_at_the_soft_limit_still_moves() {
        let sink = Rc::new(RefCell::new(Room {
            full: Some(TrafficClass::Normal),
            ..Room::default()
        }));
        let journal = Journal::new(sink.clone());
        journal.record(
            TrafficClass::Normal,
            WallNs(1),
            &Record::Marker(Marker::Recovered),
        );
        let reg = Registry::default();
        // The move's marker cannot be offered while the sink is borrowed; the move stands.
        let held = sink.borrow_mut();
        journal.cancel_only_at_soft_limit(reg.cancel_only_trip());
        drop(held);
        assert_eq!(reg.cancel_only_trip().trips(), 1);
        assert!(moves(&sink).is_empty());
    }

    /// A sink that names its gaps (the journal's queue, decision 0139): a gap closed by a
    /// record the runtime's journal does not see as back under, a Safety record's or the
    /// consumer's own, leaves the soft limit, and the next drop, into a new gap, moves the
    /// registries again; a drop into the gap the journal reached it in does not.
    #[test]
    fn a_sink_that_names_its_gaps_moves_the_registries_again_at_each_new_gap() {
        let sink = Rc::new(RefCell::new(Room {
            full: Some(TrafficClass::Normal),
            gap: Some(0),
            ..Room::default()
        }));
        let journal = Journal::new(sink.clone());
        let (first, second) = (Registry::default(), Registry::default());
        journal.cancel_only_at_soft_limit(first.cancel_only_trip());
        let trips = |reg: &Registry| reg.cancel_only_trip().trips();
        let record = Record::Marker(Marker::Recovered);
        let offer = |class, at| journal.record(class, WallNs(at), &record);

        // The first drop, into gap 0, reaches the soft limit; more drops into it, and a Safety
        // record kept from the reserve, move nothing again.
        offer(TrafficClass::Normal, 1);
        offer(TrafficClass::Normal, 2);
        offer(TrafficClass::Safety, 3);
        assert_eq!(trips(&first), 1);

        // A Safety record closes the gap: space returned under the soft limit, so a registry
        // given now is not tripped.
        sink.borrow_mut().gap = None;
        offer(TrafficClass::Safety, 4);
        journal.cancel_only_at_soft_limit(second.cancel_only_trip());
        assert_eq!(trips(&second), 0);
        // With no Normal record kept since, the next drop, into gap 5, moves both.
        sink.borrow_mut().gap = Some(5);
        offer(TrafficClass::Normal, 6);
        assert_eq!((trips(&first), trips(&second)), (2, 1));
        offer(TrafficClass::Normal, 7);
        assert_eq!((trips(&first), trips(&second)), (2, 1));

        // A gap the consumer's own records closed and opened again, unseen: the runtime's next
        // drop, into gap 9, moves both again.
        sink.borrow_mut().gap = Some(9);
        offer(TrafficClass::Normal, 8);
        assert_eq!((trips(&first), trips(&second)), (3, 2));
        let at: Vec<WallNs> = moves(&sink).into_iter().map(|(_, at)| at).collect();
        assert_eq!(at, [WallNs(1), WallNs(6), WallNs(8)]);

        // Given while still in gap 9, a registry is tripped at once, filed under that drop's
        // time.
        let third = Registry::default();
        journal.cancel_only_at_soft_limit(third.cancel_only_trip());
        assert_eq!(trips(&third), 1);
        assert_eq!(
            moves(&sink).last(),
            Some(&(TrafficClass::Safety, WallNs(8)))
        );
    }
}
