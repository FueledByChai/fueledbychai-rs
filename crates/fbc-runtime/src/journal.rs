//! The shard's journal as the runtime records into it (decision 0006, design §4.8, §5.4).
//!
//! A [`Journal`] is a handle on the consumer's [`JournalSink`], shared by the sessions of one
//! shard thread. A record is offered under the traffic class of what it records and the session
//! goes on whatever the sink answers: the sink never blocks, and a record it drops is counted
//! there, so a Safety frame whose records found no room in the reserve is still written.

use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;

use fbc_core::{TrafficClass, WallNs};
use fbc_journal::{JournalSink, Record, RecordRef};

/// The journal the sessions of one shard thread record into. Cloning it shares the sink.
#[derive(Clone)]
pub struct Journal {
    sink: Rc<RefCell<dyn JournalSink>>,
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
        Journal { sink }
    }

    /// Offers `record` under `class`, filed under the UTC day of `now`; what it records goes
    /// ahead whether the sink kept or dropped (and counted) it.
    pub(crate) fn record(&self, class: TrafficClass, now: WallNs, record: &Record) {
        let _ = self.sink.borrow_mut().record(class, now, record);
    }

    /// Offers `record`, whose large contents it borrows, so a sink with no room for it
    /// refuses it before they are copied ([`JournalSink::record_ref`]).
    pub(crate) fn record_ref(&self, class: TrafficClass, now: WallNs, record: RecordRef<'_>) {
        let _ = self.sink.borrow_mut().record_ref(class, now, record);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fbc_journal::{Marker, Recorded};

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
        assert_eq!(classes, [TrafficClass::Normal, TrafficClass::Safety]);
        assert_eq!(format!("{journal:?}"), "Journal");
    }
}
