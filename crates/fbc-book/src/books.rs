//! The books of every (instrument, book channel), fed normalized market-data events.

use std::collections::BTreeMap;

use fbc_core::{BookId, Feed, FeedHealth, InstrumentId, MdEvent};

use crate::book::{Applied, BookError, L2Book};

/// One [`L2Book`] per (instrument, [`BookId`]): two book channels of one instrument never
/// merge (decision 0014). A book exists once an event about it arrives.
#[derive(Clone, Default, Eq, PartialEq, Debug)]
pub struct Books {
    books: BTreeMap<(InstrumentId, BookId), L2Book>,
}

impl Books {
    /// No books.
    pub fn new() -> Books {
        Books::default()
    }

    /// The book of `inst` on channel `book`, if any event about it has arrived.
    pub fn get(&self, inst: InstrumentId, book: BookId) -> Option<&L2Book> {
        self.books.get(&(inst, book))
    }

    /// Applies one market-data event. Book events go to their book; `Health` with
    /// [`FeedHealth::Gap`] on a book feed invalidates that book; everything else is
    /// [`Applied::Unchanged`].
    pub fn apply(&mut self, ev: &MdEvent) -> Result<Applied, BookError> {
        match *ev {
            MdEvent::BookSnapshotBegin { inst, book, epoch } => {
                self.entry(inst, book).begin_snapshot(epoch);
                Ok(Applied::Changed)
            }
            MdEvent::BookSnapshotEnd { inst, book } => {
                self.entry(inst, book).end_snapshot()?;
                Ok(Applied::Changed)
            }
            MdEvent::Level {
                inst,
                book,
                side,
                px,
                qty,
            } => Ok(self.entry(inst, book).set_level(side, px, qty)),
            MdEvent::Window { inst, book, lo, hi } => {
                self.entry(inst, book).set_window(lo, hi)?;
                Ok(Applied::Changed)
            }
            MdEvent::Health {
                inst,
                feed: Feed::Book(book),
                h: FeedHealth::Gap,
            } => match self.books.get_mut(&(inst, book)) {
                Some(b) => {
                    b.gap();
                    Ok(Applied::Changed)
                }
                // A book that never got an event is already awaiting its first snapshot.
                None => Ok(Applied::Unchanged),
            },
            _ => Ok(Applied::Unchanged),
        }
    }

    fn entry(&mut self, inst: InstrumentId, book: BookId) -> &mut L2Book {
        self.books.entry((inst, book)).or_default()
    }
}
