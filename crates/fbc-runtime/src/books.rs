//! The runtime's books: one [`L2Book`] per (instrument, book channel), fed the events a
//! session hands on, and each instrument's configured trading book (FBC-nij).
//!
//! One instrument may be subscribed to several book channels on one connection (design §10: a
//! recorder keeps a public and an interactive book), and every book event names its
//! [`BookId`] (decision 0014 item 7), so each channel builds a book of its own: a snapshot,
//! level, window or `Health { feed: Feed::Book(id), h: Gap }` reaches only the book of its
//! instrument and channel ([`fbc_book::Books`]).
//!
//! Which channel an instrument trades on is the consumer's configuration ([`TradingBooks`];
//! design §7.2 records it in the journal header and treats a change of it as a strategy change),
//! with no default in code. [`MdBooks::trading_book`] exposes that channel's book and never
//! another, even while another channel's book of the instrument is the only valid one.
//!
//! [`BookKeeper`] is an [`MdHandler`]: it applies each envelope to its books, then hands it to
//! the consumer's [`BookHandler`] with the books as they now are, in the same call stack
//! (decision 0023).
//!
//! **A connection's end (FBC-crh, decision 0039).** Each book remembers the connection epoch
//! ([`ConnKey`]) of the last event about it. When a session's epoch ends (a drop, a reconnect
//! its codec asks for, a rotation, a silence, a stalled write or a stop), the session tells its
//! handler ([`MdHandler::on_epoch_end`]) after the last event of that epoch, and every book
//! last fed by that epoch, or by an earlier one of the same connection, is invalidated as a gap
//! ([`MdBooks::end_epoch`]): its levels and any snapshot in progress are gone, and it reads as
//! [`Gapped`](fbc_book::BookState::Gapped) until its next complete snapshot. A book another
//! connection feeds is untouched. The consumer's [`BookHandler::on_epoch_end`] is then told,
//! with the books as the end left them.

use std::collections::BTreeMap;
use std::fmt;

use fbc_book::{Applied, BookError, Books, L2Book};
use fbc_core::{BookId, ConnKey, Envelope, Feed, FeedHealth, InstrumentId, MdEvent};

use crate::session::MdHandler;

/// Each instrument's trading book channel, from the consumer's configuration. An instrument
/// absent from it has no trading book.
#[derive(Clone, Default, Eq, PartialEq, Debug)]
pub struct TradingBooks {
    by_inst: BTreeMap<InstrumentId, BookId>,
}

/// An instrument configured with two different trading book channels.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct TradingBookConflict {
    pub inst: InstrumentId,
    pub first: BookId,
    pub second: BookId,
}

impl fmt::Display for TradingBookConflict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "instrument {} is configured with two trading books, channels {} and {}",
            self.inst.get(),
            self.first.0,
            self.second.0
        )
    }
}

impl std::error::Error for TradingBookConflict {}

impl TradingBooks {
    /// The trading channel of each instrument in `pairs`; an instrument named with two
    /// different channels is refused, and one named twice with the same channel is one entry.
    pub fn new(
        pairs: impl IntoIterator<Item = (InstrumentId, BookId)>,
    ) -> Result<TradingBooks, TradingBookConflict> {
        let mut by_inst = BTreeMap::new();
        for (inst, book) in pairs {
            match by_inst.insert(inst, book) {
                Some(first) if first != book => {
                    return Err(TradingBookConflict {
                        inst,
                        first,
                        second: book,
                    });
                }
                _ => {}
            }
        }
        Ok(TradingBooks { by_inst })
    }

    /// The trading channel of `inst`, if it has one.
    pub fn get(&self, inst: InstrumentId) -> Option<BookId> {
        self.by_inst.get(&inst).copied()
    }
}

/// The books of every (instrument, book channel) a venue's sessions report, and the configured
/// trading channel of each instrument.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct MdBooks {
    books: Books,
    /// The connection epoch of the last event about each book.
    fed: BTreeMap<(InstrumentId, BookId), ConnKey>,
    trading: TradingBooks,
    refused: u64,
}

impl MdBooks {
    /// No books yet; `trading` names each instrument's trading channel.
    pub fn new(trading: TradingBooks) -> MdBooks {
        MdBooks {
            books: Books::new(),
            fed: BTreeMap::new(),
            trading,
            refused: 0,
        }
    }

    /// Applies one event, which came from connection epoch `from`, to the book of its
    /// instrument and channel, and remembers `from` as the epoch that last fed that book; an
    /// event about no book changes nothing. A refused event (a snapshot end with no snapshot
    /// begun, an inverted window) is counted.
    pub fn apply(&mut self, from: ConnKey, ev: &MdEvent) -> Result<Applied, BookError> {
        if let Some(book) = book_of(ev) {
            self.fed.insert(book, from);
        }
        let applied = self.books.apply(ev);
        if applied.is_err() {
            self.refused += 1;
        }
        applied
    }

    /// Connection epoch `ended` has ended: every book last fed by it, or by an earlier epoch of
    /// its connection, is invalidated as a gap until its next complete snapshot. A book whose
    /// last event came from another connection, or from a later epoch, is untouched. Returns
    /// how many books it invalidated.
    pub fn end_epoch(&mut self, ended: ConnKey) -> usize {
        let mut invalidated = 0;
        for (&(inst, book), &from) in &self.fed {
            // A `Live` or `Stale` report made no book.
            let exists = self.books.get(inst, book).is_some();
            if exists && from.conn == ended.conn && from.epoch <= ended.epoch {
                let h = FeedHealth::Gap;
                let feed = Feed::Book(book);
                // A gap on a book that exists is never refused.
                let _ = self.books.apply(&MdEvent::Health { inst, feed, h });
                invalidated += 1;
            }
        }
        invalidated
    }

    /// The book of `inst` on channel `book`, once an event about it has arrived.
    pub fn book(&self, inst: InstrumentId, book: BookId) -> Option<&L2Book> {
        self.books.get(inst, book)
    }

    /// The configured trading channel of `inst`.
    pub fn trading_book_id(&self, inst: InstrumentId) -> Option<BookId> {
        self.trading.get(inst)
    }

    /// The book of `inst`'s configured trading channel, once an event about it has arrived:
    /// never another channel's book, whatever state either is in. It reads invalid from the end
    /// of the connection epoch that last fed it until its next complete snapshot.
    pub fn trading_book(&self, inst: InstrumentId) -> Option<&L2Book> {
        self.book(inst, self.trading.get(inst)?)
    }

    /// How many events the books refused.
    pub fn refused(&self) -> u64 {
        self.refused
    }
}

/// Where the consumer receives a session's events once the books have taken them: called once
/// per event, in ingest order, with the books as that event left them.
pub trait BookHandler {
    fn on_md(&mut self, env: Envelope<MdEvent>, books: &MdBooks);

    /// Connection epoch `key` ended and the books it fed are invalid ([`MdBooks::end_epoch`]):
    /// called with the books as that left them. Nothing by default.
    fn on_epoch_end(&mut self, key: ConnKey, books: &MdBooks) {
        let _ = (key, books);
    }
}

impl<F: FnMut(Envelope<MdEvent>, &MdBooks)> BookHandler for F {
    fn on_md(&mut self, env: Envelope<MdEvent>, books: &MdBooks) {
        self(env, books)
    }
}

/// An [`MdHandler`] that keeps the books: each event goes to [`MdBooks::apply`], then, refused
/// or not, to the consumer's [`BookHandler`].
pub struct BookKeeper<H> {
    books: MdBooks,
    handler: H,
}

impl<H: BookHandler> BookKeeper<H> {
    /// Keeps `books`, handing each event on to `handler`.
    pub fn new(books: MdBooks, handler: H) -> BookKeeper<H> {
        BookKeeper { books, handler }
    }

    /// The books.
    pub fn books(&self) -> &MdBooks {
        &self.books
    }
}

impl<H: BookHandler> MdHandler for BookKeeper<H> {
    fn on_md(&mut self, env: Envelope<MdEvent>) {
        // A refusal is counted in the books; the consumer still sees the event.
        let _ = self.books.apply(env.stamp.conn, &env.body);
        self.handler.on_md(env, &self.books);
    }

    fn on_epoch_end(&mut self, key: ConnKey) {
        self.books.end_epoch(key);
        self.handler.on_epoch_end(key, &self.books);
    }
}

/// The book an event is about, if it is about one.
fn book_of(ev: &MdEvent) -> Option<(InstrumentId, BookId)> {
    match *ev {
        MdEvent::BookSnapshotBegin { inst, book, .. }
        | MdEvent::BookSnapshotEnd { inst, book }
        | MdEvent::Level { inst, book, .. }
        | MdEvent::Window { inst, book, .. }
        | MdEvent::Health {
            inst,
            feed: Feed::Book(book),
            ..
        } => Some((inst, book)),
        _ => None,
    }
}
