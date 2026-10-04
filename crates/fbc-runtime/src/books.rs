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
//! Not here yet (FBC-crh): a connection's end does not invalidate the books it fed. The handler
//! sees only events (decision 0023), and a session opens its next epoch without one, so a book
//! stays [`Valid`](fbc_book::BookState::Valid), with its last levels, from a drop until the new
//! epoch's snapshot ends, unless its codec reports a gap. Until FBC-crh lands, a book's validity
//! says only that no gap was reported, not that its connection is still up.

use std::collections::BTreeMap;
use std::fmt;

use fbc_book::{Applied, BookError, Books, L2Book};
use fbc_core::{BookId, Envelope, InstrumentId, MdEvent};

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
    trading: TradingBooks,
    refused: u64,
}

impl MdBooks {
    /// No books yet; `trading` names each instrument's trading channel.
    pub fn new(trading: TradingBooks) -> MdBooks {
        MdBooks {
            books: Books::new(),
            trading,
            refused: 0,
        }
    }

    /// Applies one event to the book of its instrument and channel; an event about no book
    /// changes nothing. A refused event (a snapshot end with no snapshot begun, an inverted
    /// window) is counted.
    pub fn apply(&mut self, ev: &MdEvent) -> Result<Applied, BookError> {
        let applied = self.books.apply(ev);
        if applied.is_err() {
            self.refused += 1;
        }
        applied
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
    /// never another channel's book, whatever state either is in.
    /// Its state does not yet reflect a dropped connection (FBC-crh; see the module docs).
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
        let _ = self.books.apply(&env.body);
        self.handler.on_md(env, &self.books);
    }
}
