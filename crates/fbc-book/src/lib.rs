//! A tick-indexed L2 order book built from normalized book events, the same live and in replay
//! (decisions 0006 and 0014; design §3's book on the instrument's finest grid).
//!
//! [`Books`] keeps one [`L2Book`] per (instrument, [`BookId`](fbc_core::BookId)) and feeds it
//! [`MdEvent`](fbc_core::MdEvent)s: `BookSnapshotBegin` starts a replacement under its epoch,
//! `Level` sets a level in that replacement or, as a delta, in the book (zero quantity removes
//! it), `BookSnapshotEnd` makes the replacement the book, `Window` bounds a windowed book, and
//! `Health` with a gap on the book's feed invalidates it until the next complete snapshot.
//!
//! A valid book gives its [`touch`](L2Book::touch) and [`top`](L2Book::top) levels per side,
//! never a level outside its window; two tops compare exactly, naming the first level that
//! differs ([`Top::first_difference`]); and [`L2Book::canonical_bytes`] serializes the whole
//! state so two books compare byte for byte.
//!
//! Not here yet: the ex-own projection, touch arbitration and continuity policies (the rest of
//! design §3's book), and keeping books per stream in the runtime (FBC-nij).

mod book;
mod books;

pub use book::{Applied, BookError, BookState, L2Book, LevelDiff, Top, Touch, Window};
pub use books::Books;

#[cfg(test)]
mod tests;
