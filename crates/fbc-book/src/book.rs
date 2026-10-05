//! One book channel's L2 book for one instrument.

use core::fmt;
use std::collections::BTreeMap;

use fbc_core::{BookSide, Lots, Lvl, Ticks};

/// Whether a book can be read.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum BookState {
    /// No complete snapshot yet; deltas are dropped.
    AwaitingSnapshot,
    /// Built from the complete snapshot under `epoch` and the deltas since.
    Valid { epoch: u32 },
    /// A gap was reported; invalid until the next complete snapshot.
    Gapped,
}

/// What an event did to a book.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum Applied {
    /// The book (or the snapshot in progress) took the event.
    Changed,
    /// A delta arrived while the book was not valid and no snapshot was in progress: dropped.
    DroppedNotValid,
    /// The event is not about any book this holds (a trade, a mark, another feed's health,
    /// a book feed's `Live` or `Stale`): nothing changed.
    Unchanged,
}

/// A book operation that was refused.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum BookError {
    /// A snapshot end arrived with no snapshot begun (or after a gap threw it away).
    NoSnapshotInProgress,
    /// A window whose `lo` is above its `hi`.
    InvertedWindow { lo: Ticks, hi: Ticks },
    /// The book cannot be read in this state.
    NotValid(BookState),
}

impl fmt::Display for BookError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BookError::NoSnapshotInProgress => {
                f.write_str("book snapshot end without a snapshot in progress")
            }
            BookError::InvertedWindow { lo, hi } => {
                write!(f, "book window {}..={} is inverted", lo.0, hi.0)
            }
            BookError::NotValid(state) => write!(f, "book is not valid: {state:?}"),
        }
    }
}

impl std::error::Error for BookError {}

/// The prices a windowed book covers, `lo..=hi`; levels outside are unknown.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct Window {
    lo: Ticks,
    hi: Ticks,
}

impl Window {
    /// The window `lo..=hi`, refused when `lo > hi`.
    pub fn new(lo: Ticks, hi: Ticks) -> Result<Window, BookError> {
        if lo > hi {
            return Err(BookError::InvertedWindow { lo, hi });
        }
        Ok(Window { lo, hi })
    }

    /// Its lowest price.
    pub fn lo(self) -> Ticks {
        self.lo
    }

    /// Its highest price.
    pub fn hi(self) -> Ticks {
        self.hi
    }

    /// Whether `px` is inside it.
    pub fn contains(self, px: Ticks) -> bool {
        self.lo <= px && px <= self.hi
    }
}

/// The best bid and offer; `None` for an empty side.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct Touch {
    pub bid: Option<Lvl>,
    pub ask: Option<Lvl>,
}

/// The best levels of each side: bids from the highest price down, asks from the lowest up.
#[derive(Clone, Default, Eq, PartialEq, Hash, Debug)]
pub struct Top {
    pub bids: Vec<Lvl>,
    pub asks: Vec<Lvl>,
}

impl Top {
    /// The first level at which `self` (left) and `other` (right) differ, or `None` when they
    /// are equal. Levels are compared by rank from the touch, the bid before the ask at each
    /// rank, so the difference named is the one nearest the touch; a level one side has and
    /// the other lacks differs.
    pub fn first_difference(&self, other: &Top) -> Option<LevelDiff> {
        let depth = self
            .bids
            .len()
            .max(other.bids.len())
            .max(self.asks.len())
            .max(other.asks.len());
        (0..depth).find_map(|rank| {
            [
                (BookSide::Bid, &self.bids, &other.bids),
                (BookSide::Ask, &self.asks, &other.asks),
            ]
            .into_iter()
            .find_map(|(side, l, r)| {
                let (left, right) = (l.get(rank).copied(), r.get(rank).copied());
                (left != right).then_some(LevelDiff {
                    side,
                    rank,
                    left,
                    right,
                })
            })
        })
    }
}

/// The first level at which two tops differ ([`Top::first_difference`]).
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct LevelDiff {
    pub side: BookSide,
    /// Its rank from the touch, 0 being the touch.
    pub rank: usize,
    /// The left top's level there, `None` when it has none.
    pub left: Option<Lvl>,
    /// The right top's level there, `None` when it has none.
    pub right: Option<Lvl>,
}

impl fmt::Display for LevelDiff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn level(l: Option<Lvl>) -> String {
            l.map_or_else(
                || "none".to_owned(),
                |l| format!("{}x{}", l.px.0, l.qty.get()),
            )
        }
        let side = match self.side {
            BookSide::Bid => "bid",
            BookSide::Ask => "ask",
        };
        write!(
            f,
            "{side} rank {}: left {}, right {}",
            self.rank,
            level(self.left),
            level(self.right)
        )
    }
}

/// The levels of both sides, keyed by price. Zero quantity is never stored.
#[derive(Clone, Default, Eq, PartialEq, Hash, Debug)]
struct Levels {
    bids: BTreeMap<Ticks, Lots>,
    asks: BTreeMap<Ticks, Lots>,
}

impl Levels {
    fn set(&mut self, side: BookSide, px: Ticks, qty: Lots) {
        let levels = match side {
            BookSide::Bid => &mut self.bids,
            BookSide::Ask => &mut self.asks,
        };
        if qty == Lots::ZERO {
            levels.remove(&px);
        } else {
            levels.insert(px, qty);
        }
    }

    fn retain_within(&mut self, window: Window) {
        self.bids.retain(|px, _| window.contains(*px));
        self.asks.retain(|px, _| window.contains(*px));
    }

    fn write(&self, out: &mut Vec<u8>) {
        for side in [&self.bids, &self.asks] {
            // A side holds at most one level per i64 price, far fewer than u32::MAX in memory.
            out.extend_from_slice(&(side.len() as u32).to_le_bytes());
            for (px, qty) in side {
                out.extend_from_slice(&px.0.to_le_bytes());
                out.extend_from_slice(&qty.get().to_le_bytes());
            }
        }
    }
}

/// A snapshot that has begun and not yet ended.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
struct Pending {
    epoch: u32,
    levels: Levels,
}

/// One book channel's L2 book for one instrument, in [`Ticks`] on the instrument's finest grid
/// and [`Lots`], keyed by price.
///
/// - [`begin_snapshot`](L2Book::begin_snapshot) starts a replacement under its epoch; the book
///   as it was stays the book, and is still read, until
///   [`end_snapshot`](L2Book::end_snapshot) makes the replacement the book. A second begin
///   abandons an unfinished replacement.
/// - [`set_level`](L2Book::set_level) sets a level of the replacement while one is in
///   progress, and of the book otherwise (a delta); zero quantity removes it. A delta to a
///   book that is not valid is dropped.
/// - [`set_window`](L2Book::set_window) bounds a windowed book: levels outside it are
///   unknown, so it drops them, and levels set outside it later are never reported. The
///   window holds across snapshots until the next one.
/// - [`gap`](L2Book::gap) invalidates the book, levels and replacement in progress included,
///   until the next complete snapshot.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct L2Book {
    state: BookState,
    levels: Levels,
    window: Option<Window>,
    pending: Option<Pending>,
}

impl Default for L2Book {
    fn default() -> L2Book {
        L2Book::new()
    }
}

/// The first bytes of [`L2Book::canonical_bytes`], and the layout's version after them.
const CANONICAL_MAGIC: &[u8; 4] = b"FBCB";
const CANONICAL_VERSION: u8 = 1;

impl L2Book {
    /// An empty book awaiting its first snapshot.
    pub fn new() -> L2Book {
        L2Book {
            state: BookState::AwaitingSnapshot,
            levels: Levels::default(),
            window: None,
            pending: None,
        }
    }

    /// Whether it can be read.
    pub fn state(&self) -> BookState {
        self.state
    }

    /// Its window, when the channel is windowed.
    pub fn window(&self) -> Option<Window> {
        self.window
    }

    /// The epoch of the snapshot in progress, if one is.
    pub fn snapshot_in_progress(&self) -> Option<u32> {
        self.pending.as_ref().map(|p| p.epoch)
    }

    /// Starts a replacement of the book under `epoch`.
    pub fn begin_snapshot(&mut self, epoch: u32) {
        self.pending = Some(Pending {
            epoch,
            levels: Levels::default(),
        });
    }

    /// Makes the replacement in progress the book, now valid under its epoch.
    pub fn end_snapshot(&mut self) -> Result<(), BookError> {
        let pending = self.pending.take().ok_or(BookError::NoSnapshotInProgress)?;
        self.levels = pending.levels;
        self.state = BookState::Valid {
            epoch: pending.epoch,
        };
        Ok(())
    }

    /// Sets one level to `qty` (zero removes it): in the replacement in progress, else in the
    /// book if it is valid, else nowhere ([`Applied::DroppedNotValid`]).
    pub fn set_level(&mut self, side: BookSide, px: Ticks, qty: Lots) -> Applied {
        if let Some(pending) = &mut self.pending {
            pending.levels.set(side, px, qty);
        } else if let BookState::Valid { .. } = self.state {
            self.levels.set(side, px, qty);
        } else {
            return Applied::DroppedNotValid;
        }
        Applied::Changed
    }

    /// Bounds the book to `lo..=hi`, dropping the levels outside it, from the book and from
    /// the replacement in progress.
    pub fn set_window(&mut self, lo: Ticks, hi: Ticks) -> Result<(), BookError> {
        let window = Window::new(lo, hi)?;
        self.levels.retain_within(window);
        if let Some(pending) = &mut self.pending {
            pending.levels.retain_within(window);
        }
        self.window = Some(window);
        Ok(())
    }

    /// A gap: the book is invalid, and its levels and any replacement in progress are gone,
    /// until the next complete snapshot.
    pub fn gap(&mut self) {
        self.state = BookState::Gapped;
        self.levels = Levels::default();
        self.pending = None;
    }

    /// The best bid and offer inside the window.
    pub fn touch(&self) -> Result<Touch, BookError> {
        let mut top = self.top(1)?;
        Ok(Touch {
            bid: top.bids.pop(),
            ask: top.asks.pop(),
        })
    }

    /// The size at `px` on `side`: `Some(Lots::ZERO)` when the book has no level there, and
    /// `None` when `px` is outside the window, where the size is unknown.
    pub fn level(&self, side: BookSide, px: Ticks) -> Result<Option<Lots>, BookError> {
        if !matches!(self.state, BookState::Valid { .. }) {
            return Err(BookError::NotValid(self.state));
        }
        if self.window.is_some_and(|w| !w.contains(px)) {
            return Ok(None);
        }
        let levels = match side {
            BookSide::Bid => &self.levels.bids,
            BookSide::Ask => &self.levels.asks,
        };
        Ok(Some(levels.get(&px).copied().unwrap_or(Lots::ZERO)))
    }

    /// The best `n` levels of each side inside the window.
    pub fn top(&self, n: usize) -> Result<Top, BookError> {
        if !matches!(self.state, BookState::Valid { .. }) {
            return Err(BookError::NotValid(self.state));
        }
        let shown = |(px, qty): (&Ticks, &Lots)| {
            self.window
                .is_none_or(|w| w.contains(*px))
                .then_some(Lvl { px: *px, qty: *qty })
        };
        Ok(Top {
            bids: self
                .levels
                .bids
                .iter()
                .rev()
                .filter_map(shown)
                .take(n)
                .collect(),
            asks: self.levels.asks.iter().filter_map(shown).take(n).collect(),
        })
    }

    /// A stable serialization of the book's whole state, so two books compare byte for byte:
    /// equal bytes mean equal books, and a rebuild from the same events gives the same bytes.
    ///
    /// Layout, integers little-endian: `FBCB`, the layout version (1); the state (0 awaiting a
    /// snapshot, 1 valid followed by its `u32` epoch, 2 gapped); the window (0 none, 1 followed
    /// by `lo` and `hi` as `i64`); the levels; the snapshot in progress (0 none, 1 followed by
    /// its `u32` epoch and its levels). Levels are the bids then the asks, each a `u32` count
    /// and then `(px: i64, qty: i64)` pairs in ascending price order, including any level set
    /// outside the window and so not reported.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(CANONICAL_MAGIC);
        out.push(CANONICAL_VERSION);
        match self.state {
            BookState::AwaitingSnapshot => out.push(0),
            BookState::Valid { epoch } => {
                out.push(1);
                out.extend_from_slice(&epoch.to_le_bytes());
            }
            BookState::Gapped => out.push(2),
        }
        match self.window {
            None => out.push(0),
            Some(w) => {
                out.push(1);
                out.extend_from_slice(&w.lo.0.to_le_bytes());
                out.extend_from_slice(&w.hi.0.to_le_bytes());
            }
        }
        self.levels.write(&mut out);
        match &self.pending {
            None => out.push(0),
            Some(p) => {
                out.push(1);
                out.extend_from_slice(&p.epoch.to_le_bytes());
                p.levels.write(&mut out);
            }
        }
        out
    }
}
