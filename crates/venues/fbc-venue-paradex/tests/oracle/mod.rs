//! The book and touch oracles of BT-401 (design §14 M0), as test support: fbc-book is a
//! dev-dependency only, so the venue crate keeps depending on fbc-core and protocol crates alone.
//!
//! - [`check_snapshot`]: a REST `/orderbook` snapshot matches the book built from the deltas at
//!   its seq_no, exactly, for the top [`ORDERBOOK_DEPTH`] levels per side, or the first
//!   differing level is named (fbc-book's top-n comparison).
//! - [`touch_agreement`]: each bbo sample is compared with the delta-built touch at the sample
//!   point the bbo's sequence domain gives: the equal seq_no when bbo shares the book's sequence
//!   (decision 0022). It reports the agreeing fraction and every disagreeing sample.
//!
//! Running them over a recorded soak is FBC-bxq's.

use core::fmt;
use std::collections::BTreeMap;

use fbc_book::{BookError, BookState, Books, L2Book, LevelDiff, Top, Touch};
use fbc_core::{BookId, Feed, InstrumentId, MdEvent, SeqDomain, VenueMeta};
use fbc_venue_paradex::md::rest::{ORDERBOOK_DEPTH, OrderbookSnapshot};

/// One market's book channel built from its decoded book events, the seq_no of the last frame
/// applied, and the touch after each seq_no at which the book was valid.
pub struct DeltaBook {
    inst: InstrumentId,
    book: BookId,
    books: Books,
    seq: Option<u64>,
    touch_at: BTreeMap<u64, Touch>,
}

impl DeltaBook {
    /// No frame applied yet.
    pub fn new(inst: InstrumentId, book: BookId) -> DeltaBook {
        DeltaBook {
            inst,
            book,
            books: Books::new(),
            seq: None,
            touch_at: BTreeMap::new(),
        }
    }

    /// Applies one decoded event; an event about this book moves its seq_no to the event's.
    pub fn apply(&mut self, meta: &VenueMeta, ev: &MdEvent) -> Result<(), BookError> {
        self.books.apply(ev)?;
        let about = match *ev {
            MdEvent::BookSnapshotBegin { inst, book, .. }
            | MdEvent::BookSnapshotEnd { inst, book }
            | MdEvent::Level { inst, book, .. }
            | MdEvent::Window { inst, book, .. }
            | MdEvent::Health {
                inst,
                feed: Feed::Book(book),
                ..
            } => (inst, book) == (self.inst, self.book),
            _ => false,
        };
        if let (true, Some(seq)) = (about, meta.venue_seq) {
            self.seq = Some(seq);
            if let Ok(touch) = self.l2().and_then(L2Book::touch) {
                self.touch_at.insert(seq, touch);
            }
        }
        Ok(())
    }

    /// The seq_no of the last frame applied.
    pub fn seq(&self) -> Option<u64> {
        self.seq
    }

    /// The touch after the frame at `seq`, if the book was valid there.
    pub fn touch_at(&self, seq: u64) -> Option<Touch> {
        self.touch_at.get(&seq).copied()
    }

    /// The book as it stands, refused when no book event has reached it (a gap reported
    /// before its first snapshot creates none).
    fn l2(&self) -> Result<&L2Book, BookError> {
        let built = self.books.get(self.inst, self.book);
        built.ok_or(BookError::NotValid(BookState::AwaitingSnapshot))
    }
}

/// Why a REST snapshot does not match the delta-built book.
#[derive(Clone, Eq, PartialEq, Debug)]
pub enum BookMismatch {
    /// The snapshot and the book are of different markets.
    OtherInstrument,
    /// The book is not at the snapshot's seq_no.
    NotAtSeq { seq_no: u64, book: Option<u64> },
    /// The book cannot be read.
    NotValid(BookError),
    /// The first level at which they differ; left is the snapshot, right the book.
    Differs { seq_no: u64, diff: LevelDiff },
}

impl fmt::Display for BookMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BookMismatch::OtherInstrument => {
                f.write_str("REST snapshot and delta-built book are of different markets")
            }
            BookMismatch::NotAtSeq { seq_no, book } => {
                write!(
                    f,
                    "REST snapshot at seq_no {seq_no}, delta-built book at {book:?}"
                )
            }
            BookMismatch::NotValid(err) => write!(f, "delta-built book cannot be read: {err}"),
            BookMismatch::Differs { seq_no, diff } => write!(
                f,
                "REST snapshot at seq_no {seq_no} differs from the delta-built book: {diff}"
            ),
        }
    }
}

/// Compares `snap`'s levels with the top [`ORDERBOOK_DEPTH`] of `book`, which must stand at the
/// snapshot's seq_no: equal, or the first differing level named.
pub fn check_snapshot(book: &DeltaBook, snap: &OrderbookSnapshot) -> Result<(), BookMismatch> {
    if snap.inst != book.inst {
        return Err(BookMismatch::OtherInstrument);
    }
    if book.seq != Some(snap.seq_no) {
        let (seq_no, book) = (snap.seq_no, book.seq);
        return Err(BookMismatch::NotAtSeq { seq_no, book });
    }
    let built = book.l2().and_then(|l2| l2.top(ORDERBOOK_DEPTH));
    let built = built.map_err(BookMismatch::NotValid)?;
    let rest = Top {
        bids: snap.bids.clone(),
        asks: snap.asks.clone(),
    };
    match rest.first_difference(&built) {
        None => Ok(()),
        Some(diff) => Err(BookMismatch::Differs {
            seq_no: snap.seq_no,
            diff,
        }),
    }
}

/// A bbo sample that disagrees with the delta-built touch at its sample point.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct Disagreement {
    /// Its index among the samples (bbo touches only), from 0.
    pub sample: usize,
    pub seq_no: Option<u64>,
    pub bbo: Touch,
    /// The delta-built touch there; `None` when the book had no valid frame at that seq_no.
    pub book: Option<Touch>,
}

/// How many bbo samples agree with the delta-built touch, and which do not.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Agreement {
    pub samples: usize,
    pub disagreements: Vec<Disagreement>,
}

impl Agreement {
    /// The samples that agree.
    pub fn agreeing(&self) -> usize {
        self.samples - self.disagreements.len()
    }

    /// The agreeing share in percent, rounded to a tenth; 0 with no samples.
    pub fn percent(&self) -> f64 {
        let per_mille = (self.agreeing() * 1000).checked_div(self.samples);
        per_mille.map_or(0.0, |p| p as f64 / 10.0)
    }
}

impl fmt::Display for Agreement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (agreeing, samples, percent) = (self.agreeing(), self.samples, self.percent());
        write!(
            f,
            "{agreeing} of {samples} bbo samples agree ({percent:.1}%)"
        )
    }
}

/// Why the touch agreement cannot be checked.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum OracleError {
    /// A bbo in this sequence domain has no sample point in the book's sequence.
    NoSamplePoint(SeqDomain),
}

impl fmt::Display for OracleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let OracleError::NoSamplePoint(domain) = self;
        write!(
            f,
            "no sample point in the book's sequence for a bbo in {domain:?}"
        )
    }
}

/// Compares every bbo touch in `samples` (other events are not samples) with `book`'s touch at
/// the sample point `domain` gives: the equal seq_no for [`SeqDomain::SharedWithBook`].
pub fn touch_agreement(
    domain: SeqDomain,
    book: &DeltaBook,
    samples: &[(VenueMeta, MdEvent)],
) -> Result<Agreement, OracleError> {
    if domain != SeqDomain::SharedWithBook {
        return Err(OracleError::NoSamplePoint(domain));
    }
    let touches = samples.iter().filter_map(|(meta, ev)| match *ev {
        MdEvent::Touch { bid, ask, .. } => Some((meta.venue_seq, Touch { bid, ask })),
        _ => None,
    });
    let mut agreement = Agreement {
        samples: 0,
        disagreements: Vec::new(),
    };
    for (sample, (seq_no, bbo)) in touches.enumerate() {
        agreement.samples += 1;
        let at = seq_no.and_then(|seq| book.touch_at(seq));
        if at != Some(bbo) {
            let book = at;
            let d = Disagreement {
                sample,
                seq_no,
                bbo,
                book,
            };
            agreement.disagreements.push(d);
        }
    }
    Ok(agreement)
}
