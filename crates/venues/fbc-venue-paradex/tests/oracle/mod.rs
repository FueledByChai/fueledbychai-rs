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
use fbc_core::{
    BookId, InstrumentId, MdCaps, MdEvent, SeqDomain, TagSet, TouchSourceId, VenueMeta,
};
use fbc_venue_paradex::md::rest::{ORDERBOOK_CHANNELS, ORDERBOOK_DEPTH, OrderbookSnapshot};

/// One market's book channel built from its decoded frames, the seq_no of the last frame, and
/// the touch after each frame's seq_no (`None` where the book could not be read).
///
/// The seq_no comes from the frame itself ([`frame_seq`](fbc_venue_paradex::md::book::frame_seq)),
/// not from the events it decoded to: a delta with no levels decodes to no event, yet the codec
/// accepts it and its seq_no advances the book's sequence (Codex r4177861136).
pub struct DeltaBook {
    inst: InstrumentId,
    book: BookId,
    books: Books,
    seq: Option<u64>,
    touch_at: BTreeMap<u64, Option<Touch>>,
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

    /// Applies the events one frame decoded to; `at` is the frame's market and seq_no when it
    /// is a book frame, and a frame of this market moves the book's seq_no to its own.
    pub fn frame(
        &mut self,
        at: Option<(InstrumentId, u64)>,
        events: &[(VenueMeta, MdEvent)],
    ) -> Result<(), BookError> {
        for (_, ev) in events {
            self.books.apply(ev)?;
        }
        if let Some((_, seq)) = at.filter(|&(inst, _)| inst == self.inst) {
            self.seq = Some(seq);
            let touch = self.l2().and_then(L2Book::touch).ok();
            self.touch_at.insert(seq, touch);
        }
        Ok(())
    }

    /// The seq_no of the last frame applied.
    pub fn seq(&self) -> Option<u64> {
        self.seq
    }

    /// The touch after the frame at `seq`, if there was one and the book could be read.
    pub fn touch_at(&self, seq: u64) -> Option<Touch> {
        self.touch_at.get(&seq).copied().flatten()
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
    /// The caps declare no such book channel.
    Undeclared,
    /// The book shows other order channels than the REST snapshot's public book.
    OtherChannels,
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
            BookMismatch::Undeclared => f.write_str("book channel not declared"),
            BookMismatch::OtherChannels => {
                f.write_str("delta-built book shows other order channels than the REST snapshot")
            }
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

/// Compares `snap`'s levels with the top [`ORDERBOOK_DEPTH`] of `book`, which must be a declared
/// book channel showing the snapshot's order channels ([`ORDERBOOK_CHANNELS`]; Codex
/// r4177915521) and stand at the snapshot's seq_no: equal, or the first differing level named.
pub fn check_snapshot(
    md: &MdCaps,
    book: &DeltaBook,
    snap: &OrderbookSnapshot,
) -> Result<(), BookMismatch> {
    let book_caps = md.books.get(usize::from(book.book.0));
    let book_caps = book_caps.ok_or(BookMismatch::Undeclared)?;
    if book_caps.includes_channels != TagSet::of(ORDERBOOK_CHANNELS) {
        return Err(BookMismatch::OtherChannels);
    }
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

    /// The agreeing share in percent, truncated to a tenth so it never overstates the share
    /// against a threshold (9,986 of 10,000 is 99.8, not 99.9); 0 with no samples.
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
    /// The caps declare no such touch source or book channel.
    Undeclared,
    /// The touch source and the book show different order channels (the interactive book
    /// includes RPI liquidity, bbo does not), so their touches need not agree.
    ChannelsDiffer,
    /// A bbo in this sequence domain has no sample point in the book's sequence.
    NoSamplePoint(SeqDomain),
}

impl fmt::Display for OracleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OracleError::Undeclared => f.write_str("touch source or book channel not declared"),
            OracleError::ChannelsDiffer => {
                f.write_str("touch source and book show different order channels")
            }
            OracleError::NoSamplePoint(domain) => write!(
                f,
                "no sample point in the book's sequence for a bbo in {domain:?}"
            ),
        }
    }
}

/// Compares every touch of `source` on `book`'s market in `samples` (other events, markets and
/// sources are not samples; Codex r4177861131) with `book`'s touch at the sample point the
/// source's sequence domain gives: the equal seq_no for [`SeqDomain::SharedWithBook`]. The
/// source and the book must show the same order channels (Codex r4177861133).
pub fn touch_agreement(
    md: &MdCaps,
    source: TouchSourceId,
    book: &DeltaBook,
    samples: &[(VenueMeta, MdEvent)],
) -> Result<Agreement, OracleError> {
    let touch_caps = md.touch_sources.get(usize::from(source.0));
    let book_caps = md.books.get(usize::from(book.book.0));
    let (Some(touch_caps), Some(book_caps)) = (touch_caps, book_caps) else {
        return Err(OracleError::Undeclared);
    };
    if touch_caps.includes_channels != book_caps.includes_channels {
        return Err(OracleError::ChannelsDiffer);
    }
    if touch_caps.seq_domain != SeqDomain::SharedWithBook {
        return Err(OracleError::NoSamplePoint(touch_caps.seq_domain));
    }
    let touches = samples.iter().filter_map(|(meta, ev)| match *ev {
        MdEvent::Touch {
            inst,
            bid,
            ask,
            source: from,
        } if (inst, from) == (book.inst, source) => Some((meta.venue_seq, Touch { bid, ask })),
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
