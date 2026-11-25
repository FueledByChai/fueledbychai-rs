//! The positions comparison and its re-read rule (FBC-hpg6, the owner's condition in decision
//! 0094: positions follow fills; decision 0155 items 2, 7 and 8).
//!
//! A session resyncs once per connection, so one with fills and no reconnect never reads the
//! venue's positions again after a fill. A consumer therefore reads the market's position itself
//! when it must know it (after the first fill a session sees, and at Stop once Stop's cancels are
//! answered), and this module compares what it read with the registry's position, exactly in
//! lots. The read is the consumer's, not this module's: it reads no venue (decision 0155 item 7).
//!
//! - **A reading** ([`Reading`]): when the answer landed, the venue's size of the market (or why
//!   it could not be read), and what the registry held as it landed: its position and the fills
//!   of ours applied ([`fills_in`]). It agrees when the venue's size was read and is exactly the
//!   registry's.
//! - **The re-read** ([`PositionWatch`]). A mismatch, or an answer that could not be read, is
//!   read once more after the consumer's wait, so a venue's position read lagging its fill stream
//!   (FBC-8xr's runs saw one trail the stream by up to 3.5 s) is told apart from a real
//!   disagreement (RB155-2 on PR #155): a re-read that agrees clears it ([`Verdict::Cleared`]),
//!   one that still disagrees persists ([`Verdict::Persists`]).
//! - **What follows is the consumer's.** A [`Verdict`] names no reaction: whether a persisting
//!   mismatch Stops the run, what it prints, and what it tells the owner to do are the
//!   consumer's (decision 0155 item 2).
//!
//! It reads no clock: each reading's time is the consumer's, on the shard's monotonic clock
//! ([`Link::now`], a stamp, or a consumer input's or timer's `at`; decision 0155 item 3), and the
//! re-read's wait is the consumer's, refused zero, with no default (decision 0155 item 8).
//!
//! [`Link::now`]: crate::link::Link::now

use std::fmt;
use std::time::Duration;

use fbc_core::{MonoNs, SignedLots};
use fbc_oms::PositionCheck;

use crate::link::Note;

/// One reading of the market's position: when the consumer's read landed, what the venue said of
/// the market (in lots, or why it could not be read), and what the registry held as the answer
/// landed: its position and how many fills of ours it had applied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reading {
    /// When the answer landed, on the consumer's monotonic clock.
    pub at: MonoNs,
    /// The venue's size of the market, or why it could not be read.
    pub venue: Result<SignedLots, String>,
    /// The registry's position on the market as the answer landed.
    pub registry: SignedLots,
    /// The fills of ours the registry had applied as the answer landed ([`fills_in`]).
    pub fills: usize,
}

impl Reading {
    /// Whether the venue's size was read and is exactly the registry's.
    pub fn agrees(&self) -> bool {
        self.venue.as_ref().is_ok_and(|v| *v == self.registry)
    }

    /// The reading as a consumer prints it, `read` naming the consumer's read of the venue: its
    /// time as `+Nms` (whole milliseconds of `at`), the venue's size or why it was unread, the
    /// registry's position and the fills applied.
    pub fn shown(&self, read: &str) -> String {
        let venue = match &self.venue {
            Ok(lots) => format!("{read} {} lots", lots.0),
            Err(why) => format!("{read} unread ({why})"),
        };
        format!(
            "+{}ms {venue}, the registry {} lots ({} fills applied)",
            self.at.0 / 1_000_000,
            self.registry.0,
            self.fills
        )
    }
}

/// What a comparison came to. It names no reaction: what follows is the consumer's.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// The first reading agreed.
    Agrees(Reading),
    /// The first reading disagreed (or was unread) and the re-read after the consumer's wait
    /// agreed: the venue's read lagged.
    Cleared(Reading, Reading),
    /// Both readings disagreed (or were unread).
    Persists(Reading, Reading),
}

impl Verdict {
    /// Whether the mismatch persisted through the re-read.
    pub fn persists(&self) -> bool {
        matches!(self, Verdict::Persists(..))
    }
}

/// What a [`PositionWatch`] asks for after a reading.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Next {
    /// The comparison came to its verdict.
    Done(Verdict),
    /// The reading disagreed or was unread: read once more at this time, the first reading's
    /// time plus the consumer's wait.
    ReadAgainAt(MonoNs),
}

/// A [`PositionWatch`] refused: its re-read's wait is zero, which would read again at once and
/// so never tell a lagging read from a real disagreement.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ZeroReread;

impl fmt::Display for ZeroReread {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("the position re-read's wait is zero")
    }
}

impl std::error::Error for ZeroReread {}

/// One comparison's re-read rule (module documentation): a first reading that agrees is done; one
/// that disagrees or is unread asks to be read once more after the consumer's wait, and the
/// re-read clears or persists. A consumer starts a watch for each comparison; one it abandons
/// part way (its read cut short) is dropped with it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PositionWatch {
    /// How long after a first reading that disagrees the re-read comes.
    reread_after: Duration,
    /// The first reading, once it disagreed or was unread.
    first: Option<Reading>,
}

impl PositionWatch {
    /// A watch re-reading `reread_after` after a first reading that disagrees or is unread, the
    /// consumer's (decision 0155 item 8). Refused when it is zero.
    pub fn new(reread_after: Duration) -> Result<PositionWatch, ZeroReread> {
        if reread_after.is_zero() {
            return Err(ZeroReread);
        }
        Ok(PositionWatch {
            reread_after,
            first: None,
        })
    }

    /// Judges `reading`: the first reading, done when it agrees and otherwise read again at its
    /// time plus the wait; the re-read, cleared when it agrees and otherwise persisting. Each
    /// time is the caller's: the watch reads no clock.
    pub fn judge(&mut self, reading: Reading) -> Next {
        match self.first.take() {
            None if reading.agrees() => Next::Done(Verdict::Agrees(reading)),
            None => {
                let again = reading.at + self.reread_after;
                self.first = Some(reading);
                Next::ReadAgainAt(again)
            }
            Some(first) if reading.agrees() => Next::Done(Verdict::Cleared(first, reading)),
            Some(first) => Next::Done(Verdict::Persists(first, reading)),
        }
    }
}

/// How many fills of ours `notes` show applied to the inventory.
pub fn fills_in(notes: &[Note]) -> usize {
    notes
        .iter()
        .filter(|n| matches!(n, Note::Fill { ours: true, .. }))
        .count()
}

/// Whether `notes` hold a fill of any kind: the session saw one.
pub fn fill_seen(notes: &[Note]) -> bool {
    notes.iter().any(|n| matches!(n, Note::Fill { .. }))
}

/// What the resyncs `notes` hold found wrong with the positions the registry holds: a market
/// whose venue position differs from its inventory (`Desync`), or could not be compared
/// (`Stale`, `Unsettled`), or a fill the resync could not place against its seed. A resync after
/// the first (a reconnect's) never overwrites the inventory, so a consumer cannot go on from one
/// of these; what it does is its own.
pub fn resync_disagreements(notes: &[Note]) -> Vec<String> {
    notes
        .iter()
        .filter_map(|n| match n {
            Note::Resynced { report, .. } => Some(report),
            _ => None,
        })
        .flat_map(|report| {
            let checks = report
                .checks
                .iter()
                .filter(|c| !matches!(c, PositionCheck::Agrees { .. }))
                .map(|c| format!("{c:?}"));
            let unsettled = report
                .unsettled
                .iter()
                .map(|(inst, _)| format!("Unsettled fill on instrument {}", inst.get()));
            checks.chain(unsettled).collect::<Vec<_>>()
        })
        .collect()
}
