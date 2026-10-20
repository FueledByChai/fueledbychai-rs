//! The production nonce source for a venue whose nonce is a wall-clock timestamp near server
//! time (FBC-3bq4, decision 0114): [`WallClockNonces`].
//!
//! Hibachi takes a microsecond Unix timestamp within a documented window of server time,
//! unique per account; it states no monotonic rule. Issuing monotonic wall-clock microseconds,
//! each `max(clock, last + 1)`, as the venue's `NonceScope::PerAccountMonotonic` declares,
//! satisfies that rule conservatively: two nonces never repeat within a process, nor across a
//! restart once the source is resumed from the last nonce its journal holds ([`last_nonce`]).
//! A nonce outside the window would be refused by the venue, so the source refuses first: a
//! block whose last nonce would lie further ahead of its clock than the caller's window is
//! [`NonceRefused::OutsideWindow`], nothing reserved, and the order-entry session ends with
//! that refusal as its error. The window is the caller's value, required: the source has none
//! of its own.
//!
//! The source reads its clock through [`WallClock`], injected, so tests drive it with a stub.
//! It is not journaled itself: the order-entry session journals every nonce it reserves and
//! every context it gives its codec, and replay reads them back without consulting any clock.
//! A clock running ahead of server time cannot be seen from here; a consumer that measures its
//! skew against the venue's time is beyond this source.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fbc_core::{NonceBlock, NonceRefused, NonceSource, WallNs};
use fbc_journal::{Marker, NonceSourceId, Record};

/// A wall clock: nanoseconds since the Unix epoch.
pub trait WallClock: Send {
    fn now(&self) -> WallNs;
}

/// The system's realtime clock.
#[derive(Copy, Clone, Debug, Default)]
pub struct SystemWallClock;

impl WallClock for SystemWallClock {
    fn now(&self) -> WallNs {
        // A clock before the epoch reads negative; one past i64 nanoseconds (the year 2262)
        // saturates.
        let ns = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(after) => i64::try_from(after.as_nanos()).unwrap_or(i64::MAX),
            Err(before) => i64::try_from(before.duration().as_nanos()).map_or(i64::MIN, |n| -n),
        };
        WallNs(ns)
    }
}

/// A nonce window under one microsecond, which admits no nonce of a microsecond source.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct ZeroWindow;

impl std::fmt::Display for ZeroWindow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a nonce window under one microsecond admits no nonce")
    }
}

impl std::error::Error for ZeroWindow {}

/// Monotonic wall-clock microseconds: each nonce `max(clock, last + 1)`, a block's increasing
/// by one in item order, refused whole when its last nonce would lie further ahead of the clock
/// than the caller's window. The module docs say why.
#[derive(Debug)]
pub struct WallClockNonces<C> {
    clock: C,
    window: Duration,
    window_us: u64,
    last: Option<u64>,
}

impl<C: WallClock> WallClockNonces<C> {
    /// A source reading `clock`, refusing a nonce further than `window` ahead of it, that has
    /// issued none. For a process whose journal holds no nonce of the sequence.
    pub fn new(clock: C, window: Duration) -> Result<WallClockNonces<C>, ZeroWindow> {
        let window_us = u64::try_from(window.as_micros()).unwrap_or(u64::MAX);
        if window_us == 0 {
            return Err(ZeroWindow);
        }
        Ok(WallClockNonces {
            clock,
            window,
            window_us,
            last: None,
        })
    }

    /// [`new`](WallClockNonces::new), resumed after `last`, the last nonce an earlier journal
    /// holds for the sequence ([`last_nonce`]): every nonce it issues is above `last`.
    pub fn resuming(
        clock: C,
        window: Duration,
        last: u64,
    ) -> Result<WallClockNonces<C>, ZeroWindow> {
        let mut source = WallClockNonces::new(clock, window)?;
        source.last = Some(last);
        Ok(source)
    }

    /// The last nonce issued, or resumed after; `None` before the first.
    pub fn last(&self) -> Option<u64> {
        self.last
    }

    /// The window the caller gave.
    pub fn window(&self) -> Duration {
        self.window
    }
}

impl<C: WallClock> NonceSource for WallClockNonces<C> {
    /// [`try_reserve`](NonceSource::try_reserve)'s nonces, or an empty block when it refuses.
    fn reserve(&mut self, len: u16) -> NonceBlock {
        self.try_reserve(len).unwrap_or(NonceBlock::EMPTY)
    }

    fn try_reserve(&mut self, len: u16) -> Result<NonceBlock, NonceRefused> {
        if len == 0 {
            return Ok(NonceBlock::EMPTY);
        }
        let now_ns = self.clock.now().0;
        if now_ns < 0 {
            return Err(NonceRefused::ClockBeforeEpoch);
        }
        // Whole microseconds: the fraction is dropped, never rounded up.
        let now_us = now_ns.cast_unsigned() / 1_000;
        let first = match self.last {
            None => now_us,
            Some(last) => now_us.max(last.checked_add(1).ok_or(NonceRefused::Exhausted)?),
        };
        let block_last = first
            .checked_add(u64::from(len) - 1)
            .ok_or(NonceRefused::Exhausted)?;
        let ahead_us = block_last - now_us;
        if ahead_us > self.window_us {
            let ahead = Duration::from_micros(ahead_us);
            let window = self.window;
            return Err(NonceRefused::OutsideWindow { ahead, window });
        }
        let block = NonceBlock::consecutive(first, len).ok_or(NonceRefused::Exhausted)?;
        self.last = Some(block_last);
        Ok(block)
    }
}

/// The last nonce `records`, an earlier journal's records in journal order, hold for source
/// `source`: the largest, so a source resumed after it ([`WallClockNonces::resuming`]) never
/// reissues one. `Ok(None)` when they hold none.
///
/// A journal that dropped records (a `Degraded` marker, recovered or not) may have lost the
/// last nonce sent, since a dropped Safety record's request is sent all the same (0006): it is
/// refused, [`JournalGap`], naming the first gap, rather than answered with the largest that
/// survived (Codex P2 r4321805846 on PR #187). A consumer then resumes after a bound of its
/// own: its clock plus the window is past every nonce a source within the window issued, and
/// the resumed source refuses until the clock has moved a window on. Records queued but never
/// written when a process dies leave no marker; a source's nonces track its clock, so a restart
/// whose clock has moved on past them is unaffected.
pub fn last_nonce<'a>(
    records: impl IntoIterator<Item = &'a Record>,
    source: NonceSourceId,
) -> Result<Option<u64>, JournalGap> {
    let mut last = None;
    for record in records {
        match record {
            Record::Nonce { source: s, value } if *s == source => {
                last = last.max(Some(*value));
            }
            Record::Marker(Marker::Degraded { from_seq, dropped }) => {
                let (from_seq, dropped) = (*from_seq, *dropped);
                return Err(JournalGap { from_seq, dropped });
            }
            _ => {}
        }
    }
    Ok(last)
}

/// A journal's records hold a gap, `dropped` records from sequence `from_seq`, so they name
/// no last nonce to resume after ([`last_nonce`]).
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct JournalGap {
    pub from_seq: u64,
    pub dropped: u64,
}

impl std::fmt::Display for JournalGap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let JournalGap { from_seq, dropped } = self;
        write!(
            f,
            "the journal dropped {dropped} records from sequence {from_seq}, so its last nonce \
             may be lost"
        )
    }
}

impl std::error::Error for JournalGap {}
