//! FBC-3bq4's done line, the source's half (decisions 0006, 0007, 0054): `WallClockNonces`, the
//! production `NonceSource` for a venue whose nonce is a wall-clock timestamp near server time
//! (Hibachi: microseconds, unique per account, within a documented window of server time),
//! issues monotonic wall-clock microseconds over a clock the caller injects: a block's nonces
//! increase in item order; every nonce is above every earlier one when the clock stalls or steps
//! back; a source resumed from the last nonce a journal holds never reissues one; and a clock
//! fallen behind the last nonce further than the caller's window is refused, nothing reserved,
//! never a nonce outside the window. The window is the caller's value: the source has none of
//! its own. The session's half (an error the session surfaces, and a journaled run replayed
//! byte-identically without the clock) is in `exec_journal.rs`.
//!
//! The clock is a stub the test sets; no test reads the system clock or reaches a venue.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use fbc_core::{NonceBlock, NonceRefused, NonceSource, WallNs};
use fbc_journal::{NonceSourceId, Record};
use fbc_runtime::{WallClock, WallClockNonces, ZeroWindow, last_nonce};

/// A wall time, in microseconds since the Unix epoch, near 2026-10-19.
const T_US: u64 = 1_792_000_000_000_000;
/// Hibachi's documented window, as a consumer would pass it.
const WINDOW: Duration = Duration::from_secs(15);

/// A wall clock the test sets, read only while it is set: a read of a cleared clock panics.
#[derive(Clone, Default)]
struct Stub(Arc<Mutex<Option<i64>>>);

impl Stub {
    /// A clock reading `us` microseconds since the epoch.
    fn at_us(us: u64) -> Stub {
        let stub = Stub::default();
        stub.set_us(us);
        stub
    }

    fn set_us(&self, us: u64) {
        self.set_ns(i64::try_from(us).unwrap() * 1_000);
    }

    fn set_ns(&self, ns: i64) {
        *self.0.lock().unwrap() = Some(ns);
    }
}

impl WallClock for Stub {
    fn now(&self) -> WallNs {
        WallNs(
            self.0
                .lock()
                .unwrap()
                .expect("the clock was read while cleared"),
        )
    }
}

fn source(clock: &Stub) -> WallClockNonces<Stub> {
    WallClockNonces::new(clock.clone(), WINDOW).unwrap()
}

fn ok(block: Result<NonceBlock, NonceRefused>) -> Vec<u64> {
    block.unwrap().as_slice().to_vec()
}

/// A block's nonces start at the clock's microsecond and increase by one in item order; the
/// next block starts at the clock again once it has moved past the last nonce.
#[test]
fn a_blocks_nonces_increase_in_item_order_from_the_clocks_microsecond() {
    let clock = Stub::at_us(T_US);
    let mut nonces = source(&clock);
    assert_eq!(nonces.last(), None);
    assert_eq!(ok(nonces.try_reserve(3)), [T_US, T_US + 1, T_US + 2]);
    assert_eq!(nonces.last(), Some(T_US + 2));
    clock.set_us(T_US + 1_000);
    assert_eq!(ok(nonces.try_reserve(1)), [T_US + 1_000]);
    // Sub-microsecond time is dropped, never rounded up.
    clock.set_ns(i64::try_from(T_US + 2_000).unwrap() * 1_000 + 999);
    assert_eq!(ok(nonces.try_reserve(2)), [T_US + 2_000, T_US + 2_001]);
}

/// A clock that stalls, or steps back by less than the window, still gets nonces above every
/// earlier one, increasing in item order: the source issues max(clock, last + 1).
#[test]
fn every_nonce_is_above_every_earlier_one_when_the_clock_stalls_or_steps_back() {
    let clock = Stub::at_us(T_US);
    let mut nonces = source(&clock);
    let mut issued = ok(nonces.try_reserve(2));
    // Stalled: the same microsecond, twice more.
    issued.extend(ok(nonces.try_reserve(3)));
    issued.extend(ok(nonces.try_reserve(1)));
    // Stepped back by 5 s, within the 15 s window.
    clock.set_us(T_US - 5_000_000);
    issued.extend(ok(nonces.try_reserve(4)));
    // Back to where it was, then ahead.
    clock.set_us(T_US);
    issued.extend(ok(nonces.try_reserve(1)));
    clock.set_us(T_US + 7);
    issued.extend(ok(nonces.try_reserve(2)));
    let expected: Vec<u64> = (0..13).map(|i| T_US + i).collect();
    assert_eq!(issued, expected);
    assert!(issued.windows(2).all(|w| w[0] < w[1]), "{issued:?}");
    assert_eq!(nonces.last(), Some(T_US + 12));
}

/// The trait's infallible `reserve` gives the same nonces as `try_reserve`.
#[test]
fn reserve_gives_the_same_nonces() {
    let clock = Stub::at_us(T_US);
    let mut nonces = source(&clock);
    assert_eq!(nonces.reserve(2).as_slice(), [T_US, T_US + 1]);
    assert_eq!(nonces.reserve(1).as_slice(), [T_US + 2]);
}

/// A source built from the last nonce an earlier journal holds, for its own source number,
/// never reissues one, though its clock reads earlier than that nonce (a restart within the
/// same microseconds, or a clock stepped back across it); with no nonce journaled it starts at
/// its clock.
#[test]
fn a_source_resumed_from_a_journals_last_nonce_never_reissues_one() {
    let mine = NonceSourceId(7);
    let other = NonceSourceId(8);
    let records = [
        Record::Nonce {
            source: mine,
            value: T_US + 40,
        },
        Record::Nonce {
            source: other,
            value: T_US + 9_000,
        },
        Record::Nonce {
            source: mine,
            value: T_US + 41,
        },
    ];
    let last = last_nonce(&records, mine).unwrap();
    assert_eq!(last, T_US + 41);
    assert_eq!(last_nonce(&records, NonceSourceId(9)), None);
    assert_eq!(last_nonce(&[], mine), None);

    // The clock reads before the journal's last nonce, within the window.
    let clock = Stub::at_us(T_US + 3);
    let mut resumed = WallClockNonces::resuming(clock.clone(), WINDOW, last).unwrap();
    assert_eq!(resumed.last(), Some(last));
    let next = ok(resumed.try_reserve(3));
    assert_eq!(next, [T_US + 42, T_US + 43, T_US + 44]);
    let journaled: Vec<u64> = records
        .iter()
        .filter_map(|r| match r {
            Record::Nonce { source, value } if *source == mine => Some(*value),
            _ => None,
        })
        .collect();
    assert!(next.iter().all(|n| journaled.iter().all(|j| n > j)));
}

/// A clock fallen behind the last nonce by more than the window is refused: nothing is
/// reserved, so the next nonce once the clock is back within the window is still the one after
/// the last. A block whose last nonce would pass the window is refused whole, not cut short;
/// a block that reaches exactly the window's edge is issued.
#[test]
fn a_clock_fallen_behind_its_last_nonce_further_than_the_window_is_refused() {
    let window_us = u64::try_from(WINDOW.as_micros()).unwrap();
    let clock = Stub::at_us(T_US);
    let mut nonces = source(&clock);
    assert_eq!(ok(nonces.try_reserve(1)), [T_US]);

    // 20 s back: the next nonce would lie 20 s ahead of the clock.
    clock.set_us(T_US - 20_000_000);
    let refused = nonces.try_reserve(1).unwrap_err();
    assert_eq!(
        refused,
        NonceRefused::OutsideWindow {
            ahead: Duration::from_micros(20_000_001),
            window: WINDOW,
        }
    );
    assert_eq!(nonces.last(), Some(T_US));
    // The infallible path gives no nonce either: an empty block.
    assert!(nonces.reserve(1).is_empty());
    assert_eq!(nonces.last(), Some(T_US));

    // Back to exactly the window's edge for a block of 3: its last nonce, T + 3, lies the
    // whole window ahead of the clock, and is issued.
    clock.set_us(T_US + 3 - window_us);
    assert_eq!(ok(nonces.try_reserve(3)), [T_US + 1, T_US + 2, T_US + 3]);
    // One more nonce would pass the edge by a microsecond: refused.
    let refused = nonces.try_reserve(1).unwrap_err();
    let ahead = Duration::from_micros(window_us + 1);
    assert_eq!(
        refused,
        NonceRefused::OutsideWindow {
            ahead,
            window: WINDOW
        }
    );
    assert_eq!(nonces.last(), Some(T_US + 3));
    // A block is refused whole, never cut short: with the clock a microsecond on, there is room
    // for one more nonce, and a block of two gets none.
    clock.set_us(T_US + 4 - window_us);
    assert_eq!(ok(nonces.try_reserve(1)), [T_US + 4]);
    clock.set_us(T_US + 5 - window_us);
    let refused = nonces.try_reserve(2).unwrap_err();
    assert!(matches!(refused, NonceRefused::OutsideWindow { .. }));
    assert_eq!(nonces.last(), Some(T_US + 4));

    // A source resumed past its clock by more than the window refuses its first block.
    let behind = Stub::at_us(T_US);
    let mut resumed = WallClockNonces::resuming(behind, WINDOW, T_US + window_us + 1).unwrap();
    let refused = resumed.try_reserve(1).unwrap_err();
    assert_eq!(
        refused,
        NonceRefused::OutsideWindow {
            ahead: Duration::from_micros(window_us + 2),
            window: WINDOW,
        }
    );
}

/// The window is the caller's, required: one under a microsecond is refused when the source is
/// built, and a larger one is kept as given.
#[test]
fn the_window_is_the_callers_and_a_zero_one_is_refused() {
    let clock = Stub::at_us(T_US);
    let zero = WallClockNonces::new(clock.clone(), Duration::ZERO);
    assert_eq!(zero.err(), Some(ZeroWindow));
    let sub_micro = WallClockNonces::resuming(clock.clone(), Duration::from_nanos(999), T_US);
    assert_eq!(sub_micro.err(), Some(ZeroWindow));
    assert_eq!(source(&clock).window(), WINDOW);
    let wide = Duration::from_secs(60);
    let mut nonces = WallClockNonces::resuming(clock.clone(), wide, T_US + 30_000_000).unwrap();
    assert_eq!(nonces.window(), wide);
    assert_eq!(ok(nonces.try_reserve(1)), [T_US + 30_000_001]);
}

/// A clock before the Unix epoch gives no nonce; nor does a block that would pass `u64::MAX`.
#[test]
fn a_clock_before_the_epoch_and_an_exhausted_sequence_are_refused() {
    let clock = Stub::default();
    clock.set_ns(-1);
    let mut nonces = source(&clock);
    assert_eq!(nonces.try_reserve(1), Err(NonceRefused::ClockBeforeEpoch));
    assert_eq!(nonces.last(), None);

    let top = Stub::at_us(T_US);
    let mut nonces = WallClockNonces::resuming(top.clone(), WINDOW, u64::MAX - 1).unwrap();
    // The clock is far behind too, but running out is what is wrong.
    assert_eq!(nonces.try_reserve(2), Err(NonceRefused::Exhausted));
    assert_eq!(nonces.last(), Some(u64::MAX - 1));
}

/// Each refusal names what was wrong, with the figures a log needs and no nonce.
#[test]
fn a_refusal_says_what_was_wrong() {
    let cases = [
        (
            NonceRefused::OutsideWindow {
                ahead: Duration::from_micros(20_000_001),
                window: WINDOW,
            },
            "the next nonce would lie 20.000001s ahead of the clock, past the 15s window",
        ),
        (
            NonceRefused::ClockBeforeEpoch,
            "the clock reads before the Unix epoch",
        ),
        (NonceRefused::Exhausted, "no nonce is left below u64::MAX"),
    ];
    for (refused, text) in cases {
        assert_eq!(refused.to_string(), text);
    }
    assert_eq!(
        ZeroWindow.to_string(),
        "a nonce window under one microsecond admits no nonce"
    );
}

/// An empty reservation reads no clock and reserves nothing.
#[test]
fn an_empty_reservation_reads_no_clock() {
    let clock = Stub::default();
    let mut nonces = source(&clock);
    assert!(ok(nonces.try_reserve(0)).is_empty());
    assert_eq!(nonces.last(), None);
}
