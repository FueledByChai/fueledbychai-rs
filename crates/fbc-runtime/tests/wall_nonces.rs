//! FBC-3bq4's done line, the source's half (decision 0114): `WallClockNonces`, the
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
//! FBC-ak22's done line, the source's half (decision 0133, superseding 0114's item 2): the
//! source issues a nonce only at or below a lease ceiling already persisted, renewed ahead of use
//! by a writer of its own, so a source resumed after a restart issues none at or below a nonce
//! the earlier process sent while its journal record was still unwritten, whatever the restart's
//! clock reads; and no reservation waits for a lease write: one held mid-write leaves every
//! reservation answered at once, issued under the ceiling already persisted or refused past it.
//! The session's half (a cancel sent while its lease write and its journal record are both
//! unwritten, and the restart after it) is in `exec_journal.rs`.
//!
//! The clock is a stub the test sets; no test reads the system clock or reaches a venue, but
//! the one that says so.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use fbc_core::{NonceBlock, NonceRefused, NonceSource, WallNs};
use fbc_journal::{Marker, NonceSourceId, Record};
use fbc_runtime::{
    JournalGap, LeaseFile, LeaseStore, NonceLease, NonceStartError, WallClock, WallClockNonces,
    last_nonce,
};

/// A wall time, in microseconds since the Unix epoch, 2026-10-14T17:46:40Z.
const T_US: u64 = 1_792_000_000_000_000;
/// Hibachi's documented window, as a consumer would pass it.
const WINDOW: Duration = Duration::from_secs(15);
/// How far ahead of the next nonce each lease ceiling reaches, as a consumer would pass it.
const SPAN: Duration = Duration::from_secs(4);
/// [`SPAN`] in microseconds.
const SPAN_US: u64 = 4_000_000;

/// A wall clock the test sets, read only while it is set: a read of a cleared clock panics.
#[derive(Clone, Debug, Default)]
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

    fn clear(&self) {
        *self.0.lock().unwrap() = None;
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

/// A lease store the test watches: every ceiling it persisted, in order (the first the one it
/// was built holding, if any), its writes held while it is held and failing while it is told to
/// fail.
#[derive(Clone, Default)]
struct Store(Arc<StoreState>);

#[derive(Default)]
struct StoreState {
    persisted: Mutex<Vec<u64>>,
    held: Mutex<bool>,
    released: Condvar,
    writing: AtomicBool,
    failing: AtomicBool,
}

impl Store {
    /// A store an earlier process left holding `ceiling`.
    fn holding(ceiling: u64) -> Store {
        let store = Store::default();
        store.0.persisted.lock().unwrap().push(ceiling);
        store
    }

    /// Every ceiling persisted so far, in order.
    fn persisted(&self) -> Vec<u64> {
        self.0.persisted.lock().unwrap().clone()
    }

    /// Holds every write from now on until [`release`](Store::release).
    fn hold(&self) {
        *self.0.held.lock().unwrap() = true;
    }

    fn release(&self) {
        *self.0.held.lock().unwrap() = false;
        self.0.released.notify_all();
    }

    /// Whether a write is under way, held or not yet returned.
    fn writing(&self) -> bool {
        self.0.writing.load(Ordering::SeqCst)
    }

    fn fail(&self, failing: bool) {
        self.0.failing.store(failing, Ordering::SeqCst);
    }
}

impl LeaseStore for Store {
    fn ceiling(&self) -> Option<u64> {
        self.0.persisted.lock().unwrap().last().copied()
    }

    fn persist(&mut self, ceiling: u64) -> io::Result<()> {
        let state = &self.0;
        state.writing.store(true, Ordering::SeqCst);
        let mut held = state.held.lock().unwrap();
        while *held {
            held = state.released.wait(held).unwrap();
        }
        drop(held);
        let done = if state.failing.load(Ordering::SeqCst) {
            Err(io::Error::other("the test's disk refused the write"))
        } else {
            state.persisted.lock().unwrap().push(ceiling);
            Ok(())
        };
        state.writing.store(false, Ordering::SeqCst);
        done
    }
}

/// A lease over `store` reaching [`SPAN`] ahead.
fn lease(store: &Store) -> NonceLease<Store> {
    NonceLease {
        store: store.clone(),
        span: SPAN,
    }
}

fn source(clock: &Stub) -> WallClockNonces<Stub> {
    WallClockNonces::new(clock.clone(), WINDOW, lease(&Store::default())).unwrap()
}

/// Waits, on the test's thread, until `done`, which the lease's writer brings about on its own;
/// panics after ten seconds.
fn wait_for(done: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(Instant::now() < deadline, "never happened");
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// An empty directory of this test's own under the target directory.
fn fresh_dir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join("wall_nonces")
        .join(name);
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
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
    let last = last_nonce(&records, mine).unwrap().unwrap();
    assert_eq!(last, T_US + 41);
    assert_eq!(last_nonce(&records, NonceSourceId(9)), Ok(None));
    assert_eq!(last_nonce(&[], mine), Ok(None));

    // The clock reads before the journal's last nonce, within the window.
    let clock = Stub::at_us(T_US + 3);
    let mut resumed =
        WallClockNonces::resuming(clock.clone(), WINDOW, last, lease(&Store::default())).unwrap();
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
    let mut resumed = WallClockNonces::resuming(
        behind,
        WINDOW,
        T_US + window_us + 1,
        lease(&Store::default()),
    )
    .unwrap();
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
    let store = Store::default();
    let zero = WallClockNonces::new(clock.clone(), Duration::ZERO, lease(&store));
    assert!(matches!(zero.err(), Some(NonceStartError::ZeroWindow)));
    let sub_micro = WallClockNonces::resuming(
        clock.clone(),
        Duration::from_nanos(999),
        T_US,
        lease(&store),
    );
    assert!(matches!(sub_micro.err(), Some(NonceStartError::ZeroWindow)));
    // Refused before anything was persisted.
    assert_eq!(store.persisted(), []);
    assert_eq!(source(&clock).window(), WINDOW);
    let wide = Duration::from_secs(60);
    let mut nonces =
        WallClockNonces::resuming(clock.clone(), wide, T_US + 30_000_000, lease(&store)).unwrap();
    assert_eq!(nonces.window(), wide);
    assert_eq!(ok(nonces.try_reserve(1)), [T_US + 30_000_001]);
}

/// The lease's span is the caller's, required, and lies within the window: a ceiling a span
/// ahead must leave a restart a nonce the venue takes. A span under a microsecond, or past the
/// window, is refused when the source is built, nothing persisted; one of the window's length is
/// kept as given.
#[test]
fn the_span_is_the_callers_and_lies_within_the_window() {
    let clock = Stub::at_us(T_US);
    let store = Store::default();
    for span in [
        Duration::ZERO,
        Duration::from_nanos(999),
        WINDOW + Duration::from_micros(1),
    ] {
        let lease = NonceLease {
            store: store.clone(),
            span,
        };
        let refused = WallClockNonces::new(clock.clone(), WINDOW, lease).err();
        match refused {
            Some(NonceStartError::Span { span: s, window }) => {
                assert_eq!((s, window), (span, WINDOW))
            }
            other => panic!("{span:?}: {other:?}"),
        }
    }
    assert_eq!(store.persisted(), []);
    let whole = NonceLease {
        store: store.clone(),
        span: WINDOW,
    };
    let nonces = WallClockNonces::new(clock, WINDOW, whole).unwrap();
    assert_eq!(nonces.span(), WINDOW);
    assert_eq!(store.persisted(), [T_US + 15_000_000]);
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
    let mut nonces =
        WallClockNonces::resuming(top.clone(), WINDOW, u64::MAX - 1, lease(&Store::default()))
            .unwrap();
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
        (
            NonceRefused::PastLease {
                past: Duration::from_micros(2),
            },
            "the next nonce would lie 2µs past the nonce lease persisted so far",
        ),
    ];
    for (refused, text) in cases {
        assert_eq!(refused.to_string(), text);
    }
    let starts = [
        (
            NonceStartError::ZeroWindow,
            "a nonce window under one microsecond admits no nonce",
        ),
        (
            NonceStartError::Span {
                span: Duration::from_secs(16),
                window: WINDOW,
            },
            "a nonce lease's span of 16s lies outside the range from one microsecond to the 15s \
             window",
        ),
        (
            NonceStartError::Lease(io::Error::other("the disk refused the write")),
            "the nonce lease's first ceiling was not persisted: the disk refused the write",
        ),
    ];
    for (refused, text) in starts {
        assert_eq!(refused.to_string(), text);
    }
}

/// An empty reservation reads no clock and reserves nothing. (The source reads its clock when it
/// is built, for its lease's first ceiling; the lease's writer, an hour's span leaving it no
/// renewal to make, reads none while the test runs, and none once the source is dropped.)
#[test]
fn an_empty_reservation_reads_no_clock() {
    let hour = Duration::from_secs(3_600);
    let clock = Stub::at_us(T_US);
    let lease = NonceLease {
        store: Store::default(),
        span: hour,
    };
    let mut nonces = WallClockNonces::new(clock.clone(), hour, lease).unwrap();
    clock.clear();
    assert!(ok(nonces.try_reserve(0)).is_empty());
    assert_eq!(nonces.last(), None);
}

/// The system clock reads after 2026-09-14, a month before this file was written, and a source
/// over it issues a nonce past that many microseconds since the epoch.
#[test]
fn the_system_wall_clock_reads_the_realtime_clock() {
    let written_us: u64 = T_US - 30 * 86_400 * 1_000_000;
    let now = fbc_runtime::SystemWallClock.now();
    assert!(
        now.0 > i64::try_from(written_us).unwrap() * 1_000,
        "{now:?}"
    );
    let lease = lease(&Store::default());
    let mut nonces = WallClockNonces::new(fbc_runtime::SystemWallClock, WINDOW, lease).unwrap();
    let nonce = ok(nonces.try_reserve(1))[0];
    assert!(nonce > written_us, "{nonce}");
}

/// Codex P2 r4321805846 on PR #187: a journal that dropped records (a `Degraded` marker, even
/// one later `Recovered`) may have lost the last nonce sent, so it names no last nonce to resume
/// after: `last_nonce` refuses it, naming the gap, rather than return the largest that survived.
/// A consumer then resumes after a bound of its own: its clock plus the window is past every
/// nonce the source could have issued, and the resumed source refuses until the clock has
/// moved a window on.
#[test]
fn a_journal_that_dropped_records_names_no_last_nonce() {
    let mine = NonceSourceId(7);
    let nonce = |value| Record::Nonce {
        source: mine,
        value,
    };
    let gap = Marker::Degraded {
        from_seq: 12,
        dropped: 3,
    };
    let records = [
        nonce(T_US + 40),
        Record::Marker(gap),
        Record::Marker(Marker::Recovered),
        nonce(T_US + 41),
    ];
    let refused = last_nonce(&records, mine);
    assert_eq!(
        refused,
        Err(JournalGap {
            from_seq: 12,
            dropped: 3
        })
    );
    assert_eq!(
        refused.unwrap_err().to_string(),
        "the journal dropped 3 records from sequence 12, so its last nonce may be lost"
    );
    // A gap refuses whatever source is asked for.
    assert!(last_nonce(&records, NonceSourceId(9)).is_err());

    // Resuming after the clock plus the window: refused until the clock has moved past it.
    let window_us = u64::try_from(WINDOW.as_micros()).unwrap();
    let clock = Stub::at_us(T_US);
    let bound = T_US + window_us;
    let mut resumed =
        WallClockNonces::resuming(clock.clone(), WINDOW, bound, lease(&Store::default())).unwrap();
    assert!(resumed.try_reserve(1).is_err());
    clock.set_us(T_US + 1);
    assert_eq!(ok(resumed.try_reserve(1)), [bound + 1]);
}

/// The source names the nonce every later one lies above, for the session to journal when a
/// journal is set (RB187-1 on PR #187): none before the first, the last issued, or the one it
/// resumed after; a source that names none by default, as the test counters do.
#[test]
fn the_source_names_its_high_water() {
    let clock = Stub::at_us(T_US);
    let mut fresh = source(&clock);
    assert_eq!(fresh.high_water(), None);
    ok(fresh.try_reserve(2));
    assert_eq!(fresh.high_water(), Some(T_US + 1));
    let resumed =
        WallClockNonces::resuming(clock.clone(), WINDOW, T_US + 9, lease(&Store::default()))
            .unwrap();
    assert_eq!(resumed.high_water(), Some(T_US + 9));
    // A source over a store an earlier process left holding a ceiling names that ceiling: it
    // resumed after it, the journal's last nonce below it.
    let leased = Store::holding(T_US + 50);
    let after = WallClockNonces::resuming(clock, WINDOW, T_US + 9, lease(&leased)).unwrap();
    assert_eq!(after.high_water(), Some(T_US + 50));
    struct Plain;
    impl NonceSource for Plain {
        fn reserve(&mut self, _: u16) -> NonceBlock {
            NonceBlock::EMPTY
        }
    }
    assert_eq!(Plain.high_water(), None);
}

// ---------------------------------------------------------------------------------------------
// FBC-ak22: the lease (decision 0133).
// ---------------------------------------------------------------------------------------------

/// FBC-ak22's done line, the restart: a process sends three nonces, the third while its journal
/// record is still queued, and dies before the record is written, so the journal's last nonce is
/// below the one it sent. The journal alone would have a restart reissue it. A source resumed
/// from that journal and the lease file the process left issues no nonce at or below the sent
/// one, whatever the restart's clock reads: a clock stepped back an hour, past the window by a
/// microsecond, or at the window's edge, within it, at the sent nonce or past it. Each restart
/// persists its own first ceiling, a span ahead, before it issues anything.
#[test]
fn a_source_resumed_after_a_nonce_its_journal_never_wrote_never_reissues_it_whatever_its_clock_reads()
 {
    let window_us = u64::try_from(WINDOW.as_micros()).unwrap();
    let dir = fresh_dir("resumed_after_an_unwritten_nonce");
    let path = dir.join("account.lease");
    let mine = NonceSourceId(7);

    // The first process: a lease file of its own, persisted before its first nonce.
    let clock = Stub::at_us(T_US);
    let leased = NonceLease {
        store: LeaseFile::create(&path).unwrap(),
        span: SPAN,
    };
    let mut first = WallClockNonces::new(clock.clone(), WINDOW, leased).unwrap();
    let on_disk = LeaseFile::open(&path).unwrap().ceiling().unwrap();
    assert_eq!(on_disk, T_US + SPAN_US);
    let sent = ok(first.try_reserve(3));
    assert_eq!(sent, [T_US, T_US + 1, T_US + 2]);
    let last_sent = T_US + 2;
    assert!(sent.iter().all(|n| *n <= on_disk));
    // Its journal wrote the first two records; the third was still queued when it died.
    let written = [
        Record::Nonce {
            source: mine,
            value: T_US,
        },
        Record::Nonce {
            source: mine,
            value: T_US + 1,
        },
    ];
    drop(first);
    let last = last_nonce(&written, mine).unwrap().unwrap();
    assert_eq!(last, T_US + 1);
    assert!(last < last_sent);

    // The journal alone would reissue it: a source resumed after the journal's last nonce, over
    // a store that never held the lease, issues the sent nonce again at the first clock.
    let lost = lease(&Store::default());
    let mut journal_only = WallClockNonces::resuming(clock, WINDOW, last, lost).unwrap();
    assert_eq!(ok(journal_only.try_reserve(1)), [last_sent]);

    // With the lease file, whatever the restart's clock reads.
    let clocks = [
        T_US - 3_600_000_000,
        on_disk + 1 - window_us - 1,
        on_disk + 1 - window_us,
        T_US - 1_000_000,
        T_US,
        last,
        last_sent,
        last_sent + 1,
        on_disk,
        T_US + 20_000_000,
    ];
    for (i, at) in clocks.into_iter().enumerate() {
        let copy = dir.join(format!("restart-{i}.lease"));
        fs::copy(&path, &copy).unwrap();
        let lease = NonceLease {
            store: LeaseFile::open(&copy).unwrap(),
            span: SPAN,
        };
        let restart = Stub::at_us(at);
        let mut resumed = WallClockNonces::resuming(restart.clone(), WINDOW, last, lease).unwrap();
        // Resumed after the lease's ceiling, above the journal's last nonce, with its own first
        // ceiling persisted a span ahead.
        assert_eq!(resumed.last(), Some(on_disk), "clock {at}");
        let leased = LeaseFile::open(&copy).unwrap().ceiling().unwrap();
        assert_eq!(leased, at.max(on_disk) + SPAN_US, "clock {at}");
        let next = at.max(on_disk + 1);
        assert!(next > last_sent);
        let first = resumed.try_reserve(1);
        if next - at > window_us {
            assert!(
                matches!(first, Err(NonceRefused::OutsideWindow { .. })),
                "clock {at}: {first:?}"
            );
            assert_eq!(resumed.last(), Some(on_disk));
        } else {
            assert_eq!(ok(first), [next], "clock {at}");
            // The clock moved on to it: the next block follows it, still above the sent nonce.
            restart.set_us(next);
            assert_eq!(ok(resumed.try_reserve(3)), [next + 1, next + 2, next + 3]);
        }
    }
    // The clocks stepped back past the window were refused; every other one issued.
    let refused = |at: u64| (on_disk + 1).saturating_sub(at) > window_us;
    assert_eq!(clocks.iter().filter(|at| refused(**at)).count(), 2);
}

/// The source persists its first ceiling, a span ahead of its clock, when it is built, before
/// it issues anything; it issues a nonce only at or below a ceiling already persisted; and once
/// half a span or less is left under it, its writer persists the next, a span ahead of the
/// clock or of the last nonce, whichever is later, so the clock walking on keeps finding room.
#[test]
fn every_nonce_lies_under_a_ceiling_persisted_before_it_and_the_lease_is_renewed_ahead_of_use() {
    let clock = Stub::at_us(T_US);
    let store = Store::default();
    let mut nonces = WallClockNonces::new(clock.clone(), WINDOW, lease(&store)).unwrap();
    assert_eq!(store.persisted(), [T_US + SPAN_US]);
    assert_eq!(nonces.ceiling(), T_US + SPAN_US);
    assert_eq!(nonces.span(), SPAN);
    let mut issued = Vec::new();
    for step in 0..12 {
        let now = T_US + step * SPAN_US / 4;
        clock.set_us(now);
        let ceiling = nonces.ceiling();
        let block = ok(nonces.try_reserve(2));
        assert!(block.iter().all(|n| *n <= ceiling), "{block:?} {ceiling}");
        assert!(store.persisted().contains(&ceiling));
        issued.extend(block);
        // Half a span or less left: renewed a span ahead of the block's last nonce.
        let base = now.max(*issued.last().unwrap());
        wait_for(|| nonces.ceiling() > base + SPAN_US / 2);
        assert_eq!(nonces.ceiling(), *store.persisted().last().unwrap());
    }
    assert!(issued.windows(2).all(|w| w[0] < w[1]), "{issued:?}");
    // Every ceiling persisted lies above the one before.
    let persisted = store.persisted();
    assert!(persisted.windows(2).all(|w| w[0] < w[1]), "{persisted:?}");
    assert!(persisted.len() > 2, "{persisted:?}");

    // A burst running ahead of a stalled clock is leased a span ahead of its last nonce.
    let now = T_US + 12 * SPAN_US / 4;
    clock.set_us(now);
    let burst = ok(nonces.try_reserve(u16::MAX));
    let top = *burst.last().unwrap();
    wait_for(|| nonces.ceiling() >= top + SPAN_US);
    assert_eq!(top, now + u64::from(u16::MAX) - 1);
    assert_eq!(nonces.lease_failure(), None);
}

/// FBC-ak22's done line, no reservation waiting on a durable write (0006): with the lease's
/// renewal held mid-write, every reservation is answered at once, the source's own thread never
/// touching the store: a cancel's nonce is issued under the ceiling already persisted, the
/// nonce at the ceiling too, and a block past it refused (`PastLease`), nothing reserved, rather
/// than waited for. The write released, the renewal lands and the source issues above the old
/// ceiling.
#[test]
fn no_reservation_waits_for_a_lease_write_and_none_passes_the_ceiling_persisted() {
    let clock = Stub::at_us(T_US);
    let store = Store::default();
    let mut nonces = WallClockNonces::new(clock.clone(), WINDOW, lease(&store)).unwrap();
    let ceiling = T_US + SPAN_US;
    store.hold();

    // Three quarters of a span on, a quarter is left: the reservation asks for a renewal, which
    // the store holds mid-write.
    let late = T_US + 3 * SPAN_US / 4;
    clock.set_us(late);
    assert_eq!(ok(nonces.try_reserve(1)), [late]);
    wait_for(|| store.writing());

    // A cancel's nonce, the write still held: issued at once, under the ceiling persisted.
    clock.set_us(late + 10);
    assert_eq!(ok(nonces.try_reserve(1)), [late + 10]);
    assert!(store.writing());
    assert_eq!(nonces.ceiling(), ceiling);
    // The clock at the ceiling: its nonce is issued; a block past it is refused, not waited for.
    clock.set_us(ceiling);
    assert_eq!(ok(nonces.try_reserve(1)), [ceiling]);
    clock.set_us(ceiling + 1);
    let past = Duration::from_micros(2);
    assert_eq!(nonces.try_reserve(2), Err(NonceRefused::PastLease { past }));
    // The infallible path gives no nonce either: an empty block.
    assert!(nonces.reserve(1).is_empty());
    assert_eq!(nonces.last(), Some(ceiling));
    assert!(store.writing());
    assert_eq!(store.persisted(), [ceiling]);

    // Released: the renewal asked for at `late` lands, then the next, and the source issues again.
    store.release();
    wait_for(|| nonces.ceiling() > ceiling);
    assert_eq!(store.persisted()[1], late + SPAN_US);
    assert_eq!(ok(nonces.try_reserve(2)), [ceiling + 1, ceiling + 2]);
}

/// A lease write that fails is reported (`lease_failure`), the ceiling left where it was, and
/// retried at the next reservation that finds half a span or less left; a write that then
/// succeeds clears the report.
#[test]
fn a_lease_write_that_fails_is_reported_and_retried() {
    let clock = Stub::at_us(T_US);
    let store = Store::default();
    let mut nonces = WallClockNonces::new(clock.clone(), WINDOW, lease(&store)).unwrap();
    let ceiling = T_US + SPAN_US;
    store.fail(true);
    let late = T_US + 3 * SPAN_US / 4;
    clock.set_us(late);
    assert_eq!(ok(nonces.try_reserve(1)), [late]);
    wait_for(|| nonces.lease_failure().is_some());
    assert_eq!(nonces.lease_failure(), Some(io::ErrorKind::Other));
    assert_eq!(nonces.ceiling(), ceiling);
    assert_eq!(store.persisted(), [ceiling]);

    store.fail(false);
    assert_eq!(ok(nonces.try_reserve(1)), [late + 1]);
    wait_for(|| nonces.ceiling() > ceiling);
    assert_eq!(nonces.lease_failure(), None);
    // A span ahead of the clock, or of the last nonce, whichever the writer saw.
    assert!(nonces.ceiling() >= late + SPAN_US);
}

/// A lease whose first ceiling cannot be persisted builds no source: a store that refuses the
/// write, or a lease file in a directory that does not exist.
#[test]
fn a_lease_whose_first_ceiling_is_not_persisted_builds_no_source() {
    let clock = Stub::at_us(T_US);
    let store = Store::default();
    store.fail(true);
    match WallClockNonces::new(clock.clone(), WINDOW, lease(&store)) {
        Err(NonceStartError::Lease(e)) => assert_eq!(e.kind(), io::ErrorKind::Other),
        other => panic!("{other:?}"),
    }
    assert_eq!(store.persisted(), []);
    let nowhere = fresh_dir("lease_in_a_missing_directory")
        .join("missing")
        .join("a.lease");
    let lease = NonceLease {
        store: LeaseFile::create(&nowhere).unwrap(),
        span: SPAN,
    };
    match WallClockNonces::new(clock, WINDOW, lease) {
        Err(NonceStartError::Lease(e)) => assert_eq!(e.kind(), io::ErrorKind::NotFound),
        other => panic!("{other:?}"),
    }
}

/// A lease file holds one ceiling, as a decimal line, each persist replacing it whole with no
/// file of its own left beside it; `create` is for a sequence that never issued a nonce and
/// refuses a path that exists, and `open` refuses a file that is missing or holds no ceiling,
/// so a lost lease is never read as a fresh one.
#[test]
fn a_lease_file_holds_one_ceiling_replaced_whole() {
    let dir = fresh_dir("lease_file");
    let path = dir.join("account.lease");
    let mut file = LeaseFile::create(&path).unwrap();
    assert_eq!(file.ceiling(), None);
    assert_eq!(file.path(), path);
    assert!(!path.exists());
    assert_eq!(
        LeaseFile::open(&path).unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
    file.persist(T_US).unwrap();
    file.persist(T_US + 9).unwrap();
    assert_eq!(file.ceiling(), Some(T_US + 9));
    assert_eq!(
        fs::read_to_string(&path).unwrap(),
        format!("{}\n", T_US + 9)
    );
    let names: Vec<_> = fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(names, ["account.lease"]);
    assert_eq!(LeaseFile::open(&path).unwrap().ceiling(), Some(T_US + 9));
    assert_eq!(
        LeaseFile::create(&path).unwrap_err().kind(),
        io::ErrorKind::AlreadyExists
    );
    for garbage in ["", "\n", "12x\n", "-3\n", "18446744073709551616\n"] {
        fs::write(&path, garbage).unwrap();
        let refused = LeaseFile::open(&path).unwrap_err();
        assert_eq!(refused.kind(), io::ErrorKind::InvalidData, "{garbage:?}");
    }
}
