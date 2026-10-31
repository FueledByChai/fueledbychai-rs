//! The production nonce source for a venue whose nonce is a wall-clock timestamp near server
//! time (FBC-3bq4, decision 0114; its restart FBC-ak22, decision 0133): [`WallClockNonces`].
//!
//! Hibachi takes a microsecond Unix timestamp within a documented window of server time,
//! unique per account; it states no monotonic rule. Issuing monotonic wall-clock microseconds,
//! each `max(clock, last + 1)`, as the venue's `NonceScope::PerAccountMonotonic` declares,
//! satisfies that rule conservatively: two nonces never repeat within a process.
//! A nonce outside the window would be refused by the venue, so the source refuses first: a
//! block whose last nonce would lie further ahead of its clock than the caller's window is
//! [`NonceRefused::OutsideWindow`], nothing reserved, and the order-entry session ends with
//! that refusal as its error. The window is the caller's value, required: the source has none
//! of its own.
//!
//! Nor do two nonces repeat across a restart, whatever the restarted clock reads, through the
//! source's lease (decision 0133): it issues a nonce only at or below a ceiling its
//! [`LeaseStore`] has already persisted, and a source built over that store resumes after the
//! ceiling, so every nonce an earlier process sent lies below the first it issues, a nonce sent
//! while its journal record was still queued, unwritten when the process died, included. The
//! first ceiling is persisted when the source is built, a span ahead of its clock; after that a
//! writer thread of the source's own persists the next, a span ahead of the clock or of the last
//! nonce, whichever is later, once half a span or less is left, so no reservation (a cancel's
//! included) ever waits on a write (0006). A reservation that would pass the ceiling persisted so
//! far is refused, [`NonceRefused::PastLease`], rather than waited for; the session ends with it
//! as with any refusal. [`LeaseFile`] is the store for production: one file per account.
//! The journal's [`last_nonce`] is still read: a source resumes after the larger of it and the
//! lease's ceiling.
//!
//! The source reads its clock through [`WallClock`], injected, so tests drive it with a stub.
//! It is not journaled itself: the order-entry session journals every nonce it reserves, the
//! nonce it was resumed after or last issued when a journal is set
//! ([`NonceSource::high_water`]), and every context it gives its codec, and replay reads the
//! contexts back without consulting any clock.
//! A clock running ahead of server time cannot be seen from here; a consumer that measures its
//! skew against the venue's time is beyond this source.

use std::fs::{self, File};
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::thread;
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

/// Where a [`WallClockNonces`]'s lease ceilings are persisted (FBC-ak22, decision 0133): one
/// store per nonce sequence, which is per account on Hibachi. [`LeaseFile`] is the production
/// one; a test brings its own.
pub trait LeaseStore: Send + 'static {
    /// The ceiling last persisted, by this process or an earlier one: no nonce above it was
    /// issued. `None` for a store that has held none, as for a sequence that never issued one.
    fn ceiling(&self) -> Option<u64>;

    /// Persists `ceiling` durably, returning only once a crash can no longer lose it. Called on
    /// the thread building the source for its first ceiling, then only on the lease's writer
    /// thread, never on a reservation's.
    fn persist(&mut self, ceiling: u64) -> io::Result<()>;
}

/// A source's lease: the store its ceilings are persisted to, and how far ahead of the clock,
/// or of the last nonce if that is later, each reaches. The span is the consumer's, required:
/// from a microsecond to the window, since a restart resumes after the last ceiling and its
/// first nonce must still lie within the window. Each renewal is one durable write, about two
/// a span.
#[derive(Debug)]
pub struct NonceLease<S> {
    pub store: S,
    pub span: Duration,
}

/// Why a [`WallClockNonces`] was not built.
#[derive(Debug)]
pub enum NonceStartError {
    /// A window under one microsecond admits no nonce of a microsecond source.
    ZeroWindow,
    /// The lease's span is under a microsecond or longer than the window.
    Span { span: Duration, window: Duration },
    /// The lease's first ceiling was not persisted, or its writer thread did not start.
    Lease(io::Error),
}

impl std::fmt::Display for NonceStartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NonceStartError::ZeroWindow => {
                f.write_str("a nonce window under one microsecond admits no nonce")
            }
            NonceStartError::Span { span, window } => write!(
                f,
                "a nonce lease's span of {span:?} lies outside the range from one microsecond \
                 to the {window:?} window"
            ),
            NonceStartError::Lease(e) => {
                write!(f, "the nonce lease's first ceiling was not persisted: {e}")
            }
        }
    }
}

impl std::error::Error for NonceStartError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            NonceStartError::Lease(e) => Some(e),
            _ => None,
        }
    }
}

/// Monotonic wall-clock microseconds: each nonce `max(clock, last + 1)`, a block's increasing
/// by one in item order, refused whole when its last nonce would lie further ahead of the clock
/// than the caller's window or past the ceiling its lease has persisted. The module docs say
/// why.
#[derive(Debug)]
pub struct WallClockNonces<C> {
    clock: C,
    window: Duration,
    window_us: u64,
    span: Duration,
    span_us: u64,
    last: Option<u64>,
    lease: Arc<Lease>,
}

/// What a source and its lease's writer share.
#[derive(Debug)]
struct Lease {
    /// The ceiling persisted: no nonce above it is issued.
    durable: AtomicU64,
    /// The last nonce issued or resumed after (0 for none): a renewal reaches a span past it
    /// when it is later than the clock.
    issued: AtomicU64,
    writer: Mutex<Writer>,
    wake: Condvar,
}

/// The writer's orders and its last outcome.
#[derive(Debug, Default)]
struct Writer {
    /// The source found half a span or less left: renew now.
    due: bool,
    /// The source was dropped: write nothing more, read no clock.
    stopped: bool,
    /// The last write's failure, cleared by one that succeeds.
    failure: Option<io::ErrorKind>,
}

impl Lease {
    fn writer(&self) -> MutexGuard<'_, Writer> {
        self.writer.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Asks the writer to renew now. It takes the lock the writer holds only while it reads the
    /// clock, never while it writes.
    fn renew_now(&self) {
        self.writer().due = true;
        self.wake.notify_one();
    }
}

impl<C: WallClock + Clone + 'static> WallClockNonces<C> {
    /// A source reading `clock`, refusing a nonce further than `window` ahead of it, under
    /// `lease`: it resumes after the ceiling the lease's store holds, if any, and has issued
    /// nothing else. For a process whose journal holds no nonce of the sequence.
    pub fn new<S: LeaseStore>(
        clock: C,
        window: Duration,
        lease: NonceLease<S>,
    ) -> Result<WallClockNonces<C>, NonceStartError> {
        WallClockNonces::start(clock, window, None, lease)
    }

    /// [`new`](WallClockNonces::new), resumed after `last`, the last nonce an earlier journal
    /// holds for the sequence ([`last_nonce`]), or after the ceiling the lease's store holds,
    /// whichever is larger: every nonce it issues is above both.
    pub fn resuming<S: LeaseStore>(
        clock: C,
        window: Duration,
        last: u64,
        lease: NonceLease<S>,
    ) -> Result<WallClockNonces<C>, NonceStartError> {
        WallClockNonces::start(clock, window, Some(last), lease)
    }

    /// Persists the lease's first ceiling, a span past the clock or the nonce resumed after,
    /// whichever is later, on this thread, before any nonce is issued, then starts its writer.
    fn start<S: LeaseStore>(
        clock: C,
        window: Duration,
        journal_last: Option<u64>,
        lease: NonceLease<S>,
    ) -> Result<WallClockNonces<C>, NonceStartError> {
        let window_us = micros(window);
        if window_us == 0 {
            return Err(NonceStartError::ZeroWindow);
        }
        let NonceLease { mut store, span } = lease;
        let span_us = micros(span);
        if span_us == 0 || span > window {
            return Err(NonceStartError::Span { span, window });
        }
        let last = journal_last.max(store.ceiling());
        let issued = last.unwrap_or(0);
        let first = clock_us(&clock).max(issued).saturating_add(span_us);
        store.persist(first).map_err(NonceStartError::Lease)?;
        let lease = Arc::new(Lease {
            durable: AtomicU64::new(first),
            issued: AtomicU64::new(issued),
            writer: Mutex::new(Writer::default()),
            wake: Condvar::new(),
        });
        let shared = Arc::clone(&lease);
        let read = clock.clone();
        thread::Builder::new()
            .name("fbc-nonce-lease".to_owned())
            .spawn(move || renew(&shared, &read, store, span_us))
            .map_err(NonceStartError::Lease)?;
        Ok(WallClockNonces {
            clock,
            window,
            window_us,
            span,
            span_us,
            last,
            lease,
        })
    }
}

impl<C> WallClockNonces<C> {
    /// The last nonce issued, or resumed after; `None` before the first.
    pub fn last(&self) -> Option<u64> {
        self.last
    }

    /// The window the caller gave.
    pub fn window(&self) -> Duration {
        self.window
    }

    /// The lease's span the caller gave.
    pub fn span(&self) -> Duration {
        self.span
    }

    /// The ceiling the lease has persisted: the source issues no nonce above it.
    pub fn ceiling(&self) -> u64 {
        self.lease.durable.load(Ordering::SeqCst)
    }

    /// How the lease's last write failed, if it did: cleared by the next that succeeds. A write
    /// that keeps failing leaves the ceiling where it was, and the source refuses past it.
    pub fn lease_failure(&self) -> Option<io::ErrorKind> {
        self.lease.writer().failure
    }
}

impl<C> Drop for WallClockNonces<C> {
    /// Stops the lease's writer without waiting for a write under way: it reads no clock and
    /// starts no write after this returns.
    fn drop(&mut self) {
        self.lease.writer().stopped = true;
        self.lease.wake.notify_one();
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
        // Only under a ceiling already persisted; past it, refused at once, never waited for.
        let ceiling = self.lease.durable.load(Ordering::SeqCst);
        if block_last > ceiling {
            self.lease.renew_now();
            let past = Duration::from_micros(block_last - ceiling);
            return Err(NonceRefused::PastLease { past });
        }
        let block = NonceBlock::consecutive(first, len).ok_or(NonceRefused::Exhausted)?;
        self.last = Some(block_last);
        self.lease.issued.fetch_max(block_last, Ordering::SeqCst);
        if renewal(ceiling, now_us.max(block_last), self.span_us).is_some() {
            self.lease.renew_now();
        }
        Ok(block)
    }

    /// [`last`](WallClockNonces::last): the session journals it when a journal is set, so a
    /// resumed source that issues nothing still leaves its journal naming the nonce it resumed
    /// after (RB187-1 on PR #187).
    fn high_water(&self) -> Option<u64> {
        self.last
    }
}

/// The ceiling to persist when `durable` is the one persisted and `base` the later of the clock
/// and the last nonce: a span past `base` once half a span or less is left, else none.
fn renewal(durable: u64, base: u64, span_us: u64) -> Option<u64> {
    let room = durable.saturating_sub(base);
    let next = base.saturating_add(span_us);
    (room <= span_us / 2 && next > durable).then_some(next)
}

/// The lease's writer: four times a span, and whenever the source asks, it reads the clock and,
/// once half a span or less is left, persists the next ceiling, which the source issues under
/// only once the store has returned. A renewal asked for with half a span left so has a quarter
/// span, at least, to land before the clock reaches the ceiling. It ends when the source is
/// dropped.
fn renew<C: WallClock, S: LeaseStore>(lease: &Lease, clock: &C, mut store: S, span_us: u64) {
    let period = Duration::from_micros((span_us / 4).max(1));
    loop {
        let next = {
            let mut writer = lease.writer();
            if !writer.due && !writer.stopped {
                let woke = lease.wake.wait_timeout(writer, period);
                writer = woke.unwrap_or_else(PoisonError::into_inner).0;
            }
            if writer.stopped {
                return;
            }
            writer.due = false;
            // Read under the lock the source's drop takes, so no clock is read once it returns.
            let base = clock_us(clock).max(lease.issued.load(Ordering::SeqCst));
            let durable = lease.durable.load(Ordering::SeqCst);
            match renewal(durable, base, span_us) {
                Some(next) => next,
                None => continue,
            }
        };
        let written = store.persist(next);
        let mut writer = lease.writer();
        match written {
            Ok(()) => {
                lease.durable.fetch_max(next, Ordering::SeqCst);
                writer.failure = None;
            }
            Err(e) => writer.failure = Some(e.kind()),
        }
    }
}

/// The clock's whole microseconds, 0 before the epoch (where the source refuses every nonce).
fn clock_us(clock: &impl WallClock) -> u64 {
    u64::try_from(clock.now().0).map_or(0, |ns| ns / 1_000)
}

/// `d` in whole microseconds, saturating.
fn micros(d: Duration) -> u64 {
    u64::try_from(d.as_micros()).unwrap_or(u64::MAX)
}

/// A lease file (decision 0133): the ceiling as one decimal line. Each persist writes a file
/// beside it, syncs it, renames it over the lease file and, on Unix, syncs the directory, so a
/// crash leaves the old ceiling or the new, never a torn one. One file per nonce sequence, in a
/// directory the consumer gives; the library names no path. One process holds it, as one
/// process holds the account (0007).
#[derive(Debug)]
pub struct LeaseFile {
    path: PathBuf,
    ceiling: Option<u64>,
}

impl LeaseFile {
    /// The lease file at `path` for a sequence that has never issued a nonce: refused when
    /// `path` exists. Nothing is written until the first persist, which a source makes before
    /// it issues anything.
    pub fn create(path: impl Into<PathBuf>) -> io::Result<LeaseFile> {
        let path = path.into();
        match fs::symlink_metadata(&path) {
            Ok(_) => Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!(
                    "{} exists: a nonce lease file is created only for a sequence that never \
                     issued a nonce",
                    path.display()
                ),
            )),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(LeaseFile {
                path,
                ceiling: None,
            }),
            Err(e) => Err(e),
        }
    }

    /// The lease file an earlier process persisted at `path`, with its ceiling: refused when it
    /// is missing or holds no ceiling, so a lost lease is never read as a fresh one.
    pub fn open(path: impl Into<PathBuf>) -> io::Result<LeaseFile> {
        let path = path.into();
        let text = fs::read_to_string(&path)?;
        let digits = text
            .strip_suffix('\n')
            .filter(|d| !d.is_empty() && d.bytes().all(|b| b.is_ascii_digit()));
        let ceiling = digits.and_then(|d| d.parse().ok()).ok_or_else(|| {
            let what = format!("{} holds no nonce lease ceiling", path.display());
            io::Error::new(io::ErrorKind::InvalidData, what)
        })?;
        Ok(LeaseFile {
            path,
            ceiling: Some(ceiling),
        })
    }

    /// Where it is.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The ceiling it holds: read when it was opened, or last persisted.
    pub fn ceiling(&self) -> Option<u64> {
        self.ceiling
    }
}

impl LeaseStore for LeaseFile {
    fn ceiling(&self) -> Option<u64> {
        self.ceiling
    }

    fn persist(&mut self, ceiling: u64) -> io::Result<()> {
        let mut beside = self.path.clone().into_os_string();
        beside.push(".tmp");
        let beside = PathBuf::from(beside);
        let mut file = File::create(&beside)?;
        file.write_all(format!("{ceiling}\n").as_bytes())?;
        file.sync_all()?;
        drop(file);
        fs::rename(&beside, &self.path)?;
        sync_dir(&self.path)?;
        self.ceiling = Some(ceiling);
        Ok(())
    }
}

/// Syncs the directory holding `path`, so a rename into it survives a crash.
#[cfg(unix)]
fn sync_dir(path: &Path) -> io::Result<()> {
    let dir = match path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir,
        _ => Path::new("."),
    };
    File::open(dir)?.sync_all()
}

/// Elsewhere a directory cannot be opened to sync it; the rename stands as written.
#[cfg(not(unix))]
fn sync_dir(_: &Path) -> io::Result<()> {
    Ok(())
}

/// The last nonce `records`, an earlier journal's records in journal order, hold for source
/// `source`: the largest, so a source resumed after it ([`WallClockNonces::resuming`]) never
/// reissues one. `Ok(None)` when they hold none.
///
/// A journal that dropped records (a `Degraded` marker, recovered or not) may have lost the
/// last nonce sent, since a dropped Safety record's request is sent all the same (0006): it is
/// refused, [`JournalGap`], naming the first gap, rather than answered with the largest that
/// survived (Codex P2 r4321805846 on PR #187). A session journals the nonce its source was
/// resumed after when its journal is set, so a process that reserved none still leaves its
/// journal naming it (RB187-1 on PR #187). Records queued but never written when a process dies
/// leave no marker, so the journal alone cannot bound a nonce sent with its record unwritten:
/// the source's lease does (FBC-ak22, decision 0133), and a consumer refused here resumes with
/// [`WallClockNonces::new`] over the lease alone, every nonce sent lying at or below its
/// ceiling.
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
