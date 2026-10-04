//! The sink the shard journals through, which never blocks (0006, design §5.4), and the writer
//! thread that drains it into a [`JournalWriter`].
//!
//! [`journal_queue`] makes a bounded single-producer single-consumer queue of encoded records
//! with a byte budget the consumer configures ([`SinkConfig`]; no size is chosen here, 0009).
//! Its producer end is a [`QueueSink`]; its consumer end, a [`JournalDrain`], is
//! [spawned](JournalDrain::spawn) onto a thread of its own as a [`WriterThread`].
//!
//! **Admission.** A record is encoded on the caller's thread, its redaction spans hashed under
//! the [`RedactionKey`] (so no redacted byte enters the queue), and admitted by its class: a Normal record while the queue stays at or under the
//! soft limit, a Safety record (cancels, reducing orders, acks, fills) while it stays within
//! the whole budget, the part above the soft limit being the Safety reserve. A record that
//! does not fit is dropped and counted by class, and [`record`](JournalSink::record) says so:
//! the caller proceeds either way. A record the format cannot hold, or one larger than the
//! room its class has left, is dropped and counted the same way, and its payload is not
//! copied: encoding stops where the room ends. A caller whose record would copy a large payload
//! to be built offers it with [`record_with`](JournalSink::record_with) and its size: one whose
//! payload alone exceeds that room is dropped and counted without being built. A record the
//! caller may not journal at all is [omitted](JournalSink::omit): dropped and counted the same
//! way, so the gap it leaves is marked.
//!
//! **The gap.** The sink numbers the records offered to it from 0. The first drop opens a gap;
//! it closes once space returns, that is when a record arrives and the `Degraded` marker and
//! that record both fit under the soft limit: [`Marker::Degraded`] `{ from_seq, dropped }` is
//! written first, naming the first dropped record's number and how many were dropped since,
//! then the record. Safety records admitted from the reserve while the gap is open come before
//! the marker. Markers the sink writes take no number. A gap still open when the sink closes
//! ([`WriterThread::close`], or the sink dropped) is marked by the writer as the journal's last
//! record; records offered after that are dropped and counted, not marked.
//!
//! **The queue** is hand-written and has no `unsafe` (decision 0021): a ring of `AtomicU64`
//! words whose read and write positions only the consumer and only the producer advance. An
//! entry is the body's length, the wall time to file it under, and the body in whole words.
//! Neither end ever waits for the other: the producer refuses what does not fit, and the
//! writer parks only when the ring is empty, to be woken by the next record.

use core::sync::atomic::Ordering::{Relaxed, SeqCst};
use core::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, OnceLock};
use std::thread::{self, JoinHandle, Thread};

use fbc_core::{TrafficClass, WallNs};

use crate::record::{Marker, Record};
use crate::redact::RedactionKey;
use crate::{JournalError, JournalWriter, format};

/// The queue's unit: one `AtomicU64`.
const WORD: usize = 8;
/// An entry's header: the body's length and the wall time it is filed under.
const HEADER_WORDS: usize = 2;

/// The queue's size, which is the consumer's configuration (0009); design §5.4 suggests 64 MB
/// with the soft limit at 85%.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct SinkConfig {
    /// The queue's whole budget in bytes, rounded down to whole 8-byte words. Entries are
    /// counted with their 16-byte header and their body rounded up to whole words.
    pub budget_bytes: usize,
    /// The share of the budget, in percent, Normal records may fill. The rest is the Safety
    /// reserve. [`journal_queue`] refuses a soft limit that cannot hold a `Degraded` marker and
    /// the smallest record together (a gap could never close) and a reserve that cannot hold
    /// the smallest record.
    pub soft_limit_pct: u8,
}

/// What became of a record offered to a [`JournalSink`].
#[must_use]
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum Recorded {
    /// The record is queued for the writer.
    Ok,
    /// The record was dropped and counted. The caller proceeds as if it were journaled: a
    /// cancel is still sent.
    DroppedCounted,
}

/// Where the shard journals a record. It never blocks.
pub trait JournalSink {
    /// Journals `record` under the UTC day of `now`, or drops and counts it when its class has
    /// no room left. Never blocks and never fails the caller.
    fn record(&mut self, class: TrafficClass, now: WallNs, record: &Record) -> Recorded;

    /// Journals the record `make` builds, as [`record`](JournalSink::record) does, when the
    /// record's encoding holds at least `payload` bytes (a frame's or a body's): a sink that can
    /// tell such a record has no room drops and counts it without calling `make`, so a large
    /// payload is not copied only to be refused (FBC-f3w); `usize::MAX` stands for a record the
    /// format refuses whatever the room. By default it builds and records it.
    fn record_with(
        &mut self,
        class: TrafficClass,
        now: WallNs,
        payload: usize,
        make: &mut dyn FnMut() -> Record,
    ) -> Recorded {
        let _ = payload;
        self.record(class, now, &make())
    }

    /// Counts a record of `class` that the caller withholds, its content being one the journal
    /// may not keep, as dropped: numbered into a gap like any other drop, so the journal marks
    /// the omission (a `Degraded` marker before the next record written) rather than reading as
    /// complete. Never blocks.
    fn omit(&mut self, class: TrafficClass, now: WallNs) -> Recorded;
}

/// Makes the journal queue: the sink the shard records into, which hashes redaction spans
/// under `key`, and the drain a writer thread empties it with.
pub fn journal_queue(
    config: SinkConfig,
    key: Arc<RedactionKey>,
) -> Result<(QueueSink, JournalDrain), JournalError> {
    let cap = config.budget_bytes / WORD;
    let soft = (cap as u128 * u128::from(config.soft_limit_pct) / 100) as usize;
    let marker = entry_words_of(
        &Record::Marker(Marker::Degraded {
            from_seq: 0,
            dropped: 0,
        }),
        &key,
    );
    let smallest = entry_words_of(&Record::Marker(Marker::Recovered), &key);
    // A gap closes only when a marker and a record fit under the soft limit together.
    if soft < marker + smallest {
        return Err(JournalError::Config(
            "a soft limit too small to hold a Degraded marker and a record",
        ));
    }
    if cap
        .checked_sub(soft)
        .is_none_or(|reserve| reserve < smallest)
    {
        return Err(JournalError::Config(
            "a Safety reserve too small to hold a record",
        ));
    }
    let ring = Arc::new(Ring {
        words: (0..cap).map(|_| AtomicU64::new(0)).collect(),
        head: AtomicU64::new(0),
        tail: AtomicU64::new(0),
        parked: AtomicBool::new(false),
        closed: AtomicBool::new(false),
        busy: AtomicBool::new(false),
        gap_from: AtomicU64::new(NO_GAP),
        gap_dropped: AtomicU64::new(0),
        gap_wall: AtomicU64::new(0),
        writer: OnceLock::new(),
    });
    let sink = QueueSink {
        ring: Arc::clone(&ring),
        key,
        soft,
        next_seq: 0,
        dropped: [0; 2],
        gap: None,
        body: Vec::new(),
        marker: Vec::new(),
    };
    Ok((sink, JournalDrain { ring }))
}

/// The ring both ends share. `head` (words consumed) only the drain advances; `tail` (words
/// published) only the sink. Both are `u64` on every target, so they never wrap (2^64 words
/// is far beyond any journal), and index the ring modulo its length.
struct Ring {
    words: Box<[AtomicU64]>,
    head: AtomicU64,
    tail: AtomicU64,
    /// The writer is parked, or about to park, and wants waking.
    parked: AtomicBool,
    /// Closed: the sink refuses every record from now on, and the writer stops once the ring
    /// is empty.
    closed: AtomicBool,
    /// The sink is between its look at `closed` and the end of its push. With both flags
    /// `SeqCst`, a writer that sees `closed` and then `busy` clear knows no record it has not
    /// seen will be pushed.
    busy: AtomicBool,
    /// The sink's open gap, copied for the writer to mark at shutdown: the first dropped
    /// record's number ([`NO_GAP`] when none), how many were dropped, and the first drop's
    /// wall time. The sink changes them only inside `busy` and only before it closes, so the
    /// writer's look after waiting `busy` out is final.
    gap_from: AtomicU64,
    gap_dropped: AtomicU64,
    gap_wall: AtomicU64,
    /// The writer thread, set by the thread itself before it can park.
    writer: OnceLock<Thread>,
}

/// Ends the sink's busy span when dropped: armed only while a record builder runs, so one that
/// panics cannot leave the writer waiting on a push that will never come.
struct BusyGuard<'a>(&'a AtomicBool);

impl Drop for BusyGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, SeqCst);
    }
}

/// The words an entry with a body of `len` bytes takes.
fn entry_words(len: usize) -> usize {
    HEADER_WORDS + len.div_ceil(WORD)
}

/// The words a small fixed record's entry takes.
fn entry_words_of(record: &Record, key: &RedactionKey) -> usize {
    let mut body = Vec::new();
    format::encode(record, key, &mut body).expect("a marker always encodes");
    entry_words(body.len())
}

impl Ring {
    fn slot(&self, pos: u64) -> &AtomicU64 {
        &self.words[(pos % self.words.len() as u64) as usize]
    }

    /// Words queued and not yet consumed.
    fn used(&self) -> usize {
        (self.tail.load(SeqCst) - self.head.load(SeqCst)) as usize
    }

    /// Appends an entry. Only the sink calls it, and only after checking it fits.
    fn push(&self, now: WallNs, body: &[u8]) {
        let t = self.tail.load(Relaxed);
        self.slot(t).store(body.len() as u64, Relaxed);
        self.slot(t + 1).store(now.0 as u64, Relaxed);
        for (i, chunk) in body.chunks(WORD).enumerate() {
            let mut word = [0; WORD];
            word[..chunk.len()].copy_from_slice(chunk);
            self.slot(t + (HEADER_WORDS + i) as u64)
                .store(u64::from_le_bytes(word), Relaxed);
        }
        self.tail.store(t + entry_words(body.len()) as u64, SeqCst);
    }

    /// Takes the oldest entry into `body` and returns its wall time, or `None` when the ring
    /// is empty. Only the drain calls it.
    fn pop(&self, body: &mut Vec<u8>) -> Option<WallNs> {
        let h = self.head.load(Relaxed);
        if self.tail.load(SeqCst) == h {
            return None;
        }
        let len = self.slot(h).load(Relaxed) as usize;
        let wall = WallNs(self.slot(h + 1).load(Relaxed) as i64);
        body.clear();
        for i in 0..len.div_ceil(WORD) {
            body.extend_from_slice(
                &self
                    .slot(h + (HEADER_WORDS + i) as u64)
                    .load(Relaxed)
                    .to_le_bytes(),
            );
        }
        body.truncate(len);
        self.head.store(h + entry_words(len) as u64, SeqCst);
        Some(wall)
    }

    /// Wakes the writer if it parked. With `parked` set before the writer's last look at the
    /// ring and cleared here after the sink's last change to it, one of the two always sees
    /// the other.
    fn wake(&self) {
        if self.parked.swap(false, SeqCst)
            && let Some(writer) = self.writer.get()
        {
            writer.unpark();
        }
    }
}

/// An open gap: records dropped from `from_seq` on, `dropped` of them.
struct Gap {
    from_seq: u64,
    dropped: u64,
    /// When the first record was dropped: the marker the writer writes at shutdown is filed
    /// under it.
    wall: WallNs,
}

/// `Ring::gap_from` when no gap is open.
const NO_GAP: u64 = u64::MAX;

/// The producer end of the journal queue: the [`JournalSink`] the shard records into.
/// Dropping it lets the writer finish what is queued and stop.
pub struct QueueSink {
    ring: Arc<Ring>,
    /// The key redaction spans are hashed under.
    key: Arc<RedactionKey>,
    /// The soft limit, in words.
    soft: usize,
    next_seq: u64,
    /// Drops by class: Normal, Safety.
    dropped: [u64; 2],
    gap: Option<Gap>,
    body: Vec<u8>,
    marker: Vec<u8>,
}

fn class_index(class: TrafficClass) -> usize {
    match class {
        TrafficClass::Normal => 0,
        TrafficClass::Safety => 1,
    }
}

/// Whether `need` more words fit with `used` queued under `limit`.
fn fits(used: usize, need: usize, limit: usize) -> bool {
    used.checked_add(need).is_some_and(|n| n <= limit)
}

impl QueueSink {
    /// How many records of `class` were dropped since the sink was made.
    pub fn dropped(&self, class: TrafficClass) -> u64 {
        self.dropped[class_index(class)]
    }

    /// The bytes queued and not yet taken by the writer, entry headers and padding included.
    pub fn queued_bytes(&self) -> usize {
        self.ring.used() * WORD
    }
}

impl JournalSink for QueueSink {
    fn record(&mut self, class: TrafficClass, now: WallNs, record: &Record) -> Recorded {
        self.offer(class, now, Some(record))
    }

    fn record_with(
        &mut self,
        class: TrafficClass,
        now: WallNs,
        payload: usize,
        make: &mut dyn FnMut() -> Record,
    ) -> Recorded {
        // An entry holding `payload` bytes needs at least this many words; one that exceeds the
        // room its class has even before an open gap's marker is refused unbuilt, and so is any
        // record once the writer is closed (Codex r4178427389). Otherwise the record is built
        // and admitted exactly.
        // A Normal record must also fit after an open gap's marker (Codex r4178646788).
        let (need, limit) = match class {
            TrafficClass::Normal => (self.marker_words() + entry_words(payload), self.soft),
            TrafficClass::Safety => (entry_words(payload), self.ring.words.len()),
        };
        // Busy from the look at `closed` through the build and the push, so a writer closing
        // meanwhile waits for the record instead of it being built only to be dropped (Codex
        // r4178725028).
        self.ring.busy.store(true, SeqCst);
        let open = !self.ring.closed.load(SeqCst);
        let recorded = if open && fits(self.ring.used(), need, limit) {
            // A builder that panics ends the busy span as it unwinds (Codex r4178802729).
            let unwinding = BusyGuard(&self.ring.busy);
            let record = make();
            std::mem::forget(unwinding);
            self.settle(class, now, Some(&record), open)
        } else {
            self.settle(class, now, None, open)
        };
        self.end_busy(recorded)
    }

    fn omit(&mut self, class: TrafficClass, now: WallNs) -> Recorded {
        self.offer(class, now, None)
    }
}

impl QueueSink {
    /// Offers `record`, numbering it; `None` stands for a record already known not to fit,
    /// which is dropped and counted like any other.
    fn offer(&mut self, class: TrafficClass, now: WallNs, record: Option<&Record>) -> Recorded {
        self.ring.busy.store(true, SeqCst);
        let open = !self.ring.closed.load(SeqCst);
        let recorded = self.settle(class, now, record, open);
        self.end_busy(recorded)
    }

    /// Numbers `record` and admits it, or drops and counts it, with the sink already busy and
    /// `open` what it saw of `closed` once busy.
    fn settle(
        &mut self,
        class: TrafficClass,
        now: WallNs,
        record: Option<&Record>,
        open: bool,
    ) -> Recorded {
        let seq = self.next_seq;
        self.next_seq += 1;
        let recorded = match record {
            Some(record) if open => self.admit(class, now, record),
            _ => Recorded::DroppedCounted,
        };
        if recorded == Recorded::DroppedCounted {
            self.dropped[class_index(class)] += 1;
            let gap = self.gap.get_or_insert(Gap {
                from_seq: seq,
                dropped: 0,
                wall: now,
            });
            gap.dropped += 1;
            if open {
                self.ring.gap_from.store(gap.from_seq, Relaxed);
                self.ring.gap_dropped.store(gap.dropped, Relaxed);
                self.ring.gap_wall.store(gap.wall.0 as u64, Relaxed);
            }
        }
        recorded
    }

    /// Ends the sink's busy span, waking the writer for a record pushed in it.
    fn end_busy(&self, recorded: Recorded) -> Recorded {
        self.ring.busy.store(false, SeqCst);
        if recorded == Recorded::Ok {
            self.ring.wake();
        }
        recorded
    }
}

impl QueueSink {
    /// Encodes an open gap's marker into `self.marker` and gives the words its entry takes; 0
    /// when no gap is open.
    fn marker_words(&mut self) -> usize {
        self.gap.as_ref().map_or(0, |gap| {
            self.marker.clear();
            let degraded = Record::Marker(Marker::Degraded {
                from_seq: gap.from_seq,
                dropped: gap.dropped,
            });
            format::encode(&degraded, &self.key, &mut self.marker)
                .expect("a marker always encodes");
            entry_words(self.marker.len())
        })
    }

    /// Pushes `record` (after an open gap's marker) if its class has room for it.
    fn admit(&mut self, class: TrafficClass, now: WallNs, record: &Record) -> Recorded {
        self.body.clear();
        let marker = self.marker_words();
        let used = self.ring.used();
        // A body too long for the room its class has left is refused before it is copied: a
        // Normal record has the soft limit less an open gap's marker, a Safety record the
        // whole budget. A record refused, for that or by the format, fits nowhere.
        let room = match class {
            TrafficClass::Normal => self.soft.saturating_sub(marker),
            TrafficClass::Safety => self.ring.words.len(),
        }
        .saturating_sub(used);
        let limit = room.saturating_sub(HEADER_WORDS) * WORD;
        let need = format::encode_within(record, &self.key, &mut self.body, limit)
            .map_or(usize::MAX, |()| entry_words(self.body.len()));
        if fits(used, marker.saturating_add(need), self.soft) {
            if self.gap.take().is_some() {
                self.ring.push(now, &self.marker);
                self.ring.gap_from.store(NO_GAP, Relaxed);
            }
            self.ring.push(now, &self.body);
        } else if class == TrafficClass::Safety && fits(used, need, self.ring.words.len()) {
            self.ring.push(now, &self.body);
        } else {
            return Recorded::DroppedCounted;
        }
        Recorded::Ok
    }
}

impl Drop for QueueSink {
    fn drop(&mut self) {
        self.ring.closed.store(true, SeqCst);
        self.ring.wake();
    }
}

/// The consumer end of the journal queue, until it is spawned as a [`WriterThread`]. Until
/// then nothing drains the queue: records past the soft limit and the reserve are dropped and
/// counted as they would be behind a stalled writer.
pub struct JournalDrain {
    ring: Arc<Ring>,
}

impl JournalDrain {
    /// Starts the writer thread, which writes every queued record through `writer` in queue
    /// order and flushes whenever the queue runs empty.
    pub fn spawn(self, writer: JournalWriter) -> Result<WriterThread, JournalError> {
        let ring = Arc::clone(&self.ring);
        let handle = thread::Builder::new()
            .name("fbc-journal".into())
            .spawn(move || run(&ring, writer))?;
        Ok(WriterThread {
            ring: self.ring,
            handle,
        })
    }
}

/// The writer thread's loop. It stops at the first write error, leaving the rest queued: the
/// queue then fills and the sink drops and counts, and [`WriterThread::close`] returns the
/// error.
fn run(ring: &Ring, mut writer: JournalWriter) -> Result<(), JournalError> {
    let _ = ring.writer.set(thread::current());
    let mut body = Vec::new();
    loop {
        // Read before looking at the ring. Once closed, the sink pushes nothing new after its
        // push in flight, if any, which `busy` waits out: what the ring then holds is the end.
        let closing = ring.closed.load(SeqCst);
        while closing && ring.busy.load(SeqCst) {
            thread::yield_now();
        }
        if let Some(wall) = ring.pop(&mut body) {
            writer.append_body(wall, &body)?;
            continue;
        }
        if closing {
            // The sink pushes nothing more and its gap is final: mark one still open.
            let from_seq = ring.gap_from.load(Relaxed);
            if from_seq != NO_GAP {
                let dropped = ring.gap_dropped.load(Relaxed);
                let wall = WallNs(ring.gap_wall.load(Relaxed) as i64);
                let degraded = Record::Marker(Marker::Degraded { from_seq, dropped });
                writer.append(wall, &degraded)?;
            }
            return writer.flush();
        }
        writer.flush()?;
        ring.parked.store(true, SeqCst);
        if ring.used() == 0 && !ring.closed.load(SeqCst) {
            thread::park();
        }
        ring.parked.store(false, SeqCst);
    }
}

/// The running writer thread.
pub struct WriterThread {
    ring: Arc<Ring>,
    handle: JoinHandle<Result<(), JournalError>>,
}

impl WriterThread {
    /// Closes the sink, writes what it accepted and then the marker of a gap still open,
    /// flushes, and stops the thread. The sink drops and counts every record offered after
    /// this. Returns the write error that stopped the thread, if one did.
    pub fn close(self) -> Result<(), JournalError> {
        self.ring.closed.store(true, SeqCst);
        self.handle.thread().unpark();
        self.handle
            .join()
            .expect("the journal writer thread panicked")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Opaque, Opcode};
    use fbc_core::{ConnKey, MonoNs, Stamp};

    fn key() -> Arc<RedactionKey> {
        Arc::new(RedactionKey::new(&[5; 32]).unwrap())
    }

    #[test]
    fn a_record_too_large_for_the_room_left_is_not_copied() {
        // Codex r4176799800: dropping a large frame must not copy it on the shard thread.
        let (mut sink, _drain) = journal_queue(
            SinkConfig {
                budget_bytes: 256,
                soft_limit_pct: 50,
            },
            key(),
        )
        .unwrap();
        let inbound = |len: usize| Record::Inbound {
            stamp: Stamp {
                ingest_seq: 0,
                kernel_rx: None,
                recv_mono: MonoNs(0),
                recv_wall: WallNs(0),
                conn: ConnKey { conn: 1, epoch: 1 },
            },
            opcode: Opcode::Binary,
            bytes: Opaque(vec![7; len]),
            redact: Vec::new(),
        };
        let big = inbound(1 << 20);
        for class in [TrafficClass::Normal, TrafficClass::Safety] {
            assert_eq!(
                sink.record(class, WallNs(0), &big),
                Recorded::DroppedCounted
            );
            assert!(sink.body.capacity() <= 512, "{}", sink.body.capacity());
        }
        // Codex r4176841165: with a gap open, a Normal record must leave room for the marker.
        // The queue is empty and the soft limit is 16 words; this record's entry is 14, so it
        // fits alone but not after the 5-word marker, and is dropped without being copied.
        let mid = inbound(55);
        let mut body = Vec::new();
        format::encode(&mid, &key(), &mut body).unwrap();
        assert_eq!(entry_words(body.len()), 14);
        assert_eq!(
            sink.record(TrafficClass::Normal, WallNs(0), &mid),
            Recorded::DroppedCounted
        );
        assert!(
            sink.body.capacity() < body.len(),
            "{}",
            sink.body.capacity()
        );
    }

    #[test]
    fn a_record_offered_lazily_leaves_room_for_an_open_gaps_marker_before_it_is_built() {
        // Codex r4178646788: with a gap open, a Normal record offered lazily must fit after the
        // marker that goes ahead of it, or it is refused unbuilt. The soft limit is 16 words; a
        // 96-byte payload needs a 14-word entry, which fits alone but not after the 5-word
        // marker.
        let (mut sink, _drain) = journal_queue(
            SinkConfig {
                budget_bytes: 256,
                soft_limit_pct: 50,
            },
            key(),
        )
        .unwrap();
        assert_eq!(
            sink.omit(TrafficClass::Normal, WallNs(0)),
            Recorded::DroppedCounted
        );
        assert_eq!(entry_words(96), 14);
        let refused = sink.record_with(TrafficClass::Normal, WallNs(0), 96, &mut || {
            panic!("built though it cannot fit after the marker")
        });
        assert_eq!(refused, Recorded::DroppedCounted);
        assert_eq!(sink.dropped(TrafficClass::Normal), 2);
    }

    #[test]
    fn a_record_built_lazily_is_kept_though_the_sink_closes_while_it_is_built() {
        // Codex r4178725028: the sink is busy from its look at `closed` until its push ends,
        // so a writer closing while the record is built waits for it rather than leaving a
        // record built only to be dropped.
        let (mut sink, _drain) = journal_queue(
            SinkConfig {
                budget_bytes: 256,
                soft_limit_pct: 50,
            },
            key(),
        )
        .unwrap();
        let ring = Arc::clone(&sink.ring);
        let kept = sink.record_with(TrafficClass::Normal, WallNs(0), 0, &mut || {
            assert!(ring.busy.load(SeqCst), "built outside the sink's busy span");
            ring.closed.store(true, SeqCst);
            Record::Marker(Marker::Recovered)
        });
        assert_eq!(kept, Recorded::Ok);
        assert!(!ring.busy.load(SeqCst));
        assert_ne!(ring.used(), 0);
        assert_eq!(sink.dropped(TrafficClass::Normal), 0);
    }

    #[test]
    fn a_record_builder_that_panics_leaves_the_sink_usable_and_the_writer_free_to_close() {
        // Codex r4178802729: a caller that catches the builder's panic keeps the sink, so the
        // busy span must end as the builder unwinds, not when the sink is dropped.
        let root =
            std::env::temp_dir().join(format!("fbc-journal-sink-unwind-{}", std::process::id()));
        let (mut sink, drain) = journal_queue(
            SinkConfig {
                budget_bytes: 256,
                soft_limit_pct: 50,
            },
            key(),
        )
        .unwrap();
        let writer = drain
            .spawn(JournalWriter::create(&root, 1, key()).unwrap())
            .unwrap();
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            sink.record_with(TrafficClass::Normal, WallNs(0), 0, &mut || {
                panic!("builder")
            })
        }));
        assert!(unwound.is_err());
        assert!(
            !sink.ring.busy.load(SeqCst),
            "busy left set by an unwinding builder"
        );
        let recovered = Record::Marker(Marker::Recovered);
        assert_eq!(
            sink.record(TrafficClass::Normal, WallNs(0), &recovered),
            Recorded::Ok
        );
        writer.close().unwrap();
        let read: Vec<Record> = crate::JournalReader::open(&root, 1)
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(read, [recovered]);
        drop(sink);
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn a_closing_writer_waits_out_a_push_in_flight() {
        // Unit tests get no CARGO_TARGET_TMPDIR: a directory of this process's own.
        let root =
            std::env::temp_dir().join(format!("fbc-journal-sink-in-flight-{}", std::process::id()));
        let (sink, drain) = journal_queue(
            SinkConfig {
                budget_bytes: 256,
                soft_limit_pct: 50,
            },
            key(),
        )
        .unwrap();
        let ring = Arc::clone(&sink.ring);
        // The sink is mid-push as the writer sees it close.
        ring.busy.store(true, SeqCst);
        ring.closed.store(true, SeqCst);
        let writer = drain
            .spawn(JournalWriter::create(&root, 1, key()).unwrap())
            .unwrap();
        while ring.writer.get().is_none() {
            thread::yield_now();
        }
        for _ in 0..1_000 {
            thread::yield_now();
        }
        // The push lands, then the sink is done; the writer writes it and stops.
        let mut body = Vec::new();
        format::encode(&Record::Marker(Marker::Recovered), &key(), &mut body).unwrap();
        ring.push(WallNs(0), &body);
        ring.busy.store(false, SeqCst);
        writer.close().unwrap();
        let read: Vec<Record> = crate::JournalReader::open(&root, 1)
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(read, [Record::Marker(Marker::Recovered)]);
        drop(sink);
        std::fs::remove_dir_all(&root).unwrap();
    }
}
