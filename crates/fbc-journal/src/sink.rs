//! The sink the shard journals through, which never blocks (0006, design §5.4), and the writer
//! thread that drains it into a [`JournalWriter`].
//!
//! [`journal_queue`] makes a bounded single-producer single-consumer queue of encoded records
//! with a byte budget the consumer configures ([`SinkConfig`]; no size is chosen here, 0009).
//! Its producer end is a [`QueueSink`]; its consumer end, a [`JournalDrain`], is
//! [spawned](JournalDrain::spawn) onto a thread of its own as a [`WriterThread`].
//!
//! **Admission.** A record is encoded on the caller's thread (so no redacted byte enters the
//! queue) and admitted by its class: a Normal record while the queue stays at or under the
//! soft limit, a Safety record (cancels, reducing orders, acks, fills) while it stays within
//! the whole budget, the part above the soft limit being the Safety reserve. A record that
//! does not fit is dropped and counted by class, and [`record`](JournalSink::record) says so:
//! the caller proceeds either way. A record the format cannot hold, or one larger than the
//! room its class has left, is dropped and counted the same way, and its payload is not
//! copied: encoding stops where the room ends.
//!
//! **The gap.** The sink numbers the records offered to it from 0. The first drop opens a gap;
//! it closes once space returns, that is when a record arrives and the `Degraded` marker and
//! that record both fit under the soft limit: [`Marker::Degraded`] `{ from_seq, dropped }` is
//! written first, naming the first dropped record's number and how many were dropped since,
//! then the record. Safety records admitted from the reserve while the gap is open come before
//! the marker. Markers the sink writes take no number.
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
}

/// Makes the journal queue: the sink the shard records into and the drain a writer thread
/// empties it with.
pub fn journal_queue(config: SinkConfig) -> Result<(QueueSink, JournalDrain), JournalError> {
    let cap = config.budget_bytes / WORD;
    let soft = (cap as u128 * u128::from(config.soft_limit_pct) / 100) as usize;
    let marker = entry_words_of(&Record::Marker(Marker::Degraded {
        from_seq: 0,
        dropped: 0,
    }));
    let smallest = entry_words_of(&Record::Marker(Marker::Recovered));
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
        writer: OnceLock::new(),
    });
    let sink = QueueSink {
        ring: Arc::clone(&ring),
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
    /// No more records are coming: the writer stops once the ring is empty.
    closed: AtomicBool,
    /// The writer thread, set by the thread itself before it can park.
    writer: OnceLock<Thread>,
}

/// The words an entry with a body of `len` bytes takes.
fn entry_words(len: usize) -> usize {
    HEADER_WORDS + len.div_ceil(WORD)
}

/// The words a small fixed record's entry takes.
fn entry_words_of(record: &Record) -> usize {
    let mut body = Vec::new();
    format::encode(record, &mut body).expect("a marker always encodes");
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
}

/// The producer end of the journal queue: the [`JournalSink`] the shard records into.
/// Dropping it lets the writer finish what is queued and stop.
pub struct QueueSink {
    ring: Arc<Ring>,
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
        let seq = self.next_seq;
        self.next_seq += 1;
        self.body.clear();
        let used = self.ring.used();
        // A body too long for the room its class has left is refused before it is copied; a
        // record refused, for that or by the format, needs more room than any queue has.
        let room = match class {
            TrafficClass::Normal => self.soft,
            TrafficClass::Safety => self.ring.words.len(),
        }
        .saturating_sub(used);
        let limit = room.saturating_sub(HEADER_WORDS) * WORD;
        let need = format::encode_within(record, &mut self.body, limit)
            .map_or(usize::MAX, |()| entry_words(self.body.len()));
        let marker = self.gap.as_ref().map_or(0, |gap| {
            self.marker.clear();
            let degraded = Record::Marker(Marker::Degraded {
                from_seq: gap.from_seq,
                dropped: gap.dropped,
            });
            format::encode(&degraded, &mut self.marker).expect("a marker always encodes");
            entry_words(self.marker.len())
        });
        if fits(used, marker.saturating_add(need), self.soft) {
            if self.gap.take().is_some() {
                self.ring.push(now, &self.marker);
            }
            self.ring.push(now, &self.body);
        } else if class == TrafficClass::Safety && fits(used, need, self.ring.words.len()) {
            self.ring.push(now, &self.body);
        } else {
            self.dropped[class_index(class)] += 1;
            self.gap
                .get_or_insert(Gap {
                    from_seq: seq,
                    dropped: 0,
                })
                .dropped += 1;
            return Recorded::DroppedCounted;
        }
        self.ring.wake();
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
        // Read before looking at the ring: a record queued before the sink closed is seen.
        let closing = ring.closed.load(SeqCst);
        if let Some(wall) = ring.pop(&mut body) {
            writer.append_body(wall, &body)?;
            continue;
        }
        writer.flush()?;
        if closing {
            return Ok(());
        }
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
    /// Writes what is queued, flushes, and stops the thread. Records offered after this stay
    /// queued until the queue fills, and are then dropped and counted. Returns the write error
    /// that stopped the thread, if one did.
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

    #[test]
    fn a_record_too_large_for_the_room_left_is_not_copied() {
        // Codex r4176799800: dropping a large frame must not copy it on the shard thread.
        let (mut sink, _drain) = journal_queue(SinkConfig {
            budget_bytes: 256,
            soft_limit_pct: 50,
        })
        .unwrap();
        let big = Record::Inbound {
            stamp: Stamp {
                ingest_seq: 0,
                kernel_rx: None,
                recv_mono: MonoNs(0),
                recv_wall: WallNs(0),
                conn: ConnKey { conn: 1, epoch: 1 },
            },
            opcode: Opcode::Binary,
            bytes: Opaque(vec![7; 1 << 20]),
        };
        for class in [TrafficClass::Normal, TrafficClass::Safety] {
            assert_eq!(
                sink.record(class, WallNs(0), &big),
                Recorded::DroppedCounted
            );
            assert!(sink.body.capacity() <= 512, "{}", sink.body.capacity());
        }
    }
}
