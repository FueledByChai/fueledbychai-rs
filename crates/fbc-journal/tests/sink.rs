//! FBC-870's done line: with the writer stalled, `record` never blocks; Normal records are
//! dropped from the soft limit on; Safety records are accepted until the reserve is spent and
//! dropped after; every drop is counted by class; and once the writer resumes, a `Degraded`
//! marker carrying the first dropped sequence and the drop count is written before the next
//! record (0006, design §5.4).

use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use fbc_core::{ConnKey, MonoNs, Stamp, TimerTag, TrafficClass, WallNs};
use fbc_journal::{
    JournalError, JournalReader, JournalSink, JournalWriter, Marker, Opaque, Opcode, QueueSink,
    Record, Recorded, SinkConfig, journal_queue,
};

/// The key the journal hashes redaction spans under in these tests.
fn key() -> std::sync::Arc<fbc_journal::RedactionKey> {
    std::sync::Arc::new(fbc_journal::RedactionKey::new(&[9; 32]).unwrap())
}

const SEC: i64 = 1_000_000_000;
/// 2026-10-03T12:00:00Z.
const NOW: WallNs = WallNs(1_791_028_800 * SEC);
/// How long a test waits for something that, done right, takes microseconds. Only a sink that
/// blocks or a writer that never drains runs into it.
const PATIENCE: Duration = Duration::from_secs(20);

fn fresh_dir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    let _ = fs::remove_dir_all(&dir);
    dir
}

/// A record whose timer tag is its sequence number at the sink, so the journal shows which
/// records were kept.
fn timer(seq: u64) -> Record {
    Record::Timer {
        stamp: Stamp {
            ingest_seq: seq,
            kernel_rx: None,
            recv_mono: MonoNs(seq),
            recv_wall: WallNs(NOW.0 + seq as i64),
            conn: ConnKey { conn: 1, epoch: 1 },
        },
        tag: TimerTag(seq),
    }
}

fn read_all(root: &Path, shard: u16) -> Vec<Record> {
    JournalReader::open(root, shard)
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

/// Waits until the writer has taken every queued record.
fn until_drained(sink: &QueueSink) {
    let start = std::time::Instant::now();
    while sink.queued_bytes() > 0 {
        assert!(
            start.elapsed() < PATIENCE,
            "the writer never drained the queue"
        );
        thread::yield_now();
    }
}

#[test]
fn a_stalled_writer_drops_normal_then_safety_records_counted_and_marks_the_gap() {
    let root = fresh_dir("sink_stalled");
    let config = SinkConfig {
        budget_bytes: 4096,
        soft_limit_pct: 75,
    };
    let (mut sink, drain) = journal_queue(config, key()).unwrap();

    // The writer is stalled: nothing drains the queue. The sink runs on its own thread so that
    // a `record` that blocked fails the test (no report within PATIENCE) instead of hanging it.
    let (done, report) = mpsc::channel();
    let shard = thread::spawn(move || {
        let mut seq = 0u64;
        let mut offer = |sink: &mut QueueSink, class| {
            let r = sink.record(class, NOW, &timer(seq));
            seq += 1;
            (seq - 1, r)
        };
        // Normal records until the soft limit refuses one.
        let mut kept_normal = Vec::new();
        let first_drop = loop {
            match offer(&mut sink, TrafficClass::Normal) {
                (s, Recorded::Ok) => kept_normal.push(s),
                (s, Recorded::DroppedCounted) => break s,
            }
        };
        let at_soft_limit = sink.queued_bytes();
        // Normal records stay refused from the soft limit on.
        for _ in 0..5 {
            assert_eq!(
                offer(&mut sink, TrafficClass::Normal).1,
                Recorded::DroppedCounted
            );
        }
        // Safety records use the reserve until it is spent...
        let mut kept_safety = Vec::new();
        while let (s, Recorded::Ok) = offer(&mut sink, TrafficClass::Safety) {
            kept_safety.push(s);
        }
        // ...and are dropped after it, as are Normal records.
        for _ in 0..3 {
            assert_eq!(
                offer(&mut sink, TrafficClass::Safety).1,
                Recorded::DroppedCounted
            );
            assert_eq!(
                offer(&mut sink, TrafficClass::Normal).1,
                Recorded::DroppedCounted
            );
        }
        done.send(()).unwrap();
        (
            sink,
            seq,
            first_drop,
            kept_normal,
            kept_safety,
            at_soft_limit,
        )
    });
    report
        .recv_timeout(PATIENCE)
        .expect("record blocked on a stalled writer");
    let (mut sink, next_seq, first_drop, kept_normal, kept_safety, at_soft_limit) =
        shard.join().unwrap();

    // The soft limit is 75% of the budget, and the reserve held Safety records beyond it.
    assert!(!kept_normal.is_empty() && !kept_safety.is_empty());
    assert!(at_soft_limit <= 3072, "{at_soft_limit}");
    assert!(sink.queued_bytes() <= 4096 && sink.queued_bytes() > at_soft_limit);
    // Every drop is counted by class: 1 + 5 + 3 Normal, 1 + 3 Safety.
    assert_eq!(sink.dropped(TrafficClass::Normal), 9);
    assert_eq!(sink.dropped(TrafficClass::Safety), 4);

    // The writer resumes and drains; the next record finds space under the soft limit.
    let writer = drain
        .spawn(JournalWriter::create(&root, 1, key()).unwrap())
        .unwrap();
    until_drained(&sink);
    assert_eq!(
        sink.record(TrafficClass::Normal, NOW, &timer(next_seq)),
        Recorded::Ok
    );
    writer.close().unwrap();
    // The counts stay: they are totals, not the open gap.
    assert_eq!(sink.dropped(TrafficClass::Normal), 9);
    assert_eq!(sink.dropped(TrafficClass::Safety), 4);

    let mut expected: Vec<Record> = kept_normal.iter().map(|&s| timer(s)).collect();
    expected.extend(kept_safety.iter().map(|&s| timer(s)));
    expected.push(Record::Marker(Marker::Degraded {
        from_seq: first_drop,
        dropped: 13,
    }));
    expected.push(timer(next_seq));
    assert_eq!(read_all(&root, 1), expected);
}

#[test]
fn a_running_writer_keeps_up_and_wakes_for_each_record() {
    let root = fresh_dir("sink_running");
    let (mut sink, drain) = journal_queue(
        SinkConfig {
            budget_bytes: 512,
            soft_limit_pct: 85,
        },
        key(),
    )
    .unwrap();
    let writer = drain
        .spawn(JournalWriter::create(&root, 2, key()).unwrap())
        .unwrap();
    // Many times the queue's size, one record at a time, the writer idle in between: the ring
    // wraps over and over and the writer is woken each time.
    let n = 200;
    for seq in 0..n {
        let class = if seq % 2 == 0 {
            TrafficClass::Normal
        } else {
            TrafficClass::Safety
        };
        assert_eq!(sink.record(class, NOW, &timer(seq)), Recorded::Ok);
        until_drained(&sink);
    }
    // Dropping the sink lets the writer finish what is queued and stop.
    assert_eq!(
        sink.record(TrafficClass::Normal, NOW, &timer(n)),
        Recorded::Ok
    );
    drop(sink);
    writer.close().unwrap();
    let expected: Vec<Record> = (0..=n).map(timer).collect();
    assert_eq!(read_all(&root, 2), expected);
}

#[test]
fn a_writer_draining_while_the_sink_floods_loses_nothing_it_admitted() {
    // The two ends run at once and the ring wraps thousands of times: every admitted record is
    // read back once and in order, and the markers account for every drop.
    let root = fresh_dir("sink_flood");
    let (mut sink, drain) = journal_queue(
        SinkConfig {
            budget_bytes: 1024,
            soft_limit_pct: 85,
        },
        key(),
    )
    .unwrap();
    let writer = drain
        .spawn(JournalWriter::create(&root, 3, key()).unwrap())
        .unwrap();
    let mut admitted = Vec::new();
    let mut dropped = Vec::new();
    for seq in 0..20_000 {
        let class = if seq % 3 == 0 {
            TrafficClass::Safety
        } else {
            TrafficClass::Normal
        };
        match sink.record(class, NOW, &timer(seq)) {
            Recorded::Ok => admitted.push(seq),
            Recorded::DroppedCounted => dropped.push(seq),
        }
    }
    until_drained(&sink);
    // Closes any open gap.
    assert_eq!(
        sink.record(TrafficClass::Normal, NOW, &timer(20_000)),
        Recorded::Ok
    );
    admitted.push(20_000);
    let drops = sink.dropped(TrafficClass::Normal) + sink.dropped(TrafficClass::Safety);
    assert_eq!(drops, dropped.len() as u64);
    drop(sink);
    writer.close().unwrap();

    let mut kept = Vec::new();
    let mut marked = 0;
    let mut last_from = None;
    for record in read_all(&root, 3) {
        match record {
            Record::Timer { tag, .. } => kept.push(tag.0),
            Record::Marker(Marker::Degraded {
                from_seq,
                dropped: n,
            }) => {
                assert!(dropped.contains(&from_seq), "{from_seq}");
                assert!(last_from < Some(from_seq));
                last_from = Some(from_seq);
                marked += n;
            }
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(kept, admitted);
    assert_eq!(marked, drops);
}

#[test]
fn close_ends_with_a_busy_sink_and_refuses_what_comes_after() {
    // Codex r4176868171: a sink that keeps the queue full must not hold close() up, and every
    // record the sink accepted is written.
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    let root = fresh_dir("sink_close_busy");
    let (mut sink, drain) = journal_queue(
        SinkConfig {
            budget_bytes: 1024,
            soft_limit_pct: 85,
        },
        key(),
    )
    .unwrap();
    let writer = drain
        .spawn(JournalWriter::create(&root, 4, key()).unwrap())
        .unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let accepted = Arc::new(AtomicU64::new(0));
    let producer = thread::spawn({
        let (stop, accepted) = (Arc::clone(&stop), Arc::clone(&accepted));
        move || {
            let mut kept = Vec::new();
            let mut seq = 0;
            while !stop.load(Ordering::SeqCst) {
                if sink.record(TrafficClass::Normal, NOW, &timer(seq)) == Recorded::Ok {
                    kept.push(seq);
                    accepted.fetch_add(1, Ordering::SeqCst);
                }
                seq += 1;
            }
            (sink, kept, seq)
        }
    });
    let start = std::time::Instant::now();
    while accepted.load(Ordering::SeqCst) < 1_000 {
        assert!(start.elapsed() < PATIENCE, "the producer never got going");
        thread::yield_now();
    }
    let (closed, closing) = mpsc::channel();
    let closer = thread::spawn(move || closed.send(writer.close()).unwrap());
    let result = closing
        .recv_timeout(PATIENCE)
        .expect("close never returned while the sink kept producing");
    result.unwrap();
    closer.join().unwrap();
    stop.store(true, Ordering::SeqCst);
    let (mut sink, kept, seq) = producer.join().unwrap();

    // After close, the sink refuses and counts.
    let before = sink.dropped(TrafficClass::Safety);
    assert_eq!(
        sink.record(TrafficClass::Safety, NOW, &timer(seq)),
        Recorded::DroppedCounted
    );
    assert_eq!(sink.dropped(TrafficClass::Safety), before + 1);
    let written: Vec<u64> = read_all(&root, 4)
        .into_iter()
        .filter_map(|r| match r {
            Record::Timer { tag, .. } => Some(tag.0),
            _ => None,
        })
        .collect();
    assert_eq!(written, kept);
}

#[test]
fn a_gap_still_open_at_shutdown_is_marked_last() {
    // Codex r4176902253: records dropped and no record after them before shutdown; the
    // journal must still say so. Once by closing the writer, once by dropping the sink.
    for (name, by_close) in [("sink_gap_close", true), ("sink_gap_drop", false)] {
        let root = fresh_dir(name);
        let (mut sink, drain) = journal_queue(
            SinkConfig {
                budget_bytes: 512,
                soft_limit_pct: 50,
            },
            key(),
        )
        .unwrap();
        let mut kept = Vec::new();
        let mut seq = 0;
        let first_drop = loop {
            if sink.record(TrafficClass::Normal, NOW, &timer(seq)) == Recorded::DroppedCounted {
                break seq;
            }
            kept.push(timer(seq));
            seq += 1;
        };
        assert_eq!(
            sink.record(TrafficClass::Normal, NOW, &timer(seq + 1)),
            Recorded::DroppedCounted
        );
        let writer = drain
            .spawn(JournalWriter::create(&root, 1, key()).unwrap())
            .unwrap();
        if by_close {
            writer.close().unwrap();
            // Drops after close are counted, not journaled.
            assert_eq!(
                sink.record(TrafficClass::Safety, NOW, &timer(seq + 2)),
                Recorded::DroppedCounted
            );
            assert_eq!(sink.dropped(TrafficClass::Safety), 1);
        } else {
            drop(sink);
            writer.close().unwrap();
        }
        kept.push(Record::Marker(Marker::Degraded {
            from_seq: first_drop,
            dropped: 2,
        }));
        assert_eq!(read_all(&root, 1), kept, "{name}");
    }
}

#[test]
fn a_record_the_queue_or_the_format_cannot_hold_is_dropped_and_counted() {
    let root = fresh_dir("sink_unholdable");
    let (mut sink, drain) = journal_queue(
        SinkConfig {
            budget_bytes: 256,
            soft_limit_pct: 50,
        },
        key(),
    )
    .unwrap();
    let big = Record::Inbound {
        stamp: match timer(0) {
            Record::Timer { stamp, .. } => stamp,
            _ => unreachable!(),
        },
        opcode: Opcode::Binary,
        bytes: Opaque(vec![7; 300]),
        redact: Vec::new(),
    };
    let not_utf8 = Record::Inbound {
        stamp: match timer(1) {
            Record::Timer { stamp, .. } => stamp,
            _ => unreachable!(),
        },
        opcode: Opcode::Text,
        bytes: Opaque(vec![0xff]),
        redact: Vec::new(),
    };
    assert_eq!(
        sink.record(TrafficClass::Safety, NOW, &big),
        Recorded::DroppedCounted
    );
    assert_eq!(
        sink.record(TrafficClass::Normal, NOW, &not_utf8),
        Recorded::DroppedCounted
    );
    assert_eq!(sink.dropped(TrafficClass::Safety), 1);
    assert_eq!(sink.dropped(TrafficClass::Normal), 1);
    assert_eq!(sink.queued_bytes(), 0);
    assert_eq!(
        sink.record(TrafficClass::Normal, NOW, &timer(2)),
        Recorded::Ok
    );
    let writer = drain
        .spawn(JournalWriter::create(&root, 1, key()).unwrap())
        .unwrap();
    writer.close().unwrap();
    assert_eq!(
        read_all(&root, 1),
        [
            Record::Marker(Marker::Degraded {
                from_seq: 0,
                dropped: 2
            }),
            timer(2)
        ]
    );
}

#[test]
fn a_writer_that_cannot_write_reports_it_and_the_sink_keeps_counting() {
    let root = fresh_dir("sink_failing");
    fs::create_dir_all(&root).unwrap();
    // A file where the day's directory should be.
    fs::write(root.join("20261003"), b"x").unwrap();
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
    assert_eq!(
        sink.record(TrafficClass::Normal, NOW, &timer(0)),
        Recorded::Ok
    );
    // The writer stops at the error, so the queue fills and the sink drops and counts.
    let mut seq = 1;
    while sink.record(TrafficClass::Safety, NOW, &timer(seq)) == Recorded::Ok {
        seq += 1;
    }
    assert_eq!(sink.dropped(TrafficClass::Safety), 1);
    let err = writer.close().unwrap_err();
    assert!(matches!(err, JournalError::Io(_)), "{err:?}");
}

#[test]
fn a_configuration_the_queue_cannot_use_is_refused() {
    const SOFT: &str = "a soft limit too small to hold a Degraded marker and a record";
    const RESERVE: &str = "a Safety reserve too small to hold a record";
    for (budget_bytes, soft_limit_pct, why) in [
        (7, 85, SOFT),
        (4096, 0, SOFT),
        // Codex r4176799797: six words of soft limit, and a marker and the smallest record
        // need eight, so a gap could never close.
        (64, 85, SOFT),
        // Codex r4176799793: no reserve at all.
        (4096, 100, RESERVE),
        (4096, 101, RESERVE),
        // A reserve of one word holds no record.
        (800, 99, RESERVE),
    ] {
        let err = journal_queue(
            SinkConfig {
                budget_bytes,
                soft_limit_pct,
            },
            key(),
        )
        .err()
        .unwrap();
        assert!(
            matches!(err, JournalError::Config(w) if w == why),
            "{err:?}"
        );
        assert_eq!(
            err.to_string(),
            format!("the journal cannot use its configuration: {why}")
        );
        assert!(err.source().is_none());
    }
}

#[test]
fn the_smallest_configuration_accepted_can_close_a_gap() {
    // Eight words of soft limit: a Degraded marker (five) and the smallest record (three).
    let root = fresh_dir("sink_smallest");
    let (mut sink, drain) = journal_queue(
        SinkConfig {
            budget_bytes: 88,
            soft_limit_pct: 73,
        },
        key(),
    )
    .unwrap();
    let stamp = match timer(0) {
        Record::Timer { stamp, .. } => stamp,
        _ => unreachable!(),
    };
    let too_big = Record::Inbound {
        stamp,
        opcode: Opcode::Binary,
        bytes: Opaque(vec![1; 100]),
        redact: Vec::new(),
    };
    assert_eq!(
        sink.record(TrafficClass::Normal, NOW, &too_big),
        Recorded::DroppedCounted
    );
    let small = Record::Marker(Marker::Recovered);
    assert_eq!(sink.record(TrafficClass::Normal, NOW, &small), Recorded::Ok);
    let writer = drain
        .spawn(JournalWriter::create(&root, 1, key()).unwrap())
        .unwrap();
    writer.close().unwrap();
    assert_eq!(
        read_all(&root, 1),
        [
            Record::Marker(Marker::Degraded {
                from_seq: 0,
                dropped: 1
            }),
            small
        ]
    );
}

/// A borrowed inbound frame of `bytes`, stamped `seq`.
fn frame(seq: u64, bytes: &[u8]) -> fbc_journal::RecordRef<'_> {
    fbc_journal::RecordRef::Inbound {
        stamp: Stamp {
            ingest_seq: seq,
            kernel_rx: None,
            recv_mono: MonoNs(1),
            recv_wall: NOW,
            conn: ConnKey { conn: 1, epoch: 1 },
        },
        frame: fbc_core::RawFrame::Binary(bytes),
    }
}

/// Codex r4178252055 (FBC-f3w): a record offered borrowed is encoded from what it borrows, so
/// one whose payload cannot fit the room its class has left is dropped and counted before it
/// is copied, and one that fits is journaled as the owned record it stands for; the gap a drop
/// opens is marked like any other, and a record the caller withholds is numbered into it.
#[test]
fn a_record_offered_borrowed_is_journaled_as_its_owned_record_or_dropped_unbuilt() {
    let root = fresh_dir("sink_borrowed");
    let config = SinkConfig {
        budget_bytes: 1024,
        soft_limit_pct: 50,
    };
    let (mut sink, drain) = journal_queue(config, key()).unwrap();
    let small = frame(0, b"tick");
    assert_eq!(
        sink.record_ref(TrafficClass::Normal, NOW, small),
        Recorded::Ok
    );
    // More than the whole budget, whatever its class.
    let big = vec![7; 4096];
    for class in [TrafficClass::Normal, TrafficClass::Safety] {
        let refused = sink.record_ref(class, NOW, frame(1, &big));
        assert_eq!(refused, Recorded::DroppedCounted);
        assert_eq!(sink.dropped(class), 1);
    }
    assert_eq!(
        sink.omit(TrafficClass::Normal, NOW),
        Recorded::DroppedCounted
    );
    assert_eq!(sink.dropped(TrafficClass::Normal), 2);
    let writer = drain
        .spawn(JournalWriter::create(&root, 1, key()).unwrap())
        .unwrap();
    until_drained(&sink);
    let owned = timer(4);
    assert_eq!(
        sink.record_ref(TrafficClass::Normal, NOW, (&owned).into()),
        Recorded::Ok
    );
    writer.close().unwrap();
    let degraded = Record::Marker(Marker::Degraded {
        from_seq: 1,
        dropped: 3,
    });
    assert_eq!(read_all(&root, 1), [small.to_record(), degraded, owned]);
    fs::remove_dir_all(&root).unwrap();
}

/// Codex r4178427389 (FBC-f3w): once the writer is closed, a record offered borrowed is
/// refused and counted without being encoded, however small.
#[test]
fn a_closed_sink_refuses_a_record_offered_borrowed() {
    let root = fresh_dir("sink_borrowed_closed");
    let config = SinkConfig {
        budget_bytes: 1024,
        soft_limit_pct: 50,
    };
    let (mut sink, drain) = journal_queue(config, key()).unwrap();
    let writer = drain
        .spawn(JournalWriter::create(&root, 1, key()).unwrap())
        .unwrap();
    writer.close().unwrap();
    for class in [TrafficClass::Normal, TrafficClass::Safety] {
        let refused = sink.record_ref(class, NOW, frame(0, b"tick"));
        assert_eq!(refused, Recorded::DroppedCounted);
        assert_eq!(sink.dropped(class), 1);
        assert_eq!(sink.queued_bytes(), 0);
    }
    fs::remove_dir_all(&root).unwrap();
}

/// A sink that keeps what it is offered, for the trait's default borrowed offer.
struct Keeps(Vec<Record>);

impl JournalSink for Keeps {
    fn record(&mut self, _: TrafficClass, _: WallNs, record: &Record) -> Recorded {
        self.0.push(record.clone());
        Recorded::Ok
    }

    fn omit(&mut self, _: TrafficClass, _: WallNs) -> Recorded {
        Recorded::DroppedCounted
    }
}

/// A sink that does not encode borrowed records itself records the owned record each stands
/// for.
#[test]
fn a_sink_by_default_records_the_owned_record_a_borrowed_one_stands_for() {
    let mut sink = Keeps(Vec::new());
    let owned = timer(1);
    let offers = [frame(0, b"tick"), (&owned).into()];
    for offer in offers {
        assert_eq!(
            sink.record_ref(TrafficClass::Normal, NOW, offer),
            Recorded::Ok
        );
    }
    assert_eq!(
        sink.omit(TrafficClass::Normal, NOW),
        Recorded::DroppedCounted
    );
    assert_eq!(sink.0, [offers[0].to_record(), owned]);
}
