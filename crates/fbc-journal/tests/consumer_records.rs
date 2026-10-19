//! FBC-hxkm's done line, the journal's part (decision 0101): consumer records, the consumer's
//! own inputs and annotations, written through the `JournalSink` interleaved with market-data
//! records read back in place, between the records they were written between, with their
//! kind, input flag and bytes unchanged; each kept or dropped under the traffic class its
//! writer picked; and the format version they arrived in refused by a reader of the version
//! before it, by name, at the segment's header (the reader's own check is in `src/reader.rs`).

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fbc_core::{
    ConnKey, Feed, InstrumentId, MonoNs, RawFrame, Stamp, Subscription, TimerTag, TrafficClass,
    WallNs,
};
use fbc_journal::format::{MAGIC, VERSION};
use fbc_journal::{
    ControlEvent, Entry, JournalError, JournalReader, JournalSink, JournalWriter, Marker, Opaque,
    Record, RecordRef, Recorded, RedactionKey, SinkConfig, journal_queue,
};

const SEC: i64 = 1_000_000_000;
/// 2026-10-03T12:00:00Z and an hour later: the journal rolls between them, so the first
/// segment is read back closed and compressed.
const NOON: WallNs = WallNs(1_791_028_800 * SEC);
const ONE_PM: WallNs = WallNs((1_791_028_800 + 3_600) * SEC);

fn key() -> Arc<RedactionKey> {
    Arc::new(RedactionKey::new(&[3; 32]).unwrap())
}

fn fresh_dir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    let _ = fs::remove_dir_all(&dir);
    dir
}

fn conn() -> ConnKey {
    ConnKey { conn: 4, epoch: 1 }
}

fn stamp(ingest_seq: u64, wall: WallNs) -> Stamp {
    Stamp {
        ingest_seq,
        kernel_rx: None,
        recv_mono: MonoNs(ingest_seq * 10),
        recv_wall: wall,
        conn: conn(),
    }
}

fn consumer(at: u64, kind: u16, input: bool, bytes: &[u8]) -> Record {
    Record::Consumer {
        at: MonoNs(at),
        kind,
        input,
        ingest_seq: None,
        bytes: Opaque(bytes.to_vec()),
    }
}

fn read_all(root: &Path, shard: u16) -> Vec<Entry> {
    JournalReader::open(root, shard)
        .unwrap()
        .entries()
        .collect::<Result<_, _>>()
        .unwrap()
}

/// The format version in the header of the segment at `path`.
fn version_at(path: &Path) -> u16 {
    let bytes = fs::read(path).unwrap();
    assert_eq!(bytes[..4], MAGIC);
    u16::from_le_bytes([bytes[4], bytes[5]])
}

#[test]
fn consumer_records_read_back_in_place_among_market_data_records() {
    let root = fresh_dir("consumer_in_place");
    let config = SinkConfig {
        budget_bytes: 1 << 20,
        soft_limit_pct: 85,
    };
    let (mut sink, drain) = journal_queue(config, key()).unwrap();
    let writer = drain
        .spawn(JournalWriter::create(&root, 2, key()).unwrap())
        .unwrap();
    let book = "{\"channel\":\"book\",\"px\":\"101.5\"}";
    let binary = [0u8, 0xfe, 0xff, 0x80];
    let not_utf8 = [0xffu8, 0xc3, 0x28, 0x00];
    let subscribe = [Subscription {
        inst: InstrumentId::new(7),
        feed: Feed::Trades,
    }];
    // What the shard offers, in its total order: market data, connection changes and decide
    // boundaries the runtime writes, and the consumer's inputs and annotations between them,
    // each under the class its writer picked.
    let offered: Vec<(WallNs, TrafficClass, Record)> = vec![
        (
            NOON,
            TrafficClass::Safety,
            Record::Marker(Marker::SessionStart {
                header: Opaque(b"consumer header".to_vec()),
            }),
        ),
        // A configuration hash, before anything connects.
        (
            NOON,
            TrafficClass::Safety,
            consumer(1, 3, true, b"cfg#9f2a"),
        ),
        (
            NOON,
            TrafficClass::Normal,
            Record::Control {
                at: MonoNs(2),
                ev: ControlEvent::Opened(conn()),
            },
        ),
        (
            NOON,
            TrafficClass::Normal,
            RecordRef::Subscribe {
                at: MonoNs(3),
                conn: conn(),
                add: &subscribe,
                remove: &[],
            }
            .to_record(),
        ),
        (
            NOON,
            TrafficClass::Normal,
            Record::inbound(stamp(1, NOON), RawFrame::Text(book)),
        ),
        // An operator command between two frames.
        (NOON, TrafficClass::Safety, consumer(15, 1, true, b"Start")),
        (
            NOON,
            TrafficClass::Normal,
            Record::inbound(stamp(2, NOON), RawFrame::Binary(&binary)),
        ),
        (
            NOON,
            TrafficClass::Normal,
            Record::Cycle {
                last_ingest_seq: 2,
                instruments: vec![InstrumentId::new(7)],
            },
        ),
        // The decide pass's summary, an annotation, right after its boundary; then one with
        // no bytes, of the largest kind.
        (
            NOON,
            TrafficClass::Normal,
            consumer(21, 0, false, &not_utf8),
        ),
        (
            ONE_PM,
            TrafficClass::Normal,
            consumer(30, u16::MAX, false, b""),
        ),
        (
            ONE_PM,
            TrafficClass::Normal,
            Record::Timer {
                stamp: stamp(3, ONE_PM),
                tag: TimerTag(8),
            },
        ),
        // A calendar reload, an input of no bytes.
        (ONE_PM, TrafficClass::Safety, consumer(40, 2, true, b"")),
        (
            ONE_PM,
            TrafficClass::Normal,
            Record::Control {
                at: MonoNs(50),
                ev: ControlEvent::Closed(conn()),
            },
        ),
    ];
    for (i, (now, class, record)) in offered.iter().enumerate() {
        // The frames are offered borrowed, as a session offers them.
        let recorded = match record {
            Record::Inbound { stamp, .. } if stamp.ingest_seq == 1 => sink.record_ref(
                *class,
                *now,
                RecordRef::Inbound {
                    stamp: *stamp,
                    frame: RawFrame::Text(book),
                    redact: &[],
                },
            ),
            Record::Control {
                at,
                ev: ControlEvent::Subscribe { conn, .. },
            } => sink.record_ref(
                *class,
                *now,
                RecordRef::Subscribe {
                    at: *at,
                    conn: *conn,
                    add: &subscribe,
                    remove: &[],
                },
            ),
            _ => sink.record(*class, *now, record),
        };
        assert_eq!(recorded, Recorded::Ok, "record {i}");
    }
    writer.close().unwrap();

    // The first hour's segment was closed and compressed; both are in the current version.
    let day = root.join("20261003");
    assert!(day.join("2-000000.fbcj.zst").exists());
    assert_eq!(version_at(&day.join("2-000001.fbcj")), VERSION);

    let read = read_all(&root, 2);
    assert_eq!(read.len(), offered.len());
    for ((_, _, written), entry) in offered.iter().zip(&read) {
        assert_eq!(entry.record, written.blanked());
        if let Record::Consumer { .. } = written {
            // Nothing in a consumer record is redacted: it comes back exactly as written.
            assert_eq!(entry.record, *written);
            assert!(entry.digests.is_empty());
        }
    }
    // Each consumer record lies between the records it was written between.
    let places: Vec<(usize, u16, bool, Vec<u8>)> = read
        .iter()
        .enumerate()
        .filter_map(|(i, e)| match &e.record {
            Record::Consumer {
                kind, input, bytes, ..
            } => Some((i, *kind, *input, bytes.0.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(
        places,
        [
            (1, 3, true, b"cfg#9f2a".to_vec()),
            (5, 1, true, b"Start".to_vec()),
            (8, 0, false, not_utf8.to_vec()),
            (9, u16::MAX, false, Vec::new()),
            (11, 2, true, Vec::new()),
        ]
    );
    // A consumer record shows its bytes by length only.
    let shown = format!("{:?}", read[5].record);
    assert!(
        !shown.contains("Start") && shown.contains("len: 5"),
        "{shown}"
    );
}

#[test]
fn a_consumer_record_is_kept_or_dropped_under_the_class_its_writer_picked() {
    let root = fresh_dir("consumer_class");
    let config = SinkConfig {
        budget_bytes: 4096,
        soft_limit_pct: 50,
    };
    // Nothing drains the queue yet: Normal records stop at the soft limit.
    let (mut sink, drain) = journal_queue(config, key()).unwrap();
    let mut kept = Vec::new();
    for seq in 0.. {
        let timer = Record::Timer {
            stamp: stamp(seq, NOON),
            tag: TimerTag(seq),
        };
        if sink.record(TrafficClass::Normal, NOON, &timer) == Recorded::DroppedCounted {
            break;
        }
        kept.push(timer);
    }
    let annotation = consumer(1, 7, false, b"decide summary");
    let input = consumer(2, 1, true, b"Stop");
    assert_eq!(
        sink.record(TrafficClass::Normal, NOON, &annotation),
        Recorded::DroppedCounted
    );
    assert_eq!(
        sink.record(TrafficClass::Safety, NOON, &input),
        Recorded::Ok
    );
    assert_eq!(sink.dropped(TrafficClass::Normal), 2);
    assert_eq!(sink.dropped(TrafficClass::Safety), 0);
    let writer = drain
        .spawn(JournalWriter::create(&root, 1, key()).unwrap())
        .unwrap();
    drop(sink);
    writer.close().unwrap();

    let read: Vec<Record> = read_all(&root, 1).into_iter().map(|e| e.record).collect();
    let mut expected = kept;
    expected.push(input);
    // The two drops (the timer at the soft limit and the annotation) close as one gap.
    expected.push(Record::Marker(Marker::Degraded {
        from_seq: expected.len() as u64 - 1,
        dropped: 2,
    }));
    assert_eq!(read, expected);
}

#[test]
fn a_segment_of_an_earlier_version_holding_a_consumer_record_is_malformed() {
    let root = fresh_dir("consumer_in_v7");
    let mut writer = JournalWriter::create(&root, 1, key()).unwrap();
    writer
        .append(
            NOON,
            &Record::Timer {
                stamp: stamp(1, NOON),
                tag: TimerTag(1),
            },
        )
        .unwrap();
    writer
        .append(NOON, &consumer(2, 1, true, b"Start"))
        .unwrap();
    drop(writer);
    let path = root.join("20261003/1-000000.fbcj");
    assert_eq!(version_at(&path), VERSION);
    assert_eq!(VERSION, 8);
    // Read as written, then relabelled as each earlier version: no writer of those wrote one,
    // so the segment is damaged, not newer. The record before it reads as it did.
    let back: Vec<_> = JournalReader::open(&root, 1).unwrap().collect();
    assert!(back.iter().all(Result::is_ok), "{back:?}");
    let mut bytes = fs::read(&path).unwrap();
    for version in [2, 7] {
        bytes[4..6].copy_from_slice(&u16::to_le_bytes(version));
        fs::write(&path, &bytes).unwrap();
        let back: Vec<_> = JournalReader::open(&root, 1).unwrap().collect();
        assert_eq!(back.len(), 2, "{back:?}");
        assert!(matches!(back[0], Ok(Record::Timer { .. })), "{back:?}");
        match &back[1] {
            Err(JournalError::Malformed { segment, what }) => {
                assert_eq!(*what, "record kind");
                assert_eq!(*segment, path);
            }
            other => panic!("{other:?}"),
        }
    }
}

/// A reader refuses a segment of a version newer than its own at the segment's header, naming
/// the version: the check a reader of version 7 makes of a version 8 segment, made here by the
/// version 8 reader of a version 9 one (the version 7 reader itself is in `src/reader.rs`).
#[test]
fn a_reader_refuses_a_newer_version_at_the_header_naming_it() {
    let root = fresh_dir("consumer_newer");
    let mut writer = JournalWriter::create(&root, 1, key()).unwrap();
    writer
        .append(NOON, &consumer(1, 1, true, b"Start"))
        .unwrap();
    drop(writer);
    let path = root.join("20261003/1-000000.fbcj");
    let mut bytes = fs::read(&path).unwrap();
    let newer = VERSION + 1;
    bytes[4..6].copy_from_slice(&newer.to_le_bytes());
    fs::write(&path, &bytes).unwrap();
    let back: Vec<_> = JournalReader::open(&root, 1).unwrap().collect();
    assert_eq!(back.len(), 1, "{back:?}");
    let Err(e @ JournalError::UnsupportedVersion { segment, version }) = &back[0] else {
        panic!("{back:?}");
    };
    assert_eq!((segment, *version), (&path, newer));
    let said = e.to_string();
    assert!(
        said.contains(&format!("journal format version {newer},")),
        "{said}"
    );
    assert!(said.contains(&path.display().to_string()), "{said}");
}
