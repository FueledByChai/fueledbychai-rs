//! How the writer lays out segments and how the reader finds them and reports damage.

use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

use fbc_core::{ConnKey, MonoNs, TimerTag, WallNs};
use fbc_journal::format::{MAGIC, VERSION};
use fbc_journal::{JournalError, JournalReader, JournalWriter, Marker, Record};

const SEC: i64 = 1_000_000_000;
/// 2026-10-03T12:00:00Z and the next day at the same time.
const DAY1: WallNs = WallNs(1_791_028_800 * SEC);
const DAY2: WallNs = WallNs(1_791_115_200 * SEC);

fn fresh_dir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    let _ = fs::remove_dir_all(&dir);
    dir
}

fn timer(n: u64) -> Record {
    Record::Timer {
        fired: MonoNs(n),
        conn: ConnKey { conn: 1, epoch: 1 },
        tag: TimerTag(n),
    }
}

fn read_all(root: &Path, shard: u16) -> Vec<Result<Record, JournalError>> {
    JournalReader::open(root, shard).unwrap().collect()
}

fn names(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
}

#[test]
fn a_restarted_writer_starts_the_next_segment_and_never_overwrites() {
    let root = fresh_dir("restart");
    let mut first = JournalWriter::create(&root, 1).unwrap();
    first.append(DAY1, &timer(1)).unwrap();
    first.append(DAY1, &timer(2)).unwrap();
    drop(first);
    let mut second = JournalWriter::create(&root, 1).unwrap();
    second.flush().unwrap(); // nothing open yet
    second.append(DAY1, &timer(3)).unwrap();
    second.flush().unwrap();
    drop(second);

    assert_eq!(
        names(&root.join("20261003")),
        ["1-000000.fbcj", "1-000001.fbcj"]
    );
    let read: Vec<Record> = read_all(&root, 1).into_iter().map(Result::unwrap).collect();
    assert_eq!(read, [timer(1), timer(2), timer(3)]);
}

#[test]
fn a_clock_stepping_back_across_midnight_stays_in_the_later_day() {
    let root = fresh_dir("clock_back");
    let mut w = JournalWriter::create(&root, 1).unwrap();
    w.append(DAY1, &timer(1)).unwrap();
    w.append(DAY2, &timer(2)).unwrap();
    w.append(DAY1, &timer(3)).unwrap();
    w.append(DAY2, &timer(4)).unwrap();
    drop(w);

    assert_eq!(names(&root), ["20261003", "20261004"]);
    assert_eq!(names(&root.join("20261003")), ["1-000000.fbcj"]);
    assert_eq!(names(&root.join("20261004")), ["1-000000.fbcj"]);
    let read: Vec<Record> = read_all(&root, 1).into_iter().map(Result::unwrap).collect();
    assert_eq!(read, [timer(1), timer(2), timer(3), timer(4)]);
}

#[test]
fn a_writer_restarted_on_a_clock_behind_the_journal_stays_in_the_latest_day() {
    // Codex r4176373442: the shard already has a segment under the later day, and the process
    // restarts with the wall clock back in the earlier one.
    let root = fresh_dir("restart_clock_back");
    let mut first = JournalWriter::create(&root, 1).unwrap();
    first.append(DAY2, &timer(1)).unwrap();
    drop(first);
    // Another shard's later day does not hold this shard back.
    let mut other = JournalWriter::create(&root, 2).unwrap();
    other
        .append(WallNs(DAY2.0 + 86_400 * SEC), &timer(9))
        .unwrap();
    drop(other);
    let mut second = JournalWriter::create(&root, 1).unwrap();
    second.append(DAY1, &timer(2)).unwrap();
    second.append(DAY2, &timer(3)).unwrap();
    drop(second);

    assert_eq!(names(&root), ["20261004", "20261005"]);
    assert_eq!(
        names(&root.join("20261004")),
        ["1-000000.fbcj", "1-000001.fbcj"]
    );
    let read: Vec<Record> = read_all(&root, 1).into_iter().map(Result::unwrap).collect();
    assert_eq!(read, [timer(1), timer(2), timer(3)]);
}

#[test]
fn the_reader_passes_over_other_shards_and_files_that_are_not_segments() {
    let root = fresh_dir("strays");
    let mut one = JournalWriter::create(&root, 1).unwrap();
    let mut two = JournalWriter::create(&root, 2).unwrap();
    one.append(DAY1, &timer(1)).unwrap();
    two.append(DAY1, &timer(20)).unwrap();
    one.append(DAY2, &timer(2)).unwrap();
    drop((one, two));
    fs::write(root.join("notes.txt"), b"x").unwrap();
    fs::write(root.join("20261005"), b"a file, not a day").unwrap();
    fs::create_dir(root.join("2026100x")).unwrap();
    fs::create_dir(root.join("202610051")).unwrap();
    fs::write(root.join("20261003/1-000009.zst"), b"x").unwrap();
    fs::write(root.join("20261003/readme"), b"x").unwrap();

    let read: Vec<Record> = read_all(&root, 1).into_iter().map(Result::unwrap).collect();
    assert_eq!(read, [timer(1), timer(2)]);
    let read: Vec<Record> = read_all(&root, 2).into_iter().map(Result::unwrap).collect();
    assert_eq!(read, [timer(20)]);
    assert!(read_all(&root, 3).is_empty());
}

#[test]
fn a_directory_that_cannot_be_made_is_an_io_error() {
    let root = fresh_dir("blocked");
    fs::create_dir_all(&root).unwrap();
    let file = root.join("a-file");
    fs::write(&file, b"x").unwrap();
    assert!(matches!(
        JournalWriter::create(&file, 1),
        Err(JournalError::Io(_))
    ));

    // A file where the day's directory should be.
    fs::write(root.join("20261003"), b"x").unwrap();
    let mut w = JournalWriter::create(&root, 1).unwrap();
    let err = w.append(DAY1, &timer(1)).unwrap_err();
    assert!(matches!(err, JournalError::Io(_)), "{err:?}");
    assert!(err.to_string().starts_with("journal i/o: "));
    assert!(err.source().is_some());

    assert!(matches!(
        JournalReader::open(root.join("missing"), 1),
        Err(JournalError::Io(_))
    ));
}

/// A day directory holding one segment per `(name, bytes)`.
fn damaged(name: &str, segments: &[(&str, Vec<u8>)]) -> PathBuf {
    let root = fresh_dir(name);
    let day = root.join("20261003");
    fs::create_dir_all(&day).unwrap();
    for (file, bytes) in segments {
        fs::write(day.join(file), bytes).unwrap();
    }
    root
}

/// A good segment holding `records`.
fn segment_bytes(records: &[Record]) -> Vec<u8> {
    let root = fresh_dir("scratch-segment");
    let mut w = JournalWriter::create(&root, 1).unwrap();
    for r in records {
        w.append(DAY1, r).unwrap();
    }
    drop(w);
    fs::read(root.join("20261003/1-000000.fbcj")).unwrap()
}

fn header() -> Vec<u8> {
    [&MAGIC[..], &VERSION.to_le_bytes()].concat()
}

#[test]
fn each_damaged_segment_is_reported_once_and_reading_goes_on() {
    let good = segment_bytes(&[timer(1), timer(2)]);
    let last = segment_bytes(&[Record::Marker(Marker::Recovered)]);
    let mut bad_kind = header();
    bad_kind.extend_from_slice(&1u32.to_le_bytes());
    bad_kind.push(99);
    let mut bad_magic = good.clone();
    bad_magic[0] = b'X';
    let mut bad_version = good.clone();
    bad_version[4] = 9;
    let root = damaged(
        "damaged",
        &[
            ("1-000000.fbcj", good.clone()),
            ("1-000001.fbcj", bad_magic),
            ("1-000002.fbcj", bad_version),
            ("1-000003.fbcj", MAGIC[..3].to_vec()),
            // A body cut short, then a length prefix cut short.
            ("1-000004.fbcj", good[..good.len() - 1].to_vec()),
            ("1-000005.fbcj", [&header()[..], &[1, 0]].concat()),
            ("1-000006.fbcj", bad_kind),
            ("1-000007.fbcj", header()),
            ("1-000008.fbcj", last),
        ],
    );
    let segment = |n: u32| root.join(format!("20261003/1-{n:06}.fbcj"));
    let read = read_all(&root, 1);
    let shown: Vec<String> = read
        .iter()
        .map(|r| match r {
            Ok(record) => format!("{record:?}"),
            Err(e) => e.to_string(),
        })
        .collect();
    let s = |n: u32| segment(n).display().to_string();
    assert_eq!(
        shown,
        [
            format!("{:?}", timer(1)),
            format!("{:?}", timer(2)),
            format!("{} is not a journal segment", s(1)),
            format!(
                "{} is in journal format version 9, which this reader does not know",
                s(2)
            ),
            format!("{} ends inside a record", s(3)),
            format!("{:?}", timer(1)),
            format!("{} ends inside a record", s(4)),
            format!("{} ends inside a record", s(5)),
            format!("{} has a malformed record: record kind", s(6)),
            format!("{:?}", Record::Marker(Marker::Recovered)),
        ]
    );
    for r in &read {
        if let Err(e) = r {
            assert!(e.source().is_none(), "{e:?}");
        }
    }
    assert!(matches!(
        &read[4],
        Err(JournalError::Truncated { segment: p }) if *p == segment(3)
    ));
    assert!(matches!(
        &read[8],
        Err(JournalError::Malformed {
            what: "record kind",
            ..
        })
    ));
}

#[test]
fn a_record_too_large_for_the_format_says_so() {
    let e = JournalError::TooLarge;
    assert_eq!(
        e.to_string(),
        "a journal record is too large for its format"
    );
    assert!(e.source().is_none());
}
