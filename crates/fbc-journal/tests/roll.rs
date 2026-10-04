//! The hourly roll and zstd-compressed closed segments (FBC-g67, decision 0006): the writer
//! rolls the open segment at every UTC hour and day boundary of the caller's time, compresses
//! each segment it closes, and the reader reads the closed compressed segments and the open
//! uncompressed one in write order.

use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fbc_core::{ConnKey, MonoNs, Stamp, TimerTag, WallNs};
use fbc_journal::format::{MAGIC, VERSION};
use fbc_journal::{
    COMPRESSED_EXT, JournalError, JournalReader, JournalWriter, Marker, Opaque, Opcode, Record,
    RedactionKey, SEGMENT_EXT,
};

fn key() -> Arc<RedactionKey> {
    Arc::new(RedactionKey::new(&[7; 32]).unwrap())
}

const SEC: i64 = 1_000_000_000;
const MIN: i64 = 60 * SEC;
const HOUR: i64 = 60 * MIN;
/// 2026-10-03T00:00:00Z.
const DAY1_START: i64 = 1_791_072_000 * SEC - 86_400 * SEC;

/// 2026-10-03 at `h:m` UTC; `h` = 24 is the next day.
fn at(h: i64, m: i64) -> WallNs {
    WallNs(DAY1_START + h * HOUR + m * MIN)
}

/// The zstd frame magic, little-endian 0xFD2FB528 (RFC 8878 §3.1.1).
const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

fn fresh_dir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("roll-{name}"));
    let _ = fs::remove_dir_all(&dir);
    dir
}

fn stamp(n: u64, wall: WallNs) -> Stamp {
    Stamp {
        ingest_seq: n,
        kernel_rx: None,
        recv_mono: MonoNs(n),
        recv_wall: wall,
        conn: ConnKey { conn: 1, epoch: 1 },
    }
}

fn timer(n: u64) -> Record {
    Record::Timer {
        stamp: stamp(n, WallNs(n as i64)),
        tag: TimerTag(n),
    }
}

/// An inbound frame of `len` bytes that compress well, as market data does.
fn frame(n: u64, len: usize) -> Record {
    let text = format!("{{\"seq\":{n},\"bids\":[[\"100.5\",\"2\"]]}}");
    Record::Inbound {
        stamp: stamp(n, WallNs(n as i64)),
        opcode: Opcode::Text,
        bytes: Opaque(text.bytes().cycle().take(len).collect()),
    }
}

fn names(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
}

fn read_all(root: &Path, shard: u16) -> Vec<Result<Record, JournalError>> {
    JournalReader::open(root, shard).unwrap().collect()
}

fn read_ok(root: &Path, shard: u16) -> Vec<Record> {
    read_all(root, shard)
        .into_iter()
        .map(Result::unwrap)
        .collect()
}

fn decompress(path: &Path) -> Vec<u8> {
    zstd::stream::decode_all(fs::File::open(path).unwrap()).unwrap()
}

#[test]
fn records_across_an_hour_and_a_utc_day_boundary_roll_into_compressed_closed_segments() {
    let root = fresh_dir("done");
    let mut w = JournalWriter::create(&root, 0, key()).unwrap();
    let written: Vec<(WallNs, Record)> = vec![
        (at(22, 30), frame(1, 64 * 1024)),
        (at(22, 45), timer(2)),
        (WallNs(at(23, 0).0 - 1), frame(3, 64 * 1024)),
        // The hour boundary: the 22:00 segment closes.
        (at(23, 0), timer(4)),
        (at(23, 59), frame(5, 64 * 1024)),
        // The UTC day boundary: the 23:00 segment closes in 2026-10-03's directory.
        (at(24, 0), Record::Marker(Marker::Recovered)),
        (at(24, 10), frame(7, 1024)),
    ];
    for (now, record) in &written {
        w.append(*now, record).unwrap();
    }
    w.flush().unwrap();
    let records: Vec<Record> = written.iter().map(|(_, r)| r.clone()).collect();

    assert_eq!(names(&root), ["20261003", "20261004"]);
    let closed = ["0-000000.fbcj.zst", "0-000001.fbcj.zst"];
    assert_eq!(names(&root.join("20261003")), closed);
    assert_eq!(names(&root.join("20261004")), ["0-000000.fbcj"]);
    for name in closed {
        let path = root.join("20261003").join(name);
        let compressed = fs::read(&path).unwrap();
        assert_eq!(compressed[..4], ZSTD_MAGIC, "{name} is a zstd frame");
        // Frame_Header_Descriptor bit 2: the frame carries a content checksum (RFC 8878
        // §3.1.1.1.1.5).
        assert_ne!(compressed[4] & 0b100, 0, "{name} has a content checksum");
        let raw = decompress(&path);
        assert_eq!(raw[..4], MAGIC, "{name} holds a segment");
        assert_eq!(raw[4..6], VERSION.to_le_bytes());
        assert!(
            compressed.len() * 10 < raw.len(),
            "{name}: {} bytes compressed from {}",
            compressed.len(),
            raw.len()
        );
    }
    let open = fs::read(root.join("20261004/0-000000.fbcj")).unwrap();
    assert_eq!(open[..4], MAGIC, "the open segment is not compressed");

    // Read while the writer still holds the open segment, then after it is dropped.
    assert_eq!(read_ok(&root, 0), records);
    drop(w);
    assert_eq!(read_ok(&root, 0), records);
    let entries: Vec<_> = JournalReader::open(&root, 0)
        .unwrap()
        .entries()
        .map(Result::unwrap)
        .collect();
    assert_eq!(entries.len(), records.len());
    assert!(entries.iter().all(|e| e.digests.is_empty()));
}

#[test]
fn every_hour_rolls_and_an_empty_hour_leaves_no_segment() {
    let root = fresh_dir("hours");
    let mut w = JournalWriter::create(&root, 3, key()).unwrap();
    w.append(at(1, 0), &timer(1)).unwrap();
    w.append(at(1, 59), &timer(2)).unwrap();
    w.append(at(2, 0), &timer(3)).unwrap();
    // Nothing between 03:00 and 05:00.
    w.append(at(5, 30), &timer(4)).unwrap();
    drop(w);
    assert_eq!(
        names(&root.join("20261003")),
        ["3-000000.fbcj.zst", "3-000001.fbcj.zst", "3-000002.fbcj"]
    );
    assert_eq!(read_ok(&root, 3), [timer(1), timer(2), timer(3), timer(4)]);
}

#[test]
fn a_clock_stepping_back_across_an_hour_stays_in_the_later_hour() {
    let root = fresh_dir("clock_back");
    let mut w = JournalWriter::create(&root, 1, key()).unwrap();
    w.append(at(10, 59), &timer(1)).unwrap();
    w.append(at(11, 0), &timer(2)).unwrap();
    // Back into 10:00: filed with 11:00's records, nothing reopened or rolled.
    w.append(at(10, 59), &timer(3)).unwrap();
    w.append(at(11, 30), &timer(4)).unwrap();
    drop(w);
    assert_eq!(
        names(&root.join("20261003")),
        ["1-000000.fbcj.zst", "1-000001.fbcj"]
    );
    assert_eq!(read_ok(&root, 1), [timer(1), timer(2), timer(3), timer(4)]);
}

#[test]
fn a_restarted_writer_numbers_after_compressed_segments() {
    let root = fresh_dir("restart");
    let mut first = JournalWriter::create(&root, 1, key()).unwrap();
    first.append(at(8, 0), &timer(1)).unwrap();
    first.append(at(9, 0), &timer(2)).unwrap();
    drop(first);
    // The process stopped with the 09:00 segment open: it stays uncompressed and is read.
    let mut second = JournalWriter::create(&root, 1, key()).unwrap();
    second.append(at(9, 10), &timer(3)).unwrap();
    second.append(at(10, 0), &timer(4)).unwrap();
    drop(second);
    assert_eq!(
        names(&root.join("20261003")),
        [
            "1-000000.fbcj.zst",
            "1-000001.fbcj",
            "1-000002.fbcj.zst",
            "1-000003.fbcj"
        ]
    );
    assert_eq!(read_ok(&root, 1), [timer(1), timer(2), timer(3), timer(4)]);
}

#[test]
fn a_segment_left_both_compressed_and_not_is_read_once_from_its_compressed_form() {
    // A writer that stopped after the compressed segment took its name but before the
    // uncompressed one was removed leaves both; they hold the same records.
    let root = fresh_dir("both");
    let mut w = JournalWriter::create(&root, 1, key()).unwrap();
    w.append(at(4, 0), &timer(1)).unwrap();
    w.flush().unwrap();
    let plain = root.join("20261003/1-000000.fbcj");
    let kept = fs::read(&plain).unwrap();
    w.append(at(5, 0), &timer(2)).unwrap();
    drop(w);
    // The uncompressed copy says something else, so reading it would show.
    let mut altered = kept.clone();
    altered.truncate(kept.len() - 1);
    fs::write(&plain, altered).unwrap();
    fs::write(root.join("20261003/1-000000.fbcj.zst.tmp"), b"half").unwrap();

    assert_eq!(read_ok(&root, 1), [timer(1), timer(2)]);
    // A restarted writer numbers past both.
    let mut again = JournalWriter::create(&root, 1, key()).unwrap();
    again.append(at(5, 0), &timer(3)).unwrap();
    drop(again);
    assert!(names(&root.join("20261003")).contains(&"1-000002.fbcj".to_string()));
    assert_eq!(read_ok(&root, 1), [timer(1), timer(2), timer(3)]);
}

#[test]
fn a_damaged_compressed_segment_is_reported_once_and_reading_goes_on() {
    let root = fresh_dir("damaged");
    let mut w = JournalWriter::create(&root, 1, key()).unwrap();
    for (h, n) in [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)] {
        w.append(at(h, 0), &frame(n, 4096)).unwrap();
    }
    drop(w);
    let day = root.join("20261003");
    let segment = |n: u32| day.join(format!("1-{n:06}.{SEGMENT_EXT}.{COMPRESSED_EXT}"));
    // 1: corrupt bytes after the zstd magic; 2: cut short; 3: not zstd at all; 4: a frame
    // whose content checksum, its last four bytes, does not match.
    let good = fs::read(segment(1)).unwrap();
    let mut corrupt = good.clone();
    for b in &mut corrupt[4..] {
        *b ^= 0x5A;
    }
    fs::write(segment(1), corrupt).unwrap();
    fs::write(segment(2), &good[..good.len() / 2]).unwrap();
    fs::write(segment(3), b"not zstd").unwrap();
    let mut wrong_sum = fs::read(segment(4)).unwrap();
    *wrong_sum.last_mut().unwrap() ^= 1;
    fs::write(segment(4), wrong_sum).unwrap();

    let read = read_all(&root, 1);
    // The checksum is checked at the end of the frame, so segment 4's record is returned
    // before the mismatch is reported.
    assert_eq!(read.len(), 7, "{read:?}");
    assert_eq!(*read[0].as_ref().unwrap(), frame(1, 4096));
    assert_eq!(*read[4].as_ref().unwrap(), frame(5, 4096));
    for (i, n) in [(1, 1), (2, 2), (3, 3), (5, 4)] {
        let err = read[i].as_ref().unwrap_err();
        match err {
            JournalError::Compressed { segment: p, .. } => assert_eq!(*p, segment(n)),
            other => panic!("segment {n}: {other:?}"),
        }
        assert!(
            err.to_string().starts_with(&format!(
                "{} cannot be decompressed: ",
                segment(n).display()
            )),
            "{err}"
        );
        assert!(err.source().is_some());
    }
    // The open segment, uncompressed, is still read.
    assert!(
        read[5]
            .as_ref()
            .unwrap_err()
            .to_string()
            .contains("checksum")
    );
    assert_eq!(*read[6].as_ref().unwrap(), frame(6, 4096));
}

#[test]
fn a_compressed_segment_cut_inside_a_record_is_truncated() {
    // A whole zstd frame whose segment ends inside a record: the decompression is good, the
    // segment is not.
    let root = fresh_dir("cut");
    let day = root.join("20261003");
    fs::create_dir_all(&day).unwrap();
    let mut raw = [&MAGIC[..], &VERSION.to_le_bytes()].concat();
    raw.extend_from_slice(&100u32.to_le_bytes());
    raw.extend_from_slice(b"short");
    let path = day.join("1-000000.fbcj.zst");
    fs::write(&path, zstd::stream::encode_all(&raw[..], 3).unwrap()).unwrap();
    let read = read_all(&root, 1);
    assert!(
        matches!(&read[..], [Err(JournalError::Truncated { segment })] if *segment == path),
        "{read:?}"
    );
}

#[test]
fn a_roll_that_cannot_compress_fails_the_append_and_keeps_the_closed_segment_readable() {
    let root = fresh_dir("cannot_compress");
    let mut w = JournalWriter::create(&root, 1, key()).unwrap();
    w.append(at(6, 0), &timer(1)).unwrap();
    // A directory where the compressed segment's temporary file goes.
    let day = root.join("20261003");
    fs::create_dir(day.join("1-000000.fbcj.zst.tmp")).unwrap();
    let err = w.append(at(7, 0), &timer(2)).unwrap_err();
    assert!(matches!(err, JournalError::Io(_)), "{err:?}");
    // The record that failed was not written; the closed segment stays as it was.
    assert_eq!(read_ok(&root, 1), [timer(1)]);
    // The writer goes on in a new segment, still in 06:00 (Codex r4177713018: the rejected
    // roll did not move it to 07:00), so a 06:00 record goes there and 07:00 rolls again.
    w.append(at(6, 30), &timer(5)).unwrap();
    w.append(at(7, 1), &timer(3)).unwrap();
    drop(w);
    assert_eq!(
        names(&day),
        [
            "1-000000.fbcj",
            "1-000000.fbcj.zst.tmp",
            "1-000001.fbcj.zst",
            "1-000002.fbcj"
        ]
    );
    assert_eq!(read_ok(&root, 1), [timer(1), timer(5), timer(3)]);

    // A directory where the compressed segment itself goes: the rename fails, and the
    // temporary file is removed.
    let root = fresh_dir("cannot_rename");
    let mut w = JournalWriter::create(&root, 1, key()).unwrap();
    w.append(at(6, 0), &timer(1)).unwrap();
    let day = root.join("20261003");
    fs::create_dir(day.join("1-000000.fbcj.zst")).unwrap();
    fs::write(day.join("1-000000.fbcj.zst/x"), b"x").unwrap();
    assert!(matches!(
        w.append(at(7, 0), &timer(2)),
        Err(JournalError::Io(_))
    ));
    assert_eq!(names(&day), ["1-000000.fbcj", "1-000000.fbcj.zst"]);
    // Codex r4177658809: the directory at the compressed name is not a segment, so the
    // uncompressed one is read.
    assert_eq!(read_ok(&root, 1), [timer(1)]);
}

#[cfg(unix)]
#[test]
fn a_segment_that_cannot_be_read_is_an_io_error_and_reading_goes_on() {
    // A link named like an uncompressed segment, to a directory: it opens but cannot be read.
    // (A directory itself is not a segment and is passed over.)
    let root = fresh_dir("unreadable");
    let mut w = JournalWriter::create(&root, 1, key()).unwrap();
    w.append(at(1, 0), &timer(1)).unwrap();
    w.append(at(3, 0), &timer(3)).unwrap();
    drop(w);
    // The writer made 1-000000.fbcj.zst and 1-000001.fbcj; the open one moves to sequence 3
    // and the directory takes sequence 2, between them.
    let day = root.join("20261003");
    fs::rename(day.join("1-000001.fbcj"), day.join("1-000003.fbcj")).unwrap();
    fs::create_dir(day.join("a-directory")).unwrap();
    std::os::unix::fs::symlink(day.join("a-directory"), day.join("1-000002.fbcj")).unwrap();
    fs::create_dir(day.join("1-000004.fbcj")).unwrap();
    let read = read_all(&root, 1);
    assert_eq!(read.len(), 3, "{read:?}");
    assert_eq!(*read[0].as_ref().unwrap(), timer(1));
    assert!(
        matches!(&read[1], Err(JournalError::Io(_))),
        "{:?}",
        read[1]
    );
    assert_eq!(*read[2].as_ref().unwrap(), timer(3));
}

#[test]
fn a_segment_compressed_after_the_reader_listed_it_is_read_compressed() {
    // Codex r4177658812: the reader lists the open segment uncompressed, then the writer
    // rolls, compressing it and removing the name the reader listed.
    let root = fresh_dir("rolled_under_reader");
    let mut w = JournalWriter::create(&root, 1, key()).unwrap();
    w.append(at(1, 0), &timer(1)).unwrap();
    w.append(at(1, 5), &timer(2)).unwrap();
    w.flush().unwrap();
    let reader = JournalReader::open(&root, 1).unwrap();
    w.append(at(2, 0), &timer(3)).unwrap();
    assert_eq!(
        names(&root.join("20261003")),
        ["1-000000.fbcj.zst", "1-000001.fbcj"]
    );
    let read: Vec<Record> = reader.map(Result::unwrap).collect();
    // The segments listed are read; the one started after the listing is not.
    assert_eq!(read, [timer(1), timer(2)]);
    drop(w);

    // A listed segment gone in both forms is still an i/o error, and reading goes on.
    let root = fresh_dir("gone_under_reader");
    let mut w = JournalWriter::create(&root, 1, key()).unwrap();
    w.append(at(1, 0), &timer(1)).unwrap();
    w.append(at(2, 0), &timer(2)).unwrap();
    drop(w);
    let reader = JournalReader::open(&root, 1).unwrap();
    fs::remove_file(root.join("20261003/1-000000.fbcj.zst")).unwrap();
    let read: Vec<_> = reader.collect();
    assert!(matches!(&read[0], Err(JournalError::Io(_))), "{read:?}");
    assert_eq!(*read[1].as_ref().unwrap(), timer(2));
    let reader = JournalReader::open(&root, 1).unwrap();
    fs::remove_file(root.join("20261003/1-000001.fbcj")).unwrap();
    let read: Vec<_> = reader.collect();
    assert!(
        matches!(&read[..], [Err(JournalError::Io(e))] if e.kind() == std::io::ErrorKind::NotFound),
        "{read:?}"
    );
}

#[test]
fn only_the_two_forms_of_one_name_are_merged() {
    // Codex r4177765900: two spellings of one sequence number are two segments, both read.
    let root = fresh_dir("spellings");
    let mut w = JournalWriter::create(&root, 1, key()).unwrap();
    w.append(at(1, 0), &timer(1)).unwrap();
    w.append(at(2, 0), &timer(2)).unwrap();
    drop(w);
    let day = root.join("20261003");
    fs::copy(day.join("1-000000.fbcj.zst"), day.join("1-0.fbcj.zst")).unwrap();
    fs::copy(day.join("1-000001.fbcj"), day.join("1-01.fbcj")).unwrap();
    assert_eq!(read_ok(&root, 1), [timer(1), timer(1), timer(2), timer(2)]);
}

#[test]
fn a_writer_restarted_on_an_earlier_hour_of_the_latest_day_keeps_write_order() {
    // Codex r4177765907: a restart recovers the latest day, not the latest hour. The earlier
    // hour gets a segment of its own after the ones on disk, so reading is still in write
    // order, at the cost of one more roll.
    let root = fresh_dir("restart_hour_back");
    let mut first = JournalWriter::create(&root, 1, key()).unwrap();
    first.append(at(9, 0), &timer(1)).unwrap();
    drop(first);
    let mut second = JournalWriter::create(&root, 1, key()).unwrap();
    second.append(at(8, 0), &timer(2)).unwrap();
    second.append(at(9, 0), &timer(3)).unwrap();
    drop(second);
    assert_eq!(
        names(&root.join("20261003")),
        ["1-000000.fbcj", "1-000001.fbcj.zst", "1-000002.fbcj"]
    );
    assert_eq!(read_ok(&root, 1), [timer(1), timer(2), timer(3)]);
}
