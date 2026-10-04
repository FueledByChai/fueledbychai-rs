//! The segment writer: one subdirectory per UTC day under the consumer's directory, segments
//! named `<shard>-<seq>`, rolled at every UTC hour, each closed segment compressed with zstd.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fbc_core::WallNs;

use crate::JournalError;
use crate::format::{self, MAGIC, VERSION};
use crate::record::Record;
use crate::redact::RedactionKey;

/// The extension of a segment file.
pub const SEGMENT_EXT: &str = "fbcj";

/// The extension a closed segment takes when it is compressed: `<shard>-<seq>.fbcj.zst`, one
/// zstd frame, with a content checksum, holding the segment's bytes as they were written.
pub const COMPRESSED_EXT: &str = "zst";

/// The zstd level closed segments are compressed at: zstd's own default.
const LEVEL: i32 = 3;

const NANOS_PER_DAY: i64 = 86_400 * 1_000_000_000;
const NANOS_PER_HOUR: i64 = 3_600 * 1_000_000_000;
const HOURS_PER_DAY: i64 = 24;

/// The UTC hour of `wall`, as hours since 1970-01-01T00:00Z.
fn hour_of(wall: WallNs) -> i64 {
    wall.0.div_euclid(NANOS_PER_HOUR)
}

/// The UTC day of `wall`, as days since 1970-01-01.
pub(crate) fn day_of(wall: WallNs) -> i64 {
    wall.0.div_euclid(NANOS_PER_DAY)
}

/// The directory name of a UTC day: `YYYYMMDD`. Every `WallNs` falls in years 1677 to 2262,
/// so the year always has four digits and the names sort in date order.
pub(crate) fn day_dir(day: i64) -> String {
    // Howard Hinnant's civil_from_days.
    let z = day + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!("{y:04}{m:02}{d:02}")
}

/// The day a directory name is, if it is a real UTC date in `YYYYMMDD` form that a `WallNs`
/// can fall on: the name of the day it denotes must be the name itself, so `20261331`,
/// `20260230` or `99999999` is not a day.
pub(crate) fn parse_day(name: &str) -> Option<i64> {
    if name.len() != 8 || !name.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let (y, m, d): (i64, i64, i64) = (
        name[..4].parse().ok()?,
        name[4..6].parse().ok()?,
        name[6..].parse().ok()?,
    );
    // Howard Hinnant's days_from_civil.
    let y = y - i64::from(m <= 2);
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let day = era * 146_097 + doe - 719_468;
    let possible = day_of(WallNs(i64::MIN))..=day_of(WallNs(i64::MAX));
    (possible.contains(&day) && day_dir(day) == name).then_some(day)
}

/// The sequence number of `name` if it is a segment of `shard`, and whether it is the
/// compressed form (`.fbcj.zst`).
pub(crate) fn segment_seq(name: &str, shard: u16) -> Option<(u32, bool)> {
    let (name, compressed) = match name
        .strip_suffix(COMPRESSED_EXT)
        .and_then(|n| n.strip_suffix('.'))
    {
        Some(plain) => (plain, true),
        None => (name, false),
    };
    let stem = name.strip_suffix(SEGMENT_EXT)?.strip_suffix('.')?;
    let (owner, seq) = stem.split_once('-')?;
    if owner.parse::<u16>().ok()? != shard {
        return None;
    }
    Some((seq.parse().ok()?, compressed))
}

/// A segment found on disk.
pub(crate) struct Listed {
    pub(crate) day: i64,
    pub(crate) compressed: bool,
    pub(crate) path: PathBuf,
}

/// One shard's segments under `root` in write order: the day directories (`YYYYMMDD`,
/// [`parse_day`]) in date order, each day's segments in sequence order. Anything else is
/// passed over, and so is a directory, whatever its name; an entry whose type cannot be read
/// is an error, not passed over. A segment found both compressed and not (a writer stopped
/// between writing the compressed form and removing the other) is listed once, compressed:
/// the compressed form is complete before it takes its name.
pub(crate) fn list_segments(root: &Path, shard: u16) -> io::Result<Vec<Listed>> {
    let mut found = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let Some(day) = parse_day(&entry.file_name().to_string_lossy()) else {
            continue;
        };
        if !entry.file_type()?.is_dir() {
            continue;
        }
        for file in fs::read_dir(entry.path())? {
            let file = file?;
            // A directory is never a segment, whatever its name (one at a compressed
            // segment's name must not hide the uncompressed segment).
            let Some((seq, compressed)) = file
                .file_name()
                .to_str()
                .and_then(|n| segment_seq(n, shard))
            else {
                continue;
            };
            if !file.file_type()?.is_dir() {
                found.push((day, seq, !compressed, file.path()));
            }
        }
    }
    // The compressed form of a sequence number sorts first and is the one kept.
    found.sort();
    found.dedup_by(|later, kept| (later.0, later.1) == (kept.0, kept.1));
    Ok(found
        .into_iter()
        .map(|(day, _, plain, path)| Listed {
            day,
            compressed: !plain,
            path,
        })
        .collect())
}

/// Compresses the closed segment at `plain` into `<plain>.zst` and removes `plain`. The
/// compressed form is written under a temporary name, synced, and renamed into place, and the
/// rename made durable before `plain` is removed, so a segment named `.zst` is always whole
/// and a power loss leaves at least one form; until the rename, `plain` is the segment.
fn compress(plain: &Path) -> io::Result<()> {
    let dir = plain
        .parent()
        .expect("a segment lives in its day's directory");
    let done = with_ext(plain, COMPRESSED_EXT);
    let tmp = with_ext(&done, "tmp");
    let written = File::create(&tmp).and_then(|out| {
        let mut zst = zstd::stream::write::Encoder::new(out, LEVEL)?;
        // The frame carries a checksum of its content, so a damaged segment fails to
        // decompress rather than reading back as other records.
        zst.include_checksum(true)?;
        io::copy(&mut File::open(plain)?, &mut zst)?;
        zst.finish()?.sync_all()?;
        fs::rename(&tmp, &done)
    });
    if written.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    written?;
    sync_dir(dir)?;
    fs::remove_file(plain)?;
    sync_dir(dir)
}

/// Makes the entries of `dir` (a rename, a removal) durable, where the platform can.
fn sync_dir(dir: &Path) -> io::Result<()> {
    #[cfg(unix)]
    File::open(dir)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

/// `path` with `.ext` appended to its name.
pub(crate) fn with_ext(path: &Path, ext: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".");
    name.push(ext);
    PathBuf::from(name)
}

struct Segment {
    /// The UTC hour the segment holds records of.
    hour: i64,
    path: PathBuf,
    file: BufWriter<File>,
}

/// Writes one shard's records into segments under a directory the consumer supplies.
///
/// Time comes from the caller: [`append`](JournalWriter::append) takes the wall time it
/// files the record under. A segment holds one UTC hour: a record whose hour is later than
/// the open segment's closes it and starts a segment in that hour's day directory, so the
/// segment rolls every hour and at every UTC day boundary (an hour with no record has no
/// segment). A record whose hour is earlier (the wall clock stepped back) stays in the latest
/// hour written, so reading the days in order is reading in write order. That holds across
/// restarts: a new writer starts from the latest day that holds one of the shard's segments,
/// and a new segment takes the next sequence number after the shard's existing segments in
/// its day, so a restarted writer never overwrites one.
///
/// Each segment the writer closes is compressed with zstd into `<shard>-<seq>.fbcj.zst`
/// ([`COMPRESSED_EXT`]) and the uncompressed file removed, on the thread that appends (the
/// [`WriterThread`](crate::WriterThread), never the shard's). The open segment stays
/// uncompressed, and so does the one a stopped writer left open. The reader reads both forms.
pub struct JournalWriter {
    root: PathBuf,
    shard: u16,
    /// The key redaction spans are hashed under.
    key: Arc<RedactionKey>,
    open: Option<Segment>,
    /// The latest UTC hour this shard has written to; no record is filed under an earlier
    /// one. A restarted writer starts from the first hour of the latest day on disk.
    latest: Option<i64>,
    body: Vec<u8>,
}

impl JournalWriter {
    /// A writer for `shard` under `root`, which it creates if missing, hashing redaction spans
    /// under `key`. No segment is opened until the first record.
    pub fn create(
        root: impl AsRef<Path>,
        shard: u16,
        key: Arc<RedactionKey>,
    ) -> Result<JournalWriter, JournalError> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        let latest = list_segments(&root, shard)?
            .pop()
            .map(|s| s.day * HOURS_PER_DAY);
        Ok(JournalWriter {
            root,
            shard,
            key,
            open: None,
            latest,
            body: Vec::new(),
        })
    }

    /// Writes `record`, filed under the UTC day of `now`. No byte of a redaction span and no
    /// secret header value is written: each is written as its keyed hash
    /// ([`Record::digests`]) and reads back blanked ([`Record::blanked`]).
    pub fn append(&mut self, now: WallNs, record: &Record) -> Result<(), JournalError> {
        let mut body = std::mem::take(&mut self.body);
        body.clear();
        let written = format::encode(record, &self.key, &mut body)
            .and_then(|()| self.append_body(now, &body));
        self.body = body;
        written
    }

    /// Writes a record body [`format::encode`] made, filed under the UTC hour of `now`. A
    /// record of a later hour first closes and compresses the open segment; if that fails,
    /// the record is not written, the closed segment stays as it is (uncompressed, and read
    /// as such), the writer stays in the closed segment's hour, and the next record starts a
    /// new segment.
    pub(crate) fn append_body(&mut self, now: WallNs, body: &[u8]) -> Result<(), JournalError> {
        let len = u32::try_from(body.len()).map_err(|_| JournalError::TooLarge)?;
        let hour = hour_of(now).max(self.latest.unwrap_or(i64::MIN));
        let segment = match self.open.take() {
            Some(open) if open.hour == hour => open,
            open => {
                if let Some(done) = open {
                    close(done)?;
                }
                self.start(hour)?
            }
        };
        // Only now, with the segment of `hour` open: a rejected roll leaves the hour as it was.
        self.latest = Some(hour);
        let file = &mut self.open.insert(segment).file;
        let written = file
            .write_all(&len.to_le_bytes())
            .and_then(|()| file.write_all(body));
        // After a failed write the segment may end inside a record: drop it, so the next
        // record starts a new segment and the reader reports this one's end as truncated.
        self.open = self.open.take().filter(|_| written.is_ok());
        Ok(written?)
    }

    /// Hands every record written so far to the operating system.
    pub fn flush(&mut self) -> Result<(), JournalError> {
        if let Some(open) = &mut self.open {
            open.file.flush()?;
        }
        Ok(())
    }

    /// Opens the next segment of this shard, for `hour`, in its day's directory.
    fn start(&self, hour: i64) -> Result<Segment, JournalError> {
        let dir = self.root.join(day_dir(hour.div_euclid(HOURS_PER_DAY)));
        fs::create_dir_all(&dir)?;
        let mut next = 0;
        for entry in fs::read_dir(&dir)? {
            let name = entry?.file_name();
            if let Some((seq, _)) = name.to_str().and_then(|n| segment_seq(n, self.shard)) {
                next = next.max(seq.saturating_add(1));
            }
        }
        let path = dir.join(format!("{}-{next:06}.{SEGMENT_EXT}", self.shard));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        let mut file = BufWriter::new(file);
        file.write_all(&MAGIC)?;
        file.write_all(&VERSION.to_le_bytes())?;
        Ok(Segment { hour, path, file })
    }
}

/// Closes a segment: everything written to it reaches the file, which is then compressed.
fn close(segment: Segment) -> Result<(), JournalError> {
    let Segment { path, file, .. } = segment;
    drop(file.into_inner().map_err(io::IntoInnerError::into_error)?);
    Ok(compress(&path)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn days_are_named_by_their_utc_date() {
        let sec = 1_000_000_000;
        assert_eq!(day_dir(day_of(WallNs(0))), "19700101");
        assert_eq!(day_dir(day_of(WallNs(-1))), "19691231");
        assert_eq!(day_dir(day_of(WallNs(1_791_071_999 * sec))), "20261003");
        assert_eq!(day_dir(day_of(WallNs(1_791_072_000 * sec))), "20261004");
        // A leap day and the turn of a century that is not a leap year.
        assert_eq!(day_dir(day_of(WallNs(1_709_164_800 * sec))), "20240229");
        assert_eq!(day_dir(day_of(WallNs(4_107_542_400 * sec))), "21000301");
        assert_eq!(day_dir(day_of(WallNs(i64::MIN))), "16770921");
        assert_eq!(day_dir(day_of(WallNs(i64::MAX))), "22620411");
    }

    #[test]
    fn only_real_dates_are_days() {
        for day in [
            0,
            -1,
            20_729,
            19_782,
            47_541,
            day_of(WallNs(i64::MIN)),
            day_of(WallNs(i64::MAX)),
        ] {
            assert_eq!(parse_day(&day_dir(day)), Some(day));
        }
        for name in [
            "99999999",
            "00000000",
            "20261301",
            "20260001",
            "20260230",
            "20260100",
            "21000229",
            "16770920",
            "22620412",
            "2026100",
            "202610031",
            "2026-10-",
            "2026100x",
        ] {
            assert_eq!(parse_day(name), None, "{name}");
        }
    }

    #[test]
    fn only_this_shard_s_segments_are_counted() {
        assert_eq!(segment_seq("2-000004.fbcj", 2), Some((4, false)));
        assert_eq!(segment_seq("2-4.fbcj", 2), Some((4, false)));
        assert_eq!(segment_seq("2-000004.fbcj.zst", 2), Some((4, true)));
        assert_eq!(segment_seq("3-000004.fbcj", 2), None);
        assert_eq!(segment_seq("3-000004.fbcj.zst", 2), None);
        assert_eq!(segment_seq("2-000004.zst", 2), None);
        assert_eq!(segment_seq("2-000004.fbcjzst", 2), None);
        assert_eq!(segment_seq("2-000004.fbcj.zst.tmp", 2), None);
        assert_eq!(segment_seq("2-000004.fbcj.zst.zst", 2), None);
        assert_eq!(segment_seq("2-000004fbcj", 2), None);
        assert_eq!(segment_seq("2_000004.fbcj", 2), None);
        assert_eq!(segment_seq("x-000004.fbcj", 2), None);
        assert_eq!(segment_seq("2-x.fbcj", 2), None);
    }
}
