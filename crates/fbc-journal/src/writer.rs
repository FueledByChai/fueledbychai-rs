//! The segment writer: one subdirectory per UTC day under the consumer's directory, segments
//! named `<shard>-<seq>`.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};

use fbc_core::WallNs;

use crate::JournalError;
use crate::format::{self, MAGIC, VERSION};
use crate::record::Record;

/// The extension of a segment file.
pub const SEGMENT_EXT: &str = "fbcj";

const NANOS_PER_DAY: i64 = 86_400 * 1_000_000_000;

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

/// The sequence number of `name` if it is a segment of `shard`.
pub(crate) fn segment_seq(name: &str, shard: u16) -> Option<u32> {
    let stem = name.strip_suffix(SEGMENT_EXT)?.strip_suffix('.')?;
    let (owner, seq) = stem.split_once('-')?;
    if owner.parse::<u16>().ok()? != shard {
        return None;
    }
    seq.parse().ok()
}

/// One shard's segments under `root` in write order, with their days: the day directories
/// (`YYYYMMDD`, [`parse_day`]) in date order, each day's segments in sequence order. Anything
/// else is passed over.
pub(crate) fn list_segments(root: &Path, shard: u16) -> io::Result<Vec<(i64, u32, PathBuf)>> {
    let mut found = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let day = parse_day(&entry.file_name().to_string_lossy());
        let Some(day) = day.filter(|_| entry.file_type().is_ok_and(|t| t.is_dir())) else {
            continue;
        };
        for file in fs::read_dir(entry.path())? {
            let file = file?;
            if let Some(seq) = file
                .file_name()
                .to_str()
                .and_then(|n| segment_seq(n, shard))
            {
                found.push((day, seq, file.path()));
            }
        }
    }
    found.sort();
    Ok(found)
}

struct Segment {
    day: i64,
    file: BufWriter<File>,
}

/// Writes one shard's records into segments under a directory the consumer supplies.
///
/// Time comes from the caller: [`append`](JournalWriter::append) takes the wall time it
/// files the record under. A record whose UTC day is later than the open segment's starts a
/// segment in that day's directory; one whose day is earlier (the wall clock stepped back
/// across midnight) stays in the latest day written, so reading the days in order is reading
/// in write order. That holds across restarts: a new writer starts from the latest day that
/// holds one of the shard's segments, and a new segment takes the next sequence number after
/// the shard's existing segments in its day, so a restarted writer never overwrites one.
pub struct JournalWriter {
    root: PathBuf,
    shard: u16,
    open: Option<Segment>,
    /// The latest day this shard has written to; no record is filed under an earlier one.
    latest: Option<i64>,
    body: Vec<u8>,
}

impl JournalWriter {
    /// A writer for `shard` under `root`, which it creates if missing. No segment is opened
    /// until the first record.
    pub fn create(root: impl AsRef<Path>, shard: u16) -> Result<JournalWriter, JournalError> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root)?;
        let latest = list_segments(&root, shard)?.pop().map(|(day, ..)| day);
        Ok(JournalWriter {
            root,
            shard,
            open: None,
            latest,
            body: Vec::new(),
        })
    }

    /// Writes `record`, filed under the UTC day of `now`. No byte of a redaction span and no
    /// secret header value is written (see [`Record::blanked`]).
    pub fn append(&mut self, now: WallNs, record: &Record) -> Result<(), JournalError> {
        self.body.clear();
        format::encode(record, &mut self.body)?;
        let len = u32::try_from(self.body.len()).map_err(|_| JournalError::TooLarge)?;
        let day = day_of(now).max(self.latest.unwrap_or(i64::MIN));
        self.latest = Some(day);
        let segment = match self.open.take() {
            Some(open) if open.day == day => open,
            open => {
                if let Some(mut done) = open {
                    done.file.flush()?;
                }
                self.start(day)?
            }
        };
        let file = &mut self.open.insert(segment).file;
        let written = file
            .write_all(&len.to_le_bytes())
            .and_then(|()| file.write_all(&self.body));
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

    /// Opens the next segment of this shard in `day`'s directory.
    fn start(&self, day: i64) -> Result<Segment, JournalError> {
        let dir = self.root.join(day_dir(day));
        fs::create_dir_all(&dir)?;
        let mut next = 0;
        for entry in fs::read_dir(&dir)? {
            let name = entry?.file_name();
            if let Some(seq) = name.to_str().and_then(|n| segment_seq(n, self.shard)) {
                next = next.max(seq.saturating_add(1));
            }
        }
        let name = format!("{}-{next:06}.{SEGMENT_EXT}", self.shard);
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(dir.join(name))?;
        let mut file = BufWriter::new(file);
        file.write_all(&MAGIC)?;
        file.write_all(&VERSION.to_le_bytes())?;
        Ok(Segment { day, file })
    }
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
        assert_eq!(segment_seq("2-000004.fbcj", 2), Some(4));
        assert_eq!(segment_seq("2-4.fbcj", 2), Some(4));
        assert_eq!(segment_seq("3-000004.fbcj", 2), None);
        assert_eq!(segment_seq("2-000004.zst", 2), None);
        assert_eq!(segment_seq("2-000004fbcj", 2), None);
        assert_eq!(segment_seq("2_000004.fbcj", 2), None);
        assert_eq!(segment_seq("x-000004.fbcj", 2), None);
        assert_eq!(segment_seq("2-x.fbcj", 2), None);
    }
}
