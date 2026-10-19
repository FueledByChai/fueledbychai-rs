//! The reader: one shard's records in write order, across its segments and days, compressed
//! and not.

use std::collections::VecDeque;
use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::{Path, PathBuf};

use crate::JournalError;
use crate::format::{self, MAGIC, OLDEST_READABLE, VERSION};
use crate::record::Record;
use crate::redact::SpanDigest;
use crate::writer::{COMPRESSED_EXT, Listed, list_segments, with_ext};

/// Reads one shard's records in the order they were written: the day directories in date
/// order, each day's segments in sequence order, each segment from its start. Directories
/// and files that are not this shard's segments are passed over. A closed segment is read
/// through zstd (`.fbcj.zst`), the open one, or one a stopped writer left open, as written.
/// The segments are listed when the reader opens; one listed uncompressed that the writer
/// compresses before the reader reaches it is read in its compressed form.
///
/// An error in a segment (a bad header, a record cut short, a record that cannot be read,
/// compressed data that cannot be decompressed or fails its checksum) is returned once, and
/// reading goes on with the next segment, since nothing after a damaged record in the same
/// segment can be found again. A compressed segment's checksum covers the whole segment and
/// is checked at its end, so a mismatch is reported after the segment's records.
///
/// It reads segments of every format version from [`OLDEST_READABLE`] to [`VERSION`]: a
/// version 2 segment, written before the `Nonce`, `EncodeCtx` and `Cycle` kinds existed,
/// reads back as it was written. A segment of a newer version is refused at its header,
/// naming its version ([`JournalError::UnsupportedVersion`]), rather than part way through.
///
/// As an iterator it returns records with their redaction spans blanked
/// ([`Record::blanked`]); [`entries`](JournalReader::entries) returns each with the keyed
/// hashes written in place of its spans.
pub struct JournalReader {
    segments: VecDeque<Listed>,
    open: Option<Open>,
    /// The newest format version it reads: [`VERSION`]. A segment of a newer one is refused at
    /// its header, naming its version.
    newest: u16,
}

/// The segment being read.
struct Open {
    path: PathBuf,
    compressed: bool,
    file: Box<dyn Read + Send>,
    /// The segment's format version, once its header is read.
    version: u16,
}

impl Open {
    /// Reads until `buf` is full or the segment ends; how many bytes were read.
    fn read_full(&mut self, buf: &mut [u8]) -> Result<usize, JournalError> {
        read_full(&mut self.file, buf).map_err(|e| self.failed(e))
    }

    /// The error a failed read of this segment is: one of a compressed segment is its
    /// decompression failing, which names the segment.
    fn failed(&self, error: io::Error) -> JournalError {
        if self.compressed {
            JournalError::Compressed {
                segment: self.path.clone(),
                error,
            }
        } else {
            JournalError::Io(error)
        }
    }
}

impl JournalReader {
    /// A reader of `shard`'s segments under `root`, listed now.
    pub fn open(root: impl AsRef<Path>, shard: u16) -> Result<JournalReader, JournalError> {
        JournalReader::open_reading(root.as_ref(), shard, VERSION)
    }

    /// A reader of `shard`'s segments under `root` that reads format versions up to `newest`.
    fn open_reading(root: &Path, shard: u16, newest: u16) -> Result<JournalReader, JournalError> {
        let segments = list_segments(root, shard)?.into_iter().collect();
        Ok(JournalReader {
            segments,
            open: None,
            newest,
        })
    }

    /// The records with the keyed hashes of their spans, in the same order.
    pub fn entries(self) -> Entries {
        Entries { reader: self }
    }

    /// The next entry of the open segment, opening the next segment when there is none.
    /// `Ok(None)` when the open segment ended cleanly.
    fn next_in_segment(&mut self) -> Result<Option<Entry>, JournalError> {
        let open = match &mut self.open {
            Some(open) => open,
            None => {
                let Listed {
                    mut path,
                    mut compressed,
                    ..
                } = self
                    .segments
                    .pop_front()
                    .expect("next checks for a segment");
                let file = match File::open(&path) {
                    // The writer rolled the segment since it was listed: it is now under
                    // its compressed name, whole (the compressed form takes its name before
                    // the uncompressed one is removed).
                    Err(e) if e.kind() == io::ErrorKind::NotFound && !compressed => {
                        path = with_ext(&path, COMPRESSED_EXT);
                        compressed = true;
                        File::open(&path)?
                    }
                    file => file?,
                };
                let file = BufReader::new(file);
                let file: Box<dyn Read + Send> = if compressed {
                    Box::new(zstd::stream::read::Decoder::with_buffer(file)?)
                } else {
                    Box::new(file)
                };
                let open = self.open.insert(Open {
                    path,
                    compressed,
                    file,
                    version: 0,
                });
                let mut head = [0u8; 6];
                if open.read_full(&mut head)? < head.len() {
                    return Err(JournalError::Truncated {
                        segment: open.path.clone(),
                    });
                }
                if head[..4] != MAGIC {
                    return Err(JournalError::BadMagic {
                        segment: open.path.clone(),
                    });
                }
                let version = u16::from_le_bytes([head[4], head[5]]);
                if !(OLDEST_READABLE..=self.newest).contains(&version) {
                    return Err(JournalError::UnsupportedVersion {
                        segment: open.path.clone(),
                        version,
                    });
                }
                open.version = version;
                open
            }
        };
        let mut len = [0u8; 4];
        match open.read_full(&mut len)? {
            0 => return Ok(None),
            4 => {}
            _ => {
                return Err(JournalError::Truncated {
                    segment: open.path.clone(),
                });
            }
        }
        let len = u32::from_le_bytes(len);
        let mut body = Vec::new();
        (&mut open.file)
            .take(u64::from(len))
            .read_to_end(&mut body)
            .map_err(|e| open.failed(e))?;
        if body.len() < len as usize {
            return Err(JournalError::Truncated {
                segment: open.path.clone(),
            });
        }
        format::decode_version(&body, open.version)
            .map(|(record, digests)| Some(Entry { record, digests }))
            .map_err(|what| JournalError::Malformed {
                segment: open.path.clone(),
                what,
            })
    }

    /// The next entry in write order, across segments and days.
    fn next_entry(&mut self) -> Option<Result<Entry, JournalError>> {
        while self.open.is_some() || !self.segments.is_empty() {
            match self.next_in_segment() {
                Ok(Some(entry)) => return Some(Ok(entry)),
                Ok(None) => self.open = None,
                Err(e) => {
                    self.open = None;
                    return Some(Err(e));
                }
            }
        }
        None
    }
}

impl Iterator for JournalReader {
    type Item = Result<Record, JournalError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_entry().map(|e| e.map(|entry| entry.record))
    }
}

/// A record as read back, with the keyed hash of each of its redaction spans in the order
/// [`Record::digests`] gives (decision 0024). A live record matches one read back when its
/// [`blanked`](Record::blanked) form equals `record` and its [`digests`](Record::digests)
/// under the same key equal `digests`: bytes compared outside the spans, hashes inside.
#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct Entry {
    /// The record, its spans blanked ([`Record::blanked`]).
    pub record: Record,
    /// The keyed hash of each span, in record order.
    pub digests: Vec<SpanDigest>,
}

/// A [`JournalReader`]'s entries ([`JournalReader::entries`]).
pub struct Entries {
    reader: JournalReader,
}

impl Iterator for Entries {
    type Item = Result<Entry, JournalError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader.next_entry()
    }
}

/// Reads until `buf` is full or the file ends; how many bytes were read.
fn read_full(r: &mut impl Read, buf: &mut [u8]) -> io::Result<usize> {
    let mut n = 0;
    while n < buf.len() {
        match r.read(&mut buf[n..])? {
            0 => break,
            k => n += k,
        }
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{JournalWriter, Opaque, RedactionKey};
    use fbc_core::{MonoNs, WallNs};
    use std::sync::Arc;

    /// FBC-hxkm's done line: a reader of the previous format version, 7, refuses a journal of
    /// the version consumer records arrived in at its segment's header, naming that version,
    /// rather than part way through at the first consumer record; the reader of the current
    /// version reads the same journal whole.
    #[test]
    fn a_reader_of_the_previous_version_refuses_the_new_one_naming_it() {
        assert_eq!(VERSION, 8);
        // Unit tests get no CARGO_TARGET_TMPDIR: a directory of this process's own.
        let root = std::env::temp_dir().join(format!(
            "fbc-journal-reader-previous-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        let key = Arc::new(RedactionKey::new(&[1; 32]).unwrap());
        let mut writer = JournalWriter::create(&root, 1, key).unwrap();
        let record = Record::Consumer {
            at: MonoNs(1),
            kind: 1,
            input: true,
            bytes: Opaque(b"Start".to_vec()),
        };
        writer.append(WallNs(0), &record).unwrap();
        writer.flush().unwrap();
        drop(writer);
        let current: Vec<_> = JournalReader::open(&root, 1).unwrap().collect();
        assert_eq!(current.len(), 1);
        assert_eq!(current[0].as_ref().unwrap(), &record);

        let previous: Vec<_> = JournalReader::open_reading(&root, 1, VERSION - 1)
            .unwrap()
            .collect();
        let [Err(e @ JournalError::UnsupportedVersion { segment, version })] = &previous[..] else {
            panic!("{previous:?}");
        };
        assert_eq!(*version, 8);
        assert_eq!(*segment, root.join("19700101/1-000000.fbcj"));
        let said = e.to_string();
        assert!(said.contains("journal format version 8,"), "{said}");
        std::fs::remove_dir_all(&root).unwrap();
    }
}
