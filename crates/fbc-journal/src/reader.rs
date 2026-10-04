//! The reader: one shard's records in write order, across its segments and days, compressed
//! and not.

use std::collections::VecDeque;
use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::{Path, PathBuf};

use crate::JournalError;
use crate::format::{self, MAGIC, VERSION};
use crate::record::Record;
use crate::redact::SpanDigest;
use crate::writer::{Listed, list_segments};

/// Reads one shard's records in the order they were written: the day directories in date
/// order, each day's segments in sequence order, each segment from its start. Directories
/// and files that are not this shard's segments are passed over. A closed segment is read
/// through zstd (`.fbcj.zst`), the open one, or one a stopped writer left open, as written.
///
/// An error in a segment (a bad header, a record cut short, a record that cannot be read,
/// compressed data that cannot be decompressed or fails its checksum) is returned once, and
/// reading goes on with the next segment, since nothing after a damaged record in the same
/// segment can be found again. A compressed segment's checksum covers the whole segment and
/// is checked at its end, so a mismatch is reported after the segment's records.
///
/// As an iterator it returns records with their redaction spans blanked
/// ([`Record::blanked`]); [`entries`](JournalReader::entries) returns each with the keyed
/// hashes written in place of its spans.
pub struct JournalReader {
    segments: VecDeque<Listed>,
    open: Option<Open>,
}

/// The segment being read.
struct Open {
    path: PathBuf,
    compressed: bool,
    file: Box<dyn Read + Send>,
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
        let segments = list_segments(root.as_ref(), shard)?.into_iter().collect();
        Ok(JournalReader {
            segments,
            open: None,
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
                    path, compressed, ..
                } = self
                    .segments
                    .pop_front()
                    .expect("next checks for a segment");
                let file = BufReader::new(File::open(&path)?);
                let file: Box<dyn Read + Send> = if compressed {
                    Box::new(zstd::stream::read::Decoder::with_buffer(file)?)
                } else {
                    Box::new(file)
                };
                let open = self.open.insert(Open {
                    path,
                    compressed,
                    file,
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
                if version != VERSION {
                    return Err(JournalError::UnsupportedVersion {
                        segment: open.path.clone(),
                        version,
                    });
                }
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
        format::decode(&body)
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
