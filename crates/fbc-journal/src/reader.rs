//! The reader: one shard's records in write order, across its segments and days.

use std::collections::VecDeque;
use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::{Path, PathBuf};

use crate::JournalError;
use crate::format::{self, MAGIC, VERSION};
use crate::record::Record;
use crate::writer::list_segments;

/// Reads one shard's records in the order they were written: the day directories in date
/// order, each day's segments in sequence order, each segment from its start. Directories
/// and files that are not this shard's segments are passed over.
///
/// An error in a segment (a bad header, a record cut short, a record that cannot be read)
/// is returned once, and reading goes on with the next segment, since nothing after a
/// damaged record in the same segment can be found again.
pub struct JournalReader {
    segments: VecDeque<PathBuf>,
    open: Option<(PathBuf, BufReader<File>)>,
}

impl JournalReader {
    /// A reader of `shard`'s segments under `root`, listed now.
    pub fn open(root: impl AsRef<Path>, shard: u16) -> Result<JournalReader, JournalError> {
        let segments = list_segments(root.as_ref(), shard)?
            .into_iter()
            .map(|(.., path)| path)
            .collect();
        Ok(JournalReader {
            segments,
            open: None,
        })
    }

    /// The next record of the open segment, opening the next segment when there is none.
    /// `Ok(None)` when the open segment ended cleanly.
    fn next_in_segment(&mut self) -> Result<Option<Record>, JournalError> {
        let (path, file) = match &mut self.open {
            Some(open) => open,
            None => {
                let path = self
                    .segments
                    .pop_front()
                    .expect("next checks for a segment");
                let mut file = BufReader::new(File::open(&path)?);
                let mut head = [0u8; 6];
                if read_full(&mut file, &mut head)? < head.len() {
                    return Err(JournalError::Truncated { segment: path });
                }
                if head[..4] != MAGIC {
                    return Err(JournalError::BadMagic { segment: path });
                }
                let version = u16::from_le_bytes([head[4], head[5]]);
                if version != VERSION {
                    return Err(JournalError::UnsupportedVersion {
                        segment: path,
                        version,
                    });
                }
                self.open.insert((path, file))
            }
        };
        let mut len = [0u8; 4];
        match read_full(file, &mut len)? {
            0 => return Ok(None),
            4 => {}
            _ => {
                return Err(JournalError::Truncated {
                    segment: path.clone(),
                });
            }
        }
        let len = u32::from_le_bytes(len);
        let mut body = Vec::new();
        file.take(u64::from(len)).read_to_end(&mut body)?;
        if body.len() < len as usize {
            return Err(JournalError::Truncated {
                segment: path.clone(),
            });
        }
        format::decode(&body)
            .map(Some)
            .map_err(|what| JournalError::Malformed {
                segment: path.clone(),
                what,
            })
    }
}

impl Iterator for JournalReader {
    type Item = Result<Record, JournalError>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.open.is_some() || !self.segments.is_empty() {
            match self.next_in_segment() {
                Ok(Some(record)) => return Some(Ok(record)),
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
