//! What can go wrong writing or reading a journal.

use core::fmt;
use std::io;
use std::path::PathBuf;

/// A journal write or read that failed.
#[derive(Debug)]
pub enum JournalError {
    /// The file system refused.
    Io(io::Error),
    /// A record or one of its fields is longer than the format's `u32` length.
    TooLarge,
    /// A segment does not start with the journal's magic bytes.
    BadMagic { segment: PathBuf },
    /// A segment is in a format version this reader does not know.
    UnsupportedVersion { segment: PathBuf, version: u16 },
    /// A segment ends inside its header or a record (a write cut short).
    Truncated { segment: PathBuf },
    /// A record's body cannot be read: `what` names the field.
    Malformed {
        segment: PathBuf,
        what: &'static str,
    },
}

impl From<io::Error> for JournalError {
    fn from(e: io::Error) -> JournalError {
        JournalError::Io(e)
    }
}

impl fmt::Display for JournalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JournalError::Io(e) => write!(f, "journal i/o: {e}"),
            JournalError::TooLarge => f.write_str("a journal record is too large for its format"),
            JournalError::BadMagic { segment } => {
                write!(f, "{} is not a journal segment", segment.display())
            }
            JournalError::UnsupportedVersion { segment, version } => write!(
                f,
                "{} is in journal format version {version}, which this reader does not know",
                segment.display()
            ),
            JournalError::Truncated { segment } => {
                write!(f, "{} ends inside a record", segment.display())
            }
            JournalError::Malformed { segment, what } => {
                write!(f, "{} has a malformed record: {what}", segment.display())
            }
        }
    }
}

impl std::error::Error for JournalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            JournalError::Io(e) => Some(e),
            _ => None,
        }
    }
}
