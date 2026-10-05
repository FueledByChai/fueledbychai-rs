//! Market and account leases (decision 0013 rule 3, design §13.2).
//!
//! Arming order entry for a market needs the [`MarketLease`] on (venue, account, symbol): one
//! quoter per market and account. It is also the exclusive account-and-instrument lease that
//! 0005's I7 and 0012 require before an instrument cancel-all. Where the venue's nonce scope is
//! per account, arming also needs the [`AccountLease`] on (venue, account).
//!
//! Both are keyed by stable names, never by [`InstrumentId`](crate::InstrumentId) or
//! [`AccountKey`](crate::AccountKey): those numbers are assigned per process from its own spec
//! table and configuration, so two processes could number one market differently and lock two
//! different files. The venue is [`VenueFactory::id`](crate::VenueFactory::id), the symbol the
//! instrument's [`VenueSymbol`], and the account a name the consumer supplies.
//!
//! Each is an advisory file lock (`flock` on Unix), like the
//! [`NamespaceLease`](crate::NamespaceLease), in a directory the caller supplies; the library
//! names no path. The operating system releases it when the lease is dropped or the process
//! exits, however it exits; the file is left in place, since deleting a lock file while another
//! process opens it would let two holders lock two different files. Checking that no other
//! process, Java or Rust, already quotes the market (design §13.2 steps 2 and 3) is the
//! consumer's, not this module's.
//!
//! # Lock file names
//!
//! `market+<venue>+<account>+<symbol>.lock` and `account+<venue>+<account>.lock`, each name
//! written byte by byte: ASCII letters, digits, `-`, `_` and `.` as they are, every other byte
//! as `%` and two upper-case hex digits. So a name can never leave the directory or name a
//! subdirectory (`BTC/USDT-P` is `BTC%2FUSDT-P`), and since `+` and `%` are always escaped, two
//! different keys never share a file. Letters keep their case; on a case-insensitive file system
//! two names that differ only in case share one lock, which over-excludes and never
//! under-excludes. A name is refused when it is empty or when the file name would be longer than
//! [`MAX_LOCK_FILE_NAME_LEN`] bytes.

use core::fmt;
use std::fs::{File, TryLockError};
use std::path::{Path, PathBuf};

use crate::ids::VenueSymbol;
use crate::mint::LeaseIo;

/// The longest lock file name, in bytes: the file name limit of the common file systems (ext4,
/// XFS, APFS, tmpfs).
pub const MAX_LOCK_FILE_NAME_LEN: usize = 255;

/// The exclusive lease on one market of one account: (venue, account, symbol). Held until it is
/// dropped.
#[derive(Debug)]
pub struct MarketLease {
    venue: String,
    account: String,
    symbol: VenueSymbol,
    _file: File,
}

impl MarketLease {
    /// Takes the lease on `symbol` for `account` on `venue` in `dir`, without waiting.
    ///
    /// `venue` is the venue's [`VenueFactory::id`](crate::VenueFactory::id), `account` the
    /// consumer's name for the account. Fails with [`NamedLeaseError::Held`] while any holder,
    /// in this process or another, has it; with [`NamedLeaseError::EmptyName`] or
    /// [`NamedLeaseError::NameTooLong`] when the names cannot make a file name; and with
    /// [`NamedLeaseError::Io`] when the lock file cannot be opened (for example when `dir` does
    /// not exist; the caller creates it).
    pub fn acquire(
        dir: &Path,
        venue: &str,
        account: &str,
        symbol: &VenueSymbol,
    ) -> Result<MarketLease, NamedLeaseError> {
        let file = lock_named(
            dir,
            "market",
            &[
                (LeaseName::Venue, venue),
                (LeaseName::Account, account),
                (LeaseName::Symbol, symbol.as_wire()),
            ],
        )?;
        Ok(MarketLease {
            venue: venue.to_owned(),
            account: account.to_owned(),
            symbol: symbol.clone(),
            _file: file,
        })
    }

    /// The venue it covers.
    pub fn venue(&self) -> &str {
        &self.venue
    }

    /// The account it covers.
    pub fn account(&self) -> &str {
        &self.account
    }

    /// The instrument it covers.
    pub fn symbol(&self) -> &VenueSymbol {
        &self.symbol
    }
}

/// The exclusive lease on one account of one venue: (venue, account), needed where the venue's
/// nonce scope is per account. Held until it is dropped.
#[derive(Debug)]
pub struct AccountLease {
    venue: String,
    account: String,
    _file: File,
}

impl AccountLease {
    /// Takes the lease on `account` on `venue` in `dir`, without waiting; it fails as
    /// [`MarketLease::acquire`] does.
    pub fn acquire(
        dir: &Path,
        venue: &str,
        account: &str,
    ) -> Result<AccountLease, NamedLeaseError> {
        let file = lock_named(
            dir,
            "account",
            &[(LeaseName::Venue, venue), (LeaseName::Account, account)],
        )?;
        Ok(AccountLease {
            venue: venue.to_owned(),
            account: account.to_owned(),
            _file: file,
        })
    }

    /// The venue it covers.
    pub fn venue(&self) -> &str {
        &self.venue
    }

    /// The account it covers.
    pub fn account(&self) -> &str {
        &self.account
    }
}

/// Which name of a lease's key was refused.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum LeaseName {
    Venue,
    Account,
    Symbol,
}

impl fmt::Display for LeaseName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            LeaseName::Venue => "venue",
            LeaseName::Account => "account",
            LeaseName::Symbol => "symbol",
        })
    }
}

/// Why a market or account lease was not taken. Each names the lock file, or for a refused
/// name which name it was, and nothing else.
#[derive(Debug)]
pub enum NamedLeaseError {
    /// Another holder has the lease on this lock file.
    Held { path: PathBuf },
    /// The lock file could not be opened or locked.
    Io(LeaseIo),
    /// This name is empty.
    EmptyName(LeaseName),
    /// The lock file name would be `len` bytes, over `max`.
    NameTooLong { len: usize, max: usize },
}

impl fmt::Display for NamedLeaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NamedLeaseError::Held { path } => {
                write!(f, "the lease {} is held by another holder", path.display())
            }
            NamedLeaseError::Io(io) => write!(
                f,
                "cannot take the lease {}: {}",
                io.path.display(),
                io.source
            ),
            NamedLeaseError::EmptyName(name) => {
                write!(f, "the {name} name is empty, so it cannot name a lock file")
            }
            NamedLeaseError::NameTooLong { len, max } => write!(
                f,
                "the lock file name would be {len} bytes, over the {max} a file name may have"
            ),
        }
    }
}

impl std::error::Error for NamedLeaseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            NamedLeaseError::Io(io) => Some(&io.source),
            _ => None,
        }
    }
}

/// Locks `<kind>+<name>+….lock` in `dir`, each name escaped as the module documents.
fn lock_named(
    dir: &Path,
    kind: &str,
    names: &[(LeaseName, &str)],
) -> Result<File, NamedLeaseError> {
    let mut file_name = String::from(kind);
    for &(which, name) in names {
        if name.is_empty() {
            return Err(NamedLeaseError::EmptyName(which));
        }
        file_name.push('+');
        escape_into(&mut file_name, name);
    }
    file_name.push_str(".lock");
    if file_name.len() > MAX_LOCK_FILE_NAME_LEN {
        return Err(NamedLeaseError::NameTooLong {
            len: file_name.len(),
            max: MAX_LOCK_FILE_NAME_LEN,
        });
    }
    let path = dir.join(file_name);
    match try_lock(&path) {
        Ok(Some(file)) => Ok(file),
        Ok(None) => Err(NamedLeaseError::Held { path }),
        Err(source) => Err(NamedLeaseError::Io(LeaseIo { path, source })),
    }
}

/// Appends `name` to `out`: ASCII letters, digits, `-`, `_` and `.` as they are, every other
/// byte as `%XX`.
fn escape_into(out: &mut String, name: &str) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for b in name.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.') {
            out.push(char::from(b));
        } else {
            out.push('%');
            out.push(char::from(HEX[usize::from(b >> 4)]));
            out.push(char::from(HEX[usize::from(b & 0x0f)]));
        }
    }
}

/// Opens (creating it if needed, never truncating) and locks the file at `path` without
/// waiting: `Ok(None)` while another holder has it. The lock code every lease shares.
pub(crate) fn try_lock(path: &Path) -> std::io::Result<Option<File>> {
    let file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    match file.try_lock() {
        Ok(()) => Ok(Some(file)),
        Err(TryLockError::WouldBlock) => Ok(None),
        Err(TryLockError::Error(source)) => Err(source),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_name_of_a_key_reads_as_itself() {
        // A venue symbol is never empty, so only this test spells the symbol's name.
        assert_eq!(LeaseName::Venue.to_string(), "venue");
        assert_eq!(LeaseName::Account.to_string(), "account");
        assert_eq!(LeaseName::Symbol.to_string(), "symbol");
    }

    #[test]
    fn a_name_is_escaped_byte_by_byte() {
        let mut out = String::new();
        escape_into(&mut out, "Az09-_. +%/\0\u{e9}");
        assert_eq!(out, "Az09-_.%20%2B%25%2F%00%C3%A9");
    }
}
