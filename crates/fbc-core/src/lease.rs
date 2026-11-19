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
//! names no path. The lease releases it when dropped, and the operating system when the process
//! exits, however it exits (below, for a child still sharing it); the file is left in place,
//! since deleting a lock file while another process opens it would let two holders lock two
//! different files. Checking that no other
//! process, Java or Rust, already quotes the market (design §13.2 steps 2 and 3) is the
//! consumer's, not this module's.
//!
//! # Release, and child processes
//!
//! An `flock` belongs to the open file description, and a child process shares the description
//! from its fork until its `exec` closes the descriptor (every lock file is opened
//! close-on-exec). Closing the descriptor alone would leave the lock held while any child the
//! process spawned is still between its fork and its `exec`, so a retake just after a drop could
//! read [`NamedLeaseError::Held`] (FBC-qtjn). Dropping a lease therefore first unlocks the
//! description, which releases the lock for every descriptor that shares it, a child's copy
//! included, and only then closes the file. Should the unlock itself fail, the drop ignores it
//! and the close alone releases the lock, as before FBC-qtjn. This holds for the
//! [`MarketLease`], the [`AccountLease`] and the [`NamespaceLease`](crate::NamespaceLease)
//! alike, and for every consumer it means:
//!
//! - a lease this process dropped is free at once, to this process and to every other, whatever
//!   children this process spawned while it held the lease (unless its unlock failed, above);
//! - a lease another process holds, or that this process holds through another lease value, is
//!   still refused `Held`;
//! - a lease is never handed to a child: a child shares the parent's lock only while the parent
//!   holds the lease, and a copy of a lease that a forked child drops (a raw `fork` with no
//!   `exec`) only closes the child's descriptor, since only the process that took the lease
//!   unlocks it; once the parent drops the lease, a forked child's copy excludes no one, so a
//!   forked child that keeps running and needs the lease takes one of its own;
//! - a process that ends without dropping a lease (an abort, a signal) leaves the lock held
//!   until every child that shares the description has run its `exec` or ended, which refuses
//!   the lease to others meanwhile and never grants it twice.
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
    _file: HeldLock,
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
    ///
    /// A lease this process dropped is never refused at its retake because a child process it
    /// spawned meanwhile still shares the old descriptor; a retake is refused only while a lease
    /// value in this process holds it, or another process does (the module's "Release, and
    /// child processes").
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
    _file: HeldLock,
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
) -> Result<HeldLock, NamedLeaseError> {
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

/// A lock file this process opened and locked: the lock every lease holds. Dropping it unlocks
/// the open file description before closing it, so a child process that still shares the
/// description keeps no lock (the module's "Release, and child processes").
#[derive(Debug)]
pub(crate) struct HeldLock {
    file: File,
    /// The process that took the lock, the only one whose drop unlocks it.
    owner: u32,
}

impl Drop for HeldLock {
    fn drop(&mut self) {
        // A forked child that drops its copy only closes its descriptor; unlocking there would
        // release the parent's lock. A failed unlock leaves the release to the close, as before.
        if std::process::id() == self.owner {
            let _ = self.file.unlock();
        }
    }
}

/// Opens (creating it if needed, never truncating) and locks the file at `path` without
/// waiting: `Ok(None)` while another holder has it. The lock code every lease shares.
pub(crate) fn try_lock(path: &Path) -> std::io::Result<Option<HeldLock>> {
    let file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    match file.try_lock() {
        Ok(()) => Ok(Some(HeldLock {
            file,
            owner: std::process::id(),
        })),
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

    /// A fresh directory for one test, removed when dropped.
    struct Scratch(std::path::PathBuf);

    impl Scratch {
        fn new(name: &str) -> Scratch {
            let dir = std::env::temp_dir()
                .join(format!("fbc-core-held-lock-{name}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            Scratch(dir)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn a_dropped_lock_is_free_at_once_while_a_clone_of_its_descriptor_is_still_open() {
        // The clone shares the open file description, as a child does from its fork until its
        // exec: closing the lock's descriptor alone would leave the lock held by the clone.
        let dir = Scratch::new("clone");
        let path = dir.0.join("lease.lock");
        let held = try_lock(&path).unwrap().unwrap();
        let clone = held.file.try_clone().unwrap();
        assert!(
            try_lock(&path).unwrap().is_none(),
            "held while the lock lives"
        );
        drop(held);
        let again = try_lock(&path)
            .unwrap()
            .expect("free once dropped, the clone still open");
        // The retake is exclusive again, and the old clone does not share it.
        assert!(try_lock(&path).unwrap().is_none());
        drop(clone);
        assert!(
            try_lock(&path).unwrap().is_none(),
            "the retake is still held"
        );
        drop(again);
        assert!(try_lock(&path).unwrap().is_some());
    }

    #[test]
    fn a_copy_dropped_in_a_process_that_did_not_take_the_lock_does_not_release_it() {
        // A forked child that drops its copy of a lease is a process other than the owner: it
        // closes its descriptor and leaves the description locked for the parent. The parent's
        // copy is a HeldLock this process owns, as its lease is, so its drop unlocks the
        // description and frees the lock even while another descriptor still shares it. A bare
        // File here would only close, and a sibling test's fork could keep the lock past it
        // (FBC-1mvz).
        let dir = Scratch::new("owner");
        let path = dir.0.join("lease.lock");
        let mut copy = try_lock(&path).unwrap().unwrap();
        let parent = HeldLock {
            file: copy.file.try_clone().unwrap(),
            owner: std::process::id(),
        };
        // Another descriptor sharing the description, as a sibling test's forked child holds
        // one until its exec.
        let shared = copy.file.try_clone().unwrap();
        copy.owner = std::process::id().wrapping_add(1);
        drop(copy);
        assert!(
            try_lock(&path).unwrap().is_none(),
            "the parent's descriptor still holds the lock"
        );
        drop(parent);
        assert!(
            try_lock(&path).unwrap().is_some(),
            "free once the parent's copy is dropped, another descriptor still open"
        );
        drop(shared);
    }

    #[cfg(unix)]
    #[test]
    fn a_lock_a_forked_child_drops_stays_held_by_the_process_that_took_it() {
        use std::os::unix::process::CommandExt;

        // A real fork: the child drops its copy of the lock between its fork and its exec, as a
        // raw-forked child would drop a lease. It is not the owner, so it only closes its
        // descriptor and the description stays locked for this process.
        let dir = Scratch::new("fork");
        let path = dir.0.join("lease.lock");
        let mut copy = Some(try_lock(&path).unwrap().unwrap());
        // The test binary itself, listing its tests: a program that exists wherever the test
        // runs, and that touches no lock.
        let mut cmd = std::process::Command::new(std::env::current_exe().unwrap());
        cmd.arg("--list")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        // SAFETY: the hook runs in the forked child before its exec. Dropping the lock there
        // reads the process id and closes one descriptor: no allocation and no lock.
        unsafe {
            cmd.pre_exec(move || {
                drop(copy.take());
                Ok(())
            });
        }
        let status = cmd.spawn().unwrap().wait().unwrap();
        assert!(status.success(), "the child failed: {status}");
        assert!(
            try_lock(&path).unwrap().is_none(),
            "the child's drop left the lock held"
        );
        // This process's copy lives in the command's hook: dropping it releases the lock.
        drop(cmd);
        assert!(try_lock(&path).unwrap().is_some());
    }

    #[test]
    fn a_name_is_escaped_byte_by_byte() {
        let mut out = String::new();
        escape_into(&mut out, "Az09-_. +%/\0\u{e9}");
        assert_eq!(out, "Az09-_.%20%2B%25%2F%00%C3%A9");
    }
}
