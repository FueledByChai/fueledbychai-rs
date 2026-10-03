//! Restart-safe client-id minting under a namespace lease (decisions 0004 and 0010, design
//! §4.3).
//!
//! A [`NamespaceLease`] is an exclusive file lock on one (account, namespace) pair, in a
//! directory the caller supplies; the library names no path. [`CidMint`] is the only minter
//! of [`ClientOrderId`]s, and it takes the lease by value, so minting without a held lease
//! does not compile and the lease stays held for as long as the mint lives. The lease prevents
//! client-id collisions between processes; it does not by itself stop two quoters on one
//! market (that is the market lease, decision 0010).

use core::fmt;
use std::fs::{File, TryLockError};
use std::path::{Path, PathBuf};

use crate::ids::{AccountKey, ClientOrderId, IdError, Namespace};
use crate::time::WallNs;

/// An exclusive lock on one (account, namespace) pair, held until it is dropped.
///
/// The lock is an advisory file lock (`flock` on Unix) on `account-<a>-ns-<n>.lock` in the
/// directory given to [`NamespaceLease::acquire`]. The operating system releases it when the
/// process exits, however it exits, so a crashed process never leaves a stale lease. The file
/// itself is left in place: deleting a lock file while another process opens it would let two
/// holders lock two different files.
#[derive(Debug)]
pub struct NamespaceLease {
    account: AccountKey,
    ns: Namespace,
    _file: File,
}

impl NamespaceLease {
    /// Takes the lease on (`account`, `ns`) in `dir`, without waiting.
    ///
    /// Fails with [`LeaseError::Held`] while any holder, in this process or another, has it,
    /// and with [`LeaseError::Io`] when the lock file cannot be opened (for example when `dir`
    /// does not exist; the caller creates it).
    pub fn acquire(
        dir: &Path,
        account: AccountKey,
        ns: Namespace,
    ) -> Result<NamespaceLease, LeaseError> {
        let path = dir.join(format!("account-{}-ns-{}.lock", account.get(), ns.get()));
        let locked = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .and_then(|file| match file.try_lock() {
                Ok(()) => Ok(Some(file)),
                Err(TryLockError::WouldBlock) => Ok(None),
                Err(TryLockError::Error(source)) => Err(source),
            });
        match locked {
            Ok(Some(file)) => Ok(NamespaceLease {
                account,
                ns,
                _file: file,
            }),
            Ok(None) => Err(LeaseError::Held),
            Err(source) => Err(LeaseError::Io(LeaseIo { path, source })),
        }
    }

    /// The account it covers.
    pub fn account(&self) -> AccountKey {
        self.account
    }

    /// The namespace it covers.
    pub fn namespace(&self) -> Namespace {
        self.ns
    }
}

/// Why a namespace lease was not taken.
#[derive(Debug)]
pub enum LeaseError {
    /// Another holder has the lease.
    Held,
    /// The lock file could not be opened or locked.
    Io(LeaseIo),
}

/// The I/O failure behind [`LeaseError::Io`], with the lock file's path.
#[derive(Debug)]
pub struct LeaseIo {
    /// The lock file.
    pub path: PathBuf,
    /// What the operating system said.
    pub source: std::io::Error,
}

impl fmt::Display for LeaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LeaseError::Held => f.write_str("the namespace lease is held by another holder"),
            LeaseError::Io(io) => {
                write!(
                    f,
                    "cannot take the namespace lease {}: {}",
                    io.path.display(),
                    io.source
                )
            }
        }
    }
}

impl std::error::Error for LeaseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            LeaseError::Held => None,
            LeaseError::Io(io) => Some(&io.source),
        }
    }
}

/// The only minter of [`ClientOrderId`]s, for the namespace of the lease it holds.
///
/// Its seq floor survives restarts: the first id it mints is one above the maximum of
///
/// - the persisted high-water mark for (account, namespace) — the caller persists
///   [`CidMint::high_water`] and, on reload, passes the stored value plus whatever margin covers
///   the mints since the last write;
/// - the highest own-namespace seq in the startup venue snapshot; and
/// - `wall_ms(start) << 12`, which keeps the floor monotone across restarts even when the
///   high-water file is lost.
#[derive(Debug)]
pub struct CidMint {
    lease: NamespaceLease,
    /// The last seq issued, or the floor before the first mint.
    last: u64,
}

impl CidMint {
    /// A mint for the lease's namespace, holding the lease until it is dropped.
    pub fn new(
        lease: NamespaceLease,
        persisted_hwm: u64,
        snapshot_max: u64,
        start: WallNs,
    ) -> CidMint {
        let last = persisted_hwm.max(snapshot_max).max(wall_floor(start));
        CidMint { lease, last }
    }

    /// The next client id, or [`IdError::Exhausted`] once `u64::MAX` has been issued; never a
    /// wrapped or repeated seq.
    pub fn mint(&mut self) -> Result<ClientOrderId, IdError> {
        let seq = self.last.checked_add(1).ok_or(IdError::Exhausted)?;
        self.last = seq;
        Ok(ClientOrderId::new(self.lease.ns, seq))
    }

    /// The highest seq issued so far (the floor, before the first mint): the value to persist.
    pub fn high_water(&self) -> u64 {
        self.last
    }

    /// The namespace it mints in.
    pub fn namespace(&self) -> Namespace {
        self.lease.ns
    }

    /// The lease it holds.
    pub fn lease(&self) -> &NamespaceLease {
        &self.lease
    }
}

/// `wall_ms(start) << 12`; zero before the epoch. An `i64` of nanoseconds is under 2^44
/// milliseconds, so the shift cannot overflow.
fn wall_floor(start: WallNs) -> u64 {
    let ms = u64::try_from(start.0).unwrap_or(0) / 1_000_000;
    ms << 12
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_wall_floor_is_milliseconds_shifted_by_twelve() {
        assert_eq!(wall_floor(WallNs(i64::MIN)), 0);
        assert_eq!(wall_floor(WallNs(999_999)), 0);
        assert_eq!(wall_floor(WallNs(1_000_000)), 4_096);
        assert_eq!(
            wall_floor(WallNs(i64::MAX)),
            (i64::MAX as u64 / 1_000_000) << 12
        );
    }
}
