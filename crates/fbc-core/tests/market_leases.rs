//! Decision 0013 rule 3, design §13.2: arming a market needs the market lease (one quoter per
//! market and account), plus the account lease where the venue's nonce scope is per account.
//! Both are file locks in a directory the caller supplies, keyed by stable names (the venue's
//! `VenueFactory::id()`, an account name the consumer supplies, the instrument's venue symbol),
//! never by the per-process `InstrumentId` or `AccountKey` numbering. Names and symbols here are
//! synthetic.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};

mod common;

use common::market_data_only_caps;
use fbc_core::{
    AccountLease, InstrumentId, LeaseName, MAX_LOCK_FILE_NAME_LEN, MarketLease, NamedLeaseError,
    VenueSymbol, dispatch_market_data,
};

/// A fresh lock directory per test, removed when dropped. The library names no path; the caller
/// supplies one, as the consumer does in production.
struct LockDir(PathBuf);

impl LockDir {
    fn new() -> LockDir {
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("fbc-core-leases-{}-{n}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        LockDir(dir)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for LockDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

const VENUE: &str = "SYNVENUE";
const ACCOUNT: &str = "acct-a";

/// A venue symbol, built as an adapter builds one: inside a decode scope (decision 0004).
fn symbol(wire: &str) -> VenueSymbol {
    dispatch_market_data(&market_data_only_caps(), |scope| scope.venue_symbol(wire)).unwrap()
}

/// The names of the entries directly inside `dir`, sorted.
fn entries(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
}

#[test]
fn a_market_lease_is_refused_to_a_second_holder_while_held_and_granted_again_once_dropped() {
    let dir = LockDir::new();
    let sym = symbol("SYN-USD-PERP");
    let held = MarketLease::acquire(dir.path(), VENUE, ACCOUNT, &sym).unwrap();
    assert_eq!(held.venue(), VENUE);
    assert_eq!(held.account(), ACCOUNT);
    assert_eq!(held.symbol(), &sym);
    let err = MarketLease::acquire(dir.path(), VENUE, ACCOUNT, &sym).unwrap_err();
    let NamedLeaseError::Held { path } = &err else {
        panic!("expected Held, got {err:?}");
    };
    assert_eq!(
        path,
        &dir.path().join("market+SYNVENUE+acct-a+SYN-USD-PERP.lock")
    );
    assert_eq!(
        err.to_string(),
        format!("the lease {} is held by another holder", path.display())
    );
    assert!(std::error::Error::source(&err).is_none());
    drop(held);
    let again = MarketLease::acquire(dir.path(), VENUE, ACCOUNT, &sym).unwrap();
    // The file is left in place after a release, so every holder locks the same file.
    drop(again);
    assert_eq!(
        entries(dir.path()),
        ["market+SYNVENUE+acct-a+SYN-USD-PERP.lock"]
    );
}

#[test]
fn an_account_lease_is_refused_to_a_second_holder_while_held_and_granted_again_once_dropped() {
    let dir = LockDir::new();
    let held = AccountLease::acquire(dir.path(), VENUE, ACCOUNT).unwrap();
    assert_eq!(held.venue(), VENUE);
    assert_eq!(held.account(), ACCOUNT);
    let err = AccountLease::acquire(dir.path(), VENUE, ACCOUNT).unwrap_err();
    assert!(
        matches!(&err, NamedLeaseError::Held { path }
            if path == &dir.path().join("account+SYNVENUE+acct-a.lock")),
        "{err:?}"
    );
    drop(held);
    AccountLease::acquire(dir.path(), VENUE, ACCOUNT).unwrap();
}

#[test]
fn market_leases_on_one_market_exclude_each_other_whatever_each_caller_numbers_its_instruments() {
    let dir = LockDir::new();
    // Two processes build their spec tables in different orders, so each numbers the same
    // market differently: InstrumentId 1 is BTC to the first and ETH to the second.
    let first: BTreeMap<InstrumentId, VenueSymbol> = [
        (InstrumentId::new(1), symbol("BTC-USD-PERP")),
        (InstrumentId::new(2), symbol("ETH-USD-PERP")),
    ]
    .into();
    let second: BTreeMap<InstrumentId, VenueSymbol> = [
        (InstrumentId::new(1), symbol("ETH-USD-PERP")),
        (InstrumentId::new(2), symbol("BTC-USD-PERP")),
    ]
    .into();

    let first_btc =
        MarketLease::acquire(dir.path(), VENUE, ACCOUNT, &first[&InstrumentId::new(1)]).unwrap();
    // The second caller's BTC is its instrument 2: refused, though the number differs.
    assert!(matches!(
        MarketLease::acquire(dir.path(), VENUE, ACCOUNT, &second[&InstrumentId::new(2)]),
        Err(NamedLeaseError::Held { .. })
    ));
    // The second caller's instrument 1 shares the first's number but is another market: granted.
    let second_eth =
        MarketLease::acquire(dir.path(), VENUE, ACCOUNT, &second[&InstrumentId::new(1)]).unwrap();
    assert!(matches!(
        MarketLease::acquire(dir.path(), VENUE, ACCOUNT, &first[&InstrumentId::new(2)]),
        Err(NamedLeaseError::Held { .. })
    ));
    drop((first_btc, second_eth));
}

#[test]
fn leases_for_different_venues_symbols_or_accounts_do_not_exclude_each_other() {
    let dir = LockDir::new();
    let btc = symbol("BTC-USD-PERP");
    let eth = symbol("ETH-USD-PERP");
    let _market = MarketLease::acquire(dir.path(), VENUE, ACCOUNT, &btc).unwrap();
    let _other_symbol = MarketLease::acquire(dir.path(), VENUE, ACCOUNT, &eth).unwrap();
    let _other_account = MarketLease::acquire(dir.path(), VENUE, "acct-b", &btc).unwrap();
    let _other_venue = MarketLease::acquire(dir.path(), "OTHERVENUE", ACCOUNT, &btc).unwrap();
    // The account lease is a separate lock from the account's market leases.
    let _account = AccountLease::acquire(dir.path(), VENUE, ACCOUNT).unwrap();
    let _other_account_lease = AccountLease::acquire(dir.path(), VENUE, "acct-b").unwrap();
    let _other_venue_account = AccountLease::acquire(dir.path(), "OTHERVENUE", ACCOUNT).unwrap();
    // A name may hold the separator itself without two keys sharing a file: ("a+b", "c") and
    // ("a", "b+c") are different accounts on different venues.
    let _split_one = AccountLease::acquire(dir.path(), "a+b", "c").unwrap();
    let _split_two = AccountLease::acquire(dir.path(), "a", "b+c").unwrap();
    // An escape spelled out is not the byte it escapes.
    let _slash = MarketLease::acquire(dir.path(), VENUE, ACCOUNT, &symbol("X/USDT-P")).unwrap();
    let _spelled = MarketLease::acquire(dir.path(), VENUE, ACCOUNT, &symbol("X%2FUSDT-P")).unwrap();
}

#[test]
fn a_name_that_cannot_be_a_file_name_is_refused() {
    let dir = LockDir::new();
    let sym = symbol("SYN-USD-PERP");
    let empty_venue = MarketLease::acquire(dir.path(), "", ACCOUNT, &sym).unwrap_err();
    assert!(
        matches!(empty_venue, NamedLeaseError::EmptyName(LeaseName::Venue)),
        "{empty_venue:?}"
    );
    assert_eq!(
        empty_venue.to_string(),
        "the venue name is empty, so it cannot name a lock file"
    );
    let empty_account = AccountLease::acquire(dir.path(), VENUE, "").unwrap_err();
    assert!(
        matches!(
            empty_account,
            NamedLeaseError::EmptyName(LeaseName::Account)
        ),
        "{empty_account:?}"
    );
    assert_eq!(
        empty_account.to_string(),
        "the account name is empty, so it cannot name a lock file"
    );
    assert!(matches!(
        MarketLease::acquire(dir.path(), VENUE, "", &sym),
        Err(NamedLeaseError::EmptyName(LeaseName::Account))
    ));
    assert!(matches!(
        AccountLease::acquire(dir.path(), "", ACCOUNT),
        Err(NamedLeaseError::EmptyName(LeaseName::Venue))
    ));

    // A file name longer than a file system takes is refused before anything is created.
    let long_account = "a".repeat(MAX_LOCK_FILE_NAME_LEN);
    let too_long = AccountLease::acquire(dir.path(), VENUE, &long_account).unwrap_err();
    let expected_len = "account+SYNVENUE+.lock".len() + MAX_LOCK_FILE_NAME_LEN;
    assert!(
        matches!(too_long, NamedLeaseError::NameTooLong { len, max }
            if len == expected_len && max == MAX_LOCK_FILE_NAME_LEN),
        "{too_long:?}"
    );
    assert_eq!(
        too_long.to_string(),
        format!(
            "the lock file name would be {expected_len} bytes, over the \
             {MAX_LOCK_FILE_NAME_LEN} a file name may have"
        )
    );
    assert!(std::error::Error::source(&too_long).is_none());
    // An escaped byte counts three times: a name of escapable bytes runs out sooner.
    let escaped = "/".repeat(90);
    assert!(matches!(
        MarketLease::acquire(dir.path(), VENUE, &escaped, &sym),
        Err(NamedLeaseError::NameTooLong { .. })
    ));
    assert!(entries(dir.path()).is_empty());

    // A name exactly at the limit is a file name.
    let fits = "a".repeat(MAX_LOCK_FILE_NAME_LEN - "account+SYNVENUE+.lock".len());
    AccountLease::acquire(dir.path(), VENUE, &fits).unwrap();
}

#[test]
fn every_lock_file_is_created_only_in_the_directory_the_caller_supplies() {
    let parent = LockDir::new();
    let dir = parent.path().join("locks");
    fs::create_dir(&dir).unwrap();
    // Names that would leave the directory, or name a subdirectory, if used as paths.
    let hostile = [
        "..",
        "../escape",
        "/abs",
        "sub/dir",
        "back\\slash",
        "nul\0byte",
        "日本",
    ];
    let mut held_markets = Vec::new();
    let mut held_accounts = Vec::new();
    for name in hostile {
        held_markets
            .push(MarketLease::acquire(&dir, VENUE, name, &symbol("SYN-USD-PERP")).unwrap());
        held_markets.push(MarketLease::acquire(&dir, VENUE, ACCOUNT, &symbol(name)).unwrap());
        held_accounts.push(AccountLease::acquire(&dir, name, ACCOUNT).unwrap());
    }
    // The venue spelling of a Hibachi-style symbol carries a slash.
    held_markets.push(MarketLease::acquire(&dir, VENUE, ACCOUNT, &symbol("BTC/USDT-P")).unwrap());

    // Nothing was created beside the supplied directory.
    assert_eq!(entries(parent.path()), ["locks"]);
    // Inside it, one plain file per lease and no subdirectory.
    let files = entries(&dir);
    assert_eq!(files.len(), held_markets.len() + held_accounts.len());
    for name in &files {
        let meta = fs::symlink_metadata(dir.join(name)).unwrap();
        assert!(meta.is_file(), "{name} is not a plain file");
        assert!(name.ends_with(".lock"), "{name}");
        assert!(
            name.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"+-._%".contains(&b)),
            "{name} holds a byte outside the file-name alphabet"
        );
    }
    assert!(files.contains(&"market+SYNVENUE+acct-a+BTC%2FUSDT-P.lock".to_string()));
    assert!(files.contains(&"market+SYNVENUE+..+SYN-USD-PERP.lock".to_string()));
    assert!(files.contains(&"account+..%2Fescape+acct-a.lock".to_string()));
    assert!(files.contains(&"account+nul%00byte+acct-a.lock".to_string()));
}

#[test]
fn a_lease_in_a_missing_directory_is_an_io_error_naming_the_lock_file() {
    let dir = LockDir::new();
    let missing = dir.path().join("not-there");
    let err = MarketLease::acquire(&missing, VENUE, ACCOUNT, &symbol("SYN-USD-PERP")).unwrap_err();
    let lock = missing.join("market+SYNVENUE+acct-a+SYN-USD-PERP.lock");
    assert!(
        matches!(&err, NamedLeaseError::Io(io) if io.path == lock),
        "{err:?}"
    );
    assert!(
        err.to_string()
            .starts_with(&format!("cannot take the lease {}: ", lock.display())),
        "{err}"
    );
    assert!(std::error::Error::source(&err).is_some());
    let err = AccountLease::acquire(&missing, VENUE, ACCOUNT).unwrap_err();
    assert!(
        matches!(&err, NamedLeaseError::Io(io)
            if io.path == missing.join("account+SYNVENUE+acct-a.lock")),
        "{err:?}"
    );
    // The directory is the caller's to create: the lease never makes it.
    assert!(!missing.exists());
}
