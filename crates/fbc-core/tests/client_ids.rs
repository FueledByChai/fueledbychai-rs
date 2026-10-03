//! Decision 0004, design §4.3: client ids are minted only under a held namespace lease, survive
//! a restart without re-issuing a sequence, and round-trip through the canonical codec in every
//! wire format, while ids minted by any other system decode as `Unparseable`.

use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

use fbc_core::{
    AccountKey, Charset, CidMatch, CidMint, ClientIdFormat, ClientOrderId, IdError, LeaseError,
    Namespace, NamespaceLease, WallNs, decode_cid, encode_cid,
};

/// A fresh lock directory per test, removed when dropped. The library names no path; the caller
/// supplies one, as the consumer does in production.
struct LockDir(PathBuf);

impl LockDir {
    fn new() -> LockDir {
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("fbc-core-ids-{}-{n}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        LockDir(dir)
    }
}

impl Drop for LockDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

const ACCOUNT: AccountKey = AccountKey::new(7);
const OWN: Namespace = Namespace::new(12);
const OTHER: Namespace = Namespace::new(40_001);

/// 2026-10-02T00:00:00Z in nanoseconds.
const START: WallNs = WallNs(1_759_363_200_000_000_000);

fn formats() -> Vec<ClientIdFormat> {
    vec![
        ClientIdFormat::Alnum {
            max_len: 32,
            charset: Charset::Alphanumeric,
        },
        ClientIdFormat::Alnum {
            max_len: 32,
            charset: Charset::LowerAlphanumeric,
        },
        ClientIdFormat::Uuid,
        ClientIdFormat::Numeric { max_digits: 36 },
        ClientIdFormat::Hex { len: 30 },
        ClientIdFormat::Hex { len: 32 },
    ]
}

fn mint_all(
    dir: &LockDir,
    ns: Namespace,
    hwm: u64,
    snapshot_max: u64,
    n: usize,
) -> Vec<ClientOrderId> {
    let lease = NamespaceLease::acquire(&dir.0, ACCOUNT, ns).unwrap();
    let mut mint = CidMint::new(lease, hwm, snapshot_max, START);
    (0..n).map(|_| mint.mint().unwrap()).collect()
}

#[test]
fn every_minted_id_round_trips_in_every_format() {
    let dir = LockDir::new();
    let mut ids = mint_all(&dir, OWN, 0, 0, 5_000);
    // The top of the sequence space too, up to the last id a mint can issue.
    ids.extend(mint_all(&dir, OWN, u64::MAX - 100, 0, 100));
    ids.extend(mint_all(&dir, Namespace::new(0), 0, 0, 10));
    ids.extend(mint_all(&dir, Namespace::new(u16::MAX), u64::MAX - 1, 0, 1));

    for fmt in formats() {
        let mut seen = HashSet::new();
        for &cid in &ids {
            let wire = encode_cid(&fmt, cid).unwrap();
            assert!(seen.insert(wire), "{fmt:?} encoded two ids as {wire}");
            assert_eq!(
                decode_cid(&fmt, cid.namespace(), &wire),
                CidMatch::Ours(cid),
                "{fmt:?} {wire}"
            );
        }
    }
}

#[test]
fn wire_ids_fit_their_format() {
    let dir = LockDir::new();
    let cid = mint_all(&dir, OWN, u64::MAX - 1, 0, 1)[0];
    for fmt in formats() {
        let wire = encode_cid(&fmt, cid).unwrap();
        match fmt {
            ClientIdFormat::Alnum { max_len, charset } => {
                assert!(wire.len() <= usize::from(max_len));
                let ok = |c: char| match charset {
                    Charset::Alphanumeric => c.is_ascii_alphanumeric(),
                    Charset::LowerAlphanumeric => c.is_ascii_digit() || c.is_ascii_lowercase(),
                };
                assert!(wire.chars().all(ok), "{wire}");
            }
            ClientIdFormat::Uuid => {
                assert_eq!(wire.len(), 36);
                assert_eq!(&wire[14..15], "8", "a version 8 UUID: {wire}");
            }
            ClientIdFormat::Numeric { max_digits } => {
                assert!(wire.len() <= usize::from(max_digits));
                assert!(wire.bytes().all(|b| b.is_ascii_digit()), "{wire}");
            }
            ClientIdFormat::Hex { len } => {
                assert_eq!(wire.len(), usize::from(len));
                assert!(wire.bytes().all(|b| b.is_ascii_hexdigit()), "{wire}");
            }
        }
    }
}

#[test]
fn another_namespace_decodes_as_foreign() {
    let dir = LockDir::new();
    let theirs = mint_all(&dir, OTHER, 0, 0, 50);
    for fmt in formats() {
        for &cid in &theirs {
            let wire = encode_cid(&fmt, cid).unwrap();
            assert_eq!(
                decode_cid(&fmt, OWN, &wire),
                CidMatch::Foreign(OTHER),
                "{fmt:?} {wire}"
            );
        }
    }
}

#[test]
fn ids_minted_by_other_systems_decode_as_unparseable() {
    // The Java brokers seed an AtomicLong with System.currentTimeMillis() and count up from it.
    let java_ms: Vec<String> = (0..1_000u64)
        .map(|i| (1_759_363_200_000 + i).to_string())
        .collect();
    let foreign = [
        "f47ac10b-58cc-4372-a567-0e02b2c3d479", // a random (version 4) UUID
        "F47AC10B-58CC-4372-A567-0E02B2C3D479",
        "0193a7b2-6f1e-7c3d-9a4b-5e6f7a8b9c0d", // a version 7 UUID
        "MY_ORDER_1759363200000",
        "1759363200000-4821",
        "",
        "9",
        "0",
        "00000000000000000000000000000000",
        "ffffffffffffffffffffffffffffff",
        // Multi-byte text whose byte length matches a layout must not split a character.
        "ééééééééééééééé",
        "éééééééééééééééé",
        "0éééééééééééééé0",
        "0ééééééééééééééé0",
        "9010001272063516672000012é",
    ];
    for fmt in formats() {
        for wire in java_ms.iter().map(String::as_str).chain(foreign) {
            assert_eq!(
                decode_cid(&fmt, OWN, wire),
                CidMatch::Unparseable,
                "{fmt:?} {wire:?}"
            );
        }
    }
}

#[test]
fn a_corrupted_id_decodes_as_unparseable() {
    let dir = LockDir::new();
    let ids = mint_all(&dir, OWN, 0, 0, 20);
    for fmt in formats() {
        for &cid in &ids {
            let wire = encode_cid(&fmt, cid).unwrap();
            let bytes = wire.as_bytes();
            for i in 0..bytes.len() {
                if bytes[i] == b'-' {
                    continue;
                }
                for replacement in *b"05afZ" {
                    if replacement.eq_ignore_ascii_case(&bytes[i]) {
                        continue;
                    }
                    let mut changed = bytes.to_vec();
                    changed[i] = replacement;
                    let changed = String::from_utf8(changed).unwrap();
                    assert_eq!(
                        decode_cid(&fmt, OWN, &changed),
                        CidMatch::Unparseable,
                        "{fmt:?} {wire} -> {changed}"
                    );
                }
            }
            // Truncated or extended ids are not ours either.
            assert_eq!(decode_cid(&fmt, OWN, &wire[1..]), CidMatch::Unparseable);
            assert_eq!(
                decode_cid(&fmt, OWN, &format!("{wire}0")),
                CidMatch::Unparseable
            );
        }
    }
}

#[test]
fn a_format_too_small_for_the_canonical_payload_refuses_to_encode() {
    let dir = LockDir::new();
    let cid = mint_all(&dir, OWN, 0, 0, 1)[0];
    let small = [
        ClientIdFormat::Alnum {
            max_len: 20,
            charset: Charset::Alphanumeric,
        },
        ClientIdFormat::Alnum {
            max_len: 23,
            charset: Charset::LowerAlphanumeric,
        },
        ClientIdFormat::Numeric { max_digits: 19 },
        ClientIdFormat::Hex { len: 29 },
        ClientIdFormat::Hex { len: 49 },
    ];
    for fmt in small {
        assert!(
            matches!(encode_cid(&fmt, cid), Err(IdError::DoesNotFit { .. })),
            "{fmt:?}"
        );
    }
}

/// Design §4.3: the seq floor is the maximum of the persisted high-water mark, the highest
/// own-namespace seq in the startup snapshot, and `wall_ms(start) << 12`.
#[test]
fn a_restarted_mint_never_reissues_a_seq_at_or_below_the_high_water_or_snapshot() {
    let dir = LockDir::new();
    let wall_floor = (START.0 as u64 / 1_000_000) << 12;

    // First process life: mint, persist the high-water mark, die (the lease is released).
    let lease = NamespaceLease::acquire(&dir.0, ACCOUNT, OWN).unwrap();
    let mut first = CidMint::new(lease, 0, 0, START);
    let before: Vec<u64> = (0..1_000).map(|_| first.mint().unwrap().seq()).collect();
    let persisted = first.high_water();
    assert_eq!(persisted, *before.last().unwrap());
    assert!(before.iter().all(|&s| s > wall_floor));
    drop(first);

    // A clock that stepped back an hour cannot pull the floor below what was issued.
    let earlier = WallNs(START.0 - 3_600_000_000_000);
    let cases = [
        // (persisted high-water, snapshot max)
        (persisted, 0),
        (persisted, persisted - 10),
        (0, persisted), // the high-water file was lost; the snapshot still shows ours
        (persisted, persisted + 5_000), // orders minted after the last persisted mark still rest
        (persisted + 1_000, persisted), // the +1k reload margin
    ];
    for (hwm, snapshot_max) in cases {
        let lease = NamespaceLease::acquire(&dir.0, ACCOUNT, OWN).unwrap();
        let mut again = CidMint::new(lease, hwm, snapshot_max, earlier);
        for _ in 0..1_000 {
            let seq = again.mint().unwrap().seq();
            assert!(
                seq > hwm && seq > snapshot_max,
                "re-issued {seq} (hwm {hwm}, snap {snapshot_max})"
            );
            assert!(seq > persisted && !before.contains(&seq));
        }
    }
}

#[test]
fn the_wall_clock_floor_applies_when_nothing_was_persisted() {
    let dir = LockDir::new();
    let lease = NamespaceLease::acquire(&dir.0, ACCOUNT, OWN).unwrap();
    let mut mint = CidMint::new(lease, 0, 0, START);
    let wall_floor = (START.0 as u64 / 1_000_000) << 12;
    assert_eq!(mint.high_water(), wall_floor);
    assert_eq!(mint.mint().unwrap().seq(), wall_floor + 1);
    assert_eq!(mint.high_water(), wall_floor + 1);
    assert_eq!(mint.namespace(), OWN);
    assert_eq!(mint.lease().namespace(), OWN);
    assert_eq!(mint.lease().account(), ACCOUNT);
}

#[test]
fn a_wall_time_before_the_epoch_gives_no_floor() {
    let dir = LockDir::new();
    let lease = NamespaceLease::acquire(&dir.0, ACCOUNT, OWN).unwrap();
    let mut mint = CidMint::new(lease, 0, 0, WallNs(-5));
    assert_eq!(mint.mint().unwrap().seq(), 1);
}

#[test]
fn an_exhausted_mint_refuses_rather_than_wraps() {
    let dir = LockDir::new();
    let lease = NamespaceLease::acquire(&dir.0, ACCOUNT, OWN).unwrap();
    let mut mint = CidMint::new(lease, u64::MAX - 1, 0, START);
    assert_eq!(mint.mint().unwrap().seq(), u64::MAX);
    assert_eq!(mint.mint(), Err(IdError::Exhausted));
    assert_eq!(mint.high_water(), u64::MAX);
}

#[test]
fn a_namespace_lease_is_exclusive_while_held() {
    let dir = LockDir::new();
    let held = NamespaceLease::acquire(&dir.0, ACCOUNT, OWN).unwrap();
    assert!(matches!(
        NamespaceLease::acquire(&dir.0, ACCOUNT, OWN),
        Err(LeaseError::Held)
    ));
    // Other namespaces and other accounts are separate leases.
    let _other_ns = NamespaceLease::acquire(&dir.0, ACCOUNT, OTHER).unwrap();
    let _other_account = NamespaceLease::acquire(&dir.0, AccountKey::new(8), OWN).unwrap();
    // A mint keeps its lease held for as long as it lives.
    let mint = CidMint::new(held, 0, 0, START);
    assert!(matches!(
        NamespaceLease::acquire(&dir.0, ACCOUNT, OWN),
        Err(LeaseError::Held)
    ));
    drop(mint);
    NamespaceLease::acquire(&dir.0, ACCOUNT, OWN).unwrap();
}

#[test]
fn a_lease_in_a_missing_directory_is_an_io_error() {
    let dir = LockDir::new();
    let missing = dir.0.join("not-there");
    let err = NamespaceLease::acquire(&missing, ACCOUNT, OWN).unwrap_err();
    assert!(matches!(err, LeaseError::Io(_)));
    assert!(err.to_string().contains("namespace lease"), "{err}");
    assert!(std::error::Error::source(&err).is_some());
    assert_eq!(
        LeaseError::Held.to_string(),
        "the namespace lease is held by another holder"
    );
    assert!(std::error::Error::source(&LeaseError::Held).is_none());
}

#[test]
fn the_decode_scope_tells_ours_from_foreign() {
    let dir = LockDir::new();
    let ours = mint_all(&dir, OWN, 0, 0, 1)[0];
    let theirs = mint_all(&dir, OTHER, 0, 0, 1)[0];
    let fmt = ClientIdFormat::Uuid;
    let (ours_wire, theirs_wire) = (
        encode_cid(&fmt, ours).unwrap(),
        encode_cid(&fmt, theirs).unwrap(),
    );
    fbc_core::dispatch(&fmt, OWN, |scope| {
        assert_eq!(scope.client_order_id(&ours_wire), CidMatch::Ours(ours));
        assert_eq!(
            scope.client_order_id(&theirs_wire),
            CidMatch::Foreign(OTHER)
        );
        // Venues may echo a UUID in upper case.
        assert_eq!(
            scope.client_order_id(&ours_wire.to_uppercase()),
            CidMatch::Ours(ours)
        );
    });
}

/// Wire text comes from the venue: no input may panic the decoder, whatever its length or
/// characters.
#[test]
fn arbitrary_wire_text_never_panics_the_decoder() {
    const ALPHABET: &[char] = &['0', '1', '9', 'a', 'f', 'Z', '-', 'é', '€', '😀'];
    let mut state: u64 = 0x2545_f491_4f6c_dd1d;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    for _ in 0..20_000 {
        let len = (next() % 50) as usize;
        let wire: String = (0..len).map(|_| ALPHABET[(next() % 10) as usize]).collect();
        for fmt in formats() {
            assert_eq!(
                decode_cid(&fmt, OWN, &wire),
                CidMatch::Unparseable,
                "{fmt:?} {wire:?}"
            );
        }
    }
}
