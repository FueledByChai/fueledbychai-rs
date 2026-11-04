//! FBC-77aa: QFEX's trade-socket signer and authentication (`src/sign` and `src/auth`, review
//! paths, decision 0009), offline and recording-only (`caps.exec` stays `None`).
//!
//! - The HMAC signature equals the vectors `fixtures/qfex/signing/hmac_oracle.py` computed with
//!   Python's own `hmac` and `hashlib`: nonce 0 and u64::MAX in each half (zero padding), and
//!   the whole-second floor either side of a second, at the epoch and at the latest wall time.
//! - `QfexNonces` reserves far more nonces than its window while remembering at most the window,
//!   and never repeats a value inside it.
//! - The provider account is selected explicitly or not at all, and `exec_account_key` returns
//!   the configured account key (decision 0122).
//! - The authentication frame, the opening URL and every private frame name the public key, the
//!   subaccount UUID, the signature and `user_id` as spans of their own, so none reaches a
//!   journal record in clear, and a redacted record reads back to the same JSON but for those
//!   values.
//! - No credential is read or held outside `src/auth` and `src/sign`.
//!
//! Every key, secret, account and frame here is synthetic: the secrets are the made-up ones in
//! `fixtures/qfex/signing` and the frames are `fixtures/qfex/private` (each directory's
//! `SYNTHETIC` file says where they come from).

use std::collections::{HashMap, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fbc_core::{
    AccountKey, ConfigError, ConfigScope, ConnKey, EncodeCtx, HttpResponse, HttpTag, Inbound,
    MonoNs, NonceBlock, NonceSource, RawFrame, Secret, Secrets, SignError, Stamp, VenueConfig,
    VenueError, VenueFactory, WallNs, WireSlice, check_redactions,
};
use fbc_journal::{JournalReader, JournalWriter, Opcode, Record, RedactionKey};
use fbc_venue_qfex::Qfex;
use fbc_venue_qfex::auth::{
    self, ACCOUNT_ID, ACCOUNT_KEY, AccountSelection, PUBLIC_KEY, SECRET_KEY, TRADE_URL, TradeAuth,
};
use fbc_venue_qfex::sign::{Entropy, EntropyFailed, HmacKey, KeyError, QfexNonces};
use serde_json::Value;

/// A made-up public key, in the alphabet QFEX's examples show.
const PUBLIC: &str = "qfex_pub_SYNTHETIC_0001";
/// A made-up subaccount UUID.
const SUB: &str = "5a5a5a5a-0000-4000-8000-00000000c0de";
/// The made-up user id every `fixtures/qfex/private` frame carries.
const USER: &str = "5a5a5a5a-0000-4000-8000-0000000000a1";
/// The made-up position and balance record ids those frames carry.
const RECORD_IDS: [&str; 2] = [
    "66666666-6666-4666-8666-666666666666",
    "77777777-7777-4777-8777-777777777777",
];

// ----- The HMAC vectors -----

/// One row of `hmac-vectors.json`.
struct Vector {
    name: String,
    secret: String,
    nonce: [u64; 2],
    wall: WallNs,
    nonce_hex: String,
    unix_ts: u64,
    message: String,
    signature: String,
}

fn vectors() -> Vec<Vector> {
    let file: Value = serde_json::from_str(include_str!(
        "../../../../fixtures/qfex/signing/hmac-vectors.json"
    ))
    .unwrap();
    let text = |v: &Value, key: &str| v[key].as_str().unwrap().to_owned();
    file["vectors"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| Vector {
            name: text(v, "name"),
            secret: text(v, "secret"),
            nonce: [
                v["nonce"][0].as_u64().unwrap(),
                v["nonce"][1].as_u64().unwrap(),
            ],
            wall: WallNs(v["wall_ns"].as_i64().unwrap()),
            nonce_hex: text(v, "nonce_hex"),
            unix_ts: v["unix_ts"].as_u64().unwrap(),
            message: text(v, "message"),
            signature: text(v, "signature"),
        })
        .collect()
}

fn vector(name: &str) -> Vector {
    vectors().into_iter().find(|v| v.name == name).unwrap()
}

fn key(secret: &str) -> HmacKey {
    HmacKey::new(Secret::new(secret.to_owned())).unwrap()
}

#[test]
fn signatures_equal_the_independent_python_vectors() {
    let vectors = vectors();
    for v in &vectors {
        let signed = key(&v.secret).sign(v.nonce, v.wall).unwrap();
        assert_eq!(signed.nonce(), v.nonce_hex, "{}", v.name);
        assert_eq!(signed.unix_ts(), v.unix_ts, "{}", v.name);
        assert_eq!(
            format!("{}:{}", signed.nonce(), signed.unix_ts()),
            v.message,
            "{}",
            v.name
        );
        assert_eq!(signed.signature(), v.signature, "{}", v.name);
    }
    // The cases the done line names are there, with what they pin.
    let zero = vector("nonce_zero");
    assert_eq!(
        (zero.nonce, zero.nonce_hex.as_str()),
        ([0, 0], "0".repeat(32).as_str())
    );
    let max = vector("nonce_max");
    assert_eq!(max.nonce, [u64::MAX, u64::MAX]);
    assert_eq!(max.nonce_hex, "f".repeat(32));
    // Zero padding: each half is 16 digits whatever its value.
    assert_eq!(
        vector("padding_low_half").nonce_hex,
        format!("{}{}", "0".repeat(16), "f".repeat(16))
    );
    assert_eq!(
        vector("padding_ones").nonce_hex,
        format!("{0}1{0}1", "0".repeat(15))
    );
    // The floor: the last nanosecond of a second signs that second, the next one the next.
    let last = vector("floor_last_nanosecond");
    let next = vector("floor_next_second");
    assert_eq!(last.wall.0 + 1, next.wall.0);
    assert_eq!(last.unix_ts + 1, next.unix_ts);
    assert_eq!(vector("floor_epoch_last_nanosecond").unix_ts, 0);
    assert_eq!(vector("floor_latest_wall").wall, WallNs(i64::MAX));
    // The secret's UTF-8 bytes are the key.
    assert!(!vector("utf8_secret").secret.is_ascii());
    assert_ne!(
        vector("utf8_secret").signature,
        vector("docs_shaped").signature
    );
}

#[test]
fn signing_refuses_time_before_the_epoch_and_an_empty_secret_and_shows_neither() {
    let v = vector("docs_shaped");
    assert!(matches!(
        key(&v.secret).sign(v.nonce, WallNs(-1)),
        Err(SignError::Unsignable(_))
    ));
    assert_eq!(
        HmacKey::new(Secret::new(String::new())).err(),
        Some(KeyError::Empty)
    );
    let key = key(&v.secret);
    let signed = key.sign(v.nonce, v.wall).unwrap();
    for shown in [format!("{key:?}"), format!("{signed:?}")] {
        assert!(!shown.contains(&v.secret), "{shown}");
        assert!(!shown.contains(signed.signature()), "{shown}");
    }
    assert!(format!("{signed:?}").contains(signed.nonce()));
}

// ----- The nonce source -----

/// Entropy that walks a cycle of `period` values, so a value comes round again after `period`
/// draws, and on every tenth draw instead repeats the value it gave 100 draws before, which the
/// source still remembers.
struct Cycling {
    period: u64,
    next: u64,
    draws: u64,
    given: VecDeque<u64>,
}

impl Entropy for Cycling {
    fn fill(&mut self, bytes: &mut [u8]) -> Result<(), EntropyFailed> {
        self.draws += 1;
        let value = if self.draws.is_multiple_of(10) && self.given.len() >= 100 {
            self.given[self.given.len() - 100]
        } else {
            let value = self.next % self.period;
            self.next += 1;
            self.given.push_back(value);
            if self.given.len() > 200 {
                self.given.pop_front();
            }
            value
        };
        bytes.copy_from_slice(&value.to_be_bytes());
        Ok(())
    }
}

#[test]
fn the_nonce_source_issues_far_more_than_its_window_in_bounded_memory_with_no_repeat_inside_it() {
    const WINDOW: usize = QfexNonces::<Cycling>::WINDOW;
    const LONGEST: u16 = 5000;
    let mut source = QfexNonces::with_entropy(Cycling {
        // Room for the window and the longest block, so every block can be filled.
        period: (WINDOW + usize::from(LONGEST)) as u64 + 7,
        next: 0,
        draws: 0,
        given: VecDeque::new(),
    });
    let mut issued: Vec<u64> = Vec::new();
    // Blocks of an authentication's two, of one, of a batch's many, and one longer than the
    // window itself, until twenty windows' worth have been issued.
    let sizes = [2u16, 1, 37, 2, 5, LONGEST, 2];
    let mut round = 0;
    while issued.len() < 20 * WINDOW {
        let len = sizes[round % sizes.len()];
        round += 1;
        let block = source.reserve(len);
        assert_eq!(block.len(), usize::from(len));
        issued.extend_from_slice(block.as_slice());
        assert!(source.remembered() <= WINDOW, "{}", source.remembered());
    }
    assert_eq!(source.remembered(), WINDOW);
    // No value repeats inside the window: a value seen again was last issued more than WINDOW
    // values before. Values do come round again past it: the source forgot them.
    let mut last: HashMap<u64, usize> = HashMap::new();
    let mut repeats = 0;
    for (at, value) in issued.iter().enumerate() {
        if let Some(before) = last.insert(*value, at) {
            assert!(at - before > WINDOW, "{value} at {before} and {at}");
            repeats += 1;
        }
    }
    assert!(
        repeats > 10 * WINDOW,
        "only {repeats} values came round again"
    );
}

#[test]
fn the_production_nonce_source_gives_whole_blocks_of_random_values() {
    let mut source = QfexNonces::os();
    let a = source.reserve(TradeAuth::NONCES);
    let b = source.reserve(TradeAuth::NONCES);
    assert_eq!((a.len(), b.len()), (2, 2));
    assert_ne!(a, b);
    assert_eq!(source.remembered(), 4);
    assert_eq!(source.high_water(), None);
}

// ----- Account selection and the account key (decision 0122) -----

fn cfg(account: &str) -> VenueConfig {
    cfg_without(account, "")
}

/// `cfg(account)` with `skip` left out.
fn cfg_without(account: &str, skip: &str) -> VenueConfig {
    let mut cfg = VenueConfig::new();
    for (key, value) in [
        (ACCOUNT_ID, account),
        (ACCOUNT_KEY, "4"),
        (PUBLIC_KEY, PUBLIC),
        (TRADE_URL, "wss://trade.qfex.com"),
    ] {
        if key != skip {
            cfg.insert(key, value);
        }
    }
    cfg
}

fn creds(public: &str, secret: &str) -> Secrets {
    let mut creds = Secrets::new();
    creds.insert(PUBLIC_KEY, Secret::new(public.to_owned()));
    creds.insert(SECRET_KEY, Secret::new(secret.to_owned()));
    creds
}

fn handle(account: &str) -> TradeAuth {
    TradeAuth::new(&cfg(account), creds(PUBLIC, &vector("docs_shaped").secret)).unwrap()
}

#[test]
fn the_provider_account_is_primary_or_a_canonical_subaccount_and_nothing_else() {
    assert_eq!(
        auth::account_selection(&cfg("primary")),
        Ok(AccountSelection::Primary)
    );
    assert_eq!(
        auth::account_selection(&cfg(SUB)),
        Ok(AccountSelection::Subaccount(SUB))
    );
    let missing = cfg_without("primary", ACCOUNT_ID);
    assert_eq!(
        auth::account_selection(&missing),
        Err(ConfigError::Missing(ACCOUNT_ID))
    );
    for bad in [
        "",
        "PRIMARY",
        "Primary",
        " primary",
        "primary ",
        "main",
        "5A5A5A5A-0000-4000-8000-00000000C0DE",
        "5a5a5a5a000040008000 00000000c0de",
        "5a5a5a5a00004000800000000000c0de",
        "{5a5a5a5a-0000-4000-8000-00000000c0de}",
        "urn:uuid:5a5a5a5a-0000-4000-8000-00000000c0de",
        "5a5a5a5a-0000-4000-8000-00000000c0d",
        "5a5a5a5a-0000-4000-8000-00000000c0dee",
        "5a5a5a5a-0000-4000-8000-00000000c0dg",
        "5a5a5a5a0-000-4000-8000-00000000c0de",
        "00000000-0000-0000-0000-000000000000",
        " 5a5a5a5a-0000-4000-8000-00000000c0d",
    ] {
        let cfg = cfg(bad);
        assert!(
            matches!(
                auth::account_selection(&cfg),
                Err(ConfigError::Invalid {
                    key: ACCOUNT_ID,
                    ..
                })
            ),
            "{bad:?}"
        );
        // No fallback to the primary account anywhere: no key, no handle.
        assert!(auth::account_key(&cfg).is_err(), "{bad:?}");
        assert!(Qfex.exec_account_key(&cfg).is_err(), "{bad:?}");
        assert!(TradeAuth::new(&cfg, creds(PUBLIC, "s")).is_err(), "{bad:?}");
    }
    assert!(TradeAuth::new(&missing, creds(PUBLIC, "s")).is_err());
    assert_eq!(
        Qfex.exec_account_key(&missing),
        Err(ConfigError::Missing(ACCOUNT_ID))
    );
    // The selection's Debug never shows the subaccount.
    let shown = format!("{:?}", auth::account_selection(&cfg(SUB)).unwrap());
    assert_eq!(shown, "Subaccount(<redacted>)");
    assert_eq!(format!("{:?}", AccountSelection::Primary), "Primary");
}

#[test]
fn exec_account_key_returns_the_configured_key_for_an_explicit_selection_only() {
    for account in ["primary", SUB] {
        for (raw, n) in [("0", 0u16), ("4", 4), ("65535", 65535)] {
            let mut cfg = cfg(account);
            cfg.insert(ACCOUNT_KEY, raw);
            assert_eq!(
                Qfex.exec_account_key(&cfg),
                Ok(Some(AccountKey::new(n))),
                "{account} {raw}"
            );
            assert_eq!(auth::account_key(&cfg), Ok(AccountKey::new(n)));
        }
        for bad in [
            "", "04", "+4", "-1", " 4", "4 ", "65536", "0x4", "4.0", "four",
        ] {
            let mut cfg = cfg(account);
            cfg.insert(ACCOUNT_KEY, bad);
            assert!(
                matches!(
                    Qfex.exec_account_key(&cfg),
                    Err(ConfigError::Invalid {
                        key: ACCOUNT_KEY,
                        ..
                    })
                ),
                "{bad:?}"
            );
        }
        let cfg = cfg_without(account, ACCOUNT_KEY);
        assert_eq!(
            Qfex.exec_account_key(&cfg),
            Err(ConfigError::Missing(ACCOUNT_KEY))
        );
    }
    // The handle is bound to the same key.
    assert_eq!(handle(SUB).account(), AccountKey::new(4));
    // A market-data configuration names no account: market data needs none, and no
    // order-entry session can be bound to it.
    let mut md = VenueConfig::new();
    md.insert(fbc_venue_qfex::KEY_MDS_URL, "wss://mds.qfex.com");
    assert!(Qfex.caps(&md).is_ok());
    assert_eq!(
        Qfex.exec_account_key(&md),
        Err(ConfigError::Missing(ACCOUNT_ID))
    );
    // The factory lists both keys, account-scoped; order entry stays off.
    for key in [ACCOUNT_ID, ACCOUNT_KEY] {
        let field = Qfex.config_schema().iter().find(|f| f.key == key).unwrap();
        assert_eq!(field.scope, ConfigScope::Account);
    }
    assert!(Qfex.caps(&md).unwrap().exec.is_none());
    assert_eq!(Qfex.plan_exec(&cfg(SUB)), Ok(Vec::new()));
    assert!(Qfex.exec_codec(&cfg(SUB), creds(PUBLIC, "s")).is_none());
}

// ----- The handle, the frame and the URL -----

#[test]
fn the_handle_refuses_missing_unsafe_or_mismatched_credentials_naming_only_the_key() {
    let secret = vector("docs_shaped").secret;
    let refused = |cfg: &VenueConfig, creds: Secrets| match TradeAuth::new(cfg, creds) {
        Err(VenueError::Config(err)) => err,
        other => panic!("{other:?}"),
    };
    let no_public = cfg_without(SUB, PUBLIC_KEY);
    assert_eq!(
        refused(&no_public, creds(PUBLIC, &secret)),
        ConfigError::Missing(PUBLIC_KEY)
    );
    let long = "k".repeat(257);
    for unsafe_key in ["", "bad/key", "a\"b", "a b", "k\u{e9}y", long.as_str()] {
        let mut cfg = cfg(SUB);
        cfg.insert(PUBLIC_KEY, unsafe_key);
        assert!(matches!(
            refused(&cfg, creds(unsafe_key, &secret)),
            ConfigError::Invalid {
                key: PUBLIC_KEY,
                ..
            }
        ));
    }
    let mut longest = cfg(SUB);
    let key = "k".repeat(256);
    longest.insert(PUBLIC_KEY, &key);
    assert!(TradeAuth::new(&longest, creds(&key, &secret)).is_ok());

    let mut only_secret = Secrets::new();
    only_secret.insert(SECRET_KEY, Secret::new(secret.clone()));
    assert_eq!(
        refused(&cfg(SUB), only_secret),
        ConfigError::Missing(PUBLIC_KEY)
    );
    let mut only_public = Secrets::new();
    only_public.insert(PUBLIC_KEY, Secret::new(PUBLIC.to_owned()));
    assert_eq!(
        refused(&cfg(SUB), only_public),
        ConfigError::Missing(SECRET_KEY)
    );
    let other = "qfex_pub_SYNTHETIC_0002";
    let mismatch = refused(&cfg(SUB), creds(other, &secret));
    assert!(matches!(
        mismatch,
        ConfigError::Invalid {
            key: PUBLIC_KEY,
            ..
        }
    ));
    let empty = refused(&cfg(SUB), creds(PUBLIC, ""));
    assert!(matches!(
        empty,
        ConfigError::Invalid {
            key: SECRET_KEY,
            ..
        }
    ));
    for shown in [
        format!("{mismatch:?} {mismatch}"),
        format!("{empty:?} {empty}"),
    ] {
        assert!(!shown.contains(other) && !shown.contains(PUBLIC), "{shown}");
    }
    // The handle shows its account key and kind only.
    let shown = format!("{:?}", handle(SUB));
    assert_eq!(
        shown,
        "TradeAuth { account: AccountKey(4), subaccount: true, .. }"
    );
    assert!(!handle("primary").is_subaccount());
}

fn ctx(v: &Vector) -> EncodeCtx {
    EncodeCtx {
        wall: v.wall,
        mono: MonoNs(1),
        nonces: NonceBlock::new(v.nonce.to_vec()),
    }
}

/// The bytes of each of `frame`'s spans, as text.
fn span_texts(frame: &WireSlice) -> Vec<String> {
    frame
        .redactions()
        .iter()
        .map(|span| {
            String::from_utf8(frame.bytes()[span.start as usize..span.end as usize].to_vec())
                .unwrap()
        })
        .collect()
}

#[test]
fn the_frame_is_qfex_s_auth_message_with_its_account_material_in_spans_of_its_own() {
    let v = vector("docs_shaped");
    let primary = handle("primary").frame(&ctx(&v)).unwrap();
    let text = std::str::from_utf8(primary.bytes()).unwrap();
    assert_eq!(
        text,
        format!(
            r#"{{"type":"auth","params":{{"hmac":{{"public_key":"{PUBLIC}","nonce":"{}","unix_ts":{},"signature":"{}"}}}}}}"#,
            v.nonce_hex, v.unix_ts, v.signature
        )
    );
    assert_eq!(span_texts(&primary), [PUBLIC, v.signature.as_str()]);

    let sub = handle(SUB).frame(&ctx(&v)).unwrap();
    let parsed: Value = serde_json::from_slice(sub.bytes()).unwrap();
    assert_eq!(parsed["params"]["account_id"], SUB);
    assert_eq!(parsed["params"]["hmac"]["signature"], v.signature.as_str());
    assert!(parsed["params"]["hmac"].get("account_id").is_none());
    assert_eq!(span_texts(&sub), [PUBLIC, v.signature.as_str(), SUB]);
    check_redactions(sub.bytes(), sub.redactions()).unwrap();
    let shown = format!("{sub:?}");
    for hidden in [PUBLIC, SUB, v.signature.as_str(), v.secret.as_str()] {
        assert!(!shown.contains(hidden), "{shown}");
    }
    // Exactly two nonces, and a time a Unix timestamp can state.
    for nonces in [vec![], vec![1], vec![1, 2, 3]] {
        let ctx = EncodeCtx {
            nonces: NonceBlock::new(nonces),
            ..ctx(&v)
        };
        assert!(matches!(
            handle(SUB).frame(&ctx),
            Err(SignError::Unsignable(_))
        ));
    }
    let before_epoch = EncodeCtx {
        wall: WallNs(-1),
        ..ctx(&v)
    };
    assert!(handle(SUB).frame(&before_epoch).is_err());
}

#[test]
fn the_opening_url_carries_the_public_key_in_a_span_and_refuses_any_other_origin() {
    let url = |origin: &str| {
        let mut cfg = cfg(SUB);
        cfg.insert(TRADE_URL, origin);
        auth::opening_url(&cfg)
    };
    for origin in ["wss://trade.qfex.com", "wss://trade.qfex.com/"] {
        let url = url(origin).unwrap();
        assert_eq!(
            url.as_str(),
            format!("wss://trade.qfex.com/?api_key={PUBLIC}")
        );
        let [span] = url.redactions() else {
            panic!("{:?}", url.redactions())
        };
        assert_eq!(
            &url.as_str()[span.start as usize..span.end as usize],
            PUBLIC
        );
        assert!(!format!("{url:?}").contains(PUBLIC));
    }
    for loopback in [
        "ws://127.0.0.1:9001",
        "ws://localhost:1",
        "ws://[::1]:2",
        "wss://[::1]",
    ] {
        assert!(url(loopback).is_ok(), "{loopback}");
    }
    for bad in [
        "ws://trade.qfex.com",
        "ws://10.0.0.1:9001",
        "https://trade.qfex.com",
        "wss://trade.qfex.com/path",
        "wss://trade.qfex.com?x=1",
        "wss://user@trade.qfex.com",
        "wss://trade.qfex.com:0",
        "wss://",
    ] {
        assert!(
            matches!(url(bad), Err(ConfigError::Invalid { key: TRADE_URL, .. })),
            "{bad}"
        );
    }
    let no_origin = cfg_without(SUB, TRADE_URL);
    assert_eq!(
        auth::opening_url(&no_origin),
        Err(ConfigError::Missing(TRADE_URL))
    );
    let mut bad_key = cfg(SUB);
    bad_key.insert(PUBLIC_KEY, "bad&key");
    assert!(auth::opening_url(&bad_key).is_err());
}

// ----- Redaction through the journal -----

/// The private frames of `fixtures/qfex/private`, by name, with the JSON paths each must lose.
fn fixtures() -> Vec<(&'static str, String, Vec<&'static str>)> {
    let read = |name: &str| {
        fs::read_to_string(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../../fixtures/qfex/private")
                .join(name),
        )
        .unwrap()
    };
    vec![
        (
            "order_response",
            read("order_response.json"),
            vec!["order_response.user_id"],
        ),
        (
            "all_orders_response",
            read("all_orders_response.json"),
            vec![
                "all_orders_response.orders[0].user_id",
                "all_orders_response.orders[1].user_id",
                "all_orders_response.twaps[0].user_id",
            ],
        ),
        (
            "fill_response",
            read("fill_response.json"),
            vec!["fill_response.user_id"],
        ),
        (
            "position_response",
            read("position_response.json"),
            vec!["position_response.id"],
        ),
        (
            "balance_response",
            read("balance_response.json"),
            vec!["balance_response.id", "balance_response.user_id"],
        ),
        (
            "stop_order_response",
            read("stop_order_response.json"),
            vec!["stop_order_response.user_id"],
        ),
    ]
}

/// Every JSON path where `live` and `replayed` differ; each differing value must be the
/// journal's blank for it, a run of `2`s as long as the original.
fn differing(live: &Value, replayed: &Value, path: &str, out: &mut Vec<String>) {
    let join = |name: &str| match path {
        "" => name.to_owned(),
        _ => format!("{path}.{name}"),
    };
    match (live, replayed) {
        (Value::Object(a), Value::Object(b)) if a.keys().eq(b.keys()) => {
            for (name, value) in a {
                differing(value, &b[name], &join(name), out);
            }
        }
        (Value::Array(a), Value::Array(b)) if a.len() == b.len() => {
            for (at, (x, y)) in a.iter().zip(b).enumerate() {
                differing(x, y, &format!("{path}[{at}]"), out);
            }
        }
        (a, b) if a == b => {}
        // Blanked as written: a string with escapes is longer than its decoded text.
        (Value::String(a), Value::String(b)) => {
            assert!(
                b.len() >= a.len() && b.bytes().all(|c| c == b'2'),
                "{path}: {b}"
            );
            out.push(path.to_owned());
        }
        (a, b) => panic!("{path}: {a} read back as {b}"),
    }
}

/// `bytes` with every span blanked, as the journal's reader gives them back.
fn blanked(bytes: &[u8], spans: &[core::ops::Range<u32>]) -> Vec<u8> {
    let mut out = bytes.to_vec();
    for span in spans {
        out[span.start as usize..span.end as usize].fill(b'2');
    }
    out
}

fn stamp(seq: u64) -> Stamp {
    Stamp {
        ingest_seq: seq,
        kernel_rx: None,
        recv_mono: MonoNs(seq),
        recv_wall: WallNs(1_760_000_000_000_000_000),
        conn: ConnKey { conn: 0, epoch: 1 },
    }
}

/// Every file under `dir`, read whole.
fn files(dir: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            out.extend(files(&path));
        } else {
            let bytes = fs::read(&path).unwrap();
            out.push((path, bytes));
        }
    }
    out
}

fn contains(haystack: &[u8], needle: &str) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle.as_bytes())
}

#[test]
fn no_account_material_reaches_the_journal_in_clear_and_a_redacted_record_reads_back_the_same() {
    let v = vector("docs_shaped");
    let sub = handle(SUB);
    let auth_frames = [
        handle("primary").frame(&ctx(&v)).unwrap(),
        sub.frame(&ctx(&v)).unwrap(),
    ];
    let signature = v.signature.clone();
    let auth_text = String::from_utf8(auth_frames[1].bytes().to_vec()).unwrap();

    // The private frames, and three refusals that echo what authentication sent: as an
    // object, as a string, and in free text, as QFEX's errors page says `incoming_message` and
    // `message` may.
    let mut inbound = fixtures();
    inbound.push((
        "echo_object",
        format!(
            r#"{{"err":{{"error_code":"PermissionDenied","message":"not permitted","incoming_message":{auth_text}}}}}"#
        ),
        vec![
            "err.incoming_message.params.account_id",
            "err.incoming_message.params.hmac.public_key",
            "err.incoming_message.params.hmac.signature",
        ],
    ));
    inbound.push((
        "echo_string",
        format!(
            r#"{{"err":{{"error_code":"InvalidParameter","message":null,"incoming_message":{}}}}}"#,
            serde_json::to_string(&auth_text).unwrap()
        ),
        vec!["err.incoming_message"],
    ));
    inbound.push((
        "free_text",
        format!(
            r#"{{"err":{{"error_code":"PermissionDenied","message":"key {PUBLIC} may not trade {SUB}"}}}}"#
        ),
        vec!["err.message"],
    ));
    let truncated = fixtures()[0].1.trim_end().trim_end_matches('}').to_owned();

    let root = std::env::temp_dir().join(format!("fbc-qfex-signing-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    let key = Arc::new(RedactionKey::new(&[7; 32]).unwrap());
    let mut writer = JournalWriter::create(&root, 0, key).unwrap();
    let now = WallNs(1_760_000_000_000_000_000);
    for frame in &auth_frames {
        let record = Record::Outbound {
            at: MonoNs(0),
            conn: ConnKey { conn: 0, epoch: 1 },
            rpc: None,
            opcode: Opcode::Text,
            frame: frame.clone(),
        };
        writer.append(now, &record).unwrap();
    }
    let mut live = Vec::new();
    for (seq, (name, text, _)) in inbound.iter().enumerate() {
        let frame = RawFrame::Text(text);
        let spans = sub.inbound_spans(Inbound::Frame(frame));
        assert!(!spans.body().is_empty(), "{name}");
        // An answer to an HTTP request is redacted the same way.
        let resp = HttpResponse {
            status: 200,
            headers: &[],
            body: text.as_bytes(),
        };
        let http = sub.inbound_spans(Inbound::Http(HttpTag(1), resp));
        assert_eq!((http.body(), http.headers()), (spans.body(), &[][..]));
        let record = Record::inbound_redacted(stamp(seq as u64), frame, &spans).unwrap();
        writer.append(now, &record).unwrap();
        live.push(record);
    }
    let spans = sub.inbound_spans(Inbound::Frame(RawFrame::Text(&truncated)));
    let truncated_record =
        Record::inbound_redacted(stamp(99), RawFrame::Text(&truncated), &spans).unwrap();
    writer.append(now, &truncated_record).unwrap();
    writer.flush().unwrap();
    drop(writer);

    // On disk: none of the account material in clear, though the frames around it are.
    let written = files(&root);
    assert!(!written.is_empty());
    let all: Vec<u8> = written
        .iter()
        .flat_map(|(_, bytes)| bytes.clone())
        .collect();
    assert!(contains(&all, "\"order_response\"") && contains(&all, "synthetic-cid-1"));
    for hidden in [
        PUBLIC,
        SUB,
        signature.as_str(),
        USER,
        RECORD_IDS[0],
        RECORD_IDS[1],
    ] {
        assert!(!contains(&all, hidden), "{hidden} reached the journal");
    }
    for (path, _) in &written {
        assert!(!path.to_string_lossy().ends_with(".zst"), "{path:?}");
    }

    // Read back: the same frames but for exactly the account values.
    let back: Vec<Record> = JournalReader::open(&root, 0)
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let _ = fs::remove_dir_all(&root);
    assert_eq!(back.len(), 2 + inbound.len() + 1);
    for (frame, record) in auth_frames.iter().zip(&back) {
        let Record::Outbound { frame: read, .. } = record else {
            panic!("{record:?}")
        };
        assert_eq!(read.bytes(), blanked(frame.bytes(), frame.redactions()));
        let mut paths = Vec::new();
        differing(
            &serde_json::from_slice(frame.bytes()).unwrap(),
            &serde_json::from_slice(read.bytes()).unwrap(),
            "",
            &mut paths,
        );
        let mut want = vec!["params.hmac.public_key", "params.hmac.signature"];
        if frame.redactions().len() == 3 {
            want.insert(0, "params.account_id");
        }
        assert_eq!(paths, want);
    }
    for (((name, text, want), record), written) in inbound.iter().zip(&back[2..]).zip(&live) {
        assert_eq!(record, &written.blanked(), "{name}");
        let Record::Inbound { bytes, .. } = record else {
            panic!("{record:?}")
        };
        let replayed: Value = serde_json::from_slice(&bytes.0).unwrap();
        let mut paths = Vec::new();
        differing(
            &serde_json::from_str(text).unwrap(),
            &replayed,
            "",
            &mut paths,
        );
        paths.sort();
        let mut want = want.clone();
        want.sort();
        assert_eq!(paths, want, "{name}");
    }
    // A frame cut short reads back as unparseable as it came, its user id gone.
    let Record::Inbound { bytes, .. } = back.last().unwrap() else {
        panic!()
    };
    assert!(serde_json::from_str::<Value>(&truncated).is_err());
    assert!(serde_json::from_slice::<Value>(&bytes.0).is_err());
    assert!(!contains(&bytes.0, USER));
    assert_eq!(bytes.0[0], b'{');
}

#[test]
fn the_primary_account_s_handle_marks_its_own_material_only() {
    let primary = handle("primary");
    let text = format!(r#"{{"m":"{PUBLIC} {SUB}","user_id":"{USER}"}}"#);
    let spans = primary.inbound_spans(Inbound::Frame(RawFrame::Text(&text)));
    let back = String::from_utf8(blanked(text.as_bytes(), spans.body())).unwrap();
    // The public key's string and the user id go; a subaccount it does not select is no mark.
    assert!(!back.contains(PUBLIC) && !back.contains(USER));
    let mut paths = Vec::new();
    differing(
        &serde_json::from_str(&text).unwrap(),
        &serde_json::from_str(&back).unwrap(),
        "",
        &mut paths,
    );
    assert_eq!(paths, ["m", "user_id"]);
    // A frame with nothing of the account in it has no span.
    let plain = r#"{"subscribed":"order_responses"}"#;
    assert!(
        primary
            .inbound_spans(Inbound::Frame(RawFrame::Text(plain)))
            .is_empty()
    );
}

// ----- Where credentials live -----

/// The crate's source files, by path relative to its `src`.
fn sources(dir: &Path, base: &Path, out: &mut Vec<(String, String)>) {
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            sources(&path, base, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            let name = path
                .strip_prefix(base)
                .unwrap()
                .to_string_lossy()
                .into_owned();
            out.push((name.replace('\\', "/"), fs::read_to_string(&path).unwrap()));
        }
    }
}

#[test]
fn no_credential_is_read_or_held_outside_src_auth_and_src_sign() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut all = Vec::new();
    sources(&src, &src, &mut all);
    let review = |name: &str| name == "sign.rs" || name == "auth.rs" || name.starts_with("auth/");
    // What reading, holding or naming a credential looks like in this crate.
    let forbidden = [
        "expose(",
        "Secret::",
        "Secret(",
        ": Secret",
        "<Secret>",
        "Secrets::",
        "HmacKey",
        "Hmac<",
        "TradeAuth",
        "SECRET_KEY",
        "PUBLIC_KEY",
        "qfex.secret_key",
        "qfex.public_key",
    ];
    let mut outside = 0;
    for (name, text) in &all {
        if review(name) {
            continue;
        }
        outside += 1;
        for (at, line) in text.lines().enumerate() {
            // The factory receives the consumer's credentials and drops them unread.
            let line = line.replace("_creds: Secrets", "");
            for word in forbidden {
                assert!(!line.contains(word), "src/{name}:{} names {word}", at + 1);
            }
            assert!(!line.contains("creds"), "src/{name}:{}: {line}", at + 1);
        }
    }
    assert!(outside >= 5, "{outside} files outside the review paths");
    // And the review paths are where they are read: the scan looks for the right thing.
    let read_in = |name: &str| {
        all.iter()
            .any(|(file, text)| file == name && text.contains(".expose()"))
    };
    assert!(read_in("auth.rs") && read_in("sign.rs"));
}
