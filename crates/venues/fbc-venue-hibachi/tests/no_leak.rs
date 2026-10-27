//! FBC-8mzv: with the synthetic key of `fixtures/hibachi/signing`, neither the key nor a signed
//! payload (the packed bytes, or the signature over them) shows in any log line or error. A
//! logger takes every record any crate emits, at TRACE, while every command is signed and every
//! refusal made, from the credentials through `auth` to the signature; and the values a careless
//! caller might log (the credentials, the key, the signer, a payload, a `HibachiSig` and every
//! error, by `Display` and `Debug`) are logged on purpose, so their printed forms are searched
//! too. fbc-core's `Sig`, which `HibachiSig::to_sig` gives the codec for the wire, is the
//! signature as the request carries it and the journal keeps it (decision 0006); its `Debug`
//! prints it, so it is not logged on purpose, but its printed forms are searched for in every
//! line all the same. The search is shown to find each form it looks for before it is trusted.
//! The test's own lines go at DEBUG and above: a workspace build that links fbc-runtime
//! compiles TRACE out of the log facade (decision 0079).

mod common;

use std::sync::{Mutex, Once};
use std::thread::ThreadId;

use common::{Vectors, hex, spec, venue_id};
use fbc_core::{CancelRef, CancelWire, Secret, Secrets, Side, SignError, WallNs};
use fbc_venue_hibachi::auth::{SIGNING_KEY, order_signer};
use fbc_venue_hibachi::sign::{
    CancelTarget, EcdsaKey, JsonOnly, OrderMessage, pack_arm, pack_cancel, pack_cancel_all,
    pack_order,
};
use log::{LevelFilter, Log, Metadata, Record};

/// Every record, with the thread that logged it.
static LINES: Mutex<Vec<(ThreadId, String)>> = Mutex::new(Vec::new());

struct Capture;

impl Log for Capture {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &Record<'_>) {
        let line = format!("{} {} {}", record.level(), record.target(), record.args());
        let mut lines = LINES.lock().unwrap_or_else(|e| e.into_inner());
        lines.push((std::thread::current().id(), line));
    }

    fn flush(&self) {}
}

static CAPTURE: Capture = Capture;

/// Installs the capture once for this test binary, at TRACE: every level any crate asks for.
fn install() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        log::set_logger(&CAPTURE).expect("no other logger in this test binary");
        log::set_max_level(LevelFilter::Trace);
    });
    assert_eq!(log::max_level(), LevelFilter::Trace);
}

/// Every line the calling thread logged.
fn lines_of_this_thread() -> Vec<String> {
    let me = std::thread::current().id();
    let lines = LINES.lock().unwrap_or_else(|e| e.into_inner());
    lines
        .iter()
        .filter(|(thread, _)| *thread == me)
        .map(|(_, line)| line.clone())
        .collect()
}

/// What the search looks for, lowercase: the key's digits (which its `0x` form contains), the
/// hex of the key text's ASCII bytes (as a frame dump shows it), its bytes as a Rust byte array
/// prints them, and each payload and signature as hex and as a byte array; and each signature's
/// hex text as its ASCII bytes, in hex (a frame dump) and as a byte array (fbc-core's `Sig`
/// prints so; Reviewer RULES-1 on FBC-8mzv).
fn needles(key: &str, payloads: &[Vec<u8>], signatures: &[Vec<u8>]) -> Vec<String> {
    let digits = key.trim_start_matches("0x").to_ascii_lowercase();
    let key_bytes = common::unhex(&digits);
    let mut needles = vec![
        digits.clone(),
        hex(key.as_bytes()),
        hex(digits.as_bytes()),
        format!("{key_bytes:?}"),
    ];
    for bytes in payloads.iter().chain(signatures) {
        needles.push(hex(bytes));
        needles.push(format!("{bytes:?}"));
    }
    for bytes in signatures {
        let text = hex(bytes);
        needles.push(hex(text.as_bytes()));
        needles.push(format!("{:?}", text.as_bytes()));
    }
    needles
}

fn absent(text: &str, needles: &[String], what: &str) {
    let lower = text.to_ascii_lowercase();
    for needle in needles {
        assert!(
            !lower.contains(needle),
            "{what} shows the key or a signed payload"
        );
    }
}

#[test]
fn no_log_line_or_error_shows_the_key_or_a_signed_payload() {
    install();
    let vectors = Vectors::read();
    let key = vectors.key().to_owned();
    let mut creds = Secrets::new();
    creds.insert(SIGNING_KEY, Secret::new(key.clone()));
    let signer = order_signer(&creds).unwrap();
    let parsed = EcdsaKey::from_hex(&key).unwrap();

    // Sign every command of every vector.
    let (mut payloads, mut signatures) = (Vec::new(), Vec::new());
    for row in vectors.rows.iter().filter(|r| r.signed()) {
        let spec = match row.get("kind") {
            "place" | "modify" => Some(row.spec()),
            _ => None,
        };
        let payload = match row.get("kind") {
            "place" | "modify" => pack_order(&row.order(spec.as_ref().unwrap())),
            "cancel_order_id" => pack_cancel(CancelTarget::OrderId(row.u64("order_id"))),
            "cancel_nonce" => pack_cancel(CancelTarget::PlacementNonce(row.u64("nonce"))),
            "cancel_all" => pack_cancel_all(row.u64("nonce")),
            _ => pack_arm(row.u64("nonce")),
        }
        .unwrap();
        let sig = signer.sign_payload(&payload).unwrap();
        log::debug!("payload {payload:?}, signature {sig:?}");
        payloads.push(payload.as_bytes().to_vec());
        signatures.push(sig.as_bytes().to_vec());
    }

    // Every refusal: of the key, of the credentials, and of each command the payload cannot
    // carry.
    let mut errors: Vec<String> = Vec::new();
    let mut shown = |e: &dyn std::fmt::Debug, d: &dyn std::fmt::Display| {
        let text = format!("{d} {e:?}");
        log::error!("{text}");
        errors.push(text);
    };
    for bad in [&key[2..], &key[..65], "0x", &format!("{key}0")] {
        let err = EcdsaKey::from_hex(bad).unwrap_err();
        shown(&err, &err);
        let mut bad_creds = Secrets::new();
        bad_creds.insert(SIGNING_KEY, Secret::new(bad.to_owned()));
        let err = order_signer(&bad_creds).unwrap_err();
        shown(&err, &err);
    }
    let missing = order_signer(&Secrets::new()).unwrap_err();
    shown(&missing, &missing);
    let eth = spec(1, 9, 6, "0.01", "0.001");
    let refusals: Vec<SignError> = vec![
        pack_order(&OrderMessage {
            spec: &eth,
            nonce: 1,
            side: Side::Buy,
            quantity: common::dec("0.0105"),
            price: Some(common::dec("2500.015")),
            max_fees: common::dec("0.000000001"),
            json_only: JsonOnly::NONE,
        })
        .unwrap_err(),
        pack_order(&OrderMessage {
            spec: &eth,
            nonce: 1,
            side: Side::Buy,
            quantity: common::dec("1"),
            price: None,
            max_fees: common::dec("0.000000001"),
            json_only: JsonOnly::NONE,
        })
        .unwrap_err(),
        pack_cancel_all(u64::MAX).unwrap_err(),
        signer
            .sign_cancel(&CancelWire {
                spec: &eth,
                target: CancelRef::Client("hello"),
                side: Side::Buy,
                wall: WallNs(0),
                nonce: Some(1),
            })
            .unwrap_err(),
        signer
            .sign_cancel(&CancelWire {
                spec: &eth,
                target: CancelRef::Venue(&venue_id("x")),
                side: Side::Buy,
                wall: WallNs(0),
                nonce: Some(1),
            })
            .unwrap_err(),
    ];
    for err in &refusals {
        shown(err, err);
    }

    // What a careless caller might log.
    log::info!("credentials {creds:?} / {creds}");
    log::info!("key {parsed:?}, signer {signer:?}");
    log::debug!(
        "secret {:?} / {}",
        creds.get(SIGNING_KEY).unwrap(),
        creds.get(SIGNING_KEY).unwrap()
    );

    let needles = needles(&key, &payloads, &signatures);
    let lines = lines_of_this_thread();
    assert!(
        lines.len() > payloads.len(),
        "the capture saw the lines logged"
    );
    for line in &lines {
        absent(line, &needles, "a log line");
    }
    for text in &errors {
        absent(text, &needles, "an error");
    }
}

#[test]
fn the_search_finds_every_form_it_looks_for() {
    install();
    let vectors = Vectors::read();
    let key = vectors.key();
    let row = vectors.row("limit_buy");
    let spec = row.spec();
    let payload = pack_order(&row.order(&spec)).unwrap();
    let sig = vectors.signer().sign_payload(&payload).unwrap();
    let needles = needles(
        key,
        &[payload.as_bytes().to_vec()],
        &[sig.as_bytes().to_vec()],
    );
    let digits = &key[2..];
    let forms = [
        key.to_owned(),
        digits.to_ascii_uppercase(),
        hex(key.as_bytes()),
        format!("{:?}", common::unhex(digits)),
        hex(payload.as_bytes()),
        format!("{:?}", payload.as_bytes()),
        sig.to_hex(),
        format!("{:?}", sig.as_bytes()),
        format!("{:?}", sig.to_sig()),
        hex(sig.to_sig().as_bytes()),
    ];
    for form in forms {
        log::warn!("leaked {form}");
        let line = lines_of_this_thread().pop().unwrap();
        let lower = line.to_ascii_lowercase();
        assert!(
            needles.iter().any(|n| lower.contains(n)),
            "the search misses a form"
        );
    }
}
