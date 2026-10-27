//! FBC-8mzv's hand-off (decision 0043): the venue crate's `auth` takes the signing key from the
//! consumer's `Secrets`, without taking it out of them, refuses a missing or invalid key with a
//! typed error naming the key and never its value, and hands over an initialized signer that
//! signs as the Java oracle does. The key is the synthetic one of `fixtures/hibachi/signing`.

mod common;

use common::Vectors;
use fbc_core::{ConfigError, Secret, Secrets, VenueError};
use fbc_venue_hibachi::auth::{SIGNING_KEY, SIGNING_KEY_FIELD, order_signer};

fn creds(key: &str) -> Secrets {
    let mut creds = Secrets::new();
    creds.insert(SIGNING_KEY, Secret::new(key.to_owned()));
    creds
}

/// The refusal of `key`, checked to name the credential key and never show the value.
fn refused(key: &str) -> &'static str {
    let err = order_signer(&creds(key)).map(|_| ()).unwrap_err();
    let shown = format!("{err} {err:?}");
    let digits = key.trim_start_matches("0x");
    if !digits.is_empty() {
        assert!(!shown.contains(digits), "{shown}");
        assert!(
            !shown
                .to_ascii_lowercase()
                .contains(&digits.to_ascii_lowercase())
        );
    }
    match err {
        VenueError::Config(ConfigError::Invalid { key: k, reason }) if k == SIGNING_KEY => reason,
        other => panic!("{other:?}"),
    }
}

#[test]
fn the_signer_comes_from_secrets_initialized_and_signing_as_java_does() {
    let vectors = Vectors::read();
    let creds = creds(vectors.key());
    let signer = order_signer(&creds).unwrap();
    // Read, not taken: the key is still there for whatever else needs the credentials.
    assert!(creds.get(SIGNING_KEY).is_some());
    let row = vectors.row("docs_example");
    let spec = row.spec();
    let sig = signer.sign_order(&row.order(&spec)).unwrap();
    assert_eq!(sig.to_hex(), row.get("signature"));
    assert_eq!(signer.verifying_key(), vectors.signer().verifying_key());
    // Its Debug shows that a key is there, never the key.
    assert_eq!(format!("{signer:?}"), "HibachiSigner { key: EcdsaKey(..) }");
    assert_eq!(SIGNING_KEY_FIELD.key, SIGNING_KEY);
    assert_eq!(SIGNING_KEY, "hibachi.private.key");
}

#[test]
fn a_missing_or_invalid_key_is_refused_by_name_never_by_value() {
    let missing = order_signer(&Secrets::new()).map(|_| ()).unwrap_err();
    assert_eq!(
        missing,
        VenueError::Config(ConfigError::Missing(SIGNING_KEY))
    );

    let vectors = Vectors::read();
    let key = vectors.key();
    let digits = &key[2..];
    let not_hex = "not 0x and 64 hex digits";
    // Without 0x: the SDK reads such a key as an HMAC secret; there is no HMAC path.
    assert_eq!(refused(digits), not_hex);
    assert_eq!(refused(&format!("0X{digits}")), not_hex);
    assert_eq!(refused(&key[..65]), not_hex, "cut short");
    assert_eq!(refused(&format!("{key}0")), not_hex, "a digit too many");
    assert_eq!(refused(&format!("0x{}g", &digits[1..])), not_hex);
    assert_eq!(refused(&format!(" {key}")), not_hex);
    assert_eq!(refused(""), not_hex);
    let out_of_range = "not a secp256k1 key in range";
    assert_eq!(refused(&format!("0x{}", "0".repeat(64))), out_of_range);
    assert_eq!(refused(&format!("0x{}", "f".repeat(64))), out_of_range);
}
