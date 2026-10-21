#[path = "../src/auth.rs"]
mod auth;
#[path = "../src/sign.rs"]
mod sign;

use fbc_core::{EncodeCtx, MonoNs, NonceBlock, NonceSource, Secret, WallNs};
use serde_json::Value;

#[test]
fn independent_hmac_vector_and_redacted_debug() {
    let vector: Value =
        serde_json::from_str(include_str!("../../../../fixtures/qfex/signing/hmac.json")).unwrap();
    let secret = Secret::new(vector["secret"].as_str().unwrap().to_owned());
    let nonce = [
        vector["nonce"][0].as_u64().unwrap(),
        vector["nonce"][1].as_u64().unwrap(),
    ];
    let seconds = vector["unix_ts"].as_u64().unwrap();
    let signed = sign::sign(&secret, nonce, WallNs((seconds * 1_000_000_000) as i64)).unwrap();
    assert_eq!(signed.nonce(), vector["nonce_hex"].as_str().unwrap());
    assert_eq!(signed.unix_ts(), seconds);
    assert_eq!(signed.signature(), vector["signature"].as_str().unwrap());
    let debug = format!("{signed:?}");
    assert!(!debug.contains(signed.signature()));
    assert!(!debug.contains(signed.nonce()));
    assert!(!debug.contains(secret.expose()));
}

#[test]
fn timestamp_floor_and_negative_time_refused() {
    let secret = Secret::new("synthetic-qfex-secret".to_owned());
    let a = sign::sign(&secret, [7, 8], WallNs(1_760_545_414_999_999_999)).unwrap();
    assert_eq!(a.unix_ts(), 1_760_545_414);
    assert!(sign::sign(&secret, [7, 8], WallNs(-1)).is_err());
}

#[test]
fn auth_frame_is_exact_and_redacts_both_account_key_and_signature() {
    let public = Secret::new("qfex_pub_synthetic_1".to_owned());
    let secret = Secret::new("synthetic-qfex-secret".to_owned());
    let ctx = EncodeCtx {
        wall: WallNs(1_760_545_414_000_000_000),
        mono: MonoNs(0),
        nonces: NonceBlock::new(vec![0xc0fe, 0xbabe]),
    };
    let frame = auth::frame(&public, &secret, &ctx).unwrap();
    let parsed: Value = serde_json::from_slice(frame.bytes()).unwrap();
    assert_eq!(parsed["type"], "auth");
    assert_eq!(parsed["params"]["hmac"]["public_key"], public.expose());
    assert_eq!(
        parsed["params"]["hmac"]["nonce"],
        "000000000000c0fe000000000000babe"
    );
    assert_eq!(parsed["params"]["hmac"]["unix_ts"], 1_760_545_414);
    let vector: Value =
        serde_json::from_str(include_str!("../../../../fixtures/qfex/signing/hmac.json")).unwrap();
    assert_eq!(parsed["params"]["hmac"]["signature"], vector["signature"]);
    assert_eq!(frame.redactions().len(), 2);
    let debug = format!("{frame:?}");
    assert!(!debug.contains(public.expose()));
    assert!(!debug.contains(parsed["params"]["hmac"]["signature"].as_str().unwrap()));
    assert!(!debug.contains(secret.expose()));
    assert!(!auth::valid_public_key("qfex_pub_foo\"}"));
}

#[test]
fn opening_url_redacts_key_and_mismatch_is_refused() {
    let public = Secret::new("qfex_pub_synthetic_1".to_owned());
    let url = auth::opening_url("wss://trade.qfex.io/", public.expose()).unwrap();
    assert_eq!(
        url.as_str(),
        "wss://trade.qfex.io/?api_key=qfex_pub_synthetic_1"
    );
    assert_eq!(url.redactions().len(), 1);
    let span = &url.redactions()[0];
    assert_eq!(
        &url.as_str()[span.start as usize..span.end as usize],
        public.expose()
    );
    assert!(!format!("{url:?}").contains(public.expose()));
    assert!(auth::matching_public_key(public.expose(), &public).is_ok());
    assert!(auth::matching_public_key("qfex_pub_someone_else", &public).is_err());
    assert!(auth::opening_url("wss://trade.qfex.io/", "bad/key").is_err());
}

struct FixedEntropy {
    values: Vec<[u8; 8]>,
}

impl sign::Entropy for FixedEntropy {
    fn fill(&mut self, bytes: &mut [u8]) -> Result<(), ()> {
        let next = self.values.pop().ok_or(())?;
        bytes.copy_from_slice(&next);
        Ok(())
    }
}

#[test]
fn random_nonce_source_retries_collision_and_fails_closed() {
    let mut production_source = sign::QfexNonces::os();
    assert_eq!(production_source.reserve(1).len(), 1);
    let mut source = sign::QfexNonces::with_entropy(FixedEntropy {
        values: vec![
            3u64.to_be_bytes(),
            2u64.to_be_bytes(),
            1u64.to_be_bytes(),
            1u64.to_be_bytes(),
        ],
    });
    assert_eq!(source.reserve(3).as_slice(), &[1, 2, 3]);
    assert!(source.reserve(1).is_empty());

    let mut exhausted = sign::QfexNonces::with_entropy(FixedEntropy {
        values: vec![7u64.to_be_bytes(); 17],
    });
    assert_eq!(exhausted.reserve(1).as_slice(), &[7]);
    assert!(exhausted.reserve(1).is_empty());
}

#[test]
fn auth_frame_requires_nonce_and_safe_public_key() {
    let public = Secret::new("qfex_pub_synthetic_1".to_owned());
    let secret = Secret::new("synthetic-qfex-secret".to_owned());
    let no_nonce = EncodeCtx {
        wall: WallNs(1_760_545_414_000_000_000),
        mono: MonoNs(0),
        nonces: NonceBlock::EMPTY,
    };
    assert!(auth::frame(&public, &secret, &no_nonce).is_err());
    let one_nonce = EncodeCtx {
        nonces: NonceBlock::new(vec![7]),
        ..no_nonce.clone()
    };
    assert!(auth::frame(&public, &secret, &one_nonce).is_err());
    let bad_key = Secret::new("bad/key".to_owned());
    assert!(auth::frame(&bad_key, &secret, &no_nonce).is_err());
}
