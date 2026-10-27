//! FBC-8mzv's done line, the golden half: for every vector the Java FueledByChaiTrading packer
//! and signer wrote (`HibachiSignOracle.java`, through `HibachiPayloadPacker` and
//! `EcdsaHibachiSigner`, on a synthetic key), the Rust packer writes the same bytes from the same
//! decimal inputs and the Rust signer the same 65-byte signature, r, s and v: Java's nonce is RFC
//! 6979 (web3j's `ECKeyPair.sign` over BouncyCastle's `HMacDSAKCalculator`), so equality, not
//! only verification, is asked of every row. A command Java refuses (a scaled value past 63 bits)
//! the Rust packer refuses too. The worked examples of Hibachi's signing docs are reproduced byte
//! for byte from the buffers the docs print, the example's own buffer from max fees 0.00005; the
//! same order with max fees 0.0005 packs its fee as 0xc350; and 2500.01 on ETH's decimals packs
//! to 10737461189, truncated.

mod common;

use std::collections::BTreeMap;

use common::{Row, Vectors, hex, unhex};
use fbc_core::SignError;
use fbc_venue_hibachi::sign::{
    CancelTarget, HibachiSigner, Payload, pack_arm, pack_cancel, pack_cancel_all, pack_order,
};
use k256::ecdsa::{RecoveryId, Signature, VerifyingKey};
use sha2::{Digest, Sha256};

/// The fixed vectors, one of each command, the docs' examples, the Java tests' and the edges:
/// each must be in the file exactly once.
const FIXED: &[&str] = &[
    "limit_buy",
    "limit_sell",
    "market_buy",
    "market_sell",
    "modify",
    "modify_no_price",
    "cancel_order_id",
    "cancel_nonce",
    "cancel_all",
    "arm",
    "docs_example",
    "docs_inputs_fee_0_0005",
    "docs_example_fee_0",
    "docs_cancel",
    "truncation_2500_01",
    "java_test_limit_bid",
    "java_test_market_ask",
    "java_test_cancel_order_id",
    "java_test_cancel_nonce",
    "edge_settlement_above_underlying",
    "edge_fee_zero",
    "edge_fee_eight_places",
    "edge_price_scales_to_zero",
    "edge_price_below_2_63",
    "edge_price_at_2_63",
    "edge_quantity_max",
    "edge_quantity_past_max",
    "edge_price_overflow",
    "edge_contract_id_max",
    "edge_nonce_max",
    "edge_order_id_max",
    "edge_cancel_nonce_zero",
];

/// What the Rust packer writes for `row`, or why it refuses.
fn pack(row: &Row) -> Result<Payload, SignError> {
    match row.get("kind") {
        "place" | "modify" => pack_order(&row.order(&row.spec())),
        "cancel_order_id" => pack_cancel(CancelTarget::OrderId(row.u64("order_id"))),
        "cancel_nonce" => pack_cancel(CancelTarget::PlacementNonce(row.u64("nonce"))),
        "cancel_all" => pack_cancel_all(row.u64("nonce")),
        "arm" => pack_arm(row.u64("nonce")),
        other => panic!("{}: kind {other}", row.name()),
    }
}

/// The signature the row states recovers, with its v, to `public` from the row's digest, and
/// its s is low: Java's signature checked apart from Rust's.
fn recovers(row: &Row, public: &VerifyingKey) {
    let name = row.name();
    let bytes = unhex(row.get("signature"));
    assert_eq!(bytes.len(), 65, "{name}: 65 bytes");
    let sig = Signature::from_slice(&bytes[..64]).unwrap();
    assert!(sig.normalize_s().is_none(), "{name}: s is low");
    assert!(bytes[64] <= 1, "{name}: v is 0 or 1");
    let id = RecoveryId::from_byte(bytes[64]).unwrap();
    let digest = unhex(row.get("sha256"));
    let recovered = VerifyingKey::recover_from_prehash(&digest, &sig, id).unwrap();
    assert_eq!(&recovered, public, "{name}: recovers to the synthetic key");
}

#[test]
fn every_vector_packs_and_signs_to_the_java_oracles_bytes() {
    let vectors = Vectors::read();
    let signer = vectors.signer();
    let mut seen = BTreeMap::<&str, usize>::new();
    let mut kinds = BTreeMap::<&str, usize>::new();
    let (mut random, mut refused) = (0, 0);
    for row in &vectors.rows {
        let name = row.name();
        *seen.entry(name).or_default() += 1;
        if name.starts_with("random_") {
            random += 1;
        }
        let packed = pack(row);
        if !row.signed() {
            refused += 1;
            // Java's toLongTruncating refuses a scaled value past 63 bits; so does Rust.
            let err = packed.expect_err(name);
            assert!(
                matches!(err, SignError::Unsignable(what) if what.ends_with("past 2^63 - 1 when scaled")),
                "{name}: {err:?}"
            );
            continue;
        }
        *kinds.entry(row.get("kind")).or_default() += 1;
        let payload = packed.unwrap_or_else(|e| panic!("{name}: {e:?}"));
        assert_eq!(
            hex(payload.as_bytes()),
            row.get("payload"),
            "{name}: packed bytes"
        );
        let digest = Sha256::digest(payload.as_bytes());
        assert_eq!(hex(&digest), row.get("sha256"), "{name}: digest");
        let sig = signer.sign_payload(&payload).unwrap();
        assert_eq!(sig.to_hex(), row.get("signature"), "{name}: signature");
        if matches!(row.get("kind"), "place" | "modify") {
            let spec = row.spec();
            assert_eq!(signer.sign_order(&row.order(&spec)), Ok(sig), "{name}");
        }
        recovers(row, signer.verifying_key());
        if !row.get("documented").is_empty() {
            assert_eq!(
                hex(payload.as_bytes()),
                row.get("documented"),
                "{name}: the docs' buffer"
            );
        }
    }
    for name in FIXED {
        assert_eq!(seen.get(name), Some(&1), "fixed vector {name}");
    }
    for kind in [
        "place",
        "modify",
        "cancel_order_id",
        "cancel_nonce",
        "cancel_all",
        "arm",
    ] {
        assert!(
            kinds.get(kind).is_some_and(|n| *n >= 20),
            "{kind}: {kinds:?}"
        );
    }
    let header_random: usize = vectors.header["random_intents"].parse().unwrap();
    assert_eq!(random, header_random);
    assert!(random >= 1000, "{random} random vectors");
    assert!(refused >= 3, "{refused} refused vectors");
}

#[test]
fn each_command_has_a_golden_vector_of_its_own_shape() {
    let vectors = Vectors::read();
    let shape = |name: &str, kind: &str, len: usize| {
        let row = vectors.row(name);
        assert_eq!(row.get("kind"), kind, "{name}");
        assert!(row.signed(), "{name}");
        assert_eq!(pack(row).unwrap().as_bytes().len(), len, "{name}");
        row
    };
    // Place: a limit buy and sell (40 bytes, side BID 1 and ASK 0), a market buy and sell (32
    // bytes, no price).
    for (name, len, side) in [
        ("limit_buy", 40, 1u8),
        ("limit_sell", 40, 0),
        ("market_buy", 32, 1),
        ("market_sell", 32, 0),
    ] {
        let row = shape(name, "place", len);
        assert_eq!(
            pack(row).unwrap().as_bytes()[20..24],
            [0, 0, 0, side],
            "{name}: side"
        );
        assert_eq!(row.price().is_some(), len == 40, "{name}");
    }
    // Modify: packed as a place, under its own nonce.
    let modify = shape("modify", "modify", 40);
    assert_ne!(modify.get("nonce"), vectors.row("limit_buy").get("nonce"));
    shape("modify_no_price", "modify", 32);
    // A cancel by order id, by the placement nonce of limit_buy, a cancel-all, and the arm.
    let by_id = shape("cancel_order_id", "cancel_order_id", 8);
    assert_eq!(
        pack(by_id).unwrap().as_bytes(),
        &by_id.u64("order_id").to_be_bytes()
    );
    let by_nonce = shape("cancel_nonce", "cancel_nonce", 8);
    assert_eq!(by_nonce.get("nonce"), vectors.row("limit_buy").get("nonce"));
    for name in ["cancel_all", "arm"] {
        let row = shape(name, name, 8);
        assert_eq!(
            pack(row).unwrap().as_bytes(),
            &row.u64("nonce").to_be_bytes()
        );
    }
}

#[test]
fn the_docs_worked_example_buffer_is_reproduced_byte_for_byte_from_max_fees_0_00005() {
    // https://hibachi-docs.redocly.app/api/signing and the Signing page of
    // https://api-doc.hibachi.xyz/: BTC/USDT-P (contract 2, underlying 10, settlement 6), ASK,
    // nonce 1714701600000000, quantity 1, price 100000, and the 40-byte buffer they print.
    let vectors = Vectors::read();
    let row = vectors.row("docs_example");
    assert_eq!(row.get("max_fees"), "0.00005");
    assert_eq!(
        (
            row.u64("contract_id"),
            row.u64("underlying_decimals"),
            row.u64("settlement_decimals")
        ),
        (2, 10, 6)
    );
    assert_eq!(
        (row.get("side"), row.get("quantity"), row.get("price")),
        ("ASK", "1", "100000")
    );
    assert_eq!(row.u64("nonce"), 1_714_701_600_000_000);
    let documented = unhex(row.get("documented"));
    assert_eq!(documented.len(), 40);
    let spec = row.spec();
    let payload = pack_order(&row.order(&spec)).unwrap();
    assert_eq!(payload.as_bytes(), documented.as_slice());
    // The docs' parts: quantity 10000000000, price 42949672960, fee 5000 (0x1388).
    assert_eq!(payload.as_bytes()[12..20], 10_000_000_000u64.to_be_bytes());
    assert_eq!(payload.as_bytes()[24..32], 42_949_672_960u64.to_be_bytes());
    assert_eq!(payload.as_bytes()[32..40], 5_000u64.to_be_bytes());

    // The Trade folder's example, max fees 0, and both pages' cancel of order
    // 579183763093760000 as 8 bytes.
    for name in ["docs_example_fee_0", "docs_cancel"] {
        let row = vectors.row(name);
        assert_eq!(
            pack(row).unwrap().as_bytes(),
            unhex(row.get("documented")).as_slice(),
            "{name}"
        );
    }
    assert_eq!(
        vectors.row("docs_cancel").u64("order_id"),
        579_183_763_093_760_000
    );
}

#[test]
fn max_fees_0_0005_packs_the_fee_as_0xc350_with_no_10_7_path() {
    // The docs pack their example's max fees "0.0005" as 5000, which is 0.0005 x 10^7; the SDK
    // and Java scale by 10^8, and so does the signer: 0.0005 packs as 50000.
    let vectors = Vectors::read();
    let docs = vectors.row("docs_example");
    let row = vectors.row("docs_inputs_fee_0_0005");
    assert_eq!(row.get("max_fees"), "0.0005");
    for column in [
        "nonce",
        "contract_id",
        "underlying_decimals",
        "settlement_decimals",
        "side",
    ] {
        assert_eq!(row.get(column), docs.get(column), "{column}");
    }
    for column in ["quantity", "price"] {
        assert_eq!(row.get(column), docs.get(column), "{column}");
    }
    let spec = row.spec();
    let payload = pack_order(&row.order(&spec)).unwrap();
    assert_eq!(hex(&payload.as_bytes()[32..]), "000000000000c350");
    assert_eq!(payload.as_bytes()[32..], 50_000u64.to_be_bytes());
    // The rest is the docs' buffer; the fee alone differs from it.
    let documented = unhex(docs.get("documented"));
    assert_eq!(payload.as_bytes()[..32], documented[..32]);
    assert_ne!(payload.as_bytes(), documented.as_slice());
    assert_eq!(hex(payload.as_bytes()), row.get("payload"));
}

#[test]
fn the_truncation_vector_packs_2500_01_on_eth_decimals_as_10737461189() {
    // 2500.01 x 2^32 x 10^(6 - 9) = 10737461189.67296: truncated toward zero, as the SDK's
    // price_to_bytes and Java's scalePrice do. The price lies on its 0.01 tick.
    let vectors = Vectors::read();
    let row = vectors.row("truncation_2500_01");
    assert_eq!(row.get("price"), "2500.01");
    assert_eq!(row.get("tick_size"), "0.01");
    assert_eq!(
        (
            row.u64("underlying_decimals"),
            row.u64("settlement_decimals")
        ),
        (9, 6)
    );
    let spec = row.spec();
    let payload = pack_order(&row.order(&spec)).unwrap();
    assert_eq!(payload.as_bytes()[24..32], 10_737_461_189u64.to_be_bytes());
    // Not exact: 250001 x 2^32 is not a multiple of 10^5.
    assert_ne!((250_001u128 << 32) % 100_000, 0);
    assert_eq!(hex(payload.as_bytes()), row.get("payload"));
}

#[test]
fn the_synthetic_key_is_sha256_of_its_stated_phrase_and_signs_as_javas_does() {
    let vectors = Vectors::read();
    let phrase = &vectors.header["key_phrase"];
    assert!(phrase.contains("synthetic") && phrase.contains("no account"));
    assert_eq!(
        vectors.key(),
        format!("0x{}", hex(&Sha256::digest(phrase.as_bytes())))
    );
    let signer: HibachiSigner = vectors.signer();
    let public = signer.verifying_key().to_encoded_point(false);
    assert_eq!(hex(public.as_bytes()), vectors.header["public_key"]);
    // The oracle ran on an unmodified Java checkout.
    assert_eq!(vectors.header["java_local_changes"], "0");
    assert_eq!(vectors.header["java_commit"].len(), 40);
}
