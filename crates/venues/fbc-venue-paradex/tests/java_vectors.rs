//! FBC-6 done line, first half: for every vector the Java FueledByChaiTrading signer wrote
//! (`ParadexHashOracle.java`, through `ParadexTypedDataSigner` and `ParadexOrder`'s own x1e8
//! scaling), the Rust signer computes the same SNIP-12 revision 0 message hash and the same
//! Stark-curve signature (r, s). The decimal size and price go in as the Java side got them,
//! so the x1e8 truncation is checked too, on every random intent with more than 8 decimals.

mod common;

use common::{Row, Vectors};
use fbc_core::Side;
use fbc_venue_paradex::sign::{
    Felt, OrderMessage, ParadexOrderType, RequestMessage, ScaleError, scale_e8,
};
use rust_decimal::Decimal;

fn decimal(text: &str) -> Decimal {
    Decimal::from_str_exact(text).unwrap_or_else(|e| panic!("{text}: {e}"))
}

fn order_message(row: &Row) -> OrderMessage<'_> {
    let price = match row.get("price") {
        "" => Decimal::ZERO, // a market order: Java signs price 0
        text => decimal(text),
    };
    OrderMessage {
        timestamp_ms: row.u64("timestamp"),
        market: row.get("market"),
        side: match row.get("side") {
            "BUY" => Side::Buy,
            "SELL" => Side::Sell,
            other => panic!("side {other}"),
        },
        order_type: match row.get("order_type") {
            "LIMIT" => ParadexOrderType::Limit,
            "MARKET" => ParadexOrderType::Market,
            other => panic!("order type {other}"),
        },
        size: decimal(row.get("size")),
        price,
    }
}

/// The x1e8 scaling agrees with Java's `scaleByPowerOfTen(8).toBigInteger()`.
fn check_scaling(row: &Row, msg: &OrderMessage<'_>) {
    let name = row.get("name");
    assert_eq!(
        scale_e8(msg.size).map(|v| v.to_string()),
        Ok::<_, ScaleError>(row.get("chain_size").to_owned()),
        "{name}: size scaling"
    );
    assert_eq!(
        scale_e8(msg.price).map(|v| v.to_string()),
        Ok(row.get("chain_price").to_owned()),
        "{name}: price scaling"
    );
}

#[test]
fn rust_hash_and_signature_equal_java_for_every_vector() {
    let vectors = Vectors::read();
    let signer = vectors.signer();
    let public = signer.public_key();
    let mut seen = std::collections::BTreeMap::<&str, usize>::new();
    let mut more_than_eight_places = 0;
    for row in &vectors.rows {
        let name = row.get("name");
        let kind = row.get("kind");
        let (hash, sig) = match kind {
            "order" | "modify" => {
                let msg = order_message(row);
                check_scaling(row, &msg);
                if msg.size.scale() > 8 || msg.price.scale() > 8 {
                    more_than_eight_places += 1;
                }
                if kind == "order" {
                    (
                        signer.order_hash(&msg).unwrap(),
                        signer.sign_order(&msg).unwrap(),
                    )
                } else {
                    let id = row.get("order_id");
                    (
                        signer.modify_hash(&msg, id).unwrap(),
                        signer.sign_modify(&msg, id).unwrap(),
                    )
                }
            }
            "request" => {
                let msg = RequestMessage {
                    method: row.get("method"),
                    path: row.get("path"),
                    body: row.get("body"),
                    timestamp: row.u64("timestamp"),
                    expiration: row.u64("expiration"),
                };
                (
                    signer.request_hash(&msg).unwrap(),
                    signer.sign_request(&msg).unwrap(),
                )
            }
            other => panic!("{name}: kind {other}"),
        };
        assert_eq!(hash, row.felt("hash"), "{name}: message hash");
        assert_eq!(sig.r, row.felt("r"), "{name}: signature r");
        assert_eq!(sig.s, row.felt("s"), "{name}: signature s");
        assert!(starknet_crypto::verify(&public, &hash, &sig.r, &sig.s).unwrap());
        let label = if name.starts_with("random_") {
            "random"
        } else {
            name
        };
        *seen.entry(label).or_default() += 1;
    }
    for name in [
        "buy_limit",
        "sell_limit",
        "market_buy",
        "market_sell",
        "modify",
        "auth_request",
    ] {
        assert_eq!(seen.get(name), Some(&1), "the file has the {name} vector");
    }
    assert!(seen["random"] >= 1_000, "at least 1,000 random intents");
    assert!(
        more_than_eight_places >= 1_000,
        "random intents carry more than 8 decimals"
    );
}

#[test]
fn the_auth_request_is_post_v1_auth_with_an_empty_body() {
    let vectors = Vectors::read();
    let signer = vectors.signer();
    let row = vectors.row("auth_request");
    let sig = signer
        .sign_auth_request(row.u64("timestamp"), row.u64("expiration"))
        .unwrap();
    assert_eq!((sig.r, sig.s), (row.felt("r"), row.felt("s")));
}

#[test]
fn the_vectors_name_the_java_signer_and_the_synthetic_inputs() {
    let vectors = Vectors::read();
    assert_eq!(vectors.header["seed"], "20261003");
    assert!(vectors.header["random_intents"].parse::<usize>().unwrap() >= 1_000);
    // Truncation, not rounding: 0.000000009 signs as 0 and 1.999... never rounds up.
    let row = vectors.row("edge_sub_unit");
    assert_eq!(row.get("chain_size"), "0");
    assert_eq!(row.get("chain_price"), "1");
    assert_ne!(vectors.account(), Felt::ZERO);
}
