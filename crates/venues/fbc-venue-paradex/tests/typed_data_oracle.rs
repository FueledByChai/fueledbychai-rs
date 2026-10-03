//! FBC-6 done line, second half: the hand-written hash (cached prefixes, Pedersen chains) agrees
//! with starknet-core's general `TypedData` message hash on 10,000 generated orders, given the
//! message JSON the Java `ParadexTypedDataSigner` builds (its type lists, its message shape).
//!
//! One substitution: starknet-core 0.16 accepts only its own revision 0 domain type,
//! `StarkNetDomain(name, version, chainId)`, and refuses Paradex's
//! `StarkNetDomain(name, chainId, version)` ("invalid domain type definition for revision 0").
//! So the typed data here declares starknet-core's domain, and the Rust hasher is given that
//! domain's separator (`ParadexHasher::with_domain_hash`); everything after the domain - the
//! prefix, the account, the type hashes, the felt encoding of every field, the x1e8 scaling and
//! the Pedersen chains - is compared. Paradex's own domain order is held by the Java vectors
//! (tests/java_vectors.rs), and `paradex_domain_is_not_starknet_cores` pins the difference.
//! Orders, modifies and requests are generated from a fixed seed.

mod common;

use common::Vectors;
use fbc_core::Side;
use fbc_venue_paradex::sign::{
    Felt, OrderMessage, ParadexHasher, ParadexOrderType, RequestMessage, scale_e8,
};
use rust_decimal::Decimal;
use starknet_core::types::typed_data::TypedData;

/// splitmix64: a fixed, dependency-free stream of test inputs.
struct Gen(u64);

impl Gen {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    /// A decimal with up to `int_digits` integer digits and `places` decimal places.
    fn decimal(&mut self, int_digits: u32, places: u32) -> Decimal {
        let int = if int_digits == 0 {
            0
        } else {
            self.below(10u64.pow(int_digits))
        };
        let frac = self.below(10u64.pow(places.min(18)));
        let mantissa = i128::from(int) * 10i128.pow(places) + i128::from(frac);
        Decimal::from_i128_with_scale(mantissa, places)
    }
}

const MARKETS: [&str; 6] = [
    "BTC-USD-PERP",
    "ETH-USD-PERP",
    "SOL-USD-PERP",
    "kBONK-USD-PERP",
    "HYPE-USD-PERP",
    "PAXG-USD-PERP",
];

/// The typed data's types and domain, with starknet-core's revision 0 domain field order.
fn domain_and_types(chain_hex: &str, primary: &str, fields: &[&str]) -> String {
    let list: Vec<String> = fields
        .iter()
        .map(|f| format!(r#"{{"name":"{f}","type":"felt"}}"#))
        .collect();
    format!(
        r#""types":{{"StarkNetDomain":[{{"name":"name","type":"felt"}},{{"name":"version","type":"felt"}},{{"name":"chainId","type":"felt"}}],"{primary}":[{}]}},"primaryType":"{primary}","domain":{{"name":"Paradex","version":"1","chainId":"{chain_hex}"}}"#,
        list.join(",")
    )
}

fn typed_data(json: &str) -> TypedData {
    serde_json::from_str(json).unwrap_or_else(|e| panic!("{e}: {json}"))
}

/// starknet-core's message hash of `json`, and the Rust hasher under the same domain separator.
fn oracle(json: &str, account: Felt) -> (Felt, ParadexHasher) {
    let td = typed_data(json);
    let hasher = ParadexHasher::with_domain_hash(account, td.encoder().domain().encoded_hash());
    (td.message_hash(account).unwrap(), hasher)
}

fn side_felt(side: Side) -> &'static str {
    match side {
        Side::Buy => "1",
        Side::Sell => "2",
    }
}

#[test]
fn hand_written_hash_agrees_with_starknet_core_typed_data_on_10000_orders() {
    let vectors = Vectors::read();
    let account = vectors.account();
    let chain_hex = &vectors.header["chain_id"];
    let order_fields = ["timestamp", "market", "side", "orderType", "size", "price"];
    let modify_fields = [
        "timestamp",
        "market",
        "side",
        "orderType",
        "size",
        "price",
        "id",
    ];
    let order_head = domain_and_types(chain_hex, "Order", &order_fields);
    let modify_head = domain_and_types(chain_hex, "ModifyOrder", &modify_fields);
    let mut g = Gen(0x0F_BC_00_06);
    let (mut orders, mut modifies) = (0, 0);
    for _ in 0..10_000 {
        let order_type = if g.below(4) == 0 {
            ParadexOrderType::Market
        } else {
            ParadexOrderType::Limit
        };
        let int_digits = u32::try_from(g.below(7)).unwrap();
        let size_places = u32::try_from(g.below(19)).unwrap();
        let price_places = u32::try_from(g.below(19)).unwrap();
        let msg = OrderMessage {
            timestamp_ms: 1_700_000_000_000 + g.below(200_000_000_000),
            market: MARKETS[usize::try_from(g.below(6)).unwrap()],
            side: if g.below(2) == 0 {
                Side::Buy
            } else {
                Side::Sell
            },
            order_type,
            size: g.decimal(int_digits, size_places),
            price: match order_type {
                ParadexOrderType::Market => Decimal::ZERO,
                ParadexOrderType::Limit => g.decimal(int_digits + 1, price_places),
            },
        };
        let message = format!(
            r#""timestamp":{},"market":"{}","side":"{}","orderType":"{}","size":"{}","price":"{}""#,
            msg.timestamp_ms,
            msg.market,
            side_felt(msg.side),
            msg.order_type.wire(),
            scale_e8(msg.size).unwrap(),
            scale_e8(msg.price).unwrap(),
        );
        if g.below(5) == 0 {
            let id = (u128::from(g.next()) * 1_000_000_007 + u128::from(g.next())).to_string();
            let json = format!(r#"{{{modify_head},"message":{{{message},"id":"{id}"}}}}"#);
            let (expected, hasher) = oracle(&json, account);
            assert_eq!(hasher.modify(&msg, &id), Ok(expected), "{json}");
            modifies += 1;
        } else {
            let json = format!(r#"{{{order_head},"message":{{{message}}}}}"#);
            let (expected, hasher) = oracle(&json, account);
            assert_eq!(hasher.order(&msg), Ok(expected), "{json}");
            orders += 1;
        }
    }
    assert_eq!(orders + modifies, 10_000);
    assert!(orders > 7_000 && modifies > 1_000);
}

#[test]
fn request_hash_agrees_with_starknet_core_typed_data() {
    let vectors = Vectors::read();
    let account = vectors.account();
    let head = domain_and_types(
        &vectors.header["chain_id"],
        "Request",
        &["method", "path", "body", "timestamp", "expiration"],
    );
    let mut g = Gen(6);
    for (method, path, body) in [
        ("POST", "/v1/auth", ""),
        ("GET", "/v1/orders", ""),
        ("POST", "/v1/onboarding", "x"),
    ] {
        let timestamp = 1_759_400_000 + g.below(1_000_000);
        let expiration = timestamp + 3_600;
        let msg = RequestMessage {
            method,
            path,
            body,
            timestamp,
            expiration,
        };
        let json = format!(
            r#"{{{head},"message":{{"method":"{method}","path":"{path}","body":"{body}","timestamp":{timestamp},"expiration":{expiration}}}}}"#
        );
        let (expected, hasher) = oracle(&json, account);
        assert_eq!(hasher.request(&msg), Ok(expected), "{json}");
    }
}

/// The one part starknet-core cannot check: Paradex declares its domain fields name, chainId,
/// version, and the separator differs from starknet-core's name, version, chainId. The Java
/// vectors hold the Paradex order; this pins that the two really differ, so the substitution
/// above is visible rather than silently equal.
#[test]
fn paradex_domain_is_not_starknet_cores() {
    let vectors = Vectors::read();
    let chain_hex = &vectors.header["chain_id"];
    let json = format!(
        r#"{{{},"message":{{"method":"POST","path":"/v1/auth","body":"","timestamp":1,"expiration":2}}}}"#,
        domain_and_types(
            chain_hex,
            "Request",
            &["method", "path", "body", "timestamp", "expiration"]
        )
    );
    let core = typed_data(&json).encoder().domain().encoded_hash();
    assert_ne!(ParadexHasher::domain_hash(vectors.chain_id()), core);
    let paradex_order = json.replace(
        r#"{"name":"version","type":"felt"},{"name":"chainId","type":"felt"}"#,
        r#"{"name":"chainId","type":"felt"},{"name":"version","type":"felt"}"#,
    );
    let refused = serde_json::from_str::<TypedData>(&paradex_order).unwrap_err();
    assert!(
        refused
            .to_string()
            .contains("invalid domain type definition for revision 0")
    );
}
