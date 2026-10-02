use std::hint::black_box;
use std::time::Instant;

use alloy_primitives::{keccak256, Address, B256, U256};
use alloy_sol_types::{eip712_domain, SolStruct};
use num_bigint::BigUint;
use num_traits::{One, Zero};
use rand::RngCore;
use serde::Serialize;
use sha2::{Digest, Sha256};
use starknet_core::types::typed_data::TypedData;
use starknet_core::utils::{cairo_short_string_to_felt, starknet_keccak};
use starknet_crypto::{get_public_key, pedersen_hash, rfc6979_generate_k, sign, verify, Felt};

fn bench<F: FnMut()>(name: &str, iters: usize, mut f: F) {
    for _ in 0..(iters / 10).max(10) {
        f();
    }
    let mut v = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t = Instant::now();
        f();
        v.push(t.elapsed().as_nanos() as f64 / 1000.0);
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mean: f64 = v.iter().sum::<f64>() / v.len() as f64;
    let p = |q: f64| v[((v.len() as f64 - 1.0) * q) as usize];
    println!(
        "{:<58} mean {:>9.2} us  p50 {:>9.2}  p99 {:>9.2}  max {:>9.2}",
        name,
        mean,
        p(0.5),
        p(0.99),
        v[v.len() - 1]
    );
}

// ---------------------------------------------------------------- Paradex (Stark curve, SNIP-12 rev0)

fn hash_on_elements(els: &[Felt]) -> Felt {
    let mut h = Felt::ZERO;
    for e in els {
        h = pedersen_hash(&h, e);
    }
    pedersen_hash(&h, &Felt::from(els.len() as u64))
}

struct ParadexOrderHasher {
    outer_prefix: Felt, // chain state after [PREFIX, domain_hash, account]
    order_prefix: Felt, // chain state after [order_type_hash]
}

impl ParadexOrderHasher {
    fn new(account: Felt, chain_id: Felt) -> Self {
        let domain_type = starknet_keccak(b"StarkNetDomain(name:felt,chainId:felt,version:felt)");
        let order_type = starknet_keccak(
            b"Order(timestamp:felt,market:felt,side:felt,orderType:felt,size:felt,price:felt)",
        );
        let domain_hash = hash_on_elements(&[
            domain_type,
            cairo_short_string_to_felt("Paradex").unwrap(),
            chain_id,
            Felt::ONE,
        ]);
        let mut h = Felt::ZERO;
        for e in [cairo_short_string_to_felt("StarkNet Message").unwrap(), domain_hash, account] {
            h = pedersen_hash(&h, &e);
        }
        Self { outer_prefix: h, order_prefix: pedersen_hash(&Felt::ZERO, &order_type) }
    }

    fn order_hash(&self, ts: u64, market: Felt, side: Felt, otype: Felt, size: Felt, price: Felt) -> Felt {
        let mut h = self.order_prefix;
        for e in [Felt::from(ts), market, side, otype, size, price] {
            h = pedersen_hash(&h, &e);
        }
        let struct_hash = pedersen_hash(&h, &Felt::from(7u64));
        let h2 = pedersen_hash(&self.outer_prefix, &struct_hash);
        pedersen_hash(&h2, &Felt::from(4u64))
    }
}

fn felt_to_big(f: &Felt) -> BigUint {
    BigUint::from_bytes_be(&f.to_bytes_be())
}
fn big_to_felt(b: &BigUint) -> Felt {
    Felt::from_bytes_be_slice(&b.to_bytes_be())
}

struct NonceEntry {
    r: BigUint,
    r_felt: Felt,
    kinv: BigUint,
}

fn paradex() {
    println!("\n=== Paradex: SNIP-12 rev0 typed data (Pedersen) + Stark-curve ECDSA ===");
    let chain_id = Felt::from_dec_str("8458834024819506728615521019831122032732688838300957472069977523540").unwrap();
    let account = Felt::from_hex("0x129f3dc1b8962d8a87abc692424c78fda963ade0e1cd17bf3d1c26f8d41ee7a").unwrap();
    let priv_key = Felt::from_hex("0x0139fe4d6f02e666e86a6f58e65060f115cd3c185bd9e98bd829636931458f79").unwrap();
    let pubkey = get_public_key(&priv_key);

    let ts: u64 = 1_759_400_000_123;
    let market = cairo_short_string_to_felt("BTC-USD-PERP").unwrap();
    let side = Felt::from(1u64);
    let otype = cairo_short_string_to_felt("LIMIT").unwrap();
    let size = Felt::from(1_000_000u64); // 0.01 * 1e8
    let price = Felt::from(6_512_345_000_000u64); // 65123.45 * 1e8

    let hasher = ParadexOrderHasher::new(account, chain_id);
    let fast = hasher.order_hash(ts, market, side, otype, size, price);

    // Oracle: the same JSON the Java ParadexTypedDataSigner builds, hashed by starknet-core.
    let chain_hex = format!("0x{}", hex::encode(chain_id.to_bytes_be()).trim_start_matches('0').to_uppercase());
    let json = format!(
        r#"{{"types":{{"StarkNetDomain":[{{"name":"name","type":"felt"}},{{"name":"chainId","type":"felt"}},{{"name":"version","type":"felt"}}],"Order":[{{"name":"timestamp","type":"felt"}},{{"name":"market","type":"felt"}},{{"name":"side","type":"felt"}},{{"name":"orderType","type":"felt"}},{{"name":"size","type":"felt"}},{{"name":"price","type":"felt"}}]}},"primaryType":"Order","domain":{{"name":"Paradex","chainId":"{chain_hex}","version":"1"}},"message":{{"timestamp":{ts},"market":"BTC-USD-PERP","side":"1","orderType":"LIMIT","size":"1000000","price":"6512345000000"}}}}"#
    );
    let td: TypedData = serde_json::from_str(&json).expect("typed data parse");
    let oracle = td.message_hash(account).expect("oracle hash");
    println!("hand-rolled hash == starknet-core TypedData hash: {}  ({:#x})", fast == oracle, fast);
    assert_eq!(fast, oracle);

    bench("paradex: typed-data hash via starknet-core TypedData (json)", 2000, || {
        let td: TypedData = serde_json::from_str(black_box(&json)).unwrap();
        black_box(td.message_hash(account).unwrap());
    });
    bench("paradex: hand-rolled hash (cached domain/prefix, 9 pedersen)", 5000, || {
        black_box(hasher.order_hash(black_box(ts), market, side, otype, size, price));
    });
    bench("paradex: single pedersen_hash", 20000, || {
        black_box(pedersen_hash(black_box(&market), black_box(&otype)));
    });
    bench("paradex: rfc6979 k + ecdsa sign (hash given)", 3000, || {
        let k = rfc6979_generate_k(&fast, &priv_key, None);
        black_box(sign(&priv_key, black_box(&fast), &k).unwrap());
    });
    bench("paradex: FULL hand-rolled hash + rfc6979 sign", 3000, || {
        let h = hasher.order_hash(black_box(ts), market, side, otype, size, price);
        let k = rfc6979_generate_k(&h, &priv_key, None);
        black_box(sign(&priv_key, &h, &k).unwrap());
    });

    // Offline/online ECDSA: precompute (r, k^-1) off the hot path; online s = k^-1 (z + r d) mod n.
    let n = BigUint::parse_bytes(
        b"3618502788666131213697322783095070105526743751716087489154079457884512865583",
        10,
    )
    .unwrap();
    let two_251 = BigUint::one() << 251;
    let d = felt_to_big(&priv_key);
    let mut rng = rand::rngs::OsRng;
    let t_pool = Instant::now();
    let mut pool: Vec<NonceEntry> = Vec::new();
    while pool.len() < 4000 {
        let mut b = [0u8; 32];
        rng.fill_bytes(&mut b);
        let k = BigUint::from_bytes_be(&b) % &n;
        if k.is_zero() {
            continue;
        }
        let rx = felt_to_big(&get_public_key(&big_to_felt(&k))); // x(kG)
        let r = rx % &n;
        if r.is_zero() || r >= two_251 {
            continue;
        }
        let kinv = k.modpow(&(&n - 2u32), &n);
        pool.push(NonceEntry { r_felt: big_to_felt(&r), r, kinv });
    }
    println!(
        "precomputed {} nonces in {:.1} ms ({:.1} us each, off hot path)",
        pool.len(),
        t_pool.elapsed().as_secs_f64() * 1000.0,
        t_pool.elapsed().as_secs_f64() * 1e6 / pool.len() as f64
    );
    // correctness: verify a handful
    for e in pool.iter().take(20) {
        let z = felt_to_big(&fast);
        let s = (&e.kinv * ((&z + &e.r * &d) % &n)) % &n;
        assert!(verify(&pubkey, &fast, &e.r_felt, &big_to_felt(&s)).unwrap(), "pool sig must verify");
    }
    println!("pool signatures verify with starknet_crypto::verify: true");
    let mut i = 0usize;
    bench("paradex: online sign from nonce pool (num-bigint, hash given)", 3000, || {
        let e = &pool[i % pool.len()];
        i += 1;
        let z = felt_to_big(&fast);
        let s = (&e.kinv * ((&z + &e.r * &d) % &n)) % &n;
        black_box(big_to_felt(&s));
    });
    let mut j = 0usize;
    bench("paradex: FULL hand-rolled hash + pool sign", 3000, || {
        let h = hasher.order_hash(black_box(ts), market, side, otype, size, price);
        let e = &pool[j % pool.len()];
        j += 1;
        let z = felt_to_big(&h);
        let s = (&e.kinv * ((&z + &e.r * &d) % &n)) % &n;
        black_box(big_to_felt(&s));
    });
}

// ---------------------------------------------------------------- Hibachi (packed bytes, SHA-256, secp256k1 + recid)

fn hibachi_pack(nonce: u64, contract_id: u32, qty_scaled: u64, side: u32, price_scaled: Option<u64>, max_fees: u64) -> Vec<u8> {
    let mut v = Vec::with_capacity(40);
    v.extend_from_slice(&nonce.to_be_bytes());
    v.extend_from_slice(&contract_id.to_be_bytes());
    v.extend_from_slice(&qty_scaled.to_be_bytes());
    v.extend_from_slice(&side.to_be_bytes());
    if let Some(p) = price_scaled {
        v.extend_from_slice(&p.to_be_bytes());
    }
    v.extend_from_slice(&max_fees.to_be_bytes());
    v
}

fn hibachi() {
    println!("\n=== Hibachi: 40-byte BE payload, SHA-256, secp256k1 recoverable (r||s||v, v in {{0,1}}) ===");
    // Golden vector from FBC HibachiPayloadPackerTest: qty 0.01 (9dp), price 2500 -> 2500*2^32*10^-3, fees 0.5%
    let price_scaled = (2500u128 * (1u128 << 32) / 1000) as u64;
    let p = hibachi_pack(1, 1, 10_000_000, 1, Some(price_scaled), 50_000_000);
    let expected = "00000000000000010000000100000000009896800000000100000002800000000000000002faf080";
    println!("payload matches FBC golden vector: {}", hex::encode(&p) == expected);
    assert_eq!(hex::encode(&p), expected);

    let key = hex::decode("4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318").unwrap();
    let k256_sk = k256::ecdsa::SigningKey::from_slice(&key).unwrap();
    let secp = secp256k1::Secp256k1::signing_only();
    let sk = secp256k1::SecretKey::from_slice(&key).unwrap();

    bench("hibachi: pack + sha256", 20000, || {
        let p = hibachi_pack(black_box(1_759_400_000_123_456), 1, 10_000_000, 1, Some(price_scaled), 50_000_000);
        black_box(Sha256::digest(&p));
    });
    bench("hibachi: pack + sha256 + k256 sign_prehash_recoverable", 5000, || {
        let p = hibachi_pack(black_box(1_759_400_000_123_456), 1, 10_000_000, 1, Some(price_scaled), 50_000_000);
        let dg = Sha256::digest(&p);
        black_box(k256_sk.sign_prehash_recoverable(&dg).unwrap());
    });
    bench("hibachi: pack + sha256 + libsecp256k1 sign_ecdsa_recoverable", 5000, || {
        let p = hibachi_pack(black_box(1_759_400_000_123_456), 1, 10_000_000, 1, Some(price_scaled), 50_000_000);
        let dg: [u8; 32] = Sha256::digest(&p).into();
        let msg = secp256k1::Message::from_digest(dg);
        black_box(secp.sign_ecdsa_recoverable(&msg, &sk));
    });
}

// ---------------------------------------------------------------- GRVT (EIP-712 with nested struct array)

mod grvt_types {
    alloy_sol_types::sol! {
        struct OrderLeg { uint256 assetID; uint64 contractSize; uint64 limitPrice; bool isBuyingContract; }
        struct Order { uint64 subAccountID; bool isMarket; uint8 timeInForce; bool postOnly; bool reduceOnly; OrderLeg[] legs; uint32 nonce; int64 expiration; }
    }
}

fn grvt() {
    println!("\n=== GRVT: EIP-712 (alloy sol! nested OrderLeg[]) + secp256k1 ===");
    use grvt_types::{Order, OrderLeg};
    let order = Order {
        subAccountID: 8289849667772468u64,
        isMarket: false,
        timeInForce: 1, // GOOD_TILL_TIME
        postOnly: false,
        reduceOnly: false,
        legs: vec![OrderLeg {
            assetID: U256::from(0x030501u64),
            contractSize: 1_013_000_000u64,      // 1.013 * 1e9 (baseDecimals 9)
            limitPrice: 68_900_500_000_000u64,   // 68900.5 * 1e9
            isBuyingContract: false,
        }],
        nonce: 828700936u32,
        expiration: 1730800479321350000i64,
    };
    let domain = eip712_domain! { name: "GRVT Exchange", version: "0", chain_id: 326, };
    let digest: B256 = order.eip712_signing_hash(&domain);

    let key = hex::decode("f7934647276a6e1fa0af3f4467b4b8ddaf45d25a7368fa1a295eef49a446819d").unwrap();
    let secp = secp256k1::Secp256k1::signing_only();
    let sk = secp256k1::SecretKey::from_slice(&key).unwrap();
    let rs = secp.sign_ecdsa_recoverable(&secp256k1::Message::from_digest(digest.0), &sk);
    let (recid, bytes) = rs.serialize_compact();
    let r = hex::encode(&bytes[..32]);
    let s = hex::encode(&bytes[32..]);
    let v = recid.to_i32() + 27;
    let ok = r == "b00512d986a718b15136a8ba23de1c1ec84bbdb9958629cbbe4909bae620bb04"
        && s == "79f706de61c68cc14d7734594b5d8689df2b2a7b25951f9a3f61d799f4327ffc"
        && v == 28;
    println!("alloy sol! EIP-712 signature == GRVT SDK golden vector (r,s,v): {}", ok);
    assert!(ok);

    bench("grvt: eip712_signing_hash (alloy sol!, 1 leg)", 20000, || {
        black_box(black_box(&order).eip712_signing_hash(&domain));
    });
    bench("grvt: hash + libsecp256k1 sign_ecdsa_recoverable", 5000, || {
        let d = black_box(&order).eip712_signing_hash(&domain);
        black_box(secp.sign_ecdsa_recoverable(&secp256k1::Message::from_digest(d.0), &sk));
    });
}

// ---------------------------------------------------------------- Hyperliquid (msgpack action hash + EIP-712 phantom Agent)

#[derive(Serialize)]
struct HlLimit {
    tif: &'static str,
}
#[derive(Serialize)]
struct HlOrderType {
    limit: HlLimit,
}
#[derive(Serialize)]
struct HlOrderWire {
    a: u32,
    b: bool,
    p: String,
    s: String,
    r: bool,
    t: HlOrderType,
}
#[derive(Serialize)]
struct HlOrderAction {
    #[serde(rename = "type")]
    ty: &'static str,
    orders: Vec<HlOrderWire>,
    grouping: &'static str,
}

mod hl_types {
    alloy_sol_types::sol! {
        struct Agent { string source; bytes32 connectionId; }
    }
}

fn hl_action_hash(action: &HlOrderAction, nonce: u64) -> B256 {
    let mut data = rmp_serde::to_vec_named(action).unwrap();
    data.extend_from_slice(&nonce.to_be_bytes());
    data.push(0u8); // no vault
    keccak256(&data)
}

fn hyperliquid() {
    println!("\n=== Hyperliquid: msgpack(action)||nonce||vault -> keccak -> EIP-712 Agent (chainId 1337) ===");
    let phantom_action = HlOrderAction {
        ty: "order",
        orders: vec![HlOrderWire {
            a: 4,
            b: true,
            p: "1670.1".into(),
            s: "0.0147".into(),
            r: false,
            t: HlOrderType { limit: HlLimit { tif: "Ioc" } },
        }],
        grouping: "na",
    };
    let cid = hl_action_hash(&phantom_action, 1677777606040);
    let ok_cid = format!("{:#x}", cid) == "0x0fcbeda5ae3c4950a548021552a4fea2226858c4453571bf3f24ba017eac2908";
    println!("connectionId == python SDK test_phantom_agent_creation_matches_production: {}", ok_cid);
    assert!(ok_cid);

    let action = HlOrderAction {
        ty: "order",
        orders: vec![HlOrderWire {
            a: 1,
            b: true,
            p: "100".into(),
            s: "100".into(),
            r: false,
            t: HlOrderType { limit: HlLimit { tif: "Gtc" } },
        }],
        grouping: "na",
    };
    let domain = eip712_domain! { name: "Exchange", version: "1", chain_id: 1337, verifying_contract: Address::ZERO, };
    let key = hex::decode("0123456789012345678901234567890123456789012345678901234567890123").unwrap();
    let secp = secp256k1::Secp256k1::signing_only();
    let sk = secp256k1::SecretKey::from_slice(&key).unwrap();
    let agent = hl_types::Agent { source: "a".into(), connectionId: hl_action_hash(&action, 0) };
    let digest = agent.eip712_signing_hash(&domain);
    let (recid, bytes) = secp.sign_ecdsa_recoverable(&secp256k1::Message::from_digest(digest.0), &sk).serialize_compact();
    let r = U256::from_be_slice(&bytes[..32]);
    let s = U256::from_be_slice(&bytes[32..]);
    let ok = r == U256::from_str_radix("d65369825a9df5d80099e513cce430311d7d26ddf477f5b3a33d2806b100d78e", 16).unwrap()
        && s == U256::from_str_radix("2b54116ff64054968aa237c20ca9ff68000f977c93289157748a3162b6ea940e", 16).unwrap()
        && recid.to_i32() + 27 == 28;
    println!("order signature == python SDK test_l1_action_signing_order_matches (mainnet): {}", ok);
    assert!(ok);

    bench("hyperliquid: msgpack + keccak (action hash, 1 order)", 20000, || {
        black_box(hl_action_hash(black_box(&action), 1_759_400_000_123));
    });
    bench("hyperliquid: FULL action hash + agent EIP-712 + secp sign", 5000, || {
        let a = hl_types::Agent { source: "a".into(), connectionId: hl_action_hash(black_box(&action), 1_759_400_000_123) };
        let d = a.eip712_signing_hash(&domain);
        black_box(secp.sign_ecdsa_recoverable(&secp256k1::Message::from_digest(d.0), &sk));
    });
}

// ---------------------------------------------------------------- QFEX (HMAC-SHA256 auth only)

fn qfex() {
    use hmac::{Hmac, Mac};
    println!("\n=== QFEX: HMAC-SHA256(secret, nonce:unix_ts) at connection auth only ===");
    bench("qfex: hmac-sha256 auth signature", 20000, || {
        let mut mac = Hmac::<Sha256>::new_from_slice(b"qfex_secret_xxxxxxxxxxxxxxxxxxxxxxxx").unwrap();
        mac.update(black_box(b"c0ffee00112233445566778899aabbcc:1760545414"));
        black_box(mac.finalize().into_bytes());
    });
}

fn main() {
    println!("signbench on {}", std::env::consts::ARCH);
    paradex();
    hibachi();
    grvt();
    hyperliquid();
    qfex();
}
