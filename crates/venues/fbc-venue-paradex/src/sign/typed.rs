//! The SNIP-12 revision 0 typed-data hash (Pedersen) of the three messages Paradex signs:
//! `Order`, `ModifyOrder` (the order plus the venue's order id) and the auth `Request`. Ported
//! from `fixtures/paradex/signing/signbench/main.rs`, with the domain and each type's prefix
//! hashed once.
//!
//! Revision 0, as the Java `ParadexTypedDataSigner` uses it (domain type `StarkNetDomain`):
//!
//! ```text
//! message_hash = H(["StarkNet Message", domain_hash, account, struct_hash])
//! domain_hash  = H([keccak("StarkNetDomain(name:felt,chainId:felt,version:felt)"),
//!                   "Paradex", chain_id, 1])
//! struct_hash  = H([keccak(<type string>), field values...])
//! H([a1..an])  = pedersen(pedersen(...pedersen(pedersen(0, a1), a2)..., an), n)
//! ```
//!
//! where `keccak` is Starknet's keccak (Keccak-256 masked to 250 bits) and every string value
//! becomes a felt by [`message_felt`], Java's rule.

use fbc_core::Side;
use rust_decimal::Decimal;
use sha3::{Digest, Keccak256};
use starknet_crypto::{Felt, pedersen_hash};

use super::felt::{FeltError, ScaleError, message_felt, scale_e8, short_string};

const DOMAIN_TYPE: &str = "StarkNetDomain(name:felt,chainId:felt,version:felt)";
const ORDER_TYPE: &str =
    "Order(timestamp:felt,market:felt,side:felt,orderType:felt,size:felt,price:felt)";
const MODIFY_TYPE: &str =
    "ModifyOrder(timestamp:felt,market:felt,side:felt,orderType:felt,size:felt,price:felt,id:felt)";
const REQUEST_TYPE: &str =
    "Request(method:felt,path:felt,body:felt,timestamp:felt,expiration:felt)";

/// The order types this signer signs, by the name Paradex's message carries.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum ParadexOrderType {
    Limit,
    Market,
}

impl ParadexOrderType {
    /// The name the signed message carries (and the wire's `type` field).
    pub fn wire(self) -> &'static str {
        match self {
            ParadexOrderType::Limit => "LIMIT",
            ParadexOrderType::Market => "MARKET",
        }
    }
}

/// An order as Paradex's `Order` message signs it; a modify signs the same fields and the
/// venue's order id. The size and price are decimals, scaled by [`scale_e8`] (x1e8, truncated)
/// as the Java signer scales them; a market order signs price 0, as Java does.
#[derive(Copy, Clone, PartialEq, Debug)]
pub struct OrderMessage<'a> {
    /// The signature timestamp in milliseconds since the epoch (the wire's
    /// `signature_timestamp`).
    pub timestamp_ms: u64,
    /// The market symbol, such as `BTC-USD-PERP`.
    pub market: &'a str,
    pub side: Side,
    pub order_type: ParadexOrderType,
    pub size: Decimal,
    pub price: Decimal,
}

/// A request as Paradex's `Request` message signs it; the auth request is `POST /v1/auth` with
/// an empty body, its timestamp and expiration in seconds.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct RequestMessage<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub body: &'a str,
    pub timestamp: u64,
    pub expiration: u64,
}

/// Why a message has no hash: which field could not become a felt.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum HashError {
    Market(FeltError),
    OrderId(FeltError),
    Method(FeltError),
    Path(FeltError),
    Body(FeltError),
    Size(ScaleError),
    Price(ScaleError),
}

/// Starknet's keccak: Keccak-256, masked to its low 250 bits.
fn starknet_keccak(data: &[u8]) -> Felt {
    let mut digest: [u8; 32] = Keccak256::digest(data).into();
    digest[0] &= 0x03;
    Felt::from_bytes_be(&digest)
}

/// The Pedersen chain over `elements` from `state`, without the closing length.
fn chain(state: Felt, elements: &[Felt]) -> Felt {
    elements.iter().fold(state, |h, e| pedersen_hash(&h, e))
}

/// `H(elements)`: the chain from zero, closed with the element count.
fn hash_on_elements(elements: &[Felt]) -> Felt {
    pedersen_hash(&chain(Felt::ZERO, elements), &Felt::from(elements.len()))
}

/// The SNIP-12 revision 0 hasher for one account on one chain, with the message prefix
/// (`"StarkNet Message"`, the domain hash, the account) and each type's hash chained once.
#[derive(Clone, Debug)]
pub struct ParadexHasher {
    /// The chain state after `[prefix, domain_hash, account]`.
    outer: Felt,
    /// The chain state after `[type_hash]` for each message type.
    order: Felt,
    modify: Felt,
    request: Felt,
}

impl ParadexHasher {
    /// The hasher for `account` on chain `chain_id`, under Paradex's domain.
    pub fn new(account: Felt, chain_id: Felt) -> ParadexHasher {
        ParadexHasher::with_domain_hash(account, ParadexHasher::domain_hash(chain_id))
    }

    /// Paradex's domain separator: `StarkNetDomain` with its fields in the order Paradex and
    /// the Java signer declare them, name, chainId, version (`"Paradex"`, `chain_id`, 1).
    /// starknet-core's revision 0 domain orders them name, version, chainId, so its
    /// `TypedData` cannot express this domain.
    pub fn domain_hash(chain_id: Felt) -> Felt {
        hash_on_elements(&[
            starknet_keccak(DOMAIN_TYPE.as_bytes()),
            short_string("Paradex").expect("a short string"),
            chain_id,
            Felt::ONE,
        ])
    }

    /// The hasher for `account` under the domain separator `domain_hash`, computed elsewhere.
    /// [`ParadexHasher::new`] is this with Paradex's domain; the tests use it to hold the
    /// message encoding to starknet-core's, whose domain differs.
    pub fn with_domain_hash(account: Felt, domain_hash: Felt) -> ParadexHasher {
        let prefix = short_string("StarkNet Message").expect("a short string");
        let type_state = |t: &str| pedersen_hash(&Felt::ZERO, &starknet_keccak(t.as_bytes()));
        ParadexHasher {
            outer: chain(Felt::ZERO, &[prefix, domain_hash, account]),
            order: type_state(ORDER_TYPE),
            modify: type_state(MODIFY_TYPE),
            request: type_state(REQUEST_TYPE),
        }
    }

    /// The message hash of `[outer..., struct_hash]`, closed with its four elements.
    fn finish(&self, struct_hash: Felt) -> Felt {
        pedersen_hash(&pedersen_hash(&self.outer, &struct_hash), &Felt::from(4u8))
    }

    /// The six `Order` field values, in type order.
    fn order_fields(msg: &OrderMessage<'_>) -> Result<[Felt; 6], HashError> {
        let side = match msg.side {
            Side::Buy => Felt::ONE,
            Side::Sell => Felt::TWO,
        };
        Ok([
            Felt::from(msg.timestamp_ms),
            message_felt(msg.market).map_err(HashError::Market)?,
            side,
            short_string(msg.order_type.wire()).expect("a short string"),
            Felt::from(scale_e8(msg.size).map_err(HashError::Size)?),
            Felt::from(scale_e8(msg.price).map_err(HashError::Price)?),
        ])
    }

    /// The hash Paradex checks for a new order.
    pub fn order(&self, msg: &OrderMessage<'_>) -> Result<Felt, HashError> {
        let fields = Self::order_fields(msg)?;
        let state = chain(self.order, &fields);
        Ok(self.finish(pedersen_hash(&state, &Felt::from(fields.len() + 1))))
    }

    /// The hash Paradex checks for a modify of order `id` (the venue's order id).
    pub fn modify(&self, msg: &OrderMessage<'_>, id: &str) -> Result<Felt, HashError> {
        let fields = Self::order_fields(msg)?;
        let id = message_felt(id).map_err(HashError::OrderId)?;
        let state = pedersen_hash(&chain(self.modify, &fields), &id);
        Ok(self.finish(pedersen_hash(&state, &Felt::from(fields.len() + 2))))
    }

    /// The hash Paradex checks for a signed request (the auth request among them).
    pub fn request(&self, msg: &RequestMessage<'_>) -> Result<Felt, HashError> {
        let fields = [
            message_felt(msg.method).map_err(HashError::Method)?,
            message_felt(msg.path).map_err(HashError::Path)?,
            message_felt(msg.body).map_err(HashError::Body)?,
            Felt::from(msg.timestamp),
            Felt::from(msg.expiration),
        ];
        let state = chain(self.request, &fields);
        Ok(self.finish(pedersen_hash(&state, &Felt::from(fields.len() + 1))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn order(market: &str) -> OrderMessage<'_> {
        OrderMessage {
            timestamp_ms: 1,
            market,
            side: Side::Buy,
            order_type: ParadexOrderType::Limit,
            size: Decimal::ONE,
            price: Decimal::ONE,
        }
    }

    #[test]
    fn the_cached_prefixes_equal_hashing_the_whole_message() {
        let (account, chain_id) = (Felt::from(0xACC0u64), Felt::from(0xC4A1u64));
        let hasher = ParadexHasher::new(account, chain_id);
        let msg = order("BTC-USD-PERP");
        let domain = hash_on_elements(&[
            starknet_keccak(DOMAIN_TYPE.as_bytes()),
            short_string("Paradex").unwrap(),
            chain_id,
            Felt::ONE,
        ]);
        let mut fields = vec![starknet_keccak(ORDER_TYPE.as_bytes())];
        fields.extend(ParadexHasher::order_fields(&msg).unwrap());
        let whole = hash_on_elements(&[
            short_string("StarkNet Message").unwrap(),
            domain,
            account,
            hash_on_elements(&fields),
        ]);
        assert_eq!(hasher.order(&msg), Ok(whole));
    }

    #[test]
    fn a_field_that_is_no_felt_is_named() {
        let hasher = ParadexHasher::new(Felt::ONE, Felt::ONE);
        let long = "X".repeat(32);
        assert_eq!(
            hasher.order(&order(&long)),
            Err(HashError::Market(FeltError::TooLong))
        );
        let msg = order("BTC-USD-PERP");
        assert_eq!(
            hasher.modify(&msg, "-1"),
            Err(HashError::OrderId(FeltError::Negative))
        );
        let negative = OrderMessage {
            size: Decimal::NEGATIVE_ONE,
            ..msg
        };
        assert_eq!(
            hasher.order(&negative),
            Err(HashError::Size(ScaleError::Negative))
        );
        let negative = OrderMessage {
            price: Decimal::NEGATIVE_ONE,
            ..msg
        };
        assert_eq!(
            hasher.order(&negative),
            Err(HashError::Price(ScaleError::Negative))
        );
        let req = RequestMessage {
            method: "POST",
            path: "/v1/auth",
            body: "",
            timestamp: 1,
            expiration: 2,
        };
        let bad = "\u{e9}";
        assert_eq!(
            hasher.request(&RequestMessage { method: bad, ..req }),
            Err(HashError::Method(FeltError::NotAscii))
        );
        assert_eq!(
            hasher.request(&RequestMessage { path: bad, ..req }),
            Err(HashError::Path(FeltError::NotAscii))
        );
        assert_eq!(
            hasher.request(&RequestMessage { body: bad, ..req }),
            Err(HashError::Body(FeltError::NotAscii))
        );
        assert!(hasher.request(&req).is_ok());
    }

    #[test]
    fn order_types_carry_their_wire_names() {
        assert_eq!(ParadexOrderType::Limit.wire(), "LIMIT");
        assert_eq!(ParadexOrderType::Market.wire(), "MARKET");
    }
}
