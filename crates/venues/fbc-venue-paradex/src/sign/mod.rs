//! The Paradex signer (decision 0009: a review path; the owner reviews every change here).
//!
//! Paradex signs three messages as SNIP-12 revision 0 typed data ([`typed`]): a new `Order`, a
//! `ModifyOrder` (the order plus the venue's order id) and the auth `Request`. The hash is
//! signed with Stark-curve ECDSA, its nonce drawn by RFC 6979 (HMAC-SHA256) from the key and
//! the hash, so a signature is a pure function of key and message, as in the Java signer
//! (`BcStarknetCurveSigner`). Cancels are not signed.
//!
//! Every message field is encoded as the Java FueledByChaiTrading signer encodes it: decimal
//! size and price scaled x1e8 and truncated ([`scale_e8`]), strings by Java's TypedData rule
//! ([`message_felt`]). `fixtures/paradex/signing/paradex-vectors.tsv`, written by
//! `ParadexHashOracle.java` from the Java signer, is the golden file the tests hold this to.

mod felt;
mod typed;

use std::fmt;

use fbc_core::{
    AmendRef, AmendWire, CancelWire, InstrumentSpec, Lots, OrderKind, OrderSigner, PlaceWire, Sig,
    SignError, Ticks, WallNs,
};
use rust_decimal::Decimal;
pub use starknet_crypto::Felt;
use starknet_crypto::{get_public_key, rfc6979_generate_k};

pub use felt::{FeltError, ScaleError, message_felt, scale_e8, short_string};
pub use typed::{HashError, OrderMessage, ParadexHasher, ParadexOrderType, RequestMessage};

/// The order of the Stark curve's generator: a signing key lies in `[1, EC_ORDER)`.
const EC_ORDER: &str =
    "3618502788666131213697322783095070105526743751716087489154079457884512865583";

/// A Stark-curve signing key. It is never printed: `Debug` shows only that a key is there, and
/// no error carries any part of it. It is not `Clone`, so it lives where it was built.
pub struct StarkKey(Felt);

/// Why text is not a signing key; never carries the text.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum KeyError {
    /// Not `0x` followed by hex digits.
    NotHex,
    /// Zero, or not below the curve order.
    OutOfRange,
}

impl StarkKey {
    /// The key written as `0x` and hex digits, as Paradex and the Java signer take it.
    pub fn from_hex(text: &str) -> Result<StarkKey, KeyError> {
        let digits = text.strip_prefix("0x").ok_or(KeyError::NotHex)?;
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(KeyError::NotHex);
        }
        let order = Felt::from_dec_str(EC_ORDER).expect("the curve order is a felt");
        match felt::parse_radix(digits, 16) {
            Some(k) if k != Felt::ZERO && k < order => Ok(StarkKey(k)),
            _ => Err(KeyError::OutOfRange),
        }
    }

    /// The public key: the x coordinate of key x G.
    pub fn public_key(&self) -> Felt {
        get_public_key(&self.0)
    }
}

impl fmt::Debug for StarkKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("StarkKey(..)")
    }
}

/// A Stark-curve signature.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct StarkSig {
    pub r: Felt,
    pub s: Felt,
}

impl StarkSig {
    /// The signature as Paradex's wire carries it (the `signature` field of an order, and the
    /// auth header): `["r","s"]` with both in decimal, as the Java signer writes it
    /// (`ParadexTypedDataSigner.toParadexArray`). At most 159 bytes, within
    /// [`fbc_core::MAX_SIG_LEN`].
    pub fn to_sig(&self) -> Sig {
        let wire = format!(r#"["{}","{}"]"#, self.r, self.s);
        Sig::new(wire.as_bytes()).expect("a Stark signature's wire form fits a Sig")
    }
}

impl From<HashError> for SignError {
    fn from(e: HashError) -> SignError {
        SignError::Unsignable(match e {
            HashError::Market(_) => "market",
            HashError::OrderId(_) => "order id",
            HashError::Method(_) => "method",
            HashError::Path(_) => "path",
            HashError::Body(_) => "body",
            HashError::Size(_) => "negative size",
            HashError::Price(_) => "negative price",
        })
    }
}

/// Signs Paradex orders, modifies and requests for one account on one chain.
#[derive(Debug)]
pub struct ParadexSigner {
    hasher: ParadexHasher,
    key: StarkKey,
}

impl ParadexSigner {
    /// The signer for `account` (the Starknet account address) on chain `chain_id`, signing
    /// with `key`.
    pub fn new(account: Felt, chain_id: Felt, key: StarkKey) -> ParadexSigner {
        ParadexSigner {
            hasher: ParadexHasher::new(account, chain_id),
            key,
        }
    }

    pub fn public_key(&self) -> Felt {
        self.key.public_key()
    }

    pub fn order_hash(&self, msg: &OrderMessage<'_>) -> Result<Felt, SignError> {
        Ok(self.hasher.order(msg)?)
    }

    pub fn modify_hash(&self, msg: &OrderMessage<'_>, id: &str) -> Result<Felt, SignError> {
        Ok(self.hasher.modify(msg, id)?)
    }

    pub fn request_hash(&self, msg: &RequestMessage<'_>) -> Result<Felt, SignError> {
        Ok(self.hasher.request(msg)?)
    }

    pub fn sign_order(&self, msg: &OrderMessage<'_>) -> Result<StarkSig, SignError> {
        self.sign_hash(&self.order_hash(msg)?)
    }

    /// Signs a modify of the venue's order `id`.
    pub fn sign_modify(&self, msg: &OrderMessage<'_>, id: &str) -> Result<StarkSig, SignError> {
        self.sign_hash(&self.modify_hash(msg, id)?)
    }

    pub fn sign_request(&self, msg: &RequestMessage<'_>) -> Result<StarkSig, SignError> {
        self.sign_hash(&self.request_hash(msg)?)
    }

    /// Signs the auth request (`POST /v1/auth`, empty body); `timestamp` and `expiration` in
    /// seconds since the epoch, as the auth headers carry them.
    pub fn sign_auth_request(
        &self,
        timestamp: u64,
        expiration: u64,
    ) -> Result<StarkSig, SignError> {
        self.sign_request(&RequestMessage {
            method: "POST",
            path: "/v1/auth",
            body: "",
            timestamp,
            expiration,
        })
    }

    /// Stark-curve ECDSA over `hash` with the RFC 6979 nonce. Refused only for a hash at or
    /// past 2^251, which a Pedersen hash reaches with probability about 2^-59.
    fn sign_hash(&self, hash: &Felt) -> Result<StarkSig, SignError> {
        let k = rfc6979_generate_k(hash, &self.key.0, None);
        let sig = starknet_crypto::sign(&self.key.0, hash, &k)
            .map_err(|_| SignError::Backend("stark-curve signature"))?;
        Ok(StarkSig { r: sig.r, s: sig.s })
    }
}

/// The signature timestamp: `wall` in whole milliseconds.
fn timestamp_ms(wall: WallNs) -> Result<u64, SignError> {
    u64::try_from(wall.0)
        .map(|ns| ns / 1_000_000)
        .map_err(|_| SignError::Unsignable("timestamp before 1970"))
}

/// The order size in the instrument's units: `qty` size steps.
fn size_of(spec: &InstrumentSpec, qty: Lots) -> Result<Decimal, SignError> {
    Decimal::from(qty.get())
        .checked_mul(spec.size_step.get())
        .ok_or(SignError::Unsignable("size out of range"))
}

/// The exact price of tick index `px`.
fn price_of(spec: &InstrumentSpec, px: Ticks) -> Result<Decimal, SignError> {
    if px.0 < 0 {
        return Err(SignError::Unsignable("negative price"));
    }
    spec.price_grid
        .px_of(px)
        .and_then(|p| p.to_decimal())
        .ok_or(SignError::Unsignable("price out of range"))
}

/// The fbc-core boundary. The signed message carries the market, side, type, size, price and
/// time only: time in force, post-only, reduce-only and the channel travel unsigned on the
/// wire, and Paradex takes no nonce.
impl OrderSigner for ParadexSigner {
    fn sign_place(&mut self, w: &PlaceWire<'_>) -> Result<Sig, SignError> {
        let (order_type, price) = match w.kind {
            OrderKind::Limit { px } => (ParadexOrderType::Limit, price_of(w.spec, px)?),
            OrderKind::Market => (ParadexOrderType::Market, Decimal::ZERO),
        };
        let msg = OrderMessage {
            timestamp_ms: timestamp_ms(w.wall)?,
            market: w.spec.venue_symbol.as_wire(),
            side: w.side,
            order_type,
            size: size_of(w.spec, w.qty)?,
            price,
        };
        Ok(self.sign_order(&msg)?.to_sig())
    }

    /// A modify signs the venue's order id; Paradex modifies limit orders only.
    fn sign_amend(&mut self, w: &AmendWire<'_>) -> Result<Sig, SignError> {
        let id = match w.target {
            AmendRef::Venue(id) => id.as_str(),
            AmendRef::Client(_) => return Err(SignError::Unsignable("amend by client id")),
        };
        let msg = OrderMessage {
            timestamp_ms: timestamp_ms(w.wall)?,
            market: w.spec.venue_symbol.as_wire(),
            side: w.side,
            order_type: ParadexOrderType::Limit,
            size: size_of(w.spec, w.qty)?,
            price: price_of(w.spec, w.px)?,
        };
        Ok(self.sign_modify(&msg, id)?.to_sig())
    }

    /// Paradex cancels carry no signature.
    fn sign_cancel(&mut self, _w: &CancelWire<'_>) -> Result<Option<Sig>, SignError> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_is_hex_in_range_and_never_printed() {
        assert_eq!(StarkKey::from_hex("12").map(|_| ()), Err(KeyError::NotHex));
        assert_eq!(StarkKey::from_hex("0x").map(|_| ()), Err(KeyError::NotHex));
        assert_eq!(
            StarkKey::from_hex("0xg1").map(|_| ()),
            Err(KeyError::NotHex)
        );
        assert_eq!(
            StarkKey::from_hex("0x0").map(|_| ()),
            Err(KeyError::OutOfRange)
        );
        let order = Felt::from_dec_str(EC_ORDER).unwrap();
        let at_order = format!("{order:#x}");
        assert_eq!(
            StarkKey::from_hex(&at_order).map(|_| ()),
            Err(KeyError::OutOfRange)
        );
        let below = format!("{:#x}", order - Felt::ONE);
        assert!(StarkKey::from_hex(&below).is_ok());
        let past_256_bits = format!("0x1{}", "0".repeat(64));
        assert_eq!(
            StarkKey::from_hex(&past_256_bits).map(|_| ()),
            Err(KeyError::OutOfRange)
        );
        let key = StarkKey::from_hex("0x2a").unwrap();
        assert_eq!(format!("{key:?}"), "StarkKey(..)");
        let signer = ParadexSigner::new(Felt::ONE, Felt::ONE, key);
        assert!(!format!("{signer:?}").contains("2a"));
        assert_eq!(signer.public_key(), get_public_key(&Felt::from(42u8)));
    }

    #[test]
    fn a_signature_is_the_paradex_decimal_array() {
        let sig = StarkSig {
            r: Felt::from(1u8),
            s: Felt::from(20u8),
        };
        assert_eq!(sig.to_sig().as_bytes(), br#"["1","20"]"#);
        // The longest: r and s just below the curve order, 76 digits each, 159 bytes.
        let top = Felt::from_dec_str(EC_ORDER).unwrap() - Felt::ONE;
        let longest = StarkSig { r: top, s: top }.to_sig();
        assert_eq!(longest.as_bytes().len(), 159);
        assert_eq!(
            longest.as_bytes(),
            format!(r#"["{EC_ORDER_MINUS_ONE}","{EC_ORDER_MINUS_ONE}"]"#).as_bytes()
        );
    }

    const EC_ORDER_MINUS_ONE: &str =
        "3618502788666131213697322783095070105526743751716087489154079457884512865582";

    #[test]
    fn a_hash_past_2_251_is_a_backend_refusal() {
        let signer = ParadexSigner::new(Felt::ONE, Felt::ONE, StarkKey::from_hex("0x2a").unwrap());
        assert_eq!(
            signer.sign_hash(&Felt::MAX),
            Err(SignError::Backend("stark-curve signature"))
        );
    }

    #[test]
    fn hash_errors_name_the_field() {
        let cases = [
            (HashError::Market(FeltError::TooLong), "market"),
            (HashError::OrderId(FeltError::Negative), "order id"),
            (HashError::Method(FeltError::NotAscii), "method"),
            (HashError::Path(FeltError::NotAscii), "path"),
            (HashError::Body(FeltError::NotAscii), "body"),
            (HashError::Size(ScaleError::Negative), "negative size"),
            (HashError::Price(ScaleError::Negative), "negative price"),
        ];
        for (e, what) in cases {
            assert_eq!(SignError::from(e), SignError::Unsignable(what));
        }
    }
}
