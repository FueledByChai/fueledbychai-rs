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
//!
//! # Key copies
//!
//! What is wiped: [`StarkKey`] owns the key as 32 bytes in one heap buffer, read there straight
//! from the hex digits, and its `Drop` overwrites them with zeros (`zeroize`, volatile writes),
//! a refused key's included. Each public-key or signature call converts the bytes to a `Felt`
//! for the call only, and that `Felt` and the signature's RFC 6979 nonce `k` (which recovers
//! the key from a signature) are held in `Zeroizing` and overwritten when the call returns.
//!
//! What is not, and why: the wipe stops at this module's own values. starknet-crypto 0.8.1
//! (pinned exactly) and the crates under it take the key and nonce by reference and make
//! copies we cannot reach:
//!
//! - `get_public_key` passes the key `Felt` by value to the scalar multiplication
//!   (starknet-types-core 0.2.4, `&ProjectivePoint * Felt`), which takes its integer
//!   representative and walks its bits; `sign` does the same with `k` for `r`.
//! - `rfc6979_generate_k` copies the key into a 32-byte array and a crypto-bigint `U256` that it
//!   does not wipe (it wipes only the byte copy it seeds HMAC-DRBG with); the HMAC-DRBG state
//!   (rfc6979 0.4.0), derived from the key, is dropped unwiped; and the nonce, which it keeps in
//!   `Zeroizing` while drawing it, leaves as a byte array and a `Felt` that it does not wipe.
//! - `sign` turns the key, `k` and the products `r·key` and `h + r·key` into num-bigint
//!   `BigInt`s (`mul_mod_floor`, `add_unbounded`, `mod_inverse`, `bigint_mul_mod_floor`), heap
//!   allocations freed without wiping.
//!
//! Beyond those, a moved value (the `Felt` handed to `Zeroizing`, the nonce returned by value)
//! can leave a copy in a stack slot or register that no code can name. The `text` given to
//! [`StarkKey::from_hex`] is the caller's to wipe. Reaching starknet-crypto's copies would mean
//! patching or replacing its signing path, whose signatures the Java vectors pin; this module
//! does not.

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
use zeroize::{Zeroize, Zeroizing};

pub use felt::{FeltError, ScaleError, message_felt, scale_e8, short_string};
pub use typed::{HashError, OrderMessage, ParadexHasher, ParadexOrderType, RequestMessage};

/// The order of the Stark curve's generator, as 32 big-endian bytes (in decimal
/// 3618502788666131213697322783095070105526743751716087489154079457884512865583): a signing key
/// lies in `[1, EC_ORDER)`.
const EC_ORDER_BE: [u8; 32] = [
    0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
    0xb7, 0x81, 0x12, 0x6d, 0xca, 0xe7, 0xb2, 0x32, 0x1e, 0x66, 0xa2, 0x41, 0xad, 0xc6, 0x4d, 0x2f,
];

/// A Stark-curve signing key. It is never printed: `Debug` shows only that a key is there, and
/// no error carries any part of it. It is not `Clone`, so it lives where it was built.
///
/// The key's 32 big-endian bytes sit in one heap buffer for the key's whole life (moving the
/// key, into a [`ParadexSigner`] say, moves only the pointer), and dropping the key overwrites
/// that buffer with zeros through `zeroize`'s volatile writes before it is freed. What the
/// wipe does not reach is listed in the module's [key copies](self#key-copies).
pub struct StarkKey {
    bytes: Box<[u8; 32]>,
}

/// Why text is not a signing key; never carries the text.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum KeyError {
    /// Not `0x` followed by hex digits.
    NotHex,
    /// Zero, or not below the curve order.
    OutOfRange,
}

/// A `Felt` copy of the key, or of a signature's nonce, which recovers the key as well: held
/// only inside `Zeroizing`, so it is overwritten with zero when the signature is done.
#[derive(Copy, Clone, Default)]
struct Scalar(Felt);

impl zeroize::DefaultIsZeroes for Scalar {}

impl StarkKey {
    /// The key written as `0x` and hex digits (either case, leading zeros allowed), as Paradex
    /// and the Java signer take it. The digits are read straight into the key's own buffer, so
    /// a refused key's digits are wiped too; `text` itself is the caller's to wipe.
    pub fn from_hex(text: &str) -> Result<StarkKey, KeyError> {
        let digits = text.strip_prefix("0x").ok_or(KeyError::NotHex)?;
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(KeyError::NotHex);
        }
        let digits = digits.trim_start_matches('0').as_bytes();
        if digits.len() > 64 {
            return Err(KeyError::OutOfRange);
        }
        // Built first, so its Drop wipes the buffer on every path out of here.
        let mut key = StarkKey {
            bytes: Box::new([0u8; 32]),
        };
        for (i, &digit) in digits.iter().rev().enumerate() {
            let nibble = match digit {
                b'0'..=b'9' => digit - b'0',
                _ => (digit | 0x20) - b'a' + 10,
            };
            key.bytes[31 - i / 2] |= nibble << (4 * (i % 2));
        }
        // Big-endian bytes order as the integers they hold.
        if *key.bytes == [0u8; 32] || *key.bytes >= EC_ORDER_BE {
            return Err(KeyError::OutOfRange);
        }
        Ok(key)
    }

    /// The public key: the x coordinate of key x G.
    pub fn public_key(&self) -> Felt {
        get_public_key(&self.scalar().0)
    }

    /// The key as a `Felt`, for one call into starknet-crypto, wiped when it drops.
    fn scalar(&self) -> Zeroizing<Scalar> {
        Zeroizing::new(Scalar(Felt::from_bytes_be(&self.bytes)))
    }
}

impl Drop for StarkKey {
    fn drop(&mut self) {
        self.bytes.zeroize();
        #[cfg(test)]
        wipe_probe::record(&self.bytes);
    }
}

/// The test-only hook: what a dropped key's buffer held once its `Drop` had run, per thread.
#[cfg(test)]
mod wipe_probe {
    use std::cell::Cell;

    thread_local! {
        static WIPED: Cell<Option<[u8; 32]>> = const { Cell::new(None) };
    }

    pub(super) fn record(bytes: &[u8; 32]) {
        WIPED.with(|w| w.set(Some(*bytes)));
    }

    /// The last dropped key's buffer on this thread, if a key dropped since the last take.
    pub(super) fn take() -> Option<[u8; 32]> {
        WIPED.with(Cell::take)
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
        let key = self.key.scalar();
        let k = Zeroizing::new(Scalar(rfc6979_generate_k(hash, &key.0, None)));
        let sig = starknet_crypto::sign(&key.0, hash, &k.0)
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

    const EC_ORDER: &str =
        "3618502788666131213697322783095070105526743751716087489154079457884512865583";

    #[test]
    fn the_curve_order_bytes_are_the_curve_order() {
        assert_eq!(
            Felt::from_bytes_be(&EC_ORDER_BE),
            Felt::from_dec_str(EC_ORDER).unwrap()
        );
    }

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

    #[test]
    fn dropping_a_key_overwrites_the_bytes_it_owns() {
        let _ = wipe_probe::take();
        let key = StarkKey::from_hex("0x2a").unwrap();
        let mut held = [0u8; 32];
        held[31] = 0x2a;
        assert_eq!(*key.bytes, held, "the key is held big-endian");
        assert_eq!(wipe_probe::take(), None, "nothing is wiped before the drop");
        drop(key);
        assert_eq!(wipe_probe::take(), Some([0u8; 32]));
        // Held by a signer, the key is wiped when the signer drops.
        let signer = ParadexSigner::new(Felt::ONE, Felt::ONE, StarkKey::from_hex("0x2a").unwrap());
        drop(signer);
        assert_eq!(wipe_probe::take(), Some([0u8; 32]));
    }

    #[test]
    fn a_refused_key_wipes_the_digits_it_read() {
        let _ = wipe_probe::take();
        let order = Felt::from_dec_str(EC_ORDER).unwrap();
        let at_order = format!("{order:#x}");
        assert_eq!(
            StarkKey::from_hex(&at_order).map(|_| ()),
            Err(KeyError::OutOfRange)
        );
        assert_eq!(wipe_probe::take(), Some([0u8; 32]));
        assert_eq!(
            StarkKey::from_hex("0x0").map(|_| ()),
            Err(KeyError::OutOfRange)
        );
        assert_eq!(wipe_probe::take(), Some([0u8; 32]));
        // Refused before a buffer exists: nothing to wipe.
        assert_eq!(StarkKey::from_hex("12").map(|_| ()), Err(KeyError::NotHex));
        assert_eq!(wipe_probe::take(), None);
    }

    #[test]
    fn a_key_reads_hex_of_any_case_and_leading_zeros() {
        let mixed = StarkKey::from_hex("0x00AbC").unwrap();
        assert_eq!(mixed.public_key(), get_public_key(&Felt::from(0xabcu16)));
        let padded = format!("0x{}2a", "0".repeat(80));
        let key = StarkKey::from_hex(&padded).unwrap();
        assert_eq!(key.public_key(), get_public_key(&Felt::from(42u8)));
        // 64 hex digits, the most a 256-bit buffer holds: in range below the curve order.
        let full = format!("0x0{}", "7".repeat(63));
        assert!(StarkKey::from_hex(&full).is_ok());
    }

    #[test]
    fn a_scalar_copy_is_overwritten_with_zero() {
        let mut copy = Scalar(Felt::from(42u8));
        copy.zeroize();
        assert_eq!(copy.0, Felt::ZERO);
        let key = StarkKey::from_hex("0x2a").unwrap();
        assert_eq!(key.scalar().0, Felt::from(42u8));
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
