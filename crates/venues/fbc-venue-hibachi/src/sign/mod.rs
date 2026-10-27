//! The Hibachi signer (decision 0009: a review path; the owner reviews every change here).
//!
//! Hibachi signs every order command of a trustless account: a place, a modify, a cancel and a
//! cancel-all, unlike Paradex, whose cancels are unsigned. Each signs a short big-endian payload
//! ([`pack`]); the signature is ECDSA over secp256k1 of the payload's SHA-256, its nonce drawn
//! by RFC 6979 from the key and the digest and its s made low, written as 65 bytes r || s || v,
//! with v the recovery id 0 or 1 (not Ethereum's 27 or 28), in 130 lowercase hex characters
//! (Java `EcdsaHibachiSigner.java` L36-L53; the SDK's `__sign_payload`, `api.py` L2008-L2038 at
//! 14a76b9; Hibachi's Signing page, whose JavaScript example signs with `canonical: true`, a
//! low s, and whose text says the recovery id is concatenated as the 65th byte). A signature is
//! a pure function of key and payload, as in Java, whose web3j 4.10.3 `ECKeyPair.sign` draws
//! its nonce by BouncyCastle's `HMacDSAKCalculator` (RFC 6979 over SHA-256) and lowers s.
//!
//! This is the ECDSA scheme only. The SDK takes a key without `0x` for an HMAC secret, and
//! Hibachi refuses an HMAC signature on a trustless account's order ("expected 64-byte signature
//! and a recovery ID", Java `HibachiSignerFactory.java`); there is no HMAC path here.
//!
//! `fixtures/hibachi/signing/hibachi-vectors.tsv`, written by `HibachiSignOracle.java` from the
//! Java packer and signer on a synthetic key, is the golden file the tests hold this to.
//!
//! This module holds only the cryptographic signer: the venue crate's `auth` module takes the
//! key from the consumer's credentials and hands the codec an initialized [`HibachiSigner`]
//! (decision 0043). [`HibachiSigner`] does not implement fbc-core's `OrderSigner`: a place and
//! a modify sign the order's max fees, which `PlaceWire` and `AmendWire` do not carry, and the
//! signer holds no value and no default for them, so the codec passes them with each call.
//!
//! # Key copies
//!
//! What is wiped: [`EcdsaKey::from_hex`] reads the hex digits straight into a 32-byte heap
//! buffer of its own, which it overwrites with zeros (`zeroize`, volatile writes) before it
//! returns, a refused key's included. The key itself is k256's `SigningKey`, held on the heap
//! for the key's whole life; ecdsa 0.16.9's `Drop` for it zeroizes its secret scalar
//! (`signing.rs` L380-L389), when the key, or the [`HibachiSigner`] holding it, drops.
//!
//! What is not, and why: the wipe stops at the values this module and ecdsa's `Drop` own. k256
//! 0.13.4 (pinned exactly) and the crates under it (ecdsa 0.16.9 and rfc6979 0.4.0, held by
//! `Cargo.lock` within k256's ranges, so a lockfile change re-reads what follows) make copies
//! we cannot reach:
//!
//! - `SigningKey::from_bytes` builds a `SecretKey` (wiped on drop) and the signing key by value
//!   on the stack before it is boxed, so a stack copy of the scalar can outlive the build.
//! - Each signature draws its nonce with rfc6979 0.4.0 from a copy of the key's bytes (ecdsa's
//!   `try_sign_prehashed_rfc6979` passes it `to_repr()`), and that copy and the HMAC-DRBG state
//!   derived from it are dropped unwiped; the nonce `k` and its inverse, which recover the key
//!   from a signature, are scalars on the stack that ecdsa's `sign_prehashed` does not wipe, nor
//!   the product of r and the key it forms.
//!
//! Beyond those, a moved value can leave a copy in a stack slot or register that no code can
//! name. The `text` given to [`EcdsaKey::from_hex`] is the caller's to wipe (`Secret` wipes its
//! own). Reaching k256's copies would mean patching its signing path, whose signatures the Java
//! vectors pin; this module does not.

pub mod pack;

use core::fmt;

use fbc_core::{
    AmendWire, CancelRef, CancelWire, InstrumentSpec, Lots, OrderKind, PlaceWire, Sig, SignError,
    Ticks,
};
use k256::FieldBytes;
use k256::ecdsa::{RecoveryId, SigningKey, VerifyingKey};
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};
use zeroize::Zeroize;

pub use pack::{
    CancelTarget, JsonOnly, MAX_FIELD, OrderMessage, Payload, order_id, pack_arm, pack_cancel,
    pack_cancel_all, pack_order,
};

/// A secp256k1 signing key. It is never printed: `Debug` shows only that a key is there, and no
/// error carries any part of it. It is not `Clone`, so it lives where it was built. What is
/// wiped when it drops, and what is not, is in the module's [key copies](self#key-copies).
pub struct EcdsaKey {
    key: Box<SigningKey>,
}

/// Why text is not a signing key; never carries the text.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum KeyError {
    /// Not `0x` followed by exactly 64 hex digits.
    NotHex,
    /// Zero, or not below the curve order.
    OutOfRange,
}

impl fmt::Display for KeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            KeyError::NotHex => "not 0x and 64 hex digits",
            KeyError::OutOfRange => "not a secp256k1 key in range",
        })
    }
}

impl std::error::Error for KeyError {}

/// The key's 32 big-endian bytes while the signing key is built from them: overwritten with
/// zeros when dropped.
struct KeyBytes(Box<[u8; 32]>);

impl Drop for KeyBytes {
    fn drop(&mut self) {
        self.0.zeroize();
        #[cfg(test)]
        wipe_probe::record(&self.0);
    }
}

/// The test-only hook: what a dropped key buffer held once its `Drop` had run, per thread.
#[cfg(test)]
mod wipe_probe {
    use std::cell::Cell;

    thread_local! {
        static WIPED: Cell<Option<[u8; 32]>> = const { Cell::new(None) };
    }

    pub(super) fn record(bytes: &[u8; 32]) {
        WIPED.with(|w| w.set(Some(*bytes)));
    }

    /// The last dropped buffer on this thread, if one dropped since the last take.
    pub(super) fn take() -> Option<[u8; 32]> {
        WIPED.with(Cell::take)
    }
}

impl EcdsaKey {
    /// The key written as `0x` and exactly 64 hex digits, either case, as a trustless account's
    /// key is exported: the `0x` is how the SDK tells an ECDSA key from an HMAC secret
    /// (`api.py` L403-L411), and the full 64 digits refuse a key cut short. The digits are read
    /// straight into a buffer of this module's, wiped before this returns; `text` itself is the
    /// caller's to wipe.
    pub fn from_hex(text: &str) -> Result<EcdsaKey, KeyError> {
        let digits = text.strip_prefix("0x").ok_or(KeyError::NotHex)?.as_bytes();
        if digits.len() != 64 || !digits.iter().all(u8::is_ascii_hexdigit) {
            return Err(KeyError::NotHex);
        }
        // Built first, so its Drop wipes the buffer on every path out of here.
        let mut bytes = KeyBytes(Box::new([0u8; 32]));
        for (i, &digit) in digits.iter().enumerate() {
            let nibble = match digit {
                b'0'..=b'9' => digit - b'0',
                _ => (digit | 0x20) - b'a' + 10,
            };
            bytes.0[i / 2] |= nibble << (4 * (1 - i % 2));
        }
        // Refuses zero and any value at or past the curve order.
        let key = SigningKey::from_bytes(FieldBytes::from_slice(&bytes.0[..]))
            .map_err(|_| KeyError::OutOfRange)?;
        Ok(EcdsaKey { key: Box::new(key) })
    }
}

impl fmt::Debug for EcdsaKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("EcdsaKey(..)")
    }
}

/// A signature as Hibachi takes it: r (32 bytes) || s (32 bytes) || v (the recovery id, 0 or
/// 1). Its `Debug` shows nothing of it.
#[derive(Copy, Clone, Eq, PartialEq, Hash)]
pub struct HibachiSig([u8; 65]);

impl HibachiSig {
    pub fn as_bytes(&self) -> &[u8; 65] {
        &self.0
    }

    /// The 130 lowercase hex characters the request's `signature` carries.
    pub fn to_hex(&self) -> String {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let mut text = String::with_capacity(130);
        for byte in self.0 {
            text.push(char::from(HEX[usize::from(byte >> 4)]));
            text.push(char::from(HEX[usize::from(byte & 0x0f)]));
        }
        text
    }

    /// The hex form as fbc-core's [`Sig`]: 130 bytes, within [`fbc_core::MAX_SIG_LEN`]. A `Sig`
    /// is the signature as the request carries it and the journal keeps it (decision 0006):
    /// unlike this type, its `Debug` prints every byte, so it is not a value to log.
    pub fn to_sig(&self) -> Sig {
        Sig::new(self.to_hex().as_bytes()).expect("130 hex characters fit a Sig")
    }
}

impl fmt::Debug for HibachiSig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HibachiSig(..)")
    }
}

/// The recovery id as the signature's last byte: 0 or 1. An id of 2 or 3 (r reduced modulo the
/// curve order, with probability about 2^-128) is refused, since Hibachi documents and the SDK
/// sends only 0 and 1.
fn recovery_byte(id: RecoveryId) -> Result<u8, SignError> {
    match id.to_byte() {
        v @ (0 | 1) => Ok(v),
        _ => Err(SignError::Backend("recovery id past 1")),
    }
}

/// Signs Hibachi's order commands for one trustless account.
#[derive(Debug)]
pub struct HibachiSigner {
    key: EcdsaKey,
}

impl HibachiSigner {
    /// The signer that signs with `key`.
    pub fn new(key: EcdsaKey) -> HibachiSigner {
        HibachiSigner { key }
    }

    /// The account's public key, which every signature recovers to.
    pub fn verifying_key(&self) -> &VerifyingKey {
        self.key.key.verifying_key()
    }

    /// ECDSA over secp256k1 of the payload's SHA-256, its nonce by RFC 6979 and its s low.
    pub fn sign_payload(&self, payload: &Payload) -> Result<HibachiSig, SignError> {
        let digest = Sha256::digest(payload.as_bytes());
        let (sig, id) = self
            .key
            .key
            .sign_prehash_recoverable(&digest)
            .map_err(|_| SignError::Backend("secp256k1 signature"))?;
        let mut out = [0u8; 65];
        out[..64].copy_from_slice(&sig.to_bytes());
        out[64] = recovery_byte(id)?;
        Ok(HibachiSig(out))
    }

    /// Signs a place or a modify, as its request states it.
    pub fn sign_order(&self, m: &OrderMessage<'_>) -> Result<HibachiSig, SignError> {
        self.sign_payload(&pack_order(m)?)
    }

    /// Signs a new order, from fbc-core's wire form, with the order's `max_fees` (the JSON's
    /// `maxFeesPercent`). The order's nonce is required. Its client id, time in force, post-only,
    /// reduce-only and channel are JSON only, and so is the time the request is made at.
    pub fn sign_place(
        &self,
        w: &PlaceWire<'_>,
        max_fees: Decimal,
    ) -> Result<HibachiSig, SignError> {
        let price = match w.kind {
            OrderKind::Limit { px } => Some(price_of(w.spec, px)?),
            OrderKind::Market => None,
        };
        self.sign_order(&OrderMessage {
            spec: w.spec,
            nonce: w.nonce.ok_or(SignError::Unsignable("no nonce"))?,
            side: w.side,
            quantity: quantity_of(w.spec, w.qty)?,
            price,
            max_fees,
            json_only: JsonOnly::NONE,
        })
    }

    /// Signs a modify, from fbc-core's wire form: the full post-modify order under the modify's
    /// own nonce, with its `max_fees`. The order it names, by venue or client id, is JSON only.
    pub fn sign_modify(
        &self,
        w: &AmendWire<'_>,
        max_fees: Decimal,
    ) -> Result<HibachiSig, SignError> {
        self.sign_order(&OrderMessage {
            spec: w.spec,
            nonce: w.nonce.ok_or(SignError::Unsignable("no nonce"))?,
            side: w.side,
            quantity: quantity_of(w.spec, w.qty)?,
            price: Some(price_of(w.spec, w.px)?),
            max_fees,
            json_only: JsonOnly::NONE,
        })
    }

    /// Signs a cancel by the reference it carries: the venue's order id, or the order's
    /// placement nonce. A cancel by client id is refused: only Hibachi's API collection describes
    /// it (its ASCII bytes), with no vector from Java or the SDK to hold it to (FBC-8mzv).
    pub fn sign_cancel(&self, w: &CancelWire<'_>) -> Result<HibachiSig, SignError> {
        let target = match w.target {
            CancelRef::Venue(id) => CancelTarget::OrderId(order_id(id.as_str())?),
            CancelRef::PlacementNonce(nonce) => CancelTarget::PlacementNonce(nonce),
            CancelRef::Client(_) => return Err(SignError::Unsignable("cancel by client id")),
        };
        self.sign_payload(&pack_cancel(target)?)
    }

    /// Signs a cancel-all with its `nonce`.
    pub fn sign_cancel_all(&self, nonce: u64) -> Result<HibachiSig, SignError> {
        self.sign_payload(&pack_cancel_all(nonce)?)
    }

    /// Signs the cancel-on-disconnect arm with its `nonce` ([`pack_arm`] says why it may be sent
    /// unsigned instead).
    pub fn sign_arm(&self, nonce: u64) -> Result<HibachiSig, SignError> {
        self.sign_payload(&pack_arm(nonce)?)
    }
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

/// The quantity of `qty` size steps.
fn quantity_of(spec: &InstrumentSpec, qty: Lots) -> Result<Decimal, SignError> {
    Decimal::from(qty.get())
        .checked_mul(spec.size_step.get())
        .ok_or(SignError::Unsignable("quantity out of range"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A key made up for these unit tests: 0x and 63 zeros then 2a.
    fn key_text(last: &str) -> String {
        format!("0x{}{last}", "0".repeat(64 - last.len()))
    }

    /// secp256k1's group order less `minus`, as 64 hex digits (built here, so no key-shaped
    /// literal sits in this crate).
    fn order_hex(minus: u8) -> String {
        use k256::elliptic_curve::Curve;
        use k256::elliptic_curve::bigint::Encoding;
        let mut bytes = k256::Secp256k1::ORDER.to_be_bytes();
        bytes[31] -= minus;
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    #[test]
    fn a_key_is_0x_and_64_hex_digits_in_range_and_never_printed() {
        let refused = |text: &str| EcdsaKey::from_hex(text).map(|_| ()).unwrap_err();
        assert_eq!(refused(&key_text("2a")[2..]), KeyError::NotHex, "no 0x");
        assert_eq!(
            refused(&key_text("2a").replace("0x", "0X")),
            KeyError::NotHex
        );
        assert_eq!(refused("0x2a"), KeyError::NotHex, "cut short");
        assert_eq!(
            refused(&format!("{}0", key_text("2a"))),
            KeyError::NotHex,
            "65 digits"
        );
        assert_eq!(refused(&key_text("2g")), KeyError::NotHex);
        assert_eq!(refused(&key_text("0")), KeyError::OutOfRange);
        assert_eq!(
            refused(&format!("0x{}", order_hex(0))),
            KeyError::OutOfRange
        );
        assert_eq!(
            refused(&format!("0x{}", "f".repeat(64))),
            KeyError::OutOfRange
        );
        assert!(EcdsaKey::from_hex(&format!("0x{}", order_hex(1))).is_ok());
        assert!(EcdsaKey::from_hex(&format!("0x{}", order_hex(1).to_uppercase())).is_ok());

        let key = EcdsaKey::from_hex(&key_text("2a")).unwrap();
        assert_eq!(format!("{key:?}"), "EcdsaKey(..)");
        let signer = HibachiSigner::new(key);
        assert_eq!(format!("{signer:?}"), "HibachiSigner { key: EcdsaKey(..) }");
        // The key 42 is 42 x G.
        let expected = SigningKey::from_slice(&[[0u8; 31].as_slice(), &[42]].concat()).unwrap();
        assert_eq!(signer.verifying_key(), expected.verifying_key());
        for e in [KeyError::NotHex, KeyError::OutOfRange] {
            assert!(!format!("{e} {e:?}").contains("2a"));
        }
    }

    #[test]
    fn the_key_buffer_is_wiped_whether_the_key_is_taken_or_refused() {
        let _ = wipe_probe::take();
        let key = EcdsaKey::from_hex(&key_text("2a")).unwrap();
        assert_eq!(wipe_probe::take(), Some([0u8; 32]), "taken");
        drop(key);
        assert_eq!(
            EcdsaKey::from_hex(&key_text("0")).map(|_| ()),
            Err(KeyError::OutOfRange)
        );
        assert_eq!(wipe_probe::take(), Some([0u8; 32]), "zero");
        let at_order = format!("0x{}", order_hex(0));
        assert_eq!(
            EcdsaKey::from_hex(&at_order).map(|_| ()),
            Err(KeyError::OutOfRange)
        );
        assert_eq!(wipe_probe::take(), Some([0u8; 32]), "at the order");
        // Refused before a buffer exists: nothing to wipe.
        assert_eq!(
            EcdsaKey::from_hex("0x2a").map(|_| ()),
            Err(KeyError::NotHex)
        );
        assert_eq!(wipe_probe::take(), None);
    }

    #[test]
    fn a_key_reads_big_endian() {
        let key = EcdsaKey::from_hex(&key_text("0102")).unwrap();
        let mut bytes = [0u8; 32];
        bytes[30..].copy_from_slice(&[1, 2]);
        assert_eq!(key.key.to_bytes().as_slice(), bytes.as_slice());
    }

    #[test]
    fn the_recovery_byte_is_0_or_1_and_an_x_reduced_id_is_refused() {
        assert_eq!(recovery_byte(RecoveryId::new(false, false)), Ok(0));
        assert_eq!(recovery_byte(RecoveryId::new(true, false)), Ok(1));
        let refused = Err(SignError::Backend("recovery id past 1"));
        assert_eq!(recovery_byte(RecoveryId::new(false, true)), refused);
        assert_eq!(recovery_byte(RecoveryId::new(true, true)), refused);
    }

    #[test]
    fn a_signature_is_130_lowercase_hex_and_never_printed() {
        let mut bytes = [0u8; 65];
        bytes[0] = 0xab;
        bytes[63] = 0x0f;
        bytes[64] = 1;
        let sig = HibachiSig(bytes);
        let hex = sig.to_hex();
        assert_eq!(hex.len(), 130);
        assert!(hex.starts_with("ab00"));
        assert!(hex.ends_with("0f01"));
        assert_eq!(sig.to_sig().as_bytes(), hex.as_bytes());
        assert_eq!(format!("{sig:?}"), "HibachiSig(..)");
    }

    #[test]
    fn a_signature_is_deterministic_low_s_and_recovers_to_the_key() {
        let signer = HibachiSigner::new(EcdsaKey::from_hex(&key_text("2a")).unwrap());
        let payload = pack_cancel_all(1_761_500_000_000_000).unwrap();
        let sig = signer.sign_payload(&payload).unwrap();
        assert_eq!(sig, signer.sign_payload(&payload).unwrap());
        let parsed = k256::ecdsa::Signature::from_slice(&sig.as_bytes()[..64]).unwrap();
        assert!(parsed.normalize_s().is_none(), "s is low");
        let id = RecoveryId::from_byte(sig.as_bytes()[64]).unwrap();
        let digest = Sha256::digest(payload.as_bytes());
        let recovered = VerifyingKey::recover_from_prehash(&digest, &parsed, id).unwrap();
        assert_eq!(&recovered, signer.verifying_key());
    }
}
