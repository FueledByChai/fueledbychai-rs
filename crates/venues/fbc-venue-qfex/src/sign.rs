//! The QFEX signer (decision 0009: a review path; the owner reviews every change here).
//!
//! QFEX authenticates a trade WebSocket with one HMAC signature, by the rule its
//! [authentication page](https://docs.qfex.com/websocket/channels/trade/authenticate) states:
//!
//! - the nonce is cryptographically secure random, hex encoded, at most 100 characters, and
//!   unique within a 15-minute window;
//! - the message is the nonce, a colon and the current Unix time in whole seconds,
//!   `${nonce}:${unix_ts}`;
//! - the signature is HMAC-SHA256 of the message, keyed with the API secret's UTF-8 bytes,
//!   written as lowercase hex (the page's Python example computes it this way).
//!
//! [`HmacKey::sign`] computes that signature. It reads no clock and draws no nonce: its time is
//! the `EncodeCtx`'s wall time, floored to the whole second, and its nonce is the two `u64`
//! values the runtime reserved for the call and journaled, written as 32 lowercase hex digits,
//! 16 per value, zero padded (128 random bits, well within the 100-character limit).
//! [`QfexNonces`] is where the runtime reserves them: values from the operating system's
//! random source, none repeated among the last [`QfexNonces::WINDOW`] it issued, in memory
//! that stays bounded however many it issues.
//!
//! `fixtures/qfex/signing/hmac-vectors.json`, written by `hmac_oracle.py` with Python's own
//! `hmac` and `hashlib`, is what the tests hold the signature to.
//!
//! Only `src/auth` calls this module: it takes the secret out of the consumer's credentials,
//! hands it to [`HmacKey::new`], and builds the authentication frame around a signature, so
//! no codec ever holds a secret itself (decision 0043).
//!
//! # Key copies
//!
//! What is wiped: the secret stays in the [`Secret`] the consumer built and `src/auth` moved
//! here, whose buffer is overwritten with zeros when the key drops (fbc-core, `zeroize`).
//!
//! What is not, and why: each signature keys a fresh HMAC-SHA256 state from the secret's
//! bytes (hmac 0.12.1 and sha2 0.10.9, pinned exactly). That state holds the key's inner and
//! outer pads, which are as good as the key, on the stack for the one call, and hmac 0.12
//! drops it unwiped; it has no `zeroize` feature to ask for. A moved value can also leave a
//! copy in a stack slot or register that no code can name. Wiping the pads would mean
//! re-implementing HMAC here, which the vectors pin and this module does not.

use core::fmt;
use std::collections::{HashSet, VecDeque};

use fbc_core::{NonceBlock, NonceSource, Secret, SignError, WallNs};
use hmac::{Hmac, Mac};
use sha2::Sha256;

/// The nanoseconds in a second, for the whole-second floor.
const NANOS_PER_SECOND: u64 = 1_000_000_000;

/// The HMAC key of one QFEX API credential: its secret, held as the consumer gave it. It is
/// never printed (`Debug` shows only that a key is there) and is not `Clone`, so it lives where
/// `src/auth` put it. What is wiped when it drops, and what is not, is in the module's
/// [key copies](self#key-copies).
pub struct HmacKey {
    secret: Secret,
}

/// Why a secret is not an HMAC key; never carries the secret.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub enum KeyError {
    /// The secret is empty: every signature it made would be the same for everyone.
    Empty,
}

impl fmt::Display for KeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KeyError::Empty => f.write_str("an empty secret"),
        }
    }
}

impl std::error::Error for KeyError {}

impl HmacKey {
    /// The key for `secret`, taken by move so no other copy of it is made. Refused when the
    /// secret is empty; QFEX documents no other rule for a secret, and its UTF-8 bytes are the
    /// key whatever they are.
    pub fn new(secret: Secret) -> Result<HmacKey, KeyError> {
        if secret.expose().is_empty() {
            return Err(KeyError::Empty);
        }
        Ok(HmacKey { secret })
    }

    /// Signs one authentication: the nonce `nonce` (two journaled values, item 0 first) at
    /// `wall`, floored to its second. Refused, signing nothing, for a wall time before the
    /// Unix epoch, which no Unix timestamp can state.
    pub fn sign(&self, nonce: [u64; 2], wall: WallNs) -> Result<SignedAuth, SignError> {
        let unix_ts = unix_seconds(wall)?;
        let nonce = nonce_hex(nonce);
        let message = format!("{nonce}:{unix_ts}");
        // HMAC takes a key of any length, so keying never fails.
        let mut mac = Hmac::<Sha256>::new_from_slice(self.secret.expose().as_bytes())
            .expect("HMAC takes a key of any length");
        mac.update(message.as_bytes());
        let signature = lower_hex(&mac.finalize().into_bytes());
        Ok(SignedAuth {
            nonce,
            unix_ts,
            signature,
        })
    }
}

impl fmt::Debug for HmacKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HmacKey(<redacted>)")
    }
}

/// The signed fields of one authentication frame: the nonce as sent, the Unix time in whole
/// seconds, and the signature. The signature authenticates a connection for as long as QFEX
/// accepts the nonce and time, so it is as sensitive as the session it opens: `Debug` never
/// shows it, and `src/auth` writes it only inside a redaction span.
pub struct SignedAuth {
    nonce: String,
    unix_ts: u64,
    signature: String,
}

impl SignedAuth {
    /// The nonce, 32 lowercase hex digits.
    pub fn nonce(&self) -> &str {
        &self.nonce
    }

    /// The Unix time, in whole seconds.
    pub fn unix_ts(&self) -> u64 {
        self.unix_ts
    }

    /// The signature, 64 lowercase hex digits. Only a redaction span may carry it.
    pub fn signature(&self) -> &str {
        &self.signature
    }
}

impl fmt::Debug for SignedAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SignedAuth")
            .field("nonce", &self.nonce)
            .field("unix_ts", &self.unix_ts)
            .field("signature", &"<redacted>")
            .finish()
    }
}

/// `wall` in whole seconds since the Unix epoch, rounded down; refused before the epoch.
fn unix_seconds(wall: WallNs) -> Result<u64, SignError> {
    let nanos = u64::try_from(wall.0)
        .map_err(|_| SignError::Unsignable("a wall time before the Unix epoch"))?;
    Ok(nanos / NANOS_PER_SECOND)
}

/// The two values as 32 lowercase hex digits, each zero padded to 16.
fn nonce_hex(nonce: [u64; 2]) -> String {
    format!("{:016x}{:016x}", nonce[0], nonce[1])
}

/// `bytes` as lowercase hex, two digits a byte.
fn lower_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(DIGITS[usize::from(byte >> 4)]));
        out.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    out
}

/// Where a [`QfexNonces`] draws its random bytes: the operating system in production
/// ([`OsEntropy`]); a test scripts its own.
pub trait Entropy: Send {
    /// Fills `bytes` with random bytes, or says it could not.
    fn fill(&mut self, bytes: &mut [u8]) -> Result<(), EntropyFailed>;
}

/// The random source could not give bytes; nothing is drawn from a partial fill.
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug)]
pub struct EntropyFailed;

/// The operating system's cryptographically secure random source (getrandom 0.3.4, pinned
/// exactly).
#[derive(Copy, Clone, Eq, PartialEq, Hash, Debug, Default)]
pub struct OsEntropy;

impl Entropy for OsEntropy {
    fn fill(&mut self, bytes: &mut [u8]) -> Result<(), EntropyFailed> {
        getrandom::fill(bytes).map_err(|_| EntropyFailed)
    }
}

/// The nonce source for QFEX authentication (decision 0009; FBC-77aa).
///
/// Each value is 64 bits from its [`Entropy`]; an authentication takes two, so its nonce is
/// 128 random bits, and two nonces from a working source collide with odds of about one in
/// 2^128 a pair. On top of that, no value is issued that is among the last
/// [`WINDOW`](QfexNonces::WINDOW) it issued: a draw that repeats one is drawn again, up to
/// [`DRAWS`](QfexNonces::DRAWS) times, so a source that repeats itself (a stuck generator) is
/// caught rather than trusted.
///
/// It remembers only those last values: memory stays bounded however many nonces a session
/// reserves (the runtime reserves on every encode, an order's included, not only on each
/// authentication), and a value issued more than `WINDOW` values ago may be issued again,
/// with the odds above.
///
/// A block it cannot fill whole, because the source failed or kept repeating, is refused as an
/// empty block, with nothing remembered from it: the runtime then ends the session before
/// anything is signed or written (fbc-runtime's `ExecSessionError::Nonces`).
pub struct QfexNonces<E = OsEntropy> {
    entropy: E,
    /// The values issued, oldest first: at most `WINDOW` once a reservation returns.
    issued: VecDeque<u64>,
    /// The same values, to look one up.
    recent: HashSet<u64>,
}

impl QfexNonces<OsEntropy> {
    /// The production source, over the operating system's random source.
    pub fn os() -> QfexNonces<OsEntropy> {
        QfexNonces::with_entropy(OsEntropy)
    }
}

impl<E> QfexNonces<E> {
    /// How many of the latest values a new one is checked against, and so the most this
    /// source remembers: 4,096 values, 2,048 authentications. QFEX asks for a nonce unique
    /// within 15 minutes, and a session authenticates once per connection.
    pub const WINDOW: usize = 4096;

    /// How many times one value is drawn before the source is taken to be failing.
    pub const DRAWS: usize = 16;

    /// A source over `entropy`.
    pub fn with_entropy(entropy: E) -> QfexNonces<E> {
        QfexNonces {
            entropy,
            issued: VecDeque::with_capacity(Self::WINDOW),
            recent: HashSet::with_capacity(Self::WINDOW),
        }
    }

    /// How many values it remembers now: never more than [`WINDOW`](QfexNonces::WINDOW).
    pub fn remembered(&self) -> usize {
        self.issued.len()
    }
}

impl<E: Entropy> QfexNonces<E> {
    /// One value not among those remembered, nor among `held` (the block being reserved), or
    /// `None` when the source fails or repeats itself `DRAWS` times.
    fn draw(&mut self, held: &HashSet<u64>) -> Option<u64> {
        for _ in 0..Self::DRAWS {
            let mut bytes = [0u8; 8];
            self.entropy.fill(&mut bytes).ok()?;
            let value = u64::from_be_bytes(bytes);
            if !self.recent.contains(&value) && !held.contains(&value) {
                return Some(value);
            }
        }
        None
    }

    /// Remembers `value` as issued, forgetting the oldest once more than `WINDOW` are held.
    fn remember(&mut self, value: u64) {
        self.issued.push_back(value);
        self.recent.insert(value);
        while self.issued.len() > Self::WINDOW {
            if let Some(oldest) = self.issued.pop_front() {
                self.recent.remove(&oldest);
            }
        }
    }
}

impl<E: Entropy> NonceSource for QfexNonces<E> {
    fn reserve(&mut self, len: u16) -> NonceBlock {
        let mut values = Vec::with_capacity(usize::from(len));
        let mut held = HashSet::with_capacity(usize::from(len));
        for _ in 0..len {
            match self.draw(&held) {
                Some(value) => {
                    held.insert(value);
                    values.push(value);
                }
                // Never part of a block: nothing from this one is issued or remembered.
                None => return NonceBlock::EMPTY,
            }
        }
        for &value in &values {
            self.remember(value);
        }
        NonceBlock::new(values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Entropy that gives the scripted values in order, then fails.
    struct Scripted(VecDeque<u64>);

    impl Entropy for Scripted {
        fn fill(&mut self, bytes: &mut [u8]) -> Result<(), EntropyFailed> {
            let value = self.0.pop_front().ok_or(EntropyFailed)?;
            bytes.copy_from_slice(&value.to_be_bytes());
            Ok(())
        }
    }

    fn scripted(values: &[u64]) -> QfexNonces<Scripted> {
        QfexNonces::with_entropy(Scripted(values.iter().copied().collect()))
    }

    #[test]
    fn a_repeated_draw_is_drawn_again_within_and_across_blocks() {
        // 5 repeats within the block and 7 repeats the block before.
        let mut source = scripted(&[7, 5, 5, 9, 7, 8]);
        assert_eq!(source.reserve(1).as_slice(), &[7]);
        assert_eq!(source.reserve(3).as_slice(), &[5, 9, 8]);
        assert_eq!(source.remembered(), 4);
    }

    #[test]
    fn a_failing_or_stuck_source_gives_no_block_and_remembers_nothing_of_it() {
        let mut failed = scripted(&[1]);
        assert!(failed.reserve(2).is_empty(), "never part of a block");
        assert_eq!(failed.remembered(), 0);
        // Stuck: the first 3 is issued, then sixteen more draws repeat it.
        let mut stuck = scripted(&[3; 1 + QfexNonces::<Scripted>::DRAWS]);
        assert_eq!(stuck.reserve(1).as_slice(), &[3]);
        assert!(stuck.reserve(1).is_empty());
        assert_eq!(stuck.remembered(), 1);
        // Nothing asked, nothing drawn.
        assert!(scripted(&[]).reserve(0).is_empty());
    }

    #[test]
    fn the_os_source_fills_a_block_of_distinct_values() {
        let mut os = QfexNonces::os();
        let block = os.reserve(2);
        assert_eq!(block.len(), 2);
        assert_ne!(block.as_slice()[0], block.as_slice()[1]);
        let mut bytes = [0u8; 32];
        assert_eq!(OsEntropy.fill(&mut bytes), Ok(()));
    }

    #[test]
    fn hex_and_floor_helpers() {
        assert_eq!(lower_hex(&[0x00, 0x0f, 0xa0, 0xff]), "000fa0ff");
        assert_eq!(nonce_hex([0, u64::MAX]).len(), 32);
        assert_eq!(unix_seconds(WallNs(1_999_999_999)), Ok(1));
        assert!(unix_seconds(WallNs(i64::MIN)).is_err());
        assert_eq!(KeyError::Empty.to_string(), "an empty secret");
    }
}
