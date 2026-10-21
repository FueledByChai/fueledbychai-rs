//! QFEX HMAC authentication signature. The wire format is documented at
//! <https://docs.qfex.com/websocket/channels/trade/authenticate>.
//!
//! The caller must supply a cryptographically random, unique nonce via its
//! `NonceSource`; the signer itself reads neither a clock nor an RNG. Two
//! journaled `u64` values form a 128-bit nonce, rendered as 32 lowercase
//! hexadecimal digits as QFEX's official CLI does. Never
//! log or persist the returned signature in plain text.

use core::fmt;
use std::collections::HashSet;

use fbc_core::{NonceBlock, NonceSource, Secret, SignError, WallNs};
use hmac::{Hmac, Mac};
use sha2::Sha256;

/// Signed fields for one QFEX HMAC authentication frame.
pub struct SignedHmac {
    nonce: String,
    unix_ts: u64,
    signature: String,
}

impl SignedHmac {
    pub fn nonce(&self) -> &str {
        &self.nonce
    }

    pub fn unix_ts(&self) -> u64 {
        self.unix_ts
    }

    /// Credential-equivalent for the venue's authentication window.
    pub fn signature(&self) -> &str {
        &self.signature
    }
}

impl fmt::Debug for SignedHmac {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SignedHmac")
            .field("nonce", &"<redacted>")
            .field("unix_ts", &self.unix_ts)
            .field("signature", &"<redacted>")
            .finish()
    }
}

/// Sign `${nonce}:${unix_ts}` with HMAC-SHA256 using the secret's UTF-8 bytes.
/// The caller supplies the journaled nonce and wall clock from `EncodeCtx`.
pub fn sign(secret: &Secret, nonce: [u64; 2], wall: WallNs) -> Result<SignedHmac, SignError> {
    if wall.0 < 0 {
        return Err(SignError::Unsignable("wall time before Unix epoch"));
    }
    let unix_ts = u64::try_from(wall.0 / 1_000_000_000)
        .map_err(|_| SignError::Unsignable("wall time before Unix epoch"))?;
    let nonce = format!("{:016x}{:016x}", nonce[0], nonce[1]);
    let message = format!("{nonce}:{unix_ts}");
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.expose().as_bytes())
        .map_err(|_| SignError::Backend("HMAC initialization"))?;
    mac.update(message.as_bytes());
    let bytes = mac.finalize().into_bytes();
    let mut signature = String::with_capacity(64);
    for byte in bytes {
        use fmt::Write as _;
        write!(&mut signature, "{byte:02x}").map_err(|_| SignError::Backend("hex encoding"))?;
    }
    Ok(SignedHmac {
        nonce,
        unix_ts,
        signature,
    })
}

/// Entropy for a nonce source. Tests inject fixed bytes; production uses the OS.
pub trait Entropy: Send {
    fn fill(&mut self, bytes: &mut [u8]) -> Result<(), ()>;
}

/// The operating system's cryptographic random source.
#[derive(Copy, Clone, Debug, Default)]
pub struct OsEntropy;

impl Entropy for OsEntropy {
    fn fill(&mut self, bytes: &mut [u8]) -> Result<(), ()> {
        getrandom::fill(bytes).map_err(|_| ())
    }
}

/// Random authentication nonces, unique within this source's process lifetime.
/// A failure or repeated collision returns an empty block: `ExecSession` rejects
/// the block length and ends the session before writing an authentication frame.
pub struct QfexNonces<R = OsEntropy> {
    rng: R,
    issued: HashSet<u64>,
}

impl QfexNonces<OsEntropy> {
    pub fn os() -> Self {
        Self::with_entropy(OsEntropy)
    }
}

impl<R: Entropy> QfexNonces<R> {
    pub fn with_entropy(rng: R) -> Self {
        Self {
            rng,
            issued: HashSet::new(),
        }
    }
}

impl<R: Entropy> NonceSource for QfexNonces<R> {
    fn reserve(&mut self, len: u16) -> NonceBlock {
        let mut values = Vec::with_capacity(usize::from(len));
        for _ in 0..len {
            let mut found = None;
            for _ in 0..16 {
                let mut bytes = [0u8; 8];
                if self.rng.fill(&mut bytes).is_err() {
                    return NonceBlock::EMPTY;
                }
                let value = u64::from_be_bytes(bytes);
                if self.issued.insert(value) {
                    found = Some(value);
                    break;
                }
            }
            let Some(value) = found else {
                return NonceBlock::EMPTY;
            };
            values.push(value);
        }
        NonceBlock::new(values)
    }
}
