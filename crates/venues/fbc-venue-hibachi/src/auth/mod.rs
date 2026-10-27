//! Hibachi credentials (decision 0009: a review path; the owner reviews every change here).
//!
//! - [`order_signer`] is the order-entry codec's signer: the trustless account's secp256k1 key,
//!   read from the consumer's [`Secrets`] (decision 0043) without taking it, checked, and handed
//!   over as an initialized [`HibachiSigner`], as Paradex's `auth::order_signer` hands over its
//!   signer. The codec that calls it is FBC-x7bi's; until then the factory takes no credential.
//!
//! The account's API key, which Hibachi's requests and sockets carry as a header, is FBC-bn8j's.
//! Exec code calls these by name; the words the credential placement check looks for stay in
//! this module and in `sign` (0009).

use fbc_core::{ConfigError, ConfigScope, FieldSpec, FieldUnit, Secrets, VenueError};

use crate::sign::{EcdsaKey, HibachiSigner, KeyError};

/// The credential key of the trustless account's secp256k1 private key, `0x` and 64 hex digits,
/// as the Java library names it (`HibachiConfiguration.HIBACHI_PRIVATE_KEY`).
pub const SIGNING_KEY: &str = "hibachi.private.key";

/// The signing key's schema entry; the consumer hands its value over as a credential. The
/// factory lists it once order entry lands (FBC-x7bi).
pub const SIGNING_KEY_FIELD: FieldSpec = FieldSpec {
    key: SIGNING_KEY,
    scope: ConfigScope::Account,
    unit: FieldUnit::Dimensionless,
    doc: "credential: the trustless account's secp256k1 private key (0x and 64 hex digits), \
          handed over in Secrets; it signs every order, cancel and cancel-all",
};

/// The order signer for the account `creds` hold: its key read without being taken, so `creds`
/// can then go on to whatever else needs them. Refused naming a missing or invalid key, never a
/// value: a key not `0x` and 64 hex digits, or not a secp256k1 key in range.
pub fn order_signer(creds: &Secrets) -> Result<HibachiSigner, VenueError> {
    let text = creds
        .get(SIGNING_KEY)
        .ok_or(ConfigError::Missing(SIGNING_KEY))?;
    let key = EcdsaKey::from_hex(text.expose()).map_err(|err| ConfigError::Invalid {
        key: SIGNING_KEY,
        reason: match err {
            KeyError::NotHex => "not 0x and 64 hex digits",
            KeyError::OutOfRange => "not a secp256k1 key in range",
        },
    })?;
    Ok(HibachiSigner::new(key))
}
