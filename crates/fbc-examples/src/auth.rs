//! The owner-run samples' credentials (decision 0009: a review path; the owner reviews every
//! change here). Included by path by the samples that need them (`testnet_trade`) and by their
//! tests; it is not a module of this crate's library target, which stays empty with no
//! dependency (every dependency of `fbc-examples` is a dev-dependency).
//!
//! A sample reads the Paradex account it trades from two environment variables, named as the
//! Java library names them, and moves each value straight into fbc-core's [`Secret`] (zeroed on
//! drop, a `Debug` and `Display` that show no value), keyed as the Paradex adapter reads them
//! ([`ACCOUNT_ADDRESS`], [`SIGNING_KEY`]). Nothing here prints, logs or formats a value: a
//! missing or empty variable is refused naming the variable alone. The values are checked for
//! shape by the adapter's src/auth when the session's codec is built, which also names the key
//! and never the value.

use fbc_core::{Secret, Secrets};
use fbc_venue_paradex::auth::{ACCOUNT_ADDRESS, SIGNING_KEY};

/// The environment variable holding the Paradex account address (`0x` and hex digits).
pub const ACCOUNT_VAR: &str = "PARADEX_ACCOUNT_ADDRESS";

/// The environment variable holding the account's own Stark private key (`0x` and hex digits),
/// the main key: a trading subkey's login is not built yet (FBC-2vhg).
pub const KEY_VAR: &str = "PARADEX_PRIVATE_KEY";

/// The Paradex credentials from `lookup` (the process environment when run; a map under test):
/// [`ACCOUNT_VAR`] and [`KEY_VAR`], each moved into a [`Secret`]. Refused naming the first
/// variable that is missing or empty, never a value.
pub fn paradex_secrets(lookup: impl Fn(&str) -> Option<String>) -> Result<Secrets, String> {
    let mut secrets = Secrets::new();
    for (var, key) in [(ACCOUNT_VAR, ACCOUNT_ADDRESS), (KEY_VAR, SIGNING_KEY)] {
        let value = lookup(var)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| format!("{var} is not set: export it before running the sample"))?;
        secrets.insert(key, Secret::new(value));
    }
    Ok(secrets)
}
