//! `mainnet_smoke`'s credentials (decision 0009: a review path; the owner reviews every change
//! here). Included by path by `mainnet_smoke` and its test; not a module of this crate's library
//! target, which stays empty with no dependency.
//!
//! The Paradex MAINNET account is read from two environment variables of its own,
//! [`ACCOUNT_VAR`] and [`KEY_VAR`], never the testnet samples' (`PARADEX_ACCOUNT_ADDRESS`,
//! `PARADEX_PRIVATE_KEY`, read by `src/auth.rs`), so a testnet key can never sign a mainnet
//! login by accident: a missing mainnet variable is never filled from a testnet one, and a shell
//! that has either testnet variable set refuses the run until it is unset, so the two accounts
//! are never in the environment of one run. Each value moves straight into fbc-core's
//! [`Secret`] (zeroed on drop, a `Debug` and `Display` that show no value), keyed as the Paradex
//! adapter reads them. Nothing here prints, logs or formats a value: a refusal names variables
//! only. The values are checked for shape by the adapter's src/auth when the session's codec is
//! built.

use fbc_core::{Secret, Secrets};
use fbc_venue_paradex::auth::{ACCOUNT_ADDRESS, SIGNING_KEY};

/// The environment variable holding the Paradex MAINNET account address (`0x` and hex digits).
pub const ACCOUNT_VAR: &str = "PARADEX_MAINNET_ACCOUNT_ADDRESS";

/// The environment variable holding the mainnet account's own Stark private key (`0x` and hex
/// digits), the main key: a trading subkey's login is not built yet (FBC-2vhg).
pub const KEY_VAR: &str = "PARADEX_MAINNET_PRIVATE_KEY";

/// The testnet samples' variables: either one set refuses a mainnet run.
pub const TESTNET_VARS: [&str; 2] = ["PARADEX_ACCOUNT_ADDRESS", "PARADEX_PRIVATE_KEY"];

/// The Paradex mainnet credentials from `lookup` (the process environment when run; a map under
/// test): [`ACCOUNT_VAR`] and [`KEY_VAR`], each moved into a [`Secret`]. Refused, naming
/// variables and never a value, when a [`TESTNET_VARS`] variable is set (not empty), or a
/// mainnet one is missing or empty.
pub fn paradex_mainnet_secrets(lookup: impl Fn(&str) -> Option<String>) -> Result<Secrets, String> {
    let set = |var: &str| lookup(var).is_some_and(|v| !v.is_empty());
    if let Some(var) = TESTNET_VARS.iter().find(|var| set(var)) {
        return Err(format!(
            "{var} is set: it is the testnet samples' variable, and mainnet_smoke never runs \
             with a testnet credential in its environment; unset {} and {} in this shell \
             (mainnet_smoke reads only {ACCOUNT_VAR} and {KEY_VAR})",
            TESTNET_VARS[0], TESTNET_VARS[1]
        ));
    }
    let mut secrets = Secrets::new();
    for (var, key) in [(ACCOUNT_VAR, ACCOUNT_ADDRESS), (KEY_VAR, SIGNING_KEY)] {
        let value = lookup(var).filter(|v| !v.is_empty()).ok_or_else(|| {
            format!(
                "{var} is not set: export the SEPARATE mainnet account's before running \
                 mainnet_smoke"
            )
        })?;
        secrets.insert(key, Secret::new(value));
    }
    Ok(secrets)
}
