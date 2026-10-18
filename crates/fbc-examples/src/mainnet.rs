//! What the Paradex MAINNET samples share (`mainnet_smoke`, FBC-p0aa; `testnet_quote --mainnet`,
//! FBC-tyft): the mainnet guard and the checks it rests on, run before anything connects, and
//! the chain check run before the order session connects. Included by path by each sample and
//! its test, beside [`testnet`](crate::testnet), and not a module of this crate's library target,
//! which stays empty with no dependency. The credentials are `src/auth_mainnet.rs`'s (a review
//! path), never read here.
//!
//! - **The URLs** ([`mainnet_urls`]): exactly Paradex mainnet's REST base and order-entry
//!   WebSocket ([`MAINNET_REST`], [`MAINNET_WS`]), or both a loopback stub's at `/v1`.
//! - **The chain id** ([`mainnet_guard`]): `PARADEX_CHAIN_ID`, when set, mainnet's
//!   ([`MAINNET_CHAIN`], by name or as its felt in decimal or hex); and at start
//!   `GET /system/config` must name it ([`decode_chain`], [`chain_refusal`]), the chain the
//!   login signs for.
//! - **The caps** ([`within_mainnet_caps`]): at most $11 resting per side and $50 inventory (the
//!   owner's caps; smaller are taken).
//! - **The snapshot** ([`trusted`]): the venue must declare its open-order snapshot
//!   trustworthy, so a resync seeds the market's position: a mainnet market is armed only after
//!   a trustworthy resync and never seeded by hand (owner answer C to RB-olg-3; hand-seeding is
//!   for an owner-assisted testnet run only, decision 0067). Paradex's adapter declares it
//!   trustworthy (decision 0094).
//!
//! The mainnet samples keep their leases and the client-id high-water mark in one directory
//! ([`LEASE_DIR_NAME`] under `$HOME/.fueledbychai`) under the account label [`LABEL`], so two of
//! them never trade one market or namespace of the mainnet account at once, and a mainnet run
//! never shares a testnet sample's high-water mark.

use fbc_core::{SnapshotSource, VenueConfig, VenueFactory};
use rust_decimal::Decimal;

use crate::testnet::{CHAIN_VAR, config_on, exact_or_stub};

/// Paradex mainnet's REST base and order-entry WebSocket: the only non-loopback URLs admitted,
/// exactly.
pub const MAINNET_REST: &str = "https://api.prod.paradex.trade/v1";
pub const MAINNET_WS: &str = "wss://ws.api.prod.paradex.trade/v1";

/// Paradex mainnet's Starknet chain id: the `starknet_chain_id` of GET
/// https://api.prod.paradex.trade/v1/system/config (verified 2026-10-09), the one the login
/// signs for, and the one `GET /system/config` must name at start.
pub const MAINNET_CHAIN: &str = "PRIVATE_SN_PARACLEAR_MAINNET";

/// [`MAINNET_CHAIN`]'s felt (its ASCII bytes as one big-endian number) in decimal and in hex,
/// lower case without `0x`.
pub const MAINNET_CHAIN_DECIMAL: &str =
    "8458834024819506728615521019831122032732688838300957472069977523540";
pub const MAINNET_CHAIN_HEX: &str = "505249564154455f534e5f50415241434c4541525f4d41494e4e4554";

/// The account's name in the lease files of a mainnet run: a label, never the address.
pub const LABEL: &str = "mainnet";

/// The directory under `$HOME/.fueledbychai` where the mainnet samples keep their leases and
/// the client-id high-water mark: `mainnet_smoke`'s, where the owner's first mainnet order left
/// its mark, shared by `testnet_quote --mainnet`.
pub const LEASE_DIR_NAME: &str = "mainnet_smoke";

/// The largest resting cap per side and inventory cap, in USD, a mainnet run takes (the
/// owner's caps); smaller ones are taken.
pub const RESTING_CAP_MAX: Decimal = Decimal::from_parts(11, 0, 0, false, 0);
pub const INVENTORY_CAP_MAX: Decimal = Decimal::from_parts(50, 0, 0, false, 0);

/// The usual causes of a refused mainnet login, printed with the refusal.
pub const REFUSAL_CAUSES: &str = "the usual causes: a testnet key on mainnet (Paradex derives the \
     account's L2 key per network), an Ethereum address instead of the Paradex (Starknet) one in \
     PARADEX_MAINNET_ACCOUNT_ADDRESS, or an account not onboarded on mainnet, or unfunded";

/// Whether `chain` names mainnet's chain: [`MAINNET_CHAIN`] by name, or its felt in decimal or
/// in `0x` hex (either case).
pub fn is_mainnet_chain(chain: &str) -> bool {
    let hex = chain
        .strip_prefix("0x")
        .or_else(|| chain.strip_prefix("0X"))
        .map(str::to_ascii_lowercase);
    chain == MAINNET_CHAIN
        || chain == MAINNET_CHAIN_DECIMAL
        || hex.as_deref() == Some(MAINNET_CHAIN_HEX)
}

/// The URL half of [`mainnet_guard`]: both URLs exactly mainnet's, or both a loopback stub's at
/// `/v1` (true). A refusal never shows what was typed.
pub fn mainnet_urls(rest: &str, ws: &str) -> Result<bool, String> {
    exact_or_stub(rest, ws, "mainnet", MAINNET_REST, MAINNET_WS)
}

/// The mainnet guard of `program`, run before anything connects: [`mainnet_urls`], and the
/// chain id (`PARADEX_CHAIN_ID` when set) mainnet's. True for a loopback stub.
pub fn mainnet_guard(
    rest: &str,
    ws: &str,
    chain: Option<&str>,
    program: &str,
) -> Result<bool, String> {
    let stub = mainnet_urls(rest, ws)?;
    match chain {
        Some(chain) if !is_mainnet_chain(chain) => Err(format!(
            "{CHAIN_VAR}: not the mainnet chain id {MAINNET_CHAIN} (the value is not shown: it \
             may be a secret); {program} never signs for another chain"
        )),
        _ => Ok(stub),
    }
}

/// Refuses caps above the owner's mainnet caps ([`RESTING_CAP_MAX`] per side,
/// [`INVENTORY_CAP_MAX`]); smaller ones are taken.
pub fn within_mainnet_caps(resting: Decimal, inventory: Decimal) -> Result<(), String> {
    if resting > RESTING_CAP_MAX {
        return Err(format!(
            "--resting-cap-usd {resting} is above the mainnet cap: at most ${RESTING_CAP_MAX} \
             per side"
        ));
    }
    if inventory > INVENTORY_CAP_MAX {
        return Err(format!(
            "--inventory-cap-usd {inventory} is above the mainnet cap: at most \
             ${INVENTORY_CAP_MAX}"
        ));
    }
    Ok(())
}

/// Refuses a venue whose order caps under `cfg` do not declare its open-order snapshot
/// trustworthy: no resync of it can seed the market, and `program` never seeds one by hand on
/// mainnet.
pub fn trusted(factory: &dyn VenueFactory, cfg: &VenueConfig, program: &str) -> Result<(), String> {
    let caps = factory.caps(cfg).map_err(|e| e.to_string())?;
    let declared = match caps.exec.map(|exec| exec.order.snapshot_source) {
        Some(SnapshotSource::Trustworthy) => return Ok(()),
        Some(SnapshotSource::Untrustworthy) => "untrustworthy",
        Some(SnapshotSource::None) => "absent",
        None => "absent (no order entry)",
    };
    Err(format!(
        "Paradex's snapshot source is declared {declared}, not trustworthy, so no resync can \
         seed the market's position, and a mainnet market is never seeded by hand (only an \
         owner-assisted testnet run may be, decision 0067): {program} runs on mainnet only on a \
         venue declaring it Trustworthy (Paradex's adapter does, decision 0094); nothing \
         connected"
    ))
}

/// The Paradex configuration for the URLs `rest` and `ws`, the login signing for
/// [`MAINNET_CHAIN`].
pub fn config(rest: &str, ws: &str) -> VenueConfig {
    config_on(rest, ws, MAINNET_CHAIN)
}

/// The `starknet_chain_id` a `GET /system/config` answer names.
pub fn decode_chain(body: &[u8]) -> Result<String, String> {
    let config: serde_json::Value = serde_json::from_slice(body)
        .map_err(|_| "GET /system/config: not a JSON answer".to_owned())?;
    config["starknet_chain_id"]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| "GET /system/config: no starknet_chain_id".to_owned())
}

/// Why a `GET /system/config` naming `venue_chain` refuses the run: any chain but
/// [`MAINNET_CHAIN`] (`None` for mainnet's).
pub fn chain_refusal(venue_chain: &str) -> Option<String> {
    (venue_chain != MAINNET_CHAIN).then(|| {
        format!(
            "GET /system/config names the chain {}, not {MAINNET_CHAIN}: the REST base is not \
             Paradex mainnet's, or Paradex renamed its chain; nothing sent",
            shown_chain(venue_chain)
        )
    })
}

/// A chain id the venue named, as a refusal shows it: only when shaped like one (upper-case
/// letters, digits and '_', at most 64), so an answer that carries something else never prints.
fn shown_chain(chain: &str) -> &str {
    let shaped = (1..=64).contains(&chain.len())
        && chain
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_');
    if shaped {
        chain
    } else {
        "(not shown: not shaped like a chain id)"
    }
}
