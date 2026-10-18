//! mainnet_smoke's guards and its run: testnet_trade's round trip ([`crate::trade::run_on`]) on
//! Paradex MAINNET, never seeded by hand.
//!
//! **Before anything connects** ([`run`]): both confirmations (`--mainnet`, `--sole-trader`) and
//! the smoke's caps (at most $11 resting per side and $50 inventory,
//! [`crate::smoke_args::confirmed`]); the URLs exactly mainnet's ([`MAINNET_REST`],
//! [`MAINNET_WS`]) or both a loopback stub's; `PARADEX_CHAIN_ID`, when set, mainnet's; and the
//! venue's declared snapshot source trustworthy ([`trusted`]). Paradex's adapter declares it
//! trustworthy (decision 0094, accepted by the owner 2026-10-16); a venue declaring otherwise
//! stops here: a mainnet market is armed only after a trustworthy resync (owner answer C to
//! RB-olg-3), and hand-seeding is for an owner-assisted testnet run only (decision 0067).
//!
//! **Then, each step timed out**: `GET /system/config` must name [`MAINNET_CHAIN`], the chain the
//! login signs for ([`decode_chain`]); `GET /markets/summary` gives the market's mark price
//! ([`decode_mark`]), where Paradex judges an order's minimum notional (it refuses one below it
//! only after accepting the place: the owner's 2026-10-16 testnet run), so the order must reach
//! the minimum at the mark before anything is sent. Then testnet_trade's round trip: the touch,
//! the order priced and sized, login, cancel-on-disconnect, the resync (it must be trustworthy
//! and the account flat on the market, [`crate::trade::start_refusal`]), Start, one post-only
//! order well away from the touch, its ack, its cancel, the cancel's ack, and Stop (the kill
//! switch and, after a trustworthy resync, an instrument cancel-all of the market).

use std::time::{Duration, Instant};

use fbc_core::{SnapshotSource, VenueConfig, VenueFactory};
use rust_decimal::Decimal;

use crate::args::Options;
use crate::smoke_args::{Smoke, confirmed};
use crate::testnet::{CHAIN_VAR, Lines, Out, config_on, exact_or_stub, hosts, rest_get};
use crate::trade::{Network, Report, run_on};

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

/// The account's name in the lease files, and the program a held lease's refusal names.
pub const LABEL: &str = "mainnet";
pub const PROGRAM: &str = "mainnet_smoke";

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

/// The mainnet guard, run before anything connects: [`mainnet_urls`], and the chain id
/// (`PARADEX_CHAIN_ID` when set) mainnet's. True for a loopback stub.
pub fn mainnet_guard(rest: &str, ws: &str, chain: Option<&str>) -> Result<bool, String> {
    let stub = mainnet_urls(rest, ws)?;
    match chain {
        Some(chain) if !is_mainnet_chain(chain) => Err(format!(
            "{CHAIN_VAR}: not the mainnet chain id {MAINNET_CHAIN} (the value is not shown: it \
             may be a secret); mainnet_smoke never signs for another chain"
        )),
        _ => Ok(stub),
    }
}

/// Refuses a venue whose order caps under `cfg` do not declare its open-order snapshot
/// trustworthy: no resync of it can seed the market, and this run never seeds one by hand.
pub fn trusted(factory: &dyn VenueFactory, cfg: &VenueConfig) -> Result<(), String> {
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
         owner-assisted testnet run may be, decision 0067): mainnet_smoke runs only on a venue \
         declaring it Trustworthy (Paradex's adapter does, decision 0094); nothing connected"
    ))
}

/// The Paradex configuration for `opts`' URLs, the login signing for [`MAINNET_CHAIN`].
pub fn config(opts: &Options) -> VenueConfig {
    config_on(&opts.rest_url, &opts.ws_url, MAINNET_CHAIN)
}

/// The round trip's network: `factory`, mainnet's lease label, never seeded by hand, the order
/// judged at `mark`.
pub fn network(factory: &'static dyn VenueFactory, mark: Decimal) -> Network {
    Network {
        factory,
        label: LABEL,
        program: PROGRAM,
        owner_assisted: false,
        mark: Some(mark),
        refusal_causes: REFUSAL_CAUSES,
    }
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

/// The mark price of `market` in a `GET /markets/summary?market=` answer
/// (`{"results":[{"symbol", "mark_price", ..}]}`): a positive decimal, or refused.
pub fn decode_mark(body: &[u8], market: &str) -> Result<Decimal, String> {
    let summary: serde_json::Value = serde_json::from_slice(body)
        .map_err(|_| "GET /markets/summary: not a JSON answer".to_owned())?;
    let mark = summary["results"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|s| s["symbol"].as_str() == Some(market))
        .ok_or("GET /markets/summary: no summary of the market")?["mark_price"]
        .as_str()
        .and_then(|m| m.parse::<Decimal>().ok())
        .ok_or("GET /markets/summary: the market's summary has no decimal mark_price")?;
    if mark <= Decimal::ZERO {
        return Err("GET /markets/summary: the market's mark price is not positive".to_owned());
    }
    Ok(mark)
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

/// Runs mainnet_smoke on `factory` (Paradex's; a stand-in under test): every guard first, before
/// anything connects (module documentation); then the chain and the mark, each within the step
/// timeout; then the round trip. `chain` is `PARADEX_CHAIN_ID` when set; `creds` the mainnet
/// account's credentials, twice: the session's, handed to its codec unread, and the position
/// check's own login's. An error is a refusal before
/// the session started; once it started the outcome is in the [`Report`].
pub async fn run(
    smoke: &Smoke,
    chain: Option<&str>,
    creds: crate::positions::Creds,
    out: Out,
    factory: &'static dyn VenueFactory,
) -> Result<Report, String> {
    let opts = &smoke.trade;
    confirmed(smoke)?;
    let stub = mainnet_guard(&opts.rest_url, &opts.ws_url, chain)?;
    if stub {
        crate::testnet::direct_to_stub(crate::testnet::Target::LoopbackStub, &opts.proxy)?;
    }
    let cfg = config(opts);
    trusted(factory, &cfg)?;
    let lines = Lines {
        out,
        t0: Instant::now(),
    };
    let what = if stub {
        "a loopback test stub, not Paradex"
    } else {
        "Paradex MAINNET, real money"
    };
    // The hosts only: an accepted URL's path may still carry a token.
    let (rest_host, ws_host) = hosts(&opts.rest_url, &opts.ws_url)?;
    lines.line(format_args!(
        "MAINNET {what}: REST host {rest_host} WS host {ws_host} chain {MAINNET_CHAIN}"
    ));
    let wait = Duration::from_secs(opts.step_timeout_secs);
    let body = rest_get(
        &opts.rest_url,
        "/v1/system/config",
        "GET /system/config",
        &opts.proxy,
        wait,
    )
    .await?;
    let venue_chain = decode_chain(&body)?;
    if venue_chain != MAINNET_CHAIN {
        return Err(format!(
            "GET /system/config names the chain {}, not {MAINNET_CHAIN}: the REST base is not \
             Paradex mainnet's, or Paradex renamed its chain; nothing sent",
            shown_chain(&venue_chain)
        ));
    }
    lines.step(
        "chain",
        format_args!("GET /system/config names {MAINNET_CHAIN}, the chain the login signs for"),
    );
    let path = format!("/v1/markets/summary?market={}", opts.market);
    let body = rest_get(
        &opts.rest_url,
        &path,
        "GET /markets/summary",
        &opts.proxy,
        wait,
    )
    .await?;
    let mark = decode_mark(&body, &opts.market)?;
    lines.step(
        "mark",
        format_args!(
            "mark price {mark} (GET /markets/summary): the order must reach the minimum notional \
             ${} there",
            opts.min_notional
        ),
    );
    run_on(opts, &network(factory, mark), cfg, creds, lines).await
}
