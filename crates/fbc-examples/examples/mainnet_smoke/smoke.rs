//! mainnet_smoke's guards and its run: testnet_trade's round trip ([`crate::trade::run_on`]) on
//! Paradex MAINNET, never seeded by hand.
//!
//! **Before anything connects** ([`run`]): both confirmations (`--mainnet`, `--sole-trader`) and
//! the smoke's caps (yours, positive, with no ceiling, [`crate::smoke_args::confirmed`]; both
//! printed, the `CAPS` line, before the first request); the URLs exactly mainnet's
//! ([`MAINNET_REST`], [`MAINNET_WS`]) or both a loopback stub's; `PARADEX_CHAIN_ID`, when set, mainnet's; and the
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

use fbc_core::{VenueConfig, VenueFactory};
use rust_decimal::Decimal;

use crate::args::Options;
// The mainnet guard's pieces, shared with `testnet_quote --mainnet` (`src/mainnet.rs`), named
// here as mainnet_smoke always named them; its test reads some the program does not.
#[allow(unused_imports)]
pub use crate::mainnet::{
    LABEL, MAINNET_CHAIN, MAINNET_CHAIN_DECIMAL, MAINNET_CHAIN_HEX, MAINNET_REST, MAINNET_WS,
    REFUSAL_CAUSES, chain_refusal, decode_chain,
};
use crate::smoke_args::{Smoke, confirmed};
pub use crate::testnet::decode_mark;
use crate::testnet::{Lines, Out, hosts, mark_path, rest_get};
use crate::trade::{Network, Report, run_on};

/// The program a held lease's refusal and the guards' refusals name.
pub const PROGRAM: &str = "mainnet_smoke";

/// The mainnet guard ([`crate::mainnet::mainnet_guard`]), run before anything connects: the URLs
/// mainnet's (or both a loopback stub's: true) and the chain id (`PARADEX_CHAIN_ID` when set)
/// mainnet's.
pub fn mainnet_guard(rest: &str, ws: &str, chain: Option<&str>) -> Result<bool, String> {
    crate::mainnet::mainnet_guard(rest, ws, chain, PROGRAM)
}

/// Refuses a venue whose order caps under `cfg` do not declare its open-order snapshot
/// trustworthy ([`crate::mainnet::trusted`]).
pub fn trusted(factory: &dyn VenueFactory, cfg: &VenueConfig) -> Result<(), String> {
    crate::mainnet::trusted(factory, cfg, PROGRAM)
}

/// The Paradex configuration for `opts`' URLs, the login signing for [`MAINNET_CHAIN`].
pub fn config(opts: &Options) -> VenueConfig {
    crate::mainnet::config(&opts.rest_url, &opts.ws_url)
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

/// Runs mainnet_smoke on `factory` (Paradex's; a stand-in under test): every guard first, before
/// anything connects (module documentation); then the chain and the mark, each within the step
/// timeout; then the round trip. `chain` is `PARADEX_CHAIN_ID` when set; `creds` the mainnet
/// account's credentials, twice: the session's, handed to its codec unread, and the position
/// check's own login's. An error is a refusal before
/// the session started; once it started the outcome is in the [`Report`].
pub async fn run(
    smoke: &Smoke,
    chain: Option<&str>,
    creds: (fbc_core::Secrets, fbc_core::Secrets),
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
    lines.line(format_args!(
        "CAPS ${} resting per side, ${} inventory (yours: no default, no ceiling)",
        opts.resting_cap_usd, opts.inventory_cap_usd
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
    if let Some(refused) = chain_refusal(&decode_chain(&body)?) {
        return Err(refused);
    }
    lines.step(
        "chain",
        format_args!("GET /system/config names {MAINNET_CHAIN}, the chain the login signs for"),
    );
    let body = rest_get(
        &opts.rest_url,
        &mark_path(&opts.market),
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
