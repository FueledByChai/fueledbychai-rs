//! mainnet_smoke: the owner's first Paradex MAINNET order (FBC-p0aa, owner answer "Library smoke
//! first"). testnet_trade's round trip on a SEPARATE small mainnet account: one post-only order
//! of about $11 well away from the touch through fbc-oms, its acknowledgement, its cancel, the
//! cancel's acknowledgement, then Stop (kill switch + cancel all), each step timed out and
//! printed. Refused without both `--mainnet` and `--sole-trader`, with any URL or chain id but
//! mainnet's, with caps above $11 resting per side or $50 inventory, with a testnet credential
//! variable set, or on a venue whose snapshot source is not trustworthy (Paradex's is, decision
//! 0094; no hand seed on mainnet), before anything connects. An owner-run sample: CI builds it,
//! and its wiring is tested against the conformance stub (`tests/mainnet_smoke.rs`).
//!
//! ```text
//! unset PARADEX_ACCOUNT_ADDRESS PARADEX_PRIVATE_KEY   # the testnet samples' variables
//! export PARADEX_MAINNET_ACCOUNT_ADDRESS=...          # the separate mainnet account
//! export PARADEX_MAINNET_PRIVATE_KEY=...              # its main L2 (Stark) key
//! cargo run -p fbc-examples --example mainnet_smoke -- --mainnet --sole-trader \
//!     --namespace <N> --market BTC-USD-PERP --tick <price_tick_size> \
//!     --step <order_size_increment> --min-notional <min_notional> \
//!     --resting-cap-usd 11 --inventory-cap-usd 50 --order-usd 11 \
//!     --side buy --away-bps 300
//! cargo run -p fbc-examples --example mainnet_smoke -- --help
//! ```

// testnet_trade's options and its round trip, reused by path: only part of each is used here.
#[allow(dead_code, unused_imports)]
#[path = "../testnet_trade/args.rs"]
mod args;
#[path = "../../src/auth_mainnet.rs"]
mod auth_mainnet;
// The position check's REST reads (a review path, used unchanged): it uses a part of it.
#[allow(dead_code)]
#[path = "../../src/auth_rest.rs"]
mod auth_rest;
// Shared by the samples that trade: each uses a part of it.
#[allow(dead_code)]
#[path = "../../src/link.rs"]
mod link;
// Shared by the mainnet samples: each uses a part of it.
#[allow(dead_code)]
#[path = "../../src/mainnet.rs"]
mod mainnet;
// Shared by the samples that can fill: each uses a part of it.
#[allow(dead_code)]
#[path = "../../src/positions.rs"]
mod positions;
mod smoke;
mod smoke_args;
// Shared by the samples that trade: each uses a part of it.
#[allow(dead_code)]
#[path = "../../src/testnet.rs"]
mod testnet;
#[allow(dead_code)]
#[path = "../testnet_trade/trade.rs"]
mod trade;

use std::cell::RefCell;
use std::io;
use std::process::ExitCode;
use std::rc::Rc;

use fbc_venue_paradex::ParadexFactory;
use smoke_args::Parsed;

fn main() -> ExitCode {
    let smoke = match smoke_args::parse(std::env::args().skip(1)) {
        Ok(Parsed::Smoke(smoke)) => *smoke,
        Ok(Parsed::Help) => {
            print!("{}", smoke_args::USAGE);
            return ExitCode::SUCCESS;
        }
        Err(e) => {
            eprintln!("mainnet_smoke: {e}");
            return ExitCode::from(2);
        }
    };
    // The chain id is configuration, not a credential; it is checked with the URLs before the
    // credentials are read or anything connects.
    let chain = std::env::var(testnet::CHAIN_VAR).ok();
    let guarded =
        smoke::mainnet_guard(&smoke.trade.rest_url, &smoke.trade.ws_url, chain.as_deref());
    if let Err(e) = guarded {
        eprintln!("mainnet_smoke: {e}");
        return ExitCode::from(2);
    }
    // Read twice: once for the session's codec, once for the position check's own REST login
    // (a credential set is never copied).
    let read = || auth_mainnet::paradex_mainnet_secrets(|var| std::env::var(var).ok());
    let creds = match read().and_then(|session| read().map(|positions| (session, positions))) {
        Ok(creds) => creds,
        Err(e) => {
            eprintln!("mainnet_smoke: {e}");
            return ExitCode::from(2);
        }
    };
    // One thread, as the runtime's sessions run (design §5.1).
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("mainnet_smoke: cannot start the runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    // Written by a thread of its own: a stalled standard output never blocks the runtime.
    let (writer, printing) = testnet::detached(io::stdout());
    let out: testnet::Out = Rc::new(RefCell::new(writer));
    let ran = runtime.block_on(smoke::run(
        &smoke,
        chain.as_deref(),
        creds,
        out,
        &ParadexFactory,
    ));
    drop(runtime);
    let _ = printing.join();
    match ran {
        Ok(report) if report.ok => ExitCode::SUCCESS,
        Ok(_) => ExitCode::FAILURE,
        Err(e) => {
            eprintln!("mainnet_smoke: {e}");
            ExitCode::FAILURE
        }
    }
}
