//! testnet_quote: a PLUMBING TEST, NOT A STRATEGY. On one Paradex TESTNET market it keeps one
//! post-only order of a fixed size on each side, a fixed offset behind the touch, and requotes
//! them by cancel-and-replace as the touch moves, through fbc-oms's ExecutionPlanner, its caps
//! and authorizations, and fbc-runtime's order-entry session, until Ctrl-C, `--duration` or
//! `--cycles`; then Stops (kill switch + cancel all) and prints a summary. TESTNET ONLY: a
//! mainnet URL or chain id is refused before anything connects. An owner-run sample: CI builds
//! it, and its wiring is tested against the conformance stub (`tests/testnet_quote.rs`), but
//! nothing runs it against a venue.
//!
//! ```text
//! export PARADEX_ACCOUNT_ADDRESS=...   # the testnet account
//! export PARADEX_PRIVATE_KEY=...       # its main Stark key
//! cargo run -p fbc-examples --example testnet_quote -- --sole-trader --namespace <N> \
//!     --market BTC-USD-PERP --tick <price_tick_size> --step <order_size_increment> \
//!     --min-notional <min_notional> --resting-cap-usd 11 --inventory-cap-usd 50 \
//!     --order-usd <USD> --bid-bps <N> --ask-bps <N> --duration <SECONDS>
//! cargo run -p fbc-examples --example testnet_quote -- --help
//! ```

mod args;
#[path = "../../src/auth.rs"]
mod auth;
// The position check's REST reads (a review path, used unchanged): it uses a part of it.
#[allow(dead_code)]
#[path = "../../src/auth_rest.rs"]
mod auth_rest;
// Shared by the samples that trade: each uses a part of it.
#[allow(dead_code)]
#[path = "../../src/link.rs"]
mod link;
// Shared by the samples that can fill: each uses a part of it.
#[allow(dead_code)]
#[path = "../../src/positions.rs"]
mod positions;
mod quote;
// Shared by the samples that trade: each uses a part of it.
#[allow(dead_code)]
#[path = "../../src/testnet.rs"]
mod testnet;

use std::cell::RefCell;
use std::io;
use std::process::ExitCode;
use std::rc::Rc;

use args::{CHAIN_VAR, Parsed, testnet_guard};

fn main() -> ExitCode {
    let opts = match args::parse(std::env::args().skip(1)) {
        Ok(Parsed::Quote(opts)) => *opts,
        Ok(Parsed::Help) => {
            print!("{}", args::USAGE);
            return ExitCode::SUCCESS;
        }
        Err(e) => {
            eprintln!("testnet_quote: {e}");
            return ExitCode::from(2);
        }
    };
    // The chain id is configuration, not a credential; it is checked with the URLs before the
    // credentials are read or anything connects.
    let chain = std::env::var(CHAIN_VAR).ok();
    if let Err(e) = testnet_guard(&opts.rest_url, &opts.ws_url, chain.as_deref()) {
        eprintln!("testnet_quote: {e}");
        return ExitCode::from(2);
    }
    // Read twice: once for the session's codec, once for the position check's own REST login
    // (a credential set is never copied).
    let read = || auth::paradex_secrets(|var| std::env::var(var).ok());
    let creds = match read().and_then(|session| read().map(|positions| (session, positions))) {
        Ok(creds) => creds,
        Err(e) => {
            eprintln!("testnet_quote: {e}");
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
            eprintln!("testnet_quote: cannot start the runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    let (writer, printing) = testnet::detached(io::stdout());
    let out: testnet::Out = Rc::new(RefCell::new(writer));
    // Ctrl-C Stops the loop (kill switch + cancel all); a second Ctrl-C exits at once, leaving
    // what rests to Paradex's cancel-on-disconnect as the socket closes.
    let interrupt = async {
        if tokio::signal::ctrl_c().await.is_err() {
            std::future::pending::<()>().await;
        }
        tokio::spawn(async {
            if tokio::signal::ctrl_c().await.is_ok() {
                eprintln!("testnet_quote: a second Ctrl-C: exiting now");
                std::process::exit(130);
            }
        });
    };
    let ran = runtime.block_on(quote::run(&opts, chain.as_deref(), creds, out, interrupt));
    drop(runtime);
    let _ = printing.join();
    match ran {
        Ok(report) if report.ok => ExitCode::SUCCESS,
        Ok(_) => ExitCode::FAILURE,
        Err(e) => {
            eprintln!("testnet_quote: {e}");
            ExitCode::FAILURE
        }
    }
}
