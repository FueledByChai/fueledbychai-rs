//! testnet_trade: logs in to Paradex TESTNET, resyncs, places ONE post-only limit order of about
//! $11 well away from the touch through fbc-oms, waits for its acknowledgement, cancels it,
//! waits for the cancel's acknowledgement, then Stops (kill switch + cancel all) and exits,
//! printing each step with its time. TESTNET ONLY: a mainnet URL or chain id is refused before
//! anything connects. An owner-run sample: CI builds it, and its wiring is tested against the
//! conformance stub (`tests/testnet_trade.rs`), but nothing runs it against a venue.
//!
//! ```text
//! export PARADEX_ACCOUNT_ADDRESS=...   # the testnet account
//! export PARADEX_PRIVATE_KEY=...       # its main Stark key
//! cargo run -p fbc-examples --example testnet_trade -- --sole-trader \
//!     --market BTC-USD-PERP --tick <price_tick_size> --step <order_size_increment> \
//!     --min-notional <min_notional>    # the market's, from GET /v1/markets
//! cargo run -p fbc-examples --example testnet_trade -- --help
//! ```

mod args;
#[path = "../../src/auth.rs"]
mod auth;
mod link;
mod trade;

use std::cell::RefCell;
use std::io;
use std::process::ExitCode;
use std::rc::Rc;

use args::{CHAIN_VAR, Parsed, testnet_guard};

fn main() -> ExitCode {
    let opts = match args::parse(std::env::args().skip(1)) {
        Ok(Parsed::Trade(opts)) => *opts,
        Ok(Parsed::Help) => {
            print!("{}", args::USAGE);
            return ExitCode::SUCCESS;
        }
        Err(e) => {
            eprintln!("testnet_trade: {e}");
            return ExitCode::from(2);
        }
    };
    // The chain id is configuration, not a credential; it is checked with the URLs before the
    // credentials are read or anything connects.
    let chain = std::env::var(CHAIN_VAR).ok();
    if let Err(e) = testnet_guard(&opts.rest_url, &opts.ws_url, chain.as_deref()) {
        eprintln!("testnet_trade: {e}");
        return ExitCode::from(2);
    }
    let creds = match auth::paradex_secrets(|var| std::env::var(var).ok()) {
        Ok(creds) => creds,
        Err(e) => {
            eprintln!("testnet_trade: {e}");
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
            eprintln!("testnet_trade: cannot start the runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    let out: trade::Out = Rc::new(RefCell::new(io::stdout()));
    match runtime.block_on(trade::run(&opts, chain.as_deref(), creds, out)) {
        Ok(report) if report.ok => ExitCode::SUCCESS,
        Ok(_) => ExitCode::FAILURE,
        Err(e) => {
            eprintln!("testnet_trade: {e}");
            ExitCode::FAILURE
        }
    }
}
