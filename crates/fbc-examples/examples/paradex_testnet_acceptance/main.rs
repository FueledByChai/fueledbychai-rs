//! paradex_testnet_acceptance: FBC-8xr's acceptance run on Paradex TESTNET (FBC-xji8). Through
//! fbc-oms, one run of each command type (place, amend, cancel, batch create, cancel-many, an
//! IOC expiry, cancel-on-disconnect on a dropped socket and on a graceful close) on tiny
//! post-only orders well away from the touch, each step's GET /orders and GET /positions compared
//! with the order stream, written up as an evidence report, a machine-readable evidence file and
//! a scrubbed slice of the run's journal. TESTNET ONLY: a mainnet URL or chain id is refused
//! before anything connects. An owner-run program: CI builds it, and its wiring is tested against
//! the conformance stub (`tests/paradex_testnet_acceptance.rs`), but nothing runs it against a
//! venue.
//!
//! ```text
//! export PARADEX_ACCOUNT_ADDRESS=...   # the testnet account
//! export PARADEX_PRIVATE_KEY=...       # its main Stark key
//! cargo run -p fbc-examples --example paradex_testnet_acceptance -- \
//!     --evidence-dir /abs/new/dir --sole-trader --namespace <N> \
//!     --market BTC-USD-PERP --tick <price_tick_size> --step <order_size_increment> \
//!     --min-notional <min_notional> --resting-cap-usd 11 --inventory-cap-usd 50 \
//!     --order-usd 11 --side buy --away-bps 300
//! cargo run -p fbc-examples --example paradex_testnet_acceptance -- --help
//! ```

mod accept;
mod accept_args;
// testnet_trade's command line and helpers, and the glue and testnet wiring the samples that
// trade share (`src/link.rs`, `src/testnet.rs`), reused as they are.
#[allow(dead_code)]
#[path = "../testnet_trade/args.rs"]
mod args;
#[path = "../../src/auth.rs"]
mod auth;
#[path = "../../src/auth_rest.rs"]
mod auth_rest;
mod evidence;
#[allow(dead_code)]
#[path = "../../src/link.rs"]
mod link;
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

use accept_args::AcceptParsed;
use args::{CHAIN_VAR, testnet_guard};

fn main() -> ExitCode {
    let opts = match accept_args::parse(std::env::args().skip(1)) {
        Ok(AcceptParsed::Run(opts)) => *opts,
        Ok(AcceptParsed::Help) => {
            print!("{}{}", accept_args::USAGE, args::USAGE);
            return ExitCode::SUCCESS;
        }
        Err(e) => {
            eprintln!("paradex_testnet_acceptance: {e}");
            return ExitCode::from(2);
        }
    };
    // The chain id is configuration, not a credential; it is checked with the URLs before the
    // credentials are read or anything connects.
    let chain = std::env::var(CHAIN_VAR).ok();
    if let Err(e) = testnet_guard(&opts.trade.rest_url, &opts.trade.ws_url, chain.as_deref()) {
        eprintln!("paradex_testnet_acceptance: {e}");
        return ExitCode::from(2);
    }
    // Read once now, so a missing variable refuses the run before anything connects; each
    // session and the REST reader then take their own copy.
    if let Err(e) = auth::paradex_secrets(|var| std::env::var(var).ok()) {
        eprintln!("paradex_testnet_acceptance: {e}");
        return ExitCode::from(2);
    }
    let creds = || auth::paradex_secrets(|var| std::env::var(var).ok());
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("paradex_testnet_acceptance: cannot start the runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    let (writer, printing) = testnet::detached(io::stdout());
    let out: testnet::Out = Rc::new(RefCell::new(writer));
    let ran = runtime.block_on(accept::run(&opts, chain.as_deref(), &creds, out));
    drop(runtime);
    let _ = printing.join();
    match ran {
        Ok(outcome) if outcome.ok => ExitCode::SUCCESS,
        Ok(_) => ExitCode::FAILURE,
        Err(e) => {
            eprintln!("paradex_testnet_acceptance: {e}");
            ExitCode::FAILURE
        }
    }
}
