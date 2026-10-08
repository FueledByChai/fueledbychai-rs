//! md_watch: subscribes to a Paradex market and a Binance USD-M symbol through fbc-runtime and
//! prints their live touches and top-of-book, a line per update, until Ctrl-C or `--seconds`.
//! An owner-run sample: CI builds it, and its wiring is tested against the conformance stub
//! (`tests/md_watch.rs`), but nothing runs it against a venue. It never sends an order and never
//! reads a credential.
//!
//! ```text
//! cargo run -p fbc-examples --example md_watch -- --paradex BTC-USD-PERP --binance BTCUSDT
//! cargo run -p fbc-examples --example md_watch -- --paradex BTC-USD-PERP --paradex-touch both
//! cargo run -p fbc-examples --example md_watch -- --help
//! ```

mod args;
mod watch;

use std::cell::RefCell;
use std::io;
use std::process::ExitCode;
use std::rc::Rc;
use std::time::Duration;

use args::Parsed;

fn main() -> ExitCode {
    let opts = match args::parse(std::env::args().skip(1)) {
        Ok(Parsed::Watch(opts)) => *opts,
        Ok(Parsed::Help) => {
            print!("{}", args::USAGE);
            return ExitCode::SUCCESS;
        }
        Err(e) => {
            eprintln!("md_watch: {e}");
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
            eprintln!("md_watch: cannot start the runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    let out: watch::Out = Rc::new(RefCell::new(io::stdout()));
    let seconds = opts.seconds;
    let stop = async move {
        match seconds {
            Some(n) => tokio::time::sleep(Duration::from_secs(n)).await,
            // Until Ctrl-C ends the process: nothing is held that needs a clean stop.
            None => std::future::pending().await,
        }
    };
    match runtime.block_on(watch::run(&opts, out, true, stop)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("md_watch: {e}");
            ExitCode::FAILURE
        }
    }
}
