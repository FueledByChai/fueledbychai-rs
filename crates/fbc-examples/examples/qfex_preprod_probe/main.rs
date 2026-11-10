//! qfex_preprod_probe: stage one of the QFEX probe (decision 0132 item 7, FBC-l931). On QFEX's
//! PRE-PRODUCTION environment, or with `--production` on QFEX PRODUCTION (decision 0143), an
//! owner-assisted run (on pre-production a declared testnet run, decision 0067; on production
//! with no such declaration) that arms cancel-on-disconnect once per account and records what
//! answers the arm, in a report, a machine-readable evidence file and a scrubbed slice of its
//! journal. It places nothing and sends no command but each session's arm and its own reads. An
//! owner-run program: CI builds it, and its wiring is tested against the conformance stub
//! (`tests/qfex_preprod_probe.rs`), but nothing runs it against a venue.
//!
//! ```text
//! export QFEX_PUBLIC_KEY=...       # the primary credential, for the environment run on
//! export QFEX_SECRET_KEY=...
//! export QFEX_SUBACCOUNT_ID=...    # the subaccount to arm on
//! cargo run -p fbc-examples --example qfex_preprod_probe -- [--production] \
//!     --evidence-dir /abs/new/dir --arm primary --arm subaccount \
//!     --registry-span <S> --ledger-max-age <S> --ledger-max-entries <N>
//! cargo run -p fbc-examples --example qfex_preprod_probe -- --help
//! ```

#[path = "../../src/auth_qfex.rs"]
mod auth_qfex;
mod capture;
mod ledger;
// paradex_testnet_acceptance's privacy patterns, privacy check and UTC clock, reused by path,
// with the glue whose withholding of credential-shaped words they name a frame's text by.
#[allow(dead_code)]
#[path = "../paradex_testnet_acceptance/evidence.rs"]
mod evidence;
#[allow(dead_code)]
#[path = "../../src/link.rs"]
mod link;
mod probe_args;
mod refusals;
mod stage;

use std::cell::RefCell;
use std::process::ExitCode;
use std::rc::Rc;

use probe_args::Parsed;

fn main() -> ExitCode {
    let opts = match probe_args::parse(std::env::args().skip(1)) {
        Ok(Parsed::Run(opts)) => *opts,
        Ok(Parsed::Help) => {
            print!("{}", probe_args::usage());
            return ExitCode::SUCCESS;
        }
        Err(e) => {
            eprintln!("qfex_preprod_probe: {e}");
            return ExitCode::from(2);
        }
    };
    let lookup = |var: &str| std::env::var(var).ok();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build();
    let runtime = match runtime {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("qfex_preprod_probe: cannot start the runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    let out: stage::Out = Rc::new(RefCell::new(std::io::stdout()));
    match runtime.block_on(stage::run(&opts, &lookup, out)) {
        Ok(outcome) if outcome.ok => ExitCode::SUCCESS,
        Ok(_) => ExitCode::FAILURE,
        Err(e) => {
            eprintln!("qfex_preprod_probe: {e}");
            ExitCode::from(2)
        }
    }
}
