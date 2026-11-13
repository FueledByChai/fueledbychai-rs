//! qfex_preprod_probe: stage one of the QFEX probe (decision 0132 item 7, FBC-l931). On QFEX's
//! PRE-PRODUCTION environment, or with `--production` on QFEX PRODUCTION (decision 0143), an
//! owner-assisted run (on pre-production a declared testnet run, decision 0067; on production
//! with no such declaration) that arms cancel-on-disconnect once per account and records what
//! answers the arm, in a report, a machine-readable evidence file and a scrubbed slice of its
//! journal. It places nothing and sends no command but each session's arm, its resync's reads
//! (decision 0132 item 5) and its own reads, which cover the accounts its configuration names and
//! never ask QFEX for a list of them (decision 0144): confirm in QFEX's app before each run that
//! the user has no other subaccount. With `--hold-secs` it first holds one trade connection that
//! never arms and records each ping, pong and close QFEX sends on it (the liveness hold, decision
//! 0146), and a run may hold without arming. Ctrl-C from the hold's connection's opening until its
//! close is over is the hold's ([`interrupt`]): one before the hold ended ends it as its duration
//! does, its close, the reads after it and the report following, and one while it closes is read
//! as it drops the interrupt; either stops a run that also arms, so no session starts. Ctrl-C at
//! any other time ends the program at once, writing no report, as it always has. An owner-run
//! program: CI builds it, and its wiring is tested against the conformance stub
//! (`tests/qfex_preprod_probe.rs`), but nothing runs it against a venue.
//!
//! ```text
//! export QFEX_PUBLIC_KEY=...       # the primary account's credential, for the environment run on
//! export QFEX_SECRET_KEY=...
//! export QFEX_SUBACCOUNT_ID=...    # the subaccount to read, and to arm on
//! cargo run -p fbc-examples --example qfex_preprod_probe -- [--production] \
//!     --evidence-dir /abs/new/dir --arm primary --arm subaccount \
//!     --registry-span <S> --ledger-max-age <S> --ledger-max-entries <N>
//! cargo run -p fbc-examples --example qfex_preprod_probe -- --production \
//!     --evidence-dir /abs/new/dir --hold-secs <S> --socks5 <HOST:PORT>
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
mod hold;
mod interrupt;
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

/// Each Ctrl-C the process receives, as a stream of presses for [`interrupt::wire`].
fn ctrl_c() -> impl futures_util::Stream<Item = ()> + Send + 'static {
    futures_util::stream::unfold((), |()| async {
        tokio::signal::ctrl_c().await.ok().map(|()| ((), ()))
    })
}

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
    // A run that holds takes the owner's Ctrl-C during the hold's connection as the hold's
    // ([`interrupt`]); any other run, and any interrupt outside that connection, ends the
    // program at once with status 130, writing no report, as it always has.
    let interrupt: stage::Interrupt = match opts.hold {
        Some(_) => {
            let (presses, interrupt) = interrupt::wire(ctrl_c(), || {
                eprintln!(
                    "qfex_preprod_probe: interrupted outside the hold's connection: the program \
                     ends now and writes no report"
                );
                std::process::exit(130);
            });
            runtime.spawn(presses);
            interrupt
        }
        None => Box::pin(std::future::pending()),
    };
    match runtime.block_on(stage::run_until(&opts, &lookup, out, interrupt)) {
        Ok(outcome) if outcome.ok => ExitCode::SUCCESS,
        Ok(_) => ExitCode::FAILURE,
        Err(e) => {
            eprintln!("qfex_preprod_probe: {e}");
            ExitCode::from(2)
        }
    }
}
