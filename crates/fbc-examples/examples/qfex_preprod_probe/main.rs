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
//! any other time ends the program at once, writing no report, as it always has, except in a
//! place-stage run, which it stops with its report ([`interrupt::Presses`]). An owner-run
//! program: CI builds it, and its wiring is tested against the conformance stub
//! (`tests/qfex_preprod_probe.rs`), but nothing runs it against a venue.
//!
//! With `--place-stages` (FBC-c6kw, FBC-23ch and FBC-77r8, slices of FBC-9sk2; decisions 0148 and
//! 0151) it runs the place stages on production (`--production`) and, until FBC-4plj, only against
//! a loopback test stub: a `CAPS` line first, its lease directory opened ([`places`]: tagged
//! production, a stop marker held to `--reconciled`, the namespace lease taken; 0151 items 1 and
//! 6), the hold first when `--hold-secs` asks, then the traded instrument's spec from this run's
//! `GET /refdata`, the ledger's count bound against each place registry's ceiling, the MDS touch
//! and the caps at it ([`market`]), with `--reconciled` the reads that clear the marker, then each
//! named account's drop test in turn (`stage.rs`): drop session one places X, one post-only GTC at
//! the minimum size behind the touch, compared with never-arming reads ([`compare`]), the journal's
//! frames read on their way by the tee ([`tee`]), then closes gracefully; after the reads, the read
//! before drop session two admitting X alone, drop session two places Y and its connection drops
//! abruptly; the reads after the drop give the account's verdict ([`verdict`]). The run ends after
//! the last account's, its outcome not ok, until FBC-su9i builds stage two; every stop of the place
//! stages (Ctrl-C among them) ends it sooner, an order still resting reported for the owner's hand
//! cancel, and writes the report and the stop marker. Ctrl-C from the hold's first read to its last
//! is the hold's: it stops the run before its preflight, with its report and no stop marker.
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
//! cargo run -p fbc-examples --example qfex_preprod_probe -- --production --place-stages \
//!     --evidence-dir /abs/new/dir --trade-url <LOOPBACK> --rest-url <LOOPBACK> \
//!     --mds-url <LOOPBACK> --symbol <SYMBOL> --side <buy|sell> --sole-trader \
//!     --resting-cap-usd <USD> --inventory-cap-usd <USD> --away-bps <BPS> \
//!     --flatten-cap-bps <BPS> --flatten-attempts <N> --namespace <N> --agree-wait-ms <MS> \
//!     --read-interval-ms <MS> --touch-max-age-ms <MS> --registry-span <S> \
//!     --ledger-max-age <S> --ledger-max-entries <N>
//! cargo run -p fbc-examples --example qfex_preprod_probe -- --help
//! ```

#[path = "../../src/auth_qfex.rs"]
mod auth_qfex;
mod capture;
// The place stages' comparisons of the streams with the reads (FBC-23ch), which the drop sessions
// take after each step.
mod compare;
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
mod market;
mod pages;
mod places;
mod probe_args;
mod refusals;
mod report;
mod stage;
// The journal's tee (FBC-23ch), whose stops the drop sessions wait on.
mod tee;
// The drop test's verdict (FBC-77r8): what the reads after drop session two's drop show of X
// and Y, judged per account.
mod verdict;
// The Paradex testnet samples' shared command-line pieces and cap conversion, reused by path:
// the place stages take their caps, lease directory and decimals as those samples do.
#[allow(dead_code)]
#[path = "../../src/testnet.rs"]
mod testnet;

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
    let (opts, places) = match probe_args::parse(std::env::args().skip(1)) {
        Ok(Parsed::Run(opts)) => (*opts, None),
        Ok(Parsed::Places(opts, places)) => (*opts, Some(*places)),
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
    // A place-stage run never ends the program at a press: each one the hold's connection does
    // not take goes to the run, which stops with its report ([`interrupt::Presses`], FBC-23ch).
    // The runtime has one thread, which `stage.rs` relies on to give the hold's connection every
    // press from its opening (`Run::hold`). A stage-one
    // run that holds takes the owner's Ctrl-C during the hold's connection as the hold's
    // ([`interrupt`]); any other stage-one run, and any interrupt outside that connection, ends
    // the program at once with status 130, writing no report, as it always has.
    let (interrupt, presses): (stage::Interrupt, _) = match (&places, opts.hold) {
        (Some(_), _) => {
            let (outside, presses) = interrupt::Presses::channel();
            let (task, interrupt) = interrupt::wire(ctrl_c(), outside);
            runtime.spawn(task);
            (interrupt, presses)
        }
        (None, Some(_)) => {
            let (task, interrupt) = interrupt::wire(ctrl_c(), || {
                eprintln!(
                    "qfex_preprod_probe: interrupted outside the hold's connection: the program \
                     ends now and writes no report"
                );
                std::process::exit(130);
            });
            runtime.spawn(task);
            (interrupt, interrupt::Presses::none())
        }
        (None, None) => (Box::pin(std::future::pending()), interrupt::Presses::none()),
    };
    let ran = match &places {
        None => runtime.block_on(stage::run_until(&opts, &lookup, out, interrupt)),
        Some(places) => runtime.block_on(stage::run_places_until(
            &opts, places, &lookup, out, interrupt, presses,
        )),
    };
    match ran {
        Ok(outcome) if outcome.ok => ExitCode::SUCCESS,
        Ok(_) => ExitCode::FAILURE,
        Err(e) => {
            eprintln!("qfex_preprod_probe: {e}");
            ExitCode::from(2)
        }
    }
}
