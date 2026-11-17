//! FBC-c6kw's done line: the place stages' preflight (`--place-stages`, the first slice of
//! FBC-9sk2; decision 0148), run against the QFEX model ([`super::model`]) on 127.0.0.1 under
//! `production_probe` with loopback origins, as the production tests of stage one are. Declared
//! by path in `tests/qfex_preprod_probe.rs`, so this proof is part of the test binary decision
//! 0148's Consequences name (FBC-fqv5 item 12) and Cargo builds no target of its own for it.
//! Nothing places, nothing authenticated connects for a place stage, no request reaches a venue,
//! and every symbol, cap, price, key and id is made up.
//!
//! - every place-stage flag is refused naming it and never its value, `--place-stages` without
//!   `--production` naming the reason, a flag given twice, more than 10 reads per comparison,
//!   the testnet samples' lease directory (through a symbolic link too, and any path with a
//!   `..`) and a span over an hour included; a place-stage run needs no `--arm`, takes each
//!   ledger flag as required, and holds its span bound above the drop test's floor;
//! - the `CAPS` line comes first, before any request and before the hold, both caps reach
//!   standard output and neither the report nor the evidence, a run that holds holds before the
//!   preflight with nothing else open, and the owner's Ctrl-C on the hold stops the run before
//!   its preflight;
//! - the spec comes from this run's `GET /refdata`, charged to the limiter it is given and
//!   refused with no room, at its timeout, on a failed connection or on an answer the adapter
//!   refuses, an unlisted, halted or one-shape symbol is refused before any account connects,
//!   discovery and the touch take the guarded origins, and a stale, crossed, locked, one-sided
//!   or absent touch is refused;
//! - the prices (unit tests), moved and snapped in whole ticks at a fine tick and a high price
//!   too, the flatten cap measured from the touch the IOC crosses;
//! - caps below three minimum sizes refuse the run before any account connects, while caps of
//!   exactly three admit three resting minimum-size orders in fbc-oms, and a count bound at a
//!   place registry's ceiling is refused before any account connects;
//! - `qfex.exec.production_places` is set on a place stage's session configuration alone;
//! - no `GET /user/subaccounts` is sent, and `--place-stages` is refused on every origin that is
//!   not loopback, its test run through a SOCKS5 proxy no one listens on, so a gate that let it
//!   through would reach no QFEX host.

use super::*;

use std::io::Write as _;

use fbc_core::{
    AccountKey, CidMint, Effect, Envelope, ExchNs, ExchTsKind, HttpMethod, HttpRequest,
    InstrumentId, Lots, Lvl, MarketLease, MdEvent, Namespace, NamespaceLease, NewOrder, OrderCaps,
    OrderKind, Side, Ticks, Tif, TouchSourceId, VenueMeta,
};
use fbc_oms::{LadderConfig, LeaseKeys, Leases, OmsError};
use fbc_runtime::SafetyReserve;
use fbc_venue_qfex::discover::REFDATA_TAG;
use fbc_venue_qfex::probe::{KEY_PRODUCTION_PLACES, PROBE_PLACE_STAGES};
use market::{
    BEFORE_EPOCH, CAPPED, COUNTED, CROSSED, DISCOVERED, Discovered, NO_TOUCH, ONE_SIDED, Quote,
    STALE, TOUCHED, TouchState,
};
use model::{Listed, Printed, QfexModel, SYMBOL, Venue};
use probe_args::Places;
use rust_decimal::Decimal;

/// A command line, flag by flag, each with its value when it takes one.
type Argv = Vec<(&'static str, Option<String>)>;

/// A place-stage run's command line on the given origins, writing under `dir`: every flag the
/// place stages require, the waits short enough for a test, and caps far above three minimum
/// sizes at the model's touch.
fn argv_on(dir: &Path, trade: &str, rest: &str, mds: &str) -> Argv {
    let some = |v: &str| Some(v.to_owned());
    let evidence = dir.join("evidence");
    let leases = dir.join("leases");
    vec![
        ("--production", None),
        ("--place-stages", None),
        ("--evidence-dir", some(evidence.to_str().unwrap())),
        ("--trade-url", some(trade)),
        ("--rest-url", some(rest)),
        ("--mds-url", some(mds)),
        ("--rpc-timeout-ms", some("600")),
        ("--read-wait-ms", some("1000")),
        ("--connect-wait-ms", some("1000")),
        ("--symbol", some(SYMBOL)),
        ("--side", some("buy")),
        ("--sole-trader", None),
        ("--resting-cap-usd", some("1000")),
        ("--inventory-cap-usd", some("2000")),
        ("--away-bps", some("50")),
        ("--flatten-cap-bps", some("10")),
        ("--flatten-attempts", some("1")),
        ("--namespace", some("7")),
        ("--lease-dir", some(leases.to_str().unwrap())),
        ("--agree-wait-ms", some("5000")),
        ("--read-interval-ms", some("1000")),
        ("--touch-max-age-ms", some("5000")),
        ("--registry-span", some("600")),
        ("--ledger-max-age", some("601")),
        ("--ledger-max-entries", some("100")),
    ]
}

/// [`argv_on`] the model's origins.
fn argv(dir: &Path, venue: &Venue) -> Argv {
    let (trade, rest) = (venue.trade.ws_url(""), venue.trade.http_url(""));
    argv_on(dir, &trade, &rest, &venue.mds.ws_url(""))
}

/// [`argv_on`] origins nothing listens on, for the parse alone.
fn parse_only() -> Argv {
    let dir = Path::new("/abs/probe");
    argv_on(
        dir,
        "ws://127.0.0.1:1",
        "http://127.0.0.1:2",
        "ws://127.0.0.1:3",
    )
}

/// `argv` with `flag` set to `value`: replaced where it is, appended where it is not.
fn set(mut argv: Argv, flag: &'static str, value: &str) -> Argv {
    match argv.iter_mut().find(|(f, _)| *f == flag) {
        Some((_, v)) => *v = Some(value.to_owned()),
        None => argv.push((flag, Some(value.to_owned()))),
    }
    argv
}

/// `argv` without `flag` and its value.
fn without(mut argv: Argv, flag: &str) -> Argv {
    argv.retain(|(f, _)| *f != flag);
    argv
}

/// `argv`, as the program reads its arguments.
fn flat(argv: &Argv) -> Vec<String> {
    let each = argv
        .iter()
        .flat_map(|(f, v)| std::iter::once(f.to_string()).chain(v.clone()));
    each.collect()
}

/// The command line parsed with `$HOME` at a made-up absolute path: a place-stage run's options
/// and what it asks for, or the refusal.
fn parsed(argv: &Argv) -> Result<(Options, Places), String> {
    let home = Some(std::ffi::OsString::from("/home/synthetic"));
    match probe_args::parse_with_home(flat(argv), home)? {
        Parsed::Places(opts, places) => Ok((*opts, *places)),
        other => panic!("not a place-stage run: {other:?}"),
    }
}

/// The refusal of `argv`.
fn refusal(argv: &Argv) -> String {
    let home = Some(std::ffi::OsString::from("/home/synthetic"));
    probe_args::parse_with_home(flat(argv), home).unwrap_err()
}

/// The environment of a run naming the primary account alone: no `QFEX_SUBACCOUNT_ID`.
fn primary_only() -> HashMap<&'static str, String> {
    let mut env = environment();
    for var in [SUB_ID_VAR, SUB_PUBLIC_VAR, SUB_SECRET_VAR] {
        env.remove(var);
    }
    env
}

/// Runs a place-stage run parsed from `argv` against `venue` with `env`, printing into the
/// model's [`Printed`]: its outcome or refusal, and what it printed.
async fn place_run(
    venue: &Venue,
    argv: &Argv,
    env: &HashMap<&'static str, String>,
) -> (Result<stage::Outcome, String>, String) {
    let (mut opts, places) = parsed(argv).unwrap();
    opts.rate_user = user();
    let printed = venue.printed();
    let lookup = |var: &str| env.get(var).cloned();
    let out: stage::Out = Rc::new(RefCell::new(printed.clone()));
    let ran = stage::run_places(&opts, &places, &lookup, out).await;
    (ran, printed.text())
}

/// A run's outcome, which must end after the preflight it passed, or with the refusal `why`
/// before it: never ok, since the place stages after the preflight are not built yet.
fn stopped(ran: Result<stage::Outcome, String>, printed: &str) -> Evidence {
    let outcome = ran.unwrap();
    assert!(!outcome.ok, "{printed}");
    assert!(printed.contains("DONE INCOMPLETE"), "{printed}");
    outcome.evidence
}

/// Asserts the model received no `GET /user/subaccounts` (decision 0144): only positions,
/// `/refdata` and historic-orders requests.
fn assert_no_listing(venue: &Venue) {
    let asked = venue.trade.http_requests();
    let admitted = |r: &String| {
        r == "GET /user/positions"
            || r == "GET /refdata"
            || r.starts_with("GET /user/historic-orders")
    };
    assert!(asked.iter().all(admitted), "{asked:?}");
}

/// Asserts no account connected: no trade socket opened and no account read.
fn assert_no_account(venue: &Venue) {
    assert!(venue.trade.connections().is_empty());
    let asked = venue.trade.http_requests();
    assert!(asked.iter().all(|r| r == "GET /refdata"), "{asked:?}");
}

/// The report and the evidence a run wrote under `dir`.
fn outputs_of(dir: &Path) -> (String, String) {
    let evidence = dir.join("evidence");
    let report = fs::read_to_string(evidence.join("report.md")).unwrap();
    let json = fs::read_to_string(evidence.join("evidence.json")).unwrap();
    (report, json)
}

/// The run's spec of `listed`, resolved from a `/refdata` answer listing it alone, as this run's
/// discovery resolves it.
fn spec_of(listed: Listed) -> Discovered {
    let mut cfg = VenueConfig::new();
    cfg.insert(KEY_EXEC_MODE, fbc_venue_qfex::probe::PRODUCTION_PROBE);
    cfg.insert(TRADE_URL, "ws://127.0.0.1:1");
    cfg.insert(KEY_REST_URL, "http://127.0.0.1:2");
    cfg.insert(KEY_MDS_URL, "ws://127.0.0.1:3");
    cfg.insert(KEY_PAGE_SIZE, "1000");
    cfg.insert(KEY_RESYNC_TIMEOUT, "30s");
    cfg.insert(KEY_RPC_TIMEOUT, "600ms");
    let cfg = market::market_config(&cfg, Duration::from_secs(1));
    let plan = Qfex.discover(&cfg).unwrap();
    let caps = Qfex.caps(&cfg).unwrap();
    let mut model = QfexModel {
        listed: vec![listed],
        ..QfexModel::default()
    };
    let req = fbc_conformance::HttpRequest {
        method: Method::GET,
        path: "/refdata".to_owned(),
        query: None,
        headers: Vec::new(),
        body: Vec::new(),
        params: Vec::new(),
    };
    let body = model.rest(&req).body;
    let answers = [(REFDATA_TAG, 200, fbc_runtime::http::Bytes::from(body))];
    market::resolve(plan, &caps, &answers, SYMBOL).unwrap()
}

/// The model's fresh touch in ticks of a cent: 100.00 bid, 100.10 asked.
const QUOTE: Quote = Quote {
    bid: Ticks(10_000),
    ask: Ticks(10_010),
};

// ----- The command line -----

#[test]
fn place_stage_flags_are_refused_naming_the_flag() {
    let (opts, places) = parsed(&parse_only()).unwrap();
    assert!(opts.production && opts.arms.is_empty() && opts.hold.is_none());
    assert_eq!(places.symbol, SYMBOL);
    assert_eq!(places.side, Side::Buy);
    assert_eq!(
        (places.resting_cap_usd, places.inventory_cap_usd),
        (Decimal::from(1000), Decimal::from(2000))
    );
    assert_eq!((places.away_bps, places.flatten_cap_bps), (50, 10));
    assert_eq!((places.flatten_attempts, places.namespace), (1, 7));
    assert_eq!(places.lease_dir, Path::new("/abs/probe/leases"));
    assert_eq!(places.agree_wait, Duration::from_secs(5));
    assert_eq!(places.read_interval, Duration::from_secs(1));
    assert_eq!(places.touch_max_age, Duration::from_secs(5));
    let sell = parsed(&set(parse_only(), "--side", "sell")).unwrap().1;
    assert_eq!(sell.side, Side::Sell);

    // A value refused as it is read: the flag named, the value never shown.
    let long = "S".repeat(probe_args::MAX_SYMBOL + 1);
    let bad = [
        (
            "--symbol",
            "two words",
            "--symbol: not a venue symbol of at most 32",
        ),
        ("--symbol", long.as_str(), "--symbol: not a venue symbol"),
        ("--symbol", "SYN-USDé", "--symbol: not a venue symbol"),
        ("--side", "sideways", "--side: buy or sell"),
        (
            "--resting-cap-usd",
            "x7q",
            "--resting-cap-usd: not a positive decimal",
        ),
        (
            "--inventory-cap-usd",
            "-42.5",
            "--inventory-cap-usd: not a positive decimal",
        ),
        (
            "--inventory-cap-usd",
            "0.000",
            "--inventory-cap-usd: not a positive decimal",
        ),
        (
            "--away-bps",
            "19",
            "--away-bps: not a whole number from 20 to 10000",
        ),
        (
            "--away-bps",
            "10001",
            "--away-bps: not a whole number from 20 to 10000",
        ),
        (
            "--flatten-cap-bps",
            "x7q",
            "--flatten-cap-bps: not a whole number from 1 to 9999",
        ),
        (
            "--flatten-attempts",
            "4",
            "--flatten-attempts: not a whole number from 1 to 3",
        ),
        (
            "--namespace",
            "65536",
            "--namespace: not a whole number from 1 to 65535",
        ),
        (
            "--lease-dir",
            "relative/dir",
            "--lease-dir: not an absolute path",
        ),
        (
            "--agree-wait-ms",
            "60001",
            "--agree-wait-ms: not a whole number from 1 to 60000",
        ),
        (
            "--read-interval-ms",
            "999",
            "--read-interval-ms: not a whole number from 1000",
        ),
        (
            "--touch-max-age-ms",
            "60001",
            "--touch-max-age-ms: not a whole number from 1 to",
        ),
    ];
    for (flag, typed, says) in bad {
        let err = refusal(&set(parse_only(), flag, typed));
        assert!(err.starts_with(says), "{flag}: {err}");
        assert!(!err.contains(typed), "{flag}: {err}");
    }
    // A flag whose value is left out.
    let mut cut = without(parse_only(), "--side");
    cut.push(("--side", None));
    assert_eq!(refusal(&cut), "--side needs a value");

    // Values refused together, naming both flags and neither value.
    let err = refusal(&set(parse_only(), "--flatten-cap-bps", "50"));
    assert!(
        err.starts_with("--flatten-cap-bps must be below --away-bps"),
        "{err}"
    );
    assert!(
        err.contains("the values are not shown") && !err.contains("50"),
        "{err}"
    );
    // One read, then one each second within 10 s, is 11 reads: more than 10. Within 9.999 s it
    // is 10.
    let err = refusal(&set(parse_only(), "--agree-wait-ms", "10000"));
    assert!(
        err.starts_with("--agree-wait-ms with --read-interval-ms allows more than 10 reads"),
        "{err}"
    );
    assert!(!err.contains("10000"), "{err}");
    let ten = parsed(&set(parse_only(), "--agree-wait-ms", "9999"))
        .unwrap()
        .1;
    assert_eq!(probe_args::reads(ten.agree_wait, ten.read_interval), 10);
    let longer = set(parse_only(), "--read-interval-ms", "2000");
    let longer = set(longer, "--agree-wait-ms", "19999");
    assert!(parsed(&longer).is_ok());
    assert!(refusal(&set(longer, "--agree-wait-ms", "20000")).contains("more than 10 reads"));

    // Every required flag, missing, refused naming it.
    for flag in [
        "--symbol",
        "--side",
        "--sole-trader",
        "--resting-cap-usd",
        "--inventory-cap-usd",
        "--away-bps",
        "--flatten-cap-bps",
        "--flatten-attempts",
        "--namespace",
        "--agree-wait-ms",
        "--read-interval-ms",
        "--touch-max-age-ms",
    ] {
        let err = refusal(&without(parse_only(), flag));
        let says = format!("{flag} is required with --place-stages: it has no default");
        assert!(err.starts_with(&says), "{flag}: {err}");
    }
    let err = refusal(&without(parse_only(), "--sole-trader"));
    assert!(err.contains("(decision 0148 item 7)"), "{err}");

    // Every place stage's flag is refused without --place-stages, naming it.
    let stage_one: Argv = vec![
        ("--evidence-dir", Some("/abs/probe".to_owned())),
        ("--arm", Some("primary".to_owned())),
        ("--registry-span", Some("101".to_owned())),
        ("--ledger-max-age", Some("102".to_owned())),
        ("--ledger-max-entries", Some("1".to_owned())),
    ];
    for (flag, value) in parse_only().into_iter().filter(|(f, _)| {
        ![
            "--production",
            "--place-stages",
            "--evidence-dir",
            "--trade-url",
            "--rest-url",
            "--mds-url",
            "--rpc-timeout-ms",
            "--read-wait-ms",
            "--connect-wait-ms",
            "--registry-span",
            "--ledger-max-age",
            "--ledger-max-entries",
        ]
        .contains(f)
    }) {
        let mut args = stage_one.clone();
        args.push((flag, value.clone()));
        let err = probe_args::parse(flat(&args)).unwrap_err();
        assert_eq!(
            err,
            format!("{flag} is a place stage's flag: give --place-stages too")
        );
        assert!(value.is_none_or(|v| !err.contains(&v)), "{err}");
    }

    // --help names every flag and says what the slice lands.
    let usage = probe_args::usage();
    for flag in [
        "--place-stages",
        "--symbol <SYMBOL>",
        "--side <SIDE>",
        "--sole-trader",
        "--resting-cap-usd <USD>",
        "--inventory-cap-usd <USD>",
        "--away-bps <BPS>",
        "--flatten-cap-bps <BPS>",
        "--flatten-attempts <N>",
        "--namespace <N>",
        "--lease-dir <DIR>",
        "--agree-wait-ms <MS>",
        "--read-interval-ms <MS>",
        "--touch-max-age-ms <MS>",
    ] {
        assert!(usage.contains(flag), "{flag}");
    }
    let usage = usage.split_whitespace().collect::<Vec<_>>().join(" ");
    for said in [
        "With --place-stages (FBC-c6kw, the first slice of FBC-9sk2; decision 0148) it runs the \
         place stages' preflight alone and places nothing",
        "only against a loopback test stub, every origin (trade, REST and MDS) a loopback address",
        "It prints a CAPS line with the environment and both caps first, before any request",
        "the place stages after the preflight are not built yet, so nothing authenticated \
         connects for a place stage",
        "$HOME/.fueledbychai/qfex_probe_production",
    ] {
        assert!(usage.contains(said), "{said}");
    }
}

#[test]
fn a_place_stage_flag_given_twice_is_refused_naming_it_and_never_its_value() {
    let given = parse_only();
    for (flag, value) in given.iter().cloned() {
        if !PLACE_FLAGS.contains(&flag) {
            continue;
        }
        // The second given with another value: neither is kept, and neither is shown.
        let other = match flag {
            "--side" => Some("sell".to_owned()),
            "--lease-dir" => Some("/abs/elsewhere".to_owned()),
            "--symbol" => Some("SYN-OTHER".to_owned()),
            _ => value.as_ref().map(|_| "7777".to_owned()),
        };
        let mut argv = given.clone();
        argv.push((flag, other.clone()));
        let err = refusal(&argv);
        assert_eq!(
            err,
            format!(
                "{flag} is given twice: each place-stage flag is given once (the values are not \
                 shown)"
            ),
            "{flag}"
        );
        for shown in value.iter().chain(other.iter()) {
            assert!(!err.contains(shown.as_str()), "{flag}: {err}");
        }
    }
}

/// Every flag a place-stage run alone takes, `--place-stages` aside.
const PLACE_FLAGS: [&str; 13] = [
    "--symbol",
    "--side",
    "--sole-trader",
    "--resting-cap-usd",
    "--inventory-cap-usd",
    "--away-bps",
    "--flatten-cap-bps",
    "--flatten-attempts",
    "--namespace",
    "--lease-dir",
    "--agree-wait-ms",
    "--read-interval-ms",
    "--touch-max-age-ms",
];

#[test]
fn place_stages_without_production_are_refused_naming_the_reason() {
    let err = refusal(&without(parse_only(), "--production"));
    assert!(
        err.starts_with("--place-stages runs on QFEX production alone: give --production."),
        "{err}"
    );
    assert!(
        err.contains("No pre-production place path is built (FBC-fqv5 item 1)"),
        "{err}"
    );
    assert!(err.contains("decision 0144 item 6"), "{err}");
    // With --production, the same command line is a place-stage run.
    assert!(parsed(&parse_only()).unwrap().0.production);
}

#[test]
fn a_testnet_lease_directory_is_refused_for_place_stages() {
    let home = || Some(std::ffi::OsString::from("/home/synthetic"));
    let testnet_dir = testnet::lease_dir_under(home()).unwrap();
    let argv = set(parse_only(), "--lease-dir", testnet_dir.to_str().unwrap());
    let err = refusal(&argv);
    assert!(
        err.starts_with("--lease-dir: the Paradex testnet samples' default lease directory"),
        "{err}"
    );
    assert!(
        !err.contains("/home/synthetic") && err.contains("the path is not shown"),
        "{err}"
    );
    // A trailing slash is the same directory.
    let slashed = format!("{}/", testnet_dir.to_str().unwrap());
    assert!(refusal(&set(parse_only(), "--lease-dir", &slashed)).contains("testnet samples'"));
    // A path with a `..` component is refused whatever it names, the testnet samples' default
    // spelled through one included, never shown.
    for dotted in [
        "/home/synthetic/.fueledbychai/x/../testnet_trade",
        "/home/synthetic/leases/../own",
    ] {
        let err = refusal(&set(parse_only(), "--lease-dir", dotted));
        assert_eq!(
            err,
            "--lease-dir: a path with a `..` component, which could name the Paradex testnet \
             samples' default lease directory: give it without one (the path is not shown)"
        );
    }
    // A symbolic link to the testnet samples' default, where it exists, is that directory.
    let (real_home, _removed) = temp_dir("place-lease-link");
    let testnet_real = testnet::lease_dir_under(Some(real_home.clone().into())).unwrap();
    fs::create_dir_all(&testnet_real).unwrap();
    let link = real_home.join("link");
    std::os::unix::fs::symlink(&testnet_real, &link).unwrap();
    let with_home = |dir: &Path| {
        let argv = set(parse_only(), "--lease-dir", dir.to_str().unwrap());
        probe_args::parse_with_home(flat(&argv), Some(real_home.clone().into()))
    };
    let err = with_home(&link).unwrap_err();
    assert!(
        err.starts_with("--lease-dir: the Paradex testnet samples'"),
        "{err}"
    );
    // Another existing directory is not, and one that does not exist is compared as written.
    let other = real_home.join("other");
    fs::create_dir_all(&other).unwrap();
    assert!(matches!(with_home(&other), Ok(Parsed::Places(..))));
    let absent = real_home.join("absent");
    assert!(matches!(with_home(&absent), Ok(Parsed::Places(..))));
    // Left out, the lease directory is production's own under $HOME.
    let defaulted = without(parse_only(), "--lease-dir");
    let places = parsed(&defaulted).unwrap().1;
    let own = Path::new("/home/synthetic/.fueledbychai/qfex_probe_production");
    assert_eq!(places.lease_dir, own);
    assert_ne!(places.lease_dir, testnet_dir);
    // With $HOME unset or relative, it must be given, and a given one is taken.
    for home in [None, Some(std::ffi::OsString::from("relative"))] {
        let err = probe_args::parse_with_home(flat(&defaulted), home.clone()).unwrap_err();
        assert!(err.contains("give --lease-dir"), "{err}");
        let given = probe_args::parse_with_home(flat(&parse_only()), home);
        assert!(matches!(given, Ok(Parsed::Places(..))), "{given:?}");
    }
}

#[test]
fn a_span_over_an_hour_is_refused_with_place_stages() {
    let argv = set(parse_only(), "--registry-span", "3601");
    let err = refusal(&set(argv, "--ledger-max-age", "3602"));
    assert_eq!(
        err,
        "--registry-span: at most 3600 s with --place-stages (decision 0148 item 7; the value is \
         not shown)"
    );
    let argv = set(parse_only(), "--registry-span", "3600");
    let (opts, _) = parsed(&set(argv, "--ledger-max-age", "3601")).unwrap();
    let bounds = opts.ledger.unwrap();
    assert_eq!(bounds.span(), Duration::from_secs(3600));
    // Stage one keeps its day.
    let stage_one = [
        "--evidence-dir",
        "/abs/probe",
        "--arm",
        "primary",
        "--registry-span",
        "3601",
        "--ledger-max-age",
        "3602",
        "--ledger-max-entries",
        "1",
    ];
    let stage_one = probe_args::parse(stage_one.iter().map(|a| (*a).to_owned()));
    assert!(matches!(stage_one, Ok(Parsed::Run(_))), "{stage_one:?}");
}

#[test]
fn a_place_stage_run_needs_no_arm() {
    let (opts, _) = parsed(&parse_only()).unwrap();
    assert!(opts.arms.is_empty() && opts.ledger.is_some());
    // A run with --arm too is refused: the place stages arm their own sessions.
    let err = refusal(&set(parse_only(), "--arm", "primary"));
    assert!(err.starts_with("--place-stages and --arm:"), "{err}");
    // A hold may come first.
    let (held, _) = parsed(&set(parse_only(), "--hold-secs", "1")).unwrap();
    let hold = probe_args::Hold {
        duration: Duration::from_secs(1),
        account: Armed::Primary,
    };
    assert_eq!(held.hold, Some(hold));
    // With neither --arm, --hold-secs nor --place-stages, nothing is asked.
    let err = probe_args::parse(flat(&vec![("--evidence-dir", Some("/d".to_owned()))]));
    let err = err.unwrap_err();
    assert!(
        err.contains("--arm or --hold-secs is required, or --place-stages"),
        "{err}"
    );
}

#[test]
fn each_ledger_flag_is_required_with_place_stages() {
    for flag in [
        "--registry-span",
        "--ledger-max-age",
        "--ledger-max-entries",
    ] {
        let err = refusal(&without(parse_only(), flag));
        assert_eq!(
            err,
            format!("{flag} is required: it has no default"),
            "{flag}"
        );
    }
    // Parse-time checks hold as with --arm: an age not longer than the span.
    let err = refusal(&set(parse_only(), "--ledger-max-age", "600"));
    assert!(
        err.starts_with("--ledger-max-age must be longer than --registry-span"),
        "{err}"
    );
}

#[tokio::test]
async fn the_place_stage_span_floor_counts_both_drop_sessions_and_the_reads_between_them() {
    // Each drop session: 1 s to connect, QFEX's 60 s authentication, the longer of the 0.6 s
    // request deadline and the resync's 30 s, and 5 s of read lag; twice, with a 1 s read of each
    // named account between them.
    let (opts, places) = parsed(&parse_only()).unwrap();
    let session = Duration::from_secs(1 + 60 + 30 + 5);
    let floor = |accounts: u32| session * 2 + Duration::from_secs(1) * accounts;
    assert_eq!(probe_args::place_span_floor(&opts, &places, 2), floor(2));
    assert_eq!(probe_args::place_span_floor(&opts, &places, 1), floor(1));
    let with_span = |span: u64| {
        let argv = set(parse_only(), "--registry-span", &span.to_string());
        parsed(&set(argv, "--ledger-max-age", &(span + 1).to_string()))
            .unwrap()
            .0
    };
    for (span, accounts, taken) in [
        (194, 2, false),
        (195, 2, true),
        (193, 1, false),
        (194, 1, true),
    ] {
        let checked = probe_args::check_place_span(&with_span(span), &places, accounts);
        match checked {
            Ok(()) if taken => {}
            Err(err) if !taken => {
                assert!(err.starts_with("--registry-span must be longer than the drop test's two"));
                assert!(
                    err.contains("the reads of every named account between them"),
                    "{err}"
                );
                assert!(!err.contains(&span.to_string()), "{err}");
            }
            other => panic!("{span} {accounts}: {other:?}"),
        }
    }
    // An arm's deadline longer than the resync's raises it by the difference, in each session.
    let slow = parsed(&set(parse_only(), "--rpc-timeout-ms", "45000"))
        .unwrap()
        .0;
    let raised = probe_args::place_span_floor(&slow, &places, 2);
    assert_eq!(raised, floor(2) + Duration::from_secs(15) * 2);
    // A longer read lag raises it.
    let lagging = parsed(&set(parse_only(), "--agree-wait-ms", "9000"))
        .unwrap()
        .1;
    let raised = probe_args::place_span_floor(&opts, &lagging, 2);
    assert_eq!(raised, floor(2) + Duration::from_secs(8));

    // The run reads how many accounts its configuration names: two with the subaccount set, so
    // a 194 s span is refused before anything connects; one without it, so it is taken.
    let (dir, _removed) = temp_dir("place-floor");
    let venue = Venue::start(QfexModel::default()).await;
    let argv = set(argv(&dir, &venue), "--registry-span", "194");
    let argv = set(argv, "--ledger-max-age", "195");
    let (ran, _) = place_run(&venue, &argv, &environment()).await;
    let err = ran.unwrap_err();
    assert!(
        err.starts_with("--registry-span must be longer than the drop test's two"),
        "{err}"
    );
    assert!(venue.log().is_empty() && !dir.join("evidence").exists());
    let (ran, printed) = place_run(&venue, &argv, &primary_only()).await;
    let ev = stopped(ran, &printed);
    assert_eq!(ev.stopped.as_deref(), Some(stage::NOT_BUILT));
    assert_eq!(ev.accounts, ["primary"]);
    venue.assert_served();
}

// ----- The CAPS line and the hold -----

#[tokio::test]
async fn caps_are_printed_before_the_first_request_and_before_the_hold() {
    let (dir, _removed) = temp_dir("place-caps-first");
    let venue = Venue::start(QfexModel::default()).await;
    let argv = set(argv(&dir, &venue), "--hold-secs", "1");
    let (ran, printed) = place_run(&venue, &argv, &environment()).await;
    let ev = stopped(ran, &printed);
    assert_eq!(ev.stopped.as_deref(), Some(stage::NOT_BUILT), "{printed}");
    // The first line, before every other, is the CAPS line, with the environment and both caps.
    let first = printed.lines().next().unwrap();
    assert_eq!(
        first,
        "CAPS on QFEX production: --resting-cap-usd 1000 USD, --inventory-cap-usd 2000 USD: the \
         operator's own (decision 0113), printed before any request and written to neither the \
         report nor the evidence"
    );
    // Every request and connection, the hold's reads first among them, came after it.
    let log = venue.log();
    assert!(
        log.first().unwrap().what.starts_with("GET /user/positions"),
        "{log:?}"
    );
    assert!(log.iter().all(|e| e.caps_printed), "{log:?}");
    let at = |line: &str| {
        printed
            .find(line)
            .unwrap_or_else(|| panic!("{line}: {printed}"))
    };
    assert!(at("CAPS ") < at("PROBE place stages"));
    assert!(at("PROBE place stages") < at("READ before the hold"));
    assert!(at("READ before the hold") < at("HOLD primary"));
    assert!(at("HOLD primary") < at("PREFLIGHT"));
    assert_no_listing(&venue);
    venue.assert_served();
}

#[tokio::test]
async fn the_cap_values_reach_stdout_alone() {
    let (dir, _removed) = temp_dir("place-caps-alone");
    let venue = Venue::start(QfexModel::default()).await;
    let argv = set(argv(&dir, &venue), "--resting-cap-usd", "424242.17");
    let argv = set(argv, "--inventory-cap-usd", "373737.29");
    let (ran, printed) = place_run(&venue, &argv, &environment()).await;
    let ev = stopped(ran, &printed);
    assert_eq!(
        ev.preflight.unwrap().passed,
        [DISCOVERED, COUNTED, TOUCHED, CAPPED]
    );
    assert!(
        printed.contains("424242.17") && printed.contains("373737.29"),
        "{printed}"
    );
    // No file the run wrote holds either cap, the distance, the flatten cap, the symbol or the
    // namespace: the report and the evidence state fences met, never a value (FBC-fqv5 item 10).
    let written = files(&dir.join("evidence"));
    assert!(written.iter().any(|(p, _)| p.ends_with("report.md")));
    assert!(written.iter().any(|(p, _)| p.ends_with("evidence.json")));
    for (path, text) in written {
        for value in ["424242", "373737", SYMBOL, "--away-bps 50", "--namespace 7"] {
            assert!(!text.contains(value), "{}: {value}", path.display());
        }
    }
    let (report, json) = outputs_of(&dir);
    assert!(report.contains("--resting-cap-usd and --inventory-cap-usd set and positive"));
    assert!(json.contains("\"fences_met\""), "{json}");
    assert_report_and_evidence_pass_the_privacy_check(&dir.join("evidence"));
    assert_no_secret(&dir.join("evidence"));
}

#[tokio::test]
async fn a_hold_runs_before_the_preflight_with_nothing_else_open() {
    let (dir, _removed) = temp_dir("place-hold-first");
    let venue = Venue::start(QfexModel::default()).await;
    let argv = set(argv(&dir, &venue), "--hold-secs", "1");
    let (ran, printed) = place_run(&venue, &argv, &environment()).await;
    let ev = stopped(ran, &printed);
    // The hold did what decision 0146 asks, and the preflight ran after it to its end.
    let held = ev.hold.as_ref().unwrap();
    assert!(held.ok(), "{held:#?}");
    assert_eq!(held.ended_by, hold::End::Duration);
    assert_eq!(
        ev.preflight.unwrap().passed,
        [DISCOVERED, COUNTED, TOUCHED, CAPPED]
    );
    assert_eq!(ev.stopped.as_deref(), Some(stage::NOT_BUILT));

    // The hold's connection, the one trade socket that subscribed, opened alone: every read
    // socket before it closed, and no MDS connection or /refdata read yet.
    let log = venue.log();
    let what: Vec<&str> = log.iter().map(|e| e.what.as_str()).collect();
    let subscribed = what
        .iter()
        .find(|w| w.ends_with(" subscribe") && w.starts_with("trade "));
    let conn = subscribed.unwrap().split(' ').nth(1).unwrap();
    let auth = log
        .iter()
        .find(|e| e.what == format!("trade {conn} auth"))
        .unwrap();
    assert_eq!(auth.alone, Some(true), "{log:?}");
    // Every read and the hold come before /refdata, and the MDS connection after it.
    let refdata = what.iter().position(|w| *w == "GET /refdata").unwrap();
    let last_trade = what
        .iter()
        .rposition(|w| w.starts_with("trade ") || w.contains("positions"));
    assert!(last_trade.unwrap() < refdata, "{what:?}");
    let first_mds = what.iter().position(|w| w.starts_with("mds ")).unwrap();
    assert!(refdata < first_mds, "{what:?}");
    // The hold's connection was closed when the MDS one opened: nothing else was open.
    assert!(venue.trade.connections().iter().all(|c| c.client_closed));
    let reads: Vec<&str> = ev.reads.iter().map(|r| r.before).collect();
    assert_eq!(
        reads,
        ["the hold", "the hold", "after the hold", "after the hold"]
    );
    let (report, _) = outputs_of(&dir);
    assert!(
        report.starts_with("# QFEX production place stages: the preflight\n"),
        "{report}"
    );
    assert!(report.contains("## The liveness hold (FBC-39ks, decision 0146)"));
    assert_no_listing(&venue);
    venue.assert_served();
}

#[tokio::test]
async fn the_owners_interrupt_on_the_hold_stops_a_place_run_before_its_preflight() {
    let (dir, _removed) = temp_dir("place-interrupt");
    let venue = Venue::start(QfexModel::default()).await;
    let argv = set(argv(&dir, &venue), "--hold-secs", "3600");
    let (mut opts, places) = parsed(&argv).unwrap();
    opts.rate_user = user();
    let env = environment();
    let lookup = |var: &str| env.get(var).cloned();
    let printed = venue.printed();
    let out: stage::Out = Rc::new(RefCell::new(printed.clone()));
    let (press, pressed) = tokio::sync::oneshot::channel::<()>();
    let interrupt: stage::Interrupt = Box::pin(async move {
        let _ = pressed.await;
    });
    // Pressed once the hold's three subscriptions are in and their confirmations have had a
    // second to arrive, so the hold did what decision 0146 asks before the interrupt ended it.
    let subscribed = || {
        let log = venue.log();
        let is = |e: &&model::Event| e.what.starts_with("trade ") && e.what.ends_with(" subscribe");
        log.iter().filter(is).count() >= 3
    };
    let drive = async {
        until("the hold's subscriptions", subscribed).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
        press.send(()).unwrap();
    };
    let ran = stage::run_places_until(&opts, &places, &lookup, out, interrupt);
    let (ran, ()) = tokio::join!(ran, drive);
    let ev = stopped(ran, &printed.text());
    let held = ev.hold.as_ref().unwrap();
    assert!(
        held.ok() && held.ended_by == hold::End::Interrupted,
        "{held:#?}"
    );
    assert_eq!(held.read_after, hold::ReadAfter::Clean);
    assert_eq!(
        ev.stopped.as_deref(),
        Some("the owner's interrupt (Ctrl-C) ended the hold: no preflight starts")
    );
    // No preflight: no /refdata read and no MDS connection.
    assert_eq!(ev.preflight, None);
    assert!(venue.log().iter().all(|e| e.what != "GET /refdata"));
    assert!(venue.mds.connections().is_empty());
    // The evidence says the run asked for the place stages, though none of them started.
    let (_, json) = outputs_of(&dir);
    let json = super::json(&json);
    assert_eq!(json["asked"]["place_stages"], true);
    assert_eq!(json["place_stages"], Value::Null);
    assert_no_listing(&venue);
    venue.assert_served();
}

// ----- The spec and the touch -----

#[tokio::test]
async fn the_spec_comes_from_this_runs_refdata() {
    let (dir, _removed) = temp_dir("place-spec");
    let venue = Venue::start(QfexModel::default()).await;
    let (opts, places) = parsed(&argv(&dir, &venue)).unwrap();
    let env = environment();
    let lookup = |var: &str| env.get(var).cloned();
    let (cfg, _) = stage::configured(&stage::base_config(&opts), &lookup, Armed::Primary).unwrap();
    let cfg = market::market_config(&cfg, opts.read_wait);
    assert_eq!(cfg.get(fbc_venue_qfex::KEY_REST_TIMEOUT), Some("1000ms"));
    let connector = fbc_runtime::Connector::new(ProxyConfig::Direct);
    let reserve = SafetyReserve::percent(10).unwrap();
    let limiter = RateLimiter::new(&[], reserve).unwrap();
    let found = market::discover_spec(&cfg, &places.symbol, &connector, &limiter).await;
    let found = found.unwrap();
    // The spec is the listing's: its symbol, grid, size step and minimum, in the run's one spec
    // table under the program's instrument id.
    assert_eq!(found.inst, market::INST);
    assert_eq!(found.spec.venue_symbol.as_wire(), SYMBOL);
    assert_eq!(found.tick, Decimal::new(1, 2));
    assert_eq!(found.spec.size_step.get(), Decimal::new(1, 2));
    assert_eq!(found.min_size, Lots::new(1).unwrap());
    assert_eq!(found.specs.get(market::INST), Some(&found.spec));
    assert_eq!(found.px(Ticks(10_010)), Decimal::new(10_010, 2));
    assert_eq!(venue.trade.http_requests(), ["GET /refdata"]);

    // Read again after QFEX changed its listing: the run takes this read's, not an earlier one.
    {
        let mut model = venue.model.lock().unwrap();
        model.listed[0].lot = "0.001";
        model.listed[0].min = "0.005";
    }
    let again = market::discover_spec(&cfg, &places.symbol, &connector, &limiter).await;
    assert_eq!(again.unwrap().min_size, Lots::new(5).unwrap());

    // A run reads it once, before its touch, and passes on it.
    let (ran, printed) = place_run(&venue, &argv(&dir, &venue), &env).await;
    let ev = stopped(ran, &printed);
    assert_eq!(ev.preflight.unwrap().passed[0], DISCOVERED);
    let refdata = venue
        .log()
        .iter()
        .filter(|e| e.what == "GET /refdata")
        .count();
    assert_eq!(refdata, 3);
    venue.assert_served();
}

#[tokio::test]
async fn a_halted_unlisted_or_one_shape_symbol_is_refused_before_any_account_connects() {
    let listed = |change: &dyn Fn(&mut Listed)| {
        let mut listed = Listed::trading();
        change(&mut listed);
        listed
    };
    let cases: [(Listed, &str); 6] = [
        (
            listed(&|l| {
                l.symbol = "OTH-USD";
                l.base = "OTH";
            }),
            "is not listed in this run's answer",
        ),
        (
            listed(&|l| l.status = "INACTIVE"),
            "is listed Halted, not trading",
        ),
        (
            listed(&|l| l.status = "DELISTED"),
            "is listed Delisted, not trading",
        ),
        (
            listed(&|l| l.types = json(r#"["LIMIT"]"#)),
            "does not list both a post-only GTC",
        ),
        (
            listed(&|l| l.tifs = json(r#"["GTC"]"#)),
            "does not list both a post-only GTC",
        ),
        (
            listed(&|l| l.types = Value::Null),
            "does not list both a post-only GTC",
        ),
    ];
    for (listed, says) in cases {
        let (dir, _removed) = temp_dir("place-symbol");
        let model = QfexModel {
            listed: vec![listed],
            ..QfexModel::default()
        };
        let venue = Venue::start(model).await;
        let (ran, printed) = place_run(&venue, &argv(&dir, &venue), &environment()).await;
        let ev = stopped(ran, &printed);
        let why = ev.stopped.unwrap();
        assert!(
            why.starts_with("GET /refdata: the instrument --symbol names"),
            "{why}"
        );
        assert!(why.contains(says) && !why.contains(SYMBOL), "{why}");
        assert!(ev.preflight.unwrap().passed.is_empty());
        assert_no_account(&venue);
        assert!(venue.mds.connections().is_empty());
        let (report, json) = outputs_of(&dir);
        assert!(report.contains("Passed, in order: none."), "{report}");
        assert!(!report.contains(SYMBOL) && !json.contains(SYMBOL));
        venue.assert_served();
    }
}

#[tokio::test]
async fn preflight_discovery_and_touch_take_the_guarded_origins() {
    let (dir, _removed) = temp_dir("place-origins");
    let venue = Venue::start(QfexModel::default()).await;
    let (ran, printed) = place_run(&venue, &argv(&dir, &venue), &environment()).await;
    let ev = stopped(ran, &printed);
    assert_eq!(
        ev.preflight.unwrap().passed,
        [DISCOVERED, COUNTED, TOUCHED, CAPPED]
    );
    assert_eq!(ev.stopped.as_deref(), Some(stage::NOT_BUILT));
    assert!(ev.sessions.is_empty() && ev.registries.is_empty() && ev.reads.is_empty());
    assert_eq!(
        (ev.environment, ev.target),
        ("production", "a loopback test stub")
    );
    // Discovery on the guarded REST origin, the touch on the guarded MDS origin, which is the
    // MDS stub's own: one connection, which subscribed to the instrument's bbo alone. Nothing
    // reached the trade origin, and no account was read.
    assert_eq!(venue.trade.http_requests(), ["GET /refdata"]);
    assert_no_account(&venue);
    let mds = venue.mds.connections();
    assert_eq!(mds.len(), 1);
    let subscribe = r#"{"type":"subscribe","channels":["bbo"],"symbols":["SYN-USD"]}"#;
    let [Frame::Text(sent)] = mds[0].received.as_slice() else {
        panic!("{:?}", mds[0].received);
    };
    assert_eq!(super::json(sent), super::json(subscribe));
    for line in [
        "PROBE place stages on QFEX production (production_probe), against a loopback test stub: \
         the preflight alone (FBC-c6kw)",
        "PREFLIGHT the instrument's spec from GET /refdata on the guarded REST origin",
        "PREFLIGHT the touch from the guarded MDS origin",
        "PREFLIGHT passed: the spec, the count bound, the touch and the caps",
        &format!("STOPPED {}", stage::NOT_BUILT),
    ] {
        assert!(printed.contains(line), "{line}: {printed}");
    }
    let (report, json) = outputs_of(&dir);
    assert!(
        report.contains("## The place stages' preflight (FBC-c6kw)"),
        "{report}"
    );
    assert!(
        report.contains(&format!("Passed, in order: {DISCOVERED}; {COUNTED}")),
        "{report}"
    );
    assert!(report.contains("The place stages after the preflight are not built yet"));
    let json = super::json(&json);
    assert_eq!(json["place_stages"]["passed"].as_array().unwrap().len(), 4);
    assert_eq!(
        json["asked"],
        super::json(r#"{"arms":[],"hold":false,"place_stages":true}"#)
    );
    // No session opened, since the place stages after the preflight are not built: not that the
    // run asked for none.
    assert!(
        report.contains(
            "## The sessions\n\nNone: no session opened: the place stages after the preflight \
             are not built yet (FBC-23ch).\n"
        ),
        "{report}"
    );
    assert!(!report.contains("the run asked for no session"), "{report}");
    assert_eq!(json["ok"], false);
    assert_report_and_evidence_pass_the_privacy_check(&dir.join("evidence"));
    assert_no_listing(&venue);
    venue.assert_served();

    // An origin the guard refuses, here the MDS origin production's beside loopback ones, is
    // refused before anything connects, naming its key.
    let (dir, _removed) = temp_dir("place-origins-mixed");
    let fresh = Venue::start(QfexModel::default()).await;
    let mixed = set(argv(&dir, &fresh), "--mds-url", "wss://mds.qfex.com");
    let (ran, _) = place_run(&fresh, &mixed, &environment()).await;
    let err = ran.unwrap_err();
    assert!(
        err.contains("qfex.mds_url") && err.contains("mixes a loopback origin"),
        "{err}"
    );
    assert!(fresh.log().is_empty() && fresh.mds.connections().is_empty());
}

#[tokio::test]
async fn a_stale_or_crossed_touch_is_refused() {
    let touch = |change: &dyn Fn(&mut model::Touch)| {
        let mut touch = model::Touch::fresh();
        change(&mut touch);
        touch
    };
    let cases = [
        (touch(&|t| t.age_secs = 3600), false, STALE),
        (touch(&|t| t.bid = Some(("100.20", "1.00"))), false, CROSSED),
        (touch(&|t| t.bid = Some(("100.10", "1.00"))), false, CROSSED),
        (touch(&|t| t.ask = None), false, ONE_SIDED),
        (touch(&|_| {}), true, NO_TOUCH),
    ];
    for (touch, mds_err, says) in cases {
        let (dir, _removed) = temp_dir("place-touch");
        let model = QfexModel {
            touch,
            mds_err,
            ..QfexModel::default()
        };
        let venue = Venue::start(model).await;
        let (ran, printed) = place_run(&venue, &argv(&dir, &venue), &environment()).await;
        let ev = stopped(ran, &printed);
        assert_eq!(ev.stopped.as_deref(), Some(says), "{printed}");
        assert_eq!(ev.preflight.unwrap().passed, [DISCOVERED, COUNTED]);
        assert_no_account(&venue);
        assert_eq!(venue.mds.connections().len(), 1);
        venue.assert_served();
    }
}

#[test]
fn the_touch_is_judged_on_its_epoch_its_sides_and_its_age() {
    let conn = |epoch: u32| ConnKey { conn: 9, epoch };
    let lvl = |px: i64| Lvl {
        px: Ticks(px),
        qty: Lots::new(1).unwrap(),
    };
    let touch = |key: ConnKey, inst: InstrumentId, bid: Option<Lvl>, exch: Option<i64>| Envelope {
        stamp: Stamp {
            ingest_seq: 1,
            kernel_rx: None,
            recv_mono: MonoNs(0),
            recv_wall: WALL,
            conn: key,
        },
        exch_ts: exch.map(ExchNs),
        exch_ts_kind: ExchTsKind::Unknown,
        venue_seq: None,
        body: MdEvent::Touch {
            inst,
            bid,
            ask: Some(lvl(10_010)),
            source: fbc_venue_qfex::TOUCH_BBO,
        },
    };
    let now = tokio::time::Instant::now();
    let age = Duration::from_secs(5);
    let mut state = TouchState::default();
    assert_eq!(state.judge(now, WALL, age), Err(NO_TOUCH.to_owned()));
    assert!(!state.current());
    // Another instrument's touch, another source's and an event that is no touch are passed
    // over.
    let other = touch(conn(1), InstrumentId::new(2), Some(lvl(10_000)), None);
    assert!(!state.heard(&other, market::INST, now));
    let mut source = touch(conn(1), market::INST, Some(lvl(10_000)), None);
    if let MdEvent::Touch { source: s, .. } = &mut source.body {
        *s = TouchSourceId(7);
    }
    assert!(!state.heard(&source, market::INST, now));
    let mut status = touch(conn(1), market::INST, None, None);
    status.body = MdEvent::Status {
        inst: market::INST,
        status: fbc_core::TradingStatus::Trading,
    };
    assert!(!state.heard(&status, market::INST, now));
    assert_eq!(state, TouchState::default());

    // A touch of the current epoch, stamped by QFEX at its arrival: priced from.
    let ours = touch(conn(1), market::INST, Some(lvl(10_000)), Some(WALL.0));
    assert!(state.heard(&ours, market::INST, now));
    assert!(state.current());
    assert_eq!(state.judge(now, WALL, age), Ok(QUOTE));
    // Too long since it arrived, or since QFEX stamped it: stale.
    assert_eq!(state.judge(now + age * 2, WALL, age), Err(STALE.to_owned()));
    let later = WallNs(WALL.0 + 6_000_000_000);
    assert_eq!(state.judge(now, later, age), Err(STALE.to_owned()));
    // A stamp ahead of the local clock leaves the age to its arrival.
    let ahead = touch(
        conn(1),
        market::INST,
        Some(lvl(10_000)),
        Some(WALL.0 + 60_000_000_000),
    );
    state.heard(&ahead, market::INST, now);
    assert_eq!(state.judge(now, WALL, age), Ok(QUOTE));
    // Its epoch ended: from before the current epoch, until the next epoch's first touch.
    state.ended(conn(2));
    assert!(state.current());
    state.ended(conn(1));
    assert!(!state.current());
    assert_eq!(state.judge(now, WALL, age), Err(BEFORE_EPOCH.to_owned()));
    let next = touch(conn(2), market::INST, None, None);
    state.heard(&next, market::INST, now);
    assert_eq!(state.judge(now, WALL, age), Err(ONE_SIDED.to_owned()));
    let crossed = touch(conn(2), market::INST, Some(lvl(10_011)), None);
    state.heard(&crossed, market::INST, now);
    assert_eq!(state.judge(now, WALL, age), Err(CROSSED.to_owned()));
    let _ = VenueMeta::NONE;
}

/// A discovery request for `/refdata` on `127.0.0.1:<port>` with `headers`, answered within
/// `timeout`, charged as the adapter's discovery charges it.
fn refdata_get(port: u16, headers: Vec<fbc_core::Header>, timeout: Duration) -> Effect {
    Effect::Http {
        tag: REFDATA_TAG,
        req: HttpRequest {
            method: HttpMethod::Get,
            url: fbc_core::WireUrl::plain(format!("http://127.0.0.1:{port}/refdata")),
            headers,
            body: fbc_core::WireSlice::plain(Vec::new()),
        },
        rpc: None,
        timeout,
        class: fbc_core::TrafficClass::Normal,
        charge: fbc_core::RateCharge::one(fbc_core::OpKind::Rest, None),
    }
}

#[tokio::test]
async fn discovery_makes_only_a_bare_public_get() {
    let connector = fbc_runtime::Connector::new(ProxyConfig::Direct);
    let reserve = SafetyReserve::percent(10).unwrap();
    let limiter = RateLimiter::new(&[], reserve).unwrap();
    let get = |headers| refdata_get(2, headers, Duration::from_secs(1));
    let header = vec![fbc_core::Header {
        name: "x-synthetic",
        value: "1".to_owned(),
        redact: false,
    }];
    let refused = market::fetch(&[get(header)], &connector, &limiter).await;
    let err = refused.unwrap_err();
    assert_eq!(
        err,
        "GET /refdata: the plan asks for a request the probe does not make"
    );
    let send = Effect::Reconnect {
        stream: fbc_core::StreamId(0),
        reason: "synthetic",
    };
    let err = market::fetch(&[send], &connector, &limiter)
        .await
        .unwrap_err();
    assert_eq!(err, "GET /refdata: the plan asks for something not HTTP");
}

#[tokio::test]
async fn discovery_is_charged_to_the_limiter_it_is_given_and_refused_with_no_room() {
    let (dir, _removed) = temp_dir("place-discovery-room");
    let venue = Venue::start(QfexModel::default()).await;
    let (opts, places) = parsed(&argv(&dir, &venue)).unwrap();
    let env = environment();
    let lookup = |var: &str| env.get(var).cloned();
    let (cfg, _) = stage::configured(&stage::base_config(&opts), &lookup, Armed::Primary).unwrap();
    let cfg = market::market_config(&cfg, opts.read_wait);
    let connector = fbc_runtime::Connector::new(ProxyConfig::Direct);
    // A limiter with room for one REST request an hour: the first read is charged and sent, the
    // second refused before it is sent, and the refusal counted on that limiter.
    let one = fbc_core::RateLimit {
        scope: fbc_core::LimitScope::Ip,
        ops: fbc_core::TagSet::of(&[fbc_core::OpKind::Rest]),
        per: Duration::from_secs(3_600),
        units: 1,
    };
    let limiter = RateLimiter::new(&[one], SafetyReserve::percent(0).unwrap()).unwrap();
    let found = market::discover_spec(&cfg, &places.symbol, &connector, &limiter).await;
    assert_eq!(found.unwrap().spec.venue_symbol.as_wire(), SYMBOL);
    assert_eq!(limiter.counts().refused.ip, 0);
    let refused = market::discover_spec(&cfg, &places.symbol, &connector, &limiter).await;
    assert_eq!(
        refused.unwrap_err(),
        "GET /refdata: not sent: the rate limit has no room"
    );
    assert_eq!(limiter.counts().refused.ip, 1);
    assert_eq!(venue.trade.http_requests(), ["GET /refdata"]);
    venue.assert_served();
}

#[tokio::test]
async fn discovery_refuses_a_late_answer_and_a_failed_connection() {
    let connector = fbc_runtime::Connector::new(ProxyConfig::Direct);
    let limiter = RateLimiter::new(&[], SafetyReserve::percent(10).unwrap()).unwrap();
    // A listener that never accepts: the connection opens in its backlog and the request is
    // never answered, so it is refused at its timeout.
    let silent = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = silent.local_addr().unwrap().port();
    let late = refdata_get(port, Vec::new(), Duration::from_millis(200));
    let err = market::fetch(&[late], &connector, &limiter)
        .await
        .unwrap_err();
    assert_eq!(err, "GET /refdata: no answer within 200 ms");
    drop(silent);
    // Nothing listening: the connector's refusal, named as the read's.
    let closed = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let refused = refdata_get(closed, Vec::new(), Duration::from_secs(5));
    let err = market::fetch(&[refused], &connector, &limiter)
        .await
        .unwrap_err();
    assert!(
        err.starts_with("GET /refdata: ") && !err.contains("no answer within"),
        "{err}"
    );
}

#[tokio::test]
async fn a_refused_refdata_answer_stops_the_run_before_the_touch_and_any_account() {
    for (status, body) in [(500, r#"{"error":"synthetic"}"#), (200, "not json")] {
        let (dir, _removed) = temp_dir("place-refdata-refused");
        let model = QfexModel {
            refdata: Some((status, body)),
            ..QfexModel::default()
        };
        let venue = Venue::start(model).await;
        let (ran, printed) = place_run(&venue, &argv(&dir, &venue), &environment()).await;
        let ev = stopped(ran, &printed);
        let why = ev.stopped.unwrap();
        // The adapter's parse refused the answer: named as the read's, never showing the symbol.
        assert!(why.starts_with("GET /refdata: "), "{status}: {why}");
        assert!(!why.contains(SYMBOL), "{why}");
        assert!(ev.preflight.unwrap().passed.is_empty());
        assert!(venue.mds.connections().is_empty());
        assert_no_account(&venue);
        venue.assert_served();
    }
}

// ----- The prices -----

#[test]
fn a_resting_order_rests_away_bps_behind_the_touch_on_its_side() {
    let found = spec_of(Listed::trading());
    let spec = &found.spec;
    // 50 bps below the 100.00 bid is 99.50, and above the 100.10 ask 100.6005, ceiled to 100.61.
    assert_eq!(
        market::rest_price(spec, Side::Buy, QUOTE, 50),
        Ok(Ticks(9_950))
    );
    assert_eq!(
        market::rest_price(spec, Side::Sell, QUOTE, 50),
        Ok(Ticks(10_061))
    );
    // Off the grid, a buy floors and a sell ceils: never nearer the touch than asked. 50 bps
    // below 100.03 is 99.52985, floored to 99.52.
    let odd = Quote {
        bid: Ticks(10_003),
        ask: Ticks(10_009),
    };
    assert_eq!(
        market::rest_price(spec, Side::Buy, odd, 50),
        Ok(Ticks(9_952))
    );
    // 10000 bps below the bid is zero: no price.
    let err = market::rest_price(spec, Side::Buy, QUOTE, 10_000).unwrap_err();
    assert_eq!(
        err,
        "the price: no positive price on the grid on its safe side"
    );
    // A price past an i64's reach is refused.
    let huge = Quote {
        bid: Ticks(i64::MAX - 1),
        ask: Ticks(i64::MAX),
    };
    assert!(market::rest_price(spec, Side::Sell, huge, 10_000).is_err());
}

#[test]
fn crossing_orders_take_the_far_touch() {
    let found = spec_of(Listed::trading());
    // The crossing post-only and the taker IOC: a buy at the ask, a sell at the bid.
    assert_eq!(
        market::far_touch(&found.spec, Side::Buy, QUOTE),
        Ok(QUOTE.ask)
    );
    assert_eq!(
        market::far_touch(&found.spec, Side::Sell, QUOTE),
        Ok(QUOTE.bid)
    );
}

#[test]
fn a_flatten_ioc_is_capped_from_the_touch_it_crosses() {
    let found = spec_of(Listed::trading());
    let spec = &found.spec;
    let odd = Quote {
        bid: Ticks(10_003),
        ask: Ticks(10_009),
    };
    // A sell crosses the bid: 7 bps below 100.03 is 99.959979, ceiled to 99.96, so never further
    // through than the cap. Measured from the 100.09 ask instead, it would be 100.019937, ceiled
    // to 100.02.
    let sell = market::flatten_price(spec, Side::Sell, odd, 7).unwrap();
    assert_eq!(sell, Ticks(9_996));
    let bid = Decimal::from(odd.bid.0);
    let through = (bid - Decimal::from(sell.0)) / bid * Decimal::from(10_000);
    assert!(through <= Decimal::from(7) && sell <= odd.bid, "{through}");
    assert_ne!(sell, Ticks(10_002));
    // A buy crosses the ask: 7 bps above 100.09 is 100.160063, floored to 100.16.
    let buy = market::flatten_price(spec, Side::Buy, odd, 7).unwrap();
    assert_eq!(buy, Ticks(10_016));
    let ask = Decimal::from(odd.ask.0);
    let through = (Decimal::from(buy.0) - ask) / ask * Decimal::from(10_000);
    assert!(through <= Decimal::from(7) && buy >= odd.ask, "{through}");
    // A cap finer than the grid leaves the IOC at the touch it crosses: 1 bps below 1.03 is
    // 1.029897, ceiled back to the bid.
    let low = Quote {
        bid: Ticks(103),
        ask: Ticks(105),
    };
    assert_eq!(market::flatten_price(spec, Side::Sell, low, 1), Ok(low.bid));
    assert_eq!(market::flatten_price(spec, Side::Buy, low, 1), Ok(low.ask));
}

#[test]
fn prices_are_snapped_in_whole_ticks_at_a_fine_tick_and_a_high_price() {
    // A tick of 0.00000001 near 900000: a price there spans about 1e14 ticks, more than an f64
    // holds to a fraction of a tick, so each price is moved and snapped in whole ticks. Each
    // case's exact price lies a fraction of a tick off the grid, where a float would land one
    // tick on the unsafe side.
    let found = spec_of(Listed {
        tick: "0.00000001",
        ..Listed::trading()
    });
    let spec = &found.spec;
    assert_eq!(found.tick, Decimal::new(1, 8));
    // A resting buy 21 bps below the bid: 89811000002375.9999 ticks, floored.
    let buy = Quote {
        bid: Ticks(90_000_000_002_381),
        ask: Ticks(90_000_000_002_400),
    };
    let rest = market::rest_price(spec, Side::Buy, buy, 21);
    assert_eq!(rest, Ok(Ticks(89_811_000_002_375)));
    // A resting sell 21 bps above the ask: 90189000002386.0001 ticks, ceiled.
    let sell = Quote {
        bid: Ticks(90_000_000_002_300),
        ask: Ticks(90_000_000_002_381),
    };
    let rest = market::rest_price(spec, Side::Sell, sell, 21);
    assert_eq!(rest, Ok(Ticks(90_189_000_002_387)));
    // A flatten sell 7 bps through the bid: 89937000002855.0001 ticks, ceiled back toward it.
    let at = Quote {
        bid: Ticks(90_000_000_002_857),
        ask: Ticks(90_000_000_002_857),
    };
    let flatten = market::flatten_price(spec, Side::Sell, at, 7);
    assert_eq!(flatten, Ok(Ticks(89_937_000_002_856)));
    // A flatten buy 7 bps through the ask: 90063000002858.9999 ticks, floored back toward it.
    let flatten = market::flatten_price(spec, Side::Buy, at, 7);
    assert_eq!(flatten, Ok(Ticks(90_063_000_002_858)));
}

// ----- The caps and the count bound -----

/// The model's touch, a buy 50 bps behind it at 99.50: the highest price an order takes is the
/// 100.10 ask, so a lot of 0.01 costs 1.001 USD and three minimum sizes 3.003.
const THREE_MIN_USD: &str = "3.003";
const BELOW_THREE_USD: &str = "3.002";

#[tokio::test]
async fn caps_below_three_minimum_sizes_refuse_the_run_before_any_account_connects() {
    for flag in ["--resting-cap-usd", "--inventory-cap-usd"] {
        let (dir, _removed) = temp_dir("place-caps-below");
        let venue = Venue::start(QfexModel::default()).await;
        let argv = set(argv(&dir, &venue), flag, BELOW_THREE_USD);
        let (ran, printed) = place_run(&venue, &argv, &environment()).await;
        let ev = stopped(ran, &printed);
        let why = ev.stopped.unwrap();
        let says = format!("{flag} admits fewer than three of the instrument's minimum size");
        assert!(why.starts_with(&says), "{why}");
        assert!(!why.contains(BELOW_THREE_USD), "{why}");
        assert_eq!(ev.preflight.unwrap().passed, [DISCOVERED, COUNTED, TOUCHED]);
        assert_no_account(&venue);
        venue.assert_served();
    }
    // At exactly three minimum sizes, both caps pass.
    let (dir, _removed) = temp_dir("place-caps-three");
    let venue = Venue::start(QfexModel::default()).await;
    let argv = set(argv(&dir, &venue), "--resting-cap-usd", THREE_MIN_USD);
    let argv = set(argv, "--inventory-cap-usd", THREE_MIN_USD);
    let (ran, printed) = place_run(&venue, &argv, &environment()).await;
    let ev = stopped(ran, &printed);
    assert_eq!(
        ev.preflight.unwrap().passed,
        [DISCOVERED, COUNTED, TOUCHED, CAPPED]
    );
    assert_no_account(&venue);

    // A minimum size whose three would overflow a lot count admits no cap.
    let mut huge = spec_of(Listed::trading());
    huge.min_size = Lots::new(i64::MAX).unwrap();
    let places = parsed(&parse_only()).unwrap().1;
    let err = market::place_caps(&places, &huge, QUOTE).unwrap_err();
    assert!(
        err.starts_with("--resting-cap-usd admits fewer than three"),
        "{err}"
    );
}

#[test]
fn caps_of_three_minimum_sizes_admit_three_resting_orders() {
    let found = spec_of(Listed::trading());
    let argv = set(parse_only(), "--resting-cap-usd", THREE_MIN_USD);
    let (opts, places) = parsed(&set(argv, "--inventory-cap-usd", THREE_MIN_USD)).unwrap();
    let caps = market::place_caps(&places, &found, QUOTE).unwrap();
    let three = Lots::new(3).unwrap();
    assert_eq!((caps.resting, caps.inventory), (three, three));

    // Stage three's registry under those caps, armed on a flat hand seed: three resting
    // minimum-size post-only GTCs on --side are admitted, and a fourth is not.
    let bounds = market::count_bound(opts.ledger.as_ref().unwrap(), &found, &places).unwrap();
    let acct = AccountKey::new(1);
    let order: OrderCaps = probe_caps().exec.unwrap().order;
    let symbol = found.spec.venue_symbol.clone();
    let keys = LeaseKeys::new(Qfex.id(), "primary", &order).with_market(market::INST, symbol);
    let registry = ledger::PlaceRegistry::new(
        ledger::Step::Pages,
        "primary",
        acct,
        &bounds,
        opened(MonoNs(0)),
        market::INST,
        market::pre_trade_caps(market::INST, caps),
        keys,
    );
    let ladder = LadderConfig::new(
        Duration::from_secs(5),
        Duration::ZERO,
        Duration::from_secs(60),
        2,
    )
    .unwrap();
    let (mut link, _run) = registry.into_link(order, ladder);
    let (dir, _removed) = temp_dir("place-caps-admit");
    let reg = link.reg();
    reg.seed_position(market::INST, fbc_core::SignedLots(0))
        .unwrap();
    let lease = MarketLease::acquire(&dir, Qfex.id(), "primary", &found.spec.venue_symbol);
    reg.start(market::INST, Leases::market(lease.unwrap()))
        .unwrap();
    let ns = NamespaceLease::acquire(&dir, acct, Namespace::new(places.namespace)).unwrap();
    let mut mint = CidMint::new(ns, 0, 0, WallNs(0));
    let px = market::rest_price(&found.spec, places.side, QUOTE, places.away_bps).unwrap();
    let mut place = |reg: &mut fbc_oms::Registry| {
        reg.place(NewOrder {
            cid: mint.mint().unwrap(),
            inst: market::INST,
            side: places.side,
            qty: found.min_size,
            kind: OrderKind::Limit { px },
            tif: Tif::Gtc,
            channel: fbc_core::Channel::Public,
            post_only: true,
            reduce_only: false,
            reducing: false,
        })
    };
    // Each command is held, so each counts as resting when the next is built.
    let held: Vec<_> = (0..3).map(|_| place(reg).unwrap()).collect();
    assert_eq!(held.len(), 3);
    assert!(matches!(place(reg), Err(OmsError::Capped(_))));
}

#[tokio::test]
async fn a_count_bound_at_the_ceiling_is_refused_before_any_account_connects() {
    // A minimum of one size step and one flatten attempt: stage three's registry may place
    // three post-onlys and one flatten IOC, a ceiling of four, the highest of the four lists.
    let mut said = Vec::new();
    for (entries, min) in [("4", "0.01"), ("20", "0.05")] {
        let (dir, _removed) = temp_dir("place-count");
        let model = QfexModel {
            listed: vec![Listed {
                min,
                ..Listed::trading()
            }],
            ..QfexModel::default()
        };
        let venue = Venue::start(model).await;
        let argv = set(argv(&dir, &venue), "--ledger-max-entries", entries);
        let (ran, printed) = place_run(&venue, &argv, &environment()).await;
        let ev = stopped(ran, &printed);
        let why = ev.stopped.unwrap();
        assert!(
            why.starts_with("--ledger-max-entries must be above each place registry's fill"),
            "{why}"
        );
        assert_eq!(ev.preflight.unwrap().passed, [DISCOVERED]);
        // Before the touch, and before any account.
        assert!(venue.mds.connections().is_empty());
        assert_no_account(&venue);
        said.push(why);
    }
    // The refusal is the same whatever the ceiling: it shows no value, since a ceiling counts
    // --flatten-attempts (FBC-fqv5 item 10).
    assert_eq!(said[0], said[1]);
    // One above the ceiling clears it.
    let (dir, _removed) = temp_dir("place-count-above");
    let venue = Venue::start(QfexModel::default()).await;
    let argv = set(argv(&dir, &venue), "--ledger-max-entries", "5");
    let (ran, printed) = place_run(&venue, &argv, &environment()).await;
    let ev = stopped(ran, &printed);
    assert_eq!(
        ev.preflight.unwrap().passed,
        [DISCOVERED, COUNTED, TOUCHED, CAPPED]
    );
}

// ----- The session configuration -----

#[test]
fn the_key_is_set_only_on_place_stage_sessions() {
    let (opts, _) = parsed(&parse_only()).unwrap();
    let env = environment();
    let lookup = |var: &str| env.get(var).cloned();
    let base = stage::base_config(&opts);
    assert_eq!(base.get(KEY_PRODUCTION_PLACES), None);
    for armed in [Armed::Primary, Armed::Subaccount] {
        let (place, _) = stage::place_config(&base, &lookup, armed).unwrap();
        assert_eq!(place.get(KEY_PRODUCTION_PLACES), Some(PROBE_PLACE_STAGES));
        // The adapter reads it as the place stages' key under production_probe: the codec's
        // fence of decision 0148 item 4.
        let probe = fbc_venue_qfex::probe::mode(&place).unwrap().unwrap();
        assert!(probe.place_stages());
        // Otherwise it is the session's own configuration.
        let (session, _) = stage::configured(&base, &lookup, armed).unwrap();
        assert_eq!(session.get(KEY_PRODUCTION_PLACES), None);
        for key in [ACCOUNT_KEY_CFG, "qfex.account_id", KEY_EXEC_MODE, TRADE_URL] {
            assert_eq!(place.get(key), session.get(key), "{key}");
        }
    }
    // The preflight's own configuration, discovery's and the touch's, carries none either.
    let market_cfg = market::market_config(&base, opts.read_wait);
    assert_eq!(market_cfg.get(KEY_PRODUCTION_PLACES), None);
}

/// The configuration key the program numbers an account by (decision 0122).
const ACCOUNT_KEY_CFG: &str = fbc_venue_qfex::auth::ACCOUNT_KEY;

#[test]
fn a_stage_one_production_run_sets_no_key() {
    let argv = [
        "--production",
        "--evidence-dir",
        "/abs/probe",
        "--arm",
        "primary",
        "--arm",
        "subaccount",
        "--registry-span",
        "101",
        "--ledger-max-age",
        "102",
        "--ledger-max-entries",
        "1",
    ];
    let Ok(Parsed::Run(opts)) = probe_args::parse(argv.iter().map(|a| (*a).to_owned())) else {
        panic!("not a stage-one run");
    };
    let env = environment();
    let lookup = |var: &str| env.get(var).cloned();
    let base = stage::base_config(&opts);
    assert_eq!(base.get(KEY_EXEC_MODE), Some("production_probe"));
    assert_eq!(base.get(KEY_PRODUCTION_PLACES), None);
    for armed in opts.arms.iter().copied() {
        let (cfg, _) = stage::configured(&base, &lookup, armed).unwrap();
        assert_eq!(cfg.get(KEY_PRODUCTION_PLACES), None);
        // Without the key the production probe codec refuses every place (decision 0145).
        let probe = fbc_venue_qfex::probe::mode(&cfg).unwrap().unwrap();
        assert!(!probe.place_stages());
    }
}

// ----- The loopback-only gate -----

#[tokio::test]
async fn place_stages_refuse_a_non_loopback_origin_until_built() {
    let env = environment();
    // Production's own origins, the defaults with --production, and the same named: refused
    // before anything connects, naming the reason.
    let defaults = without(
        without(without(parse_only(), "--trade-url"), "--rest-url"),
        "--mds-url",
    );
    let named = set(defaults.clone(), "--trade-url", "wss://trade.qfex.com");
    let named = set(named, "--rest-url", "https://api.qfex.com");
    let named = set(named, "--mds-url", "wss://mds.qfex.com");
    // Through a SOCKS5 proxy no one listens on, as stage one's guard tests run a non-loopback
    // origin: the guard admits production's origins through a proxy, so the gate is still what
    // the run meets, and a gate that let it through would reach no QFEX host.
    let nowhere = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        ProxyConfig::Socks5 {
            host: "127.0.0.1".into(),
            port,
        }
    };
    for argv in [defaults, named] {
        let (mut opts, places) = parsed(&argv).unwrap();
        assert_eq!(opts.mds_url, "wss://mds.qfex.com");
        let (dir, _removed) = temp_dir("place-gate");
        opts.evidence_dir = dir.join("evidence");
        opts.proxy = nowhere.clone();
        let printed = Printed::default();
        let lookup = |var: &str| env.get(var).cloned();
        let out: stage::Out = Rc::new(RefCell::new(printed.clone()));
        let err = stage::run_places(&opts, &places, &lookup, out)
            .await
            .unwrap_err();
        assert_eq!(err, stage::LOOPBACK_ONLY);
        assert!(err.contains("until FBC-4plj"), "{err}");
        assert!(!opts.evidence_dir.exists());
        // The CAPS line still came first; nothing after it but the refusal.
        let text = printed.text();
        assert_eq!(text.lines().count(), 1, "{text}");
        assert!(text.starts_with("CAPS on QFEX production"));
        printed.clone().flush().unwrap();
    }
}

// ----- The model -----

#[test]
fn the_model_answers_each_read_as_qfex_does() {
    let mut model = QfexModel::default();
    let get = |path: &str, account: Option<&str>| fbc_conformance::HttpRequest {
        method: Method::GET,
        path: path.to_owned(),
        query: None,
        headers: account
            .map(|a| ("x-qfex-requested-account-id".to_owned(), a.to_owned()))
            .into_iter()
            .collect(),
        body: Vec::new(),
        params: Vec::new(),
    };
    // The listing is refused as QFEX refused a key scoped to one account; historic orders are
    // none, in the documented shape.
    assert_eq!(model.rest(&get("/user/subaccounts", None)).status, 403);
    let historic = model.rest(&get("/user/historic-orders", None));
    let resp = fbc_core::HttpResponse {
        status: historic.status,
        headers: &[],
        body: &historic.body,
    };
    let decoded = fbc_venue_qfex::account_reads::decode_historic(&resp).unwrap();
    assert_eq!(decoded.count(), 0);
    // The lag knob: each account's first reads answer its stale body, then its current one.
    model.lag = 1;
    model.stale.insert(None, model::LONG.to_owned());
    let flat = |reply: HttpReply| String::from_utf8(reply.body).unwrap();
    assert_eq!(flat(model.rest(&get("/user/positions", None))), model::LONG);
    assert_eq!(flat(model.rest(&get("/user/positions", None))), model::FLAT);
    assert_eq!(
        flat(model.rest(&get("/user/positions", Some(SUB)))),
        model::FLAT
    );
    let logged: Vec<&str> = model.log.iter().map(|e| e.what.as_str()).collect();
    assert_eq!(logged[4], format!("GET /user/positions {SUB}"));
    // A trade socket answers nothing but its reads: an order, a binary frame and text that is
    // not JSON are refused.
    let order = Frame::text(r#"{"type":"add_order","params":{}}"#);
    let refused = model.trade(0, &order).unwrap_err();
    assert_eq!(refused, "the model answers no add_order yet (FBC-23ch)");
    assert!(model.trade(0, &Frame::Binary(vec![1])).is_err());
    assert!(model.mds(0, &Frame::text("not json")).is_err());
    let mut printed = Printed::default();
    printed.write_all(b"CAPS ").unwrap();
    printed.flush().unwrap();
    assert_eq!(printed.text(), "CAPS ");
}
