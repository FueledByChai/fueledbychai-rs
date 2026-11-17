//! FBC-c6kw's done line: the place stages' preflight (`--place-stages`, the first slice of
//! FBC-9sk2; decision 0148), run against the QFEX model ([`super::model`]) on 127.0.0.1 under
//! `production_probe` with loopback origins, as the production tests of stage one are. Declared
//! by path in `tests/qfex_preprod_probe.rs`, so this proof is part of the test binary decision
//! 0148's Consequences name (0151 item 12) and Cargo builds no target of its own for it. No
//! request reaches a venue, and every symbol, cap, price, key and id is made up. Past the
//! preflight, each named account's drop test places X and Y on the model alone (FBC-23ch and
//! FBC-77r8, below).
//!
//! - every place-stage flag is refused naming it and never its value, `--place-stages` without
//!   `--production` naming the reason, a flag given twice, more than 10 reads per comparison,
//!   the testnet samples' lease directory (through a symbolic link too, and any path with a
//!   `..`) and a span over an hour included; a place-stage run needs no `--arm`, takes each
//!   ledger flag as required, and holds its span bound above the drop test's floor;
//! - the `CAPS` line comes first, before any request and before the hold, both caps reach
//!   standard output and neither the report nor the evidence, a run that holds holds before the
//!   preflight with nothing else open, and the owner's Ctrl-C on the hold, during its reads
//!   before it (its connection never opening) or during the reads after it (which end first)
//!   stops the run before its preflight, with no stop marker;
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
//!
//! FBC-23ch's done line (`places.rs`, `tee.rs`, `compare.rs` and the driver in `stage.rs`;
//! decisions 0132 items 4 and 7, 0148 and 0151), against the model's trade sockets:
//!
//! - the run's one mint under one namespace lease in its lease directory, its high-water mark
//!   written before an id is returned and kept for a second run, a held lease refusing a second
//!   run, and a lease directory tagged production on its first use, one tagged for another
//!   environment refused (0151 item 1);
//! - Start and Flatten under the market lease, with no account lease;
//! - drop session one's steps and reads in order on the model's log: clean reads of every named
//!   account, the drop registry built, its trip given to the journal and its market seeded flat,
//!   one session armed under an authority for its own account, Start, X at the instrument's
//!   minimum size as an fbc-oms authorization (the one place before an echo, within its
//!   registry's list), X's ACK, the streams compared with never-arming reads (every frame
//!   charged first, at the read interval, at most ten, the first agreeing read timed), a graceful
//!   close (whether it cancelled X recorded), the reads after it, X and its trades; no second
//!   session, no reconnect, and X still resting reported for the owner's hand cancel and marked;
//! - every stop, each with its after-session read, report and marker: an order event or a fill
//!   without our client id, a stray order_response (0151 item 13), a replayed fill refused for
//!   its time, a resync that disagrees or leaves a fill unsettled, a position not flat, a provider
//!   rate refusal on the session, a read socket or REST, a read showing an order or a position
//!   the streams did not, a server close, the span bound, Ctrl-C (in the preflight, in the
//!   session, and twice during the stop's cancels) and a maker fill of X; only Ctrl-C and a fill
//!   of ours cancel in a drop session, by venue id, and held cancels end by a graceful close
//!   (0151 items 2 and 11);
//! - a resync page listing X paired and counted on the traded spec, and no synthetic value in
//!   the report, the evidence, the drop-production/ slice or standard output but the two caps on
//!   its CAPS line;
//! - Ctrl-C during the preflight or the reads, outside the hold, stopping the run with its
//!   report and its stop marker, which keeps the stops of a marker it replaces, and the marker
//!   held to `--reconciled` by the next run, which names every stop in it, cleared only after
//!   clean reads of every named account, the declaration reported unchecked (0151 item 6).
//!
//! FBC-77r8's done line (decisions 0132 items 4 and 7, 0144 item 4, 0148 and 0151): each named
//! account's whole drop test in turn, against the model's trade sockets:
//!
//! - the read before drop session two admitting X alone, one open order on its symbol
//!   (`the_read_before_session_two_admits_x_alone`), and any other open order, a provider rate
//!   refusal, an account it cannot read or the owner's Ctrl-C stopping the run there, X resting
//!   and marked (`any_other_open_order_before_session_two_stops_the_run` and the three after it);
//! - the drop test's steps in order on the model's log, with no cancel, no reconnect and no
//!   later arm from the first arm to the last read
//!   (`the_drop_tests_steps_run_in_order_with_no_cancel_or_reconnect`), and its two sessions on
//!   one registry seeded once (`the_drop_tests_sessions_share_one_registry_and_seed_once`);
//! - each verdict: the drop cancelling X and Y and the test passing, on both accounts; a
//!   graceful close that cancelled X, Y still judged; the arm cancelling X, reported, and
//!   silently, a stop after the drop; X absent before the drop, the reach over it not measured;
//!   Y still open after the wait, a trade row naming Y, historic-orders not showing Y ended and
//!   a provider rate refusal after the drop, each failing it;
//! - a session's capture ended at its end when its abrupt drop journals no Closed control
//!   (`an_abrupt_drop_with_no_closed_control_ends_the_capture_at_the_sessions_end`), and every
//!   session's capture whole in a run;
//! - a fill of X in drop session two stopping with cancels, not flat, and a fill of Y seen
//!   after the drop stopping not flat, the position the owner's (0151 item 9); Ctrl-C between
//!   the sessions reporting X resting, and after the drop, each a stop with no cancel; the span
//!   bound in drop session two cancelling nothing and failing the test;
//! - a failed drop test placing nothing more on its account or any other, and a passed verdict
//!   not carried into a second run;
//! - the report's drop items for both accounts, and the report, the evidence and the
//!   drop-production/ slice passing scripts/privacy-check.sh.
//!
//! The verdict at function level, the read before drop session two's judgment and the QFEX
//! model's arm, cancel-on-disconnect and later knobs are proven in `verdict.rs`'s own tests and
//! in `drop_two.rs`.
//!
//! The stops a drop session one cannot meet on QFEX's answers (a replayed fill, which its resync
//! reads none of, and a resync with a fill unsettled across it) are proven on the handler and the
//! report's own path: each raised by `PlaceHandler` from the events that carry it, its marker
//! written and the report naming it.

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
pub(super) type Argv = Vec<(&'static str, Option<String>)>;

/// A place-stage run's command line on the given origins, writing under `dir`: every flag the
/// place stages require, the waits short enough for a test, and caps far above three minimum
/// sizes at the model's touch.
pub(super) fn argv_on(dir: &Path, trade: &str, rest: &str, mds: &str) -> Argv {
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

/// [`argv_on`] the model's origins, a touch priced from for up to a minute: the model sends its
/// touch once, as the MDS connection subscribes, and every named account's drop test prices from
/// it (FBC-77r8).
pub(super) fn argv(dir: &Path, venue: &Venue) -> Argv {
    let (trade, rest) = (venue.trade.ws_url(""), venue.trade.http_url(""));
    let argv = argv_on(dir, &trade, &rest, &venue.mds.ws_url(""));
    set(argv, "--touch-max-age-ms", "60000")
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
pub(super) fn parsed(argv: &Argv) -> Result<(Options, Places), String> {
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
    // One read, then one each second within 10.001 s, none beginning at its end, is 11 reads:
    // more than 10. Within 10 s it is 10, the last beginning at 9 s, since one due at the end
    // itself is not made.
    let err = refusal(&set(parse_only(), "--agree-wait-ms", "10001"));
    assert!(
        err.starts_with("--agree-wait-ms with --read-interval-ms allows more than 10 reads"),
        "{err}"
    );
    assert!(!err.contains("10001"), "{err}");
    let ten = parsed(&set(parse_only(), "--agree-wait-ms", "10000"))
        .unwrap()
        .1;
    assert_eq!(probe_args::reads(ten.agree_wait, ten.read_interval), 10);
    let longer = set(parse_only(), "--read-interval-ms", "2000");
    let longer = set(longer, "--agree-wait-ms", "20000");
    assert!(parsed(&longer).is_ok());
    assert!(refusal(&set(longer, "--agree-wait-ms", "20001")).contains("more than 10 reads"));

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
        "With --place-stages (FBC-c6kw, FBC-23ch and FBC-77r8, slices of FBC-9sk2; decisions \
         0148 and 0151) it runs the place stages' preflight, then decision 0132 item 4's drop \
         test on each named account in turn",
        "only against a loopback test stub, every origin (trade, REST and MDS) a loopback address",
        "It prints a CAPS line with the environment and both caps first, before any request",
        "Drop session two: every named account read again, X alone admitted, as one open order \
         on its symbol",
        "then the abrupt drop of its connection, with no WebSocket close",
        "With no stop the run ends after the last account's drop test, its outcome not ok, until \
         FBC-su9i builds stage two.",
        "between the drop test's sessions it stops the run once the reads under way end, X still \
         resting reported for your hand cancel",
        "In a drop session only Ctrl-C and a fill of ours cancel (0151 item 2)",
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
        err.contains("No pre-production place path is built (0151 item 1)"),
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
    // namespace: the report and the evidence state fences met, never a value (0151 item 10). The
    // journal, which stays local (decision 0006), keeps X's add_order as written (FBC-23ch).
    let written = files(&dir.join("evidence"));
    assert!(written.iter().any(|(p, _)| p.ends_with("report.md")));
    assert!(written.iter().any(|(p, _)| p.ends_with("evidence.json")));
    let journal = dir.join("evidence").join("journal");
    for (path, text) in written
        .into_iter()
        .filter(|(p, _)| !p.starts_with(&journal))
    {
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
    // Every read around the hold and the hold come before /refdata, and the MDS connection after
    // it; drop session one's reads follow the preflight (FBC-23ch).
    let refdata = what.iter().position(|w| *w == "GET /refdata").unwrap();
    let on_hold = format!("trade {conn} ");
    let held_last = what.iter().rposition(|w| w.starts_with(&on_hold));
    assert!(held_last < Some(refdata), "{what:?}");
    let read_after = what
        .iter()
        .take(refdata)
        .rposition(|w| w.starts_with("trade ") || w.contains("positions"));
    assert!(read_after > held_last, "{what:?}");
    let first_mds = what.iter().position(|w| w.starts_with("mds ")).unwrap();
    assert!(refdata < first_mds, "{what:?}");
    let between = &what[refdata..first_mds];
    assert!(between.iter().all(|w| !w.starts_with("trade ")), "{what:?}");
    // Every trade socket, the hold's included, was closed by the probe, but each drop session
    // two's, dropped abruptly with no close (decision 0132 item 4, FBC-77r8).
    let ended = || venue.trade.connections().iter().all(|c| !c.open);
    until("every trade socket's end", ended).await;
    let unclosed = venue
        .trade
        .connections()
        .iter()
        .filter(|c| !c.client_closed)
        .count();
    assert_eq!(unclosed, 2);
    let reads: Vec<&str> = ev.reads.iter().map(|r| r.before).collect();
    assert_eq!(
        reads[..4],
        ["the hold", "the hold", "after the hold", "after the hold"]
    );
    let (report, _) = outputs_of(&dir);
    assert!(
        report.starts_with(
            "# QFEX production place stages: the preflight and each named account's drop test\n"
        ),
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
    let presses = interrupt::Presses::none();
    let ran = stage::run_places_until(&opts, &places, &lookup, out, interrupt, presses);
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

/// Runs a place-stage run of `opts` and `places` against `venue`, its presses wired as
/// `main.rs` wires them ([`interrupt::wire`] handing each press the hold does not take to the
/// run's [`interrupt::Presses::channel`]), the owner pressing Ctrl-C once `when` holds of the
/// model's log.
async fn wired_run(
    venue: &Venue,
    (mut opts, places): (Options, Places),
    when: impl Fn(&[model::Event]) -> bool,
) -> (Result<stage::Outcome, String>, String) {
    opts.rate_user = user();
    let env = environment();
    let printed = venue.printed();
    let lookup = |var: &str| env.get(var).cloned();
    let out: stage::Out = Rc::new(RefCell::new(printed.clone()));
    let (outside, presses) = interrupt::Presses::channel();
    let (press, _taken, stream) = super::presses();
    let (task, hold) = interrupt::wire(stream, outside);
    tokio::spawn(task);
    let drive = async {
        until("the moment to press", || when(&venue.log())).await;
        press.send(()).unwrap();
    };
    let ran = stage::run_places_until(&opts, &places, &lookup, out, hold, presses);
    let (ran, ()) = tokio::join!(ran, drive);
    (ran, printed.text())
}

/// Asserts that the hold's interrupt in a place-stage run started no preflight and wrote no
/// stop marker: it is the hold's, not a stop of the place stages.
fn assert_no_preflight_and_no_marker(dir: &Path, venue: &Venue, ev: &Evidence) {
    assert_eq!(ev.preflight, None);
    assert!(venue.log().iter().all(|e| e.what != "GET /refdata"));
    assert!(venue.mds.connections().is_empty());
    assert_eq!(ev.stop, None);
    assert!(!leases_of(dir).join(places::MARKER).exists());
    assert_eq!(ev.marker.as_ref().unwrap().written, None);
}

#[tokio::test]
async fn ctrl_c_during_the_holds_reads_keeps_its_connection_from_opening() {
    let (dir, _removed) = temp_dir("place-hold-reads-ctrl-c");
    // The read socket's authentication is never answered, so the hold's reads wait on it.
    let model = QfexModel {
        reads_silent: true,
        ..QfexModel::default()
    };
    let venue = Venue::start(model).await;
    let argv = set(waiting(&dir, &venue), "--hold-secs", "3600");
    let pressed = |log: &[model::Event]| logged(log, "trade 0 auth");
    let (ran, printed) = wired_run(&venue, parsed(&argv).unwrap(), pressed).await;
    let ev = stopped(ran, &printed);
    assert_eq!(
        ev.stopped.as_deref(),
        Some(stage::INTERRUPTED_BEFORE_HOLD),
        "{printed}"
    );
    // The hold's connection never opened: the one trade socket is the read's, which never
    // subscribed, and no HOLD line was printed.
    assert_eq!(venue.trade.connections().len(), 1);
    assert!(venue.log().iter().all(|e| !e.what.ends_with(" subscribe")));
    assert!(!printed.contains("HOLD primary"), "{printed}");
    assert!(ev.hold.is_none());
    assert_no_preflight_and_no_marker(&dir, &venue, &ev);
}

#[tokio::test]
async fn ctrl_c_during_the_reads_after_the_hold_starts_no_preflight() {
    let (dir, _removed) = temp_dir("place-hold-after-ctrl-c");
    let venue = Venue::start(QfexModel::default()).await;
    let argv = set(argv(&dir, &venue), "--hold-secs", "1");
    // Trade sockets 0 and 1 are the reads before the hold and 2 is the hold's: the press comes
    // as the first read after it authenticates, once the hold's connection no longer listens.
    let pressed = |log: &[model::Event]| logged(log, "trade 3 auth");
    let (ran, printed) = wired_run(&venue, parsed(&argv).unwrap(), pressed).await;
    let ev = stopped(ran, &printed);
    let held = ev.hold.as_ref().unwrap();
    assert!(held.ok(), "{held:#?}");
    assert_eq!(held.ended_by, hold::End::Duration);
    assert!(!held.interrupted_closing);
    // The reads after the hold ran to their end, clean, and then the run stopped.
    assert_eq!(held.read_after, hold::ReadAfter::Clean);
    let reads: Vec<&str> = ev.reads.iter().map(|r| r.before).collect();
    assert_eq!(
        reads,
        ["the hold", "the hold", "after the hold", "after the hold"]
    );
    assert_eq!(
        ev.stopped.as_deref(),
        Some(stage::INTERRUPTED_AFTER_HOLD),
        "{printed}"
    );
    assert_no_preflight_and_no_marker(&dir, &venue, &ev);
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
    // Past the preflight, each named account's drop test ran, two sessions each (FBC-23ch and
    // FBC-77r8), and no stage-one registry was built.
    assert_eq!(ev.sessions.len(), 4);
    assert!(ev.registries.is_empty());
    assert_eq!(
        (ev.environment, ev.target),
        ("production", "a loopback test stub")
    );
    // Discovery on the guarded REST origin, the touch on the guarded MDS origin, which is the
    // MDS stub's own: one connection, which subscribed to the instrument's bbo alone. The trade
    // origin heard nothing before /refdata was read and the touch arrived.
    assert_eq!(venue.trade.http_requests()[0], "GET /refdata");
    let mds = venue.mds.connections();
    assert_eq!(mds.len(), 1);
    let subscribe = r#"{"type":"subscribe","channels":["bbo"],"symbols":["SYN-USD"]}"#;
    let [Frame::Text(sent)] = mds[0].received.as_slice() else {
        panic!("{:?}", mds[0].received);
    };
    assert_eq!(super::json(sent), super::json(subscribe));
    for line in [
        "PROBE place stages on QFEX production (production_probe), against a loopback test stub: \
         the preflight, then each named account's drop test in turn (FBC-23ch and FBC-77r8)",
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
    assert!(report.contains("stage two and the stages after it are not built yet"));
    let json = super::json(&json);
    assert_eq!(json["place_stages"]["passed"].as_array().unwrap().len(), 4);
    assert_eq!(
        json["asked"],
        super::json(r#"{"arms":[],"hold":false,"place_stages":true}"#)
    );
    // Drop session one is reported, not that the run asked for no session.
    assert!(
        report.contains("### drop session one (primary)"),
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
    // Past the preflight, drop session one runs (FBC-23ch).
    assert_eq!(ev.stopped.as_deref(), Some(stage::NOT_BUILT));

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
    // --flatten-attempts (0151 item 10).
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
    // A trade socket refuses what the model does not answer: a modify, a binary frame and text
    // that is not JSON.
    let modify = Frame::text(r#"{"type":"modify_order","params":{}}"#);
    let refused = model.trade(0, &modify).unwrap_err();
    assert_eq!(refused, "the model answers no modify_order");
    assert!(model.trade(0, &Frame::Binary(vec![1])).is_err());
    assert!(model.mds(0, &Frame::text("not json")).is_err());
    let mut printed = Printed::default();
    printed.write_all(b"CAPS ").unwrap();
    printed.flush().unwrap();
    assert_eq!(printed.text(), "CAPS ");
}

// ----- FBC-23ch: the leases, the mint, the stops, the allowance and the stop marker -----

/// The lease directory a run parsed from [`argv`] under `dir` holds its leases in.
fn leases_of(dir: &Path) -> PathBuf {
    dir.join("leases")
}

/// The command line parsed with `$HOME` at `home`, as [`parsed`] parses it.
fn parsed_home(argv: &Argv, home: &Path) -> (Options, Places) {
    let home = Some(home.as_os_str().to_owned());
    match probe_args::parse_with_home(flat(argv), home).unwrap() {
        Parsed::Places(opts, places) => (*opts, *places),
        other => panic!("not a place-stage run: {other:?}"),
    }
}

/// Runs a place-stage run of `opts` and `places` against `venue` with `env`, the owner pressing
/// Ctrl-C once `when` holds of the model's log, having first done `before` (each through
/// [`interrupt::Presses::channel`], as `main.rs` hands the run each press the hold does not take).
async fn pressed_run(
    venue: &Venue,
    (mut opts, places): (Options, Places),
    env: &HashMap<&'static str, String>,
    when: impl Fn(&[model::Event]) -> bool,
    before: impl FnOnce(),
) -> (Result<stage::Outcome, String>, String) {
    opts.rate_user = user();
    let printed = venue.printed();
    let lookup = |var: &str| env.get(var).cloned();
    let out: stage::Out = Rc::new(RefCell::new(printed.clone()));
    let (mut press, presses) = interrupt::Presses::channel();
    let hold: stage::Interrupt = Box::pin(std::future::pending());
    let drive = async {
        until("the moment to press", || when(&venue.log())).await;
        before();
        press();
    };
    let ran = stage::run_places_until(&opts, &places, &lookup, out, hold, presses);
    let (ran, ()) = tokio::join!(ran, drive);
    (ran, printed.text())
}

/// Whether the model's log holds an event `what` names.
fn logged(log: &[model::Event], what: &str) -> bool {
    log.iter().any(|e| e.what == what)
}

/// [`argv`] with each read waiting ten seconds: a test that presses Ctrl-C during a wait for an
/// answer that never comes then has ten seconds to press, however loaded the machine.
fn waiting(dir: &Path, venue: &Venue) -> Argv {
    set(argv(dir, venue), "--read-wait-ms", "10000")
}

/// A model whose MDS answers its subscribe with nothing, so the preflight waits for a touch.
fn silent_mds() -> QfexModel {
    QfexModel {
        mds_silent: true,
        ..QfexModel::default()
    }
}

/// A run under `dir` stopped by the owner's Ctrl-C during its preflight, its marker written.
async fn interrupted_in_the_preflight(dir: &Path) -> Evidence {
    let venue = Venue::start(silent_mds()).await;
    let argv = parsed(&waiting(dir, &venue)).unwrap();
    let pressed = |log: &[model::Event]| logged(log, "mds 0 subscribe");
    let (ran, printed) = pressed_run(&venue, argv, &environment(), pressed, || {}).await;
    stopped(ran, &printed)
}

/// Sets `dir`'s permission bits to `mode`.
fn chmod(dir: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(dir, fs::Permissions::from_mode(mode)).unwrap();
}

#[tokio::test]
async fn ids_are_minted_under_one_lease_and_the_mark_survives_a_second_run() {
    let (dir, _removed) = temp_dir("place-mint");
    let ns = Namespace::new(7);
    let wait = Duration::from_secs(5);
    // The run's one mint, under its namespace lease, the ids in --namespace, each above the
    // last, and the mark on the disk once they are returned.
    let mut mint = places::Mint::open(&dir, ns, WALL, wait).unwrap();
    let ids = mint.mint(2).await.unwrap();
    assert!(ids.iter().all(|cid| cid.namespace() == ns), "{ids:?}");
    assert!(ids[0].seq() < ids[1].seq());
    let mark = testnet::HighWater::at(&dir, ns).read().unwrap();
    assert_eq!(mark, ids[1].seq());
    assert_eq!(mint.high_water(), Some(mark));
    // One lease: a second mint on the namespace there is refused while the first lives, as a
    // second run would be, but another namespace is its own.
    let err = places::Mint::open(&dir, ns, WALL, wait).err().unwrap();
    assert!(
        err.contains("the client-id namespace's lease is held"),
        "{err}"
    );
    assert!(!err.contains(dir.to_str().unwrap()), "{err}");
    assert!(places::Mint::open(&dir, Namespace::new(8), WALL, wait).is_ok());
    drop(mint);
    // The second run, its wall clock stepped back to the epoch: its ids are above every id the
    // first minted, from the mark it kept.
    let taken = "the client-id namespace's lease is held";
    let mut again = once_free(taken, || places::Mint::open(&dir, ns, WallNs(0), wait));
    let next = again.mint(1).await.unwrap();
    assert_eq!(next[0].seq(), mark + 1);
    drop(again);

    // A mark that cannot be kept returns no id, and the mint is kept for the next.
    let mut mint = once_free(taken, || places::Mint::open(&dir, ns, WallNs(0), wait));
    chmod(&dir, 0o555);
    let err = mint.mint(1).await.unwrap_err();
    chmod(&dir, 0o755);
    assert_eq!(
        err,
        "the client-id high-water mark could not be kept (the path is not shown)"
    );
    assert!(mint.mint(1).await.is_ok());
    // A write of the mark that does not end within its wait loses the mint: no id goes out.
    let mut slow = places::Mint::open(&dir, Namespace::new(9), WallNs(0), Duration::ZERO).unwrap();
    let held = testnet::held_writes::hold(&dir);
    let err = slow.mint(1).await.unwrap_err();
    assert!(err.ends_with("its write did not end within 0 ms"), "{err}");
    let err = slow.mint(1).await.unwrap_err();
    assert_eq!(err, "no client-id mint: a write of its mark failed");
    assert_eq!(slow.high_water(), None);
    held.release();
    // A mint whose mark is the last seq mints no more.
    let (full, _removed) = temp_dir("place-mint-full");
    testnet::HighWater::at(&full, ns).write(u64::MAX).unwrap();
    let mut spent = places::Mint::open(&full, ns, WallNs(0), wait).unwrap();
    assert_eq!(spent.mint(1).await.unwrap_err(), "no client id: Exhausted");
    assert_eq!(spent.high_water(), Some(u64::MAX));
    // A mark that is not one, and a directory that is not there, refuse the run.
    let (bad, _removed) = temp_dir("place-mint-bad");
    fs::write(bad.join("cid-high-water-account-1-ns-7"), "x7q").unwrap();
    let err = places::Mint::open(&bad, ns, WallNs(0), wait).err().unwrap();
    assert!(
        err.contains("high-water mark in the lease directory cannot be read"),
        "{err}"
    );
    let err = places::Mint::open(&bad.join("absent"), ns, WallNs(0), wait)
        .err()
        .unwrap();
    assert!(
        err.contains("the client-id namespace's lease cannot be taken"),
        "{err}"
    );

    // A run takes the lease in its lease directory and gives it up as it ends.
    let (run_dir, _removed) = temp_dir("place-mint-run");
    let venue = Venue::start(QfexModel::default()).await;
    let (ran, printed) = place_run(&venue, &argv(&run_dir, &venue), &environment()).await;
    assert_eq!(
        stopped(ran, &printed).stopped.as_deref(),
        Some(stage::NOT_BUILT)
    );
    once_free(taken, || {
        places::Mint::open(&leases_of(&run_dir), ns, WALL, wait)
    });
}

#[tokio::test]
async fn a_held_namespace_lease_refuses_a_second_run() {
    let (dir, _removed) = temp_dir("place-ns-held");
    let leases = leases_of(&dir);
    fs::create_dir_all(&leases).unwrap();
    let held = NamespaceLease::acquire(&leases, places::MINT_ACCOUNT, Namespace::new(7)).unwrap();
    let venue = Venue::start(QfexModel::default()).await;
    let (ran, printed) = place_run(&venue, &argv(&dir, &venue), &environment()).await;
    let err = ran.unwrap_err();
    assert_eq!(
        err,
        "refused, nothing sent: the client-id namespace's lease is held: another run holds \
         --namespace in this lease directory (the values are not shown)"
    );
    // Refused before anything connects: the CAPS line alone, no request, no evidence.
    assert_eq!(printed.lines().count(), 1, "{printed}");
    assert!(venue.log().is_empty() && !dir.join("evidence").exists());
    // Another namespace there is its own lease, and runs.
    let other = set(argv(&dir, &venue), "--namespace", "8");
    let (ran, printed) = place_run(&venue, &other, &environment()).await;
    assert_eq!(
        stopped(ran, &printed).stopped.as_deref(),
        Some(stage::NOT_BUILT)
    );
    drop(held);
    venue.assert_served();
}

/// `open()` once it succeeds, within a minute, retried while it is refused for a lease `held`
/// names: a lease just given up can still be held for a moment by a child process another test
/// of this binary is spawning, which shares the lock until it execs.
fn once_free<T>(held: &str, mut open: impl FnMut() -> Result<T, String>) -> T {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        match open() {
            Ok(opened) => return opened,
            Err(e) if e.contains(held) && std::time::Instant::now() < deadline => {
                std::thread::yield_now();
            }
            Err(e) => panic!("{e}"),
        }
    }
}

#[tokio::test]
async fn a_lease_directory_tagged_for_another_environment_is_refused() {
    // A new lease directory is tagged production on its first use, and is taken again so tagged.
    let (dir, _removed) = temp_dir("place-tag");
    let venue = Venue::start(QfexModel::default()).await;
    {
        let (fresh, _gone) = temp_dir("place-tag-evidence");
        let argv = set(
            argv(&fresh, &venue),
            "--lease-dir",
            leases_of(&dir).to_str().unwrap(),
        );
        let (ran, printed) = place_run(&venue, &argv, &environment()).await;
        assert_eq!(
            stopped(ran, &printed).stopped.as_deref(),
            Some(stage::NOT_BUILT)
        );
        let tag = fs::read_to_string(leases_of(&dir).join(places::ENVIRONMENT)).unwrap();
        assert_eq!(tag, "production\n");
    }
    places::tag(&leases_of(&dir)).unwrap();
    let tag = fs::read_to_string(leases_of(&dir).join(places::ENVIRONMENT)).unwrap();
    assert_eq!(tag, "production\n");
    // One tagged for another environment, or for none this program writes, is refused before
    // anything connects, its tag and its path never shown, and its tag left as it was.
    for tag in ["pre-production", "synthetic-tag-x7q", ""] {
        let venue = Venue::start(QfexModel::default()).await;
        let (dir, _removed) = temp_dir("place-tag-other");
        fs::create_dir_all(leases_of(&dir)).unwrap();
        let file = leases_of(&dir).join(places::ENVIRONMENT);
        fs::write(&file, tag).unwrap();
        let (ran, printed) = place_run(&venue, &argv(&dir, &venue), &environment()).await;
        let err = ran.unwrap_err();
        assert!(
            err.starts_with(
                "refused, nothing sent: --lease-dir: tagged for another environment than \
                 production"
            ),
            "{err}"
        );
        assert!(err.contains("(0151 item 1; the tag is not shown) (the path is not shown)"));
        assert!(tag.is_empty() || !err.contains(tag), "{err}");
        assert!(!err.contains(dir.to_str().unwrap()), "{err}");
        assert_eq!(printed.lines().count(), 1, "{printed}");
        assert!(!dir.join("evidence").exists());
        assert_eq!(fs::read_to_string(&file).unwrap(), tag);
        assert!(venue.log().is_empty());
    }
    // A tag that cannot be read, a directory that cannot be made, and one where no tag can be
    // written, are refused too, naming what failed and never the path.
    let (dir, _removed) = temp_dir("place-tag-unread");
    fs::create_dir_all(dir.join(places::ENVIRONMENT)).unwrap();
    let err = places::tag(&dir).unwrap_err();
    assert!(
        err.starts_with("refused, nothing sent: --lease-dir: its environment tag cannot be read")
    );
    fs::write(dir.join("file"), "").unwrap();
    let err = places::tag(&dir.join("file").join("leases")).unwrap_err();
    assert!(
        err.starts_with("refused, nothing sent: --lease-dir: cannot be created"),
        "{err}"
    );
    let locked = dir.join("locked");
    fs::create_dir_all(&locked).unwrap();
    chmod(&locked, 0o555);
    let err = places::tag(&locked).unwrap_err();
    chmod(&locked, 0o755);
    assert!(
        err.starts_with(
            "refused, nothing sent: --lease-dir: its environment tag cannot be written"
        ),
        "{err}"
    );
    assert!(!err.contains(dir.to_str().unwrap()), "{err}");
    venue.assert_served();
}

#[tokio::test]
async fn ctrl_c_during_the_preflight_stops_with_a_report() {
    let (dir, _removed) = temp_dir("place-ctrl-c");
    let venue = Venue::start(silent_mds()).await;
    let asked = parsed(&waiting(&dir, &venue)).unwrap();
    // Pressed once the MDS connection subscribed: the preflight is waiting for its touch.
    let pressed = |log: &[model::Event]| logged(log, "mds 0 subscribe");
    let (ran, printed) = pressed_run(&venue, asked, &environment(), pressed, || {}).await;
    let ev = stopped(ran, &printed);
    // A stop by name, with what it is: no id and no amount.
    assert_eq!(ev.stop, Some("Interrupted"));
    let why = ev.stopped.clone().unwrap();
    assert_eq!(
        why,
        "Interrupted during the preflight: the owner's interrupt, Ctrl-C (decision 0148 item 8); \
         no further session runs"
    );
    assert_eq!(ev.preflight.as_ref().unwrap().passed, [DISCOVERED, COUNTED]);
    assert!(printed.contains(&format!("STOPPED {why}")), "{printed}");
    assert!(
        printed
            .contains("MARKER written (Interrupted): the next place-stage run needs --reconciled")
    );
    // The marker: the stop's name and the run's start, in the lease directory.
    let marker = fs::read_to_string(leases_of(&dir).join(places::MARKER)).unwrap();
    assert_eq!(
        marker,
        format!("stop: Interrupted\nstarted: {}\n", ev.started)
    );
    // The report and the evidence say so, and pass the privacy check.
    let (report, json) = outputs_of(&dir);
    assert!(
        report.contains("**The place stages' stop:** Interrupted."),
        "{report}"
    );
    assert!(report.contains(
        "## The stop marker (0151 item 6)\n\nNo stop marker from an earlier place-stage run was \
         found. This run stopped (Interrupted) and wrote a stop marker in its lease directory"
    ));
    let json = super::json(&json);
    assert_eq!(json["ticket"], "FBC-9sk2");
    assert_eq!(json["stop"], "Interrupted");
    assert_eq!(json["stop_marker"]["written"], "Interrupted");
    assert_eq!(json["stop_marker"]["found"], super::json("[]"));
    assert_report_and_evidence_pass_the_privacy_check(&dir.join("evidence"));
    assert_no_secret(&dir.join("evidence"));
    // Nothing connected for an account.
    assert_no_account(&venue);
    venue.assert_served();

    // A press made before the preflight starts stops it as it starts.
    let (dir, _removed) = temp_dir("place-ctrl-c-early");
    let venue = Venue::start(QfexModel::default()).await;
    let asked = parsed(&argv(&dir, &venue)).unwrap();
    let (ran, printed) = pressed_run(&venue, asked, &environment(), |_| true, || {}).await;
    let ev = stopped(ran, &printed);
    assert_eq!(ev.stop, Some("Interrupted"));
    assert!(ev.preflight.unwrap().passed.is_empty());
    assert!(venue.log().is_empty() && venue.mds.connections().is_empty());
}

#[tokio::test]
async fn a_marker_that_cannot_be_written_is_an_error_of_the_run() {
    let (dir, _removed) = temp_dir("place-marker-unwritten");
    let venue = Venue::start(silent_mds()).await;
    let argv = parsed(&waiting(&dir, &venue)).unwrap();
    let leases = leases_of(&dir);
    let pressed = |log: &[model::Event]| logged(log, "mds 0 subscribe");
    let lock = || chmod(&leases, 0o555);
    let (ran, printed) = pressed_run(&venue, argv, &environment(), pressed, lock).await;
    chmod(&leases, 0o755);
    let ev = stopped(ran, &printed);
    assert_eq!(ev.stop, Some("Interrupted"));
    assert!(!leases.join(places::MARKER).exists());
    let said = "the stop marker could not be written";
    assert!(
        ev.errors.iter().any(|e| e.starts_with(said)),
        "{:?}",
        ev.errors
    );
    let marker = ev.marker.unwrap();
    assert_eq!(marker.written, None);
    assert!(marker.error.unwrap().contains("the path is not shown"));
    assert!(!printed.contains("MARKER written"), "{printed}");
    let (report, _) = outputs_of(&dir);
    assert!(report.contains("This run wrote no stop marker. Error: the stop marker could not"));
}

#[tokio::test]
async fn a_stopped_run_leaves_a_marker_the_next_place_stage_run_refuses_without_reconciled() {
    let (dir, _removed) = temp_dir("place-marker-left");
    let ev = interrupted_in_the_preflight(&dir).await;
    assert_eq!(ev.marker.unwrap().written, Some("Interrupted"));
    // The next run, on the same lease directory and without --reconciled, is refused before
    // anything connects, naming the earlier stop and what to do.
    let venue = Venue::start(QfexModel::default()).await;
    let (next, _removed) = temp_dir("place-marker-next");
    let asked = set(
        argv(&next, &venue),
        "--lease-dir",
        leases_of(&dir).to_str().unwrap(),
    );
    let (ran, printed) = place_run(&venue, &asked, &environment()).await;
    let err = ran.unwrap_err();
    assert!(
        err.starts_with(
            "refused, nothing sent: a stop marker from an earlier place-stage run (its stop: \
             Interrupted) is in the lease directory or production's default"
        ),
        "{err}"
    );
    assert!(
        err.contains("give --reconciled <hand-cancel|hand-close|both|none>"),
        "{err}"
    );
    assert_eq!(printed.lines().count(), 1, "{printed}");
    assert!(venue.log().is_empty() && !next.join("evidence").exists());
    assert!(leases_of(&dir).join(places::MARKER).exists());

    // A marker in production's default lease directory holds a run that names another, and one
    // whose stop this program does not write is named not readable.
    let (home, _removed) = temp_dir("place-marker-home");
    let default = home.join(".fueledbychai").join(probe_args::LEASE_DIR_NAME);
    fs::create_dir_all(&default).unwrap();
    fs::write(default.join(places::MARKER), "stop: synthetic-x7q\n").unwrap();
    let (elsewhere, _removed) = temp_dir("place-marker-elsewhere");
    let (opts, places) = parsed_home(&argv(&elsewhere, &venue), &home);
    assert_eq!(places.default_lease_dir.as_deref(), Some(default.as_path()));
    let printed = Printed::default();
    let env = environment();
    let lookup = |var: &str| env.get(var).cloned();
    let out: stage::Out = Rc::new(RefCell::new(printed.clone()));
    let err = stage::run_places(&opts, &places, &lookup, out)
        .await
        .unwrap_err();
    assert!(
        err.contains("(its stop: not readable)") && !err.contains("x7q"),
        "{err}"
    );
    assert!(venue.log().is_empty());
    // Left to its default, the lease directory is production's own, and its marker is one.
    let defaulted = without(argv(&elsewhere, &venue), "--lease-dir");
    let (_, places) = parsed_home(&set(defaulted, "--reconciled", "none"), &home);
    assert_eq!(places.lease_dir, default);
    let opened = places::RunLeases::open(&places, WALL, Duration::from_secs(1)).unwrap();
    assert_eq!(opened.marked, std::slice::from_ref(&default));
    assert_eq!(opened.marker.found, ["not readable"]);
    assert!(opened.reconciling());
}

#[tokio::test]
async fn a_stopped_run_needs_the_reconciled_flag_and_clean_reads_to_clear_its_marker() {
    let (dir, _removed) = temp_dir("place-reconcile");
    let leases = leases_of(&dir);
    places::tag(&leases).unwrap();
    assert!(places::write_marker(&leases, places::Stop::LeftResting, &[], "synthetic").unwrap());
    let fresh = |what: &str| temp_dir(what);
    let with = |evidence: &Path, venue: &Venue, said: &str| {
        let argv = set(
            argv(evidence, venue),
            "--lease-dir",
            leases.to_str().unwrap(),
        );
        set(argv, "--reconciled", said)
    };
    // An account that is not flat: the reads after the preflight are not clean, so the run
    // stops with the marker kept.
    let not_flat = QfexModel {
        positions: HashMap::from([
            (None, model::LONG.to_owned()),
            (Some(SUB.to_owned()), model::FLAT.to_owned()),
        ]),
        ..QfexModel::default()
    };
    let venue = Venue::start(not_flat).await;
    let (first, _removed) = fresh("place-reconcile-dirty");
    let (ran, printed) =
        place_run(&venue, &with(&first, &venue, "hand-cancel"), &environment()).await;
    let ev = stopped(ran, &printed);
    let why = ev.stopped.clone().unwrap();
    assert!(
        why.ends_with("; the stop marker is kept (0151 item 6)"),
        "{why}"
    );
    assert!(why.starts_with("primary (drop) "), "{why}");
    assert_eq!(ev.stop, None);
    assert!(printed.contains(
        "MARKER a stop marker from an earlier place-stage run (its stop: LeftResting): \
         --reconciled hand-cancel, the owner's declaration, which the probe does not check"
    ));
    assert!(leases.join(places::MARKER).exists());
    let marker = ev.marker.clone().unwrap();
    assert_eq!(marker.found, ["LeftResting"]);
    assert_eq!(
        (marker.reconciled, marker.cleared),
        (Some("hand-cancel"), false)
    );
    // The reads came after the preflight, before drop session one.
    assert_eq!(
        ev.preflight.unwrap().passed,
        [DISCOVERED, COUNTED, TOUCHED, CAPPED]
    );
    assert_eq!(ev.reads[0].before, places::DROP_ONE);
    let what: Vec<String> = venue.log().iter().map(|e| e.what.clone()).collect();
    let first_mds = what.iter().position(|w| w.starts_with("mds ")).unwrap();
    let first_read = what
        .iter()
        .position(|w| w.starts_with("GET /user/positions"))
        .unwrap();
    assert!(first_mds < first_read, "{what:?}");
    let (report, _) = outputs_of(&first);
    assert!(
        report.contains("| before drop session one | primary (drop) |"),
        "{report}"
    );
    assert!(
        report.contains("It was kept: not every named account read clean"),
        "{report}"
    );
    assert_no_listing(&venue);

    // Every account flat: the marker is cleared, in the lease directory and in production's
    // default, and the declaration is reported as the owner's, unchecked.
    let (home, _removed) = temp_dir("place-reconcile-home");
    let default = home.join(".fueledbychai").join(probe_args::LEASE_DIR_NAME);
    fs::create_dir_all(&default).unwrap();
    assert!(places::write_marker(&default, places::Stop::NotOurCid, &[], "synthetic").unwrap());
    // Drop session one then runs; its graceful close cancels X here, so the run leaves no
    // order resting and writes no marker of its own (FBC-23ch).
    let venue = Venue::start(QfexModel {
        cod_on_close: true,
        ..QfexModel::default()
    })
    .await;
    let (second, _removed) = fresh("place-reconcile-clean");
    // Each run on a namespace of its own, so none takes a lease the last just gave up.
    let asked = set(with(&second, &venue, "both"), "--namespace", "9");
    let (mut opts, places) = parsed_home(&asked, &home);
    opts.rate_user = user();
    let printed = venue.printed();
    let env = environment();
    let lookup = |var: &str| env.get(var).cloned();
    let out: stage::Out = Rc::new(RefCell::new(printed.clone()));
    let ran = stage::run_places(&opts, &places, &lookup, out).await;
    let ev = stopped(ran, &printed.text());
    assert_eq!(ev.stopped.as_deref(), Some(stage::NOT_BUILT));
    assert!(!leases.join(places::MARKER).exists() && !default.join(places::MARKER).exists());
    assert!(
        printed
            .text()
            .contains("MARKER cleared: every named account read clean")
    );
    let marker = ev.marker.clone().unwrap();
    assert_eq!(marker.found, ["LeftResting", "NotOurCid"]);
    assert!(marker.cleared && marker.written.is_none());
    let reads: Vec<&str> = ev.reads.iter().map(|r| r.before).collect();
    assert_eq!(reads[..2], [places::DROP_ONE, places::DROP_ONE]);
    // The reads that cleared the marker are drop session one's: it reads none of its own, the
    // reads after it next.
    let after = refusals::AFTER_DROP_ONE;
    assert!(reads[2..4].iter().all(|r| *r == after), "{reads:?}");
    let (report, json) = outputs_of(&second);
    assert!(report.contains(
        "The owner declared `--reconciled both`: the owner's own declaration of what was done \
         by hand, which the probe does not check (decision 0148 item 9). It was cleared after \
         every named account read clean."
    ));
    assert!(report.contains(
        "decision 0132 item 4's drop test runs on each named account in turn (FBC-23ch and \
         FBC-77r8)"
    ));
    let json = super::json(&json);
    assert_eq!(json["stop_marker"]["reconciled"], "both");
    assert_eq!(json["stop_marker"]["declaration_checked"], false);
    assert_eq!(json["stop_marker"]["cleared"], true);
    assert_report_and_evidence_pass_the_privacy_check(&second.join("evidence"));
    assert_no_listing(&venue);
    venue.assert_served();

    // Cleared, the marker no longer holds a run: the next takes no --reconciled (on another
    // namespace, whose lease the last run did not hold).
    let (third, _removed) = fresh("place-reconcile-after");
    let argv = set(
        argv(&third, &venue),
        "--lease-dir",
        leases.to_str().unwrap(),
    );
    let argv = set(argv, "--namespace", "8");
    let (ran, printed) = place_run(&venue, &argv, &environment()).await;
    assert_eq!(
        stopped(ran, &printed).stopped.as_deref(),
        Some(stage::NOT_BUILT)
    );
}

#[tokio::test]
async fn ctrl_c_during_the_reads_that_clear_a_marker_stops_the_run_and_marks_it() {
    let (dir, _removed) = temp_dir("place-reconcile-ctrl-c");
    let leases = leases_of(&dir);
    places::tag(&leases).unwrap();
    places::write_marker(&leases, places::Stop::SpanBound, &[], "synthetic").unwrap();
    // The read socket's authentication is never answered, so the read waits on it.
    let model = QfexModel {
        reads_silent: true,
        ..QfexModel::default()
    };
    let venue = Venue::start(model).await;
    let asked = parsed(&set(waiting(&dir, &venue), "--reconciled", "none")).unwrap();
    let pressed = |log: &[model::Event]| logged(log, "trade 0 auth");
    let (ran, printed) = pressed_run(&venue, asked, &environment(), pressed, || {}).await;
    let ev = stopped(ran, &printed);
    assert_eq!(ev.stop, Some("Interrupted"));
    assert!(
        (ev.stopped.as_deref().unwrap())
            .starts_with("Interrupted during the reads that clear the marker"),
        "{printed}"
    );
    // This run's marker replaces the earlier one and keeps its stop, still unreconciled, after
    // its own.
    let marker = fs::read_to_string(leases.join(places::MARKER)).unwrap();
    assert!(
        marker.starts_with("stop: Interrupted\nstop: SpanBound\nstarted: "),
        "{marker}"
    );
    let run = ev.marker.unwrap();
    assert_eq!((run.found, run.cleared), (vec!["SpanBound"], false));
    assert_eq!(run.written, Some("Interrupted"));
    // So the next run, without --reconciled, is refused naming both stops.
    let venue = Venue::start(QfexModel::default()).await;
    let (next, _removed) = temp_dir("place-reconcile-ctrl-c-next");
    let asked = set(argv(&next, &venue), "--lease-dir", leases.to_str().unwrap());
    let (ran, _) = place_run(&venue, &asked, &environment()).await;
    let err = ran.unwrap_err();
    assert!(
        err.starts_with(
            "refused, nothing sent: a stop marker from an earlier place-stage run (its stops: \
             Interrupted; SpanBound) is in the lease directory"
        ),
        "{err}"
    );
    assert!(venue.log().is_empty());
}

#[tokio::test]
async fn a_marker_that_cannot_be_cleared_is_kept_and_named() {
    let (dir, _removed) = temp_dir("place-reconcile-locked");
    let leases = leases_of(&dir);
    places::tag(&leases).unwrap();
    places::write_marker(&leases, places::Stop::NotFlat, &[], "synthetic").unwrap();
    let venue = Venue::start(QfexModel::default()).await;
    let argv = parsed(&set(argv(&dir, &venue), "--reconciled", "hand-close")).unwrap();
    // The lease directory locked once the preflight began, after the run opened it.
    let began = |log: &[model::Event]| logged(log, "GET /refdata");
    let lock = || chmod(&leases, 0o555);
    let (ran, printed) = pressed_run_never(&venue, argv, began, lock).await;
    chmod(&leases, 0o755);
    let ev = stopped(ran, &printed);
    let why = ev.stopped.clone().unwrap();
    assert!(
        why.starts_with("the stop marker could not be cleared:"),
        "{why}"
    );
    assert!(why.ends_with("(the path is not shown)"), "{why}");
    assert!(leases.join(places::MARKER).exists());
    let run = ev.marker.unwrap();
    assert!(!run.cleared && run.error.is_some());
    assert_eq!(ev.stop, None);
}

/// [`pressed_run`] with no press: `before` alone, once `when` holds.
async fn pressed_run_never(
    venue: &Venue,
    (mut opts, places): (Options, Places),
    when: impl Fn(&[model::Event]) -> bool,
    before: impl FnOnce(),
) -> (Result<stage::Outcome, String>, String) {
    opts.rate_user = user();
    let env = environment();
    let printed = venue.printed();
    let lookup = |var: &str| env.get(var).cloned();
    let out: stage::Out = Rc::new(RefCell::new(printed.clone()));
    let drive = async {
        until("the moment", || when(&venue.log())).await;
        before();
    };
    let ran = stage::run_places(&opts, &places, &lookup, out);
    let (ran, ()) = tokio::join!(ran, drive);
    (ran, printed.text())
}

#[tokio::test]
async fn reconciled_is_refused_when_no_marker_exists() {
    let (dir, _removed) = temp_dir("place-reconciled-none");
    for said in ["hand-cancel", "hand-close", "both", "none"] {
        let venue = Venue::start(QfexModel::default()).await;
        let argv = set(argv(&dir, &venue), "--reconciled", said);
        assert_eq!(parsed(&argv).unwrap().1.reconciled.unwrap().name(), said);
        let (ran, printed) = place_run(&venue, &argv, &environment()).await;
        assert_eq!(
            ran.unwrap_err(),
            "refused, nothing sent: --reconciled, but no stop marker is in the lease directory or \
             production's default: give it only after a place-stage run that stopped (0151 item 6)"
        );
        assert_eq!(printed.lines().count(), 1, "{printed}");
        assert!(venue.log().is_empty() && !dir.join("evidence").exists());
    }
    // The flag's value is refused naming the flag and never the value, given twice is refused,
    // and it is a place stage's flag.
    let err = refusal(&set(parse_only(), "--reconciled", "x7q-synthetic"));
    assert_eq!(
        err,
        "--reconciled: hand-cancel, hand-close, both or none (the value is not shown)"
    );
    let mut twice = set(parse_only(), "--reconciled", "both");
    twice.push(("--reconciled", Some("none".to_owned())));
    assert!(refusal(&twice).starts_with("--reconciled is given twice"));
    let stage_one = [
        "--evidence-dir",
        "/abs/probe",
        "--arm",
        "primary",
        "--reconciled",
        "none",
    ];
    let err = probe_args::parse(stage_one.iter().map(|a| (*a).to_owned())).unwrap_err();
    assert_eq!(
        err,
        "--reconciled is a place stage's flag: give --place-stages too"
    );
    assert_eq!(parsed(&parse_only()).unwrap().1.reconciled, None);
    // --help names it.
    let usage = probe_args::usage();
    assert!(usage.contains("--reconciled <WHAT>"));
    let usage = usage.split_whitespace().collect::<Vec<_>>().join(" ");
    assert!(usage.contains(
        "it refuses to start while a stop marker an earlier place-stage run wrote is in it or in \
         production's default lease directory, unless --reconciled is given, and refuses \
         --reconciled when there is none"
    ));
    assert!(usage.contains(
        "In a place-stage run Ctrl-C outside the hold's connection never ends the program"
    ));
}

#[test]
fn the_marker_and_the_reconciled_flag_are_read_as_written() {
    let (dir, _removed) = temp_dir("place-marker-unit");
    assert_eq!(places::marker_in(&dir).unwrap(), None);
    // NoEcho is not a fault and writes none; every other stop writes its name.
    assert!(!places::write_marker(&dir, places::Stop::NoEcho, &[], "synthetic").unwrap());
    assert_eq!(places::marker_in(&dir).unwrap(), None);
    for stop in places::Stop::ALL
        .into_iter()
        .filter(|s| *s != places::Stop::NoEcho)
    {
        assert!(places::write_marker(&dir, stop, &[], "synthetic").unwrap());
        let found = places::marker_in(&dir).unwrap().unwrap();
        assert_eq!(found.stops, [stop.name()]);
        assert_eq!(found.names(), [stop.name()]);
    }
    // A marker written over an earlier one still unreconciled keeps the earlier stops after its
    // own, each once, and NoEcho leaves the earlier marker as it is.
    let earlier = ["LeftResting", "NotFlat"];
    assert!(places::write_marker(&dir, places::Stop::NotFlat, &earlier, "synthetic").unwrap());
    let found = places::marker_in(&dir).unwrap().unwrap();
    assert_eq!(found.stops, ["NotFlat", "LeftResting"]);
    assert!(!places::write_marker(&dir, places::Stop::NoEcho, &["SpanBound"], "x").unwrap());
    assert_eq!(places::marker_in(&dir).unwrap().unwrap(), found);
    // Only the lines naming a stop this program writes are read back, in their order.
    fs::write(
        dir.join(places::MARKER),
        "stop: x7q-synthetic\nstop: SpanBound\nstarted: synthetic\n",
    )
    .unwrap();
    let found = places::marker_in(&dir).unwrap().unwrap();
    assert_eq!(found.names(), ["SpanBound"]);
    // How the stops found are said: one, several, or none readable.
    assert_eq!(places::stops_said(&["NotFlat"]), "its stop: NotFlat");
    assert_eq!(
        places::stops_said(&["NotFlat", "SpanBound"]),
        "its stops: NotFlat; SpanBound"
    );
    // Cleared, it is gone; clearing again, or where there is none, is no error.
    let dirs = [dir.clone(), dir.join("absent")];
    places::clear_markers(&dirs).unwrap();
    places::clear_markers(&dirs).unwrap();
    assert_eq!(places::marker_in(&dir).unwrap(), None);
    // A marker that cannot be read refuses the run, never showing the path.
    fs::create_dir_all(dir.join(places::MARKER)).unwrap();
    let err = places::marker_in(&dir).unwrap_err();
    assert!(
        err.starts_with("refused, nothing sent: a lease directory's stop marker cannot be read")
    );
    assert!(!err.contains(dir.to_str().unwrap()));
    // A directory a marker cannot be written in.
    let absent = dir.join("absent");
    let err = places::write_marker(&absent, places::Stop::NotFlat, &[], "x").unwrap_err();
    assert!(
        err.starts_with("the stop marker could not be written"),
        "{err}"
    );
    // The marker found and --reconciled, together.
    let found = [places::Found { stops: Vec::new() }];
    assert_eq!(found[0].names(), ["not readable"]);
    let none: [places::Found; 0] = [];
    assert!(places::check_reconciled(&none, None).is_ok());
    assert!(places::check_reconciled(&found, Some(probe_args::Reconciled::None)).is_ok());
    let err = places::check_reconciled(&found, None).unwrap_err();
    assert!(err.contains("(its stop: not readable)"), "{err}");
    let err = places::check_reconciled(&none, Some(probe_args::Reconciled::Both)).unwrap_err();
    assert!(err.contains("--reconciled, but no stop marker"), "{err}");
}

#[test]
fn each_stop_is_named_and_said_with_no_id_or_amount() {
    use places::Stop;
    let names: Vec<&str> = Stop::ALL.iter().map(|s| s.name()).collect();
    let mut unique = names.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(unique.len(), names.len(), "{names:?}");
    for stop in Stop::ALL {
        assert_eq!(Stop::named(stop.name()), Some(stop));
        // Each says what it is and the record it comes from.
        let says = stop.says();
        assert!(
            says.contains("decision 01") || says.contains("0151 item"),
            "{says}"
        );
        // Only NoEcho writes no marker, and only Ctrl-C and a fill of ours cancel in a drop
        // session (0151 item 2).
        assert_eq!(stop.marks(), stop != Stop::NoEcho);
        let cancels = matches!(stop, Stop::Interrupted | Stop::UnexpectedFill);
        assert_eq!(stop.cancels_in_drop(false), cancels, "{}", stop.name());
    }
    assert_eq!(Stop::named("synthetic"), None);
    assert_eq!(
        Stop::RateRefused {
            on: places::RefusedOn::Rest
        }
        .name(),
        "RateRefused(Rest)"
    );
}

#[tokio::test]
async fn the_place_stages_presses_are_taken_and_counted() {
    // None: never pressed.
    let mut none = interrupt::Presses::none();
    assert!(none.pressed().now_or_never().is_none());
    assert_eq!(none.taken(), 0);
    // main.rs's wiring: each press the hold does not take goes to the run, which counts each
    // it takes, a second one included, and none is lost while it is not listening.
    let (outside, mut presses) = interrupt::Presses::channel();
    let (press, _taken, stream) = super::presses();
    let (task, hold) = interrupt::wire(stream, outside);
    tokio::spawn(task);
    drop(hold);
    press.send(()).unwrap();
    press.send(()).unwrap();
    presses.pressed().await;
    presses.pressed().await;
    assert_eq!(presses.taken(), 2);
    assert!(presses.pressed().now_or_never().is_none());
    // Once the source of presses has ended, it is never pressed.
    drop(press);
    let (outside, mut ended) = interrupt::Presses::channel();
    drop(outside);
    assert!(ended.pressed().now_or_never().is_none());
    // A press already made is taken without a wait, and counted; none waiting, none is taken.
    assert!(!none.try_pressed() && !ended.try_pressed());
    let (mut outside, mut waiting) = interrupt::Presses::channel();
    assert!(!waiting.try_pressed());
    outside();
    assert!(waiting.try_pressed());
    assert!(!waiting.try_pressed());
    assert_eq!(waiting.taken(), 1);
}

#[test]
fn a_fill_of_ours_cancels_in_a_drop_session_whichever_stop_came_first() {
    use places::Stop;
    // Once a fill of ours reached the drop session, every stop takes the cancelling path (0151
    // item 2): the run's stop is the first raised, so a fill of X that came without our client
    // id, NotOurCid, or after another stop, cancels as UnexpectedFill would.
    for stop in Stop::ALL {
        assert!(stop.cancels_in_drop(true), "{}", stop.name());
    }
    assert!(!Stop::NotOurCid.cancels_in_drop(false));
    assert!(!Stop::ReadDisagrees.cancels_in_drop(false));
    assert!(Stop::UnexpectedFill.cancels_in_drop(false));
}

/// The allowance alone (`places::Allowance`), every registry's list; the run that spends it is
/// `the_probe_authorizes_no_place_beyond_a_registrys_list`.
#[test]
fn the_allowance_refuses_a_place_whose_kind_has_none_left() {
    use ledger::{FLATTEN, PAGED, PlaceLists, Step, TAKER, THROUGH, X, Y};
    let lists = PlaceLists::new(Lots::new(1).unwrap(), 1).unwrap();
    let mut allowance = places::Allowance::new(&lists);
    // The drop registry: X once and Y once, nothing else of the stages'.
    assert_eq!(allowance.left(Step::Drop, X), 1);
    assert_eq!(allowance.take(Step::Drop, X), Ok(()));
    assert_eq!(allowance.take(Step::Drop, X), Err(places::Stop::BeyondList));
    assert_eq!(allowance.take(Step::Drop, Y), Ok(()));
    assert_eq!(allowance.take(Step::Drop, Y), Err(places::Stop::BeyondList));
    for other in [THROUGH, TAKER, PAGED] {
        assert_eq!(allowance.left(Step::Drop, other), 0);
        assert_eq!(
            allowance.take(Step::Drop, other),
            Err(places::Stop::BeyondList)
        );
    }
    // Each later registry its own: one crossing post-only, one taker IOC, three stage-three
    // orders, and no X on any of them.
    assert_eq!(allowance.take(Step::Cross, THROUGH), Ok(()));
    assert_eq!(
        allowance.take(Step::Cross, THROUGH),
        Err(places::Stop::BeyondList)
    );
    assert_eq!(allowance.take(Step::Take, TAKER), Ok(()));
    assert_eq!(
        allowance.take(Step::Take, TAKER),
        Err(places::Stop::BeyondList)
    );
    for _ in 0..3 {
        assert_eq!(allowance.take(Step::Pages, PAGED), Ok(()));
    }
    assert_eq!(
        allowance.take(Step::Pages, PAGED),
        Err(places::Stop::BeyondList)
    );
    assert_eq!(
        allowance.take(Step::Pages, X),
        Err(places::Stop::BeyondList)
    );
    // A refusal spends nothing: the flatten IOC is still there.
    assert_eq!(allowance.left(Step::Pages, FLATTEN), 1);
    // The stop names what it is: a place not built.
    let stop = places::Stop::BeyondList;
    assert_eq!(stop.name(), "BeyondList");
    assert!(stop.says().contains("beyond its registry's list"));
}

#[test]
fn the_allowance_holds_an_accounts_flatten_iocs_to_one_budget_across_its_registries() {
    use ledger::{FLATTEN, PlaceLists, Step};
    for attempts in 1..=3u64 {
        let lists = PlaceLists::new(Lots::new(2).unwrap(), attempts).unwrap();
        // Every registry's list carries the attempts, but the account spends them once.
        for step in Step::ALL {
            let listed = lists.list(step);
            assert_eq!(listed.last().unwrap().what, FLATTEN);
            assert_eq!(listed.last().unwrap().orders, attempts);
        }
        let mut allowance = places::Allowance::new(&lists);
        let mut taken = 0;
        // Asked for in turn by each registry, more than the budget in all.
        for step in Step::ALL.into_iter().cycle().take(8) {
            match allowance.take(step, FLATTEN) {
                Ok(()) => taken += 1,
                Err(stop) => {
                    assert_eq!(stop, places::Stop::BeyondList);
                    assert_eq!(allowance.left(step, FLATTEN), 0, "{step:?}");
                }
            }
        }
        assert_eq!(taken, attempts);
        // Spent in the drop registry alone, the budget leaves none to the others.
        let mut alone = places::Allowance::new(&lists);
        for _ in 0..attempts {
            assert_eq!(alone.take(Step::Drop, FLATTEN), Ok(()));
        }
        for step in Step::ALL {
            assert_eq!(
                alone.take(step, FLATTEN),
                Err(places::Stop::BeyondList),
                "{step:?}"
            );
        }
        // Each account has its own budget.
        let mut other = places::Allowance::new(&lists);
        assert_eq!(other.take(Step::Take, FLATTEN), Ok(()));
    }
}

#[test]
fn start_holds_the_market_lease() {
    let found = spec_of(Listed::trading());
    let (opts, places) = parsed(&parse_only()).unwrap();
    let bounds = market::count_bound(opts.ledger.as_ref().unwrap(), &found, &places).unwrap();
    let caps = market::place_caps(&places, &found, QUOTE).unwrap();
    let order: OrderCaps = probe_caps().exec.unwrap().order;
    // No account lease: QFEX's nonces are not one sequence per account.
    assert_eq!(order.nonce_scope, fbc_core::NonceScope::None);
    let symbol = found.spec.venue_symbol.clone();
    let registry = |keys: LeaseKeys| {
        let built = ledger::PlaceRegistry::new(
            ledger::Step::Drop,
            "primary",
            AccountKey::new(1),
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
        let (mut link, _run) = built.into_link(order.clone(), ladder);
        link.reg()
            .seed_position(market::INST, fbc_core::SignedLots(0))
            .unwrap();
        link
    };
    let keys = || places::lease_keys("primary", &order, market::INST, symbol.clone());
    let (dir, _removed) = temp_dir("place-start-lease");
    // Without the market lease, Start is refused.
    let mut link = registry(keys());
    let refused = link.reg().start(market::INST, Leases::none());
    assert!(refused.is_err());
    // Start under it: the market is armed, and the lease is held while it is, so no other run
    // takes it.
    places::start(link.reg(), &dir, market::INST).unwrap();
    assert!(link.reg().entry(market::INST).armed());
    let other = MarketLease::acquire(&dir, Qfex.id(), "primary", &symbol);
    assert!(other.is_err(), "the lease is held");
    // A second registry on the same names cannot Start, or Flatten, while it is held.
    let mut second = registry(keys());
    let err = places::start(second.reg(), &dir, market::INST).unwrap_err();
    assert_eq!(
        err,
        "the market's lease is held: another run trades this market on this account (the names \
         are not shown)"
    );
    let err = places::flatten(second.reg(), &dir, market::INST).unwrap_err();
    assert!(err.starts_with("the market's lease is held"), "{err}");
    // Once the first is gone, its lease is free: Flatten takes it, into Exit.
    drop(link);
    let held = "the market's lease is held";
    once_free(held, || places::flatten(second.reg(), &dir, market::INST));
    assert!(second.reg().entry(market::INST).armed());
    // A lease directory that is not there, names that name no market, and none at all.
    let mut third = registry(keys());
    let err = places::start(third.reg(), &dir.join("absent"), market::INST).unwrap_err();
    assert_eq!(
        err,
        "the market's lease cannot be taken (the path and the names are not shown)"
    );
    let unnamed = LeaseKeys::new(Qfex.id(), "primary", &order);
    let mut fourth = registry(unnamed);
    let err = places::start(fourth.reg(), &dir, market::INST).unwrap_err();
    assert_eq!(err, "the registry's lease names name no such market");
    let mut bare = fbc_oms::Registry::new();
    let err = places::start(&mut bare, &dir, market::INST).unwrap_err();
    assert_eq!(err, "the registry has no lease names");
    // Start and Flatten the registry refuses with the lease in hand: its kill switch on.
    let (free, _removed) = temp_dir("place-start-killed");
    let mut killed = registry(keys());
    killed.reg().kill(market::INST);
    let err = places::start(killed.reg(), &free, market::INST).unwrap_err();
    assert!(err.starts_with("Start was refused: "), "{err}");
    let err = places::flatten(killed.reg(), &free, market::INST).unwrap_err();
    assert!(err.starts_with("Flatten was refused: "), "{err}");
}

// ----- FBC-23ch: drop session one -----

/// [`argv`] with every wait long enough that a loaded machine never times out a read or a
/// connection the model answers at once.
fn drop_argv(dir: &Path, venue: &Venue) -> Argv {
    let argv = set(argv(dir, venue), "--read-wait-ms", "10000");
    set(argv, "--connect-wait-ms", "10000")
}

/// A place-stage run that reached drop session one, and what it left.
struct Dropped {
    dir: PathBuf,
    _removed: Removed,
    venue: Venue,
    ev: Evidence,
    printed: String,
}

impl Dropped {
    /// Every request and connection the model saw, by what it was, in order.
    fn what(&self) -> Vec<String> {
        self.venue.log().iter().map(|e| e.what.clone()).collect()
    }

    /// The model's events whose name ends `kind`, on trade sockets.
    fn count(&self, kind: &str) -> usize {
        let trade = |w: &String| w.starts_with("trade ") && w.ends_with(&format!(" {kind}"));
        self.what().iter().filter(|w| trade(w)).count()
    }

    /// The run's drop session one.
    fn drop_run(&self) -> &places::DropRun {
        self.ev.drops.first().expect("drop session one ran")
    }

    /// The stop marker the run left in its lease directory.
    fn marker(&self) -> Option<String> {
        fs::read_to_string(leases_of(&self.dir).join(places::MARKER)).ok()
    }

    /// The report and the evidence.
    fn outputs(&self) -> (String, Value) {
        let (report, json) = outputs_of(&self.dir);
        (report, super::json(&json))
    }

    /// Where `line` was printed, panicking when it was not.
    fn at(&self, line: &str) -> usize {
        let printed = &self.printed;
        printed
            .find(line)
            .unwrap_or_else(|| panic!("{line}: {printed}"))
    }
}

/// A place-stage run of `argv` against `model`, under a directory of its own, that reached drop
/// session one.
async fn dropped_with(model: QfexModel, what: &str, argv: impl Fn(Argv) -> Argv) -> Dropped {
    let (dir, removed) = temp_dir(what);
    let venue = Venue::start(model).await;
    let asked = argv(drop_argv(&dir, &venue));
    let (ran, printed) = place_run(&venue, &asked, &environment()).await;
    let ev = stopped(ran, &printed);
    assert!(!ev.drops.is_empty(), "{printed}");
    Dropped {
        dir,
        _removed: removed,
        venue,
        ev,
        printed,
    }
}

/// [`dropped_with`] on the run's own command line.
async fn dropped(model: QfexModel, what: &str) -> Dropped {
    dropped_with(model, what, |argv| argv).await
}

/// Asserts that `d` stopped with `stop`, by name and with what it is, its report and evidence
/// naming it and passing the privacy check, its marker written, and every named account read
/// after drop session one.
fn assert_stopped(d: &Dropped, stop: places::Stop) {
    let name = stop.name();
    assert_eq!(d.ev.stop, Some(name), "{}", d.printed);
    let marker = d.marker().unwrap_or_default();
    assert!(marker.starts_with(&format!("stop: {name}\n")), "{marker}");
    assert!(
        d.printed.contains(&format!(
            "MARKER written ({name}): the next place-stage run"
        )),
        "{}",
        d.printed
    );
    assert_after_reads(d);
    let (report, json) = d.outputs();
    assert!(
        report.contains(&format!("**The place stages' stop:** {name}.")),
        "{report}"
    );
    assert_eq!(json["stop"], name);
    assert_eq!(json["ticket"], "FBC-9sk2");
    assert_report_and_evidence_pass_the_privacy_check(&d.dir.join("evidence"));
    assert_no_secret(&d.dir.join("evidence"));
    d.venue.assert_served();
}

/// Asserts every named account was read after the first named account's drop session one,
/// primary then subaccount.
fn assert_after_reads(d: &Dropped) {
    let after: Vec<&str> = (d.ev.reads.iter())
        .filter(|r| r.before == refusals::AFTER_DROP_ONE)
        .map(|r| r.label.as_str())
        .take(2)
        .collect();
    assert_eq!(after, ["primary (drop)", "subaccount"], "{}", d.printed);
}

/// Asserts the run sent no cancel and ended drop session one by a graceful close, X still
/// resting reported for the owner's hand cancel (0151 item 2).
fn assert_no_cancel_and_x_resting(d: &Dropped) {
    assert_eq!(d.count("cancel_order"), 0, "{:?}", d.what());
    let run = d.drop_run();
    assert_eq!(run.cancels, None);
    assert_eq!(run.ended_by, Some(stage::GRACEFUL));
    assert!(run.x_resting, "{run:?}");
    assert!(
        d.printed
            .contains(&format!("RESTING {}", places::HAND_CANCEL))
    );
    let (report, json) = d.outputs();
    assert!(report.contains(places::HAND_CANCEL), "{report}");
    assert_eq!(
        json["drop_tests"][0]["session_one"]["hand_cancel"],
        places::HAND_CANCEL
    );
}

#[tokio::test]
async fn drop_session_one_steps_and_reads_run_in_order() {
    let d = dropped(QfexModel::default(), "drop-order").await;
    let what = d.what();
    let first = |w: &str| {
        what.iter()
            .position(|e| e == w || e.starts_with(w))
            .unwrap_or_else(|| panic!("{w}: {what:?}"))
    };
    let last = |w: &str| {
        what.iter()
            .rposition(|e| e == w || e.starts_with(w))
            .unwrap_or_else(|| panic!("{w}: {what:?}"))
    };
    // The preflight, the clean reads of both named accounts, then the session's authentication,
    // its subscriptions, its arm and its resync, X, the comparison's socket, the close, the
    // reads after it, and X and its trades.
    let refdata = first("GET /refdata");
    let session = what
        .iter()
        .position(|w| w.ends_with(" subscribe") && w.starts_with("trade "))
        .unwrap();
    let conn = what[session].split(' ').nth(1).unwrap().to_owned();
    let on = |kind: &str| first(&format!("trade {conn} {kind}"));
    let place = on("add_order");
    assert!(refdata < first("GET /user/positions"), "{what:?}");
    assert!(first("GET /user/positions") < on("auth"), "{what:?}");
    assert!(on("auth") < session && session < on("cancel_on_disconnect"));
    assert!(on("cancel_on_disconnect") < place, "{what:?}");
    assert!(on("get_user_orders") < place, "{what:?}");
    let compared = (place + 1..what.len())
        .find(|i| what[*i].ends_with(" get_user_orders"))
        .unwrap();
    let closed = first(&format!("cod {conn} close"));
    let trades = first("trade ")
        + what[first("trade ")..]
            .iter()
            .position(|w| w.ends_with("get_user_trades"))
            .unwrap();
    let historic = what[closed..]
        .iter()
        .position(|w| w.starts_with("GET /user/historic-orders"))
        .map(|i| closed + i)
        .unwrap();
    assert!(place < compared && compared < closed, "{what:?}");
    assert!(closed < trades && trades < historic, "{what:?}");
    assert!(historic < last("GET /user/historic-orders"), "{what:?}");
    // Two sessions on each named account, none of which reconnected, one add_order each and no
    // cancel: X and Y alone (FBC-77r8).
    assert_eq!(d.count("subscribe"), 12);
    assert_eq!((d.count("add_order"), d.count("cancel_order")), (4, 0));
    // The lines, in order.
    let lines = [
        "READ before drop session one: primary (drop)",
        "READ before drop session one: subaccount",
        "REGISTRY drop on primary",
        "SESSION primary (drop session one): it arms",
        "START primary: the market armed under its market lease",
        "PLACE X on primary",
        "COMPARE after X's place",
        "SESSION primary (drop session one): arm outcome Accepted, ended by a graceful close",
        "READ after drop session one: primary (drop)",
        "READ after drop session one: subaccount",
        "READ before drop session two: primary (drop)",
        "SESSION primary (drop session two)",
        "STOPPED stage two",
    ];
    for pair in lines.windows(2) {
        assert!(d.at(pair[0]) < d.at(pair[1]), "{pair:?}: {}", d.printed);
    }
    // X rested after the close, but the run went on to drop session two: no hand cancel, no
    // stop and no marker, the run ending after every account's drop test, naming FBC-su9i.
    assert!(!d.printed.contains("RESTING"), "{}", d.printed);
    assert!(d.marker().is_none() && d.ev.stop.is_none());
    let stopped = d.ev.stopped.clone().unwrap();
    assert_eq!(stopped, stage::NOT_BUILT);
    assert!(stopped.contains("FBC-su9i"), "{stopped}");
    // What the session did, as the evidence keeps it.
    let run = d.drop_run();
    assert!(run.authenticated && run.resynced && run.started);
    assert_eq!(run.arm.as_deref(), Some("Accepted"));
    assert!(
        run.x.submitted && run.x.acked && run.x.exact_echo,
        "{run:?}"
    );
    assert_eq!(run.compared.len(), 1);
    assert_eq!(run.compared[0].ended, compare::Ended::Agreed);
    let after = run.after.as_ref().unwrap();
    assert_eq!((after.trades, after.trades_naming_x), (0, 0));
    assert_eq!(after.terminal_status, None);
    assert_eq!(run.as_encoded, Some(true));
    assert_eq!(d.ev.sessions.len(), 4);
    let session = &d.ev.sessions[0];
    assert_eq!(
        (session.label(), session.account),
        (places::DROP_ONE, "primary")
    );
    assert_eq!((session.attempts, session.failed_attempts), (1, 0));
    assert!(
        session.arm_sent && session.partial.is_empty(),
        "{session:?}"
    );
    d.venue.assert_served();
}

#[tokio::test]
async fn every_place_is_the_minimum_size() {
    let d = dropped(QfexModel::default(), "drop-min-size").await;
    let model = d.venue.model.lock().unwrap();
    // X and Y on each named account (FBC-77r8), each at the instrument's minimum size (one lot of
    // 0.01), a post-only GTC on --side, --away-bps (50) behind the bid of 100.00, snapped onto
    // the cent grid.
    assert_eq!(model.orders.len(), 4);
    for x in &model.orders {
        assert_eq!(x.quantity, Decimal::new(1, 2));
        assert_eq!((x.kind.as_str(), x.tif.as_str()), ("ALO", "GTC"));
        assert_eq!(x.side, "BUY");
        assert_eq!(x.price, super::json("99.5"));
    }
    // The registry's list held it, and the codec's place-stage key admits it only at the
    // minimum (decision 0148 item 4).
    let found = spec_of(Listed::trading());
    assert_eq!(found.min_size, Lots::new(1).unwrap());
}

/// The outbound frames the session wrote of each `kind` on its connection, each with whether it
/// was written under a request id, read from the run's journal.
fn written(d: &Dropped, kind: &str) -> Vec<bool> {
    let reader = fbc_journal::JournalReader::open(d.dir.join("evidence").join("journal"), 0);
    let mut found = Vec::new();
    for entry in reader.unwrap().entries() {
        if let fbc_journal::Record::Outbound { rpc, frame, .. } = entry.unwrap().record {
            let text = String::from_utf8_lossy(frame.bytes()).into_owned();
            if text.contains(&format!(r#""type":"{kind}""#)) {
                found.push(rpc.is_some());
            }
        }
    }
    found
}

#[tokio::test]
async fn every_place_and_cancel_is_an_authorization() {
    // The owner's Ctrl-C once X rests: X placed, then cancelled by venue id.
    let (dir, removed) = temp_dir("drop-authorized");
    let venue = Venue::start(QfexModel::default()).await;
    let asked = parsed(&drop_argv(&dir, &venue)).unwrap();
    let pressed = |log: &[model::Event]| log.iter().any(|e| e.what.ends_with(" add_order"));
    let (ran, printed) = pressed_run(&venue, asked, &environment(), pressed, || {}).await;
    let ev = stopped(ran, &printed);
    let d = Dropped {
        dir,
        _removed: removed,
        venue,
        ev,
        printed,
    };
    // Every place and cancel the session wrote is a command submitted under a request id: only
    // an fbc-oms Authorization reaches an ExecSession's orders (Link::submit), and the session
    // takes nothing else from the program but its own arm, which names no order.
    assert_eq!(written(&d, "add_order"), [true]);
    assert_eq!(written(&d, "cancel_order"), [true]);
    let run = d.drop_run();
    assert!(run.x.submitted);
    assert_eq!(run.cancels.as_ref().map(|c| c.sent), Some(1));
    // The model saw them from the session's connection alone.
    assert_eq!((d.count("add_order"), d.count("cancel_order")), (1, 1));
}

#[tokio::test]
async fn each_session_arms_under_an_authority_for_its_own_account() {
    let d = dropped(QfexModel::default(), "drop-authority").await;
    // The session armed: an ExecSession refuses an account-wide cancel-on-disconnect arm whose
    // authority names another account than its configuration's (decisions 0121 and 0122), so an
    // arm accepted is one armed under the authority for its own account.
    assert_eq!(d.drop_run().arm.as_deref(), Some("Accepted"));
    let model = d.venue.model.lock().unwrap();
    let mut armed: Vec<(&usize, &model::Conn)> =
        model.conns.iter().filter(|(_, c)| c.armed).collect();
    armed.sort_by_key(|(n, _)| **n);
    // Each drop test's two sessions armed on its own account, as their authentication named it:
    // the primary account's, then the subaccount's (FBC-77r8).
    let accounts: Vec<Option<&str>> = armed.iter().map(|(_, c)| c.account.as_deref()).collect();
    assert_eq!(accounts, [None, None, Some(SUB), Some(SUB)]);
    assert!(armed.iter().all(|(_, c)| c.session));
    let arms =
        d.ev.drops
            .iter()
            .flat_map(|r| [r.arm.clone(), r.two.clone().unwrap().arm]);
    assert!(
        arms.into_iter()
            .all(|arm| arm.as_deref() == Some("Accepted"))
    );
    drop(model);
    // Its configuration's account key is the primary account's, as the place config sets it.
    let env = environment();
    let lookup = |var: &str| env.get(var).cloned();
    let (opts, _) = parsed(&drop_argv(&d.dir, &d.venue)).unwrap();
    let base = stage::base_config(&opts);
    let (cfg, _) = stage::place_config(&base, &lookup, Armed::Primary).unwrap();
    assert_eq!(cfg.get(ACCOUNT_KEY_CFG), Some("1"));
    let (cfg, _) = stage::place_config(&base, &lookup, Armed::Subaccount).unwrap();
    assert_eq!(cfg.get(ACCOUNT_KEY_CFG), Some("2"));
}

#[tokio::test]
async fn a_registrys_trip_reaches_the_journal_before_any_market_arms() {
    let d = dropped(QfexModel::default(), "drop-trip").await;
    // The drop registry's CancelOnlyTrip is given to the journal (decision 0139) as the registry
    // is built, before its market is Started and before X; `place_registries` proves the trip
    // given that way moves a Quoting market to Cancel-only at the soft limit.
    assert!(d.at("REGISTRY drop on primary") < d.at("START primary"));
    assert!(d.at("START primary") < d.at("PLACE X"));
    assert!(d.printed.contains(
        "its CancelOnlyTrip given to the journal before any market arms (decision 0139)"
    ));
    // One drop registry per named account (FBC-77r8).
    let registries = &d.ev.place_registries;
    assert_eq!(registries.len(), 2);
    for registry in registries {
        assert_eq!(registry.step, ledger::Step::Drop);
        assert_eq!(registry.declaration, ledger::PLACES_DECLARATION);
    }
}

#[tokio::test]
async fn the_seed_follows_clean_reads_of_every_named_account() {
    let d = dropped(QfexModel::default(), "drop-seed").await;
    assert!(d.at("READ before drop session one: primary (drop)") < d.at("REGISTRY drop"));
    assert!(d.at("READ before drop session one: subaccount") < d.at("REGISTRY drop"));
    assert!(
        d.printed
            .contains("then seeded flat after clean reads of every named account")
    );
    let before: Vec<&str> = (d.ev.reads.iter())
        .filter(|r| r.before == places::DROP_ONE)
        .map(|r| r.label.as_str())
        .collect();
    assert_eq!(
        before,
        [
            "primary (drop)",
            "subaccount",
            "primary",
            "subaccount (drop)"
        ]
    );
    // Each named account's registry is seeded once, after clean reads of every named account
    // that follow the drop test before it (FBC-77r8).
    let sub = d.at("READ before drop session one: subaccount (drop)");
    assert!(d.at("VERDICT drop test on primary") < sub);
    assert!(sub < d.at("REGISTRY drop on subaccount"));
    assert_eq!(d.printed.matches("seeded flat").count(), 2);
    // An account that does not read clean stops the run before any registry is built, so
    // nothing is seeded and no session opens.
    let not_flat = QfexModel {
        positions: HashMap::from([
            (None, model::FLAT.to_owned()),
            (Some(SUB.to_owned()), model::LONG.to_owned()),
        ]),
        ..QfexModel::default()
    };
    let (dir, _removed) = temp_dir("drop-seed-dirty");
    let venue = Venue::start(not_flat).await;
    let (ran, printed) = place_run(&venue, &drop_argv(&dir, &venue), &environment()).await;
    let ev = stopped(ran, &printed);
    assert!(
        ev.stopped
            .unwrap()
            .starts_with("subaccount holds an open order")
    );
    assert!(ev.drops.is_empty() && ev.place_registries.is_empty());
    assert!(!printed.contains("REGISTRY") && !printed.contains("SESSION"));
    let trade = |w: &str| w.starts_with("trade ") && w.ends_with(" subscribe");
    assert!(venue.log().iter().all(|e| !trade(&e.what)));
}

#[tokio::test]
async fn no_two_sessions_are_ever_open() {
    let d = dropped(QfexModel::default(), "drop-alone").await;
    // One session's connection subscribed, and none while another's was open: the comparison's
    // and the reads' sockets never arm or subscribe.
    let model = d.venue.model.lock().unwrap();
    assert!(!model.overlapped);
    // Two sessions on each named account (FBC-77r8), one after the other.
    let sessions = model.conns.values().filter(|c| c.session).count();
    assert_eq!(sessions, 4);
    assert_eq!(model.conns.values().filter(|c| c.armed).count(), 4);
}

#[tokio::test]
async fn a_place_stage_session_never_reconnects() {
    // The server closes the session's connection as it reads X: no second attempt.
    let model = QfexModel {
        close_on_place: true,
        ..QfexModel::default()
    };
    let d = dropped_with(model, "drop-no-reconnect", patient_rpc).await;
    // This proves the outcome: the handler's end of the session through its control ends it
    // whatever the pacing allows. The pacing itself allows one attempt a day.
    let pacing = stage::place_pacing(Duration::from_secs(1));
    assert_eq!((pacing.budget(), pacing.window()), (1, stage::DAY));
    let session = &d.ev.sessions[0];
    assert_eq!(session.attempts, 1, "{session:?}");
    assert_eq!(d.count("subscribe"), 3);
    assert_eq!(d.count("cancel_on_disconnect"), 1);
    assert_eq!(d.drop_run().ended_by, Some(stage::CONNECTION_ENDED));
}

#[tokio::test]
async fn before_the_echo_each_session_places_at_most_one_order() {
    // X's ACK never comes before its deadline: X ends Unknown, and nothing else is placed in
    // its session; drop session two then places Y alone (FBC-77r8).
    let model = QfexModel {
        ack_delay: Some(Duration::from_secs(3600)),
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-one-place").await;
    assert_eq!(d.places_by_session(), [1, 1, 1, 1]);
    let run = d.drop_run();
    assert!(!run.x.acked && !run.x.exact_echo, "{run:?}");
    assert_eq!(run.x.outcome.as_deref(), Some("Unknown"));
    // And with the echo: still one a session, X or Y (FBC-77r8).
    let d = dropped(QfexModel::default(), "drop-one-place-echoed").await;
    assert_eq!(d.places_by_session(), [1, 1, 1, 1]);
    assert!(d.drop_run().x.exact_echo);
}

#[tokio::test]
async fn the_probe_authorizes_no_place_beyond_a_registrys_list() {
    use ledger::{PlaceLists, Step, X};
    // The drop registry's list holds X once (and Y once, FBC-77r8's): a place whose kind has
    // none left is refused before it is built, BeyondList, which stops the run.
    let lists = PlaceLists::new(Lots::new(1).unwrap(), 1).unwrap();
    let mut allowance = places::Allowance::new(&lists);
    assert_eq!(allowance.take(Step::Drop, X), Ok(()));
    let refused = allowance.take(Step::Drop, X).unwrap_err();
    assert_eq!(refused, places::Stop::BeyondList);
    assert!(refused.marks() && !refused.cancels_in_drop(false));
    // A run spends its list: X and Y alone reached QFEX, once each on each named account.
    let d = dropped(QfexModel::default(), "drop-list").await;
    assert_eq!(d.count("add_order"), 4);
    assert_eq!(d.ev.place_registries[0].step, Step::Drop);
    // Its list holds X and Y once: a third resting place is beyond it.
    assert_eq!(allowance.take(Step::Drop, ledger::Y), Ok(()));
    assert_eq!(
        allowance.take(Step::Drop, ledger::Y),
        Err(places::Stop::BeyondList)
    );
    // A run stopped BeyondList reports it by name and marks it, as every stop.
    let (dir, _removed) = temp_dir("drop-list-marker");
    places::tag(&dir).unwrap();
    assert!(places::write_marker(&dir, refused, &[], "synthetic").unwrap());
    let found = places::marker_in(&dir).unwrap().unwrap();
    assert_eq!(found.stops, ["BeyondList"]);
    let ev = Evidence {
        places_asked: true,
        stop: Some(refused.name()),
        ..Evidence::default()
    };
    let report = report::markdown(&ev);
    assert!(
        report.contains("**The place stages' stop:** BeyondList."),
        "{report}"
    );
}

#[tokio::test]
async fn a_graceful_close_that_cancels_x_is_recorded() {
    let model = QfexModel {
        cod_on_close: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-close-cancels").await;
    let run = d.drop_run();
    assert_eq!(run.ended_by, Some(stage::GRACEFUL));
    assert_eq!(run.close_cancelled_x, Some(true));
    assert!(!run.x_resting);
    // X is ended on QFEX's historic orders, CANCELLED, and named by no trade.
    let after = run.after.as_ref().unwrap();
    assert_eq!(after.terminal_status.as_deref(), Some("CANCELLED"));
    assert_eq!(after.trades_naming_x, 0);
    // Nothing rests, so nothing stops the run and no marker is written: it ends naming FBC-77r8.
    assert_eq!(d.ev.stop, None);
    assert_eq!(d.ev.stopped.as_deref(), Some(stage::NOT_BUILT));
    assert!(d.marker().is_none());
    let (report, json) = d.outputs();
    assert!(
        report.contains(
            "Whether the graceful close cancelled X: yes: X rested before it, no cancel of ours \
             was sent, and the read after it no longer listed X"
        ),
        "{report}"
    );
    assert_eq!(
        json["drop_tests"][0]["session_one"]["close_cancelled_x"],
        true
    );
    assert_after_reads(&d);
    // And one that does not: X rests on, which the next test reports for a hand cancel.
    let d = dropped(QfexModel::default(), "drop-close-keeps").await;
    assert_eq!(d.drop_run().close_cancelled_x, Some(false));
}

#[tokio::test]
async fn x_resting_after_session_one_is_reported_for_a_hand_cancel_and_marked() {
    // X rests after drop session one, and the run ends there: the read before drop session two
    // cannot read the first account (its first GET /user/positions fails), so X is left
    // resting, unjudged, a stop with its marker (FBC-77r8: with that read clean, drop session
    // two would follow).
    let model = QfexModel {
        rest_fails: vec![3],
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-x-resting").await;
    assert_stopped(&d, places::Stop::LeftResting);
    assert_no_cancel_and_x_resting(&d);
    let stopped = d.ev.stopped.clone().unwrap();
    assert!(
        stopped.starts_with("primary (drop) could not be read:"),
        "{stopped}"
    );
    assert!(
        stopped.contains("LeftResting in the read before drop session two"),
        "{stopped}"
    );
    // The run ends there, X still on QFEX's book, and no drop session two ran.
    assert!(d.drop_run().two.is_none());
    assert_eq!(d.count("subscribe"), 3);
    let model = d.venue.model.lock().unwrap();
    assert_eq!(model.orders[0].state, model::State::Open);
    drop(model);
    let x_read = (d.ev.reads.iter())
        .find(|r| r.before == refusals::AFTER_DROP_ONE && r.label == "primary (drop)")
        .unwrap();
    assert_eq!((x_read.page_orders, x_read.page_allowed), (1, 1));
}

#[tokio::test]
async fn stop_on_an_order_event_without_our_client_id() {
    // After X's ACK, an order_response naming X with no client id.
    let model = QfexModel {
        cid_absent_update: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-no-cid-order").await;
    assert_stopped(&d, places::Stop::NotOurCid);
    assert_no_cancel_and_x_resting(&d);
    let (report, _) = d.outputs();
    assert!(report.contains("order_response ACK: absent"), "{report}");
}

#[tokio::test]
async fn stop_on_a_fill_without_our_client_id() {
    // After X's ACK, X filled whole, its fill_response naming no client id: a fill of ours, so
    // the drop session cancels (0151 item 2), with nothing left to cancel.
    let model = QfexModel {
        stray_fill: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-no-cid-fill").await;
    assert_stopped(&d, places::Stop::NotOurCid);
    let run = d.drop_run();
    let cancels = run.cancels.as_ref().expect("a fill of ours cancels");
    assert_eq!(cancels.sent, 0);
    assert!(cancels.ended && !cancels.held);
    assert_eq!(run.ended_by, Some(stage::GRACEFUL));
    assert!(!run.x_resting);
    let (report, _) = d.outputs();
    assert!(report.contains("fill_response: absent"), "{report}");
}

#[tokio::test]
async fn stop_on_a_stray_order_response() {
    // After X's ACK, an order_response naming an order no run placed (0151 item 13).
    let model = QfexModel {
        stray_order: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-stray").await;
    assert_stopped(&d, places::Stop::StrayOrder);
    assert_no_cancel_and_x_resting(&d);
}

/// A `Link` under a [`places::PlaceHandler`], as drop session one builds them, its fill ledger
/// keeping `max_entries` fills, its watermark well before every fill time the tests give.
fn handled(max_entries: usize) -> (places::PlaceHandler, Rc<RefCell<link::Link>>) {
    let config = fbc_oms::LedgerConfig {
        max_age: Duration::from_secs(601),
        max_entries,
    };
    let ledger = fbc_oms::FillLedger::new(config, WallNs(1_000)).unwrap();
    let order: OrderCaps = probe_caps().exec.unwrap().order;
    let ladder = LadderConfig::new(
        Duration::from_secs(5),
        Duration::ZERO,
        Duration::from_secs(60),
        2,
    )
    .unwrap();
    let link = link::Link::new(fbc_oms::Registry::new(), ledger, order, ladder);
    let link = Rc::new(RefCell::new(link));
    let handler = places::PlaceHandler {
        inner: link::LinkHandler {
            link: Rc::clone(&link),
            wake: Rc::new(tokio::sync::Notify::new()),
        },
        watched: Rc::default(),
        control: Rc::default(),
    };
    (handler, link)
}

/// `fill` as a session hands it, the `n`th input, timed by the matching engine at `exch` when
/// given.
fn fill_env(n: u64, fill: fbc_core::FillEvent, exch: Option<i64>) -> Envelope<fbc_core::ExecEvent> {
    Envelope {
        stamp: Stamp {
            ingest_seq: n,
            kernel_rx: None,
            recv_mono: MonoNs(n),
            recv_wall: WALL,
            conn: conn(0),
        },
        exch_ts: exch.map(ExchNs),
        exch_ts_kind: ExchTsKind::MatchingEngine,
        venue_seq: None,
        body: fbc_core::ExecEvent::Fill(fill),
    }
}

/// Asserts that the place stages' common stop path names `stop`: its marker written whole
/// (0151 item 6), and the report and the evidence naming it, as every run stopped with it does
/// after its after-session read ([`assert_stopped`]).
fn assert_stop_reported(stop: places::Stop) {
    let (dir, _removed) = temp_dir("drop-stop-path");
    places::tag(&dir).unwrap();
    assert!(places::write_marker(&dir, stop, &[], "synthetic").unwrap());
    let found = places::marker_in(&dir).unwrap().unwrap();
    assert_eq!(found.stops, [stop.name()]);
    let ev = Evidence {
        places_asked: true,
        stop: Some(stop.name()),
        ..Evidence::default()
    };
    let report = report::markdown(&ev);
    let named = format!("**The place stages' stop:** {}.", stop.name());
    assert!(report.contains(&named), "{report}");
    assert_eq!(report::to_json(&ev)["stop"], stop.name());
}

#[test]
fn stop_on_an_untimed_replayed_fill() {
    use fbc_runtime::ExecHandler;
    // A replayed fill with no matching-engine time, which nothing shows newer than the
    // registry's watermark: its ledger refuses it Untimed, and the handler stops the session.
    let (mut handler, link) = handled(100);
    handler.on_exec(fill_env(1, qfex_fill(our_cid(), T1, true), None));
    assert_eq!(link.borrow().fill_refusals()[0].admission, "Untimed");
    let stop = places::Stop::ReplayRefused(places::Replay::Untimed);
    assert_eq!(handler.watched.borrow().stop(), Some(stop));
    assert_eq!(stop.name(), "ReplayRefused(Untimed)");
    assert!(stop.marks() && !stop.cancels_in_drop(false));
    assert_stop_reported(stop);
}

#[test]
fn stop_on_a_beyond_horizon_replayed_fill() {
    use fbc_runtime::ExecHandler;
    // The ledger keeps one fill: a second forgets the first, so a replay at or before the
    // horizon may be one it applied and forgot, refused BeyondHorizon.
    let (mut handler, link) = handled(1);
    let cid = our_cid();
    handler.on_exec(fill_env(1, qfex_fill(cid, T1, false), Some(2_000)));
    handler.on_exec(fill_env(2, qfex_fill(cid, T2, false), Some(3_000)));
    handler.on_exec(fill_env(3, qfex_fill(cid, T3, true), Some(1_500)));
    let refused: Vec<&str> = (link.borrow().fill_refusals().iter())
        .map(|r| r.admission)
        .collect();
    assert_eq!(refused, ["BeyondHorizon"]);
    let stop = places::Stop::ReplayRefused(places::Replay::BeyondHorizon);
    assert!(handler.watched.borrow().stops.contains(&stop));
    assert_stop_reported(stop);
}

#[tokio::test]
async fn stop_on_a_resync_desync() {
    // The account reads flat before the session, and its resync's GET /user/positions then shows
    // a position the flat seed does not hold: the registry reports a desync.
    let moved = r#"{"balance":{},"positions":[{"symbol":"SYN-USD","position":1,"open_orders":0}]}"#;
    let model = QfexModel {
        positions: HashMap::from([
            (None, moved.to_owned()),
            (Some(SUB.to_owned()), model::FLAT.to_owned()),
        ]),
        lag: 1,
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-desync").await;
    assert_stopped(&d, places::Stop::ResyncDesync);
    // No X: the resync ended the steps before Start.
    let run = d.drop_run();
    assert!(!run.started && !run.x.submitted, "{run:?}");
    assert_eq!(d.count("add_order"), 0);
    assert_eq!(run.ended_by, Some(stage::GRACEFUL));
}

#[test]
fn stop_on_a_resync_unsettled() {
    use fbc_oms::{PositionCheck, ResyncReport};
    // A resync whose report leaves a market unsettled, by its check or by a fill across it.
    let checked = ResyncReport {
        checks: vec![PositionCheck::Unsettled(market::INST)],
        ..ResyncReport::default()
    };
    assert_eq!(
        places::resync_stop(&checked),
        Some(places::Stop::ResyncUnsettled)
    );
    let fill = qfex_fill(our_cid(), T1, false);
    let crossed = ResyncReport {
        unsettled: vec![(market::INST, fill.key())],
        ..ResyncReport::default()
    };
    assert_eq!(
        places::resync_stop(&crossed),
        Some(places::Stop::ResyncUnsettled)
    );
    // A desync is a desync, a clean report no stop.
    let desync = ResyncReport {
        checks: vec![PositionCheck::Desync {
            inst: market::INST,
            venue: fbc_core::SignedLots(1),
            ledger: Some(fbc_core::SignedLots(0)),
        }],
        ..ResyncReport::default()
    };
    assert_eq!(
        places::resync_stop(&desync),
        Some(places::Stop::ResyncDesync)
    );
    assert_eq!(places::resync_stop(&ResyncReport::default()), None);
    assert_stop_reported(places::Stop::ResyncUnsettled);
}

#[tokio::test]
async fn stop_on_positions_not_flat_at_session_end() {
    // Once the session's connection ended, the account shows a position no fill of ours made.
    let model = QfexModel {
        position_after_close: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-not-flat").await;
    assert_stopped(&d, places::Stop::NotFlat);
    assert!(
        d.ev.stopped
            .as_deref()
            .unwrap()
            .starts_with("NotFlat after drop session one")
    );
    let after = (d.ev.reads.iter())
        .find(|r| r.before == refusals::AFTER_DROP_ONE && r.label == "primary (drop)")
        .unwrap();
    assert!(!after.flat);
}

#[tokio::test]
async fn a_rate_limited_place_is_reported_as_one_refusal_and_stops_the_run() {
    // QFEX answers X with a RateLimited err echoing it: a provider rate refusal on the session.
    let model = QfexModel {
        rate_limit: Some(model::Limited::PlaceErr),
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-rate-place").await;
    assert_stopped(
        &d,
        places::Stop::RateRefused {
            on: places::RefusedOn::Session,
        },
    );
    // Reported as one refusal, met on the session (decision 0135 item 2), under FBC-s5ox.
    let session = &d.ev.sessions[0];
    assert_eq!(session.refusals.errs, 1, "{:?}", session.refusals);
    assert_eq!(session.refusals.met(), 1);
    assert_eq!(refusals::met(&d.ev, &d.ev.sessions), 1);
    let (report, json) = d.outputs();
    assert!(
        report.contains("provider rate refusals met, at least: 1"),
        "{report}"
    );
    assert_eq!(json["provider_rate_refusals"]["met_at_least"], 1);
    // Nothing rests: no cancel, a graceful close.
    assert_eq!(d.count("cancel_order"), 0);
    assert!(!d.drop_run().x_resting);
}

#[tokio::test]
async fn a_rate_limited_read_socket_stops_the_run() {
    // The comparison's first get_user_orders page after X is answered a RateLimited err.
    let model = QfexModel {
        rate_limit: Some(model::Limited::ReadSocket(0)),
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-rate-read").await;
    assert_stopped(
        &d,
        places::Stop::RateRefused {
            on: places::RefusedOn::ReadSocket,
        },
    );
    let compared = &d.drop_run().compared[0];
    assert_eq!(compared.ended, compare::Ended::Refused);
    assert_eq!(compared.reads.len(), 1);
    // Counted on the comparison's read socket, under drop session one.
    let socket = (d.ev.read_sockets.iter())
        .find(|r| r.label == "primary (the comparison after X's place)")
        .unwrap();
    assert_eq!(socket.before, places::DROP_ONE);
    assert_eq!(socket.refusals.errs, 1);
    assert_no_cancel_and_x_resting(&d);
    // QFEX refused once: counted once, on the comparison's socket, and not again as the
    // session's, though the session ran beside it.
    assert_eq!(d.ev.sessions[0].refusals.limiter, 0);
    let rows = refusals::by_session(&d.ev.read_sockets, &d.ev.sessions, None);
    let row = rows
        .iter()
        .find(|(name, ..)| *name == places::DROP_ONE)
        .unwrap();
    assert_eq!(row.1, 1, "{rows:?}");
    assert_eq!(refusals::total(&d.ev.read_sockets, &d.ev.sessions), 1);
}

#[tokio::test]
async fn a_rest_429_stops_the_run() {
    // The comparison's GET /user/positions after X is answered HTTP 429.
    let model = QfexModel {
        rate_limit: Some(model::Limited::Rest(0)),
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-rest-429").await;
    assert_stopped(
        &d,
        places::Stop::RateRefused {
            on: places::RefusedOn::Rest,
        },
    );
    assert_eq!(d.drop_run().compared[0].ended, compare::Ended::Refused);
    assert_no_cancel_and_x_resting(&d);
}

/// [`drop_argv`] with a request deadline of ten seconds: a session waiting for X's answer is
/// still waiting as the model closes its connection, however loaded the machine.
fn patient_rpc(argv: Argv) -> Argv {
    set(argv, "--rpc-timeout-ms", "10000")
}

/// [`drop_argv`] with a comparison's wait of three reads: at once, then one each second.
fn short_wait(argv: Argv) -> Argv {
    set(argv, "--agree-wait-ms", "2500")
}

#[tokio::test]
async fn stop_on_a_read_listing_an_order_the_streams_never_showed() {
    // With X, the account holds an open order no frame named: every read lists it.
    let model = QfexModel {
        foreign_order: true,
        ..QfexModel::default()
    };
    let d = dropped_with(model, "drop-read-order", short_wait).await;
    assert_stopped(&d, places::Stop::ReadDisagrees);
    let compared = &d.drop_run().compared[0];
    assert_eq!(compared.ended, compare::Ended::Disagreed);
    // Each read the wait allowed disagreed: three, or two on a machine so loaded that a read
    // took longer than half a second.
    assert!((2..=3).contains(&compared.reads.len()), "{compared:?}");
    assert!(
        compared
            .reads
            .iter()
            .all(|(_, t)| *t == compare::Took::Disagreed)
    );
    assert_no_cancel_and_x_resting(&d);
}

#[tokio::test]
async fn stop_on_a_read_showing_a_position_the_streams_did_not() {
    // With X, the account's REST position moves though no fill reached the session.
    let model = QfexModel {
        position_on_place: true,
        ..QfexModel::default()
    };
    let d = dropped_with(model, "drop-read-position", short_wait).await;
    assert_stopped(&d, places::Stop::ReadDisagrees);
    let compared = &d.drop_run().compared[0];
    assert_eq!(compared.ended, compare::Ended::Disagreed);
    assert_eq!(compared.first_agreed, None);
    assert_no_cancel_and_x_resting(&d);
}

#[tokio::test]
async fn reads_keep_the_read_interval() {
    let model = QfexModel {
        foreign_order: true,
        ..QfexModel::default()
    };
    let d = dropped_with(model, "drop-interval", short_wait).await;
    let began: Vec<Duration> = d.drop_run().compared[0]
        .reads
        .iter()
        .map(|(at, _)| *at)
        .collect();
    assert!((2..=3).contains(&began.len()), "{began:?}");
    // Each read begins at least --read-interval-ms after the one before, and none after
    // --agree-wait-ms.
    for pair in began.windows(2) {
        assert!(
            pair[1] - pair[0] >= Duration::from_millis(1_000),
            "{began:?}"
        );
    }
    assert!(
        began.iter().all(|at| *at <= Duration::from_millis(2_500)),
        "{began:?}"
    );
}

#[tokio::test]
async fn each_step_is_compared_and_the_first_agreeing_read_is_timed() {
    // The reads lag the streams by one read after X: the first misses X, the second agrees.
    let model = QfexModel {
        rest_lag: 1,
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-lag").await;
    let compared = &d.drop_run().compared;
    assert_eq!(compared.len(), 1);
    assert_eq!(compared[0].step, "X's place");
    let took: Vec<compare::Took> = compared[0].reads.iter().map(|(_, t)| *t).collect();
    assert_eq!(took, [compare::Took::Lagged, compare::Took::Agreed]);
    let first = compared[0].first_agreed.unwrap();
    assert!(first >= Duration::from_millis(1_000), "{first:?}");
    let (report, json) = d.outputs();
    assert!(report.contains("| X's place | 2 |"), "{report}");
    assert_eq!(
        json["drop_tests"][0]["session_one"]["compared"][0]["ended"],
        "agreed"
    );
    assert!(
        d.printed
            .contains("COMPARE after X's place: 2 reads, the first agreeing")
    );
}

#[tokio::test(start_paused = true)]
async fn a_comparison_reads_no_more_than_its_cap() {
    /// A reader whose every read lags, counting them.
    struct Lagging(usize);
    impl compare::Reader for Lagging {
        async fn read(&mut self) -> compare::Attempt {
            self.0 += 1;
            compare::Attempt::Read(compare::Read::default())
        }
    }
    let streams = || compare::Streams {
        orders: Vec::new(),
        position: Decimal::ONE,
    };
    // The run's cadence at the longest wait the command line takes at its interval: ten reads,
    // the last beginning at 9 s, since none begins at the wait's end.
    let argv = set(parse_only(), "--agree-wait-ms", "10000");
    let argv = set(argv, "--read-interval-ms", "1000");
    let (_, places) = parsed(&argv).unwrap();
    let cadence = compare::Cadence::of(&places);
    assert_eq!(cadence.reads(), probe_args::MAX_READS);
    let mut reader = Lagging(0);
    let now = tokio::time::Instant::now();
    let compared = compare::compare("X's place", &mut reader, streams, cadence, now).await;
    assert_eq!(reader.0, 10);
    assert_eq!(compared.reads.len(), 10);
    assert_eq!(compared.stop, Some(places::Stop::ReadDisagrees));
}

/// A run of `model` against which the owner presses Ctrl-C once for each of `when`, each once
/// the model's log first holds it, in turn, the answers the model holds sent after each press
/// ([`Venue::release_held`]).
async fn pressed_drop(
    model: QfexModel,
    what: &str,
    argv: impl Fn(Argv) -> Argv,
    when: &[&str],
) -> Dropped {
    let (dir, removed) = temp_dir(what);
    let venue = Venue::start(model).await;
    let (mut opts, places) = parsed(&argv(drop_argv(&dir, &venue))).unwrap();
    opts.rate_user = user();
    let env = environment();
    let printed = venue.printed();
    let lookup = |var: &str| env.get(var).cloned();
    let out: stage::Out = Rc::new(RefCell::new(printed.clone()));
    let (mut press, presses) = interrupt::Presses::channel();
    let hold: stage::Interrupt = Box::pin(std::future::pending());
    let drive = async {
        for what in when {
            let seen = |log: &[model::Event]| log.iter().any(|e| e.what.ends_with(what));
            until("the moment to press", || seen(&venue.log())).await;
            press();
            venue.release_held().await;
        }
    };
    let ran = stage::run_places_until(&opts, &places, &lookup, out, hold, presses);
    let (ran, ()) = tokio::join!(ran, drive);
    let printed = printed.text();
    let ev = stopped(ran, &printed);
    Dropped {
        dir,
        _removed: removed,
        venue,
        ev,
        printed,
    }
}

#[tokio::test]
async fn ctrl_c_in_a_drop_session_cancels_by_venue_id_and_fails_it() {
    let d = pressed_drop(QfexModel::default(), "drop-ctrl-c", |a| a, &[" add_order"]).await;
    assert_stopped(&d, places::Stop::Interrupted);
    // Kill, then X cancelled by its venue id, seen to end, the market disarmed, a graceful
    // close: the drop test's measurement is over, so it does not pass (0151 item 2).
    assert_eq!(d.count("cancel_order"), 1);
    let model = d.venue.model.lock().unwrap();
    assert_eq!(model.orders[0].state, model::State::Cancelled);
    let x = model.orders[0].id.clone();
    drop(model);
    let journal = d.dir.join("evidence").join("journal");
    let reader = fbc_journal::JournalReader::open(&journal, 0).unwrap();
    let named = reader.entries().any(|e| match e.unwrap().record {
        fbc_journal::Record::Outbound { frame, .. } => {
            let text = String::from_utf8_lossy(frame.bytes()).into_owned();
            text.contains("cancel_order") && text.contains(&x)
        }
        _ => false,
    });
    assert!(named, "the cancel names X by its venue id");
    let run = d.drop_run();
    let cancels = run.cancels.as_ref().unwrap();
    assert_eq!(
        (cancels.sent, cancels.held, cancels.ended),
        (1, false, true)
    );
    assert_eq!(run.ended_by, Some(stage::GRACEFUL));
    assert!(!run.x_resting && run.close_cancelled_x.is_none());
    // The streams compared again after the cancels.
    let steps: Vec<&str> = run.compared.iter().map(|c| c.step).collect();
    assert!(steps.ends_with(&["the stop's cancels"]), "{steps:?}");
    assert!(d.printed.contains(
        "STOP cancels: Kill, then 1 cancels by venue id (0151 item 2), every order they named \
         seen to end; the market disarmed"
    ));
    let (report, _) = d.outputs();
    assert!(
        report.contains("The stop's cancels: Kill, then 1 cancel by venue id"),
        "{report}"
    );
}

#[tokio::test]
async fn a_second_ctrl_c_during_the_stops_cancels_is_recorded() {
    // The cancel's answer comes two seconds after it: the second press lands while the stop's
    // cancels wait for X to end, which --rpc-timeout-ms (five seconds here) bounds.
    let model = QfexModel {
        cancel_delay: Some(Duration::from_secs(2)),
        ..QfexModel::default()
    };
    let rpc = |argv| set(argv, "--rpc-timeout-ms", "5000");
    let d = pressed_drop(
        model,
        "drop-ctrl-c-twice",
        rpc,
        &[" add_order", " cancel_order"],
    )
    .await;
    assert_stopped(&d, places::Stop::Interrupted);
    let cancels = d.drop_run().cancels.as_ref().unwrap();
    assert_eq!(cancels.presses, 1);
    assert!(cancels.ended && !cancels.held, "{cancels:?}");
    assert!(
        d.printed
            .contains("STOP cancels: 1 more presses of Ctrl-C while they ran, recorded")
    );
    let (report, json) = d.outputs();
    assert!(
        report.contains("1 press of Ctrl-C while they ran, each recorded"),
        "{report}"
    );
    assert_eq!(
        json["drop_tests"][0]["session_one"]["cancels"]["presses"],
        1
    );
}

#[tokio::test]
async fn held_stop_cancels_end_by_a_graceful_close_before_a_passed_drop_test() {
    // The owner's Ctrl-C, whose cancel of X QFEX answers with a RateLimited err: the cancels
    // are held, and with no drop test passed the session ends by a graceful close, X resting
    // (0151 item 11).
    let model = QfexModel {
        rate_limit: Some(model::Limited::Cancel),
        ..QfexModel::default()
    };
    let d = pressed_drop(model, "drop-held", |a| a, &[" add_order"]).await;
    assert_stopped(&d, places::Stop::Interrupted);
    let run = d.drop_run();
    let cancels = run.cancels.as_ref().unwrap();
    assert!(cancels.held && !cancels.ended, "{cancels:?}");
    assert_eq!(run.ended_by, Some(stage::GRACEFUL));
    assert!(run.x_resting);
    let model = d.venue.model.lock().unwrap();
    assert_eq!(model.orders[0].state, model::State::Open);
    let closed: Vec<&String> = model
        .log
        .iter()
        .map(|e| &e.what)
        .filter(|w| w.starts_with("cod "))
        .collect();
    assert!(closed.iter().all(|w| w.contains(" close ")), "{closed:?}");
    drop(model);
    let (report, _) = d.outputs();
    assert!(
        report.contains("held or refused, so not every order was seen to end"),
        "{report}"
    );
}

#[tokio::test]
async fn a_server_close_mid_session_stops_the_run_with_its_reads_and_report() {
    // QFEX closes the session's connection as it reads X, which it leaves unanswered.
    let model = QfexModel {
        close_on_place: true,
        ..QfexModel::default()
    };
    let d = dropped_with(model, "drop-server-close", patient_rpc).await;
    assert_stopped(&d, places::Stop::ConnectionEnded);
    let run = d.drop_run();
    assert_eq!(run.ended_by, Some(stage::CONNECTION_ENDED));
    assert_eq!(run.cancels, None);
    assert_eq!(d.count("cancel_order"), 0);
    // The reads after it show X as QFEX left it once the connection ended.
    assert!(run.x_read);
    let rests = d.venue.model.lock().unwrap().orders[0].state == model::State::Open;
    assert_eq!(run.x_resting, rests);
    assert!(
        d.printed.contains("ended by the connection's end"),
        "{}",
        d.printed
    );
}

#[tokio::test(start_paused = true)]
async fn the_span_bound_in_the_drop_test_cancels_nothing_and_fails_it() {
    let frozen = freeze();
    // X is never answered; while the session waits for it, the registry's span bound (600 s
    // here) passes: no cancel, a graceful close, and the stop.
    let model = QfexModel {
        ack_delay: Some(Duration::from_secs(86_400)),
        ..QfexModel::default()
    };
    let (dir, removed) = temp_dir("drop-span");
    let venue = Venue::start(model).await;
    let (mut opts, places) = parsed(&drop_argv(&dir, &venue)).unwrap();
    opts.rate_user = user();
    let env = environment();
    let printed = venue.printed();
    let lookup = |var: &str| env.get(var).cloned();
    let out: stage::Out = Rc::new(RefCell::new(printed.clone()));
    let drive = async {
        let placed = |log: &[model::Event]| log.iter().any(|e| e.what.ends_with(" add_order"));
        until("X's place", || placed(&venue.log())).await;
        tokio::time::advance(Duration::from_secs(601)).await;
    };
    let ran = stage::run_places(&opts, &places, &lookup, out);
    let (ran, ()) = tokio::join!(ran, drive);
    drop(frozen);
    let printed = printed.text();
    let ev = stopped(ran, &printed);
    let d = Dropped {
        dir,
        _removed: removed,
        venue,
        ev,
        printed,
    };
    assert_stopped(&d, places::Stop::SpanBound);
    assert_no_cancel_and_x_resting(&d);
    assert!(
        d.ev.stopped
            .as_deref()
            .unwrap()
            .starts_with("SpanBound in drop session one"),
        "{:?}",
        d.ev.stopped
    );
    // Its registry's ledger, at the session's end, past its span bound.
    let end = &d.ev.place_registries[0];
    assert!(!end.held() || end.deadline() <= tokio::time::Instant::now());
}

#[tokio::test]
async fn a_maker_fill_of_x_in_drop_session_one_stops_with_cancels_not_flat() {
    // After X's ACK, X fills whole as a maker: a fill of ours, so the session cancels by venue
    // id (nothing of ours is left to cancel), closes gracefully, and the reads after it show a
    // position: the run stops not flat (FBC-ou9g adds the flatten, 0151 item 9).
    let model = QfexModel {
        maker_fill: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-maker-fill").await;
    assert_stopped(&d, places::Stop::UnexpectedFill);
    let run = d.drop_run();
    let cancels = run.cancels.as_ref().expect("a fill of ours cancels");
    assert!(cancels.ended && !cancels.held);
    assert_eq!(run.ended_by, Some(stage::GRACEFUL));
    let after = (d.ev.reads.iter())
        .find(|r| r.before == refusals::AFTER_DROP_ONE && r.label == "primary (drop)")
        .unwrap();
    assert!(!after.flat);
    // X filled is named by a trade since its send and ended FILLED on historic orders.
    let x = run.after.as_ref().unwrap();
    assert_eq!(x.trades_naming_x, 1);
    assert_eq!(x.terminal_status.as_deref(), Some("FILLED"));
    let steps: Vec<&str> = run.compared.iter().map(|c| c.step).collect();
    assert_eq!(steps, ["the stop's cancels"]);
}

#[tokio::test]
async fn a_stop_in_a_drop_session_sends_no_cancel_and_reports_x_resting() {
    // Each stop of the session but Ctrl-C and a fill of ours: no cancel, a graceful close, X
    // reported resting for the owner's hand cancel (0151 item 2).
    let cases: [(QfexModel, places::Stop); 3] = [
        (
            QfexModel {
                stray_order: true,
                ..QfexModel::default()
            },
            places::Stop::StrayOrder,
        ),
        (
            QfexModel {
                cid_absent_update: true,
                ..QfexModel::default()
            },
            places::Stop::NotOurCid,
        ),
        (
            QfexModel {
                rate_limit: Some(model::Limited::Rest(0)),
                ..QfexModel::default()
            },
            places::Stop::RateRefused {
                on: places::RefusedOn::Rest,
            },
        ),
    ];
    for (model, stop) in cases {
        let d = dropped(model, "drop-no-cancel").await;
        assert_stopped(&d, stop);
        assert_no_cancel_and_x_resting(&d);
        assert!(!d.printed.contains("STOP cancels"), "{}", d.printed);
    }
}

#[tokio::test]
async fn every_read_and_page_is_charged_to_the_shared_limiter() {
    // As QFEX reads X, the run's shared provider-user budget is held for 5 s: each read of the
    // comparison after X is charged to it first, so those made meanwhile are skipped, nothing
    // of them sent, and not waited for; the first one after the hold reads and agrees.
    let rate_user = user();
    let limiter = shared_limiter(rate_user);
    let model = QfexModel {
        hold_on_place: Some((limiter.clone(), Duration::from_secs(5))),
        ..QfexModel::default()
    };
    let (dir, removed) = temp_dir("drop-charged");
    let venue = Venue::start(model).await;
    let asked = set(drop_argv(&dir, &venue), "--agree-wait-ms", "9000");
    let (mut opts, places) = parsed(&asked).unwrap();
    opts.rate_user = rate_user;
    let env = environment();
    let printed = venue.printed();
    let lookup = |var: &str| env.get(var).cloned();
    let out: stage::Out = Rc::new(RefCell::new(printed.clone()));
    let ran = stage::run_places(&opts, &places, &lookup, out).await;
    let printed = printed.text();
    let ev = stopped(ran, &printed);
    let d = Dropped {
        dir,
        _removed: removed,
        venue,
        ev,
        printed,
    };
    let took: Vec<compare::Took> = d.drop_run().compared[0]
        .reads
        .iter()
        .map(|(_, t)| *t)
        .collect();
    assert_eq!(
        took[..2],
        [compare::Took::Skipped, compare::Took::Skipped],
        "{took:?}"
    );
    assert_eq!(took.last(), Some(&compare::Took::Agreed), "{took:?}");
    // The skipped reads wrote nothing: only the one made authenticated a comparison socket.
    let what = d.what();
    let place = what.iter().position(|w| w.ends_with(" add_order")).unwrap();
    let closed = what.iter().position(|w| w.starts_with("cod ")).unwrap();
    let auths = what[place..closed]
        .iter()
        .filter(|w| w.ends_with(" auth"))
        .count();
    assert_eq!(auths, 1, "{what:?}");
    // The run's refusals, counted on the shared budget the run and the model share.
    assert!(limiter.counts().refused.user >= 2);
    // The reads after the session, which must be made, waited the hold out and were made; the
    // drop tests then ran to the run's end (FBC-77r8).
    assert_after_reads(&d);
    assert_eq!(d.ev.stop, None);
    assert_eq!(d.ev.stopped.as_deref(), Some(stage::NOT_BUILT));
}

#[test]
fn a_resync_page_listing_x_is_paired_and_counted() {
    let found = spec_of(Listed::trading());
    let ask = |offset: u32| {
        format!(r#"{{"type":"get_user_orders","params":{{"limit":1,"offset":{offset}}}}}"#)
    };
    let x = |price: &str, id: &str| {
        format!(
            r#"{{"all_orders_response":{{"orders":[{{"order_id":"{id}","symbol":"SYN-USD","status":"ACK","quantity":0.01,"price":{price},"side":"BUY","type":"ALO","time_in_force":"GTC","quantity_remaining":0.01,"client_order_id":"synthetic"}}],"twaps":[]}}}}"#
        )
    };
    let id = "11111111-1111-4111-8111-000000000001";
    // A drop session holds the traded spec, so a page listing X on its symbol is read as the
    // codec reads it: paired with the ask it answers, and full at a page size of one.
    let mut pages = pages::Pages::default();
    pages.sent(ask(0).as_bytes());
    pages.heard_on(&x("99.5", id), &found.specs);
    pages.sent(ask(1).as_bytes());
    pages.heard_on(
        r#"{"all_orders_response":{"orders":[],"twaps":[]}}"#,
        &found.specs,
    );
    assert_eq!((pages.asked, pages.read), (2, 2));
    assert_eq!(pages.full, [pages::Page { size: 1, offset: 0 }]);
    // A row on the traded symbol the codec refuses, a price off the grid or an order id that is
    // no UUID, is no page: the codec refuses it and ends its epoch, so the ask is unanswered.
    for (price, id) in [("99.505", id), ("99.5", "x7q")] {
        let mut refused = pages::Pages::default();
        refused.sent(ask(0).as_bytes());
        refused.heard_on(&x(price, id), &found.specs);
        assert_eq!((refused.asked, refused.read), (1, 0), "{price} {id}");
        assert_eq!(pages::rows_on(&x(price, id), 1, &found.specs), None);
        // On an empty spec table, as stage one's sessions run, the codec reads no more of it.
        assert_eq!(pages::rows(&x(price, id), 1), Some(1));
    }
    // Each member the codec reads of a held row, read as it does.
    let spec = found.specs.get(market::INST).unwrap();
    let row = |change: &dyn Fn(&mut serde_json::Map<String, Value>)| {
        let page = super::json(&x("99.5", id));
        let mut row = page["all_orders_response"]["orders"][0]
            .as_object()
            .unwrap()
            .clone();
        change(&mut row);
        pages::held(&row, spec)
    };
    assert!(row(&|_| {}));
    assert!(row(&|r| {
        r.remove("client_order_id");
    }));
    assert!(row(&|r| {
        r.insert("client_order_id".into(), Value::Null);
    }));
    assert!(row(&|r| {
        r.insert("quantity_remaining".into(), super::json("0"));
    }));
    assert!(row(&|r| {
        r.insert("price".into(), super::json("9.95e1"));
    }));
    for bad in [
        ("side", "\"LONG\""),
        ("price", "0"),
        ("price", "\"99.5\""),
        ("quantity", "0.015"),
        ("quantity_remaining", "0.02"),
        ("quantity_remaining", "-0.01"),
        ("quantity_remaining", "0.005"),
        ("client_order_id", "7"),
    ] {
        assert!(
            !row(&|r| {
                r.insert(bad.0.into(), super::json(bad.1));
            }),
            "{bad:?}"
        );
    }
}

#[tokio::test]
async fn no_synthetic_value_reaches_an_output_but_the_journal() {
    let caps = |argv: Argv| {
        let argv = set(argv, "--resting-cap-usd", "424242.17");
        let argv = set(argv, "--inventory-cap-usd", "373737.29");
        set(argv, "--namespace", "61437")
    };
    let d = dropped_with(QfexModel::default(), "drop-no-value", caps).await;
    assert_eq!(d.ev.stop, None);
    let model = d.venue.model.lock().unwrap();
    let orders = model.orders.clone();
    drop(model);
    // Every synthetic value the run met: the credentials and the subaccount's id, X's and Y's
    // venue ids and client ids on both accounts, the user id QFEX's frames name, their price and
    // size, both caps, the symbol and the namespace.
    let mut values: Vec<String> = [PUBLIC, SECRET, SUB_PUBLIC, SUB_SECRET, SUB, model::USER_ID]
        .map(str::to_owned)
        .to_vec();
    assert_eq!(orders.len(), 4);
    for order in &orders {
        values.extend([order.id.clone(), order.cid.clone()]);
    }
    values.extend(["99.5".to_owned(), "0.01".to_owned()]);
    values.extend(["424242", "373737", SYMBOL, "61437"].map(str::to_owned));
    // The report, the evidence and the drop-production/ slice the owner commits.
    let evidence = d.dir.join("evidence");
    let slice = evidence.join(stage::DROP_SLICE);
    assert!(slice.join("README.md").exists());
    assert!(!evidence.join("fixtures").exists());
    assert!(d.ev.fixtures.len() > 1, "{:?}", d.ev);
    assert!(
        d.ev.withheld.is_empty() && d.ev.errors.is_empty(),
        "{:?}",
        d.ev.errors
    );
    let journal = evidence.join("journal");
    let outputs: Vec<(PathBuf, String)> = files(&evidence)
        .into_iter()
        .filter(|(p, _)| !p.starts_with(&journal))
        .collect();
    assert!(outputs.len() >= 3);
    for (path, text) in &outputs {
        for value in &values {
            assert!(
                !text.contains(value.as_str()),
                "{}: {value}",
                path.display()
            );
        }
    }
    // Standard output shows none of them, but the two cap values on its CAPS line.
    for line in d.printed.lines() {
        let caps = line.starts_with("CAPS ");
        for value in &values {
            let a_cap = value == "424242" || value == "373737";
            if !(caps && a_cap) {
                assert!(!line.contains(value.as_str()), "{line}: {value}");
            }
        }
    }
    assert!(d.printed.lines().next().unwrap().contains("424242.17"));
    // The journal, which stays local (decision 0006), keeps each add_order as written.
    assert_eq!(written(&d, "add_order"), [true; 4]);
    // Each output passes scripts/privacy-check.sh, the slice by the run itself.
    assert_report_and_evidence_pass_the_privacy_check(&evidence);
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/privacy-check.sh");
    evidence::privacy_check(&script, &slice).unwrap();
    let (report, _) = d.outputs();
    for account in ["primary", "subaccount"] {
        let heading =
            format!("## The drop test on {account} (FBC-23ch and FBC-77r8, decision 0132 item 4)");
        assert!(report.contains(&heading), "{report}");
    }
    assert!(report.contains("## The place stages' registries"));
    let readme = fs::read_to_string(slice.join("README.md")).unwrap();
    assert!(
        readme.contains("both sessions of each named account's drop test of decision 0132 item 4"),
        "{readme}"
    );
}

#[tokio::test]
async fn an_arm_not_accepted_places_nothing_and_stops_the_run() {
    // QFEX rejects the cancel-on-disconnect arm: the session's runtime ends the connection whose
    // arm failed, so X is never placed, the end is unplanned, and the run stops with its reads
    // after it, its report and its marker.
    let model = QfexModel {
        arm_err: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-arm-refused").await;
    assert_stopped(&d, places::Stop::ConnectionEnded);
    let run = d.drop_run();
    assert_eq!(run.arm.as_deref(), Some("Rejected"));
    assert!(!run.started && !run.x.submitted, "{run:?}");
    assert_eq!(run.ended_by, Some(stage::CONNECTION_ENDED));
    assert_eq!(d.count("add_order"), 0);
    assert!(d.ev.sessions[0].arm_failures > 0 || d.ev.sessions[0].arm.is_some());
    let (report, _) = d.outputs();
    assert!(
        report.contains("X resting when the run ended: no: X was not placed"),
        "{report}"
    );
}

/// What the probe prints, which presses Ctrl-C once, as the line holding `at` is written: a
/// press made at that point of the run, before it awaits anything more.
struct PressAt {
    printed: Printed,
    at: &'static str,
    press: Option<Box<dyn FnMut()>>,
}

impl std::io::Write for PressAt {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.printed.write(buf)?;
        if self.printed.text().contains(self.at)
            && let Some(mut press) = self.press.take()
        {
            press();
        }
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn a_press_made_after_the_arm_and_before_x_places_nothing() {
    // The owner presses Ctrl-C as the market is Started, after the arm's wait and before X is
    // built: the press is taken before X, so nothing is placed (0151 item 2).
    let (dir, removed) = temp_dir("drop-press-before-x");
    let venue = Venue::start(QfexModel::default()).await;
    let (mut opts, places) = parsed(&drop_argv(&dir, &venue)).unwrap();
    opts.rate_user = user();
    let env = environment();
    let printed = venue.printed();
    let lookup = |var: &str| env.get(var).cloned();
    let (press, presses) = interrupt::Presses::channel();
    let out: stage::Out = Rc::new(RefCell::new(PressAt {
        printed: printed.clone(),
        at: "START primary",
        press: Some(Box::new(press)),
    }));
    let hold: stage::Interrupt = Box::pin(std::future::pending());
    let ran = stage::run_places_until(&opts, &places, &lookup, out, hold, presses).await;
    let printed = printed.text();
    let ev = stopped(ran, &printed);
    let d = Dropped {
        dir,
        _removed: removed,
        venue,
        ev,
        printed,
    };
    assert_stopped(&d, places::Stop::Interrupted);
    assert_eq!(d.count("add_order"), 0, "{:?}", d.what());
    let run = d.drop_run();
    assert!(run.started && !run.x.submitted, "{run:?}");
    assert!(!d.printed.contains("PLACE X"), "{}", d.printed);
}

#[tokio::test]
async fn a_rate_refusal_in_the_reads_after_the_session_stops_the_run() {
    // The after-session read's page is answered a RateLimited err, or its GET /user/positions
    // HTTP 429: a provider rate refusal, so a stop, its marker written, though X still rests.
    let cases = [
        (
            model::Limited::ReadSocketAfterSession,
            places::RefusedOn::ReadSocket,
        ),
        (model::Limited::RestAfterSession, places::RefusedOn::Rest),
    ];
    for (limited, on) in cases {
        let model = QfexModel {
            rate_limit: Some(limited),
            ..QfexModel::default()
        };
        let d = dropped(model, "drop-rate-after").await;
        let stop = places::Stop::RateRefused { on };
        assert_eq!(d.ev.stop, Some(stop.name()), "{}", d.printed);
        let marker = d.marker().unwrap_or_default();
        assert!(
            marker.starts_with(&format!("stop: {}\n", stop.name())),
            "{marker}"
        );
        assert!(
            d.ev.stopped.as_deref().unwrap().starts_with(&format!(
                "{} in the reads after drop session one",
                stop.name()
            )),
            "{:?}",
            d.ev.stopped
        );
        // The primary's read failed on the refusal; the subaccount's was still made.
        let after: Vec<&str> = (d.ev.reads.iter())
            .filter(|r| r.before == refusals::AFTER_DROP_ONE)
            .map(|r| r.label.as_str())
            .collect();
        assert_eq!(after, ["subaccount"], "{}", d.printed);
        assert_eq!(d.drop_run().ended_by, Some(stage::GRACEFUL));
        let (report, json) = d.outputs();
        assert!(
            report.contains(&format!("**The place stages' stop:** {}.", stop.name())),
            "{report}"
        );
        assert_eq!(json["stop"], stop.name());
        assert_report_and_evidence_pass_the_privacy_check(&d.dir.join("evidence"));
        d.venue.assert_served();
    }
}

#[tokio::test]
async fn a_stop_raised_while_the_close_waits_for_room_is_the_runs() {
    // As QFEX reads X, the shared budget is held for 5 s, so the comparison reads nothing within
    // its one second and the close waits for room; at 2.5 s, while it waits, QFEX sends an
    // order_response naming no order of ours (0151 item 13): the run's stop, marked.
    let rate_user = user();
    let limiter = shared_limiter(rate_user);
    let model = QfexModel {
        hold_on_place: Some((limiter, Duration::from_secs(5))),
        stray_after: Some(Duration::from_millis(2500)),
        ..QfexModel::default()
    };
    let (dir, removed) = temp_dir("drop-stop-at-close");
    let venue = Venue::start(model).await;
    let asked = set(drop_argv(&dir, &venue), "--agree-wait-ms", "1000");
    let (mut opts, places) = parsed(&asked).unwrap();
    opts.rate_user = rate_user;
    let env = environment();
    let printed = venue.printed();
    let lookup = |var: &str| env.get(var).cloned();
    let out: stage::Out = Rc::new(RefCell::new(printed.clone()));
    let ran = stage::run_places(&opts, &places, &lookup, out).await;
    let printed = printed.text();
    let ev = stopped(ran, &printed);
    let d = Dropped {
        dir,
        _removed: removed,
        venue,
        ev,
        printed,
    };
    let compared = &d.drop_run().compared[0];
    assert_eq!(compared.ended, compare::Ended::Unread, "{compared:?}");
    assert_eq!(compared.stop, None);
    assert_stopped(&d, places::Stop::StrayOrder);
    assert_no_cancel_and_x_resting(&d);
}

#[tokio::test]
async fn a_close_with_no_room_in_the_shared_budget_is_not_counted_graceful() {
    // As QFEX reads X, the shared budget is held for an hour: the session's close frame has no
    // room within AFTER_PATIENCE, so the runtime drops it unsent, and the close is not counted
    // graceful nor measured as cancelling X (0151 item 11).
    let rate_user = user();
    let limiter = shared_limiter(rate_user);
    let model = QfexModel {
        hold_on_place: Some((limiter, Duration::from_secs(3600))),
        ..QfexModel::default()
    };
    let (dir, removed) = temp_dir("drop-no-room");
    let venue = Venue::start(model).await;
    let asked = set(drop_argv(&dir, &venue), "--agree-wait-ms", "1000");
    let (mut opts, places) = parsed(&asked).unwrap();
    opts.rate_user = rate_user;
    let env = environment();
    let printed = venue.printed();
    let lookup = |var: &str| env.get(var).cloned();
    let out: stage::Out = Rc::new(RefCell::new(printed.clone()));
    let ran = stage::run_places(&opts, &places, &lookup, out).await;
    let printed = printed.text();
    let ev = stopped(ran, &printed);
    let run = ev.drops.first().expect("drop session one ran");
    assert_eq!(run.ended_by, Some(stage::NO_ROOM_TO_CLOSE), "{printed}");
    assert_eq!(run.close_cancelled_x, None);
    assert!(
        printed.contains(&format!("ended by {}", stage::NO_ROOM_TO_CLOSE)),
        "{printed}"
    );
    let (report, _) = outputs_of(&dir);
    assert!(report.contains(stage::NO_ROOM_TO_CLOSE), "{report}");
    drop(removed);
}

#[tokio::test]
async fn ctrl_c_while_x_and_its_trades_are_read_stops_the_run() {
    // The owner presses Ctrl-C while X's trades are read after the session, X still resting:
    // the press is the run's stop, marked, the reads ending first.
    let model = QfexModel {
        hold_trades: true,
        ..QfexModel::default()
    };
    let d = pressed_drop(model, "drop-press-x-read", |a| a, &[" get_user_trades"]).await;
    assert_stopped(&d, places::Stop::Interrupted);
    assert!(
        d.ev.stopped
            .as_deref()
            .unwrap()
            .starts_with("Interrupted during the reads after drop session one"),
        "{:?}",
        d.ev.stopped
    );
    let run = d.drop_run();
    assert!(run.after.as_ref().unwrap().error.is_none(), "{run:?}");
    assert_eq!(d.count("cancel_order"), 0);
}

#[tokio::test]
async fn a_connection_that_ends_during_the_stops_cancels_ends_the_session() {
    // The owner's Ctrl-C after X; QFEX closes the session's connection as it reads the cancel,
    // which it leaves unanswered: the session ends there, by its connection's end, and the run
    // stops with its reads, report and marker.
    let model = QfexModel {
        close_on_cancel: true,
        ..QfexModel::default()
    };
    let d = pressed_drop(model, "drop-close-in-cancels", patient_rpc, &[" add_order"]).await;
    assert_stopped(&d, places::Stop::Interrupted);
    let run = d.drop_run();
    assert_eq!(run.ended_by, Some(stage::CONNECTION_ENDED), "{run:?}");
    let cancels = run.cancels.as_ref().unwrap();
    assert!(cancels.held && !cancels.ended, "{cancels:?}");
    assert_eq!(d.count("cancel_order"), 1);
}

#[tokio::test]
async fn an_x_that_ended_without_resting_is_not_counted_cancelled_by_the_close() {
    // QFEX answers X with an order_response CANCELLED naming its client id: X never rested, so
    // it was not acked, and the graceful close is not measured as cancelling it.
    let model = QfexModel {
        cancel_place: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-x-ended").await;
    let run = d.drop_run();
    assert!(
        run.x.submitted && !run.x.acked && !run.x.rested_at_close,
        "{run:?}"
    );
    assert_eq!(run.x.ack_after, None);
    assert_eq!(run.ended_by, Some(stage::GRACEFUL));
    assert_eq!(run.close_cancelled_x, None);
    assert!(!run.x_resting);
    let (report, json) = d.outputs();
    let primary = report
        .split("## The drop test on subaccount")
        .next()
        .unwrap();
    assert!(!primary.contains("X rested before it"), "{report}");
    assert_eq!(
        json["drop_tests"][0]["session_one"]["close_cancelled_x"],
        Value::Null
    );
    // And one QFEX cancelled itself after its ACK: acked, but no longer resting as the close
    // was sent, so the close is not measured as cancelling it either.
    let model = QfexModel {
        venue_cancel: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-x-venue-cancel").await;
    let run = d.drop_run();
    assert!(run.x.acked && !run.x.rested_at_close, "{run:?}");
    assert_eq!(run.ended_by, Some(stage::GRACEFUL));
    assert_eq!(run.close_cancelled_x, None);
    assert!(!run.x_resting);
}

#[tokio::test]
async fn a_place_beyond_the_registrys_list_stops_the_run_before_x() {
    // X spent from the drop registry's list before the session: its place is refused before it
    // is built, BeyondList, which stops the run with its reads, report and marker.
    let (dir, removed) = temp_dir("drop-beyond-list");
    let venue = Venue::start(QfexModel::default()).await;
    let (mut opts, mut places) = parsed(&drop_argv(&dir, &venue)).unwrap();
    places.test.x_spent = true;
    opts.rate_user = user();
    let env = environment();
    let printed = venue.printed();
    let lookup = |var: &str| env.get(var).cloned();
    let out: stage::Out = Rc::new(RefCell::new(printed.clone()));
    let ran = stage::run_places(&opts, &places, &lookup, out).await;
    let printed = printed.text();
    let ev = stopped(ran, &printed);
    let d = Dropped {
        dir,
        _removed: removed,
        venue,
        ev,
        printed,
    };
    assert_stopped(&d, places::Stop::BeyondList);
    assert_eq!(d.count("add_order"), 0, "{:?}", d.what());
    let run = d.drop_run();
    assert!(run.started && !run.x.submitted, "{run:?}");
}

#[tokio::test]
async fn the_drop_registrys_trip_is_given_to_the_runs_own_journal() {
    // The run's journal answers every Normal record dropped, so it reaches its soft limit on
    // the session's first one: the journal trips each registry it was given (decision 0139),
    // and journals the move as a CancelOnly marker, which it writes only for a trip it holds.
    let (dir, removed) = temp_dir("drop-trip-journal");
    let venue = Venue::start(QfexModel::default()).await;
    let (mut opts, mut places) = parsed(&drop_argv(&dir, &venue)).unwrap();
    places.test.refuse_normal = true;
    opts.rate_user = user();
    let env = environment();
    let printed = venue.printed();
    let lookup = |var: &str| env.get(var).cloned();
    let out: stage::Out = Rc::new(RefCell::new(printed.clone()));
    let ran = stage::run_places(&opts, &places, &lookup, out).await;
    let printed = printed.text();
    let _ = stopped(ran, &printed);
    let journal = dir.join("evidence").join("journal");
    let reader = fbc_journal::JournalReader::open(&journal, 0).unwrap();
    let moved = reader.entries().any(|e| {
        matches!(
            e.unwrap().record,
            fbc_journal::Record::Marker(fbc_journal::Marker::CancelOnly)
        )
    });
    assert!(moved, "{printed}");
    drop(removed);
}

#[test]
fn a_session_that_ended_first_names_its_stop_and_its_error_by_kind() {
    use fbc_runtime::ExecSessionError;
    // Its handler heard the end, or the tee raised a stop: that stop, the run's error kept.
    let (stop, error) = stage::ended_first(Some(places::Stop::StrayOrder), Ok(()));
    assert_eq!((stop, error), (Some(places::Stop::StrayOrder), None));
    // It ended before any epoch opened, so nothing raised a stop: ConnectionEnded, and its
    // error by the variant's name alone.
    let (stop, error) = stage::ended_first(None, Err(ExecSessionError::NoOrderEntry));
    assert_eq!(stop, Some(places::Stop::ConnectionEnded));
    assert_eq!(
        error.as_deref(),
        Some("the session ended before it ran its course: NoOrderEntry")
    );
}

#[test]
fn a_refusal_is_named_by_its_variant_alone() {
    // fbc-oms's refusals carry client ids and lot counts, which never reach an output.
    let cid = our_cid();
    let named = places::variant_name(&OmsError::DuplicateCid(cid));
    assert_eq!(named, "DuplicateCid");
    let capped = OmsError::Capped(fbc_oms::CapRefusal::RestingCap {
        inst: InstrumentId::new(7),
        side: Side::Buy,
        resting: Some(Lots::new(123).unwrap()),
        cap: Lots::new(45).unwrap(),
    });
    assert_eq!(places::variant_name(&capped), "Capped");
    assert_eq!(
        places::variant_name(&fbc_oms::IssueRefusal::Released(cid)),
        "Released"
    );
    assert_eq!(places::variant_name(&"text"), "");
}

// ----- PR #272's review findings (FBC-23ch) -----

/// A run of `argv` against `model` whose owner presses Ctrl-C once, as the line holding `at`
/// is printed ([`PressAt`]), under the provider user `rate_user` ([`pressed_at`]'s, with those).
async fn pressed_at_as(
    model: QfexModel,
    what: &str,
    rate_user: RateUserKey,
    argv: impl Fn(Argv) -> Argv,
    at: &'static str,
) -> Dropped {
    let (dir, removed) = temp_dir(what);
    let venue = Venue::start(model).await;
    let (mut opts, places) = parsed(&argv(drop_argv(&dir, &venue))).unwrap();
    opts.rate_user = rate_user;
    let env = environment();
    let printed = venue.printed();
    let lookup = |var: &str| env.get(var).cloned();
    let (press, presses) = interrupt::Presses::channel();
    let out: stage::Out = Rc::new(RefCell::new(PressAt {
        printed: printed.clone(),
        at,
        press: Some(Box::new(press)),
    }));
    let hold: stage::Interrupt = Box::pin(std::future::pending());
    let ran = stage::run_places_until(&opts, &places, &lookup, out, hold, presses).await;
    let printed = printed.text();
    let ev = stopped(ran, &printed);
    Dropped {
        dir,
        _removed: removed,
        venue,
        ev,
        printed,
    }
}

#[tokio::test]
async fn a_press_while_the_close_waits_for_room_cancels_x_before_the_close() {
    // Thread 4605918659. As QFEX reads X, the shared budget is held for 5 s, so the comparison
    // reads nothing within its one second and the close waits for room; the owner presses
    // Ctrl-C as that comparison ends, so the close is still waiting: the press is the run's
    // stop, and its cancels (Kill, X by its venue id, the disarm) go out once there is room,
    // before the graceful close (0151 item 2).
    let rate_user = user();
    let model = QfexModel {
        hold_on_place: Some((shared_limiter(rate_user), Duration::from_secs(5))),
        ..QfexModel::default()
    };
    let wait = |argv| set(argv, "--agree-wait-ms", "1000");
    let d = pressed_at_as(
        model,
        "drop-press-at-close",
        rate_user,
        wait,
        "COMPARE after X's place",
    )
    .await;
    assert_eq!(d.drop_run().compared[0].ended, compare::Ended::Unread);
    assert_eq!(d.count("cancel_order"), 1, "{:?}", d.what());
    assert_stopped(&d, places::Stop::Interrupted);
    let run = d.drop_run();
    let cancels = run.cancels.as_ref().unwrap();
    assert_eq!(
        (cancels.sent, cancels.held, cancels.ended),
        (1, false, true)
    );
    assert_eq!(run.ended_by, Some(stage::GRACEFUL));
    assert!(!run.x_resting && run.close_cancelled_x.is_none(), "{run:?}");
    // The cancels were the stop's whole: Kill first, the market disarmed after, both printed
    // before the session's end by its graceful close.
    let cancels = d.at(
        "STOP cancels: Kill, then 1 cancels by venue id (0151 item 2), every order they named \
         seen to end; the market disarmed",
    );
    assert!(cancels < d.at(&format!("ended by {}", stage::GRACEFUL)));
    let model = d.venue.model.lock().unwrap();
    assert_eq!(model.orders[0].state, model::State::Cancelled);
    let at = |what: &str| model.log.iter().position(|e| e.what.contains(what));
    let (cancel, close) = (at(" cancel_order").unwrap(), at("cod ").unwrap());
    assert!(cancel < close, "{:?}", model.log);
}

/// A place-stage run of `argv` against `model`, under the provider user `rate_user`, that
/// reached drop session one.
async fn held_drop(
    model: QfexModel,
    what: &str,
    rate_user: RateUserKey,
    argv: impl Fn(Argv) -> Argv,
) -> Dropped {
    let (dir, removed) = temp_dir(what);
    let venue = Venue::start(model).await;
    let (mut opts, places) = parsed(&argv(drop_argv(&dir, &venue))).unwrap();
    opts.rate_user = rate_user;
    let env = environment();
    let printed = venue.printed();
    let lookup = |var: &str| env.get(var).cloned();
    let out: stage::Out = Rc::new(RefCell::new(printed.clone()));
    let ran = stage::run_places(&opts, &places, &lookup, out).await;
    let printed = printed.text();
    let ev = stopped(ran, &printed);
    assert!(!ev.drops.is_empty(), "{printed}");
    Dropped {
        dir,
        _removed: removed,
        venue,
        ev,
        printed,
    }
}

#[tokio::test]
async fn a_fill_of_ours_while_the_close_waits_for_room_cancels_x_before_the_close() {
    // Thread 4605918659's twin. As QFEX reads X, the shared budget is held for 5 s, so the
    // comparison reads nothing within its one second and the close waits for room; at 2.5 s,
    // while it waits, half of X fills as a maker, the rest resting: a fill of ours, so the
    // stop it raises cancels (0151 item 2), Kill, X's rest by its venue id and the disarm,
    // once there is room and before the graceful close. X is ten lots of 0.001 here, so half
    // of it can fill.
    let rate_user = user();
    let model = QfexModel {
        listed: vec![model::Listed {
            lot: "0.001",
            ..model::Listed::trading()
        }],
        hold_on_place: Some((shared_limiter(rate_user), Duration::from_secs(5))),
        part_fill_after: Some(Duration::from_millis(2500)),
        ..QfexModel::default()
    };
    let wait = |argv| set(argv, "--agree-wait-ms", "1000");
    let d = held_drop(model, "drop-fill-at-close", rate_user, wait).await;
    assert_eq!(d.drop_run().compared[0].ended, compare::Ended::Unread);
    assert_stopped(&d, places::Stop::UnexpectedFill);
    assert_eq!(d.count("cancel_order"), 1, "{:?}", d.what());
    let run = d.drop_run();
    let cancels = run.cancels.as_ref().expect("a fill of ours cancels");
    assert_eq!(
        (cancels.sent, cancels.held, cancels.ended),
        (1, false, true)
    );
    assert_eq!(run.ended_by, Some(stage::GRACEFUL));
    assert!(!run.x_resting && run.close_cancelled_x.is_none(), "{run:?}");
    let cancels = d.at(
        "STOP cancels: Kill, then 1 cancels by venue id (0151 item 2), every order they named \
         seen to end; the market disarmed",
    );
    assert!(cancels < d.at(&format!("ended by {}", stage::GRACEFUL)));
    let model = d.venue.model.lock().unwrap();
    assert_eq!(model.orders[0].state, model::State::Cancelled);
    assert_eq!(model.fills.len(), 1);
    let at = |what: &str| model.log.iter().position(|e| e.what.contains(what));
    let (cancel, close) = (at(" cancel_order").unwrap(), at("cod ").unwrap());
    assert!(cancel < close, "{:?}", model.log);
}

#[tokio::test]
async fn a_press_while_the_close_waits_is_the_stop_when_the_connection_ends_in_that_wait() {
    // As the twin above, the owner presses Ctrl-C as the comparison after X ends, while the
    // close waits for room; at 2.5 s, before the hold lifts, the server closes the session's
    // connection: nothing is left to cancel on, but the press is still the run's stop, marked,
    // not the connection's end that followed it.
    let rate_user = user();
    let model = QfexModel {
        hold_on_place: Some((shared_limiter(rate_user), Duration::from_secs(5))),
        close_after: Some(Duration::from_millis(2500)),
        ..QfexModel::default()
    };
    let wait = |argv| set(argv, "--agree-wait-ms", "1000");
    let d = pressed_at_as(
        model,
        "drop-press-then-end",
        rate_user,
        wait,
        "COMPARE after X's place",
    )
    .await;
    assert_eq!(d.drop_run().compared[0].ended, compare::Ended::Unread);
    assert_stopped(&d, places::Stop::Interrupted);
    let run = d.drop_run();
    assert_eq!(run.ended_by, Some(stage::CONNECTION_ENDED), "{run:?}");
    assert_eq!(run.cancels, None);
    assert_eq!(d.count("cancel_order"), 0);
}

#[tokio::test]
async fn a_close_is_measured_only_on_a_read_of_xs_own_account() {
    // Thread 4605918661. The after-session read of X's account, the primary, fails on a
    // RateLimited err while the subaccount's is made: whether the graceful close cancelled X is
    // not measured, since nothing read X's account after it.
    let model = QfexModel {
        rate_limit: Some(model::Limited::ReadSocketAfterSession),
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-close-unread").await;
    let run = d.drop_run();
    assert!(run.x.rested_at_close && run.cancels.is_none(), "{run:?}");
    assert_eq!(run.ended_by, Some(stage::GRACEFUL));
    assert!(!run.x_read, "{run:?}");
    assert_eq!(run.close_cancelled_x, None, "{run:?}");
    let (report, json) = d.outputs();
    assert_eq!(
        json["drop_tests"][0]["session_one"]["close_cancelled_x"],
        Value::Null
    );
    assert!(
        report.contains("Whether the graceful close cancelled X: not measured"),
        "{report}"
    );
}

#[tokio::test]
async fn x_filled_after_the_close_is_not_counted_as_cancelled_by_it() {
    // PR #272's thread 4608720833. X rests as the graceful close is sent and fills whole once
    // the model has settled the close, before the after-session read of its account: the page
    // no longer lists X, and a get_user_trades row names it. X's absence is a fill's, not the
    // close's cancel (decision 0132 item 4), so it is not measured, and the trade stops the run
    // not flat (0151 item 9).
    let model = QfexModel {
        fill_after_close: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-fill-after-close").await;
    let run = d.drop_run();
    assert!(run.x.rested_at_close && run.cancels.is_none(), "{run:?}");
    assert_eq!(run.ended_by, Some(stage::GRACEFUL));
    assert!(run.x_read && !run.x_resting, "{run:?}");
    let after = run.after.as_ref().unwrap();
    assert_eq!((after.trades_naming_x, after.error.as_ref()), (1, None));
    assert!(after.trades_read, "{after:?}");
    assert_eq!(run.close_cancelled_x, None, "{run:?}");
    assert_not_measured(&d, places::CloseUnmeasured::XTraded);
    assert_stopped(&d, places::Stop::NotFlat);
}

#[tokio::test]
async fn x_gone_with_its_trades_unread_is_not_counted_as_cancelled_by_the_close() {
    // A sibling of PR #272's thread 4608720833. The graceful close cancels X, so the read after
    // it no longer lists X, but every get_user_trades page is answered an InvalidRequest err,
    // which is no rate refusal: nothing rules out a fill of X, so whether the close cancelled
    // it is not measured (decision 0132 item 4).
    let model = QfexModel {
        cod_on_close: true,
        trades_err: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-close-trades-unread").await;
    let run = d.drop_run();
    assert!(run.x.rested_at_close && run.cancels.is_none(), "{run:?}");
    assert_eq!(run.ended_by, Some(stage::GRACEFUL));
    assert!(run.x_read && !run.x_resting, "{run:?}");
    let after = run.after.as_ref().unwrap();
    assert!(
        after.error.is_some() && after.trades_naming_x == 0,
        "{after:?}"
    );
    assert!(!after.trades_read, "{after:?}");
    // FBC-77r8: X's trades or historic orders not read whole after drop session one stop the
    // run LeftResting before drop session two, X left unjudged.
    assert_stopped(&d, places::Stop::LeftResting);
    assert_eq!(run.close_cancelled_x, None, "{run:?}");
    assert_not_measured(&d, places::CloseUnmeasured::TradesUnread);
    let (report, json) = d.outputs();
    assert_eq!(
        json["drop_tests"][0]["session_one"]["after"]["trades_read"],
        false
    );
    assert!(
        report.contains("0 trade rows since X's send, not read whole, 0 naming X"),
        "{report}"
    );
}

#[tokio::test]
async fn x_part_filled_after_the_close_then_gone_is_not_counted_as_cancelled_by_it() {
    // A sibling of PR #272's thread 4608720833. Half of X fills once the model has settled the
    // graceful close, and the close then cancels the rest: the read after it no longer lists X,
    // and a get_user_trades row names it. Decision 0132 item 4 counts X cancelled only with no
    // row naming it, so a part fill leaves the close not measured too, and stops the run not
    // flat (0151 item 9).
    let model = QfexModel {
        part_fill_after_close: true,
        cod_on_close: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-part-fill-after-close").await;
    let run = d.drop_run();
    assert!(run.x.rested_at_close && run.cancels.is_none(), "{run:?}");
    assert!(run.x_read && !run.x_resting, "{run:?}");
    let after = run.after.as_ref().unwrap();
    assert_eq!((after.trades_naming_x, after.error.as_ref()), (1, None));
    assert_eq!(after.terminal_status.as_deref(), Some("CANCELLED"));
    assert_eq!(run.close_cancelled_x, None, "{run:?}");
    assert_not_measured(&d, places::CloseUnmeasured::XTraded);
    assert_stopped(&d, places::Stop::NotFlat);
}

#[tokio::test]
async fn x_gone_with_its_trades_read_whole_and_naming_it_in_no_row_was_cancelled_by_the_close() {
    // PR #272's thread 4608720833, the measurement it keeps: X rested as the graceful close was
    // sent, the read after it no longer lists X, and every get_user_trades page since X's send
    // was read, none naming X, so the close cancelled X (decision 0132 item 4).
    let model = QfexModel {
        cod_on_close: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-close-trades-read").await;
    let run = d.drop_run();
    let after = run.after.as_ref().unwrap();
    assert_eq!((after.trades_naming_x, after.error.as_ref()), (0, None));
    assert!(after.trades_read, "{after:?}");
    assert_eq!(run.close_cancelled_x, Some(true), "{run:?}");
    assert_eq!(run.close_not_measured, None, "{run:?}");
    let (report, json) = d.outputs();
    assert_eq!(
        json["drop_tests"][0]["session_one"]["close_cancelled_x"],
        true
    );
    assert_eq!(
        json["drop_tests"][0]["session_one"]["close_not_measured"],
        Value::Null
    );
    assert_eq!(
        json["drop_tests"][0]["session_one"]["after"]["trades_read"],
        true
    );
    assert!(
        report.contains(
            "Whether the graceful close cancelled X: yes: X rested before it, no cancel of ours \
             was sent, and the read after it no longer listed X, with X's account flat after it \
             and X's trades since its send read whole, no row since then on the account \
             (decision 0132 item 4)"
        ),
        "{report}"
    );
}

#[tokio::test]
async fn x_still_listed_after_the_close_was_not_cancelled_by_it_whatever_its_trades_read() {
    // A sibling of PR #272's thread 4608720833. The read after the graceful close still lists
    // X: the close did not cancel it, whether or not its trades could be read.
    for (trades_err, what) in [(false, "drop-close-keeps-read"), (true, "drop-close-keeps")] {
        let model = QfexModel {
            trades_err,
            ..QfexModel::default()
        };
        let d = dropped(model, what).await;
        let run = d.drop_run();
        assert!(
            run.x.rested_at_close && run.x_read && run.x_resting,
            "{run:?}"
        );
        assert_eq!(run.after.as_ref().unwrap().error.is_some(), trades_err);
        assert_eq!(run.close_cancelled_x, Some(false), "{run:?}");
        assert_eq!(run.close_not_measured, None, "{run:?}");
        let (report, json) = d.outputs();
        assert_eq!(
            json["drop_tests"][0]["session_one"]["close_cancelled_x"],
            false
        );
        assert_eq!(
            json["drop_tests"][0]["session_one"]["close_not_measured"],
            Value::Null
        );
        assert!(
            report.contains(
                "Whether the graceful close cancelled X: no: X rested before it and still did \
                 after it"
            ),
            "{report}"
        );
        // FBC-77r8: with X's trades read whole, the drop test runs on and passes; with them
        // not read whole, the run stops LeftResting before drop session two.
        match trades_err {
            true => assert_stopped(&d, places::Stop::LeftResting),
            false => assert_eq!(d.ev.stop, None, "{}", d.printed),
        }
    }
}

#[tokio::test]
async fn x_gone_with_its_account_not_flat_is_not_counted_as_cancelled_by_the_close() {
    // A sibling of PR #272's thread 4608720833. The graceful close cancels X, and X's account
    // then shows a position no fill of ours made, with no trade row: something traded there,
    // which decision 0132 item 4 rules out, so the cause of X's absence is unknown and the
    // close is not measured; the position stops the run not flat.
    let model = QfexModel {
        cod_on_close: true,
        position_after_close: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-close-not-flat").await;
    let run = d.drop_run();
    assert!(run.x.rested_at_close && run.cancels.is_none(), "{run:?}");
    assert_eq!(run.ended_by, Some(stage::GRACEFUL));
    assert!(run.x_read && !run.x_resting, "{run:?}");
    let after = run.after.as_ref().unwrap();
    assert_eq!((after.trades, after.error.as_ref()), (0, None), "{after:?}");
    assert!(after.trades_read, "{after:?}");
    assert_eq!(run.close_cancelled_x, None, "{run:?}");
    assert_not_measured(&d, places::CloseUnmeasured::XNotFlat);
    assert_stopped(&d, places::Stop::NotFlat);
}

#[tokio::test]
async fn x_gone_after_a_trade_naming_another_order_or_none_is_not_counted_cancelled() {
    // A sibling of PR #272's thread 4608720833. The graceful close cancels X, and as it is
    // settled a trade of an order the model never had comes on X's account: its row names
    // another order, or none, which a row of X's own fill could be. Either way something traded
    // since X's send, so the close is not measured (decision 0132 item 4), and the position it
    // left stops the run not flat.
    for (stray, what) in [
        (model::StrayTrade::Other, "drop-close-other-trade"),
        (model::StrayTrade::Unnamed, "drop-close-unnamed-trade"),
    ] {
        let model = QfexModel {
            cod_on_close: true,
            trade_after_close: Some(stray),
            ..QfexModel::default()
        };
        let d = dropped(model, what).await;
        let run = d.drop_run();
        assert!(run.x.rested_at_close && run.cancels.is_none(), "{run:?}");
        assert!(run.x_read && !run.x_resting, "{run:?}");
        let after = run.after.as_ref().unwrap();
        assert_eq!((after.trades, after.trades_naming_x), (1, 0), "{after:?}");
        assert!(after.trades_read && after.error.is_none(), "{after:?}");
        assert_eq!(after.terminal_status.as_deref(), Some("CANCELLED"));
        assert_eq!(run.close_cancelled_x, None, "{run:?}");
        assert_not_measured(&d, places::CloseUnmeasured::OtherTrade);
        assert_stopped(&d, places::Stop::NotFlat);
    }
}

#[tokio::test]
async fn a_trade_of_another_order_after_drop_session_one_on_an_account_still_flat_places_no_y() {
    // FBC-77r8: as drop session one's graceful close is settled, two trades of an order the
    // model never had come on X's account, one each way, so the position does not move, and X
    // still rests. Their rows since X's send name another order, or none: something else traded
    // on the account, which decision 0132 item 4 rules out, so the drop test has failed already.
    // The run stops DropFailed before drop session two, which never runs, so Y is never placed,
    // and nothing is cancelled with no session open (0151 item 2); X is left for the owner's
    // hand cancel.
    for (stray, what) in [
        (model::StrayTrade::Other, "drop-close-netting-other"),
        (model::StrayTrade::Unnamed, "drop-close-netting-unnamed"),
    ] {
        let model = QfexModel {
            netting_after_close: Some(stray),
            ..QfexModel::default()
        };
        let d = dropped(model, what).await;
        assert_stopped(&d, places::Stop::DropFailed);
        let run = d.drop_run();
        assert!(run.x.rested_at_close && run.cancels.is_none(), "{run:?}");
        assert!(run.x_read && run.x_resting, "{run:?}");
        let after = run.after.as_ref().unwrap();
        assert_eq!((after.trades, after.trades_naming_x), (2, 0), "{after:?}");
        assert!(after.trades_read && after.error.is_none(), "{after:?}");
        assert_eq!(run.close_cancelled_x, Some(false), "{run:?}");
        assert!(run.two.is_none() && run.verdict.is_none(), "{run:?}");
        assert_eq!(d.count("add_order"), 1);
        assert_eq!(d.count("cancel_order"), 0);
        assert_eq!(d.session_conns().len(), 1);
        let said = d.ev.stopped.as_deref().unwrap();
        assert!(
            said.starts_with(
                "DropFailed after drop session one, a get_user_trades row since X's send naming \
                 another order or none (decision 0132 item 4)"
            ),
            "{said}"
        );
        assert!(
            d.printed
                .contains(&format!("RESTING {}", places::HAND_CANCEL))
        );
        let flat = (d.ev.reads.iter())
            .filter(|r| r.before == refusals::AFTER_DROP_ONE)
            .all(|r| r.flat);
        assert!(flat, "{:?}", d.ev.reads);
    }
}

#[tokio::test]
async fn a_close_whose_historic_orders_read_fails_still_counts_x_cancelled() {
    // A sibling of PR #272's thread 4608720833. The graceful close cancels X, X's trades since
    // its send are read whole with no row, and GET /user/historic-orders then fails HTTP 500.
    // 0132 item 4's two reads show the close cancelled X, and 0151 item 13's historic-orders
    // condition is the drop test's pass, not this measurement, so it is still counted; the
    // report says which read failed, and that X's status there is not known.
    let model = QfexModel {
        cod_on_close: true,
        historic_fails: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-close-historic-fails").await;
    let run = d.drop_run();
    let after = run.after.as_ref().unwrap();
    assert!(after.trades_read && after.trades == 0, "{after:?}");
    assert_eq!(after.terminal_status, None, "{after:?}");
    let error = after.error.as_deref().unwrap_or_default();
    assert!(
        error.starts_with("GET /user/historic-orders: "),
        "{after:?}"
    );
    assert_eq!(run.close_cancelled_x, Some(true), "{run:?}");
    assert_eq!(run.close_not_measured, None, "{run:?}");
    // FBC-77r8: X's trades or historic orders not read whole after drop session one stop the
    // run LeftResting before drop session two, X left unjudged.
    assert_stopped(&d, places::Stop::LeftResting);
    let (report, json) = d.outputs();
    assert_eq!(
        json["drop_tests"][0]["session_one"]["close_cancelled_x"],
        true
    );
    assert_eq!(
        json["drop_tests"][0]["session_one"]["after"]["trades_read"],
        true
    );
    let said = format!(
        "0 trade rows since X's send, read whole, 0 naming X; X on GET /user/historic-orders: \
         not known; a read failed: {error}"
    );
    assert!(report.contains(&said), "{report}");
    assert!(!report.contains("not read whole"), "{report}");
}

#[tokio::test]
async fn x_gone_with_a_rate_refusal_on_its_trades_is_not_measured_and_stops_the_run() {
    // A sibling of PR #272's thread 4608720833. The graceful close cancels X, and X's first
    // get_user_trades page is answered a RateLimited err: the trades are not read, so the close
    // is not measured, and the refusal is the run's stop, marked (decision 0148 item 8).
    let model = QfexModel {
        cod_on_close: true,
        rate_limit: Some(model::Limited::Trades),
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-close-trades-refused").await;
    let run = d.drop_run();
    assert!(run.x_read && !run.x_resting, "{run:?}");
    let after = run.after.as_ref().unwrap();
    assert!(!after.trades_read && after.error.is_some(), "{after:?}");
    assert_eq!(run.close_cancelled_x, None, "{run:?}");
    assert_not_measured(&d, places::CloseUnmeasured::TradesUnread);
    let on = places::RefusedOn::ReadSocket;
    assert_stopped(&d, places::Stop::RateRefused { on });
    let said = d.ev.stopped.as_deref().unwrap();
    assert!(said.contains("reading X after drop session one"), "{said}");
}

#[tokio::test]
async fn a_connection_that_ends_first_is_not_measured_as_a_close_sent() {
    // A sibling of PR #272's thread 4608720833. The server closes the session's connection as
    // it reads X, which rests: no close was sent, so the reason is the connection's end, not
    // that X did not rest.
    let model = QfexModel {
        close_on_place: true,
        ..QfexModel::default()
    };
    let d = dropped_with(model, "drop-ended-reason", patient_rpc).await;
    let run = d.drop_run();
    assert_eq!(run.ended_by, Some(stage::CONNECTION_ENDED), "{run:?}");
    assert_eq!(run.close_cancelled_x, None, "{run:?}");
    assert_not_measured(&d, places::CloseUnmeasured::NotClosed);
    // And one that ends as a stop's cancels wait, held: still the connection's end.
    let model = QfexModel {
        close_on_cancel: true,
        ..QfexModel::default()
    };
    let d = pressed_drop(
        model,
        "drop-ended-in-cancels-reason",
        patient_rpc,
        &[" add_order"],
    )
    .await;
    let run = d.drop_run();
    assert_eq!(run.ended_by, Some(stage::CONNECTION_ENDED), "{run:?}");
    assert!(run.cancels.as_ref().is_some_and(|c| c.held), "{run:?}");
    assert_not_measured(&d, places::CloseUnmeasured::NotClosed);
}

#[tokio::test]
async fn held_stop_cancels_before_a_graceful_close_are_the_reason_it_is_not_measured() {
    // A sibling of PR #272's thread 4608720833. The owner's Ctrl-C, whose cancel of X QFEX
    // answers with a RateLimited err: the cancels are held and the session ends by a graceful
    // close. A stop that cancels came, though none was seen to end X, so that is the reason.
    let model = QfexModel {
        rate_limit: Some(model::Limited::Cancel),
        ..QfexModel::default()
    };
    let d = pressed_drop(model, "drop-held-reason", |a| a, &[" add_order"]).await;
    let run = d.drop_run();
    assert_eq!(run.ended_by, Some(stage::GRACEFUL));
    assert!(run.cancels.as_ref().is_some_and(|c| c.held), "{run:?}");
    assert_eq!(run.close_cancelled_x, None, "{run:?}");
    assert_not_measured(&d, places::CloseUnmeasured::CancelStop);
    // Said as a stop that came, not as a cancel sent, which none may have been.
    let (report, _) = d.outputs();
    let said = "not measured: a stop that cancels came before the close (0151 item 2), its \
                cancels sent, held or refused";
    assert!(report.contains(said), "{report}");
}

#[tokio::test]
async fn a_close_with_no_room_is_not_measured_for_its_room() {
    // A sibling of PR #272's thread 4608720833, on the run of
    // a_close_with_no_room_in_the_shared_budget_is_not_counted_graceful: the close was sent
    // with no room, so it may not have been graceful, and that is the reason given, not the
    // connection's end, which did not come first.
    let rate_user = user();
    let limiter = shared_limiter(rate_user);
    let model = QfexModel {
        hold_on_place: Some((limiter, Duration::from_secs(3600))),
        ..QfexModel::default()
    };
    let (dir, removed) = temp_dir("drop-no-room-reason");
    let venue = Venue::start(model).await;
    let asked = set(drop_argv(&dir, &venue), "--agree-wait-ms", "1000");
    let (mut opts, places) = parsed(&asked).unwrap();
    opts.rate_user = rate_user;
    let env = environment();
    let printed = venue.printed();
    let lookup = |var: &str| env.get(var).cloned();
    let out: stage::Out = Rc::new(RefCell::new(printed.clone()));
    let ran = stage::run_places(&opts, &places, &lookup, out).await;
    let printed = printed.text();
    let ev = stopped(ran, &printed);
    let d = Dropped {
        dir,
        _removed: removed,
        venue,
        ev,
        printed,
    };
    let run = d.drop_run();
    assert_eq!(run.ended_by, Some(stage::NO_ROOM_TO_CLOSE), "{}", d.printed);
    assert!(run.x.rested_at_close, "{run:?}");
    assert_not_measured(&d, places::CloseUnmeasured::NoRoomToClose);
}

/// Asserts `d`'s graceful close was not measured as cancelling X for `why`, as its report and
/// its evidence say it.
fn assert_not_measured(d: &Dropped, why: places::CloseUnmeasured) {
    assert_eq!(d.drop_run().close_not_measured, Some(why));
    let (report, json) = d.outputs();
    assert_eq!(
        json["drop_tests"][0]["session_one"]["close_cancelled_x"],
        Value::Null
    );
    assert_eq!(
        json["drop_tests"][0]["session_one"]["close_not_measured"],
        why.says()
    );
    let said = format!(
        "Whether the graceful close cancelled X: not measured: {}",
        why.says()
    );
    assert!(report.contains(&said), "{report}");
}

#[test]
fn a_close_is_judged_cancelling_x_only_on_xs_absence_and_its_trades_read_whole() {
    // PR #272's thread 4608720833, each of close_verdict's reasons on a run built by hand.
    use places::{CloseUnmeasured as U, XAccountRead, close_verdict};
    let rested = places::DropRun {
        x: places::XRun {
            rested_at_close: true,
            ..places::XRun::default()
        },
        ended_by: Some(stage::GRACEFUL),
        after: Some(places::XAfter {
            trades_read: true,
            ..places::XAfter::default()
        }),
        ..places::DropRun::default()
    };
    let gone = XAccountRead {
        read: true,
        listed: false,
        flat: true,
    };
    let listed = XAccountRead {
        listed: true,
        ..gone
    };
    let moved = XAccountRead {
        flat: false,
        ..gone
    };
    assert_eq!(close_verdict(&rested, gone), Ok(true));
    assert_eq!(close_verdict(&rested, listed), Ok(false));
    let unread = XAccountRead {
        read: false,
        ..gone
    };
    assert_eq!(close_verdict(&rested, unread), Err(U::XUnread));
    // How the session ended comes first: no close sent, a close with no room, or none reached,
    // whether or not X rested and a stop that cancels came.
    for (ended, why) in [
        (Some(stage::CONNECTION_ENDED), U::NotClosed),
        (Some(stage::NO_ROOM_TO_CLOSE), U::NoRoomToClose),
        (None, U::NoClose),
    ] {
        let mut run = rested.clone();
        run.ended_by = ended;
        assert_eq!(close_verdict(&run, gone), Err(why));
        run.x.rested_at_close = false;
        run.cancels = Some(places::Cancels::default());
        assert_eq!(close_verdict(&run, gone), Err(why));
    }
    // A stop that cancels, its cancels held and none sent included, then X not resting.
    let mut run = rested.clone();
    run.cancels = Some(places::Cancels {
        sent: 0,
        held: true,
        ..places::Cancels::default()
    });
    assert_eq!(close_verdict(&run, gone), Err(U::CancelStop));
    run.x.rested_at_close = false;
    assert_eq!(close_verdict(&run, gone), Err(U::CancelStop));
    run.cancels = None;
    assert_eq!(close_verdict(&run, gone), Err(U::NotRested));
    // X absent: its trades not read whole, or not read at all, a row naming X, even one read
    // before the read failed, another row since its send, or its account not flat, leave the
    // close not measured; X listed was not cancelled by it, whatever its trades and position.
    let mut run = rested.clone();
    run.after.as_mut().unwrap().trades_read = false;
    assert_eq!(close_verdict(&run, gone), Err(U::TradesUnread));
    assert_eq!(close_verdict(&run, listed), Ok(false));
    run.after = None;
    assert_eq!(close_verdict(&run, gone), Err(U::TradesUnread));
    assert_eq!(close_verdict(&run, moved), Err(U::XNotFlat));
    let mut run = rested.clone();
    let after = run.after.as_mut().unwrap();
    (after.trades, after.trades_naming_x) = (1, 1);
    assert_eq!(close_verdict(&run, gone), Err(U::XTraded));
    assert_eq!(close_verdict(&run, moved), Err(U::XTraded));
    assert_eq!(close_verdict(&run, listed), Ok(false));
    run.after.as_mut().unwrap().trades_read = false;
    assert_eq!(close_verdict(&run, gone), Err(U::XTraded));
    // A row naming another order or none, whether or not the trades were read whole.
    let mut run = rested.clone();
    run.after.as_mut().unwrap().trades = 1;
    assert_eq!(close_verdict(&run, gone), Err(U::OtherTrade));
    assert_eq!(close_verdict(&run, moved), Err(U::OtherTrade));
    assert_eq!(close_verdict(&run, listed), Ok(false));
    run.after.as_mut().unwrap().trades_read = false;
    assert_eq!(close_verdict(&run, gone), Err(U::OtherTrade));
    // X's account not flat with no row since X's send: a trade the rows do not show.
    assert_eq!(close_verdict(&rested, moved), Err(U::XNotFlat));
    let mut run = rested.clone();
    run.after.as_mut().unwrap().trades_read = false;
    assert_eq!(close_verdict(&run, moved), Err(U::XNotFlat));
    assert_eq!(
        close_verdict(
            &rested,
            XAccountRead {
                listed: true,
                ..moved
            }
        ),
        Ok(false)
    );
}

/// Asserts `d` stopped on a read socket, met `when`, as [`assert_stopped`] asserts a stop but
/// for the reads after the session, which the caller asserts, its socket labelled `label`
/// showing one refusal in a frame kept whole and none in clear, and no cancel sent.
fn assert_refused_whole(d: &Dropped, when: &str, label: &str) {
    let stop = places::Stop::RateRefused {
        on: places::RefusedOn::ReadSocket,
    };
    let name = stop.name();
    assert_eq!(d.ev.stop, Some(name), "{}", d.printed);
    let marker = d.marker().unwrap_or_default();
    assert!(marker.starts_with(&format!("stop: {name}\n")), "{marker}");
    let (report, json) = d.outputs();
    assert!(
        report.contains(&format!("**The place stages' stop:** {name}.")),
        "{report}"
    );
    assert_eq!(json["stop"], name);
    assert_report_and_evidence_pass_the_privacy_check(&d.dir.join("evidence"));
    assert_no_secret(&d.dir.join("evidence"));
    d.venue.assert_served();
    let said = d.ev.stopped.as_deref().unwrap();
    assert!(
        said.starts_with(&format!("{} {when}", stop.name())),
        "{said}"
    );
    // The last socket so labelled: the primary's before the session is labelled alike.
    let socket = (d.ev.read_sockets.iter()).rev().find(|r| r.label == label);
    let refusals = socket.unwrap_or_else(|| panic!("{label}")).refusals;
    let shown = (refusals.errs, refusals.unread, refusals.whole);
    assert_eq!(shown, (0, 1, 1), "{refusals:?}");
    assert_eq!(d.count("cancel_order"), 0);
}

#[tokio::test]
async fn a_refusal_kept_whole_on_the_comparison_socket_stops_the_run() {
    // Thread 4605918671. The comparison's authentication after X is answered a RateLimited err
    // echoing it, which the journal keeps whole (decision 0134 item 2), so the adapter reads no
    // hold from it: the refusal its frame shows is a stop on the read socket all the same
    // (decision 0135 item 2, 0151 item 4), the comparison reconnecting for no second read.
    let model = QfexModel {
        rate_limit: Some(model::Limited::ReadAuthEcho(0)),
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-rate-whole").await;
    let compared = &d.drop_run().compared[0];
    assert_eq!(compared.ended, compare::Ended::Refused, "{compared:?}");
    assert_eq!(compared.reads.len(), 1);
    let label = "primary (the comparison after X's place)";
    assert_refused_whole(&d, "in drop session one", label);
    assert_after_reads(&d);
    assert_no_cancel_and_x_resting(&d);
}

#[tokio::test]
async fn a_refusal_kept_whole_in_the_reads_after_the_session_stops_the_run() {
    // The twin on the after-session reads: the primary's read socket, the second to
    // authenticate after X (the comparison's first), is answered that err.
    let model = QfexModel {
        rate_limit: Some(model::Limited::ReadAuthEcho(1)),
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-rate-whole-after").await;
    let when = "in the reads after drop session one";
    assert_refused_whole(&d, when, "primary (drop)");
    // The primary's read failed on the refusal; the subaccount's was still made.
    let after: Vec<&str> = (d.ev.reads.iter())
        .filter(|r| r.before == refusals::AFTER_DROP_ONE)
        .map(|r| r.label.as_str())
        .collect();
    assert_eq!(after, ["subaccount"], "{}", d.printed);
}

#[tokio::test]
async fn a_refusal_kept_whole_on_xs_read_socket_stops_the_run() {
    // The twin on X's read after the session: its socket, the fourth to authenticate after X
    // (the comparison's, then the primary's and the subaccount's reads), is answered that err.
    let model = QfexModel {
        rate_limit: Some(model::Limited::ReadAuthEcho(3)),
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-rate-whole-x").await;
    let when = "reading X after drop session one";
    assert_refused_whole(&d, when, "primary (X and its trades)");
    assert_after_reads(&d);
}

#[tokio::test]
async fn a_fill_of_x_seen_only_by_its_trade_read_stops_the_run_not_flat() {
    // Thread 4605918676. X fills as its trades are read after the session, after the reads
    // that showed every account flat: a fill seen with no session open, so the run stops not
    // flat, marked, the position the owner's (0151 item 9), and sends no cancel.
    let model = QfexModel {
        fill_on_trades: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-fill-unseen").await;
    let after = d.drop_run().after.as_ref().unwrap();
    assert_eq!(after.trades_naming_x, 1, "{after:?}");
    assert!(
        (d.ev.reads.iter())
            .filter(|r| r.before == refusals::AFTER_DROP_ONE)
            .all(|r| r.flat)
    );
    assert_stopped(&d, places::Stop::NotFlat);
    let said = d.ev.stopped.as_deref().unwrap();
    assert!(said.starts_with("NotFlat after drop session one"), "{said}");
    // What it prints says the cause it met: a fill seen with no session open.
    assert!(
        said.contains("a fill of ours seen after it with no session open"),
        "{said}"
    );
    assert_eq!(d.count("cancel_order"), 0);
}

#[tokio::test]
async fn a_touch_that_moves_past_the_caps_after_the_preflight_sends_no_x() {
    // Thread 4605918689. Both caps admit exactly three minimum sizes at the preflight's touch
    // near 100; by X's place the touch is near 400, where they admit none, while the registry's
    // lot caps, fixed at the preflight, still admit three: X is not sent, and the run stops,
    // marked, its session ended by a graceful close with no cancel (decision 0113).
    let moved = model::Touch {
        bid: Some(("400.00", "1.00")),
        ask: Some(("400.10", "2.00")),
        age_secs: 0,
    };
    let model = QfexModel {
        touch_moves: Some(moved),
        ..QfexModel::default()
    };
    let caps = |argv| {
        let argv = set(argv, "--resting-cap-usd", THREE_MIN_USD);
        set(argv, "--inventory-cap-usd", THREE_MIN_USD)
    };
    let d = dropped_with(model, "drop-caps-moved", caps).await;
    assert_eq!(d.count("add_order"), 0, "{:?}", d.what());
    let run = d.drop_run();
    assert!(run.started && !run.x.submitted, "{run:?}");
    assert!(!d.printed.contains("PLACE X"), "{}", d.printed);
    assert_stopped(&d, places::Stop::BeyondCaps);
    assert_eq!(run.ended_by, Some(stage::GRACEFUL));
    assert_eq!(run.cancels, None);
}

#[test]
fn the_caps_are_asked_again_at_the_touch_a_place_is_priced_at() {
    // Thread 4605918689's arithmetic (decision 0113): at the model's touch, caps of three
    // minimum sizes are three lots, which admit those lots and X at that touch and at one
    // below it, where a lot costs less, but not at a touch where three minimum sizes cost more.
    let found = spec_of(Listed::trading());
    let argv = set(parse_only(), "--resting-cap-usd", THREE_MIN_USD);
    let (_, places) = parsed(&set(argv, "--inventory-cap-usd", THREE_MIN_USD)).unwrap();
    let caps = market::place_caps(&places, &found, QUOTE).unwrap();
    let x = found.min_size;
    assert!(market::caps_admit(&places, &found, QUOTE, caps, x));
    let lower = Quote {
        bid: Ticks(QUOTE.bid.0 / 2),
        ask: Ticks(QUOTE.ask.0 / 2),
    };
    assert!(market::caps_admit(&places, &found, lower, caps, x));
    let risen = Quote {
        bid: Ticks(QUOTE.bid.0 * 4),
        ask: Ticks(QUOTE.ask.0 * 4),
    };
    assert!(!market::caps_admit(&places, &found, risen, caps, x));
    // A touch a tick above the preflight's admits fewer lots than the registry's three: the
    // registry would admit more than the USD caps allow there, so nothing goes out.
    let tick = Quote {
        bid: Ticks(QUOTE.bid.0 + 1),
        ask: Ticks(QUOTE.ask.0 + 1),
    };
    assert!(!market::caps_admit(&places, &found, tick, caps, x));
    // With the caps unequal, each comparison alone refuses that touch: the tight cap admits
    // fewer lots there than the registry's three, the other cap admitting them all.
    let three = Lots::new(x.get() * 3).unwrap();
    let lots = market::PlaceCaps {
        resting: three,
        inventory: three,
    };
    for (resting, inventory) in [("1000", THREE_MIN_USD), (THREE_MIN_USD, "1000")] {
        let argv = set(parse_only(), "--resting-cap-usd", resting);
        let (_, places) = parsed(&set(argv, "--inventory-cap-usd", inventory)).unwrap();
        assert!(market::caps_admit(&places, &found, QUOTE, lots, x));
        let admitted = market::caps_admit(&places, &found, tick, lots, x);
        assert!(
            !admitted,
            "--resting-cap-usd {resting}, --inventory-cap-usd {inventory}"
        );
    }
    // And an order above what the caps admit at the touch is refused whatever the registry's
    // lot caps.
    let small = market::PlaceCaps {
        resting: x,
        inventory: x,
    };
    let two = Lots::new(x.get() * 2).unwrap();
    assert!(!market::caps_admit(
        &places,
        &found,
        QUOTE,
        small,
        Lots::new(4).unwrap()
    ));
    assert!(market::caps_admit(&places, &found, QUOTE, small, two));
}

/// The provider rate refusals [`Limited`](model::Limited) answers in the reads before any place,
/// each with the stop it is: one in clear, one the journal keeps whole, and a REST 429.
fn refused_before() -> [(model::Limited, places::Stop); 3] {
    let on = |on| places::Stop::RateRefused { on };
    [
        (
            model::Limited::ReadSocketBefore,
            on(places::RefusedOn::ReadSocket),
        ),
        (
            model::Limited::PageKeptWholeBefore,
            on(places::RefusedOn::ReadSocket),
        ),
        (model::Limited::RestBefore, on(places::RefusedOn::Rest)),
    ]
}

#[tokio::test]
async fn a_rate_refusal_in_the_reads_before_drop_session_one_stops_the_run_marked() {
    // PR #272's thread 4606842299. The primary's read before drop session one is answered a
    // RateLimited err on its page, or HTTP 429 on its GET /user/positions: a provider rate
    // refusal, so a stop (decision 0148 item 8), marked, so the next run needs --reconciled
    // (0151 item 6). Nothing is placed.
    for (limited, stop) in refused_before() {
        let (dir, _removed) = temp_dir("place-rate-before");
        let venue = Venue::start(QfexModel {
            rate_limit: Some(limited),
            ..QfexModel::default()
        })
        .await;
        let (ran, printed) = place_run(&venue, &drop_argv(&dir, &venue), &environment()).await;
        let ev = stopped(ran, &printed);
        let name = stop.name();
        assert_eq!(ev.stop, Some(name), "{printed}");
        let said = ev.stopped.as_deref().unwrap();
        assert!(
            said.starts_with(&format!("{name} in the reads before drop session one")),
            "{said}"
        );
        let marker = fs::read_to_string(leases_of(&dir).join(places::MARKER)).unwrap_or_default();
        assert!(marker.starts_with(&format!("stop: {name}\n")), "{marker}");
        assert!(ev.drops.is_empty());
        assert!(!venue.log().iter().any(|e| e.what.ends_with(" add_order")));
        venue.assert_served();
    }
}

#[tokio::test]
async fn a_rate_refusal_in_the_reads_that_clear_a_marker_stops_the_run_and_keeps_it() {
    // The twin on the reconciliation's reads (0151 item 6): the refusal is this run's stop,
    // marked before the earlier one, which stays unreconciled.
    for (limited, stop) in refused_before() {
        let (dir, _removed) = temp_dir("place-rate-reconcile");
        let leases = leases_of(&dir);
        places::tag(&leases).unwrap();
        places::write_marker(&leases, places::Stop::LeftResting, &[], "synthetic").unwrap();
        let venue = Venue::start(QfexModel {
            rate_limit: Some(limited),
            ..QfexModel::default()
        })
        .await;
        let asked = set(drop_argv(&dir, &venue), "--reconciled", "hand-cancel");
        let (ran, printed) = place_run(&venue, &asked, &environment()).await;
        let ev = stopped(ran, &printed);
        let name = stop.name();
        assert_eq!(ev.stop, Some(name), "{printed}");
        let said = ev.stopped.as_deref().unwrap();
        assert!(
            said.starts_with(&format!("{name} in the reads that clear the marker")),
            "{said}"
        );
        let marker = fs::read_to_string(leases.join(places::MARKER)).unwrap();
        assert!(
            marker.starts_with(&format!("stop: {name}\nstop: LeftResting\n")),
            "{marker}"
        );
        let run = ev.marker.unwrap();
        assert!(!run.cleared);
        assert!(!venue.log().iter().any(|e| e.what.ends_with(" add_order")));
        venue.assert_served();
    }
}

#[tokio::test]
async fn x_listed_by_its_venue_id_alone_after_the_session_is_still_resting() {
    // PR #272's thread 4606842313. After drop session one, the primary's page lists X by its
    // venue id with an empty client id: X is found there by the venue id the streams gave it,
    // so it is still resting, reported for the owner's hand cancel, and the graceful close is
    // not counted as cancelling it. The read before drop session two still admits X only by
    // both ids (`AllowedOrder::matches`), so it refuses that row and the run stops there,
    // ReadDisagrees, marked, with no drop session two (FBC-77r8).
    let model = QfexModel {
        blank_cid_after_session: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-x-venue-id").await;
    let run = d.drop_run();
    assert!(run.x.rested_at_close && run.x_read, "{run:?}");
    assert_eq!(run.close_cancelled_x, Some(false), "{run:?}");
    assert!(run.two.is_none(), "{run:?}");
    assert_stopped(&d, places::Stop::ReadDisagrees);
    let stopped = d.ev.stopped.as_deref().unwrap_or_default();
    assert!(
        stopped.contains("ReadDisagrees in the read before drop session two"),
        "{stopped}"
    );
    assert_no_cancel_and_x_resting(&d);
    let model = d.venue.model.lock().unwrap();
    assert_eq!(model.orders[0].state, model::State::Open);
}

#[tokio::test]
async fn an_account_reads_socket_sends_its_close_only_with_room_for_it() {
    // A sibling of PR #272's thread 4606842296: the primary's read before drop session one is
    // answered a RateLimited err, which holds the shared budget (decision 0126), so its socket
    // has no room for its close and is dropped with no close frame (decisions 0030, 0135). With
    // room, every read socket of a run closes with one.
    let (dir, _removed) = temp_dir("place-read-close");
    let venue = Venue::start(QfexModel {
        rate_limit: Some(model::Limited::ReadSocketBefore),
        ..QfexModel::default()
    })
    .await;
    let (ran, printed) = place_run(&venue, &drop_argv(&dir, &venue), &environment()).await;
    stopped(ran, &printed);
    let ended = || venue.trade.connections().iter().all(|c| !c.open);
    until("the read socket's end", ended).await;
    let conns = venue.trade.connections();
    assert_eq!(conns.len(), 1, "{printed}");
    assert!(!conns[0].client_closed, "a close sent under the hold");
    let d = dropped(QfexModel::default(), "drop-read-close").await;
    // The run's last read socket may still be closing as the run ends: its end is waited for.
    let ended = || d.venue.trade.connections().iter().all(|c| !c.open);
    until("every read socket's end", ended).await;
    let conns = d.venue.trade.connections();
    let session = (d.venue.model.lock().unwrap().conns.iter())
        .filter(|(_, c)| c.session)
        .map(|(n, _)| *n)
        .collect::<Vec<usize>>();
    let reads = (0..conns.len()).filter(|n| !session.contains(n));
    assert!(reads.clone().count() >= 4, "{conns:?}");
    for n in reads {
        assert!(
            conns[n].client_closed,
            "read socket {n} closed with no close frame"
        );
    }
}

#[tokio::test]
async fn x_absent_from_a_full_page_after_the_session_is_not_measured() {
    // A sibling of PR #272's thread 4606842313: the after-session read asks one page, of one
    // order here, and the account holds another order, listed first. The full page leaves X
    // unread, not absent, so whether the graceful close cancelled it is not measured.
    let model = QfexModel {
        foreign_order: true,
        newest_first: true,
        ..QfexModel::default()
    };
    let paged = |argv| set(short_wait(argv), "--page-size", "1");
    let d = dropped_with(model, "drop-x-full-page", paged).await;
    let after = (d.ev.reads.iter())
        .find(|r| r.before == refusals::AFTER_DROP_ONE && r.label == "primary (drop)")
        .unwrap();
    assert!(after.page_full && !after.page_lists_it, "{after:?}");
    let run = d.drop_run();
    assert!(run.x.rested_at_close && run.cancels.is_none(), "{run:?}");
    assert!(!run.x_read, "{run:?}");
    assert_eq!(run.close_cancelled_x, None, "{run:?}");
    assert_eq!(d.ev.stop, Some(places::Stop::ReadDisagrees.name()));
}

/// [`dropped`], with the test hooks `hooked` sets on the run's parsed command line.
async fn dropped_hooked(
    model: QfexModel,
    what: &str,
    hooked: impl FnOnce(&mut probe_args::TestHooks),
) -> Dropped {
    let (dir, removed) = temp_dir(what);
    let venue = Venue::start(model).await;
    let (mut opts, mut places) = parsed(&drop_argv(&dir, &venue)).unwrap();
    hooked(&mut places.test);
    opts.rate_user = user();
    let env = environment();
    let printed = venue.printed();
    let lookup = |var: &str| env.get(var).cloned();
    let out: stage::Out = Rc::new(RefCell::new(printed.clone()));
    let ran = stage::run_places(&opts, &places, &lookup, out).await;
    let printed = printed.text();
    let ev = stopped(ran, &printed);
    Dropped {
        dir,
        _removed: removed,
        venue,
        ev,
        printed,
    }
}

#[tokio::test]
async fn a_stop_after_a_session_that_failed_with_no_stop_is_raised_and_marked() {
    // The second review of PR #272's thread 4606842299: drop session one fails with no stop, X
    // not priced once the market armed, and the reads after it meet a provider rate refusal or
    // show a position not flat. The stop is raised and its marker written, the session's
    // failure kept as its error (decision 0148 item 8, 0151 item 6).
    // The model answers a page's RateLimited err only after an add_order, which this run never
    // sends, so the refusal is REST's.
    let refused = |limited, on| {
        let model = QfexModel {
            rate_limit: Some(limited),
            ..QfexModel::default()
        };
        (model, places::Stop::RateRefused { on })
    };
    let not_flat = QfexModel {
        position_after_close: true,
        ..QfexModel::default()
    };
    let cases = [
        refused(model::Limited::RestAfterSession, places::RefusedOn::Rest),
        (not_flat, places::Stop::NotFlat),
    ];
    for (model, stop) in cases {
        let d = dropped_hooked(model, "drop-failed-then-stop", |t| t.x_unpriced = true).await;
        let name = stop.name();
        assert_eq!(d.ev.stop, Some(name), "{}", d.printed);
        let marker = d.marker().unwrap_or_default();
        assert!(marker.starts_with(&format!("stop: {name}\n")), "{marker}");
        let stopped = d.ev.stopped.as_deref().unwrap_or_default();
        let when = match stop {
            places::Stop::NotFlat => "after drop session one",
            _ => "in the reads after drop session one",
        };
        assert!(stopped.starts_with(&format!("{name} {when}")), "{stopped}");
        let run = d.drop_run();
        assert_eq!(run.error.as_deref(), Some("a test's X could not be priced"));
        assert!(run.started && !run.x.submitted, "{run:?}");
        assert_eq!(run.ended_by, Some(stage::GRACEFUL));
        assert_eq!(d.count("add_order"), 0, "{:?}", d.what());
        d.venue.assert_served();
    }
}

#[tokio::test]
async fn a_session_that_failed_with_no_stop_and_clean_reads_after_it_ends_on_its_failure() {
    // A sibling of the test above: with clean reads after it, the run ends on the session's
    // failure, with no stop and no marker.
    let d = dropped_hooked(QfexModel::default(), "drop-failed-clean", |t| {
        t.x_unpriced = true;
    })
    .await;
    assert_eq!(d.ev.stop, None, "{}", d.printed);
    assert!(d.marker().is_none());
    assert_eq!(
        d.ev.stopped.as_deref(),
        Some("a test's X could not be priced")
    );
    assert_after_reads(&d);
}

#[tokio::test]
async fn a_full_page_repeating_the_one_before_fails_the_read_of_x_rather_than_paging_on() {
    // The second review of PR #272's thread 4606842313: on pages of one, a venue answering every
    // get_user_trades page, or every GET /user/historic-orders page, as if asked from offset 0
    // would be paged without end, for as long as the limiter had room or, for historic orders,
    // which QFEX declares no bucket for, for ever. The read of X fails at the first repeat.
    let historic = QfexModel {
        cod_on_close: true,
        ..QfexModel::default()
    };
    let trades = QfexModel {
        maker_fill: true,
        stuck_trades: true,
        ..QfexModel::default()
    };
    let cases = [
        (historic, "GET /user/historic-orders"),
        (trades, "get_user_trades"),
    ];
    for (model, what) in cases {
        let run = dropped_with(model, "drop-repeated-page", |argv| {
            set(argv, "--page-size", "1")
        });
        let d = tokio::time::timeout(Duration::from_secs(60), run)
            .await
            .expect("the run ended");
        let after = d.drop_run().after.as_ref().expect("X was read");
        let why = format!("a full {what} page repeated the page before it");
        assert_eq!(after.error.as_deref(), Some(why.as_str()), "{after:?}");
        d.venue.assert_served();
    }
}

// ----- FBC-77r8: the drop test's second session's parts -----

#[test]
fn an_abrupt_drop_with_no_closed_control_ends_the_capture_at_the_sessions_end() {
    let (dir, _removed) = temp_dir("abrupt-drop-capture");
    let ask = r#"{"type":"get_user_orders","params":{"limit":1000,"offset":0}}"#;
    let asked = |rpc: u64| {
        Offer::Keep(Record::Outbound {
            at: MonoNs(0),
            conn: conn(0),
            rpc: Some(RpcId(rpc)),
            opcode: Opcode::Text,
            frame: WireSlice::plain(ask.as_bytes().to_vec()),
        })
    };
    let paged = || {
        let page = r#"{"all_orders_response":{"orders":[],"twaps":[]}}"#;
        Offer::Keep(Record::inbound(stamp(0), RawFrame::Text(page)))
    };
    // The first session dropped abruptly: it armed, its arm acked, one page answered and a
    // second asked, and no Closed control and no deadline of the arm journaled after them.
    let mut offers = vec![
        Offer::Keep(Record::Control {
            at: MonoNs(0),
            ev: ControlEvent::Opened(conn(0)),
        }),
        Offer::Keep(Record::inbound(stamp(0), RawFrame::Text(AUTH_OK))),
        Offer::Keep(Record::Outbound {
            at: MonoNs(0),
            conn: conn(0),
            rpc: Some(RpcId(1)),
            opcode: Opcode::Text,
            frame: WireSlice::plain(ARM.as_bytes().to_vec()),
        }),
        ack(0),
        asked(2),
        paged(),
        asked(3),
    ];
    // The next session, whole; then a record of the first that the journal holds after the
    // next one began (none does in a run: it stands for anything after the first one's end).
    offers.extend(session_records(1, vec![ack(1)]));
    offers.push(paged());
    let sink = journal(&dir, offers);
    let (ev, files) = sliced(&dir, &sink, Ok(()));
    let files = files.unwrap();
    // The first session's end is taken as its close: its window ends there, and the page it
    // waited for is answered by nothing after it.
    let first = &ev.sessions[0];
    let whens: Vec<When> = first.frames.iter().map(|f| f.when).collect();
    let want = [
        When::BeforeArm,
        When::InWindow,
        When::InWindow,
        When::AfterWindow,
    ];
    assert_eq!(whens, want);
    assert_eq!((first.pages.asked, first.pages.read), (2, 1));
    // A Closed control missing is no gap: both captures are whole, every frame placed.
    assert!(ev.sessions.iter().all(|s| s.partial.is_empty()), "{ev:?}");
    assert!(first.arm_sent);
    let names: Vec<&str> = files.iter().map(|(name, _)| name.as_str()).collect();
    assert!(
        names.contains(&"primary-004-after-window.json"),
        "{names:?}"
    );
    let second = &ev.sessions[1];
    let whens: Vec<When> = second.frames.iter().map(|f| f.when).collect();
    assert_eq!(whens, [When::BeforeArm, When::InWindow]);
}

/// The reads of `d` before drop session two, by label.
fn reads_before_two(d: &Dropped) -> Vec<&capture::AccountRead> {
    let reads = d.ev.reads.iter();
    reads.filter(|r| r.before == places::DROP_TWO).collect()
}

#[tokio::test]
async fn the_read_before_session_two_admits_x_alone() {
    let d = dropped(QfexModel::default(), "before-two-clean").await;
    // X rests after the graceful close, so the read before drop session two reads both named
    // accounts, admitting X alone on its own: one open order there, counted once by REST. Each
    // account's drop test reads so before its second session.
    let reads = reads_before_two(&d);
    let labels: Vec<&str> = reads.iter().map(|r| r.label.as_str()).collect();
    let want = [
        "primary (drop)",
        "subaccount",
        "primary",
        "subaccount (drop)",
    ];
    assert_eq!(labels, want, "{}", d.printed);
    assert_eq!((reads[0].page_orders, reads[0].page_allowed), (1, 1));
    assert_eq!(
        (reads[0].rest_open_orders, reads[0].rest_open_elsewhere),
        (1, 0)
    );
    assert_eq!((reads[1].page_orders, reads[1].page_allowed), (0, 0));
    assert_eq!((reads[3].page_orders, reads[3].page_allowed), (1, 1));
    assert!(reads.iter().all(|r| r.refusal().is_none()));
    let lines = [
        "READ after drop session one: subaccount",
        "READ before drop session two: primary (drop): 1 open orders, 0 TWAPs, flat true, REST \
         open_orders 1",
        "READ before drop session two: subaccount: 0 open orders",
        "SESSION primary (drop session two): it arms",
    ];
    for pair in lines.windows(2) {
        assert!(d.at(pair[0]) < d.at(pair[1]), "{pair:?}: {}", d.printed);
    }
    // X admitted, drop session two follows: no hand cancel and no stop.
    assert!(!d.printed.contains("RESTING"), "{}", d.printed);
    assert_eq!(d.ev.stop, None);
    assert!(d.ev.drops.iter().all(|r| r.x_before_two == Some(true)));
    let (report, json) = d.outputs();
    assert!(
        report.contains("| before drop session two | primary (drop) | 1 | yes | 1 | 1 | 0 | no |"),
        "{report}"
    );
    assert!(
        report.contains(
            "- X listed by the read before drop session two: yes: X open on the account as drop \
             session two armed"
        ),
        "{report}"
    );
    assert_eq!(json["drop_tests"][0]["x_before_two"], true);
}

#[tokio::test]
async fn any_other_open_order_before_session_two_stops_the_run() {
    // Another trader's order rests beside X once drop session one closed: the read before drop
    // session two refuses the first account, so the run stops there, X resting.
    let model = QfexModel {
        foreign_after_close: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "before-two-foreign").await;
    assert_stopped(&d, places::Stop::ReadDisagrees);
    assert_no_cancel_and_x_resting(&d);
    let reads = reads_before_two(&d);
    assert_eq!(reads.len(), 1, "{}", d.printed);
    assert_eq!((reads[0].page_orders, reads[0].page_allowed), (2, 1));
    let stopped = d.ev.stopped.clone().unwrap();
    assert!(
        stopped.starts_with(
            "primary (drop) holds an open order, a TWAP or a position that is not flat: no \
             session starts (decision 0132 item 7); ReadDisagrees in the read before drop \
             session two:"
        ),
        "{stopped}"
    );
}

#[tokio::test]
async fn a_rate_refusal_in_the_read_before_drop_session_two_stops_the_run() {
    // The fourth GET /user/positions or read-socket page after X: the comparison after X made
    // one, the reads after drop session one two, so the read before drop session two's first.
    for (limited, on) in [
        (model::Limited::Rest(3), places::RefusedOn::Rest),
        (model::Limited::ReadSocket(3), places::RefusedOn::ReadSocket),
    ] {
        let model = QfexModel {
            rate_limit: Some(limited),
            ..QfexModel::default()
        };
        let d = dropped(model, "before-two-rate").await;
        assert_stopped(&d, places::Stop::RateRefused { on });
        assert_no_cancel_and_x_resting(&d);
        assert!(reads_before_two(&d).is_empty(), "{}", d.printed);
        let stopped = d.ev.stopped.clone().unwrap();
        assert!(
            stopped.starts_with("primary (drop) could not be read:"),
            "{stopped}"
        );
    }
}

#[tokio::test]
async fn ctrl_c_during_the_read_before_drop_session_two_is_taken_after_it_and_stops_the_run() {
    // The read before drop session two's first authentication goes unanswered (the fifth after
    // X: the comparison's, the two reads after drop session one and X's own read), and the owner
    // presses Ctrl-C while it waits: the read runs to its end, then the run stops, X resting.
    let (dir, removed) = temp_dir("before-two-pressed");
    let model = QfexModel {
        silent_auth: Some(4),
        ..QfexModel::default()
    };
    let venue = Venue::start(model).await;
    let argv = set(drop_argv(&dir, &venue), "--read-wait-ms", "5000");
    let (opts, places) = parsed(&argv).unwrap();
    let waiting = |log: &[model::Event]| {
        let after = log.iter().skip_while(|e| !e.what.ends_with(" add_order"));
        after.filter(|e| e.what.ends_with(" auth")).count() >= 5
    };
    let asked = (opts, places);
    let (ran, printed) = pressed_run(&venue, asked, &environment(), waiting, || {}).await;
    let ev = stopped(ran, &printed);
    let d = Dropped {
        dir,
        _removed: removed,
        venue,
        ev,
        printed,
    };
    assert_stopped(&d, places::Stop::Interrupted);
    assert_no_cancel_and_x_resting(&d);
    let stopped = d.ev.stopped.clone().unwrap();
    assert!(
        stopped.starts_with("primary (drop) could not be read:"),
        "{stopped}"
    );
    assert!(
        stopped.contains("Interrupted in the read before drop session two"),
        "{stopped}"
    );
}

// ----- FBC-77r8: drop session two, the abrupt drop and the verdict, run against the model -----

/// [`dropped_with`] with the environment `env`, which names the accounts the run reads.
async fn dropped_in(
    model: QfexModel,
    what: &str,
    env: &HashMap<&'static str, String>,
    argv: impl Fn(Argv) -> Argv,
) -> Dropped {
    let (dir, removed) = temp_dir(what);
    let venue = Venue::start(model).await;
    let asked = argv(drop_argv(&dir, &venue));
    let (ran, printed) = place_run(&venue, &asked, env).await;
    let ev = stopped(ran, &printed);
    assert!(!ev.drops.is_empty(), "{printed}");
    Dropped {
        dir,
        _removed: removed,
        venue,
        ev,
        printed,
    }
}

/// A short wait for the reads after the drop: at most two reads, at once and a second later.
fn short_agree(argv: Argv) -> Argv {
    set(argv, "--agree-wait-ms", "2000")
}

impl Dropped {
    /// The first named account's drop session two.
    fn two(&self) -> &places::TwoRun {
        self.drop_run().two.as_ref().expect("drop session two ran")
    }

    /// The first named account's verdict.
    fn verdict(&self) -> &verdict::DropVerdict {
        self.drop_run().verdict.as_ref().expect("a verdict")
    }

    /// How many `add_order`s each of the model's sessions sent, in order.
    fn places_by_session(&self) -> Vec<usize> {
        let what = self.what();
        let sent = |conn: &String| {
            let place = format!("trade {conn} add_order");
            what.iter().filter(|w| **w == place).count()
        };
        self.session_conns().iter().map(sent).collect()
    }

    /// The trade connections, in order, of the model's sessions (each subscribed).
    fn session_conns(&self) -> Vec<String> {
        let what = self.what();
        let mut conns: Vec<String> = Vec::new();
        for w in what
            .iter()
            .filter(|w| w.starts_with("trade ") && w.ends_with(" subscribe"))
        {
            let conn = w.split(' ').nth(1).unwrap().to_owned();
            if !conns.contains(&conn) {
                conns.push(conn);
            }
        }
        conns
    }
}

#[tokio::test]
async fn the_drop_tests_steps_run_in_order_with_no_cancel_or_reconnect() {
    let d = dropped_in(
        QfexModel::default(),
        "drop-test-order",
        &primary_only(),
        |a| a,
    )
    .await;
    let what = d.what();
    let at = |w: &str| {
        what.iter()
            .position(|e| e == w)
            .unwrap_or_else(|| panic!("{w}: {what:?}"))
    };
    let conns = d.session_conns();
    assert_eq!(conns.len(), 2, "{what:?}");
    let (one, two) = (&conns[0], &conns[1]);
    let on = |conn: &str, kind: &str| at(&format!("trade {conn} {kind}"));
    // Session one: its arm, X, its graceful close.
    let first_arm = on(one, "cancel_on_disconnect");
    assert!(first_arm < on(one, "add_order"));
    let closed = at(&format!("cod {one} close keeps"));
    // The read before drop session two, on a socket that never arms, then session two: its
    // arm, Y, the read before the drop, the abrupt drop.
    let before_two = (closed..what.len())
        .find(|i| what[*i].ends_with(" get_user_orders") && !what[*i].contains(&format!(" {two} ")))
        .unwrap();
    assert!(before_two < on(two, "auth"), "{what:?}");
    assert!(on(two, "cancel_on_disconnect") < on(two, "add_order"));
    let dropped = at(&format!("cod {two} drop cancels"));
    let before_drop = (on(two, "add_order")..dropped)
        .find(|i| what[*i].ends_with(" get_user_orders") && !what[*i].contains(&format!(" {two} ")))
        .unwrap();
    assert!(on(two, "add_order") < before_drop, "{what:?}");
    // The reads after the drop, then the trades since X's send and historic-orders, then the
    // read of every named account.
    let after_drop = (dropped..what.len())
        .find(|i| what[*i].ends_with(" get_user_orders"))
        .unwrap();
    let trades = (after_drop..what.len())
        .find(|i| what[*i].ends_with(" get_user_trades"))
        .unwrap();
    let historic = (trades..what.len())
        .find(|i| what[*i].starts_with("GET /user/historic-orders"))
        .unwrap();
    let last = what.len() - 1;
    assert!(
        dropped < after_drop && after_drop < trades && historic < last,
        "{what:?}"
    );
    // From the first arm to the last read: no cancel, two arms alone, and no reconnect: one
    // connection attempt a session, and no session connection after the second.
    let span = &what[first_arm..];
    assert!(
        span.iter().all(|w| !w.ends_with(" cancel_order")),
        "{what:?}"
    );
    let arms = span
        .iter()
        .filter(|w| w.ends_with(" cancel_on_disconnect"))
        .count();
    assert_eq!(arms, 2, "{what:?}");
    assert_eq!(d.count("subscribe"), 6);
    let attempts: Vec<(u64, u64)> = (d.ev.sessions.iter())
        .map(|s| (s.attempts, s.failed_attempts))
        .collect();
    assert_eq!(attempts, [(1, 0), (1, 0)]);
    let ended: Vec<&str> = d.ev.sessions.iter().map(|s| s.ended_by).collect();
    assert_eq!(ended, [stage::GRACEFUL, stage::DROPPED]);
    // The abrupt drop sent no WebSocket close; every other socket was closed.
    let n: usize = two.parse().unwrap();
    assert!(!d.venue.trade.connections()[n].client_closed);
    // The lines, in order.
    let lines = [
        "SESSION primary (drop session one): arm outcome Accepted, ended by a graceful close",
        "READ before drop session two: primary (drop)",
        "SESSION primary (drop session two): it arms, places Y",
        "PLACE Y on primary",
        "READ before the drop: primary: X listed true, Y listed true",
        "SESSION primary (drop session two): arm outcome Accepted, ended by the abrupt drop",
        "READ after the drop: primary:",
        "VERDICT drop test on primary: passed",
        "READ after drop session two: primary (drop)",
        "STOPPED stage two",
    ];
    for pair in lines.windows(2) {
        assert!(d.at(pair[0]) < d.at(pair[1]), "{pair:?}: {}", d.printed);
    }
    assert!(d.verdict().passed, "{:?}", d.verdict());
    d.venue.assert_served();
}

#[tokio::test]
async fn the_drop_tests_sessions_share_one_registry_and_seed_once() {
    let d = dropped_in(
        QfexModel::default(),
        "drop-one-registry",
        &primary_only(),
        |a| a,
    )
    .await;
    // One drop registry for the account, built, tripped and seeded once, whose ledger is read at
    // the end of each of its two sessions (decision 0148 item 3).
    assert_eq!(d.ev.place_registries.len(), 1);
    let ends = &d.ev.place_registries[0].registry_run().ends;
    let sessions: Vec<&str> = ends.iter().map(|e| e.session).collect();
    assert_eq!(sessions, [places::DROP_ONE, places::DROP_TWO]);
    assert_eq!(d.printed.matches("REGISTRY drop on primary").count(), 1);
    assert_eq!(d.printed.matches("seeded flat").count(), 1);
    // Started once, by drop session one: drop session two's market stays armed.
    assert_eq!(d.printed.matches("START primary").count(), 1);
    assert!(d.at("REGISTRY drop on primary") < d.at("SESSION primary (drop session one)"));
    let names: Vec<(&str, &str)> = (d.ev.sessions.iter())
        .map(|s| (s.account, s.label()))
        .collect();
    assert_eq!(
        names,
        [("primary", places::DROP_ONE), ("primary", places::DROP_TWO)]
    );
    // Y went out through the same Link and registry: the market Started in session one placed
    // it, and drop session two's resync seeded nothing.
    let two = d.two();
    assert!(two.y.submitted && two.y.acked && two.resynced, "{two:?}");
}

#[tokio::test]
async fn the_drop_cancels_x_and_y_and_the_test_passes() {
    let d = dropped(QfexModel::default(), "drop-passes").await;
    // On each named account: X listed before the drop, then X and Y absent after it, named by
    // no trade and ended on historic orders: the test passes, the drop's reach over X shown.
    assert_eq!(d.ev.drops.len(), 2);
    for run in &d.ev.drops {
        let verdict = run.verdict.as_ref().unwrap();
        assert!(verdict.passed, "{verdict:?}");
        assert_eq!(verdict.reach, verdict::Reach::Cancelled);
        assert_eq!(verdict.arm, verdict::ArmCancel::Survived);
        let cancelled = Some("CANCELLED".to_owned());
        assert_eq!(verdict.terminal, [cancelled.clone(), cancelled]);
        let two = run.two.as_ref().unwrap();
        assert_eq!(two.before_drop, Some([true, true]));
        assert_eq!(two.ended_by, Some(stage::DROPPED));
        let after = two.after_drop.as_ref().unwrap();
        assert!(after.absent_after.is_some() && after.refused.is_none());
        assert!(!two.resting && two.read);
        assert_eq!((run.stop, run.others_clean), (None, Some(true)));
    }
    let model = d.venue.model.lock().unwrap();
    assert!(
        model
            .orders
            .iter()
            .all(|o| o.state == model::State::Cancelled)
    );
    drop(model);
    assert_eq!((d.ev.stop, d.marker()), (None, None));
    assert_eq!(d.ev.stopped.as_deref(), Some(stage::NOT_BUILT));
    assert_eq!(d.count("cancel_order"), 0);
    let (report, json) = d.outputs();
    for (account, followed) in [
        ("primary", places::NEXT_ACCOUNT),
        ("subaccount", places::LAST_ACCOUNT),
    ] {
        let section = report
            .split(&format!("## The drop test on {account}"))
            .nth(1)
            .unwrap();
        for line in [
            "- The drop test: passed: X and Y absent after the drop, named by no trades row, and \
             listed by GET /user/historic-orders",
            "- The drop's reach over X, an order of an earlier connection: shown: the drop \
             cancelled X",
            &format!("- What followed: {followed}"),
            "- Ended by: the abrupt drop",
        ] {
            assert!(section.contains(line), "{line}: {section}");
        }
    }
    let tests = json["drop_tests"].as_array().unwrap();
    assert!(tests.iter().all(|t| t["verdict"]["passed"] == true));
    assert!(
        tests
            .iter()
            .all(|t| t["session_two"]["ended_by"] == stage::DROPPED)
    );
}

#[tokio::test]
async fn a_graceful_close_that_cancelled_x_is_recorded_and_y_still_judged() {
    let model = QfexModel {
        cod_on_close: true,
        ..QfexModel::default()
    };
    let d = dropped_in(model, "drop-close-then-y", &primary_only(), |a| a).await;
    let run = d.drop_run();
    // The graceful close cancelled X: recorded, and drop session two still runs, for Y.
    assert_eq!(run.close_cancelled_x, Some(true));
    assert_eq!(run.x_before_two, Some(false));
    let two = d.two();
    assert!(two.y.acked, "{two:?}");
    assert_eq!(two.before_drop, Some([false, true]));
    // Y is judged: absent after the drop, ended on historic orders; whether enabling cancels
    // and the drop's reach over X are not measured, never failed.
    let verdict = d.verdict();
    assert!(verdict.passed, "{verdict:?}");
    assert_eq!(verdict.arm, verdict::ArmCancel::NotMeasured);
    assert_eq!(verdict.reach, verdict::Reach::NotMeasured);
    assert_eq!(verdict.terminal[1].as_deref(), Some("CANCELLED"));
    assert_eq!(d.ev.stop, None);
    let (report, _) = d.outputs();
    assert!(
        report.contains(
            "- Whether a graceful close cancels: yes: X rested before it, no cancel of ours was \
             sent, and the read after it no longer listed X"
        ),
        "{report}"
    );
    assert!(
        report.contains(
            "- X listed by the read before drop session two: no: X no longer open on the account"
        ),
        "{report}"
    );
}

#[tokio::test]
async fn the_arm_cancelling_x_reported_is_recorded() {
    let model = QfexModel {
        arm_cancels_open: model::ArmCancels::Reported,
        ..QfexModel::default()
    };
    let d = dropped_in(model, "drop-arm-reported", &primary_only(), |a| a).await;
    let two = d.two();
    // Drop session two's arm cancelled X and QFEX reported it, in the arm's window, before Y.
    assert!(two.x_cancel_reported, "{two:?}");
    assert!(
        two.echoes
            .iter()
            .any(|e| e == "order_response CANCELLED: exact"),
        "{:?}",
        two.echoes
    );
    let verdict = d.verdict();
    assert_eq!(verdict.arm, verdict::ArmCancel::Reported);
    assert!(verdict.passed, "{verdict:?}");
    assert_eq!(d.ev.stop, None);
    let two_conn = &d.session_conns()[1];
    let reported = format!("arm {two_conn} cancels 1 reported");
    assert!(logged(&d.venue.log(), &reported), "{:?}", d.what());
    let (report, json) = d.outputs();
    assert!(
        report.contains(
            "- Whether enabling cancel-on-disconnect cancels open orders, and whether QFEX \
             reports it: yes, reported: a CANCELLED of X came in the arm's window"
        ),
        "{report}"
    );
    assert!(
        report.contains("- A CANCELLED of X reported in the arm's window: yes"),
        "{report}"
    );
    assert_eq!(
        json["drop_tests"][0]["session_two"]["x_cancel_reported"],
        true
    );
}

#[tokio::test]
async fn an_arm_that_cancels_x_silently_is_recorded_and_stops_after_the_drop() {
    let model = QfexModel {
        arm_cancels_open: model::ArmCancels::Silent,
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-arm-silent").await;
    assert_stopped(&d, places::Stop::ArmCancelUnreported);
    // The read before the drop no longer listed X, and no CANCELLED of it came: recorded, and
    // the run stops after the drop, which still ran.
    let two = d.two();
    assert!(!two.x_cancel_reported);
    assert_eq!(two.before_drop, Some([false, true]));
    assert_eq!(two.ended_by, Some(stage::DROPPED));
    let verdict = d.verdict();
    assert_eq!(verdict.arm, verdict::ArmCancel::Unreported);
    assert_eq!(verdict.reach, verdict::Reach::NotMeasured);
    assert!(verdict.passed, "{verdict:?}");
    let dropped = d.what().iter().position(|w| w.contains(" drop cancels"));
    assert!(dropped.is_some(), "{:?}", d.what());
    assert!(d.at("VERDICT drop test on primary") < d.at("STOPPED ArmCancelUnreported"));
    assert!(
        d.ev.stopped
            .as_deref()
            .unwrap()
            .starts_with("ArmCancelUnreported after the drop"),
        "{:?}",
        d.ev.stopped
    );
    // No later account's drop test runs: the subaccount places nothing.
    assert_eq!(d.ev.drops.len(), 1);
    assert_eq!(d.count("add_order"), 2);
    let (report, _) = d.outputs();
    assert!(
        report.contains(
            "yes, not reported: the read before the drop no longer listed X and no CANCELLED of \
             it came (decision 0132 item 4), a stop after the drop"
        ),
        "{report}"
    );
}

#[tokio::test]
async fn x_absent_before_the_drop_leaves_the_reach_over_x_not_measured() {
    // The arm cancelled X with no report of it: the read before the drop lists Y alone, and no
    // CANCELLED of X came, so X's absence alone leaves the drop's reach over X, an earlier
    // connection's order, not measured, and the test is not failed for it (the run stops after
    // the drop on the unreported cancel).
    let model = QfexModel {
        arm_cancels_open: model::ArmCancels::Silent,
        ..QfexModel::default()
    };
    let d = dropped_in(model, "drop-x-absent", &primary_only(), |a| a).await;
    assert_eq!(d.two().before_drop, Some([false, true]));
    assert!(!d.two().x_cancel_reported && !d.two().x_cancelled_before_drop);
    let verdict = d.verdict();
    assert_eq!(verdict.reach, verdict::Reach::NotMeasured);
    assert!(verdict.passed && verdict.failures.is_empty(), "{verdict:?}");
    let (report, json) = d.outputs();
    assert!(
        report.contains(
            "- The drop's reach over X, an order of an earlier connection: not measured: the \
             read before the drop did not list X"
        ),
        "{report}"
    );
    assert_eq!(
        json["drop_tests"][0]["session_two"]["before_drop"],
        super::json(r#"{"x_listed":false,"y_listed":true}"#)
    );
}

#[tokio::test]
async fn y_still_open_after_the_wait_fails_the_drop_test() {
    // The drop cancels X alone: Y, its own connection's order, is still listed when the wait
    // ends, so the test fails, a stop with Y resting for the owner's hand cancel.
    let model = QfexModel {
        cod_reach: model::Reach::Earlier,
        ..QfexModel::default()
    };
    let d = dropped_with(model, "drop-y-open", short_agree).await;
    assert_stopped(&d, places::Stop::DropFailed);
    let verdict = d.verdict();
    assert!(!verdict.passed);
    // Y still listed, and so not ended on historic orders either (0151 item 13).
    let y = verdict::Which::Y;
    let failures = [
        verdict::Failure::Listed(y),
        verdict::Failure::NotTerminal(y),
    ];
    assert_eq!(verdict.failures, failures);
    let after = d.two().after_drop.as_ref().unwrap();
    assert!(
        after.reads.len() >= 2 && after.absent_after.is_none(),
        "{after:?}"
    );
    assert!(d.two().resting);
    assert!(
        d.printed
            .contains(&format!("RESTING {}", places::HAND_CANCEL_ORDERS))
    );
    assert_eq!(d.count("cancel_order"), 0);
    let (report, json) = d.outputs();
    assert!(
        report.contains(
            "- The drop test: failed: Y still listed by get_user_orders when the wait ended"
        ),
        "{report}"
    );
    assert_eq!(
        json["drop_tests"][0]["session_two"]["hand_cancel"],
        places::HAND_CANCEL_ORDERS
    );
}

#[tokio::test]
async fn a_trade_row_naming_y_fails_the_drop_test() {
    // Y filled as the connection dropped: absent after it, but a get_user_trades row names it.
    let model = QfexModel {
        fill_on_drop: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-y-traded").await;
    let verdict = d.verdict();
    assert!(!verdict.passed);
    assert!(
        verdict
            .failures
            .contains(&verdict::Failure::Traded(verdict::Which::Y)),
        "{verdict:?}"
    );
    let traded = d.two().after.as_ref().unwrap();
    assert_eq!(traded.naming, [0, 1]);
    assert!(traded.trades_whole && traded.historic_whole);
    assert_eq!(traded.terminal[1].as_deref(), Some("FILLED"));
    let (report, _) = d.outputs();
    assert!(report.contains("a get_user_trades row names Y"), "{report}");
    // The fields the row carried: names alone, never a value.
    assert!(
        report.contains("- The members a get_user_trades row carries: client_order_id, fee,"),
        "{report}"
    );
    assert!(traded.fields.iter().any(|f| f == "trade_id"), "{traded:?}");
}

#[tokio::test]
async fn a_trade_row_naming_another_order_or_none_after_the_drop_fails_it_unmeasured() {
    // Carried from FBC-23ch's fourth review fix: as the connection drops, two trades of an order
    // the model never had, one each way, so the account stays flat; their rows name another
    // order, or none, which a row of X's own fill could be. Decision 0132 item 4 rules out
    // anything else trading on the account, so X's absence is not shown the drop's cancel, its
    // reason given, and the drop test fails on the rows.
    for (stray, what) in [
        (model::StrayTrade::Other, "drop-two-other-trade"),
        (model::StrayTrade::Unnamed, "drop-two-unnamed-trade"),
    ] {
        let model = QfexModel {
            trade_on_drop: Some(stray),
            ..QfexModel::default()
        };
        let d = dropped_in(model, what, &primary_only(), |a| a).await;
        let stop = Some(places::Stop::DropFailed.name());
        assert_eq!(d.ev.stop, stop, "{}", d.printed);
        let traded = d.two().after.as_ref().unwrap();
        assert_eq!((traded.trades, traded.naming.clone()), (2, vec![0, 0]));
        assert!(traded.trades_whole && traded.historic_whole, "{traded:?}");
        let verdict = d.verdict();
        assert_eq!(
            verdict.failures,
            [verdict::Failure::OtherTrade],
            "{verdict:?}"
        );
        let why = verdict::Unmeasured::OtherTrade;
        assert_eq!(verdict.reach, verdict::Reach::Unmeasured(why));
        assert_eq!(verdict.arm, verdict::ArmCancel::Survived);
        let after = (d.ev.reads.iter())
            .find(|r| r.before == refusals::AFTER_DROP_TWO && r.label == "primary (drop)")
            .unwrap();
        assert!(after.flat, "{after:?}");
        let (report, json) = d.outputs();
        let reach = format!(
            "- The drop's reach over X, an order of an earlier connection: not measured: the \
             reads after the drop no longer listed X, but {}",
            why.says()
        );
        assert!(report.contains(&reach), "{report}");
        assert!(
            report.contains(
                "- The drop test: failed: a get_user_trades row since X's send names another \
                 order or none: something else traded on the account (decision 0132 item 4)"
            ),
            "{report}"
        );
        assert_eq!(
            json["drop_tests"][0]["verdict"]["reach_over_x"],
            verdict.reach.says()
        );
    }
}

#[tokio::test]
async fn an_arm_time_absence_with_another_trade_after_it_is_no_unreported_cancel() {
    // Carried from FBC-23ch's fourth review fix: the arm cancels X silently, so the read before
    // the drop no longer lists X, and a trade naming another order comes as the connection
    // drops. X's absence is not shown a cancel (decision 0132 item 4), so what the arm did is
    // not measured, its reason given, and the run stops on the failed test, not on an
    // unreported arm-time cancel.
    let model = QfexModel {
        arm_cancels_open: model::ArmCancels::Silent,
        trade_on_drop: Some(model::StrayTrade::Other),
        ..QfexModel::default()
    };
    let d = dropped_in(model, "drop-arm-silent-other-trade", &primary_only(), |a| a).await;
    assert_eq!(d.two().before_drop, Some([false, true]));
    let verdict = d.verdict();
    let why = verdict::Unmeasured::OtherTrade;
    assert_eq!(
        verdict.arm,
        verdict::ArmCancel::Unmeasured(why),
        "{verdict:?}"
    );
    assert_eq!(verdict.reach, verdict::Reach::NotMeasured);
    assert_eq!(verdict.failures, [verdict::Failure::OtherTrade]);
    let stop = Some(places::Stop::DropFailed.name());
    assert_eq!(d.ev.stop, stop, "{}", d.printed);
    let (report, json) = d.outputs();
    let arm = format!(
        "- Whether enabling cancel-on-disconnect cancels open orders, and whether QFEX reports \
         it: not measured: the read before the drop no longer listed X and no CANCELLED of it \
         came, but {}",
        why.says()
    );
    assert!(report.contains(&arm), "{report}");
    assert_eq!(
        json["drop_tests"][0]["verdict"]["arm_cancels_open_orders"],
        verdict.arm.says()
    );
}

#[tokio::test]
async fn a_graceful_close_not_measured_is_said_so_in_the_drop_tests_summary() {
    // Carried from FBC-23ch's fourth review fix: QFEX cancels X itself after its ACK, so X no
    // longer rests as drop session one's graceful close is sent. Whether a graceful close
    // cancels is not measured, its reason given, in the drop test's summary as in drop session
    // one's section; drop session two still runs for Y, and what the arm did is not measured.
    let model = QfexModel {
        venue_cancel: true,
        ..QfexModel::default()
    };
    let d = dropped_in(
        model,
        "drop-close-unmeasured-summary",
        &primary_only(),
        |a| a,
    )
    .await;
    let run = d.drop_run();
    let why = places::CloseUnmeasured::NotRested;
    assert_eq!(
        (run.close_cancelled_x, run.close_not_measured),
        (None, Some(why))
    );
    assert_eq!(run.x_before_two, Some(false));
    assert!(d.two().y.acked, "{:?}", d.two());
    let verdict = d.verdict();
    assert_eq!(verdict.arm, verdict::ArmCancel::NotMeasured);
    let (report, _) = d.outputs();
    let summary = format!(
        "- Whether a graceful close cancels: not measured: {}",
        why.says()
    );
    assert!(report.contains(&summary), "{report}");
    let section = format!(
        "- Whether the graceful close cancelled X: not measured: {}",
        why.says()
    );
    assert!(report.contains(&section), "{report}");
}

#[tokio::test]
async fn historic_orders_not_terminal_fails_the_drop_test() {
    // GET /user/historic-orders leaves Y out: X and Y absent and untraded, but Y not shown
    // ended, so the test fails (0151 item 13).
    let model = QfexModel {
        historic_omits_last: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-historic").await;
    assert_stopped(&d, places::Stop::DropFailed);
    let verdict = d.verdict();
    assert_eq!(
        verdict.failures,
        [verdict::Failure::NotTerminal(verdict::Which::Y)]
    );
    assert_eq!(verdict.terminal, [Some("CANCELLED".to_owned()), None]);
    let (report, _) = d.outputs();
    assert!(
        report
            .contains("- terminal_status on GET /user/historic-orders: X CANCELLED; Y not listed"),
        "{report}"
    );
    assert!(
        report.contains("no GET /user/historic-orders row names Y"),
        "{report}"
    );
}

#[tokio::test]
async fn a_fill_of_x_in_drop_session_two_stops_with_cancels_not_flat() {
    // Drop session two's arm comes with a maker fill of X: a fill of ours while the session is
    // open, so it cancels by venue id (nothing of ours is left open; Y is never placed), closes
    // gracefully, and the reads after it show a position: the run stops not flat (FBC-ou9g adds
    // the flatten, 0151 item 9).
    let model = QfexModel {
        fill_at_arm: Some(1),
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-two-fill-x").await;
    assert_stopped(&d, places::Stop::UnexpectedFill);
    let two = d.two();
    let cancels = two.cancels.as_ref().expect("a fill of ours cancels");
    assert!(cancels.ended && !cancels.held, "{cancels:?}");
    assert_eq!(two.ended_by, Some(stage::GRACEFUL));
    assert!(!two.y.submitted);
    assert!(d.drop_run().verdict.is_none());
    assert_eq!(d.count("add_order"), 1);
    // X filled whole and Y never placed: nothing of ours was left open to cancel.
    assert_eq!((d.count("cancel_order"), cancels.sent), (0, 0));
    let after = (d.ev.reads.iter())
        .find(|r| r.before == refusals::AFTER_DROP_TWO && r.label == "primary (drop)")
        .unwrap();
    assert!(!after.flat);
    assert!(
        d.ev.stopped
            .as_deref()
            .unwrap()
            .starts_with("UnexpectedFill in drop session two"),
        "{:?}",
        d.ev.stopped
    );
    let steps: Vec<&str> = two.compared.iter().map(|c| c.step).collect();
    assert_eq!(steps, ["the stop's cancels"]);
    let (report, _) = d.outputs();
    assert!(
        report.contains(
            "- The drop test: not judged: the drop test ended before its drop (decision 0132 \
             item 4), so it did not pass"
        ),
        "{report}"
    );
}

#[tokio::test]
async fn a_fill_of_y_seen_after_the_drop_stops_not_flat() {
    // Y filled as the connection dropped, which no session was open to hear: the trades row
    // after the drop shows it, and the run stops not flat, the position the owner's.
    let model = QfexModel {
        fill_on_drop: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-y-filled").await;
    assert_stopped(&d, places::Stop::UnexpectedFill);
    assert_eq!(d.two().ended_by, Some(stage::DROPPED));
    assert_eq!(d.two().cancels, None);
    assert_eq!(d.count("cancel_order"), 0);
    let verdict = d.verdict();
    assert!(
        verdict.failures.contains(&verdict::Failure::NotFlat),
        "{verdict:?}"
    );
    let after = (d.ev.reads.iter())
        .find(|r| r.before == refusals::AFTER_DROP_TWO && r.label == "primary (drop)")
        .unwrap();
    assert!(!after.flat);
    assert!(
        d.ev.stopped
            .as_deref()
            .unwrap()
            .starts_with("UnexpectedFill after the drop"),
        "{:?}",
        d.ev.stopped
    );
    let (report, _) = d.outputs();
    assert!(report.contains(places::OWNERS_POSITION), "{report}");
}

#[tokio::test]
async fn ctrl_c_between_the_drop_sessions_reports_x_resting_and_stops() {
    // The owner presses Ctrl-C once drop session one closed: the reads after it run to their
    // end, every named account read, then the run stops, X resting for the owner's hand cancel,
    // the marker written, and drop session two never opens.
    let at = "SESSION primary (drop session one): arm outcome Accepted, ended by a graceful close";
    let d = pressed_at(QfexModel::default(), "drop-between", at).await;
    assert_stopped(&d, places::Stop::Interrupted);
    assert_no_cancel_and_x_resting(&d);
    assert!(d.drop_run().two.is_none());
    assert_eq!(d.count("subscribe"), 3);
    assert_eq!(d.count("cancel_on_disconnect"), 1);
    let model = d.venue.model.lock().unwrap();
    assert_eq!(model.orders.len(), 1);
    assert_eq!(model.orders[0].state, model::State::Open);
    drop(model);
    // Pressed as the session's end is printed, before the reads after it: they take it.
    assert!(
        d.ev.stopped
            .as_deref()
            .unwrap()
            .starts_with("Interrupted during the reads after drop session one"),
        "{:?}",
        d.ev.stopped
    );
    assert!(reads_before_two(&d).is_empty(), "{}", d.printed);
}

#[tokio::test(start_paused = true)]
async fn the_span_bound_in_drop_session_two_cancels_nothing_and_fails_it() {
    let frozen = freeze();
    // Y is never answered; while drop session two waits for it, the registry's span bound (600
    // s here) passes: no cancel, a graceful close, no drop and no verdict, and the stop.
    let model = QfexModel {
        ack_delay: Some(Duration::from_secs(86_400)),
        ack_delay_on: 1,
        ..QfexModel::default()
    };
    let (dir, removed) = temp_dir("drop-two-span");
    let venue = Venue::start(model).await;
    let (mut opts, places) = parsed(&drop_argv(&dir, &venue)).unwrap();
    opts.rate_user = user();
    let env = environment();
    let printed = venue.printed();
    let lookup = |var: &str| env.get(var).cloned();
    let out: stage::Out = Rc::new(RefCell::new(printed.clone()));
    let drive = async {
        let placed = |log: &[model::Event]| {
            log.iter()
                .filter(|e| e.what.ends_with(" add_order"))
                .count()
                == 2
        };
        until("Y's place", || placed(&venue.log())).await;
        tokio::time::advance(Duration::from_secs(601)).await;
    };
    let ran = stage::run_places(&opts, &places, &lookup, out);
    let (ran, ()) = tokio::join!(ran, drive);
    drop(frozen);
    let printed = printed.text();
    let ev = stopped(ran, &printed);
    let d = Dropped {
        dir,
        _removed: removed,
        venue,
        ev,
        printed,
    };
    assert_stopped(&d, places::Stop::SpanBound);
    assert_eq!(d.count("cancel_order"), 0);
    let two = d.two();
    assert_eq!(
        (two.cancels.as_ref(), two.ended_by),
        (None, Some(stage::GRACEFUL))
    );
    assert!(two.after_drop.is_none() && d.drop_run().verdict.is_none());
    assert!(two.resting, "{two:?}");
    assert!(
        d.ev.stopped
            .as_deref()
            .unwrap()
            .starts_with("SpanBound in drop session two"),
        "{:?}",
        d.ev.stopped
    );
    let closes: Vec<String> = d
        .what()
        .into_iter()
        .filter(|w| w.starts_with("cod "))
        .collect();
    assert!(closes.iter().all(|w| w.contains(" close ")), "{closes:?}");
}

#[tokio::test]
async fn a_failed_drop_test_places_nothing_more_on_that_account() {
    // The drop cancels nothing: X and Y are still listed when the wait ends, so the primary
    // account's drop test fails, a stop, and nothing more is placed, on it or on the subaccount.
    let model = QfexModel {
        cod_on_drop: false,
        ..QfexModel::default()
    };
    let d = dropped_with(model, "drop-failed", short_agree).await;
    assert_stopped(&d, places::Stop::DropFailed);
    let verdict = d.verdict();
    let listed = [verdict::Which::X, verdict::Which::Y].map(verdict::Failure::Listed);
    assert_eq!(verdict.failures[..2], listed);
    assert_eq!(verdict.reach, verdict::Reach::NotShown);
    // X and Y, then nothing: no session on the subaccount, and nothing after the drop.
    assert_eq!(d.count("add_order"), 2);
    assert_eq!(d.count("subscribe"), 6);
    assert_eq!(d.ev.drops.len(), 1);
    let dropped = d
        .what()
        .iter()
        .position(|w| w.contains(" drop keeps"))
        .unwrap();
    assert!(
        d.what()[dropped..]
            .iter()
            .all(|w| !w.ends_with(" add_order"))
    );
    assert!(
        d.printed
            .contains(&format!("RESTING {}", places::HAND_CANCEL_ORDERS))
    );
    let (report, _) = d.outputs();
    assert!(
        report.contains(
            "- What followed: the run's stop, DropFailed: nothing more is placed on this \
             account, and no later named account's drop test runs (decision 0132 item 7)"
        ),
        "{report}"
    );
}

#[tokio::test]
async fn a_passed_verdict_is_not_carried_into_a_second_run() {
    // A run whose drop test passes, then a second on the same lease directory: it runs its own
    // drop test from the start, and its own verdict, failed here, is the one it acts on.
    let (dir, _removed) = temp_dir("drop-not-carried");
    let leases = leases_of(&dir);
    let venue = Venue::start(QfexModel::default()).await;
    let first = dir.join("first");
    let argv = set(
        drop_argv(&first, &venue),
        "--lease-dir",
        leases.to_str().unwrap(),
    );
    let (ran, printed) = place_run(&venue, &argv, &primary_only()).await;
    let ev = stopped(ran, &printed);
    assert!(ev.drops[0].verdict.as_ref().unwrap().passed);
    assert_eq!(ev.stop, None);
    // Nothing of it is kept where a later run looks: the lease directory holds no marker and no
    // verdict.
    assert!(!leases.join(places::MARKER).exists());
    for entry in fs::read_dir(&leases).unwrap() {
        let path = entry.unwrap().path();
        let text = fs::read(&path).unwrap_or_default();
        let text = String::from_utf8_lossy(&text);
        assert!(
            !text.contains("passed") && !text.contains("verdict"),
            "{}",
            path.display()
        );
    }
    let venue = Venue::start(QfexModel {
        cod_on_drop: false,
        ..QfexModel::default()
    })
    .await;
    let second = dir.join("second");
    let argv = set(
        drop_argv(&second, &venue),
        "--lease-dir",
        leases.to_str().unwrap(),
    );
    let argv = short_agree(set(argv, "--namespace", "8"));
    let (ran, printed) = place_run(&venue, &argv, &primary_only()).await;
    let ev = stopped(ran, &printed);
    // Its own drop test, from the start: clean reads, its own registry seeded, X and Y.
    assert!(
        printed.contains("READ before drop session one: primary (drop)"),
        "{printed}"
    );
    assert!(printed.contains("REGISTRY drop on primary"), "{printed}");
    assert_eq!(
        venue
            .log()
            .iter()
            .filter(|e| e.what.ends_with(" add_order"))
            .count(),
        2
    );
    assert!(!ev.drops[0].verdict.as_ref().unwrap().passed);
    assert_eq!(ev.stop, Some("DropFailed"));
}

#[tokio::test]
async fn the_report_gives_each_accounts_drop_items_and_the_slice_passes_the_privacy_check() {
    let d = dropped(QfexModel::default(), "drop-report").await;
    let (report, json) = d.outputs();
    for account in ["primary", "subaccount"] {
        let section = report
            .split(&format!("## The drop test on {account} "))
            .nth(1)
            .unwrap();
        let section = section.split("\n## ").next().unwrap();
        for item in [
            "- What answers the arm: drop session one Accepted; drop session two Accepted",
            "- Whether enabling cancel-on-disconnect cancels open orders, and whether QFEX \
             reports it: no: X survived drop session two's arm",
            "- The drop test: passed",
            "- Whether a graceful close cancels: no: X rested before it and still did after it",
            "- Whether the other named accounts stayed clean: yes: every read of every other \
             named account was clean",
            "- terminal_status on GET /user/historic-orders: X CANCELLED; Y CANCELLED",
            "- The members a get_user_trades row carries: not shown: no row was read",
            "### Drop session one (FBC-23ch)",
            "### Drop session two (FBC-77r8)",
            "- Y: handed to the session yes; shown resting by an order event yes; an ACK echoing \
             its client id byte for byte yes",
            "- The read before the drop: X listed yes; Y listed yes",
            " reads; the first listing neither X nor Y ended ",
            "- X's and Y's trades after the drop: 0 trade rows since X's send, 0 naming X, 0 \
             naming Y",
        ] {
            assert!(section.contains(item), "{account}: {item}: {section}");
        }
    }
    let tests = json["drop_tests"].as_array().unwrap();
    let accounts: Vec<&str> = tests
        .iter()
        .map(|t| t["account"].as_str().unwrap())
        .collect();
    assert_eq!(accounts, ["primary", "subaccount"]);
    for t in tests {
        assert_eq!(
            t["verdict"]["arm_cancels_open_orders"],
            "no: X survived drop session two's arm"
        );
        assert_eq!(t["others_clean"], true);
        assert_eq!(
            t["verdict"]["terminal_status"],
            super::json(r#"["CANCELLED","CANCELLED"]"#)
        );
    }
    // Every session's capture is whole, drop session two's, which journals no Closed control,
    // included; its frames are in the drop-production/ slice, which passes the privacy check.
    assert_eq!(d.ev.sessions.len(), 4);
    assert!(
        d.ev.sessions
            .iter()
            .all(|s| s.partial.is_empty() && s.arm_sent)
    );
    let evidence = d.dir.join("evidence");
    let slice = evidence.join(stage::DROP_SLICE);
    for account in ["primary", "subaccount"] {
        let prefix = format!("{account}-drop-session-two-");
        assert!(
            d.ev.fixtures.iter().any(|f| f.starts_with(&prefix)),
            "{:?}",
            d.ev.fixtures
        );
    }
    let readme = fs::read_to_string(slice.join("README.md")).unwrap();
    assert!(
        readme.contains("drop session two by an abrupt drop"),
        "{readme}"
    );
    assert_report_and_evidence_pass_the_privacy_check(&evidence);
    assert_no_secret(&evidence);
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/privacy-check.sh");
    evidence::privacy_check(&script, &slice).unwrap();
}

#[tokio::test]
async fn a_rate_refusal_in_the_reads_after_the_drop_fails_the_test_and_stops_the_run() {
    // The first read after the drop has its GET /user/positions answered HTTP 429: a provider
    // rate refusal, which ends the reads at once, fails the test and stops the run (decision
    // 0148 item 8), with nothing cancelled.
    let model = QfexModel {
        rate_limit: Some(model::Limited::RestAfterDrop),
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-after-refused").await;
    let on = places::RefusedOn::Rest;
    assert_stopped(&d, places::Stop::RateRefused { on });
    let verdict = d.verdict();
    assert_eq!(verdict.refused, Some(on));
    assert!(
        verdict.failures.contains(&verdict::Failure::RateRefused),
        "{verdict:?}"
    );
    let after = d.two().after_drop.as_ref().unwrap();
    assert_eq!(after.refused, Some(on));
    // The refused read is the last: none follows it.
    let last = after.reads.last().map(|(_, shown)| *shown);
    assert_eq!(last, Some(verdict::Shown::Refused), "{after:?}");
    assert_eq!(d.count("cancel_order"), 0);
    assert!(
        d.ev.stopped
            .as_deref()
            .unwrap()
            .starts_with("RateRefused(Rest) after the drop"),
        "{:?}",
        d.ev.stopped
    );
    let (report, json) = d.outputs();
    assert!(
        report.contains("cut short by a provider rate refusal (decision 0148 item 8)"),
        "{report}"
    );
    assert_eq!(
        json["drop_tests"][0]["session_two"]["after_drop"]["refused"],
        "RateRefused(Rest)"
    );
}

#[tokio::test]
async fn ctrl_c_after_the_drop_is_taken_once_the_reads_end_and_stops_the_run() {
    // The owner presses Ctrl-C as the drop is seen: the reads after it, the trades, historic
    // orders and every named account's read run to their end, the verdict is judged, and then
    // the run stops, no session being open, so nothing is cancelled (0151 item 2).
    let d = pressed_drop(
        QfexModel::default(),
        "drop-after-ctrl-c",
        |a| a,
        &[" drop cancels"],
    )
    .await;
    assert_stopped(&d, places::Stop::Interrupted);
    assert!(d.verdict().passed, "{:?}", d.verdict());
    assert_eq!(d.count("cancel_order"), 0);
    let read = |label: &str| {
        (d.ev.reads.iter()).any(|r| r.before == refusals::AFTER_DROP_TWO && r.label == label)
    };
    assert!(read("primary (drop)") && read("subaccount"));
    assert!(
        d.ev.stopped
            .as_deref()
            .unwrap()
            .starts_with("Interrupted during the reads after drop session two"),
        "{:?}",
        d.ev.stopped
    );
    assert_eq!(d.ev.drops.len(), 1);
}

/// A place-stage run against `model`, under a directory of its own, in which the owner presses
/// Ctrl-C as the probe prints `at`: at a known point of the run, whatever the machine's load.
async fn pressed_at(model: QfexModel, what: &str, at: &'static str) -> Dropped {
    let (dir, removed) = temp_dir(what);
    let venue = Venue::start(model).await;
    let (mut opts, places) = parsed(&drop_argv(&dir, &venue)).unwrap();
    opts.rate_user = user();
    let env = environment();
    let printed = venue.printed();
    let lookup = |var: &str| env.get(var).cloned();
    let (press, presses) = interrupt::Presses::channel();
    let out: stage::Out = Rc::new(RefCell::new(PressAt {
        printed: printed.clone(),
        at,
        press: Some(Box::new(press)),
    }));
    let hold: stage::Interrupt = Box::pin(std::future::pending());
    let ran = stage::run_places_until(&opts, &places, &lookup, out, hold, presses).await;
    let printed = printed.text();
    let ev = stopped(ran, &printed);
    assert!(!ev.drops.is_empty(), "{printed}");
    Dropped {
        dir,
        _removed: removed,
        venue,
        ev,
        printed,
    }
}

/// A place-stage run against `model`, under a directory of its own, in which `act` is done to the
/// model as the probe prints `at`: ordered by what the probe did, whatever the machine's load.
async fn acted_at(
    model: QfexModel,
    what: &str,
    at: &'static str,
    act: fn(&mut QfexModel),
) -> Dropped {
    let (dir, removed) = temp_dir(what);
    let venue = Venue::start(model).await;
    let (mut opts, places) = parsed(&drop_argv(&dir, &venue)).unwrap();
    opts.rate_user = user();
    let env = environment();
    let printed = venue.printed();
    let lookup = |var: &str| env.get(var).cloned();
    let model = std::sync::Arc::clone(&venue.model);
    let out: stage::Out = Rc::new(RefCell::new(PressAt {
        printed: printed.clone(),
        at,
        press: Some(Box::new(move || act(&mut model.lock().unwrap()))),
    }));
    let hold: stage::Interrupt = Box::pin(std::future::pending());
    let presses = interrupt::Presses::none();
    let ran = stage::run_places_until(&opts, &places, &lookup, out, hold, presses).await;
    let printed = printed.text();
    let ev = stopped(ran, &printed);
    assert!(!ev.drops.is_empty(), "{printed}");
    assert!(printed.contains(at), "{printed}");
    Dropped {
        dir,
        _removed: removed,
        venue,
        ev,
        printed,
    }
}

/// Whether the run's journal holds an outbound `cancel_order` naming the venue id `id`.
fn cancel_names(d: &Dropped, id: &str) -> bool {
    let journal = d.dir.join("evidence").join("journal");
    let reader = fbc_journal::JournalReader::open(&journal, 0).unwrap();
    reader.entries().any(|e| match e.unwrap().record {
        fbc_journal::Record::Outbound { frame, .. } => {
            let text = String::from_utf8_lossy(frame.bytes()).into_owned();
            text.contains("cancel_order") && text.contains(id)
        }
        _ => false,
    })
}

#[tokio::test]
async fn an_arm_refused_in_drop_session_two_ends_it_marked_with_x_left_resting() {
    // Drop session two's arm is refused: the session's connection ends with it, a stop, Y not
    // placed and nothing cancelled. X still rests, reported for the hand cancel, and the stop's
    // marker is written, so the next run's --reconciled hand-cancel is honoured (0151 item 6).
    let model = QfexModel {
        arm_err_on: Some(1),
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-two-arm-refused").await;
    assert_stopped(&d, places::Stop::ConnectionEnded);
    let two = d.two();
    assert_eq!(two.arm.as_deref(), Some("Rejected"));
    assert!(!two.y.submitted && two.resting, "{two:?}");
    assert!(d.drop_run().verdict.is_none());
    assert_eq!((d.count("add_order"), d.count("cancel_order")), (1, 0));
    let model = d.venue.model.lock().unwrap();
    assert_eq!(model.orders[0].state, model::State::Open);
    drop(model);
    assert!(
        d.printed
            .contains(&format!("RESTING {}", places::HAND_CANCEL_ORDERS))
    );
    assert_eq!(d.ev.drops.len(), 1);
}

#[tokio::test]
async fn a_failure_in_drop_session_two_that_is_no_stop_marks_x_left_resting() {
    // The touch is crossed by the time Y is priced: a failure that is no stop, Y not placed, a
    // graceful close with no cancel. X still rests, so the reads after the session add their
    // stop, LeftResting, marked for the hand cancel the report asks for (0151 item 6).
    let model = QfexModel {
        crossed_on_place: Some(0),
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-two-failed").await;
    assert_stopped(&d, places::Stop::LeftResting);
    let two = d.two();
    let error = two.error.clone().unwrap();
    assert_eq!(error, market::CROSSED);
    assert_eq!(two.arm.as_deref(), Some("Accepted"));
    assert!(!two.y.submitted && two.resting, "{two:?}");
    assert_eq!(two.ended_by, Some(stage::GRACEFUL));
    assert!(d.drop_run().verdict.is_none());
    assert_eq!((d.count("add_order"), d.count("cancel_order")), (1, 0));
    let model = d.venue.model.lock().unwrap();
    assert_eq!(model.orders[0].state, model::State::Open);
    drop(model);
    let stopped = d.ev.stopped.clone().unwrap();
    assert!(stopped.starts_with(&error), "{stopped}");
    assert!(
        stopped.contains("; LeftResting after drop session two"),
        "{stopped}"
    );
    assert!(
        d.printed
            .contains(&format!("RESTING {}", places::HAND_CANCEL_ORDERS))
    );
    assert_eq!(d.ev.drops.len(), 1);
}

#[tokio::test]
async fn a_trade_naming_x_after_a_failed_drop_session_two_stops_not_flat() {
    // As a_failure_in_drop_session_two_that_is_no_stop_marks_x_left_resting, but X fills as
    // its trades are read after the account reads that showed every account flat: X's trades
    // are read after a failure that is no stop, as after drop session one, so the run stops not
    // flat, marked, the position the owner's (0151 item 9), and not LeftResting.
    let model = QfexModel {
        crossed_on_place: Some(0),
        fill_on_trades_page: Some(1),
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-two-failed-fill").await;
    assert_stopped(&d, places::Stop::NotFlat);
    let two = d.two();
    let error = two.error.clone().unwrap();
    assert_eq!(error, market::CROSSED);
    assert!(!two.y.submitted && two.resting, "{two:?}");
    assert!(two.after_drop.is_none(), "{two:?}");
    let after = two.after.as_ref().expect("X's trades were read");
    assert_eq!(after.naming, vec![1], "{after:?}");
    assert!(d.drop_run().verdict.is_none());
    assert_eq!((d.count("add_order"), d.count("cancel_order")), (1, 0));
    let stopped = d.ev.stopped.clone().unwrap();
    assert!(stopped.starts_with(&error), "{stopped}");
    assert!(
        stopped.contains("; NotFlat after drop session two"),
        "{stopped}"
    );
    let (report, json) = d.outputs();
    assert!(
        report.contains(
            "- X's and Y's trades after the session: 1 trade rows since X's send, 1 naming X, 0 \
             naming Y"
        ),
        "{report}"
    );
    assert_eq!(
        json["drop_tests"][0]["session_two"]["after"]["naming"],
        super::json("[1]")
    );
}

#[tokio::test]
async fn after_drop_session_two_a_full_page_still_counts_its_account_read() {
    // With no X to find after drop session two, the account counts as read when its read
    // ended, a full page included: on pages of one, Y still rests after the drop, so the
    // account's last page is full, and the evidence says it was read, Y resting.
    let model = QfexModel {
        cod_reach: model::Reach::Earlier,
        ..QfexModel::default()
    };
    let paged = |argv| set(short_agree(argv), "--page-size", "1");
    let d = dropped_with(model, "drop-two-full-page", paged).await;
    assert_stopped(&d, places::Stop::DropFailed);
    let two = d.two();
    assert!(two.resting && two.read, "{two:?}");
    let (report, json) = d.outputs();
    let session_two = &json["drop_tests"][0]["session_two"];
    assert_eq!(session_two["read_after"], true);
    assert_eq!(session_two["resting"], true);
    assert!(
        report.contains(&format!(
            "- An order resting on the account when the run ended: {}",
            places::HAND_CANCEL_ORDERS
        )),
        "{report}"
    );
}

#[test]
fn the_reads_after_drop_session_two_stop_on_the_first_that_applies() {
    use places::Stop;
    let refused = Some(Stop::RateRefused {
        on: places::RefusedOn::Rest,
    });
    let after = "after drop session two";
    let during = "during the reads after drop session two";
    let cases = [
        (
            (refused, true, true, true),
            refused.map(|s| (s, "in the reads after drop session two")),
        ),
        ((None, true, true, true), Some((Stop::Interrupted, during))),
        ((None, false, true, true), Some((Stop::NotFlat, after))),
        ((None, false, false, true), Some((Stop::LeftResting, after))),
        ((None, false, false, false), None),
    ];
    for ((refused, pressed, not_flat, resting), want) in cases {
        let got = stage::read_stop(refused, pressed, not_flat, resting);
        assert_eq!(got, want, "{refused:?} {pressed} {not_flat} {resting}");
        // Each is marked (0151 item 6).
        assert!(got.is_none_or(|(stop, _)| stop.marks()));
    }
}

#[tokio::test]
async fn ctrl_c_in_drop_session_two_cancels_x_and_y_by_venue_id_and_fails_it() {
    // The owner presses Ctrl-C as the read before the drop is printed, its socket closed and the
    // drop next: the press is taken before the drop, so X and Y are each cancelled by venue id,
    // seen to end, and the session closes gracefully, with no drop and no verdict (0151 item 2).
    let d = pressed_at(
        QfexModel::default(),
        "drop-two-ctrl-c",
        "READ before the drop",
    )
    .await;
    assert_stopped(&d, places::Stop::Interrupted);
    let two = d.two();
    assert_eq!(two.before_drop, Some([true, true]));
    let cancels = two.cancels.as_ref().expect("Ctrl-C cancels");
    assert_eq!(
        (cancels.sent, cancels.held, cancels.ended),
        (2, false, true)
    );
    assert_eq!(two.ended_by, Some(stage::GRACEFUL));
    assert!(two.after_drop.is_none() && d.drop_run().verdict.is_none());
    assert_eq!(d.count("cancel_order"), 2);
    let model = d.venue.model.lock().unwrap();
    let ids: Vec<String> = model.orders.iter().map(|o| o.id.clone()).collect();
    assert_eq!(ids.len(), 2);
    assert!(
        model
            .orders
            .iter()
            .all(|o| o.state == model::State::Cancelled)
    );
    drop(model);
    for id in &ids {
        assert!(
            cancel_names(&d, id),
            "a cancel names each order by its venue id"
        );
    }
    assert!(
        d.what().iter().all(|w| !w.contains(" drop ")),
        "{:?}",
        d.what()
    );
    assert!(
        d.ev.stopped
            .as_deref()
            .unwrap()
            .starts_with("Interrupted in drop session two"),
        "{:?}",
        d.ev.stopped
    );
    assert_eq!(d.ev.drops.len(), 1);
}

#[tokio::test]
async fn a_fill_of_y_in_drop_session_two_stops_with_cancels_not_flat() {
    // Y fills whole as a maker right after its ACK, the session open: a fill of ours, so X, the
    // one order of ours still open, is cancelled by its venue id, the session closes gracefully
    // with no drop, and the reads after it show the position: a stop not flat (0151 items 2 and
    // 9; FBC-ou9g adds the flatten).
    let model = QfexModel {
        on_ack: Some((1, model::OnAck::MakerFill)),
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-two-fill-y").await;
    assert_stopped(&d, places::Stop::UnexpectedFill);
    let two = d.two();
    assert!(two.y.submitted, "{two:?}");
    let cancels = two.cancels.as_ref().expect("a fill of ours cancels");
    assert_eq!(
        (cancels.sent, cancels.held, cancels.ended),
        (1, false, true)
    );
    assert_eq!(two.ended_by, Some(stage::GRACEFUL));
    assert!(d.drop_run().verdict.is_none());
    assert_eq!(d.count("cancel_order"), 1);
    // The model held the read pages from Y's fill until the session wrote again: the cancel the
    // fill's stop sent released them, so the comparison after it agreed, its connection still
    // open (FBC-77r8).
    let compared: Vec<_> = (two.compared.iter())
        .map(|c| c.first_agreed.is_some())
        .collect();
    assert_eq!(compared, [true], "{:?}", two.compared);
    let model = d.venue.model.lock().unwrap();
    let states: Vec<model::State> = model.orders.iter().map(|o| o.state).collect();
    assert_eq!(states, [model::State::Cancelled, model::State::Filled]);
    let x = model.orders[0].id.clone();
    drop(model);
    assert!(cancel_names(&d, &x), "the cancel names X by its venue id");
    let after = (d.ev.reads.iter())
        .find(|r| r.before == refusals::AFTER_DROP_TWO && r.label == "primary (drop)")
        .unwrap();
    assert!(!after.flat);
    assert!(
        d.ev.stopped
            .as_deref()
            .unwrap()
            .starts_with("UnexpectedFill in drop session two"),
        "{:?}",
        d.ev.stopped
    );
}

#[tokio::test]
async fn y_ended_before_the_drop_fails_the_drop_test() {
    // QFEX cancels Y itself right after its ACK, on the open connection: the reads after the
    // drop show X and Y absent, untraded and ended, but the drop had no Y of its own to cancel,
    // so the test fails (0151 item 13). Y's CANCELLED is a frame written after its ACK, which
    // the kernel can hold, and the session writes nothing before the drop, so whether the
    // session heard it by the drop is not ordered. The read before the drop is: made after Y's
    // ACK, on a model that ended Y as it acked it, it never lists Y, so the test fails by that
    // read (`YNotListedBeforeDrop`), with `YEndedBeforeDrop` beside it only when the session
    // heard the CANCELLED first.
    let model = QfexModel {
        on_ack: Some((1, model::OnAck::VenueCancel)),
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-y-ended").await;
    assert_stopped(&d, places::Stop::DropFailed);
    let two = d.two();
    assert!(two.y.acked, "{two:?}");
    assert_eq!(two.before_drop, Some([true, false]), "{two:?}");
    assert!(!two.x_cancelled_before_drop);
    assert_eq!(two.ended_by, Some(stage::DROPPED));
    let verdict = d.verdict();
    use verdict::Failure::{YEndedBeforeDrop, YNotListedBeforeDrop};
    assert!(
        verdict.failures.contains(&YNotListedBeforeDrop),
        "{verdict:?}"
    );
    let heard = |f: &verdict::Failure| matches!(f, YEndedBeforeDrop | YNotListedBeforeDrop);
    assert!(verdict.failures.iter().all(heard), "{verdict:?}");
    assert_eq!(
        verdict.failures.contains(&YEndedBeforeDrop),
        !two.y_open_at_drop
    );
    assert_eq!(verdict.reach, verdict::Reach::Cancelled);
    let (report, json) = d.outputs();
    assert!(
        report.contains(
            "Y rested, but the read before the drop did not list it, so the drop's cancel of Y \
             is not shown"
        ),
        "{report}"
    );
    assert!(
        report.contains("- The read before the drop: X listed yes; Y listed no"),
        "{report}"
    );
    assert_eq!(
        json["drop_tests"][0]["session_two"]["before_drop"],
        serde_json::json!({"x_listed": true, "y_listed": false})
    );
    assert_eq!(d.ev.drops.len(), 1);
}

#[tokio::test]
async fn a_late_cancelled_of_x_in_the_arms_window_is_reported_and_leaves_the_reach_unmeasured() {
    // The arm cancels X and QFEX reports it late, once the resync ended, with Y's answer before
    // its ACK: still in the arm's window, which ends with the read before the drop, so it is
    // reported, the drop is not credited with X, and nothing stops the run.
    let model = QfexModel {
        arm_cancels_open: model::ArmCancels::Late,
        ..QfexModel::default()
    };
    let d = dropped_in(model, "drop-arm-late", &primary_only(), |a| a).await;
    let two = d.two();
    assert!(
        two.x_cancel_reported && two.x_cancelled_before_drop,
        "{two:?}"
    );
    assert!(two.y_open_at_drop, "{two:?}");
    assert_eq!(two.before_drop, Some([false, true]));
    let verdict = d.verdict();
    assert_eq!(verdict.arm, verdict::ArmCancel::Reported);
    assert_eq!(verdict.reach, verdict::Reach::NotMeasured);
    assert!(verdict.passed, "{verdict:?}");
    assert_eq!((d.ev.stop, d.marker()), (None, None));
    let two_conn = &d.session_conns()[1];
    let late = format!("arm {two_conn} cancels 1 reported late");
    assert!(logged(&d.venue.log(), &late), "{:?}", d.what());
}

#[tokio::test]
async fn the_drop_reaching_only_its_own_orders_fails_with_x_left_resting() {
    // Cancel-on-disconnect reaches only the dropped connection's own orders: Y is cancelled, X,
    // drop session one's, is not, which is what the drop test is there to show (decision 0132
    // item 4). The test fails, X is left resting for the owner's hand cancel, and no session
    // runs on the subaccount.
    let model = QfexModel {
        cod_reach: model::Reach::Own,
        ..QfexModel::default()
    };
    let d = dropped_with(model, "drop-reach-own", short_agree).await;
    assert_stopped(&d, places::Stop::DropFailed);
    let verdict = d.verdict();
    let x = verdict::Which::X;
    assert_eq!(
        verdict.failures,
        [
            verdict::Failure::Listed(x),
            verdict::Failure::NotTerminal(x)
        ]
    );
    assert_eq!(verdict.reach, verdict::Reach::NotShown);
    let model = d.venue.model.lock().unwrap();
    let states: Vec<model::State> = model.orders.iter().map(|o| o.state).collect();
    assert_eq!(states, [model::State::Open, model::State::Cancelled]);
    drop(model);
    assert!(d.two().resting);
    assert!(
        d.printed
            .contains(&format!("RESTING {}", places::HAND_CANCEL_ORDERS))
    );
    assert_eq!(d.count("cancel_order"), 0);
    assert_eq!(d.ev.drops.len(), 1);
    assert!(d.ev.sessions.iter().all(|s| s.account == "primary"));
    assert_eq!(d.session_conns().len(), 2);
}

#[test]
fn the_other_named_accounts_stay_clean_only_when_every_read_of_them_is() {
    let read = |label: &str, page_orders: usize| capture::AccountRead {
        label: label.to_owned(),
        flat: true,
        page_orders,
        ..capture::AccountRead::default()
    };
    let named = [Armed::Primary, Armed::Subaccount];
    let clean = [read("primary (drop)", 1), read("subaccount", 0)];
    assert_eq!(
        stage::others_clean(&clean, &named, Armed::Primary),
        Some(true)
    );
    // The account's own reads are not the others': X listed there is no fault of theirs.
    let dirty = [read("primary (drop)", 0), read("subaccount", 1)];
    assert_eq!(
        stage::others_clean(&dirty, &named, Armed::Primary),
        Some(false)
    );
    assert_eq!(
        stage::others_clean(&dirty, &named, Armed::Subaccount),
        Some(true)
    );
    assert_eq!(
        stage::others_clean(&dirty, &named[..1], Armed::Primary),
        None
    );
}

#[test]
fn a_trade_rows_member_names_are_kept_as_names_alone() {
    for name in ["trade_id", "fee", "order_type", "x1"] {
        assert_eq!(places::member_name(name), name);
    }
    for odd in ["", "Trade_Id", "trade id", "trade-id", &"a".repeat(33)] {
        assert_eq!(places::member_name(odd), "(not a member name)", "{odd}");
    }
}

#[test]
fn the_drop_tests_report_says_each_case_with_no_id_or_value() {
    use places::{DropRun, Traded, TwoRun};
    // Drop session two that stopped before its drop, X's cancel reported by its arm, the reads
    // after it not made and the account unread.
    let stopped = DropRun {
        account: "primary",
        x_before_two: None,
        two: Some(TwoRun {
            x_cancel_reported: true,
            ..TwoRun::default()
        }),
        stop: Some("SpanBound"),
        ..DropRun::default()
    };
    let mut s = String::new();
    places::drop_markdown(&mut s, &stopped, false);
    for said in [
        "- What answers the arm: drop session one none reported; drop session two none reported",
        "- Whether enabling cancel-on-disconnect cancels open orders, and whether QFEX reports \
         it: yes, reported: a CANCELLED of X came in the arm's window",
        "- The drop test: not judged: the drop test ended before its drop",
        "- The drop's reach over X, an order of an earlier connection: not measured: the drop \
         test ended before its drop",
        "- Whether the other named accounts stayed clean: no other account is named",
        "- terminal_status on GET /user/historic-orders: X not listed; Y not listed",
        "- What followed: the run's stop, SpanBound: nothing more is placed on this account",
        "- X listed by the read before drop session two: not read",
        "- The read before the drop: not made",
        "- The reads after the drop: not made: the session ended short of its drop",
        "- X's and Y's trades after the drop: not read",
        "- An order resting on the account when the run ended: not known: the account was not \
         read after the session",
        "- Ended by: it did not open",
    ] {
        assert!(s.contains(said), "{said}: {s}");
    }
    // Without drop session two, enabling is not measured; another account read unclean is
    // said so; a comparison after the stop's cancels is tabled.
    let one = DropRun {
        account: "subaccount",
        others_clean: Some(false),
        ..DropRun::default()
    };
    let mut s = String::new();
    places::drop_markdown(&mut s, &one, false);
    assert!(
        s.contains("not measured: drop session two did not reach its drop"),
        "{s}"
    );
    assert!(
        s.contains("- Whether the other named accounts stayed clean: no:"),
        "{s}"
    );
    assert!(
        s.contains(&format!("- What followed: {}", places::NEXT_ACCOUNT)),
        "{s}"
    );
    assert!(!s.contains("### Drop session two"), "{s}");
    // The reads after the drop cut short by a refusal, its trades read in part.
    let cut = DropRun {
        account: "primary",
        two: Some(TwoRun {
            after_drop: Some(verdict::AfterDrop {
                reads: vec![(Duration::ZERO, verdict::Shown::Refused)],
                refused: Some(places::RefusedOn::Rest),
                ..verdict::AfterDrop::default()
            }),
            after: Some(Traded {
                trades: 2,
                naming: vec![0, 1],
                terminal: vec![Some("CANCELLED".to_owned()), None],
                error: Some("GET /user/historic-orders: HTTP 500".to_owned()),
                ..Traded::default()
            }),
            compared: vec![compare::Compared {
                step: "the stop's cancels",
                reads: Vec::new(),
                first_agreed: None,
                ended: compare::Ended::Unread,
                stop: None,
            }],
            ..TwoRun::default()
        }),
        ..DropRun::default()
    };
    let mut s = String::new();
    places::drop_markdown(&mut s, &cut, true);
    for said in [
        "- The reads after the drop: 1 reads; none listed neither X nor Y; cut short by a \
         provider rate refusal (decision 0148 item 8)",
        "- X's and Y's trades after the drop: 2 trade rows since X's send, 0 naming X, 1 naming \
         Y; not read whole: GET /user/historic-orders: HTTP 500",
        "- terminal_status on GET /user/historic-orders: X CANCELLED; Y not listed",
        &format!("- What followed: {}", places::LAST_ACCOUNT),
        "| the stop's cancels | 0 | none | unread: no read was made |",
    ] {
        assert!(s.contains(said), "{said}: {s}");
    }
    let json = places::drop_to_json(&cut);
    assert_eq!(json["session_two"]["after"]["naming"], super::json("[0,1]"));
    assert_eq!(
        json["session_two"]["compared"][0]["ended"],
        "unread: no read was made"
    );
    assert_eq!(json["verdict"], Value::Null);
}

#[test]
fn a_later_session_watches_only_what_follows_it() {
    use fbc_runtime::ExecHandler;
    // An earlier session's fill refused for its time, noted on the Link and raised there.
    let (mut handler, link) = handled(100);
    handler.on_exec(fill_env(1, qfex_fill(our_cid(), T1, true), None));
    assert!(handler.watched.borrow().stop().is_some());
    // A later session's handler on the same Link takes none of it for its own.
    let later = places::Watched::after(&link.borrow());
    let watched = Rc::new(RefCell::new(later));
    let mut next = places::PlaceHandler {
        inner: link::LinkHandler {
            link: Rc::clone(&link),
            wake: Rc::new(tokio::sync::Notify::new()),
        },
        watched: Rc::clone(&watched),
        control: Rc::default(),
    };
    next.on_decode_error(conn(0));
    let seen = watched.borrow();
    assert!(seen.stop().is_none() && !seen.fill_of_ours, "{seen:?}");
    assert!(!seen.authenticated && seen.arm.is_none() && !seen.resynced);
}

// ----- FBC-77r8: FBC-23ch's review fixes (PR #272), carried into the drop test -----

/// Asserts `d` stopped `RateRefused` on a read socket, marked and named, `when` it was met,
/// with the last socket labelled `label` showing one refusal, in a frame kept whole.
fn assert_refused_whole_at(d: &Dropped, when: &str, label: &str) {
    let stop = places::Stop::RateRefused {
        on: places::RefusedOn::ReadSocket,
    };
    let name = stop.name();
    assert_eq!(d.ev.stop, Some(name), "{}", d.printed);
    let marker = d.marker().unwrap_or_default();
    assert!(marker.starts_with(&format!("stop: {name}\n")), "{marker}");
    let said = d.ev.stopped.as_deref().unwrap();
    assert!(said.contains(&format!("{name} {when}")), "{said}");
    assert_report_and_evidence_pass_the_privacy_check(&d.dir.join("evidence"));
    assert_no_secret(&d.dir.join("evidence"));
    d.venue.assert_served();
    let socket = (d.ev.read_sockets.iter()).rev().find(|r| r.label == label);
    let refusals = socket
        .unwrap_or_else(|| panic!("{label}: {}", d.printed))
        .refusals;
    let shown = (refusals.errs, refusals.unread, refusals.whole);
    assert_eq!(shown, (0, 1, 1), "{refusals:?}");
}

#[tokio::test]
async fn a_refusal_kept_whole_in_the_read_before_drop_session_two_stops_the_run() {
    // PR #272's thread 4605918671, in the read before drop session two: the primary's read
    // socket, the fifth to authenticate after X (the comparison's, the two reads after drop
    // session one and X's own read first), is answered a RateLimited err echoing it, which the
    // journal keeps whole, so the adapter reads no hold from it: a provider rate refusal all the
    // same (decision 0135 item 2), never an account left unread with X resting.
    let model = QfexModel {
        rate_limit: Some(model::Limited::ReadAuthEcho(4)),
        ..QfexModel::default()
    };
    let d = dropped(model, "before-two-rate-whole").await;
    assert_refused_whole_at(&d, "in the read before drop session two", "primary (drop)");
    assert_no_cancel_and_x_resting(&d);
    assert!(reads_before_two(&d).is_empty(), "{}", d.printed);
    assert!(d.drop_run().two.is_none());
    assert_eq!(d.count("add_order"), 1);
}

#[tokio::test]
async fn a_refusal_kept_whole_in_the_read_before_the_drop_stops_drop_session_two() {
    // The same err on the read before the drop, the eighth socket to authenticate after X
    // (drop session two's own among them): a stop in drop session two that sends no cancel, so
    // the session ends by a graceful close, no drop, X and Y left resting for the owner.
    let model = QfexModel {
        rate_limit: Some(model::Limited::ReadAuthEcho(7)),
        ..QfexModel::default()
    };
    let d = dropped(model, "before-drop-rate-whole").await;
    let label = "primary (the read before the drop)";
    assert_refused_whole_at(&d, "in drop session two", label);
    let two = d.two();
    assert_eq!(two.before_drop, None);
    assert_eq!(
        (two.ended_by, two.cancels.as_ref()),
        (Some(stage::GRACEFUL), None)
    );
    assert!(two.after_drop.is_none() && d.drop_run().verdict.is_none());
    assert_eq!(d.count("cancel_order"), 0);
    assert!(
        d.what().iter().all(|w| !w.contains(" drop ")),
        "{:?}",
        d.what()
    );
    let model = d.venue.model.lock().unwrap();
    assert_eq!(model.orders.len(), 2);
    assert!(model.orders.iter().all(|o| o.state == model::State::Open));
}

#[tokio::test]
async fn a_refusal_kept_whole_in_the_reads_after_the_drop_fails_the_drop_test() {
    // The same err on the first of the reads after the drop (verdict::until_absent), the ninth
    // socket to authenticate after X: the reads end there, refused, and the verdict's stop is
    // the provider rate refusal, marked (decision 0148 item 8), never a pass on a later read.
    let model = QfexModel {
        rate_limit: Some(model::Limited::ReadAuthEcho(8)),
        ..QfexModel::default()
    };
    let d = dropped(model, "after-drop-rate-whole").await;
    let label = "primary (the reads after the drop)";
    assert_refused_whole_at(&d, "after the drop", label);
    let after = d
        .two()
        .after_drop
        .as_ref()
        .expect("the reads after the drop");
    assert_eq!(
        after.refused,
        Some(places::RefusedOn::ReadSocket),
        "{after:?}"
    );
    assert_eq!(after.reads.len(), 1, "{after:?}");
    assert!(!d.verdict().passed);
}

#[tokio::test]
async fn a_trade_naming_x_after_drop_session_one_stops_before_drop_session_two() {
    // PR #272's thread 4605918676, before drop session two: X fills as its trades are read after
    // drop session one, after the reads that showed every account flat. The run stops not flat,
    // marked, the position the owner's (0151 item 9), before the read before drop session two:
    // no account is read again, no second session opens, and Y is never placed.
    let model = QfexModel {
        fill_on_trades: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-fill-before-two").await;
    let after = d.drop_run().after.as_ref().unwrap();
    assert_eq!(after.trades_naming_x, 1, "{after:?}");
    assert_stopped(&d, places::Stop::NotFlat);
    let said = d.ev.stopped.as_deref().unwrap();
    assert!(said.starts_with("NotFlat after drop session one"), "{said}");
    assert!(reads_before_two(&d).is_empty(), "{}", d.printed);
    assert!(d.drop_run().two.is_none());
    assert_eq!(d.count("subscribe"), 3);
    assert_eq!((d.count("add_order"), d.count("cancel_order")), (1, 0));
}

#[tokio::test]
async fn x_s_reads_failing_after_drop_session_one_stop_before_drop_session_two() {
    // X's trades and historic orders read after drop session one, where GET
    // /user/historic-orders is answered HTTP 500, a failure that is no provider rate refusal:
    // X is left unjudged, so the run stops LeftResting, marked, X still resting for the owner's
    // hand cancel, before the read before drop session two: no account is read again, no second
    // session opens, and Y is never placed.
    let model = QfexModel {
        historic_fails: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-x-unread-before-two").await;
    let after = d.drop_run().after.as_ref().unwrap();
    assert_eq!(after.trades_naming_x, 0, "{after:?}");
    let error = after.error.as_deref().unwrap_or_default();
    assert!(error.starts_with("GET /user/historic-orders"), "{after:?}");
    assert_stopped(&d, places::Stop::LeftResting);
    let said = d.ev.stopped.as_deref().unwrap();
    assert!(
        said.starts_with("LeftResting reading X after drop session one"),
        "{said}"
    );
    assert!(d.printed.contains("RESTING "), "{}", d.printed);
    assert!(reads_before_two(&d).is_empty(), "{}", d.printed);
    assert!(d.drop_run().two.is_none());
    assert_eq!(d.count("subscribe"), 3);
    assert_eq!((d.count("add_order"), d.count("cancel_order")), (1, 0));
}

#[tokio::test]
async fn a_touch_that_moves_past_the_caps_before_y_sends_no_y() {
    // PR #272's thread 4605918689, at Y's place: both caps admit exactly three minimum sizes at
    // the preflight's touch near 100, where X is placed; at drop session two's arm the touch
    // moves near 400, where they admit none, while the registry's lot caps, fixed at the
    // preflight, still admit three. Y is not sent and never resized, and the run stops, marked,
    // its session ended by a graceful close with no cancel and no drop (decision 0113).
    let moved = model::Touch {
        bid: Some(("400.00", "1.00")),
        ask: Some(("400.10", "2.00")),
        age_secs: 0,
    };
    let model = QfexModel {
        touch_moves_at_arm: Some((1, moved)),
        ..QfexModel::default()
    };
    let caps = |argv| {
        let argv = set(argv, "--resting-cap-usd", THREE_MIN_USD);
        set(argv, "--inventory-cap-usd", THREE_MIN_USD)
    };
    let d = dropped_with(model, "drop-caps-moved-y", caps).await;
    assert_eq!(d.count("add_order"), 1, "{:?}", d.what());
    assert!(d.drop_run().x.submitted);
    let two = d.two();
    assert!(!two.y.submitted, "{two:?}");
    assert!(!d.printed.contains("PLACE Y"), "{}", d.printed);
    assert_stopped(&d, places::Stop::BeyondCaps);
    let said = d.ev.stopped.as_deref().unwrap();
    assert!(said.starts_with("BeyondCaps in drop session two"), "{said}");
    assert_eq!(
        (two.ended_by, two.cancels.as_ref()),
        (Some(stage::GRACEFUL), None)
    );
    assert_eq!(d.count("cancel_order"), 0);
}

#[tokio::test]
async fn a_fill_of_ours_while_drop_session_twos_close_waits_for_room_cancels_x_and_y_first() {
    // The twin above's, in drop session two: the read before the drop is answered a RateLimited
    // err holding the shared budget for 3 s, a stop that sends no cancel, so the session's
    // graceful close waits for room; as the probe prints that refusal, while the close waits,
    // half of Y fills as a maker, the rest resting (ordered by that line, not by a timer): a
    // fill of ours, so the session cancels (0151 item 2), once there is room and before the
    // close: Kill, X and Y's rest cancelled by their venue ids and seen to end, the disarm. The
    // refusal stays the run's stop. Y is ten lots of 0.001 here, so half of it can fill.
    let model = QfexModel {
        listed: vec![model::Listed {
            lot: "0.001",
            ..model::Listed::trading()
        }],
        rate_limit: Some(model::Limited::ReadSocket(5)),
        ..QfexModel::default()
    };
    let refused = "READ before the drop: primary: refused by a provider rate refusal";
    let d = acted_at(
        model,
        "drop-two-fill-at-close",
        refused,
        QfexModel::part_fill_last,
    )
    .await;
    let stop = places::Stop::RateRefused {
        on: places::RefusedOn::ReadSocket,
    };
    assert_stopped(&d, stop);
    let said = d.ev.stopped.as_deref().unwrap();
    assert!(
        said.starts_with("RateRefused(ReadSocket) in drop session two"),
        "{said}"
    );
    let two = d.two();
    assert_eq!(two.before_drop, None);
    let cancels = two.cancels.as_ref().expect("a fill of ours cancels");
    assert_eq!(
        (cancels.sent, cancels.held, cancels.ended),
        (2, false, true)
    );
    assert_eq!(two.ended_by, Some(stage::GRACEFUL));
    assert!(two.after_drop.is_none() && d.drop_run().verdict.is_none());
    assert_eq!(d.count("cancel_order"), 2, "{:?}", d.what());
    assert!(
        d.what().iter().all(|w| !w.contains(" drop ")),
        "{:?}",
        d.what()
    );
    let cancelled = d.at(
        "STOP cancels: Kill, then 2 cancels by venue id (0151 item 2), every order they named \
         seen to end; the market disarmed",
    );
    assert!(cancelled > d.at("READ before the drop: primary: refused by a provider rate refusal"));
    let model = d.venue.model.lock().unwrap();
    assert_eq!(model.fills.len(), 1);
    assert!(
        model
            .orders
            .iter()
            .all(|o| o.state == model::State::Cancelled)
    );
    let at = |what: &str| model.log.iter().rposition(|e| e.what.contains(what));
    let (cancel, close) = (at(" cancel_order").unwrap(), at("cod ").unwrap());
    assert!(cancel < close, "{:?}", model.log);
}

#[tokio::test]
async fn a_press_while_drop_session_twos_close_waits_for_room_cancels_x_and_y_first() {
    // PR #272's thread 4605918659, in drop session two: the read before the drop is answered a
    // RateLimited err holding the shared budget for 3 s, a stop that sends no cancel, so the
    // session's graceful close waits for room; the owner presses Ctrl-C as that refusal is
    // printed, so the press arrives while the close waits. It is taken before the close: once
    // there is room, Kill, X and Y cancelled by their venue ids and seen to end, the disarm,
    // then the graceful close (0151 items 2 and 11). The refusal stays the run's stop.
    let model = QfexModel {
        rate_limit: Some(model::Limited::ReadSocket(5)),
        ..QfexModel::default()
    };
    let at = "READ before the drop: primary: refused by a provider rate refusal";
    let d = pressed_at(model, "drop-two-press-at-close", at).await;
    let stop = places::Stop::RateRefused {
        on: places::RefusedOn::ReadSocket,
    };
    assert_stopped(&d, stop);
    let said = d.ev.stopped.as_deref().unwrap();
    assert!(
        said.starts_with("RateRefused(ReadSocket) in drop session two"),
        "{said}"
    );
    let two = d.two();
    assert_eq!(two.before_drop, None);
    let cancels = two.cancels.as_ref().expect("the press cancels");
    assert_eq!(
        (cancels.sent, cancels.held, cancels.ended),
        (2, false, true)
    );
    assert_eq!(two.ended_by, Some(stage::GRACEFUL));
    assert!(two.after_drop.is_none() && d.drop_run().verdict.is_none());
    assert_eq!(d.count("cancel_order"), 2, "{:?}", d.what());
    assert!(
        d.what().iter().all(|w| !w.contains(" drop ")),
        "{:?}",
        d.what()
    );
    let cancelled = d.at(
        "STOP cancels: Kill, then 2 cancels by venue id (0151 item 2), every order they named \
         seen to end; the market disarmed",
    );
    assert!(cancelled > d.at(at));
    let model = d.venue.model.lock().unwrap();
    assert!(
        model
            .orders
            .iter()
            .all(|o| o.state == model::State::Cancelled)
    );
    let at = |what: &str| model.log.iter().rposition(|e| e.what.contains(what));
    let (cancel, close) = (at(" cancel_order").unwrap(), at("cod ").unwrap());
    assert!(cancel < close, "{:?}", model.log);
}

// ----- FBC-77r8: FBC-23ch's second review fixes carried into the drop test -----

#[tokio::test]
async fn a_failure_of_drop_session_one_with_an_order_still_resting_is_marked_left_resting() {
    // FBC-23ch's second review fix to drop_one's ending order, in drop_sessions: drop session one
    // fails with no stop, X not priced, and the reads after it show an order of ours still
    // resting (counted by REST, as a stop order would be). The failure comes after every stop
    // the reads show: LeftResting is marked for the owner's hand cancel, the run ends on the
    // session's failure, kept as its error, and drop session two never runs (0151 item 6).
    let model = QfexModel {
        stop_order_after_close: true,
        ..QfexModel::default()
    };
    let d = dropped_hooked(model, "drop-failed-resting", |t| t.x_unpriced = true).await;
    let name = places::Stop::LeftResting.name();
    assert_eq!(d.ev.stop, Some(name), "{}", d.printed);
    let marker = d.marker().unwrap_or_default();
    assert!(marker.starts_with(&format!("stop: {name}\n")), "{marker}");
    assert_eq!(
        d.ev.stopped.as_deref(),
        Some("a test's X could not be priced")
    );
    let run = d.drop_run();
    assert_eq!(run.error.as_deref(), Some("a test's X could not be priced"));
    assert!(
        run.started && !run.x.submitted && run.two.is_none(),
        "{run:?}"
    );
    assert_after_reads(&d);
    assert_eq!(d.count("add_order"), 0, "{:?}", d.what());
    d.venue.assert_served();
}

#[tokio::test]
async fn a_full_page_repeating_the_one_before_after_the_drop_fails_the_read_rather_than_paging_on()
{
    // FBC-23ch's second review fix to the history page loops, carried to X's and Y's reads after
    // drop session two: on pages of one, a venue answering every get_user_trades page as if
    // asked from offset 0 (a fill of Y at the drop), or every GET /user/historic-orders page
    // whole (X and Y cancelled by the drop), would be paged without end. The read fails at the
    // first repeat, so the verdict reads neither whole and the drop test does not pass.
    let historic = QfexModel::default();
    let trades = QfexModel {
        fill_on_drop: true,
        stuck_trades: true,
        ..QfexModel::default()
    };
    let cases = [
        (historic, "GET /user/historic-orders"),
        (trades, "get_user_trades"),
    ];
    for (model, what) in cases {
        let run = dropped_with(model, "drop-two-repeated-page", |argv| {
            set(argv, "--page-size", "1")
        });
        let d = tokio::time::timeout(Duration::from_secs(60), run)
            .await
            .expect("the run ended");
        let two = d.two();
        assert_eq!(two.ended_by, Some(stage::DROPPED), "{two:?}");
        let after = two.after.as_ref().expect("X and Y were read");
        let why = format!("a full {what} page repeated the page before it");
        assert_eq!(after.error.as_deref(), Some(why.as_str()), "{after:?}");
        let verdict = d.verdict();
        assert!(!verdict.passed, "{verdict:?}");
        assert!(d.ev.stop.is_some(), "{}", d.printed);
        d.venue.assert_served();
    }
}
