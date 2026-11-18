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
//!
//! FBC-su9i's done line (decisions 0005, 0132 item 7, 0148 items 2, 6 and 8, 0151 items 5, 9 and
//! 11): stage two's crossing post-only, against the model's trade sockets, and its judgments at
//! function level (with the helpers and the rest in `crossing.rs`):
//!
//! - the exact-echo gate open on an exact ACK echo in this run and shut for upper-cased,
//!   escaped, absent, null and empty client ids, an echo's exactness recorded but never opening it
//!   (`an_exact_echo_opens_the_gate`, `an_upper_cased_or_escaped_echo_does_not_open_the_gate`);
//!   with it shut, by an upper-cased or escaped echo, every account's drop test runs and the run
//!   ends NoEcho, no marker, while an ACK whose client id is absent, null or empty names no order
//!   of ours and stops the run a stray's in drop session one, before the gate is judged (0151
//!   item 13, which wins over the ticket's NoEcho for these three)
//!   (`an_absent_echo_ends_the_run_after_every_accounts_drop_test`); the gate per run, a second
//!   run on the same lease directory judging its own
//!   (`an_open_gate_is_not_carried_into_a_second_run`);
//! - the crossing post-only refused CANCELLED, by an unmapped status and by an err echo, each
//!   recorded and reported, the last two leaving its place Unknown, counted resting, a stop
//!   (`the_crossing_post_only_is_refused_and_recorded`); an Unknown place never resent and
//!   reported (`an_unknown_place_is_never_resent_and_is_reported`); a fill a stop, a refused arm
//!   or a touch it cannot be priced from placing none;
//! - no crossing order beside another order of ours, on the account's registries or reads or on
//!   another named account's, judged at function level on hand-built registries and reads
//!   (`a_crossing_order_waits_for_no_other_order_of_ours`,
//!   `a_crossing_order_waits_for_no_order_of_ours_on_any_named_account`,
//!   `an_unknown_crossing_post_only_blocks_the_taker_ioc`); in a run, the one path a model's
//!   answer cannot reach, an order of ours held in the first account's cross registry, through a
//!   test's hook (`a_crossing_order_is_kept_back_in_a_run_while_another_order_of_ours_rests`),
//!   and an Unknown crossing post-only's stop keeping every later crossing order back
//!   (`an_unknown_place_is_never_resent_and_is_reported`,
//!   `the_crossing_post_only_is_refused_and_recorded`);
//!   the run's glue from the drop tests' reads to X and Y counted ended is not reached by a run
//!   whose drop test did not pass, since only such an account crosses;
//! - the session machinery: cancels by venue id before each crossing session stops, at Ctrl-C
//!   too, its span bound met exactly one `--rpc-timeout-ms` early on a paused clock and not a
//!   millisecond before, an order left resting a stop whether the registry counts it or only a
//!   read shows it, a named account not read after a crossing session a stop, and held cancels
//!   ending it by the abrupt drop after a passed drop test;
//! - the report's characters exercised, whether add_order was accepted as encoded, an upper-cased
//!   echo read "not byte for byte", and the places-production/ slice passing the privacy check.
//!
//! FBC-ou9g's flatten (decisions 0012, 0063, 0148 items 5, 6, 8 and 9, 0151 items 5, 8 and 9),
//! reached from every registry, run against the model, with its pure parts in `flatten_run.rs`:
//!
//! - a maker fill of X in drop session one, of X or Y in drop session two, and a crossing
//!   post-only that fills, each flattened while its session is open, before its Stop: every
//!   other order of ours cancelled by venue id and seen to end, the owner's Flatten into Exit,
//!   one reducing limit IOC at the minimum size with no reduce-only flag, within the cap of the
//!   touch it crosses, REST flat; then the run stops UnexpectedFill
//!   (`a_maker_fill_of_x_in_drop_session_one_is_flattened_then_stops`,
//!   `a_crossing_post_only_that_fills_is_flattened_then_stops` and the drop session two ones);
//! - each flatten stop, not flat and marked: no price within the cap (`Capped`), the budget used
//!   up after one or two expired IOCs (`UsedUp`), an err echoing the IOC, fbc-oms refusing the IOC
//!   or the owner's Flatten (`Refused`), a residual below the minimum after an IOC filled in part
//!   (`BelowMinimum`); an Unknown IOC never resent and keeping the next back; the account's one
//!   budget across its registries; a position frame before or after a fill changing nothing; a
//!   connection that ends, Ctrl-C, other orders not seen to end, no touch and a rate refusal
//!   during it.

use super::*;

use std::io::Write as _;
use std::str::FromStr as _;

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
pub(super) fn spec_of(listed: Listed) -> Discovered {
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
        "With --place-stages (FBC-c6kw, FBC-23ch, FBC-77r8, FBC-su9i and FBC-ou9g, slices of \
         FBC-9sk2; decisions 0148 and 0151) it runs the place stages' preflight, then decision \
         0132 item 4's drop test on each named account in turn, then stage two's crossing \
         post-only and taker IOC on each in turn",
        "only against a loopback test stub, every origin (trade, REST and MDS) a loopback address",
        "It prints a CAPS line with the environment and both caps first, before any request",
        "Drop session two: every named account read again, X alone admitted, as one open order \
         on its symbol",
        "then the abrupt drop of its connection, with no WebSocket close",
        "With no stop, stage two follows.",
        "With no stop the run ends after the last account's taker IOC, its outcome not ok, until \
         FBC-4plj builds stage three.",
        "it stops the run not flat with no flatten (IocRemainder; 0151 item 8, until FBC-jdj7)",
        "the key qfex.exec.production_places = probe_place_stages, the CodProbeRun declarations, \
         the fences met (never their values), --sole-trader and your confirmations as unchecked, \
         and every hand action",
        "A crossing or taker IOC session also cancels every order of ours by venue id before it \
         stops",
        "a place never answered, or refused by a status the adapter does not map or by an err \
         echo, ending Unknown, never resent and counted resting (decision 0005)",
        "an ACK whose client id is absent, null or empty names no order of ours, so it stops the \
         run in drop session one, StrayOrder, before the gate is judged",
        "a press during the preflight, the reads, a drop session, a crossing session or a \
         session's stop's cancels stops the run, Interrupted",
        "in a drop session or a crossing session it cancels by venue id first.",
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
            "# QFEX production place stages: the preflight, each named account's drop test and \
             stage two's crossing post-only and taker IOC\n"
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
    // FBC-77r8), then each one's crossing session (FBC-su9i) and its taker IOC session
    // (FBC-ou9g), and no stage-one registry was built.
    assert_eq!(ev.sessions.len(), 8);
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
        "then stage two (FBC-su9i): with the run's exact-echo gate open, each named account in \
         turn places its crossing post-only",
        "then, in a session of its own (FBC-ou9g), one limit IOC at the minimum size at the far \
         touch, whose fill is flattened through Exit before that session stops; with the \
         exact-echo gate shut the run ends NoEcho; the run ends after the last account's taker \
         IOC, not ok, until FBC-4plj builds stage three",
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
    assert!(report.contains(
        "the run ends after the last account's taker IOC: stage three is not built yet (FBC-4plj \
         builds it)"
    ));
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

#[test]
fn the_owners_flatten_takes_no_new_lease_on_an_armed_market() {
    // FBC-ou9g: on a market Started under its lease, the owner's Flatten moves it to Exit under
    // the lease the registry holds, taking none of its own, where a second take of that lease
    // would be refused (decision 0148 items 3 and 5).
    let found = spec_of(Listed::trading());
    let (opts, places) = parsed(&parse_only()).unwrap();
    let bounds = market::count_bound(opts.ledger.as_ref().unwrap(), &found, &places).unwrap();
    let caps = market::place_caps(&places, &found, QUOTE).unwrap();
    let order: OrderCaps = probe_caps().exec.unwrap().order;
    let keys = places::lease_keys(
        "primary",
        &order,
        market::INST,
        found.spec.venue_symbol.clone(),
    );
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
    let (mut link, _run) = built.into_link(order, ladder);
    let seeded = link
        .reg()
        .seed_position(market::INST, fbc_core::SignedLots(0));
    seeded.unwrap();
    let (dir, _removed) = temp_dir("place-flatten-armed");
    places::start(link.reg(), &dir, market::INST).unwrap();
    places::flatten(link.reg(), &dir, market::INST).unwrap();
    let entry = link.reg().entry(market::INST);
    assert!(entry.armed());
    let exit = fbc_oms::EntryState::Exit(fbc_oms::ExitKind::Flatten);
    assert_eq!(entry.state(), exit);
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
    // cancel: X and Y alone (FBC-77r8); then each one's crossing session, its crossing post-only
    // refused CANCELLED, so nothing to cancel (FBC-su9i); then each one's taker IOC session, its
    // taker IOC filled and flattened by one flatten IOC, so nothing to cancel either (FBC-ou9g).
    assert_eq!(d.count("subscribe"), 24);
    assert_eq!((d.count("add_order"), d.count("cancel_order")), (10, 0));
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
        "STOPPED stage three",
    ];
    for pair in lines.windows(2) {
        assert!(d.at(pair[0]) < d.at(pair[1]), "{pair:?}: {}", d.printed);
    }
    // X rested after the close, but the run went on to drop session two: no hand cancel, no
    // stop and no marker, the run ending after every account's taker IOC, naming FBC-4plj
    // (FBC-ou9g).
    assert!(!d.printed.contains("RESTING"), "{}", d.printed);
    assert!(d.marker().is_none() && d.ev.stop.is_none());
    let stopped = d.ev.stopped.clone().unwrap();
    assert_eq!(stopped, stage::NOT_BUILT);
    assert!(stopped.contains("FBC-4plj"), "{stopped}");
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
    assert_eq!(d.ev.sessions.len(), 8);
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
    // the cent grid; then each one's crossing post-only (FBC-su9i), at the minimum size too, at
    // the far touch, the ask of 100.10; then each one's taker IOC (FBC-ou9g), a limit IOC at the
    // minimum size at the far touch too, and its flatten IOC, a limit IOC at the minimum size on
    // the other side.
    assert_eq!(model.orders.len(), 10);
    for (n, x) in model.orders.iter().enumerate() {
        assert_eq!(x.quantity, Decimal::new(1, 2));
        let (kind, side) = match n {
            0..4 => (("ALO", "GTC"), "BUY"),
            4 | 7 => (("ALO", "GTC"), "BUY"),
            5 | 8 => (("LIMIT", "IOC"), "BUY"),
            _ => (("LIMIT", "IOC"), "SELL"),
        };
        assert_eq!((x.kind.as_str(), x.tif.as_str()), kind, "{n}");
        assert_eq!(x.side, side, "{n}");
        let price = match n {
            0..4 => Some("99.5"),
            4 | 5 | 7 | 8 => Some("100.1"),
            _ => None,
        };
        if let Some(price) = price {
            assert_eq!(x.price, super::json(price));
        }
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
    // the primary account's, then the subaccount's (FBC-77r8); then each crossing session and
    // taker IOC session, on its own account too (FBC-su9i, FBC-ou9g).
    let accounts: Vec<Option<&str>> = armed.iter().map(|(_, c)| c.account.as_deref()).collect();
    assert_eq!(
        accounts,
        [
            None,
            None,
            Some(SUB),
            Some(SUB),
            None,
            None,
            Some(SUB),
            Some(SUB)
        ]
    );
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
    // One drop registry per named account (FBC-77r8), then one cross registry (FBC-su9i) and
    // one take registry (FBC-ou9g) each, whose trip is given before its market arms too.
    let registries = &d.ev.place_registries;
    let steps: Vec<ledger::Step> = registries.iter().map(|r| r.step).collect();
    use ledger::Step::{Cross, Drop, Take};
    assert_eq!(steps, [Drop, Drop, Cross, Take, Cross, Take]);
    for registry in registries {
        assert_eq!(registry.declaration, ledger::PLACES_DECLARATION);
    }
    let start = "START the cross registry on primary";
    assert!(d.at("REGISTRY cross on primary") < d.at(start));
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
    // And each one's cross and take registries once each, after clean reads of every named
    // account (FBC-su9i, FBC-ou9g).
    assert_eq!(d.printed.matches("seeded flat").count(), 6);
    let cross = d.at("READ before the crossing session: subaccount");
    assert!(cross < d.at("REGISTRY cross on primary"));
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
    // Two sessions on each named account (FBC-77r8), then one crossing session (FBC-su9i) and
    // one taker IOC session (FBC-ou9g) each, one after the other.
    let sessions = model.conns.values().filter(|c| c.session).count();
    assert_eq!(sessions, 8);
    assert_eq!(model.conns.values().filter(|c| c.armed).count(), 8);
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
    // Y's ACK echoed exactly, so each crossing session placed its one crossing post-only too
    // (FBC-su9i), and each taker IOC session its taker IOC and the one flatten IOC its fill
    // needed (FBC-ou9g).
    assert_eq!(d.places_by_session(), [1, 1, 1, 1, 1, 2, 1, 2]);
    let run = d.drop_run();
    assert!(!run.x.acked && !run.x.exact_echo, "{run:?}");
    assert_eq!(run.x.outcome.as_deref(), Some("Unknown"));
    // And with the echo: still one a session, X or Y (FBC-77r8), then as above.
    let d = dropped(QfexModel::default(), "drop-one-place-echoed").await;
    assert_eq!(d.places_by_session(), [1, 1, 1, 1, 1, 2, 1, 2]);
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
    // A run spends its list: X and Y alone reached QFEX from the drop registries, once each on
    // each named account, one crossing post-only from each cross registry (FBC-su9i), and one
    // taker IOC and one flatten IOC from each take registry (FBC-ou9g).
    let d = dropped(QfexModel::default(), "drop-list").await;
    assert_eq!(d.count("add_order"), 10);
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
    // the drop session flattens it and cancels (0151 items 2 and 9), with nothing left to
    // cancel.
    let model = QfexModel {
        stray_fill: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-no-cid-fill").await;
    assert_stopped(&d, places::Stop::NotOurCid);
    let run = d.drop_run();
    // Counted on X by its venue id, it is a fill of ours the session flattens while open (0151
    // item 9), the run's stop still the first raised.
    assert_flattened(run.flatten.as_ref().expect("a flatten"), 0);
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
pub(super) fn handled(max_entries: usize) -> (places::PlaceHandler, Rc<RefCell<link::Link>>) {
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
pub(super) fn fill_env(
    n: u64,
    fill: fbc_core::FillEvent,
    exch: Option<i64>,
) -> Envelope<fbc_core::ExecEvent> {
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

/// As [`pressed_drop`], under `rate_user`, with one press of Ctrl-C once `ready` holds of the
/// venue, then the release of what the model held.
async fn pressed_once_when(
    model: QfexModel,
    what: &str,
    rate_user: RateUserKey,
    argv: impl Fn(Argv) -> Argv,
    ready: impl Fn(&Venue) -> bool,
) -> Dropped {
    let (dir, removed) = temp_dir(what);
    let venue = Venue::start(model).await;
    let (mut opts, places) = parsed(&argv(drop_argv(&dir, &venue))).unwrap();
    opts.rate_user = rate_user;
    let env = environment();
    let printed = venue.printed();
    let lookup = |var: &str| env.get(var).cloned();
    let out: stage::Out = Rc::new(RefCell::new(printed.clone()));
    let (mut press, presses) = interrupt::Presses::channel();
    let hold: stage::Interrupt = Box::pin(std::future::pending());
    let drive = async {
        until("the moment to press", || ready(&venue)).await;
        press();
        venue.release_held().await;
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
            .contains("STOP cancels: 1 press of Ctrl-C while they ran, recorded")
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
async fn a_maker_fill_of_x_in_drop_session_one_is_flattened_then_stops() {
    // After X's ACK, X fills whole as a maker: a fill of ours other than the taker IOC's and the
    // flatten IOCs', so, the session still open, the flatten (0151 item 9): no other order of
    // ours to cancel, the owner's Flatten into Exit, one reducing limit IOC at the minimum size
    // within the cap, filled against the bid, REST flat; then the session's Stop and a graceful
    // close, and the run stops UnexpectedFill, flat.
    let model = QfexModel {
        maker_fill: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-maker-fill").await;
    assert_stopped(&d, places::Stop::UnexpectedFill);
    let run = d.drop_run();
    let flatten = run.flatten.as_ref().expect("a flatten");
    assert_flattened(flatten, 0);
    assert_eq!(flatten.after, flatten::AFTER_FILL);
    // The Stop after it cancels nothing more, and the session closes gracefully.
    let cancels = run.cancels.as_ref().expect("a fill of ours cancels");
    assert!(
        cancels.ended && !cancels.held && cancels.sent == 0,
        "{cancels:?}"
    );
    assert_eq!(run.ended_by, Some(stage::GRACEFUL));
    assert_eq!(d.count("add_order"), 2, "{:?}", d.what());
    assert_eq!(d.count("cancel_order"), 0, "{:?}", d.what());
    assert_flatten_iocs_within_the_cap(&d, &["SELL"]);
    let after = (d.ev.reads.iter())
        .find(|r| r.before == refusals::AFTER_DROP_ONE && r.label == "primary (drop)")
        .unwrap();
    assert!(after.flat, "{after:?}");
    // X filled is named by a trade since its send and ended FILLED on historic orders.
    let x = run.after.as_ref().unwrap();
    assert_eq!(x.terminal_status.as_deref(), Some("FILLED"));
    let steps: Vec<&str> = run.compared.iter().map(|c| c.step).collect();
    assert_eq!(steps, ["the stop's cancels"]);
    let (report, json) = d.outputs();
    assert!(
        report.contains("#### The flatten (FBC-ou9g; decision 0148 item 5, 0151 item 9)"),
        "{report}"
    );
    assert!(report.contains("  - IOC 1 (sell): filled whole, its fills settled"));
    assert!(report.contains(&format!("- How it ended: {}", flatten.ended())));
    let entry = &json["drop_tests"][0]["session_one"]["flatten"];
    assert_eq!(entry["rest_flat"], true);
    assert_eq!(entry["iocs"][0]["came_back"], flatten::Ioc::Filled.says());
    assert!(
        d.printed.contains("FLATTENED on primary: flat"),
        "{}",
        d.printed
    );
    // The drop-production/ slice, the flatten IOC's frames in it, passed the privacy check.
    assert!(
        d.ev.errors.is_empty() && !d.ev.fixtures.is_empty(),
        "{:?}",
        d.ev.errors
    );
}

/// Asserts `flatten` ran its course flat: `cancels` cancels seen to end, the owner's Flatten
/// into Exit, every IOC sent filled whole, the registry flat and REST flat, no stop of its own.
fn assert_flattened(flatten: &flatten::FlattenRun, cancels: usize) {
    let c = flatten.cancels.as_ref().expect("its cancels");
    assert!(c.ended && !c.held && c.sent == cancels, "{flatten:?}");
    assert_eq!(flatten.exit, Some(true), "{flatten:?}");
    assert!(!flatten.iocs.is_empty(), "{flatten:?}");
    for ioc in &flatten.iocs {
        assert!(ioc.sent, "{flatten:?}");
        assert_eq!(ioc.came_back, Some(flatten::Ioc::Filled), "{flatten:?}");
    }
    assert!(flatten.flat && flatten.rest_flat, "{flatten:?}");
    assert_eq!((flatten.stop, &flatten.error), (None, &None), "{flatten:?}");
    assert_eq!(flatten.blocked, None);
}

/// Asserts the model's limit IOCs, the flatten's, are each a reducing order on `sides`, in
/// order, at the instrument's minimum size, with no reduce-only flag, priced at most the run's
/// --flatten-cap-bps (10) through the model's touch it crosses and still crossing it.
fn assert_flatten_iocs_within_the_cap(d: &Dropped, sides: &[&str]) {
    let model = d.venue.model.lock().unwrap();
    let iocs: Vec<&model::Order> = (model.orders.iter())
        .filter(|o| o.kind == "LIMIT" && o.tif == "IOC")
        .collect();
    let got: Vec<&str> = iocs.iter().map(|o| o.side.as_str()).collect();
    assert_eq!(got, sides);
    let touch = |side: Option<(&str, &str)>| Decimal::from_str(side.unwrap().0).unwrap();
    let (bid, ask) = (touch(model.touch.bid), touch(model.touch.ask));
    let cap = Decimal::new(10, 4);
    for ioc in iocs {
        assert_eq!(ioc.quantity, Decimal::new(1, 2), "the minimum size");
        let px = Decimal::from_str(&ioc.price.to_string()).unwrap();
        match ioc.side.as_str() {
            "SELL" => assert!(px <= bid && px >= bid * (Decimal::ONE - cap), "{px}"),
            _ => assert!(px >= ask && px <= ask * (Decimal::ONE + cap), "{px}"),
        }
    }
    drop(model);
    // No add_order the run sent carried a reduce-only flag (decision 0148 item 4).
    let journal = d.dir.join("evidence").join("journal");
    let reader = fbc_journal::JournalReader::open(&journal, 0).unwrap();
    for e in reader.entries() {
        if let fbc_journal::Record::Outbound { frame, .. } = e.unwrap().record {
            let text = String::from_utf8_lossy(frame.bytes()).into_owned();
            assert!(!text.contains("reduce_only"), "{text}");
        }
    }
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
    // venue ids and client ids on both accounts, and the crossing post-only's (FBC-su9i), the
    // taker IOC's and its flatten IOC's (FBC-ou9g), the user id QFEX's frames name, their prices
    // and size, both caps, the symbol and the namespace.
    let mut values: Vec<String> = [PUBLIC, SECRET, SUB_PUBLIC, SUB_SECRET, SUB, model::USER_ID]
        .map(str::to_owned)
        .to_vec();
    assert_eq!(orders.len(), 10);
    for order in &orders {
        values.extend([order.id.clone(), order.cid.clone(), order.price.to_string()]);
    }
    // The fee amount the model writes in every fill_response and trades row (FBC-ou9g): only
    // its sign reaches an output.
    values.extend(["99.5", "100.1", "0.01", "0.001"].map(str::to_owned));
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
    assert_eq!(written(&d, "add_order"), [true; 10]);
    // Each output passes scripts/privacy-check.sh, each slice by the run itself.
    assert_report_and_evidence_pass_the_privacy_check(&evidence);
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/privacy-check.sh");
    evidence::privacy_check(&script, &slice).unwrap();
    evidence::privacy_check(&script, &evidence.join(stage::PLACES_SLICE)).unwrap();
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
    // of it can fill. Since FBC-ou9g that fill is flattened too, after the cancels and before
    // the disarm and the close (0151 item 9): half of the minimum size is below it, so no IOC
    // is placed and the run stops `Flatten(BelowMinimum)`, the flatten's own stop in place of
    // the fill's.
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
    let below = places::Stop::Flatten(places::Flattening::BelowMinimum);
    assert_stopped(&d, below);
    assert_eq!(d.count("cancel_order"), 1, "{:?}", d.what());
    let run = d.drop_run();
    let cancels = run.cancels.as_ref().expect("a fill of ours cancels");
    assert_eq!(
        (cancels.sent, cancels.held, cancels.ended),
        (1, false, true)
    );
    assert_eq!(run.ended_by, Some(stage::GRACEFUL));
    assert!(!run.x_resting && run.close_cancelled_x.is_none(), "{run:?}");
    let flatten = run.flatten.as_ref().expect("a flatten");
    assert_eq!(flatten.after, flatten::AFTER_LATE_FILL);
    assert_eq!(d.count("add_order"), 1, "no flatten IOC: {:?}", d.what());
    let lifted = d.at("FLATTEN on primary: the Stop's Kill lifted");
    let cancels = d.at(
        "STOP cancels: Kill, then 1 cancels by venue id (0151 item 2), every order they named \
         seen to end; the market disarmed",
    );
    assert!(d.at("COMPARE after the stop's cancels") < lifted);
    assert!(lifted < cancels);
    assert!(cancels < d.at(&format!("ended by {}", stage::GRACEFUL)));
    let model = d.venue.model.lock().unwrap();
    assert_eq!(model.orders[0].state, model::State::Cancelled);
    assert_eq!(model.fills.len(), 1);
    let at = |what: &str| model.log.iter().position(|e| e.what.contains(what));
    let (cancel, close) = (at(" cancel_order").unwrap(), at("cod ").unwrap());
    assert!(cancel < close, "{:?}", model.log);
}

#[tokio::test]
async fn a_late_flatten_ages_a_touch_heard_while_the_close_waited_from_its_arrival() {
    // As QFEX reads X the shared budget is held for 5 s, so the comparison after X's place reads
    // nothing within its one second and the close waits for room. QFEX's clock runs an hour
    // ahead of the probe's, so a touch's stamp ages it by nothing and only its arrival does. The
    // touch is pushed again at the first trade-socket authentication, so X is priced from a
    // fresh one, and again 2 s after X's place, while the close waits; at 2.5 s X fills whole as
    // a maker. The MDS touch is read while the close waits, so the touch heard at 2 s is aged
    // from then: once there is room and the stop's cancels ran, it is older than
    // --touch-max-age-ms, 2500 ms, so the late flatten sends no IOC, Flatten(NoTouch), and the
    // position is left to the owner.
    let rate_user = user();
    let ahead = model::Touch {
        age_secs: -3600,
        ..model::Touch::fresh()
    };
    let model = QfexModel {
        touch: ahead.clone(),
        touch_moves: Some(ahead),
        touch_after: Some(Duration::from_secs(2)),
        hold_on_place: Some((shared_limiter(rate_user), Duration::from_secs(5))),
        fill_after: Some(Duration::from_millis(2500)),
        ..QfexModel::default()
    };
    let argv = |argv| {
        let argv = set(argv, "--agree-wait-ms", "1000");
        set(argv, "--touch-max-age-ms", "2500")
    };
    let d = held_drop(model, "drop-touch-at-close", rate_user, argv).await;
    let stop = places::Stop::Flatten(places::Flattening::NoTouch);
    let flatten = assert_flatten_stopped(&d, stop, 0);
    assert_eq!(flatten.after, flatten::AFTER_LATE_FILL);
    assert_eq!((flatten.exit, flatten.iocs.len()), (Some(true), 0));
    assert_eq!(flatten.error.as_deref(), Some(STALE));
    assert_eq!(d.count("add_order"), 1, "no flatten IOC: {:?}", d.what());
    let run = d.drop_run();
    assert_eq!(run.compared[0].ended, compare::Ended::Unread);
    assert_eq!(run.ended_by, Some(stage::GRACEFUL));
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
    // FBC-gisx: X's history not read for a provider rate refusal is no History of the owner's
    // ruling, the refusal the stop.
    assert_eq!(run.x_unseen, None, "{run:?}");
    let (report, json) = d.outputs();
    assert!(
        report.contains(
            "- X left unjudged after the session (decision 0005; the owner's ruling under 0151 \
             item 6): no\n"
        ),
        "{report}"
    );
    assert_eq!(
        json["drop_tests"][0]["session_one"]["x_unseen"],
        Value::Null
    );
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
    // The drop test's two sessions, then the crossing session (FBC-su9i) and the taker IOC
    // session (FBC-ou9g).
    let conns = d.session_conns();
    assert_eq!(conns.len(), 4, "{what:?}");
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
    // The last read of the drop test, before the crossing session's (FBC-su9i).
    let last = at(&format!("trade {} auth", conns[2])) - 1;
    assert!(
        dropped < after_drop && after_drop < trades && historic < last,
        "{what:?}"
    );
    // From the first arm to the last read before the crossing session: no cancel, two arms
    // alone, and no reconnect: one connection attempt a session, and no session connection
    // after the second but the crossing session's (FBC-su9i), its own registry's.
    let crossing = at(&format!("trade {} auth", conns[2]));
    let span = &what[first_arm..crossing];
    assert!(
        span.iter().all(|w| !w.ends_with(" cancel_order")),
        "{what:?}"
    );
    let arms = span
        .iter()
        .filter(|w| w.ends_with(" cancel_on_disconnect"))
        .count();
    assert_eq!(arms, 2, "{what:?}");
    assert_eq!(d.count("subscribe"), 12);
    let attempts: Vec<(u64, u64)> = (d.ev.sessions.iter())
        .map(|s| (s.attempts, s.failed_attempts))
        .collect();
    assert_eq!(attempts, [(1, 0); 4]);
    let ended: Vec<&str> = d.ev.sessions.iter().map(|s| s.ended_by).collect();
    let graceful = stage::GRACEFUL;
    assert_eq!(ended, [graceful, stage::DROPPED, graceful, graceful]);
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
        "STOPPED stage three",
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
    // the end of each of its two sessions (decision 0148 item 3); the cross registry after it is
    // the account's next (FBC-su9i), then the take registry (FBC-ou9g).
    assert_eq!(d.ev.place_registries.len(), 3);
    assert_eq!(d.ev.place_registries[1].step, ledger::Step::Cross);
    assert_eq!(d.ev.place_registries[2].step, ledger::Step::Take);
    let ends = &d.ev.place_registries[0].registry_run().ends;
    let sessions: Vec<&str> = ends.iter().map(|e| e.session).collect();
    assert_eq!(sessions, [places::DROP_ONE, places::DROP_TWO]);
    assert_eq!(d.printed.matches("REGISTRY drop on primary").count(), 1);
    assert_eq!(d.printed.matches("seeded flat").count(), 3);
    // Started once, by drop session one: drop session two's market stays armed.
    assert_eq!(d.printed.matches("START primary").count(), 1);
    assert!(d.at("REGISTRY drop on primary") < d.at("SESSION primary (drop session one)"));
    let names: Vec<(&str, &str)> = (d.ev.sessions.iter())
        .map(|s| (s.account, s.label()))
        .collect();
    assert_eq!(
        names,
        [
            ("primary", places::DROP_ONE),
            ("primary", places::DROP_TWO),
            ("primary", places::CROSS),
            ("primary", places::TAKE)
        ]
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
        let clean = places::OthersClean::Clean;
        assert_eq!((run.stop, run.others_clean), (None, clean));
    }
    // Every post-only order ended cancelled; the taker IOCs and their flatten IOCs filled
    // (FBC-ou9g).
    let model = d.venue.model.lock().unwrap();
    let post_only = model.orders.iter().filter(|o| o.kind == "ALO");
    assert!(
        post_only
            .clone()
            .all(|o| o.state == model::State::Cancelled)
    );
    assert_eq!(post_only.count(), 6);
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
async fn a_fill_of_x_in_drop_session_two_is_flattened_then_stops() {
    // Drop session two's arm comes with a maker fill of X: a fill of ours while the session is
    // open, so the flatten (0151 item 9): nothing of ours left open to cancel (Y is never
    // placed), the owner's Flatten into Exit, one reducing IOC, REST flat; then the session's
    // Stop and a graceful close, and the run stops UnexpectedFill, flat.
    let model = QfexModel {
        fill_at_arm: Some(1),
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-two-fill-x").await;
    assert_stopped(&d, places::Stop::UnexpectedFill);
    let two = d.two();
    assert_flattened(two.flatten.as_ref().expect("a flatten"), 0);
    let cancels = two.cancels.as_ref().expect("a fill of ours cancels");
    assert!(cancels.ended && !cancels.held, "{cancels:?}");
    assert_eq!(two.ended_by, Some(stage::GRACEFUL));
    assert!(!two.y.submitted);
    assert!(d.drop_run().verdict.is_none());
    // X and the flatten IOC; X filled whole and Y never placed: nothing of ours was left open to
    // cancel.
    assert_eq!(d.count("add_order"), 2);
    assert_eq!((d.count("cancel_order"), cancels.sent), (0, 0));
    assert_flatten_iocs_within_the_cap(&d, &["SELL"]);
    let after = (d.ev.reads.iter())
        .find(|r| r.before == refusals::AFTER_DROP_TWO && r.label == "primary (drop)")
        .unwrap();
    assert!(after.flat);
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
    let (report, json) = d.outputs();
    assert!(
        report.contains(
            "- The drop test: not judged: the drop test ended before its drop (decision 0132 \
             item 4), so it did not pass"
        ),
        "{report}"
    );
    let entry = &json["drop_tests"][0]["session_two"]["flatten"];
    assert_eq!(entry["rest_flat"], true);
}

#[tokio::test]
async fn a_fill_heard_during_drop_session_twos_resync_is_flattened_once_it_may_place() {
    // FBC-23ch's third review fix carried to the flatten: drop session two's arm fills X as a
    // maker, but the fill's frames reach the session only with its resync's first page, as a
    // kernel holding them for the client's acknowledgement sends them once the client writes.
    // A place before the resync's end is NotSent(Disconnected) (decision 0058), so the flatten
    // waits for the session's place gate before its IOC: one reducing IOC, REST flat, and the
    // run stops UnexpectedFill, flat, as when the fill comes with the arm. The model holds the
    // page that ends the resync until the flatten says it waits for the gate, so the IOC can go
    // out only through that wait.
    let model = QfexModel {
        fill_in_resync: Some(1),
        resync_page_held: true,
        ..QfexModel::default()
    };
    let (dir, removed) = temp_dir("drop-two-fill-in-resync");
    let venue = Venue::start(model).await;
    let (mut opts, places) = parsed(&drop_argv(&dir, &venue)).unwrap();
    opts.rate_user = user();
    let env = environment();
    let printed = venue.printed();
    let lookup = |var: &str| env.get(var).cloned();
    let out: stage::Out = Rc::new(RefCell::new(printed.clone()));
    let ran = stage::run_places(&opts, &places, &lookup, out);
    tokio::pin!(ran);
    let gate = format!("FLATTEN on primary: {}", stage::PLACE_GATE);
    let release = async {
        until("the flatten's wait for the gate", || {
            printed.text().contains(&gate)
        })
        .await;
        venue.release_held().await;
        std::future::pending::<()>().await
    };
    let ran = tokio::select! {
        ran = &mut ran => ran,
        () = release => unreachable!("the release never ends"),
    };
    let printed = printed.text();
    let ev = stopped(ran, &printed);
    let d = Dropped {
        dir,
        _removed: removed,
        venue,
        ev,
        printed,
    };
    assert_stopped(&d, places::Stop::UnexpectedFill);
    assert!(d.printed.contains(&gate), "{}", d.printed);
    let two = d.two();
    let flatten = two.flatten.as_ref().expect("a flatten");
    assert_flattened(flatten, 0);
    assert_eq!(two.ended_by, Some(stage::GRACEFUL));
    assert!(!two.y.submitted);
    assert_eq!(d.count("add_order"), 2);
    assert_flatten_iocs_within_the_cap(&d, &["SELL"]);
    assert!(
        d.printed.contains("FLATTENED on primary: flat"),
        "{}",
        d.printed
    );
}

#[tokio::test]
async fn a_taker_iocs_final_order_response_after_its_fill_is_awaited_for_the_frame_order() {
    // FBC-23ch's third review fix carried to the taker IOC: QFEX fills the taker IOC whole, its
    // fill_response first and its FILLED order_response 300 ms later (--rpc-timeout-ms is 600),
    // as a frame held behind another can come. The fill alone ends it whole in the take
    // registry, so the wait goes on for the final order_response, and the frame order
    // (FBC-tfxk) is read on both frames.
    let model = QfexModel {
        ioc_order_after: Some(Duration::from_millis(300)),
        ..QfexModel::default()
    };
    let d = dropped(model, "take-order-late").await;
    assert_eq!((d.ev.stop, d.marker()), (None, None), "{}", d.printed);
    for n in 0..2 {
        let taken = d.take(n).taken.as_ref().expect("the taker IOC");
        assert_eq!(taken.ioc, Some(flatten::Ioc::Filled));
        assert_eq!(taken.frame_order, Some(taker::FrameOrder::FillFirst), "{n}");
    }
}

#[tokio::test]
async fn a_taker_iocs_ack_then_fill_with_no_final_order_response_leaves_the_frame_order_unmeasured()
{
    // PR #277's review (Codex, P2): QFEX acknowledges the taker IOC, then sends its
    // fill_response, and its FILLED order_response comes only long after its deadline
    // (--rpc-timeout-ms is 600). The wait ends at the deadline with the ACK the last
    // order_response heard, which is no final order_response: the order FBC-tfxk asks about is
    // not measured, in the record, the report and the evidence, never OrderFirst. The fill
    // reached the take registry, so it is flattened and the run ends with no stop.
    let model = QfexModel {
        ioc_acked: true,
        ioc_order_after: Some(Duration::from_secs(3600)),
        ..QfexModel::default()
    };
    let d = dropped_in(model, "take-acked-unordered", &primary_only(), |a| a).await;
    assert_eq!((d.ev.stop, d.marker()), (None, None), "{}", d.printed);
    let run = d.take(0);
    let taken = (run.taken.as_ref()).unwrap_or_else(|| panic!("{run:?}: {}", d.printed));
    assert_eq!(taken.ioc, Some(flatten::Ioc::Filled));
    assert_eq!(taken.frame_order, Some(taker::FrameOrder::NoOrder));
    assert_flattened(run.flatten.as_ref().unwrap(), 0);
    let said = taker::FrameOrder::NoOrder.says();
    let (report, json) = d.outputs();
    let section = take_section(&report, "primary");
    assert!(
        section.contains(&format!(
            "- The order of its final order_response and its fill_response (FBC-tfxk): {said}"
        )),
        "{section}"
    );
    assert_eq!(json["stage_two"]["takes"][0]["taken"]["frame_order"], said);
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
    // The crossing and taker IOC sessions' frames are in the places-production/ slice (FBC-su9i,
    // FBC-ou9g).
    assert_eq!(d.ev.sessions.len(), 8);
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
async fn a_fill_of_y_in_drop_session_two_is_flattened_then_stops() {
    // Y fills whole as a maker right after its ACK, the session open: a fill of ours, so the
    // flatten (0151 items 2 and 9): X, the one order of ours still open, cancelled by its venue
    // id and seen to end, the owner's Flatten into Exit, one reducing IOC, REST flat; then the
    // session's Stop, with nothing left to cancel, a graceful close with no drop, and the run
    // stops UnexpectedFill, flat.
    let model = QfexModel {
        on_ack: Some((1, model::OnAck::MakerFill)),
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-two-fill-y").await;
    assert_stopped(&d, places::Stop::UnexpectedFill);
    let two = d.two();
    assert!(two.y.submitted, "{two:?}");
    assert_flattened(two.flatten.as_ref().expect("a flatten"), 1);
    let cancels = two.cancels.as_ref().expect("a fill of ours cancels");
    assert_eq!(
        (cancels.sent, cancels.held, cancels.ended),
        (0, false, true)
    );
    assert_eq!(two.ended_by, Some(stage::GRACEFUL));
    assert!(d.drop_run().verdict.is_none());
    assert_eq!(d.count("cancel_order"), 1);
    // The model held the read pages from Y's fill until the session wrote again: the flatten's
    // cancel of X by its venue id, the session's first write after the fill, released them, so
    // the comparison after the Stop's cancels agreed, its connection still open (FBC-77r8).
    let compared: Vec<_> = (two.compared.iter())
        .map(|c| c.first_agreed.is_some())
        .collect();
    assert_eq!(compared, [true], "{:?}", two.compared);
    let model = d.venue.model.lock().unwrap();
    let states: Vec<model::State> = model.orders.iter().map(|o| o.state).collect();
    let filled = model::State::Filled;
    assert_eq!(states, [model::State::Cancelled, filled, filled]);
    let x = model.orders[0].id.clone();
    drop(model);
    assert!(cancel_names(&d, &x), "the cancel names X by its venue id");
    assert_flatten_iocs_within_the_cap(&d, &["SELL"]);
    let after = (d.ev.reads.iter())
        .find(|r| r.before == refusals::AFTER_DROP_TWO && r.label == "primary (drop)")
        .unwrap();
    assert!(after.flat);
    let compared: Vec<&str> = (two.flatten.as_ref().unwrap().compared.iter())
        .map(|c| c.step)
        .collect();
    assert_eq!(compared, ["the flatten's cancels", "a flatten IOC"]);
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
        "- terminal_status on GET /user/historic-orders: X not read; Y not read",
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
        others_clean: places::OthersClean::NotClean,
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
        "- terminal_status on GET /user/historic-orders: X CANCELLED; Y not read",
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
    // Since FBC-ou9g the fill is flattened too, after the cancels and before the close (0151
    // item 9): half of the minimum size is below it, so no IOC is placed, and the refusal stays
    // the run's stop.
    let flatten = two.flatten.as_ref().expect("a flatten");
    assert_eq!(flatten.after, flatten::AFTER_LATE_FILL);
    let below = places::Stop::Flatten(places::Flattening::BelowMinimum);
    assert_eq!(flatten.stop, Some(below.name()), "{flatten:?}");
    assert_eq!(d.count("add_order"), 2, "no flatten IOC: {:?}", d.what());
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
    // The stop said after the session's failure, as after drop session two (FBC-gisx's review).
    let stopped = format!(
        "a test's X could not be priced; LeftResting after drop session one: {}; no further \
         session runs",
        places::Stop::LeftResting.says()
    );
    assert_eq!(d.ev.stopped.as_deref(), Some(stopped.as_str()));
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

// ----- FBC-77r8's review of PR #273: positive evidence, unexpected orders, X's end ------------

/// The line the report gives of X when the run ended, after drop session two's reads.
const X_AT_END: &str = "- X resting when the run ended: ";

#[tokio::test]
async fn a_fill_a_trades_page_names_before_a_later_page_fails_stops_the_run_unexpected_fill() {
    // PR #273's thread 4611183910: after the drop, X fills as a maker as its trades are first
    // asked, no frame telling any socket, and on pages of one a venue answering every page as if
    // asked from offset 0 fails the read at its second page, with no provider rate refusal. The
    // page read named X: a fill of ours seen with no session open, which the unfinished read
    // does not erase. With no refusal, the verdict's first stop is UnexpectedFill, the position
    // the owner's (0151 item 9), not DropFailed for the trades not read whole.
    let model = QfexModel {
        fill_on_trades_page: Some(1),
        stuck_trades: true,
        ..QfexModel::default()
    };
    let d = dropped_with(model, "drop-two-fill-then-unread", |argv| {
        set(argv, "--page-size", "1")
    })
    .await;
    assert_stopped(&d, places::Stop::UnexpectedFill);
    let two = d.two();
    assert_eq!(two.ended_by, Some(stage::DROPPED), "{two:?}");
    let after = two.after.as_ref().expect("X and Y were read");
    assert!(!after.trades_whole, "{after:?}");
    assert_eq!(
        after.error.as_deref(),
        Some("a full get_user_trades page repeated the page before it")
    );
    // The reads after the drop showed the account flat and X and Y absent: the fill came after.
    let last = two.after_drop.as_ref().and_then(|a| a.last).unwrap();
    assert!(last.flat && !last.x && !last.y, "{last:?}");
    let verdict = d.verdict();
    let x = verdict::Which::X;
    for failure in [verdict::Failure::Traded(x), verdict::Failure::TradesNotRead] {
        assert!(verdict.failures.contains(&failure), "{verdict:?}");
    }
    assert_eq!(
        verdict.reach,
        verdict::Reach::Unmeasured(verdict::Unmeasured::Traded)
    );
    let (report, _) = d.outputs();
    assert!(report.contains(places::OWNERS_POSITION), "{report}");
    assert!(report.contains("a get_user_trades row names X"), "{report}");
    assert_eq!(d.ev.drops.len(), 1);
}

#[tokio::test]
async fn a_status_a_historic_orders_page_shows_before_a_later_page_fails_is_kept() {
    // A sibling of thread 4611183910: on pages of one, a venue answering every GET
    // /user/historic-orders page whole fails the read at its second page, after the first named
    // X and Y CANCELLED. Those rows show X and Y terminal (0151 item 5), so the verdict keeps
    // their statuses and shows the drop's reach over X, though the test does not pass.
    let d = dropped_with(QfexModel::default(), "drop-two-historic-unread", |argv| {
        set(argv, "--page-size", "1")
    })
    .await;
    assert_stopped(&d, places::Stop::DropFailed);
    let after = d.two().after.as_ref().expect("X and Y were read");
    assert!(after.trades_whole && !after.historic_whole, "{after:?}");
    let verdict = d.verdict();
    assert_eq!(verdict.failures, [verdict::Failure::HistoricNotRead]);
    let cancelled = Some("CANCELLED".to_owned());
    assert_eq!(verdict.terminal, [cancelled.clone(), cancelled]);
    assert_eq!(verdict.reach, verdict::Reach::Cancelled);
}

#[tokio::test]
async fn a_position_the_reads_after_a_failed_drop_test_show_stops_the_run_not_flat() {
    // A sibling of thread 4611183910: historic-orders leaves Y out, so the drop test fails, and
    // the account's position moves once that read is answered, after the reads after the drop
    // showed it flat and with no trade row. The read of every named account after them shows the
    // position: a stop not flat, the position the owner's (0151 item 9), which a test that only
    // did not pass does not outrank.
    let model = QfexModel {
        historic_omits_last: true,
        after_drop_historic: Some(model::AfterHistoric::Position),
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-two-position-after").await;
    assert_stopped(&d, places::Stop::NotFlat);
    let verdict = d.verdict();
    let y = verdict::Which::Y;
    assert_eq!(verdict.failures, [verdict::Failure::NotTerminal(y)]);
    assert_eq!(verdict.stop(), Some(places::Stop::DropFailed));
    let stopped = d.ev.stopped.as_deref().unwrap_or_default();
    assert!(
        stopped.starts_with("NotFlat after drop session two"),
        "{stopped}"
    );
    let (report, _) = d.outputs();
    assert!(report.contains(places::OWNERS_POSITION), "{report}");
}

#[tokio::test]
async fn an_order_not_ours_in_the_read_before_the_drop_stops_drop_session_two_before_its_drop() {
    // PR #273's thread 4611183918: beside Y, the account holds an order no run placed, which the
    // read before the drop lists. That read shows an order the streams did not, a stop (decision
    // 0148 item 8), before the drop, so the account-wide drop never cancels it out of sight. The
    // stop cancels nothing (0151 item 2): the session ends by a graceful close, X and Y left for
    // the owner's hand cancel, and no drop test runs on the subaccount.
    let model = QfexModel {
        foreign_order_on: Some(1),
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-two-foreign-before-drop").await;
    assert_stopped(&d, places::Stop::ReadDisagrees);
    let two = d.two();
    assert_eq!(two.before_drop, Some([true, true]), "{two:?}");
    assert_eq!(two.ended_by, Some(stage::GRACEFUL));
    assert_eq!(
        (two.cancels.as_ref(), two.after_drop.as_ref()),
        (None, None)
    );
    assert!(d.drop_run().verdict.is_none());
    assert_eq!(d.count("cancel_order"), 0);
    let what = d.what();
    assert!(!what.iter().any(|w| w.contains(" drop ")), "{what:?}");
    let model = d.venue.model.lock().unwrap();
    let open =
        |cid: &str| (model.orders.iter()).any(|o| o.cid == cid && o.state == model::State::Open);
    assert!(open(model::STRAY_CID));
    assert_eq!(
        (model.orders.iter())
            .filter(|o| o.state == model::State::Open)
            .count(),
        3
    );
    drop(model);
    assert!(two.resting, "{two:?}");
    assert!(d.ev.sessions.iter().all(|s| s.account == "primary"));
    let stopped = d.ev.stopped.as_deref().unwrap_or_default();
    assert!(
        stopped.starts_with("ReadDisagrees in drop session two"),
        "{stopped}"
    );
    let disagrees = "READ before the drop: primary: an order, a TWAP or a position the streams \
                     did not show (decision 0148 item 8): no drop";
    assert!(d.at("READ before the drop: primary: X listed true, Y listed true") < d.at(disagrees));
    let (report, json) = d.outputs();
    let said = "- The read before the drop: X listed yes; Y listed yes; it showed an order, a \
                TWAP or a position the streams did not, a stop before the drop (decision 0148 \
                item 8)";
    assert!(report.contains(said), "{report}");
    let two = &json["drop_tests"][0]["session_two"];
    assert_eq!(two["before_drop_disagrees"], Value::Bool(true), "{two}");
}

#[tokio::test]
async fn an_order_not_ours_the_reads_after_the_drop_list_fails_the_test_read_disagrees() {
    // A sibling of thread 4611183918: once the drop cancelled X and Y, the account holds an order
    // no run placed, which the reads after the drop list. X and Y are absent and the position
    // flat, but the reads show an order the streams did not: the test fails and the run stops
    // ReadDisagrees (decision 0148 item 8), not LeftResting for an order of ours.
    let model = QfexModel {
        foreign_on_drop: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-two-foreign-after-drop").await;
    assert_stopped(&d, places::Stop::ReadDisagrees);
    let two = d.two();
    let after = two.after_drop.as_ref().unwrap();
    assert!(after.absent_after.is_some(), "{after:?}");
    let verdict = d.verdict();
    assert_eq!(verdict.failures.len(), 1, "{verdict:?}");
    assert_eq!(verdict.stop(), Some(places::Stop::ReadDisagrees));
    let stopped = d.ev.stopped.as_deref().unwrap_or_default();
    assert!(
        stopped.starts_with("ReadDisagrees after the drop"),
        "{stopped}"
    );
    assert_eq!(d.ev.drops.len(), 1);
    let (report, json) = d.outputs();
    let failed = "- The drop test: failed: an order or a TWAP not ours listed by a read after the \
                  drop: something else trades on the account (decision 0132 item 4)";
    assert!(report.contains(failed), "{report}");
    let after = &json["drop_tests"][0]["session_two"]["after_drop"];
    assert_eq!(after["others"], Value::Bool(true), "{after}");
}

#[tokio::test]
async fn an_order_or_twap_not_ours_the_reads_after_drop_session_two_show_stops_read_disagrees() {
    // A sibling of thread 4611183918: the drop test passes, then, once its historic-orders read
    // is answered, the account holds an order no run placed, or lists a TWAP. The read of every
    // named account after the drop shows what the streams did not: a stop, ReadDisagrees
    // (decision 0148 item 8), not LeftResting for an order of ours nor, for a TWAP, no stop at
    // all, which left the next account's reads to refuse with no marker.
    for (change, what) in [
        (model::AfterHistoric::Foreign, "drop-two-foreign-end"),
        (model::AfterHistoric::Twap, "drop-two-twap-end"),
    ] {
        let model = QfexModel {
            after_drop_historic: Some(change),
            ..QfexModel::default()
        };
        let d = dropped(model, what).await;
        assert_stopped(&d, places::Stop::ReadDisagrees);
        assert!(d.verdict().passed, "{:?}", d.verdict());
        let stopped = d.ev.stopped.as_deref().unwrap_or_default();
        assert!(
            stopped.starts_with("ReadDisagrees after drop session two"),
            "{stopped}"
        );
        assert!(d.ev.sessions.iter().all(|s| s.account == "primary"));
    }
}

#[tokio::test]
async fn a_twap_the_reads_after_a_failed_drop_session_one_show_stops_read_disagrees() {
    // A sibling of thread 4611183918: drop session one fails with no stop, X not priced, and once
    // its connection ended the account lists a TWAP. The reads after the session show what the
    // streams did not: a stop, marked (decision 0148 item 8, 0151 item 6), not the session's
    // failure alone with no marker.
    let model = QfexModel {
        twap_after_close: true,
        ..QfexModel::default()
    };
    let d = dropped_hooked(model, "drop-failed-twap", |t| t.x_unpriced = true).await;
    assert_eq!(d.ev.stop, Some("ReadDisagrees"), "{}", d.printed);
    let marker = d.marker().unwrap_or_default();
    assert!(marker.starts_with("stop: ReadDisagrees\n"), "{marker}");
    // The session's failure stays the run's end, as with an order of ours left resting, the stop
    // said after it, as after drop session two (FBC-gisx's review).
    let error = "a test's X could not be priced";
    let stopped = format!(
        "{error}; ReadDisagrees after drop session one: {}; no further session runs",
        places::Stop::ReadDisagrees.says()
    );
    assert_eq!(d.ev.stopped.as_deref(), Some(stopped.as_str()));
    assert_eq!(d.drop_run().error.as_deref(), Some(error));
    assert_eq!(d.count("add_order"), 0, "{:?}", d.what());
    d.venue.assert_served();
}

#[tokio::test]
async fn x_resting_when_the_run_ended_is_said_from_the_reads_after_drop_session_two() {
    // PR #273's thread 4611183928: what the report says of X when the run ended comes from the
    // read of X's account after drop session two, never from session two having been built. X
    // survives a drop that reaches only its own connection's orders, and survives a session two
    // whose arm was refused: it still rests, for the owner's hand cancel. A drop that cancelled
    // it leaves it absent, as that read confirms.
    let resting = format!(
        "{X_AT_END}yes, shown by the read of X's account after drop session two: {}",
        places::HAND_CANCEL
    );
    let own = QfexModel {
        cod_reach: model::Reach::Own,
        ..QfexModel::default()
    };
    let refused = QfexModel {
        arm_err_on: Some(1),
        ..QfexModel::default()
    };
    for (model, what) in [
        (own, "drop-two-x-survives"),
        (refused, "drop-two-x-unarmed"),
    ] {
        let d = dropped(model, what).await;
        let (report, json) = d.outputs();
        assert!(report.contains(&resting), "{report}");
        assert!(!report.contains(&format!("{X_AT_END}no")), "{report}");
        let two = &json["drop_tests"][0]["session_two"];
        assert_eq!(
            (&two["x_resting"], &two["x_read_after"]),
            (&Value::Bool(true), &Value::Bool(true))
        );
    }
    let d = dropped(QfexModel::default(), "drop-two-x-absent").await;
    let (report, json) = d.outputs();
    let two = &json["drop_tests"][0]["session_two"];
    assert_eq!(
        (&two["x_resting"], &two["x_read_after"]),
        (&Value::Bool(false), &Value::Bool(true))
    );
    let absent =
        format!("{X_AT_END}no: the read of X's account after drop session two no longer listed it");
    assert!(report.contains(&absent), "{report}");
}

#[test]
fn x_resting_when_the_run_ended_is_not_known_when_its_account_was_not_read() {
    // Thread 4611183928's third case: drop session two built, but X's account not read after it
    // (or read on a full page not listing X), so whether X rests is not known.
    use places::{DropRun, TwoRun};
    let unread = DropRun {
        account: "primary",
        x_resting: true,
        x_read: true,
        x: places::XRun {
            submitted: true,
            ..places::XRun::default()
        },
        two: Some(TwoRun::default()),
        ..DropRun::default()
    };
    let mut s = String::new();
    places::drop_markdown(&mut s, &unread, false);
    let said = format!(
        "{X_AT_END}not known: X's account was not read after drop session two, or its page was \
         full and did not list X; read it on QFEX's site before the next run"
    );
    assert!(s.contains(&said), "{s}");
    // Statuses not read are said so, apart from statuses read and not listed.
    assert!(
        s.contains("- terminal_status on GET /user/historic-orders: X not read; Y not read"),
        "{s}"
    );
}

#[tokio::test]
async fn a_terminal_status_not_read_is_said_not_read_rather_than_not_listed() {
    // A sibling of thread 4611183928: X's GET /user/historic-orders read after drop session one
    // fails, so the run stops before drop session two. The report's terminal_status line says
    // X's was not read, and Y's too (Y was never placed), rather than that neither was listed.
    let model = QfexModel {
        historic_fails: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-one-historic-unread").await;
    assert_stopped(&d, places::Stop::LeftResting);
    let (report, _) = d.outputs();
    assert!(
        report.contains("- terminal_status on GET /user/historic-orders: X not read; Y not read"),
        "{report}"
    );
}

// ----- FBC-77r8's second review of PR #273: unread accounts, the read before the drop, ranks ---

/// Asserts that `d` stopped with `stop`, marked, its report and evidence naming it and passing
/// the privacy check: [`assert_stopped`] for a run whose reads after drop session one need not
/// all have been made.
fn assert_marked(d: &Dropped, stop: places::Stop) {
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
}

#[tokio::test]
async fn a_failure_of_drop_session_one_with_x_s_account_unread_is_marked_left_resting() {
    // Drop session one fails with no stop once X was answered, X resting, and the read of X's
    // account after it fails with no provider refusal (the first GET /user/positions after X's
    // place, answered HTTP 500). X counts as resting: LeftResting is marked for the owner's hand
    // cancel (0151 item 6), said as counted resting (FBC-gisx's stop, the owner's ruling, which
    // runs on every path), after the session's failure, which is kept.
    let model = QfexModel {
        rest_fails: vec![0],
        ..QfexModel::default()
    };
    let d = dropped_hooked(model, "drop-one-failed-unread", |t| {
        t.fail_after_place = Some(0);
    })
    .await;
    assert_marked(&d, places::Stop::LeftResting);
    let error = "a test's session failed after X's answer";
    let stopped = format!(
        "{error}; LeftResting after drop session one, X's account not read after it (Positions), \
         so X counts as resting (decision 0005): {}; no further session runs",
        places::COUNTED_RESTING
    );
    assert_eq!(d.ev.stopped.as_deref(), Some(stopped.as_str()));
    let run = d.drop_run();
    assert_eq!(run.error.as_deref(), Some(error));
    assert!(run.x.submitted && run.x.acked, "{run:?}");
    assert!(
        !run.x_read && !run.x_resting && run.two.is_none(),
        "{run:?}"
    );
    let model = d.venue.model.lock().unwrap();
    assert!(
        (model.orders.iter()).all(|o| o.state == model::State::Open),
        "X still rests"
    );
    drop(model);
    let (report, _) = d.outputs();
    let said = format!(
        "- X resting when the run ended: {}",
        places::HAND_CANCEL_UNREAD
    );
    assert!(report.contains(&said), "{report}");
}

#[tokio::test]
async fn a_failure_of_drop_session_two_with_x_s_account_unread_is_marked_left_resting() {
    // PR #273's second review, its first P1: drop session two fails with no stop once Y was
    // answered, X and Y resting, and the read of X's account after it fails with no provider
    // refusal (the seventh GET /user/positions after X's place: after the comparison, the two
    // account reads after drop session one, the two before drop session two and session two's
    // resync). X and Y count as resting: LeftResting is marked (0151 item 6), so the next run
    // needs --reconciled, said as counted resting with the orders that count (FBC-gisx's stop,
    // the owner's ruling), and the run ends on the session's failure.
    let model = QfexModel {
        rest_fails: vec![6],
        ..QfexModel::default()
    };
    let d = dropped_hooked(model, "drop-two-failed-unread", |t| {
        t.fail_after_place = Some(1);
    })
    .await;
    assert_marked(&d, places::Stop::LeftResting);
    let stopped = d.ev.stopped.as_deref().unwrap_or_default();
    let said = format!(
        "a test's session failed after Y's answer; LeftResting after drop session two, X's \
         account not read after it (Positions), so X and Y count as resting (decision 0005): {}; \
         no further session runs",
        places::COUNTED_RESTING
    );
    assert_eq!(stopped, said);
    let two = d.two();
    assert_eq!(
        two.error.as_deref(),
        Some("a test's session failed after Y's answer")
    );
    assert!(two.y.submitted && two.y.acked, "{two:?}");
    assert!(!two.read && !two.x_read && !two.resting, "{two:?}");
    assert_eq!(two.ended_by, Some(stage::GRACEFUL));
    let model = d.venue.model.lock().unwrap();
    let open = (model.orders.iter()).filter(|o| o.state == model::State::Open);
    assert_eq!(open.count(), 2, "X and Y still rest");
    drop(model);
    let (report, _) = d.outputs();
    let said = format!("{X_AT_END}{}", places::X_COUNTED_AFTER_TWO);
    assert!(report.contains(&said), "{report}");
    assert!(d.ev.sessions.iter().all(|s| s.account == "primary"));
}

#[tokio::test]
async fn a_read_before_the_drop_not_made_stops_drop_session_two_before_its_drop() {
    // PR #273's second review: beside Y the account holds an order no run placed, and the read
    // before the drop fails (its GET /user/positions answered HTTP 500, the sixth after session
    // two's resync's), so nothing looked at the account. The drop is not made, so it cannot
    // cancel that order out of sight: a stop, LeftResting, that cancels nothing (0151 item 2),
    // the session ending by a graceful close, X, Y and that order left for the owner.
    let model = QfexModel {
        foreign_order_on: Some(1),
        rest_fails: vec![6],
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-two-before-drop-unread").await;
    assert_stopped(&d, places::Stop::LeftResting);
    let two = d.two();
    assert_eq!(two.before_drop, None, "{two:?}");
    assert_eq!(two.ended_by, Some(stage::GRACEFUL));
    assert_eq!(
        (two.cancels.as_ref(), two.after_drop.as_ref()),
        (None, None)
    );
    assert!(d.drop_run().verdict.is_none());
    assert_eq!(d.count("cancel_order"), 0);
    let what = d.what();
    assert!(!what.iter().any(|w| w.contains(" drop ")), "{what:?}");
    let model = d.venue.model.lock().unwrap();
    let open = (model.orders.iter()).filter(|o| o.state == model::State::Open);
    assert_eq!(open.count(), 3, "X, Y and the order not ours still rest");
    drop(model);
    let stopped = d.ev.stopped.as_deref().unwrap_or_default();
    assert!(
        stopped.starts_with("LeftResting in drop session two"),
        "{stopped}"
    );
    let _ = d.at("READ before the drop: primary: not made, so no drop");
}

#[tokio::test]
async fn a_position_the_streams_never_showed_in_the_read_before_the_drop_stops_before_it() {
    // PR #273's second review: the position half of the read before the drop. With Y's place the
    // account's position moves with no fill frame, so the read lists X and Y but shows a position
    // the registry does not hold: ReadDisagrees (decision 0148 item 8), before the drop and
    // cancelling nothing. What the session showed of X and Y as it dropped is not measured, since
    // it did not drop.
    let model = QfexModel {
        position_on_place_on: Some(1),
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-two-position-before-drop").await;
    assert_stopped(&d, places::Stop::ReadDisagrees);
    let two = d.two();
    assert_eq!(two.before_drop, Some([true, true]), "{two:?}");
    assert!(two.before_drop_disagrees, "{two:?}");
    assert_eq!(two.ended_by, Some(stage::GRACEFUL));
    assert_eq!(d.count("cancel_order"), 0);
    let what = d.what();
    assert!(!what.iter().any(|w| w.contains(" drop ")), "{what:?}");
    let (report, json) = d.outputs();
    let not = "not measured: the session did not reach its drop";
    let said = format!(
        "- A CANCELLED of X before the drop: {not}; Y open as the connection dropped: {not}"
    );
    assert!(report.contains(&said), "{report}");
    let two = &json["drop_tests"][0]["session_two"];
    assert_eq!(
        (&two["x_cancelled_before_drop"], &two["y_open_at_drop"]),
        (&Value::Null, &Value::Null),
        "{two}"
    );
    // A session that dropped measures both.
    let d = dropped(QfexModel::default(), "drop-two-measured-at-drop").await;
    let (report, json) = d.outputs();
    let said = "- A CANCELLED of X before the drop: no; Y open as the connection dropped: yes";
    assert!(report.contains(said), "{report}");
    let two = &json["drop_tests"][0]["session_two"];
    assert_eq!(
        (&two["x_cancelled_before_drop"], &two["y_open_at_drop"]),
        (&Value::Bool(false), &Value::Bool(true)),
        "{two}"
    );
}

/// A run on `model` whose verdict stops `judged` and whose read of every named account after the
/// drop outranks it with `stop` ([`stage`]'s `outranks`): the run stops `stop`, after drop
/// session two, its report saying the position is the owner's when `stop` is `NotFlat`.
async fn outranked(model: QfexModel, what: &str, judged: places::Stop, stop: places::Stop) {
    let d = dropped(model, what).await;
    assert_eq!(d.verdict().stop(), Some(judged), "{:?}", d.verdict());
    assert_stopped(&d, stop);
    let stopped = d.ev.stopped.as_deref().unwrap_or_default();
    let after = format!("{} after drop session two", stop.name());
    assert!(stopped.starts_with(&after), "{stopped}");
    if stop == places::Stop::NotFlat {
        let (report, _) = d.outputs();
        assert!(report.contains(places::OWNERS_POSITION), "{report}");
    }
}

#[tokio::test]
async fn an_order_not_ours_the_reads_after_a_failed_drop_test_list_outranks_drop_failed() {
    // PR #273's second review: the read of every named account after the drop outranks a
    // verdict's stop that comes after its own in the verdict's order. An order not ours it lists
    // outranks a test that only did not pass (Y not on historic-orders).
    let model = QfexModel {
        historic_omits_last: true,
        after_drop_historic: Some(model::AfterHistoric::Foreign),
        ..QfexModel::default()
    };
    let (judged, stop) = (places::Stop::DropFailed, places::Stop::ReadDisagrees);
    outranked(model, "drop-two-rank-foreign", judged, stop).await;
}

#[tokio::test]
async fn a_position_the_reads_after_the_drop_show_outranks_an_arm_cancel_unreported() {
    // A position the read of every named account after the drop shows outranks an arm-time
    // cancel QFEX did not report, the position then the owner's (0151 item 9).
    let model = QfexModel {
        arm_cancels_open: model::ArmCancels::Silent,
        after_drop_historic: Some(model::AfterHistoric::Position),
        ..QfexModel::default()
    };
    let (judged, stop) = (places::Stop::ArmCancelUnreported, places::Stop::NotFlat);
    outranked(model, "drop-two-rank-silent", judged, stop).await;
}

#[tokio::test]
async fn a_position_the_reads_after_the_drop_show_outranks_an_order_not_ours_after_it() {
    // PR #273's second review, its P3 on the stops' order: a position the read of every named
    // account after the drop shows outranks an order not ours the reads after the drop listed,
    // as NotFlat comes before ReadDisagrees in the verdict's own order (0151 item 9).
    let model = QfexModel {
        foreign_on_drop: true,
        after_drop_historic: Some(model::AfterHistoric::Position),
        ..QfexModel::default()
    };
    let (judged, stop) = (places::Stop::ReadDisagrees, places::Stop::NotFlat);
    outranked(model, "drop-two-rank-not-flat", judged, stop).await;
}

#[tokio::test]
async fn a_fill_a_trades_page_names_before_a_refusal_is_said_the_owner_s_position() {
    // PR #273's second review: on pages of one, X fills as a maker as its trades are first asked
    // after the drop, that page naming it, and the next page is refused by a provider rate
    // refusal. The refusal is the run's stop (decision 0148 item 8), but the fill is still the
    // owner's position (0151 item 9), and the report says so beside the stop.
    let model = QfexModel {
        fill_on_trades_page: Some(1),
        rate_limit: Some(model::Limited::TradesPage(2)),
        ..QfexModel::default()
    };
    let d = dropped_with(model, "drop-two-fill-then-refused", |argv| {
        set(argv, "--page-size", "1")
    })
    .await;
    let on = places::RefusedOn::ReadSocket;
    assert_stopped(&d, places::Stop::RateRefused { on });
    let verdict = d.verdict();
    let x = verdict::Which::X;
    assert!(
        verdict.failures.contains(&verdict::Failure::Traded(x)),
        "{verdict:?}"
    );
    let (report, _) = d.outputs();
    assert!(report.contains(places::OWNERS_POSITION_BESIDE), "{report}");
    assert!(!report.contains(places::OWNERS_POSITION), "{report}");
}

#[tokio::test]
async fn a_refusal_after_the_trades_were_read_whole_leaves_them_read_whole() {
    // PR #273's second review: X's and Y's trades after the drop are read whole, then the first
    // GET /user/historic-orders is answered HTTP 429. The refusal is a failure, and the run's
    // stop, of its own; it does not make the trades not read whole.
    let model = QfexModel {
        rate_limit: Some(model::Limited::HistoricAfterDrop),
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-two-historic-refused").await;
    let on = places::RefusedOn::Rest;
    assert_stopped(&d, places::Stop::RateRefused { on });
    let after = d.two().after.as_ref().expect("X and Y were read");
    assert!(after.trades_whole && !after.historic_whole, "{after:?}");
    let verdict = d.verdict();
    use verdict::Failure::{HistoricNotRead, RateRefused, TradesNotRead};
    assert_eq!(
        verdict.failures,
        [RateRefused, HistoricNotRead],
        "{verdict:?}"
    );
    assert!(!verdict.failures.contains(&TradesNotRead));
}

#[tokio::test]
async fn x_s_status_read_whole_after_drop_session_one_and_not_listed_is_said_not_listed() {
    // PR #273's second review: X's GET /user/historic-orders after drop session one is read
    // whole and names no terminal status of X, and an order not ours then stops the run before
    // drop session two. The report says X's status was not listed, apart from Y's, never read.
    let model = QfexModel {
        foreign_after_close: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-one-x-not-listed").await;
    assert_stopped(&d, places::Stop::ReadDisagrees);
    let run = d.drop_run();
    assert!(run.two.is_none(), "{run:?}");
    let after = run.after.as_ref().expect("X was read");
    assert_eq!(
        (after.error.as_ref(), after.terminal_status.as_ref()),
        (None, None)
    );
    let (report, _) = d.outputs();
    assert!(
        report.contains("- terminal_status on GET /user/historic-orders: X not listed; Y not read"),
        "{report}"
    );
}

#[tokio::test]
async fn the_other_accounts_cleanness_is_not_known_when_a_read_of_one_failed() {
    // PR #273's second review: the subaccount's GET /user/positions fails after drop session one
    // and again before drop session two, so the run stops LeftResting, every read of it made
    // having been clean. Whether it stayed clean is not known, not yes.
    let model = QfexModel {
        rest_fails: vec![2, 4],
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-others-unread").await;
    assert_marked(&d, places::Stop::LeftResting);
    let run = d.drop_run();
    assert_eq!(run.others_clean, places::OthersClean::NotKnown, "{run:?}");
    let (report, json) = d.outputs();
    let said = "- Whether the other named accounts stayed clean: not known: every read of another \
                named account made was clean, but one could not be made";
    assert!(report.contains(said), "{report}");
    assert_eq!(json["drop_tests"][0]["others_clean"], "not known");
}

// ----- Stage two's parts (FBC-su9i; decisions 0005, 0148 items 2 and 6, 0151 item 5) -----

#[test]
fn an_exact_echo_opens_the_gate() {
    let cids: Vec<String> = crossing::minted(2)
        .into_iter()
        .map(crossing::text_of)
        .collect();
    let mut model = QfexModel::default();
    crossing::session(&mut model, 0);
    let (mut tee, tap) = crossing::teed();
    // Before any ACK the run's gate is shut.
    let gate = stage_two::EchoGate::of(&tap.kept());
    assert!(!gate.open, "{gate:?}");
    // The model's ACK echoes our client id byte for byte, as the run's tee reads the frame: open.
    let frame = crossing::add_order(&cids[0], "BUY", "99.50");
    crossing::exchange(&mut model, &mut tee, &frame);
    let gate = stage_two::EchoGate::of(&tap.kept());
    assert!(gate.open, "{gate:?}");
    assert_eq!(gate.echoes.acks.exact, 1);
    assert!(gate.says().starts_with("open: an ACK in this run echoed"));
    assert_eq!(tap.stop(), None);
    // Per run, in memory: the next run's tee, a new one, is shut whatever this one heard.
    let (_, next) = crossing::teed();
    assert!(!stage_two::EchoGate::of(&next.kept()).open);
    // An exact ACK of a client id the run never sent opens nothing: it is not ours.
    let (mut tee, tap) = crossing::teed();
    crossing::sent(&mut tee, &crossing::add_order(&cids[0], "BUY", "99.50"));
    let foreign = model.trade(
        0,
        &Frame::text(crossing::add_order(&cids[1], "BUY", "99.50")),
    );
    crossing::heard(&mut tee, &foreign.unwrap());
    assert!(!stage_two::EchoGate::of(&tap.kept()).open);
    assert_eq!(tap.stop(), Some(places::Stop::StrayOrder));
}

#[test]
fn an_upper_cased_or_escaped_echo_does_not_open_the_gate() {
    let cid = crossing::text_of(crossing::minted(1)[0]);
    let frame = crossing::add_order(&cid, "BUY", "99.50");
    for echo in [
        model::Echo::Upper,
        model::Echo::Escaped,
        model::Echo::Absent,
        model::Echo::Null,
        model::Echo::Empty,
    ] {
        let mut model = QfexModel {
            ack_echo: echo,
            ..QfexModel::default()
        };
        crossing::session(&mut model, 0);
        let (mut tee, tap) = crossing::teed();
        crossing::exchange(&mut model, &mut tee, &frame);
        let gate = stage_two::EchoGate::of(&tap.kept());
        assert!(!gate.open, "{echo:?}");
        let acks = gate.echoes.acks;
        let counted = match echo {
            model::Echo::Upper | model::Echo::Escaped => acks.differs,
            model::Echo::Absent | model::Echo::Null => acks.absent,
            _ => acks.empty,
        };
        // Its exactness is recorded. An ACK of an order not yet known ours with no client id
        // names no order of ours, so it is a stray's (0151 item 13), a stop; it is still counted
        // among the ACKs by how it came, since nothing in it ties it to an order.
        assert_eq!((acks.total(), counted), (1, 1), "{echo:?}");
        match echo {
            model::Echo::Upper | model::Echo::Escaped => {
                assert!(acks.says().starts_with("1 not byte for byte"), "{echo:?}");
                assert_eq!(tap.stop(), None, "{echo:?}");
            }
            model::Echo::Absent | model::Echo::Null => {
                assert_eq!(acks.says(), "1 absent or null", "{echo:?}");
                assert_eq!(tap.stop(), Some(places::Stop::StrayOrder), "{echo:?}");
            }
            _ => {
                assert_eq!(acks.says(), "1 empty", "{echo:?}");
                assert_eq!(tap.stop(), Some(places::Stop::StrayOrder), "{echo:?}");
            }
        }
    }
    // An ack or err echo of the add_order naming our client id exactly is recorded as exact, and
    // never opens the gate.
    let (mut tee, tap) = crossing::teed();
    crossing::sent(&mut tee, &frame);
    crossing::heard_echo(&mut tee, "ack", &frame, &cid);
    crossing::heard_echo(&mut tee, "err", &frame, &cid);
    let gate = stage_two::EchoGate::of(&tap.kept());
    assert!(!gate.open);
    assert_eq!(
        (gate.echoes.ack_echoes.exact, gate.echoes.err_echoes.exact),
        (1, 1)
    );
}

/// The registries and reads of a run whose drop tests passed on both named accounts, X and Y
/// shown ended on all three of 0151 item 5's reads, and nothing else placed.
struct Ran {
    drops: [fbc_oms::Registry; 2],
    /// Each account's X and Y.
    xy: [[fbc_core::ClientOrderId; 2]; 2],
    ended: [Vec<fbc_core::ClientOrderId>; 2],
    cross: [fbc_oms::Registry; 2],
    reads: [capture::AccountRead; 2],
}

impl Ran {
    fn new() -> Ran {
        let cids = crossing::minted(4);
        let xy = [[cids[0], cids[1]], [cids[2], cids[3]]];
        // X and Y rest in the drop registry as its streams left them: nothing settles them.
        let drops = xy.map(|[x, y]| {
            let mut reg = fbc_oms::Registry::new();
            crossing::holding(&mut reg, x, crossing::As::Open);
            crossing::holding(&mut reg, y, crossing::As::Open);
            reg
        });
        Ran {
            drops,
            xy,
            ended: [xy[0].to_vec(), xy[1].to_vec()],
            cross: [fbc_oms::Registry::new(), fbc_oms::Registry::new()],
            reads: [crossing::clean(), crossing::clean()],
        }
    }

    /// Whether a crossing order may be placed now.
    fn judge(&self) -> Result<(), stage_two::Blocked> {
        let mut built = Vec::new();
        for (n, account) in ["primary", "subaccount"].into_iter().enumerate() {
            built.push(stage_two::Built {
                account,
                step: ledger::Step::Drop,
                registry: &self.drops[n],
                ended: &self.ended[n],
            });
            built.push(stage_two::Built {
                account,
                step: ledger::Step::Cross,
                registry: &self.cross[n],
                ended: &[],
            });
        }
        let reads = [
            ("primary", Some(&self.reads[0])),
            ("subaccount", Some(&self.reads[1])),
        ];
        stage_two::no_other_order(crossing::INST, &built, &reads)
    }
}

/// The refusal of a registry of `account` at `step`.
fn blocked(account: &'static str, step: ledger::Step) -> Result<(), stage_two::Blocked> {
    Err(stage_two::Blocked::Registry { account, step })
}

#[test]
fn a_crossing_order_waits_for_no_other_order_of_ours() {
    // X and Y shown ended after the drop, and nothing else of ours: a crossing order may go.
    assert_eq!(Ran::new().judge(), Ok(()));
    // X or Y not shown ended on every read after the drop stays counted resting (0151 item 5).
    let none = places::TwoRun::default();
    assert_eq!(stage_two::ended_after_drop(&none), [false, false]);
    let mut ran = Ran::new();
    ran.ended[0] = vec![ran.xy[0][0]];
    assert_eq!(ran.judge(), blocked("primary", ledger::Step::Drop));
    // Another order of the account's, open or in flight on the cross registry, before its
    // crossing post-only.
    for stands in [crossing::As::Open, crossing::As::Sent] {
        let mut ran = Ran::new();
        let other = crossing::minted(1)[0];
        crossing::holding(&mut ran.cross[0], other, stands);
        assert_eq!(
            ran.judge(),
            blocked("primary", ledger::Step::Cross),
            "{stands:?}"
        );
    }
    // One that ended counts for nothing.
    let mut ran = Ran::new();
    crossing::holding(
        &mut ran.cross[0],
        crossing::minted(1)[0],
        crossing::As::Refused,
    );
    assert_eq!(ran.judge(), Ok(()));
    // The account's never-arming read listing an order, a TWAP, or REST counting one.
    type Change = fn(&mut capture::AccountRead);
    let listed: [Change; 3] = [
        |r| r.page_orders = 1,
        |r| r.page_twaps = 1,
        |r| r.rest_open_orders = 1,
    ];
    for list in listed {
        let mut ran = Ran::new();
        list(&mut ran.reads[0]);
        let refused = stage_two::Blocked::Listed { account: "primary" };
        assert_eq!(ran.judge(), Err(refused.clone()));
        assert!(
            refused
                .says()
                .starts_with("primary's never-arming read lists")
        );
    }
}

#[test]
fn a_crossing_order_waits_for_no_order_of_ours_on_any_named_account() {
    // The subaccount's X not shown ended, its cross registry holding an order, its read listing
    // one, or no read of it at all: each keeps the primary account's crossing order back.
    let mut ran = Ran::new();
    ran.ended[1].clear();
    assert_eq!(ran.judge(), blocked("subaccount", ledger::Step::Drop));
    let mut ran = Ran::new();
    crossing::holding(
        &mut ran.cross[1],
        crossing::minted(1)[0],
        crossing::As::Open,
    );
    assert_eq!(ran.judge(), blocked("subaccount", ledger::Step::Cross));
    let mut ran = Ran::new();
    ran.reads[1].page_orders = 1;
    let listed = stage_two::Blocked::Listed {
        account: "subaccount",
    };
    assert_eq!(ran.judge(), Err(listed));
    let ran = Ran::new();
    let built: Vec<stage_two::Built<'_>> = Vec::new();
    let reads = [("primary", Some(&ran.reads[0])), ("subaccount", None)];
    let unread = stage_two::no_other_order(crossing::INST, &built, &reads);
    let want = stage_two::Blocked::Unread {
        account: "subaccount",
    };
    assert_eq!(unread, Err(want));
    // An order of ours on another market is not on this one.
    let mut ran = Ran::new();
    let elsewhere = fbc_core::NewOrder {
        inst: fbc_core::InstrumentId::new(2),
        ..crossing::order(crossing::minted(1)[0], 1)
    };
    ran.cross[1].insert(elsewhere).unwrap();
    assert_eq!(ran.judge(), Ok(()));
}

#[test]
fn an_unknown_crossing_post_only_blocks_the_taker_ioc() {
    // The crossing post-only, left Unknown at its deadline: never resent (decision 0005), it
    // stays counted resting on the cross registry, so no_other_order() refuses the taker IOC.
    let mut ran = Ran::new();
    let crossing_post_only = crossing::minted(1)[0];
    crossing::holding(&mut ran.cross[0], crossing_post_only, crossing::As::Unknown);
    let state = ran.cross[0].get(crossing_post_only).map(|r| r.state());
    assert_eq!(state, Some(fbc_oms::OrdState::Unknown));
    let refused = ran.judge();
    assert_eq!(refused, blocked("primary", ledger::Step::Cross));
    let said = refused.unwrap_err().says();
    assert!(said.contains("Unknown order is never resent"), "{said}");
    // On the subaccount's, it keeps the primary account's taker IOC back too.
    let mut ran = Ran::new();
    crossing::holding(&mut ran.cross[1], crossing_post_only, crossing::As::Unknown);
    assert_eq!(ran.judge(), blocked("subaccount", ledger::Step::Cross));
}

// ----- FBC-su9i: stage two's crossing post-only, run against the model -----

impl Dropped {
    /// Stage two, as the evidence holds it.
    fn stage_two(&self) -> &stage_two::StageTwo {
        self.ev.stage_two.as_ref().expect("stage two's record")
    }

    /// The `n`th crossing session, in run order.
    fn cross(&self, n: usize) -> &stage_two::CrossRun {
        &self.stage_two().crosses[n]
    }

    /// The trade connections of the crossing sessions, in order: after the drop tests' two
    /// sessions each, every other session, since each account's taker IOC session follows its
    /// crossing session (FBC-ou9g).
    fn cross_conns(&self) -> Vec<String> {
        let drops = 2 * self.ev.drops.len();
        let conns = self.session_conns().into_iter().skip(drops);
        conns.step_by(2).collect()
    }

    /// The trade connections of the taker IOC sessions, in order (FBC-ou9g): each the session
    /// after its account's crossing session.
    fn take_conns(&self) -> Vec<String> {
        let drops = 2 * self.ev.drops.len();
        let conns = self.session_conns().into_iter().skip(drops + 1);
        conns.step_by(2).collect()
    }

    /// Whether the run's journal holds a `kind` frame it wrote naming `id`.
    fn wrote_naming(&self, kind: &str, id: &str) -> bool {
        let journal = self.dir.join("evidence").join("journal");
        let reader = fbc_journal::JournalReader::open(&journal, 0).unwrap();
        reader.entries().any(|e| match e.unwrap().record {
            fbc_journal::Record::Outbound { frame, .. } => {
                let text = String::from_utf8_lossy(frame.bytes()).into_owned();
                text.contains(&format!(r#""type":"{kind}""#)) && text.contains(id)
            }
            _ => false,
        })
    }

    /// The model's crossing post-only orders, the orders after X and Y on each named account.
    fn crossing_orders(&self) -> Vec<model::Order> {
        let model = self.venue.model.lock().unwrap();
        let drops = 2 * self.ev.drops.len();
        let after = model.orders.iter().skip(drops);
        // The post-only orders alone: no taker IOC or flatten IOC (FBC-ou9g).
        after.filter(|o| o.kind == "ALO").cloned().collect()
    }
}

/// The model's default, but every post-only priced through the touch rests as any other.
fn resting_cross() -> QfexModel {
    QfexModel {
        alo_cross: None,
        ..QfexModel::default()
    }
}

/// Asserts that `d` stopped in or after a crossing session with `stop`, by name and with what it
/// is, its report and evidence naming it and passing the privacy check, its marker written, and
/// every named account read after the crossing session.
fn assert_cross_stopped(d: &Dropped, stop: places::Stop) {
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
    let after: Vec<&str> = (d.ev.reads.iter())
        .filter(|r| r.before == refusals::AFTER_CROSS)
        .map(|r| r.label.as_str())
        .collect();
    assert_eq!(
        after[..2],
        ["primary (cross)", "subaccount"],
        "{}",
        d.printed
    );
    let (report, json) = d.outputs();
    assert!(
        report.contains(&format!("**The place stages' stop:** {name}.")),
        "{report}"
    );
    assert!(
        report.contains("| after the crossing session | primary (cross) |"),
        "{report}"
    );
    assert_eq!(json["stop"], name);
    assert_eq!(json["stage_two"]["crossings"][0]["stop"], name);
    assert_report_and_evidence_pass_the_privacy_check(&d.dir.join("evidence"));
    assert_no_secret(&d.dir.join("evidence"));
    d.venue.assert_served();
}

/// The report's section on the crossing post-only on `account`.
fn cross_section(report: &str, account: &str) -> String {
    let heading = format!("### The crossing post-only on {account} (FBC-su9i)");
    let section = report
        .split(&heading)
        .nth(1)
        .unwrap_or_else(|| panic!("{report}"));
    let section = section.split("\n## ").next().unwrap();
    section.split("\n### ").next().unwrap().to_owned()
}

/// The report's section on `account`'s taker IOC session (FBC-ou9g), up to the next section.
fn take_section(report: &str, account: &str) -> String {
    let heading = format!("### The taker IOC on {account} (FBC-ou9g)");
    let section = report
        .split(&heading)
        .nth(1)
        .unwrap_or_else(|| panic!("{report}"));
    let section = section.split("\n## ").next().unwrap();
    section.split("\n### ").next().unwrap().to_owned()
}

#[tokio::test]
async fn an_absent_echo_ends_the_run_after_every_accounts_drop_test() {
    // "Absent" is the exact echo's absence: no ACK of the run echoes our client id byte for
    // byte, each writing it upper-cased, or with its first character a JSON escape. The gate
    // stays shut: every named account's drop test still runs, and passes, and the run then ends
    // NoEcho, which is no fault and writes no stop marker, placing no crossing order (decision
    // 0148 item 2). An ACK whose client id member is absent, null or empty never gets that far.
    for (echo, what) in [
        (model::Echo::Upper, "cross-no-echo-upper"),
        (model::Echo::Escaped, "cross-no-echo-escaped"),
    ] {
        let model = QfexModel {
            ack_echo: echo,
            ..QfexModel::default()
        };
        let d = dropped(model, what).await;
        assert_eq!(d.ev.drops.len(), 2, "{echo:?}: {}", d.printed);
        for run in &d.ev.drops {
            let passed = run.verdict.as_ref().is_some_and(|v| v.passed);
            assert!(passed, "{echo:?}: {run:?}");
            assert!(!run.x.exact_echo, "{echo:?}");
        }
        assert_eq!(d.ev.stop, Some("NoEcho"), "{echo:?}");
        assert!(d.marker().is_none(), "{echo:?}");
        assert!(!d.printed.contains("MARKER written"), "{}", d.printed);
        let stopped = d.ev.stopped.clone().unwrap();
        assert!(
            stopped.starts_with("NoEcho once every named account's drop test ran: no ACK"),
            "{stopped}"
        );
        assert!(d.at("VERDICT drop test on subaccount") < d.at("GATE shut: no ACK"));
        assert!(d.printed.contains(&format!("STOPPED {stopped}")));
        // No crossing session and no crossing order: X and Y alone, on four sessions.
        assert_eq!(d.ev.sessions.len(), 4);
        assert_eq!(d.count("add_order"), 4);
        assert!(!d.printed.contains("REGISTRY cross"), "{}", d.printed);
        let two = d.stage_two();
        assert_eq!(two.gate, Some(false));
        assert!(two.crosses.is_empty());
        assert_eq!((two.echoes.acks.exact, two.echoes.acks.differs), (0, 4));
        let (report, json) = d.outputs();
        assert!(
            report.contains(&format!(
                "- The exact-echo gate: {}",
                stage_two::EchoGate::shut_said()
            )),
            "{report}"
        );
        assert!(report.contains("**The place stages' stop:** NoEcho."));
        assert_eq!(json["stage_two"]["exact_echo_gate"], false);
        assert_eq!(json["stop"], "NoEcho");
        assert!(!d.dir.join("evidence").join(stage::PLACES_SLICE).exists());
        assert_report_and_evidence_pass_the_privacy_check(&d.dir.join("evidence"));
        d.venue.assert_served();
    }
    // An ACK naming no client id at all, absent, null or empty, is an order_response naming no
    // order of ours, since nothing yet ties X to us: a stray's, a stop (0151 item 13, which wins
    // over the ticket's NoEcho for these three) in drop session one, marked, so the run never
    // reaches the gate, which is not judged, and places no crossing order.
    for (echo, what) in [
        (model::Echo::Absent, "cross-absent-echo"),
        (model::Echo::Null, "cross-null-echo"),
        (model::Echo::Empty, "cross-empty-echo"),
    ] {
        let model = QfexModel {
            ack_echo: echo,
            ..QfexModel::default()
        };
        let d = dropped(model, what).await;
        assert_stopped(&d, places::Stop::StrayOrder);
        assert_eq!(d.ev.drops.len(), 1, "{echo:?}");
        assert_eq!(d.stage_two().gate, None, "{echo:?}");
        let acks = d.stage_two().echoes.acks;
        let counted = match echo {
            model::Echo::Empty => acks.empty,
            _ => acks.absent,
        };
        assert_eq!((acks.total(), counted), (1, 1), "{echo:?}");
        let (report, _) = d.outputs();
        assert!(
            report.contains("- The exact-echo gate: not judged: the run stopped before"),
            "{report}"
        );
    }
}

#[tokio::test]
async fn the_crossing_post_only_is_refused_and_recorded() {
    use model::AloCross;
    use stage_two::Crossed;
    // CANCELLED, which ends it: recorded on each named account, the run going on to the next and
    // ending after the last, naming FBC-ou9g. An unmapped status or an err echo, which the
    // adapter reads as no end: recorded, its place ending Unknown at its deadline, never resent
    // and counted resting (decision 0005), so an order of ours left resting, a stop, and no
    // crossing order on the next account (decision 0148 item 6).
    let cases = [
        (
            AloCross::Cancelled,
            Crossed::Cancelled { exact: true },
            None,
        ),
        (
            AloCross::Unmapped,
            Crossed::Unmapped {
                status: model::UNMAPPED.to_owned(),
                exact: true,
            },
            Some(places::Stop::LeftResting),
        ),
        (
            AloCross::Err,
            Crossed::ErrEcho(tee::EchoCid::Exact),
            Some(places::Stop::LeftResting),
        ),
    ];
    for (how, want, stop) in cases {
        let model = QfexModel {
            alo_cross: Some(how),
            ..QfexModel::default()
        };
        let d = dropped(model, "cross-refused").await;
        let run = d.cross(0);
        assert_eq!(run.crossed.as_ref(), Some(&want), "{how:?}: {}", d.printed);
        assert!(
            run.started && run.placed.submitted && !run.placed.acked,
            "{run:?}"
        );
        assert!(run.blocked.is_none(), "{run:?}");
        // One crossing post-only on its session, a post-only GTC at the far touch, never resent.
        let conns = d.cross_conns();
        let placed = |conn: &String| {
            let place = format!("trade {conn} add_order");
            d.what().iter().filter(|w| **w == place).count()
        };
        assert_eq!(placed(&conns[0]), 1, "{how:?}");
        // QFEX's err echo makes no order; each other answer names the one it ended.
        if how != AloCross::Err {
            let crossing = &d.crossing_orders()[0];
            assert_eq!(
                (crossing.kind.as_str(), crossing.tif.as_str()),
                ("ALO", "GTC")
            );
            assert_eq!(crossing.price, super::json("100.1"));
        }
        assert!(
            d.printed
                .contains(&format!("CROSSED on primary: {}", want.says()))
        );
        assert!(
            d.printed
                .contains("ANSWERED the crossing post-only on primary: it does not rest"),
            "{how:?}: {}",
            d.printed
        );
        let (report, json) = d.outputs();
        let section = cross_section(&report, "primary");
        assert!(
            section.contains(&format!("- How QFEX refused it: {}", want.says())),
            "{section}"
        );
        assert_eq!(json["stage_two"]["crossings"][0]["refused"], want.says());
        match stop {
            None => {
                assert_eq!((d.ev.stop, d.marker()), (None, None), "{how:?}");
                assert_eq!(d.ev.stopped.as_deref(), Some(stage::NOT_BUILT));
                assert_eq!(d.stage_two().crosses.len(), 2);
                assert_eq!(d.cross(1).crossed.as_ref(), Some(&want));
                assert_eq!(placed(&conns[1]), 1);
                assert!(section.contains(&format!("- What followed: {}", taker::THEN_TAKE)));
                let last = take_section(&report, "subaccount");
                assert!(
                    last.contains(&format!("- What followed: {}", taker::LAST_TAKE)),
                    "{last}"
                );
            }
            Some(stop) => {
                assert_cross_stopped(&d, stop);
                assert!(run.counted_resting && !run.resting, "{run:?}");
                assert_eq!(run.placed.outcome.as_deref(), Some("Unknown"));
                assert_eq!(d.stage_two().crosses.len(), 1, "{how:?}");
                assert_eq!(d.cross_conns().len(), 1);
                assert!(
                    d.ev.stopped
                        .as_deref()
                        .unwrap()
                        .starts_with("LeftResting after the crossing session"),
                    "{:?}",
                    d.ev.stopped
                );
            }
        }
    }
}

#[tokio::test]
async fn an_unknown_place_is_never_resent_and_is_reported() {
    // The crossing post-only's ACK never comes (QFEX rests it, and answers only an hour later):
    // its place ends Unknown at its deadline, recorded and reported; the probe runs no Unknown
    // ladder, so it is never resent (decision 0005); it stays counted resting, an order of ours
    // left resting after the session, a stop, so no taker IOC and no crossing order on the next
    // account (decision 0148 item 6).
    let model = QfexModel {
        ack_delay: Some(Duration::from_secs(3600)),
        ack_delay_on: 4,
        ..resting_cross()
    };
    let d = dropped(model, "cross-unknown").await;
    assert_cross_stopped(&d, places::Stop::LeftResting);
    let run = d.cross(0);
    assert_eq!(
        run.crossed,
        Some(stage_two::Crossed::Unknown),
        "{}",
        d.printed
    );
    assert_eq!(run.placed.outcome.as_deref(), Some("Unknown"));
    assert!(!run.placed.acked && !run.placed.exact_echo);
    assert!(run.counted_resting, "{run:?}");
    // One add_order on the crossing session and none after it: never resent.
    let conns = d.cross_conns();
    assert_eq!(conns.len(), 1);
    assert_eq!(d.places_by_session(), [1, 1, 1, 1, 1]);
    assert_eq!(d.count("add_order"), 5);
    let (report, json) = d.outputs();
    let section = cross_section(&report, "primary");
    let said = stage_two::Crossed::Unknown.says();
    assert!(
        section.contains(&format!("- How QFEX refused it: {said}")),
        "{section}"
    );
    assert!(said.contains("never resent and stays counted resting (decision 0005)"));
    assert!(
        section.contains("Unknown and never resent (decision 0005)")
            || section.contains(stage_two::HAND_CANCEL_CROSS),
        "{section}"
    );
    assert_eq!(json["stage_two"]["crossings"][0]["counted_resting"], true);
}

/// A run of `model` pressed once the model has seen `places` add_orders, as [`pressed_drop`]
/// presses.
async fn pressed_at_place(model: QfexModel, what: &str, places: usize) -> Dropped {
    pressed_at_place_with(model, what, places, |argv| argv).await
}

/// [`pressed_at_place`] on the run's command line as `argv` changes it.
async fn pressed_at_place_with(
    model: QfexModel,
    what: &str,
    places: usize,
    argv: impl Fn(Argv) -> Argv,
) -> Dropped {
    let (dir, removed) = temp_dir(what);
    let venue = Venue::start(model).await;
    let (mut opts, parsed_places) = parsed(&argv(drop_argv(&dir, &venue))).unwrap();
    opts.rate_user = user();
    let env = environment();
    let printed = venue.printed();
    let lookup = |var: &str| env.get(var).cloned();
    let out: stage::Out = Rc::new(RefCell::new(printed.clone()));
    let (mut press, presses) = interrupt::Presses::channel();
    let hold: stage::Interrupt = Box::pin(std::future::pending());
    let drive = async {
        let placed = |log: &[model::Event]| {
            log.iter()
                .filter(|e| e.what.ends_with(" add_order"))
                .count()
                >= places
        };
        until("the moment to press", || placed(&venue.log())).await;
        press();
    };
    let ran = stage::run_places_until(&opts, &parsed_places, &lookup, out, hold, presses);
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
async fn each_session_cancels_by_venue_id_before_it_stops() {
    // Each crossing post-only rests (the model's answer here): once it met its deadline the
    // session ran its course, and before it stops it cancels every order of ours by venue id, an
    // fbc-oms authorization through its Link, sees it end, compares, and closes gracefully
    // (decision 0132 item 7, 0148 item 8).
    let d = dropped(resting_cross(), "cross-cancels").await;
    assert_eq!((d.ev.stop, d.marker()), (None, None), "{}", d.printed);
    assert_eq!(d.ev.stopped.as_deref(), Some(stage::NOT_BUILT));
    let orders = d.crossing_orders();
    assert_eq!(orders.len(), 2);
    assert!(orders.iter().all(|o| o.state == model::State::Cancelled));
    let what = d.what();
    for (n, conn) in d.cross_conns().iter().enumerate() {
        let run = d.cross(n);
        assert_eq!(run.crossed, Some(stage_two::Crossed::Rested));
        assert!(run.placed.acked && run.placed.exact_echo && run.placed.ack_after.is_some());
        let cancels = run.cancels.as_ref().unwrap();
        assert_eq!(
            (cancels.sent, cancels.held, cancels.blocked, cancels.ended),
            (1, false, false, true)
        );
        assert_eq!(run.ended_by, Some(stage::GRACEFUL));
        assert!(!run.resting && !run.counted_resting && run.read, "{run:?}");
        // Its cancel names its order by venue id, and comes before its close.
        assert!(d.wrote_naming("cancel_order", &orders[n].id), "{n}");
        let at = |w: &str| what.iter().position(|e| e.starts_with(w)).unwrap();
        let cancel = at(&format!("trade {conn} cancel_order"));
        assert!(cancel < at(&format!("cod {conn} close")), "{what:?}");
        let steps: Vec<&str> = run.compared.iter().map(|c| c.step).collect();
        assert_eq!(steps, ["the crossing post-only", "the stop's cancels"]);
    }
    assert!(d.printed.contains(
        "STOP cancels: Kill, then 1 cancels by venue id (decision 0132 item 7, 0148 item 8), \
         every order they named seen to end; the market disarmed"
    ));
    let (report, _) = d.outputs();
    let section = cross_section(&report, "primary");
    assert!(
        section.contains(
            "- The Stop's cancels: Kill, then 1 cancel by venue id; every order they named was \
             seen to end"
        ),
        "{section}"
    );
    assert!(section.contains("- How QFEX refused it: rested: QFEX acknowledged it (ACK)"));
}

#[tokio::test]
async fn stop_on_ctrl_c_cancels_by_venue_id() {
    // The owner's Ctrl-C as the primary account's crossing post-only goes out (the fifth place
    // of the run): the crossing session stops, cancelling it by venue id first (decision 0148
    // item 8), and the run stops, marked, no crossing order on the subaccount.
    let d = pressed_at_place(resting_cross(), "cross-ctrl-c", 5).await;
    assert_cross_stopped(&d, places::Stop::Interrupted);
    let orders = d.crossing_orders();
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0].state, model::State::Cancelled);
    assert!(d.wrote_naming("cancel_order", &orders[0].id));
    assert_eq!(d.count("cancel_order"), 1);
    let run = d.cross(0);
    let cancels = run.cancels.as_ref().unwrap();
    assert!(cancels.ended && !cancels.held, "{cancels:?}");
    assert_eq!(run.ended_by, Some(stage::GRACEFUL));
    assert_eq!(d.stage_two().crosses.len(), 1);
    assert!(
        d.ev.stopped
            .as_deref()
            .unwrap()
            .starts_with("Interrupted in the crossing session"),
        "{:?}",
        d.ev.stopped
    );
}

#[tokio::test]
async fn ctrl_c_during_the_crossing_sessions_stop_cancels_stops_the_run() {
    // The primary account's crossing post-only rests to its deadline, so the crossing session
    // runs its course with no stop and its Stop cancels the order by its venue id (decision 0132
    // item 7). QFEX holds the answer to that cancel, the run's first, until the owner has pressed
    // Ctrl-C while the cancels wait for it: the press ends none of them and is the session's stop
    // (decision 0148 item 8), so the run stops Interrupted, marked, and no crossing order goes
    // out on the subaccount (FBC-su9i's review).
    let model = QfexModel {
        hold_cancel: true,
        ..resting_cross()
    };
    let rpc = |argv| set(argv, "--rpc-timeout-ms", "5000");
    let d = pressed_drop(model, "cross-stop-cancels-ctrl-c", rpc, &[" cancel_order"]).await;
    assert_cross_stopped(&d, places::Stop::Interrupted);
    assert_eq!(d.stage_two().crosses.len(), 1, "{}", d.printed);
    assert_eq!(d.crossing_orders().len(), 1, "{}", d.printed);
    let run = d.cross(0);
    let cancels = run.cancels.as_ref().expect("the session's Stop");
    assert_eq!((cancels.sent, cancels.presses), (1, 1), "{cancels:?}");
    assert!(cancels.ended && !cancels.held, "{cancels:?}");
    assert_eq!(run.ended_by, Some(stage::GRACEFUL));
    assert_eq!(d.count("cancel_order"), 1, "{:?}", d.what());
    let stopped = d.ev.stopped.as_deref().unwrap_or_default();
    assert!(
        stopped.starts_with("Interrupted in the crossing session"),
        "{stopped}"
    );
    assert!(
        d.printed
            .contains("STOP cancels: 1 press of Ctrl-C while they ran, recorded"),
        "{}",
        d.printed
    );
}

#[tokio::test]
async fn ctrl_c_during_stop_cancels_that_the_connections_end_cuts_short_stops_the_run() {
    // As above, but QFEX leaves the cancel unanswered and closes the crossing session's
    // connection once the owner has pressed Ctrl-C: the press, taken while the cancels wait, came
    // before the connection's end, so the session's stop is Interrupted, the press and the cancel
    // sent recorded, though the connection's end cut the cancels short (decision 0148 item 8;
    // FBC-su9i's review).
    let model = QfexModel {
        close_on_cancel: true,
        hold_cancel: true,
        ..resting_cross()
    };
    let rpc = |argv| set(argv, "--rpc-timeout-ms", "5000");
    let d = pressed_drop(model, "cross-cut-cancels-ctrl-c", rpc, &[" cancel_order"]).await;
    assert_cross_stopped(&d, places::Stop::Interrupted);
    assert_eq!(d.stage_two().crosses.len(), 1, "{}", d.printed);
    let run = d.cross(0);
    assert_eq!(run.ended_by, Some(stage::CONNECTION_ENDED), "{run:?}");
    let cancels = run.cancels.as_ref().expect("the session's Stop");
    assert_eq!((cancels.sent, cancels.presses), (1, 1), "{cancels:?}");
    assert!(
        cancels.held && cancels.cut_short && !cancels.ended && !cancels.blocked,
        "{cancels:?}"
    );
    assert_eq!(d.count("cancel_order"), 1, "{:?}", d.what());
    let (report, json) = d.outputs();
    let section = cross_section(&report, "primary");
    assert!(
        section.contains(&format!(
            "Kill, then 1 cancel by venue id; {}",
            places::CUT_SHORT
        )),
        "{section}"
    );
    let cancels = &json["stage_two"]["crossings"][0]["cancels"];
    assert_eq!(cancels["sent"], 1, "{cancels}");
    assert_eq!(cancels["cut_short"], true, "{cancels}");
}

#[tokio::test]
async fn a_rate_refusal_before_a_press_during_the_crossing_sessions_cancels_is_the_stop() {
    // The crossing post-only rests to its deadline, and QFEX answers the Stop's cancel of it with
    // a RateLimited err, which settles nothing, so the cancels wait. The owner presses Ctrl-C only
    // once the probe has met that refusal, the shared limiter counting it: the refusal came first,
    // so it is the session's stop, and the run's, the press recorded (FBC-su9i's review).
    let rate_user = user();
    let model = QfexModel {
        rate_limit: Some(model::Limited::Cancel),
        ..resting_cross()
    };
    let rpc = |argv| set(argv, "--rpc-timeout-ms", "5000");
    let limiter = shared_limiter(rate_user);
    let refused = |venue: &Venue| {
        let read = venue
            .log()
            .iter()
            .any(|e| e.what.ends_with(" cancel_order"));
        read && limiter.counts().rejected.user > 0
    };
    let d = pressed_once_when(model, "cross-refused-then-ctrl-c", rate_user, rpc, refused).await;
    let refused = places::Stop::RateRefused {
        on: places::RefusedOn::Session,
    };
    assert_cross_stopped(&d, refused);
    assert_eq!(d.stage_two().crosses.len(), 1, "{}", d.printed);
    let run = d.cross(0);
    let cancels = run.cancels.as_ref().expect("the session's Stop");
    assert_eq!((cancels.sent, cancels.presses), (1, 1), "{cancels:?}");
    assert!(cancels.held && cancels.blocked, "{cancels:?}");
    let stopped = d.ev.stopped.as_deref().unwrap_or_default();
    let said = format!("{} in the crossing session", refused.name());
    assert!(stopped.starts_with(&said), "{stopped}");
}

#[tokio::test]
async fn a_press_after_the_crossing_sessions_cancels_is_taken_by_the_reads_after_it() {
    // The primary account's crossing post-only rests to its deadline and its Stop cancels it, seen
    // to end. The owner presses Ctrl-C as the cancels' line is printed, once they took their last
    // press, so the comparison after them, which takes none, runs with the press waiting. The
    // cancels took no press, so the session has no stop; the reads after it take the press, which
    // stops the run, marked, after every named account was read, and no crossing order goes out
    // on the subaccount (FBC-su9i's review).
    let at = "STOP cancels: Kill, then";
    let d = pressed_at_as(resting_cross(), "cross-press-at-compare", user(), |a| a, at).await;
    assert_cross_stopped(&d, places::Stop::Interrupted);
    let said = d.ev.stopped.as_deref().unwrap_or_default();
    assert!(
        said.starts_with("Interrupted during the reads after the crossing session"),
        "{said}"
    );
    assert_eq!(d.stage_two().crosses.len(), 1, "{}", d.printed);
    assert_eq!(d.crossing_orders().len(), 1, "{}", d.printed);
    let run = d.cross(0);
    let cancels = run.cancels.as_ref().expect("the session's Stop");
    assert_eq!((cancels.sent, cancels.presses), (1, 0), "{cancels:?}");
    assert!(cancels.ended && !cancels.held, "{cancels:?}");
    let steps: Vec<&str> = run.compared.iter().map(|c| c.step).collect();
    assert_eq!(steps, ["the crossing post-only", "the stop's cancels"]);
    assert_eq!(run.ended_by, Some(stage::GRACEFUL));
}

#[tokio::test]
async fn a_press_as_the_last_read_after_a_crossing_session_ends_is_taken_by_those_reads() {
    // The owner presses Ctrl-C as the last named account's read after the primary account's
    // crossing session prints its line, so the press is made in the poll that ends that read,
    // which wins the reads' wait for it: the reads still take it as they end, so the run stops
    // Interrupted after them, marked, and the subaccount's reads before its own crossing session
    // never begin (FBC-su9i's review).
    let at = "READ after the crossing session: subaccount: ";
    let d = pressed_at_as(resting_cross(), "cross-press-last-read", user(), |a| a, at).await;
    assert_cross_stopped(&d, places::Stop::Interrupted);
    let said = d.ev.stopped.as_deref().unwrap_or_default();
    assert!(
        said.starts_with("Interrupted during the reads after the crossing session"),
        "{said}"
    );
    assert_eq!(d.stage_two().crosses.len(), 1, "{}", d.printed);
    assert_eq!(d.crossing_orders().len(), 1, "{}", d.printed);
    let before = (d.ev.reads.iter()).filter(|r| r.before == places::CROSS);
    assert_eq!(before.count(), 2, "{}", d.printed);
}

/// Where the probe's lines go in [`the_span_bound_begins_cancels_one_rpc_timeout_early`]: the
/// model's [`Printed`], and the instant on the paused clock the cross registry was built.
struct TimedAt {
    printed: Printed,
    built: Rc<std::cell::Cell<Option<tokio::time::Instant>>>,
}

impl std::io::Write for TimedAt {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.printed.write(buf)?;
        let built = self.printed.text().contains("REGISTRY cross on primary");
        if built && self.built.get().is_none() {
            self.built.set(Some(tokio::time::Instant::now()));
        }
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Lets every task run until `done`, as `until` does, but for the reads after each drop: there
/// the clock, which stands still but for the test's own advance, moves on one
/// `--read-interval-ms` (1 s here) each time the model answers a page, so a read after the drop
/// that came before the model saw the drop is read again, as it would be live, instead of
/// waiting for ever. The reads after the drop run with no session open, so nothing else waits on
/// the clock then but their own waits, each longer than one interval. Panics after two minutes of
/// wall-clock time, so a regression fails instead of hanging, even under the full check's load.
async fn until_stepping_after_drops(venue: &Venue, printed: &Printed, done: impl Fn() -> bool) {
    let expired = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = expired.clone();
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(120));
        flag.store(true, Ordering::SeqCst);
    });
    // The pages the model had answered when the reads after a drop began, then at each step.
    let mut stepped: Option<usize> = None;
    while !done() {
        assert!(
            !expired.load(Ordering::SeqCst),
            "the crossing post-only's answer: not within 120 s: {}",
            printed.text()
        );
        let text = printed.text();
        let ended = format!("({}): arm outcome", places::DROP_TWO);
        let reading = text.matches(&ended).count() > text.matches("READ after the drop:").count();
        let page = |e: &model::Event| e.what.ends_with(" get_user_orders");
        let pages = venue.log().iter().filter(|e| page(e)).count();
        match (reading, stepped) {
            (false, _) => stepped = None,
            (true, None) => stepped = Some(pages),
            (true, Some(before)) if pages > before => {
                stepped = Some(pages);
                tokio::time::advance(Duration::from_secs(1)).await;
            }
            (true, Some(_)) => {}
        }
        tokio::task::yield_now().await;
    }
}

/// A run on the primary account alone, every crossing post-only resting, whose clock stands
/// still but for the test's own advance: once the crossing post-only is placed and has its ACK,
/// the clock is moved to `short` before the cross registry's span bound (600 s from its building,
/// the instant the registry's line is printed).
async fn span_run(what: &str, short: Duration) -> Dropped {
    let frozen = freeze();
    let (dir, removed) = temp_dir(what);
    let venue = Venue::start(resting_cross()).await;
    let (mut opts, places) = parsed(&drop_argv(&dir, &venue)).unwrap();
    opts.rate_user = user();
    let env = primary_only();
    let printed = venue.printed();
    let lookup = |var: &str| env.get(var).cloned();
    let built = Rc::new(std::cell::Cell::new(None));
    let out: stage::Out = Rc::new(RefCell::new(TimedAt {
        printed: printed.clone(),
        built: Rc::clone(&built),
    }));
    // Whether the run ended, so a run that ended before the crossing post-only's ACK fails at
    // once on what ended it, not when the wait below runs out.
    let ended = std::cell::Cell::new(false);
    let drive = async {
        // Its ACK, read by the session before any clock moves: the model answers it at once.
        let acked = || {
            printed
                .text()
                .contains("ANSWERED the crossing post-only on primary")
        };
        until_stepping_after_drops(&venue, &printed, || ended.get() || acked()).await;
        if ended.get() {
            return;
        }
        let bound = built.get().unwrap() + Duration::from_secs(600);
        tokio::time::advance((bound - short) - tokio::time::Instant::now()).await;
    };
    let ran = async {
        let ran = stage::run_places(&opts, &places, &lookup, out).await;
        ended.set(true);
        ran
    };
    let (ran, ()) = tokio::join!(ran, drive);
    drop(frozen);
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

#[tokio::test(start_paused = true)]
async fn the_span_bound_begins_cancels_one_rpc_timeout_early() {
    // The clock stands still but for the test's own advance. The crossing post-only rests and
    // waits for its deadline; the test then moves the clock to exactly one --rpc-timeout-ms (600
    // ms) before the cross registry's span bound: the span bound, met one --rpc-timeout-ms early,
    // stops the session there, and its Stop's cancels begin 600 ms before the bound itself
    // (decision 0148 item 8). Met any later, the session would have run its course first (its
    // deadline is due by then), so no SpanBound; met any earlier, the next test's run would stop.
    let d = span_run("cross-span", Duration::from_millis(600)).await;
    assert_eq!(d.ev.stop, Some("SpanBound"), "{}", d.printed);
    assert!(d.marker().unwrap().starts_with("stop: SpanBound\n"));
    let run = d.cross(0);
    assert_eq!(
        run.cancels_lead,
        Some(Duration::from_millis(600)),
        "{run:?}"
    );
    let cancels = run.cancels.as_ref().unwrap();
    assert_eq!((cancels.sent, cancels.ended), (1, true), "{cancels:?}");
    let orders = d.crossing_orders();
    assert_eq!(orders[0].state, model::State::Cancelled);
    assert!(d.wrote_naming("cancel_order", &orders[0].id));
    assert!(
        d.ev.stopped
            .as_deref()
            .unwrap()
            .starts_with("SpanBound in the crossing session"),
        "{:?}",
        d.ev.stopped
    );
    let (report, _) = d.outputs();
    assert!(
        report.contains("the span bound stopped it, its cancels begun 600 ms before the bound"),
        "{report}"
    );
}

#[tokio::test(start_paused = true)]
async fn the_span_bound_is_not_met_a_millisecond_before_one_rpc_timeout_early() {
    // As the test before, but the clock moved to 601 ms before the span bound: the crossing
    // post-only's deadline is due, the span bound's early instant is not, so the session runs its
    // course, cancels by venue id as it stops, and the run goes on with no stop to the account's
    // taker IOC session (FBC-ou9g), whose own course this test leaves aside.
    let d = span_run("cross-span-short", Duration::from_millis(601)).await;
    assert_eq!((d.ev.stop, d.marker()), (None, None), "{}", d.printed);
    assert_eq!(d.stage_two().takes.len(), 1, "{}", d.printed);
    let run = d.cross(0);
    assert_eq!(run.cancels_lead, None, "{run:?}");
    assert_eq!(run.crossed, Some(stage_two::Crossed::Rested));
    let cancels = run.cancels.as_ref().unwrap();
    assert_eq!((cancels.sent, cancels.ended), (1, true), "{cancels:?}");
    assert_eq!(d.crossing_orders()[0].state, model::State::Cancelled);
}

#[tokio::test]
async fn stop_on_an_order_left_resting() {
    // The crossing post-only rests, and QFEX answers the Stop's cancel of it NO_SUCH_ORDER while
    // it still rests: the read of every named account after the session lists it, an order of
    // ours left resting, which stops the probe, reported for the owner's hand cancel (decision
    // 0132 item 7).
    let model = QfexModel {
        cancel_misses: true,
        ..resting_cross()
    };
    let d = dropped(model, "cross-left-resting").await;
    assert_cross_stopped(&d, places::Stop::LeftResting);
    let run = d.cross(0);
    assert!(run.resting && run.read, "{run:?}");
    let cancels = run.cancels.as_ref().unwrap();
    assert!(
        cancels.held && !cancels.blocked && !cancels.ended,
        "{cancels:?}"
    );
    assert_eq!(run.ended_by, Some(stage::GRACEFUL));
    assert_eq!(d.crossing_orders()[0].state, model::State::Open);
    assert!(
        d.printed
            .contains(&format!("RESTING {}", stage_two::HAND_CANCEL_CROSS))
    );
    let (report, json) = d.outputs();
    assert!(report.contains(stage_two::HAND_CANCEL_CROSS), "{report}");
    let section = cross_section(&report, "primary");
    assert!(
        section.contains("answered other than accepted"),
        "{section}"
    );
    assert_eq!(
        json["stage_two"]["crossings"][0]["hand_cancel"],
        stage_two::HAND_CANCEL_CROSS
    );
    assert_eq!(d.stage_two().crosses.len(), 1);
}

#[tokio::test]
async fn stop_on_an_order_left_resting_that_only_a_read_shows() {
    // The crossing post-only rests, and QFEX answers the Stop's cancel of it CANCELLED while it
    // still rests: the cross registry holds it ended, so only the reads show it resting. The
    // comparison after the Stop's cancels sees a read listing an order the streams ended, so the
    // session's stop is ReadDisagrees (decision 0148 item 8), marked; the read of every named
    // account after the session still shows it resting, reported for the owner's hand cancel
    // (decision 0132 item 7).
    let model = QfexModel {
        cancel_lies: true,
        ..resting_cross()
    };
    let d = dropped_with(model, "cross-read-resting", short_agree).await;
    assert_cross_stopped(&d, places::Stop::ReadDisagrees);
    let run = d.cross(0);
    let compared = run.compared.last().unwrap();
    assert_eq!(compared.ended, compare::Ended::Disagreed, "{compared:?}");
    assert!(run.resting && !run.counted_resting && run.read, "{run:?}");
    let cancels = run.cancels.as_ref().unwrap();
    assert!(
        cancels.ended && !cancels.held && !cancels.blocked,
        "{cancels:?}"
    );
    assert_eq!(d.crossing_orders()[0].state, model::State::Open);
    assert!(
        d.ev.stopped
            .as_deref()
            .unwrap()
            .starts_with("ReadDisagrees in the crossing session"),
        "{:?}",
        d.ev.stopped
    );
    assert!(
        d.printed
            .contains(&format!("RESTING {}", stage_two::HAND_CANCEL_CROSS))
    );
    let (report, json) = d.outputs();
    let section = cross_section(&report, "primary");
    assert!(section.contains(stage_two::HAND_CANCEL_CROSS), "{section}");
    assert_eq!(
        json["stage_two"]["crossings"][0]["hand_cancel"],
        stage_two::HAND_CANCEL_CROSS
    );
    assert_eq!(json["stage_two"]["crossings"][0]["counted_resting"], false);
    assert_eq!(d.stage_two().crosses.len(), 1);
}

#[tokio::test]
async fn a_named_account_not_read_after_a_crossing_session_stops_the_run_marked() {
    // Each crossing post-only is refused CANCELLED, so the cross registry counts nothing
    // resting; but once the subaccount's, the last, arrived, the primary account's
    // authentications go unanswered, so the read of it after that session fails. Nobody looked
    // at it after an order was placed: an order of ours may rest there unjudged, so the run
    // stops LeftResting, marked, and the next run must reconcile (decision 0132 item 7, 0151
    // item 6), as before every session an account that cannot be read stops the probe.
    let model = QfexModel {
        alo_cross: Some(model::AloCross::Cancelled),
        silent_after_cross: Some(1),
        ..QfexModel::default()
    };
    let d = dropped_with(model, "cross-unread", |argv| {
        set(argv, "--read-wait-ms", "3000")
    })
    .await;
    let name = places::Stop::LeftResting.name();
    assert_eq!(d.ev.stop, Some(name), "{}", d.printed);
    let marker = d.marker().unwrap_or_default();
    assert!(marker.starts_with(&format!("stop: {name}\n")), "{marker}");
    assert!(d.printed.contains(&format!("MARKER written ({name})")));
    let stopped = d.ev.stopped.clone().unwrap();
    assert!(
        stopped.starts_with(
            "LeftResting after the crossing session, a named account not read after it"
        ),
        "{stopped}"
    );
    assert_eq!(d.stage_two().crosses.len(), 2);
    let first = d.cross(0);
    assert_eq!(first.stop, None, "{first:?}");
    let last = d.cross(1);
    assert_eq!(last.account, "subaccount");
    assert_eq!(
        last.crossed,
        Some(stage_two::Crossed::Cancelled { exact: true })
    );
    assert!(
        last.read && !last.resting && !last.counted_resting,
        "{last:?}"
    );
    assert_eq!(last.stop, Some(name));
    assert!(!d.ev.errors.is_empty(), "{}", d.printed);
    let (report, json) = d.outputs();
    assert!(
        report.contains(&format!("**The place stages' stop:** {name}.")),
        "{report}"
    );
    assert_eq!(json["stage_two"]["crossings"][1]["stop"], name);
    assert_report_and_evidence_pass_the_privacy_check(&d.dir.join("evidence"));
    assert_no_secret(&d.dir.join("evidence"));
}

#[tokio::test]
async fn an_open_gate_is_not_carried_into_a_second_run() {
    // A run whose gate opened, ending after its crossings with no stop and no marker, then a
    // second on the same lease directory whose every ACK writes our client id upper-cased: its
    // own gate, judged on its own frames alone, stays shut, so it ends NoEcho.
    let (dir, _removed) = temp_dir("gate-not-carried");
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
    assert_eq!(ev.stage_two.as_ref().unwrap().gate, Some(true), "{printed}");
    assert_eq!(
        (ev.stop, ev.stopped.as_deref()),
        (None, Some(stage::NOT_BUILT))
    );
    assert!(!leases.join(places::MARKER).exists());
    let venue = Venue::start(QfexModel {
        ack_echo: model::Echo::Upper,
        ..QfexModel::default()
    })
    .await;
    let second = dir.join("second");
    let argv = set(
        drop_argv(&second, &venue),
        "--lease-dir",
        leases.to_str().unwrap(),
    );
    let argv = set(argv, "--namespace", "8");
    let (ran, printed) = place_run(&venue, &argv, &primary_only()).await;
    let ev = stopped(ran, &printed);
    assert_eq!(
        ev.stage_two.as_ref().unwrap().gate,
        Some(false),
        "{printed}"
    );
    assert_eq!(ev.stop, Some("NoEcho"), "{printed}");
    assert!(printed.contains("GATE shut: no ACK"), "{printed}");
    assert!(!printed.contains("REGISTRY cross"), "{printed}");
    assert!(!leases.join(places::MARKER).exists());
}

#[tokio::test]
async fn held_stop_cancels_end_by_a_drop_after_a_passed_drop_test() {
    // The crossing post-only rests, and QFEX answers the Stop's cancel of it with a RateLimited
    // err: the cancels are held by a provider rate refusal, and since this account's drop test
    // passed in this run, which showed the drop cancels, the session ends by the abrupt drop,
    // with no WebSocket close, and the drop cancels the order (0151 item 11). The rate refusal
    // stops the run, after the read of every named account.
    let model = QfexModel {
        rate_limit: Some(model::Limited::Cancel),
        ..resting_cross()
    };
    let d = dropped(model, "cross-held").await;
    assert_cross_stopped(
        &d,
        places::Stop::RateRefused {
            on: places::RefusedOn::Session,
        },
    );
    let run = d.cross(0);
    let cancels = run.cancels.as_ref().unwrap();
    assert!(
        cancels.held && cancels.blocked && !cancels.ended,
        "{cancels:?}"
    );
    assert_eq!(run.ended_by, Some(stage::DROPPED));
    let conn: usize = d.cross_conns()[0].parse().unwrap();
    assert!(!d.venue.trade.connections()[conn].client_closed);
    assert!(d.what().contains(&format!("cod {conn} drop cancels")));
    assert_eq!(d.crossing_orders()[0].state, model::State::Cancelled);
    assert!(!run.resting && run.read, "{run:?}");
    let section = cross_section(&d.outputs().0, "primary");
    assert!(
        section.contains("refused locally or met by a provider rate refusal"),
        "{section}"
    );
    assert!(section.contains("- Ended by: the abrupt drop"), "{section}");
}

#[tokio::test]
async fn the_reads_after_a_held_drop_read_again_until_its_cancel_shows() {
    // As held_stop_cancels_end_by_a_drop_after_a_passed_drop_test, but QFEX's first read of the
    // account after the drop still shows the crossing post-only open, as a read made before QFEX
    // learns of the drop would. The reads after the crossing session read every named account
    // again at the comparisons' cadence (0151 items 4 and 11), and the next one shows it gone:
    // nothing of ours is left resting, and the run reports no hand cancel.
    let model = QfexModel {
        rate_limit: Some(model::Limited::Cancel),
        // The third drop of the run: each named account's drop session two, then this one.
        drop_lag: Some((2, 1)),
        ..resting_cross()
    };
    let d = dropped(model, "cross-held-lag").await;
    assert_cross_stopped(
        &d,
        places::Stop::RateRefused {
            on: places::RefusedOn::Session,
        },
    );
    let run = d.cross(0);
    assert_eq!(run.ended_by, Some(stage::DROPPED));
    let conn: usize = d.cross_conns()[0].parse().unwrap();
    assert!(d.what().contains(&format!("cod {conn} drop cancels")));
    assert_eq!(d.crossing_orders()[0].state, model::State::Cancelled);
    assert!(!run.resting && run.read, "{run:?}");
    assert!(!d.printed.contains("RESTING "), "{}", d.printed);
    let line = "READ after the crossing session's drop: primary: ";
    let said = d.printed.lines().find(|l| l.starts_with(line));
    let said = said.unwrap_or_else(|| panic!("{}", d.printed));
    assert!(!said.contains(": 1 of at most 5 rounds"), "{said}");
    assert!(
        said.ends_with("the last judged listing no order of ours"),
        "{said}"
    );
}

#[tokio::test]
async fn the_reads_after_a_held_drop_end_at_the_wait_with_an_order_still_listed() {
    // QFEX's reads show the crossing post-only open after the drop for longer than the wait:
    // the reads after the crossing session read every named account again at the comparisons'
    // cadence, at most --agree-wait-ms / --read-interval-ms times (5 here), and none due at the
    // end of the wait, then report the order resting for the owner's hand cancel.
    let model = QfexModel {
        rate_limit: Some(model::Limited::Cancel),
        drop_lag: Some((2, 100)),
        ..resting_cross()
    };
    let d = dropped(model, "cross-held-lag-long").await;
    assert_cross_stopped(
        &d,
        places::Stop::RateRefused {
            on: places::RefusedOn::Session,
        },
    );
    let run = d.cross(0);
    assert_eq!(run.ended_by, Some(stage::DROPPED));
    assert!(run.resting && run.read, "{run:?}");
    assert!(
        d.printed
            .contains(&format!("RESTING {}", stage_two::HAND_CANCEL_CROSS)),
        "{}",
        d.printed
    );
    assert!(
        d.printed.contains(
            "READ after the crossing session's drop: primary: 5 of at most 5 rounds of reads of \
             every named account, the last judged listing an order of ours"
        ),
        "{}",
        d.printed
    );
    let after: Vec<&str> = (d.ev.reads.iter())
        .filter(|r| r.before == refusals::AFTER_CROSS)
        .map(|r| r.label.as_str())
        .collect();
    assert_eq!(after.len(), 10, "{after:?}");
}

#[tokio::test]
async fn the_report_lists_the_characters_exercised() {
    let d = dropped(QfexModel::default(), "cross-characters").await;
    // The characters the run's client ids exercised, by class of QFEX's client-id format (a
    // lowercase UUID), those an ACK echoed byte for byte, those sent but not shown so, and the
    // rest named as not measured: never an id.
    let chars = d.stage_two().characters.clone().unwrap();
    let names: Vec<&str> = chars.classes.iter().map(|c| c.name).collect();
    assert_eq!(
        names,
        ["decimal digits", "lowercase letters a to f", "the hyphen"]
    );
    assert!(chars.classes[2].echoed.contains(&'-'));
    assert_eq!(chars.outside, 0);
    let (report, json) = d.outputs();
    let line = format!(
        "- The characters our client ids exercised, by class of QFEX's client-id format \
         (decision 0132's Consequences; the rest not measured): {}",
        chars.says()
    );
    assert!(report.contains(&line), "{report}");
    assert!(
        chars
            .says()
            .starts_with("decimal digits: echoed byte for byte by an ACK ")
    );
    assert_eq!(json["stage_two"]["characters"], chars.says());
    let orders = d.venue.model.lock().unwrap().orders.clone();
    assert!(orders.iter().all(|o| !report.contains(&o.cid)));
    // The crossing sessions' frames are in the places-production/ slice, apart from the drop
    // test's, each passing the privacy check.
    let evidence = d.dir.join("evidence");
    let places = evidence.join(stage::PLACES_SLICE);
    let readme = fs::read_to_string(places.join("README.md")).unwrap();
    assert!(
        readme.starts_with(
            "# QFEX trade-socket frames: stage two's crossing post-only and taker IOC"
        ),
        "{readme}"
    );
    assert!(readme.contains(
        "the crossing session on primary then the taker IOC session on primary then the crossing \
         session on subaccount then the taker IOC session on subaccount"
    ));
    let listed = |dir: &Path| -> Vec<String> {
        let names = fs::read_dir(dir).unwrap().map(|e| e.unwrap().file_name());
        names.map(|n| n.to_string_lossy().into_owned()).collect()
    };
    let crossing = listed(&places);
    assert!(
        crossing
            .iter()
            .any(|f| f.starts_with("primary-the-crossing-session-"))
    );
    assert!(
        crossing
            .iter()
            .any(|f| f.starts_with("subaccount-the-crossing-session-"))
    );
    assert!(
        crossing.iter().all(|f| !f.contains("drop-session")),
        "{crossing:?}"
    );
    let drop_slice = listed(&evidence.join(stage::DROP_SLICE));
    assert!(
        drop_slice.iter().all(|f| !f.contains("crossing")),
        "{drop_slice:?}"
    );
    let drop_readme = fs::read_to_string(evidence.join(stage::DROP_SLICE).join("README.md"));
    assert!(!drop_readme.unwrap().contains("the crossing session"));
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/privacy-check.sh");
    evidence::privacy_check(&script, &places).unwrap();
    assert_report_and_evidence_pass_the_privacy_check(&evidence);
}

#[tokio::test]
async fn the_report_says_whether_add_order_was_accepted_as_encoded() {
    let d = dropped(QfexModel::default(), "cross-as-encoded").await;
    // Every add_order went out as the codec encodes it, with none of take_profit, stop_loss and
    // reduce_only, and QFEX acknowledged one: accepted as encoded (FBC-blcw).
    let two = d.stage_two();
    assert_eq!(two.as_encoded, Some(stage_two::AsEncoded::Accepted));
    assert!(d.cross(0).as_encoded == Some(true));
    let (report, json) = d.outputs();
    let line = format!(
        "- Whether QFEX accepted add_order as encoded, without take_profit, stop_loss or \
         reduce_only (FBC-blcw): {}",
        stage_two::AsEncoded::Accepted.says()
    );
    assert!(report.contains(&line), "{report}");
    assert!(
        stage_two::AsEncoded::Accepted
            .says()
            .starts_with("yes: QFEX acknowledged")
    );
    assert_eq!(
        json["stage_two"]["add_order_as_encoded"],
        stage_two::AsEncoded::Accepted.says()
    );
    let section = cross_section(&report, "primary");
    assert!(
        section.contains("- add_order sent as encoded (FBC-blcw): yes"),
        "{section}"
    );
    // Which statuses were rejects and which ends: the crossing post-only's CANCELLED before any
    // ACK, a reject; the drop's ends of X and Y read only after the drop, so none heard.
    assert_eq!(two.statuses.rejects, ["CANCELLED"]);
    assert!(
        report.contains(&format!(
            "- Which statuses are rejects and which ends: {}",
            two.statuses.says()
        )),
        "{report}"
    );
}

#[tokio::test]
async fn an_upper_cased_echo_reads_not_byte_for_byte() {
    let model = QfexModel {
        ack_echo: model::Echo::Upper,
        ..QfexModel::default()
    };
    let d = dropped(model, "cross-upper").await;
    // Every ACK of the run writes our client id upper-cased: ours to fbc-core, but not byte for
    // byte, which the report says, kind by kind and frame by frame, and the gate stays shut.
    let (report, json) = d.outputs();
    assert!(
        report
            .contains("  - ACK: 4 not byte for byte: ours, in another case or with a JSON escape"),
        "{report}"
    );
    assert_eq!(
        json["stage_two"]["echoes"]["ACK"],
        "4 not byte for byte: ours, in another case or with a JSON escape"
    );
    assert!(
        report.contains("order_response ACK: ours, not byte for byte"),
        "{report}"
    );
    assert_eq!(d.stage_two().gate, Some(false));
}

#[tokio::test]
async fn a_crossing_post_only_that_fills_is_flattened_then_stops() {
    // QFEX fills the crossing post-only as a taker: a fill of ours other than the taker IOC's
    // and the flatten IOCs' (0151 item 9). The session still open, the flatten: no other order
    // of ours to cancel, the owner's Flatten into Exit, one reducing limit IOC within the cap,
    // filled against the bid, REST flat; then the session's Stop, every order of ours cancelled
    // by venue id, and the run stops UnexpectedFill, flat.
    let model = QfexModel {
        alo_cross: Some(model::AloCross::Fills),
        ..QfexModel::default()
    };
    let d = dropped(model, "cross-fills").await;
    assert_cross_stopped(&d, places::Stop::UnexpectedFill);
    let run = d.cross(0);
    assert_eq!(run.crossed, Some(stage_two::Crossed::Filled));
    let flatten = run.flatten.as_ref().expect("a flatten");
    assert_flattened(flatten, 0);
    let cancels = run.cancels.as_ref().expect("the session's Stop");
    assert!(
        cancels.ended && !cancels.held && cancels.sent == 0,
        "{cancels:?}"
    );
    assert_eq!(run.ended_by, Some(stage::GRACEFUL));
    assert_flatten_iocs_within_the_cap(&d, &["SELL"]);
    // The crossing post-only on the primary account alone: the stop keeps the subaccount's back.
    assert_eq!(d.stage_two().crosses.len(), 1);
    let flat = (d.ev.reads.iter())
        .filter(|r| r.before == refusals::AFTER_CROSS)
        .all(|r| r.flat);
    assert!(flat, "{:?}", d.ev.reads);
    let (report, json) = d.outputs();
    let section = cross_section(&report, "primary");
    assert!(section.contains(stage_two::CROSS_FILLED), "{section}");
    assert!(
        section.contains("#### The flatten (FBC-ou9g; decision 0148 item 5, 0151 item 9)"),
        "{section}"
    );
    // The places-production/ slice, the flatten IOC's frames in it, passed the privacy check.
    assert!(
        d.ev.errors.is_empty() && !d.ev.fixtures.is_empty(),
        "{:?}",
        d.ev.errors
    );
    let places = d.dir.join("evidence").join(stage::PLACES_SLICE);
    assert!(places.join("README.md").exists());
    let entry = &json["stage_two"]["crossings"][0]["flatten"];
    assert_eq!(
        (&entry["flat"], &entry["rest_flat"]),
        (&Value::Bool(true), &Value::Bool(true))
    );
}

#[tokio::test]
async fn an_arm_not_accepted_in_the_crossing_session_places_no_crossing_order() {
    // QFEX rejects the crossing session's arm (the run's fifth): the session's runtime ends the
    // connection whose arm failed, so no crossing order is placed, the end is unplanned, a stop,
    // and the run stops with its reads after it, its report and its marker.
    let model = QfexModel {
        arm_err_on: Some(4),
        ..QfexModel::default()
    };
    let d = dropped(model, "cross-arm-err").await;
    assert_cross_stopped(&d, places::Stop::ConnectionEnded);
    assert_eq!(d.count("add_order"), 4, "{:?}", d.what());
    let run = d.cross(0);
    assert_eq!(run.arm.as_deref(), Some("Rejected"));
    assert!(
        !run.started && !run.placed.submitted && run.crossed.is_none(),
        "{run:?}"
    );
    assert_eq!(run.ended_by, Some(stage::CONNECTION_ENDED));
    let (report, _) = d.outputs();
    let section = cross_section(&report, "primary");
    assert!(
        section.contains("- How QFEX refused it: not measured: it was not placed"),
        "{section}"
    );
    assert!(
        section.contains("- The Stop's cancels: none: the session's connection ended first"),
        "{section}"
    );
}

#[tokio::test]
async fn a_crossing_post_only_with_no_touch_to_price_it_is_not_placed() {
    // As the subaccount's Y goes out the MDS touch turns crossed, so the crossing post-only, the
    // next place, has no touch to be priced from: it is not placed, a failure that is no stop,
    // recorded as the crossing session's error; the session still cancels by venue id (nothing of
    // ours rests) and closes, the reads after it are made, and the run ends on that failure, with
    // no marker.
    let model = QfexModel {
        crossed_on_place: Some(3),
        ..QfexModel::default()
    };
    let d = dropped(model, "cross-no-touch").await;
    assert_eq!(d.count("add_order"), 4, "{:?}", d.what());
    let run = d.cross(0);
    assert!(
        run.started && !run.placed.submitted && run.crossed.is_none(),
        "{run:?}"
    );
    let error = run.error.clone().unwrap();
    assert_eq!(error, market::CROSSED, "{}", d.printed);
    assert_eq!(d.ev.stopped.as_deref(), Some(error.as_str()));
    assert_eq!((d.ev.stop, d.marker()), (None, None));
    let cancels = run.cancels.as_ref().unwrap();
    assert_eq!((cancels.sent, cancels.ended), (0, true));
    assert_eq!(run.ended_by, Some(stage::GRACEFUL));
    assert!(run.read && !run.resting, "{run:?}");
    let (report, json) = d.outputs();
    let section = cross_section(&report, "primary");
    assert!(section.contains(&format!("- Error: {error}")), "{section}");
    assert_eq!(json["stage_two"]["crossings"][0]["error"], error);
}

#[tokio::test]
async fn a_touch_that_moves_past_the_caps_before_the_crossing_post_only_sends_none() {
    // FBC-23ch's review fix of PR #272 (thread 4605918689), carried to the crossing post-only:
    // both caps admit exactly three minimum sizes at the preflight's touch near 100, where X and
    // Y are placed on both accounts; at the primary account's crossing session's arm, the run's
    // fifth, the touch moves near 400, where they admit none, while the cross registry's lot
    // caps, fixed at the preflight, still admit three. The crossing post-only is not sent and
    // never resized, and the run stops, marked; the session cancels by venue id, nothing of
    // ours resting, and ends by a graceful close (decision 0113).
    let moved = model::Touch {
        bid: Some(("400.00", "1.00")),
        ask: Some(("400.10", "2.00")),
        age_secs: 0,
    };
    let model = QfexModel {
        touch_moves_at_arm: Some((4, moved)),
        ..QfexModel::default()
    };
    let caps = |argv| {
        let argv = set(argv, "--resting-cap-usd", THREE_MIN_USD);
        set(argv, "--inventory-cap-usd", THREE_MIN_USD)
    };
    let d = dropped_with(model, "cross-caps-moved", caps).await;
    assert_eq!(d.count("add_order"), 4, "{:?}", d.what());
    assert_cross_stopped(&d, places::Stop::BeyondCaps);
    let said = d.ev.stopped.as_deref().unwrap();
    assert!(
        said.starts_with("BeyondCaps in the crossing session"),
        "{said}"
    );
    let run = d.cross(0);
    assert!(
        run.started && !run.placed.submitted && run.crossed.is_none(),
        "{run:?}"
    );
    assert!(
        !d.printed.contains("PLACE the crossing post-only"),
        "{}",
        d.printed
    );
    let cancels = run.cancels.as_ref().unwrap();
    assert_eq!((cancels.sent, cancels.ended), (0, true), "{cancels:?}");
    assert_eq!(run.ended_by, Some(stage::GRACEFUL));
    assert_eq!(d.count("cancel_order"), 0);
    assert!(d.crossing_orders().is_empty());
}

#[tokio::test]
async fn a_crossing_order_is_kept_back_in_a_run_while_another_order_of_ours_rests() {
    // An order of ours, never sent, held in the primary account's cross registry before a
    // crossing session's check (a test's hook: the reads before each session, and the stop an
    // order left resting after one is, keep the model from ever reaching it): on the account's
    // own registry before its crossing post-only, and on the primary account's registry before
    // the subaccount's. Either keeps the crossing order back, a stop, LeftResting, with no
    // crossing order (decision 0148 item 6).
    // Before the subaccount's, the primary account's crossing post-only, then its taker IOC and
    // the flatten IOC its fill needed (FBC-ou9g).
    for (n, account, places_before) in [(0, "primary", 4), (1, "subaccount", 7)] {
        let (dir, removed) = temp_dir("cross-blocked");
        let venue = Venue::start(QfexModel::default()).await;
        let (mut opts, mut places) = parsed(&drop_argv(&dir, &venue)).unwrap();
        places.test.other_order = Some(n);
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
        assert_eq!(d.ev.stop, Some("LeftResting"), "{}", d.printed);
        assert_eq!(d.count("add_order"), places_before, "{:?}", d.what());
        let run = d.cross(n);
        let blocked = run.blocked.clone().unwrap();
        assert!(
            blocked.starts_with("the cross registry on primary holds an order of ours"),
            "{blocked}"
        );
        assert!(run.started && !run.placed.submitted, "{run:?}");
        assert!(d.printed.contains(&format!(
            "BLOCKED the crossing post-only on {account}: {blocked} (decision 0148 item 6)"
        )));
        let section = cross_section(&d.outputs().0, account);
        assert!(
            section.contains(&format!(
                "- No other order of ours (decision 0148 item 6): kept back: {blocked}"
            )),
            "{section}"
        );
        assert!(
            d.ev.stopped
                .as_deref()
                .unwrap()
                .starts_with("LeftResting in the crossing session"),
            "{:?}",
            d.ev.stopped
        );
    }
}

#[tokio::test]
async fn ctrl_c_during_the_reads_before_the_crossing_session_stops_the_run() {
    // The owner presses Ctrl-C as the reads before the crossing session begin: they stop there,
    // no cross registry is built, and the run stops, marked (decision 0148 item 8).
    let (dir, removed) = temp_dir("cross-reads-press");
    let venue = Venue::start(QfexModel::default()).await;
    let (mut opts, places) = parsed(&drop_argv(&dir, &venue)).unwrap();
    opts.rate_user = user();
    let env = environment();
    let printed = venue.printed();
    let lookup = |var: &str| env.get(var).cloned();
    let (press, presses) = interrupt::Presses::channel();
    let out: stage::Out = Rc::new(RefCell::new(PressAt {
        printed: printed.clone(),
        at: "GATE open",
        press: Some(Box::new(press)),
    }));
    let hold: stage::Interrupt = Box::pin(std::future::pending());
    let ran = stage::run_places_until(&opts, &places, &lookup, out, hold, presses).await;
    let printed = printed.text();
    let ev = stopped(ran, &printed);
    assert_eq!(ev.stop, Some("Interrupted"), "{printed}");
    assert!(
        ev.stopped
            .as_deref()
            .unwrap()
            .starts_with("Interrupted during the reads before the crossing session"),
        "{:?}",
        ev.stopped
    );
    assert!(!printed.contains("REGISTRY cross"), "{printed}");
    let two = ev.stage_two.as_ref().unwrap();
    assert!(two.crosses.is_empty() && two.gate == Some(true));
    assert!(fs::read_to_string(leases_of(&dir).join(places::MARKER)).is_ok());
    drop(removed);
}

#[tokio::test]
async fn a_rate_refusal_in_the_reads_before_the_crossing_session_stops_the_run_marked() {
    // FBC-23ch's second review fix to the reads before a session (0151 items 4 and 6), carried to
    // the reads before the crossing session: as the gate opens, the model refuses the next
    // get_user_orders page on a read socket with a RateLimited err, or the next GET
    // /user/positions with HTTP 429. That provider rate refusal is the run's stop, marked, so the
    // next run needs --reconciled; no cross registry is built and nothing crosses the touch.
    let cases = [
        (true, places::RefusedOn::ReadSocket),
        (false, places::RefusedOn::Rest),
    ];
    for (socket, on) in cases {
        let (dir, removed) = temp_dir("cross-reads-refused");
        let venue = Venue::start(QfexModel::default()).await;
        let (mut opts, places) = parsed(&drop_argv(&dir, &venue)).unwrap();
        opts.rate_user = user();
        let env = environment();
        let printed = venue.printed();
        let lookup = |var: &str| env.get(var).cloned();
        let shared = Arc::clone(&venue.model);
        let refuse = move || {
            let mut m = shared.lock().unwrap();
            m.rate_limit = Some(match socket {
                true => model::Limited::ReadSocket(m.pages_after),
                false => model::Limited::Rest(m.rest_after),
            });
        };
        let out: stage::Out = Rc::new(RefCell::new(PressAt {
            printed: printed.clone(),
            at: "GATE open",
            press: Some(Box::new(refuse)),
        }));
        let hold: stage::Interrupt = Box::pin(std::future::pending());
        let none = interrupt::Presses::none();
        let ran = stage::run_places_until(&opts, &places, &lookup, out, hold, none).await;
        let printed = printed.text();
        let ev = stopped(ran, &printed);
        let stop = places::Stop::RateRefused { on };
        let name = stop.name();
        assert_eq!(ev.stop, Some(name), "{printed}");
        let said = ev.stopped.as_deref().unwrap();
        assert!(
            said.starts_with(&format!("{name} in the reads before the crossing session")),
            "{said}"
        );
        let marker = fs::read_to_string(leases_of(&dir).join(places::MARKER)).unwrap_or_default();
        assert!(marker.starts_with(&format!("stop: {name}\n")), "{marker}");
        // The read failure the refusal made is kept among the errors.
        let failed = match socket {
            true => "get_user_orders: QFEX refused the page read: RateLimited",
            false => "GET /user/positions: HTTP 429, a provider rate refusal",
        };
        assert!(
            ev.errors.iter().any(|e| e.ends_with(failed)),
            "{:?}",
            ev.errors
        );
        assert!(!printed.contains("REGISTRY cross"), "{printed}");
        let two = ev.stage_two.as_ref().unwrap();
        assert!(two.crosses.is_empty() && two.gate == Some(true));
        assert_eq!(venue.model.lock().unwrap().crossings, 0);
        venue.assert_served();
        drop(removed);
    }
}

#[tokio::test]
async fn a_stop_the_comparison_after_the_crossing_sessions_cancels_raises_ends_the_run_marked() {
    // The primary account's crossing session runs its course, its crossing post-only cancelled
    // by QFEX, so no stop comes from its steps; under Policy::Stage its Stop still cancels every
    // order of ours by venue id and compares the streams with the reads after them. As the
    // session's steps end, before those cancels, the model refuses the next get_user_orders page
    // on a read socket with a RateLimited err, or the next GET /user/positions with HTTP 429, or
    // shows a position on the primary account that no fill made. That comparison's stop, a
    // provider rate refusal or ReadDisagrees, is the session's: the run stops, marked, and the
    // subaccount's crossing post-only is never placed.
    let cases = [
        Some(places::RefusedOn::ReadSocket),
        Some(places::RefusedOn::Rest),
        None,
    ];
    for refused in cases {
        let (dir, removed) = temp_dir("cross-cancels-compared");
        let venue = Venue::start(QfexModel::default()).await;
        let (mut opts, places) = parsed(&drop_argv(&dir, &venue)).unwrap();
        opts.rate_user = user();
        let env = environment();
        let printed = venue.printed();
        let lookup = |var: &str| env.get(var).cloned();
        let shared = Arc::clone(&venue.model);
        let then = move || {
            let mut m = shared.lock().unwrap();
            match refused {
                Some(places::RefusedOn::ReadSocket) => {
                    m.rate_limit = Some(model::Limited::ReadSocket(m.pages_after));
                }
                Some(places::RefusedOn::Rest) => {
                    m.rate_limit = Some(model::Limited::Rest(m.rest_after));
                }
                Some(on) => unreachable!("no case refuses on {on:?}"),
                // The primary account, keyed by no subaccount id.
                None => m.moved.push(None),
            }
        };
        let out: stage::Out = Rc::new(RefCell::new(PressAt {
            printed: printed.clone(),
            // Before the Stop's cancels, which read nothing: the next read is the comparison after
            // them. Their own line comes after that comparison and the late flatten.
            at: "CROSSED on primary",
            press: Some(Box::new(then)),
        }));
        let hold: stage::Interrupt = Box::pin(std::future::pending());
        let none = interrupt::Presses::none();
        let ran = stage::run_places_until(&opts, &places, &lookup, out, hold, none).await;
        let printed = printed.text();
        let ev = stopped(ran, &printed);
        let stop = match refused {
            Some(on) => places::Stop::RateRefused { on },
            None => places::Stop::ReadDisagrees,
        };
        let name = stop.name();
        assert_eq!(ev.stop, Some(name), "{printed}");
        let said = ev.stopped.as_deref().unwrap();
        assert!(
            said.starts_with(&format!("{name} in the crossing session")),
            "{said}"
        );
        let marker = fs::read_to_string(leases_of(&dir).join(places::MARKER)).unwrap_or_default();
        assert!(marker.starts_with(&format!("stop: {name}\n")), "{marker}");
        let two = ev.stage_two.as_ref().unwrap();
        assert_eq!(two.crosses.len(), 1, "{printed}");
        let cross = &two.crosses[0];
        assert!(cross.cancels.is_some(), "{cross:?}");
        assert_eq!(cross.compared.last().unwrap().stop, Some(stop), "{cross:?}");
        assert_eq!(cross.stop, Some(name));
        assert_eq!(venue.model.lock().unwrap().crossings, 1);
        venue.assert_served();
        drop(removed);
    }
}

#[tokio::test]
async fn a_rate_refusal_met_while_the_stops_cancels_wait_holds_them_at_the_waits_end() {
    // The owner's Ctrl-C while the crossing post-only's ACK is on its way (QFEX answers it 1.5 s
    // later): its cancel waits for that ACK, goes out by venue id once it lands, and QFEX answers
    // it with a RateLimited err, which wakes no wait of the cancels. The wait, --rpc-timeout-ms
    // (4 s here), ends before the cancel's own deadline, and the refusal met by then holds the
    // cancels: refused for rate, so, the drop test passed, the session ends by the abrupt drop
    // (0151 item 11), and the drop cancels the order.
    let model = QfexModel {
        ack_delay: Some(Duration::from_millis(1500)),
        ack_delay_on: 4,
        rate_limit: Some(model::Limited::Cancel),
        ..resting_cross()
    };
    let rpc = |argv| set(argv, "--rpc-timeout-ms", "4000");
    let d = pressed_at_place_with(model, "cross-held-late", 5, rpc).await;
    assert_cross_stopped(&d, places::Stop::Interrupted);
    let run = d.cross(0);
    let cancels = run.cancels.as_ref().unwrap();
    assert_eq!(cancels.sent, 1, "{cancels:?}");
    assert!(
        cancels.held && cancels.blocked && !cancels.ended,
        "{cancels:?}"
    );
    assert_eq!(run.ended_by, Some(stage::DROPPED));
    assert_eq!(d.crossing_orders()[0].state, model::State::Cancelled);
    assert!(d.wrote_naming("cancel_order", &d.crossing_orders()[0].id));
}

#[tokio::test]
async fn a_press_while_the_crossing_sessions_close_waits_for_room_is_taken_by_the_reads_after_it() {
    // The crossing post-only rests, and as QFEX reads the Stop's cancel of it, the run's first,
    // the shared budget is held for 3 s, as a provider rate refusal echoing nothing would. The
    // cancel is answered and its order seen to end, so the cancels are not held, but the
    // comparison after them reads nothing within its one second, and the graceful close waits
    // for room (0151 item 11). The owner presses Ctrl-C as that comparison ends, so the close is
    // still waiting. The cancels have already run, so the wait does not take the press: the
    // session ends by its graceful close once there is room, and the reads after it take the
    // press, which stops the run, marked, after every named account was read.
    let rate_user = user();
    let model = QfexModel {
        hold_on_cancel: Some((shared_limiter(rate_user), Duration::from_secs(3))),
        ..resting_cross()
    };
    let wait = |argv| set(argv, "--agree-wait-ms", "1000");
    let at = "COMPARE after the stop's cancels";
    let d = pressed_at_as(model, "cross-press-at-close", rate_user, wait, at).await;
    assert_cross_stopped(&d, places::Stop::Interrupted);
    let said = d.ev.stopped.as_deref().unwrap();
    assert!(
        said.starts_with("Interrupted during the reads after the crossing session"),
        "{said}"
    );
    assert_eq!(d.stage_two().crosses.len(), 1);
    let run = d.cross(0);
    assert_eq!(run.crossed, Some(stage_two::Crossed::Rested));
    // One round of cancels, before the wait: none ran again for the press.
    let cancels = run.cancels.as_ref().unwrap();
    assert_eq!(
        (cancels.sent, cancels.held, cancels.blocked, cancels.ended),
        (1, false, false, true)
    );
    assert_eq!(cancels.presses, 0, "{cancels:?}");
    assert_eq!(d.count("cancel_order"), 1, "{:?}", d.what());
    let steps: Vec<_> = run.compared.iter().map(|c| (c.step, c.ended)).collect();
    assert_eq!(steps[1], ("the stop's cancels", compare::Ended::Unread));
    // The close waited for room, so its frame was sent and the venue saw the client close it,
    // after the cancel.
    assert_eq!(run.ended_by, Some(stage::GRACEFUL));
    let conn = &d.cross_conns()[0];
    let n: usize = conn.parse().unwrap();
    assert!(d.venue.trade.connections()[n].client_closed);
    let what = d.what();
    let at = |w: &str| what.iter().position(|e| e.starts_with(w)).unwrap();
    let cancel = at(&format!("trade {conn} cancel_order"));
    assert!(cancel < at(&format!("cod {conn} close")), "{what:?}");
    assert_eq!(d.crossing_orders()[0].state, model::State::Cancelled);
    assert!(!run.resting && !run.counted_resting && run.read, "{run:?}");
}

// ----- FBC-77r8's review of PR #273, carried into stage two (FBC-su9i) ---------------------------

/// The line `d` printed when the reads after a crossing session listed an order or a TWAP not
/// ours.
fn others_after_cross(d: &Dropped) -> bool {
    let line = format!(
        "READ after the crossing session: {}",
        stage_two::OTHERS_CROSS
    );
    d.printed.contains(&line)
}

#[tokio::test]
async fn an_order_not_ours_the_reads_after_a_crossing_session_list_stops_read_disagrees() {
    // The crossing post-only is refused CANCELLED, so nothing of ours rests; but once its
    // connection ended, the primary account holds an order no run placed, as another trader's
    // would, which only the reads after the session see. It is what the streams did not show, so
    // the run stops ReadDisagrees (decision 0148 item 8), marked, not LeftResting with a hand
    // cancel of an order of ours (FBC-77r8's review).
    let model = QfexModel {
        after_cross_end: vec![model::AfterHistoric::Foreign],
        ..QfexModel::default()
    };
    let d = dropped(model, "cross-after-foreign").await;
    assert_cross_stopped(&d, places::Stop::ReadDisagrees);
    let stopped = d.ev.stopped.clone().unwrap();
    assert!(
        stopped.starts_with(
            "ReadDisagrees after the crossing session, a read listing an order or a TWAP not ours"
        ),
        "{stopped}"
    );
    let run = d.cross(0);
    assert_eq!(run.others, Some(true), "{run:?}");
    assert!(!run.resting && !run.counted_resting && run.read, "{run:?}");
    assert!(others_after_cross(&d), "{}", d.printed);
    assert!(!d.printed.contains("RESTING "), "{}", d.printed);
    let (report, json) = d.outputs();
    let section = cross_section(&report, "primary");
    assert!(section.contains(stage_two::OTHERS_CROSS), "{section}");
    // The session reached the check for another order of ours, which found none.
    assert!(
        section.contains("- No other order of ours (decision 0148 item 6): none: it may be placed"),
        "{section}"
    );
    assert_eq!(json["stage_two"]["crossings"][0]["others_after"], true);
    assert_eq!(
        json["stage_two"]["crossings"][0]["other_order_checked"],
        true
    );
    assert!(json["stage_two"]["crossings"][0]["hand_cancel"].is_null());
    assert_eq!(d.stage_two().crosses.len(), 1);
}

#[tokio::test]
async fn a_twap_the_reads_after_a_crossing_session_list_stops_read_disagrees() {
    // As above, but what the primary account's pages list once the crossing session's
    // connection ended is a TWAP: no order rests, and still the streams did not show it, so the
    // run stops ReadDisagrees, marked, rather than going on to the next account's crossing.
    let model = QfexModel {
        after_cross_end: vec![model::AfterHistoric::Twap],
        ..QfexModel::default()
    };
    let d = dropped(model, "cross-after-twap").await;
    assert_cross_stopped(&d, places::Stop::ReadDisagrees);
    let run = d.cross(0);
    assert_eq!(run.others, Some(true), "{run:?}");
    assert!(!run.resting && run.read, "{run:?}");
    assert!(others_after_cross(&d), "{}", d.printed);
    assert_eq!(d.stage_two().crosses.len(), 1);
}

#[tokio::test]
async fn a_position_the_reads_after_a_crossing_session_show_outranks_an_order_not_ours() {
    // Once the crossing session's connection ended, the primary account shows both a position
    // no order of ours made and an order not ours: a position not flat comes before an order the
    // streams did not show, as after drop session two, so the run stops NotFlat, the order not
    // ours still recorded (FBC-77r8's review).
    let model = QfexModel {
        after_cross_end: vec![
            model::AfterHistoric::Position,
            model::AfterHistoric::Foreign,
        ],
        ..QfexModel::default()
    };
    let d = dropped(model, "cross-after-position").await;
    assert_cross_stopped(&d, places::Stop::NotFlat);
    let run = d.cross(0);
    assert_eq!(run.others, Some(true), "{run:?}");
    assert!(others_after_cross(&d), "{}", d.printed);
}

#[tokio::test]
async fn an_order_not_ours_a_read_after_a_crossing_drop_lists_is_not_erased_by_a_later_one() {
    // The crossing post-only rests, and beside it the primary account holds an order no run
    // placed, which the comparison after it shows: ReadDisagrees, the session's stop. QFEX
    // answers the Stop's cancel RateLimited, so the session ends by the abrupt drop (0151 item
    // 11), whose cancel-on-disconnect cancels both. The first read after the drop still shows
    // both open, as one made before QFEX learns of the drop would; the next shows neither. The
    // first read's order not ours is not erased by the second: the reads end there, reporting it
    // (FBC-77r8's review).
    let model = QfexModel {
        rate_limit: Some(model::Limited::Cancel),
        // The third drop of the run: each named account's drop session two, then this one.
        drop_lag: Some((2, 1)),
        // The run's fifth place: X and Y on each named account, then the crossing post-only.
        foreign_order_on: Some(4),
        ..resting_cross()
    };
    let d = dropped_with(model, "cross-drop-foreign", short_agree).await;
    assert_cross_stopped(&d, places::Stop::ReadDisagrees);
    let run = d.cross(0);
    assert_eq!(run.ended_by, Some(stage::DROPPED), "{run:?}");
    assert_eq!(d.crossing_orders()[0].state, model::State::Cancelled);
    assert_eq!(run.others, Some(true), "{run:?}");
    assert!(others_after_cross(&d), "{}", d.printed);
    assert!(
        d.printed.contains(
            "READ after the crossing session's drop: primary: 1 of at most 2 rounds of reads of \
             every named account, the last judged listing an order or a TWAP not ours"
        ),
        "{}",
        d.printed
    );
}

// ----- FBC-su9i's review of its restack onto FBC-77r8's review fixes -----------------------------

#[tokio::test]
async fn an_order_not_ours_beside_an_order_of_ours_left_resting_stops_read_disagrees() {
    // The crossing post-only rests, and QFEX answers the Stop's cancel of it NO_SUCH_ORDER while
    // it still rests; once its connection ended, the primary account also holds an order no run
    // placed, which only the reads after the session see. An order not ours is what the streams
    // did not show, which comes before an order of ours left resting: the run stops
    // ReadDisagrees, marked, not LeftResting, and the order of ours is still reported for the
    // owner's hand cancel (decision 0148 item 8, 0132 item 7).
    let model = QfexModel {
        cancel_misses: true,
        after_cross_end: vec![model::AfterHistoric::Foreign],
        ..resting_cross()
    };
    let d = dropped(model, "cross-ours-and-foreign").await;
    assert_cross_stopped(&d, places::Stop::ReadDisagrees);
    let stopped = d.ev.stopped.clone().unwrap();
    assert!(
        stopped.starts_with(&format!("ReadDisagrees {}", stage::CROSS_OTHERS)),
        "{stopped}"
    );
    let run = d.cross(0);
    assert!(run.resting && run.read, "{run:?}");
    assert_eq!(run.others, Some(true), "{run:?}");
    assert_eq!(d.crossing_orders()[0].state, model::State::Open);
    assert!(others_after_cross(&d), "{}", d.printed);
    assert!(
        d.printed
            .contains(&format!("RESTING {}", stage_two::HAND_CANCEL_CROSS)),
        "{}",
        d.printed
    );
    let (report, json) = d.outputs();
    let section = cross_section(&report, "primary");
    assert!(section.contains(stage_two::HAND_CANCEL_CROSS), "{section}");
    assert!(section.contains(stage_two::OTHERS_CROSS), "{section}");
    assert_eq!(
        json["stage_two"]["crossings"][0]["hand_cancel"],
        stage_two::HAND_CANCEL_CROSS
    );
    assert_eq!(json["stage_two"]["crossings"][0]["others_after"], true);
    assert_eq!(d.stage_two().crosses.len(), 1);
}

#[tokio::test]
async fn a_later_round_after_a_crossing_drop_that_cannot_read_an_account_erases_nothing() {
    // As the_reads_after_a_held_drop_end_at_the_wait_with_an_order_still_listed, but once QFEX
    // learns of the drop, the primary account's GET /user/positions answers one read, then fails
    // every one after it. The round that cannot read the account is not judged and ends the
    // reads: the round before it, which listed the crossing post-only, stands, so the order of
    // ours is still reported for the owner's hand cancel and the account still read after the
    // session (FBC-su9i's review).
    let model = QfexModel {
        rate_limit: Some(model::Limited::Cancel),
        // The third drop of the run: each named account's drop session two, then this one.
        drop_lag: Some((2, 100)),
        rest_fails_after_drop: Some((2, 1)),
        ..resting_cross()
    };
    let d = dropped(model, "cross-held-lag-unread").await;
    assert_cross_stopped(
        &d,
        places::Stop::RateRefused {
            on: places::RefusedOn::Session,
        },
    );
    let run = d.cross(0);
    assert_eq!(run.ended_by, Some(stage::DROPPED), "{run:?}");
    assert!(run.resting && run.read, "{run:?}");
    assert!(
        d.printed
            .contains(&format!("RESTING {}", stage_two::HAND_CANCEL_CROSS)),
        "{}",
        d.printed
    );
    let line = "READ after the crossing session's drop: primary: ";
    let said = d.printed.lines().find(|l| l.starts_with(line));
    let said = said.unwrap_or_else(|| panic!("{}", d.printed));
    assert!(!said.contains(": 1 of at most"), "{said}");
    assert!(
        said.ends_with("the last judged listing an order of ours"),
        "{said}"
    );
    assert!(
        (d.ev.errors.iter()).any(|e| e.starts_with("primary (cross) could not be read")),
        "{:?}",
        d.ev.errors
    );
    let json = d.outputs().1;
    assert_eq!(
        json["stage_two"]["crossings"][0]["hand_cancel"],
        stage_two::HAND_CANCEL_CROSS
    );
    assert_eq!(json["stage_two"]["crossings"][0]["read_after"], true);
}

#[tokio::test]
async fn the_reads_after_a_crossing_drop_end_at_a_round_showing_a_position() {
    // The crossing post-only rests, and as it arrives the primary account shows a position no
    // order of ours made, which the comparison after it sees: ReadDisagrees, the session's stop.
    // QFEX answers the Stop's cancel RateLimited, so the session ends by the abrupt drop, and the
    // reads after it show the crossing post-only still open beside the position. A position not
    // flat ends those reads at once, so no later round can erase it, and the report says the
    // position is the owner's beside the session's stop (0151 item 9; FBC-su9i's review).
    let model = QfexModel {
        rate_limit: Some(model::Limited::Cancel),
        drop_lag: Some((2, 100)),
        // The run's fifth place: X and Y on each named account, then the crossing post-only.
        position_on_place_on: Some(4),
        ..resting_cross()
    };
    let d = dropped_with(model, "cross-drop-position", short_agree).await;
    assert_cross_stopped(&d, places::Stop::ReadDisagrees);
    let run = d.cross(0);
    assert_eq!(run.ended_by, Some(stage::DROPPED), "{run:?}");
    assert_eq!(run.not_flat_after, Some(true), "{run:?}");
    assert!(run.resting, "{run:?}");
    assert!(
        d.printed.contains(
            "READ after the crossing session's drop: primary: 1 of at most 2 rounds of reads of \
             every named account, the last judged listing an order of ours"
        ),
        "{}",
        d.printed
    );
    let (report, json) = d.outputs();
    let section = cross_section(&report, "primary");
    assert!(
        section.contains(&format!(
            "- A position not flat in the reads after the session: {}",
            stage_two::CROSS_POSITION_BESIDE
        )),
        "{section}"
    );
    assert_eq!(json["stage_two"]["crossings"][0]["not_flat_after"], true);
}

#[tokio::test]
async fn a_position_the_reads_after_a_crossing_session_show_beside_its_stop_is_the_owners() {
    // The crossing post-only is refused CANCELLED, but as it arrives the primary account shows a
    // position no order of ours made, as a hand trade's or a maker fill no frame told would: the
    // comparison after the crossing post-only sees it, so the session's stop is ReadDisagrees.
    // The reads after the session show it too: the report and the evidence say the position is
    // the owner's, though the run stopped first on the session's stop (0151 item 9; FBC-su9i's
    // review).
    let model = QfexModel {
        position_on_place_on: Some(4),
        ..QfexModel::default()
    };
    let d = dropped_with(model, "cross-position-beside", short_agree).await;
    assert_cross_stopped(&d, places::Stop::ReadDisagrees);
    let stopped = d.ev.stopped.clone().unwrap();
    assert!(
        stopped.starts_with("ReadDisagrees in the crossing session"),
        "{stopped}"
    );
    let run = d.cross(0);
    assert_eq!(run.not_flat_after, Some(true), "{run:?}");
    assert!(!run.resting && run.read, "{run:?}");
    let (report, json) = d.outputs();
    let section = cross_section(&report, "primary");
    assert!(
        section.contains(&format!(
            "- A position not flat in the reads after the session: {}",
            stage_two::CROSS_POSITION_BESIDE
        )),
        "{section}"
    );
    let crossing = &json["stage_two"]["crossings"][0];
    assert_eq!(crossing["not_flat_after"], true);
    assert_eq!(
        crossing["owners_position"],
        stage_two::CROSS_POSITION_BESIDE
    );
}

#[tokio::test]
async fn an_open_order_rest_alone_counts_after_a_crossing_session_stops_left_resting() {
    // The crossing post-only is refused CANCELLED, but as it arrives the primary account's GET
    // /user/positions counts one open order that no page lists, as a stop order would. The
    // comparisons read no such count; the reads after the session do, and an open order REST
    // alone counts is not told from ours: an order of ours left resting, a stop, marked, for the
    // owner's hand cancel (decision 0132 item 7; FBC-su9i's review).
    let model = QfexModel {
        stop_order_on: Some(4),
        ..QfexModel::default()
    };
    let d = dropped(model, "cross-rest-counted").await;
    assert_cross_stopped(&d, places::Stop::LeftResting);
    let stopped = d.ev.stopped.clone().unwrap();
    assert!(
        stopped.starts_with("LeftResting after the crossing session")
            && !stopped.contains("not read"),
        "{stopped}"
    );
    let run = d.cross(0);
    assert_eq!(
        run.crossed,
        Some(stage_two::Crossed::Cancelled { exact: true })
    );
    assert!(run.resting && !run.counted_resting && run.read, "{run:?}");
    assert_eq!(run.others, Some(false), "{run:?}");
    assert!(
        d.printed
            .contains(&format!("RESTING {}", stage_two::HAND_CANCEL_CROSS)),
        "{}",
        d.printed
    );
    let json = d.outputs().1;
    assert_eq!(
        json["stage_two"]["crossings"][0]["hand_cancel"],
        stage_two::HAND_CANCEL_CROSS
    );
    assert_eq!(d.stage_two().crosses.len(), 1);
}

/// [`short_wait`], three reads, for a run whose drop test comes before the step whose wait it
/// shortens: the drop test's reads after the drop have that wait too, and the model sees the drop
/// only once the conformance stub has recorded the connection's end on a task of its own, which a
/// loaded machine can put after a read made at once, so one read is not enough (FBC-4ptk). A
/// budget held so that a later comparison reads nothing is held well past that comparison's last
/// read, which begins 2 s after its first.
fn wait_past_a_drop(argv: Argv) -> Argv {
    short_wait(argv)
}

#[tokio::test]
async fn a_fill_of_ours_while_the_crossing_sessions_close_waits_for_room_is_flattened_first() {
    // The crossing post-only rests, and QFEX finds it filled whole as a maker as the Stop's
    // cancel of it arrives, the run's first: the FILLED order_response ends it at once, the
    // cancel answered NO_SUCH_ORDER after it, and the fill_response follows 4 s later. As QFEX
    // reads that cancel the shared budget is held for 6 s, so, the cancels ended and the market
    // disarmed, the graceful close waits for room (0151 item 11): the comparison after the
    // cancels, when the NO_SUCH_ORDER did not hold them first, reads nothing within its wait.
    // The fill reaches the session as the close waits, after the Stop's cancels and the disarm: it
    // is flattened once there is room and before the close (0151 item 9), the owner's Flatten
    // taking the market lease again to put the market in Exit, one reducing limit IOC filled,
    // and the market disarmed again after it. The run stops UnexpectedFill, flat.
    let rate_user = user();
    let model = QfexModel {
        hold_on_cancel: Some((shared_limiter(rate_user), Duration::from_secs(6))),
        fill_on_cancel: true,
        fill_on_cancel_after: Some(Duration::from_secs(4)),
        ..resting_cross()
    };
    let d = held_drop(model, "cross-fill-at-close", rate_user, wait_past_a_drop).await;
    assert_cross_stopped(&d, places::Stop::UnexpectedFill);
    assert_eq!(d.stage_two().crosses.len(), 1);
    let run = d.cross(0);
    assert_eq!(run.crossed, Some(stage_two::Crossed::Rested));
    let cancels = run.cancels.as_ref().unwrap();
    assert_eq!(
        (cancels.sent, cancels.blocked, cancels.ended),
        (1, false, true)
    );
    let flatten = run.flatten.as_ref().expect("a flatten");
    assert_eq!(flatten.after, flatten::AFTER_LATE_FILL);
    assert_flattened(flatten, 0);
    assert_flatten_iocs_within_the_cap(&d, &["SELL"]);
    assert_eq!(run.ended_by, Some(stage::GRACEFUL));
    // The disarm, then the flatten, then the disarm again, all before the session's end.
    let lines = [
        "STOP cancels: Kill, then 1 cancels by venue id (decision 0132 item 7, 0148 item 8)",
        "FLATTEN on primary: the Stop's Kill lifted",
        "FLATTEN on primary: the owner's Flatten, the market in Exit",
        "FLATTENED on primary: flat",
        "FLATTEN on primary: the market disarmed again, as the Stop left it",
        "SESSION primary (the crossing session): arm outcome Accepted, ended by a graceful close",
    ];
    for pair in lines.windows(2) {
        assert!(d.at(pair[0]) < d.at(pair[1]), "{pair:?}: {}", d.printed);
    }
    let flat = (d.ev.reads.iter())
        .filter(|r| r.before == refusals::AFTER_CROSS)
        .all(|r| r.flat);
    assert!(flat, "{:?}", d.ev.reads);
}

// ----- FBC-ou9g: the flatten, reached from every registry (decision 0148 items 5, 6, 8 and 9,
// 0151 items 5, 8 and 9), run against the model -----

/// The model's default, but X filled whole as a maker right after its ACK, and every limit IOC
/// answered as `ioc` says.
fn maker_filled(ioc: model::IocAnswer) -> QfexModel {
    QfexModel {
        maker_fill: true,
        ioc,
        ..QfexModel::default()
    }
}

/// [`dropped_with`], with the test's `hooks` set on the run (`probe_args::TestHooks`).
async fn hooked(
    model: QfexModel,
    what: &str,
    argv: impl Fn(Argv) -> Argv,
    hooks: probe_args::TestHooks,
) -> Dropped {
    let (dir, removed) = temp_dir(what);
    let venue = Venue::start(model).await;
    let (mut opts, mut places) = parsed(&argv(drop_argv(&dir, &venue))).unwrap();
    places.test = hooks;
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

/// `argv` with `--flatten-attempts` set to `n`.
fn attempts(n: &'static str) -> impl Fn(Argv) -> Argv {
    move |argv| set(argv, "--flatten-attempts", n)
}

/// How many times the model logged `event`.
fn events(d: &Dropped, event: &str) -> usize {
    d.what().iter().filter(|w| *w == event).count()
}

/// Asserts drop session one's flatten stopped with `stop`, the run's stop, not flat, after
/// `sent` flatten IOCs; the account read not flat after the session; its record, its report
/// section and stdout saying so. Gives the flatten.
fn assert_flatten_stopped(d: &Dropped, stop: places::Stop, sent: usize) -> &flatten::FlattenRun {
    assert_stopped(d, stop);
    let flatten = d.drop_run().flatten.as_ref().expect("a flatten");
    assert_eq!(flatten.stop, Some(stop.name()), "{flatten:?}");
    assert!(!flatten.flat && !flatten.rest_flat, "{flatten:?}");
    assert_eq!(flatten.sent(), sent, "{flatten:?}");
    let after = (d.ev.reads.iter())
        .find(|r| r.before == refusals::AFTER_DROP_ONE && r.label == "primary (drop)")
        .unwrap();
    assert!(!after.flat, "{after:?}");
    let (report, _) = d.outputs();
    assert!(
        report.contains(&format!("- How it ended: {}", flatten.ended())),
        "{report}"
    );
    let when = format!("{} in drop session one", stop.name());
    assert!(
        d.ev.stopped.as_deref().unwrap().starts_with(&when),
        "{:?}",
        d.ev.stopped
    );
    flatten
}

#[tokio::test]
async fn a_flatten_past_its_cap_is_not_sent_and_stops_capped() {
    // As X goes out, the touch's bid turns zero: once X fills as a maker, no positive price on
    // the grid is within --flatten-cap-bps of that bid, so the flatten IOC is not sent and the
    // run stops Flatten(Capped), not flat, the position the owner's (decision 0148 items 5 and
    // 8). The cap held on a grid coarser than the touch is `flatten_run.rs`'s.
    let zero_bid = model::Touch {
        bid: Some(("0.00", "1.00")),
        ask: Some(("100.10", "2.00")),
        age_secs: 0,
    };
    let model = QfexModel {
        maker_fill: true,
        touch_on_place: Some((0, zero_bid)),
        ..QfexModel::default()
    };
    let d = dropped(model, "flatten-capped").await;
    let capped = places::Stop::Flatten(places::Flattening::Capped);
    let flatten = assert_flatten_stopped(&d, capped, 0);
    assert_eq!(flatten.exit, Some(true));
    assert_eq!(flatten.iocs.len(), 1, "{flatten:?}");
    assert_eq!(flatten.iocs[0].side, "sell");
    assert_eq!(d.count("add_order"), 1, "{:?}", d.what());
    assert_eq!(events(&d, "mds touch"), 1);
    assert_flatten_iocs_within_the_cap(&d, &[]);
    let (report, json) = d.outputs();
    assert!(report.contains("  - IOC 1 (sell): not sent"), "{report}");
    assert_eq!(
        json["drop_tests"][0]["session_one"]["flatten"]["stop"],
        "Flatten(Capped)"
    );
    assert!(
        d.printed.contains("FLATTEN capped on primary"),
        "{}",
        d.printed
    );
}

#[tokio::test]
async fn flatten_attempts_used_up_stop_the_run_not_flat() {
    // Every flatten IOC expires unfilled: with --flatten-attempts 1, one IOC, then the budget is
    // used up; with 2, two, each placed only once the one before ended; either way the run stops
    // Flatten(UsedUp), not flat (decision 0148 items 5 and 8, 0151 item 9).
    for (n, sent) in [("1", 1), ("2", 2)] {
        let model = maker_filled(model::IocAnswer::Expire);
        let d = dropped_with(model, "flatten-used-up", attempts(n)).await;
        let used_up = places::Stop::Flatten(places::Flattening::UsedUp);
        let flatten = assert_flatten_stopped(&d, used_up, sent);
        for ioc in &flatten.iocs {
            assert_eq!(ioc.came_back, Some(flatten::Ioc::Expired), "{flatten:?}");
        }
        assert_eq!(events(&d, "ioc Expire"), sent);
        let sides = vec!["SELL"; sent];
        assert_flatten_iocs_within_the_cap(&d, &sides);
        // Compared after each IOC: an expired one leaves the position, which REST shows.
        let compared: Vec<crate::compare::Ended> =
            flatten.compared.iter().map(|c| c.ended).collect();
        assert_eq!(compared, vec![crate::compare::Ended::Agreed; sent]);
    }
}

#[tokio::test]
async fn a_refused_flatten_stops_not_flat() {
    // QFEX echoes the flatten IOC's add_order with an err: refused, left in flight, never resent
    // though a second attempt is left; the run stops Flatten(Refused), not flat (decision 0148
    // items 5 and 8).
    let model = maker_filled(model::IocAnswer::Refuse);
    let d = dropped_with(model, "flatten-refused", attempts("2")).await;
    let refused = places::Stop::Flatten(places::Flattening::Refused);
    let flatten = assert_flatten_stopped(&d, refused, 1);
    assert_eq!(flatten.iocs[0].came_back, Some(flatten::Ioc::Refused));
    assert_eq!(events(&d, "ioc Refuse"), 1);
    // fbc-oms refusing the IOC (its market Killed, a test's hook): never sent; and refusing the
    // owner's Flatten itself: no IOC built.
    let at_ioc = probe_args::TestHooks {
        kill_in_flatten: Some(probe_args::KillAt::Ioc),
        ..probe_args::TestHooks::default()
    };
    let d = hooked(
        maker_filled(model::IocAnswer::Full),
        "flatten-ioc-killed",
        |a| a,
        at_ioc,
    )
    .await;
    let flatten = assert_flatten_stopped(&d, refused, 0);
    assert_eq!(
        flatten.iocs[0].refused.as_deref(),
        Some("State"),
        "{flatten:?}"
    );
    assert_eq!(d.count("add_order"), 1, "{:?}", d.what());
    assert!(
        d.printed.contains("FLATTEN IOC refused on primary"),
        "{}",
        d.printed
    );
    let at_flatten = probe_args::TestHooks {
        kill_in_flatten: Some(probe_args::KillAt::Flatten),
        ..probe_args::TestHooks::default()
    };
    let model = maker_filled(model::IocAnswer::Full);
    let d = hooked(model, "flatten-exit-killed", |a| a, at_flatten).await;
    let flatten = assert_flatten_stopped(&d, refused, 0);
    assert_eq!((flatten.exit, flatten.iocs.len()), (Some(false), 0));
    assert!(
        d.printed.contains("FLATTEN refused on primary"),
        "{}",
        d.printed
    );
}

#[tokio::test]
async fn the_flatten_budget_is_shared_by_an_accounts_registries() {
    // The primary account's crossing post-only fills, so its cross registry flattens, from the
    // account's one budget its drop registry spends from too (decision 0148 items 5 and 7): with
    // that budget spent by the drop registry (a test's hook, as a flatten there would have spent
    // it, which a run never reaches since a flatten stops it), --flatten-attempts 1 leaves the
    // cross registry none, so no IOC goes out and the run stops Flatten(UsedUp); 2 leave it one,
    // which flattens. The account's allowance at function level is
    // `the_allowance_holds_an_accounts_flatten_iocs_to_one_budget_across_its_registries`.
    let spent = probe_args::TestHooks {
        flatten_spent: true,
        ..probe_args::TestHooks::default()
    };
    let crossing = || QfexModel {
        alo_cross: Some(model::AloCross::Fills),
        ..QfexModel::default()
    };
    let d = hooked(
        crossing(),
        "flatten-budget-spent",
        attempts("1"),
        spent.clone(),
    )
    .await;
    let used_up = places::Stop::Flatten(places::Flattening::UsedUp);
    assert_cross_stopped(&d, used_up);
    let flatten = d.cross(0).flatten.as_ref().expect("a flatten");
    assert_eq!(
        (flatten.sent(), flatten.exit),
        (0, Some(true)),
        "{flatten:?}"
    );
    assert_eq!(flatten.stop, Some(used_up.name()));
    assert_flatten_iocs_within_the_cap(&d, &[]);
    let d = hooked(crossing(), "flatten-budget-one-left", attempts("2"), spent).await;
    assert_cross_stopped(&d, places::Stop::UnexpectedFill);
    assert_flattened(d.cross(0).flatten.as_ref().expect("a flatten"), 0);
    assert_flatten_iocs_within_the_cap(&d, &["SELL"]);
}

#[tokio::test]
async fn an_unknown_order_blocks_a_flatten_ioc() {
    // QFEX answers the first flatten IOC with nothing: Unknown at its deadline, never resent and
    // counted resting, it keeps the second attempt back (decision 0148 item 6, 0151 item 5): the
    // run stops LeftResting, not flat, one IOC sent.
    let model = maker_filled(model::IocAnswer::Silent);
    let d = dropped_with(model, "flatten-unknown", attempts("2")).await;
    let flatten = assert_flatten_stopped(&d, places::Stop::LeftResting, 1);
    assert_eq!(flatten.iocs[0].came_back, Some(flatten::Ioc::Unknown));
    let blocked = flatten.blocked.clone().expect("kept back");
    assert!(
        blocked.starts_with("the drop registry on primary holds an order of ours"),
        "{blocked}"
    );
    assert_eq!(events(&d, "ioc Silent"), 1);
    assert!(
        d.printed
            .contains(&format!("BLOCKED a flatten IOC on primary: {blocked}")),
        "{}",
        d.printed
    );
}

#[tokio::test]
async fn a_flatten_ioc_filled_in_part_leaves_a_residual_below_the_minimum_and_stops() {
    // On an instrument of two lots' minimum, the flatten IOC fills one lot and QFEX ends it
    // IOC_PARTIALLY_FILLED with the rest left, which the adapter decodes as open with a fill
    // (0151 item 8): what is left of the position is below the minimum size, so the run stops
    // Flatten(BelowMinimum), not flat (decision 0148 items 5 and 8).
    let listed = Listed {
        min: "0.02",
        ..Listed::trading()
    };
    let model = QfexModel {
        listed: vec![listed],
        ..maker_filled(model::IocAnswer::Partial)
    };
    let d = dropped_with(model, "flatten-partial", short_agree).await;
    let below = places::Stop::Flatten(places::Flattening::BelowMinimum);
    let flatten = assert_flatten_stopped(&d, below, 1);
    assert_eq!(flatten.iocs[0].came_back, Some(flatten::Ioc::PartOpen));
    assert_eq!(events(&d, "ioc Partial"), 1);
    let model = d.venue.model.lock().unwrap();
    let ioc = model.orders.last().unwrap();
    assert_eq!(
        (ioc.quantity, ioc.filled),
        (Decimal::new(2, 2), Decimal::new(1, 2))
    );
}

#[tokio::test]
async fn a_position_frame_before_or_after_a_fill_leaves_the_flatten_to_the_registry() {
    // QFEX's position_response after each fill, before its fill_response or after its
    // order_response: the flatten reads the position from the registry alone, so it places one
    // IOC and ends flat either way.
    for push in [model::PositionPush::Before, model::PositionPush::After] {
        let model = QfexModel {
            position_push: Some(push),
            ..maker_filled(model::IocAnswer::Full)
        };
        let d = dropped(model, "flatten-position-push").await;
        assert_stopped(&d, places::Stop::UnexpectedFill);
        assert_flattened(d.drop_run().flatten.as_ref().expect("a flatten"), 0);
        assert_flatten_iocs_within_the_cap(&d, &["SELL"]);
        let journal = d.dir.join("evidence").join("journal");
        let reader = fbc_journal::JournalReader::open(&journal, 0).unwrap();
        let positions = reader
            .entries()
            .filter(|e| match &e.as_ref().unwrap().record {
                fbc_journal::Record::Inbound { bytes, .. } => {
                    String::from_utf8_lossy(&bytes.0).contains("position_response")
                }
                _ => false,
            })
            .count();
        // Each fill's position_response, X's and the IOC's, is pushed with its frames. Before
        // them, the session reads both, ahead of each fill_response it waits on. After them,
        // X's comes ahead of the IOC's answer, which the flatten waits on, so it is read; but
        // the IOC's comes after its order_response, the last frame the session waits on, and
        // the session may close before reading it. The journal keeps only what the session
        // read, so it holds 1 or 2 there, and nothing orders the second (FBC-4ptk's third
        // kind; PR #277's CI run 44483279366 counted 1).
        match push {
            model::PositionPush::Before => {
                assert_eq!(positions, 2, "X's fill and the IOC's: {push:?}")
            }
            model::PositionPush::After => assert!(
                (1..=2).contains(&positions),
                "X's fill's, and the IOC's if read before the close: {positions} {push:?}"
            ),
        }
        assert!(d.ev.errors.is_empty(), "{:?}", d.ev.errors);
    }
}

#[tokio::test]
async fn a_connection_that_ends_during_the_flatten_ends_the_session_not_flat() {
    // QFEX closes the session's connection as the flatten IOC arrives, answering nothing: the
    // flatten ends there, the session with it, with no second attempt, and the run stops on the
    // fill it answered, not flat.
    let d = dropped(maker_filled(model::IocAnswer::Close), "flatten-closed").await;
    assert_stopped(&d, places::Stop::UnexpectedFill);
    let run = d.drop_run();
    let flatten = run.flatten.as_ref().expect("a flatten");
    assert_eq!(flatten.error.as_deref(), Some(flatten::CONNECTION_ENDED));
    assert!(!flatten.flat && flatten.stop.is_none(), "{flatten:?}");
    assert_eq!(run.ended_by, Some(stage::CONNECTION_ENDED));
    assert_eq!(events(&d, "ioc Close"), 1);
    // The IOC went out before the connection ended: recorded as sent, its answer never heard.
    assert_eq!(flatten.sent(), 1, "{flatten:?}");
    assert_eq!(flatten.iocs[0].came_back, None, "{flatten:?}");
    let (report, json) = d.outputs();
    assert!(
        report.contains(&format!(
            "- The flatten IOCs, 1 sent:\n  - IOC 1 (sell): {}",
            flatten::IN_FLIGHT
        )),
        "{report}"
    );
    let ioc = &json["drop_tests"][0]["session_one"]["flatten"]["iocs"][0];
    assert_eq!(
        (&ioc["sent"], &ioc["came_back"]),
        (&Value::Bool(true), &Value::Null)
    );
    let after = (d.ev.reads.iter())
        .find(|r| r.before == refusals::AFTER_DROP_ONE && r.label == "primary (drop)")
        .unwrap();
    assert!(!after.flat);
}

#[tokio::test]
async fn ctrl_c_during_the_flatten_is_recorded_and_ends_nothing() {
    // The owner presses Ctrl-C as the flatten IOC, which QFEX leaves unanswered, waits for its
    // deadline: the press is recorded and the flatten runs its course, here to its budget's end,
    // Flatten(UsedUp), the run's stop over the fill it answered.
    let model = maker_filled(model::IocAnswer::Silent);
    let d = pressed_at_place_with(model, "flatten-ctrl-c", 2, attempts("1")).await;
    let used_up = places::Stop::Flatten(places::Flattening::UsedUp);
    let flatten = assert_flatten_stopped(&d, used_up, 1);
    assert_eq!(flatten.presses, 1, "{flatten:?}");
    let (report, _) = d.outputs();
    assert!(
        report.contains("- Presses of Ctrl-C while it ran, each recorded, none ending it: 1"),
        "{report}"
    );
}

#[tokio::test]
async fn ctrl_c_during_the_taker_flatten_stops_the_run_once_flat() {
    // The owner presses Ctrl-C as the primary account's flatten IOC after its taker fill goes
    // out. The flatten takes the press and runs its course, flat, and the press is then the
    // session's stop (decision 0148 item 8), since the taker IOC's fill is expected and the
    // flatten gives no stop of its own. The session's Stop cancels as before; the run stops
    // Interrupted, marked, after the reads after the taker IOC session, and the subaccount's
    // crossing post-only and taker IOC are never placed.
    let at = "FLATTEN IOC on primary: one reducing";
    let d = pressed_at(QfexModel::default(), "take-flatten-ctrl-c", at).await;
    assert_take_stopped(&d, places::Stop::Interrupted);
    assert_eq!(d.stage_two().takes.len(), 1, "{}", d.printed);
    let run = d.take(0);
    let flatten = run.flatten.as_ref().expect("a flatten");
    assert_flattened(flatten, 0);
    assert_eq!((flatten.after, flatten.presses), (taker::AFTER_TAKER, 1));
    assert!(run.cancels.is_some(), "the session's Stop: {run:?}");
    assert_eq!((d.iocs("BUY").len(), d.iocs("SELL").len()), (1, 1));
    assert!(
        !d.printed
            .contains("PLACE the crossing post-only on subaccount")
    );
}

#[tokio::test]
async fn ctrl_c_during_a_late_taker_flatten_stops_the_run_once_flat() {
    // As in a_taker_fill_that_reaches_the_session_after_its_steps_is_flattened_before_the_close,
    // the primary account's taker IOC fills only as the Stop's cancel of it arrives, and is
    // flattened then, before the close. The owner presses Ctrl-C as that flatten's IOC goes out:
    // the flatten ends flat, and the press is the session's stop, so the run stops Interrupted,
    // marked, and the subaccount's sessions never start.
    let model = QfexModel {
        ioc: model::IocAnswer::Rest,
        ioc_sells: Some(model::IocAnswer::Full),
        fill_on_cancel: true,
        ..QfexModel::default()
    };
    let at = "FLATTEN IOC on primary: one reducing";
    let d = pressed_at(model, "take-late-flatten-ctrl-c", at).await;
    assert_take_stopped(&d, places::Stop::Interrupted);
    assert_eq!(d.stage_two().takes.len(), 1, "{}", d.printed);
    let flatten = d.take(0).flatten.as_ref().expect("a late flatten");
    assert_flattened(flatten, 0);
    assert_eq!(
        (flatten.after, flatten.presses),
        (taker::AFTER_LATE_TAKER, 1)
    );
    assert_eq!((d.iocs("BUY").len(), d.iocs("SELL").len()), (1, 1));
}

#[tokio::test]
async fn ctrl_c_during_the_taker_sessions_stop_cancels_stops_the_run() {
    // QFEX acks the primary account's taker IOC and leaves it open, so the session runs its
    // course with no stop and its Stop cancels the IOC by its venue id (decision 0132 item 7).
    // That cancel, the run's first, is answered two seconds late, and the owner presses Ctrl-C
    // while the cancels wait for it: the press ends none of them and is the session's stop
    // (decision 0148 item 8), so the run stops Interrupted, marked, and the subaccount's
    // sessions never start.
    let model = QfexModel {
        ioc: model::IocAnswer::Rest,
        cancel_delay: Some(Duration::from_secs(2)),
        ..QfexModel::default()
    };
    let rpc = |argv| set(argv, "--rpc-timeout-ms", "5000");
    let d = pressed_drop(model, "take-stop-cancels-ctrl-c", rpc, &[" cancel_order"]).await;
    assert_take_stopped(&d, places::Stop::Interrupted);
    assert_eq!(d.stage_two().takes.len(), 1, "{}", d.printed);
    let cancels = d.take(0).cancels.as_ref().expect("the session's Stop");
    assert_eq!((cancels.sent, cancels.presses), (1, 1), "{cancels:?}");
    assert!(cancels.ended && !cancels.held, "{cancels:?}");
    assert_eq!(d.count("cancel_order"), 1, "{:?}", d.what());
}

#[tokio::test]
async fn a_rate_refusal_before_a_press_during_the_taker_sessions_cancels_is_the_stop() {
    // QFEX acks the primary account's taker IOC and leaves it open, so the session's Stop cancels
    // it by its venue id, and QFEX answers that cancel, the run's first, with a RateLimited err,
    // which settles nothing, so the cancels wait. The owner presses Ctrl-C only once the probe has
    // met that refusal, the shared limiter counting it: the refusal came first, so it is the
    // session's stop, and the run's, the press recorded (FBC-su9i's review, on the taker path).
    let rate_user = user();
    let model = QfexModel {
        ioc: model::IocAnswer::Rest,
        rate_limit: Some(model::Limited::Cancel),
        ..QfexModel::default()
    };
    let rpc = |argv| set(argv, "--rpc-timeout-ms", "5000");
    let limiter = shared_limiter(rate_user);
    let refused = |venue: &Venue| {
        let log = venue.log();
        let read = log.iter().any(|e| e.what.ends_with(" cancel_order"));
        read && limiter.counts().rejected.user > 0
    };
    let d = pressed_once_when(model, "take-refused-then-ctrl-c", rate_user, rpc, refused).await;
    let refused = places::Stop::RateRefused {
        on: places::RefusedOn::Session,
    };
    assert_take_stopped(&d, refused);
    assert_eq!(d.stage_two().takes.len(), 1, "{}", d.printed);
    let cancels = d.take(0).cancels.as_ref().expect("the session's Stop");
    assert_eq!((cancels.sent, cancels.presses), (1, 1), "{cancels:?}");
    assert!(cancels.held && cancels.blocked, "{cancels:?}");
    let stopped = d.ev.stopped.as_deref().unwrap_or_default();
    let said = format!("{} in the taker IOC session", refused.name());
    assert!(stopped.starts_with(&said), "{stopped}");
}

#[tokio::test]
async fn ctrl_c_during_taker_stop_cancels_that_the_connections_end_cuts_short_stops_the_run() {
    // QFEX acks the primary account's taker IOC and leaves it open, so the session's Stop cancels
    // it by its venue id. QFEX leaves that cancel unanswered and closes the connection once the
    // owner has pressed Ctrl-C: the press, taken while the cancels wait, came before the
    // connection's end, so the session's stop is Interrupted, the press and the cancel sent
    // recorded, though the connection's end cut the cancels short (decision 0148 item 8;
    // FBC-su9i's review, on the taker path).
    let model = QfexModel {
        ioc: model::IocAnswer::Rest,
        close_on_cancel: true,
        hold_cancel: true,
        ..QfexModel::default()
    };
    let rpc = |argv| set(argv, "--rpc-timeout-ms", "5000");
    let d = pressed_drop(model, "take-cut-cancels-ctrl-c", rpc, &[" cancel_order"]).await;
    assert_take_stopped(&d, places::Stop::Interrupted);
    assert_eq!(d.stage_two().takes.len(), 1, "{}", d.printed);
    let run = d.take(0);
    assert_eq!(run.ended_by, Some(stage::CONNECTION_ENDED), "{run:?}");
    let cancels = run.cancels.as_ref().expect("the session's Stop");
    assert_eq!((cancels.sent, cancels.presses), (1, 1), "{cancels:?}");
    assert!(
        cancels.held && cancels.cut_short && !cancels.ended && !cancels.blocked,
        "{cancels:?}"
    );
    assert_eq!(d.count("cancel_order"), 1, "{:?}", d.what());
    let (report, json) = d.outputs();
    let section = take_section(&report, "primary");
    assert!(section.contains(places::CUT_SHORT), "{section}");
    let cancels = &json["stage_two"]["takes"][0]["cancels"];
    assert_eq!(cancels["sent"], 1, "{cancels}");
    assert_eq!(cancels["cut_short"], true, "{cancels}");
}

#[tokio::test]
async fn a_press_as_the_last_read_after_a_taker_session_ends_is_taken_by_those_reads() {
    // The owner presses Ctrl-C as the last named account's read after the primary account's
    // taker IOC session prints its line, so the press is made in the poll that ends that read,
    // which wins the reads' wait for it: the reads still take it as they end, so the run stops
    // Interrupted after them, marked, and the subaccount's reads before its own crossing session
    // never begin (FBC-su9i's review, on the taker path).
    let at = "READ after the taker IOC session: subaccount: ";
    let d = pressed_at_as(
        QfexModel::default(),
        "take-press-last-read",
        user(),
        |a| a,
        at,
    )
    .await;
    assert_take_stopped(&d, places::Stop::Interrupted);
    let said = d.ev.stopped.as_deref().unwrap_or_default();
    assert!(
        said.starts_with("Interrupted during the reads after the taker IOC session"),
        "{said}"
    );
    assert_eq!(d.stage_two().takes.len(), 1, "{}", d.printed);
    assert_eq!(d.iocs("BUY").len(), 1, "{}", d.printed);
    let before = (d.ev.reads.iter()).filter(|r| r.before == places::CROSS);
    assert_eq!(before.count(), 2, "{}", d.printed);
}

#[tokio::test]
async fn a_rate_refusal_before_a_press_during_a_flattens_ioc_is_the_stop() {
    // The primary account's taker IOC fills, so its session flattens: one sell IOC, whose answer
    // QFEX holds, sending a RateLimited err in its place. The owner presses Ctrl-C only once the
    // probe has met that refusal, the shared limiter counting it, while the flatten waits for its
    // IOC; then QFEX answers the IOC filled, and the flatten ends flat. The refusal came before
    // the press, so it is the session's stop, and the run's, in place of Interrupted (decision
    // 0148 item 8; FBC-su9i's review, on the flatten's path).
    let rate_user = user();
    let model = QfexModel {
        hold_ioc_sell: true,
        rate_with_ioc_sell: true,
        ..QfexModel::default()
    };
    let limiter = shared_limiter(rate_user);
    let refused = |venue: &Venue| {
        let log = venue.log();
        let iocs = log.iter().filter(|e| e.what.ends_with("ioc Full")).count();
        iocs == 2 && limiter.counts().rejected.user > 0
    };
    let d = pressed_once_when(
        model,
        "flatten-refused-then-ctrl-c",
        rate_user,
        |a| a,
        refused,
    )
    .await;
    let refused = places::Stop::RateRefused {
        on: places::RefusedOn::Session,
    };
    assert_take_stopped(&d, refused);
    assert_eq!(d.stage_two().takes.len(), 1, "{}", d.printed);
    let flatten = d.take(0).flatten.as_ref().expect("a flatten");
    assert_flattened(flatten, 0);
    assert_eq!(flatten.presses, 1, "{flatten:?}");
    let stopped = d.ev.stopped.as_deref().unwrap_or_default();
    let said = format!("{} in the taker IOC session", refused.name());
    assert!(stopped.starts_with(&said), "{stopped}");
}

#[tokio::test]
async fn a_press_during_a_flattens_ioc_that_the_connections_end_cuts_short_is_counted() {
    // The primary account's taker IOC fills, so its session flattens: one sell IOC, which QFEX
    // leaves unanswered, closing the connection once the owner has pressed Ctrl-C while the
    // flatten waits for it. The connection's end drops the flatten there: the press it took is
    // the session's stop, Interrupted, and its record counts it (FBC-su9i's review, on the
    // flatten's path).
    let model = QfexModel {
        ioc_sells: Some(model::IocAnswer::Close),
        hold_ioc_sell: true,
        ..QfexModel::default()
    };
    let d = pressed_drop(model, "flatten-cut-ctrl-c", |a| a, &["ioc Close"]).await;
    let flatten = d.take(0).flatten.as_ref().expect("a flatten");
    assert_eq!(flatten.presses, 1, "{flatten:?}");
    assert_eq!(
        flatten.error.as_deref(),
        Some(flatten::CONNECTION_ENDED),
        "{flatten:?}"
    );
    assert_eq!(d.take(0).ended_by, Some(stage::CONNECTION_ENDED));
    assert_eq!(d.stage_two().takes.len(), 1, "{}", d.printed);
    assert_eq!(
        d.ev.stop,
        Some(places::Stop::Interrupted.name()),
        "{}",
        d.printed
    );
}

#[tokio::test]
async fn a_flatten_whose_other_orders_are_not_seen_to_end_places_no_ioc() {
    // Y fills as a maker in drop session two, and QFEX answers the cancel of X NO_SUCH_ORDER,
    // leaving it open: X not seen to end, no flatten IOC is placed (decision 0148 item 6), and
    // the run stops on the flatten's own stop, Flatten(NotEnded), in place of the fill's, not
    // flat, X resting for the owner's hand cancel.
    let model = QfexModel {
        on_ack: Some((1, model::OnAck::MakerFill)),
        cancel_misses: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "flatten-not-ended").await;
    let not_ended = places::Stop::Flatten(places::Flattening::NotEnded);
    assert_stopped(&d, not_ended);
    let flatten = d.two().flatten.as_ref().expect("a flatten");
    assert_eq!(flatten.stop, Some(not_ended.name()));
    let cancels = flatten.cancels.as_ref().unwrap();
    assert!(cancels.held && cancels.sent == 1, "{cancels:?}");
    assert_eq!(flatten.error.as_deref(), Some(flatten::NOT_ENDED));
    assert_eq!((flatten.exit, flatten.sent()), (None, 0));
    assert_eq!(events(&d, "ioc Full"), 0);
    assert!(d.two().resting, "{:?}", d.two());
}

#[tokio::test]
async fn a_flatten_with_no_touch_to_price_from_places_no_ioc() {
    // As X goes out the touch turns crossed: once X fills, the flatten has no touch to price an
    // IOC from, so none is placed, and the run stops on the flatten's own stop, Flatten(NoTouch),
    // in place of the fill's, not flat.
    let crossed = model::Touch {
        bid: Some(("100.20", "1.00")),
        ask: Some(("100.10", "2.00")),
        age_secs: 0,
    };
    let model = QfexModel {
        maker_fill: true,
        touch_on_place: Some((0, crossed)),
        ..QfexModel::default()
    };
    let d = dropped(model, "flatten-no-touch").await;
    let no_touch = places::Stop::Flatten(places::Flattening::NoTouch);
    let flatten = assert_flatten_stopped(&d, no_touch, 0);
    assert_eq!(flatten.error.as_deref(), Some(market::CROSSED));
    assert_eq!((flatten.exit, flatten.iocs.len()), (Some(true), 0));
    assert_eq!(d.count("add_order"), 1);
}

#[tokio::test]
async fn a_rate_refusal_in_a_flatten_comparison_stops_it() {
    // The read after the flatten IOC is refused RateLimited: a provider rate refusal, a stop
    // (decision 0148 item 8), which the flatten gives its session over the fill it answered. The
    // refusal is the first page after the flatten IOC came: a comparison after X's place begun
    // before the session read X's fill is never the one refused (FBC-23ch's third review fix).
    let model = QfexModel {
        rate_limit: Some(model::Limited::ReadSocketAfterIoc),
        ..maker_filled(model::IocAnswer::Full)
    };
    let d = dropped(model, "flatten-rate").await;
    let refused = places::Stop::RateRefused {
        on: places::RefusedOn::ReadSocket,
    };
    assert_stopped(&d, refused);
    let flatten = d.drop_run().flatten.as_ref().expect("a flatten");
    assert_eq!(flatten.stop, Some(refused.name()));
    assert_eq!(flatten.iocs[0].came_back, Some(flatten::Ioc::Filled));
    let ended: Vec<crate::compare::Ended> = flatten.compared.iter().map(|c| c.ended).collect();
    assert_eq!(ended, [crate::compare::Ended::Refused]);
}

#[tokio::test]
async fn a_fill_during_the_stops_cancels_is_flattened_while_the_session_is_open() {
    // The owner presses Ctrl-C in drop session one, and QFEX fills X as a maker as the Stop's
    // cancel of it arrives, answering the cancel NO_SUCH_ORDER: a fill of ours that reached the
    // session after its steps ended, while it is still open. The flatten follows the Stop's
    // cancels (0151 item 9): the Stop's Kill lifted, the owner's Flatten into Exit, one reducing
    // limit IOC within the cap, REST flat; then the market Killed again and disarmed, the close,
    // and the run stops Interrupted, its first stop, flat.
    let model = QfexModel {
        fill_on_cancel: true,
        ..QfexModel::default()
    };
    let d = pressed_at_place(model, "flatten-late", 1).await;
    assert_stopped(&d, places::Stop::Interrupted);
    let run = d.drop_run();
    // The Stop's one cancel, of X: X seen to end by its fill, whether or not NO_SUCH_ORDER was
    // read before the wait saw it, so held or not.
    let cancels = run.cancels.as_ref().expect("the Stop's cancels");
    assert_eq!(cancels.sent, 1, "{cancels:?}");
    let flatten = run.flatten.as_ref().expect("a flatten");
    assert_eq!(flatten.after, flatten::AFTER_LATE_FILL);
    assert_flattened(flatten, 0);
    assert_eq!(run.ended_by, Some(stage::GRACEFUL));
    assert_eq!(events(&d, "ioc Full"), 1);
    assert_flatten_iocs_within_the_cap(&d, &["SELL"]);
    let after = (d.ev.reads.iter())
        .find(|r| r.before == refusals::AFTER_DROP_ONE && r.label == "primary (drop)")
        .unwrap();
    assert!(after.flat, "{after:?}");
    assert!(
        d.printed.contains(
            "FLATTEN on primary: the Stop's Kill lifted, the market Cancel-only (decision 0012)"
        ),
        "{}",
        d.printed
    );
    assert!(
        d.printed
            .contains("FLATTEN on primary: the market Killed again, as the Stop left it"),
        "{}",
        d.printed
    );
    let (report, _) = d.outputs();
    assert!(
        report.contains(&format!(
            "After {}, while the session was open",
            flatten.after
        )),
        "{report}"
    );
}

#[tokio::test]
async fn a_crossing_post_only_filled_during_the_stops_cancels_is_flattened() {
    // The crossing post-only rests, and QFEX fills it as a maker as the session's Stop cancel of
    // it arrives: the flatten follows the Stop's cancels while the session is open (0151 item 9),
    // and the run stops UnexpectedFill, flat.
    let model = QfexModel {
        alo_cross: None,
        fill_on_cancel: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "cross-late-fill").await;
    assert_cross_stopped(&d, places::Stop::UnexpectedFill);
    let run = d.cross(0);
    let cancels = run.cancels.as_ref().expect("the session's Stop");
    assert_eq!(cancels.sent, 1, "{cancels:?}");
    let flatten = run.flatten.as_ref().expect("a flatten");
    assert_eq!(flatten.after, flatten::AFTER_LATE_FILL);
    assert_flattened(flatten, 0);
    assert_flatten_iocs_within_the_cap(&d, &["SELL"]);
    let flat = (d.ev.reads.iter())
        .filter(|r| r.before == refusals::AFTER_CROSS)
        .all(|r| r.flat);
    assert!(flat, "{:?}", d.ev.reads);
}

#[tokio::test]
async fn a_cancel_answered_cancelled_while_the_order_rests_keeps_the_flatten_ioc_back() {
    // Y fills as a maker in drop session two, and QFEX answers the flatten's cancel of X
    // CANCELLED while X still rests: the registry holds X ended, but the comparison after the
    // flatten's cancels reads it open, a stop (decision 0148 item 8), so no flatten IOC goes out
    // beside another order of ours (decision 0148 item 6), and the run stops ReadDisagrees, not
    // flat.
    let model = QfexModel {
        on_ack: Some((1, model::OnAck::MakerFill)),
        cancel_lies: true,
        ..QfexModel::default()
    };
    let d = dropped_with(model, "flatten-cancel-lies", short_agree).await;
    assert_eq!(
        d.ev.stop,
        Some(places::Stop::ReadDisagrees.name()),
        "{}",
        d.printed
    );
    let flatten = d.two().flatten.as_ref().expect("a flatten");
    let cancels = flatten.cancels.as_ref().unwrap();
    assert!(
        cancels.ended && !cancels.held && cancels.sent == 1,
        "{cancels:?}"
    );
    let ended: Vec<crate::compare::Ended> = flatten.compared.iter().map(|c| c.ended).collect();
    assert_eq!(ended, [crate::compare::Ended::Disagreed]);
    assert_eq!(flatten.stop, Some(places::Stop::ReadDisagrees.name()));
    assert_eq!(
        (flatten.exit, flatten.sent(), flatten.flat),
        (None, 0, false)
    );
    assert_eq!(events(&d, "ioc Full"), 0);
}

#[tokio::test]
async fn a_flatten_whose_other_order_is_not_acked_by_its_deadline_places_no_ioc() {
    // As Y's add_order arrives, QFEX fills X as a maker and never answers Y: the flatten's
    // cancels wait for Y's acknowledgement to cancel it by venue id, which never comes, so by
    // --rpc-timeout-ms Y is neither cancelled nor seen to end, nothing held. No flatten IOC is
    // placed beside it (decision 0148 item 6), and the run stops on the flatten's own stop,
    // Flatten(NotEnded), not flat.
    let model = QfexModel {
        fill_other_on_place: Some(1),
        ..QfexModel::default()
    };
    let d = dropped(model, "flatten-unacked").await;
    let flatten = d.two().flatten.as_ref().expect("a flatten");
    let cancels = flatten.cancels.as_ref().unwrap();
    assert!(
        !cancels.ended && !cancels.held && cancels.sent == 0,
        "{cancels:?}"
    );
    assert_eq!(flatten.error.as_deref(), Some(flatten::NOT_ENDED));
    assert_eq!(
        (flatten.exit, flatten.sent(), flatten.flat),
        (None, 0, false)
    );
    assert!(flatten.compared.is_empty(), "{flatten:?}");
    assert_eq!(events(&d, "ioc Full"), 0);
    assert_stopped(&d, places::Stop::Flatten(places::Flattening::NotEnded));
}

#[tokio::test]
async fn a_named_accounts_read_listing_an_order_keeps_a_flatten_ioc_back() {
    // X fills as a maker in drop session one, and the first named account's read the flatten
    // judges by lists an order (a test's hook: every named account reads clean before each
    // session, so no answer of the QFEX model reaches this): no flatten IOC goes out (decision
    // 0148 item 6), and the run stops ReadDisagrees, not flat.
    let listed = probe_args::TestHooks {
        listed_in_flatten: true,
        ..probe_args::TestHooks::default()
    };
    let model = maker_filled(model::IocAnswer::Full);
    let d = hooked(model, "flatten-listed", |a| a, listed).await;
    let flatten = assert_flatten_stopped(&d, places::Stop::ReadDisagrees, 0);
    let blocked = flatten.blocked.clone().expect("kept back");
    assert!(
        blocked.starts_with("primary's never-arming read lists an open order"),
        "{blocked}"
    );
    assert_eq!((flatten.exit, flatten.iocs.len()), (Some(true), 0));
    assert_eq!(events(&d, "ioc Full"), 0);
}

// ----- Stage two's taker IOC (FBC-ou9g; decision 0132 item 7, 0148 items 5 and 10, 0151 items 8,
// 9 and 10) -----

impl Dropped {
    /// The `n`th taker IOC session, in run order.
    fn take(&self, n: usize) -> &stage_two::CrossRun {
        &self.stage_two().takes[n]
    }

    /// The model's limit IOCs on `side`, in order: the taker IOCs on --side (buy), the flatten
    /// IOCs on the other.
    fn iocs(&self, side: &str) -> Vec<model::Order> {
        let model = self.venue.model.lock().unwrap();
        let iocs = (model.orders.iter()).filter(|o| o.kind == "LIMIT" && o.tif == "IOC");
        iocs.filter(|o| o.side == side).cloned().collect()
    }

    /// The JSON frames of the places-production/ slice's files whose name begins `prefix`, by
    /// the frame's one top-level member's name.
    fn sliced(&self, prefix: &str, member: &str) -> Vec<Value> {
        let places = self.dir.join("evidence").join(stage::PLACES_SLICE);
        let mut found = Vec::new();
        let mut names: Vec<String> = (fs::read_dir(&places).unwrap())
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with(prefix) && n.ends_with(".json"))
            .collect();
        names.sort();
        for name in names {
            let v: Value = super::json(&fs::read_to_string(places.join(&name)).unwrap());
            if let Some(body) = v.get(member) {
                found.push(body.clone());
            }
        }
        found
    }
}

/// The run's lines that say where the taker IOC session on `account` stood, in order: its
/// fill flattened and REST shown flat while it was open, then its Stop and its end, then the
/// read of every named account after it.
fn assert_flattened_before_the_stop(d: &Dropped, account: &str) {
    let lines = [
        format!("PLACE the taker IOC on {account}"),
        format!("TAKEN on {account}: filled whole, its fills settled"),
        format!("FLATTEN on {account}: the owner's Flatten, the market in Exit"),
        format!("FLATTENED on {account}: flat"),
        format!("SESSION {account} (the taker IOC session): arm outcome Accepted"),
        format!("READ after the taker IOC session: {account} (take)"),
    ];
    for pair in lines.windows(2) {
        assert!(d.at(&pair[0]) < d.at(&pair[1]), "{pair:?}: {}", d.printed);
    }
}

#[tokio::test]
async fn the_taker_ioc_fills_at_the_touch() {
    // Each named account's drop test passed and the exact-echo gate opened, so after each
    // account's crossing session, its taker IOC session: one limit IOC at the minimum size at the
    // far touch, the ask of 100.10, through it, which QFEX fills whole as a taker against the
    // touch. Its fill is expected, so once flattened it stops nothing: the run ends after the
    // last account's taker IOC, naming FBC-4plj, with no stop and no marker.
    let d = dropped(QfexModel::default(), "take-fills").await;
    assert_eq!((d.ev.stop, d.marker()), (None, None), "{}", d.printed);
    assert_eq!(d.ev.stopped.as_deref(), Some(stage::NOT_BUILT));
    assert!(stage::NOT_BUILT.contains("FBC-4plj"));
    assert_eq!(d.stage_two().takes.len(), 2);
    for (n, account) in ["primary", "subaccount"].into_iter().enumerate() {
        let run = d.take(n);
        assert_eq!(run.account, account);
        assert!(run.authenticated && run.resynced && run.started, "{run:?}");
        assert_eq!(run.arm.as_deref(), Some("Accepted"));
        assert!(run.placed.submitted && run.blocked.is_none(), "{run:?}");
        let taken = run.taken.as_ref().expect("the taker IOC");
        assert_eq!(taken.ioc, Some(flatten::Ioc::Filled));
        assert_eq!(taken.fees, [taker::FeeSign::Paid]);
        assert_eq!(taken.frame_order, Some(taker::FrameOrder::FillFirst));
        assert!(run.read && !run.resting && !run.counted_resting, "{run:?}");
        assert_eq!((run.stop, &run.error), (None, &None));
        assert!(d.printed.contains(&format!(
            "TAKEN on {account}: filled whole, its fills settled"
        )));
    }
    // The model saw one taker IOC a session: a limit IOC buy at the minimum size at the ask,
    // filled whole as a taker, on the taker IOC session's own connection.
    let takers = d.iocs("BUY");
    assert_eq!(takers.len(), 2);
    for ioc in &takers {
        assert_eq!(ioc.quantity, Decimal::new(1, 2));
        assert_eq!(ioc.price, super::json("100.1"));
        assert_eq!(
            (ioc.filled, ioc.state),
            (ioc.quantity, model::State::Filled)
        );
    }
    assert_eq!(events(&d, "ioc Full"), 4);
    let conns = d.take_conns();
    assert_eq!(conns.len(), 2);
    for conn in &conns {
        let placed = format!("trade {conn} add_order");
        assert_eq!(d.what().iter().filter(|w| **w == placed).count(), 2);
    }
    let (report, json) = d.outputs();
    let section = take_section(&report, "primary");
    for said in [
        "- The taker IOC: handed to the session yes;",
        "- How the taker IOC came back: filled whole, its fills settled",
        "- The taker fee's sign (decision 0148 item 10; no amount): paid",
        &format!("- What followed: {}", taker::NEXT_ACCOUNT),
    ] {
        assert!(section.contains(said), "{said}: {section}");
    }
    let last = take_section(&report, "subaccount");
    assert!(last.contains(&format!("- What followed: {}", taker::LAST_TAKE)));
    let entry = &json["stage_two"]["takes"][0];
    assert_eq!(entry["account"], "primary");
    assert_eq!(
        entry["taken"]["came_back"],
        "filled whole, its fills settled"
    );
    assert_eq!(entry["taken"]["fee_sign"], "paid");
    assert_eq!(entry["taker_ioc"]["submitted"], true);
}

#[tokio::test]
async fn the_taker_fill_is_flattened_through_exit() {
    // The taker IOC's fill leaves the take registry long one minimum size. While its session is
    // open: no other order of ours to cancel, the owner's Flatten into Exit (Start arms only
    // while flat, so Flatten is what reaches Exit after a fill: decision 0148 item 3), then one
    // reducing limit IOC at the minimum size that QFEX fills against the bid, the registry's
    // position read flat, and a never-arming read agreeing with it: REST positions flat, all
    // before the session's Stop and the read after it (decision 0148 item 5).
    let d = dropped(QfexModel::default(), "take-flattened").await;
    assert_eq!(d.ev.stop, None, "{}", d.printed);
    for (n, account) in ["primary", "subaccount"].into_iter().enumerate() {
        let run = d.take(n);
        let flatten = run.flatten.as_ref().expect("a flatten");
        assert_flattened(flatten, 0);
        assert_eq!(flatten.after, taker::AFTER_TAKER);
        assert_eq!(flatten.sent(), 1);
        assert_eq!(flatten.iocs[0].side, "sell");
        assert_eq!(flatten.compared.len(), 1);
        assert_eq!(flatten.compared[0].ended, compare::Ended::Agreed);
        // Then the session's Stop: nothing of ours left to cancel, and a graceful close.
        let cancels = run.cancels.as_ref().expect("the session's Stop");
        assert!(cancels.ended && !cancels.held && cancels.sent == 0);
        assert_eq!(run.ended_by, Some(stage::GRACEFUL));
        assert_flattened_before_the_stop(&d, account);
    }
    // Every read after a taker IOC session showed every named account flat with no open order.
    let after: Vec<&capture::AccountRead> = (d.ev.reads.iter())
        .filter(|r| r.before == refusals::AFTER_TAKE)
        .collect();
    assert_eq!(after.len(), 4);
    assert!(
        after.iter().all(|r| r.flat && r.page_orders == 0),
        "{after:?}"
    );
    let (report, json) = d.outputs();
    let section = take_section(&report, "primary");
    assert!(
        section.contains(&format!(
            "After {}, while the session was open",
            taker::AFTER_TAKER
        )),
        "{section}"
    );
    assert!(section.contains("- The owner's Flatten: pressed: the market in Exit"));
    let entry = &json["stage_two"]["takes"][1]["flatten"];
    assert_eq!(
        (&entry["flat"], &entry["rest_flat"], &entry["exit"]),
        (&Value::Bool(true), &Value::Bool(true), &Value::Bool(true))
    );
}

#[tokio::test]
async fn the_flatten_prices_within_its_cap() {
    // Each flatten IOC after a taker fill is a sell, the side that reduces the position, at the
    // minimum size with no reduce-only flag, priced at most --flatten-cap-bps (10) through the
    // bid it crosses and still crossing it; the taker IOCs, buys at the ask, sit within the cap
    // of the ask too, since each is at it (assert_flatten_iocs_within_the_cap).
    let d = dropped(QfexModel::default(), "take-flatten-cap").await;
    assert_eq!(d.ev.stop, None, "{}", d.printed);
    assert_flatten_iocs_within_the_cap(&d, &["BUY", "SELL", "BUY", "SELL"]);
    let flattens = d.iocs("SELL");
    assert_eq!(flattens.len(), 2);
    for ioc in &flattens {
        // Through the bid of 100.00: 99.90 is the cap's floor, snapped up onto the cent grid.
        let px = Decimal::from_str(&ioc.price.to_string()).unwrap();
        assert!(
            px < Decimal::new(100, 0) && px >= Decimal::new(999, 1),
            "{px}"
        );
        assert_eq!(ioc.state, model::State::Filled);
    }
    // At the cap's floor exactly, on a cap of 13 bps: 99.87 is on the grid, so the flatten IOC
    // is priced there and no lower.
    let d = dropped_in(
        QfexModel::default(),
        "take-flatten-cap-13",
        &primary_only(),
        |a| set(a, "--flatten-cap-bps", "13"),
    )
    .await;
    assert_eq!(d.ev.stop, None, "{}", d.printed);
    let flattens = d.iocs("SELL");
    assert_eq!(flattens.len(), 1);
    assert_eq!(flattens[0].price, super::json("99.87"));
}

#[tokio::test]
async fn an_ioc_that_expires_unfilled_is_recorded_and_flattens_nothing() {
    // QFEX expires every limit IOC, IOC_CANCELLED with nothing filled: the taker IOC is recorded
    // as expired unfilled, nothing is flattened, and the run goes on with no stop, ending after
    // the last account's taker IOC.
    let model = QfexModel {
        ioc: model::IocAnswer::Expire,
        ..QfexModel::default()
    };
    let d = dropped(model, "take-expires").await;
    assert_eq!((d.ev.stop, d.marker()), (None, None), "{}", d.printed);
    assert_eq!(d.ev.stopped.as_deref(), Some(stage::NOT_BUILT));
    assert_eq!(events(&d, "ioc Expire"), 2);
    assert_eq!(d.iocs("SELL").len(), 0, "no flatten IOC");
    for (n, account) in ["primary", "subaccount"].into_iter().enumerate() {
        let run = d.take(n);
        let taken = run.taken.as_ref().unwrap();
        assert_eq!(taken.ioc, Some(flatten::Ioc::Expired));
        assert!(taken.fees.is_empty());
        assert_eq!(taken.frame_order, Some(taker::FrameOrder::NoFill));
        assert_eq!(run.flatten, None);
        assert!(!run.counted_resting && !run.resting && run.read, "{run:?}");
        assert!(d.printed.contains(&format!(
            "TAKEN on {account}: expired unfilled: recorded, and nothing flattened"
        )));
    }
    assert!(!d.printed.contains("FLATTEN"), "{}", d.printed);
    let (report, json) = d.outputs();
    let section = take_section(&report, "primary");
    for said in [
        "- How the taker IOC came back: expired unfilled: recorded, and nothing flattened",
        "- The taker fee's sign (decision 0148 item 10; no amount): not measured: the taker IOC \
         brought no fill",
        "(FBC-tfxk): not measured: no fill_response named the taker IOC",
    ] {
        assert!(section.contains(said), "{said}: {section}");
    }
    assert!(!section.contains("#### The flatten"), "{section}");
    assert_eq!(json["stage_two"]["takes"][0]["flatten"], Value::Null);
    // The reads after each crossing and taker IOC session ran when that session did, so the
    // refusals each session met say so (FBC-ou9g).
    let rows = refusals::by_session(&d.ev.read_sockets, &d.ev.sessions, None);
    for after in [refusals::AFTER_CROSS, refusals::AFTER_TAKE] {
        let row = rows.iter().find(|(name, ..)| *name == after).unwrap();
        assert_eq!((row.1, row.2), (0, true), "{rows:?}");
    }
    assert!(!report.contains("(its session did not run)"), "{report}");
}

#[tokio::test]
async fn a_partial_taker_fill_stops_the_run_not_flat() {
    // On an instrument of two lots' minimum, QFEX fills one lot of the taker IOC and ends it
    // IOC_PARTIALLY_FILLED with the rest left, which the adapter decodes as open with a fill:
    // the run stops IocRemainder, not flat, with no flatten (0151 item 8, until FBC-jdj7). The
    // session's Stop still cancels by venue id what the registry counts open, and the reads
    // after it show the position off flat, the owner's to close by hand.
    let listed = Listed {
        min: "0.02",
        ..Listed::trading()
    };
    let model = QfexModel {
        listed: vec![listed],
        ioc: model::IocAnswer::Partial,
        ..QfexModel::default()
    };
    let d = dropped_with(model, "take-partial", wait_past_a_drop).await;
    let stop = places::Stop::IocRemainder;
    assert_stopped(&d, stop);
    assert_eq!(events(&d, "ioc Partial"), 1);
    assert_eq!(d.stage_two().takes.len(), 1, "no later account's session");
    assert_eq!(d.stage_two().crosses.len(), 1);
    let run = d.take(0);
    let taken = run.taken.as_ref().unwrap();
    assert_eq!(taken.ioc, Some(flatten::Ioc::PartOpen));
    assert_eq!(taken.fees, [taker::FeeSign::Paid]);
    assert_eq!(run.flatten, None, "no flatten");
    assert_eq!(d.iocs("SELL").len(), 0);
    assert_eq!(run.stop, Some("IocRemainder"));
    let cancels = run.cancels.as_ref().expect("the session's Stop");
    assert_eq!(cancels.sent, 1, "{cancels:?}");
    let after: Vec<&capture::AccountRead> = (d.ev.reads.iter())
        .filter(|r| r.before == refusals::AFTER_TAKE)
        .collect();
    assert!(after.iter().any(|r| !r.flat), "{after:?}");
    let model = d.venue.model.lock().unwrap();
    let taker = model.orders.last().unwrap();
    assert_eq!(
        (taker.quantity, taker.filled),
        (Decimal::new(2, 2), Decimal::new(1, 2))
    );
    drop(model);
    assert!(
        d.ev.stopped
            .as_deref()
            .unwrap()
            .starts_with("IocRemainder in the taker IOC session"),
        "{:?}",
        d.ev.stopped
    );
    let (report, json) = d.outputs();
    let section = take_section(&report, "primary");
    assert!(
        section.contains(&format!(
            "- How the taker IOC came back: {}",
            taker::REMAINDER
        )),
        "{section}"
    );
    assert_eq!(json["stage_two"]["takes"][0]["stop"], "IocRemainder");
    let declared = declared_section(&report);
    assert!(
        declared.contains("a hand close of the position"),
        "{declared}"
    );
    assert_eq!(json["declared"]["hand_actions_left"]["hand_close"], true);
}

/// The report's section on what the run declares and the owner's part (FBC-ou9g).
fn declared_section(report: &str) -> String {
    let heading = "## What the run declares, and the owner's part";
    let section = report
        .split(heading)
        .nth(1)
        .unwrap_or_else(|| panic!("{report}"));
    section.split("\n## ").next().unwrap().to_owned()
}

#[tokio::test]
async fn the_report_names_the_key_declarations_fences_and_fee_sign_and_no_value() {
    // The report names the key every place-stage session carries, each registry's CodProbeRun
    // declaration, the fences met, the taker fee's sign on each account, --sole-trader and the
    // owner's confirmations as unchecked, and every hand action (decision 0148 item 10), and
    // states the caps, the distance, the flatten cap and the attempts only as fences met, never
    // as values (0151 item 10): no value the command line gave reaches the section or the
    // evidence's entry.
    let values = |argv: Argv| {
        let argv = set(argv, "--resting-cap-usd", "424242.17");
        let argv = set(argv, "--inventory-cap-usd", "373737.29");
        let argv = set(argv, "--away-bps", "47");
        let argv = set(argv, "--flatten-cap-bps", "13");
        set(argv, "--flatten-attempts", "2")
    };
    let d = dropped_with(QfexModel::default(), "take-declared", values).await;
    assert_eq!(d.ev.stop, None, "{}", d.printed);
    let (report, json) = d.outputs();
    let section = declared_section(&report);
    for said in [
        "- The key: `qfex.exec.production_places = probe_place_stages`, on every place-stage \
         session's configuration and on no read's (decision 0148 item 4)",
        "- The CodProbeRun declarations, each on its registry's one market, none a testnet run \
         (decision 0148 item 3): drop on primary: CodProbeRun::owner_supervised_places; drop on \
         subaccount: CodProbeRun::owner_supervised_places; cross on primary: \
         CodProbeRun::owner_supervised_places; take on primary: \
         CodProbeRun::owner_supervised_places; cross on subaccount: \
         CodProbeRun::owner_supervised_places; take on subaccount: \
         CodProbeRun::owner_supervised_places",
        &format!("- The fences met, never their values: {}", taker::FENCES),
        "- The taker fee's sign, by account (no amount): primary: paid; subaccount: paid",
        &format!("- Not checked by the probe: {}", taker::UNCHECKED),
        "- The hand actions declared: no `--reconciled` declaration was given",
        "- The hand actions this run leaves the owner: none: the last read of every named \
         account showed no order of ours and a flat position",
    ] {
        assert!(section.contains(said), "{said}: {section}");
    }
    // No value: once the record's own fixed fences and decision citations are taken out, no
    // digit is left in the section.
    let mut bare = section.clone();
    for fixed in [
        "0148",
        "0151",
        "0144",
        "0132",
        "0113",
        "0006",
        "item 10",
        "item 3",
        "item 4",
        "item 7",
        "item 9",
        "at least 20 bps",
        "1 to 3",
    ] {
        bare = bare.replace(fixed, "");
    }
    assert!(!bare.chars().any(|c| c.is_ascii_digit()), "{bare}");
    let declared = &json["declared"];
    assert_eq!(declared["sole_trader_checked"], false);
    assert_eq!(declared["confirmations_checked"], false);
    let text = declared.to_string();
    for value in ["424242", "373737", "47", "13"] {
        assert!(!text.contains(value), "{value}: {text}");
    }
    fn numbers(v: &Value) -> bool {
        match v {
            Value::Number(_) => true,
            Value::Array(a) => a.iter().any(numbers),
            Value::Object(o) => o.values().any(numbers),
            _ => false,
        }
    }
    assert!(!numbers(declared), "{declared}");
    // The report and the evidence pass the privacy check, as does the places-production/ slice,
    // which holds the taker IOC sessions' frames.
    let evidence = d.dir.join("evidence");
    assert_report_and_evidence_pass_the_privacy_check(&evidence);
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/privacy-check.sh");
    evidence::privacy_check(&script, &evidence.join(stage::PLACES_SLICE)).unwrap();
    // With --reconciled, the declaration is reported as given and unchecked: a second run after
    // a stop marker the test writes.
    let (dir, _removed) = temp_dir("take-declared-reconciled");
    let venue = Venue::start(QfexModel::default()).await;
    let leases = leases_of(&dir);
    fs::create_dir_all(&leases).unwrap();
    places::tag(&leases).unwrap();
    assert!(places::write_marker(&leases, places::Stop::NotFlat, &[], "synthetic").unwrap());
    let asked = set(drop_argv(&dir, &venue), "--reconciled", "hand-close");
    let (ran, printed) = place_run(&venue, &asked, &primary_only()).await;
    let ev = stopped(ran, &printed);
    assert_eq!(ev.stop, None, "{printed}");
    let (report, _) = outputs_of(&dir);
    let section = declared_section(&report);
    assert!(
        section.contains(
            "- The hand actions declared: `--reconciled hand-close`, the owner's declaration of \
             what was done by hand after an earlier run's stop (0151 item 6): recorded as given \
             and unchecked"
        ),
        "{section}"
    );
}

#[tokio::test]
async fn the_fee_sign_knob_gives_the_matching_sign_word() {
    // QFEX writes the taker fill's fee paid (positive), zero or received (negative): the report
    // gives its sign as the adapter decodes it into fbc-core's Fee, a cost positive when paid
    // (decision 0004), never its amount, and the places-production/ slice keeps that fee as its
    // sign alone (0151 item 7).
    for (knob, sign, word, kept) in [
        (model::FeeSign::Paid, taker::FeeSign::Paid, "paid", 1),
        (model::FeeSign::Zero, taker::FeeSign::Zero, "zero", 0),
        (
            model::FeeSign::Received,
            taker::FeeSign::Received,
            "received",
            -1,
        ),
    ] {
        let model = QfexModel {
            fee_sign: knob,
            ..QfexModel::default()
        };
        let d = dropped_in(model, "take-fee-sign", &primary_only(), |a| a).await;
        assert_eq!(d.ev.stop, None, "{word}: {}", d.printed);
        let taken = d.take(0).taken.as_ref().unwrap();
        assert_eq!(taken.fees, [sign], "{word}");
        assert!(d.printed.contains(&format!("the taker fee's sign: {word}")));
        let (report, json) = d.outputs();
        let section = take_section(&report, "primary");
        assert!(
            section.contains(&format!(
                "- The taker fee's sign (decision 0148 item 10; no amount): {word}\n"
            )),
            "{section}"
        );
        let declared = declared_section(&report);
        assert!(declared.contains(&format!("(no amount): primary: {word}\n")));
        assert_eq!(json["stage_two"]["takes"][0]["taken"]["fee_sign"], word);
        // The slice's taker fill keeps the fee's sign alone.
        let fills = d.sliced("primary-the-taker-IOC-session-", "fill_response");
        assert!(!fills.is_empty());
        assert_eq!(fills[0]["fee"], kept, "{word}");
        assert_eq!(fills[0]["execution_type"], "TAKER");
        let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/privacy-check.sh");
        let places = d.dir.join("evidence").join(stage::PLACES_SLICE);
        evidence::privacy_check(&script, &places).unwrap();
    }
}

#[tokio::test]
async fn the_report_gives_the_iocs_frame_order() {
    // QFEX sends the taker IOC's fill_response before its final order_response, or after it
    // (FBC-tfxk asks which): the report gives the order the session heard, and either way the
    // fill reaches the registry, so the taker fill is flattened and the run ends with no stop.
    for (knob, order) in [
        (
            model::IocFrameOrder::FillFirst,
            taker::FrameOrder::FillFirst,
        ),
        (
            model::IocFrameOrder::OrderFirst,
            taker::FrameOrder::OrderFirst,
        ),
    ] {
        let model = QfexModel {
            ioc_frame_order: knob,
            ..QfexModel::default()
        };
        let d = dropped_in(model, "take-frame-order", &primary_only(), |a| a).await;
        assert_eq!(d.ev.stop, None, "{order:?}: {}", d.printed);
        let run = d.take(0);
        let taken = (run.taken.as_ref()).unwrap_or_else(|| panic!("{run:?}: {}", d.printed));
        assert_eq!(taken.frame_order, Some(order));
        assert_eq!(taken.ioc, Some(flatten::Ioc::Filled));
        assert_flattened(run.flatten.as_ref().unwrap(), 0);
        let (report, json) = d.outputs();
        let section = take_section(&report, "primary");
        assert!(
            section.contains(&format!(
                "- The order of its final order_response and its fill_response (FBC-tfxk): {}",
                order.says()
            )),
            "{section}"
        );
        assert_eq!(
            json["stage_two"]["takes"][0]["taken"]["frame_order"],
            order.says()
        );
        // The slice keeps the frames in the order received: the taker session's first
        // order_response or fill_response is the one QFEX sent first.
        let places = d.dir.join("evidence").join(stage::PLACES_SLICE);
        let mut names: Vec<String> = (fs::read_dir(&places).unwrap())
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with("primary-the-taker-IOC-session-"))
            .collect();
        names.sort_by_key(|n| {
            let n = n.trim_start_matches("primary-the-taker-IOC-session-");
            n.split('-').next().unwrap().parse::<u32>().unwrap()
        });
        let first = names.iter().find_map(|name| {
            let v: Value = super::json(&fs::read_to_string(places.join(name)).unwrap());
            ["fill_response", "order_response"].into_iter().find(|m| {
                v.get(m)
                    .is_some_and(|b| b.get("status") != Some(&serde_json::json!("ACK")))
            })
        });
        let want = match order {
            taker::FrameOrder::FillFirst => "fill_response",
            _ => "order_response",
        };
        assert_eq!(first, Some(want), "{names:?}");
    }
}

/// Asserts that `d` stopped with `stop` during or after the primary account's taker IOC session:
/// its marker written, every named account read after that session, and the report and the
/// evidence naming it and passing the privacy check.
fn assert_take_stopped(d: &Dropped, stop: places::Stop) {
    let name = stop.name();
    assert_eq!(d.ev.stop, Some(name), "{}", d.printed);
    let marker = d.marker().unwrap_or_default();
    assert!(marker.starts_with(&format!("stop: {name}\n")), "{marker}");
    let after: Vec<&str> = (d.ev.reads.iter())
        .filter(|r| r.before == refusals::AFTER_TAKE)
        .map(|r| r.label.as_str())
        .collect();
    assert_eq!(after, ["primary (take)", "subaccount"], "{}", d.printed);
    // Said as met in or after the taker IOC session, never after the crossing session.
    let stopped = d.ev.stopped.as_deref().unwrap_or_default();
    let when = stopped.split(':').next().unwrap_or_default();
    assert!(when.starts_with(&format!("{name} ")), "{stopped}");
    assert!(
        when.contains("the taker IOC session") && !when.contains("crossing"),
        "{stopped}"
    );
    let (report, json) = d.outputs();
    assert!(
        report.contains("| after the taker IOC session | primary (take) |"),
        "{report}"
    );
    assert_eq!(json["stage_two"]["takes"][0]["stop"], name);
    assert_eq!(d.stage_two().crosses.len(), 1, "no later account's session");
    assert_report_and_evidence_pass_the_privacy_check(&d.dir.join("evidence"));
    assert_no_secret(&d.dir.join("evidence"));
    d.venue.assert_served();
}

#[tokio::test]
async fn an_arm_not_accepted_in_the_taker_ioc_session_places_no_taker_ioc() {
    // QFEX rejects the taker IOC session's arm (the run's sixth): its connection ends, so no taker
    // IOC is placed, the end is unplanned, a stop, and the run stops with its reads after it.
    let model = QfexModel {
        arm_err_on: Some(5),
        ..QfexModel::default()
    };
    let d = dropped(model, "take-arm-err").await;
    assert_take_stopped(&d, places::Stop::ConnectionEnded);
    assert_eq!(d.count("add_order"), 5, "{:?}", d.what());
    let run = d.take(0);
    assert_eq!(run.arm.as_deref(), Some("Rejected"));
    assert!(!run.started && !run.placed.submitted && run.taken.is_none());
    assert_eq!(run.ended_by, Some(stage::CONNECTION_ENDED));
    let section = take_section(&d.outputs().0, "primary");
    assert!(
        section.contains("- How the taker IOC came back: not measured: it was not placed"),
        "{section}"
    );
}

#[tokio::test]
async fn a_taker_ioc_waits_for_no_other_order_of_ours() {
    // An order of ours, never sent, held in the primary account's cross registry before the
    // taker IOC session's check (a test's hook, as for the crossing order): the taker IOC is kept
    // back, a stop, LeftResting, with no taker IOC (decision 0148 item 6).
    let hooks = probe_args::TestHooks {
        take_other_order: Some(0),
        ..probe_args::TestHooks::default()
    };
    let d = hooked(QfexModel::default(), "take-blocked", |a| a, hooks).await;
    assert_take_stopped(&d, places::Stop::LeftResting);
    assert_eq!(d.count("add_order"), 5, "{:?}", d.what());
    let run = d.take(0);
    let blocked = run.blocked.clone().unwrap();
    assert!(
        blocked.starts_with("the cross registry on primary holds an order of ours"),
        "{blocked}"
    );
    assert!(run.started && !run.placed.submitted && run.taken.is_none());
    assert!(d.printed.contains(&format!(
        "BLOCKED the taker IOC on primary: {blocked} (decision 0148 item 6)"
    )));
    let section = take_section(&d.outputs().0, "primary");
    assert!(
        section.contains(&format!(
            "- No other order of ours (decision 0148 item 6): kept back: {blocked}"
        )),
        "{section}"
    );
}

#[tokio::test]
async fn a_taker_ioc_never_answered_is_never_resent_and_stops_the_run() {
    // QFEX never answers the taker IOC: at its deadline it is Unknown, never resent (decision
    // 0005) and counted resting, so nothing is flattened, and the take registry still counting
    // it resting after the session stops the run, LeftResting.
    let model = QfexModel {
        ioc: model::IocAnswer::Silent,
        ..QfexModel::default()
    };
    let d = dropped(model, "take-silent").await;
    assert_take_stopped(&d, places::Stop::LeftResting);
    assert_eq!(events(&d, "ioc Silent"), 1);
    let run = d.take(0);
    assert_eq!(run.taken.as_ref().unwrap().ioc, Some(flatten::Ioc::Unknown));
    assert_eq!(run.flatten, None);
    assert!(run.counted_resting && !run.resting, "{run:?}");
    let section = take_section(&d.outputs().0, "primary");
    assert!(
        section.contains("the take registry still counts an order of ours resting"),
        "{section}"
    );
}

#[tokio::test]
async fn a_taker_ioc_refused_by_an_err_is_recorded_and_stops_the_run() {
    // QFEX refuses the taker IOC with an err echoing its add_order: recorded as refused, nothing
    // filled or flattened; the err settles nothing, so the take registry still counts it after
    // the session, a stop, LeftResting, never resent.
    let model = QfexModel {
        ioc: model::IocAnswer::Refuse,
        ..QfexModel::default()
    };
    let d = dropped(model, "take-refused").await;
    assert_take_stopped(&d, places::Stop::LeftResting);
    let run = d.take(0);
    assert_eq!(run.taken.as_ref().unwrap().ioc, Some(flatten::Ioc::Refused));
    assert_eq!(run.flatten, None);
    assert!(d.printed.contains("TAKEN on primary: refused by an err"));
}

#[tokio::test]
async fn a_taker_ioc_left_resting_is_reported_for_the_hand_cancel() {
    // QFEX acks the taker IOC and leaves it resting, which no IOC does, and answers the Stop's
    // cancel of it NO_SUCH_ORDER: the read after the session lists it, an order of ours left
    // resting, reported for the owner's hand cancel (decision 0132 item 7).
    let model = QfexModel {
        ioc: model::IocAnswer::Rest,
        cancel_misses: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "take-rests").await;
    assert_take_stopped(&d, places::Stop::LeftResting);
    let run = d.take(0);
    assert!(run.resting && run.read, "{run:?}");
    assert_eq!(run.taken.as_ref().unwrap().ioc, Some(flatten::Ioc::Open));
    assert!(
        d.printed
            .contains(&format!("RESTING {}", taker::HAND_CANCEL_TAKE))
    );
    let (report, json) = d.outputs();
    assert!(take_section(&report, "primary").contains(taker::HAND_CANCEL_TAKE));
    assert_eq!(
        json["stage_two"]["takes"][0]["hand_cancel"],
        taker::HAND_CANCEL_TAKE
    );
    let declared = declared_section(&report);
    assert!(
        declared.contains("a hand cancel on QFEX production's site"),
        "{declared}"
    );
}

#[tokio::test]
async fn the_reads_after_a_taker_sessions_held_drop_read_again_until_its_cancel_shows() {
    // FBC-23ch's third review fix, as the crossing session carries it, carried to the taker IOC
    // session: QFEX acks the taker IOC and leaves it resting, and refuses the Stop's cancel of
    // it RateLimited, so the session ends by the abrupt drop (0151 item 11), whose cancel QFEX
    // makes once it learns of it. Its first read of the account after the drop still shows the
    // IOC open, as a read made before then would. The reads after the taker IOC session read
    // every named account again at the comparisons' cadence (0151 item 4), and the next one shows
    // it gone: nothing of ours is left resting, and the run reports no hand cancel.
    let model = QfexModel {
        ioc: model::IocAnswer::Rest,
        rate_limit: Some(model::Limited::Cancel),
        // The third drop of the run: each named account's drop session two, then this one.
        drop_lag: Some((2, 1)),
        ..QfexModel::default()
    };
    let d = dropped(model, "take-held-lag").await;
    let refused = places::Stop::RateRefused {
        on: places::RefusedOn::Session,
    };
    assert_eq!(d.ev.stop, Some(refused.name()), "{}", d.printed);
    let run = d.take(0);
    assert_eq!(run.ended_by, Some(stage::DROPPED), "{run:?}");
    assert!(!run.resting && run.read, "{run:?}");
    assert!(!d.printed.contains("RESTING "), "{}", d.printed);
    let line = "READ after the taker IOC session's drop: primary: ";
    let said = d.printed.lines().find(|l| l.starts_with(line));
    let said = said.unwrap_or_else(|| panic!("{}", d.printed));
    assert!(!said.contains(": 1 of at most 5 rounds"), "{said}");
    assert!(
        said.ends_with("the last judged listing no order of ours"),
        "{said}"
    );
}

#[tokio::test]
async fn a_taker_ioc_with_no_touch_to_price_it_is_not_placed() {
    // As the primary account's crossing post-only goes out the MDS touch turns crossed, so the
    // taker IOC has no touch to be priced from: it is not placed, a failure that is no stop,
    // recorded as the taker IOC session's error, and the run ends on it with no marker.
    let model = QfexModel {
        crossed_on_place: Some(4),
        ..QfexModel::default()
    };
    let d = dropped(model, "take-no-touch").await;
    assert_eq!(d.count("add_order"), 5, "{:?}", d.what());
    let run = d.take(0);
    assert!(run.started && !run.placed.submitted, "{run:?}");
    let error = run.error.clone().unwrap();
    assert_eq!(error, market::CROSSED, "{}", d.printed);
    assert_eq!(d.ev.stopped.as_deref(), Some(error.as_str()));
    assert_eq!((d.ev.stop, d.marker()), (None, None));
    assert_eq!(run.taken, None);
    let section = take_section(&d.outputs().0, "primary");
    assert!(section.contains(&format!("- Error: {error}")), "{section}");
}

// ----- FBC-ou9g: what the flatten and the taker IOC stop on when they cannot run their course
// (decision 0005, 0148 items 5, 6 and 9, 0151 item 9) -----

#[tokio::test]
async fn a_taker_ioc_whose_fill_never_reaches_the_session_stops_the_run_not_flat() {
    // QFEX ends the taker IOC FILLED, but its fill_response never reaches the session: at its
    // deadline the take registry holds it ended with no fill and its position flat, while QFEX's
    // count states the fill. The flatten would judge the registry's position, which is not
    // QFEX's, so nothing is flattened and the run stops IocUnsettled, not flat, marked, in the
    // session, before the reads after it, whatever they show. REST lags the fill, so the
    // comparison after the IOC agrees with the registry: with one read lagging, the reads after
    // the session show the position; with three, they show it flat too. REST lags from the
    // taker IOC, the run's sixth add_order, on: a read before the drop must list Y.
    for lag in [1, 3] {
        let model = QfexModel {
            ioc_frame_order: model::IocFrameOrder::Withheld,
            rest_lag: lag,
            rest_lag_from: Some(5),
            ..QfexModel::default()
        };
        let d = dropped(model, "take-unsettled").await;
        let stop = places::Stop::IocUnsettled;
        assert_take_stopped(&d, stop);
        let run = d.take(0);
        let taken = run.taken.as_ref().unwrap();
        assert_eq!(taken.ioc, Some(flatten::Ioc::Unsettled), "{lag}");
        assert!(taken.fees.is_empty());
        assert_eq!(taken.frame_order, Some(taker::FrameOrder::NoFill));
        assert_eq!(run.flatten, None, "no flatten");
        assert_eq!(d.iocs("SELL").len(), 0, "no flatten IOC");
        assert_eq!(run.stop, Some("IocUnsettled"));
        let stopped = d.ev.stopped.as_deref().unwrap();
        assert!(
            stopped.starts_with("IocUnsettled in the taker IOC session"),
            "{stopped}"
        );
        assert!(
            d.printed
                .contains(&format!("TAKEN on primary: {}", taker::UNSETTLED)),
            "{}",
            d.printed
        );
        // QFEX holds the fill: the model filled the taker IOC whole.
        let model = d.venue.model.lock().unwrap();
        let ioc = model.orders.last().unwrap();
        assert_eq!(
            (ioc.filled, ioc.state),
            (ioc.quantity, model::State::Filled)
        );
        drop(model);
        let section = take_section(&d.outputs().0, "primary");
        assert!(
            section.contains(&format!(
                "- How the taker IOC came back: {}",
                taker::UNSETTLED
            )),
            "{section}"
        );
    }
}

#[tokio::test]
async fn a_flatten_ioc_whose_fill_never_reaches_the_session_is_the_last_one_sent() {
    // X fills as a maker in drop session one; QFEX fills the flatten IOC whole, but its
    // fill_response never reaches the session, and REST lags the fill, so a read agrees with the
    // registry's stale position. The registry's position is not QFEX's, so no second flatten IOC
    // is built though a second attempt is left (on QFEX it would cross zero): the run stops
    // Flatten(Unsettled), not flat (decision 0148 items 5 and 9).
    let model = QfexModel {
        ioc_frame_order: model::IocFrameOrder::Withheld,
        rest_lag: 3,
        ..maker_filled(model::IocAnswer::Full)
    };
    let d = dropped_with(model, "flatten-unsettled", attempts("2")).await;
    let unsettled = places::Stop::Flatten(places::Flattening::Unsettled);
    let flatten = assert_flatten_stopped(&d, unsettled, 1);
    assert_eq!(flatten.iocs.len(), 1, "{flatten:?}");
    assert_eq!(flatten.iocs[0].came_back, Some(flatten::Ioc::Unsettled));
    assert_eq!(events(&d, "ioc Full"), 1);
    assert_eq!(d.iocs("SELL").len(), 1);
}

#[tokio::test]
async fn a_flatten_after_the_taker_fill_with_no_touch_to_price_from_stops_the_run() {
    // As the taker IOC goes out the MDS touch turns crossed, though QFEX still fills it: the
    // flatten has no touch to price an IOC from, so none is sent, and the run stops
    // Flatten(NoTouch), not flat, marked, in the taker IOC session, though the taker's fill is
    // expected (0151 item 9).
    let crossed = model::Touch {
        bid: Some(("100.20", "1.00")),
        ask: Some(("100.10", "2.00")),
        age_secs: 0,
    };
    let model = QfexModel {
        touch_on_place: Some((5, crossed)),
        ..QfexModel::default()
    };
    let d = dropped(model, "take-flatten-no-touch").await;
    let no_touch = places::Stop::Flatten(places::Flattening::NoTouch);
    assert_take_stopped(&d, no_touch);
    let run = d.take(0);
    assert_eq!(run.taken.as_ref().unwrap().ioc, Some(flatten::Ioc::Filled));
    let flatten = run.flatten.as_ref().expect("a flatten");
    assert_eq!(flatten.error.as_deref(), Some(market::CROSSED));
    assert_eq!(
        (flatten.exit, flatten.sent(), flatten.stop, flatten.flat),
        (Some(true), 0, Some(no_touch.name()), false)
    );
    assert_eq!(d.iocs("SELL").len(), 0, "no flatten IOC");
    assert!(
        d.ev.stopped
            .as_deref()
            .unwrap()
            .starts_with("Flatten(NoTouch) in the taker IOC session"),
        "{:?}",
        d.ev.stopped
    );
}

#[tokio::test]
async fn each_flatten_ioc_is_priced_from_a_touch_read_just_before_it() {
    // X fills as a maker; QFEX expires each flatten IOC, and as the first arrives the touch's bid
    // moves from 100.00 to 99.00: the second is priced from the touch read just before it,
    // within --flatten-cap-bps (10) of the new bid, not from the first's touch.
    let moved = model::Touch {
        bid: Some(("99.00", "1.00")),
        ask: Some(("99.10", "2.00")),
        age_secs: 0,
    };
    let model = QfexModel {
        touch_on_place: Some((1, moved)),
        ..maker_filled(model::IocAnswer::Expire)
    };
    let d = dropped_with(model, "flatten-fresh-touch", attempts("2")).await;
    let used_up = places::Stop::Flatten(places::Flattening::UsedUp);
    assert_flatten_stopped(&d, used_up, 2);
    let prices: Vec<Decimal> = (d.iocs("SELL").iter())
        .map(|ioc| Decimal::from_str(&ioc.price.to_string()).unwrap())
        .collect();
    assert_eq!(prices.len(), 2);
    let within = |px: Decimal, bid: i64| {
        let bid = Decimal::new(bid, 0);
        px <= bid && px >= bid * (Decimal::ONE - Decimal::new(10, 4))
    };
    assert!(within(prices[0], 100), "{prices:?}");
    assert!(within(prices[1], 99), "{prices:?}");
}

#[tokio::test]
async fn a_flatten_whose_client_id_is_not_minted_stops_refused() {
    // The run's mint refuses the flatten IOC's client id (a test's hook): no IOC is built, and
    // the run stops Flatten(Refused), not flat, the refusal recorded as the flatten's error.
    let hooks = probe_args::TestHooks {
        refuse_mint_in_flatten: true,
        ..probe_args::TestHooks::default()
    };
    let model = maker_filled(model::IocAnswer::Full);
    let d = hooked(model, "flatten-mint-refused", |a| a, hooks).await;
    let refused = places::Stop::Flatten(places::Flattening::Refused);
    let flatten = assert_flatten_stopped(&d, refused, 0);
    assert_eq!(
        flatten.error.as_deref(),
        Some("a test's refusal of the flatten's client id")
    );
    assert_eq!((flatten.exit, flatten.iocs.len()), (Some(true), 0));
    assert_eq!(d.count("add_order"), 1, "{:?}", d.what());
}

#[tokio::test]
async fn what_followed_a_session_the_run_ended_after_is_the_runs_end() {
    // The primary account's taker IOC is not placed, no touch to price it from, an error that is
    // no stop: the run ends on it, so what followed that session is the run's end, not the
    // subaccount's crossing session, which never ran, nor the run's planned end.
    let model = QfexModel {
        crossed_on_place: Some(4),
        ..QfexModel::default()
    };
    let d = dropped(model, "followed-take-error").await;
    assert_eq!(d.stage_two().crosses.len(), 1);
    let section = take_section(&d.outputs().0, "primary");
    assert!(
        section.contains(&format!("- What followed: {}", taker::RUN_ENDED)),
        "{section}"
    );
    assert!(!section.contains(taker::LAST_TAKE), "{section}");
    // The crossing post-only is not placed, likewise: no taker IOC session followed it.
    let model = QfexModel {
        crossed_on_place: Some(3),
        ..QfexModel::default()
    };
    let d = dropped(model, "followed-cross-error").await;
    assert!(d.stage_two().takes.is_empty());
    let section = cross_section(&d.outputs().0, "primary");
    assert!(
        section.contains(&format!("- What followed: {}", taker::RUN_ENDED)),
        "{section}"
    );
    assert!(!section.contains(taker::THEN_TAKE), "{section}");
}

// ----- FBC-23ch's review fixes of PR #272, carried to the taker IOC and the flatten -----

/// A touch near 400, where caps of [`THREE_MIN_USD`] admit no minimum size.
fn risen_touch() -> model::Touch {
    model::Touch {
        bid: Some(("400.00", "1.00")),
        ask: Some(("400.10", "2.00")),
        age_secs: 0,
    }
}

/// Both caps at [`THREE_MIN_USD`], three minimum sizes at the model's own touch.
fn three_min_caps(argv: Argv) -> Argv {
    let argv = set(argv, "--resting-cap-usd", THREE_MIN_USD);
    set(argv, "--inventory-cap-usd", THREE_MIN_USD)
}

#[tokio::test]
async fn a_touch_that_moves_past_the_caps_before_the_taker_ioc_sends_none() {
    // PR #272's thread 4605918689, carried to the taker IOC: both caps admit exactly three
    // minimum sizes at the preflight's touch near 100, where X, Y and the primary account's
    // crossing post-only are placed; at that account's taker IOC session's arm, the run's sixth,
    // the touch moves near 400, where they admit none, while the take registry's lot caps, fixed
    // at the preflight, still admit three. The taker IOC is not sent and never resized, and the
    // run stops, marked; the session cancels by venue id, nothing of ours resting, and ends by a
    // graceful close (decision 0113).
    let model = QfexModel {
        touch_moves_at_arm: Some((5, risen_touch())),
        ..QfexModel::default()
    };
    let d = dropped_with(model, "take-caps-moved", three_min_caps).await;
    assert_eq!(d.count("add_order"), 5, "{:?}", d.what());
    assert!(d.iocs("BUY").is_empty(), "{:?}", d.what());
    assert_take_stopped(&d, places::Stop::BeyondCaps);
    let said = d.ev.stopped.as_deref().unwrap();
    assert!(
        said.starts_with("BeyondCaps in the taker IOC session"),
        "{said}"
    );
    let run = d.take(0);
    assert!(run.started && !run.placed.submitted, "{run:?}");
    assert!(run.taken.is_none(), "{run:?}");
    assert!(!d.printed.contains("PLACE the taker IOC"), "{}", d.printed);
    let cancels = run.cancels.as_ref().unwrap();
    assert_eq!((cancels.sent, cancels.ended), (0, true), "{cancels:?}");
    assert_eq!(run.ended_by, Some(stage::GRACEFUL));
    assert_eq!(d.count("cancel_order"), 0);
}

#[tokio::test]
async fn a_flatten_ioc_the_caps_would_not_admit_at_its_touch_is_not_sent_and_stops_not_flat() {
    // PR #272's thread 4605918689, carried to the flatten IOC: both caps admit exactly three
    // minimum sizes at the preflight's touch near 100, where X is placed; as X arrives the touch
    // moves near 400, where they admit none, and X then fills whole as a maker. fbc-oms applies
    // the caps to reducing orders too (0013 rule 2), so the flatten IOC is not sent and never
    // resized (decision 0113): the run stops Flatten(BeyondCaps), marked, not flat, the position
    // the owner's, nothing of the flatten budget spent on it.
    let model = QfexModel {
        touch_on_place: Some((0, risen_touch())),
        ..maker_filled(model::IocAnswer::Full)
    };
    let d = dropped_with(model, "flatten-caps-moved", three_min_caps).await;
    let beyond = places::Stop::Flatten(places::Flattening::BeyondCaps);
    let flatten = assert_flatten_stopped(&d, beyond, 0);
    assert_eq!(flatten.exit, Some(true), "{flatten:?}");
    assert_eq!(flatten.iocs.len(), 1, "{flatten:?}");
    assert!(!flatten.iocs[0].sent, "{flatten:?}");
    assert_eq!(d.count("add_order"), 1, "{:?}", d.what());
    assert!(d.iocs("SELL").is_empty(), "{:?}", d.what());
    assert!(
        d.printed
            .contains("FLATTEN beyond the caps on primary: --resting-cap-usd or"),
        "{}",
        d.printed
    );
    assert!(beyond.marks() && beyond.says().contains("never resized"));
}

#[tokio::test]
async fn a_refusal_kept_whole_in_a_flatten_comparison_stops_it() {
    // PR #272's thread 4605918671, carried to the flatten: X fills as a maker, and the read
    // socket of the comparison after the flatten IOC, the first to authenticate after the IOC
    // came (so never a comparison's begun before the session read X's fill, FBC-23ch's third
    // review fix), is answered a RateLimited err echoing its authentication, which the journal
    // keeps whole (decision 0134 item 2), so the adapter reads no hold from it. The refusal its
    // frame shows is a stop on the read socket all the same (decision 0135 item 2, 0151 item 4),
    // which the flatten gives its session over the fill it answered, its comparison reconnecting
    // for no second read.
    let model = QfexModel {
        rate_limit: Some(model::Limited::ReadAuthEchoAfterIoc),
        ..maker_filled(model::IocAnswer::Full)
    };
    let d = dropped(model, "flatten-rate-whole").await;
    let refused = places::Stop::RateRefused {
        on: places::RefusedOn::ReadSocket,
    };
    assert_stopped(&d, refused);
    let flatten = d.drop_run().flatten.as_ref().expect("a flatten");
    assert_eq!(flatten.stop, Some(refused.name()));
    assert_eq!(flatten.iocs[0].came_back, Some(flatten::Ioc::Filled));
    assert_eq!(flatten.compared.len(), 1, "{flatten:?}");
    let compared = &flatten.compared[0];
    assert_eq!(compared.ended, compare::Ended::Refused, "{compared:?}");
    assert_eq!(compared.reads.len(), 1, "{compared:?}");
    let socket = (d.ev.read_sockets.iter()).find(|r| r.refusals.whole > 0);
    let refusals = socket.expect("the flatten comparison's socket").refusals;
    let shown = (refusals.errs, refusals.unread, refusals.whole);
    assert_eq!(shown, (0, 1, 1), "{refusals:?}");
}

#[tokio::test]
async fn a_press_while_the_taker_sessions_close_waits_for_room_is_taken_by_the_reads_after_it() {
    // PR #272's thread 4605918659, carried to the taker IOC session: QFEX acks the taker IOC and
    // leaves it resting, and as it reads the Stop's cancel of it, the run's first cancel_order,
    // the shared budget is held for 6 s, as a provider rate refusal echoing nothing would. The
    // cancel is answered and the IOC seen to end, but the comparison after the cancels reads
    // nothing within its wait, and the graceful close waits for room (0151 item 11). The owner
    // presses Ctrl-C as the market is disarmed, so the close is still waiting. The cancels have
    // already run, so the wait does not take the press: the session ends by its graceful close
    // once there is room, and the reads after it take the press, which stops the run, marked,
    // after every named account was read.
    let rate_user = user();
    let model = QfexModel {
        ioc: model::IocAnswer::Rest,
        hold_on_cancel: Some((shared_limiter(rate_user), Duration::from_secs(6))),
        ..QfexModel::default()
    };
    let at = "STOP cancels: Kill, then 1 cancels by venue id (decision 0132 item 7, 0148 item 8)";
    let d = pressed_at_as(
        model,
        "take-press-at-close",
        rate_user,
        wait_past_a_drop,
        at,
    )
    .await;
    assert_take_stopped(&d, places::Stop::Interrupted);
    let said = d.ev.stopped.as_deref().unwrap();
    assert!(
        said.starts_with("Interrupted during the reads after the taker IOC session"),
        "{said}"
    );
    let run = d.take(0);
    assert!(run.placed.submitted, "{run:?}");
    // One round of cancels, before the wait: none ran again for the press.
    let cancels = run.cancels.as_ref().unwrap();
    assert_eq!((cancels.sent, cancels.presses), (1, 0), "{cancels:?}");
    assert_eq!(d.count("cancel_order"), 1, "{:?}", d.what());
    let steps: Vec<_> = run.compared.iter().map(|c| (c.step, c.ended)).collect();
    assert_eq!(
        steps.last(),
        Some(&("the stop's cancels", compare::Ended::Unread))
    );
    // The close waited for room, so its frame was sent and the venue saw the client close it,
    // after the cancel.
    assert_eq!(run.ended_by, Some(stage::GRACEFUL));
    let conn = &d.take_conns()[0];
    let n: usize = conn.parse().unwrap();
    assert!(d.venue.trade.connections()[n].client_closed);
    let what = d.what();
    let at = |w: &str| what.iter().position(|e| e.starts_with(w)).unwrap();
    let cancel = at(&format!("trade {conn} cancel_order"));
    assert!(cancel < at(&format!("cod {conn} close")), "{what:?}");
    assert!(!run.resting && !run.counted_resting && run.read, "{run:?}");
}

#[tokio::test]
async fn a_taker_fill_that_reaches_the_session_after_its_steps_is_flattened_before_the_close() {
    // QFEX acks the primary account's taker IOC and leaves it open, so at its deadline it is
    // open with no fill, the registry flat, and nothing follows it. The Stop's cancel of it is
    // the run's first cancel_order, and QFEX fills the order whole as a maker as that cancel
    // arrives. The flatten IOC, a sell, is filled whole against the bid as QFEX fills an IOC by
    // default. Its fill is the taker IOC's, expected, and reaches the session only after its
    // steps ended: it is flattened then, while the session is still open, before its close (0151
    // item 9), and, once flat, it stops nothing. The subaccount's taker IOC rests too, and its
    // cancel fills nothing, so the run ends after it with no stop.
    let model = QfexModel {
        ioc: model::IocAnswer::Rest,
        ioc_sells: Some(model::IocAnswer::Full),
        fill_on_cancel: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "take-late-fill").await;
    assert_eq!((d.ev.stop, d.marker()), (None, None), "{}", d.printed);
    let run = d.take(0);
    assert_eq!(run.taken.as_ref().unwrap().ioc, Some(flatten::Ioc::Open));
    assert_eq!(run.stop, None, "{run:?}");
    // QFEX answers the cancel NO_SUCH_ORDER, its order filled first; the order was seen to end,
    // whether the wait met that answer (which holds the cancels) or the order's end first.
    let cancels = run.cancels.as_ref().expect("the session's Stop");
    assert_eq!((cancels.sent, cancels.ended), (1, true), "{cancels:?}");
    let flatten = run.flatten.as_ref().expect("a late flatten");
    assert_eq!(flatten.after, taker::AFTER_LATE_TAKER, "{}", d.printed);
    assert_flattened(flatten, 0);
    assert_eq!(flatten.sent(), 1);
    assert_eq!(d.iocs("SELL").len(), 1, "one flatten IOC");
    assert_eq!(d.take(1).flatten, None);
    // The flatten ran after the Stop's cancel and before the session's close.
    let conn = &d.take_conns()[0];
    let what = d.what();
    let at = |w: &str| what.iter().position(|e| e.starts_with(w)).unwrap();
    let cancel = at(&format!("trade {conn} cancel_order"));
    let sells: Vec<usize> = (what.iter().enumerate())
        .filter(|(_, e)| e.starts_with(&format!("trade {conn} add_order")))
        .map(|(n, _)| n)
        .collect();
    let flatten_ioc = *sells.last().unwrap();
    assert!(cancel < flatten_ioc, "{what:?}");
    assert!(flatten_ioc < at(&format!("cod {conn} close")), "{what:?}");
    let lines = [
        "STOP cancels: Kill, then 1 cancels by venue id (decision 0132 item 7, 0148 item 8)"
            .to_owned(),
        "FLATTENED on primary: flat".to_owned(),
        "READ after the taker IOC session: primary (take)".to_owned(),
    ];
    assert!(d.at(&lines[1]) < d.at(&lines[2]), "{}", d.printed);
    assert!(d.printed.contains(&lines[0]), "{}", d.printed);
    let after: Vec<&capture::AccountRead> = (d.ev.reads.iter())
        .filter(|r| r.before == refusals::AFTER_TAKE)
        .collect();
    assert!(after.iter().all(|r| r.flat), "{after:?}");
}

#[tokio::test]
async fn a_taker_fill_that_reaches_the_session_while_its_close_waits_for_room_is_flattened_first() {
    // As above, the primary account's taker IOC is acked and left open, and QFEX fills it whole as
    // a maker as the Stop's cancel of it arrives, the run's first: the FILLED order_response ends
    // it at once, the cancel answered NO_SUCH_ORDER after it, and the fill_response follows 4 s
    // later. As QFEX reads that cancel the shared budget is held for 6 s, so the graceful close
    // waits for room (0151 item 11). Until the fill_response arrives QFEX's count of the IOC's
    // fills is above the registry's, so nothing is flattened yet; it arrives as the close waits,
    // after the Stop's cancels and the disarm, and leaves the registry long: it is flattened once
    // there is room and before the close (0151 item 9), the market disarmed again after it, and,
    // the fill expected, the run stops nothing.
    let rate_user = user();
    let model = QfexModel {
        ioc: model::IocAnswer::Rest,
        ioc_sells: Some(model::IocAnswer::Full),
        hold_on_cancel: Some((shared_limiter(rate_user), Duration::from_secs(6))),
        fill_on_cancel: true,
        fill_on_cancel_after: Some(Duration::from_secs(4)),
        ..QfexModel::default()
    };
    let d = held_drop(model, "take-fill-at-close", rate_user, wait_past_a_drop).await;
    assert_eq!((d.ev.stop, d.marker()), (None, None), "{}", d.printed);
    let run = d.take(0);
    assert_eq!(run.taken.as_ref().unwrap().ioc, Some(flatten::Ioc::Open));
    assert_eq!(run.stop, None, "{run:?}");
    let flatten = run.flatten.as_ref().expect("a late flatten");
    assert_eq!(flatten.after, taker::AFTER_LATE_TAKER, "{}", d.printed);
    assert_flattened(flatten, 0);
    assert_eq!(d.iocs("SELL").len(), 1, "one flatten IOC");
    assert_eq!(run.ended_by, Some(stage::GRACEFUL));
    // The disarm, then the flatten, then the disarm again, all before the session's end.
    let lines = [
        "STOP cancels: Kill, then 1 cancels by venue id (decision 0132 item 7, 0148 item 8)",
        "FLATTEN on primary: the Stop's Kill lifted",
        "FLATTEN on primary: the owner's Flatten, the market in Exit",
        "FLATTENED on primary: flat",
        "FLATTEN on primary: the market disarmed again, as the Stop left it",
        "SESSION primary (the taker IOC session): arm outcome Accepted, ended by a graceful close",
        "READ after the taker IOC session: primary (take)",
    ];
    for pair in lines.windows(2) {
        assert!(d.at(pair[0]) < d.at(pair[1]), "{pair:?}: {}", d.printed);
    }
    let after: Vec<&capture::AccountRead> = (d.ev.reads.iter())
        .filter(|r| r.before == refusals::AFTER_TAKE)
        .collect();
    assert!(after.iter().all(|r| r.flat), "{after:?}");
}

/// A whole run on `model`, as [`dropped`] runs it, but `then` changes the model once the run first
/// prints a line holding `at`.
async fn changed_at(
    model: QfexModel,
    what: &str,
    at: &'static str,
    then: impl FnMut(&mut QfexModel) + 'static,
) -> Dropped {
    let (dir, removed) = temp_dir(what);
    let venue = Venue::start(model).await;
    let (mut opts, places) = parsed(&drop_argv(&dir, &venue)).unwrap();
    opts.rate_user = user();
    let env = environment();
    let printed = venue.printed();
    let lookup = |var: &str| env.get(var).cloned();
    let shared = Arc::clone(&venue.model);
    let mut then = then;
    let out: stage::Out = Rc::new(RefCell::new(PressAt {
        printed: printed.clone(),
        at,
        press: Some(Box::new(move || then(&mut shared.lock().unwrap()))),
    }));
    let hold: stage::Interrupt = Box::pin(std::future::pending());
    let none = interrupt::Presses::none();
    let ran = stage::run_places_until(&opts, &places, &lookup, out, hold, none).await;
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

/// The provider rate refusal `on` names, set on the model for the `ahead`th read of its kind from
/// now, from 0: a `get_user_orders` page on a read socket, or a `GET /user/positions`; with no
/// `on`, a position shown on the primary account that no fill made.
fn refuse_ahead(on: Option<places::RefusedOn>, ahead: usize) -> impl FnMut(&mut QfexModel) {
    move |m: &mut QfexModel| match on {
        Some(places::RefusedOn::ReadSocket) => {
            m.rate_limit = Some(model::Limited::ReadSocket(m.pages_after + ahead));
        }
        Some(places::RefusedOn::Rest) => {
            m.rate_limit = Some(model::Limited::Rest(m.rest_after + ahead));
        }
        Some(on) => unreachable!("no case refuses on {on:?}"),
        // The primary account, keyed by no subaccount id.
        None => m.moved.push(None),
    }
}

#[tokio::test]
async fn a_rate_refusal_in_the_reads_before_the_taker_ioc_session_stops_the_run_marked() {
    // FBC-23ch's second review fix to the reads before a session (0151 items 4 and 6), carried to
    // the reads before the taker IOC session: as the primary account's crossing session ends,
    // the model refuses the third get_user_orders page on a read socket from then, with a
    // RateLimited err, or the third GET /user/positions with HTTP 429. The reads after the
    // crossing session read the two named accounts first, so the refusal meets the primary
    // account's read before its taker IOC session. That provider rate refusal is the run's stop,
    // marked, so the next run needs --reconciled; no take registry is built and no taker IOC is
    // sent.
    let cases = [
        (
            places::RefusedOn::ReadSocket,
            "get_user_orders: QFEX refused the page read: RateLimited",
        ),
        (
            places::RefusedOn::Rest,
            "GET /user/positions: HTTP 429, a provider rate refusal",
        ),
    ];
    for (on, failed) in cases {
        let at = "SESSION primary (the crossing session): arm outcome";
        let d = changed_at(QfexModel::default(), "take-reads-refused", at, {
            refuse_ahead(Some(on), 2)
        })
        .await;
        let stop = places::Stop::RateRefused { on };
        let name = stop.name();
        assert_eq!(d.ev.stop, Some(name), "{}", d.printed);
        let said = d.ev.stopped.as_deref().unwrap();
        assert!(
            said.starts_with(&format!("{name} in the reads before the taker IOC session")),
            "{said}"
        );
        let marker = d.marker().unwrap_or_default();
        assert!(marker.starts_with(&format!("stop: {name}\n")), "{marker}");
        // The read failure the refusal made is kept among the errors.
        let errors = &d.ev.errors;
        assert!(errors.iter().any(|e| e.ends_with(failed)), "{errors:?}");
        // The reads after the crossing session were clean; the refusal met the next ones.
        let after = (d.ev.reads.iter()).filter(|r| r.before == refusals::AFTER_CROSS);
        assert_eq!(after.count(), 2, "{:?}", d.ev.reads);
        assert!(!d.printed.contains("REGISTRY take"), "{}", d.printed);
        let two = d.stage_two();
        assert_eq!((two.crosses.len(), two.takes.len()), (1, 0), "{two:?}");
        assert!(d.iocs("BUY").is_empty(), "no taker IOC");
        d.venue.assert_served();
    }
}

#[tokio::test]
async fn a_stop_the_comparison_after_the_taker_sessions_cancels_raises_ends_the_run_marked() {
    // FBC-23ch's second review fix to a session's ending order, carried to the taker IOC
    // session: QFEX expires the primary account's taker IOC unfilled, so no stop comes from its
    // steps and nothing is flattened; under Policy::Stage its Stop still cancels every order of
    // ours by venue id and compares the streams with the reads after them. As the IOC is taken,
    // before those cancels, which read nothing, the model refuses the next get_user_orders page
    // on a read socket with a RateLimited err, or the next GET /user/positions with HTTP 429, or
    // shows a position on the primary account that no fill made. That comparison's stop, a
    // provider rate refusal or ReadDisagrees, is the session's: the run stops, marked, and the
    // subaccount's crossing post-only is never placed.
    let cases = [
        Some(places::RefusedOn::ReadSocket),
        Some(places::RefusedOn::Rest),
        None,
    ];
    for refused in cases {
        let model = QfexModel {
            ioc: model::IocAnswer::Expire,
            ..QfexModel::default()
        };
        let at = "TAKEN on primary";
        let d = changed_at(model, "take-cancels-compared", at, refuse_ahead(refused, 0)).await;
        let stop = match refused {
            Some(on) => places::Stop::RateRefused { on },
            None => places::Stop::ReadDisagrees,
        };
        let name = stop.name();
        assert_take_stopped(&d, stop);
        let said = d.ev.stopped.as_deref().unwrap();
        assert!(
            said.starts_with(&format!("{name} in the taker IOC session")),
            "{said}"
        );
        let take = d.take(0);
        assert_eq!(
            take.taken.as_ref().unwrap().ioc,
            Some(flatten::Ioc::Expired)
        );
        assert!(take.cancels.is_some(), "{take:?}");
        let last = take.compared.last().unwrap();
        assert_eq!((last.step, last.stop), ("the stop's cancels", Some(stop)));
        assert_eq!(take.flatten, None);
        assert_eq!(venue_crossings(&d), 1);
    }
}

#[tokio::test]
async fn a_comparison_stop_after_the_stops_cancels_is_kept_before_a_late_flattens_own() {
    // The primary account's crossing post-only rests. QFEX answers the Stop's cancel of it
    // CANCELLED, so the cancels end, each accepted, but it had filled the order whole as a maker
    // as the cancel arrived, and that fill reaches the session after the answer: a fill of ours
    // after the Stop's cancels, flattened before the close (0151 item 9). As the session's steps
    // end, before those cancels, the model refuses the next get_user_orders page on a read
    // socket, which is the comparison's after the cancels. That comparison's provider rate
    // refusal is the session's stop, and it stays so through the flatten, whatever the flatten's
    // own: the run stops RateRefused in the crossing session, marked, and the subaccount's
    // crossing post-only is never placed.
    let model = QfexModel {
        fill_behind_cancel: Some(Duration::ZERO),
        ..resting_cross()
    };
    let on = Some(places::RefusedOn::ReadSocket);
    let at = "CROSSED on primary";
    let d = changed_at(model, "cross-refused-late-fill", at, refuse_ahead(on, 0)).await;
    let stop = places::Stop::RateRefused {
        on: places::RefusedOn::ReadSocket,
    };
    let name = stop.name();
    assert_eq!(d.ev.stop, Some(name), "{}", d.printed);
    let said = d.ev.stopped.as_deref().unwrap();
    assert!(
        said.starts_with(&format!("{name} in the crossing session")),
        "{said}"
    );
    let marker = d.marker().unwrap_or_default();
    assert!(marker.starts_with(&format!("stop: {name}\n")), "{marker}");
    let run = d.cross(0);
    let last = run.compared.last().unwrap();
    assert_eq!((last.step, last.stop), ("the stop's cancels", Some(stop)));
    let cancels = run.cancels.as_ref().unwrap();
    assert_eq!(
        (cancels.sent, cancels.held, cancels.ended),
        (1, false, true)
    );
    // The flatten ran after the comparison, its own outcome kept in its record: flat, or, when
    // the refusal's hold on the shared budget kept its IOC unsent, Flatten(Refused), not flat.
    let flatten = run.flatten.as_ref().expect("a late flatten");
    assert_eq!(flatten.after, flatten::AFTER_LATE_FILL);
    assert_eq!(flatten.exit, Some(true));
    let refused = places::Stop::Flatten(places::Flattening::Refused);
    let own = (flatten.stop, flatten.flat);
    assert!(
        own == (None, true) || own == (Some(refused.name()), false),
        "{flatten:?}"
    );
    assert_eq!(run.stop, Some(name), "{run:?}");
    assert_eq!(d.stage_two().crosses.len(), 1);
    assert_eq!(venue_crossings(&d), 1);
}

/// How many crossing post-only orders the model saw.
fn venue_crossings(d: &Dropped) -> usize {
    d.venue.model.lock().unwrap().crossings
}

// ----- FBC-77r8's review of PR #273, carried into the taker IOC and the flatten (FBC-ou9g) -------

/// Whether the run printed the line the reads after the taker IOC session give for an order or a
/// TWAP not ours.
fn others_after_take(d: &Dropped) -> bool {
    let line = format!("READ after the taker IOC session: {}", taker::OTHERS_TAKE);
    d.printed.contains(&line)
}

#[tokio::test]
async fn an_order_not_ours_the_reads_after_a_taker_ioc_session_list_stops_read_disagrees() {
    // The taker IOC fills and is flattened, flat; but once its connection ended, the primary
    // account holds an order no run placed, which only the reads after the session see. It is
    // what the streams did not show, so the run stops ReadDisagrees, marked, said as met after
    // the taker IOC session, not LeftResting with a hand cancel of an order of ours (FBC-77r8's
    // review, carried).
    let model = QfexModel {
        after_take_end: vec![model::AfterHistoric::Foreign],
        ..QfexModel::default()
    };
    let d = dropped(model, "take-after-foreign").await;
    assert_take_stopped(&d, places::Stop::ReadDisagrees);
    let stopped = d.ev.stopped.clone().unwrap();
    assert!(
        stopped.starts_with(&format!("ReadDisagrees {}", stage::TAKE_OTHERS)),
        "{stopped}"
    );
    let run = d.take(0);
    assert_eq!(run.others, Some(true), "{run:?}");
    assert_eq!(run.not_flat_after, Some(false), "{run:?}");
    assert!(!run.resting && !run.counted_resting && run.read, "{run:?}");
    assert!(others_after_take(&d), "{}", d.printed);
    assert!(!d.printed.contains("RESTING "), "{}", d.printed);
    let (report, json) = d.outputs();
    let section = take_section(&report, "primary");
    assert!(section.contains(taker::OTHERS_TAKE), "{section}");
    assert!(
        section.contains("- No other order of ours (decision 0148 item 6): none: it may be placed"),
        "{section}"
    );
    let take = &json["stage_two"]["takes"][0];
    assert_eq!(take["others_after"], true);
    assert_eq!(take["other_order_checked"], true);
    assert!(take["hand_cancel"].is_null());
}

#[tokio::test]
async fn a_twap_the_reads_after_a_taker_ioc_session_list_stops_read_disagrees() {
    // As above, with a TWAP: no order rests, and still the streams did not show it, so the run
    // stops ReadDisagrees, marked, rather than going on to the next account's crossing.
    let model = QfexModel {
        after_take_end: vec![model::AfterHistoric::Twap],
        ..QfexModel::default()
    };
    let d = dropped(model, "take-after-twap").await;
    assert_take_stopped(&d, places::Stop::ReadDisagrees);
    let run = d.take(0);
    assert_eq!(run.others, Some(true), "{run:?}");
    assert!(!run.resting && run.read, "{run:?}");
    assert!(others_after_take(&d), "{}", d.printed);
}

#[tokio::test]
async fn a_position_the_reads_after_a_taker_ioc_session_show_is_said_the_owners() {
    // Once the taker IOC session's connection ended, the primary account shows a position no
    // order of ours made and an order not ours: a position not flat comes first, so the run
    // stops NotFlat, the order not ours still recorded, and the report and the evidence say the
    // position is the owner's (0151 item 9; FBC-77r8's review, carried).
    let model = QfexModel {
        after_take_end: vec![
            model::AfterHistoric::Position,
            model::AfterHistoric::Foreign,
        ],
        ..QfexModel::default()
    };
    let d = dropped(model, "take-after-position").await;
    assert_take_stopped(&d, places::Stop::NotFlat);
    let run = d.take(0);
    assert_eq!(run.others, Some(true), "{run:?}");
    assert_eq!(run.not_flat_after, Some(true), "{run:?}");
    let (report, json) = d.outputs();
    let section = take_section(&report, "primary");
    assert!(
        section.contains(&format!(
            "- A position not flat in the reads after the session: {}",
            stage_two::CROSS_POSITION
        )),
        "{section}"
    );
    let take = &json["stage_two"]["takes"][0];
    assert_eq!(take["not_flat_after"], true);
    assert_eq!(take["owners_position"], stage_two::CROSS_POSITION);
}

#[tokio::test]
async fn a_flatten_ioc_left_resting_after_a_crossing_session_is_read_as_ours() {
    // The crossing post-only fills as a taker, and QFEX acks the flatten IOC and leaves it
    // resting, answering every cancel NO_SUCH_ORDER: the reads after the crossing session list
    // it. It is ours, a flatten IOC the session sent, so it is an order of ours left resting,
    // reported for the hand cancel, and not an order the streams did not show (FBC-77r8's
    // review, carried: the session's orders are its own and its flatten IOCs).
    let model = QfexModel {
        alo_cross: Some(model::AloCross::Fills),
        ioc: model::IocAnswer::Rest,
        cancel_misses: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "cross-flatten-rests").await;
    assert_eq!(d.iocs("SELL").len(), 1, "{:?}", d.what());
    assert_eq!(d.stage_two().crosses.len(), 1);
    let run = d.cross(0);
    assert!(run.resting && run.read, "{run:?}");
    assert_eq!(run.others, Some(false), "{run:?}");
    assert!(!others_after_cross(&d), "{}", d.printed);
    assert!(
        d.printed
            .contains(&format!("RESTING {}", stage_two::HAND_CANCEL_CROSS)),
        "{}",
        d.printed
    );
    let json = d.outputs().1;
    assert_eq!(
        json["stage_two"]["crossings"][0]["hand_cancel"],
        stage_two::HAND_CANCEL_CROSS
    );
}

#[tokio::test]
async fn a_taker_session_that_ends_before_its_check_says_the_other_order_not_checked() {
    // QFEX rejects the taker IOC session's arm (the run's sixth): the session ends before its
    // check for another order of ours, so the report says that check was not made, and the
    // evidence says so too, rather than "none" from the session's shape (FBC-77r8's review,
    // carried).
    let model = QfexModel {
        arm_err_on: Some(5),
        ..QfexModel::default()
    };
    let d = dropped(model, "take-not-checked").await;
    assert_take_stopped(&d, places::Stop::ConnectionEnded);
    let run = d.take(0);
    assert!(!run.checked && run.blocked.is_none(), "{run:?}");
    let (report, json) = d.outputs();
    let section = take_section(&report, "primary");
    assert!(
        section.contains(
            "- No other order of ours (decision 0148 item 6): not checked: the session ended \
             before it"
        ),
        "{section}"
    );
    assert_eq!(json["stage_two"]["takes"][0]["other_order_checked"], false);
    // The crossing session before it reached its check.
    assert_eq!(
        json["stage_two"]["crossings"][0]["other_order_checked"],
        true
    );
}

#[tokio::test]
async fn the_hand_actions_are_not_known_when_a_named_accounts_last_read_failed() {
    // As a_named_account_not_read_after_a_crossing_session_stops_the_run_marked: the primary
    // account's read after the subaccount's crossing session fails. Its last read was not made,
    // so what the run leaves the owner by hand is not known, and the report says so rather than
    // judging that account by an earlier read (FBC-77r8's review, carried).
    let model = QfexModel {
        alo_cross: Some(model::AloCross::Cancelled),
        silent_after_cross: Some(1),
        ..QfexModel::default()
    };
    let d = dropped_with(model, "hand-not-known", |argv| {
        set(argv, "--read-wait-ms", "3000")
    })
    .await;
    assert_eq!(d.ev.stop, Some(places::Stop::LeftResting.name()));
    assert_eq!(d.ev.last_unread, ["primary"], "{}", d.printed);
    let (report, json) = d.outputs();
    let declared = declared_section(&report);
    assert!(
        declared.contains(&format!(
            "- The hand actions this run leaves the owner: {}",
            taker::HAND_NOT_KNOWN
        )),
        "{declared}"
    );
    assert_eq!(json["declared"]["hand_actions_left"]["known"], false);
}

#[tokio::test]
async fn a_flatten_ioc_left_resting_after_drop_session_one_is_read_as_ours() {
    // X fills whole as a maker in drop session one, and QFEX acks the flatten IOC and leaves it
    // open with no fill, answering every cancel NO_SUCH_ORDER; the close cancels nothing. The
    // second attempt is kept back by it, so the run stops LeftResting, not flat. The reads after
    // drop session one list the IOC: it is ours, a flatten IOC the session sent, so it is an
    // order of ours left resting, asked of the owner by hand, and not an order the streams did
    // not show (the restack's review: the drop sessions' flatten IOCs are read as ours).
    let model = QfexModel {
        cancel_misses: true,
        ..maker_filled(model::IocAnswer::Rest)
    };
    let d = dropped_with(model, "drop-flatten-rests", attempts("2")).await;
    let flatten = assert_flatten_stopped(&d, places::Stop::LeftResting, 1);
    assert_eq!(flatten.iocs[0].came_back, Some(flatten::Ioc::Open));
    assert!(flatten.blocked.is_some(), "{flatten:?}");
    let run = d.drop_run();
    assert!(!run.x_resting && run.x_read, "{run:?}");
    assert_eq!(run.hand_cancel, Some(places::HAND_CANCEL_ONE_ORDERS));
    let after = (d.ev.reads.iter())
        .find(|r| r.before == refusals::AFTER_DROP_ONE && r.label == "primary (drop)")
        .unwrap();
    assert_eq!((after.page_orders, after.page_unknown), (1, 0), "{after:?}");
    assert!(
        d.printed
            .contains(&format!("RESTING {}", places::HAND_CANCEL_ONE_ORDERS)),
        "{}",
        d.printed
    );
    let (report, json) = d.outputs();
    assert!(
        report.contains(&format!(
            "- An order of ours (X or a flatten IOC) resting after drop session one: {}",
            places::HAND_CANCEL_ONE_ORDERS
        )),
        "{report}"
    );
    assert!(report.contains(flatten::Ioc::Open.says()), "{report}");
    let one = &json["drop_tests"][0]["session_one"];
    assert_eq!(one["hand_cancel"], places::HAND_CANCEL_ONE_ORDERS);
    assert_eq!(
        one["flatten"]["iocs"][0]["came_back"],
        flatten::Ioc::Open.says()
    );
}

#[tokio::test]
async fn a_named_accounts_read_made_after_one_that_failed_makes_the_hand_actions_known() {
    // The subaccount's read after the primary account's drop session one goes unanswered (the
    // third authentication after X: the comparison's, then the primary account's read after the
    // session) and fails at --read-wait-ms. The read before drop session two reads it again,
    // cleanly, and every later read of it is made: its last read was made, so the hand actions
    // the run leaves the owner are known, and none is asked (the restack's review: a read made
    // clears an earlier failure, Run::read_one).
    let model = QfexModel {
        silent_auth: Some(2),
        ..QfexModel::default()
    };
    let d = dropped_with(model, "hand-known-again", |argv| {
        set(argv, "--read-wait-ms", "3000")
    })
    .await;
    let failed: Vec<&String> = (d.ev.errors.iter())
        .filter(|e| e.starts_with("subaccount could not be read"))
        .collect();
    assert_eq!(failed.len(), 1, "{:?}", d.ev.errors);
    let after_one = (d.ev.reads.iter())
        .filter(|r| r.before == refusals::AFTER_DROP_ONE && r.label == "subaccount")
        .count();
    assert_eq!(after_one, 0, "{}", d.printed);
    assert!(d.ev.last_unread.is_empty(), "{:?}", d.ev.last_unread);
    let (report, json) = d.outputs();
    let declared = declared_section(&report);
    assert!(
        declared.contains(
            "- The hand actions this run leaves the owner: none: the last read of every named \
             account showed no order of ours and a flat position"
        ),
        "{declared}"
    );
    assert_eq!(json["declared"]["hand_actions_left"]["known"], true);
}

#[tokio::test]
async fn a_flatten_ioc_left_resting_after_drop_session_two_is_read_as_ours() {
    // Y fills whole as a maker in drop session two. The flatten cancels X, which ends, and QFEX
    // acks its IOC and leaves it open with no fill; the Stop's cancel of the IOC, the run's
    // second cancel_order, is answered NO_SUCH_ORDER, and the close cancels nothing. The reads
    // after drop session two list the IOC: it is ours, a flatten IOC the session sent, so it is
    // an order of ours left resting, and not an order the streams did not show (the restack's
    // review: the drop sessions' flatten IOCs are read as ours).
    let model = QfexModel {
        on_ack: Some((1, model::OnAck::MakerFill)),
        ioc: model::IocAnswer::Rest,
        cancel_misses_from: Some(1),
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-two-flatten-rests").await;
    let two = d.two();
    let flatten = two.flatten.as_ref().expect("a flatten");
    assert_eq!(flatten.sent(), 1, "{flatten:?}");
    assert_eq!(flatten.iocs[0].came_back, Some(flatten::Ioc::Open));
    assert_eq!(d.count("cancel_order"), 2, "{:?}", d.what());
    assert!(two.resting && two.read, "{two:?}");
    let after = (d.ev.reads.iter())
        .find(|r| r.before == refusals::AFTER_DROP_TWO && r.label == "primary (drop)")
        .unwrap();
    assert_eq!((after.page_orders, after.page_unknown), (1, 0), "{after:?}");
    assert!(
        d.printed
            .contains(&format!("RESTING {}", places::HAND_CANCEL_ORDERS)),
        "{}",
        d.printed
    );
    let json = d.outputs().1;
    assert_eq!(
        json["drop_tests"][0]["session_two"]["hand_cancel"],
        places::HAND_CANCEL_ORDERS
    );
}

// ----- FBC-gisx: X's account not read after drop session one, X counted resting -----

/// Asserts that `d` stopped [`places::Stop::LeftResting`] on X's account not read after drop
/// session one, the read's step `failed`: X counted resting and reported for the owner's hand
/// cancel, said as not read and never as seen resting, the marker written, nothing more placed
/// and nothing cancelled (0151 item 2), the report and evidence passing the privacy check.
fn assert_x_counted_resting(d: &Dropped, failed: places::AccountUnread) {
    let name = places::Stop::LeftResting.name();
    assert_eq!(d.ev.stop, Some(name), "{}", d.printed);
    let marker = d.marker().unwrap_or_default();
    assert!(marker.starts_with(&format!("stop: {name}\n")), "{marker}");
    assert!(
        d.printed
            .contains("MARKER written (LeftResting): the next place-stage run"),
        "{}",
        d.printed
    );
    // The whole stop line: what X was counted, never that it was seen resting.
    let stopped = d.ev.stopped.as_deref().unwrap_or_default();
    let line = format!(
        "LeftResting after drop session one, X's account not read after it ({}), so X counts as \
         resting (decision 0005): {}; no further session runs",
        failed.name(),
        places::COUNTED_RESTING
    );
    assert_eq!(stopped, line);
    assert!(
        !stopped.contains(places::Stop::LeftResting.says()),
        "{stopped}"
    );
    assert!(
        d.printed.contains(&format!("STOPPED {line}")),
        "{}",
        d.printed
    );
    // The RESTING line says the account was not read, never that X was seen resting.
    let unread = format!("RESTING {}", places::HAND_CANCEL_UNREAD);
    assert!(d.printed.contains(&unread), "{}", d.printed);
    assert!(!d.printed.contains(places::HAND_CANCEL), "{}", d.printed);
    let run = d.drop_run();
    assert_eq!(
        run.x_unseen,
        Some(places::Unseen::Account(failed)),
        "{run:?}"
    );
    assert!(!run.x_read && !run.x_resting, "{run:?}");
    assert!(run.x.submitted && run.x.acked, "{run:?}");
    assert_eq!(run.close_cancelled_x, None, "{run:?}");
    assert_eq!(run.stop, Some(name));
    // X's account was not read after the session, the subaccount's was.
    let after: Vec<&str> = (d.ev.reads.iter())
        .filter(|r| r.before == refusals::AFTER_DROP_ONE)
        .map(|r| r.label.as_str())
        .collect();
    assert_eq!(after, ["subaccount"], "{}", d.printed);
    // Nothing more is placed and nothing is cancelled: no read before drop session two, no
    // second session, X still resting on the model's book.
    assert!(reads_before_two(d).is_empty(), "{}", d.printed);
    assert!(run.two.is_none());
    assert_eq!(d.count("subscribe"), 3);
    assert_eq!((d.count("add_order"), d.count("cancel_order")), (1, 0));
    let model = d.venue.model.lock().unwrap();
    assert_eq!(model.orders[0].state, model::State::Open);
    drop(model);
    let (report, json) = d.outputs();
    assert!(
        report.contains(&format!(
            "- X resting when the run ended: {}",
            places::HAND_CANCEL_UNREAD
        )),
        "{report}"
    );
    let unjudged = format!(
        "- X left unjudged after the session (decision 0005; the owner's ruling under 0151 item \
         6): yes: X's account could not be read after the session ({}: {}): X was not seen \
         resting or ended, so it counts as resting",
        failed.name(),
        failed.says()
    );
    assert!(report.contains(&unjudged), "{report}");
    assert!(!report.contains(places::HAND_CANCEL), "{report}");
    assert!(
        report.contains("**The place stages' stop:** LeftResting."),
        "{report}"
    );
    let one = &json["drop_tests"][0]["session_one"];
    let shown = format!("Account({})", failed.name());
    assert_eq!(one["x_unseen"], shown.as_str());
    assert_eq!(one["hand_cancel"], places::HAND_CANCEL_UNREAD);
    assert_eq!(one["x_read_after"], false);
    assert_eq!(json["stop"], name);
    assert_report_and_evidence_pass_the_privacy_check(&d.dir.join("evidence"));
    assert_no_secret(&d.dir.join("evidence"));
    d.venue.assert_served();
}

#[tokio::test]
async fn x_s_account_not_read_after_drop_session_one_counts_x_resting_and_is_marked() {
    // FBC-gisx, as the owner ruled under 0151 item 6: X rests after drop session one's graceful
    // close, and the read of its account after the session fails with no provider rate
    // refusal, its GET /user/positions answered HTTP 500 (the second after X's place: the
    // comparison's is the first). X counts as resting, as decision 0005 counts an order whose
    // end nothing showed: the run stops LeftResting, marked, reported for the owner's hand
    // cancel as not read, never as seen resting. A second run in the same lease directory is
    // refused without --reconciled.
    let model = QfexModel {
        rest_fails: vec![1],
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-x-account-unread").await;
    assert_x_counted_resting(&d, places::AccountUnread::Positions);
    let failed = "primary (drop) could not be read: GET /user/positions";
    assert!(
        d.ev.errors.iter().any(|e| e.starts_with(failed)),
        "{:?}",
        d.ev.errors
    );
    // The next run, on the same lease directory and without --reconciled, is refused before
    // anything connects, naming the stop.
    let venue = Venue::start(QfexModel::default()).await;
    let (next, _removed) = temp_dir("drop-x-account-unread-next");
    let leases = leases_of(&d.dir);
    let asked = set(argv(&next, &venue), "--lease-dir", leases.to_str().unwrap());
    let (ran, printed) = place_run(&venue, &asked, &environment()).await;
    let err = ran.unwrap_err();
    assert!(
        err.starts_with(
            "refused, nothing sent: a stop marker from an earlier place-stage run (its stop: \
             LeftResting) is in the lease directory or production's default"
        ),
        "{err}"
    );
    assert!(
        err.contains("give --reconciled <hand-cancel|hand-close|both|none>"),
        "{err}"
    );
    assert_eq!(printed.lines().count(), 1, "{printed}");
    assert!(venue.log().is_empty() && !next.join("evidence").exists());
    assert!(leases.join(places::MARKER).exists());
}

#[tokio::test]
async fn x_s_account_read_socket_failing_after_drop_session_one_counts_x_resting() {
    // FBC-gisx's socket-side failures of the same read: X's account's read socket, the first
    // to ask for a page once drop session one's connection ended, is closed by the server
    // before it answers, or answered a frame that is no page. Neither is a provider rate
    // refusal, so X counts as resting as above, the read's step named by its variant.
    let cases = [
        (model::PageFault::Close, places::AccountUnread::Socket),
        (model::PageFault::Undecodable, places::AccountUnread::Page),
    ];
    for (fault, failed) in cases {
        let model = QfexModel {
            page_after_session: Some(fault),
            ..QfexModel::default()
        };
        let d = dropped(model, "drop-x-socket-unread").await;
        assert_x_counted_resting(&d, failed);
        // The read socket's refusals are recorded, and it met none.
        let socket = (d.ev.read_sockets.iter())
            .find(|r| r.before == refusals::AFTER_DROP_ONE && r.label == "primary (drop)")
            .expect("X's account's read socket");
        assert_eq!(socket.refusals.shown(), 0, "{:?}", socket.refusals);
        assert_eq!(socket.refusals.limiter, 0, "{:?}", socket.refusals);
    }
}

#[tokio::test]
async fn a_rate_refusal_on_x_s_account_after_drop_session_one_stays_its_own_stop() {
    // FBC-gisx's boundary: the read of X's account after drop session one refused by a provider
    // rate refusal, a page's RateLimited err or GET /user/positions answered 429, stays the stop
    // it is (RateRefused, marked; decision 0148 item 8): X is not counted resting by the ruling,
    // so the report keeps saying it was not known.
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
        let d = dropped(model, "drop-x-account-refused").await;
        let stop = places::Stop::RateRefused { on };
        assert_eq!(d.ev.stop, Some(stop.name()), "{}", d.printed);
        let said = d.ev.stopped.as_deref().unwrap();
        assert!(
            said.starts_with(&format!(
                "{} in the reads after drop session one",
                stop.name()
            )),
            "{said}"
        );
        let run = d.drop_run();
        assert_eq!(run.x_unseen, None, "{run:?}");
        assert!(!run.x_read, "{run:?}");
        assert!(!d.printed.contains("RESTING "), "{}", d.printed);
        let (report, json) = d.outputs();
        assert!(
            report.contains(
                "- X resting when the run ended: not known: the account was not read after the \
                 session"
            ),
            "{report}"
        );
        assert!(
            report.contains(
                "- X left unjudged after the session (decision 0005; the owner's ruling under \
                 0151 item 6): no"
            ),
            "{report}"
        );
        assert_eq!(
            json["drop_tests"][0]["session_one"]["x_unseen"],
            Value::Null
        );
        assert_eq!(
            json["drop_tests"][0]["session_one"]["hand_cancel"],
            Value::Null
        );
    }
}

#[tokio::test]
async fn x_s_history_not_read_whole_after_drop_session_one_is_said_so() {
    // The ruling's other case, FBC-77r8's stop: X's history after drop session one not read
    // whole with no provider rate refusal, its GET /user/historic-orders read answered HTTP 500,
    // or every get_user_trades page answered an InvalidRequest err. The run stops LeftResting,
    // marked, as before, and the report and the stop's line say X's history was not read whole,
    // never that X was seen resting: with the graceful close cancelling X, X is not listed, so
    // no hand cancel is asked for.
    let historic = QfexModel {
        historic_fails: true,
        cod_on_close: true,
        ..QfexModel::default()
    };
    let trades = QfexModel {
        trades_err: true,
        cod_on_close: true,
        ..QfexModel::default()
    };
    for model in [historic, trades] {
        let d = dropped(model, "drop-x-history-unread").await;
        let name = places::Stop::LeftResting.name();
        assert_eq!(d.ev.stop, Some(name), "{}", d.printed);
        let marker = d.marker().unwrap_or_default();
        assert!(marker.starts_with(&format!("stop: {name}\n")), "{marker}");
        let said = d.ev.stopped.as_deref().unwrap();
        let line = format!(
            "LeftResting reading X after drop session one: its get_user_trades or GET \
             /user/historic-orders pages not all read, X left unjudged (decision 0005): {}; no \
             further session runs",
            places::LEFT_UNJUDGED
        );
        assert_eq!(said, line);
        assert!(!said.contains(places::Stop::LeftResting.says()), "{said}");
        let run = d.drop_run();
        assert_eq!(run.x_unseen, Some(places::Unseen::History), "{run:?}");
        assert!(run.x_read && !run.x_resting, "{run:?}");
        assert!(!d.printed.contains("RESTING "), "{}", d.printed);
        let (report, json) = d.outputs();
        assert!(
            report.contains(
                "- X left unjudged after the session (decision 0005; the owner's ruling under \
                 0151 item 6): yes: X's get_user_trades or GET /user/historic-orders pages after \
                 the session were not all read: neither a fill of X nor its end is shown by them"
            ),
            "{report}"
        );
        assert!(
            report.contains("- X resting when the run ended: no\n"),
            "{report}"
        );
        assert!(report.contains(&line), "{report}");
        assert!(!report.contains(places::HAND_CANCEL), "{report}");
        let one = &json["drop_tests"][0]["session_one"];
        assert_eq!(one["x_unseen"], "History");
        assert_eq!(one["hand_cancel"], Value::Null);
        assert_eq!(json["stopped"], line.as_str());
        assert_report_and_evidence_pass_the_privacy_check(&d.dir.join("evidence"));
    }
}

#[tokio::test]
async fn x_s_account_not_read_after_a_failed_drop_session_two_counts_x_alone_resting() {
    // The survey's one other case (FBC-gisx): drop session two fails with no stop, Y not priced
    // once its arm was accepted, so no verdict judges X and Y, and the read of X's account after
    // it fails with no provider rate refusal, its GET /user/positions answered HTTP 500 (the
    // seventh after X's place: the comparison's, two after drop session one, two before drop
    // session two, then drop session two's resync's). X still rests, unseen, and Y was never
    // sent: X alone counts as resting, so the run stops LeftResting, marked, after the
    // session's failure, reported for the owner's hand cancel as not read, Y named as never
    // sent.
    let model = QfexModel {
        rest_fails: vec![6],
        ..QfexModel::default()
    };
    let d = dropped_hooked(model, "drop-two-account-unread", |t| t.y_unpriced = true).await;
    let name = places::Stop::LeftResting.name();
    assert_eq!(d.ev.stop, Some(name), "{}", d.printed);
    let marker = d.marker().unwrap_or_default();
    assert!(marker.starts_with(&format!("stop: {name}\n")), "{marker}");
    let stopped = d.ev.stopped.as_deref().unwrap();
    let line = format!(
        "a test's Y could not be priced; LeftResting after drop session two, X's account not read \
         after it (Positions), so X counts as resting (decision 0005): {}; no further session \
         runs",
        places::COUNTED_RESTING
    );
    assert_eq!(stopped, line);
    let two = d.two();
    assert_eq!(two.error.as_deref(), Some("a test's Y could not be priced"));
    let counted = (places::AccountUnread::Positions, places::Counted::X);
    assert_eq!(two.unread, Some(counted), "{two:?}");
    assert!(!two.read && !two.resting && !two.y.submitted, "{two:?}");
    assert!(d.drop_run().verdict.is_none());
    let unread = format!("RESTING {}", places::HAND_CANCEL_X_UNREAD);
    assert!(d.printed.contains(&unread), "{}", d.printed);
    for other in [
        places::HAND_CANCEL_ORDERS,
        places::HAND_CANCEL_ORDERS_UNREAD,
        places::HAND_CANCEL_Y_UNREAD,
    ] {
        assert!(!d.printed.contains(other), "{}", d.printed);
    }
    assert_eq!((d.count("add_order"), d.count("cancel_order")), (1, 0));
    let model = d.venue.model.lock().unwrap();
    assert_eq!(model.orders[0].state, model::State::Open);
    drop(model);
    let (report, json) = d.outputs();
    let said = format!(
        "- An order resting on the account when the run ended: {}; the read that failed: \
         Positions: {}",
        places::HAND_CANCEL_X_UNREAD,
        places::AccountUnread::Positions.says()
    );
    assert!(report.contains(&said), "{report}");
    assert!(
        !report.contains(places::HAND_CANCEL_ORDERS_UNREAD),
        "{report}"
    );
    let two = &json["drop_tests"][0]["session_two"];
    assert_eq!(two["read_after_failed"], "Positions");
    assert_eq!(two["counted_resting"], "X");
    assert_eq!(two["hand_cancel"], places::HAND_CANCEL_X_UNREAD);
    assert_report_and_evidence_pass_the_privacy_check(&d.dir.join("evidence"));
    assert_no_secret(&d.dir.join("evidence"));
    d.venue.assert_served();
}

#[test]
fn each_account_unread_and_unseen_is_named_and_said_with_no_value() {
    use places::{AccountUnread, Unseen};
    let all = [
        AccountUnread::Positions,
        AccountUnread::Socket,
        AccountUnread::Page,
    ];
    let names: Vec<&str> = all.iter().map(|a| a.name()).collect();
    assert_eq!(names, ["Positions", "Socket", "Page"]);
    for failed in all {
        assert!(!failed.says().is_empty());
        let unseen = Unseen::Account(failed);
        assert_eq!(unseen.name(), format!("Account({})", failed.name()));
        assert!(unseen.says().contains(failed.says()), "{}", unseen.says());
    }
    assert_eq!(Unseen::History.name(), "History");
    // A read that never sent GET /user/positions is said so too (a reader, a request or the
    // shared limiter's room failing first).
    assert_eq!(
        AccountUnread::Positions.says(),
        "GET /user/positions was not sent, gave no answer, or gave one that is no success or \
         does not decode"
    );
    // The stop's own words never say an order was seen resting.
    for says in [places::COUNTED_RESTING, places::LEFT_UNJUDGED] {
        assert!(says.contains("not seen resting"), "{says}");
        assert_ne!(says, places::Stop::LeftResting.says());
    }
}

#[test]
fn a_stop_is_said_in_its_callers_words_only_when_it_is_the_runs() {
    // FBC-gisx: what an order counted resting or left unjudged is said as replaces Stop::says
    // when that stop is the run's; an earlier stop is said as itself.
    use places::Stop;
    let resting = Stop::LeftResting;
    let counted = Some(places::COUNTED_RESTING);
    assert_eq!(
        stage::stop_said(resting, resting, "after it", counted),
        format!(
            "LeftResting after it: {}; no further session runs",
            places::COUNTED_RESTING
        )
    );
    assert_eq!(
        stage::stop_said(resting, resting, "after it", None),
        format!(
            "LeftResting after it: {}; no further session runs",
            resting.says()
        )
    );
    assert_eq!(
        stage::stop_said(Stop::NotFlat, resting, "after it", counted),
        format!(
            "NotFlat after it: {}; no further session runs",
            Stop::NotFlat.says()
        )
    );
}

#[test]
fn the_orders_counted_resting_after_drop_session_two_name_only_those_sent() {
    // FBC-gisx: X unless the streams showed its end, Y once it was submitted, each said by name
    // with its own hand cancel, none when neither counts.
    use places::Counted;
    assert_eq!(Counted::of(true, true, false), Some(Counted::Both));
    assert_eq!(Counted::of(true, false, false), Some(Counted::X));
    assert_eq!(Counted::of(false, true, false), Some(Counted::Y));
    assert_eq!(Counted::of(false, false, false), None);
    let all = [Counted::X, Counted::Y, Counted::Both];
    let said: Vec<(&str, &str, &str)> = (all.iter())
        .map(|c| (c.name(), c.count(), c.hand_cancel()))
        .collect();
    assert_eq!(
        said,
        [
            ("X", "X counts", places::HAND_CANCEL_X_UNREAD),
            ("Y", "Y counts", places::HAND_CANCEL_Y_UNREAD),
            (
                "X and Y",
                "X and Y count",
                places::HAND_CANCEL_ORDERS_UNREAD
            ),
        ]
    );
    assert!(places::HAND_CANCEL_X_UNREAD.contains("Y was never sent"));
    assert!(places::HAND_CANCEL_Y_UNREAD.contains("the streams showed X end"));
    for c in all {
        let counts = format!("so {} as resting", c.count());
        assert!(c.hand_cancel().contains(&counts), "{c:?}");
        assert!(
            c.hand_cancel().contains("--reconciled hand-cancel"),
            "{c:?}"
        );
    }
}

#[tokio::test]
async fn another_named_account_unread_after_drop_session_one_does_not_count_x_resting() {
    // FBC-gisx: the ruling covers X's own account alone. The subaccount's read after drop
    // session one fails with no provider rate refusal, its GET /user/positions answered HTTP 500
    // (the third after X's place: the comparison's, then X's account's), while X's account
    // reads clean, X listed. X is not counted resting by the ruling, so no hand cancel is asked
    // for it as not read, and the run goes on to the read before drop session two.
    let model = QfexModel {
        rest_fails: vec![2],
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-other-account-unread").await;
    let failed = "subaccount could not be read: GET /user/positions";
    assert!(
        d.ev.errors.iter().any(|e| e.starts_with(failed)),
        "{:?}",
        d.ev.errors
    );
    let run = d.drop_run();
    assert_eq!(run.x_unseen, None, "{run:?}");
    assert!(run.x_read && run.x_resting, "{run:?}");
    assert!(
        !d.printed.contains(places::HAND_CANCEL_UNREAD),
        "{}",
        d.printed
    );
    assert!(!reads_before_two(&d).is_empty(), "{}", d.printed);
    assert!(run.two.is_some(), "{run:?}");
    let (_, json) = d.outputs();
    assert_eq!(
        json["drop_tests"][0]["session_one"]["x_unseen"],
        Value::Null
    );
}

#[tokio::test]
async fn x_s_account_unread_after_its_cancel_was_seen_to_end_does_not_count_x_resting() {
    // FBC-gisx: the owner's Ctrl-C in drop session one, X's cancel by venue id answered
    // CANCELLED, so the streams showed X end; then X's account's read socket after the session
    // is closed by the server before it answers. Decision 0005 counts an order resting until
    // something shows its end, and something did: X is not counted resting, the stop stays
    // Interrupted, marked, and no hand cancel is asked for.
    let model = QfexModel {
        page_after_session: Some(model::PageFault::Close),
        ..QfexModel::default()
    };
    let d = pressed_drop(model, "drop-ctrl-c-unread", |a| a, &[" add_order"]).await;
    let name = places::Stop::Interrupted.name();
    assert_eq!(d.ev.stop, Some(name), "{}", d.printed);
    let marker = d.marker().unwrap_or_default();
    assert!(marker.starts_with(&format!("stop: {name}\n")), "{marker}");
    let run = d.drop_run();
    let cancels = run.cancels.as_ref().unwrap();
    assert!(cancels.ended && !cancels.held, "{cancels:?}");
    assert!(!run.x_read && !run.x_resting, "{run:?}");
    assert_eq!(run.x_unseen, None, "{run:?}");
    let failed = "primary (drop) could not be read:";
    assert!(
        d.ev.errors.iter().any(|e| e.starts_with(failed)),
        "{:?}",
        d.ev.errors
    );
    assert!(!d.printed.contains("RESTING "), "{}", d.printed);
    let (report, json) = d.outputs();
    assert!(
        report.contains(
            "- X left unjudged after the session (decision 0005; the owner's ruling under 0151 \
             item 6): no\n"
        ),
        "{report}"
    );
    assert!(!report.contains(places::HAND_CANCEL_UNREAD), "{report}");
    let one = &json["drop_tests"][0]["session_one"];
    assert_eq!(one["x_unseen"], Value::Null);
    assert_eq!(one["hand_cancel"], Value::Null);
}

#[tokio::test]
async fn x_s_account_unread_after_a_drop_session_one_that_placed_no_x_counts_nothing_resting() {
    // FBC-gisx: drop session one fails with no stop before X is placed (X not priced), and X's
    // account's read socket after it is closed by the server before it answers. No order of
    // ours was sent, so none is counted resting: the run ends on the session's failure, with no
    // stop, no marker and no hand cancel.
    let model = QfexModel {
        page_after_session: Some(model::PageFault::Close),
        ..QfexModel::default()
    };
    let d = dropped_hooked(model, "drop-unplaced-unread", |t| t.x_unpriced = true).await;
    assert_eq!(d.ev.stop, None, "{}", d.printed);
    assert!(d.marker().is_none());
    assert_eq!(
        d.ev.stopped.as_deref(),
        Some("a test's X could not be priced")
    );
    let run = d.drop_run();
    assert!(!run.x.submitted, "{run:?}");
    assert_eq!(run.x_unseen, None, "{run:?}");
    let failed = "primary (drop) could not be read:";
    assert!(
        d.ev.errors.iter().any(|e| e.starts_with(failed)),
        "{:?}",
        d.ev.errors
    );
    assert!(!d.printed.contains("RESTING "), "{}", d.printed);
    assert_eq!(d.count("add_order"), 0);
    let (_, json) = d.outputs();
    let one = &json["drop_tests"][0]["session_one"];
    assert_eq!(one["x_unseen"], Value::Null);
    assert_eq!(one["hand_cancel"], Value::Null);
}

#[tokio::test]
async fn x_s_account_unread_after_a_drop_session_two_that_reached_its_verdict_counts_nothing() {
    // FBC-gisx: drop session two runs its course to a verdict that passed, the reads after the
    // drop showing X and Y end, then the read of X's account after the session fails with no
    // provider rate refusal, its GET /user/positions answered HTTP 500 (the first after the
    // historic-orders read after the drop). The verdict showed X's and Y's ends, so neither is
    // counted resting: no hand cancel, no stop, and the run goes on to the crossing session's
    // reads, which read every account again.
    let model = QfexModel {
        rest_fails_after_drop_reads: true,
        ..QfexModel::default()
    };
    let d = dropped_in(model, "drop-two-verdict-unread", &primary_only(), |a| a).await;
    assert!(d.verdict().passed, "{:?}", d.verdict());
    let failed = "primary (drop) could not be read: GET /user/positions";
    assert!(
        d.ev.errors.iter().any(|e| e.starts_with(failed)),
        "{:?}",
        d.ev.errors
    );
    let two = d.two();
    assert_eq!(two.unread, None, "{two:?}");
    assert!(!two.read && !two.resting, "{two:?}");
    assert!(!d.printed.contains("RESTING "), "{}", d.printed);
    assert_eq!(d.ev.stop, None, "{}", d.printed);
    assert!(d.marker().is_none());
    d.at("READ before the crossing session: primary (cross)");
    let (_, json) = d.outputs();
    let two = &json["drop_tests"][0]["session_two"];
    assert_eq!(two["read_after_failed"], Value::Null);
    assert_eq!(two["counted_resting"], Value::Null);
    assert_eq!(two["hand_cancel"], Value::Null);
}

// ----- FBC-gisx's restack onto FBC-77r8's review fixes (PR #273) -------------------------------

#[test]
fn a_stage_two_left_resting_no_read_listed_is_said_counted_and_never_seen_resting() {
    // FBC-gisx's review: a stage-two session's LeftResting that no read listing an order of ours
    // made is said as counted resting, by the registry or on an account not read, never as an
    // order seen resting (the owner's ruling under 0151 item 6). Every other stop is said as
    // itself.
    use places::Stop;
    let says = stage::stage_counted_says;
    assert_eq!(
        says(Stop::LeftResting, false, true),
        Some(places::COUNTED_BY_REGISTRY)
    );
    assert_eq!(
        says(Stop::LeftResting, false, false),
        Some(places::COUNTED_UNREAD)
    );
    assert_eq!(says(Stop::LeftResting, true, true), None);
    assert_eq!(says(Stop::LeftResting, true, false), None);
    for stop in [Stop::NotFlat, Stop::ReadDisagrees, Stop::Interrupted] {
        assert_eq!(says(stop, false, false), None, "{}", stop.name());
    }
    for said in [places::COUNTED_BY_REGISTRY, places::COUNTED_UNREAD] {
        assert!(said.contains("counted resting, not seen resting"), "{said}");
        assert!(!said.contains(Stop::LeftResting.says()), "{said}");
    }
}

#[tokio::test]
async fn a_crossing_order_counted_resting_by_its_registry_alone_is_said_counted_not_seen() {
    // FBC-gisx's review: QFEX answers the crossing post-only with a status the adapter does not
    // map, so its place ends Unknown at its deadline and the cross registry counts it resting
    // (decision 0005), while no read after the session lists it. The run stops LeftResting,
    // marked, its line saying the order is counted resting by the registry, never that a read
    // saw it resting.
    let model = QfexModel {
        alo_cross: Some(model::AloCross::Unmapped),
        ..QfexModel::default()
    };
    let d = dropped(model, "cross-counted-said").await;
    assert_cross_stopped(&d, places::Stop::LeftResting);
    let run = d.cross(0);
    assert!(run.counted_resting && !run.resting, "{run:?}");
    let line = format!(
        "LeftResting after the crossing session: {}; no further session runs",
        places::COUNTED_BY_REGISTRY
    );
    assert_eq!(d.ev.stopped.as_deref(), Some(line.as_str()));
    let (report, json) = d.outputs();
    assert!(report.contains(&line), "{report}");
    assert_eq!(json["stopped"], line.as_str());
    // Its second review: the report, the evidence and the RESTING line give the hand cancel as
    // counted resting by the registry, never as an order seen resting.
    let counted = stage_two::HAND_CANCEL_CROSS_COUNTED;
    let resting = format!("RESTING {counted}");
    assert!(d.printed.contains(&resting), "{}", d.printed);
    let said = format!("- An order resting on the account when the session ended: {counted}\n");
    let section = cross_section(&report, "primary");
    assert!(section.contains(&said), "{section}");
    assert!(
        !report.contains("an order of ours still resting after a session"),
        "{report}"
    );
    assert_eq!(json["stage_two"]["crossings"][0]["hand_cancel"], counted);
}

#[tokio::test]
async fn a_named_account_not_read_after_a_crossing_session_is_said_counted_not_seen() {
    // FBC-gisx's review: each crossing post-only is refused CANCELLED, so the cross registry
    // counts nothing resting, and the read of the primary account after the subaccount's
    // crossing session fails at --read-wait-ms. The run stops LeftResting, marked, its line
    // saying an order of ours may rest there counted resting, never that a read saw one.
    let model = QfexModel {
        alo_cross: Some(model::AloCross::Cancelled),
        silent_after_cross: Some(1),
        ..QfexModel::default()
    };
    let d = dropped_with(model, "cross-unread-said", |argv| {
        set(argv, "--read-wait-ms", "3000")
    })
    .await;
    assert_eq!(d.ev.stop, Some(places::Stop::LeftResting.name()));
    let last = d.cross(1);
    assert!(
        last.read && !last.resting && !last.counted_resting,
        "{last:?}"
    );
    let line = format!(
        "LeftResting after the crossing session, a named account not read after it: {}; no \
         further session runs",
        places::COUNTED_UNREAD
    );
    assert_eq!(d.ev.stopped.as_deref(), Some(line.as_str()));
    let (report, _) = d.outputs();
    assert!(report.contains(&line), "{report}");
    assert!(
        !report.contains(places::Stop::LeftResting.says()),
        "{report}"
    );
}

#[tokio::test]
async fn a_twap_another_account_lists_outranks_x_counted_resting_after_drop_session_one() {
    // FBC-77r8's review, carried: an order or a TWAP not ours outranks an order of ours resting.
    // X rests after drop session one, X's account cannot be read after it (its GET
    // /user/positions answered HTTP 500, the second after X's place), and the subaccount's read
    // lists a TWAP no run placed. What the streams did not show is the stop, ReadDisagrees,
    // marked (decision 0148 item 8), X still counted resting for the owner's hand cancel as not
    // read (FBC-gisx).
    let model = QfexModel {
        rest_fails: vec![1],
        twap_elsewhere_after_close: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-x-unread-twap-elsewhere").await;
    assert_marked(&d, places::Stop::ReadDisagrees);
    let stopped = d.ev.stopped.as_deref().unwrap_or_default();
    assert!(
        stopped.starts_with("ReadDisagrees after drop session one"),
        "{stopped}"
    );
    let run = d.drop_run();
    let unseen = places::Unseen::Account(places::AccountUnread::Positions);
    assert_eq!(run.x_unseen, Some(unseen), "{run:?}");
    assert!(run.two.is_none(), "{run:?}");
    assert!(
        d.printed
            .contains(&format!("RESTING {}", places::HAND_CANCEL_UNREAD)),
        "{}",
        d.printed
    );
    let (_, json) = d.outputs();
    assert_eq!(
        json["drop_tests"][0]["session_one"]["hand_cancel"],
        places::HAND_CANCEL_UNREAD
    );
}

#[tokio::test]
async fn x_shown_ended_by_a_historic_orders_row_is_not_counted_resting_with_its_account_unread() {
    // FBC-77r8's review, carried: what a page showed survives a read that did not. The graceful
    // close of drop session one cancels X (cancel-on-disconnect on the close), and X's
    // historic-orders read, read whole, names X CANCELLED, while the read of X's account fails
    // (its GET /user/positions answered HTTP 500, the second after X's place). X's end was shown,
    // so X is not counted resting (FBC-gisx counts only an order whose end nothing showed): no
    // stop after drop session one, and drop session two follows once every account reads clean.
    let model = QfexModel {
        rest_fails: vec![1],
        cod_on_close: true,
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-x-unread-shown-ended").await;
    let run = d.drop_run();
    let after = run.after.as_ref().expect("X was read");
    assert_eq!(
        after.terminal_status.as_deref(),
        Some("CANCELLED"),
        "{after:?}"
    );
    assert_eq!(run.x_unseen, None, "{run:?}");
    assert_eq!(run.hand_cancel, None, "{run:?}");
    assert!(run.two.is_some(), "{}", d.printed);
    assert!(
        !d.printed.contains("LeftResting after drop session one"),
        "{}",
        d.printed
    );
}

#[tokio::test]
async fn x_s_status_a_historic_orders_page_shows_leaves_x_judged_though_a_later_page_fails() {
    // FBC-77r8's review, carried: on pages of one, the graceful close of drop session one
    // cancels X, and X's historic-orders read names X CANCELLED on its first page, then fails at
    // its second, which repeats it, while X's trades are read whole with no row. X's end and the
    // absence of a fill of it are both shown, so X is not left unjudged (FBC-gisx's History):
    // no stop after drop session one, and drop session two follows.
    let model = QfexModel {
        cod_on_close: true,
        ..QfexModel::default()
    };
    let d = dropped_with(model, "drop-x-historic-page-shown", |argv| {
        set(argv, "--page-size", "1")
    })
    .await;
    let run = d.drop_run();
    let after = run.after.as_ref().expect("X was read");
    assert!(after.trades_read && after.error.is_some(), "{after:?}");
    assert_eq!(
        after.terminal_status.as_deref(),
        Some("CANCELLED"),
        "{after:?}"
    );
    assert_eq!(run.x_unseen, None, "{run:?}");
    assert!(run.two.is_some(), "{}", d.printed);
    assert!(
        !d.printed.contains("reading X after drop session one"),
        "{}",
        d.printed
    );
}

#[tokio::test]
async fn a_flatten_ioc_counts_resting_beside_y_when_the_account_is_unread_after_drop_session_two() {
    // The restack's review names a flatten IOC as ours, carried into FBC-gisx's count: Y fills
    // whole as a maker in drop session two, the flatten cancels X, which ends, and QFEX acks its
    // IOC and leaves it open; the Stop's cancel of the IOC is answered NO_SUCH_ORDER. The read of
    // the account after the session then fails, its read socket's page undecodable. Y, sent, and
    // the IOC, whose end nothing showed, count as resting, and X, whose end the streams showed,
    // does not: the hand cancel names a flatten IOC and Y alone, never that one was seen
    // resting, and the report says the streams showed X end (FBC-gisx's review).
    let model = QfexModel {
        on_ack: Some((1, model::OnAck::MakerFill)),
        ioc: model::IocAnswer::Rest,
        cancel_misses_from: Some(1),
        page_after_ioc_end: Some(model::PageFault::Undecodable),
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-two-flatten-unread").await;
    let two = d.two();
    let flatten = two.flatten.as_ref().expect("a flatten");
    assert_eq!(flatten.iocs[0].came_back, Some(flatten::Ioc::Open));
    let with = places::Counted::WithFlatten { x: false, y: true };
    let counted = (places::AccountUnread::Page, with);
    assert_eq!(two.unread, Some(counted), "{two:?}");
    assert!(!two.read && !two.resting && two.x_ended, "{two:?}");
    let resting = format!("RESTING {}", places::HAND_CANCEL_FLATTEN_UNREAD);
    assert!(d.printed.contains(&resting), "{}", d.printed);
    let (report, json) = d.outputs();
    let x_at_end = format!("{X_AT_END}{}", places::X_ENDED_AFTER_TWO);
    assert!(report.contains(&x_at_end), "{report}");
    let two = &json["drop_tests"][0]["session_two"];
    assert_eq!(two["counted_resting"], "Y and a flatten IOC");
    assert_eq!(two["hand_cancel"], places::HAND_CANCEL_FLATTEN_UNREAD);
}

#[test]
fn a_flatten_ioc_whose_end_nothing_showed_is_counted_beside_x_and_y() {
    use places::Counted;
    // FBC-gisx's review: each order counted beside the IOC is named alone, X only while the
    // streams did not show its end and Y only once sent.
    let said = [
        (
            true,
            true,
            "X, Y and a flatten IOC",
            "X, Y and a flatten IOC count",
        ),
        (
            true,
            false,
            "X and a flatten IOC",
            "X and a flatten IOC count",
        ),
        (
            false,
            true,
            "Y and a flatten IOC",
            "Y and a flatten IOC count",
        ),
        (false, false, "a flatten IOC", "a flatten IOC counts"),
    ];
    for (x, y, name, count) in said {
        let c = Counted::of(x, y, true).unwrap();
        assert_eq!(c, Counted::WithFlatten { x, y });
        assert_eq!((c.name(), c.count(), c.counts_x()), (name, count, x));
        assert_eq!(c.hand_cancel(), places::HAND_CANCEL_FLATTEN_UNREAD);
    }
    let hand = places::HAND_CANCEL_FLATTEN_UNREAD;
    assert!(hand.contains("--reconciled hand-cancel"), "{hand}");
    assert!(!hand.contains("X, Y"), "{hand}");
    assert!(!hand.contains(places::Stop::LeftResting.says()), "{hand}");
}

// ----- FBC-gisx's restack, its second review ---------------------------------------------------

#[tokio::test]
async fn a_crossing_session_s_own_account_not_read_after_it_asks_for_the_hand_cancel_as_counted() {
    // The crossing post-only is refused CANCELLED, so the cross registry counts nothing resting,
    // and once it arrived the primary account's authentications go unanswered, so the read of
    // its own account after its crossing session fails at --read-wait-ms. The run stops
    // LeftResting, marked, and the report, the evidence and the RESTING line ask for the hand
    // cancel of an order of ours that may rest there, counted resting, never seen resting.
    let model = QfexModel {
        alo_cross: Some(model::AloCross::Cancelled),
        silent_after_cross: Some(0),
        ..QfexModel::default()
    };
    let d = dropped_with(model, "cross-own-unread", |argv| {
        set(argv, "--read-wait-ms", "3000")
    })
    .await;
    assert_eq!(d.ev.stop, Some(places::Stop::LeftResting.name()));
    assert!(d.marker().is_some());
    let run = d.cross(0);
    assert!(
        run.placed.submitted && !run.read && !run.resting && !run.counted_resting,
        "{run:?}"
    );
    let line = format!(
        "LeftResting after the crossing session, a named account not read after it: {}; no \
         further session runs",
        places::COUNTED_UNREAD
    );
    assert_eq!(d.ev.stopped.as_deref(), Some(line.as_str()));
    let unread = stage_two::HAND_CANCEL_STAGE_UNREAD;
    assert!(
        d.printed.contains(&format!("RESTING {unread}")),
        "{}",
        d.printed
    );
    let (report, json) = d.outputs();
    let said = format!("- An order resting on the account when the session ended: {unread}\n");
    let section = cross_section(&report, "primary");
    assert!(section.contains(&said), "{section}");
    assert!(
        !report.contains("an order of ours still resting after a session"),
        "{report}"
    );
    assert_eq!(json["stage_two"]["crossings"][0]["hand_cancel"], unread);
}

#[tokio::test]
async fn a_taker_ioc_counted_resting_by_its_registry_alone_is_said_counted_not_seen() {
    // QFEX never answers the taker IOC: at its deadline it is Unknown, never resent and counted
    // resting by the take registry, which no read after the session lists. The run stops
    // LeftResting, marked, its line, the report, the evidence and the RESTING line saying the
    // order is counted resting by the registry, never that a read saw it resting.
    let model = QfexModel {
        ioc: model::IocAnswer::Silent,
        ..QfexModel::default()
    };
    let d = dropped(model, "take-counted-said").await;
    assert_take_stopped(&d, places::Stop::LeftResting);
    let run = d.take(0);
    assert!(run.counted_resting && !run.resting && run.read, "{run:?}");
    let line = format!(
        "LeftResting after the taker IOC session: {}; no further session runs",
        places::COUNTED_BY_REGISTRY
    );
    assert_eq!(d.ev.stopped.as_deref(), Some(line.as_str()));
    let counted = taker::HAND_CANCEL_TAKE_COUNTED;
    assert!(
        d.printed.contains(&format!("RESTING {counted}")),
        "{}",
        d.printed
    );
    let (report, json) = d.outputs();
    assert!(report.contains(&line), "{report}");
    let said = format!("- An order resting on the account when the session ended: {counted}\n");
    let section = take_section(&report, "primary");
    assert!(section.contains(&said), "{section}");
    assert!(
        !report.contains("an order of ours still resting after a session"),
        "{report}"
    );
    assert_eq!(json["stage_two"]["takes"][0]["hand_cancel"], counted);
}

#[tokio::test]
async fn an_order_a_registry_holds_that_keeps_a_crossing_order_back_is_said_counted_not_seen() {
    // An order of ours, never sent, held in the primary account's cross registry before its
    // crossing session's check (a test's hook): it keeps the crossing post-only back, a stop,
    // LeftResting, said as counted resting by a registry of the run, never as an order a read
    // saw resting (decision 0148 item 6; the owner's ruling under 0151 item 6).
    let (dir, removed) = temp_dir("cross-blocked-said");
    let venue = Venue::start(QfexModel::default()).await;
    let (mut opts, mut places) = parsed(&drop_argv(&dir, &venue)).unwrap();
    places.test.other_order = Some(0);
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
    assert!(d.cross(0).blocked.is_some(), "{:?}", d.cross(0));
    let line = format!(
        "LeftResting in the crossing session: {}; no further session runs",
        places::COUNTED_BLOCKED
    );
    assert_eq!(d.ev.stopped.as_deref(), Some(line.as_str()));
    let (report, _) = d.outputs();
    assert!(report.contains(&line), "{report}");
    assert!(
        !report.contains(places::Stop::LeftResting.says()),
        "{report}"
    );
}

#[tokio::test]
async fn an_account_the_read_before_drop_session_two_cannot_read_is_said_counted_not_seen() {
    // QFEX cancels X itself after its ACK, its CANCELLED naming our client id, so the streams
    // show X end in drop session one and the reads after it do not list X. Then the primary's
    // GET /user/positions in the read before drop session two answers HTTP 500 (the fourth after
    // X's place). The run stops LeftResting, marked, said as an account not read, counted
    // resting, never as an order seen resting.
    let model = QfexModel {
        venue_cancel: true,
        rest_fails: vec![3],
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-before-two-unread-said").await;
    assert_marked(&d, places::Stop::LeftResting);
    let run = d.drop_run();
    assert!(run.x_read && !run.x_resting && run.two.is_none(), "{run:?}");
    let stopped = d.ev.stopped.as_deref().unwrap_or_default();
    assert!(
        stopped.starts_with("primary (drop) could not be read:"),
        "{stopped}"
    );
    let line = format!(
        "; LeftResting in the read before drop session two: {}; no further session runs",
        places::COUNTED_UNREAD
    );
    assert!(stopped.ends_with(&line), "{stopped}");
    assert!(
        !stopped.contains(places::Stop::LeftResting.says()),
        "{stopped}"
    );
}

#[tokio::test]
async fn the_read_before_the_drop_not_made_is_said_counted_not_seen() {
    // The read before drop session two's drop fails (its GET /user/positions answered HTTP 500,
    // the seventh after X's place), so the drop is not made and the session closes gracefully,
    // cancelling nothing. The run stops LeftResting, marked, said as X and Y counted resting
    // until a read shows their end, never as orders seen resting.
    let model = QfexModel {
        rest_fails: vec![6],
        ..QfexModel::default()
    };
    let d = dropped(model, "drop-two-before-drop-said").await;
    assert_marked(&d, places::Stop::LeftResting);
    assert!(d.two().before_drop_unread, "{:?}", d.two());
    let line = format!(
        "LeftResting in drop session two: {}; no further session runs",
        places::UNREAD_BEFORE_DROP
    );
    assert_eq!(d.ev.stopped.as_deref(), Some(line.as_str()));
    let (report, _) = d.outputs();
    assert!(report.contains(&line), "{report}");
}

#[tokio::test]
async fn x_s_account_unread_after_a_failed_drop_session_one_that_saw_x_end_is_not_marked() {
    // The base's fix 5 narrowed by the owner's ruling (FBC-gisx): QFEX answers X CANCELLED as it
    // comes, so the registry shows X's end, drop session one then fails with no stop, and the
    // read of X's account after it fails (the first GET /user/positions after X's place,
    // answered HTTP 500). No order of ours is left whose end nothing showed: no stop and no
    // marker, the run ending on the session's failure, and the report saying the streams showed
    // X end and the account was not read.
    let model = QfexModel {
        cancel_place: true,
        rest_fails: vec![0],
        ..QfexModel::default()
    };
    let d = dropped_hooked(model, "drop-one-failed-ended-unread", |t| {
        t.fail_after_place = Some(0);
    })
    .await;
    assert_eq!(d.ev.stop, None, "{}", d.printed);
    assert!(d.marker().is_none());
    let error = "a test's session failed after X's answer";
    assert_eq!(d.ev.stopped.as_deref(), Some(error));
    let run = d.drop_run();
    assert!(run.x.submitted && run.x_ended, "{run:?}");
    assert!(!run.x_read && run.x_unseen.is_none(), "{run:?}");
    assert!(!d.printed.contains("RESTING "), "{}", d.printed);
    let (report, json) = d.outputs();
    let said = format!(
        "- X resting when the run ended: {}\n",
        places::X_ENDED_AFTER_ONE
    );
    assert!(report.contains(&said), "{report}");
    assert_eq!(
        json["drop_tests"][0]["session_one"]["hand_cancel"],
        Value::Null
    );
}

#[tokio::test]
async fn x_s_account_unread_after_a_failed_drop_session_two_with_nothing_counted_is_not_marked() {
    // The base's fix 5 narrowed by the owner's ruling (FBC-gisx): drop session two's arm cancels
    // X, reported, so the registry shows X's end; Y is then not priced, so the session fails with
    // no stop and Y is never sent; and the read of X's account after it fails (the seventh GET
    // /user/positions after X's place). Nothing of ours is counted resting: no stop and no
    // marker, the run ending on the session's failure, and the report saying the account was
    // not read and the streams showed X end.
    let model = QfexModel {
        arm_cancels_open: model::ArmCancels::Reported,
        rest_fails: vec![6],
        ..QfexModel::default()
    };
    let d = dropped_hooked(model, "drop-two-failed-nothing-counted", |t| {
        t.y_unpriced = true;
    })
    .await;
    assert_eq!(d.ev.stop, None, "{}", d.printed);
    assert!(d.marker().is_none());
    assert_eq!(
        d.ev.stopped.as_deref(),
        Some("a test's Y could not be priced")
    );
    let two = d.two();
    assert!(two.x_ended && !two.y.submitted, "{two:?}");
    assert!(!two.read && !two.x_read && two.unread.is_none(), "{two:?}");
    assert!(!d.printed.contains("RESTING "), "{}", d.printed);
    let (report, json) = d.outputs();
    let resting = "- An order resting on the account when the run ended: not known: the account \
                   was not read after the session; read it on QFEX's site before the next run\n";
    assert!(report.contains(resting), "{report}");
    let x_at_end = format!("{X_AT_END}{}\n", places::X_ENDED_AFTER_TWO);
    assert!(report.contains(&x_at_end), "{report}");
    let two = &json["drop_tests"][0]["session_two"];
    assert_eq!(two["counted_resting"], Value::Null);
    assert_eq!(two["hand_cancel"], Value::Null);
}

// ----- FBC-gisx's restack onto FBC-su9i's review fixes (PR #274) -------------------------------

/// As [`pressed_at_as`], under the shared limiter's user, with `hooked` applied to the run's test
/// hooks: the owner presses Ctrl-C once, as the line holding `at` is printed.
async fn pressed_at_hooked(
    model: QfexModel,
    what: &str,
    hooked: impl FnOnce(&mut probe_args::TestHooks),
    at: &'static str,
) -> Dropped {
    let (dir, removed) = temp_dir(what);
    let venue = Venue::start(model).await;
    let (mut opts, mut places) = parsed(&drop_argv(&dir, &venue)).unwrap();
    hooked(&mut places.test);
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
    Dropped {
        dir,
        _removed: removed,
        venue,
        ev,
        printed,
    }
}

#[tokio::test]
async fn a_press_as_the_last_read_after_drop_session_one_ends_outranks_x_counted_resting() {
    // FBC-su9i's review fix to the reads after a session, on FBC-gisx's own path: X rests after
    // drop session one's graceful close, and the read of its account after the session fails
    // with no provider rate refusal (its GET /user/positions answered HTTP 500), so X counts as
    // resting. The owner presses Ctrl-C as the last named account's read after the session
    // prints its line, in the poll that ends that read. The press is taken, and it is the run's
    // stop, Interrupted, marked, ahead of X counted resting, which came after it in the run's
    // order: the RESTING line still asks for X's hand cancel as not read, and nothing more is
    // placed or cancelled.
    let model = QfexModel {
        rest_fails: vec![1],
        ..QfexModel::default()
    };
    let at = "READ after drop session one: subaccount";
    let d = pressed_at_hooked(model, "drop-one-press-x-unread", |_| {}, at).await;
    let name = places::Stop::Interrupted.name();
    assert_eq!(d.ev.stop, Some(name), "{}", d.printed);
    let marker = d.marker().unwrap_or_default();
    assert!(marker.starts_with(&format!("stop: {name}\n")), "{marker}");
    let line = format!(
        "Interrupted during the reads after drop session one: {}; no further session runs",
        places::Stop::Interrupted.says()
    );
    assert_eq!(d.ev.stopped.as_deref(), Some(line.as_str()));
    assert!(!line.contains(places::COUNTED_RESTING));
    let unread = format!("RESTING {}", places::HAND_CANCEL_UNREAD);
    assert!(d.printed.contains(&unread), "{}", d.printed);
    let run = d.drop_run();
    let failed = places::AccountUnread::Positions;
    assert_eq!(
        run.x_unseen,
        Some(places::Unseen::Account(failed)),
        "{run:?}"
    );
    assert!(!run.x_read && run.two.is_none(), "{run:?}");
    assert!(reads_before_two(&d).is_empty(), "{}", d.printed);
    assert_eq!((d.count("add_order"), d.count("cancel_order")), (1, 0));
    let model = d.venue.model.lock().unwrap();
    assert_eq!(model.orders[0].state, model::State::Open);
    drop(model);
    let (report, json) = d.outputs();
    let resting = format!(
        "- X resting when the run ended: {}",
        places::HAND_CANCEL_UNREAD
    );
    assert!(report.contains(&resting), "{report}");
    assert!(
        report.contains(&format!("**The place stages' stop:** {name}.")),
        "{report}"
    );
    assert_eq!(json["stop"], name);
    assert_eq!(
        json["drop_tests"][0]["session_one"]["x_unseen"],
        "Account(Positions)"
    );
    assert_report_and_evidence_pass_the_privacy_check(&d.dir.join("evidence"));
}

#[tokio::test]
async fn a_press_as_the_last_read_after_a_failed_drop_session_two_ends_outranks_x_counted() {
    // The same on drop session two's path (FBC-gisx): the session fails with no stop, Y not
    // priced once its arm was accepted, so no verdict judges X and Y, and the read of X's
    // account after it fails with no provider rate refusal (the seventh GET /user/positions after
    // X's place), so X alone counts as resting. The owner presses Ctrl-C as the last named
    // account's read after the session prints its line: the press is the run's stop after the
    // session's failure, Interrupted, marked, ahead of X counted resting, and the RESTING line
    // still asks for X's hand cancel as not read.
    let model = QfexModel {
        rest_fails: vec![6],
        ..QfexModel::default()
    };
    let at = "READ after drop session two: subaccount";
    let hooked = |t: &mut probe_args::TestHooks| t.y_unpriced = true;
    let d = pressed_at_hooked(model, "drop-two-press-x-unread", hooked, at).await;
    let name = places::Stop::Interrupted.name();
    assert_eq!(d.ev.stop, Some(name), "{}", d.printed);
    let marker = d.marker().unwrap_or_default();
    assert!(marker.starts_with(&format!("stop: {name}\n")), "{marker}");
    let line = format!(
        "a test's Y could not be priced; Interrupted during the reads after drop session two: {}; \
         no further session runs",
        places::Stop::Interrupted.says()
    );
    assert_eq!(d.ev.stopped.as_deref(), Some(line.as_str()));
    let unread = format!("RESTING {}", places::HAND_CANCEL_X_UNREAD);
    assert!(d.printed.contains(&unread), "{}", d.printed);
    let two = d.two();
    let counted = (places::AccountUnread::Positions, places::Counted::X);
    assert_eq!(two.unread, Some(counted), "{two:?}");
    assert!(!two.read && !two.y.submitted, "{two:?}");
    assert!(d.drop_run().verdict.is_none());
    assert_eq!((d.count("add_order"), d.count("cancel_order")), (1, 0));
    let (report, json) = d.outputs();
    assert!(
        report.contains(&format!("**The place stages' stop:** {name}.")),
        "{report}"
    );
    let two = &json["drop_tests"][0]["session_two"];
    assert_eq!(two["counted_resting"], "X");
    assert_eq!(two["hand_cancel"], places::HAND_CANCEL_X_UNREAD);
    assert_report_and_evidence_pass_the_privacy_check(&d.dir.join("evidence"));
}
