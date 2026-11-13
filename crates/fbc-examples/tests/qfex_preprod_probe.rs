//! FBC-l931's done line: qfex_preprod_probe, run against the conformance kit's stub server on
//! 127.0.0.1 standing in for QFEX (its account REST reads and its trade socket), with synthetic
//! credentials:
//!
//! - refuses a production host, an invalid account (decision 0122 item 1), a proxied loopback
//!   origin and missing credentials before anything connects;
//! - refuses to start a session on an open order or a position that is not flat, and on an
//!   account it cannot read;
//! - sends no command but each session's arm and its reads, runs the sessions one after the
//!   other, and stops each at its arm's first failure with no second connection attempt;
//! - journals the arm's window as decision 0132 item 3 keeps it, and writes the report, the
//!   evidence and a scrubbed fixture slice that pass scripts/privacy-check.sh, with no
//!   credential or account id in any of them;
//! - with `--production` (FBC-3prm, decision 0143 item 3), runs stage one in the adapter's
//!   `production_probe` mode to a complete run whose report, evidence and fixture README name
//!   production and whose registries carry no testnet declaration, and refuses a pre-production
//!   origin with it, and a production origin without it, before anything connects;
//! - on either environment (FBC-5ph8, decision 0144), sends no `GET /user/subaccounts`: before
//!   every session it reads the accounts its configuration names, the primary account and the
//!   subaccount `QFEX_SUBACCOUNT_ID` names, and its report, evidence and output say the accounts
//!   came from the configuration and not from QFEX; a configured subaccount it cannot read stops
//!   it before anything arms.
//!
//! The stub answers as QFEX's pages show (each frame below names its page): each account's
//! positions selected by `x-qfex-requested-account-id`, the authentication reply, the
//! subscription confirmations and the `get_user_orders` page. It answers `GET /user/subaccounts`
//! HTTP 403, as QFEX answered a key scoped to one account in FBC-291a's first production run, and
//! records every request, so a test can show the probe sent none. Every key, secret, account id
//! and answer is synthetic: the REST answers are `fixtures/qfex/private` (its `SYNTHETIC` file).
//! The run is on real time; each session waits out its arm's 600 ms deadline.

#[path = "../src/auth_qfex.rs"]
mod auth_qfex;
#[path = "../examples/qfex_preprod_probe/capture.rs"]
mod capture;
#[allow(dead_code)]
#[path = "../examples/paradex_testnet_acceptance/evidence.rs"]
mod evidence;
#[path = "../examples/qfex_preprod_probe/ledger.rs"]
mod ledger;
#[allow(dead_code)]
#[path = "../src/link.rs"]
mod link;
#[path = "../examples/qfex_preprod_probe/probe_args.rs"]
mod probe_args;
#[path = "../examples/qfex_preprod_probe/refusals.rs"]
mod refusals;
#[path = "../examples/qfex_preprod_probe/stage.rs"]
mod stage;

use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use auth_qfex::{Armed, PUBLIC_VAR, SECRET_VAR, SUB_ID_VAR, SUB_PUBLIC_VAR, SUB_SECRET_VAR};
use capture::{Arrived, Evidence, SessionRun, When};
use fbc_conformance::{
    Frame, HttpReply, HttpRouter, PathPattern, Responder, Step, StubServer, WsScript,
};
use fbc_core::{
    ConnKey, InboundSpans, MonoNs, RawFrame, RpcId, Stamp, TrafficClass, WallNs, WireSlice,
};
use fbc_journal::{
    ControlEvent, JournalError, JournalSink, JournalWriter, Opcode, QueueSink, Record, Recorded,
    RedactionKey, SinkConfig, WriterThread, journal_queue,
};
use fbc_runtime::http::Method;
use fbc_runtime::{ProxyConfig, RateUserKey};
use fbc_venue_qfex::exec::ARM;
use fbc_venue_qfex::probe::{
    Environment, Origins, PREPROD_MDS, PREPROD_REST, PREPROD_TRADE, PRODUCTION_MDS,
    PRODUCTION_REST, PRODUCTION_TRADE,
};
use probe_args::{Options, Parsed};
use refusals::Refusals;
use serde_json::Value;

/// Made-up credentials, in the alphabet QFEX's examples show.
const PUBLIC: &str = "qfex_pub_SYNTHETIC_0001";
const SECRET: &str = "synthetic-qfex-secret-0001";
const SUB_PUBLIC: &str = "qfex_pub_SYNTHETIC_0002";
const SUB_SECRET: &str = "synthetic-qfex-secret-0002";
/// The made-up subaccount the environment names (`QFEX_SUBACCOUNT_ID`), in the shape QFEX writes
/// one.
const SUB: &str = "5a5a5a5a-0000-4000-8000-00000000c0de";

/// The authenticate page's success reply.
const AUTH_OK: &str = r#"{"type":"auth","result":"success"}"#;
/// An empty `all_orders_response` (the get_user_orders page and the AsyncAPI).
const EMPTY_PAGE: &str = r#"{"all_orders_response":{"orders":[],"twaps":[]}}"#;
/// The arm's echo in the AsyncAPI's generic `ack` (decision 0132 item 1).
const ARM_ACK: &str = r#"{"ack":{"incoming_message":{"type":"cancel_on_disconnect","params":{"cancel_on_disconnect":true}}}}"#;
/// A flat err echoing another request, in the errors page's shape.
const OTHER_ERR: &str = r#"{"type":"Err","error_code":"InvalidParameter","message":"synthetic text","incoming_message":{"type":"get_order","params":{"symbol":"AAPL-USD"}}}"#;

fn fixture(name: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/qfex/private");
    fs::read_to_string(path.join(name)).unwrap()
}

fn ok(body: &str) -> HttpReply {
    HttpReply {
        status: 200,
        body: body.as_bytes().to_vec(),
    }
}

/// A directory removed when dropped.
struct Removed(PathBuf);

impl Drop for Removed {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// A fresh directory of the test's own, removed when the guard drops.
fn temp_dir(what: &str) -> (PathBuf, Removed) {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir =
        std::env::temp_dir().join(format!("fbc-qfex-probe-{what}-{}-{n}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    (dir.clone(), Removed(dir))
}

/// The environment: both credentials, and the subaccount armed on.
fn environment() -> HashMap<&'static str, String> {
    HashMap::from([
        (PUBLIC_VAR, PUBLIC.to_owned()),
        (SECRET_VAR, SECRET.to_owned()),
        (SUB_ID_VAR, SUB.to_owned()),
        (SUB_PUBLIC_VAR, SUB_PUBLIC.to_owned()),
        (SUB_SECRET_VAR, SUB_SECRET.to_owned()),
    ])
}

/// What QFEX answered FBC-291a's first production run's `GET /user/subaccounts`, made with a key
/// scoped to the primary account: HTTP 403, insufficient API key permissions (decision 0144), in
/// the REST reference's problem shape, synthetic.
fn listing_refused() -> HttpReply {
    HttpReply {
        status: 403,
        body: br#"{"title":"Forbidden","status":403,"detail":"insufficient API key permissions"}"#
            .to_vec(),
    }
}

/// The stub's REST answers: each account's positions by its selection header (`None` for the
/// primary account), and the listing refused ([`listing_refused`]), which the probe never asks
/// for (decision 0144): a run that sent it would stop there.
fn rest(positions: HashMap<Option<&'static str>, HttpReply>) -> HttpRouter {
    let positions: HashMap<Option<String>, HttpReply> = positions
        .into_iter()
        .map(|(k, v)| (k.map(str::to_owned), v))
        .collect();
    HttpRouter::new()
        .route(
            Method::GET,
            PathPattern::exact("/user/subaccounts"),
            listing_refused(),
        )
        .route_fn(
            Method::GET,
            PathPattern::exact("/user/positions"),
            move |req| {
                let account = req.header("x-qfex-requested-account-id").map(str::to_owned);
                positions[&account].clone()
            },
        )
}

/// `fixtures/qfex/private/positions_flat.json`, every position flat, with each row's
/// `open_orders` set to `open_orders` (the fixture counts one on a flat row).
fn positions_flat(open_orders: u64) -> String {
    let mut body = json(&fixture("positions_flat.json"));
    for row in body["positions"].as_array_mut().unwrap() {
        row["open_orders"] = open_orders.into();
    }
    body.to_string()
}

/// Both accounts flat, with no open order counted.
fn flat() -> HashMap<Option<&'static str>, HttpReply> {
    let flat = ok(&positions_flat(0));
    HashMap::from([(None, flat.clone()), (Some(SUB), flat)])
}

/// Asserts the stub received no `GET /user/subaccounts` (decision 0144): every request it
/// received is a positions read.
fn assert_no_listing(server: &StubServer) {
    let asked = server.http_requests();
    assert!(
        asked.iter().all(|r| r == "GET /user/positions"),
        "{asked:?}"
    );
}

/// A responder sending `frames`, whatever it read.
fn reply(frames: &[&str]) -> Responder {
    let frames: Vec<Frame> = frames.iter().map(|f| Frame::text(*f)).collect();
    Responder::new(move |_| Ok(frames.clone()))
}

/// A responder confirming the subscription it read, as the AsyncAPI's `subscribed` reply does.
fn confirm() -> Responder {
    Responder::new(|frame| {
        let Frame::Text(text) = frame else {
            return Err("a binary subscribe".to_owned());
        };
        let v: Value = serde_json::from_str(text).map_err(|e| e.to_string())?;
        let channel = v["params"]["channels"][0].as_str().ok_or("no channel")?;
        Ok(vec![Frame::text(format!(
            r#"{{"subscribed":"{channel}"}}"#
        ))])
    })
}

/// Connection `conn`, a read socket: its authentication answered, then its page.
fn read_socket(conn: usize, page: &str) -> Vec<Step> {
    vec![
        Step::Accept,
        Step::Respond {
            conn,
            with: reply(&[AUTH_OK]),
        },
        Step::Respond {
            conn,
            with: reply(&[page]),
        },
    ]
}

/// Connection `conn`, a session: authenticated, the three subscriptions confirmed, and the arm
/// read and answered with `after_arm`.
fn session_socket(conn: usize, after_arm: &[&str]) -> Vec<Step> {
    let mut steps = vec![
        Step::Accept,
        Step::Respond {
            conn,
            with: reply(&[AUTH_OK]),
        },
    ];
    for _ in 0..3 {
        steps.push(Step::Respond {
            conn,
            with: confirm(),
        });
    }
    steps.push(Step::Respond {
        conn,
        with: reply(after_arm),
    });
    steps
}

/// The options of a run against `server`, writing into `dir/evidence`.
fn options(dir: &Path, server: &StubServer, arms: &[Armed]) -> Options {
    Options {
        evidence_dir: dir.join("evidence"),
        arms: arms.to_vec(),
        production: false,
        trade_url: server.ws_url(""),
        rest_url: server.http_url(""),
        mds_url: server.ws_url(""),
        proxy: ProxyConfig::Direct,
        rpc_timeout_ms: 600,
        page_size: 1000,
        read_wait: Duration::from_secs(5),
        connect_wait: Duration::from_secs(5),
        session_wait: Duration::from_secs(30),
        ledger: bounds(Duration::from_secs(60)),
        rate_user: user(),
    }
}

/// A provider user of the run's own. A user's rate budget is process-wide (decision 0123), and
/// the tests run probes at once in one process: each counts its refusals, and holds its budget,
/// for its own user alone.
fn user() -> RateUserKey {
    static NEXT: AtomicU64 = AtomicU64::new(1_000);
    RateUserKey::new(NEXT.fetch_add(1, Ordering::Relaxed))
}

/// Stage one's ledger bounds under the span bound `span`: an age a second longer, and a count
/// of one, above stage one's fill ceiling of zero.
fn bounds(span: Duration) -> ledger::Bounds {
    let age = span + Duration::from_secs(1);
    ledger::Bounds::new(age, 1, span, &ledger::stage_one()).unwrap()
}

/// Runs the probe with `opts` and `env`: its outcome or refusal, and what it printed.
async fn probe(
    opts: &Options,
    env: &HashMap<&'static str, String>,
) -> (Result<stage::Outcome, String>, String) {
    let lookup = |var: &str| env.get(var).cloned();
    let printed = Rc::new(RefCell::new(Vec::<u8>::new()));
    let ran = stage::run(opts, &lookup, printed.clone()).await;
    let printed = String::from_utf8(printed.borrow().clone()).unwrap();
    (ran, printed)
}

/// The text frames connection `conn` received.
fn received(server: &StubServer, conn: usize) -> Vec<String> {
    let conns = server.connections();
    let frames = conns[conn].received.iter();
    frames
        .map(|f| match f {
            Frame::Text(t) => t.clone(),
            Frame::Binary(_) => "<binary>".to_owned(),
        })
        .collect()
}

fn json(text: &str) -> Value {
    serde_json::from_str(text).unwrap()
}

/// Asserts `frames` are a read socket's: the authentication of `account` (its `account_id` only
/// for a subaccount) with `public`, and one get_user_orders page of 1000 from offset 0.
fn assert_read_socket(frames: &[String], account: Option<&str>, public: &str) {
    assert_eq!(frames.len(), 2, "{frames:?}");
    let auth = json(&frames[0]);
    assert_eq!(auth["type"], "auth");
    assert_eq!(auth["params"]["hmac"]["public_key"], public);
    assert_eq!(auth["params"]["account_id"].as_str(), account);
    assert_eq!(
        frames[1],
        r#"{"type":"get_user_orders","params":{"limit":1000,"offset":0}}"#
    );
}

/// Asserts `frames` are a session's: its authentication of `account` with `public`, the three
/// subscriptions, then the arm, and nothing else.
fn assert_session_socket(frames: &[String], account: Option<&str>, public: &str) {
    assert_eq!(frames.len(), 5, "{frames:?}");
    let auth = json(&frames[0]);
    assert_eq!(auth["params"]["hmac"]["public_key"], public);
    assert_eq!(auth["params"]["account_id"].as_str(), account);
    for (frame, channel) in frames[1..4]
        .iter()
        .zip(["order_responses", "fills", "positions"])
    {
        let sub = format!(r#"{{"type":"subscribe","params":{{"channels":["{channel}"]}}}}"#);
        assert_eq!(*frame, sub);
    }
    assert_eq!(frames[4], ARM);
}

/// Every file under `dir`, recursively, with its text.
fn files(dir: &Path) -> Vec<(PathBuf, String)> {
    let mut found = Vec::new();
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            found.extend(files(&path));
        } else {
            found.push((
                path.clone(),
                String::from_utf8_lossy(&fs::read(&path).unwrap()).into(),
            ));
        }
    }
    found
}

/// Asserts no credential and no account id is in the report, the evidence or a fixture.
fn assert_no_secret(dir: &Path) {
    let outputs = files(dir)
        .into_iter()
        .filter(|(p, _)| !p.starts_with(dir.join("journal")));
    for (path, text) in outputs {
        for secret in [PUBLIC, SECRET, SUB_PUBLIC, SUB_SECRET, SUB] {
            assert!(
                !text.contains(secret),
                "{} holds a credential or id",
                path.display()
            );
        }
    }
}

// ----- The capture -----

#[tokio::test]
async fn each_named_account_arms_once_in_turn_with_no_subaccount_listing_and_its_capture_is_written_scrubbed()
 {
    let (dir, _removed) = temp_dir("capture");
    let position = fixture("position_response.json");
    let position = position.split_whitespace().collect::<String>();
    let mut steps = Vec::new();
    // Before the primary account's session: the primary account, then the subaccount the
    // environment names, the first read pinged between its two replies (a ping is passed over).
    steps.extend(read_socket(0, EMPTY_PAGE));
    steps.insert(2, Step::Barrier { conn: 0 });
    steps.extend(read_socket(1, EMPTY_PAGE));
    steps.extend(session_socket(2, &[ARM_ACK, OTHER_ERR, &position]));
    // Before the subaccount's session, both accounts again; then its session, whose arm
    // nothing answers.
    steps.extend(read_socket(3, EMPTY_PAGE));
    steps.extend(read_socket(4, EMPTY_PAGE));
    steps.extend(session_socket(5, &[]));
    let server = StubServer::start(WsScript::new(steps), rest(flat()))
        .await
        .unwrap();
    let opts = options(&dir, &server, &[Armed::Primary, Armed::Subaccount]);

    let (ran, printed) = probe(&opts, &environment()).await;
    let outcome = ran.unwrap();
    assert!(outcome.ok, "{printed}\n{:#?}", outcome.evidence);
    server.finished().await.unwrap();

    // No command but the arm: each read socket sent its authentication and one page, each
    // session its authentication, its subscriptions and one arm. Six connections, so no
    // session attempted a second.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(server.connections().len(), 6);
    for (conn, account, public) in [
        (0, None, PUBLIC),
        (1, Some(SUB), SUB_PUBLIC),
        (3, None, PUBLIC),
        (4, Some(SUB), SUB_PUBLIC),
    ] {
        assert_read_socket(&received(&server, conn), account, public);
    }
    assert_session_socket(&received(&server, 2), None, PUBLIC);
    assert_session_socket(&received(&server, 5), Some(SUB), SUB_PUBLIC);
    // No GET /user/subaccounts (decision 0144): only the two accounts' positions, before each
    // session, though the stub would have refused a listing 403.
    let asked = vec!["GET /user/positions".to_owned(); 4];
    assert_eq!(server.http_requests(), asked);
    assert_no_listing(&server);

    // What the sessions reported: each authenticated, armed once, its arm Unknown at its
    // deadline (no frame answers it yet), stopped there by the program, one attempt each.
    let ev = &outcome.evidence;
    // The accounts the configuration names, read before every session, flat with no order.
    assert_eq!(ev.accounts, ["primary", "subaccount"]);
    assert_eq!(ev.reads.len(), 4);
    assert!(ev.reads.iter().all(|r| r.refusal().is_none()));
    assert!(ev.reads.iter().all(|r| r.flat && r.page_orders == 0));
    let labels: Vec<(&str, &str)> = ev
        .reads
        .iter()
        .map(|r| (r.before, r.label.as_str()))
        .collect();
    assert_eq!(
        labels,
        [
            ("primary", "primary (armed)"),
            ("primary", "subaccount"),
            ("subaccount", "primary"),
            ("subaccount", "subaccount (armed)"),
        ]
    );
    for (run, account) in ev.sessions.iter().zip(["primary", "subaccount"]) {
        assert_eq!(run.account, account);
        assert!(run.authenticated && run.arm_sent, "{run:?}");
        assert_eq!(run.arm.as_deref(), Some("Unknown"));
        assert_eq!(run.ended_by, "the arm's outcome");
        assert_eq!((run.attempts, run.failed_attempts), (1, 0));
        assert!(run.ok());
    }

    // The primary account's frames, placed against its arm, and their fixtures: the arm's own
    // ack as received, the other echo as decision 0134 keeps it (its documented code and mapped
    // type in clear, its message and params spanned), and the position's numbers zeroed and its
    // id spanned.
    let primary = &ev.sessions[0];
    let kinds: Vec<(&str, When)> = primary
        .frames
        .iter()
        .map(|f| (f.kind.as_str(), f.when))
        .collect();
    assert_eq!(
        kinds,
        [
            ("{result, type}", When::BeforeArm),
            ("{subscribed}", When::BeforeArm),
            ("{subscribed}", When::BeforeArm),
            ("{subscribed}", When::BeforeArm),
            ("ack echoing the arm", When::InWindow),
            (
                "err echoing another request (InvalidParameter)",
                When::InWindow
            ),
            ("{position_response}", When::InWindow),
        ]
    );
    assert!(primary.answer().starts_with("An ack echoing the arm"));
    assert!(
        ev.sessions[1]
            .frames
            .iter()
            .all(|f| f.when == When::BeforeArm)
    );
    assert!(
        ev.sessions[1]
            .answer()
            .starts_with("Nothing arrived in the arm's window")
    );
    let fixtures = opts.evidence_dir.join("fixtures");
    let read = |name: &str| fs::read_to_string(fixtures.join(name)).unwrap();
    assert_eq!(read("primary-005-in-window.json"), ARM_ACK);
    assert_eq!(
        read("primary-006-in-window.json"),
        r#"{"type":"Err","error_code":"InvalidParameter","message":"22222222222222","incoming_message":{"type":"get_order","params":{"symbol":"22222222"}}}"#
    );
    let position = json(&read("primary-007-in-window.json"));
    assert_eq!(position["position_response"]["symbol"], "AAPL-USD");
    assert_eq!(position["position_response"]["id"], "2".repeat(36));
    assert_eq!(position["position_response"]["position"], 0);
    assert_eq!(position["position_response"]["average_price"], 0);
    assert_eq!(
        read("subaccount-002-before-arm.json"),
        r#"{"subscribed":"order_responses"}"#
    );
    assert!(read("README.md").contains("not synthetic"));
    assert!(
        read("README.md").contains(
            "one session per account (primary then subaccount, each named by the run's \
             configuration, not listed by QFEX: decision 0144)"
        ),
        "{}",
        read("README.md")
    );
    // The members whose string value a fixture keeps, named for the owner's reading: never a
    // span the codec blanked (the position's id, the err's message).
    assert_eq!(
        ev.strings,
        ["error_code", "result", "subscribed", "symbol", "type"]
    );
    assert!(read("README.md").contains("error_code; result; subscribed; symbol; type"));
    assert!(ev.withheld.is_empty() && ev.errors.is_empty(), "{ev:#?}");
    assert_eq!(ev.fixtures.len(), 7 + 4 + 1);
    // The journal kept every record: its sink dropped none, and no capture is partial.
    assert_eq!(ev.dropped, [0, 0]);
    assert!(ev.sessions.iter().all(|run| run.partial.is_empty()));

    // The files: the report and the evidence say what answered each arm, the fixtures pass
    // the repository's privacy check, and nothing holds a credential or an account id.
    let report = fs::read_to_string(opts.evidence_dir.join("report.md")).unwrap();
    assert!(report.contains("Result: complete."), "{report}");
    assert!(report.contains("An ack echoing the arm arrived in its window"));
    assert!(report.contains("Nothing arrived in the arm's window"));
    assert!(report.contains("read every fixture for an identifying string"));
    assert!(report.contains(
        "Records the journal's sink dropped: 0 Normal, 0 Safety. Sessions whose capture is \
         partial: none."
    ));
    // They say the accounts came from the configuration, not from QFEX, and that the owner, not
    // the probe, must confirm there is no other subaccount, which the probe does not record
    // (decision 0144).
    assert!(
        report.contains(
            "Accounts named by the run's configuration, to be read before every session: 2 \
             (primary, subaccount; ids not shown), not a list QFEX gave: the probe sends no \
             `GET /user/subaccounts` (decision 0144). The table below lists the reads made. \
             Before the run the owner must confirm in QFEX's app that the user has no \
             subaccount the configuration does not name; the probe records no such \
             confirmation and cannot check it, so the checks below, and what they show of the \
             user's other accounts, cover the named accounts only."
        ),
        "{report}"
    );
    assert!(report.contains("whether the user has a subaccount the configuration does not name"));
    assert!(!report.contains("listed:"), "{report}");
    let evidence = json(&fs::read_to_string(opts.evidence_dir.join("evidence.json")).unwrap());
    assert_eq!(evidence["ok"], true);
    assert_eq!(
        evidence["accounts"],
        json(
            r#"{"source":"configuration","named":["primary","subaccount"],"count":2,"others_checked":false,"note":"named by the run's configuration, not listed by QFEX: the probe sends no GET /user/subaccounts. The owner must confirm in QFEX's app before the run that the user has no other subaccount; the probe records no such confirmation and cannot check it (decision 0144)"}"#
        )
    );
    assert_eq!(evidence.get("subaccounts_listed"), None);
    assert_eq!(evidence["kept_string_members"][0], "error_code");
    assert_eq!(evidence["sessions"][1]["arm_outcome"], "Unknown");
    assert_eq!(evidence["journal_dropped"]["normal"], 0);
    assert_eq!(evidence["journal_dropped"]["safety"], 0);
    assert_eq!(evidence["sessions"][1]["capture"], "whole");
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/privacy-check.sh");
    evidence::privacy_check(&script, &fixtures).unwrap();
    assert_no_secret(&opts.evidence_dir);
    assert!(printed.contains("DONE ok"), "{printed}");

    // A run without --production is on pre-production's mode, whatever its origins (a stub's
    // here), and builds its registries as decision 0132 item 7 says, declared owner-assisted
    // testnet runs (decision 0143 item 3).
    assert_eq!(
        (ev.environment, ev.mode, ev.target),
        ("pre-production", "preprod_probe", "a loopback test stub")
    );
    assert!(ev.registries.iter().all(|r| r.testnet_run));
    assert!(
        report.starts_with("# QFEX pre-production stage one"),
        "{report}"
    );
    assert!(
        report.contains(
            "Environment: QFEX pre-production, under the probe mode `preprod_probe`. Started"
        ),
        "{report}"
    );
    assert_eq!(
        (&evidence["environment"], &evidence["exec_mode"]),
        (&json("\"pre-production\""), &json("\"preprod_probe\""))
    );
    assert_eq!(evidence["registries"][0]["testnet_run"], true);
    assert!(
        read("README.md").contains("on QFEX pre-production (the probe mode `preprod_probe`)"),
        "{}",
        read("README.md")
    );
    assert!(
        printed.contains("PROBE stage one on QFEX pre-production (preprod_probe)"),
        "{printed}"
    );
    assert!(
        printed.contains(
            "ACCOUNTS 2 named by the run's configuration, not listed by QFEX (decision 0144): \
             primary, subaccount"
        ),
        "{printed}"
    );
    assert!(!printed.contains("LISTED"), "{printed}");

    // Each session's own registry (each arms its own account), its ledger under the run's
    // bounds, and at its session's end a full horizon and no replay (decision 0132 item 5), in
    // the report and the evidence, which pass the privacy check.
    let shown: Vec<(&str, usize)> = (ev.registries.iter())
        .map(|r| (r.account, r.ends.len()))
        .collect();
    assert_eq!(shown, [("primary", 1), ("subaccount", 1)]);
    for r in &ev.registries {
        assert_eq!(r.bounds, opts.ledger);
        let end = &r.ends[0];
        assert_eq!(end.session, r.account);
        assert!(end.after < opts.session_wait, "{end:?}");
        assert_eq!((end.horizon, end.held), (fbc_oms::Horizon::Full, 0));
        assert_eq!(end.replays, fbc_oms::ReplayCounts::default());
        assert!(r.held());
    }
    for account in ["primary", "subaccount"] {
        let row =
            format!("| {account} | 61s | 1 | 60s | 0 | no order: its sessions place none | yes |");
        assert!(report.contains(&row), "{report}");
        let end = format!("| {account} | {account} | ");
        assert!(report.contains(&end), "{report}");
        assert!(
            report.contains(" | no | full | 0 | 0 | 0 | 0 | 0 | 0 |"),
            "{report}"
        );
    }
    let registry = &evidence["registries"][1];
    assert_eq!(registry["account"], "subaccount");
    assert_eq!(
        (
            &registry["ledger_max_age_ms"],
            &registry["ledger_max_entries"]
        ),
        (&json("61000"), &json("1"))
    );
    assert_eq!(registry["registry_span_ms"], 60_000);
    assert_eq!(registry["fill_ceiling"], 0);
    assert_eq!(registry["held"], true);
    let end = &registry["session_ends"][0];
    assert_eq!(
        (&end["session"], &end["horizon"]),
        (&json("\"subaccount\""), &json("\"full\""))
    );
    assert_eq!(end["replays"]["untimed"], 0);
    assert_eq!(end["replays"]["duplicate"], 0);
    assert_eq!(end["past_span_ms"], Value::Null);

    // A clean run met no provider rate refusal (decision 0135 item 2): every read socket and
    // both sessions count none, the in-window err of another request included, and the report
    // and the evidence say so.
    assert_eq!(ev.read_sockets.len(), 4);
    let befores: Vec<&str> = ev.read_sockets.iter().map(|r| r.before).collect();
    assert_eq!(befores, ["primary", "primary", "subaccount", "subaccount"]);
    assert!(
        ev.read_sockets
            .iter()
            .all(|r| r.refusals == Refusals::default())
    );
    for run in &ev.sessions {
        assert_eq!(run.refusals, Refusals::default(), "{run:?}");
        assert!(run.refusal_frames.is_empty());
    }
    assert!(
        report.contains("No provider rate refusal was counted."),
        "{report}"
    );
    assert!(report.contains("Frames unread, kept whole or not JSON: 0."));
    assert!(report.contains(
        "Met by each session, at least, with the read sockets before it: primary 0; subaccount 0."
    ));
    assert!(report.contains(
        "| subaccount | read socket: subaccount (armed) | 0 | 0 | 0 | 0 | 0 | 0 | not journaled |"
    ));
    assert!(report.contains("| primary | the session | 0 | 0 | 0 | 0 | not seen | 0 | none |"));
    assert!(!report.contains("Provider rate-limit evidence"), "{report}");
    let rates = &evidence["provider_rate_refusals"];
    assert_eq!(rates["met_at_least"], 0);
    assert_eq!(rates["record_under"], Value::Null);
    assert_eq!(rates["frames_unread"], 0);
    assert_eq!(rates["read_sockets"].as_array().unwrap().len(), 4);
    assert_eq!(
        rates["read_sockets"][0]["counts"],
        json(
            r#"{"limiter_rejected":0,"rate_limited_errs":0,"rate_limited_order_responses":0,"frames_unread":0,"rate_limited_in_frames_kept_whole":0,"met_at_least":0}"#
        )
    );
    assert_eq!(
        rates["by_session"],
        json(
            r#"[{"session":"primary","met_at_least":0,"session_ran":true},{"session":"subaccount","met_at_least":0,"session_ran":true}]"#
        )
    );
    assert_eq!(rates["sessions"][1]["counts"]["met_at_least"], 0);
    // A session's frames reach only its codec: a refusal kept whole in one is not seen.
    assert_eq!(
        rates["sessions"][1]["counts"]["rate_limited_in_frames_kept_whole"],
        Value::Null
    );
    assert_eq!(rates["sessions"][1]["frames"], json("[]"));
    assert_report_and_evidence_pass_the_privacy_check(&opts.evidence_dir);
    assert!(!printed.contains(SUB) && !printed.contains(PUBLIC));
}

// ----- On production (FBC-3prm, decision 0143) -----

#[tokio::test]
async fn with_production_stage_one_completes_with_no_subaccount_listing_names_production_and_its_registries_carry_no_testnet_declaration()
 {
    let (dir, _removed) = temp_dir("production");
    // As a pre-production run: the primary account and the subaccount the environment names,
    // each read on a socket that never arms, then the session, whose arm is echoed in its
    // window.
    let mut steps = Vec::new();
    steps.extend(read_socket(0, EMPTY_PAGE));
    steps.extend(read_socket(1, EMPTY_PAGE));
    steps.extend(session_socket(2, &[ARM_ACK]));
    let server = StubServer::start(WsScript::new(steps), rest(flat()))
        .await
        .unwrap();
    // The run's options are the ones the command line's --production gives, the stub's origins
    // named in place of production's defaults, and its waits short enough for a test.
    let evidence_dir = dir.join("evidence");
    let (ws, http) = (server.ws_url(""), server.http_url(""));
    let argv = [
        "--production",
        "--evidence-dir",
        evidence_dir.to_str().unwrap(),
        "--arm",
        "primary",
        "--trade-url",
        &ws,
        "--rest-url",
        &http,
        "--mds-url",
        &ws,
        "--rpc-timeout-ms",
        "600",
        "--read-wait-ms",
        "5000",
        "--connect-wait-ms",
        "5000",
        "--registry-span",
        "70",
        "--ledger-max-age",
        "71",
        "--ledger-max-entries",
        "1",
    ];
    let Ok(Parsed::Run(parsed)) = probe_args::parse(argv.iter().map(|a| (*a).to_owned())) else {
        panic!("not parsed");
    };
    let mut opts = *parsed;
    assert!(opts.production);
    assert_eq!(opts.environment(), Environment::Production);
    // A provider user of the test's own: the program's is one per process (decision 0123).
    opts.rate_user = user();

    let (ran, printed) = probe(&opts, &environment()).await;
    let outcome = ran.unwrap();
    assert!(outcome.ok, "{printed}\n{:#?}", outcome.evidence);
    server.finished().await.unwrap();

    // Every other check of decision 0132 item 7 ran unchanged: each account the configuration
    // names read before the session, flat with no open order, and one session that sent its
    // authentication, its subscriptions and its arm on one connection attempt, and nothing
    // else. No GET /user/subaccounts (decision 0144), though the stub would have refused it.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(server.connections().len(), 3);
    for (conn, account, public) in [(0, None, PUBLIC), (1, Some(SUB), SUB_PUBLIC)] {
        assert_read_socket(&received(&server, conn), account, public);
    }
    assert_session_socket(&received(&server, 2), None, PUBLIC);
    let asked = vec!["GET /user/positions".to_owned(); 2];
    assert_eq!(server.http_requests(), asked);
    assert_no_listing(&server);
    let ev = &outcome.evidence;
    assert_eq!(ev.accounts, ["primary", "subaccount"]);
    let labels: Vec<(&str, &str)> = (ev.reads.iter())
        .map(|r| (r.before, r.label.as_str()))
        .collect();
    assert_eq!(
        labels,
        [("primary", "primary (armed)"), ("primary", "subaccount")]
    );
    assert!(ev.reads.iter().all(|r| r.refusal().is_none()));
    let [run] = ev.sessions.as_slice() else {
        panic!("{:#?}", ev.sessions);
    };
    assert!(run.ok() && run.authenticated && run.arm_sent, "{run:?}");
    assert_eq!(run.arm.as_deref(), Some("Unknown"));
    assert!(run.answer().starts_with("An ack echoing the arm"));

    // The environment the mode names, beside where the origins pointed: a stub, here.
    assert_eq!(
        (ev.environment, ev.mode, ev.target),
        ("production", "production_probe", "a loopback test stub")
    );
    // Its registries carry no testnet declaration (decision 0067), and their bounds held.
    let [registry] = ev.registries.as_slice() else {
        panic!("{:#?}", ev.registries);
    };
    assert_eq!(registry.account, "primary");
    assert!(!registry.testnet_run && registry.held(), "{registry:?}");

    // The report, the evidence and the fixtures' README name production.
    let report = fs::read_to_string(opts.evidence_dir.join("report.md")).unwrap();
    assert!(
        report.starts_with("# QFEX production stage one"),
        "{report}"
    );
    assert!(
        report.contains(
            "Environment: QFEX production, under the probe mode `production_probe`. Started"
        ),
        "{report}"
    );
    assert!(report.contains("against a loopback test stub. Result: complete."));
    assert!(
        report.contains(
            "| primary | 71s | 1 | 70s | 0 | no order: its sessions place none | yes | no |"
        ),
        "{report}"
    );
    assert!(
        report.contains(
            "Accounts named by the run's configuration, to be read before every session: 2 \
             (primary, subaccount; ids not shown), not a list QFEX gave"
        ),
        "{report}"
    );
    let evidence = json(&fs::read_to_string(opts.evidence_dir.join("evidence.json")).unwrap());
    assert_eq!(evidence["ok"], true);
    assert_eq!(evidence["accounts"]["source"], "configuration");
    assert_eq!(
        evidence["accounts"]["named"],
        json(r#"["primary","subaccount"]"#)
    );
    assert_eq!(evidence["accounts"]["others_checked"], false);
    assert_eq!(evidence.get("subaccounts_listed"), None);
    assert_eq!(evidence["environment"], "production");
    assert_eq!(evidence["exec_mode"], "production_probe");
    assert_eq!(evidence["target"], "a loopback test stub");
    assert_eq!(evidence["registries"][0]["testnet_run"], false);
    let fixtures = opts.evidence_dir.join("fixtures");
    let readme = fs::read_to_string(fixtures.join("README.md")).unwrap();
    assert!(
        readme.contains(
            "on QFEX production (the probe mode `production_probe`), from a loopback test stub"
        ),
        "{readme}"
    );
    assert!(readme.contains("not synthetic"), "{readme}");
    assert_eq!(
        fs::read_to_string(fixtures.join("primary-005-in-window.json")).unwrap(),
        ARM_ACK
    );
    // Scrubbed exactly as a pre-production run's (decision 0009).
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/privacy-check.sh");
    evidence::privacy_check(&script, &fixtures).unwrap();
    assert_report_and_evidence_pass_the_privacy_check(&opts.evidence_dir);
    assert_no_secret(&opts.evidence_dir);
    assert!(
        printed.contains(
            "PROBE stage one on QFEX production (production_probe), against a loopback test stub"
        ),
        "{printed}"
    );
    assert!(printed.contains("ACCOUNTS 2 named by the run's configuration"));
    assert!(printed.contains("DONE ok"), "{printed}");
}

#[tokio::test]
async fn with_production_a_preproduction_origin_and_without_it_a_production_origin_are_refused_before_connecting()
 {
    let (dir, _removed) = temp_dir("environments");
    let server = StubServer::start(WsScript::new(Vec::new()), rest(flat()))
        .await
        .unwrap();
    let base = options(&dir, &server, &[Armed::Primary, Armed::Subaccount]);
    let with = |production: bool, f: &dyn Fn(&mut Options)| {
        let mut opts = base.clone();
        opts.production = production;
        f(&mut opts);
        opts
    };
    let production = "a production qfex.com host";
    let preproduction = "a pre-production qfex.io host";
    // A run naming no loopback origin goes through a SOCKS5 proxy no one listens on, so a guard
    // that let it through would still reach no QFEX host.
    let nowhere = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        ProxyConfig::Socks5 {
            host: "127.0.0.1".into(),
            port,
        }
    };
    let cases: Vec<(Options, String)> = vec![
        // Without --production: each production origin, and all three.
        (
            with(false, &|o| o.trade_url = PRODUCTION_TRADE.into()),
            format!("qfex.trade_url: {production}"),
        ),
        (
            with(false, &|o| o.rest_url = PRODUCTION_REST.into()),
            format!("qfex.rest_url: {production}"),
        ),
        (
            with(false, &|o| o.mds_url = PRODUCTION_MDS.into()),
            format!("qfex.mds_url: {production}"),
        ),
        (
            with(false, &|o| {
                o.trade_url = PRODUCTION_TRADE.into();
                o.rest_url = PRODUCTION_REST.into();
                o.mds_url = PRODUCTION_MDS.into();
                o.proxy = nowhere.clone();
            }),
            format!("qfex.trade_url: {production}"),
        ),
        // With it: each pre-production origin, and all three, the defaults without it.
        (
            with(true, &|o| o.trade_url = PREPROD_TRADE.into()),
            format!("qfex.trade_url: {preproduction}"),
        ),
        (
            with(true, &|o| o.rest_url = PREPROD_REST.into()),
            format!("qfex.rest_url: {preproduction}"),
        ),
        (
            with(true, &|o| o.mds_url = PREPROD_MDS.into()),
            format!("qfex.mds_url: {preproduction}"),
        ),
        (
            with(true, &|o| {
                o.trade_url = PREPROD_TRADE.into();
                o.rest_url = PREPROD_REST.into();
                o.mds_url = PREPROD_MDS.into();
                o.proxy = nowhere.clone();
            }),
            format!("qfex.trade_url: {preproduction}"),
        ),
        // With it: a mix of sides, and loopback origins behind a proxy.
        (
            with(true, &|o| o.trade_url = PRODUCTION_TRADE.into()),
            "qfex.rest_url: mixes a loopback origin with a production one".to_owned(),
        ),
        (
            with(true, &|o| {
                o.proxy = ProxyConfig::Socks5 {
                    host: "127.0.0.1".into(),
                    port: 1080,
                }
            }),
            "--socks5 with loopback origins".to_owned(),
        ),
    ];
    for (opts, refused) in cases {
        let (ran, printed) = probe(&opts, &environment()).await;
        let err = ran
            .err()
            .unwrap_or_else(|| panic!("not refused: {refused}"));
        assert!(err.contains(&refused), "{err} (expected {refused})");
        for value in [
            PUBLIC,
            SECRET,
            SUB,
            PREPROD_TRADE,
            PRODUCTION_TRADE,
            PREPROD_MDS,
        ] {
            assert!(!err.contains(value), "{err}");
        }
        assert!(printed.is_empty(), "{printed}");
        assert!(
            !opts.evidence_dir.exists(),
            "{refused}: the evidence directory was made"
        );
    }
    // Nothing connected: no socket and no request.
    assert!(server.connections().is_empty());
    assert!(server.http_requests().is_empty());
}

// ----- Refusals before anything connects -----

#[tokio::test]
async fn a_production_host_an_invalid_account_a_proxied_loopback_or_a_missing_credential_is_refused_before_connecting()
 {
    let (dir, _removed) = temp_dir("refused");
    let server = StubServer::start(WsScript::new(Vec::new()), rest(flat()))
        .await
        .unwrap();
    let base = options(&dir, &server, &[Armed::Primary, Armed::Subaccount]);
    let with = |f: &dyn Fn(&mut Options)| {
        let mut opts = base.clone();
        f(&mut opts);
        opts
    };
    let without = |var: &'static str| {
        let mut env = environment();
        env.remove(var);
        env
    };
    let env_with = |var: &'static str, value: &str| {
        let mut env = environment();
        env.insert(var, value.to_owned());
        env
    };
    let cases: Vec<(Options, HashMap<&'static str, String>, &str)> = vec![
        (
            with(&|o| o.trade_url = "wss://trade.qfex.com".into()),
            environment(),
            "qfex.trade_url",
        ),
        (
            with(&|o| {
                o.trade_url = "wss://trade.qfex.io".into();
                o.rest_url = "https://api.qfex.io".into();
                o.mds_url = "wss://mds.qfex.com".into();
            }),
            environment(),
            "qfex.mds_url: a production qfex.com host",
        ),
        (
            base.clone(),
            env_with(SUB_ID_VAR, &SUB.to_uppercase()),
            "qfex.account_id",
        ),
        (
            base.clone(),
            env_with(SUB_ID_VAR, "Primary"),
            "qfex.account_id",
        ),
        // `primary` is an account id 0122 item 1 admits, but not a subaccount: the subaccount
        // slot naming it would read the primary account twice and count it as two, whether or
        // not a session arms on it (decision 0144 item 1).
        (
            base.clone(),
            env_with(SUB_ID_VAR, "primary"),
            "QFEX_SUBACCOUNT_ID selects the primary account",
        ),
        (
            with(&|o| o.arms = vec![Armed::Primary]),
            env_with(SUB_ID_VAR, "primary"),
            "QFEX_SUBACCOUNT_ID selects the primary account",
        ),
        // A subaccount the environment names is read before every session, so its id is judged
        // before anything connects even when no session arms on it (decision 0144 item 1).
        (
            with(&|o| o.arms = vec![Armed::Primary]),
            env_with(SUB_ID_VAR, &SUB.to_uppercase()),
            "qfex.account_id",
        ),
        (
            with(&|o| {
                o.proxy = ProxyConfig::Socks5 {
                    host: "127.0.0.1".into(),
                    port: 1080,
                }
            }),
            environment(),
            "--socks5 with loopback origins",
        ),
        (
            base.clone(),
            without(SECRET_VAR),
            "QFEX_SECRET_KEY is not set",
        ),
        (
            base.clone(),
            without(SUB_ID_VAR),
            "QFEX_SUBACCOUNT_ID is not set",
        ),
        (base.clone(), without(SUB_SECRET_VAR), "or neither"),
        (
            base.clone(),
            env_with(PUBLIC_VAR, "a key with spaces"),
            "qfex.public_key",
        ),
    ];
    for (opts, env, refused) in cases {
        let (ran, printed) = probe(&opts, &env).await;
        let err = ran
            .err()
            .unwrap_or_else(|| panic!("not refused: {refused}"));
        assert!(err.contains(refused), "{err} (expected {refused})");
        for secret in [
            PUBLIC,
            SECRET,
            SUB,
            SUB_PUBLIC,
            SUB_SECRET,
            "a key with spaces",
        ] {
            assert!(!err.contains(secret) || refused.contains(secret), "{err}");
        }
        assert!(printed.is_empty(), "{printed}");
        assert!(
            !opts.evidence_dir.exists(),
            "{refused}: the evidence directory was made"
        );
    }
    // A run on the primary account alone reads no subaccount variable.
    let mut env = environment();
    env.retain(|var, _| [PUBLIC_VAR, SECRET_VAR].contains(var));
    let opts = with(&|o| {
        o.arms = vec![Armed::Primary];
        o.trade_url = "wss://trade.qfex.com".into();
    });
    let err = probe(&opts, &env).await.0.err().unwrap();
    assert!(err.contains("qfex.trade_url"), "{err}");
    // A directory that is not empty is refused too, before anything connects.
    fs::create_dir_all(base.evidence_dir.join("old")).unwrap();
    let err = probe(&base, &environment()).await.0.err().unwrap();
    assert!(err.contains("--evidence-dir is not empty"), "{err}");
    assert!(server.connections().is_empty());
    assert!(server.http_requests().is_empty());
}

// ----- Refusals before a session -----

/// Runs the probe against a stub playing `steps` and answering `positions`, arming on the
/// primary account then the subaccount: the evidence, which must say the probe stopped with
/// `stopped` before any session armed, and the stub, which received no listing.
async fn stopped(
    what: &str,
    steps: Vec<Step>,
    positions: HashMap<Option<&'static str>, HttpReply>,
    tune: impl Fn(&mut Options),
    stopped: &str,
) -> (Evidence, StubServer) {
    let (dir, _removed) = temp_dir(what);
    let server = StubServer::start(WsScript::new(steps), rest(positions))
        .await
        .unwrap();
    let mut opts = options(&dir, &server, &[Armed::Primary, Armed::Subaccount]);
    tune(&mut opts);
    let (ran, printed) = probe(&opts, &environment()).await;
    let outcome = ran.unwrap();
    assert!(!outcome.ok, "{what}");
    let why = outcome.evidence.stopped.clone().unwrap_or_default();
    assert!(
        why.contains(stopped),
        "{what}: stopped with {why:?}, expected {stopped}"
    );
    assert!(
        printed.contains("STOPPED") && printed.contains("DONE INCOMPLETE"),
        "{printed}"
    );
    assert!(
        outcome.evidence.sessions.is_empty(),
        "{what}: a session started"
    );
    assert_no_session(&server);
    assert_no_listing(&server);
    // The accounts it was to read are named whatever stopped it, as the configuration gives them.
    assert_eq!(outcome.evidence.accounts, ["primary", "subaccount"]);
    let report = fs::read_to_string(opts.evidence_dir.join("report.md")).unwrap();
    assert!(
        report.contains("**The probe stopped before its sessions ended:**"),
        "{report}"
    );
    // Named, not read: the table holds the reads that were made.
    assert!(report.contains(
        "Accounts named by the run's configuration, to be read before every session: 2 \
         (primary, subaccount; ids not shown)"
    ));
    assert_no_secret(&opts.evidence_dir);
    (outcome.evidence, server)
}

/// No connection received a subscription or an arm: no session ever opened.
fn assert_no_session(server: &StubServer) {
    for conn in 0..server.connections().len() {
        let frames = received(server, conn);
        assert!(
            frames.iter().all(|f| !f.contains("subscribe") && *f != ARM),
            "{frames:?}"
        );
    }
}

#[tokio::test]
async fn an_open_order_or_a_position_not_flat_refuses_the_session() {
    // The primary account lists an open order: nothing more is read.
    let page = fixture("all_orders_response.json");
    let (ev, server) = stopped(
        "open-order",
        read_socket(0, &page),
        flat(),
        |_| {},
        "primary (armed) holds an open order",
    )
    .await;
    assert_eq!((ev.reads[0].page_orders, ev.reads[0].page_twaps), (2, 1));
    assert_eq!(server.connections().len(), 1);

    // The configured subaccount holds an open order: both accounts were read, no session
    // opened.
    let mut steps = read_socket(0, EMPTY_PAGE);
    steps.extend(read_socket(1, &page));
    let (ev, server) = stopped(
        "sub-open-order",
        steps,
        flat(),
        |_| {},
        "subaccount holds an open order",
    )
    .await;
    assert_eq!(ev.reads.len(), 2);
    assert_eq!(ev.reads[1].label, "subaccount");
    assert_eq!(ev.reads[1].page_orders, 2);
    assert_eq!(server.connections().len(), 2);

    // The configured subaccount holds a position: both accounts were read, no session opened.
    let mut positions = flat();
    positions.insert(Some(SUB), ok(&fixture("positions.json")));
    let mut steps = read_socket(0, EMPTY_PAGE);
    steps.extend(read_socket(1, EMPTY_PAGE));
    let (ev, server) = stopped("not-flat", steps, positions, |_| {}, "subaccount holds").await;
    assert_eq!(ev.reads.len(), 2);
    assert!(!ev.reads[1].flat && ev.reads[1].rest_open_orders == 2);
    assert_eq!(server.connections().len(), 2);

    // The configured subaccount is flat and its page empty, but REST counts an open order: the
    // two reads disagree, and the probe reads that strictly.
    let mut positions = flat();
    positions.insert(Some(SUB), ok(&fixture("positions_flat.json")));
    let mut steps = read_socket(0, EMPTY_PAGE);
    steps.extend(read_socket(1, EMPTY_PAGE));
    let (ev, server) = stopped(
        "rest-count",
        steps,
        positions,
        |_| {},
        "subaccount shows no open order on get_user_orders, but GET /user/positions counts 1",
    )
    .await;
    let read = &ev.reads[1];
    assert!(read.flat && read.page_orders == 0 && read.rest_open_orders == 1);
    assert!(read.refusal().is_some());
    assert_eq!(server.connections().len(), 2);
}

#[tokio::test]
async fn an_account_that_cannot_be_read_stops_the_probe_before_anything_arms() {
    let refused = |status, body: &str| HttpReply {
        status,
        body: body.as_bytes().to_vec(),
    };
    // The configured subaccount's positions refused: with a key that does not reach it, as
    // QFEX refused the listing to a key scoped to the primary account (decision 0144), and
    // unauthenticated. The primary account was read; nothing armed.
    for (status, body) in [
        (403, r#"{"detail":"insufficient API key permissions"}"#),
        (401, ""),
    ] {
        let mut positions = flat();
        positions.insert(Some(SUB), refused(status, body));
        let (ev, server) = stopped(
            "sub-positions",
            read_socket(0, EMPTY_PAGE),
            positions,
            |_| {},
            &format!(
                "subaccount could not be read: GET /user/positions: QFEX answered HTTP {status}"
            ),
        )
        .await;
        assert_eq!(ev.reads.len(), 1);
        assert_eq!(server.connections().len(), 1);
        assert_eq!(server.http_requests().len(), 2);
    }
    // The configured subaccount's read socket not authenticated: its positions were read, its
    // page never asked for, and nothing armed.
    let err = r#"{"type":"Err","error_code":"PermissionDenied","message":"no","incoming_message":"auth"}"#;
    let mut steps = read_socket(0, EMPTY_PAGE);
    steps.extend([
        Step::Accept,
        Step::Respond {
            conn: 1,
            with: reply(&[err]),
        },
    ]);
    let (ev, server) = stopped(
        "sub-auth",
        steps,
        flat(),
        |_| {},
        "subaccount could not be read: QFEX did not authenticate the read socket",
    )
    .await;
    assert_eq!(ev.reads.len(), 1);
    assert_eq!(received(&server, 1).len(), 1);
    // The primary account's read socket's authentication refused.
    let steps = vec![
        Step::Accept,
        Step::Respond {
            conn: 0,
            with: reply(&[err]),
        },
    ];
    stopped(
        "auth",
        steps,
        flat(),
        |_| {},
        "QFEX did not authenticate the read socket",
    )
    .await;
    // The page refused for its rate, answered in binary, closed, or never answered.
    let limited = r#"{"type":"Err","error_code":"RateLimited","message":"Rate limit exceeded, retry after 3 seconds","incoming_message":{"type":"get_user_orders","params":{"limit":1000,"offset":0}}}"#;
    stopped(
        "limited",
        read_socket(0, limited),
        flat(),
        |_| {},
        "get_user_orders: QFEX refused the page read: RateLimited",
    )
    .await;
    let mut steps = read_socket(0, EMPTY_PAGE);
    steps[2] = Step::Read { conn: 0 };
    steps.push(Step::Push {
        conn: 0,
        frame: Frame::Binary(EMPTY_PAGE.as_bytes().to_vec()),
    });
    stopped("binary", steps, flat(), |_| {}, "not a text frame").await;
    let mut steps = read_socket(0, EMPTY_PAGE);
    steps[2] = Step::Read { conn: 0 };
    steps.push(Step::Close { conn: 0 });
    stopped("closed", steps, flat(), |_| {}, "closed before it answered").await;
    let mut steps = read_socket(0, EMPTY_PAGE);
    steps[2] = Step::Read { conn: 0 };
    let fast = |o: &mut Options| o.read_wait = Duration::from_millis(300);
    stopped("silent", steps, flat(), fast, "no answer within 300 ms").await;
    // The trade socket's origin refuses connections.
    let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = closed.local_addr().unwrap().port();
    drop(closed);
    let gone = move |o: &mut Options| o.trade_url = format!("ws://127.0.0.1:{port}");
    stopped("no-socket", Vec::new(), flat(), gone, "the read socket:").await;
    // The REST origin refuses connections: the first request, the primary account's positions,
    // is not answered.
    let gone = move |o: &mut Options| o.rest_url = format!("http://127.0.0.1:{port}");
    stopped(
        "no-rest",
        Vec::new(),
        flat(),
        gone,
        "primary (armed) could not be read: GET /user/positions:",
    )
    .await;
}

// ----- The accounts the configuration names (FBC-5ph8, decision 0144) -----

#[tokio::test]
async fn the_probe_reads_only_the_accounts_its_configuration_names() {
    // Runs a complete stage one arming on the primary account under `env`, with `reads` read
    // sockets before its session: the outcome, what it printed, the stub and the report.
    let run = |what: &'static str, env: HashMap<&'static str, String>, reads: usize| async move {
        let (dir, _removed) = temp_dir(what);
        let mut steps = Vec::new();
        for conn in 0..reads {
            steps.extend(read_socket(conn, EMPTY_PAGE));
        }
        steps.extend(session_socket(reads, &[ARM_ACK]));
        let server = StubServer::start(WsScript::new(steps), rest(flat()))
            .await
            .unwrap();
        let opts = options(&dir, &server, &[Armed::Primary]);
        let (ran, printed) = probe(&opts, &env).await;
        let outcome = ran.unwrap();
        assert!(outcome.ok, "{what}: {printed}\n{:#?}", outcome.evidence);
        server.finished().await.unwrap();
        let report = fs::read_to_string(opts.evidence_dir.join("report.md")).unwrap();
        let evidence = json(&fs::read_to_string(opts.evidence_dir.join("evidence.json")).unwrap());
        assert_no_secret(&opts.evidence_dir);
        (outcome.evidence, printed, server, report, evidence)
    };

    // No subaccount named: the primary account alone is read before its session, and nothing
    // else is asked, the listing included.
    let mut env = environment();
    env.retain(|var, _| [PUBLIC_VAR, SECRET_VAR].contains(var));
    let (ev, printed, server, report, evidence) = run("primary-only", env, 1).await;
    assert_eq!(ev.accounts, ["primary"]);
    let labels: Vec<&str> = ev.reads.iter().map(|r| r.label.as_str()).collect();
    assert_eq!(labels, ["primary (armed)"]);
    assert_read_socket(&received(&server, 0), None, PUBLIC);
    assert_session_socket(&received(&server, 1), None, PUBLIC);
    assert_eq!(server.http_requests(), ["GET /user/positions"]);
    assert_no_listing(&server);
    assert!(
        report.contains(
            "Accounts named by the run's configuration, to be read before every session: 1 \
             (primary; ids not shown), not a list QFEX gave"
        ),
        "{report}"
    );
    assert_eq!(evidence["accounts"]["named"], json(r#"["primary"]"#));
    assert_eq!(evidence["accounts"]["count"], 1);
    assert!(printed.contains(
        "ACCOUNTS 1 named by the run's configuration, not listed by QFEX (decision 0144): primary"
    ));

    // A subaccount variable set but empty names no subaccount: the primary account alone is read.
    let mut env = environment();
    env.insert(SUB_ID_VAR, String::new());
    env.remove(SUB_PUBLIC_VAR);
    env.remove(SUB_SECRET_VAR);
    let (ev, printed, server, _, evidence) = run("empty-sub-id", env, 1).await;
    assert_eq!(ev.accounts, ["primary"]);
    let labels: Vec<&str> = ev.reads.iter().map(|r| r.label.as_str()).collect();
    assert_eq!(labels, ["primary (armed)"]);
    assert_eq!(server.http_requests(), ["GET /user/positions"]);
    assert_eq!(evidence["accounts"]["count"], 1);
    assert!(printed.contains("ACCOUNTS 1 named"), "{printed}");

    // A subaccount named with no pair of its own: it is read too, with the primary account's
    // credential, as before (decision 0144 item 1), though no session arms on it.
    let mut env = environment();
    env.remove(SUB_PUBLIC_VAR);
    env.remove(SUB_SECRET_VAR);
    let (ev, _, server, _, evidence) = run("sub-with-primary-key", env, 2).await;
    assert_eq!(ev.accounts, ["primary", "subaccount"]);
    let labels: Vec<&str> = ev.reads.iter().map(|r| r.label.as_str()).collect();
    assert_eq!(labels, ["primary (armed)", "subaccount"]);
    assert_read_socket(&received(&server, 0), None, PUBLIC);
    assert_read_socket(&received(&server, 1), Some(SUB), PUBLIC);
    assert_session_socket(&received(&server, 2), None, PUBLIC);
    assert_eq!(server.http_requests().len(), 2);
    assert_no_listing(&server);
    assert_eq!(evidence["accounts"]["count"], 2);
}

// ----- A session that never arms -----

#[tokio::test]
async fn a_session_that_never_arms_is_stopped_with_no_second_attempt() {
    let read_all = || {
        let mut steps = read_socket(0, EMPTY_PAGE);
        steps.extend(read_socket(1, EMPTY_PAGE));
        steps
    };
    let run = |what: &'static str, session: Vec<Step>, wait: u64| {
        let mut steps = read_all();
        steps.extend(session);
        async move {
            let (dir, _removed) = temp_dir(what);
            let server = StubServer::start(WsScript::new(steps), rest(flat()))
                .await
                .unwrap();
            let mut opts = options(&dir, &server, &[Armed::Primary]);
            opts.session_wait = Duration::from_secs(wait);
            let (ran, printed) = probe(&opts, &environment()).await;
            let outcome = ran.unwrap();
            assert!(!outcome.ok, "{printed}");
            tokio::time::sleep(Duration::from_millis(300)).await;
            // Two read sockets and the one session: no second attempt.
            assert_eq!(server.connections().len(), 3, "{what}");
            let report = fs::read_to_string(opts.evidence_dir.join("report.md")).unwrap();
            assert!(report.contains("Result: INCOMPLETE") && report.contains("No arm was sent"));
            assert_no_secret(&opts.evidence_dir);
            outcome.evidence.sessions[0].clone()
        }
    };
    // QFEX refuses the session's authentication: the codec ends the epoch, and the handler stops
    // the session there.
    let refusal = r#"{"type":"Err","error_code":"PermissionDenied","message":"no","incoming_message":{"type":"auth"}}"#;
    let refused = vec![
        Step::Accept,
        Step::Respond {
            conn: 2,
            with: reply(&[refusal]),
        },
    ];
    let session = run("auth-refused", refused, 30).await;
    assert!(!session.authenticated && !session.arm_sent && session.arm.is_none());
    assert_eq!(session.ended_by, "the connection's end");
    assert_eq!((session.attempts, session.failed_attempts), (1, 0));
    assert_eq!(session.frames.len(), 1);
    assert_eq!(session.frames[0].kind, "not a JSON object");
    // QFEX never answers it: the program stops the session at --session-wait.
    let silent = vec![Step::Accept, Step::Read { conn: 2 }];
    let session = run("auth-silent", silent, 1).await;
    assert!(!session.authenticated && !session.arm_sent);
    assert_eq!(session.ended_by, "--session-wait");
    assert_eq!(session.attempts, 1);
    assert!(session.frames.is_empty());
}

// ----- The capture's parts -----

#[test]
fn a_frame_is_named_by_what_it_echoes_or_its_members() {
    let arm = r#"{"type":"cancel_on_disconnect","params":{"cancel_on_disconnect":true}}"#;
    let cases = [
        ("2222", "not a JSON object"),
        ("[1]", "not a JSON object"),
        (ARM_ACK, "ack echoing the arm"),
        (
            &format!(r#"{{"ack":{{"incoming_message":{arm},"extra":1}}}}"#),
            "ack echoing the arm",
        ),
        (
            r#"{"ack":{"incoming_message":{"type":"get_order"}}}"#,
            "ack echoing another request",
        ),
        (r#"{"ack":{}}"#, "ack echoing nothing"),
        (r#"{"ack":"text"}"#, "{ack}"),
        (
            &format!(r#"{{"err":{{"error_code":"RateLimited","incoming_message":{arm}}}}}"#),
            "err echoing the arm (RateLimited)",
        ),
        (
            &format!(r#"{{"type":"Err","error_code":"2222","incoming_message":{arm}}}"#),
            "err echoing the arm (2222)",
        ),
        (OTHER_ERR, "err echoing another request (InvalidParameter)"),
        (r#"{"type":"Err","message":"x"}"#, "err echoing nothing"),
        (r#"{"ack":{},"err":{}}"#, "{ack, err}"),
        (r#"{"subscribed":["order_responses"]}"#, "{subscribed}"),
        ("{}", "{}"),
    ];
    for (text, kind) in cases {
        assert_eq!(capture::kind(text), kind, "{text}");
    }
}

#[test]
fn a_frame_that_echoes_no_request_keeps_its_shape_and_loses_its_numbers() {
    let text = r#"{"a":-1.5e-3,"b":[0, 12,true,null,false],"c":"x\"9\\","d":{"e":7E+2}}"#;
    assert_eq!(
        capture::zero_numbers(text),
        r#"{"a":0,"b":[0, 0,true,null,false],"c":"x\"9\\","d":{"e":0}}"#
    );
}

#[test]
fn what_the_capture_shows_of_the_arm() {
    let frame = |kind: &str, when| Arrived {
        kind: kind.to_owned(),
        when,
        file: String::new(),
    };
    let mut run = SessionRun {
        arm_sent: true,
        frames: vec![frame("err echoing the arm (InvalidOrder)", When::InWindow)],
        ..SessionRun::default()
    };
    assert!(run.answer().starts_with("An err echoing the arm"));
    // Frames in the window that echo nothing of the arm are listed by kind, once each; an echo
    // of the arm after the window is named late.
    run.frames = vec![
        frame("{order_response}", When::InWindow),
        frame("{position_response}", When::InWindow),
        frame("{order_response}", When::InWindow),
        frame("ack echoing another request", When::AfterWindow),
        Arrived {
            file: "primary-004-after-window.json".to_owned(),
            ..frame("ack echoing the arm", When::AfterWindow)
        },
    ];
    let answer = run.answer();
    assert!(
        answer.starts_with(
            "Frames arrived in the arm's window, but none echoed the arm: {order_response}; \
             {position_response}. This capture shows none of them"
        ),
        "{answer}"
    );
    assert!(
        answer.contains(
            "After the window, late: ack echoing the arm (primary-004-after-window.json). The \
             arm was echoed only after its deadline"
        ),
        "{answer}"
    );
    assert!(!answer.contains("another request"), "{answer}");
    // Nothing in the window: no reply before the deadline, and a late echo named if one came.
    run.frames.retain(|f| f.when == When::AfterWindow);
    let answer = run.answer();
    assert!(
        answer.starts_with("Nothing arrived in the arm's window: no reply came before")
            && answer.contains("late: ack echoing the arm"),
        "{answer}"
    );
    run.frames.clear();
    assert!(!run.answer().contains("After the window"));
    run.arm_sent = false;
    assert!(run.answer().starts_with("No arm was sent"));
    assert!(!run.ok());
}

// ----- Journal gaps (decision 0006) -----

/// What a test offers the journal's sink: a record under its class, or one withheld (`omit`),
/// which the sink drops and counts as it does a record it has no room for.
enum Offer {
    Keep(Record),
    Drop,
}

const WALL: WallNs = WallNs(1_762_000_000_000_000_000);
/// A time in the hour after `WALL`'s: its first record makes the writer roll its segment.
const NEXT_HOUR: WallNs = WallNs(WALL.0 + 3_600 * 1_000_000_000);
/// The UTC day `WALL` falls on, the journal's directory for it, and the first segment the
/// writer opens there.
const DAY_DIR: &str = "20251101";
const SEGMENT: &str = "0-000000.fbcj";

fn conn(n: u16) -> ConnKey {
    ConnKey { conn: n, epoch: 1 }
}

fn stamp(n: u16) -> Stamp {
    Stamp {
        ingest_seq: 0,
        kernel_rx: None,
        recv_mono: MonoNs(0),
        recv_wall: WALL,
        conn: conn(n),
    }
}

/// A session's records on connection `n`, as the runtime journals them: opened, the
/// authentication reply, the arm, `in_window`, the arm's deadline, closed.
fn session_records(n: u16, in_window: Vec<Offer>) -> Vec<Offer> {
    let mut offers = vec![
        Offer::Keep(Record::Control {
            at: MonoNs(0),
            ev: ControlEvent::Opened(conn(n)),
        }),
        Offer::Keep(Record::inbound(stamp(n), RawFrame::Text(AUTH_OK))),
        Offer::Keep(Record::Outbound {
            at: MonoNs(0),
            conn: conn(n),
            rpc: Some(RpcId(1)),
            opcode: Opcode::Text,
            frame: WireSlice::plain(ARM.as_bytes().to_vec()),
        }),
    ];
    offers.extend(in_window);
    offers.push(Offer::Keep(Record::RpcTimeout {
        stamp: stamp(n),
        rpc: RpcId(1),
    }));
    offers.push(Offer::Keep(Record::Control {
        at: MonoNs(0),
        ev: ControlEvent::Closed(conn(n)),
    }));
    offers
}

fn ack(n: u16) -> Offer {
    Offer::Keep(Record::inbound(stamp(n), RawFrame::Text(ARM_ACK)))
}

/// The probe's own sink and writer thread, as the run makes them, journaling under `dir`.
fn queue(dir: &Path) -> (QueueSink, WriterThread) {
    let key = Arc::new(RedactionKey::new(&[9; 32]).unwrap());
    let config = SinkConfig {
        budget_bytes: 16 << 20,
        soft_limit_pct: 90,
    };
    let (sink, drain) = journal_queue(config, Arc::clone(&key)).unwrap();
    let writer = JournalWriter::create(dir.join("journal"), 0, key).unwrap();
    (sink, drain.spawn(writer).unwrap())
}

/// Journals `offers` under `WALL` through the probe's own sink and writer thread under `dir`;
/// the sink is handed back with its writer closed.
fn journal(dir: &Path, offers: Vec<Offer>) -> QueueSink {
    let (mut sink, writing) = queue(dir);
    for offer in offers {
        let (recorded, kept) = match offer {
            Offer::Keep(record) => (sink.record(TrafficClass::Normal, WALL, &record), true),
            Offer::Drop => (sink.omit(TrafficClass::Normal, WALL), false),
        };
        assert_eq!(recorded == Recorded::Ok, kept);
    }
    writing.close().unwrap();
    sink
}

/// Journals `offers`, each under its wall time, as [`journal`] does, but with the writer's
/// first roll into a later hour failing: the name it compresses the closed segment into is
/// taken by a directory, so the writer thread stops there, as it does at any write error, and
/// every record the sink accepted after the last one written stays unwritten. The sink is
/// handed back with what closing the writer returned.
fn journal_until_the_roll_fails(
    dir: &Path,
    offers: Vec<(WallNs, Offer)>,
) -> (QueueSink, Result<(), JournalError>) {
    let day = dir.join("journal").join(DAY_DIR);
    fs::create_dir_all(day.join(format!("{SEGMENT}.zst.tmp"))).unwrap();
    let (mut sink, writing) = queue(dir);
    for (wall, offer) in offers {
        let _ = match offer {
            Offer::Keep(record) => sink.record(TrafficClass::Normal, wall, &record),
            Offer::Drop => sink.omit(TrafficClass::Normal, wall),
        };
    }
    let closed = writing.close();
    // The first hour's records were written, in the segment the failed roll left uncompressed.
    assert!(day.join(SEGMENT).is_file());
    (sink, closed)
}

/// Cuts the journal under `dir` inside its entry `n` (from 0, a marker counting as one), as a
/// write that failed part way leaves a segment: its length and two bytes of its body stay.
fn cut_inside(dir: &Path, n: usize) {
    let segment = dir.join("journal").join(DAY_DIR).join(SEGMENT);
    let bytes = fs::read(&segment).unwrap();
    // After the segment's header: its magic and its format version.
    let mut at = 6;
    for _ in 0..n {
        let len = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap());
        at += 4 + usize::try_from(len).unwrap();
    }
    fs::write(&segment, &bytes[..at + 4 + 2]).unwrap();
}

/// Two sessions, primary then subaccount, each as the probe reports one that armed once and
/// had its arm's outcome: what the journal shows is the slice's to add.
fn reported() -> Vec<SessionRun> {
    ["primary", "subaccount"]
        .map(|account| SessionRun {
            account,
            authenticated: true,
            arm: Some("Unknown".to_owned()),
            ended_by: "the arm's outcome",
            attempts: 1,
            ..SessionRun::default()
        })
        .to_vec()
}

/// The evidence of a run whose sessions were `sessions`, its sink having dropped `dropped`.
fn run_of(sessions: Vec<SessionRun>, dropped: [u64; 2]) -> Evidence {
    Evidence {
        started: "2026-11-06 00:00:00Z".to_owned(),
        target: "a loopback test stub",
        sessions,
        dropped,
        ..Evidence::default()
    }
}

/// The evidence of a run whose sessions were [`reported`] and whose journal is under `dir`, as
/// the run makes it: `sink`'s writer having closed as `closed` says (`stage::journal_closed`),
/// then the slice of the journal, which it hands back.
fn sliced(
    dir: &Path,
    sink: &QueueSink,
    closed: Result<(), JournalError>,
) -> (Evidence, Result<Vec<(String, String)>, String>) {
    let mut ev = run_of(reported(), [0, 0]);
    stage::journal_closed(&mut ev, closed, sink);
    let files = capture::slice(&dir.join("journal"), 0, &mut ev);
    (ev, files)
}

#[test]
fn a_degraded_marker_in_a_sessions_window_makes_its_capture_partial_and_the_run_incomplete() {
    let (dir, _removed) = temp_dir("gap-in-window");
    // The primary account's arm is acked; in the subaccount's window the sink drops a frame
    // (record 9), and the arm's deadline after it closes the gap behind a Degraded marker.
    let mut offers = session_records(0, vec![ack(0)]);
    offers.extend(session_records(1, vec![Offer::Drop]));
    let sink = journal(&dir, offers);
    let (ev, files) = sliced(&dir, &sink, Ok(()));
    files.unwrap();

    // The gap could hold only the subaccount's records: the primary account's capture is whole.
    let [primary, sub] = [&ev.sessions[0], &ev.sessions[1]];
    assert!(primary.partial.is_empty() && primary.ok(), "{primary:?}");
    assert!(primary.answer().starts_with("An ack echoing the arm"));
    assert_eq!(
        sub.partial,
        [
            "a journal gap, 1 dropped from its record 9 on (a Degraded marker, decision 0006), \
          could hold this session's records"
        ]
    );
    assert!(sub.arm_sent && !sub.ok(), "{sub:?}");
    // Its window looks empty, but nothing is concluded from it.
    assert!(sub.frames.iter().all(|f| f.when == When::BeforeArm));
    let answer = sub.answer();
    assert!(answer.starts_with("The capture is partial"), "{answer}");
    assert!(!answer.contains("Nothing arrived"), "{answer}");
    assert!(
        answer.contains("Echoes of the arm it kept: none."),
        "{answer}"
    );

    // The run, whose sink's counts agree, is incomplete, and its report and evidence name the
    // partial session and why.
    assert_eq!(ev.dropped, [1, 0]);
    assert!(!ev.ok());
    // Even counts the sink never reported could not make it complete.
    assert!(!run_of(ev.sessions.clone(), [0, 0]).ok());
    let report = capture::markdown(&ev);
    assert!(report.contains("Result: INCOMPLETE."), "{report}");
    assert!(report.contains("**The journal is incomplete** (decision 0006)"));
    assert!(report.contains("1 Normal, 0 Safety. Sessions whose capture is partial: subaccount."));
    assert!(report.contains("- Capture: PARTIAL: a journal gap, 1 dropped from its record 9 on"));
    assert!(!report.contains("Nothing arrived"), "{report}");
    // Its frames' counts of provider rate refusals may be short, and the report says so.
    assert!(report.contains(
        "| subaccount | the session (capture partial: its frames' counts may be short) | 0 | 0 | \
         0 | 0 | not seen | 0 | none |"
    ));
    let readme = capture::readme(&ev);
    assert!(
        readme.contains(
            "**This capture is partial** (decision 0006): the capture of subaccount is partial \
             (the run's report says why; the journal's sink dropped 1 Normal and 0 Safety \
             records)."
        ),
        "{readme}"
    );
    let evidence = capture::to_json(&ev);
    assert_eq!(evidence["ok"], false);
    assert_eq!(evidence["journal_dropped"]["normal"], 1);
    assert_eq!(evidence["journal_writer_failed"], Value::Null);
    assert_eq!(evidence["sessions"][0]["capture"], "whole");
    assert_eq!(evidence["sessions"][1]["capture"], "partial");
    assert_eq!(evidence["sessions"][1]["ok"], false);
    assert!(
        evidence["sessions"][1]["partial_because"][0]
            .as_str()
            .unwrap()
            .contains("from its record 9")
    );
}

#[test]
fn a_gap_is_placed_on_every_session_whose_records_it_could_hold() {
    // A gap opened in the primary account's window and closed only by the subaccount's first
    // record could hold either's: both are partial.
    let (dir, _removed) = temp_dir("gap-across");
    let mut offers = session_records(0, vec![Offer::Drop]);
    // Its deadline and close dropped too, so the gap is still open when the next session opens.
    let closing = offers.split_off(offers.len() - 2);
    offers.extend(closing.into_iter().map(|_| Offer::Drop));
    offers.extend(session_records(1, vec![ack(1)]));
    let sink = journal(&dir, offers);
    let (ev, files) = sliced(&dir, &sink, Ok(()));
    files.unwrap();
    for run in &ev.sessions {
        assert_eq!(run.partial.len(), 1, "{run:?}");
        assert!(run.partial[0].contains("3 dropped from its record 3 on"));
    }
    // A kept echo of the arm is named, with no position against the arm and no conclusion.
    let answer = ev.sessions[1].answer();
    assert!(answer.starts_with("The capture is partial"), "{answer}");
    assert!(
        answer.contains(
            "Echoes of the arm it kept: ack echoing the arm (position unverified, \
             subaccount-002-partial.json)."
        ),
        "{answer}"
    );
    assert!(!answer.contains("Final acceptance"), "{answer}");

    // A gap still open when the sink closes is marked last, by the writer: it could hold the
    // records of every session from the one it opened in to the last.
    let (dir, _removed) = temp_dir("gap-at-close");
    let mut offers = session_records(0, vec![ack(0)]);
    let mut sub = session_records(1, Vec::new());
    sub.truncate(3);
    sub.push(Offer::Drop);
    offers.extend(sub);
    let sink = journal(&dir, offers);
    let (ev, files) = sliced(&dir, &sink, Ok(()));
    files.unwrap();
    assert!(ev.sessions[0].partial.is_empty(), "{:?}", ev.sessions[0]);
    assert_eq!(ev.sessions[1].partial.len(), 1, "{:?}", ev.sessions[1]);
    assert!(ev.sessions[1].partial[0].contains("1 dropped from its record 9 on"));
}

#[test]
fn a_drop_the_journal_holds_no_marker_for_leaves_no_session_shown_whole() {
    // The clean case: every record kept, both sessions whole, the run complete.
    let (dir, _removed) = temp_dir("dropped-unmarked");
    let mut offers = session_records(0, vec![ack(0)]);
    offers.extend(session_records(1, vec![ack(1)]));
    let mut sink = journal(&dir, offers);
    let (ev, files) = sliced(&dir, &sink, Ok(()));
    files.unwrap();
    assert!(
        ev.sessions
            .iter()
            .all(|run| run.partial.is_empty() && run.ok())
    );
    assert_eq!(ev.dropped, [0, 0]);
    assert!(ev.ok(), "{ev:#?}");
    let report = capture::markdown(&ev);
    assert!(report.contains("Result: complete."), "{report}");
    assert!(
        report.contains("dropped: 0 Normal, 0 Safety. Sessions whose capture is partial: none.")
    );
    assert!(!report.contains("The journal is incomplete"));
    assert_eq!(report.matches("- Capture: whole").count(), 2);
    assert!(!capture::readme(&ev).contains("partial"));

    // A record offered once the sink closed is dropped and counted, and no marker reaches the
    // journal for it. The slice reads no gap, but the markers it read account for fewer drops
    // than the sink counted: a drop no marker shows could be any session's, so each is partial.
    assert_eq!(
        sink.omit(TrafficClass::Safety, WALL),
        Recorded::DroppedCounted
    );
    let (ev, files) = sliced(&dir, &sink, Ok(()));
    files.unwrap();
    assert_eq!(ev.dropped, [0, 1]);
    for run in &ev.sessions {
        assert_eq!(
            run.partial,
            [
                "records dropped: 1 by the journal's sink's count, 0 by its Degraded markers \
                 (decision 0006), so a drop no marker shows could hold this session's records"
            ],
            "{run:?}"
        );
        assert!(run.answer().starts_with("The capture is partial"));
    }
    assert!(!ev.ok());
    let report = capture::markdown(&ev);
    assert!(report.contains("Result: INCOMPLETE."), "{report}");
    assert!(report.contains("**The journal is incomplete** (decision 0006)"));
    assert!(report.contains(
        "dropped: 0 Normal, 1 Safety. Sessions whose capture is partial: primary; subaccount."
    ));
    assert!(!report.contains("partial: none"), "{report}");
    assert!(!report.contains("- Capture: whole"), "{report}");
    assert!(
        !report.contains("An ack echoing the arm arrived"),
        "{report}"
    );
    assert_eq!(capture::to_json(&ev)["journal_dropped"]["safety"], 1);
    assert!(capture::readme(&ev).contains("**This capture is partial**"));
}

#[test]
fn a_writer_stopped_by_a_failed_roll_leaves_the_sessions_it_could_hold_partial() {
    let (dir, _removed) = temp_dir("writer-stops");
    // The primary account's session, then the subaccount's opening, authentication and arm, in
    // this hour; QFEX's ack of that arm, its deadline and its close in the next, which the
    // writer stops at before writing any. The sink accepted every record: it drops none.
    let mut offers: Vec<(WallNs, Offer)> = session_records(0, vec![ack(0)])
        .into_iter()
        .map(|offer| (WALL, offer))
        .collect();
    let mut sub = session_records(1, vec![ack(1)]);
    let later = sub.split_off(3);
    offers.extend(sub.into_iter().map(|offer| (WALL, offer)));
    offers.extend(later.into_iter().map(|offer| (NEXT_HOUR, offer)));
    let (sink, closed) = journal_until_the_roll_fails(&dir, offers);
    assert!(closed.is_err());
    let mut ev = run_of(reported(), [0, 0]);
    stage::journal_closed(&mut ev, closed, &sink);
    assert_eq!(ev.dropped, [0, 0]);
    assert!(ev.writer_failed.is_some(), "{ev:?}");
    assert!(ev.errors[0].starts_with("the journal's writer: "), "{ev:?}");
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/privacy-check.sh");
    stage::write_slice(&dir, &mut ev, &script).unwrap();
    let (report, evidence) = capture::outputs(&mut ev);

    // Every record written precedes the subaccount's arm, so whatever the writer did not write
    // came after it: the primary account's capture is whole and its ack stands.
    let [primary, sub] = [&ev.sessions[0], &ev.sessions[1]];
    assert!(primary.partial.is_empty(), "{primary:?}");
    assert!(primary.answer().starts_with("An ack echoing the arm"));
    // The subaccount's arm was written, and what answered it is in no file: its capture is
    // partial, and nothing is concluded from its empty window.
    assert!(sub.arm_sent, "{sub:?}");
    assert_eq!(
        sub.partial,
        [
            "the journal's writer stopped at an error, so the records the sink accepted after \
             the last one it wrote are in no file, neither counted nor marked as dropped \
             (decision 0006), and could be this session's"
        ]
    );
    let answer = sub.answer();
    assert!(answer.starts_with("The capture is partial"), "{answer}");
    assert!(!answer.contains("Nothing arrived"), "{answer}");
    assert!(sub.frames.iter().all(|f| f.file.ends_with("-partial.json")));

    assert!(report.contains("Result: INCOMPLETE."), "{report}");
    assert!(report.contains("**The journal is incomplete** (decision 0006)"));
    assert!(report.contains("0 Normal, 0 Safety. Sessions whose capture is partial: subaccount."));
    assert_eq!(report.matches("- Capture: whole").count(), 1, "{report}");
    assert!(report.contains("- Capture: PARTIAL: the journal's writer stopped at an error"));
    assert!(!report.contains("Nothing arrived"), "{report}");
    let evidence = json(&evidence);
    assert_eq!(evidence["sessions"][0]["capture"], "whole");
    assert_eq!(evidence["sessions"][1]["capture"], "partial");
    assert!(evidence["journal_writer_failed"].is_string());
    let readme = fs::read_to_string(dir.join("fixtures/README.md")).unwrap();
    assert!(
        readme.contains("**This capture is partial** (decision 0006): the capture of subaccount"),
        "{readme}"
    );
    assert!(
        !readme.contains("so whatever answered the arm is here"),
        "{readme}"
    );
    assert!(dir.join("fixtures/subaccount-001-partial.json").is_file());
    assert!(dir.join("fixtures/primary-002-in-window.json").is_file());
}

#[test]
fn drops_after_the_writer_stopped_reach_no_marker_and_no_session_is_shown_whole() {
    let (dir, _removed) = temp_dir("writer-stops-drops");
    let mut offers: Vec<(WallNs, Offer)> = session_records(0, vec![ack(0)])
        .into_iter()
        .map(|offer| (WALL, offer))
        .collect();
    let mut sub = session_records(1, Vec::new());
    let later = sub.split_off(3);
    offers.extend(sub.into_iter().map(|offer| (WALL, offer)));
    // In the next hour the writer stops at its first record. The queue, which nothing drains
    // now, fills with frames of the subaccount's window, more than the soft limit holds: the
    // sink drops what has no room, counted, and no marker of it reaches the journal.
    let pad = format!("{{\"pad\":\"{}\"}}", "x".repeat(1 << 20));
    for _ in 0..16 {
        let frame = Record::inbound(stamp(1), RawFrame::Text(&pad));
        offers.push((NEXT_HOUR, Offer::Keep(frame)));
    }
    offers.push((NEXT_HOUR, ack(1)));
    offers.extend(later.into_iter().map(|offer| (NEXT_HOUR, offer)));
    let (sink, closed) = journal_until_the_roll_fails(&dir, offers);
    let (ev, files) = sliced(&dir, &sink, closed);
    files.unwrap();
    assert!(ev.dropped[0] > 0, "{:?}", ev.dropped);
    assert!(ev.writer_failed.is_some());

    // The markers read account for none of the drops, which could then be any session's, the
    // primary account's included; and the writer stopped in the subaccount's.
    let unmarked = format!(
        "records dropped: {} by the journal's sink's count, 0 by its Degraded markers",
        ev.dropped[0]
    );
    let [primary, sub] = [&ev.sessions[0], &ev.sessions[1]];
    assert_eq!(primary.partial.len(), 1, "{primary:?}");
    assert!(primary.partial[0].starts_with(&unmarked), "{primary:?}");
    assert_eq!(sub.partial.len(), 2, "{sub:?}");
    assert!(sub.partial[0].starts_with("the journal's writer stopped at an error"));
    assert!(sub.partial[1].starts_with(&unmarked));
    for run in &ev.sessions {
        assert!(
            run.answer().starts_with("The capture is partial"),
            "{run:?}"
        );
    }
    let report = capture::markdown(&ev);
    assert!(report.contains("**The journal is incomplete** (decision 0006)"));
    assert!(
        report.contains("Sessions whose capture is partial: primary; subaccount."),
        "{report}"
    );
    assert!(!report.contains("partial: none"), "{report}");
    assert!(!report.contains("- Capture: whole"), "{report}");
    assert!(!report.contains("Nothing arrived"), "{report}");
    assert!(
        !report.contains("An ack echoing the arm arrived"),
        "{report}"
    );
    let readme = capture::readme(&ev);
    assert!(
        readme.contains("the capture of primary; subaccount is partial"),
        "{readme}"
    );
}

#[test]
fn a_journal_cut_inside_a_record_leaves_every_session_from_the_cut_on_partial() {
    // A gap in the subaccount's window, its marker written before the arm's deadline, and the
    // journal cut inside that deadline (entry 10: nine records, then the marker). The slice
    // stops at the cut with the gap it read still open.
    let (dir, _removed) = temp_dir("cut-after-marker");
    let mut offers = session_records(0, vec![ack(0)]);
    offers.extend(session_records(1, vec![Offer::Drop]));
    let sink = journal(&dir, offers);
    cut_inside(&dir, 10);
    let (ev, files) = sliced(&dir, &sink, Ok(()));
    let err = files.unwrap_err();
    assert!(err.starts_with("the journal: "), "{err}");
    let [primary, sub] = [&ev.sessions[0], &ev.sessions[1]];
    assert!(primary.partial.is_empty() && primary.ok(), "{primary:?}");
    assert_eq!(sub.partial.len(), 2, "{sub:?}");
    assert!(sub.partial[0].starts_with("a journal gap, 1 dropped from its record 9 on"));
    assert!(
        sub.partial[1].starts_with("the journal could not be read to its end (the journal: "),
        "{sub:?}"
    );
    assert!(sub.answer().starts_with("The capture is partial"));
    let report = capture::markdown(&ev);
    assert!(report.contains("Sessions whose capture is partial: subaccount."));
    assert!(!report.contains("Nothing arrived"), "{report}");

    // A clean journal cut inside the subaccount's authentication reply (entry 7): its arm is
    // never read, and the session is partial, never one whose arm was not sent.
    let (dir, _removed) = temp_dir("cut-before-arm");
    let mut offers = session_records(0, vec![ack(0)]);
    offers.extend(session_records(1, vec![ack(1)]));
    let sink = journal(&dir, offers);
    cut_inside(&dir, 7);
    let (ev, files) = sliced(&dir, &sink, Ok(()));
    assert!(files.is_err());
    let [primary, sub] = [&ev.sessions[0], &ev.sessions[1]];
    assert!(primary.partial.is_empty() && primary.ok(), "{primary:?}");
    assert!(!sub.arm_sent && sub.frames.is_empty(), "{sub:?}");
    assert_eq!(sub.partial.len(), 1, "{sub:?}");
    assert!(sub.partial[0].starts_with("the journal could not be read to its end"));
    let answer = sub.answer();
    assert!(answer.starts_with("The capture is partial"), "{answer}");
    assert!(!answer.contains("No arm was sent"), "{answer}");
    let report = capture::markdown(&ev);
    assert!(report.contains("Sessions whose capture is partial: subaccount."));
    assert!(report.contains("- Arm written: not shown (the capture is partial)"));
    assert!(!report.contains("No arm was sent"), "{report}");
    assert_eq!(
        capture::to_json(&ev)["sessions"][1]["arm_sent"],
        Value::Null
    );
    assert_eq!(capture::to_json(&ev)["sessions"][0]["arm_sent"], true);
}

#[test]
fn a_partial_sessions_frames_are_named_and_listed_with_no_position_against_its_arm() {
    // The subaccount's arm deadline is dropped, and QFEX's ack after it closes the gap: read
    // back, the ack falls in a window nothing ended. The gap could hold that deadline, so no
    // position is stated: not in its file's name, its listing, the evidence or the answer.
    let (dir, _removed) = temp_dir("no-position");
    let mut offers = session_records(0, vec![ack(0)]);
    let mut sub = session_records(1, Vec::new());
    let closed = sub.pop().unwrap();
    let deadline = sub.pop().unwrap();
    assert!(matches!(deadline, Offer::Keep(Record::RpcTimeout { .. })));
    sub.extend([Offer::Drop, ack(1), closed]);
    offers.extend(sub);
    let sink = journal(&dir, offers);
    let (mut ev, files) = sliced(&dir, &sink, Ok(()));
    let files = files.unwrap();
    let names: Vec<&str> = files.iter().map(|(file, _)| file.as_str()).collect();
    assert_eq!(
        names,
        [
            "primary-001-before-arm.json",
            "primary-002-in-window.json",
            "subaccount-001-partial.json",
            "subaccount-002-partial.json",
        ]
    );
    let sub = &ev.sessions[1];
    assert_eq!(sub.partial.len(), 1, "{sub:?}");
    // As read, the ack falls in the window: the position the capture does not state.
    assert_eq!(sub.frames[1].when, When::InWindow);
    let answer = sub.answer();
    assert!(
        answer.contains(
            "Echoes of the arm it kept: ack echoing the arm (position unverified, \
             subaccount-002-partial.json)."
        ),
        "{answer}"
    );
    assert!(!answer.contains("in-window"), "{answer}");
    let report = capture::markdown(&ev);
    assert!(
        report.contains(
            "| 2 | position unverified | ack echoing the arm | subaccount-002-partial.json |"
        ),
        "{report}"
    );
    assert!(
        report.contains("| 2 | in-window | ack echoing the arm | primary-002-in-window.json |")
    );
    assert!(!report.contains("subaccount-002-in-window"), "{report}");
    let evidence = capture::to_json(&ev);
    assert_eq!(evidence["sessions"][0]["frames"][1]["when"], "in-window");
    assert_eq!(
        evidence["sessions"][1]["frames"][1]["when"],
        "position unverified"
    );
    capture::write_fixtures(&dir.join("fixtures"), files, &mut ev).unwrap();
    let readme = fs::read_to_string(dir.join("fixtures/README.md")).unwrap();
    assert!(
        readme.contains("Each of their files is named `<account>-<n>-partial.json`"),
        "{readme}"
    );
    assert!(dir.join("fixtures/subaccount-002-partial.json").is_file());
}

#[test]
fn a_gap_placed_on_no_session_is_never_shown_beside_a_whole_capture() {
    // The slice places what it finds of every gap, so only a run with no session, or one whose
    // journal was never sliced, has a gap on no session. Its report never says, beside the
    // gap, that no capture is partial.
    let ev = run_of(Vec::new(), [2, 0]);
    let report = capture::markdown(&ev);
    assert!(
        report.contains("**The journal is incomplete** (decision 0006)"),
        "{report}"
    );
    assert!(
        report.contains(
            "Sessions whose capture is partial: the drops could not be placed on a session; no \
             session's capture is shown whole."
        ),
        "{report}"
    );
    assert!(!report.contains("partial: none"), "{report}");
    let readme = capture::readme(&ev);
    assert!(
        readme.contains(
            "dropped 2 Normal and 0 Safety records, and the drops could not be placed on a \
             session; no session's capture is shown whole."
        ),
        "{readme}"
    );
    // Sessions it was never placed on are each shown partial for it, with no conclusion and no
    // position.
    let mut sessions = reported();
    sessions[0].arm_sent = true;
    sessions[0].frames.push(Arrived {
        kind: "ack echoing the arm".to_owned(),
        when: When::InWindow,
        file: "primary-001-in-window.json".to_owned(),
    });
    let ev = run_of(sessions, [0, 1]);
    let report = capture::markdown(&ev);
    assert!(
        report.contains("Sessions whose capture is partial: primary; subaccount."),
        "{report}"
    );
    assert!(!report.contains("- Capture: whole"), "{report}");
    assert!(
        !report.contains("An ack echoing the arm arrived"),
        "{report}"
    );
    assert!(report.contains("| 1 | position unverified | ack echoing the arm |"));
    let evidence = capture::to_json(&ev);
    assert_eq!(evidence["sessions"][0]["capture"], "partial");
    assert_eq!(evidence["sessions"][0]["arm_sent"], true);
    assert_eq!(evidence["sessions"][1]["arm_sent"], Value::Null);
}

#[test]
fn only_a_top_level_echo_keeps_its_numbers_and_the_strings_kept_are_named() {
    // A top-level envelope echoing a request is kept as the codec's spans left it.
    let ack = r#"{"ack":{"incoming_message":{"type":"x","n":222}}}"#;
    assert_eq!(capture::scrub(ack), ack);
    let flat = r#"{"type":"Err","error_code":"x","code":12,"incoming_message":"2222"}"#;
    assert_eq!(capture::scrub(flat), flat);
    // Any other frame loses its numbers, one with an incoming_message nested below the top
    // level, an envelope echoing nothing, or text that is not JSON included.
    let nested = r#"{"order_response":{"price":12.5,"incoming_message":{"type":"x"}}}"#;
    assert_eq!(
        capture::scrub(nested),
        r#"{"order_response":{"price":0,"incoming_message":{"type":"x"}}}"#
    );
    let bare = r#"{"ack":{"balance":7}}"#;
    assert_eq!(capture::scrub(bare), r#"{"ack":{"balance":0}}"#);
    assert_eq!(capture::scrub("x 12"), "x 0");
    // The members whose string value is kept: at any depth, an array's under its member, a
    // span of the codec's (all `2`s) or an empty string left out, and nothing from non-JSON.
    let text = r#"{"a":"x","b":{"c":"y","d":"2222","e":""},"f":["z",1],"g":[{"h":"w"}],"i":5}"#;
    let kept: Vec<String> = capture::kept_strings(text).into_iter().collect();
    assert_eq!(kept, ["a", "c", "f", "h"]);
    assert_eq!(capture::kept_strings(r#""top""#).len(), 1);
    assert!(capture::kept_strings("not json").is_empty());
}

#[test]
fn a_file_the_privacy_patterns_find_something_in_is_withheld() {
    let (dir, _removed) = temp_dir("withheld");
    let mut ev = Evidence {
        started: "2026-11-05 00:00:00Z".to_owned(),
        target: "a loopback test stub",
        ..Evidence::default()
    };
    let files = vec![
        ("kept.json".to_owned(), "{}".to_owned()),
        (
            "long.json".to_owned(),
            format!(r#"{{"x":"{}"}}"#, "2".repeat(64)),
        ),
    ];
    capture::write_fixtures(&dir.join("fixtures"), files, &mut ev).unwrap();
    assert_eq!(ev.fixtures, ["kept.json", "README.md"]);
    assert_eq!(ev.withheld, ["long.json: a bare run of 64 hex digits"]);
    assert!(!dir.join("fixtures/long.json").exists());
    assert!(!ev.ok());

    // A stop reason the patterns find something in withholds the report and the evidence.
    ev.stopped = Some(format!("{}{}", "ey", "J0b2tlbiI6InN5bnRoZXRpYyJ9"));
    let (report, json) = capture::outputs(&mut ev);
    assert!(report.starts_with("withheld:") && json.starts_with("withheld:"));
    assert_eq!(ev.errors.len(), 2, "{:?}", ev.errors);
}

#[test]
fn fixtures_the_privacy_check_cannot_pass_are_removed() {
    let (dir, _removed) = temp_dir("unchecked");
    fs::create_dir_all(dir.join("journal")).unwrap();
    let mut ev = Evidence::default();
    let missing = dir.join("no-such-check.sh");
    let err = stage::write_slice(&dir, &mut ev, &missing).unwrap_err();
    assert!(err.contains("the fixtures directory was removed"), "{err}");
    assert!(!dir.join("fixtures").exists());
    assert!(ev.fixtures.is_empty());
}

// ----- The command line -----

/// The ledger's three flags, which every run needs (decision 0132 item 5): a span bound just
/// longer than the defaults' 10 + 60 + 10 s to the arm's deadline, an age a second longer.
const LEDGER: [&str; 6] = [
    "--registry-span",
    "81",
    "--ledger-max-age",
    "82",
    "--ledger-max-entries",
    "1",
];

#[test]
fn the_command_line_takes_each_flag_and_refuses_without_showing_a_value() {
    let parse = |args: &[&str]| probe_args::parse(args.iter().map(|a| (*a).to_owned()));
    assert_eq!(parse(&["--help"]), Ok(Parsed::Help));
    let least = ["--evidence-dir", "/abs/dir", "--arm", "primary"];
    let Ok(Parsed::Run(opts)) = parse(&[&least[..], &LEDGER[..]].concat()) else {
        panic!("not parsed");
    };
    assert_eq!(opts.arms, [Armed::Primary]);
    assert_eq!(opts.trade_url, "wss://trade.qfex.io");
    // One provider user per process, the program's own numbering (decision 0123).
    assert_eq!(opts.rate_user, probe_args::USER);
    assert_eq!((opts.rpc_timeout_ms, opts.page_size), (10_000, 1000));
    let all = [
        "--evidence-dir",
        "/abs/dir",
        "--arm",
        "subaccount",
        "--arm",
        "primary",
        "--rpc-timeout-ms",
        "501",
        "--page-size",
        "1",
        "--read-wait-ms",
        "7",
        "--connect-wait-ms",
        "8",
        "--session-wait",
        "61",
        "--trade-url",
        "ws://127.0.0.1:1",
        "--rest-url",
        "http://127.0.0.1:2",
        "--mds-url",
        "ws://127.0.0.1:3",
        "--socks5",
        "[::1]:1080",
        "--registry-span",
        "61",
        "--ledger-max-age",
        "604800",
        "--ledger-max-entries",
        "1000000",
    ];
    let Ok(Parsed::Run(opts)) = parse(&all) else {
        panic!("not parsed");
    };
    let largest = ledger::Bounds::new(
        Duration::from_secs(604_800),
        1_000_000,
        Duration::from_secs(61),
        &[],
    );
    assert_eq!(opts.ledger, largest.unwrap());
    assert_eq!(opts.arms, [Armed::Subaccount, Armed::Primary]);
    assert_eq!((opts.rpc_timeout_ms, opts.page_size), (501, 1));
    assert_eq!(opts.read_wait, Duration::from_millis(7));
    assert_eq!(opts.connect_wait, Duration::from_millis(8));
    assert_eq!(opts.session_wait, Duration::from_secs(61));
    assert_eq!(
        (
            opts.trade_url.as_str(),
            opts.rest_url.as_str(),
            opts.mds_url.as_str()
        ),
        ("ws://127.0.0.1:1", "http://127.0.0.1:2", "ws://127.0.0.1:3")
    );
    let proxy = ProxyConfig::Socks5 {
        host: "::1".to_owned(),
        port: 1080,
    };
    assert_eq!(opts.proxy, proxy);
    let refused = [
        (vec!["--arm", "primary"], "--evidence-dir is required"),
        (vec!["--evidence-dir", "/d"], "--arm is required"),
        (vec!["--evidence-dir", "relative"], "not an absolute path"),
        (vec!["--arm", "secret-ish"], "primary or subaccount"),
        (vec!["--arm", "primary", "--arm", "primary"], "given twice"),
        (vec!["--rpc-timeout-ms", "500"], "from 501 to 60000"),
        (vec!["--page-size", "1001"], "from 1 to 1000"),
        (vec!["--read-wait-ms", "0"], "from 1 to 120000"),
        (vec!["--session-wait", "x"], "from 1 to 3600"),
        (vec!["--socks5", "host"], "not host:port"),
        (vec!["--socks5", "host:0"], "not host:port"),
        (vec!["--socks5", "a b:1"], "not host:port"),
        (vec!["--trade-url"], "--trade-url needs a value"),
        (vec!["--rest-url", "--arm"], "--rest-url needs a value"),
        (vec!["--secret-ish"], "an unknown argument"),
    ];
    for (args, expected) in refused {
        let err = parse(&args).unwrap_err();
        assert!(err.contains(expected), "{args:?}: {err}");
        assert!(
            !err.contains("secret-ish") && !err.contains("relative"),
            "{err}"
        );
    }
    // --session-wait must outlast the connection, QFEX's 60 s authentication deadline and the
    // arm's: 10 + 60 + 10 s at the defaults, 120 + 60 + 60 s at the largest.
    let run = [
        "--evidence-dir",
        "/d",
        "--arm",
        "primary",
        "--registry-span",
        "300",
        "--ledger-max-age",
        "301",
        "--ledger-max-entries",
        "1",
    ];
    let waits = [
        (vec!["--session-wait", "80"], false),
        (vec!["--session-wait", "81"], true),
        (
            vec!["--rpc-timeout-ms", "20000", "--session-wait", "90"],
            false,
        ),
        (
            vec![
                "--connect-wait-ms",
                "120000",
                "--rpc-timeout-ms",
                "60000",
                "--session-wait",
                "240",
            ],
            false,
        ),
        (
            vec![
                "--session-wait",
                "241",
                "--connect-wait-ms",
                "120000",
                "--rpc-timeout-ms",
                "60000",
            ],
            true,
        ),
    ];
    for (args, taken) in waits {
        let args: Vec<&str> = run.iter().copied().chain(args).collect();
        match parse(&args) {
            Ok(Parsed::Run(_)) if taken => {}
            Err(err) if !taken => {
                assert!(err.contains("--session-wait must be longer than"), "{err}");
                assert!(err.contains("the values are not shown"), "{err}");
            }
            other => panic!("{args:?}: {other:?}"),
        }
    }
    let usage = probe_args::usage();
    for flag in [
        "--evidence-dir",
        "--arm",
        "--session-wait",
        "--registry-span",
        "--ledger-max-age",
        "--ledger-max-entries",
        PUBLIC_VAR,
        SUB_SECRET_VAR,
    ] {
        assert!(usage.contains(flag), "{flag}");
    }
    // Its opening says what is refused before anything connects, the ledger's bounds included,
    // and every stop, the span bound's included.
    let flat = usage.split_whitespace().collect::<Vec<_>>().join(" ");
    for said in [
        "a missing or unaccepted --registry-span, --ledger-max-age or --ledger-max-entries: \
         fill-ledger bounds that cannot outlast a registry's sessions and fills",
        "at the arm's answer, its deadline, the connection's end, --session-wait or its \
         registry's span bound the program stops it, through its stop control",
    ] {
        assert!(flat.contains(said), "{said}");
    }
}

#[test]
fn the_production_flag_selects_production_probe_and_its_origins_as_the_defaults() {
    let parse = |args: &[&str]| {
        let least = ["--evidence-dir", "/abs/dir", "--arm", "primary"];
        let argv = [&least[..], &LEDGER[..], args].concat();
        match probe_args::parse(argv.iter().map(|a| (*a).to_owned())) {
            Ok(Parsed::Run(opts)) => *opts,
            other => panic!("{args:?}: {other:?}"),
        }
    };
    let origins = |o: &Options| [o.trade_url.clone(), o.rest_url.clone(), o.mds_url.clone()];
    // Without it nothing changes: the pre-production mode and origins (decision 0143 item 3).
    let opts = parse(&[]);
    assert!(!opts.production);
    assert_eq!(opts.environment(), Environment::PreProduction);
    assert_eq!(origins(&opts), [PREPROD_TRADE, PREPROD_REST, PREPROD_MDS]);
    // With it, the production mode and origins.
    let opts = parse(&["--production"]);
    assert!(opts.production);
    assert_eq!(opts.environment(), Environment::Production);
    assert_eq!(
        origins(&opts),
        [PRODUCTION_TRADE, PRODUCTION_REST, PRODUCTION_MDS]
    );
    // An origin given is taken, before or after the flag; the adapter's guard judges it.
    for args in [
        ["--trade-url", "ws://127.0.0.1:1", "--production"],
        ["--production", "--trade-url", "ws://127.0.0.1:1"],
    ] {
        let opts = parse(&args);
        assert!(opts.production, "{args:?}");
        assert_eq!(
            origins(&opts),
            ["ws://127.0.0.1:1", PRODUCTION_REST, PRODUCTION_MDS]
        );
    }
    let opts = parse(&["--mds-url", "ws://127.0.0.1:3"]);
    assert_eq!(
        origins(&opts),
        [PREPROD_TRADE, PREPROD_REST, "ws://127.0.0.1:3"]
    );
    // --help names the flag, both environments' defaults and what it refuses.
    let usage = probe_args::usage();
    let flat = usage.split_whitespace().collect::<Vec<_>>().join(" ");
    for said in [
        "--production",
        "production_probe",
        PRODUCTION_TRADE,
        PRODUCTION_REST,
        PRODUCTION_MDS,
        PREPROD_TRADE,
        "without --production, a production origin is refused; with it, a pre-production one",
        "on pre-production a declared testnet run (decision 0067), on production with no such \
         declaration (decision 0143)",
        "or as fixtures/qfex/exec-recorded/arm-production/ with --production",
    ] {
        assert!(flat.contains(said), "{said}: {usage}");
    }
}

#[test]
fn the_report_names_each_environment_and_each_side_of_the_guard() {
    assert_eq!(
        stage::environment(Environment::PreProduction),
        "pre-production"
    );
    assert_eq!(stage::environment(Environment::Production), "production");
    assert_eq!(stage::target(Origins::PreProduction), "QFEX pre-production");
    assert_eq!(stage::target(Origins::Production), "QFEX production");
    assert_eq!(stage::target(Origins::Loopback), "a loopback test stub");
}

// ----- The fill ledger's bounds (FBC-5hif, decision 0132 item 5) -----

#[test]
fn the_ledger_flags_are_required_and_a_bound_that_cannot_outlast_the_registry_is_refused_naming_its_flag()
 {
    let run = ["--evidence-dir", "/d", "--arm", "primary"];
    let with = |extra: &[&str]| -> Vec<String> {
        let mut args: Vec<&str> = run.to_vec();
        args.extend_from_slice(extra);
        args.into_iter().map(str::to_owned).collect()
    };
    let refused = |args: Vec<String>, expected: &str| {
        let err = probe_args::parse(args.clone()).unwrap_err();
        assert!(err.contains(expected), "{args:?}: {err}");
        // No value typed is shown: the ledger values below are all distinctive.
        for typed in ["777", "778", "4242", "x7", "-9"] {
            assert!(!err.contains(typed), "{args:?}: {err}");
        }
    };
    // Each flag missing, the others given: it has no default.
    let span = ["--registry-span", "777"];
    let age = ["--ledger-max-age", "778"];
    let count = ["--ledger-max-entries", "4242"];
    refused(
        with(&[&age[..], &count[..]].concat()),
        "--registry-span is required",
    );
    refused(
        with(&[&span[..], &count[..]].concat()),
        "--ledger-max-age is required",
    );
    refused(
        with(&[&span[..], &age[..]].concat()),
        "--ledger-max-entries is required",
    );
    // Each one zero, malformed or past its range, naming the flag.
    for flag in [
        "--registry-span",
        "--ledger-max-age",
        "--ledger-max-entries",
    ] {
        for value in ["0", "x7", "-9", "99999999999999999999"] {
            refused(
                with(&[flag, value]),
                &format!("{flag}: not a whole number from 1 to"),
            );
        }
    }
    refused(with(&["--registry-span", "86401"]), "from 1 to 86400");
    refused(with(&["--ledger-max-age", "604801"]), "from 1 to 604800");
    refused(
        with(&["--ledger-max-entries", "1000001"]),
        "from 1 to 1000000",
    );
    // A flag given no value.
    refused(
        with(&["--ledger-max-age"]),
        "--ledger-max-age needs a value",
    );
    // The age must be longer than the span bound: equal or shorter is refused.
    for age in ["777", "100"] {
        refused(
            with(&[&span[..], &["--ledger-max-age", age], &count[..]].concat()),
            "--ledger-max-age must be longer than --registry-span",
        );
    }
    // The span bound stops a session, so it must outlast the arm as --session-wait does: 10 +
    // 60 + 10 s at the defaults.
    refused(
        with(&[
            "--registry-span",
            "80",
            "--ledger-max-age",
            "778",
            "--ledger-max-entries",
            "1",
        ]),
        "--registry-span must be longer than --connect-wait-ms",
    );
    let Ok(Parsed::Run(opts)) = probe_args::parse(with(&[
        "--registry-span",
        "81",
        "--ledger-max-age",
        "82",
        "--ledger-max-entries",
        "1",
    ])) else {
        panic!("not parsed");
    };
    let taken = ledger::Bounds::new(Duration::from_secs(82), 1, Duration::from_secs(81), &[]);
    assert_eq!(opts.ledger, taken.unwrap());
    assert_eq!(opts.ledger.config().max_age, Duration::from_secs(82));
    assert_eq!(opts.ledger.config().max_entries, 1);

    // The count must be above the fill ceiling: stage one's is zero, so every count the flag
    // takes is above it; over synthetic place limits whose ceiling is 7, a count of 7 is
    // refused and 8 taken.
    let places = [
        ledger::MayPlace {
            what: "far post-only GTC",
            orders: 1,
            largest_steps: 1,
        },
        ledger::MayPlace {
            what: "taker IOC",
            orders: 2,
            largest_steps: 3,
        },
    ];
    let secs = Duration::from_secs;
    let err = ledger::Bounds::new(secs(82), 7, secs(81), &places).unwrap_err();
    assert!(
        err.contains("--ledger-max-entries must be above the fill ceiling, 7"),
        "{err}"
    );
    assert!(ledger::Bounds::new(secs(82), 8, secs(81), &places).is_ok());
    // What a test may build by hand is held to the same bounds: a zero or overlong span, a zero
    // age or count, and an overflowing ceiling.
    let one = [ledger::MayPlace {
        what: "x",
        orders: 1,
        largest_steps: 1,
    }];
    for (age, count, span, places, expected) in [
        (
            secs(2),
            1,
            secs(0),
            &[][..],
            "--registry-span: not from 1 to 86400 s",
        ),
        (
            secs(90_000),
            1,
            secs(86_401),
            &[][..],
            "--registry-span: not from 1 to 86400 s",
        ),
        (secs(0), 1, secs(1), &[][..], "--ledger-max-age: zero"),
        (secs(2), 0, secs(1), &[][..], "--ledger-max-entries: zero"),
        (
            secs(1),
            1,
            secs(1),
            &one[..],
            "--ledger-max-age must be longer",
        ),
    ] {
        let err = ledger::Bounds::new(age, count, span, places).unwrap_err();
        assert!(err.contains(expected), "{err}");
    }
}

#[test]
fn the_fill_ceiling_is_the_sum_of_each_orders_largest_size_in_size_steps() {
    let place = |orders, largest_steps| ledger::MayPlace {
        what: "synthetic",
        orders,
        largest_steps,
    };
    // Stage one places nothing: its ceiling is zero.
    assert!(ledger::stage_one().is_empty());
    assert_eq!(ledger::fill_ceiling(&ledger::stage_one()), Ok(0));
    // Each order counted at its largest size in steps, since every fill moves at least one.
    assert_eq!(ledger::fill_ceiling(&[place(1, 1)]), Ok(1));
    assert_eq!(
        ledger::fill_ceiling(&[place(1, 1), place(3, 5), place(2, 0)]),
        Ok(1 + 15)
    );
    assert_eq!(
        ledger::fill_ceiling(&[place(1, 10), place(1, 20), place(1, 30)]),
        Ok(60)
    );
    // A ceiling that overflows, by a product or a sum, is refused.
    for places in [
        vec![place(u64::MAX, 2)],
        vec![place(1, u64::MAX), place(1, 1)],
    ] {
        let err = ledger::fill_ceiling(&places).unwrap_err();
        assert!(err.contains("the fill ceiling overflows"), "{err}");
        let err = ledger::Bounds::new(Duration::from_secs(2), 1, Duration::from_secs(1), &places)
            .unwrap_err();
        assert!(err.contains("overflows"), "{err}");
    }
}

/// The probe mode's caps, whose scope decodes the test's fills as the codec would.
fn probe_caps() -> fbc_core::VenueCaps {
    use fbc_core::VenueFactory;
    let mut cfg = fbc_core::VenueConfig::new();
    cfg.insert(
        fbc_venue_qfex::probe::KEY_EXEC_MODE,
        fbc_venue_qfex::probe::PREPROD_PROBE,
    );
    cfg.insert(fbc_venue_qfex::auth::TRADE_URL, "ws://127.0.0.1:1");
    cfg.insert(fbc_venue_qfex::KEY_REST_URL, "http://127.0.0.1:2");
    cfg.insert(fbc_venue_qfex::KEY_MDS_URL, "ws://127.0.0.1:3");
    cfg.insert(fbc_venue_qfex::probe::KEY_PAGE_SIZE, "1000");
    cfg.insert(fbc_venue_qfex::probe::KEY_RESYNC_TIMEOUT, "30s");
    cfg.insert(fbc_venue_qfex::probe::KEY_RPC_TIMEOUT, "600ms");
    fbc_venue_qfex::Qfex.caps(&cfg).unwrap()
}

/// A client id of the probe's namespace, minted under a lease of its own.
fn our_cid() -> fbc_core::ClientOrderId {
    let (dir, _removed) = temp_dir("lease");
    let lease = fbc_core::NamespaceLease::acquire(
        &dir,
        fbc_core::AccountKey::new(1),
        fbc_core::Namespace::new(1),
    )
    .unwrap();
    fbc_core::CidMint::new(lease, 0, 0, WallNs(0))
        .mint()
        .unwrap()
}

/// A buy of one size step of `cid`'s order, keyed by the made-up trade id `trade` as both QFEX
/// reads key a fill (decision 0132 item 5): live, or a replay (`FillEvent::replay`) as a resync
/// pushes one.
fn qfex_fill(cid: fbc_core::ClientOrderId, trade: &str, replay: bool) -> fbc_core::FillEvent {
    use fbc_core::{AssetSym, CidMatch, FillIdent, FillOrigin, InstrumentId, Liquidity3, Lots};
    let caps = probe_caps();
    fbc_core::dispatch(&caps, fbc_core::Namespace::new(1), |scope| {
        fbc_core::FillEvent {
            ident: FillIdent::Venue {
                fill: scope.fill_id(trade).unwrap(),
                vid: Some(
                    scope
                        .venue_order_id("5a5a5a5a-0000-4000-8000-0000000000a1")
                        .unwrap(),
                ),
                cum_after: None,
            },
            origin: FillOrigin::Order {
                cid: Some(CidMatch::Ours(cid)),
                px: fbc_core::Ticks(1),
            },
            inst: InstrumentId::new(1),
            side: fbc_core::Side::Buy,
            qty: Lots::new(1).unwrap(),
            liquidity: Liquidity3::Maker,
            fee: scope.fee(0, AssetSym::new("USD").unwrap()).unwrap(),
            realized_pnl: None,
            realized_funding: None,
            replay,
        }
    })
}

/// When a registry's first session opened: `at` on the monotonic clock, the watermark well
/// before every fill time the tests give.
fn opened(at: MonoNs) -> ledger::Opened {
    ledger::Opened {
        mono: at,
        wall: WallNs(1_000),
        at: tokio::time::Instant::now(),
    }
}

/// A matching-engine time `exch`, aligned to the same wall-clock number.
fn engine(exch: i64) -> Option<fbc_oms::FillTime> {
    Some(fbc_oms::FillTime {
        exch: fbc_core::ExchNs(exch),
        kind: fbc_core::ExchTsKind::MatchingEngine,
        aligned: WallNs(exch),
    })
}

const T1: &str = "5a5a5a5a-0000-4000-8000-0000000000f1";
const T2: &str = "5a5a5a5a-0000-4000-8000-0000000000f2";
const T3: &str = "5a5a5a5a-0000-4000-8000-0000000000f3";

#[tokio::test]
async fn a_fill_heard_live_at_the_registrys_start_is_a_duplicate_when_replayed_at_its_span_bound() {
    use fbc_oms::{FillRouted, Horizon, ReplayCounts};
    use ledger::Judged;
    let span = Duration::from_secs(600);
    let start = MonoNs(5_000_000_000);
    let cid = our_cid();
    let live = qfex_fill(cid, T1, false);
    let replay = qfex_fill(cid, T1, true);
    assert_eq!(live.key(), replay.key());

    // The accepted bounds: an age longer than the span bound, a count above stage one's fill
    // ceiling.
    let accepted = bounds(span);
    let acct = fbc_core::AccountKey::new(1);
    let mut registry = ledger::RegistryLedger::new(
        "primary",
        acct,
        &accepted,
        opened(start),
        Environment::PreProduction,
    );
    assert!(registry.deadline() >= tokio::time::Instant::now() + span - Duration::from_secs(1));
    // The first session: the fill heard live as it starts, with no venue time (QFEX's until
    // FBC-jdj7), is applied, and counted for our namespace's order.
    let judged = registry.admit(&live, None, start);
    assert_eq!(judged, Judged::Applied(FillRouted::OursUntracked(cid)));
    registry.session_ended("first", start + Duration::from_secs(100));
    // The second session on the same registry: a resync reads the fill again, as a replay with
    // no matching-engine time, at the end of the span bound. The ledger still holds it, so it
    // is a duplicate, never refused Untimed, and its horizon is still full.
    let judged = registry.admit(&replay, None, start + span);
    assert_eq!(judged, Judged::Duplicate);
    registry.session_ended("second", start + span);
    let run = registry.run();
    assert_eq!(run.account, "primary");
    assert_eq!(run.bounds, accepted);
    let ends: Vec<(&str, Duration, Horizon, usize)> = (run.ends.iter())
        .map(|e| (e.session, e.after, e.horizon, e.held))
        .collect();
    assert_eq!(
        ends,
        [
            ("first", Duration::from_secs(100), Horizon::Full, 1),
            ("second", span, Horizon::Full, 1),
        ]
    );
    assert_eq!(run.ends[0].replays, ReplayCounts::default());
    let counts = ReplayCounts {
        duplicate: 1,
        ..ReplayCounts::default()
    };
    assert_eq!(run.ends[1].replays, counts);
    assert!(run.held());

    // With max_age equal to the span bound, which the program refuses, the same replay at the
    // same instant finds the fill forgotten: refused Untimed, the horizon lost.
    let refused = ledger::Bounds::new(span, 1, span, &ledger::stage_one()).unwrap_err();
    assert!(refused.contains("--ledger-max-age must be longer than --registry-span"));
    let config = fbc_oms::LedgerConfig {
        max_age: span,
        max_entries: 1,
    };
    let mut reg = fbc_oms::Registry::new();
    let mut short = fbc_oms::FillLedger::new(config, WallNs(1_000)).unwrap();
    let judged = ledger::judge(&mut reg, &mut short, &live, None, start);
    assert_eq!(judged, Judged::Applied(FillRouted::OursUntracked(cid)));
    let judged = ledger::judge(&mut reg, &mut short, &replay, None, start + span);
    assert_eq!(judged, Judged::Untimed);
    assert_eq!(short.horizon(), Horizon::Lost);
    assert_eq!(short.replays().untimed, 1);
}

#[tokio::test]
async fn every_judgement_of_the_ledger_and_the_registry_is_reported() {
    use fbc_oms::{FillRouted, Horizon, OmsError};
    use ledger::Judged;
    let cid = our_cid();
    let start = MonoNs(0);
    // A count of one: a second fill applied forgets the first, whose matching-engine time then
    // bounds the horizon (only the tests give a count this tight against two fills).
    let tight = ledger::Bounds::new(
        Duration::from_secs(10),
        1,
        Duration::from_secs(5),
        &ledger::stage_one(),
    )
    .unwrap();
    let acct = fbc_core::AccountKey::new(2);
    let mut registry = ledger::RegistryLedger::new(
        "subaccount",
        acct,
        &tight,
        opened(start),
        Environment::PreProduction,
    );
    let first = qfex_fill(cid, T1, false);
    let second = qfex_fill(cid, T2, false);
    let applied = Judged::Applied(FillRouted::OursUntracked(cid));
    assert_eq!(registry.admit(&first, engine(2_000), start), applied);
    assert_eq!(registry.admit(&second, engine(3_000), start), applied);
    // A replay of the forgotten fill, at or before the horizon: refused, it may have been
    // applied.
    let again = qfex_fill(cid, T1, true);
    assert_eq!(
        registry.admit(&again, engine(2_000), start),
        Judged::BeyondHorizon
    );
    // A replay executed before the watermark: the starting position holds it.
    let old = qfex_fill(cid, T3, true);
    assert_eq!(
        registry.admit(&old, engine(500), start),
        Judged::BeforeWatermark
    );
    registry.session_ended("subaccount", start + Duration::from_secs(1));
    let run = registry.run();
    let end = &run.ends[0];
    assert_eq!(end.horizon, Horizon::At(fbc_core::ExchNs(2_000)));
    assert_eq!(
        (end.replays.beyond_horizon, end.replays.before_watermark),
        (1, 1)
    );
    assert!(!run.held());
    // A registry refuses a fill from a second ledger.
    let mut reg = fbc_oms::Registry::new();
    let config = tight.config();
    let mut one = fbc_oms::FillLedger::new(config, WallNs(1_000)).unwrap();
    let mut other = fbc_oms::FillLedger::new(config, WallNs(1_000)).unwrap();
    assert_eq!(
        ledger::judge(&mut reg, &mut one, &first, None, start),
        applied
    );
    assert_eq!(
        ledger::judge(&mut reg, &mut other, &second, None, start),
        Judged::Refused(OmsError::OtherLedger)
    );
}

#[tokio::test]
async fn a_stage_one_session_at_its_span_bound_is_stopped_through_its_stop_control() {
    let (dir, _removed) = temp_dir("span");
    let mut steps = read_socket(0, EMPTY_PAGE);
    steps.extend(read_socket(1, EMPTY_PAGE));
    // QFEX never answers the session's authentication, so nothing but the span bound, before
    // --session-wait, ends it.
    steps.extend([Step::Accept, Step::Read { conn: 2 }]);
    let server = StubServer::start(WsScript::new(steps), rest(flat()))
        .await
        .unwrap();
    let mut opts = options(&dir, &server, &[Armed::Primary]);
    let span = Duration::from_secs(1);
    // An age bound well past the span, so the moment the stopped session takes to end stays
    // within it however loaded the machine.
    let age = span + Duration::from_secs(20);
    opts.ledger = ledger::Bounds::new(age, 1, span, &ledger::stage_one()).unwrap();
    assert!(opts.session_wait > span * 10);
    let began = std::time::Instant::now();
    let (ran, printed) = probe(&opts, &environment()).await;
    let took = began.elapsed();
    let outcome = ran.unwrap();
    // It ended at the span bound, through its stop control, with no second attempt.
    assert!(took >= span && took < opts.session_wait, "{took:?}");
    let session = &outcome.evidence.sessions[0];
    assert_eq!(session.ended_by, "--registry-span");
    assert!(!session.authenticated && !session.arm_sent);
    assert_eq!((session.attempts, session.failed_attempts), (1, 0));
    assert!(session.error.is_none(), "{session:?}");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(server.connections().len(), 3);
    assert!(
        !outcome.ok && printed.contains("ended by --registry-span"),
        "{printed}"
    );
    // Its registry's ledger, recorded as it ended: the bounds, a full horizon, no replay.
    let registries = &outcome.evidence.registries;
    assert_eq!(registries.len(), 1);
    let end = &registries[0].ends[0];
    assert_eq!(end.session, "primary");
    assert!(
        end.after >= span && end.after < opts.session_wait,
        "{end:?}"
    );
    // The bound is when the stop is issued; the session ends a moment later, and the evidence
    // says by how much.
    assert!(end.after < age, "{end:?}");
    assert_eq!(end.past_span(span), end.after.checked_sub(span));
    assert_eq!(end.horizon, fbc_oms::Horizon::Full);
    assert!(registries[0].held());
    let evidence = json(&fs::read_to_string(opts.evidence_dir.join("evidence.json")).unwrap());
    assert_eq!(evidence["sessions"][0]["ended_by"], "--registry-span");
    let registry = &evidence["registries"][0];
    assert_eq!(registry["registry_span_ms"], 1_000);
    assert_eq!(registry["ledger_max_age_ms"], 21_000);
    let at_end = &registry["session_ends"][0];
    assert_eq!(at_end["horizon"], "full");
    let after_ms = at_end["after_ms"].as_u64().unwrap();
    assert_eq!(at_end["past_span_ms"].as_u64(), after_ms.checked_sub(1_000));
    assert_report_and_evidence_pass_the_privacy_check(&opts.evidence_dir);
    assert_no_secret(&opts.evidence_dir);
}

/// Copies `dir`'s report and evidence alone into a directory of their own and runs
/// scripts/privacy-check.sh over it.
fn assert_report_and_evidence_pass_the_privacy_check(dir: &Path) {
    let (checked, _removed) = temp_dir("privacy");
    for name in ["report.md", "evidence.json"] {
        fs::copy(dir.join(name), checked.join(name)).unwrap();
    }
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/privacy-check.sh");
    evidence::privacy_check(&script, &checked).unwrap();
}

#[test]
fn the_report_and_the_evidence_carry_each_registrys_bounds_horizons_and_replay_counts() {
    use fbc_oms::{Horizon, ReplayCounts};
    let secs = Duration::from_secs;
    let places = [ledger::MayPlace {
        what: "far post-only GTC",
        orders: 1,
        largest_steps: 2,
    }];
    let held = ledger::RegistryRun {
        account: "primary",
        testnet_run: true,
        bounds: ledger::Bounds::new(secs(301), 10, secs(300), &places).unwrap(),
        ends: vec![ledger::AtEnd {
            session: "primary",
            after: Duration::from_millis(1_250),
            horizon: Horizon::Full,
            held: 0,
            replays: ReplayCounts::default(),
        }],
    };
    let counts = ReplayCounts {
        applied: 1,
        duplicate: 2,
        before_watermark: 3,
        beyond_horizon: 4,
        untimed: 5,
    };
    let forgot = ledger::RegistryRun {
        account: "subaccount",
        testnet_run: false,
        bounds: bounds(secs(60)),
        ends: vec![
            ledger::AtEnd {
                session: "drop test 1",
                after: secs(10),
                horizon: Horizon::At(fbc_core::ExchNs(1_762_000_000_000_000_000)),
                held: 6,
                replays: counts,
            },
            ledger::AtEnd {
                session: "drop test 2",
                after: secs(20),
                horizon: Horizon::Lost,
                held: 7,
                replays: counts,
            },
        ],
    };
    // Two sessions that did all stage one asks, the journal showing each arm written.
    let sessions = (reported().into_iter())
        .map(|run| SessionRun {
            arm_sent: true,
            ..run
        })
        .collect();
    let mut ev = run_of(sessions, [0, 0]);
    ev.registries = vec![held.clone()];
    assert!(ev.ok(), "{ev:#?}");
    let (report, evidence) = capture::outputs(&mut ev);
    assert!(
        report.contains("## The fill ledger of each registry"),
        "{report}"
    );
    assert!(
        report.contains(
            "| primary | 301s | 10 | 300s | 2 | 1 far post-only GTC of at most 2 steps | yes | yes |"
        ),
        "{report}"
    );
    assert!(
        report.contains("| primary | primary | 1.25s | no | full | 0 | 0 | 0 | 0 | 0 | 0 |"),
        "{report}"
    );
    let v = json(&evidence);
    let r = &v["registries"][0];
    assert_eq!(r["ledger_max_age_ms"], 301_000);
    assert_eq!(r["ledger_max_entries"], 10);
    assert_eq!(r["registry_span_ms"], 300_000);
    assert_eq!(r["fill_ceiling"], 2);
    assert_eq!(r["ceiling_counts"][0]["largest_steps"], 2);
    assert_eq!(r["held"], true);
    assert_eq!(r["testnet_run"], true);
    assert_eq!(r["session_ends"][0]["after_ms"], 1_250);
    assert_eq!(r["session_ends"][0]["past_span_ms"], Value::Null);
    // A registry whose ledger forgot a fill: its horizon moved, the bounds did not hold, and
    // the run is incomplete.
    ev.registries.push(forgot);
    assert!(!ev.ok());
    let (report, evidence) = capture::outputs(&mut ev);
    assert!(report.contains("Result: INCOMPLETE"), "{report}");
    assert!(
        report.contains(
            "| subaccount | 61s | 1 | 60s | 0 | no order: its sessions place none | NO | no |"
        ),
        "{report}"
    );
    assert!(
        report.contains(
            "| subaccount | drop test 1 | 10s | no | at 1762000000000000000 (venue ns) | 6 | 1 | 2 \
             | 3 | 4 | 5 |"
        ),
        "{report}"
    );
    assert!(
        report.contains("| subaccount | drop test 2 | 20s | no | lost | 7 |"),
        "{report}"
    );
    let v = json(&evidence);
    let r = &v["registries"][1];
    assert_eq!(r["held"], false);
    assert_eq!(r["testnet_run"], false);
    assert_eq!(r["session_ends"][1]["horizon"], "lost");
    let replays = &r["session_ends"][0]["replays"];
    for (field, n) in [
        ("applied", 1),
        ("duplicate", 2),
        ("before_watermark", 3),
        ("beyond_horizon", 4),
        ("untimed", 5),
    ] {
        assert_eq!(replays[field], n, "{field}");
    }
    assert_eq!(v["ok"], false);
    // Both pass the repository's privacy check.
    let (dir, _removed) = temp_dir("ledger-report");
    fs::write(dir.join("report.md"), report).unwrap();
    fs::write(dir.join("evidence.json"), evidence).unwrap();
    assert_report_and_evidence_pass_the_privacy_check(&dir);
}

#[tokio::test]
async fn a_registry_whose_session_ends_at_its_age_bound_has_not_held_them_though_its_horizon_reads_full()
 {
    use fbc_oms::{FillRouted, Horizon};
    use ledger::Judged;
    let secs = Duration::from_secs;
    let cid = our_cid();
    let start = MonoNs(0);
    let span = secs(5);
    let bounds = ledger::Bounds::new(secs(10), 5, span, &ledger::stage_one()).unwrap();
    let acct = fbc_core::AccountKey::new(1);
    let mut registry = ledger::RegistryLedger::new(
        "primary",
        acct,
        &bounds,
        opened(start),
        Environment::PreProduction,
    );
    let live = qfex_fill(cid, T1, false);
    let applied = Judged::Applied(FillRouted::OursUntracked(cid));
    assert_eq!(registry.admit(&live, None, start), applied);
    // A session ending just within the age bound: the fill applied at the registry's start is
    // still held, so the bounds held, though the session ran past the span bound.
    let within = start + (secs(10) - Duration::from_nanos(1));
    registry.session_ended("first", within);
    assert!(registry.run().held());
    let first = &registry.run().ends[0];
    assert_eq!(
        first.past_span(span),
        Some(secs(5) - Duration::from_nanos(1))
    );
    // Two sessions that did all stage one asks: with this registry the run is whole.
    let sessions: Vec<SessionRun> = (reported().into_iter())
        .map(|run| SessionRun {
            arm_sent: true,
            ..run
        })
        .collect();
    let mut ev = run_of(sessions, [0, 0]);
    ev.registries = vec![registry.run().clone()];
    assert!(ev.ok(), "{ev:#?}");
    // One ending at the age bound: fbc-oms forgets an aged fill only as it is given one, so
    // with no fill since, the horizon still reads full and the fill is still counted held...
    registry.session_ended("second", start + secs(10));
    let second = &registry.run().ends[1];
    assert_eq!((second.horizon, second.held), (Horizon::Full, 1));
    assert_eq!(second.past_span(span), Some(secs(5)));
    // ... but the bounds did not hold: a replay of that fill now finds it gone, refused
    // Untimed, which would stop the probe (decision 0132 item 7).
    assert!(!registry.run().held());
    let replay = qfex_fill(cid, T1, true);
    assert_eq!(
        registry.admit(&replay, None, start + secs(10)),
        Judged::Untimed
    );
    // The run is incomplete, and the report and the evidence say so.
    ev.registries = vec![registry.run().clone()];
    assert!(!ev.ok());
    let (report, evidence) = capture::outputs(&mut ev);
    assert!(report.contains("Result: INCOMPLETE"), "{report}");
    assert!(
        report.contains("| primary | 10s | 5 | 5s | 0 | no order: its sessions place none | NO |"),
        "{report}"
    );
    assert!(
        report.contains("| primary | second | 10s | 5s | full | 1 | 0 | 0 | 0 | 0 | 0 |"),
        "{report}"
    );
    let v = json(&evidence);
    let r = &v["registries"][0];
    assert_eq!(r["held"], false);
    assert_eq!(r["session_ends"][0]["past_span_ms"], 4_999);
    assert_eq!(r["session_ends"][1]["past_span_ms"], 5_000);
    assert_eq!(r["session_ends"][1]["horizon"], "full");
    let (dir, _removed) = temp_dir("aged-report");
    fs::write(dir.join("report.md"), report).unwrap();
    fs::write(dir.join("evidence.json"), evidence).unwrap();
    assert_report_and_evidence_pass_the_privacy_check(&dir);
}

#[tokio::test(start_paused = true)]
async fn a_registrys_times_are_read_on_the_runtimes_clock_as_the_sessions_stamps_are() {
    let secs = Duration::from_secs;
    // The run's origin, then three seconds on the paused runtime clock, which no real time
    // passes on: the run's monotonic clock reads them, as the sessions' IngestClock does.
    let origin = tokio::time::Instant::now();
    tokio::time::advance(secs(3)).await;
    assert_eq!(ledger::mono_since(origin), MonoNs(3_000_000_000));
    let opened = ledger::Opened::now(origin, WallNs(1_000));
    assert_eq!(opened.mono, MonoNs(3_000_000_000));
    assert_eq!(opened.at, tokio::time::Instant::now());
    let span = secs(600);
    let acct = fbc_core::AccountKey::new(1);
    let mut registry = ledger::RegistryLedger::new(
        "primary",
        acct,
        &bounds(span),
        opened,
        Environment::PreProduction,
    );
    assert_eq!(registry.deadline(), opened.at + span);
    // The span bound's timer and the registry's times move together: at the deadline its
    // session has run the whole span and no more.
    tokio::time::sleep_until(registry.deadline()).await;
    registry.session_ended("primary", ledger::mono_since(origin));
    let end = &registry.run().ends[0];
    assert_eq!((end.after, end.past_span(span)), (span, None));
    assert!(registry.run().held());
    // An origin after now reads zero, never wrapping.
    let later = tokio::time::Instant::now() + secs(1);
    assert_eq!(ledger::mono_since(later), MonoNs(0));
}

// ----- Provider rate refusals (decision 0135 item 2, FBC-m9rj) -----

/// A RateLimited err answering a get_user_orders page, flat, with the rate page's seconds hint.
const LIMITED_PAGE: &str = r#"{"type":"Err","error_code":"RateLimited","message":"Rate limit exceeded, retry after 3 seconds","incoming_message":{"type":"get_user_orders","params":{"limit":1000,"offset":0}}}"#;
/// A RateLimited err echoing the arm, flat, with a one-second hint: the arm's type names no
/// bucket, so the codec holds every frame budget for it.
const LIMITED_ARM: &str = r#"{"type":"Err","error_code":"RateLimited","message":"Rate limit exceeded, retry after 1 seconds","incoming_message":{"type":"cancel_on_disconnect","params":{"cancel_on_disconnect":true}}}"#;
/// An order QFEX refused for its rate, in the AsyncAPI's `order_response`.
const LIMITED_ORDER: &str = r#"{"order_response":{"symbol":"AAPL-USD","status":"RATE_LIMITED"}}"#;
/// A RateLimited err echoing nothing, flat, with the rate page's seconds hint: no account
/// material and no echo, so the journal keeps it in clear.
const LIMITED: &str = r#"{"type":"Err","error_code":"RateLimited","message":"Rate limit exceeded, retry after 3 seconds"}"#;

/// A RateLimited err echoing a read socket's authentication with `public`'s key: decision 0134
/// item 2 keeps it whole, so the journal would keep only `2`s of it.
fn limited_auth(public: &str) -> String {
    format!(
        r#"{{"type":"Err","error_code":"RateLimited","message":"Rate limit exceeded, retry after 3 seconds","incoming_message":{{"type":"auth","params":{{"hmac":{{"public_key":"{public}"}}}}}}}}"#
    )
}

#[tokio::test]
async fn a_read_answered_rate_limited_reports_one_provider_rate_refusal_for_its_session_to_record_under_fbc_s5ox()
 {
    let (dir, _removed) = temp_dir("rate-limited-read");
    // The primary account's session, after reads answered in full; then, before the
    // subaccount's, QFEX refuses the first read's page for its rate.
    let mut steps = read_socket(0, EMPTY_PAGE);
    steps.extend(read_socket(1, EMPTY_PAGE));
    steps.extend(session_socket(2, &[ARM_ACK]));
    steps.extend(read_socket(3, LIMITED_PAGE));
    let server = StubServer::start(WsScript::new(steps), rest(flat()))
        .await
        .unwrap();
    let opts = options(&dir, &server, &[Armed::Primary, Armed::Subaccount]);
    let (ran, printed) = probe(&opts, &environment()).await;
    let outcome = ran.unwrap();
    server.finished().await.unwrap();
    let ev = &outcome.evidence;

    // The refused read stops the probe before the subaccount's session, as any refused read
    // does, and nothing is sent after it: the probe has returned, and the script has played.
    assert!(
        !outcome.ok && printed.contains("DONE INCOMPLETE"),
        "{printed}"
    );
    let stopped = ev.stopped.as_deref().unwrap_or_default();
    assert!(
        stopped.contains(
            "primary could not be read: get_user_orders: QFEX refused the page read: RateLimited"
        ),
        "{stopped}"
    );
    assert_eq!(server.connections().len(), 4);
    assert_eq!(ev.sessions.len(), 1);

    // One provider rate refusal, for the subaccount's session: the read socket before it held
    // the shared budget for its page once (the limiter's count), and that page was a
    // RateLimited err. Every other read socket and the primary account's session met none.
    let none = Refusals::default();
    let one = Refusals {
        limiter: 1,
        errs: 1,
        ..Refusals::default()
    };
    let met: Vec<(&str, &str, Refusals)> = (ev.read_sockets.iter())
        .map(|r| (r.before, r.label.as_str(), r.refusals))
        .collect();
    assert_eq!(
        met,
        [
            ("primary", "primary (armed)", none),
            ("primary", "subaccount", none),
            ("subaccount", "primary", one),
        ]
    );
    assert_eq!(one.met(), 1);
    assert_eq!(ev.sessions[0].refusals, none);
    assert_eq!(refusals::total(&ev.read_sockets, &ev.sessions), 1);

    // report.md names it as provider rate-limit evidence to record under FBC-s5ox, for the
    // subaccount's session, with no fixture said to hold it: a read socket is not journaled.
    let report = fs::read_to_string(opts.evidence_dir.join("report.md")).unwrap();
    assert!(
        report.contains(
            "**Provider rate-limit evidence to record under FBC-s5ox** (decision 0135 item 2): \
             provider rate refusals met, at least: 1."
        ),
        "{report}"
    );
    assert!(report.contains("**This is provider rate-limit evidence, to record under FBC-s5ox.**"));
    assert!(report.contains(
        "A refusal met on a read socket has no frame in the journal, since the probe's reads are \
         not journaled: only its count is here."
    ));
    assert!(!report.contains("The fixtures that hold a session's refusal frames"));
    assert!(
        report.contains(
            "Met by each session, at least, with the read sockets before it: primary 0; \
             subaccount 1 (its session did not run)."
        ),
        "{report}"
    );
    assert!(
        report.contains(
            "| subaccount | read socket: primary | 1 | 1 | 0 | 0 | 0 | 1 | not journaled |"
        ),
        "{report}"
    );
    assert!(report.contains("| primary | the session | 0 | 0 | 0 | 0 | not seen | 0 | none |"));
    assert!(!report.contains("No provider rate refusal was counted"));

    // evidence.json says the same.
    let evidence = json(&fs::read_to_string(opts.evidence_dir.join("evidence.json")).unwrap());
    let rates = &evidence["provider_rate_refusals"];
    assert_eq!(rates["met_at_least"], 1);
    assert_eq!(rates["record_under"], "FBC-s5ox");
    assert_eq!(
        rates["by_session"],
        json(
            r#"[{"session":"primary","met_at_least":0,"session_ran":true},{"session":"subaccount","met_at_least":1,"session_ran":false}]"#
        )
    );
    assert_eq!(
        rates["read_sockets"][2],
        json(
            r#"{"before":"subaccount","account":"primary","counts":{"limiter_rejected":1,"rate_limited_errs":1,"rate_limited_order_responses":0,"frames_unread":0,"rate_limited_in_frames_kept_whole":0,"met_at_least":1}}"#
        )
    );
    assert_eq!(rates["read_sockets"][0]["counts"]["met_at_least"], 0);
    assert_eq!(rates["sessions"][0]["counts"]["met_at_least"], 0);
    assert_report_and_evidence_pass_the_privacy_check(&opts.evidence_dir);
    assert_no_secret(&opts.evidence_dir);
}

/// A probe arming on the primary account whose first read socket QFEX answers `answer` in
/// reply to its authentication: the probe asks for no page and stops there, as at any account
/// it cannot read. Its directory's guard, options, outcome and what it printed.
async fn authentication_answered(
    what: &str,
    answer: &str,
) -> (Removed, Options, stage::Outcome, String) {
    let (dir, removed) = temp_dir(what);
    let steps = vec![
        Step::Accept,
        Step::Respond {
            conn: 0,
            with: reply(&[answer]),
        },
    ];
    let server = StubServer::start(WsScript::new(steps), rest(flat()))
        .await
        .unwrap();
    let opts = options(&dir, &server, &[Armed::Primary]);
    let (ran, printed) = probe(&opts, &environment()).await;
    let outcome = ran.unwrap();
    server.finished().await.unwrap();
    assert!(
        !outcome.ok && printed.contains("DONE INCOMPLETE"),
        "{printed}"
    );
    let stopped = outcome.evidence.stopped.as_deref().unwrap_or_default();
    assert!(
        stopped.contains(
            "primary (armed) could not be read: QFEX did not authenticate the read socket"
        ),
        "{stopped}"
    );
    assert_eq!(server.connections().len(), 1);
    assert_eq!(received(&server, 0).len(), 1);
    assert!(outcome.evidence.sessions.is_empty());
    (removed, opts, outcome, printed)
}

#[tokio::test]
async fn a_read_sockets_authentication_answered_rate_limited_holds_the_shared_budget_and_counts_one_refusal()
 {
    let (_removed, opts, outcome, _) = authentication_answered("rate-limited-auth", LIMITED).await;
    let ev = &outcome.evidence;

    // The err, which the journal would keep in clear, is counted; and the probe held the shared
    // budget for it, as a session's runtime holds it for its codec's err in every state
    // (decision 0126): the limiter's count. One refusal, met before the primary's session.
    let one = Refusals {
        limiter: 1,
        errs: 1,
        ..Refusals::default()
    };
    assert_eq!(ev.read_sockets.len(), 1);
    assert_eq!(ev.read_sockets[0].refusals, one);
    let report = fs::read_to_string(opts.evidence_dir.join("report.md")).unwrap();
    assert!(
        report.contains(
            "| primary | read socket: primary (armed) | 1 | 1 | 0 | 0 | 0 | 1 | not journaled |"
        ),
        "{report}"
    );
    assert!(report.contains("provider rate refusals met, at least: 1."));
    assert!(report.contains(
        "Met by each session, at least, with the read sockets before it: primary 1 (its session \
         did not run)."
    ));
    let evidence = json(&fs::read_to_string(opts.evidence_dir.join("evidence.json")).unwrap());
    let rates = &evidence["provider_rate_refusals"];
    assert_eq!(
        (&rates["met_at_least"], &rates["record_under"]),
        (&json("1"), &json(r#""FBC-s5ox""#))
    );
    assert_eq!(
        rates["read_sockets"][0]["counts"],
        json(
            r#"{"limiter_rejected":1,"rate_limited_errs":1,"rate_limited_order_responses":0,"frames_unread":0,"rate_limited_in_frames_kept_whole":0,"met_at_least":1}"#
        )
    );
    assert_report_and_evidence_pass_the_privacy_check(&opts.evidence_dir);
    assert_no_secret(&opts.evidence_dir);
}

#[tokio::test]
async fn a_rate_limited_err_echoing_the_authentication_is_kept_whole_and_still_reported_as_a_refusal()
 {
    let answer = limited_auth(PUBLIC);
    let (_removed, opts, outcome, printed) =
        authentication_answered("rate-limited-auth-echo", &answer).await;
    let ev = &outcome.evidence;

    // Decision 0134 item 2 keeps it whole: as a journal would keep it, it shows no code and holds
    // no budget (0134 item 6), so neither the limiter nor the kept frame counts it, and it is
    // unread. Read once more in memory as it arrived, it was a RateLimited err: one refusal,
    // reported, never a clean zero.
    let whole = Refusals {
        unread: 1,
        whole: 1,
        ..Refusals::default()
    };
    assert_eq!(ev.read_sockets.len(), 1);
    assert_eq!(ev.read_sockets[0].refusals, whole);
    assert_eq!(whole.met(), 1);
    let report = fs::read_to_string(opts.evidence_dir.join("report.md")).unwrap();
    assert!(
        report.contains(
            "**Provider rate-limit evidence to record under FBC-s5ox** (decision 0135 item 2): \
             provider rate refusals met, at least: 1."
        ),
        "{report}"
    );
    assert!(report.contains(
        "Of them, 1 arrived in a frame decision 0134 item 2 keeps whole, which holds no budget \
         (0134 item 6), read only in memory as it arrived: the case decision 0135's \"What would \
         show this was wrong\" names, for FBC-s5ox to judge against that record."
    ));
    assert!(report.contains("Frames unread, kept whole or not JSON: 1."));
    assert!(
        report.contains(
            "| primary | read socket: primary (armed) | 0 | 0 | 0 | 1 | 1 | 1 | not journaled |"
        ),
        "{report}"
    );
    assert!(!report.contains("No provider rate refusal was counted"));
    let evidence = json(&fs::read_to_string(opts.evidence_dir.join("evidence.json")).unwrap());
    let rates = &evidence["provider_rate_refusals"];
    assert_eq!(
        (&rates["met_at_least"], &rates["frames_unread"]),
        (&json("1"), &json("1"))
    );
    assert_eq!(rates["record_under"], "FBC-s5ox");
    assert_eq!(
        rates["read_sockets"][0]["counts"],
        json(
            r#"{"limiter_rejected":0,"rate_limited_errs":0,"rate_limited_order_responses":0,"frames_unread":1,"rate_limited_in_frames_kept_whole":1,"met_at_least":1}"#
        )
    );
    // The echo held the account's public key: read in memory, it is written nowhere.
    assert!(!printed.contains(PUBLIC));
    assert_report_and_evidence_pass_the_privacy_check(&opts.evidence_dir);
    assert_no_secret(&opts.evidence_dir);
}

#[tokio::test]
async fn a_session_that_meets_rate_refusals_reports_each_from_its_limiter_and_its_journal() {
    let (dir, _removed) = temp_dir("rate-limited-session");
    // QFEX refuses for its rate in the arm's window: an err echoing the arm, and an order.
    let mut steps = read_socket(0, EMPTY_PAGE);
    steps.extend(read_socket(1, EMPTY_PAGE));
    steps.extend(session_socket(2, &[LIMITED_ARM, LIMITED_ORDER]));
    let server = StubServer::start(WsScript::new(steps), rest(flat()))
        .await
        .unwrap();
    let opts = options(&dir, &server, &[Armed::Primary]);
    let (ran, printed) = probe(&opts, &environment()).await;
    let outcome = ran.unwrap();
    server.finished().await.unwrap();
    let ev = &outcome.evidence;

    // A refusal is evidence, not a failure: the session armed and its arm ended Unknown at its
    // deadline, the refusal settling nothing (decision 0126).
    assert!(outcome.ok, "{printed}\n{ev:#?}");
    let run = &ev.sessions[0];
    assert_eq!(run.arm.as_deref(), Some("Unknown"));
    assert_eq!(run.attempts, 1);
    // The runtime held the shared budget for each (the limiter's count), and the journal kept
    // both frames: one RateLimited err and one RATE_LIMITED order response.
    let two = Refusals {
        limiter: 2,
        errs: 1,
        orders: 1,
        ..Refusals::default()
    };
    assert_eq!(run.refusals, two);
    assert_eq!(run.refusals.met(), 2);
    assert!(
        ev.read_sockets
            .iter()
            .all(|r| r.refusals == Refusals::default())
    );
    let kinds: Vec<&str> = (run.refusal_frames.iter())
        .map(|&i| run.frames[i].kind.as_str())
        .collect();
    assert_eq!(
        kinds,
        ["err echoing the arm (RateLimited)", "{order_response}"]
    );
    let files = ["primary-005-in-window.json", "primary-006-in-window.json"];
    let fixtures = opts.evidence_dir.join("fixtures");
    for file in files {
        assert!(fixtures.join(file).is_file(), "{file}");
    }

    let report = fs::read_to_string(opts.evidence_dir.join("report.md")).unwrap();
    assert!(report.contains("Result: complete."), "{report}");
    assert!(
        report.contains("provider rate refusals met, at least: 2."),
        "{report}"
    );
    assert!(report.contains(
        "The fixtures that hold a session's refusal frames are named in the table below."
    ));
    assert!(!report.contains("A refusal met on a read socket"));
    assert!(
        report
            .contains("Met by each session, at least, with the read sockets before it: primary 2.")
    );
    assert!(
        report.contains(
            "| primary | the session | 2 | 1 | 1 | 0 | not seen | 2 | \
             primary-005-in-window.json; primary-006-in-window.json |"
        ),
        "{report}"
    );
    let evidence = json(&fs::read_to_string(opts.evidence_dir.join("evidence.json")).unwrap());
    let rates = &evidence["provider_rate_refusals"];
    assert_eq!(rates["met_at_least"], 2);
    assert_eq!(rates["record_under"], "FBC-s5ox");
    assert_eq!(
        rates["sessions"][0],
        json(
            r#"{"session":"primary","capture_whole":true,"counts":{"limiter_rejected":2,"rate_limited_errs":1,"rate_limited_order_responses":1,"frames_unread":0,"rate_limited_in_frames_kept_whole":null,"met_at_least":2},"frames":["primary-005-in-window.json","primary-006-in-window.json"]}"#
        )
    );
    assert_report_and_evidence_pass_the_privacy_check(&opts.evidence_dir);
    evidence::privacy_check(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/privacy-check.sh"),
        &fixtures,
    )
    .unwrap();
    assert_no_secret(&opts.evidence_dir);
}

#[test]
fn a_refusal_frames_fixture_is_named_as_written_withheld_or_not_written() {
    let frame = |file: &str| Arrived {
        kind: "{order_response}".to_owned(),
        when: When::InWindow,
        file: file.to_owned(),
    };
    let [written, withheld, removed] = [
        "primary-001-in-window.json",
        "primary-002-in-window.json",
        "primary-003-in-window.json",
    ];
    let run = SessionRun {
        refusals: Refusals {
            limiter: 3,
            orders: 3,
            ..Refusals::default()
        },
        frames: vec![frame(written), frame(withheld), frame(removed)],
        refusal_frames: vec![0, 1, 2],
        ..reported()[0].clone()
    };
    let mut ev = run_of(vec![run], [0, 0]);
    ev.fixtures = vec![written.to_owned()];
    ev.withheld = vec![format!("{withheld}: a JWT")];

    // Each refusal frame's file is named as the run left it, in the report and the evidence.
    let report = capture::markdown(&ev);
    assert!(
        report.contains(
            "| primary | the session | 3 | 0 | 3 | 0 | not seen | 3 | primary-001-in-window.json; \
             primary-002-in-window.json (withheld); primary-003-in-window.json (not written) |"
        ),
        "{report}"
    );
    assert!(report.contains(
        "The fixtures that hold a session's refusal frames are named in the table below."
    ));
    let evidence = capture::to_json(&ev);
    assert_eq!(
        evidence["provider_rate_refusals"]["sessions"][0]["frames"],
        json(
            r#"["primary-001-in-window.json","primary-002-in-window.json (withheld)","primary-003-in-window.json (not written)"]"#
        )
    );

    // With no fixture written (the privacy check removed the directory), none is said to hold
    // a refusal frame.
    ev.fixtures.clear();
    let report = capture::markdown(&ev);
    assert!(!report.contains("The fixtures that hold"), "{report}");
    assert!(report.contains("| 3 | primary-001-in-window.json (not written); "));
}

#[test]
fn a_read_sockets_frame_is_read_as_the_journal_keeps_it() {
    // What the journal's reader returns of a frame recorded with `spans`.
    let journaled = |frame: RawFrame<'_>, spans: &InboundSpans| {
        let record = Record::inbound_redacted(stamp(0), frame, spans).unwrap();
        match record.blanked() {
            Record::Inbound { bytes, .. } => String::from_utf8_lossy(&bytes.0).into_owned(),
            other => panic!("{other:?}"),
        }
    };
    let binary = br#"{"type":"Err","error_code":"RateLimited"}"#;
    for (frame, spans) in [
        (RawFrame::Text(OTHER_ERR), Vec::new()),
        (RawFrame::Text(OTHER_ERR), vec![2..6, 40..52]),
        (RawFrame::Binary(binary), vec![1..7, 9..13]),
    ] {
        let spans = InboundSpans::frame(spans);
        assert_eq!(refusals::kept(frame, &spans), journaled(frame, &spans));
    }
    // Spans that do not fit the frame are a codec defect the journal refuses; the runtime then
    // marks all of the frame, and so it is kept: unread.
    let past = std::iter::once(0..u32::try_from(AUTH_OK.len() + 1).unwrap());
    let past = InboundSpans::frame(past.collect());
    let frame = RawFrame::Text(AUTH_OK);
    assert!(Record::inbound_redacted(stamp(0), frame, &past).is_err());
    let kept = refusals::kept(frame, &past);
    assert_eq!(kept, "2".repeat(AUTH_OK.len()));
    let mut heard = Refusals::default();
    assert!(!heard.count(&kept));
    assert_eq!(heard.unread, 1);
}

#[test]
fn a_provider_rate_refusal_is_a_rate_limited_err_or_a_rate_limited_order_response() {
    use refusals::Refusal;
    let cases = [
        (LIMITED_PAGE, Some(Refusal::Err)),
        (LIMITED_ARM, Some(Refusal::Err)),
        (LIMITED, Some(Refusal::Err)),
        (
            r#"{"err":{"error_code":"RateLimited","message":"Too many requests."}}"#,
            Some(Refusal::Err),
        ),
        (OTHER_ERR, None),
        // A code the journal spanned, or a frame it kept whole, shows none.
        (
            r#"{"type":"Err","error_code":"22222222222","incoming_message":"2222"}"#,
            None,
        ),
        ("22222222222222222222", None),
        (r#"{"ack":{"error_code":"RateLimited"}}"#, None),
        (r#"{"ack":{},"err":{"error_code":"RateLimited"}}"#, None),
        (LIMITED_ORDER, Some(Refusal::Order)),
        (r#"{"order_response":{"status":"ACK"}}"#, None),
        (
            r#"{"order_response":{"status":"RATE_LIMITED"},"x":1}"#,
            None,
        ),
        (r#"{"order_response":"RATE_LIMITED"}"#, None),
        (r#"{"position_response":{"status":"RATE_LIMITED"}}"#, None),
        ("[1]", None),
    ];
    for (text, refusal) in cases {
        assert_eq!(refusals::refusal(text), refusal, "{text}");
    }
    // What one met, at least: the larger of the limiter's count and the frames' together.
    let mut heard = Refusals::default();
    assert!(heard.count(LIMITED_PAGE) && heard.count(LIMITED_ORDER) && !heard.count(OTHER_ERR));
    assert_eq!(
        heard,
        Refusals {
            errs: 1,
            orders: 1,
            ..Refusals::default()
        }
    );
    assert_eq!(heard.met(), 2);
    assert_eq!(
        Refusals {
            limiter: 3,
            ..heard
        }
        .met(),
        3
    );
    // A frame no JSON object is kept of, blanked whole or not JSON at all, is unread: no code
    // can be read from it.
    let mut heard = Refusals::default();
    assert!(!heard.count("22222222") && !heard.count("not JSON") && !heard.count("[1]"));
    assert_eq!(
        heard,
        Refusals {
            unread: 3,
            ..Refusals::default()
        }
    );
    // On a read socket, an unread frame is read once more as it arrived, for a refusal kept
    // whole; a frame read as kept is never read as it arrived, so none counts twice.
    let mut heard = Refusals::default();
    let arrived = limited_auth(PUBLIC);
    heard.hear(&"2".repeat(arrived.len()), &arrived);
    heard.hear("2222", ARM_ACK);
    heard.hear(LIMITED_PAGE, LIMITED_PAGE);
    heard.hear(AUTH_OK, LIMITED);
    assert_eq!(
        heard,
        Refusals {
            limiter: 0,
            errs: 1,
            orders: 0,
            unread: 2,
            whole: 1,
        }
    );
    assert_eq!(heard.met(), 2);
    assert_eq!(
        Refusals {
            limiter: 2,
            ..heard
        }
        .met(),
        2
    );
    // A probe stopped before any read socket opened met none, and the report says so.
    let ev = run_of(Vec::new(), [0, 0]);
    assert_eq!(refusals::total(&ev.read_sockets, &ev.sessions), 0);
    let report = capture::markdown(&ev);
    assert!(
        report.contains(
            "Met by each session, at least, with the read sockets before it: none (no read \
             socket opened)."
        ),
        "{report}"
    );
    assert!(report.contains("No provider rate refusal was counted."));
}
