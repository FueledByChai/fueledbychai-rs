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
//!   credential or account id in any of them.
//!
//! The stub answers as QFEX's pages show (each frame below names its page): the account listing,
//! each account's positions selected by `x-qfex-requested-account-id`, the authentication reply,
//! the subscription confirmations and the `get_user_orders` page. Every key, secret, account id
//! and answer is synthetic: the REST answers are `fixtures/qfex/private` (its `SYNTHETIC` file).
//! The run is on real time; each session waits out its arm's 600 ms deadline.

#[path = "../src/auth_qfex.rs"]
mod auth_qfex;
#[path = "../examples/qfex_preprod_probe/capture.rs"]
mod capture;
#[allow(dead_code)]
#[path = "../examples/paradex_testnet_acceptance/evidence.rs"]
mod evidence;
#[allow(dead_code)]
#[path = "../src/link.rs"]
mod link;
#[path = "../examples/qfex_preprod_probe/probe_args.rs"]
mod probe_args;
#[path = "../examples/qfex_preprod_probe/stage.rs"]
mod stage;

use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use auth_qfex::{Armed, PUBLIC_VAR, SECRET_VAR, SUB_ID_VAR, SUB_PUBLIC_VAR, SUB_SECRET_VAR};
use capture::{Arrived, Evidence, SessionRun, When};
use fbc_conformance::{
    Frame, HttpReply, HttpRouter, PathPattern, Responder, Step, StubServer, WsScript,
};
use fbc_runtime::ProxyConfig;
use fbc_runtime::http::Method;
use fbc_venue_qfex::exec::ARM;
use probe_args::{Options, Parsed};
use serde_json::Value;

/// Made-up credentials, in the alphabet QFEX's examples show.
const PUBLIC: &str = "qfex_pub_SYNTHETIC_0001";
const SECRET: &str = "synthetic-qfex-secret-0001";
const SUB_PUBLIC: &str = "qfex_pub_SYNTHETIC_0002";
const SUB_SECRET: &str = "synthetic-qfex-secret-0002";
/// The made-up subaccounts `fixtures/qfex/private/subaccounts.json` lists; the first is armed.
const SUB: &str = "5a5a5a5a-0000-4000-8000-00000000c0de";
const SUB2: &str = "5a5a5a5a-0000-4000-8000-00000000beef";

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

/// The stub's REST answers: the listing, and each account's positions by its selection header
/// (`None` for the primary account).
fn rest(listing: HttpReply, positions: HashMap<Option<&'static str>, HttpReply>) -> HttpRouter {
    let positions: HashMap<Option<String>, HttpReply> = positions
        .into_iter()
        .map(|(k, v)| (k.map(str::to_owned), v))
        .collect();
    HttpRouter::new()
        .route(
            Method::GET,
            PathPattern::exact("/user/subaccounts"),
            listing,
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

/// Every account flat, with no open order counted.
fn flat() -> HashMap<Option<&'static str>, HttpReply> {
    let flat = ok(&positions_flat(0));
    HashMap::from([
        (None, flat.clone()),
        (Some(SUB), flat.clone()),
        (Some(SUB2), flat),
    ])
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
        trade_url: server.ws_url(""),
        rest_url: server.http_url(""),
        mds_url: server.ws_url(""),
        proxy: ProxyConfig::Direct,
        rpc_timeout_ms: 600,
        page_size: 1000,
        read_wait: Duration::from_secs(5),
        connect_wait: Duration::from_secs(5),
        session_wait: Duration::from_secs(30),
    }
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
        for secret in [PUBLIC, SECRET, SUB_PUBLIC, SUB_SECRET, SUB, SUB2] {
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
async fn each_account_arms_once_in_turn_and_its_capture_is_written_scrubbed() {
    let (dir, _removed) = temp_dir("capture");
    let position = fixture("position_response.json");
    let position = position.split_whitespace().collect::<String>();
    let mut steps = Vec::new();
    // Before the primary account's session: the primary account, then each listed subaccount,
    // the first read pinged between its two replies (a ping is passed over).
    steps.extend(read_socket(0, EMPTY_PAGE));
    steps.insert(2, Step::Barrier { conn: 0 });
    steps.extend(read_socket(1, EMPTY_PAGE));
    steps.extend(read_socket(2, EMPTY_PAGE));
    steps.extend(session_socket(3, &[ARM_ACK, OTHER_ERR, &position]));
    // Before the subaccount's session, every account again; then its session, whose arm
    // nothing answers.
    steps.extend(read_socket(4, EMPTY_PAGE));
    steps.extend(read_socket(5, EMPTY_PAGE));
    steps.extend(read_socket(6, EMPTY_PAGE));
    steps.extend(session_socket(7, &[]));
    let listing = ok(&fixture("subaccounts.json"));
    let server = StubServer::start(WsScript::new(steps), rest(listing, flat()))
        .await
        .unwrap();
    let opts = options(&dir, &server, &[Armed::Primary, Armed::Subaccount]);

    let (ran, printed) = probe(&opts, &environment()).await;
    let outcome = ran.unwrap();
    assert!(outcome.ok, "{printed}\n{:#?}", outcome.evidence);
    server.finished().await.unwrap();

    // No command but the arm: each read socket sent its authentication and one page, each
    // session its authentication, its subscriptions and one arm. Eight connections, so no
    // session attempted a second.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(server.connections().len(), 8);
    for (conn, account, public) in [
        (0, None, PUBLIC),
        (1, Some(SUB), SUB_PUBLIC),
        (2, Some(SUB2), PUBLIC),
        (4, None, PUBLIC),
        (5, Some(SUB), SUB_PUBLIC),
        (6, Some(SUB2), PUBLIC),
    ] {
        assert_read_socket(&received(&server, conn), account, public);
    }
    assert_session_socket(&received(&server, 3), None, PUBLIC);
    assert_session_socket(&received(&server, 7), Some(SUB), SUB_PUBLIC);
    let mut asked = vec!["GET /user/subaccounts".to_owned()];
    asked.extend(std::iter::repeat_n("GET /user/positions".to_owned(), 6));
    assert_eq!(server.http_requests(), asked);

    // What the sessions reported: each authenticated, armed once, its arm Unknown at its
    // deadline (no frame answers it yet), stopped there by the program, one attempt each.
    let ev = &outcome.evidence;
    assert_eq!(ev.listed, Some(2));
    assert_eq!(ev.reads.len(), 6);
    assert!(ev.reads.iter().all(|r| r.refusal().is_none()));
    let labels: Vec<(&str, &str)> = ev
        .reads
        .iter()
        .map(|r| (r.before, r.label.as_str()))
        .collect();
    assert_eq!(
        labels,
        [
            ("primary", "primary (armed)"),
            ("primary", "subaccount 1"),
            ("primary", "subaccount 2"),
            ("subaccount", "primary"),
            ("subaccount", "subaccount 1 (armed)"),
            ("subaccount", "subaccount 2"),
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
    // The members whose string value a fixture keeps, named for the owner's reading: never a
    // span the codec blanked (the position's id, the err's message).
    assert_eq!(
        ev.strings,
        ["error_code", "result", "subscribed", "symbol", "type"]
    );
    assert!(read("README.md").contains("error_code; result; subscribed; symbol; type"));
    assert!(ev.withheld.is_empty() && ev.errors.is_empty(), "{ev:#?}");
    assert_eq!(ev.fixtures.len(), 7 + 4 + 1);

    // The files: the report and the evidence say what answered each arm, the fixtures pass
    // the repository's privacy check, and nothing holds a credential or an account id.
    let report = fs::read_to_string(opts.evidence_dir.join("report.md")).unwrap();
    assert!(report.contains("Result: complete."), "{report}");
    assert!(report.contains("An ack echoing the arm arrived in its window"));
    assert!(report.contains("Nothing arrived in the arm's window"));
    assert!(report.contains("read every fixture for an identifying string"));
    let evidence = json(&fs::read_to_string(opts.evidence_dir.join("evidence.json")).unwrap());
    assert_eq!(evidence["ok"], true);
    assert_eq!(evidence["kept_string_members"][0], "error_code");
    assert_eq!(evidence["sessions"][1]["arm_outcome"], "Unknown");
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/privacy-check.sh");
    evidence::privacy_check(&script, &fixtures).unwrap();
    assert_no_secret(&opts.evidence_dir);
    assert!(printed.contains("DONE ok"), "{printed}");
    assert!(!printed.contains(SUB) && !printed.contains(PUBLIC));
}

// ----- Refusals before anything connects -----

#[tokio::test]
async fn a_production_host_an_invalid_account_a_proxied_loopback_or_a_missing_credential_is_refused_before_connecting()
 {
    let (dir, _removed) = temp_dir("refused");
    let listing = ok(&fixture("subaccounts.json"));
    let server = StubServer::start(WsScript::new(Vec::new()), rest(listing, flat()))
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

/// Runs the probe against a stub playing `steps` and answering `listing` and `positions`, arming
/// on the primary account then the subaccount: the evidence, which must say the probe stopped
/// with `stopped`, and the stub.
async fn stopped(
    what: &str,
    steps: Vec<Step>,
    listing: HttpReply,
    positions: HashMap<Option<&'static str>, HttpReply>,
    tune: impl Fn(&mut Options),
    stopped: &str,
) -> (Evidence, StubServer) {
    let (dir, _removed) = temp_dir(what);
    let server = StubServer::start(WsScript::new(steps), rest(listing, positions))
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
    let report = fs::read_to_string(opts.evidence_dir.join("report.md")).unwrap();
    assert!(
        report.contains("**The probe stopped before its sessions ended:**"),
        "{report}"
    );
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
    let listing = || ok(&fixture("subaccounts.json"));
    // The primary account lists an open order: nothing more is read.
    let page = fixture("all_orders_response.json");
    let (ev, server) = stopped(
        "open-order",
        read_socket(0, &page),
        listing(),
        flat(),
        |_| {},
        "primary (armed) holds an open order",
    )
    .await;
    assert_eq!((ev.reads[0].page_orders, ev.reads[0].page_twaps), (2, 1));
    assert_eq!(server.connections().len(), 1);
    assert_no_session(&server);

    // The second subaccount holds a position: every account was read, no session opened.
    let mut positions = flat();
    positions.insert(Some(SUB2), ok(&fixture("positions.json")));
    let mut steps = read_socket(0, EMPTY_PAGE);
    steps.extend(read_socket(1, EMPTY_PAGE));
    steps.extend(read_socket(2, EMPTY_PAGE));
    let (ev, server) = stopped(
        "not-flat",
        steps,
        listing(),
        positions,
        |_| {},
        "subaccount 2 holds",
    )
    .await;
    assert_eq!(ev.reads.len(), 3);
    assert!(!ev.reads[2].flat && ev.reads[2].rest_open_orders == 2);
    assert_eq!(server.connections().len(), 3);
    assert_no_session(&server);

    // The second subaccount is flat and its page empty, but REST counts an open order: the
    // two reads disagree, and the probe reads that strictly.
    let mut positions = flat();
    positions.insert(Some(SUB2), ok(&fixture("positions_flat.json")));
    let mut steps = read_socket(0, EMPTY_PAGE);
    steps.extend(read_socket(1, EMPTY_PAGE));
    steps.extend(read_socket(2, EMPTY_PAGE));
    let (ev, server) = stopped(
        "rest-count",
        steps,
        listing(),
        positions,
        |_| {},
        "subaccount 2 shows no open order on get_user_orders, but GET /user/positions counts 1",
    )
    .await;
    let read = &ev.reads[2];
    assert!(read.flat && read.page_orders == 0 && read.rest_open_orders == 1);
    assert!(read.refusal().is_some());
    assert_eq!(server.connections().len(), 3);
    assert_no_session(&server);
}

#[tokio::test]
async fn an_account_that_cannot_be_read_or_a_subaccount_not_listed_stops_the_probe() {
    let listing = || ok(&fixture("subaccounts.json"));
    let refused = |status| HttpReply {
        status,
        body: Vec::new(),
    };
    // The listing refused.
    let (ev, _) = stopped(
        "listing",
        Vec::new(),
        refused(500),
        flat(),
        |_| {},
        "HTTP 500",
    )
    .await;
    assert_eq!(ev.listed, None);
    // The subaccount armed on is not listed.
    let only = ok(&format!(r#"{{"account_ids":["{SUB2}"]}}"#));
    let read_all = || {
        let mut steps = read_socket(0, EMPTY_PAGE);
        steps.extend(read_socket(1, EMPTY_PAGE));
        steps
    };
    let mut positions = flat();
    positions.remove(&Some(SUB));
    let (ev, _) = stopped(
        "unlisted",
        read_all(),
        only,
        positions,
        |o| o.arms.reverse(),
        "names no subaccount",
    )
    .await;
    assert!(ev.reads.is_empty());
    // A subaccount's positions refused.
    let mut positions = flat();
    positions.insert(Some(SUB), refused(401));
    let (ev, _) = stopped(
        "positions",
        read_socket(0, EMPTY_PAGE),
        listing(),
        positions,
        |_| {},
        "subaccount 1 could not be read: GET /user/positions: QFEX answered HTTP 401",
    )
    .await;
    assert_eq!(ev.reads.len(), 1);
    // The read socket's authentication refused.
    let err = r#"{"type":"Err","error_code":"PermissionDenied","message":"no","incoming_message":"auth"}"#;
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
        listing(),
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
        listing(),
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
    stopped(
        "binary",
        steps,
        listing(),
        flat(),
        |_| {},
        "not a text frame",
    )
    .await;
    let mut steps = read_socket(0, EMPTY_PAGE);
    steps[2] = Step::Read { conn: 0 };
    steps.push(Step::Close { conn: 0 });
    stopped(
        "closed",
        steps,
        listing(),
        flat(),
        |_| {},
        "closed before it answered",
    )
    .await;
    let mut steps = read_socket(0, EMPTY_PAGE);
    steps[2] = Step::Read { conn: 0 };
    let fast = |o: &mut Options| o.read_wait = Duration::from_millis(300);
    stopped(
        "silent",
        steps,
        listing(),
        flat(),
        fast,
        "no answer within 300 ms",
    )
    .await;
    // The trade socket's origin refuses connections.
    let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = closed.local_addr().unwrap().port();
    drop(closed);
    let gone = move |o: &mut Options| o.trade_url = format!("ws://127.0.0.1:{port}");
    stopped(
        "no-socket",
        Vec::new(),
        listing(),
        flat(),
        gone,
        "the read socket:",
    )
    .await;
    // The REST origin refuses connections.
    let gone = move |o: &mut Options| o.rest_url = format!("http://127.0.0.1:{port}");
    stopped(
        "no-rest",
        Vec::new(),
        listing(),
        flat(),
        gone,
        "GET /user/subaccounts:",
    )
    .await;
}

// ----- A session that never arms -----

#[tokio::test]
async fn a_session_that_never_arms_is_stopped_with_no_second_attempt() {
    let listing = || ok(&fixture("subaccounts.json"));
    let read_all = || {
        let mut steps = read_socket(0, EMPTY_PAGE);
        steps.extend(read_socket(1, EMPTY_PAGE));
        steps.extend(read_socket(2, EMPTY_PAGE));
        steps
    };
    let run = |what: &'static str, session: Vec<Step>, wait: u64| {
        let mut steps = read_all();
        steps.extend(session);
        async move {
            let (dir, _removed) = temp_dir(what);
            let server = StubServer::start(WsScript::new(steps), rest(listing(), flat()))
                .await
                .unwrap();
            let mut opts = options(&dir, &server, &[Armed::Primary]);
            opts.session_wait = Duration::from_secs(wait);
            let (ran, printed) = probe(&opts, &environment()).await;
            let outcome = ran.unwrap();
            assert!(!outcome.ok, "{printed}");
            tokio::time::sleep(Duration::from_millis(300)).await;
            // Three read sockets and the one session: no second attempt.
            assert_eq!(server.connections().len(), 4, "{what}");
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
            conn: 3,
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
    let silent = vec![Step::Accept, Step::Read { conn: 3 }];
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

#[test]
fn the_command_line_takes_each_flag_and_refuses_without_showing_a_value() {
    let parse = |args: &[&str]| probe_args::parse(args.iter().map(|a| (*a).to_owned()));
    assert_eq!(parse(&["--help"]), Ok(Parsed::Help));
    let Ok(Parsed::Run(opts)) = parse(&["--evidence-dir", "/abs/dir", "--arm", "primary"]) else {
        panic!("not parsed");
    };
    assert_eq!(opts.arms, [Armed::Primary]);
    assert_eq!(opts.trade_url, "wss://trade.qfex.io");
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
    ];
    let Ok(Parsed::Run(opts)) = parse(&all) else {
        panic!("not parsed");
    };
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
    let run = ["--evidence-dir", "/d", "--arm", "primary"];
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
        PUBLIC_VAR,
        SUB_SECRET_VAR,
    ] {
        assert!(usage.contains(flag), "{flag}");
    }
}
