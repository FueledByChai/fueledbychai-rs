//! FBC-x69b's done line: testnet_trade's wiring, run against the conformance kit's stub server on
//! 127.0.0.1 standing in for Paradex, with synthetic credentials, goes through login, arm,
//! resync, start, place, ack, cancel, cancel ack and stop in that order, the session sending
//! exactly one place (`order.create`) and one cancel (`order.cancel`); a mainnet URL or chain id
//! is refused before any connection; no credential appears in what it prints; and an order that
//! does not fit the market's minimum notional is refused before the session connects.
//!
//! The stub answers, over HTTP, the login (`POST /v1/auth`, a made-up token), the order book the
//! order is priced from (the hand-built `fixtures/paradex/rest/orderbook-btc-2002.json`) and the
//! resync's open orders and positions (none: flat); over the order socket, the auth frame, the
//! four private subscriptions, the cancel-on-disconnect arm, the place (its order echoed under a
//! made-up venue id) and the cancel (queued, then the order event, an SBE `OrderEvent` at 1:2
//! built here from Paradex's schema, reporting it CLOSED by USER_CANCELED). The credentials are
//! the synthetic account and key of `fixtures/paradex/signing` (read from the vectors file's
//! header, so no key-shaped value sits in this crate). The run is on real time, bounded at 60 s.

#[path = "../examples/testnet_trade/args.rs"]
mod args;
#[path = "../src/auth.rs"]
mod auth;
#[path = "../examples/testnet_trade/link.rs"]
mod link;
#[path = "../examples/testnet_trade/trade.rs"]
mod trade;

use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use args::{Options, OrderSide, Parsed, TESTNET_CHAIN, USAGE};
use fbc_conformance::{
    Frame, HttpReply, HttpRouter, PathPattern, Responder, Step, StubServer, WsScript,
};
use fbc_core::{Secret, Secrets};
use fbc_runtime::ProxyConfig;
use fbc_runtime::http::Method;
use fbc_venue_paradex::auth::{ACCOUNT_ADDRESS, SIGNING_KEY};
use rust_decimal::Decimal;
use serde_json::{Value, json};

/// The session token the stub's login gives: made-up text that never looks like a JWT.
const TOKEN: &str = "SYNTHETIC.session-token.testnet-trade";
/// The venue id the stub gives the order.
const VID: &str = "1759500000000000777";
const MARKET: &str = "BTC-USD-PERP";

fn fixture(rel: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures")
        .join(rel)
}

/// The synthetic account and key from the signing vectors' header.
fn synthetic() -> (String, String) {
    let text = fs::read_to_string(fixture("paradex/signing/paradex-vectors.tsv")).unwrap();
    let mut header = HashMap::new();
    for line in text.lines().filter_map(|l| l.strip_prefix("# ")) {
        for pair in line.split(' ') {
            if let Some((k, v)) = pair.split_once('=') {
                header.insert(k.to_owned(), v.to_owned());
            }
        }
    }
    (header["account"].clone(), header["key"].clone())
}

fn secrets() -> Secrets {
    let (account, key) = synthetic();
    let mut creds = Secrets::new();
    creds.insert(ACCOUNT_ADDRESS, Secret::new(account));
    creds.insert(SIGNING_KEY, Secret::new(key));
    creds
}

/// A lease directory of the test's own.
fn lease_dir() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir =
        std::env::temp_dir().join(format!("fbc-testnet-trade-test-{}-{n}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn options(stub: &StubServer, min_notional: &str) -> Options {
    Options {
        market: MARKET.to_owned(),
        tick: Decimal::from_str("0.1").unwrap(),
        step: Decimal::from_str("0.00001").unwrap(),
        min_notional: Decimal::from_str(min_notional).unwrap(),
        side: OrderSide::Buy,
        away_bps: 300,
        hold_secs: 0,
        step_timeout_secs: 10,
        rest_url: stub.http_url("/v1"),
        ws_url: stub.ws_url("/v1"),
        proxy: ProxyConfig::Direct,
        lease_dir: lease_dir(),
        sole_trader: true,
    }
}

fn reply(status: u16, body: impl Into<Vec<u8>>) -> HttpReply {
    HttpReply {
        status,
        body: body.into(),
    }
}

/// The stub's HTTP answers: the login, the order book and the resync's two reads.
fn routes() -> HttpRouter {
    let book = fs::read(fixture("paradex/rest/orderbook-btc-2002.json")).unwrap();
    let empty = r#"{"results":[]}"#;
    HttpRouter::new()
        .route(
            Method::POST,
            PathPattern::exact("/v1/auth"),
            reply(200, format!(r#"{{"jwt_token":"{TOKEN}"}}"#)),
        )
        .route(
            Method::GET,
            PathPattern::exact(&format!("/v1/orderbook/{MARKET}")),
            reply(200, book),
        )
        .route(
            Method::GET,
            PathPattern::exact("/v1/orders"),
            reply(200, empty),
        )
        .route(
            Method::GET,
            PathPattern::exact("/v1/positions"),
            reply(200, empty),
        )
}

/// A decimal string as Paradex's SBE 10^-8 mantissa.
fn e8(text: &str) -> i64 {
    let d = Decimal::from_str(text).unwrap() * Decimal::from(100_000_000);
    i64::try_from(d.mantissa() / 10i128.pow(d.scale())).unwrap()
}

/// An `OrderEvent` (template 20) at schema 1:2 in its 128-byte block layout, as
/// `fixtures/paradex/exec/README.md` describes it: our post-only limit buy `cid` of `size` at
/// `price`, venue id `vid`, CLOSED by USER_CANCELED with nothing filled. The account is 32
/// made-up bytes.
fn order_closed(seq: i64, vid: &str, cid: &str, price: &str, size: &str) -> Vec<u8> {
    let null = i64::MIN;
    let ts = 1_759_500_000_205_011i64;
    let mut f = Vec::new();
    for v in [128u16, 20, 1, 2] {
        f.extend(v.to_le_bytes());
    }
    f.extend(ts.to_le_bytes());
    f.extend(seq.to_le_bytes());
    f.extend([4u8, 1, 1, 3]); // CLOSED, BUY, LIMIT, POST_ONLY
    f.extend(e8(price).to_le_bytes());
    f.extend(null.to_le_bytes()); // triggerPrice
    f.extend(e8(size).to_le_bytes()); // size
    f.extend(e8(size).to_le_bytes()); // sizeOpen: nothing filled
    f.extend(null.to_le_bytes()); // avgFillPrice
    f.extend(ts.to_le_bytes()); // createdAt
    f.extend(ts.to_le_bytes()); // updatedAt
    f.extend(0xa0u8..=0xbf); // account, made up
    f.extend(ts.to_le_bytes()); // receivedAt
    f.extend(ts.to_le_bytes()); // publishedAt
    f.extend([0u8, 0, 0, 0]); // stp, flags, requestStatus, requestType
    assert_eq!(f.len(), 8 + 128);
    for s in [vid, cid, MARKET, "USER_CANCELED", "", ""] {
        f.push(u8::try_from(s.len()).unwrap());
        f.extend(s.as_bytes());
    }
    f
}

/// What the stub saw of the place, for the cancel's order event.
#[derive(Default)]
struct Placed {
    cid: String,
    price: String,
    size: String,
}

/// Answers each JSON-RPC frame the session sends by its method: the auth frame and the
/// subscriptions with an empty result, the arm with `enabled: true`, the place with its order
/// under [`VID`], the cancel queued and then the order event closing the order.
fn responder(placed: Arc<Mutex<Placed>>) -> Responder {
    Responder::new(move |frame| {
        let Frame::Text(text) = frame else {
            return Err("a binary frame from the client".to_owned());
        };
        let req: Value = serde_json::from_str(text).map_err(|e| e.to_string())?;
        let id = req["id"].clone();
        let params = &req["params"];
        let ok = |result: Value| {
            Frame::text(json!({"jsonrpc": "2.0", "result": result, "id": id}).to_string())
        };
        match req["method"].as_str().unwrap_or("") {
            "auth" | "subscribe" => Ok(vec![ok(json!({}))]),
            "order.cancel_on_disconnect" => Ok(vec![ok(json!({"enabled": true}))]),
            "order.create" => {
                let mut p = placed.lock().unwrap();
                p.cid = params["client_id"].as_str().unwrap_or("").to_owned();
                p.price = params["price"].as_str().unwrap_or("").to_owned();
                p.size = params["size"].as_str().unwrap_or("").to_owned();
                let order = json!({
                    "id": VID, "client_id": p.cid, "market": MARKET, "side": "BUY",
                    "type": "LIMIT", "instruction": "POST_ONLY", "price": p.price,
                    "size": p.size, "remaining_size": p.size, "status": "NEW",
                });
                Ok(vec![ok(json!({"order": order}))])
            }
            "order.cancel" => {
                let p = placed.lock().unwrap();
                let queued = ok(json!({"order_id": VID, "status": "QUEUED_FOR_CANCELLATION"}));
                let closed = order_closed(5_001, VID, &p.cid, &p.price, &p.size);
                Ok(vec![queued, Frame::Binary(closed)])
            }
            other => Err(format!("an unexpected method {other}")),
        }
    })
}

/// The order socket's script: one connection, eight frames answered (auth, four
/// subscriptions, the arm, the place, the cancel).
fn script(placed: Arc<Mutex<Placed>>) -> WsScript {
    let with = responder(placed);
    let mut steps = vec![Step::Accept];
    steps.extend((0..8).map(|_| Step::Respond {
        conn: 0,
        with: with.clone(),
    }));
    WsScript::new(steps)
}

/// Runs testnet_trade against `stub` with `opts`, bounded at 60 s: its report and what it
/// printed.
async fn run_against(opts: &Options) -> (trade::Report, String) {
    let buf = Rc::new(RefCell::new(Vec::<u8>::new()));
    let out: trade::Out = buf.clone();
    let run = trade::run(opts, None, secrets(), out);
    let ran = tokio::time::timeout(Duration::from_secs(60), run).await;
    let printed = String::from_utf8(buf.borrow().clone()).unwrap();
    let report = ran
        .unwrap_or_else(|_| panic!("testnet_trade did not finish in 60 s; it printed:\n{printed}"))
        .unwrap_or_else(|e| panic!("refused: {e}; it printed:\n{printed}"));
    (report, printed)
}

/// The JSON-RPC methods the session sent on the socket, in order.
fn methods(stub: &StubServer) -> Vec<String> {
    stub.connections()
        .iter()
        .flat_map(|c| c.received.iter())
        .filter_map(|f| match f {
            Frame::Text(t) => serde_json::from_str::<Value>(t).ok(),
            Frame::Binary(_) => None,
        })
        .map(|v| v["method"].as_str().unwrap_or("").to_owned())
        .collect()
}

/// The first word after `STEP ` of each step line, in order.
fn steps(printed: &str) -> Vec<String> {
    printed
        .lines()
        .filter_map(|l| l.strip_prefix("STEP "))
        .map(|l| l.split_whitespace().next().unwrap().to_owned())
        .collect()
}

#[tokio::test]
async fn testnet_trade_places_one_post_only_order_and_cancels_it_against_the_stub() {
    let placed = Arc::new(Mutex::new(Placed::default()));
    let stub = StubServer::start(script(Arc::clone(&placed)), routes())
        .await
        .unwrap();
    let opts = options(&stub, "10");
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    assert!(report.ok, "{printed}");
    assert_eq!(
        steps(&printed),
        [
            "login",
            "arm",
            "resync",
            "start",
            "place",
            "ack",
            "cancel",
            "cancel-ack",
            "closed",
            "stop"
        ],
        "{printed}"
    );
    assert_eq!(
        (report.places_sent, report.cancels_sent),
        (1, 1),
        "{printed}"
    );
    // Exactly one place and one cancel reached the venue, and nothing else order-affecting.
    let sent = methods(&stub);
    let count = |m: &str| sent.iter().filter(|s| s.as_str() == m).count();
    assert_eq!(count("order.create"), 1, "{sent:?}");
    assert_eq!(count("order.cancel"), 1, "{sent:?}");
    assert_eq!(
        count("order.cancel_all") + count("order.cancel_batch") + count("order.modify"),
        0,
        "{sent:?}"
    );
    assert_eq!(count("order.cancel_on_disconnect"), 1, "{sent:?}");
    assert_eq!(stub.connections().len(), 1);
    // The order: post-only, priced 3% under the fixture's best bid of 62000.2 on the 0.1 tick
    // (60140.1 after the floor), sized to the $11 resting cap on the 0.00001 step: 18 lots.
    let create = stub.connections()[0]
        .received
        .iter()
        .find_map(|f| match f {
            Frame::Text(t) if t.contains("order.create") => serde_json::from_str::<Value>(t).ok(),
            _ => None,
        })
        .unwrap();
    assert_eq!(create["params"]["instruction"], "POST_ONLY");
    assert_eq!(create["params"]["side"], "BUY");
    assert_eq!(create["params"]["price"], "60140.1");
    assert_eq!(create["params"]["size"], "0.00018");
    // The steps print what the owner needs.
    assert!(
        printed.contains("TESTNET a loopback test stub"),
        "{printed}"
    );
    assert!(printed.contains("OWNER-ASSISTED testnet run"), "{printed}");
    assert!(
        printed.contains("SEED position 0 lots seeded by hand"),
        "{printed}"
    );
    assert!(
        printed.contains("DONE ok places sent 1 cancels sent 1"),
        "{printed}"
    );
    // No credential, and not the session token, appears in what it printed.
    let (account, key) = synthetic();
    let lower = printed.to_ascii_lowercase();
    for secret in [&account, &key, &TOKEN.to_owned()] {
        let bare = secret.trim_start_matches("0x").to_ascii_lowercase();
        assert!(
            !lower.contains(&bare),
            "a credential was printed:\n{printed}"
        );
    }
}

#[tokio::test]
async fn an_unanswered_auth_frame_times_out_the_login_and_stops_with_nothing_placed() {
    // The login answers over HTTP, but the auth frame is never acknowledged.
    let script = WsScript::new(vec![Step::Accept, Step::Read { conn: 0 }]);
    let stub = StubServer::start(script, routes()).await.unwrap();
    let mut opts = options(&stub, "10");
    opts.step_timeout_secs = 1;
    let (report, printed) = run_against(&opts).await;
    assert!(!report.ok, "{printed}");
    assert_eq!(
        (report.places_sent, report.cancels_sent),
        (0, 0),
        "{printed}"
    );
    assert!(printed.contains("TIMEOUT login"), "{printed}");
    assert_eq!(steps(&printed), ["stop"], "{printed}");
    assert!(printed.contains("cancel all: 0 cancels sent"), "{printed}");
    assert!(printed.contains("DONE failed"), "{printed}");
    assert_eq!(methods(&stub), ["auth"]);
}

#[tokio::test]
async fn an_unanswered_place_times_out_and_stop_leaves_it_to_cancel_on_disconnect() {
    // Everything is answered but the place: the ack step times out and the order is not
    // acknowledged. Paradex takes no cancel by client id before the acknowledgement, so Stop's
    // cancel waits for one; none comes within the step timeout, so nothing more is sent and the
    // socket's close leaves the order to cancel-on-disconnect.
    let with = responder(Arc::new(Mutex::new(Placed::default())));
    let mut script = vec![Step::Accept];
    script.extend((0..6).map(|_| Step::Respond {
        conn: 0,
        with: with.clone(),
    }));
    script.push(Step::Read { conn: 0 });
    let stub = StubServer::start(WsScript::new(script), routes())
        .await
        .unwrap();
    let mut opts = options(&stub, "10");
    opts.step_timeout_secs = 1;
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(!report.ok, "{printed}");
    assert_eq!(
        steps(&printed),
        ["login", "arm", "resync", "start", "place", "stop"],
        "{printed}"
    );
    assert!(printed.contains("TIMEOUT ack"), "{printed}");
    assert!(printed.contains("FBC-m8vm"), "{printed}");
    assert!(
        printed.contains("1 orders not yet acknowledged"),
        "{printed}"
    );
    assert!(
        printed.contains("cancel-on-disconnect to cancel them"),
        "{printed}"
    );
    assert!(printed.contains("cancel all: 0 cancels sent"), "{printed}");
    assert_eq!(
        (report.places_sent, report.cancels_sent),
        (1, 0),
        "{printed}"
    );
    let sent = methods(&stub);
    assert_eq!(sent.iter().filter(|m| *m == "order.create").count(), 1);
    assert!(
        !sent
            .iter()
            .any(|m| m.starts_with("order.cancel_") && m != "order.cancel_on_disconnect")
    );
    assert!(!sent.iter().any(|m| m == "order.cancel"), "{sent:?}");
}

#[tokio::test]
async fn a_mainnet_chain_id_or_url_is_refused_before_any_connection() {
    let stub = StubServer::start(WsScript::new(vec![]), routes())
        .await
        .unwrap();
    // The stub's own URLs, but the mainnet chain id.
    let opts = options(&stub, "10");
    let out: trade::Out = Rc::new(RefCell::new(Vec::<u8>::new()));
    let refused = trade::run(&opts, Some("PRIVATE_SN_PARACLEAR_MAINNET"), secrets(), out)
        .await
        .unwrap_err();
    assert!(refused.contains("not the testnet chain id"), "{refused}");
    // Mainnet URLs, testnet chain: refused naming the URL.
    for (rest, ws, named) in [
        (
            "https://api.prod.paradex.trade/v1",
            "wss://ws.api.prod.paradex.trade/v1",
            "--rest-url",
        ),
        (
            "https://api.testnet.paradex.trade/v1",
            "wss://ws.api.prod.paradex.trade/v1",
            "--ws-url",
        ),
        (
            "http://api.testnet.paradex.trade/v1",
            "wss://ws.api.testnet.paradex.trade/v1",
            "--rest-url",
        ),
    ] {
        let mut opts = options(&stub, "10");
        opts.rest_url = rest.to_owned();
        opts.ws_url = ws.to_owned();
        let out: trade::Out = Rc::new(RefCell::new(Vec::<u8>::new()));
        let refused = trade::run(&opts, None, secrets(), out).await.unwrap_err();
        assert!(refused.starts_with(named), "{refused}");
        assert!(refused.contains("testnet only"), "{refused}");
    }
    // Nothing connected: no socket, no HTTP request.
    assert!(stub.connections().is_empty());
    assert!(stub.http_requests().is_empty());
}

#[tokio::test]
async fn an_order_below_the_minimum_notional_is_refused_before_the_session_connects() {
    let stub = StubServer::start(WsScript::new(vec![]), routes())
        .await
        .unwrap();
    let opts = options(&stub, "100");
    let out: trade::Out = Rc::new(RefCell::new(Vec::<u8>::new()));
    let refused = trade::run(&opts, None, secrets(), out).await.unwrap_err();
    assert!(
        refused.contains("below the market's minimum $100"),
        "{refused}"
    );
    // Only the order book was read; no socket was opened and no login made.
    assert_eq!(
        stub.http_requests(),
        [format!("GET /v1/orderbook/{MARKET}?depth=15")]
    );
    assert!(stub.connections().is_empty());
}

fn strings(args: &[&str]) -> Vec<String> {
    args.iter().map(|a| (*a).to_owned()).collect()
}

const MARKET_ARGS: [&str; 9] = [
    "--sole-trader",
    "--market",
    "BTC-USD-PERP",
    "--tick",
    "0.1",
    "--step",
    "0.00001",
    "--min-notional",
    "10",
];

#[test]
fn the_command_line_defaults_to_testnet_and_refuses_mainnet_urls() {
    let Ok(Parsed::Trade(opts)) = args::parse(strings(&MARKET_ARGS)) else {
        panic!("the market flags parse");
    };
    assert_eq!(opts.rest_url, "https://api.testnet.paradex.trade/v1");
    assert_eq!(opts.ws_url, "wss://ws.api.testnet.paradex.trade/v1");
    assert_eq!((opts.side, opts.away_bps), (OrderSide::Buy, 300));
    for (flag, url) in [
        ("--rest-url", "https://api.prod.paradex.trade/v1"),
        ("--ws-url", "wss://ws.api.prod.paradex.trade/v1"),
        ("--rest-url", "https://user@api.testnet.paradex.trade/v1"),
        (
            "--rest-url",
            "https://api.testnet.paradex.trade.evil.example/v1",
        ),
    ] {
        let mut argv = strings(&MARKET_ARGS);
        argv.extend(strings(&[flag, url]));
        let err = args::parse(argv).unwrap_err();
        assert!(err.starts_with(flag), "{flag} {url}: {err}");
    }
    // A loopback stub on one side only is refused too.
    let mut argv = strings(&MARKET_ARGS);
    argv.extend(strings(&["--rest-url", "http://127.0.0.1:9/v1"]));
    assert!(
        args::parse(argv)
            .unwrap_err()
            .contains("one is a loopback stub")
    );
    // Every market flag is required, and the bounds hold.
    let mut argv = strings(&MARKET_ARGS);
    argv.remove(0);
    assert!(
        args::parse(argv)
            .unwrap_err()
            .starts_with("--sole-trader is required")
    );
    let mut argv = strings(&MARKET_ARGS);
    argv.extend(strings(&[
        "--rest-url",
        "http://127.0.0.1:9/v1",
        "--ws-url",
        "ws://127.0.0.1:9/v1",
        "--socks5",
        "proxy.example:1080",
    ]));
    assert!(
        args::parse(argv)
            .unwrap_err()
            .starts_with("--socks5 with loopback stub URLs")
    );
    for missing in ["--market", "--tick", "--step", "--min-notional"] {
        let i = MARKET_ARGS.iter().position(|a| *a == missing).unwrap();
        let mut argv = strings(&MARKET_ARGS);
        argv.drain(i..i + 2);
        let err = args::parse(argv).unwrap_err();
        assert!(err.starts_with(missing), "{err}");
    }
    for (flag, bad) in [("--away-bps", "99"), ("--hold", "61"), ("--side", "both")] {
        let mut argv = strings(&MARKET_ARGS);
        argv.extend(strings(&[flag, bad]));
        assert!(args::parse(argv).unwrap_err().starts_with(flag));
    }
}

#[test]
fn the_guard_admits_only_the_testnet_chain_id() {
    let (rest, ws) = (args::TESTNET_REST, args::TESTNET_WS);
    assert_eq!(
        args::testnet_guard(rest, ws, None),
        Ok(args::Target::Testnet)
    );
    assert_eq!(
        args::testnet_guard(rest, ws, Some(TESTNET_CHAIN)),
        Ok(args::Target::Testnet)
    );
    for chain in ["PRIVATE_SN_PARACLEAR_MAINNET", "0x1", ""] {
        let err = args::testnet_guard(rest, ws, Some(chain)).unwrap_err();
        assert!(err.contains("not the testnet chain id"), "{chain}: {err}");
    }
}

#[test]
fn help_names_the_environment_the_flags_and_every_line() {
    let parsed = args::parse(strings(&["--help"])).unwrap();
    assert_eq!(parsed, Parsed::Help);
    for word in [
        auth::ACCOUNT_VAR,
        auth::KEY_VAR,
        args::CHAIN_VAR,
        "PRIVATE_SN_POTC_SEPOLIA",
        "--market",
        "--tick",
        "--step",
        "--min-notional",
        "--side",
        "--away-bps",
        "--hold",
        "--step-timeout",
        "--rest-url",
        "--ws-url",
        "--socks5",
        "--lease-dir",
        "--sole-trader",
        "STEP login",
        "STEP arm",
        "STEP resync",
        "STEP start",
        "STEP place",
        "STEP ack",
        "STEP cancel ",
        "STEP cancel-ack",
        "STEP closed",
        "STEP stop",
        "TIMEOUT",
        "DONE",
    ] {
        assert!(USAGE.contains(word), "--help does not mention {word}");
    }
}

#[test]
fn credentials_come_from_the_two_variables_and_a_missing_one_is_named_not_shown() {
    let (account, key) = synthetic();
    let env = |a: Option<&str>, k: Option<&str>| {
        let (a, k) = (a.map(str::to_owned), k.map(str::to_owned));
        move |var: &str| match var {
            "PARADEX_ACCOUNT_ADDRESS" => a.clone(),
            "PARADEX_PRIVATE_KEY" => k.clone(),
            _ => None,
        }
    };
    let creds = auth::paradex_secrets(env(Some(&account), Some(&key))).unwrap();
    assert_eq!(creds.get(ACCOUNT_ADDRESS).unwrap().expose(), account);
    assert_eq!(creds.get(SIGNING_KEY).unwrap().expose(), key);
    assert!(!format!("{creds:?}").contains(&account[2..]));
    let err = auth::paradex_secrets(env(Some(&account), None)).unwrap_err();
    assert!(err.starts_with("PARADEX_PRIVATE_KEY is not set"), "{err}");
    assert!(!err.contains(&account[2..]));
    let err = auth::paradex_secrets(env(Some(""), Some(&key))).unwrap_err();
    assert!(
        err.starts_with("PARADEX_ACCOUNT_ADDRESS is not set"),
        "{err}"
    );
    assert!(!err.contains(&key[2..]));
}

#[tokio::test]
async fn a_run_without_the_sole_trader_confirmation_or_through_a_proxy_to_a_stub_is_refused() {
    let stub = StubServer::start(WsScript::new(vec![]), routes())
        .await
        .unwrap();
    let mut opts = options(&stub, "10");
    opts.sole_trader = false;
    let out: trade::Out = Rc::new(RefCell::new(Vec::<u8>::new()));
    let refused = trade::run(&opts, None, secrets(), out).await.unwrap_err();
    assert!(
        refused.starts_with("--sole-trader is required"),
        "{refused}"
    );
    // A SOCKS5 proxy would reach its own host's loopback, not the stub on this machine.
    let mut opts = options(&stub, "10");
    opts.proxy = ProxyConfig::Socks5 {
        host: "127.0.0.1".to_owned(),
        port: 9,
    };
    let out: trade::Out = Rc::new(RefCell::new(Vec::<u8>::new()));
    let refused = trade::run(&opts, None, secrets(), out).await.unwrap_err();
    assert!(
        refused.starts_with("--socks5 with loopback stub URLs"),
        "{refused}"
    );
    assert!(stub.connections().is_empty());
    assert!(stub.http_requests().is_empty());
}

#[tokio::test]
async fn the_client_id_high_water_mark_is_read_before_minting_and_kept_after() {
    // A mark far above the wall-clock floor, as an earlier run would leave after its clock ran
    // ahead: the run mints above it and keeps the new mark before the place goes out.
    let placed = Arc::new(Mutex::new(Placed::default()));
    let stub = StubServer::start(script(Arc::clone(&placed)), routes())
        .await
        .unwrap();
    let opts = options(&stub, "10");
    let hwm = trade::HighWater::at(&opts.lease_dir);
    let mark = 1u64 << 60;
    hwm.write(mark).unwrap();
    let (report, printed) = run_against(&opts).await;
    assert!(report.ok, "{printed}");
    assert_eq!(hwm.read().unwrap(), mark + 1);
    // A mark that is not a number is refused before anything is sent.
    fs::write(
        opts.lease_dir.join("cid-high-water-account-1-ns-1"),
        "not a number",
    )
    .unwrap();
    let dir = opts.lease_dir.clone();
    let stub = StubServer::start(WsScript::new(vec![]), routes())
        .await
        .unwrap();
    let mut opts = options(&stub, "10");
    opts.lease_dir = dir;
    let out: trade::Out = Rc::new(RefCell::new(Vec::<u8>::new()));
    let refused = trade::run(&opts, None, secrets(), out).await.unwrap_err();
    assert!(refused.contains("not a high-water mark"), "{refused}");
    assert!(stub.connections().is_empty());
}

#[test]
fn the_resync_the_run_seeds_from_is_the_latest() {
    use fbc_core::{InstrumentId, MonoNs, SignedLots, WallNs};
    use fbc_oms::{ResyncReport, ResyncSnapshot};
    let resynced = |pos: i64| link::Note::Resynced {
        report: ResyncReport::default(),
        snapshot: ResyncSnapshot {
            watermark: WallNs(pos),
            requested_at: MonoNs(0),
            orders: Vec::new(),
            positions: vec![(InstrumentId::new(1), SignedLots(pos))],
        },
    };
    assert!(trade::latest_resync(&[]).is_none());
    // An earlier epoch's resync, a reconnect, then the current epoch's.
    let notes = [
        resynced(3),
        link::Note::EpochEnd(fbc_core::ConnKey { conn: 0, epoch: 1 }),
        resynced(-2),
    ];
    let (_, snap) = trade::latest_resync(&notes).unwrap();
    assert_eq!(snap.positions, [(InstrumentId::new(1), SignedLots(-2))]);
}
