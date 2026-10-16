//! FBC-p0aa's done line: mainnet_smoke's wiring, run against the conformance kit's stub server on
//! 127.0.0.1 standing in for Paradex MAINNET, with synthetic credentials, goes through the chain
//! check, the mark, login, arm, resync, start, place, ack, cancel, cancel ack and stop in that
//! order, the session sending exactly one place (`order.create`) and one cancel
//! (`order.cancel`), and Stop's instrument cancel-all (`order.cancel_all` for the market: a
//! trustworthy resync showed every open order); a run without `--mainnet` or `--sole-trader`,
//! with a URL or chain id that is not mainnet's, or whose `GET /system/config` names another
//! chain is refused before any order; Start is refused unless the resync was trustworthy; and no
//! credential appears in what it prints.
//!
//! The run is tested through [`Declared`], a stand-in for the Paradex factory that is Paradex's in
//! everything but the snapshot source its caps declare: [`TRUSTED`] for the full path, and
//! [`UNTRUSTED`] for the refusals, so each test holds whatever Paradex's adapter declares
//! (FBC-2f7w's decision record may flip it); `smoke::trusted` on Paradex's own factory is checked
//! to agree with `ParadexFactory.caps()`. The stub answers as in
//! `tests/testnet_trade.rs`, plus `GET /v1/system/config` and `GET /v1/markets/summary`, and
//! the instrument cancel-all with `{"status":"ok"}`. The credentials are the synthetic account
//! and key of `fixtures/paradex/signing`. The run is on real time, bounded at 60 s.

// testnet_trade's options and its round trip, reused by path: only part of each is used here.
#[allow(dead_code, unused_imports)]
#[path = "../examples/testnet_trade/args.rs"]
mod args;
#[path = "../src/auth_mainnet.rs"]
mod auth_mainnet;
#[allow(dead_code)]
#[path = "../src/link.rs"]
mod link;
#[path = "../examples/mainnet_smoke/smoke.rs"]
mod smoke;
#[path = "../examples/mainnet_smoke/smoke_args.rs"]
mod smoke_args;
#[allow(dead_code)]
#[path = "../src/testnet.rs"]
mod testnet;
#[allow(dead_code)]
#[path = "../examples/testnet_trade/trade.rs"]
mod trade;

use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::path::PathBuf;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use fbc_conformance::{
    Frame, HttpReply, HttpRouter, PathPattern, Responder, Step, StubServer, WsScript,
};
use fbc_core::{
    AccountSummary, AssetKey, ConfigError, EndpointPlan, ExecCodec, ExecEndpoint, FieldSpec,
    HttpPlan, InstrumentSpecDraft, MdCodec, Secret, Secrets, SignedLots, SnapshotSource, SpecTable,
    Subscription, SymbolError, VenueCaps, VenueConfig, VenueError, VenueFactory,
};
use fbc_runtime::http::Method;
use fbc_venue_paradex::ParadexFactory;
use fbc_venue_paradex::auth::{ACCOUNT_ADDRESS, SIGNING_KEY};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use smoke_args::{Parsed, Smoke};

/// The session token the stub's login gives: made-up text that never looks like a JWT.
const TOKEN: &str = "SYNTHETIC.session-token.mainnet-smoke";
/// The venue id the stub gives the order.
const VID: &str = "1759500000000000888";
const MARKET: &str = "BTC-USD-PERP";
/// The stub's mark: between the fixture book's best bid (62000.2) and ask (62000.5).
const MARK: &str = "62000.3";

/// Paradex's factory in everything but its caps, which declare the snapshot source given.
struct Declared(SnapshotSource);

/// Declares the snapshot source trustworthy: what Paradex's will be once FBC-2f7w's record flips
/// it.
static TRUSTED: Declared = Declared(SnapshotSource::Trustworthy);

/// Declares the snapshot source untrustworthy, whatever Paradex's own adapter declares.
static UNTRUSTED: Declared = Declared(SnapshotSource::Untrustworthy);

impl VenueFactory for Declared {
    fn id(&self) -> &'static str {
        ParadexFactory.id()
    }
    fn config_schema(&self) -> &'static [FieldSpec] {
        ParadexFactory.config_schema()
    }
    fn caps(&self, cfg: &VenueConfig) -> Result<VenueCaps, ConfigError> {
        let mut caps = ParadexFactory.caps(cfg)?;
        if let Some(exec) = caps.exec.as_mut() {
            exec.order.snapshot_source = self.0;
        }
        Ok(caps)
    }
    fn parse_fbc_common_symbol(&self, s: &str) -> Result<AssetKey, SymbolError> {
        ParadexFactory.parse_fbc_common_symbol(s)
    }
    fn discover(
        &self,
        cfg: &VenueConfig,
    ) -> Result<HttpPlan<Vec<InstrumentSpecDraft>>, VenueError> {
        ParadexFactory.discover(cfg)
    }
    fn plan_md(
        &self,
        cfg: &VenueConfig,
        specs: &SpecTable,
        subs: &BTreeSet<Subscription>,
    ) -> Result<Vec<EndpointPlan>, VenueError> {
        ParadexFactory.plan_md(cfg, specs, subs)
    }
    fn md_codec(&self, cfg: &VenueConfig, ep: &EndpointPlan) -> Box<dyn MdCodec> {
        ParadexFactory.md_codec(cfg, ep)
    }
    fn plan_exec(&self, cfg: &VenueConfig) -> Result<Vec<ExecEndpoint>, VenueError> {
        ParadexFactory.plan_exec(cfg)
    }
    fn exec_codec(
        &self,
        cfg: &VenueConfig,
        creds: Secrets,
    ) -> Option<Result<Box<dyn ExecCodec>, VenueError>> {
        ParadexFactory.exec_codec(cfg, creds)
    }
    fn test_connection(
        &self,
        cfg: &VenueConfig,
        creds: Secrets,
    ) -> Option<Result<HttpPlan<AccountSummary>, VenueError>> {
        ParadexFactory.test_connection(cfg, creds)
    }
}

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

/// A directory removed when it is dropped.
struct Removed(PathBuf);

impl Drop for Removed {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

thread_local! {
    /// The lease directories the test on this thread made: removed when its thread ends.
    static MADE: RefCell<Vec<Removed>> = const { RefCell::new(Vec::new()) };
}

/// A lease directory of the test's own, removed when the test's thread ends.
fn lease_dir() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir =
        std::env::temp_dir().join(format!("fbc-mainnet-smoke-test-{}-{n}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    MADE.with(|made| made.borrow_mut().push(Removed(dir.clone())));
    dir
}

fn strings(args: &[&str]) -> Vec<String> {
    args.iter().map(|a| (*a).to_owned()).collect()
}

/// The market, the caps at their largest and the order, as the owner types them.
const ORDER_ARGS: [&str; 24] = [
    "--mainnet",
    "--sole-trader",
    "--namespace",
    "1",
    "--market",
    MARKET,
    "--tick",
    "0.1",
    "--step",
    "0.00001",
    "--min-notional",
    "10",
    "--resting-cap-usd",
    "11",
    "--inventory-cap-usd",
    "50",
    "--order-usd",
    "11",
    "--side",
    "buy",
    "--away-bps",
    "300",
    "--step-timeout",
    "10",
];

/// [`ORDER_ARGS`] with `extra` after them, parsed.
fn parse_with(extra: &[&str]) -> Result<Parsed, String> {
    let mut all = strings(&ORDER_ARGS);
    all.extend(strings(extra));
    smoke_args::parse(all)
}

/// The run's options against `stub`, parsed from the command line.
fn smoke_on(stub: &StubServer) -> Smoke {
    let dir = lease_dir();
    let rest = stub.http_url("/v1");
    let ws = stub.ws_url("/v1");
    let extra = [
        "--rest-url",
        rest.as_str(),
        "--ws-url",
        ws.as_str(),
        "--lease-dir",
        dir.to_str().unwrap(),
    ];
    match parse_with(&extra) {
        Ok(Parsed::Smoke(smoke)) => *smoke,
        other => panic!("the stub's command line parses: {other:?}"),
    }
}

fn reply(status: u16, body: impl Into<Vec<u8>>) -> HttpReply {
    HttpReply {
        status,
        body: body.into(),
    }
}

/// The stub's `GET /v1/system/config` naming `chain`.
fn system_config(chain: &str) -> String {
    json!({
        "starknet_chain_id": chain, "l1_chain_id": "1",
        "block_explorer_url": "https://example.invalid/", "environment": "stub",
    })
    .to_string()
}

/// The stub's `GET /v1/markets/summary?market=` answer with `mark`.
fn summary(mark: &str) -> String {
    json!({"results": [{
        "symbol": MARKET, "mark_price": mark, "oracle_price": mark,
        "last_traded_price": mark, "bid": "62000.2", "ask": "62000.5",
        "funding_rate": "0.0001", "open_interest": "1", "volume_24h": "1",
        "created_at": 1_759_500_000_000u64,
    }]})
    .to_string()
}

/// The stub's HTTP answers: the chain, the mark, the login, the order book and the resync's two
/// reads (`positions`).
fn routes_full(chain: &str, mark: &str, positions: &str) -> HttpRouter {
    let book = fs::read(fixture("paradex/rest/orderbook-btc-2002.json")).unwrap();
    HttpRouter::new()
        .route(
            Method::GET,
            PathPattern::exact("/v1/system/config"),
            reply(200, system_config(chain)),
        )
        .route(
            Method::GET,
            PathPattern::exact("/v1/markets/summary"),
            reply(200, summary(mark)),
        )
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
            reply(200, r#"{"results":[]}"#),
        )
        .route(
            Method::GET,
            PathPattern::exact("/v1/positions"),
            reply(200, positions.to_owned()),
        )
}

/// [`routes_full`] on mainnet's chain, at [`MARK`], flat.
fn routes() -> HttpRouter {
    routes_full(smoke::MAINNET_CHAIN, MARK, r#"{"results":[]}"#)
}

/// A decimal string as Paradex's SBE 10^-8 mantissa.
fn e8(text: &str) -> i64 {
    let d = Decimal::from_str(text).unwrap() * Decimal::from(100_000_000);
    i64::try_from(d.mantissa() / 10i128.pow(d.scale())).unwrap()
}

/// An `OrderEvent` (template 20) at schema 1:2, as `tests/testnet_trade.rs` builds it: our
/// post-only limit buy `cid` of `size` at `price`, venue id `vid`, CLOSED by USER_CANCELED with
/// nothing filled. The account is 32 made-up bytes.
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
    f.extend(e8(size).to_le_bytes()); // sizeOpen
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

/// Answers each JSON-RPC frame by its method: the auth frame and the subscriptions with an
/// empty result, the arm with `enabled: true`, the place with its order under [`VID`], the
/// cancel queued and then the order event closing the order, the cancel-all with `ok`.
fn responder(placed: Arc<Mutex<Placed>>) -> Responder {
    Responder::new(move |frame: &Frame| {
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
            "order.cancel_all" => Ok(vec![ok(json!({"status": "ok"}))]),
            other => Err(format!("an unexpected method {other}")),
        }
    })
}

/// The order socket's script: one connection, `frames` frames answered.
fn script(placed: Arc<Mutex<Placed>>, frames: usize) -> WsScript {
    let with = responder(placed);
    let mut steps = vec![Step::Accept];
    steps.extend((0..frames).map(|_| Step::Respond {
        conn: 0,
        with: with.clone(),
    }));
    WsScript::new(steps)
}

/// A script that accepts nothing: a run refused before the session connects.
fn no_script() -> WsScript {
    WsScript::new(Vec::new())
}

/// Runs mainnet_smoke on `factory` with `smoke`, bounded at 60 s: what it returned and what it
/// printed.
async fn run_on(
    smoke: &Smoke,
    chain: Option<&str>,
    factory: &'static dyn VenueFactory,
) -> (Result<trade::Report, String>, String) {
    let buf = Rc::new(RefCell::new(Vec::<u8>::new()));
    let out: testnet::Out = buf.clone();
    let run = smoke::run(smoke, chain, secrets(), out, factory);
    let ran = tokio::time::timeout(Duration::from_secs(60), run).await;
    let printed = String::from_utf8(buf.borrow().clone()).unwrap();
    let ran = ran
        .unwrap_or_else(|_| panic!("mainnet_smoke did not finish in 60 s; it printed:\n{printed}"));
    (ran, printed)
}

/// The JSON-RPC methods the session sent on the socket, in order, each with its params.
fn sent(stub: &StubServer) -> Vec<(String, Value)> {
    stub.connections()
        .iter()
        .flat_map(|c| c.received.iter())
        .filter_map(|f| match f {
            Frame::Text(t) => serde_json::from_str::<Value>(t).ok(),
            Frame::Binary(_) => None,
        })
        .map(|v| {
            (
                v["method"].as_str().unwrap_or("").to_owned(),
                v["params"].clone(),
            )
        })
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

/// Asserts no credential, nor the session token, appears in `printed`.
fn no_credential_in(printed: &str) {
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
async fn mainnet_smoke_places_one_post_only_order_cancels_it_and_stops_against_a_trustworthy_stub()
{
    let placed = Arc::new(Mutex::new(Placed::default()));
    // auth, four subscriptions, the arm, the place, the cancel and Stop's cancel-all.
    let stub = StubServer::start(script(Arc::clone(&placed), 9), routes())
        .await
        .unwrap();
    let smoke = smoke_on(&stub);
    let (ran, printed) = run_on(&smoke, None, &TRUSTED).await;
    stub.finished().await.unwrap();
    let report = ran.unwrap_or_else(|e| panic!("refused: {e}; it printed:\n{printed}"));

    assert!(report.ok, "{printed}");
    assert_eq!(
        steps(&printed),
        [
            "chain",
            "mark",
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
    // One place and one cancel of it, plus Stop's cancel-all of the market (a trustworthy
    // resync: never an account cancel-all), and nothing else order-affecting. The report counts
    // the cancel-all among the cancels sent.
    assert_eq!(
        (report.places_sent, report.cancels_sent),
        (1, 2),
        "{printed}"
    );
    let sent = sent(&stub);
    let count = |m: &str| sent.iter().filter(|(s, _)| s == m).count();
    assert_eq!(count("order.create"), 1, "{sent:?}");
    assert_eq!(count("order.cancel"), 1, "{sent:?}");
    assert_eq!(count("order.cancel_all"), 1, "{sent:?}");
    assert_eq!(
        count("order.cancel_batch") + count("order.modify"),
        0,
        "{sent:?}"
    );
    let (_, all) = sent.iter().find(|(m, _)| m == "order.cancel_all").unwrap();
    assert_eq!(all["market"], MARKET, "{sent:?}");
    let (_, create) = sent.iter().find(|(m, _)| m == "order.create").unwrap();
    assert_eq!(create["instruction"], "POST_ONLY");
    assert_eq!(create["side"], "BUY");
    // 3% under the best bid of 62000.2 on the 0.1 tick, $11 on the 0.00001 step: 18 lots
    // ($11.16 at the mark, above the $10 minimum).
    assert_eq!(create["price"], "60140.1");
    assert_eq!(create["size"], "0.00018");
    assert!(
        printed.contains("caps: resting 18 lots per side, inventory 80 lots"),
        "{printed}"
    );
    assert!(
        printed.contains("ORDER notional $11.1601 at the mark 62000.3"),
        "{printed}"
    );
    assert!(
        printed.contains("MAINNET a loopback test stub"),
        "{printed}"
    );
    assert!(printed.contains("the snapshot is trustworthy"), "{printed}");
    assert!(printed.contains("an instrument cancel-all"), "{printed}");
    // Never a hand seed, never an owner-assisted testnet run.
    assert!(!printed.contains("SEED"), "{printed}");
    assert!(!printed.contains("OWNER-ASSISTED"), "{printed}");
    assert!(
        printed.contains("DONE ok places sent 1 cancels sent 2"),
        "{printed}"
    );
    // The chain and the mark were read before anything else.
    let requests = stub.http_requests();
    assert_eq!(requests[0], "GET /v1/system/config", "{requests:?}");
    assert_eq!(
        requests[1],
        format!("GET /v1/markets/summary?market={MARKET}"),
        "{requests:?}"
    );
    no_credential_in(&printed);
}

#[tokio::test]
async fn an_untrustworthy_snapshot_source_refuses_the_run_before_anything_connects() {
    let stub = StubServer::start(no_script(), routes()).await.unwrap();
    let smoke = smoke_on(&stub);
    let (ran, printed) = run_on(&smoke, None, &UNTRUSTED).await;
    let err = ran
        .err()
        .unwrap_or_else(|| panic!("the run was not refused:\n{printed}"));
    assert!(err.contains("untrustworthy"), "{err}");
    assert!(err.contains("FBC-2f7w"), "{err}");
    assert!(err.contains("never seeded by hand"), "{err}");
    assert!(
        stub.http_requests().is_empty(),
        "{:?}",
        stub.http_requests()
    );
    assert!(stub.connections().is_empty());
    assert!(printed.is_empty(), "{printed}");
    // Paradex's own factory is admitted exactly when its caps declare the snapshot trustworthy
    // (Untrustworthy until FBC-2f7w's record flips it).
    let cfg = smoke::config(&smoke.trade);
    let declared = ParadexFactory
        .caps(&cfg)
        .unwrap()
        .exec
        .map(|exec| exec.order.snapshot_source);
    assert_eq!(
        smoke::trusted(&ParadexFactory, &cfg).is_ok(),
        declared == Some(SnapshotSource::Trustworthy),
        "{declared:?}"
    );
}

#[tokio::test]
async fn a_resync_that_is_not_trustworthy_refuses_start_with_nothing_placed() {
    // The round trip run as mainnet_smoke runs it (never seeded by hand) on a venue declaring an
    // untrustworthy snapshot, past the run's up-front check: the resync seeds nothing, so Start
    // is refused.
    let placed = Arc::new(Mutex::new(Placed::default()));
    // auth, four subscriptions and the arm; Stop has nothing to cancel.
    let stub = StubServer::start(script(placed, 6), routes())
        .await
        .unwrap();
    let smoke = smoke_on(&stub);
    let buf = Rc::new(RefCell::new(Vec::<u8>::new()));
    let out: testnet::Out = buf.clone();
    let lines = testnet::Lines {
        out,
        t0: Instant::now(),
    };
    let net = smoke::network(&UNTRUSTED, Decimal::from_str(MARK).unwrap());
    let cfg = smoke::config(&smoke.trade);
    let report = trade::run_on(&smoke.trade, &net, cfg, secrets(), lines)
        .await
        .unwrap();
    stub.finished().await.unwrap();
    let printed = String::from_utf8(buf.borrow().clone()).unwrap();
    assert!(!report.ok, "{printed}");
    assert_eq!(
        steps(&printed),
        ["login", "arm", "resync", "stop"],
        "{printed}"
    );
    assert!(printed.contains("Start refused"), "{printed}");
    assert!(printed.contains("never seeds one by hand"), "{printed}");
    assert_eq!((report.places_sent, report.cancels_sent), (0, 0));
    assert!(
        sent(&stub)
            .iter()
            .all(|(m, _)| !m.starts_with("order.") || m == "order.cancel_on_disconnect"),
        "{:?}",
        sent(&stub)
    );
    no_credential_in(&printed);
}

#[tokio::test]
async fn a_position_on_the_market_refuses_start_with_nothing_placed() {
    let placed = Arc::new(Mutex::new(Placed::default()));
    let long = json!({"results": [{
        "market": MARKET, "side": "LONG", "size": "0.00002", "status": "OPEN",
        "average_entry_price": "62000",
    }]})
    .to_string();
    let stub = StubServer::start(
        script(placed, 6),
        routes_full(smoke::MAINNET_CHAIN, MARK, &long),
    )
    .await
    .unwrap();
    let smoke = smoke_on(&stub);
    let (ran, printed) = run_on(&smoke, None, &TRUSTED).await;
    stub.finished().await.unwrap();
    let report = ran.unwrap();
    assert!(!report.ok, "{printed}");
    assert!(
        printed.contains("Start refused: the account holds 2 lots"),
        "{printed}"
    );
    assert_eq!(report.places_sent, 0, "{printed}");
    assert!(!steps(&printed).contains(&"start".to_owned()), "{printed}");
}

#[tokio::test]
async fn a_system_config_naming_another_chain_is_refused_before_any_order() {
    for (chain, shown) in [
        (
            "PRIVATE_SN_PARACLEAR_TESTNET",
            "PRIVATE_SN_PARACLEAR_TESTNET",
        ),
        ("PRIVATE_SN_POTC_SEPOLIA", "PRIVATE_SN_POTC_SEPOLIA"),
        // Not shaped like a chain's name: never echoed.
        ("0xdeadbeef secret", "not shown"),
    ] {
        let stub = StubServer::start(no_script(), routes_full(chain, MARK, r#"{"results":[]}"#))
            .await
            .unwrap();
        let smoke = smoke_on(&stub);
        let (ran, printed) = run_on(&smoke, None, &TRUSTED).await;
        let err = ran
            .err()
            .unwrap_or_else(|| panic!("not refused:\n{printed}"));
        assert!(err.contains("PRIVATE_SN_PARACLEAR_MAINNET"), "{err}");
        assert!(err.contains(shown), "{err}");
        assert!(!err.contains("deadbeef"), "{err}");
        assert_eq!(stub.http_requests(), ["GET /v1/system/config"]);
        assert!(stub.connections().is_empty());
    }
    // An answer that is not 2xx, or names no chain, is refused too.
    assert!(smoke::decode_chain(b"{}").is_err());
    assert!(smoke::decode_chain(b"not json").is_err());
    assert_eq!(
        smoke::decode_chain(system_config("PRIVATE_SN_PARACLEAR_MAINNET").as_bytes()).unwrap(),
        "PRIVATE_SN_PARACLEAR_MAINNET"
    );
    let stub = StubServer::start(
        no_script(),
        HttpRouter::new().route(
            Method::GET,
            PathPattern::exact("/v1/system/config"),
            reply(503, "down"),
        ),
    )
    .await
    .unwrap();
    let smoke = smoke_on(&stub);
    let (ran, _) = run_on(&smoke, None, &TRUSTED).await;
    let err = ran.err().unwrap();
    assert!(err.contains("GET /system/config: status 503"), "{err}");
    assert!(stub.connections().is_empty());
}

#[tokio::test]
async fn an_order_below_the_minimum_notional_at_the_mark_is_refused_before_the_session_connects() {
    // $11 at 60140.1 is 18 lots; at a mark of 50000 that is $9, below the $10 minimum: Paradex
    // would refuse it after accepting it (the owner's 2026-10-16 testnet run).
    let stub = StubServer::start(
        no_script(),
        routes_full(smoke::MAINNET_CHAIN, "50000", r#"{"results":[]}"#),
    )
    .await
    .unwrap();
    let smoke = smoke_on(&stub);
    let (ran, printed) = run_on(&smoke, None, &TRUSTED).await;
    let err = ran
        .err()
        .unwrap_or_else(|| panic!("not refused:\n{printed}"));
    assert!(err.contains("at the mark price 50000 is $9"), "{err}");
    assert!(err.contains("below the market's minimum $10"), "{err}");
    assert!(stub.connections().is_empty());
}

#[test]
fn the_mark_is_the_markets_own_positive_mark_price() {
    let mark = smoke::decode_mark(summary("62000.3").as_bytes(), MARKET).unwrap();
    assert_eq!(mark, Decimal::from_str("62000.3").unwrap());
    // Another market's summary, none, a missing or non-positive mark: refused.
    for body in [
        summary("62000.3").replace(MARKET, "ETH-USD-PERP"),
        r#"{"results":[]}"#.to_owned(),
        r#"{"results":[{"symbol":"BTC-USD-PERP"}]}"#.to_owned(),
        summary("0"),
        summary("-1"),
        summary("x"),
        "not json".to_owned(),
    ] {
        assert!(
            smoke::decode_mark(body.as_bytes(), MARKET).is_err(),
            "{body}"
        );
    }
}

#[test]
fn the_inventory_cap_is_converted_at_no_less_than_the_mark() {
    let Ok(Parsed::Smoke(smoke)) = parse_with(&[]) else {
        panic!("parses");
    };
    let opts = &smoke.trade;
    let specs = trade::specs(&ParadexFactory, &smoke::config(opts), opts).unwrap();
    let bytes = fs::read(fixture("paradex/rest/orderbook-btc-2002.json")).unwrap();
    let book = fbc_venue_paradex::md::rest::decode_orderbook(&bytes, &specs).unwrap();
    let priced = trade::price(opts, &book).unwrap();
    assert_eq!(priced.inventory.get(), 80);
    // No mark: unchanged.
    let same = trade::at_mark(opts, trade::price(opts, &book).unwrap(), None).unwrap();
    assert_eq!(
        (same.inventory, same.mark_notional),
        (priced.inventory, None)
    );
    // A mark above the ask: $50 at 70000 is 71 lots.
    let high = trade::at_mark(opts, priced, Some(Decimal::from(70_000))).unwrap();
    assert_eq!(high.inventory.get(), 71);
    assert_eq!(high.mark_notional, Some(Decimal::from_str("12.6").unwrap()));
}

#[test]
fn start_is_refused_unless_the_resync_was_trustworthy_and_the_account_flat() {
    // An owner-assisted testnet run seeds by hand: never refused here.
    assert_eq!(trade::start_refusal(true, true, SignedLots(5)), None);
    let untrusted = trade::start_refusal(false, true, SignedLots(0)).unwrap();
    assert!(untrusted.contains("FBC-2f7w"), "{untrusted}");
    let held = trade::start_refusal(false, false, SignedLots(-3)).unwrap();
    assert!(held.contains("-3 lots"), "{held}");
    assert_eq!(trade::start_refusal(false, false, SignedLots(0)), None);
}

#[tokio::test]
async fn a_run_not_confirmed_mainnet_and_sole_trader_is_refused_before_anything_connects() {
    for (flag, why) in [
        ("--mainnet", "--mainnet is required"),
        ("--sole-trader", "--sole-trader is required"),
    ] {
        let args: Vec<&str> = ORDER_ARGS.iter().copied().filter(|a| *a != flag).collect();
        let err = smoke_args::parse(strings(&args)).err().unwrap();
        assert!(err.contains(why), "{err}");
    }
    // Options built some other way are checked again by the run, before anything connects.
    let stub = StubServer::start(no_script(), routes()).await.unwrap();
    for unconfirm in [
        |s: &mut Smoke| s.mainnet = false,
        |s: &mut Smoke| s.trade.sole_trader = false,
    ] {
        let mut smoke = smoke_on(&stub);
        unconfirm(&mut smoke);
        let (ran, _) = run_on(&smoke, None, &TRUSTED).await;
        assert!(ran.is_err());
    }
    assert!(stub.http_requests().is_empty());
    assert!(stub.connections().is_empty());
}

#[test]
fn the_command_line_defaults_to_mainnet_and_refuses_any_other_url() {
    let Ok(Parsed::Smoke(smoke)) = parse_with(&[]) else {
        panic!("the mainnet flags parse");
    };
    assert!(smoke.mainnet);
    assert_eq!(smoke.trade.rest_url, "https://api.prod.paradex.trade/v1");
    assert_eq!(smoke.trade.ws_url, "wss://ws.api.prod.paradex.trade/v1");
    assert_eq!(smoke.trade.order_usd, Decimal::from(11));
    assert!(
        smoke
            .trade
            .lease_dir
            .ends_with(".fueledbychai/mainnet_smoke"),
        "{:?}",
        smoke.trade.lease_dir
    );
    for (flag, url) in [
        ("--rest-url", "https://api.testnet.paradex.trade/v1"),
        ("--ws-url", "wss://ws.api.testnet.paradex.trade/v1"),
        ("--rest-url", "https://api.prod.paradex.trade/v2"),
        ("--rest-url", "https://api.prod.paradex.trade/v1/"),
        ("--ws-url", "ws://ws.api.prod.paradex.trade/v1"),
        ("--rest-url", "https://evil.example/v1"),
    ] {
        let err = parse_with(&[flag, url])
            .err()
            .unwrap_or_else(|| panic!("{url}"));
        assert!(err.contains("mainnet"), "{err}");
        assert!(!err.contains(url), "{err}");
    }
    // A loopback stub on one URL only.
    let err = parse_with(&["--rest-url", "http://127.0.0.1:9/v1"])
        .err()
        .unwrap();
    assert!(err.contains("both"), "{err}");
}

#[tokio::test]
async fn the_guard_admits_only_the_mainnet_chain_id() {
    let (rest, ws) = (smoke::MAINNET_REST, smoke::MAINNET_WS);
    assert_eq!(smoke::mainnet_guard(rest, ws, None), Ok(false));
    for chain in [
        smoke::MAINNET_CHAIN,
        smoke::MAINNET_CHAIN_DECIMAL,
        &format!("0x{}", smoke::MAINNET_CHAIN_HEX),
        &format!("0X{}", smoke::MAINNET_CHAIN_HEX.to_ascii_uppercase()),
    ] {
        assert_eq!(
            smoke::mainnet_guard(rest, ws, Some(chain)),
            Ok(false),
            "{chain}"
        );
    }
    for chain in [
        "PRIVATE_SN_PARACLEAR_TESTNET",
        testnet::TESTNET_CHAIN_DECIMAL,
        "PRIVATE_SN_POTC_SEPOLIA",
        "0xabc123",
    ] {
        let err = smoke::mainnet_guard(rest, ws, Some(chain)).err().unwrap();
        assert!(err.contains("PARADEX_CHAIN_ID"), "{err}");
        assert!(!err.contains(chain), "{err}");
    }
    // The felt is the name's ASCII bytes as one number, in hex and in decimal.
    let hex: String = smoke::MAINNET_CHAIN
        .bytes()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert_eq!(hex, smoke::MAINNET_CHAIN_HEX);
    assert_eq!(
        smoke::MAINNET_CHAIN_DECIMAL,
        "8458834024819506728615521019831122032732688838300957472069977523540"
    );
    // A run with the testnet chain id set is refused before anything connects: run against a
    // loopback stub, which sees nothing.
    let stub = StubServer::start(no_script(), routes()).await.unwrap();
    let smoke = smoke_on(&stub);
    let (ran, printed) = run_on(&smoke, Some("PRIVATE_SN_PARACLEAR_TESTNET"), &TRUSTED).await;
    assert!(ran.err().unwrap().contains("PARADEX_CHAIN_ID"));
    assert!(printed.is_empty(), "{printed}");
    assert!(
        stub.http_requests().is_empty(),
        "{:?}",
        stub.http_requests()
    );
    assert!(stub.connections().is_empty());
}

#[tokio::test]
async fn the_caps_are_at_most_eleven_dollars_resting_and_fifty_inventory() {
    // Smaller caps are taken, larger refused, and the order is bounded by the resting cap.
    let set = |flag: &str, value: &str| -> Result<Parsed, String> {
        let mut args = strings(&ORDER_ARGS);
        let at = args.iter().position(|a| a == flag).unwrap();
        args[at + 1] = value.to_owned();
        smoke_args::parse(args)
    };
    assert!(set("--resting-cap-usd", "11.01").is_err());
    assert!(set("--inventory-cap-usd", "50.5").is_err());
    let err = set("--resting-cap-usd", "12").err().unwrap();
    assert!(err.contains("at most $11"), "{err}");
    let err = set("--inventory-cap-usd", "51").err().unwrap();
    assert!(err.contains("at most $50"), "{err}");
    let err = set("--order-usd", "11.5").err().unwrap();
    assert!(err.contains("--resting-cap-usd"), "{err}");
    let Ok(Parsed::Smoke(small)) = set("--inventory-cap-usd", "20") else {
        panic!("a smaller cap parses");
    };
    assert_eq!(small.trade.inventory_cap_usd, Decimal::from(20));
    assert!(matches!(
        set("--resting-cap-usd", "11"),
        Ok(Parsed::Smoke(_))
    ));
    // The run checks them again, before anything connects: run against a loopback stub, which
    // sees nothing.
    let stub = StubServer::start(no_script(), routes()).await.unwrap();
    for (resting, inventory, why) in [(12, 50, "at most $11"), (11, 51, "at most $50")] {
        let mut big = smoke_on(&stub);
        big.trade.resting_cap_usd = Decimal::from(resting);
        big.trade.inventory_cap_usd = Decimal::from(inventory);
        let (ran, printed) = run_on(&big, None, &TRUSTED).await;
        let err = ran.err().unwrap();
        assert!(err.contains(why), "{err}");
        assert!(printed.is_empty(), "{printed}");
    }
    assert!(
        stub.http_requests().is_empty(),
        "{:?}",
        stub.http_requests()
    );
    assert!(stub.connections().is_empty());
}

#[test]
fn the_command_line_refuses_what_it_does_not_take_and_never_echoes_a_value() {
    // Shaped like a key (64 hex digits), built here so no key-shaped literal is in the source.
    let secretish = format!("0x{}", "1234567890abcdef".repeat(4));
    let secretish = secretish.as_str();
    for extra in [
        vec!["--side", secretish],
        vec!["--namespace", secretish],
        vec!["--market", secretish],
        vec!["--tick", secretish],
        vec!["--min-notional", secretish],
        vec!["--away-bps", "50"],
        vec!["--hold", "61"],
        vec!["--socks5", secretish],
        vec!["--lease-dir", "relative/dir"],
        vec![secretish],
        vec!["--unknown"],
        vec!["--rest-url"],
    ] {
        let err = parse_with(&extra)
            .err()
            .unwrap_or_else(|| panic!("{extra:?}"));
        assert!(!err.contains("1234567890abcdef"), "{err}");
    }
    let err = parse_with(&["--key=0xabcdef"]).err().unwrap();
    assert!(err.contains("--key") && !err.contains("abcdef"), "{err}");
    for (missing, value) in [("--market", 2), ("--order-usd", 2), ("--namespace", 2)] {
        let mut args = strings(&ORDER_ARGS);
        let at = args.iter().position(|a| a == missing).unwrap();
        args.drain(at..at + value);
        let err = smoke_args::parse(args).err().unwrap();
        assert!(err.contains(&format!("{missing} is required")), "{err}");
    }
    // Every optional flag parses.
    let Ok(Parsed::Smoke(smoke)) = parse_with(&[
        "--hold",
        "5",
        "--socks5",
        "127.0.0.1:1080",
        "--lease-dir",
        "/tmp/fbc-smoke",
        "--side",
        "sell",
    ]) else {
        panic!("the optional flags parse");
    };
    assert_eq!(smoke.trade.hold_secs, 5);
    assert_eq!(smoke.trade.side, args::OrderSide::Sell);
    assert_eq!(smoke.trade.lease_dir, PathBuf::from("/tmp/fbc-smoke"));
    assert!(matches!(
        smoke.trade.proxy,
        fbc_runtime::ProxyConfig::Socks5 { .. }
    ));
    // A SOCKS5 proxy with loopback stub URLs is refused.
    let err = parse_with(&[
        "--socks5",
        "127.0.0.1:1080",
        "--rest-url",
        "http://127.0.0.1:9/v1",
        "--ws-url",
        "ws://127.0.0.1:9/v1",
    ])
    .err()
    .unwrap();
    assert!(err.contains("--socks5"), "{err}");
}

#[test]
fn help_names_the_environment_the_prerequisites_and_every_line() {
    assert!(matches!(
        smoke_args::parse(strings(&["--help"])),
        Ok(Parsed::Help)
    ));
    assert!(matches!(
        smoke_args::parse(strings(&["-h"])),
        Ok(Parsed::Help)
    ));
    for needed in [
        "PARADEX_MAINNET_ACCOUNT_ADDRESS",
        "PARADEX_MAINNET_PRIVATE_KEY",
        "PARADEX_ACCOUNT_ADDRESS",
        "PRIVATE_SN_PARACLEAR_MAINNET",
        "--mainnet",
        "--sole-trader",
        "https://api.prod.paradex.trade/v1",
        "wss://ws.api.prod.paradex.trade/v1",
        "SEPARATE",
        "FBC-2f7w",
        "STEP chain",
        "STEP mark",
        "STEP stop",
        "TIMEOUT",
        "DONE ok|failed",
    ] {
        assert!(smoke_args::USAGE.contains(needed), "--help lacks {needed}");
    }
}

#[test]
fn credentials_come_only_from_the_mainnet_variables_and_are_never_shown() {
    let (account, key) = synthetic();
    let env = |pairs: &[(&str, &str)]| {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        move |var: &str| map.get(var).cloned()
    };
    let creds = auth_mainnet::paradex_mainnet_secrets(env(&[
        ("PARADEX_MAINNET_ACCOUNT_ADDRESS", &account),
        ("PARADEX_MAINNET_PRIVATE_KEY", &key),
    ]))
    .unwrap();
    assert!(creds.get(ACCOUNT_ADDRESS).is_some());
    assert!(creds.get(SIGNING_KEY).is_some());
    // A missing or empty mainnet variable is named; a testnet value is never taken in its place.
    for pairs in [
        vec![("PARADEX_MAINNET_PRIVATE_KEY", key.as_str())],
        vec![
            ("PARADEX_MAINNET_ACCOUNT_ADDRESS", ""),
            ("PARADEX_MAINNET_PRIVATE_KEY", key.as_str()),
        ],
        vec![
            ("PARADEX_ACCOUNT_ADDRESS", account.as_str()),
            ("PARADEX_MAINNET_PRIVATE_KEY", key.as_str()),
        ],
    ] {
        let err = auth_mainnet::paradex_mainnet_secrets(env(&pairs))
            .err()
            .unwrap();
        assert!(err.contains("PARADEX_MAINNET_ACCOUNT_ADDRESS"), "{err}");
        assert!(!err.contains(&account) && !err.contains(&key), "{err}");
    }
    let err = auth_mainnet::paradex_mainnet_secrets(env(&[(
        "PARADEX_MAINNET_ACCOUNT_ADDRESS",
        &account,
    )]))
    .err()
    .unwrap();
    assert!(err.contains("PARADEX_MAINNET_PRIVATE_KEY"), "{err}");
    // A testnet variable set in the shell refuses the run, the mainnet ones set or not.
    for testnet_var in ["PARADEX_ACCOUNT_ADDRESS", "PARADEX_PRIVATE_KEY"] {
        let err = auth_mainnet::paradex_mainnet_secrets(env(&[
            ("PARADEX_MAINNET_ACCOUNT_ADDRESS", &account),
            ("PARADEX_MAINNET_PRIVATE_KEY", &key),
            (testnet_var, "0x1"),
        ]))
        .err()
        .unwrap();
        assert!(err.contains(testnet_var), "{err}");
        assert!(err.contains("unset"), "{err}");
        assert!(!err.contains(&account) && !err.contains(&key), "{err}");
    }
    // An empty testnet variable is not set.
    assert!(
        auth_mainnet::paradex_mainnet_secrets(env(&[
            ("PARADEX_MAINNET_ACCOUNT_ADDRESS", &account),
            ("PARADEX_MAINNET_PRIVATE_KEY", &key),
            ("PARADEX_PRIVATE_KEY", ""),
        ]))
        .is_ok()
    );
}

#[test]
fn the_mainnet_network_never_seeds_by_hand_and_names_its_own_leases() {
    let net = smoke::network(&TRUSTED, Decimal::ONE);
    assert!(!net.owner_assisted);
    assert_eq!(net.label, "mainnet");
    assert_eq!(net.program, "mainnet_smoke");
    assert_eq!(net.mark, Some(Decimal::ONE));
    assert!(
        net.refusal_causes
            .contains("PARADEX_MAINNET_ACCOUNT_ADDRESS")
    );
    // The session signs for mainnet's chain.
    let Ok(Parsed::Smoke(smoke)) = parse_with(&[]) else {
        panic!("parses");
    };
    let cfg = smoke::config(&smoke.trade);
    assert_eq!(
        cfg.get(fbc_venue_paradex::auth::CHAIN_ID),
        Some(smoke::MAINNET_CHAIN)
    );
}
