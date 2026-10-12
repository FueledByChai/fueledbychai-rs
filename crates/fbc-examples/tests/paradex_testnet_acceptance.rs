//! FBC-xji8's done line: paradex_testnet_acceptance, run against the conformance kit's stub
//! server on 127.0.0.1 standing in for Paradex, with synthetic credentials, sends each scripted
//! step (place, batch create, amend, cancel, cancel-many, cancel-on-disconnect) once and maps its
//! outcome to the order lattice in the report it writes; the report holds the GET /orders and
//! GET /positions versus stream comparison table; the scrubbed fixtures pass
//! scripts/privacy-check.sh with no token or account value; and a mainnet URL or chain id is
//! refused before any connection.
//!
//! The stub is a small model of Paradex's account ([`Venue`]): the order socket's JSON-RPC
//! methods change its orders and answer as Paradex does (a reply, then an SBE `OrderEvent` at
//! 1:2 built here from Paradex's schema, as `tests/testnet_trade.rs` builds them), and its REST
//! answers (`GET /v1/orders`, `/v1/positions`, `/v1/orders-history`) are computed from the same
//! orders. Its cancel-on-disconnect closes the orders a connection placed once the stub's record
//! shows that connection ended, as the venue would ([`fbc_conformance::ConnectionsView`]). The
//! credentials are the synthetic account and key of `fixtures/paradex/signing`; the login's
//! token is JWT-shaped and every frame and answer carries the synthetic account address, so a
//! fixture or report that kept either would fail the privacy check. The run is on real time,
//! bounded at 120 s.

#[path = "../examples/paradex_testnet_acceptance/accept.rs"]
mod accept;
#[allow(dead_code)]
#[path = "../examples/paradex_testnet_acceptance/accept_args.rs"]
mod accept_args;
#[allow(dead_code)]
#[path = "../examples/testnet_trade/args.rs"]
mod args;
#[allow(dead_code)]
#[path = "../src/auth.rs"]
mod auth;
#[path = "../src/auth_rest.rs"]
mod auth_rest;
#[path = "../examples/paradex_testnet_acceptance/evidence.rs"]
mod evidence;
#[allow(dead_code)]
#[path = "../examples/testnet_trade/link.rs"]
mod link;
#[allow(dead_code)]
#[path = "../examples/testnet_trade/trade.rs"]
mod trade;

use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use accept_args::{AcceptOptions, AcceptParsed};
use args::{Options, OrderSide};
use fbc_conformance::{
    ConnectionsView, Frame, HttpReply, HttpRequest, HttpRouter, PathPattern, Responder, Step,
    StubServer, WsScript,
};
use fbc_core::{Secret, Secrets};
use fbc_runtime::ProxyConfig;
use fbc_runtime::http::Method;
use fbc_venue_paradex::auth::{ACCOUNT_ADDRESS, SIGNING_KEY};
use rust_decimal::Decimal;
use serde_json::{Value, json};

/// The session token the stub's login gives: JWT-shaped, so the privacy check finds it if a
/// fixture or the report kept it, and made up. Built at run time, so this file holds no
/// JWT-shaped text for the repository's own privacy check to refuse.
fn token() -> String {
    [
        "ey",
        "JhbGciOiJTWU5USEVUSUMifQ",
        ".",
        "ey",
        "JzdWIiOiJzeW50aGV0aWMifQ",
        ".c3ludGhldGljLXNpZw",
    ]
    .concat()
}
const MARKET: &str = "BTC-USD-PERP";
/// The account bytes the stub's SBE frames carry: none of the scrubbed fixtures may keep them.
const ACCOUNT_BYTE: u8 = 0x5a;

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

fn secrets() -> Result<Secrets, String> {
    let (account, key) = synthetic();
    let mut creds = Secrets::new();
    creds.insert(ACCOUNT_ADDRESS, Secret::new(account));
    creds.insert(SIGNING_KEY, Secret::new(key));
    Ok(creds)
}

/// A directory removed when dropped.
struct Removed(PathBuf);

impl Drop for Removed {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// A fresh directory of the test's own, removed when the returned guard drops.
fn temp_dir(what: &str) -> (PathBuf, Removed) {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "fbc-acceptance-test-{what}-{}-{n}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    (dir.clone(), Removed(dir))
}

/// One order of the model.
#[derive(Clone, Debug)]
struct VOrder {
    vid: String,
    cid: String,
    /// 1 BUY, 2 SELL.
    side: u8,
    /// 1 GTC, 2 IOC, 3 POST_ONLY.
    tif: u8,
    price: String,
    size: String,
    open: String,
    status: &'static str,
    cancel_reason: String,
    /// The connection it was placed on.
    conn: usize,
}

impl VOrder {
    fn json(&self, account: &str) -> Value {
        json!({
            "account": account, "avg_fill_price": "", "cancel_reason": self.cancel_reason,
            "client_id": self.cid, "created_at": 1759622400000u64, "flags": [], "id": self.vid,
            "instruction": match self.tif { 1 => "GTC", 2 => "IOC", _ => "POST_ONLY" },
            "last_updated_at": 1759622401000u64, "market": MARKET, "price": self.price,
            "published_at": 1759622401002u64, "received_at": 1759622400001u64,
            "remaining_size": self.open, "request_info": null, "seq_no": 41,
            "side": if self.side == 1 { "BUY" } else { "SELL" }, "size": self.size,
            "status": self.status, "stp": "EXPIRE_TAKER", "timestamp": 1759622400000u64,
            "trigger_price": "0", "type": "LIMIT",
        })
    }
}

/// The stub's model of the account (module documentation).
struct Venue {
    orders: Vec<VOrder>,
    seq: i64,
    next_vid: u64,
    account: String,
    /// The stub's connections, once it started.
    conns: Arc<OnceLock<ConnectionsView>>,
    /// Whether the venue cancels a connection's orders when it is dropped, and when it is
    /// closed with a close frame.
    cod_on_drop: bool,
    cod_on_close: bool,
    /// Whether an order a dropped socket left resting fills instead (a fill racing the
    /// disconnect), moving the account's position.
    fill_on_drop: bool,
    /// The account's signed position in the market.
    position: Decimal,
    /// Whether the order book shows no ask (a testnet market nobody sells on).
    one_sided: bool,
    /// Whether an IOC fills instead of expiring (the touch moved under it).
    ioc_fills: bool,
}

impl Venue {
    /// Closes the orders of every connection that ended, as cancel-on-disconnect says.
    fn disconnects(&mut self) {
        let Some(view) = self.conns.get() else { return };
        let conns = view.connections();
        for o in self.orders.iter_mut().filter(|o| o.status != "CLOSED") {
            let Some(c) = conns.get(o.conn) else { continue };
            let cancels = if c.client_closed {
                self.cod_on_close
            } else {
                self.cod_on_drop
            };
            if !c.open && !c.client_closed && self.fill_on_drop {
                o.status = "CLOSED";
                o.open = "0".to_owned();
                let size = Decimal::from_str(&o.size).unwrap();
                self.position += if o.side == 1 { size } else { -size };
            } else if !c.open && cancels {
                o.status = "CLOSED";
                o.cancel_reason = "SYNTHETIC_DISCONNECT".to_owned();
                o.open = o.size.clone();
            }
        }
    }

    fn next_seq(&mut self) -> i64 {
        self.seq += 1;
        self.seq
    }
}

/// A decimal string as Paradex's SBE 10^-8 mantissa.
fn e8(text: &str) -> i64 {
    let d = Decimal::from_str(text).unwrap() * Decimal::from(100_000_000);
    i64::try_from(d.mantissa() / 10i128.pow(d.scale())).unwrap()
}

/// An `OrderEvent` (template 20) at schema 1:2 in its 128-byte block layout
/// (`fixtures/paradex/exec/README.md`) for `o`, its status byte (1 NEW, 3 OPEN, 4 CLOSED) and,
/// for a modify, its request_info (`request`: status, type, id).
fn order_event(o: &VOrder, seq: i64, status: u8, request: Option<(u8, u8, &str)>) -> Vec<u8> {
    let null = i64::MIN;
    let ts = 1_759_500_000_205_011i64;
    let mut f = Vec::new();
    for v in [128u16, 20, 1, 2] {
        f.extend(v.to_le_bytes());
    }
    f.extend(ts.to_le_bytes());
    f.extend(seq.to_le_bytes());
    f.extend([status, o.side, 1, o.tif]);
    f.extend(e8(&o.price).to_le_bytes());
    f.extend(null.to_le_bytes()); // triggerPrice
    f.extend(e8(&o.size).to_le_bytes());
    f.extend(e8(&o.open).to_le_bytes());
    f.extend(null.to_le_bytes()); // avgFillPrice
    f.extend(ts.to_le_bytes()); // createdAt
    f.extend(ts.to_le_bytes()); // updatedAt
    f.extend([ACCOUNT_BYTE; 32]); // account
    f.extend(ts.to_le_bytes()); // receivedAt
    f.extend(ts.to_le_bytes()); // publishedAt
    let (rs, rt, rid) = request.unwrap_or((0, 0, ""));
    f.extend([0u8, 0, rs, rt]); // stp, flags, requestStatus, requestType
    assert_eq!(f.len(), 8 + 128);
    let reason = if status == 4 {
        o.cancel_reason.as_str()
    } else {
        ""
    };
    for s in [o.vid.as_str(), o.cid.as_str(), MARKET, reason, rid, ""] {
        f.push(u8::try_from(s.len()).unwrap());
        f.extend(s.as_bytes());
    }
    f
}

fn side_byte(side: &Value) -> u8 {
    if side == "SELL" { 2 } else { 1 }
}

fn tif_byte(instruction: &Value) -> u8 {
    match instruction.as_str() {
        Some("IOC") => 2,
        Some("GTC") => 1,
        _ => 3,
    }
}

/// The model's answers on connection `conn` (module documentation).
fn responder(venue: Arc<Mutex<Venue>>, conn: usize) -> Responder {
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
        let mut v = venue.lock().unwrap();
        let account = v.account.clone();
        let mut frames = Vec::new();
        match req["method"].as_str().unwrap_or("") {
            "auth" => {
                v.disconnects();
                frames.push(ok(json!({})));
            }
            "subscribe" => frames.push(ok(json!({}))),
            "order.cancel_on_disconnect" => frames.push(ok(json!({"enabled": true}))),
            "order.create" | "order.create_batch" => {
                let items: Vec<Value> = match params.get("orders") {
                    Some(Value::Array(items)) => items.clone(),
                    _ => vec![params.clone()],
                };
                let mut created = Vec::new();
                for p in &items {
                    v.next_vid += 1;
                    let o = VOrder {
                        vid: format!("17595000000000{:05}", v.next_vid),
                        cid: p["client_id"].as_str().unwrap_or("").to_owned(),
                        side: side_byte(&p["side"]),
                        tif: tif_byte(&p["instruction"]),
                        price: p["price"].as_str().unwrap_or("").to_owned(),
                        size: p["size"].as_str().unwrap_or("").to_owned(),
                        open: p["size"].as_str().unwrap_or("").to_owned(),
                        status: "NEW",
                        cancel_reason: String::new(),
                        conn,
                    };
                    created.push(o);
                }
                let reply = if items.len() == 1 && params.get("orders").is_none() {
                    json!({"order": created[0].json(&account)})
                } else {
                    let results: Vec<Value> = created
                        .iter()
                        .map(|o| json!({"order": o.json(&account)}))
                        .collect();
                    json!({"results": results})
                };
                frames.push(ok(reply));
                for mut o in created {
                    let seq = v.next_seq();
                    if o.tif == 2 && v.ioc_fills {
                        o.status = "CLOSED";
                        o.open = "0".to_owned();
                        let size = Decimal::from_str(&o.size).unwrap();
                        v.position += if o.side == 1 { size } else { -size };
                        frames.push(Frame::Binary(order_event(&o, seq, 4, None)));
                    } else if o.tif == 2 {
                        // An IOC priced behind the touch fills nothing: the venue ends it.
                        o.status = "CLOSED";
                        o.cancel_reason = "SYNTHETIC_IOC_EXPIRED".to_owned();
                        frames.push(Frame::Binary(order_event(&o, seq, 4, None)));
                    } else {
                        o.status = "OPEN";
                        frames.push(Frame::Binary(order_event(&o, seq, 3, None)));
                    }
                    v.orders.push(o);
                }
            }
            "order.modify" => {
                let vid = params["id"].as_str().unwrap_or("").to_owned();
                let seq = v.next_seq();
                let o = v
                    .orders
                    .iter_mut()
                    .find(|o| o.vid == vid)
                    .ok_or("no such order")?;
                o.price = params["price"].as_str().unwrap_or("").to_owned();
                o.size = params["size"].as_str().unwrap_or("").to_owned();
                o.open = o.size.clone();
                let o = o.clone();
                frames.push(ok(json!({"order": o.json(&account)})));
                let rid = format!("req-{}", id);
                frames.push(Frame::Binary(order_event(&o, seq, 3, Some((4, 1, &rid)))));
            }
            "order.cancel" | "order.cancel_batch" => {
                let ids: Vec<String> = match params.get("order_ids") {
                    Some(Value::Array(ids)) => ids
                        .iter()
                        .map(|i| i.as_str().unwrap_or("").to_owned())
                        .collect(),
                    _ => vec![params["id"].as_str().unwrap_or("").to_owned()],
                };
                let mut events = Vec::new();
                let mut results = Vec::new();
                for vid in &ids {
                    let seq = v.next_seq();
                    let o = v
                        .orders
                        .iter_mut()
                        .find(|o| &o.vid == vid)
                        .ok_or("no such order")?;
                    o.status = "CLOSED";
                    o.cancel_reason = "USER_CANCELED".to_owned();
                    let o = o.clone();
                    results.push(
                        json!({"id": vid, "market": MARKET, "status": "QUEUED_FOR_CANCELLATION"}),
                    );
                    events.push(Frame::Binary(order_event(&o, seq, 4, None)));
                }
                if params.get("order_ids").is_some() {
                    frames.push(ok(json!({"results": results})));
                } else {
                    frames.push(ok(
                        json!({"order_id": ids[0], "status": "QUEUED_FOR_CANCELLATION"}),
                    ));
                }
                frames.extend(events);
            }
            other => return Err(format!("an unexpected method {other}")),
        }
        Ok(frames)
    })
}

fn reply(status: u16, body: impl Into<Vec<u8>>) -> HttpReply {
    HttpReply {
        status,
        body: body.into(),
    }
}

/// Whether `req` carries the stub's token as Paradex's REST reads do.
fn authorized(req: &HttpRequest) -> bool {
    req.header("authorization") == Some(&format!("Bearer {}", token()))
}

/// The stub's HTTP answers, computed from the model.
fn routes(venue: Arc<Mutex<Venue>>) -> HttpRouter {
    let mut book: Value =
        serde_json::from_slice(&fs::read(fixture("paradex/rest/orderbook-btc-2002.json")).unwrap())
            .unwrap();
    if venue.lock().unwrap().one_sided {
        book["asks"] = json!([]);
    }
    let book = book.to_string();
    let (orders, history) = (Arc::clone(&venue), Arc::clone(&venue));
    let positions = Arc::clone(&venue);
    HttpRouter::new()
        .route(
            Method::POST,
            PathPattern::exact("/v1/auth"),
            reply(200, format!(r#"{{"jwt_token":"{}"}}"#, token())),
        )
        .route(
            Method::GET,
            PathPattern::exact(&format!("/v1/orderbook/{MARKET}")),
            reply(200, book),
        )
        .route_fn(Method::GET, PathPattern::exact("/v1/orders"), move |req| {
            if !authorized(req) {
                return reply(401, "{}");
            }
            let mut v = orders.lock().unwrap();
            v.disconnects();
            let account = v.account.clone();
            let open: Vec<Value> = v
                .orders
                .iter()
                .filter(|o| o.status != "CLOSED")
                .map(|o| o.json(&account))
                .collect();
            reply(200, json!({ "results": open }).to_string())
        })
        .route_fn(
            Method::GET,
            PathPattern::exact("/v1/orders-history"),
            move |req| {
                if !authorized(req) {
                    return reply(401, "{}");
                }
                let mut v = history.lock().unwrap();
                v.disconnects();
                let account = v.account.clone();
                let cid = req
                    .query
                    .as_deref()
                    .and_then(|q| q.strip_prefix("client_id="))
                    .unwrap_or("")
                    .to_owned();
                let rows: Vec<Value> = v
                    .orders
                    .iter()
                    .filter(|o| o.cid == cid)
                    .map(|o| o.json(&account))
                    .collect();
                reply(
                    200,
                    json!({"next": null, "prev": null, "results": rows}).to_string(),
                )
            },
        )
        // Flat unless an order filled (far from the touch, only as `fill_on_drop` says).
        .route_fn(
            Method::GET,
            PathPattern::exact("/v1/positions"),
            move |req| {
                if !authorized(req) {
                    return reply(401, "{}");
                }
                let mut v = positions.lock().unwrap();
                v.disconnects();
                let rows: Vec<Value> = if v.position.is_zero() {
                    Vec::new()
                } else {
                    let side = if v.position > Decimal::ZERO {
                        "LONG"
                    } else {
                        "SHORT"
                    };
                    vec![json!({
                        "account": v.account, "market": MARKET, "side": side,
                        "size": v.position.to_string(), "status": "OPEN",
                        "average_entry_price": "60140.1",
                    })]
                };
                reply(200, json!({ "results": rows }).to_string())
            },
        )
}

/// The order socket's script: the main connection (the opening's six frames, then place,
/// modify, cancel, batch, cancel-many, the IOC and the order left for the dropped socket), the
/// graceful phase's (the opening, `extra_graceful` Safety cancels, one place) and the stop
/// phase's (the opening, `extra_stop` Safety cancels).
fn script(
    venue: &Arc<Mutex<Venue>>,
    main: usize,
    extra_graceful: usize,
    extra_stop: usize,
) -> WsScript {
    let mut steps = Vec::new();
    for (conn, frames) in [(0, main), (1, 7 + extra_graceful), (2, 6 + extra_stop)] {
        steps.push(Step::Accept);
        let with = responder(Arc::clone(venue), conn);
        steps.extend((0..frames).map(|_| Step::Respond {
            conn,
            with: with.clone(),
        }));
    }
    WsScript::new(steps)
}

fn options(stub: &StubServer, evidence: &Path, lease: &Path, cod_wait: u64) -> AcceptOptions {
    AcceptOptions {
        trade: Options {
            market: MARKET.to_owned(),
            tick: Decimal::from_str("0.1").unwrap(),
            step: Decimal::from_str("0.00001").unwrap(),
            min_notional: Decimal::from(10),
            resting_cap_usd: Decimal::from(11),
            inventory_cap_usd: Decimal::from(50),
            order_usd: Decimal::from(11),
            namespace: 1,
            side: OrderSide::Buy,
            away_bps: 300,
            hold_secs: 0,
            step_timeout_secs: 10,
            rest_url: stub.http_url("/v1"),
            ws_url: stub.ws_url("/v1"),
            proxy: ProxyConfig::Direct,
            lease_dir: lease.to_path_buf(),
            sole_trader: true,
        },
        evidence_dir: evidence.to_path_buf(),
        cod_wait_secs: cod_wait,
        rest_wait_secs: 5,
    }
}

/// Runs the program against `opts`, bounded at 120 s: its outcome and what it printed.
async fn run_against(opts: &AcceptOptions) -> (accept::Outcome, String) {
    let buf = Rc::new(RefCell::new(Vec::<u8>::new()));
    let out: trade::Out = buf.clone();
    let creds = secrets;
    let run = accept::run(opts, None, &creds, out);
    let ran = tokio::time::timeout(Duration::from_secs(120), run).await;
    let printed = String::from_utf8(buf.borrow().clone()).unwrap();
    let outcome = ran
        .unwrap_or_else(|_| panic!("the program did not finish in 120 s; it printed:\n{printed}"))
        .unwrap_or_else(|e| panic!("refused: {e}; it printed:\n{printed}"));
    (outcome, printed)
}

/// The JSON-RPC methods the program sent on connection `conn`.
fn methods(stub: &StubServer, conn: usize) -> Vec<String> {
    stub.connections()[conn]
        .received
        .iter()
        .filter_map(|f| match f {
            Frame::Text(t) => serde_json::from_str::<Value>(t).ok(),
            Frame::Binary(_) => None,
        })
        .map(|v| v["method"].as_str().unwrap_or("").to_owned())
        .collect()
}

/// Every file under `dir`, recursively, as text (lossy).
fn texts(dir: &Path) -> Vec<(PathBuf, String)> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            out.extend(texts(&path));
        } else {
            let text = String::from_utf8_lossy(&fs::read(&path).unwrap()).into_owned();
            out.push((path, text));
        }
    }
    out
}

/// scripts/privacy-check.sh run over `dir` alone, with no master key file: its exit status
/// and output.
fn privacy_check(dir: &Path) -> (bool, String) {
    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../scripts/privacy-check.sh");
    let out = std::process::Command::new("bash")
        .arg(script)
        .env("LOOP_ROOT", dir)
        .env("FBC_PRIVACY_KEY_FILES", "")
        .output()
        .unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.success(), text)
}

fn new_venue(cod_on_drop: bool, cod_on_close: bool) -> Arc<Mutex<Venue>> {
    Arc::new(Mutex::new(Venue {
        orders: Vec::new(),
        seq: 5_000,
        next_vid: 0,
        account: synthetic().0,
        conns: Arc::new(OnceLock::new()),
        cod_on_drop,
        cod_on_close,
        fill_on_drop: false,
        position: Decimal::ZERO,
        one_sided: false,
        ioc_fills: false,
    }))
}

/// The main connection's frames: the opening's six, then place, modify, cancel, batch,
/// cancel-many, the IOC and the order left for the dropped socket.
const MAIN: usize = 13;

async fn stub_for(
    venue: &Arc<Mutex<Venue>>,
    main: usize,
    extra_graceful: usize,
    extra_stop: usize,
) -> StubServer {
    let stub = StubServer::start(
        script(venue, main, extra_graceful, extra_stop),
        routes(Arc::clone(venue)),
    )
    .await
    .unwrap();
    let conns = Arc::clone(&venue.lock().unwrap().conns);
    let _ = conns.set(stub.connections_view());
    stub
}

#[tokio::test]
async fn the_program_runs_every_step_once_maps_each_to_the_lattice_and_writes_scrubbed_evidence() {
    let venue = new_venue(true, true);
    let stub = stub_for(&venue, MAIN, 0, 0).await;
    let (evidence_dir, _e) = temp_dir("evidence");
    let (lease_dir, _l) = temp_dir("lease");
    let opts = options(&stub, &evidence_dir.join("run"), &lease_dir, 10);
    let (outcome, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    assert!(
        outcome.ok,
        "{printed}\n{}",
        fs::read_to_string(&outcome.report).unwrap()
    );

    // Each scripted step sent once, on the connection its phase runs.
    let main = methods(&stub, 0);
    let count = |sent: &[String], m: &str| sent.iter().filter(|s| s.as_str() == m).count();
    assert_eq!(
        count(&main, "order.create"),
        3,
        "place, ioc, cod-dropped: {main:?}"
    );
    assert_eq!(count(&main, "order.create_batch"), 1, "{main:?}");
    assert_eq!(count(&main, "order.modify"), 1, "{main:?}");
    assert_eq!(count(&main, "order.cancel"), 1, "{main:?}");
    assert_eq!(count(&main, "order.cancel_batch"), 1, "{main:?}");
    assert_eq!(count(&main, "order.cancel_all"), 0, "{main:?}");
    let graceful = methods(&stub, 1);
    assert_eq!(
        count(&graceful, "order.create"),
        1,
        "cod-graceful: {graceful:?}"
    );
    assert_eq!(graceful.len(), 7, "{graceful:?}");
    let stop = methods(&stub, 2);
    assert_eq!(stop.len(), 6, "nothing left to cancel at Stop: {stop:?}");
    // The dropped socket sent no close frame; the graceful one did.
    let conns = stub.connections();
    assert_eq!(conns.len(), 3);
    assert!(
        !conns[0].client_closed,
        "the main phase's socket was dropped"
    );
    assert!(
        conns[1].client_closed,
        "the graceful phase's socket was closed"
    );

    // The order lattice per step, in the report and the evidence.
    let ev = &outcome.evidence;
    let step = |name: &str| ev.steps.iter().find(|s| s.name == name).unwrap();
    for name in [
        "place",
        "amend",
        "cancel",
        "batch",
        "cancel-many",
        "ioc",
        "cod-dropped",
        "cod-graceful",
        "stop",
    ] {
        let s = step(name);
        assert!(s.pass, "{name}: {s:?}");
        assert_eq!(s.ran, evidence::Ran::Yes, "{name}");
    }
    let sent_kinds = |name: &str| -> Vec<(String, usize)> {
        step(name)
            .sent
            .iter()
            .map(|c| (c.kind.clone(), c.items))
            .collect()
    };
    assert_eq!(sent_kinds("place"), [("place".to_owned(), 1)]);
    // Each placement's final phase (0092), from its order event, after the reply's
    // provisional acceptance.
    for name in ["place", "ioc", "cod-dropped", "cod-graceful"] {
        let outcomes = &step(name).sent[0].outcomes;
        assert!(
            outcomes.iter().any(|o| o.contains("Final")),
            "{name}: {outcomes:?}"
        );
    }
    // The per-IP limit check's counts: what was sent before each comparison.
    let first = &step("place").comparisons[0];
    assert_eq!(
        (first.ws_since, first.connections_since),
        (1, 1),
        "{first:?}"
    );
    assert!(
        first.rest_since >= 2,
        "the order-book reads at least: {first:?}"
    );
    let amend = &step("amend").comparisons[0];
    assert_eq!(
        (amend.ws_since, amend.connections_since),
        (1, 0),
        "{amend:?}"
    );
    assert_eq!(sent_kinds("amend"), [("amend".to_owned(), 1)]);
    assert_eq!(sent_kinds("cancel"), [("cancel".to_owned(), 1)]);
    assert_eq!(sent_kinds("batch"), [("batch".to_owned(), 2)]);
    assert_eq!(sent_kinds("cancel-many"), [("cancel-many".to_owned(), 2)]);
    assert_eq!(sent_kinds("cod-dropped"), [("place".to_owned(), 1)]);
    assert_eq!(sent_kinds("cod-graceful"), [("place".to_owned(), 1)]);
    let last = |name: &str| -> Vec<String> {
        step(name)
            .orders
            .iter()
            .map(|o| o.states.last().cloned().unwrap_or_default())
            .collect()
    };
    // 3% under the fixture's best bid of 62000.2, floored onto the tick; $11 is 18 lots there,
    // but the resting cap is in lots at the dearer side's price (the sell at 63860.6): 17.
    assert!(
        last("place")[0].starts_with("Open @60140.1 x 0.00017"),
        "{:?}",
        step("place").orders
    );
    // Ten basis points further (602 ticks of 0.1), the same total: one step smaller would be
    // below the $10 minimum notional.
    assert!(
        last("amend")[0].starts_with("Open @60079.9 x 0.00017"),
        "{:?}",
        step("amend").orders
    );
    assert!(
        last("cancel")[0].starts_with("Terminal(Canceled(Requested))"),
        "{:?}",
        step("cancel").orders
    );
    assert_eq!(last("batch").len(), 2);
    assert!(
        last("batch").iter().all(|s| s.starts_with("Open")),
        "{:?}",
        step("batch").orders
    );
    assert!(
        last("cancel-many")
            .iter()
            .all(|s| s.starts_with("Terminal(Canceled(Requested))"))
    );
    assert!(
        last("ioc")[0].starts_with("Terminal("),
        "{:?}",
        step("ioc").orders
    );
    let place = &step("place").orders[0];
    assert_eq!(
        place.states.first().map(|s| s.starts_with("PendingNew")),
        Some(true),
        "{place:?}"
    );

    // The REST comparison after every step, and its answers.
    for name in [
        "place",
        "amend",
        "cancel",
        "batch",
        "cancel-many",
        "ioc",
        "stop",
    ] {
        let c = &step(name).comparisons;
        assert!(
            !c.is_empty() && c.iter().all(|c| c.orders_agreed && c.positions_agreed),
            "{name}: {c:?}"
        );
    }
    let dropped = step("cod-dropped").disconnect.as_ref().unwrap();
    assert!(dropped.cancelled, "{dropped:?}");
    assert_eq!(dropped.resting_at_reconnect, Some(false));
    let history = dropped.history.as_ref().unwrap();
    assert_eq!(history.status.as_deref(), Some("CLOSED"));
    assert_eq!(
        history.cancel_reason.as_deref(),
        Some("SYNTHETIC_DISCONNECT")
    );
    // The disconnects' comparisons: the order gone, the position the fills'.
    for name in ["cod-dropped", "cod-graceful"] {
        let c = step(name).comparisons.last().unwrap();
        assert!(
            c.after.ends_with("(position only)") && !c.orders_compared && c.positions_agreed,
            "{name}: {c:?}"
        );
    }
    let closed = step("cod-graceful").disconnect.as_ref().unwrap();
    assert!(
        closed.cancelled && closed.how.starts_with("graceful"),
        "{closed:?}"
    );
    let ioc = &step("ioc").histories[0];
    assert_eq!(ioc.cancel_reason.as_deref(), Some("SYNTHETIC_IOC_EXPIRED"));

    let report = fs::read_to_string(&outcome.report).unwrap();
    assert!(
        report.contains("| REST vs stream after | Stream showed | GET /orders agreed |"),
        "{report}"
    );
    assert!(report.contains("lattice PendingNew"), "{report}");
    assert!(
        report.contains("(a) GET /orders lists every order"),
        "{report}"
    );
    assert!(report.contains("SYNTHETIC_IOC_EXPIRED"), "{report}");
    // The amend's order event and its request_info, read from the journal.
    assert!(
        ev.frames
            .iter()
            .any(|f| f.request_status == Some(4) && f.request_type == Some(1)),
        "{:?}",
        ev.frames
    );
    let json: Value = serde_json::from_str(&fs::read_to_string(&outcome.json).unwrap()).unwrap();
    assert_eq!(json["ok"], true);
    assert_eq!(json["steps"].as_array().unwrap().len(), 9);

    // The scrubbed fixtures: frames and answers, no token, no account value.
    assert!(
        outcome
            .fixture_files
            .iter()
            .any(|f| f.ends_with(".sbe.txt")),
        "{:?}",
        outcome.fixture_files
    );
    assert!(outcome.fixture_files.iter().any(|f| f == "README.md"));
    assert!(ev.withheld.is_empty(), "{:?}", ev.withheld);
    let account = synthetic().0;
    let hex = account.trim_start_matches("0x").to_ascii_lowercase();
    let stub_bytes = ["5a"; 32].join(" ");
    for (path, text) in texts(&outcome.fixtures).into_iter().chain(
        [&outcome.report, &outcome.json]
            .into_iter()
            .map(|p| (p.clone(), fs::read_to_string(p).unwrap())),
    ) {
        assert!(!text.contains(&token()), "{}", path.display());
        assert!(
            !text.to_ascii_lowercase().contains(&hex),
            "{}",
            path.display()
        );
        assert!(!text.contains(&stub_bytes), "{}", path.display());
    }
    let (passed, said) = privacy_check(&outcome.fixtures);
    assert!(passed, "{said}");
    // The check would have failed on the frames as the stub sent them.
    let (raw, _r) = temp_dir("raw");
    fs::write(
        raw.join("frame.json"),
        json!({"account": account}).to_string(),
    )
    .unwrap();
    assert!(!privacy_check(&raw).0);

    // No credential in what it printed.
    assert!(!printed.contains(&token()));
    assert!(!printed.to_ascii_lowercase().contains(&hex));
    assert!(printed.contains("DONE ok"), "{printed}");
}

#[tokio::test]
async fn a_venue_that_keeps_orders_after_a_disconnect_is_recorded_and_the_next_connection_cancels_them()
 {
    // Paradex keeps the orders: each later connection's resync shows the earlier one's order
    // resting and Safety-cancels it (one cancel-many each, decision 0080).
    let venue = new_venue(false, false);
    let stub = stub_for(&venue, MAIN, 1, 1).await;
    let (evidence_dir, _e) = temp_dir("evidence-kept");
    let (lease_dir, _l) = temp_dir("lease-kept");
    let opts = options(&stub, &evidence_dir.join("run"), &lease_dir, 1);
    let (outcome, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    let ev = &outcome.evidence;
    let step = |name: &str| ev.steps.iter().find(|s| s.name == name).unwrap();
    for name in ["cod-dropped", "cod-graceful"] {
        let d = step(name).disconnect.as_ref().unwrap();
        assert!(!d.cancelled, "{name}: {d:?}");
        assert_eq!(d.resting_at_reconnect, Some(true), "{name}: {d:?}");
        assert!(
            step(name).pass,
            "measured either way: {name}: {:?}",
            step(name)
        );
    }
    assert_eq!(
        methods(&stub, 1)
            .iter()
            .filter(|m| *m == "order.cancel_batch")
            .count(),
        1
    );
    assert_eq!(
        methods(&stub, 2)
            .iter()
            .filter(|m| *m == "order.cancel_batch")
            .count(),
        1
    );
    assert!(step("stop").pass, "{printed}");
    let report = fs::read_to_string(&outcome.report).unwrap();
    assert!(report.contains("the order was NOT cancelled"), "{report}");
}

#[tokio::test]
async fn a_mainnet_url_or_chain_id_is_refused_before_any_connection() {
    let venue = new_venue(true, true);
    let stub = stub_for(&venue, MAIN, 0, 0).await;
    let (evidence_dir, _e) = temp_dir("evidence-refused");
    let (lease_dir, _l) = temp_dir("lease-refused");
    let creds = secrets;
    let refused = |opts: AcceptOptions, chain: Option<&'static str>| {
        let buf = Rc::new(RefCell::new(Vec::<u8>::new()));
        let out: trade::Out = buf.clone();
        async move { accept::run(&opts, chain, &creds, out).await.err() }
    };
    let mut mainnet = options(&stub, &evidence_dir.join("a"), &lease_dir, 10);
    mainnet.trade.rest_url = "https://api.prod.paradex.trade/v1".to_owned();
    mainnet.trade.ws_url = "wss://ws.api.prod.paradex.trade/v1".to_owned();
    let why = refused(mainnet, None)
        .await
        .expect("a mainnet URL is refused");
    assert!(why.contains("--rest-url"), "{why}");
    let stub_urls = options(&stub, &evidence_dir.join("b"), &lease_dir, 10);
    let why = refused(stub_urls, Some("PRIVATE_SN_PARACLEAR_MAINNET"))
        .await
        .expect("a mainnet chain id is refused");
    assert!(why.contains("PARADEX_CHAIN_ID"), "{why}");
    assert!(stub.connections().is_empty());
    assert!(
        stub.http_requests().is_empty(),
        "{:?}",
        stub.http_requests()
    );
    assert!(!evidence_dir.join("a").exists() && !evidence_dir.join("b").exists());

    // The command line refuses the same before anything runs.
    let argv = |extra: &[&str]| -> Vec<String> {
        let mut v: Vec<String> = [
            "--evidence-dir",
            "/tmp/x",
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
        ]
        .iter()
        .map(|s| (*s).to_owned())
        .collect();
        v.extend(extra.iter().map(|s| (*s).to_owned()));
        v
    };
    assert!(matches!(
        accept_args::parse(argv(&[])),
        Ok(AcceptParsed::Run(_))
    ));
    let err =
        accept_args::parse(argv(&["--rest-url", "https://api.prod.paradex.trade/v1"])).unwrap_err();
    assert!(err.contains("--rest-url"), "{err}");
    let mut relative = argv(&[]);
    relative[1] = "relative/dir".to_owned();
    assert!(
        accept_args::parse(relative)
            .unwrap_err()
            .contains("--evidence-dir")
    );
    assert!(
        accept_args::parse(argv(&["--cod-wait", "0"]))
            .unwrap_err()
            .contains("--cod-wait")
    );
    assert!(matches!(
        accept_args::parse(["--help".to_owned()]),
        Ok(AcceptParsed::Help)
    ));
}

#[test]
fn the_scrubber_replaces_account_values_and_the_check_finds_what_it_would_leave() {
    let (account, _) = synthetic();
    let mut v = json!({"account": account, "order": {"signature": format!("0x{}", "ab".repeat(30)), "price": "1"}});
    evidence::scrub_json(&mut v);
    assert_eq!(v["account"], "SCRUBBED");
    assert_eq!(v["order"]["signature"], "SCRUBBED");
    assert_eq!(v["order"]["price"], "1");
    assert!(evidence::privacy_hits(&v.to_string()).is_empty());
    assert!(!evidence::privacy_hits(&account).is_empty());
    assert!(!evidence::privacy_hits(&"c".repeat(64)).is_empty());
    assert!(evidence::privacy_hits(&"c".repeat(65)).is_empty());
    assert!(!evidence::privacy_hits(&token()).is_empty());
    let header = format!("Authorization: {} {}", "Bearer", "abcdefghij".repeat(2));
    assert!(!evidence::privacy_hits(&header).is_empty());

    let o = VOrder {
        vid: "1759500000000000001".to_owned(),
        cid: "01000700-199b-81ab-8200-00054d0aa3f5".to_owned(),
        side: 1,
        tif: 3,
        price: "60140.1".to_owned(),
        size: "0.00018".to_owned(),
        open: "0.00018".to_owned(),
        status: "CLOSED",
        cancel_reason: "USER_CANCELED".to_owned(),
        conn: 0,
    };
    let frame = order_event(&o, 7, 4, Some((4, 1, "req-9")));
    let scrubbed = evidence::scrub_sbe(&frame).unwrap();
    assert_eq!(
        &scrubbed[8 + 76..8 + 108],
        (0xa0u8..=0xbf).collect::<Vec<_>>()
    );
    assert_eq!(scrubbed.len(), frame.len());
    let row = evidence::order_row(&scrubbed).unwrap();
    assert_eq!(
        (
            row.status.as_str(),
            row.price.as_str(),
            row.size.as_str(),
            row.cancel_reason.as_str()
        ),
        ("CLOSED", "60140.1", "0.00018", "USER_CANCELED")
    );
    assert_eq!((row.request_status, row.request_type), (Some(4), Some(1)));
    assert_eq!(row.request_id.as_deref(), Some("req-9"));
    // An account frame (a balance) is never a fixture; a frame too short is refused.
    let mut account_frame = frame.clone();
    account_frame[2] = 23;
    assert!(evidence::scrub_sbe(&account_frame).is_none());
    assert!(evidence::scrub_sbe(&frame[..60]).is_none());
    assert_eq!(evidence::utc(0), "1970-01-01 00:00:00Z");
    assert_eq!(evidence::utc(1_760_227_200), "2025-10-12 00:00:00Z");
}

#[tokio::test]
async fn an_order_that_fills_as_the_socket_drops_is_no_cancel_on_disconnect_and_fails_the_step() {
    let venue = new_venue(false, true);
    venue.lock().unwrap().fill_on_drop = true;
    let stub = stub_for(&venue, MAIN, 0, 0).await;
    let (evidence_dir, _e) = temp_dir("evidence-filled");
    let (lease_dir, _l) = temp_dir("lease-filled");
    let opts = options(&stub, &evidence_dir.join("run"), &lease_dir, 2);
    let (outcome, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    let ev = &outcome.evidence;
    let step = |name: &str| ev.steps.iter().find(|s| s.name == name).unwrap();
    let dropped = step("cod-dropped");
    let d = dropped.disconnect.as_ref().unwrap();
    // Gone from GET /orders, but filled: not a cancel, and the step fails.
    assert!(!d.cancelled, "{d:?}");
    assert_eq!(d.history.as_ref().unwrap().filled, Some(true));
    assert!(!dropped.pass, "{printed}");
    let c = dropped.comparisons.last().unwrap();
    assert!(
        !c.positions_agreed,
        "the fills the stream saw are flat, GET /positions is not: {c:?}"
    );
    assert!(!outcome.ok);
    assert!(
        printed.contains("FILLED before or during the disconnect"),
        "{printed}"
    );
    // The next phases seed the filled position by hand and agree with it.
    assert!(step("cod-graceful").pass, "{:?}", step("cod-graceful"));
    assert!(step("stop").pass, "{:?}", step("stop"));
}

#[tokio::test]
async fn a_book_with_no_ask_skips_only_the_pair_and_the_other_steps_pass() {
    let venue = new_venue(true, true);
    venue.lock().unwrap().one_sided = true;
    // No batch and no cancel-many on the main connection.
    let stub = stub_for(&venue, MAIN - 2, 0, 0).await;
    let (evidence_dir, _e) = temp_dir("evidence-one-sided");
    let (lease_dir, _l) = temp_dir("lease-one-sided");
    let opts = options(&stub, &evidence_dir.join("run"), &lease_dir, 10);
    let (outcome, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    let ev = &outcome.evidence;
    for s in &ev.steps {
        match s.name.as_str() {
            "batch" | "cancel-many" => assert!(
                matches!(&s.ran, evidence::Ran::Skipped(why) if why.contains("no ask")),
                "{s:?}"
            ),
            _ => assert!(s.pass, "{s:?}"),
        }
    }
    assert!(outcome.ok, "{printed}");
    let main = methods(&stub, 0);
    assert!(
        !main
            .iter()
            .any(|m| m == "order.create_batch" || m == "order.cancel_batch")
    );
}

#[test]
fn a_list_answer_without_results_is_no_empty_snapshot() {
    assert_eq!(accept::parse_results(br#"{"results":[]}"#), Ok(Vec::new()));
    assert!(accept::parse_results(b"{}").is_err());
    assert!(accept::parse_results(br#"{"results":{}}"#).is_err());
    assert!(accept::parse_results(b"<html>").is_err());
}

#[test]
fn a_position_agrees_only_with_its_size_side_and_status() {
    use evidence::RestPosition;
    let d = |t: &str| Decimal::from_str(t).unwrap();
    let pos = |side: &str, size: &str, status: &str| RestPosition {
        side: side.to_owned(),
        size: size.to_owned(),
        status: status.to_owned(),
    };
    let agrees = |p: Option<&RestPosition>, s: &str| accept::position_mismatch(p, d(s)).is_none();
    assert!(agrees(None, "0"));
    assert!(!agrees(None, "0.1"));
    assert!(agrees(Some(&pos("LONG", "0.1", "OPEN")), "0.1"));
    assert!(agrees(Some(&pos("SHORT", "-0.1", "OPEN")), "-0.1"));
    assert!(agrees(Some(&pos("LONG", "0", "CLOSED")), "0"));
    assert!(
        !agrees(Some(&pos("LONG", "-0.1", "OPEN")), "-0.1"),
        "a short shown LONG"
    );
    assert!(
        !agrees(Some(&pos("SHORT", "0.1", "OPEN")), "0.1"),
        "a long shown SHORT"
    );
    assert!(
        !agrees(Some(&pos("LONG", "0", "OPEN")), "0"),
        "flat but OPEN"
    );
    assert!(
        !agrees(Some(&pos("LONG", "0.1", "CLOSED")), "0.1"),
        "sized but CLOSED"
    );
    assert!(!agrees(Some(&pos("LONG", "0.2", "OPEN")), "0.1"));
    assert!(!agrees(Some(&pos("LONG", "x", "OPEN")), "0.1"));
}

#[test]
fn an_output_not_written_fails_the_run() {
    let mut ev = evidence::Evidence::default();
    assert!(ev.ok());
    ev.artifact_errors
        .push("no fixtures written: synthetic".to_owned());
    assert!(!ev.ok());
    assert!(evidence::markdown(&ev).contains("Outputs not written (the run fails)"));
}

#[tokio::test]
async fn an_ioc_that_fills_exercised_no_expiry_and_fails_its_step() {
    let venue = new_venue(true, true);
    venue.lock().unwrap().ioc_fills = true;
    let stub = stub_for(&venue, MAIN, 0, 0).await;
    let (evidence_dir, _e) = temp_dir("evidence-ioc-filled");
    let (lease_dir, _l) = temp_dir("lease-ioc-filled");
    let opts = options(&stub, &evidence_dir.join("run"), &lease_dir, 10);
    let (outcome, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    let ioc = outcome
        .evidence
        .steps
        .iter()
        .find(|s| s.name == "ioc")
        .unwrap();
    assert!(!ioc.pass, "{ioc:?}");
    assert!(
        ioc.notes
            .iter()
            .any(|n| n.contains("did not expire unfilled")),
        "{ioc:?}"
    );
    assert!(!outcome.ok, "{printed}");
}

#[test]
fn the_fixture_scan_finds_every_bearer_token_and_a_pem_header_and_the_check_runs() {
    let late = format!("bearer x; {} {}", "Bearer", "abcdefghij".repeat(2));
    assert!(evidence::privacy_hits(&late).contains(&"a bearer token"));
    let pem = format!("-----BEGIN {}PRIVATE KEY-----", "EC ");
    assert!(evidence::privacy_hits(&pem).contains(&"a PEM private-key header"));
    // The repository's check itself, over a directory: what it refuses is refused here too.
    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../scripts/privacy-check.sh");
    let (clean, _c) = temp_dir("check-clean");
    fs::write(clean.join("a.json"), r#"{"account":"SCRUBBED"}"#).unwrap();
    assert_eq!(evidence::privacy_check(&script, &clean), Ok(()));
    let (dirty, _d) = temp_dir("check-dirty");
    fs::write(
        dirty.join("a.json"),
        json!({"account": synthetic().0}).to_string(),
    )
    .unwrap();
    let refused = evidence::privacy_check(&script, &dirty).unwrap_err();
    assert!(refused.contains("a.json"), "{refused}");
    assert!(
        !refused
            .to_ascii_lowercase()
            .contains(&synthetic().0[2..].to_ascii_lowercase())
    );
    assert!(evidence::privacy_check(&dirty.join("no-such-script"), &clean).is_err());
}

#[test]
fn a_disconnect_is_credited_only_with_a_readable_unfilled_history() {
    let history = |listed: bool, filled: Option<bool>| evidence::History {
        listed,
        filled,
        status: Some("CLOSED".to_owned()),
        ..evidence::History::default()
    };
    assert!(accept::disconnect_cancelled(
        true,
        &history(true, Some(false))
    ));
    assert!(
        !accept::disconnect_cancelled(true, &history(true, None)),
        "unreadable quantities are not 'nothing filled'"
    );
    assert!(!accept::disconnect_cancelled(
        true,
        &history(true, Some(true))
    ));
    assert!(!accept::disconnect_cancelled(
        true,
        &history(false, Some(false))
    ));
    assert!(!accept::disconnect_cancelled(
        false,
        &history(true, Some(false))
    ));
}
