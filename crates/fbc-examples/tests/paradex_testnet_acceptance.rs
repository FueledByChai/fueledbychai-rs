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
// Its re-exports of the shared guard are for the samples' binaries; this test uses a part.
#[allow(dead_code, unused_imports)]
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
#[path = "../src/link.rs"]
mod link;
#[allow(dead_code)]
#[path = "../src/testnet.rs"]
mod testnet;
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
    /// Whether the first GET /positions once an order was placed answers a stale position
    /// (REST lagging the fills), the reads after it the account's.
    stale_position_once: bool,
    /// A client id in our namespace that the program never minted: when set, the first
    /// order.create also sends an order event of an order under it (one the registry does not
    /// hold).
    stray_cid: Option<String>,
    /// Cancel-on-disconnect applied late and REST failing after it (Reviewer B, RB7): 1 arms
    /// it; the first GET /orders once the main socket dropped then lists the dropped socket's
    /// order as it stood (2), and every GET /orders after it answers 500 until an
    /// orders-history read comes (3).
    late_then_failing: u8,
    /// Whether GET /orders lists the first order the program cancelled again, as resting,
    /// once the batch was placed and while the main socket is open (Codex r4244249052).
    ghost_cancelled: bool,
    /// Whether the first GTC placement's order events are held back: its reply accepts it
    /// provisionally, and its final phase comes only before the answer to the next request
    /// (the CI failure on c5f8479: the final phase later than the reply).
    hold_place_final: bool,
    /// Held order events, sent before the answer to the next request.
    held: Vec<Frame>,
    /// Whether GET /orders answers a further page's cursor (Codex r4277685465).
    orders_cursor: bool,
    /// Whether GET /orders-history never lists an IOC (Codex r4277685475).
    ioc_history_hidden: bool,
}

impl Venue {
    /// Whether the main connection (the first) has ended.
    fn main_ended(&self) -> bool {
        self.conns
            .get()
            .and_then(|view| view.connections().first().map(|c| !c.open))
            .unwrap_or(false)
    }

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
        let held = std::mem::take(&mut v.held);
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
                if let Some(cid) = v.stray_cid.take() {
                    let mut stray = created[0].clone();
                    stray.cid = cid;
                    stray.vid = "1759599999999999999".to_owned();
                    stray.status = "OPEN";
                    let seq = v.next_seq();
                    frames.push(Frame::Binary(order_event(&stray, seq, 3, None)));
                }
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
                        let event = Frame::Binary(order_event(&o, seq, 3, None));
                        if std::mem::take(&mut v.hold_place_final) {
                            v.held.push(event);
                        } else {
                            frames.push(event);
                        }
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
        // What was held goes first: the venue sends its order events in order.
        Ok(held.into_iter().chain(frames).collect())
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
            match v.late_then_failing {
                1 if v.main_ended() => v.late_then_failing = 2,
                2 => return reply(500, "{}"),
                _ => v.disconnects(),
            }
            let account = v.account.clone();
            let mut open: Vec<Value> = v
                .orders
                .iter()
                .filter(|o| o.status != "CLOSED")
                .map(|o| o.json(&account))
                .collect();
            if v.ghost_cancelled && v.orders.len() >= 3 && !v.main_ended() {
                let ghost = v.orders.iter().find(|o| o.cancel_reason == "USER_CANCELED");
                if let Some(o) = ghost {
                    let mut row = o.json(&account);
                    row["status"] = json!("OPEN");
                    open.push(row);
                }
            }
            if v.orders_cursor {
                return reply(
                    200,
                    json!({ "next": "c2Vjb25kLXBhZ2U", "prev": null, "results": open }).to_string(),
                );
            }
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
                if v.late_then_failing == 2 {
                    v.late_then_failing = 3;
                }
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
                    .filter(|o| o.cid == cid && !(v.ioc_history_hidden && o.tif == 2))
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
                if v.stale_position_once && !v.orders.is_empty() {
                    v.stale_position_once = false;
                    let row = json!({
                        "account": v.account, "market": MARKET, "side": "LONG",
                        "size": "0.00017", "status": "OPEN", "average_entry_price": "60140.1",
                    });
                    return reply(200, json!({ "results": [row] }).to_string());
                }
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
    let out: testnet::Out = buf.clone();
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

/// The WebSocket order methods the comparisons counted, over the whole run.
fn ws_counted(ev: &evidence::Evidence) -> u64 {
    ev.steps
        .iter()
        .flat_map(|s| &s.comparisons)
        .map(|c| c.ws_since)
        .sum()
}

/// The JSON-RPC `order.*` methods the stub received on every connection.
fn order_methods(stub: &StubServer) -> u64 {
    (0..stub.connections().len())
        .flat_map(|c| methods(stub, c))
        .filter(|m| m.starts_with("order."))
        .count() as u64
}

/// The report's answer to the question starting `q`.
fn answer(ev: &evidence::Evidence, q: &str) -> String {
    evidence::answers(ev)
        .into_iter()
        .find(|(question, _)| question.starts_with(q))
        .map(|(_, a)| a)
        .unwrap()
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
        stale_position_once: false,
        stray_cid: None,
        late_then_failing: 0,
        ghost_cancelled: false,
        hold_place_final: false,
        held: Vec::new(),
        orders_cursor: false,
        ioc_history_hidden: false,
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
    let batch = &step("batch").sent[0].outcomes;
    for item in [
        "item 0 Accepted { ack: Final }",
        "item 1 Accepted { ack: Final }",
    ] {
        assert!(batch.iter().any(|o| o == item), "{batch:?}");
    }
    // The per-IP limit check's counts: what was sent before each comparison. The first
    // counts the connection's cancel-on-disconnect arm as well as the place.
    let first = &step("place").comparisons[0];
    assert_eq!(
        (first.ws_since, first.connections_since),
        (2, 1),
        "{first:?}"
    );
    assert_eq!(
        ws_counted(ev),
        order_methods(&stub),
        "every WebSocket order method the stub received is counted once"
    );
    assert!(
        first.rest_since >= 2,
        "the order-book reads at least: {first:?}"
    );
    // Every HTTP request the stub answered is counted once: the program's own, and the
    // sessions' (logins and resync reads) as their journal records them. Only the stop
    // comparison's GET /positions comes after the last sample.
    assert!(
        first.session_rest_since >= 3,
        "the session's login and two resync reads: {first:?}"
    );
    let counted: u64 = ev
        .steps
        .iter()
        .flat_map(|s| &s.comparisons)
        .map(|c| c.rest_since + c.session_rest_since - c.session_not_sent_since)
        .sum();
    assert_eq!(
        counted + 1,
        stub.http_requests().len() as u64,
        "{:?}",
        stub.http_requests()
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
    let of_orders = ev
        .steps
        .iter()
        .flat_map(|s| &s.comparisons)
        .filter(|c| c.orders_compared)
        .count();
    assert!(
        answer(ev, "(a)").contains(&format!(
            "({of_orders} of {of_orders} agreed at the first read)"
        )),
        "{}",
        answer(ev, "(a)")
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
    // One journal for the three phases: each phase's connection its own, ingest order one
    // sequence across them.
    let reader =
        fbc_journal::JournalReader::open(evidence_dir.join("run").join("journal"), 0).unwrap();
    let stamps: Vec<fbc_core::Stamp> = reader
        .entries()
        .filter_map(|e| match e.unwrap().record {
            fbc_journal::Record::Inbound { stamp, .. }
            | fbc_journal::Record::InboundControl { stamp, .. } => Some(stamp),
            _ => None,
        })
        .collect();
    assert!(
        stamps.windows(2).all(|w| w[0].ingest_seq < w[1].ingest_seq),
        "ingest order repeats across phases"
    );
    let mut conns: Vec<u16> = stamps.iter().map(|s| s.conn.conn).collect();
    conns.dedup();
    assert_eq!(conns.len(), 3, "one connection number per phase: {conns:?}");
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
    // (b) is REST's lag behind the stream: a disconnect's position-only comparison waits out
    // --cod-wait (1 s here, the order still listed) before it reads, so it carries no lag.
    let comparisons: Vec<&evidence::Comparison> =
        ev.steps.iter().flat_map(|s| &s.comparisons).collect();
    for c in comparisons.iter().filter(|c| !c.orders_compared) {
        assert_eq!(c.lag_ms, None, "{c:?}");
    }
    let max = comparisons
        .iter()
        .filter(|c| c.orders_compared)
        .filter_map(|c| c.lag_ms)
        .max()
        .unwrap();
    assert_eq!(answer(ev, "(b)"), format!("at most {max} ms"));
    // The Safety cancels at each reconnect and each connection's arm are counted too.
    assert_eq!(ws_counted(ev), order_methods(&stub));
}

#[tokio::test]
async fn a_position_read_that_lags_keeps_the_agreeing_answer_and_the_orders_first_read_counts() {
    // GET /positions answers one stale position at the place's comparison; GET /orders agrees
    // at once.
    let venue = new_venue(true, true);
    venue.lock().unwrap().stale_position_once = true;
    let stub = stub_for(&venue, MAIN, 0, 0).await;
    let (evidence_dir, _e) = temp_dir("evidence-lag");
    let (lease_dir, _l) = temp_dir("lease-lag");
    let opts = options(&stub, &evidence_dir.join("run"), &lease_dir, 10);
    let (outcome, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(outcome.ok, "{printed}");

    let ev = &outcome.evidence;
    let place = &ev
        .steps
        .iter()
        .find(|s| s.name == "place")
        .unwrap()
        .comparisons[0];
    assert!(
        place.first_read_orders_agreed && !place.first_read_positions_agreed,
        "{place:?}"
    );
    assert!(
        place.orders_agreed && place.positions_agreed && place.reads == 2,
        "{place:?}"
    );
    // Answer (a) is of GET /orders alone: the lagging position does not take from it.
    let of_orders = ev
        .steps
        .iter()
        .flat_map(|s| &s.comparisons)
        .filter(|c| c.orders_compared)
        .count();
    assert!(
        answer(ev, "(a)").contains(&format!(
            "({of_orders} of {of_orders} agreed at the first read)"
        )),
        "{}",
        answer(ev, "(a)")
    );
    // The fixtures keep the stale first answer and the one that agreed.
    let slug = "rest-positions-place";
    let kept: Vec<&String> = outcome
        .fixture_files
        .iter()
        .filter(|f| f.contains(slug))
        .collect();
    assert!(
        kept.iter().any(|f| f.ends_with("-agreed.json"))
            && kept.iter().any(|f| !f.ends_with("-agreed.json")),
        "{:?}",
        outcome.fixture_files
    );
    let agreed = kept.iter().find(|f| f.ends_with("-agreed.json")).unwrap();
    let body = fs::read_to_string(outcome.fixtures.join(agreed)).unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&body).unwrap()["results"],
        json!([]),
        "the agreeing answer: flat"
    );
}

#[tokio::test]
async fn an_order_event_the_registry_cannot_place_fails_the_run_though_every_step_passed() {
    // An order event of a client id in our namespace that this run never minted: the link
    // notes it (an orphan), and the run must not claim a clean result.
    use fbc_core::{AccountKey, CidMint, Namespace, NamespaceLease, WallNs, encode_cid};
    let (stray_lease, _s) = temp_dir("lease-stray-mint");
    let lease =
        NamespaceLease::acquire(&stray_lease, AccountKey::new(1), Namespace::new(1)).unwrap();
    let mut mint = CidMint::new(lease, u64::MAX / 2, 0, WallNs(1_000_000_000));
    let fmt = fbc_venue_paradex::exec::exec_caps().order.client_id;
    let stray = encode_cid(&fmt, mint.mint().unwrap()).unwrap().to_string();
    let venue = new_venue(true, true);
    venue.lock().unwrap().stray_cid = Some(stray);
    let stub = stub_for(&venue, MAIN, 0, 0).await;
    let (evidence_dir, _e) = temp_dir("evidence-stray");
    let (lease_dir, _l) = temp_dir("lease-stray");
    let opts = options(&stub, &evidence_dir.join("run"), &lease_dir, 10);
    let (outcome, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    assert!(!outcome.ok, "{printed}");
    let stop = outcome
        .evidence
        .steps
        .iter()
        .find(|s| s.name == "stop")
        .unwrap();
    assert!(!stop.pass, "{stop:?}");
    assert!(
        outcome
            .evidence
            .notes
            .iter()
            .any(|n| n.contains("registry does not hold")),
        "{:?}",
        outcome.evidence.notes
    );
}

#[tokio::test]
async fn a_disconnect_wait_with_no_orders_answer_is_not_measured_and_not_called_not_cancelled() {
    // No --cod-wait: no GET /orders after the disconnect can answer within it.
    let venue = new_venue(true, true);
    let stub = stub_for(&venue, MAIN, 0, 0).await;
    let (evidence_dir, _e) = temp_dir("evidence-unread");
    let (lease_dir, _l) = temp_dir("lease-unread");
    let opts = options(&stub, &evidence_dir.join("run"), &lease_dir, 0);
    let (outcome, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    let ev = &outcome.evidence;
    for name in ["cod-dropped", "cod-graceful"] {
        let s = ev.steps.iter().find(|s| s.name == name).unwrap();
        let d = s.disconnect.as_ref().unwrap();
        assert!(!d.cancelled && !d.still_listed, "{name}: {d:?}");
        assert!(!s.pass, "{name}: {s:?}");
        assert!(
            s.notes
                .iter()
                .any(|n| n.contains("no GET /orders answered")),
            "{name}: {:?}",
            s.notes
        );
    }
    let report = fs::read_to_string(&outcome.report).unwrap();
    assert!(!report.contains("NOT cancelled"), "{report}");
    assert!(report.contains("not measured"), "{report}");
    assert!(!outcome.ok, "{printed}");
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
        let out: testnet::Out = buf.clone();
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
    assert_eq!(
        accept::parse_results(br#"{"results":[]}"#, accept::ORDER_ROW),
        Ok(Vec::new())
    );
    assert!(accept::parse_results(b"{}", accept::ORDER_ROW).is_err());
    assert!(accept::parse_results(br#"{"results":{}}"#, accept::ORDER_ROW).is_err());
    assert!(accept::parse_results(b"<html>", accept::ORDER_ROW).is_err());
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
fn the_privacy_check_scans_fixtures_written_under_a_git_ignored_directory() {
    // An evidence directory inside a checkout's ignored target/: git lists none of its files,
    // and the check must still scan every one.
    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../scripts/privacy-check.sh");
    let (repo, _r) = temp_dir("ignored-repo");
    let init = std::process::Command::new("git")
        .args(["init", "-q"])
        .current_dir(&repo)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .status()
        .unwrap();
    assert!(init.success());
    fs::write(repo.join(".gitignore"), "target/\n").unwrap();
    let fixtures = repo.join("target/run/fixtures");
    fs::create_dir_all(&fixtures).unwrap();
    fs::write(fixtures.join("a.json"), r#"{"account":"SCRUBBED"}"#).unwrap();
    assert_eq!(evidence::privacy_check(&script, &fixtures), Ok(()));
    fs::write(
        fixtures.join("b.json"),
        json!({"account": synthetic().0}).to_string(),
    )
    .unwrap();
    let refused = evidence::privacy_check(&script, &fixtures).unwrap_err();
    assert!(refused.contains("b.json"), "{refused}");
}

#[test]
fn a_session_counts_each_order_method_and_its_arm() {
    use fbc_core::{ConnState, RpcId, SubmitOutcome};
    use link::Note;
    let accepted = || SubmitOutcome::Accepted {
        ack: fbc_core::AckLevel::Final,
    };
    // Two epochs (a reconnect), each with its arm (answered twice: provisional, then final);
    // one login refused in between; three requests of ours sent, one not sent.
    let notes = vec![
        Note::Conn(ConnState::Authenticated),
        Note::Outcome {
            rpc: RpcId(1),
            item: None,
            outcome: accepted(),
            ours: false,
        },
        Note::Outcome {
            rpc: RpcId(1),
            item: None,
            outcome: accepted(),
            ours: false,
        },
        Note::Submitted {
            rpc: RpcId(2),
            sent: Ok(()),
        },
        Note::Submitted {
            rpc: RpcId(3),
            sent: Err(fbc_core::NotSentReason::Disconnected),
        },
        Note::LoginRefused("refused".to_owned()),
        Note::Conn(ConnState::Authenticated),
        Note::Outcome {
            rpc: RpcId(4),
            item: None,
            outcome: accepted(),
            ours: false,
        },
        Note::Submitted {
            rpc: RpcId(5),
            sent: Ok(()),
        },
        Note::Submitted {
            rpc: RpcId(6),
            sent: Ok(()),
        },
    ];
    assert_eq!(accept::session_counts(&notes), 5);
}

#[test]
fn an_asynchronous_reject_the_shared_link_notes_is_a_link_problem_of_the_run() {
    // FBC-elg7 moved the glue to src/link.rs, which notes an asynchronous reject for its driver
    // to judge (Note::AsyncReject) instead of as a problem: this run still counts every one.
    use fbc_core::{Namespace, OpKind, OrderRef, Reject, RejectKind};
    use link::Note;
    let caps = fbc_venue_paradex::factory::caps();
    let vid = fbc_core::dispatch(&caps, Namespace::new(1), |s| s.venue_order_id("V-9")).unwrap();
    let reject = Reject {
        kind: RejectKind::PostOnlyWouldCross,
        venue_code: Some("POST_ONLY_WOULD_CROSS".into()),
        raw: "POST_ONLY_WOULD_CROSS".into(),
    };
    let notes = vec![Note::AsyncReject {
        target: OrderRef::Venue(vid),
        op: OpKind::Place,
        reject,
    }];
    let problems = accept::link_problems(&notes);
    assert_eq!(problems.len(), 1, "{problems:?}");
    assert!(
        problems[0].starts_with("the link noted a problem: asynchronous reject of a Place"),
        "{problems:?}"
    );
}

#[test]
fn the_sessions_http_requests_and_connections_are_counted_as_their_journal_records_them() {
    // Codex r4244249042: what the sessions sent, not what their notes imply. A login whose
    // socket dropped in flight was still asked for; a cached token asks for none.
    use fbc_core::{
        ConnKey, HttpFailure, HttpMethod, HttpRequest, HttpTag, MonoNs, Stamp, TrafficClass,
        WallNs, WireSlice, WireUrl,
    };
    use fbc_journal::{ControlEvent, JournalSink, Record, RecordRef, Recorded};
    #[derive(Default)]
    struct Keep(usize);
    impl JournalSink for Keep {
        fn record(&mut self, _: TrafficClass, _: WallNs, _: &Record) -> Recorded {
            self.0 += 1;
            Recorded::Ok
        }
        fn omit(&mut self, _: TrafficClass, _: WallNs) -> Recorded {
            Recorded::DroppedCounted
        }
    }
    let mut sink = accept::JournalCount::new(Keep::default());
    let counts = sink.counts();
    let key = ConnKey { conn: 1, epoch: 1 };
    let req = HttpRequest {
        method: HttpMethod::Post,
        url: WireUrl::plain("http://127.0.0.1:9/v1/auth"),
        headers: Vec::new(),
        body: WireSlice::plain(Vec::new()),
    };
    let ask = || RecordRef::HttpRequest {
        at: MonoNs(1),
        conn: key,
        tag: HttpTag(1),
        rpc: None,
        req: &req,
    };
    let stamp = Stamp {
        ingest_seq: 1,
        kernel_rx: None,
        recv_mono: MonoNs(2),
        recv_wall: WallNs(2),
        conn: key,
    };
    let (n, w) = (TrafficClass::Normal, WallNs(1));
    let _ = sink.record(
        n,
        w,
        &Record::Control {
            at: MonoNs(1),
            ev: ControlEvent::Opened(key),
        },
    );
    // A login, a read, and one the limiter refused (never sent).
    for _ in 0..3 {
        let _ = sink.record_ref(n, w, ask());
    }
    let _ = sink.record_ref(
        n,
        w,
        RecordRef::HttpResult {
            stamp,
            tag: HttpTag(1),
            result: Err(HttpFailure::NotSent),
        },
    );
    // A request lost in flight was sent.
    let _ = sink.record_ref(
        n,
        w,
        RecordRef::HttpResult {
            stamp,
            tag: HttpTag(1),
            result: Err(HttpFailure::Lost),
        },
    );
    let _ = sink.record(
        n,
        w,
        &Record::Control {
            at: MonoNs(3),
            ev: ControlEvent::Closed(key),
        },
    );
    let _ = sink.record(
        n,
        w,
        &Record::Control {
            at: MonoNs(4),
            ev: ControlEvent::Opened(ConnKey { conn: 1, epoch: 2 }),
        },
    );
    assert_eq!(
        counts.get(),
        accept::SessionsSent {
            asked: 3,
            not_sent: 1,
            connections: 2
        }
    );
    assert_eq!(sink.inner().0, 8, "every record goes on to the journal");
    assert_eq!(sink.omit(n, w), Recorded::DroppedCounted);
}

#[test]
fn an_sbe_frame_holding_a_credential_as_bytes_is_withheld_before_it_is_written_as_hex() {
    let mut frame = vec![0u8; 16];
    frame.extend(format!("{} {}", "Bearer", "abcdefghij".repeat(3)).as_bytes());
    let refused = evidence::sbe_fixture("c", &frame).unwrap_err();
    assert!(refused.contains(&"a bearer token"), "{refused:?}");
    let mut jwt = vec![1u8; 8];
    jwt.extend(token().as_bytes());
    assert!(evidence::sbe_fixture("c", &jwt).is_err());
    let text = evidence::sbe_fixture("c", &[0x20, 0x00, 0x14, 0x00]).unwrap();
    assert!(text.contains("20 00 14 00"), "{text}");
}

#[tokio::test]
async fn a_rest_read_waits_for_the_shared_limiter_and_is_not_sent_without_room() {
    use fbc_core::{LimitScope, OpKind, RateLimit, TagSet};
    use fbc_runtime::{RateLimiter, SafetyReserve};
    let venue = new_venue(true, true);
    let stub = stub_for(&venue, 0, 0, 0).await;
    let (lease_dir, _l) = temp_dir("lease-limit");
    let (evidence_dir, _e) = temp_dir("evidence-limit");
    let opts = options(&stub, &evidence_dir, &lease_dir, 1);
    // One account read a minute, already spent by a session.
    let limiter = RateLimiter::new(
        &[RateLimit {
            scope: LimitScope::Account,
            ops: TagSet::of(&[OpKind::Query]),
            per: Duration::from_secs(60),
            units: 1,
        }],
        SafetyReserve::percent(0).unwrap(),
    )
    .unwrap();
    let mut rest = auth_rest::RestReader::new(
        &trade::config(&opts.trade),
        secrets().unwrap(),
        fbc_runtime::Connector::new(ProxyConfig::Direct),
        limiter.clone(),
    )
    .unwrap();
    let first = rest
        .get(
            fbc_venue_paradex::exec::open_orders_request(&opts.trade.rest_url),
            std::time::Instant::now() + Duration::from_secs(5),
        )
        .await;
    assert_eq!(first.map(|a| a.status), Ok(200));
    let refused = rest
        .get(
            fbc_venue_paradex::exec::open_orders_request(&opts.trade.rest_url),
            std::time::Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap_err();
    assert!(refused.contains("rate limit"), "{refused}");
    // The login and the one read went out; the refused read did not.
    let orders = stub
        .http_requests()
        .iter()
        .filter(|r| r.contains("/v1/orders"))
        .count();
    assert_eq!(orders, 1, "{:?}", stub.http_requests());
    assert_eq!(rest.requests(), 2);
}

#[test]
fn a_disconnect_is_measured_only_with_an_answer_that_listed_the_order_or_a_cancel() {
    // (cancelled, still listed, a read failed, filled)
    assert!(accept::disconnect_measured(true, false, false, false));
    assert!(accept::disconnect_measured(false, true, false, false));
    assert!(
        !accept::disconnect_measured(false, false, false, false),
        "no GET /orders answered within the wait: nothing measured"
    );
    assert!(!accept::disconnect_measured(false, true, true, false));
    assert!(!accept::disconnect_measured(false, true, false, true));
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

#[tokio::test]
async fn a_listing_followed_by_failed_reads_is_not_measured_and_never_called_not_cancelled() {
    // Reviewer B, RB7: the first GET /orders after the drop still lists B3 (Paradex has not
    // applied cancel-on-disconnect yet), every later one within --cod-wait answers 500, and
    // the history then shows B3 closed by the disconnect.
    let venue = new_venue(true, true);
    venue.lock().unwrap().late_then_failing = 1;
    let stub = stub_for(&venue, MAIN, 0, 0).await;
    let (evidence_dir, _e) = temp_dir("evidence-late");
    let (lease_dir, _l) = temp_dir("lease-late");
    let opts = options(&stub, &evidence_dir.join("run"), &lease_dir, 2);
    let (outcome, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    let ev = &outcome.evidence;
    let s = ev.steps.iter().find(|s| s.name == "cod-dropped").unwrap();
    let d = s.disconnect.as_ref().unwrap();
    assert!(!d.cancelled && !d.still_listed, "{d:?}");
    assert!(!s.pass, "{s:?}");
    // Every read is recorded: the one that listed it, then the ones that failed.
    assert!(
        d.reads.first().is_some_and(|r| r.contains("listed")),
        "{d:?}"
    );
    assert!(
        d.reads.iter().skip(1).all(|r| r.contains("status 500")),
        "{d:?}"
    );
    let verdict = answer(ev, "cancel-on-disconnect, dropped");
    assert!(!verdict.contains("NOT cancelled"), "{verdict}");
    assert!(verdict.contains("not measured"), "{verdict}");
    assert!(!outcome.ok, "{printed}");
}

#[tokio::test]
async fn an_order_ended_earlier_that_a_later_listing_shows_again_is_a_mismatch() {
    // Codex r4244249052: B1, cancelled at the cancel step, is listed again once the batch is
    // placed; the batch's comparison must not agree.
    let venue = new_venue(true, true);
    venue.lock().unwrap().ghost_cancelled = true;
    let stub = stub_for(&venue, MAIN, 0, 0).await;
    let (evidence_dir, _e) = temp_dir("evidence-ghost");
    let (lease_dir, _l) = temp_dir("lease-ghost");
    let mut opts = options(&stub, &evidence_dir.join("run"), &lease_dir, 2);
    opts.rest_wait_secs = 1;
    let (outcome, printed) = run_against(&opts).await;
    // The failed batch ends the main phase's steps early: the script is not played out.
    drop(stub);

    let ev = &outcome.evidence;
    let step = |name: &str| ev.steps.iter().find(|s| s.name == name).unwrap();
    assert!(step("cancel").pass, "{:?}", step("cancel"));
    let batch = &step("batch").comparisons[0];
    assert!(!batch.orders_agreed, "{batch:?}");
    assert!(
        batch
            .last_mismatches
            .iter()
            .any(|m| m.starts_with("B1") && m.contains("ended on the stream, still listed")),
        "{batch:?}"
    );
    assert!(!step("batch").pass);
    assert!(answer(ev, "(a)").starts_with("NO"), "{}", answer(ev, "(a)"));
    assert!(!outcome.ok, "{printed}");
}

#[test]
fn a_list_row_without_its_fields_is_no_snapshot() {
    use accept::{ORDER_ROW, POSITION_ROW};
    // Codex r4244249050: {"results":[{}]} is no empty market and no flat position.
    assert!(accept::parse_results(br#"{"results":[{}]}"#, ORDER_ROW).is_err());
    assert!(accept::parse_results(br#"{"results":[{}]}"#, POSITION_ROW).is_err());
    assert!(accept::parse_results(br#"{"results":[7]}"#, ORDER_ROW).is_err());
    let order = json!({"client_id": "c", "market": MARKET, "status": "OPEN", "price": "1",
        "size": "0.1", "remaining_size": "0.1"});
    let mut numeric = order.clone();
    numeric["size"] = json!(0.1);
    let body = |row: &Value| json!({ "results": [row] }).to_string().into_bytes();
    assert!(accept::parse_results(&body(&numeric), ORDER_ROW).is_err());
    assert_eq!(
        accept::parse_results(&body(&order), ORDER_ROW).map(|r| r.len()),
        Ok(1)
    );
    let position = json!({"market": MARKET, "side": "LONG", "size": "0.1", "status": "OPEN"});
    assert_eq!(
        accept::parse_results(&body(&position), POSITION_ROW).map(|r| r.len()),
        Ok(1)
    );
    assert!(accept::parse_results(&body(&position), ORDER_ROW).is_err());
}

#[test]
fn impossible_history_quantities_are_refused_not_read_as_nothing_filled() {
    // Codex r4244249043.
    let d = |t: &str| Some(Decimal::from_str(t).unwrap());
    let one = d("0.0002");
    assert_eq!(
        accept::history_filled(d("0.0002"), d("0.0002"), one),
        Ok(false)
    );
    assert_eq!(
        accept::history_filled(d("0.0002"), d("0.0001"), one),
        Ok(true)
    );
    assert_eq!(accept::history_filled(d("0.0002"), d("0"), one), Ok(true));
    for (size, left) in [
        ("-0.0002", "-0.0002"),
        ("0", "0"),
        ("0.0002", "1"),
        ("0.0002", "-0.0001"),
    ] {
        assert!(
            accept::history_filled(d(size), d(left), one).is_err(),
            "{size} {left}"
        );
    }
    assert!(
        accept::history_filled(d("0.0003"), d("0.0003"), one).is_err(),
        "a size other than the order's"
    );
    assert!(accept::history_filled(None, d("0.0002"), one).is_err());
    assert_eq!(
        accept::history_filled(d("0.0003"), d("0.0003"), None),
        Ok(false)
    );
}

#[test]
fn a_close_that_errored_or_did_not_finish_is_no_clean_graceful_close() {
    // Codex r4244249045.
    assert_eq!(accept::graceful_end(Some(Ok(()))), Ok(()));
    let errored = accept::graceful_end(Some(Err("the journal failed".to_owned()))).unwrap_err();
    assert!(errored.contains("the journal failed"), "{errored}");
    assert!(accept::graceful_end(None).unwrap_err().contains("5 s"));
}

/// Evidence with one step holding `comparisons`.
fn with_comparisons(comparisons: Vec<evidence::Comparison>) -> evidence::Evidence {
    let mut ev = evidence::Evidence::default();
    ev.step("place").comparisons = comparisons;
    ev
}

#[test]
fn no_lag_bound_is_given_when_a_comparison_never_agreed() {
    // Codex r4244249048.
    let agreed = evidence::Comparison {
        orders_compared: true,
        orders_agreed: true,
        positions_agreed: true,
        lag_ms: Some(50),
        ..evidence::Comparison::default()
    };
    let stale = evidence::Comparison {
        orders_compared: true,
        lag_ms: None,
        ..evidence::Comparison::default()
    };
    let ev = with_comparisons(vec![agreed.clone(), stale]);
    let b = answer(&ev, "(b)");
    assert!(!b.contains("at most"), "{b}");
    assert!(b.contains("not established") && b.contains("1 of 2"), "{b}");
    let ev = with_comparisons(vec![agreed]);
    assert_eq!(answer(&ev, "(b)"), "at most 50 ms");
}

#[test]
fn the_history_lag_of_a_disconnect_is_labelled_as_timed_from_the_socket_end() {
    // Reviewer B, RB8.
    let mut ev = evidence::Evidence::default();
    ev.step("place").histories.push(evidence::History {
        label: "B1".to_owned(),
        wanted: "OPEN".to_owned(),
        listed: true,
        lag_ms: Some(30),
        ..evidence::History::default()
    });
    ev.step("cod-dropped").disconnect = Some(evidence::Disconnect {
        history: Some(evidence::History {
            label: "B3".to_owned(),
            wanted: "CLOSED".to_owned(),
            listed: true,
            lag_ms: Some(2_000),
            ..evidence::History::default()
        }),
        ..evidence::Disconnect::default()
    });
    let e = answer(&ev, "(e)");
    assert!(e.contains("B1 OPEN: listed after 30 ms;"), "{e}");
    assert!(
        e.contains("B3 CLOSED: listed 2000 ms after the socket ended")
            && e.contains("cancel-on-disconnect latency"),
        "{e}"
    );
}

#[test]
fn positions_with_no_fill_are_said_to_be_compared_with_the_seed_only() {
    // Reviewer B, RB9.
    let flat = evidence::Comparison {
        positions_agreed: true,
        ..evidence::Comparison::default()
    };
    let mut ev = with_comparisons(vec![flat]);
    let c = answer(&ev, "(c)");
    assert!(c.contains("no fill event arrived"), "{c}");
    ev.fills = 1;
    let c = answer(&ev, "(c)");
    assert!(!c.contains("no fill event arrived"), "{c}");
    assert!(c.starts_with("yes at every comparison"), "{c}");
}

#[test]
fn a_report_or_evidence_file_holding_a_credential_is_withheld_and_fails_the_run() {
    // Reviewer B, RB4: notes reach both files.
    let mut ev = evidence::Evidence::default();
    let (report, json) = evidence::outputs(&mut ev).unwrap();
    assert!(report.contains("# Paradex testnet acceptance"), "{report}");
    assert!(ev.ok());
    let leaked = format!("{} {}", "Bearer", "abcdefghij".repeat(3));
    ev.notes.push(format!("a session error echoed {leaked}"));
    let (report, json2) = evidence::outputs(&mut ev).unwrap();
    for text in [&report, &json2] {
        assert!(!text.contains(&leaked), "{text}");
        assert!(evidence::privacy_hits(text).is_empty(), "{text}");
        assert!(text.contains("withheld"), "{text}");
    }
    assert!(!ev.ok());
    assert!(
        ev.artifact_errors
            .iter()
            .any(|e| e.contains("report.md") && e.contains("a bearer token")),
        "{:?}",
        ev.artifact_errors
    );
    assert!(serde_json::from_str::<Value>(&json).is_ok());
    assert!(serde_json::from_str::<Value>(&json2).is_ok());
}

/// The order socket's script for a run that stops after the place: the main connection's
/// opening, the place and Stop's cancel of B1 (one order.cancel_batch), then the stop phase's
/// opening.
async fn stub_stopping_after_place(venue: &Arc<Mutex<Venue>>) -> StubServer {
    let mut steps = Vec::new();
    for (conn, frames) in [(0, 6 + 2), (1, 6)] {
        steps.push(Step::Accept);
        let with = responder(Arc::clone(venue), conn);
        steps.extend((0..frames).map(|_| Step::Respond {
            conn,
            with: with.clone(),
        }));
    }
    let stub = StubServer::start(WsScript::new(steps), routes(Arc::clone(venue)))
        .await
        .unwrap();
    let conns = Arc::clone(&venue.lock().unwrap().conns);
    let _ = conns.set(stub.connections_view());
    stub
}

#[tokio::test]
async fn a_placement_is_recorded_only_with_its_final_phase_and_never_passes_without_it() {
    // The CI failure on c5f8479: the reply's provisional acceptance already shows the order
    // open, so the place step was recorded before the order event's final phase came. Here
    // the final phase comes only before the answer to the next request (Stop's cancel): the
    // step waits for it, by that event, no longer than the step timeout, and never passes
    // without it.
    let venue = new_venue(true, true);
    venue.lock().unwrap().hold_place_final = true;
    let stub = stub_stopping_after_place(&venue).await;
    let (evidence_dir, _e) = temp_dir("evidence-final-late");
    let (lease_dir, _l) = temp_dir("lease-final-late");
    let mut opts = options(&stub, &evidence_dir.join("run"), &lease_dir, 10);
    opts.trade.step_timeout_secs = 1;
    opts.rest_wait_secs = 1;
    let (outcome, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    let ev = &outcome.evidence;
    let place = ev.steps.iter().find(|s| s.name == "place").unwrap();
    assert!(!place.pass, "{place:?}\n{printed}");
    assert!(
        place.notes.iter().any(|n| n.contains("final phase")),
        "{place:?}"
    );
    assert!(
        place.sent[0].outcomes.iter().all(|o| !o.contains("Final")),
        "{place:?}"
    );
    // Stop's cancel of B1 went out, and only then did the held final phase come.
    let main = methods(&stub, 0);
    assert!(
        main.last().is_some_and(|m| m.starts_with("order.cancel")),
        "{main:?}"
    );
    assert!(!outcome.ok, "{printed}");
}

#[tokio::test]
async fn a_paginated_orders_answer_is_never_agreement() {
    // Codex r4277685465: a further page could list an order the stream ended.
    let venue = new_venue(true, true);
    venue.lock().unwrap().orders_cursor = true;
    let stub = stub_stopping_after_place(&venue).await;
    let (evidence_dir, _e) = temp_dir("evidence-cursor");
    let (lease_dir, _l) = temp_dir("lease-cursor");
    let mut opts = options(&stub, &evidence_dir.join("run"), &lease_dir, 10);
    opts.rest_wait_secs = 1;
    let (outcome, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    let ev = &outcome.evidence;
    let place = ev.steps.iter().find(|s| s.name == "place").unwrap();
    assert!(!place.pass, "{place:?}\n{printed}");
    let c = &place.comparisons[0];
    assert!(!c.orders_agreed && !c.first_read_orders_agreed, "{c:?}");
    assert!(
        c.first_mismatches
            .iter()
            .any(|m| m.contains("further page")),
        "{c:?}"
    );
    assert!(c.page_keys.contains(&"next".to_owned()), "{c:?}");
    assert!(!outcome.ok, "{printed}");
}

#[tokio::test]
async fn an_ioc_whose_history_gives_no_cancel_reason_is_not_measured_and_fails_its_step() {
    // Codex r4277685475: 0092's question is the reason; without it the step is no pass.
    let venue = new_venue(true, true);
    venue.lock().unwrap().ioc_history_hidden = true;
    let stub = stub_for(&venue, MAIN, 0, 0).await;
    let (evidence_dir, _e) = temp_dir("evidence-ioc-hidden");
    let (lease_dir, _l) = temp_dir("lease-ioc-hidden");
    let mut opts = options(&stub, &evidence_dir.join("run"), &lease_dir, 10);
    opts.rest_wait_secs = 1;
    let (outcome, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    let ev = &outcome.evidence;
    let ioc = ev.steps.iter().find(|s| s.name == "ioc").unwrap();
    assert!(!ioc.pass, "{ioc:?}");
    assert!(
        ioc.notes.iter().any(|n| n.starts_with("not measured")),
        "{ioc:?}"
    );
    let a = answer(ev, "0092 (1)");
    assert!(a.starts_with("not measured"), "{a}");
    assert!(!outcome.ok, "{printed}");
}

#[test]
fn an_ioc_history_row_without_a_cancel_reason_is_not_measured() {
    let closed = |reason: Option<&str>| evidence::History {
        label: "IOC".to_owned(),
        wanted: "CLOSED".to_owned(),
        listed: true,
        status: Some("CLOSED".to_owned()),
        cancel_reason: reason.map(str::to_owned),
        ..evidence::History::default()
    };
    assert_eq!(
        evidence::ioc_reason(Some(&closed(Some("EXPIRED")))),
        Ok("EXPIRED")
    );
    for h in [
        closed(None),
        closed(Some("")),
        evidence::History {
            listed: false,
            ..closed(Some("EXPIRED"))
        },
    ] {
        let why = evidence::ioc_reason(Some(&h)).unwrap_err();
        assert!(why.starts_with("not measured"), "{why}");
    }
    assert!(evidence::ioc_reason(None).is_err());
}

#[test]
fn request_info_is_reported_for_the_amended_order_alone_and_matched_to_the_amend() {
    // Codex r4277685488.
    let mut ev = evidence::Evidence::default();
    ev.step("amend").sent.push(evidence::Sent {
        kind: "amend".to_owned(),
        rpc: 9,
        items: 1,
        outcomes: Vec::new(),
    });
    ev.step("amend").orders.push(evidence::Trace {
        label: "B1".to_owned(),
        client_id: "b1".to_owned(),
        ..evidence::Trace::default()
    });
    let row = |cid: &str, rt: u8, rid: &str| evidence::FrameRow {
        client_id: cid.to_owned(),
        status: "OPEN".to_owned(),
        request_status: Some(4),
        request_type: Some(rt),
        request_id: Some(rid.to_owned()),
        ..evidence::FrameRow::default()
    };
    ev.frames = vec![row("other", 1, "9"), row("b1", 1, "9")];
    let a = answer(&ev, "FBC-g3bw");
    assert!(a.contains("1 of B1's events carried it"), "{a}");
    assert!(
        a.contains("MODIFY_ORDER yes") && a.contains("requestId is the amend's id 9: yes"),
        "{a}"
    );
    assert!(
        a.contains("1 event of another order carried request_info"),
        "{a}"
    );
    ev.frames = vec![row("b1", 2, "req-9")];
    let a = answer(&ev, "FBC-g3bw");
    assert!(
        a.contains("MODIFY_ORDER no") && a.contains("requestId is the amend's id 9: no"),
        "{a}"
    );
    assert!(!a.contains("another order"), "{a}");
    ev.frames = vec![row("other", 1, "9")];
    let a = answer(&ev, "FBC-g3bw");
    assert!(
        a.starts_with("no OrderEvent of B1 carried request_info"),
        "{a}"
    );
}
