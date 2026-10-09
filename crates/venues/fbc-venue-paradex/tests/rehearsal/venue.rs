//! The stub Paradex the rehearsal runs against: fbc-conformance's stub server, started on a
//! thread and runtime of its own (so what tungstenite logs for the stub's side of a socket is
//! never taken for the session's), answering from the requests it reads (FBC-xg7) with Paradex's
//! documented replies.
//!
//! - **HTTP** (`/v1`): the login (`POST /v1/auth`, a made-up token), and the resync's two reads,
//!   `GET /v1/orders` (the open orders the test sets) and `GET /v1/positions` (the position the
//!   test sets), each answered from the [`Book`] at the moment it is read.
//! - **The order socket**: each JSON-RPC frame is answered by its method. `auth` and `subscribe`
//!   with an empty result; `order.cancel_on_disconnect` with `enabled: true`; `order.create`
//!   with the order under a venue id of the stub's, then the `OrderEvent` frames (SBE schema
//!   1:2, built here from Paradex's schema as `fixtures/paradex/exec/README.md` lays it out)
//!   that the order's [`Fate`] says: resting (OPEN), or filled (OPEN, the `FillEvent`, the same
//!   `FillEvent` again, then CLOSED with nothing open); `order.cancel` and
//!   `order.cancel_batch` queued, then each order's CLOSED by `USER_CANCELED`. Any other method
//!   fails the script. The account in every SBE frame is 32 made-up bytes.

use std::collections::{BTreeMap, VecDeque};
use std::str::FromStr;
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};

use fbc_conformance::{
    ConnRecord, Frame, HttpReply, HttpRouter, PathPattern, Responder, ScriptError, Step,
    StubServer, WsScript,
};
use fbc_runtime::http::Method;
use rust_decimal::Decimal;
use serde_json::{Value, json};

/// The session token the stub's login gives: made-up text that never looks like a JWT.
pub const TOKEN: &str = "SYNTHETIC.session-token.offline-rehearsal";
/// The one market.
pub const MARKET: &str = "BTC-USD-PERP";

/// What becomes of the next order the stub is asked to create.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum Fate {
    /// Acknowledged and resting (OPEN).
    Rest,
    /// Acknowledged, then filled whole by one fill (sent twice, as a venue may repeat it),
    /// then CLOSED with nothing open.
    Fill,
}

/// One order the stub holds.
#[derive(Clone, Debug)]
pub struct Order {
    pub vid: String,
    pub cid: String,
    pub side: &'static str,
    pub price: String,
    pub size: String,
    pub reduce_only: bool,
    pub open: bool,
}

/// What the stub's venue holds: its open orders, the account's position on the market (in
/// BTC, signed), the fates of the orders still to be created, and the counters.
#[derive(Default)]
pub struct Book {
    pub orders: BTreeMap<String, Order>,
    pub position: Decimal,
    pub fates: VecDeque<Fate>,
    next_vid: u64,
    seq: i64,
    fill_seq: u64,
    /// Each login's `PARADEX-STARKNET-SIGNATURE` header, as the stub read it.
    pub login_signatures: Vec<String>,
    /// The logins to refuse, in order, each as an HTTP status and a body in which `{account}`
    /// and `{signature}` stand for the login's own headers, as a venue echoing them would
    /// write them (FBC-3f8z); once none is left, a login gives [`TOKEN`].
    pub refused_logins: VecDeque<(u16, String)>,
}

impl Book {
    fn vid(&mut self) -> String {
        self.next_vid += 1;
        format!("17595000000000{:05}", self.next_vid)
    }

    fn seq(&mut self) -> i64 {
        self.seq += 1;
        5_000 + self.seq
    }

    /// The `GET /orders` answer: every open order.
    fn orders_json(&self) -> String {
        let results: Vec<Value> = self
            .orders
            .values()
            .filter(|o| o.open)
            .map(|o| {
                let flags: Vec<&str> = if o.reduce_only {
                    vec!["REDUCE_ONLY"]
                } else {
                    vec![]
                };
                json!({
                    "id": o.vid, "client_id": o.cid, "market": MARKET, "side": o.side,
                    "type": "LIMIT", "instruction": "POST_ONLY", "price": o.price,
                    "size": o.size, "remaining_size": o.size, "status": "OPEN", "flags": flags,
                    "account": account(),
                    "avg_fill_price": "", "cancel_reason": "", "created_at": 1759622400000_i64,
                    "last_updated_at": 1759622400000_i64, "published_at": 1759622400000_i64,
                    "received_at": 1759622400000_i64, "request_info": null, "seq_no": 1,
                    "stp": "EXPIRE_TAKER", "timestamp": 1759622400000_i64, "trigger_price": "0",
                })
            })
            .collect();
        json!({ "results": results }).to_string()
    }

    /// The `GET /positions` answer: the market's position, or none when flat.
    fn positions_json(&self) -> String {
        if self.position.is_zero() {
            return r#"{"results":[]}"#.to_owned();
        }
        let side = if self.position.is_sign_positive() {
            "LONG"
        } else {
            "SHORT"
        };
        json!({ "results": [{
            "account": account(),
            "average_entry_price": "60000", "average_entry_price_usd": "60000",
            "average_exit_price": "0", "cached_funding_index": "0", "closed_at": 0,
            "cost": "0", "cost_usd": "0", "created_at": 1759622300000_i64,
            "id": "BTC-USD-PERP-position", "last_fill_id": "fill-BTC-USD-PERP",
            "last_updated_at": 1759622390000_i64, "leverage": "", "liquidation_price": "",
            "market": MARKET, "realized_positional_funding_pnl": "0",
            "realized_positional_pnl": "0", "seq_no": 7, "side": side,
            "size": self.position.to_string(), "status": "OPEN",
            "unrealized_funding_pnl": "0", "unrealized_pnl": "0",
        }]})
        .to_string()
    }
}

/// The account the REST answers name: the 32 made-up bytes the SBE frames carry, as hex (built
/// here, so no address-shaped value sits in this file).
fn account() -> String {
    let hex: String = (0xa0u8..=0xbf).map(|b| format!("{b:02x}")).collect();
    format!("0x{hex}")
}

pub type Shared = Arc<Mutex<Book>>;

pub fn lock(book: &Shared) -> MutexGuard<'_, Book> {
    book.lock().unwrap_or_else(|e| e.into_inner())
}

fn reply(status: u16, body: impl Into<Vec<u8>>) -> HttpReply {
    HttpReply {
        status,
        body: body.into(),
    }
}

/// The stub's HTTP answers, read from `book` at each request.
fn routes(book: &Shared) -> HttpRouter {
    let (login, orders, positions) = (book.clone(), book.clone(), book.clone());
    HttpRouter::new()
        .route_fn(Method::POST, PathPattern::exact("/v1/auth"), move |req| {
            let signature = req.header("PARADEX-STARKNET-SIGNATURE").unwrap_or("");
            let account = req.header("PARADEX-STARKNET-ACCOUNT").unwrap_or("");
            let mut book = lock(&login);
            book.login_signatures.push(signature.to_owned());
            match book.refused_logins.pop_front() {
                Some((status, body)) => {
                    // Each header as the inside of a JSON string, as a venue's echo writes it.
                    let inside = |text: &str| {
                        let quoted = Value::String(text.to_owned()).to_string();
                        quoted[1..quoted.len() - 1].to_owned()
                    };
                    let body = body
                        .replace("{account}", &inside(account))
                        .replace("{signature}", &inside(signature));
                    reply(status, body)
                }
                None => reply(200, format!(r#"{{"jwt_token":"{TOKEN}"}}"#)),
            }
        })
        .route_fn(Method::GET, PathPattern::exact("/v1/orders"), move |_| {
            reply(200, lock(&orders).orders_json())
        })
        .route_fn(
            Method::GET,
            PathPattern::exact("/v1/positions"),
            move |_| reply(200, lock(&positions).positions_json()),
        )
}

/// A decimal string as Paradex's SBE 10^-8 mantissa.
fn e8(text: &str) -> i64 {
    let d = Decimal::from_str(text).unwrap() * Decimal::from(100_000_000);
    i64::try_from(d.mantissa() / 10i128.pow(d.scale())).unwrap()
}

/// An `OrderEvent` (template 20) at schema 1:2 in its 128-byte block layout: `o` with status
/// `status` (1 NEW, 3 OPEN, 4 CLOSED), `open` of it still open and the cancel reason `reason`.
fn order_event(o: &Order, seq: i64, status: u8, open: &str, reason: &str) -> Vec<u8> {
    let null = i64::MIN;
    let ts = 1_759_500_000_205_011i64;
    let side = if o.side == "BUY" { 1u8 } else { 2 };
    let mut f = Vec::new();
    for v in [128u16, 20, 1, 2] {
        f.extend(v.to_le_bytes());
    }
    f.extend(ts.to_le_bytes());
    f.extend(seq.to_le_bytes());
    f.extend([status, side, 1, 3]); // the status, the side, LIMIT, POST_ONLY
    f.extend(e8(&o.price).to_le_bytes());
    f.extend(null.to_le_bytes()); // triggerPrice
    f.extend(e8(&o.size).to_le_bytes()); // size
    f.extend(e8(open).to_le_bytes()); // sizeOpen
    f.extend(null.to_le_bytes()); // avgFillPrice
    f.extend(ts.to_le_bytes()); // createdAt
    f.extend(ts.to_le_bytes()); // updatedAt
    f.extend(0xa0u8..=0xbf); // account, made up
    f.extend(ts.to_le_bytes()); // receivedAt
    f.extend(ts.to_le_bytes()); // publishedAt
    f.push(0); // stp
    f.push(u8::from(o.reduce_only)); // flags: REDUCE_ONLY is bit 0
    f.extend([0u8, 0]); // requestStatus, requestType
    assert_eq!(f.len(), 8 + 128);
    for s in [o.vid.as_str(), o.cid.as_str(), MARKET, reason, "", ""] {
        f.push(u8::try_from(s.len()).unwrap());
        f.extend(s.as_bytes());
    }
    f
}

/// A `FillEvent` (template 21) at version 1 in its 107-byte block layout: `o` made whole at its
/// price for no fee under the fill id `fill_id`.
fn fill_event(o: &Order, seq: i64, fill_id: &str) -> Vec<u8> {
    let ts = 1_759_500_000_204_011i64;
    let side = if o.side == "BUY" { 1u8 } else { 2 };
    let mut f = Vec::new();
    for v in [107u16, 21, 1, 1] {
        f.extend(v.to_le_bytes());
    }
    f.extend(ts.to_le_bytes());
    f.extend(seq.to_le_bytes());
    f.extend([1u8, side, 1]); // FILL, the side, MAKER
    f.extend(e8(&o.price).to_le_bytes());
    f.extend(e8(&o.size).to_le_bytes()); // size
    f.extend(0i64.to_le_bytes()); // fee
    f.extend(i64::MIN.to_le_bytes()); // realizedPnl: null
    f.extend(ts.to_le_bytes()); // createdAt
    f.extend(0xa0u8..=0xbf); // account, made up
    f.extend(e8(&o.price).to_le_bytes()); // underlyingPrice
    f.extend(0i64.to_le_bytes()); // realizedFunding
    assert_eq!(f.len(), 8 + 107);
    for s in [
        fill_id,
        o.vid.as_str(),
        o.cid.as_str(),
        "9615262148007719001",
        MARKET,
    ] {
        f.push(u8::try_from(s.len()).unwrap());
        f.extend(s.as_bytes());
    }
    f
}

/// The order `vid` CLOSED by `USER_CANCELED`, its frame, when the stub holds it open.
fn cancelled(book: &mut Book, vid: &str) -> Option<Frame> {
    let seq = book.seq();
    let o = book.orders.get_mut(vid).filter(|o| o.open)?;
    o.open = false;
    let o = o.clone();
    Some(Frame::Binary(order_event(
        &o,
        seq,
        4,
        &o.size,
        "USER_CANCELED",
    )))
}

/// The vid of the order `params` names: by venue id (`id`) or by client id.
fn named(book: &Book, params: &Value) -> Option<String> {
    if let Some(id) = params["id"].as_str() {
        return Some(id.to_owned());
    }
    let cid = params["client_id"].as_str()?;
    book.orders
        .values()
        .find(|o| o.cid == cid)
        .map(|o| o.vid.clone())
}

/// Answers each JSON-RPC frame by its method (module documentation).
pub fn responder(book: Shared) -> Responder {
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
        let mut book = lock(&book);
        match req["method"].as_str().unwrap_or("") {
            "auth" | "subscribe" => Ok(vec![ok(json!({}))]),
            "order.cancel_on_disconnect" => Ok(vec![ok(json!({"enabled": true}))]),
            "order.create" => {
                let fate = book
                    .fates
                    .pop_front()
                    .ok_or("an order no fate was set for")?;
                let reduce_only = params["flags"]
                    .as_array()
                    .is_some_and(|f| f.iter().any(|v| v == "REDUCE_ONLY"));
                let vid = book.vid();
                let order = Order {
                    vid: vid.clone(),
                    cid: params["client_id"].as_str().unwrap_or("").to_owned(),
                    side: if params["side"] == "SELL" {
                        "SELL"
                    } else {
                        "BUY"
                    },
                    price: params["price"].as_str().unwrap_or("").to_owned(),
                    size: params["size"].as_str().unwrap_or("").to_owned(),
                    reduce_only,
                    open: true,
                };
                let echo = json!({
                    "id": vid, "client_id": order.cid, "market": MARKET, "side": order.side,
                    "type": "LIMIT", "instruction": "POST_ONLY", "price": order.price,
                    "size": order.size, "remaining_size": order.size, "status": "NEW",
                });
                let mut frames = vec![ok(json!({ "order": echo }))];
                let seq = book.seq();
                frames.push(Frame::Binary(order_event(&order, seq, 3, &order.size, "")));
                if fate == Fate::Fill {
                    book.fill_seq += 1;
                    let fill_id = format!("86152621480077{:05}", book.fill_seq);
                    let seq = book.seq();
                    let fill = fill_event(&order, seq, &fill_id);
                    // The same fill twice: a venue that repeats one must not move the
                    // inventory twice.
                    frames.push(Frame::Binary(fill.clone()));
                    frames.push(Frame::Binary(fill));
                    let seq = book.seq();
                    frames.push(Frame::Binary(order_event(&order, seq, 4, "0", "")));
                    let size = Decimal::from_str(&order.size).unwrap();
                    book.position += if order.side == "BUY" { size } else { -size };
                    book.orders.insert(
                        vid,
                        Order {
                            open: false,
                            ..order
                        },
                    );
                } else {
                    book.orders.insert(vid, order);
                }
                Ok(frames)
            }
            "order.cancel" => {
                let vid = named(&book, params).ok_or("a cancel of no order the stub holds")?;
                let queued = ok(json!({"order_id": vid, "status": "QUEUED_FOR_CANCELLATION"}));
                let mut frames = vec![queued];
                frames.extend(cancelled(&mut book, &vid));
                Ok(frames)
            }
            "order.cancel_batch" => {
                let mut results = Vec::new();
                let mut events = Vec::new();
                let ids = params["order_ids"].as_array().cloned().unwrap_or_default();
                let cids = params["client_order_ids"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default();
                let by_cid = cids
                    .iter()
                    .filter_map(|c| named(&book, &json!({ "client_id": c })));
                let vids: Vec<String> = ids
                    .iter()
                    .filter_map(|v| v.as_str().map(str::to_owned))
                    .chain(by_cid)
                    .collect();
                for vid in vids {
                    results.push(
                        json!({"id": vid, "market": MARKET, "status": "QUEUED_FOR_CANCELLATION"}),
                    );
                    events.extend(cancelled(&mut book, &vid));
                }
                let mut frames = vec![ok(json!({ "results": results }))];
                frames.extend(events);
                Ok(frames)
            }
            other => Err(format!("an unexpected method {other}")),
        }
    })
}

/// How many frames the stub answers on each connection, in accept order: the stub closes each
/// connection but the last once it has answered that many.
pub fn script(book: &Shared, frames_per_conn: &[usize]) -> WsScript {
    let with = responder(book.clone());
    let mut steps = Vec::new();
    for (conn, &frames) in frames_per_conn.iter().enumerate() {
        steps.push(Step::Accept);
        steps.extend((0..frames).map(|_| Step::Respond {
            conn,
            with: with.clone(),
        }));
        if conn + 1 < frames_per_conn.len() {
            steps.push(Step::Close { conn });
        }
    }
    WsScript::new(steps)
}

/// The stub, running on a thread of its own until dropped.
pub struct Stub {
    stub: Option<StubServer>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<JoinHandle<()>>,
}

impl Stub {
    /// Starts the stub with `book`, answering `frames_per_conn` frames as [`script`] says.
    pub fn start(book: &Shared, frames_per_conn: &[usize]) -> Stub {
        Stub::start_with(book, script(book, frames_per_conn))
    }

    /// Starts the stub with `book`, its socket following `script`.
    pub fn start_with(book: &Shared, script: WsScript) -> Stub {
        let router = routes(book);
        let (started_tx, started) = std::sync::mpsc::channel();
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let thread = thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let stub = StubServer::start(script, router).await.unwrap();
                started_tx.send(stub).unwrap();
                // Its tasks run on this runtime until the test is done with it.
                let _ = stopped.await;
            });
        });
        let stub = started.recv().unwrap();
        Stub {
            stub: Some(stub),
            stop: Some(stop),
            thread: Some(thread),
        }
    }

    fn server(&self) -> &StubServer {
        self.stub.as_ref().unwrap()
    }

    /// `ws://127.0.0.1:<port>/v1`.
    pub fn ws_url(&self) -> String {
        self.server().ws_url("/v1")
    }

    /// `http://127.0.0.1:<port>/v1`.
    pub fn rest_url(&self) -> String {
        self.server().http_url("/v1")
    }

    /// Every connection the socket endpoint accepted.
    pub fn connections(&self) -> Vec<ConnRecord> {
        self.server().connections()
    }

    /// The JSON-RPC requests received on connection `conn`, in order.
    pub fn requests(&self, conn: usize) -> Vec<Value> {
        self.connections()
            .get(conn)
            .map(|c| {
                c.received
                    .iter()
                    .filter_map(|f| match f {
                        Frame::Text(t) => serde_json::from_str::<Value>(t).ok(),
                        Frame::Binary(_) => None,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// The methods of [`Stub::requests`].
    pub fn methods(&self, conn: usize) -> Vec<String> {
        self.requests(conn)
            .iter()
            .map(|v| v["method"].as_str().unwrap_or("").to_owned())
            .collect()
    }

    /// Every text frame received on any connection, verbatim.
    pub fn texts(&self) -> Vec<String> {
        self.connections()
            .iter()
            .flat_map(|c| c.received.iter())
            .filter_map(|f| match f {
                Frame::Text(t) => Some(t.clone()),
                Frame::Binary(_) => None,
            })
            .collect()
    }

    /// Waits until the script has played to its end or failed: its outcome.
    pub async fn finished(&self) -> Result<(), ScriptError> {
        self.server().finished().await
    }
}

impl Drop for Stub {
    fn drop(&mut self) {
        drop(self.stub.take());
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
