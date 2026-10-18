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
// testnet_trade's round trip, reused by path, holds the position check: unused here.
#[allow(dead_code)]
#[path = "../src/positions.rs"]
mod positions;
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
    /// Whether it was sent reduce-only.
    reduce_only: bool,
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
    /// The book's touch, each `(price, size)`: the fixture's best bid and best ask, the ask's
    /// size settable. An order that is not post-only and prices through the far touch takes
    /// that level (and no more), as the fill steps' orders do (FBC-6fnc).
    bid_top: (Decimal, Decimal),
    ask_top: (Decimal, Decimal),
    /// Fill ids handed out.
    next_fill: u64,
    /// Whether order.modify's size is read as the remainder of a partly filled order, not its
    /// total (FBC-b0z9).
    modify_as_remainder: bool,
    /// From the n-th order placed on, while the main socket is open, GET /positions answers a
    /// stale position (Codex r4285575623).
    stale_position_from: Option<usize>,
    /// The largest absolute position the account ever held.
    max_position: Decimal,
    /// Whether a take's frames (its fill, order event and position) are held until the next
    /// request, so its placement never sees its final phase in time (Codex r4289239739).
    hold_take: bool,
    /// Whether an order.modify of a partly filled order (no other) is answered, then the order
    /// fills its rest at its old price and closes before any event confirms the amend (Codex
    /// r4289239759).
    modify_fills: bool,
    /// Whether the take's fill event (the first IOC not reduce-only that takes) is never sent,
    /// its order event and position sent as usual (Codex r4289405166).
    drop_take_fill: bool,
    /// Whether the first reduce-only IOC's fill event is never sent (Codex r4289405169).
    drop_flatten_fill: bool,
    /// Whether the first cancel of a partly filled order finds it filled its rest first: the
    /// order event says so, no fill event is sent (Reviewer A, RB-6fnc-1).
    cancel_fills: bool,
    /// Whether a GTC order that takes in part states OPEN, nothing filled, before its fill
    /// (Reviewer B, RB-6fnc-2).
    open_before_fill: bool,
    /// Whether the GET /orderbook after the take (the first IOC not reduce-only that takes)
    /// answers 500, the reads after it the book (Codex r4289567002).
    book_fail_after_take: bool,
    /// Set by `book_fail_after_take`: the next GET /orderbook answers 500.
    book_fail_next: bool,
    /// The cancel reason the take (the first IOC not reduce-only that would take) is closed
    /// for with nothing filled: the venue's refusal after its provisional acceptance (Codex
    /// r4289566988).
    refuse_take: Option<&'static str>,
    /// Whether GET /positions answers a stale position once the order.modify of a partly
    /// filled order was received, while the main socket is open (Codex r4290085861).
    stale_position_after_modify: bool,
    /// Set once the order.modify of a partly filled order was received.
    modified: bool,
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

/// A `FillEvent` (template 21) at version 2 in its 116-byte block layout (`fill.rs`): `side`
/// (1 BUY, 2 SELL) took `size` of order `o` at `price` as taker, a fee of 0.0002 in USDC,
/// `pnl` its realized P&L (null when none).
fn fill_event(
    o: &VOrder,
    fill: u64,
    seq: i64,
    price: &str,
    size: &str,
    pnl: Option<&str>,
) -> Vec<u8> {
    let ts = 1_759_500_000_204_011i64;
    let mut f = Vec::new();
    for v in [116u16, 21, 1, 2] {
        f.extend(v.to_le_bytes());
    }
    f.extend(ts.to_le_bytes());
    f.extend(seq.to_le_bytes());
    f.extend([1u8, o.side, 2]); // FILL, our side, TAKER
    f.extend(e8(price).to_le_bytes());
    f.extend(e8(size).to_le_bytes());
    f.extend(e8("0.0002").to_le_bytes()); // fee, paid
    f.extend(pnl.map_or(i64::MIN, e8).to_le_bytes()); // realizedPnl
    f.extend(ts.to_le_bytes()); // createdAt
    f.extend([ACCOUNT_BYTE; 32]); // account
    f.extend(e8(price).to_le_bytes()); // underlyingPrice
    f.extend(0i64.to_le_bytes()); // realizedFunding
    f.push(0); // flags
    f.extend(7i64.to_le_bytes()); // orderbookSeqNo
    assert_eq!(f.len(), 8 + 116);
    let id = format!("86152621480077{fill:05}");
    let trade = format!("96152621480077{fill:05}");
    for s in [
        id.as_str(),
        o.vid.as_str(),
        o.cid.as_str(),
        &trade,
        MARKET,
        "USDC",
    ] {
        f.push(u8::try_from(s.len()).unwrap());
        f.extend(s.as_bytes());
    }
    f
}

/// A `PositionEvent` (template 22) in its 154-byte block (`account.rs`): the account's signed
/// `position` in the market, OPEN with a size and CLOSED at 0.
fn position_event(position: Decimal, seq: i64) -> Vec<u8> {
    let ts = 1_759_500_000_206_011i64;
    let mut f = Vec::new();
    for v in [154u16, 22, 1, 1] {
        f.extend(v.to_le_bytes());
    }
    f.extend(ts.to_le_bytes());
    f.extend(seq.to_le_bytes());
    f.push(if position < Decimal::ZERO { 2 } else { 1 });
    f.extend(e8(&position.abs().to_string()).to_le_bytes());
    let entry = if position.is_zero() {
        i64::MIN
    } else {
        e8("62000.5")
    };
    f.extend(entry.to_le_bytes());
    for _ in 0..6 {
        f.extend(0i64.to_le_bytes()); // P&L, mark, liquidation, leverage, updatedAt
    }
    f.extend([ACCOUNT_BYTE; 32]);
    for _ in 0..5 {
        f.extend(0i64.to_le_bytes()); // entry USD, cost, cost USD, funding index, funding
    }
    f.push(if position.is_zero() { 2 } else { 1 });
    assert_eq!(f.len(), 8 + 154);
    for s in [MARKET, "8615262148007700001"] {
        f.push(u8::try_from(s.len()).unwrap());
        f.extend(s.as_bytes());
    }
    f
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
                // A batch's params are the bare array of order requests (FBC-mu1o).
                let batch = params.as_array();
                let items: Vec<Value> = batch.cloned().unwrap_or_else(|| vec![params.clone()]);
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
                        reduce_only: p["flags"]
                            .as_array()
                            .is_some_and(|f| f.iter().any(|f| f == "REDUCE_ONLY")),
                    };
                    created.push(o);
                }
                let reply = if batch.is_none() {
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
                    let mut seq = v.next_seq();
                    let px = Decimal::from_str(&o.price).unwrap_or_default();
                    let (far_px, far_size) = if o.side == 1 { v.ask_top } else { v.bid_top };
                    let crosses = o.tif != 3
                        && if o.side == 1 {
                            px >= far_px
                        } else {
                            px <= far_px
                        };
                    let refused = (crosses && o.tif == 2 && !o.reduce_only)
                        .then(|| v.refuse_take.take())
                        .flatten();
                    if let Some(reason) = refused {
                        o.status = "CLOSED";
                        o.cancel_reason = reason.to_owned();
                        frames.push(Frame::Binary(order_event(&o, seq, 4, None)));
                    } else if crosses && !(o.tif == 2 && v.ioc_fills) {
                        if o.tif == 2
                            && !o.reduce_only
                            && std::mem::take(&mut v.book_fail_after_take)
                        {
                            v.book_fail_next = true;
                        }
                        // It takes the far level, at most: a fill event, the order event and
                        // the position.
                        let size = Decimal::from_str(&o.size).unwrap();
                        let took = size.min(far_size);
                        let left = size - took;
                        if o.tif == 1 && !left.is_zero() && std::mem::take(&mut v.open_before_fill)
                        {
                            o.status = "OPEN";
                            frames.push(Frame::Binary(order_event(&o, seq, 3, None)));
                            seq = v.next_seq();
                        }
                        v.next_fill += 1;
                        let fill = v.next_fill;
                        let fseq = v.next_seq();
                        // A fill that reduces the position realizes P&L; one that opens it none.
                        let reduces = !v.position.is_zero()
                            && ((o.side == 1) == (v.position < Decimal::ZERO));
                        let pnl = reduces.then_some("-0.00005");
                        frames.push(Frame::Binary(fill_event(
                            &o,
                            fill,
                            fseq,
                            &far_px.to_string(),
                            &took.to_string(),
                            pnl,
                        )));
                        let dropped = if o.reduce_only {
                            std::mem::take(&mut v.drop_flatten_fill)
                        } else {
                            o.tif == 2 && std::mem::take(&mut v.drop_take_fill)
                        };
                        if dropped {
                            frames.pop();
                        }
                        v.position += if o.side == 1 { took } else { -took };
                        v.max_position = v.max_position.max(v.position.abs());
                        o.open = left.to_string();
                        let status = if o.tif == 1 && !left.is_zero() {
                            o.status = "OPEN";
                            3
                        } else {
                            o.status = "CLOSED";
                            if !left.is_zero() {
                                o.cancel_reason = "SYNTHETIC_IOC_REMAINDER".to_owned();
                            }
                            4
                        };
                        let pseq = v.next_seq();
                        let events = [
                            Frame::Binary(order_event(&o, seq, status, None)),
                            Frame::Binary(position_event(v.position, pseq)),
                        ];
                        if o.tif == 2 && std::mem::take(&mut v.hold_take) {
                            // The fill event went in `frames` first: hold it with them.
                            let fill = frames.pop().unwrap();
                            v.held.push(fill);
                            v.held.extend(events);
                        } else {
                            frames.extend(events);
                        }
                    } else if o.tif == 2 && v.ioc_fills {
                        o.status = "CLOSED";
                        o.open = "0".to_owned();
                        let size = Decimal::from_str(&o.size).unwrap();
                        v.position += if o.side == 1 { size } else { -size };
                        v.next_fill += 1;
                        let (fill, fseq) = (v.next_fill, v.next_seq());
                        let (price, qty) = (o.price.clone(), o.size.clone());
                        frames.push(Frame::Binary(fill_event(
                            &o, fill, fseq, &price, &qty, None,
                        )));
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
            "order.modify"
                if v.modify_fills
                    && v.orders
                        .iter()
                        .any(|o| params["id"] == o.vid.as_str() && o.open != o.size) =>
            {
                let vid = params["id"].as_str().unwrap_or("").to_owned();
                let (seq, fseq, pseq) = (v.next_seq(), v.next_seq(), v.next_seq());
                v.next_fill += 1;
                let fill = v.next_fill;
                let o = v
                    .orders
                    .iter_mut()
                    .find(|o| o.vid == vid)
                    .ok_or("no such order")?;
                frames.push(ok(json!({"order": o.json(&account)})));
                let left = o.open.clone();
                o.open = "0".to_owned();
                o.status = "CLOSED";
                let o = o.clone();
                frames.push(Frame::Binary(fill_event(
                    &o, fill, fseq, &o.price, &left, None,
                )));
                frames.push(Frame::Binary(order_event(&o, seq, 4, None)));
                v.position += Decimal::from_str(&left).unwrap();
                frames.push(Frame::Binary(position_event(v.position, pseq)));
            }
            "order.modify" => {
                let remainder = v.modify_as_remainder;
                let vid = params["id"].as_str().unwrap_or("").to_owned();
                let seq = v.next_seq();
                let o = v
                    .orders
                    .iter_mut()
                    .find(|o| o.vid == vid)
                    .ok_or("no such order")?;
                let filled =
                    Decimal::from_str(&o.size).unwrap() - Decimal::from_str(&o.open).unwrap();
                let sent = Decimal::from_str(params["size"].as_str().unwrap_or("0")).unwrap();
                o.price = params["price"].as_str().unwrap_or("").to_owned();
                // The size sent is the total (filled part included), or what remains.
                let (size, open) = if remainder {
                    (sent + filled, sent)
                } else {
                    (sent, sent - filled)
                };
                o.size = size.to_string();
                o.open = open.to_string();
                let o = o.clone();
                // Only a partly filled order's amend (P1's) arms the stale position.
                v.modified |= !filled.is_zero();
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
                let mut cancel_fills = v.cancel_fills;
                for vid in &ids {
                    let seq = v.next_seq();
                    let o = v
                        .orders
                        .iter_mut()
                        .find(|o| &o.vid == vid)
                        .ok_or("no such order")?;
                    o.status = "CLOSED";
                    o.cancel_reason = "USER_CANCELED".to_owned();
                    let rest = (o.open != o.size && cancel_fills)
                        .then(|| Decimal::from_str(&o.open).unwrap());
                    if rest.is_some() {
                        o.open = "0".to_owned();
                        o.cancel_reason = String::new();
                    }
                    let o = o.clone();
                    if let Some(rest) = rest {
                        cancel_fills = false;
                        v.cancel_fills = false;
                        v.position += if o.side == 1 { rest } else { -rest };
                    }
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
            "order.cancel_all" => {
                // An instrument cancel-all, which fbc-oms builds under 0005's I7 guard now that
                // Paradex's resync is trustworthy (decision 0094): `{"status":"ok"}`, then every
                // order still open on the market CLOSED, each by its order event.
                if params["market"] != MARKET {
                    return Err(format!("a cancel-all not of the market: {params}"));
                }
                frames.push(ok(json!({"status": "ok"})));
                for i in 0..v.orders.len() {
                    if v.orders[i].status == "CLOSED" {
                        continue;
                    }
                    let seq = v.next_seq();
                    let o = &mut v.orders[i];
                    o.status = "CLOSED";
                    o.cancel_reason = "USER_CANCELED".to_owned();
                    let o = o.clone();
                    frames.push(Frame::Binary(order_event(&o, seq, 4, None)));
                }
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
    } else {
        let size = venue.lock().unwrap().ask_top.1;
        book["asks"][0][1] = json!(size.to_string());
    }
    let book = book.to_string();
    let (orders, history) = (Arc::clone(&venue), Arc::clone(&venue));
    let positions = Arc::clone(&venue);
    let books = Arc::clone(&venue);
    HttpRouter::new()
        .route(
            Method::POST,
            PathPattern::exact("/v1/auth"),
            reply(200, format!(r#"{{"jwt_token":"{}"}}"#, token())),
        )
        .route_fn(
            Method::GET,
            PathPattern::exact(&format!("/v1/orderbook/{MARKET}")),
            move |_| {
                if std::mem::take(&mut books.lock().unwrap().book_fail_next) {
                    return reply(500, "{}");
                }
                reply(200, book.clone())
            },
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
                let stale_from = v
                    .stale_position_from
                    .is_some_and(|n| v.orders.len() >= n && !v.main_ended())
                    || (v.stale_position_after_modify && v.modified && !v.main_ended());
                if stale_from || (v.stale_position_once && !v.orders.is_empty()) {
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
    // The stop connection: the opening six, Stop's instrument cancel-all (fbc-oms builds one
    // under 0005's I7 guard once Paradex's trustworthy resync showed the account's open
    // orders, decision 0094), and what a test adds.
    for (conn, frames) in [(0, main), (1, 7 + extra_graceful), (2, 7 + extra_stop)] {
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
        allow_fill: false,
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
        bid_top: (
            Decimal::from_str("62000.2").unwrap(),
            Decimal::from_str("0.333").unwrap(),
        ),
        ask_top: (
            Decimal::from_str("62000.5").unwrap(),
            Decimal::from_str("0.1").unwrap(),
        ),
        next_fill: 0,
        modify_as_remainder: false,
        stale_position_from: None,
        max_position: Decimal::ZERO,
        hold_take: false,
        modify_fills: false,
        drop_take_fill: false,
        drop_flatten_fill: false,
        cancel_fills: false,
        open_before_fill: false,
        book_fail_after_take: false,
        book_fail_next: false,
        refuse_take: None,
        stale_position_after_modify: false,
        modified: false,
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
    // Nothing of ours is left, but Stop's cancel everything is the instrument cancel-all
    // (0005's I7 under Paradex's trustworthy resync, decision 0094), sent all the same.
    assert_eq!(stop.len(), 7, "{stop:?}");
    assert_eq!(stop[6], "order.cancel_all", "{stop:?}");
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
    assert_eq!(json["steps"].as_array().unwrap().len(), 11);

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
        // Codex r4285575668: the order still rests with no socket once --cod-wait passed, so
        // its history and the position are read once each, never waited on again before the
        // next connection's Safety cancel.
        assert_eq!(d.history.as_ref().unwrap().reads, 1, "{name}: {d:?}");
        assert_eq!(step(name).comparisons.last().unwrap().reads, 1, "{name}");
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
        reduce_only: false,
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
            "fill" | "fill-partial" => assert!(
                matches!(&s.ran, evidence::Ran::Skipped(why) if why.contains("--allow-fill")),
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

/// A stub whose connections get `frames` client frames each, in order.
async fn stub_frames(venue: &Arc<Mutex<Venue>>, frames: &[usize]) -> StubServer {
    let mut steps = Vec::new();
    for (conn, n) in frames.iter().enumerate() {
        steps.push(Step::Accept);
        let with = responder(Arc::clone(venue), conn);
        steps.extend((0..*n).map(|_| Step::Respond {
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

/// The order.create parameters the program sent on connection `conn`.
fn creates(stub: &StubServer, conn: usize) -> Vec<Value> {
    stub.connections()[conn]
        .received
        .iter()
        .filter_map(|f| match f {
            Frame::Text(t) => serde_json::from_str::<Value>(t).ok(),
            Frame::Binary(_) => None,
        })
        .filter(|v| v["method"] == "order.create")
        .map(|v| v["params"].clone())
        .collect()
}

#[tokio::test]
async fn the_fill_step_takes_flattens_and_compares_positions_with_the_fills() {
    // FBC-6fnc (1): with --allow-fill on a two-sided book, one IOC takes the ask, GET
    // /positions is compared with the fills, a reduce-only IOC on the bid flattens it, and it
    // is compared again. The ask's level is larger than the order: no partial fill arises.
    let venue = new_venue(true, true);
    let stub = stub_frames(&venue, &[MAIN + 2, 7, 7]).await;
    let (evidence_dir, _e) = temp_dir("evidence-fill");
    let (lease_dir, _l) = temp_dir("lease-fill");
    let mut opts = options(&stub, &evidence_dir.join("run"), &lease_dir, 10);
    opts.allow_fill = true;
    let (outcome, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    let report = fs::read_to_string(&outcome.report).unwrap();
    assert!(outcome.ok, "{printed}\n{report}");

    let ev = &outcome.evidence;
    let step = |name: &str| ev.steps.iter().find(|s| s.name == name).unwrap();
    let fill = step("fill");
    assert!(fill.pass && fill.ran == evidence::Ran::Yes, "{fill:?}");
    let after: Vec<&str> = fill.comparisons.iter().map(|c| c.after.as_str()).collect();
    assert_eq!(
        after,
        [
            "fill: before the IOC",
            "fill: after the IOC",
            "fill: after the flatten"
        ]
    );
    // GET /positions followed the fills: long the take, then flat.
    let c = &fill.comparisons;
    assert_eq!(c[0].stream_position, "0");
    assert_eq!(c[1].stream_position, "0.00017");
    assert_eq!(
        c[1].rest_position,
        Some(evidence::RestPosition {
            side: "LONG".to_owned(),
            size: "0.00017".to_owned(),
            status: "OPEN".to_owned(),
        })
    );
    assert_eq!(c[2].stream_position, "0");
    assert!(
        c.iter().all(|c| c.positions_agreed && c.orders_agreed),
        "{c:?}"
    );
    // The take, then the flatten: an IOC at the ask, then a reduce-only IOC at the bid.
    let sent = creates(&stub, 0);
    let take = sent.iter().find(|p| p["price"] == "62000.5").unwrap();
    assert_eq!(
        (&take["side"], &take["instruction"]),
        (&json!("BUY"), &json!("IOC"))
    );
    assert!(take.get("flags").is_none_or(|f| f == &json!([])), "{take}");
    let flat = sent.iter().find(|p| p["price"] == "62000.2").unwrap();
    assert_eq!(
        (
            &flat["side"],
            &flat["instruction"],
            &flat["flags"],
            &flat["size"]
        ),
        (
            &json!("SELL"),
            &json!("IOC"),
            &json!(["REDUCE_ONLY"]),
            &json!("0.00017")
        )
    );
    // Never past the inventory cap ($50 at the dearer side, 78 lots).
    assert!(venue.lock().unwrap().max_position <= Decimal::from_str("0.00078").unwrap());
    assert!(venue.lock().unwrap().position.is_zero());
    // The fills and PositionEvents as received (FBC-uiy, FBC-4zz).
    assert_eq!(ev.fills, 2);
    let rows = &ev.fill_rows;
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert_eq!(
        (
            rows[0].order.as_str(),
            rows[0].side.as_str(),
            rows[0].size.as_str(),
            rows[0].price.as_str(),
            rows[0].liquidity.as_str(),
            rows[0].fee.as_str(),
            rows[0].fee_currency.as_str(),
            rows[0].realized_pnl.as_str(),
        ),
        (
            "T1 take", "Buy", "0.00017", "62000.5", "taker", "0.0002", "USDC", "-"
        )
    );
    assert!(
        rows[0].fill_id.starts_with("FILL:86152621480077"),
        "{rows:?}"
    );
    assert_eq!(
        (
            rows[1].order.as_str(),
            rows[1].side.as_str(),
            rows[1].realized_pnl.as_str()
        ),
        ("F1 flatten", "Sell", "-0.00005")
    );
    assert_eq!(ev.position_events.len(), 2, "{:?}", ev.position_events);
    assert!(ev.position_events[0].starts_with("size 0.00017"));
    assert!(ev.position_events[1].starts_with("size 0 "));
    // No smaller level: the partial amend is not exercised, and says why.
    let partial = step("fill-partial");
    assert!(
        matches!(&partial.ran, evidence::Ran::Skipped(why) if why.contains("not smaller")),
        "{partial:?}"
    );
    assert!(answer(ev, "FBC-b0z9").starts_with("not exercised"));
    assert!(
        answer(ev, "FBC-uiy").contains("T1 take"),
        "{}",
        answer(ev, "FBC-uiy")
    );
    assert!(!answer(ev, "(c)").contains("no fill event arrived"));
    assert!(report.contains("## Fills as received"), "{report}");
    let (passed, said) = privacy_check(&outcome.fixtures);
    assert!(passed, "{said}");
}

#[tokio::test]
async fn a_partly_filled_order_is_amended_and_what_the_venue_made_of_the_size_recorded() {
    // FBC-6fnc (2): the ask's level (5 lots) is smaller than the order (17): the take fills 5
    // and its rest expires; then P1, a GTC limit at the ask, fills 5 and rests 12, is amended
    // with its total sent, cancelled, and what filled flattened.
    let venue = new_venue(true, true);
    venue.lock().unwrap().ask_top.1 = Decimal::from_str("0.00005").unwrap();
    // The main phase: the opening, the steps to the IOC, T1 and F1, then P1, its amend, its
    // cancel and F2, then B3.
    let stub = stub_frames(&venue, &[MAIN + 6, 7, 7]).await;
    let (evidence_dir, _e) = temp_dir("evidence-partial");
    let (lease_dir, _l) = temp_dir("lease-partial");
    let mut opts = options(&stub, &evidence_dir.join("run"), &lease_dir, 10);
    opts.allow_fill = true;
    let (outcome, printed) = run_against(&opts).await;
    let report = fs::read_to_string(&outcome.report).unwrap();
    stub.finished()
        .await
        .unwrap_or_else(|e| panic!("{e:?}\n{printed}\n{report}"));
    assert!(outcome.ok, "{printed}\n{report}");

    let ev = &outcome.evidence;
    let step = |name: &str| ev.steps.iter().find(|s| s.name == name).unwrap();
    assert!(step("fill").pass, "{:?}", step("fill"));
    let partial = step("fill-partial");
    assert!(
        partial.pass && partial.ran == evidence::Ran::Yes,
        "{partial:?}"
    );
    let reading = ev.partial_amend.as_deref().unwrap();
    assert!(
        reading.starts_with("the total: the order event after the amend states size 0.00017"),
        "{reading}"
    );
    assert!(
        reading.contains("0.00005 filled and 0.00012 remaining"),
        "{reading}"
    );
    assert_eq!(answer(ev, "FBC-b0z9"), reading);
    let p1 = partial
        .orders
        .iter()
        .find(|o| o.label == "P1 partial")
        .unwrap();
    assert!(
        p1.states.iter().any(|s| s.starts_with("PartiallyFilled")),
        "{p1:?}"
    );
    assert!(
        p1.states.last().unwrap().starts_with("Terminal(Canceled"),
        "{p1:?}"
    );
    let sent = creates(&stub, 0);
    let p1_sent = sent
        .iter()
        .filter(|p| p["price"] == "62000.5" && p["instruction"] == "GTC")
        .count();
    assert_eq!(p1_sent, 1, "{sent:?}");
    assert!(
        methods(&stub, 0)
            .iter()
            .filter(|m| *m == "order.modify")
            .count()
            == 2
    );
    assert!(venue.lock().unwrap().position.is_zero());
    // Four fills: T1's and P1's five lots, each flattened.
    assert_eq!(ev.fill_rows.len(), 4, "{:?}", ev.fill_rows);
}

#[test]
fn what_a_venue_made_of_a_modify_size_is_read_as_the_total_the_remainder_or_neither() {
    let d = |t: &str| Decimal::from_str(t).unwrap();
    assert!(accept::modify_reading(d("0.17"), d("0.17"), d("0.05")).starts_with("the total"));
    assert!(accept::modify_reading(d("0.17"), d("0.22"), d("0.05")).starts_with("the remainder"));
    assert!(accept::modify_reading(d("0.17"), d("0.3"), d("0.05")).starts_with("neither"));
}

#[tokio::test]
async fn the_fill_steps_need_both_sides_of_the_book_and_send_nothing_that_takes_without_them() {
    let venue = new_venue(true, true);
    venue.lock().unwrap().one_sided = true;
    let stub = stub_frames(&venue, &[MAIN - 2, 7, 7]).await;
    let (evidence_dir, _e) = temp_dir("evidence-fill-one-sided");
    let (lease_dir, _l) = temp_dir("lease-fill-one-sided");
    let mut opts = options(&stub, &evidence_dir.join("run"), &lease_dir, 10);
    opts.allow_fill = true;
    let (outcome, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(outcome.ok, "{printed}");
    for name in ["fill", "fill-partial"] {
        let s = outcome
            .evidence
            .steps
            .iter()
            .find(|s| s.name == name)
            .unwrap();
        assert!(
            matches!(&s.ran, evidence::Ran::Skipped(why) if why.contains("no ask")),
            "{s:?}"
        );
    }
    assert!(
        creates(&stub, 0).iter().all(|p| p["instruction"] != "GTC"),
        "no order that is not post-only rests or takes"
    );
    assert_eq!(venue.lock().unwrap().max_position, Decimal::ZERO);
}

#[test]
fn allow_fill_is_off_unless_the_flag_is_given() {
    let parsed = |extra: &[&str]| {
        let mut argv: Vec<String> = [
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
        argv.extend(extra.iter().map(|s| (*s).to_owned()));
        match accept_args::parse(argv) {
            Ok(AcceptParsed::Run(o)) => o.allow_fill,
            other => panic!("{other:?}"),
        }
    };
    assert!(!parsed(&[]));
    assert!(parsed(&["--allow-fill"]));
}

#[tokio::test]
async fn a_disconnect_order_whose_position_never_agreed_is_not_dropped_and_fails_its_step() {
    // Codex r4285575623: GET /positions answers a stale position from B3 (the fifth order)
    // on, while the main socket is open. The step fails before the socket ends: its pass can
    // never come from the disconnect's later position-only comparison.
    let venue = new_venue(true, true);
    venue.lock().unwrap().stale_position_from = Some(5);
    // The main phase's steps and Stop's cancel of B3, then the stop phase's opening.
    let stub = stub_frames(&venue, &[MAIN + 1, 6]).await;
    let (evidence_dir, _e) = temp_dir("evidence-cod-position");
    let (lease_dir, _l) = temp_dir("lease-cod-position");
    let mut opts = options(&stub, &evidence_dir.join("run"), &lease_dir, 10);
    opts.rest_wait_secs = 1;
    let (outcome, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    let ev = &outcome.evidence;
    let cod = ev.steps.iter().find(|s| s.name == "cod-dropped").unwrap();
    assert!(!cod.pass && cod.disconnect.is_none(), "{cod:?}");
    assert!(
        cod.notes.iter().any(|n| n.contains("REST never agreed")),
        "{cod:?}"
    );
    assert!(!cod.comparisons[0].positions_agreed, "{cod:?}");
    // Closed, never dropped: Stop cancelled B3 on the open socket.
    assert!(stub.connections()[0].client_closed);
    assert!(
        methods(&stub, 0)
            .last()
            .unwrap()
            .starts_with("order.cancel")
    );
    assert!(!outcome.ok, "{printed}");
    let report = fs::read_to_string(&outcome.report).unwrap();
    assert!(!report.contains("every step passed"), "{report}");
}

#[test]
fn a_round_agrees_only_with_what_the_stream_shows_once_rest_answered() {
    // Codex r4285575635: the stream goes on applying events while REST is read, so a round
    // is judged against what it shows once both answers are in, and a stream that changed
    // during the round makes it no agreement.
    let at = std::time::Instant::now();
    let want = |b1: Option<(Option<Decimal>, Decimal)>, lots: i64| accept::Want {
        expected: vec![("B1".to_owned(), "cid-1".to_owned(), b1)],
        lots,
        since: at,
        changes: 0,
    };
    let d = |t: &str| Decimal::from_str(t).unwrap();
    let resting = want(Some((Some(d("60140.1")), d("0.00017"))), 0);
    let ended = want(None, 0);
    let row = json!({
        "client_id": "cid-1", "market": MARKET, "status": "OPEN", "price": "60140.1",
        "size": "0.00017", "remaining_size": "0.00017",
    });
    let rows = [&row];
    let step = d("0.00001");
    // REST lists B1 as the round began; the stream ended it while REST was read.
    let j = accept::judge_round(
        &resting,
        &ended,
        Some((&rows, false)),
        Some(None),
        step,
        false,
    );
    assert!(!j.orders && !j.positions, "{j:?}");
    assert!(
        j.mismatches
            .iter()
            .any(|m| m.contains("ended on the stream, still listed")),
        "{j:?}"
    );
    assert!(
        j.mismatches.iter().any(|m| m.contains("stream changed")),
        "{j:?}"
    );
    // A fill the stream applied during the round: the flat REST answer is no agreement.
    let filled = want(Some((Some(d("60140.1")), d("0.00017"))), 17);
    let j = accept::judge_round(
        &resting,
        &filled,
        Some((&rows, false)),
        Some(None),
        step,
        false,
    );
    assert!(!j.positions, "{j:?}");
    // A stream that did not move, and REST that matches it: agreement.
    let j = accept::judge_round(
        &resting,
        &resting,
        Some((&rows, false)),
        Some(None),
        step,
        false,
    );
    assert!(j.orders && j.positions && j.mismatches.is_empty(), "{j:?}");
}

#[tokio::test(start_paused = true)]
async fn an_order_book_read_takes_no_longer_than_the_step_timeout_with_the_limiter_wait() {
    // Codex r4285575649: a limiter wait that eats most of --step-timeout leaves the answer
    // only what remains, never another full timeout. A listener that never answers; the
    // limiter's one request per 10 s already spent; a step timeout of 12 s.
    use fbc_core::{LimitScope, OpKind, RateLimit, TagSet};
    use fbc_runtime::{RateLimiter, SafetyReserve};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let held = tokio::spawn(async move {
        let mut open = Vec::new();
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            open.push(socket);
        }
    });
    let limiter = RateLimiter::new(
        &[RateLimit {
            scope: LimitScope::Ip,
            ops: TagSet::of(&[OpKind::Rest]),
            per: Duration::from_secs(10),
            units: 1,
        }],
        SafetyReserve::percent(0).unwrap(),
    )
    .unwrap();
    let start = tokio::time::Instant::now();
    auth_rest::admit(
        &limiter,
        OpKind::Rest,
        (start + Duration::from_secs(1)).into_std(),
        "spent",
    )
    .await
    .unwrap();
    let (lease_dir, _l) = temp_dir("lease-touch-deadline");
    let opts = Options {
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
        step_timeout_secs: 12,
        rest_url: format!("http://127.0.0.1:{port}/v1"),
        ws_url: format!("ws://127.0.0.1:{port}/v1"),
        proxy: ProxyConfig::Direct,
        lease_dir,
        sole_trader: true,
    };
    let specs = trade::specs(
        &fbc_venue_paradex::ParadexFactory,
        &trade::config(&opts),
        &opts,
    )
    .unwrap();
    let why = accept::touch(&opts, &specs, &limiter).await.unwrap_err();
    assert!(why.contains("no answer in time"), "{why}");
    // The limiter's wait (10 s) and the answer's together within the 12 s.
    assert_eq!(start.elapsed(), Duration::from_secs(12));
    held.abort();
}

#[tokio::test]
async fn an_order_book_off_the_tick_or_step_grid_names_the_flags_to_check() {
    // FBC-6fnc (4): ETH run with BTC's grid read as Malformed("bids: size").
    for (tick, step, flag) in [("0.1", "0.01", "--step"), ("1", "0.00001", "--tick")] {
        let venue = new_venue(true, true);
        let stub = stub_frames(&venue, &[]).await;
        let (evidence_dir, _e) = temp_dir("evidence-grid");
        let (lease_dir, _l) = temp_dir("lease-grid");
        let mut opts = options(&stub, &evidence_dir.join("run"), &lease_dir, 10);
        opts.trade.tick = Decimal::from_str(tick).unwrap();
        opts.trade.step = Decimal::from_str(step).unwrap();
        let buf = Rc::new(RefCell::new(Vec::<u8>::new()));
        let out: testnet::Out = buf.clone();
        let creds = secrets;
        let why = accept::run(&opts, None, &creds, out).await.unwrap_err();
        assert!(
            why.contains(&format!("not on the {flag} grid"))
                && why.contains("malformed")
                && why.contains("GET /markets"),
            "{why}"
        );
        assert!(!why.contains("Malformed"), "{why}");
        assert!(stub.connections().is_empty());
    }
}

#[tokio::test]
async fn an_unresolved_take_is_settled_before_it_is_flattened_and_its_step_fails() {
    // Codex r4289239739: the take's events come only after the next request, so its placement
    // gets no final phase in time. The step fails, the take is cancelled and must end with its
    // fill on the stream before anything is called flat; what it filled is then flattened.
    let venue = new_venue(true, true);
    venue.lock().unwrap().hold_take = true;
    // To the IOC, then T1, its cancel and F1, then B3.
    let stub = stub_frames(&venue, &[MAIN + 3, 7, 7]).await;
    let (evidence_dir, _e) = temp_dir("evidence-take-unresolved");
    let (lease_dir, _l) = temp_dir("lease-take-unresolved");
    let mut opts = options(&stub, &evidence_dir.join("run"), &lease_dir, 10);
    opts.allow_fill = true;
    opts.trade.step_timeout_secs = 1;
    opts.rest_wait_secs = 1;
    let (outcome, printed) = run_against(&opts).await;
    let report = fs::read_to_string(&outcome.report).unwrap();
    stub.finished()
        .await
        .unwrap_or_else(|e| panic!("{e:?}\n{printed}\n{report}"));
    let fill = outcome
        .evidence
        .steps
        .iter()
        .find(|s| s.name == "fill")
        .unwrap();
    assert!(!fill.pass, "{fill:?}");
    let main = methods(&stub, 0);
    let t1 = main.iter().position(|m| m == "order.create").unwrap();
    assert!(
        main.iter().skip(t1).any(|m| m == "order.cancel"),
        "{main:?}"
    );
    assert!(
        creates(&stub, 0)
            .iter()
            .any(|p| p["flags"] == json!(["REDUCE_ONLY"])),
        "the take's fill was flattened: {printed}"
    );
    assert!(venue.lock().unwrap().position.is_zero(), "{printed}");
    assert!(!outcome.ok, "{printed}");
}

#[tokio::test]
async fn a_take_that_would_reduce_a_seeded_position_is_skipped() {
    // Codex r4289239755: a short the resync seeded, and --side buy: a reduce-only flatten
    // could not restore it, so no take is sent.
    let venue = new_venue(true, true);
    {
        let mut v = venue.lock().unwrap();
        v.position = Decimal::from_str("-0.0001").unwrap();
        // A level smaller than the order: fill-partial too gets as far as the position.
        v.ask_top.1 = Decimal::from_str("0.00005").unwrap();
    }
    let stub = stub_frames(&venue, &[MAIN, 7, 7]).await;
    let (evidence_dir, _e) = temp_dir("evidence-take-reduces");
    let (lease_dir, _l) = temp_dir("lease-take-reduces");
    let mut opts = options(&stub, &evidence_dir.join("run"), &lease_dir, 10);
    opts.allow_fill = true;
    let (outcome, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    for name in ["fill", "fill-partial"] {
        let s = outcome
            .evidence
            .steps
            .iter()
            .find(|s| s.name == name)
            .unwrap();
        assert!(
            matches!(&s.ran, evidence::Ran::Skipped(why) if why.contains("would reduce")),
            "{s:?}"
        );
    }
    assert_eq!(
        venue.lock().unwrap().position,
        Decimal::from_str("-0.0001").unwrap()
    );
    assert!(outcome.ok, "{printed}");
}

#[tokio::test]
async fn a_partial_amend_whose_remainder_reading_could_pass_the_cap_is_not_sent_and_not_passed() {
    // Codex r4289239752 and r4289239743: a long of 60 lots the resync seeded, a cap of 78:
    // P1 (17) fits, but once it filled 5, its amend read as the remainder could hold
    // 65 + 17. No amend is sent, and the step is skipped as not exercised, never passed.
    let venue = new_venue(true, true);
    {
        let mut v = venue.lock().unwrap();
        v.position = Decimal::from_str("0.0006").unwrap();
        v.ask_top.1 = Decimal::from_str("0.00005").unwrap();
    }
    // To the IOC, T1 and F1, P1, its cancel and F2, then B3.
    let stub = stub_frames(&venue, &[MAIN + 5, 7, 7]).await;
    let (evidence_dir, _e) = temp_dir("evidence-amend-cap");
    let (lease_dir, _l) = temp_dir("lease-amend-cap");
    let mut opts = options(&stub, &evidence_dir.join("run"), &lease_dir, 10);
    opts.allow_fill = true;
    let (outcome, printed) = run_against(&opts).await;
    let report = fs::read_to_string(&outcome.report).unwrap();
    stub.finished()
        .await
        .unwrap_or_else(|e| panic!("{e:?}\n{printed}\n{report}"));
    let ev = &outcome.evidence;
    let partial = ev.steps.iter().find(|s| s.name == "fill-partial").unwrap();
    assert!(
        matches!(&partial.ran, evidence::Ran::Skipped(why) if why.contains("past the caps")),
        "{partial:?}"
    );
    assert!(!partial.pass);
    assert_eq!(
        methods(&stub, 0)
            .iter()
            .filter(|m| *m == "order.modify")
            .count(),
        1,
        "B1's amend only"
    );
    assert_eq!(
        venue.lock().unwrap().position,
        Decimal::from_str("0.0006").unwrap()
    );
    assert!(outcome.ok, "{printed}");
}

#[tokio::test]
async fn an_amend_whose_order_ends_at_its_old_price_is_not_read_as_a_modify() {
    // Codex r4289239759: P1's amend is answered, then P1 fills its rest at its old price and
    // closes before any event confirms the amend: that event says nothing of order.modify.
    let venue = new_venue(true, true);
    {
        let mut v = venue.lock().unwrap();
        v.ask_top.1 = Decimal::from_str("0.00005").unwrap();
        v.modify_fills = true;
    }
    // To the IOC, T1 and F1, P1, its amend and F2 (P1 closed: no cancel), then B3.
    let stub = stub_frames(&venue, &[MAIN + 5, 7, 7]).await;
    let (evidence_dir, _e) = temp_dir("evidence-amend-old-price");
    let (lease_dir, _l) = temp_dir("lease-amend-old-price");
    let mut opts = options(&stub, &evidence_dir.join("run"), &lease_dir, 10);
    opts.allow_fill = true;
    opts.trade.step_timeout_secs = 1;
    opts.rest_wait_secs = 1;
    let (outcome, printed) = run_against(&opts).await;
    let report = fs::read_to_string(&outcome.report).unwrap();
    stub.finished()
        .await
        .unwrap_or_else(|e| panic!("{e:?}\n{printed}\n{report}"));
    let ev = &outcome.evidence;
    let partial = ev.steps.iter().find(|s| s.name == "fill-partial").unwrap();
    assert!(!partial.pass, "{partial:?}");
    let reading = ev.partial_amend.as_deref().unwrap();
    assert!(reading.starts_with("not read"), "{reading}");
    assert!(venue.lock().unwrap().position.is_zero(), "{printed}");
    assert!(!outcome.ok, "{printed}");
}

#[tokio::test]
async fn a_take_whose_fill_never_arrives_is_not_flattened_and_stops_the_phase() {
    // Codex r4289405166, Reviewer B RB-6fnc-1: T1's order event and position come, its fill
    // never does. Its inventory is unknown, so nothing is flattened from it, the step fails
    // with the line to flatten by hand, and the phase stops (no B3).
    let venue = new_venue(true, true);
    venue.lock().unwrap().drop_take_fill = true;
    // To the IOC, then T1 alone; the phase stops, so the stop phase follows (no graceful).
    let stub = stub_frames(&venue, &[MAIN, 6]).await;
    let (evidence_dir, _e) = temp_dir("evidence-take-fill-lost");
    let (lease_dir, _l) = temp_dir("lease-take-fill-lost");
    let mut opts = options(&stub, &evidence_dir.join("run"), &lease_dir, 10);
    opts.allow_fill = true;
    opts.trade.step_timeout_secs = 1;
    opts.rest_wait_secs = 1;
    let (outcome, printed) = run_against(&opts).await;
    let report = fs::read_to_string(&outcome.report).unwrap();
    stub.finished()
        .await
        .unwrap_or_else(|e| panic!("{e:?}\n{printed}\n{report}"));
    let fill = outcome
        .evidence
        .steps
        .iter()
        .find(|s| s.name == "fill")
        .unwrap();
    assert!(!fill.pass && fill.ran == evidence::Ran::Yes, "{fill:?}");
    assert!(
        printed.contains("FAIL fill T1's outcome is unresolved")
            && printed.contains("flatten by hand"),
        "{printed}"
    );
    let sent = creates(&stub, 0);
    assert!(
        sent.iter().all(|p| p["flags"] != json!(["REDUCE_ONLY"])),
        "{sent:?}"
    );
    assert_eq!(
        sent.last().map(|p| &p["price"]),
        Some(&json!("62000.5")),
        "nothing after T1: {sent:?}"
    );
    assert_eq!(
        venue.lock().unwrap().position,
        Decimal::from_str("0.00017").unwrap()
    );
    assert!(!outcome.ok, "{printed}");
}

#[tokio::test]
async fn a_flatten_whose_fill_never_arrives_is_not_retried_and_stops_the_phase() {
    // Codex r4289405169: F1's order event comes, its fill never does. No second IOC is sized
    // from the stale inventory; the step fails with the line to flatten by hand, and the
    // phase stops.
    let venue = new_venue(true, true);
    venue.lock().unwrap().drop_flatten_fill = true;
    // To the IOC, T1 and F1.
    let stub = stub_frames(&venue, &[MAIN + 1, 6]).await;
    let (evidence_dir, _e) = temp_dir("evidence-flatten-fill-lost");
    let (lease_dir, _l) = temp_dir("lease-flatten-fill-lost");
    let mut opts = options(&stub, &evidence_dir.join("run"), &lease_dir, 10);
    opts.allow_fill = true;
    opts.trade.step_timeout_secs = 1;
    opts.rest_wait_secs = 1;
    let (outcome, printed) = run_against(&opts).await;
    let report = fs::read_to_string(&outcome.report).unwrap();
    stub.finished()
        .await
        .unwrap_or_else(|e| panic!("{e:?}\n{printed}\n{report}"));
    let fill = outcome
        .evidence
        .steps
        .iter()
        .find(|s| s.name == "fill")
        .unwrap();
    assert!(!fill.pass, "{fill:?}");
    assert!(
        printed.contains("F1 flatten's outcome is unresolved"),
        "{printed}"
    );
    let flattens = creates(&stub, 0)
        .iter()
        .filter(|p| p["flags"] == json!(["REDUCE_ONLY"]))
        .count();
    assert_eq!(flattens, 1, "{printed}");
    assert!(venue.lock().unwrap().position.is_zero(), "{printed}");
    assert!(!outcome.ok, "{printed}");
}

#[tokio::test]
async fn a_partial_order_whose_last_fill_trails_its_cancel_is_not_flattened() {
    // Reviewer A, RB-6fnc-1: P1's cancel finds it filled its rest; the order event says so
    // and its fill never comes. What P1 filled is unknown, so nothing is flattened from it:
    // the step fails with the line to flatten by hand and the phase stops (no F2, no B3).
    let venue = new_venue(true, true);
    {
        let mut v = venue.lock().unwrap();
        v.ask_top.1 = Decimal::from_str("0.00005").unwrap();
        v.cancel_fills = true;
    }
    // To the IOC, T1 and F1, P1, its amend and its cancel.
    let stub = stub_frames(&venue, &[MAIN + 4, 6]).await;
    let (evidence_dir, _e) = temp_dir("evidence-partial-fill-trails");
    let (lease_dir, _l) = temp_dir("lease-partial-fill-trails");
    let mut opts = options(&stub, &evidence_dir.join("run"), &lease_dir, 10);
    opts.allow_fill = true;
    opts.trade.step_timeout_secs = 1;
    opts.rest_wait_secs = 1;
    let (outcome, printed) = run_against(&opts).await;
    let report = fs::read_to_string(&outcome.report).unwrap();
    stub.finished()
        .await
        .unwrap_or_else(|e| panic!("{e:?}\n{printed}\n{report}"));
    let partial = outcome
        .evidence
        .steps
        .iter()
        .find(|s| s.name == "fill-partial")
        .unwrap();
    assert!(!partial.pass, "{partial:?}");
    assert!(
        printed.contains("P1's outcome is unresolved") && printed.contains("flatten by hand"),
        "{printed}"
    );
    let flattens = creates(&stub, 0)
        .iter()
        .filter(|p| p["flags"] == json!(["REDUCE_ONLY"]))
        .count();
    assert_eq!(flattens, 1, "F1 only: {printed}");
    assert_eq!(
        venue.lock().unwrap().position,
        Decimal::from_str("0.00017").unwrap()
    );
    assert!(!outcome.ok, "{printed}");
}

#[tokio::test]
async fn a_partial_order_stating_open_before_its_fill_is_still_amended() {
    // Reviewer B, RB-6fnc-2: P1's first order event states it OPEN with nothing filled, then
    // its fill and the partly filled event come. The step waits for the fill, so the amend
    // is exercised, not skipped.
    let venue = new_venue(true, true);
    {
        let mut v = venue.lock().unwrap();
        v.ask_top.1 = Decimal::from_str("0.00005").unwrap();
        v.open_before_fill = true;
    }
    let stub = stub_frames(&venue, &[MAIN + 6, 7, 7]).await;
    let (evidence_dir, _e) = temp_dir("evidence-open-before-fill");
    let (lease_dir, _l) = temp_dir("lease-open-before-fill");
    let mut opts = options(&stub, &evidence_dir.join("run"), &lease_dir, 10);
    opts.allow_fill = true;
    let (outcome, printed) = run_against(&opts).await;
    let report = fs::read_to_string(&outcome.report).unwrap();
    stub.finished()
        .await
        .unwrap_or_else(|e| panic!("{e:?}\n{printed}\n{report}"));
    assert!(outcome.ok, "{printed}\n{report}");
    let ev = &outcome.evidence;
    let partial = ev.steps.iter().find(|s| s.name == "fill-partial").unwrap();
    assert!(
        partial.pass && partial.ran == evidence::Ran::Yes,
        "{partial:?}"
    );
    assert!(
        ev.partial_amend
            .as_deref()
            .unwrap()
            .starts_with("the total"),
        "{:?}",
        ev.partial_amend
    );
    assert!(venue.lock().unwrap().position.is_zero());
}

#[test]
fn a_round_whose_stream_moved_and_came_back_is_no_agreement() {
    // Codex r4289405172: two fills that return the position to where it was while REST is
    // read still change the stream: the round is no agreement.
    let at = std::time::Instant::now();
    let want = |changes: u64| accept::Want {
        expected: Vec::new(),
        lots: 0,
        since: at,
        changes,
    };
    let step = Decimal::from_str("0.00001").unwrap();
    let still = accept::judge_round(
        &want(4),
        &want(4),
        Some((&[], false)),
        Some(None),
        step,
        true,
    );
    assert!(still.orders && still.positions, "{still:?}");
    let moved = accept::judge_round(
        &want(4),
        &want(6),
        Some((&[], false)),
        Some(None),
        step,
        true,
    );
    assert!(!moved.orders && !moved.positions, "{moved:?}");
    assert!(
        moved
            .mismatches
            .iter()
            .any(|m| m.contains("stream changed")),
        "{moved:?}"
    );
}

#[test]
fn a_risk_refusal_is_answered_from_the_fill_steps_own_ioc_placements() {
    // Codex r4289405174, r4289566988: 0092 (2) is answered from the refusals the fill steps
    // recorded of their own IOC placements: never from a rejected amend or cancel, a refused
    // GTC order, or a NOT_ENOUGH_MARGIN refusal (its own case), and never from whether the
    // steps ran.
    let mut ev = evidence::Evidence {
        allow_fill: true,
        fill_iocs: 3,
        ..evidence::Evidence::default()
    };
    let mut fill = evidence::Step::new("fill");
    fill.ran = evidence::Ran::Yes;
    let rejected = "Rejected(Reject { kind: Risk, venue_code: None, raw: <12 bytes> })";
    for (kind, rpc) in [("amend", 9), ("cancel", 10)] {
        fill.sent.push(evidence::Sent {
            kind: kind.to_owned(),
            rpc,
            items: 1,
            outcomes: vec![rejected.to_owned()],
        });
    }
    fill.refusals.push(evidence::Refusal {
        order: "P1 partial".to_owned(),
        rpc: 11,
        immediate: false,
        margin: false,
        shown: "Other: SOME_REASON".to_owned(),
    });
    ev.steps.push(fill);
    let q = "0092 (2)";
    let a = answer(&ev, q);
    assert!(a.starts_with("not observed"), "{a}");
    ev.steps[0].refusals.push(evidence::Refusal {
        order: "T1 take".to_owned(),
        rpc: 12,
        immediate: true,
        margin: true,
        shown: "Other: NOT_ENOUGH_MARGIN".to_owned(),
    });
    let a = answer(&ev, q);
    assert!(
        a.starts_with("not observed")
            && a.contains("T1 take (request 12): Other: NOT_ENOUGH_MARGIN"),
        "{a}"
    );
    ev.steps[0].refusals.push(evidence::Refusal {
        order: "F1 flatten".to_owned(),
        rpc: 13,
        immediate: true,
        margin: false,
        shown: "Risk: <0 bytes>".to_owned(),
    });
    let a = answer(&ev, q);
    assert!(
        a.starts_with("observed")
            && a.contains("fill F1 flatten (request 13): Risk")
            && !a.contains("P1 partial")
            && !a.contains("request 9")
            && !a.contains("request 10"),
        "{a}"
    );
}

#[tokio::test]
async fn a_take_refused_for_want_of_margin_is_recorded_as_its_own_case_not_a_risk_refusal() {
    // Codex r4289566988: T1 is accepted provisionally, then closed NOT_ENOUGH_MARGIN with
    // nothing filled. The fill step records the refusal as one for want of margin, and 0092
    // (2), which excludes that reason, is not observed.
    let venue = new_venue(true, true);
    venue.lock().unwrap().refuse_take = Some("NOT_ENOUGH_MARGIN");
    let stub = stub_frames(&venue, &[MAIN + 1, 7, 7]).await;
    let (evidence_dir, _e) = temp_dir("evidence-take-margin");
    let (lease_dir, _l) = temp_dir("lease-take-margin");
    let mut opts = options(&stub, &evidence_dir.join("run"), &lease_dir, 10);
    opts.allow_fill = true;
    let (outcome, printed) = run_against(&opts).await;
    let report = fs::read_to_string(&outcome.report).unwrap();
    stub.finished()
        .await
        .unwrap_or_else(|e| panic!("{e:?}\n{printed}\n{report}"));
    let ev = &outcome.evidence;
    let fill = ev.steps.iter().find(|s| s.name == "fill").unwrap();
    assert!(!fill.pass, "{fill:?}");
    assert_eq!(fill.refusals.len(), 1, "{fill:?}");
    let r = &fill.refusals[0];
    assert!(
        r.order == "T1 take" && r.immediate && r.margin && r.shown.contains("NOT_ENOUGH_MARGIN"),
        "{r:?}"
    );
    let a = answer(ev, "0092 (2)");
    assert!(
        a.starts_with("not observed") && a.contains("NOT_ENOUGH_MARGIN"),
        "{a}"
    );
    assert!(venue.lock().unwrap().position.is_zero(), "{printed}");
}

#[tokio::test]
async fn a_take_is_not_sent_when_rest_never_agreed_on_the_position_before_it() {
    // Codex r4289567009: GET /positions answers a position the stream does not show from the
    // IOC step (the fourth order) on. The comparison before the take never agrees, so no take
    // is sent: the fill step fails and the phase stops.
    let venue = new_venue(true, true);
    venue.lock().unwrap().stale_position_from = Some(4);
    // To the IOC; nothing after it.
    let stub = stub_frames(&venue, &[MAIN - 1, 6]).await;
    let (evidence_dir, _e) = temp_dir("evidence-take-unagreed");
    let (lease_dir, _l) = temp_dir("lease-take-unagreed");
    let mut opts = options(&stub, &evidence_dir.join("run"), &lease_dir, 10);
    opts.allow_fill = true;
    opts.rest_wait_secs = 1;
    let (outcome, printed) = run_against(&opts).await;
    let report = fs::read_to_string(&outcome.report).unwrap();
    stub.finished()
        .await
        .unwrap_or_else(|e| panic!("{e:?}\n{printed}\n{report}"));
    let fill = outcome
        .evidence
        .steps
        .iter()
        .find(|s| s.name == "fill")
        .unwrap();
    assert!(!fill.pass && fill.ran == evidence::Ran::Yes, "{fill:?}");
    assert!(
        printed.contains("FAIL fill") && printed.contains("before the IOC"),
        "{printed}"
    );
    assert!(
        creates(&stub, 0).iter().all(|p| p["price"] != "62000.5"),
        "no take: {printed}"
    );
    let cod = outcome
        .evidence
        .steps
        .iter()
        .find(|s| s.name == "cod-dropped")
        .unwrap();
    assert_eq!(cod.ran, evidence::Ran::NotRun, "{cod:?}");
    assert!(venue.lock().unwrap().position.is_zero());
    assert!(!outcome.ok, "{printed}");
}

#[tokio::test]
async fn a_position_rest_disagrees_on_after_the_flatten_stops_the_phase() {
    // Codex r4289567009: GET /positions answers T1's position from T1 (the fifth order) on,
    // after the flatten too, while the stream is flat. The phase stops there: no further
    // order (no P1, no B3) is sent onto an exposure REST shows and the stream does not.
    let venue = new_venue(true, true);
    venue.lock().unwrap().stale_position_from = Some(5);
    // To the IOC, T1 and F1.
    let stub = stub_frames(&venue, &[MAIN + 1, 6]).await;
    let (evidence_dir, _e) = temp_dir("evidence-flatten-unagreed");
    let (lease_dir, _l) = temp_dir("lease-flatten-unagreed");
    let mut opts = options(&stub, &evidence_dir.join("run"), &lease_dir, 10);
    opts.allow_fill = true;
    opts.rest_wait_secs = 1;
    let (outcome, printed) = run_against(&opts).await;
    let report = fs::read_to_string(&outcome.report).unwrap();
    stub.finished()
        .await
        .unwrap_or_else(|e| panic!("{e:?}\n{printed}\n{report}"));
    let step = |name: &str| {
        outcome
            .evidence
            .steps
            .iter()
            .find(|s| s.name == name)
            .unwrap()
            .clone()
    };
    let fill = step("fill");
    assert!(!fill.pass, "{fill:?}");
    assert!(
        printed.contains("FAIL fill") && printed.contains("after the flatten"),
        "{printed}"
    );
    assert_eq!(step("fill-partial").ran, evidence::Ran::NotRun);
    assert_eq!(step("cod-dropped").ran, evidence::Ran::NotRun);
    assert!(venue.lock().unwrap().position.is_zero());
    assert!(!outcome.ok, "{printed}");
}

#[tokio::test]
async fn a_failed_book_read_during_the_flatten_fails_the_step_though_a_later_read_flattened() {
    // Codex r4289567002: the flatten's first GET /orderbook answers 500, the second the book:
    // F2 flattens what T1 took, and the step still fails for the read that failed.
    let venue = new_venue(true, true);
    venue.lock().unwrap().book_fail_after_take = true;
    let stub = stub_frames(&venue, &[MAIN + 2, 7, 7]).await;
    let (evidence_dir, _e) = temp_dir("evidence-flatten-book-fails");
    let (lease_dir, _l) = temp_dir("lease-flatten-book-fails");
    let mut opts = options(&stub, &evidence_dir.join("run"), &lease_dir, 10);
    opts.allow_fill = true;
    let (outcome, printed) = run_against(&opts).await;
    let report = fs::read_to_string(&outcome.report).unwrap();
    stub.finished()
        .await
        .unwrap_or_else(|e| panic!("{e:?}\n{printed}\n{report}"));
    let fill = outcome
        .evidence
        .steps
        .iter()
        .find(|s| s.name == "fill")
        .unwrap();
    assert!(!fill.pass && fill.ran == evidence::Ran::Yes, "{fill:?}");
    assert!(
        fill.orders.iter().any(|t| t.label == "F2 flatten"),
        "{fill:?}"
    );
    assert!(venue.lock().unwrap().position.is_zero(), "{printed}");
    assert!(!outcome.ok, "{printed}");
    assert!(!report.contains("every step passed"), "{report}");
}

#[test]
fn a_take_past_an_i64_bound_is_refused_by_the_cap_check_not_wrapped() {
    // Codex r4289405179: the cap check adds the take to the position checked.
    let cap = fbc_core::Lots::new(250).unwrap();
    let qty = fbc_core::Lots::new(5).unwrap();
    let near = fbc_core::SignedLots(i64::MAX - 1);
    assert!(accept::past_cap(near, OrderSide::Buy, qty, cap).is_some());
    let flat = fbc_core::SignedLots(0);
    assert!(accept::past_cap(flat, OrderSide::Buy, qty, cap).is_none());
    let at_cap = fbc_core::SignedLots(-250);
    assert!(accept::past_cap(at_cap, OrderSide::Sell, qty, cap).is_some());
}

#[test]
fn a_partial_amend_whose_worst_case_is_past_an_i64_bound_is_past_the_caps_not_wrapped() {
    // Codex r4289888345: the amend's worst case adds the total sent to the position checked.
    let lots = |n: i64| fbc_core::Lots::new(n).unwrap();
    let near = fbc_core::SignedLots(i64::MAX - 1);
    assert!(accept::amend_past_caps(
        near,
        OrderSide::Buy,
        lots(5),
        lots(250),
        lots(250)
    ));
    let low = fbc_core::SignedLots(i64::MIN + 1);
    assert!(accept::amend_past_caps(
        low,
        OrderSide::Sell,
        lots(5),
        lots(250),
        lots(250)
    ));
    let flat = fbc_core::SignedLots(5);
    assert!(!accept::amend_past_caps(
        flat,
        OrderSide::Buy,
        lots(17),
        lots(20),
        lots(250)
    ));
    assert!(accept::amend_past_caps(
        flat,
        OrderSide::Buy,
        lots(21),
        lots(20),
        lots(250)
    ));
    assert!(accept::amend_past_caps(
        fbc_core::SignedLots(240),
        OrderSide::Buy,
        lots(17),
        lots(20),
        lots(250)
    ));
}

#[tokio::test]
async fn a_partly_filled_order_rest_disagrees_on_is_not_amended_and_stops_the_phase() {
    // Codex r4289888356: GET /positions answers a position the stream does not show from P1
    // (the seventh order) on. The comparison while P1 is partly filled never agrees: P1 is not
    // amended, it is cancelled, nothing is flattened from it, and the phase stops.
    let venue = new_venue(true, true);
    {
        let mut v = venue.lock().unwrap();
        v.ask_top.1 = Decimal::from_str("0.00005").unwrap();
        v.stale_position_from = Some(7);
    }
    // To the IOC, T1 and F1, P1 and its cancel.
    let stub = stub_frames(&venue, &[MAIN + 3, 6]).await;
    let (evidence_dir, _e) = temp_dir("evidence-partial-unagreed");
    let (lease_dir, _l) = temp_dir("lease-partial-unagreed");
    let mut opts = options(&stub, &evidence_dir.join("run"), &lease_dir, 10);
    opts.allow_fill = true;
    opts.rest_wait_secs = 1;
    let (outcome, printed) = run_against(&opts).await;
    let report = fs::read_to_string(&outcome.report).unwrap();
    stub.finished()
        .await
        .unwrap_or_else(|e| panic!("{e:?}\n{printed}\n{report}"));
    let partial = outcome
        .evidence
        .steps
        .iter()
        .find(|s| s.name == "fill-partial")
        .unwrap();
    assert!(
        !partial.pass && partial.ran == evidence::Ran::Yes,
        "{partial:?}"
    );
    assert!(
        printed.contains("FAIL fill-partial") && printed.contains("flatten by hand"),
        "{printed}"
    );
    let main = methods(&stub, 0);
    assert_eq!(
        main.iter().filter(|m| *m == "order.modify").count(),
        1,
        "the amend step's alone: {main:?}"
    );
    // P1's cancel, then the phase's Stop: the instrument cancel-all (0005's I7 under
    // Paradex's trustworthy resync, decision 0094).
    assert_eq!(
        main[main.len() - 2..],
        ["order.cancel", "order.cancel_all"],
        "{main:?}"
    );
    assert_eq!(
        venue.lock().unwrap().position,
        Decimal::from_str("0.00005").unwrap()
    );
    assert!(!outcome.ok, "{printed}");
}

#[tokio::test]
async fn a_recovered_take_rest_disagrees_on_after_its_flatten_stops_the_phase() {
    // Codex r4290028707, Reviewer B RB-6fnc-5: T1's placement gets no final phase in time
    // (its events held), so the step fails; T1 is cancelled, ends with its fill and is
    // flattened. GET /positions answers a position the stream does not show from T1 (the
    // fifth order) on: the phase stops there, no P1 and no B3 sent.
    let venue = new_venue(true, true);
    {
        let mut v = venue.lock().unwrap();
        v.hold_take = true;
        v.stale_position_from = Some(5);
    }
    // To the IOC, then T1, its cancel and F1.
    let stub = stub_frames(&venue, &[MAIN + 2, 6]).await;
    let (evidence_dir, _e) = temp_dir("evidence-recovered-unagreed");
    let (lease_dir, _l) = temp_dir("lease-recovered-unagreed");
    let mut opts = options(&stub, &evidence_dir.join("run"), &lease_dir, 10);
    opts.allow_fill = true;
    opts.trade.step_timeout_secs = 1;
    opts.rest_wait_secs = 1;
    let (outcome, printed) = run_against(&opts).await;
    let report = fs::read_to_string(&outcome.report).unwrap();
    stub.finished()
        .await
        .unwrap_or_else(|e| panic!("{e:?}\n{printed}\n{report}"));
    let step = |name: &str| {
        outcome
            .evidence
            .steps
            .iter()
            .find(|s| s.name == name)
            .unwrap()
            .clone()
    };
    let fill = step("fill");
    assert!(!fill.pass, "{fill:?}");
    assert!(
        printed.contains("FAIL fill") && printed.contains("after the flatten"),
        "{printed}"
    );
    assert!(
        fill.comparisons
            .iter()
            .any(|c| c.after == "fill: after the flatten" && !c.positions_agreed),
        "{fill:?}"
    );
    assert_eq!(step("fill-partial").ran, evidence::Ran::NotRun);
    assert_eq!(step("cod-dropped").ran, evidence::Ran::NotRun);
    assert!(venue.lock().unwrap().position.is_zero(), "{printed}");
    assert!(!outcome.ok, "{printed}");
}

#[tokio::test]
async fn a_partly_filled_order_rest_disagrees_on_after_its_amend_is_not_flattened() {
    // Codex r4290085861: GET /positions answers a position the stream does not show once P1's
    // amend was sent. P1 is cancelled and ends with its fill; REST disagreeing then leaves
    // what it filled unknown: nothing is flattened from it (no F2), and the phase stops with
    // the line to flatten by hand.
    let venue = new_venue(true, true);
    {
        let mut v = venue.lock().unwrap();
        v.ask_top.1 = Decimal::from_str("0.00005").unwrap();
        v.stale_position_after_modify = true;
    }
    // To the IOC, T1 and F1, P1, its amend and its cancel.
    let stub = stub_frames(&venue, &[MAIN + 4, 6]).await;
    let (evidence_dir, _e) = temp_dir("evidence-partial-after-amend");
    let (lease_dir, _l) = temp_dir("lease-partial-after-amend");
    let mut opts = options(&stub, &evidence_dir.join("run"), &lease_dir, 10);
    opts.allow_fill = true;
    opts.rest_wait_secs = 1;
    let (outcome, printed) = run_against(&opts).await;
    let report = fs::read_to_string(&outcome.report).unwrap();
    stub.finished()
        .await
        .unwrap_or_else(|e| panic!("{e:?}\n{printed}\n{report}"));
    let partial = outcome
        .evidence
        .steps
        .iter()
        .find(|s| s.name == "fill-partial")
        .unwrap();
    assert!(!partial.pass, "{partial:?}");
    assert!(
        printed.contains("FAIL fill-partial") && printed.contains("flatten by hand"),
        "{printed}"
    );
    let reduce_only = creates(&stub, 0)
        .iter()
        .filter(|p| p["flags"] == json!(["REDUCE_ONLY"]))
        .count();
    assert_eq!(reduce_only, 1, "F1 alone: {printed}");
    assert_eq!(
        venue.lock().unwrap().position,
        Decimal::from_str("0.00005").unwrap()
    );
    assert!(!outcome.ok, "{printed}");
}

#[test]
fn a_flattens_book_depth_past_an_i64_bound_covers_the_size_not_wrapped() {
    // Codex r4290028714: the flatten adds up the book's depth checked; a sum past i64::MAX
    // covers any size.
    let lvl = |px: i64, qty: i64| fbc_core::Lvl {
        px: fbc_core::Ticks(px),
        qty: fbc_core::Lots::new(qty).unwrap(),
    };
    let levels = [lvl(10, i64::MAX - 1), lvl(9, 5), lvl(8, 1)];
    let max = fbc_core::Lots::new(i64::MAX).unwrap();
    assert_eq!(accept::flatten_px(&levels, max), Some(fbc_core::Ticks(9)));
    let small = fbc_core::Lots::new(3).unwrap();
    assert_eq!(
        accept::flatten_px(&levels, small),
        Some(fbc_core::Ticks(10))
    );
    let shallow = [lvl(10, 1), lvl(9, 1)];
    assert_eq!(
        accept::flatten_px(&shallow, small),
        Some(fbc_core::Ticks(9))
    );
    assert_eq!(accept::flatten_px(&[], small), None);
}

#[test]
fn the_risk_answer_follows_the_ioc_placements_the_fill_steps_sent() {
    // Codex r4290085864: 0092 (2) is answered from the IOC placements the fill steps sent
    // and the flag, never from the fill step's status.
    let q = "0092 (2)";
    // --allow-fill given, the fill step skipped, fill-partial sending only its GTC order.
    let mut ev = evidence::Evidence {
        allow_fill: true,
        ..evidence::Evidence::default()
    };
    let mut fill = evidence::Step::new("fill");
    fill.ran = evidence::Ran::Skipped("a one-sided book".to_owned());
    ev.steps.push(fill);
    let a = answer(&ev, q);
    assert!(
        a.starts_with("not exercised") && a.contains("--allow-fill was given"),
        "{a}"
    );
    // The fill step failed before its placement: still no IOC sent.
    ev.steps[0].ran = evidence::Ran::Yes;
    let a = answer(&ev, q);
    assert!(a.starts_with("not exercised"), "{a}");
    // Two IOCs sent, neither refused.
    ev.fill_iocs = 2;
    let a = answer(&ev, q);
    assert!(a.starts_with("not observed") && a.contains("2 sent"), "{a}");
    // Without the flag.
    let ev = evidence::Evidence::default();
    let a = answer(&ev, q);
    assert!(a.contains("without --allow-fill"), "{a}");
}

#[test]
fn only_p1s_amend_admits_an_order_that_filled_in_part() {
    // Codex r4289888364: the phase's caps are Paradex's, which refuse the amend of a partly
    // filled order; only P1's amend is judged under caps that admit it. (No stub can fill B1
    // between the place step's Open check and its amend: that race is a live venue's, and the
    // phase judges B1's amend under these caps.)
    let paradex = fbc_venue_paradex::exec::exec_caps().order;
    assert!(
        !paradex.amend.as_ref().unwrap().when_partially_filled,
        "{paradex:?}"
    );
    let p1 = accept::partial_amend_caps(&paradex);
    assert!(p1.amend.as_ref().unwrap().when_partially_filled, "{p1:?}");
    let mut without = paradex.clone();
    without.amend = None;
    assert!(accept::partial_amend_caps(&without).amend.is_none());
}
