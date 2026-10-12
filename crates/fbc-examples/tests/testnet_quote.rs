//! FBC-elg7's done line: testnet_quote, run against the conformance kit's stub server on
//! 127.0.0.1 standing in for Paradex, with synthetic credentials, over several requote cycles:
//! no amend frame is ever sent; every replace is a cancel acknowledged (and the order ended)
//! before the next place on that level; every place is within the $50 inventory cap and the $11
//! resting cap per side; after a scripted reconnect every order of ours still open on the market
//! is cancelled before any place; a resync position mismatch Stops it (kill switch + cancel all)
//! with no place after; a mainnet URL or chain id is refused before any connection; and no
//! credential appears in what it prints or in what the stack logs.
//!
//! **The reconnect's cancel.** The ticket asks for an instrument cancel-all there. fbc-oms
//! builds one only under 0005's I7 guard, which needs a trustworthy resync to have shown the
//! account's open orders; Paradex's snapshot source is Untrustworthy (owner answer C to
//! RB-olg-3), so fbc-oms refuses it (`CancelAllRefusal::NotViewed`) and the loop cancels each
//! order of ours still open explicitly: those the resync shows through the session's Safety
//! cancels (decision 0080), and any other the registry holds open (one the snapshot did not
//! show) through its own. The loop takes the instrument cancel-all whenever fbc-oms builds one.
//!
//! The stub is a small stateful venue ([`Venue`]): it answers the login, the order book the
//! test scripts cycle by cycle, the resync's open orders (from what it holds) and positions over
//! HTTP, and on the order socket the auth frame, the subscriptions, the cancel-on-disconnect
//! arm, each place (its order echoed under a new venue id) and each cancel (queued, then an SBE
//! `OrderEvent` at 1:2 reporting the order CLOSED by USER_CANCELED). The run is on real time,
//! each bounded at 60 s.

#[path = "../examples/testnet_quote/args.rs"]
mod args;
// Shared by the samples that trade: each uses a part of it.
#[allow(dead_code)]
#[path = "../src/link.rs"]
mod link;
#[path = "../examples/testnet_quote/quote.rs"]
mod quote;
// Shared by the samples that trade: each uses a part of it.
#[allow(dead_code)]
#[path = "../src/testnet.rs"]
mod testnet;

use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Once};

use std::time::Duration;

use args::{Options, Parsed, USAGE};
use fbc_conformance::{
    Frame, HttpReply, HttpRouter, PathPattern, Responder, Step, StubServer, WsScript,
};
use fbc_core::{
    AccountKey, CidMint, Lots, Namespace, NamespaceLease, Secret, Secrets, Side, SignedLots, Ticks,
    WallNs, encode_cid,
};
use fbc_runtime::ProxyConfig;
use fbc_runtime::http::Method;
use fbc_venue_paradex::auth::{ACCOUNT_ADDRESS, SIGNING_KEY};
use rust_decimal::Decimal;
use serde_json::{Value, json};
use tokio::sync::Notify;

/// The session token the stub's login gives: made-up text that never looks like a JWT.
const TOKEN: &str = "SYNTHETIC.session-token.testnet-quote";
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

/// A directory removed when it is dropped.
struct Removed(PathBuf);

impl Drop for Removed {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

thread_local! {
    /// The lease directories the test on this thread made, removed when its thread ends.
    static MADE: RefCell<Vec<Removed>> = const { RefCell::new(Vec::new()) };
}

/// A lease directory of the test's own, removed when the test's thread ends.
fn lease_dir() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    let dir =
        std::env::temp_dir().join(format!("fbc-testnet-quote-test-{}-{n}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    MADE.with(|made| made.borrow_mut().push(Removed(dir.clone())));
    dir
}

fn dec(text: &str) -> Decimal {
    Decimal::from_str(text).unwrap()
}

/// The owner's caps ($11 resting per side, $50 inventory), quotes of $10.50 30 bps behind the
/// touch on the BTC grid (tick 0.1, step 0.00001), at most `cycles` cycles 100 ms apart.
fn options(stub: &StubServer, cycles: u64) -> Options {
    Options {
        market: MARKET.to_owned(),
        tick: dec("0.1"),
        step: dec("0.00001"),
        min_notional: dec("5"),
        resting_cap_usd: dec("11"),
        inventory_cap_usd: dec("50"),
        order_usd: dec("10.5"),
        namespace: 1,
        bid_bps: 30,
        ask_bps: 30,
        duration_secs: 60,
        cycles: Some(cycles),
        interval_ms: 100,
        step_timeout_secs: 10,
        rest_url: stub.http_url("/v1"),
        ws_url: stub.ws_url("/v1"),
        proxy: ProxyConfig::Direct,
        lease_dir: lease_dir(),
        sole_trader: true,
    }
}

/// A `GET /orderbook` answer with one level a side (`None`: the side is empty).
fn book(seq: u64, bid: Option<&str>, ask: Option<&str>) -> String {
    let level = |px: Option<&str>| match px {
        Some(px) => json!([[px, "0.5"]]),
        None => json!([]),
    };
    json!({
        "market": MARKET, "seq_no": seq, "last_updated_at": 1_759_500_000_000_i64,
        "bids": level(bid), "asks": level(ask),
    })
    .to_string()
}

/// The first touch every test starts from: bid 62000.2, ask 62000.5.
fn b0() -> String {
    book(1, Some("62000.2"), Some("62000.5"))
}

/// A decimal string as Paradex's SBE 10^-8 mantissa.
fn e8(text: &str) -> i64 {
    let d = dec(text) * Decimal::from(100_000_000);
    i64::try_from(d.mantissa() / 10i128.pow(d.scale())).unwrap()
}

/// An `OrderEvent` (template 20) at schema 1:2, as `fixtures/paradex/exec/README.md` describes
/// it: our post-only limit order `cid` on `side` (1 BUY, 2 SELL) of `size` at `price`, venue id
/// `vid`, CLOSED by USER_CANCELED with nothing filled. The account is 32 made-up bytes.
fn order_closed(side: u8, seq: i64, vid: &str, cid: &str, price: &str, size: &str) -> Vec<u8> {
    order_event(side, seq, vid, cid, price, size, size, "USER_CANCELED")
}

/// As [`order_closed`], with `open` of `size` still open and the cancel reason `reason` (empty
/// and nothing open: filled).
#[allow(clippy::too_many_arguments)]
fn order_event(
    side: u8,
    seq: i64,
    vid: &str,
    cid: &str,
    price: &str,
    size: &str,
    open: &str,
    reason: &str,
) -> Vec<u8> {
    let null = i64::MIN;
    let ts = 1_759_500_000_205_011i64;
    let mut f = Vec::new();
    for v in [128u16, 20, 1, 2] {
        f.extend(v.to_le_bytes());
    }
    f.extend(ts.to_le_bytes());
    f.extend(seq.to_le_bytes());
    f.extend([4u8, side, 1, 3]); // CLOSED, the side, LIMIT, POST_ONLY
    f.extend(e8(price).to_le_bytes());
    f.extend(null.to_le_bytes()); // triggerPrice
    f.extend(e8(size).to_le_bytes()); // size
    f.extend(e8(open).to_le_bytes()); // sizeOpen
    f.extend(null.to_le_bytes()); // avgFillPrice
    f.extend(ts.to_le_bytes()); // createdAt
    f.extend(ts.to_le_bytes()); // updatedAt
    f.extend(0xa0u8..=0xbf); // account, made up
    f.extend(ts.to_le_bytes()); // receivedAt
    f.extend(ts.to_le_bytes()); // publishedAt
    f.extend([0u8, 0, 0, 0]); // stp, flags, requestStatus, requestType
    assert_eq!(f.len(), 8 + 128);
    for s in [vid, cid, MARKET, reason, "", ""] {
        f.push(u8::try_from(s.len()).unwrap());
        f.extend(s.as_bytes());
    }
    f
}

/// A `FillEvent` (template 21) at version 1, as `fixtures/paradex/exec/README.md` describes it:
/// our buy `cid` (venue id `vid`) made `size` at `price` for no fee, fill id `fill`.
fn fill_event(seq: i64, fill: &str, vid: &str, cid: &str, price: &str, size: &str) -> Vec<u8> {
    let ts = 1_759_500_000_204_011i64;
    let mut f = Vec::new();
    for v in [107u16, 21, 1, 1] {
        f.extend(v.to_le_bytes());
    }
    f.extend(ts.to_le_bytes());
    f.extend(seq.to_le_bytes());
    f.extend([1u8, 1, 1]); // FILL, BUY, MAKER
    f.extend(e8(price).to_le_bytes());
    f.extend(e8(size).to_le_bytes());
    f.extend(0i64.to_le_bytes()); // fee
    f.extend(i64::MIN.to_le_bytes()); // realizedPnl: null
    f.extend(ts.to_le_bytes()); // createdAt
    f.extend(0xa0u8..=0xbf); // account, made up
    f.extend(e8(price).to_le_bytes()); // underlyingPrice
    f.extend(0i64.to_le_bytes()); // realizedFunding
    assert_eq!(f.len(), 8 + 107);
    for s in [fill, vid, cid, "9615262148007719001", MARKET] {
        f.push(u8::try_from(s.len()).unwrap());
        f.extend(s.as_bytes());
    }
    f
}

/// The account the REST answers name: the 32 made-up bytes the SBE frames carry, as hex.
fn account() -> String {
    let hex: String = (0xa0u8..=0xbf).map(|b| format!("{b:02x}")).collect();
    format!("0x{hex}")
}

/// An order the stub venue holds.
#[derive(Clone, Debug)]
struct Held {
    vid: String,
    cid: String,
    /// BUY or SELL.
    side: String,
    price: String,
    size: String,
    open: bool,
}

/// The stub venue: what it holds and what it is told to do.
#[derive(Default)]
struct Venue {
    orders: Vec<Held>,
    next_vid: u64,
    seq: i64,
    /// The order books `GET /orderbook` answers in turn, the last one again after.
    books: Vec<String>,
    book_reads: usize,
    orders_reads: usize,
    positions_reads: usize,
    /// From the second `GET /orders` on: hide the open sell orders (a lagging snapshot).
    hide_sells_after_first: bool,
    /// From the second `GET /positions` on: this position (a size, long) instead of flat.
    position_after_first: Option<&'static str>,
    /// Answer the next single cancel ALREADY_CLOSED, with no order event.
    already_closed_once: bool,
    /// Answer the next single cancel of a buy queued, then fill the buy whole (its fill event
    /// sent twice, the second a duplicate) and close it filled: a fill racing the cancel.
    fill_next_buy_cancel: bool,
    /// Answer every batch cancel's second item ALREADY_CLOSED, its order's end sent first.
    batch_second_already_closed: bool,
    /// Send a frame no codec decodes after the reply to this place (from 1).
    garbage_after_create: Option<usize>,
    /// Notified at this `GET /orderbook` read (from 1): the loop's cycle before it settled.
    signal: Option<(usize, Arc<Notify>)>,
    /// Close the order this place (from 1) created right after its reply, nothing filled, for
    /// this reason: Paradex's risk check refusing it (`POST_ONLY_WOULD_CROSS`: the touch moved
    /// through the quote).
    close_create: Option<(usize, &'static str)>,
    creates: usize,
    /// Every frame the stub sent, with the connection's number, for the wire-order checks.
    log: Vec<(usize, String)>,
}

type Shared = Arc<Mutex<Venue>>;

fn lock(venue: &Shared) -> MutexGuard<'_, Venue> {
    venue.lock().unwrap_or_else(|e| e.into_inner())
}

fn reply(status: u16, body: impl Into<Vec<u8>>) -> HttpReply {
    HttpReply {
        status,
        body: body.into(),
    }
}

/// The stub's HTTP answers, from `venue`.
fn routes(venue: &Shared, login: HttpReply) -> HttpRouter {
    let (books, orders, positions) = (venue.clone(), venue.clone(), venue.clone());
    HttpRouter::new()
        .route(Method::POST, PathPattern::exact("/v1/auth"), login)
        .route_fn(
            Method::GET,
            PathPattern::exact(&format!("/v1/orderbook/{MARKET}")),
            move |_| {
                let mut v = lock(&books);
                let at = v.book_reads.min(v.books.len() - 1);
                v.book_reads += 1;
                if let Some((n, signal)) = &v.signal
                    && v.book_reads == *n
                {
                    signal.notify_one();
                }
                reply(200, v.books[at].clone())
            },
        )
        .route_fn(Method::GET, PathPattern::exact("/v1/orders"), move |_| {
            let mut v = lock(&orders);
            v.orders_reads += 1;
            let hide = v.hide_sells_after_first && v.orders_reads > 1;
            let results: Vec<Value> = v
                .orders
                .iter()
                .filter(|o| o.open && !(hide && o.side == "SELL"))
                .map(|o| {
                    json!({
                        "id": o.vid, "client_id": o.cid, "market": MARKET, "side": o.side,
                        "type": "LIMIT", "instruction": "POST_ONLY", "price": o.price,
                        "size": o.size, "remaining_size": o.size, "status": "OPEN",
                        "flags": [], "account": account(),
                    })
                })
                .collect();
            reply(200, json!({ "results": results }).to_string())
        })
        .route_fn(
            Method::GET,
            PathPattern::exact("/v1/positions"),
            move |_| {
                let mut v = lock(&positions);
                v.positions_reads += 1;
                let size = v.position_after_first.filter(|_| v.positions_reads > 1);
                let body = match size {
                    None => r#"{"results":[]}"#.to_owned(),
                    Some(size) => json!({ "results": [{
                        "account": account(),
                        "average_entry_price": "62000", "average_entry_price_usd": "62000",
                        "average_exit_price": "0", "cached_funding_index": "0", "closed_at": 0,
                        "cost": "0", "cost_usd": "0", "created_at": 1_759_622_300_000_i64,
                        "id": "BTC-USD-PERP-position", "last_fill_id": "fill-BTC-USD-PERP",
                        "last_updated_at": 1_759_622_390_000_i64, "leverage": "",
                        "liquidation_price": "", "market": MARKET,
                        "realized_positional_funding_pnl": "0", "realized_positional_pnl": "0",
                        "seq_no": 7, "side": "LONG", "size": size, "status": "OPEN",
                        "unrealized_funding_pnl": "0", "unrealized_pnl": "0",
                    }]})
                    .to_string(),
                };
                reply(200, body)
            },
        )
}

/// The stub's answer to each JSON-RPC frame on connection `conn`, from `venue`.
fn responder(venue: &Shared, conn: usize) -> Responder {
    let venue = venue.clone();
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
        let mut v = lock(&venue);
        let method = req["method"].as_str().unwrap_or("").to_owned();
        v.log.push((conn, text.clone()));
        let closed = |v: &mut Venue, at: usize| {
            v.orders[at].open = false;
            v.seq += 1;
            let o = v.orders[at].clone();
            let side = if o.side == "BUY" { 1 } else { 2 };
            Frame::Binary(order_closed(
                side,
                5_000 + v.seq,
                &o.vid,
                &o.cid,
                &o.price,
                &o.size,
            ))
        };
        let frames = match method.as_str() {
            "auth" | "subscribe" => vec![ok(json!({}))],
            "order.cancel_on_disconnect" => vec![ok(json!({"enabled": true}))],
            "order.create" => {
                v.next_vid += 1;
                let held = Held {
                    vid: format!("17595000000000{:05}", v.next_vid),
                    cid: params["client_id"].as_str().unwrap_or("").to_owned(),
                    side: params["side"].as_str().unwrap_or("").to_owned(),
                    price: params["price"].as_str().unwrap_or("").to_owned(),
                    size: params["size"].as_str().unwrap_or("").to_owned(),
                    open: true,
                };
                let order = json!({
                    "id": held.vid, "client_id": held.cid, "market": MARKET, "side": held.side,
                    "type": "LIMIT", "instruction": "POST_ONLY", "price": held.price,
                    "size": held.size, "remaining_size": held.size, "status": "NEW",
                });
                v.orders.push(held);
                v.creates += 1;
                let mut frames = vec![ok(json!({ "order": order }))];
                if let Some((n, reason)) = v.close_create
                    && n == v.creates
                {
                    let at = v.orders.len() - 1;
                    v.orders[at].open = false;
                    v.seq += 1;
                    let o = v.orders[at].clone();
                    let side = if o.side == "BUY" { 1 } else { 2 };
                    frames.push(Frame::Binary(order_event(
                        side,
                        5_000 + v.seq,
                        &o.vid,
                        &o.cid,
                        &o.price,
                        &o.size,
                        &o.size,
                        reason,
                    )));
                }
                if v.garbage_after_create == Some(v.creates) {
                    frames.push(Frame::Binary(b"\xff".to_vec()));
                }
                frames
            }
            "order.cancel" => {
                let at = v
                    .orders
                    .iter()
                    .position(|o| {
                        params["id"].as_str() == Some(o.vid.as_str())
                            || params["client_id"].as_str() == Some(o.cid.as_str())
                    })
                    .ok_or("a cancel of no order the stub holds")?;
                let vid = v.orders[at].vid.clone();
                if v.fill_next_buy_cancel && v.orders[at].side == "BUY" {
                    v.fill_next_buy_cancel = false;
                    v.orders[at].open = false;
                    let o = v.orders[at].clone();
                    v.seq += 2;
                    let fill = fill_event(
                        5_000 + v.seq,
                        "8615262148007719001",
                        &o.vid,
                        &o.cid,
                        &o.price,
                        &o.size,
                    );
                    let filled =
                        order_event(1, 5_001 + v.seq, &o.vid, &o.cid, &o.price, &o.size, "0", "");
                    let queued = ok(json!({"order_id": vid, "status": "QUEUED_FOR_CANCELLATION"}));
                    vec![
                        queued,
                        Frame::Binary(fill.clone()),
                        Frame::Binary(fill),
                        Frame::Binary(filled),
                    ]
                } else if v.already_closed_once {
                    v.already_closed_once = false;
                    vec![ok(json!({"order_id": vid, "status": "ALREADY_CLOSED"}))]
                } else {
                    let queued = ok(json!({"order_id": vid, "status": "QUEUED_FOR_CANCELLATION"}));
                    vec![queued, closed(&mut v, at)]
                }
            }
            "order.cancel_batch" => {
                let ids = params["order_ids"].as_array().cloned().unwrap_or_default();
                let mut results = Vec::new();
                let mut events = Vec::new();
                let mut first = Vec::new();
                for (i, id) in ids.iter().enumerate() {
                    let vid = id.as_str().unwrap_or("");
                    let at = v
                        .orders
                        .iter()
                        .position(|o| o.vid == vid)
                        .ok_or("a batch cancel of no order the stub holds")?;
                    let status = if v.batch_second_already_closed && i == 1 {
                        // Its end is sent before the reply: it closed before the cancel came.
                        first.push(closed(&mut v, at));
                        "ALREADY_CLOSED"
                    } else {
                        events.push(closed(&mut v, at));
                        "QUEUED_FOR_CANCELLATION"
                    };
                    results.push(json!({"id": vid, "market": MARKET, "status": status}));
                }
                let mut frames = first;
                frames.push(ok(json!({ "results": results })));
                frames.extend(events);
                frames
            }
            other => return Err(format!("an unexpected method {other}")),
        };
        Ok(frames)
    })
}

/// `n` answered frames on connection `conn`.
fn answered(venue: &Shared, conn: usize, n: usize) -> Vec<Step> {
    let with = responder(venue, conn);
    (0..n)
        .map(|_| Step::Respond {
            conn,
            with: with.clone(),
        })
        .collect()
}

/// One connection, `n` frames answered.
fn one_connection(venue: &Shared, n: usize) -> WsScript {
    let mut steps = vec![Step::Accept];
    steps.extend(answered(venue, 0, n));
    WsScript::new(steps)
}

/// Two connections: `first` frames answered on the first, which the stub then closes, and
/// `second` on the next.
fn reconnecting(venue: &Shared, first: usize, second: usize) -> WsScript {
    let mut steps = vec![Step::Accept];
    steps.extend(answered(venue, 0, first));
    steps.push(Step::Close { conn: 0 });
    steps.push(Step::Accept);
    steps.extend(answered(venue, 1, second));
    WsScript::new(steps)
}

/// A venue answering `books` in turn.
fn venue(books: Vec<String>) -> Shared {
    Arc::new(Mutex::new(Venue {
        books,
        ..Venue::default()
    }))
}

fn login_ok() -> HttpReply {
    reply(200, format!(r#"{{"jwt_token":"{TOKEN}"}}"#))
}

/// Runs testnet_quote with `opts` until it ends or `interrupt` completes, bounded at 60 s: its
/// report and what it printed.
async fn run_until(
    opts: &Options,
    interrupt: impl std::future::Future<Output = ()>,
) -> (quote::Report, String) {
    let buf = Rc::new(RefCell::new(Vec::<u8>::new()));
    let out: testnet::Out = buf.clone();
    let run = quote::run(opts, None, secrets(), out, interrupt);
    let ran = tokio::time::timeout(Duration::from_secs(60), run).await;
    let printed = String::from_utf8(buf.borrow().clone()).unwrap();
    let report = ran
        .unwrap_or_else(|_| panic!("testnet_quote did not finish in 60 s; it printed:\n{printed}"))
        .unwrap_or_else(|e| panic!("refused: {e}; it printed:\n{printed}"));
    (report, printed)
}

/// Runs testnet_quote with `opts` until it ends by itself.
async fn run_against(opts: &Options) -> (quote::Report, String) {
    run_until(opts, std::future::pending()).await
}

/// The JSON-RPC requests the session sent on connection `conn`, in order.
fn requests(venue: &Shared, conn: usize) -> Vec<Value> {
    lock(venue)
        .log
        .iter()
        .filter(|(c, _)| *c == conn)
        .map(|(_, t)| serde_json::from_str(t).unwrap())
        .collect()
}

fn methods(venue: &Shared, conn: usize) -> Vec<String> {
    requests(venue, conn)
        .iter()
        .map(|r| r["method"].as_str().unwrap_or("").to_owned())
        .collect()
}

/// The first four words after the tag of each line tagged `tag`.
fn tagged<'a>(printed: &'a str, tag: &str) -> Vec<&'a str> {
    printed
        .lines()
        .filter(|l| l.split_whitespace().next() == Some(tag))
        .collect()
}

/// No credential, and not the session token, in `text`.
fn no_credential_in(text: &str) {
    let (account, key) = synthetic();
    let lower = text.to_ascii_lowercase();
    for secret in [&account, &key, &TOKEN.to_owned()] {
        let bare = secret.trim_start_matches("0x").to_ascii_lowercase();
        assert!(!lower.contains(&bare), "a credential appeared:\n{text}");
    }
}

/// Everything the stack logs through the `log` facade, captured at every level it compiles in.
mod capture {
    use super::*;
    use log::{LevelFilter, Log, Metadata, Record};

    static LINES: Mutex<Vec<String>> = Mutex::new(Vec::new());

    struct Capture;

    impl Log for Capture {
        fn enabled(&self, _: &Metadata<'_>) -> bool {
            true
        }

        fn log(&self, record: &Record<'_>) {
            let line = format!("{} {} {}", record.level(), record.target(), record.args());
            LINES.lock().unwrap_or_else(|e| e.into_inner()).push(line);
        }

        fn flush(&self) {}
    }

    static CAPTURE: Capture = Capture;

    /// Installs the capture once for the test binary.
    pub fn install() {
        static ONCE: Once = Once::new();
        ONCE.call_once(|| {
            log::set_logger(&CAPTURE).expect("no other logger in this test binary");
            log::set_max_level(LevelFilter::Trace);
        });
    }

    /// Every record logged so far, by any test of the binary.
    pub fn all() -> String {
        LINES.lock().unwrap_or_else(|e| e.into_inner()).join("\n")
    }
}

/// The side (BUY or SELL) of the order venue id `vid` names.
fn side_of(venue: &Shared, vid: &str) -> String {
    let v = lock(venue);
    v.orders.iter().find(|o| o.vid == vid).unwrap().side.clone()
}

/// The venue ids a cancel request names.
fn cancelled_ids(req: &Value) -> Vec<String> {
    match req["method"].as_str() {
        Some("order.cancel") => vec![req["params"]["id"].as_str().unwrap().to_owned()],
        Some("order.cancel_batch") => req["params"]["order_ids"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_owned())
            .collect(),
        _ => Vec::new(),
    }
}

#[tokio::test]
async fn testnet_quote_requotes_by_cancel_and_replace_within_the_caps_against_the_stub() {
    capture::install();
    // Cycle 1 quotes the first touch; the touch moves up for cycle 2 and down for cycle 3, so
    // both sides are replaced twice; cycle 4 sees no change and sends nothing.
    let books = vec![
        b0(),
        b0(),
        book(2, Some("62100.0"), Some("62100.3")),
        book(3, Some("61900.0"), Some("61900.4")),
    ];
    let venue = venue(books);
    // The opening six (auth, four subscriptions, the arm), two places, two replaces of two
    // cancels and two places each, and Stop's one batch cancel.
    let stub = StubServer::start(
        one_connection(&venue, 6 + 2 + 4 + 4 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = options(&stub, 4);
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    assert!(report.ok, "{printed}");
    assert_eq!(report.ended, "--cycles 4 reached", "{printed}");
    assert_eq!(
        (report.cycles, report.places_sent, report.cancels_sent),
        (4, 6, 5),
        "{printed}"
    );
    assert_eq!(
        (report.cancelled, report.fills, report.inventory),
        (6, 0, 0),
        "{printed}"
    );
    // Nothing of the answered requests is kept, in the loop or the link.
    assert_eq!(report.requests_held, 0, "{printed}");

    // No amend frame, ever: every change is a cancel and a place.
    let sent = methods(&venue, 0);
    let count = |m: &str| sent.iter().filter(|s| s.as_str() == m).count();
    assert_eq!(count("order.modify"), 0, "{sent:?}");
    assert_eq!(count("order.create"), 6, "{sent:?}");
    assert_eq!(count("order.cancel"), 4, "{sent:?}");
    assert_eq!(count("order.cancel_batch"), 1, "{sent:?}");
    assert_eq!(count("order.cancel_all"), 0, "{sent:?}");

    // Per side, on the wire: a place, then the cancel of that very order before the next place.
    let reqs = requests(&venue, 0);
    for side in ["BUY", "SELL"] {
        let mut resting: Option<String> = None;
        let mut places = 0;
        for req in &reqs {
            match req["method"].as_str().unwrap() {
                "order.create" if req["params"]["side"] == side => {
                    assert!(
                        resting.is_none(),
                        "a place while {resting:?} rests: {reqs:#?}"
                    );
                    let cid = req["params"]["client_id"].as_str().unwrap();
                    let v = lock(&venue);
                    resting = Some(v.orders.iter().find(|o| o.cid == cid).unwrap().vid.clone());
                    places += 1;
                }
                "order.cancel" | "order.cancel_batch" => {
                    for id in cancelled_ids(req) {
                        if side_of(&venue, &id) == side {
                            assert_eq!(resting.as_deref(), Some(id.as_str()), "{reqs:#?}");
                            resting = None;
                        }
                    }
                }
                _ => {}
            }
        }
        assert_eq!(places, 3, "{side}: {reqs:#?}");
        assert!(resting.is_none(), "{side}: Stop left one resting");
    }

    // Per side, in what it printed before Stop: each place acknowledged, and each replace's
    // cancel acknowledged and its order closed before the next place.
    let before_stop = printed.split("\nSTOP ").next().unwrap();
    for side in ["bid", "ask"] {
        let tags: Vec<&str> = before_stop
            .lines()
            .filter(|l| l.split_whitespace().nth(2) == Some(side))
            .map(|l| l.split_whitespace().next().unwrap())
            .collect();
        assert_eq!(
            tags,
            [
                "PLACE",
                "ACK",
                "CANCEL",
                "CANCEL-ACK",
                "CLOSED",
                "PLACE",
                "ACK",
                "CANCEL",
                "CANCEL-ACK",
                "CLOSED",
                "PLACE",
                "ACK"
            ],
            "{side}:\n{printed}"
        );
    }

    // Every place within the caps: its notional at its price at most the $11 resting cap, and
    // a fill of it (from flat) worth at most the $50 inventory cap at the touch.
    for req in reqs.iter().filter(|r| r["method"] == "order.create") {
        assert_eq!(req["params"]["instruction"], "POST_ONLY");
        let px = dec(req["params"]["price"].as_str().unwrap());
        let size = dec(req["params"]["size"].as_str().unwrap());
        assert!(px * size <= dec("11"), "{req}");
        assert!(dec("62100.3") * size <= dec("50"), "{req}");
        // 16 lots: $10.50 at the first touch's ask quote, 62186.6.
        assert_eq!(size, dec("0.00016"), "{req}");
    }
    // The quotes 30 bps behind each touch: the first at 61814.1 / 62186.6.
    let first = |side: &str| {
        reqs.iter()
            .find(|r| r["method"] == "order.create" && r["params"]["side"] == side)
            .map(|r| r["params"]["price"].as_str().unwrap().to_owned())
            .unwrap()
    };
    assert_eq!(
        (first("BUY"), first("SELL")),
        ("61814.1".into(), "62186.6".into())
    );

    assert!(
        printed.contains("PLUMBING TEST, not a strategy"),
        "{printed}"
    );
    assert!(
        printed.contains(
            "QUOTE 16 lots (0.00016 at the size step) per side; caps at 62186.6: \
                          resting 17 lots per side, inventory 80 lots"
        ),
        "{printed}"
    );
    assert_eq!(tagged(&printed, "CYCLE").len(), 3, "{printed}");
    assert!(
        printed.contains(
            "SUMMARY cycles 4 orders placed 6 cancels sent 5 orders cancelled 6 fills 0 \
             inventory 0 lots"
        ),
        "{printed}"
    );
    assert!(
        printed.contains("DONE ok (--cycles 4 reached)"),
        "{printed}"
    );
    assert!(printed.contains("STEP stop"), "{printed}");
    // No credential, and not the session token, in what it printed or what the stack logged.
    no_credential_in(&printed);
    no_credential_in(&capture::all());
}

#[tokio::test]
async fn after_a_reconnect_every_order_of_ours_is_cancelled_before_any_place() {
    capture::install();
    let venue = venue(vec![b0()]);
    // The resync after the reconnect shows the bid resting but not the ask (a lagging
    // snapshot): the session tells the bid unprotected and the link Safety-cancels it (decision
    // 0080), and the loop's own sweep cancels the ask, which the registry still holds open.
    lock(&venue).hide_sells_after_first = true;
    // First connection: the opening six and two places, then the stub closes it. Second: the
    // opening six, the bid's Safety cancel, the ask's sweep cancel, two places, Stop's cancel.
    let script = reconnecting(&venue, 6 + 2, 6 + 1 + 1 + 2 + 1);
    let stub = StubServer::start(script, routes(&venue, login_ok()))
        .await
        .unwrap();
    let opts = options(&stub, 3);
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    assert!(report.ok, "{printed}");
    let conn1 = methods(&venue, 1);
    let first_place = conn1
        .iter()
        .position(|m| m == "order.create")
        .unwrap_or_else(|| panic!("nothing placed after the reconnect: {conn1:?}\n{printed}"));
    // Before any place on the new connection, a cancel of each order of ours conn 0 left.
    let cancelled: Vec<String> = requests(&venue, 1)[..first_place]
        .iter()
        .flat_map(cancelled_ids)
        .collect();
    let left: Vec<String> = lock(&venue).orders[..2]
        .iter()
        .map(|o| o.vid.clone())
        .collect();
    for vid in &left {
        assert!(
            cancelled.contains(vid),
            "{vid} not cancelled first: {conn1:?}\n{printed}"
        );
    }
    assert_eq!(
        conn1[..6].iter().filter(|m| m.contains("order.")).count(),
        1,
        "{conn1:?}"
    );
    assert_eq!(
        conn1.iter().filter(|m| *m == "order.create").count(),
        2,
        "{conn1:?}"
    );
    assert_eq!(
        conn1.iter().filter(|m| *m == "order.modify").count(),
        0,
        "{conn1:?}"
    );
    // The reconnect says how: explicit cancels, since fbc-oms builds no instrument cancel-all
    // without a trustworthy resync (0005's I7).
    assert!(
        printed.contains("RECONNECT every order of ours still open on the market is cancelled"),
        "{printed}"
    );
    assert!(
        printed.contains("no trustworthy resync has shown the account's open orders yet"),
        "{printed}"
    );
    assert!(printed.contains("Safety cancel requests"), "{printed}");
    no_credential_in(&printed);
}

#[tokio::test]
async fn a_resync_position_mismatch_stops_it_with_no_place_after() {
    let venue = venue(vec![b0()]);
    // After the reconnect the venue reports a long position the inventory (flat) does not hold.
    lock(&venue).position_after_first = Some("0.001");
    // Second connection: the opening six and the Safety cancel of both resting orders; then
    // Stop, which finds both cancels in flight, and nothing more.
    let script = reconnecting(&venue, 6 + 2, 6 + 1);
    let stub = StubServer::start(script, routes(&venue, login_ok()))
        .await
        .unwrap();
    let opts = options(&stub, 50);
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    assert!(!report.ok, "{printed}");
    assert!(
        report
            .ended
            .starts_with("a resync found the position other than the registry's"),
        "{printed}"
    );
    let conn1 = methods(&venue, 1);
    assert!(!conn1.iter().any(|m| m == "order.create"), "{conn1:?}");
    assert!(printed.contains("STEP stop"), "{printed}");
    assert!(printed.contains("kill switch on"), "{printed}");
    // Every order of ours ended cancelled all the same.
    assert_eq!(report.cancelled, 2, "{printed}");
    assert!(printed.contains("DONE failed"), "{printed}");
    let stop = printed.find("STOP ").unwrap();
    assert!(!printed[stop..].contains("PLACE "), "{printed}");
}

#[tokio::test]
async fn the_interrupt_stops_it_cancelling_every_order() {
    let venue = venue(vec![b0()]);
    let signal = Arc::new(Notify::new());
    // The interrupt comes as cycle 2 reads the touch: cycle 1's two orders rest, acknowledged.
    lock(&venue).signal = Some((3, Arc::clone(&signal)));
    let stub = StubServer::start(
        one_connection(&venue, 6 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = options(&stub, 1_000);
    let interrupt = async move { signal.notified().await };
    let (report, printed) = run_until(&opts, interrupt).await;
    stub.finished().await.unwrap();

    assert!(report.ok, "{printed}");
    assert_eq!(report.ended, "interrupted (Ctrl-C)", "{printed}");
    assert_eq!(
        (report.places_sent, report.cancels_sent),
        (2, 1),
        "{printed}"
    );
    assert_eq!(report.cancelled, 2, "{printed}");
}

#[tokio::test]
async fn a_cancel_that_comes_back_unknown_stops_it() {
    // The touch moves for cycle 2; the bid's cancel is answered ALREADY_CLOSED, which Paradex's
    // single cancel decodes as Unknown (FBC-rht4), and the stub leaves the bid open: the loop
    // Stops, places nothing more, and Stop cancels the bid again.
    let venue = venue(vec![b0(), b0(), book(2, Some("62100.0"), Some("62100.3"))]);
    lock(&venue).already_closed_once = true;
    // The opening six, two places, the two replace cancels, and Stop's cancel of the bid.
    let stub = StubServer::start(
        one_connection(&venue, 6 + 2 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = options(&stub, 10);
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    assert!(!report.ok, "{printed}");
    assert!(report.ended.contains("came back Unknown"), "{printed}");
    let reqs = requests(&venue, 0);
    let sent = methods(&venue, 0);
    assert_eq!(
        sent.iter().filter(|m| *m == "order.create").count(),
        2,
        "{sent:?}"
    );
    let bid = lock(&venue).orders[0].vid.clone();
    assert_eq!(cancelled_ids(&reqs[10]), [bid], "{sent:?}");
    assert!(printed.contains("gone Unknown"), "{printed}");
    assert!(printed.contains("STEP stop"), "{printed}");
    // Every order of ours ended all the same.
    assert_eq!(report.cancelled, 2, "{printed}");
}

#[tokio::test]
async fn a_refused_login_stops_it_at_once() {
    let venue = venue(vec![b0()]);
    let body = json!({
        "error": "STARKNET_SIGNATURE_VERIFICATION_FAILED",
        "message": "verification failed on Curve",
    });
    let script = WsScript::new(vec![Step::Accept, Step::Accept, Step::Accept]);
    let stub = StubServer::start(script, routes(&venue, reply(401, body.to_string())))
        .await
        .unwrap();
    let mut opts = options(&stub, 3);
    opts.step_timeout_secs = 40;
    let started = std::time::Instant::now();
    let (report, printed) = run_against(&opts).await;
    assert!(started.elapsed() < Duration::from_secs(30), "{printed}");
    assert!(!report.ok, "{printed}");
    assert!(
        report
            .ended
            .starts_with("a login was refused: the Paradex login was refused: HTTP 401"),
        "{printed}"
    );
    assert_eq!(
        (report.places_sent, report.cancels_sent),
        (0, 0),
        "{printed}"
    );
    assert!(methods(&venue, 0).is_empty());
    no_credential_in(&printed);
}

#[tokio::test]
async fn a_mainnet_url_or_chain_id_is_refused_before_any_connection() {
    let venue = venue(vec![b0()]);
    let stub = StubServer::start(WsScript::new(vec![]), routes(&venue, login_ok()))
        .await
        .unwrap();
    let opts = options(&stub, 1);
    let out: testnet::Out = Rc::new(RefCell::new(Vec::<u8>::new()));
    for chain in ["PRIVATE_SN_PARACLEAR_MAINNET", "PRIVATE_SN_POTC_SEPOLIA"] {
        let refused = quote::run(
            &opts,
            Some(chain),
            secrets(),
            out.clone(),
            std::future::pending(),
        )
        .await
        .unwrap_err();
        assert!(refused.contains("not the testnet chain id"), "{refused}");
        assert!(!refused.contains("POTC"), "{refused}");
    }
    let mut mainnet = opts.clone();
    mainnet.rest_url = "https://api.prod.paradex.trade/v1".to_owned();
    mainnet.ws_url = "wss://ws.api.prod.paradex.trade/v1".to_owned();
    let refused = quote::run(
        &mainnet,
        None,
        secrets(),
        out.clone(),
        std::future::pending(),
    )
    .await
    .unwrap_err();
    assert!(refused.contains("testnet only"), "{refused}");
    assert!(stub.connections().is_empty());
    assert!(
        stub.http_requests().is_empty(),
        "{:?}",
        stub.http_requests()
    );
    // The command line refuses mainnet URLs too.
    let mut argv = required();
    argv.extend(strings(&[
        "--rest-url",
        "https://api.prod.paradex.trade/v1",
    ]));
    assert!(args::parse(argv).unwrap_err().contains("testnet only"));
}

#[tokio::test]
async fn an_order_on_the_market_that_is_not_ours_refuses_the_run_before_start() {
    let venue = venue(vec![b0()]);
    lock(&venue).orders.push(Held {
        vid: "1759500000000099999".to_owned(),
        cid: "someone-else".to_owned(),
        side: "BUY".to_owned(),
        price: "60000".to_owned(),
        size: "0.0001".to_owned(),
        open: true,
    });
    let stub = StubServer::start(one_connection(&venue, 6), routes(&venue, login_ok()))
        .await
        .unwrap();
    let opts = options(&stub, 3);
    let (report, printed) = run_against(&opts).await;
    assert!(!report.ok, "{printed}");
    assert!(report.ended.contains("not ours"), "{printed}");
    assert!(!printed.contains("STEP start"), "{printed}");
    assert_eq!(report.places_sent, 0, "{printed}");
}

#[tokio::test]
async fn the_duration_ends_the_loop() {
    let venue = venue(vec![b0()]);
    let stub = StubServer::start(
        one_connection(&venue, 6 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let mut opts = options(&stub, 1_000_000);
    opts.duration_secs = 1;
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(report.ok, "{printed}");
    assert_eq!(report.ended, "--duration 1 s reached", "{printed}");
}

#[tokio::test]
async fn an_earlier_runs_order_is_cancelled_before_the_first_place() {
    // An order of ours an earlier run left resting: the session tells it unprotected as the
    // connection opens, the link Safety-cancels it, and the loop places only once it ended.
    let venue = venue(vec![b0()]);
    let lease =
        NamespaceLease::acquire(&lease_dir(), AccountKey::new(1), Namespace::new(1)).unwrap();
    let mut mint = CidMint::new(lease, 0, 0, WallNs(1_000_000_000));
    let fmt = fbc_venue_paradex::exec::exec_caps().order.client_id;
    let cid = encode_cid(&fmt, mint.mint().unwrap()).unwrap().to_string();
    lock(&venue).orders.push(Held {
        vid: "1759500000000088888".to_owned(),
        cid,
        side: "SELL".to_owned(),
        price: "70000".to_owned(),
        size: "0.00001".to_owned(),
        open: true,
    });
    let stub = StubServer::start(
        one_connection(&venue, 6 + 1 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = options(&stub, 2);
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();
    assert!(report.ok, "{printed}");
    let sent = methods(&venue, 0);
    assert_eq!(sent[6], "order.cancel_batch", "{sent:?}");
    assert_eq!(
        cancelled_ids(&requests(&venue, 0)[6]),
        ["1759500000000088888"]
    );
    assert_eq!(report.cancelled, 3, "{printed}");
}

#[tokio::test]
async fn a_fill_during_a_replace_requotes_from_the_position_it_left_never_past_the_usd_cap() {
    // The market rose 29% since the first touch, so fbc-oms's caps in lots (from the first
    // touch) admit more than the USD caps at the market. Cycle 2 replaces both quotes; the bid's
    // cancel races a fill of all of it (12 lots; the venue sends the fill twice). Placing the
    // bid again from the position the cycle read (flat) would admit 24 lots long, $19.2 at
    // 80000, past the $16 inventory cap; the loop requotes from the position the fill left.
    let venue = venue(vec![b0(), b0(), book(2, Some("80000.0"), Some("80000.2"))]);
    lock(&venue).fill_next_buy_cancel = true;
    // The opening six, two places, two replace cancels, the ask's new place, Stop's cancel.
    let script = one_connection(&venue, 6 + 2 + 2 + 1 + 1);
    let stub = StubServer::start(script, routes(&venue, login_ok()))
        .await
        .unwrap();
    let mut opts = options(&stub, 3);
    opts.order_usd = dec("8");
    opts.inventory_cap_usd = dec("16");
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    assert!(report.ok, "{printed}");
    // 12 lots a quote; the caps admit 25 lots of inventory at the first touch.
    assert!(printed.contains("QUOTE 12 lots"), "{printed}");
    assert!(printed.contains("inventory 25 lots"), "{printed}");
    let reqs = requests(&venue, 0);
    let buys = reqs
        .iter()
        .filter(|r| r["method"] == "order.create" && r["params"]["side"] == "BUY")
        .count();
    assert_eq!(
        buys, 1,
        "the bid was placed again after the fill: {reqs:#?}\n{printed}"
    );
    assert!(
        printed.contains("SKIP bid: a fill would take the position to 24 lots"),
        "{printed}"
    );
    // The fill is counted once, its duplicate not at all; the inventory moved by it.
    assert_eq!((report.fills, report.inventory), (1, 12), "{printed}");
    assert_eq!(tagged(&printed, "FILL").len(), 1, "{printed}");
}

#[tokio::test]
async fn a_stop_batch_cancel_with_an_item_refused_fails_the_run() {
    // Stop's batch cancel of both quotes: the first item queued, the second ALREADY_CLOSED (its
    // order ended just before). Every order ended, but not every Stop cancel was accepted.
    let venue = venue(vec![b0()]);
    lock(&venue).batch_second_already_closed = true;
    let stub = StubServer::start(
        one_connection(&venue, 6 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = options(&stub, 1);
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    assert!(!report.ok, "{printed}");
    assert_eq!(report.ended, "--cycles 1 reached", "{printed}");
    assert!(printed.contains("Stop's cancel"), "{printed}");
    assert!(printed.contains("DONE failed"), "{printed}");
}

#[tokio::test]
async fn a_frame_the_codec_cannot_decode_stops_it_at_once() {
    // After the second place's reply the venue sends a frame no codec decodes: it may have held
    // a fill or an order event, so the loop Stops at once instead of quoting on (Codex
    // r4243908424), and every order of ours is cancelled.
    let venue = venue(vec![b0(), b0(), book(2, Some("62100.0"), Some("62100.3"))]);
    lock(&venue).garbage_after_create = Some(2);
    let stub = StubServer::start(
        one_connection(&venue, 6 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = options(&stub, 10);
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    assert!(!report.ok, "{printed}");
    assert!(report.ended.contains("could not decode"), "{printed}");
    // Cycle 1 may or may not have settled when the frame was read; nothing was placed after.
    assert!(report.cycles <= 1, "{printed}");
    assert_eq!((report.places_sent, report.cancelled), (2, 2), "{printed}");
    let sent = methods(&venue, 0);
    assert_eq!(
        sent.iter().filter(|m| *m == "order.create").count(),
        2,
        "{sent:?}"
    );
}

#[tokio::test]
async fn a_quote_the_venue_closes_post_only_would_cross_is_placed_again_next_cycle() {
    // Paradex's risk check closes cycle 1's first quote POST_ONLY_WOULD_CROSS (the touch moved
    // through it between GET /orderbook and the place): an asynchronous reject of our own
    // post-only place, which leaves that side out of the cycle; cycle 2 places it again, and the
    // run goes on (RB-elg7-1).
    let venue = venue(vec![b0()]);
    lock(&venue).close_create = Some((1, "POST_ONLY_WOULD_CROSS"));
    // The opening six, two places, cycle 2's place again, Stop's cancel.
    let stub = StubServer::start(
        one_connection(&venue, 6 + 2 + 1 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = options(&stub, 2);
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    assert!(report.ok, "{printed}");
    assert_eq!(report.ended, "--cycles 2 reached", "{printed}");
    assert_eq!(report.places_sent, 3, "{printed}");
    assert!(printed.contains("POST_ONLY_WOULD_CROSS"), "{printed}");
    assert!(printed.contains("DONE ok"), "{printed}");
    let creates = requests(&venue, 0)
        .into_iter()
        .filter(|r| r["method"] == "order.create")
        .map(|r| r["params"]["side"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    // The side the venue closed is the one placed again.
    assert_eq!(creates[2], creates[0], "{creates:?}\n{printed}");
}

#[tokio::test]
async fn an_asynchronous_reject_other_than_post_only_would_cross_stops_it() {
    // The risk check closes cycle 1's first quote for another reason: the loop Stops.
    let venue = venue(vec![b0()]);
    lock(&venue).close_create = Some((1, "NOT_ENOUGH_MARGIN"));
    // The opening six, two places, Stop's cancel of the other quote.
    let stub = StubServer::start(
        one_connection(&venue, 6 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let opts = options(&stub, 5);
    let (report, printed) = run_against(&opts).await;
    stub.finished().await.unwrap();

    assert!(!report.ok, "{printed}");
    assert!(report.ended.contains("asynchronous reject"), "{printed}");
    assert!(report.ended.contains("NOT_ENOUGH_MARGIN"), "{printed}");
    assert_eq!(report.places_sent, 2, "{printed}");
    assert!(printed.contains("DONE failed"), "{printed}");
}

#[tokio::test]
async fn the_interrupt_while_a_pass_reads_a_fresh_touch_places_nothing_after() {
    // As in the fill-during-a-replace test, the bid's cancel races a fill of all of it, so the
    // pass reads a fresh touch (GET /orderbook read 4); the interrupt comes during that read.
    // The pass must take it before planning: nothing is placed after (Codex r4243989985).
    let venue = venue(vec![b0(), b0(), book(2, Some("80000.0"), Some("80000.2"))]);
    let signal = Arc::new(Notify::new());
    {
        let mut v = lock(&venue);
        v.fill_next_buy_cancel = true;
        v.signal = Some((4, Arc::clone(&signal)));
    }
    // The opening six, two places, two replace cancels; nothing rests for Stop to cancel.
    let script = one_connection(&venue, 6 + 2 + 2);
    let stub = StubServer::start(script, routes(&venue, login_ok()))
        .await
        .unwrap();
    let mut opts = options(&stub, 3);
    opts.order_usd = dec("8");
    opts.inventory_cap_usd = dec("16");
    let interrupt = async move { signal.notified().await };
    let (report, printed) = run_until(&opts, interrupt).await;
    stub.finished().await.unwrap();

    assert_eq!(lock(&venue).book_reads, 4, "{printed}");
    assert_eq!(report.ended, "interrupted (Ctrl-C)", "{printed}");
    assert!(report.ok, "{printed}");
    let sent = methods(&venue, 0);
    assert_eq!(
        sent.iter().filter(|m| *m == "order.create").count(),
        2,
        "placed after the interrupt: {sent:?}\n{printed}"
    );
    assert_eq!(report.places_sent, 2, "{printed}");
}

/// A TCP relay in front of the stub's HTTP port (`base`, its `http_url("/v1")`) that never
/// forwards the `n`th `GET /v1/orderbook` request (from 1), telling `signal` as it holds it: a
/// touch the venue does not answer. Its own base URL.
async fn holding_relay(base: &str, n: usize, signal: Arc<Notify>) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let target = base
        .strip_prefix("http://")
        .and_then(|s| s.strip_suffix("/v1"))
        .unwrap()
        .to_owned();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen = Arc::new(AtomicU64::new(0));
    tokio::spawn(async move {
        while let Ok((client, _)) = listener.accept().await {
            let Ok(server) = tokio::net::TcpStream::connect(&target).await else {
                return;
            };
            let (seen, signal) = (Arc::clone(&seen), Arc::clone(&signal));
            tokio::spawn(async move {
                let (mut from_client, mut to_client) = client.into_split();
                let (mut from_server, mut to_server) = server.into_split();
                let up = async move {
                    let mut buf = vec![0u8; 16_384];
                    loop {
                        let k = match from_client.read(&mut buf).await {
                            Ok(0) | Err(_) => break,
                            Ok(k) => k,
                        };
                        let read = &buf[..k];
                        if read.windows(17).any(|w| w == b"GET /v1/orderbook") {
                            let at = seen.fetch_add(1, Ordering::SeqCst) + 1;
                            if at == n as u64 {
                                signal.notify_one();
                                std::future::pending::<()>().await;
                            }
                        }
                        if to_server.write_all(read).await.is_err() {
                            break;
                        }
                    }
                };
                let down = async move {
                    let _ = tokio::io::copy(&mut from_server, &mut to_client).await;
                };
                tokio::join!(up, down);
            });
        }
    });
    format!("http://{addr}/v1")
}

#[tokio::test]
async fn the_interrupt_while_the_touch_is_unanswered_stops_it_at_once() {
    // Cycle 2's GET /orderbook is never answered; the interrupt comes as it is sent. The loop
    // Stops at once, cancelling the quotes cycle 1 left, instead of waiting out --step-timeout
    // for the answer (Codex r4243989987).
    let venue = venue(vec![b0()]);
    let stub = StubServer::start(
        one_connection(&venue, 6 + 2 + 1),
        routes(&venue, login_ok()),
    )
    .await
    .unwrap();
    let signal = Arc::new(Notify::new());
    let mut opts = options(&stub, 1_000);
    opts.rest_url = holding_relay(&opts.rest_url, 3, Arc::clone(&signal)).await;
    opts.step_timeout_secs = 20;
    let interrupt = async move { signal.notified().await };
    let (report, printed) = run_until(&opts, interrupt).await;
    stub.finished().await.unwrap();

    assert!(report.ok, "{printed}");
    assert_eq!(report.ended, "interrupted (Ctrl-C)", "{printed}");
    assert!(!printed.contains("TIMEOUT"), "{printed}");
    assert_eq!(
        (report.places_sent, report.cancels_sent, report.cancelled),
        (2, 1, 2),
        "{printed}"
    );
}

fn strings(args: &[&str]) -> Vec<String> {
    args.iter().map(|s| (*s).to_owned()).collect()
}

/// Every required flag, valid.
fn required() -> Vec<String> {
    strings(&[
        "--sole-trader",
        "--namespace",
        "7",
        "--market",
        "BTC-USD-PERP",
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
        "10.5",
        "--bid-bps",
        "25",
        "--ask-bps",
        "35",
        "--duration",
        "600",
    ])
}

fn parsed(argv: Vec<String>) -> Options {
    match args::parse(argv) {
        Ok(Parsed::Quote(opts)) => *opts,
        other => panic!("{other:?}"),
    }
}

#[test]
fn the_command_line_takes_every_required_flag_and_defaults_to_testnet() {
    let opts = parsed(required());
    assert_eq!((opts.bid_bps, opts.ask_bps), (25, 35));
    assert_eq!((opts.duration_secs, opts.cycles), (600, None));
    assert_eq!((opts.interval_ms, opts.step_timeout_secs), (1_000, 15));
    assert_eq!(opts.rest_url, args::TESTNET_REST);
    assert_eq!(opts.ws_url, args::TESTNET_WS);
    assert_eq!(opts.namespace, 7);
    assert!(
        opts.lease_dir.ends_with(".fueledbychai/testnet_trade"),
        "{:?}",
        opts.lease_dir
    );
    let mut argv = required();
    argv.extend(strings(&[
        "--cycles",
        "5",
        "--interval-ms",
        "250",
        "--step-timeout",
        "30",
        "--lease-dir",
        "/var/tmp/q",
        "--socks5",
        "127.0.0.1:1080",
    ]));
    let opts = parsed(argv);
    assert_eq!(
        (opts.cycles, opts.interval_ms, opts.step_timeout_secs),
        (Some(5), 250, 30)
    );
    assert_eq!(opts.lease_dir, PathBuf::from("/var/tmp/q"));
    assert!(matches!(opts.proxy, ProxyConfig::Socks5 { .. }));
    assert_eq!(args::parse(strings(&["--help"])), Ok(Parsed::Help));
}

#[test]
fn the_command_line_refuses_what_is_missing_out_of_range_or_unknown_never_echoing_it() {
    // Each required flag left out is named.
    for flag in [
        "--market",
        "--tick",
        "--step",
        "--min-notional",
        "--resting-cap-usd",
        "--inventory-cap-usd",
        "--order-usd",
        "--namespace",
        "--bid-bps",
        "--ask-bps",
        "--duration",
    ] {
        let argv = required();
        let at = argv.iter().position(|a| a == flag).unwrap();
        let mut without = argv.clone();
        without.drain(at..at + 2);
        let refused = args::parse(without).unwrap_err();
        assert!(
            refused.contains(&format!("{flag} is required")),
            "{refused}"
        );
    }
    let mut without = required();
    without.remove(0);
    assert!(
        args::parse(without)
            .unwrap_err()
            .contains("--sole-trader is required")
    );
    // Out of range, or not a number: refused, the value never shown.
    for (flag, value) in [
        ("--bid-bps", "0"),
        ("--ask-bps", "2001"),
        ("--duration", "86401"),
        ("--cycles", "0"),
        ("--interval-ms", "99"),
        ("--step-timeout", "121"),
        ("--tick", "-1"),
        ("--min-notional", "SECRETVALUE"),
        ("--market", "SECRETVALUE"),
    ] {
        let mut argv = required();
        argv.extend(strings(&[flag, value]));
        let refused = args::parse(argv).unwrap_err();
        assert!(refused.contains(flag), "{flag}: {refused}");
        assert!(!refused.contains("SECRETVALUE"), "{refused}");
    }
    let mut argv = required();
    argv.extend(strings(&["--order-usd", "12"]));
    assert!(
        args::parse(argv)
            .unwrap_err()
            .contains("above --resting-cap-usd")
    );
    let mut argv = required();
    argv.push("--bid-bps".to_owned());
    assert!(
        args::parse(argv)
            .unwrap_err()
            .contains("--bid-bps needs a value")
    );
    let mut argv = required();
    argv.push("--secret=SECRETVALUE".to_owned());
    let refused = args::parse(argv).unwrap_err();
    assert!(refused.contains("--secret typed with '='"), "{refused}");
    assert!(!refused.contains("SECRETVALUE"), "{refused}");
    let mut argv = required();
    argv.push("--nope".to_owned());
    assert!(
        args::parse(argv)
            .unwrap_err()
            .contains("unknown argument --nope")
    );
    let mut argv = required();
    argv.push("SECRETVALUE".to_owned());
    let refused = args::parse(argv).unwrap_err();
    assert!(refused.contains("not a flag"), "{refused}");
    assert!(!refused.contains("SECRETVALUE"), "{refused}");
}

#[test]
fn help_says_it_is_a_plumbing_test_not_a_strategy_and_names_every_line() {
    assert!(USAGE.contains("PLUMBING TEST, NOT A STRATEGY"));
    assert!(USAGE.contains("cancel-and-replace, never an amend"));
    for name in [
        "PARADEX_ACCOUNT_ADDRESS",
        "PARADEX_PRIVATE_KEY",
        args::CHAIN_VAR,
        "--bid-bps",
        "--ask-bps",
        "--duration",
        "--cycles",
        "Ctrl-C",
        "TESTNET ONLY",
        "PLACE",
        "CANCEL-ACK",
        "RECONNECT",
        "SUMMARY",
        "DONE ok|failed",
    ] {
        assert!(USAGE.contains(name), "{name}");
    }
}

/// A book snapshot for the pure functions, through the decoder the loop reads with.
fn snapshot(
    bid: Option<&str>,
    ask: Option<&str>,
) -> fbc_venue_paradex::md::rest::OrderbookSnapshot {
    let cfg = testnet::config("http://127.0.0.1:1/v1", "ws://127.0.0.1:1/v1");
    let specs = testnet::specs(
        &fbc_venue_paradex::ParadexFactory,
        &cfg,
        MARKET,
        dec("0.1"),
        dec("0.00001"),
    )
    .unwrap();
    fbc_venue_paradex::md::rest::decode_orderbook(book(1, bid, ask).as_bytes(), &specs).unwrap()
}

fn plain_options() -> Options {
    let mut opts = parsed(required());
    opts.min_notional = dec("5");
    opts.bid_bps = 30;
    opts.ask_bps = 30;
    opts
}

#[test]
fn each_side_is_quoted_only_within_the_caps_and_behind_its_own_touch() {
    let opts = plain_options();
    let qty = Lots::new(16).unwrap();
    let full = snapshot(Some("62000.2"), Some("62000.5"));
    let wanted = quote::desired(&opts, qty, SignedLots(0), &full);
    assert_eq!(
        (wanted.bid, wanted.ask),
        (Some(Ticks(618_141)), Some(Ticks(621_866)))
    );
    assert!(wanted.skipped.is_empty(), "{:?}", wanted.skipped);
    assert_eq!(wanted.book.quote(Side::Buy, 0).unwrap().qty, qty);
    assert!(wanted.book.quote(Side::Sell, 0).unwrap().post_only);

    // A one-sided book quotes the side that rests.
    let bids_only = quote::desired(&opts, qty, SignedLots(0), &snapshot(Some("62000.2"), None));
    assert_eq!((bids_only.bid.is_some(), bids_only.ask), (true, None));
    assert!(bids_only.skipped[0].contains("ask: its book side is empty"));

    // A quote above the resting cap at its price, or below the minimum notional, is left out.
    let big = quote::desired(&opts, Lots::new(18).unwrap(), SignedLots(0), &full);
    assert!(
        big.skipped
            .iter()
            .all(|s| s.contains("above --resting-cap-usd")),
        "{:?}",
        big.skipped
    );
    let small = quote::desired(&opts, Lots::new(5).unwrap(), SignedLots(0), &full);
    assert!(
        small
            .skipped
            .iter()
            .all(|s| s.contains("below the market's minimum"))
    );
    assert_eq!(small.book.quote(Side::Buy, 0), None);

    // Long 70 lots: a bid fill would take the position to 86 lots, $53.5 at the ask quote,
    // past the $50 inventory cap; the ask, reducing it, is quoted.
    let long = quote::desired(&opts, qty, SignedLots(70), &full);
    assert_eq!((long.bid, long.ask.is_some()), (None, true));
    assert!(long.skipped[0].starts_with("bid: a fill would take the position to 86 lots"));
    let short = quote::desired(&opts, qty, SignedLots(-70), &full);
    assert_eq!((short.bid.is_some(), short.ask), (true, None));

    // A bid so far down it would be priced at zero is refused, not quoted at zero.
    let mut far = opts.clone();
    far.bid_bps = 2_000;
    let tiny = quote::quote_prices(&far, &snapshot(Some("0.1"), Some("0.2")));
    assert!(tiny.0.unwrap_err().contains("strictly behind the touch"));
}

#[test]
fn the_quote_size_and_caps_come_from_the_first_touch() {
    let opts = plain_options();
    let sizing = quote::size(&opts, &snapshot(Some("62000.2"), Some("62000.5"))).unwrap();
    assert_eq!(
        (
            sizing.qty.get(),
            sizing.resting.get(),
            sizing.inventory.get(),
            sizing.at
        ),
        (16, 17, 80, dec("62186.6"))
    );
    // No side to quote, no lot in the size, or a notional below the market's minimum: refused.
    assert!(
        quote::size(&opts, &snapshot(None, None))
            .unwrap_err()
            .contains("no side")
    );
    let mut tiny = opts.clone();
    tiny.order_usd = dec("0.1");
    assert!(
        quote::size(&tiny, &snapshot(Some("62000.2"), None))
            .unwrap_err()
            .contains("less than one size step")
    );
    let mut strict = opts.clone();
    strict.min_notional = dec("10");
    assert!(
        quote::size(&strict, &snapshot(Some("62000.2"), Some("62000.5")))
            .unwrap_err()
            .contains("below the market's minimum")
    );
}

#[test]
fn the_planner_never_gets_an_amend_and_the_shape_is_checked_against_the_caps() {
    let caps = fbc_venue_paradex::exec::exec_caps().order;
    quote::shape_admitted(&caps).unwrap();
    assert!(quote::cancel_and_replace(&caps).amend.is_none());
    let mut no_post_only = caps.clone();
    no_post_only.post_only = false;
    assert!(
        quote::shape_admitted(&no_post_only)
            .unwrap_err()
            .contains("post-only GTC limit")
    );
}
